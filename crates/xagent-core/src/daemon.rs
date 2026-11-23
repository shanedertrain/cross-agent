//! The daemon: NATS supervision, the session table, session open and close, and lease keeping.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::time::Duration;

use serde::Serialize;
use tokio::sync::{Notify, OnceCell};
use tracing::{error, info, warn};
use xagent_bus::{BootstrapReport, Bus, BusError, Connection, NatsSettings};
use xagent_proto::{
    AgentId, Client, Conversation, Error, ErrorCode, WORKSPACE, conversation_agent_id, now_ts,
};
use xagent_store::{Store, StoreError};
use xagent_wake::Endpoint;

use crate::session::{EndReason, LeaseTiming, RefreshOutcome, Session};

/// Delay between bootstrap attempts while NATS is unreachable.
const BOOTSTRAP_RETRY: Duration = Duration::from_secs(3);
/// How long a new session waits for a closing session of the same conversation.
const CLOSE_WAIT: Duration = Duration::from_secs(10);
/// Extra time a lease refresh may take beyond the request timeout before it counts as failed.
const REFRESH_SLACK: Duration = Duration::from_millis(3500);

/// Everything the daemon needs to run.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    pub host_id: String,
    /// The daemon user's home: the clients' default profile directories (`~/.claude`, `~/.codex`)
    /// for a session opened without a window.
    pub home: PathBuf,
    pub nats: NatsSettings,
    pub socket: PathBuf,
    pub state_db: PathBuf,
    pub wake: xagent_wake::WakeConfig,
    /// Project directories and their root topics.
    pub projects: Vec<Project>,
    /// Oldest local copy kept on this host; the broker's own retention caps it.
    pub local_retention: Duration,
}

/// A project directory tree and the root topic its sessions join.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Project {
    pub path: PathBuf,
    /// A single-token topic, e.g. `editor-auto`.
    pub topic: String,
}

/// State of the JetStream bootstrap, for `status`.
#[derive(Debug, Clone, Default)]
pub(crate) enum Readiness {
    #[default]
    Connecting,
    Ready(BootstrapReport),
    /// A fatal mismatch: the message pipeline will not run until an operator fixes it.
    Incompatible(String),
    /// Another live daemon holds this host's `host_id`; retried until its lease ends.
    HostIdTaken(String),
}

/// The shared daemon state. Cheap to clone handles live in `Arc<Daemon>`.
#[derive(Debug)]
pub(crate) struct Daemon {
    pub(crate) config: DaemonConfig,
    pub(crate) run_id: String,
    pub(crate) started_at: String,
    store: Mutex<Store>,
    pub(crate) connection: Connection,
    bus: OnceCell<Bus>,
    pub(crate) readiness: Mutex<Readiness>,
    sessions: Mutex<HashMap<AgentId, Arc<Session>>>,
    pub(crate) outbox_kick: Notify,
    pub(crate) waker: xagent_wake::Waker,
    pub(crate) reach_cache: Mutex<Option<crate::messaging::ReachCache>>,
    pub(crate) windows: Mutex<crate::windows::Windows>,
    pub(crate) peek_kick: Notify,
    /// Why the daemon must stop (its host lease was taken over); `fatal_kick` signals it.
    pub(crate) fatal: OnceLock<String>,
    pub(crate) fatal_kick: Notify,
}

pub(crate) fn internal(err: &StoreError) -> Error {
    error!(error = %err, "local store failure");
    Error::new(ErrorCode::Internal, "local state store failure")
}

fn nats_unavailable(what: &str) -> Error {
    Error::new(
        ErrorCode::NatsUnavailable,
        format!("{what} needs NATS, which is unreachable"),
    )
}

#[derive(Debug, Serialize)]
pub(crate) struct Whoami {
    agent_id: String,
    instance_id: String,
    workspace: &'static str,
    client: String,
    conversation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease: Option<&'static str>,
}

impl Daemon {
    pub(crate) fn new(config: DaemonConfig, store: Store, connection: Connection) -> Arc<Self> {
        Arc::new_cyclic(|daemon| Self {
            waker: xagent_wake::Waker::new(
                config.wake.clone(),
                Arc::new(crate::wake::WakeView::new(daemon.clone())),
            ),
            config,
            run_id: uuid::Uuid::new_v4().to_string(),
            started_at: now_ts(),
            store: Mutex::new(store),
            connection,
            bus: OnceCell::new(),
            readiness: Mutex::new(Readiness::Connecting),
            sessions: Mutex::new(HashMap::new()),
            outbox_kick: Notify::new(),
            reach_cache: Mutex::new(None),
            windows: Mutex::default(),
            peek_kick: Notify::new(),
            fatal: OnceLock::new(),
            fatal_kick: Notify::new(),
        })
    }

    pub(crate) fn store(&self) -> MutexGuard<'_, Store> {
        self.store.lock().expect("store lock poisoned")
    }

    pub(crate) fn readiness(&self) -> Readiness {
        self.readiness
            .lock()
            .expect("readiness lock poisoned")
            .clone()
    }

    /// The bootstrapped bus, while NATS is connected.
    pub(crate) fn bus(&self) -> Option<&Bus> {
        self.bus.get().filter(|_| self.connection.is_connected())
    }

    pub(crate) fn require_bus(&self, what: &str) -> Result<&Bus, Error> {
        self.bus().ok_or_else(|| nats_unavailable(what))
    }

    pub(crate) fn sessions(&self) -> Vec<Arc<Session>> {
        self.sessions
            .lock()
            .expect("session lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn session_by_instance(&self, instance_id: &str) -> Option<Arc<Session>> {
        self.sessions()
            .into_iter()
            .find(|s| s.instance_id.to_string() == instance_id)
    }

    /// Bootstraps JetStream once NATS is reachable and takes the host lease, retrying until it
    /// succeeds or finds a fatal mismatch; then keeps the host lease until the daemon stops.
    pub(crate) async fn bootstrap_loop(self: Arc<Self>) {
        loop {
            match self.connection.bootstrap().await {
                Ok((bus, report)) => {
                    let revision = self.acquire_host(&bus).await;
                    self.bus.set(bus.clone()).expect("bootstrap succeeds once");
                    *self.readiness.lock().expect("readiness lock poisoned") =
                        Readiness::Ready(report);
                    info!("nats ready");
                    self.outbox_kick.notify_one();
                    return self.keep_host_lease(bus, revision).await;
                }
                Err(BusError::Incompatible(mismatches)) => {
                    for m in &mismatches {
                        error!(error_code = %ErrorCode::StreamMismatch, mismatch = %m,
                               "incompatible jetstream resource");
                    }
                    let summary = mismatches
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>();
                    *self.readiness.lock().expect("readiness lock poisoned") =
                        Readiness::Incompatible(summary.join("; "));
                    return;
                }
                Err(err) => {
                    warn!(error = %err, "jetstream bootstrap failed; retrying");
                    tokio::time::sleep(BOOTSTRAP_RETRY).await;
                }
            }
        }
    }

    /// Binds a session to `conversation`, woken at `endpoint` (the window's; the client's default
    /// when it has none or names another client): derives its agent id, takes the lease, registers the
    /// agent, ensures its durable consumer, and starts lease keeping and receiving.
    ///
    /// # Errors
    ///
    /// `duplicate_active_agent` when another session (on any host) holds the identity,
    /// `nats_unavailable` when the broker cannot confirm the lease.
    pub(crate) async fn open_session(
        self: &Arc<Self>,
        conversation: Conversation,
        client: Client,
        cwd: Option<&Path>,
        endpoint: Option<Endpoint>,
    ) -> Result<Arc<Session>, Error> {
        let agent = conversation_agent_id(&client, &conversation);
        let endpoint = endpoint
            .filter(|e| e.client() == client)
            .unwrap_or_else(|| Endpoint::default_for(client.clone(), &self.config.home));
        let bus = self.require_bus("starting a session")?.clone();
        let session = loop {
            let closing = {
                let mut sessions = self.sessions.lock().expect("session lock poisoned");
                match sessions.get(&agent) {
                    // A client that reconnects may beat its old connection's close: wait for it.
                    Some(existing) if existing.is_closing() => Arc::clone(existing),
                    Some(existing) => {
                        return Err(Error::new(
                            ErrorCode::DuplicateActiveAgent,
                            format!(
                                "agent {agent} already has a live session on this host (client \
                                 {}, since {}). Stop it first.",
                                existing.client, existing.started_at
                            ),
                        ));
                    }
                    None => {
                        let session = Arc::new(Session::new(
                            agent.clone(),
                            client.clone(),
                            conversation.clone(),
                            endpoint,
                            LeaseTiming::new(bus.lease_ttl()),
                        ));
                        // Reserved before awaiting, so a concurrent open on this host fails at once.
                        sessions.insert(agent.clone(), Arc::clone(&session));
                        break session;
                    }
                }
            };
            if tokio::time::timeout(CLOSE_WAIT, closing.wait_closed())
                .await
                .is_err()
            {
                return Err(Error::new(
                    ErrorCode::DuplicateActiveAgent,
                    format!(
                        "agent {agent}'s previous session on this host is still closing; retry"
                    ),
                ));
            }
        };
        if let Err(err) = self.start_session(&bus, &session, cwd).await {
            self.close_session(&session, EndReason::StartFailed).await;
            return Err(err);
        }
        info!(agent = %agent, instance_id = %session.instance_id, client = %client,
              "session opened");
        Ok(session)
    }

    async fn start_session(
        self: &Arc<Self>,
        bus: &Bus,
        session: &Arc<Session>,
        cwd: Option<&Path>,
    ) -> Result<(), Error> {
        let record = session.record(bus, &self.config.host_id, &self.run_id);
        session
            .acquire(bus, &record)
            .await
            .map_err(|err| match err {
                BusError::LeaseHeld { .. } => {
                    Error::new(ErrorCode::DuplicateActiveAgent, err.to_string())
                }
                other => {
                    warn!(agent = %session.agent, error = %other, "lease acquire failed");
                    nats_unavailable("confirming the session lease")
                }
            })?;
        session.add_task(tokio::spawn(
            Arc::clone(self).keep_lease(Arc::clone(session)),
        ));
        bus.upsert_agent(
            WORKSPACE,
            &session.agent,
            session.client.as_str(),
            &self.config.host_id,
        )
        .await
        .map_err(|err| {
            warn!(agent = %session.agent, error = %err, "agent registration failed");
            nats_unavailable("registering the agent")
        })?;
        // Subscriptions are restored and the project joined before anything is pulled, so no
        // topic message is judged against an incomplete set.
        self.restore_subscriptions(bus, session).await?;
        if let Some(topic) = self.project_topic(cwd) {
            self.join_project(bus, session, topic).await?;
        }
        let inbox = self.bind_consumer(bus, session).await.map_err(|err| {
            warn!(agent = %session.agent, error = %err, "consumer setup failed");
            nats_unavailable("creating the agent's inbox consumer")
        })?;
        session.add_task(tokio::spawn(
            Arc::clone(self).receive(Arc::clone(session), inbox),
        ));
        Ok(())
    }

    /// Ends a session: stop pulling, give its queued sends a last chance, fail the rest, then
    /// release the lease for another host to take.
    pub(crate) async fn close_session(&self, session: &Arc<Session>, reason: EndReason) {
        if !session.begin_close() {
            return;
        }
        session.stop_tasks().await;
        if session.may_send() {
            self.flush_outbox().await;
        }
        match self.store().fail_pending_for_instance(
            &session.instance_id.to_string(),
            "sender_not_authorized: session ended before delivery",
        ) {
            Ok(0) => {}
            Ok(n) => error!(agent = %session.agent, failed = n,
                            "queued sends of an ended session will not be published"),
            Err(err) => {
                internal(&err);
            }
        }
        if session.phase() != crate::session::LeasePhase::Lost
            && let Some(bus) = self.bus()
            && let Err(err) = bus
                .release_lease(WORKSPACE, &session.agent, session.instance_id)
                .await
        {
            warn!(agent = %session.agent, error = %err, "lease release failed; it expires on its own");
        }
        {
            let mut sessions = self.sessions.lock().expect("session lock poisoned");
            if sessions
                .get(&session.agent)
                .is_some_and(|s| Arc::ptr_eq(s, session))
            {
                sessions.remove(&session.agent);
            }
        }
        session.mark_closed();
        info!(agent = %session.agent, instance_id = %session.instance_id,
              reason = reason.as_str(), "session closed");
    }

    /// Closes every session (daemon shutdown).
    pub(crate) async fn close_all(&self) {
        for session in self.sessions() {
            self.close_session(&session, EndReason::DaemonShutdown)
                .await;
        }
    }

    /// Refreshes the session's lease until the session ends.
    async fn keep_lease(self: Arc<Self>, session: Arc<Session>) {
        loop {
            tokio::time::sleep(session.next_refresh_in()).await;
            let Some(bus) = self.bus().cloned() else {
                session.mark_unconfirmed("nats unavailable".to_owned());
                continue;
            };
            let record = session.record(&bus, &self.config.host_id, &self.run_id);
            let deadline = session.next_refresh_in().max(REFRESH_SLACK);
            let outcome = tokio::time::timeout(deadline, session.refresh(&bus, &record)).await;
            let reason = match outcome {
                Ok(RefreshOutcome::Confirmed | RefreshOutcome::Unconfirmed) => continue,
                Ok(RefreshOutcome::Reacquired) => {
                    "sender_not_authorized: lease lapsed before delivery"
                }
                Ok(RefreshOutcome::Lost) => "sender_not_authorized: lease lost",
                Err(_) => {
                    session.mark_unconfirmed(format!("lease refresh timed out after {deadline:?}"));
                    continue;
                }
            };
            warn!(agent = %session.agent, reason, lease_error = ?session.lease_error(),
                  "lease gap: queued sends of this session will not be published");
            if let Err(err) = self
                .store()
                .fail_pending_for_instance(&session.instance_id.to_string(), reason)
            {
                internal(&err);
            }
        }
    }

    pub(crate) fn whoami(session: &Session) -> Whoami {
        Whoami {
            agent_id: session.agent.to_string(),
            instance_id: session.instance_id.to_string(),
            workspace: WORKSPACE,
            client: session.client.to_string(),
            conversation: session.conversation.to_string(),
            lease: (!session.may_consume()).then(|| session.phase().as_str()),
        }
    }

    /// Refuses a call on a session that lost its identity.
    pub(crate) fn check_session(session: &Session) -> Result<(), Error> {
        if session.phase() == crate::session::LeasePhase::Lost {
            return Err(Error::new(
                ErrorCode::LeaseLost,
                format!(
                    "another instance now owns agent {}; this session can no longer act. Restart it.",
                    session.agent
                ),
            ));
        }
        Ok(())
    }
}
