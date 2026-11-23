//! Live sessions and the lease that proves a session owns its agent identity.
//!
//! One lease key per agent in `XAGENT_LEASES`, refreshed every ~10 s. The bucket's server-side
//! TTL (~30 s) removes a key that stops being refreshed, so expiry is decided by the NATS server
//! alone. Local authority is bounded independently: a session may pull or send only while its
//! last successful lease write was *sent* less than `TTL - margin` ago (monotonic clock). The
//! server can only expire the key at least TTL after storing that write, so local authority
//! always ends before another host can take the identity over.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use xagent_bus::{Bus, BusError};
use xagent_proto::{
    AgentId, Client, Conversation, LeaseRecord, MessageId, PersistentSubscription, WORKSPACE,
    now_ts,
};

/// Upper bound on the lease refresh interval.
const REFRESH_INTERVAL_MAX: Duration = Duration::from_secs(10);
/// Retry interval while a lease is not confirmed.
const RETRY_INTERVAL: Duration = Duration::from_secs(1);

/// Where a session's lease stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LeasePhase {
    /// Confirmed by the last refresh.
    Held,
    /// The last refresh failed (NATS unreachable, timeout): no pulling; sends only while fresh.
    Unconfirmed,
    /// Another instance owns the identity. Terminal.
    Lost,
}

impl LeasePhase {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Held => "held",
            Self::Unconfirmed => "held_unconfirmed",
            Self::Lost => "lost",
        }
    }
}

#[derive(Debug)]
struct LeaseState {
    phase: LeasePhase,
    revision: Option<u64>,
    /// Monotonic send time of the last successful acquire or refresh.
    confirmed_at: Instant,
    error: Option<String>,
}

/// Timing derived from the server-side lease TTL.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LeaseTiming {
    ttl: Duration,
}

impl LeaseTiming {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self { ttl }
    }

    /// How long after a confirmed write this instance may act: the TTL minus a safety margin.
    fn authority_window(self) -> Duration {
        self.ttl
            .saturating_sub((self.ttl / 10).min(Duration::from_secs(2)))
    }

    pub(crate) fn refresh_interval(self) -> Duration {
        REFRESH_INTERVAL_MAX.min(self.ttl / 3)
    }
}

/// Why a session ended or failed to start, for logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndReason {
    Disconnect,
    DaemonShutdown,
    StartFailed,
}

impl EndReason {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Disconnect => "disconnect",
            Self::DaemonShutdown => "daemon_shutdown",
            Self::StartFailed => "start_failed",
        }
    }
}

/// The session's view of its agent's persistent subscription set (`XAGENT_SUBSCRIPTIONS` is the
/// authority). Its lock serializes every change to the set and the consumer filters, and the
/// receiver holds it while deciding whether a topic message is delivered.
#[derive(Debug, Default)]
pub(crate) struct Subscriptions {
    pub(crate) topics: Vec<PersistentSubscription>,
    /// A join whose shared write had an unknown outcome: topic deliveries wait until the set has
    /// been read again.
    pub(crate) unsettled: bool,
}

/// One conversation bound to one IPC connection, owning its agent identity while its lease holds.
#[derive(Debug)]
pub(crate) struct Session {
    pub(crate) instance_id: MessageId,
    pub(crate) agent: AgentId,
    pub(crate) client: Client,
    pub(crate) conversation: Conversation,
    /// The Claude Code process of the conversation's window, when the session was opened by one:
    /// the wake reads its registry record by pid.
    pub(crate) claude_pid: Option<u32>,
    pub(crate) started_at: String,
    pub(crate) opened: Instant,
    timing: LeaseTiming,
    lease: Mutex<LeaseState>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    closing: AtomicBool,
    /// Stop-hook reminder bookkeeping.
    pub(crate) notice: Mutex<crate::wake::Notice>,
    /// Becomes `true` once the session is fully closed and its lease released.
    closed: watch::Sender<bool>,
    pub(crate) subscriptions: tokio::sync::Mutex<Subscriptions>,
    /// The project root topic joined at session start, if the session's directory has one.
    pub(crate) project_topic: Mutex<Option<String>>,
}

/// What a refresh found.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RefreshOutcome {
    Confirmed,
    /// The key had vanished (server-side expiry) and was taken again: anything queued before the
    /// gap must not be published after it.
    Reacquired,
    Unconfirmed,
    Lost,
}

impl Session {
    pub(crate) fn new(
        agent: AgentId,
        client: Client,
        conversation: Conversation,
        claude_pid: Option<u32>,
        timing: LeaseTiming,
    ) -> Self {
        Self {
            instance_id: MessageId::new_v4(),
            agent,
            client,
            conversation,
            claude_pid,
            started_at: now_ts(),
            opened: Instant::now(),
            timing,
            lease: Mutex::new(LeaseState {
                phase: LeasePhase::Unconfirmed,
                revision: None,
                confirmed_at: Instant::now(),
                error: Some("not yet acquired".to_owned()),
            }),
            tasks: Mutex::new(Vec::new()),
            closing: AtomicBool::new(false),
            notice: Mutex::new(crate::wake::Notice::default()),
            closed: watch::Sender::new(false),
            subscriptions: tokio::sync::Mutex::default(),
            project_topic: Mutex::new(None),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, LeaseState> {
        self.lease.lock().expect("lease lock poisoned")
    }

    pub(crate) fn phase(&self) -> LeasePhase {
        self.state().phase
    }

    pub(crate) fn lease_error(&self) -> Option<String> {
        self.state().error.clone()
    }

    fn fresh(&self, state: &LeaseState) -> bool {
        state.revision.is_some() && state.confirmed_at.elapsed() < self.timing.authority_window()
    }

    /// Remaining local authority.
    pub(crate) fn authority_remaining(&self) -> Duration {
        let state = self.state();
        if state.phase == LeasePhase::Lost || state.revision.is_none() {
            return Duration::ZERO;
        }
        self.timing
            .authority_window()
            .saturating_sub(state.confirmed_at.elapsed())
    }

    /// Pull and ACK the agent's consumer.
    pub(crate) fn may_consume(&self) -> bool {
        let state = self.state();
        state.phase == LeasePhase::Held && self.fresh(&state)
    }

    /// Create and publish a message as this agent right now.
    pub(crate) fn may_send(&self) -> bool {
        let state = self.state();
        state.phase != LeasePhase::Lost && self.fresh(&state)
    }

    /// Starts closing; `false` if another caller already did.
    pub(crate) fn begin_close(&self) -> bool {
        !self.closing.swap(true, Ordering::SeqCst)
    }

    pub(crate) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::SeqCst)
    }

    pub(crate) fn mark_closed(&self) {
        self.closed.send_replace(true);
    }

    /// Waits until the session is fully closed.
    pub(crate) async fn wait_closed(&self) {
        let mut rx = self.closed.subscribe();
        let _ = rx.wait_for(|closed| *closed).await;
    }

    pub(crate) fn add_task(&self, task: JoinHandle<()>) {
        self.tasks.lock().expect("task lock poisoned").push(task);
    }

    /// Stops the session's background tasks and waits for them to end.
    pub(crate) async fn stop_tasks(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().expect("task lock poisoned"));
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
    }

    pub(crate) fn record(&self, bus: &Bus, host_id: &str, run_id: &str) -> LeaseRecord {
        LeaseRecord {
            workspace: WORKSPACE.to_owned(),
            agent_id: self.agent.clone(),
            instance_id: self.instance_id,
            host_id: host_id.to_owned(),
            daemon_run_id: run_id.to_owned(),
            client: self.client.as_str().to_owned(),
            acquired_at: self.started_at.clone(),
            refreshed_at: now_ts(),
            ttl_seconds: bus.lease_ttl().as_secs_f64(),
        }
    }

    fn confirm(&self, revision: u64, sent_at: Instant) {
        let mut state = self.state();
        if state.phase == LeasePhase::Lost {
            return;
        }
        state.phase = LeasePhase::Held;
        state.revision = Some(revision);
        state.confirmed_at = sent_at;
        state.error = None;
    }

    fn unconfirm(&self, error: String) {
        let mut state = self.state();
        if state.phase != LeasePhase::Lost {
            state.phase = LeasePhase::Unconfirmed;
        }
        state.error = Some(error);
    }

    fn lose(&self, reason: String) {
        let mut state = self.state();
        state.phase = LeasePhase::Lost;
        state.error = Some(reason);
    }

    /// Takes the lease for a new session.
    ///
    /// # Errors
    ///
    /// The bus error: [`BusError::LeaseHeld`] when another instance owns the identity.
    pub(crate) async fn acquire(&self, bus: &Bus, record: &LeaseRecord) -> Result<(), BusError> {
        let sent_at = Instant::now();
        let revision = bus.acquire_lease(record).await?;
        self.confirm(revision, sent_at);
        Ok(())
    }

    /// One lease refresh: compare-and-set on the known revision, resolving conflicts.
    pub(crate) async fn refresh(&self, bus: &Bus, record: &LeaseRecord) -> RefreshOutcome {
        let (phase, revision) = {
            let state = self.state();
            (state.phase, state.revision)
        };
        if phase == LeasePhase::Lost {
            return RefreshOutcome::Lost;
        }
        let sent_at = Instant::now();
        let result = match revision {
            Some(revision) => bus.refresh_lease(record, revision).await,
            None => Err(BusError::LeaseConflict),
        };
        match result {
            Ok(revision) => {
                self.confirm(revision, sent_at);
                return RefreshOutcome::Confirmed;
            }
            Err(BusError::LeaseConflict) => {}
            Err(err) => {
                self.unconfirm(err.to_string());
                return RefreshOutcome::Unconfirmed;
            }
        }
        // The key expired server-side, another instance wrote it, or our own earlier write
        // landed without its reply.
        match bus.lease(WORKSPACE, &self.agent).await {
            Ok(Some((held, revision))) if held.instance_id == self.instance_id => {
                let sent_at = Instant::now();
                if let Ok(revision) = bus.refresh_lease(record, revision).await {
                    self.confirm(revision, sent_at);
                    RefreshOutcome::Confirmed
                } else {
                    self.unconfirm("lease revision race; retrying".to_owned());
                    RefreshOutcome::Unconfirmed
                }
            }
            Ok(Some((held, _))) => {
                self.lose(format!("taken over by host {}", held.host_id));
                RefreshOutcome::Lost
            }
            Ok(None) => match self.acquire(bus, record).await {
                Ok(()) => RefreshOutcome::Reacquired,
                Err(BusError::LeaseHeld { host_id, .. }) => {
                    self.lose(format!("taken over by host {host_id}"));
                    RefreshOutcome::Lost
                }
                Err(err) => {
                    self.unconfirm(err.to_string());
                    RefreshOutcome::Unconfirmed
                }
            },
            Err(err) => {
                self.unconfirm(err.to_string());
                RefreshOutcome::Unconfirmed
            }
        }
    }

    /// Marks the lease unconfirmed (e.g. a refresh timed out).
    pub(crate) fn mark_unconfirmed(&self, error: String) {
        self.unconfirm(error);
    }

    /// The delay before the next refresh attempt.
    pub(crate) fn next_refresh_in(&self) -> Duration {
        if self.phase() == LeasePhase::Held {
            self.timing.refresh_interval()
        } else {
            RETRY_INTERVAL.min(self.timing.refresh_interval())
        }
    }
}
