//! Topics, persistent subscriptions and the project root topic.
//!
//! An agent's persistent subscription set lives in `XAGENT_SUBSCRIPTIONS` and follows the
//! identity between hosts; the session holding the lease keeps a copy, adopted from KV before it
//! pulls anything. Its durable consumer's filters are always derived from that set: the DM subject
//! plus one chat subject per topic.
//!
//! A join takes effect from the stream position at subscribe time, not from history: each
//! subscription records the stream's last sequence (`start_seq`) and only later messages on the
//! topic are delivered. The filter is activated before the boundary is taken, so nothing after the
//! boundary can be missed, and anything the filter admits up to the boundary is dropped.

use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;
use tracing::{info, warn};
use xagent_bus::{Bus, BusError, Inbox};
use xagent_proto::{
    AgentId, Error, ErrorCode, PersistentSubscription, TopicRecord, WORKSPACE, chat_subject,
    dm_subject, now_ts, validate_topic,
};

use crate::daemon::Daemon;
use crate::protocol::{TopicCreate, TopicList, TopicRef};
use crate::session::{Session, Subscriptions};

const DESCRIPTION_MAX_CHARS: usize = 500;

pub(crate) fn nats_error(what: &str, err: &BusError) -> Error {
    warn!(error = %err, "{what} failed");
    Error::new(
        ErrorCode::NatsUnavailable,
        format!("{what} needs NATS, which did not answer; retry"),
    )
}

/// The consumer filters for an agent: its DM subject and one subject per exact topic. Patterns
/// that are not exact topics (wildcards written by older versions) are not applied.
pub(crate) fn filters(agent: &AgentId, topics: &[PersistentSubscription]) -> Vec<String> {
    let mut out = vec![dm_subject(WORKSPACE, agent)];
    for sub in topics {
        if validate_topic(&sub.pattern).is_ok() {
            out.push(chat_subject(WORKSPACE, &sub.pattern));
        } else {
            warn!(agent = %agent, pattern = sub.pattern, "subscription pattern not applied");
        }
    }
    out
}

/// Whether a topic message at stream sequence `seq` is past the boundary of a subscription.
pub(crate) fn is_subscribed(topics: &[PersistentSubscription], topic: &str, seq: u64) -> bool {
    topics
        .iter()
        .any(|s| s.pattern == topic && s.start_seq.is_some_and(|start| seq > start))
}

#[derive(Debug, Serialize)]
struct TopicView {
    name: String,
    description: String,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    archived: bool,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    subscribed: bool,
}

/// What a join did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum JoinStatus {
    Subscribed,
    AlreadySubscribed,
}

impl Daemon {
    /// The registered topic, or `topic_not_found`.
    pub(crate) async fn require_topic(&self, bus: &Bus, name: &str) -> Result<TopicRecord, Error> {
        validate_topic(name)?;
        match bus.topic(WORKSPACE, name).await {
            Ok(Some(record)) => Ok(record),
            Ok(None) => Err(Error::new(
                ErrorCode::TopicNotFound,
                format!(
                    "topic {name:?} is not registered. Check topic_list, or topic_create it if a \
                     new room is intended."
                ),
            )),
            Err(err) => Err(nats_error("checking the topic", &err)),
        }
    }

    /// Adopts the shared set (a compare-and-set write that fences off older writers) and makes the
    /// local copy equal to it.
    async fn adopt(bus: &Bus, session: &Session, subs: &mut Subscriptions) -> Result<(), BusError> {
        let set = bus
            .update_subscriptions(WORKSPACE, &session.agent, |_| {}, || session.may_consume())
            .await?;
        subs.topics = set.subscriptions;
        subs.unsettled = false;
        Ok(())
    }

    /// Applies the filters derived from `subs` to the agent's durable consumer.
    async fn apply_filters(
        &self,
        bus: &Bus,
        session: &Session,
        subs: &Subscriptions,
    ) -> Result<Inbox, BusError> {
        let inbox = bus
            .ensure_consumer(
                WORKSPACE,
                &session.agent,
                filters(&session.agent, &subs.topics),
            )
            .await?;
        self.forget_reach();
        Ok(inbox)
    }

    /// Restores the agent's subscriptions at session start.
    pub(crate) async fn restore_subscriptions(
        &self,
        bus: &Bus,
        session: &Session,
    ) -> Result<(), Error> {
        let mut subs = session.subscriptions.lock().await;
        Self::adopt(bus, session, &mut subs)
            .await
            .map_err(|err| nats_error("restoring the agent's subscriptions", &err))
    }

    /// Creates or updates the agent's consumer with its full filter set.
    pub(crate) async fn bind_consumer(
        &self,
        bus: &Bus,
        session: &Session,
    ) -> Result<Inbox, BusError> {
        let subs = session.subscriptions.lock().await;
        self.apply_filters(bus, session, &subs).await
    }

    /// Before a topic message is judged: settles an uncertain join by reading the shared set
    /// again. `false` when that is not possible yet.
    pub(crate) async fn settle(&self, session: &Session, subs: &mut Subscriptions) -> bool {
        if !subs.unsettled {
            return true;
        }
        let Some(bus) = self.bus().cloned() else {
            return false;
        };
        match Self::adopt(&bus, session, subs).await {
            Ok(()) => {
                if let Err(err) = self.apply_filters(&bus, session, subs).await {
                    warn!(agent = %session.agent, error = %err, "consumer update deferred");
                }
                info!(agent = %session.agent, "uncertain join reconciled");
                true
            }
            Err(err) => {
                warn!(agent = %session.agent, error = %err, "join reconciliation failed");
                false
            }
        }
    }

    /// The root topic of the project `cwd` lies in: the most specific configured path wins;
    /// otherwise the git checkout's directory name (a linked worktree counts as its main
    /// checkout); otherwise `scratch`.
    pub(crate) fn project_topic(&self, cwd: Option<&Path>) -> Option<String> {
        let here = real(cwd?);
        let configured = self
            .config
            .projects
            .iter()
            .map(|p| (real(&p.path), &p.topic))
            .filter(|(path, _)| here.starts_with(path))
            .max_by_key(|(path, _)| path.components().count())
            .map(|(_, topic)| topic.clone());
        configured.or_else(|| {
            let checkout = here.ancestors().find(|d| d.join(".git").exists());
            let name = checkout.map(main_checkout).and_then(|d| topic_name(&d));
            Some(name.unwrap_or_else(|| SCRATCH_TOPIC.to_owned()))
        })
    }

    /// Joins the project root topic, creating it if it does not exist yet.
    pub(crate) async fn join_project(
        &self,
        bus: &Bus,
        session: &Session,
        topic: String,
    ) -> Result<(), Error> {
        let record = TopicRecord {
            workspace: WORKSPACE.to_owned(),
            name: topic.clone(),
            description: String::new(),
            created_by: session.agent.to_string(),
            created_at: now_ts(),
            archived: false,
        };
        match bus.create_topic(&record).await {
            Ok(()) => info!(topic, agent = %session.agent, "project topic created"),
            Err(BusError::Exists) => {}
            Err(err) => return Err(nats_error("creating the project topic", &err)),
        }
        self.join(bus, session, &topic).await?;
        if let Err(err) = bus
            .set_project_topic(WORKSPACE, &session.agent, &topic)
            .await
        {
            warn!(agent = %session.agent, error = %err, "recording the project topic failed");
        }
        *session.project_topic.lock().expect("project lock poisoned") = Some(topic);
        Ok(())
    }

    /// Adds `topic` to the agent's persistent set from the current stream position.
    async fn join(&self, bus: &Bus, session: &Session, topic: &str) -> Result<JoinStatus, Error> {
        let mut subs = session.subscriptions.lock().await;
        Self::adopt(bus, session, &mut subs)
            .await
            .map_err(|err| nats_error("reading the agent's subscriptions", &err))?;
        if subs.topics.iter().any(|s| s.pattern == topic) {
            if let Err(err) = self.apply_filters(bus, session, &subs).await {
                warn!(agent = %session.agent, error = %err, "consumer update deferred");
            }
            return Ok(JoinStatus::AlreadySubscribed);
        }
        // Filter first, boundary second: everything after the boundary is admitted.
        let mut wanted = subs.topics.clone();
        wanted.push(PersistentSubscription {
            pattern: topic.to_owned(),
            start_seq: None,
            created_at: now_ts(),
        });
        bus.ensure_consumer(WORKSPACE, &session.agent, filters(&session.agent, &wanted))
            .await
            .map_err(|err| nats_error("activating the topic filter", &err))?;
        self.forget_reach();
        let start_seq = bus
            .last_sequence()
            .await
            .map_err(|err| nats_error("reading the stream position", &err))?;
        let item = PersistentSubscription {
            pattern: topic.to_owned(),
            start_seq: Some(start_seq),
            created_at: now_ts(),
        };
        let written = bus
            .update_subscriptions(
                WORKSPACE,
                &session.agent,
                |items| {
                    // An existing subscription keeps its original boundary.
                    if !items.iter().any(|x| x.pattern == item.pattern) {
                        items.push(item.clone());
                    }
                },
                || session.may_consume(),
            )
            .await;
        match written {
            Ok(set) => {
                subs.topics = set.subscriptions;
                subs.unsettled = false;
                info!(agent = %session.agent, topic, start_seq, "subscribed");
                Ok(JoinStatus::Subscribed)
            }
            Err(BusError::NoAuthority) => Err(Error::new(
                ErrorCode::LeasePending,
                "cannot change subscriptions: this session has not confirmed ownership of its \
                 agent id. Retry shortly.",
            )),
            Err(err @ BusError::Contended(_)) => {
                Err(Error::new(ErrorCode::Internal, err.to_string()))
            }
            Err(err) => {
                subs.unsettled = true;
                warn!(agent = %session.agent, topic, error = %err, "join outcome unknown");
                Err(Error::new(
                    ErrorCode::NatsUnavailable,
                    "join outcome unknown: it may have committed. Retry the subscribe to reconcile.",
                ))
            }
        }
    }

    fn check_may_change(session: &Session) -> Result<(), Error> {
        Self::check_session(session)?;
        if !session.may_send() {
            return Err(Error::new(
                ErrorCode::LeasePending,
                "cannot change subscriptions: this session has not confirmed ownership of its \
                 agent id (NATS unreachable). Retry once connected.",
            ));
        }
        Ok(())
    }

    pub(crate) async fn topic_create(
        &self,
        session: &Session,
        params: TopicCreate,
    ) -> Result<Value, Error> {
        Self::check_session(session)?;
        validate_topic(&params.name)?;
        if params.description.chars().count() > DESCRIPTION_MAX_CHARS {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                format!("description must be at most {DESCRIPTION_MAX_CHARS} characters"),
            ));
        }
        let bus = self.require_bus("creating a topic")?;
        let record = TopicRecord {
            workspace: WORKSPACE.to_owned(),
            name: params.name.clone(),
            description: params.description,
            created_by: session.agent.to_string(),
            created_at: now_ts(),
            archived: false,
        };
        match bus.create_topic(&record).await {
            Ok(()) => {
                info!(topic = params.name, agent = %session.agent, "topic created");
                Ok(serde_json::json!({ "name": params.name, "status": "created" }))
            }
            Err(BusError::Exists) => Err(Error::new(
                ErrorCode::TopicExists,
                format!("topic {:?} already exists", params.name),
            )),
            Err(err) => Err(nats_error("creating a topic", &err)),
        }
    }

    pub(crate) async fn topic_list(
        &self,
        session: &Session,
        params: TopicList,
    ) -> Result<Value, Error> {
        Self::check_session(session)?;
        if let Some(prefix) = &params.prefix {
            validate_topic(prefix)?;
        }
        let bus = self.require_bus("listing topics")?;
        let records = bus
            .list_topics(WORKSPACE)
            .await
            .map_err(|err| nats_error("listing topics", &err))?;
        let subs = session.subscriptions.lock().await;
        let topics: Vec<TopicView> = records
            .into_iter()
            .filter(|t| {
                params.prefix.as_deref().is_none_or(|p| {
                    t.name == p || t.name.strip_prefix(p).is_some_and(|r| r.starts_with('.'))
                })
            })
            .filter(|t| params.include_archived || !t.archived)
            .map(|t| TopicView {
                subscribed: subs.topics.iter().any(|s| s.pattern == t.name),
                name: t.name,
                description: t.description,
                archived: t.archived,
            })
            .collect();
        Ok(serde_json::json!({ "topics": topics }))
    }

    pub(crate) async fn topic_subscribe(
        &self,
        session: &Session,
        params: TopicRef,
    ) -> Result<Value, Error> {
        Self::check_may_change(session)?;
        let bus = self.require_bus("subscribing")?.clone();
        self.require_topic(&bus, &params.topic).await?;
        let status = self.join(&bus, session, &params.topic).await?;
        Ok(serde_json::json!({ "topic": params.topic, "status": status }))
    }

    pub(crate) async fn topic_unsubscribe(
        &self,
        session: &Session,
        params: TopicRef,
    ) -> Result<Value, Error> {
        Self::check_may_change(session)?;
        validate_topic(&params.topic)?;
        let bus = self.require_bus("unsubscribing")?.clone();
        let mut subs = session.subscriptions.lock().await;
        Self::adopt(&bus, session, &mut subs)
            .await
            .map_err(|err| nats_error("reading the agent's subscriptions", &err))?;
        if !subs.topics.iter().any(|s| s.pattern == params.topic) {
            return Err(Error::new(
                ErrorCode::NotSubscribed,
                format!(
                    "no subscription to {:?}; see topic_subscriptions",
                    params.topic
                ),
            ));
        }
        let set = bus
            .update_subscriptions(
                WORKSPACE,
                &session.agent,
                |items| items.retain(|x| x.pattern != params.topic),
                || session.may_consume(),
            )
            .await
            .map_err(|err| nats_error("unsubscribing", &err))?;
        subs.topics = set.subscriptions;
        if let Err(err) = self.apply_filters(&bus, session, &subs).await {
            // The receiver drops the topic's messages anyway; the next change retries the filter.
            warn!(agent = %session.agent, error = %err, "consumer update deferred");
        }
        info!(agent = %session.agent, topic = params.topic, "unsubscribed");
        Ok(serde_json::json!({ "topic": params.topic, "status": "unsubscribed" }))
    }

    pub(crate) async fn topic_subscriptions(&self, session: &Session) -> Result<Value, Error> {
        Self::check_session(session)?;
        let subs = session.subscriptions.lock().await;
        let topics: Vec<&str> = subs.topics.iter().map(|s| s.pattern.as_str()).collect();
        let project = session
            .project_topic
            .lock()
            .expect("project lock poisoned")
            .clone();
        let mut out = serde_json::json!({ "topics": topics, "dm": session.agent });
        if let Some(project) = project {
            out["project_topic"] = Value::String(project);
        }
        Ok(out)
    }
}

/// The project topic of a directory that is in no configured project and no git checkout.
const SCRATCH_TOPIC: &str = "scratch";

fn real(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_owned())
}

/// A linked worktree (`.git` is a file `gitdir: <main>/.git/worktrees/<name>`) belongs to its
/// main checkout.
fn main_checkout(checkout: &Path) -> PathBuf {
    let marker = checkout.join(".git");
    let Ok(line) = std::fs::read_to_string(&marker) else {
        return checkout.to_owned(); // a directory: this is the main checkout
    };
    let Some(gitdir) = line.trim().strip_prefix("gitdir:") else {
        return checkout.to_owned();
    };
    let gitdir = real(&checkout.join(gitdir.trim()));
    match (gitdir.parent(), gitdir.parent().and_then(Path::parent)) {
        (Some(worktrees), Some(dot_git))
            if worktrees.file_name() == Some("worktrees".as_ref())
                && dot_git.file_name() == Some(".git".as_ref()) =>
        {
            dot_git
                .parent()
                .map_or_else(|| checkout.to_owned(), Path::to_owned)
        }
        _ => checkout.to_owned(),
    }
}

/// A directory name as a topic: lowercased, runs of other characters as `-`.
fn topic_name(dir: &Path) -> Option<String> {
    let lower = dir.file_name()?.to_string_lossy().to_lowercase();
    // Python's `re.sub(r"[^a-z0-9_-]+", "-", name)`: each run of other characters becomes one `-`.
    let mut name = String::with_capacity(lower.len());
    let mut in_run = false;
    for c in lower.chars() {
        let kept = c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-';
        if kept {
            name.push(c);
        } else if !in_run {
            name.push('-');
        }
        in_run = !kept;
    }
    let name = name.trim_matches('-').to_owned();
    xagent_proto::is_valid_id(&name).then_some(name)
}
