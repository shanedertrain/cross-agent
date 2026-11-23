//! Method handlers and the views they return.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;
use xagent_proto::{
    Content, Error, ErrorCode, Kind, Message, Priority, Reference, Sender, WORKSPACE,
    validate_topic,
};
use xagent_store::{InboxItem, ReadFilter};

use crate::daemon::{Daemon, Readiness, internal};
use crate::outbox::SendStatus;
use crate::protocol::{DmSend, InboxList, MessageRead};
use crate::session::Session;

const INBOX_LIST_DEFAULT: u32 = 10;
const INBOX_LIST_MAX: u32 = 50;
const READ_MAX_IDS: usize = 20;
/// A session younger than this waits for messages already waiting in its consumer before the
/// first inbox answer, so a fresh session's first check is not falsely empty.
const SETTLE_WINDOW: Duration = Duration::from_secs(15);
const SETTLE_TIMEOUT: Duration = Duration::from_secs(3);
const SETTLE_POLL: Duration = Duration::from_millis(100);

pub(crate) fn to_value(view: &impl Serialize) -> Value {
    serde_json::to_value(view).expect("views serialize to JSON")
}

#[derive(Debug, Serialize)]
struct InboxStatusView {
    unread_total: u64,
    dm_unread: u64,
    high_priority: u64,
    urgent: u64,
    by_topic: BTreeMap<String, u64>,
    awaiting_reply: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    outbox_pending: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    outbox_failed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nats: Option<&'static str>,
}

/// An inbox header: never the body.
#[derive(Debug, Serialize)]
pub(crate) struct SummaryView {
    id: String,
    channel: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    recipient: Option<String>,
    sender: String,
    sent_at: String,
    kind: String,
    priority: String,
    summary: String,
    body_bytes: u64,
    thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    read: Option<bool>,
}

impl SummaryView {
    pub(crate) fn new(item: InboxItem, filter: ReadFilter) -> Self {
        Self {
            id: item.id,
            channel: item.channel.as_str(),
            topic: item.topic,
            recipient: item.recipient,
            sender: item.sender,
            sent_at: item.sent_at,
            kind: Kind::from(item.kind.as_str()).display().to_owned(),
            priority: item.priority,
            summary: item.summary,
            body_bytes: item.body_bytes,
            thread_id: item.thread_id,
            reply_to: item.reply_to,
            read: (filter == ReadFilter::All).then_some(item.read),
        }
    }
}

/// The compact message view: what a reader needs, without transport details.
#[derive(Debug, Serialize)]
pub(crate) struct MessageView {
    id: String,
    from: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<String>,
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<Priority>,
    sent_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    thread_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_to: Option<String>,
    summary: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    body: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    mentions: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    refs: Vec<RefView>,
}

#[derive(Debug, Serialize)]
struct RefView {
    #[serde(rename = "type")]
    kind: Value,
    value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<String>,
}

impl From<Reference> for RefView {
    fn from(r: Reference) -> Self {
        Self {
            kind: serde_json::to_value(&r.kind).expect("ref type serializes"),
            value: r.value,
            label: r.label,
        }
    }
}

impl From<Message> for MessageView {
    fn from(m: Message) -> Self {
        Self {
            id: m.id.to_string(),
            from: m.sender.agent_id.to_string(),
            to: m.recipient.map(|r| r.to_string()),
            topic: m.topic,
            kind: m.kind.display().to_owned(),
            priority: (m.priority != Priority::Normal).then_some(m.priority),
            sent_at: m.sent_at,
            thread_id: (m.thread_id != m.id).then(|| m.thread_id.to_string()),
            reply_to: m.reply_to.map(|r| r.to_string()),
            summary: m.summary,
            body: m.body,
            mentions: m.mentions.iter().map(ToString::to_string).collect(),
            refs: m.refs.into_iter().map(RefView::from).collect(),
        }
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct SendView {
    pub(crate) status: SendStatus,
    pub(crate) id: String,
    pub(crate) thread_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) topic: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) recipient: Option<String>,
    /// Durable consumers on any host, other than the sender's, that the broker delivers this to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) receivers: Option<usize>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub(crate) warnings: Vec<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) note: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) error: Option<String>,
}

#[derive(Debug, Serialize)]
struct SessionView {
    agent_id: String,
    client: &'static str,
    /// Where the session is woken (`xagent doctor` checks these).
    endpoint: xagent_wake::Endpoint,
    instance_id: String,
    lease: &'static str,
    authority_s: f64,
    since: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease_error: Option<String>,
}

impl Daemon {
    pub(crate) fn status(&self) -> Result<Value, Error> {
        let mut host_id_taken = None;
        let (ready, jetstream) = match self.readiness() {
            Readiness::Connecting => (false, Value::Null),
            Readiness::HostIdTaken(reason) => {
                host_id_taken = Some(reason);
                (false, Value::Null)
            }
            Readiness::Ready(report) => (
                true,
                serde_json::json!({
                    "created": report.created,
                    "mismatches": report.mismatches.iter().map(ToString::to_string).collect::<Vec<_>>(),
                }),
            ),
            Readiness::Incompatible(reason) => (false, serde_json::json!({ "fatal": reason })),
        };
        let sessions: Vec<SessionView> = self
            .sessions()
            .iter()
            .map(|s| SessionView {
                agent_id: s.agent.to_string(),
                client: s.client.as_str(),
                endpoint: s.endpoint.clone(),
                instance_id: s.instance_id.to_string(),
                lease: s.phase().as_str(),
                authority_s: (s.authority_remaining().as_secs_f64() * 10.0).round() / 10.0,
                since: s.started_at.clone(),
                lease_error: s.lease_error(),
            })
            .collect();
        let outbox = self
            .store()
            .outbox_counts(None)
            .map_err(|err| internal(&err))?;
        Ok(serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "host_id": self.config.host_id,
            "run_id": self.run_id,
            "started_at": self.started_at,
            "nats": {
                "connected": self.connection.is_connected(),
                "ready": ready,
                "host_id_taken": host_id_taken,
            },
            "jetstream": jetstream,
            "state_db": self.config.state_db,
            "sessions": sessions,
            "outbox": outbox,
        }))
    }

    /// Waits (bounded) until messages already waiting in a fresh session's consumer are stored.
    async fn settle_inbox(&self, session: &Session) {
        if session.opened.elapsed() > SETTLE_WINDOW {
            return;
        }
        let deadline = tokio::time::Instant::now() + SETTLE_TIMEOUT;
        while tokio::time::Instant::now() < deadline {
            let Some(bus) = self.bus() else { return };
            match bus.consumer_backlog(WORKSPACE, &session.agent).await {
                Ok((0, 0)) | Err(_) => return,
                Ok(_) => tokio::time::sleep(SETTLE_POLL).await,
            }
        }
    }

    pub(crate) async fn inbox_status(&self, session: &Session) -> Result<Value, Error> {
        Self::check_session(session)?;
        self.settle_inbox(session).await;
        let (counts, outbox) = {
            let store = self.store();
            let counts = store
                .inbox_counts(&session.agent)
                .map_err(|err| internal(&err))?;
            let outbox = store
                .outbox_counts(Some(&session.agent))
                .map_err(|err| internal(&err))?;
            (counts, outbox)
        };
        Ok(to_value(&InboxStatusView {
            unread_total: counts.unread_total,
            dm_unread: counts.dm_unread,
            high_priority: counts.high_priority,
            urgent: counts.urgent,
            by_topic: counts.by_topic,
            awaiting_reply: counts.awaiting_reply,
            outbox_pending: outbox.get("pending").copied(),
            outbox_failed: outbox.get("failed").copied(),
            nats: self.bus().is_none().then_some("unavailable"),
        }))
    }

    pub(crate) async fn inbox_list(
        &self,
        session: &Session,
        params: InboxList,
    ) -> Result<Value, Error> {
        Self::check_session(session)?;
        let limit = match params.limit {
            None => INBOX_LIST_DEFAULT,
            Some(0) => {
                return Err(Error::new(
                    ErrorCode::InvalidArgument,
                    "limit must be a positive integer",
                ));
            }
            Some(n) => n.min(INBOX_LIST_MAX),
        };
        let filter = if params.unread_only {
            ReadFilter::UnreadOnly
        } else {
            ReadFilter::All
        };
        if let Some(topic) = &params.topic {
            validate_topic(topic)?;
        }
        self.settle_inbox(session).await;
        let limit = usize::try_from(limit).expect("limit fits usize");
        let mut items = self
            .store()
            .inbox_list(
                &session.agent,
                filter,
                params.topic.as_deref(),
                params.channel,
                limit + 1,
            )
            .map_err(|err| internal(&err))?;
        let more = items.len() > limit;
        items.truncate(limit);
        let messages: Vec<SummaryView> = items
            .into_iter()
            .map(|i| SummaryView::new(i, filter))
            .collect();
        let mut out = serde_json::json!({ "messages": messages });
        if more {
            out["more"] = Value::Bool(true);
        }
        Ok(out)
    }

    pub(crate) fn message_read(
        &self,
        session: &Session,
        params: MessageRead,
    ) -> Result<Value, Error> {
        Self::check_session(session)?;
        if params.ids.is_empty() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "ids must be a non-empty list of message ids",
            ));
        }
        if params.ids.len() > READ_MAX_IDS {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                format!("at most {READ_MAX_IDS} ids per call"),
            ));
        }
        let mut wanted: Vec<String> = Vec::with_capacity(params.ids.len());
        for id in params.ids {
            let id = id.trim().to_lowercase();
            if !wanted.contains(&id) {
                wanted.push(id);
            }
        }
        let store = self.store();
        let found = store
            .visible_messages(&session.agent, &wanted)
            .map_err(|err| internal(&err))?;
        let mut messages = Vec::new();
        let mut not_found = Vec::new();
        let mut read = Vec::new();
        for id in wanted {
            match found.get(&id).map(|raw| Message::decode(raw.as_bytes())) {
                Some(Ok(msg)) => {
                    messages.push(MessageView::from(msg));
                    read.push(id);
                }
                Some(Err(_)) | None => not_found.push(id),
            }
        }
        if params.mark_read {
            store
                .mark_read(&session.agent, &read)
                .map_err(|err| internal(&err))?;
        }
        let mut out = serde_json::json!({ "messages": messages });
        if !not_found.is_empty() {
            out["not_found"] = serde_json::json!(not_found);
        }
        Ok(out)
    }

    pub(crate) async fn dm_send(&self, session: &Session, params: DmSend) -> Result<Value, Error> {
        Self::check_session(session)?;
        if !session.may_send() {
            return Err(Error::new(
                ErrorCode::LeasePending,
                "cannot send: this session has not confirmed ownership of its agent id (NATS \
                 unreachable). Reads still work; retry once connected.",
            ));
        }
        self.require_agent(&params.recipient).await?;
        let sender = Sender {
            agent_id: session.agent.clone(),
            instance_id: session.instance_id,
            client: session.client.as_str().to_owned(),
        };
        let msg = Message::new_dm(
            sender,
            params.recipient,
            Content {
                kind: params.kind,
                priority: params.priority,
                summary: params.summary,
                body: params.body,
                mentions: params.mentions,
                refs: params.refs,
            },
        );
        self.deliver(&msg).await
    }
}
