//! Topic messages, replies, threads, topic history, and the reach of a send.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Serialize;
use serde_json::Value;
use tracing::warn;
use xagent_proto::{
    AgentId, Channel, Content, Error, ErrorCode, Message, Sender, WORKSPACE, chat_subject,
    consumer_name, subject_matches, validate_topic,
};
use xagent_store::{InboxItem, ReadFilter};

use crate::api::{MessageView, SendView, SummaryView, to_value};
use crate::daemon::{Daemon, internal};
use crate::outbox::SendStatus;
use crate::protocol::{MessageReply, MessageSend, ThreadRead, TopicHistory};
use crate::session::Session;

/// How long the stream's consumer list is reused for reach counts. This host's own consumer
/// changes drop it at once; another host's show up within this time.
const REACH_CACHE_TTL: Duration = Duration::from_secs(20);
const THREAD_DEFAULT: u32 = 20;
const THREAD_MAX: u32 = 100;
const HISTORY_DEFAULT: u32 = 20;
const HISTORY_MAX: u32 = 100;

/// `(consumer name, filter subjects)` of every durable consumer, and when it was read.
#[derive(Debug)]
pub(crate) struct ReachCache {
    read_at: Instant,
    consumers: Arc<Vec<(String, Vec<String>)>>,
}

const QUEUED_NOTE: &str = "NATS unreachable: stored locally and retried with the same id while \
                           this session stays open. Not yet delivered; dropped if the session \
                           ends first.";
const NOBODY_CHAT: &str = "nobody receives this: no agent on any host subscribes to it. Check \
                           topic_list and agent_list, or dm_send the agent who needs it.";
const NOBODY_DM: &str = "nobody receives this yet: the recipient has no inbox consumer. It will \
                         be delivered if that identity starts.";

fn bounded(limit: Option<u32>, default: u32, max: u32) -> Result<usize, Error> {
    let n = match limit {
        None => default,
        Some(0) => {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "limit must be a positive integer",
            ));
        }
        Some(n) => n.min(max),
    };
    Ok(usize::try_from(n).expect("limit fits usize"))
}

fn content(
    kind: xagent_proto::Kind,
    priority: xagent_proto::Priority,
    summary: String,
    body: String,
    mentions: Vec<AgentId>,
    refs: Vec<xagent_proto::Reference>,
) -> Content {
    Content {
        kind,
        priority,
        summary,
        body,
        mentions,
        refs,
    }
}

/// A history entry: the inbox header, plus the body when asked for.
#[derive(Debug, Serialize)]
struct HistoryItem {
    #[serde(flatten)]
    header: SummaryView,
    #[serde(skip_serializing_if = "Option::is_none")]
    body: Option<String>,
}

impl HistoryItem {
    fn new(msg: Message, include_body: bool) -> Self {
        let item = InboxItem {
            id: msg.id.to_string(),
            channel: msg.channel,
            topic: msg.topic,
            recipient: msg.recipient.map(String::from),
            sender: msg.sender.agent_id.to_string(),
            sent_at: msg.sent_at,
            kind: msg.kind.as_str().to_owned(),
            priority: msg.priority.as_str().to_owned(),
            summary: msg.summary,
            body_bytes: msg.body.len() as u64,
            thread_id: msg.thread_id.to_string(),
            reply_to: msg.reply_to.map(|id| id.to_string()),
            read: false,
        };
        Self {
            header: SummaryView::new(item, ReadFilter::UnreadOnly),
            body: include_body.then_some(msg.body),
        }
    }
}

impl Daemon {
    fn sender(session: &Session) -> Sender {
        Sender {
            agent_id: session.agent.clone(),
            instance_id: session.instance_id,
            client: session.client.as_str().to_owned(),
        }
    }

    fn check_send_allowed(session: &Session) -> Result<(), Error> {
        Self::check_session(session)?;
        if !session.may_send() {
            return Err(Error::new(
                ErrorCode::LeasePending,
                "cannot send: this session has not confirmed ownership of its agent id (NATS \
                 unreachable). Reads still work; retry once connected.",
            ));
        }
        Ok(())
    }

    /// The recipient must have started a session at least once.
    pub(crate) async fn require_agent(&self, agent: &AgentId) -> Result<(), Error> {
        let bus = self.require_bus("checking the recipient")?;
        let registered = bus.agent(WORKSPACE, agent).await.map_err(|err| {
            warn!(error = %err, "agent registry read failed");
            Error::new(
                ErrorCode::NatsUnavailable,
                "cannot verify recipient: NATS registry unavailable",
            )
        })?;
        if registered.is_none() {
            return Err(Error::new(
                ErrorCode::AgentNotFound,
                format!(
                    "agent {agent} is not registered. It must have started a session at least once."
                ),
            ));
        }
        Ok(())
    }

    /// A topic that exists and takes posts.
    async fn open_topic(&self, topic: &str) -> Result<(), Error> {
        let bus = self.require_bus("checking the topic")?.clone();
        let record = self.require_topic(&bus, topic).await?;
        if record.archived {
            return Err(Error::new(
                ErrorCode::TopicArchived,
                format!("topic {topic:?} is archived"),
            ));
        }
        Ok(())
    }

    /// Drops the cached consumer list (this host changed a consumer).
    pub(crate) fn forget_reach(&self) {
        *self.reach_cache.lock().expect("reach lock poisoned") = None;
    }

    /// How many durable consumers other than `sender`'s the broker delivers `subject` to; `None`
    /// when the consumer list cannot be read.
    async fn reach(&self, subject: &str, sender: &AgentId) -> Option<usize> {
        let cached = {
            let cache = self.reach_cache.lock().expect("reach lock poisoned");
            cache
                .as_ref()
                .filter(|c| c.read_at.elapsed() < REACH_CACHE_TTL)
                .map(|c| Arc::clone(&c.consumers))
        };
        let consumers = if let Some(consumers) = cached {
            consumers
        } else {
            let bus = self.bus()?;
            match bus.consumer_filters().await {
                Ok(list) => {
                    let list = Arc::new(list);
                    *self.reach_cache.lock().expect("reach lock poisoned") = Some(ReachCache {
                        read_at: Instant::now(),
                        consumers: Arc::clone(&list),
                    });
                    list
                }
                Err(err) => {
                    warn!(error = %err, "listing consumers for reach failed");
                    return None;
                }
            }
        };
        let own = consumer_name(WORKSPACE, sender);
        Some(
            consumers
                .iter()
                .filter(|(name, _)| *name != own)
                .filter(|(_, filters)| filters.iter().any(|f| subject_matches(f, subject)))
                .count(),
        )
    }

    /// Stores, publishes and reports one outbound message.
    pub(crate) async fn deliver(&self, msg: &Message) -> Result<Value, Error> {
        let (status, error) = self.send(msg).await?;
        let receivers = self.reach(&msg.subject(), &msg.sender.agent_id).await;
        let mut warnings = Vec::new();
        if status == SendStatus::Published && receivers == Some(0) {
            warnings.push(match msg.channel {
                Channel::Chat => NOBODY_CHAT,
                Channel::Dm => NOBODY_DM,
            });
        }
        Ok(to_value(&SendView {
            status,
            id: msg.id.to_string(),
            thread_id: msg.thread_id.to_string(),
            topic: msg.topic.clone(),
            recipient: msg.recipient.as_ref().map(ToString::to_string),
            receivers,
            warnings,
            note: (status == SendStatus::QueuedForRetry).then_some(QUEUED_NOTE),
            error,
        }))
    }

    pub(crate) async fn message_send(
        &self,
        session: &Session,
        params: MessageSend,
    ) -> Result<Value, Error> {
        Self::check_send_allowed(session)?;
        self.open_topic(&params.topic).await?;
        let msg = Message::new_chat(
            Self::sender(session),
            params.topic,
            content(
                params.kind,
                params.priority,
                params.summary,
                params.body,
                params.mentions,
                params.refs,
            ),
        );
        self.deliver(&msg).await
    }

    /// Replies in the parent's thread: to its topic, or to the other party of a DM.
    pub(crate) async fn message_reply(
        &self,
        session: &Session,
        params: MessageReply,
    ) -> Result<Value, Error> {
        Self::check_send_allowed(session)?;
        let id = params.message_id.trim().to_lowercase();
        let parent = self
            .store()
            .visible_messages(&session.agent, std::slice::from_ref(&id))
            .map_err(|err| internal(&err))?
            .remove(&id)
            .and_then(|raw| Message::decode(raw.as_bytes()).ok())
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::MessageNotFound,
                    "message not found in this agent's inbox or topic history",
                )
            })?;
        let body = content(
            params.kind,
            params.priority,
            params.summary,
            params.body,
            params.mentions,
            params.refs,
        );
        let msg = match (&parent.channel, &parent.topic, &parent.recipient) {
            (Channel::Chat, Some(topic), _) => {
                self.open_topic(topic).await?;
                Message::new_chat(Self::sender(session), topic.clone(), body)
            }
            (Channel::Dm, _, Some(recipient)) => {
                let other = if parent.sender.agent_id == session.agent {
                    recipient.clone()
                } else {
                    parent.sender.agent_id.clone()
                };
                self.require_agent(&other).await?;
                Message::new_dm(Self::sender(session), other, body)
            }
            _ => unreachable!("validated messages carry a topic or a recipient"),
        }
        .in_reply_to(&parent);
        self.deliver(&msg).await
    }

    pub(crate) fn thread_read(
        &self,
        session: &Session,
        params: &ThreadRead,
    ) -> Result<Value, Error> {
        Self::check_session(session)?;
        let limit = bounded(params.limit, THREAD_DEFAULT, THREAD_MAX)?;
        let thread_id = params.thread_id.trim().to_lowercase();
        let store = self.store();
        let mut raw = store
            .thread_messages(&session.agent, &thread_id, limit + 1)
            .map_err(|err| internal(&err))?;
        if raw.is_empty() {
            return Err(Error::new(
                ErrorCode::MessageNotFound,
                "thread not found locally",
            ));
        }
        let more = raw.len() > limit;
        raw.truncate(limit);
        let messages: Vec<Message> = raw
            .iter()
            .filter_map(|r| Message::decode(r.as_bytes()).ok())
            .collect();
        if params.mark_read {
            let ids: Vec<String> = messages.iter().map(|m| m.id.to_string()).collect();
            store
                .mark_read(&session.agent, &ids)
                .map_err(|err| internal(&err))?;
        }
        let views: Vec<MessageView> = messages.into_iter().map(MessageView::from).collect();
        let mut out = serde_json::json!({ "thread_id": thread_id, "messages": views });
        if more {
            out["more"] = Value::Bool(true);
        }
        Ok(out)
    }

    /// The newest messages of a topic, from JetStream (cached locally so they can be read and
    /// replied to, but not added to the inbox), or from the local cache without NATS.
    pub(crate) async fn topic_history(
        &self,
        session: &Session,
        params: TopicHistory,
    ) -> Result<Value, Error> {
        Self::check_session(session)?;
        validate_topic(&params.topic)?;
        let limit = bounded(params.limit, HISTORY_DEFAULT, HISTORY_MAX)?;
        let streamed = match self.bus().cloned() {
            Some(bus) => {
                self.require_topic(&bus, &params.topic).await?;
                let subject = chat_subject(WORKSPACE, &params.topic);
                // Boxed: the binary search's future is large.
                match Box::pin(bus.subject_tail(&subject, limit)).await {
                    Ok(scanned) => Some(
                        scanned
                            .into_iter()
                            .filter_map(|s| self.cache_scanned(&subject, &s.payload, s.sequence))
                            .collect::<Vec<_>>(),
                    ),
                    Err(err) => {
                        warn!(topic = params.topic, error = %err,
                              "stream history failed; using the local cache");
                        None
                    }
                }
            }
            None => None,
        };
        let source_local = streamed.is_none();
        let messages = match streamed {
            Some(messages) => messages,
            None => self
                .store()
                .local_topic_history(&params.topic, limit)
                .map_err(|err| internal(&err))?
                .iter()
                .filter_map(|raw| Message::decode(raw.as_bytes()).ok())
                .collect(),
        };
        let items: Vec<HistoryItem> = messages
            .into_iter()
            .map(|m| HistoryItem::new(m, params.include_body))
            .collect();
        let mut out = serde_json::json!({ "topic": params.topic, "messages": items });
        if source_local {
            out["source"] = Value::String("local_cache".to_owned());
        }
        Ok(out)
    }

    /// Validates a message read from the stream and caches it; `None` if it is not valid there.
    fn cache_scanned(&self, subject: &str, payload: &[u8], sequence: u64) -> Option<Message> {
        let msg = Message::decode(payload).ok()?;
        if msg.subject() != subject {
            return None;
        }
        let raw = std::str::from_utf8(payload).ok()?;
        if let Err(err) = self.store().cache_message(&msg, raw, sequence) {
            warn!(message_id = %msg.id, error = %err, "history message not cached");
            return None;
        }
        Some(msg)
    }
}
