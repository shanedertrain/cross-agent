//! The local IPC protocol between the daemon and its clients (the CLI, hooks and the MCP server).
//!
//! One JSON object per line over a Unix socket:
//!
//! ```text
//! request   {"id":1,"method":"session_open","params":{"conversation":"...","client":"codex"}}
//! response  {"id":1,"ok":true,"result":{...}}
//!           {"id":1,"ok":false,"error":{"code":"...","message":"..."}}
//! ```
//!
//! A connection's `session_open` binds it to one conversation identity; the connection then is
//! the session, and closing it ends the session (and releases its lease).

use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use xagent_proto::{AgentId, Channel, Client, Conversation, Error, Kind, Priority, Reference};

/// Longest accepted request or response line, in bytes.
pub const MAX_LINE_BYTES: usize = 1 << 20;

/// One request line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Request {
    #[serde(default)]
    pub id: u64,
    #[serde(flatten)]
    pub call: Call,
}

/// A method and its parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "method", content = "params", rename_all = "snake_case")]
pub enum Call {
    /// Liveness check; no session needed.
    Ping,
    /// Daemon health; no session needed.
    Status,
    /// Declare this connection a client window (an MCP server of a running conversation). Until
    /// it binds, the daemon wakes the window when messages wait for the identity it would bind.
    WindowOpen(WindowOpen),
    /// Bind this connection to a conversation identity, acquiring its lease.
    SessionOpen(SessionOpen),
    Whoami,
    InboxStatus,
    InboxList(InboxList),
    MessageRead(MessageRead),
    DmSend(DmSend),
    /// For the Stop hook of a client: whether the conversation should read its inbox before
    /// stopping. Acts on another process's session, so it needs none of its own.
    StopCheck(StopCheck),
    /// For the Claude `SendMessage` hook: log the send on the bus as the conversation's session.
    SendmessageMirror(SendmessageMirror),
    /// Update what peers see of the bound identity (its display name).
    SessionUpdate(SessionUpdate),
    AgentList(AgentList),
    TopicCreate(TopicCreate),
    TopicList(TopicList),
    TopicSubscribe(TopicRef),
    TopicUnsubscribe(TopicRef),
    TopicSubscriptions,
    TopicHistory(TopicHistory),
    MessageSend(MessageSend),
    MessageReply(MessageReply),
    ThreadRead(ThreadRead),
    /// Local retention pass; no session needed.
    Gc(Gc),
    /// Rebuild inboxes from JetStream; no session needed.
    Recover(Recover),
}

impl Call {
    /// Whether the method acts as the connection's bound identity.
    pub fn needs_session(&self) -> bool {
        !matches!(
            self,
            Self::Ping
                | Self::Status
                | Self::WindowOpen(_)
                | Self::SessionOpen(_)
                | Self::StopCheck(_)
                | Self::SendmessageMirror(_)
                | Self::Gc(_)
                | Self::Recover(_)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionOpen {
    pub conversation: Conversation,
    pub client: Client,
    /// The client's working directory: inside a configured project, its root topic is joined.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowOpen {
    pub client: Client,
    /// The client's working directory (absolute).
    pub cwd: PathBuf,
    /// The conversation named by the launch environment: dsh's session id, Claude's session id
    /// at launch (the live registry record, found by `claude_pid`, wins). Codex names none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation: Option<Conversation>,
    /// The Claude Code process the window belongs to.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claude_pid: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StopCheck {
    pub conversation: Conversation,
    /// The clients whose sessions this hook can belong to (`claude-stop` also serves dsh).
    pub clients: Vec<Client>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendmessageMirror {
    pub conversation: Conversation,
    pub to: String,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxList {
    #[serde(default = "yes")]
    pub unread_only: bool,
    #[serde(default)]
    pub limit: Option<u32>,
    /// Only messages of this topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Only topic messages (`chat`) or only DMs (`dm`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<Channel>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageRead {
    pub ids: Vec<String>,
    #[serde(default = "yes")]
    pub mark_read: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DmSend {
    pub recipient: AgentId,
    pub summary: String,
    #[serde(default)]
    pub body: String,
    #[serde(default = "message_kind")]
    pub kind: Kind,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub mentions: Vec<AgentId>,
    #[serde(default)]
    pub refs: Vec<Reference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionUpdate {
    /// The client's name for the conversation (Claude Code's session name is its `SendMessage`
    /// address); at most 128 characters, empty to clear.
    pub display_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentList {
    #[serde(default)]
    pub online_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicCreate {
    pub name: String,
    #[serde(default)]
    pub description: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicList {
    /// Only this topic and its children.
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub include_archived: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicRef {
    pub topic: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicHistory {
    pub topic: String,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default)]
    pub include_body: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageSend {
    pub topic: String,
    pub summary: String,
    #[serde(default)]
    pub body: String,
    #[serde(default = "message_kind")]
    pub kind: Kind,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub mentions: Vec<AgentId>,
    #[serde(default)]
    pub refs: Vec<Reference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MessageReply {
    pub message_id: String,
    pub summary: String,
    #[serde(default)]
    pub body: String,
    #[serde(default = "message_kind")]
    pub kind: Kind,
    #[serde(default)]
    pub priority: Priority,
    #[serde(default)]
    pub mentions: Vec<AgentId>,
    #[serde(default)]
    pub refs: Vec<Reference>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThreadRead {
    pub thread_id: String,
    #[serde(default)]
    pub limit: Option<u32>,
    #[serde(default = "yes")]
    pub mark_read: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Gc {
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Recover {
    /// The window to re-read, in hours (1 to the stream's 7-day retention).
    pub hours: u32,
    /// Agents to recover; empty means every agent last registered from this host.
    #[serde(default)]
    pub agents: Vec<AgentId>,
}

fn yes() -> bool {
    true
}

fn message_kind() -> Kind {
    Kind::Message
}

/// One response line.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub id: u64,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
}

impl Response {
    pub fn new(id: u64, outcome: Result<serde_json::Value, Error>) -> Self {
        match outcome {
            Ok(result) => Self {
                id,
                ok: true,
                result: Some(result),
                error: None,
            },
            Err(error) => Self {
                id,
                ok: false,
                result: None,
                error: Some(error),
            },
        }
    }

    /// The result, or the error the daemon returned.
    pub fn into_result(self) -> Result<serde_json::Value, Error> {
        match (self.ok, self.result, self.error) {
            (true, result, _) => Ok(result.unwrap_or(serde_json::Value::Null)),
            (false, _, Some(error)) => Err(error),
            (false, _, None) => Err(Error::new(
                xagent_proto::ErrorCode::Internal,
                "error response without an error",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_parse_with_and_without_params() {
        let ping: Request = serde_json::from_str(r#"{"id":3,"method":"ping"}"#).expect("ping");
        assert_eq!(ping.call, Call::Ping);
        let list: Request =
            serde_json::from_str(r#"{"id":4,"method":"inbox_list","params":{}}"#).expect("list");
        assert_eq!(
            list.call,
            Call::InboxList(InboxList {
                unread_only: true,
                limit: None,
                topic: None,
                channel: None,
            })
        );
        let open: Request = serde_json::from_str(
            r#"{"id":1,"method":"session_open","params":{"conversation":"c-1","client":"claude"}}"#,
        )
        .expect("open");
        let Call::SessionOpen(open) = open.call else {
            panic!("not session_open: {open:?}");
        };
        assert_eq!(open.client, Client::ClaudeCode);
        assert!(
            serde_json::from_str::<Request>(r#"{"id":1,"method":"inbox_list","params":{"x":1}}"#)
                .is_err()
        );
        assert!(serde_json::from_str::<Request>(r#"{"id":1,"method":"nope"}"#).is_err());
    }
}
