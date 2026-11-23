//! The `xagent.message.v1` envelope.
//!
//! Two validation modes exist. Strict is used for everything this host creates and publishes:
//! unknown `kind` and reference `type` values are rejected. Lenient is used on receive: such
//! values from future protocol revisions are tolerated (shown as `message` / `other`), and
//! unknown fields are ignored. The serialized form is compact JSON with non-ASCII characters
//! unescaped, fields in the order below; the size cap is measured on exactly those bytes.

use std::fmt;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::time::{is_rfc3339_with_offset, now_ts};
use crate::{AgentId, Error, ErrorCode, WORKSPACE, chat_subject, dm_subject, is_valid_id};

/// Hard cap on the serialized envelope, in bytes.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 16384;
const SUMMARY_MAX_CHARS: usize = 200;
const MAX_REFS: usize = 20;
const MAX_MENTIONS: usize = 20;
const REF_VALUE_MAX_CHARS: usize = 1024;
const REF_LABEL_MAX_CHARS: usize = 200;
const CLIENT_MAX_CHARS: usize = 32;

fn parse_canonical_uuid(value: &str) -> Result<Uuid, String> {
    let parsed = Uuid::try_parse(value).map_err(|_| format!("not a UUID: {value:?}"))?;
    if parsed.hyphenated().to_string() != value.to_ascii_lowercase() {
        return Err(format!("UUID must be canonical hyphenated form: {value:?}"));
    }
    Ok(parsed)
}

/// A message, thread or session-instance id: a UUID, written in canonical lowercase hyphenated form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct MessageId(Uuid);

impl MessageId {
    /// A fresh random id.
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }
}

impl TryFrom<String> for MessageId {
    type Error = String;

    fn try_from(value: String) -> Result<Self, String> {
        parse_canonical_uuid(&value).map(Self)
    }
}

impl std::str::FromStr for MessageId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        parse_canonical_uuid(s)
            .map(Self)
            .map_err(|m| Error::new(ErrorCode::InvalidArgument, m))
    }
}

impl From<MessageId> for String {
    fn from(id: MessageId) -> Self {
        id.to_string()
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0.hyphenated(), f)
    }
}

/// Topic message or direct message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Channel {
    Chat,
    Dm,
}

impl Channel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Chat => "chat",
            Self::Dm => "dm",
        }
    }
}

/// What a message is for. `Other` holds a kind from a future protocol revision (receive only).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Message,
    Question,
    Answer,
    Handoff,
    ReviewRequest,
    Decision,
    Status,
    Alert,
    #[serde(untagged)]
    Other(String),
}

impl Kind {
    /// The kind shown to a reader: an unknown kind reads as `message`.
    pub fn display(&self) -> &str {
        match self {
            Self::Message | Self::Other(_) => "message",
            Self::Question => "question",
            Self::Answer => "answer",
            Self::Handoff => "handoff",
            Self::ReviewRequest => "review_request",
            Self::Decision => "decision",
            Self::Status => "status",
            Self::Alert => "alert",
        }
    }

    /// The kind as stored on the wire (an unknown kind keeps its own name).
    pub fn as_str(&self) -> &str {
        match self {
            Self::Other(s) => s,
            known => known.display(),
        }
    }
}

impl From<&str> for Kind {
    /// A stored kind name: a known kind, or [`Kind::Other`].
    fn from(name: &str) -> Self {
        match name {
            "message" => Self::Message,
            "question" => Self::Question,
            "answer" => Self::Answer,
            "handoff" => Self::Handoff,
            "review_request" => Self::ReviewRequest,
            "decision" => Self::Decision,
            "status" => Self::Status,
            "alert" => Self::Alert,
            other => Self::Other(other.to_owned()),
        }
    }
}

/// Delivery urgency (strict on receive too).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    #[default]
    Normal,
    High,
    Urgent,
}

impl Priority {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Normal => "normal",
            Self::High => "high",
            Self::Urgent => "urgent",
        }
    }
}

/// The type of a reference. `Other` holds a type from a future protocol revision (receive only);
/// the known `other` type is [`RefType::OtherRef`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefType {
    GitCommit,
    GitBranch,
    Path,
    Url,
    Artifact,
    Message,
    #[serde(rename = "other")]
    OtherRef,
    #[serde(untagged)]
    Other(String),
}

/// A pointer to something outside the message: a commit, branch, path, URL, artifact or message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Reference {
    #[serde(rename = "type")]
    pub kind: RefType,
    pub value: String,
    #[serde(default)]
    pub label: Option<String>,
}

/// Who sent a message: set by the sending daemon from the session.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sender {
    pub agent_id: AgentId,
    pub instance_id: MessageId,
    pub client: String,
}

/// The model-supplied content of an outbound message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Content {
    pub kind: Kind,
    pub priority: Priority,
    pub summary: String,
    pub body: String,
    pub mentions: Vec<AgentId>,
    pub refs: Vec<Reference>,
}

/// The `xagent.message.v1` envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "schema", rename = "xagent.message.v1")]
pub struct Message {
    pub id: MessageId,
    /// The destination workspace; it is what the subject encodes.
    pub workspace: String,
    pub channel: Channel,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub recipient: Option<AgentId>,
    pub kind: Kind,
    pub sender: Sender,
    pub sent_at: String,
    pub thread_id: MessageId,
    #[serde(default)]
    pub reply_to: Option<MessageId>,
    #[serde(default)]
    pub priority: Priority,
    pub summary: String,
    pub body: String,
    #[serde(default)]
    pub mentions: Vec<AgentId>,
    pub refs: Vec<Reference>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Strictness {
    Strict,
    Lenient,
}

/// `^[a-z][a-z0-9_]{0,31}$`: the shape of a future enum value tolerated on receive.
fn is_future_enum(value: &str) -> bool {
    let bytes = value.as_bytes();
    let Some((&first, rest)) = bytes.split_first() else {
        return false;
    };
    bytes.len() <= 32
        && first.is_ascii_lowercase()
        && rest
            .iter()
            .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::InvalidMessage, message)
}

impl Message {
    /// A new direct message from `sender` to `recipient`, starting a new thread.
    pub fn new_dm(sender: Sender, recipient: AgentId, content: Content) -> Self {
        let id = MessageId::new_v4();
        Self {
            id,
            workspace: WORKSPACE.to_owned(),
            channel: Channel::Dm,
            topic: None,
            recipient: Some(recipient),
            kind: content.kind,
            sender,
            sent_at: now_ts(),
            thread_id: id,
            reply_to: None,
            priority: content.priority,
            summary: content.summary,
            body: content.body,
            mentions: content.mentions,
            refs: content.refs,
        }
    }

    /// A new topic message from `sender` to `topic`, starting a new thread.
    pub fn new_chat(sender: Sender, topic: String, content: Content) -> Self {
        let id = MessageId::new_v4();
        Self {
            id,
            workspace: WORKSPACE.to_owned(),
            channel: Channel::Chat,
            topic: Some(topic),
            recipient: None,
            kind: content.kind,
            sender,
            sent_at: now_ts(),
            thread_id: id,
            reply_to: None,
            priority: content.priority,
            summary: content.summary,
            body: content.body,
            mentions: content.mentions,
            refs: content.refs,
        }
    }

    /// Makes this new message a reply to `parent`: same thread, `reply_to` the parent.
    #[must_use]
    pub fn in_reply_to(mut self, parent: &Message) -> Self {
        self.thread_id = parent.thread_id;
        self.reply_to = Some(parent.id);
        self
    }

    /// The NATS subject this message is published on.
    pub fn subject(&self) -> String {
        match (&self.topic, &self.recipient) {
            (Some(topic), _) if self.channel == Channel::Chat => {
                chat_subject(&self.workspace, topic)
            }
            (_, Some(recipient)) => dm_subject(&self.workspace, recipient),
            _ => unreachable!("validated messages carry a topic or a recipient"),
        }
    }

    /// Strictly validates the envelope and serializes it for publishing.
    ///
    /// # Errors
    ///
    /// `invalid_message` for a malformed envelope, `message_too_large` when the serialized form
    /// exceeds [`DEFAULT_MAX_MESSAGE_BYTES`].
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate(Strictness::Strict)?;
        let bytes = serde_json::to_vec(self).expect("envelope serialization cannot fail");
        if bytes.len() > DEFAULT_MAX_MESSAGE_BYTES {
            return Err(Error::new(
                ErrorCode::MessageTooLarge,
                format!(
                    "serialized message is {} bytes; cap is {DEFAULT_MAX_MESSAGE_BYTES}. Put detail in a \
                     file/commit and send a reference.",
                    bytes.len()
                ),
            ));
        }
        Ok(bytes)
    }

    /// Leniently validates a received payload.
    ///
    /// # Errors
    ///
    /// `message_too_large` over the cap, `invalid_message` for anything that is not a valid
    /// `xagent.message.v1` envelope.
    pub fn decode(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() > DEFAULT_MAX_MESSAGE_BYTES {
            return Err(Error::new(
                ErrorCode::MessageTooLarge,
                format!(
                    "payload {} bytes exceeds cap {DEFAULT_MAX_MESSAGE_BYTES}",
                    raw.len()
                ),
            ));
        }
        let text = std::str::from_utf8(raw).map_err(|_| invalid("payload is not valid UTF-8"))?;
        let value: serde_json::Value =
            serde_json::from_str(text).map_err(|_| invalid("payload is not JSON"))?;
        let serde_json::Value::Object(map) = &value else {
            return Err(invalid("payload is not a JSON object"));
        };
        if map.get("schema").and_then(serde_json::Value::as_str) != Some("xagent.message.v1") {
            return Err(invalid(format!(
                "unsupported schema {}",
                map.get("schema").unwrap_or(&serde_json::Value::Null)
            )));
        }
        let msg: Self = serde_json::from_value(value).map_err(|e| invalid(e.to_string()))?;
        msg.validate(Strictness::Lenient)?;
        Ok(msg)
    }

    fn validate(&self, strictness: Strictness) -> Result<(), Error> {
        if !is_valid_id(&self.workspace) {
            return Err(invalid("workspace: invalid workspace"));
        }
        if let Kind::Other(kind) = &self.kind
            && (strictness == Strictness::Strict || !is_future_enum(kind))
        {
            return Err(invalid(
                "kind: must be one of message, question, answer, handoff, review_request, \
                 decision, status, alert",
            ));
        }
        let client_chars = self.sender.client.chars().count();
        if !(1..=CLIENT_MAX_CHARS).contains(&client_chars) {
            return Err(invalid("sender.client: must be 1-32 characters"));
        }
        if !is_rfc3339_with_offset(&self.sent_at) {
            return Err(invalid("sent_at: must be RFC 3339 with a UTC offset"));
        }
        self.validate_summary()?;
        if self.mentions.len() > MAX_MENTIONS {
            return Err(invalid(format!(
                "mentions: at most {MAX_MENTIONS} mentions"
            )));
        }
        let mut seen = std::collections::HashSet::new();
        if !self.mentions.iter().all(|m| seen.insert(m)) {
            return Err(invalid("mentions: duplicate mentions"));
        }
        if self.refs.len() > MAX_REFS {
            return Err(invalid(format!("refs: at most {MAX_REFS} refs")));
        }
        for r in &self.refs {
            validate_ref(r, strictness)?;
        }
        self.validate_shape()
    }

    fn validate_summary(&self) -> Result<(), Error> {
        if self.summary.trim().is_empty() {
            return Err(invalid("summary: summary is required"));
        }
        let chars = self.summary.chars().count();
        if chars > SUMMARY_MAX_CHARS {
            return Err(invalid(format!(
                "summary: summary exceeds {SUMMARY_MAX_CHARS} characters ({chars})"
            )));
        }
        if self
            .summary
            .chars()
            .any(|c| matches!(c, '\u{0}'..='\u{1f}' | '\u{7f}'))
        {
            return Err(invalid("summary: summary must be single-line plain text"));
        }
        Ok(())
    }

    fn validate_shape(&self) -> Result<(), Error> {
        match self.channel {
            Channel::Chat => {
                let Some(topic) = &self.topic else {
                    return Err(invalid("topic is required for chat messages"));
                };
                if self.recipient.is_some() {
                    return Err(invalid("recipient must be null for chat messages"));
                }
                crate::validate_topic(topic).map_err(|e| invalid(e.message))?;
            }
            Channel::Dm => {
                if self.recipient.is_none() {
                    return Err(invalid("recipient is required for direct messages"));
                }
                if self.topic.is_some() {
                    return Err(invalid("topic must be null for direct messages"));
                }
            }
        }
        match self.reply_to {
            None if self.thread_id != self.id => {
                Err(invalid("a new thread must have thread_id == id"))
            }
            Some(parent) if parent == self.id => Err(invalid("reply_to cannot reference itself")),
            Some(_) if self.thread_id == self.id => {
                Err(invalid("a reply must carry the original thread_id"))
            }
            _ => Ok(()),
        }
    }
}

fn validate_ref(r: &Reference, strictness: Strictness) -> Result<(), Error> {
    if let RefType::Other(kind) = &r.kind
        && (strictness == Strictness::Strict || !is_future_enum(kind))
    {
        return Err(invalid(
            "refs: ref type must be one of git_commit, git_branch, path, url, artifact, \
             message, other",
        ));
    }
    if !(1..=REF_VALUE_MAX_CHARS).contains(&r.value.chars().count()) {
        return Err(invalid("refs: value must be 1-1024 characters"));
    }
    if r.label
        .as_ref()
        .is_some_and(|l| l.chars().count() > REF_LABEL_MAX_CHARS)
    {
        return Err(invalid("refs: label must be at most 200 characters"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encoded by the Python reference (`build_message(env).encode()`), 607 bytes.
    const PYTHON_ENCODED: &str = concat!(
        r#"{"schema":"xagent.message.v1","id":"7d1f6a0e-3b2c-4d5e-8f90-a1b2c3d4e5f6","#,
        r#""workspace":"conversations","channel":"dm","topic":null,"#,
        r#""recipient":"chat-bbbbbbbbbbbbbbbbbbbbbbbb","kind":"message","#,
        r#""sender":{"agent_id":"chat-aaaaaaaaaaaaaaaaaaaaaaaa","#,
        r#""instance_id":"11111111-2222-4333-8444-555555555555","client":"claude-code"},"#,
        r#""sent_at":"2026-11-23T10:20:30.123Z","thread_id":"7d1f6a0e-3b2c-4d5e-8f90-a1b2c3d4e5f6","#,
        r#""reply_to":null,"priority":"normal","summary":"Grüße — status","#,
        r#""body":"naïve 日本語 \"quoted\" \n tab\t ctl\u0001 / end","mentions":[],"#,
        r#""refs":[{"type":"path","value":"/tmp/x","label":null}]}"#
    );

    fn sample() -> Message {
        Message::decode(PYTHON_ENCODED.as_bytes()).expect("python envelope decodes")
    }

    #[test]
    fn encoding_is_byte_identical_to_python() {
        let encoded = sample().encode().expect("valid");
        assert_eq!(
            String::from_utf8(encoded.clone()).expect("utf-8"),
            PYTHON_ENCODED
        );
        assert_eq!(encoded.len(), 607);
    }

    #[test]
    fn size_cap_boundary_matches_python() {
        // Python: an empty-body envelope like this one is 493 bytes; 7945 × "é" + "x" reaches
        // exactly 16384, one more byte is rejected.
        let mut msg = sample();
        msg.summary = "s".into();
        msg.body = String::new();
        msg.refs.clear();
        assert_eq!(msg.encode().expect("valid").len(), 493);
        msg.body = format!("{}x", "é".repeat(7945));
        assert_eq!(msg.encode().expect("at the cap").len(), 16384);
        msg.body.push('x');
        let err = msg.encode().expect_err("over the cap");
        assert_eq!(err.code, ErrorCode::MessageTooLarge);
        assert!(
            err.message.starts_with("serialized message is 16385 bytes"),
            "{err}"
        );
    }

    #[test]
    fn receive_is_lenient_send_is_strict() {
        let raw = PYTHON_ENCODED
            .replace(r#""kind":"message""#, r#""kind":"poll""#)
            .replace(
                r#""schema":"xagent.message.v1","#,
                r#""schema":"xagent.message.v1","future":1,"#,
            );
        let msg = Message::decode(raw.as_bytes()).expect("future kind and field tolerated");
        assert_eq!(msg.kind, Kind::Other("poll".into()));
        assert_eq!(msg.kind.display(), "message");
        assert_eq!(
            msg.encode().expect_err("strict").code,
            ErrorCode::InvalidMessage
        );

        let bad_kind = PYTHON_ENCODED.replace(r#""kind":"message""#, r#""kind":"Not-A-Kind""#);
        assert!(Message::decode(bad_kind.as_bytes()).is_err());
        let bad_priority = PYTHON_ENCODED.replace(r#""priority":"normal""#, r#""priority":"meh""#);
        assert!(Message::decode(bad_priority.as_bytes()).is_err());
        let bad_schema = PYTHON_ENCODED.replace("xagent.message.v1", "xagent.message.v2");
        assert!(Message::decode(bad_schema.as_bytes()).is_err());
        let extra_sender = PYTHON_ENCODED.replace(
            r#""client":"claude-code"}"#,
            r#""client":"claude-code","x":1}"#,
        );
        assert!(Message::decode(extra_sender.as_bytes()).is_err());
    }

    #[test]
    fn shape_and_field_rules() {
        let mut m = sample();
        m.topic = Some("a.b".into());
        assert!(m.encode().is_err(), "dm with topic");

        let mut m = sample();
        m.thread_id = MessageId::new_v4();
        assert!(m.encode().is_err(), "new thread with foreign thread id");

        let mut m = sample();
        m.summary = "x".repeat(201);
        assert!(m.encode().is_err(), "summary over 200 chars");
        m.summary = "é".repeat(200);
        assert!(m.encode().is_ok(), "200 non-ASCII chars");
        m.summary = "two\nlines".into();
        assert!(m.encode().is_err(), "control char in summary");
        m.summary = "   ".into();
        assert!(m.encode().is_err(), "blank summary");

        let upper = PYTHON_ENCODED.replace(
            "7d1f6a0e-3b2c-4d5e-8f90-a1b2c3d4e5f6",
            "7D1F6A0E-3B2C-4D5E-8F90-A1B2C3D4E5F6",
        );
        let m = Message::decode(upper.as_bytes()).expect("uppercase UUID accepted");
        assert_eq!(m.id.to_string(), "7d1f6a0e-3b2c-4d5e-8f90-a1b2c3d4e5f6");
        let simple = PYTHON_ENCODED.replace(
            "7d1f6a0e-3b2c-4d5e-8f90-a1b2c3d4e5f6",
            "7d1f6a0e3b2c4d5e8f90a1b2c3d4e5f6",
        );
        assert!(
            Message::decode(simple.as_bytes()).is_err(),
            "non-hyphenated UUID"
        );
    }
}
