//! The xagent wire protocol: message envelope, registry records, validation, NATS subjects and
//! the conversation identity derivation.
//!
//! Everything here is pure logic with no I/O. The NATS wire format is the `xagent.message.v1`
//! contract shared by every host; changing an encoding here changes what other hosts receive.

mod envelope;
mod error;
mod id;
mod identity;
mod records;
mod subject;
mod time;

pub use envelope::{
    Channel, Content, DEFAULT_MAX_MESSAGE_BYTES, Kind, Message, MessageId, Priority, RefType,
    Reference, Sender,
};
pub use error::{Error, ErrorCode};
pub use id::{AgentId, is_valid_id, validate_topic};
pub use identity::{Client, Conversation, conversation_agent_id};
pub use records::{AgentRecord, LeaseRecord, PersistentSubscription, SubscriptionSet, TopicRecord};
pub use subject::{
    STREAM_SUBJECTS, chat_subject, consumer_name, dm_subject, parse_subject, registry_key,
    subject_matches,
};
pub use time::{now_ts, ts_before};

/// The one workspace every conversation identity lives in.
pub const WORKSPACE: &str = "conversations";
