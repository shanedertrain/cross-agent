//! Stable, model-facing error codes.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A domain error code. The string form is part of the public contract (MCP tools, IPC, CLI).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    TopicNotFound,
    TopicExists,
    TopicArchived,
    AgentNotFound,
    MessageNotFound,
    DuplicateActiveAgent,
    LeaseLost,
    LeasePending,
    MessageIdCollision,
    MessageTooLarge,
    InvalidTopic,
    InvalidAgentId,
    InvalidMessage,
    InvalidArgument,
    NotSubscribed,
    SubscriptionConflict,
    UnsupportedReplay,
    NatsUnavailable,
    PermissionDenied,
    NotBound,
    DaemonUnavailable,
    StreamMismatch,
    #[serde(rename = "internal_error")]
    Internal,
}

impl ErrorCode {
    /// The wire string of this code.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TopicNotFound => "topic_not_found",
            Self::TopicExists => "topic_exists",
            Self::TopicArchived => "topic_archived",
            Self::AgentNotFound => "agent_not_found",
            Self::MessageNotFound => "message_not_found",
            Self::DuplicateActiveAgent => "duplicate_active_agent",
            Self::LeaseLost => "lease_lost",
            Self::LeasePending => "lease_pending",
            Self::MessageIdCollision => "message_id_collision",
            Self::MessageTooLarge => "message_too_large",
            Self::InvalidTopic => "invalid_topic",
            Self::InvalidAgentId => "invalid_agent_id",
            Self::InvalidMessage => "invalid_message",
            Self::InvalidArgument => "invalid_argument",
            Self::NotSubscribed => "not_subscribed",
            Self::SubscriptionConflict => "subscription_conflict",
            Self::UnsupportedReplay => "unsupported_replay",
            Self::NatsUnavailable => "nats_unavailable",
            Self::PermissionDenied => "permission_denied",
            Self::NotBound => "not_bound",
            Self::DaemonUnavailable => "daemon_unavailable",
            Self::StreamMismatch => "stream_mismatch",
            Self::Internal => "internal_error",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An error with a stable code and a short, actionable message that is safe to show to a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[error("{code}: {message}")]
pub struct Error {
    pub code: ErrorCode,
    pub message: String,
}

impl Error {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
