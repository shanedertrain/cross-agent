//! JetStream KV record values. Unknown fields are ignored on read.

use serde::{Deserialize, Serialize};

use crate::{AgentId, MessageId};

/// `XAGENT_AGENTS` value: one registered agent identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "schema", rename = "xagent.agent.v1")]
pub struct AgentRecord {
    pub workspace: String,
    pub agent_id: AgentId,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub project_topic: Option<String>,
    #[serde(default = "unknown")]
    pub client: String,
    #[serde(default)]
    pub host_id: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    pub created_at: String,
    pub updated_at: String,
}

fn unknown() -> String {
    "unknown".to_owned()
}

/// `XAGENT_LEASES` value: proof that one session instance owns an agent identity.
///
/// The timestamps are informational: expiry is the lease bucket's server-side TTL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "schema", rename = "xagent.lease.v1")]
pub struct LeaseRecord {
    pub workspace: String,
    pub agent_id: AgentId,
    pub instance_id: MessageId,
    pub host_id: String,
    pub daemon_run_id: String,
    #[serde(default = "unknown")]
    pub client: String,
    pub acquired_at: String,
    pub refreshed_at: String,
    pub ttl_seconds: f64,
}

/// `XAGENT_TOPICS` value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "schema", rename = "xagent.topic.v1")]
pub struct TopicRecord {
    pub workspace: String,
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub created_by: String,
    pub created_at: String,
    #[serde(default)]
    pub archived: bool,
}

/// One persistent subscription inside an agent's [`SubscriptionSet`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PersistentSubscription {
    pub pattern: String,
    /// Stream last sequence at subscribe time (replay = new).
    #[serde(default)]
    pub start_seq: Option<u64>,
    pub created_at: String,
}

/// `XAGENT_SUBSCRIPTIONS` value: the complete persistent subscription set of one agent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "schema", rename = "xagent.subscriptions.v1")]
pub struct SubscriptionSet {
    pub workspace: String,
    pub agent_id: AgentId,
    #[serde(default)]
    pub subscriptions: Vec<PersistentSubscription>,
    pub updated_at: String,
}
