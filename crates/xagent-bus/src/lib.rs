//! The NATS JetStream side of xagent: connecting, bootstrapping and verifying the message
//! stream and the registry buckets, the agent registry, live-session leases, durable per-agent
//! consumers, and publishing.
//!
//! Lease expiry belongs to the NATS server: the `XAGENT_LEASES` bucket's `max_age` removes a
//! key that stops being refreshed, so no host ever compares clocks.

mod bootstrap;
mod connect;
mod directory;
mod inbox;
mod registry;
mod scan;
mod topics;

use std::time::Duration;

use async_nats::jetstream::{self, kv};
use bytes::Bytes;
use xagent_proto::{AgentId, consumer_name};

pub use bootstrap::{BootstrapReport, Mismatch};
pub use connect::{Connection, NatsSettings, UserPassword, connect};
pub use inbox::{Inbox, Incoming, Pull};
pub use scan::Scanned;

/// The message stream.
pub const STREAM: &str = "XAGENT_MESSAGES";
/// Topic registry bucket.
pub const BUCKET_TOPICS: &str = "XAGENT_TOPICS";
/// Agent registry bucket.
pub const BUCKET_AGENTS: &str = "XAGENT_AGENTS";
/// Live-session lease bucket; its `max_age` is the lease TTL.
pub const BUCKET_LEASES: &str = "XAGENT_LEASES";
/// Persistent subscription sets bucket.
pub const BUCKET_SUBSCRIPTIONS: &str = "XAGENT_SUBSCRIPTIONS";

/// Unacknowledged messages are redelivered after this.
pub const ACK_WAIT: Duration = Duration::from_secs(30);
/// Messages a consumer may have outstanding without an ACK.
const MAX_ACK_PENDING: i64 = 1000;

/// A failure talking to NATS, or a refusal from it.
#[derive(Debug, thiserror::Error)]
pub enum BusError {
    /// Another session instance owns the identity.
    #[error(
        "agent {agent} is already live (host {host_id}, client {client}, since {since}). Stop that \
         session or wait for its lease to expire"
    )]
    LeaseHeld {
        agent: AgentId,
        host_id: String,
        client: String,
        since: String,
    },
    /// A create found the key already present.
    #[error("key already exists")]
    Exists,
    /// A compare-and-set kept losing to concurrent writers.
    #[error("{0} is changing too fast; retry")]
    Contended(String),
    /// The caller's authority ended before the write.
    #[error("authority ended before the write")]
    NoAuthority,
    /// A compare-and-set on a lease found another revision.
    #[error("lease revision changed")]
    LeaseConflict,
    /// The server refused a publish for good (it would be refused again).
    #[error("publish rejected by the server: {0}")]
    Rejected(String),
    /// The stream or a bucket exists with settings the protocol cannot run on.
    #[error("incompatible JetStream resources: {0:?}")]
    Incompatible(Vec<Mismatch>),
    /// A registry value that is not a valid record.
    #[error("invalid registry record: {0}")]
    Record(#[from] serde_json::Error),
    /// Connection loss, timeout or server error: worth retrying.
    #[error("nats: {0}")]
    Nats(async_nats::Error),
}

impl BusError {
    fn nats(err: impl Into<async_nats::Error>) -> Self {
        Self::Nats(err.into())
    }
}

/// The message stream's broker-wide limits, shared by every host. Whichever is reached first
/// removes the oldest messages (discard policy `old`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamLimits {
    /// Age at which the broker deletes a message (`max_age`).
    pub retention: Duration,
    /// Size cap of the whole stream (`max_bytes`).
    pub max_bytes: i64,
}

impl StreamLimits {
    fn of(config: &jetstream::stream::Config) -> Self {
        Self {
            retention: config.max_age,
            max_bytes: config.max_bytes,
        }
    }
}

/// The outcome of an acknowledged publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Published {
    pub sequence: u64,
    /// The stream had already stored a message with this id (deduplicated).
    pub duplicate: bool,
}

/// Bootstrapped JetStream handles shared by the whole daemon. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Bus {
    js: jetstream::Context,
    agents: kv::Store,
    leases: kv::Store,
    topics: kv::Store,
    subscriptions: kv::Store,
    lease_ttl: Duration,
}

impl Bus {
    /// The server-side lease TTL in force (the lease bucket's `max_age`).
    pub fn lease_ttl(&self) -> Duration {
        self.lease_ttl
    }

    /// The message stream's limits in force on the broker, read live.
    ///
    /// # Errors
    ///
    /// [`BusError::Nats`] when the stream cannot be read.
    pub async fn stream_limits(&self) -> Result<StreamLimits, BusError> {
        let mut stream = self.js.get_stream(STREAM).await.map_err(BusError::nats)?;
        let info = stream.info().await.map_err(BusError::nats)?;
        Ok(StreamLimits::of(&info.config))
    }

    /// Changes the message stream's limits on the broker, for every host (a `None` keeps that
    /// limit); returns the previous limits. The broker removes the oldest messages at once until
    /// the stream is within the new limits.
    ///
    /// # Errors
    ///
    /// [`BusError::Nats`] when the stream cannot be read or updated.
    pub async fn set_stream_limits(
        &self,
        retention: Option<Duration>,
        max_bytes: Option<i64>,
    ) -> Result<StreamLimits, BusError> {
        let mut stream = self.js.get_stream(STREAM).await.map_err(BusError::nats)?;
        let mut config = stream.info().await.map_err(BusError::nats)?.config.clone();
        let old = StreamLimits::of(&config);
        if let Some(retention) = retention {
            config.max_age = retention;
        }
        if let Some(max_bytes) = max_bytes {
            config.max_bytes = max_bytes;
        }
        self.js
            .update_stream(config)
            .await
            .map_err(BusError::nats)?;
        Ok(old)
    }

    /// Creates the agent's durable consumer with `filters`, or updates the filters of an existing
    /// one in place (keeping its delivery cursor).
    pub async fn ensure_consumer(
        &self,
        workspace: &str,
        agent: &AgentId,
        filters: Vec<String>,
    ) -> Result<Inbox, BusError> {
        let name = consumer_name(workspace, agent);
        let stream = self.js.get_stream(STREAM).await.map_err(BusError::nats)?;
        let existing = match stream.consumer_info(&name).await {
            Ok(info) => Some(info),
            Err(err) if is_not_found(&err) => None,
            Err(err) => return Err(BusError::nats(err)),
        };
        let mut wanted = filters.clone();
        wanted.sort();
        if let Some(info) = &existing {
            let mut have = if info.config.filter_subjects.is_empty() {
                vec![info.config.filter_subject.clone()]
            } else {
                info.config.filter_subjects.clone()
            };
            have.sort();
            if have == wanted && info.config.ack_wait == ACK_WAIT {
                return stream
                    .get_consumer(&name)
                    .await
                    .map(Inbox::new)
                    .map_err(BusError::Nats);
            }
        }
        let (filter_subject, filter_subjects) = match <[String; 1]>::try_from(filters) {
            Ok([one]) => (one, Vec::new()),
            Err(many) => (String::new(), many),
        };
        let mut config = jetstream::consumer::pull::Config {
            durable_name: Some(name),
            description: Some(format!("xagent inbox {workspace}/{agent}")),
            ack_policy: jetstream::consumer::AckPolicy::Explicit,
            deliver_policy: jetstream::consumer::DeliverPolicy::New,
            ack_wait: ACK_WAIT,
            max_ack_pending: MAX_ACK_PENDING,
            filter_subject,
            filter_subjects,
            ..Default::default()
        };
        if let Some(info) = existing {
            // An update must restate the original deliver policy: the server refuses a change.
            config.deliver_policy = info.config.deliver_policy;
        }
        stream
            .create_consumer(config)
            .await
            .map(Inbox::new)
            .map_err(BusError::nats)
    }

    /// Messages of the agent's consumer not yet delivered, and delivered but not yet acknowledged.
    pub async fn consumer_backlog(
        &self,
        workspace: &str,
        agent: &AgentId,
    ) -> Result<(u64, u64), BusError> {
        let stream = self.js.get_stream(STREAM).await.map_err(BusError::nats)?;
        let info = stream
            .consumer_info(consumer_name(workspace, agent))
            .await
            .map_err(BusError::nats)?;
        Ok((info.num_pending, info.num_ack_pending as u64))
    }

    /// Publishes with `Nats-Msg-Id` set to the message id, so a retry is deduplicated.
    ///
    /// # Errors
    ///
    /// [`BusError::Rejected`] when the server refuses the message itself (4xx); [`BusError::Nats`]
    /// for anything worth retrying.
    pub async fn publish(
        &self,
        subject: String,
        message_id: &str,
        payload: Vec<u8>,
    ) -> Result<Published, BusError> {
        let publish = jetstream::message::PublishMessage::build()
            .payload(Bytes::from(payload))
            .message_id(message_id);
        let outcome = match self.js.send_publish(subject, publish).await {
            Ok(ack) => ack.await,
            Err(err) => Err(err),
        };
        match outcome {
            Ok(ack) => Ok(Published {
                sequence: ack.sequence,
                duplicate: ack.duplicate,
            }),
            Err(err) => Err(classify_publish_error(err)),
        }
    }
}

/// Whether the server refused the publish for good: a 4xx API error other than 408, or a payload
/// over the server's limit.
fn classify_publish_error(err: jetstream::context::PublishError) -> BusError {
    use jetstream::context::PublishErrorKind;
    if err.kind() == PublishErrorKind::MaxPayloadExceeded {
        return BusError::Rejected(err.to_string());
    }
    let api_status = std::error::Error::source(&err)
        .and_then(|s| s.downcast_ref::<jetstream::Error>())
        .map(jetstream::Error::code);
    match api_status {
        Some(code) if (400..500).contains(&code) && code != 408 => {
            BusError::Rejected(err.to_string())
        }
        _ => BusError::nats(err),
    }
}

fn is_not_found(err: &jetstream::context::ConsumerInfoError) -> bool {
    err.kind() == jetstream::context::ConsumerInfoErrorKind::NotFound
}

fn stream_missing(err: &jetstream::context::GetStreamError) -> bool {
    matches!(
        err.kind(),
        jetstream::context::GetStreamErrorKind::JetStream(e)
            if e.error_code() == jetstream::ErrorCode::STREAM_NOT_FOUND
    )
}
