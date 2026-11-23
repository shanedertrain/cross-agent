//! Idempotent bootstrap and verification of the message stream and the KV buckets.
//!
//! Existing resources are never deleted or rewritten. A mismatch in a setting the protocol
//! depends on is fatal: the daemon refuses to run the message pipeline until an operator fixes it.

use std::fmt;
use std::time::Duration;

use async_nats::jetstream::{
    self,
    stream::{Config, DiscardPolicy, RetentionPolicy, StorageType},
};
use tracing::{info, warn};
use xagent_proto::{DEFAULT_MAX_MESSAGE_BYTES, STREAM_SUBJECTS};

use crate::{
    BUCKET_AGENTS, BUCKET_LEASES, BUCKET_SUBSCRIPTIONS, BUCKET_TOPICS, Bus, BusError, STREAM,
    stream_missing,
};

/// Configured lease TTL; an existing lease bucket's `max_age` wins if it differs.
const LEASE_TTL: Duration = Duration::from_secs(30);
/// NATS counts headers (`Nats-Msg-Id`) toward the stream's maximum message size.
const HEADER_SLACK_BYTES: usize = 1024;
/// An existing stream must accept at least the envelope cap plus this much header room.
const MIN_HEADER_ROOM_BYTES: usize = 128;
const MAX_BYTES: i64 = 1 << 30;
/// Retention of a newly created message stream.
pub const DEFAULT_RETENTION: Duration = Duration::from_secs(7 * 86_400);
const DUPLICATE_WINDOW: Duration = Duration::from_secs(600);
/// KV duplicate window, as the NATS clients set it: two minutes, or the TTL if shorter.
const KV_DUPLICATE_WINDOW: Duration = Duration::from_secs(120);

/// One setting of an existing resource that differs from what this build expects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    pub resource: String,
    pub field: &'static str,
    pub expected: String,
    pub actual: String,
    /// The protocol cannot run on this resource.
    pub fatal: bool,
}

impl fmt::Display for Mismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}: expected {}, found {}",
            self.resource, self.field, self.expected, self.actual
        )
    }
}

/// What bootstrap created and which non-fatal differences it found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BootstrapReport {
    pub created: Vec<String>,
    pub mismatches: Vec<Mismatch>,
}

fn expected_stream(retention: Duration) -> Config {
    Config {
        name: STREAM.to_owned(),
        description: Some("xagent v1 chat and direct messages".to_owned()),
        subjects: STREAM_SUBJECTS.iter().map(|s| (*s).to_owned()).collect(),
        retention: RetentionPolicy::Limits,
        storage: StorageType::File,
        num_replicas: 1,
        max_age: retention,
        max_bytes: MAX_BYTES,
        max_message_size: i32::try_from(DEFAULT_MAX_MESSAGE_BYTES + HEADER_SLACK_BYTES)
            .expect("stream message size fits i32"),
        discard: DiscardPolicy::Old,
        duplicate_window: DUPLICATE_WINDOW,
        ..Default::default()
    }
}

fn stream_mismatch(
    field: &'static str,
    expected: impl fmt::Debug,
    actual: impl fmt::Debug,
    fatal: bool,
) -> Mismatch {
    Mismatch {
        resource: STREAM.to_owned(),
        field,
        expected: format!("{expected:?}"),
        actual: format!("{actual:?}"),
        fatal,
    }
}

/// Settings of an existing stream that differ from `expected`; fatal ones break the protocol.
fn compare_stream(expected: &Config, actual: &Config) -> Vec<Mismatch> {
    let mut out = Vec::new();
    let sorted = |v: &[String]| {
        let mut v = v.to_vec();
        v.sort();
        v
    };
    let (want_subjects, have_subjects) = (sorted(&expected.subjects), sorted(&actual.subjects));
    if want_subjects != have_subjects {
        out.push(stream_mismatch(
            "subjects",
            want_subjects,
            have_subjects,
            true,
        ));
    }
    if actual.retention != RetentionPolicy::Limits {
        out.push(stream_mismatch(
            "retention",
            RetentionPolicy::Limits,
            actual.retention,
            true,
        ));
    }
    if actual.storage != StorageType::File {
        out.push(stream_mismatch(
            "storage",
            StorageType::File,
            actual.storage,
            true,
        ));
    }
    out.extend(compare_stream_limits(expected, actual));
    out
}

/// Size, age and deduplication limits of an existing stream.
fn compare_stream_limits(expected: &Config, actual: &Config) -> Vec<Mismatch> {
    let mut out = Vec::new();
    let app_cap = DEFAULT_MAX_MESSAGE_BYTES + MIN_HEADER_ROOM_BYTES;
    let (want_size, have_size) = (expected.max_message_size, actual.max_message_size);
    if usize::try_from(have_size).unwrap_or(0) < app_cap {
        out.push(stream_mismatch(
            "max_msg_size",
            format!(">= {app_cap}"),
            have_size,
            true,
        ));
    } else if have_size != want_size {
        out.push(stream_mismatch("max_msg_size", want_size, have_size, false));
    }
    let (want_window, have_window) = (expected.duplicate_window, actual.duplicate_window);
    if have_window.is_zero() {
        out.push(stream_mismatch(
            "duplicate_window",
            want_window,
            have_window,
            true,
        ));
    } else if have_window.abs_diff(want_window) > Duration::from_secs(1) {
        out.push(stream_mismatch(
            "duplicate_window",
            want_window,
            have_window,
            false,
        ));
    }
    if actual.max_bytes <= 0 {
        out.push(stream_mismatch(
            "max_bytes",
            "a positive cap",
            actual.max_bytes,
            true,
        ));
    } else if actual.max_bytes != expected.max_bytes {
        out.push(stream_mismatch(
            "max_bytes",
            expected.max_bytes,
            actual.max_bytes,
            false,
        ));
    }
    if actual.num_replicas != expected.num_replicas {
        out.push(stream_mismatch(
            "num_replicas",
            expected.num_replicas,
            actual.num_replicas,
            false,
        ));
    }
    if actual.discard != DiscardPolicy::Old {
        out.push(stream_mismatch(
            "discard",
            DiscardPolicy::Old,
            actual.discard,
            false,
        ));
    }
    out
}

/// The backing stream of a KV bucket, exactly as the Python client created it.
fn bucket_stream(bucket: &str, description: &str, history: i64, ttl: Duration) -> Config {
    let duplicate_window = if !ttl.is_zero() && ttl < KV_DUPLICATE_WINDOW {
        ttl
    } else {
        KV_DUPLICATE_WINDOW
    };
    Config {
        name: format!("KV_{bucket}"),
        description: Some(description.to_owned()),
        subjects: vec![format!("$KV.{bucket}.>")],
        allow_rollup: true,
        allow_message_ttl: true,
        deny_delete: true,
        discard: DiscardPolicy::New,
        duplicate_window,
        max_age: ttl,
        max_bytes: -1,
        max_consumers: -1,
        max_message_size: -1,
        max_messages: -1,
        max_messages_per_subject: history,
        num_replicas: 1,
        storage: StorageType::File,
        ..Default::default()
    }
}

fn bucket_streams() -> [Config; 4] {
    [
        bucket_stream(BUCKET_TOPICS, "xagent topic registry", 5, Duration::ZERO),
        bucket_stream(BUCKET_AGENTS, "xagent agent registry", 5, Duration::ZERO),
        bucket_stream(BUCKET_LEASES, "xagent live-session leases", 1, LEASE_TTL),
        bucket_stream(
            BUCKET_SUBSCRIPTIONS,
            "xagent persistent subscriptions",
            1,
            Duration::ZERO,
        ),
    ]
}

/// Creates the stream if it is missing. Returns the existing stream's configuration, or `None`
/// when this call created it. A stream another host creates concurrently counts as existing.
async fn ensure_stream(
    js: &jetstream::Context,
    config: Config,
    report: &mut BootstrapReport,
) -> Result<Option<Config>, BusError> {
    let name = config.name.clone();
    let mut created_elsewhere = false;
    loop {
        match js.get_stream(&name).await {
            Ok(mut stream) => {
                let info = stream.info().await.map_err(BusError::nats)?;
                return Ok(Some(info.config.clone()));
            }
            Err(err) if stream_missing(&err) => {}
            Err(err) => return Err(BusError::nats(err)),
        }
        match js.create_stream(config.clone()).await {
            Ok(_) => {
                report.created.push(name);
                return Ok(None);
            }
            // Lost a creation race: look again once.
            Err(_) if !created_elsewhere => created_elsewhere = true,
            Err(err) => return Err(BusError::nats(err)),
        }
    }
}

/// Ensures the stream and the buckets exist, creating missing ones, and verifies existing ones.
///
/// # Errors
///
/// [`BusError::Incompatible`] if a resource has a fatal mismatch; [`BusError::Nats`] otherwise.
pub(crate) async fn bootstrap(js: jetstream::Context) -> Result<(Bus, BootstrapReport), BusError> {
    let mut report = BootstrapReport::default();
    // A new stream keeps messages for the default retention; an existing stream's `max_age` is the
    // operator's choice (`Bus::set_retention`) and is not compared.
    let expected = expected_stream(DEFAULT_RETENTION);
    if let Some(actual) = ensure_stream(&js, expected.clone(), &mut report).await? {
        report.mismatches.extend(compare_stream(&expected, &actual));
    }
    let mut lease_ttl = LEASE_TTL;
    for config in bucket_streams() {
        let leases = config.name == format!("KV_{BUCKET_LEASES}");
        if let Some(actual) = ensure_stream(&js, config, &mut report).await?
            && leases
        {
            lease_ttl = verify_lease_bucket(&actual, &mut report);
        }
    }

    let fatal: Vec<Mismatch> = report
        .mismatches
        .iter()
        .filter(|m| m.fatal)
        .cloned()
        .collect();
    if !fatal.is_empty() {
        return Err(BusError::Incompatible(fatal));
    }
    for m in &report.mismatches {
        warn!(resource = %m.resource, field = m.field, expected = %m.expected, actual = %m.actual,
              "jetstream setting differs from the default");
    }
    if !report.created.is_empty() {
        info!(created = ?report.created, "bootstrap created resources");
    }
    let agents = js
        .get_key_value(BUCKET_AGENTS)
        .await
        .map_err(BusError::nats)?;
    let leases = js
        .get_key_value(BUCKET_LEASES)
        .await
        .map_err(BusError::nats)?;
    let topics = js
        .get_key_value(BUCKET_TOPICS)
        .await
        .map_err(BusError::nats)?;
    let subscriptions = js
        .get_key_value(BUCKET_SUBSCRIPTIONS)
        .await
        .map_err(BusError::nats)?;
    Ok((
        Bus {
            js,
            agents,
            leases,
            topics,
            subscriptions,
            lease_ttl,
        },
        report,
    ))
}

/// The lease bucket's `max_age` is the TTL every host honours; one set by an operator (or another
/// host's config) is adopted with a warning. None at all is fatal: leases would never expire.
fn verify_lease_bucket(actual: &Config, report: &mut BootstrapReport) -> Duration {
    let resource = format!("KV_{BUCKET_LEASES}");
    if actual.max_messages_per_subject > 1 {
        report.mismatches.push(Mismatch {
            resource: resource.clone(),
            field: "history",
            expected: "1".into(),
            actual: actual.max_messages_per_subject.to_string(),
            fatal: false,
        });
    }
    if actual.max_age.is_zero() {
        report.mismatches.push(Mismatch {
            resource,
            field: "ttl",
            expected: format!("{LEASE_TTL:?}"),
            actual: "none".into(),
            fatal: true,
        });
        return LEASE_TTL;
    }
    if actual.max_age.abs_diff(LEASE_TTL) > Duration::from_millis(1) {
        report.mismatches.push(Mismatch {
            resource,
            field: "ttl",
            expected: format!("{LEASE_TTL:?}"),
            actual: format!("{:?}", actual.max_age),
            fatal: false,
        });
    }
    actual.max_age
}
