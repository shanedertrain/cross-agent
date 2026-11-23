//! Operator actions: retention and the stream's size cap, local cleanup (gc), and rebuilding
//! inboxes from JetStream (recover).

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tracing::{error, info, warn};
use xagent_proto::{AgentId, Channel, Error, ErrorCode, Message, WORKSPACE, ts_before};
use xagent_store::{Delivery, StoreError};

use crate::daemon::{Daemon, internal};
use crate::protocol::{Recover, Retention};
use crate::topics::{filters, is_subscribed, nats_error};

const GC_INTERVAL: Duration = Duration::from_secs(3600);
/// Most messages re-read per agent.
const RECOVER_MAX_MESSAGES: usize = 5000;
/// Longest broker retention `xagent retention --days` accepts (ten years).
const MAX_RETENTION_DAYS: u32 = 3650;
/// Largest stream size cap `xagent retention --max-mb` accepts (1 TiB).
const MAX_STREAM_MB: u32 = 1 << 20;
const MIB: i64 = 1 << 20;

impl Daemon {
    /// This host's retention: `[retention] local_days`, capped by the broker's (read live; the
    /// local value alone while NATS is unavailable).
    async fn effective_retention(&self) -> Duration {
        let local = self.config.local_retention;
        match self.bus() {
            Some(bus) => match bus.stream_limits().await.map(|l| l.retention) {
                Ok(broker) if !broker.is_zero() => local.min(broker),
                Ok(_) => local,
                Err(err) => {
                    warn!(error = %err, "broker retention unreadable; using the local value");
                    local
                }
            },
            None => local,
        }
    }

    /// Deletes every local copy older than this host's retention (pending sends excepted).
    pub(crate) async fn gc(&self, dry_run: bool) -> Result<Value, Error> {
        let retention = self.effective_retention().await;
        let cutoff = ts_before(retention);
        let counts = self
            .store()
            .gc(&cutoff, dry_run)
            .map_err(|err| internal(&err))?;
        if !dry_run && counts.deliveries + counts.outbox + counts.messages > 0 {
            info!(
                deliveries = counts.deliveries,
                outbox = counts.outbox,
                messages = counts.messages,
                "retention gc"
            );
        }
        Ok(serde_json::json!({
            "dry_run": dry_run,
            "retention_days": retention.as_secs() / 86_400,
            "cutoff": cutoff,
            "deleted": {
                "deliveries": counts.deliveries,
                "outbox": counts.outbox,
                "messages": counts.messages,
            },
        }))
    }

    /// Runs gc at start and every hour.
    pub(crate) async fn gc_loop(self: Arc<Self>) {
        loop {
            if let Err(err) = self.gc(false).await {
                error!(error = %err, "retention gc failed");
            }
            tokio::time::sleep(GC_INTERVAL).await;
        }
    }

    /// Shows the broker's limits and this host's retention; sets the broker's limits first when
    /// `params` names any.
    pub(crate) async fn retention(&self, params: Retention) -> Result<Value, Error> {
        let bus = self.require_bus("reading the retention")?.clone();
        let invalid = |message: &str| Error::new(ErrorCode::InvalidArgument, message);
        if params
            .days
            .is_some_and(|d| !(1..=MAX_RETENTION_DAYS).contains(&d))
        {
            return Err(invalid("days must be 1-3650"));
        }
        if params
            .max_mb
            .is_some_and(|mb| !(1..=MAX_STREAM_MB).contains(&mb))
        {
            return Err(invalid("max_mb must be 1-1048576"));
        }
        let previous = if params.days.is_some() || params.max_mb.is_some() {
            let old = bus
                .set_stream_limits(
                    params
                        .days
                        .map(|d| Duration::from_secs(u64::from(d) * 86_400)),
                    params.max_mb.map(|mb| i64::from(mb) * MIB),
                )
                .await
                .map_err(|err| nats_error("setting the stream limits", &err))?;
            info!(
                old_days = old.retention.as_secs() / 86_400,
                old_max_mb = old.max_bytes / MIB,
                days = params.days,
                max_mb = params.max_mb,
                "broker stream limits set"
            );
            Some(old)
        } else {
            None
        };
        let broker = bus
            .stream_limits()
            .await
            .map_err(|err| nats_error("reading the retention", &err))?;
        let local = self.config.local_retention;
        Ok(serde_json::json!({
            "broker_days": broker.retention.as_secs() / 86_400,
            "broker_max_mb": broker.max_bytes / MIB,
            "local_days": local.as_secs() / 86_400,
            "effective_local_days": local.min(broker.retention).as_secs() / 86_400,
            "previous_broker_days": previous.map(|p| p.retention.as_secs() / 86_400),
            "previous_broker_max_mb": previous.map(|p| p.max_bytes / MIB),
        }))
    }

    /// Re-reads the last `hours` of the stream into the inboxes of the given agents (default:
    /// every agent last registered from this host), as unread. Messages already stored keep
    /// their state; topic messages count only from each subscription's start.
    pub(crate) async fn recover(&self, params: Recover) -> Result<Value, Error> {
        let bus = self.require_bus("recovering inboxes")?.clone();
        // The stream keeps the broker's retention; a longer window reads nothing more.
        let broker = bus
            .stream_limits()
            .await
            .map_err(|err| nats_error("reading the retention", &err))?
            .retention;
        let max_hours = u32::try_from(broker.as_secs() / 3600).unwrap_or(u32::MAX);
        if !(1..=max_hours).contains(&params.hours) {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                format!("hours must be 1-{max_hours}"),
            ));
        }
        let fail = |err: xagent_bus::BusError| {
            warn!(error = %err, "recover failed");
            Error::new(
                ErrorCode::NatsUnavailable,
                format!("recover needs NATS: {err}"),
            )
        };
        let agents = if params.agents.is_empty() {
            bus.agents_last_on_host(WORKSPACE, &self.config.host_id)
                .await
                .map_err(fail)?
        } else {
            params.agents
        };
        let window = Duration::from_secs(u64::from(params.hours) * 3600);
        let mut results = Vec::new();
        for agent in agents {
            let topics = bus.subscriptions(WORKSPACE, &agent).await.map_err(fail)?;
            let scanned = bus
                .scan_recent(filters(&agent, &topics), window, RECOVER_MAX_MESSAGES)
                .await
                .map_err(fail)?;
            let mut inserted = 0_u64;
            for s in &scanned {
                let Ok(msg) = Message::decode(&s.payload) else {
                    continue;
                };
                let wanted = msg.subject() == s.subject
                    && msg.sender.agent_id != agent
                    && match (&msg.channel, &msg.topic, &msg.recipient) {
                        (Channel::Dm, _, Some(recipient)) => *recipient == agent,
                        (Channel::Chat, Some(topic), _) => {
                            is_subscribed(&topics, topic, s.sequence)
                        }
                        _ => false,
                    };
                if wanted && self.store_recovered(&agent, &msg, &s.payload, s.sequence)? {
                    inserted += 1;
                }
            }
            info!(agent = %agent, scanned = scanned.len(), inserted, "recovered");
            results.push(serde_json::json!({
                "agent_id": agent,
                "scanned": scanned.len(),
                "inserted": inserted,
                "truncated": scanned.len() >= RECOVER_MAX_MESSAGES,
            }));
        }
        Ok(serde_json::json!({ "since": ts_before(window), "agents": results }))
    }

    fn store_recovered(
        &self,
        agent: &AgentId,
        msg: &Message,
        payload: &[u8],
        sequence: u64,
    ) -> Result<bool, Error> {
        let raw = std::str::from_utf8(payload).expect("decoded envelopes are UTF-8");
        match self.store().record_incoming(msg, raw, agent, sequence) {
            Ok(delivery) => Ok(delivery == Delivery::Stored),
            Err(StoreError::IdCollision(id)) => {
                error!(message_id = %id, "message id collision during recover");
                Ok(false)
            }
            Err(err) => Err(internal(&err)),
        }
    }
}
