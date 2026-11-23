//! Operator actions: local retention (gc) and rebuilding inboxes from JetStream (recover).

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tracing::{error, info, warn};
use xagent_proto::{AgentId, Channel, Error, ErrorCode, Message, WORKSPACE, ts_before};
use xagent_store::{Delivery, StoreError};

use crate::daemon::{Daemon, internal};
use crate::protocol::Recover;
use crate::topics::{filters, is_subscribed};

/// No local copy outlives the broker's own 7-day retention.
const MAX_LOCAL_AGE: Duration = Duration::from_secs(7 * 86_400);
const GC_INTERVAL: Duration = Duration::from_secs(3600);
/// The stream keeps 7 days; a longer window reads nothing more.
const RECOVER_MAX_HOURS: u32 = 7 * 24;
/// Most messages re-read per agent.
const RECOVER_MAX_MESSAGES: usize = 5000;

impl Daemon {
    /// Deletes every local copy older than the broker's retention (pending sends excepted).
    pub(crate) fn gc(&self, dry_run: bool) -> Result<Value, Error> {
        let cutoff = ts_before(MAX_LOCAL_AGE);
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
            if let Err(err) = self.gc(false) {
                error!(error = %err, "retention gc failed");
            }
            tokio::time::sleep(GC_INTERVAL).await;
        }
    }

    /// Re-reads the last `hours` of the stream into the inboxes of the given agents (default:
    /// every agent last registered from this host), as unread. Messages already stored keep
    /// their state; topic messages count only from each subscription's start.
    pub(crate) async fn recover(&self, params: Recover) -> Result<Value, Error> {
        if !(1..=RECOVER_MAX_HOURS).contains(&params.hours) {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                format!("hours must be 1-{RECOVER_MAX_HOURS}"),
            ));
        }
        let bus = self.require_bus("recovering inboxes")?.clone();
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
