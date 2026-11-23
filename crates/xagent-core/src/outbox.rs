//! Send pipeline: outbox row first, publish with `Nats-Msg-Id`, retry with the same id.
//!
//! A row is published only by its own live session while that session's lease is held and
//! fresh. Rows of ended sessions, and rows queued before a lease gap, are failed instead: once
//! the session is gone nothing can prove that no other instance owned the identity meanwhile.

use std::sync::Arc;
use std::time::Duration;

use serde::Serialize;
use tracing::{error, info, warn};
use xagent_bus::BusError;
use xagent_proto::{Message, WORKSPACE};

use crate::daemon::{Daemon, internal};
use crate::session::LeasePhase;

/// Interval between retries of pending rows.
const RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// The state of a send as the caller sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SendStatus {
    /// JetStream acknowledged the publish.
    Published,
    /// In the local outbox; retried while the session stays open.
    QueuedForRetry,
    /// The server rejected it.
    Failed,
}

/// Whether a pending row may be published now.
enum Verdict {
    Publish,
    Defer,
    Fail(&'static str),
}

impl Daemon {
    /// Publishes one stored outbox row and records the outcome.
    pub(crate) async fn publish_row(
        &self,
        message_id: &str,
        subject: String,
        payload: Vec<u8>,
    ) -> (SendStatus, Option<String>) {
        let Some(bus) = self.bus().cloned() else {
            if let Err(err) = self.store().outbox_attempt(message_id, "nats_unavailable") {
                internal(&err);
            }
            return (SendStatus::QueuedForRetry, None);
        };
        match bus.publish(subject.clone(), message_id, payload).await {
            Ok(published) => {
                if let Err(err) = self
                    .store()
                    .outbox_published(message_id, published.sequence)
                {
                    internal(&err);
                }
                info!(
                    message_id,
                    subject,
                    stream_seq = published.sequence,
                    server_duplicate = published.duplicate,
                    "published"
                );
                (SendStatus::Published, None)
            }
            Err(BusError::Rejected(reason)) => {
                if let Err(err) = self
                    .store()
                    .outbox_fail(message_id, &format!("rejected by server: {reason}"))
                {
                    internal(&err);
                }
                error!(message_id, subject, reason, "publish rejected");
                (SendStatus::Failed, Some(reason))
            }
            Err(err) => {
                if let Err(store_err) = self.store().outbox_attempt(message_id, &err.to_string()) {
                    internal(&store_err);
                }
                warn!(message_id, subject, error = %err, "publish failed; queued for retry");
                (SendStatus::QueuedForRetry, None)
            }
        }
    }

    fn verdict(&self, sender_instance_id: &str) -> Verdict {
        let Some(session) = self.session_by_instance(sender_instance_id) else {
            return Verdict::Fail("sender_not_authorized: session ended before delivery");
        };
        match session.phase() {
            LeasePhase::Lost => Verdict::Fail("sender_not_authorized: lease lost"),
            LeasePhase::Held if session.may_send() => Verdict::Publish,
            // Reconnecting: wait until the lease is re-confirmed, lapses or is lost.
            _ => Verdict::Defer,
        }
    }

    /// Retries every pending row in creation order; stops at the first transient failure so
    /// order is kept.
    pub(crate) async fn flush_outbox(&self) {
        let pending = match self.store().pending_outbox() {
            Ok(pending) => pending,
            Err(err) => {
                internal(&err);
                return;
            }
        };
        for row in pending {
            match self.verdict(&row.sender_instance_id) {
                Verdict::Defer => {}
                Verdict::Fail(reason) => {
                    error!(
                        message_id = row.message_id,
                        reason, "outbox row not authorized"
                    );
                    if let Err(err) = self.store().outbox_fail(&row.message_id, reason) {
                        internal(&err);
                    }
                }
                Verdict::Publish => {
                    let (status, _) = self
                        .publish_row(&row.message_id, row.subject, row.payload)
                        .await;
                    if status == SendStatus::QueuedForRetry {
                        return;
                    }
                }
            }
        }
    }

    /// Retries pending rows periodically, and at once when kicked (NATS became ready).
    pub(crate) async fn outbox_loop(self: Arc<Self>) {
        loop {
            let _ = tokio::time::timeout(RETRY_INTERVAL, self.outbox_kick.notified()).await;
            if self.bus().is_some() {
                self.flush_outbox().await;
            }
        }
    }

    /// Stores `msg` with its outbox row, then publishes it.
    pub(crate) async fn send(
        &self,
        msg: &Message,
    ) -> Result<(SendStatus, Option<String>), xagent_proto::Error> {
        let payload = msg.encode()?;
        self.store()
            .record_outgoing(msg, &payload)
            .map_err(|err| internal(&err))?;
        let outcome = self
            .publish_row(&msg.id.to_string(), msg.subject(), payload)
            .await;
        info!(message_id = %msg.id, sender = %msg.sender.agent_id, workspace = WORKSPACE,
              recipient = ?msg.recipient.as_ref().map(ToString::to_string), status = ?outcome.0,
              "send");
        Ok(outcome)
    }
}
