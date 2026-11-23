//! Receive pipeline: pull -> validate -> SQLite commit -> ACK.
//!
//! A session pulls its agent's durable consumer only while it holds the lease. The lease is
//! exclusive across hosts, so at most one daemon anywhere pulls an agent's consumer; messages for
//! an offline agent wait in JetStream. A crash between commit and ACK means a redelivery, which
//! the store absorbs by message id.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, error, info, warn};
use xagent_bus::{Inbox, Incoming};
use xagent_proto::{Channel, Message, WORKSPACE, parse_subject};
use xagent_store::{Delivery, StoreError};

use crate::daemon::Daemon;
use crate::session::Session;
use crate::topics::is_subscribed;

/// Messages pulled per request.
const FETCH_BATCH: usize = 50;
/// Longest wait of one pull request.
const FETCH_WAIT: Duration = Duration::from_secs(2);
/// Below this much remaining authority, no pull is started.
const MIN_FETCH_WAIT: Duration = Duration::from_millis(50);
/// Back-off after a consumer error before binding again.
const RETRY_DELAY: Duration = Duration::from_secs(1);
/// Redelivery delay of a topic message while an uncertain join is reconciled.
const UNSETTLED_RETRY: Duration = Duration::from_secs(1);

impl Daemon {
    /// The consume loop of one session. Runs until the session's tasks are stopped.
    pub(crate) async fn receive(self: Arc<Self>, session: Arc<Session>, inbox: Inbox) {
        let mut consumer = Some(inbox);
        loop {
            let Some(bus) = self.bus().cloned() else {
                consumer = None;
                tokio::time::sleep(RETRY_DELAY).await;
                continue;
            };
            let wait = FETCH_WAIT.min(session.authority_remaining());
            if !session.may_consume() || wait <= MIN_FETCH_WAIT {
                tokio::time::sleep(MIN_FETCH_WAIT).await;
                continue;
            }
            let inbox = match &consumer {
                Some(inbox) => inbox,
                None => match self.bind_consumer(&bus, &session).await {
                    Ok(inbox) => consumer.insert(inbox),
                    Err(err) => {
                        warn!(agent = %session.agent, error = %err, "consumer bind failed");
                        tokio::time::sleep(RETRY_DELAY).await;
                        continue;
                    }
                },
            };
            let mut batch = match inbox.pull(FETCH_BATCH, wait).await {
                Ok(batch) => batch,
                Err(err) => {
                    warn!(agent = %session.agent, error = %err, "pull failed");
                    consumer = None;
                    tokio::time::sleep(RETRY_DELAY).await;
                    continue;
                }
            };
            while let Some(next) = batch.next().await {
                let msg = match next {
                    Ok(msg) => msg,
                    Err(err) => {
                        debug!(agent = %session.agent, error = %err, "pull batch ended");
                        break;
                    }
                };
                if !session.may_consume() {
                    // Authority ended mid-batch: hand the message back now instead of letting it
                    // wait out ack_wait; whoever holds the lease next receives it.
                    let _ = msg.nak().await;
                    continue;
                }
                self.process(&session, &msg).await;
            }
        }
    }

    async fn process(&self, session: &Session, msg: &Incoming) {
        let seq = msg.stream_sequence();
        let envelope = match Self::validate(session, msg) {
            Ok(envelope) => envelope,
            Err(reason) => {
                warn!(agent = %session.agent, stream_seq = seq, size = msg.payload().len(), reason,
                      "rejected inbound message");
                // Poison: never redeliver, never store.
                let _ = msg.term().await;
                return;
            }
        };
        // A topic message is judged against the subscription set under its lock, so a join or
        // leave in progress decides first.
        let mut subs = session.subscriptions.lock().await;
        if let (Channel::Chat, Some(topic)) = (envelope.channel, &envelope.topic) {
            if !self.settle(session, &mut subs).await {
                let _ = msg.nak_after(UNSETTLED_RETRY).await;
                return;
            }
            if !is_subscribed(&subs.topics, topic, seq) {
                // Unsubscribed while in flight, or from before the subscription's start.
                info!(agent = %session.agent, message_id = %envelope.id, topic, stream_seq = seq,
                      "dropped: not subscribed");
                let _ = msg.ack().await;
                return;
            }
        }
        let raw = std::str::from_utf8(msg.payload()).expect("validated as UTF-8");
        let stored = self
            .store()
            .record_incoming(&envelope, raw, &session.agent, seq);
        match stored {
            Ok(delivery) => {
                // Commit first, then ACK. A failed ACK only means a redelivery.
                if let Err(err) = msg.ack().await {
                    warn!(agent = %session.agent, message_id = %envelope.id, error = %err,
                          "ack failed; the message will be redelivered and deduplicated");
                }
                if delivery == Delivery::Stored {
                    self.message_stored(session);
                    info!(agent = %session.agent, message_id = %envelope.id,
                          sender = %envelope.sender.agent_id, stream_seq = seq,
                          size = msg.payload().len(), "received");
                } else {
                    debug!(agent = %session.agent, message_id = %envelope.id, "duplicate absorbed");
                }
            }
            Err(StoreError::IdCollision(id)) => {
                error!(agent = %session.agent, message_id = %id, stream_seq = seq,
                       "message id collision: different content under an existing id");
                let _ = msg.term().await;
            }
            Err(err) => {
                // Not stored: leave it unacknowledged for redelivery after ack_wait.
                error!(agent = %session.agent, error = %err, stream_seq = seq,
                       "storing a received message failed");
            }
        }
    }

    /// Decodes the envelope and checks it matches the subject it arrived on.
    fn validate(session: &Session, msg: &Incoming) -> Result<Message, String> {
        let envelope = Message::decode(msg.payload()).map_err(|e| e.to_string())?;
        let (channel, workspace, rest) = parse_subject(msg.subject()).map_err(|e| e.to_string())?;
        if envelope.workspace != workspace || envelope.subject() != msg.subject() {
            return Err("envelope does not match its subject".to_owned());
        }
        if channel == Channel::Dm && (workspace != WORKSPACE || rest != session.agent.as_str()) {
            return Err("DM delivered to the wrong agent".to_owned());
        }
        Ok(envelope)
    }
}
