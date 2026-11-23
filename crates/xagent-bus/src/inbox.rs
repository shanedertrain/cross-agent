//! Pulling an agent's durable consumer and acknowledging what was pulled.

use std::time::Duration;

use async_nats::jetstream::{self, AckKind, consumer::PullConsumer};
use futures_util::StreamExt;

use crate::BusError;

/// An agent's durable pull consumer.
#[derive(Debug, Clone)]
pub struct Inbox {
    consumer: PullConsumer,
}

impl Inbox {
    pub(crate) fn new(consumer: PullConsumer) -> Self {
        Self { consumer }
    }

    /// Starts one pull request for up to `max` messages, waiting at most `wait` for them.
    pub async fn pull(&self, max: usize, wait: Duration) -> Result<Pull, BusError> {
        let batch = self
            .consumer
            .batch()
            .max_messages(max)
            .expires(wait)
            .messages()
            .await
            .map_err(BusError::nats)?;
        Ok(Pull { batch })
    }
}

/// The messages of one pull request, yielded as they arrive.
pub struct Pull {
    batch: jetstream::consumer::pull::Batch,
}

impl std::fmt::Debug for Pull {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pull").finish_non_exhaustive()
    }
}

impl Pull {
    /// The next message; `None` once the request is complete or expired.
    pub async fn next(&mut self) -> Option<Result<Incoming, BusError>> {
        let next = self.batch.next().await?;
        Some(next.map(|msg| Incoming { msg }).map_err(BusError::nats))
    }
}

/// One pulled message. Unacknowledged messages are redelivered after the consumer's ack wait.
#[derive(Debug)]
pub struct Incoming {
    msg: jetstream::Message,
}

impl Incoming {
    pub fn subject(&self) -> &str {
        self.msg.subject.as_str()
    }

    pub fn payload(&self) -> &[u8] {
        &self.msg.payload
    }

    /// The message's sequence in the stream.
    pub fn stream_sequence(&self) -> u64 {
        self.msg
            .info()
            .map(|i| i.stream_sequence)
            .unwrap_or_default()
    }

    /// Acknowledges and waits for the server to confirm it.
    pub async fn ack(&self) -> Result<(), BusError> {
        self.msg.double_ack().await.map_err(BusError::Nats)
    }

    /// Hands the message back for immediate redelivery.
    pub async fn nak(&self) -> Result<(), BusError> {
        self.msg
            .ack_with(AckKind::Nak(None))
            .await
            .map_err(BusError::Nats)
    }

    /// Hands the message back for redelivery after `delay`.
    pub async fn nak_after(&self, delay: Duration) -> Result<(), BusError> {
        self.msg
            .ack_with(AckKind::Nak(Some(delay)))
            .await
            .map_err(BusError::Nats)
    }

    /// Never redeliver: the message is poison.
    pub async fn term(&self) -> Result<(), BusError> {
        self.msg
            .ack_with(AckKind::Term)
            .await
            .map_err(BusError::Nats)
    }
}
