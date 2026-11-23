//! Bounded reads of the message stream through short-lived, server-expiring consumers: topic
//! history and inbox recovery.

use std::collections::VecDeque;
use std::time::Duration;

use async_nats::jetstream::{
    self,
    consumer::{AckPolicy, DeliverPolicy, PullConsumer, pull},
};
use futures_util::StreamExt;

use crate::{Bus, BusError, STREAM};

/// Messages fetched per request while scanning.
const SCAN_BATCH: u64 = 500;
/// Longest wait of one scan fetch.
const SCAN_WAIT: Duration = Duration::from_secs(2);
/// The server removes a scan consumer this long after it was last used.
const SCAN_INACTIVE: Duration = Duration::from_secs(30);

/// One message read from the stream.
#[derive(Debug, Clone)]
pub struct Scanned {
    pub subject: String,
    pub payload: Vec<u8>,
    pub sequence: u64,
}

impl Bus {
    async fn stream(&self) -> Result<jetstream::stream::Stream, BusError> {
        self.js.get_stream(STREAM).await.map_err(BusError::nats)
    }

    /// An ephemeral, in-memory, unacknowledged consumer over `filters` from `deliver_policy`.
    async fn scan_consumer(
        stream: &jetstream::stream::Stream,
        filters: Vec<String>,
        deliver_policy: DeliverPolicy,
    ) -> Result<PullConsumer, BusError> {
        let (filter_subject, filter_subjects) = match <[String; 1]>::try_from(filters) {
            Ok([one]) => (one, Vec::new()),
            Err(many) => (String::new(), many),
        };
        stream
            .create_consumer(pull::Config {
                ack_policy: AckPolicy::None,
                deliver_policy,
                filter_subject,
                filter_subjects,
                inactive_threshold: SCAN_INACTIVE,
                memory_storage: true,
                ..Default::default()
            })
            .await
            .map_err(BusError::nats)
    }

    /// Reads what `consumer` had pending when it was created, at most `max` messages, then
    /// deletes it.
    async fn drain(
        stream: &jetstream::stream::Stream,
        consumer: PullConsumer,
        max: usize,
    ) -> Result<Vec<Scanned>, BusError> {
        let name = consumer.cached_info().name.clone();
        let total = usize::try_from(consumer.cached_info().num_pending).unwrap_or(usize::MAX);
        let want = total.min(max);
        let mut out = Vec::with_capacity(want);
        let result = async {
            while out.len() < want {
                let batch = (want - out.len()).min(usize::try_from(SCAN_BATCH).unwrap_or(500));
                let mut messages = consumer
                    .fetch()
                    .max_messages(batch)
                    .expires(SCAN_WAIT)
                    .messages()
                    .await
                    .map_err(BusError::nats)?;
                let before = out.len();
                while let Some(msg) = messages.next().await {
                    let msg = msg.map_err(BusError::nats)?;
                    let sequence = msg.info().map(|i| i.stream_sequence).unwrap_or_default();
                    out.push(Scanned {
                        subject: msg.subject.to_string(),
                        payload: msg.payload.to_vec(),
                        sequence,
                    });
                }
                if out.len() == before {
                    break; // nothing more arrived in time
                }
            }
            Ok(())
        }
        .await;
        if let Err(err) = stream.delete_consumer(&name).await {
            tracing::debug!(consumer = name, error = %err, "scan consumer left to expire");
        }
        result.map(|()| out)
    }

    /// Stored messages on `subject` with sequence >= `start` (a server-side count).
    async fn pending_from(
        stream: &jetstream::stream::Stream,
        subject: &str,
        start: u64,
    ) -> Result<u64, BusError> {
        let probe = Self::scan_consumer(
            stream,
            vec![subject.to_owned()],
            DeliverPolicy::ByStartSequence {
                start_sequence: start,
            },
        )
        .await?;
        let info = probe.cached_info();
        let pending = info.num_pending;
        let _ = stream.delete_consumer(&info.name).await;
        Ok(pending)
    }

    /// The newest `limit` messages on one subject, oldest first.
    ///
    /// JetStream cannot iterate a filtered subject backwards, so the start sequence is found by
    /// binary search over server-side pending counts (O(log stream length) probes), then exactly
    /// that window is fetched. The cost does not grow with the topic's size.
    pub async fn subject_tail(
        &self,
        subject: &str,
        limit: usize,
    ) -> Result<Vec<Scanned>, BusError> {
        let stream = self.stream().await?;
        let state = &stream.cached_info().state;
        let (first, last) = (state.first_sequence.max(1), state.last_sequence);
        if last < first || limit == 0 {
            return Ok(Vec::new());
        }
        let total = Self::pending_from(&stream, subject, first).await?;
        if total == 0 {
            return Ok(Vec::new());
        }
        let want = u64::try_from(limit).unwrap_or(u64::MAX);
        // Invariant: pending(lo) >= limit, or lo == first.
        let (mut lo, mut hi) = (first, last);
        if total > want {
            while lo < hi {
                let mid = lo + (hi - lo).div_ceil(2);
                if Self::pending_from(&stream, subject, mid).await? >= want {
                    lo = mid;
                } else {
                    hi = mid - 1;
                }
            }
        }
        let consumer = Self::scan_consumer(
            &stream,
            vec![subject.to_owned()],
            DeliverPolicy::ByStartSequence { start_sequence: lo },
        )
        .await?;
        let mut window: VecDeque<Scanned> =
            Self::drain(&stream, consumer, usize::MAX).await?.into();
        while window.len() > limit {
            window.pop_front();
        }
        Ok(window.into())
    }

    /// Messages on `filters` stored during the last `window`, oldest first, at most `max`.
    pub async fn scan_recent(
        &self,
        filters: Vec<String>,
        window: Duration,
        max: usize,
    ) -> Result<Vec<Scanned>, BusError> {
        let stream = self.stream().await?;
        let start_time = time::OffsetDateTime::now_utc() - window;
        let consumer =
            Self::scan_consumer(&stream, filters, DeliverPolicy::ByStartTime { start_time })
                .await?;
        Self::drain(&stream, consumer, max).await
    }
}
