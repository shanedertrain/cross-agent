//! Local durable state of one xagent daemon: received and sent messages, per-agent delivery and
//! read state, and the outbox of messages waiting to be published.
//!
//! JetStream ACK state is transport state; `deliveries.read_at` is reading state. Neither is
//! derived from the other. The schema has one version and no migrations: a database written by a
//! different schema version is refused, and unread state is rebuilt from JetStream instead.

use std::collections::BTreeMap;
use std::fs;
use std::os::unix::fs::{DirBuilderExt, PermissionsExt};
use std::path::Path;

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, params_from_iter};
use xagent_proto::{AgentId, Channel, Message, MessageId, now_ts};

/// Bump on any schema change; an existing database of another version is refused.
const SCHEMA_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    subject TEXT NOT NULL,
    channel TEXT NOT NULL CHECK (channel IN ('chat', 'dm')),
    topic TEXT,
    recipient_agent_id TEXT,
    kind TEXT NOT NULL,
    sender_agent_id TEXT NOT NULL,
    sender_instance_id TEXT NOT NULL,
    sender_client TEXT NOT NULL,
    sent_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    reply_to TEXT,
    priority TEXT NOT NULL,
    summary TEXT NOT NULL,
    body_bytes INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    stream_sequence INTEGER
) STRICT;
CREATE INDEX messages_thread_sent ON messages (thread_id, sent_at);
CREATE INDEX messages_received ON messages (received_at);
CREATE INDEX messages_topic_sent ON messages (topic, sent_at) WHERE channel = 'chat';
CREATE INDEX messages_reply_to ON messages (reply_to, sender_agent_id);

CREATE TABLE deliveries (
    agent_id TEXT NOT NULL,
    message_id TEXT NOT NULL REFERENCES messages (id),
    direction TEXT NOT NULL CHECK (direction IN ('in', 'out')),
    received_at TEXT NOT NULL,
    read_at TEXT,
    PRIMARY KEY (agent_id, message_id)
) STRICT;
CREATE INDEX deliveries_unread ON deliveries (agent_id, read_at);
CREATE INDEX deliveries_message ON deliveries (message_id);

CREATE TABLE outbox (
    message_id TEXT PRIMARY KEY REFERENCES messages (id),
    sender_agent_id TEXT NOT NULL,
    sender_instance_id TEXT NOT NULL,
    subject TEXT NOT NULL,
    payload BLOB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    published_at TEXT,
    stream_sequence INTEGER
) STRICT;
CREATE INDEX outbox_status ON outbox (status, created_at);
";

/// A failure of the local store.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("creating the state directory: {0}")]
    Io(#[from] std::io::Error),
    #[error(
        "state database has schema version {found}, this build uses {SCHEMA_VERSION}; \
         remove it and recover unread state from JetStream"
    )]
    SchemaVersion { found: i64 },
    #[error("message id {0} already stored with different content")]
    IdCollision(MessageId),
}

/// Whether a received message was new to this agent's inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Delivery {
    Stored,
    Duplicate,
}

/// Unread counters of one agent's inbox.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct InboxCounts {
    pub unread_total: u64,
    pub dm_unread: u64,
    pub high_priority: u64,
    pub urgent: u64,
    /// Unread chat messages per topic.
    pub by_topic: BTreeMap<String, u64>,
    /// Read questions, handoffs and review requests from others that this agent has not replied to.
    pub awaiting_reply: u64,
}

/// What a retention pass deleted (or would delete, on a dry run).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GcCounts {
    pub deliveries: u64,
    pub outbox: u64,
    pub messages: u64,
}

/// Message kinds that ask the reader for something back (a reply obligation).
const REPLY_KINDS: &str = "'question', 'handoff', 'review_request'";

/// Header of one inbox entry: everything but the body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxItem {
    pub id: String,
    pub channel: Channel,
    pub topic: Option<String>,
    pub recipient: Option<String>,
    pub sender: String,
    pub sent_at: String,
    pub kind: String,
    pub priority: String,
    pub summary: String,
    pub body_bytes: u64,
    pub thread_id: String,
    pub reply_to: Option<String>,
    pub read: bool,
}

/// Which inbox entries to list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFilter {
    UnreadOnly,
    All,
}

/// A pending outbox row with what is needed to authorize and publish it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingSend {
    pub message_id: String,
    pub sender_instance_id: String,
    pub subject: String,
    pub payload: Vec<u8>,
}

/// The daemon's SQLite database.
#[derive(Debug)]
pub struct Store {
    conn: Connection,
}

impl Store {
    /// Opens (creating if needed) the database at `path`, in an owner-only directory.
    ///
    /// # Errors
    ///
    /// I/O and SQLite failures, and [`StoreError::SchemaVersion`] for a database of another schema.
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        if let Some(dir) = path.parent() {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(dir)?;
        }
        let conn = Connection::open(path)?;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
        Self::init(conn)
    }

    fn init(mut conn: Connection) -> Result<Self, StoreError> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "FULL")?;
        conn.pragma_update(None, "foreign_keys", true)?;
        conn.busy_timeout(std::time::Duration::from_secs(5))?;
        let version: i64 = conn.pragma_query_value(None, "user_version", |r| r.get(0))?;
        match version {
            0 => {
                let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
                tx.execute_batch(SCHEMA)?;
                tx.pragma_update(None, "user_version", SCHEMA_VERSION)?;
                tx.commit()?;
            }
            SCHEMA_VERSION => {}
            found => return Err(StoreError::SchemaVersion { found }),
        }
        Ok(Self { conn })
    }

    /// Stores an outbound message, the sender's own (already read) copy and its pending outbox row,
    /// in one transaction.
    pub fn record_outgoing(&mut self, msg: &Message, payload: &[u8]) -> Result<(), StoreError> {
        let raw = std::str::from_utf8(payload).expect("encoded envelopes are UTF-8");
        let now = now_ts();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        insert_message(&tx, msg, raw, None, &now)?;
        tx.execute(
            "INSERT OR IGNORE INTO deliveries (agent_id, message_id, direction, received_at, read_at)
             VALUES (?1, ?2, 'out', ?3, ?3)",
            params![msg.sender.agent_id.as_str(), msg.id.to_string(), now],
        )?;
        tx.execute(
            "INSERT INTO outbox (message_id, sender_agent_id, sender_instance_id, subject, payload,
                                 status, attempts, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, 'pending', 0, ?6, ?6)",
            params![
                msg.id.to_string(),
                msg.sender.agent_id.as_str(),
                msg.sender.instance_id.to_string(),
                msg.subject(),
                payload,
                now
            ],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Stores a received message and its delivery to `agent` in one transaction.
    ///
    /// A redelivery of a stored message is absorbed ([`Delivery::Duplicate`]); the same id with
    /// different content is refused with [`StoreError::IdCollision`] and writes nothing.
    pub fn record_incoming(
        &mut self,
        msg: &Message,
        raw: &str,
        agent: &AgentId,
        stream_sequence: u64,
    ) -> Result<Delivery, StoreError> {
        let now = now_ts();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        check_collision(&tx, msg, raw)?;
        insert_message(&tx, msg, raw, Some(stream_sequence), &now)?;
        let inserted = tx.execute(
            "INSERT OR IGNORE INTO deliveries (agent_id, message_id, direction, received_at, read_at)
             VALUES (?1, ?2, 'in', ?3, NULL)",
            params![agent.as_str(), msg.id.to_string(), now],
        )?;
        tx.commit()?;
        Ok(if inserted == 1 {
            Delivery::Stored
        } else {
            Delivery::Duplicate
        })
    }

    /// Stores a message seen outside any inbox (topic history), so it can be read and replied to.
    pub fn cache_message(
        &mut self,
        msg: &Message,
        raw: &str,
        stream_sequence: u64,
    ) -> Result<(), StoreError> {
        let now = now_ts();
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        check_collision(&tx, msg, raw)?;
        insert_message(&tx, msg, raw, Some(stream_sequence), &now)?;
        tx.commit()?;
        Ok(())
    }

    /// Unread counters of `agent`'s inbox.
    pub fn inbox_counts(&self, agent: &AgentId) -> Result<InboxCounts, StoreError> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT m.channel, m.topic, m.priority, COUNT(*)
             FROM deliveries d JOIN messages m ON m.id = d.message_id
             WHERE d.agent_id = ?1 AND d.read_at IS NULL AND d.direction = 'in'
             GROUP BY m.channel, m.topic, m.priority",
        )?;
        let mut counts = InboxCounts::default();
        let rows = stmt.query_map([agent.as_str()], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, u64>(3)?,
            ))
        })?;
        for row in rows {
            let (channel, topic, priority, n) = row?;
            counts.unread_total += n;
            match (channel.as_str(), topic) {
                ("dm", _) => counts.dm_unread += n,
                (_, Some(topic)) => *counts.by_topic.entry(topic).or_default() += n,
                (_, None) => {}
            }
            match priority.as_str() {
                "high" => counts.high_priority += n,
                "urgent" => counts.urgent += n,
                _ => {}
            }
        }
        counts.awaiting_reply = self.awaiting_reply(agent)?;
        Ok(counts)
    }

    /// Messages delivered to `agent` and read, of a kind that asks for something back, not sent
    /// by `agent` itself, that no message from `agent` replies to.
    fn awaiting_reply(&self, agent: &AgentId) -> Result<u64, StoreError> {
        let sql = format!(
            "SELECT COUNT(*) FROM deliveries d JOIN messages m ON m.id = d.message_id
             WHERE d.agent_id = ?1 AND d.direction = 'in' AND d.read_at IS NOT NULL
               AND m.kind IN ({REPLY_KINDS}) AND m.sender_agent_id != ?1
               AND NOT EXISTS (SELECT 1 FROM messages r
                               WHERE r.reply_to = m.id AND r.sender_agent_id = ?1)"
        );
        Ok(self
            .conn
            .prepare_cached(&sql)?
            .query_row([agent.as_str()], |r| r.get(0))?)
    }

    /// Inbox headers of `agent`, oldest first, at most `limit`; only those of `topic` and of
    /// `channel` when given.
    pub fn inbox_list(
        &self,
        agent: &AgentId,
        filter: ReadFilter,
        topic: Option<&str>,
        channel: Option<Channel>,
        limit: usize,
    ) -> Result<Vec<InboxItem>, StoreError> {
        let unread_clause = match filter {
            ReadFilter::UnreadOnly => "AND d.read_at IS NULL",
            ReadFilter::All => "",
        };
        let sql = format!(
            "SELECT m.id, m.channel, m.topic, m.recipient_agent_id, m.sender_agent_id, m.sent_at,
                    m.kind, m.priority, m.summary, m.body_bytes, m.thread_id, m.reply_to,
                    d.read_at IS NOT NULL
             FROM deliveries d JOIN messages m ON m.id = d.message_id
             WHERE d.agent_id = ?1 AND d.direction = 'in' {unread_clause}
               AND (?3 IS NULL OR m.topic = ?3) AND (?4 IS NULL OR m.channel = ?4)
             ORDER BY m.sent_at ASC, d.received_at ASC
             LIMIT ?2"
        );
        let mut stmt = self.conn.prepare_cached(&sql)?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let channel = channel.map(Channel::as_str);
        let rows = stmt.query_map(params![agent.as_str(), limit, topic, channel], |r| {
            let channel: String = r.get(1)?;
            Ok(InboxItem {
                id: r.get(0)?,
                channel: if channel == "dm" {
                    Channel::Dm
                } else {
                    Channel::Chat
                },
                topic: r.get(2)?,
                recipient: r.get(3)?,
                sender: r.get(4)?,
                sent_at: r.get(5)?,
                kind: r.get(6)?,
                priority: r.get(7)?,
                summary: r.get(8)?,
                body_bytes: r.get(9)?,
                thread_id: r.get(10)?,
                reply_to: r.get(11)?,
                read: r.get(12)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Raw envelopes `agent` may read, keyed by id: its own inbox (sent or received), and any chat
    /// message stored on this host. DMs are visible only to their sender and recipient.
    pub fn visible_messages(
        &self,
        agent: &AgentId,
        ids: &[String],
    ) -> Result<BTreeMap<String, String>, StoreError> {
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let marks = vec!["?"; ids.len()].join(",");
        let sql = format!(
            "SELECT m.id, m.payload_json
             FROM messages m
             LEFT JOIN deliveries d ON d.message_id = m.id AND d.agent_id = ?
             WHERE m.id IN ({marks}) AND (d.message_id IS NOT NULL OR m.channel = 'chat')"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let args = std::iter::once(agent.as_str()).chain(ids.iter().map(String::as_str));
        let rows = stmt.query_map(params_from_iter(args), |r| Ok((r.get(0)?, r.get(1)?)))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Raw envelopes of one thread visible to `agent`, in stream order (messages not yet published
    /// last), at most `limit`.
    pub fn thread_messages(
        &self,
        agent: &AgentId,
        thread_id: &str,
        limit: usize,
    ) -> Result<Vec<String>, StoreError> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT m.payload_json
             FROM messages m
             LEFT JOIN deliveries d ON d.message_id = m.id AND d.agent_id = ?1
             WHERE m.thread_id = ?2 AND (d.message_id IS NOT NULL OR m.channel = 'chat')
             ORDER BY COALESCE(m.stream_sequence, 9223372036854775807) ASC, m.sent_at ASC
             LIMIT ?3",
        )?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let rows = stmt.query_map(params![agent.as_str(), thread_id, limit], |r| r.get(0))?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// The newest `limit` stored messages of a topic (raw envelopes), oldest first.
    pub fn local_topic_history(
        &self,
        topic: &str,
        limit: usize,
    ) -> Result<Vec<String>, StoreError> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT payload_json FROM messages WHERE channel = 'chat' AND topic = ?1
             ORDER BY sent_at DESC LIMIT ?2",
        )?;
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        let mut rows = stmt
            .query_map(params![topic, limit], |r| r.get(0))?
            .collect::<Result<Vec<String>, _>>()?;
        rows.reverse();
        Ok(rows)
    }

    /// Deletes every local copy received before `cutoff` (deliveries read or not, outbox rows that
    /// are no longer pending, and messages no pending outbox row needs). A dry run deletes nothing.
    pub fn gc(&mut self, cutoff: &str, dry_run: bool) -> Result<GcCounts, StoreError> {
        let tx = self
            .conn
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let deliveries = tx.execute("DELETE FROM deliveries WHERE received_at < ?1", [cutoff])?;
        let outbox = tx.execute(
            "DELETE FROM outbox WHERE status != 'pending' AND created_at < ?1",
            [cutoff],
        )?;
        let messages = tx.execute(
            "DELETE FROM messages WHERE received_at < ?1
               AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.message_id = messages.id)
               AND NOT EXISTS (SELECT 1 FROM deliveries d WHERE d.message_id = messages.id)",
            [cutoff],
        )?;
        if dry_run {
            tx.rollback()?;
        } else {
            tx.commit()?;
        }
        let count = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
        Ok(GcCounts {
            deliveries: count(deliveries),
            outbox: count(outbox),
            messages: count(messages),
        })
    }

    /// Marks the given messages read in `agent`'s inbox. Returns how many changed.
    pub fn mark_read(&self, agent: &AgentId, ids: &[String]) -> Result<usize, StoreError> {
        if ids.is_empty() {
            return Ok(0);
        }
        let marks = vec!["?"; ids.len()].join(",");
        let sql = format!(
            "UPDATE deliveries SET read_at = ? WHERE agent_id = ? AND read_at IS NULL
             AND message_id IN ({marks})"
        );
        let now = now_ts();
        let args = [now.as_str(), agent.as_str()]
            .into_iter()
            .chain(ids.iter().map(String::as_str));
        Ok(self.conn.execute(&sql, params_from_iter(args))?)
    }

    /// Pending outbox rows in creation order.
    pub fn pending_outbox(&self) -> Result<Vec<PendingSend>, StoreError> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT message_id, sender_instance_id, subject, payload FROM outbox
             WHERE status = 'pending' ORDER BY created_at ASC, rowid ASC",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(PendingSend {
                message_id: r.get(0)?,
                sender_instance_id: r.get(1)?,
                subject: r.get(2)?,
                payload: r.get(3)?,
            })
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }

    /// Records a failed publish attempt that will be retried.
    pub fn outbox_attempt(&self, message_id: &str, error: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE outbox SET attempts = attempts + 1, last_error = ?1, updated_at = ?2
             WHERE message_id = ?3",
            params![error, now_ts(), message_id],
        )?;
        Ok(())
    }

    /// Records a publish the stream acknowledged at `stream_sequence`.
    pub fn outbox_published(
        &mut self,
        message_id: &str,
        stream_sequence: u64,
    ) -> Result<(), StoreError> {
        let now = now_ts();
        let tx = self.conn.transaction()?;
        tx.execute(
            "UPDATE outbox SET status = 'published', attempts = attempts + 1, last_error = NULL,
                    updated_at = ?1, published_at = ?1, stream_sequence = ?2
             WHERE message_id = ?3",
            params![now, stream_sequence, message_id],
        )?;
        tx.execute(
            "UPDATE messages SET stream_sequence = COALESCE(stream_sequence, ?1) WHERE id = ?2",
            params![stream_sequence, message_id],
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Fails one pending row for good.
    pub fn outbox_fail(&self, message_id: &str, reason: &str) -> Result<(), StoreError> {
        self.conn.execute(
            "UPDATE outbox SET status = 'failed', attempts = attempts + 1, last_error = ?1,
                    updated_at = ?2
             WHERE message_id = ?3 AND status = 'pending'",
            params![reason, now_ts(), message_id],
        )?;
        Ok(())
    }

    /// Fails every pending row queued by one session instance. Returns how many.
    pub fn fail_pending_for_instance(
        &self,
        instance_id: &str,
        reason: &str,
    ) -> Result<usize, StoreError> {
        Ok(self.conn.execute(
            "UPDATE outbox SET status = 'failed', last_error = ?1, updated_at = ?2
             WHERE status = 'pending' AND sender_instance_id = ?3",
            params![reason, now_ts(), instance_id],
        )?)
    }

    /// Fails every pending row (sessions of a previous daemon run). Returns how many.
    pub fn fail_all_pending(&self, reason: &str) -> Result<usize, StoreError> {
        Ok(self.conn.execute(
            "UPDATE outbox SET status = 'failed', last_error = ?1, updated_at = ?2
             WHERE status = 'pending'",
            params![reason, now_ts()],
        )?)
    }

    /// Outbox rows per status, optionally only those of one sender.
    pub fn outbox_counts(
        &self,
        sender: Option<&AgentId>,
    ) -> Result<BTreeMap<String, u64>, StoreError> {
        let mut stmt = self.conn.prepare_cached(
            "SELECT status, COUNT(*) FROM outbox
             WHERE ?1 IS NULL OR sender_agent_id = ?1 GROUP BY status",
        )?;
        let rows = stmt.query_map([sender.map(AgentId::as_str)], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;
        Ok(rows.collect::<Result<_, _>>()?)
    }
}

/// Refuses a message whose id is already stored with different content.
fn check_collision(
    tx: &rusqlite::Transaction<'_>,
    msg: &Message,
    raw: &str,
) -> Result<(), StoreError> {
    let existing: Option<String> = tx
        .query_row(
            "SELECT payload_json FROM messages WHERE id = ?1",
            [msg.id.to_string()],
            |r| r.get(0),
        )
        .optional()?;
    if existing.as_deref().is_some_and(|stored| stored != raw) {
        return Err(StoreError::IdCollision(msg.id));
    }
    Ok(())
}

fn insert_message(
    tx: &rusqlite::Transaction<'_>,
    msg: &Message,
    raw: &str,
    stream_sequence: Option<u64>,
    now: &str,
) -> Result<(), StoreError> {
    tx.execute(
        "INSERT INTO messages (id, subject, channel, topic, recipient_agent_id, kind,
             sender_agent_id, sender_instance_id, sender_client, sent_at, received_at, thread_id,
             reply_to, priority, summary, body_bytes, payload_json, stream_sequence)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
         ON CONFLICT (id) DO UPDATE
             SET stream_sequence = COALESCE(messages.stream_sequence, excluded.stream_sequence)",
        params![
            msg.id.to_string(),
            msg.subject(),
            msg.channel.as_str(),
            msg.topic,
            msg.recipient.as_ref().map(AgentId::as_str),
            msg.kind.as_str(),
            msg.sender.agent_id.as_str(),
            msg.sender.instance_id.to_string(),
            msg.sender.client,
            msg.sent_at,
            now,
            msg.thread_id.to_string(),
            msg.reply_to.map(|id| id.to_string()),
            msg.priority.as_str(),
            msg.summary,
            msg.body.len(),
            raw,
            stream_sequence,
        ],
    )?;
    Ok(())
}
