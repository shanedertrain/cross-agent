//! Client-facing reminders: the wake hook point, the Stop hook check and the `SendMessage` mirror.

use std::sync::{Arc, Weak};
use std::time::Instant;

use serde_json::{Value, json};
use xagent_bus::BusError;
use xagent_proto::{Client, Error, Kind, Priority, TopicRecord, WORKSPACE, now_ts};
use xagent_wake::{Counts, Target, WakeSource};

use crate::daemon::{Daemon, internal};
use crate::protocol::{MessageSend, SendmessageMirror, StopCheck};
use crate::session::Session;
use crate::topics::nats_error;

/// The topic Claude Code `SendMessage` traffic is mirrored to.
const MIRROR_TOPIC: &str = "sendmessage";

/// Per-session Stop-hook bookkeeping, shared with the wake policy.
#[derive(Debug)]
pub(crate) struct Notice {
    /// Inbox state at the last reminder (`-1`: none, or the inbox was empty since).
    mark: i64,
    /// When that reminder was made.
    reminded_at: Option<Instant>,
    /// When a message was last stored for the session.
    last_delivery: Option<Instant>,
}

impl Default for Notice {
    fn default() -> Self {
        Self {
            mark: -1,
            reminded_at: None,
            last_delivery: None,
        }
    }
}

/// The inbox state a reminder reports: unread count and owed replies in one number.
fn inbox_state(unread: u64, owed: u64) -> i64 {
    i64::try_from(unread * 1000 + owed.min(999)).unwrap_or(i64::MAX)
}

impl Daemon {
    /// The hook point: a message was stored for `session`'s agent.
    pub(crate) fn message_stored(&self, session: &Session) {
        session
            .notice
            .lock()
            .expect("notice lock poisoned")
            .last_delivery = Some(Instant::now());
        self.waker.delivered(target_of(session));
    }

    fn counts_of(&self, session: &Session) -> Result<Counts, Error> {
        let counts = self
            .store()
            .inbox_counts(&session.agent)
            .map_err(|err| internal(&err))?;
        Ok(Counts {
            unread_total: counts.unread_total,
            dm_unread: counts.dm_unread,
            awaiting_reply: counts.awaiting_reply,
            waiting: None,
        })
    }

    fn session_of(
        &self,
        conversation: &xagent_proto::Conversation,
        clients: &[xagent_proto::Client],
    ) -> Option<Arc<Session>> {
        self.sessions().into_iter().find(|s| {
            !s.is_closing() && s.conversation == *conversation && clients.contains(&s.client)
        })
    }

    /// For a Stop hook: remind once per inbox state, and only a conversation that holds a session
    /// (an unbound window shares its directory's identity with unrelated windows, so reminding it
    /// would block every one of them).
    pub(crate) fn stop_check(&self, params: &StopCheck) -> Result<Value, Error> {
        let Some(session) = self.session_of(&params.conversation, &params.clients) else {
            return Ok(json!({ "remind": false }));
        };
        let counts = self.counts_of(&session)?;
        let state = inbox_state(counts.unread_total, counts.awaiting_reply);
        let mut notice = session.notice.lock().expect("notice lock poisoned");
        let remind = state > 0 && state != notice.mark;
        if remind {
            notice.mark = state;
            notice.reminded_at = Some(Instant::now());
        } else if state == 0 {
            notice.mark = -1;
        }
        Ok(json!({
            "remind": remind,
            "workspace": WORKSPACE,
            "agent_id": session.agent.as_str(),
            "status": {
                "unread_total": counts.unread_total,
                "dm_unread": counts.dm_unread,
                "awaiting_reply": counts.awaiting_reply,
            },
        }))
    }

    /// For the `SendMessage` hook: the send is logged on the bus by the conversation's own session
    /// (the hook's process cannot: the daemon holds the lease), to the `sendmessage` topic, created
    /// on first use. Nobody subscribes to it by default; `topic_history` reads it.
    pub(crate) async fn sendmessage_mirror(
        &self,
        params: SendmessageMirror,
    ) -> Result<Value, Error> {
        let Some(session) = self.session_of(&params.conversation, &[Client::ClaudeCode]) else {
            return Ok(
                json!({ "mirrored": false, "reason": "conversation has no bound xagent session" }),
            );
        };
        let bus = self.require_bus("mirroring a SendMessage")?.clone();
        let record = TopicRecord {
            workspace: WORKSPACE.to_owned(),
            name: MIRROR_TOPIC.to_owned(),
            description: "Claude Code SendMessage traffic, mirrored by the PostToolUse hook"
                .to_owned(),
            created_by: session.agent.to_string(),
            created_at: now_ts(),
            archived: false,
        };
        match bus.create_topic(&record).await {
            Ok(()) | Err(BusError::Exists) => {}
            Err(err) => return Err(nats_error("creating the sendmessage topic", &err)),
        }
        let mirror = MessageSend {
            topic: MIRROR_TOPIC.to_owned(),
            summary: mirror_summary(&params.to, &params.message),
            body: mirror_body(&params.message, xagent_proto::DEFAULT_MAX_MESSAGE_BYTES).to_owned(),
            kind: Kind::Status,
            priority: Priority::Low,
            mentions: Vec::new(),
            refs: Vec::new(),
        };
        let sent = self.message_send(&session, mirror).await?;
        Ok(json!({ "mirrored": true, "id": sent["id"], "topic": MIRROR_TOPIC }))
    }
}

fn target_of(session: &Session) -> Target {
    Target {
        key: session.agent.to_string(),
        workspace: WORKSPACE.to_owned(),
        agent: session.agent.to_string(),
        client: session.client,
        conversation: session.conversation.clone(),
        claude_pid: session.claude_pid,
    }
}

/// The wake policy's view of the daemon.
#[derive(Debug)]
pub(crate) struct WakeView(Weak<Daemon>);

impl WakeView {
    pub(crate) fn new(daemon: Weak<Daemon>) -> Self {
        Self(daemon)
    }

    fn session(&self, target: &Target) -> Option<Arc<Session>> {
        self.0
            .upgrade()?
            .sessions()
            .into_iter()
            .find(|s| s.agent.as_str() == target.agent && !s.is_closing())
    }

    /// Messages waiting for the identity of an unbound window, at the last peek.
    fn waiting(&self, target: &Target) -> Option<u64> {
        self.0.upgrade()?.waiting(&target.agent)
    }
}

impl WakeSource for WakeView {
    /// A bound session, or a window that would bind the identity.
    fn is_open(&self, target: &Target) -> bool {
        self.session(target).is_some() || self.waiting(target).is_some()
    }

    fn counts(&self, target: &Target) -> Option<Counts> {
        if let Some(session) = self.session(target) {
            return self.0.upgrade()?.counts_of(&session).ok();
        }
        let waiting = self.waiting(target)?;
        Some(Counts {
            unread_total: waiting,
            dm_unread: 0,
            awaiting_reply: 0,
            waiting: Some(waiting),
        })
    }

    /// The Stop hook reminded exactly this state, and no message arrived after the reminder
    /// (equal counts can hide a new message that replaced one read meanwhile).
    fn already_told(&self, target: &Target, counts: &Counts) -> bool {
        let Some(session) = self.session(target) else {
            return false;
        };
        let notice = session.notice.lock().expect("notice lock poisoned");
        let state = inbox_state(counts.unread_total, counts.awaiting_reply);
        notice.mark == state
            && notice
                .reminded_at
                .is_some_and(|at| notice.last_delivery.is_none_or(|last| at >= last))
    }
}

/// Summary of a mirrored `SendMessage`: one line, control characters replaced by spaces so the
/// envelope's single-line rule never drops it.
pub(crate) fn mirror_summary(to: &str, message: &str) -> String {
    let flat = |text: &str| -> String {
        text.chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect()
    };
    let first = message.trim().lines().next().unwrap_or_default();
    let to: String = flat(to.trim()).chars().take(60).collect();
    let summary = format!("SendMessage -> {to}: {}", flat(first));
    summary
        .chars()
        .take(200)
        .collect::<String>()
        .trim_end()
        .to_owned()
}

/// The message as the mirror's body: at most half the envelope cap, cut at a character boundary.
pub(crate) fn mirror_body(message: &str, max_message_bytes: usize) -> &str {
    let cap = max_message_bytes / 2;
    if message.len() <= cap {
        return message;
    }
    let mut end = cap;
    while !message.is_char_boundary(end) {
        end -= 1;
    }
    &message[..end]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mirror_summary_is_always_a_valid_single_line() {
        let summary = mirror_summary("  peer\tname ", "\n  first\u{1b}[0m line\r\nsecond");
        assert_eq!(summary, "SendMessage -> peer name: first [0m line");
        assert!(!summary.chars().any(char::is_control));
        let long = mirror_summary(&"t".repeat(100), &"é".repeat(500));
        assert_eq!(long.chars().count(), 200);
        assert_eq!(mirror_body("aé", 4), "a");
    }
}
