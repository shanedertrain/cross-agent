//! Claude Code's native cross-session inbox (the transport behind its `SendMessage` tool).
//!
//! Every interactive session records `messagingSocketPath` in `<sessions_dir>/<pid>.json` and
//! writes its peer token to `<pid>.<sha256 of the socket path>.key`. The wire format is one JSON
//! object per line: `{"type":"auth","token":...}`, then `{"type":"user","message":{...}}`; Claude
//! shows the message as a `<cross-session-message>` and starts a turn when idle. The inbox is
//! undocumented: when a Claude update changes it, the address lookup fails and wakes are dropped.

use std::os::unix::fs::FileTypeExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;

use crate::error::WakeError;
use crate::policy::{Readiness, Target};
use crate::sessions::{SessionRecord, find_record, live_records, ticks};

const FROM_NAME: &str = "xagent";
/// Claude marks a session idle a moment before its Stop hook has run.
const IDLE_SETTLE: Duration = Duration::from_secs(3);
const BUSY_RETRY: Duration = Duration::from_secs(2);
const SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// `<pid>.<sha256 of the socket path>.key`
pub fn key_file_name(pid: u32, socket: &str) -> String {
    format!("{pid}.{:x}.key", Sha256::digest(socket.as_bytes()))
}

/// Socket path and peer token of a live session's inbox, or `None` when it has none we can use.
fn inbox_address(sessions_dir: &Path, rec: &SessionRecord) -> Option<(PathBuf, String)> {
    let socket = rec.messaging_socket_path.as_deref()?;
    if !socket.starts_with('/') || !std::fs::metadata(socket).ok()?.file_type().is_socket() {
        return None;
    }
    let key: Value = serde_json::from_slice(
        &std::fs::read(sessions_dir.join(key_file_name(rec.pid, socket))).ok()?,
    )
    .ok()?;
    let token = key.get("peerToken")?.as_str()?;
    if token.len() != 32
        || !token
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    {
        return None;
    }
    if let Some(recorded) = key.get("procStart").filter(|v| !v.is_null())
        && ticks(Some(recorded)) != rec.proc_start
    {
        return None; // a key left behind by an earlier process with the same pid
    }
    Some((PathBuf::from(socket), token.to_owned()))
}

/// The two lines written to the inbox socket: auth, then the user message in Claude's own
/// cross-session envelope. No `from-mode`: xagent is not a session and attests no permission mode.
pub fn inbox_frame(token: &str, text: &str) -> Vec<u8> {
    let wrapped = format!(
        "<cross-session-message from-name=\"{FROM_NAME}\">\n{text}\n</cross-session-message>"
    );
    let auth = serde_json::json!({ "type": "auth", "token": token });
    let user =
        serde_json::json!({ "type": "user", "message": { "role": "user", "content": wrapped } });
    format!("{auth}\n{user}\n").into_bytes()
}

/// One live interactive Claude session, for `xagent doctor`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionReport {
    pub pid: u32,
    pub name: Option<String>,
    pub status: Option<String>,
    /// Whether its native inbox can be reached (socket present, key file and token valid).
    pub reachable: bool,
}

/// Wakes a Claude Code session through its native inbox.
#[derive(Debug, Clone)]
pub struct ClaudeInbox {
    sessions_dir: PathBuf,
}

impl ClaudeInbox {
    pub fn new(sessions_dir: PathBuf) -> Self {
        Self { sessions_dir }
    }

    /// The live interactive sessions in the registry and whether each inbox is reachable.
    /// `None` when the registry directory cannot be read.
    pub fn sessions(&self) -> Option<Vec<SessionReport>> {
        let mut reports: Vec<SessionReport> = live_records(&self.sessions_dir)?
            .into_iter()
            .filter(|rec| rec.kind.as_deref().unwrap_or("interactive") == "interactive")
            .map(|rec| SessionReport {
                pid: rec.pid,
                reachable: inbox_address(&self.sessions_dir, &rec).is_some(),
                name: rec.name,
                status: rec.status,
            })
            .collect();
        reports.sort_by_key(|r| r.pid);
        Some(reports)
    }

    /// The live record of the target's conversation. Found by pid, it must still carry the
    /// target's session id: after a `/clear` the process runs a new conversation, which is not
    /// woken for the old one's messages.
    fn record(&self, target: &Target, pid: Option<u32>) -> Option<SessionRecord> {
        let conversation = target.conversation.as_str();
        find_record(&self.sessions_dir, pid, Some(conversation))
            .filter(|rec| rec.session_id.as_deref() == Some(conversation))
    }

    /// Whether the session is idle (and has been for a moment) per its registry record.
    pub(crate) fn ready(&self, target: &Target, pid: Option<u32>) -> Readiness {
        let Some(rec) = self.record(target, pid) else {
            return Readiness::Gone(
                "no live Claude session record of this conversation".to_owned(),
            );
        };
        if inbox_address(&self.sessions_dir, &rec).is_none() {
            return Readiness::Gone("session has no usable inbox".to_owned());
        }
        if rec.status.as_deref() != Some("idle") {
            return Readiness::Wait(BUSY_RETRY, "session busy");
        }
        if let Some(since_ms) = rec.status_updated_at {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0.0, |d| d.as_secs_f64() * 1000.0);
            if now_ms - since_ms < IDLE_SETTLE.as_secs_f64() * 1000.0 {
                return Readiness::Wait(IDLE_SETTLE, "session just went idle");
            }
        }
        Readiness::Ready
    }

    pub(crate) async fn send(&self, target: &Target, pid: Option<u32>, text: &str) -> bool {
        let Some((socket, token)) = self
            .record(target, pid)
            .and_then(|rec| inbox_address(&self.sessions_dir, &rec))
        else {
            return false;
        };
        let frame = inbox_frame(&token, text);
        let result: Result<(), WakeError> = async {
            let mut stream =
                tokio::time::timeout(SEND_TIMEOUT, UnixStream::connect(&socket)).await??;
            tokio::time::timeout(SEND_TIMEOUT, stream.write_all(&frame)).await??;
            stream.shutdown().await?;
            Ok(())
        }
        .await;
        if let Err(err) = &result {
            tracing::warn!(error = %err, "claude inbox send failed");
        }
        result.is_ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_file_is_pid_dot_sha256_of_the_socket_path() {
        // sha256("/tmp/cc.sock")
        assert_eq!(
            key_file_name(42, "/tmp/cc.sock"),
            "42.9aafc8a89ee33e0ab162492fe66766e343d6acf97bc196e762ab929d7393c21d.key"
        );
    }

    #[test]
    fn frame_is_two_json_lines_with_the_wrapped_text() {
        let frame =
            String::from_utf8(inbox_frame(&"a".repeat(32), "hi \"x\"\nline")).expect("utf8");
        let lines: Vec<&str> = frame.split_inclusive('\n').collect();
        assert_eq!(lines.len(), 2, "{frame:?}");
        let auth: Value = serde_json::from_str(lines[0]).expect("auth json");
        assert_eq!(
            auth,
            serde_json::json!({"type": "auth", "token": "a".repeat(32)})
        );
        let user: Value = serde_json::from_str(lines[1]).expect("user json");
        assert_eq!(user["type"], "user");
        assert_eq!(user["message"]["role"], "user");
        assert_eq!(
            user["message"]["content"],
            "<cross-session-message from-name=\"xagent\">\nhi \"x\"\nline\n</cross-session-message>"
        );
    }
}
