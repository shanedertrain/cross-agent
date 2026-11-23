//! End-to-end: wakes through a fake Claude session (registry record, key file and inbox socket),
//! the Stop hook, and doctor, against a real daemon on a private nats-server.

mod harness;

use std::io::Read;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use harness::{HeldSession, Host, Nats, wait_for};
use serde_json::{Value, json};

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

/// A Claude Code session as the daemon sees it: files in the host user's default
/// `~/.claude/sessions` (sessions opened without an MCP window are woken there) and an inbox
/// socket whose received bytes are forwarded to `frames`. The record names this test process,
/// which is alive and whose start time is read the way Claude records it.
struct FakeClaude {
    dir: PathBuf,
    conversation: String,
    socket: PathBuf,
    frames: Receiver<String>,
}

impl FakeClaude {
    fn new(host: &Host, conversation: &str) -> Self {
        let dir = host.dir().join("home/.claude/sessions");
        std::fs::create_dir_all(&dir).expect("sessions dir");
        let socket = dir.join("cc.sock");
        let listener = UnixListener::bind(&socket).expect("fake inbox socket");
        let (tx, frames) = channel();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut received = String::new();
                let _ = stream.read_to_string(&mut received);
                if tx.send(received).is_err() {
                    return;
                }
            }
        });
        let claude = Self {
            dir,
            conversation: conversation.to_owned(),
            socket,
            frames,
        };
        let pid = std::process::id();
        let key = serde_json::to_vec(
            &json!({ "peerToken": TOKEN, "procStart": FakeClaude::proc_start() }),
        )
        .expect("key json");
        std::fs::write(
            claude.dir.join(xagent_wake::key_file_name(
                pid,
                claude.socket.to_str().expect("utf8"),
            )),
            key,
        )
        .expect("key file");
        claude.set_status("busy");
        claude
    }

    /// Field 22 of `/proc/<pid>/stat`: the process start time in ticks.
    fn proc_start() -> String {
        let stat = std::fs::read_to_string("/proc/self/stat").expect("stat");
        let after_comm = &stat[stat.rfind(')').expect("comm") + 2..];
        after_comm
            .split_whitespace()
            .nth(19)
            .expect("starttime")
            .to_owned()
    }

    /// Rewrites the record: `status`, last changed ten seconds ago.
    fn set_status(&self, status: &str) {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis();
        let record = json!({
            "pid": std::process::id(),
            "procStart": Self::proc_start(),
            "sessionId": self.conversation,
            "kind": "interactive",
            "status": status,
            "statusUpdatedAt": now_ms - 10_000,
            "messagingSocketPath": self.socket,
        });
        let tmp = self.dir.join("record.tmp");
        std::fs::write(&tmp, record.to_string()).expect("record");
        std::fs::rename(tmp, self.dir.join(format!("{}.json", std::process::id())))
            .expect("record rename");
    }
}

/// Sends a DM from conversation `from` (a fresh codex session per call) to agent `to`.
fn dm(host: &Host, from: &str, to: &str, summary: &str) {
    host.as_conv(from, "dm", &["--to", to, "--summary", summary])
        .expect("dm sent");
}

fn unread(session: &mut HeldSession) -> u64 {
    session.inbox_status()["unread_total"]
        .as_u64()
        .expect("unread_total")
}

fn wait_unread(session: &mut HeldSession, want: u64) {
    wait_for(
        Duration::from_secs(10),
        "unread messages to be stored",
        || (unread(session) == want).then_some(()),
    );
}

/// Two DMs arrive while the session is busy: nothing is written. Once it is idle, exactly one
/// correctly framed wake with the counts only; a third DM inside the rate limit adds no wake.
#[test]
fn idle_claude_session_gets_one_framed_wake_and_busy_gets_none() {
    let nats = Nats::start();
    let host = Host::start(&nats, "host-a");
    let claude = FakeClaude::new(&host, "claude-wake-1");
    let mut bob = host
        .hold_as("claude-wake-1", "claude-code")
        .expect("session");
    let bob_id = bob.agent_id.clone();

    dm(&host, "conv-alice", &bob_id, "SECRET peer summary one");
    dm(&host, "conv-alice", &bob_id, "SECRET peer summary two");
    wait_unread(&mut bob, 2);
    assert!(
        claude.frames.recv_timeout(Duration::from_secs(3)).is_err(),
        "no wake while the session is busy"
    );

    claude.set_status("idle");
    let frame = claude
        .frames
        .recv_timeout(Duration::from_secs(10))
        .expect("a wake once idle");
    let lines: Vec<&str> = frame.lines().collect();
    assert_eq!(lines.len(), 2, "auth line and user line: {frame:?}");
    assert_eq!(
        serde_json::from_str::<Value>(lines[0]).expect("auth json"),
        json!({ "type": "auth", "token": TOKEN })
    );
    let user: Value = serde_json::from_str(lines[1]).expect("user json");
    assert_eq!(user["type"], "user");
    assert_eq!(user["message"]["role"], "user");
    assert_eq!(
        user["message"]["content"],
        format!(
            "<cross-session-message from-name=\"xagent\">\n[xagent] 2 unread messages for \
             conversations/{bob_id} (2 DM). Check your xagent inbox.\n</cross-session-message>"
        )
    );
    assert!(
        !frame.contains("SECRET"),
        "peer text never reaches the client"
    );

    dm(&host, "conv-alice", &bob_id, "third");
    wait_unread(&mut bob, 3);
    assert!(
        claude.frames.recv_timeout(Duration::from_secs(3)).is_err(),
        "the rate limit keeps a second wake from following at once"
    );
}

/// The Stop hook blocks once per unread state with the fixed reminder, lets the same state
/// through, and is silent for a conversation without a session and when the daemon is down.
#[test]
fn claude_stop_hook_blocks_once_per_state_and_is_silent_when_unbound() {
    let nats = Nats::start();
    let mut host = Host::start(&nats, "host-a");
    let mut bob = host.hold_as("stop-conv", "claude-code").expect("session");
    let bob_id = bob.agent_id.clone();
    let stop = |host: &Host, event: Value| {
        let (code, out) = host.run_with_stdin(&["hook", "claude-stop"], &event.to_string());
        assert_eq!(code, Some(0), "a hook never fails its client");
        out
    };
    let event = json!({ "session_id": "stop-conv", "cwd": "/work", "stop_hook_active": false });

    assert_eq!(stop(&host, event.clone()), "", "nothing unread");
    dm(&host, "conv-alice", &bob_id, "hello");
    wait_unread(&mut bob, 1);

    let out = stop(&host, event.clone());
    let reply: Value = serde_json::from_str(&out).expect("a block decision");
    assert_eq!(reply["decision"], "block");
    let reason = reply["reason"].as_str().expect("reason");
    assert!(
        reason.starts_with(&format!(
            "[xagent] 1 unread message for conversations/{bob_id} (1 DM)"
        )),
        "{reason}"
    );
    assert!(
        !reason.contains("hello"),
        "peer text never reaches the client"
    );
    assert_eq!(
        stop(&host, event.clone()),
        "",
        "same state: let the stop through"
    );
    assert_eq!(
        stop(
            &host,
            json!({ "session_id": "stop-conv", "stop_hook_active": true })
        ),
        ""
    );
    assert_eq!(
        stop(
            &host,
            json!({ "session_id": "never-bound", "cwd": "/work" })
        ),
        "",
        "silent for a conversation without a session"
    );

    dm(&host, "conv-alice", &bob_id, "again");
    wait_unread(&mut bob, 2);
    assert!(
        stop(&host, event.clone()).contains("2 unread messages"),
        "new state, new reminder"
    );

    drop(bob);
    host.crash();
    assert_eq!(stop(&host, event), "", "no daemon: silent, exit 0");
}

/// Doctor reports a healthy installation with a reachable Claude session, and fails when the
/// daemon is gone; client-config prints the hooks stanza naming this binary.
#[test]
fn doctor_reports_and_client_config_prints_hooks() {
    let nats = Nats::start();
    let mut host = Host::start(&nats, "host-a");
    let _claude = FakeClaude::new(&host, "claude-doctor");

    let (code, report) = host.run_with_stdin(&["doctor"], "");
    assert_eq!(code, Some(0), "{report}");
    for want in [
        "PASS  config",
        "PASS  nats server",
        "PASS  daemon",
        "PASS  jetstream",
        "PASS  claude inbox",
    ] {
        assert!(report.contains(want), "missing {want:?} in:\n{report}");
    }

    let (code, out) = host.run_with_stdin(&["client-config", "--client", "claude"], "");
    assert_eq!(code, Some(0));
    assert!(
        out.contains("hook claude-stop") && out.contains("hook claude-sendmessage"),
        "{out}"
    );
    assert!(out.contains("\"matcher\": \"SendMessage\""), "{out}");
    let (_, out) = host.run_with_stdin(&["client-config", "--client", "codex"], "");
    assert!(
        out.contains("hook codex-stop") && out.contains("[mcp_servers.xagent]"),
        "{out}"
    );

    host.crash();
    let (code, report) = host.run_with_stdin(&["doctor"], "");
    assert_eq!(code, Some(1), "{report}");
    assert!(report.contains("FAIL  daemon"), "{report}");
}

/// The `SendMessage` hook mirrors a send onto the bus as the conversation's own session, to the
/// `sendmessage` topic (created on first use), and does nothing for an unbound conversation.
#[test]
fn sendmessage_hook_mirrors_the_send_to_its_topic() {
    let nats = Nats::start();
    let host = Host::start(&nats, "host-a");
    let held = host.hold_as("mirror-conv", "claude-code").expect("session");
    let hook = |session: &str| {
        let event = json!({
            "session_id": session, "tool_name": "SendMessage",
            "tool_input": { "to": "infra-12", "message": "please rebase\nthen ping me" },
        });
        let (code, out) = host.run_with_stdin(&["hook", "claude-sendmessage"], &event.to_string());
        assert_eq!(code, Some(0), "a hook never fails its client");
        assert_eq!(out, "", "a PostToolUse hook prints nothing");
    };
    hook("never-bound");
    hook("mirror-conv");

    let history = host
        .run(&[
            "topic",
            "history",
            "--conversation",
            "reader",
            "--client",
            "codex",
            "sendmessage",
            "--body",
        ])
        .expect("topic history");
    let messages = history["messages"].as_array().expect("messages");
    assert_eq!(messages.len(), 1, "{history}");
    let mirrored = &messages[0];
    assert_eq!(
        mirrored["summary"],
        "SendMessage -> infra-12: please rebase"
    );
    assert_eq!(mirrored["body"], "please rebase\nthen ping me");
    assert_eq!(mirrored["sender"], held.agent_id.as_str());
    assert_eq!(mirrored["kind"], "status");
    assert_eq!(mirrored["priority"], "low");
}
