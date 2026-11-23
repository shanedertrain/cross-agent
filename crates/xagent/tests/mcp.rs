//! End-to-end over MCP: agent clients launch `xagent mcp` and speak JSON-RPC over its stdio; the
//! daemon serves the tools. Claude Code sessions are stand-ins: a `sleep` process with a record in
//! a temporary sessions directory (never the real `~/.claude`), with a fake inbox socket where a
//! wake is expected; the Codex app-server is a fake on a temporary socket.

mod harness;

use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::net::UnixListener;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{Receiver, channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use harness::{Host, Nats, wait_for};
use serde_json::{Value, json};
use tempfile::TempDir;

const REPLY_TIMEOUT: Duration = Duration::from_secs(20);

/// The launch environment the relay forwards; cleared so the test's own session never leaks in.
const CONTEXT_ENV: [&str; 7] = [
    "XAGENT_CONVERSATION_ID",
    "XAGENT_SESSION_TITLE",
    "CLAUDE_CODE_SESSION_ID",
    "CLAUDE_CODE_MESSAGING_SOCKET",
    "CLAUDE_PID",
    "CLAUDE_CONFIG_DIR",
    "CODEX_HOME",
];

/// An MCP client driving one `xagent mcp` process over its stdio.
struct Mcp {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Receiver<String>,
    next_id: u64,
}

impl Mcp {
    fn launch(host: &Host, client: &str, env: &[(&str, &str)]) -> Self {
        Self::launch_in(host, client, env, None)
    }

    /// Like [`Mcp::launch`], with the client running in `cwd`.
    fn launch_in(host: &Host, client: &str, env: &[(&str, &str)], cwd: Option<&Path>) -> Self {
        let mut cmd: Command = host.command();
        if let Some(cwd) = cwd {
            cmd.current_dir(cwd);
        }
        for key in CONTEXT_ENV {
            cmd.env_remove(key);
        }
        cmd.envs(env.iter().copied())
            .args(["mcp", "--client", client])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = cmd.spawn().expect("xagent mcp starts");
        let stdout = child.stdout.take().expect("piped stdout");
        let (tx, lines) = channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        let mut mcp = Self {
            stdin: child.stdin.take(),
            child,
            lines,
            next_id: 1,
        };
        let init = mcp.request(
            "initialize",
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": {},
                "clientInfo": { "name": "xagent-e2e", "version": "0" },
            }),
        );
        assert_eq!(init["serverInfo"]["name"], "xagent", "{init}");
        assert!(
            init["instructions"]
                .as_str()
                .is_some_and(|i| i.contains("inbox_status")),
            "{init}"
        );
        mcp.send(&json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }));
        mcp
    }

    fn send(&mut self, message: &Value) {
        let stdin = self.stdin.as_mut().expect("relay stdin open");
        let mut line = serde_json::to_vec(message).expect("JSON");
        line.push(b'\n');
        stdin.write_all(&line).expect("write to relay");
        stdin.flush().expect("flush relay stdin");
    }

    /// Sends a request and returns its result (panics on a JSON-RPC error).
    fn request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let mut message = json!({ "jsonrpc": "2.0", "id": id, "method": method });
        message["params"] = params;
        self.send(&message);
        loop {
            let line = self
                .lines
                .recv_timeout(REPLY_TIMEOUT)
                .unwrap_or_else(|_| panic!("no reply to {method}"));
            let message: Value = serde_json::from_str(&line).expect("relay writes JSON lines");
            if message["id"] == json!(id) {
                assert!(message.get("error").is_none(), "{method}: {message}");
                return message["result"].clone();
            }
        }
    }

    /// Sends a request and returns the whole reply (result or JSON-RPC error).
    fn raw_request(&mut self, method: &str, params: Value) -> Value {
        let id = self.next_id;
        self.next_id += 1;
        let mut message = json!({ "jsonrpc": "2.0", "id": id, "method": method });
        message["params"] = params;
        self.send(&message);
        loop {
            let line = self
                .lines
                .recv_timeout(REPLY_TIMEOUT)
                .unwrap_or_else(|_| panic!("no reply to {method}"));
            let message: Value = serde_json::from_str(&line).expect("relay writes JSON lines");
            if message["id"] == json!(id) {
                return message;
            }
        }
    }

    /// Calls a tool; `Ok` with its JSON result, or `Err` with its `{code, message}` error.
    fn tool(&mut self, name: &str, arguments: Value, meta: Option<Value>) -> Result<Value, Value> {
        let mut params = json!({ "name": name });
        params["arguments"] = arguments;
        if let Some(meta) = meta {
            params["_meta"] = meta;
        }
        let result = self.request("tools/call", params);
        let text = result["content"][0]["text"].as_str().expect("text content");
        let value: Value = serde_json::from_str(text).expect("tool output is JSON");
        if result["isError"] == json!(true) {
            Err(value["error"].clone())
        } else {
            Ok(value)
        }
    }

    fn call(&mut self, name: &str, arguments: Value) -> Value {
        self.tool(name, arguments, None)
            .unwrap_or_else(|err| panic!("{name} failed: {err}"))
    }

    /// Closes the client's end, as a client does when its conversation ends.
    fn close(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("relay exits");
        assert!(status.success(), "relay exit status {status}");
    }
}

impl Drop for Mcp {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// A stand-in Claude Code process with its record in a fake sessions directory, and optionally
/// an idle session's native inbox whose received frames arrive on `frames`.
struct ClaudeSession {
    process: Child,
    record: PathBuf,
    start: String,
    inbox: Option<PathBuf>,
}

const TOKEN: &str = "0123456789abcdef0123456789abcdef";

impl ClaudeSession {
    /// An idle session with a reachable inbox (socket and key file).
    fn with_inbox(config_dir: &Path, session_id: &str) -> (Self, Receiver<String>) {
        let mut session = Self::start(config_dir, session_id, "", "derived");
        let socket = config_dir.join("cc.sock");
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
        let socket_text = socket.to_str().expect("utf-8 socket path");
        std::fs::write(
            config_dir.join("sessions").join(xagent_wake::key_file_name(
                session.process.id(),
                socket_text,
            )),
            json!({ "peerToken": TOKEN, "procStart": session.start }).to_string(),
        )
        .expect("key file");
        session.inbox = Some(socket);
        session.write(session_id, "", "derived");
        (session, frames)
    }

    fn start(config_dir: &Path, session_id: &str, name: &str, name_source: &str) -> Self {
        let process = Command::new("sleep")
            .arg("600")
            .spawn()
            .expect("sleep starts");
        let proc_stat =
            std::fs::read_to_string(format!("/proc/{}/stat", process.id())).expect("stat");
        let start = proc_stat[proc_stat.rfind(')').expect("comm") + 2..]
            .split_whitespace()
            .nth(19)
            .expect("start time")
            .to_owned();
        let sessions = config_dir.join("sessions");
        std::fs::create_dir_all(&sessions).expect("sessions dir");
        let session = Self {
            record: sessions.join(format!("{}.json", process.id())),
            process,
            start,
            inbox: None,
        };
        session.write(session_id, name, name_source);
        session
    }

    fn pid(&self) -> String {
        self.process.id().to_string()
    }

    /// (Re)writes the record, as Claude does on start, `/rename` and `/clear`.
    fn write(&self, session_id: &str, name: &str, name_source: &str) {
        let mut record = json!({
            "pid": self.process.id(), "sessionId": session_id, "cwd": "/work",
            "procStart": self.start, "name": name, "nameSource": name_source,
        });
        if let Some(inbox) = &self.inbox {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_millis();
            record["kind"] = json!("interactive");
            record["status"] = json!("idle");
            record["statusUpdatedAt"] = json!(now_ms - 10_000);
            record["messagingSocketPath"] = json!(inbox);
        }
        std::fs::write(&self.record, record.to_string()).expect("write record");
    }
}

impl Drop for ClaudeSession {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

fn agent<'a>(list: &'a Value, id: &str) -> &'a Value {
    list["agents"]
        .as_array()
        .expect("agents")
        .iter()
        .find(|a| a["id"] == id)
        .unwrap_or_else(|| panic!("{id} not in {list}"))
}

/// The tools served, each refusing unknown arguments.
fn assert_tools(mcp: &mut Mcp) {
    let tools = mcp.request("tools/list", json!({}));
    let mut names: Vec<&str> = tools["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|t| t["name"].as_str().expect("name"))
        .collect();
    names.sort_unstable();
    assert_eq!(
        names,
        [
            "agent_list",
            "dm_send",
            "inbox_list",
            "inbox_status",
            "message_read",
            "message_reply",
            "message_send",
            "thread_read",
            "topic_create",
            "topic_history",
            "topic_list",
            "topic_subscribe",
            "topic_subscriptions",
            "topic_unsubscribe",
            "whoami",
        ]
    );
    for tool in tools["tools"].as_array().expect("tools") {
        assert_eq!(tool["inputSchema"]["additionalProperties"], false, "{tool}");
    }
}

/// A `/clear` gives the Claude process a new session id: a new conversation with a fresh identity
/// and inbox, bound at A's next tool call; the old identity is released, and the Stop hook (which
/// reports the new session id) matches the new session. Returns the new agent id.
fn clear_starts_a_new_conversation(
    host: &Host,
    alpha: &ClaudeSession,
    a: &mut Mcp,
    b: &mut Mcp,
    alpha_id: &str,
) -> String {
    // A /clear gives the Claude process a new session id: a new conversation, a fresh identity.
    alpha.write("sess-alpha-2", "infra-12", "derived");
    let after_clear = a.call("whoami", json!({}));
    assert_eq!(after_clear["conversation"], "sess-alpha-2", "{after_clear}");
    assert_ne!(after_clear["agent_id"], alpha_id);
    let cleared_id = after_clear["agent_id"]
        .as_str()
        .expect("agent_id")
        .to_owned();
    assert_eq!(a.call("inbox_status", json!({}))["unread_total"], 0);
    wait_for(
        Duration::from_secs(10),
        "the old identity's release",
        || {
            let online = b.call("agent_list", json!({ "online_only": true }));
            let online: Vec<&str> = online["agents"]
                .as_array()
                .expect("agents")
                .iter()
                .filter_map(|a| a["id"].as_str())
                .collect();
            (online.contains(&cleared_id.as_str()) && !online.contains(&alpha_id)).then_some(())
        },
    );
    b.call(
        "dm_send",
        json!({ "recipient": cleared_id, "summary": "after the clear" }),
    );
    wait_for(Duration::from_secs(10), "the DM to reach A", || {
        (a.call("inbox_status", json!({}))["unread_total"] == 1).then_some(())
    });
    let stop = |session: &str| {
        let event = json!({ "session_id": session, "cwd": "/work" }).to_string();
        let (code, out) = host.run_with_stdin(&["hook", "claude-stop"], &event);
        assert_eq!(code, Some(0));
        out
    };
    assert_eq!(stop("sess-alpha-1"), "", "the old conversation is gone");
    let blocked = stop("sess-alpha-2");
    assert!(
        blocked.contains(&format!("1 unread message for conversations/{cleared_id}")),
        "{blocked}"
    );
    cleared_id
}

/// Two Claude Code conversations DM each other through their MCP servers; the receiver lists and
/// reads the message; `agent_list` shows each session's name (its `SendMessage` address); a
/// `/clear` starts a new conversation: the connection re-binds to its identity at the next tool
/// call, and the Stop hook (which reports the new session id) matches it; closing the client
/// releases it.
#[test]
fn claude_code_conversations_message_each_other_over_mcp() {
    let nats = Nats::start();
    let host = Host::start(&nats, "host-a");
    let claude_dir = TempDir::new().expect("fake claude config dir");
    let claude_config = claude_dir.path().to_str().expect("utf-8 path");
    // A's environment still names the session id from before a /clear; the record is current.
    let alpha = ClaudeSession::start(claude_dir.path(), "sess-alpha-1", "infra-12", "derived");
    let bravo = ClaudeSession::start(claude_dir.path(), "sess-bravo", "Bravo", "user");

    let mut a = Mcp::launch(
        &host,
        "claude-code",
        &[
            ("CLAUDE_CONFIG_DIR", claude_config),
            ("CLAUDE_PID", &alpha.pid()),
            ("CLAUDE_CODE_SESSION_ID", "sess-alpha-0"),
        ],
    );
    let socket = format!("/tmp/cc-socks-test/{}.sock", bravo.pid());
    let mut b = Mcp::launch(
        &host,
        "claude",
        &[
            ("CLAUDE_CONFIG_DIR", claude_config),
            ("CLAUDE_CODE_MESSAGING_SOCKET", &socket),
            ("CLAUDE_CODE_SESSION_ID", "sess-bravo"),
        ],
    );

    assert_tools(&mut a);

    let who_a = a.call("whoami", json!({}));
    assert_eq!(who_a["conversation"], "sess-alpha-1", "{who_a}");
    assert_eq!(who_a["client"], "claude-code");
    assert_eq!(who_a["display_name"], "infra-12");
    let alpha_id = who_a["agent_id"].as_str().expect("agent_id").to_owned();
    let who_b = b.call("whoami", json!({}));
    assert_eq!(who_b["conversation"], "sess-bravo");
    assert_eq!(
        who_b["client"], "claude-code",
        "`claude` is an alias of claude-code"
    );
    let bravo_id = who_b["agent_id"].as_str().expect("agent_id").to_owned();

    let agents = a.call("agent_list", json!({}));
    let listed_b = agent(&agents, &bravo_id);
    assert_eq!(listed_b["display_name"], "Bravo", "{agents}");
    assert_eq!(listed_b["online"], true);
    assert_eq!(listed_b["client"], "claude-code");
    let listed_a = agent(&agents, &alpha_id);
    assert_eq!(listed_a["self"], true, "{agents}");
    assert_eq!(
        listed_a["display_name"], "infra-12",
        "a derived name is the SendMessage address too"
    );

    let sent = a.call(
        "dm_send",
        json!({
            "recipient": bravo_id, "summary": "review the lease change", "body": "see 530f7a8",
            "kind": "review_request", "refs": [{ "type": "git_commit", "value": "530f7a8" }],
        }),
    );
    assert_eq!(sent["status"], "published", "{sent}");
    let id = sent["id"].as_str().expect("message id").to_owned();

    let status = wait_for(Duration::from_secs(10), "the DM to reach B", || {
        let status = b.call("inbox_status", json!({}));
        (status["unread_total"] == 1).then_some(status)
    });
    assert_eq!(status["dm_unread"], 1, "{status}");
    let list = b.call("inbox_list", json!({}));
    let header = &list["messages"][0];
    assert_eq!(header["id"], id.as_str(), "{list}");
    assert_eq!(header["sender"], alpha_id.as_str());
    assert_eq!(header["kind"], "review_request");
    assert!(header.get("body").is_none(), "{header}");
    let read = b.call("message_read", json!({ "ids": [id] }));
    assert_eq!(read["messages"][0]["body"], "see 530f7a8", "{read}");
    assert_eq!(read["messages"][0]["refs"][0]["value"], "530f7a8");
    assert_eq!(b.call("inbox_status", json!({}))["unread_total"], 0);

    let cleared_id = clear_starts_a_new_conversation(&host, &alpha, &mut a, &mut b, &alpha_id);

    // A /rename reaches the registry at the next tool call.
    bravo.write("sess-bravo", "Bravo Two", "user");
    b.call("inbox_status", json!({}));
    let agents = a.call("agent_list", json!({}));
    assert_eq!(agent(&agents, &bravo_id)["display_name"], "Bravo Two");

    // The client closing its MCP server ends the session and releases the identity.
    a.close();
    wait_for(Duration::from_secs(10), "A's lease to be released", || {
        let agents = b.call("agent_list", json!({ "online_only": true }));
        agents["agents"]
            .as_array()
            .expect("agents")
            .iter()
            .all(|a| a["id"] != cleared_id.as_str())
            .then_some(())
    });
}

/// A Codex server learns its conversation from `_meta.threadId` of a tool call (one server per
/// thread); a dsh server from `XAGENT_CONVERSATION_ID`. Both bind at their first tool call.
#[test]
fn codex_and_dsh_conversations_come_from_their_launch_context() {
    let nats = Nats::start();
    let host = Host::start(&nats, "host-a");
    let codex_home = TempDir::new().expect("fake codex home");
    let thread = "019a7b3c-1d2e-7f40-8a9b-0c1d2e3f4a5b";
    std::fs::write(
        codex_home.path().join("session_index.jsonl"),
        format!(
            "{{\"id\":\"{thread}\",\"thread_name\":\"Fix lease\",\"updated_at\":\"2026-11-23T10:00:00Z\"}}\n\
             {{\"id\":\"{thread}\",\"thread_name\":\"Bench Rig\",\"updated_at\":\"2026-11-23T11:00:00Z\"}}\n"
        ),
    )
    .expect("session index");

    let mut codex = Mcp::launch(
        &host,
        "codex",
        &[("CODEX_HOME", codex_home.path().to_str().expect("utf-8"))],
    );
    let unnamed = codex
        .tool("whoami", json!({}), None)
        .expect_err("no thread named yet");
    assert_eq!(unnamed["code"], "not_bound", "{unnamed}");
    let meta = json!({ "threadId": thread });
    let who = codex
        .tool("whoami", json!({}), Some(meta.clone()))
        .expect("whoami");
    assert_eq!(who["conversation"], thread, "{who}");
    assert_eq!(who["client"], "codex");
    // The same id the daemon derives for the conversation (Python reference value).
    assert_eq!(who["agent_id"], "chat-60b55906d0d938f1f2d0089d");
    assert_eq!(who["display_name"], "Bench Rig");
    // Later calls without `_meta` act as the bound thread.
    assert_eq!(
        codex.call("whoami", json!({}))["agent_id"],
        "chat-60b55906d0d938f1f2d0089d"
    );

    let mut dsh = Mcp::launch(
        &host,
        "dsh",
        &[
            ("XAGENT_CONVERSATION_ID", "session 42"),
            ("XAGENT_SESSION_TITLE", "Training Data"),
        ],
    );
    let who = dsh.call("whoami", json!({}));
    assert_eq!(who["agent_id"], "chat-e58f4e7091e6e29514e79993", "{who}");
    let agents = dsh.call("agent_list", json!({}));
    assert_eq!(
        agent(&agents, "chat-e58f4e7091e6e29514e79993")["display_name"],
        "Training Data"
    );
    assert_eq!(
        agent(&agents, "chat-60b55906d0d938f1f2d0089d")["display_name"],
        "Bench Rig"
    );

    // Tool errors carry the stable code: unknown arguments, unknown recipients.
    let refused = dsh
        .tool(
            "dm_send",
            json!({ "recipient": "chat-60b55906d0d938f1f2d0089d", "summary": "x", "colour": "red" }),
            None,
        )
        .expect_err("unknown argument refused");
    assert_eq!(refused["code"], "invalid_argument", "{refused}");
    let missing = dsh
        .tool(
            "dm_send",
            json!({ "recipient": "chat-000000000000000000000000", "summary": "x" }),
            None,
        )
        .expect_err("unregistered recipient");
    assert_eq!(missing["code"], "agent_not_found", "{missing}");
}

fn ids(list: &Value) -> Vec<&str> {
    list["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| m["id"].as_str().expect("id"))
        .collect()
}

/// Topics over MCP: create, subscribe, post, receive (listed by topic and by channel), read a
/// question (a reply is now owed), reply in its thread (the debt is paid), read the thread and the
/// topic's history.
#[test]
fn topic_round_trip_with_a_reply_obligation_over_mcp() {
    let nats = Nats::start();
    let host = Host::start(&nats, "host-a");
    let mut a = Mcp::launch(&host, "dsh", &[("XAGENT_CONVERSATION_ID", "dsh-alpha-1")]);
    let mut b = Mcp::launch(&host, "dsh", &[("XAGENT_CONVERSATION_ID", "dsh-bravo-1")]);
    let b_id = b.call("whoami", json!({}))["agent_id"]
        .as_str()
        .expect("agent_id")
        .to_owned();

    let created = a.call(
        "topic_create",
        json!({ "name": "firmware.usb", "description": "USB stack" }),
    );
    assert_eq!(created["status"], "created", "{created}");
    for mcp in [&mut a, &mut b] {
        let joined = mcp.call("topic_subscribe", json!({ "topic": "firmware.usb" }));
        assert_eq!(joined["status"], "subscribed", "{joined}");
    }
    // The project topic of the test's cwd (this repository's checkout) is joined too.
    let topics = b.call("topic_subscriptions", json!({}))["topics"].clone();
    assert!(
        topics
            .as_array()
            .is_some_and(|t| t.contains(&json!("firmware.usb"))),
        "{topics}"
    );
    let listed = b.call("topic_list", json!({ "prefix": "firmware" }));
    assert_eq!(listed["topics"][0]["name"], "firmware.usb", "{listed}");
    assert_eq!(listed["topics"][0]["subscribed"], true);
    let refused = b
        .tool(
            "topic_subscribe",
            json!({ "topic": "firmware.usb", "replay": "24h" }),
            None,
        )
        .expect_err("unknown argument refused");
    assert_eq!(refused["code"], "invalid_argument", "{refused}");

    a.call(
        "dm_send",
        json!({ "recipient": b_id, "summary": "side note" }),
    );
    let question = a.call(
        "message_send",
        json!({
            "topic": "firmware.usb", "summary": "which descriptor layout?",
            "body": "see usb.rs", "kind": "question",
        }),
    );
    assert_eq!(question["status"], "published", "{question}");
    assert_eq!(question["receivers"], 1, "{question}");
    let qid = question["id"].as_str().expect("id").to_owned();

    let status = wait_for(Duration::from_secs(10), "B's two messages", || {
        let status = b.call("inbox_status", json!({}));
        (status["unread_total"] == 2).then_some(status)
    });
    assert_eq!(status["dm_unread"], 1, "{status}");
    assert_eq!(status["by_topic"]["firmware.usb"], 1, "{status}");
    assert_eq!(status["awaiting_reply"], 0);
    let chat = b.call("inbox_list", json!({ "channel": "chat" }));
    assert_eq!(ids(&chat), [qid.as_str()], "{chat}");
    let on_topic = b.call("inbox_list", json!({ "topic": "firmware.usb" }));
    assert_eq!(ids(&on_topic), [qid.as_str()], "{on_topic}");
    let dms = b.call("inbox_list", json!({ "channel": "dm" }));
    assert_eq!(dms["messages"][0]["summary"], "side note", "{dms}");
    assert_eq!(ids(&dms).len(), 1);

    let read = b.call("message_read", json!({ "ids": [qid] }));
    assert_eq!(read["messages"][0]["body"], "see usb.rs", "{read}");
    assert_eq!(b.call("inbox_status", json!({}))["awaiting_reply"], 1);
    let reply = b.call(
        "message_reply",
        json!({ "message_id": qid, "summary": "layout B", "kind": "answer" }),
    );
    assert_eq!(reply["topic"], "firmware.usb", "{reply}");
    assert_eq!(reply["thread_id"], qid.as_str());
    assert_eq!(b.call("inbox_status", json!({}))["awaiting_reply"], 0);

    wait_for(Duration::from_secs(10), "the reply to reach A", || {
        (a.call("inbox_status", json!({}))["unread_total"] == 1).then_some(())
    });
    let thread = a.call("thread_read", json!({ "thread_id": qid }));
    let messages = thread["messages"].as_array().expect("messages");
    assert_eq!(messages.len(), 2, "{thread}");
    assert_eq!(messages[1]["reply_to"], qid.as_str());
    assert_eq!(messages[1]["summary"], "layout B");
    let history = b.call(
        "topic_history",
        json!({ "topic": "firmware.usb", "include_body": true }),
    );
    assert_eq!(history["messages"].as_array().map(Vec::len), Some(2));
    assert_eq!(history["messages"][0]["body"], "see usb.rs", "{history}");

    let left = b.call("topic_unsubscribe", json!({ "topic": "firmware.usb" }));
    assert_eq!(left["status"], "unsubscribed", "{left}");
    let topics = b.call("topic_subscriptions", json!({}))["topics"].clone();
    assert!(
        topics
            .as_array()
            .is_some_and(|t| !t.contains(&json!("firmware.usb"))),
        "{topics}"
    );
}

/// A Claude window whose MCP server has not called a tool yet (a resumed conversation) is woken,
/// from counters only, when messages wait for the identity it would bind; its first tool call
/// binds it and finds them. Its profile lives in a non-default `CLAUDE_CONFIG_DIR`: the window and
/// the session it binds are woken there, and doctor checks that registry.
#[test]
fn unbound_claude_window_is_woken_for_waiting_messages() {
    let nats = Nats::start();
    let claude_dir = TempDir::new().expect("fake claude config dir");
    let (claude, frames) = ClaudeSession::with_inbox(claude_dir.path(), "sess-resumed");
    let host = Host::start_with(&nats, "host-a", "[notify]\nwake_min_interval_seconds = 0\n");
    // The conversation ran before (its identity is registered), and is closed now.
    let who = host
        .run(&[
            "whoami",
            "--conversation",
            "sess-resumed",
            "--client",
            "claude-code",
        ])
        .expect("whoami");
    let id = who["agent_id"].as_str().expect("agent_id").to_owned();
    host.as_conv("conv-alice", "dm", &["--to", &id, "--summary", "SECRET"])
        .expect("dm sent");

    let mut window = Mcp::launch(
        &host,
        "claude-code",
        &[
            (
                "CLAUDE_CONFIG_DIR",
                claude_dir.path().to_str().expect("utf-8"),
            ),
            ("CLAUDE_PID", &claude.pid()),
        ],
    );
    let frame = frames
        .recv_timeout(Duration::from_secs(20))
        .expect("a wake for the waiting message");
    assert!(
        frame.contains(&format!(
            "[xagent] 1 message waiting for conversations/{id}. Check your xagent inbox."
        )),
        "{frame}"
    );
    assert!(
        !frame.contains("SECRET"),
        "peer text never reaches the client"
    );
    let status = window.call("inbox_status", json!({}));
    assert_eq!(status["unread_total"], 1, "{status}");
    assert_eq!(window.call("whoami", json!({}))["agent_id"], id.as_str());

    // Bound now: the session is woken at the window's registry too (an unread-count line, where
    // the unbound window got a "waiting" line).
    host.as_conv("conv-alice", "dm", &["--to", &id, "--summary", "again"])
        .expect("dm sent");
    let bound = format!("unread messages for conversations/{id}");
    wait_for(
        Duration::from_secs(20),
        "a wake for the bound session",
        || {
            frames
                .recv_timeout(Duration::from_secs(1))
                .ok()
                .filter(|frame| frame.contains(&bound))
        },
    );
    let (_, report) = host.run_with_stdin(&["doctor"], "");
    let registry = claude_dir.path().join("sessions");
    assert!(
        report.contains(&format!(
            "PASS  claude inbox       1 live session(s) in {}, all reachable",
            registry.display()
        )),
        "{report}"
    );
}

/// A fake Codex app-server: one loaded user thread running in `cwd`, idle; `turn/start` params
/// arrive on the returned channel.
fn fake_app_server(socket: &Path, thread: &str, cwd: &Path) -> Receiver<Value> {
    use tokio_tungstenite::tungstenite::{Message, accept};

    let listener = UnixListener::bind(socket).expect("fake app-server socket");
    let (tx, turns) = channel();
    let (thread, cwd) = (thread.to_owned(), cwd.to_owned());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { return };
            let (tx, thread, cwd) = (tx.clone(), thread.clone(), cwd.clone());
            std::thread::spawn(move || {
                let Ok(mut ws) = accept(stream) else { return };
                while let Ok(frame) = ws.read() {
                    let Message::Text(text) = frame else { continue };
                    let msg: Value = serde_json::from_str(text.as_str()).expect("json");
                    let Some(id) = msg.get("id") else { continue };
                    let result = match msg["method"].as_str() {
                        Some("thread/loaded/list") => {
                            json!({ "data": [thread], "nextCursor": null })
                        }
                        Some("thread/read") if msg["params"]["threadId"] == thread.as_str() => {
                            json!({ "thread": { "id": thread, "cwd": cwd, "status": { "type": "idle" } } })
                        }
                        Some("turn/start") => {
                            let _ = tx.send(msg["params"].clone());
                            json!({})
                        }
                        _ => json!({}),
                    };
                    let reply = json!({ "id": id, "result": result }).to_string();
                    if ws.send(Message::text(reply)).is_err() {
                        return;
                    }
                }
            });
        }
    });
    turns
}

/// A Codex window learns its thread only at its first tool call; before that the daemon finds the
/// user threads loaded in the window's directory through the app-server, and wakes the one whose
/// identity has messages waiting.
#[test]
fn unbound_codex_thread_is_found_and_woken() {
    let nats = Nats::start();
    let dir = TempDir::new().expect("temp dir");
    let project = dir.path().join("project");
    std::fs::create_dir(&project).expect("project dir");
    // The app-server of a non-default CODEX_HOME: the window is woken through it.
    let socket = dir
        .path()
        .join("app-server-control/app-server-control.sock");
    std::fs::create_dir(dir.path().join("app-server-control")).expect("control dir");
    let thread = "019a7b3c-1d2e-7f40-8a9b-0c1d2e3f4a5b";
    let turns = fake_app_server(&socket, thread, &project);
    let host = Host::start(&nats, "host-a");
    let id = host.agent_id(thread);
    host.as_conv("conv-alice", "dm", &["--to", &id, "--summary", "SECRET"])
        .expect("dm sent");

    let _window = Mcp::launch_in(
        &host,
        "codex",
        &[("CODEX_HOME", dir.path().to_str().expect("utf-8"))],
        Some(&project),
    );
    let turn = turns
        .recv_timeout(Duration::from_secs(20))
        .expect("a turn started on the thread");
    assert_eq!(
        turn,
        json!({ "threadId": thread, "input": [{ "type": "text", "text": format!(
            "[xagent] 1 message waiting for conversations/{id}. Check your xagent inbox."
        ) }] })
    );
}

/// A client of any name connects over MCP, and a daemon restart (an upgrade) does not cost it its
/// MCP server: the relay refuses calls
/// while the daemon is down, reconnects, replays the handshake, and the conversation keeps its
/// identity and inbox.
#[test]
fn any_client_relays_and_survives_a_daemon_restart() {
    let nats = Nats::start();
    let mut host = Host::start(&nats, "host-a");
    // Any client name speaks MCP the way dsh does: the conversation comes from the environment.
    let mut dsh = Mcp::launch(
        &host,
        "my-agent",
        &[("XAGENT_CONVERSATION_ID", "session 7")],
    );
    let before = dsh.call("whoami", json!({}));
    assert_eq!(before["client"], "my-agent", "{before}");

    host.stop();
    let refused = dsh.raw_request("tools/call", json!({ "name": "whoami", "arguments": {} }));
    assert_eq!(refused["error"]["code"], -32000, "{refused}");
    host.spawn_daemon();

    let after = wait_for(REPLY_TIMEOUT, "the relay to reconnect", || {
        let reply = dsh.raw_request("tools/call", json!({ "name": "whoami", "arguments": {} }));
        let text = reply["result"]["content"][0]["text"].as_str()?;
        let value: Value = serde_json::from_str(text).ok()?;
        (reply["result"]["isError"] != json!(true)).then_some(value)
    });
    assert_eq!(after["agent_id"], before["agent_id"], "{after}");
    dsh.close();
}
