//! The Codex and dsh transports against fake endpoints (a WebSocket-over-Unix-socket app-server
//! and a cookie-checking HTTP server), driven through the public [`Waker`].

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UnixListener};
use tokio_tungstenite::tungstenite::Message;
use xagent_wake::{Counts, Endpoint, Target, WakeConfig, WakeSource, Waker};

const ID: &str = "019a7b3c-1d2e-7f40-8a9b-0c1d2e3f4a5b";
const SUB: &str = "019a7b3c-ffff-7f40-8a9b-0c1d2e3f4a5b";

#[derive(Debug)]
struct Source;

impl WakeSource for Source {
    fn is_open(&self, _: &Target) -> bool {
        true
    }
    fn counts(&self, _: &Target) -> Option<Counts> {
        Some(Counts {
            unread_total: 1,
            dm_unread: 1,
            ..Counts::default()
        })
    }
    fn already_told(&self, _: &Target, _: &Counts) -> bool {
        false
    }
}

fn target(endpoint: &Endpoint, conversation: &str) -> Target {
    Target {
        key: conversation.to_owned(),
        workspace: "conversations".to_owned(),
        agent: "chat-abc".to_owned(),
        conversation: conversation.parse().expect("conversation"),
        endpoint: endpoint.clone(),
    }
}

fn config(home: &std::path::Path) -> WakeConfig {
    let mut config = WakeConfig::defaults(home);
    config.min_interval = Duration::ZERO;
    config
}

const WAKE_LINE: &str =
    "[xagent] 1 unread message for conversations/chat-abc (1 DM). Check your xagent inbox.";

/// A thread is woken only when idle (the busy one is re-read until it is), exactly one line; a
/// sub-agent thread and an unknown one never.
#[tokio::test]
async fn codex_thread_is_woken_only_when_idle_and_only_if_a_user_thread() {
    let home = tempfile::tempdir().expect("home");
    let config = config(home.path());
    let socket = home.path().join("app.sock");
    let listener = UnixListener::bind(&socket).expect("fake app-server");
    let codex = Endpoint::Codex {
        control_socket: socket,
    };
    let state = Arc::new(Mutex::new((String::from("active"), Vec::<Value>::new())));
    let served = Arc::clone(&state);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let state = Arc::clone(&served);
            tokio::spawn(async move {
                let mut ws = tokio_tungstenite::accept_async(stream)
                    .await
                    .expect("handshake");
                while let Some(Ok(Message::Text(text))) = ws.next().await {
                    let msg: Value = serde_json::from_str(&text).expect("json");
                    assert!(
                        msg.get("jsonrpc").is_none(),
                        "app-server messages carry no jsonrpc"
                    );
                    let Some(id) = msg.get("id") else { continue };
                    let result = match msg["method"].as_str() {
                        Some("thread/read") if msg["params"]["threadId"] == SUB => {
                            json!({"thread": {
                            "status": {"type": "idle"},
                            "source": {"subAgent": {"thread_spawn": {"parent_thread_id": ID}}}}})
                        }
                        Some("thread/read") if msg["params"]["threadId"] == ID => {
                            json!({"thread": {"status": {"type": state.lock().expect("lock").0}}})
                        }
                        Some("turn/start") => {
                            state.lock().expect("lock").1.push(msg["params"].clone());
                            json!({})
                        }
                        _ => json!({}),
                    };
                    ws.send(Message::text(
                        json!({"id": id, "result": result}).to_string(),
                    ))
                    .await
                    .expect("reply");
                }
            });
        }
    });

    let waker = Waker::new(config, Arc::new(Source));
    waker.delivered(target(&codex, SUB));
    waker.delivered(target(&codex, "ffffffff-0000-0000-0000-000000000000"));
    waker.delivered(target(&codex, ID));
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert!(
        state.lock().expect("lock").1.is_empty(),
        "no turn while the thread is active"
    );

    state.lock().expect("lock").0 = "idle".to_owned();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let started = state.lock().expect("lock").1.clone();
    assert_eq!(
        started,
        vec![json!({"threadId": ID, "input": [{"type": "text", "text": WAKE_LINE}]})]
    );
}

/// dsh: login with the newest token only, no RPC without the cookie, then one queued prompt.
#[tokio::test]
async fn dsh_session_is_prompted_through_its_rpc_with_the_newest_token() {
    let home = tempfile::tempdir().expect("home");
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("fake dsh");
    let port = listener.local_addr().expect("addr").port();
    let mut config = config(home.path());
    config.dsh_url = format!("http://127.0.0.1:{port}");
    config.dsh_web_log = home.path().join("web.log");
    std::fs::write(
        &config.dsh_web_log,
        format!(
            "http://127.0.0.1:{port}/?token=stale\nhttp://127.0.0.1:{port}/?token=fresh\n\
             http://127.0.0.1:1/?token=otherport\n"
        ),
    )
    .expect("web log");
    let prompts = Arc::new(Mutex::new(Vec::<Value>::new()));
    let served = Arc::clone(&prompts);
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let prompts = Arc::clone(&served);
            tokio::spawn(async move {
                let mut buf = Vec::new();
                let (head, body) = loop {
                    let mut chunk = [0u8; 4096];
                    let n = stream.read(&mut chunk).await.expect("read");
                    buf.extend_from_slice(&chunk[..n]);
                    let text = String::from_utf8_lossy(&buf).into_owned();
                    if let Some((head, body)) = text.split_once("\r\n\r\n") {
                        let want = head
                            .lines()
                            .find_map(|l| {
                                l.to_ascii_lowercase()
                                    .strip_prefix("content-length: ")
                                    .map(|v| v.trim().parse::<usize>().expect("len"))
                            })
                            .unwrap_or(0);
                        if body.len() >= want {
                            break (head.to_owned(), body.to_owned());
                        }
                    }
                    assert!(n > 0, "connection closed early");
                };
                let request_line = head.lines().next().expect("request line").to_owned();
                let has_cookie = head.to_ascii_lowercase().contains("cookie: sid=ok");
                let (status, extra, payload) = if request_line.starts_with("GET /?token=fresh ") {
                    (
                        "200 OK",
                        "Set-Cookie: sid=ok; Path=/\r\n",
                        String::from("ok"),
                    )
                } else if request_line.starts_with("GET ") || !has_cookie {
                    ("401 Unauthorized", "", String::new())
                } else if request_line.contains("/api/session/list") {
                    let list = json!({"result": {"ok": true, "value": {"items": [
                        {"sessionId": "dsh-session-1", "running": false}]}}});
                    (
                        "200 OK",
                        "Content-Type: application/json\r\n",
                        list.to_string(),
                    )
                } else {
                    let call: Value = serde_json::from_str(&body).expect("rpc body");
                    prompts
                        .lock()
                        .expect("lock")
                        .push(call["payload"]["args"]["request"].clone());
                    let ok = json!({"result": {"ok": true, "value": {"accepted": true}}});
                    (
                        "200 OK",
                        "Content-Type: application/json\r\n",
                        ok.to_string(),
                    )
                };
                let reply = format!(
                    "HTTP/1.1 {status}\r\n{extra}Content-Length: {}\r\nConnection: close\r\n\r\n{payload}",
                    payload.len()
                );
                stream.write_all(reply.as_bytes()).await.expect("reply");
            });
        }
    });

    let waker = Waker::new(config, Arc::new(Source));
    waker.delivered(target(&Endpoint::Dsh, "dsh-session-1"));
    tokio::time::sleep(Duration::from_secs(3)).await;
    let sent = prompts.lock().expect("lock").clone();
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert_eq!(sent[0]["sessionId"], "dsh-session-1");
    assert_eq!(sent[0]["mode"], "queue");
    assert_eq!(
        sent[0]["content"],
        json!([{"type": "text", "text": WAKE_LINE}])
    );
    assert!(
        sent[0]["requestId"]
            .as_str()
            .expect("id")
            .starts_with("xagent-wake-")
    );
}
