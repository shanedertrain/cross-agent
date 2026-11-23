//! Codex app-server wake.
//!
//! The Codex TUI runs its threads in a shared app-server whose control socket any same-user
//! client may join: JSON-RPC (messages without `"jsonrpc"`) in WebSocket text frames over a Unix
//! socket. `turn/start` on a loaded thread starts a turn the TUI renders and answers. Before
//! starting one the thread is re-read and must be idle: `turn/start` on an active thread is folded
//! into the running turn. Ephemeral threads (Codex's title generator) and sub-agent threads are
//! never woken.

use std::path::{Path, PathBuf};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::UnixStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{WebSocketStream, client_async_with_config};

use crate::error::WakeError;
use crate::policy::{Readiness, Target};

const TIMEOUT: Duration = Duration::from_secs(5);
const ACTIVE_RETRY: Duration = Duration::from_secs(3);
const MAX_FRAME: usize = 16 << 20;
/// Pages of `thread/loaded/list` read at most.
const MAX_PAGES: usize = 10;

/// A minimal JSON-RPC client of the control socket.
#[derive(Debug)]
struct AppServer {
    ws: WebSocketStream<UnixStream>,
    next_id: u64,
}

impl AppServer {
    async fn connect(path: &Path) -> Result<Self, WakeError> {
        let stream = tokio::time::timeout(TIMEOUT, UnixStream::connect(path)).await??;
        let config = WebSocketConfig::default()
            .max_message_size(Some(MAX_FRAME))
            .max_frame_size(Some(MAX_FRAME));
        let (ws, _) = tokio::time::timeout(
            TIMEOUT,
            client_async_with_config("ws://localhost/", stream, Some(config)),
        )
        .await??;
        let mut server = Self { ws, next_id: 1 };
        server
            .call(
                "initialize",
                json!({ "clientInfo": { "name": "xagent", "title": "xagent wake", "version": "1" } }),
            )
            .await?;
        server.send(&json!({ "method": "initialized" })).await?;
        Ok(server)
    }

    async fn send(&mut self, value: &Value) -> Result<(), WakeError> {
        self.ws.send(Message::text(value.to_string())).await?;
        Ok(())
    }

    async fn call(&mut self, method: &str, params: Value) -> Result<Value, WakeError> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({ "id": id, "method": method, "params": params }))
            .await?;
        let answer = async {
            while let Some(frame) = self.ws.next().await {
                let Message::Text(text) = frame? else {
                    continue; // pings are answered by the library; binary frames are not JSON-RPC
                };
                let msg: Value = serde_json::from_str(&text)
                    .map_err(|e| WakeError::Protocol(format!("{method}: {e}")))?;
                if msg.get("id") == Some(&json!(id)) && msg.get("method").is_none() {
                    return Ok(msg);
                }
            }
            Err(WakeError::Protocol(format!("{method}: connection closed")))
        };
        let msg = tokio::time::timeout(TIMEOUT, answer).await??;
        if let Some(error) = msg.get("error") {
            let text = error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("error");
            return Err(WakeError::Protocol(format!("{method}: {text}")));
        }
        Ok(msg.get("result").cloned().unwrap_or(Value::Null))
    }

    async fn thread(&mut self, id: &str) -> Result<Value, WakeError> {
        let result = self.call("thread/read", json!({ "threadId": id })).await?;
        Ok(result.get("thread").cloned().unwrap_or(Value::Null))
    }
}

/// `^[0-9a-fA-F][0-9a-fA-F-]{7,63}$`
fn is_thread_id(id: &str) -> bool {
    (8..=64).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_hexdigit() || b == b'-')
        && id.as_bytes()[0] != b'-'
}

/// A user thread loaded in the app-server, and the directory it runs in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadedThread {
    pub id: String,
    pub cwd: PathBuf,
}

fn is_user_thread(thread: &Value) -> bool {
    thread.is_object()
        && thread.get("ephemeral").and_then(Value::as_bool) != Some(true)
        && !is_sub_agent(thread)
}

fn is_sub_agent(thread: &Value) -> bool {
    thread
        .pointer("/source/subAgent/thread_spawn/parent_thread_id")
        .and_then(Value::as_str)
        .is_some_and(is_thread_id)
}

/// Whether the thread can be woken now. Only a user thread is.
fn judge(thread: &Value) -> Readiness {
    if !is_user_thread(thread) {
        return Readiness::Gone("not a user thread".to_owned());
    }
    match thread.pointer("/status/type").and_then(Value::as_str) {
        Some("idle") => Readiness::Ready,
        Some("active") => Readiness::Wait(ACTIVE_RETRY, "thread busy"),
        other => Readiness::Gone(format!("thread {}", other.unwrap_or("unknown"))),
    }
}

/// Wakes a Codex thread through the shared app-server.
#[derive(Debug, Clone)]
pub struct CodexAppServer {
    socket: PathBuf,
}

impl CodexAppServer {
    pub fn new(socket: PathBuf) -> Self {
        Self { socket }
    }

    /// The user threads loaded in the app-server (how the daemon finds the thread of a Codex
    /// window that has not named it in a tool call yet).
    ///
    /// # Errors
    ///
    /// When the app-server cannot be reached or answers out of protocol.
    pub async fn loaded_user_threads(&self) -> Result<Vec<LoadedThread>, WakeError> {
        let mut server = AppServer::connect(&self.socket).await?;
        let mut ids = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let params = cursor.map_or_else(|| json!({}), |c| json!({ "cursor": c }));
            let page = server.call("thread/loaded/list", params).await?;
            ids.extend(
                page.get("data")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                    .filter(|id| is_thread_id(id))
                    .map(str::to_owned),
            );
            cursor = page
                .get("nextCursor")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        let mut threads = Vec::new();
        for id in ids {
            let thread = server.thread(&id).await?;
            if let Some(cwd) = thread.get("cwd").and_then(Value::as_str)
                && is_user_thread(&thread)
            {
                threads.push(LoadedThread {
                    id,
                    cwd: PathBuf::from(cwd),
                });
            }
        }
        Ok(threads)
    }

    /// For Codex the conversation id is the thread id.
    pub(crate) fn reaches(target: &Target) -> bool {
        is_thread_id(target.conversation.as_str())
    }

    pub(crate) async fn ready(&self, target: &Target) -> Readiness {
        let result = async {
            let mut server = AppServer::connect(&self.socket).await?;
            Ok::<_, WakeError>(judge(&server.thread(target.conversation.as_str()).await?))
        }
        .await;
        result.unwrap_or_else(|err| Readiness::Gone(format!("app-server: {err}")))
    }

    pub(crate) async fn send(&self, target: &Target, text: &str) -> bool {
        let thread = target.conversation.as_str();
        let result = async {
            let mut server = AppServer::connect(&self.socket).await?;
            // Re-check in the same breath as the start.
            if !matches!(judge(&server.thread(thread).await?), Readiness::Ready) {
                return Ok(false);
            }
            server
                .call(
                    "turn/start",
                    json!({ "threadId": thread, "input": [{ "type": "text", "text": text }] }),
                )
                .await?;
            Ok::<_, WakeError>(true)
        }
        .await;
        match result {
            Ok(sent) => sent,
            Err(err) => {
                tracing::warn!(error = %err, "codex wake failed");
                false
            }
        }
    }
}
