//! Client windows: the MCP connections of running conversations, bound or not.
//!
//! An MCP connection claims no identity until its first tool call, yet messages may
//! already wait for the identity it would bind: a resumed conversation's, sent while it was
//! closed. Each connection declares itself a window on its daemon connection (`window_open`), and
//! the daemon peeks, by counters only (nothing is pulled or acknowledged, so no lease is needed),
//! at the identity of every window that holds no session. When more messages wait than at the
//! last peek, the window is woken with a "messages waiting" line.
//!
//! A window's conversation: dsh names it at launch; Claude's comes from its live registry record
//! (by pid, so a `/clear` is followed; a pre-warmed spare with no record is never woken); a Codex
//! server learns its thread only at a tool call, so the daemon asks the shared app-server for the
//! user threads loaded in the window's directory.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, MutexGuard};
use std::time::Duration;

use tracing::debug;
use xagent_proto::{
    AgentId, Client, Conversation, Error, ErrorCode, WORKSPACE, conversation_agent_id,
};
use xagent_wake::{CodexAppServer, Target, sessions};

use crate::daemon::Daemon;
use crate::protocol::WindowOpen;

/// How often unbound windows are peeked at (and at once when a window opens).
const PEEK_INTERVAL: Duration = Duration::from_secs(15);

/// A registered window, for the life of its daemon connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct WindowId(u64);

#[derive(Debug, Default)]
pub(crate) struct Windows {
    next_id: u64,
    open: HashMap<WindowId, WindowOpen>,
    /// Messages waiting per identity of an unbound window, at the last peek.
    peeked: HashMap<AgentId, u64>,
}

fn real(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_owned())
}

fn target(client: Client, conversation: Conversation, claude_pid: Option<u32>) -> Target {
    let agent = conversation_agent_id(client, &conversation).to_string();
    Target {
        key: agent.clone(),
        workspace: WORKSPACE.to_owned(),
        agent,
        client,
        conversation,
        claude_pid,
    }
}

/// Adds the window target of `conversation` (the first window of an identity wins).
fn add(
    targets: &mut BTreeMap<AgentId, Target>,
    client: Client,
    conversation: Conversation,
    claude_pid: Option<u32>,
) {
    targets
        .entry(conversation_agent_id(client, &conversation))
        .or_insert_with(|| target(client, conversation, claude_pid));
}

impl Daemon {
    fn windows(&self) -> MutexGuard<'_, Windows> {
        self.windows.lock().expect("windows lock poisoned")
    }

    pub(crate) fn open_window(&self, window: WindowOpen) -> Result<WindowId, Error> {
        if !window.cwd.is_absolute() {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "cwd must be an absolute path",
            ));
        }
        let id = {
            let mut windows = self.windows();
            windows.next_id += 1;
            let id = WindowId(windows.next_id);
            windows.open.insert(id, window);
            id
        };
        self.peek_kick.notify_one();
        Ok(id)
    }

    pub(crate) fn close_window(&self, id: WindowId) {
        self.windows().open.remove(&id);
    }

    pub(crate) fn window_claude_pid(&self, id: WindowId) -> Option<u32> {
        self.windows().open.get(&id)?.claude_pid
    }

    /// Messages waiting for `agent`, the identity of an unbound window, at the last peek.
    pub(crate) fn waiting(&self, agent: &str) -> Option<u64> {
        let agent: AgentId = agent.parse().ok()?;
        self.windows()
            .peeked
            .get(&agent)
            .copied()
            .filter(|&n| n > 0)
    }

    /// Peeks at unbound windows until the daemon stops.
    pub(crate) async fn peek_loop(self: Arc<Self>) {
        loop {
            tokio::select! {
                () = tokio::time::sleep(PEEK_INTERVAL) => {}
                () = self.peek_kick.notified() => {}
            }
            if self.config.wake.enabled {
                self.peek_unbound().await;
            }
        }
    }

    /// Wakes each unbound window whose identity has more messages waiting than at the last peek.
    async fn peek_unbound(&self) {
        let windows: Vec<WindowOpen> = self.windows().open.values().cloned().collect();
        let live: HashSet<AgentId> = self
            .sessions()
            .iter()
            .filter(|s| !s.is_closing())
            .map(|s| s.agent.clone())
            .collect();
        let mut targets = BTreeMap::new();
        for window in &windows {
            let conversation = match window.client {
                Client::Codex => continue,
                Client::Dsh => window.conversation.clone(),
                Client::ClaudeCode => sessions::find_record(
                    &self.config.wake.claude_sessions_dir,
                    window.claude_pid,
                    window.conversation.as_ref().map(Conversation::as_str),
                )
                .and_then(|record| record.session_id?.parse().ok()),
            };
            if let Some(conversation) = conversation {
                add(&mut targets, window.client, conversation, window.claude_pid);
            }
        }
        for thread in self.codex_threads(&windows).await {
            add(&mut targets, Client::Codex, thread, None);
        }
        targets.retain(|agent, _| !live.contains(agent));

        let mut counts = HashMap::new();
        for agent in targets.keys() {
            counts.insert(agent.clone(), self.waiting_count(agent).await);
        }
        let wakes: Vec<Target> = {
            let mut windows = self.windows();
            let previous = std::mem::replace(&mut windows.peeked, counts);
            targets
                .into_iter()
                .filter(|(agent, _)| {
                    windows.peeked[agent] > previous.get(agent).copied().unwrap_or(0)
                })
                .map(|(_, target)| target)
                .collect()
        };
        for target in wakes {
            debug!(
                agent = target.agent,
                "messages waiting for an unbound window"
            );
            self.waker.delivered(target);
        }
    }

    /// The user threads of the shared Codex app-server loaded in the directory of a Codex window.
    async fn codex_threads(&self, windows: &[WindowOpen]) -> Vec<Conversation> {
        let dirs: HashSet<PathBuf> = windows
            .iter()
            .filter(|w| w.client == Client::Codex)
            .map(|w| real(&w.cwd))
            .collect();
        let socket = &self.config.wake.codex_control_socket;
        if dirs.is_empty() || !socket.exists() {
            return Vec::new();
        }
        match CodexAppServer::new(socket.clone())
            .loaded_user_threads()
            .await
        {
            Ok(threads) => threads
                .into_iter()
                .filter(|t| dirs.contains(&real(&t.cwd)))
                .filter_map(|t| t.id.parse().ok())
                .collect(),
            Err(err) => {
                debug!(error = %err, "codex thread discovery failed");
                Vec::new()
            }
        }
    }

    /// Messages for an identity nobody holds: unread here, plus what its durable consumer still
    /// holds on the broker (none when it has no consumer).
    async fn waiting_count(&self, agent: &AgentId) -> u64 {
        let local = self
            .store()
            .inbox_counts(agent)
            .map_or(0, |c| c.unread_total);
        let remote = match self.bus() {
            Some(bus) => bus
                .consumer_backlog(WORKSPACE, agent)
                .await
                .map_or(0, |(pending, ack_pending)| pending + ack_pending),
            None => 0,
        };
        local + remote
    }
}
