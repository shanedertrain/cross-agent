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
use xagent_proto::{AgentId, Conversation, Error, ErrorCode, WORKSPACE, conversation_agent_id};
use xagent_wake::{CodexAppServer, Endpoint, Target, sessions};

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

/// Adds the window target of `conversation` (the first window of an identity wins).
fn add(targets: &mut BTreeMap<AgentId, Target>, conversation: Conversation, endpoint: &Endpoint) {
    let agent = conversation_agent_id(endpoint.client(), &conversation);
    targets.entry(agent.clone()).or_insert_with(|| Target {
        key: agent.to_string(),
        workspace: WORKSPACE.to_owned(),
        agent: agent.to_string(),
        conversation,
        endpoint: endpoint.clone(),
    });
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

    pub(crate) fn window_endpoint(&self, id: WindowId) -> Option<Endpoint> {
        Some(self.windows().open.get(&id)?.endpoint.clone())
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
            let conversation = match &window.endpoint {
                Endpoint::Codex { .. } => continue,
                Endpoint::Dsh => window.conversation.clone(),
                Endpoint::ClaudeCode { sessions_dir, pid } => sessions::find_record(
                    sessions_dir,
                    *pid,
                    window.conversation.as_ref().map(Conversation::as_str),
                )
                .and_then(|record| record.session_id?.parse().ok()),
            };
            if let Some(conversation) = conversation {
                add(&mut targets, conversation, &window.endpoint);
            }
        }
        for (thread, endpoint) in self.codex_threads(&windows).await {
            add(&mut targets, thread, &endpoint);
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

    /// The user threads loaded in the directory of a Codex window, asked of the shared
    /// app-server of that window's Codex home; each with the endpoint it is woken at.
    async fn codex_threads(&self, windows: &[WindowOpen]) -> Vec<(Conversation, Endpoint)> {
        let mut dirs: HashMap<&PathBuf, HashSet<PathBuf>> = HashMap::new();
        for window in windows {
            if let Endpoint::Codex { control_socket } = &window.endpoint {
                dirs.entry(control_socket)
                    .or_default()
                    .insert(real(&window.cwd));
            }
        }
        let mut found = Vec::new();
        for (socket, dirs) in dirs {
            if !socket.exists() {
                continue;
            }
            let threads = match CodexAppServer::new(socket.clone())
                .loaded_user_threads()
                .await
            {
                Ok(threads) => threads,
                Err(err) => {
                    debug!(error = %err, socket = %socket.display(), "codex thread discovery failed");
                    continue;
                }
            };
            let endpoint = Endpoint::Codex {
                control_socket: socket.clone(),
            };
            found.extend(
                threads
                    .into_iter()
                    .filter(|t| dirs.contains(&real(&t.cwd)))
                    .filter_map(|t| t.id.parse().ok())
                    .map(|thread| (thread, endpoint.clone())),
            );
        }
        found
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
