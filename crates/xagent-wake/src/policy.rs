//! The wake policy, shared by every transport.
//!
//! * Coalesced per target: one pending wake at a time, at most one line per `min_interval`, and a
//!   new wake only for messages that arrived after the last one.
//! * A wake waits until its transport says the agent is idle; one that cannot find an idle moment
//!   within `max_wait` is dropped (the messages stay unread for the next manual check).
//! * Just before sending, the unread counts are re-read: nothing is sent when they dropped to
//!   zero, or when the client's own Stop hook already reported exactly this state.

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::time::Instant;
use tracing::{debug, info};
use xagent_proto::{Client, Conversation};

use crate::claude::ClaudeInbox;
use crate::codex::CodexAppServer;
use crate::dsh::Dsh;
use crate::text::Counts;

/// Longest single sleep while waiting for the rate limit.
const RATE_SLEEP_MAX: Duration = Duration::from_secs(5);
/// Shortest re-check delay while the agent is busy.
const WAIT_MIN: Duration = Duration::from_millis(500);

/// Wake settings (`[notify]` of the host config).
#[derive(Debug, Clone)]
pub struct WakeConfig {
    pub enabled: bool,
    /// At most one wake line per target in this interval.
    pub min_interval: Duration,
    /// A wake that finds no idle moment for this long is dropped.
    pub max_wait: Duration,
    /// Claude Code's live-session registry (`<pid>.json` per interactive session).
    pub claude_sessions_dir: PathBuf,
    /// The shared Codex app-server's control socket.
    pub codex_control_socket: PathBuf,
    /// dsh's local RPC endpoint (loopback only) and the launch log with its login token.
    pub dsh_url: String,
    pub dsh_web_log: PathBuf,
}

impl WakeConfig {
    /// The defaults for a user whose home directory is `home`.
    pub fn defaults(home: &std::path::Path) -> Self {
        Self {
            enabled: true,
            min_interval: Duration::from_secs(120),
            max_wait: Duration::from_secs(1800),
            claude_sessions_dir: home.join(".claude/sessions"),
            codex_control_socket: home.join(".codex/app-server-control/app-server-control.sock"),
            dsh_url: "http://127.0.0.1:3080".to_owned(),
            dsh_web_log: home.join(".dsh/web.log"),
        }
    }
}

/// Whom to wake: a conversation of one client, and the agent identity it holds.
#[derive(Debug, Clone)]
pub struct Target {
    /// Coalescing key (the agent id, or a listener key for an unbound conversation).
    pub key: String,
    pub workspace: String,
    pub agent: String,
    pub client: Client,
    /// The client's native conversation id: Claude session id, Codex thread id, dsh session id.
    pub conversation: Conversation,
    /// The Claude process id when known (the registry is then read by pid, which survives a
    /// `/clear`); the session-id scan is the fallback.
    pub claude_pid: Option<u32>,
}

/// What the daemon tells the policy about a target.
pub trait WakeSource: Send + Sync + Debug {
    /// Whether the target's session still exists (a closed one is never woken).
    fn is_open(&self, target: &Target) -> bool;
    /// The target's unread counters now; `None` when nothing is known.
    fn counts(&self, target: &Target) -> Option<Counts>;
    /// Whether the client's Stop hook already reported exactly this inbox state, with no message
    /// arriving after the reminder.
    fn already_told(&self, target: &Target, counts: &Counts) -> bool;
}

/// A transport's answer to "can I deliver now?".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    /// Not idle yet: ask again after the delay.
    Wait(Duration, &'static str),
    /// The transport cannot reach the agent: drop this wake.
    Gone(String),
}

#[derive(Debug, Default)]
struct KeyState {
    running: bool,
    /// Messages that arrived since the wake last read the counts.
    new: u32,
    last_sent: Option<Instant>,
}

#[derive(Debug)]
struct Inner {
    config: WakeConfig,
    source: Arc<dyn WakeSource>,
    claude: ClaudeInbox,
    codex: CodexAppServer,
    dsh: Dsh,
    state: Mutex<HashMap<String, KeyState>>,
}

/// The wake policy. Cheap to clone.
#[derive(Debug, Clone)]
pub struct Waker {
    inner: Arc<Inner>,
}

impl Waker {
    pub fn new(config: WakeConfig, source: Arc<dyn WakeSource>) -> Self {
        let inner = Inner {
            claude: ClaudeInbox::new(config.claude_sessions_dir.clone()),
            codex: CodexAppServer::new(config.codex_control_socket.clone()),
            dsh: Dsh::new(&config.dsh_url, config.dsh_web_log.clone()),
            config,
            source,
            state: Mutex::new(HashMap::new()),
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// A message was stored for `target`: wake it unless a wake for it is already pending.
    pub fn delivered(&self, target: Target) {
        if !self.inner.config.enabled || !self.inner.reaches(&target) {
            return;
        }
        let mut state = self.inner.state();
        let entry = state.entry(target.key.clone()).or_default();
        entry.new += 1;
        if !entry.running {
            entry.running = true;
            tokio::spawn(Arc::clone(&self.inner).wake_loop(target));
        }
    }
}

impl Inner {
    fn state(&self) -> MutexGuard<'_, HashMap<String, KeyState>> {
        self.state.lock().expect("wake state lock poisoned")
    }

    fn reaches(&self, target: &Target) -> bool {
        match target.client {
            Client::ClaudeCode => ClaudeInbox::reaches(target),
            Client::Codex => CodexAppServer::reaches(target),
            Client::Dsh => self.dsh.reaches(target),
        }
    }

    async fn ready(&self, target: &Target) -> Readiness {
        match target.client {
            Client::ClaudeCode => self.claude.ready(target),
            Client::Codex => self.codex.ready(target).await,
            Client::Dsh => self.dsh.ready(target).await,
        }
    }

    async fn send(&self, target: &Target, text: &str) -> bool {
        match target.client {
            Client::ClaudeCode => self.claude.send(target, text).await,
            Client::Codex => self.codex.send(target, text).await,
            Client::Dsh => self.dsh.send(target, text).await,
        }
    }

    /// Runs wakes for one target until no message arrived since the last one.
    async fn wake_loop(self: Arc<Self>, target: Target) {
        loop {
            self.wake_once(&target).await;
            let mut state = self.state();
            let entry = state.entry(target.key.clone()).or_default();
            if entry.new > 0 && self.source.is_open(&target) {
                // Messages arrived after the counts were read: wake again (rate limit applies).
                continue;
            }
            entry.running = false;
            entry.new = 0;
            return;
        }
    }

    fn reset_new(&self, key: &str) {
        self.state().entry(key.to_owned()).or_default().new = 0;
    }

    fn drop_wake(&self, target: &Target, reason: &str) {
        info!(agent = %target.agent, reason, "wake dropped");
        self.reset_new(&target.key); // dropped: only later messages start a new wake
    }

    async fn wake_once(&self, target: &Target) {
        let started = Instant::now();
        while self.source.is_open(target) {
            let last = self.state().get(&target.key).and_then(|s| s.last_sent);
            if let Some(last) = last {
                let remaining = self.config.min_interval.saturating_sub(last.elapsed());
                if !remaining.is_zero() {
                    tokio::time::sleep(remaining.min(RATE_SLEEP_MAX)).await;
                    continue;
                }
            }
            match self.ready(target).await {
                Readiness::Gone(reason) => return self.drop_wake(target, &reason),
                Readiness::Wait(retry, reason) => {
                    if started.elapsed() > self.config.max_wait {
                        return self.drop_wake(target, reason);
                    }
                    tokio::time::sleep(retry.max(WAIT_MIN)).await;
                    continue;
                }
                Readiness::Ready => {}
            }
            self.reset_new(&target.key);
            let Some(counts) = self.source.counts(target).filter(|c| c.unread_total > 0) else {
                return; // already read meanwhile
            };
            if self.source.already_told(target, &counts) {
                debug!(agent = %target.agent, "wake skipped: the Stop hook already reported this state");
                return;
            }
            let text = counts.line(&target.workspace, &target.agent);
            if !self.send(target, &text).await {
                return self.drop_wake(target, "transport refused");
            }
            self.state()
                .entry(target.key.clone())
                .or_default()
                .last_sent = Some(Instant::now());
            info!(agent = %target.agent, unread = counts.unread_total, "woke agent");
            return;
        }
    }
}
