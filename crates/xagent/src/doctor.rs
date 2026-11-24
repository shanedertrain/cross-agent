//! `xagent doctor`: make installation problems obvious.
//!
//! Local checks run directly, so a broken daemon does not hide a broken configuration and the
//! other way round; broker health comes from the daemon's own status.

use std::collections::BTreeSet;
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::Value;
use xagent_core::protocol::Call;

use xagent_wake::{ClaudeInbox, Endpoint, Hook};

use crate::config::Config;
use xagent_core::client::Client;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Warn,
    Fail,
    Skip,
}

impl Verdict {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "PASS",
            Self::Warn => "WARN",
            Self::Fail => "FAIL",
            Self::Skip => "SKIP",
        }
    }
}

#[derive(Debug)]
pub struct Check {
    pub name: &'static str,
    pub verdict: Verdict,
    pub detail: String,
    pub fix: Option<String>,
}

#[derive(Debug, Default)]
struct Report(Vec<Check>);

impl Report {
    fn add(&mut self, name: &'static str, verdict: Verdict, detail: impl Into<String>) {
        self.0.push(Check {
            name,
            verdict,
            detail: detail.into(),
            fix: None,
        });
    }

    fn add_fix(
        &mut self,
        name: &'static str,
        verdict: Verdict,
        detail: impl Into<String>,
        fix: impl Into<String>,
    ) {
        self.add(name, verdict, detail);
        self.0.last_mut().expect("just added").fix = Some(fix.into());
    }
}

fn owner_only(path: &Path) -> Option<bool> {
    let mode = std::fs::metadata(path).ok()?.permissions().mode();
    Some(mode.trailing_zeros() >= 6)
}

/// `nats://host:port` and friends to `host:port` (default port 4222).
fn host_port(server: &str) -> String {
    let rest = server.split_once("://").map_or(server, |(_, rest)| rest);
    let rest = rest.rsplit_once('@').map_or(rest, |(_, host)| host);
    if rest.contains(':') {
        rest.to_owned()
    } else {
        format!("{rest}:4222")
    }
}

fn reachable(server: &str) -> Result<(), String> {
    let addr = host_port(server);
    let target = addr
        .to_socket_addrs()
        .map_err(|e| e.to_string())?
        .next()
        .ok_or("no address")?;
    TcpStream::connect_timeout(&target, Duration::from_secs(2))
        .map(drop)
        .map_err(|e| e.to_string())
}

fn local_checks(config: &Config, report: &mut Report) {
    match config.source() {
        Some(path) => report.add("config", Verdict::Pass, path.display().to_string()),
        None => report.add_fix(
            "config",
            Verdict::Warn,
            "no config file; using defaults (nats://127.0.0.1:4222, no auth)",
            "copy config.example.toml to $XDG_CONFIG_HOME/xagent/config.toml (default ~/.config)",
        ),
    }
    match config.wake() {
        Ok(wake) => report.add(
            "wake",
            Verdict::Pass,
            if wake.enabled {
                "enabled"
            } else {
                "disabled by [notify] wake = false"
            },
        ),
        Err(err) => report.add("wake", Verdict::Fail, format!("{err:#}")),
    }

    match config.nats_credentials() {
        Err(err) => report.add("nats credentials", Verdict::Fail, format!("{err:#}")),
        Ok(None) => report.add_fix(
            "nats credentials",
            Verdict::Warn,
            "none configured (anonymous)",
            "set [nats] user + password_file, or credentials_file",
        ),
        Ok(Some(credentials)) => {
            let file = credentials.file();
            match owner_only(file) {
                None => report.add_fix(
                    "nats credentials",
                    Verdict::Fail,
                    format!("{} missing", file.display()),
                    "create it or fix the path in the config",
                ),
                Some(false) => report.add_fix(
                    "nats credentials",
                    Verdict::Warn,
                    format!("{} readable by group or others", file.display()),
                    format!("chmod 600 {}", file.display()),
                ),
                Some(true) => report.add(
                    "nats credentials",
                    Verdict::Pass,
                    format!("{} (0600)", file.display()),
                ),
            }
        }
    }
    if let Some(ca) = config.nats_tls_ca() {
        let verdict = if ca.exists() {
            Verdict::Pass
        } else {
            Verdict::Fail
        };
        report.add("nats tls_ca", verdict, ca.display().to_string());
    }
    for server in config.nats_servers() {
        match reachable(server) {
            Ok(()) => report.add(
                "nats server",
                Verdict::Pass,
                format!("{server} accepts connections"),
            ),
            Err(err) => report.add_fix(
                "nats server",
                Verdict::Fail,
                format!("{server}: {err}"),
                "is nats-server running? check [nats] servers",
            ),
        }
    }
    let db = config.state_db();
    match owner_only(&db) {
        Some(true) => report.add("state db", Verdict::Pass, db.display().to_string()),
        Some(false) => report.add_fix(
            "state db",
            Verdict::Warn,
            db.display().to_string(),
            format!("chmod 600 {}", db.display()),
        ),
        None => report.add(
            "state db",
            Verdict::Skip,
            format!("{} not created yet", db.display()),
        ),
    }
    match which_xagent() {
        Some(path) => report.add("xagent on PATH", Verdict::Pass, path),
        None => report.add_fix(
            "xagent on PATH",
            Verdict::Warn,
            "not found",
            "client configs must then use the absolute path to the binary",
        ),
    }
}

fn which_xagent() -> Option<String> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join("xagent"))
        .find(|path| path.is_file())
        .map(|path| path.display().to_string())
}

/// Checks the daemon and returns its status, when it answers.
async fn daemon_checks(config: &Config, report: &mut Report) -> Option<Value> {
    let socket = config.socket();
    let status = match Client::connect(&socket).await {
        Ok(mut client) => client.call(Call::Status).await,
        Err(err) => Err(err),
    };
    let status = match status {
        Ok(status) => status,
        Err(err) => {
            report.add_fix(
                "daemon",
                Verdict::Fail,
                err.message,
                "start it: `xagent daemon` (or the service unit)",
            );
            return None;
        }
    };
    report.add(
        "daemon",
        Verdict::Pass,
        format!(
            "{} (host {}, version {})",
            socket.display(),
            status["host_id"],
            status["version"]
        ),
    );
    if owner_only(&socket) == Some(false) {
        report.add_fix(
            "daemon socket",
            Verdict::Fail,
            "accessible by others",
            format!("chmod 600 {}", socket.display()),
        );
    }
    let nats = &status["nats"];
    if nats["ready"] == Value::Bool(true) {
        report.add(
            "jetstream",
            Verdict::Pass,
            "stream and buckets present and compatible",
        );
    } else if let Some(taken) = nats["host_id_taken"].as_str() {
        report.add_fix(
            "host_id",
            Verdict::Fail,
            taken,
            "set a unique host_id in this host's config, or stop the other daemon",
        );
    } else {
        let why = status["jetstream"]["fatal"].as_str().unwrap_or(
            if nats["connected"] == Value::Bool(true) {
                "bootstrapping"
            } else {
                "disconnected"
            },
        );
        report.add_fix(
            "jetstream",
            Verdict::Fail,
            format!("not ready: {why}"),
            "see the daemon log",
        );
    }
    if let Some(mismatches) = status["jetstream"]["mismatches"]
        .as_array()
        .filter(|m| !m.is_empty())
    {
        let list: Vec<&str> = mismatches.iter().filter_map(Value::as_str).collect();
        report.add("stream config", Verdict::Warn, list.join("; "));
    }
    for (state, verdict) in [("pending", Verdict::Warn), ("failed", Verdict::Warn)] {
        if let Some(n) = status["outbox"][state].as_u64().filter(|n| *n > 0) {
            report.add("outbox", verdict, format!("{n} {state} message(s)"));
        }
    }
    let sessions = status["sessions"].as_array().map_or(0, Vec::len);
    report.add("sessions", Verdict::Pass, format!("{sessions} open"));
    Some(status)
}

/// The Claude session registries and Codex control sockets to check: the clients' defaults, and
/// those of the daemon's live sessions (a client whose profile directory was moved is woken there).
fn wake_endpoints(
    config: &Config,
    status: Option<&Value>,
) -> (BTreeSet<PathBuf>, BTreeSet<PathBuf>) {
    let live = status
        .and_then(|s| s["sessions"].as_array())
        .into_iter()
        .flatten()
        .filter_map(|s| serde_json::from_value::<Endpoint>(s["endpoint"].clone()).ok());
    let defaults = [
        xagent_proto::Client::ClaudeCode,
        xagent_proto::Client::Codex,
    ]
    .map(|c| Endpoint::default_for(c, config.home()));
    let (mut claude, mut codex) = (BTreeSet::new(), BTreeSet::new());
    for endpoint in defaults.into_iter().chain(live) {
        match endpoint {
            Endpoint::ClaudeCode { sessions_dir, .. } => {
                claude.insert(sessions_dir);
            }
            Endpoint::Codex { control_socket } => {
                codex.insert(control_socket);
            }
            Endpoint::Dsh | Endpoint::Antigravity { .. } | Endpoint::Other { .. } => {}
        }
    }
    (claude, codex)
}

fn wake_checks(config: &Config, status: Option<&Value>, report: &mut Report) {
    let Ok(wake) = config.wake() else { return };
    if !wake.enabled {
        report.add("claude inbox", Verdict::Skip, "wake disabled");
        return;
    }
    let (claude, codex) = wake_endpoints(config, status);
    for dir in &claude {
        claude_check(dir, report);
    }
    for socket in &codex {
        if socket.exists() {
            report.add(
                "codex app-server",
                Verdict::Pass,
                socket.display().to_string(),
            );
        } else {
            report.add(
                "codex app-server",
                Verdict::Skip,
                format!("no control socket at {}", socket.display()),
            );
        }
    }
    if wake.dsh_web_log.exists() {
        report.add(
            "dsh",
            Verdict::Pass,
            format!("{} and {}", wake.dsh_url, wake.dsh_web_log.display()),
        );
    } else {
        report.add(
            "dsh",
            Verdict::Skip,
            format!("no launch log at {}", wake.dsh_web_log.display()),
        );
    }
    antigravity_check(config.home(), report);
    for (name, hook) in &wake.hooks {
        match hook {
            Hook::Command(argv) => match find_program(&argv[0]) {
                Some(path) => report.add(
                    "wake hook",
                    Verdict::Pass,
                    format!("{name}: {}", path.display()),
                ),
                None => report.add(
                    "wake hook",
                    Verdict::Fail,
                    format!(
                        "{name}: {} not found: fix wake_command in [clients.{name}]",
                        argv[0]
                    ),
                ),
            },
            // Not probed: a request would deliver a wake.
            Hook::Webhook { url, .. } => {
                report.add("wake hook", Verdict::Pass, format!("{name}: {url}"));
            }
        }
    }
}

/// `program` as the wake command would start it: a path, or a name looked up on `PATH`.
fn find_program(program: &str) -> Option<PathBuf> {
    if program.contains('/') {
        return Some(PathBuf::from(program)).filter(|p| p.is_file());
    }
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|dir| dir.join(program))
        .find(|p| p.is_file())
}

/// Checks whether Antigravity's global hooks file (`~/.gemini/config/hooks.json`) wires up xagent.
fn antigravity_check(home: &Path, report: &mut Report) {
    let config_dir = home.join(".gemini/config");
    if !config_dir.exists() {
        report.add(
            "antigravity",
            Verdict::Skip,
            format!("no Antigravity config at {}", config_dir.display()),
        );
        return;
    }
    let hooks_path = config_dir.join("hooks.json");
    let Ok(text) = std::fs::read_to_string(&hooks_path) else {
        report.add_fix(
            "antigravity",
            Verdict::Warn,
            format!("no hooks file at {}", hooks_path.display()),
            "run `xagent client-config --client antigravity` and add its hooks to ~/.gemini/config/hooks.json",
        );
        return;
    };
    let required = [
        "antigravity-bind",
        "antigravity-stop",
        "antigravity-sendmessage",
    ];
    let missing: Vec<&str> = required
        .into_iter()
        .filter(|name| !text.contains(name))
        .collect();
    if missing.is_empty() {
        report.add(
            "antigravity",
            Verdict::Pass,
            hooks_path.display().to_string(),
        );
    } else {
        report.add_fix(
            "antigravity",
            Verdict::Warn,
            format!(
                "{} is missing hook(s): {}",
                hooks_path.display(),
                missing.join(", ")
            ),
            "run `xagent client-config --client antigravity` to update ~/.gemini/config/hooks.json",
        );
    }
}

/// The live sessions of one Claude session registry and whether each inbox is reachable.
fn claude_check(dir: &Path, report: &mut Report) {
    match ClaudeInbox::new(dir.to_owned()).sessions() {
        None => report.add(
            "claude inbox",
            Verdict::Skip,
            format!("no Claude session registry at {}", dir.display()),
        ),
        Some(sessions) if sessions.is_empty() => report.add(
            "claude inbox",
            Verdict::Skip,
            format!("no live Claude sessions in {}", dir.display()),
        ),
        Some(sessions) => {
            let lost: Vec<String> = sessions
                .iter()
                .filter(|s| !s.reachable)
                .map(|s| s.name.clone().unwrap_or_else(|| s.pid.to_string()))
                .collect();
            if lost.is_empty() {
                report.add(
                    "claude inbox",
                    Verdict::Pass,
                    format!(
                        "{} live session(s) in {}, all reachable",
                        sessions.len(),
                        dir.display()
                    ),
                );
            } else {
                report.add_fix(
                    "claude inbox",
                    Verdict::Warn,
                    format!("{} of {} live session(s) have no usable inbox ({}); they are not woken, only the Stop hook reaches them", lost.len(), sessions.len(), lost.join(", ")),
                    "Claude changed its inbox, or the session predates it: restart the session",
                );
            }
        }
    }
}

/// Runs every check, prints the report, and returns whether none failed.
pub async fn run(config: &Config) -> bool {
    let mut report = Report::default();
    local_checks(config, &mut report);
    let status = daemon_checks(config, &mut report).await;
    wake_checks(config, status.as_ref(), &mut report);
    for check in &report.0 {
        print!(
            "{}  {:<18} {}",
            check.verdict.as_str(),
            check.name,
            check.detail
        );
        match &check.fix {
            Some(fix) => println!("\n      fix: {fix}"),
            None => println!(),
        }
    }
    report.0.iter().all(|c| c.verdict != Verdict::Fail)
}
