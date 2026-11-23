//! `xagent doctor`: make installation problems obvious.
//!
//! Local checks run directly, so a broken daemon does not hide a broken configuration and the
//! other way round; broker health comes from the daemon's own status.

use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use serde_json::Value;
use xagent_core::protocol::Call;
use xagent_wake::ClaudeInbox;

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
            "copy config.example.toml to ~/.config/xagent/config.toml",
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

    let (password_file, tls_ca) = config.nats_files();
    match password_file {
        None => report.add_fix(
            "nats credentials",
            Verdict::Warn,
            "none configured (anonymous)",
            "set [nats] user + password_file",
        ),
        Some(path) => match owner_only(&path) {
            None => report.add_fix(
                "nats credentials",
                Verdict::Fail,
                format!("{} missing", path.display()),
                "create it or fix the path in the config",
            ),
            Some(false) => report.add_fix(
                "nats credentials",
                Verdict::Warn,
                format!("{} readable by group or others", path.display()),
                format!("chmod 600 {}", path.display()),
            ),
            Some(true) => report.add(
                "nats credentials",
                Verdict::Pass,
                format!("{} (0600)", path.display()),
            ),
        },
    }
    if let Some(ca) = tls_ca {
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

async fn daemon_checks(config: &Config, report: &mut Report) {
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
            return;
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
}

fn wake_checks(config: &Config, report: &mut Report) {
    let Ok(wake) = config.wake() else { return };
    if !wake.enabled {
        report.add("claude inbox", Verdict::Skip, "wake disabled");
        return;
    }
    match ClaudeInbox::new(wake.claude_sessions_dir.clone()).sessions() {
        None => report.add(
            "claude inbox",
            Verdict::Skip,
            format!(
                "no Claude session registry at {}",
                wake.claude_sessions_dir.display()
            ),
        ),
        Some(sessions) if sessions.is_empty() => {
            report.add("claude inbox", Verdict::Skip, "no live Claude sessions");
        }
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
                    format!("{} live session(s), all reachable", sessions.len()),
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
    if wake.codex_control_socket.exists() {
        report.add(
            "codex app-server",
            Verdict::Pass,
            wake.codex_control_socket.display().to_string(),
        );
    } else {
        report.add(
            "codex app-server",
            Verdict::Skip,
            format!(
                "no control socket at {}",
                wake.codex_control_socket.display()
            ),
        );
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
}

/// Runs every check, prints the report, and returns whether none failed.
pub async fn run(config: &Config) -> bool {
    let mut report = Report::default();
    local_checks(config, &mut report);
    daemon_checks(config, &mut report).await;
    wake_checks(config, &mut report);
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
