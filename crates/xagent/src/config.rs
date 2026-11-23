//! Host configuration: `--config`, else `$XAGENT_CONFIG`, else `~/.config/xagent/config.toml`.
//!
//! Secrets are never in this file: it names a password file, read at startup.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use xagent_bus::{NatsSettings, UserPassword};
use xagent_core::{DaemonConfig, Project};
use xagent_wake::WakeConfig;

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    host_id: Option<String>,
    #[serde(default)]
    nats: NatsSection,
    #[serde(default)]
    daemon: DaemonSection,
    #[serde(default)]
    notify: NotifySection,
    #[serde(default)]
    projects: Vec<ProjectSection>,
}

/// `[notify]`: the wake policy and its transports.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotifySection {
    wake: Option<bool>,
    wake_min_interval_seconds: Option<f64>,
    wake_max_wait_seconds: Option<f64>,
    claude_sessions_dir: Option<PathBuf>,
    codex_control_socket: Option<PathBuf>,
    dsh_url: Option<String>,
    dsh_web_log: Option<PathBuf>,
}

/// `[[projects]]`: a directory tree whose sessions join `topic`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectSection {
    path: PathBuf,
    /// Root topic (one token, e.g. `editor-auto`).
    topic: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct NatsSection {
    #[serde(default = "default_servers")]
    servers: Vec<String>,
    user: Option<String>,
    password_file: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
}

impl Default for NatsSection {
    fn default() -> Self {
        Self {
            servers: default_servers(),
            user: None,
            password_file: None,
            tls_ca: None,
        }
    }
}

fn default_servers() -> Vec<String> {
    vec!["nats://127.0.0.1:4222".to_owned()]
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct DaemonSection {
    socket: Option<PathBuf>,
    state_db: Option<PathBuf>,
}

/// The resolved configuration.
#[derive(Debug)]
pub struct Config {
    file: File,
    home: PathBuf,
    /// The config file that was read; `None` when the defaults apply.
    source: Option<PathBuf>,
}

fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}

/// `~/x` relative to `home`; anything else unchanged.
fn expand(path: &Path, home: &Path) -> PathBuf {
    match path.strip_prefix("~") {
        Ok(rest) => home.join(rest),
        Err(_) => path.to_owned(),
    }
}

impl Config {
    /// Loads the config file; a missing file at the default location means all defaults.
    pub fn load(explicit: Option<&Path>) -> Result<Self> {
        let home = home()?;
        let (path, required) = match explicit {
            Some(path) => (path.to_owned(), true),
            None => match std::env::var_os("XAGENT_CONFIG") {
                Some(path) => (expand(Path::new(&path), &home), true),
                None => (home.join(".config/xagent/config.toml"), false),
            },
        };
        let (file, source) = match std::fs::read_to_string(&path) {
            Ok(text) => (
                toml::from_str(&text)
                    .with_context(|| format!("invalid config {}", path.display()))?,
                Some(path),
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound && !required => {
                (File::default(), None)
            }
            Err(err) => {
                return Err(err).with_context(|| format!("reading config {}", path.display()));
            }
        };
        Ok(Self { file, home, source })
    }

    pub fn source(&self) -> Option<&Path> {
        self.source.as_deref()
    }

    pub fn nats_servers(&self) -> &[String] {
        &self.file.nats.servers
    }

    /// The NATS password file and CA bundle named by the config, `~` expanded.
    pub fn nats_files(&self) -> (Option<PathBuf>, Option<PathBuf>) {
        let nats = &self.file.nats;
        (
            nats.password_file.as_deref().map(|p| expand(p, &self.home)),
            nats.tls_ca.as_deref().map(|p| expand(p, &self.home)),
        )
    }

    pub fn state_db(&self) -> PathBuf {
        match &self.file.daemon.state_db {
            Some(path) => expand(path, &self.home),
            None => self.home.join(".local/share/xagent/xagent.db"),
        }
    }

    /// The wake settings: `[notify]` over the defaults.
    pub fn wake(&self) -> Result<WakeConfig> {
        let n = &self.file.notify;
        let mut wake = WakeConfig::defaults(&self.home);
        let seconds = |name: &str, value: f64| -> Result<Duration> {
            Duration::try_from_secs_f64(value)
                .with_context(|| format!("[notify] {name} must be a non-negative number"))
        };
        wake.enabled = n.wake.unwrap_or(wake.enabled);
        if let Some(s) = n.wake_min_interval_seconds {
            wake.min_interval = seconds("wake_min_interval_seconds", s)?;
        }
        if let Some(s) = n.wake_max_wait_seconds {
            wake.max_wait = seconds("wake_max_wait_seconds", s)?;
        }
        if let Some(p) = &n.claude_sessions_dir {
            wake.claude_sessions_dir = expand(p, &self.home);
        }
        if let Some(p) = &n.codex_control_socket {
            wake.codex_control_socket = expand(p, &self.home);
        }
        if let Some(p) = &n.dsh_web_log {
            wake.dsh_web_log = expand(p, &self.home);
        }
        if let Some(url) = &n.dsh_url {
            anyhow::ensure!(
                xagent_wake::is_loopback_url(url),
                "[notify] dsh_url must be a loopback http URL: the dsh login token is sent there"
            );
            wake.dsh_url.clone_from(url);
        }
        Ok(wake)
    }

    /// The daemon socket the CLI talks to.
    pub fn socket(&self) -> PathBuf {
        match &self.file.daemon.socket {
            Some(path) => expand(path, &self.home),
            None => self.home.join(".local/state/xagent/xagentd.sock"),
        }
    }

    /// The socket the daemon serves MCP on, next to its IPC socket.
    pub fn mcp_socket(&self) -> PathBuf {
        self.socket().with_file_name("mcp.sock")
    }

    /// The home directory of the user running xagent.
    pub fn home(&self) -> &Path {
        &self.home
    }

    /// Everything the daemon needs, with the NATS password read from its file.
    pub fn daemon(&self) -> Result<DaemonConfig> {
        let nats = &self.file.nats;
        let auth = match (&nats.user, &nats.password_file) {
            (Some(user), Some(file)) => {
                let file = expand(file, &self.home);
                let password = std::fs::read_to_string(&file)
                    .with_context(|| format!("reading NATS password file {}", file.display()))?;
                Some(UserPassword {
                    user: user.clone(),
                    password: password.trim().to_owned(),
                })
            }
            (None, None) => None,
            _ => anyhow::bail!("[nats] user and password_file go together"),
        };
        let host_id = match &self.file.host_id {
            Some(id) => id.clone(),
            None => gethostname::gethostname()
                .to_string_lossy()
                .split('.')
                .next()
                .unwrap_or_default()
                .to_owned(),
        };
        let projects = self
            .file
            .projects
            .iter()
            .map(|p| {
                let valid =
                    xagent_proto::validate_topic(&p.topic).is_ok() && !p.topic.contains('.');
                anyhow::ensure!(
                    valid,
                    "[[projects]] {}: topic {:?} must be one topic token (a-z 0-9 _ -)",
                    p.path.display(),
                    p.topic
                );
                Ok(Project {
                    path: expand(&p.path, &self.home),
                    topic: p.topic.clone(),
                })
            })
            .collect::<Result<_>>()?;
        Ok(DaemonConfig {
            projects,
            nats: NatsSettings {
                servers: nats.servers.clone(),
                auth,
                tls_ca: nats.tls_ca.as_deref().map(|p| expand(p, &self.home)),
                client_name: format!("xagentd@{host_id}"),
            },
            host_id,
            socket: self.socket(),
            state_db: self.state_db(),
            wake: self.wake()?,
        })
    }
}
