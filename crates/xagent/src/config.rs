//! Host configuration: `--config`, else `$XAGENT_CONFIG`, else `$XDG_CONFIG_HOME/xagent/config.toml`.
//!
//! Default paths follow the XDG base directory spec: `$XDG_CONFIG_HOME`, `$XDG_DATA_HOME` and
//! `$XDG_STATE_HOME` when set to an absolute path, else `~/.config`, `~/.local/share` and
//! `~/.local/state`.
//!
//! Secrets are never in this file: it names a password or credentials file, read at startup.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::Deserialize;
use xagent_bus::{NatsAuth, NatsSettings, UserPassword};
use xagent_core::{DaemonConfig, Project};
use xagent_proto::Client;
use xagent_wake::{Hook, WakeConfig};

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
    #[serde(default)]
    retention: RetentionSection,
    #[serde(default)]
    clients: BTreeMap<String, ClientSection>,
}

/// `[clients.<name>]`: how a client that is not built in is woken. Exactly one of `wake_command`
/// and `wake_url`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientSection {
    #[serde(rename = "wake_command")]
    command: Option<Vec<String>>,
    #[serde(rename = "wake_url")]
    url: Option<String>,
    /// File holding a bearer token for `wake_url`.
    #[serde(rename = "wake_token_file")]
    token_file: Option<PathBuf>,
}

/// `[retention]`: how long this host keeps local copies. The broker's retention, shared by every
/// host, is set with `xagent retention --days N` and caps this.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RetentionSection {
    #[serde(default = "default_retention_days")]
    local_days: u32,
}

impl Default for RetentionSection {
    fn default() -> Self {
        Self {
            local_days: default_retention_days(),
        }
    }
}

fn default_retention_days() -> u32 {
    7
}

/// `[notify]`: the wake policy and its transports.
#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct NotifySection {
    wake: Option<bool>,
    wake_min_interval_seconds: Option<f64>,
    wake_max_wait_seconds: Option<f64>,
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
    /// A `.creds` file (JWT and `NKey` seed); excludes `user` and `password_file`.
    credentials_file: Option<PathBuf>,
    tls_ca: Option<PathBuf>,
}

impl Default for NatsSection {
    fn default() -> Self {
        Self {
            servers: default_servers(),
            user: None,
            password_file: None,
            credentials_file: None,
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

/// Where the NATS credentials come from, validated, paths `~` expanded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NatsCredentials {
    /// `user` with the password in `password_file`.
    Password { user: String, file: PathBuf },
    /// A `.creds` file for JWT/NKey auth.
    CredentialsFile(PathBuf),
}

impl NatsCredentials {
    /// The secret file, which must be readable by its owner only.
    pub fn file(&self) -> &Path {
        match self {
            Self::Password { file, .. } | Self::CredentialsFile(file) => file,
        }
    }
}

/// The resolved configuration.
#[derive(Debug)]
pub struct Config {
    file: File,
    home: PathBuf,
    /// `$XDG_DATA_HOME/xagent`: the default state database lives here.
    data_dir: PathBuf,
    /// `$XDG_STATE_HOME/xagent`: the default sockets live here.
    state_dir: PathBuf,
    /// The config file that was read; `None` when the defaults apply.
    source: Option<PathBuf>,
}

fn home() -> Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .context("HOME is not set")
}

/// The xagent directory under an XDG base directory: `$var` when it is an absolute path (the spec
/// ignores relative ones), else `home/fallback`.
fn xdg_dir(var: &str, home: &Path, fallback: &str) -> PathBuf {
    std::env::var_os(var)
        .map(PathBuf::from)
        .filter(|dir| dir.is_absolute())
        .unwrap_or_else(|| home.join(fallback))
        .join("xagent")
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
                None => (
                    xdg_dir("XDG_CONFIG_HOME", &home, ".config").join("config.toml"),
                    false,
                ),
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
        Ok(Self {
            file,
            data_dir: xdg_dir("XDG_DATA_HOME", &home, ".local/share"),
            state_dir: xdg_dir("XDG_STATE_HOME", &home, ".local/state"),
            home,
            source,
        })
    }

    pub fn source(&self) -> Option<&Path> {
        self.source.as_deref()
    }

    pub fn nats_servers(&self) -> &[String] {
        &self.file.nats.servers
    }

    /// The NATS credentials named by `[nats]`; `None` is anonymous.
    pub fn nats_credentials(&self) -> Result<Option<NatsCredentials>> {
        let nats = &self.file.nats;
        match (&nats.user, &nats.password_file, &nats.credentials_file) {
            (None, None, None) => Ok(None),
            (Some(user), Some(file), None) => Ok(Some(NatsCredentials::Password {
                user: user.clone(),
                file: expand(file, &self.home),
            })),
            (None, None, Some(file)) => Ok(Some(NatsCredentials::CredentialsFile(expand(
                file, &self.home,
            )))),
            (_, _, Some(_)) => anyhow::bail!(
                "[nats] credentials_file excludes user and password_file: use one or the other"
            ),
            _ => anyhow::bail!("[nats] user and password_file go together"),
        }
    }

    /// The CA bundle named by `[nats] tls_ca`, `~` expanded.
    pub fn nats_tls_ca(&self) -> Option<PathBuf> {
        self.file
            .nats
            .tls_ca
            .as_deref()
            .map(|p| expand(p, &self.home))
    }

    pub fn state_db(&self) -> PathBuf {
        match &self.file.daemon.state_db {
            Some(path) => expand(path, &self.home),
            None => self.data_dir.join("xagent.db"),
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
        for (name, section) in &self.file.clients {
            let (key, hook) = self.hook(name, section)?;
            wake.hooks.insert(key, hook);
        }
        Ok(wake)
    }

    fn hook(&self, name: &str, section: &ClientSection) -> Result<(String, Hook)> {
        let at = format!("[clients.{name}]");
        let parsed = name.parse::<Client>().ok();
        let canonical = match parsed {
            Some(Client::Antigravity) => Client::Antigravity.as_str().to_owned(),
            Some(Client::Other(other)) => other,
            _ => anyhow::bail!(
                "{at}: a client name is lowercase [a-z][a-z0-9-]* and not claude-code, codex, \
                 or dsh (which have their own wake transports)"
            ),
        };
        let token = section
            .token_file
            .as_ref()
            .map(|path| {
                let path = expand(path, &self.home);
                std::fs::read_to_string(&path)
                    .map(|t| t.trim().to_owned())
                    .with_context(|| format!("{at} wake_token_file {}", path.display()))
            })
            .transpose()?;
        let hook = match (&section.command, &section.url) {
            (Some(argv), None) => {
                anyhow::ensure!(token.is_none(), "{at}: wake_token_file goes with wake_url");
                anyhow::ensure!(
                    argv.first().is_some_and(|p| !p.is_empty()),
                    "{at}: wake_command needs a program"
                );
                Hook::Command(argv.clone())
            }
            (None, Some(url)) => {
                Hook::webhook(url, token).map_err(|e| anyhow::anyhow!("{at}: {e}"))?
            }
            _ => anyhow::bail!("{at}: set exactly one of wake_command and wake_url"),
        };
        Ok((canonical, hook))
    }

    /// The daemon socket the CLI talks to.
    pub fn socket(&self) -> PathBuf {
        match &self.file.daemon.socket {
            Some(path) => expand(path, &self.home),
            None => self.state_dir.join("xagentd.sock"),
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
        let auth = match self.nats_credentials()? {
            Some(NatsCredentials::Password { user, file }) => {
                let password = std::fs::read_to_string(&file)
                    .with_context(|| format!("reading NATS password file {}", file.display()))?;
                Some(NatsAuth::UserPassword(UserPassword {
                    user,
                    password: password.trim().to_owned(),
                }))
            }
            Some(NatsCredentials::CredentialsFile(file)) => Some(NatsAuth::CredentialsFile(file)),
            None => None,
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
        anyhow::ensure!(
            xagent_proto::is_valid_host_id(&host_id),
            "host_id {host_id:?} must be 1-64 of A-Z a-z 0-9 _ - (set host_id in the config)"
        );
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
        let days = self.file.retention.local_days;
        anyhow::ensure!(
            (1..=3650).contains(&days),
            "[retention] local_days must be 1-3650, got {days}"
        );
        Ok(DaemonConfig {
            projects,
            local_retention: std::time::Duration::from_secs(u64::from(days) * 86_400),
            nats: NatsSettings {
                servers: nats.servers.clone(),
                auth,
                tls_ca: self.nats_tls_ca(),
                client_name: format!("xagentd@{host_id}"),
            },
            host_id,
            home: self.home.clone(),
            socket: self.socket(),
            state_db: self.state_db(),
            wake: self.wake()?,
        })
    }
}
