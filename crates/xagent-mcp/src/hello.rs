//! The first line of an MCP connection: the launch context the relay (`xagent mcp`) passes, and
//! the daemon's answer. After the answer the connection carries MCP (newline-delimited JSON-RPC).
//!
//! ```text
//! relay   {"client":"claude-code","cwd":"/home/u/p","env":{"CLAUDE_PID":"4242",...}}
//! daemon  {"ok":true}            or  {"ok":false,"error":{"code":"...","message":"..."}}
//! ```

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use xagent_proto::{Client, Error};

use xagent_wake::sessions;

/// Longest accepted hello line, in bytes.
pub const MAX_HELLO_BYTES: usize = 64 * 1024;

/// The environment variables the relay forwards: only what names the conversation and where the
/// client keeps its state. Never credentials.
pub const CONTEXT_ENV: [&str; 7] = [
    "XAGENT_CONVERSATION_ID",
    "XAGENT_SESSION_TITLE",
    "CLAUDE_CODE_SESSION_ID",
    "CLAUDE_CODE_MESSAGING_SOCKET",
    "CLAUDE_PID",
    "CLAUDE_CONFIG_DIR",
    "CODEX_HOME",
];

/// The relay's launch context.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hello {
    pub client: Client,
    /// The client's working directory (the project the conversation runs in).
    pub cwd: PathBuf,
    /// The [`CONTEXT_ENV`] variables that are set.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}

impl Hello {
    /// The context of the current process.
    pub fn from_process(client: Client) -> std::io::Result<Self> {
        let env = CONTEXT_ENV
            .iter()
            .filter_map(|&key| Some((key.to_owned(), std::env::var(key).ok()?)))
            .collect();
        Ok(Self {
            client,
            cwd: std::env::current_dir()?,
            env,
        })
    }

    fn var(&self, key: &str) -> Option<&str> {
        self.env
            .get(key)
            .map(|v| v.trim())
            .filter(|v| !v.is_empty())
    }
}

/// The daemon's answer to a hello.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Ack {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Error>,
}

/// Where a connection's native conversation id comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Launch {
    /// The Claude session registry, read by the Claude process's pid; the session id from the
    /// environment when no live record is found.
    ClaudeCode {
        sessions_dir: PathBuf,
        pid: Option<u32>,
        session_id: Option<String>,
    },
    /// `_meta.threadId` of each tool call; names from the Codex session index.
    Codex { codex_home: PathBuf },
    /// dsh starts one server per session and names it in the environment.
    Dsh {
        conversation: Option<String>,
        title: Option<String>,
    },
}

impl Launch {
    /// Resolves a hello; `home` is the daemon user's home, for the clients' default state dirs.
    pub(crate) fn new(hello: &Hello, home: &Path) -> Self {
        match hello.client {
            Client::ClaudeCode => {
                let config_dir = hello
                    .var("CLAUDE_CONFIG_DIR")
                    .map_or_else(|| home.join(".claude"), PathBuf::from);
                Self::ClaudeCode {
                    sessions_dir: config_dir.join("sessions"),
                    pid: sessions::pid_from_env(
                        hello.var("CLAUDE_CODE_MESSAGING_SOCKET"),
                        hello.var("CLAUDE_PID"),
                    ),
                    session_id: hello.var("CLAUDE_CODE_SESSION_ID").map(str::to_owned),
                }
            }
            Client::Codex => Self::Codex {
                codex_home: hello
                    .var("CODEX_HOME")
                    .map_or_else(|| home.join(".codex"), PathBuf::from),
            },
            Client::Dsh => Self::Dsh {
                conversation: hello.var("XAGENT_CONVERSATION_ID").map(str::to_owned),
                title: hello.var("XAGENT_SESSION_TITLE").map(str::to_owned),
            },
        }
    }
}
