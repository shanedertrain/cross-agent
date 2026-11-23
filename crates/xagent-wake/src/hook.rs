//! Wake for any other client: a command or a webhook the host config names per client
//! (`[clients.<name>]`).
//!
//! Both receive the same JSON payload ([`payload`]): the command on stdin, the webhook as the body
//! of a `POST`. Exit status 0 or a 2xx answer counts as delivered. xagent does not ask a hooked
//! client whether it is idle: the receiver queues the wake until its agent can take it.

use std::net::IpAddr;
use std::process::Stdio;
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tracing::warn;
use url::{Host, Url};

use crate::policy::Target;

/// Longest a wake command may run, or a webhook take to answer.
const TIMEOUT: Duration = Duration::from_secs(20);

/// How a hooked client is woken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hook {
    /// Program and arguments; the payload arrives on stdin.
    Command(Vec<String>),
    /// `POST <url>` with the payload, and `Authorization: Bearer <token>` when a token is set.
    Webhook { url: Url, token: Option<String> },
}

impl Hook {
    /// A webhook at `url`: `https`, or plain `http` only to this machine, so a bearer token or
    /// an agent's ids never cross the network in clear text.
    pub fn webhook(url: &str, token: Option<String>) -> Result<Self, String> {
        let url = Url::parse(url).map_err(|e| format!("{url:?} is not a URL: {e}"))?;
        let loopback = match url.host() {
            Some(Host::Domain(name)) => name == "localhost",
            Some(Host::Ipv4(ip)) => IpAddr::V4(ip).is_loopback(),
            Some(Host::Ipv6(ip)) => IpAddr::V6(ip).is_loopback(),
            None => false,
        };
        match url.scheme() {
            "https" => {}
            "http" if loopback => {}
            _ => return Err(format!("{url} must be https (plain http only to loopback)")),
        }
        Ok(Self::Webhook { url, token })
    }

    pub(crate) async fn send(&self, target: &Target, text: &str) -> bool {
        let body = payload(target, text);
        let result = match self {
            Self::Command(argv) => run(argv, &body).await,
            Self::Webhook { url, token } => post(url, token.as_deref(), &body).await,
        };
        match result {
            Ok(()) => true,
            Err(reason) => {
                warn!(agent = %target.agent, client = %target.endpoint.client(), %reason, "wake hook failed");
                false
            }
        }
    }
}

/// The wake payload, `xagent.wake.v1`. `text` is the fixed wake line; no peer text is included.
pub fn payload(target: &Target, text: &str) -> Value {
    json!({
        "type": "xagent.wake.v1",
        "client": target.endpoint.client().as_str(),
        "conversation": target.conversation.as_str(),
        "workspace": target.workspace,
        "agent_id": target.agent,
        "text": text,
    })
}

async fn run(argv: &[String], body: &Value) -> Result<(), String> {
    let (program, rest) = argv.split_first().ok_or("empty wake_command")?;
    let mut child = Command::new(program)
        .args(rest)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("cannot start {program}: {e}"))?;
    let mut stdin = child.stdin.take().expect("piped stdin");
    let mut line = body.to_string().into_bytes();
    line.push(b'\n');
    // A command that ignores its stdin may exit before reading it: not a failure.
    let _ = stdin.write_all(&line).await;
    drop(stdin);
    let status = tokio::time::timeout(TIMEOUT, child.wait())
        .await
        .map_err(|_| format!("{program} ran longer than {}s", TIMEOUT.as_secs()))?
        .map_err(|e| format!("{program}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{program} exited with {status}"))
    }
}

async fn post(url: &Url, token: Option<&str>, body: &Value) -> Result<(), String> {
    let http = crate::http_builder()
        .timeout(TIMEOUT)
        .build()
        .map_err(|e| e.to_string())?;
    let mut request = http.post(url.clone()).json(body);
    if let Some(token) = token {
        request = request.bearer_auth(token);
    }
    let response = request
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!("webhook answered {}", response.status()))
    }
}
