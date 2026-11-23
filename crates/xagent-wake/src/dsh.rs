//! dsh wake (the dsh agent harness).
//!
//! dsh's own RPC starts a turn: `POST <dsh_url>/api/session/prompt` with `mode: "queue"` runs on
//! an idle session at once and after the current turn on a busy one; `session/list` reports
//! `running` per session. Every RPC needs dsh's browser-session cookie. The launch token rotates
//! per dsh launch and is printed only to dsh's web log: each wake reads the newest token,
//! exchanges it for the cookie with `GET /?token=...` in a fresh agent, and drops both. The URL
//! must be loopback `http`, so the token only ever goes back to the dsh that printed it.

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Value, json};
use url::{Host, Url};
use xagent_proto::Client;

use crate::error::WakeError;
use crate::policy::{Readiness, Target};

const BUSY_RETRY: Duration = Duration::from_secs(10);
const TIMEOUT: Duration = Duration::from_secs(20);

/// Whether `url` is a plain loopback `http` URL, the only kind the dsh login token is sent to.
pub fn is_loopback_url(url: &str) -> bool {
    loopback_url(url).is_some()
}

/// `^[A-Za-z0-9][A-Za-z0-9_-]{7,127}$`
fn is_session_id(id: &str) -> bool {
    (8..=128).contains(&id.len())
        && id.as_bytes()[0].is_ascii_alphanumeric()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// A plain `http` URL to this machine: no path, query or credentials.
fn loopback_url(url: &str) -> Option<Url> {
    let url = Url::parse(url).ok()?;
    let host_ok = match url.host()? {
        Host::Domain(name) => name == "localhost",
        Host::Ipv4(ip) => IpAddr::V4(ip).is_loopback(),
        Host::Ipv6(ip) => IpAddr::V6(ip).is_loopback(),
    };
    (url.scheme() == "http"
        && host_ok
        && url.path() == "/"
        && url.query().is_none()
        && url.username().is_empty())
    .then_some(url)
}

/// The newest launch token dsh printed for `port`.
fn latest_token(log: &str, port: u16) -> Option<&str> {
    ["127.0.0.1", "localhost"]
        .iter()
        .flat_map(|host| {
            let prefix = format!("http://{host}:{port}/?token=");
            log.match_indices(&prefix)
                .map(|(at, _)| {
                    let start = at + prefix.len();
                    let end = log[start..]
                        .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '&'))
                        .map_or(log.len(), |n| start + n);
                    (start, &log[start..end])
                })
                .collect::<Vec<_>>()
        })
        .max_by_key(|(start, _)| *start)
        .map(|(_, token)| token)
        .filter(|token| !token.is_empty())
}

/// One authenticated RPC conversation with dsh: a fresh client whose cookie jar holds the
/// browser-session cookie the login sets.
struct Rpc {
    base: String,
    http: reqwest::Client,
}

impl Rpc {
    async fn login(url: &Url, web_log: &Path) -> Result<Self, WakeError> {
        let log = std::fs::read_to_string(web_log)?;
        let port = url.port_or_known_default().unwrap_or(80);
        let token = latest_token(&log, port)
            .ok_or_else(|| WakeError::Protocol("no dsh launch token in the launch log".into()))?;
        let http = reqwest::Client::builder()
            .cookie_store(true)
            .no_proxy()
            .timeout(TIMEOUT)
            .build()?;
        let base = url.as_str().trim_end_matches('/').to_owned();
        let mut login =
            Url::parse(&format!("{base}/")).map_err(|e| WakeError::Protocol(e.to_string()))?;
        login.query_pairs_mut().append_pair("token", token);
        http.get(login).send().await?.error_for_status()?;
        Ok(Self { base, http })
    }

    async fn call(&self, method: &str, args: &Value) -> Result<Value, WakeError> {
        let body = json!({
            "type": "client-request",
            "rpcId": format!("xagent-{}", &uuid::Uuid::new_v4().simple().to_string()[..12]),
            "method": method,
            "payload": { "args": args },
        });
        let out: Value = self
            .http
            .post(format!("{}/api/{method}", self.base))
            .json(&body)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let result = out.get("result").cloned().unwrap_or(Value::Null);
        if result.get("ok") != Some(&Value::Bool(true)) {
            let code = result
                .pointer("/error/code")
                .and_then(Value::as_str)
                .unwrap_or("error");
            return Err(WakeError::Protocol(format!("{method}: {code}")));
        }
        Ok(result.get("value").cloned().unwrap_or(Value::Null))
    }

    /// Whether the session's agent is mid-turn; `None` when dsh does not list it.
    async fn running(&self, session: &str) -> Result<Option<bool>, WakeError> {
        let list = self
            .call("session/list", &json!({ "_request": {} }))
            .await?;
        Ok(list
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .find(|item| item.get("sessionId").and_then(Value::as_str) == Some(session))
            .map(|item| {
                item.get("running")
                    .and_then(Value::as_bool)
                    .unwrap_or(false)
            }))
    }

    async fn prompt(&self, session: &str, text: &str) -> Result<bool, WakeError> {
        let value = self
            .call(
                "session/prompt",
                &json!({ "request": {
                    "requestId": format!("xagent-wake-{}", uuid::Uuid::new_v4().simple()),
                    "sessionId": session,
                    "content": [{ "type": "text", "text": text }],
                    "mode": "queue",
                }}),
            )
            .await?;
        Ok(value
            .get("accepted")
            .and_then(Value::as_bool)
            .unwrap_or(false))
    }
}

/// Queues the wake line as a prompt in the target's dsh session.
#[derive(Debug, Clone)]
pub struct Dsh {
    url: Option<Url>,
    web_log: PathBuf,
}

impl Dsh {
    /// A non-loopback `url` disables the transport: the login token must stay on this machine.
    pub fn new(url: &str, web_log: PathBuf) -> Self {
        Self {
            url: loopback_url(url),
            web_log,
        }
    }

    /// For dsh the conversation id is the dsh session id.
    pub(crate) fn reaches(&self, target: &Target) -> bool {
        target.client == Client::Dsh
            && is_session_id(target.conversation.as_str())
            && self.url.is_some()
            && self.web_log.exists()
    }

    async fn login(&self) -> Result<Rpc, WakeError> {
        let url = self
            .url
            .as_ref()
            .ok_or(WakeError::Protocol("dsh_url is not loopback http".into()))?;
        Rpc::login(url, &self.web_log).await
    }

    pub(crate) async fn ready(&self, target: &Target) -> Readiness {
        let session = target.conversation.as_str();
        let running = async { self.login().await?.running(session).await };
        match running.await {
            Ok(Some(false)) => Readiness::Ready,
            Ok(Some(true)) => Readiness::Wait(BUSY_RETRY, "session busy"),
            Ok(None) => Readiness::Gone("dsh does not list the session".to_owned()),
            Err(err) => Readiness::Gone(format!("dsh: {err}")),
        }
    }

    pub(crate) async fn send(&self, target: &Target, text: &str) -> bool {
        let session = target.conversation.as_str();
        let prompt = async { self.login().await?.prompt(session, text).await };
        match prompt.await {
            Ok(accepted) => accepted,
            Err(err) => {
                tracing::warn!(error = %err, "dsh wake failed");
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_plain_loopback_http_urls_are_accepted() {
        for (url, ok) in [
            ("http://127.0.0.1:3080", true),
            ("http://localhost:3080/", true),
            ("http://[::1]:3080", true),
            ("https://127.0.0.1:3080", false),
            ("http://192.0.2.10:3080", false),
            ("http://127.0.0.1:3080/x", false),
            ("http://127.0.0.1:3080/?a=1", false),
            ("http://u@127.0.0.1:3080", false),
            ("http://127.0.0.1.evil.example:3080", false),
        ] {
            assert_eq!(loopback_url(url).is_some(), ok, "{url}");
        }
    }

    #[test]
    fn the_newest_token_for_the_port_wins() {
        let log = "open http://127.0.0.1:3080/?token=old1\nother http://127.0.0.1:9999/?token=zzz\n\
                   open http://localhost:3080/?token=new2&x=1 done";
        assert_eq!(latest_token(log, 3080), Some("new2"));
        assert_eq!(latest_token(log, 1234), None);
    }
}
