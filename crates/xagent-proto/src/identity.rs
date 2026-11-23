//! Conversation identity: an agent id derived from the client and its native conversation id.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{AgentId, Error, ErrorCode};

/// Longest accepted native conversation id, in characters.
const CONVERSATION_MAX_CHARS: usize = 128;

/// Number of hex digits of the SHA-256 digest kept in a conversation agent id.
const AGENT_DIGEST_HEX: usize = 24;

/// Longest accepted client name, in characters.
const CLIENT_MAX_CHARS: usize = 32;

/// The agent client a conversation belongs to: one of the built-in clients, or any other agent
/// that names itself (`my-agent`) and reaches xagent through the generic launch contract.
///
/// `claude` is accepted as an alias of `claude-code`; both derive the same agent id.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum Client {
    ClaudeCode,
    Codex,
    Dsh,
    /// Any other client: a lowercase name, `[a-z][a-z0-9-]*`, at most 32 characters.
    Other(String),
}

impl Client {
    /// The canonical client string, used in identity hashing and on the wire.
    pub fn as_str(&self) -> &str {
        match self {
            Self::ClaudeCode => "claude-code",
            Self::Codex => "codex",
            Self::Dsh => "dsh",
            Self::Other(name) => name,
        }
    }
}

fn is_client_name(name: &str) -> bool {
    name.len() <= CLIENT_MAX_CHARS
        && name.starts_with(|c: char| c.is_ascii_lowercase())
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

impl FromStr for Client {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        match s {
            "claude-code" | "claude" => Ok(Self::ClaudeCode),
            "codex" => Ok(Self::Codex),
            "dsh" => Ok(Self::Dsh),
            other if is_client_name(other) => Ok(Self::Other(other.to_owned())),
            other => Err(Error::new(
                ErrorCode::InvalidArgument,
                format!(
                    "invalid client {other:?}: claude-code, codex, dsh, or a lowercase name \
                     ([a-z][a-z0-9-]*, at most {CLIENT_MAX_CHARS} characters)"
                ),
            )),
        }
    }
}

impl TryFrom<String> for Client {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        value.parse()
    }
}

impl From<Client> for String {
    fn from(client: Client) -> Self {
        client.as_str().to_owned()
    }
}

impl fmt::Display for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A client's native conversation id (Claude Code session id, Codex thread id, dsh session).
///
/// 1-128 printable characters, not only whitespace.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Conversation(String);

impl Conversation {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Printable: no control characters and no whitespace other than the ASCII space.
fn is_printable(c: char) -> bool {
    !c.is_control() && (c == ' ' || !c.is_whitespace())
}

impl TryFrom<String> for Conversation {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        let chars = value.chars().count();
        if chars == 0
            || chars > CONVERSATION_MAX_CHARS
            || value.trim().is_empty()
            || !value.chars().all(is_printable)
        {
            return Err(Error::new(
                ErrorCode::InvalidArgument,
                "conversation must be a short printable id",
            ));
        }
        Ok(Self(value))
    }
}

impl FromStr for Conversation {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Self::try_from(s.to_owned())
    }
}

impl From<Conversation> for String {
    fn from(c: Conversation) -> Self {
        c.0
    }
}

impl fmt::Display for Conversation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The stable agent id of a conversation: `chat-` + hex(sha256(client + "\0" + conversation))[:24].
pub fn conversation_agent_id(client: &Client, conversation: &Conversation) -> AgentId {
    let mut hasher = Sha256::new();
    hasher.update(client.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(conversation.as_str().as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!("chat-{}", &digest[..AGENT_DIGEST_HEX])
        .parse()
        .expect("chat- plus lowercase hex satisfies the id rule")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(client: &str, conversation: &str) -> String {
        let client: Client = client.parse().expect("known client");
        let conversation: Conversation = conversation.parse().expect("valid conversation");
        conversation_agent_id(&client, &conversation).to_string()
    }

    // Expected values computed with the Python reference `xagent.identity.conversation_agent_id`.
    #[test]
    fn matches_python_reference() {
        let cases = [
            (
                "claude-code",
                "5f1c3e2a-8d47-4b8e-9a51-2c7e0b9d6f13",
                "chat-f2e6d54bfe674ba01189bfde",
            ),
            (
                "codex",
                "019a7b3c-1d2e-7f40-8a9b-0c1d2e3f4a5b",
                "chat-60b55906d0d938f1f2d0089d",
            ),
            ("dsh", "session 42", "chat-e58f4e7091e6e29514e79993"),
            ("claude-code", "café-ü", "chat-0d5f767e05429d65379b2e7e"),
        ];
        for (client, conversation, want) in cases {
            assert_eq!(id(client, conversation), want, "{client} {conversation:?}");
        }
    }

    #[test]
    fn claude_alias_hashes_as_claude_code() {
        // Python hashed "claude" separately (chat-993009a7bd4e07f899cd8b6a): two ids for one
        // conversation. The port canonicalises before hashing.
        let conversation = "5f1c3e2a-8d47-4b8e-9a51-2c7e0b9d6f13";
        assert_eq!(id("claude", conversation), "chat-f2e6d54bfe674ba01189bfde");
        assert_eq!(
            "claude".parse::<Client>().expect("alias").as_str(),
            "claude-code"
        );
        assert_eq!(
            "my-agent".parse::<Client>().expect("generic"),
            Client::Other("my-agent".to_owned())
        );
        for bad in ["My-Agent", "-x", "a b", "", &"x".repeat(33)] {
            assert!(bad.parse::<Client>().is_err(), "{bad:?}");
        }
    }

    #[test]
    fn conversation_rule() {
        let max = "x".repeat(128);
        let over = "x".repeat(129);
        let cases = [
            ("abc", true),
            ("session 42", true),
            (max.as_str(), true),
            (over.as_str(), false),
            ("", false),
            ("   ", false),
            ("a\tb", false),
            ("a\nb", false),
            ("a\u{2028}b", false),
            ("café", true),
        ];
        for (input, want) in cases {
            assert_eq!(
                input.parse::<Conversation>().is_ok(),
                want,
                "input {input:?}"
            );
        }
    }
}
