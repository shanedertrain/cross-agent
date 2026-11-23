//! Identifier and topic validation.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::{Error, ErrorCode};

/// Maximum number of dot-separated tokens in a topic.
const MAX_TOPIC_TOKENS: usize = 16;

/// Whether `value` matches the id rule `^[a-z0-9_-]{3,64}$` (agent ids and workspaces).
pub fn is_valid_id(value: &str) -> bool {
    (3..=64).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

/// One topic token: `^[a-z0-9][a-z0-9_-]{0,63}$`.
fn is_valid_token(token: &str) -> bool {
    let bytes = token.as_bytes();
    let Some((&first, rest)) = bytes.split_first() else {
        return false;
    };
    bytes.len() <= 64
        && (first.is_ascii_lowercase() || first.is_ascii_digit())
        && rest
            .iter()
            .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
}

/// Validates an exact (non-wildcard) topic path such as `firmware.usb`.
pub fn validate_topic(name: &str) -> Result<(), Error> {
    if name.is_empty() {
        return Err(Error::new(
            ErrorCode::InvalidTopic,
            "topic must be a non-empty string",
        ));
    }
    if name.split('.').count() > MAX_TOPIC_TOKENS {
        return Err(Error::new(
            ErrorCode::InvalidTopic,
            format!("topic has more than {MAX_TOPIC_TOKENS} tokens"),
        ));
    }
    if !name.split('.').all(is_valid_token) {
        return Err(Error::new(
            ErrorCode::InvalidTopic,
            format!(
                "topic {name:?} invalid: dot-separated tokens of a-z 0-9 _ - (token starts a-z0-9)"
            ),
        ));
    }
    Ok(())
}

/// A validated agent id (`^[a-z0-9_-]{3,64}$`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct AgentId(String);

impl AgentId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for AgentId {
    type Error = Error;

    fn try_from(value: String) -> Result<Self, Error> {
        if is_valid_id(&value) {
            Ok(Self(value))
        } else {
            Err(Error::new(
                ErrorCode::InvalidAgentId,
                format!("agent id {value:?} invalid: use 3-64 chars of a-z 0-9 _ -"),
            ))
        }
    }
}

impl FromStr for AgentId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Error> {
        Self::try_from(s.to_owned())
    }
}

impl From<AgentId> for String {
    fn from(id: AgentId) -> Self {
        id.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn id_rule_matches_python() {
        let cases = [
            ("abc", true),
            ("ab", false),
            (&"a".repeat(64), true),
            (&"a".repeat(65), false),
            ("Abc", false),
            ("a.b", false),
            ("a_b-c", true),
            ("chat-xyz", true),
            ("ä12", false),
        ];
        for (input, want) in cases {
            assert_eq!(is_valid_id(input), want, "input {input:?}");
        }
    }

    #[test]
    fn topic_rule_matches_python() {
        let long_token = format!("a.{}", "b".repeat(63));
        let too_long_token = format!("a{}", "b".repeat(64));
        let sixteen = vec!["a"; 16].join(".");
        let seventeen = vec!["a"; 17].join(".");
        let cases = [
            ("firmware.usb", true),
            ("-a", false),
            ("a..b", false),
            (long_token.as_str(), true),
            (sixteen.as_str(), true),
            (seventeen.as_str(), false),
            (too_long_token.as_str(), false),
            ("A", false),
            ("", false),
        ];
        for (input, want) in cases {
            assert_eq!(validate_topic(input).is_ok(), want, "input {input:?}");
        }
    }
}
