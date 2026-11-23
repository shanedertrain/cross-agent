//! NATS subjects, durable consumer names and registry keys: the only place they are built.

use sha1::{Digest, Sha1};

use crate::{AgentId, Channel, Error, ErrorCode};

const CHAT_PREFIX: &str = "xagent.v1.chat";
const DM_PREFIX: &str = "xagent.v1.dm";

/// Subjects bound to the `XAGENT_MESSAGES` stream.
pub const STREAM_SUBJECTS: [&str; 2] = ["xagent.v1.chat.>", "xagent.v1.dm.>"];

/// Hex digits of the SHA-1 digest that make a consumer name collision-free.
const CONSUMER_DIGEST_HEX: usize = 10;

/// `xagent.v1.chat.<workspace>.<topic>`; `topic` must already be validated.
pub fn chat_subject(workspace: &str, topic: &str) -> String {
    format!("{CHAT_PREFIX}.{workspace}.{topic}")
}

/// `xagent.v1.dm.<workspace>.<agent>`.
pub fn dm_subject(workspace: &str, agent: &AgentId) -> String {
    format!("{DM_PREFIX}.{workspace}.{agent}")
}

/// Splits a received subject into its channel, workspace and the rest (topic or recipient).
pub fn parse_subject(subject: &str) -> Result<(Channel, &str, &str), Error> {
    let outside = || {
        Error::new(
            ErrorCode::InvalidMessage,
            format!("subject outside protocol namespace: {subject}"),
        )
    };
    let mut parts = subject.splitn(5, '.');
    let (Some("xagent"), Some("v1"), Some(channel), Some(workspace), Some(rest)) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return Err(outside());
    };
    let channel = match channel {
        "chat" => Channel::Chat,
        "dm" => Channel::Dm,
        _ => return Err(outside()),
    };
    Ok((channel, workspace, rest))
}

/// Durable consumer name: `xagent_<ws>_<agent>_<sha1(ws\0agent)[:10]>`.
pub fn consumer_name(workspace: &str, agent: &AgentId) -> String {
    let mut hasher = Sha1::new();
    hasher.update(workspace.as_bytes());
    hasher.update([0]);
    hasher.update(agent.as_str().as_bytes());
    let digest = format!("{:x}", hasher.finalize());
    format!(
        "xagent_{workspace}_{agent}_{}",
        &digest[..CONSUMER_DIGEST_HEX]
    )
}

/// Whether a NATS filter subject (`*` matches one token, a trailing `>` one or more) matches a
/// concrete subject.
pub fn subject_matches(filter: &str, subject: &str) -> bool {
    let mut want = filter.split('.');
    let mut have = subject.split('.');
    loop {
        match (want.next(), have.next()) {
            (Some(">"), Some(_)) => return want.next().is_none(),
            (Some("*"), Some(_)) => {}
            (Some(w), Some(h)) if w == h => {}
            (None, None) => return true,
            _ => return false,
        }
    }
}

/// KV key for the topic, agent, lease and subscription registries: `<workspace>.<name>`.
pub fn registry_key(workspace: &str, name: &str) -> String {
    format!("{workspace}.{name}")
}

/// A `host_id`: 1-64 of `A-Z a-z 0-9 _ -` (one KV key token).
pub fn is_valid_host_id(host_id: &str) -> bool {
    (1..=64).contains(&host_id.len())
        && host_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

/// `XAGENT_LEASES` key of a daemon's host lease: `_host.<host_id>`. A workspace token never
/// starts with `_`, so it cannot collide with an agent lease (`<workspace>.<agent_id>`).
pub fn host_lease_key(host_id: &str) -> String {
    format!("_host.{host_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn agent(s: &str) -> AgentId {
        s.parse().expect("valid agent id")
    }

    // Expected values computed with the Python reference `xagent.protocol.validation`.
    #[test]
    fn matches_python_reference() {
        let a = agent("chat-f2e6d54bfe674ba01189bfde");
        assert_eq!(
            consumer_name("conversations", &a),
            "xagent_conversations_chat-f2e6d54bfe674ba01189bfde_9c5b80a169"
        );
        assert_eq!(
            consumer_name("conversations", &agent("chat-0123456789abcdef01234567")),
            "xagent_conversations_chat-0123456789abcdef01234567_e4c2d93365"
        );
        assert_eq!(
            dm_subject("conversations", &a),
            "xagent.v1.dm.conversations.chat-f2e6d54bfe674ba01189bfde"
        );
        assert_eq!(
            chat_subject("conversations", "editor-auto.ci"),
            "xagent.v1.chat.conversations.editor-auto.ci"
        );
    }

    #[test]
    fn filter_matching() {
        let s = "xagent.v1.chat.conversations.fw.usb";
        for (filter, want) in [
            (s, true),
            ("xagent.v1.chat.conversations.fw", false),
            ("xagent.v1.chat.conversations.fw.usb.x", false),
            ("xagent.v1.chat.conversations.fw.*", true),
            ("xagent.v1.chat.conversations.*", false),
            ("xagent.v1.chat.conversations.>", true),
            ("xagent.v1.chat.conversations.fw.usb.>", false),
            ("xagent.v1.*.conversations.fw.usb", true),
            ("xagent.v1.dm.conversations.fw.usb", false),
        ] {
            assert_eq!(subject_matches(filter, s), want, "{filter}");
        }
    }

    #[test]
    fn parses_subjects() {
        assert_eq!(
            parse_subject("xagent.v1.chat.conversations.editor-auto.ci").expect("chat"),
            (Channel::Chat, "conversations", "editor-auto.ci")
        );
        assert_eq!(
            parse_subject("xagent.v1.dm.conversations.chat-abc").expect("dm"),
            (Channel::Dm, "conversations", "chat-abc")
        );
        for bad in [
            "xagent.v1.task.conversations.x",
            "xagent.v2.dm.conversations.x",
            "xagent.v1.dm.conversations",
            "other.v1.dm.conversations.x",
        ] {
            assert!(parse_subject(bad).is_err(), "{bad}");
        }
    }
}
