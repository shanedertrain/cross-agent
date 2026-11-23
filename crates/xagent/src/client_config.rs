//! `xagent client-config`: the MCP entry and hooks stanza to put in a client's configuration.

use serde_json::{Value, json};
use xagent_proto::Client;

/// `~/.claude/settings.json` hooks: the end-of-turn reminder and the `SendMessage` mirror.
fn claude_hooks(exe: &str) -> Value {
    json!({ "hooks": {
        "Stop": [{ "hooks": [{ "type": "command", "command": format!("{exe} hook claude-stop"), "timeout": 10 }] }],
        "PostToolUse": [{ "matcher": "SendMessage",
            "hooks": [{ "type": "command", "command": format!("{exe} hook claude-sendmessage"), "timeout": 10 }] }],
    }})
}

/// `~/.codex/hooks.json`: the end-of-turn reminder.
fn codex_hooks(exe: &str) -> Value {
    json!({ "hooks": { "Stop": [{ "hooks": [
        { "type": "command", "command": format!("{exe} hook codex-stop"), "timeout": 10 }
    ] }] } })
}

fn pretty(value: &Value) -> String {
    serde_json::to_string_pretty(value).expect("JSON values serialize")
}

/// The text to print for `client`; `exe` is the absolute path of this binary.
pub fn render(client: &Client, exe: &str) -> String {
    match client {
        Client::ClaudeCode => format!(
            "# .mcp.json (project), or: claude mcp add -s user xagent -- {exe} mcp --client claude-code\n{}\n\n\
             # Idle sessions are woken through Claude's own cross-session inbox; no launch flag is needed.\n\
             # ~/.claude/settings.json: end-of-turn inbox reminder, and SendMessage mirrored to the bus\n{}\n\n\
             # ~/.claude/settings.json, optional: deliver the wake line without holding it for the user.\n\
             # A bypass-mode session holds messages from senders that attest no permission mode. \"accept\"\n\
             # applies to EVERY cross-session message, not only xagent's: see docs/installation.md first.\n{}",
            pretty(&json!({ "mcpServers": { "xagent": {
                "type": "stdio", "command": exe, "args": ["mcp", "--client", "claude-code"] } } })),
            pretty(&claude_hooks(exe)),
            pretty(&json!({ "crossSessionInbound": "accept" })),
        ),
        Client::Codex => format!(
            "# ~/.codex/config.toml (or a trusted project's .codex/config.toml)\n[mcp_servers.xagent]\n\
             command = \"{exe}\"\nargs = [\"mcp\", \"--client\", \"codex\"]\nstartup_timeout_sec = 20\ntool_timeout_sec = 30\n\n\
             # ~/.codex/hooks.json: end-of-turn inbox reminder. Codex asks once, in the TUI, to trust a new\n\
             # or changed hook (\"Hooks need review\"); an untrusted hook does not run.\n{}",
            pretty(&codex_hooks(exe)),
        ),
        Client::Dsh => format!(
            "# dsh plugin `mcp-xagent` in ~/.dsh/profiles/web/cordis.patch.yml\n\
             command: {exe}\nargs: ['mcp', '--client', 'dsh']\nperAgent: true\n\
             sessionIdEnv: XAGENT_CONVERSATION_ID     # the dsh session id is the conversation\n\n\
             # ~/.dsh/xagent-hooks.json (optional end-of-turn reminder; dsh runs Claude-format hooks)\n{}",
            pretty(&json!({ "hooks": { "Stop": [{ "hooks": [
                { "type": "command", "command": format!("{exe} hook claude-stop"), "timeout": 10 }
            ] }] } })),
        ),
        Client::Other(name) => format!(
            "# Launch the MCP server once per conversation, over stdio, with the conversation named:\n\
             command: {exe}\nargs: ['mcp', '--client', '{name}']\nenv:\n\
             \x20 XAGENT_CONVERSATION_ID: <conversation id>   # required: the identity\n\
             \x20 XAGENT_SESSION_TITLE: <display name>        # optional\n\n\
             # Wake an idle conversation when messages arrive: in the xagent host config, one of\n\
             [clients.{name}]\n\
             wake_command = [\"/path/to/notify-{name}\"]           # the wake JSON on stdin\n\
             # wake_url = \"https://example.invalid/xagent-wake\"   # the wake JSON as a POST body\n\
             # wake_token_file = \"~/.config/xagent/{name}.token\"  # bearer token for wake_url\n\n\
             # Without MCP: xagent send|dm|inbox|read --conversation ID --client {name}\n\
             # The contract: docs/integrating.md"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hooks_stanzas_name_the_hook_subcommands() {
        let claude = claude_hooks("/bin/xagent").to_string();
        assert!(claude.contains("/bin/xagent hook claude-stop"));
        assert!(claude.contains("\"matcher\":\"SendMessage\""));
        assert!(claude.contains("/bin/xagent hook claude-sendmessage"));
        assert!(
            codex_hooks("/bin/xagent")
                .to_string()
                .contains("hook codex-stop")
        );
    }
}
