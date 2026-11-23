# Changelog

## 2.0.0

xagent is rewritten in Rust: one `xagent` binary (daemon, MCP relay, hooks, CLI), a cargo workspace of seven crates, no
Python runtime.

- The daemon serves MCP itself on a Unix socket; clients launch `xagent mcp`, a stdio relay. No separate MCP service.
- Identity is the native conversation: `chat-` + 24 hex of sha256(client, conversation), in the `conversations`
  workspace. A session joins its project's root topic automatically.
- Wake transports: Claude Code's native cross-session inbox, the Codex app-server, dsh's RPC. Stop hooks for Claude
  Code and Codex; `SendMessage` traffic mirrored to the `sendmessage` topic.
- `xagent doctor`, `gc`, `recover`; `xagent client-config` prints the client setup.
- Any agent or system joins under its own client name: `xagent mcp --client my-agent` (conversation from
  `XAGENT_CONVERSATION_ID`) or the CLI's `--client my-agent`. `[clients.<name>]` wakes it with a command or an `https`
  webhook (`xagent.wake.v1` payload). [docs/integrating.md](docs/integrating.md) has the contract.
- `xagent mcp` survives a daemon restart: it reconnects and replays the client's handshake.
- Retention is the broker's, shared by every host: `xagent retention --days N --max-mb N` (new streams: 7 days,
  1024 MB). Each host can keep local copies shorter with `[retention] local_days`.
- `host_id` is unique: each daemon holds a host lease, and a second daemon with the same id waits, not ready.
- NATS auth by user and password file, or by a `.creds` file (`[nats] credentials_file`); TLS with a private CA
  (`scripts/setup-nats.sh --tls`).
- Paths follow the XDG base directories. Claude and Codex are woken in their own profile directory
  (`CLAUDE_CONFIG_DIR`, `CODEX_HOME`).
- NATS wire format (subjects, `xagent.message.v1` envelope, stream, KV buckets, consumer names) is unchanged.

Dropped: role and numbered-instance identities, multiple workspaces and `ws/name` addressing, topic wildcards and
subscription replay, the Claude development-channel wake, tmux wake, the shared MCP service, the legacy MCP transport,
the Python package and its pilot scripts.
