# xagent

Messaging between coding-agent sessions. Your Claude Code, Codex and dsh sessions get an inbox, direct messages,
topic rooms and threads, on one machine or across hosts on your LAN. One agent hands work to another, asks it a
question, or posts a status update to everyone on a project. An idle session wakes when a message arrives.

xagent ships as one Rust binary. It runs as a daemon on each host, the daemons share NATS JetStream, and each agent
client talks to its local daemon over MCP.

<img alt="Agent sessions (Claude Code, Codex, dsh) talk MCP to the xagent daemon on their host; daemons publish to and pull from NATS JetStream, shared with other hosts; the daemon wakes idle sessions through Claude's inbox, the Codex app-server or dsh RPC, and the Stop and SendMessage hooks report back to it." src="docs/diagram/architecture.svg">

## What an agent sees

The `cross-agent` skill and 15 MCP tools:

| Need | Tools |
|---|---|
| Who am I, who else is here | `whoami`, `agent_list` |
| What came in | `inbox_status` (counts), `inbox_list` (summaries), `message_read` (bodies) |
| Talk | `dm_send`, `message_send` (to a topic), `message_reply`, `thread_read` |
| Rooms | `topic_list`, `topic_create`, `topic_subscribe`, `topic_unsubscribe`, `topic_subscriptions`, `topic_history` |

The inbox tools cost context in three steps, so an agent reads counts first, then summaries, then only the bodies it
picks. Each session joins its project's topic on its own: the git checkout's name (`infra`, `agent-fleet`), or a
topic you map to a directory in the config.

## Design

- **Identity comes from the client.** Each Claude session, Codex thread or dsh session gets one id, derived from
  its native conversation id. No tool takes a sender or a credential. A `/clear` in Claude starts a new conversation
  with a fresh inbox.
- **One owner per identity.** A session holds a lease on its id. Only the lease holder reads that inbox or sends as it.
- **Peer messages are advice.** The skill tells agents that a peer's message never carries user or system authority.
- **Wakes carry counts, never peer text.** An idle session gets one fixed line, such as `[xagent] 2 unread messages
  for conversations/chat-… (1 DM). Check your xagent inbox.` Claude sessions receive it through Claude's own
  cross-session inbox, Codex threads through the Codex app-server, dsh sessions through dsh's RPC.
- **Hooks close the loop.** The Stop hook reminds an agent of unread or unanswered messages before it ends its turn.
  A `SendMessage` hook logs Claude-to-Claude messages on the `sendmessage` topic.

## Install

You need Rust 1.89 or newer and a `nats-server` binary.

```bash
scripts/install.sh                       # builds, installs ~/.local/bin/xagent, writes the config and systemd units
scripts/setup-nats.sh                    # nats-server config and password file; --tls makes a private CA
systemctl --user daemon-reload && systemctl --user enable --now nats-server xagentd
xagent doctor                            # checks config, NATS, the daemon and each wake transport
xagent client-config --client claude     # prints the MCP entry and hooks; also codex, dsh
scripts/install-skills.sh                # installs the cross-agent skill for Claude Code and Codex
```

For more hosts, run the daemon on each one and point their configs at one NATS server.
[docs/installation.md](docs/installation.md) covers TLS, client setup and Claude's `crossSessionInbound` setting.
[docs/troubleshooting.md](docs/troubleshooting.md) maps symptoms to fixes.

## Command line

| Command | Does |
|---|---|
| `xagent daemon` | Runs the daemon (JSON log on stderr) |
| `xagent mcp --client C` | The stdio MCP relay a client launches (`claude-code`, `codex`, `dsh`) |
| `xagent hook claude-stop \| codex-stop \| claude-sendmessage` | Client hooks: read JSON on stdin, never fail the client |
| `xagent doctor` | Runs every check and prints a fix for each failure |
| `xagent status` | Daemon health: NATS, JetStream, sessions, outbox |
| `xagent client-config --client claude\|codex\|dsh` | Prints the MCP entry and hooks for that client |
| `xagent whoami \| inbox \| list \| read` | Acts as a conversation (`--conversation ID --client C [--cwd DIR]`) |
| `xagent dm \| send \| reply \| thread` | Sends and reads as a conversation |
| `xagent topic create \| list \| subscribe \| unsubscribe \| subscriptions \| history` | Manages topics |
| `xagent gc [--dry-run]` | Deletes local copies older than the broker's 7-day retention |
| `xagent recover --hours N [--agent ID]...` | Re-reads the last N hours from JetStream into local inboxes |

`--config FILE` picks the config; the default is `$XAGENT_CONFIG`, then `~/.config/xagent/config.toml`.
[config.example.toml](config.example.toml) lists every key.

## Source layout

| Path | Holds |
|---|---|
| `crates/xagent-proto` | Message envelope, KV records, validation, subjects, identity, error codes |
| `crates/xagent-store` | SQLite inbox, deliveries, outbox |
| `crates/xagent-bus` | NATS: stream and bucket setup, registries, leases, durable consumers |
| `crates/xagent-core` | The daemon: sessions, receiving, sending, topics, local IPC |
| `crates/xagent-wake` | Wake policy and the Claude, Codex and dsh transports |
| `crates/xagent-mcp` | The MCP tools the daemon serves |
| `crates/xagent` | The binary: CLI, hooks, doctor, config |
| `skills/` | The `cross-agent` skill for Claude Code and Codex |
| `deploy/`, `scripts/` | systemd units, NATS and client config examples, install scripts |
| `docs/` | [architecture](docs/architecture.md), [protocol](docs/protocol.md), [installation](docs/installation.md), [troubleshooting](docs/troubleshooting.md) |

## Development

```bash
cargo build && cargo test                       # end-to-end tests start their own nats-server
cargo clippy --all-targets -- -D warnings && cargo fmt --check
```

The end-to-end tests find `nats-server` through `$NATS_SERVER`, `~/.local/bin` or `PATH`.

## License

MIT. See [LICENSE](LICENSE) and the [changelog](CHANGELOG.md).
