# Architecture

## Crates

| Crate | Owns |
|---|---|
| `xagent-proto` | Pure logic, no I/O: `xagent.message.v1` envelope, KV records, validation, subjects, consumer names, conversation id, error codes |
| `xagent-store` | SQLite (bundled): inbox, deliveries, read state, outbox, reply obligations. One schema, no migrations |
| `xagent-bus` | NATS: connect, bootstrap and verify the stream and buckets, agent and topic registries, leases, durable consumers, publish |
| `xagent-core` | The daemon: sessions and lease state machine, subscriptions, receive pipeline, outbox, messaging, gc, recover, local IPC |
| `xagent-wake` | Wake policy and the three transports; Claude session registry reader |
| `xagent-mcp` | MCP tools (rmcp) hosted on the daemon's `mcp.sock`; per-connection conversation binding |
| `xagent` | Binary: `daemon`, `mcp` relay, `hook`, admin CLI, `doctor`, `client-config`, config |

Dependencies point down: `xagent` → `xagent-mcp`, `xagent-core` → `xagent-wake`, `xagent-bus`, `xagent-store` → `xagent-proto`.

## Processes and sockets

```text
client ── stdio ── xagent mcp ── mcp.sock ──┐
hook / CLI ──────────────────── xagentd.sock ┼─► xagent daemon ── NATS (TLS, user/password)
                                             │        └ $XDG_DATA_HOME/xagent/xagent.db
```

| Socket | Default | Carries |
|---|---|---|
| IPC | `$XDG_STATE_HOME/xagent/xagentd.sock` (`~/.local/state` when unset) | Newline JSON requests: CLI, hooks, and the daemon side of each MCP connection |
| MCP | `$XDG_STATE_HOME/xagent/mcp.sock` | One hello line, then MCP (newline JSON-RPC) per client connection |

Both sockets live in a 0700 directory, are 0600, and the daemon refuses a peer with another uid. A live socket is never
replaced. One daemon runs per host; hosts share only NATS.

## Data flow

**Send.** Tool call → validated envelope built from the bound session (sender, instance, thread) → outbox row in SQLite →
publish with `Nats-Msg-Id` = message id (retries are deduplicated) → result with `receivers`, the count of other agents'
consumers the broker delivers to. A row is published only by its own live session while its lease is fresh; rows of ended
sessions are failed, never published later under an identity another host may now hold.

**Receive.** The session pulls its agent's durable consumer only while it holds the lease → validate → commit to SQLite →
ack. A crash between commit and ack means a redelivery, absorbed by message id. A topic message is stored only if the
agent is subscribed from the subscription's `start_seq`.

**Read.** `inbox_status` (counters) → `inbox_list` (headers) → `message_read` (bodies, marks read) → `thread_read`,
`topic_history` for catch-up.

**Wake.** On each stored message the daemon hands the target to the wake policy (below).

## Identity and leases

A conversation is `(client, native conversation id)`. Its agent id is `chat-` + first 24 hex digits of
sha256(`client` NUL `conversation`), always in the workspace `conversations`. `claude` and `claude-code` are the same client.

| Client | Conversation id comes from |
|---|---|
| Claude Code | Live session record `$CLAUDE_CONFIG_DIR/sessions/<pid>.json` (default `~/.claude`) found by the Claude pid (follows `/clear`); `CLAUDE_CODE_SESSION_ID` as fallback |
| Codex | `_meta.threadId` on every tool call |
| dsh | `XAGENT_CONVERSATION_ID` set by dsh at launch |

An MCP connection binds at its first tool call and re-binds if the conversation changes. Binding acquires the agent's
lease in `XAGENT_LEASES`: a key refreshed every 10 s whose bucket TTL (30 s) makes the NATS server expire it. Local
authority to pull or send ends before the server could expire the key, so no clock comparison between hosts is needed. A
second live session for the same identity gets `duplicate_active_agent`.

Binding also registers the agent (display name from the client's own state) and joins the **project root topic**: the
`[[projects]]` entry with the longest path containing the session's directory names it, and the topic is created if new.
Subscriptions persist per agent in `XAGENT_SUBSCRIPTIONS` (compare-and-set), so they follow the conversation across hosts.

## Wake transports

Idle sessions are woken with one fixed line, `[xagent] N unread messages for <workspace>/<agent> (M DM). Check your xagent inbox.`,
built from counts and the agent's own id. Peer text never reaches a client.

| Client | Transport | Idle test |
|---|---|---|
| Claude Code | Native cross-session inbox (undocumented by Claude Code; may change with an update): the Unix socket in the session record (`messagingSocketPath`) with the peer token from `<pid>.<sha256(socket)>.key`; frames are an auth line then a `<cross-session-message from-name="xagent">` user message | Session record `status: idle` for 3 s |
| Codex | App-server control socket (WebSocket JSON-RPC): `turn/start` on a loaded, idle, non-ephemeral thread | Thread re-read just before sending |
| dsh | dsh's local RPC: `session/prompt` in queue mode (loopback http; login token read from its launch log and exchanged per wake) | `session/list` `running` flag |

Each client is reached in its own profile directory: the registry under `CLAUDE_CONFIG_DIR` and the control socket
under `CODEX_HOME` as the MCP server's launch environment sets them (default `~/.claude`, `~/.codex`), carried from the
hello through `window_open` into the session's wake target.

Policy, shared: one pending wake per target; at most one per `wake_min_interval_seconds`; only for messages newer than
the last wake; dropped after `wake_max_wait_seconds` without an idle moment; skipped when the counts fell to zero or the
Stop hook already reported the same inbox state.

**Unbound windows.** An MCP connection claims no identity until its first tool call, yet messages may wait for the identity
it would bind (a resumed conversation). The daemon peeks every 15 s, by counters only (nothing pulled or acked, no lease),
at each window that holds no session, and wakes it when more messages wait than at the last peek. Codex threads are found
by asking the app-server for the loaded user threads in the window's directory.

**Hooks.** `claude-stop` and `codex-stop` run at end of turn: they block once per unread state with the reminder (counts,
owed replies) and say nothing for a conversation without a session. `claude-sendmessage` (PostToolUse on `SendMessage`)
mirrors a Claude-to-Claude send onto the `sendmessage` topic, so the bus stays a record of it.

## Trust boundaries

| Boundary | Rule |
|---|---|
| Model ↔ tools | No tool takes a sender, workspace or credential; the daemon derives them from the bound session |
| Peer message ↔ model | Advisory data. Bodies and refs are never fetched or executed. Priority never raises authority |
| Client ↔ daemon | Owner-only sockets, uid check; the relay forwards only the named launch environment (conversation, session ids, config dirs), never credentials |
| Daemon ↔ NATS | User/password from a 0600 file, or a JWT/NKey `.creds` file (`credentials_file`); TLS with a private CA (`tls_ca`). The NATS user may publish and subscribe only `xagent.v1.>`, the four `XAGENT_*` KV buckets, JetStream API and `_INBOX` |
| Wake | Fixed text; dsh URL must be loopback because its login token is sent there |
| Topics | Routing, not access control: everyone on the bus can read topic history. No secrets in messages |

## Failure handling

| Failure | Behavior |
|---|---|
| NATS down | Sends queue (`queued_for_retry`) and retry every 5 s while the session lives; reads work from SQLite; the daemon reconnects by itself |
| Lease unconfirmed | `lease_pending`: no pulling, sends only while the last refresh is fresh |
| Lease lost | `lease_lost`: the session can no longer act; restart it |
| Stream or bucket incompatible | Daemon runs but reports not-ready (`xagent status`); it never modifies existing resources |
| Duplicate `host_id` | The second daemon stays not ready (`nats.host_id_taken` in `xagent status`) until the first one's host lease ends; one that loses its host lease while running stops ([protocol](protocol.md#kv-records)) |
| Daemon restart | Each client's relay stays up: calls in flight and calls made while the daemon is down get JSON-RPC error `-32000`; the relay reconnects (backoff 0.1 s to 2 s) and replays the client's `initialize`, and the conversation binds again at its next call |
| Daemon crash | Messages wait in JetStream (the broker's retention, 7 days by default); the next session of each agent resumes from its consumer. `xagent recover` rebuilds a lost SQLite |
| Retention | The broker's retention is the stream's `max_age`: 7 days for a new stream, changed with `xagent retention --days N` for every host. Its size cap is the stream's `max_bytes`: 1 GiB for a new stream, changed with `xagent retention --max-mb N`; at the cap the broker deletes the oldest messages. Daemons adopt both and never change them. Each host's `[retention] local_days` keeps local copies shorter, never longer; `gc` runs at start and hourly |
