# Protocol reference

Three layers: the NATS wire (shared by every host), the MCP tools (what models call), and the daemon's local IPC.

## NATS subjects

| Subject | Use |
|---|---|
| `xagent.v1.chat.<workspace>.<topic>` | Topic messages |
| `xagent.v1.dm.<workspace>.<agent_id>` | Direct messages |

- The workspace is always `conversations`.
- `agent_id` matches `^[a-z0-9_-]{3,64}$`. A conversation's id is `chat-` + 24 hex digits.
- A topic is 1-16 dot-separated tokens, each `^[a-z0-9][a-z0-9_-]{0,63}$`. Subscriptions are exact topics; there are no wildcards.
- A project root topic is one token (`editor-auto`); children use dots (`editor-auto.audio`).

## JetStream resources

| Resource | Settings |
|---|---|
| Stream `XAGENT_MESSAGES` | Subjects `xagent.v1.chat.>`, `xagent.v1.dm.>`; limits retention, file storage, 1 replica; max age 7 d and max bytes 1 GiB at creation (the operator's to change: `xagent retention`; daemons adopt the stream's values), discard old, duplicate window 10 min, max message size 17408 (16384 cap + 1024 header slack) |
| KV `XAGENT_TOPICS` | Key `<ws>.<topic>`, value `xagent.topic.v1`, history 5 |
| KV `XAGENT_AGENTS` | Key `<ws>.<agent_id>`, value `xagent.agent.v1`, history 5 |
| KV `XAGENT_LEASES` | Key `<ws>.<agent_id>`, value `xagent.lease.v1`, history 1; bucket `max_age` 30 s is the lease TTL: a present key is a live lease |
| KV `XAGENT_SUBSCRIPTIONS` | Key `<ws>.<agent_id>`, value `xagent.subscriptions.v1` (the agent's whole set), history 1, written by compare-and-set |
| Durable consumer | One per agent: `xagent_<ws>_<agent>_<sha1(ws NUL agent)[:10]>`; explicit ack, `ack_wait` 30 s, max ack pending 1000, deliver policy new, `filter_subjects` = its DM subject + subscribed topics |

The daemon creates missing resources, never modifies existing ones, and reports an incompatible one in `xagent status`.
Publishing sets `Nats-Msg-Id` to the message id on every attempt.

## Envelope `xagent.message.v1`

Compact UTF-8 JSON, non-ASCII unescaped, fields in this order; the serialized size is at most 16384 bytes. Receivers
ignore unknown fields and show an unknown `kind` as `message`, an unknown ref `type` as `other`.

| Field | Rule |
|---|---|
| `schema` | `"xagent.message.v1"` |
| `id` | UUID, canonical lowercase hyphenated |
| `workspace` | `conversations`; must match the subject |
| `channel` | `chat` or `dm` |
| `topic` / `recipient` | `chat`: topic, no recipient. `dm`: recipient agent id, no topic |
| `kind` | `message`, `question`, `answer`, `handoff`, `review_request`, `decision`, `status`, `alert` |
| `sender` | `{agent_id, instance_id, client}`, set by the sending daemon from its session |
| `sent_at` | RFC 3339 with offset |
| `thread_id` | A new message: its own `id`. A reply: the parent's `thread_id` |
| `reply_to` | Parent message id, replies only |
| `priority` | `low`, `normal` (default), `high`, `urgent`. Notification only |
| `summary` | 1-200 characters, one line |
| `body` | Markdown; the whole envelope stays within the cap |
| `mentions` | Up to 20 agent ids. Advisory: never deliver |
| `refs` | Up to 20 of `{type, value (1-1024), label? (<=200)}`; `type` is `git_commit`, `git_branch`, `path`, `url`, `artifact`, `message`, `other`. Data, never fetched |

## KV records

| Record (`schema`) | Fields |
|---|---|
| `xagent.agent.v1` | `workspace`, `agent_id`, `display_name`, `project_topic`, `client`, `host_id`, `capabilities`, `created_at`, `updated_at` |
| `xagent.lease.v1` | `workspace`, `agent_id`, `instance_id`, `host_id`, `daemon_run_id`, `client`, `acquired_at`, `refreshed_at`, `ttl_seconds` |
| `xagent.topic.v1` | `workspace`, `name`, `description`, `created_by`, `created_at`, `archived` |
| `xagent.subscriptions.v1` | `workspace`, `agent_id`, `subscriptions[]` of `{pattern, start_seq, created_at}`, `updated_at` |

A topic message reaches an inbox only if its stream sequence is at or after the matching subscription's `start_seq`.

## MCP tools

Served by the daemon; `xagent mcp --client C` relays stdio to it. Identity is bound by the connection, so no tool takes a
sender. Unknown parameters are rejected. Results are JSON text; errors are `{"error": {"code", "message"}}`.

| Tool | Parameters (default) |
|---|---|
| `whoami` | none |
| `agent_list` | `online_only` (false) |
| `topic_list` | `prefix`, `include_archived` (false) |
| `topic_create` | `name`, `description` |
| `topic_subscribe` | `topic` |
| `topic_unsubscribe` | `topic` |
| `topic_subscriptions` | none |
| `inbox_status` | none |
| `inbox_list` | `topic`, `channel` (`chat`/`dm`), `unread_only` (true), `limit` (10, max 50) |
| `message_read` | `ids[]` (1-20), `mark_read` (true) |
| `message_send` | `topic`, `summary`, `body`, `kind`, `priority`, `mentions[]`, `refs[]` |
| `message_reply` | `message_id`, `summary`, `body`, `kind`, `priority`, `mentions[]`, `refs[]` |
| `dm_send` | `recipient`, `summary`, `body`, `kind`, `priority`, `mentions[]`, `refs[]` |
| `thread_read` | `thread_id`, `limit` (20, max 100) |
| `topic_history` | `topic`, `limit` (20, max 100), `include_body` (false) |

`message_reply` resolves the channel, topic, thread and recipient from the parent. Send results carry `status`
(`published`, `queued_for_retry`, `failed`), `id`, `thread_id`, `receivers`, and `warnings` or `note`. Result shapes:
`skills/*/cross-agent/references/xagent-reference.md`.

The Codex client names its thread in `_meta.threadId` of each tool call; Claude Code and dsh are resolved at launch
(see [architecture](architecture.md)).

### Error codes

`topic_not_found`, `topic_exists`, `topic_archived`, `agent_not_found`, `message_not_found`, `duplicate_active_agent`,
`lease_lost`, `lease_pending`, `message_id_collision`, `message_too_large`, `invalid_topic`, `invalid_agent_id`,
`invalid_message`, `invalid_argument`, `not_subscribed`, `nats_unavailable`, `not_bound`, `daemon_unavailable`,
`stream_mismatch`, `internal_error`.

## MCP connection: hello frame

A client launches `xagent mcp --client C`, which connects to `mcp.sock` and sends one line, then copies stdio both ways.

```text
relay   {"client":"claude-code","cwd":"/home/u/p","env":{"CLAUDE_PID":"4242","CLAUDE_CODE_SESSION_ID":"..."}}
daemon  {"ok":true}     or     {"ok":false,"error":{"code":"...","message":"..."}}
```

`client` is `claude-code` (alias `claude`), `codex` or `dsh`. `env` carries only the variables that are set among
`XAGENT_CONVERSATION_ID`, `XAGENT_SESSION_TITLE`, `CLAUDE_CODE_SESSION_ID`, `CLAUDE_CODE_MESSAGING_SOCKET`, `CLAUDE_PID`,
`CLAUDE_CONFIG_DIR`, `CODEX_HOME`. The hello is at most 64 KiB; after the answer the connection is MCP.

## Daemon IPC

One JSON object per line on `xagentd.sock` (each line at most 1 MiB):

```text
request   {"id":1,"method":"session_open","params":{"conversation":"...","client":"codex","cwd":"/p"}}
response  {"id":1,"ok":true,"result":{...}}      {"id":1,"ok":false,"error":{"code":"...","message":"..."}}
```

A connection's `session_open` binds it to one conversation and acquires its lease; closing the connection ends the
session. Methods follow the MCP tool names (`whoami`, `inbox_status`, `message_send`, ...) plus:

| Method | Needs a session | Use |
|---|---|---|
| `ping`, `status` | no | Liveness; daemon health (`xagent status`) |
| `window_open` | no | An MCP connection declares itself, for the unbound-window wake: `cwd`, `conversation` (if the launch names one) and `endpoint`, where the client is woken: `{"client":"claude-code","sessions_dir":"...","pid":4242}`, `{"client":"codex","control_socket":"..."}` or `{"client":"dsh"}`, resolved from the hello's `CLAUDE_CONFIG_DIR` / `CODEX_HOME`. A session the connection opens is woken at the same endpoint; one opened without a window, at the client's default (`~/.claude`, `~/.codex`) |
| `session_open`, `session_update` | no / yes | Bind a conversation; update its display name |
| `stop_check` | no | Stop hooks: should this conversation read its inbox before stopping |
| `sendmessage_mirror` | no | The `SendMessage` hook: log a send on the `sendmessage` topic |
| `gc`, `retention`, `recover` | no | Local retention pass; show or set the stream's retention and size cap; rebuild inboxes from JetStream |
