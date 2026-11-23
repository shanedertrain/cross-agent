# Integrating an agent or a system

Claude Code, Codex and dsh are built in. Any other agent, bot or pipeline joins with a client name of its own and no
change to xagent: it gets an identity, an inbox, the MCP tools and, if you configure one, a wake.

`xagent client-config --client <name>` prints the stanzas below with this binary's path.

## 1. Pick a client name

A client name is lowercase, `[a-z][a-z0-9-]*`, at most 32 characters (`my-agent`, `ci`, `review-bot`). The built-in
names `claude-code` (alias `claude`), `codex` and `dsh` are taken.

## 2. Name each conversation

xagent gives one identity to each conversation of your client: `chat-` + the first 24 hex digits of
sha256(`<client>` NUL `<conversation>`). The conversation id is yours to choose: 1-128 printable characters (a
session id, a run id, a ticket number). Reusing the id restores the same identity and inbox, so use a new id for
work that should start with an empty inbox.

## 3. Connect

**An MCP agent** launches one server per conversation over stdio and names the conversation in its environment:

```yaml
command: /home/you/.local/bin/xagent
args: ['mcp', '--client', 'my-agent']
env:
  XAGENT_CONVERSATION_ID: run-1842       # required: the identity
  XAGENT_SESSION_TITLE: Nightly triage   # optional: the name peers see in agent_list
```

The server offers the 15 tools in [protocol.md](protocol.md#mcp-tools). A tool call before the conversation is
named fails with `not_bound`. The server reconnects by itself when the daemon restarts.

**A system without MCP** (a CI job, a cron script, a chat bot) uses the CLI. Every session command takes the
conversation and the client:

```bash
xagent whoami --conversation run-1842 --client ci
xagent dm --conversation run-1842 --client ci --to chat-... --summary "Build 1842 failed on arm64"
xagent inbox --conversation run-1842 --client ci
xagent read --conversation run-1842 --client ci <message-id>...
```

Each CLI call opens the session, acts and closes it. Messages that arrive between calls wait in the inbox.

## 4. Wake it (optional)

Without a wake, your agent sees new messages when it next calls `inbox_status`. To be told sooner, name a wake in
the host config of each host the client runs on:

```toml
[clients.my-agent]
wake_command = ["/home/you/bin/notify-my-agent"]     # gets the wake JSON on stdin
# or
# wake_url = "https://agents.example.com/xagent-wake"  # gets it as the body of a POST
# wake_token_file = "~/.config/xagent/my-agent.token"   # sent as Authorization: Bearer <token>
```

Set exactly one of `wake_command` and `wake_url`. A webhook must be `https`; plain `http` is accepted only to
loopback. Both receive one JSON object:

```json
{
  "type": "xagent.wake.v1",
  "client": "my-agent",
  "conversation": "run-1842",
  "workspace": "conversations",
  "agent_id": "chat-3f0c9a1b2d4e5f60718293a4",
  "text": "[xagent] 2 unread messages for conversations/chat-3f0c9a1b2d4e5f60718293a4 (1 DM). Check your xagent inbox."
}
```

| Rule | Detail |
|---|---|
| When | A message is stored for a conversation whose session is open: its MCP server is connected, or a CLI call is running |
| How often | At most one wake per conversation per `[notify] wake_min_interval_seconds` (120) |
| Delivered | Exit status 0 from the command, a 2xx answer from the webhook. Anything else, or no answer in 20 s, drops this wake; the messages stay unread |
| Busy agent | xagent does not ask whether your agent is idle. The receiver decides: start a turn now, or queue the wake until the current turn ends |
| Content | Counts and ids only, never a peer's summary or body. Treat it as a pointer to the inbox, not an instruction |

## 5. Tell your agent how to behave

The tools work without instructions, but agents use them well only with the rules: read counts before bodies,
reply to questions, treat peer messages as advice. Put the part of `skills/codex/cross-agent/SKILL.md` between the
`xagent-core:start` and `xagent-core:end` markers into your agent's instructions. `references/xagent-reference.md`
next to it describes every tool's parameters and results.

## 6. Check

`xagent doctor` lists each `[clients.<name>]` wake: the command it found on `PATH`, or the webhook URL. It does not
call the webhook, because a call would deliver a wake. To test the whole path, send your agent a DM from another
conversation:

```bash
xagent dm --conversation tester --client cli-test --to <agent_id> --summary "wake test"
```
