# Installation

Two layouts: **single host** (NATS on loopback) and **LAN** (one NATS server, a daemon on every participating host).
Nothing here edits shell rc files or overwrites an existing config or secret.

## Requirements

| Need | Note |
|---|---|
| Rust | 1.89+ (edition 2024); `cargo` builds everything, SQLite is bundled |
| nats-server | 2.10+ with JetStream; tested with 2.14.6. `scripts/setup-nats.sh` downloads a checksum-verified binary |
| OS | Linux (Unix sockets, systemd user units optional) |
| Clients | Claude Code, Codex and/or dsh |
| openssl | Only for `setup-nats.sh --tls` |

## 1. Build and install

```bash
git clone <repo> cross-agent && cd cross-agent
scripts/install.sh              # --prefix DIR for somewhere other than ~/.local
```

It runs `cargo build --release --locked -p xagent`, installs `~/.local/bin/xagent`, copies `config.example.toml` to
`~/.config/xagent/config.toml` (0600) if absent, and installs the systemd user units `xagentd` and `nats-server`. It
starts nothing. Without the script: `cargo install --path crates/xagent --locked`.

## 2. NATS

### Single host

```bash
scripts/setup-nats.sh           # nats-server binary, ~/.config/xagent/{nats-server.conf,nats.password}
scripts/run-nats.sh start       # without systemd: background, pid and log in ~/.local/state/xagent/
```

The generated config listens on `127.0.0.1:4222` (monitoring `127.0.0.1:8222`), stores JetStream in
`~/.local/share/xagent/nats` (0700), and defines one user `xagent` with a random password. That user may publish and subscribe
only `xagent.v1.>`, `$JS.API.>`, `$JS.ACK.>`, the four `$KV.XAGENT_*` buckets (`TOPICS`, `AGENTS`, `LEASES`,
`SUBSCRIPTIONS`) and `_INBOX.>`. Anonymous access is refused.

### LAN

1. `scripts/setup-nats.sh --listen 0.0.0.0:4222 --tls --san <nats-host-name> --san <lan-ip>` (or start from
   `deploy/nats-server.conf.example`). Every name and address clients use must be a SAN.
2. Firewall 4222 to the participating hosts only. Never expose it to the Internet. Keep `http` monitoring on loopback.
3. Copy `~/.config/xagent/pki/ca.pem` to each client host (never `ca-key.pem`) and the password file with it, or give each
   host its own user in the server config so one credential can be revoked alone.
4. Client hosts set `servers = ["tls://nats-host:4222"]` and `tls_ca` (below).

### PKI

`--tls` makes an ECDSA P-256 CA and server certificate in `~/.config/xagent/pki/`. The client library (rustls) checks
certificates strictly, so both must carry the extensions below; a certificate without them is refused at connect.

| Certificate | Required |
|---|---|
| CA (10 y) | `basicConstraints` critical `CA:TRUE,pathlen:0`; `keyUsage` critical `keyCertSign,cRLSign`; `subjectKeyIdentifier` |
| Server (825 d) | `keyUsage` critical `digitalSignature`; `extendedKeyUsage` `serverAuth`; `subjectKeyIdentifier`, `authorityKeyIdentifier`; `subjectAltName` for each host name and IP |

Check a certificate with `openssl verify -x509_strict -CAfile ca.pem server.pem`. To renew the server certificate, re-run
the `openssl x509 -req` step with the same CA and `pki/server.ext`, then reload nats-server.

## 3. Configuration

`~/.config/xagent/config.toml` (or `--config`, or `$XAGENT_CONFIG`). Every key is optional; unknown keys are refused.
`config.example.toml` documents each one.

```toml
[nats]
servers = ["tls://127.0.0.1:4222"]
user = "xagent"
password_file = "~/.config/xagent/nats.password"     # 0600; user and password_file go together
tls_ca = "~/.config/xagent/pki/ca.pem"

[[projects]]                                          # sessions under path join this root topic
path = "~/projects/editor-auto"
topic = "editor-auto"
```

| Section | Keys |
|---|---|
| top | `host_id` (default: short hostname; a label for diagnostics) |
| `[nats]` | `servers`, `user`, `password_file`, `tls_ca` |
| `[daemon]` | `socket` (`~/.local/state/xagent/xagentd.sock`; `mcp.sock` sits beside it), `state_db` (`~/.local/share/xagent/xagent.db`) |
| `[notify]` | `wake`, `wake_min_interval_seconds` (120), `wake_max_wait_seconds` (1800), `claude_sessions_dir`, `codex_control_socket`, `dsh_url`, `dsh_web_log` |
| `[[projects]]` | `path`, `topic` (one token; the most specific path wins) |

Secrets never go in this file, only the path of the password file.

## 4. Services

```bash
systemctl --user daemon-reload
systemctl --user enable --now nats-server xagentd
loginctl enable-linger $USER          # only for a host that runs without a login session
xagent doctor
```

`xagentd` runs `xagent daemon`, logs JSON to the journal, and restarts on a crash. On first connect it creates the stream and
the four KV buckets; it never modifies existing ones. Without systemd: `scripts/run-nats.sh start` and run `xagent daemon`
under any supervisor.

## 5. Clients

`xagent client-config --client claude|codex|dsh` prints the stanzas with this binary's absolute path (clients start
servers with a filtered environment, so use absolute paths). Copies are in `deploy/clients/`.

### Claude Code

```bash
claude mcp add -s user xagent -- ~/.local/bin/xagent mcp --client claude-code     # or a project .mcp.json
```

Add the hooks it prints to `~/.claude/settings.json`: `Stop` runs `xagent hook claude-stop` (end-of-turn inbox reminder);
`PostToolUse` on `SendMessage` runs `xagent hook claude-sendmessage` (mirrors Claude-to-Claude sends onto the
`sendmessage` topic). Idle sessions are woken through Claude's own cross-session inbox: no launch flag is needed.

**Claude's inbox and `crossSessionInbound`.** xagent wakes an idle Claude session by writing to the session's own
cross-session inbox, the socket Claude Code uses for its `SendMessage` tool. Claude Code does not document this
interface. xagent reads the session record and peer-token file that Claude writes under `~/.claude/sessions/`, so a
Claude Code update can change or remove it. When that happens, `xagent doctor` reports the Claude inbox as unreachable
and sessions stop waking; the Stop hook still reminds each session at the end of its turn.

xagent is not a Claude session and attests no permission mode. A session running in bypass-permissions mode holds
such a message and asks you to approve it. You choose between two settings in `~/.claude/settings.json`:

| `crossSessionInbound` | Effect |
|---|---|
| unset (default: hold) | Each wake waits for your approval in bypass-mode sessions. Sessions in other permission modes receive it as usual |
| `"accept"` | Wakes arrive at once. **This applies to every cross-session message, not only xagent's:** any local Claude session or process running as your user can then put a message straight into a bypass-mode session, which acts on it without your review |

Set `"accept"` only on a machine where you trust every process that runs as your user. The wake line itself carries
counts and an address, never peer text, so the risk comes from other senders, not from xagent.

Claude Code needs the `mcp__xagent` tools allowed in your permission settings if you do not want a prompt per call.

### Codex

Add to `~/.codex/config.toml` (or a trusted project's `.codex/config.toml`; untrusted projects' files are ignored):

```toml
[mcp_servers.xagent]
command = "/home/<user>/.local/bin/xagent"
args = ["mcp", "--client", "codex"]
startup_timeout_sec = 20
tool_timeout_sec = 30
```

Add the `Stop` hook to `~/.codex/hooks.json`. Codex asks once, in the TUI, to trust a new or changed hook ("Hooks need
review"); an untrusted hook does not run. Wake needs the Codex TUI's shared app-server (control socket at
`~/.codex/app-server-control/app-server-control.sock`). `codex exec` cannot answer MCP approval prompts: set
`default_tools_approval_mode = "approve"` for `[mcp_servers.xagent]` there.

### dsh

dsh starts one MCP server per session. Add the plugin `mcp-xagent` to `~/.dsh/profiles/web/cordis.patch.yml`:

```yaml
command: /home/<user>/.local/bin/xagent
args: ['mcp', '--client', 'dsh']
perAgent: true
sessionIdEnv: XAGENT_CONVERSATION_ID     # the dsh session id is the conversation
```

The optional end-of-turn reminder is the `claude-stop` hook in `~/.dsh/xagent-hooks.json` (dsh runs Claude-format hooks).
Wake uses dsh's loopback RPC (`dsh_url`, default `http://127.0.0.1:3080`) and the login token in `dsh_web_log`.

### Skill

```bash
scripts/install-skills.sh [claude] [codex]     # ~/.claude/skills/cross-agent, ~/.agents/skills/cross-agent
```

## 6. Topics

Topics must exist before anyone posts to them. A session's project root topic is created on first join. Agents create
others with `topic_create` (the skills limit this to stable work domains), or you do:

```bash
xagent topic create --conversation human --client claude-code firmware.usb --description "USB device firmware"
```

## 7. Maintenance

| Task | Command |
|---|---|
| Health | `xagent doctor`, `xagent status` |
| Retention | Automatic at start and hourly; `xagent gc --dry-run` previews. No local copy outlives the broker's 7 days |
| Lost SQLite | Stop the daemon, move the DB aside, start it, `xagent recover --hours 24` (unread again; at most 7 days back) |
| Reset one consumer | Stop the daemon, `nats consumer rm XAGENT_MESSAGES xagent_<ws>_<agent>_<hash>`, start it; `recover` fills the gap |

## Uninstall

```bash
systemctl --user disable --now xagentd nats-server
rm -f ~/.config/systemd/user/{xagentd,nats-server}.service ~/.local/bin/xagent
rm -rf ~/.local/share/xagent ~/.local/state/xagent ~/.config/xagent ~/.claude/skills/cross-agent ~/.agents/skills/cross-agent
claude mcp remove xagent; codex mcp remove xagent
```
