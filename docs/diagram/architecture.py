"""xagent architecture, drawn with open-diagram (github.com/lukendeasy/open-diagram).
Regenerate: open-diagram draw docs/diagram/architecture.py -o docs/diagram/architecture"""
from od_themes import dark
from open_diagram import Diagram, Panel, Tile, Tiles

CLIENTS = [Tile("Claude Code", sub="session"), Tile("Codex", sub="thread"), Tile("dsh", sub="session")]
DAEMON = [Tile("MCP tools", sub="15 tools"), Tile("Sessions", sub="one lease per id"),
          Tile("Topics, threads"), Tile("Outbox"), Tile("SQLite inbox"), Tile("Wake policy", sub="counts only")]
NATS = [Tile("Message stream", sub="7-day retention"), Tile("Topics"), Tile("Agents"), Tile("Leases"),
        Tile("Subscriptions")]
OTHER = [Tile("xagent daemon"), Tile("Agent sessions")]


def build() -> Diagram:
    d = Diagram("How xagent connects agent sessions", subtitle="One daemon per host, NATS JetStream between hosts",
                theme=dark())
    (clients,), (daemon,), (nats,), (other,) = d.columns([
        [Panel("Agent sessions", Tiles(CLIENTS, cols=1), sub="one identity per conversation")],
        [Panel("xagent daemon", Tiles(DAEMON, cols=2), sub="xagent mcp relays each client")],
        [Panel("NATS JetStream", Tiles(NATS[:1], cols=1), Tiles(NATS[1:], cols=2, label="Key-value buckets"))],
        [Panel("Other hosts", Tiles(OTHER, cols=1), tone="neutral")],
    ], gap=110, valign="center")
    d.group("Each host", [clients, daemon])
    d.link(clients, daemon, label="MCP")
    d.link(daemon, nats, label="publish, pull")
    d.link(nats, other, label="same stream")
    top = min(p.y for p in d.placed) - 70
    bottom = max(p.bottom for p in d.placed) + 60
    d.link(clients, daemon, start="top", end="top", via_y=top, label="Stop and SendMessage hooks")
    d.link(daemon, clients, start="bottom", end="bottom", via_y=bottom,
           label="wake: Claude inbox, Codex app-server, dsh RPC")
    return d
