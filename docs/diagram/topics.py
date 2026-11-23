"""xagent topics, drawn with open-diagram (github.com/lukendeasy/open-diagram).
Regenerate: open-diagram draw docs/diagram/topics.py -o docs/diagram/topics"""
from od_themes import dark
from open_diagram import Diagram, Panel, Tile, Tiles

PROJECT = [Tile("1. Configured path", sub="[[projects]] path maps to a topic"),
           Tile("2. Git checkout name", sub="a linked worktree counts as its main checkout"),
           Tile("3. scratch", sub="no project, no git")]
ROOMS = [Tile("webapp", sub="project root topic"), Tile("webapp.api", sub="child: dotted name"),
         Tile("sendmessage", sub="mirrors Claude SendMessage")]
INBOX = [Tile("Session A", sub="webapp"), Tile("Session B", sub="webapp + webapp.api"),
         Tile("Session C", sub="webapp.api only")]
SEND = [Tile("receivers", sub="count of subscribers reached"), Tile("receivers: 0", sub="warning: nobody receives"),
        Tile("message_reply", sub="stays in the same thread")]
PAST = [Tile("topic_history", sub="reads old posts, adds nothing to the inbox")]


def build() -> Diagram:
    d = Diagram("How xagent topics work", subtitle="A topic is a named room in the shared conversations workspace",
                theme=dark())
    (proj,), (rooms, past), (inbox, sendp) = d.columns([
        [Panel("Project topic", Tiles(PROJECT, cols=1), sub="chosen from the session's directory; joined on start")],
        [Panel("Topics", Tiles(ROOMS, cols=1), sub="one NATS subject each: xagent.v1.chat.conversations.<topic>"),
         Panel("Catch-up", Tiles(PAST, cols=1), tone="neutral")],
        [Panel("Subscriber inboxes", Tiles(INBOX, cols=1), sub="a subscription starts now, with no history"),
         Panel("Send result", Tiles(SEND, cols=1), tone="neutral")],
    ], gap=120, row_gap=50, valign="top")
    d.link(proj, rooms, label="auto-join root")
    d.link(rooms, inbox, label="message_send")
    top = min(p.y for p in d.placed) - 70
    d.link(inbox, rooms, start="top", end="top", via_y=top,
           label="topic_subscribe (children opt-in, persists)")
    d.link(rooms, past, start="bottom", end="top")
    return d
