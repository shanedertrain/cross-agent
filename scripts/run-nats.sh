#!/usr/bin/env bash
# Start nats-server in the background with the xagent config (no systemd required).
#   scripts/run-nats.sh [start|stop|status]
set -euo pipefail
CONF_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/xagent"
STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/xagent"
CONF="${NATS_CONF:-$CONF_DIR/nats-server.conf}"
PIDFILE="$STATE_DIR/nats-server.pid"
LOG="$STATE_DIR/nats-server.log"
mkdir -p "$STATE_DIR" && chmod 700 "$STATE_DIR"

running() { [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; }

case "${1:-start}" in
  start)
    if running; then echo "nats-server already running (pid $(cat "$PIDFILE"))"; exit 0; fi
    [[ -f "$CONF" ]] || { echo "missing $CONF — run scripts/setup-nats.sh" >&2; exit 1; }
    nohup nats-server -c "$CONF" -P "$PIDFILE" >>"$LOG" 2>&1 &
    for _ in $(seq 50); do running && break; sleep 0.1; done
    running && echo "nats-server started (pid $(cat "$PIDFILE")), log $LOG" || { echo "failed; see $LOG" >&2; exit 1; }
    ;;
  stop)
    running && kill "$(cat "$PIDFILE")" && echo "nats-server stopped" || echo "not running"
    ;;
  status)
    running && echo "running (pid $(cat "$PIDFILE"))" || { echo "not running"; exit 1; }
    ;;
  *) echo "usage: $0 [start|stop|status]" >&2; exit 2 ;;
esac
