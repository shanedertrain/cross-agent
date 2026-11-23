#!/usr/bin/env bash
# Build and install xagent for the current user. Idempotent; never overwrites an existing config
# or secret; never edits shell rc files; never starts or restarts a service.
#
#   scripts/install.sh [--prefix DIR]     (default ~/.local: binary in DIR/bin)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PREFIX="$HOME/.local"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prefix) PREFIX="$2"; shift 2 ;;
    -h|--help) sed -n '2,6p' "$0"; exit 0 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done

command -v cargo >/dev/null || { echo "cargo not found: install Rust (https://rustup.rs)" >&2; exit 1; }
cargo build --release --locked --manifest-path "$REPO/Cargo.toml" -p xagent

BIN="$PREFIX/bin/xagent"
mkdir -p "$PREFIX/bin"
install -m 0755 "$REPO/target/release/xagent" "$BIN.new" && mv -f "$BIN.new" "$BIN"
echo "==> installed $BIN ($("$BIN" --version))"

CONF_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/xagent"
mkdir -p "$CONF_DIR" && chmod 700 "$CONF_DIR"
if [[ ! -f "$CONF_DIR/config.toml" ]]; then
  install -m 0600 "$REPO/config.example.toml" "$CONF_DIR/config.toml"
  echo "==> wrote $CONF_DIR/config.toml"
fi

UNIT_DIR="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$UNIT_DIR"
sed "s|^ExecStart=.*|ExecStart=$BIN daemon|" "$REPO/deploy/systemd/xagentd.service" > "$UNIT_DIR/xagentd.service"
cp "$REPO/deploy/systemd/nats-server.service" "$UNIT_DIR/nats-server.service"
echo "==> installed units in $UNIT_DIR"

cat <<NEXT

Next steps:
  scripts/setup-nats.sh [--tls]     nats-server, password, optional PKI (skip if you have a NATS server)
  edit $CONF_DIR/config.toml        [nats] servers, user, password_file, tls_ca; [[projects]]
  systemctl --user daemon-reload && systemctl --user enable --now nats-server xagentd
  $BIN doctor
  $BIN client-config --client claude|codex|dsh      client setup
  scripts/install-skills.sh         the cross-agent skill for Claude Code and Codex
NEXT
