#!/usr/bin/env bash
# Build and install xagent and its systemd user unit for the current user. Idempotent; never
# overwrites an existing config or secret; never edits shell rc files; never starts or restarts a
# service. scripts/setup-nats.sh installs nats-server and its unit.
#
#   scripts/install.sh [--prefix DIR]     (default ~/.local: binary in DIR/bin)
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# XDG base directory: $1 when it is an absolute path (the spec ignores relative ones), else ~/$2.
xdg() { local dir="${!1:-}"; [[ "$dir" == /* ]] || dir="$HOME/$2"; printf '%s\n' "$dir"; }
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

CONF_DIR="$(xdg XDG_CONFIG_HOME .config)/xagent"
mkdir -p "$CONF_DIR" && chmod 700 "$CONF_DIR"
if [[ ! -f "$CONF_DIR/config.toml" ]]; then
  install -m 0600 "$REPO/config.example.toml" "$CONF_DIR/config.toml"
  echo "==> wrote $CONF_DIR/config.toml"
fi

UNIT_DIR="$(xdg XDG_CONFIG_HOME .config)/systemd/user"
mkdir -p "$UNIT_DIR"
# The user manager may not see this shell's XDG variables: pass the ones set to the daemon, so it
# uses the same config, state database and socket as the CLI.
ENV_LINES=""
for var in XDG_CONFIG_HOME XDG_DATA_HOME XDG_STATE_HOME; do
  [[ "${!var:-}" == /* ]] && ENV_LINES+="Environment=$var=${!var}\n"
done
sed -e "s|^ExecStart=.*|${ENV_LINES}ExecStart=$BIN daemon|" "$REPO/deploy/systemd/xagentd.service" > "$UNIT_DIR/xagentd.service"
echo "==> installed $UNIT_DIR/xagentd.service"

cat <<NEXT

Next steps:
  scripts/setup-nats.sh [--prefix $PREFIX] [--tls]
                                    nats-server, its unit, password, optional PKI (skip if you have a NATS server)
  edit $CONF_DIR/config.toml        [nats] servers, user, password_file, tls_ca; [[projects]]
  systemctl --user daemon-reload && systemctl --user enable --now nats-server xagentd
  $BIN doctor
  $BIN client-config --client claude|codex|dsh|antigravity      client setup
  scripts/install-skills.sh         the cross-agent skill for Claude Code, Codex and Antigravity
NEXT
