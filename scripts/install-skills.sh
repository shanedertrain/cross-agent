#!/usr/bin/env bash
# Install the cross-agent skill for Claude Code and/or Codex (user scope). Copies files;
# replaces only a previous copy of this same skill directory.
#   scripts/install-skills.sh [claude] [codex]      (default: both)
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
targets=("$@"); [[ ${#targets[@]} -gt 0 ]] || targets=(claude codex)
for t in "${targets[@]}"; do
  case "$t" in
    claude) dest="${CLAUDE_CONFIG_DIR:-$HOME/.claude}/skills/cross-agent"; src="$REPO/skills/claude/cross-agent" ;;
    codex)  dest="$HOME/.agents/skills/cross-agent"; src="$REPO/skills/codex/cross-agent" ;;
    *) echo "unknown target $t (claude|codex)" >&2; exit 2 ;;
  esac
  if [[ -e "$dest" && ! -f "$dest/SKILL.md" ]]; then
    echo "refusing to replace $dest: not a skill directory" >&2; exit 1
  fi
  if [[ -f "$dest/SKILL.md" ]] && ! grep -q "xagent-core:start" "$dest/SKILL.md"; then
    echo "refusing to replace $dest: a different skill named cross-agent exists" >&2; exit 1
  fi
  mkdir -p "$(dirname "$dest")"
  rm -rf "$dest"
  cp -R "$src" "$dest"
  echo "installed $t skill -> $dest"
done
