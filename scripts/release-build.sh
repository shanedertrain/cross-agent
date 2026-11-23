#!/usr/bin/env bash
# Builds a static (musl) xagent for this machine's architecture from a git ref, inside the official
# rust:alpine image, and packs it for a GitHub release:
#   <out>/xagent-<version>-<arch>-linux-musl.tar.gz (xagent, LICENSE, README.md) and its .sha256.
# Run it once per architecture, on a machine of that architecture.
#
# Usage: scripts/release-build.sh [REF] [OUT_DIR]     (defaults: HEAD, dist)
set -euo pipefail

ref=${1:-HEAD}
root=$(git rev-parse --show-toplevel)
out=$(realpath -m "${2:-$root/dist}")
image=rust:1.98-alpine
arch=$(uname -m)
version=$(git -C "$root" describe --tags --exact-match "$ref" 2>/dev/null || git -C "$root" rev-parse --short "$ref")
name="xagent-${version}-${arch}-linux-musl"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
git -C "$root" archive "$ref" | tar -x -C "$work"

docker run --rm -v "$work:/work" -w /work "$image" sh -euc "
  apk add --no-cache musl-dev >/dev/null
  cargo build --release --locked
  chown -R $(id -u):$(id -g) /work"

bin="$work/target/release/xagent"
file "$bin" | grep -q 'static' || { echo "not a static binary: $(file "$bin")" >&2; exit 1; }
# Runs on a bare image with no libc of its own.
docker run --rm -v "$bin:/xagent:ro" busybox:stable /xagent --version

mkdir -p "$out" "$work/$name"
cp "$bin" "$root/LICENSE" "$root/README.md" "$work/$name/"
tar -C "$work" -czf "$out/$name.tar.gz" "$name"
(cd "$out" && sha256sum "$name.tar.gz" > "$name.tar.gz.sha256")
echo "$out/$name.tar.gz"
