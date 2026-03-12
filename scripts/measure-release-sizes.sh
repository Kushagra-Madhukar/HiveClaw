#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

TARGET_DIR="${TARGET_DIR:-target/phase9-release-matrix}"
BIN_PATH="$TARGET_DIR/release/aria-x"

build_and_measure() {
  local label="$1"
  shift

  cargo build -p aria-x --release --target-dir "$TARGET_DIR" "$@" >/dev/null
  if [[ ! -f "$BIN_PATH" ]]; then
    echo "missing binary after build for $label" >&2
    exit 1
  fi

  local size_bytes
  size_bytes="$(wc -c < "$BIN_PATH" | tr -d ' ')"
  printf '| %s | `%s` | %s |\n' "$label" "cargo build -p aria-x --release $*" "$size_bytes"
}

echo '| Feature Set | Command | Size (bytes) |'
echo '| --- | --- | ---: |'
build_and_measure "default" 
build_and_measure "low-end-device" --no-default-features --features low-end-device
build_and_measure "low-end-device + mcp-runtime" --no-default-features --features low-end-device,mcp-runtime
build_and_measure "low-end-device + speech-runtime" --no-default-features --features low-end-device,speech-runtime
