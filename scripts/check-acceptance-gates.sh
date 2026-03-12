#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

matrix="docs/ARCHITECTURE_ACCEPTANCE_MATRIX.md"

if [[ ! -f "$matrix" ]]; then
  echo "missing acceptance matrix: $matrix" >&2
  exit 1
fi

core_pending=$(awk '
  /^## Core/ { in_core=1; next }
  /^## Optional/ { in_core=0 }
  in_core && /\|/ && $0 ~ /`pending`/ { count++ }
  END { print count+0 }
' "$matrix")

if [[ "$core_pending" -ne 0 ]]; then
  echo "core acceptance matrix still has pending entries" >&2
  exit 1
fi

echo "acceptance gates passed"
