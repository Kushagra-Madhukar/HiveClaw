#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[soak] running repeated architecture soak suite"

for round in 1 2 3; do
  echo "[soak] round ${round}"
  scripts/run-stress-suite.sh
  cargo test -p aria-x supervised_adapter_restarts_after_panic_and_updates_health --quiet
  cargo test -p aria-x inspect_operational_alerts_json_flags_synthetic_alerts --quiet
done

echo "[soak] suite completed"
