#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[stress] running bounded architecture stress suite"

cargo test -p aria-x startup_smoke_core_inspect_path_stays_within_budget --quiet
cargo test -p aria-x memory_smoke_session_tool_cache_remains_bounded_under_many_sessions --quiet
cargo test -p aria-x memory_smoke_web_storage_policy_keeps_browser_and_crawl_state_bounded_under_stress --quiet
cargo test -p aria-x ingress_queue_bridge_preserves_fifo_order_for_mixed_channels --quiet
cargo test -p aria-x partitioned_ingress_queue_bridge_preserves_same_key_order --quiet
cargo test -p aria-x shared_quota_claim_rejects_second_holder_when_limit_is_one --quiet
cargo test -p aria-x poll_due_job_events_from_store_claims_and_dispatches_due_job --quiet
cargo test -p aria-x send_universal_response_applies_opt_in_fanout_policy --quiet
cargo test -p aria-x supervised_adapter_restarts_after_panic_and_updates_health --quiet
cargo test -p aria-x admin_inspect_command_reads_channel_transport_diagnostics --quiet

echo "[stress] suite completed"
