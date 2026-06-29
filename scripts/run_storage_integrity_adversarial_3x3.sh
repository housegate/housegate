#!/usr/bin/env bash
set -euo pipefail

SCENARIO="${1:-all}"

if [[ "$SCENARIO" == "all" ]]; then
    base_run_id="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
    for scenario in replay-minority unsafe-mismatch safe-audit-minority; do
        echo "[$(date -u +%H:%M:%S)] running adversarial scenario: $scenario"
        RUN_ID="$base_run_id-$scenario" "$0" "$scenario"
    done
    echo "[$(date -u +%H:%M:%S)] E2E PASS: adversarial 3x3 scenarios completed"
    exit 0
fi

case "$SCENARIO" in
    replay-minority)
        export BASE_PREFIX="${BASE_PREFIX:-e2e-storage-integrity-adversarial-replay}"
        export HG_WORKER_REPLAY=false
        export HG_WORKER_UNSAFE_VALIDATION=false
        export HG_WORKER_PROMOTION=false
        export HG_WORKER_ROLLBACK=false
        export HG_WORKER_SAFE_AUDIT=false
        export HG_WORKER_FINALITY=false
        ;;
    unsafe-mismatch)
        export BASE_PREFIX="${BASE_PREFIX:-e2e-storage-integrity-adversarial-unsafe}"
        export HG_WORKER_REPLAY=true
        export HG_WORKER_UNSAFE_VALIDATION=false
        export HG_WORKER_PROMOTION=false
        export HG_WORKER_ROLLBACK=false
        export HG_WORKER_SAFE_AUDIT=false
        export HG_WORKER_FINALITY=false
        ;;
    safe-audit-minority)
        export BASE_PREFIX="${BASE_PREFIX:-e2e-storage-integrity-adversarial-safe-audit}"
        export HG_WORKER_REPLAY=true
        export HG_WORKER_UNSAFE_VALIDATION=true
        export HG_WORKER_PROMOTION=true
        export HG_WORKER_ROLLBACK=false
        export HG_WORKER_SAFE_AUDIT=false
        export HG_WORKER_FINALITY=false
        ;;
    *)
        echo "usage: $0 [all|replay-minority|unsafe-mismatch|safe-audit-minority]" >&2
        exit 2
        ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/run_storage_integrity_sql_rewriter_3x3.sh"

keeper_create_or_set() {
    local path="$1"
    local data="$2"
    if ! keeper_create_path "$path" "$data" >/dev/null 2>&1; then
        keeper_query "${KEEPER_CLIENT_PORTS[0]}" "set $(keeper_quote_path "$path") '$data'" >/dev/null
    fi
}

keeper_create_checked() {
    local path="$1"
    local data="$2"
    local out
    out="$(keeper_create_path "$path" "$data" 2>&1 || true)"
    if [[ "$out" == *"Coordination error"* || "$out" == *"Can't create"* ]]; then
        die "keeper create failed for $path: $out"
    fi
}

keeper_ensure_path() {
    local path="$1"
    local out
    out="$(keeper_create_path "$path" "created_by=e2e\\n" 2>&1 || true)"
    if [[ "$out" == *"Coordination error"* && "$out" != *"Node exists"* && "$out" != *"already exists"* ]]; then
        die "keeper ensure path failed for $path: $out"
    fi
    if ! keeper_path_exists "$path"; then
        die "keeper ensure path did not create $path: $out"
    fi
}

assert_keeper_path_absent_after() {
    local label="$1"
    local path="$2"
    local seconds="$3"
    sleep "$seconds"
    if keeper_path_exists "$path"; then
        local data
        data="$(keeper_get "$path" 2>&1 | tr -d '\r' || true)"
        die "$label unexpectedly exists at $path; data=$data"
    fi
    log "$label absent at $path"
}

set_generated_worker_flag() {
    local i="$1"
    local name="$2"
    local value="$3"
    local cfg="$BASE/hg$i/housegate.yaml"
    sed -i "s/^    ${name}: .*/    ${name}: ${value}/" "$cfg"
}

configure_server_housegate_workers() {
    local replay="$1"
    local unsafe_validation="$2"
    local promotion="$3"
    local rollback="$4"
    local safe_audit="$5"
    local finality="$6"
    for i in 1 2 3; do
        set_generated_worker_flag "$i" replay "$replay"
        set_generated_worker_flag "$i" unsafe_validation "$unsafe_validation"
        set_generated_worker_flag "$i" promotion "$promotion"
        set_generated_worker_flag "$i" rollback "$rollback"
        set_generated_worker_flag "$i" safe_audit "$safe_audit"
        set_generated_worker_flag "$i" finality "$finality"
    done
}

restart_server_housegates() {
    for i in 1 2 3; do
        start_container "si3x3rw-$RUN_ID-hg$i" "$BASE/logs/hg$i-restart.stdout" "$HOUSEGATE_BIN -config $BASE/hg$i/housegate.yaml -log-level debug"
    done
    for port in "${SERVER_HG_PORTS[@]}"; do
        wait_for_tcp "server-housegate:$port restarted" "$port" || {
            for i in 1 2 3; do tail -180 "$BASE/logs/hg$i-restart.stdout" || true; done
            die "server housegate ports did not reopen"
        }
    done
}

adversarial_statement_id() {
    local statement_id
    statement_id="$(first_storage_integrity_statement)" || die "no statement ledger found"
    wait_for_keeper_path "statement replay job" "/housekeeper/v1/storage_integrity/replay_jobs/$statement_id" 30 >&2
    wait_for_keeper_path "statement unsafe task" "/housekeeper/v1/storage_integrity/unsafe_tasks/$statement_id" 30 >&2
    printf '%s\n' "$statement_id"
}

submit_replay_attestation() {
    local statement_id="$1"
    local worker_id="$2"
    local state_root="$3"
    local data="computed_state_root=$state_root\\nreceipt_hash=receipt-$state_root\\nmatch_source_root=true\\nsignature=signature-$worker_id\\n"
    keeper_create_checked "/housekeeper/v1/storage_integrity/attestations/$statement_id/$worker_id" "$data"
    wait_for_keeper_path "replay attestation $worker_id" "/housekeeper/v1/storage_integrity/attestations/$statement_id/$worker_id" 10
}

stop_replication_queues() {
    for port in "$@"; do
        ch_query "$port" "SYSTEM STOP REPLICATION QUEUES hg_unsafe.\`realbin.t_a\`"
    done
}

corrupt_unsafe_on_ch3() {
    log "corrupting unsafe table on ch3 only"
    stop_replication_queues "${CH_TCP_PORTS[0]}" "${CH_TCP_PORTS[1]}"
    ch_query "${CH_TCP_PORTS[2]}" "INSERT INTO hg_unsafe.\`realbin.t_a\` VALUES (unhex('eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee'), 999, 'evil-unsafe', toDateTime('2026-06-29 00:00:00'), 42)"
    wait_for_query_value "unsafe count remains honest on ch1" "${CH_TCP_PORTS[0]}" "SELECT count() FROM hg_unsafe.\`realbin.t_a\`" "2" 15
    wait_for_query_value "unsafe count remains honest on ch2" "${CH_TCP_PORTS[1]}" "SELECT count() FROM hg_unsafe.\`realbin.t_a\`" "2" 15
    wait_for_query_value "unsafe count is corrupted on ch3" "${CH_TCP_PORTS[2]}" "SELECT count() FROM hg_unsafe.\`realbin.t_a\`" "3" 15
}

corrupt_safe_on_ch3() {
    log "corrupting safe table on ch3 only"
    ch_query "${CH_TCP_PORTS[2]}" "INSERT INTO hg_safe.\`realbin.t\` VALUES (unhex('ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff'), 999, 'evil-safe', toDateTime('2026-06-29 00:00:00'), 42)"
    wait_for_query_value "safe count remains honest on ch1" "${CH_TCP_PORTS[0]}" "SELECT count() FROM hg_safe.\`realbin.t\`" "2" 15
    wait_for_query_value "safe count remains honest on ch2" "${CH_TCP_PORTS[1]}" "SELECT count() FROM hg_safe.\`realbin.t\`" "2" 15
    wait_for_query_value "safe count is corrupted on ch3" "${CH_TCP_PORTS[2]}" "SELECT count() FROM hg_safe.\`realbin.t\`" "3" 15
}

run_replay_minority_e2e() {
    start_storage_integrity_topology
    send_storage_integrity_insert

    local statement_id
    statement_id="$(adversarial_statement_id)"
    log "E2E replay-minority statement_id=$statement_id"

    submit_replay_attestation "$statement_id" r1 state-a
    submit_replay_attestation "$statement_id" r2 state-a
    wait_for_keeper_data_contains "replay decision" "/housekeeper/v1/storage_integrity/decisions/$statement_id" "replay_quorum_met=true" 30
    wait_for_keeper_data_contains "replay decision" "/housekeeper/v1/storage_integrity/decisions/$statement_id" "replay_result_hash=state-a" 30
    keeper_ensure_path "/housekeeper/v1/storage_integrity/replay_quarantine"
    wait_for_keeper_path "replay quarantine root" "/housekeeper/v1/storage_integrity/replay_quarantine" 10
    submit_replay_attestation "$statement_id" r3 state-b
    wait_for_keeper_data_contains "replay quarantine r3" "/housekeeper/v1/storage_integrity/replay_quarantine/r3" "reason=replay_minority_mismatch" 30
    wait_for_keeper_data_contains "replay quarantine r3" "/housekeeper/v1/storage_integrity/replay_quarantine/r3" "majority_hash=state-a" 30
    wait_for_keeper_data_contains "replay quarantine r3" "/housekeeper/v1/storage_integrity/replay_quarantine/r3" "reported_hash=state-b" 30
    log "E2E PASS: replay minority quarantines r3"
}

run_unsafe_mismatch_e2e() {
    start_storage_integrity_topology
    send_storage_integrity_insert

    local statement_id
    statement_id="$(adversarial_statement_id)"
    log "E2E unsafe-mismatch statement_id=$statement_id"

    wait_for_keeper_children_contains "keeper replay attestations" "/housekeeper/v1/storage_integrity/attestations/$statement_id" 90 r1 r2 r3
    wait_for_keeper_data_contains "keeper replay decision" "/housekeeper/v1/storage_integrity/decisions/$statement_id" "replay_quorum_met=true" 90

    corrupt_unsafe_on_ch3
    configure_server_housegate_workers false true false false false false
    restart_server_housegates

    wait_for_keeper_children_contains "unsafe participant results" "/housekeeper/v1/storage_integrity/unsafe_results/$statement_id" 90 r1 r2 r3
    submit_mock_finality "$statement_id"
    wait_for_keeper_data_contains "unsafe mismatch decision" "/housekeeper/v1/storage_integrity/decisions/$statement_id" "unsafe_validated=false" 30
    wait_for_keeper_data_contains "unsafe mismatch decision" "/housekeeper/v1/storage_integrity/decisions/$statement_id" "finalized=true" 30
    wait_for_keeper_data_contains "unsafe mismatch decision" "/housekeeper/v1/storage_integrity/decisions/$statement_id" "promotion_ready=false" 30
    assert_keeper_path_absent_after "unsafe mismatch promotion task" "/housekeeper/v1/storage_integrity/promotions/$statement_id" 5
    log "E2E PASS: unsafe replica mismatch blocks promotion"
}

run_safe_audit_minority_e2e() {
    start_storage_integrity_topology
    send_storage_integrity_insert

    local statement_id audit_id
    statement_id="$(adversarial_statement_id)"
    audit_id="audit-$statement_id"
    log "E2E safe-audit-minority statement_id=$statement_id"

    wait_for_keeper_children_contains "keeper replay attestations" "/housekeeper/v1/storage_integrity/attestations/$statement_id" 90 r1 r2 r3
    wait_for_keeper_data_contains "keeper replay decision" "/housekeeper/v1/storage_integrity/decisions/$statement_id" "replay_quorum_met=true" 90
    wait_for_keeper_children "keeper unsafe_results" "/housekeeper/v1/storage_integrity/unsafe_results" 90 >/dev/null || die "no unsafe result ledger found"
    submit_mock_finality "$statement_id"
    for port in "${CH_TCP_PORTS[@]}"; do
        wait_for_query_value "safe count before corruption on ch:$port" "$port" "SELECT count() FROM hg_safe.\`realbin.t\`" "2" 180
    done
    wait_for_keeper_children_contains "keeper promotion results" "/housekeeper/v1/storage_integrity/promotion_results/$statement_id" 180 r1 r2 r3
    wait_for_keeper_path "keeper safeAudit task" "/housekeeper/v1/safe_audits/tasks/$audit_id" 180

    corrupt_safe_on_ch3
    configure_server_housegate_workers false false false false true false
    restart_server_housegates

    wait_for_keeper_children_contains "keeper safeAudit votes" "/housekeeper/v1/safe_audits/votes/$audit_id" 90 r1 r2 r3
    wait_for_keeper_data_contains "safeAudit decision" "/housekeeper/v1/safe_audits/decisions/$audit_id" "status=majority" 90
    wait_for_keeper_data_contains "safeAudit decision" "/housekeeper/v1/safe_audits/decisions/$audit_id" "majority_count=2" 90
    wait_for_keeper_data_contains "safeAudit decision" "/housekeeper/v1/safe_audits/decisions/$audit_id" "minority_replicas=r3" 90
    wait_for_keeper_data_contains "safeAudit quarantine r3" "/housekeeper/v1/safe_audits/quarantine/$audit_id/r3" "reason=safe_audit_minority" 90
    wait_for_keeper_data_contains "safeAudit quarantine r3" "/housekeeper/v1/safe_audits/quarantine/$audit_id/r3" "replica_id=r3" 90
    log "E2E PASS: safeAudit minority quarantines r3"
}

case "$SCENARIO" in
    replay-minority)
        run_replay_minority_e2e
        ;;
    unsafe-mismatch)
        run_unsafe_mismatch_e2e
        ;;
    safe-audit-minority)
        run_safe_audit_minority_e2e
        ;;
esac
