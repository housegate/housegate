#!/usr/bin/env bash
set -Eeuo pipefail

# Prepared harness only. Actual execution requires a later explicit root authorization.
EXPECTED_COMMIT=ab9ec0a1c27e257f7d96be1e3011f6e5f274a609
EXPECTED_TREE=3219ed991de31ada4e20ebd720aa192b15c04ca6
EXPECTED_BRANCH=urwt/codex/issue-153
JOB_ROOT=${CI2_JOB_ROOT:?missing admitted job root}
BUNDLE="$JOB_ROOT/input/source.bundle"
BUNDLE_SHA=$(/usr/bin/python3 -c 'import json,sys; print(json.loads(open(sys.argv[1]).read(1048577))["bundle_sha256"])' "$JOB_ROOT/input/prepared.json")
RUNTIME_DIR=${CI2_V16_RUNTIME_DIR:?missing verified runtime directory}
RUNTIME_MANIFEST="$RUNTIME_DIR/ci2-runtime-v16.sha256"
CONTAINER_RUNNER="$RUNTIME_DIR/run-linux-qualification-v16-container.sh"
LOGGER_HELPER="$RUNTIME_DIR/ci2-bounded-log-v16.py"
WATCHDOG_HELPER="$RUNTIME_DIR/ci2-watchdog-v16.py"
TOOLCHAIN_HELPER="$RUNTIME_DIR/ci2-toolchain-evidence-v16.py"
TRANSFER_HELPER="$RUNTIME_DIR/ci2-transfer-v16.py"
STALL_HELPER="$RUNTIME_DIR/ci2-bazel-stall-v16.py"
HOMEBREW_HELPER="$RUNTIME_DIR/ci2-homebrew-isolation-v16.py"
TERMINATOR_HELPER="$RUNTIME_DIR/ci2-terminate-owned-v16.py"
OUTPUT_BOUND_HELPER="$RUNTIME_DIR/ci2-enforce-output-bound-v16.py"
DIAGNOSTICS_HELPER="$RUNTIME_DIR/ci2-host-diagnostics-v16.py"
PHASE_OWNER_HELPER="$RUNTIME_DIR/ci2-phase-owner-v16.py"
IMAGE='gcr.io/bazel-public/bazel@sha256:4476bca3b2d6f49f30f4ab4dae5d11224db1a484cab2120b054fe6c2de5ccd12'
IMAGE_ID=sha256:88d299f855cf04a627b9e37ef57e285f6dab29eea93ac9cdad115cb6fdce3644
NATIVE_BOOTSTRAP="$RUNTIME_DIR/run-linux-qualification-native-v1.sh"
DOCKER_CONFIG_FIXED="$JOB_ROOT/docker-config"
DOCKER_SOCKET=unix:///var/run/docker.sock
WORK_SECONDS=3060
TOTAL_SECONDS=3300
CONTAINER_EVIDENCE_LIMIT=524288000
HOST_OUTPUT_LIMIT=536870912
LAYER_LIMIT=536870912

[[ "${CI2_V16_WORKER:-0}" == 1 ]]
[[ "${CI2_LINUX_QUALIFICATION_AUTHORIZED:-}" == "$EXPECTED_COMMIT" ]]
[[ "${CI2_V16_MANIFEST_SHA256:-}" =~ ^[0-9a-f]{64}$ ]]
if [[ ! "${CI2_RUN_ID:-}" =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,47}$ ]]; then
  echo "CI2_RUN_ID must match [A-Za-z0-9][A-Za-z0-9._-]{0,47}" >&2
  exit 64
fi

START_EPOCH=${CI2_V16_STARTED_EPOCH:?missing supervisor start time}
WORK_DEADLINE=$((START_EPOCH + WORK_SECONDS))
CLEANUP_DEADLINE=$((START_EPOCH + TOTAL_SECONDS))
CONTAINER="ci2-native-ab9-${CI2_RUN_ID}"
OUTPUT_DIR=${CI2_V16_OUTPUT_DIR:?missing atomically owned output directory}
OWNER_TOKEN=${CI2_OWNER_TOKEN:?missing output owner token}
OUTPUT_OWNED=1
CONTAINER_OWNED=0
CANDIDATE_ID=''
CONTAINER_ID=''
SUCCESS=0
RECOVERING=0
FINALIZER_SELECTED=0
CURRENT_PHASE=preflight
PHASE_HANDOFF_REQUIRED=0
PHASE_OBSERVED_RC=0
DOCKER_BASE=(env -i PATH=/usr/bin:/bin HOME="$JOB_ROOT/input" DOCKER_CONFIG="$DOCKER_CONFIG_FIXED" /usr/bin/docker --host "$DOCKER_SOCKET")
manifest_sha_for() { awk -v n="$1" '$2==n {print $1}' "$RUNTIME_MANIFEST"; }
RUNNER_SHA=$(manifest_sha_for run-linux-qualification-v16-container.sh)
LOGGER_SHA=$(manifest_sha_for ci2-bounded-log-v16.py)
TOOLCHAIN_SHA=$(manifest_sha_for ci2-toolchain-evidence-v16.py)
TRANSFER_SHA=$(manifest_sha_for ci2-transfer-v16.py)
STALL_SHA=$(manifest_sha_for ci2-bazel-stall-v16.py)
HOMEBREW_SHA=$(manifest_sha_for ci2-homebrew-isolation-v16.py)
TERMINATOR_SHA=$(manifest_sha_for ci2-terminate-owned-v16.py)
OUTPUT_BOUND_SHA=$(manifest_sha_for ci2-enforce-output-bound-v16.py)
DIAGNOSTICS_SHA=$(manifest_sha_for ci2-host-diagnostics-v16.py)
PHASE_OWNER_SHA=$(manifest_sha_for ci2-phase-owner-v16.py)

assert_output_owned() {
  local recorded_owner
  [[ -d "$OUTPUT_DIR" && ! -L "$OUTPUT_DIR" && -f "$OUTPUT_DIR/.ci2-owner" && ! -L "$OUTPUT_DIR/.ci2-owner" ]] || return 1
  recorded_owner=$(cat "$OUTPUT_DIR/.ci2-owner") || return 1
  [[ "$recorded_owner" == "$OWNER_TOKEN" ]] || return 1
}

assert_owned_subdir() {
  local path=$1 recorded_owner
  assert_output_owned || return 1
  [[ -d "$path" && ! -L "$path" && -f "$path/.ci2-owner" && ! -L "$path/.ci2-owner" ]] || return 1
  recorded_owner=$(cat "$path/.ci2-owner") || return 1
  [[ "$recorded_owner" == "$OWNER_TOKEN" ]] || return 1
}

verify_runtime() {
  local recorded_owner manifest_digest
  assert_output_owned || return 1
  [[ "$RUNTIME_DIR" == "$OUTPUT_DIR/runtime" && -d "$RUNTIME_DIR" && ! -L "$RUNTIME_DIR" ]] || return 1
  recorded_owner=$(cat "$RUNTIME_DIR/.ci2-owner") || return 1
  [[ "$recorded_owner" == "$OWNER_TOKEN" ]] || return 1
  manifest_digest=$(sha256sum "$RUNTIME_MANIFEST" | awk '{print $1}') || return 1
  [[ "$manifest_digest" == "$CI2_V16_MANIFEST_SHA256" ]] || return 1
  (cd "$RUNTIME_DIR" && sha256sum -c ci2-runtime-v16.sha256 >/dev/null) || return 1
}

assert_output_owned
assert_owned_subdir "$OUTPUT_DIR/host"
verify_runtime
[[ "$OUTPUT_DIR" == "$JOB_ROOT/execution" ]]

remaining_until() {
  local deadline=$1 now
  now=$(date +%s)
  (( now < deadline )) || return 1
  printf '%s\n' "$((deadline - now))"
}

run_deadlined() {
  local deadline=$1 requested=$2
  shift 2
  local remain hard soft
  remain=$(remaining_until "$deadline") || return 124
  hard=$requested
  (( hard <= remain )) || hard=$remain
  (( hard > 1 )) || return 124
  if (( hard > 6 )); then soft=$((hard - 5)); else soft=1; fi
  verify_runtime || return 1
  /usr/bin/python3 "$WATCHDOG_HELPER" --soft-seconds "$soft" --hard-seconds "$hard" -- "$@"
}

run_work() { run_deadlined "$WORK_DEADLINE" "$@"; }
run_cleanup() { run_deadlined "$CLEANUP_DEADLINE" "$@"; }

docker_work() {
  local seconds=$1
  shift
  native_capture run_work "$seconds" "${DOCKER_BASE[@]}" "$@"
}

docker_cleanup() {
  local seconds=$1
  shift
  native_capture run_cleanup "$seconds" "${DOCKER_BASE[@]}" "$@"
}

native_capture() {
  local lane=$1 budget=$2 name rc
  shift 2
  # Every Docker producer uses the unchanged bounded private-pipe collector.
  name="native-${BASHPID}-${RANDOM}-${RANDOM}"
  if run_captured "$lane" "$budget" "$name" "$@"; then rc=0; else rc=$?; fi
  /usr/bin/python3 - "$OUTPUT_DIR/host/unsealed-diagnostics/$name" "$rc" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]); rc=int(sys.argv[2])
status=json.loads(p.with_suffix('.status.json').read_text())
assert status['state']=='returned' and all(not x['truncated'] for x in status['streams'].values())
sys.stdout.buffer.write(p.with_suffix('.stdout').read_bytes())
sys.stderr.buffer.write(p.with_suffix('.stderr').read_bytes())
raise SystemExit(rc)
PY
}

host_bytes() {
  /usr/bin/python3 - "$1" <<'PY'
import os, sys
total=0
for root, _, files in os.walk(sys.argv[1]):
    for name in files:
        total += os.lstat(os.path.join(root, name)).st_size
print(total)
PY
}

inspect_owned_container() {
  (( CONTAINER_OWNED == 1 )) || return 1
  local line
  line=$(docker_cleanup 10 inspect --format '{{.Id}}|{{.Name}}|{{index .Config.Labels "com.housegate.ci2.task"}}|{{index .Config.Labels "com.housegate.ci2.run"}}|{{index .Config.Labels "com.housegate.ci2.commit"}}|{{index .Config.Labels "com.housegate.ci2.owner"}}' "$CONTAINER_ID") || return 1
  [[ "$line" == "$CONTAINER_ID|/$CONTAINER|release-qualification-v16|$CI2_RUN_ID|$EXPECTED_COMMIT|$OWNER_TOKEN" ]]
}

container_running() {
  local running
  inspect_owned_container || return 1
  running=$(docker_cleanup 10 inspect --format '{{.State.Running}}' "$CONTAINER_ID") || return 1
  [[ "$running" == true ]]
}

record_container_state() {
  (( OUTPUT_OWNED == 1 )) || return 0
  assert_owned_subdir "$OUTPUT_DIR/host"
  if (( CONTAINER_OWNED == 1 )) && inspect_owned_container; then
    run_cleanup 20 /bin/bash "$NATIVE_BOOTSTRAP" identity-check || return 1
    docker_cleanup 10 inspect --size "$CONTAINER_ID" >"$OUTPUT_DIR/host/container-state.json" 2>&1 || true
    if container_running && [[ ! -e "$OUTPUT_DIR/host/terminal-cgroup.json" ]]; then
      run_cleanup 30 /bin/bash "$NATIVE_BOOTSTRAP" terminal-metrics "$CONTAINER_ID" || true
    fi
    if ! container_running; then
      {
        echo 'tmpfs_evidence_available=false'
        echo 'reason=owned container is not running; /ci2 tmpfs is unavailable and must not be recreated'
        docker_cleanup 10 inspect --format 'id={{.Id}} running={{.State.Running}} oom_killed={{.State.OOMKilled}} exit_code={{.State.ExitCode}} error={{json .State.Error}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}' "$CONTAINER_ID" 2>&1 || true
      } >"$OUTPUT_DIR/host/tmpfs-unavailable.txt"
    fi
  elif [[ -n "$CANDIDATE_ID" ]]; then
    printf 'unverified_create_return_id=%s\ncleanup_authorized=false\n' "$CANDIDATE_ID" >"$OUTPUT_DIR/host/unowned-container-candidate.txt"
  fi
}

check_layer_bound() {
  assert_owned_subdir "$OUTPUT_DIR/host"
  inspect_owned_container
  local size
  size=$(docker_work 15 inspect --size --format '{{.SizeRw}}' "$CONTAINER_ID")
  [[ "$size" =~ ^[0-9]+$ ]]
  (( size <= LAYER_LIMIT ))
  printf '%s %s\n' "$CURRENT_PHASE" "$size" >>"$OUTPUT_DIR/host/layer-sizes.txt"
}

copy_and_verify_evidence() {
  local expected_result=$1 marker result container_bytes marker_manifest copied_bytes copied_manifest meta_result total_bytes host_size transfer_digest
  [[ "$expected_result" == PASS || "$expected_result" == FAIL ]] || return 1
  assert_owned_subdir "$OUTPUT_DIR/host" || return 1
  # Never accept a pre-existing completion pathname as proof of a prior export.
  [[ ! -e "$OUTPUT_DIR/host/evidence-$expected_result-complete" ]] || return 1
  container_running || return 1
  marker=$(docker_cleanup 10 exec "$CONTAINER_ID" cat /ci2/control/EVIDENCE_READY) || return 1
  result=$(awk -F= '$1=="result" {print $2}' <<<"$marker") || return 1
  [[ "$result" == "$expected_result" ]] || return 1
  container_bytes=$(awk -F= '$1=="file_bytes" {print $2}' <<<"$marker") || return 1
  marker_manifest=$(awk -F= '$1=="manifest_sha256" {print $2}' <<<"$marker") || return 1
  [[ "$container_bytes" =~ ^(0|[1-9][0-9]{0,8})$ && "$marker_manifest" =~ ^[0-9a-f]{64}$ ]] || return 1
  [[ "$marker" == "$(printf 'result=%s\nfile_bytes=%s\nmanifest_sha256=%s' "$result" "$container_bytes" "$marker_manifest")" ]] || return 1
  (( container_bytes <= CONTAINER_EVIDENCE_LIMIT )) || return 1
  printf '%s\n' "$marker" >"$OUTPUT_DIR/host/container-evidence-ready.txt" || return 1
  docker_cleanup 10 top "$CONTAINER_ID" -eo pid,comm,args >"$OUTPUT_DIR/host/processes-before-copy.txt" || return 1
  if tail -n +2 "$OUTPUT_DIR/host/processes-before-copy.txt" | grep -E '/ci2/run-linux-qualification-v16-container\.sh (setup|homebrew|ci-build|ci-test-ffi|release-linux|release-darwin|finalize|abort-finalize)|bazel.*output_user_root=/ci2|ci2-bounded-log-v16|ci2-bazel-stall-v16|ci2-phase-owner-v16.py|ci2-terminate-owned-v16.py terminate|apt-get|git -C /ci2|jcmd .*Thread\.print' >/dev/null; then
    echo 'owned workload or logger still active before evidence copy' >&2
    return 1
  else
    local process_gate_rc=$?
    (( process_gate_rc == 1 )) || return 1
  fi
  docker_cleanup 10 exec "$CONTAINER_ID" test -f /ci2/control/WORKLOAD_COMPLETE || return 1
  if [[ "$expected_result" == PASS ]]; then
    local truncated
    truncated=$(docker_cleanup 10 exec "$CONTAINER_ID" find /ci2/evidence -name '*.truncated' -print -quit) || return 1
    [[ -z "$truncated" ]] || { echo 'truncated evidence forbids PASS' >&2; return 1; }
  fi
  if [[ -e "$OUTPUT_DIR/evidence" ]]; then
    assert_owned_subdir "$OUTPUT_DIR/evidence" || return 1
    rm -rf "$OUTPUT_DIR/evidence" || return 1
  fi
  assert_output_owned || return 1
  host_size=$(host_bytes "$OUTPUT_DIR") || return 1
  [[ "$host_size" =~ ^[0-9]{1,9}$ ]] || return 1
  # Reserve 2MiB for later diagnostics, status and teardown records.
  (( host_size + container_bytes + 2097152 <= HOST_OUTPUT_LIMIT )) || return 1
  mkdir "$OUTPUT_DIR/evidence" || return 1
  printf '%s\n' "$OWNER_TOKEN" >"$OUTPUT_DIR/evidence/.ci2-owner" || return 1
  verify_runtime || return 1
  transfer_digest=$(sha256sum "$TRANSFER_HELPER" | awk '{print $1}') || return 1
  [[ "$transfer_digest" == "$TRANSFER_SHA" ]] || return 1
  run_cleanup 90 /usr/bin/python3 "$TRANSFER_HELPER" download \
    --output-root "$OUTPUT_DIR" --owner "$OWNER_TOKEN" --container "$CONTAINER_ID" \
    --stderr "$OUTPUT_DIR/host/download-$expected_result.stderr" --destination "$OUTPUT_DIR/evidence" \
    --expected-result "$expected_result" --expected-bytes "$container_bytes" --expected-manifest "$marker_manifest" \
    -- "${DOCKER_BASE[@]}" >"$OUTPUT_DIR/host/download-$expected_result.json" || return 1
  copied_bytes=$(host_bytes "$OUTPUT_DIR/evidence/payload") || return 1
  [[ "$copied_bytes" == "$container_bytes" ]] || return 1
  copied_manifest=$(sha256sum "$OUTPUT_DIR/evidence/payload/MANIFEST.sha256" | awk '{print $1}') || return 1
  [[ "$copied_manifest" == "$marker_manifest" ]] || return 1
  (cd "$OUTPUT_DIR/evidence/payload" && sha256sum -c MANIFEST.sha256) || return 1
  meta_result=$(awk -F= '$1=="result" {print $2}' "$OUTPUT_DIR/evidence/payload/MANIFEST.meta") || return 1
  [[ "$meta_result" == "$expected_result" ]] || return 1
  total_bytes=$(host_bytes "$OUTPUT_DIR") || return 1
  [[ "$total_bytes" =~ ^[0-9]{1,9}$ ]] || return 1
  (( total_bytes <= HOST_OUTPUT_LIMIT )) || return 1
  : >"$OUTPUT_DIR/host/evidence-$expected_result-complete" || return 1
}

enforce_fail_output_bound() {
  assert_output_owned
  verify_runtime
  [[ "$(sha256sum "$OUTPUT_BOUND_HELPER" | awk '{print $1}')" == "$OUTPUT_BOUND_SHA" ]]
  /usr/bin/python3 "$OUTPUT_BOUND_HELPER" --output "$OUTPUT_DIR" --owner-token "$OWNER_TOKEN" --limit "$HOST_OUTPUT_LIMIT"
  (( $(host_bytes "$OUTPUT_DIR") <= HOST_OUTPUT_LIMIT ))
}

run_captured() {
  local lane=$1 budget=$2 name=$3
  shift 3
  verify_runtime || return 1
  [[ "$(sha256sum "$DIAGNOSTICS_HELPER" | awk '{print $1}')" == "$DIAGNOSTICS_SHA" ]] || return 1
  "$lane" "$budget" /usr/bin/python3 "$DIAGNOSTICS_HELPER" capture \
    --output-root "$OUTPUT_DIR" --owner "$OWNER_TOKEN" --name "$name" -- "$@"
}

collect_unsealed_diagnostics() {
  local name=$1 rc
  inspect_owned_container || return 1
  verify_runtime || return 1
  [[ "$(sha256sum "$DIAGNOSTICS_HELPER" | awk '{print $1}')" == "$DIAGNOSTICS_SHA" ]] || return 1
  if run_cleanup 12 /usr/bin/python3 "$DIAGNOSTICS_HELPER" snapshot \
    --output-root "$OUTPUT_DIR" --owner "$OWNER_TOKEN" --name "$name" --container "$CONTAINER_ID" \
    -- "${DOCKER_BASE[@]}"; then rc=0; else rc=$?; fi
  printf 'snapshot=%s host_exit=%s sealed=false complete_acquisition=false\n' "$name" "$rc" >>"$OUTPUT_DIR/host/diagnostic-status.txt" || return 1
  return "$rc"
}

record_operation_status() {
  local operation=$1 rc=$2
  assert_owned_subdir "$OUTPUT_DIR/host"
  printf 'operation=%s exit=%s utc=%s\n' "$operation" "$rc" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$OUTPUT_DIR/host/operation-status.txt"
}

recover() {
  local reason=$1 finalizer_rc original_deadline=$CLEANUP_DEADLINE now failure_epoch handoff_reply handoff_ok=1
  (( RECOVERING == 0 )) || return 0
  RECOVERING=1
  (( OUTPUT_OWNED == 1 )) || { echo "recovery before output ownership: $reason" >&2; return 0; }
  assert_owned_subdir "$OUTPUT_DIR/host"
  printf 'phase=%s\nreason=%s\nutc=%s\n' "$CURRENT_PHASE" "$reason" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$OUTPUT_DIR/host/failure.txt"
  # Reserve the final 60s of the unchanged absolute envelope for exact teardown.
  # Early failure also receives at most 240s of cleanup, not the unused work time.
  now=$(date +%s)
  (( CLEANUP_DEADLINE <= now + 240 )) || CLEANUP_DEADLINE=$((now + 240))
  if (( PHASE_HANDOFF_REQUIRED == 1 )); then
    # Receipt validates actual parent wait statuses plus fresh exact-group absence.
    # It is never authority to start another terminator or retry a lost handoff.
    handoff_ok=0
    if handoff_reply=$(docker_cleanup 10 exec --user 0:0 \
      --env CI2_RUN_ID="$CI2_RUN_ID" --env CI2_OWNER_TOKEN="$OWNER_TOKEN" \
      --env CI2_RUNNER_SHA="$RUNNER_SHA" --env CI2_PHASE_OWNER_SHA="$PHASE_OWNER_SHA" \
      --env CI2_TERMINATOR_SHA="$TERMINATOR_SHA" "$CONTAINER_ID" \
      /usr/bin/python3 /ci2/ci2-phase-owner-v16.py receipt "$CURRENT_PHASE" --observed-rc "$PHASE_OBSERVED_RC") &&
       [[ "$handoff_reply" =~ ^(retired|refused)\ ([0-9]{1,12})$ ]]; then
      failure_epoch=${BASH_REMATCH[2]}
      [[ "${BASH_REMATCH[1]}" != retired ]] || handoff_ok=1
      (( CLEANUP_DEADLINE <= failure_epoch + 240 )) || CLEANUP_DEADLINE=$((failure_epoch + 240))
      printf 'first_failure_epoch=%s\ncleanup_deadline=%s\n' "$failure_epoch" "$CLEANUP_DEADLINE" >"$OUTPUT_DIR/host/ordinary-handoff-time.txt"
    else
      printf '%s\n' 'ordinary handoff timing unproved; no completion evidence permitted' >>"$OUTPUT_DIR/host/failure.txt"
    fi
    if (( handoff_ok == 0 )); then
      printf '%s\n' 'ordinary handoff unproved; teardown only; no diagnostics/finalizer/export' >>"$OUTPUT_DIR/host/failure.txt"
      now=$(date +%s)
      (( CLEANUP_DEADLINE <= now + 60 )) || CLEANUP_DEADLINE=$((now + 60))
    fi
  fi
  original_deadline=$CLEANUP_DEADLINE
  CLEANUP_DEADLINE=$((original_deadline - 60))
  if (( handoff_ok == 1 )); then record_container_state; fi
  if (( handoff_ok == 1 && CONTAINER_OWNED == 1 && FINALIZER_SELECTED == 0 )) && container_running; then
    FINALIZER_SELECTED=1
    collect_unsealed_diagnostics before-recovery || true
    if run_captured run_cleanup 75 finalize-fail "${DOCKER_BASE[@]}" exec --user 0:0 \
      --env CI2_RUN_ID="$CI2_RUN_ID" --env CI2_OWNER_TOKEN="$OWNER_TOKEN" \
      --env CI2_RUNNER_SHA="$RUNNER_SHA" --env CI2_LOGGER_SHA="$LOGGER_SHA" --env CI2_TOOLCHAIN_SHA="$TOOLCHAIN_SHA" \
      --env CI2_STALL_SHA="$STALL_SHA" --env CI2_HOMEBREW_SHA="$HOMEBREW_SHA" --env CI2_TERMINATOR_SHA="$TERMINATOR_SHA" \
      "$CONTAINER_ID" /usr/bin/setsid --wait /bin/bash /ci2/run-linux-qualification-v16-container.sh abort-finalize "$reason"; then finalizer_rc=0; else finalizer_rc=$?; fi
    record_operation_status finalize-fail "$finalizer_rc"
    collect_unsealed_diagnostics after-recovery || true
    copy_and_verify_evidence FAIL || printf '%s\n' 'FAIL evidence unavailable or unverified' >>"$OUTPUT_DIR/host/failure.txt"
  fi
  CLEANUP_DEADLINE=$original_deadline
  if (( CONTAINER_OWNED == 1 )) && inspect_owned_container; then
    run_cleanup 20 /bin/bash "$NATIVE_BOOTSTRAP" identity-check || return 1
    docker_cleanup 15 stop --time 10 "$CONTAINER_ID" >/dev/null 2>&1 || true
    inspect_owned_container && docker_cleanup 15 rm -f "$CONTAINER_ID" >/dev/null 2>&1 || true
    run_cleanup 30 /bin/bash "$NATIVE_BOOTSTRAP" retirement "$CONTAINER_ID" "$CONTAINER" || true
  fi
  enforce_fail_output_bound
}

on_exit() {
  local rc=$?
  trap - EXIT INT TERM
  if (( SUCCESS == 0 )); then recover "host worker exit rc=$rc"; fi
  exit "$rc"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

for file in "$BUNDLE" "$CONTAINER_RUNNER" "$LOGGER_HELPER" "$WATCHDOG_HELPER" "$TOOLCHAIN_HELPER" "$STALL_HELPER" "$HOMEBREW_HELPER" "$TERMINATOR_HELPER" "$OUTPUT_BOUND_HELPER" "$TRANSFER_HELPER" "$DIAGNOSTICS_HELPER" "$PHASE_OWNER_HELPER"; do [[ -f "$file" ]]; done
assert_output_owned
[[ "$(sha256sum "$BUNDLE" | awk '{print $1}')" == "$BUNDLE_SHA" ]]
[[ "$(stat -c '%a' "$BUNDLE")" == 444 ]]
run_work 60 /bin/bash "$NATIVE_BOOTSTRAP" host-admit
daemon_line='native-hosted-v1; see native-admission.json'
image_line="$IMAGE_ID|linux|amd64|1065765003|ubuntu|[\"/usr/local/bin/bazel\"]|/home/ubuntu"
[[ -z "$(docker_work 15 ps -aq --filter "name=^/${CONTAINER}$")" ]]

assert_owned_subdir "$OUTPUT_DIR/host"
{
  echo "authorization_commit=$EXPECTED_COMMIT"
  echo "authorization_runtime_manifest_sha256=$CI2_V16_MANIFEST_SHA256"
  echo "run_id=$CI2_RUN_ID"
  echo "owner_token=$OWNER_TOKEN"
  echo "started_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "work_deadline_epoch=$WORK_DEADLINE"
  echo "cleanup_deadline_epoch=$CLEANUP_DEADLINE"
  echo "daemon=$daemon_line"
  echo "image=$image_line"
  echo "bundle_sha256=$BUNDLE_SHA"
  echo "container_runner_sha256=$RUNNER_SHA"
  echo "logger_helper_sha256=$LOGGER_SHA"
  echo "toolchain_helper_sha256=$TOOLCHAIN_SHA"
  echo "transfer_helper_sha256=$TRANSFER_SHA"
  echo "stall_helper_sha256=$STALL_SHA"
  echo "homebrew_helper_sha256=$HOMEBREW_SHA"
  echo "terminator_helper_sha256=$TERMINATOR_SHA"
  echo "output_bound_helper_sha256=$OUTPUT_BOUND_SHA"
  echo "watchdog_helper_sha256=$(manifest_sha_for ci2-watchdog-v16.py)"
  echo "host_worker_sha256=$(manifest_sha_for run-linux-qualification-native-v1-worker.sh)"
} >"$OUTPUT_DIR/host/preflight.txt"

CURRENT_PHASE=create
set +e
CANDIDATE_ID=$(docker_work 30 create --pull=never --name "$CONTAINER" --platform linux/amd64 --user 0:0 \
  --label com.housegate.ci2.task=release-qualification-v16 \
  --label "com.housegate.ci2.run=$CI2_RUN_ID" \
  --label "com.housegate.ci2.commit=$EXPECTED_COMMIT" \
  --label "com.housegate.ci2.owner=$OWNER_TOKEN" \
  --init --cpus 3 --memory 12g --memory-swap 12g --pids-limit 1024 --shm-size 64m \
  --tmpfs /ci2:rw,exec,nosuid,nodev,size=6g,mode=0755 \
  --tmpfs /tmp:rw,noexec,nosuid,nodev,size=128m,mode=1777 \
  --mount "type=bind,src=$BUNDLE,dst=/input/source.bundle,readonly,bind-propagation=rprivate" \
  --log-driver local --log-opt max-size=20m --log-opt max-file=2 \
  --entrypoint /usr/bin/tail "$IMAGE" -f /dev/null)
create_rc=$?
set -e
(( create_rc == 0 )) || exit "$create_rc"
[[ "$CANDIDATE_ID" =~ ^[0-9a-f]{64}$ ]]
CONTAINER_ID=$CANDIDATE_ID
owned_line=$(docker_work 15 inspect --format '{{.Id}}|{{.Name}}|{{index .Config.Labels "com.housegate.ci2.task"}}|{{index .Config.Labels "com.housegate.ci2.run"}}|{{index .Config.Labels "com.housegate.ci2.commit"}}|{{index .Config.Labels "com.housegate.ci2.owner"}}' "$CONTAINER_ID")
[[ "$owned_line" == "$CONTAINER_ID|/$CONTAINER|release-qualification-v16|$CI2_RUN_ID|$EXPECTED_COMMIT|$OWNER_TOKEN" ]]
CONTAINER_OWNED=1
run_work 45 /bin/bash "$NATIVE_BOOTSTRAP" container-admit "$CONTAINER_ID"
docker_work 20 start "$CONTAINER_ID" >/dev/null
run_work 30 /bin/bash "$NATIVE_BOOTSTRAP" live-init-admit "$CONTAINER_ID"
# docker cp addresses the underlying layer, not the live /ci2 tmpfs. Install
# the exact fixed helper set through exec stdin and verify every mounted byte.
inspect_owned_container
verify_runtime
[[ "$(sha256sum "$TRANSFER_HELPER" | awk '{print $1}')" == "$TRANSFER_SHA" ]]
run_work 60 /usr/bin/python3 "$TRANSFER_HELPER" upload \
  --output-root "$OUTPUT_DIR" --owner "$OWNER_TOKEN" --container "$CONTAINER_ID" \
  --stderr "$OUTPUT_DIR/host/upload.stderr" --runtime-dir "$RUNTIME_DIR" \
  -- "${DOCKER_BASE[@]}" >"$OUTPUT_DIR/host/upload.json"

run_phase() {
  local name=$1 budget=$2 command=$3 rc phase_deadline
  CURRENT_PHASE=$name
  run_work 20 /bin/bash "$NATIVE_BOOTSTRAP" phase-disk "before-$name"
  PHASE_HANDOFF_REQUIRED=1
  PHASE_OBSERVED_RC=0
  phase_deadline=$(date +%s)
  phase_deadline=$((phase_deadline + budget))
  (( phase_deadline <= WORK_DEADLINE )) || phase_deadline=$WORK_DEADLINE
  set +e
  run_captured run_work "$budget" "phase-$name" "${DOCKER_BASE[@]}" exec --user 0:0 \
    --env CI2_EXPECTED_COMMIT="$EXPECTED_COMMIT" --env CI2_EXPECTED_TREE="$EXPECTED_TREE" \
    --env CI2_RUN_ID="$CI2_RUN_ID" --env CI2_OWNER_TOKEN="$OWNER_TOKEN" \
    --env CI2_RUNNER_SHA="$RUNNER_SHA" --env CI2_LOGGER_SHA="$LOGGER_SHA" --env CI2_TOOLCHAIN_SHA="$TOOLCHAIN_SHA" \
    --env CI2_STALL_SHA="$STALL_SHA" --env CI2_HOMEBREW_SHA="$HOMEBREW_SHA" --env CI2_TERMINATOR_SHA="$TERMINATOR_SHA" \
    --env CI2_PHASE_OWNER_SHA="$PHASE_OWNER_SHA" --env CI2_PHASE_DEADLINE="$phase_deadline" \
    "$CONTAINER_ID" /usr/bin/setsid --wait /usr/bin/python3 /ci2/ci2-phase-owner-v16.py run "$command"
  rc=$?
  set -e
  PHASE_OBSERVED_RC=$rc
  record_operation_status "$name" "$rc"
  if (( rc != 0 )); then echo "phase failed: $name rc=$rc" >&2; return "$rc"; fi
  PHASE_HANDOFF_REQUIRED=0
  check_layer_bound
  run_work 20 /bin/bash "$NATIVE_BOOTSTRAP" phase-disk "after-$name"
}

# Phase maxima total 47 minutes; preflight, six layer checks, runtime revalidation,
# and transitions share the remaining four minutes of the 51-minute work window.
run_phase setup 300 setup
run_phase homebrew 300 homebrew
run_phase ci-build 540 ci-build
run_phase ci-test-ffi 840 ci-test-ffi
run_phase release-linux 420 release-linux
run_phase release-darwin 420 release-darwin

CURRENT_PHASE=finalize-copy
FINALIZER_SELECTED=1
if run_captured run_cleanup 60 finalize-pass "${DOCKER_BASE[@]}" exec --user 0:0 --env CI2_RUN_ID="$CI2_RUN_ID" --env CI2_OWNER_TOKEN="$OWNER_TOKEN" \
  --env CI2_RUNNER_SHA="$RUNNER_SHA" --env CI2_LOGGER_SHA="$LOGGER_SHA" --env CI2_TOOLCHAIN_SHA="$TOOLCHAIN_SHA" \
  --env CI2_STALL_SHA="$STALL_SHA" --env CI2_HOMEBREW_SHA="$HOMEBREW_SHA" --env CI2_TERMINATOR_SHA="$TERMINATOR_SHA" \
  "$CONTAINER_ID" /usr/bin/setsid --wait /bin/bash /ci2/run-linux-qualification-v16-container.sh finalize; then finalizer_rc=0; else finalizer_rc=$?; fi
record_operation_status finalize-pass "$finalizer_rc"
(( finalizer_rc == 0 )) || exit "$finalizer_rc"
copy_and_verify_evidence PASS
assert_owned_subdir "$OUTPUT_DIR/host"
printf 'completed_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >"$OUTPUT_DIR/host/success.txt"
assert_output_owned
(( $(host_bytes "$OUTPUT_DIR") <= HOST_OUTPUT_LIMIT ))
inspect_owned_container
run_cleanup 30 /bin/bash "$NATIVE_BOOTSTRAP" terminal-metrics "$CONTAINER_ID"
run_cleanup 20 /bin/bash "$NATIVE_BOOTSTRAP" identity-check
docker_cleanup 15 stop --time 10 "$CONTAINER_ID" >/dev/null
inspect_owned_container
docker_cleanup 15 rm "$CONTAINER_ID" >/dev/null
run_cleanup 30 /bin/bash "$NATIVE_BOOTSTRAP" retirement "$CONTAINER_ID" "$CONTAINER"
# Host sealing is deferred until the outer owner has returned and joined.
SUCCESS=1
trap - EXIT INT TERM
echo "CI2 Linux qualification PASS: $OUTPUT_DIR"
