#!/usr/bin/env bash
set -Eeuo pipefail

EXPECTED_COMMIT=${CI2_EXPECTED_COMMIT:-ab9ec0a1c27e257f7d96be1e3011f6e5f274a609}
EXPECTED_TREE=${CI2_EXPECTED_TREE:-3219ed991de31ada4e20ebd720aa192b15c04ca6}
RUN_ID=${CI2_RUN_ID:?missing run id}
OWNER_TOKEN=${CI2_OWNER_TOKEN:?missing owner token}
RUNNER_SHA=${CI2_RUNNER_SHA:?missing runner sha}
LOGGER_SHA=${CI2_LOGGER_SHA:?missing logger sha}
TOOLCHAIN_SHA=${CI2_TOOLCHAIN_SHA:?missing toolchain sha}
STALL_SHA=${CI2_STALL_SHA:?missing stall helper sha}
HOMEBREW_SHA=${CI2_HOMEBREW_SHA:?missing homebrew helper sha}
TERMINATOR_SHA=${CI2_TERMINATOR_SHA:?missing terminator helper sha}
BASE_COMMIT=50f0a6d9f95aa50e7b19af7efbb836a1b16b9c6f
SRC=/ci2/src
EVIDENCE=/ci2/evidence
CONTROL=/ci2/control
CACHE=/ci2/cache
DISK_CACHE=/ci2/cache/disk
LOGGER=/ci2/ci2-bounded-log-v16.py
TOOLCHAIN_CHECK=/ci2/ci2-toolchain-evidence-v16.py
STALL_HELPER=/ci2/ci2-bazel-stall-v16.py
HOMEBREW_CHECK=/ci2/ci2-homebrew-isolation-v16.py
TERMINATOR=/ci2/ci2-terminate-owned-v16.py
LOG_LIMIT=33554432
AQUERY_LIMIT=67108864
PROFILE_LIMIT=16777216
EVIDENCE_LIMIT=524288000
INVALID_CC=/ci2/forbidden-host-cc
BAZEL_BINARY_SHA=a667454f3f4f8878df8199136b82c199f6ada8477b337fae3b1ef854f01e4e2f

# The build account shares GID 0; root control/evidence must never be group-writable.
umask 022
mkdir -p "$CONTROL"
chmod 0700 "$CONTROL"

validate_ubuntu_identity() {
  local entry account password uid gid gecos home shell effective
  entry=$(getent passwd ubuntu) || { echo 'CI2_ACCOUNT_IDENTITY_REFUSED: ubuntu absent' >&2; return 78; }
  IFS=: read -r account password uid gid gecos home shell <<<"$entry"
  if [[ "$account" != ubuntu || "$uid" != 1000 || "$gid" != 0 || "$home" != /home/ubuntu ]] ||
     [[ "$(id -u ubuntu)" != 1000 || "$(id -g ubuntu)" != 0 ]]; then
    echo 'CI2_ACCOUNT_IDENTITY_REFUSED: expected ubuntu uid=1000 gid=0 home=/home/ubuntu' >&2
    return 78
  fi
  effective=$(runuser -u ubuntu -- /bin/bash -c 'printf "%s:%s" "$(id -u)" "$(id -g)"') || {
    echo 'CI2_ACCOUNT_IDENTITY_REFUSED: cannot inspect effective build IDs' >&2; return 78;
  }
  [[ "$effective" == 1000:0 ]] || { echo 'CI2_ACCOUNT_IDENTITY_REFUSED: effective build IDs differ from 1000:0' >&2; return 78; }
  printf 'account=ubuntu uid=1000 gid=0 home=/home/ubuntu effective=%s build_home=/ci2/home\n' "$effective" >>"$EVIDENCE/account-identity.txt"
}

as_ubuntu() {
  validate_ubuntu_identity || return "$?"
  runuser -u ubuntu -- env HOME=/ci2/home XDG_CACHE_HOME=/ci2/cache/xdg \
    TEST_TMPDIR=/ci2/cache/output-root TMPDIR=/ci2/tmp CC="$INVALID_CC" \
    PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin /bin/bash -c '
      if [[ "$(id -u)" != 1000 || "$(id -g)" != 0 ]]; then
        echo "CI2_ACCOUNT_IDENTITY_REFUSED: command effective IDs differ from 1000:0" >&2
        exit 78
      fi
      exec "$@"
    ' ci2-build "$@"
}

bazel_u() {
  local command=$1
  shift
  as_ubuntu /bin/bash -c 'cd /ci2/src && exec bazel --output_user_root=/ci2/cache/output-root "$@"' \
    bash "$command" --disk_cache=/ci2/cache/disk --experimental_disk_cache_gc_max_size=1G "$@"
}

write_status() {
  local path=$1 producer=$2 logger_out=$3 logger_err=${4:-none}
  printf 'producer_exit=%s\nstdout_logger_exit=%s\nstderr_logger_exit=%s\n' \
    "$producer" "$logger_out" "$logger_err" >"$path"
}

run_bounded() {
  local log=$1
  shift
  mkdir -p "$(dirname "$log")"
  set +e
  "$@" 2>&1 | /usr/bin/python3 "$LOGGER" "$log" "$LOG_LIMIT"
  local statuses=("${PIPESTATUS[@]}")
  set -e
  write_status "$log.exit" "${statuses[0]}" "${statuses[1]}"
  (( statuses[1] == 0 )) || return "${statuses[1]}"
  (( statuses[0] == 0 )) || return "${statuses[0]}"
}

run_split_bounded() {
  local stdout_path=$1 stdout_limit=$2 stderr_path=$3 stderr_limit=$4
  shift 4
  local fifo_dir fifo_out fifo_err out_pid err_pid producer_rc out_rc err_rc
  mkdir -p "$(dirname "$stdout_path")" "$(dirname "$stderr_path")"
  fifo_dir=$(mktemp -d /ci2/tmp/ci2-pipes.XXXXXX)
  fifo_out="$fifo_dir/stdout"
  fifo_err="$fifo_dir/stderr"
  mkfifo "$fifo_out" "$fifo_err"
  /usr/bin/python3 "$LOGGER" "$stdout_path" "$stdout_limit" <"$fifo_out" & out_pid=$!
  /usr/bin/python3 "$LOGGER" "$stderr_path" "$stderr_limit" <"$fifo_err" & err_pid=$!
  set +e
  "$@" >"$fifo_out" 2>"$fifo_err"
  producer_rc=$?
  wait "$out_pid"; out_rc=$?
  wait "$err_pid"; err_rc=$?
  set -e
  rm -rf "$fifo_dir"
  write_status "$stdout_path.exit" "$producer_rc" "$out_rc" "$err_rc"
  (( out_rc == 0 )) || return "$out_rc"
  (( err_rc == 0 )) || return "$err_rc"
  (( producer_rc == 0 )) || return "$producer_rc"
}

expect_state() {
  [[ -f "$CONTROL/$1" ]] || { echo "missing state $1" >&2; exit 70; }
}

phase_starttime() {
  awk '{print $22}' "/proc/$1/stat"
}

operation_start_token() {
  local helper_digest
  helper_digest=$(sha256sum "$TERMINATOR" | awk '{print $1}') || return 74
  [[ "$helper_digest" == "$TERMINATOR_SHA" ]] || return 74
  /usr/bin/python3 "$TERMINATOR" start-token "$1" || return "$?"
}
begin_operation() {
  local phase=$1 pid=$BASHPID pgid start runner_sha tmp
  [[ "$phase" =~ ^(setup|homebrew|ci-build|ci-test-ffi|release-linux|release-darwin|finalize-pass|finalize-fail)$ ]]
  pgid=$(ps -o pgid= -p "$pid" | tr -d ' ')
  [[ "$pgid" == "$pid" ]] || { echo "operation is not its own process group" >&2; return 71; }
  start=$(operation_start_token "$pid")
  runner_sha=$(sha256sum /ci2/run-linux-qualification-v16-container.sh | awk '{print $1}')
  [[ "$runner_sha" == "$RUNNER_SHA" ]]
  tmp="$CONTROL/phase.identity.$pid.tmp"
  {
    echo "run_id=$RUN_ID"
    echo "owner_token=$OWNER_TOKEN"
    echo "phase=$phase"
    echo "pid=$pid"
    echo "pgid=$pgid"
    echo "starttime=$start"
    echo "runner_sha256=$runner_sha"
  } >"$tmp"
  cp "$tmp" "$CONTROL/phase.last-identity.$pid.tmp"
  mv "$CONTROL/phase.last-identity.$pid.tmp" "$CONTROL/phase.last-identity"
  mv "$tmp" "$CONTROL/phase.identity"
  trap 'operation_exit $?' EXIT
  trap 'exit 143' TERM INT
}

begin_phase() { begin_operation "$1"; }

operation_exit() {
  local rc=$1 failed_epoch phase
  if (( rc != 0 )); then ci_note "${CI_BUILD_ACTIVE:-unknown}" failure "$rc"; fi
  # One failure notification only. TERM from the owned terminator must not recurse.
  trap - EXIT TERM INT
  failed_epoch=$(date +%s)
  phase=$(awk -F= '$1=="phase" {print $2}' "$CONTROL/phase.identity" 2>/dev/null || echo unknown)
  if (( rc != 0 )); then
    mkdir -p "$EVIDENCE"
    printf 'phase=%s rc=%s utc=%s\n' "$(awk -F= '$1=="phase" {print $2}' "$CONTROL/phase.identity" 2>/dev/null || echo unknown)" "$rc" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >>"$EVIDENCE/phase-aborts.txt" || true
  fi
  if (( rc != 0 )) && [[ "$phase" =~ ^(setup|homebrew|ci-build|ci-test-ffi|release-linux|release-darwin)$ ]] &&
     [[ "${CI2_OWNER_NOTIFY_FD:-}" =~ ^[0-9]+$ && "${CI2_OWNER_PERMIT_FD:-}" =~ ^[0-9]+$ ]]; then
    printf '%s %s %s\n' "$BASHPID" "$rc" "$failed_epoch" >&"$CI2_OWNER_NOTIFY_FD" || exit 74
    # The synchronous owner is outside this PGID and is already reading. It
    # starts the strict terminator before waiting for Docker exec to return.
    # No further permit is sent; only exact-owned termination may release us.
    IFS= read -r unused <&"$CI2_OWNER_PERMIT_FD"
    exit 74
  fi
  exit "$rc"
}

end_phase() {
  local marker=$1
  trap - EXIT TERM INT
  rm -f "$CONTROL/phase.identity"
  : >"$CONTROL/$marker"
}

start_finalizer() {
  local operation=$1
  [[ "$operation" =~ ^finalize-(pass|fail)$ ]]
  mkdir "$CONTROL/finalizer.lock" || { echo 'another finalizer owns the lock' >&2; return 72; }
  {
    echo "run_id=$RUN_ID"
    echo "owner_token=$OWNER_TOKEN"
    echo "operation=$operation"
    echo "runner_sha256=$RUNNER_SHA"
  } >"$CONTROL/finalizer.lock/identity"
  if ! begin_operation "$operation"; then
    rm -rf "$CONTROL/finalizer.lock"
    return 72
  fi
  cp "$CONTROL/phase.identity" "$CONTROL/finalizer.lock/identity"
}

finish_finalizer() {
  local operation=$1 result=$2
  assert_current_finalizer_identity "$operation"
  validate_finalizer_lock "$operation"
  trap - EXIT TERM INT
  rm -f "$CONTROL/phase.identity"
  printf 'result=%s\n' "$result" >"$CONTROL/finalizer.complete"
  rm -rf "$CONTROL/finalizer.lock"
}

record_usage() {
  local label=$1
  {
    echo "label=$label"
    date -u +utc=%Y-%m-%dT%H:%M:%SZ
    df -Pk /ci2 /tmp
    du -sk "$CACHE" "$EVIDENCE" 2>/dev/null || true
  } >>"$EVIDENCE/usage.log"
}

evidence_bytes() {
  /usr/bin/python3 - "$EVIDENCE" <<'PY'
import os,sys
total=0
for root,_,files in os.walk(sys.argv[1]):
    for name in files: total += os.lstat(os.path.join(root,name)).st_size
print(total)
PY
}

copy_profile_if_bounded() {
  local source=$1 destination=$2 note=$3 size
  [[ -f "$source" ]] || return 0
  size=$(stat -c %s "$source")
  if (( size <= PROFILE_LIMIT )); then
    cp -f "$source" "$destination"
  else
    printf '%s profile_bytes=%s limit=%s\n' "$note" "$size" "$PROFILE_LIMIT" >>"$EVIDENCE/diagnostics/profile-omitted.txt"
  fi
}

capture_homebrew_stall() {
  local server_pid=$1 server_start=$2
  /usr/bin/python3 "$STALL_HELPER" --pid "$server_pid" --starttime "$server_start" \
    --expected-cmd-fragment /ci2/cache/output-root --delay-seconds 240 \
    --output-dir "$EVIDENCE/diagnostics" --profile "$CACHE/homebrew.profile.gz" \
    --log-limit "$LOG_LIMIT" --profile-limit "$PROFILE_LIMIT"
}

collect_toolchain_evidence() {
  local mode=$1 prefix=$2
  shift 2
  local flags=("$@")
  local resolution="$EVIDENCE/toolchains/$prefix-resolution.log" exec_root
  local aquery="$EVIDENCE/toolchains/$prefix-aquery.json"
  local cquery="$EVIDENCE/toolchains/$prefix-cquery.json"
  local invocation="$EVIDENCE/toolchains/$prefix-invocation.json"
  ci_note toolchain-setup active
  mkdir -p "$EVIDENCE/toolchains"
  ci_note toolchain-setup success
  ci_note toolchain-invocation active
  # Retain exact analysis argv; the verifier requires the same flags on both dumps.
  /usr/bin/python3 - "$invocation" "${flags[@]}" <<'PYARGS'
import json, sys
flags = sys.argv[2:]
value = dict(target="//cmd:housegate", flags=flags,
             aquery=["aquery", *flags, "--include_commandline", "--output=jsonproto", "deps(//cmd:housegate)"],
             cquery=["cquery", *flags, "--output=jsonproto", "--transitions=lite", "--proto:include_configurations", "deps(//cmd:housegate)"])
with open(sys.argv[1], "w") as f:
    json.dump(value, f, indent=2)
    f.write("\n")
PYARGS
  ci_note toolchain-invocation success
  ci_note resolution active
  run_bounded "$resolution" bazel_u build "${flags[@]}" --nobuild --toolchain_resolution_debug='.*' //cmd:housegate
  ci_note resolution success
  ci_note aquery active
  run_split_bounded "$aquery" "$AQUERY_LIMIT" "$EVIDENCE/toolchains/$prefix-aquery.stderr" "$LOG_LIMIT" \
    bazel_u aquery "${flags[@]}" --include_commandline --output=jsonproto 'deps(//cmd:housegate)'
  ci_note aquery success
  ci_note cquery active
  run_split_bounded "$cquery" "$AQUERY_LIMIT" "$EVIDENCE/toolchains/$prefix-cquery.stderr" "$LOG_LIMIT" \
    bazel_u cquery "${flags[@]}" --output=jsonproto --transitions=lite --proto:include_configurations 'deps(//cmd:housegate)'
  ci_note cquery success
  ci_note execution-root active
  if [[ "$mode" == ci-linux ]]; then
    exec_root=$(bazel_u info execution_root 2>"$EVIDENCE/toolchains/$prefix-execution-root.stderr")
  else
    exec_root=$(bazel_u info execution_root)
  fi
  ci_note execution-root success
  ci_note execution-root-value active
  [[ "$exec_root" == /ci2/* && -d "$exec_root" ]]
  ci_note execution-root-value success
  ci_note verifier active
  if [[ "$mode" == ci-linux ]]; then
    /usr/bin/python3 "$TOOLCHAIN_CHECK" --mode "$mode" --resolution "$resolution" --aquery "$aquery" --cquery "$cquery" \
      --invocation "$invocation" --exec-root "$exec_root" --owned-root /ci2 \
      --output "$EVIDENCE/toolchains/$prefix-verified.json" >"$EVIDENCE/toolchains/$prefix-verifier.stdout" 2>"$EVIDENCE/toolchains/$prefix-verifier.stderr"
  else
    /usr/bin/python3 "$TOOLCHAIN_CHECK" --mode "$mode" --resolution "$resolution" --aquery "$aquery" --cquery "$cquery" \
      --invocation "$invocation" --exec-root "$exec_root" --owned-root /ci2 \
      --output "$EVIDENCE/toolchains/$prefix-verified.json"
  fi
  ci_note verifier success
}

identity_field() { awk -F= -v key="$1" '$1==key {print $2}' "$CONTROL/phase.identity"; }

assert_current_finalizer_identity() {
  local expected=$1 pid=$BASHPID pgid value token runner_digest
  [[ -f "$CONTROL/phase.identity" ]] || return 74
  value=$(identity_field run_id) || return 74
  [[ "$value" == "$RUN_ID" ]] || return 74
  value=$(identity_field owner_token) || return 74
  [[ "$value" == "$OWNER_TOKEN" ]] || return 74
  value=$(identity_field phase) || return 74
  [[ "$value" == "$expected" ]] || return 74
  value=$(identity_field pid) || return 74
  [[ "$value" == "$pid" ]] || return 74
  value=$(identity_field pgid) || return 74
  [[ "$value" == "$pid" ]] || return 74
  value=$(identity_field starttime) || return 74
  token=$(operation_start_token "$pid") || return 74
  [[ "$value" == "$token" ]] || return 74
  value=$(identity_field runner_sha256) || return 74
  [[ "$value" == "$RUNNER_SHA" ]] || return 74
  runner_digest=$(sha256sum /ci2/run-linux-qualification-v16-container.sh | awk '{print $1}') || return 74
  [[ "$runner_digest" == "$RUNNER_SHA" ]] || return 74
  pgid=$(ps -o pgid= -p "$pid" | tr -d ' ') || return 74
  [[ "$pgid" == "$pid" ]] || return 74
}
bounded_shutdown() {
  # Finalizer identity is registered before this first new recovery operation.
  local finalizer=$1 helper_digest
  assert_current_finalizer_identity "$finalizer" || return 74
  helper_digest=$(sha256sum "$TERMINATOR" | awk '{print $1}') || return 74
  [[ "$helper_digest" == "$TERMINATOR_SHA" ]] || return 74
  validate_ubuntu_identity || return 74
  mkdir -p "$EVIDENCE/diagnostics" || return 74
  /usr/bin/python3 - "$TERMINATOR" "$EVIDENCE/diagnostics" <<'PYSHUTDOWN'
# Embedded by v16-edit-container.py; this file is preparation/evidence, not runtime.
import importlib.util,json,os,selectors,signal,stat,subprocess,sys,time
from pathlib import Path
helper,directory=sys.argv[1:]; directory=Path(directory)
spec=importlib.util.spec_from_file_location('owned_terminator',helper); owned=importlib.util.module_from_spec(spec);spec.loader.exec_module(owned)
started=time.monotonic(); deadline=started+24
record=dict(kind='CONTAINER_SHUTDOWN',namespace='container',soft_seconds=20,kill_seconds=22,hard_seconds=24,private_server='not_signalled; separate freeze proof required',quiescent=False,timed_out=False)
proc=None; captured={}; files={}; failure=None; snapshot={}
server_identity=directory/'homebrew-server-preidentified.txt';proc_root=Path('/proc')
def read_server_identity():
 try:
  fd=os.open(server_identity,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
  try:
   before=os.fstat(fd)
   if not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_size>16384:
    return None,'identity-invalid'
   data=os.read(fd,16385)
   after=os.fstat(fd)
  finally:os.close(fd)
  if len(data)>16384 or (before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
   return None,'identity-changed-or-truncated'
  values={}
  for raw in data.decode('utf-8','strict').splitlines():
   key,value=raw.split('=',1)
   if key in values or not key or not value:raise ValueError('duplicate-or-empty-field')
   values[key]=value
  if set(values)!= {'pid','starttime','cmdline'} or not values['pid'].isdigit() or not values['starttime'].isdigit():
   return None,'identity-malformed'
  return (int(values['pid']),values['starttime']),None
 except FileNotFoundError:return None,'identity-missing'
 except Exception as exc:return None,('identity-unreadable-'+type(exc).__name__)[:128]
known_server,known_server_error=None,'not-read'
def read_stat_identity(pid):
 try:
  fd=os.open(proc_root/str(pid)/'stat',os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
  try:data=os.read(fd,4097)
  finally:os.close(fd)
 except FileNotFoundError:return dict(status='absent')
 except Exception as exc:return dict(status='unreadable',detail=type(exc).__name__[:64])
 try:
  if len(data)>4096:return dict(status='oversized')
  text=data.decode('ascii','strict');head,rest=text.split(' (',1);rest=rest[rest.rindex(')')+1:].split()
  if int(head)!=pid or len(rest)<20 or rest[0] not in ('R','S','D','Z','T','t','X','x','K','W','P','I'):raise ValueError()
  if any(not rest[i].isdigit() or len(rest[i])>20 for i in (1,2,3,19)):raise ValueError()
  return dict(status='observed',pid=pid,process_state=rest[0],ppid=int(rest[1]),process_group=int(rest[2]),session=int(rest[3]),starttime=rest[19])
 except Exception:return dict(status='malformed')
previous_parent=None
def observe_known_server(stage):
 global previous_parent
 row=dict(stage=stage,no_signal_sent=True)
 if known_server is None:
  row.update(state='unreadable',detail=known_server_error);return row
 pid,recorded_start=known_server;row.update(pid=pid,recorded_starttime=recorded_start)
 before=read_stat_identity(pid);row['server_before']=before
 if before['status']!='observed':row['state']=before['status'];return row
 row.update(observed_starttime=before['starttime'],process_state=before['process_state'])
 if before['starttime']!=recorded_start:row['state']='reused';return row
 row['state']='zombie' if before['process_state']=='Z' else 'exact-live'
 parent=read_stat_identity(before['ppid']) if before['ppid']>0 else dict(status='no-positive-parent')
 row['parent']=parent
 after=read_stat_identity(pid);row['server_after']=after
 fields=('pid','starttime','ppid','process_group','session')
 stable=after['status']=='observed' and all(before[k]==after[k] for k in fields)
 row['relation']='stable-at-samples' if stable else 'unstable'
 row['parent_identity_continuity']='unknown-first-or-different-parent'
 if parent['status']=='observed':
  if previous_parent and previous_parent['pid']==parent['pid']:
   row['parent_identity_continuity']='same-starttime' if previous_parent['starttime']==parent['starttime'] else 'reused-between-samples'
  if stable:previous_parent=parent
 return row
def expired(*_): raise TimeoutError('container shutdown absolute deadline reached')
signal.signal(signal.SIGALRM,expired)
def bounded_server_observation(stage):
 global known_server,known_server_error
 remaining=deadline-time.monotonic()
 if remaining<=0:
  record['timed_out']=True
  return dict(stage=stage,state='not_attempted_deadline',no_signal_sent=True)
 try:
  signal.setitimer(signal.ITIMER_REAL,remaining)
  if stage=='before-shutdown':known_server,known_server_error=read_server_identity()
  row=observe_known_server(stage)
  if time.monotonic()>=deadline:
   record['timed_out']=True
   row.update(state='partial_deadline',complete_observation=False)
  return row
 except TimeoutError:
  record['timed_out']=True
  return dict(stage=stage,state='partial_deadline',no_signal_sent=True,complete_observation=False)
 finally:signal.setitimer(signal.ITIMER_REAL,0)
record['private_server_observations']=[bounded_server_observation('before-shutdown')]
def save():
 record['elapsed_seconds']=time.monotonic()-started
 (directory/'shutdown.json').write_text(json.dumps(record,sort_keys=True)+'\n')
def observe():
 members=owned.group_members(proc.pid)
 if not members: return {}
 if owned.proc_start_token(proc.pid)!=record['start_token']:
  if any(snapshot.get(p)!=t for p,t in members.items()): raise RuntimeError('leader absent or reused before new member registration')
 else:
  try: actual_pgid=os.getpgid(proc.pid)
  except ProcessLookupError:
   if not owned.group_members(proc.pid): return {}
   raise RuntimeError('shutdown leader vanished with remaining members')
  if actual_pgid!=proc.pid: raise RuntimeError('shutdown leader PGID changed')
  if any(p in snapshot and snapshot[p]!=t for p,t in members.items()): raise RuntimeError('shutdown member token reused')
  snapshot.update(members)
 return members

def send(sig):
 members=observe()
 # Validate snapshot again immediately before signalling this exact-owned group.
 if any(snapshot.get(p)!=t for p,t in members.items()): raise RuntimeError('unowned shutdown group member')
 if members:
  os.killpg(proc.pid,sig)
  record.setdefault('signals',[]).append(dict(signal=sig,seconds=time.monotonic()-started,members=members))
try:
 remaining=deadline-time.monotonic()
 if remaining<=0:
  record['timed_out']=True
  raise TimeoutError('container shutdown absolute deadline reached before command')
 signal.setitimer(signal.ITIMER_REAL,remaining)
 for k in ('stdout','stderr'): files[k]=(directory/('shutdown.'+k)).open('wb',buffering=0);captured[k]=0
 # The child waits on our private pipe until actual PID/PGID/start-token are durable.
 command=['/bin/bash','-c','read -r permit; [[ "$permit" == CI2_SHUTDOWN ]] || exit 79; exec /usr/sbin/runuser -u ubuntu -- env HOME=/ci2/home XDG_CACHE_HOME=/ci2/cache/xdg TEST_TMPDIR=/ci2/cache/output-root TMPDIR=/ci2/tmp CC=/ci2/forbidden-host-cc PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin /bin/bash -c \'[[ "$(id -u):$(id -g)" == 1000:0 ]] || exit 78; cd /ci2/src && exec bazel --output_user_root=/ci2/cache/output-root shutdown\'']
 proc=subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
 token=owned.proc_start_token(proc.pid)
 if not token or os.getpgid(proc.pid)!=proc.pid: raise RuntimeError('cannot verify shutdown child identity')
 record.update(pid=proc.pid,pgid=proc.pid,start_token=token);snapshot[proc.pid]=token;save()
 proc.stdin.write(b'CI2_SHUTDOWN\n');proc.stdin.close()
 sel=selectors.DefaultSelector()
 for k,pipe in [('stdout',proc.stdout),('stderr',proc.stderr)]:sel.register(pipe,selectors.EVENT_READ,k)
 term=False; killed=False
 while time.monotonic()<deadline:
  for key,_ in sel.select(.05):
   data=os.read(key.fileobj.fileno(),65536)
   if not data:sel.unregister(key.fileobj);key.fileobj.close();continue
   k=key.data;files[k].write(data[:max(0,65536-captured[k])]);captured[k]+=len(data)
  members=observe(); elapsed=time.monotonic()-started
  if proc.poll() is not None and not members and not sel.get_map():record['quiescent']=True;break
  if elapsed>=20 and not term:record['timed_out']=True;send(signal.SIGTERM);term=True;save()
  if elapsed>=22 and not killed:send(signal.SIGKILL);killed=True;save()
 record['child_exit']=proc.poll()
 record['streams']={k:dict(observed_bytes=n,retained_bytes=min(n,65536),truncated=n>65536) for k,n in captured.items()}
except Exception as e:
 failure=str(e);record['refusal']=failure
 # No broader discovery or unverifiable kill: host must tear down exact container.
finally:
 signal.setitimer(signal.ITIMER_REAL,0)
 for f in files.values():f.close()
 record['private_server_observations'].append(bounded_server_observation('after-shutdown'))
 save();print(json.dumps(record,sort_keys=True),flush=True)
if failure or record['timed_out'] or not record['quiescent'] or record.get('child_exit')!=0 or any(n>65536 for n in captured.values()):raise SystemExit(74)
PYSHUTDOWN
}

freeze_workloads() {
  local finalizer=$1 check_rc
  assert_current_finalizer_identity "$finalizer" || return 74
  if [[ -d "$SRC" ]]; then
    bounded_shutdown "$finalizer" || return 74
  fi
  # A detached server is not a member of the shutdown client group. Only its
  # previously verified PID/start record is consulted; never discover or kill by name.
  if [[ -f "$EVIDENCE/diagnostics/server-pid.stdout" ]]; then
    local server_pid server_start
    [[ -f "$EVIDENCE/diagnostics/homebrew-server-preidentified.txt" ]] || {
      echo 'private Bazel server identity unavailable; freeze refused' >&2; return 74;
    }
    server_pid=$(awk -F= '$1=="pid" {print $2}' "$EVIDENCE/diagnostics/homebrew-server-preidentified.txt") || return 74
    server_start=$(awk -F= '$1=="starttime" {print $2}' "$EVIDENCE/diagnostics/homebrew-server-preidentified.txt") || return 74
    [[ "$server_pid" =~ ^[0-9]+$ && "$server_start" =~ ^[0-9]+$ ]] || return 74
    if [[ -d "/proc/$server_pid" ]]; then
      printf 'private_server_pid=%s recorded_start=%s quiescence=unproved no_signal_sent=true\n' "$server_pid" "$server_start" >&2
      return 74
    fi
    printf 'private_server_pid=%s recorded_start=%s observed=absent no_signal_sent=true\n' "$server_pid" "$server_start" >"$EVIDENCE/diagnostics/private-server-state.txt"
  fi
  if pgrep -f 'run-linux-qualification-v16-container.sh (setup|homebrew|ci-build|ci-test-ffi|release-linux|release-darwin)|bazel.*output_user_root=/ci2|ci2-bounded-log-v16.py|ci2-bazel-stall-v16.py|ci2-phase-owner-v16.py|ci2-terminate-owned-v16.py terminate|apt-get|git -C /ci2|jcmd .*Thread\.print' >/dev/null 2>&1; then
    echo 'owned workload or logger remains during freeze; no private-server retirement inferred' >&2
    return 74
  else
    check_rc=$?
    (( check_rc == 1 )) || return 74
  fi
  : >"$CONTROL/WORKLOAD_COMPLETE"
}

finalize_evidence() {
  local result=$1 finalizer=$2 reason=${3:-none} bytes manifest_tmp manifest_sha
  freeze_workloads "$finalizer" || return "$?"
  if [[ "$result" == PASS ]]; then
    for marker in SETUP_DONE HOMEBREW_DONE CI_BUILD_DONE CI_TEST_FFI_DONE RELEASE_LINUX_DONE RELEASE_DARWIN_DONE; do expect_state "$marker"; done
    if find "$EVIDENCE" -name '*.truncated' -print -quit | grep -q .; then
      echo 'truncated evidence forbids PASS' >&2
      return 76
    fi
  fi
  mkdir -p "$EVIDENCE"
  chmod -R u+w "$EVIDENCE" 2>/dev/null || true
  rm -f "$CONTROL/EVIDENCE_READY" "$EVIDENCE/MANIFEST.sha256"
  {
    echo "result=$result"
    echo "reason=$reason"
    echo "commit=$EXPECTED_COMMIT"
    echo "tree=$EXPECTED_TREE"
    echo "run_id=$RUN_ID"
    echo "owner_token=$OWNER_TOKEN"
    echo "cc=$INVALID_CC"
    echo 'bazel_version=9.1.0'
    echo 'disk_cache=/ci2/cache/disk'
    echo 'disk_cache_gc_max_size=1G'
    date -u +finalized_utc=%Y-%m-%dT%H:%M:%SZ
  } >"$EVIDENCE/MANIFEST.meta"
  record_usage final
  bytes=$(evidence_bytes)
  (( bytes <= EVIDENCE_LIMIT )) || return 75
  manifest_tmp="$CONTROL/MANIFEST.sha256.tmp"
  (cd "$EVIDENCE" && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum >"$manifest_tmp")
  mv "$manifest_tmp" "$EVIDENCE/MANIFEST.sha256"
  bytes=$(evidence_bytes)
  (( bytes <= EVIDENCE_LIMIT )) || return 75
  chmod -R a-w "$EVIDENCE"
  manifest_sha=$(sha256sum "$EVIDENCE/MANIFEST.sha256" | awk '{print $1}')
  printf 'result=%s\nfile_bytes=%s\nmanifest_sha256=%s\n' "$result" "$bytes" "$manifest_sha" >"$CONTROL/EVIDENCE_READY"
}

setup() {
  begin_phase setup
  [[ ! -e "$INVALID_CC" && ! -e "$SRC" ]]
  mkdir -p "$EVIDENCE/logs" "$EVIDENCE/products" "$EVIDENCE/toolchains" "$EVIDENCE/diagnostics"
  validate_ubuntu_identity
  mkdir -p "$CACHE/disk" "$CACHE/output-root" "$CACHE/xdg" /ci2/home /ci2/tmp "$SRC"
  chown 1000:0 "$SRC" "$CACHE" "$CACHE/disk" "$CACHE/output-root" "$CACHE/xdg" /ci2/home /ci2/tmp
  chmod 0755 "$SRC" "$CACHE" "$CACHE/disk" "$CACHE/output-root" "$CACHE/xdg" /ci2/home /ci2/tmp
  chmod 0700 "$CONTROL"
  local owned
  for owned in "$SRC" "$CACHE" "$CACHE/disk" "$CACHE/output-root" "$CACHE/xdg" /ci2/home /ci2/tmp; do
    [[ "$(stat -c '%u:%g:%a' "$owned")" == 1000:0:755 ]] || {
      echo 'CI2_ACCOUNT_OWNERSHIP_REFUSED: source/cache directory identity or mode differs' >&2; return 78;
    }
  done
  [[ "$(stat -c '%u:%g:%a' "$CONTROL")" == 0:0:700 && "$(stat -c '%u:%g:%a' "$EVIDENCE")" == 0:0:755 ]] || {
    echo 'CI2_ACCOUNT_OWNERSHIP_REFUSED: root control/evidence boundary differs' >&2; return 78;
  }
  [[ -z "$(find "$DISK_CACHE" -mindepth 1 -print -quit)" ]]
  [[ "$(/usr/local/bin/bazel --version)" == 'bazel 9.1.0' ]]
  [[ "$(sha256sum /usr/local/bin/bazel | awk '{print $1}')" == "$BAZEL_BINARY_SHA" ]]
  [[ "$(sha256sum /ci2/run-linux-qualification-v16-container.sh | awk '{print $1}')" == "$RUNNER_SHA" ]]
  [[ "$(sha256sum "$LOGGER" | awk '{print $1}')" == "$LOGGER_SHA" ]]
  [[ "$(sha256sum "$TOOLCHAIN_CHECK" | awk '{print $1}')" == "$TOOLCHAIN_SHA" ]]
  [[ "$(sha256sum "$STALL_HELPER" | awk '{print $1}')" == "$STALL_SHA" ]]
  [[ "$(sha256sum "$HOMEBREW_CHECK" | awk '{print $1}')" == "$HOMEBREW_SHA" ]]
  [[ "$(sha256sum "$TERMINATOR" | awk '{print $1}')" == "$TERMINATOR_SHA" ]]
  {
    for tool in bash git python3 runuser setsid timeout java jcmd gcc g++ make sha256sum ps pgrep; do
      printf '%s=%s\n' "$tool" "$(command -v "$tool")"
    done
    /usr/local/bin/bazel --version
    gcc --version
    g++ --version
    java -version 2>&1
    sha256sum /usr/local/bin/bazel
  } >"$EVIDENCE/image-tools-runtime.txt"
  if command -v ruby >/dev/null 2>&1 || command -v file >/dev/null 2>&1; then
    echo 'fixed image unexpectedly already contains Ruby or file' >&2
    return 72
  fi
  run_bounded "$EVIDENCE/logs/apt-install.log" /bin/bash -c \
    'apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ruby file && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*.deb'
  {
    ruby --version
    file --version
  } >"$EVIDENCE/installed-tools-runtime.txt"
  run_bounded "$EVIDENCE/logs/source-clone.log" as_ubuntu git -C /ci2 clone /input/source.bundle /ci2/src
  as_ubuntu git -C "$SRC" checkout --detach "$EXPECTED_COMMIT" >>"$EVIDENCE/logs/source-clone.log" 2>&1
  as_ubuntu git -C "$SRC" remote remove origin
  {
    as_ubuntu git -C "$SRC" rev-parse HEAD
    as_ubuntu git -C "$SRC" rev-parse 'HEAD^{tree}'
    as_ubuntu git -C "$SRC" status --short --untracked-files=no
    as_ubuntu git -C "$SRC" diff --name-only "$BASE_COMMIT..HEAD"
    as_ubuntu git -C "$SRC" replace -l
    test ! -e "$SRC/.git/info/grafts"
    as_ubuntu git -C "$SRC" fsck --full --no-dangling
    as_ubuntu git -C "$SRC" bundle verify /input/source.bundle
    as_ubuntu /bin/bash -c "cd '$SRC' && ./tools/workspace_status.sh"
  } >"$EVIDENCE/source-identity.txt" 2>&1
  [[ "$(as_ubuntu git -C "$SRC" rev-parse HEAD)" == "$EXPECTED_COMMIT" ]]
  [[ "$(as_ubuntu git -C "$SRC" rev-parse 'HEAD^{tree}')" == "$EXPECTED_TREE" ]]
  [[ -z "$(as_ubuntu git -C "$SRC" status --short --untracked-files=no)" ]]
  mapfile -t changed < <(as_ubuntu git -C "$SRC" diff --name-only "$BASE_COMMIT..HEAD")
  [[ "${changed[*]}" == '.bazelrc .github/workflows/ci.yml .github/workflows/release.yml MODULE.bazel Makefile' ]]
  [[ -z "$(as_ubuntu git -C "$SRC" replace -l)" && ! -e "$SRC/.git/info/grafts" ]]
  {
    echo 'bazel_version=9.1.0'
    dpkg-query -W -f='${Package}=${Version}\n' ruby file gcc g++ 2>&1 || true
    sha256sum /input/source.bundle /ci2/run-linux-qualification-v16-container.sh "$LOGGER" "$TOOLCHAIN_CHECK" "$STALL_HELPER" "$HOMEBREW_CHECK" "$TERMINATOR"
    sha256sum "$SRC/.bazelrc" "$SRC/.github/workflows/ci.yml" "$SRC/.github/workflows/release.yml" "$SRC/MODULE.bazel" "$SRC/Makefile"
  } >"$EVIDENCE/tool-source-identity.txt"
  [[ -z "$(find "$DISK_CACHE" -mindepth 1 -print -quit)" ]]
  record_usage setup
  end_phase SETUP_DONE
}

homebrew_step() {
  local step=$1 rc
  shift
  printf 'step=%s state=start\n' "$step" >>"$EVIDENCE/diagnostics/homebrew-steps.txt"
  if "$@"; then rc=0; else rc=$?; fi
  printf 'step=%s state=result exit=%s\n' "$step" "$rc" >>"$EVIDENCE/diagnostics/homebrew-steps.txt"
  if (( rc != 0 )); then
    printf 'step=%s state=refused exit=%s\n' "$step" "$rc" >>"$EVIDENCE/diagnostics/homebrew-steps.txt"
  fi
  return "$rc"
}

homebrew_cold_cache() {
  local check=${1:?missing cold-cache check label}
  /usr/bin/python3 - "$DISK_CACHE" "$check" "$EVIDENCE/diagnostics/homebrew-steps.txt" <<'PYCOLD'
import base64,json,os,stat,sys
root,check,diagnostic=sys.argv[1:]
row=dict(kind='COLD_CACHE_OBSERVATION',check=check,root=root,scan_exit=None,outcome='scan_error',first_entry='unavailable')
status=78
try:
 fd=os.open(root,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
 try:
  opened=os.fstat(fd)
  if not stat.S_ISDIR(opened.st_mode):raise NotADirectoryError(root)
  with os.scandir(fd) as entries:
   entry=next(entries,None)
   if entry is None:
    row.update(scan_exit=0,outcome='empty',first_entry=None);status=0
   else:
    name=os.fsencode(entry.name);path=os.path.join(os.fsencode(root),name)
    first=dict(path_base64=base64.b64encode(path).decode('ascii'),name_bytes=len(name),type='unavailable')
    row.update(scan_exit=0,outcome='nonempty',first_entry=first);status=1
    try:
     value=entry.stat(follow_symlinks=False)
     kinds=((stat.S_ISREG,'regular'),(stat.S_ISDIR,'directory'),(stat.S_ISLNK,'symlink'),
            (stat.S_ISFIFO,'fifo'),(stat.S_ISSOCK,'socket'),(stat.S_ISCHR,'character'),(stat.S_ISBLK,'block'))
     first.update(type=next((label for predicate,label in kinds if predicate(value.st_mode)),'unknown'),dev=value.st_dev,ino=value.st_ino)
    except Exception as exc:
     row.update(scan_exit=getattr(exc,'errno',None),outcome='scan_error');first['stat_error']=type(exc).__name__[:64];status=78
 finally:os.close(fd)
except Exception as exc:
 row.update(scan_exit=getattr(exc,'errno',None),outcome='scan_error',error_type=type(exc).__name__[:64]);status=78
def append_record(value):
 data=(json.dumps(value,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')
 if len(data)>4096:
  value=dict(kind='COLD_CACHE_OBSERVATION',check=check,root=root,scan_exit=None,outcome='diagnostic_refused',first_entry='unavailable',detail='record-too-long')
  data=(json.dumps(value,sort_keys=True,separators=(',',':'))+'\n').encode('utf-8')
  if len(data)>4096:raise ValueError('fallback-record-too-long')
 fd=os.open(diagnostic,os.O_WRONLY|os.O_APPEND|os.O_NOFOLLOW|os.O_NONBLOCK)
 try:
  opened=os.fstat(fd)
  if not stat.S_ISREG(opened.st_mode) or opened.st_nlink!=1 or opened.st_size+len(data)>16384:raise ValueError('diagnostic-not-bounded-regular')
  offset=0
  while offset<len(data):
   wrote=os.write(fd,data[offset:])
   if wrote<=0:raise OSError('short-diagnostic-write')
   offset+=wrote
 finally:os.close(fd)
try:append_record(row)
except Exception as exc:
 print('CI2_COLD_CACHE_DIAGNOSTIC_REFUSED check=%s error=%s'%(check,type(exc).__name__),file=sys.stderr)
 raise SystemExit(79)
raise SystemExit(status)
PYCOLD
}

homebrew_server_identity() {
  local server_pid server_start server_cmdline
  server_pid=$(cat "$EVIDENCE/diagnostics/server-pid.stdout") || return "$?"
  [[ "$server_pid" =~ ^[0-9]+$ && -r "/proc/$server_pid/stat" ]] || return 78
  server_start=$(operation_start_token "$server_pid") || return "$?"
  [[ "$server_start" =~ ^proc:[0-9]+$ ]] || return 78
  server_cmdline=$(tr '\0' ' ' <"/proc/$server_pid/cmdline") || return "$?"
  [[ "${server_cmdline,,}" == *java* && "${server_cmdline,,}" == *bazel* && "$server_cmdline" == *'/ci2/cache/output-root'* ]] || return 78
  printf 'pid=%s\nstarttime=%s\ncmdline=%s\n' "$server_pid" "${server_start#proc:}" "$server_cmdline" >"$EVIDENCE/diagnostics/homebrew-server-preidentified.txt"
}

homebrew() {
  expect_state SETUP_DONE
  begin_phase homebrew
  local diag_pid diag_rc server_pid server_start rc
  homebrew_step cold-cache-before-server homebrew_cold_cache cold-cache-before-server
  homebrew_step server-pid-command run_split_bounded "$EVIDENCE/diagnostics/server-pid.stdout" 16384 \
    "$EVIDENCE/diagnostics/server-pid.stderr" 16384 bazel_u info --disk_cache= server_pid
  homebrew_step server-pid-validation homebrew_server_identity
  server_pid=$(awk -F= '$1=="pid" {print $2}' "$EVIDENCE/diagnostics/homebrew-server-preidentified.txt")
  server_start=$(awk -F= '$1=="starttime" {print $2}' "$EVIDENCE/diagnostics/homebrew-server-preidentified.txt")
  homebrew_step cold-cache-before-target homebrew_cold_cache cold-cache-before-target
  capture_homebrew_stall "$server_pid" "$server_start" >"$EVIDENCE/diagnostics/homebrew-stall-helper.stdout" 2>"$EVIDENCE/diagnostics/homebrew-stall-helper.stderr" &
  diag_pid=$!
  if homebrew_step target run_bounded "$EVIDENCE/logs/homebrew.log" bazel_u test --profile="$CACHE/homebrew.profile.gz" \
    //:homebrew_formula_updater_test --test_output=errors --toolchain_resolution_debug='.*'; then rc=0; else rc=$?; fi
  if kill "$diag_pid" 2>/dev/null; then
    if wait "$diag_pid" 2>/dev/null; then diag_rc=0; else diag_rc=$?; fi
    printf 'step=stall-helper state=cancelled wait_exit=%s\n' "$diag_rc" >>"$EVIDENCE/diagnostics/homebrew-steps.txt"
  else
    if wait "$diag_pid"; then diag_rc=0; else diag_rc=$?; fi
    printf 'step=stall-helper state=result exit=%s\n' "$diag_rc" >>"$EVIDENCE/diagnostics/homebrew-steps.txt"
    (( diag_rc == 0 )) || return "$diag_rc"
  fi
  (( rc == 0 )) || return "$rc"
  homebrew_step isolation-checker run_split_bounded "$EVIDENCE/diagnostics/isolation.stdout" 16384 \
    "$EVIDENCE/diagnostics/isolation.stderr" 16384 /usr/bin/python3 "$HOMEBREW_CHECK" \
    --log "$EVIDENCE/logs/homebrew.log" --profile "$CACHE/homebrew.profile.gz" --profile-limit "$PROFILE_LIMIT" \
    --output "$EVIDENCE/homebrew-isolation-verified.json"
  homebrew_step agents-check run_bounded "$EVIDENCE/logs/agents.log" as_ubuntu /bin/bash -c "cd '$SRC' && ./scripts/check-agents-md-tracked.sh"
  homebrew_step profile-copy copy_profile_if_bounded "$CACHE/homebrew.profile.gz" "$EVIDENCE/diagnostics/homebrew.profile.gz" complete
  homebrew_step module-graph run_bounded "$EVIDENCE/bazel-module-graph.log" bazel_u mod graph
  homebrew_step module-dependency grep -F 'hermetic_cc_toolchain@4.2.0' "$EVIDENCE/bazel-module-graph.log"
  homebrew_step output-base-command run_split_bounded "$EVIDENCE/diagnostics/output-base.stdout" 16384 \
    "$EVIDENCE/diagnostics/output-base.stderr" 16384 bazel_u info output_base
  local output_base
  output_base=$(cat "$EVIDENCE/diagnostics/output-base.stdout")
  homebrew_step output-base-validation test "${output_base#/ci2/}" != "$output_base"
  printf 'output_base=%s\ndisk_cache=%s\n' "$output_base" "$DISK_CACHE" >"$EVIDENCE/cache-paths.txt"
  homebrew_step usage record_usage homebrew
  end_phase HOMEBREW_DONE
}

# Finite diagnostic enum only. No wrapped command or conditional function call.
# A missing/failed write is a diagnostic gap; it cannot change the producer rc.
ci_note() {
  [[ "${CI_BUILD_OBSERVING:-0}" == 1 ]] || return 0
  case "$1" in state|begin|first-build|success-text|toolchain-setup|toolchain-invocation|resolution|aquery|cquery|execution-root|execution-root-value|verifier|usage|end) ;; *) return 0 ;; esac
  case "$2" in active|success|failure) ;; *) return 0 ;; esac
  [[ "${3:-0}" =~ ^[0-9]{1,3}$ ]] || return 0
  CI_BUILD_ACTIVE=$1
  CI_BUILD_RECORDS=$(( ${CI_BUILD_RECORDS:-0} + 1 ))
  if (( CI_BUILD_RECORDS <= 48 )); then
    printf 'step=%s state=%s rc=%s\n' "$1" "$2" "${3:-0}" >>"$EVIDENCE/diagnostics/ci-build-steps.txt" || true
  fi
  return 0
}

ci_build() {
  CI_BUILD_OBSERVING=1
  ci_note state active
  expect_state HOMEBREW_DONE
  ci_note state success
  ci_note begin active
  begin_phase ci-build
  ci_note begin success
  ci_note first-build active
  run_bounded "$EVIDENCE/logs/ci-build.log" bazel_u build --config=ci //...
  ci_note first-build success
  ci_note success-text active
  grep -F 'Build completed successfully' "$EVIDENCE/logs/ci-build.log" >/dev/null
  ci_note success-text success
  collect_toolchain_evidence ci-linux ci --config=ci
  ci_note usage active
  record_usage ci-build
  ci_note usage success
  ci_note end active
  end_phase CI_BUILD_DONE
  ci_note end success
}

ci_test_ffi() {
  expect_state CI_BUILD_DONE
  begin_phase ci-test-ffi
  run_bounded "$EVIDENCE/logs/ci-test.log" bazel_u test --config=ci //...
  grep -F 'Build completed successfully' "$EVIDENCE/logs/ci-test.log" >/dev/null
  grep -E '(PASSED|Executed [0-9]+ out of [0-9]+ tests)' "$EVIDENCE/logs/ci-test.log" >/dev/null
  if grep -E '(^| )FAILED( |$)' "$EVIDENCE/logs/ci-test.log" >/dev/null; then return 1; fi
  run_split_bounded "$EVIDENCE/logs/ffi-fetch.stdout" "$LOG_LIMIT" "$EVIDENCE/logs/ffi-fetch.stderr" "$LOG_LIMIT" \
    bazel_u run --config=ci //cmd:housegate -- fetch-rewriter-lib --tag v0.10.0
  local ffi_path
  ffi_path=$(tail -n 1 "$EVIDENCE/logs/ffi-fetch.stdout")
  [[ "$ffi_path" == /ci2/* && -s "$ffi_path" ]]
  printf 'ffi_path=%s\n' "$ffi_path" >"$EVIDENCE/ffi-identity.txt"
  record_usage ci-test-ffi
  end_phase CI_TEST_FFI_DONE
}

release_linux() {
  expect_state CI_TEST_FFI_DONE
  begin_phase release-linux
  local flags=(--extra_toolchains=@zig_sdk//toolchain:linux_amd64_gnu.2.31 --platforms=@rules_go//go/toolchain:linux_amd64 --@rules_go//go/config:static=true)
  run_bounded "$EVIDENCE/logs/release-linux.log" bazel_u build "${flags[@]}" //cmd:housegate
  grep -F 'Build completed successfully' "$EVIDENCE/logs/release-linux.log" >/dev/null
  collect_toolchain_evidence release-linux release-linux "${flags[@]}"
  cp -f "$SRC/bazel-bin/cmd/housegate_/housegate" "$EVIDENCE/products/housegate-linux-amd64"
  file "$EVIDENCE/products/housegate-linux-amd64" >"$EVIDENCE/products/linux-file.txt"
  grep -E 'ELF 64-bit.*x86-64|ELF 64-bit.*x86_64' "$EVIDENCE/products/linux-file.txt" >/dev/null
  record_usage release-linux
  end_phase RELEASE_LINUX_DONE
}

release_darwin() {
  expect_state RELEASE_LINUX_DONE
  begin_phase release-darwin
  local flags=(--extra_toolchains=@zig_sdk//toolchain:darwin_arm64 --platforms=@rules_go//go/toolchain:darwin_arm64)
  run_bounded "$EVIDENCE/logs/release-darwin.log" bazel_u build "${flags[@]}" //cmd:housegate
  grep -F 'Build completed successfully' "$EVIDENCE/logs/release-darwin.log" >/dev/null
  collect_toolchain_evidence release-darwin release-darwin "${flags[@]}"
  cp -f "$SRC/bazel-bin/cmd/housegate_/housegate" "$EVIDENCE/products/housegate-darwin-arm64"
  file "$EVIDENCE/products/housegate-darwin-arm64" >"$EVIDENCE/products/darwin-file.txt"
  grep -E 'Mach-O 64-bit.*arm64' "$EVIDENCE/products/darwin-file.txt" >/dev/null
  sha256sum "$EVIDENCE/products/housegate-linux-amd64" "$EVIDENCE/products/housegate-darwin-arm64" >"$EVIDENCE/products/SHA256SUMS"
  record_usage release-darwin
  end_phase RELEASE_DARWIN_DONE
}

validate_finalizer_lock() {
  local operation=$1
  [[ -f "$CONTROL/finalizer.lock/identity" ]]
  [[ "$(awk -F= '$1=="run_id" {print $2}' "$CONTROL/finalizer.lock/identity")" == "$RUN_ID" ]]
  [[ "$(awk -F= '$1=="owner_token" {print $2}' "$CONTROL/finalizer.lock/identity")" == "$OWNER_TOKEN" ]]
  [[ "$(awk -F= '$1=="phase" {print $2}' "$CONTROL/finalizer.lock/identity")" == "$operation" ]]
  [[ "$(awk -F= '$1=="runner_sha256" {print $2}' "$CONTROL/finalizer.lock/identity")" == "$RUNNER_SHA" ]]
}

terminate_recorded_operation() {
  local recovery_identity="$CONTROL/phase.last-identity" helper_digest
  [[ -f "$recovery_identity" ]] || return 0
  helper_digest=$(sha256sum "$TERMINATOR" | awk '{print $1}') || return 74
  [[ "$helper_digest" == "$TERMINATOR_SHA" ]] || return 74
  /usr/bin/python3 "$TERMINATOR" terminate \
    --identity "$recovery_identity" --shadow-identity "$CONTROL/phase.identity" \
    --runner /ci2/run-linux-qualification-v16-container.sh \
    --expected-run "$RUN_ID" \
    --expected-owner "$OWNER_TOKEN" \
    --expected-runner-sha "$RUNNER_SHA" \
    --finalizer-lock "$CONTROL/finalizer.lock" \
    --term-seconds 20 --kill-seconds 5 --total-seconds 30 || return "$?"
}

run_pass_finalizer() {
  start_finalizer finalize-pass
  finalize_evidence PASS finalize-pass complete
  finish_finalizer finalize-pass PASS
}

abort_finalize() {
  local reason=${1:-host_failure}
  terminate_recorded_operation || return "$?"
  if [[ -d "$CONTROL/finalizer.lock" ]]; then
    echo 'unowned or incomplete finalizer lock remains' >&2
    return 72
  fi
  start_finalizer finalize-fail
  chmod -R u+w "$EVIDENCE" 2>/dev/null || true
  finalize_evidence FAIL finalize-fail "$reason"
  finish_finalizer finalize-fail FAIL
}

command=${1:-}
case "$command" in
  setup) setup ;;
  homebrew) homebrew ;;
  ci-build) ci_build ;;
  ci-test-ffi) ci_test_ffi ;;
  release-linux) release_linux ;;
  release-darwin) release_darwin ;;
  finalize) run_pass_finalizer ;;
  abort-finalize) shift; abort_finalize "$*" ;;
  *) echo "unknown command: $command" >&2; exit 64 ;;
esac
