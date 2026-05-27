#!/usr/bin/env bash
# A4 — the key assumption behind the whole transparent-quorum-endpoint design:
# when the thing fronting 9181 (here: kproxy, later: housegate) force-drops a
# CH's keeper connection, does ClickHouse's keeper client self-heal cleanly
# (reconnect, keep/re-establish session, re-arm the ReplicatedMergeTree
# ephemerals/watches) with NO operator intervention and NO config reload?
#
# Run from tests/keeper-testbed:  ./scripts/a4-reconnect-test.sh
set -uo pipefail
cd "$(dirname "$0")/.."

PASS="✅ 通过"; FAIL="❌ 失败"; SKIP="⚠️ 跳过"
rc=0

chq()  { docker compose exec -T "$1" clickhouse-client -q "$2" 2>&1; }   # service, sql
ctrl() { curl -s "localhost:$1$2"; }                                     # hostport, path

repl_field() { # service, column -> value for testdb.events
  chq "$1" "SELECT $2 FROM system.replicas WHERE database='testdb' AND table='events'"
}
sess_uptime() { chq "$1" "SELECT session_uptime_elapsed_seconds FROM system.zookeeper_connection"; }

echo "============================================================"
echo " A4: CH keeper-client self-heal after proxy-forced disconnect"
echo "============================================================"

# ---- setup: one ReplicatedMergeTree, two replicas, through kproxy ----------
echo; echo ">> setup: create ReplicatedMergeTree on both replicas"
DDL_DB="CREATE DATABASE IF NOT EXISTS testdb"
DDL_TBL="CREATE TABLE IF NOT EXISTS testdb.events (ts DateTime, uid UInt64, data String)
         ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/testdb/events','{replica}')
         ORDER BY (ts, uid)"
for c in ch-1 ch-2; do
  for attempt in $(seq 1 15); do
    out=$(chq "$c" "$DDL_DB"); out2=$(chq "$c" "$DDL_TBL")
    if [ -z "$out$out2" ]; then echo "   $c: table ready"; break; fi
    [ "$attempt" = 15 ] && { echo "   $c: DDL failed: $out $out2"; echo "$FAIL setup"; exit 1; }
    sleep 2
  done
done

# ---- baseline: replication works THROUGH kproxy ----------------------------
echo; echo ">> baseline: insert on ch-1, read on ch-2 (replication via kproxy)"
chq ch-1 "INSERT INTO testdb.events SELECT now(), number, 'baseline' FROM numbers(1000)" >/dev/null
chq ch-2 "SYSTEM SYNC REPLICA testdb.events" >/dev/null
n2=$(chq ch-2 "SELECT count() FROM testdb.events")
echo "   ch-2 sees $n2 rows"
if [ "$n2" = "1000" ]; then echo "   $PASS baseline replication"; else echo "   $FAIL baseline (expected 1000)"; rc=1; fi

# ---- pre-drop snapshot ------------------------------------------------------
echo; echo ">> pre-drop state"
for c in ch-1 ch-2; do
  printf "   %s: session_uptime=%ss is_session_expired=%s is_readonly=%s active_replicas=%s\n" \
    "$c" "$(sess_uptime $c)" "$(repl_field $c is_session_expired)" \
    "$(repl_field $c is_readonly)" "$(repl_field $c active_replicas)"
done

# ---- A4 trigger: force-drop every CH<->keeper connection -------------------
echo; echo ">> TRIGGER: POST /drop on both kproxies (force disconnect)"
echo "   kproxy-1: $(ctrl 8181 /drop)"
echo "   kproxy-2: $(ctrl 8182 /drop)"
t0=$(date +%s.%N)

# ---- measure recovery -------------------------------------------------------
echo; echo ">> waiting for self-heal (no operator action, no reload) ..."
recovered=""
for i in $(seq 1 60); do
  ok=1
  for c in ch-1 ch-2; do
    se=$(repl_field $c is_session_expired); ro=$(repl_field $c is_readonly)
    ar=$(repl_field $c active_replicas);    up=$(sess_uptime $c)
    # healed = session not expired, not readonly, both replicas active again,
    # and the TCP session is young (uptime reset => a real reconnect happened)
    if [ "$se" != "0" ] || [ "$ro" != "0" ] || [ "$ar" != "2" ] || [ -z "$up" ] || [ "$up" -gt 30 ] 2>/dev/null; then ok=0; fi
  done
  if [ "$ok" = "1" ]; then recovered=$(date +%s.%N); break; fi
  sleep 0.5
done

if [ -n "$recovered" ]; then
  dt=$(awk "BEGIN{printf \"%.2f\", $recovered-$t0}")
  echo "   $PASS self-healed in ~${dt}s"
else
  echo "   $FAIL did not self-heal within 30s"; rc=1
fi

echo; echo ">> post-drop state"
for c in ch-1 ch-2; do
  printf "   %s: session_uptime=%ss is_session_expired=%s is_readonly=%s active_replicas=%s\n" \
    "$c" "$(sess_uptime $c)" "$(repl_field $c is_session_expired)" \
    "$(repl_field $c is_readonly)" "$(repl_field $c active_replicas)"
done

# ---- post-recovery: replication still works --------------------------------
echo; echo ">> post-recovery: insert again on ch-1, verify it replicates to ch-2"
chq ch-1 "INSERT INTO testdb.events SELECT now(), number, 'after-drop' FROM numbers(500)" >/dev/null
chq ch-2 "SYSTEM SYNC REPLICA testdb.events" >/dev/null
n2b=$(chq ch-2 "SELECT count() FROM testdb.events")
echo "   ch-2 now sees $n2b rows (expect 1500)"
if [ "$n2b" = "1500" ]; then echo "   $PASS replication resumed after reconnect"; else echo "   $FAIL replication broken after reconnect"; rc=1; fi

echo; echo ">> kproxy counters (served_total grew => CH reconnected through proxy)"
echo "   kproxy-1: $(ctrl 8181 /status)"
echo "   kproxy-2: $(ctrl 8182 /status)"

echo; echo "============================================================"
[ "$rc" = "0" ] && echo " A4 RESULT: $PASS — CH self-heals on proxy-forced disconnect" \
                 || echo " A4 RESULT: $FAIL — see above"
echo "============================================================"
exit $rc
