#!/usr/bin/env bash
set -euo pipefail

WORK="${WORK:-/home/sentio/limengyu/work/verify-housegate-storage-integrity-321f8ef}"
CLICKHOUSE_BIN="$WORK/ClickHouse/build-housekeeper-clang22/programs/clickhouse"
HOUSEGATE_BAZEL_BIN="$WORK/housegate/bazel-bin/cmd/housegate_/housegate"
HOUSEGATE_BIN="$WORK/e2e-bin/housegate"
IMAGE="${E2E_IMAGE:-clickhouse/binary-builder:latest}"
SQL_REWRITER_IMAGE="${SQL_REWRITER_IMAGE:-us-west1-docker.pkg.dev/sentio-352722/sentio/sql-rewriter:0.1.27}"
SQL_REWRITER_BIN="${SQL_REWRITER_BIN:-/clickhouse_sentio_rewriter}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%d%H%M%S)}"
BASE_PREFIX="${BASE_PREFIX:-e2e-sql-rewriter-3x3}"
BASE="${BASE:-$WORK/$BASE_PREFIX-$RUN_ID}"
DOCKER_USER="$(id -u):$(id -g)"

AGENT_KEY="0x03bef7ad0f263e542a6bc04ba07ca41a958e5ed6e44a504deaec98aebf04d8b2"
AGENT_ADDR="0xb5757c11d515b9d70785b338d85efed045892187"

KEEPER_CLIENT_PORTS=(56181 56182 56183)
KEEPER_RAFT_PORTS=(56281 56282 56283)
CH_TCP_PORTS=(56301 56302 56303)
CH_HTTP_PORTS=(56401 56402 56403)
CH_INTERSERVER_PORTS=(56501 56502 56503)
SERVER_HG_PORTS=(56601 56602 56603)
SERVER_HG_METRICS_PORTS=(56701 56702 56703)
CLIENT_HG_PORT=56651
CLIENT_HG_METRICS_PORT=56751
SQL_REWRITER_PORT=56801

CONTAINERS=()
SQL_REWRITER_CONTAINER=""

HG_WORKER_REPLAY="${HG_WORKER_REPLAY:-true}"
HG_WORKER_UNSAFE_VALIDATION="${HG_WORKER_UNSAFE_VALIDATION:-true}"
HG_WORKER_PROMOTION="${HG_WORKER_PROMOTION:-true}"
HG_WORKER_ROLLBACK="${HG_WORKER_ROLLBACK:-true}"
HG_WORKER_SAFE_AUDIT="${HG_WORKER_SAFE_AUDIT:-true}"
HG_WORKER_FINALITY="${HG_WORKER_FINALITY:-true}"
E2E_INSERT_QUERY="${E2E_INSERT_QUERY:-INSERT INTO realbin.t VALUES (1,'a',now(),random()),(2,'b',now(),random())}"

log() {
    printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*"
}

die() {
    log "ERROR: $*"
    exit 1
}

cleanup() {
    local status=$?
    for c in "${CONTAINERS[@]:-}"; do
        if [[ "$c" == "${SQL_REWRITER_CONTAINER:-}" ]]; then
            docker logs "$c" > "$BASE/logs/sql-rewriter.stdout" 2>&1 || true
        fi
        docker rm -f "$c" >/dev/null 2>&1 || true
    done
    log "run_dir=$BASE"
    exit "$status"
}
trap cleanup EXIT

require_file() {
    test -x "$1" || die "executable not found: $1"
}

check_ports_free() {
    local used
    for p in "$@"; do
        used="$(ss -ltn "sport = :$p" 2>/dev/null | tail -n +2 || true)"
        test -z "$used" || die "port $p is already in use"
    done
}

ch_query() {
    local port="$1"
    local query="$2"
    docker run --rm --network host \
        --user "$DOCKER_USER" \
        -v "$WORK:$WORK" \
        -e CH_QUERY="$query" \
        "$IMAGE" /bin/bash -lc "exec timeout '${CH_QUERY_TIMEOUT:-15s}' '$CLICKHOUSE_BIN' client --connect_timeout 5 --send_timeout 5 --receive_timeout 5 --host 127.0.0.1 --port '$port' --query \"\$CH_QUERY\""
}

ch_multiquery() {
    local port="$1"
    local query="$2"
    docker run --rm --network host \
        --user "$DOCKER_USER" \
        -v "$WORK:$WORK" \
        -e CH_QUERY="$query" \
        "$IMAGE" /bin/bash -lc "exec timeout '${CH_MULTIQUERY_TIMEOUT:-90s}' '$CLICKHOUSE_BIN' client --connect_timeout 5 --send_timeout 5 --receive_timeout 5 --host 127.0.0.1 --port '$port' --multiquery --query \"\$CH_QUERY\""
}

keeper_query() {
    local port="$1"
    local query="$2"
    docker run --rm --network host \
        --user "$DOCKER_USER" \
        -v "$WORK:$WORK" \
        -e KEEPER_QUERY="$query" \
        "$IMAGE" /bin/bash -lc "exec '$CLICKHOUSE_BIN' keeper-client --host 127.0.0.1 --port '$port' --query \"\$KEEPER_QUERY\""
}

keeper_quote_path() {
    printf '"%s"' "$1"
}

keeper_ls() {
    local path="$1"
    keeper_query "${KEEPER_CLIENT_PORTS[0]}" "ls $(keeper_quote_path "$path")"
}

keeper_get() {
    local path="$1"
    keeper_query "${KEEPER_CLIENT_PORTS[0]}" "get $(keeper_quote_path "$path")"
}

keeper_create_path() {
    local path="$1"
    local data="$2"
    keeper_query "${KEEPER_CLIENT_PORTS[0]}" "create $(keeper_quote_path "$path") '$data'"
}

wait_for_keeper_children() {
    local label="$1"
    local path="$2"
    local timeout="$3"
    local start children
    start="$(date +%s)"
    while true; do
        children="$(keeper_ls "$path" | tr -d '\r' | tail -n 1 || true)"
        if [[ -n "$children" && "$children" != "[]" ]]; then
            log "$label found $children" >&2
            printf '%s\n' "$children"
            return 0
        fi
        if (( "$(date +%s)" - start >= timeout )); then
            log "$label not found under $path after ${timeout}s" >&2
            return 1
        fi
        sleep 1
    done
}

keeper_path_exists() {
    local path="$1"
    local out
    out="$(keeper_get "$path" 2>&1 || true)"
    [[ "$out" != *"No node"* && "$out" != *"node doesn't exist"* && "$out" != *"Can't get data for node"* && "$out" != *"Coordination error"* ]]
}

wait_for_keeper_path() {
    local label="$1"
    local path="$2"
    local timeout="$3"
    local start
    start="$(date +%s)"
    while true; do
        if keeper_path_exists "$path"; then
            log "$label exists at $path"
            return 0
        fi
        if (( "$(date +%s)" - start >= timeout )); then
            die "$label did not appear at $path after ${timeout}s"
        fi
        sleep 1
    done
}

wait_for_keeper_data_contains() {
    local label="$1"
    local path="$2"
    local needle="$3"
    local timeout="$4"
    local start data
    start="$(date +%s)"
    while true; do
        data="$(keeper_get "$path" 2>/dev/null | tr -d '\r' || true)"
        if [[ "$data" == *"$needle"* ]]; then
            log "$label contains $needle"
            return 0
        fi
        if (( "$(date +%s)" - start >= timeout )); then
            die "$label at $path did not contain '$needle' after ${timeout}s; data=$data"
        fi
        sleep 1
    done
}

wait_for_keeper_children_contains() {
    local label="$1"
    local path="$2"
    local timeout="$3"
    shift 3
    local expected=("$@")
    local start children missing child
    start="$(date +%s)"
    while true; do
        children="$(keeper_ls "$path" 2>/dev/null | tr -d '\r' | tail -n 1 || true)"
        missing=0
        for child in "${expected[@]}"; do
            if [[ "$children" != *"$child"* ]]; then
                missing=1
                break
            fi
        done
        if [[ "$missing" == "0" ]]; then
            log "$label contains ${expected[*]}: $children"
            return 0
        fi
        if (( "$(date +%s)" - start >= timeout )); then
            die "$label at $path missing ${expected[*]} after ${timeout}s; children=$children"
        fi
        sleep 1
    done
}

submit_mock_finality() {
    local statement_id="$1"
    local path="/housekeeper/v1/storage_integrity/finality/$statement_id"
    local data="kind=mock\\nfinalized=true\\n"
    keeper_create_path "$path" "$data" >/dev/null
}

start_container() {
    local name="$1"
    local logfile="$2"
    local cmd="$3"
    docker rm -f "$name" >/dev/null 2>&1 || true
    local cid
    cid="$(docker run -d --name "$name" --network host \
        --user "$DOCKER_USER" \
        --ulimit nofile=262144:262144 \
        -v "$WORK:$WORK" \
        -w "$WORK" \
        "$IMAGE" /bin/bash -lc "exec $cmd > '$logfile' 2>&1")"
    CONTAINERS+=("$name")
    log "started $name container=$cid log=$logfile"
}

start_sql_rewriter_container() {
    local name="$1"
    docker rm -f "$name" >/dev/null 2>&1 || true
    local cid
    cid="$(docker run -d --name "$name" --network host \
        --user "$DOCKER_USER" \
        --ulimit nofile=262144:262144 \
        "$SQL_REWRITER_IMAGE" "$SQL_REWRITER_BIN" "$SQL_REWRITER_PORT")"
    CONTAINERS+=("$name")
    SQL_REWRITER_CONTAINER="$name"
    log "started $name container=$cid image=$SQL_REWRITER_IMAGE log=$BASE/logs/sql-rewriter.stdout"
}

wait_for_cmd() {
    local label="$1"
    local timeout="$2"
    shift 2
    local start
    start="$(date +%s)"
    while true; do
        if "$@" >/dev/null 2>&1; then
            log "$label ready"
            return 0
        fi
        if (( "$(date +%s)" - start >= timeout )); then
            log "$label not ready after ${timeout}s"
            return 1
        fi
        sleep 1
    done
}

wait_for_tcp() {
    local label="$1"
    local port="$2"
    wait_for_cmd "$label" 90 bash -lc ":</dev/tcp/127.0.0.1/$port"
}

wait_for_query_value() {
    local label="$1"
    local port="$2"
    local query="$3"
    local want="$4"
    local timeout="$5"
    local start got
    start="$(date +%s)"
    while true; do
        got="$(ch_query "$port" "$query" 2>/dev/null | tr -d '\r' | tail -n 1 || true)"
        if [[ "$got" == "$want" ]]; then
            log "$label matched $want"
            return 0
        fi
        if (( "$(date +%s)" - start >= timeout )); then
            die "$label got '$got', want '$want'"
        fi
        sleep 1
    done
}

write_users_xml() {
    local path="$1"
    cat > "$path" <<'XML'
<clickhouse>
    <profiles>
        <default>
            <max_memory_usage>2000000000</max_memory_usage>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks>
                <ip>127.0.0.1</ip>
                <ip>::1</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>
    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>
XML
}

write_keeper_config() {
    local i="$1"
    local cfg="$BASE/keeper$i/keeper.xml"
    mkdir -p "$BASE/keeper$i/logs" "$BASE/keeper$i/snapshots"
    cat > "$cfg" <<XML
<clickhouse>
    <logger>
        <level>information</level>
        <log>$BASE/logs/keeper$i.log</log>
        <errorlog>$BASE/logs/keeper$i.err.log</errorlog>
        <console>1</console>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <keeper_server>
        <tcp_port>${KEEPER_CLIENT_PORTS[$((i-1))]}</tcp_port>
        <server_id>$i</server_id>
        <log_storage_path>$BASE/keeper$i/logs</log_storage_path>
        <snapshot_storage_path>$BASE/keeper$i/snapshots</snapshot_storage_path>
        <coordination_settings>
            <operation_timeout_ms>10000</operation_timeout_ms>
            <min_session_timeout_ms>10000</min_session_timeout_ms>
            <session_timeout_ms>100000</session_timeout_ms>
            <raft_logs_level>information</raft_logs_level>
            <compress_logs>false</compress_logs>
        </coordination_settings>
        <hostname_checks_enabled>false</hostname_checks_enabled>
        <raft_configuration>
            <server><id>1</id><hostname>127.0.0.1</hostname><port>${KEEPER_RAFT_PORTS[0]}</port></server>
            <server><id>2</id><hostname>127.0.0.1</hostname><port>${KEEPER_RAFT_PORTS[1]}</port></server>
            <server><id>3</id><hostname>127.0.0.1</hostname><port>${KEEPER_RAFT_PORTS[2]}</port></server>
        </raft_configuration>
    </keeper_server>
</clickhouse>
XML
}

write_clickhouse_config() {
    local i="$1"
    local replica="r$i"
    local cfg="$BASE/ch$i/config.xml"
    mkdir -p "$BASE/ch$i"/{data,tmp,user_files,format_schemas,user_defined,access}
    write_users_xml "$BASE/ch$i/users.xml"
    cat > "$cfg" <<XML
<clickhouse>
    <logger>
        <level>information</level>
        <log>$BASE/logs/ch$i.log</log>
        <errorlog>$BASE/logs/ch$i.err.log</errorlog>
        <console>1</console>
    </logger>
    <display_name>si-ch-$i</display_name>
    <listen_host>127.0.0.1</listen_host>
    <http_port>${CH_HTTP_PORTS[$((i-1))]}</http_port>
    <tcp_port>${CH_TCP_PORTS[$((i-1))]}</tcp_port>
    <interserver_http_host>127.0.0.1</interserver_http_host>
    <interserver_http_port>${CH_INTERSERVER_PORTS[$((i-1))]}</interserver_http_port>
    <path>$BASE/ch$i/data/</path>
    <tmp_path>$BASE/ch$i/tmp/</tmp_path>
    <user_files_path>$BASE/ch$i/user_files/</user_files_path>
    <format_schema_path>$BASE/ch$i/format_schemas/</format_schema_path>
    <user_defined_path>$BASE/ch$i/user_defined/</user_defined_path>
    <access_control_path>$BASE/ch$i/access/</access_control_path>
    <users_config>$BASE/ch$i/users.xml</users_config>
    <mark_cache_size>134217728</mark_cache_size>
    <uncompressed_cache_size>67108864</uncompressed_cache_size>
    <max_server_memory_usage>2147483648</max_server_memory_usage>
    <background_pool_size>4</background_pool_size>
    <background_schedule_pool_size>4</background_schedule_pool_size>
    <max_thread_pool_size>128</max_thread_pool_size>
    <merge_tree>
        <number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation>
        <number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
        <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
    </merge_tree>
    <zookeeper>
        <node><host>127.0.0.1</host><port>${KEEPER_CLIENT_PORTS[0]}</port></node>
        <node><host>127.0.0.1</host><port>${KEEPER_CLIENT_PORTS[1]}</port></node>
        <node><host>127.0.0.1</host><port>${KEEPER_CLIENT_PORTS[2]}</port></node>
    </zookeeper>
    <macros>
        <shard>01</shard>
        <replica>$replica</replica>
    </macros>
</clickhouse>
XML
}

write_server_housegate_config() {
    local i="$1"
    local hg_dir="$BASE/hg$i"
    mkdir -p "$hg_dir"
    cat > "$hg_dir/network_state.yaml" <<YAML
indexer_infos:
  1000:
    indexer_id: 1000
    indexer_url: "127.0.0.1"
    clickhouse_proxy_port: ${SERVER_HG_PORTS[$((i-1))]}
processor_allocations: {}
processor_infos: {}
database_infos:
  realbin:
    database_id: "realbin"
    db_type: 0
    indexer_id: 1000
    tables:
      - table_id: "t"
        table_type: "MergeTree"
database_permissions:
  "$AGENT_ADDR":
    realbin: ["read", "write", "admin"]
YAML
    cat > "$hg_dir/ckh_manager.yaml" <<YAML
pick_lb_strategy: "hash"
roles:
  admin: &admin_role
    username: default
    password: ""
credential:
  sentio: { <<: [*admin_role] }
  subgraph: { <<: [*admin_role] }
settings:
  max_memory_usage: 2000000000
shards:
  - index: 0
    name: "si-3x3"
    allow_tiers: [0, 1, 2, 3, 16]
    allow_projects: []
    allow_organizations: []
    addresses:
      internal_tcp_addr: 127.0.0.1:${CH_TCP_PORTS[$((i-1))]}
      internal_tcp_replicas: 127.0.0.1:${CH_TCP_PORTS[$((i-1))]}
      internal_tcp_proxy: 127.0.0.1:${SERVER_HG_PORTS[$((i-1))]}
      external_tcp_addr: 127.0.0.1:${CH_TCP_PORTS[$((i-1))]}
      external_tcp_replicas: 127.0.0.1:${CH_TCP_PORTS[$((i-1))]}
      external_tcp_proxy: 127.0.0.1:${SERVER_HG_PORTS[$((i-1))]}
YAML
    cat > "$hg_dir/housegate.yaml" <<YAML
listen: "127.0.0.1:${SERVER_HG_PORTS[$((i-1))]}"
upstream: "127.0.0.1:${CH_TCP_PORTS[$((i-1))]}"
metrics_listen: "127.0.0.1:${SERVER_HG_METRICS_PORTS[$((i-1))]}"
dial_timeout: "5s"
idle_timeout: "5m"
shutdown_timeout: "10s"
stats_interval: "10s"
streaming_buf_size: 131072
validate_checksum: false
indexer_id: 1000
credential_replace_enabled: false
ckh_manager_config_path: "$hg_dir/ckh_manager.yaml"
network_state:
  source: "$hg_dir/network_state.yaml"
auth:
  enabled: true
  allowed_addresses:
    - "$AGENT_ADDR"
  max_token_age: "5m"
  allow_no_auth: false
rewriter:
  service_addr: "127.0.0.1:$SQL_REWRITER_PORT"
  timeout: "3s"
  physical_database: "realbin"
observability:
  collector:
    enabled: false
  pprof:
    enabled: false
usage:
  enabled: false
concurrency_limit:
  enabled: false
storage_integrity:
  enabled: true
  mock_payload_store:
    path: "$BASE/payloads"
  mock_finality:
    delay: "500ms"
  mock_part_registry:
    partition_ids:
      - "202606"
  housekeeper:
    endpoints:
      - "127.0.0.1:${KEEPER_CLIENT_PORTS[0]}"
      - "127.0.0.1:${KEEPER_CLIENT_PORTS[1]}"
      - "127.0.0.1:${KEEPER_CLIENT_PORTS[2]}"
    root: "/housekeeper/v1/storage_integrity"
    worker_id: "r$i"
    replay_quorum: 2
    session_timeout: "60s"
  unsafe_validation:
    query_timeout: "60s"
    replicas:
      - replica_id: "r1"
        addr: "127.0.0.1:${CH_TCP_PORTS[0]}"
      - replica_id: "r2"
        addr: "127.0.0.1:${CH_TCP_PORTS[1]}"
      - replica_id: "r3"
        addr: "127.0.0.1:${CH_TCP_PORTS[2]}"
  safe_audit:
    replicas:
      - replica_id: "r1"
      - replica_id: "r2"
      - replica_id: "r3"
    network_id: "mock-3x3"
    schema_hash: "mock-schema"
  workers:
    poll_interval: "1s"
    replay: $HG_WORKER_REPLAY
    unsafe_validation: $HG_WORKER_UNSAFE_VALIDATION
    promotion: $HG_WORKER_PROMOTION
    rollback: $HG_WORKER_ROLLBACK
    safe_audit: $HG_WORKER_SAFE_AUDIT
    finality: $HG_WORKER_FINALITY
YAML
}

write_client_housegate_config() {
    local hg_dir="$BASE/hg-client"
    mkdir -p "$hg_dir"
    cat > "$hg_dir/housegate.yaml" <<YAML
listen: "127.0.0.1:$CLIENT_HG_PORT"
metrics_listen: "127.0.0.1:$CLIENT_HG_METRICS_PORT"
dial_timeout: "5s"
idle_timeout: "5m"
shutdown_timeout: "10s"
stats_interval: "10s"
streaming_buf_size: 131072
validate_checksum: false
agent:
  mode: true
  upstream: "127.0.0.1:${SERVER_HG_PORTS[0]}"
  private_key_hex: "$AGENT_KEY"
  storage_integrity:
    enabled: true
    network_id: "mock-3x3"
observability:
  collector:
    enabled: false
  pprof:
    enabled: false
usage:
  enabled: false
concurrency_limit:
  enabled: false
YAML
}

prepare_storage_integrity_e2e() {
    require_file "$CLICKHOUSE_BIN"
    require_file "$HOUSEGATE_BAZEL_BIN"
    mkdir -p "$WORK/e2e-bin"
    cp -f "$(readlink -f "$HOUSEGATE_BAZEL_BIN")" "$HOUSEGATE_BIN"
    chmod +x "$HOUSEGATE_BIN"
    check_ports_free "${KEEPER_CLIENT_PORTS[@]}" "${KEEPER_RAFT_PORTS[@]}" "${CH_TCP_PORTS[@]}" "${CH_HTTP_PORTS[@]}" "${CH_INTERSERVER_PORTS[@]}" "${SERVER_HG_PORTS[@]}" "${SERVER_HG_METRICS_PORTS[@]}" "$CLIENT_HG_PORT" "$CLIENT_HG_METRICS_PORT" "$SQL_REWRITER_PORT"
    mkdir -p "$BASE/logs" "$BASE/payloads"

    log "base=$BASE"
    log "docker_image=$IMAGE"
    log "sql_rewriter_image=$SQL_REWRITER_IMAGE"
    log "agent_addr=$AGENT_ADDR"
}

create_storage_integrity_schema() {
    local schema_sql='
CREATE DATABASE IF NOT EXISTS realbin;
CREATE DATABASE IF NOT EXISTS hg_unsafe;
CREATE DATABASE IF NOT EXISTS hg_safe;
DROP TABLE IF EXISTS realbin.t;
CREATE TABLE IF NOT EXISTS hg_unsafe.`realbin.t_a` (_hg_row_id FixedString(32), id UInt64, v String, created_at DateTime, r UInt64) ENGINE = ReplicatedMergeTree('\''/clickhouse/tables/si3x3rw/hg_unsafe/realbin_t_a'\'', '\''{replica}'\'') PARTITION BY toYYYYMM(created_at) ORDER BY (_hg_row_id, id);
CREATE TABLE IF NOT EXISTS hg_safe.`realbin.t` (_hg_row_id FixedString(32), id UInt64, v String, created_at DateTime, r UInt64) ENGINE = MergeTree PARTITION BY toYYYYMM(created_at) ORDER BY (_hg_row_id, id);
'
    for port in "${CH_TCP_PORTS[@]}"; do
        ch_multiquery "$port" "$schema_sql"
        wait_for_query_value "realbin.t absent on ch:$port" "$port" "EXISTS TABLE realbin.t" "0" 30
    done
}

start_storage_integrity_topology() {
    prepare_storage_integrity_e2e

    start_sql_rewriter_container "si3x3rw-$RUN_ID-rewriter"
    wait_for_tcp "sql-rewriter:$SQL_REWRITER_PORT" "$SQL_REWRITER_PORT"

    for i in 1 2 3; do
        write_keeper_config "$i"
        start_container "si3x3rw-$RUN_ID-keeper$i" "$BASE/logs/keeper$i.stdout" "$CLICKHOUSE_BIN keeper --config-file=$BASE/keeper$i/keeper.xml"
    done

    for port in "${KEEPER_CLIENT_PORTS[@]}"; do
        wait_for_cmd "keeper:$port" 90 keeper_query "$port" "ls /" || {
            for i in 1 2 3; do tail -120 "$BASE/logs/keeper$i.stdout" || true; done
            die "keeper quorum did not become ready"
        }
    done

    for i in 1 2 3; do
        write_clickhouse_config "$i"
        start_container "si3x3rw-$RUN_ID-ch$i" "$BASE/logs/ch$i.stdout" "$CLICKHOUSE_BIN server --config-file=$BASE/ch$i/config.xml"
    done

    for port in "${CH_TCP_PORTS[@]}"; do
        wait_for_cmd "clickhouse:$port" 120 ch_query "$port" "SELECT 1" || {
            for i in 1 2 3; do tail -160 "$BASE/logs/ch$i.stdout" || true; done
            die "clickhouse servers did not become ready"
        }
    done

    create_storage_integrity_schema

    for i in 1 2 3; do
        write_server_housegate_config "$i"
        start_container "si3x3rw-$RUN_ID-hg$i" "$BASE/logs/hg$i.stdout" "$HOUSEGATE_BIN -config $BASE/hg$i/housegate.yaml -log-level debug"
    done

    for port in "${SERVER_HG_PORTS[@]}"; do
        wait_for_tcp "server-housegate:$port" "$port" || {
            for i in 1 2 3; do tail -180 "$BASE/logs/hg$i.stdout" || true; done
            die "server housegate ports did not open"
        }
    done

    write_client_housegate_config
    start_container "si3x3rw-$RUN_ID-hg-client" "$BASE/logs/hg-client.stdout" "$HOUSEGATE_BIN -config $BASE/hg-client/housegate.yaml -log-level debug"
    wait_for_cmd "client-housegate:$CLIENT_HG_PORT signed path" 90 ch_query "$CLIENT_HG_PORT" "SELECT 1" || {
        log "client signed path diagnostic output follows"
        ch_query "$CLIENT_HG_PORT" "SELECT 1" 2>&1 || true
        tail -180 "$BASE/logs/hg-client.stdout" || true
        for i in 1 2 3; do tail -180 "$BASE/logs/hg$i.stdout" || true; done
        docker logs "$SQL_REWRITER_CONTAINER" 2>&1 | tail -180 || true
        die "client housegate signed path did not become ready"
    }
}

send_storage_integrity_insert() {
    log "sending raw client insert through client-side housegate"
    ch_query "$CLIENT_HG_PORT" "$E2E_INSERT_QUERY"

    for port in "${CH_TCP_PORTS[@]}"; do
        wait_for_query_value "unsafe count on ch:$port" "$port" "SELECT count() FROM hg_unsafe.\`realbin.t_a\`" "2" 120
    done
}

first_storage_integrity_statement() {
    local statements
    statements="$(wait_for_keeper_children "keeper statements" "/housekeeper/v1/storage_integrity/statements" 120)" || return 1
    printf '%s\n' "${statements%% *}"
}

run_storage_integrity_happy_path() {
    start_storage_integrity_topology
    send_storage_integrity_insert

    local statement_id
    statement_id="$(first_storage_integrity_statement)" || die "no statement ledger found"
    wait_for_keeper_children_contains "keeper replay attestations" "/housekeeper/v1/storage_integrity/attestations/$statement_id" 180 r1 r2 r3
    wait_for_keeper_data_contains "keeper replay decision" "/housekeeper/v1/storage_integrity/decisions/$statement_id" "replay_quorum_met=true" 180
    wait_for_keeper_children "keeper unsafe_results" "/housekeeper/v1/storage_integrity/unsafe_results" 180 >/dev/null || die "no unsafe result ledger found"

    log "submitting mock finality for statement=$statement_id after unsafe validation"
    submit_mock_finality "$statement_id" || die "mock finality submit failed"

    for port in "${CH_TCP_PORTS[@]}"; do
        wait_for_query_value "safe count on ch:$port" "$port" "SELECT count() FROM hg_safe.\`realbin.t\`" "2" 180
    done

    local promotion_participants
    promotion_participants="$(wait_for_keeper_children "keeper participant promotion_results" "/housekeeper/v1/storage_integrity/promotion_results/$statement_id" 180)" || die "no participant promotion result ledger found"
    for participant in r1 r2 r3; do
        [[ "$promotion_participants" == *"$participant"* ]] || die "promotion results missing participant $participant: $promotion_participants"
    done

    local audit_id="audit-$statement_id"
    wait_for_keeper_path "keeper safeAudit task" "/housekeeper/v1/safe_audits/tasks/$audit_id" 180
    wait_for_keeper_children_contains "keeper safeAudit votes" "/housekeeper/v1/safe_audits/votes/$audit_id" 180 r1 r2 r3
    wait_for_keeper_data_contains "keeper safeAudit decision" "/housekeeper/v1/safe_audits/decisions/$audit_id" "status=majority" 180

    wait_for_query_value "client SELECT routes to safe" "$CLIENT_HG_PORT" "SELECT concat(toString(count()), ':', groupArray(v)[1]) FROM realbin.t" "2:a" 60

    local statements finality promotions promotion_results unsafe_results attestations replay_decision safe_audit_votes safe_audit_decision
    statements="$(keeper_ls "/housekeeper/v1/storage_integrity/statements" | tr -d '\r' | tail -n 1 || true)"
    finality="$(keeper_ls "/housekeeper/v1/storage_integrity/finality" | tr -d '\r' | tail -n 1 || true)"
    promotions="$(keeper_ls "/housekeeper/v1/storage_integrity/promotions" | tr -d '\r' | tail -n 1 || true)"
    promotion_results="$(keeper_ls "/housekeeper/v1/storage_integrity/promotion_results" | tr -d '\r' | tail -n 1 || true)"
    unsafe_results="$(keeper_ls "/housekeeper/v1/storage_integrity/unsafe_results" | tr -d '\r' | tail -n 1 || true)"
    attestations="$(keeper_ls "/housekeeper/v1/storage_integrity/attestations/$statement_id" | tr -d '\r' | tail -n 1 || true)"
    replay_decision="$(keeper_get "/housekeeper/v1/storage_integrity/decisions/$statement_id" | tr -d '\r' || true)"
    safe_audit_votes="$(keeper_ls "/housekeeper/v1/safe_audits/votes/$audit_id" | tr -d '\r' | tail -n 1 || true)"
    safe_audit_decision="$(keeper_get "/housekeeper/v1/safe_audits/decisions/$audit_id" | tr -d '\r' || true)"

    log "keeper statements=$statements"
    log "keeper attestations/$statement_id=$attestations"
    log "keeper replay_decision=$replay_decision"
    log "keeper unsafe_results=$unsafe_results"
    log "keeper finality=$finality"
    log "keeper promotions=$promotions"
    log "keeper promotion_results=$promotion_results"
    log "keeper safe_audit_votes/$audit_id=$safe_audit_votes"
    log "keeper safe_audit_decision=$safe_audit_decision"
    [[ -n "$statements" && "$statements" != "[]" ]] || die "no statement ledger found"
    [[ -n "$attestations" && "$attestations" != "[]" ]] || die "no replay attestation ledger found"
    [[ -n "$unsafe_results" && "$unsafe_results" != "[]" ]] || die "no unsafe result ledger found"
    [[ -n "$finality" && "$finality" != "[]" ]] || die "no finality ledger found"
    [[ -n "$promotion_results" && "$promotion_results" != "[]" ]] || die "no promotion result ledger found"
    [[ -n "$safe_audit_votes" && "$safe_audit_votes" != "[]" ]] || die "no safeAudit vote ledger found"

    log "E2E PASS: client HG signed/materialized INSERT -> server HG sql-rewriter -> real replay quorum -> unsafe quorum -> finality -> attach-partition promotion -> real safeAudit -> safe SELECT"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    run_storage_integrity_happy_path
fi
