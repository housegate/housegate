#!/usr/bin/env bash
# Experiment B: end-to-end latency. Inject rows tagged with wall-clock
# time on source; in target, measure now64() - inject_ts.
set -euo pipefail
cd "$(dirname "$0")/../../.."
ANCHOR_ADDR="${ANCHOR_ADDR:?set ANCHOR_ADDR}"
CELESTIA_TOKEN="${CELESTIA_TOKEN:?set CELESTIA_TOKEN}"

# Setup
clickhouse-client --port 19100 < tools/da-mvp/scripts/setup-experiment-schema.sql
clickhouse-client --port 19200 < tools/da-mvp/scripts/setup-experiment-schema.sql

# Publisher in --interval 5s for tightest latency
bazel run //tools/da-mvp/cmd/da-publisher -- \
  --source-ch-port 19100 --database exp --table events \
  --anchor-rpc http://localhost:8545 --anchor-contract "$ANCHOR_ADDR" \
  --anchor-private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --celestia-token "$CELESTIA_TOKEN" \
  --interval 5s --checkpoint-file /tmp/damvp-pub.state \
  --metrics-listen :9100 > /tmp/damvp-pub-b.log 2>&1 &
PUB_PID=$!

# Rebuilder in --follow with poll 2s
bazel run //tools/da-mvp/cmd/da-rebuilder -- \
  --target-ch-port 19200 --database exp --table events \
  --anchor-contract "$ANCHOR_ADDR" \
  --celestia-token "$CELESTIA_TOKEN" \
  --follow --poll 2s > /tmp/damvp-reb-b.log 2>&1 &
REB_PID=$!

# Loadgen at moderate rate
bazel run //tools/da-mvp/cmd/loadgen -- --port 19100 --rate 1000 --duration 30m

sleep 60  # let last anchors drain

# Compute percentiles on target
clickhouse-client --port 19200 --query "
  SELECT
    quantile(0.50)(now64(6) - inject_ts) AS p50,
    quantile(0.95)(now64(6) - inject_ts) AS p95,
    quantile(0.99)(now64(6) - inject_ts) AS p99
  FROM exp.events
  FORMAT TabSeparatedWithNames"

kill "$PUB_PID" "$REB_PID" 2>/dev/null || true
