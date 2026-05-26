#!/usr/bin/env bash
# Experiment A: throughput. For each rate in RATES, run loadgen +
# publisher for $DURATION_PER_RATE, scrape publish_lag_seconds and
# bytes_published_total, and print one CSV row per rate.
set -euo pipefail
cd "$(dirname "$0")/../../.."
ANCHOR_ADDR="${ANCHOR_ADDR:?set ANCHOR_ADDR}"
CELESTIA_TOKEN="${CELESTIA_TOKEN:?set CELESTIA_TOKEN}"
DURATION_PER_RATE="${DURATION_PER_RATE:-2h}"
RATES="${RATES:-100 1000 10000 100000}"

# Ensure schema is fresh
clickhouse-client --port 19100 < tools/da-mvp/scripts/setup-experiment-schema.sql

echo "rate_rows_per_s,sustained_mb_per_s,final_lag_seconds,total_blobs"
for R in $RATES; do
  # Start publisher in bg
  rm -f /tmp/damvp-pub.state
  bazel run //tools/da-mvp/cmd/da-publisher -- \
    --source-ch-port 19100 --database exp --table events \
    --anchor-rpc http://localhost:8545 --anchor-contract "$ANCHOR_ADDR" \
    --anchor-private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
    --celestia-token "$CELESTIA_TOKEN" \
    --interval 30s --checkpoint-file /tmp/damvp-pub.state \
    --metrics-listen :9100 > /tmp/damvp-pub-r$R.log 2>&1 &
  PUB_PID=$!
  # Start loadgen
  bazel run //tools/da-mvp/cmd/loadgen -- \
    --port 19100 --rate "$R" --duration "$DURATION_PER_RATE" > /tmp/damvp-gen-r$R.log 2>&1
  # Capture final metrics
  BYTES=$(tools/da-mvp/scripts/collect-metrics.sh housegate_da_publisher_bytes_published_total)
  LAG=$(tools/da-mvp/scripts/collect-metrics.sh housegate_da_publisher_publish_lag_seconds)
  BLOBS=$(tools/da-mvp/scripts/collect-metrics.sh housegate_da_publisher_blobs_published_total)
  kill "$PUB_PID" 2>/dev/null || true
  MB=$(echo "scale=2; $BYTES / 1024 / 1024" | bc)
  echo "$R,$MB,$LAG,$BLOBS"
done
