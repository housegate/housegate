#!/usr/bin/env bash
# Experiment C: cost per GB. Publish ~100 GB and capture TIA consumed
# (from light node CLI) and gas tx count (from anvil).
set -euo pipefail
cd "$(dirname "$0")/../../.."
ANCHOR_ADDR="${ANCHOR_ADDR:?set ANCHOR_ADDR}"
CELESTIA_TOKEN="${CELESTIA_TOKEN:?set CELESTIA_TOKEN}"

clickhouse-client --port 19100 < tools/da-mvp/scripts/setup-experiment-schema.sql

# Note TIA balance before
TIA_BEFORE=$(celestia state balance | awk '{print $2}')

bazel run //tools/da-mvp/cmd/da-publisher -- \
  --source-ch-port 19100 --database exp --table events \
  --anchor-rpc http://localhost:8545 --anchor-contract "$ANCHOR_ADDR" \
  --anchor-private-key 0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80 \
  --celestia-token "$CELESTIA_TOKEN" \
  --interval 30s --checkpoint-file /tmp/damvp-pub.state \
  --metrics-listen :9100 > /tmp/damvp-pub-c.log 2>&1 &
PUB_PID=$!

# Loadgen for ~100 GB. At ~120 bytes/row in TSV → ~9e8 rows ≈ 100 GB.
bazel run //tools/da-mvp/cmd/loadgen -- --port 19100 --rate 5000 --duration 24h

TIA_AFTER=$(celestia state balance | awk '{print $2}')
BYTES=$(tools/da-mvp/scripts/collect-metrics.sh housegate_da_publisher_bytes_published_total)
GB=$(echo "scale=4; $BYTES / 1024 / 1024 / 1024" | bc)
SPENT=$(echo "scale=6; $TIA_BEFORE - $TIA_AFTER" | bc)

echo "bytes_published=$BYTES"
echo "gb_published=$GB"
echo "tia_spent=$SPENT"

kill "$PUB_PID" 2>/dev/null || true
