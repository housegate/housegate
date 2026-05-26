#!/usr/bin/env bash
# Usage: collect-metrics.sh <metric-name> [host:port]
# Scrapes /metrics from the publisher and prints the value of the named metric.
set -euo pipefail
METRIC="$1"
ADDR="${2:-localhost:9100}"
curl -s "http://$ADDR/metrics" | grep -E "^$METRIC " | awk '{print $2}'
