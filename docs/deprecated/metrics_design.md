# Metrics Implementation Design

## Overview
This document outlines the design for adding Prometheus metrics to the ClickHouse Proxy. This enables monitoring of traffic, connection counts, and error rates via Grafana.

## Metrics Designed

The following metrics will be exposed at the `/metrics` endpoint:

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `clickhouse_proxy_active_connections` | Gauge | None | Number of active client connections. |
| `clickhouse_proxy_packets_total` | Counter | `type` (Query, Data, Hello, etc.) | Count of ClickHouse packets processed. |
| `clickhouse_proxy_bytes_transferred_total` | Counter | `direction` (client_to_upstream, upstream_to_client) | Total bytes forwarded. |

## Implementation Details

### Configuration
A new configuration field `metrics_listen` will be added to `config.json` (env `CK_METRICS_LISTEN`), defaulting to `:9091`.

### Code Architecture
- **Main**: Starts a separate HTTP server for `promhttp.Handler`.
- **Proxy**:
    - Increment/Decrement connection gauge on `handleConnection`.
    - Count packets in packet detection loop.
    - Count bytes in copy loops.

### Kubernetes Integration
- The deployment/statefulset should expose port `9091`.
- Annotations for Prometheus scraping:
  ```yaml
  prometheus.io/scrape: "true"
  prometheus.io/port: "9091"
  ```
