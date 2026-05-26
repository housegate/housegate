# DA-Layer Data Sync MVP

A two-week measurement prototype for the DA-layer reliability path. See
[`../../docs/superpowers/specs/2026-05-25-da-mvp-design.md`](../../docs/superpowers/specs/2026-05-25-da-mvp-design.md)
for the design.

## Runtime dependencies

- `clickhouse-client` 24.x or newer on PATH (the publisher and rebuilder
  shell out to it for Parquet export/import).
- A Celestia light node connected to Mocha-4 testnet. Quick start:
  `celestia light start --p2p.network mocha`.
- `anvil` (Foundry) for the local anchor chain. Quick start:
  `anvil --block-time 1`.
- `forge` (Foundry) for compiling and testing the Solidity contract.

## Build

```
bazel build //tools/da-mvp/cmd/da-publisher //tools/da-mvp/cmd/da-rebuilder
```

## Quick smoke test (Day 1)

```
bazel run //tools/da-mvp/cmd/pfb-demo -- \
  --celestia-rpc http://localhost:26658 \
  --celestia-token "$(cat ~/.celestia-light-mocha-4/keys/auth.token)" \
  --namespace 0x68676d76000000000001
```

Should print the submitted blob's height and confirm it was readable back.

## Running an experiment

See `scripts/run-experiment-a.sh` (throughput), `-b.sh` (latency),
`-c.sh` (cost). Each prints a single CSV row that the report harvests.
