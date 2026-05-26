# DA-Layer MVP — Measurement Report

**Date:** 2026-XX-XX
**Status:** Draft → Final
**Authors:** [engineer name], Claude
**Spec:** [`2026-05-25-da-mvp-design.md`](./2026-05-25-da-mvp-design.md)

## TL;DR

> [1–2 sentences. Final go/no-go recommendation.]

## 1. Setup

| Component | Version | Notes |
|---|---|---|
| ClickHouse (source) | 25.8 | docker-compose port 19100 |
| ClickHouse (target) | 25.8 | docker-compose port 19200 |
| Celestia light node | v0.X.Y | Mocha-4 testnet |
| Anvil | from Foundry 1.7.1 | block_time=1s |
| DAAnchor.sol | commit `XXXXXXX` | deployed at `0xYYYY` |
| Loadgen | commit `XXXXXXX` | |

## 2. Experiment A — Throughput

Run via `scripts/run-experiment-a.sh` with
`DURATION_PER_RATE=2h RATES="100 1000 10000 100000"`.

| Rate (rows/s) | Sustained MB/s | Final lag (s) | Total blobs | Mocha block utilization (%) |
|---|---|---|---|---|
| 100 | | | | |
| 1,000 | | | | |
| 10,000 | | | | |
| 100,000 | | | | |

Findings:
- [bullet 1]
- [bullet 2]

## 3. Experiment B — Latency

Run via `scripts/run-experiment-b.sh` at 1k rows/s for 30 minutes.

| Stage | Median (s) | P95 (s) | P99 (s) |
|---|---|---|---|
| Source part flush | | | |
| Publisher poll cycle | | | |
| Celestia commit | | | |
| Rebuilder fetch+INSERT | | | |
| **End-to-end** | | | |

Findings:
- [bullets]

## 4. Experiment C — Cost

Run via `scripts/run-experiment-c.sh`.

| Quantity | Value |
|---|---|
| Bytes published | |
| GB published | |
| Testnet TIA spent | |
| Anvil tx count | |
| Mainnet TIA price (spot) | |
| Extrapolated $/GB | |
| vs. S3 baseline ($0.023/GB-mo) | |

Findings:
- [bullets]

## 5. Go / No-Go Decision

Following the decision tree from spec §8:

- **Q1 (publisher keeps up at 100k rows/s):** [yes/no, evidence row]
- **Q2 ($/GB within 10× S3):** [yes/no, evidence row]
- **Q3 (P99 latency < 5 min):** [yes/no, evidence row]

**Recommendation:** [one of:]
- DA as primary data plane (all three yes).
- DA as commitment-only auxiliary (any Q1/Q2 no).
- DA as commitment + selected hot-DB full publish (Q3 no, Q1/Q2 yes).
- Abandon DA path; pursue Keeper + RMT.

## 6. Surprises & Caveats

- [things we didn't predict]
- [confounders in the measurement]

## 7. Recommended Next Steps

- [if positive: list of post-MVP roadmap items from spec §10]
- [if negative: pivot plan referencing the Keeper + RMT alternative]
