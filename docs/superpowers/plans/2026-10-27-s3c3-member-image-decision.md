# Plan S3-C3 member image: sentio-node on arbiter-core v0.18.1 (user decision, 2026-10-27)

Plan S3-C3 (`2026-10-15-multi-source-si-s3c3-p2-hosts-rollout.md`) runs its si-members on the image Plan S3-C2 Task 35 recorded as `SN_COMMIT_P2` / `SN_DIGEST_P2`. That image pins arbiter-core `v0.18.0` (`CORE_TAG_P2`). While Plan S3-C2 executed, three si-member gaps were found and parked as must-fix before devnet2; they ship in arbiter-core `v0.18.1` (sentioxyz/arbiter-core#55), and sentio-node pins it in sentioxyz/sentio-node#202. The user decided in chat on 2026-10-27 that Plan S3-C3 deploys the image of that pin instead.

## Decision

Wherever Plan S3-C3 reads `SN_COMMIT_P2` or `SN_DIGEST_P2` (Task 38's ledger inputs, the image in the devnet2 overlays and `test_render.py`, the registry digest check, the runbook and every later check of the deployed member image), it uses the values below instead. Every other ledger value it consumes is unchanged.

| Plan S3-C3 input | Value used | Source |
|---|---|---|
| `SN_COMMIT_P2` | `6ec59b933da367399fe2187462f221f877e5698d` | sentioxyz/sentio-node#202 squash-merged on 2026-10-27 UTC |
| `SN_DIGEST_P2` | `sha256:8954a338ea2fae62e0d492844bf6e78ecd162837a52ae24d1d36c6389188d525` | index digest of `ghcr.io/sentioxyz/sentio-node:sha-6ec59b933da367399fe2187462f221f877e5698d` |

Plan S3-C2's Appendix C is not changed: `SN_COMMIT_P2` / `SN_DIGEST_P2` stay recorded as the image Task 35 released (`8652bc0`, `sha256:42a4c1f0…`), and `CORE_TAG_P2` stays `v0.18.0`. This document is not a ledger row, so `s3-ledger.py get` still prints the recorded values; a Plan S3-C3 reader replaces them with the values above.

## What changed between the two images

- sentio-node `6ec59b9` differs from `8652bc0` only in `go.mod`, `go.sum` and `MODULE.bazel`: arbiter-core `v0.18.0` → `v0.18.1` (git_override commit `f5225822` → `dddaf7df`); housegate stays `v0.22.0` and arbiter-proto `v0.13.0`.
- arbiter-core `v0.18.1` (`dddaf7df1b2429ec6a3d1421a7358cdb1cc4c4c4`, Cut Release run 40507306296, whose member confinement step printed `confinement: 14 test(s) passed on clickhouse/clickhouse-server:26.3.37.3` and `sources: 13 ClickHouse test(s) passed on clickhouse/clickhouse-server:26.3.37.3`) adds three fixes and no consensus change:
  - every byte-source fetch and restore has a bounded deadline; expiry is outcome `refused`, and the installer moves to the next source (spec §8.6, K35);
  - recreating a vanished copy invalidates the installer's audit coverage, so `/readyz` does not report ready on an empty copy;
  - the table registry's one-live-incarnation-per-key invariant is checked.

## Evidence

- sentioxyz/sentio-node#202: CI `build` and `integration-clickhouse` (with `si-member install acceptance`) passed; one commit, authored and signed as `poetry <chen@sentio.xyz>`.
- `CI Build` run 40518551376 on `6ec59b9`: `build`, `integration-clickhouse` and `docker-push` succeeded, and its log carries `--- PASS: TestSIMemberInstallsACopyFromTheOwnerPull`.
- `ghcr.io/sentioxyz/sentio-node:sha-6ec59b933da367399fe2187462f221f877e5698d`: OCI image index `sha256:8954a338ea2fae62e0d492844bf6e78ecd162837a52ae24d1d36c6389188d525` with a `linux/amd64` image whose `org.opencontainers.image.revision` label is `6ec59b933da367399fe2187462f221f877e5698d`.
- The P2 deployed-image confinement run (`P2_CONFINEMENT`, 2026-10-27) measured the member users on the deployed ClickHouse build, not the member binary, so it still applies.
