# Plan S3-C3 member image: sentio-node on arbiter-core v0.18.1 (user decision, 2026-10-27)

Plan S3-C3 (`2026-10-15-multi-source-si-s3c3-p2-hosts-rollout.md`) was written to run its si-members on the image Plan S3-C2 Task 35 recorded as `SN_COMMIT_P2` / `SN_DIGEST_P2`. That image pins arbiter-core `v0.18.0` (`CORE_TAG_P2`). While Plan S3-C2 executed, three si-member gaps were found and parked as must-fix before devnet2; they ship in arbiter-core `v0.18.1` (sentioxyz/arbiter-core#55), and sentio-node pins it in sentioxyz/sentio-node#202. The user decided in chat on 2026-10-27 that Plan S3-C3 deploys the image of that pin instead.

## Decision

Plan S3-C3 deploys sentio-node `6ec59b9`. The values are three rows of Plan S3-C3's own Appendix C, filled with this decision. No task records them and no task changes them (D23):

| Ledger name (Plan S3-C3 Appendix C) | Value | Pattern (`s3-ledger.py` suffix) | Source |
|---|---|---|---|
| `CORE_MEMBER_TAG_P2` | `v0.18.1` | `_TAG`: `vX.Y.Z` | arbiter-core `dddaf7df1b2429ec6a3d1421a7358cdb1cc4c4c4` (sentioxyz/arbiter-core#55), the version `6ec59b9`'s `go.mod` pins |
| `SN_MEMBER_COMMIT_P2` | `6ec59b933da367399fe2187462f221f877e5698d` | `_COMMIT`: 40 hex | sentioxyz/sentio-node#202 squash-merged on 2026-10-27 UTC |
| `SN_MEMBER_DIGEST_P2` | `sha256:8954a338ea2fae62e0d492844bf6e78ecd162837a52ae24d1d36c6389188d525` | `_DIGEST`: `sha256:` + 64 hex | index digest of `ghcr.io/sentioxyz/sentio-node:sha-6ec59b933da367399fe2187462f221f877e5698d` |

The rows have the form `` | `NAME` | `value` | Task 37 | ``, because a ledger row has to name a task. Task 37 runs no `s3-ledger.py set` and has no run-record item, so `set` never fills or changes these rows (it refuses a recorded row anyway). `s3-ledger.py get <housegate> CORE_MEMBER_TAG_P2 SN_MEMBER_COMMIT_P2 SN_MEMBER_DIGEST_P2` reads them from Plan S3-C3's ledger copy (housegate `main`), as the plan's Global Constraints require. The `empty` checks of Tasks 39, 40 and 41 (`P2_PR MEMBER_IDS`, `ENGINE_CENSUS P2_RESTART_END UNSAFE_INC_ROOT P2_END`, `P2_END`) name other rows and still pass.

Every place in Plan S3-C3 that reads, substitutes, validates or describes the member image now uses `SN_MEMBER_COMMIT_P2` / `SN_MEMBER_DIGEST_P2` in place of `SN_COMMIT_P2` / `SN_DIGEST_P2`:

- Task 38: Step 1's `get` and `inputs38.py`, which read the ledger, check the registry digest and persist the values; the placeholders `<SN_MEMBER_COMMIT_P2>` / `<SN_MEMBER_DIGEST_P2>` in `DEVNET2_MEMBER_IMAGE` (`test_render.py`) and in the three host overlays; the `sed` and placeholder checks of Steps 2 and 5; and Step 7's strict-loader checkout of the released commit.
- Task 39: Step 1's `get` and the `DEVNET2_MEMBER_IMAGE` assertion; runbook §0 `si_p2_image`; and §1.1, which covers the pin table, the placeholders of block `p2-1-1-1`, its `get`, its ledger-equality loop, the registry digest check, the `test_render.py` assertion and lines 6–7 of `$si_backup/pins`. Step 5 fills the new placeholders.
- Task 40: Step 2's `get` into `ledger.env`, `t40_pins_lines`, and `prepare.py`'s fill of the `p2-1-1-1` pin line.
- Task 41: Step 1's `get` into `ledger-41.env`, `t41_image` (render, pod and state checks), and the record's `ledger_values` and image lines.
- Contract §25.1, §26 and §28.1/§28.6, and Appendix C.

The member's arbiter-core version is `CORE_MEMBER_TAG_P2`. Runbook §1.1 states it in the pin table. Block `p2-1-1-1` checks that the tag exists in arbiter-core and that `$SN_MEMBER_COMMIT_P2:go.mod` pins exactly `$CORE_MEMBER_TAG_P2`. `CORE_TAG_P2` (`v0.18.0`) stays the build base of the throwaway `partstates` and `blockowners` tools: line 5 of `$si_backup/pins`, `si_blockowners_build`, Task 39 Step 5 and Task 40's `p2-1-3-2`. Those tools talk only to the voters' `SafeState` API, and `v0.18.1` does not change it. `$si_backup/pins` keeps its eight lines.

Plan S3-C2's Appendix C is not changed. `SN_COMMIT_P2` / `SN_DIGEST_P2` stay recorded as the image Task 35 released (`8652bc0…`, `sha256:42a4c1f0…`) and `CORE_TAG_P2` stays `v0.18.0`. `s3-ledger.py get` still prints those recorded values. Plan S3-C3 no longer reads `SN_COMMIT_P2` / `SN_DIGEST_P2` and keeps them only as history in its prerequisites and §28.6.

Later plans that read `SN_COMMIT_P2` / `SN_DIGEST_P2` as the deployed member image are not changed by this decision. They have to be amended before they run: Plan S3-C4 uses them as the P2 member image and rollback target (Task 48 `p2_image`, `p3_p2_image` and the record's rollback line) and as Task 46's sentio-node base (`6ec59b9` descends from `8652bc0`, so that ancestry check still holds).

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
