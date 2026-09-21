# Signed INSERT ... SELECT — Status Review and TODO Plan (2026-09-20)

> **For agentic workers:** this is a status review plus an ordered TODO list for the remaining Tier 3 work of [issue #153](https://github.com/housegate/housegate/issues/153). It is not itself an executable step-by-step plan: each TODO below names the component-plan task it completes, and the executor writes or refreshes that task's bite-sized steps (superpowers:writing-plans) before dispatching implementers (superpowers:subagent-driven-development). Every runtime gate stays default-off until D4 passes.

**Goal:** finish the signed `INSERT ... SELECT` lane described in the [design](../specs/2026-09-16-signed-insert-select-design.md), the [master plan](2026-09-16-signed-insert-select.md) and its four component plans ([A contracts](2026-09-16-signed-insert-select-contracts.md), [B replay](2026-09-16-signed-insert-select-replay.md), [C coordination](2026-09-16-signed-insert-select-coordination.md), [D integration](2026-09-16-signed-insert-select-integration.md)), starting from the state left by the two-day Codex implementation and the 2026-09-20 [remediation plan](2026-09-20-signed-insert-select-remediation.md).

**Method:** read-only gap analysis of every repository's `origin/main` on 2026-09-20 against the component plans, cross-checked with the sealed 2026-09-19 handoff (`PROGRESS.md` sections 3–4). File:line evidence in the tables refers to those tips.

## Baseline

| Repo | `origin/main` | Merged today (remediation) | Notes |
|---|---|---|---|
| HG housegate | `036be2e` | #172–#179 | remediation complete |
| AR arbiter | `a661482` | #84–#91 | pins HG `18340e7`, AC `ae1c54f` |
| AC arbiter-core | `a462498` | #30–#33 | pins HG `8729876`, AP `1ac937b` |
| AP arbiter-proto | `1ac937b` | — | current |
| RG rewriter-go | `4e4a14a` | #37 | latest tag v0.11.0 predates the engine |
| RC rewriter | `7567dc5` | #57, #58 | paired CI green again; latest tag v0.14.0 predates the engine |
| RP rewriter-proto | `d3844a5` | — | consistent across HG/RG/RC/manifest |
| SN sentio-node | `e633e00` | — | no snapshot-query code at all |

The remediation fixed correctness and consistency defects in what Codex had merged; it did not implement missing lifecycle pieces. Per the master plan's completion boundary, the current state is **code/PR completion for A and parts of B/C/D, unit-test CI only, no disposable-network acceptance, no activation**.

## 1. Status matrix (plan task → state at the 2026-09-20 tips)

| Task | State | Evidence | What remains |
|---|---|---|---|
| A1 wire records / canonical bytes | done | HG `pkg/replay/snapshotquery`, AC `wire/snapshot_query.go` (1379 lines), `wire/artifact_disposition.go` (12 actions) | nothing until D4's cross-version audit |
| A2 v3 sign / verify | done | HG `pkg/auth` v3, `snapshotquery.VerifyEnvelope` (#178), AC `authority/artifact_disposition.go` | consumers must keep using the same raw token bytes (D1/D2/C3) |
| A3 Analyze / Prepare RPCs | done | RP `d3844a5`; RG `snapshot_query.go`; RC `src/handlers/snapshot_query.cc` (1112 lines) | none |
| A4 closed AST + parity | done for the frozen corpus | 636/130 corpus byte-identical (`3d7c1f10…`); RC paired CI green on `7567dc5` | re-measure for every new role binary/FFI (by design); ordinary-class policy [rewriter-go#38](https://github.com/housegate/rewriter-go/issues/38); manifest `sources.hg` (`022ee95`) is not a main ancestor |
| A5 HG wrapper / probe | done | `pkg/rewriter/snapshot_query.go:52,83,262,388` | nothing configures `Options.SnapshotQueryProfilePath` (D3) |
| B1 artifact publication / retention | **missing** | AC `dataplane/snapshot_restore.go:27-34` interface only; no Publish / HGPART / checksum-zstd / registry code | everything |
| B2 scratch restore | partial | HG `pkg/replay/chexec/snapshot.go:47,291,318`; AC `dataplane/snapshot_restore.go:49,68` | row-level verification, grant-based isolation, docker ATTACH case (HG) |
| B3 canonical sort / output | done | HG `canonical.go`, `sort.go`, `output.go` | none |
| B4 executor / state assembly | mostly done | HG `executor.go:71,152,362`, `apply_rows.go:168` | export the authenticated `HistoricalPolicy` constructor; real `QueryUseAdmission` / `SnapshotReadStore` adapters |
| B5 receipts / historical dispatch | HG done; AC partial | HG `verifier.go:43,110,151`; AC `verifier/verifier.go:262,271`, `historical_snapshot_query_adapter.go:68` | AC concrete record source + trusted reference; funded verifier lifecycle client; default routes still nil |
| C1 profile / disposition / capacity | partial | AR `fsm/apply_artifact_disposition.go:39-55` (7 of 12 actions), receipts + quorum (#86) | 4 public actions; `Capability` setter; capacity ledger charging; Raft commands 26/27; `RecordSnapshotArtifactReady` |
| C2 fenced reservation | partial | AR drain/grant `artifact_reservation_drain.go:55,86,230`, `_grant.go:59` (#87, #91) | all three RPCs still refuse (`server/snapshot_query_control.go:48`); no Raft proposer; begin does not fence new SI admissions (`_drain.go:227`); no exported release seam |
| C3 singleton sequencing / abort | **missing** | `pb.UnimplementedArbiterIngressServer` answers Submit/Status/Abort | everything |
| C4 sequence-first intake | partial | HG `snapshot_query_intake.go:62,141,178`, phase port, journal v2 (#179) | `recoverRecord` has no `PreparedOutput` case; stages after Sequenced; `QuerySource` ports; ACK2/safe-ACK fields; compaction |
| C5 source stage / claim / fence | AR partial; AC **missing** | AR attestation + atomic projection + private gateway (injection-only); AC `snode/` has zero snapshot-query code | AR transports + barrier reader; AC whole source lifecycle |
| D1 agent obtain / finalize / sign | partial, unwired | HG `pkg/plugins/sisnapshotquery/plugin.go:328,594`, `relay_agent_prepare.go` | no `build.go` / config wiring; `SQL_x_snapshot_query_input` transport absent; ports do not revalidate the schema object |
| D2 host query-only admission | relay done; admission injection-only | HG `relay_query_only.go:48,170` (#171, #177, #179); `pkg/plugin/query_only_host.go:35` | production host plugin, `SnapshotQueryHostKey`, binding to intake `Submit` |
| D3 runtime / adapters | **missing** | no snapshot-query symbol in HG `build.go` / `pkg/config`; nothing in SN | config block, wiring, SN adapter, pins |
| D4 acceptance matrix / runbook | **missing** | no `pkg/integration/snapshotquery`, no CI target | multi-role cluster harness, A1–A14 evidence, release manifest, runbook |

Cross-cutting facts that shape the order below:

- **No Raft apply path reaches any snapshot-query state in AR.** The four `ApplyServerOwned*` seams and both `Propose*` interfaces have no production implementation; `fsm/fsm.go` dispatches only `cmd.ArtifactDisposition`, and `Capability` has no non-test setter, so every tag-28 command is rejected in a real deployment.
- **AR's snapshot format is v9 (accepting 3–9).** The coordination plan still says "version 2"; C2/C3 request-history and tombstones must be authored as v10. #87 added `DrainFrontierBlockSeq` inside v9 without a bump; a pre-#87 v9 snapshot carrying a draining record restores permanently ungrantable and release has no exported seam.
- **HG's intake recovery aborts on a staged output.** `SnapshotQueryStagePreparedOutput` is in the journal map but `recoverRecord` (`snapshot_query_intake.go:178-209`) has no case for it, so one staged statement stops recovery of every later record.
- **Pins are one step behind everywhere:** AR → HG 8 commits (`18340e7` → `036be2e`), AR → AC 1 (`ae1c54f` → `a462498`), AC → HG 1 (`8729876` → `036be2e`), HG → RG 1 (missing #37). The RC manifest's `sources.hg` (`022ee95`) is a dev-branch SHA whose profile tool is byte-identical to main's.

## 2. Ordering rules

- A precedes B and C; C5 requires B5; D requires A, B and C (master plan). Land shared contracts before consumers and pin dependents to immutable commits.
- Wave 0 unblocks and de-risks; Wave 1 builds the control plane that every later wave calls; Wave 2 completes the data plane against Wave 1 contracts; Wave 3 wires transport and activation; Wave 4 is engine follow-up that can run in parallel with anything.
- One PR per task, default-off, task review (spec + quality) before merge, merge only on green CI. Pin bumps move go.mod, go.sum, `MODULE.bazel` (`bazel_dep` + `git_override`) and any submodule or manifest together.
- Sizes: S ≤ 1 agent-day, M 1–2, L 3–5, XL more. Suggested tier: Sonnet for S with a complete brief, Opus for M/L and for anything touching Raft, signatures or recovery.

## 3. TODO

### Wave 0 — unblockers, hygiene, decisions

| ID | Repo | Task | Completes | Depends on | Size |
|---|---|---|---|---|---|
| T0.1 | HG | Add the `PreparedOutput` case to `recoverRecord`: a staged record recovers to its prepared-output state (or, until C4's later stages exist, is reported as recoverable-later without aborting the loop); test that one staged record no longer stops recovery of later records and that a same-statement re-`Submit` returns the accepted result. | C4 | — | S |
| T0.2 | AR | Close the #87 in-version hazard: bump `fsm/snapshot.go` to v10 (accept 3–10), migrate or reject v9 draining records without `DrainFrontierBlockSeq`, and export an apply seam for `applyReleaseReservation` / `applyAbsentReservationCancellation` so a stuck draining record has an exit. | C2 | — | S/M |
| T0.3 | AR, AC, HG | Pin lockstep: AR → HG `036be2e` + AC `a462498`; AC → HG `036be2e`; HG → RG `4e4a14a` (#37). Clean-fetch proof bounded to the changed modules. | — | — | S each |
| T0.4 | HG docs | Refresh stale plan text: coordination plan snapshot version (v2 → v10) and tombstone location, contracts plan A4 evidence (RC runs on `7567dc5`), integration plan paths (`pkg/plugin/query_only_host.go`, `pkg/plugins/sisnapshotquery`), and the rewriter-go#38 policy note. | docs | T0.2 | S |
| T0.5 | RC | Re-pin the paired-CI manifest `sources.hg` to a main ancestor (`036be2e`); the profile tool is byte-identical so the run should stay green (~25 min on the build box). Record the eight pin sites next to the corpus per the #153 comment. | A4 | — | S |
| T0.6 | user | Decisions: (a) rewriter-go#38 — keep GRANT/REVOKE/DESCRIBE/SHOW as unknown classes, pin the measured behavior, or make them ordinary in both engines; (b) HG #156 v2 append semantics — replay owner confirms no root change on devnet2; (c) whether to tag RG v0.12.0 / RC v0.15.0 now or when HG needs a release. | A4, A13 | — | — |

### Wave 1 — control plane (AR + AC)

| ID | Repo | Task | Completes | Depends on | Size |
|---|---|---|---|---|---|
| T1.1 | AR | C1 completion: dispatch `FinishRetirement`, `CloseUse`, `OpenChallenge`, `ResolveObligation`; add the enabling path for `Capability` (Raft command or genesis config) with tests that genesis stays off; candidate-bound capacity charging so the capacity ledger and disposition can be enabled together; Raft commands 26 (`publish_executor_profile_transition`) and 27 (`record_snapshot_artifact_ready`); `SourceClaims.RecordSnapshotArtifactReady`; authenticated leader-Barrier reads for publication/history. | C1 | T0.2 | L |
| T1.2 | AR | C2 completion: verify the control JWS in Acquire/Lookup/Release and call the FSM; implement `ProposeServerOwnedReservationGrant` over Raft with a leader read round; fence new SI admissions at drain begin (`fsm/admission.go`); registry Retain and obligation creation before the grant response escapes; durable request-status and cancellation tombstones (v10); lost-response recovery per the master plan. | C2 | T0.2, T1.1 (registry) | L |
| T1.3 | AR (+ AC fixtures) | C3: `SubmitSnapshotQuery` / `GetSnapshotQueryStatus` / abort RPCs; statement-seq assignment and singleton block seal; `expected_user_jws_hash` identity-conflict semantics; `snapshot-query-abort-v1` record and domain; the settlement record `ResolveObligation` reads; abort authority under a query-specific JWS purpose. AC adds wire fixtures for the new records. | C3 | T1.2 | L |
| T1.4 | AC | B1: `ArtifactBackend` (filesystem), `PartExporter`, `ArtifactOwnerRegistry` (five-class lifecycle), HGPART v1 archive/parser, private checksum-ZSTD leaf, `NewSnapshotArtifacts`, journal and terminal proof; read-only source/verifier construction with no publisher secret. | B1 | AC wire (done); AR C1 client contract | L |
| T1.5 | AC | B5 completion: concrete `historicalSnapshotQueryAuthenticatedRecordSource` / `TrustedReference` over AR C2 lookup; funded verifier lifecycle client (`AdmitUse` / `Retain` / `CloseUse` / `Release`) over `wire/artifact_disposition.go`; keep default routes nil. | B5 | T1.2 | M |

### Wave 2 — data plane completion (HG + AC + AR)

| ID | Repo | Task | Completes | Depends on | Size |
|---|---|---|---|---|---|
| T2.1 | HG | B2 completion: scan attached rows through `payloadexec.RowElementHash`, fold per-part LtHash and recompute partition roots against the manifest, read `_hg_row_id` back; replace the SQL-substring isolation in `QueryRows` with a grant-restricted reader; add the docker ATTACH/grant/readback case (plan B2 step 5). | B2 | — | M |
| T2.2 | HG | B4 completion: export the authenticated `HistoricalPolicy` constructor for C1 wiring; provide production `QueryUseAdmission` and `SnapshotReadStore` adapters (or document that AC supplies them and add the seam test). | B4 | T1.5 | S/M |
| T2.3 | HG | C4 completion: stages Executing / OutputDurable / CapacityReserved / PrepareUnknown / Prepared / ClaimUnknown / Claimed / Resolving / Applied / Aborted; the `QuerySource` port set (`PrepareSnapshotQuery`, `LookupSnapshotQueryPreparation`, `RegisterSnapshotQueryClaim`); populate `AckLevel` / `ExecutionOutcome` / `OutputRowsRoot` for ACK2 and safe-ACK; terminal journal compaction; give `PreparedOutputStager` its intake caller; recovery for every new stage. Bump the journal version. | C4 | T0.1, T1.2, T1.3 | L |
| T2.4 | AC | C5 source lifecycle: `snode/query_staged.go` + `query_journal.go` (or equivalent) — capacity reservation → unsafe write → exact candidate scan → claim; cleanup, abort, fence and restart; mirrors the INSERT staged path but for snapshot-query reads. | C5 | T2.3 ports, T1.3, T1.5 | L |
| T2.5 | AR | C5 transports: `RegisterSnapshotQueryClaim` and `SubmitSnapshotQueryAttestation` RPCs with authenticated source identity; `historicalGatewayBarrier` and `historicalGatewayMaterialReader` implementations and gateway construction; claim → exact-candidate-scan → three-way promotion validation; late-fence refusal. | C5 | T1.2 | M/L |

### Wave 3 — transport and activation (HG + SN)

| ID | Repo | Task | Completes | Depends on | Size |
|---|---|---|---|---|---|
| T3.1 | HG | D1 completion: carry the canonical input as `SQL_x_snapshot_query_input` (add it to `housegateOwnedSettingKeys`); upgrade the plugin ports to `rewriter.SnapshotQueryAnalyzer` and a `SnapshotCatalogSource` returning `AuthenticatedSnapshotQuerySchemaV1` with schema-object revalidation and target/referenced-table eligibility; journal stage constants; `(signer, network, query ID)` retry correlation. | D1 | T2.3 | M/L |
| T3.2 | HG | D2 host admission: production plugin (`pkg/plugins/snapshotquery`) decoding the v3 input and token, re-analyzing against the pinned authenticated object, setting `SnapshotQueryHostKey`; bind `QueryOnlyPlan.Run` to `SnapshotQueryIntake.Submit`; measured CLI and Go-driver packet traces. | D2 | T3.1, T2.3 | M/L |
| T3.3 | HG, SN | D3: `storage_integrity.snapshot_query.*` config block (`enabled`, `profile_manifest_path`, `state_dir`, `artifact_dir`) and `build.go` wiring of analyzer, policy, logical names, use admission, reservation client, catalog, intake and source ports, all default-off; SN `storageintegrityadapter` wiring and pins; fixed identity / proof / registry / filesystem fences. | D3 | T3.1, T3.2, T1.x clients | M |
| T3.4 | HG (+ all) | D4: `pkg/integration/snapshotquery` docker multi-role cluster, `cases.json`, fault injection at every durable boundary (A10–A12), the A1–A14 evidence table against one immutable release manifest, CI registration in the explicit integration list, activation and rollback runbook. | D4 | everything above | XL |

### Wave 4 — engines and tooling (parallel)

| ID | Repo | Task | Completes | Depends on | Size |
|---|---|---|---|---|---|
| T4.1 | RG, RC | Implement the rewriter-go#38 decision if it is option 1 or 2 (corpus + 4 RG / 8 RC pin sites; prepared patches are in the remediation ledger's `task-17-preserved/`). | A4 | T0.6(a) | M |
| T4.2 | RG, RC | Re-run the paired qualification for each new role binary or FFI (by design), and cut RG / RC release tags when HG needs a released dependency. | A4, A13 | T0.6(c) | S |
| T4.3 | RC | Operator fixes from the #153 comment: document `./build/tests/rewriter_tests`, the rsync/ninja mtime trap, the full pin-site list, and a documented local rehearsal-profile script with its "not a CI qualification" caveat. | docs | — | S |

## 4. Open decisions for uranuswch

1. rewriter-go#38 ordinary-class policy (T0.6a). Recommendation: option 1 (pin the measured behavior) unless a product need for ordinary GRANT/SHOW through the signed lane exists, because option 2 relocates the divergence to `SHOW CREATE TABLE` and `SHOW … FORMAT`.
2. HG #156 v2 append semantics on devnet2 (T0.6b) — a replay owner must confirm no root change.
3. Release tagging for RG / RC (T0.6c). Recommendation: tag when T3.3 pins them, not before.
4. Whether Wave 1 should be one AR lane (serial, safest for the FSM) or two (C1 and C2 in parallel with a shared v10 migration landing first via T0.2).

## 5. Risks and traps

- The AR control plane is the critical path: C2 → C3 → C4 → C5 → D1–D4 all wait on it. Start T1.2 first and keep its PRs small and default-off.
- The RC paired CI runs on a shared build box whose ClickHouse checkout is symlinked into `/home/sentio/chen/rewriter-grpc`; a CI run hard-resets that tree, so nobody should build there while CI is running. Docker images pinned by ID must stay tagged (the September prune cost a day).
- The AR snapshot format has moved seven versions past the plan text; every persistence change needs an explicit version bump and an old-snapshot fixture.
- Two of today's four gap reports contained one wrong fact each (an AC report read a stale local checkout for AR's pins; a T17 brief reasoned from RG source instead of measuring it). Verify pins and engine behavior by running them, not by reading.
- Nothing in this lane is reachable from a production session today; that is the intended state until D4, and D3 must keep it so.

## 6. Execution notes

- Run the work as superpowers:subagent-driven-development with one lane per repository, a ledger under `.superpowers/sdd/<this file's basename>/`, briefs extracted per TODO, review packages as files, and rulings recorded inline.
- PR bodies end with `Refs https://github.com/housegate/housegate/issues/153` and the design link https://github.com/housegate/housegate/pull/154; commits end with the attribution trailer.
- Bazel is the test authority in HG; AR / AC also run `bazel test //...`; RC's only real gate is its private build-box workflow; RG's public CI skips the measured corpus by design.
