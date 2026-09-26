# Storage-Integrity Data-Plane Reconciler Implementation Plan (sub-project 4, plan A)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every data-plane node (the source SNode and every verifier) follow the arbiter's dynamic storage-integrity table registry and keep its own `hg_unsafe` / `hg_safe` / `hg_promote` tables equal to what the registry asks for, without a restart or a config edit: create and verify the tables of Pending, Active and Retiring incarnations; drop the tables, the Keeper replica and (on the source) decommissioned replicas of Purging incarnations and report `SubmitTablePurged`; drop leftovers of retired keys; never drop what the registry does not know. The SNode admits a fresh statement only for an Active, Ready table; a verifier attests a transition that adds tables only once they are Ready here. The arbiter answers `SubmitTablePurged` idempotently, serves the purge node set, tightens restore (P12) and runs the follower in its reference binaries.

**Architecture:** Three repositories change, in order. `arbiter-proto` adds `PromotionGateway.SubmitTablePurged(RecordTablePurgedCmd)` and `TableRegistry.GetPurgeNodeSet` (Task 1). `arbiter-core` pins it and gains, bottom-up: a `wire` decoder of `TableRegistrySnapshot` with arbiter's `Live` rule (Task 2); a `dataplane.RegistryFollower` (one `GetTableRegistry` through `WithLeaderRetry`, then a resumable `WatchTableRegistry` on the existing `runSubscription` loop) plus the purge-report client (Task 3); a per-table `ddl` lifecycle — `EnsureTable`, `DropTable`, Keeper replica listing and removal, and a `COMMENT 'hg_incarnation=<seq>'` marker that tells incarnations of one key apart (Task 4); the level-triggered `dataplane/tableset.Reconciler` (Task 5); and the SNode (Task 6) and verifier (Task 7) integrations, which replace the verify-only reconcile loop with the reconciler. `arbiter` bumps both pins (Task 8), adds the two handlers (Task 9), tightens restore and rebuilds the two orchestrator fixtures that forged now-refused states (Task 10), and wires the follower into `arbiter-snode` / `arbiter-verifier` (Task 11). The controller releases (Task 12). The final section hands plan B (housegate release, sentio-core, sentio-node) the names it consumes.

**Tech Stack:** protobuf with buf, protoc-gen-go v1.36.11, protoc-gen-go-grpc v1.5.1 (`make tools`); Go 1.26.3 (both Go modules; `go test` works everywhere), Bazel 9.1.0 with Bzlmod and gazelle (`bazel run //:gazelle`, CI runs `bazel test`); ClickHouse 25.8 with an embedded Keeper for the docker-gated tests.

**Spec:** `housegate/housegate` `docs/superpowers/specs/2026-09-25-dynamic-si-table-set-data-plane-design.md` §5–§8, the arbiter-core and arbiter rows of §10, and §11 step 1; umbrella `2026-09-23-dynamic-si-table-set-design.md` §14–§15; sub-project 3 `2026-09-24-dynamic-si-table-set-housegate-design.md` §13 (host contract, consumed by plan B). Measured bases (2026-09-25, `origin/main`): arbiter-proto `a3dbb1f`, arbiter-core `d059aab`, arbiter `ee26f54`, ClickHouse server `25.8.28`. Every code block below was built and run in throwaway worktrees at those bases: arbiter-core's full `go test ./...` against a two-server Keeper-backed ClickHouse, `bazel test //...`, and `-race` on `dataplane`, `dataplane/tableset`, `snode` and `verifier`; arbiter's full `go test ./...`, the docker `chpipeline` and `startup` suites, and `bazel test` of the changed targets; each task was replayed in order on a clean checkout, red then green. Out of scope: every sentio-core, sentio-node and housegate change (plan B), the helm chart and rollout (sub-project 5).

## Global Constraints

- **RPCs (arbiter-proto).** `rpc SubmitTablePurged (RecordTablePurgedCmd) returns (Ack) {}` in `service PromotionGateway` (`proto/arbiter.proto`, which now imports `table_registry.proto`); `rpc GetPurgeNodeSet (google.protobuf.Empty) returns (PurgeNodeSet) {}` in `service TableRegistry`; `message PurgeNodeSet { repeated string node_ids = 1; }` in `proto/table_registry.proto`. `RecordTablePurgedCmd` is unchanged (`node_id = 1`, `incarnation_seq = 2`).
- **Handler answers (arbiter).** Behind the leader read barrier: registry disabled → `FAILED_PRECONDITION` `table registry is disabled`; `incarnation_seq` 0 → `INVALID_ARGUMENT` `incarnation_seq is required`; empty `node_id` → `INVALID_ARGUMENT` `node_id is required`; unknown seq → `INVALID_ARGUMENT` `unknown table incarnation <seq>`; Purged, or `node_id` already in `purged_by` → `Ack` without a proposal; any other non-Purging status → `FAILED_PRECONDITION` `table incarnation <seq> is <status>, not purging` (no NotLeader detail); Purging → propose `RecordTablePurged`; an FSM rejection of an incarnation that is Purged by then → `Ack`; a follower → `FAILED_PRECONDITION` with a `NotLeader` detail.
- **Purge node set.** Every registered, non-evicted node with the SNode or verifier role, sorted ascending — the set `fsm.completePurgesLocked` waits on. A Keeper replica named outside it is decommissioned.
- **Incarnation marker.** Chain-origin incarnation `<seq>`: `COMMENT 'hg_incarnation=<seq>'` on all three tables. Genesis tables carry no comment, so today's DDL and golden output are unchanged.
- **Keeper.** Path `/sentio/<keeper_shard_id>/unsafe/<db>__<table>`; replica name = the node id (SNode `NodeID`, verifier `ReplicaID`); replicas listed with `SELECT name FROM system.zookeeper WHERE path = '<path>/replicas'` (an absent path lists nothing); removal `SYSTEM DROP REPLICA '<r>' FROM ZKPATH '<path>'` (ClickHouse refuses an active replica with code 305 `TABLE_WAS_NOT_DROPPED`); a stale own replica makes `CREATE` fail with code 253 `REPLICA_ALREADY_EXISTS`.
- **Drop order.** `DROP TABLE IF EXISTS <db>.<t> SYNC` for `hg_promote`, then `hg_safe`, then `hg_unsafe`, then this node's own replica if it survived, then (source SNode only) every replica outside the purge node set, then `SubmitTablePurged`.
- **Reconciler states** (`tableset.State`): `creating`, `ready`, `waiting_quiescence`, `purging`, `purge_reported`.
- **Timeouts.** `dataplane.DefaultRegistryStartupTimeout = 2 * time.Minute`; `verifier.DefaultAddTransitionReadyWait = 10 * time.Second`; reconcile cadence `ddl.DefaultReconcileInterval` (60 s); per-table backoff `ddl.ReconcileBackoff(failures, interval)`. No new YAML key in the reference binaries.
- **Errors.** `snode.ErrTableNotReady` (new, pre-write); `verifier.ErrAddedTableNotReady`; `dataplane.ErrTableNotPurging`; `dataplane.ErrRegistryNotReady`; `dataplane.TableRegistryDisabledMessage = "table registry is disabled"`.
- **Releases.** arbiter-proto `v0.8.0` on `PROTO_SHA`; arbiter-core `v0.10.0` on `CORE_SHA`; arbiter merges only. The release workflows compute versions from the release day; the tags actually cut are authoritative.
- **Docker-gated tests.** `ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1 CH_ADDR=<a> CH_REPLICA_ADDR=<b>`, two ClickHouse 25.8 servers sharing one Keeper exactly like arbiter-core's CI job `integration-clickhouse` (Task 4 Step 1 gives the local commands). The new `//dataplane/tableset:tableset_test` target joins that job's list.
- **Branches.** `feat/si-table-purge` in arbiter-proto; `feat/si-data-plane-reconciler` in arbiter-core and in arbiter; one PR each. Never push to `main`. Plans and docs are not hard-wrapped.

## Review Focus

1. **Cross-incarnation leftovers.** Umbrella D9 lets a key be recreated once its previous incarnation is Purged, and a node that was evicted or offline when that happened still holds the old tables under the same physical name. `CREATE TABLE IF NOT EXISTS` and verification alone would adopt them — old rows in the new incarnation, and a `Ready` that lets a verifier attest the new add. Tests: Task 4 `TestEnsureTable_MarksVerifiesAndIsIdempotent` (marker drift); Task 5 `TestReconciler_DropsLeftoversOfRetiredKeysAndEarlierIncarnations`, `TestReconciler_ForeignMarkerIsDriftNotALeftover` (an unattributable marker is never dropped), `TestReconciler_ReadyNeverCountsAnEarlierIncarnation`.
2. **`WithLeaderRetry` reads every `FAILED_PRECONDITION` as a leader miss** (`dataplane/client.go:205` `notLeaderHint` returns `("", true)` without a `NotLeader` detail), so the leader's "table registry is disabled" and "not purging" answers would retry forever, or a caller might read them as success. Tests: Task 3 `TestRegistryFollower_DisabledThenFirstSnapshot`, `TestSubmitTablePurged_PreconditionIsNotSuccessAndNotALeaderMiss` (exactly one call, `ErrTableNotPurging`); Task 9 `TestSubmitTablePurged_IdempotentAndFailedPreconditionBeforePurging` (the answer carries no `NotLeader` detail).
3. **Dropping a table the registry still wants.** D2 naming is not injective across every key the registry ever saw (a Legacy `x__y.z` and a live `x.y__z` share `x__y__z`), and a table the registry does not know must only be reported. Tests: Task 5 `TestReconciler_NeverDropsATableALiveKeyOwns`, `TestReconciler_UnknownTablesAreOnlyReported`, `TestReconciler_RegistryDisabledReconcilesTheGenesisSetOnly`.
4. **Keeper state that outlives its table.** A decommissioned replica keeps the table path alive and a same-name recreation with a new structure then fails (code 122, measured); a crash between Keeper registration and local metadata leaves this node's own replica behind (code 253 on the next `CREATE`); another server actively holding this node's replica name must not be stolen. Tests: Task 4 `TestEnsureTable_RecoversItsOwnStrandedReplica`, `TestEnsureTable_RefusesToStealAnActiveReplica`, `TestDropTable_RemovesItsOwnKeeperOnlyReplica`, `TestDropReplica_DecommissionedReplicaUnblocksSameNameRecreation`; Task 5 `TestReconciler_SweepsDecommissionedReplicas` (a current node's replica survives the sweep).
5. **Ordering at the lifecycle edges.** The SNode must not drop `hg_unsafe` while a promotion or unsafe cleanup still needs it (a later cleanup command would fail forever); a verifier must not attest an add before its own tables exist; a purge report that loses a race with the incarnation's completion must still succeed. Tests: Task 5 `TestReconciler_PurgeWaitsForQuiescence`; Task 6 `TestTableQuiescent_WaitsForPromotionCleanupAndIntake`, `TestPrepareLocalStatement_RegistryGatesFreshIntakeOnChainTables`; Task 7 `TestReplayJob_AddTransitionIsAttestedOnlyOnceTheTableIsReady`, `TestRun_AddTransitionGateLetsTheReconcileLoopCreateTheTable`; Task 9 `TestSubmitTablePurged_RejectedProposalOnAPurgedIncarnationIsSuccess`.

## Plan decisions

- **P1 — `SubmitTablePurged` rides `PromotionGateway` and takes `RecordTablePurgedCmd`; its authentication is `AckCleanup`'s (measured item 1).** `AckCleanup` is `PromotionGateway` (`proto/arbiter.proto:494-499`). No data-plane RPC is authenticated at the transport: `cmd/arbiter/grpc.go:47` builds `grpc.NewServer()` with no interceptor or credentials, the client dials with `insecure.NewCredentials()` (`dataplane/client.go:98`), and `Server.RegisterDataPlane` puts every data-plane service on one listener, so verifiers reach `PromotionGateway` as SNodes do. Identity is the `node_id` field, checked by `fsm.applyRecordTablePurged` (`fsm/apply_table_registry.go:229-249`: a registered, non-evicted SNode or verifier), exactly as `AckCleanup`'s `node_id`. Reusing the replicated command as the request follows `UpdateConsensusParamsCmd` ("both the administration request and the replicated command", `proto/consensus.proto:47-52`) and keeps `arbiter.proto` free of a new message, so its snapshot-ledger conformance gate needs no entry.
- **P2 — The handler answers from committed state and changes no FSM semantics (measured item 2).** Existing data-plane handlers propose directly and map `fsm.Rejected` to `INVALID_ARGUMENT` and raft `ErrNotLeader` to `NotLeader` (`server/server.go:194-219`); reads cross `consensusReadBarrier`, which maps a follower to `NotLeader` (`server/consensus_admin.go:441-474`). `applyRecordTablePurged` already returns `Applied` for a recorded node but `Rejected` for a Purged incarnation, and changing Apply would change mixed-version replicas' answers. So `SubmitTablePurged` crosses the barrier, decides the idempotent and precondition answers from `TableRegistryView` without a proposal, proposes only for a Purging incarnation this node has not reported, and on a rejection re-reads committed state: Purged means the report lost a race with the last report or an eviction (`applyEvictNode` also completes purges, `fsm/apply.go:91-104`), which is success.
- **P3 — The verifier gate waits 10 s and relies on redelivery (measured item 3).** A handler error goes nowhere: `runSubscription` ignores `deliver`'s result (`dataplane/subscribe.go:71`). The leader re-sends the `ReplayJob` to every connected verifier of the block's fixed verifier set that has not attested on every rescan (`orchestrator/dispatch.go` `dispatchEvidence`), rescans run every `dispatch.retry_interval` (default 5 s, `config/raw.go:149`; `orchestrator/loop.go:97-123`), and nothing times out; quorum is reached by the verifiers that did attest. The per-verifier stream buffer holds 16 messages with a non-blocking send (`server/registry.go:24-58`), and the gate blocks the subscription while it waits, so the bound must stay short: `DefaultAddTransitionReadyWait = 10 * time.Second` (two retry intervals, room for one reconcile pass on a loaded ClickHouse), settable through `verifier.Config.AddTransitionReadyWait`.
- **P4 — The source-claim root stays a registry-set root (measured item 4).** It is diagnostic end to end: the RC's `SourceClaimRoot` becomes the job's through `fsm.blockSourceClaimRoot` ("diagnostic input to replay, not the root to anchor", `fsm/reads_helpers.go:21-29`); housegate's `replay.Verifier` copies it into the receipt and derives the advisory `MatchSourceRoot` (`pkg/replay/verifier.go:153-155`); the FSM's check 1 recomputes the root itself and "the advisory MatchSourceRoot flag is deliberately unread" (`fsm/threeway.go:86-87`). RC idempotency compares the whole RC (`fsm/apply.go:374,388`), but the SNode journals its RC and re-sends that copy (`snode/staged.go:349`), so a registry change between retries cannot make a retried RC differ. With the registry enabled the root covers Active and Retiring incarnations and uses `payloadexec.SchemaRootFromHashes` over their registry hashes; a registry holding only the genesis tables yields exactly the static root (tested).
- **P5 — "Current node set" is the Membership set, served by a new read (measured item 5).** `completePurgesLocked` waits on every registered, non-evicted SNode and verifier (`fsm/apply_table_registry.go:259-283`); the consensus parameters carry no verifier list (`ConsensusMutableParams` fields 1–4). No data-plane read exposes Membership, so `TableRegistry.GetPurgeNodeSet` serves `fsm.PurgeNodeSet()` behind the leader barrier. Replica names are node ids (`ddl.Pinned.NodeID` is the SNode `NodeID` and the verifier `ReplicaID`). Measured on ClickHouse 25.8 with two servers sharing a Keeper: `system.zookeeper` lists an absent path as empty; `SYSTEM DROP REPLICA` of an active replica fails with 305; dropping the last replica, by `DROP TABLE ... SYNC` or by `SYSTEM DROP REPLICA`, removes the table path; with a stale replica left, a same-name `CREATE` with another structure fails with 122 `INCOMPATIBLE_COLUMNS`. Only the source SNode sweeps (`SweepDecommissioned`), after its own drop and before its report, and again for any key whose live incarnation has no tables but whose Keeper path survived — the case of a node evicted after the first sweep, whose eviction completed the purge.
- **P6 — Proto pins by tag (measured item 6).** arbiter-core pins arbiter-proto in `go.mod` only (`go.mod:14`, pseudo-version `v0.7.2-0.20260923160758-1b3de4c17375`; Bazel resolves it through `go_deps.from_file`, and `bazel mod tidy` leaves `MODULE.bazel.lock` untouched, as recorded in arbiter-core `d059aab`). arbiter pins arbiter-proto the same way and arbiter-core three ways: `go.mod`, `MODULE.bazel` `bazel_dep(version = ...)` and `git_override(commit = ...)`. arbiter-proto's last tag is `v0.7.1` on an old commit, and every consumer since pins `main` pseudo-versions. This plan tags `v0.8.0` on `PROTO_SHA` (its Cut Release adds one to Y on a new UTC day) and pins the tag everywhere, so arbiter-core, arbiter and sentio-node resolve one name; arbiter-core's own Cut Release (`scripts/next-version.sh`) then yields `v0.10.0`, pinned by tag in arbiter.
- **P7 — `ddl.EnsureTable` is the per-table entry point (measured item 7).** `EnsureTable(ctx, conn, pinned, schema, incarnationSeq, mode)` creates (in `ModeCreateAndVerify`) and verifies one table's three intents; `incarnationSeq` 0 renders today's unmarked genesis DDL. A `REPLICA_ALREADY_EXISTS` from `hg_unsafe` while the local table is absent is this node's own crash leftover: it is removed with `SYSTEM DROP REPLICA` (which ClickHouse refuses for an active holder, so a duplicated node id is not stolen) and the create is retried once. `DropTable` drops the three tables and this node's surviving replica, so it is repeatable after a crash at any point. `EnsureProtocolTables` stays for existing callers; the roles no longer call it.
- **P8 — The `hg_incarnation` COMMENT is the only way to honour spec §7.1's "leftovers of an earlier incarnation".** ClickHouse state cannot otherwise tell two incarnations of one key apart. The comment is local metadata: `engine_full` excludes it (measured), so `ParseEngineFullSettings` is unaffected, and `VerifyProtocolTable` compares it only when the intent carries one. A local table whose marker names an earlier incarnation of the key that the registry records as Purged or Refused (or, when unmarked, the key's Purged genesis incarnation) is dropped before the create; any other foreign marker is drift and fatal — it is never dropped, because only a wrong registry (another network's arbiter) or an operator can produce it.
- **P9 — Failure classes.** Fatal, as today: drift, a verify-only table missing, an unattributable marker; the role exits. Genesis tables keep the role's existing retry budget (`ProtocolTablesMaxFailures`, `DefaultReconcileMaxFailures = 5`), so the existing reconcile tests stay unchanged. Chain-table creation and every purge step (quiescence error, drop, sweep, report — including `ErrTableNotPurging` and `Unimplemented` from an old arbiter, spec §11.3) are isolated per table with `ddl.ReconcileBackoff`; `Trigger` clears the backoff.
- **P10 — arbiter-core registers no metrics.** It has no Prometheus dependency (`go.mod`) and arbiter's standalone `arbiter-snode` / `arbiter-verifier` serve no metrics endpoint (`cmd/arbiter-snode/main.go`). `tableset.Reconciler.Stats()` (tables per state, cumulative failures per table, unknown tables, leftover drops) is exposed through `snode.Role.TableSetStats()` / `verifier.Role.TableSetStats()`; plan B exports it on housegate's `MetricsRegistry()`. The standalone binaries log every drop, sweep, failure and unknown table.
- **P11 — Only a fresh intake is gated.** `PrepareLocalStatement` loads the journal first; with no record (or a `Cleaned` one) the target must be Active, the envelope must sign the registry hash, and the reconciler must report Ready. Pending or not Ready is `ErrTableNotReady` (pre-write, retryable); any other status is `ErrSchemaUnknown` as before. A recorded statement, and promotion and replace, resolve the key's live incarnation when it is Active, Retiring or Purging (spec §8), so a statement admitted before a retirement still converges and promotes.
- **P12 — Registry first for scans and schemas.** Spec §8 says the verifier scanner "checks the configured genesis tables first". Read literally, a same-name chain recreation of a retired genesis table would be scanned with the stale genesis schema. The scanner and the SNode therefore let the key's live incarnation decide when the registry is enabled — a genesis-origin one still uses the configured schema, which is the spec's intent — and fall back to the configured tables otherwise (tested with a recreated genesis key).
- **P13 — Startup gate.** `Register` and `RunWithReady`/`Run` both call the role's ensure seam first; the seam waits for the follower's Ready (`dataplane.WaitReady`, `RegistryStartupTimeout`, default 2 min) before the first pass, so ClickHouse is not touched before the arbiter answers. `ErrRegistryNotReady` is not transient for `startup.RunRole`, so the binary exits. An arbiter without the `TableRegistry` service (`Unimplemented`) counts as a disabled registry; the watch keeps retrying it at the maximum backoff.
- **P14 — The follower belongs to the host.** Roles take a `dataplane.RegistryView` in `Deps.Registry`; nil keeps the static behaviour. The reference binaries (Task 11) and sentio-node (plan B) construct one follower, run it, and share it. `New` refuses a `Registry` together with unmanaged DDL (`SchemaSourceUnmanaged`), because the reconciler is what makes tables Ready.
- **P15 — The two forged orchestrator fixtures (P12 carry-over).** After P12 no restorable state can make a transition block's dispatch material unavailable: `replayTableSetTransitionLocked` fails only for a missing, hash-mismatched or schema-less added incarnation, restore already matches add blocks to headers (`validateTableSetTransitions`) and now requires `schema_json`. `TestHandleDispatchRows_DoesNotChallengeATransitionBlockWithoutDispatchInfo` is therefore removed with that reasoning; its header-only guard stays covered by `TestHandleDispatchRows_DoesNotChallengeATransitionBlock`. `TestCheckSeal_LogsAnUnsealableTableSetChange` keeps its purpose with a restorable forgery: a Pending chain duplicate of the genesis key (D9 is enforced by admission, not by restore), which the sealer refuses as "already in the base table set".
- **P16 — The reference binaries run the follower beside the role.** `startup.RunRoleWithRegistry` starts the follower, stops the role when the follower fails with a non-retryable error, and returns that error. Because roles now wait for the registry before ClickHouse, the two startup tests that expect a ClickHouse dial get an arbiter peer that answers "table registry is disabled".
- **P17 — The snapshot-query lane stays static.** `verifier.handleSnapshotQueryJob` refuses a read-set table outside `Config.Tables` before the signature check and before any historical read or submission.

---

## File Structure

**arbiter-proto** (`github.com/sentioxyz/arbiter-proto`)
- Modify `proto/arbiter.proto` (import at 21, `service PromotionGateway` at 494-499) and `proto/table_registry.proto` (`WatchTableRegistryRequest` at 153, `service TableRegistry` at 159-167); regenerate `gen/pb/arbiter.pb.go`, `arbiter_grpc.pb.go`, `table_registry.pb.go`, `table_registry_grpc.pb.go`.
- Create `conformance/table_purge_test.go`; modify `conformance/table_registry_read_test.go:82-97`.

**arbiter-core** (`github.com/sentioxyz/arbiter-core`)
- Pins: `go.mod`, `go.sum`.
- `wire/`: create `table_registry_snapshot.go`, `table_registry_snapshot_test.go`.
- `dataplane/`: create `registry_follower.go`, `table_purge.go`, `registry_follower_test.go`.
- `dataplane/ddl/`: create `table.go`, `table_ch_test.go`; modify `build.go` (`TableIntent`, `SQL`), `verify.go` (`VerifyProtocolTable`).
- `dataplane/tableset/`: create `reconciler.go`, `reconciler_ch_test.go`.
- `snode/`: create `registry.go`, `registry_test.go`; modify `snode.go`, `config.go`, `staged.go`, `view.go`.
- `verifier/`: create `registry.go`, `registry_test.go`; modify `verifier.go`, `config.go`, `backends.go`.
- `BUILD.bazel` files regenerated by gazelle in `wire/`, `dataplane/`, `dataplane/ddl/`, `dataplane/tableset/` (new), `snode/`, `verifier/`.
- Modify `README.md` (package table), `.github/workflows/ci.yml` (`integration-clickhouse` target list).

**arbiter** (`github.com/sentioxyz/arbiter`)
- Pins: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock`.
- Modify `cmd/internal/startup/role_fixtures_test.go` (fake ClickHouse answers the reconciler's reads).
- `fsm/`: modify `apply_table_registry.go` (`PurgeNodeSet`), `table_registry_validation.go` (P12); create `table_registry_restore_p12_test.go`.
- `server/`: create `table_purge.go`, `table_purge_test.go`.
- `orchestrator/`: modify `table_set_transition_test.go` (P15).
- `cmd/internal/startup/`: create `registry.go`, `registry_test.go`.
- `cmd/arbiter-snode/`, `cmd/arbiter-verifier/`: modify `main.go`, `config.go`, `main_test.go`, `startup_test.go`; create `registry_fixture_test.go`.
- Modify `README.md` (schema-source section).
- `BUILD.bazel` files regenerated by gazelle in `server/`, `fsm/`, `cmd/internal/startup/`, `cmd/arbiter-snode/`, `cmd/arbiter-verifier/`.

**housegate, sentio-core, sentio-node** — no change (plan B).

---

## Task 1: arbiter-proto — `SubmitTablePurged` and `GetPurgeNodeSet`

**Files:**
- Modify: `proto/arbiter.proto:21`, `:494-499`
- Modify: `proto/table_registry.proto:153`, `:159-167`
- Regenerate: `gen/pb/arbiter.pb.go`, `gen/pb/arbiter_grpc.pb.go`, `gen/pb/table_registry.pb.go`, `gen/pb/table_registry_grpc.pb.go`
- Modify: `conformance/table_registry_read_test.go:82-97`
- Test: create `conformance/table_purge_test.go`

**Interfaces:**
- Consumes: nothing new.
- Produces (Go package `pb`): `PromotionGatewayClient.SubmitTablePurged(ctx, *RecordTablePurgedCmd, ...grpc.CallOption) (*Ack, error)` and `PromotionGatewayServer.SubmitTablePurged(context.Context, *RecordTablePurgedCmd) (*Ack, error)`; `TableRegistryClient.GetPurgeNodeSet(ctx, *emptypb.Empty, ...) (*PurgeNodeSet, error)` and the server method; `type PurgeNodeSet struct` with `NodeIds []string` / `GetNodeIds()`.

- [ ] **Step 1: Branch and write the failing test**

```bash
cd arbiter-proto && git fetch origin && git switch -c feat/si-table-purge origin/main && make tools
```

Create `conformance/table_purge_test.go`:

```go
package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestTablePurgeContract pins the sub-project 4 purge surface: the
// SubmitTablePurged RPC rides PromotionGateway next to AckCleanup and takes
// the replicated RecordTablePurgedCmd as its request (the precedent is
// UpdateConsensusParamsCmd), and GetPurgeNodeSet exposes the node set a purge
// waits on.
func TestTablePurgeContract(t *testing.T) {
	svc := pb.File_arbiter_proto.Services().ByName("PromotionGateway")
	if svc == nil {
		t.Fatal("PromotionGateway service missing")
	}
	m := svc.Methods().ByName("SubmitTablePurged")
	if m == nil || m.Input().FullName() != "arbiter.RecordTablePurgedCmd" || m.Output().FullName() != "arbiter.Ack" || m.IsStreamingClient() || m.IsStreamingServer() {
		t.Fatalf("PromotionGateway.SubmitTablePurged = %v", m)
	}
	if ack := svc.Methods().ByName("AckCleanup"); ack == nil || ack.Output().FullName() != "arbiter.Ack" {
		t.Fatalf("PromotionGateway.AckCleanup = %v", ack)
	}
	cmd := (&pb.RecordTablePurgedCmd{}).ProtoReflect().Descriptor().Fields()
	if cmd.Len() != 2 {
		t.Fatalf("RecordTablePurgedCmd has %d fields, want 2", cmd.Len())
	}
	if f := cmd.ByName("node_id"); f == nil || f.Number() != 1 || f.Kind() != protoreflect.StringKind {
		t.Fatalf("RecordTablePurgedCmd.node_id = %v", f)
	}
	if f := cmd.ByName("incarnation_seq"); f == nil || f.Number() != 2 || f.Kind() != protoreflect.Uint64Kind {
		t.Fatalf("RecordTablePurgedCmd.incarnation_seq = %v", f)
	}
	set := (&pb.PurgeNodeSet{}).ProtoReflect().Descriptor().Fields()
	if f := set.ByName("node_ids"); set.Len() != 1 || f == nil || f.Number() != 1 || f.Kind() != protoreflect.StringKind || !f.IsList() {
		t.Fatalf("PurgeNodeSet fields = %v", set)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `go test ./conformance/ -run TestTablePurgeContract`
Expected: build failure `undefined: pb.PurgeNodeSet`.

- [ ] **Step 3: Edit the protos and the read-contract test, then regenerate**

In `proto/arbiter.proto` replace:

```proto
import "replay.proto";
```

with:

```proto
import "replay.proto";
import "table_registry.proto";
```

and replace:

```proto
  rpc AckCleanup (CleanupAck) returns (Ack) {}
}
```

with:

```proto
  rpc AckCleanup (CleanupAck) returns (Ack) {}
  // A data-plane node (the source SNode or a verifier) reports that it dropped
  // its hg_* tables and Keeper replica for one Purging incarnation of the
  // dynamic SI table registry. Authenticated like AckCleanup: the node is named
  // by node_id and the FSM accepts only a registered, non-evicted SNode or
  // verifier. Idempotent: an already-recorded node, or an incarnation that is
  // already Purged, returns Ack. FAILED_PRECONDITION without a NotLeader
  // detail means the incarnation is not Purging yet; the caller retries and
  // never reads it as success.
  rpc SubmitTablePurged (RecordTablePurgedCmd) returns (Ack) {}
}
```

In `proto/table_registry.proto` replace:

```proto
message WatchTableRegistryRequest {
```

with:

```proto
// PurgeNodeSet is the committed set of data-plane nodes whose
// SubmitTablePurged completes a purge: every registered, non-evicted SNode and
// verifier, sorted ascending. A replica under a table's Keeper path whose name
// is not in the set belongs to a decommissioned node.
message PurgeNodeSet {
  repeated string node_ids = 1;
}

message WatchTableRegistryRequest {
```

and replace:

```proto
  rpc WatchTableRegistry (WatchTableRegistryRequest) returns (stream TableRegistrySnapshot) {}
}
```

with:

```proto
  rpc WatchTableRegistry (WatchTableRegistryRequest) returns (stream TableRegistrySnapshot) {}
  // The current purge node set. Leader-barriered like GetTableRegistry and
  // answered whether or not the registry is enabled.
  rpc GetPurgeNodeSet (google.protobuf.Empty) returns (PurgeNodeSet) {}
}
```

In `conformance/table_registry_read_test.go` replace:

```go
	if svc == nil || svc.Methods().Len() != 2 {
```

with:

```go
	if svc == nil || svc.Methods().Len() != 3 {
```

and replace:

```go
		{"WatchTableRegistry", "arbiter.WatchTableRegistryRequest", "arbiter.TableRegistrySnapshot", true},
	} {
```

with:

```go
		{"WatchTableRegistry", "arbiter.WatchTableRegistryRequest", "arbiter.TableRegistrySnapshot", true},
		{"GetPurgeNodeSet", "google.protobuf.Empty", "arbiter.PurgeNodeSet", false},
	} {
```

Run: `make proto`

- [ ] **Step 4: Run the full gate to verify it passes**

Run: `buf lint && buf breaking --against '.git#branch=origin/main' && make test && git diff --stat -- gen/`
Expected: `buf lint` and `buf breaking` print nothing (an added method and an added message are FILE-compatible); `ok github.com/sentioxyz/arbiter-proto/conformance` (including the unchanged snapshot-baseline and ledger tests, which the new import does not disturb); `gen/` shows the four regenerated files (measured on the prototype: 229 lines across `gen/`, `proto/arbiter.proto` +10, `proto/table_registry.proto` +11).

- [ ] **Step 5: Commit and open the PR**

```bash
git add proto/arbiter.proto proto/table_registry.proto gen/pb/ conformance/table_purge_test.go conformance/table_registry_read_test.go
git commit -m "feat(proto): add SubmitTablePurged and GetPurgeNodeSet for the dynamic SI table set" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/si-table-purge
gh pr create -R sentioxyz/arbiter-proto --fill
```

The controller merges it and runs Task 12 gate R1 before Task 2 starts.

---

## Task 2: arbiter-core — pin the proto and decode the registry snapshot

**Precondition:** Task 12 gate R1 is done; arbiter-proto `v0.8.0` exists on `PROTO_SHA`.

**Files:**
- Modify: `go.mod:14`, `go.sum`
- Create: `wire/table_registry_snapshot.go`
- Modify: `wire/BUILD.bazel` (gazelle)
- Test: create `wire/table_registry_snapshot_test.go`

**Interfaces:**
- Consumes: `pb.TableRegistrySnapshot`, `pb.TableIncarnation`, `pb.TableIncarnationStatus`, `pb.TableIncarnationOrigin`, `pb.TableRetireReason` (arbiter-proto `a3dbb1f`, already on `main`); existing `wire.TableRetireReason`, `wire.TableRegistryParamsFromPB/ToPB`, `wire.L2EventRefFromPB/ToPB`, `payloadexec.DecodeTableSchemaJSON(tableID, schemaJSON string) (payloadexec.TableSchema, error)`.
- Produces (package `wire`): `type TableIncarnationStatus string` (`TableStatusLegacy` … `TableStatusPurged`, arbiter fsm spellings); `type TableOrigin string` (`TableOriginGenesis`, `TableOriginLegacy`, `TableOriginChain`); `type TableRegistryCursor struct`; `type TableIncarnation struct` with `Key() string`, `HasPhysicalTables() bool`, `Schema() (payloadexec.TableSchema, error)`; `type TableRegistrySnapshot struct` with `Live(key string) *TableIncarnation`, `ActiveTables() []TableIncarnation`; `func TableRegistrySnapshotFromPB(*pb.TableRegistrySnapshot) (TableRegistrySnapshot, error)`; `func TableRegistrySnapshotToPB(TableRegistrySnapshot) *pb.TableRegistrySnapshot`.

- [ ] **Step 1: Branch, pin the proto and check the baseline**

```bash
cd arbiter-core && git fetch origin && git switch -c feat/si-data-plane-reconciler origin/main
go get github.com/sentioxyz/arbiter-proto@v0.8.0 && go mod tidy
grep 'sentioxyz/arbiter-proto' go.mod    # must print exactly: github.com/sentioxyz/arbiter-proto v0.8.0
bazel mod tidy && git status --short     # expect go.mod and go.sum only; MODULE.bazel.lock unchanged
go build ./... && go test ./... && bazel test //...
```

Expected: every package `ok` (the new RPCs add methods to the generated `Unimplemented*Server` types that every fake already embeds).

- [ ] **Step 2: Write the failing test**

Create `wire/table_registry_snapshot_test.go`:

```go
package wire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func registrySnapshotFixture(t *testing.T) TableRegistrySnapshot {
	t.Helper()
	schema := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "p",
		Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	ev := func(n uint64) *arbiter.L2EventRef {
		return &arbiter.L2EventRef{BlockNumber: n, BlockHash: "0xb", LogIndex: 1, TxHash: "0xt"}
	}
	return TableRegistrySnapshot{
		Params: arbiter.TableRegistryParams{ChainID: 7, DatabasesContract: "0x00000000000000000000000000000000000000d1",
			SIIndexerID: 1, ActivationBlock: 100, Confirmation: arbiter.TableRegistryConfirmationSafe},
		Version: 9, Seeded: true, Cursor: TableRegistryCursor{BlockNumber: 120, BlockHash: "0xc", LogIndex: 3, BlockComplete: true},
		Incarnations: []TableIncarnation{
			{Seq: 1, DatabaseID: "db", TableID: "g", Origin: TableOriginGenesis, Status: TableStatusActive, SchemaHash: "0xg"},
			{Seq: 2, DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusPurged, Created: ev(101), SchemaRef: ev(102),
				SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash("net", schema), SchemaJSON: string(js),
				Deleted: ev(103), RetireReason: TableRetireReasonTableDeleted, AddBlockSeq: 4, RetireBlockSeq: 6, PurgedBy: []string{"s1", "v1"}},
			{Seq: 3, DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusRefused, Created: ev(104), SchemaRef: ev(105),
				SchemaVersion: 1, RefusedCode: "column_type", RefusedReason: "column v has type UUID"},
			{Seq: 4, DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusPending, Created: ev(106), SchemaRef: ev(107),
				SchemaVersion: 2, SchemaHash: payloadexec.TableSchemaHash("net", schema), SchemaJSON: string(js)},
			{Seq: 5, DatabaseID: "db", TableID: "old", Origin: TableOriginLegacy, Status: TableStatusLegacy, Created: ev(50)},
		},
	}
}

func TestTableRegistrySnapshotRoundTrip(t *testing.T) {
	want := registrySnapshotFixture(t)
	b, err := proto.Marshal(TableRegistrySnapshotToPB(want))
	if err != nil {
		t.Fatal(err)
	}
	var m pb.TableRegistrySnapshot
	if err := proto.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	got, err := TableRegistrySnapshotFromPB(&m)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, want)
	}
}

func TestTableRegistrySnapshotRefusesUnknownVocabulary(t *testing.T) {
	for name, mutate := range map[string]func(*pb.TableRegistrySnapshot){
		"unspecified status": func(m *pb.TableRegistrySnapshot) {
			m.Incarnations[0].Status = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_UNSPECIFIED
		},
		"future status": func(m *pb.TableRegistrySnapshot) { m.Incarnations[0].Status = pb.TableIncarnationStatus(99) },
		"unspecified origin": func(m *pb.TableRegistrySnapshot) {
			m.Incarnations[0].Origin = pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_UNSPECIFIED
		},
		"future origin": func(m *pb.TableRegistrySnapshot) { m.Incarnations[0].Origin = pb.TableIncarnationOrigin(9) },
		"future reason": func(m *pb.TableRegistrySnapshot) { m.Incarnations[1].RetireReason = pb.TableRetireReason(3) },
		"misnumbered":   func(m *pb.TableRegistrySnapshot) { m.Incarnations[2].Seq = 7 },
	} {
		t.Run(name, func(t *testing.T) {
			m := TableRegistrySnapshotToPB(registrySnapshotFixture(t))
			mutate(m)
			if _, err := TableRegistrySnapshotFromPB(m); err == nil {
				t.Fatal("decoder accepted an unknown value")
			}
		})
	}
	if _, err := TableRegistrySnapshotFromPB(nil); err == nil {
		t.Fatal("nil snapshot must be refused")
	}
}

func TestTableRegistrySnapshotLiveAndActive(t *testing.T) {
	s := registrySnapshotFixture(t)
	if live := s.Live("db.t"); live == nil || live.Seq != 4 {
		t.Fatalf("Live(db.t) = %+v, want the Pending incarnation 4 (Refused 3 and Purged 2 never shadow it)", live)
	}
	s.Incarnations[3].Status = TableStatusPurged
	if live := s.Live("db.t"); live == nil || live.Seq != 4 {
		t.Fatalf("Live(db.t) with every incarnation terminal = %+v, want the newest (4)", live)
	}
	if live := s.Live("db.none"); live != nil {
		t.Fatalf("Live(db.none) = %+v, want nil", live)
	}
	active := s.ActiveTables()
	if len(active) != 1 || active[0].Key() != "db.g" {
		t.Fatalf("ActiveTables() = %+v", active)
	}
}

func TestTableIncarnationSchema(t *testing.T) {
	s := registrySnapshotFixture(t)
	schema, err := s.Incarnations[3].Schema()
	if err != nil {
		t.Fatal(err)
	}
	if schema.TableID != "db.t" || schema.PartitionBy != "p" || len(schema.Columns) != 2 {
		t.Fatalf("schema = %+v", schema)
	}
	if payloadexec.TableSchemaHash("net", schema) != s.Incarnations[3].SchemaHash {
		t.Fatal("decoded schema must hash to the registry hash")
	}
	if _, err := s.Incarnations[0].Schema(); err == nil || !strings.Contains(err.Error(), "no schema_json") {
		t.Fatalf("genesis incarnation schema err = %v", err)
	}
	bad := s.Incarnations[3]
	bad.TableID = "other"
	if _, err := bad.Schema(); err == nil {
		t.Fatal("schema_json naming another table must be refused")
	}
}
```

- [ ] **Step 3: Run it to verify it fails**

Run: `go test ./wire/ -run 'TableRegistrySnapshot|TableIncarnationSchema'`
Expected: build failure `undefined: TableRegistrySnapshot` (and the other new names).

- [ ] **Step 4: Implement the decoder**

Create `wire/table_registry_snapshot.go`:

```go
package wire

import (
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

// TableIncarnationStatus mirrors arbiter fsm.TableIncarnationStatus: the same
// lowercase spellings, one per pb.TableIncarnationStatus value.
type TableIncarnationStatus string

const (
	TableStatusLegacy   TableIncarnationStatus = "legacy"
	TableStatusPending  TableIncarnationStatus = "pending"
	TableStatusRefused  TableIncarnationStatus = "refused"
	TableStatusActive   TableIncarnationStatus = "active"
	TableStatusRetiring TableIncarnationStatus = "retiring"
	TableStatusPurging  TableIncarnationStatus = "purging"
	TableStatusPurged   TableIncarnationStatus = "purged"
)

// TableOrigin mirrors arbiter fsm.TableOrigin.
type TableOrigin string

const (
	TableOriginGenesis TableOrigin = "genesis"
	TableOriginLegacy  TableOrigin = "legacy"
	TableOriginChain   TableOrigin = "chain"
)

var tableStatusFromPB = map[pb.TableIncarnationStatus]TableIncarnationStatus{
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_LEGACY:   TableStatusLegacy,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PENDING:  TableStatusPending,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_REFUSED:  TableStatusRefused,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE:   TableStatusActive,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_RETIRING: TableStatusRetiring,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGING:  TableStatusPurging,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGED:   TableStatusPurged,
}

var tableOriginFromPB = map[pb.TableIncarnationOrigin]TableOrigin{
	pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_GENESIS: TableOriginGenesis,
	pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_LEGACY:  TableOriginLegacy,
	pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_CHAIN:   TableOriginChain,
}

// TableRegistryCursor mirrors pb.TableRegistryCursor.
type TableRegistryCursor struct {
	BlockNumber   uint64
	BlockHash     string
	LogIndex      uint64
	BlockComplete bool
}

// TableIncarnation mirrors pb.TableIncarnation (arbiter fsm.TableIncarnation).
type TableIncarnation struct {
	Seq            uint64
	DatabaseID     string
	TableID        string
	Origin         TableOrigin
	Status         TableIncarnationStatus
	Created        *arbiter.L2EventRef
	SchemaRef      *arbiter.L2EventRef
	SchemaVersion  uint32
	SchemaHash     string
	SchemaJSON     string
	RefusedReason  string
	RefusedCode    string
	Deleted        *arbiter.L2EventRef
	RetireReason   TableRetireReason
	AddBlockSeq    uint64
	RetireBlockSeq uint64
	PurgedBy       []string
}

// Key is the logical table id <database>.<table>.
func (t TableIncarnation) Key() string { return arbiter.TableKey(t.DatabaseID, t.TableID) }

// HasPhysicalTables reports whether data-plane nodes hold hg_* tables for the
// incarnation: Pending, Active, Retiring and Purging, as arbiter's
// fsm.TableIncarnation.hasPhysicalTables.
func (t TableIncarnation) HasPhysicalTables() bool {
	switch t.Status {
	case TableStatusPending, TableStatusActive, TableStatusRetiring, TableStatusPurging:
		return true
	}
	return false
}

// Schema decodes the registry's schema_json. A genesis incarnation carries no
// schema_json (its schema is configured statically), so it reports an error.
func (t TableIncarnation) Schema() (payloadexec.TableSchema, error) {
	if t.SchemaJSON == "" {
		return payloadexec.TableSchema{}, fmt.Errorf("table registry: incarnation %d of %s has no schema_json", t.Seq, t.Key())
	}
	return payloadexec.DecodeTableSchemaJSON(t.Key(), t.SchemaJSON)
}

// TableRegistrySnapshot mirrors pb.TableRegistrySnapshot: the whole committed
// registry at one version.
type TableRegistrySnapshot struct {
	Params       arbiter.TableRegistryParams
	Version      uint64
	Seeded       bool
	Cursor       TableRegistryCursor
	Incarnations []TableIncarnation
}

// Live mirrors arbiter's TableRegistryView.Live: the newest incarnation of key
// that is neither Purged nor Refused, else the newest one, else nil. The
// pointer aliases the snapshot's slice.
func (s TableRegistrySnapshot) Live(key string) *TableIncarnation {
	var newest *TableIncarnation
	for i := len(s.Incarnations) - 1; i >= 0; i-- {
		inc := &s.Incarnations[i]
		if inc.Key() != key {
			continue
		}
		if newest == nil {
			newest = inc
		}
		if inc.Status != TableStatusPurged && inc.Status != TableStatusRefused {
			return inc
		}
	}
	return newest
}

// ActiveTables returns the Active incarnations in seq order.
func (s TableRegistrySnapshot) ActiveTables() []TableIncarnation {
	var out []TableIncarnation
	for _, inc := range s.Incarnations {
		if inc.Status == TableStatusActive {
			out = append(out, inc)
		}
	}
	return out
}

// TableRegistrySnapshotFromPB decodes a snapshot. Every status and origin must
// be a known, specified value and every retire reason a known value; the
// incarnations must be numbered 1..n in order.
func TableRegistrySnapshotFromPB(m *pb.TableRegistrySnapshot) (TableRegistrySnapshot, error) {
	if m == nil {
		return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot is required")
	}
	out := TableRegistrySnapshot{Version: m.GetVersion(), Seeded: m.GetSeeded()}
	if p := TableRegistryParamsFromPB(m.GetParams()); p != nil {
		out.Params = *p
	}
	if c := m.GetCursor(); c != nil {
		out.Cursor = TableRegistryCursor{BlockNumber: c.GetBlockNumber(), BlockHash: c.GetBlockHash(), LogIndex: c.GetLogIndex(), BlockComplete: c.GetBlockComplete()}
	}
	for i, inc := range m.GetIncarnations() {
		if inc.GetSeq() != uint64(i)+1 {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has seq %d", i+1, inc.GetSeq())
		}
		status, ok := tableStatusFromPB[inc.GetStatus()]
		if !ok {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has unknown status %v", inc.GetSeq(), inc.GetStatus())
		}
		origin, ok := tableOriginFromPB[inc.GetOrigin()]
		if !ok {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has unknown origin %v", inc.GetSeq(), inc.GetOrigin())
		}
		reason := TableRetireReason(inc.GetRetireReason())
		switch reason {
		case TableRetireReasonUnspecified, TableRetireReasonTableDeleted, TableRetireReasonDatabaseDeleted:
		default:
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has unknown retire reason %v", inc.GetSeq(), inc.GetRetireReason())
		}
		out.Incarnations = append(out.Incarnations, TableIncarnation{
			Seq: inc.GetSeq(), DatabaseID: inc.GetDatabaseId(), TableID: inc.GetTableId(),
			Origin: origin, Status: status,
			Created: L2EventRefFromPB(inc.GetCreated()), SchemaRef: L2EventRefFromPB(inc.GetSchemaRef()),
			SchemaVersion: inc.GetSchemaVersion(), SchemaHash: inc.GetSchemaHash(), SchemaJSON: inc.GetSchemaJson(),
			RefusedReason: inc.GetRefusedReason(), RefusedCode: inc.GetRefusedCode(),
			Deleted: L2EventRefFromPB(inc.GetDeleted()), RetireReason: reason,
			AddBlockSeq: inc.GetAddBlockSeq(), RetireBlockSeq: inc.GetRetireBlockSeq(),
			PurgedBy: mapSlice(inc.GetPurgedBy(), func(id string) string { return id }),
		})
	}
	return out, nil
}

// TableRegistrySnapshotToPB returns an independent transport message. Fake
// arbiter servers in tests and hosts that re-serve a snapshot use it.
func TableRegistrySnapshotToPB(s TableRegistrySnapshot) *pb.TableRegistrySnapshot {
	params := s.Params
	out := &pb.TableRegistrySnapshot{
		Params: TableRegistryParamsToPB(&params), Version: s.Version, Seeded: s.Seeded,
		Cursor: &pb.TableRegistryCursor{BlockNumber: s.Cursor.BlockNumber, BlockHash: s.Cursor.BlockHash,
			LogIndex: s.Cursor.LogIndex, BlockComplete: s.Cursor.BlockComplete},
	}
	for _, inc := range s.Incarnations {
		out.Incarnations = append(out.Incarnations, &pb.TableIncarnation{
			Seq: inc.Seq, DatabaseId: inc.DatabaseID, TableId: inc.TableID,
			Origin: tableOriginToPB(inc.Origin), Status: tableStatusToPB(inc.Status),
			Created: L2EventRefToPB(inc.Created), SchemaRef: L2EventRefToPB(inc.SchemaRef),
			SchemaVersion: inc.SchemaVersion, SchemaHash: inc.SchemaHash, SchemaJson: inc.SchemaJSON,
			RefusedReason: inc.RefusedReason, RefusedCode: inc.RefusedCode,
			Deleted: L2EventRefToPB(inc.Deleted), RetireReason: pb.TableRetireReason(inc.RetireReason),
			AddBlockSeq: inc.AddBlockSeq, RetireBlockSeq: inc.RetireBlockSeq,
			PurgedBy: mapSlice(inc.PurgedBy, func(id string) string { return id }),
		})
	}
	return out
}

func tableStatusToPB(s TableIncarnationStatus) pb.TableIncarnationStatus {
	for k, v := range tableStatusFromPB {
		if v == s {
			return k
		}
	}
	return pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_UNSPECIFIED
}

func tableOriginToPB(o TableOrigin) pb.TableIncarnationOrigin {
	for k, v := range tableOriginFromPB {
		if v == o {
			return k
		}
	}
	return pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_UNSPECIFIED
}
```

Run: `bazel run //:gazelle`
Expected: `wire/BUILD.bazel` gains `table_registry_snapshot.go` and `@housegate//pkg/replay/payloadexec` in the library, and the test file plus `@housegate//pkg/lthash` and `@housegate//pkg/replay/payloadexec` in the test; nothing else changes.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./wire/ -count=1 && bazel test //wire:wire_test`
Expected: `ok github.com/sentioxyz/arbiter-core/wire`; `//wire:wire_test PASSED`. `TestTableRegistrySnapshotRefusesUnknownVocabulary` covers an UNSPECIFIED and a future status, an UNSPECIFIED and a future origin, a future retire reason and a misnumbered seq.

- [ ] **Step 6: Commit**

```bash
git add go.mod go.sum wire/
git commit -m "feat(wire): pin arbiter-proto v0.8.0 and decode the table registry snapshot with the Live rule" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 3: arbiter-core — the registry follower and the purge-report client

**Files:**
- Create: `dataplane/registry_follower.go`, `dataplane/table_purge.go`
- Modify: `dataplane/BUILD.bazel` (gazelle)
- Test: create `dataplane/registry_follower_test.go`

**Interfaces:**
- Consumes: `wire.TableRegistrySnapshot`, `wire.TableRegistrySnapshotFromPB` (Task 2); the unexported `runSubscription`, `recvStream`, `notLeaderHint`, `(*Client).WithLeaderRetry` (existing, `dataplane/subscribe.go`, `dataplane/client.go`); `pb.PromotionGatewayClient.SubmitTablePurged`, `pb.TableRegistryClient.GetPurgeNodeSet` (Task 1).
- Produces (package `dataplane`): `type RegistryView interface { View() (wire.TableRegistrySnapshot, bool); Changed() <-chan struct{}; Ready() <-chan struct{} }`; `type RegistryFollower struct`; `func NewRegistryFollower(c *Client, logger *slog.Logger) *RegistryFollower`; `func (f *RegistryFollower) Run(ctx context.Context) error`; `View`, `Changed`, `Ready`, `Connected() bool`; `func WaitReady(ctx context.Context, v RegistryView, timeout time.Duration) error`; `const DefaultRegistryStartupTimeout = 2 * time.Minute`; `const TableRegistryDisabledMessage = "table registry is disabled"`; `var ErrRegistryNotReady`; `func (c *Client) SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error`; `func (c *Client) PurgeNodeSet(ctx context.Context) ([]string, error)`; `var ErrTableNotPurging`.

- [ ] **Step 1: Write the failing tests**

Create `dataplane/registry_follower_test.go` (it reuses `streamNotLeaderErr` from `subscribe_test.go`):

```go
package dataplane

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// fakeRegistry serves TableRegistry and the purge RPC of PromotionGateway.
type fakeRegistry struct {
	pb.UnimplementedTableRegistryServer
	pb.UnimplementedPromotionGatewayServer

	mu        sync.Mutex
	getSnap   *pb.TableRegistrySnapshot
	getErr    error
	watchErr  error
	watchReqs []uint64
	sends     chan *pb.TableRegistrySnapshot
	end       chan error
	purgeErr  error
	purges    []*pb.RecordTablePurgedCmd
	nodeSet   []string
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{sends: make(chan *pb.TableRegistrySnapshot, 16), end: make(chan error, 1)}
}

func (r *fakeRegistry) GetTableRegistry(context.Context, *emptypb.Empty) (*pb.TableRegistrySnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getSnap, r.getErr
}

func (r *fakeRegistry) WatchTableRegistry(req *pb.WatchTableRegistryRequest, stream grpc.ServerStreamingServer[pb.TableRegistrySnapshot]) error {
	r.mu.Lock()
	r.watchReqs = append(r.watchReqs, req.GetSinceVersion())
	err := r.watchErr
	r.mu.Unlock()
	if err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-r.end:
			return err
		case m := <-r.sends:
			if err := stream.Send(m); err != nil {
				return err
			}
		}
	}
}

func (r *fakeRegistry) SubmitTablePurged(_ context.Context, cmd *pb.RecordTablePurgedCmd) (*pb.Ack, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.purges = append(r.purges, cmd)
	if r.purgeErr != nil {
		return nil, r.purgeErr
	}
	return &pb.Ack{}, nil
}

func (r *fakeRegistry) GetPurgeNodeSet(context.Context, *emptypb.Empty) (*pb.PurgeNodeSet, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return &pb.PurgeNodeSet{NodeIds: r.nodeSet}, nil
}

func (r *fakeRegistry) watchRequests() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.watchReqs...)
}

func startRegistryPeer(t *testing.T, r *fakeRegistry) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterTableRegistryServer(srv, r)
	pb.RegisterPromotionGatewayServer(srv, r)
	done := make(chan struct{})
	go func() { _ = srv.Serve(ln); close(done) }()
	t.Cleanup(func() { srv.Stop(); <-done })
	return ln.Addr().String()
}

func registrySnapshotPB(version uint64) *pb.TableRegistrySnapshot {
	return &pb.TableRegistrySnapshot{Version: version, Seeded: true, Incarnations: []*pb.TableIncarnation{{
		Seq: 1, DatabaseId: "db", TableId: "g", SchemaHash: "0xg",
		Origin: pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_GENESIS,
		Status: pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE,
	}}}
}

func disabledErr() error {
	return status.Error(codes.FailedPrecondition, TableRegistryDisabledMessage)
}

func newTestClient(t *testing.T, peers ...Peer) *Client {
	t.Helper()
	c, err := New(Config{Peers: peers, RetryBackoffMin: 5 * time.Millisecond, RetryBackoffMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func runFollower(t *testing.T, f *RegistryFollower) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	})
}

func waitChanged(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestRegistryFollower_DisabledThenFirstSnapshot(t *testing.T) {
	r := newFakeRegistry()
	r.getErr = disabledErr()
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), nil)
	runFollower(t, f)

	waitChanged(t, f.Ready(), "ready")
	if _, enabled := f.View(); enabled {
		t.Fatal("a disabled registry must report enabled=false")
	}
	changed := f.Changed()
	r.sends <- registrySnapshotPB(1)
	waitChanged(t, changed, "first snapshot")
	snap, enabled := f.View()
	if !enabled || snap.Version != 1 || len(snap.Incarnations) != 1 {
		t.Fatalf("view = %+v enabled=%v", snap, enabled)
	}
	if got := r.watchRequests(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("watch since_version = %v, want [0]", got)
	}
}

func TestRegistryFollower_ResumesFromLastVersionAfterDisconnect(t *testing.T) {
	r := newFakeRegistry()
	r.getSnap = registrySnapshotPB(3)
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), nil)
	runFollower(t, f)
	waitChanged(t, f.Ready(), "ready")
	if snap, enabled := f.View(); !enabled || snap.Version != 3 {
		t.Fatalf("view after Get = %+v enabled=%v", snap, enabled)
	}
	changed := f.Changed()
	r.sends <- registrySnapshotPB(4)
	waitChanged(t, changed, "version 4")
	r.end <- status.Error(codes.Unavailable, "stream reset")
	changed = f.Changed()
	r.sends <- registrySnapshotPB(6)
	waitChanged(t, changed, "version 6 after reconnect")
	if got := r.watchRequests(); len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("watch since_version = %v, want [3 4]", got)
	}
	if !f.Connected() {
		t.Fatal("an open stream must report connected")
	}
}

func TestRegistryFollower_FollowsNotLeader(t *testing.T) {
	follower := newFakeRegistry()
	follower.getErr = streamNotLeaderErr(t, "n2")
	follower.watchErr = streamNotLeaderErr(t, "n2")
	leader := newFakeRegistry()
	leader.getSnap = registrySnapshotPB(2)
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, follower)}, Peer{ID: "n2", GRPCAddr: startRegistryPeer(t, leader)})
	f := NewRegistryFollower(c, nil)
	runFollower(t, f)
	waitChanged(t, f.Ready(), "ready")
	if snap, enabled := f.View(); !enabled || snap.Version != 2 {
		t.Fatalf("view = %+v enabled=%v", snap, enabled)
	}
	changed := f.Changed()
	leader.sends <- registrySnapshotPB(3)
	waitChanged(t, changed, "version 3 from the leader")
	if got := leader.watchRequests(); len(got) == 0 || got[0] != 2 {
		t.Fatalf("leader watch since_version = %v, want [2 ...]", got)
	}
}

func TestRegistryFollower_IgnoresStaleVersions(t *testing.T) {
	r := newFakeRegistry()
	r.getSnap = registrySnapshotPB(5)
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), nil)
	runFollower(t, f)
	waitChanged(t, f.Ready(), "ready")
	changed := f.Changed()
	r.sends <- registrySnapshotPB(4)
	r.sends <- registrySnapshotPB(5)
	bad := registrySnapshotPB(6)
	bad.Incarnations[0].Status = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_UNSPECIFIED
	r.sends <- bad
	r.sends <- registrySnapshotPB(7)
	waitChanged(t, changed, "version 7")
	if snap, _ := f.View(); snap.Version != 7 {
		t.Fatalf("version = %d, want 7 (4 and 5 are stale, 6 does not decode)", snap.Version)
	}
}

func TestSubmitTablePurged_PreconditionIsNotSuccessAndNotALeaderMiss(t *testing.T) {
	r := newFakeRegistry()
	r.purgeErr = status.Error(codes.FailedPrecondition, "record table purged: incarnation 4 is not purging")
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.SubmitTablePurged(ctx, "s1", 4)
	if !errors.Is(err, ErrTableNotPurging) {
		t.Fatalf("err = %v, want ErrTableNotPurging", err)
	}
	r.mu.Lock()
	calls := len(r.purges)
	r.mu.Unlock()
	if calls != 1 {
		t.Fatalf("calls = %d, want exactly 1 (no leader-miss retry loop)", calls)
	}
	r.mu.Lock()
	r.purgeErr = nil
	r.nodeSet = []string{"s1", "v1"}
	r.mu.Unlock()
	if err := c.SubmitTablePurged(ctx, "s1", 4); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if got, err := c.PurgeNodeSet(ctx); err != nil || len(got) != 2 || got[1] != "v1" {
		t.Fatalf("PurgeNodeSet = %v, %v", got, err)
	}
}

func TestSubmitTablePurged_FollowsNotLeader(t *testing.T) {
	follower := newFakeRegistry()
	follower.purgeErr = streamNotLeaderErr(t, "n2")
	leader := newFakeRegistry()
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, follower)}, Peer{ID: "n2", GRPCAddr: startRegistryPeer(t, leader)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.SubmitTablePurged(ctx, "v1", 9); err != nil {
		t.Fatal(err)
	}
	leader.mu.Lock()
	defer leader.mu.Unlock()
	if len(leader.purges) != 1 || leader.purges[0].GetNodeId() != "v1" || leader.purges[0].GetIncarnationSeq() != 9 {
		t.Fatalf("leader purges = %v", leader.purges)
	}
}

func TestSubmitTablePurged_UnimplementedIsReturned(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterPromotionGatewayServer(srv, &pb.UnimplementedPromotionGatewayServer{})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: ln.Addr().String()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.SubmitTablePurged(ctx, "s1", 1); status.Code(err) != codes.Unimplemented {
		t.Fatalf("err = %v, want Unimplemented", err)
	}
}

func TestWaitReady_TimesOutWithoutAnArbiter(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: every Get fails with Unavailable
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: addr}), nil)
	runFollower(t, f)
	err = WaitReady(context.Background(), f, 200*time.Millisecond)
	if !errors.Is(err, ErrRegistryNotReady) {
		t.Fatalf("err = %v, want ErrRegistryNotReady", err)
	}
}

func TestRegistryFollower_ArbiterWithoutTheServiceCountsAsDisabled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterPromotionGatewayServer(srv, &pb.UnimplementedPromotionGatewayServer{})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: ln.Addr().String()}), nil)
	runFollower(t, f)
	if err := WaitReady(context.Background(), f, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, enabled := f.View(); enabled {
		t.Fatal("an arbiter without TableRegistry must read as a disabled registry")
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./dataplane/ -run 'RegistryFollower|SubmitTablePurged|WaitReady'`
Expected: build failure `undefined: RegistryFollower` (and `NewRegistryFollower`, `TableRegistryDisabledMessage`, `WaitReady`, `ErrRegistryNotReady`, `ErrTableNotPurging`, `c.SubmitTablePurged`, `c.PurgeNodeSet`).

- [ ] **Step 3: Implement the follower and the purge client**

Create `dataplane/registry_follower.go`:

```go
package dataplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core/wire"
)

// TableRegistryDisabledMessage is the FAILED_PRECONDITION message the arbiter
// leader answers GetTableRegistry with until governance enables the registry.
const TableRegistryDisabledMessage = "table registry is disabled"

// DefaultRegistryStartupTimeout bounds how long a role waits for the
// follower's first GetTableRegistry answer before its startup fails.
const DefaultRegistryStartupTimeout = 2 * time.Minute

// ErrRegistryNotReady reports that the follower had no answer from the
// arbiter within the startup timeout.
var ErrRegistryNotReady = errors.New("dataplane: table registry follower is not ready")

// RegistryView is the read side of a RegistryFollower. Roles and hosts take
// this interface so tests can supply a fixed view.
type RegistryView interface {
	// View returns the last accepted snapshot and whether the registry is
	// enabled. Before Ready, and while the registry is disabled, it returns
	// the zero snapshot and false. The snapshot is shared and read-only.
	View() (wire.TableRegistrySnapshot, bool)
	// Changed returns a channel closed when the next version is accepted
	// after this call.
	Changed() <-chan struct{}
	// Ready is closed once the first GetTableRegistry answered (with a
	// snapshot or "disabled").
	Ready() <-chan struct{}
}

// RegistryFollower follows the arbiter's dynamic SI table registry: one
// GetTableRegistry through WithLeaderRetry, then a resumable
// WatchTableRegistry stream built on the subscription loop (leader hints,
// reconnect, backoff, no per-call timeout). Each accepted message is the
// whole registry; only a strictly greater version is accepted.
type RegistryFollower struct {
	c      *Client
	logger *slog.Logger

	mu        sync.Mutex
	snap      wire.TableRegistrySnapshot
	enabled   bool
	changed   chan struct{}
	ready     chan struct{}
	readyOnce sync.Once
	connected atomic.Bool
}

// NewRegistryFollower returns a follower that reads through c. Call Run to
// start it; one follower serves every consumer in a process.
func NewRegistryFollower(c *Client, logger *slog.Logger) *RegistryFollower {
	if logger == nil {
		logger = slog.Default()
	}
	return &RegistryFollower{c: c, logger: logger, changed: make(chan struct{}), ready: make(chan struct{})}
}

// View implements RegistryView.
func (f *RegistryFollower) View() (wire.TableRegistrySnapshot, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap, f.enabled
}

// Changed implements RegistryView.
func (f *RegistryFollower) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

// Ready implements RegistryView.
func (f *RegistryFollower) Ready() <-chan struct{} { return f.ready }

// Connected reports whether a WatchTableRegistry stream is currently open.
// Hosts export it as the follower connection metric.
func (f *RegistryFollower) Connected() bool { return f.connected.Load() }

// WaitReady blocks until Ready or until timeout elapses (ErrRegistryNotReady).
func WaitReady(ctx context.Context, v RegistryView, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = DefaultRegistryStartupTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-v.Ready():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("%w after %s", ErrRegistryNotReady, timeout)
	}
}

// Run answers the startup GetTableRegistry and then follows
// WatchTableRegistry until ctx ends. It returns ctx's error, or a
// non-retryable transport error. An arbiter without the TableRegistry service
// (Unimplemented) counts as a disabled registry; the watch keeps retrying it
// at the maximum backoff.
func (f *RegistryFollower) Run(ctx context.Context) error {
	if err := f.initial(ctx); err != nil {
		return err
	}
	for {
		err := runSubscription(ctx, f.c, f.openWatch, f.deliver)
		f.connected.Store(false)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if status.Code(err) != codes.Unimplemented {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.c.cfg.RetryBackoffMax):
		}
	}
}

func (f *RegistryFollower) initial(ctx context.Context) error {
	var got *pb.TableRegistrySnapshot
	err := f.c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewTableRegistryClient(conn).GetTableRegistry(ctx, &emptypb.Empty{})
		switch {
		case err == nil:
			got = m
			return nil
		case registryDisabled(err):
			return nil
		case status.Code(err) == codes.Unimplemented:
			f.logger.Warn("arbiter has no TableRegistry service; following the configured genesis tables only")
			return nil
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("get table registry: %w", err)
	}
	if got != nil {
		f.deliver(got)
	}
	f.readyOnce.Do(func() { close(f.ready) })
	return nil
}

func (f *RegistryFollower) openWatch(ctx context.Context, conn *grpc.ClientConn) (recvStream[*pb.TableRegistrySnapshot], error) {
	f.mu.Lock()
	since := f.snap.Version
	f.mu.Unlock()
	stream, err := pb.NewTableRegistryClient(conn).WatchTableRegistry(ctx, &pb.WatchTableRegistryRequest{SinceVersion: since})
	if err != nil {
		return nil, err
	}
	f.connected.Store(true)
	return connectedStream{inner: stream, connected: &f.connected}, nil
}

// deliver accepts m when it decodes and its version is strictly greater than
// the current one. It never returns an error: the subscription loop ignores
// deliver's result, and a bad message must not end the stream.
func (f *RegistryFollower) deliver(m *pb.TableRegistrySnapshot) error {
	snap, err := wire.TableRegistrySnapshotFromPB(m)
	if err != nil {
		f.logger.Error("table registry snapshot refused", "version", m.GetVersion(), "err", err)
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.enabled && snap.Version <= f.snap.Version {
		return nil
	}
	f.snap, f.enabled = snap, true
	close(f.changed)
	f.changed = make(chan struct{})
	return nil
}

type connectedStream struct {
	inner     recvStream[*pb.TableRegistrySnapshot]
	connected *atomic.Bool
}

func (s connectedStream) Recv() (*pb.TableRegistrySnapshot, error) {
	m, err := s.inner.Recv()
	if err != nil {
		s.connected.Store(false)
	}
	return m, err
}

// registryDisabled reports the leader's "table registry is disabled" answer:
// FAILED_PRECONDITION without a NotLeader detail.
func registryDisabled(err error) bool {
	st, ok := leaderPrecondition(err)
	return ok && st.Message() == TableRegistryDisabledMessage
}

// leaderPrecondition returns err's status when it is a FAILED_PRECONDITION
// that carries no NotLeader detail, i.e. a genuine precondition answered by
// the leader rather than a redirect. WithLeaderRetry treats every
// FAILED_PRECONDITION as a leader miss, so callers must intercept these.
func leaderPrecondition(err error) (*status.Status, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return nil, false
	}
	for _, detail := range st.Details() {
		if _, isNotLeader := detail.(*pb.NotLeader); isNotLeader {
			return nil, false
		}
	}
	return st, true
}
```

Create `dataplane/table_purge.go`:

```go
package dataplane

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ErrTableNotPurging is SubmitTablePurged's FAILED_PRECONDITION answer: the
// incarnation is not Purging yet. It is retried, never read as success.
var ErrTableNotPurging = errors.New("dataplane: incarnation is not purging yet")

// SubmitTablePurged reports that nodeID dropped its hg_* tables for one
// Purging incarnation. It returns nil on Ack (including the idempotent
// already-recorded and already-Purged answers), an error wrapping
// ErrTableNotPurging on the leader's FAILED_PRECONDITION, and the gRPC error
// otherwise (an arbiter without the RPC answers Unimplemented; the caller
// retries it like a transport error).
func (c *Client) SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error {
	var precondition error
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).SubmitTablePurged(ctx, &pb.RecordTablePurgedCmd{NodeId: nodeID, IncarnationSeq: incarnationSeq})
		if st, ok := leaderPrecondition(err); ok {
			precondition = fmt.Errorf("%w: %s", ErrTableNotPurging, st.Message())
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return precondition
}

// PurgeNodeSet returns the arbiter's current purge node set: every
// registered, non-evicted SNode and verifier, sorted ascending.
func (c *Client) PurgeNodeSet(ctx context.Context) ([]string, error) {
	var out []string
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewTableRegistryClient(conn).GetPurgeNodeSet(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}
		out = m.GetNodeIds()
		return nil
	})
	return out, err
}
```

Run: `bazel run //:gazelle`
Expected: `dataplane/BUILD.bazel` gains the two sources, the test, and `@org_golang_google_protobuf//types/known/emptypb` in both rules; nothing else changes.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `go test -race -count=3 ./dataplane/ -run 'RegistryFollower|SubmitTablePurged|WaitReady|WithoutTheService' && go test ./dataplane/ -count=1 && bazel test //dataplane:dataplane_test`
Expected: `ok` three times under `-race` (measured on the prototype: 1.9 s), then the whole package `ok`, then `PASSED`.

- [ ] **Step 5: Commit**

```bash
git add dataplane/registry_follower.go dataplane/table_purge.go dataplane/registry_follower_test.go dataplane/BUILD.bazel
git commit -m "feat(dataplane): follow the table registry and report purges to the arbiter" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 4: arbiter-core — per-table protocol-table lifecycle in `ddl`

**Files:**
- Modify: `dataplane/ddl/build.go:20-30` (`TableIntent`), `:60-61` (`SQL`)
- Modify: `dataplane/ddl/verify.go:27-31`, `:42-44` (`VerifyProtocolTable`)
- Create: `dataplane/ddl/table.go`
- Modify: `dataplane/ddl/BUILD.bazel` (gazelle)
- Test: create `dataplane/ddl/table_ch_test.go`

**Interfaces:**
- Consumes: `ddl.Intents`, `ddl.VerifyProtocolTable`, `ddl.ZooKeeperPath`, `ddl.CHTableName`, `quoteIdent`, `quoteLiteral` (existing).
- Produces (package `ddl`): field `TableIntent.Comment string`; `func IncarnationComment(seq uint64) string`; `func ParseIncarnationComment(comment string) (uint64, bool)`; `func IncarnationIntents(p Pinned, t payloadexec.TableSchema, incarnationSeq uint64) (TableIntent, TableIntent, TableIntent, error)`; `func EnsureTable(ctx context.Context, conn clickhouse.Conn, p Pinned, t payloadexec.TableSchema, incarnationSeq uint64, mode Mode) error`; `func DropTable(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID string) error`; `func KeeperReplicas(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID string) ([]string, error)`; `func KeeperUnsafeTables(ctx context.Context, conn clickhouse.Conn, p Pinned) ([]string, error)`; `func DropReplica(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID, replica string) error`; `func ReplicaActive(err error) bool`; `type LocalTable struct { Database, Table, Comment string }`; `func ListProtocolTables(ctx context.Context, conn clickhouse.Conn, p Pinned) ([]LocalTable, error)`.

- [ ] **Step 1: Start two ClickHouse servers sharing one Keeper (once, for Tasks 4–7)**

This mirrors the CI job `integration-clickhouse` (`.github/workflows/ci.yml`):

```bash
cd arbiter-core
docker network create sp4-net
docker run -d --rm --name sp4-ch-a --hostname sp4-ch-a --network sp4-net --network-alias arbiter-core-clickhouse-a \
  -p 127.0.0.1::9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-shared-keeper-server.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" clickhouse/clickhouse-server:25.8
docker run -d --rm --name sp4-ch-b --hostname sp4-ch-b --network sp4-net \
  -p 127.0.0.1::9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-shared-keeper-client.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" clickhouse/clickhouse-server:25.8
for _ in $(seq 1 60); do docker exec sp4-ch-b clickhouse-client -q "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1 && break; sleep 1; done
export ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1
export CH_ADDR=$(docker port sp4-ch-a 9000) CH_REPLICA_ADDR=$(docker port sp4-ch-b 9000)
go test ./dataplane/ddl/ -count=1    # baseline: ok
```

Tear down after Task 7: `docker rm -f sp4-ch-a sp4-ch-b && docker network rm sp4-net`.

- [ ] **Step 2: Write the failing tests**

Create `dataplane/ddl/table_ch_test.go` (it reuses `requireCH`, `requireReplicaCH`, `requireKeeper`, `uniqueSuffix`, `testPinned`, `dropDatabasesSync` from `ch_test.go` and `ensureSchema` from `ensure_ch_test.go`):

```go
package ddl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func tableComments(t *testing.T, conn clickhouse.Conn, p Pinned, tableID string) map[string]string {
	t.Helper()
	tables, err := ListProtocolTables(context.Background(), conn, p)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, lt := range tables {
		if lt.Table == CHTableName(tableID) {
			out[lt.Database] = lt.Comment
		}
	}
	return out
}

// strandReplica leaves a replica named replica under tableID's Keeper path
// with no attached table anywhere: it creates hg_unsafe on the second server
// and detaches it, which is the Keeper state a crash between Keeper
// registration and local metadata leaves behind. The cleanup re-attaches and
// drops the detached table if its replica still exists.
func strandReplica(t *testing.T, replicaConn clickhouse.Conn, p Pinned, sch payloadexec.TableSchema, replica string) {
	t.Helper()
	ctx := context.Background()
	stranded := p
	stranded.NodeID = replica
	unsafe, _, _, err := Intents(stranded, sch)
	if err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(p.UnsafeDB)); err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, unsafe.SQL()); err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, fmt.Sprintf("DETACH TABLE %s.%s", quoteIdent(p.UnsafeDB), quoteIdent(unsafe.Table))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = replicaConn.Exec(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(p.UnsafeDB)+" SYNC")
	})
}

func TestEnsureTable_MarksVerifiesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)

	for i := 0; i < 2; i++ {
		if err := EnsureTable(ctx, conn, p, sch, 7, ModeCreateAndVerify); err != nil {
			t.Fatalf("ensure %d: %v", i, err)
		}
	}
	comments := tableComments(t, conn, p, sch.TableID)
	for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
		if comments[db] != "hg_incarnation=7" {
			t.Fatalf("%s comment = %q, want hg_incarnation=7 (all: %v)", db, comments[db], comments)
		}
	}
	unsafe, _, _, err := IncarnationIntents(p, sch, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyProtocolTable(ctx, conn, unsafe); !errors.Is(err, ErrProtocolTableDrift) {
		t.Fatalf("verify against incarnation 8 = %v, want comment drift", err)
	}
	if err := EnsureTable(ctx, conn, p, sch, 7, ModeVerifyOnly); err != nil {
		t.Fatalf("verify-only: %v", err)
	}
}

func TestEnsureTable_VerifyOnlyReportsMissing(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	err := EnsureTable(ctx, conn, p, ensureSchema(t), 0, ModeVerifyOnly)
	if !errors.Is(err, ErrProtocolTableMissing) {
		t.Fatalf("err = %v, want ErrProtocolTableMissing", err)
	}
}

func TestEnsureTable_RecoversItsOwnStrandedReplica(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	strandReplica(t, replicaConn, p, sch, p.NodeID)

	if err := EnsureTable(ctx, conn, p, sch, 3, ModeCreateAndVerify); err != nil {
		t.Fatalf("ensure over a stranded own replica: %v", err)
	}
	replicas, err := KeeperReplicas(ctx, conn, p, sch.TableID)
	if err != nil || !slices.Equal(replicas, []string{p.NodeID}) {
		t.Fatalf("replicas = %v, %v; want only %s", replicas, err, p.NodeID)
	}
}

func TestEnsureTable_RefusesToStealAnActiveReplica(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	dropDatabasesSync(t, replicaConn, p)
	sch := ensureSchema(t)
	if err := EnsureTable(ctx, replicaConn, p, sch, 3, ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	err := EnsureTable(ctx, conn, p, sch, 3, ModeCreateAndVerify)
	if err == nil || !ReplicaActive(err) {
		t.Fatalf("err = %v, want the active-replica refusal", err)
	}
}

func TestDropTable_RemovesTablesAndKeeperPathAndIsRepeatable(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	if err := EnsureTable(ctx, conn, p, sch, 5, ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := DropTable(ctx, conn, p, sch.TableID); err != nil {
			t.Fatalf("drop %d: %v", i, err)
		}
	}
	if left := tableComments(t, conn, p, sch.TableID); len(left) != 0 {
		t.Fatalf("tables left after drop: %v", left)
	}
	paths, err := KeeperUnsafeTables(ctx, conn, p)
	if err != nil || slices.Contains(paths, CHTableName(sch.TableID)) {
		t.Fatalf("keeper paths = %v, %v; the table path must be gone", paths, err)
	}
}

func TestDropTable_RemovesItsOwnKeeperOnlyReplica(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	strandReplica(t, replicaConn, p, sch, p.NodeID)
	if err := DropTable(ctx, conn, p, sch.TableID); err != nil {
		t.Fatal(err)
	}
	paths, err := KeeperUnsafeTables(ctx, conn, p)
	if err != nil || slices.Contains(paths, CHTableName(sch.TableID)) {
		t.Fatalf("keeper paths = %v, %v; a crash-stranded own replica must be removed", paths, err)
	}
}

func TestDropReplica_DecommissionedReplicaUnblocksSameNameRecreation(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	if err := EnsureTable(ctx, conn, p, sch, 4, ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	strandReplica(t, replicaConn, p, sch, "decommissioned")
	if err := DropTable(ctx, conn, p, sch.TableID); err != nil {
		t.Fatal(err)
	}
	replicas, err := KeeperReplicas(ctx, conn, p, sch.TableID)
	if err != nil || !slices.Equal(replicas, []string{"decommissioned"}) {
		t.Fatalf("replicas after own drop = %v, %v", replicas, err)
	}
	recreated := sch
	recreated.Columns = append(slices.Clone(sch.Columns), lthash.Column{Name: "w", Type: "String"})
	if err := EnsureTable(ctx, conn, p, recreated, 9, ModeCreateAndVerify); err == nil {
		t.Fatal("a same-name recreation with a new structure must fail while the stale path survives")
	}
	if err := DropTable(ctx, conn, p, sch.TableID); err != nil {
		t.Fatal(err)
	}
	if err := DropReplica(ctx, conn, p, sch.TableID, "decommissioned"); err != nil {
		t.Fatal(err)
	}
	if paths, err := KeeperUnsafeTables(ctx, conn, p); err != nil || slices.Contains(paths, CHTableName(sch.TableID)) {
		t.Fatalf("keeper paths = %v, %v; dropping the last replica must remove the path", paths, err)
	}
	if err := EnsureTable(ctx, conn, p, recreated, 9, ModeCreateAndVerify); err != nil {
		t.Fatalf("same-name recreation after the sweep: %v", err)
	}
}
```

- [ ] **Step 3: Run them to verify they fail**

Run: `go test ./dataplane/ddl/ -count=1`
Expected: build failure `undefined: ListProtocolTables`, `undefined: EnsureTable`, `undefined: IncarnationIntents` (and the other new names).

- [ ] **Step 4: Add the comment to the intent and to verification**

In `dataplane/ddl/build.go` replace:

```go
	SortingKey    []string
	Settings      []PinnedSetting
}
```

with:

```go
	SortingKey    []string
	Settings      []PinnedSetting
	// Comment marks the tables of one chain-origin registry incarnation
	// (IncarnationComment). Genesis tables carry none.
	Comment string
}
```

and replace:

```go
	b.WriteString("SETTINGS " + strings.Join(settings, ", "))
	return b.String()
```

with:

```go
	b.WriteString("SETTINGS " + strings.Join(settings, ", "))
	if t.Comment != "" {
		b.WriteString("\nCOMMENT " + quoteLiteral(t.Comment))
	}
	return b.String()
```

In `dataplane/ddl/verify.go` replace:

```go
	var engine, engineFull, sortingKey, partitionKey string
	err := conn.QueryRow(ctx, `
		SELECT engine, engine_full, sorting_key, partition_key
		FROM system.tables WHERE database = ? AND name = ?`, want.Database, want.Table,
	).Scan(&engine, &engineFull, &sortingKey, &partitionKey)
```

with:

```go
	var engine, engineFull, sortingKey, partitionKey, comment string
	err := conn.QueryRow(ctx, `
		SELECT engine, engine_full, sorting_key, partition_key, comment
		FROM system.tables WHERE database = ? AND name = ?`, want.Database, want.Table,
	).Scan(&engine, &engineFull, &sortingKey, &partitionKey, &comment)
```

and replace:

```go
	if engine != want.Engine {
		drift("engine", engine, want.Engine)
	}
```

with:

```go
	if engine != want.Engine {
		drift("engine", engine, want.Engine)
	}
	if want.Comment != "" && comment != want.Comment {
		drift("comment", comment, want.Comment)
	}
```

- [ ] **Step 5: Implement the per-table lifecycle**

Create `dataplane/ddl/table.go`:

```go
package ddl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ClickHouse error codes the per-table lifecycle distinguishes.
const (
	codeReplicaAlreadyExists = 253 // REPLICA_ALREADY_EXISTS
	codeTableWasNotDropped   = 305 // TABLE_WAS_NOT_DROPPED ("because it's active")
)

// incarnationCommentPrefix starts the COMMENT of every table created for a
// chain-origin registry incarnation.
const incarnationCommentPrefix = "hg_incarnation="

// IncarnationComment is the COMMENT that marks the protocol tables of
// registry incarnation seq. It lets a node tell its tables for the current
// incarnation apart from leftovers of an earlier incarnation of the same key.
func IncarnationComment(seq uint64) string {
	return incarnationCommentPrefix + strconv.FormatUint(seq, 10)
}

// ParseIncarnationComment returns the incarnation a table COMMENT names; ok is
// false for an empty or foreign comment (a genesis table has none).
func ParseIncarnationComment(comment string) (uint64, bool) {
	rest, found := strings.CutPrefix(comment, incarnationCommentPrefix)
	if !found {
		return 0, false
	}
	seq, err := strconv.ParseUint(rest, 10, 64)
	if err != nil || seq == 0 || strconv.FormatUint(seq, 10) != rest {
		return 0, false
	}
	return seq, true
}

// IncarnationIntents is Intents for one registry incarnation: incarnationSeq
// 0 renders the unmarked genesis tables, any other value marks all three with
// IncarnationComment(incarnationSeq).
func IncarnationIntents(p Pinned, t payloadexec.TableSchema, incarnationSeq uint64) (TableIntent, TableIntent, TableIntent, error) {
	unsafe, safe, promote, err := Intents(p, t)
	if err != nil {
		return TableIntent{}, TableIntent{}, TableIntent{}, err
	}
	if incarnationSeq != 0 {
		comment := IncarnationComment(incarnationSeq)
		unsafe.Comment, safe.Comment, promote.Comment = comment, comment, comment
	}
	return unsafe, safe, promote, nil
}

// EnsureTable creates (ModeCreateAndVerify) and verifies the hg_unsafe,
// hg_safe and hg_promote tables of one table; ModeVerifyOnly only verifies and
// ModeOff does nothing. incarnationSeq is 0 for a genesis table and the
// registry incarnation otherwise (IncarnationIntents). A crash between
// ClickHouse registering this node's hg_unsafe replica in Keeper and writing
// the local table leaves the replica behind, and the next CREATE fails with
// REPLICA_ALREADY_EXISTS; EnsureTable then removes the stale replica (refused
// by ClickHouse while any server holds it active) and creates once more.
func EnsureTable(ctx context.Context, conn clickhouse.Conn, p Pinned, t payloadexec.TableSchema, incarnationSeq uint64, mode Mode) error {
	if mode == ModeOff {
		return nil
	}
	if err := validatePinned(conn, p); err != nil {
		return err
	}
	unsafe, safe, promote, err := IncarnationIntents(p, t, incarnationSeq)
	if err != nil {
		return err
	}
	intents := []TableIntent{unsafe, safe, promote}
	if mode == ModeCreateAndVerify {
		for _, database := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
			if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(database)); err != nil {
				return fmt.Errorf("ddl: create database %s: %w", database, err)
			}
		}
		for _, intent := range intents {
			err := conn.Exec(ctx, intent.SQL())
			if err != nil && intent.Engine == EngineReplicatedMergeTree && clickHouseCode(err) == codeReplicaAlreadyExists {
				if dropErr := DropReplica(ctx, conn, p, t.TableID, p.NodeID); dropErr != nil {
					return fmt.Errorf("ddl: create %s.%s: stale replica %s: %w", intent.Database, intent.Table, p.NodeID, dropErr)
				}
				err = conn.Exec(ctx, intent.SQL())
			}
			if err != nil {
				return fmt.Errorf("ddl: create %s.%s: %w", intent.Database, intent.Table, err)
			}
		}
	}
	var errs []error
	for _, intent := range intents {
		if err := VerifyProtocolTable(ctx, conn, intent); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// DropTable drops tableID's hg_promote, hg_safe and hg_unsafe tables, in that
// order, each with DROP TABLE IF EXISTS ... SYNC. Dropping hg_unsafe removes
// this node's replica from Keeper, and the last replica removes the table's
// Keeper path. If a crash separated the local drop from the Keeper removal,
// this node's replica is still listed with no local table; DropTable removes
// it too, so a repeated call converges.
func DropTable(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID string) error {
	if err := validatePinned(conn, p); err != nil {
		return err
	}
	table := CHTableName(tableID)
	for _, database := range []string{p.PromoteDB, p.SafeDB, p.UnsafeDB} {
		if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC", quoteIdent(database), quoteIdent(table))); err != nil {
			return fmt.Errorf("ddl: drop %s.%s: %w", database, table, err)
		}
	}
	replicas, err := KeeperReplicas(ctx, conn, p, tableID)
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		if replica == p.NodeID {
			return DropReplica(ctx, conn, p, tableID, replica)
		}
	}
	return nil
}

// KeeperReplicas returns the sorted replica names registered under tableID's
// Keeper path; none when the path does not exist.
func KeeperReplicas(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID string) ([]string, error) {
	return keeperChildren(ctx, conn, ZooKeeperPath(p, tableID)+"/replicas")
}

// KeeperUnsafeTables returns the physical table names that have a Keeper path
// under /sentio/<keeper_shard_id>/unsafe.
func KeeperUnsafeTables(ctx context.Context, conn clickhouse.Conn, p Pinned) ([]string, error) {
	return keeperChildren(ctx, conn, fmt.Sprintf("/sentio/%d/unsafe", p.KeeperShardID))
}

// DropReplica removes one replica of tableID's hg_unsafe table from Keeper
// (SYSTEM DROP REPLICA ... FROM ZKPATH). ClickHouse refuses an active replica;
// removing the last replica also removes the table's Keeper path.
func DropReplica(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID, replica string) error {
	path := ZooKeeperPath(p, tableID)
	if err := conn.Exec(ctx, fmt.Sprintf("SYSTEM DROP REPLICA %s FROM ZKPATH %s", quoteLiteral(replica), quoteLiteral(path))); err != nil {
		return fmt.Errorf("ddl: drop replica %s from %s: %w", replica, path, err)
	}
	return nil
}

// ReplicaActive reports whether err is ClickHouse refusing to drop an active
// replica.
func ReplicaActive(err error) bool { return clickHouseCode(err) == codeTableWasNotDropped }

// LocalTable is one table in a protocol database.
type LocalTable struct {
	Database string
	Table    string
	Comment  string
}

// ListProtocolTables returns every table of the three protocol databases,
// sorted by table then database.
func ListProtocolTables(ctx context.Context, conn clickhouse.Conn, p Pinned) ([]LocalTable, error) {
	rows, err := conn.Query(ctx, `SELECT database, name, comment FROM system.tables WHERE database IN (?, ?, ?)`, p.UnsafeDB, p.SafeDB, p.PromoteDB)
	if err != nil {
		return nil, fmt.Errorf("ddl: list protocol tables: %w", err)
	}
	defer rows.Close()
	var out []LocalTable
	for rows.Next() {
		var t LocalTable
		if err := rows.Scan(&t.Database, &t.Table, &t.Comment); err != nil {
			return nil, fmt.Errorf("ddl: scan protocol tables: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ddl: list protocol tables: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Database < out[j].Database
	})
	return out, nil
}

func keeperChildren(ctx context.Context, conn clickhouse.Conn, path string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT name FROM system.zookeeper WHERE path = ?`, path)
	if err != nil {
		return nil, fmt.Errorf("ddl: list keeper path %s: %w", path, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("ddl: scan keeper path %s: %w", path, err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ddl: list keeper path %s: %w", path, err)
	}
	sort.Strings(out)
	return out, nil
}

func validatePinned(conn clickhouse.Conn, p Pinned) error {
	if conn == nil {
		return errors.New("ddl: clickhouse connection is required")
	}
	if p.UnsafeDB == "" || p.SafeDB == "" || p.PromoteDB == "" || p.NodeID == "" {
		return errors.New("ddl: Pinned needs UnsafeDB, SafeDB, PromoteDB and NodeID")
	}
	return nil
}

func clickHouseCode(err error) int32 {
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		return exception.Code
	}
	return 0
}
```

Run: `bazel run //:gazelle`
Expected: `dataplane/ddl/BUILD.bazel` gains `table.go` and `table_ch_test.go`; nothing else changes.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./dataplane/ddl/ -count=1 -v -run 'EnsureTable|DropTable|DropReplica' 2>&1 | grep -E '^(---|ok)' && go test ./dataplane/ddl/ -count=1 && bazel test //dataplane/ddl:ddl_test`
Expected: seven `--- PASS` lines and no `SKIP` (with the Step 1 environment); the package `ok` (the existing `BuildDDL` golden and `EnsureProtocolTables` tests are unchanged because genesis intents carry no comment); `PASSED` (Bazel runs without the docker environment, so the new tests skip there). Afterwards `docker exec sp4-ch-a clickhouse-client -q "SELECT name FROM system.zookeeper WHERE path = '/sentio/0/unsafe'"` prints nothing: every test removes its Keeper path.

- [ ] **Step 7: Commit**

```bash
git add dataplane/ddl/
git commit -m "feat(ddl): per-table ensure and drop with incarnation markers and Keeper replica cleanup" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 5: arbiter-core — the table-set reconciler

**Files:**
- Create: `dataplane/tableset/reconciler.go`, `dataplane/tableset/BUILD.bazel` (gazelle)
- Modify: `.github/workflows/ci.yml:96-101` (`integration-clickhouse` target list), `README.md:17` (package table)
- Test: create `dataplane/tableset/reconciler_ch_test.go`

**Interfaces:**
- Consumes: `dataplane.RegistryView` (Task 3); `wire.TableRegistrySnapshot`, `wire.TableIncarnation` and its methods (Task 2); `ddl.EnsureTable`, `ddl.DropTable`, `ddl.DropReplica`, `ddl.KeeperReplicas`, `ddl.KeeperUnsafeTables`, `ddl.ListProtocolTables`, `ddl.IncarnationComment`, `ddl.ParseIncarnationComment` (Task 4); `ddl.FatalReconcileError`, `ddl.ReconcileBackoff`, `ddl.DefaultReconcileInterval`, `ddl.ValidatePhysicalTableNames` (existing).
- Produces (package `tableset`, import path `github.com/sentioxyz/arbiter-core/dataplane/tableset`): `type State string` with `StateCreating`, `StateReady`, `StateWaitingQuiescence`, `StatePurging`, `StatePurgeReported`; `type Arbiter interface { SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error; PurgeNodeSet(ctx context.Context) ([]string, error) }` (satisfied by `*dataplane.Client`); `type Config struct { Pinned ddl.Pinned; Genesis []payloadexec.TableSchema; Interval time.Duration; SweepDecommissioned bool }`; `type Deps struct { Conn clickhouse.Conn; Registry dataplane.RegistryView; Arbiter Arbiter; Quiescent func(tableID string) (bool, error); Logger *slog.Logger }`; `type Stats struct { States map[State]int; Failures map[string]uint64; Unknown []string; LeftoverDrops uint64 }`; `func New(cfg Config, d Deps) (*Reconciler, error)`; methods `Reconcile(ctx context.Context, genesisMode ddl.Mode) error`, `Ready(tableID string) bool`, `WaitReady(ctx context.Context, tableIDs []string, timeout time.Duration) bool`, `Trigger()`, `Wake() (registry <-chan struct{}, trigger <-chan struct{})`, `NextDelay() time.Duration`, `Stats() Stats`. `Ready`, `Trigger`, `Wake` and `NextDelay` are nil-receiver safe.

- [ ] **Step 1: Write the failing tests**

Create `dataplane/tableset/reconciler_ch_test.go` (the Task 4 Step 1 environment must be exported):

```go
package tableset

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

const testNetwork = "net-tableset"

func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn := openCH(t, addr)
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but ClickHouse has no Keeper: %v", err)
		}
		t.Skipf("ClickHouse has no Keeper: %v", err)
	}
	return conn
}

func requireReplicaCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_REPLICA") != "1" {
		t.Skip("set ARBITER_CH_REPLICA=1 and CH_REPLICA_ADDR to run the two-node tests")
	}
	addr := os.Getenv("CH_REPLICA_ADDR")
	if addr == "" {
		t.Fatal("ARBITER_CH_REPLICA=1 requires CH_REPLICA_ADDR")
	}
	return openCH(t, addr)
}

func openCH(t *testing.T, addr string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func suffix(t *testing.T) string {
	sum := sha1.Sum([]byte(t.Name()))
	return hex.EncodeToString(sum[:])[:10]
}

func testPinned(t *testing.T, conns ...clickhouse.Conn) ddl.Pinned {
	t.Helper()
	s := suffix(t)
	p := ddl.Pinned{UnsafeDB: "hg_unsafe_" + s, SafeDB: "hg_safe_" + s, PromoteDB: "hg_promote_" + s, NodeID: "node-" + s}
	t.Cleanup(func() {
		for _, conn := range conns {
			for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
				_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
			}
		}
	})
	return p
}

func schemaFor(t *testing.T, name string, extra ...lthash.Column) payloadexec.TableSchema {
	cols := []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}
	return payloadexec.TableSchema{TableID: "db." + name + "_" + suffix(t), PartitionBy: "p", Columns: append(cols, extra...)}
}

func chainInc(t *testing.T, seq uint64, schema payloadexec.TableSchema, st wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	db, table, _ := cutKey(schema.TableID)
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: st,
		SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash(testNetwork, schema), SchemaJSON: string(js)}
}

func cutKey(key string) (string, string, bool) {
	for i := 0; i < len(key); i++ {
		if key[i] == '.' {
			return key[:i], key[i+1:], true
		}
	}
	return "", "", false
}

// fakeView is a settable RegistryView.
type fakeView struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	changed chan struct{}
	ready   chan struct{}
}

func newFakeView() *fakeView {
	v := &fakeView{changed: make(chan struct{}), ready: make(chan struct{})}
	close(v.ready)
	return v
}

func (v *fakeView) View() (wire.TableRegistrySnapshot, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snap, v.enabled
}

func (v *fakeView) Changed() <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.changed
}

func (v *fakeView) Ready() <-chan struct{} { return v.ready }

// set installs the incarnations as the next version (seqs are renumbered
// 1..n by the caller's order).
func (v *fakeView) set(incs ...wire.TableIncarnation) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snap = wire.TableRegistrySnapshot{Version: v.snap.Version + 1, Seeded: true, Incarnations: incs}
	v.enabled = true
	close(v.changed)
	v.changed = make(chan struct{})
}

type fakeArbiter struct {
	mu      sync.Mutex
	purges  []string
	err     error
	nodeSet []string
}

func (a *fakeArbiter) SubmitTablePurged(_ context.Context, nodeID string, seq uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.purges = append(a.purges, fmt.Sprintf("%s/%d", nodeID, seq))
	return a.err
}

func (a *fakeArbiter) PurgeNodeSet(context.Context) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.nodeSet), nil
}

func (a *fakeArbiter) reported() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.purges)
}

func newReconciler(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, view *fakeView, arb *fakeArbiter, mutate func(*Config, *Deps)) *Reconciler {
	t.Helper()
	cfg := Config{Pinned: p}
	deps := Deps{Conn: conn, Registry: view, Arbiter: arb}
	if mutate != nil {
		mutate(&cfg, &deps)
	}
	r, err := New(cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func localComments(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, tableID string) map[string]string {
	t.Helper()
	tables, err := ddl.ListProtocolTables(context.Background(), conn, p)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, lt := range tables {
		if lt.Table == ddl.CHTableName(tableID) {
			out[lt.Database] = lt.Comment
		}
	}
	return out
}

func keeperPathExists(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, tableID string) bool {
	t.Helper()
	paths, err := ddl.KeeperUnsafeTables(context.Background(), conn, p)
	if err != nil {
		t.Fatal(err)
	}
	return slices.Contains(paths, ddl.CHTableName(tableID))
}

func mustReconcile(t *testing.T, r *Reconciler) {
	t.Helper()
	if err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func TestReconciler_CreatesPendingThenKeepsActiveReady(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusPending))
	if r.Ready(s.TableID) {
		t.Fatal("ready before any pass")
	}
	mustReconcile(t, r)
	if !r.Ready(s.TableID) {
		t.Fatal("pending table not ready after a pass")
	}
	for db, c := range localComments(t, conn, p, s.TableID) {
		if c != "hg_incarnation=1" {
			t.Fatalf("%s comment = %q", db, c)
		}
	}
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	if !r.Ready(s.TableID) || r.Stats().States[StateReady] != 1 {
		t.Fatalf("active table not ready: %+v", r.Stats())
	}
}

func TestReconciler_PurgingDropsTablesAndKeeperPathThenReports(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if left := localComments(t, conn, p, s.TableID); len(left) != 0 {
		t.Fatalf("tables left: %v", left)
	}
	if keeperPathExists(t, conn, p, s.TableID) {
		t.Fatal("the last replica's drop must remove the Keeper table path")
	}
	if got := arb.reported(); !slices.Equal(got, []string{p.NodeID + "/1"}) {
		t.Fatalf("reports = %v", got)
	}
	if r.Ready(s.TableID) {
		t.Fatal("a purged table is not ready")
	}
	inc := chainInc(t, 1, s, wire.TableStatusPurging)
	inc.PurgedBy = []string{p.NodeID}
	view.set(inc)
	mustReconcile(t, r)
	if got := arb.reported(); len(got) != 1 {
		t.Fatalf("a recorded purge must not be resubmitted: %v", got)
	}
}

func TestReconciler_PurgeWaitsForQuiescence(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	quiet := false
	r := newReconciler(t, conn, p, view, arb, func(_ *Config, d *Deps) {
		d.Quiescent = func(string) (bool, error) { return quiet, nil }
	})
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if len(localComments(t, conn, p, s.TableID)) != 3 || len(arb.reported()) != 0 {
		t.Fatal("a non-quiescent table must be neither dropped nor reported")
	}
	if r.Stats().States[StateWaitingQuiescence] != 1 {
		t.Fatalf("stats = %+v", r.Stats())
	}
	quiet = true
	mustReconcile(t, r)
	if len(localComments(t, conn, p, s.TableID)) != 0 || len(arb.reported()) != 1 {
		t.Fatal("a quiescent Purging table must be dropped and reported")
	}
}

func TestReconciler_SweepsDecommissionedReplicas(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t, conn, replicaConn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned = true })
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	// Two more replicas on the second server, in their own databases (the
	// Keeper path depends on the table only): verifier-live is a current node
	// that has not purged yet; verifier-gone is decommissioned (detached).
	live, gone := p, p
	live.UnsafeDB, live.SafeDB, live.PromoteDB, live.NodeID = p.UnsafeDB+"_live", p.SafeDB+"_live", p.PromoteDB+"_live", "verifier-live"
	gone.UnsafeDB, gone.SafeDB, gone.PromoteDB, gone.NodeID = p.UnsafeDB+"_gone", p.SafeDB+"_gone", p.PromoteDB+"_gone", "verifier-gone"
	t.Cleanup(func() {
		for _, q := range []ddl.Pinned{live, gone} {
			for _, db := range []string{q.UnsafeDB, q.SafeDB, q.PromoteDB} {
				_ = replicaConn.Exec(ctx, "DROP DATABASE IF EXISTS "+db+" SYNC")
			}
		}
	})
	for _, q := range []ddl.Pinned{live, gone} {
		if err := ddl.EnsureTable(ctx, replicaConn, q, s, 1, ddl.ModeCreateAndVerify); err != nil {
			t.Fatal(err)
		}
	}
	if err := replicaConn.Exec(ctx, fmt.Sprintf("DETACH TABLE %s.%s", gone.UnsafeDB, ddl.CHTableName(s.TableID))); err != nil {
		t.Fatal(err)
	}
	arb.nodeSet = []string{p.NodeID, "verifier-live"}
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	replicas, err := ddl.KeeperReplicas(ctx, conn, p, s.TableID)
	if err != nil || !slices.Equal(replicas, []string{"verifier-live"}) {
		t.Fatalf("replicas = %v, %v; want only the current verifier-live", replicas, err)
	}
	if got := arb.reported(); !slices.Equal(got, []string{p.NodeID + "/1"}) {
		t.Fatalf("reports = %v", got)
	}
	// verifier-live purges its own tables: the last replica removes the path.
	if err := ddl.DropTable(ctx, replicaConn, live, s.TableID); err != nil {
		t.Fatal(err)
	}
	if keeperPathExists(t, conn, p, s.TableID) {
		t.Fatal("keeper path must be gone once every current node dropped")
	}
}

func TestReconciler_ConvergesAfterCrashMidCreate(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	unsafe, _, _, err := ddl.IncarnationIntents(p, s, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(context.Background(), "CREATE DATABASE IF NOT EXISTS "+p.UnsafeDB); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(context.Background(), unsafe.SQL()); err != nil {
		t.Fatal(err)
	}
	view.set(chainInc(t, 1, s, wire.TableStatusPending))
	mustReconcile(t, r)
	if got := localComments(t, conn, p, s.TableID); len(got) != 3 || !r.Ready(s.TableID) {
		t.Fatalf("after a crash mid-create: tables %v ready %v", got, r.Ready(s.TableID))
	}
}

func TestReconciler_ConvergesAfterCrashMidPurge(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	for _, db := range []string{p.PromoteDB, p.SafeDB} {
		if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE %s.%s SYNC", db, ddl.CHTableName(s.TableID))); err != nil {
			t.Fatal(err)
		}
	}
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if len(localComments(t, conn, p, s.TableID)) != 0 || keeperPathExists(t, conn, p, s.TableID) || len(arb.reported()) != 1 {
		t.Fatal("a purge interrupted after hg_safe must converge")
	}
}

func TestReconciler_DropsLeftoversOfRetiredKeysAndEarlierIncarnations(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	gone, reused := schemaFor(t, "gone"), schemaFor(t, "reused")
	for _, s := range []payloadexec.TableSchema{gone, reused} {
		if err := ddl.EnsureTable(context.Background(), conn, p, s, 1, ddl.ModeCreateAndVerify); err != nil {
			t.Fatal(err)
		}
	}
	// gone: its only incarnation is Purged. reused: incarnation 2 is Purged
	// and a new Pending incarnation 3 exists (this node missed the purge).
	g := chainInc(t, 1, gone, wire.TableStatusPurged)
	old := chainInc(t, 2, reused, wire.TableStatusPurged)
	next := chainInc(t, 3, reused, wire.TableStatusPending)
	// The tables were created as incarnation 1; renumber reused's marker to 2.
	if err := ddl.DropTable(context.Background(), conn, p, reused.TableID); err != nil {
		t.Fatal(err)
	}
	if err := ddl.EnsureTable(context.Background(), conn, p, reused, 2, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view.set(g, old, next)
	mustReconcile(t, r)
	if left := localComments(t, conn, p, gone.TableID); len(left) != 0 {
		t.Fatalf("leftover of a Purged key survives: %v", left)
	}
	for db, c := range localComments(t, conn, p, reused.TableID) {
		if c != "hg_incarnation=3" {
			t.Fatalf("%s comment = %q, want the new incarnation", db, c)
		}
	}
	if st := r.Stats(); st.LeftoverDrops != 2 || !r.Ready(reused.TableID) {
		t.Fatalf("stats = %+v ready=%v", st, r.Ready(reused.TableID))
	}
}

func TestReconciler_ForeignMarkerIsDriftNotALeftover(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	if err := ddl.EnsureTable(context.Background(), conn, p, s, 9, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view.set(chainInc(t, 1, s, wire.TableStatusPending))
	err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly)
	if !errors.Is(err, ddl.ErrProtocolTableDrift) {
		t.Fatalf("err = %v, want drift", err)
	}
	if got := localComments(t, conn, p, s.TableID); len(got) != 3 {
		t.Fatalf("an unattributable table must never be dropped: %v", got)
	}
}

func TestReconciler_UnknownTablesAreOnlyReported(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	stray := schemaFor(t, "stray")
	if err := ddl.EnsureTable(context.Background(), conn, p, stray, 0, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view.set()
	mustReconcile(t, r)
	if got := localComments(t, conn, p, stray.TableID); len(got) != 3 {
		t.Fatalf("unknown tables must survive: %v", got)
	}
	if st := r.Stats(); len(st.Unknown) != 3 {
		t.Fatalf("unknown = %v", st.Unknown)
	}
}

func TestReconciler_IsolatesPerTableFailures(t *testing.T) {
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t, conn, replicaConn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	blocked, fine := schemaFor(t, "blocked"), schemaFor(t, "fine")
	// Another server actively holds this node's replica name for blocked.
	if err := ddl.EnsureTable(context.Background(), replicaConn, p, blocked, 1, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view.set(chainInc(t, 1, blocked, wire.TableStatusPending), chainInc(t, 2, fine, wire.TableStatusPending))
	mustReconcile(t, r)
	if r.Ready(blocked.TableID) || !r.Ready(fine.TableID) {
		t.Fatalf("ready blocked=%v fine=%v", r.Ready(blocked.TableID), r.Ready(fine.TableID))
	}
	mustReconcile(t, r)
	if st := r.Stats(); st.Failures[blocked.TableID] != 1 {
		t.Fatalf("a backed-off table must not be retried by the next pass: %+v", st)
	}
	r.Trigger()
	mustReconcile(t, r)
	if st := r.Stats(); st.Failures[blocked.TableID] != 2 {
		t.Fatalf("Trigger clears the backoff: %+v", st)
	}
}

func TestReconciler_UnimplementedPurgeIsRetriedNotFatal(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view := newFakeView()
	arb := &fakeArbiter{err: status.Error(codes.Unimplemented, "unknown method SubmitTablePurged")}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if st := r.Stats(); st.States[StatePurging] != 1 || st.Failures[s.TableID] != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestReconciler_ReadyNeverCountsAnEarlierIncarnation(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	view.set(chainInc(t, 1, s, wire.TableStatusPurged), chainInc(t, 2, s, wire.TableStatusPending))
	if r.Ready(s.TableID) {
		t.Fatal("incarnation 1's tables must not make incarnation 2 ready")
	}
	mustReconcile(t, r)
	if !r.Ready(s.TableID) {
		t.Fatal("incarnation 2 not ready after its pass")
	}
}

func TestReconciler_RegistryDisabledReconcilesTheGenesisSetOnly(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	g, stray := schemaFor(t, "g"), schemaFor(t, "stray")
	if err := ddl.EnsureTable(context.Background(), conn, p, stray, 4, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.Genesis = []payloadexec.TableSchema{g} })
	if err := r.Reconcile(context.Background(), ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	if !r.Ready(g.TableID) || len(localComments(t, conn, p, stray.TableID)) != 3 {
		t.Fatal("disabled registry: genesis created, nothing dropped")
	}
	for db, c := range localComments(t, conn, p, g.TableID) {
		if c != "" {
			t.Fatalf("genesis %s carries comment %q", db, c)
		}
	}
	// Enabled: the genesis table is a genesis-origin Active incarnation and
	// keeps its unmarked tables and its configured mode.
	view.set(wire.TableIncarnation{Seq: 1, DatabaseID: "db", TableID: g.TableID[3:], Origin: wire.TableOriginGenesis,
		Status: wire.TableStatusActive, SchemaHash: payloadexec.TableSchemaHash(testNetwork, g)})
	mustReconcile(t, r)
	if !r.Ready(g.TableID) {
		t.Fatal("genesis table not ready under the enabled registry")
	}
}

// D2 naming is not injective across every key the registry ever saw: a Legacy
// key "x__y.z" and a live key "x.y__z" share the physical name x__y__z. The
// live key's tables must never be dropped as the Legacy key's leftovers.
func TestReconciler_NeverDropsATableALiveKeyOwns(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := suffix(t)
	live := payloadexec.TableSchema{TableID: "x.y__z_" + s, Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	legacy := wire.TableIncarnation{Seq: 1, DatabaseID: "x__y", TableID: "z_" + s, Origin: wire.TableOriginLegacy, Status: wire.TableStatusLegacy}
	if ddl.CHTableName(legacy.Key()) != ddl.CHTableName(live.TableID) {
		t.Fatal("fixture: the two keys must share a physical name")
	}
	view.set(legacy, chainInc(t, 2, live, wire.TableStatusActive))
	mustReconcile(t, r)
	mustReconcile(t, r)
	if got := localComments(t, conn, p, live.TableID); len(got) != 3 || !r.Ready(live.TableID) {
		t.Fatalf("tables %v ready %v: the live key's tables must survive", got, r.Ready(live.TableID))
	}
	if st := r.Stats(); st.LeftoverDrops != 0 {
		t.Fatalf("stats = %+v", st)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./dataplane/tableset/ -count=1`
Expected: build failure `undefined: Config`, `undefined: Deps`, `undefined: Reconciler` (the package has no non-test file yet).

- [ ] **Step 3: Implement the reconciler**

Create `dataplane/tableset/reconciler.go`:

```go
// Package tableset is the level-triggered table-set reconciler of the
// dynamic storage-integrity table set (sub-project 4 §7). Each data-plane role
// runs one: it derives the desired hg_unsafe / hg_safe / hg_promote tables
// from the arbiter's table registry (or, while the registry is disabled, from
// the configured genesis set), compares them with ClickHouse and closes the
// difference: it creates and verifies the tables of Pending, Active and
// Retiring incarnations, drops those of Purging incarnations and reports
// SubmitTablePurged, drops leftovers of Purged, Refused and Legacy keys, and
// only reports protocol tables the registry does not know.
package tableset

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// State is one table's reconciler state, the label of the "tables per state"
// metric.
type State string

const (
	// StateCreating: the tables should exist but are not verified yet.
	StateCreating State = "creating"
	// StateReady: all three tables of the current incarnation exist and verify.
	StateReady State = "ready"
	// StateWaitingQuiescence: Purging, but the role's quiescence hook still
	// reports promotion or cleanup work for the table.
	StateWaitingQuiescence State = "waiting_quiescence"
	// StatePurging: dropping the tables or reporting the purge.
	StatePurging State = "purging"
	// StatePurgeReported: this node's purge is recorded by the arbiter.
	StatePurgeReported State = "purge_reported"
)

// Arbiter is the slice of *dataplane.Client the reconciler calls.
type Arbiter interface {
	SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error
	PurgeNodeSet(ctx context.Context) ([]string, error)
}

// Config fixes one reconciler.
type Config struct {
	// Pinned names the protocol databases, the keeper shard and this node's
	// replica name (the SNode node id or the verifier replica id).
	Pinned ddl.Pinned
	// Genesis is the configured genesis table set. While the registry is
	// disabled it is the whole desired set; afterwards it supplies the
	// schemas of genesis-origin incarnations, which carry no schema_json.
	Genesis []payloadexec.TableSchema
	// Interval is the steady-state pass cadence (0 = ddl.DefaultReconcileInterval);
	// it also caps the per-table failure backoff.
	Interval time.Duration
	// SweepDecommissioned makes this node remove, after its own drop,
	// every Keeper replica of a purged table that is not in the arbiter's
	// purge node set. Only the source SNode sets it.
	SweepDecommissioned bool
}

// Deps are the reconciler's collaborators.
type Deps struct {
	Conn clickhouse.Conn
	// Registry is the registry follower. Nil means the registry is never
	// followed: only the genesis set is reconciled.
	Registry dataplane.RegistryView
	// Arbiter receives purge reports. Required when Registry is set.
	Arbiter Arbiter
	// Quiescent, when set, gates a purge: it reports whether the role has no
	// promotion or unsafe cleanup work left that references tableID.
	Quiescent func(tableID string) (bool, error)
	Logger    *slog.Logger
}

// Stats is a point-in-time copy of the reconciler's metrics.
type Stats struct {
	States        map[State]int
	Failures      map[string]uint64
	Unknown       []string
	LeftoverDrops uint64
}

type tableStatus struct {
	state       State
	seq         uint64
	failures    int
	nextAttempt time.Time
}

// Reconciler reconciles one node's protocol tables. Reconcile passes are
// serialized; Ready, Stats, Trigger and Wake are safe for concurrent use.
type Reconciler struct {
	cfg     Config
	d       Deps
	genesis map[string]payloadexec.TableSchema
	now     func() time.Time

	passMu sync.Mutex

	mu            sync.Mutex
	tables        map[string]*tableStatus
	failures      map[string]uint64
	unknown       []string
	leftoverDrops uint64
	trigger       chan struct{}
	passDone      chan struct{}
}

// New validates cfg and returns a reconciler.
func New(cfg Config, d Deps) (*Reconciler, error) {
	if d.Conn == nil {
		return nil, errors.New("tableset: clickhouse connection is required")
	}
	if cfg.Pinned.UnsafeDB == "" || cfg.Pinned.SafeDB == "" || cfg.Pinned.PromoteDB == "" || cfg.Pinned.NodeID == "" {
		return nil, errors.New("tableset: Pinned needs UnsafeDB, SafeDB, PromoteDB and NodeID")
	}
	if d.Registry != nil && d.Arbiter == nil {
		return nil, errors.New("tableset: an arbiter client is required to follow the registry")
	}
	if err := ddl.ValidatePhysicalTableNames(cfg.Genesis); err != nil {
		return nil, err
	}
	if cfg.Interval <= 0 {
		cfg.Interval = ddl.DefaultReconcileInterval
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	genesis := make(map[string]payloadexec.TableSchema, len(cfg.Genesis))
	for _, t := range cfg.Genesis {
		genesis[t.TableID] = t
	}
	return &Reconciler{
		cfg: cfg, d: d, genesis: genesis, now: time.Now,
		tables: map[string]*tableStatus{}, failures: map[string]uint64{},
		trigger: make(chan struct{}, 1), passDone: make(chan struct{}),
	}, nil
}

// Ready reports whether all three tables of tableID's current incarnation
// exist and verify. With the registry enabled the verified incarnation must
// be the key's live one, so tables of an earlier incarnation never count.
func (r *Reconciler) Ready(tableID string) bool {
	if r == nil {
		return false
	}
	var snap wire.TableRegistrySnapshot
	enabled := false
	if r.d.Registry != nil {
		snap, enabled = r.d.Registry.View()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readyLocked(tableID, snap, enabled)
}

func (r *Reconciler) readyLocked(tableID string, snap wire.TableRegistrySnapshot, enabled bool) bool {
	ts := r.tables[tableID]
	if ts == nil || ts.state != StateReady {
		return false
	}
	if !enabled {
		return true
	}
	live := snap.Live(tableID)
	return live != nil && live.Seq == ts.seq && live.HasPhysicalTables() && live.Status != wire.TableStatusPurging
}

// Trigger requests an early pass and clears every per-table backoff.
func (r *Reconciler) Trigger() {
	if r == nil {
		return
	}
	r.mu.Lock()
	for _, ts := range r.tables {
		ts.nextAttempt = time.Time{}
	}
	r.mu.Unlock()
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

// Wake returns the channels that start an early pass: the registry's next
// accepted version and Trigger. A nil reconciler returns nil channels.
func (r *Reconciler) Wake() (registry <-chan struct{}, trigger <-chan struct{}) {
	if r == nil {
		return nil, nil
	}
	if r.d.Registry != nil {
		registry = r.d.Registry.Changed()
	}
	return registry, r.trigger
}

// NextDelay is how long the caller's loop may sleep before the next pass:
// the interval, shortened to the earliest per-table retry.
func (r *Reconciler) NextDelay() time.Duration {
	if r == nil {
		return ddl.DefaultReconcileInterval
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delay := r.cfg.Interval
	now := r.now()
	for _, ts := range r.tables {
		if ts.nextAttempt.IsZero() {
			continue
		}
		if d := ts.nextAttempt.Sub(now); d < delay {
			delay = max(d, 0)
		}
	}
	return delay
}

// WaitReady triggers a pass and waits up to timeout for every table in
// tableIDs to be Ready. It reports whether they all are.
func (r *Reconciler) WaitReady(ctx context.Context, tableIDs []string, timeout time.Duration) bool {
	if r == nil {
		return false
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	r.Trigger()
	for {
		var snap wire.TableRegistrySnapshot
		enabled := false
		if r.d.Registry != nil {
			snap, enabled = r.d.Registry.View()
		}
		r.mu.Lock()
		done := r.passDone
		ready := true
		for _, id := range tableIDs {
			if !r.readyLocked(id, snap, enabled) {
				ready = false
				break
			}
		}
		r.mu.Unlock()
		if ready {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-done:
		}
	}
}

// Stats returns a copy of the reconciler's metrics.
func (r *Reconciler) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := Stats{States: map[State]int{}, Failures: map[string]uint64{}, Unknown: slices.Clone(r.unknown), LeftoverDrops: r.leftoverDrops}
	for _, ts := range r.tables {
		out.States[ts.state]++
	}
	for id, n := range r.failures {
		out.Failures[id] = n
	}
	return out
}

// Reconcile runs one pass. genesisMode governs genesis-origin tables (the
// role's schema-source mode at startup, ddl.ModeVerifyOnly afterwards);
// chain-origin tables are always created and verified. It returns fatal
// errors (drift, a verify-only table missing, an unattributable table) and
// any failure of a genesis table, which the role's existing retry budget
// handles; every other per-table failure is recorded, backed off and retried
// by a later pass.
func (r *Reconciler) Reconcile(ctx context.Context, genesisMode ddl.Mode) error {
	r.passMu.Lock()
	defer r.passMu.Unlock()
	defer r.finishPass()
	var snap wire.TableRegistrySnapshot
	enabled := false
	if r.d.Registry != nil {
		snap, enabled = r.d.Registry.View()
	}
	if !enabled {
		return r.reconcileGenesis(ctx, genesisMode)
	}
	return r.reconcileRegistry(ctx, snap, genesisMode)
}

func (r *Reconciler) finishPass() {
	r.mu.Lock()
	defer r.mu.Unlock()
	close(r.passDone)
	r.passDone = make(chan struct{})
}

// reconcileGenesis is the registry-disabled pass: exactly the configured
// genesis set, in its configured mode; nothing is ever dropped.
func (r *Reconciler) reconcileGenesis(ctx context.Context, mode ddl.Mode) error {
	if mode == ddl.ModeOff {
		return nil
	}
	var errs []error
	desired := map[string]bool{}
	for _, schema := range r.cfg.Genesis {
		desired[ddl.CHTableName(schema.TableID)] = true
		if err := ddl.EnsureTable(ctx, r.d.Conn, r.cfg.Pinned, schema, 0, mode); err != nil {
			r.fail(schema.TableID, 0, StateCreating, err)
			errs = append(errs, err)
			continue
		}
		r.setState(schema.TableID, 0, StateReady)
	}
	if err := r.reportUnknown(ctx, desired, nil); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

type target struct {
	inc    wire.TableIncarnation
	schema payloadexec.TableSchema
	mode   ddl.Mode
	marker uint64
}

func (r *Reconciler) reconcileRegistry(ctx context.Context, snap wire.TableRegistrySnapshot, genesisMode ddl.Mode) error {
	present := map[string]target{}                // physical name -> table to create and verify
	purging := map[string]wire.TableIncarnation{} // physical name -> incarnation to purge
	history := map[string]string{}                // physical name -> key of a Purged/Refused/Legacy key
	seen := map[string]bool{}
	var fatal, genesisErrs []error
	for _, inc := range snap.Incarnations {
		key := inc.Key()
		if seen[key] {
			continue
		}
		seen[key] = true
		live := snap.Live(key)
		physical := ddl.CHTableName(key)
		switch {
		case live.Status == wire.TableStatusPurging:
			purging[physical] = *live
		case live.HasPhysicalTables():
			t, err := r.targetFor(*live, genesisMode)
			if err != nil {
				r.fail(key, live.Seq, StateCreating, err)
				if live.Origin == wire.TableOriginGenesis {
					genesisErrs = append(genesisErrs, err)
				}
				continue
			}
			present[physical] = t
		default:
			if _, taken := history[physical]; !taken {
				history[physical] = key
			}
		}
	}
	r.forgetAbsent(seen)

	local, err := ddl.ListProtocolTables(ctx, r.d.Conn, r.cfg.Pinned)
	if err != nil {
		return err
	}
	comments := map[string][]string{}
	for _, lt := range local {
		comments[lt.Table] = append(comments[lt.Table], lt.Comment)
	}

	for _, physical := range sortedKeys(present) {
		t := present[physical]
		if err := r.ensurePresent(ctx, snap, t, comments[physical]); err != nil {
			switch {
			case ddl.FatalReconcileError(err):
				fatal = append(fatal, err)
			case t.inc.Origin == wire.TableOriginGenesis:
				genesisErrs = append(genesisErrs, err)
			}
		}
	}
	for _, physical := range sortedKeys(purging) {
		r.purge(ctx, purging[physical], len(comments[physical]) > 0)
	}
	desired := map[string]bool{}
	for physical := range present {
		desired[physical] = true
	}
	for physical := range purging {
		desired[physical] = true
	}
	for _, physical := range sortedKeys(comments) {
		if desired[physical] {
			continue
		}
		if key, ok := history[physical]; ok {
			r.dropLeftover(ctx, key)
		}
	}
	if r.cfg.SweepDecommissioned {
		r.sweepPurgedKeeperPaths(ctx, history, desired)
	}
	if err := r.reportUnknown(ctx, desired, history); err != nil {
		fatal = append(fatal, err)
	}
	return errors.Join(append(fatal, genesisErrs...)...)
}

func (r *Reconciler) targetFor(inc wire.TableIncarnation, genesisMode ddl.Mode) (target, error) {
	if inc.Origin == wire.TableOriginGenesis {
		schema, ok := r.genesis[inc.Key()]
		if !ok {
			return target{}, fmt.Errorf("tableset: genesis table %s is not in the configured genesis set", inc.Key())
		}
		return target{inc: inc, schema: schema, mode: genesisMode}, nil
	}
	schema, err := inc.Schema()
	if err != nil {
		return target{}, err
	}
	return target{inc: inc, schema: schema, mode: ddl.ModeCreateAndVerify, marker: inc.Seq}, nil
}

// ensurePresent creates and verifies one table. The expected table comment
// is empty for a genesis table and IncarnationComment(seq) for a chain one. A
// local table with another comment is a leftover of an earlier incarnation
// of the key when the registry records that incarnation (the marked one, or
// for an unmarked table the key's genesis incarnation) as Purged or Refused;
// it is dropped before the create. Any other comment is drift.
func (r *Reconciler) ensurePresent(ctx context.Context, snap wire.TableRegistrySnapshot, t target, comments []string) error {
	key := t.inc.Key()
	// A genesis table keeps the role's own retry budget (its failures are
	// returned), so only chain tables are skipped while backing off.
	if t.inc.Origin != wire.TableOriginGenesis && !r.due(key) {
		return nil
	}
	expected := ""
	if t.marker != 0 {
		expected = ddl.IncarnationComment(t.marker)
	}
	leftover := false
	for _, comment := range comments {
		if comment == expected {
			continue
		}
		if t.marker == 0 || !earlierRetired(snap, key, ownerOf(snap, key, comment), t.inc.Seq) {
			err := fmt.Errorf("%w: %s tables carry comment %q, want %q (incarnation %d)", ddl.ErrProtocolTableDrift, key, comment, expected, t.inc.Seq)
			r.fail(key, t.inc.Seq, StateCreating, err)
			return err
		}
		leftover = true
	}
	if leftover {
		if err := ddl.DropTable(ctx, r.d.Conn, r.cfg.Pinned, key); err != nil {
			r.fail(key, t.inc.Seq, StateCreating, err)
			return err
		}
		r.countLeftover(key, "earlier incarnation")
	}
	if err := ddl.EnsureTable(ctx, r.d.Conn, r.cfg.Pinned, t.schema, t.marker, t.mode); err != nil {
		r.fail(key, t.inc.Seq, StateCreating, err)
		return err
	}
	r.setState(key, t.inc.Seq, StateReady)
	return nil
}

// ownerOf maps a table comment to the incarnation seq it marks: the marked
// seq, or for an unmarked table the key's genesis incarnation; 0 when the
// comment names no incarnation of key.
func ownerOf(snap wire.TableRegistrySnapshot, key, comment string) uint64 {
	if comment == "" {
		for _, inc := range snap.Incarnations {
			if inc.Key() == key && inc.Origin == wire.TableOriginGenesis {
				return inc.Seq
			}
		}
		return 0
	}
	if seq, ok := ddl.ParseIncarnationComment(comment); ok {
		return seq
	}
	return 0
}

// earlierRetired reports whether owner is an earlier incarnation of key that
// the registry records as Purged or Refused.
func earlierRetired(snap wire.TableRegistrySnapshot, key string, owner, current uint64) bool {
	if owner == 0 || owner >= current || owner > uint64(len(snap.Incarnations)) {
		return false
	}
	inc := snap.Incarnations[owner-1]
	return inc.Key() == key && (inc.Status == wire.TableStatusPurged || inc.Status == wire.TableStatusRefused)
}

// purge drops one Purging incarnation's tables and reports it. hasLocal is
// false when no local table of that name exists (the drop is then a Keeper
// check only).
func (r *Reconciler) purge(ctx context.Context, inc wire.TableIncarnation, hasLocal bool) {
	key := inc.Key()
	if slices.Contains(inc.PurgedBy, r.cfg.Pinned.NodeID) {
		r.setState(key, inc.Seq, StatePurgeReported)
		return
	}
	if !r.due(key) {
		return
	}
	if r.d.Quiescent != nil {
		quiet, err := r.d.Quiescent(key)
		if err != nil {
			r.fail(key, inc.Seq, StateWaitingQuiescence, err)
			return
		}
		if !quiet {
			r.setState(key, inc.Seq, StateWaitingQuiescence)
			return
		}
	}
	r.setState(key, inc.Seq, StatePurging)
	if err := ddl.DropTable(ctx, r.d.Conn, r.cfg.Pinned, key); err != nil {
		r.fail(key, inc.Seq, StatePurging, err)
		return
	}
	if hasLocal {
		r.d.Logger.Info("dropped purging table", "table", key, "incarnation", inc.Seq)
	}
	if r.cfg.SweepDecommissioned {
		if err := r.sweep(ctx, key); err != nil {
			r.fail(key, inc.Seq, StatePurging, err)
			return
		}
	}
	if err := r.d.Arbiter.SubmitTablePurged(ctx, r.cfg.Pinned.NodeID, inc.Seq); err != nil {
		if status.Code(err) == codes.Unimplemented {
			err = fmt.Errorf("arbiter does not implement SubmitTablePurged yet: %w", err)
		}
		r.fail(key, inc.Seq, StatePurging, err)
		return
	}
	r.setState(key, inc.Seq, StatePurgeReported)
	r.d.Logger.Info("reported table purged", "table", key, "incarnation", inc.Seq, "node", r.cfg.Pinned.NodeID)
}

// sweep removes every replica under key's Keeper path that is neither this
// node nor in the arbiter's purge node set.
func (r *Reconciler) sweep(ctx context.Context, key string) error {
	replicas, err := ddl.KeeperReplicas(ctx, r.d.Conn, r.cfg.Pinned, key)
	if err != nil || len(replicas) == 0 {
		return err
	}
	nodes, err := r.d.Arbiter.PurgeNodeSet(ctx)
	if err != nil {
		return fmt.Errorf("tableset: purge node set: %w", err)
	}
	for _, replica := range replicas {
		if replica == r.cfg.Pinned.NodeID || slices.Contains(nodes, replica) {
			continue
		}
		if err := ddl.DropReplica(ctx, r.d.Conn, r.cfg.Pinned, key, replica); err != nil {
			return err
		}
		r.d.Logger.Warn("dropped decommissioned replica", "table", key, "replica", replica)
	}
	return nil
}

// sweepPurgedKeeperPaths catches a Keeper path that survived its purge
// because a node was evicted after this node's sweep: for every key whose
// live incarnation has no tables but whose path still exists, drop this
// node's own stranded replica and sweep the rest.
func (r *Reconciler) sweepPurgedKeeperPaths(ctx context.Context, history map[string]string, desired map[string]bool) {
	paths, err := ddl.KeeperUnsafeTables(ctx, r.d.Conn, r.cfg.Pinned)
	if err != nil {
		r.d.Logger.Warn("list keeper table paths", "err", err)
		return
	}
	for _, physical := range paths {
		key, ok := history[physical]
		if !ok || desired[physical] {
			continue
		}
		if err := ddl.DropTable(ctx, r.d.Conn, r.cfg.Pinned, key); err != nil {
			r.d.Logger.Warn("drop stranded replica", "table", key, "err", err)
			continue
		}
		if err := r.sweep(ctx, key); err != nil {
			r.d.Logger.Warn("sweep surviving keeper path", "table", key, "err", err)
		}
	}
}

func (r *Reconciler) dropLeftover(ctx context.Context, key string) {
	if err := ddl.DropTable(ctx, r.d.Conn, r.cfg.Pinned, key); err != nil {
		r.d.Logger.Warn("drop leftover protocol tables", "table", key, "err", err)
		r.mu.Lock()
		r.failures[key]++
		r.mu.Unlock()
		return
	}
	r.countLeftover(key, "retired key")
}

func (r *Reconciler) countLeftover(key, why string) {
	r.mu.Lock()
	r.leftoverDrops++
	r.mu.Unlock()
	r.d.Logger.Warn("dropped leftover protocol tables", "table", key, "reason", why)
}

// reportUnknown logs and records local protocol tables that neither the
// desired set nor any registry key accounts for. They are never dropped.
func (r *Reconciler) reportUnknown(ctx context.Context, desired map[string]bool, history map[string]string) error {
	local, err := ddl.ListProtocolTables(ctx, r.d.Conn, r.cfg.Pinned)
	if err != nil {
		return err
	}
	var unknown []string
	for _, lt := range local {
		if desired[lt.Table] {
			continue
		}
		if _, ok := history[lt.Table]; ok {
			continue
		}
		name := lt.Database + "." + lt.Table
		if !slices.Contains(unknown, name) {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	r.mu.Lock()
	previous := r.unknown
	r.unknown = unknown
	r.mu.Unlock()
	if !slices.Equal(previous, unknown) && len(unknown) > 0 {
		r.d.Logger.Warn("unknown protocol tables (reported, never dropped)", "tables", unknown)
	}
	return nil
}

func (r *Reconciler) due(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ts := r.tables[key]
	return ts == nil || ts.nextAttempt.IsZero() || !r.now().Before(ts.nextAttempt)
}

func (r *Reconciler) setState(key string, seq uint64, state State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ts := r.tables[key]
	if ts == nil || ts.seq != seq {
		ts = &tableStatus{seq: seq}
		r.tables[key] = ts
	}
	ts.state = state
	if state == StateReady || state == StatePurgeReported {
		ts.failures, ts.nextAttempt = 0, time.Time{}
	}
}

func (r *Reconciler) fail(key string, seq uint64, state State, err error) {
	r.mu.Lock()
	ts := r.tables[key]
	if ts == nil || ts.seq != seq {
		ts = &tableStatus{seq: seq}
		r.tables[key] = ts
	}
	ts.state = state
	ts.failures++
	ts.nextAttempt = r.now().Add(ddl.ReconcileBackoff(ts.failures, r.cfg.Interval))
	r.failures[key]++
	failures := ts.failures
	r.mu.Unlock()
	r.d.Logger.Warn("table reconcile failed", "table", key, "incarnation", seq, "state", state, "consecutive_failures", failures, "err", err)
}

// forgetAbsent drops the status of keys no longer in the registry snapshot.
func (r *Reconciler) forgetAbsent(seen map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.tables {
		if !seen[key] {
			delete(r.tables, key)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
```

Run: `bazel run //:gazelle`
Expected: a new `dataplane/tableset/BUILD.bazel` with `go_library(name = "tableset")` over `reconciler.go` and `go_test(name = "tableset_test")` over `reconciler_ch_test.go`; nothing else changes.

- [ ] **Step 4: Add the target to the docker CI job and the package to the README**

In `.github/workflows/ci.yml` replace:

```yaml
            //dataplane/ddl:ddl_test \
            //snode:snode_test \
```

with:

```yaml
            //dataplane/ddl:ddl_test \
            //dataplane/tableset:tableset_test \
            //snode:snode_test \
```

In `README.md` replace:

```markdown
| `dataplane` | Leader-aware Arbiter clients, subscriptions, manifests, and payload stores. |
```

with:

```markdown
| `dataplane` | Leader-aware Arbiter clients, subscriptions, manifests, payload stores, the table-registry follower, and purge reports. |
| `dataplane/tableset` | The level-triggered table-set reconciler: creates, verifies, and purges the `hg_*` tables the table registry asks for. |
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./dataplane/tableset/ -count=1 -v 2>&1 | grep -E '^(---|ok)' && go test -race ./dataplane/tableset/ -count=1 && bazel test //dataplane/tableset:tableset_test`
Expected: fourteen `--- PASS` lines, no `SKIP`, `ok` (measured: 3.4 s); `-race` `ok`; `PASSED` (skipping) under Bazel. The Keeper listing from Task 4 Step 6 is still empty afterwards.

- [ ] **Step 6: Commit**

```bash
git add dataplane/tableset/ .github/workflows/ci.yml README.md
git commit -m "feat(tableset): level-triggered reconciler for the dynamic SI table set" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 6: arbiter-core — SNode follows the registry

**Files:**
- Create: `snode/registry.go`
- Modify: `snode/snode.go:16-19` (imports), `:27-32` (`Deps`), `:82-85` (`Role`), `:112-113` (`New`), `:207-218` (`ensureProtocolTablesMode`), `:461-472` (`reconcileProtocolTables`), `:502-509` (old `schemaFor`, removed)
- Modify: `snode/config.go:22-46` (`Config`)
- Modify: `snode/staged.go:96-105` (`PrepareLocalStatement`)
- Modify: `snode/view.go:1-35` (`sourceClaimRoot`)
- Modify: `snode/BUILD.bazel` (gazelle)
- Test: create `snode/registry_test.go`

**Interfaces:**
- Consumes: `dataplane.RegistryView`, `dataplane.WaitReady` (Task 3); `tableset.New`, `(*tableset.Reconciler).Reconcile/Ready/Wake/NextDelay/Stats`, `tableset.Stats` (Task 5); `wire.TableRegistrySnapshot` and statuses (Task 2); `payloadexec.SchemaRootFromHashes`, `replay.AssembleStateRoot` (housegate `db46c31`, pinned).
- Produces (package `snode`): field `Deps.Registry dataplane.RegistryView`; field `Config.RegistryStartupTimeout time.Duration`; `var ErrTableNotReady`; `func (r *Role) TableReady(tableID string) bool`; `func (r *Role) TableSetStats() tableset.Stats`; unexported `schemaFor` (registry-aware), `requireAdmissible`, `tableQuiescent`, `stateStore.quiescent`, `stateRootTables`.

- [ ] **Step 1: Write the failing tests**

Create `snode/registry_test.go` (it reuses `testConfigS`, `intakeSchema`, `intakeEnvelope`, `nativePayload`, `pv`, `testRevision`, `testRecord`, `setUniqueDatabases`, `requireCH`, `requireKeeperS`, `sourceClaimsFake`, `startSourceClaimsFake` from the existing tests):

```go
package snode

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/fspayload"
	"github.com/sentioxyz/arbiter-core/wire"
)

// fakeRegistryS is a settable dataplane.RegistryView.
type fakeRegistryS struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	changed chan struct{}
	ready   chan struct{}
}

func newFakeRegistryS() *fakeRegistryS {
	v := &fakeRegistryS{changed: make(chan struct{}), ready: make(chan struct{})}
	close(v.ready)
	return v
}

func (v *fakeRegistryS) View() (wire.TableRegistrySnapshot, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snap, v.enabled
}

func (v *fakeRegistryS) Changed() <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.changed
}

func (v *fakeRegistryS) Ready() <-chan struct{} { return v.ready }

func (v *fakeRegistryS) set(incs ...wire.TableIncarnation) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snap = wire.TableRegistrySnapshot{Version: v.snap.Version + 1, Seeded: true, Incarnations: incs}
	v.enabled = true
	close(v.changed)
	v.changed = make(chan struct{})
}

func chainIncarnationS(t *testing.T, seq uint64, schema payloadexec.TableSchema, status wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: status,
		SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash("testnet", schema), SchemaJSON: string(js)}
}

func genesisIncarnationS(seq uint64, schema payloadexec.TableSchema) wire.TableIncarnation {
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginGenesis,
		Status: wire.TableStatusActive, SchemaHash: payloadexec.TableSchemaHash("testnet", schema)}
}

func chainSchemaS(tableID string) payloadexec.TableSchema {
	s := intakeSchema()
	s.TableID = tableID
	return s
}

func TestSchemaFor_FollowsTheLiveIncarnation(t *testing.T) {
	cfg := testConfigS(t)
	view := newFakeRegistryS()
	r := &Role{cfg: cfg, d: Deps{Registry: view}}
	if got, err := r.schemaFor("db.t"); err != nil || got.TableID != "db.t" {
		t.Fatalf("disabled registry: %+v, %v", got, err)
	}
	c := chainSchemaS("db.c")
	for _, tc := range []struct {
		status wire.TableIncarnationStatus
		ok     bool
	}{
		{wire.TableStatusPending, false}, {wire.TableStatusActive, true}, {wire.TableStatusRetiring, true},
		{wire.TableStatusPurging, true}, {wire.TableStatusPurged, false}, {wire.TableStatusRefused, false},
	} {
		view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, tc.status))
		got, err := r.schemaFor("db.c")
		if (err == nil) != tc.ok {
			t.Fatalf("%s: err = %v, want ok=%v", tc.status, err, tc.ok)
		}
		if tc.ok && (got.PartitionBy != "p" || len(got.Columns) != 2) {
			t.Fatalf("%s: schema = %+v", tc.status, got)
		}
	}
	if got, err := r.schemaFor("db.t"); err != nil || len(got.Columns) != 1 {
		t.Fatalf("genesis-origin schema comes from config: %+v, %v", got, err)
	}
	if _, err := r.schemaFor("db.none"); err == nil {
		t.Fatal("a key outside the registry must be unknown")
	}
	bad := chainIncarnationS(t, 2, c, wire.TableStatusActive)
	bad.SchemaHash = "0xother"
	view.set(genesisIncarnationS(1, cfg.Tables[0]), bad)
	if _, err := r.schemaFor("db.c"); err == nil || !strings.Contains(err.Error(), "hashes to") {
		t.Fatalf("a schema_json that does not hash to the registry hash must be refused: %v", err)
	}
}

func TestRequireAdmissible_GatesFreshIntake(t *testing.T) {
	cfg := testConfigS(t)
	view := newFakeRegistryS()
	r := &Role{cfg: cfg, d: Deps{Registry: view}}
	c := chainSchemaS("db.c")
	hash := payloadexec.TableSchemaHash("testnet", c)
	if err := r.requireAdmissible("db.c", hash); err != nil {
		t.Fatalf("disabled registry admits as before: %v", err)
	}
	for _, tc := range []struct {
		status wire.TableIncarnationStatus
		want   error
	}{
		{wire.TableStatusPending, ErrTableNotReady},
		{wire.TableStatusActive, ErrTableNotReady}, // not Ready: no reconciler pass
		{wire.TableStatusRetiring, ErrSchemaUnknown},
		{wire.TableStatusPurged, ErrSchemaUnknown},
	} {
		view.set(chainIncarnationS(t, 1, c, tc.status))
		if err := r.requireAdmissible("db.c", hash); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.status, err, tc.want)
		}
	}
	view.set(chainIncarnationS(t, 1, c, wire.TableStatusActive))
	if err := r.requireAdmissible("db.c", "0xstale"); !errors.Is(err, ErrSchemaHashMismatch) {
		t.Fatalf("err = %v, want ErrSchemaHashMismatch", err)
	}
	if err := r.requireAdmissible("db.none", hash); !errors.Is(err, ErrSchemaUnknown) {
		t.Fatalf("err = %v, want ErrSchemaUnknown", err)
	}
}

func TestSourceClaimRoot_CoversActiveAndRetiringRegistryTables(t *testing.T) {
	cfg := testConfigS(t)
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	view := newFakeRegistryS()
	r := &Role{cfg: cfg, state: st, d: Deps{Registry: view}}
	static, err := r.sourceClaimRoot()
	if err != nil {
		t.Fatal(err)
	}
	view.set(genesisIncarnationS(1, cfg.Tables[0]))
	if got, err := r.sourceClaimRoot(); err != nil || got != static {
		t.Fatalf("a registry holding only the genesis table must give the static root: %s vs %s (%v)", got, static, err)
	}
	a, b, p := chainSchemaS("db.a"), chainSchemaS("db.b"), chainSchemaS("db.p")
	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, a, wire.TableStatusActive),
		chainIncarnationS(t, 3, b, wire.TableStatusRetiring), chainIncarnationS(t, 4, p, wire.TableStatusPending))
	hashes := map[string]string{
		"db.t": payloadexec.TableSchemaHash("testnet", cfg.Tables[0]),
		"db.a": payloadexec.TableSchemaHash("testnet", a),
		"db.b": payloadexec.TableSchemaHash("testnet", b),
	}
	var tables []replay.TableManifest
	for _, id := range []string{"db.a", "db.b", "db.t"} {
		tables = append(tables, replay.TableManifest{TableID: id, SchemaHash: hashes[id]})
	}
	_, want, err := replay.AssembleStateRoot(cfg.SchemaSnapshotID, payloadexec.SchemaRootFromHashes(hashes), cfg.ExecutorProfileID, tables)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := r.sourceClaimRoot(); err != nil || got != want {
		t.Fatalf("root = %s, want %s over Active and Retiring only (%v)", got, want, err)
	}
}

func TestTableQuiescent_WaitsForPromotionCleanupAndIntake(t *testing.T) {
	cfg := testConfigS(t)
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := &Role{cfg: cfg, state: st, journal: journal}
	quiet := func() bool {
		t.Helper()
		ok, err := r.tableQuiescent("db.t")
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !quiet() {
		t.Fatal("an untouched table is quiescent")
	}
	k := partitionKey{Table: "db.t", Partition: "p_x"}
	one := "0x" + strings.Repeat("01", len(lthash.New().Bytes()))
	if err := st.AddUnpromoted(k, one); err != nil {
		t.Fatal(err)
	}
	if quiet() {
		t.Fatal("unpromoted rows keep the table busy")
	}
	if err := st.DrainUnpromoted(k, []string{one}); err != nil {
		t.Fatal(err)
	}
	if !quiet() {
		t.Fatal("a drained partition is quiescent")
	}
	st.mu.Lock()
	st.s.PromotedUnsafeParts[key("db.t", "p_x")] = []string{"p_x_1_1_0"}
	st.mu.Unlock()
	if quiet() {
		t.Fatal("a promoted part awaiting cleanup keeps the table busy")
	}
	if err := st.RecordCleanup(k, []string{"p_x_1_1_0"}); err != nil {
		t.Fatal(err)
	}
	other := testRecord("0xabc:6:n")
	other.Envelope.TargetTableID = "db.other"
	if err := journal.save(other); err != nil {
		t.Fatal(err)
	}
	if !quiet() {
		t.Fatal("an unfinished intake of another table does not block this one")
	}
	rec := testRecord("0xabc:7:n")
	rec.Envelope.TargetTableID = "db.t"
	if err := journal.save(rec); err != nil {
		t.Fatal(err)
	}
	if quiet() {
		t.Fatal("an unfinished intake keeps the table busy")
	}
}

func TestPrepareLocalStatement_RegistryGatesFreshIntakeOnChainTables(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	cfg := testConfigS(t)
	setUniqueDatabases(t, &cfg)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	suffix := strings.TrimPrefix(cfg.UnsafeDatabase, "hg_unsafe_")
	cfg.NodeID = "snode-" + suffix
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	claims := &sourceClaimsFake{}
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startSourceClaimsFake(t, claims)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	payloads, err := fspayload.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	view := newFakeRegistryS()
	c := chainSchemaS("db.c_" + suffix)
	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, wire.TableStatusPending))
	role, err := New(cfg, Deps{Client: client, Conn: conn, Payloads: payloads, Registry: view})
	if err != nil {
		t.Fatal(err)
	}

	payload := nativePayload(t, pv{"p0", 1})
	env := intakeEnvelope(payload)
	env.TargetTableID = c.TableID
	env.SQL = "INSERT INTO " + c.TableID + " FORMAT Native"
	env.SQLHash = replay.DigestString(env.SQL)
	env.SchemaHash = payloadexec.TableSchemaHash("testnet", c)
	req := PrepareRequest{Envelope: env, PayloadEncoding: stagedNativeEncoding, Revision: testRevision}
	if _, err := role.PrepareLocalStatement(ctx, req, payload); !errors.Is(err, ErrTableNotReady) {
		t.Fatalf("pending table: err = %v, want ErrTableNotReady", err)
	}
	if _, ok, _ := role.journal.load(env.StatementID.Flat()); ok {
		t.Fatal("a not-ready refusal must write no journal record")
	}

	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, wire.TableStatusActive))
	if err := role.ensureProtocolTables(ctx); err != nil {
		t.Fatalf("startup reconcile: %v", err)
	}
	if !role.TableReady(c.TableID) {
		t.Fatal("the Active chain table must be Ready after the reconcile")
	}
	first, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare on a ready chain table: %v", err)
	}

	// Retired: a fresh statement is refused, the recorded one still converges.
	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, wire.TableStatusRetiring))
	again, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil || again.StatementID != first.StatementID {
		t.Fatalf("recorded statement after retirement: %+v, %v", again, err)
	}
	fresh := req
	fresh.Envelope.StatementID.ClientSeq = 2
	if _, err := role.PrepareLocalStatement(ctx, fresh, payload); !errors.Is(err, ErrSchemaUnknown) {
		t.Fatalf("fresh statement on a retiring table: err = %v, want ErrSchemaUnknown", err)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./snode/ -count=1`
Expected: build failure `unknown field Registry in struct literal of type Deps`, `r.requireAdmissible undefined (type *Role has no field or method requireAdmissible)`, `undefined: ErrTableNotReady` (and more of the same kind).

- [ ] **Step 3: Add the registry-aware schema resolution, the intake gate and the purge gate**

Create `snode/registry.go`:

```go
package snode

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// ErrTableNotReady refuses a fresh intake whose target table is not yet
// admissible on this source: the registry has not made it Active, or the
// reconciler has not created and verified its local tables. It is raised
// before any journal record or ClickHouse write, and a retry can succeed.
var ErrTableNotReady = errors.New("snode: target table is not ready on this source")

// registryView returns the followed registry snapshot; enabled is false when
// the role follows no registry or the registry is disabled.
func (r *Role) registryView() (wire.TableRegistrySnapshot, bool) {
	if r.d.Registry == nil {
		return wire.TableRegistrySnapshot{}, false
	}
	return r.d.Registry.View()
}

func (r *Role) genesisSchema(tableID string) (payloadexec.TableSchema, bool) {
	for _, t := range r.cfg.Tables {
		if t.TableID == tableID {
			return t, true
		}
	}
	return payloadexec.TableSchema{}, false
}

// schemaFor resolves tableID for promotion, cleanup and recorded intake.
// Without an enabled registry it is the configured genesis table. With one it
// is the schema of the key's live storage-integrity incarnation when that is
// Active, Retiring or Purging (so a statement admitted before a retirement
// still converges and promotes): the configured schema for a genesis-origin
// incarnation, the decoded schema_json otherwise.
func (r *Role) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	snap, enabled := r.registryView()
	if !enabled {
		if t, ok := r.genesisSchema(tableID); ok {
			return t, nil
		}
		return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
	}
	live := snap.Live(tableID)
	if live == nil {
		return payloadexec.TableSchema{}, fmt.Errorf("table %s is not in the table registry", tableID)
	}
	switch live.Status {
	case wire.TableStatusActive, wire.TableStatusRetiring, wire.TableStatusPurging:
	default:
		return payloadexec.TableSchema{}, fmt.Errorf("table %s is %s in the table registry", tableID, live.Status)
	}
	return r.incarnationSchema(*live)
}

func (r *Role) incarnationSchema(inc wire.TableIncarnation) (payloadexec.TableSchema, error) {
	if inc.Origin == wire.TableOriginGenesis {
		if t, ok := r.genesisSchema(inc.Key()); ok {
			return t, nil
		}
		return payloadexec.TableSchema{}, fmt.Errorf("genesis table %s is not configured", inc.Key())
	}
	schema, err := inc.Schema()
	if err != nil {
		return payloadexec.TableSchema{}, err
	}
	if got := payloadexec.TableSchemaHash(r.cfg.NetworkID, schema); got != inc.SchemaHash {
		return payloadexec.TableSchema{}, fmt.Errorf("table %s schema_json hashes to %s, registry records %s", inc.Key(), got, inc.SchemaHash)
	}
	return schema, nil
}

// requireAdmissible is the fresh-intake gate (defence in depth: HouseGate
// refuses first). With an enabled registry the target must be Active, the
// envelope must sign the registry's schema hash, and the reconciler must
// report the table Ready.
func (r *Role) requireAdmissible(tableID, schemaHash string) error {
	snap, enabled := r.registryView()
	if !enabled {
		return nil
	}
	live := snap.Live(tableID)
	switch {
	case live == nil:
		return fmt.Errorf("table %s is not in the table registry: %w", tableID, ErrSchemaUnknown)
	case live.Status == wire.TableStatusPending:
		return fmt.Errorf("table %s is pending in the table registry: %w", tableID, ErrTableNotReady)
	case live.Status != wire.TableStatusActive:
		return fmt.Errorf("table %s is %s in the table registry: %w", tableID, live.Status, ErrSchemaUnknown)
	case live.SchemaHash != schemaHash:
		return fmt.Errorf("table %s schema_hash %q, registry has %q: %w", tableID, schemaHash, live.SchemaHash, ErrSchemaHashMismatch)
	case !r.tables.Ready(tableID):
		return fmt.Errorf("table %s protocol tables are not ready: %w", tableID, ErrTableNotReady)
	}
	return nil
}

// TableReady reports whether this SNode's hg_* tables for tableID's current
// incarnation exist and verify (tableset.Reconciler.Ready). An embedding host
// reports a table Active only when the registry says Active and this is true.
func (r *Role) TableReady(tableID string) bool { return r.tables.Ready(tableID) }

// TableSetStats returns the reconciler's metrics for the host to export.
func (r *Role) TableSetStats() tableset.Stats {
	if r.tables == nil {
		return tableset.Stats{}
	}
	return r.tables.Stats()
}

// tableQuiescent is the purge gate: no promotion intent, no promoted part
// awaiting cleanup, no unpromoted rows and no unfinished intake still
// reference tableID.
func (r *Role) tableQuiescent(tableID string) (bool, error) {
	quiet, err := r.state.quiescent(tableID)
	if err != nil || !quiet {
		return false, err
	}
	records, err := r.journal.list()
	if err != nil {
		return false, err
	}
	for _, rec := range records {
		if rec.Envelope.TargetTableID != tableID {
			continue
		}
		switch rec.Lifecycle {
		case LifecyclePreparing, LifecycleAbortPending, LifecycleUnsafeWritten:
			return false, nil
		}
	}
	return true, nil
}

func (st *stateStore) quiescent(table string) (bool, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	prefix := table + "\x00"
	zero := lthash.New().Bytes()
	for ks := range st.s.PromotionIntents {
		if strings.HasPrefix(ks, prefix) {
			return false, nil
		}
	}
	for ks, parts := range st.s.PromotedUnsafeParts {
		if strings.HasPrefix(ks, prefix) && len(parts) > 0 {
			return false, nil
		}
	}
	for ks, sum := range st.s.UnpromotedSums {
		if !strings.HasPrefix(ks, prefix) {
			continue
		}
		acc, err := parseAccumulatorHex(sum)
		if err != nil {
			return false, err
		}
		if !bytes.Equal(acc.Bytes(), zero) {
			return false, nil
		}
	}
	return true, nil
}
```

- [ ] **Step 4: Wire the reconciler into the role**

In `snode/snode.go` replace:

```go
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)
```

with:

```go
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
)
```

replace:

```go
type Deps struct {
	Client   *dataplane.Client
	Conn     clickhouse.Conn
	Payloads PayloadSpool
	Logger   *slog.Logger
}
```

with:

```go
type Deps struct {
	Client   *dataplane.Client
	Conn     clickhouse.Conn
	Payloads PayloadSpool
	Logger   *slog.Logger
	// Registry is the table-registry follower (usually shared with the host,
	// which runs it). Nil keeps the role on its configured tables exactly as
	// before the dynamic table set.
	Registry dataplane.RegistryView
}
```

replace:

```go
	// ensureFn is the protocol-table lifecycle seam. New wires production to
	// ensureProtocolTablesMode; tests inject deterministic reconcile outcomes.
	ensureFn func(context.Context, ddl.Mode) error
}
```

with:

```go
	// ensureFn is the protocol-table lifecycle seam. New wires production to
	// ensureProtocolTablesMode; tests inject deterministic reconcile outcomes.
	ensureFn func(context.Context, ddl.Mode) error
	// tables is the table-set reconciler; nil when the host owns DDL.
	tables *tableset.Reconciler
}
```

replace:

```go
	r.ensureFn = r.ensureProtocolTablesMode
	return r, nil
}
```

with:

```go
	r.ensureFn = r.ensureProtocolTablesMode
	if cfg.protocolTables != ddl.ModeOff && d.Conn != nil {
		tables, err := tableset.New(tableset.Config{
			Pinned: r.pinned(), Genesis: cfg.Tables, Interval: cfg.ProtocolTablesReconcile, SweepDecommissioned: true,
		}, tableset.Deps{Conn: d.Conn, Registry: d.Registry, Arbiter: d.Client, Quiescent: r.tableQuiescent, Logger: d.Logger})
		if err != nil {
			return nil, fmt.Errorf("snode: %w", err)
		}
		r.tables = tables
	} else if d.Registry != nil {
		return nil, errors.New("snode: following the table registry requires managed protocol tables")
	}
	return r, nil
}
```

replace:

```go
	if r.d.Conn == nil {
		return errors.New("snode: clickhouse connection is required to ensure protocol tables")
	}
	if err := ddl.EnsureProtocolTables(ctx, r.d.Conn, r.pinned(), r.cfg.Tables, mode, r.d.Logger); err != nil {
		return fmt.Errorf("snode: ensure protocol tables: %w", err)
	}
	return nil
}
```

with:

```go
	if r.tables == nil {
		return errors.New("snode: clickhouse connection is required to ensure protocol tables")
	}
	if r.d.Registry != nil {
		if err := dataplane.WaitReady(ctx, r.d.Registry, r.cfg.RegistryStartupTimeout); err != nil {
			return fmt.Errorf("snode: %w", err)
		}
	}
	if err := r.tables.Reconcile(ctx, mode); err != nil {
		return fmt.Errorf("snode: ensure protocol tables: %w", err)
	}
	return nil
}
```

in `reconcileProtocolTables` replace:

```go
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		err := r.ensureFn(ctx, ddl.ModeVerifyOnly)
		switch {
		case err == nil:
			consecutive = 0
			timer.Reset(interval)
```

with:

```go
	for {
		registryChanged, triggered := r.tables.Wake()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		case <-registryChanged:
		case <-triggered:
		}

		err := r.ensureFn(ctx, ddl.ModeVerifyOnly)
		switch {
		case err == nil:
			consecutive = 0
			timer.Reset(min(interval, r.tables.NextDelay()))
```

and delete the old static resolver (it moves to `registry.go`):

```go
func (r *Role) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	for _, t := range r.cfg.Tables {
		if t.TableID == tableID {
			return t, nil
		}
	}
	return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
}

```

In `snode/config.go` replace:

```go
	NodeID             string
	NetworkID          string
	SchemaSnapshotID   string
	ExecutorProfileID  string
	SchemaRoot         string
	Tables             []payloadexec.TableSchema
```

with (gofmt realigns the first five fields because the comment splits the block):

```go
	NodeID            string
	NetworkID         string
	SchemaSnapshotID  string
	ExecutorProfileID string
	SchemaRoot        string
	// Tables is the genesis table set. It is the whole table set while the
	// table registry is disabled (or not followed); afterwards the registry
	// takes over and Tables only supplies genesis-origin schemas.
	Tables             []payloadexec.TableSchema
```

and replace:

```go
	// KeeperShardID feeds /sentio/<shard>/unsafe/<table>; v1 uses zero.
	KeeperShardID uint32
```

with:

```go
	// KeeperShardID feeds /sentio/<shard>/unsafe/<table>; v1 uses zero.
	KeeperShardID uint32
	// RegistryStartupTimeout bounds the wait for the registry follower's
	// first answer (0 = dataplane.DefaultRegistryStartupTimeout).
	RegistryStartupTimeout time.Duration
```

- [ ] **Step 5: Gate fresh intake and move the source-claim root to the registry set**

In `snode/staged.go` (`PrepareLocalStatement`) replace:

```go
	schema, err := r.resolveEnvelopeSchema(req.Envelope)
	if err != nil {
		return PreparedLocalResult{}, err
	}

	flat := req.Envelope.StatementID.Flat()
	rec, ok, err := r.journal.load(flat)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("intake journal: %w", err)
	}
	if ok {
```

with:

```go
	flat := req.Envelope.StatementID.Flat()
	rec, ok, err := r.journal.load(flat)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("intake journal: %w", err)
	}
	if !ok || rec.Lifecycle == LifecycleCleaned {
		// A fresh admission: the table must be Active in the registry and
		// Ready on this source. A recorded statement converges regardless.
		if err := r.requireAdmissible(req.Envelope.TargetTableID, req.Envelope.SchemaHash); err != nil {
			return PreparedLocalResult{}, err
		}
	}
	schema, err := r.resolveEnvelopeSchema(req.Envelope)
	if err != nil {
		return PreparedLocalResult{}, err
	}
	if ok {
```

In `snode/view.go` replace everything from the start of the file through the end of `sourceClaimRoot` (lines 1-35) with:

```go
package snode

import (
	"fmt"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/wire"
)

// sourceClaimRoot is the diagnostic state root the source attaches to its
// RC. With an enabled registry it covers the tables still in the state root
// (Active and Retiring incarnations) and derives the schema root from their
// registry hashes; otherwise it covers the configured tables and the
// configured schema root. Nothing compares it: the FSM's check 1 ignores the
// receipt's MatchSourceRoot (arbiter fsm/threeway.go).
func (r *Role) sourceClaimRoot() (string, error) {
	schemaRoot, hashes := r.stateRootTables()
	tables := make([]replay.TableManifest, 0, len(hashes))
	for _, tableID := range sortedTableIDs(hashes) {
		tm := replay.TableManifest{
			TableID:    tableID,
			SchemaHash: hashes[tableID],
		}
		for _, pk := range r.state.partitionsOf(tableID) {
			base := r.state.baseRootOr(pk, "")
			unpromoted := r.state.unpromotedSumOr(pk, "")
			root, err := lthashCombineHex(base, unpromoted)
			if err != nil {
				return "", fmt.Errorf("partition %s/%s: %w", tableID, pk.Partition, err)
			}
			tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{
				TableID: tableID, PartitionID: pk.Partition, Root: root,
			})
		}
		tables = append(tables, tm)
	}
	_, stateRoot, err := replay.AssembleStateRoot(r.cfg.SchemaSnapshotID, schemaRoot, r.cfg.ExecutorProfileID, tables)
	return stateRoot, err
}

// stateRootTables returns the schema root and the table id -> schema hash
// set the source claim root covers.
func (r *Role) stateRootTables() (string, map[string]string) {
	hashes := map[string]string{}
	snap, enabled := r.registryView()
	if !enabled {
		for _, sch := range r.cfg.Tables {
			hashes[sch.TableID] = payloadexec.TableSchemaHash(r.cfg.NetworkID, sch)
		}
		return r.cfg.SchemaRoot, hashes
	}
	for _, inc := range snap.Incarnations {
		if inc.Status == wire.TableStatusActive || inc.Status == wire.TableStatusRetiring {
			hashes[inc.Key()] = inc.SchemaHash
		}
	}
	return payloadexec.SchemaRootFromHashes(hashes), hashes
}

func sortedTableIDs(hashes map[string]string) []string {
	out := make([]string, 0, len(hashes))
	for id := range hashes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
```

Run: `gofmt -l snode; bazel run //:gazelle`
Expected: `gofmt` prints nothing; `snode/BUILD.bazel` gains `registry.go`, `registry_test.go` and `//dataplane/tableset`; nothing else changes.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `go test ./snode/ -count=1 -v -run 'SchemaFor_Follows|RequireAdmissible|SourceClaimRoot_Covers|TableQuiescent|RegistryGatesFresh' 2>&1 | grep -E '^(---|ok)' && go test -race ./snode/ -count=1 && bazel test //snode:snode_test`
Expected: five `--- PASS`; the whole package `ok` under `-race` (measured: 10.4 s), including every existing protocol-table test — `TestRun_ReconcileIsVerifyOnlyAndDroppedTableFailsClosed` still fails closed because a registry-less role reconciles its configured tables in verify-only mode, and the three `TestReconcile_*` retry-budget tests are unchanged because genesis failures keep the role's budget (P9); `PASSED` under Bazel.

- [ ] **Step 7: Commit**

```bash
git add snode/
git commit -m "feat(snode): follow the table registry for intake, promotion and protocol tables" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 7: arbiter-core — the verifier follows the registry

**Files:**
- Create: `verifier/registry.go`
- Modify: `verifier/verifier.go:22-24` (imports), `:54-56` (`Deps`), `:61-63` (`Role`), `:79-81` (`New`), `:147-155` (`ensureProtocolTablesMode`), `:217-228` (`reconcileProtocolTables`), `:250-251` (`handleReplayJob`), `:274-275` (`handleSnapshotQueryJob`)
- Modify: `verifier/config.go:27-30`, `:40-42`, `:89` (`Config`, `validate`)
- Modify: `verifier/backends.go:15-17` (imports), `:71-82` (`CHScanner`, `NewScanner`), `:136-137` (`schemaFor`)
- Modify: `verifier/BUILD.bazel` (gazelle)
- Test: create `verifier/registry_test.go`

**Interfaces:**
- Consumes: as Task 6, plus `replay.ReplayJob.TableSetTransition.Adds[].TableID` and `replay.SnapshotQueryJob.Statement.Envelope.Input.ReadSet.Tables[].TableID` (housegate).
- Produces (package `verifier`): field `Deps.Registry dataplane.RegistryView`; fields `Config.RegistryStartupTimeout`, `Config.AddTransitionReadyWait time.Duration`; `const DefaultAddTransitionReadyWait = 10 * time.Second`; `var ErrAddedTableNotReady`; `func NewRegistryScanner(cfg Config, conn clickhouse.Conn, registry dataplane.RegistryView) *CHScanner` (`NewScanner` delegates with nil); `func (r *Role) TableReady(tableID string) bool`; `func (r *Role) TableSetStats() tableset.Stats`.

- [ ] **Step 1: Write the failing tests**

Create `verifier/registry_test.go` (it reuses `testConfigV`, `scanTableSchema`, `requireCH`, `newVerifierFakeServer`, `startVerifierFakeServer`, `fakeReplayCore`, `fakeScanner`, `fakeSnapshotQueryCore`, `fakeSnapshotQueryReferenceProvider`, `newRoleHarnessVWithSnapshotQuery`, `signedSnapshotQueryJob` from the existing tests):

```go
package verifier

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// fakeRegistryView is a settable dataplane.RegistryView.
type fakeRegistryView struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	changed chan struct{}
	ready   chan struct{}
}

func newFakeRegistryView() *fakeRegistryView {
	v := &fakeRegistryView{changed: make(chan struct{}), ready: make(chan struct{})}
	close(v.ready)
	return v
}

func (v *fakeRegistryView) View() (wire.TableRegistrySnapshot, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snap, v.enabled
}

func (v *fakeRegistryView) Changed() <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.changed
}

func (v *fakeRegistryView) Ready() <-chan struct{} { return v.ready }

func (v *fakeRegistryView) set(incs ...wire.TableIncarnation) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snap = wire.TableRegistrySnapshot{Version: v.snap.Version + 1, Seeded: true, Incarnations: incs}
	v.enabled = true
	close(v.changed)
	v.changed = make(chan struct{})
}

func chainIncarnationV(t *testing.T, seq uint64, networkID string, schema payloadexec.TableSchema, status wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: status,
		SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash(networkID, schema), SchemaJSON: string(js)}
}

func genesisIncarnationV(seq uint64, networkID string, schema payloadexec.TableSchema) wire.TableIncarnation {
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginGenesis,
		Status: wire.TableStatusActive, SchemaHash: payloadexec.TableSchemaHash(networkID, schema)}
}

func addTransitionJob(blockSeq uint64, tableID string) *pb.ReplayJob {
	return wire.ReplayJobToPB(replay.ReplayJob{BlockSeq: blockSeq, TableSetTransition: &replay.ReplayTableSetTransition{
		Adds: []replay.ReplayTableSchema{{TableID: tableID, SchemaJSON: "{}"}}, NewSchemaRoot: "0xroot"}})
}

// newRegistryRoleV builds a verifier on real ClickHouse that follows view.
func newRegistryRoleV(t *testing.T, view *fakeRegistryView, core *fakeReplayCore) (*Role, *verifierFakeServer, Config) {
	t.Helper()
	conn := requireCH(t)
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	server := newVerifierFakeServer()
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startVerifierFakeServer(t, server)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	cfg := testConfigV()
	cfg.ReplicaID = "verifier-" + suffix
	g := scanTableSchema()
	g.TableID = "db.g_" + suffix
	cfg.Tables = []payloadexec.TableSchema{g}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase = "hg_unsafe_"+suffix, "hg_safe_"+suffix, "hg_promote_"+suffix
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	cfg.AddTransitionReadyWait = 200 * time.Millisecond
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	view.set(genesisIncarnationV(1, cfg.NetworkID, g))
	role, err := New(cfg, Deps{Client: client, Replay: core, Scanner: &fakeScanner{}, Conn: conn, Registry: view})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatalf("register: %v", err)
	}
	return role, server, cfg
}

func TestReplayJob_AddTransitionIsAttestedOnlyOnceTheTableIsReady(t *testing.T) {
	view, core := newFakeRegistryView(), &fakeReplayCore{}
	role, server, cfg := newRegistryRoleV(t, view, core)
	n := scanTableSchema()
	n.TableID = "db.n_" + cfg.ReplicaID[len("verifier-"):]
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, n, wire.TableStatusPending))

	// No reconcile pass runs: the gate waits its bound, then refuses.
	err := role.handleReplayJob(context.Background(), addTransitionJob(7, n.TableID))
	if !errors.Is(err, ErrAddedTableNotReady) {
		t.Fatalf("err = %v, want ErrAddedTableNotReady", err)
	}
	if core.jobCount() != 0 {
		t.Fatal("the replay core must not run before the added table is ready")
	}
	if _, _, atts, _ := server.snapshot(); len(atts) != 0 {
		t.Fatalf("attestations = %d, want 0", len(atts))
	}

	if err := role.tables.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
		t.Fatal(err)
	}
	if err := role.handleReplayJob(context.Background(), addTransitionJob(7, n.TableID)); err != nil {
		t.Fatalf("after the table is ready: %v", err)
	}
	if _, _, atts, _ := server.snapshot(); len(atts) != 1 {
		t.Fatalf("attestations = %d, want 1", len(atts))
	}
}

func TestRun_AddTransitionGateLetsTheReconcileLoopCreateTheTable(t *testing.T) {
	view, core := newFakeRegistryView(), &fakeReplayCore{}
	role, server, cfg := newRegistryRoleV(t, view, core)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	n := scanTableSchema()
	n.TableID = "db.n_" + cfg.ReplicaID[len("verifier-"):]
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, n, wire.TableStatusPending))
	// The arbiter re-sends an unattested job every dispatch.retry_interval;
	// the test re-sends it every 500ms until the verifier attests.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		server.push(&pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: addTransitionJob(8, n.TableID)}})
		wait := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(wait) {
			if _, _, atts, _ := server.snapshot(); len(atts) > 0 {
				if !role.TableReady(n.TableID) {
					t.Fatal("attested before the added table was ready")
				}
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Fatal("the transition was never attested")
}

func TestScanner_ResolvesChainTablesThroughTheRegistry(t *testing.T) {
	cfg := testConfigV()
	chain := payloadexec.TableSchema{TableID: "db.c", Columns: []lthash.Column{{Name: "w", Type: "String"}}}
	view := newFakeRegistryView()
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, chain, wire.TableStatusActive))
	s := NewRegistryScanner(cfg, nil, view)
	got, err := s.schemaFor("db.c")
	if err != nil || got.TableID != "db.c" || len(got.Columns) != 1 || got.Columns[0].Name != "w" {
		t.Fatalf("schemaFor(db.c) = %+v, %v", got, err)
	}
	if got, err := s.schemaFor("db.t"); err != nil || got.Columns[0].Name != "v" {
		t.Fatalf("schemaFor(db.t) = %+v, %v", got, err)
	}
	if _, err := NewScanner(cfg, nil).schemaFor("db.c"); err == nil {
		t.Fatal("a scanner without the registry must not know chain tables")
	}
	// A same-name chain recreation of a retired genesis table is scanned
	// with its registry schema, not the stale genesis one.
	recreated := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "x", Type: "Int64"}}}
	purged := genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0])
	purged.Status = wire.TableStatusPurged
	view.set(purged, chainIncarnationV(t, 2, cfg.NetworkID, recreated, wire.TableStatusActive))
	if got, err := s.schemaFor("db.t"); err != nil || got.Columns[0].Name != "x" {
		t.Fatalf("schemaFor(recreated db.t) = %+v, %v", got, err)
	}
}

func TestHandleSnapshotQueryJob_RefusesTablesOutsideTheGenesisSet(t *testing.T) {
	core := &fakeSnapshotQueryCore{}
	references := &fakeSnapshotQueryReferenceProvider{references: []string{"ref"}}
	role, server := newRoleHarnessVWithSnapshotQuery(t, core, references)
	job := signedSnapshotQueryJob(t)
	job.Statement.Envelope.Input.ReadSet.Tables = []replay.SnapshotReadTable{{TableID: "db.dynamic"}}
	err := role.handleSnapshotQueryJob(context.Background(), wire.SnapshotQueryJobToPB(job))
	if err == nil || !strings.Contains(err.Error(), "outside the genesis table set") {
		t.Fatalf("err = %v", err)
	}
	if jobs, _ := core.snapshot(); len(jobs) != 0 || len(references.snapshot()) != 0 || len(server.queryAttestationsSnapshot()) != 0 {
		t.Fatal("a refused read must use no dependency and submit nothing")
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./verifier/ -count=1`
Expected: build failure `cfg.AddTransitionReadyWait undefined (type Config has no field or method AddTransitionReadyWait)`, `unknown field Registry in struct literal of type Deps`, `undefined: ErrAddedTableNotReady`, `role.tables undefined`, `role.TableReady undefined` (and more of the same kind).

- [ ] **Step 3: Add the attestation gate and the snapshot-query refusal**

Create `verifier/registry.go`:

```go
package verifier

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

// DefaultAddTransitionReadyWait bounds how long a replay job whose table-set
// transition adds tables waits for this verifier's reconciler to create them.
// The arbiter re-sends an unattested job to every connected verifier of the
// block every dispatch.retry_interval (default 5s) until quorum, so a refusal
// only delays this verifier's vote to a later redelivery.
const DefaultAddTransitionReadyWait = 10 * time.Second

// ErrAddedTableNotReady refuses to attest a transition that adds a table
// whose hg_* tables this verifier has not created and verified.
var ErrAddedTableNotReady = errors.New("verifier: added table is not ready")

// requireAddedTablesReady is the attestation gate: a transition that adds
// tables is attested only once every added table is Ready here, so an add
// cannot reach quorum before verifiers hold its tables.
func (r *Role) requireAddedTablesReady(ctx context.Context, job replay.ReplayJob) error {
	if job.TableSetTransition == nil || len(job.TableSetTransition.Adds) == 0 {
		return nil
	}
	ids := make([]string, 0, len(job.TableSetTransition.Adds))
	missing := false
	for _, add := range job.TableSetTransition.Adds {
		ids = append(ids, add.TableID)
		if !r.tables.Ready(add.TableID) {
			missing = true
		}
	}
	if !missing {
		return nil
	}
	if r.tables.WaitReady(ctx, ids, r.cfg.AddTransitionReadyWait) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: block %d adds %v", ErrAddedTableNotReady, job.BlockSeq, ids)
}

// requireGenesisReadSet keeps the snapshot-query lane on the static genesis
// table set: its dynamic appender is not built, so a read of any other table
// is refused before any historical read.
func (r *Role) requireGenesisReadSet(job replay.SnapshotQueryJob) error {
	for _, table := range job.Statement.Envelope.Input.ReadSet.Tables {
		found := false
		for _, t := range r.cfg.Tables {
			if t.TableID == table.TableID {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("snapshot query reads %s, which is outside the genesis table set; the snapshot-query lane keeps a static table set", table.TableID)
		}
	}
	return nil
}
```

- [ ] **Step 4: Wire the reconciler, the gate and the registry scanner into the role**

In `verifier/verifier.go` replace:

```go
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)
```

with:

```go
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)
```

replace:

```go
	Conn                   clickhouse.Conn
	Logger                 *slog.Logger
}
```

with:

```go
	Conn                   clickhouse.Conn
	Logger                 *slog.Logger
	// Registry is the table-registry follower. Nil keeps the verifier on its
	// configured tables; it then never attests a transition that adds one.
	Registry dataplane.RegistryView
}
```

replace:

```go
	priv     ed25519.PrivateKey
	ensureFn func(context.Context, ddl.Mode) error
}
```

with:

```go
	priv     ed25519.PrivateKey
	ensureFn func(context.Context, ddl.Mode) error
	// tables is the table-set reconciler; nil when the host owns DDL.
	tables *tableset.Reconciler
}
```

replace:

```go
	r.ensureFn = r.ensureProtocolTablesMode
	return r, nil
}
```

with:

```go
	r.ensureFn = r.ensureProtocolTablesMode
	if cfg.protocolTables != ddl.ModeOff {
		tables, err := tableset.New(tableset.Config{
			Pinned: ddl.Pinned{
				UnsafeDB: cfg.UnsafeDatabase, SafeDB: cfg.SafeDatabase, PromoteDB: cfg.PromoteDatabase,
				NodeID: cfg.ReplicaID, KeeperShardID: cfg.KeeperShardID,
			},
			Genesis: cfg.Tables, Interval: cfg.ProtocolTablesReconcile,
		}, tableset.Deps{Conn: d.Conn, Registry: d.Registry, Arbiter: d.Client, Logger: d.Logger})
		if err != nil {
			return nil, fmt.Errorf("verifier: %w", err)
		}
		r.tables = tables
	} else if d.Registry != nil {
		return nil, fmt.Errorf("verifier: following the table registry requires managed protocol tables")
	}
	return r, nil
}

// TableReady reports whether this verifier's hg_* tables for tableID's
// current incarnation exist and verify.
func (r *Role) TableReady(tableID string) bool { return r.tables.Ready(tableID) }

// TableSetStats returns the reconciler's metrics for the host to export.
func (r *Role) TableSetStats() tableset.Stats {
	if r.tables == nil {
		return tableset.Stats{}
	}
	return r.tables.Stats()
}
```

replace:

```go
	pinned := ddl.Pinned{
		UnsafeDB: r.cfg.UnsafeDatabase, SafeDB: r.cfg.SafeDatabase, PromoteDB: r.cfg.PromoteDatabase,
		NodeID: r.cfg.ReplicaID, KeeperShardID: r.cfg.KeeperShardID,
	}
	if err := ddl.EnsureProtocolTables(ctx, r.d.Conn, pinned, r.cfg.Tables, mode, r.d.Logger); err != nil {
		return fmt.Errorf("verifier: ensure protocol tables: %w", err)
	}
```

with:

```go
	if r.d.Registry != nil {
		if err := dataplane.WaitReady(ctx, r.d.Registry, r.cfg.RegistryStartupTimeout); err != nil {
			return fmt.Errorf("verifier: %w", err)
		}
	}
	if err := r.tables.Reconcile(ctx, mode); err != nil {
		return fmt.Errorf("verifier: ensure protocol tables: %w", err)
	}
```

in `reconcileProtocolTables` replace:

```go
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		err := r.ensureFn(ctx, ddl.ModeVerifyOnly)
		switch {
		case err == nil:
			consecutive = 0
			timer.Reset(interval)
```

with:

```go
	for {
		registryChanged, triggered := r.tables.Wake()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		case <-registryChanged:
		case <-triggered:
		}

		err := r.ensureFn(ctx, ddl.ModeVerifyOnly)
		switch {
		case err == nil:
			consecutive = 0
			timer.Reset(min(interval, r.tables.NextDelay()))
```

replace:

```go
func (r *Role) handleReplayJob(ctx context.Context, m *pb.ReplayJob) error {
	att, err := r.d.Replay.Verify(ctx, wire.ReplayJobFromPB(m))
```

with:

```go
func (r *Role) handleReplayJob(ctx context.Context, m *pb.ReplayJob) error {
	job := wire.ReplayJobFromPB(m)
	if err := r.requireAddedTablesReady(ctx, job); err != nil {
		r.d.Logger.Warn("table-set transition adds a table this verifier has not created; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	att, err := r.d.Replay.Verify(ctx, job)
```

and in `handleSnapshotQueryJob` replace:

```go
	job := wire.SnapshotQueryJobFromPB(m)
	if _, err := verifySnapshotQueryEnvelope(job.Statement.Envelope); err != nil {
```

with:

```go
	job := wire.SnapshotQueryJobFromPB(m)
	if err := r.requireGenesisReadSet(job); err != nil {
		return err
	}
	if _, err := verifySnapshotQueryEnvelope(job.Statement.Envelope); err != nil {
```

In `verifier/config.go` replace:

```go
	Tables            []payloadexec.TableSchema
	UnsafeDatabase    string
	SafeDatabase      string
	PromoteDatabase   string
```

with:

```go
	// Tables is the genesis table set: the whole set while the table
	// registry is disabled (or not followed), afterwards only the schemas of
	// genesis-origin incarnations.
	Tables          []payloadexec.TableSchema
	UnsafeDatabase  string
	SafeDatabase    string
	PromoteDatabase string
```

replace:

```go
	ProtocolTablesMaxFailures int
	KeeperShardID             uint32
}
```

with:

```go
	ProtocolTablesMaxFailures int
	KeeperShardID             uint32
	// RegistryStartupTimeout bounds the wait for the registry follower's
	// first answer (0 = dataplane.DefaultRegistryStartupTimeout).
	RegistryStartupTimeout time.Duration
	// AddTransitionReadyWait bounds the attestation gate's wait for added
	// tables (0 = DefaultAddTransitionReadyWait).
	AddTransitionReadyWait time.Duration
}
```

and in `validate` replace:

```go
	if c.ProtocolTablesMaxFailures < 0 {
```

with:

```go
	if c.AddTransitionReadyWait < 0 {
		errs = append(errs, errors.New("add transition ready wait must not be negative"))
	} else if c.AddTransitionReadyWait == 0 {
		c.AddTransitionReadyWait = DefaultAddTransitionReadyWait
	}
	if c.ProtocolTablesMaxFailures < 0 {
```

In `verifier/backends.go` replace:

```go
	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)
```

with:

```go
	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)
```

replace:

```go
type CHScanner struct {
	cfg  Config
	conn clickhouse.Conn
}

// NewScanner builds a ClickHouse-backed byte-side scanner.
func NewScanner(cfg Config, conn clickhouse.Conn) *CHScanner {
	if cfg.UnsafeDatabase == "" {
		cfg.UnsafeDatabase = defaultUnsafeDatabase
	}
	return &CHScanner{cfg: cfg, conn: conn}
}
```

with:

```go
type CHScanner struct {
	cfg      Config
	conn     clickhouse.Conn
	registry dataplane.RegistryView
}

// NewScanner builds a ClickHouse-backed byte-side scanner over the
// configured tables only.
func NewScanner(cfg Config, conn clickhouse.Conn) *CHScanner {
	return NewRegistryScanner(cfg, conn, nil)
}

// NewRegistryScanner builds a scanner that also resolves chain-origin tables
// through the table registry (nil registry: configured tables only).
func NewRegistryScanner(cfg Config, conn clickhouse.Conn, registry dataplane.RegistryView) *CHScanner {
	if cfg.UnsafeDatabase == "" {
		cfg.UnsafeDatabase = defaultUnsafeDatabase
	}
	return &CHScanner{cfg: cfg, conn: conn, registry: registry}
}
```

and replace:

```go
func (s *CHScanner) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	for _, t := range s.cfg.Tables {
```

with:

```go
// schemaFor resolves the table a scan names. With an enabled registry the
// key's live, non-Purged incarnation decides: a genesis-origin one uses the
// configured schema, a chain-origin one its registry schema_json (so a
// same-name recreation of a retired genesis table is scanned with its new
// schema). Otherwise the configured genesis tables apply.
func (s *CHScanner) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	if s.registry != nil {
		if snap, enabled := s.registry.View(); enabled {
			if live := snap.Live(tableID); live != nil && live.HasPhysicalTables() && live.Origin == wire.TableOriginChain {
				return live.Schema()
			}
		}
	}
	for _, t := range s.cfg.Tables {
```

Run: `gofmt -l verifier; bazel run //:gazelle`
Expected: `gofmt` prints nothing; `verifier/BUILD.bazel` gains `registry.go`, `registry_test.go` and `//dataplane/tableset`; nothing else changes.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./verifier/ -count=1 -v -run 'AddTransition|ResolvesChainTables|OutsideTheGenesisSet' 2>&1 | grep -E '^(---|ok)' && go test -race ./verifier/ -count=1 && bazel test //verifier:verifier_test`
Expected: four `--- PASS`; `ok` under `-race`; `PASSED`.

- [ ] **Step 6: Run the whole module gate, tear the servers down, commit and open the PR**

```bash
go vet ./... && gofmt -l . && bash scripts/check-public-boundary.sh
go test -count=1 ./...                                   # with the Task 4 Step 1 environment: every package ok
go test -race -count=1 ./dataplane/... ./snode/ ./verifier/
bazel run //:gazelle && git status --short               # nothing new: every BUILD file is current
bazel test //...                                         # 13 targets pass
docker exec sp4-ch-a clickhouse-client -q "SELECT name FROM system.zookeeper WHERE path = '/sentio/0/unsafe'"   # prints nothing
docker rm -f sp4-ch-a sp4-ch-b && docker network rm sp4-net
git add verifier/
git commit -m "feat(verifier): follow the table registry and attest an added table only once it is ready" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/si-data-plane-reconciler
gh pr create -R sentioxyz/arbiter-core --fill
```

Expected: every command succeeds as annotated (measured on the prototype: `bazel test //...` 13 of 13; the docker job additionally runs `//dataplane/tableset:tableset_test` in CI). The controller merges the PR and runs Task 12 gate R2 before Task 8 starts.

---

## Task 8: arbiter — pin arbiter-core v0.10.0 and arbiter-proto v0.8.0

**Precondition:** Task 12 gate R2 is done; arbiter-core `v0.10.0` exists on `CORE_SHA`.

**Files:**
- Modify: `go.mod:20-21`, `go.sum`, `MODULE.bazel:9-24`, `MODULE.bazel.lock`
- Modify: `cmd/internal/startup/role_fixtures_test.go:199-230` (the fake ClickHouse answers the reconciler)
- Test: the existing `cmd/internal/startup` role tests

**Interfaces:**
- Consumes: arbiter-core `v0.10.0` (Tasks 2–7); arbiter-proto `v0.8.0` (Task 1).
- Produces: nothing new; the build compiles against the new arbiter-core.

- [ ] **Step 1: Branch and bump the pins**

```bash
cd arbiter && git fetch origin && git switch -c feat/si-data-plane-reconciler origin/main
go get github.com/sentioxyz/arbiter-core@v0.10.0 github.com/sentioxyz/arbiter-proto@v0.8.0 && go mod tidy
grep -E 'sentioxyz/arbiter-(core|proto) ' go.mod   # must print v0.10.0 and v0.8.0
```

In `MODULE.bazel` replace:

```starlark
bazel_dep(
    name = "arbiter_core",
    version = "0.9.1-0.20260923163832-d059aab910fd",
)
```

with:

```starlark
bazel_dep(
    name = "arbiter_core",
    version = "0.10.0",
)
```

and replace:

```starlark
git_override(
    module_name = "arbiter_core",
    # Resolved arbiter-core v0.9.1-0.20260923163832-d059aab910fd; source is pinned by the commit below.
    commit = "d059aab910fdb43e60b6b626943c96e68f0feb09",
    remote = "https://github.com/sentioxyz/arbiter-core",
)
```

with (`CORE_SHA` is the full 40-character commit of the `v0.10.0` tag):

```starlark
git_override(
    module_name = "arbiter_core",
    # Resolved arbiter-core v0.10.0; source is pinned by the commit below.
    commit = "CORE_SHA",
    remote = "https://github.com/sentioxyz/arbiter-core",
)
```

Run: `bazel mod tidy && go build ./... && bazel build //...`
Expected: both builds succeed; `MODULE.bazel.lock` changes only as `bazel mod tidy` records the new module.

- [ ] **Step 2: Run the role tests to see them fail**

Run: `go test ./cmd/internal/startup/ -count=1`
Expected: `TestRunRole_RealRolesRecoverWithoutReregistering` and `TestRunRole_RealRolesReturnPermanentFailures` fail, e.g. `RunRole = snode: ensure protocol tables: ddl: read system.tables for hg_unsafe.db__t: metadata scan: got 5 destinations for 4 values ... ddl: list protocol tables: unexpected query "SELECT database, name, comment FROM system.tables WHERE database IN (?, ?, ?)"`. The fake ClickHouse predates the reconciler's `comment` column and its protocol-table listing.

- [ ] **Step 3: Teach the fake ClickHouse the reconciler's reads**

In `cmd/internal/startup/role_fixtures_test.go` replace:

```go
			strings.Join(intent.SortingKey, ", "), intent.PartitionKey,
		}}
```

with:

```go
			strings.Join(intent.SortingKey, ", "), intent.PartitionKey, intent.Comment,
		}}
```

replace:

```go
func (c *protocolFixtureConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	intent, ok := c.intents[args[0].(string)]
```

with:

```go
func (c *protocolFixtureConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	// The table-set reconciler lists the protocol databases to report
	// tables no desired set accounts for; the fixture holds exactly its intents.
	if strings.Contains(query, "FROM system.tables WHERE database IN") {
		rows := &protocolListRows{}
		for _, intent := range c.intents {
			rows.tables = append(rows.tables, [3]string{intent.Database, intent.Table, intent.Comment})
		}
		return rows, nil
	}
	intent, ok := c.intents[args[0].(string)]
```

and replace:

```go
type protocolFixtureRow struct {
```

with:

```go
type protocolListRows struct {
	driver.Rows
	tables [][3]string
	index  int
}

func (r *protocolListRows) Next() bool { return r.index < len(r.tables) }
func (r *protocolListRows) Scan(dest ...any) error {
	t := r.tables[r.index]
	r.index++
	*dest[0].(*string), *dest[1].(*string), *dest[2].(*string) = t[0], t[1], t[2]
	return nil
}
func (*protocolListRows) Close() error { return nil }
func (*protocolListRows) Err() error   { return nil }

type protocolFixtureRow struct {
```

- [ ] **Step 4: Run the whole suite to verify it passes**

Run: `go test ./... -count=1 && ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 go test ./integration/chpipeline/ ./cmd/internal/startup/ -count=1 && bazel test //...`
Expected: every package `ok` (the second command needs a ClickHouse 25.8 on `127.0.0.1:9000`, like CI's `data-plane integration (docker ClickHouse)` step; measured: `chpipeline` 22 s); `bazel test //...` passes.

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock cmd/internal/startup/role_fixtures_test.go
git commit -m "chore(deps): consume arbiter-core v0.10.0 and arbiter-proto v0.8.0 (table-set reconciler)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 9: arbiter — `SubmitTablePurged` and `GetPurgeNodeSet` handlers

**Files:**
- Modify: `fsm/apply_table_registry.go:256-270` (`purgeNodeIDsLocked`, `PurgeNodeSet`, `completePurgesLocked`)
- Create: `server/table_purge.go`
- Modify: `server/BUILD.bazel` (gazelle)
- Test: create `server/table_purge_test.go`

**Interfaces:**
- Consumes: `pb.PromotionGatewayServer.SubmitTablePurged`, `pb.TableRegistryServer.GetPurgeNodeSet` (Task 1); `wire.RecordTablePurged` (existing); `(*Server).consensusReadBarrier`, `(*Server).propose`, `requireStreamID`, `tableRegistryDisabledMessage` (existing).
- Produces: `func (f *fsm.FSM) PurgeNodeSet() []string`; `(*promotionGatewayService).SubmitTablePurged`; `(*tableRegistryService).GetPurgeNodeSet`; `(*Server).tablePurgedAnswer(nodeID string, seq uint64) (bool, error)`.

- [ ] **Step 1: Write the failing tests**

Create `server/table_purge_test.go` (it reuses `newServerTestFSM`, `startServerWithConfig`, `publishServerGenesis`, `enableRegistryWithPendingAdd`, `mustDirectApply`, `isNotLeader`, `fakeNode`):

```go
package server

import (
	"bytes"
	"slices"
	"testing"

	"github.com/hashicorp/raft"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
)

// registerPurgeNodes registers one SNode (s1) and one verifier (v1): the
// purge node set of the fixtures below.
func registerPurgeNodes(t *testing.T, node *fakeNode) {
	t.Helper()
	mustDirectApply(t, node, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}})
	mustDirectApply(t, node, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: bytes.Repeat([]byte{1}, 32)}}})
}

// retirePending retires the Pending db.n (incarnation 2) whose add was never
// sealed: it goes straight to Purging.
func retirePending(t *testing.T, node *fakeNode) {
	t.Helper()
	mustDirectApply(t, node, wire.Command{RetireTables: &wire.RetireTables{DatabaseID: "db", TableIDs: []string{"n"},
		Deleted: arbiter.L2EventRef{BlockNumber: 102, BlockHash: "0x102", TxHash: "0xtx"}, Reason: wire.TableRetireReasonTableDeleted}})
}

func incarnationStatus(t *testing.T, state *fsm.FSM, seq uint64) (fsm.TableIncarnationStatus, []string) {
	t.Helper()
	view, ok := state.TableRegistryView()
	if !ok || seq > uint64(len(view.Incarnations)) {
		t.Fatalf("incarnation %d missing", seq)
	}
	inc := view.Incarnations[seq-1]
	return inc.Status, inc.PurgedBy
}

func TestSubmitTablePurged_IdempotentAndFailedPreconditionBeforePurging(t *testing.T) {
	state := newServerTestFSM(t)
	conn, _, node := startServerWithConfig(t, startServerConfig{f: state, leader: true})
	publishServerGenesis(t, node)
	client := pb.NewPromotionGatewayClient(conn)
	report := func(nodeID string, seq uint64) error {
		_, err := client.SubmitTablePurged(t.Context(), &pb.RecordTablePurgedCmd{NodeId: nodeID, IncarnationSeq: seq})
		return err
	}
	if err := report("s1", 2); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("disabled registry: %v, want FailedPrecondition", err)
	}
	enableRegistryWithPendingAdd(t, state, node)
	registerPurgeNodes(t, node)
	if err := report("s1", 2); status.Code(err) != codes.FailedPrecondition || isNotLeader(err) {
		t.Fatalf("pending incarnation: %v, want FailedPrecondition without NotLeader", err)
	}
	if err := report("s1", 1); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("active incarnation: %v, want FailedPrecondition", err)
	}
	if err := report("s1", 9); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown incarnation: %v, want InvalidArgument", err)
	}
	if err := report("", 2); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("missing node id: %v, want InvalidArgument", err)
	}
	retirePending(t, node)
	if err := report("stranger", 2); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unregistered node: %v, want the FSM's InvalidArgument", err)
	}
	if err := report("s1", 2); err != nil {
		t.Fatalf("first report: %v", err)
	}
	applied := node.appliedCount()
	if err := report("s1", 2); err != nil {
		t.Fatalf("repeated report: %v", err)
	}
	if node.appliedCount() != applied {
		t.Fatal("an already-recorded node must be answered without a proposal")
	}
	if st, by := incarnationStatus(t, state, 2); st != fsm.TableStatusPurging || !slices.Equal(by, []string{"s1"}) {
		t.Fatalf("after s1: %s %v", st, by)
	}
	if err := report("v1", 2); err != nil {
		t.Fatalf("last report: %v", err)
	}
	if st, _ := incarnationStatus(t, state, 2); st != fsm.TableStatusPurged {
		t.Fatalf("after every node reported: %s, want purged", st)
	}
	for _, id := range []string{"s1", "v1", "stranger"} {
		if err := report(id, 2); err != nil {
			t.Fatalf("report %s on a Purged incarnation: %v, want success", id, err)
		}
	}
}

func TestSubmitTablePurged_FollowerAnswersNotLeader(t *testing.T) {
	state := newServerTestFSM(t)
	conn, _, _ := startServerWithConfig(t, startServerConfig{f: state, leader: false})
	_, err := pb.NewPromotionGatewayClient(conn).SubmitTablePurged(t.Context(), &pb.RecordTablePurgedCmd{NodeId: "s1", IncarnationSeq: 2})
	if !isNotLeader(err) {
		t.Fatalf("follower = %v, want NotLeader", err)
	}
	if _, err := pb.NewTableRegistryClient(conn).GetPurgeNodeSet(t.Context(), &emptypb.Empty{}); !isNotLeader(err) {
		t.Fatalf("follower purge node set = %v, want NotLeader", err)
	}
}

// racedPurgeFuture commits an eviction of s1 (which completes the purge,
// because v1 already reported) just before s1's own proposal applies, the
// interleaving in which the proposal is rejected as "not purging".
type racedPurgeFuture struct {
	state *fsm.FSM
	resp  any
}

func (f *racedPurgeFuture) Error() error {
	for i, cmd := range []wire.Command{
		{EvictNode: &wire.EvictNode{NodeID: "s1", Reason: "decommissioned"}},
		{RecordTablePurged: &wire.RecordTablePurged{NodeID: "s1", IncarnationSeq: 2}},
	} {
		b, err := wire.Encode(cmd)
		if err != nil {
			return err
		}
		f.resp = f.state.Apply(&raft.Log{Index: uint64(1000 + i), Data: b})
	}
	return nil
}

func (f *racedPurgeFuture) Response() any { return f.resp }
func (f *racedPurgeFuture) Index() uint64 { return 1001 }

func TestSubmitTablePurged_RejectedProposalOnAPurgedIncarnationIsSuccess(t *testing.T) {
	state := newServerTestFSM(t)
	conn, _, node := startServerWithConfig(t, startServerConfig{f: state, leader: true})
	publishServerGenesis(t, node)
	enableRegistryWithPendingAdd(t, state, node)
	registerPurgeNodes(t, node)
	retirePending(t, node)
	mustDirectApply(t, node, wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: "v1", IncarnationSeq: 2}})
	raced := &racedPurgeFuture{state: state}
	node.future = raced
	if _, err := pb.NewPromotionGatewayClient(conn).SubmitTablePurged(t.Context(), &pb.RecordTablePurgedCmd{NodeId: "s1", IncarnationSeq: 2}); err != nil {
		t.Fatalf("report whose proposal lost the race: %v, want success", err)
	}
	if _, rejected := raced.resp.(fsm.Rejected); !rejected {
		t.Fatalf("fixture: the proposal must have been rejected, got %T", raced.resp)
	}
	if st, _ := incarnationStatus(t, state, 2); st != fsm.TableStatusPurged {
		t.Fatalf("status = %s, want purged", st)
	}
}

func TestGetPurgeNodeSet(t *testing.T) {
	state := newServerTestFSM(t)
	conn, _, node := startServerWithConfig(t, startServerConfig{f: state, leader: true})
	publishServerGenesis(t, node)
	client := pb.NewTableRegistryClient(conn)
	got, err := client.GetPurgeNodeSet(t.Context(), &emptypb.Empty{})
	if err != nil || len(got.GetNodeIds()) != 0 {
		t.Fatalf("empty membership = %v, %v", got, err)
	}
	registerPurgeNodes(t, node)
	mustDirectApply(t, node, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "v0", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: bytes.Repeat([]byte{2}, 32)}}})
	mustDirectApply(t, node, wire.Command{EvictNode: &wire.EvictNode{NodeID: "v0", Reason: "decommissioned"}})
	got, err = client.GetPurgeNodeSet(t.Context(), &emptypb.Empty{})
	if err != nil || !slices.Equal(got.GetNodeIds(), []string{"s1", "v1"}) {
		t.Fatalf("purge node set = %v, %v; want [s1 v1] (sorted, evicted excluded)", got.GetNodeIds(), err)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./server/ -run 'SubmitTablePurged|GetPurgeNodeSet' -count=1`
Expected: FAIL, e.g. `disabled registry: rpc error: code = Unimplemented desc = method SubmitTablePurged not implemented, want FailedPrecondition`.

- [ ] **Step 3: Expose the purge node set from the FSM**

In `fsm/apply_table_registry.go` replace:

```go
// completePurgesLocked marks Purging incarnations Purged once every current
// non-evicted SNode and verifier has acknowledged. It reads committed state
// only, so re-running it after membership changes is deterministic.
func (f *FSM) completePurgesLocked() {
	reg := f.st.TableRegistry
	if reg == nil {
		return
	}
	required := make([]string, 0, len(f.st.Nodes))
	for id, node := range f.st.Nodes {
		if node != nil && node.Status != NodeEvicted && purgeRole(node) {
			required = append(required, id)
		}
	}
	for _, inc := range reg.Incarnations {
```

with:

```go
// purgeNodeIDsLocked returns every registered, non-evicted SNode and
// verifier, sorted: the nodes whose RecordTablePurged completes a purge.
func (f *FSM) purgeNodeIDsLocked() []string {
	required := make([]string, 0, len(f.st.Nodes))
	for id, node := range f.st.Nodes {
		if node != nil && node.Status != NodeEvicted && purgeRole(node) {
			required = append(required, id)
		}
	}
	slices.Sort(required)
	return required
}

// PurgeNodeSet is purgeNodeIDsLocked for readers outside Apply. The source
// SNode drops a Keeper replica of a purged table only when its name is not in
// this set (a decommissioned node's replica).
func (f *FSM) PurgeNodeSet() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.purgeNodeIDsLocked()
}

// completePurgesLocked marks Purging incarnations Purged once every current
// non-evicted SNode and verifier has acknowledged. It reads committed state
// only, so re-running it after membership changes is deterministic.
func (f *FSM) completePurgesLocked() {
	reg := f.st.TableRegistry
	if reg == nil {
		return
	}
	required := f.purgeNodeIDsLocked()
	for _, inc := range reg.Incarnations {
```

- [ ] **Step 4: Implement the handlers**

Create `server/table_purge.go`:

```go
package server

import (
	"context"
	"slices"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
)

// SubmitTablePurged records that a data-plane node dropped its hg_* tables for
// one Purging incarnation (sub-project 4 §5). It is authenticated like
// AckCleanup: the node is named by node_id and the FSM accepts only a
// registered, non-evicted SNode or verifier. The answer is idempotent:
// an already-recorded node or an already-Purged incarnation succeeds without a
// proposal; an incarnation that is not Purging yet is FAILED_PRECONDITION
// (never success); a follower answers NotLeader.
func (svc *promotionGatewayService) SubmitTablePurged(ctx context.Context, req *pb.RecordTablePurgedCmd) (*pb.Ack, error) {
	if err := requireStreamID("node_id", req.GetNodeId()); err != nil {
		return nil, err
	}
	if req.GetIncarnationSeq() == 0 {
		return nil, status.Error(codes.InvalidArgument, "incarnation_seq is required")
	}
	if err := svc.s.consensusReadBarrier(ctx); err != nil {
		return nil, err
	}
	if done, err := svc.s.tablePurgedAnswer(req.GetNodeId(), req.GetIncarnationSeq()); done {
		if err != nil {
			return nil, err
		}
		return &pb.Ack{}, nil
	}
	res, err := svc.s.propose(ctx, wire.Command{RecordTablePurged: &wire.RecordTablePurged{
		NodeID: req.GetNodeId(), IncarnationSeq: req.GetIncarnationSeq(),
	}})
	if err != nil {
		// A rejection can race the incarnation's last report or an eviction:
		// answer from the committed state the rejection was decided on.
		if status.Code(err) == codes.InvalidArgument {
			if done, answer := svc.s.tablePurgedAnswer(req.GetNodeId(), req.GetIncarnationSeq()); done && answer == nil {
				return &pb.Ack{}, nil
			}
		}
		return nil, err
	}
	if _, ok := res.(fsm.Applied); !ok {
		return nil, status.Errorf(codes.Internal, "unexpected table purged result %T", res)
	}
	return &pb.Ack{}, nil
}

// tablePurgedAnswer decides a purge report from committed state without a
// proposal. done=false means the incarnation is Purging and this node has
// not reported yet, so the report must be proposed.
func (s *Server) tablePurgedAnswer(nodeID string, seq uint64) (bool, error) {
	view, ok := s.d.FSM.TableRegistryView()
	if !ok {
		return true, status.Error(codes.FailedPrecondition, tableRegistryDisabledMessage)
	}
	if seq > uint64(len(view.Incarnations)) {
		return true, status.Errorf(codes.InvalidArgument, "unknown table incarnation %d", seq)
	}
	inc := view.Incarnations[seq-1]
	switch {
	case inc.Status == fsm.TableStatusPurged:
		return true, nil
	case inc.Status != fsm.TableStatusPurging:
		return true, status.Errorf(codes.FailedPrecondition, "table incarnation %d is %s, not purging", seq, inc.Status)
	case slices.Contains(inc.PurgedBy, nodeID):
		return true, nil
	}
	return false, nil
}

// GetPurgeNodeSet serves the committed purge node set behind the leader
// barrier, whether or not the registry is enabled.
func (svc *tableRegistryService) GetPurgeNodeSet(ctx context.Context, _ *emptypb.Empty) (*pb.PurgeNodeSet, error) {
	if err := svc.s.consensusReadBarrier(ctx); err != nil {
		return nil, err
	}
	return &pb.PurgeNodeSet{NodeIds: svc.s.d.FSM.PurgeNodeSet()}, nil
}
```

Run: `bazel run //:gazelle`
Expected: `server/BUILD.bazel` gains `table_purge.go` and `table_purge_test.go`; nothing else changes.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./server/ ./fsm/ -count=1 && bazel test //server:server_test //fsm:fsm_test`
Expected: `ok` twice (all four new tests pass; `TestSubmitTablePurged_RejectedProposalOnAPurgedIncarnationIsSuccess` fails with `code = InvalidArgument desc = record table purged: incarnation 2 is not purging` if the post-rejection re-read is removed — measured); `PASSED` twice.

- [ ] **Step 6: Commit**

```bash
git add fsm/apply_table_registry.go server/table_purge.go server/table_purge_test.go server/BUILD.bazel
git commit -m "feat(server): idempotent SubmitTablePurged and the purge node set read" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 10: arbiter — restore tightening (P12) and the rebuilt orchestrator fixtures

**Files:**
- Modify: `fsm/table_registry_validation.go:68-71` (`validateTableRegistry`, chain branch)
- Modify: `fsm/BUILD.bazel` (gazelle)
- Modify: `orchestrator/table_set_transition_test.go:201-235` (removed test), `:390-401` (`restoreWithGhostRetire` → `restoreWithDuplicateGenesisAdd`), `:406-425` (`TestCheckSeal_LogsAnUnsealableTableSetChange`)
- Test: create `fsm/table_registry_restore_p12_test.go`

**Interfaces:**
- Consumes: `seededRegistryFSM`, `addCmd`, `ev`, `retireCmd`, `snapshotBytes`, `rewriteSnapshotDocument`, `liveIncarnation`, `restoreInto`, `testParams` (existing fsm tests); `restoreForged`, `baseRegistryHarness`, `captureLogs` (existing orchestrator tests).
- Produces: two new restore refusals, `table registry: chain incarnation <seq> is <status> without schema_json` (Pending, Active, Retiring or Purging) and `table registry: chain incarnation <seq> is retiring without an add block`.

- [ ] **Step 1: Write the failing tests**

Create `fsm/table_registry_restore_p12_test.go`:

```go
package fsm

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core/wire"
)

// P12 (carried from sub-project 2c): AddTable admits an incarnation only after
// decoding its schema_json, and only a sealed add can be retired into
// Retiring. A restored container that says otherwise is a forgery.
func TestRestoreRefusesForgedChainIncarnations(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(map[string]any)
		want   string
	}{
		"pending without schema_json": {func(inc map[string]any) { delete(inc, "schema_json") }, "pending without schema_json"},
		"retiring without add block":  {func(inc map[string]any) { inc["status"] = "retiring" }, "retiring without an add block"},
	} {
		t.Run(name, func(t *testing.T) {
			f := seededRegistryFSM(t)
			mustApply(t, f, addCmd(t, f, "db", "n", ev(100, "0x100", 0), ev(101, "0x101", 0), 1))
			data := snapshotBytes(t, f)
			forged := rewriteSnapshotDocument(t, data, data[4], func(doc map[string]any) { tc.mutate(liveIncarnation(doc, "n")) })
			g, err := New(testParams())
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Restore(io.NopCloser(bytes.NewReader(forged))); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("restore = %v, want a refusal naming %q", err, tc.want)
			}
		})
	}
}

// A declaration whose schema_json was empty is recorded Refused
// (schema_decode) and may be retired to Purged; P12 must keep accepting it.
func TestRestoreAcceptsARefusedEmptyDeclaration(t *testing.T) {
	f := seededRegistryFSM(t)
	empty := addCmd(t, f, "db", "e", ev(100, "0x100", 0), ev(101, "0x101", 0), 1)
	empty.AddTable.SchemaJSON = ""
	mustApply(t, f, empty)
	mustApply(t, f, retireCmd("db", wire.TableRetireReasonTableDeleted, ev(110, "0x110", 0), "e"))
	g := restoreInto(t, snapshotBytes(t, f))
	if inc := g.st.TableRegistry.Live("db.e"); inc == nil || inc.Status != TableStatusPurged || inc.SchemaJSON != "" {
		t.Fatalf("restored db.e = %+v, want a Purged incarnation without schema_json", inc)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./fsm/ -run 'ForgedChainIncarnations|RefusedEmptyDeclaration' -count=1`
Expected: FAIL: `restore = <nil>, want a refusal naming "pending without schema_json"` and `restore = <nil>, want a refusal naming "retiring without an add block"`; `TestRestoreAcceptsARefusedEmptyDeclaration` passes.

- [ ] **Step 3: Tighten restore**

In `fsm/table_registry_validation.go` replace:

```go
		case TableOriginChain:
			if inc.Created == nil || inc.SchemaRef == nil || inc.Created.BlockNumber < params.ActivationBlock {
				return fmt.Errorf("table registry: chain incarnation %d lacks its evidence", inc.Seq)
			}
```

with:

```go
		case TableOriginChain:
			if inc.Created == nil || inc.SchemaRef == nil || inc.Created.BlockNumber < params.ActivationBlock {
				return fmt.Errorf("table registry: chain incarnation %d lacks its evidence", inc.Seq)
			}
			// AddTable admits an incarnation only after decoding its
			// schema_json, and only a sealed add can be retired into
			// Retiring (an unsealed Pending one goes straight to Purging).
			if inc.hasPhysicalTables() && inc.SchemaJSON == "" {
				return fmt.Errorf("table registry: chain incarnation %d is %s without schema_json", inc.Seq, inc.Status)
			}
			if inc.Status == TableStatusRetiring && inc.AddBlockSeq == 0 {
				return fmt.Errorf("table registry: chain incarnation %d is retiring without an add block", inc.Seq)
			}
```

Run: `bazel run //:gazelle && go test ./fsm/ -count=1 && go test ./orchestrator/ -count=1`
Expected: `fsm` `ok`; `orchestrator` FAILS in exactly the two fixtures that forged the now-refused states: `TestHandleDispatchRows_DoesNotChallengeATransitionBlockWithoutDispatchInfo` (`restore the forged state: restore state: table registry: chain incarnation 2 is pending without schema_json`) and `TestCheckSeal_LogsAnUnsealableTableSetChange` (`... chain incarnation 2 is retiring without schema_json`).

- [ ] **Step 4: Rebuild the two orchestrator fixtures (P15)**

In `orchestrator/table_set_transition_test.go` delete the test whose forged state restore now refuses (the header-only guard stays covered by `TestHandleDispatchRows_DoesNotChallengeATransitionBlock` just above it):

```go
// M2: the don't-challenge guard reads the header itself, so it holds even when
// the block's registry-derived dispatch material is unavailable (here the
// added incarnation's schema JSON is gone).
func TestHandleDispatchRows_DoesNotChallengeATransitionBlockWithoutDispatchInfo(t *testing.T) {
	f, node, _, o, _ := registryHarness(t)
	if err := o.checkSeal(context.Background()); err != nil {
		t.Fatal(err)
	}
	markReplayingViaNode(t, node, 1)
	for _, rid := range verifierSetOf(t, f, 1) {
		attestTransitionViaNode(t, node, f, 1, rid, "0xwrong")
	}
	restoreForged(t, f, func(doc map[string]any) {
		for _, raw := range doc["table_registry"].(map[string]any)["incarnations"].([]any) {
			if inc := raw.(map[string]any); inc["table_id"] == "n" {
				delete(inc, "schema_json")
			}
		}
	})
	if _, ok := f.BlockDispatchInfo(1); ok {
		t.Fatal("fixture: the transition block's dispatch info must be unavailable")
	}
	ws, err := f.WorkSet()
	if err != nil || len(ws.QuorumFailed) != 1 {
		t.Fatalf("workset = %+v, %v", ws, err)
	}
	before := node.applyCount()
	if err := o.handleDispatchRows(context.Background(), ws); err != nil {
		t.Fatal(err)
	}
	if node.applyCount() != before {
		t.Fatal("a transition block must never be challenged, dispatch info or not")
	}
}

```

replace:

```go
// restoreWithGhostRetire appends a Retiring chain incarnation db.ghost that
// no table set ever held: a change set that cannot apply to the published
// base.
func restoreWithGhostRetire(t *testing.T, f *fsm.FSM) {
	t.Helper()
	restoreForged(t, f, func(doc map[string]any) {
		reg := doc["table_registry"].(map[string]any)
		incs := reg["incarnations"].([]any)
		reg["incarnations"] = append(incs, map[string]any{"seq": len(incs) + 1, "database_id": "db", "table_id": "ghost", "origin": "chain", "status": "retiring",
			"created": map[string]any{"block_number": 100, "block_hash": "0x100"}, "schema_ref": map[string]any{"block_number": 101, "block_hash": "0x101"}})
	})
}
```

with:

```go
// restoreWithDuplicateGenesisAdd appends a Pending chain incarnation of the
// genesis table db.t that the base already holds: a committed change set that
// cannot apply to the published base. Every field is one AddTable could have
// written, so restore (including the P12 checks) accepts it; only the D9 rule
// that admission applies would have refused the declaration.
func restoreWithDuplicateGenesisAdd(t *testing.T, f *fsm.FSM, params fsm.Params) {
	t.Helper()
	schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "value", Type: "UInt64"}}}
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	restoreForged(t, f, func(doc map[string]any) {
		reg := doc["table_registry"].(map[string]any)
		incs := reg["incarnations"].([]any)
		reg["incarnations"] = append(incs, map[string]any{"seq": len(incs) + 1, "database_id": "db", "table_id": "t", "origin": "chain", "status": "pending",
			"created": map[string]any{"block_number": 100, "block_hash": "0x100"}, "schema_ref": map[string]any{"block_number": 101, "block_hash": "0x101"},
			"schema_version": 1, "schema_hash": payloadexec.TableSchemaHash(params.NetworkID, schema), "schema_json": string(js)})
	})
}
```

and in `TestCheckSeal_LogsAnUnsealableTableSetChange` replace:

```go
	f, node, _, o, _ := baseRegistryHarness(t)
	restoreWithGhostRetire(t, f)
```

with:

```go
	f, node, _, o, params := baseRegistryHarness(t)
	restoreWithDuplicateGenesisAdd(t, f, params)
```

and replace:

```go
	if n := strings.Count(out, "level=ERROR"); n != 1 || !strings.Contains(out, "cannot be sealed") || !strings.Contains(out, "not in the base table set") {
```

with:

```go
	if n := strings.Count(out, "level=ERROR"); n != 1 || !strings.Contains(out, "cannot be sealed") || !strings.Contains(out, "already in the base table set") {
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `go test ./fsm/ ./orchestrator/ ./server/ ./tableregistry/ -count=1 && bazel test //fsm:fsm_test //orchestrator:orchestrator_test`
Expected: `ok` four times; `PASSED` twice.

- [ ] **Step 6: Commit**

```bash
git add fsm/table_registry_validation.go fsm/table_registry_restore_p12_test.go fsm/BUILD.bazel orchestrator/table_set_transition_test.go
git commit -m "fix(fsm): refuse restoring a schema-less chain incarnation or a Retiring one without an add block (P12)" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 11: arbiter — the reference roles follow the registry

**Files:**
- Create: `cmd/internal/startup/registry.go`
- Modify: `cmd/arbiter-snode/main.go:205-211`, `cmd/arbiter-verifier/main.go:212-218` (`run`)
- Modify: `cmd/arbiter-snode/config.go:21`, `cmd/arbiter-verifier/config.go:25` (`Config` doc)
- Modify: `cmd/arbiter-snode/main_test.go:305`, `cmd/arbiter-verifier/main_test.go:309`, `cmd/arbiter-snode/startup_test.go:27`, `cmd/arbiter-verifier/startup_test.go:27`
- Create: `cmd/arbiter-snode/registry_fixture_test.go`, `cmd/arbiter-verifier/registry_fixture_test.go`
- Modify: `README.md:552-556` (schema sources)
- Modify: `cmd/internal/startup/BUILD.bazel`, `cmd/arbiter-snode/BUILD.bazel`, `cmd/arbiter-verifier/BUILD.bazel` (gazelle)
- Test: create `cmd/internal/startup/registry_test.go`

**Interfaces:**
- Consumes: `dataplane.NewRegistryFollower`, `snode.Deps.Registry`, `verifier.Deps.Registry`, `verifier.NewRegistryScanner` (arbiter-core v0.10.0); `startup.RunRole` (existing).
- Produces: `type startup.RegistryRunner interface { Run(context.Context) error }`; `func startup.RunRoleWithRegistry(ctx context.Context, logger *slog.Logger, name string, registry RegistryRunner, role Role) error`.

- [ ] **Step 1: Write the failing tests**

Create `cmd/internal/startup/registry_test.go`:

```go
package startup

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type registryRunnerFunc func(context.Context) error

func (f registryRunnerFunc) Run(ctx context.Context) error { return f(ctx) }

type blockingRole struct{ registered chan struct{} }

func (r blockingRole) Register(ctx context.Context) error {
	close(r.registered)
	<-ctx.Done()
	return ctx.Err()
}

func (blockingRole) Run(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }

func TestRunRoleWithRegistry_FollowerFailureStopsTheRole(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	role := blockingRole{registered: make(chan struct{})}
	failure := errors.New("unimplemented method")
	err := RunRoleWithRegistry(ctx, slog.Default(), "snode", registryRunnerFunc(func(ctx context.Context) error {
		<-role.registered
		return failure
	}), role)
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "table registry follower") {
		t.Fatalf("err = %v, want the follower failure", err)
	}
}

func TestRunRoleWithRegistry_RoleResultWinsWhenTheFollowerIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	role := blockingRole{registered: make(chan struct{})}
	go func() { <-role.registered; cancel() }()
	err := RunRoleWithRegistry(ctx, slog.Default(), "verifier", registryRunnerFunc(func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}), role)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want the role's cancellation", err)
	}
}
```

- [ ] **Step 2: Run them to verify they fail**

Run: `go test ./cmd/internal/startup/ -run RunRoleWithRegistry -count=1`
Expected: build failure `undefined: RunRoleWithRegistry`.

- [ ] **Step 3: Implement the runner and wire both binaries**

Create `cmd/internal/startup/registry.go`:

```go
package startup

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// RegistryRunner is the lifecycle of the arbiter-core table-registry
// follower (*dataplane.RegistryFollower).
type RegistryRunner interface {
	Run(context.Context) error
}

// RunRoleWithRegistry runs the registry follower beside the role. Every role
// with arbiter peers follows the registry; no config key turns it on. The role
// itself waits for the follower's first answer. A follower that fails with a
// non-retryable error stops the role, and its error is returned.
func RunRoleWithRegistry(ctx context.Context, logger *slog.Logger, name string, registry RegistryRunner, role Role) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	followerErr := make(chan error, 1)
	go func() {
		err := registry.Run(ctx)
		if ctx.Err() == nil {
			cancel()
		}
		followerErr <- err
	}()
	roleErr := RunRole(ctx, logger, name, role)
	cancel()
	if err := <-followerErr; err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("table registry follower: %w", err)
	}
	return roleErr
}
```

In `cmd/arbiter-snode/main.go` (`run`) replace:

```go
	role, err := snode.New(cfg.toRoleConfig(tables, schemaSourceFor(cfg)), snode.Deps{
		Client: client, Conn: conn, Payloads: payloads, Logger: logger,
	})
	if err != nil {
		return err
	}
	return startup.RunRole(ctx, logger, "snode", role)
```

with:

```go
	registry := dataplane.NewRegistryFollower(client, logger)
	role, err := snode.New(cfg.toRoleConfig(tables, schemaSourceFor(cfg)), snode.Deps{
		Client: client, Conn: conn, Payloads: payloads, Logger: logger, Registry: registry,
	})
	if err != nil {
		return err
	}
	return startup.RunRoleWithRegistry(ctx, logger, "snode", registry, role)
```

In `cmd/arbiter-verifier/main.go` (`run`) replace:

```go
	role, err := verifier.New(roleCfg, verifier.Deps{
		Client: client, Replay: core, Scanner: verifier.NewScanner(roleCfg, conn), Conn: conn, Logger: logger,
	})
	if err != nil {
		return err
	}
	return startup.RunRole(ctx, logger, "verifier", role)
```

with:

```go
	registry := dataplane.NewRegistryFollower(client, logger)
	role, err := verifier.New(roleCfg, verifier.Deps{
		Client: client, Replay: core, Scanner: verifier.NewRegistryScanner(roleCfg, conn, registry), Conn: conn, Logger: logger,
		Registry: registry,
	})
	if err != nil {
		return err
	}
	return startup.RunRoleWithRegistry(ctx, logger, "verifier", registry, role)
```

In both `cmd/arbiter-snode/config.go` and `cmd/arbiter-verifier/config.go` replace:

```go
type Config struct {
```

with:

```go
// Config is the role's YAML config. Exactly one of Tables and TableIDs names
// the genesis table set; the dynamic table registry, followed through Peers
// with no further key, owns every other table.
type Config struct {
```

Run: `go test ./cmd/internal/startup/ ./cmd/arbiter-snode/ ./cmd/arbiter-verifier/ -count=1`
Expected: `startup` `ok`; each binary package FAILS the two tests that expect a ClickHouse dial before any arbiter call: `TestRun_ClickHouseUnavailableWaitsForCancellation/table_ids=false` (`log "" does not contain "level=WARN"`) and `TestCLI_SIGTERMInterruptsClickHouseHandshake/table_ids=false` (`accept ClickHouse dial: ... i/o timeout`). The role now waits for the registry first, and their `peers` point at nothing (P16).

- [ ] **Step 4: Give those tests an arbiter whose registry is disabled**

Create `cmd/arbiter-snode/registry_fixture_test.go`:

```go
package main

import (
	"context"
	"net"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// disabledRegistry is an arbiter peer whose table registry is disabled: the
// role's registry follower becomes Ready at once and the role proceeds to
// ClickHouse, which is what the startup tests exercise.
type disabledRegistry struct {
	pb.UnimplementedTableRegistryServer
}

func (disabledRegistry) GetTableRegistry(context.Context, *emptypb.Empty) (*pb.TableRegistrySnapshot, error) {
	return nil, status.Error(codes.FailedPrecondition, "table registry is disabled")
}

func (disabledRegistry) WatchTableRegistry(_ *pb.WatchTableRegistryRequest, stream grpc.ServerStreamingServer[pb.TableRegistrySnapshot]) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

// disabledRegistryPeer serves disabledRegistry and returns its address.
func disabledRegistryPeer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterTableRegistryServer(srv, disabledRegistry{})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}
```

Create `cmd/arbiter-verifier/registry_fixture_test.go` with the same content (it is `package main` in its own directory):

```go
package main

import (
	"context"
	"net"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// disabledRegistry is an arbiter peer whose table registry is disabled: the
// role's registry follower becomes Ready at once and the role proceeds to
// ClickHouse, which is what the startup tests exercise.
type disabledRegistry struct {
	pb.UnimplementedTableRegistryServer
}

func (disabledRegistry) GetTableRegistry(context.Context, *emptypb.Empty) (*pb.TableRegistrySnapshot, error) {
	return nil, status.Error(codes.FailedPrecondition, "table registry is disabled")
}

func (disabledRegistry) WatchTableRegistry(_ *pb.WatchTableRegistryRequest, stream grpc.ServerStreamingServer[pb.TableRegistrySnapshot]) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

// disabledRegistryPeer serves disabledRegistry and returns its address.
func disabledRegistryPeer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterTableRegistryServer(srv, disabledRegistry{})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}
```

In both `cmd/arbiter-snode/main_test.go` and `cmd/arbiter-verifier/main_test.go` (`TestRun_ClickHouseUnavailableWaitsForCancellation`) replace:

```go
			cfg.ClickHouseAddr = "127.0.0.1:1"
```

with:

```go
			cfg.ClickHouseAddr = "127.0.0.1:1"
			cfg.Peers[0].GRPCAddr = disabledRegistryPeer(t)
```

In `cmd/arbiter-snode/startup_test.go` replace:

```go
			config := strings.Replace(validSNodeConfig(t), "127.0.0.1:9000", listener.Addr().String(), 1)
```

with:

```go
			config := strings.Replace(validSNodeConfig(t), "127.0.0.1:9000", listener.Addr().String(), 1)
			config = strings.Replace(config, "127.0.0.1:7080", disabledRegistryPeer(t), 1)
```

and in `cmd/arbiter-verifier/startup_test.go` replace:

```go
			config := strings.Replace(validVerifierConfig(t), "127.0.0.1:9000", listener.Addr().String(), 1)
```

with:

```go
			config := strings.Replace(validVerifierConfig(t), "127.0.0.1:9000", listener.Addr().String(), 1)
			config = strings.Replace(config, "127.0.0.1:7080", disabledRegistryPeer(t), 1)
```

(The helper-process child dials the parent test's listener, which stays up for the whole subtest.)

In `README.md` replace:

```markdown
- `table_ids` derives columns and `partition_by` from the local ClickHouse
  `system.columns`/`system.tables` metadata. Logical `db.t` maps to physical
  table `db__t` in `unsafe_database`.
```

with:

```markdown
- `table_ids` derives columns and `partition_by` from the local ClickHouse
  `system.columns`/`system.tables` metadata. Logical `db.t` maps to physical
  table `db__t` in `unsafe_database`.

Either key names only the genesis table set. Both roles always follow the
arbiter's dynamic storage-integrity table registry through their `peers` (no
key turns this on): startup waits up to two minutes for the registry's first
answer and fails otherwise. While the registry is disabled the genesis set is
the whole table set, exactly as before; once governance enables it, the
registry decides which tables exist, the role creates and drops chain-origin
`hg_*` tables itself (always in `create` mode), and the genesis tables keep the
mode their schema source implies. Startup preflight (`table_ids` schema
discovery) still waits only for the genesis tables.
```

(This README section is hard-wrapped already; the new paragraph follows the surrounding style.)

Run: `gofmt -l cmd; bazel run //:gazelle`
Expected: `gofmt` prints nothing; the three `BUILD.bazel` files gain the new sources and, for the two binaries' tests, `@com_github_sentioxyz_arbiter_proto//gen/pb`, `@org_golang_google_grpc//:grpc`, `@org_golang_google_grpc//codes`, `@org_golang_google_grpc//status`, `@org_golang_google_protobuf//types/known/emptypb`.

- [ ] **Step 5: Run the whole suite to verify it passes**

Run: `go test ./... -count=1 && ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 go test ./integration/chpipeline/ ./cmd/internal/startup/ -count=1 && bazel test //...`
Expected: every package `ok` (measured on the prototype: no failure anywhere); `bazel test //...` passes.

- [ ] **Step 6: Commit and open the PR**

```bash
git add cmd/ README.md
git commit -m "feat(cmd): run the table-registry follower beside the reference SNode and verifier" -m "Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
git push -u origin feat/si-data-plane-reconciler
gh pr create -R sentioxyz/arbiter --fill
```

The controller merges it in Task 12 gate R3.

---

## Task 12: Release and pin sequence (controller)

**Files:** none (PR merges, git tags, GitHub releases).

**Interfaces:**
- Produces: `PROTO_SHA` and tag arbiter-proto `v0.8.0`; `CORE_SHA` and tag arbiter-core `v0.10.0`; the arbiter merge commit `ARB_SHA`.

- [ ] **Gate R1 — arbiter-proto (after Task 1)**

```bash
gh pr checks -R sentioxyz/arbiter-proto feat/si-table-purge        # the `proto` job green: lint, breaking, gen/ current, make test
gh pr merge -R sentioxyz/arbiter-proto --squash feat/si-table-purge
git -C arbiter-proto fetch origin && PROTO_SHA=$(git -C arbiter-proto rev-parse origin/main) && echo $PROTO_SHA
git -C arbiter-proto show $PROTO_SHA:proto/arbiter.proto | grep -n 'rpc SubmitTablePurged (RecordTablePurgedCmd) returns (Ack)'
gh workflow run cut-release.yml -R sentioxyz/arbiter-proto --ref main -f dry_run=true     # prints the version; expect v0.8.0
gh workflow run cut-release.yml -R sentioxyz/arbiter-proto --ref main
gh run watch -R sentioxyz/arbiter-proto $(gh run list -R sentioxyz/arbiter-proto --workflow cut-release.yml --limit 1 --json databaseId --jq '.[0].databaseId')
git -C arbiter-proto fetch --tags && git -C arbiter-proto rev-parse 'v0.8.0^{commit}'     # must equal PROTO_SHA
```

If the workflow derives another version (a second cut on the same UTC day gives `v0.7.2`), use the tag it cut everywhere this plan says `v0.8.0`.

- [ ] **Gate R2 — arbiter-core (after Task 7)**

```bash
gh pr checks -R sentioxyz/arbiter-core feat/si-data-plane-reconciler    # `test` and `integration-clickhouse` green; the latter lists //dataplane/tableset:tableset_test
gh pr merge -R sentioxyz/arbiter-core --squash feat/si-data-plane-reconciler
git -C arbiter-core fetch origin && CORE_SHA=$(git -C arbiter-core rev-parse origin/main) && echo $CORE_SHA
git -C arbiter-core show $CORE_SHA:go.mod | grep 'sentioxyz/arbiter-proto v0.8.0'
gh workflow run cut-release.yml -R sentioxyz/arbiter-core --ref main    # Cut Release derives the version from scripts/next-version.sh; expect v0.10.0
gh run watch -R sentioxyz/arbiter-core $(gh run list -R sentioxyz/arbiter-core --workflow cut-release.yml --limit 1 --json databaseId --jq '.[0].databaseId')
git -C arbiter-core fetch --tags && git -C arbiter-core rev-parse 'v0.10.0^{commit}'    # must equal CORE_SHA
```

If the workflow derives another version, use the tag it cut everywhere this plan says `v0.10.0`, including Task 8's `MODULE.bazel` edit.

- [ ] **Gate R3 — arbiter (after Task 11)**

```bash
gh pr checks -R sentioxyz/arbiter feat/si-data-plane-reconciler   # unit (race), data-plane integration (docker ClickHouse) and the rest green
gh pr merge -R sentioxyz/arbiter --squash feat/si-data-plane-reconciler
git -C arbiter fetch origin && ARB_SHA=$(git -C arbiter rev-parse origin/main) && echo $ARB_SHA
```

arbiter is not tagged by this plan; sub-project 5 deploys it (arbiter voters first, then verifiers, then SNodes, umbrella §12 order).

- [ ] **Gate R4 — hand over**

Record `PROTO_SHA`, the arbiter-proto tag, `CORE_SHA`, the arbiter-core tag and `ARB_SHA` in plan B's tracking issue. Plan B's pins are in the next section.

---

## Handoff to plan B

1. **Releases and pins.** arbiter-proto tag `v0.8.0` on `PROTO_SHA` (the `main` commit that merges Task 1); pin it by the tag: go.mod `github.com/sentioxyz/arbiter-proto v0.8.0` (arbiter-proto has no Bazel pin; `go_deps.from_file` resolves it). arbiter-core tag `v0.10.0` on `CORE_SHA` (the `main` commit that merges Tasks 2–7); pin it the way sentio-node pins arbiter-core today: go.mod `github.com/sentioxyz/arbiter-core v0.10.0`, and in `MODULE.bazel` `bazel_dep(name = "arbiter_core", version = "0.10.0")` plus `git_override(module_name = "arbiter_core", commit = CORE_SHA, remote = "https://github.com/sentioxyz/arbiter-core")` with the comment `# Resolved arbiter-core v0.10.0; source is pinned by the commit below.` arbiter-core `v0.10.0` still pins housegate `db46c31` (`v0.14.2-0.20260923160348-db46c31d5501`). The tags actually cut in Task 12 are authoritative; if the workflows cut other versions, gate R4 records them.
2. **Constructing the follower from a `dataplane.Client`** (package `github.com/sentioxyz/arbiter-core/dataplane`). One follower per process: the host constructs it, runs it, and passes the same value to `snode.Deps.Registry` and to its own `TableState`:
   ```go
   client, err := dataplane.New(dataplane.Config{Peers: peers})   // existing; the SNode already needs one
   follower := dataplane.NewRegistryFollower(client, logger)      // *dataplane.RegistryFollower
   go func() { errc <- follower.Run(ctx) }()                       // returns ctx.Err() or a non-retryable transport error
   if err := dataplane.WaitReady(ctx, follower, dataplane.DefaultRegistryStartupTimeout); err != nil {
       return err // wraps dataplane.ErrRegistryNotReady after the timeout
   }
   snap, enabled := follower.View()                               // build the first TableState snapshot before housegate runs
   ```
   - `func NewRegistryFollower(c *Client, logger *slog.Logger) *RegistryFollower`
   - `func (f *RegistryFollower) Run(ctx context.Context) error` — one `GetTableRegistry` through `WithLeaderRetry`, then `WatchTableRegistry(since_version = last accepted)` on the subscription loop (leader hints, reconnect, backoff, no per-call timeout). A "table registry is disabled" answer, or an arbiter without the `TableRegistry` service, makes the follower Ready and disabled; the watch then waits and delivers the first snapshot once governance enables the registry.
   - `func (f *RegistryFollower) View() (wire.TableRegistrySnapshot, bool)` — the last accepted snapshot and `enabled`; the zero snapshot and `false` before Ready and while disabled. Only a strictly greater version is accepted; an undecodable message is logged and skipped. The snapshot is shared: read-only.
   - `func (f *RegistryFollower) Changed() <-chan struct{}` — closed when the next version after this call is accepted (take a fresh channel after each wake).
   - `func (f *RegistryFollower) Ready() <-chan struct{}` — closed after the first `GetTableRegistry` answer.
   - `func (f *RegistryFollower) Connected() bool` — a `WatchTableRegistry` stream is open (the "registry follower connection state" metric).
   - `type RegistryView interface { View() (wire.TableRegistrySnapshot, bool); Changed() <-chan struct{}; Ready() <-chan struct{} }` — `*RegistryFollower` implements it; a test can supply a settable fake.
   - `func WaitReady(ctx context.Context, v RegistryView, timeout time.Duration) error`; `const DefaultRegistryStartupTimeout = 2 * time.Minute`; `var ErrRegistryNotReady`; `const TableRegistryDisabledMessage = "table registry is disabled"`.
3. **Wire snapshot types** (package `github.com/sentioxyz/arbiter-core/wire`):
   - `type TableRegistrySnapshot struct { Params arbiter.TableRegistryParams; Version uint64; Seeded bool; Cursor TableRegistryCursor; Incarnations []TableIncarnation }` — incarnations in seq order, `Seq == index + 1`; `Params.SIIndexerID` and `Params.ActivationBlock` are the fields the `Lookup` rules need (`arbiter.TableRegistryParams`, root package `github.com/sentioxyz/arbiter-core`).
   - `type TableRegistryCursor struct { BlockNumber uint64; BlockHash string; LogIndex uint64; BlockComplete bool }`.
   - `type TableIncarnation struct { Seq uint64; DatabaseID, TableID string; Origin TableOrigin; Status TableIncarnationStatus; Created, SchemaRef *arbiter.L2EventRef; SchemaVersion uint32; SchemaHash, SchemaJSON, RefusedReason, RefusedCode string; Deleted *arbiter.L2EventRef; RetireReason TableRetireReason; AddBlockSeq, RetireBlockSeq uint64; PurgedBy []string }`.
   - `type TableIncarnationStatus string`: `TableStatusLegacy = "legacy"`, `TableStatusPending = "pending"`, `TableStatusRefused = "refused"`, `TableStatusActive = "active"`, `TableStatusRetiring = "retiring"`, `TableStatusPurging = "purging"`, `TableStatusPurged = "purged"` (arbiter fsm spellings). `type TableOrigin string`: `TableOriginGenesis = "genesis"`, `TableOriginLegacy = "legacy"`, `TableOriginChain = "chain"`. `TableRetireReason` is the existing int32 type (`TableRetireReasonUnspecified`, `TableRetireReasonTableDeleted`, `TableRetireReasonDatabaseDeleted`).
   - `func (s TableRegistrySnapshot) Live(key string) *TableIncarnation` — arbiter's `TableRegistryView.Live`: the newest incarnation of `key` (`<database>.<table>`, `arbiter.TableKey`) that is neither Purged nor Refused, else the newest one, else nil. The pointer aliases the snapshot.
   - `func (s TableRegistrySnapshot) ActiveTables() []TableIncarnation` (seq order).
   - `func (t TableIncarnation) Key() string`; `func (t TableIncarnation) HasPhysicalTables() bool` (Pending, Active, Retiring, Purging).
   - `func (t TableIncarnation) Schema() (payloadexec.TableSchema, error)` — `payloadexec.DecodeTableSchemaJSON(t.Key(), t.SchemaJSON)`; an error for a genesis incarnation, which has no `schema_json`. It does not check the hash: the `TableState` builder recomputes `payloadexec.TableSchemaHash(network_id, schema)` against `SchemaHash` (spec §9.2).
   - `func TableRegistrySnapshotFromPB(*pb.TableRegistrySnapshot) (TableRegistrySnapshot, error)` (an UNSPECIFIED or unknown status or origin, an unknown retire reason, or a misnumbered seq is an error) and `func TableRegistrySnapshotToPB(TableRegistrySnapshot) *pb.TableRegistrySnapshot` (for a fake registry server in sentio-node's acceptance test).
4. **Readiness of an embedded SNode** (package `github.com/sentioxyz/arbiter-core/snode`):
   - Pass the follower as `snode.Deps{..., Registry: follower}`. Nil keeps the SNode on its configured tables exactly as before. Following the registry requires managed protocol tables (`SchemaSource` `network_state` or `chain`, or `clickhouse` for verify-only genesis tables) and a ClickHouse connection; `snode.New` refuses `Registry` with `SchemaSourceUnmanaged`.
   - `func (r *Role) TableReady(tableID string) bool` — true when all three `hg_*` tables of the key's current incarnation exist and verify on this node; with the registry enabled the verified incarnation must also be the key's live one, so an earlier incarnation's tables never count. Report a table Active to housegate only when the registry says Active and this is true (spec E6). It turns true within one reconcile pass of a registry change (the SNode's reconcile loop wakes on `Changed()`), so the `TableState` should rebuild on `follower.Changed()` and also re-check readiness periodically (for example once per second while any registry-Active table is not yet Ready).
   - `func (r *Role) TableSetStats() tableset.Stats` — `tableset.Stats{States map[tableset.State]int; Failures map[string]uint64; Unknown []string; LeftoverDrops uint64}`, states `creating`, `ready`, `waiting_quiescence`, `purging`, `purge_reported` (package `github.com/sentioxyz/arbiter-core/dataplane/tableset`). arbiter-core registers no metrics; export these on housegate's `MetricsRegistry()` if wanted.
   - `snode.Config.Tables` now means the genesis table set. `snode.Config.RegistryStartupTimeout time.Duration` (0 = 2 minutes): `Register` and `RunWithReady` first wait for the follower's Ready, then fail with an error wrapping `dataplane.ErrRegistryNotReady`.
   - `snode.ErrTableNotReady`: a fresh `PrepareLocalStatement` on a Pending table, or on an Active table this source has not made Ready, is refused before any journal record, payload spool or ClickHouse write. A fresh statement on a Retiring, Purging, Purged, Refused, Legacy or unknown table is refused with the existing `ErrSchemaUnknown`; a schema hash that differs from the registry's with `ErrSchemaHashMismatch`. The sentio-node adapter (`storageintegrityadapter/adapter.go`) should add `snode.ErrTableNotReady` to its pre-write terminal list next to `ErrSchemaHashMismatch` (housegate refuses such statements first; this is defence in depth). Statements already journaled converge regardless of the registry.
   - Purging: the SNode drops a table only once its promotion intents, promoted-but-uncleaned unsafe parts, unpromoted rows and unfinished intake records for it are gone; it then removes decommissioned Keeper replicas and reports `SubmitTablePurged` itself. The host has nothing to do for purges.
5. **Behaviour plan B can rely on.** Chain-origin tables carry `COMMENT 'hg_incarnation=<seq>'` on all three tables; genesis tables stay unmarked. A table the registry does not know is never dropped, only reported (`Stats.Unknown`). While the registry is disabled (or the arbiter lacks the service) the SNode reconciles exactly its configured genesis tables in their configured mode, as before. `verifier.Deps.Registry`, `verifier.NewRegistryScanner` and `verifier.Role.TableReady` / `TableSetStats` exist for hosts that embed a verifier; sentio-node does not.

### Gate R4 record (2026-09-26, execution)

- arbiter-proto: PR sentioxyz/arbiter-proto#16 merged; `PROTO_SHA = de2cb9120af81846236de510b62203eb696c5503`; tag `v0.8.0` → `PROTO_SHA`.
- arbiter-core: PR sentioxyz/arbiter-core#41 merged; `CORE_SHA = 755f0946497ed743c9e7372a09bc35ffc76579b7`; tag `v0.10.0` → `CORE_SHA`. It still pins housegate `db46c31` (`v0.14.2-0.20260923160348-db46c31d5501`).
- arbiter: PR sentioxyz/arbiter#121 merged; `ARB_SHA = 10ab91759f23054e5b25adfd4c70261c69fae795` (not tagged).
- Additions to the names above that review added before `v0.10.0` (all additive; plan B's names are unchanged):
  - `tableset.Deps.Dropped` — the SNode wires it internally to forget a dropped table's promotion ledger and intake parts before `SubmitTablePurged`, so a same-name recreation (D9) promotes from an empty base. Hosts do nothing.
  - Before creating a chain incarnation's `hg_unsafe`, every node checks its Keeper path for replicas outside `PurgeNodeSet()` ∪ {self}; the source SNode removes them, other nodes back off until it has.
  - `dataplane.RegistrySchema(networkID, genesis, snap, tableID)` is the one schema rule the SNode and verifier share.
  - `verifier.New` refuses a `Deps.Registry` together with a `*CHScanner` that does not follow the same view (hosts embedding a verifier build the scanner with `NewRegistryScanner(cfg, conn, sameFollower)`).
- Carried to plan B's handoff to sub-project 5: (1) upgrade every arbiter voter before any data-plane node (§12 order) — a follower whose first peer answers `Unimplemented` stays "disabled"; (2) an evicted source SNode rejoins only with a wiped local state directory — its leftover-drop path is otherwise fail-closed (intake refusal on a recreated key); (3) follow-up: the data-plane client uses gRPC's default 4 MB receive limit while the whole registry snapshot grows monotonically (Purged incarnations are kept), so a large registry stops every data-plane role at once.
