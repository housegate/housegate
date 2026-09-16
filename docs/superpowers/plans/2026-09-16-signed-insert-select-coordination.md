# Signed INSERT ... SELECT Coordination Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sequence one fenced snapshot query, recover it durably, and publish either its verified append or an explicit aborted no-op without releasing stale work.

**Architecture:** The Arbiter persists active profile policy, a draining/granted/consumed reservation and its generation in Raft state. Housegate's query intake journals signed input before sequencing and runs source work only after acceptance. Source preparation, claim, promotion and cleanup carry the same operation identity and generation, while snapshot retention outlives the scheduling barrier.

**Tech Stack:** Go, Raft command protobufs, durable FSM snapshots/journals, ClickHouse source staging, Bazel.

**Spec:** [Signed INSERT ... SELECT design](../specs/2026-09-16-signed-insert-select-design.md), D1 and D7–D10; [index](2026-09-16-signed-insert-select.md), [contracts](2026-09-16-signed-insert-select-contracts.md), [replay](2026-09-16-signed-insert-select-replay.md).

## Global Constraints

- All [index constraints](2026-09-16-signed-insert-select.md#global-constraints) apply; new runtime APIs in this plan are proposed.
- Drain to published safe or committed abort, not ACK2. Snapshot, schema and active profile policy cannot overtake a reservation.
- Submit atomically consumes the reservation into a singleton block before source execution; unknown Submit outcomes are reconciled by exact identity.
- An installed historical profile is replay-only. Admission requires both signed IDs to equal both the reservation pair and active policy pair.
- An accepted block cannot disappear on client disconnect or a local timer. Only an authorized committed abort can release it without an applied publication.
- Abort must fence claims/promotion and settle or quarantine exact candidate ownership before releasing the barrier. It preserves predecessor data and consumes the operation identity.
- Wall clocks, rewriter calls, artifact fetches and source RPCs are outside deterministic FSM application.

---

## File map

| Owner | Create | Modify |
|---|---|---|
| HG profile protocol | `pkg/replay/profile_transition.go`, `profile_transition_test.go` | `pkg/replay/BUILD.bazel`; A1 wire records as required for the new contracts |
| AP control wire | New control/profile/claim messages in existing proto files | `proto/arbiter.proto`, `proto/replay.proto`, `proto/raftlog.proto`, matching generated Go files |
| AC control wire | `wire/snapshot_query_control.go`, `snapshot_query_control_test.go` | `wire/command.go`, `wire/BUILD.bazel`, `conformance/snapshot_query_wire_test.go` |
| AR policy/FSM | `fsm/profile_transition.go`, `profile_transition_test.go`, `barrier.go`, `barrier_test.go`, `reservation.go`, `reservation_test.go`, `admission_v3.go`, `admission_v3_test.go`, `abort.go`, `abort_test.go`, `snapshot_migrate.go`, `snapshot_migrate_test.go` | `fsm/state.go`, `fsm/apply.go`, `fsm/admission.go`, `fsm/snapshot.go`, `fsm/reads.go`, `fsm/BUILD.bazel` |
| AR service | `server/reservations.go`, `reservations_test.go`, `snapshot_queries.go`, `snapshot_queries_test.go` | `server/ingress.go`, `claims.go`, `safestate.go`, `server.go`, `BUILD.bazel` |
| HG query intake | `pkg/storageintegrity/query_intake.go`, `query_intake_test.go`, `query_journal.go`, `query_journal_test.go`, `query_recovery.go`, `query_recovery_test.go`, `query_arbiter_proto.go`, `query_arbiter_proto_test.go` | `pkg/storageintegrity/BUILD.bazel` |
| HG control auth | `pkg/auth/snapshot_query_control.go`, `snapshot_query_control_test.go` | `pkg/auth/BUILD.bazel` |
| AC source | `snode/query_staged.go`, `query_staged_test.go`, `query_journal.go`, `query_journal_test.go`, `query_converge.go`, `query_converge_test.go` | `snode/snode.go`, `snode/config.go`, `snode/cleanup.go`, `snode/promote.go`, `snode/BUILD.bazel`; verifier dispatch from B5 |

### Task C1: Publish explicit executor transitions and persist active profile pairs

**Files:** HG profile protocol, AP/AC control wire, AR policy files. Old `prevSafeSnapshot` equality checks remain intact.

**Interfaces:** HG defines the canonical control records below and `ValidateExecutorProfileTransition(prev SafeSnapshotManifest, next SafeSnapshotManifest, transition ExecutorProfileTransition) error`. Fields have the listed JSON order. `ActiveQueryPolicy` is historical consensus state as well as current admission state.

| Type | Fields |
|---|---|
| `ActiveQueryPolicy` | `activation_id string`, `network_id string`, `keeper_shard_id uint32`, `activation_block_seq uint64`, `executor_profile_id string`, `query_profile_id string`, `enabled bool` |
| `ExecutorProfileTransition` | `network_id string`, `keeper_shard_id uint32`, `prev_snapshot_id string`, `prev_state_root string`, `old_executor_profile_id string`, `new_executor_profile_id string`, `schema_snapshot_id string`, `schema_root string`, `data_root string`, `next_snapshot_id string`, `next_state_root string`, `next_manifest_root string`, `activation ActiveQueryPolicy` |
| `ExecutorProfileTransitionReceipt` | `transition_root string`, `replica_id string`, `signature string` |

Hash the transition with `CanonicalDigest("executor-profile-transition-v1", transition)`. AR stores approved compatible profile-pair records, the active policy, policy history indexed by activation ID/block and the transition's receipts. `GetPublishedSnapshot` and B5's historical-policy port must retrieve authenticated committed records plus the relevant manifest; neither authorizes new admissions using the installed profile catalog. Add a separate `publish_executor_profile_transition` Raft command at new tag `26` after A1's reserved tags, subject to the same collision recheck.

Also freeze `SnapshotArtifactReady{SnapshotID string, ManifestRoot string, SchemaRoot string, ArtifactSetRoot string, PublisherID string, RetentionPolicyID string}` with snake-case fields in that order. `artifact_set_root` commits a sorted complete list of `{table_id, partition_id, part_name, part_phys_hash, object_digest, bytes}` plus the schema artifact digest. Hash the readiness record under `snapshot-query-artifact-ready-v1`; the existing authenticated publisher role signs that hash. Add `RecordSnapshotArtifactReady` plus Raft command `record_snapshot_artifact_ready = 27`, which stores only a signature-checked readiness record for that exact manifest. B1 implements its publisher. AR requires the record before a snapshot becomes eligible for snapshot queries, including snapshots produced by v2 writers after activation; a manifest RPC response alone never implies artifact availability.

- [ ] **Step 1: Add transition vectors and fail-before-change tests.** Produce a predecessor with at least two nonempty tables and prove unchanged schema/data/table/part ledger, changed executor identity/state root, sealed child manifest and exact activated pair. Mutate old/new IDs, schema, data root, omitted table, parent, target manifest or unsupported compatibility pair and require rejection.

```text
old manifest executor=E1, data=D, schema=H
approved transition E1 -> E2(composite): next.data=D, next.schema=H, next.executor=E2
next.parent=old.snapshot_id; next.safe_block_seq advances at a control-plane boundary
next.state_root=existing safe-snapshot-state formula(H,E2,D)
publish transition -> active pair=(E2,Q1); first v3 job chains from next
history job (E1,old payload profile) still dispatches to E1
change query policy Q1 -> Q2 after drain: new admissions use Q2; old Q1 history still replays
```

- [ ] **Step 2: Run HG `bazel test //pkg/replay:replay_test --test_filter=TestExecutorProfileTransition`, AR `bazel test //fsm:fsm_test`, AC `bazel test //wire:wire_test //conformance:conformance_test`.** Require transition APIs/records or preserved-ledger assertions to fail initially.

- [ ] **Step 3: Implement transition verification and disabled policy state.** The control-plane operator proposes a compatible pair under the existing authenticated administrative authority. Require no active reservation, unsettled prior work or profile/schema transition. Copy the entire old ledger and call the existing manifest sealing/root functions; independently verify all unchanged fields. A profile change is not a fabricated INSERT and does not edit the old manifest. Publish only after required transition receipts and artifact readiness validate.

```text
activate(pair):
  require feature capability, approved compatibility record and fully drained boundary
  if executor changes: require verified executor-profile-transition child publication
  if only query profile changes: commit a new activation record at the drained block boundary
  record exact pair and activation_id in active policy and immutable history
  do not remove historical profile executables or rewrite old policy records
```

For genesis, derive the authenticated empty manifest under the composite executor and configured complete schema set; create its explicit published genesis record before granting a reservation. Do not equate missing tables or an omitted pin with genesis. Preserve the v2 empty-prev spelling and historical behavior.

- [ ] **Step 4: Add/verify snapshot migration.** Current AR snapshots use version `2` and reject other versions. Add a new explicit reader/version with `ActiveQueryPolicy`, policy history, pending transition and reservation state; migrate v2 to `enabled=false`, empty history/reservations, unchanged legacy fields. Unknown versions fail clearly. Load a v2 fixture, round-trip a new snapshot with historical Q1/current Q2, and prove no new admission can use Q1 after restart. An old binary must refuse the newer snapshot/history, not silently drop it.

- [ ] **Step 5: Commit `feat(arbiter): activate snapshot query profiles at safe boundaries`.** Publish the HG/AP/AC contract/pin commits before the AR consumer. Keep the default policy disabled.

### Task C2: Acquire a fenced reservation after a complete network/shard drain

**Files:** AR barrier/reservation/service files, HG control auth, AP/AC control records. Extend AR `fsm/admission.go` to respect the barrier for existing v2 writes as well.

**Interfaces:** Add RPCs `AcquireSnapshotQuery`, `GetSnapshotQueryReservation`, and `ReleaseSnapshotQuery` under `ArbiterIngress`. The request contains `network_id`, `keeper_shard_id`, `client_account`, `statement_id`, `request_id`, and `control_jws`; acquire returns A1's `SnapshotQueryReservation` only after grant. Lookup/release return A1's separate `SnapshotQueryReservationStatus` and additionally bind known `reservation_id` and `fencing_generation`. A `SnapshotQueryControlBinding` in HG auth has ordered fields `operation string`, `network_id string`, `keeper_shard_id uint32`, `client_account string`, `statement_id string`, `request_id string`, `reservation_id string`, `fencing_generation uint64`; `operation` is `acquire`, `lookup` or `release`. Sign it with purpose `housegate-snapshot-query-control-v1`, issuance time and the existing canonical ES256K primitives. Empty reservation ID/generation are allowed for acquire/lookup-by-request-ID and the conditional absent-request cancellation below; a draining release has an empty reservation ID but its exact nonzero generation. Replayable identical control requests return the same state, never grant another reservation.

The proposed authenticated status is keyed by `(network_id, keeper_shard_id, client_account, statement_id, request_id)`, including after the active barrier returns idle. `version=1`, `found=true` identifies one of `draining` (no reservation yet), `granted` (complete reservation), `consumed` (complete reservation and assigned block; includes resolving until authoritative terminal publication), or `released` (durable cancellation/terminal tombstone and nonempty terminal proof). `fencing_generation` is the current request fence and is nonzero for found records. `found=false` has empty state, no reservation/block/proof, and echoes the authenticated request identity; it is not inferred from a transport failure. Released records retain whether a block was consumed via `block_seq` and the verified terminal proof. A late acquire/lookup for a released request returns that terminal status/refusal, never allocates another grant.

Pre-submit cancellation first looks up the persisted request identity. Release of draining/granted work atomically checks the exact request/fence and that it is unconsumed, commits a higher fence and a durable released tombstone, then returns authenticated proof. An absent-request release uses empty reservation ID and generation zero and commits a cancellation tombstone only if the request is still absent when applied; if begin/grant won the race it returns a state conflict for lookup and exact-generation release, and if Submit won it refuses release. This conditional tombstone prevents a delayed in-flight acquire from appearing after a negative lookup. If an unrelated request owns the active barrier, return bounded busy and retain caller cancellation debt rather than clearing or refencing that barrier; retry lookup/conditional cancellation later. Only the matching active request may transition its barrier to idle. No client boolean/context timeout can authorize release of consumed work; C3 alone resolves it. Persist tombstones in C1's new snapshot version and migrate old snapshots with an empty request-history map.

AR keeps `SnapshotBarrier{State string, Generation uint64, RequestID string, ClientAccount string, StatementID string, Reservation *replay.SnapshotQueryReservation, BlockSeq uint64}` where states are `idle`, `draining`, `granted`, `consumed`, `resolving`, plus durable request-status/tombstone history. The generation increments on begin/grant/release/fencing and refuses overflow; a draining request already has a fence. Profiles in acquire are chosen by committed policy; there is no client-selectable profile field.

- [ ] **Step 1: Write drain and authorization tests.** Sequence a v2 write, register its claim and reach ACK2 without safe publication; acquire must remain draining. Attempt a new v2 write, a second query, schema change, profile change and unrelated safe publication during the reservation and require refusal/defer. Prior in-flight publications needed to finish the drain must still be able to complete.

```text
prior W is ACK2 but not safe -> begin acquire persists draining -> no pin granted
publish W safe -> grant exact current S and active pair -> reservation generation G
unknown/expired control JWS or different account -> refuse before barrier acquisition
grant response lost -> lookup(request_id) returns the same reservation, not a new one
release at generation G-1 -> refuse; release G before submit -> committed fenced idle
restart while draining/granted -> restore barrier before processing another write
Cancel/EOF during drain -> lookup request -> release exact draining generation -> tombstone
acquire response lost after grant -> lookup complete grant -> release only if unconsumed
negative lookup races late begin -> conditional absent release fences request or returns conflict
release response lost -> lookup released tombstone -> no reacquire and no duplicate release effect
release races Submit -> either release fences Submit or consumed status prohibits release
absent cancellation during another request barrier -> busy; unrelated owner/fence unchanged
restart with cancellation tombstone -> late begin/grant cannot recreate canceled work
```

Use a server-supplied timer only to propose a release/expiry log entry; vary every FSM replica's wall clock while replaying identical commands and assert byte-identical state. Keep one pending reservation per network/shard; another request returns a bounded busy response instead of joining an unbounded queue. An identical request ID performs a status lookup. Unauthorized callers cannot reserve the global barrier.

- [ ] **Step 2: Run `bazel test //fsm:fsm_test //server:server_test` in AR and `bazel test //pkg/auth:auth_test` in HG.** Expected failures: acquire has no drain/fence implementation or an ACK2-only write incorrectly permits grant.

- [ ] **Step 3: Implement begin/grant/release as committed transitions.** Authenticate the control request and admission account before proposing begin. FSM begin fences **new** SI admissions and schema/profile changes while previously admitted work is drained to safe/committed abort. The leader observes committed progress and proposes grant with the selected published pin; FSM independently checks that the predecessor, drain counters and policy still match. It performs no remote I/O or SQL analysis.

```text
begin: idle -> draining; increment fence; persist requester and idempotency key; reject tombstone
grant: require draining and all prior work durably resolved; select published S and active pair
       draining -> granted; persist reservation ID/generation/account/statement/pin/activation
release: require matching unconsumed draining/granted identity and exact generation
         or atomically prove still absent for conditional request-identity cancellation
         commit new fence and released tombstone; only its matching barrier becomes idle
         absent cancellation while an unrelated barrier is active returns busy; never release consumed work
         no local timer mutates FSM state; return authenticated release/status proof
```

Add snapshot readiness to the grant check: required input schemas/parts must have durable publication records before S is eligible. Retain the reservation's snapshot reference before returning the grant; an indeterminate retain is reconciled idempotently and cannot silently free the pin. No safe publication may overtake the **granted/consumed** reservation except its own terminal outcome.

- [ ] **Step 4: Verify complete admission interception and restart.** Test every existing SI admission and schema/profile mutation entry, plus publication paths, against the barrier. Verify read-only traffic remains serviceable and an in-flight prior write can finish the drain. Run new-version FSM snapshot/restore and leadership-change tests under concurrent acquire/release/submit.

- [ ] **Step 5: Commit `feat(arbiter): reserve fenced safe snapshots for queries`.** Document the read-only reservation/status API and its exact authentication requirement for D1.

### Task C3: Atomically sequence, verify or abort a singleton query block

**Files:** AR `admission_v3.go`, `abort.go`, query service/claims/publication paths; AP/AC query commands and receipts; C1 snapshot migrations.

**Interfaces:** `SubmitSnapshotQuery(SnapshotQueryEnvelope) returns (SnapshotQuerySubmitResult)` and `GetSnapshotQueryStatus` use A1's exact result/status records. Status is keyed by account/statement ID and expected input root; a reused statement ID with a different root/JWS rejects. `SnapshotQueryAbortRecord` has ordered fields `block_seq uint64`, `statement_id string`, `input_root string`, `reservation_id string`, `fencing_generation uint64`, `reason_code string`, `cleanup_authorization_root string`, `prev_snapshot_id string`, `next_snapshot_id string`. A1's new receipt includes `abort_record_root string` immediately after `execution_outcome`; applied receipts use `""`, aborted receipts bind the committed abort record. Hash the abort record under `snapshot-query-abort-v1` and freeze a byte vector. Old receipt domains are unchanged.

- [ ] **Step 1: Write equality, idempotency, singleton and abort tests.** A validly signed weaker/historical query profile with the current executor must be refused even if installed. Test every pin/schema/account/statement/generation/profile mismatch and unknown variant. A valid repeated submission returns its existing block; a changed root/JWS under the same identity refuses. No v2 entry shares the new singleton block.

```go
// applySubmitSnapshotQuery uses values already decoded and signature-checked.
b := envelope.Input.Binding
r := reservation
if b.ExecutorProfileID != r.ExecutorProfileID || b.QueryProfileID != r.QueryProfileID ||
    b.ExecutorProfileID != active.ExecutorProfileID || b.QueryProfileID != active.QueryProfileID {
    return Rejected{Reason: "snapshot query profile pair is not active for this reservation"}
}
```

Add crash before/after assignment and block seal, stale claim/promotion after abort, abort with outstanding unknown source write, zero-result applied publication, and next-block progress after terminal abort.

- [ ] **Step 2: Run AR `bazel test //fsm:fsm_test //server:server_test` and AC `bazel test //wire:wire_test //conformance:conformance_test`.** The historical-profile submission and late-generation claim tests must fail before their guards exist.

- [ ] **Step 3: Implement deterministic validation and atomic singleton acceptance.** Check A1 canonical input/read roots, A2 pure JWS verification, exact reservation/published pin/active pair, schema and spent identity. Atomically assign statement sequence, select source, consume reservation and seal one new query block with its new statement root. Persist full signed input/descriptor and original JWS in durable L3 storage before returning accepted status; query input has no payload DA pin. Do not call the rewriter or source while applying the command.

```text
granted -> atomic Submit -> consumed(block_seq, statement_seq, input_root, source)
source claim + honest replay + partition delta + exact candidate scan -> resolving(applied)
applied publication durably confirmed + all prior claims/promotions resolved -> fenced idle

accepted execution cannot finish -> authorized abort -> resolving(aborted)
fence old claims/promotions; settle or quarantine exact candidates with durable cleanup debt
publish child manifest copying predecessor data/schema/executor and whole part ledger
commit aborted outcome and abort_record_root; consume identity; fenced idle
```

The applied/aborted terminal child must match the reserved predecessor and assigned block; unrelated calls to `applyPublishSafeSnapshot` cannot bypass this check. A zero-row applied query advances the manifest with an empty-output hash and no candidate parts; an abort has `execution_outcome=aborted`, no claimed executed output, `output_row_count=0`, `output_rows_root=""`, and a nonempty abort root. No execution error is converted to an applied match receipt. Abort authority follows the existing authenticated control/challenge path and its committed evidence, not a source's unsigned local timeout.

- [ ] **Step 4: Verify reconciliation, safe publication and migration.** Lose Submit's response, replay the Raft log and recover the same identity/block. Abort during unknown prepare must retain ownership until the source settles or is durably quarantined/fenced. After release, every old generation is refused by claim/scan/promotion paths. Persist remaining cleanup and retention debt across restart; the next block may proceed only after authoritative fencing/publication, while challenge artifacts remain retained. Run existing three-way, promotion, cleanup, seal and snapshot suites as regressions.

- [ ] **Step 5: Commit `feat(arbiter): sequence and resolve snapshot query blocks`.** Include AP/AC wire fixtures, new-version restart evidence and the old-v2 compatibility result.

### Task C4: Journal a sequence-first Housegate query intake

**Files:** HG query intake/journal/recovery/proto row. Keep `StatementEnvelope` comparable and leave `prepareAndSubmit` and the existing payload journal format unchanged.

**Interfaces:** Add `SnapshotQueryIntake.Submit(ctx context.Context, env replay.SnapshotQueryEnvelope, gate SnapshotQuerySubmitGate) (SnapshotQueryIntakeResult, error)` and `Recover(ctx context.Context) error`. The proposed `SnapshotQuerySubmitGate` interface has `TryStart() bool`; D2 supplies a one-shot relay generation/cancellation gate. Intake invokes it after durable submit intent and immediately before the first sequencer attempt, not merely before a journal write. A false result means no Submit call and C2 cancellation reconciliation; a true result transfers ambiguous/accepted outcome ownership to the durable intake. The gate performs no I/O and grants no release authority. Recovery uses persisted intent and authenticated sequencer status, never resurrects a canceled client gate. `SnapshotQueryIntakeResult` contains `StatementID string`, `InputRoot string`, `BlockSeq uint64`, `AckLevel string`, `ExecutionOutcome string`, `OutputRowsRoot string`. Add a separate `SnapshotQueryJournal` with `Load(ctx, statementID) (SnapshotQueryJournalRecord, bool, error)`, `List(ctx) ([]SnapshotQueryJournalRecord, error)`, `Save(ctx, SnapshotQueryJournalRecord) error`; the concrete file journal uses an independently versioned directory/record, not an untagged v2 payload record.

The record contains version, full envelope/reservation, immutable source-frontier ordinal, stage, assigned block/statement/source identity, submit/prepare/claim unknown flags, pre-submit cancel intent and release reconciliation debt, durable output root/count/cache reference, touched partitions, capacity reservation ID, exact candidates, observed candidate identities, terminal outcome, cleanup debt and retention reference IDs. Identity comparisons use canonical input root plus original JWS, not Go struct `==` on slices. The proposed pre-submit cancellation stages are `CancelPending` and `Released`; accepted operations never use these to bypass C3 terminal resolution. The remaining proposed stages are `Signed`, `SubmitUnknown`, `Sequenced`, `Executing`, `OutputDurable`, `CapacityReserved`, `PrepareUnknown`, `Prepared`, `ClaimUnknown`, `Claimed`, `Resolving`, `Applied`, `Aborted`.

Ports use A1/C3 wire records plus C5's `SnapshotQueryPrepareRequest` and `SnapshotQueryPrepared`: `QuerySequencer.SubmitSnapshotQuery(ctx, env) (replay.SnapshotQuerySubmitResult, error)`, `LookupSnapshotQuery(ctx, statementID, inputRoot) (replay.SnapshotQueryStatus, error)`, `QuerySource.PrepareSnapshotQuery(ctx, req) (SnapshotQueryPrepared, error)`, `LookupSnapshotQueryPreparation(ctx, statementID, inputRoot) (SnapshotQueryPrepared, bool, error)`, and `RegisterSnapshotQueryClaim(ctx, statementID, inputRoot) error`. Define the C3 result/status records in HG `pkg/replay/snapshot_query_types.go` and AP together; status carries `Found`, accepted submit result, lifecycle and terminal outcome rather than inferring absence from an empty result.

- [ ] **Step 1: Add an ordered event-log test and crash table.** Use fakes that append port calls to a slice and a journal that fails at a selected fsync boundary. Require the following order and forbidden calls:

```text
persist_signed -> persist_submit_intent -> TryStart -> submit -> persist_sequenced -> source_prepare -> persist_prepared -> claim
cancel during submit-intent fsync -> TryStart=false -> submit count=0; reconcile C2 release
TryStart wins before cancel -> SubmitUnknown retained until authoritative lookup
submit rejected: source_prepare count=0
submit response lost: lookup_submit before any source_prepare
prepare response lost: lookup_prepare before any repeated source_prepare
client EOF after accepted Submit: background recovery still owns the same id/pin
second call with changed SQL/read root/JWS: reject before side effects
```

The source port in C5 encapsulates execute/output/capacity/unsafe stages and reports their durable state for the HG journal. HG must not perform a second independent SELECT or invent touched partitions. Test process restart at every journal stage and source-frontier ordering against prior payload work.

- [ ] **Step 2: Run `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter=TestSnapshotQuery`.** Expect missing query intake or source preparation before sequencing on the old path.

- [ ] **Step 3: Implement the new lifecycle and exact reconciliation.** Persist signed input before Submit and accepted identity before source work. Unknown remote outcomes remain owned; `NotFound` must be authoritative at the requested identity/generation before retry. Use a service-owned recovery context after acceptance, with bounded attempt contexts, instead of canceling accepted work with the client context.

```text
before Submit: cancel may release only after lookup proves reservation unconsumed
after Submit: cancel stops client delivery; durable reconciler continues or requests committed abort
retry: match full input root + original JWS + source/block/fence; never re-sign/rebase
ACK2: emit only after accepted input and durable registered source claim
safe ACK: wait for applied safe publication; abort produces query failure at every ACK level
terminal journal compaction: retain input identity, outcome and cleanup/retention ownership
```

Use atomic file replacement and directory fsync consistent with the existing journal. Surface local resource/back-pressure refusal without an applied receipt; retain accepted sequencing and pin until AR resolves it. Do not return an ordinary upstream-execution fallback on a missing port or unsupported variant.

- [ ] **Step 4: Run journal, source-frontier, idempotency and cancellation tests.** `bazel test //pkg/storageintegrity:storageintegrity_test`. Verify no double write, no early capacity release and no lost retention across all unknown-outcome cases. Existing payload staged/deferred tests must pass without adopting new query-only assumptions.

- [ ] **Step 5: Commit `feat(intake): recover sequenced snapshot queries`.** Expose only injected ports; D2 installs them in the plugin chain after capability checks.

### Task C5: Stage derived rows and fence exact source candidates

**Files:** AC source row, HG query intake ports, AC verifier integration from B5, AR claim/scan/promotion validation from C3.

**Interfaces:** In HG storageintegrity define `SnapshotQueryPrepareRequest{Envelope replay.SnapshotQueryEnvelope, Accepted replay.SnapshotQuerySubmitResult}` and `SnapshotQueryPrepared{StatementID string, InputRoot string, BlockSeq uint64, FencingGeneration uint64, OutputRowsRoot string, OutputRowCount uint64, TouchedPartitionIDs []string, CapacityReservationID string, Candidates []replay.SnapshotReadPart, SourceClaimRoot string, Stage string}`. Use the new complete candidate projection, not existing `CandidatePart`, which lacks a physical hash. AC `snode.Role` exposes the query methods through its existing embedding adapter pattern. Keep old `RCRecord` and payload candidate types untouched.

HG replay/AP freeze `SnapshotQueryClaim` with ordered fields `source_node string`, `statement_id string`, `statement_seq uint64`, `block_seq uint64`, `input_root string`, `reservation_id string`, `fencing_generation uint64`, `execution_outcome string`, `output_row_count uint64`, `output_rows_root string`, `computed_state_root string`, `partition_deltas []PartitionCommitment`, `partition_commitments_after []PartitionCommitment`, `candidate_parts []SnapshotReadPart`. Here each `partition_deltas.root` is the sum of this operation's new-part LtHashes; `partition_commitments_after` contains the resulting full partition commitments. Sort arrays by structured table/partition/part identity, reject duplicates, serialize empty arrays as `[]`, and exclude fetch hints. `SnapshotQueryClaim.Hash()` uses `CanonicalDigest("snapshot-query-claim-v1", claim)`; it contains no claim-root field of its own. The source submits the claim through authenticated source-role transport, and AR binds its sender to the assigned source before committing it. A1's verifier job carries this full committed claim and its digest; B5 compares replay state/output against the claim's corresponding fields, not against the compound claim digest.

- [ ] **Step 1: Add source-stage order and fraud tests.** Inject source executor, journal, pressure and ClickHouse ports. Require no unsafe write before sequencer acceptance and exact partition capacity. Tests compare actual named candidate bytes through the scanner, not just fake output hashes:

```text
accepted input -> execute at S -> persist output root/count/cache -> derive exact touched partitions
reserve exact partitions atomically -> persist capacity -> mark prepare unknown -> write unsafe rows
discover and scan exact candidates -> persist prepared claim -> register fenced claim

zero output: no pressure reservation, no parts, durable empty applied claim
wrong output cache/root, extra partition, mismatched source/fence: refuse before claim
capacity unavailable: no unsafe write; retain accepted operation for retry/abort
write response lost: lookup journal and scan row IDs; never blind reinsert
```

- [ ] **Step 2: Run AC `bazel test //snode:snode_test //verifier:verifier_test` and HG intake tests.** Expected failures: missing query source methods or incorrect write/pressure/recovery order. The real candidate scan scenarios belong to the same branch's ClickHouse integration tests.

- [ ] **Step 3: Implement a separate query journal and reuse exact candidate machinery.** Call B4's `Executor.Prepare` once for the source and own its `PreparedQueryExecution`. Read the exact globally ordered rows from `Output.OpenRows()`; copy/fsync a durable cache bound to input root, pin, output count/root and global IDs, then persist that binding before `Close` or any unsafe work. Reopen the durable cache for staging and derive touched partitions via the shared authority. Do not call Replay and then repeat the SELECT to recover discarded rows. Reserve capacity before **any** unsafe table write/candidate creation/claim. Scratch restore/evaluation has its own limits and semaphore, not the payload's known-byte budget.

Reuse existing `staged.go`, `converge.go`, `cleanup.go` and promotion helpers only below their payload validation boundary: factor exact inventory, row-ID attribution, candidate scan and cleanup functions rather than feeding empty payload to `PrepareLocalStatement`. Durable source records include input/pin/fence, accepted identity, expected global IDs/rows, output root/cache reference, pre-write inventory, candidate observation state and pressure ownership. Recompute scratch output after a crash only from the original input/S and require the already recorded output commitment to match.

```text
prepare unknown -> authoritative lookup -> scan attributed exact row IDs and candidate names
complete expected write -> reuse prepared result; partial write -> durable AbortPending cleanup
cleanup removes/quarantines only attributed candidates; retain delayed/unobserved ownership
late claim/promotion with old fence -> refuse even if local journal says Prepared
release capacity only after applied/aborted terminal authority plus reconciled exact ownership
```

Wire replay, source-root/delta and exact byte-scan evidence to C3's promotion gate. Bind candidate scan responses to the selected candidate identities and current generation; a correct logical root does not authorize swapped physical parts. The source cannot self-authorize abort release or snapshot garbage collection.

- [ ] **Step 4: Run crash/restart, adversarial source and cleanup tests.** AC unit targets plus the repository's ClickHouse integration command for `//dataplane/ddl:ddl_test //snode:snode_test //verifier:verifier_test`, with its documented `ARBITER_CH_INTEGRATION`, `ARBITER_CH_KEEPER`, `ARBITER_CH_REPLICA`, `CH_ADDR` and `CH_REPLICA_ADDR` test environment. Verify crashes around prepare/claim/cleanup and delayed part visibility, source/verifier disagreement, and next-block liveness after a committed no-op abort. D4 automates these services and supplies non-skipping evidence.

Include Prepare/output-handle ownership, cache-copy fsync failure and crash between durable cache and handle Close. Require no second SELECT on the normal staging path, no unsafe write without the persisted exact output binding, and identical reopened rows/global IDs after restart. Verifier replay always evaluates independently and never consumes this source cache.

- [ ] **Step 5: Commit `feat(source): stage and recover snapshot query results`.** The task is complete only with AR late-fence and three-way promotion tests passing against the same HG/AP/AC pins.
