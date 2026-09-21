# Signed INSERT ... SELECT — Wave 1a-2b: Authorized Abort and the Aborted No-op Child (C3, part 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give a sequenced snapshot query its first terminal: an authority-signed, admin-proposed `AbortSnapshotQuery` (Raft tag 22) that, in one committed transition, marks the singleton query block safe, publishes the aborted no-op child manifest (predecessor data, schema, executor and part ledger unchanged), releases the consumed reservation into a tombstone carrying the abort as terminal proof, returns the barrier to idle at the next fencing generation, and records the admission as `terminal` / `aborted` so `GetSnapshotQueryStatus` answers with the authenticated abort — all behind the default-off dependencies Waves 1a-1 and 1a-2 introduced.

**Architecture:** The abort is an authority-signed command, never a timer or a client cancellation: arbiter-core gains the purpose `housegate-snapshot-query-abort-v1` (its own command domain over the frozen `SnapshotQueryAbortRecord`), Apply verifies it clock-free against the committed `Params.AuthorityAddresses`, and a new `ConsensusAdmin` RPC authorizes freshness before proposing. The proposer chooses nothing: the FSM derives the only abort it would accept (`GetSnapshotQueryAbortCandidate` — block, statement, input root, reservation, generation, `prev_snapshot_id` = the current watermark, `next_snapshot_id` = the sealed no-op child), the authority fills `reason_code`, signs and submits, and Apply refuses any other record. The no-op child is the ordinary `SafeSnapshotManifest` the v2 lane would publish once the query statement is `StatusSafe`, sealed inside apply through the same `validateManifestPublication` path as `PublishSafeSnapshot`, so `SafeWatermark` advances past the query block and the next v2 block chains onto the child. Snapshot format moves to v12 for the terminal fields.

**Tech Stack:** Go 1.26, Bazel 9.1.0, hashicorp/raft, arbiter-core `authority` + `wire`, arbiter-proto (`buf generate`), housegate `pkg/replay` (`SnapshotQueryAbortRecord.Hash`, `SafeSnapshotManifest.Seal`, `CanonicalDigest`).

**Spec:** [design D9](../specs/2026-09-16-signed-insert-select-design.md) (lines 35, 45, 238, 240: the aborted no-op outcome, "Only an Arbiter-authorized terminal abort can release the barrier", "Barrier release requires durable publication of the applied or aborted outcome"); [coordination plan Task C3](2026-09-16-signed-insert-select-coordination.md) (the state diagram, line 230 "the applied/aborted terminal child must match the reserved predecessor and assigned block", line 213 "Authenticate admin abort commands under distinct new-lane authority JWS purpose/domain", line 195 the frozen abort record); [Wave 1a-2 plan](2026-09-21-signed-insert-select-wave1a-2-c3-sequencing.md) and its execution amendments (the C3 part-1 code this builds on); [master plan Global Constraints](2026-09-16-signed-insert-select.md).

## Global Constraints

- Tier 3 of [issue #153](https://github.com/housegate/housegate/issues/153): merging enables no runtime capability; `cmd/arbiter` sets neither `server.Deps.SnapshotQueryReservations` nor its `Statements` validator; both new RPCs refuse (`FailedPrecondition`) without them, before touching Raft.
- The abort is authority-authenticated: a new arbiter-core purpose `housegate-snapshot-query-abort-v1` (constant `authority.SnapshotQueryAbortPurpose`) with its own command domain `arbiter-snapshot-query-abort-command-v1`; a promotion, cleanup, consensus-update, disposition or control token cannot authorize it and it authorizes nothing else; Apply and snapshot restore verify with the clock-free `VerifySnapshotQueryAbort` against the committed `Params.AuthorityAddresses` (fail-closed on an empty set); only the server's `AuthorizeSnapshotQueryAbort` reads the clock (`ConsensusAdminMaxTokenAge`). The abort is never inferred from client cancellation, a source's local timeout or a leader timer (raftlog.proto: "never inferred from client cancellation").
- The frozen record `replay.SnapshotQueryAbortRecord{BlockSeq, StatementID, InputRoot, ReservationID, FencingGeneration, ReasonCode, CleanupAuthorizationRoot, PrevSnapshotID, NextSnapshotID}` and its `snapshot-query-abort-v1` digest (`Record.Hash()`) are not changed; that digest is the receipt's `abort_record_root`. `cleanup_authorization_root` must be empty this wave (fail closed; no candidate exists because no source execution exists). `reason_code` is a required non-empty NUL-free string with no frozen vocabulary yet.
- One atomic transition: every check precedes every state change; on success the query statement is `StatusSafe`, `Manifests` gains the no-op child (its `DataRoot` and `StateRoot` equal the predecessor's), `SafeWatermark` = the child at the query block's sequence, the reservation record becomes a `released` tombstone at `FencingGeneration+1` with `TerminalProof` = the canonical JSON bytes of `wire.AbortSnapshotQuery{record, authority_jws}`, the barrier is `idle` at that generation, the admission is `Lifecycle = "terminal"`, `ExecutionOutcome = "aborted"` with the abort record; an identical replay is `Applied` with no state change; a different abort for the same statement is refused as terminal. No half-aborted state is representable.
- The proposer chooses nothing: `PrevSnapshotID`, `NextSnapshotID`, `BlockSeq`, `StatementID`, `InputRoot`, `ReservationID` and `FencingGeneration` must equal the replica-derived candidate; only `ReasonCode` is the authority's.
- Status vocabulary: `Lifecycle` ∈ {`sequenced`, `terminal`}; `ExecutionOutcome` ∈ {`""`, `aborted`} (`applied` is C5's); `TerminalProof` is opaque bytes to the client (the frozen fixture status has `lifecycle:"terminal"`, `execution_outcome:"aborted"`, opaque `terminal_proof`). This amends the 1a-2 plan's phrase "lifecycle vocabulary: `applied` and `aborted` (later)": those are execution outcomes under the `terminal` lifecycle.
- Frozen bytes: `L3BlockHeader` JSON unchanged; `StatementState` unchanged; `fsm/l3_commitment_golden_test.go` passes unchanged. Snapshot format: current v11; this plan bumps to v12 following the established recipe; a `< v12` container carrying a terminal admission is refused.
- Cross-repo pins move by commit, not by release: arbiter-core via `bash scripts/update-arbiter-core.sh <sha>` (go.mod pseudo-version + `MODULE.bazel` `bazel_dep` version + `git_override` commit), arbiter-proto via `go get github.com/sentioxyz/arbiter-proto@<sha> && go mod tidy && bazel mod tidy` (a `go_deps` module, no `git_override`); `scripts/update_dependency_test.go` must stay green.
- Tests: arbiter-core `bazel test //authority/... //conformance/...`; arbiter-proto `make proto && make lint && make test && go test ./conformance/...`; arbiter `bazel test //fsm:fsm_test //server:server_test --jobs=4` per task and `bazel test //... --jobs=4` (19 targets) before each PR; rejections asserted with `strings.Contains(got.(fsm.Rejected).Reason, …)`; successes with `reflect.DeepEqual`; every refusal asserted to leave byte-identical snapshots.
- Conventions: English comments; conventional commit subjects; commits end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; PR bodies end with `Refs https://github.com/housegate/housegate/issues/153`, `Design: https://github.com/housegate/housegate/pull/154` and `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. One isolated worktree per repository (`fix/153-wave1a-2b-abort`), never the main checkouts; merge only on green CI, squash with branch deletion; task review precedes every merge; single serial lane.

## Baseline facts (arbiter `origin/main` d01badf after PR #102; arbiter-core 30199f380bd9; arbiter-proto 1ac937bd4aaa; housegate 48ccc08a463b)

- `wire.Command.AbortSnapshotQuery *wire.AbortSnapshotQuery{Record replay.SnapshotQueryAbortRecord; AuthorityJWS string}` (arbiter-core `wire/snapshot_query.go:1192`) is encodable (tag 22, `command.go:105,214,276`) but has no `FSM.Apply` case (`fsm/fsm.go:60-119`; the last query arm is `SubmitSnapshotQuery` at `:113`). Nothing in `authority/` mentions abort; the purpose template to copy is `authority/consensus.go` (`ConsensusParamsUpdatePurpose`, own domain, `JWSCommandPayload{Iat, Purpose, CmdHash}`, `signPayload`, `verify(wantCmdHash, wantPurpose, token, enforceAge)`, `Verify*` clock-free / `Authorize*` age-enforced). `authority/BUILD.bazel` already depends on `@housegate//pkg/replay`. No abort RPC exists on any service; `ConsensusAdmin` (`consensus.proto:67-75`) holds `GetProtocolInfo`, `GetConsensusParams`, `UpdateConsensusParams(UpdateConsensusParamsCmd) returns (Ack)`; `raftlog.proto` imports `consensus.proto` and `replay.proto`, so the RPC request must be a new message in `consensus.proto` (importing `replay.proto`), not `AbortSnapshotQueryCmd`. arbiter-proto conformance: `conformance/snapshot_query_services_test.go:55 TestSnapshotQueryServicesRemainUnimplemented` (table of service/method/input/output + `Unimplemented*Server` stubs) and the descriptor baselines in `snapshot_query_compatibility_test.go`.
- Sequencing state after 1a-2: `SnapshotQueryAdmissionState{ClientAccount, StatementID, RequestID, ReservationKey, StatementSeq, BlockSeq (sealed block), SourceNode, InputRoot, OriginalJWSHash, StatementRoot, Lifecycle, Reservation, CommitIndex}` (`fsm/state.go:559-578`), `SnapshotQueryLifecycleSequenced = "sequenced"` (`:546`), `Result()` (`fsm/apply_snapshot_query_submit.go:194`), `SnapshotQueryStatusRead` (`:209`); the query statement is `StatusSequenced` and no path moves it; the reservation record and barrier are `consumed` (`BlockSeq` on both is the begin-time request identity, `fsm/apply_snapshot_query_reservation.go:228-248`); `applyReleaseReservation` refuses `consumed` (`fsm/artifact_reservation_release.go:58-60`) and otherwise builds the tombstone `{State: released, FencingGeneration: active+1, BlockSeq: release.BlockSeq, TerminalProof, TerminalIndex}` and returns the barrier to `idle` at that generation (`:73-95`); `released` is a tombstone state, never an active or barrier state (`fsm/artifact_reservation_state.go:28-44`); `putArtifactDispositionReservationBarrier` allows `consumed → idle` only with a bumped generation (`:269-289`).
- Safe publication: `safePrefixLocked` (`fsm/reads_work.go:42-66`) checks only `Status == StatusSafe` over `blockStatementsComplete`; the two `StatusSafe` writers are `applyRecordPromotionAck` (`fsm/apply.go:588-590`) and `applyResolveChallenge` (`:695-697`), both unreachable for a query statement and neither guarded by `IsSnapshotQuery()`; `applyPublishSafeSnapshot` (`fsm/apply.go:596-605`) = `validateManifestPublication` → `Manifests[id] = cloneManifest` → `SafeWatermark = {id, SafeBlockSeq, ManifestRoot}` → `emit(EventManifestPublished)`; `validateManifestPublication` (`fsm/manifest_validation.go:15-74`) requires `manifest.Validate()`, `SnapshotID == ManifestRoot`, schema/executor = `Params`, `SafeBlockSeq > watermark.SafeBlockSeq`, `ParentSnapshotID == watermark.SnapshotID`, `SafeBlockSeq == safePrefixLocked(f)`, schema identity, part-ledger bijection and `ManifestRoot == Seal(manifestInputsLocked(f))`; `manifestInputsLocked` (`fsm/reads_manifest.go:26-129`) starts from the parent manifest's tables and overlays `f.st.Partitions` / `f.st.SafeParts`, so with nothing promoted since the parent the tables are byte-equal; `replay.ComputeDataRoot`/`ComputeStateRoot` exclude `ParentSnapshotID` and `SafeBlockSeq`, so the child's data/state roots equal the parent's and only `ManifestRoot`/`SnapshotID` differ; `fillSealedHeaderChainFieldsLocked` stamps `PrevSafeSnapshotID = SafeWatermark.SnapshotID` (`fsm/apply.go:162-183`); `validateBlockPins` (`fsm/genesis_validation.go:133-147`) requires `Manifests[PrevSafeSnapshotID].StateRoot == PrevStateRoot` and `base.SafeBlockSeq < seq`.
- Authority in arbiter: `fsm/consensus_updates.go:83-85` `authority.Validator{AllowedAddresses: authorityAddressSet(previous.AuthorityAddresses)}.VerifyConsensusParamsUpdate` (clock-free), `authorityAddressSet` at `fsm/consensus_updates.go:100` and `server/consensus_admin.go:361`; `Params.AuthorityAddresses []string` (`fsm/state.go:90`, lowercase, wired from config at `cmd/arbiter/services.go:35-43`); server precedent `UpdateConsensusParams` (`server/consensus_admin.go:79-98`) → `authorizeConsensusParamsUpdate` (`:116-145`: token shape → `consensusLeaderCheck` → `ConsensusParamsView` preconditions → `authority.Validator{AllowedAddresses: authorityAddressSet(view.Current.AuthorityAddresses), MaxTokenAge: s.d.Cfg.ConsensusAdminMaxTokenAge}.Authorize…` → `PermissionDenied`); `consensusAdminService{pb.UnimplementedConsensusAdminServer; s *Server}` registered at `server/server.go:94`.
- Snapshot: `snapshotVersion = 11` (`fsm/snapshot.go:20-32`), accept list `:198-200`, predicates `:215,:224,:332`, scrubs `:337-364`, validators `:368-391` (`validateSnapshotQueryReservationFrontiers`, `validateSnapshotQueryStatements`, `validateSnapshotQueryAdmissions` — the last refuses any lifecycle other than `sequenced` at `fsm/genesis_validation.go:271-273` and already accepts a tombstone for the reservation key at `:291-294`); version-byte tests: `fsm/snapshot_test.go:228-229,255,490-491`, `fsm/snapshot_query_block_test.go:286`, `fsm/artifact_reservation_release_seam_test.go:30-31,59-60`, `fsm/apply_artifact_disposition_test.go:124-125,675`, `fsm/artifact_capacity_test.go:207`, `fsm/consensus_history_test.go:79,257`, `fsm/promotion_legacy_replay_test.go:604-605,708`, `cmd/arbiter/storage_protocol_test.go`.
- housegate: `SnapshotQueryStatus{Version, Found, Accepted, Lifecycle, ExecutionOutcome, TerminalProof []byte}`; the intake reads none of the three terminal fields today (`recoverRecord`, `pkg/storageintegrity/snapshot_query_intake.go:187-205`, turns any `found=true` into a `sequenced` result — the Wave 3 sequencer client must branch on `terminal`/`aborted`); the frozen fixture `pkg/replay/testdata/snapshot_query_v1.json` already carries an `abort` vector (`0x22fec686…`) and a status `{lifecycle:"terminal", execution_outcome:"aborted", terminal_proof:<opaque>}`; arbiter-core pins that fixture's SHA (`conformance/snapshot_query_wire_test.go:57`) — this plan does not edit it.
- Tests/helpers: fsm `newTestFSM`, `testParams`, `applyCmd`, `snapshotQueryTestSigner`, `grantedReservation`, `signedQueryEnvelope`, `snapshotBytes`, `restoreInto`, `rewriteSnapshotDocument`, `publishManifestFromInputs`/`manifestFromInputs` (`fsm/promotion_frontier_test.go:15-41`), `registerActive`; authority test signers via `authority.NewSignerFromHex` (`fsm/consensus_history_test.go:51`); server `startServerWithConfig`, `publishServerGenesis`, `activateServerTestQueryPolicy`, `enabledReservationDeps`, `signedQueryEnvelope`, `fakeNode` (numbered log entries), `TestSubmitSnapshotQuery…` suites in `server/snapshot_query_submit_test.go`.

## Design decisions (rulings carried into the tasks)

1. **Authority-signed, admin-proposed, leader-verified.** The abort travels as `wire.AbortSnapshotQuery{Record, AuthorityJWS}`; the token's `cmd_hash` is `authority.SnapshotQueryAbortHash(record)` under the new command domain (one domain per command kind, as `authority/payload.go` argues), not the record's own `snapshot-query-abort-v1` digest, which stays the receipt's `abort_record_root`. Two `ConsensusAdmin` RPCs carry it: `GetSnapshotQueryAbortCandidate` (leader read with a barrier) and `AbortSnapshotQuery` (leader-only, authorized mutation, returns the terminal `SnapshotQueryStatus`). No leader timer proposes aborts. Cost: a two-step operator flow (fetch, sign, submit) — the price of a record that binds the child snapshot id.
2. **Replica-derived candidate.** `snapshotQueryAbortCandidateLocked` is the single derivation used by the read and by Apply: the consumed barrier's reservation, its `sequenced` admission whose block is the chain tip, all earlier blocks safe and published (`safePrefixLocked == BlockSeq-1 == SafeWatermark.SafeBlockSeq`), the query header's `PrevSafeSnapshotID` equal to the watermark and to the reservation's read snapshot, and the no-op child sealed from `manifestInputsAtLocked(f, BlockSeq)` (a refactor of `manifestInputsLocked` that takes the prefix instead of reading it) with data/state roots equal to the parent's. Apply refuses a record that differs from the candidate anywhere but `ReasonCode`.
3. **Atomic terminal.** Apply order after verification: mark the statement `StatusSafe` → `validateManifestPublication(child)` (the same validator as `PublishSafeSnapshot`; a refusal reverts the status and rejects) → clone the disposition, tombstone (`released`, generation+1, `BlockSeq` = the record's begin-time identity, `TerminalProof` = canonical JSON of the signed abort, `TerminalIndex` = index), barrier `idle` at generation+1, admission `terminal`/`aborted` with the abort → publish the child (`Manifests`, `SafeWatermark`) → swap the clone → `emit(EventManifestPublished)`. Barrier release and child publication are one entry, which is what design D9 line 240 demands.
4. **Vocabulary and proof.** `SnapshotQueryLifecycleTerminal = "terminal"`, `SnapshotQueryExecutionOutcomeAborted = "aborted"`; `TerminalProof` (status and tombstone) is `json.Marshal(wire.AbortSnapshotQuery{Record, AuthorityJWS})` — self-describing and re-verifiable by any holder of the authority allowlist; `GetSnapshotQueryStatus` populates `ExecutionOutcome` and `TerminalProof` from the admission through one shared builder.
5. **Snapshot v12.** No new container; the admission gains `execution_outcome` and `abort` (omitempty); restore re-verifies the abort signature, the record ↔ admission ↔ statement ↔ block ↔ child manifest ↔ tombstone linkage and the no-op invariant, and a `< v12` container carrying a terminal admission is refused.
6. **Defensive guards.** `applyRecordPromotionAck` and `applyResolveChallenge` refuse a query statement before any mutation ("unreachable but refuse anyway", as `applySealL3Block` does); `applyReleaseReservation` keeps refusing `consumed` (the abort is the only exit this wave; C5's applied terminal is the other); `legacyMutationGuard` makes the abort unreachable under an enabled capacity ledger exactly like submit (1a-3 routes both through the charged path).
7. **Fencing.** After the abort the barrier is `idle` at generation+1, so the C2 admission fence lifts and v2 statements chain onto the child; `RecordSnapshotQueryClaim`/`Attestation` have no Apply arm yet and the attestation path requires a `consumed` record plus an open query obligation, so a stale claim after abort fails structurally — nothing else needs an explicit fence this wave (recorded, not assumed: Task 3 pins "next v2 block after abort" and "release after abort is terminal").

Execution order: Task 1 (arbiter-core) → Task 2 (arbiter-proto) → Task 3 (arbiter fsm, starts by bumping both pins) → Task 4 (arbiter server) → Task 5 (docs). One PR per task (Tasks 3 and 4 may be one PR at the executor's discretion). Each arbiter task ends with `bazel test //fsm:fsm_test //server:server_test --jobs=4` green and gofmt clean.

---

### Task 1: The `housegate-snapshot-query-abort-v1` authority purpose (arbiter-core)

**Files:**
- Create: `authority/snapshot_query_abort.go`, `authority/snapshot_query_abort_test.go`
- Modify: `authority/BUILD.bazel` (gazelle), `README.md` (package table row for `authority`)

**Interfaces:**
- Produces: `const SnapshotQueryAbortPurpose = "housegate-snapshot-query-abort-v1"`; `func ValidateSnapshotQueryAbortRecord(r replay.SnapshotQueryAbortRecord) error`; `func SnapshotQueryAbortHash(r replay.SnapshotQueryAbortRecord) (string, error)`; `func (s *Signer) SignSnapshotQueryAbort(r) (string, error)` and `SignSnapshotQueryAbortAt(r, iat int64)`; `func (v *Validator) VerifySnapshotQueryAbort(r, token string) (string, error)` (clock-free); `func (v *Validator) AuthorizeSnapshotQueryAbort(r, token string) (string, error)` (age-enforced).

- [ ] **Step 1: Write the failing tests**

Create `authority/snapshot_query_abort_test.go`:

```go
package authority

import (
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

func testAbortRecord() replay.SnapshotQueryAbortRecord {
	return replay.SnapshotQueryAbortRecord{
		BlockSeq: 13, StatementID: "0x1234:1:fixture", InputRoot: "0x" + strings.Repeat("ab", 32),
		ReservationID: "reservation-1", FencingGeneration: 9, ReasonCode: "resource_exhausted",
		PrevSnapshotID: "0x" + strings.Repeat("11", 32), NextSnapshotID: "0x" + strings.Repeat("22", 32),
	}
}

func TestSnapshotQueryAbortSignVerifyAuthorizeRoundTrip(t *testing.T) {
	s := testSigner(t) // the existing helper used by consensus_test.go; if it is named differently, use that name
	rec := testAbortRecord()
	token, err := s.SignSnapshotQueryAbort(rec)
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	for name, fn := range map[string]func(replay.SnapshotQueryAbortRecord, string) (string, error){"verify": v.VerifySnapshotQueryAbort, "authorize": v.AuthorizeSnapshotQueryAbort} {
		if got, err := fn(rec, token); err != nil || got != s.Address() {
			t.Fatalf("%s = %q, %v", name, got, err)
		}
	}
}

func TestSnapshotQueryAbortHashIsNotTheRecordDigest(t *testing.T) {
	rec := testAbortRecord()
	cmd, err := SnapshotQueryAbortHash(rec)
	if err != nil {
		t.Fatal(err)
	}
	record, err := rec.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if cmd == record {
		t.Fatal("command digest must live in its own domain, not snapshot-query-abort-v1")
	}
}

func TestSnapshotQueryAbortRefusesTamperedRecordWrongPurposeAndUnlistedSigner(t *testing.T) {
	s := testSigner(t)
	rec := testAbortRecord()
	token, _ := s.SignSnapshotQueryAbort(rec)
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	tampered := rec
	tampered.ReasonCode = "operator_requested"
	if _, err := v.VerifySnapshotQueryAbort(tampered, token); err == nil {
		t.Fatal("tampered record accepted")
	}
	// A consensus-update token over the same bytes cannot authorize an abort, and vice versa.
	foreign, _ := s.signPayload(JWSCommandPayload{Iat: time.Now().Unix(), Purpose: ConsensusParamsUpdatePurpose, CmdHash: mustAbortHash(t, rec)})
	if _, err := v.VerifySnapshotQueryAbort(rec, foreign); err == nil {
		t.Fatal("consensus-purpose token authorized an abort")
	}
	other := Validator{AllowedAddresses: map[string]bool{"0x" + strings.Repeat("0f", 20): true}, MaxTokenAge: time.Minute}
	if _, err := other.VerifySnapshotQueryAbort(rec, token); err == nil {
		t.Fatal("unlisted signer accepted")
	}
	if _, err := (&Validator{MaxTokenAge: time.Minute}).VerifySnapshotQueryAbort(rec, token); err == nil {
		t.Fatal("empty allowlist accepted")
	}
}

func TestSnapshotQueryAbortVerifyIgnoresAgeAndAuthorizeEnforcesIt(t *testing.T) {
	s := testSigner(t)
	rec := testAbortRecord()
	stale, err := s.SignSnapshotQueryAbortAt(rec, time.Now().Add(-48*time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	if _, err := v.VerifySnapshotQueryAbort(rec, stale); err != nil {
		t.Fatalf("deterministic verify read the clock: %v", err)
	}
	if _, err := v.AuthorizeSnapshotQueryAbort(rec, stale); err == nil {
		t.Fatal("stale token authorized")
	}
	if _, err := (&Validator{AllowedAddresses: map[string]bool{s.Address(): true}}).AuthorizeSnapshotQueryAbort(rec, stale); err == nil {
		t.Fatal("zero MaxTokenAge must fail closed")
	}
}

func TestSnapshotQueryAbortRecordValidation(t *testing.T) {
	cases := map[string]func(*replay.SnapshotQueryAbortRecord){
		"zero block":        func(r *replay.SnapshotQueryAbortRecord) { r.BlockSeq = 0 },
		"zero generation":   func(r *replay.SnapshotQueryAbortRecord) { r.FencingGeneration = 0 },
		"empty statement":   func(r *replay.SnapshotQueryAbortRecord) { r.StatementID = " " },
		"empty input root":  func(r *replay.SnapshotQueryAbortRecord) { r.InputRoot = "" },
		"empty reservation": func(r *replay.SnapshotQueryAbortRecord) { r.ReservationID = "" },
		"empty reason":      func(r *replay.SnapshotQueryAbortRecord) { r.ReasonCode = "" },
		"nul reason":        func(r *replay.SnapshotQueryAbortRecord) { r.ReasonCode = "a\x00b" },
		"empty prev":        func(r *replay.SnapshotQueryAbortRecord) { r.PrevSnapshotID = "" },
		"empty next":        func(r *replay.SnapshotQueryAbortRecord) { r.NextSnapshotID = "" },
		"prev equals next":  func(r *replay.SnapshotQueryAbortRecord) { r.NextSnapshotID = r.PrevSnapshotID },
	}
	for name, mutate := range cases {
		rec := testAbortRecord()
		mutate(&rec)
		if err := ValidateSnapshotQueryAbortRecord(rec); err == nil {
			t.Fatalf("%s accepted", name)
		}
		if _, err := SnapshotQueryAbortHash(rec); err == nil {
			t.Fatalf("%s hashed", name)
		}
	}
	if err := ValidateSnapshotQueryAbortRecord(testAbortRecord()); err != nil {
		t.Fatal(err)
	}
}

func mustAbortHash(t *testing.T, r replay.SnapshotQueryAbortRecord) string {
	t.Helper()
	h, err := SnapshotQueryAbortHash(r)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
```

`testSigner(t)` does not exist yet (`authority_test.go:29` calls `NewSignerFromHex(testKeyHex)` inline): add `func testSigner(t *testing.T) *Signer` to the new test file, minting from the existing `testKeyHex` and failing the test on error.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //authority:authority_test --test_output=errors`
Expected: build FAILS on `SignSnapshotQueryAbort` / `SnapshotQueryAbortHash` / `ValidateSnapshotQueryAbortRecord` undefined.

- [ ] **Step 3: Implement the purpose**

Create `authority/snapshot_query_abort.go`:

```go
package authority

import (
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

// SnapshotQueryAbortPurpose is the token family for C3's authorized terminal
// abort of a sequenced snapshot query. A promotion, cleanup, consensus-update,
// disposition or control token cannot authorize an abort, and an abort token
// authorizes nothing else. It is never inferred from a client cancellation, a
// source's local timeout or an Arbiter leader timer.
const SnapshotQueryAbortPurpose = "housegate-snapshot-query-abort-v1"

// snapshotQueryAbortCommandDomain keeps the signed command digest apart from
// the record's own snapshot-query-abort-v1 digest, which is the receipt's
// abort_record_root: one domain per command kind, as payload.go explains.
const snapshotQueryAbortCommandDomain = "arbiter-snapshot-query-abort-command-v1"

// ValidateSnapshotQueryAbortRecord enforces the constraints every abort record
// must satisfy regardless of committed state. State-dependent validation
// (block, reservation, generation, predecessor and child snapshot ids) belongs
// to the Arbiter FSM. cleanup_authorization_root may be empty here; its
// contract is the FSM's.
func ValidateSnapshotQueryAbortRecord(r replay.SnapshotQueryAbortRecord) error {
	if r.BlockSeq == 0 || r.FencingGeneration == 0 {
		return fmt.Errorf("snapshot query abort: block_seq and fencing_generation must be positive")
	}
	fields := []struct{ name, value string }{
		{"statement_id", r.StatementID}, {"input_root", r.InputRoot}, {"reservation_id", r.ReservationID},
		{"reason_code", r.ReasonCode}, {"prev_snapshot_id", r.PrevSnapshotID}, {"next_snapshot_id", r.NextSnapshotID},
	}
	for _, f := range fields {
		if strings.TrimSpace(f.value) == "" || strings.ContainsRune(f.value, 0) {
			return fmt.Errorf("snapshot query abort: %s must be non-empty and NUL-free", f.name)
		}
	}
	if strings.ContainsRune(r.CleanupAuthorizationRoot, 0) {
		return fmt.Errorf("snapshot query abort: cleanup_authorization_root must be NUL-free")
	}
	if r.PrevSnapshotID == r.NextSnapshotID {
		return fmt.Errorf("snapshot query abort: next_snapshot_id must differ from prev_snapshot_id")
	}
	return nil
}

// SnapshotQueryAbortHash binds every record field under the command domain.
// Both live authorization and deterministic replay use it.
func SnapshotQueryAbortHash(r replay.SnapshotQueryAbortRecord) (string, error) {
	if err := ValidateSnapshotQueryAbortRecord(r); err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(snapshotQueryAbortCommandDomain, r)
	if err != nil {
		return "", fmt.Errorf("hash snapshot query abort: %w", err)
	}
	return h, nil
}

// SignSnapshotQueryAbort signs one complete abort record at the current time.
func (s *Signer) SignSnapshotQueryAbort(r replay.SnapshotQueryAbortRecord) (string, error) {
	return s.SignSnapshotQueryAbortAt(r, time.Now().Unix())
}

// SignSnapshotQueryAbortAt signs with an explicit Unix issue time; issue time
// is an API-boundary freshness guard, never a replay guard.
func (s *Signer) SignSnapshotQueryAbortAt(r replay.SnapshotQueryAbortRecord, iat int64) (string, error) {
	h, err := SnapshotQueryAbortHash(r)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: SnapshotQueryAbortPurpose, CmdHash: h})
}

// VerifySnapshotQueryAbort checks signature, purpose, command hash and the
// authority allowlist without reading the clock. Raft Apply and snapshot
// restore must use this path; an empty allowlist still fails closed.
func (v *Validator) VerifySnapshotQueryAbort(r replay.SnapshotQueryAbortRecord, token string) (string, error) {
	h, err := SnapshotQueryAbortHash(r)
	if err != nil {
		return "", err
	}
	return v.verify(h, SnapshotQueryAbortPurpose, token, false)
}

// AuthorizeSnapshotQueryAbort additionally enforces token age for an API
// boundary. It must not be used inside replicated Apply or snapshot replay.
func (v *Validator) AuthorizeSnapshotQueryAbort(r replay.SnapshotQueryAbortRecord, token string) (string, error) {
	h, err := SnapshotQueryAbortHash(r)
	if err != nil {
		return "", err
	}
	return v.verify(h, SnapshotQueryAbortPurpose, token, true)
}
```

Update the `README.md` package-table row to: `| \`authority\` | Domain-separated promotion, cleanup, consensus-update and snapshot-query-abort signing and validation. |`. Run gazelle (`bazel run //:gazelle`) so `BUILD.bazel` lists the two new files.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //authority/... //conformance/... --test_output=errors` and `bash scripts/check-public-boundary.sh`.
Expected: PASS.

- [ ] **Step 5: Commit and open the PR**

```bash
git add authority/snapshot_query_abort.go authority/snapshot_query_abort_test.go authority/BUILD.bazel README.md
git commit -m "feat(authority): add the housegate-snapshot-query-abort-v1 purpose"
```

PR title `feat(authority): add the housegate-snapshot-query-abort-v1 purpose`; after the squash merge record the main commit SHA — Task 3 pins it.

---

### Task 2: `ConsensusAdmin` abort RPCs (arbiter-proto)

**Files:**
- Modify: `proto/consensus.proto` (import `replay.proto`; two messages; two RPCs), `gen/pb/*` (regenerated), `conformance/snapshot_query_services_test.go` (two new table rows), `docs/compatibility/snapshot-query-raft-allocation.md` (a "Wave 1a-2b" note that the two RPCs are additive and unimplemented until arbiter installs them)

**Interfaces:**
- Produces (proto, package `arbiter`, Go package `pb`): `message GetSnapshotQueryAbortCandidateRequest { string network_id = 1; uint32 keeper_shard_id = 2; string client_account = 3; string statement_id = 4; }`; `message AbortSnapshotQueryRequest { SnapshotQueryAbortRecord record = 1; string authority_jws = 2; }`; on `service ConsensusAdmin`: `rpc GetSnapshotQueryAbortCandidate (GetSnapshotQueryAbortCandidateRequest) returns (SnapshotQueryAbortRecord) {}` and `rpc AbortSnapshotQuery (AbortSnapshotQueryRequest) returns (SnapshotQueryStatus) {}`.

- [ ] **Step 1: Extend the conformance table first (the failing test)**

In `conformance/snapshot_query_services_test.go`'s `TestSnapshotQueryServicesRemainUnimplemented` table add:

```go
		{"ConsensusAdmin", "GetSnapshotQueryAbortCandidate", "GetSnapshotQueryAbortCandidateRequest", "SnapshotQueryAbortRecord", pb.UnimplementedConsensusAdminServer{}},
		{"ConsensusAdmin", "AbortSnapshotQuery", "AbortSnapshotQueryRequest", "SnapshotQueryStatus", pb.UnimplementedConsensusAdminServer{}},
```

The loop resolves services through `pb.File_arbiter_proto`; `ConsensusAdmin` lives in `consensus.proto`, so resolve by file: change the lookup to try `pb.File_consensus_proto` when `pb.File_arbiter_proto` has no such service (a two-line fallback), and record it.

Run: `go test ./conformance/...`
Expected: FAIL ("service missing" or method nil).

- [ ] **Step 2: Add the messages and RPCs**

In `proto/consensus.proto` add `import "replay.proto";` next to the existing imports (raftlog.proto already imports both consensus.proto and replay.proto, so no cycle), then after `UpdateConsensusParamsCmd`:

```protobuf
// Identity of one sequenced snapshot query whose consumed reservation an
// authority intends to abort. Version 1 uses keeper_shard_id 0.
message GetSnapshotQueryAbortCandidateRequest {
  string network_id = 1;
  uint32 keeper_shard_id = 2;
  string client_account = 3;
  string statement_id = 4;
}

// An authority-authenticated terminal abort of a sequenced snapshot query.
// The record must equal the replica-derived candidate except reason_code;
// never inferred from client cancellation or a local timeout.
message AbortSnapshotQueryRequest {
  SnapshotQueryAbortRecord record = 1;
  string authority_jws = 2;
}
```

and in `service ConsensusAdmin`:

```protobuf
  // Leader-only read with a barrier: the replica-derived abort record for the
  // consumed reservation of one account/statement, with reason_code and
  // cleanup_authorization_root left empty for the authority to fill and sign.
  // NotFound when that identity holds no consumed reservation.
  rpc GetSnapshotQueryAbortCandidate (GetSnapshotQueryAbortCandidateRequest) returns (SnapshotQueryAbortRecord) {}
  // Leader-only, authority-authenticated terminal abort; returns the terminal
  // status (lifecycle "terminal", execution_outcome "aborted", terminal_proof).
  rpc AbortSnapshotQuery (AbortSnapshotQueryRequest) returns (SnapshotQueryStatus) {}
```

Regenerate: `make tools && make proto && make lint && make test`. If `snapshot_query_compatibility_test.go` compares the current descriptor set against a pinned baseline in a way that refuses additive service methods, extend its allow-list the way the file documents for additive changes (record exactly what you changed); do not regenerate or re-pin the historical baseline files.

- [ ] **Step 3: Document and verify**

Append to `docs/compatibility/snapshot-query-raft-allocation.md` one paragraph: Wave 1a-2b adds `ConsensusAdmin.GetSnapshotQueryAbortCandidate` and `ConsensusAdmin.AbortSnapshotQuery` (no new Raft tag; tag 22 remains `abort_snapshot_query`); both are unimplemented until arbiter installs the default-off sequencing dependency.

Run: `go test ./...` (module root) and `make lint`.
Expected: PASS.

- [ ] **Step 4: Commit and open the PR**

```bash
git add proto/consensus.proto gen/pb conformance/snapshot_query_services_test.go docs/compatibility/snapshot-query-raft-allocation.md
git commit -m "feat(consensus): add the ConsensusAdmin snapshot-query abort RPCs"
```

After the squash merge record the main commit SHA — Task 3 pins it.

---

### Task 3: Abort candidate, atomic terminal transition and snapshot v12 (arbiter fsm)

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (pins), `fsm/state.go` (constants, `SnapshotQueryAbortState`, admission fields, clone), `fsm/fsm.go` (arm), `fsm/reads_manifest.go` (`manifestInputsAtLocked`), `fsm/apply.go` (two guards), `fsm/genesis_validation.go` (`validateSnapshotQueryAdmissions`), `fsm/snapshot.go` (v12 + gate), `fsm/BUILD.bazel` (gazelle: `@arbiter_core//authority`), the version-byte tests listed in Baseline facts
- Create: `fsm/apply_snapshot_query_abort.go`, `fsm/apply_snapshot_query_abort_test.go`

**Interfaces:**
- Consumes: `authority.ValidateSnapshotQueryAbortRecord`, `authority.Validator.VerifySnapshotQueryAbort`, `authorityAddressSet` (fsm/consensus_updates.go:100), `validateManifestPublication`, `cloneManifest`, `putArtifactDispositionReservationTombstone`, `putArtifactDispositionReservationBarrier`, `cloneArtifactDispositionForReservationGrant`, `ensureDispositionMaps`, `safePrefixLocked`, `normalizedArtifactDispositionReservationBarrier`, `artifactDispositionReservationKey`, `reservationsEqual`.
- Produces: `const SnapshotQueryLifecycleTerminal = "terminal"`, `const SnapshotQueryExecutionOutcomeAborted = "aborted"`; `type SnapshotQueryAbortState struct { Record replay.SnapshotQueryAbortRecord; AuthorityJWS string; AbortRecordRoot string; Authority string; CommitIndex uint64 }` (json `record`, `authority_jws`, `abort_record_root`, `authority`, `commit_index`); `SnapshotQueryAdmissionState.ExecutionOutcome string json:"execution_outcome,omitempty"` and `Abort *SnapshotQueryAbortState json:"abort,omitempty"`; `func (a SnapshotQueryAdmissionState) TerminalProof() []byte` (nil unless terminal); `type SnapshotQueryAbortCandidate struct { ClientAccount string; Record replay.SnapshotQueryAbortRecord }`; `func (f *FSM) SnapshotQueryAbortCandidate() (SnapshotQueryAbortCandidate, bool)`; `func (f *FSM) applyAbortSnapshotQuery(c *wire.AbortSnapshotQuery, index uint64) any` (returns `Applied{}` or `Rejected`); `func snapshotQueryTerminalProof(rec replay.SnapshotQueryAbortRecord, jws string) []byte`; `func manifestInputsAtLocked(f *FSM, safePrefix uint64) (ManifestInputs, bool)`; `snapshotVersion = 12`, `snapshotVersionV12`.

- [ ] **Step 1: Bump the pins**

```bash
bash scripts/update-arbiter-core.sh <Task 1 merge SHA>
go get github.com/sentioxyz/arbiter-proto@<Task 2 merge SHA> && go mod tidy && bazel mod tidy
bazel test //scripts/... //fsm:fsm_test --jobs=4 --test_output=errors
```

Expected: green (the `pb` package now has the two RPCs; nothing implements them yet, and `scripts/update_dependency_test.go` still passes). Commit: `chore(deps): consume arbiter-core snapshot-query-abort authority and arbiter-proto abort RPCs`.

- [ ] **Step 2: Write the failing tests**

Create `fsm/apply_snapshot_query_abort_test.go`:

```go
package fsm

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

const testAbortAuthorityKey = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

// sequencedQuery grants and submits one query and installs the abort authority.
func sequencedQuery(t *testing.T, f *FSM) (*auth.RelaySigner, *authority.Signer, replay.SnapshotQuerySubmitResult) {
	t.Helper()
	s := snapshotQueryTestSigner(t)
	statementID := canonicalQueryStatementID(s, 1)
	r := grantedReservation(t, f, s, statementID, "request-1")
	env := signedQueryEnvelope(t, s, r)
	got, ok := applyCmd(t, f, 43, wire.Command{SubmitSnapshotQuery: &wire.SubmitSnapshotQuery{Envelope: env}}).(SnapshotQuerySubmitApplied)
	if !ok {
		t.Fatalf("submit = %#v", got)
	}
	a, err := authority.NewSignerFromHex(testAbortAuthorityKey)
	if err != nil {
		t.Fatal(err)
	}
	f.st.Params.AuthorityAddresses = []string{a.Address()}
	return s, a, got.Result
}

func signedAbort(t *testing.T, a *authority.Signer, rec replay.SnapshotQueryAbortRecord) wire.Command {
	t.Helper()
	jws, err := a.SignSnapshotQueryAbort(rec)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{AbortSnapshotQuery: &wire.AbortSnapshotQuery{Record: rec, AuthorityJWS: jws}}
}

func TestAbortSnapshotQueryPublishesTheNoOpChildAndReleasesTheBarrier(t *testing.T) {
	f := newTestFSM(t)
	s, a, res := sequencedQuery(t, f)
	parent := f.st.Manifests[f.st.SafeWatermark.SnapshotID]
	cand, ok := f.SnapshotQueryAbortCandidate()
	if !ok || cand.ClientAccount != s.Address() || cand.Record.BlockSeq != res.BlockSeq || cand.Record.InputRoot != res.InputRoot ||
		cand.Record.ReservationID != res.Reservation.ReservationID || cand.Record.FencingGeneration != res.Reservation.FencingGeneration ||
		cand.Record.PrevSnapshotID != parent.SnapshotID || cand.Record.NextSnapshotID == "" || cand.Record.ReasonCode != "" || cand.Record.CleanupAuthorizationRoot != "" {
		t.Fatalf("candidate = %+v ok=%v", cand, ok)
	}
	rec := cand.Record
	rec.ReasonCode = "resource_exhausted"
	cmd := signedAbort(t, a, rec)
	if got := applyCmd(t, f, 44, cmd); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("abort = %#v", got)
	}
	child := f.st.Manifests[rec.NextSnapshotID]
	if child == nil || child.ParentSnapshotID != parent.SnapshotID || child.SafeBlockSeq != res.BlockSeq || child.DataRoot != parent.DataRoot || child.StateRoot != parent.StateRoot || !reflect.DeepEqual(child.Tables, parent.Tables) {
		t.Fatalf("child = %+v", child)
	}
	if w := f.st.SafeWatermark; w.SnapshotID != child.SnapshotID || w.SafeBlockSeq != res.BlockSeq || w.ManifestRoot != child.ManifestRoot {
		t.Fatalf("watermark = %+v", w)
	}
	if st := f.st.Statements[res.StatementSeq]; st.Status != StatusSafe || safePrefixLocked(f) != res.BlockSeq {
		t.Fatalf("statement = %+v prefix = %d", st, safePrefixLocked(f))
	}
	d := f.st.ArtifactDisposition
	key, _ := artifactDispositionReservationKey(s.Address(), rec.StatementID, "request-1")
	tomb := d.ReservationTombstones[key]
	if d.Reservations[key] != nil || tomb == nil || tomb.State != ArtifactReservationReleased || tomb.FencingGeneration != rec.FencingGeneration+1 || !reflect.DeepEqual(tomb.TerminalProof, snapshotQueryTerminalProof(rec, cmd.AbortSnapshotQuery.AuthorityJWS)) || tomb.TerminalIndex != 44 {
		t.Fatalf("tombstone = %+v", tomb)
	}
	if b := normalizedArtifactDispositionReservationBarrier(d.ReservationBarrier); b.State != ArtifactReservationIdle || b.Generation != rec.FencingGeneration+1 {
		t.Fatalf("barrier = %+v", b)
	}
	adm, found := f.SnapshotQueryStatusRead(s.Address(), rec.StatementID)
	root, _ := rec.Hash()
	if !found || adm.Lifecycle != SnapshotQueryLifecycleTerminal || adm.ExecutionOutcome != SnapshotQueryExecutionOutcomeAborted || adm.Abort == nil || adm.Abort.AbortRecordRoot != root || adm.Abort.Authority != a.Address() || adm.Abort.CommitIndex != 44 || !reflect.DeepEqual(adm.Result(), res) {
		t.Fatalf("admission = %+v", adm)
	}
	var proof wire.AbortSnapshotQuery
	if err := json.Unmarshal(adm.TerminalProof(), &proof); err != nil || proof.Record != rec || proof.AuthorityJWS != cmd.AbortSnapshotQuery.AuthorityJWS {
		t.Fatalf("terminal proof = %s (%v)", adm.TerminalProof(), err)
	}
	// The fence lifted: a v2 statement is admitted again and its block chains onto the child.
	if _, ok := f.applySubmitStatement(testSubmitStatement(t)).(SubmitResult); !ok {
		t.Fatal("v2 admission still fenced after abort")
	}
	if got := f.applySealL3Block(); reflect.TypeOf(got) == reflect.TypeOf(Rejected{}) {
		t.Fatalf("seal after abort = %#v", got)
	}
	if h := f.st.Blocks[len(f.st.Blocks)-1]; h.PrevSafeSnapshotID != child.SnapshotID || h.PrevStateRoot != child.StateRoot || h.L3BlockSeq != res.BlockSeq+1 {
		t.Fatalf("next header = %+v", h)
	}
	// Identical replay is idempotent; a different abort for the same statement is terminal; release is terminal.
	before := snapshotBytes(t, f)
	if got := applyCmd(t, f, 45, cmd); !reflect.DeepEqual(got, Applied{}) || string(snapshotBytes(t, f)) != string(before) {
		t.Fatalf("replay = %#v", got)
	}
	other := rec
	other.ReasonCode = "operator_requested"
	if got, ok := applyCmd(t, f, 46, signedAbort(t, a, other)).(Rejected); !ok || !strings.Contains(got.Reason, "terminal") || string(snapshotBytes(t, f)) != string(before) {
		t.Fatalf("second abort = %#v", got)
	}
}

func TestAbortSnapshotQueryRefusesMismatchesBeforeAnyStateChange(t *testing.T) {
	f := newTestFSM(t)
	_, a, _ := sequencedQuery(t, f)
	cand, ok := f.SnapshotQueryAbortCandidate()
	if !ok {
		t.Fatal("no candidate")
	}
	base := cand.Record
	base.ReasonCode = "resource_exhausted"
	before := snapshotBytes(t, f)
	other, _ := authority.NewSignerFromHex("dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
	cases := map[string]struct {
		mutate func(r *replay.SnapshotQueryAbortRecord)
		signer *authority.Signer
		tamper func(c *wire.Command)
		want   string
	}{
		"wrong block":         {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.BlockSeq++ }, want: "candidate"},
		"wrong statement":     {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.StatementID = r.StatementID + "x" }, want: "candidate"},
		"wrong input root":    {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.InputRoot = "0x" + strings.Repeat("00", 32) }, want: "candidate"},
		"wrong reservation":   {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.ReservationID = "other" }, want: "candidate"},
		"wrong generation":    {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.FencingGeneration++ }, want: "candidate"},
		"wrong prev":          {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.PrevSnapshotID = "0x" + strings.Repeat("01", 32) }, want: "candidate"},
		"wrong next":          {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.NextSnapshotID = "0x" + strings.Repeat("02", 32) }, want: "candidate"},
		"empty reason":        {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.ReasonCode = "" }, want: "reason_code"},
		"cleanup root":        {mutate: func(r *replay.SnapshotQueryAbortRecord) { r.CleanupAuthorizationRoot = "0x01" }, want: "cleanup_authorization_root"},
		"unlisted authority":  {signer: other, want: "authority"},
		"tampered signature":  {tamper: func(c *wire.Command) { j := c.AbortSnapshotQuery.AuthorityJWS; c.AbortSnapshotQuery.AuthorityJWS = j[:len(j)-2] + "AA" }, want: "authority"},
		"consensus purpose":   {tamper: func(c *wire.Command) { c.AbortSnapshotQuery.AuthorityJWS = consensusPurposeTokenOver(t, a, c.AbortSnapshotQuery.Record) }, want: "authority"},
		"zero index":          {tamper: func(c *wire.Command) {}, want: "log index"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := base
			if tc.mutate != nil {
				tc.mutate(&rec)
			}
			signer := a
			if tc.signer != nil {
				signer = tc.signer
			}
			cmd := signedAbort(t, signer, rec)
			if tc.tamper != nil {
				tc.tamper(&cmd)
			}
			index := uint64(50)
			if name == "zero index" {
				index = 0
			}
			var got any
			if index == 0 {
				got = f.applyAbortSnapshotQuery(cmd.AbortSnapshotQuery, 0)
			} else {
				got = applyCmd(t, f, index, cmd)
			}
			rej, ok := got.(Rejected)
			if !ok || !strings.Contains(rej.Reason, tc.want) {
				t.Fatalf("%s = %#v, want reason containing %q", name, got, tc.want)
			}
			if string(snapshotBytes(t, f)) != string(before) {
				t.Fatalf("%s mutated state", name)
			}
		})
	}
	// No consumed reservation at all: nothing to abort.
	g := newTestFSM(t)
	g.st.Params.AuthorityAddresses = []string{a.Address()}
	if _, ok := g.SnapshotQueryAbortCandidate(); ok {
		t.Fatal("candidate without a consumed reservation")
	}
	if got, ok := applyCmd(t, g, 50, signedAbort(t, a, base)).(Rejected); !ok || !strings.Contains(got.Reason, "not consumed") {
		t.Fatalf("no reservation = %#v", got)
	}
}

// consensusPurposeTokenOver mints a token of another authority family from the same key; Apply must refuse it on purpose alone, whatever its cmd_hash.
func consensusPurposeTokenOver(t *testing.T, a *authority.Signer, _ replay.SnapshotQueryAbortRecord) string {
	t.Helper()
	tok, err := a.SignConsensusParamsUpdateAt(testConsensusUpdateForAbortSeparation(), 1)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestAbortSnapshotQueryRestoresAndRefusesAPreV12Container(t *testing.T) {
	f := newTestFSM(t)
	s, a, res := sequencedQuery(t, f)
	cand, _ := f.SnapshotQueryAbortCandidate()
	rec := cand.Record
	rec.ReasonCode = "resource_exhausted"
	if got := applyCmd(t, f, 44, signedAbort(t, a, rec)); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("abort = %#v", got)
	}
	f.st.ArtifactDisposition.QueryPolicyReadiness = nil // test scaffolding restore refuses by design
	data := snapshotBytes(t, f)
	if data[4] != snapshotVersionV12 {
		t.Fatalf("version byte = %d, want 12", data[4])
	}
	g := restoreInto(t, data)
	want, _ := f.SnapshotQueryStatusRead(s.Address(), rec.StatementID)
	got, found := g.SnapshotQueryStatusRead(s.Address(), rec.StatementID)
	if !found || !reflect.DeepEqual(got, want) || g.st.Statements[res.StatementSeq].Status != StatusSafe || g.st.SafeWatermark != f.st.SafeWatermark {
		t.Fatalf("restored = %+v", got)
	}
	refuse := func(name string, version byte, mutate func(map[string]any)) {
		t.Helper()
		doc := rewriteSnapshotDocument(t, data, version, mutate)
		if err := restoreErr(t, doc); err == nil || !strings.Contains(err.Error(), "snapshot query") {
			t.Fatalf("%s: restore = %v", name, err)
		}
	}
	refuse("pre-v12 container", snapshotVersionV11, func(map[string]any) {})
	refuse("tampered authority jws", snapshotVersionV12, func(doc map[string]any) { setAdmissionAbortField(doc, "authority_jws", "h.p.s") })
	refuse("missing child manifest", snapshotVersionV12, func(doc map[string]any) { deleteManifest(doc, rec.NextSnapshotID) })
	refuse("terminal without abort", snapshotVersionV12, func(doc map[string]any) { setAdmissionAbortField(doc, "", nil) })
}

func TestV2PromotionPathsRefuseAQueryStatement(t *testing.T) {
	f := newTestFSM(t)
	_, _, res := sequencedQuery(t, f)
	// Neither v2 StatusSafe writer may touch the query statement even if handed its sequence.
	if got, ok := f.applyRecordPromotionAckForSeqs([]uint64{res.StatementSeq}).(Rejected); !ok || !strings.Contains(got.Reason, "query statement") {
		t.Fatalf("promotion ack = %#v", got)
	}
	if got, ok := f.applyResolveChallengeForSeq(res.StatementSeq).(Rejected); !ok || !strings.Contains(got.Reason, "query statement") {
		t.Fatalf("resolve challenge = %#v", got)
	}
	if f.st.Statements[res.StatementSeq].Status != StatusSequenced {
		t.Fatal("query statement moved")
	}
}

func TestManifestInputsAtLockedMatchesManifestInputsLocked(t *testing.T) {
	f := newTestFSM(t)
	a, okA := manifestInputsLocked(f)
	b, okB := manifestInputsAtLocked(f, safePrefixLocked(f))
	if okA != okB || !reflect.DeepEqual(a, b) {
		t.Fatalf("inputs diverged: %v/%v %+v %+v", okA, okB, a, b)
	}
}
```

Test-only helpers to write in the same file (name them exactly): `restoreErr(t, doc []byte) error` (build a fresh FSM exactly the way `restoreInto` does at fsm/snapshot_test.go:44-54 and return `Restore`'s error instead of failing), `setAdmissionAbortField(doc map[string]any, field string, value any)` (navigates `artifact_disposition.query_admissions.<the one key>.abort`; an empty field name deletes the whole `abort` object), `deleteManifest(doc, id)` (removes `manifests[id]`), `testConsensusUpdateForAbortSeparation()` (a structurally valid `arbiter.ConsensusParamsUpdate` — copy the literal `fsm/consensus_updates_test.go` uses), and the two thin wrappers `applyRecordPromotionAckForSeqs` / `applyResolveChallengeForSeq` — implement them as exported-for-test entry points ONLY if the real transitions cannot be driven with a legitimate command that names the query statement; otherwise drive the real commands and delete the wrappers. Record which.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestAbortSnapshotQuery|TestV2PromotionPaths|TestManifestInputsAtLocked' --jobs=4 --test_output=errors`
Expected: build FAILS on the undefined `SnapshotQueryAbortCandidate` / `applyAbortSnapshotQuery` / `SnapshotQueryLifecycleTerminal` / `manifestInputsAtLocked` symbols.

- [ ] **Step 4: Implement**

In `fsm/state.go`, next to `SnapshotQueryLifecycleSequenced`:

```go
// SnapshotQueryLifecycleTerminal is the admission's final lifecycle: the query
// block has a committed outcome (ExecutionOutcome) and the reservation is
// released. SnapshotQueryExecutionOutcomeAborted is the only outcome this
// wave can commit; "applied" is C5's.
const (
	SnapshotQueryLifecycleTerminal       = "terminal"
	SnapshotQueryExecutionOutcomeAborted = "aborted"
)

// SnapshotQueryAbortState is the committed, authority-authenticated abort of
// one admission. AbortRecordRoot is Record.Hash() (snapshot-query-abort-v1),
// the digest a receipt names as abort_record_root; Authority is the recovered
// lowercase signer address.
type SnapshotQueryAbortState struct {
	Record          replay.SnapshotQueryAbortRecord `json:"record"`
	AuthorityJWS    string                          `json:"authority_jws"`
	AbortRecordRoot string                          `json:"abort_record_root"`
	Authority       string                          `json:"authority"`
	CommitIndex     uint64                          `json:"commit_index"`
}
```

Add to `SnapshotQueryAdmissionState` after `Lifecycle`: `ExecutionOutcome string \`json:"execution_outcome,omitempty"\`` and after `CommitIndex`: `Abort *SnapshotQueryAbortState \`json:"abort,omitempty"\``; deep-copy `Abort` in `cloneSnapshotQueryAdmissionState`. Add:

```go
// TerminalProof is the opaque proof a status read returns for a terminal
// admission: the canonical JSON of the signed abort. Nil before the terminal.
func (a SnapshotQueryAdmissionState) TerminalProof() []byte {
	if a.Abort == nil {
		return nil
	}
	return snapshotQueryTerminalProof(a.Abort.Record, a.Abort.AuthorityJWS)
}
```

In `fsm/reads_manifest.go` split `manifestInputsLocked` into `func manifestInputsLocked(f *FSM) (ManifestInputs, bool) { return manifestInputsAtLocked(f, safePrefixLocked(f)) }` and `func manifestInputsAtLocked(f *FSM, safePrefix uint64) (ManifestInputs, bool)` holding the existing body with `safePrefix` as the parameter (no other change).

Create `fsm/apply_snapshot_query_abort.go`:

```go
package fsm

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// SnapshotQueryAbortCandidate is the only abort this replica would accept for
// the consumed reservation: every field but ReasonCode is derived from
// committed state, and cleanup_authorization_root is empty this wave.
type SnapshotQueryAbortCandidate struct {
	ClientAccount string
	Record        replay.SnapshotQueryAbortRecord
	child         replay.SafeSnapshotManifest
}

// SnapshotQueryAbortCandidate derives the abort record an authority must sign.
func (f *FSM) SnapshotQueryAbortCandidate() (SnapshotQueryAbortCandidate, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	c, _, err := f.snapshotQueryAbortCandidateLocked()
	return c, err == nil
}

// snapshotQueryTerminalProof is the opaque terminal proof: the canonical JSON
// of the signed abort, re-verifiable by any holder of the authority allowlist.
func snapshotQueryTerminalProof(rec replay.SnapshotQueryAbortRecord, jws string) []byte {
	proof, err := json.Marshal(wire.AbortSnapshotQuery{Record: rec, AuthorityJWS: jws})
	if err != nil {
		return nil // encoding/json cannot fail on this flat struct
	}
	return proof
}

func (f *FSM) snapshotQueryAbortCandidateLocked() (SnapshotQueryAbortCandidate, *SnapshotQueryAdmissionState, error) {
	d := &f.st.ArtifactDisposition
	bar := normalizedArtifactDispositionReservationBarrier(d.ReservationBarrier)
	if bar.State != ArtifactReservationConsumed || bar.Reservation == nil {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("snapshot query reservation is not consumed")
	}
	key, err := artifactDispositionReservationKey(bar.ClientAccount, bar.StatementID, bar.RequestID)
	if err != nil {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("snapshot query reservation key is invalid")
	}
	active := d.Reservations[key]
	if active == nil || active.State != ArtifactReservationConsumed || active.Reservation == nil || !reservationsEqual(active.Reservation, bar.Reservation) || active.FencingGeneration != bar.Generation {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("snapshot query reservation record is not consumed under the barrier")
	}
	if active.FencingGeneration == math.MaxUint64 {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("snapshot query reservation fence overflow")
	}
	adm := d.QueryAdmissions[snapshotQueryAdmissionKey(bar.ClientAccount, bar.StatementID)]
	if adm == nil || adm.Lifecycle != SnapshotQueryLifecycleSequenced || adm.ReservationKey != key ||
		adm.Reservation.ReservationID != active.Reservation.ReservationID || adm.Reservation.FencingGeneration != active.FencingGeneration {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("snapshot query admission is not sequenced under the consumed reservation")
	}
	stmt := f.st.Statements[adm.StatementSeq]
	if stmt == nil || !stmt.IsSnapshotQuery() || stmt.Status != StatusSequenced || adm.BlockSeq == 0 || stmt.BlockSeq != adm.BlockSeq || adm.BlockSeq != uint64(len(f.st.Blocks)) {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("snapshot query block is not the sequenced chain tip")
	}
	if safePrefixLocked(f) != adm.BlockSeq-1 || f.st.SafeWatermark.SafeBlockSeq != adm.BlockSeq-1 {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("blocks before the query block are not all safe and published")
	}
	parent := f.st.Manifests[f.st.SafeWatermark.SnapshotID]
	header := f.st.Blocks[adm.BlockSeq-1]
	if parent == nil || header.PrevSafeSnapshotID != parent.SnapshotID || active.Reservation.ReadSnapshot.SnapshotID != parent.SnapshotID {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("query block predecessor is not the current watermark")
	}
	mi, ok := manifestInputsAtLocked(f, adm.BlockSeq)
	if !ok || mi.SafeBlockSeq != adm.BlockSeq || mi.ParentSnapshotID != parent.SnapshotID {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("no-op child manifest inputs are unavailable")
	}
	child, err := (replay.SafeSnapshotManifest{
		ParentSnapshotID: parent.SnapshotID, SafeBlockSeq: adm.BlockSeq,
		SchemaSnapshotID: f.st.Params.SchemaSnapshotID, SchemaRoot: mi.SchemaRoot, ExecutorProfileID: f.st.Params.ExecutorProfileID,
		Tables: mi.Tables,
	}).Seal()
	if err != nil {
		return SnapshotQueryAbortCandidate{}, nil, fmt.Errorf("seal no-op child manifest: %w", err)
	}
	if child.DataRoot != parent.DataRoot || child.StateRoot != parent.StateRoot {
		return SnapshotQueryAbortCandidate{}, nil, errors.New("no-op child manifest would change the predecessor's data or state root")
	}
	return SnapshotQueryAbortCandidate{
		ClientAccount: bar.ClientAccount,
		Record: replay.SnapshotQueryAbortRecord{
			BlockSeq: adm.BlockSeq, StatementID: bar.StatementID, InputRoot: adm.InputRoot,
			ReservationID: active.Reservation.ReservationID, FencingGeneration: active.FencingGeneration,
			PrevSnapshotID: parent.SnapshotID, NextSnapshotID: child.SnapshotID,
		},
		child: child,
	}, adm, nil
}

// terminalQueryAdmissionFor finds the admission an already committed abort
// produced for this statement and input root, in key order so the answer is
// deterministic; statement IDs embed the account, so at most one matches.
func terminalQueryAdmissionFor(d *ArtifactDispositionState, statementID, inputRoot string) *SnapshotQueryAdmissionState {
	keys := make([]string, 0, len(d.QueryAdmissions))
	for k := range d.QueryAdmissions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if a := d.QueryAdmissions[k]; a != nil && a.StatementID == statementID && a.InputRoot == inputRoot && a.Lifecycle == SnapshotQueryLifecycleTerminal {
			return a
		}
	}
	return nil
}

// applyAbortSnapshotQuery is the authorized terminal of a sequenced query:
// verify, then in one entry mark the statement safe, publish the aborted
// no-op child, release the reservation and record the outcome. The proposer
// chooses nothing but reason_code.
func (f *FSM) applyAbortSnapshotQuery(c *wire.AbortSnapshotQuery, index uint64) any {
	if c == nil || index == 0 {
		return Rejected{Reason: "abort snapshot query requires a command and a committed log index"}
	}
	rec := c.Record
	if err := authority.ValidateSnapshotQueryAbortRecord(rec); err != nil {
		return Rejected{Reason: err.Error()}
	}
	if rec.CleanupAuthorizationRoot != "" {
		return Rejected{Reason: "cleanup_authorization_root has no contract yet and must be empty"}
	}
	validator := authority.Validator{AllowedAddresses: authorityAddressSet(f.st.Params.AuthorityAddresses)}
	signer, err := validator.VerifySnapshotQueryAbort(rec, c.AuthorityJWS)
	if err != nil {
		return Rejected{Reason: "snapshot query abort authority: " + err.Error()}
	}
	root, err := rec.Hash()
	if err != nil {
		return Rejected{Reason: "snapshot query abort record root: " + err.Error()}
	}
	d := &f.st.ArtifactDisposition
	if existing := terminalQueryAdmissionFor(d, rec.StatementID, rec.InputRoot); existing != nil {
		if existing.Abort != nil && existing.Abort.AbortRecordRoot == root && existing.Abort.AuthorityJWS == c.AuthorityJWS {
			return Applied{}
		}
		return Rejected{Reason: "snapshot query abort is terminal: a different abort is already committed for this statement"}
	}
	cand, adm, err := f.snapshotQueryAbortCandidateLocked()
	if err != nil {
		return Rejected{Reason: "snapshot query abort is not available: " + err.Error()}
	}
	want := cand.Record
	want.ReasonCode = rec.ReasonCode
	if want != rec {
		return Rejected{Reason: "snapshot query abort record does not match the replica-derived candidate"}
	}
	proof := snapshotQueryTerminalProof(rec, c.AuthorityJWS)
	// Atomic tail. The statement is marked safe first because the publication
	// validator ties the child's safe_block_seq to the completed safe prefix;
	// a refusal reverts that one field and leaves everything else untouched.
	stmt := f.st.Statements[adm.StatementSeq]
	stmt.Status = StatusSafe
	child := cand.child
	if err := f.validateManifestPublication(&child); err != nil {
		stmt.Status = StatusSequenced
		return Rejected{Reason: "aborted no-op child manifest: " + err.Error()}
	}
	next := cloneArtifactDispositionForReservationGrant(d)
	ensureDispositionMaps(next)
	active := next.Reservations[adm.ReservationKey]
	tombstone := &ArtifactDispositionReservationTombstoneState{
		ClientAccount: active.ClientAccount, StatementID: active.StatementID, RequestID: active.RequestID,
		ControlBindingDigest: active.ControlBindingDigest, State: ArtifactReservationReleased,
		FencingGeneration: active.FencingGeneration + 1,
		Reservation:       cloneArtifactDispositionReservation(active.Reservation),
		CapacityBinding:   cloneArtifactDispositionReservationCapacityBinding(active.CapacityBinding),
		BlockSeq:          active.BlockSeq, TerminalProof: proof, TerminalIndex: index,
		GrantActorID: active.GrantActorID, GrantCommandRoot: active.GrantCommandRoot,
	}
	if err := putArtifactDispositionReservationTombstone(next, tombstone); err != nil {
		stmt.Status = StatusSequenced
		return Rejected{Reason: err.Error()}
	}
	if err := putArtifactDispositionReservationBarrier(next, ArtifactDispositionReservationBarrierState{State: ArtifactReservationIdle, Generation: tombstone.FencingGeneration}); err != nil {
		stmt.Status = StatusSequenced
		return Rejected{Reason: err.Error()}
	}
	a := next.QueryAdmissions[snapshotQueryAdmissionKey(cand.ClientAccount, rec.StatementID)]
	a.Lifecycle = SnapshotQueryLifecycleTerminal
	a.ExecutionOutcome = SnapshotQueryExecutionOutcomeAborted
	a.Abort = &SnapshotQueryAbortState{Record: rec, AuthorityJWS: c.AuthorityJWS, AbortRecordRoot: root, Authority: signer, CommitIndex: index}
	if index > next.LastAppliedIndex {
		next.LastAppliedIndex = index
	}
	f.st.Manifests[child.SnapshotID] = cloneManifest(&child)
	f.st.SafeWatermark = SafeWatermark{SnapshotID: child.SnapshotID, SafeBlockSeq: child.SafeBlockSeq, ManifestRoot: child.ManifestRoot}
	f.st.ArtifactDisposition = *next
	f.emit(Event{Kind: EventManifestPublished})
	return Applied{}
}
```

In `fsm/fsm.go` add `case cmd.AbortSnapshotQuery != nil: return f.applyAbortSnapshotQuery(cmd.AbortSnapshotQuery, l.Index)` after the `SubmitSnapshotQuery` arm.

Guards in `fsm/apply.go`: at the top of `applyRecordPromotionAck`'s statement handling (before any mutation) and at the top of `applyResolveChallenge`'s statement handling, `if ss.IsSnapshotQuery() { return Rejected{Reason: "query statement cannot be marked safe by the v2 promotion path"} }` — place each check in a pre-pass over the statements the command names so no earlier statement is mutated when a later one is refused.

Restore validation in `fsm/genesis_validation.go` — extend `validateSnapshotQueryAdmissions` (keep every existing check) with:

```go
	switch a.Lifecycle {
	case SnapshotQueryLifecycleSequenced:
		if a.Abort != nil || a.ExecutionOutcome != "" || stmt.Status != StatusSequenced {
			return fmt.Errorf("snapshot query admission %q is sequenced but carries terminal fields", key)
		}
	case SnapshotQueryLifecycleTerminal:
		if err := validateSnapshotQueryTerminalAdmission(st, key, a, stmt); err != nil {
			return err
		}
	default:
		return fmt.Errorf("snapshot query admission %q has unknown lifecycle %q", key, a.Lifecycle)
	}
```

and add `validateSnapshotQueryTerminalAdmission(st *State, key string, a *SnapshotQueryAdmissionState, stmt *StatementState) error` requiring: `a.ExecutionOutcome == SnapshotQueryExecutionOutcomeAborted`, `a.Abort != nil`, `a.Abort.CommitIndex != 0`, `authority.ValidateSnapshotQueryAbortRecord(a.Abort.Record) == nil` and `CleanupAuthorizationRoot == ""`, `Record.BlockSeq == a.BlockSeq`, `Record.StatementID == a.StatementID`, `Record.InputRoot == a.InputRoot`, `Record.ReservationID == a.Reservation.ReservationID`, `Record.FencingGeneration == a.Reservation.FencingGeneration`, `a.Abort.AbortRecordRoot == Record.Hash()`, `authority.Validator{AllowedAddresses: authorityAddressSet(st.Params.AuthorityAddresses)}.VerifySnapshotQueryAbort(Record, AuthorityJWS)` returning `a.Abort.Authority`, `stmt.Status == StatusSafe`, the tombstone `st.ArtifactDisposition.ReservationTombstones[a.ReservationKey]` present with `State == ArtifactReservationReleased`, `FencingGeneration == Record.FencingGeneration+1` and `bytes.Equal(TerminalProof, a.TerminalProof())`, no active record under that key, `child := st.Manifests[Record.NextSnapshotID]` present with `SafeBlockSeq == a.BlockSeq` and `ParentSnapshotID == Record.PrevSnapshotID`, `parent := st.Manifests[Record.PrevSnapshotID]` present with `parent.DataRoot == child.DataRoot && parent.StateRoot == child.StateRoot`, and `st.Blocks[a.BlockSeq-1].PrevSafeSnapshotID == Record.PrevSnapshotID`. Error strings start with `snapshot query` so the tests' `Contains` checks hold.

Snapshot v12 in `fsm/snapshot.go`: `snapshotVersion = 12`, `snapshotVersionV12 = 12`, keep `snapshotVersionV11`; extend the accept list and error text, the allowance predicate, the BootstrapParams predicate and the disposition-restore predicate with v11; no new scrub (nothing container-level is new); add, right before `validateSnapshotQueryAdmissions` runs, the version gate:

```go
	if ver[0] < snapshotVersionV12 {
		for key, a := range st.ArtifactDisposition.QueryAdmissions {
			if a != nil && (a.Lifecycle != SnapshotQueryLifecycleSequenced || a.Abort != nil || a.ExecutionOutcome != "") {
				return nil, fmt.Errorf("snapshot query admission %q is terminal in a pre-v12 container", key)
			}
		}
	}
```

Update the version-byte assertions listed in Baseline facts to v12 (fixtures that deliberately build a v9/v10/v11 container stay as they are; `cmd/arbiter/storage_protocol_test.go` too).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS, including `fsm/l3_commitment_golden_test.go` unchanged and every existing promotion/manifest/reservation test.

- [ ] **Step 6: Commit**

```bash
git add fsm/apply_snapshot_query_abort.go fsm/apply_snapshot_query_abort_test.go fsm/state.go fsm/fsm.go fsm/reads_manifest.go fsm/apply.go fsm/genesis_validation.go fsm/snapshot.go fsm/BUILD.bazel fsm/*_test.go cmd/arbiter/storage_protocol_test.go
git commit -m "feat(fsm): abort a sequenced snapshot query into an aborted no-op child; snapshot format v12"
```

---

### Task 4: `GetSnapshotQueryAbortCandidate` and `AbortSnapshotQuery` handlers (server)

**Files:**
- Create: `server/snapshot_query_abort.go`, `server/snapshot_query_abort_test.go`
- Modify: `server/snapshot_query_submit.go` (status builder shared with the abort reply), `server/BUILD.bazel` (gazelle: `@arbiter_core//authority`)

**Interfaces:**
- Consumes: `fsm.SnapshotQueryAbortCandidate`, `fsm.SnapshotQueryStatusRead`, `SnapshotQueryAdmissionState.TerminalProof()`, `authority.ValidateSnapshotQueryAbortRecord`, `authority.Validator.AuthorizeSnapshotQueryAbort`, `authorityAddressSet`, `s.d.FSM.ConsensusParamsView()`, `s.d.Cfg.ConsensusAdminMaxTokenAge`, `snapshotQuerySequencingEnabled`, `snapshotQueryNetworkScope`, `consensusLeaderCheck`, `consensusReadBarrier`, `propose`, `wire.SnapshotQueryAbortRecordFromPB/ToPB`, `wire.SnapshotQueryStatusToPB`.
- Produces: `func snapshotQueryStatusFromAdmission(adm fsm.SnapshotQueryAdmissionState) replay.SnapshotQueryStatus` (used by `getSnapshotQueryStatus` and the abort reply: `{Version: 1, Found: true, Accepted: adm.Result(), Lifecycle: adm.Lifecycle, ExecutionOutcome: adm.ExecutionOutcome, TerminalProof: adm.TerminalProof()}`); `(*consensusAdminService).GetSnapshotQueryAbortCandidate(ctx, *pb.GetSnapshotQueryAbortCandidateRequest) (*pb.SnapshotQueryAbortRecord, error)`; `(*consensusAdminService).AbortSnapshotQuery(ctx, *pb.AbortSnapshotQueryRequest) (*pb.SnapshotQueryStatus, error)`.

- [ ] **Step 1: Write the failing tests**

Create `server/snapshot_query_abort_test.go` driving the real gRPC client on the bufconn server with a `pb.NewConsensusAdminClient(conn)`; install the authority by setting the test FSM's `Params.AuthorityAddresses` to an `authority.NewSignerFromHex` address (through the same seam `newServerTestFSM`/`consensus` tests use, e.g. the params passed to `newServerTestFSM` or `fsm.TestingSetAuthorityAddresses` if one exists — record which) and `startServerConfig{maxTokenAge: time.Minute}` (already plumbed to `Cfg.ConsensusAdminMaxTokenAge` at server/server_test.go:187). Scenarios: (a) both RPCs are `FailedPrecondition` without the sequencing dependency and never reach Raft or a barrier; (b) candidate: `NotFound` before any query is sequenced; after acquire → submit through the ingress RPCs, the candidate returns the record with the sequenced block, input root, reservation id/generation, `prev_snapshot_id` = the watermark and a non-empty `next_snapshot_id`, `reason_code`/`cleanup_authorization_root` empty; a follower answers the barrier's not-leader code; (c) abort: sign the candidate with `reason_code "resource_exhausted"` → the reply is `found=true`, `lifecycle "terminal"`, `execution_outcome "aborted"`, `accepted` equal to the submit result, and `terminal_proof` that decodes as `wire.AbortSnapshotQuery` whose JWS `VerifySnapshotQueryAbort`s under the authority; `GetSnapshotQueryStatus` afterwards returns the identical status; a second identical abort returns the same status (idempotent); a `SubmitStatement` through the ingress RPC is accepted afterwards (the fence lifted); (d) before Raft: a token signed by an unlisted key is `PermissionDenied`; a stale token (`SignSnapshotQueryAbortAt` 48 h ago) is `PermissionDenied`; a record whose `next_snapshot_id` differs from the candidate is `FailedPrecondition` ("candidate"); an empty `reason_code` or non-empty `cleanup_authorization_root` is `InvalidArgument`; all with zero new applies; (e) a follower refuses the abort with the not-leader code before Raft; (f) a cross-repo pin test `TestSnapshotQueryTerminalVocabularyIsPinned`: `fsm.SnapshotQueryLifecycleTerminal == "terminal"`, `fsm.SnapshotQueryExecutionOutcomeAborted == "aborted"`, `authority.SnapshotQueryAbortPurpose == "housegate-snapshot-query-abort-v1"`, and `fsm.SnapshotQueryLifecycleSequenced == "sequenced"` unchanged.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //server:server_test --test_filter='TestAbortSnapshotQuery|TestGetSnapshotQueryAbortCandidate|TestSnapshotQueryTerminalVocabulary' --jobs=4 --test_output=errors`
Expected: the end-to-end cases FAIL with `Unimplemented`; the pin test fails to build until Task 3's constants are visible (they are — it passes once written; keep it).

- [ ] **Step 3: Implement the handlers**

Create `server/snapshot_query_abort.go`:

```go
package server

import (
	"context"
	"strings"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetSnapshotQueryAbortCandidate answers the replica-derived abort record for
// one account/statement after a leader barrier, or NotFound.
func (svc *consensusAdminService) GetSnapshotQueryAbortCandidate(ctx context.Context, req *pb.GetSnapshotQueryAbortCandidateRequest) (*pb.SnapshotQueryAbortRecord, error) {
	s := svc.s
	for _, f := range []struct{ name, value string }{{"network_id", req.GetNetworkId()}, {"client_account", req.GetClientAccount()}, {"statement_id", req.GetStatementId()}} {
		if strings.TrimSpace(f.value) == "" || strings.ContainsRune(f.value, 0) {
			return nil, status.Errorf(codes.InvalidArgument, "snapshot query abort candidate: %s is required and must be NUL-free", f.name)
		}
	}
	if req.GetKeeperShardId() != 0 {
		return nil, status.Error(codes.InvalidArgument, "snapshot query abort candidate: keeper_shard_id must be 0")
	}
	if _, err := s.snapshotQuerySequencingEnabled(); err != nil {
		return nil, err
	}
	if err := s.snapshotQueryNetworkScope(req.GetNetworkId()); err != nil {
		return nil, err
	}
	if err := s.consensusReadBarrier(ctx); err != nil {
		return nil, err
	}
	cand, ok := s.d.FSM.SnapshotQueryAbortCandidate()
	if !ok || cand.ClientAccount != strings.ToLower(req.GetClientAccount()) || cand.Record.StatementID != req.GetStatementId() {
		return nil, status.Error(codes.NotFound, "no consumed snapshot query reservation for this account and statement")
	}
	return wire.SnapshotQueryAbortRecordToPB(cand.Record), nil
}

// AbortSnapshotQuery proposes an authority-signed terminal abort after the
// local prechecks that make an unauthorized or stale request free for the
// quorum: record shape, the capability gate, leadership, equality with the
// replica-derived candidate and the token's freshness/allowlist. Apply
// re-verifies the signature deterministically; nothing decided here is
// replayed.
func (svc *consensusAdminService) AbortSnapshotQuery(ctx context.Context, req *pb.AbortSnapshotQueryRequest) (*pb.SnapshotQueryStatus, error) {
	s := svc.s
	if req.GetRecord() == nil {
		return nil, status.Error(codes.InvalidArgument, "snapshot query abort record is required")
	}
	rec := wire.SnapshotQueryAbortRecordFromPB(req.GetRecord())
	if err := authority.ValidateSnapshotQueryAbortRecord(rec); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if rec.CleanupAuthorizationRoot != "" {
		return nil, status.Error(codes.InvalidArgument, "cleanup_authorization_root has no contract yet and must be empty")
	}
	if len(strings.Split(req.GetAuthorityJws(), ".")) != 3 {
		return nil, status.Error(codes.InvalidArgument, "authority_jws must be a compact JWS")
	}
	if _, err := s.snapshotQuerySequencingEnabled(); err != nil {
		return nil, err
	}
	if err := s.consensusLeaderCheck(ctx); err != nil {
		return nil, err
	}
	cand, ok := s.d.FSM.SnapshotQueryAbortCandidate()
	want := cand.Record
	want.ReasonCode = rec.ReasonCode
	if !ok || want != rec {
		return nil, status.Error(codes.FailedPrecondition, "snapshot query abort record does not match the current candidate; fetch GetSnapshotQueryAbortCandidate again")
	}
	view, err := s.d.FSM.ConsensusParamsView()
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "consensus parameters unavailable: %v", err)
	}
	validator := authority.Validator{AllowedAddresses: authorityAddressSet(view.Current.AuthorityAddresses), MaxTokenAge: s.d.Cfg.ConsensusAdminMaxTokenAge}
	if _, err := validator.AuthorizeSnapshotQueryAbort(rec, req.GetAuthorityJws()); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "snapshot query abort authority: %v", err)
	}
	res, err := s.propose(ctx, wire.Command{AbortSnapshotQuery: &wire.AbortSnapshotQuery{Record: rec, AuthorityJWS: req.GetAuthorityJws()}})
	if err != nil {
		return nil, err
	}
	if _, ok := res.(fsm.Applied); !ok {
		return nil, status.Errorf(codes.Internal, "unexpected snapshot query abort result %T", res)
	}
	adm, found := s.d.FSM.SnapshotQueryStatusRead(cand.ClientAccount, rec.StatementID)
	if !found || adm.Lifecycle != fsm.SnapshotQueryLifecycleTerminal {
		return nil, status.Error(codes.Internal, "snapshot query abort committed but the admission is not terminal")
	}
	return wire.SnapshotQueryStatusToPB(snapshotQueryStatusFromAdmission(adm)), nil
}

// snapshotQueryStatusFromAdmission is the one place a status answer is built
// from committed sequencing facts, so the status RPC and the abort reply
// cannot disagree.
func snapshotQueryStatusFromAdmission(adm fsm.SnapshotQueryAdmissionState) replay.SnapshotQueryStatus {
	return replay.SnapshotQueryStatus{Version: snapshotQueryStatusVersion, Found: true, Accepted: adm.Result(), Lifecycle: adm.Lifecycle, ExecutionOutcome: adm.ExecutionOutcome, TerminalProof: adm.TerminalProof()}
}
```

Replace the `found=true` branch of `getSnapshotQueryStatus` (server/snapshot_query_submit.go) with `wire.SnapshotQueryStatusToPB(snapshotQueryStatusFromAdmission(adm))` (the identity-conflict check stays before it). `snapshotQueryNetworkScope` today takes the request's network id — keep its signature; if it takes a request struct, add the two-line overload and record it. A rejected proposal surfaces through `propose` as `InvalidArgument` with the FSM's reason (a stale candidate loses to a committed abort as "terminal"); no extra mapping is needed.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //server:server_test //fsm:fsm_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS (19 targets).

- [ ] **Step 5: Commit**

```bash
git add server/snapshot_query_abort.go server/snapshot_query_abort_test.go server/snapshot_query_submit.go server/BUILD.bazel server/server_test.go
git commit -m "feat(server): serve GetSnapshotQueryAbortCandidate and AbortSnapshotQuery behind the default-off dependency"
```

---

### Task 5: Documentation (arbiter)

**Files:**
- Modify: `docs/snapshot-query-reservations.md` (new section "Aborting a sequenced query", the "Not yet implemented" list, the "Snapshot format" section, the Tests section), `README.md` (the link sentence names the abort RPCs)

- [ ] **Step 1: Write the section**

One paragraph per line, verified against the code on the branch: the two `ConsensusAdmin` RPCs and their check order and codes (`InvalidArgument` shape → `FailedPrecondition` gate/leader/candidate mismatch → `PermissionDenied` authority → Raft; `NotFound` candidate); the authority purpose `housegate-snapshot-query-abort-v1`, its command domain, the clock-free Apply verification against `Params.AuthorityAddresses` and the age-enforced server authorization (`ConsensusAdminMaxTokenAge`); the replica-derived candidate and why the proposer chooses only `reason_code` (and `cleanup_authorization_root` must be empty); the atomic terminal (statement safe, no-op child with unchanged data/state roots, watermark, tombstone with the signed abort as terminal proof, barrier idle at generation+1, admission `terminal`/`aborted`); the status vocabulary (`sequenced` → `terminal`; `execution_outcome` `aborted`; `terminal_proof` = canonical JSON of the signed abort) and the fact that housegate's intake does not yet branch on it (Wave 3); the v2 chain resuming on the child; snapshot v12 and the pre-v12 refusal; `applyReleaseReservation` still refusing `consumed`; the deferred list (`reason_code` vocabulary, `cleanup_authorization_root` and exact-candidate settlement with C5, the applied terminal and receipts, the `output_rows_root` discrepancy between coordination plan line 230 and the frozen aborted receipt golden, endpoint authentication for status reads, the housegate sequencer client's terminal handling, the charged-lane route under `legacyMutationGuard`). Name the new test files.

- [ ] **Step 2: Commit**

```bash
git add docs/snapshot-query-reservations.md README.md
git commit -m "docs: describe the authorized snapshot query abort and its aborted no-op child"
```

---

## Deferred (recorded, not placeholders)

- `reason_code` vocabulary (only `resource_exhausted` is observed in the frozen fixture) and `cleanup_authorization_root` semantics: both wait for C5's exact-candidate settlement; this wave fails closed on a non-empty cleanup root.
- The applied terminal (`execution_outcome = "applied"`, receipts with `abort_record_root == ""`) and the receipt's `output_rows_root` for aborts — coordination plan line 230 says `""`, the frozen `aborted_receipt` golden carries a non-empty root; resolve when receipts land (a fixture edit must bump arbiter-core's pinned SHA in the same change).
- housegate's `recoverRecord` treats any `found=true` as a `sequenced` success and ignores `lifecycle`/`execution_outcome`/`terminal_proof`; the Wave 3 sequencer client must surface `terminal`/`aborted` as a query failure at every ACK level (design D9: "abort produces query failure at every ACK level").
- Endpoint authentication for status and candidate reads (D3), before the read barrier.
- 1a-3: the query obligation / Q-family transfer and the charged consume/abort route (both `SubmitSnapshotQuery` and `AbortSnapshotQuery` are rejected by `legacyMutationGuard` under an enabled capacity ledger).
- `GetL3Block`'s representation of query blocks and the arbiter-proto `query_statement_root` header field (unchanged by this wave; an aborted query block is still refused by `L3BlockView`).

## Subsequent plans

| Plan | Scope | Written when |
|---|---|---|
| 1a-3 — C1 remaining | four disposition actions, `Capability` enable command, capacity charging (submit and abort through the charged path), Raft 26/27, leader-Barrier publication/history reads | after this plan merges |
| Wave 2 / 3 | source execution and the applied terminal (C5/B5); housegate sequencer client with terminal handling (T3.1) | per the TODO plan |

## Execution amendments (2026-09-21)

Recorded during subagent-driven execution (arbiter-core #35 Task 1, arbiter-proto #10 Task 2, arbiter #103 Task 3, arbiter #104 Task 4, Task 5 docs). Each item is a ruling made against the plan text above; the merged code is authoritative where the two differ.

- **Task 1 (arbiter-core).** The brief's validation table omitted the `cleanup_authorization_root` NUL guard; it is covered now, the negative tests check their signing errors, purpose separation is asserted in both directions, and the first-failing-field ordering is pinned. `testSigner` is a new helper (the package had none).
- **Task 2 (arbiter-proto).** `conformance/consensus_test.go`'s `TestConsensusAdminRPCSignatures` freezes the `ConsensusAdmin` surface and was extended from 3 to 5 methods with the two new signature checks; the unimplemented-services table resolves `ConsensusAdmin` through `File_consensus_proto`; the historical descriptor baselines are untouched. Pre-existing gap recorded: `TestSnapshotQueryLedgerCoversEveryNewMessage` does not scan `consensus.proto`.
- **Task 3 (arbiter fsm).** The brief's restore rule (a `sequenced` admission's statement is `StatusSequenced`) stays strict; the hole it exposed — `MarkReplaying` and `OpenChallenge` were `Applied` on a query block and would have left an unrestorable snapshot — is closed at the source: the block-addressed `MarkReplaying`, `OpenChallenge` and `ResolveChallenge` refuse a query block through one `refuseQueryBlock` that reads the sealed header's marker, and the statement-addressed `RecordPromotionIssued` and `RecordPromotionAck` through its sibling `refuseQueryStatements`; anchor finality is deliberately unguarded; the threeway bail-out is documented as load-bearing. The brief's "tampered signature" tamper only flipped the recovery byte (inert ~12%, flaky) and now rewrites the first signature character; "empty reason" is a post-signing tamper; tests set `BootstrapParams.AuthorityAddresses` as well; the promotion-path guards are driven through the real commands (a seeded `PendingPromotion` for the ack, since no legitimate issuance can cover a query statement); `SnapshotQueryStatusRead` deep-copies `Abort`; the two post-clone lookups carry defensive refusals; an unknown lifecycle is refused before the pre-v12 gate at every container version. Design D9's "a retry after abort needs a new statement id" is enforced: a submit for a statement with a terminal admission returns admission code 2 ("snapshot query statement identity is consumed by a terminal outcome; retry with a new statement id"), so a re-granted reservation cannot be parked behind a stale replay.
- **Task 4 (arbiter server).** RPC-level idempotency needed a lookup the candidate cannot provide once the barrier is idle: the additive `fsm.SnapshotQueryTerminalRead(statementID, inputRoot)` lets `AbortSnapshotQuery` answer a re-sent identical abort (same record root and authority JWS) from committed state without a Raft round and refuse a different abort for a terminal statement (`FailedPrecondition`); the candidate RPC answers `FailedPrecondition` "already terminal" instead of `NotFound` (account-keyed, since its request carries no input root); the idempotent reply precedes the leader check (a committed terminal admission is immutable; a lagging follower falls through to the leader check). The same "already terminal" condition lost to a concurrent abort surfaces through `propose` as `InvalidArgument`. Not taken: a follower-with-terminal ordering test and a candidate/FSM drift-fencing test.
- **Carried to later slices.** `reason_code` vocabulary and `cleanup_authorization_root` (with C5's settlement); the applied terminal and the receipt `output_rows_root` discrepancy; endpoint authentication for status/candidate reads before the barrier; housegate's terminal handling (today `recoverRecord` reports an aborted query as a `sequenced` success); the charged-lane route for submit and abort under `legacyMutationGuard`; `GetL3Block`'s representation of query blocks.
