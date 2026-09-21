# Signed INSERT ... SELECT — Wave 1a-2: Singleton Sequencing and Status (C3, part 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `SubmitSnapshotQuery` and `GetSnapshotQueryStatus` real in arbiter: a signed v3 envelope is verified deterministically inside the FSM, consumes its granted reservation atomically into a singleton L3 block, gets a statement sequence, block sequence and source assignment, and can be looked up by exact original-JWS identity — all behind the default-off dependency that Wave 1a-1 introduced. Abort (Raft tag 22, the `snapshot-query-abort-v1` record and the aborted no-op child manifest) is 1a-2b.

**Architecture:** The v2 block chain is reused, not forked: a snapshot-query statement is a `StatementState` with `Kind = replay.SnapshotQueryStatementKind` and the v3 envelope in a new `Query` field, and its block is an ordinary `L3BlockHeader` with `StatementCount = 1` whose `StatementsRoot` is housegate's `SnapshotQueryStatementRoot`; a new `omitempty` header field marks it so every v2 chain hash stays byte-identical. Every reader that walks a block's statements for v2 work (dispatch, custody, work views, three-way verdicts) skips query statements; the safe prefix stalls at a query block until its own terminal (C5 / 1a-2b), which is design D1's "no safe publication overtakes the reserved block". The sequencing facts are also kept in a disposition-state container keyed by account and statement ID so status reads and identity conflicts need no block walk. `Apply` dispatches `SubmitSnapshotQueryCmd` (tag 21) through the same replica-derived validation style as C2. The server authenticates freshness and allowlist before proposing and reads status after a leader barrier. Snapshot format moves to v11 for the new fields.

**Tech Stack:** Go 1.26, Bazel 9.1.0 (`bazel run //:gazelle`), hashicorp/raft via `raftnode.ConsensusNode`, arbiter-core `wire` (protobuf `RaftCommand`, `SnapshotQueryEnvelopeFromPB`, `SnapshotQuerySubmitResultToPB`, `SnapshotQueryStatusToPB`), housegate `pkg/replay` (`SnapshotQueryStatementRoot`, `DigestString`, types), `pkg/replay/snapshotquery.VerifyEnvelope`, `pkg/auth` (`EthValidator.ValidateStatementV3`).

**Spec:** [coordination plan Task C3](2026-09-16-signed-insert-select-coordination.md) (interfaces, the `applySubmitSnapshotQuery` profile check, Step 1–4 tests); [design D1/D2/D9](../specs/2026-09-16-signed-insert-select-design.md); [master plan Global Constraints](2026-09-16-signed-insert-select.md); [Wave 1a-1 plan](2026-09-21-signed-insert-select-wave1a-1-c2.md) and its execution amendments (the C2 code this builds on).

## Global Constraints

- Tier 3 of [issue #153](https://github.com/housegate/housegate/issues/153): merging enables no runtime capability; `cmd/arbiter` must not set `server.Deps.SnapshotQueryReservations`; every new RPC path is gated by that dependency.
- Submission consumes the granted reservation atomically into a singleton block; the FSM requires both signed profile IDs to equal the reservation's pair and the active policy pair, and refuses a different snapshot pin, schema, account, statement ID or fencing generation before any state change. A valid repeated submission returns its existing result; a changed input root or JWS under the same statement ID is an identity conflict, never a second sequence. No v2 statement shares the query block.
- `statement_seq`, block sequence and source identity are assigned in apply, deterministically (`NextStatementSeq`, `NextL3BlockSeq`, `selectSource`), never by the proposer; the FSM performs no I/O and no clock read; the pure `snapshotquery.VerifyEnvelope` (housegate, pinned at ≥ `48ccc08a463b`) re-verifies the signature inside apply, freshness only on the server.
- Status is keyed by network / shard / account / statement ID and requires `expected_input_root` and `expected_user_jws_hash` (tag 6, `replay.DigestString` of the exact original compact JWS); a mismatch is a conflict (`FailedPrecondition`), never `found=false` and never another signature's result; `found=false` only when no submission exists; `version=1`.
- Admission codes follow `arbiter.AdmissionCode`: accepted = 1; terminal rejections use 2..8 (housegate treats 2..8 as terminal and anything else as retry-later).
- Frozen bytes: `L3BlockHeader`'s JSON is the chain-hash preimage, so every new header field is `omitempty` and the golden test `fsm/l3_commitment_golden_test.go` must pass unchanged; `StatementState` gains only `omitempty` fields.
- Snapshot format: current v10; this plan bumps to v11 following the established recipe (constants, accept list + error text, allowance predicate, BootstrapParams predicate, disposition-restore predicate, per-field validators), with a fixed-boundary scrub for the new container and a restore validator for query statements.
- Tests: `bazel test //fsm:fsm_test //server:server_test --jobs=4` per task; `bazel test //...` (19 targets) before each PR; rejections asserted with `strings.Contains(got.(fsm.Rejected).Reason, …)` or the typed result; successes with `reflect.DeepEqual`.
- Conventions: English comments; conventional commit subjects; commits end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; PR bodies end with `Refs https://github.com/housegate/housegate/issues/153`, `Design: https://github.com/housegate/housegate/pull/154` and `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. One isolated worktree (`fix/153-wave1a-c3`), never the main checkouts; merge only on green CI, squash with branch deletion; task review precedes every merge; single serial lane (uranuswch's decision 4).

## Baseline facts (arbiter `origin/main` after PR #98, 2026-09-21)

- `wire.Command.SubmitSnapshotQuery *wire.SubmitSnapshotQuery{Envelope replay.SnapshotQueryEnvelope; NonMembershipProof []byte}` (arbiter-core `wire/snapshot_query.go:1170`) is encodable but has no `FSM.Apply` case (`fsm/fsm.go` falls to `Rejected{"empty command"}`); `SubmitSnapshotQuery`/`GetSnapshotQueryStatus` fall through `pb.UnimplementedArbiterIngressServer` (`server/ingress.go:23`). `NonMembershipProof` has no defined semantics anywhere.
- Block model: `OpenL3Block{StatementSeqStart, StatementSeqs}` (`fsm/state.go:146`); `L3BlockHeader{L3BlockSeq, PrevL3Hash, StatementSeqStart, StatementCount uint32, SchemaSnapshotID, ExecutorProfileID, PrevSafeSnapshotID, PrevStateRoot, SpentIDsRootAfter, StatementsRoot, L2AnchorRef}` (`:153`), `ChainHash()` (`:173`); `StatementState{Env arbiter.StatementEnvelope; Seq; BlockSeq; SourceNode; Status; RC; UnpromotedParts}` (`:132`); `applySealL3Block` (`fsm/apply.go:161-228`: prev hash, `statementsRoot = replay.CanonicalDigest(arbiter.DomainL3Statements, envs)`, `SpentIDsRootAfter`, `Verifications[seq]`, `NextL3BlockSeq++`, `OpenBlock` reset, `EventBlockSealed`, returns `SealResult{BlockSeq, ChainHash}`); `blockStatements`/`blockStatementsComplete` (`fsm/apply.go:109,133`) read `Statements[StatementSeqStart .. +StatementCount)`; readers: `fsm/apply.go:156`, `genesis_validation.go:169`, `pinned_state.go:16`, `reads_custody.go:35`, `reads_dispatch.go:39`, `reads_work.go:49,81` (`safePrefixLocked` requires every statement `StatusSafe`), `threeway.go:50`.
- Admission: `applySubmitStatement` (`fsm/admission.go:17`) assigns `seq := NextStatementSeq; NextStatementSeq++`, `SourceNode: f.selectSource(flat)` (`fsm/select.go:44`), `ByStatementID[flat] = seq`, appends to `OpenBlock.StatementSeqs`; the C2 fence (`snapshotQueryBarrierFencesAdmission`, `:81`) refuses new v2 admissions while the barrier is draining/granted/consumed/resolving, so the open block is empty from grant onward.
- Reservation state after C2: barrier `{State, Generation, RequestID, ClientAccount, StatementID, Reservation, BlockSeq}`; `Reservations[key]` with `State == granted` and `Reservation *replay.SnapshotQueryReservation{ReservationID, FencingGeneration, ClientAccount, StatementID, ReadSnapshot, ExecutorProfileID, QueryProfileID, ActivationID}`; `putArtifactDispositionReservationBarrier` (`fsm/artifact_reservation_state.go:235-289`) whitelists `grantToConsumed` at the same generation; `ArtifactReservationConsumed`/`Resolving` constants exist; `applyReleaseReservation` refuses consumed records (a consumed reservation's exit is 1a-2b's abort or C5's applied terminal); `QueryPolicyReadiness.Policy` is the active pair; `f.st.Params.NetworkID`.
- housegate contracts: `replay.SnapshotQueryBinding` (22 fields incl. `NetworkID, KeeperShardID, ReadSnapshot SnapshotPin, SchemaSnapshotID, SchemaRoot, QueryProfileID, ExecutorProfileID, ReservationID, FencingGeneration`; no `ActivationID`), `SnapshotQueryEnvelope{Input{Binding, SQL, ReadSet}, InputRoot, UserJWS}`, `SnapshotQueryStatement{StatementSeq, Envelope}`, `SnapshotQueryStatementRoot(st) (string, error)` (recomputes and checks the input root, requires `StatementSeq != 0` and non-empty JWS), `SnapshotQuerySubmitResult{AdmissionCode uint32, Message, StatementSeq, BlockSeq, SourceNode, InputRoot, Reservation}`, `SnapshotQueryStatus{Version, Found, Accepted, Lifecycle string, ExecutionOutcome string, TerminalProof []byte}` (no lifecycle vocabulary defined anywhere — this plan defines `sequenced`), `snapshotquery.VerifyEnvelope(env) (account string, err error)` (validate complete input, recompute input root, pure signature check, account equality), `auth.EthValidator.ValidateStatementV3(token, auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding, InputRoot}) (string, error)` (freshness + allowlist). housegate's intake accepts a result only when `AdmissionCode == 1`, `InputRoot`, `StatementSeq != 0`, `BlockSeq != 0`, `SourceNode != ""` and the reservation equals the signed binding on ID, generation, account, statement, pin and both profiles (`validateSnapshotQueryAccepted`, `pkg/storageintegrity/snapshot_query_intake.go:357`).
- Server: `propose` maps `fsm.Rejected` → `InvalidArgument`; `consensusLeaderCheck`, `consensusReadBarrier`; `server/snapshot_query_reservations.go` holds `SnapshotQueryReservationDeps{Enabled, Control, AcquireWait, GrantInterval}`, `NewHousegateSnapshotQueryControlAuthorizer`, `readReservationView`; harness `startServerWithConfig(startServerConfig{f, leader, custody, snapshotQueryReservations, …})`, `publishServerGenesis`, `mustDirectApply`, `signedControl`, `enabledReservationDeps`, `activateServerTestQueryPolicy`; the end-to-end acquire→grant path is `TestAcquireGrantsImmediatelyWhenTheFrontierIsPublished` (`server/snapshot_query_reservations_test.go`).
- fsm tests: `newTestFSM`, `applyCmd(t, f, index, cmd)`, `snapshotQueryTestSigner`, `signedAcquireRequest`, `activateTestQueryPolicy`, `f.SnapshotQueryGrantCandidate()`, `snapshotBytes`/`restoreInto`, `rewriteSnapshotDocument`; a granted reservation end to end is begin → activate → candidate → grant (`TestGrantSnapshotQueryRequiresPublishedFrontierAndActivePolicy`).

## Design decisions (rulings carried into the tasks)

1. **Query statements live in `Statements` and query blocks in `Blocks`.** The alternative (a side table outside the chain) cannot advance `SafeWatermark`, whose publication validator ties `SafeBlockSeq` to `safePrefixLocked` over `Blocks`. Cost: seven v2 readers must skip `Kind == 2`; the plan enumerates each with its required behavior.
2. **Capacity-free path only.** With `Capacity == nil` (the state every deployment has), the submit transition consumes the reservation directly (record → consumed, barrier → consumed) and records a `SnapshotQueryAdmission` in disposition state; the Q-family transfer journal and the `query` obligation belong to 1a-3 with the capacity ledger. `TestingActivateQueryPolicy` remains the test-only policy source until C1's activation command.
3. **Deterministic rejections carry an admission code.** Signature/binding failures → 5 (`INVALID_SIGNATURE`); reservation/pin/profile/policy mismatch → 6 (`INVALID_PROOF`); structural (empty SQL, non-empty `NonMembershipProof`, wrong network/shard) → 7 (`MALFORMED`); same statement ID with a different input root or JWS → 2 (`DUPLICATE_CLIENT_SEQ`). "Not grantable right now" (barrier not granted for this account/statement) is a plain `Rejected` the server maps to `Unavailable` so the client retries after lookup.
4. **Status identity is the read's authentication.** The RPC carries no token; requiring the exact input root and the digest of the original compact JWS (which only the signer's durable journal holds) is the C3-mandated identity check; endpoint authentication is D3's. Lifecycle vocabulary: `sequenced` (this plan), `applied` and `aborted` (later).
5. **`NonMembershipProof` must be empty** until a contract defines it (fail closed, code 7).

Execution order: Task 1 → 2 → 3 → 4 → 5, one PR per task (Task 1 alone because it touches every block reader; Tasks 2–3 may be one PR at the executor's discretion). Each task ends with `bazel test //fsm:fsm_test //server:server_test --jobs=4` green and gofmt clean.

---

### Task 1: Query statement model, singleton block seal helper and snapshot v11 (fsm)

**Files:**
- Modify: `fsm/state.go` (`StatementState`, `L3BlockHeader`, `ArtifactDispositionState`), `fsm/snapshot.go` (v11), `fsm/genesis_validation.go:160-180`, `fsm/reads_dispatch.go:30-60`, `fsm/reads_work.go:40-100`, `fsm/reads_custody.go:25-50`, `fsm/pinned_state.go:10-40`, `fsm/threeway.go:40-70`, `fsm/apply.go:150-160`
- Create: `fsm/snapshot_query_block.go`
- Test: `fsm/snapshot_query_block_test.go`, `fsm/snapshot_test.go` (append), `fsm/l3_commitment_golden_test.go` (must pass unchanged)

**Interfaces:**
- Produces: `StatementState.Kind uint32 json:"kind,omitempty"` (0 = v2 INSERT, `replay.SnapshotQueryStatementKind` = 2 = snapshot query) and `StatementState.Query *replay.SnapshotQueryEnvelope json:"query,omitempty"`; `L3BlockHeader.QueryStatementRoot string json:"query_statement_root,omitempty"`; `func (s *StatementState) IsSnapshotQuery() bool`; `type SnapshotQueryAdmissionState struct { ClientAccount, StatementID, RequestID string; ReservationKey string; StatementSeq, BlockSeq uint64; SourceNode, InputRoot, OriginalJWSHash, StatementRoot string; Lifecycle string; Reservation replay.SnapshotQueryReservation; CommitIndex uint64 }` stored in `ArtifactDispositionState.QueryAdmissions map[string]*SnapshotQueryAdmissionState json:"query_admissions,omitempty"` keyed by `snapshotQueryAdmissionKey(account, statement) = account + "\x00" + statement`; `const SnapshotQueryLifecycleSequenced = "sequenced"`; `func (f *FSM) sealSnapshotQueryBlockLocked(seq uint64, env replay.SnapshotQueryEnvelope, source string) (blockSeq uint64, statementRoot string, err error)`; `snapshotVersion = 11`, `snapshotVersionV11`, `validateSnapshotQueryStatements(st *State) error`.

- [ ] **Step 1: Write the failing tests**

Create `fsm/snapshot_query_block_test.go`:

```go
package fsm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func testQueryEnvelope(t *testing.T, statementID string) replay.SnapshotQueryEnvelope {
	t.Helper()
	// A structurally complete, unsigned envelope is enough for the block helper; signing is Task 2's concern.
	env := replay.SnapshotQueryEnvelope{Input: replay.SnapshotQueryInput{Binding: replay.SnapshotQueryBinding{EnvelopeVersion: replay.SnapshotQueryEnvelopeVersion, InputKind: replay.SnapshotQueryInputKind, StatementKind: replay.SnapshotQueryStatementKind, ClientAccount: "0x0000000000000000000000000000000000000001", StatementID: statementID, NetworkID: testParams().NetworkID}, SQL: "INSERT INTO db.t SELECT value FROM db.t"}, UserJWS: "h.p.s"}
	root, err := replay.SnapshotQueryInputRoot(env.Input)
	if err != nil {
		t.Fatal(err)
	}
	env.InputRoot = root
	return env
}

func TestSealSnapshotQueryBlockAppendsASingletonBlockWithTheStatementRoot(t *testing.T) {
	f := newTestFSM(t)
	before := len(f.st.Blocks)
	env := testQueryEnvelope(t, "statement-1")
	seq := f.st.NextStatementSeq
	f.st.NextStatementSeq++
	f.st.Statements[seq] = &StatementState{Kind: replay.SnapshotQueryStatementKind, Query: &env, Seq: seq, Status: StatusSequenced, SourceNode: "source-a"}
	blockSeq, root, err := f.sealSnapshotQueryBlockLocked(seq, env, "source-a")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := replay.SnapshotQueryStatementRoot(replay.SnapshotQueryStatement{StatementSeq: seq, Envelope: env})
	if len(f.st.Blocks) != before+1 || blockSeq != uint64(before+1) || root != want {
		t.Fatalf("blocks=%d blockSeq=%d root=%s want=%s", len(f.st.Blocks), blockSeq, root, want)
	}
	h := f.st.Blocks[len(f.st.Blocks)-1]
	if h.StatementSeqStart != seq || h.StatementCount != 1 || h.StatementsRoot != want || h.QueryStatementRoot != want || h.L3BlockSeq != blockSeq {
		t.Fatalf("header = %+v", h)
	}
	if f.st.Statements[seq].BlockSeq != blockSeq || f.st.OpenBlock == nil || len(f.st.OpenBlock.StatementSeqs) != 0 || f.st.OpenBlock.StatementSeqStart != f.st.NextStatementSeq || f.st.NextL3BlockSeq != blockSeq+1 {
		t.Fatalf("post-seal state: stmt=%+v open=%+v next=%d", f.st.Statements[seq], f.st.OpenBlock, f.st.NextL3BlockSeq)
	}
	if v := f.st.Verifications[blockSeq]; v == nil || !reflect.DeepEqual(v.SourceNodes, []string{"source-a"}) {
		t.Fatalf("verification = %+v", v)
	}
	// The query block stalls the safe prefix until its own terminal.
	if got := safePrefixLocked(f); got != uint64(before) {
		t.Fatalf("safe prefix = %d, want %d", got, before)
	}
	stmts, complete := f.blockStatementsComplete(blockSeq)
	if !complete || len(stmts) != 1 || !stmts[0].IsSnapshotQuery() {
		t.Fatalf("block statements: complete=%v n=%d", complete, len(stmts))
	}
}

func TestSealSnapshotQueryBlockRefusesANonEmptyOpenBlockAndAMissingStatement(t *testing.T) {
	f := newTestFSM(t)
	env := testQueryEnvelope(t, "statement-1")
	if _, _, err := f.sealSnapshotQueryBlockLocked(f.st.NextStatementSeq, env, "source-a"); err == nil || !strings.Contains(err.Error(), "statement") {
		t.Fatalf("missing statement: %v", err)
	}
	if _, ok := f.applySubmitStatement(testSubmitStatement(t)).(SubmitResult); !ok {
		t.Fatal("seed v2 statement not admitted")
	}
	seq := f.st.NextStatementSeq
	f.st.NextStatementSeq++
	f.st.Statements[seq] = &StatementState{Kind: replay.SnapshotQueryStatementKind, Query: &env, Seq: seq, Status: StatusSequenced}
	if _, _, err := f.sealSnapshotQueryBlockLocked(seq, env, "source-a"); err == nil || !strings.Contains(err.Error(), "open block") {
		t.Fatalf("non-empty open block: %v", err)
	}
}

func TestQueryStatementsAreInvisibleToV2WorkViews(t *testing.T) {
	f := newTestFSM(t)
	env := testQueryEnvelope(t, "statement-1")
	seq := f.st.NextStatementSeq
	f.st.NextStatementSeq++
	f.st.Statements[seq] = &StatementState{Kind: replay.SnapshotQueryStatementKind, Query: &env, Seq: seq, Status: StatusSequenced, SourceNode: "source-a"}
	if _, _, err := f.sealSnapshotQueryBlockLocked(seq, env, "source-a"); err != nil {
		t.Fatal(err)
	}
	// Each v2 reader must ignore the query statement rather than treat it as INSERT work.
	assertNoV2WorkForQueryStatement(t, f, seq) // defined in Step 3 per reader
	data := snapshotBytes(t, f)
	if data[4] != snapshotVersionV11 {
		t.Fatalf("version byte = %d, want 11", data[4])
	}
	g := restoreInto(t, data)
	if s := g.st.Statements[seq]; s == nil || !s.IsSnapshotQuery() || s.Query == nil || s.Query.InputRoot != env.InputRoot {
		t.Fatalf("query statement did not survive restore: %+v", s)
	}
	// A v10 container (no query fields) still restores.
	legacy := snapshotBytes(t, newTestFSM(t))
	legacy[4] = snapshotVersionV10
	if got := restoreInto(t, legacy); got == nil {
		t.Fatal("v10 restore failed")
	}
}
```

`assertNoV2WorkForQueryStatement(t, f, seq)` is written in Step 3 after the readers are changed: it calls each reader's public view (`DispatchView`-style read in `reads_dispatch.go`, the work view in `reads_work.go`, the custody read in `reads_custody.go`, the three-way input in `threeway.go`) for the query block and asserts the query statement is absent from every v2 work list while `blockStatementsComplete` still reports the block complete. Name the exact view functions after reading each file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestSealSnapshotQueryBlock|TestQueryStatementsAreInvisible' --jobs=4 --test_output=errors`
Expected: build FAILS on the undefined `Kind`/`Query`/`IsSnapshotQuery`/`sealSnapshotQueryBlockLocked`/`snapshotVersionV11` symbols.

- [ ] **Step 3: Implement the model, the helper, the readers and the bump**

In `fsm/state.go`: add to `StatementState` after `Env`:

```go
	// Kind distinguishes a v2 INSERT (0) from a snapshot query
	// (replay.SnapshotQueryStatementKind). Query statements carry the signed v3
	// envelope in Query and leave Env zero; v2 readers skip them.
	Kind  uint32                        `json:"kind,omitempty"`
	Query *replay.SnapshotQueryEnvelope `json:"query,omitempty"`
```

and `func (s *StatementState) IsSnapshotQuery() bool { return s != nil && s.Kind == replay.SnapshotQueryStatementKind }`. Add to `L3BlockHeader` after `StatementsRoot`: `QueryStatementRoot string \`json:"query_statement_root,omitempty"\`` with the comment that it is set only for singleton query blocks and stays empty (omitted from the chain-hash preimage) for v2 blocks. Add `SnapshotQueryAdmissionState`, `QueryAdmissions` (after `QueryAttestations` in `ArtifactDispositionState`, initialised to an empty map in `newState` and `ensureDispositionMaps`), `snapshotQueryAdmissionKey`, and `SnapshotQueryLifecycleSequenced`.

Create `fsm/snapshot_query_block.go`:

```go
package fsm

import (
	"errors"
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
)

// sealSnapshotQueryBlockLocked appends the singleton block that carries one
// already-stored snapshot-query statement. It mirrors applySealL3Block for
// the chain fields (previous hash, spent-IDs root, schema/executor identity,
// predecessor snapshot) but the statements root is housegate's
// SnapshotQueryStatementRoot, and the header marks the block through
// QueryStatementRoot. The open block must be empty: admissions are fenced from
// drain begin, so a non-empty open block means the caller bypassed the fence.
func (f *FSM) sealSnapshotQueryBlockLocked(seq uint64, env replay.SnapshotQueryEnvelope, source string) (uint64, string, error) {
	stmt := f.st.Statements[seq]
	if stmt == nil || !stmt.IsSnapshotQuery() || stmt.Query == nil {
		return 0, "", errors.New("snapshot query statement is not stored")
	}
	if f.st.OpenBlock != nil && len(f.st.OpenBlock.StatementSeqs) != 0 {
		return 0, "", errors.New("open block is not empty; new SI admissions were not fenced")
	}
	root, err := replay.SnapshotQueryStatementRoot(replay.SnapshotQueryStatement{StatementSeq: seq, Envelope: env})
	if err != nil {
		return 0, "", fmt.Errorf("snapshot query statement root: %w", err)
	}
	blockSeq := f.st.NextL3BlockSeq
	header := L3BlockHeader{
		L3BlockSeq: blockSeq, StatementSeqStart: seq, StatementCount: 1,
		SchemaSnapshotID: f.st.Params.SchemaSnapshotID, ExecutorProfileID: f.st.Params.ExecutorProfileID,
		StatementsRoot: root, QueryStatementRoot: root,
	}
	// Previous hash, predecessor snapshot fields and SpentIDsRootAfter follow applySealL3Block exactly (fsm/apply.go:161-228): copy those assignments here rather than calling it, because it consumes the open block.
	f.fillSealedHeaderChainFieldsLocked(&header)
	stmt.BlockSeq = blockSeq
	f.st.Blocks = append(f.st.Blocks, header)
	f.st.Verifications[blockSeq] = &BlockVerification{BlockSeq: blockSeq, SourceNodes: []string{source}}
	f.st.NextL3BlockSeq++
	f.st.OpenBlock = &OpenL3Block{StatementSeqStart: f.st.NextStatementSeq}
	f.emit(Event{Kind: EventBlockSealed, BlockSeq: blockSeq})
	return blockSeq, root, nil
}
```

`fillSealedHeaderChainFieldsLocked(h *L3BlockHeader)` is extracted from `applySealL3Block` in this task: move the code that sets `PrevL3Hash`, `PrevSafeSnapshotID`, `PrevStateRoot` and `SpentIDsRootAfter` into it and call it from both sealers, so the two cannot drift (the v2 sealer's other work — statements root over envelopes, sorting source unions, stamping `ss.BlockSeq` — stays in place). The `Event` literal must match the existing `emit(Event{Kind: EventBlockSealed, …})` call in `applySealL3Block`; copy its exact fields.

Readers — apply this rule at each site and record every hunk in the report:

| Site | Required behavior |
|---|---|
| `fsm/apply.go:156` (v2 seal loop over `blockStatements`) | unchanged: a v2 open block never contains a query statement (the fence); add a defensive `if ss.IsSnapshotQuery() { return Rejected{Reason: "query statement in a v2 block"} }` |
| `fsm/genesis_validation.go:169` (restore) | accept `Kind == 2` statements: `Query != nil`, `Env` zero, `BlockSeq != 0`, and the block's `QueryStatementRoot == StatementsRoot` — implemented as `validateSnapshotQueryStatements`, called from `readSnapshot` after the disposition validators |
| `fsm/pinned_state.go:16` (verifier pinned state) | skip query statements (B5/C5 pin them through the query job, not the v2 pinned state) |
| `fsm/reads_custody.go:35` | skip (no payload custody for a query) |
| `fsm/reads_dispatch.go:39` | skip (sources must never receive a query as INSERT work) |
| `fsm/reads_work.go:49` (`safePrefixLocked`) | unchanged: `Status != StatusSafe` stalls the prefix — intended until the query terminal; `:81` skip in work lists |
| `fsm/threeway.go:50` | skip (no three-way verdict for a query block; its verification is B5's) |

Snapshot v11 in `fsm/snapshot.go`: `snapshotVersion = 11`, `snapshotVersionV11 = 11`, keep `snapshotVersionV10`; extend the accept list and error text, the allowance-validation predicate, the BootstrapParams predicate and the disposition-restore predicate with v10; add the scrub `if ver[0] < snapshotVersionV11 { st.ArtifactDisposition.QueryAdmissions = nil }` next to the other fixed-boundary scrubs; call `validateSnapshotQueryStatements` (statements) and `validateSnapshotQueryAdmissions` (every admission's `StatementSeq` names a query statement with the same `InputRoot`, `BlockSeq` matches, `ReservationKey` names a consumed record or a tombstone, `Lifecycle` is a known value) after the existing validators. Update `cmd/arbiter/storage_protocol_test.go`'s version byte and the Wave 1a-1 tests that assert `snapshotVersionV10` (they now assert `snapshotVersionV11` for current containers; the v9 frontier-refusal fixtures stay v9).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS, including `fsm/l3_commitment_golden_test.go` unchanged (v2 headers omit the new field) and every existing seal/promotion/three-way test.

- [ ] **Step 5: Commit**

```bash
git add fsm/state.go fsm/snapshot.go fsm/snapshot_query_block.go fsm/snapshot_query_block_test.go fsm/snapshot_test.go fsm/apply.go fsm/genesis_validation.go fsm/pinned_state.go fsm/reads_custody.go fsm/reads_dispatch.go fsm/reads_work.go fsm/threeway.go fsm/BUILD.bazel cmd/arbiter/storage_protocol_test.go
git commit -m "feat(fsm): model snapshot-query statements and singleton query blocks; snapshot format v11"
```

---

### Task 2: Dispatch `SubmitSnapshotQuery` — verify, consume the grant, sequence, seal (fsm)

**Files:**
- Create: `fsm/apply_snapshot_query_submit.go`
- Modify: `fsm/fsm.go` (add the `case cmd.SubmitSnapshotQuery != nil` arm), `fsm/BUILD.bazel` (gazelle adds `@housegate//pkg/replay/snapshotquery`)
- Test: `fsm/apply_snapshot_query_submit_test.go`

**Interfaces:**
- Consumes: `snapshotquery.VerifyEnvelope`, `sealSnapshotQueryBlockLocked`, `putArtifactDispositionReservationBarrier`, `f.selectSource`, `snapshotQueryAdmissionKey`, `SnapshotQueryAdmissionState`.
- Produces: `type SnapshotQuerySubmitApplied struct { Result replay.SnapshotQuerySubmitResult }`; `type SnapshotQuerySubmitRejected struct { Code uint32; Message string }` (terminal, code 2..8); `func (f *FSM) applySubmitSnapshotQuery(c *wire.SubmitSnapshotQuery, index uint64) any`; `func (f *FSM) SnapshotQueryStatusRead(account, statement string) (SnapshotQueryAdmissionState, bool)` (read lock, detached copy).

- [ ] **Step 1: Write the failing tests**

Create `fsm/apply_snapshot_query_submit_test.go`:

```go
package fsm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/wire"
)

// grantedReservation runs begin -> activate -> grant and returns the granted reservation.
func grantedReservation(t *testing.T, f *FSM, s *auth.RelaySigner, statementID, requestID string) replay.SnapshotQueryReservation {
	t.Helper()
	if got := applyCmd(t, f, 41, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: signedAcquireRequest(t, s, statementID, requestID)}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	activateTestQueryPolicy(f)
	cand, ok := f.SnapshotQueryGrantCandidate()
	if !ok {
		t.Fatal("no grant candidate")
	}
	if got := applyCmd(t, f, 42, wire.Command{GrantSnapshotQuery: &wire.GrantSnapshotQuery{RequestID: cand.RequestID, Reservation: cand.Reservation}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("grant = %#v", got)
	}
	return cand.Reservation
}

// signedQueryEnvelope binds the signer's account to the granted reservation and signs it with the v3 statement purpose.
func signedQueryEnvelope(t *testing.T, s *auth.RelaySigner, r replay.SnapshotQueryReservation) replay.SnapshotQueryEnvelope {
	t.Helper()
	b := replay.SnapshotQueryBinding{EnvelopeVersion: replay.SnapshotQueryEnvelopeVersion, InputKind: replay.SnapshotQueryInputKind, StatementKind: replay.SnapshotQueryStatementKind, ClientAccount: s.Address(), StatementID: r.StatementID, NetworkID: testParams().NetworkID, KeeperShardID: 0, ReadSnapshot: r.ReadSnapshot, SchemaSnapshotID: r.ReadSnapshot.SchemaSnapshotID, SchemaRoot: r.ReadSnapshot.SchemaRoot, QueryProfileID: r.QueryProfileID, ExecutorProfileID: r.ExecutorProfileID, ReservationID: r.ReservationID, FencingGeneration: r.FencingGeneration, SQLHash: replay.DigestString("INSERT INTO db.t SELECT value FROM db.t"), RowIDProfileID: "housegate-row-id-v1", TargetTableID: "db.t"}
	env := replay.SnapshotQueryEnvelope{Input: replay.SnapshotQueryInput{Binding: b, SQL: "INSERT INTO db.t SELECT value FROM db.t", ReadSet: replay.SnapshotReadSet{ReadSnapshot: r.ReadSnapshot}}}
	root, err := replay.SnapshotQueryInputRoot(env.Input)
	if err != nil {
		t.Fatal(err)
	}
	env.InputRoot = root
	env.UserJWS, err = s.SignStatementV3(auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Iat: 1, Binding: b, InputRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func TestSubmitSnapshotQuerySequencesASingletonBlockAndConsumesTheGrant(t *testing.T) {
	f := newTestFSM(t)
	s := snapshotQueryTestSigner(t)
	r := grantedReservation(t, f, s, "statement-1", "request-1")
	env := signedQueryEnvelope(t, s, r)
	blocksBefore := len(f.st.Blocks)
	got, ok := applyCmd(t, f, 43, wire.Command{SubmitSnapshotQuery: &wire.SubmitSnapshotQuery{Envelope: env}}).(SnapshotQuerySubmitApplied)
	if !ok {
		t.Fatalf("submit = %#v", got)
	}
	res := got.Result
	if res.AdmissionCode != 1 || res.StatementSeq == 0 || res.BlockSeq != uint64(blocksBefore+1) || res.SourceNode == "" || res.InputRoot != env.InputRoot || !reflect.DeepEqual(res.Reservation, r) {
		t.Fatalf("result = %+v", res)
	}
	key, _ := artifactDispositionReservationKey(s.Address(), "statement-1", "request-1")
	d := f.st.ArtifactDisposition
	if d.Reservations[key].State != ArtifactReservationConsumed || d.ReservationBarrier.State != ArtifactReservationConsumed || d.ReservationBarrier.Generation != r.FencingGeneration {
		t.Fatalf("reservation not consumed: %+v / %+v", d.Reservations[key], d.ReservationBarrier)
	}
	adm := d.QueryAdmissions[snapshotQueryAdmissionKey(s.Address(), "statement-1")]
	if adm == nil || adm.StatementSeq != res.StatementSeq || adm.BlockSeq != res.BlockSeq || adm.OriginalJWSHash != replay.DigestString(env.UserJWS) || adm.Lifecycle != SnapshotQueryLifecycleSequenced || adm.CommitIndex != 43 {
		t.Fatalf("admission = %+v", adm)
	}
	if st := f.st.Statements[res.StatementSeq]; st == nil || !st.IsSnapshotQuery() || st.BlockSeq != res.BlockSeq || st.SourceNode != res.SourceNode {
		t.Fatalf("statement = %+v", st)
	}
	// Identical replay returns the same result; a re-signed envelope for the same statement is an identity conflict.
	if again, ok := applyCmd(t, f, 44, wire.Command{SubmitSnapshotQuery: &wire.SubmitSnapshotQuery{Envelope: env}}).(SnapshotQuerySubmitApplied); !ok || !reflect.DeepEqual(again.Result, res) {
		t.Fatalf("replay = %#v", again)
	}
	resigned := env
	resigned.UserJWS, _ = s.SignStatementV3(auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Iat: 2, Binding: env.Input.Binding, InputRoot: env.InputRoot})
	if rej, ok := applyCmd(t, f, 45, wire.Command{SubmitSnapshotQuery: &wire.SubmitSnapshotQuery{Envelope: resigned}}).(SnapshotQuerySubmitRejected); !ok || rej.Code != 2 {
		t.Fatalf("re-signed same statement = %#v, want code 2", rej)
	}
	view, found := f.SnapshotQueryStatusRead(s.Address(), "statement-1")
	if !found || view.StatementSeq != res.StatementSeq || view.OriginalJWSHash != replay.DigestString(env.UserJWS) {
		t.Fatalf("status read = %+v found=%v", view, found)
	}
}

func TestSubmitSnapshotQueryRefusesMismatchesBeforeAnyStateChange(t *testing.T) {
	f := newTestFSM(t)
	s := snapshotQueryTestSigner(t)
	r := grantedReservation(t, f, s, "statement-1", "request-1")
	base := signedQueryEnvelope(t, s, r)
	before := snapshotBytes(t, f)
	cases := map[string]struct {
		mutate func(e *replay.SnapshotQueryEnvelope, s *auth.RelaySigner)
		code   uint32
	}{
		"tampered signature": {func(e *replay.SnapshotQueryEnvelope, _ *auth.RelaySigner) { e.UserJWS = e.UserJWS[:len(e.UserJWS)-2] + "AA" }, 5},
		"wrong profile":      {func(e *replay.SnapshotQueryEnvelope, s *auth.RelaySigner) { e.Input.Binding.QueryProfileID = "query-2"; resign(t, e, s) }, 6},
		"wrong generation":   {func(e *replay.SnapshotQueryEnvelope, s *auth.RelaySigner) { e.Input.Binding.FencingGeneration++; resign(t, e, s) }, 6},
		"wrong pin":          {func(e *replay.SnapshotQueryEnvelope, s *auth.RelaySigner) { e.Input.Binding.ReadSnapshot.SnapshotID = "other"; e.Input.ReadSet.ReadSnapshot.SnapshotID = "other"; resign(t, e, s) }, 6},
		"wrong network":      {func(e *replay.SnapshotQueryEnvelope, s *auth.RelaySigner) { e.Input.Binding.NetworkID = "other-net"; resign(t, e, s) }, 7},
		"proof present":      {func(e *replay.SnapshotQueryEnvelope, _ *auth.RelaySigner) {}, 7},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := base
			tc.mutate(&e, s)
			cmd := wire.Command{SubmitSnapshotQuery: &wire.SubmitSnapshotQuery{Envelope: e}}
			if name == "proof present" {
				cmd.SubmitSnapshotQuery.NonMembershipProof = []byte{1}
			}
			got := applyCmd(t, f, 43, cmd)
			rej, ok := got.(SnapshotQuerySubmitRejected)
			if !ok || rej.Code != tc.code {
				t.Fatalf("%s = %#v, want code %d", name, got, tc.code)
			}
			if after := snapshotBytes(t, f); string(after) != string(before) {
				t.Fatalf("%s mutated state", name)
			}
		})
	}
	// A different account's valid envelope for a reservation it does not own is refused too.
	other, _ := auth.NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	foreign := signedQueryEnvelope(t, other, r) // binds other's account to r
	if rej, ok := applyCmd(t, f, 46, wire.Command{SubmitSnapshotQuery: &wire.SubmitSnapshotQuery{Envelope: foreign}}).(SnapshotQuerySubmitRejected); !ok || rej.Code != 6 {
		t.Fatalf("foreign account = %#v", rej)
	}
	// No grant at all: a plain retry-later rejection.
	g := newTestFSM(t)
	if got, ok := applyCmd(t, g, 43, wire.Command{SubmitSnapshotQuery: &wire.SubmitSnapshotQuery{Envelope: base}}).(Rejected); !ok || !strings.Contains(got.Reason, "not granted") {
		t.Fatalf("no grant = %#v", got)
	}
}

func resign(t *testing.T, e *replay.SnapshotQueryEnvelope, s *auth.RelaySigner) {
	t.Helper()
	root, err := replay.SnapshotQueryInputRoot(e.Input)
	if err != nil {
		t.Fatal(err)
	}
	e.InputRoot = root
	e.UserJWS, err = s.SignStatementV3(auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Iat: 1, Binding: e.Input.Binding, InputRoot: root})
	if err != nil {
		t.Fatal(err)
	}
}
```

`(*auth.RelaySigner).SignStatementV3(auth.JWSStatementPayloadV3) (string, error)` exists (used by housegate's intake tests); if the binding needs more fields to pass `VerifyEnvelope`'s complete-input validation (read set tables, settings hash, client revision), read `pkg/replay/snapshotquery/envelope.go` in the module cache, fill the minimum in `signedQueryEnvelope`, and record it.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestSubmitSnapshotQuery' --jobs=4 --test_output=errors`
Expected: build FAILS on `SnapshotQuerySubmitApplied` (new API); after stubs, the first test FAILS with `submit = fsm.Rejected{Reason:"empty command"}`.

- [ ] **Step 3: Implement the submit arm**

Create `fsm/apply_snapshot_query_submit.go`:

```go
package fsm

import (
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
	"github.com/sentioxyz/arbiter-core/wire"
)

// SnapshotQuerySubmitApplied is the accepted result of SubmitSnapshotQuery.
type SnapshotQuerySubmitApplied struct{ Result replay.SnapshotQuerySubmitResult }

// SnapshotQuerySubmitRejected is a deterministic terminal refusal carrying an
// arbiter.AdmissionCode value in 2..8; housegate never retries these.
type SnapshotQuerySubmitRejected struct {
	Code    uint32
	Message string
}

const (
	snapshotQueryCodeIdentityConflict = 2 // DUPLICATE_CLIENT_SEQ: same statement, different root or JWS
	snapshotQueryCodeInvalidSignature = 5
	snapshotQueryCodeInvalidProof     = 6 // reservation / pin / profile / policy mismatch
	snapshotQueryCodeMalformed        = 7
)

func snapshotQueryReject(code uint32, msg string) any { return SnapshotQuerySubmitRejected{Code: code, Message: msg} }

// applySubmitSnapshotQuery consumes the granted reservation for the signed
// envelope's account and statement into a singleton block. Everything it
// checks is committed state or the envelope's own bytes; the proposer chooses
// nothing.
func (f *FSM) applySubmitSnapshotQuery(c *wire.SubmitSnapshotQuery, index uint64) any {
	if c == nil {
		return Rejected{Reason: "submit snapshot query command is required"}
	}
	if len(c.NonMembershipProof) != 0 {
		return snapshotQueryReject(snapshotQueryCodeMalformed, "non_membership_proof has no contract yet and must be empty")
	}
	env := c.Envelope
	b := env.Input.Binding
	if b.NetworkID != f.st.Params.NetworkID || b.KeeperShardID != 0 || strings.TrimSpace(env.Input.SQL) == "" {
		return snapshotQueryReject(snapshotQueryCodeMalformed, "snapshot query binding names another network, a non-zero shard or an empty statement")
	}
	account, err := snapshotquery.VerifyEnvelope(env)
	if err != nil {
		return snapshotQueryReject(snapshotQueryCodeInvalidSignature, fmt.Sprintf("snapshot query envelope: %v", err))
	}
	if !strings.EqualFold(account, b.ClientAccount) {
		return snapshotQueryReject(snapshotQueryCodeInvalidSignature, "snapshot query signer is not the binding account")
	}
	d := &f.st.ArtifactDisposition
	admKey := snapshotQueryAdmissionKey(b.ClientAccount, b.StatementID)
	if existing := d.QueryAdmissions[admKey]; existing != nil {
		if existing.InputRoot == env.InputRoot && existing.OriginalJWSHash == replay.DigestString(env.UserJWS) {
			return SnapshotQuerySubmitApplied{Result: snapshotQuerySubmitResult(existing)}
		}
		return snapshotQueryReject(snapshotQueryCodeIdentityConflict, "snapshot query statement identity conflict: a different input root or signature is already sequenced")
	}
	bar := normalizedArtifactDispositionReservationBarrier(d.ReservationBarrier)
	if bar.State != ArtifactReservationGranted || !strings.EqualFold(bar.ClientAccount, b.ClientAccount) || bar.StatementID != b.StatementID {
		return Rejected{Reason: "snapshot query reservation is not granted for this account and statement"}
	}
	key, err := artifactDispositionReservationKey(bar.ClientAccount, bar.StatementID, bar.RequestID)
	if err != nil {
		return Rejected{Reason: "snapshot query reservation key is invalid"}
	}
	active := d.Reservations[key]
	if active == nil || active.State != ArtifactReservationGranted || active.Reservation == nil {
		return Rejected{Reason: "snapshot query reservation is not granted for this account and statement"}
	}
	r := *active.Reservation
	policy := d.QueryPolicyReadiness
	if policy == nil || !policy.Policy.Enabled {
		return snapshotQueryReject(snapshotQueryCodeInvalidProof, "no active snapshot query policy")
	}
	if r.ReservationID != b.ReservationID || r.FencingGeneration != b.FencingGeneration || r.ReadSnapshot != b.ReadSnapshot || env.Input.ReadSet.ReadSnapshot != b.ReadSnapshot ||
		b.SchemaSnapshotID != r.ReadSnapshot.SchemaSnapshotID || b.SchemaRoot != r.ReadSnapshot.SchemaRoot ||
		b.ExecutorProfileID != r.ExecutorProfileID || b.QueryProfileID != r.QueryProfileID ||
		b.ExecutorProfileID != policy.Policy.ExecutorProfileID || b.QueryProfileID != policy.Policy.QueryProfileID {
		return snapshotQueryReject(snapshotQueryCodeInvalidProof, "snapshot query binding does not match the granted reservation and active policy")
	}
	if f.st.OpenBlock != nil && len(f.st.OpenBlock.StatementSeqs) != 0 {
		return Rejected{Reason: "open block is not empty; the drain fence was bypassed"}
	}
	// Everything below is the atomic consume + sequence + seal.
	seq := f.st.NextStatementSeq
	f.st.NextStatementSeq++
	source := f.selectSource(b.ClientAccount + "\x00" + b.StatementID)
	stored := env
	f.st.Statements[seq] = &StatementState{Kind: replay.SnapshotQueryStatementKind, Query: &stored, Seq: seq, Status: StatusSequenced, SourceNode: source}
	blockSeq, statementRoot, err := f.sealSnapshotQueryBlockLocked(seq, env, source)
	if err != nil {
		// Cannot happen after the checks above; keep state consistent rather than half-sequenced.
		delete(f.st.Statements, seq)
		f.st.NextStatementSeq = seq
		return Rejected{Reason: "seal snapshot query block: " + err.Error()}
	}
	next := cloneArtifactDispositionForReservationGrant(d)
	rec := next.Reservations[key]
	rec.State = ArtifactReservationConsumed
	rec.BlockSeq = blockSeq
	if err := putArtifactDispositionReservationBarrier(next, ArtifactDispositionReservationBarrierState{
		State: ArtifactReservationConsumed, Generation: bar.Generation, RequestID: bar.RequestID, ClientAccount: bar.ClientAccount, StatementID: bar.StatementID,
		Reservation: cloneArtifactDispositionReservation(&r), BlockSeq: blockSeq,
	}); err != nil {
		return Rejected{Reason: err.Error()}
	}
	adm := &SnapshotQueryAdmissionState{
		ClientAccount: b.ClientAccount, StatementID: b.StatementID, RequestID: bar.RequestID, ReservationKey: key,
		StatementSeq: seq, BlockSeq: blockSeq, SourceNode: source, InputRoot: env.InputRoot, OriginalJWSHash: replay.DigestString(env.UserJWS),
		StatementRoot: statementRoot, Lifecycle: SnapshotQueryLifecycleSequenced, Reservation: r, CommitIndex: index,
	}
	next.QueryAdmissions[admKey] = adm
	if index > next.LastAppliedIndex {
		next.LastAppliedIndex = index
	}
	f.st.ArtifactDisposition = *next
	return SnapshotQuerySubmitApplied{Result: snapshotQuerySubmitResult(adm)}
}

func snapshotQuerySubmitResult(a *SnapshotQueryAdmissionState) replay.SnapshotQuerySubmitResult {
	return replay.SnapshotQuerySubmitResult{AdmissionCode: 1, StatementSeq: a.StatementSeq, BlockSeq: a.BlockSeq, SourceNode: a.SourceNode, InputRoot: a.InputRoot, Reservation: a.Reservation}
}

// SnapshotQueryStatusRead returns a detached copy of the sequencing facts for
// one account/statement, or found=false when nothing was ever sequenced.
func (f *FSM) SnapshotQueryStatusRead(account, statement string) (SnapshotQueryAdmissionState, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	a := f.st.ArtifactDisposition.QueryAdmissions[snapshotQueryAdmissionKey(account, statement)]
	if a == nil {
		return SnapshotQueryAdmissionState{}, false
	}
	return *a, true
}
```

Notes for the implementer: the barrier/record mutation must go through the clone-and-swap pattern the other transitions use (`cloneArtifactDispositionForReservationGrant`, `putArtifactDispositionReservationBarrier` — the `grantToConsumed` transition is whitelisted); `f.emit`'s `Event` for the sealed block is emitted by the helper; the statement-store and seal happen before the disposition swap, so if the swap is refused (it cannot be, after the checks) the code above unwinds the statement — keep that unwind. In `fsm/fsm.go` add `case cmd.SubmitSnapshotQuery != nil: return f.applySubmitSnapshotQuery(cmd.SubmitSnapshotQuery, l.Index)` next to the C2 arms. `selectSource` takes the flat statement id; if its argument type differs (e.g. it expects an `arbiter.StatementID`), adapt and record.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test --jobs=4 --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm/apply_snapshot_query_submit.go fsm/apply_snapshot_query_submit_test.go fsm/fsm.go fsm/BUILD.bazel
git commit -m "feat(fsm): dispatch SubmitSnapshotQuery as an atomic consume-sequence-seal transition"
```

---

### Task 3: `SubmitSnapshotQuery` and `GetSnapshotQueryStatus` handlers (server)

**Files:**
- Create: `server/snapshot_query_submit.go`
- Modify: `server/snapshot_query_reservations.go` (deps gain `Statements`), `server/ingress.go` (register the two methods on `ingressService`), `server/BUILD.bazel`
- Test: `server/snapshot_query_submit_test.go`

**Interfaces:**
- Consumes: `fsm.SnapshotQuerySubmitApplied/Rejected`, `fsm.SnapshotQueryStatusRead`, `wire.SnapshotQueryEnvelopeFromPB`, `wire.SnapshotQuerySubmitResultToPB`, `wire.SnapshotQueryStatusToPB`, `wire.GetSnapshotQueryStatusRequestFromPB`, `consensusLeaderCheck`, `consensusReadBarrier`, `propose`.
- Produces: `SnapshotQueryReservationDeps.Statements snapshotQueryStatementValidator` (interface `ValidateStatementV3(token string, want auth.JWSStatementPayloadV3) (string, error)`) and `NewHousegateSnapshotQueryStatementValidator(allowedAddresses []string, maxTokenAge time.Duration) snapshotQueryStatementValidator` (fail-closed on an empty allowlist, like the control authorizer); handlers `(*ingressService).SubmitSnapshotQuery(ctx, *pb.SnapshotQueryEnvelope) (*pb.SnapshotQuerySubmitResult, error)` and `(*ingressService).GetSnapshotQueryStatus(ctx, *pb.GetSnapshotQueryStatusRequest) (*pb.SnapshotQueryStatus, error)`.

- [ ] **Step 1: Write the failing tests**

Create `server/snapshot_query_submit_test.go` with: (a) both RPCs return `FailedPrecondition` without the dependency and never reach Raft; (b) end to end on a leader with `publishServerGenesis`, `activateServerTestQueryPolicy`, enabled deps whose `Statements` validator allowlists the signer: acquire (through the RPC, as in `TestAcquireGrantsImmediatelyWhenTheFrontierIsPublished`) → build the envelope with the returned reservation (port `signedQueryEnvelope` from the fsm test into a server helper that takes the pb reservation via `wire.SnapshotQueryReservationFromPB`) → `SubmitSnapshotQuery` returns `admission_code 1`, `statement_seq != 0`, `block_seq != 0`, `source_node != ""`, `input_root` and `reservation` equal → `GetSnapshotQueryStatus` with the exact input root and `replay.DigestString(env.UserJWS)` returns `found=true`, `version 1`, `lifecycle "sequenced"`, `accepted` equal to the submit result → a second submit returns the same result → status with a wrong `expected_user_jws_hash` is `FailedPrecondition` ("identity conflict") → status for an unknown statement is `found=false` with no accepted result; (c) a stale token (`Iat` far in the past with `maxTokenAge` one minute) is `PermissionDenied` before Raft; a signer outside the allowlist is `PermissionDenied`; (d) submit without a grant for that statement is `Unavailable` (the FSM's retry-later rejection); (e) a follower refuses both RPCs before Raft/barrier; (f) status requires `expected_input_root` and `expected_user_jws_hash` (`InvalidArgument` when empty).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //server:server_test --test_filter='TestSubmitSnapshotQuery|TestGetSnapshotQueryStatus' --jobs=4 --test_output=errors`
Expected: build FAILS on the missing validator constructor; after stubs, the end-to-end test FAILS with `Unimplemented`.

- [ ] **Step 3: Implement the handlers**

Create `server/snapshot_query_submit.go`:

```go
package server

import (
	"context"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type snapshotQueryStatementValidator interface {
	ValidateStatementV3(token string, want auth.JWSStatementPayloadV3) (string, error)
}

type housegateSnapshotQueryStatementValidator struct{ validator *auth.EthValidator }

// NewHousegateSnapshotQueryStatementValidator checks freshness and the account
// allowlist of a v3 statement token before it is proposed; the FSM repeats the
// pure signature check on apply. An empty allowlist admits nobody.
func NewHousegateSnapshotQueryStatementValidator(allowedAddresses []string, maxTokenAge time.Duration) snapshotQueryStatementValidator {
	return housegateSnapshotQueryStatementValidator{validator: auth.NewEthValidator(allowedAddresses, maxTokenAge, true, false, "", nil)}
}

func (v housegateSnapshotQueryStatementValidator) ValidateStatementV3(token string, want auth.JWSStatementPayloadV3) (string, error) {
	if v.validator == nil || len(v.validator.AllowedAddresses) == 0 {
		return "", status.Error(codes.PermissionDenied, "snapshot query statement allowlist is empty")
	}
	return v.validator.ValidateStatementV3(token, want)
}

func (s *Server) submitSnapshotQuery(ctx context.Context, req *pb.SnapshotQueryEnvelope) (*pb.SnapshotQuerySubmitResult, error) {
	d, err := s.snapshotQueryReservationsEnabled()
	if err != nil {
		return nil, err
	}
	if d.Statements == nil {
		return nil, status.Error(codes.FailedPrecondition, snapshotQueryAdmissionUnavailable)
	}
	env := wire.SnapshotQueryEnvelopeFromPB(req)
	b := env.Input.Binding
	if env.UserJWS == "" || env.InputRoot == "" || b.StatementID == "" || b.ClientAccount == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot query envelope is incomplete")
	}
	if signer, err := d.Statements.ValidateStatementV3(env.UserJWS, auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: b, InputRoot: env.InputRoot}); err != nil {
		if _, ok := status.FromError(err); ok {
			return nil, err
		}
		return nil, status.Errorf(codes.PermissionDenied, "snapshot query statement: %v", err)
	} else if !strings.EqualFold(signer, b.ClientAccount) {
		return nil, status.Error(codes.PermissionDenied, "snapshot query statement signer is not the binding account")
	}
	if err := s.consensusLeaderCheck(ctx); err != nil {
		return nil, err
	}
	res, err := s.propose(ctx, wire.Command{SubmitSnapshotQuery: &wire.SubmitSnapshotQuery{Envelope: env}})
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument && strings.Contains(st.Message(), "not granted") {
			return nil, status.Error(codes.Unavailable, "snapshot query reservation is not granted; look it up and retry")
		}
		return nil, err
	}
	switch r := res.(type) {
	case fsm.SnapshotQuerySubmitApplied:
		return wire.SnapshotQuerySubmitResultToPB(r.Result), nil
	case fsm.SnapshotQuerySubmitRejected:
		return wire.SnapshotQuerySubmitResultToPB(replay.SnapshotQuerySubmitResult{AdmissionCode: r.Code, Message: r.Message, InputRoot: env.InputRoot}), nil
	default:
		return nil, status.Errorf(codes.Internal, "unexpected snapshot query submit result %T", res)
	}
}

func (s *Server) getSnapshotQueryStatus(ctx context.Context, req *pb.GetSnapshotQueryStatusRequest) (*pb.SnapshotQueryStatus, error) {
	if _, err := s.snapshotQueryReservationsEnabled(); err != nil {
		return nil, err
	}
	q := wire.GetSnapshotQueryStatusRequestFromPB(req)
	if q.NetworkID == "" || q.KeeperShardID != 0 || q.ClientAccount == "" || q.StatementID == "" || q.ExpectedInputRoot == "" || q.ExpectedUserJWSHash == "" {
		return nil, status.Error(codes.InvalidArgument, "snapshot query status requires network, zero shard, account, statement, expected_input_root and expected_user_jws_hash")
	}
	if err := s.consensusReadBarrier(ctx); err != nil {
		return nil, err
	}
	adm, found := s.d.FSM.SnapshotQueryStatusRead(q.ClientAccount, q.StatementID)
	if !found {
		return wire.SnapshotQueryStatusToPB(replay.SnapshotQueryStatus{Version: 1, Found: false}), nil
	}
	if adm.InputRoot != q.ExpectedInputRoot || adm.OriginalJWSHash != q.ExpectedUserJWSHash {
		return nil, status.Error(codes.FailedPrecondition, "snapshot query identity conflict: the sequenced statement has a different input root or signature")
	}
	return wire.SnapshotQueryStatusToPB(replay.SnapshotQueryStatus{Version: 1, Found: true, Accepted: replay.SnapshotQuerySubmitResult{AdmissionCode: 1, StatementSeq: adm.StatementSeq, BlockSeq: adm.BlockSeq, SourceNode: adm.SourceNode, InputRoot: adm.InputRoot, Reservation: adm.Reservation}, Lifecycle: adm.Lifecycle}), nil
}
```

In `server/ingress.go` add the two `ingressService` methods delegating to these. Add `Statements snapshotQueryStatementValidator` to `SnapshotQueryReservationDeps` with a doc line (nil keeps both new RPCs refusing). Check the pb field of `GetSnapshotQueryStatusRequest`'s network is validated against `s.d.FSM`'s network the way the control RPCs do it (`validSnapshotQueryControlBinding` compares `NetworkID` to the FSM's — mirror it).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //server:server_test //fsm:fsm_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS (19 targets).

- [ ] **Step 5: Commit**

```bash
git add server/snapshot_query_submit.go server/snapshot_query_submit_test.go server/snapshot_query_reservations.go server/ingress.go server/BUILD.bazel
git commit -m "feat(server): serve SubmitSnapshotQuery and GetSnapshotQueryStatus behind the default-off dependency"
```

---

### Task 4: Cross-repo conformance and lifecycle pins (arbiter tests)

**Files:**
- Create: `server/snapshot_query_submit_crossrepo_test.go`
- Test only.

- [ ] **Step 1: Write the tests**

Mirror `server/snapshot_query_control_crossrepo_test.go`: build an envelope the way housegate's intake does (`auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding, InputRoot}` signed with `auth.NewRelaySigner`), run it through the real `SubmitSnapshotQuery` RPC on the bufconn server, then assert the returned result satisfies housegate's own acceptance rule by re-implementing `validateSnapshotQueryAccepted`'s checks literally (code 1, input root, non-zero seq/block, non-empty source, reservation equality on ID/generation/account/statement/pin/profiles); and assert `GetSnapshotQueryStatus` with `replay.DigestString(env.UserJWS)` returns `found=true`. Add a pinned-vocabulary test that `fsm.SnapshotQueryLifecycleSequenced == "sequenced"` so a rename is a conscious wire change.

- [ ] **Step 2: Run and commit**

Run: `bazel test //server:server_test --jobs=4 --test_output=errors`.

```bash
git add server/snapshot_query_submit_crossrepo_test.go
git commit -m "test(server): pin the snapshot query submit result against housegate's acceptance rule"
```

---

### Task 5: Documentation (arbiter)

**Files:**
- Modify: `docs/snapshot-query-reservations.md` (new section "Submitting a query and reading its status"), `README.md` (no change unless the link line needs a wider title)

- [ ] **Step 1: Write the section**

One paragraph per line: the two RPCs and their contracts; the admission-code table (1 accepted; 2 identity conflict; 5 signature; 6 reservation/pin/profile/policy; 7 malformed incl. non-empty `non_membership_proof`; `Unavailable` for "not granted yet"); the singleton block (one statement, `StatementsRoot = SnapshotQueryStatementRoot`, header `query_statement_root`, v2 readers skip query statements, the safe prefix stalls at the query block until its terminal); status identity (`expected_input_root` + `expected_user_jws_hash`, conflict vs not found, lifecycle `sequenced`); snapshot v11; deferred (abort and the aborted no-op child, query obligation / Q-family transfer, `NonMembershipProof` contract, endpoint authentication for status reads, housegate-side sequencer client).

- [ ] **Step 2: Commit**

```bash
git add docs/snapshot-query-reservations.md
git commit -m "docs: describe snapshot query submission, singleton blocks and status identity"
```

---

## Deferred to 1a-2b / 1a-3 (recorded, not placeholders)

- Abort: `AbortSnapshotQueryCmd` (tag 22) dispatch, a new arbiter-core authority purpose `housegate-snapshot-query-abort-v1` mirroring `VerifyConsensusParamsUpdate` (arbiter-core PR + pin bump), the `snapshot-query-abort-v1` record on the admission state, the consumed → released transition with the abort root as terminal proof, and the aborted no-op child manifest (`safePrefixLocked` must treat an aborted query block as safe; the v2 promotion path's `StatusSafe` is the hook).
- The query obligation and Q-family transfer journal (`ApplyServerOwnedReservationQueryTransfer`) on the capacity lane; with the ledger enabled, `legacyMutationGuard` rejects every non-disposition command including submit, so 1a-3 must route submit through the charged path.
- `NonMembershipProof` semantics (gap proof against the SpentIDs accumulator); the v2 lane never populates it either.
- Endpoint authentication for status reads (D3) and the housegate-side `QuerySequencer` adapter (Wave 3 T3.1).
- Source selection policy beyond `selectSource` (C5 assigns/verifies the source through claims).

## Subsequent plans

| Plan | Scope | Written when |
|---|---|---|
| 1a-2b — abort and terminal | tag 22, authority purpose (AC), abort record, released tombstone with abort proof, aborted no-op child publication, `applied`/`aborted` lifecycle | after this plan merges |
| 1a-3 — C1 remaining | four disposition actions, `Capability` enable command, capacity charging (submit through the charged path), Raft 26/27, leader-Barrier publication/history reads | after 1a-2b |

## Execution amendments (2026-09-21)

Recorded during subagent-driven execution on arbiter (PRs #99 Task 1, #100 Task 2, #101 Tasks 3+4, Task 5 docs). Each item is a ruling made against the plan text above; the merged code is authoritative where the two differ.

- **Pre-flight defects in the plan's Task 2 code.** (1) `putArtifactDispositionReservationBarrier`'s `grantToConsumed` whitelist required `old.CapacityBinding != nil` and `old.BlockSeq == barrier.BlockSeq`; the plan's consumed-barrier literal (`BlockSeq: blockSeq`, no capacity binding) would have been refused with "fence conflict" on the first submit. Rulings: the reservation record's and barrier's `BlockSeq` stay the begin-time request identity, and the sealed query block's sequence lives only in `SnapshotQueryAdmissionState.BlockSeq` / `StatementState.BlockSeq`; the whitelist accepts a nil capacity binding on both sides (the capacity-free lane), still binding the two with `reflect.DeepEqual`. (2) `cloneArtifactDispositionForReservationGrant` shallow-copied the new `QueryAdmissions` map; Task 1 deep-copies it and Task 2 calls `ensureDispositionMaps(next)` after cloning.
- **Task 1.** The brief's test envelope did not pass `replay.ValidateSnapshotQueryInput` (every binding string field, non-zero `FencingGeneration`/`ClientRevision`, `SQLHash`, `ReadSetRoot`, and a canonical `0x<lowercase account>:<seq>:<nonce>` statement ID are required); `fillSealedHeaderChainFieldsLocked` returns an error; the work-list skip is keyed on the header's `QueryStatementRoot` (the statement list can be short); the `ByStatementID` restore rebuild skips query statements (their zero `Env` would collide on one key with a map-iteration-order winner read by an Apply decision); `L3BlockView` (`GetL3Block`) refuses a query block by name until the slice that gives query blocks a wire surface; `TestL3BlockHeaderMirrorsProto` carries a documented exemption for `query_statement_root` (arbiter-proto field deferred). Review fix round: the seal helper dropped its envelope parameter and seals from `*stmt.Query` (envelope divergence became unrepresentable), refuses an already-sealed statement, and validates `ChainHash()` before appending.
- **Task 2.** The atomic tail is reordered: clone + barrier put on the detached clone first, then statement store + seal, then swap (the plan's unwind did not reverse the seal's mutations). The plan's `EqualFold` account check was dropped because `snapshotquery.VerifyEnvelope` already enforces exact signer == binding account; the recovered account is used downstream. Admission codes come from `arbiter.AdmissionCode*`; `SnapshotQueryNotGrantedReason` is exported for the server. The plan's foreign-account test line expected code 6 while its own implementation returns the retry-later `Rejected` — the latter is right (ordinary contention has the same observable shape). Review fix round: a grant whose record or barrier already carries a capacity binding is refused with a plain `Rejected` naming the charged query-transfer path (the two consume paths stay disjoint); `index == 0` is refused before any state change; policy-disabled / policy-moved cases measured.
- **Tasks 3+4.** Check order follows the Wave 1a-1 ruling (shape → capability gate → network scope → authentication → leader/barrier → Raft); both RPCs refuse without the `Statements` validator through one shared gate; the server lowercases `client_account` before the status read (the FSM key is the recovered lowercase signer; a checksummed spelling would otherwise answer `found=false` and make housegate double-submit); "not granted" is mapped by exact equality with the exported constant; a foreign network is `FailedPrecondition` on both RPCs (the lane's convention) without echoing the replica's network, and submit pre-checks it before authentication so a misconfigured client never costs a Raft round trip; `FSM.NetworkID()` and `SnapshotQueryAdmissionState.Result()` were added; the test `fakeNode` numbers its Raft log entries (the transition refuses index 0) and holds its lock across the apply; housegate's `validateSnapshotQueryAccepted` rule is applied to `status.Accepted` as well as the submit result (housegate's recovery re-validates it after a lost submit response).
- **Task 5.** The docs also correct a pre-existing prediction: no wall clock enters the FSM with C3's sealing; the only clock read is the server's pre-Raft freshness check of the v3 statement token. The docs review's two findings (a test-attribution slip and an ambiguous clause) were applied by the controller directly.
- **Carried to later slices.** Endpoint authentication for status reads must run before the read barrier (today an unauthenticated caller costs one Raft barrier per request); the C5 history projection requires a consumed record to carry a capacity binding, so capacity-free consumed reservations are invisible to it until 1a-3 routes submit through the charged path; the `GetL3Block` representation of query blocks and the arbiter-proto `query_statement_root` field belong to the wire-visibility slice; `ClientRevision` and the other client-chosen binding fields are unconstrained this wave.
