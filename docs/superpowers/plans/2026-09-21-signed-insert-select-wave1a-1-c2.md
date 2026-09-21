# Signed INSERT ... SELECT — Wave 1a-1: Fenced Snapshot-Query Reservations (C2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make arbiter's three snapshot-query control RPCs (`AcquireSnapshotQuery`, `GetSnapshotQueryReservation`, `ReleaseSnapshotQuery`) real: an authenticated acquire proposes a committed drain-begin that fences new SI admissions, a leader-side coordinator proposes the grant once the captured drain frontier is published safe, lookup answers after a leader read barrier, and release commits a fenced tombstone — all behind a default-off server dependency.

**Architecture:** Three snapshot-query Raft commands that arbiter-proto and arbiter-core already define (`BeginSnapshotQueryCmd` tag 30, `GrantSnapshotQueryCmd` tag 19, `ReleaseSnapshotQueryCmd` tag 20) gain `FSM.Apply` cases that delegate to the existing private drain/complete/release transitions; the FSM re-verifies the housegate control JWS deterministically, derives the reservation identity and pin from committed state, and refuses anything the proposer got wrong. The server authenticates before proposing (the `UpdateConsensusParams` pattern), performs `VerifyLeader`+`Barrier` before every read, and runs one grant loop on the leader. The snapshot format moves to v10 to refuse frontier-less draining records. Nothing is reachable until `server.Deps.SnapshotQueryReservations` is injected, which `cmd/arbiter` does not do.

**Tech Stack:** Go 1.26, Bazel 9.1.0 (`bazel run //:gazelle` after adding files; `fsm/BUILD.bazel` and `server/BUILD.bazel` list explicit srcs), hashicorp/raft via `raftnode.ConsensusNode`, arbiter-core `wire` (protobuf `RaftCommand`), housegate `pkg/auth` (ES256K control JWS) and `pkg/replay` (types, `DigestString`).

**Spec:** [coordination plan Task C2](2026-09-16-signed-insert-select-coordination.md) (binding contract, lifecycle text, drain table); [design D1](../specs/2026-09-16-signed-insert-select-design.md); [master plan Global Constraints](2026-09-16-signed-insert-select.md); [TODO plan T1.2](2026-09-20-signed-insert-select-todo.md). Wave 1a order and rationale: C2 (this plan) → C3 (1a-2) → C1 remaining actions and capacity charging (1a-3), one serial arbiter lane.

## Global Constraints

- Tier 3 of [issue #153](https://github.com/housegate/housegate/issues/153): merging enables no runtime capability and authorizes no deployment. Every runtime gate defaults off; `cmd/arbiter/services.go` must not set the new dependency.
- Acquire drains earlier SI work through published safe or committed abort, not ACK2. A draining record already has a fence. The generation increments on begin/grant/release/fencing and refuses overflow. Keep one pending reservation per network/shard; another request returns a bounded busy response. An identical request ID performs a status lookup. Unauthorized callers cannot reserve the global barrier.
- The FSM performs no remote I/O or SQL analysis; it independently checks that the predecessor, drain counters and policy still match. No local timer mutates FSM state. Every replica applying identical commands must produce byte-identical state.
- Control JWS: purpose `housegate-snapshot-query-control-v1`, operations `acquire`/`lookup`/`release`, binding fields `operation, network_id, keeper_shard_id, client_account, statement_id, request_id, reservation_id, fencing_generation` (no wildcards; empty/zero are exact values). Verification uses housegate `pkg/auth` (`VerifySnapshotQueryControlSignature` is pure; `(*EthValidator).ValidateSnapshotQueryControl` adds freshness and allowlist). `keeper_shard_id` must be 0 in v1.
- Status contract (`SnapshotQueryReservationStatus`): `version=1`; `found=true` states are `draining`, `granted`, `consumed`, `released`; `fencing_generation` nonzero for found records; `found=false` echoes the authenticated identity with empty state and no reservation/block/proof and is never inferred from a transport failure.
- Reads that answer authoritatively run `VerifyLeader` then `Barrier` (`consensusReadBarrier`); a follower refuses rather than serving a stale local view.
- Use `replay.DigestString` for every new digest; new domains introduced here are `snapshot-query-control-binding-v1` and `snapshot-query-reservation-id-v1`, string-prefixed as shown in Task 1, and are arbiter-internal (never signed by clients).
- Snapshot format: current v9 (`snapshotVersion = 9`, `snapshotVersionV9`); this plan bumps to v10 following the v8→v9 recipe (constants, accept list and error text, allowance-validation predicate, disposition-restore predicate, downgrade scrub) and refuses a draining record whose `DrainFrontierBlockSeq` is nil.
- Tests: `bazel test //fsm:fsm_test //server:server_test --jobs=4` per task; `bazel test //...` (19 targets) before each PR. Rejections are asserted with `strings.Contains(got.(fsm.Rejected).Reason, …)`; successes with `reflect.DeepEqual(got, fsm.Applied{})`.
- Conventions: English comments; conventional commit subjects; commits end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; PR bodies end with `Refs https://github.com/housegate/housegate/issues/153`, `Design: https://github.com/housegate/housegate/pull/154` and `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. One isolated worktree for the whole lane (`fix/153-wave1a-c2`), never the main checkout `/Users/uranuswch/Dev/sentio_xyz/arbiter`. Merge only on green CI.

## Baseline facts (arbiter `origin/main` 8d69908, 2026-09-21)

- `fsm/fsm.go:60-112` `Apply` decodes `wire.Command` (protobuf) and dispatches 18 legacy commands plus `ArtifactDisposition`; every snapshot-query kind falls to `default: Rejected{Reason: "empty command"}`.
- `wire.Command` (arbiter-core `wire/command.go:82-112`) already carries `BeginSnapshotQuery *BeginSnapshotQuery{Request AcquireSnapshotQueryRequest}`, `GrantSnapshotQuery *GrantSnapshotQuery{RequestID string; Reservation replay.SnapshotQueryReservation}`, `ReleaseSnapshotQuery *ReleaseSnapshotQuery{Request ReleaseSnapshotQueryRequest}`; `wire.Encode`/`Decode` handle them. `wire.AcquireSnapshotQueryRequest` / `ReleaseSnapshotQueryRequest` mirror the proto fields (`NetworkID, KeeperShardID, ClientAccount, StatementID, RequestID, ControlJWS` and, for release, `ReservationID, FencingGeneration`).
- Private transitions exist and are tested: `applyBeginReservationDrain(begin ArtifactDispositionReservationDrainBegin, index) any` (`fsm/artifact_reservation_drain.go:55`, captures `DrainFrontierBlockSeq`), `applyCompleteReservationDrain(complete ArtifactDispositionReservationDrainComplete, index) any` (`:117`, requires `reservationDrainFrontierPublished`), `applyReleaseReservation(release ArtifactDispositionReservationRelease, index) any` (`fsm/artifact_reservation_release.go:34`, exported seam `ApplyServerOwnedReservationRelease`). Request structs: `DrainBegin{ClientAccount, StatementID, RequestID, ControlBindingDigest string; BlockSeq uint64}`, `DrainComplete{… + FencingGeneration, BlockSeq uint64; Reservation *replay.SnapshotQueryReservation}`, `Release{ClientAccount, StatementID, RequestID, ControlBindingDigest string; FencingGeneration, BlockSeq uint64; TerminalProof []byte}`.
- Barrier: `f.st.ArtifactDisposition.ReservationBarrier` (value) with `State` in `idle/draining/granted/consumed/resolving/released` constants `ArtifactReservationIdle…`; records in `Reservations[key]`, tombstones in `ReservationTombstones[key]`, key = `artifactDispositionReservationKey(account, statement, request)`.
- Active policy: `f.st.ArtifactDisposition.QueryPolicyReadiness` (`*ArtifactDispositionQueryPolicyReadiness`, field `Policy replay.ActiveQueryPolicy{ActivationID, NetworkID, KeeperShardID, ActivationBlockSeq, ExecutorProfileID, QueryProfileID, Enabled}`); nil until C1's activation lands.
- Safe watermark: `f.st.SafeWatermark` (`SafeWatermark{SnapshotID, SafeBlockSeq, ManifestRoot}`, exported view `f.SafeWatermarkView()` in `fsm/reads.go:97`). Frontier at begin = `len(f.st.Blocks)` (+1 when `f.st.OpenBlock` has `StatementSeqs`).
- SI admission: `applySubmitStatement(c *wire.SubmitStatement) any` (`fsm/admission.go:17`) has no barrier consultation; the fence belongs before `SpentIDs.Insert` (`:81`).
- Reads: `f.ArtifactDispositionReservationRead(ArtifactDispositionReservationReadSelector{ClientAccount, StatementID, RequestID}) (ArtifactDispositionReservationReadView{NetworkID, ReadIndex, Active, Terminal}, bool, error)` (`fsm/artifact_reservation_reads.go:25`), wrapped by `server/snapshot_query_reservation_projection.go`'s `fsmSnapshotQueryReservationProjectionReader`.
- Server: `propose(ctx, wire.Command) (any, error)` (`server/server.go:177`, maps `fsm.Rejected` → `codes.InvalidArgument`, not-leader → `NotLeader` detail); `consensusLeaderCheck` (`server/consensus_admin.go:372`), `consensusReadBarrier` (`:395`); `Deps` (`server/server.go:44`) has package-private `ArtifactDisposition *artifactDispositionAdmissionDeps` and the `snapshotQueryControlAuthorizer` interface `AuthorizeSnapshotQueryControl(ctx, account string, binding wire.SnapshotQueryControlBinding, token string) error` (`server/artifact_disposition.go:127`). The control RPCs (`server/snapshot_query_control.go`) validate the binding shape (`validSnapshotQueryControlBinding` `:102`) and refuse with `snapshotQueryAdmissionUnavailable`; tests `TestSnapshotQueryControlRPCsAreDefaultOffBeforeEverySideEffect` (`server/server_test.go:254`) and `TestSnapshotQueryControlRPCsValidateEveryRequestField` (`:317`) must keep passing.
- Server test harness: `startServerWithConfig(t, startServerConfig{f, leader, …, artifactDisposition, configureNode})` returns `(*grpc.ClientConn, *Server, *fakeNode)`; `fakeNode.Apply` applies into the real FSM when `leader`; `newServerTestFSM(t)`; `mustDirectApply(t, node, wire.Command)`; fixtures `snapshotQueryControlRequestFields` (`server_test.go:299`) with `.acquire()/.get()/.release()`.
- housegate control signer for tests: `auth.NewRelaySigner(<64-hex key>)` → `SignSnapshotQueryControl(auth.SnapshotQueryControlBinding) (string, error)`, `Address()`; `wire.SnapshotQueryControlBinding` converts to `auth.SnapshotQueryControlBinding` by direct type conversion (`server/snapshot_query_control_crossrepo_test.go`).

Execution order: Task 1 → 2 → 3 → 4 → 5 (FSM, one PR each or batched two per PR at the executor's discretion) → 6 → 7 (server) → 8 (docs). Each task ends with `bazel test //fsm:fsm_test //server:server_test --jobs=4` green and gofmt clean.

---

### Task 1: Dispatch `BeginSnapshotQuery` — authenticated, deterministic drain begin (fsm)

**Files:**
- Create: `fsm/apply_snapshot_query_reservation.go`
- Modify: `fsm/fsm.go:70-111` (add three `case` arms; only the begin arm is implemented in this task, the other two return a named rejection until Tasks 3–4)
- Modify: `fsm/BUILD.bazel` (gazelle adds the file and the `@housegate//pkg/auth` dep)
- Test: `fsm/apply_snapshot_query_reservation_test.go`

**Interfaces:**
- Consumes: `wire.BeginSnapshotQuery`, `applyBeginReservationDrain`, `auth.VerifySnapshotQueryControlSignature(token string, want auth.SnapshotQueryControlBinding) (string, error)`, `auth.SnapshotQueryControlOperationAcquire`.
- Produces: `snapshotQueryControlBindingDigest(networkID string, shard uint32, account, statement, request string) string`; `snapshotQueryReservationID(networkID, account, statement, request string, generation uint64) string`; `const snapshotQueryReservationBusyReason = "artifact disposition reservation barrier is busy"` (already the string `applyBeginReservationDrain` emits; re-exported as a constant for the server); `func (f *FSM) applyBeginSnapshotQuery(c *wire.BeginSnapshotQuery, index uint64) any`.

- [ ] **Step 1: Write the failing tests**

Create `fsm/apply_snapshot_query_reservation_test.go`:

```go
package fsm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/sentioxyz/arbiter-core/wire"
)

const snapshotQueryTestSignerKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func snapshotQueryTestSigner(t *testing.T) *auth.RelaySigner {
	t.Helper()
	s, err := auth.NewRelaySigner(snapshotQueryTestSignerKey)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func signedAcquireRequest(t *testing.T, s *auth.RelaySigner, statementID, requestID string) wire.AcquireSnapshotQueryRequest {
	t.Helper()
	b := auth.SnapshotQueryControlBinding{Operation: auth.SnapshotQueryControlOperationAcquire, NetworkID: testParams().NetworkID, ClientAccount: s.Address(), StatementID: statementID, RequestID: requestID}
	jws, err := s.SignSnapshotQueryControl(b)
	if err != nil {
		t.Fatal(err)
	}
	return wire.AcquireSnapshotQueryRequest{NetworkID: b.NetworkID, KeeperShardID: 0, ClientAccount: b.ClientAccount, StatementID: b.StatementID, RequestID: b.RequestID, ControlJWS: jws}
}

func applyCmd(t *testing.T, f *FSM, index uint64, c wire.Command) any {
	t.Helper()
	data, err := wire.Encode(c)
	if err != nil {
		t.Fatal(err)
	}
	return f.Apply(&raft.Log{Index: index, Data: data})
}

func TestBeginSnapshotQueryCommitsDrainingRecordWithDeterministicDigest(t *testing.T) {
	f := newTestFSM(t)
	s := snapshotQueryTestSigner(t)
	req := signedAcquireRequest(t, s, "statement-1", "request-1")
	if got := applyCmd(t, f, 41, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: req}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	key, _ := artifactDispositionReservationKey(s.Address(), "statement-1", "request-1")
	rec := f.st.ArtifactDisposition.Reservations[key]
	if rec == nil || rec.State != ArtifactReservationDraining || rec.FencingGeneration != 1 || rec.DrainFrontierBlockSeq == nil || rec.CreatedIndex != 41 {
		t.Fatalf("draining record = %+v", rec)
	}
	if want := snapshotQueryControlBindingDigest(req.NetworkID, 0, req.ClientAccount, req.StatementID, req.RequestID); rec.ControlBindingDigest != want {
		t.Fatalf("binding digest = %s, want %s", rec.ControlBindingDigest, want)
	}
	if b := f.st.ArtifactDisposition.ReservationBarrier; b.State != ArtifactReservationDraining || b.Generation != 1 || b.RequestID != "request-1" {
		t.Fatalf("barrier = %+v", b)
	}
	// Identical replay is a lookup, never a second begin.
	if got := applyCmd(t, f, 42, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: req}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("replayed begin = %#v", got)
	}
	if f.st.ArtifactDisposition.ReservationBarrier.Generation != 1 {
		t.Fatal("replayed begin changed the fence")
	}
}

func TestBeginSnapshotQueryRefusesBadSignatureWrongAccountAndBusyBarrier(t *testing.T) {
	f := newTestFSM(t)
	s := snapshotQueryTestSigner(t)
	good := signedAcquireRequest(t, s, "statement-1", "request-1")

	forged := good
	forged.ClientAccount = "0x000000000000000000000000000000000000dead"
	if got, ok := applyCmd(t, f, 41, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: forged}}).(Rejected); !ok || !strings.Contains(got.Reason, "control") {
		t.Fatalf("forged account = %#v", got)
	}
	tampered := good
	tampered.ControlJWS = good.ControlJWS[:len(good.ControlJWS)-2] + "AA"
	if got, ok := applyCmd(t, f, 42, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: tampered}}).(Rejected); !ok || !strings.Contains(got.Reason, "control") {
		t.Fatalf("tampered token = %#v", got)
	}
	wrongNet := signedAcquireRequest(t, s, "statement-1", "request-1")
	wrongNet.NetworkID = "other-net"
	if got, ok := applyCmd(t, f, 43, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: wrongNet}}).(Rejected); !ok || !strings.Contains(got.Reason, "network") {
		t.Fatalf("wrong network = %#v", got)
	}
	if len(f.st.ArtifactDisposition.Reservations) != 0 {
		t.Fatal("a refused begin left state behind")
	}

	if got := applyCmd(t, f, 44, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: good}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	second := signedAcquireRequest(t, s, "statement-2", "request-2")
	if got, ok := applyCmd(t, f, 45, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: second}}).(Rejected); !ok || got.Reason != snapshotQueryReservationBusyReason {
		t.Fatalf("second begin during drain = %#v, want busy", got)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test --test_filter='TestBeginSnapshotQuery' --jobs=4 --test_output=errors`
Expected: build FAILS on the undefined `snapshotQueryControlBindingDigest` / `snapshotQueryReservationBusyReason` symbols (acceptable first failure); after adding only those two symbols, the first test FAILS with `begin = fsm.Rejected{Reason:"empty command"}`.

- [ ] **Step 3: Implement the begin arm**

Create `fsm/apply_snapshot_query_reservation.go`:

```go
package fsm

import (
	"fmt"
	"strconv"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/wire"
)

// snapshotQueryReservationBusyReason is the rejection every caller sees when
// another request owns the single network/shard barrier. The server maps it
// to a bounded busy response instead of an argument error.
const snapshotQueryReservationBusyReason = "artifact disposition reservation barrier is busy"

// snapshotQueryControlBindingDigest identifies one request across begin,
// lookup and release without depending on the token bytes, which differ per
// operation. It is arbiter-internal and never signed by a client.
func snapshotQueryControlBindingDigest(networkID string, shard uint32, account, statement, request string) string {
	return replay.DigestString("snapshot-query-control-binding-v1\x00" + networkID + "\x00" + strconv.FormatUint(uint64(shard), 10) + "\x00" + account + "\x00" + statement + "\x00" + request)
}

// snapshotQueryReservationID is derived from committed identity so every
// replica and the proposing leader agree on it without a random source.
func snapshotQueryReservationID(networkID, account, statement, request string, generation uint64) string {
	return replay.DigestString("snapshot-query-reservation-id-v1\x00" + networkID + "\x00" + account + "\x00" + statement + "\x00" + request + "\x00" + strconv.FormatUint(generation, 10))
}

// verifySnapshotQueryControl re-verifies the client's control token inside
// apply. The server already checked freshness and the allowlist; the FSM
// repeats the pure signature/binding check so a proposer cannot commit a
// begin or release the client never signed. Freshness is deliberately not
// re-checked here: replicas apply the same log at different wall-clock times.
func (f *FSM) verifySnapshotQueryControl(operation, networkID string, shard uint32, account, statement, request, reservationID string, generation uint64, token string) error {
	if networkID != f.st.Params.NetworkID {
		return fmt.Errorf("snapshot query control network %q does not match %q", networkID, f.st.Params.NetworkID)
	}
	if shard != 0 {
		return fmt.Errorf("snapshot query control keeper_shard_id must be 0 in v1")
	}
	want := auth.SnapshotQueryControlBinding{Operation: operation, NetworkID: networkID, KeeperShardID: shard, ClientAccount: account, StatementID: statement, RequestID: request, ReservationID: reservationID, FencingGeneration: generation}
	if _, err := auth.VerifySnapshotQueryControlSignature(token, want); err != nil {
		return fmt.Errorf("snapshot query control token: %w", err)
	}
	return nil
}

// applyBeginSnapshotQuery is the committed form of AcquireSnapshotQuery. It
// authenticates the acquire binding, then claims the global lane as draining
// through the existing transition, which captures the drain frontier and
// allocates the fence. It never grants: grant is a separate committed command
// proposed only after the frontier is published safe.
func (f *FSM) applyBeginSnapshotQuery(c *wire.BeginSnapshotQuery, index uint64) any {
	if c == nil {
		return Rejected{Reason: "begin snapshot query command is required"}
	}
	r := c.Request
	if err := f.verifySnapshotQueryControl(auth.SnapshotQueryControlOperationAcquire, r.NetworkID, r.KeeperShardID, r.ClientAccount, r.StatementID, r.RequestID, "", 0, r.ControlJWS); err != nil {
		return Rejected{Reason: err.Error()}
	}
	begin := ArtifactDispositionReservationDrainBegin{
		ClientAccount: r.ClientAccount, StatementID: r.StatementID, RequestID: r.RequestID,
		ControlBindingDigest: snapshotQueryControlBindingDigest(r.NetworkID, r.KeeperShardID, r.ClientAccount, r.StatementID, r.RequestID),
		BlockSeq:             uint64(len(f.st.Blocks)),
	}
	return f.applyBeginReservationDrain(begin, index)
}
```

In `fsm/fsm.go`, inside the `switch` at line 70, add before `case cmd.ArtifactDisposition != nil:`:

```go
	case cmd.BeginSnapshotQuery != nil:
		return f.applyBeginSnapshotQuery(cmd.BeginSnapshotQuery, l.Index)
	case cmd.GrantSnapshotQuery != nil:
		return Rejected{Reason: "grant snapshot query is not dispatched yet"}
	case cmd.ReleaseSnapshotQuery != nil:
		return Rejected{Reason: "release snapshot query is not dispatched yet"}
```

Check `applyBeginReservationDrain`'s idempotent re-begin branch (`fsm/artifact_reservation_drain.go:65`): it must return `Applied{}` for an identical identity+digest while draining. If it compares `BlockSeq` too and the second command computed a different `len(f.st.Blocks)`, keep the first record's `BlockSeq` by looking the record up before building `begin` (copy `existing.BlockSeq` when the key already exists). Record the adaptation.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel test //fsm:fsm_test --jobs=4 --test_output=errors`
Expected: PASS, including every pre-existing drain/release/seam test.

- [ ] **Step 5: Commit**

```bash
git add fsm/apply_snapshot_query_reservation.go fsm/apply_snapshot_query_reservation_test.go fsm/fsm.go fsm/BUILD.bazel
git commit -m "feat(fsm): dispatch BeginSnapshotQuery as an authenticated drain begin"
```

---

### Task 2: Fence new SI admissions while a reservation barrier is active (fsm)

**Files:**
- Modify: `fsm/admission.go:17-110` (`applySubmitStatement`), `fsm/artifact_reservation_drain.go:221-229` (comment)
- Test: `fsm/apply_snapshot_query_reservation_test.go` (append)

**Interfaces:**
- Produces: `const snapshotQueryAdmissionFencedReason = "snapshot query reservation barrier is active; retry after release"`; `func (f *FSM) snapshotQueryBarrierFencesAdmission() bool` (true when `ReservationBarrier.State` is draining, granted, consumed or resolving).

- [ ] **Step 1: Write the failing test**

Append:

```go
func TestSubmitStatementIsFencedWhileReservationBarrierIsActive(t *testing.T) {
	f := newTestFSM(t)
	s := snapshotQueryTestSigner(t)
	before := snapshotBytes(t, f)
	if got := applyCmd(t, f, 41, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: signedAcquireRequest(t, s, "statement-1", "request-1")}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	fenced := snapshotBytes(t, f)
	got := f.applySubmitStatement(testSubmitStatement(t))
	rej, ok := got.(Rejected)
	if !ok || rej.Reason != snapshotQueryAdmissionFencedReason {
		t.Fatalf("submit during drain = %#v, want fenced rejection", got)
	}
	if after := snapshotBytes(t, f); string(after) != string(fenced) {
		t.Fatal("a fenced submit mutated state")
	}
	_ = before
	// Idle barrier admits again.
	f.st.ArtifactDisposition.ReservationBarrier = ArtifactDispositionReservationBarrierState{State: ArtifactReservationIdle, Generation: 1}
	if _, ok := f.applySubmitStatement(testSubmitStatement(t)).(SubmitResult); !ok {
		t.Fatal("submit after release was not admitted")
	}
}
```

`testSubmitStatement(t)` must build a `*wire.SubmitStatement` the existing admission tests accept: reuse the helper the existing `fsm/admission_test.go` uses to produce a valid signed v2 statement (find it with `grep -n "SubmitStatement{" fsm/*_test.go`; if the helper has a different name or signature, call that helper instead and record the adaptation — the assertion is that the same statement is refused while draining and admitted when idle).

- [ ] **Step 2: Run the test to verify it fails**

Run: `bazel test //fsm:fsm_test --test_filter='TestSubmitStatementIsFencedWhileReservationBarrierIsActive' --jobs=4 --test_output=errors`
Expected: FAIL — `submit during drain = fsm.SubmitResult{...}` (the statement is admitted today).

- [ ] **Step 3: Implement the fence**

In `fsm/apply_snapshot_query_reservation.go` add:

```go
// snapshotQueryAdmissionFencedReason tells a v2 client that the network/shard
// is draining or executing a snapshot query. The server maps it to a
// retryable code; the FSM state is untouched.
const snapshotQueryAdmissionFencedReason = "snapshot query reservation barrier is active; retry after release"

// snapshotQueryBarrierFencesAdmission reports whether new SI admissions must
// wait. Draining fences so the captured frontier can be reached; granted,
// consumed and resolving fence so the reserved snapshot is not overtaken.
func (f *FSM) snapshotQueryBarrierFencesAdmission() bool {
	switch f.st.ArtifactDisposition.ReservationBarrier.State {
	case ArtifactReservationDraining, ArtifactReservationGranted, ArtifactReservationConsumed, ArtifactReservationResolving:
		return true
	}
	return false
}
```

In `fsm/admission.go` `applySubmitStatement`, immediately before the `SpentIDs` insertion (line 81), add:

```go
	if f.snapshotQueryBarrierFencesAdmission() {
		return Rejected{Reason: snapshotQueryAdmissionFencedReason}
	}
```

Placement matters: after the shape/signature checks (so a malformed statement still gets its deterministic v2 result) and before any state mutation (no `SpentIDs` insert, no sequence assignment). Replace the sentence in the drain comment (`fsm/artifact_reservation_drain.go:226-228`) `begin does not yet fence new SI admissions, which remains outstanding C2 work` with `begin fences new SI admissions through snapshotQueryBarrierFencesAdmission, so the frontier captured here is the complete set of work the grant must wait for`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel test //fsm:fsm_test --jobs=4 --test_output=errors`
Expected: PASS. If an existing admission test now fails because its fixture left a barrier active, that fixture is wrong about the new invariant; fix the fixture (set the barrier idle) rather than weakening the fence, and record it.

- [ ] **Step 5: Commit**

```bash
git add fsm/admission.go fsm/apply_snapshot_query_reservation.go fsm/apply_snapshot_query_reservation_test.go fsm/artifact_reservation_drain.go
git commit -m "feat(fsm): fence new SI admissions while a snapshot query reservation barrier is active"
```

---

### Task 3: Dispatch `GrantSnapshotQuery` — deterministic grant against the published frontier and active policy (fsm)

**Files:**
- Modify: `fsm/apply_snapshot_query_reservation.go`, `fsm/fsm.go` (replace the grant placeholder arm)
- Test: `fsm/apply_snapshot_query_reservation_test.go` (append)

**Interfaces:**
- Consumes: `applyCompleteReservationDrain`, `reservationDrainFrontierPublished`, `f.st.SafeWatermark`, `f.st.ArtifactDisposition.QueryPolicyReadiness.Policy`, the published safe manifest for `SafeWatermark.SnapshotID` (see Step 3 note), `replay.SnapshotPin`.
- Produces: `type SnapshotQueryGrantCandidate struct { ClientAccount, StatementID, RequestID string; FencingGeneration uint64; Reservation replay.SnapshotQueryReservation }`; `func (f *FSM) SnapshotQueryGrantCandidate() (SnapshotQueryGrantCandidate, bool)` — the exported read the server's grant loop uses: true only when the barrier is draining, the frontier is published safe and an active policy exists; the returned `Reservation` is exactly what the FSM will accept; `func (f *FSM) applyGrantSnapshotQuery(c *wire.GrantSnapshotQuery, index uint64) any`.

- [ ] **Step 1: Write the failing tests**

Append:

```go
func activateTestQueryPolicy(f *FSM) {
	f.st.ArtifactDisposition.QueryPolicyReadiness = &ArtifactDispositionQueryPolicyReadiness{Policy: replay.ActiveQueryPolicy{ActivationID: "activation-1", NetworkID: testParams().NetworkID, ExecutorProfileID: "executor-1", QueryProfileID: "query-1", Enabled: true}}
}

func TestGrantSnapshotQueryRequiresPublishedFrontierAndActivePolicy(t *testing.T) {
	f := newTestFSM(t)
	s := snapshotQueryTestSigner(t)
	req := signedAcquireRequest(t, s, "statement-1", "request-1")
	if got := applyCmd(t, f, 41, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: req}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	if _, ok := f.SnapshotQueryGrantCandidate(); ok {
		t.Fatal("candidate offered without an active policy")
	}
	activateTestQueryPolicy(f)
	cand, ok := f.SnapshotQueryGrantCandidate()
	if !ok {
		t.Fatal("no grant candidate although the frontier (0 blocks) is published and a policy is active")
	}
	if cand.RequestID != "request-1" || cand.FencingGeneration != 1 || cand.Reservation.ReservationID != snapshotQueryReservationID(testParams().NetworkID, s.Address(), "statement-1", "request-1", 1) || cand.Reservation.ExecutorProfileID != "executor-1" || cand.Reservation.QueryProfileID != "query-1" || cand.Reservation.ActivationID != "activation-1" || cand.Reservation.ReadSnapshot.SnapshotID != f.st.SafeWatermark.SnapshotID {
		t.Fatalf("candidate = %+v", cand)
	}
	// A proposer that alters any field is refused.
	wrong := cand.Reservation
	wrong.QueryProfileID = "query-2"
	if got, ok := applyCmd(t, f, 42, wire.Command{GrantSnapshotQuery: &wire.GrantSnapshotQuery{RequestID: cand.RequestID, Reservation: wrong}}).(Rejected); !ok || !strings.Contains(got.Reason, "reservation") {
		t.Fatalf("altered grant = %#v", got)
	}
	if got := applyCmd(t, f, 43, wire.Command{GrantSnapshotQuery: &wire.GrantSnapshotQuery{RequestID: cand.RequestID, Reservation: cand.Reservation}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("grant = %#v", got)
	}
	key, _ := artifactDispositionReservationKey(s.Address(), "statement-1", "request-1")
	rec := f.st.ArtifactDisposition.Reservations[key]
	if rec == nil || rec.State != ArtifactReservationGranted || rec.Reservation == nil || rec.Reservation.ReservationID != cand.Reservation.ReservationID || f.st.ArtifactDisposition.ReservationBarrier.State != ArtifactReservationGranted {
		t.Fatalf("granted record = %+v barrier = %+v", rec, f.st.ArtifactDisposition.ReservationBarrier)
	}
	if got := applyCmd(t, f, 44, wire.Command{GrantSnapshotQuery: &wire.GrantSnapshotQuery{RequestID: cand.RequestID, Reservation: cand.Reservation}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("replayed grant = %#v", got)
	}
	if _, ok := f.SnapshotQueryGrantCandidate(); ok {
		t.Fatal("candidate still offered after grant")
	}
}

func TestGrantSnapshotQueryWaitsForTheOpenBlockToBePublished(t *testing.T) {
	f := newTestFSM(t)
	activateTestQueryPolicy(f)
	if _, ok := f.applySubmitStatement(testSubmitStatement(t)).(SubmitResult); !ok {
		t.Fatal("seed statement was not admitted")
	}
	s := snapshotQueryTestSigner(t)
	if got := applyCmd(t, f, 41, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: signedAcquireRequest(t, s, "statement-1", "request-1")}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	if _, ok := f.SnapshotQueryGrantCandidate(); ok {
		t.Fatal("candidate offered while the open block is unpublished")
	}
	key, _ := artifactDispositionReservationKey(s.Address(), "statement-1", "request-1")
	if got := *f.st.ArtifactDisposition.Reservations[key].DrainFrontierBlockSeq; got != 1 {
		t.Fatalf("frontier = %d, want 1 (open block counts)", got)
	}
	f.st.SafeWatermark.SafeBlockSeq = 1 // published safe up to the frontier (test shortcut for seal+publish)
	if _, ok := f.SnapshotQueryGrantCandidate(); !ok {
		t.Fatal("candidate not offered after the frontier was published")
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestGrantSnapshotQuery' --jobs=4 --test_output=errors`
Expected: build FAILS on `SnapshotQueryGrantCandidate` (new API); after adding a stub returning `(SnapshotQueryGrantCandidate{}, false)`, the first test FAILS at "no grant candidate …".

- [ ] **Step 3: Implement the candidate read and the grant arm**

Add to `fsm/apply_snapshot_query_reservation.go`:

```go
// SnapshotQueryGrantCandidate is the leader's grant proposal input. Every
// field is derived from committed state so a replica can recompute and
// compare it in applyGrantSnapshotQuery.
type SnapshotQueryGrantCandidate struct {
	ClientAccount, StatementID, RequestID string
	FencingGeneration                     uint64
	Reservation                           replay.SnapshotQueryReservation
}

// currentSafeSnapshotPin builds the pin of the latest published safe
// snapshot. SafeWatermark carries the ID, block and manifest root; the state
// and schema roots come from that snapshot's published manifest.
func (f *FSM) currentSafeSnapshotPin() (replay.SnapshotPin, error) {
	w := f.st.SafeWatermark
	if w.SnapshotID == "" {
		return replay.SnapshotPin{}, fmt.Errorf("no published safe snapshot")
	}
	m, ok := f.publishedSafeManifest(w.SnapshotID)
	if !ok {
		return replay.SnapshotPin{}, fmt.Errorf("published safe manifest %s is not retained", w.SnapshotID)
	}
	return replay.SnapshotPin{NetworkID: f.st.Params.NetworkID, KeeperShardID: 0, SnapshotID: w.SnapshotID, SafeBlockSeq: w.SafeBlockSeq, ManifestRoot: w.ManifestRoot, StateRoot: m.StateRoot, SchemaSnapshotID: m.SchemaSnapshotID, SchemaRoot: m.SchemaRoot}, nil
}

func (f *FSM) snapshotQueryGrantCandidateLocked() (SnapshotQueryGrantCandidate, bool) {
	d := &f.st.ArtifactDisposition
	b := d.ReservationBarrier
	if b.State != ArtifactReservationDraining {
		return SnapshotQueryGrantCandidate{}, false
	}
	key, err := artifactDispositionReservationKey(b.ClientAccount, b.StatementID, b.RequestID)
	if err != nil {
		return SnapshotQueryGrantCandidate{}, false
	}
	active := d.Reservations[key]
	if active == nil || active.State != ArtifactReservationDraining || f.reservationDrainFrontierPublished(active) != nil {
		return SnapshotQueryGrantCandidate{}, false
	}
	policy := d.QueryPolicyReadiness
	if policy == nil || !policy.Policy.Enabled || policy.Policy.NetworkID != f.st.Params.NetworkID {
		return SnapshotQueryGrantCandidate{}, false
	}
	pin, err := f.currentSafeSnapshotPin()
	if err != nil {
		return SnapshotQueryGrantCandidate{}, false
	}
	return SnapshotQueryGrantCandidate{
		ClientAccount: active.ClientAccount, StatementID: active.StatementID, RequestID: active.RequestID, FencingGeneration: active.FencingGeneration,
		Reservation: replay.SnapshotQueryReservation{
			ReservationID:     snapshotQueryReservationID(f.st.Params.NetworkID, active.ClientAccount, active.StatementID, active.RequestID, active.FencingGeneration),
			FencingGeneration: active.FencingGeneration, ClientAccount: active.ClientAccount, StatementID: active.StatementID,
			ReadSnapshot: pin, ExecutorProfileID: policy.Policy.ExecutorProfileID, QueryProfileID: policy.Policy.QueryProfileID, ActivationID: policy.Policy.ActivationID,
		},
	}, true
}

// SnapshotQueryGrantCandidate is the exported, lock-taking read for the
// leader's grant loop.
func (f *FSM) SnapshotQueryGrantCandidate() (SnapshotQueryGrantCandidate, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.snapshotQueryGrantCandidateLocked()
}

// applyGrantSnapshotQuery accepts a grant only when it equals the candidate
// this replica derives itself: a proposer chooses nothing.
func (f *FSM) applyGrantSnapshotQuery(c *wire.GrantSnapshotQuery, index uint64) any {
	if c == nil {
		return Rejected{Reason: "grant snapshot query command is required"}
	}
	r := c.Reservation
	key, err := artifactDispositionReservationKey(r.ClientAccount, r.StatementID, c.RequestID)
	if err != nil {
		return Rejected{Reason: "grant snapshot query identity is invalid"}
	}
	if existing := f.st.ArtifactDisposition.Reservations[key]; existing != nil && existing.State == ArtifactReservationGranted && existing.Reservation != nil && reservationsEqual(existing.Reservation, &r) {
		return Applied{} // committed grant replayed after a lost response
	}
	cand, ok := f.snapshotQueryGrantCandidateLocked()
	if !ok {
		return Rejected{Reason: "snapshot query reservation is not grantable: no draining request with a published frontier and an active policy"}
	}
	if cand.RequestID != c.RequestID || !reservationsEqual(&cand.Reservation, &r) {
		return Rejected{Reason: "grant snapshot query reservation does not match the committed candidate"}
	}
	active := f.st.ArtifactDisposition.Reservations[key]
	complete := ArtifactDispositionReservationDrainComplete{
		ClientAccount: active.ClientAccount, StatementID: active.StatementID, RequestID: active.RequestID,
		ControlBindingDigest: active.ControlBindingDigest, FencingGeneration: active.FencingGeneration, BlockSeq: active.BlockSeq,
		Reservation: &r,
	}
	return f.applyCompleteReservationDrain(complete, index)
}
```

`reservationsEqual(a, b *replay.SnapshotQueryReservation) bool` already exists (used by `barrierOwnsReservationDrain`); reuse it. `publishedSafeManifest(snapshotID string) (replay.SafeSnapshotManifest, bool)` is the one lookup this plan cannot name from the baseline: implement it as a thin accessor over the state field `applyPublishSafeSnapshot` writes the published manifest into (find it with `grep -n "PublishSafeSnapshot" fsm/*.go` and follow the assignment; if that state stores only manifest roots and not the manifest, read `StateRoot`, `SchemaSnapshotID` and `SchemaRoot` from wherever `SafeStateView`/`GetPublishedSnapshot` obtains them). Record the exact field used in the report. Replace the grant placeholder arm in `fsm/fsm.go` with `return f.applyGrantSnapshotQuery(cmd.GrantSnapshotQuery, l.Index)`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel test //fsm:fsm_test --jobs=4 --test_output=errors`
Expected: PASS. `TestGrantSnapshotQueryWaitsForTheOpenBlockToBePublished` sets `SafeWatermark.SafeBlockSeq` directly as a shortcut; if `currentSafeSnapshotPin` then fails because the genesis watermark has no `SnapshotID`, seed one through `publishServerGenesis`-equivalent state in `newTestFSM` (check what `seedTestGenesis` publishes) and record it.

- [ ] **Step 5: Commit**

```bash
git add fsm/apply_snapshot_query_reservation.go fsm/apply_snapshot_query_reservation_test.go fsm/fsm.go
git commit -m "feat(fsm): dispatch GrantSnapshotQuery against the replica-derived candidate"
```

---

### Task 4: Dispatch `ReleaseSnapshotQuery` — authenticated release and conditional absent cancellation (fsm)

**Files:**
- Modify: `fsm/apply_snapshot_query_reservation.go`, `fsm/fsm.go` (replace the release placeholder arm)
- Test: `fsm/apply_snapshot_query_reservation_test.go` (append)

**Interfaces:**
- Consumes: `applyReleaseReservation` (via the same body `ApplyServerOwnedReservationRelease` wraps), `auth.SnapshotQueryControlOperationRelease`.
- Produces: `func (f *FSM) applyReleaseSnapshotQuery(c *wire.ReleaseSnapshotQuery, index uint64) any`; terminal proof = the UTF-8 bytes of the authenticated release control JWS.

- [ ] **Step 1: Write the failing tests**

Append:

```go
func signedReleaseRequest(t *testing.T, s *auth.RelaySigner, statementID, requestID, reservationID string, generation uint64) wire.ReleaseSnapshotQueryRequest {
	t.Helper()
	b := auth.SnapshotQueryControlBinding{Operation: auth.SnapshotQueryControlOperationRelease, NetworkID: testParams().NetworkID, ClientAccount: s.Address(), StatementID: statementID, RequestID: requestID, ReservationID: reservationID, FencingGeneration: generation}
	jws, err := s.SignSnapshotQueryControl(b)
	if err != nil {
		t.Fatal(err)
	}
	return wire.ReleaseSnapshotQueryRequest{NetworkID: b.NetworkID, KeeperShardID: 0, ClientAccount: b.ClientAccount, StatementID: b.StatementID, RequestID: b.RequestID, ReservationID: reservationID, FencingGeneration: generation, ControlJWS: jws}
}

func TestReleaseSnapshotQueryReleasesDrainingAndGrantedWorkAtTheExactFence(t *testing.T) {
	f := newTestFSM(t)
	s := snapshotQueryTestSigner(t)
	if got := applyCmd(t, f, 41, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: signedAcquireRequest(t, s, "statement-1", "request-1")}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	stale := signedReleaseRequest(t, s, "statement-1", "request-1", "", 2)
	if got, ok := applyCmd(t, f, 42, wire.Command{ReleaseSnapshotQuery: &wire.ReleaseSnapshotQuery{Request: stale}}).(Rejected); !ok || !strings.Contains(got.Reason, "fence") {
		t.Fatalf("stale fence release = %#v", got)
	}
	exact := signedReleaseRequest(t, s, "statement-1", "request-1", "", 1)
	if got := applyCmd(t, f, 43, wire.Command{ReleaseSnapshotQuery: &wire.ReleaseSnapshotQuery{Request: exact}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("draining release = %#v", got)
	}
	key, _ := artifactDispositionReservationKey(s.Address(), "statement-1", "request-1")
	d := f.st.ArtifactDisposition
	if d.Reservations[key] != nil || d.ReservationTombstones[key] == nil || string(d.ReservationTombstones[key].TerminalProof) != exact.ControlJWS || d.ReservationBarrier.State != ArtifactReservationIdle || d.ReservationBarrier.Generation != 2 {
		t.Fatalf("after draining release: tombstone=%+v barrier=%+v", d.ReservationTombstones[key], d.ReservationBarrier)
	}
	if got := applyCmd(t, f, 44, wire.Command{ReleaseSnapshotQuery: &wire.ReleaseSnapshotQuery{Request: exact}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("replayed release = %#v", got)
	}
	// A released request cannot be re-acquired.
	if got, ok := applyCmd(t, f, 45, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: signedAcquireRequest(t, s, "statement-1", "request-1")}}).(Rejected); !ok || !strings.Contains(got.Reason, "terminal") {
		t.Fatalf("re-acquire after release = %#v", got)
	}

	// Granted work releases with its reservation ID and generation.
	activateTestQueryPolicy(f)
	if got := applyCmd(t, f, 46, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: signedAcquireRequest(t, s, "statement-2", "request-2")}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin 2 = %#v", got)
	}
	cand, ok := f.SnapshotQueryGrantCandidate()
	if !ok {
		t.Fatal("no candidate")
	}
	if got := applyCmd(t, f, 47, wire.Command{GrantSnapshotQuery: &wire.GrantSnapshotQuery{RequestID: cand.RequestID, Reservation: cand.Reservation}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("grant 2 = %#v", got)
	}
	rel := signedReleaseRequest(t, s, "statement-2", "request-2", cand.Reservation.ReservationID, cand.FencingGeneration)
	if got := applyCmd(t, f, 48, wire.Command{ReleaseSnapshotQuery: &wire.ReleaseSnapshotQuery{Request: rel}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("granted release = %#v", got)
	}
	if f.st.ArtifactDisposition.ReservationBarrier.State != ArtifactReservationIdle {
		t.Fatal("barrier not idle after granted release")
	}
}

func TestReleaseSnapshotQueryAbsentCancellationAndBusyAndConsumed(t *testing.T) {
	f := newTestFSM(t)
	s := snapshotQueryTestSigner(t)
	// Absent request, generation zero: conditional cancellation tombstone, barrier untouched.
	absent := signedReleaseRequest(t, s, "statement-9", "request-9", "", 0)
	if got := applyCmd(t, f, 41, wire.Command{ReleaseSnapshotQuery: &wire.ReleaseSnapshotQuery{Request: absent}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("absent cancellation = %#v", got)
	}
	key9, _ := artifactDispositionReservationKey(s.Address(), "statement-9", "request-9")
	if f.st.ArtifactDisposition.ReservationTombstones[key9] == nil || f.st.ArtifactDisposition.ReservationBarrier.State != ArtifactReservationIdle {
		t.Fatal("absent cancellation did not tombstone or touched the barrier")
	}
	if got, ok := applyCmd(t, f, 42, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: signedAcquireRequest(t, s, "statement-9", "request-9")}}).(Rejected); !ok || !strings.Contains(got.Reason, "terminal") {
		t.Fatalf("late begin after cancellation = %#v", got)
	}
	// Another request owns the barrier: release of a different absent request stays a tombstone (allowed), but
	// releasing the active request from a wrong signer is refused before any mutation.
	if got := applyCmd(t, f, 43, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: signedAcquireRequest(t, s, "statement-1", "request-1")}}); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	other, err := auth.NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatal(err)
	}
	foreign := signedReleaseRequest(t, other, "statement-1", "request-1", "", 1)
	foreign.ClientAccount = s.Address() // claims the owner's account with a foreign signature
	if got, ok := applyCmd(t, f, 44, wire.Command{ReleaseSnapshotQuery: &wire.ReleaseSnapshotQuery{Request: foreign}}).(Rejected); !ok || !strings.Contains(got.Reason, "control") {
		t.Fatalf("foreign release = %#v", got)
	}
	// Consumed work is never released by a client.
	key1, _ := artifactDispositionReservationKey(s.Address(), "statement-1", "request-1")
	f.st.ArtifactDisposition.Reservations[key1].State = ArtifactReservationConsumed
	f.st.ArtifactDisposition.ReservationBarrier.State = ArtifactReservationConsumed
	if got, ok := applyCmd(t, f, 45, wire.Command{ReleaseSnapshotQuery: &wire.ReleaseSnapshotQuery{Request: signedReleaseRequest(t, s, "statement-1", "request-1", "", 1)}}).(Rejected); !ok || !strings.Contains(got.Reason, "cannot be released") {
		t.Fatalf("consumed release = %#v", got)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestReleaseSnapshotQuery' --jobs=4 --test_output=errors`
Expected: FAIL — `draining release = fsm.Rejected{Reason:"release snapshot query is not dispatched yet"}`.

- [ ] **Step 3: Implement the release arm**

Add to `fsm/apply_snapshot_query_reservation.go`:

```go
// applyReleaseSnapshotQuery is the committed form of ReleaseSnapshotQuery.
// A nonzero generation names one exact draining or granted fence; an empty
// reservation ID with generation zero is the conditional cancellation of a
// request that must still be absent when applied. The authenticated release
// token is the terminal proof the tombstone retains.
func (f *FSM) applyReleaseSnapshotQuery(c *wire.ReleaseSnapshotQuery, index uint64) any {
	if c == nil {
		return Rejected{Reason: "release snapshot query command is required"}
	}
	r := c.Request
	if err := f.verifySnapshotQueryControl(auth.SnapshotQueryControlOperationRelease, r.NetworkID, r.KeeperShardID, r.ClientAccount, r.StatementID, r.RequestID, r.ReservationID, r.FencingGeneration, r.ControlJWS); err != nil {
		return Rejected{Reason: err.Error()}
	}
	key, err := artifactDispositionReservationKey(r.ClientAccount, r.StatementID, r.RequestID)
	if err != nil {
		return Rejected{Reason: "release snapshot query identity is invalid"}
	}
	if active := f.st.ArtifactDisposition.Reservations[key]; active != nil && r.ReservationID != "" && (active.Reservation == nil || active.Reservation.ReservationID != r.ReservationID) {
		return Rejected{Reason: "release snapshot query reservation identity or fence conflict"}
	}
	release := ArtifactDispositionReservationRelease{
		ClientAccount: r.ClientAccount, StatementID: r.StatementID, RequestID: r.RequestID,
		ControlBindingDigest: snapshotQueryControlBindingDigest(r.NetworkID, r.KeeperShardID, r.ClientAccount, r.StatementID, r.RequestID),
		FencingGeneration:    r.FencingGeneration, BlockSeq: uint64(len(f.st.Blocks)),
		TerminalProof:        []byte(r.ControlJWS),
	}
	return f.applyReleaseReservation(release, index)
}
```

Replace the release placeholder arm in `fsm/fsm.go` with `return f.applyReleaseSnapshotQuery(cmd.ReleaseSnapshotQuery, l.Index)`. Check `releaseMatchesTombstone` (`fsm/artifact_reservation_release.go`): a replayed identical release must return `Applied{}`; if it compares `TerminalProof` bytes it matches (same token); if it compares `FencingGeneration` against the tombstone's incremented value, adjust the comparison to accept the request's original generation and record it.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel test //fsm:fsm_test --jobs=4 --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm/apply_snapshot_query_reservation.go fsm/apply_snapshot_query_reservation_test.go fsm/fsm.go
git commit -m "feat(fsm): dispatch ReleaseSnapshotQuery with authenticated release and conditional cancellation"
```

---

### Task 5: Snapshot format v10 — refuse frontier-less draining records (fsm)

**Files:**
- Modify: `fsm/snapshot.go:20-30` (constants), `:181-182` (accept list + error text), `:199` (allowance predicate), `:315` (disposition-restore predicate), `:318-333` (downgrade scrub), `fsm/state.go:632-638` (comment)
- Test: `fsm/snapshot_test.go` (append)

**Interfaces:**
- Produces: `snapshotVersion = 10`, `snapshotVersionV10 = 10`, `snapshotVersionV9 = 9` retained; `validateSnapshotQueryReservationFrontiers(d *ArtifactDispositionState) error` refusing any draining/granted record with nil `DrainFrontierBlockSeq`.

- [ ] **Step 1: Write the failing tests**

Append to `fsm/snapshot_test.go`:

```go
func TestSnapshotVersion10RefusesFrontierlessDrainingRecordAndAcceptsV9(t *testing.T) {
	f := newTestFSM(t)
	begin := drainBegin()
	if got := f.applyBeginReservationDrain(begin, 41); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	data := snapshotBytes(t, f)
	if data[4] != snapshotVersionV10 || snapshotVersion != 10 {
		t.Fatalf("version byte = %d, want 10", data[4])
	}
	if got := restoreInto(t, data).st.ArtifactDisposition.ReservationBarrier.State; got != ArtifactReservationDraining {
		t.Fatalf("v10 round trip lost the draining barrier: %s", got)
	}
	// A v9 container with a frontier-less draining record is refused with a named reason.
	key, _ := artifactDispositionReservationKey(begin.ClientAccount, begin.StatementID, begin.RequestID)
	f.st.ArtifactDisposition.Reservations[key].DrainFrontierBlockSeq = nil
	legacy := snapshotBytes(t, f)
	legacy[4] = snapshotVersionV9
	g, err := New(testParams())
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Restore(io.NopCloser(bytes.NewReader(legacy))); err == nil || !strings.Contains(err.Error(), "drain frontier") {
		t.Fatalf("frontier-less v9 draining record restored: %v", err)
	}
	// A v9 container without such a record still restores.
	f.st.ArtifactDisposition.Reservations[key].DrainFrontierBlockSeq = new(uint64)
	ok := snapshotBytes(t, f)
	ok[4] = snapshotVersionV9
	if _, err := New(testParams()); err != nil {
		t.Fatal(err)
	}
	if got := restoreInto(t, ok).st.ArtifactDisposition.Reservations[key]; got == nil || got.DrainFrontierBlockSeq == nil {
		t.Fatal("v9 container with a frontier did not restore")
	}
}
```

Update `fsm/artifact_reservation_release_seam_test.go` (Task 2 of Wave 0): its `data[4] != snapshotVersionV9` assertions now fail by design; change them to `snapshotVersionV10` and, because v10 refuses the nil-frontier record at restore, restructure that test to assert the refusal instead of the "survives restore" branch (keep the release-through-seam proof on an in-memory FSM without the restore round trip). Record the change in the report — this is the deliberate consequence the Wave 0 test was pinning.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestSnapshotVersion10|TestServerOwnedRelease' --jobs=4 --test_output=errors`
Expected: FAIL — `version byte = 9, want 10` and the seam test's version assertion.

- [ ] **Step 3: Implement the bump**

In `fsm/snapshot.go` constants: `snapshotVersion = 10`, add `snapshotVersionV10 = 10`, keep `snapshotVersionV9 = 9`. Extend the accept list at `:181` with `ver[0] != snapshotVersionV9` alongside the others (the current value moves to `snapshotVersion`) and add `%d` for v9 to the error text and argument list. Extend the allowance predicate at `:199` to include `snapshotVersionV9` and the disposition-restore predicate at `:315` likewise (the v9-and-later predicates simply gain one more version). Add the validator:

```go
// validateSnapshotQueryReservationFrontiers refuses a container whose
// draining or granted record predates DrainFrontierBlockSeq: such a record can
// never complete, and v10 is the version that stops carrying it forward.
func validateSnapshotQueryReservationFrontiers(d *ArtifactDispositionState) error {
	for key, r := range d.Reservations {
		if r == nil {
			continue
		}
		if (r.State == ArtifactReservationDraining || r.State == ArtifactReservationGranted) && r.DrainFrontierBlockSeq == nil {
			return fmt.Errorf("artifact disposition reservation %q has no drain frontier; release it through ApplyServerOwnedReservationRelease on the previous binary before upgrading", key)
		}
	}
	return nil
}
```

Call it in `readSnapshot` right after the existing per-field validators (after `validateArtifactDispositionSnapshotQueryAttestations`, before the derived-index rebuild at `:353`), for every accepted version. No downgrade scrub is needed (no new field). In `fsm/state.go:632-638` replace `the next snapshot version bump must either migrate or refuse such records` with `snapshot version 10 refuses such records at restore`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel test //fsm:fsm_test --jobs=4 --test_output=errors`
Expected: PASS, including `TestSnapshotVersion4_RefusesPreV3Containers` and the capacity tests that iterate accepted versions (extend their version tables with `snapshotVersionV9` where they enumerate `{snapshotVersionV7, snapshotVersionV8, snapshotVersion}` and record it).

- [ ] **Step 5: Commit**

```bash
git add fsm/snapshot.go fsm/snapshot_test.go fsm/state.go fsm/artifact_reservation_release_seam_test.go
git commit -m "feat(fsm): snapshot format v10 refuses frontier-less draining records"
```

---

### Task 6: Wire the three control RPCs behind a default-off dependency (server)

**Files:**
- Create: `server/snapshot_query_reservations.go`
- Modify: `server/server.go:44-56` (`Deps`), `server/snapshot_query_control.go` (handlers), `server/ingress.go:45-90` (map the fence sentinel), `server/BUILD.bazel` (gazelle)
- Test: `server/snapshot_query_reservations_test.go`

**Interfaces:**
- Consumes: `fsm.SnapshotQueryGrantCandidate` (Task 3), `snapshotQueryReservationBusyReason`, `snapshotQueryAdmissionFencedReason` (Tasks 1–2; export them as `fsm.SnapshotQueryReservationBusyReason` / `fsm.SnapshotQueryAdmissionFencedReason` in this task by adding exported aliases in `fsm/apply_snapshot_query_reservation.go`), `consensusLeaderCheck`, `consensusReadBarrier`, `propose`, `fsmSnapshotQueryReservationProjectionReader`, housegate `auth.EthValidator`.
- Produces: exported `server.SnapshotQueryReservationDeps{Enabled bool; Control snapshotQueryControlAuthorizer; AcquireWait time.Duration; GrantInterval time.Duration}` field `Deps.SnapshotQueryReservations *SnapshotQueryReservationDeps`; `NewHousegateSnapshotQueryControlAuthorizer(allowedAddresses []string, maxTokenAge time.Duration) snapshotQueryControlAuthorizer`; the three handlers implemented; `snapshotQueryReservationStatusFromView(view fsm.ArtifactDispositionReservationReadView, found bool, binding wire.SnapshotQueryControlBinding) *pb.SnapshotQueryReservationStatus`.

- [ ] **Step 1: Write the failing tests**

Create `server/snapshot_query_reservations_test.go`:

```go
package server

import (
	"context"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func signedControl(t *testing.T, s *auth.RelaySigner, b wire.SnapshotQueryControlBinding) string {
	t.Helper()
	jws, err := s.SignSnapshotQueryControl(auth.SnapshotQueryControlBinding(b))
	if err != nil {
		t.Fatal(err)
	}
	return jws
}

func enabledReservationDeps(t *testing.T, allowed ...string) *SnapshotQueryReservationDeps {
	t.Helper()
	return &SnapshotQueryReservationDeps{Enabled: true, Control: NewHousegateSnapshotQueryControlAuthorizer(allowed, time.Minute), AcquireWait: 200 * time.Millisecond, GrantInterval: 10 * time.Millisecond}
}

func TestSnapshotQueryControlRPCsStayOffWithoutTheDependency(t *testing.T) {
	f := newServerTestFSM(t)
	conn, _, node := startServerWithConfig(t, startServerConfig{f: f, leader: true})
	client := pb.NewArbiterIngressClient(conn)
	fields := snapshotQueryControlRequestFields()
	if _, err := client.AcquireSnapshotQuery(context.Background(), fields.acquire()); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("acquire without deps = %v", err)
	}
	select {
	case <-node.applyStarted:
		t.Fatal("a disabled acquire reached Raft")
	default:
	}
}

func TestAcquireGrantsImmediatelyWhenTheFrontierIsPublished(t *testing.T) {
	f := newServerTestFSM(t)
	s, _ := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	activateServerTestQueryPolicy(t, f)
	conn, _, _ := startServerWithConfig(t, startServerConfig{f: f, leader: true, snapshotQueryReservations: enabledReservationDeps(t, s.Address())})
	client := pb.NewArbiterIngressClient(conn)
	b := wire.SnapshotQueryControlBinding{Operation: "acquire", NetworkID: testServerNetworkID, ClientAccount: s.Address(), StatementID: "statement-1", RequestID: "request-1"}
	req := &pb.AcquireSnapshotQueryRequest{NetworkId: b.NetworkID, ClientAccount: b.ClientAccount, StatementId: b.StatementID, RequestId: b.RequestID, ControlJws: signedControl(t, s, b)}
	res, err := client.AcquireSnapshotQuery(context.Background(), req)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if res.FencingGeneration != 1 || res.ClientAccount != s.Address() || res.StatementId != "statement-1" || res.ReservationId == "" || res.ExecutorProfileId != "executor-1" {
		t.Fatalf("reservation = %+v", res)
	}
	// Lost response: lookup returns the same grant.
	lb := wire.SnapshotQueryControlBinding{Operation: "lookup", NetworkID: testServerNetworkID, ClientAccount: s.Address(), StatementID: "statement-1", RequestID: "request-1"}
	st, err := client.GetSnapshotQueryReservation(context.Background(), &pb.GetSnapshotQueryReservationRequest{NetworkId: lb.NetworkID, ClientAccount: lb.ClientAccount, StatementId: lb.StatementID, RequestId: lb.RequestID, ControlJws: signedControl(t, s, lb)})
	if err != nil || !st.Found || st.State != "granted" || st.Reservation == nil || st.Reservation.ReservationId != res.ReservationId || st.Version != 1 {
		t.Fatalf("lookup = %+v err=%v", st, err)
	}
	// Same acquire again is a lookup, not a second grant.
	again, err := client.AcquireSnapshotQuery(context.Background(), req)
	if err != nil || again.ReservationId != res.ReservationId {
		t.Fatalf("replayed acquire = %+v err=%v", again, err)
	}
	// Release at the exact fence.
	rb := wire.SnapshotQueryControlBinding{Operation: "release", NetworkID: testServerNetworkID, ClientAccount: s.Address(), StatementID: "statement-1", RequestID: "request-1", ReservationID: res.ReservationId, FencingGeneration: res.FencingGeneration}
	rel, err := client.ReleaseSnapshotQuery(context.Background(), &pb.ReleaseSnapshotQueryRequest{NetworkId: rb.NetworkID, ClientAccount: rb.ClientAccount, StatementId: rb.StatementID, RequestId: rb.RequestID, ReservationId: rb.ReservationID, FencingGeneration: rb.FencingGeneration, ControlJws: signedControl(t, s, rb)})
	if err != nil || !rel.Found || rel.State != "released" || len(rel.TerminalProof) == 0 {
		t.Fatalf("release = %+v err=%v", rel, err)
	}
}

func TestAcquireStaysDrainingWhileTheOpenBlockIsUnpublished(t *testing.T) {
	f := newServerTestFSM(t)
	s, _ := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	activateServerTestQueryPolicy(t, f)
	conn, _, node := startServerWithConfig(t, startServerConfig{f: f, leader: true, snapshotQueryReservations: enabledReservationDeps(t, s.Address())})
	mustDirectApply(t, node, wire.Command{SubmitStatement: testServerSubmitStatement(t)}) // one admitted statement in the open block
	client := pb.NewArbiterIngressClient(conn)
	b := wire.SnapshotQueryControlBinding{Operation: "acquire", NetworkID: testServerNetworkID, ClientAccount: s.Address(), StatementID: "statement-1", RequestID: "request-1"}
	_, err := client.AcquireSnapshotQuery(context.Background(), &pb.AcquireSnapshotQueryRequest{NetworkId: b.NetworkID, ClientAccount: b.ClientAccount, StatementId: b.StatementID, RequestId: b.RequestID, ControlJws: signedControl(t, s, b)})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("acquire while draining = %v, want Unavailable", err)
	}
	lb := wire.SnapshotQueryControlBinding{Operation: "lookup", NetworkID: testServerNetworkID, ClientAccount: s.Address(), StatementID: "statement-1", RequestID: "request-1"}
	st, err := client.GetSnapshotQueryReservation(context.Background(), &pb.GetSnapshotQueryReservationRequest{NetworkId: lb.NetworkID, ClientAccount: lb.ClientAccount, StatementId: lb.StatementID, RequestId: lb.RequestID, ControlJws: signedControl(t, s, lb)})
	if err != nil || !st.Found || st.State != "draining" || st.FencingGeneration != 1 {
		t.Fatalf("lookup = %+v err=%v", st, err)
	}
	// A v2 write is fenced with a retryable code while the barrier is active.
	if _, err := pb.NewArbiterIngressClient(conn).SubmitStatement(context.Background(), testServerStatementEnvelope(t)); status.Code(err) != codes.Unavailable {
		t.Fatalf("fenced submit = %v, want Unavailable", err)
	}
	// Release the draining request at generation 1 with an empty reservation ID.
	rb := wire.SnapshotQueryControlBinding{Operation: "release", NetworkID: testServerNetworkID, ClientAccount: s.Address(), StatementID: "statement-1", RequestID: "request-1", FencingGeneration: 1}
	rel, err := client.ReleaseSnapshotQuery(context.Background(), &pb.ReleaseSnapshotQueryRequest{NetworkId: rb.NetworkID, ClientAccount: rb.ClientAccount, StatementId: rb.StatementID, RequestId: rb.RequestID, FencingGeneration: 1, ControlJws: signedControl(t, s, rb)})
	if err != nil || rel.State != "released" {
		t.Fatalf("release draining = %+v err=%v", rel, err)
	}
}

func TestSnapshotQueryControlRefusesWrongSignerAndFollowerBeforeRaft(t *testing.T) {
	f := newServerTestFSM(t)
	s, _ := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	other, _ := auth.NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	conn, _, node := startServerWithConfig(t, startServerConfig{f: f, leader: true, snapshotQueryReservations: enabledReservationDeps(t, s.Address())})
	client := pb.NewArbiterIngressClient(conn)
	b := wire.SnapshotQueryControlBinding{Operation: "acquire", NetworkID: testServerNetworkID, ClientAccount: s.Address(), StatementID: "statement-1", RequestID: "request-1"}
	req := &pb.AcquireSnapshotQueryRequest{NetworkId: b.NetworkID, ClientAccount: b.ClientAccount, StatementId: b.StatementID, RequestId: b.RequestID, ControlJws: signedControl(t, other, b)}
	if _, err := client.AcquireSnapshotQuery(context.Background(), req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("foreign signature = %v, want PermissionDenied", err)
	}
	select {
	case <-node.applyStarted:
		t.Fatal("an unauthenticated acquire reached Raft")
	default:
	}
	notAllowed := wire.SnapshotQueryControlBinding{Operation: "acquire", NetworkID: testServerNetworkID, ClientAccount: other.Address(), StatementID: "statement-1", RequestID: "request-1"}
	if _, err := client.AcquireSnapshotQuery(context.Background(), &pb.AcquireSnapshotQueryRequest{NetworkId: notAllowed.NetworkID, ClientAccount: notAllowed.ClientAccount, StatementId: notAllowed.StatementID, RequestId: notAllowed.RequestID, ControlJws: signedControl(t, other, notAllowed)}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("account outside allowlist = %v", err)
	}
	// Follower refuses without proposing or answering a lookup from its local view.
	fconn, _, fnode := startServerWithConfig(t, startServerConfig{f: newServerTestFSM(t), leader: false, snapshotQueryReservations: enabledReservationDeps(t, s.Address())})
	if _, err := pb.NewArbiterIngressClient(fconn).AcquireSnapshotQuery(context.Background(), &pb.AcquireSnapshotQueryRequest{NetworkId: b.NetworkID, ClientAccount: b.ClientAccount, StatementId: b.StatementID, RequestId: b.RequestID, ControlJws: signedControl(t, s, b)}); status.Code(err) != codes.FailedPrecondition && status.Code(err) != codes.Unavailable {
		t.Fatalf("follower acquire = %v", err)
	}
	select {
	case <-fnode.applyStarted:
		t.Fatal("a follower acquire reached Raft")
	default:
	}
}
```

Helpers: `activateServerTestQueryPolicy(t, f)` calls the exported hook `fsm.TestingActivateQueryPolicy(f, replay.ActiveQueryPolicy{ActivationID: "activation-1", NetworkID: testServerNetworkID, ExecutorProfileID: "executor-1", QueryProfileID: "query-1", Enabled: true})` (added in Step 3; a plain exported function in a non-test file, because Bazel `go_test` cannot see `_test.go` symbols across packages and build tags would hide it from the `server` test target); `testServerSubmitStatement(t) *wire.SubmitStatement` and `testServerStatementEnvelope(t) *pb.StatementEnvelopeV2` reuse whatever `ingress_test.go` already builds for `SubmitStatement` (grep `StatementEnvelopeV2{` in `server/ingress_test.go`); adapt names and record. `startServerConfig` gains a `snapshotQueryReservations *SnapshotQueryReservationDeps` field that `startServerWithConfig` copies into `Deps`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel run //:gazelle && bazel test //server:server_test --test_filter='TestSnapshotQueryControl|TestAcquire' --jobs=4 --test_output=errors`
Expected: build FAILS on `SnapshotQueryReservationDeps` / `NewHousegateSnapshotQueryControlAuthorizer`; after adding the types with the handlers still refusing, `TestAcquireGrantsImmediately…` FAILS with `acquire: … FailedPrecondition`.

- [ ] **Step 3: Implement the dependency, authorizer and handlers**

Create `server/snapshot_query_reservations.go`:

```go
package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SnapshotQueryReservationDeps enables the C2 control RPCs. It is nil by
// default and cmd/arbiter never sets it; D3 wires it from verified config.
type SnapshotQueryReservationDeps struct {
	Enabled       bool
	Control       snapshotQueryControlAuthorizer
	AcquireWait   time.Duration // how long Acquire waits for the grant before answering Unavailable
	GrantInterval time.Duration // leader grant-loop tick (Task 7)
}

type housegateSnapshotQueryControlAuthorizer struct{ validator *auth.EthValidator }

// NewHousegateSnapshotQueryControlAuthorizer verifies housegate control tokens
// with freshness and an account allowlist. The FSM re-verifies the pure
// signature on apply; this layer is what keeps unauthorized callers off Raft.
func NewHousegateSnapshotQueryControlAuthorizer(allowedAddresses []string, maxTokenAge time.Duration) snapshotQueryControlAuthorizer {
	return housegateSnapshotQueryControlAuthorizer{validator: auth.NewEthValidator(allowedAddresses, maxTokenAge, true, false, "", nil)}
}

func (a housegateSnapshotQueryControlAuthorizer) AuthorizeSnapshotQueryControl(_ context.Context, account string, binding wire.SnapshotQueryControlBinding, token string) error {
	signer, err := a.validator.ValidateSnapshotQueryControl(token, auth.SnapshotQueryControlBinding(binding))
	if err != nil {
		return err
	}
	if !strings.EqualFold(signer, account) {
		return fmt.Errorf("control token signer %s is not the request account %s", signer, account)
	}
	return nil
}

func (s *Server) snapshotQueryReservationsEnabled() (*SnapshotQueryReservationDeps, error) {
	d := s.d.SnapshotQueryReservations
	if d == nil || !d.Enabled || d.Control == nil {
		return nil, status.Error(codes.FailedPrecondition, snapshotQueryAdmissionUnavailable)
	}
	return d, nil
}

func (s *Server) authorizeSnapshotQueryControl(ctx context.Context, d *SnapshotQueryReservationDeps, binding wire.SnapshotQueryControlBinding, token string) error {
	if err := d.Control.AuthorizeSnapshotQueryControl(ctx, binding.ClientAccount, binding, token); err != nil {
		return status.Errorf(codes.PermissionDenied, "snapshot query control: %v", err)
	}
	return nil
}

func (s *Server) readReservationView(ctx context.Context, binding wire.SnapshotQueryControlBinding) (fsm.ArtifactDispositionReservationReadView, bool, error) {
	if err := s.consensusReadBarrier(ctx); err != nil {
		return fsm.ArtifactDispositionReservationReadView{}, false, err
	}
	return fsmSnapshotQueryReservationProjectionReader{fsm: s.d.FSM}.read(fsm.ArtifactDispositionReservationReadSelector{ClientAccount: binding.ClientAccount, StatementID: binding.StatementID, RequestID: binding.RequestID})
}

// snapshotQueryReservationStatusFromView maps a committed record to the frozen
// status contract; found=false echoes the authenticated identity only.
func snapshotQueryReservationStatusFromView(view fsm.ArtifactDispositionReservationReadView, found bool, binding wire.SnapshotQueryControlBinding) *pb.SnapshotQueryReservationStatus {
	out := &pb.SnapshotQueryReservationStatus{Version: 1, Found: found, RequestId: binding.RequestID, ClientAccount: binding.ClientAccount, StatementId: binding.StatementID}
	if !found {
		return out
	}
	if a := view.Active; a != nil {
		out.State, out.FencingGeneration, out.BlockSeq = a.State, a.FencingGeneration, a.BlockSeq
		if a.Reservation != nil {
			out.Reservation = wire.SnapshotQueryReservationToPB(*a.Reservation)
		}
		return out
	}
	t := view.Terminal
	out.State, out.FencingGeneration, out.BlockSeq, out.TerminalProof = fsm.ArtifactReservationReleased, t.FencingGeneration, t.BlockSeq, t.TerminalProof
	if t.Reservation != nil {
		out.Reservation = wire.SnapshotQueryReservationToPB(*t.Reservation)
	}
	return out
}

func (s *Server) proposeSnapshotQueryControl(ctx context.Context, cmd wire.Command) error {
	if err := s.consensusLeaderCheck(ctx); err != nil {
		return err
	}
	res, err := s.propose(ctx, cmd)
	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument && strings.Contains(st.Message(), fsm.SnapshotQueryReservationBusyReason) {
			return status.Error(codes.ResourceExhausted, fsm.SnapshotQueryReservationBusyReason)
		}
		return err
	}
	if _, ok := res.(fsm.Applied); !ok {
		return status.Errorf(codes.Internal, "unexpected snapshot query control result %T", res)
	}
	return nil
}

func (s *Server) acquireSnapshotQuery(ctx context.Context, req *pb.AcquireSnapshotQueryRequest) (*pb.SnapshotQueryReservation, error) {
	d, err := s.snapshotQueryReservationsEnabled()
	if err != nil {
		return nil, err
	}
	binding := acquireSnapshotQueryBinding(req)
	if err := validSnapshotQueryControlBinding(binding, req.GetControlJws()); err != nil {
		return nil, err
	}
	if err := s.authorizeSnapshotQueryControl(ctx, d, binding, req.GetControlJws()); err != nil {
		return nil, err
	}
	// An identical request ID is a status lookup: never a second begin.
	view, found, err := s.readReservationView(ctx, binding)
	if err != nil {
		return nil, err
	}
	if !found {
		if err := s.proposeSnapshotQueryControl(ctx, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: wire.AcquireSnapshotQueryRequestFromPB(req)}}); err != nil {
			return nil, err
		}
	}
	wait := d.AcquireWait
	if wait <= 0 {
		wait = time.Second
	}
	deadline := time.Now().Add(wait)
	for {
		view, found, err = s.readReservationView(ctx, binding)
		if err != nil {
			return nil, err
		}
		if found && view.Terminal != nil {
			return nil, status.Error(codes.FailedPrecondition, "snapshot query reservation is terminal; it cannot be acquired again")
		}
		if found && view.Active != nil && view.Active.State == fsm.ArtifactReservationGranted && view.Active.Reservation != nil {
			return wire.SnapshotQueryReservationToPB(*view.Active.Reservation), nil
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return nil, status.Error(codes.Unavailable, "snapshot query reservation is draining; look it up by request_id")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(d.GrantInterval):
		}
	}
}

func (s *Server) getSnapshotQueryReservation(ctx context.Context, req *pb.GetSnapshotQueryReservationRequest) (*pb.SnapshotQueryReservationStatus, error) {
	d, err := s.snapshotQueryReservationsEnabled()
	if err != nil {
		return nil, err
	}
	binding := getSnapshotQueryReservationBinding(req)
	if err := validSnapshotQueryControlBinding(binding, req.GetControlJws()); err != nil {
		return nil, err
	}
	if err := s.authorizeSnapshotQueryControl(ctx, d, binding, req.GetControlJws()); err != nil {
		return nil, err
	}
	view, found, err := s.readReservationView(ctx, binding)
	if err != nil {
		return nil, err
	}
	if found && binding.ReservationID != "" {
		id, gen := reservationIdentityOfView(view)
		if id != binding.ReservationID || gen != binding.FencingGeneration {
			return nil, status.Error(codes.FailedPrecondition, "snapshot query reservation identity or generation conflict")
		}
	}
	return snapshotQueryReservationStatusFromView(view, found, binding), nil
}

func reservationIdentityOfView(view fsm.ArtifactDispositionReservationReadView) (string, uint64) {
	if view.Active != nil {
		if view.Active.Reservation != nil {
			return view.Active.Reservation.ReservationID, view.Active.FencingGeneration
		}
		return "", view.Active.FencingGeneration
	}
	if view.Terminal != nil && view.Terminal.Reservation != nil {
		return view.Terminal.Reservation.ReservationID, view.Terminal.FencingGeneration
	}
	return "", 0
}

func (s *Server) releaseSnapshotQuery(ctx context.Context, req *pb.ReleaseSnapshotQueryRequest) (*pb.SnapshotQueryReservationStatus, error) {
	d, err := s.snapshotQueryReservationsEnabled()
	if err != nil {
		return nil, err
	}
	binding := releaseSnapshotQueryBinding(req)
	if err := validSnapshotQueryControlBinding(binding, req.GetControlJws()); err != nil {
		return nil, err
	}
	if err := s.authorizeSnapshotQueryControl(ctx, d, binding, req.GetControlJws()); err != nil {
		return nil, err
	}
	if err := s.proposeSnapshotQueryControl(ctx, wire.Command{ReleaseSnapshotQuery: &wire.ReleaseSnapshotQuery{Request: wire.ReleaseSnapshotQueryRequestFromPB(req)}}); err != nil {
		return nil, err
	}
	view, found, err := s.readReservationView(ctx, binding)
	if err != nil {
		return nil, err
	}
	return snapshotQueryReservationStatusFromView(view, found, binding), nil
}

```

In `server/snapshot_query_control.go`, the three handlers become one-line delegations (`return s.acquireSnapshotQuery(ctx, req)` etc., dropping the `_` on `ctx`); keep `snapshotQueryAdmissionDisabled` only as the message source (`snapshotQueryAdmissionUnavailable`) and delete it if unused. In `server/server.go` add `SnapshotQueryReservations *SnapshotQueryReservationDeps` to `Deps` after `ArtifactDisposition`. In `server/ingress.go` `SubmitStatement`, after `propose` returns an error, add:

```go
	if st, ok := status.FromError(err); ok && st.Code() == codes.InvalidArgument && strings.Contains(st.Message(), fsm.SnapshotQueryAdmissionFencedReason) {
		return nil, status.Error(codes.Unavailable, fsm.SnapshotQueryAdmissionFencedReason)
	}
```

In `fsm/apply_snapshot_query_reservation.go` add `const SnapshotQueryReservationBusyReason = snapshotQueryReservationBusyReason` and `const SnapshotQueryAdmissionFencedReason = snapshotQueryAdmissionFencedReason`, and the test hook `func TestingActivateQueryPolicy(f *FSM, p replay.ActiveQueryPolicy)` (sets `QueryPolicyReadiness = &ArtifactDispositionQueryPolicyReadiness{Policy: p}` under the lock; comment: test support only until C1's activation command lands). The acquire wait in the test with `GrantInterval` of 10 ms relies on Task 7's grant loop; until Task 7 lands, run only the default-off, wrong-signer, follower and draining tests (the immediate-grant test is expected to fail with Unavailable until Task 7 — note it in the report and keep it in the file).

- [ ] **Step 4: Run the tests**

Run: `bazel test //server:server_test //fsm:fsm_test --jobs=4 --test_output=errors`
Expected: PASS except `TestAcquireGrantsImmediatelyWhenTheFrontierIsPublished` (needs Task 7) — commit with that single test temporarily skipped via `t.Skip("grant loop lands in Task 7")` and remove the skip in Task 7. The pre-existing `TestSnapshotQueryControlRPCsAreDefaultOffBeforeEverySideEffect` and `…ValidateEveryRequestField` must still pass unchanged.

- [ ] **Step 5: Commit**

```bash
git add server/snapshot_query_reservations.go server/snapshot_query_reservations_test.go server/snapshot_query_control.go server/server.go server/ingress.go server/server_test.go server/BUILD.bazel fsm/apply_snapshot_query_reservation.go fsm/BUILD.bazel
git commit -m "feat(server): serve the snapshot query control RPCs behind a default-off dependency"
```

---

### Task 7: Leader grant loop (server)

**Files:**
- Create: `server/snapshot_query_grant_loop.go`
- Modify: `server/server.go` (start/stop the loop when `SnapshotQueryReservations` is enabled: hook into whatever `Server` already uses for background work — if `Server` has no lifecycle today, add `func (s *Server) StartSnapshotQueryGrantLoop(ctx context.Context)` and call it from the test harness and, later, D3), `server/snapshot_query_reservations_test.go` (remove the skip)
- Test: `server/snapshot_query_grant_loop_test.go`

**Interfaces:**
- Consumes: `fsm.SnapshotQueryGrantCandidate()`, `propose`, `raftnode.ConsensusNode.VerifyLeader`.
- Produces: `func (s *Server) StartSnapshotQueryGrantLoop(ctx context.Context)` — ticks every `GrantInterval`, on the leader reads the candidate and proposes `GrantSnapshotQuery{RequestID, Reservation}`; a rejection (someone else granted or released first) is logged at debug and the loop continues; returns when `ctx` ends.

- [ ] **Step 1: Write the failing test**

Create `server/snapshot_query_grant_loop_test.go`:

```go
package server

import (
	"context"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/sentioxyz/arbiter-core/wire"
)

func TestGrantLoopProposesExactlyOneGrantForAPublishedFrontier(t *testing.T) {
	f := newServerTestFSM(t)
	s, _ := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	activateServerTestQueryPolicy(t, f)
	_, srv, node := startServerWithConfig(t, startServerConfig{f: f, leader: true, snapshotQueryReservations: enabledReservationDeps(t, s.Address())})
	b := wire.SnapshotQueryControlBinding{Operation: "acquire", NetworkID: testServerNetworkID, ClientAccount: s.Address(), StatementID: "statement-1", RequestID: "request-1"}
	mustDirectApply(t, node, wire.Command{BeginSnapshotQuery: &wire.BeginSnapshotQuery{Request: wire.AcquireSnapshotQueryRequest{NetworkID: b.NetworkID, ClientAccount: b.ClientAccount, StatementID: b.StatementID, RequestID: b.RequestID, ControlJWS: signedControl(t, s, b)}}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go srv.StartSnapshotQueryGrantLoop(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for {
		view, found, _ := f.ArtifactDispositionReservationRead(fsmSelector(s.Address(), "statement-1", "request-1"))
		if found && view.Active != nil && view.Active.State == "granted" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("grant loop never granted")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The loop must not keep proposing after the grant.
	applied := node.appliedCount()
	time.Sleep(50 * time.Millisecond)
	if node.appliedCount() != applied {
		t.Fatal("grant loop kept proposing after the grant")
	}
}
```

`fsmSelector` builds `fsm.ArtifactDispositionReservationReadSelector`; `fakeNode.appliedCount()` is a small counter added to the existing fake (`Apply` increments it under a mutex). If the fake already exposes an equivalent, use it and record the name.

- [ ] **Step 2: Run the test to verify it fails**

Run: `bazel test //server:server_test --test_filter='TestGrantLoop' --jobs=4 --test_output=errors`
Expected: build FAILS on `StartSnapshotQueryGrantLoop` / `appliedCount`.

- [ ] **Step 3: Implement the loop**

Create `server/snapshot_query_grant_loop.go`:

```go
package server

import (
	"context"
	"time"

	"github.com/sentioxyz/arbiter-core/wire"
)

// StartSnapshotQueryGrantLoop proposes the grant for a draining reservation
// once the replica-derived candidate exists. Only the leader proposes; a
// follower ticks idle. Every proposal is re-validated by every replica in
// applyGrantSnapshotQuery, so a stale or duplicate proposal is refused, never
// applied twice. It returns when ctx ends and does nothing when the
// dependency is absent or disabled.
func (s *Server) StartSnapshotQueryGrantLoop(ctx context.Context) {
	d := s.d.SnapshotQueryReservations
	if d == nil || !d.Enabled || s.d.FSM == nil {
		return
	}
	interval := d.GrantInterval
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if s.d.Node.VerifyLeader() != nil {
			continue
		}
		cand, ok := s.d.FSM.SnapshotQueryGrantCandidate()
		if !ok {
			continue
		}
		if _, err := s.propose(ctx, wire.Command{GrantSnapshotQuery: &wire.GrantSnapshotQuery{RequestID: cand.RequestID, Reservation: cand.Reservation}}); err != nil {
			s.d.Logger.Debug("snapshot query grant proposal refused", "request_id", cand.RequestID, "err", err)
		}
	}
}
```

Start the loop where the server starts other leader work; if `Server` has no such place, have `startServerWithConfig` start it for tests when the dependency is enabled (with a `t.Cleanup` cancel) and leave production start to D3, documenting that in the loop comment. Remove the `t.Skip` from Task 6's immediate-grant test.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel test //server:server_test //fsm:fsm_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS (19 targets).

- [ ] **Step 5: Commit**

```bash
git add server/snapshot_query_grant_loop.go server/snapshot_query_grant_loop_test.go server/server.go server/server_test.go server/snapshot_query_reservations_test.go server/BUILD.bazel
git commit -m "feat(server): grant snapshot query reservations from the leader once the drain frontier is published"
```

---

### Task 8: Document the reservation API and its authentication requirement (docs)

**Files:**
- Create: `docs/snapshot-query-reservations.md` in arbiter (create `docs/` if absent)
- Modify: `README.md` (one link line under the existing API/operations section)

- [ ] **Step 1: Write the document**

One paragraph per line, English. Cover: the three RPCs and their request/response contracts (from the coordination plan's C2 text and the `SnapshotQueryReservationStatus` states), the control JWS requirement (purpose, binding fields, freshness and allowlist enforced by the server, pure re-verification in the FSM), the drain semantics (frontier = sealed blocks plus the open block at begin; grant only when the safe watermark reaches it; SI admissions fenced meanwhile with `Unavailable`), busy (`ResourceExhausted`) and draining (`Unavailable`, poll lookup) responses, release rules (exact generation; empty reservation ID plus generation zero for conditional cancellation; consumed work is never client-released), the default-off `server.Deps.SnapshotQueryReservations` and `StartSnapshotQueryGrantLoop`, and the v10 snapshot refusal of frontier-less draining records.

- [ ] **Step 2: Commit and open the lane's final PR**

```bash
git add docs/snapshot-query-reservations.md README.md
git commit -m "docs: describe the snapshot query reservation API and its authentication"
```

---

## Deferred to 1a-2 / 1a-3 (recorded, not placeholders)

- Registry `Retain` and the `reservation` disposition obligation "before the response escapes", and the prepaid Q-family binding on grant: they need the capacity ledger charging and the C1 obligation state that 1a-3 builds; until then a grant is a barrier/record transition only, exactly as `applyCompleteReservationDrain` implements today.
- Fencing schema/profile changes and "unrelated safe publication" at drain begin: the commands that change schema or profile (`ActivateQueryProfile`, `PublishExecutorProfileTransition`) are not dispatched yet; 1a-3 adds them and their fence. Safe publications are deliberately not fenced here because the drain depends on them.
- Wall-clock replay determinism test across replicas (C2 Step 1 "vary every FSM replica's wall clock"): no FSM path here reads a clock; the test is added in 1a-2 together with C3's block sealing where a clock could enter.
- housegate side: `sisnapshotquery.ReservationPort` wires only `AcquireSnapshotQuery`; the lookup/release client belongs to Wave 3 T3.1.

## Subsequent plans

| Plan | Scope | Written when |
|---|---|---|
| 1a-2 — C3 singleton sequencing | `SubmitSnapshotQuery` / `GetSnapshotQueryStatus` handlers and FSM apply (`SubmitSnapshotQueryCmd`, `AbortSnapshotQueryCmd`), `expected_user_jws_hash` identity conflict, `snapshot-query-abort-v1` record, query-transfer through `ApplyServerOwnedReservationQueryTransfer`, block seal | after this plan merges |
| 1a-3 — C1 remaining | `FinishRetirement`, `CloseUse`, `OpenChallenge`, `ResolveObligation`, `GrantReservation` public path; `Capability` enable command; capacity charging so `Capacity.Enabled` can be true; `PublishExecutorProfileTransition` (26) and `RecordSnapshotArtifactReady` (27) dispatch; authenticated leader-Barrier reads for publication/history | after 1a-2 |

## Execution amendments (2026-09-21)

Corrections found while executing this plan; the merged code is the authority, the task text above is kept as written for the record.

- Task 1: `applyBeginSnapshotQuery` reuses the existing record's `BlockSeq` on an identical re-begin so a client retry after new blocks sealed is a lookup, not an identity conflict (arbiter #94).
- Task 4 Step 3: `BlockSeq` must echo the request's own record — the active record when one exists, else the existing tombstone's value on replay, else `0` for a truly absent cancellation — never `len(f.st.Blocks)`, which broke identical-replay idempotency (`releaseMatchesTombstone` compares `BlockSeq`) and diverged the tombstone's `block_seq` from the request's record (arbiter #95).
- Task 5 Step 3: the refusal text points operators at a binary that dispatches `ReleaseSnapshotQuery` but still writes snapshot version 9 (arbiter main between #95 and the v10 bump), not at `ApplyServerOwnedReservationRelease`, which has no dispatch path; the `QueryAttestations` downgrade scrub must be repinned to `< snapshotVersionV9` and the BootstrapParams-presence predicate extended with v9, both now regression-tested (arbiter #96).
- Task 6 Step 3: validate the binding shape before the capability gate so malformed requests get `InvalidArgument` regardless of the dependency (keeps the pre-existing field-validation test); hold `fakeNode.applyStarted` in a local variable because `Apply` nils the field after closing it; the two statement helpers mint separate accounts so the fenced submit is not refused as a duplicate `client_seq` first; the control authorizer is fail-closed on an empty allowlist; acquire performs one `VerifyLeader`+`Barrier` before the propose decision and polls the local FSM under `VerifyLeader` only (arbiter #97).
- Task 7 Step 3: the loop uses the cancellable `consensusLeaderCheck(ctx)` instead of an inline `VerifyLeader`, guards a nil `Node`, names the 250 ms default, and the immediate-grant test needs `publishServerGenesis` so a safe pin exists (arbiter #97).
- Deferred items recorded in the plan's ledger: the absent-key generation-0 release while another request owns the barrier commits a tombstone without touching the barrier (the seam's reviewed behavior) rather than the C2 text's "bounded busy"; refused grant proposals still commit a `Rejected` Raft entry per tick without backoff (D3 note); `TestingActivateQueryPolicy` writes readiness without a candidate until C1's activation command lands.
