# Signed INSERT ... SELECT — Wave 0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute Wave 0 of the [2026-09-20 TODO plan](2026-09-20-signed-insert-select-todo.md): close the two known correctness holes (housegate intake recovery, arbiter frontier-less draining records), bring every cross-repository pin into lockstep, make the rewriter paired-CI manifest reachable from `main`, and refresh the stale plan text, so that Wave 1 (arbiter/arbiter-core control plane) can start from a clean, consistent baseline.

**Architecture:** Seven small, independent, default-off changes across five repositories, each its own PR: two behavioral fixes with TDD (HG `pkg/storageintegrity` recovery, AR `fsm` release seam), four pin/manifest moves that prove reproducibility before they change provenance, and one docs refresh. Nothing here enables a runtime capability. Later waves get their own plans (see "Subsequent plans").

**Tech Stack:** Go 1.26, Bazel 9.1.0/Bzlmod (`bazel_dep` + `git_override` lockstep with go.mod), GitHub Actions (rewriter's private build-box paired CI), git submodules (rewriter → rewriter-proto).

**Spec:** [design](../specs/2026-09-16-signed-insert-select-design.md) (D1 drain frontier, D8 sequence-first intake); [master plan](2026-09-16-signed-insert-select.md); [coordination plan](2026-09-16-signed-insert-select-coordination.md) (C2, C4); [TODO plan](2026-09-20-signed-insert-select-todo.md) (status matrix, wave ordering, evidence). This plan argues from those; executors read the TODO plan's status matrix first.

## Global Constraints

- This is Tier 3 of [issue #153](https://github.com/housegate/housegate/issues/153); merging these changes enables no runtime capability and authorizes no deployment. Every runtime gate defaults off until the D4 release gate passes.
- Use `replay.CanonicalDigest`; preserve `safe-snapshot-data-v2`, the existing state-root formula, `housegate-row-id-v1`, and all old JWS/L3/receipt vectors byte for byte. Do not regenerate expected outputs from the new implementation.
- Housegate owns canonical replay code; arbiter-core imports it. Housegate must not import arbiter-core or sentio-node.
- Bazel is Housegate's test authority; local macOS commands do not use the Linux-only `--config=ci`. Arbiter and arbiter-core also gate on `bazel test //...`. rewriter's only real gate is its private "Build & test (build box)" workflow.
- A dependency pin moves in lockstep: `go.mod`, `go.sum`, `MODULE.bazel` (`bazel_dep` version string without the leading `v` **and** the `git_override` `commit` plus its comment) and, for rewriter, the `third_party/rewriter-proto` gitlink and `tests/testdata/snapshot_query_ci_pins.json`. A pin target must be a commit reachable from the dependency's `main` (never a PR head), proven by a clean fetch with `GOPROXY=direct GOPRIVATE=github.com/housegate,github.com/sentioxyz` into a fresh `GOMODCACHE` bounded to the changed module (never `go mod download all`).
- rewriter's paired CI manifest (`tests/testdata/snapshot_query_ci_pins.json`, policy in `.github/scripts/snapshot-query-ci.md`) describes reviewed immutable prerequisites: never rebuild or replace a prerequisite silently; any provenance change needs explicit review, updated pins and a green paired-CI run. The manifest's `sources.rg` go.mod must name `sources.rp` (`snapshot-query-paired-ci.sh:57`).
- Authoritative documentation is English, one paragraph per line. Conventional commit subjects; explicitly stage the listed files; commits end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; PR bodies end with `Refs https://github.com/housegate/housegate/issues/153`, the design link `https://github.com/housegate/housegate/pull/154` and `🤖 Generated with [Claude Code](https://claude.com/claude-code)`.
- One isolated worktree per repository lane; never modify the user's main checkouts (`/Users/uranuswch/Dev/housegate/housegate`, `/Users/uranuswch/Dev/sentio_xyz/arbiter`, `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`, `/Users/uranuswch/Dev/housegate/rewriter-go`, `/Users/uranuswch/Dev/housegate/rewriter-grpc`). Merge only on green CI, squash with branch deletion. Task review (spec + quality) precedes every merge.
- The rewriter build box (`ssh -p 30100 sentio@64.38.131.242`) is shared: its CI checkout symlinks `clickHouse` into `/home/sentio/chen/rewriter-grpc/clickHouse` and hard-resets that tree under `/home/sentio/ci/rewriter-build-box.lock` on every run. Do not build in that directory while a paired-CI run is in progress.

## Baseline (2026-09-21)

| Repo | `origin/main` | Relevant pins at that commit |
|---|---|---|
| HG housegate | `036be2e` | rewriter-go `v0.11.1-0.20260918115403-a25c526eefc8` (#36, missing #37), rewriter-proto `d3844a56d7b4` |
| AR arbiter | `a661482` | housegate `18340e79a2ef`, arbiter-core `ae1c54fe794d` |
| AC arbiter-core | `a462498` | housegate `872987639cc4`, arbiter-proto `1ac937bd4aaa` (current) |
| RG rewriter-go | `4e4a14a` | rewriter-proto `d3844a56d7b4` |
| RC rewriter | `7567dc5` | manifest `sources.rg=4e4a14a…`, `sources.hg=022ee95…` (dev-branch SHA, tool byte-identical to main), `sources.rp=d3844a5…`, `images.cpp=sha256:6cbad364…` |

Execution order: Task 1 → Task 2 (independent of 1) → Task 3 → Task 4 → Task 5 → Task 6 → Task 7. Tasks 1, 2, 3 and 7 touch disjoint repositories or files and may run as parallel lanes; Tasks 4, 5 and 6 consume the merge commits recorded by earlier tasks and run after them.

## Decisions required from uranuswch before Wave 1 (not blocking this plan)

1. rewriter-go#38 ordinary-class policy (option 1 pin measured behavior / option 2 make the class ordinary in both engines / option 3 leave as documented dead code). Recommendation: option 1.
2. HG #156 v2 append semantics: a replay owner confirms no root change on devnet2.
3. Release tags for rewriter-go / rewriter: recommendation is to tag when D3 pins them.
4. Wave 1 lane shape for arbiter: one serial lane (safest for the FSM) or C1 and C2 in parallel after the v10 snapshot bump lands first.

---

### Task 1: Recover a durably staged `PreparedOutput` record (HG, C4)

**Why:** `SnapshotQueryStagePreparedOutput` is written by `PreparedOutputStager.Stage` and accepted by the journal's stage map, but `recoverRecord` has no case for it, so it falls to `default:` and `Recover` returns `unsupported snapshot query journal stage "PreparedOutput"`. Because the recovery loop returns on the first error, one staged statement stops recovery of every record after it, and a same-statement re-`Submit` (which routes found records through `recoverRecord`) gets the same hard error instead of the accepted result.

**Files:**
- Modify: `pkg/storageintegrity/snapshot_query_intake.go:178-209` (`recoverRecord` switch) and add `recoverPreparedOutput` below it
- Test: `pkg/storageintegrity/snapshot_query_intake_test.go` (append three tests and one helper at the end)

**Interfaces:**
- Consumes (all existing, same package): `validateSnapshotQueryAccepted(env replay.SnapshotQueryEnvelope, accepted replay.SnapshotQuerySubmitResult) error`; `validateSnapshotQueryPrepared(env, accepted, p SnapshotQueryPrepared) error`; `verifyPreparedOutput(path string, want preparedHeader, digest string) error`; `preparedHeader{StatementID, InputRoot, ReservationID string; FencingGeneration, BlockSeq uint64; OutputRowsRoot string; OutputRowCount uint64; ComputedStateRoot string}`; `resultFromSubmit(statementID, inputRoot string, submit replay.SnapshotQuerySubmitResult) SnapshotQueryIntakeResult`.
- Produces: `func (s *SnapshotQueryIntake) recoverPreparedOutput(rec SnapshotQueryJournalRecord) (SnapshotQueryIntakeResult, error)` — recovery result for a `PreparedOutput` record; Wave 2's C4 stage work (T2.3) replaces the terminal part of this function with the claim/terminal flow but keeps its validation.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/storageintegrity/snapshot_query_intake_test.go` (the file already imports `context`, `testing`, `replay`; add `"os"` and `"strings"` if not present; `payloadexec` is imported by `snapshot_query_prepared_output_test.go` in the same package, add `"github.com/housegate/housegate/pkg/replay/payloadexec"` here if the compiler asks for it):

```go
// stageDurablePreparedOutput stages one real one-shot output through the public
// stager so the journal record and cache file are exactly what production
// writes; the journal must already hold the Sequenced record for env.
func stageDurablePreparedOutput(t *testing.T, journal SnapshotQueryJournal, env replay.SnapshotQueryEnvelope, accepted replay.SnapshotQuerySubmitResult) SnapshotQueryPrepared {
	t.Helper()
	out := &canonicalOutputFake{rows: []payloadexec.Row{{RowID: []byte{1, 2}, Values: []any{"value", int64(7)}, PartitionID: "p2", RawBytes: 9}}, root: replay.DigestString("canonical-output"), partitions: []string{"p2"}}
	result := replay.ExecutionResult{SnapshotQuery: &replay.SnapshotQueryEvidence{ExecutionOutcome: "applied", OutputRowsRoot: out.OutputRowsRoot(), OutputRowCount: out.RowCount()}, ComputedStateRoot: replay.DigestString("state")}
	p := PreparedOutputAdapter{
		JobValue:    replay.SnapshotQueryJob{BlockSeq: accepted.BlockSeq, Reservation: accepted.Reservation, Statement: replay.SnapshotQueryStatement{StatementSeq: accepted.StatementSeq, Envelope: env}},
		ResultValue: result,
		Rows:        out,
		CloseFunc:   func() error { return nil },
	}
	s, err := NewPreparedOutputStager(journal, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := s.Stage(context.Background(), SnapshotQueryPrepareRequest{Envelope: env, Accepted: accepted}, p)
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

func TestSnapshotQueryIntakeRecoverAcceptsDurablyPreparedOutput(t *testing.T) {
	intake, env, _, journal, _, _ := newSnapshotQueryIntakeFixture(t)
	accepted := snapshotQueryAcceptedResult(env)
	rec := newSnapshotQueryRecord(env)
	rec.Stage, rec.Submit, rec.HasSubmit = SnapshotQueryStageSequenced, accepted, true
	journal.records[rec.StatementID] = rec
	stageDurablePreparedOutput(t, journal, env, accepted)
	if got := journal.records[rec.StatementID].Stage; got != SnapshotQueryStagePreparedOutput {
		t.Fatalf("fixture stage=%q, want PreparedOutput", got)
	}
	if err := intake.Recover(context.Background()); err != nil {
		t.Fatalf("recovery aborted on a durably staged output: %v", err)
	}
	if got := journal.records[rec.StatementID]; got.Stage != SnapshotQueryStagePreparedOutput || got.PreparedOutput == nil {
		t.Fatalf("recovery altered the staged record: %+v", got)
	}
}

func TestSnapshotQueryIntakeResubmitOfPreparedOutputReturnsAcceptedWithoutSequencer(t *testing.T) {
	intake, env, events, journal, _, _ := newSnapshotQueryIntakeFixture(t)
	accepted := snapshotQueryAcceptedResult(env)
	rec := newSnapshotQueryRecord(env)
	rec.Stage, rec.Submit, rec.HasSubmit = SnapshotQueryStageSequenced, accepted, true
	journal.records[rec.StatementID] = rec
	stageDurablePreparedOutput(t, journal, env, accepted)
	before := len(*events)
	got, err := intake.Submit(context.Background(), env, snapshotQueryFakeGate{events: events, win: true})
	if err != nil {
		t.Fatalf("re-submit of a staged statement: %v", err)
	}
	if got.StatementID != rec.StatementID || got.InputRoot != env.InputRoot || got.BlockSeq != accepted.BlockSeq {
		t.Fatalf("result=%+v, want the accepted sequence for %s", got, rec.StatementID)
	}
	for _, e := range (*events)[before:] {
		if strings.Contains(e, "submit") {
			t.Fatalf("re-submit reached the sequencer: %v", (*events)[before:])
		}
	}
	if journal.records[rec.StatementID].Stage != SnapshotQueryStagePreparedOutput {
		t.Fatalf("stage=%q, want unchanged PreparedOutput", journal.records[rec.StatementID].Stage)
	}
}

func TestSnapshotQueryIntakeRecoverRefusesTamperedPreparedOutputCache(t *testing.T) {
	intake, env, _, journal, _, _ := newSnapshotQueryIntakeFixture(t)
	accepted := snapshotQueryAcceptedResult(env)
	rec := newSnapshotQueryRecord(env)
	rec.Stage, rec.Submit, rec.HasSubmit = SnapshotQueryStageSequenced, accepted, true
	journal.records[rec.StatementID] = rec
	prepared := stageDurablePreparedOutput(t, journal, env, accepted)
	if err := os.WriteFile(prepared.CachePath, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := intake.Recover(context.Background())
	if err == nil || !strings.Contains(err.Error(), "prepared output cache") {
		t.Fatalf("tampered cache recovered: %v", err)
	}
	if journal.records[rec.StatementID].Stage != SnapshotQueryStagePreparedOutput {
		t.Fatalf("stage=%q, want unchanged PreparedOutput", journal.records[rec.StatementID].Stage)
	}
}
```

Notes for the implementer: `snapshotQueryFakeSequencer` records its Submit calls in the shared `events` slice with a name containing `submit`; if the fake uses a different word, read `snapshot_query_intake_test.go:16-146` and adjust the substring in the second test. The memory journal's `List` order is map order, so do not assert on "later records" — the regression is that `Recover` returned an error at all.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestSnapshotQueryIntakeRecoverAcceptsDurablyPreparedOutput|TestSnapshotQueryIntakeResubmitOfPreparedOutput|TestSnapshotQueryIntakeRecoverRefusesTamperedPreparedOutputCache' --test_output=errors`
Expected: the first two FAIL with `unsupported snapshot query journal stage "PreparedOutput"`; the third FAILS because the error text is the unsupported-stage message, not `prepared output cache`.

- [ ] **Step 3: Implement recovery of the stage**

In `pkg/storageintegrity/snapshot_query_intake.go`, add a case to `recoverRecord` before `default:`:

```go
	case SnapshotQueryStagePreparedOutput:
		return s.recoverPreparedOutput(rec)
```

and add, directly below `recoverRecord`:

```go
// recoverPreparedOutput re-admits a durably staged one-shot output. The stage
// sits after Sequenced, so the accepted submit result is the recovered result.
// The projection and the cache bytes must still match the record exactly; any
// mismatch fails closed and leaves the record untouched, because the SELECT
// can never be repeated to recreate the rows. Later C4 stages (claim, terminal)
// extend this function rather than replacing its checks.
func (s *SnapshotQueryIntake) recoverPreparedOutput(rec SnapshotQueryJournalRecord) (SnapshotQueryIntakeResult, error) {
	if !rec.HasSubmit || rec.SubmitUnknown || rec.PreparedOutput == nil {
		return SnapshotQueryIntakeResult{}, errors.New("storageintegrity: prepared output record is not durably sequenced")
	}
	if err := validateSnapshotQueryAccepted(rec.Envelope, rec.Submit); err != nil {
		return SnapshotQueryIntakeResult{}, err
	}
	p := *rec.PreparedOutput
	if err := validateSnapshotQueryPrepared(rec.Envelope, rec.Submit, p); err != nil {
		return SnapshotQueryIntakeResult{}, err
	}
	b := rec.Envelope.Input.Binding
	want := preparedHeader{b.StatementID, rec.Envelope.InputRoot, b.ReservationID, b.FencingGeneration, rec.Submit.BlockSeq, p.OutputRowsRoot, p.OutputRowCount, p.ComputedStateRoot}
	if err := verifyPreparedOutput(p.CachePath, want, p.CacheDigest); err != nil {
		return SnapshotQueryIntakeResult{}, fmt.Errorf("storageintegrity: prepared output cache: %w", err)
	}
	return resultFromSubmit(rec.StatementID, rec.Envelope.InputRoot, rec.Submit), nil
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS, including every pre-existing `TestSnapshotQuery*` and `TestPreparedOutput*` test.

- [ ] **Step 5: Run the wider gate and format**

Run: `gofmt -l pkg/storageintegrity` (no output) and `bazel test //pkg/storageintegrity/... //pkg/plugins/sisnapshotquery/... //pkg/proxy:proxy_test --test_output=errors`.
Expected: PASS.

- [ ] **Step 6: Commit and open the PR**

```bash
git add pkg/storageintegrity/snapshot_query_intake.go pkg/storageintegrity/snapshot_query_intake_test.go
git commit -m "fix(intake): recover a durably staged PreparedOutput record instead of aborting recovery"
```

PR title: `fix(intake): recover a durably staged PreparedOutput record instead of aborting recovery`. Body: the "Why" paragraph above, the three tests, and the footer lines from Global Constraints. Record the merge commit SHA in the ledger as `HG_TASK1_SHA`; Tasks 4, 5 and 6 pin it.

---

### Task 2: Export a release seam so a frontier-less draining record has an exit (AR, C2)

**Why:** arbiter #87 added `DrainFrontierBlockSeq *uint64` (`fsm/state.go:636`, `omitempty`) to the v9 snapshot without a version bump. A v9 snapshot written by a pre-#87 binary that carries a draining record restores with a nil frontier, and `reservationDrainFrontierPublished` (`fsm/artifact_reservation_drain.go:230`) then rejects completion and grant forever. The only exit is release, but `applyReleaseReservation` (`fsm/artifact_reservation_release.go:34`) is package-private and no `ApplyServerOwned*` seam exposes it, so a future Raft adapter could not free the global lane without re-genesis. The v10 snapshot bump itself belongs to Wave 1's C2 tombstone history (T1.2), which adds the first new field; this task only guarantees the exit and pins the v9 behavior in a test.

**Files:**
- Create: `fsm/artifact_reservation_release_seam.go`
- Modify: `fsm/state.go:633-637` (comment on `DrainFrontierBlockSeq`)
- Test: `fsm/artifact_reservation_release_seam_test.go`
- Modify: `fsm/BUILD.bazel` only if gazelle changes it (`bazel run //:gazelle`)

**Interfaces:**
- Consumes (existing, same package): `applyReleaseReservation(release ArtifactDispositionReservationRelease, index uint64) any`; `applyBeginReservationDrain(begin ArtifactDispositionReservationDrainBegin, index uint64) any`; `applyCompleteReservationDrain(complete ArtifactDispositionReservationDrainComplete, index uint64) any`; test helpers `newTestFSM(t)`, `drainBegin()`, `drainCompletion(fence uint64)`, `snapshotBytes(t, f)`, `restoreInto(t, data)`, `artifactDispositionReservationKey(account, statement, request string) (string, error)`.
- Produces: `func (f *FSM) ApplyServerOwnedReservationRelease(release ArtifactDispositionReservationRelease, index uint64) any` — the Raft-adapter seam Wave 1's T1.2 proposer calls for release and conditional cancellation.

- [ ] **Step 1: Write the failing test**

Create `fsm/artifact_reservation_release_seam_test.go`:

```go
package fsm

import (
	"reflect"
	"strings"
	"testing"
)

// A pre-#87 v9 snapshot may carry a draining record without a captured drain
// frontier. Such a record can never complete or be granted; the exported
// release seam must be its exit, and both the record and its tombstone must
// survive snapshot round trips.
func TestServerOwnedReleaseFreesFrontierlessDrainingRecordAcrossRestore(t *testing.T) {
	f := newTestFSM(t)
	begin := drainBegin()
	if got := f.applyBeginReservationDrain(begin, 41); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	key, _ := artifactDispositionReservationKey(begin.ClientAccount, begin.StatementID, begin.RequestID)
	f.st.ArtifactDisposition.Reservations[key].DrainFrontierBlockSeq = nil

	g := restoreInto(t, snapshotBytes(t, f))
	record := g.st.ArtifactDisposition.Reservations[key]
	if record == nil || record.State != ArtifactReservationDraining || record.DrainFrontierBlockSeq != nil {
		t.Fatalf("frontier-less draining record did not survive restore: %+v", record)
	}
	got, ok := g.applyCompleteReservationDrain(drainCompletion(1), 42).(Rejected)
	if !ok || !strings.Contains(got.Reason, "frontier") {
		t.Fatalf("completion of a frontier-less record = %#v, want a frontier rejection", got)
	}

	release := ArtifactDispositionReservationRelease{
		ClientAccount: begin.ClientAccount, StatementID: begin.StatementID, RequestID: begin.RequestID,
		ControlBindingDigest: begin.ControlBindingDigest, FencingGeneration: 1, BlockSeq: begin.BlockSeq,
		TerminalProof: []byte("terminal"),
	}
	if got := g.ApplyServerOwnedReservationRelease(release, 43); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("release = %#v", got)
	}
	d := g.st.ArtifactDisposition
	if d.Reservations[key] != nil || d.ReservationTombstones[key] == nil || d.ReservationTombstones[key].State != ArtifactReservationReleased || d.ReservationBarrier.State != ArtifactReservationIdle {
		t.Fatalf("release did not tombstone the record and idle the barrier: %+v", d)
	}
	if h := restoreInto(t, snapshotBytes(t, g)); h.st.ArtifactDisposition.ReservationTombstones[key] == nil || h.st.ArtifactDisposition.Reservations[key] != nil {
		t.Fatal("tombstone did not survive restore")
	}
}

func TestServerOwnedReleaseRejectsForeignFenceLikeTheInternalPath(t *testing.T) {
	f := newTestFSM(t)
	begin := drainBegin()
	if got := f.applyBeginReservationDrain(begin, 41); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("begin = %#v", got)
	}
	release := ArtifactDispositionReservationRelease{
		ClientAccount: begin.ClientAccount, StatementID: begin.StatementID, RequestID: begin.RequestID,
		ControlBindingDigest: begin.ControlBindingDigest, FencingGeneration: 2, BlockSeq: begin.BlockSeq,
		TerminalProof: []byte("terminal"),
	}
	if got, ok := f.ApplyServerOwnedReservationRelease(release, 42).(Rejected); !ok || !strings.Contains(got.Reason, "fence") {
		t.Fatalf("stale fence release = %#v, want fence rejection", got)
	}
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `bazel test //fsm:fsm_test --test_filter='TestServerOwnedRelease' --test_output=errors`
Expected: build FAILS with `f.ApplyServerOwnedReservationRelease undefined` (a missing new symbol is the acceptable first failure for a new API; the behavioral assertions then run in Step 4).

- [ ] **Step 3: Add the seam and the field comment**

Create `fsm/artifact_reservation_release_seam.go`:

```go
package fsm

// ApplyServerOwnedReservationRelease is the deterministic, Raft-adapter seam
// for releasing one reservation request or committing its conditional
// cancellation tombstone.  Like the other ApplyServerOwned* seams it accepts
// no client protocol object and is not reached from FSM.Apply: current public
// command dispatch remains default-disabled.  A future adapter must call this
// only while applying a committed, private coordinator record and pass that
// log's index.
//
// It is also the only exit for a draining record whose DrainFrontierBlockSeq is
// nil (a record restored from a pre-#87 v9 snapshot): completion and grant are
// permanently refused for it, so without this seam it would hold the global
// lane until re-genesis.
func (f *FSM) ApplyServerOwnedReservationRelease(release ArtifactDispositionReservationRelease, index uint64) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applyReleaseReservation(release, index)
}
```

In `fsm/state.go`, replace the sentence `nil means the record predates this field and can never complete.` in the `DrainFrontierBlockSeq` comment with: `nil means the record predates this field and can never complete; ApplyServerOwnedReservationRelease is its only exit, and the next snapshot version bump must either migrate or refuse such records.`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle` (if the repo has that target; otherwise leave BUILD alone — a new file in an existing `go_library` glob needs no edit) then `bazel test //fsm:fsm_test --test_output=errors`.
Expected: PASS, including every pre-existing reservation, drain, snapshot and allocator test.

- [ ] **Step 5: Run the repository gate**

Run: `gofmt -l fsm` (no output), `bazel test //... --test_output=errors`.
Expected: PASS (16 test targets at the baseline).

- [ ] **Step 6: Commit and open the PR**

```bash
git add fsm/artifact_reservation_release_seam.go fsm/artifact_reservation_release_seam_test.go fsm/state.go
git commit -m "feat(fsm): export the reservation release seam so a frontier-less draining record can exit"
```

PR body: the "Why" paragraph, the note that the v10 bump is deliberately deferred to C2's tombstone history, and the footer lines.

---

### Task 3: Move housegate's rewriter-go pin to the commit that consumes rewriter-proto `d3844a5` (HG)

**Why:** housegate pins rewriter-go at `a25c526e` (#36); rewriter-go #37 (`4e4a14a`) re-pinned its own rewriter-proto requirement from an unreachable PR-head pseudo-version to `d3844a56d7b4`. Go's MVS currently masks the mismatch because housegate itself requires `d3844a56d7b4`, but the pin names a commit whose go.mod is not fetchable clean and drifts from rewriter-go's corrected main.

**Files:**
- Modify: `go.mod`, `go.sum`

**Interfaces:**
- Produces: housegate `go.mod` line `github.com/housegate/rewriter-go v0.11.1-0.20260918<time>-4e4a14a70841` (the exact pseudo-version is printed by `go list -m`); no Go API change.

- [ ] **Step 1: Resolve the target and bump**

```bash
git -C /Users/uranuswch/Dev/housegate/rewriter-go fetch -q origin main
git -C /Users/uranuswch/Dev/housegate/rewriter-go merge-base --is-ancestor 4e4a14a70841535b3213a38a0c8f6a05de72c7bb origin/main && echo reachable
GOFLAGS=-mod=mod GOPRIVATE=github.com/housegate,github.com/sentioxyz go get github.com/housegate/rewriter-go@4e4a14a70841535b3213a38a0c8f6a05de72c7bb
go mod tidy
grep -n "rewriter-go\|rewriter-proto" go.mod
```
Expected: `reachable`; `go.mod` names rewriter-go `…-4e4a14a70841` and rewriter-proto stays `v0.2.1-0.20260918115349-d3844a56d7b4`; the diff touches only `go.mod` and `go.sum`.

- [ ] **Step 2: Clean-fetch proof bounded to the changed module**

```bash
export GOMODCACHE="$(mktemp -d)" GOFLAGS=-mod=mod GOPROXY=direct GOPRIVATE=github.com/housegate
go mod download github.com/housegate/rewriter-go && go build ./... && echo clean-fetch-ok
unset GOMODCACHE GOFLAGS GOPROXY GOPRIVATE
```
Expected: `clean-fetch-ok`. Do not run `go mod download all`.

- [ ] **Step 3: Bazel gate**

Run: `bazel mod tidy && git status --short` (expect only `go.mod`/`go.sum`, plus `MODULE.bazel.lock` if Bazel refreshes it) then `bazel build //cmd:housegate && bazel test //pkg/rewriter/... //pkg/plugins/materialize/... //pkg/plugins/sisnapshotquery/... --test_output=errors`.
Expected: PASS. (`TestNativeEngineSmoke` skips without `POLYGLOT_SQL_FFI_PATH`; that is the baseline behavior.)

- [ ] **Step 4: Commit and open the PR**

```bash
git add go.mod go.sum MODULE.bazel.lock
git commit -m "chore(deps): pin rewriter-go to the main commit that consumes rewriter-proto d3844a5"
```

PR body: the "Why" paragraph, the clean-fetch command and result, and the footer lines. Record the merge SHA; if it lands before Task 1's merge, Tasks 4–6 pin whichever HG main commit contains both.

---

### Task 4: Move arbiter-core's housegate pin to HG main containing Task 1 (AC)

**Why:** arbiter-core pins housegate `8729876` (#178); HG main has since merged #179 and will merge Tasks 1 and 3. Consumers must pin a main commit that contains every change they will exercise.

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel:11` (`bazel_dep(name = "housegate", version = "0.13.2-0.<time>-<sha12>")`), `MODULE.bazel:20-21` (`git_override` comment and `commit = "<40-hex>"`)

**Interfaces:**
- Consumes: `HG_TASK1_SHA` from the ledger (the HG `origin/main` commit that contains Task 1 and Task 3; verify both with `git merge-base --is-ancestor`).
- Produces: arbiter-core `go.mod` housegate pseudo-version `AC_HG_PIN` and merge commit `AC_TASK4_SHA` for Task 5.

- [ ] **Step 1: Bump go.mod**

```bash
HG_SHA=<HG_TASK1_SHA>
git -C /Users/uranuswch/Dev/housegate/housegate fetch -q origin main
git -C /Users/uranuswch/Dev/housegate/housegate merge-base --is-ancestor "$HG_SHA" origin/main && echo reachable
GOFLAGS=-mod=mod GOPRIVATE=github.com/housegate,github.com/sentioxyz go get github.com/housegate/housegate@"$HG_SHA"
go mod tidy
grep -n "housegate/housegate " go.mod
```
Expected: `reachable`; go.mod names `v0.13.2-0.<yyyymmddhhmmss>-<sha12>`; record it as `AC_HG_PIN`.

- [ ] **Step 2: Align MODULE.bazel with the same commit**

Edit `MODULE.bazel`: set the housegate `bazel_dep` `version` to `AC_HG_PIN` without the leading `v`; set the `git_override` `commit` to the full 40-hex `HG_SHA` and its comment line to `# Resolved Housegate <AC_HG_PIN>; source is pinned by the commit below.` Then:

```bash
bazel mod tidy
grep -n "housegate" MODULE.bazel
```
Expected: the three housegate strings name the same commit.

- [ ] **Step 3: Clean-fetch proof and gate**

```bash
export GOMODCACHE="$(mktemp -d)" GOFLAGS=-mod=mod GOPROXY=direct GOPRIVATE=github.com/housegate,github.com/sentioxyz
go mod download github.com/housegate/housegate && go build ./... && echo clean-fetch-ok
unset GOMODCACHE GOFLAGS GOPROXY GOPRIVATE
bazel build //... && bazel test //... --test_output=errors
```
Expected: `clean-fetch-ok`; 12/12 test targets PASS (baseline count).

- [ ] **Step 4: Commit and open the PR**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock
git commit -m "chore(deps): consume housegate main with the PreparedOutput recovery fix"
```

Record the merge SHA as `AC_TASK4_SHA`.

---

### Task 5: Move arbiter's housegate and arbiter-core pins (AR)

**Why:** arbiter pins housegate `18340e7` (eight commits behind, missing F1/F4 and Task 1) and arbiter-core `ae1c54f` (missing #33's `VerifyEnvelope` refactor and Task 4).

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel:9-16` (the two `bazel_dep` version strings for `arbiter_core` and `housegate`), `MODULE.bazel:20-22` and `:27-29` (the two `git_override` blocks: comment + `commit`)

**Interfaces:**
- Consumes: `HG_TASK1_SHA` and `AC_HG_PIN` (Task 4), `AC_TASK4_SHA` (Task 4 merge commit). arbiter must pin the same housegate commit arbiter-core pins, or Bazel resolves two housegate versions.
- Produces: arbiter main consuming both; `AR_TASK5_SHA`.

- [ ] **Step 1: Bump both modules in go.mod**

```bash
HG_SHA=<HG_TASK1_SHA>; AC_SHA=<AC_TASK4_SHA>
git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core fetch -q origin main
git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core merge-base --is-ancestor "$AC_SHA" origin/main && echo ac-reachable
GOFLAGS=-mod=mod GOPRIVATE=github.com/housegate,github.com/sentioxyz go get github.com/housegate/housegate@"$HG_SHA" github.com/sentioxyz/arbiter-core@"$AC_SHA"
go mod tidy
grep -n "housegate/housegate \|sentioxyz/arbiter-core " go.mod
```
Expected: `ac-reachable`; the housegate pseudo-version equals `AC_HG_PIN` exactly; record the arbiter-core pseudo-version as `AR_AC_PIN`.

- [ ] **Step 2: Align MODULE.bazel**

Set `bazel_dep(name = "housegate", version = ...)` to `AC_HG_PIN` without `v`, `bazel_dep(name = "arbiter_core", version = ...)` to `AR_AC_PIN` without `v`, each `git_override` `commit` to the matching 40-hex SHA and its comment to `# Resolved <module> <pseudo-version>; source is pinned by the commit below.` Then `bazel mod tidy`.

- [ ] **Step 3: Clean-fetch proof and gate**

```bash
export GOMODCACHE="$(mktemp -d)" GOFLAGS=-mod=mod GOPROXY=direct GOPRIVATE=github.com/housegate,github.com/sentioxyz
go mod download github.com/housegate/housegate github.com/sentioxyz/arbiter-core && go build ./... && echo clean-fetch-ok
unset GOMODCACHE GOFLAGS GOPROXY GOPRIVATE
bazel build //... && bazel test //... --test_output=errors
```
Expected: `clean-fetch-ok`; 16/16 test targets PASS (baseline count). If `server/snapshot_query_control_crossrepo_test.go` fails, housegate's frozen `auth.SnapshotQueryControlOperation*` names changed — that is a real regression to report, not a pin problem.

- [ ] **Step 4: Commit and open the PR**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock
git commit -m "chore(deps): consume housegate and arbiter-core main after the Wave 0 fixes"
```

---

### Task 6: Re-pin the rewriter paired-CI manifest's housegate source to a `main` ancestor (RC)

**Why:** `tests/testdata/snapshot_query_ci_pins.json` `sources.hg = 022ee95c…` is a dev-branch SHA (merge base with main is `2e6633c`; the content was squash-merged under different SHAs). `snapshot-query-paired-ci.sh:52` fetches it by SHA on every run; if those branches are deleted the run fails exactly like the September image prune. `tools/snapshot-query-profile/` is byte-identical between `022ee95` and HG main, so the reviewed `files.profile_tool` prerequisite's provenance can be re-stated without replacing the binary — provided a rebuild from the new commit reproduces its digest.

**Files:**
- Modify: `tests/testdata/snapshot_query_ci_pins.json:6` (`"hg": "..."`)

**Interfaces:**
- Consumes: `HG_TASK1_SHA` (or any later HG main commit; must satisfy `git diff --stat 022ee95… <sha> -- tools/snapshot-query-profile` empty).
- Produces: a manifest whose every `sources.*` entry is reachable from its repository's `main`.

- [ ] **Step 1: Prove the tool source is unchanged and the binary reproduces**

In an isolated housegate worktree at `HG_TASK1_SHA` (never the main checkout):

```bash
git diff --stat 022ee95c919b9b1e4bac177f675cda8f1036d6c7 HEAD -- tools/snapshot-query-profile
bazel build //tools/snapshot-query-profile:snapshot-query-profile --platforms=@rules_go//go/toolchain:linux_amd64
shasum -a 256 bazel-bin/tools/snapshot-query-profile/snapshot-query-profile_/snapshot-query-profile
```
Expected: an empty diff and the digest `30f8d075b5a8c2a2f9260ef73c3a2aa17c8a82c4905700fdc2190ebd34195b57` (the manifest's `files.profile_tool.sha256`; the output path may differ slightly, use `bazel cquery --output=files` if needed). If the digest differs, STOP and report: the prerequisite would have to be replaced under the ci.md review policy, which is outside this task.

- [ ] **Step 2: Edit the manifest**

Replace the `"hg"` value with the full 40-hex `HG_TASK1_SHA`. `python3 -c 'import json;json.load(open("tests/testdata/snapshot_query_ci_pins.json"))'` must succeed. No other field changes (`files.profile_tool.sha256` stays because Step 1 proved reproduction).

- [ ] **Step 3: Commit, push, and let the paired CI be the gate**

```bash
git add tests/testdata/snapshot_query_ci_pins.json
git commit -m "ci: pin the manifest's housegate source to a main ancestor with an identical profile tool"
```

PR body: Step 1's diff/digest evidence, the reachability argument, and the footer. Expected: "Remote build and test (build box)" and "Build & test (build box)" SUCCESS (about 25 minutes). Do not build in `/home/sentio/chen/rewriter-grpc` while it runs. Merge only when green.

---

### Task 7: Refresh the stale plan text (HG docs)

**Why:** the coordination plan still describes arbiter's snapshot as "version 2" (it is v9 accepting 3–9, and the C2 tombstone history must be authored as v10), and the contracts plan's A4 does not record the rewriter-go#38 policy question or the dead-key finding, so the next executor would repeat Task 17's misreading.

**Files:**
- Modify: `docs/superpowers/plans/2026-09-16-signed-insert-select-coordination.md:73` and `:142`
- Modify: `docs/superpowers/plans/2026-09-16-signed-insert-select-contracts.md` (Task A4 section, after Step 4 at line 305)

- [ ] **Step 1: Edit the coordination plan**

Line 73: replace `C1 selects the coordinated still-free snapshot version from current source; version 2 and default-disabled semantics remain unchanged, and an enabled ingress cannot bypass the ledger through an old command.` with `C1 selects the coordinated still-free snapshot version from current source (v9 at 2026-09-20, accepting 3–9; the next bump is v10) and keeps default-disabled semantics unchanged, and an enabled ingress cannot bypass the ledger through an old command.`

Line 142: replace `Persist tombstones in C1's new snapshot version and migrate old snapshots with an empty request-history map.` with `Persist tombstones in the v10 snapshot version and migrate old snapshots with an empty request-history map; v10 must also migrate or refuse a v9 draining record whose DrainFrontierBlockSeq is nil (arbiter #87 added the field without a bump; ApplyServerOwnedReservationRelease is that record's only exit).`

- [ ] **Step 2: Add the A4 note to the contracts plan**

After the A4 Step 4 paragraph, add one paragraph: `Measured 2026-09-20: both engines return INVALID_INPUT "snapshot query requires one recognized statement" for GRANT/REVOKE/DESCRIBE/SHOW because polyglot's ClickHouse dialect parses them as generic command nodes; the grant/revoke/describe/show keys at rewriter-go internal/engine/snapshot_query.go:243 are unreachable and only TRUNCATE is ordinary. Whether that class becomes ordinary is the open policy question in rewriter-go#38; do not change one engine without the other, and re-measure rather than reading source when extending the corpus.`

- [ ] **Step 3: Check and commit**

Run: `grep -n "version 2 and default-disabled" docs/superpowers/plans/2026-09-16-signed-insert-select-coordination.md` (no output) and ensure each edited paragraph is still one line.

```bash
git add docs/superpowers/plans/2026-09-16-signed-insert-select-coordination.md docs/superpowers/plans/2026-09-16-signed-insert-select-contracts.md
git commit -m "docs(plans): record the v9 snapshot baseline, the v10 migration duty and the rewriter-go#38 finding"
```

Include the two 2026-09-20/21 plan documents (`2026-09-20-signed-insert-select-todo.md`, this file) in the same PR if they are still uncommitted in the executing worktree.

---

## Subsequent plans

Each later wave is written as its own plan from the corresponding component plan once its inputs exist; the TODO plan's tables give the evidence pointers.

| Plan | Scope (TODO IDs) | Written when | Source tasks |
|---|---|---|---|
| Wave 1a — arbiter control plane | T1.1 C1 completion, T1.2 C2 RPC→FSM + Raft proposer + admission fence + v10 tombstones, T1.3 C3 | after Task 2 and Task 5 merge and decision 4 | coordination plan C1–C3 |
| Wave 1b — arbiter-core publication and verifier lifecycle | T1.4 B1, T1.5 B5 completion | after Task 4 merges; T1.5 needs T1.2's lookup contract | replay plan B1, B5 |
| Wave 2 — data plane | T2.1 B2 rows, T2.2 B4 exports, T2.3 C4 stages/ports, T2.4 AC C5 source lifecycle, T2.5 AR C5 transports | after Wave 1a contracts (T1.2, T1.3) | replay plan B2/B4, coordination plan C4/C5 |
| Wave 3 — transport and activation | T3.1 D1, T3.2 D2, T3.3 D3 (+ sentio-node), T3.4 D4 | after Wave 2 | integration plan D1–D4 |
| Wave 4 — engines (parallel) | T4.1 rewriter-go#38 outcome, T4.2 re-measurement and tags, T4.3 RC operator docs | after decision 1 / on demand | contracts plan A4; #153 comment |
