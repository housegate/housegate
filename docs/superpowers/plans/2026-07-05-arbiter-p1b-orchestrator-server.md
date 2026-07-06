# Sentio Arbiter P1b — Orchestrator, gRPC Server, cmd/arbiter, Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement P1b per [2026-07-05-arbiter-p1b-orchestrator-server-design.md](../specs/2026-07-05-arbiter-p1b-orchestrator-server-design.md): fsm transition events + read facade + roll-forwards, the leader-only orchestrator with §10.2 failover re-entry, the `anchor.Client` seam with a local backend, the six gRPC services with subscribe streams and NotLeader handling, the `config` package (Open Q8), `cmd/arbiter` (raft-boltdb + TCP transport), and the in-process kill-the-leader pipeline integration test.

**Architecture:** Scan-is-truth / events-as-hint (§2 of the design): `Apply` emits compact drop-safe events into a bounded leader-local channel; the orchestrator seeds its work queues from `fsm.WorkSet()` on leadership and rescans periodically. The orchestrator proposes commands through the frozen `ConsensusNode` seam and performs all I/O; the server maps gRPC ⇄ proposals and owns the two dispatch-stream registries; cmd wires everything.

**Tech Stack:** Go 1.26.3, hashicorp/raft v1.7.3 + NEW github.com/hashicorp/raft-boltdb/v2, google.golang.org/grpc (promoted from indirect), arbiter-proto v0.2.0, housegate `pkg/replay`/`pkg/lthash`/`pkg/log`. Tests: stdlib only; bufconn for server tests.

## Global Constraints

- **One repo.** All work in `/Users/uranuswch/src/sentio_xyz/arbiter` (main, direct-to-main, conventional commits ending `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`). housegate receives documentation only.
- **fsm red lines stay green** (CI tripwires): no direct `gen/pb` import in fsm, no `time.Now`/`rand` in fsm — the new read facade and event hook included. Wall-clock reads live ONLY in orchestrator/server/anchor/cmd.
- **Events are hints:** non-blocking send, full channel drops; no correctness may depend on delivery (the kill-all-events test enforces this). The scan (`WorkSet`) is the source of truth.
- **Every orchestrator side effect is preceded by `VerifyLeader()`;** the orchestrator never mutates FSM state except by proposing commands; all actions idempotent per §10.3 keys.
- **Frozen consensus constants stay constants** (quorum=2/select=3/INSERT-only) — no new config knobs for them. Consensus parameters (`genesis.*`, `authority.allowed_addresses`) feed `fsm.Params` and must be documented "identical on every node or the cluster forks".
- **`authority.Signer` is used only by the orchestrator** — grep tripwire: no `authority.Signer` reference under `server/`. SafeState handlers perform no proposals.
- **NotLeader mapping:** `raft.ErrNotLeader`/`raft.ErrLeadershipLost` → gRPC `FAILED_PRECONDITION` with `pb.NotLeader{leader_addr}` detail; other apply failures → `UNAVAILABLE`.
- **v1 plaintext gRPC**, unauthenticated Membership (trusted network; documented). No TLS code.
- **After every task:** `go build ./... && go vet ./... && go test ./...` green; `gofmt -l .` empty (excluding `gen/`).
- ed25519/lthash/JWS conventions exactly as P1a froze them (hex signatures without 0x over hash-string bytes; lthash scalars "0x"+hex of raw 2048B; user-JWS qhash = keccak256; sql_hash = replay.DigestString).
- Clock-skew tolerance for the ingress freshness gate: 5 seconds (mirrors `authority.clockSkewToleranceSeconds`).

## File Structure (final state)

```text
arbiter/
  fsm/watch.go                    # Task 1: Event/EventKind + NewWithNotify + emissions
  fsm/watch_test.go               # Task 1
  fsm/reads.go                    # Tasks 2-3: read facade (all RLock, copies only)
  fsm/reads_test.go               # Tasks 2-3
  fsm/apply.go fsm/snapshot.go fsm/state.go   # Task 4: roll-forwards + SafeParts tracking
  fsm/promotion_test.go fsm/snapshot_test.go  # Task 4 additions
  fsm/admission_test.go fsm/threeway_test.go  # Task 5: hardening tests (+ userjws_test.go new)
  wire/convert.go wire/dispatch.go            # Task 6: exported converters + dispatch builders
  wire/wire_test.go                           # Task 6 additions
  anchor/client.go anchor/local.go anchor/local_test.go   # Task 7
  config/config.go config/duration.go config/config_test.go  # Task 8
  orchestrator/orchestrator.go    # frozen seam (unchanged)
  orchestrator/deps.go            # Task 9: Deps + directory interfaces + Config
  orchestrator/loop.go            # Task 9: Run/re-entry/tickers/event pump
  orchestrator/seal.go            # Task 9: seal trigger
  orchestrator/dispatch.go        # Task 10: evidence rows + challenge row
  orchestrator/promotion.go       # Task 11: anchor/promote/manifest/cleanup rows
  orchestrator/*_test.go          # Tasks 9-11 (fakes in orchestrator_fakes_test.go)
  server/server.go                # Task 12: Server, propose helper, NotLeader mapping
  server/safestate.go server/membership.go    # Task 12
  server/ingress.go server/claims.go          # Task 13
  server/gateway.go server/registry.go        # Task 14: subscribe streams + registries
  server/*_test.go                # Tasks 12-14 (bufconn helper in server_test.go)
  cmd/arbiter/main.go             # Task 15
  integration/pipeline_test.go    # Task 16: kill-the-leader full pipeline
  integration/fakes_test.go       # Task 16: scripted verifier/SNode clients
  README.md                       # Task 15: P1b section
  go.mod                          # Tasks 12/15: grpc direct, raft-boltdb/v2
```

Task order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 → 15 → 16 (sequential).

---

### Task 1: fsm transition events (`fsm/watch.go`)

**Files:**
- Create: `fsm/watch.go`, `fsm/watch_test.go`
- Modify: `fsm/fsm.go` (constructor + emission calls in Apply's dispatch), `fsm/apply.go`/`fsm/admission.go`/`fsm/threeway.go` (emission points)

**Interfaces:**
- Consumes: P1a FSM internals (same package).
- Produces (Tasks 9-11, 15-16 rely on): `type EventKind uint8` with constants `EventAdmitted, EventBlockSealed, EventRCBound, EventEvidenceRecorded, EventQuorumReached, EventAnchorFinal, EventPromotionIssued, EventPromotionAcked, EventManifestPublished, EventCleanupScheduled, EventCleanupAcked, EventMembershipChanged`; `type Event struct { Kind EventKind; StatementSeq, BlockSeq, PromotionSeq uint64 }`; `func NewWithNotify(params Params, notify chan<- Event) *FSM` (`New(params)` unchanged ≡ `NewWithNotify(params, nil)`); unexported `(*FSM).emit(Event)` non-blocking.

- [ ] **Step 1: Write the failing tests**

Create `fsm/watch_test.go`:

```go
package fsm

import (
	"testing"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// newNotifyFSM builds an FSM with a buffered event channel.
func newNotifyFSM(t *testing.T, buf int) (*FSM, chan Event) {
	t.Helper()
	ch := make(chan Event, buf)
	return NewWithNotify(testParams(), ch), ch
}

func drain(ch chan Event) []Event {
	var out []Event
	for {
		select {
		case e := <-ch:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestEvents_EmittedPerLifecycleStep(t *testing.T) {
	f, ch := newNotifyFSM(t, 128)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	if evs := drain(ch); len(evs) != 2 || evs[0].Kind != EventMembershipChanged || evs[1].Kind != EventMembershipChanged {
		t.Fatalf("membership events: %+v", evs)
	}
	key, account := testAccount(t)
	if r := submit(t, f, validEnvelope(t, key, account, 1)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("submit: %+v", r)
	}
	evs := drain(ch)
	if len(evs) != 1 || evs[0].Kind != EventAdmitted || evs[0].StatementSeq != 1 {
		t.Fatalf("admitted event: %+v", evs)
	}
	res := f.Apply(mkLog(t, wire.Command{SealL3Block: &wire.SealL3Block{}}))
	if _, ok := res.(SealResult); !ok {
		t.Fatalf("seal: %+v", res)
	}
	evs = drain(ch)
	if len(evs) != 1 || evs[0].Kind != EventBlockSealed || evs[0].BlockSeq != 1 {
		t.Fatalf("sealed event: %+v", evs)
	}
}

func TestEvents_RejectionEmitsNothing(t *testing.T) {
	f, ch := newNotifyFSM(t, 8)
	applyExpectReject(t, f, wire.Command{MarkActive: &wire.MarkActive{NodeID: "ghost"}})
	if evs := drain(ch); len(evs) != 0 {
		t.Fatalf("rejection must not emit: %+v", evs)
	}
}

func TestEvents_FullChannelDropsWithoutBlocking(t *testing.T) {
	f, ch := newNotifyFSM(t, 1) // capacity 1: the second emission must drop, not block
	registerActive(t, f, "s1", arbiter.NodeRoleSNode) // 2 emissions through a cap-1 channel
	if len(ch) != 1 {
		t.Fatalf("want exactly 1 buffered event, got %d", len(ch))
	}
	// If emit blocked, registerActive would deadlock and the test would time out.
}

func TestEvents_NilChannelIsNoop(t *testing.T) {
	f := New(testParams()) // P1a constructor: nil channel
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	// No panic, no block — behavior identical to P1a.
	if f.st.Nodes["s1"].Status != NodeActive {
		t.Fatal("state must still mutate")
	}
}

func TestEvents_QuorumAndPromotionKinds(t *testing.T) {
	// Reuse the Task-12/13 fixtures: drive to quorum and through promotion,
	// asserting the marker events appear (kinds only; payload-free).
	signer, params := authorityFixture(t)
	ch := make(chan Event, 256)
	f := NewWithNotify(params, ch)
	set, partHash := evidenceBlock(t, f)
	for _, rid := range set[:2] {
		attest(t, f, rid, goodReceipt(f, partHash))
		scanIn(t, f, rid, goodScan(partHash))
	}
	kinds := map[EventKind]bool{}
	for _, e := range drain(ch) {
		kinds[e.Kind] = true
	}
	for _, want := range []EventKind{EventAdmitted, EventBlockSealed, EventRCBound, EventEvidenceRecorded, EventQuorumReached} {
		if !kinds[want] {
			t.Fatalf("missing kind %v in %v", want, kinds)
		}
	}
	mustApply(t, f, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: 1, Anchor: arbiter.AnchorRef{L3BlockHash: "0xaa", StateRoot: "0xbb"}, FinalityReached: true, LastMergeableReached: true}})
	promote := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1,
		CandidateParts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}}}
	token, err := signer.SignPromotion(promote)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	mustApply(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote, AuthorityJWS: token}})
	kinds = map[EventKind]bool{}
	for _, e := range drain(ch) {
		kinds[e.Kind] = true
	}
	if !kinds[EventAnchorFinal] || !kinds[EventPromotionIssued] {
		t.Fatalf("anchor/promotion kinds missing: %v", kinds)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `cd /Users/uranuswch/src/sentio_xyz/arbiter && go test ./fsm/ -run TestEvents 2>&1 | head -5`
Expected: compile failure (`NewWithNotify`, `Event` undefined).

- [ ] **Step 3: Implement `fsm/watch.go`**

```go
package fsm

// EventKind labels a committed state transition. Events are leader-local
// WAKE HINTS for the orchestrator (design §2): payload-free, drop-safe,
// never load-bearing — the WorkSet scan is the source of truth.
type EventKind uint8

const (
	EventAdmitted EventKind = iota + 1
	EventBlockSealed
	EventRCBound
	EventEvidenceRecorded
	EventQuorumReached
	EventAnchorFinal
	EventPromotionIssued
	EventPromotionAcked
	EventManifestPublished
	EventCleanupScheduled
	EventCleanupAcked
	EventMembershipChanged
)

// Event is a compact transition notification. Zero fields mean "not scoped".
type Event struct {
	Kind         EventKind
	StatementSeq uint64
	BlockSeq     uint64
	PromotionSeq uint64
}

// emit sends non-blockingly; a full channel DROPS the event (wake hint —
// truth lives in the scan; the orchestrator's rescan ticker is the
// backstop). Never called on rejection paths.
func (f *FSM) emit(e Event) {
	if f.notify == nil {
		return
	}
	select {
	case f.notify <- e:
	default:
	}
}
```

In `fsm/fsm.go`: add field `notify chan<- Event` to `FSM`; add:

```go
// NewWithNotify builds an FSM that emits transition events into notify
// (nil = no events; New(params) keeps the P1a behavior). The channel is
// leader-local and never feeds back into Apply (§3.2).
func NewWithNotify(params Params, notify chan<- Event) *FSM {
	return &FSM{st: newState(params), notify: notify}
}
```

and make `New(params Params) *FSM { return NewWithNotify(params, nil) }`.

**Emission points** (each at the END of the successful mutation, while still holding the write lock; rejection paths emit nothing):
- `applySubmitStatement` success → `f.emit(Event{Kind: EventAdmitted, StatementSeq: seq})`
- `applySealL3Block` success → `{EventBlockSealed, BlockSeq: hdr.L3BlockSeq}`
- `bindRC` (both bind-after and adoption paths route through it) → `{EventRCBound, StatementSeq: ss.Seq, BlockSeq: ss.BlockSeq}`
- `applyRecordAttestation` / `applyRecordByteSideScan` recorded (not dup-absorbed) → `{EventEvidenceRecorded, BlockSeq: ...}`
- `reevaluateBlock` when the verdict transitions to Quorum (was not quorum before this evaluation) → `{EventQuorumReached, BlockSeq: blockSeq}`
- `applyRecordAnchorFinality` success → `{EventAnchorFinal, BlockSeq: c.L3BlockSeq}`
- `applyRecordPromotionIssued` success → `{EventPromotionIssued, PromotionSeq: c.Promote.PromotionSeq}`
- `applyRecordPromotionAck` success (incl. applied=false record) → `{EventPromotionAcked, PromotionSeq: ack.PromotionSeq}`
- `applyPublishSafeSnapshot` success → `{EventManifestPublished}`
- `applyScheduleUnsafeCleanup` newly scheduled → `{EventCleanupScheduled, PromotionSeq: ...}`; `applyRecordCleanupAck` consumed → `{EventCleanupAcked, PromotionSeq: ...}`
- `applyRegisterNode` / `applyMarkActive` / `applyEvictNode` success → `{EventMembershipChanged}`

MarkReplaying deliberately emits nothing (the orchestrator proposed it and reacts via its own ApplyFuture; the per-replica dispatch obligation is derived from the scan). Duplicate-absorbed `Applied{}` paths (first-wins re-registration, idempotent re-acks) emit nothing.

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -run 'TestEvents' -v 2>&1 | tail -8` then `go test ./... 2>&1 | tail -5`
Expected: ALL PASS; the P1a suite is unaffected (nil-channel default).

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): drop-safe transition events behind NewWithNotify

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 2: fsm read facade, part 1 (point reads)

**Files:**
- Create: `fsm/reads.go`, `fsm/reads_test.go`

**Interfaces:**
- Consumes: P1a state (same package).
- Produces (server Tasks 12-13 rely on): `type OpenBlockStats struct { Count int; StatementSeqStart uint64 }`; `type StatementAckInfo struct { Seq uint64; Env arbiter.StatementEnvelope }`; `func (f *FSM) OpenBlockStats() OpenBlockStats`; `func (f *FSM) StatementAck(flatID string) (StatementAckInfo, bool)`; `func (f *FSM) SafeWatermarkView() SafeWatermark`; `func (f *FSM) ManifestByID(id string) (*replay.SafeSnapshotManifest, bool)`; `func (f *FSM) ManifestBySafeBlock(seq uint64) (*replay.SafeSnapshotManifest, bool)`. All RLock; all return copies (manifest returned as a cloned pointer).

- [ ] **Step 1: Write the failing tests**

Create `fsm/reads_test.go`:

```go
package fsm

import (
	"testing"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

func TestReads_OpenBlockStatsAndStatementAck(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	env := validEnvelope(t, key, account, 1)
	if r := submit(t, f, env); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("submit: %+v", r)
	}
	st := f.OpenBlockStats()
	if st.Count != 1 || st.StatementSeqStart != 1 {
		t.Fatalf("open block stats: %+v", st)
	}
	flat := env.StatementID.Flat()
	info, ok := f.StatementAck(flat)
	if !ok || info.Seq != 1 {
		t.Fatalf("StatementAck: %+v ok=%v", info, ok)
	}
	// stored envelope is account-normalized; compare against the normalized form
	wantEnv := env
	wantEnv.StatementID.ClientAccount = account // validEnvelope already uses lowercase account
	if info.Env != wantEnv {
		t.Fatalf("ack env mismatch:\n got %+v\nwant %+v", info.Env, wantEnv)
	}
	if _, ok := f.StatementAck("0xnope:1:n"); ok {
		t.Fatal("unknown flat id must miss")
	}
	// returned copy must not alias state
	info.Env.SQL = "MUTATED"
	if f.st.Statements[1].Env.SQL == "MUTATED" {
		t.Fatal("StatementAck must return a copy")
	}
}

func TestReads_WatermarkAndManifests(t *testing.T) {
	f, _, partHash, _ := issuePromotion(t)
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: arbiter.PromotionAck{
		NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", PostPartitionCommitment: partHash, Applied: true}}})
	m, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq: 1, SchemaSnapshotID: "schema-genesis", SchemaRoot: "0xschr", ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []replay.TableManifest{{TableID: "db.t", PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: partHash}}}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	mustApply(t, f, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: m}})

	wm := f.SafeWatermarkView()
	if wm.SnapshotID != m.SnapshotID || wm.SafeBlockSeq != 1 {
		t.Fatalf("watermark: %+v", wm)
	}
	got, ok := f.ManifestByID(m.SnapshotID)
	if !ok || got.ManifestRoot != m.ManifestRoot {
		t.Fatalf("ManifestByID: ok=%v got=%+v", ok, got)
	}
	got.Tables[0].TableID = "MUTATED"
	if f.st.Manifests[m.SnapshotID].Tables[0].TableID == "MUTATED" {
		t.Fatal("ManifestByID must deep-copy Tables")
	}
	byBlock, ok := f.ManifestBySafeBlock(1)
	if !ok || byBlock.SnapshotID != m.SnapshotID {
		t.Fatalf("ManifestBySafeBlock: ok=%v got=%+v", ok, byBlock)
	}
	if _, ok := f.ManifestBySafeBlock(99); ok {
		t.Fatal("unknown safe block must miss")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run TestReads 2>&1 | head -5`
Expected: compile failure (methods undefined).

- [ ] **Step 3: Implement `fsm/reads.go` (part 1)**

```go
package fsm

import (
	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
)

// Read facade (design §3): every method takes RLock and returns copies —
// never interior pointers into live state. Consumed by the orchestrator
// and the gRPC server on any node.

// OpenBlockStats reports the open buffer's size. The FSM stores no
// timestamps (determinism red line): statement AGE is computed by the
// CALLER from its own leader-local clock (design §3 wall-clock note).
type OpenBlockStats struct {
	Count             int
	StatementSeqStart uint64
}

func (f *FSM) OpenBlockStats() OpenBlockStats {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return OpenBlockStats{Count: len(f.st.OpenBlock.StatementSeqs), StatementSeqStart: f.st.OpenBlock.StatementSeqStart}
}

// StatementAckInfo carries what the ingress needs for the idempotent
// re-ack path (§6): the assigned seq and the stored (account-normalized)
// envelope for field-equality comparison.
type StatementAckInfo struct {
	Seq uint64
	Env arbiter.StatementEnvelope
}

func (f *FSM) StatementAck(flatID string) (StatementAckInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	seq, ok := f.st.ByStatementID[flatID]
	if !ok {
		return StatementAckInfo{}, false
	}
	ss := f.st.Statements[seq]
	return StatementAckInfo{Seq: seq, Env: ss.Env}, true
}

// SafeWatermarkView returns the published safe-state tip.
func (f *FSM) SafeWatermarkView() SafeWatermark {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.st.SafeWatermark
}

func cloneManifest(m *replay.SafeSnapshotManifest) *replay.SafeSnapshotManifest {
	c := *m
	c.Tables = make([]replay.TableManifest, len(m.Tables))
	for i, tbl := range m.Tables {
		ct := tbl
		ct.PartitionRoots = append([]replay.PartitionCommitment(nil), tbl.PartitionRoots...)
		ct.ActiveParts = append([]replay.PartManifestEntry(nil), tbl.ActiveParts...)
		c.Tables[i] = ct
	}
	return &c
}

// ManifestByID returns a deep copy of the published manifest.
func (f *FSM) ManifestByID(id string) (*replay.SafeSnapshotManifest, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	m, ok := f.st.Manifests[id]
	if !ok {
		return nil, false
	}
	return cloneManifest(m), true
}

// ManifestBySafeBlock resolves the as_of_safe time-travel read (§11.2):
// the manifest whose SafeBlockSeq equals seq.
func (f *FSM) ManifestBySafeBlock(seq uint64) (*replay.SafeSnapshotManifest, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	for _, m := range f.st.Manifests {
		if m.SafeBlockSeq == seq {
			return cloneManifest(m), true
		}
	}
	return nil, false
}
```

(Map iteration in `ManifestBySafeBlock` is a read-only lookup with a unique key — `applyPublishSafeSnapshot` enforces strictly-advancing SafeBlockSeq, so at most one manifest matches; no ordering dependence.)

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v -run TestReads 2>&1 | tail -6` then `go test ./... 2>&1 | tail -4`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): point-read facade (ack info, watermark, manifests, open-block stats)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 3: fsm read facade, part 2 (`WorkSet` + `BlockDispatchInfo` + `ManifestInputs`)

**Files:**
- Modify: `fsm/reads.go`, `fsm/reads_test.go`

**Interfaces:**
- Consumes: Task 2 facade; P1a state.
- Produces (orchestrator Tasks 9-11 rely on — exact shapes):

```go
type BlockEvidence struct {
	BlockSeq       uint64
	VerifierSet    []string
	HasAttestation map[string]bool
	HasScan        map[string]bool
}
type BlockAnchor struct {
	BlockSeq      uint64
	ChainHash     string
	StateRoot     string // last RC's SourceClaimRoot (anchor input, §4)
	Anchored      bool
	Finality      bool
	LastMergeable bool
}
type PromotionWork struct {
	Partition          arbiter.TablePartition
	Parts              []arbiter.PartRef // undrained verified candidate parts, sorted by PartRowLtHash
	BaseSafeSnapshotID string
	BasePartitionRoot  string // LIVE partition base — the promote command's CAS anchor (§8.3)
	StatementSeqs      []uint64
}
type PendingPromotionInfo struct{ Promote arbiter.PromoteSafePartition }
type CleanupWork struct {
	PromotionSeq uint64
	Cleanup      arbiter.UnsafeCleanup
}
type WorkSet struct {
	OpenBlock          OpenBlockStats
	SealedUnmarked     []uint64
	AwaitingRC         []uint64
	EvidenceIncomplete []BlockEvidence
	QuorumFailed       []uint64
	UnanchoredVerified []BlockAnchor
	PromotablePending  []PromotionWork
	IssuedUnacked      []PendingPromotionInfo
	NeedManifest       bool
	PublishedUncleaned []CleanupWork
}
func (f *FSM) WorkSet() (WorkSet, error)                 // error only from ChainHash computation
type BlockDispatchInfo struct {
	Header          L3BlockHeader
	ChainHash       string
	SourceClaimRoot string
	Statements      []replay.Statement // the §5.6 replay projection, statement_seq order
	CandidateParts  []arbiter.PartRef
	VerifierSet     []string
	HasAttestation  map[string]bool
	HasScan         map[string]bool
}
func (f *FSM) BlockDispatchInfo(blockSeq uint64) (BlockDispatchInfo, bool)
type ManifestInputs struct {
	ParentSnapshotID string
	SafeBlockSeq     uint64                  // highest prefix-complete all-Safe block
	Tables           []replay.TableManifest  // PartitionRoots from live bases + ActiveParts from SafeParts, both sorted
}
func (f *FSM) ManifestInputs() (ManifestInputs, bool)     // false when nothing new to publish
```

Inventory rules (deterministic derivations, documented in code):
- `SealedUnmarked`: `Verifications[b]` with empty `VerifierSet` and every block statement's `RC != nil`.
- `AwaitingRC`: empty `VerifierSet`, ≥1 statement with `RC == nil` (observability-only; no orchestrator action).
- `EvidenceIncomplete`: non-empty `VerifierSet`, verdict absent or non-quorum, ≥1 member missing attestation or scan, and the block is not under/past challenge (no statement in ChallengeReplay/Rejected).
- `QuorumFailed`: all members have both bundles, verdict non-quorum, unchallenged (same exclusion).
- `UnanchoredVerified`: verdict quorum, NOT (Finality && LastMergeable).
- `PromotablePending`: statements with `Status == StatusPromotable`, their undrained `UnpromotedParts` grouped by the part's partition (from `RC.CandidateParts`), excluding partitions that already have an un-acked `PendingPromotions` entry. Parts sorted by `PartRowLtHash`; StatementSeqs sorted.
- `IssuedUnacked`: `PendingPromotions` with `Acked == false` (sorted by seq).
- `NeedManifest`: highest H such that every statement of every block ≤ H is `StatusSafe` (and H ≥ 1) is > `SafeWatermark.SafeBlockSeq`.
- `PublishedUncleaned`: acked+applied pending promotions whose parts are still present in `PromotedUnsafe`, with no `PendingCleanups[seq]`, and whose covered statements' max BlockSeq ≤ `SafeWatermark.SafeBlockSeq`; the `Cleanup` is assembled from the promote command's partition + parts.
- Task 13's applied=false semantics carry over: an `Acked` promotion that was NOT applied contributes to neither IssuedUnacked nor PublishedUncleaned (rebase/re-issue is a fresh PromotablePending round — the parts are still undrained).

- [ ] **Step 1: Write the failing tests**

Append to `fsm/reads_test.go` (drives one FSM through the pipeline stage by stage, asserting the inventory at each stop — reusing `evidenceBlock`/`attest`/`scanIn`/`issuePromotion` fixtures):

```go
func TestWorkSet_TracksPipelineStages(t *testing.T) {
	signer, params := authorityFixture(t)
	f := New(params)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	for _, id := range []string{"v1", "v2", "v3"} {
		registerActive(t, f, id, arbiter.NodeRoleVerifier)
	}
	key, account := testAccount(t)
	if r := submit(t, f, validEnvelope(t, key, account, 1)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("submit: %+v", r)
	}
	ws, err := f.WorkSet()
	if err != nil {
		t.Fatalf("WorkSet: %v", err)
	}
	if ws.OpenBlock.Count != 1 || len(ws.SealedUnmarked) != 0 {
		t.Fatalf("stage submit: %+v", ws)
	}

	mustApply(t, f, wire.Command{SealL3Block: &wire.SealL3Block{}})
	ws, _ = f.WorkSet()
	if len(ws.AwaitingRC) != 1 || ws.AwaitingRC[0] != 1 {
		t.Fatalf("stage sealed-no-rc: %+v", ws)
	}

	partHash := lthashHex("rowA")
	rc := rcFor(f, 1, "0xr00t", partHash)
	rc.PartitionNewPartSums = []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p0", NewPartsLtHashSum: partHash}}
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	ws, _ = f.WorkSet()
	if len(ws.SealedUnmarked) != 1 || ws.SealedUnmarked[0] != 1 || len(ws.AwaitingRC) != 0 {
		t.Fatalf("stage rc-bound: %+v", ws)
	}

	mustApply(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	ws, _ = f.WorkSet()
	if len(ws.EvidenceIncomplete) != 1 {
		t.Fatalf("stage marked: %+v", ws)
	}
	be := ws.EvidenceIncomplete[0]
	if be.BlockSeq != 1 || len(be.VerifierSet) != 3 || len(be.HasAttestation) != 0 {
		t.Fatalf("evidence row: %+v", be)
	}

	set := f.st.Verifications[1].VerifierSet
	for _, rid := range set[:2] {
		attest(t, f, rid, goodReceipt(f, partHash))
		scanIn(t, f, rid, goodScan(partHash))
	}
	ws, _ = f.WorkSet()
	if len(ws.EvidenceIncomplete) != 0 { // quorum reached: no more dispatch obligation
		t.Fatalf("stage quorum: %+v", ws)
	}
	if len(ws.UnanchoredVerified) != 1 || ws.UnanchoredVerified[0].BlockSeq != 1 || ws.UnanchoredVerified[0].Anchored {
		t.Fatalf("unanchored: %+v", ws.UnanchoredVerified)
	}
	wantChain, _ := f.st.Blocks[0].ChainHash()
	if ws.UnanchoredVerified[0].ChainHash != wantChain || ws.UnanchoredVerified[0].StateRoot != "0xr00t" {
		t.Fatalf("anchor inputs: %+v", ws.UnanchoredVerified[0])
	}

	mustApply(t, f, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: 1,
		Anchor: arbiter.AnchorRef{L3BlockHash: wantChain, StateRoot: "0xr00t"}, FinalityReached: true, LastMergeableReached: true}})
	ws, _ = f.WorkSet()
	if len(ws.UnanchoredVerified) != 0 || len(ws.PromotablePending) != 1 {
		t.Fatalf("stage promotable: %+v", ws)
	}
	pw := ws.PromotablePending[0]
	if pw.Partition != (arbiter.TablePartition{TableID: "db.t", PartitionID: "p0"}) ||
		len(pw.Parts) != 1 || pw.Parts[0].PartRowLtHash != partHash || pw.BasePartitionRoot != "" {
		t.Fatalf("promotion work: %+v", pw)
	}

	promote := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1, CandidateParts: pw.Parts}
	token, err := signer.SignPromotion(promote)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	mustApply(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote, AuthorityJWS: token}})
	ws, _ = f.WorkSet()
	if len(ws.PromotablePending) != 0 || len(ws.IssuedUnacked) != 1 || ws.IssuedUnacked[0].Promote.PromotionSeq != 1 {
		t.Fatalf("stage issued: %+v", ws)
	}

	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: arbiter.PromotionAck{
		NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", PostPartitionCommitment: partHash, Applied: true}}})
	ws, _ = f.WorkSet()
	if len(ws.IssuedUnacked) != 0 || !ws.NeedManifest {
		t.Fatalf("stage acked: %+v", ws)
	}
	if len(ws.PublishedUncleaned) != 0 { // watermark not yet advanced: cleanup waits for the manifest
		t.Fatalf("cleanup must wait for publish: %+v", ws.PublishedUncleaned)
	}

	mi, ok := f.ManifestInputs()
	if !ok || mi.SafeBlockSeq != 1 || len(mi.Tables) != 1 || len(mi.Tables[0].ActiveParts) != 1 {
		t.Fatalf("manifest inputs: ok=%v %+v", ok, mi)
	}
	m := replay.SafeSnapshotManifest{
		ParentSnapshotID: mi.ParentSnapshotID, SafeBlockSeq: mi.SafeBlockSeq,
		SchemaSnapshotID: params.SchemaSnapshotID, SchemaRoot: replay.DigestString("schema:" + params.SchemaSnapshotID),
		ExecutorProfileID: params.ExecutorProfileID, Tables: mi.Tables,
	}
	sealed, err := m.Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	mustApply(t, f, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: sealed}})
	ws, _ = f.WorkSet()
	if ws.NeedManifest {
		t.Fatal("manifest published: NeedManifest must clear")
	}
	if len(ws.PublishedUncleaned) != 1 || ws.PublishedUncleaned[0].PromotionSeq != 1 ||
		len(ws.PublishedUncleaned[0].Cleanup.Parts) != 1 {
		t.Fatalf("stage cleanup: %+v", ws.PublishedUncleaned)
	}

	ctoken, err := signer.SignCleanup(ws.PublishedUncleaned[0].Cleanup)
	if err != nil {
		t.Fatalf("sign cleanup: %v", err)
	}
	mustApply(t, f, wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: ws.PublishedUncleaned[0].Cleanup, AuthorityJWS: ctoken}})
	mustApply(t, f, wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0"}}})
	ws, _ = f.WorkSet()
	if len(ws.PublishedUncleaned) != 0 {
		t.Fatalf("stage done: %+v", ws.PublishedUncleaned)
	}
}

func TestWorkSet_QuorumFailedAndChallengeExclusion(t *testing.T) {
	_, params := authorityFixture(t)
	f := New(params)
	set, partHash := evidenceBlock(t, f)
	bad := goodReceipt(f, partHash)
	bad.ComputedStateRoot = "0xdifferent"
	for _, rid := range set[:2] {
		attest(t, f, rid, bad)
		scanIn(t, f, rid, goodScan(partHash))
	}
	attest(t, f, set[2], goodReceipt(f, partHash))
	scanIn(t, f, set[2], goodScan(partHash))
	ws, _ := f.WorkSet()
	if len(ws.QuorumFailed) != 1 || ws.QuorumFailed[0] != 1 || len(ws.EvidenceIncomplete) != 0 {
		t.Fatalf("quorum-failed inventory: %+v", ws)
	}
	mustApply(t, f, wire.Command{OpenChallenge: &wire.OpenChallenge{BlockSeq: 1, Reason: "mismatch", OpenedBy: "orchestrator"}})
	ws, _ = f.WorkSet()
	if len(ws.QuorumFailed) != 0 {
		t.Fatal("challenged block must leave the QuorumFailed inventory")
	}
}

func TestBlockDispatchInfo_ReplayJobMaterial(t *testing.T) {
	_, params := authorityFixture(t)
	f := New(params)
	_, partHash := evidenceBlock(t, f)
	info, ok := f.BlockDispatchInfo(1)
	if !ok {
		t.Fatal("block 1 must resolve")
	}
	if info.Header.L3BlockSeq != 1 || info.SourceClaimRoot != "0xr00t" || len(info.VerifierSet) != 3 {
		t.Fatalf("info: %+v", info)
	}
	if len(info.Statements) != 1 {
		t.Fatalf("projection count: %+v", info.Statements)
	}
	s := info.Statements[0]
	ss := f.st.Statements[1]
	if s.StatementID != ss.Env.StatementID.Flat() || s.StatementSeq != 1 || s.SQL != ss.Env.SQL ||
		s.SQLHash != ss.Env.SQLHash || s.TargetTableID != ss.Env.TargetTableID || s.UserJWS != ss.Env.UserJWS {
		t.Fatalf("projection fields: %+v", s)
	}
	if len(info.CandidateParts) != 1 || info.CandidateParts[0].PartRowLtHash != partHash {
		t.Fatalf("candidate parts: %+v", info.CandidateParts)
	}
	if _, ok := f.BlockDispatchInfo(42); ok {
		t.Fatal("unknown block must miss")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run 'TestWorkSet|TestBlockDispatchInfo' 2>&1 | head -5`
Expected: compile failure.

- [ ] **Step 3: Implement in `fsm/reads.go`**

Append (complete implementation; iterate blocks by index — deterministic; maps only feed sorted collections or lookups):

```go
type BlockEvidence struct {
	BlockSeq       uint64
	VerifierSet    []string
	HasAttestation map[string]bool
	HasScan        map[string]bool
}

type BlockAnchor struct {
	BlockSeq      uint64
	ChainHash     string
	StateRoot     string
	Anchored      bool
	Finality      bool
	LastMergeable bool
}

type PromotionWork struct {
	Partition          arbiter.TablePartition
	Parts              []arbiter.PartRef
	BaseSafeSnapshotID string
	BasePartitionRoot  string
	StatementSeqs      []uint64
}

type PendingPromotionInfo struct{ Promote arbiter.PromoteSafePartition }

type CleanupWork struct {
	PromotionSeq uint64
	Cleanup      arbiter.UnsafeCleanup
}

// WorkSet is the §10.2 re-entry inventory: everything the orchestrator
// owes the pipeline, derived read-only from committed state. AwaitingRC is
// observability-only (the RC comes from the source SNode).
type WorkSet struct {
	OpenBlock          OpenBlockStats
	SealedUnmarked     []uint64
	AwaitingRC         []uint64
	EvidenceIncomplete []BlockEvidence
	QuorumFailed       []uint64
	UnanchoredVerified []BlockAnchor
	PromotablePending  []PromotionWork
	IssuedUnacked      []PendingPromotionInfo
	NeedManifest       bool
	PublishedUncleaned []CleanupWork
}

func (f *FSM) WorkSet() (WorkSet, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	ws := WorkSet{OpenBlock: OpenBlockStats{Count: len(f.st.OpenBlock.StatementSeqs), StatementSeqStart: f.st.OpenBlock.StatementSeqStart}}

	for i := range f.st.Blocks {
		blockSeq := f.st.Blocks[i].L3BlockSeq
		bv := f.st.Verifications[blockSeq]
		if bv == nil {
			continue
		}
		stmts := f.blockStatements(blockSeq)
		challengedOrDone := false
		allSafe := len(stmts) > 0
		rcComplete := true
		for _, ss := range stmts {
			switch ss.Status {
			case StatusChallengeReplay, StatusRejected:
				challengedOrDone = true
			}
			if ss.Status != StatusSafe {
				allSafe = false
			}
			if ss.RC == nil {
				rcComplete = false
			}
		}
		_ = allSafe // consumed by the NeedManifest pass below via safePrefix

		if len(bv.VerifierSet) == 0 {
			if rcComplete {
				ws.SealedUnmarked = append(ws.SealedUnmarked, blockSeq)
			} else {
				ws.AwaitingRC = append(ws.AwaitingRC, blockSeq)
			}
			continue
		}
		quorum := bv.Verdict != nil && bv.Verdict.Quorum
		if !quorum && !challengedOrDone {
			missing := false
			hasAtt := make(map[string]bool, len(bv.VerifierSet))
			hasScan := make(map[string]bool, len(bv.VerifierSet))
			for _, rid := range bv.VerifierSet {
				if bv.Attestations[rid] != nil {
					hasAtt[rid] = true
				} else {
					missing = true
				}
				if bv.ByteScans[rid] != nil {
					hasScan[rid] = true
				} else {
					missing = true
				}
			}
			if missing {
				ws.EvidenceIncomplete = append(ws.EvidenceIncomplete, BlockEvidence{
					BlockSeq: blockSeq, VerifierSet: append([]string(nil), bv.VerifierSet...),
					HasAttestation: hasAtt, HasScan: hasScan})
			} else {
				ws.QuorumFailed = append(ws.QuorumFailed, blockSeq)
			}
			continue
		}
		if quorum && !(bv.Finality && bv.LastMergeable) {
			chain, err := f.st.Blocks[i].ChainHash()
			if err != nil {
				return WorkSet{}, err
			}
			stateRoot := ""
			if last := stmts[len(stmts)-1]; last.RC != nil {
				stateRoot = last.RC.SourceClaimRoot
			}
			ws.UnanchoredVerified = append(ws.UnanchoredVerified, BlockAnchor{
				BlockSeq: blockSeq, ChainHash: chain, StateRoot: stateRoot,
				Anchored: bv.Anchor != nil, Finality: bv.Finality, LastMergeable: bv.LastMergeable})
		}
	}

	// PromotablePending: group undrained parts of Promotable statements by
	// partition, excluding partitions with a live (un-acked) promotion.
	livePartition := map[arbiter.TablePartition]bool{}
	for _, pp := range f.st.PendingPromotions {
		if !pp.Acked {
			livePartition[arbiter.TablePartition{TableID: pp.Promote.TableID, PartitionID: pp.Promote.PartitionID}] = true
		}
	}
	group := map[arbiter.TablePartition]*PromotionWork{}
	seqs := make([]uint64, 0, len(f.st.Statements))
	for seq := range f.st.Statements {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	for _, seq := range seqs {
		ss := f.st.Statements[seq]
		if ss.Status != StatusPromotable || ss.RC == nil {
			continue
		}
		for _, cp := range ss.RC.CandidateParts {
			if !ss.UnpromotedParts[cp.PartRowLtHash] {
				continue
			}
			tp := arbiter.TablePartition{TableID: cp.TableID, PartitionID: cp.PartitionID}
			if livePartition[tp] {
				continue
			}
			w := group[tp]
			if w == nil {
				base := f.st.Partitions[tp]
				w = &PromotionWork{Partition: tp}
				if base != nil {
					w.BaseSafeSnapshotID = base.BaseSafeSnapshotID
					w.BasePartitionRoot = base.BasePartitionRoot
				}
				group[tp] = w
			}
			w.Parts = append(w.Parts, arbiter.PartRef{TableID: cp.TableID, PartitionID: cp.PartitionID, PartRowLtHash: cp.PartRowLtHash, PartName: cp.PartName})
			if len(w.StatementSeqs) == 0 || w.StatementSeqs[len(w.StatementSeqs)-1] != seq {
				w.StatementSeqs = append(w.StatementSeqs, seq)
			}
		}
	}
	tps := make([]arbiter.TablePartition, 0, len(group))
	for tp := range group {
		tps = append(tps, tp)
	}
	sort.Slice(tps, func(i, j int) bool {
		if tps[i].TableID != tps[j].TableID {
			return tps[i].TableID < tps[j].TableID
		}
		return tps[i].PartitionID < tps[j].PartitionID
	})
	for _, tp := range tps {
		w := group[tp]
		sort.Slice(w.Parts, func(i, j int) bool { return w.Parts[i].PartRowLtHash < w.Parts[j].PartRowLtHash })
		ws.PromotablePending = append(ws.PromotablePending, *w)
	}

	// IssuedUnacked, sorted by promotion seq.
	pseqs := make([]uint64, 0, len(f.st.PendingPromotions))
	for seq, pp := range f.st.PendingPromotions {
		if !pp.Acked {
			pseqs = append(pseqs, seq)
		}
	}
	sort.Slice(pseqs, func(i, j int) bool { return pseqs[i] < pseqs[j] })
	for _, seq := range pseqs {
		ws.IssuedUnacked = append(ws.IssuedUnacked, PendingPromotionInfo{Promote: f.st.PendingPromotions[seq].Promote})
	}

	ws.NeedManifest = f.safePrefixLocked() > f.st.SafeWatermark.SafeBlockSeq

	// PublishedUncleaned: acked+applied promotions with parts still in the
	// registry, unscheduled, and covered by the published watermark.
	cseqs := make([]uint64, 0, len(f.st.PendingPromotions))
	for seq := range f.st.PendingPromotions {
		cseqs = append(cseqs, seq)
	}
	sort.Slice(cseqs, func(i, j int) bool { return cseqs[i] < cseqs[j] })
	for _, seq := range cseqs {
		pp := f.st.PendingPromotions[seq]
		if !pp.Acked {
			continue
		}
		if _, scheduled := f.st.PendingCleanups[seq]; scheduled {
			continue
		}
		tp := arbiter.TablePartition{TableID: pp.Promote.TableID, PartitionID: pp.Promote.PartitionID}
		reg := f.st.PromotedUnsafe[tp]
		if reg == nil {
			continue // applied=false ack, or already cleaned
		}
		present := false
		maxBlock := uint64(0)
		for _, pr := range pp.Promote.CandidateParts {
			if reg[pr.PartRowLtHash] {
				present = true
			}
		}
		for _, sseq := range pp.StatementSeqs {
			if ss, ok := f.st.Statements[sseq]; ok && ss.BlockSeq > maxBlock {
				maxBlock = ss.BlockSeq
			}
		}
		if !present || maxBlock > f.st.SafeWatermark.SafeBlockSeq {
			continue
		}
		ws.PublishedUncleaned = append(ws.PublishedUncleaned, CleanupWork{
			PromotionSeq: seq,
			Cleanup: arbiter.UnsafeCleanup{TableID: pp.Promote.TableID, PartitionID: pp.Promote.PartitionID,
				PromotionSeq: seq, Parts: append([]arbiter.PartRef(nil), pp.Promote.CandidateParts...)},
		})
	}
	return ws, nil
}

// safePrefixLocked returns the highest H such that blocks 1..H exist and
// every statement in them is Safe (0 when none). Caller holds the lock.
func (f *FSM) safePrefixLocked() uint64 {
	var h uint64
	for i := range f.st.Blocks {
		blockSeq := f.st.Blocks[i].L3BlockSeq
		for _, ss := range f.blockStatements(blockSeq) {
			if ss.Status != StatusSafe {
				return h
			}
		}
		h = blockSeq
	}
	return h
}

// BlockDispatchInfo assembles the §5.6 ReplayJob material for one block.
type BlockDispatchInfo struct {
	Header          L3BlockHeader
	ChainHash       string
	SourceClaimRoot string
	Statements      []replay.Statement
	CandidateParts  []arbiter.PartRef
	VerifierSet     []string
	HasAttestation  map[string]bool
	HasScan         map[string]bool
}

func (f *FSM) BlockDispatchInfo(blockSeq uint64) (BlockDispatchInfo, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	idx := int(blockSeq) - 1
	if idx < 0 || idx >= len(f.st.Blocks) {
		return BlockDispatchInfo{}, false
	}
	bv := f.st.Verifications[blockSeq]
	if bv == nil {
		return BlockDispatchInfo{}, false
	}
	hdr := f.st.Blocks[idx]
	chain, err := hdr.ChainHash()
	if err != nil {
		return BlockDispatchInfo{}, false
	}
	info := BlockDispatchInfo{Header: hdr, ChainHash: chain,
		VerifierSet:    append([]string(nil), bv.VerifierSet...),
		HasAttestation: map[string]bool{}, HasScan: map[string]bool{}}
	for _, rid := range bv.VerifierSet {
		if bv.Attestations[rid] != nil {
			info.HasAttestation[rid] = true
		}
		if bv.ByteScans[rid] != nil {
			info.HasScan[rid] = true
		}
	}
	stmts := f.blockStatements(blockSeq)
	for _, ss := range stmts {
		info.Statements = append(info.Statements, replay.Statement{
			StatementID: ss.Env.StatementID.Flat(), StatementSeq: ss.Seq,
			SQL: ss.Env.SQL, SQLHash: ss.Env.SQLHash, SettingsHash: ss.Env.SettingsHash,
			PayloadRef: ss.Env.PayloadRef, PayloadHash: ss.Env.PayloadHash, PayloadLength: ss.Env.PayloadLength,
			TargetTableID: ss.Env.TargetTableID, UserJWS: ss.Env.UserJWS,
		})
		if ss.RC != nil {
			for _, cp := range ss.RC.CandidateParts {
				info.CandidateParts = append(info.CandidateParts, arbiter.PartRef{
					TableID: cp.TableID, PartitionID: cp.PartitionID, PartRowLtHash: cp.PartRowLtHash, PartName: cp.PartName})
			}
		}
	}
	if len(stmts) > 0 {
		if last := stmts[len(stmts)-1]; last.RC != nil {
			info.SourceClaimRoot = last.RC.SourceClaimRoot
		}
	}
	return info, true
}

// ManifestInputs assembles the next manifest's content (design §4
// AckedUnpublished row): parent = current watermark, SafeBlockSeq = the
// all-Safe block prefix, tables from live partition bases + SafeParts.
// ok=false when there is nothing new to publish.
func (f *FSM) ManifestInputs() (ManifestInputs, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	target := f.safePrefixLocked()
	if target == 0 || target <= f.st.SafeWatermark.SafeBlockSeq {
		return ManifestInputs{}, false
	}
	byTable := map[string]*replay.TableManifest{}
	tps := make([]arbiter.TablePartition, 0, len(f.st.Partitions))
	for tp := range f.st.Partitions {
		tps = append(tps, tp)
	}
	sort.Slice(tps, func(i, j int) bool {
		if tps[i].TableID != tps[j].TableID {
			return tps[i].TableID < tps[j].TableID
		}
		return tps[i].PartitionID < tps[j].PartitionID
	})
	for _, tp := range tps {
		ps := f.st.Partitions[tp]
		if ps.BasePartitionRoot == "" {
			continue
		}
		tm := byTable[tp.TableID]
		if tm == nil {
			tm = &replay.TableManifest{TableID: tp.TableID}
			byTable[tp.TableID] = tm
		}
		tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{TableID: tp.TableID, PartitionID: tp.PartitionID, Root: ps.BasePartitionRoot})
		tm.ActiveParts = append(tm.ActiveParts, append([]replay.PartManifestEntry(nil), f.st.SafeParts[tp]...)...)
	}
	mi := ManifestInputs{ParentSnapshotID: f.st.SafeWatermark.SnapshotID, SafeBlockSeq: target}
	tables := make([]string, 0, len(byTable))
	for id := range byTable {
		tables = append(tables, id)
	}
	sort.Strings(tables)
	for _, id := range tables {
		mi.Tables = append(mi.Tables, *byTable[id])
	}
	return mi, true
}

type ManifestInputs struct {
	ParentSnapshotID string
	SafeBlockSeq     uint64
	Tables           []replay.TableManifest
}
```

`reads.go` gains the `"sort"` import. NOTE: `f.st.SafeParts` does not exist until Task 4 — Tasks 3 and 4 are committed TOGETHER if the implementer prefers, or Task 3 references it and Task 4 lands first; the plan orders them 3→4 with Task 3's `ManifestInputs`/its test EXCLUDED from Task 3's commit and moved into Task 4's steps. To keep tasks independently green: **implement everything above EXCEPT `ManifestInputs` (and its assertions in the stage test — comment those 12 lines with a `// Task 4:` marker) in Task 3; Task 4 adds SafeParts + ManifestInputs and un-comments.** The stage test's manifest-publish stage still runs in Task 3 by building the manifest inline from `partHash` (as `TestReads_WatermarkAndManifests` does).

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v -run 'TestWorkSet|TestBlockDispatchInfo' 2>&1 | tail -8` then `go test ./... 2>&1 | tail -4`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): WorkSet inventory + BlockDispatchInfo (§10.2 re-entry reads)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 4: fsm write-path roll-forwards + SafeParts tracking + ManifestInputs

**Files:**
- Modify: `fsm/state.go` (SafeParts field), `fsm/apply.go` (four changes), `fsm/snapshot.go` (SafeParts in doc + length cap), `fsm/reads.go` (un-comment/land `ManifestInputs`), `fsm/promotion_test.go`, `fsm/snapshot_test.go`, `fsm/reads_test.go` (un-comment the Task-3 markers)

**Interfaces:**
- Consumes: Task 3 reads.
- Produces: `State.SafeParts map[arbiter.TablePartition][]replay.PartManifestEntry` (appended on applied acks; snapshot-carried); `applyPublishSafeSnapshot` rejects `SnapshotID != ManifestRoot`; `applyRegisterNode` rejects a pubkey registered under a different node_id; `readSnapshot` caps the JSON length prefix at `maxSnapshotDocBytes = 1 << 30`; `ManifestInputs` live (Task 3 shape). Design note: SafeParts is a spec-level completion — published manifests must carry `ActiveParts` or P1c verifiers cannot replay from a non-zero base.

- [ ] **Step 1: Write the failing tests**

Append to `fsm/promotion_test.go`:

```go
func TestPromotionAck_AccumulatesSafeParts(t *testing.T) {
	f, _, partHash, _ := issuePromotion(t)
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0",
		PostPartitionCommitment: partHash, Applied: true,
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: partHash, SafePartName: "all_9_9_0", PartPhysHash: "0x99"}}}
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack}})
	tp := arbiter.TablePartition{TableID: "db.t", PartitionID: "p0"}
	parts := f.st.SafeParts[tp]
	if len(parts) != 1 {
		t.Fatalf("SafeParts: %+v", parts)
	}
	p := parts[0]
	// identity from the ack mapping; row count/bytes cross-referenced from the RC's CandidatePart
	if p.PartRowLtHash != partHash || p.PartName != "all_9_9_0" || p.PartPhysHash != "0x99" ||
		p.TableID != "db.t" || p.PartitionID != "p0" || p.RowCount != 1 || p.Bytes != 32 {
		t.Fatalf("entry: %+v", p)
	}
	// idempotent re-ack must not duplicate
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack}})
	if len(f.st.SafeParts[tp]) != 1 {
		t.Fatal("re-ack must not duplicate SafeParts")
	}
}

func TestPublishSafeSnapshot_RejectsNonContentAddressedID(t *testing.T) {
	f, _, partHash, _ := issuePromotion(t)
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: arbiter.PromotionAck{
		NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", PostPartitionCommitment: partHash, Applied: true}}})
	m, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq: 1, SchemaSnapshotID: "schema-genesis", SchemaRoot: "0xschr", ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []replay.TableManifest{{TableID: "db.t", PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: partHash}}}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	forged := m
	forged.SnapshotID = "hand-picked-id" // still passes replay.Validate (Validate does not bind SnapshotID)
	applyExpectReject(t, f, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: forged}})
	// the honest content-addressed manifest still publishes
	mustApply(t, f, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: m}})
}

func TestRegisterNode_RejectsDuplicatePubkeyAcrossNodeIDs(t *testing.T) {
	f := newTestFSM(t)
	mustApply(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: testPubkey(1)}}})
	applyExpectReject(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "v2", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: testPubkey(1)}}})
	// re-registering the SAME node with its own key stays legal
	mustApply(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: testPubkey(1)}}})
}
```

Append to `fsm/snapshot_test.go`:

```go
func TestSnapshotRoundTrip_CarriesSafeParts(t *testing.T) {
	f := newTestFSM(t)
	tp := arbiter.TablePartition{TableID: "db.t", PartitionID: "p0"}
	f.st.SafeParts[tp] = []replay.PartManifestEntry{{TableID: "db.t", PartitionID: "p0", PartName: "all_1_1_0", PartRowLtHash: "0xffee", RowCount: 2, Bytes: 64}}
	g := restoreInto(t, snapshotBytes(t, f))
	if !reflect.DeepEqual(f.st.SafeParts, g.st.SafeParts) {
		t.Fatal("SafeParts must round-trip")
	}
}

func TestRestoreRejectsOversizedLengthPrefix(t *testing.T) {
	// container with a length prefix beyond the sanity cap
	buf := []byte{'A', 'F', 'S', 'M', 1, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
	g := New(Params{})
	if err := g.Restore(io.NopCloser(bytes.NewReader(buf))); err == nil {
		t.Fatal("oversized length prefix must error before allocating")
	}
}
```

(`snapshot_test.go` gains the `replay` import; `reads_test.go`: un-comment the `// Task 4:` ManifestInputs block from Task 3's stage test.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run 'TestPromotionAck_Accumulates|TestPublishSafeSnapshot_Rejects|TestRegisterNode_RejectsDuplicate|TestSnapshotRoundTrip_CarriesSafeParts|TestRestoreRejects' 2>&1 | head -6`
Expected: compile failure (`SafeParts` undefined) / test failures.

- [ ] **Step 3: Implement**

`fsm/state.go`: add to `State`: `SafeParts map[arbiter.TablePartition][]replay.PartManifestEntry`; initialize `SafeParts: map[arbiter.TablePartition][]replay.PartManifestEntry{}` in `newState`. Comment: `// SafeParts is the cumulative safe-part inventory per partition (appended on applied promotion acks; feeds manifest ActiveParts so P1c verifiers can replay from a published base).`

`fsm/apply.go` — four changes:

(a) In `applyRecordPromotionAck`, success path, after the registry writes: append SafeParts entries by joining the ack's mappings with the covered statements' RC candidate parts (for RowCount/Bytes), dedup by PartRowLtHash:

```go
	// Accumulate the safe-part inventory (manifest ActiveParts source).
	// RowCount/Bytes come from the RC candidate part with the same content
	// commitment; the ack mapping contributes the safe name / phys hash.
	rcByHash := map[string]arbiter.CandidatePart{}
	for _, seq := range pp.StatementSeqs {
		if ss, ok := f.st.Statements[seq]; ok && ss.RC != nil {
			for _, cp := range ss.RC.CandidateParts {
				rcByHash[cp.PartRowLtHash] = cp
			}
		}
	}
	existing := map[string]bool{}
	for _, e := range f.st.SafeParts[tp] {
		existing[e.PartRowLtHash] = true
	}
	for _, m := range ack.Parts {
		if existing[m.PartRowLtHash] {
			continue
		}
		cp := rcByHash[m.PartRowLtHash]
		f.st.SafeParts[tp] = append(f.st.SafeParts[tp], replay.PartManifestEntry{
			TableID: ack.TableID, PartitionID: ack.PartitionID,
			PartName: m.SafePartName, PartPhysHash: m.PartPhysHash,
			PartRowLtHash: m.PartRowLtHash, RowCount: cp.RowCount, Bytes: cp.Bytes,
		})
		existing[m.PartRowLtHash] = true
	}
```

(Deterministic: `ack.Parts` is a logged slice, iterated in order; the rcByHash map is a lookup only. Idempotent re-acks are already absorbed by `pp.Acked` before this point — the dedup guard is defense in depth for replayed logs.)

(b) In `applyPublishSafeSnapshot`, after `m.Validate()`:

```go
	if m.SnapshotID != m.ManifestRoot {
		// Content-addressing is load-bearing: check 2 pins bases by
		// Manifests[PrevSafeSnapshotID], so ids must be collision-free by
		// construction (P1a whole-branch review follow-up).
		return Rejected{Reason: "snapshot_id must equal manifest_root (content-addressed)"}
	}
```

(c) In `applyRegisterNode`, before the map write:

```go
	for id, n := range f.st.Nodes {
		if id != r.NodeID && bytes.Equal(n.Registration.Ed25519Pubkey, r.Ed25519Pubkey) && len(r.Ed25519Pubkey) > 0 {
			return Rejected{Reason: "ed25519 pubkey already registered to another node"}
		}
	}
```

(map iteration feeds a pure any-match check — order-independent; `apply.go` gains the `"bytes"` import.)

(d) On `applyRecordAnchorFinality`'s anchor overwrite line, add the comment: `// Last-wins on anchor CONTENT is deliberate (flags OR-latch); the log is the audit trail (P1a review note).`

`fsm/snapshot.go`: add `SafeParts map[arbiter.TablePartition][]replay.PartManifestEntry \`json:"safe_parts,omitempty"\`` to `snapshotDoc`, wire into `writeSnapshot`/`readSnapshot` (nil-guarded like the sibling maps), and add the cap:

```go
const maxSnapshotDocBytes = 1 << 30 // sanity bound on the length prefix (P1a review follow-up)
```

with, in `readSnapshot` after decoding `lenBuf`:

```go
	n := binary.BigEndian.Uint64(lenBuf[:])
	if n > maxSnapshotDocBytes {
		return nil, fmt.Errorf("snapshot doc length %d exceeds sanity bound", n)
	}
	jb := make([]byte, n)
```

`fsm/reads.go`: land `ManifestInputs` (the Task 3 block) and un-comment the stage-test assertions.

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -8` then `go test ./... 2>&1 | tail -4`
Expected: ALL PASS (including the now-complete `TestWorkSet_TracksPipelineStages`).

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): SafeParts inventory, content-addressed manifest ids, dup-pubkey reject, snapshot cap

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 5: fsm hardening tests (P1a roll-forward, test-only)

**Files:**
- Create: `fsm/userjws_test.go`
- Modify: `fsm/admission_test.go`, `fsm/threeway_test.go`

**Interfaces:**
- Consumes: existing helpers (`signUserJWS`, `validEnvelope`, `evidenceBlock`, `attest`, `scanIn`, `goodReceipt`, `goodScan`).
- Produces: test coverage only; no production change permitted in this task (if a test exposes a bug, STOP and report — that is a finding, not a fix-in-place).

- [ ] **Step 1: Write the tests**

Create `fsm/userjws_test.go`:

```go
package fsm

import (
	"crypto/ecdsa"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"
)

// signUserJWSWithV builds a user JWS whose recovery byte is offset by +27
// (the Ethereum legacy V convention) — verifyUserJWS must normalize it.
func signUserJWSWithV(t *testing.T, key *ecdsa.PrivateKey, sql string) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256K","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"iat":1700000000,"qhash":"0x%x"}`, crypto.Keccak256([]byte(sql)))))
	signingInput := header + "." + payload
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	sig[64] += 27
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestVerifyUserJWS_NormalizesLegacyV(t *testing.T) {
	key, account := testAccount(t)
	sql := "INSERT INTO db.t VALUES (1)"
	if err := verifyUserJWS(signUserJWSWithV(t, key, sql), sql, account); err != nil {
		t.Fatalf("v=27/28 signature must verify after normalization: %v", err)
	}
}

func TestVerifyUserJWS_MalformedEncodings(t *testing.T) {
	key, account := testAccount(t)
	sql := "INSERT INTO db.t VALUES (1)"
	good := signUserJWS(t, key, sql)
	parts := strings.Split(good, ".")
	cases := map[string]string{
		"two parts":         parts[0] + "." + parts[1],
		"bad header b64":    "!!!." + parts[1] + "." + parts[2],
		"bad payload b64":   parts[0] + ".!!!." + parts[2],
		"bad signature b64": parts[0] + "." + parts[1] + ".!!!",
		"short signature":   parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString([]byte{1, 2, 3}),
		"wrong alg":         base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`)) + "." + parts[1] + "." + parts[2],
		"header not json":   base64.RawURLEncoding.EncodeToString([]byte(`{`)) + "." + parts[1] + "." + parts[2],
		"payload not json":  parts[0] + "." + base64.RawURLEncoding.EncodeToString([]byte(`{`)) + "." + parts[2],
	}
	for name, tok := range cases {
		if err := verifyUserJWS(tok, sql, account); err == nil {
			t.Errorf("%s: must reject", name)
		}
	}
}
```

Append to `fsm/admission_test.go`:

```go
func TestAdmission_GapSplitTriggersBudget(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	// jump to open ranges up to exactly K-1=63, then a mid-range split
	// (GapFillable coordinate) pushes to 64 = K: allowed; one more split rejects.
	for s := uint64(2); s <= 126; s += 2 { // 63 singleton ranges [1,1],[3,3],...
		if r := submit(t, f, validEnvelope(t, key, account, s)); r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("seq %d: %+v", s, r)
		}
	}
	// one wide jump creates range [127,199] → 64 ranges total = K
	if r := submit(t, f, validEnvelope(t, key, account, 200)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("wide jump: %+v", r)
	}
	// mid-range fill of [127,199] would SPLIT it → 65 ranges → budget exceeded,
	// exercising the GapFillable-but-Insert-rejects path (Status said fillable)
	if r := submit(t, f, validEnvelope(t, key, account, 150)); r.Code != arbiter.AdmissionCodeGapBudgetExceeded {
		t.Fatalf("mid-range split at budget: %+v", r)
	}
	// edge fill never increases the count: still accepted at budget
	if r := submit(t, f, validEnvelope(t, key, account, 127)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("edge fill: %+v", r)
	}
}
```

Append to `fsm/threeway_test.go`:

```go
func TestThreeWay_TwoPassOneFailReachesQuorum(t *testing.T) {
	f := newTestFSM(t)
	set, partHash := evidenceBlock(t, f)
	bad := goodReceipt(f, partHash)
	bad.ComputedStateRoot = "0xdifferent"
	attest(t, f, set[0], bad) // one actively failing replica
	scanIn(t, f, set[0], goodScan(partHash))
	for _, rid := range set[1:] { // two passing replicas
		attest(t, f, rid, goodReceipt(f, partHash))
		scanIn(t, f, rid, goodScan(partHash))
	}
	v := f.st.Verifications[1].Verdict
	if v == nil || !v.Quorum {
		t.Fatalf("2 passing + 1 failing must reach quorum: %+v", v)
	}
	if v.Replicas[set[0]].Pass {
		t.Fatal("the failing replica must not be marked passing")
	}
}
```

- [ ] **Step 2: Run the tests**

Run: `go test ./fsm/ -run 'TestVerifyUserJWS|TestAdmission_GapSplit|TestThreeWay_TwoPassOneFail' -v 2>&1 | tail -8`
Expected: ALL PASS against the existing production code (these pin fail-closed branches). If any FAILS, stop and report the failure verbatim — a production bug, not a test to adjust.

- [ ] **Step 3: Full suite + commit**

Run: `go test ./... 2>&1 | tail -4`

```bash
git add fsm/
git commit -m "test(fsm): JWS v-normalization/malformed table, gap-split budget, quorum boundary

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 6: wire converter exports + dispatch builders

**Files:**
- Modify: `wire/convert.go` (export renames), `wire/command.go` (call-site renames), `wire/wire_test.go`
- Create: `wire/dispatch.go`

**Interfaces:**
- Consumes: P1a converters; `replay.ReplayJob`; pb dispatch messages.
- Produces (server Tasks 12-14 + orchestrator Tasks 10-11 rely on): exported `EnvelopeFromPB/EnvelopeToPB`, `RCFromPB/RCToPB`, `AttestationFromPB/AttestationToPB`, `ScanFromPB/ScanToPB`, `PromotionAckFromPB/PromotionAckToPB`, `CleanupAckFromPB/CleanupAckToPB`, `ManifestFromPB/ManifestToPB`, `AnchorRefFromPB/AnchorRefToPB`, `RegistrationFromPB/RegistrationToPB`, `PromoteToPB`, `CleanupToPB` (all mechanical renames of the P1a unexported functions; every internal call site updated). NEW in `wire/dispatch.go`: `func ReplayJobToPB(j replay.ReplayJob) *pb.ReplayJob`, `func ReplayJobDispatch(j replay.ReplayJob) *pb.VerifierDispatch`, `func ByteSideScanDispatch(blockSeq uint64, parts []arbiter.PartRef) *pb.VerifierDispatch`, `func PromotionCommandPB(p arbiter.PromoteSafePartition, jws string) *pb.PromotionCommand`, `func CleanupCommandPB(c arbiter.UnsafeCleanup, jws string) *pb.PromotionCommand`.

- [ ] **Step 1: Write the failing tests**

Append to `wire/wire_test.go`:

```go
func TestDispatchBuilders(t *testing.T) {
	job := replay.ReplayJob{BlockSeq: 4, PrevSafeSnapshotID: "s0", PrevStateRoot: "0xps",
		SchemaSnapshotID: "sch", ExecutorProfileID: "prof", SourceClaimRoot: "0xr",
		Statements: []replay.Statement{{StatementID: "0xa:1:n", StatementSeq: 7, SQL: "INSERT", SQLHash: "0xh", TargetTableID: "db.t"}}}
	d := ReplayJobDispatch(job)
	pj := d.GetReplayJob()
	if pj == nil || pj.GetBlockSeq() != 4 || pj.GetSourceClaimRoot() != "0xr" ||
		len(pj.GetStatements()) != 1 || pj.GetStatements()[0].GetStatementSeq() != 7 {
		t.Fatalf("replay job dispatch: %+v", d)
	}
	sd := ByteSideScanDispatch(4, []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: "0xffee"}})
	sr := sd.GetByteSideScan()
	if sr == nil || sr.GetBlockSeq() != 4 || len(sr.GetParts()) != 1 || sr.GetParts()[0].GetPartRowLthash() != "0xffee" {
		t.Fatalf("scan dispatch: %+v", sd)
	}
	pc := PromotionCommandPB(arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1}, "x.y.z")
	if pc.GetPromote() == nil || pc.GetPromote().GetPromotionSeq() != 1 || pc.GetAuthorityJws() != "x.y.z" {
		t.Fatalf("promotion command: %+v", pc)
	}
	cc := CleanupCommandPB(arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1}, "x.y.z")
	if cc.GetCleanup() == nil || cc.GetCleanup().GetPromotionSeq() != 1 {
		t.Fatalf("cleanup command: %+v", cc)
	}
}

func TestExportedConverterRoundTrip(t *testing.T) {
	env := arbiter.StatementEnvelope{StatementID: arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"},
		StatementKind: arbiter.StatementKindInsert, SQL: "INSERT", SQLHash: "0xh", UserJWS: "a.b.c"}
	if got := EnvelopeFromPB(EnvelopeToPB(env)); got != env {
		t.Fatalf("envelope round trip: %+v", got)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./wire/ 2>&1 | head -5`
Expected: compile failure (exported names undefined).

- [ ] **Step 3: Implement**

In `wire/convert.go`, rename to exported: `envelopeFromPB→EnvelopeFromPB`, `envelopeToPB→EnvelopeToPB`, `rcFromPB→RCFromPB`, `rcToPB→RCToPB`, `attestationFromPB→AttestationFromPB`, `attestationToPB→AttestationToPB`, `scanFromPB→ScanFromPB`, `scanToPB→ScanToPB`, `promotionAckFromPB→PromotionAckFromPB`, `promotionAckToPB→PromotionAckToPB`, `cleanupAckFromPB→CleanupAckFromPB`, `cleanupAckToPB→CleanupAckToPB`, `manifestFromPB→ManifestFromPB`, `manifestToPB→ManifestToPB`, `anchorFromPB→AnchorRefFromPB`, `anchorToPB→AnchorRefToPB`, `registrationFromPB→RegistrationFromPB`, `registrationToPB→RegistrationToPB`, `promoteToPB→PromoteToPB`, `cleanupToPB→CleanupToPB` (leave the statement-id/part-level helpers unexported; update every call site in `command.go`/`convert.go` — `gofmt` + compiler enforce completeness).

Create `wire/dispatch.go`:

```go
package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
)

// Dispatch builders: leader→data-plane stream payloads (design §6). The
// replay.Statement wire form was field-name-frozen against pkg/replay in
// P0; these builders are the send-side complement of the P1a converters.

func statementToPB(s replay.Statement) *pb.Statement {
	return &pb.Statement{
		StatementId: s.StatementID, StatementSeq: s.StatementSeq,
		Sql: s.SQL, SqlHash: s.SQLHash, SettingsHash: s.SettingsHash,
		PayloadRef: s.PayloadRef, PayloadHash: s.PayloadHash, PayloadLength: s.PayloadLength,
		TargetTableId: s.TargetTableID, UserJws: s.UserJWS,
	}
}

// ReplayJobToPB converts the §5.6 job to its wire form.
func ReplayJobToPB(j replay.ReplayJob) *pb.ReplayJob {
	return &pb.ReplayJob{
		BlockSeq: j.BlockSeq, PrevSafeSnapshotId: j.PrevSafeSnapshotID, PrevStateRoot: j.PrevStateRoot,
		SchemaSnapshotId: j.SchemaSnapshotID, ExecutorProfileId: j.ExecutorProfileID,
		SourceClaimRoot: j.SourceClaimRoot, Statements: mapSlice(j.Statements, statementToPB),
	}
}

// ReplayJobDispatch wraps a job in the VerifierDispatch oneof.
func ReplayJobDispatch(j replay.ReplayJob) *pb.VerifierDispatch {
	return &pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: ReplayJobToPB(j)}}
}

// ByteSideScanDispatch wraps the check-3 scan request (§7.1 round two).
func ByteSideScanDispatch(blockSeq uint64, parts []arbiter.PartRef) *pb.VerifierDispatch {
	return &pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ByteSideScan{ByteSideScan: &pb.ByteSideScanRequest{
		BlockSeq: blockSeq, Parts: mapSlice(parts, partRefToPB),
	}}}
}

// PromotionCommandPB wraps a signed promotion for the SNode stream.
func PromotionCommandPB(p arbiter.PromoteSafePartition, jws string) *pb.PromotionCommand {
	return &pb.PromotionCommand{Cmd: &pb.PromotionCommand_Promote{Promote: PromoteToPB(p)}, AuthorityJws: jws}
}

// CleanupCommandPB wraps a signed cleanup for the SNode stream.
func CleanupCommandPB(c arbiter.UnsafeCleanup, jws string) *pb.PromotionCommand {
	return &pb.PromotionCommand{Cmd: &pb.PromotionCommand_Cleanup{Cleanup: CleanupToPB(c)}, AuthorityJws: jws}
}
```

Generated-name caveat as in P1a: oneof wrapper spellings (`pb.VerifierDispatch_ReplayJob`, `pb.PromotionCommand_Promote`) follow protoc-gen-go; check `gen/pb/arbiter.pb.go` if the compiler disagrees.

- [ ] **Step 4: Run the tests**

Run: `go test ./wire/ ./conformance/ -v 2>&1 | tail -8` then `go test ./... 2>&1 | tail -4`
Expected: ALL PASS (conformance untouched by renames).

- [ ] **Step 5: Commit**

```bash
git add wire/
git commit -m "feat(wire): export RPC-boundary converters + dispatch builders

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 7: anchor seam + local backend (`anchor/`)

**Files:**
- Create: `anchor/client.go`, `anchor/local.go`, `anchor/local_test.go`

**Interfaces:**
- Consumes: `arbiter.AnchorRef` (root types).
- Produces (orchestrator Task 11, cmd Task 15 rely on): `type Client interface { Anchor(ctx context.Context, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error); WaitFinality(ctx context.Context, ref arbiter.AnchorRef) (finality, lastMergeable bool, err error) }`; `func NewLocal() *Local` implementing `Client`.

- [ ] **Step 1: Write the failing test**

Create `anchor/local_test.go`:

```go
package anchor

import (
	"context"
	"testing"
)

func TestLocal_DeterministicRefAndImmediateFinality(t *testing.T) {
	c := NewLocal()
	ref1, err := c.Anchor(context.Background(), "0xhash1", "0xroot1")
	if err != nil {
		t.Fatalf("Anchor: %v", err)
	}
	if ref1.L3BlockHash != "0xhash1" || ref1.StateRoot != "0xroot1" || ref1.L2TxRef != "local:0xhash1" || ref1.L2BlockNumber != 1 {
		t.Fatalf("ref1: %+v", ref1)
	}
	ref2, _ := c.Anchor(context.Background(), "0xhash2", "0xroot2")
	if ref2.L2BlockNumber != 2 {
		t.Fatalf("monotonic block number: %+v", ref2)
	}
	fin, lm, err := c.WaitFinality(context.Background(), ref1)
	if err != nil || !fin || !lm {
		t.Fatalf("WaitFinality: %v %v %v", fin, lm, err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./anchor/ 2>&1 | head -3` — Expected: package missing.

- [ ] **Step 3: Implement**

`anchor/client.go`:

```go
// Package anchor is the L2-anchoring seam (P1b design §5). Anchoring is
// leader-side I/O: determinism constraints apply to the RecordAnchorFinality
// command the orchestrator proposes, never to the client itself. The
// config anchor.backend allowlist selects the implementation; P1b ships
// only the local backend — a real L2 client is additive behind this seam.
package anchor

import (
	"context"

	"github.com/sentioxyz/arbiter"
)

// Client posts one L3 block commitment and reports its finality.
type Client interface {
	Anchor(ctx context.Context, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error)
	// WaitFinality reports (finality, last_mergeable) for a posted anchor.
	WaitFinality(ctx context.Context, ref arbiter.AnchorRef) (finality, lastMergeable bool, err error)
}
```

`anchor/local.go`:

```go
package anchor

import (
	"context"
	"sync/atomic"

	"github.com/sentioxyz/arbiter"
)

// Local is the dev/test backend: deterministic refs, immediate finality
// and last_mergeable. The block-number counter is process-local (resets on
// restart) — harmless, the ref is only an audit reference in v1.
type Local struct {
	n atomic.Uint64
}

var _ Client = (*Local)(nil)

func NewLocal() *Local { return &Local{} }

func (l *Local) Anchor(_ context.Context, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error) {
	return arbiter.AnchorRef{
		L3BlockHash: l3BlockHash, StateRoot: stateRoot,
		L2TxRef: "local:" + l3BlockHash, L2BlockNumber: l.n.Add(1),
	}, nil
}

func (l *Local) WaitFinality(context.Context, arbiter.AnchorRef) (bool, bool, error) {
	return true, true, nil
}
```

- [ ] **Step 4: Run + commit**

Run: `go test ./anchor/ -v && go test ./... 2>&1 | tail -3` — Expected: PASS.

```bash
git add anchor/
git commit -m "feat(anchor): Client seam with local immediate-finality backend

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 8: `config` package (Open Q8)

**Files:**
- Create: `config/duration.go`, `config/config.go`, `config/config_test.go`

**Interfaces:**
- Consumes: gopkg.in/yaml.v3 (new direct dep), go-ethereum crypto (key validation).
- Produces (cmd Task 15 relies on):

```go
type Duration struct{ time.Duration }            // yaml "5s" strings; warn < 1s via Validate notes
type RaftPeer struct{ ID, Addr string }
type RaftConfig struct {
	Listen, Advertise, DataDir string
	Bootstrap bool
	Peers []RaftPeer
	ElectionTimeout, HeartbeatTimeout, LeaderLeaseTimeout, CommitTimeout, SnapshotInterval Duration
	SnapshotThreshold, TrailingLogs uint64
}
type SealConfig struct{ MaxStatements int; MaxAge Duration }
type IngressConfig struct{ MaxStatementAge Duration }
type DispatchConfig struct{ RetryInterval Duration }
type AnchorConfig struct{ Backend string }        // allowlist: "local"
type AuthorityConfig struct{ PrivateKeyHex string; AllowedAddresses []string }
type GenesisConfig struct{ SchemaSnapshotID, ExecutorProfileID string }
type Config struct {
	NodeID string; Raft RaftConfig; GRPCListen string; ApplyTimeout Duration
	Seal SealConfig; Ingress IngressConfig; Dispatch DispatchConfig
	Anchor AnchorConfig; Authority AuthorityConfig; Genesis GenesisConfig
}
func Load(path string) (Config, error)            // yaml + env override ARBITER_AUTHORITY_PRIVATE_KEY_HEX + defaults + Validate
func (c *Config) Validate() error                 // errors.Join aggregate
```

Defaults exactly per design §7 (election/heartbeat 1s, lease 500ms, commit 50ms, snapshot 120s/8192, trailing 10240, seal 256/2s, ingress 5m, retry 5s, anchor local, apply 5s). `Validate` requirements: node_id, raft.listen, raft.data_dir, grpc_listen non-empty; positive durations; `bootstrap ⇒ len(peers) > 0`; `anchor.backend ∈ {"local"}`; authority key parses via `crypto.HexToECDSA` when set; addresses lowercased in place; genesis fields non-empty. Doc comments on `Genesis`/`AllowedAddresses`: "consensus parameters — identical on every node or the cluster forks."

- [ ] **Step 1: Write the failing tests**

Create `config/config_test.go`:

```go
package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "arbiter.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return p
}

const minimal = `
node_id: arb-1
raft:
  listen: "127.0.0.1:7000"
  data_dir: /tmp/arb
grpc_listen: "127.0.0.1:7001"
genesis:
  schema_snapshot_id: schema-genesis
  executor_profile_id: housegate-replay-mvp-v0
`

func TestLoad_DefaultsApplied(t *testing.T) {
	c, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Raft.ElectionTimeout.Duration != time.Second || c.Raft.HeartbeatTimeout.Duration != time.Second ||
		c.Raft.LeaderLeaseTimeout.Duration != 500*time.Millisecond || c.Raft.CommitTimeout.Duration != 50*time.Millisecond ||
		c.Raft.SnapshotInterval.Duration != 120*time.Second || c.Raft.SnapshotThreshold != 8192 || c.Raft.TrailingLogs != 10240 {
		t.Fatalf("raft defaults: %+v", c.Raft)
	}
	if c.Seal.MaxStatements != 256 || c.Seal.MaxAge.Duration != 2*time.Second ||
		c.Ingress.MaxStatementAge.Duration != 5*time.Minute || c.Dispatch.RetryInterval.Duration != 5*time.Second ||
		c.Anchor.Backend != "local" || c.ApplyTimeout.Duration != 5*time.Second {
		t.Fatalf("defaults: %+v", c)
	}
}

func TestLoad_ValidateFailures(t *testing.T) {
	cases := map[string]string{
		"missing node_id":   strings.Replace(minimal, "node_id: arb-1", "", 1),
		"missing data_dir":  strings.Replace(minimal, "  data_dir: /tmp/arb", "", 1),
		"bad anchor":        minimal + "anchor: { backend: mars }\n",
		"bootstrap no peers": minimal + "raft_extra: x\n", // replaced below
	}
	delete(cases, "bootstrap no peers")
	for name, body := range cases {
		if _, err := Load(writeConfig(t, body)); err == nil {
			t.Errorf("%s: must fail validation", name)
		}
	}
	boot := strings.Replace(minimal, "  data_dir: /tmp/arb", "  data_dir: /tmp/arb\n  bootstrap: true", 1)
	if _, err := Load(writeConfig(t, boot)); err == nil {
		t.Error("bootstrap without peers must fail")
	}
	badKey := minimal + "authority: { private_key_hex: nothex }\n"
	if _, err := Load(writeConfig(t, badKey)); err == nil {
		t.Error("unparseable authority key must fail")
	}
}

func TestLoad_EnvOverrideAndAddressNormalization(t *testing.T) {
	key := "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032" // well-known test vector key
	body := minimal + "authority:\n  private_key_hex: \"deadbeef\"\n  allowed_addresses: [\"0xABCDEF0123456789abcdef0123456789ABCDEF01\"]\n"
	t.Setenv("ARBITER_AUTHORITY_PRIVATE_KEY_HEX", key)
	c, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Authority.PrivateKeyHex != key {
		t.Fatal("env must override the yaml key")
	}
	if c.Authority.AllowedAddresses[0] != "0xabcdef0123456789abcdef0123456789abcdef01" {
		t.Fatalf("addresses must be lowercased: %v", c.Authority.AllowedAddresses)
	}
}

func TestDuration_ParsesStrings(t *testing.T) {
	c, err := Load(writeConfig(t, minimal+"apply_timeout: \"7s\"\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.ApplyTimeout.Duration != 7*time.Second {
		t.Fatalf("apply_timeout: %v", c.ApplyTimeout)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./config/ 2>&1 | head -3` — Expected: package missing.

- [ ] **Step 3: Implement**

`config/duration.go` (housegate-convention local type):

```go
package config

import (
	"fmt"
	"time"
)

// Duration accepts "5s"-style yaml strings (housegate convention).
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalYAML(unmarshal func(any) error) error {
	var s string
	if err := unmarshal(&s); err == nil {
		v, perr := time.ParseDuration(s)
		if perr != nil {
			return fmt.Errorf("duration %q: %w", s, perr)
		}
		d.Duration = v
		return nil
	}
	var n int64
	if err := unmarshal(&n); err != nil {
		return fmt.Errorf("duration: want string or integer nanoseconds")
	}
	d.Duration = time.Duration(n)
	return nil
}
```

`config/config.go` (structs per the Produces block, yaml tags matching the design §7 keys — `node_id`, `raft.listen`, `election_timeout`, …; full `Load` + `Validate`):

```go
package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
	"gopkg.in/yaml.v3"
)

// EnvAuthorityKey overrides authority.private_key_hex when set.
const EnvAuthorityKey = "ARBITER_AUTHORITY_PRIVATE_KEY_HEX"

type RaftPeer struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

type RaftConfig struct {
	Listen             string     `yaml:"listen"`
	Advertise          string     `yaml:"advertise"`
	DataDir            string     `yaml:"data_dir"`
	Bootstrap          bool       `yaml:"bootstrap"`
	Peers              []RaftPeer `yaml:"peers"`
	ElectionTimeout    Duration   `yaml:"election_timeout"`
	HeartbeatTimeout   Duration   `yaml:"heartbeat_timeout"`
	LeaderLeaseTimeout Duration   `yaml:"leader_lease_timeout"`
	CommitTimeout      Duration   `yaml:"commit_timeout"`
	SnapshotInterval   Duration   `yaml:"snapshot_interval"`
	SnapshotThreshold  uint64     `yaml:"snapshot_threshold"`
	TrailingLogs       uint64     `yaml:"trailing_logs"`
}

type SealConfig struct {
	MaxStatements int      `yaml:"max_statements"`
	MaxAge        Duration `yaml:"max_age"`
}

type IngressConfig struct {
	MaxStatementAge Duration `yaml:"max_statement_age"`
}

type DispatchConfig struct {
	RetryInterval Duration `yaml:"retry_interval"`
}

type AnchorConfig struct {
	Backend string `yaml:"backend"`
}

// AuthorityConfig: AllowedAddresses are CONSENSUS PARAMETERS — they feed
// fsm.Params and must be identical on every node or the cluster forks.
type AuthorityConfig struct {
	PrivateKeyHex    string   `yaml:"private_key_hex"`
	AllowedAddresses []string `yaml:"allowed_addresses"`
}

// GenesisConfig: CONSENSUS PARAMETERS — identical on every node or the
// cluster forks (design §7).
type GenesisConfig struct {
	SchemaSnapshotID  string `yaml:"schema_snapshot_id"`
	ExecutorProfileID string `yaml:"executor_profile_id"`
}

type Config struct {
	NodeID       string         `yaml:"node_id"`
	Raft         RaftConfig     `yaml:"raft"`
	GRPCListen   string         `yaml:"grpc_listen"`
	ApplyTimeout Duration       `yaml:"apply_timeout"`
	Seal         SealConfig     `yaml:"seal"`
	Ingress      IngressConfig  `yaml:"ingress"`
	Dispatch     DispatchConfig `yaml:"dispatch"`
	Anchor       AnchorConfig   `yaml:"anchor"`
	Authority    AuthorityConfig `yaml:"authority"`
	Genesis      GenesisConfig  `yaml:"genesis"`
}

func Load(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if env := os.Getenv(EnvAuthorityKey); env != "" {
		c.Authority.PrivateKeyHex = env
	}
	for i, a := range c.Authority.AllowedAddresses {
		c.Authority.AllowedAddresses[i] = strings.ToLower(a)
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	setDur := func(d *Duration, v string) {
		if d.Duration == 0 {
			d.Duration, _ = time.ParseDuration(v)
		}
	}
	setDur(&c.Raft.ElectionTimeout, "1s")
	setDur(&c.Raft.HeartbeatTimeout, "1s")
	setDur(&c.Raft.LeaderLeaseTimeout, "500ms")
	setDur(&c.Raft.CommitTimeout, "50ms")
	setDur(&c.Raft.SnapshotInterval, "120s")
	if c.Raft.SnapshotThreshold == 0 {
		c.Raft.SnapshotThreshold = 8192
	}
	if c.Raft.TrailingLogs == 0 {
		c.Raft.TrailingLogs = 10240
	}
	setDur(&c.ApplyTimeout, "5s")
	if c.Seal.MaxStatements == 0 {
		c.Seal.MaxStatements = 256
	}
	setDur(&c.Seal.MaxAge, "2s")
	setDur(&c.Ingress.MaxStatementAge, "5m")
	setDur(&c.Dispatch.RetryInterval, "5s")
	if c.Anchor.Backend == "" {
		c.Anchor.Backend = "local"
	}
	if c.Raft.Advertise == "" {
		c.Raft.Advertise = c.Raft.Listen
	}
}

func (c *Config) Validate() error {
	var errs []error
	req := func(v, name string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req(c.NodeID, "node_id")
	req(c.Raft.Listen, "raft.listen")
	req(c.Raft.DataDir, "raft.data_dir")
	req(c.GRPCListen, "grpc_listen")
	req(c.Genesis.SchemaSnapshotID, "genesis.schema_snapshot_id")
	req(c.Genesis.ExecutorProfileID, "genesis.executor_profile_id")
	if c.Raft.Bootstrap && len(c.Raft.Peers) == 0 {
		errs = append(errs, errors.New("raft.bootstrap requires raft.peers"))
	}
	if c.Anchor.Backend != "local" {
		errs = append(errs, fmt.Errorf("anchor.backend %q not in allowlist [local]", c.Anchor.Backend))
	}
	if c.Authority.PrivateKeyHex != "" {
		if _, err := crypto.HexToECDSA(strings.TrimPrefix(c.Authority.PrivateKeyHex, "0x")); err != nil {
			errs = append(errs, fmt.Errorf("authority.private_key_hex: %w", err))
		}
	}
	for _, d := range []struct {
		v    Duration
		name string
	}{
		{c.Raft.ElectionTimeout, "raft.election_timeout"}, {c.Raft.HeartbeatTimeout, "raft.heartbeat_timeout"},
		{c.Raft.LeaderLeaseTimeout, "raft.leader_lease_timeout"}, {c.Raft.CommitTimeout, "raft.commit_timeout"},
		{c.Raft.SnapshotInterval, "raft.snapshot_interval"}, {c.ApplyTimeout, "apply_timeout"},
		{c.Seal.MaxAge, "seal.max_age"}, {c.Ingress.MaxStatementAge, "ingress.max_statement_age"},
		{c.Dispatch.RetryInterval, "dispatch.retry_interval"},
	} {
		if d.v.Duration <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive", d.name))
		}
	}
	if c.Seal.MaxStatements <= 0 {
		errs = append(errs, errors.New("seal.max_statements must be positive"))
	}
	return errors.Join(errs...)
}
```

(`config.go` needs `"time"` in duration defaults — put `applyDefaults`' ParseDuration usage accordingly; run `go mod tidy` to add `gopkg.in/yaml.v3`.)

- [ ] **Step 4: Run + commit**

Run: `go mod tidy && go test ./config/ -v 2>&1 | tail -8 && go test ./... 2>&1 | tail -3` — Expected: PASS.

```bash
git add config/ go.mod go.sum
git commit -m "feat(config): arbiter config schema with Open-Q8 defaults and validation

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 9: orchestrator core — deps, loop, re-entry, seal trigger

**Files:**
- Create: `orchestrator/deps.go`, `orchestrator/loop.go`, `orchestrator/seal.go`, `orchestrator/orchestrator_fakes_test.go`, `orchestrator/loop_test.go`
- (The frozen `orchestrator/orchestrator.go` seam is untouched; `*Orchestrator` satisfies it.)

**Interfaces:**
- Consumes: `raftnode.ConsensusNode`, `fsm` (`NewWithNotify`, `Event`, `WorkSet`, `OpenBlockStats`, results), `wire.Encode`, `anchor.Client`, `authority.Signer`, pb dispatch types.
- Produces (Tasks 10-11 extend; Task 15/16 consume):

```go
type VerifierDirectory interface {
	Send(replicaID string, msg *pb.VerifierDispatch) error
	Connected() []string
}
type SNodeDirectory interface {
	Send(nodeID string, msg *pb.PromotionCommand) error
	Connected() []string
}
type Config struct {
	ApplyTimeout      time.Duration
	SealMaxStatements int
	SealMaxAge        time.Duration
	RetryInterval     time.Duration
}
type Deps struct {
	Node      raftnode.ConsensusNode
	FSM       *fsm.FSM
	Events    <-chan fsm.Event
	Anchor    anchor.Client
	Signer    *authority.Signer
	Verifiers VerifierDirectory
	SNodes    SNodeDirectory
	Cfg       Config
	Logger    *slog.Logger // nil → slog.Default()
}
func New(d Deps) *Orchestrator          // *Orchestrator implements the frozen seam's Run(ctx) error
func (o *Orchestrator) Poke()            // non-blocking wake (server calls it on subscribe)
var ErrRejected = errors.New("orchestrator: proposal rejected by fsm")
func (o *Orchestrator) propose(cmd wire.Command) (any, error)   // VerifyLeader → Encode → Apply → map fsm.Rejected to ErrRejected
```

Loop contract: `Run` = `VerifyLeader → Barrier(ApplyTimeout) → rescan → select {Events, wake, seal ticker (250ms), retry ticker (RetryInterval → rescan), ctx.Done}`. `rescan` reads `WorkSet()` and calls the per-inventory handlers; Tasks 10-11 fill `handleWorkSet`'s rows — Task 9 lands the loop with ONLY the seal row live and a `handleWorkSet(ws)` extension point that Tasks 10-11 grow (unhandled rows are no-ops in Task 9, NOT stubs that reject).

Seal rule (leader-local age clock; design §3 wall-clock note): the orchestrator remembers `(StatementSeqStart, firstSeen time.Time)`; when `OpenBlockStats().StatementSeqStart` changes and Count > 0, reset `firstSeen = now`. Propose `SealL3Block` when `Count >= SealMaxStatements` or (`Count > 0` and `now - firstSeen >= SealMaxAge`). Age resets on failover (accepted: delays a seal by ≤ SealMaxAge).

- [ ] **Step 1: Write the fakes + failing tests**

Create `orchestrator/orchestrator_fakes_test.go`:

```go
package orchestrator

import (
	"crypto/ed25519"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/wire"
)

// fakeFuture satisfies raft.ApplyFuture over a precomputed response.
type fakeFuture struct {
	err  error
	resp any
}

func (f fakeFuture) Error() error          { return f.err }
func (f fakeFuture) Response() any         { return f.resp }
func (f fakeFuture) Index() uint64         { return 0 }

// fakeNode applies commands directly to a real FSM (single-node "raft").
type fakeNode struct {
	mu     sync.Mutex
	f      *fsm.FSM
	leader bool
}

func (n *fakeNode) Apply(cmd []byte, _ time.Duration) raft.ApplyFuture {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.leader {
		return fakeFuture{err: raft.ErrNotLeader}
	}
	return fakeFuture{resp: n.f.Apply(&raft.Log{Data: cmd})}
}
func (n *fakeNode) VerifyLeader() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.leader {
		return raft.ErrNotLeader
	}
	return nil
}
func (n *fakeNode) LeaderCh() <-chan bool                  { return nil }
func (n *fakeNode) Barrier(time.Duration) error            { return nil }

type sentDispatch struct {
	to  string
	msg *pb.VerifierDispatch
}

type fakeVerifiers struct {
	mu        sync.Mutex
	connected []string
	sent      []sentDispatch
}

func (v *fakeVerifiers) Send(id string, msg *pb.VerifierDispatch) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	for _, c := range v.connected {
		if c == id {
			v.sent = append(v.sent, sentDispatch{to: id, msg: msg})
			return nil
		}
	}
	return errors.New("not connected")
}
func (v *fakeVerifiers) Connected() []string {
	v.mu.Lock()
	defer v.mu.Unlock()
	return append([]string(nil), v.connected...)
}
func (v *fakeVerifiers) drain() []sentDispatch {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := v.sent
	v.sent = nil
	return out
}

type fakeSNodes struct {
	mu        sync.Mutex
	connected []string
	sent      []*pb.PromotionCommand
}

func (s *fakeSNodes) Send(id string, msg *pb.PromotionCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, msg)
	return nil
}
func (s *fakeSNodes) Connected() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.connected...)
}

type fakeAnchor struct {
	mu    sync.Mutex
	calls int
}

func (a *fakeAnchor) Anchor(_ context.Context, hash, root string) (arbiter.AnchorRef, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return arbiter.AnchorRef{L3BlockHash: hash, StateRoot: root, L2TxRef: "fake:" + hash, L2BlockNumber: uint64(a.calls)}, nil
}
func (a *fakeAnchor) WaitFinality(context.Context, arbiter.AnchorRef) (bool, bool, error) {
	return true, true, nil
}

// waitFor polls a condition with a deadline (bounded; no bare sleeps as sync).
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

// mustEncode is a test helper for direct-to-fsm setup outside the loop.
func mustEncode(t *testing.T, c wire.Command) []byte {
	t.Helper()
	b, err := wire.Encode(c)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}
```

(Fixtures for driving the FSM — accounts, envelopes, RCs, receipts, signed evidence — are needed here too but live in the fsm package's tests. Rather than duplicating them, `orchestrator` tests build statements through a SMALL local helper set: copy `signUserJWS`/`validEnvelope`/`testAccount`-equivalents into `orchestrator_fakes_test.go` with the same code as fsm's versions — cross-package test reuse is not possible without an export, and a `fsmtest` helper package is not worth it for three 15-line helpers. Copy them verbatim from `fsm/admission_test.go` / `fsm/evidence_test.go` (`ed25519KeyFor`, `signedAttestationFor` building `replay.ReplayAttestation` with `ed25519.Sign`), adjusting only package-private references.)

Create `orchestrator/loop_test.go`:

```go
package orchestrator

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/fsm"
)

// newHarness builds an FSM + fake node + orchestrator with fast tickers.
func newHarness(t *testing.T, params fsm.Params) (*fsm.FSM, *fakeNode, *fakeVerifiers, *fakeSNodes, *fakeAnchor, *Orchestrator, chan fsm.Event) {
	t.Helper()
	ev := make(chan fsm.Event, 256)
	f := fsm.NewWithNotify(params, ev)
	node := &fakeNode{f: f, leader: true}
	fv := &fakeVerifiers{connected: []string{"v1", "v2", "v3"}}
	fs := &fakeSNodes{connected: []string{"s1"}}
	fa := &fakeAnchor{}
	o := New(Deps{Node: node, FSM: f, Events: ev, Anchor: fa, Verifiers: fv, SNodes: fs,
		Cfg:    Config{ApplyTimeout: time.Second, SealMaxStatements: 4, SealMaxAge: 40 * time.Millisecond, RetryInterval: 25 * time.Millisecond},
		Logger: slog.Default()})
	return f, node, fv, fs, fa, o, ev
}

func runLoop(t *testing.T, o *Orchestrator) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = o.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return cancel
}

func TestLoop_SealsByCount(t *testing.T) {
	f, node, _, _, _, o, _ := newHarness(t, testParamsO())
	seedWriterAndVerifiers(t, node) // registers s1 + v1..v3 Active via node.Apply
	runLoop(t, o)
	key, account := testAccountO(t)
	for seq := uint64(1); seq <= 4; seq++ { // SealMaxStatements = 4
		submitViaNode(t, node, key, account, seq)
	}
	waitFor(t, "seal by count", 2*time.Second, func() bool {
		s, _ := f.Summary()
		return s.NextL3BlockSeq == 2
	})
}

func TestLoop_SealsByAge(t *testing.T) {
	f, node, _, _, _, o, _ := newHarness(t, testParamsO())
	seedWriterAndVerifiers(t, node)
	runLoop(t, o)
	key, account := testAccountO(t)
	submitViaNode(t, node, key, account, 1) // single statement, below count threshold
	waitFor(t, "seal by age", 2*time.Second, func() bool {
		s, _ := f.Summary()
		return s.NextL3BlockSeq == 2
	})
}

func TestLoop_NotLeaderStopsSideEffects(t *testing.T) {
	_, node, _, _, _, o, _ := newHarness(t, testParamsO())
	node.leader = false
	if err := o.Run(context.Background()); err == nil {
		t.Fatal("Run on a non-leader must fail fast (VerifyLeader gate)")
	}
	_ = raft.ErrNotLeader
}
```

(`testParamsO`, `testAccountO`, `seedWriterAndVerifiers`, `submitViaNode` are the copied helper set described above — full bodies in the fakes file; `submitViaNode` builds a `validEnvelope` and applies `wire.Command{SubmitStatement: ...}` through `node.Apply`, failing the test on a `fsm.Rejected`/`fsm.SubmitResult` non-Accepted response.)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./orchestrator/ 2>&1 | head -5` — Expected: compile failure (`New`, `Deps` undefined).

- [ ] **Step 3: Implement `deps.go` + `loop.go` + `seal.go`**

`orchestrator/deps.go`:

```go
package orchestrator

import (
	"errors"
	"log/slog"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter/anchor"
	"github.com/sentioxyz/arbiter/authority"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/raftnode"
)

// VerifierDirectory is the server's verifier stream registry as the
// orchestrator sees it (defined here so server does not import
// orchestrator nor vice versa; cmd wires the concrete registry in).
type VerifierDirectory interface {
	Send(replicaID string, msg *pb.VerifierDispatch) error
	Connected() []string
}

// SNodeDirectory is the SNode-side counterpart.
type SNodeDirectory interface {
	Send(nodeID string, msg *pb.PromotionCommand) error
	Connected() []string
}

// Config carries the loop's timing knobs (from config yaml via cmd).
type Config struct {
	ApplyTimeout      time.Duration
	SealMaxStatements int
	SealMaxAge        time.Duration
	RetryInterval     time.Duration
}

// Deps is everything the leader loop needs. The orchestrator performs ALL
// I/O and never mutates FSM state except by proposing commands (§3.2).
type Deps struct {
	Node      raftnode.ConsensusNode
	FSM       *fsm.FSM
	Events    <-chan fsm.Event
	Anchor    anchor.Client
	Signer    *authority.Signer
	Verifiers VerifierDirectory
	SNodes    SNodeDirectory
	Cfg       Config
	Logger    *slog.Logger
}

// ErrRejected marks a proposal the FSM rejected (state moved underneath
// us — typically a failover race). The caller logs and rescans; never
// blind-retries the same bytes (design §4 failure semantics).
var ErrRejected = errors.New("orchestrator: proposal rejected by fsm")
```

`orchestrator/loop.go`:

```go
package orchestrator

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/wire"
)

// sealCheckInterval is the open-block age poll period (leader-local).
const sealCheckInterval = 250 * time.Millisecond

// Orchestrator is the leader-only side-effect loop implementing the frozen
// §3.4 seam. Single goroutine: I/O helpers run inline (v1 volumes) —
// every side effect re-checks VerifyLeader first.
type Orchestrator struct {
	d    Deps
	wake chan struct{}

	// leader-local seal age clock (design §3 wall-clock note): tracks when
	// the CURRENT open block was first observed by this leader.
	openStart     uint64
	openFirstSeen time.Time
}

// New builds the loop. Logger nil → slog.Default().
func New(d Deps) *Orchestrator {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &Orchestrator{d: d, wake: make(chan struct{}, 1)}
}

// Poke wakes the loop (non-blocking); the server calls it on subscribe.
func (o *Orchestrator) Poke() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// Run implements the frozen Orchestrator seam: §10.2 re-entry then the
// event/ticker select loop. Returns when ctx is cancelled or leadership
// cannot be verified at entry.
func (o *Orchestrator) Run(ctx context.Context) error {
	if err := o.d.Node.VerifyLeader(); err != nil {
		return fmt.Errorf("orchestrator: not leader at entry: %w", err)
	}
	if err := o.d.Node.Barrier(o.d.Cfg.ApplyTimeout); err != nil {
		return fmt.Errorf("orchestrator: barrier: %w", err)
	}
	o.rescan(ctx)

	sealT := time.NewTicker(sealCheckInterval)
	defer sealT.Stop()
	retryT := time.NewTicker(o.d.Cfg.RetryInterval)
	defer retryT.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-o.d.Events:
			o.onEvent(ctx, e)
		case <-o.wake:
			o.rescan(ctx)
		case <-sealT.C:
			o.checkSeal(ctx)
		case <-retryT.C:
			o.rescan(ctx) // drop-safety backstop: the scan is the truth
		}
	}
}

// rescan re-derives the whole work inventory and acts on every row.
func (o *Orchestrator) rescan(ctx context.Context) {
	ws, err := o.d.FSM.WorkSet()
	if err != nil {
		o.d.Logger.Warn("workset scan failed", "err", err)
		return
	}
	o.checkSeal(ctx)
	o.handleWorkSet(ctx, ws)
}

// handleWorkSet acts on the inventory. Task 9 wires no rows beyond seal;
// Task 10 adds mark/dispatch/challenge, Task 11 adds
// anchor/promotion/manifest/cleanup. Rows without handlers are no-ops.
func (o *Orchestrator) handleWorkSet(ctx context.Context, ws fsm.WorkSet) {
	o.handleDispatchRows(ctx, ws)  // Task 10 (no-op body in Task 9)
	o.handlePromotionRows(ctx, ws) // Task 11 (no-op body in Task 9)
}

// Task 9 placeholders — REPLACED by Tasks 10/11 (deliberately empty, not
// rejecting: the loop must run end-to-end from Task 9 onward).
func (o *Orchestrator) handleDispatchRows(context.Context, fsm.WorkSet)  {}
func (o *Orchestrator) handlePromotionRows(context.Context, fsm.WorkSet) {}

// onEvent routes a wake hint to the cheapest targeted action; every
// handler re-reads committed state, so a stale or dropped event is safe.
func (o *Orchestrator) onEvent(ctx context.Context, e fsm.Event) {
	switch e.Kind {
	case fsm.EventAdmitted:
		o.checkSeal(ctx)
	default:
		o.rescan(ctx)
	}
}

// propose encodes and applies one command through the consensus seam.
func (o *Orchestrator) propose(cmd wire.Command) (any, error) {
	if err := o.d.Node.VerifyLeader(); err != nil {
		return nil, err
	}
	b, err := wire.Encode(cmd)
	if err != nil {
		return nil, fmt.Errorf("encode: %w", err)
	}
	fut := o.d.Node.Apply(b, o.d.Cfg.ApplyTimeout)
	if err := fut.Error(); err != nil {
		return nil, err
	}
	res := fut.Response()
	if r, ok := res.(fsm.Rejected); ok {
		o.d.Logger.Warn("proposal rejected", "reason", r.Reason)
		return res, ErrRejected
	}
	return res, nil
}
```

`orchestrator/seal.go`:

```go
package orchestrator

import (
	"context"
	"time"

	"github.com/sentioxyz/arbiter/wire"
)

// checkSeal applies the §5.1 trigger: count OR leader-observed age.
func (o *Orchestrator) checkSeal(ctx context.Context) {
	st := o.d.FSM.OpenBlockStats()
	if st.Count == 0 {
		o.openStart = 0
		return
	}
	if st.StatementSeqStart != o.openStart {
		o.openStart = st.StatementSeqStart
		o.openFirstSeen = time.Now()
	}
	if st.Count >= o.d.Cfg.SealMaxStatements || time.Since(o.openFirstSeen) >= o.d.Cfg.SealMaxAge {
		if _, err := o.propose(wire.Command{SealL3Block: &wire.SealL3Block{}}); err != nil && err != ErrRejected {
			o.d.Logger.Warn("seal proposal failed", "err", err)
		}
	}
	_ = ctx
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./orchestrator/ -race -v 2>&1 | tail -8` then `go test ./... 2>&1 | tail -4`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add orchestrator/
git commit -m "feat(orchestrator): leader loop with §10.2 re-entry and seal trigger

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 10: orchestrator dispatch rows — mark, evidence two-round, challenge

**Files:**
- Create: `orchestrator/dispatch.go`, `orchestrator/dispatch_test.go`
- Modify: `orchestrator/loop.go` (replace the `handleDispatchRows` placeholder body)

**Interfaces:**
- Consumes: Task 3 `WorkSet`/`BlockDispatchInfo`, Task 6 dispatch builders, Task 9 loop/propose/fakes.
- Produces: real `handleDispatchRows(ctx, ws)` covering `SealedUnmarked` (propose MarkReplaying), `EvidenceIncomplete` (ReplayJob to members missing attestation; ByteSideScanRequest to members with attestation but no scan), `QuorumFailed` (OpenChallenge → ResolveChallenge(REJECTED), v1 immediate).

- [ ] **Step 1: Write the failing tests**

Create `orchestrator/dispatch_test.go`:

```go
package orchestrator

import (
	"testing"
	"time"
)

func TestDispatch_MarksAndSendsJobsThenScans(t *testing.T) {
	f, node, fv, _, _, o, _ := newHarness(t, testParamsO())
	seedWriterAndVerifiers(t, node)
	key, account := testAccountO(t)
	submitViaNode(t, node, key, account, 1)
	sealViaNode(t, node)
	registerRCViaNode(t, node, f, 1) // binds an RC claiming partHashO (helper: fixed part hash)
	runLoop(t, o)

	// The loop must propose MarkReplaying, then send ReplayJobs to all
	// three connected members of the selected set.
	waitFor(t, "replay jobs sent", 2*time.Second, func() bool {
		jobs := 0
		for _, d := range fv.drain() {
			if d.msg.GetReplayJob() != nil {
				jobs++
			}
		}
		return jobs >= 3
	})

	// Land one attestation directly (as if a verifier answered): the loop
	// must follow up with a ByteSideScanRequest to THAT replica only.
	set := verifierSetOf(t, f, 1)
	applyAttestationViaNode(t, node, f, 1, set[0])
	o.Poke()
	waitFor(t, "scan request follows attestation", 2*time.Second, func() bool {
		for _, d := range fv.drain() {
			if sr := d.msg.GetByteSideScan(); sr != nil && d.to == set[0] && sr.GetBlockSeq() == 1 {
				return true
			}
		}
		return false
	})
}

func TestDispatch_QuorumFailureOpensAndResolvesChallenge(t *testing.T) {
	f, node, _, _, _, o, _ := newHarness(t, testParamsO())
	seedWriterAndVerifiers(t, node)
	key, account := testAccountO(t)
	submitViaNode(t, node, key, account, 1)
	sealViaNode(t, node)
	registerRCViaNode(t, node, f, 1)
	markReplayingViaNode(t, node, 1)
	// complete 3/3 evidence with mismatching roots: quorum fails
	for _, rid := range verifierSetOf(t, f, 1) {
		applyBadAttestationViaNode(t, node, f, 1, rid)
		applyScanViaNode(t, node, f, 1, rid)
	}
	runLoop(t, o)
	waitFor(t, "challenge resolved REJECTED", 2*time.Second, func() bool {
		return statementStatusOf(t, f, 1) == "Rejected"
	})
}

func TestDispatch_RetriesSilentVerifier(t *testing.T) {
	f, node, fv, _, _, o, _ := newHarness(t, testParamsO())
	seedWriterAndVerifiers(t, node)
	key, account := testAccountO(t)
	submitViaNode(t, node, key, account, 1)
	sealViaNode(t, node)
	registerRCViaNode(t, node, f, 1)
	runLoop(t, o)
	waitFor(t, "first wave", 2*time.Second, func() bool { return len(fv.drain()) > 0 })
	// no attestation ever lands: the retry ticker must re-send
	waitFor(t, "retry wave", 2*time.Second, func() bool {
		for _, d := range fv.drain() {
			if d.msg.GetReplayJob() != nil {
				return true
			}
		}
		return false
	})
}
```

(New helpers in `orchestrator_fakes_test.go` — full bodies to add there: `sealViaNode`, `registerRCViaNode` (builds the same RC shape as fsm's `rcFor` with `partHashO := lthashHex-equivalent` computed via `pkg/lthash` locally, plus PartitionNewPartSums), `markReplayingViaNode`, `verifierSetOf` (reads `f.BlockDispatchInfo(seq).VerifierSet`), `applyAttestationViaNode` / `applyBadAttestationViaNode` (build `replay.ReplayAttestation` with `receiptForBlockO` + `ed25519KeyFor`-signed hash; the bad variant sets `ComputedStateRoot: "0xdifferent"`), `applyScanViaNode` (matching `signedScan` equivalent), `statementStatusOf` (maps `fsm` status via a small exported-string helper — add `func (s Status) String() string` to fsm/state.go in this task if not present; keep it a plain switch).)

- [ ] **Step 2: Run to verify failure**

Run: `go test ./orchestrator/ -run TestDispatch 2>&1 | head -5` — Expected: FAIL (rows are no-ops).

- [ ] **Step 3: Implement `orchestrator/dispatch.go`** (and delete the Task 9 `handleDispatchRows` placeholder)

```go
package orchestrator

import (
	"context"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/wire"
)

// handleDispatchRows drives SealedUnmarked → EvidenceIncomplete →
// QuorumFailed (design §4 rows 2-4). All actions idempotent: re-marking is
// FSM-rejected (absorbed), re-sent jobs are deduped verifier-side by
// (replica, block) first-wins, challenge commands re-tolerate.
func (o *Orchestrator) handleDispatchRows(ctx context.Context, ws fsm.WorkSet) {
	for _, blockSeq := range ws.SealedUnmarked {
		if _, err := o.propose(wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: blockSeq}}); err != nil && err != ErrRejected {
			o.d.Logger.Warn("mark replaying failed", "block", blockSeq, "err", err)
		}
	}
	for _, be := range ws.EvidenceIncomplete {
		o.dispatchEvidence(ctx, be)
	}
	for _, blockSeq := range ws.QuorumFailed {
		// v1 immediate adjudication (§7.5): the same predicate already
		// failed over complete evidence — open and resolve REJECTED.
		if _, err := o.propose(wire.Command{OpenChallenge: &wire.OpenChallenge{BlockSeq: blockSeq, Reason: "three-way quorum failed over complete evidence", OpenedBy: "orchestrator"}}); err != nil && err != ErrRejected {
			o.d.Logger.Warn("open challenge failed", "block", blockSeq, "err", err)
			continue
		}
		if _, err := o.propose(wire.Command{ResolveChallenge: &wire.ResolveChallenge{BlockSeq: blockSeq, Verdict: wire.ChallengeVerdictRejected}}); err != nil && err != ErrRejected {
			o.d.Logger.Warn("resolve challenge failed", "block", blockSeq, "err", err)
		}
	}
}

// dispatchEvidence sends round-one jobs and round-two scan requests to the
// connected members that still owe evidence (§7.1).
func (o *Orchestrator) dispatchEvidence(_ context.Context, be fsm.BlockEvidence) {
	info, ok := o.d.FSM.BlockDispatchInfo(be.BlockSeq)
	if !ok {
		return
	}
	job := replay.ReplayJob{
		BlockSeq:           info.Header.L3BlockSeq,
		PrevSafeSnapshotID: info.Header.PrevSafeSnapshotID,
		PrevStateRoot:      info.Header.PrevStateRoot,
		SchemaSnapshotID:   info.Header.SchemaSnapshotID,
		ExecutorProfileID:  info.Header.ExecutorProfileID,
		SourceClaimRoot:    info.SourceClaimRoot,
		Statements:         info.Statements,
	}
	connected := map[string]bool{}
	for _, id := range o.d.Verifiers.Connected() {
		connected[id] = true
	}
	for _, rid := range info.VerifierSet {
		if !connected[rid] {
			continue
		}
		if err := o.d.Node.VerifyLeader(); err != nil {
			return
		}
		switch {
		case !info.HasAttestation[rid]:
			if err := o.d.Verifiers.Send(rid, wire.ReplayJobDispatch(job)); err != nil {
				o.d.Logger.Warn("replay job send failed", "replica", rid, "err", err)
			}
		case !info.HasScan[rid]:
			if err := o.d.Verifiers.Send(rid, wire.ByteSideScanDispatch(be.BlockSeq, info.CandidateParts)); err != nil {
				o.d.Logger.Warn("scan request send failed", "replica", rid, "err", err)
			}
		}
	}
}
```

- [ ] **Step 4: Run + commit**

Run: `go test ./orchestrator/ -race -v 2>&1 | tail -8 && go test ./... 2>&1 | tail -3` — Expected: PASS.

```bash
git add orchestrator/ fsm/
git commit -m "feat(orchestrator): mark/evidence-two-round/challenge dispatch rows

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 11: orchestrator promotion chain — anchor, promote, manifest, cleanup

**Files:**
- Create: `orchestrator/promotion.go`, `orchestrator/promotion_test.go`
- Modify: `orchestrator/loop.go` (replace `handlePromotionRows` placeholder), `fsm/reads.go` (+`Ref` field on `BlockAnchor`; +`PromotionSeqView()`; +`GenesisParams()`), `fsm/reads_test.go`

**Interfaces:**
- Consumes: Tasks 3/6/7/9-10; `authority.Signer.SignPromotion/SignCleanup`; `replay.SafeSnapshotManifest.Seal`.
- Produces: real `handlePromotionRows` covering `UnanchoredVerified` (anchor + finality → RecordAnchorFinality; if already Anchored but flags incomplete → re-WaitFinality on the recorded `Ref`), `PromotablePending` (sign + RecordPromotionIssued + stream to all connected SNodes), `IssuedUnacked` (re-sign + re-send — §10.2 failover semantics), `NeedManifest` (ManifestInputs → SchemaRoot rule `replay.DigestString("schema:" + schemaID)` → Seal → PublishSafeSnapshot), `PublishedUncleaned` (sign cleanup + ScheduleUnsafeCleanup + stream). fsm additions: `BlockAnchor.Ref arbiter.AnchorRef` (recorded anchor when `Anchored`), `func (f *FSM) PromotionSeqView() uint64`, `func (f *FSM) GenesisParams() (schemaSnapshotID, executorProfileID string)`.

- [ ] **Step 1: Write the failing tests**

Create `orchestrator/promotion_test.go`:

```go
package orchestrator

import (
	"testing"
	"time"
)

// driveToQuorum: submit→seal→rc→mark→3/3 good evidence via node helpers.
func driveToQuorum(t *testing.T, node *fakeNode, f fsmHandle) { // fsmHandle = *fsm.FSM alias in fakes
	t.Helper()
	key, account := testAccountO(t)
	submitViaNode(t, node, key, account, 1)
	sealViaNode(t, node)
	registerRCViaNode(t, node, f, 1)
	markReplayingViaNode(t, node, 1)
	for _, rid := range verifierSetOf(t, f, 1)[:2] {
		applyAttestationViaNode(t, node, f, 1, rid)
		applyScanViaNode(t, node, f, 1, rid)
	}
}

func TestPromotion_FullChainToCleanup(t *testing.T) {
	f, node, _, fsn, fa, o, _ := newHarness(t, authorityParamsO(t)) // params carrying a real signer allowlist
	seedWriterAndVerifiers(t, node)
	driveToQuorum(t, node, f)
	runLoop(t, o)

	// anchor → RecordAnchorFinality → Promotable → issued → streamed
	waitFor(t, "promotion streamed", 3*time.Second, func() bool {
		fsn.mu.Lock()
		defer fsn.mu.Unlock()
		for _, pc := range fsn.sent {
			if pc.GetPromote() != nil && pc.GetAuthorityJws() != "" {
				return true
			}
		}
		return false
	})
	if fa.calls == 0 {
		t.Fatal("anchor client must have been called")
	}

	// SNode acks (helper computes the closure-correct post commitment)
	ackPromotionViaNode(t, node, f, 1)
	o.Poke()

	// manifest published + cleanup streamed + cleanup scheduled
	waitFor(t, "watermark advanced", 3*time.Second, func() bool {
		return f.SafeWatermarkView().SafeBlockSeq == 1
	})
	waitFor(t, "cleanup streamed", 3*time.Second, func() bool {
		fsn.mu.Lock()
		defer fsn.mu.Unlock()
		for _, pc := range fsn.sent {
			if pc.GetCleanup() != nil {
				return true
			}
		}
		return false
	})
	// published manifest must be content-addressed and resolvable
	wm := f.SafeWatermarkView()
	if m, ok := f.ManifestByID(wm.SnapshotID); !ok || m.SnapshotID != m.ManifestRoot {
		t.Fatalf("manifest: ok=%v", ok)
	}
}

func TestPromotion_ResendIssuedUnacked(t *testing.T) {
	f, node, _, fsn, _, o, _ := newHarness(t, authorityParamsO(t))
	seedWriterAndVerifiers(t, node)
	driveToQuorum(t, node, f)
	runLoop(t, o)
	waitFor(t, "first promotion send", 3*time.Second, func() bool {
		fsn.mu.Lock()
		defer fsn.mu.Unlock()
		return len(fsn.sent) >= 1
	})
	// never ack: the retry ticker must re-sign and re-send
	waitFor(t, "re-send", 3*time.Second, func() bool {
		fsn.mu.Lock()
		defer fsn.mu.Unlock()
		promos := 0
		for _, pc := range fsn.sent {
			if pc.GetPromote() != nil {
				promos++
			}
		}
		return promos >= 2
	})
}

func TestPromotion_EventLossBackstop(t *testing.T) {
	// The tripwire test: NO events delivered at all — the retry-ticker
	// rescan alone must drive the pipeline to the watermark.
	f, node, _, _, _, o, ev := newHarness(t, authorityParamsO(t))
	go func() { // swallow every event before the loop can see it
		for range ev {
		}
	}()
	seedWriterAndVerifiers(t, node)
	driveToQuorum(t, node, f)
	runLoop(t, o)
	ackWhenIssued(t, node, f, 1) // helper: polls IssuedUnacked, then acks
	waitFor(t, "pipeline completes without events", 5*time.Second, func() bool {
		return f.SafeWatermarkView().SafeBlockSeq == 1
	})
}
```

(New helpers: `authorityParamsO` generates a signer key, returns `fsm.Params` with the address allowlisted AND stores the signer for `newHarness` to place into `Deps.Signer` — adjust `newHarness` to accept an optional signer; `ackPromotionViaNode` reads `WorkSet().IssuedUnacked[0]`, computes the closure-correct post commitment by lthash-adding the promote's part hashes over the current base, applies `RecordPromotionAck` via node; `ackWhenIssued` = waitFor(IssuedUnacked non-empty) + ackPromotionViaNode.)

Append to `fsm/reads_test.go`:

```go
func TestReads_PromotionSeqAndGenesis(t *testing.T) {
	f := newTestFSM(t)
	if f.PromotionSeqView() != 0 {
		t.Fatal("fresh promotion seq must be 0")
	}
	sid, pid := f.GenesisParams()
	if sid != "schema-genesis" || pid != "housegate-replay-mvp-v0" {
		t.Fatalf("genesis params: %q %q", sid, pid)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./orchestrator/ -run TestPromotion 2>&1 | head -5` — Expected: FAIL (rows are no-ops) / compile failure on new fsm reads.

- [ ] **Step 3: Implement**

`fsm/reads.go` additions: `Ref arbiter.AnchorRef` field on `BlockAnchor` (populated from `bv.Anchor` when non-nil in the WorkSet builder);

```go
// PromotionSeqView returns the last issued promotion seq.
func (f *FSM) PromotionSeqView() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.st.PromotionSeq
}

// GenesisParams exposes the consensus genesis identifiers for manifest
// assembly (they are fsm Params — identical cluster-wide by construction).
func (f *FSM) GenesisParams() (schemaSnapshotID, executorProfileID string) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.st.Params.SchemaSnapshotID, f.st.Params.ExecutorProfileID
}
```

`orchestrator/promotion.go` (delete the Task 9 placeholder):

```go
package orchestrator

import (
	"context"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/wire"
)

// handlePromotionRows drives anchor → promote → manifest → cleanup
// (design §4 rows 5-9). Every action is idempotent or FSM-gated (§10.3);
// re-signing on re-send is the §10.2 failover semantic.
func (o *Orchestrator) handlePromotionRows(ctx context.Context, ws fsm.WorkSet) {
	for _, ba := range ws.UnanchoredVerified {
		o.anchorBlock(ctx, ba)
	}
	for _, pw := range ws.PromotablePending {
		o.issuePromotion(pw)
	}
	for _, pp := range ws.IssuedUnacked {
		o.streamPromotion(pp.Promote)
	}
	if ws.NeedManifest {
		o.publishManifest()
	}
	for _, cw := range ws.PublishedUncleaned {
		o.scheduleCleanup(cw)
	}
}

func (o *Orchestrator) anchorBlock(ctx context.Context, ba fsm.BlockAnchor) {
	if err := o.d.Node.VerifyLeader(); err != nil {
		return
	}
	ref := ba.Ref
	if !ba.Anchored {
		var err error
		ref, err = o.d.Anchor.Anchor(ctx, ba.ChainHash, ba.StateRoot)
		if err != nil {
			o.d.Logger.Warn("anchor failed", "block", ba.BlockSeq, "err", err)
			return
		}
	}
	fin, lm, err := o.d.Anchor.WaitFinality(ctx, ref)
	if err != nil {
		o.d.Logger.Warn("wait finality failed", "block", ba.BlockSeq, "err", err)
		return
	}
	if _, err := o.propose(wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{
		L3BlockSeq: ba.BlockSeq, Anchor: ref, FinalityReached: fin, LastMergeableReached: lm}}); err != nil && err != ErrRejected {
		o.d.Logger.Warn("record anchor failed", "block", ba.BlockSeq, "err", err)
	}
}

func (o *Orchestrator) issuePromotion(pw fsm.PromotionWork) {
	if o.d.Signer == nil {
		o.d.Logger.Warn("no authority signer configured; skipping promotion", "partition", pw.Partition)
		return
	}
	cmd := arbiter.PromoteSafePartition{
		TableID: pw.Partition.TableID, PartitionID: pw.Partition.PartitionID,
		PromotionSeq:       o.d.FSM.PromotionSeqView() + 1,
		BaseSafeSnapshotID: pw.BaseSafeSnapshotID, BasePartitionRoot: pw.BasePartitionRoot,
		CandidateParts: pw.Parts,
	}
	jws, err := o.d.Signer.SignPromotion(cmd)
	if err != nil {
		o.d.Logger.Warn("sign promotion failed", "err", err)
		return
	}
	if _, err := o.propose(wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: cmd, AuthorityJWS: jws}}); err != nil {
		return // Rejected = state moved (e.g. seq raced a failover); rescan will retry
	}
	o.streamPromotionSigned(cmd, jws)
}

// streamPromotion re-signs (§10.2: any elected leader may re-sign) and
// sends an already-recorded promotion to every connected SNode.
func (o *Orchestrator) streamPromotion(cmd arbiter.PromoteSafePartition) {
	if o.d.Signer == nil {
		return
	}
	jws, err := o.d.Signer.SignPromotion(cmd)
	if err != nil {
		o.d.Logger.Warn("re-sign promotion failed", "err", err)
		return
	}
	o.streamPromotionSigned(cmd, jws)
}

func (o *Orchestrator) streamPromotionSigned(cmd arbiter.PromoteSafePartition, jws string) {
	if err := o.d.Node.VerifyLeader(); err != nil {
		return
	}
	msg := wire.PromotionCommandPB(cmd, jws)
	for _, id := range o.d.SNodes.Connected() {
		if err := o.d.SNodes.Send(id, msg); err != nil {
			o.d.Logger.Warn("promotion send failed", "snode", id, "err", err)
		}
	}
}

func (o *Orchestrator) publishManifest() {
	mi, ok := o.d.FSM.ManifestInputs()
	if !ok {
		return
	}
	schemaID, profileID := o.d.FSM.GenesisParams()
	m := replay.SafeSnapshotManifest{
		ParentSnapshotID: mi.ParentSnapshotID, SafeBlockSeq: mi.SafeBlockSeq,
		SchemaSnapshotID: schemaID,
		// v1 placeholder rule until the DDL lane mints real schema roots
		// (design §4): deterministic, documented.
		SchemaRoot:        replay.DigestString("schema:" + schemaID),
		ExecutorProfileID: profileID,
		Tables:            mi.Tables,
	}
	sealed, err := m.Seal()
	if err != nil {
		o.d.Logger.Warn("manifest seal failed", "err", err)
		return
	}
	if _, err := o.propose(wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: sealed}}); err != nil && err != ErrRejected {
		o.d.Logger.Warn("publish manifest failed", "err", err)
	}
}

func (o *Orchestrator) scheduleCleanup(cw fsm.CleanupWork) {
	if o.d.Signer == nil {
		return
	}
	jws, err := o.d.Signer.SignCleanup(cw.Cleanup)
	if err != nil {
		o.d.Logger.Warn("sign cleanup failed", "err", err)
		return
	}
	if _, err := o.propose(wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: cw.Cleanup, AuthorityJWS: jws}}); err != nil && err != ErrRejected {
		return
	}
	if err := o.d.Node.VerifyLeader(); err != nil {
		return
	}
	msg := wire.CleanupCommandPB(cw.Cleanup, jws)
	for _, id := range o.d.SNodes.Connected() {
		if err := o.d.SNodes.Send(id, msg); err != nil {
			o.d.Logger.Warn("cleanup send failed", "snode", id, "err", err)
		}
	}
}
```

- [ ] **Step 4: Run + commit**

Run: `go test ./orchestrator/ ./fsm/ -race 2>&1 | tail -5 && go test ./... 2>&1 | tail -3` — Expected: PASS (incl. the event-loss backstop tripwire).

```bash
git add orchestrator/ fsm/
git commit -m "feat(orchestrator): anchor/promotion/manifest/cleanup chain with re-sign resend

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 12: server core — propose helper, NotLeader mapping, SafeState, Membership

**Files:**
- Create: `server/server.go`, `server/safestate.go`, `server/membership.go`, `server/server_test.go`
- Modify: `go.mod` (grpc promoted to direct via imports; run `go mod tidy`)

**Interfaces:**
- Consumes: `raftnode.ConsensusNode`, fsm reads, `wire` exported converters, pb service stubs.
- Produces (Tasks 13-16 rely on):

```go
type Deps struct {
	Node        raftnode.ConsensusNode
	FSM         *fsm.FSM
	LeaderAddr  func() string          // cmd wires from node.Raw().LeaderWithID()
	Cfg         Config
	Logger      *slog.Logger
	OnSubscribe func()                 // optional; called on any new subscription (orchestrator.Poke)
}
type Config struct {
	ApplyTimeout    time.Duration
	MaxStatementAge time.Duration      // ingress freshness gate (Task 13)
}
func New(d Deps) *Server
func (s *Server) RegisterAll(g *grpc.Server)   // registers all six services
func (s *Server) OnLeadershipLost()            // closes both registries' streams (Task 14 wires bodies)
func (s *Server) propose(ctx context.Context, cmd wire.Command) (any, error)  // NotLeader/UNAVAILABLE/InvalidArgument mapping
func notLeaderErr(addr string) error           // FAILED_PRECONDITION + pb.NotLeader detail
```

Task 12 implements SafeState + Membership for real; the other four services register with `pb.Unimplemented*Server` embeddings that Tasks 13-14 override (gRPC answers UNIMPLEMENTED until then — the service structs exist; later tasks add method bodies).

- [ ] **Step 1: Write the failing tests**

Create `server/server_test.go`:

```go
package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/sentioxyz/arbiter/fsm"
)

type fakeFuture struct {
	err  error
	resp any
}

func (f fakeFuture) Error() error  { return f.err }
func (f fakeFuture) Response() any { return f.resp }
func (f fakeFuture) Index() uint64 { return 0 }

// fakeNode applies commands directly to a real FSM.
type fakeNode struct {
	f      *fsm.FSM
	leader bool
}

func (n *fakeNode) Apply(cmd []byte, _ time.Duration) raft.ApplyFuture {
	if !n.leader {
		return fakeFuture{err: raft.ErrNotLeader}
	}
	return fakeFuture{resp: n.f.Apply(&raft.Log{Data: cmd})}
}
func (n *fakeNode) VerifyLeader() error {
	if !n.leader {
		return raft.ErrNotLeader
	}
	return nil
}
func (n *fakeNode) LeaderCh() <-chan bool       { return nil }
func (n *fakeNode) Barrier(time.Duration) error { return nil }

// startServer spins the full grpc server on bufconn.
func startServer(t *testing.T, f *fsm.FSM, leader bool) (*grpc.ClientConn, *Server, *fakeNode) {
	t.Helper()
	node := &fakeNode{f: f, leader: leader}
	s := New(Deps{Node: node, FSM: f, LeaderAddr: func() string { return "leader-addr:7001" },
		Cfg: Config{ApplyTimeout: time.Second, MaxStatementAge: 5 * time.Minute}})
	g := grpc.NewServer()
	s.RegisterAll(g)
	lis := bufconn.Listen(1 << 20)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, s, node
}

func TestMembershipAndSafeState(t *testing.T) {
	f := fsm.New(fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	conn, _, _ := startServer(t, f, true)
	mem := pb.NewMembershipClient(conn)
	ctx := context.Background()
	pk := make([]byte, 32)
	pk[0] = 1
	if _, err := mem.RegisterNode(ctx, &pb.NodeRegistration{NodeId: "v1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}, Ed25519Pubkey: pk}); err != nil {
		t.Fatalf("RegisterNode: %v", err)
	}
	if _, err := mem.MarkActive(ctx, &pb.NodeRef{NodeId: "v1"}); err != nil {
		t.Fatalf("MarkActive: %v", err)
	}
	if _, err := mem.MarkActive(ctx, &pb.NodeRef{NodeId: "ghost"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("ghost MarkActive: %v", err)
	}
	ss := pb.NewSafeStateClient(conn)
	wm, err := ss.GetSafeWatermark(ctx, &pb.GetSafeWatermarkRequest{})
	if err != nil || wm.GetSnapshotId() != "" || wm.GetSafeBlockSeq() != 0 {
		t.Fatalf("fresh watermark: %+v err=%v", wm, err)
	}
	if _, err := ss.GetManifest(ctx, &pb.SnapshotRef{SnapshotId: "nope"}); status.Code(err) != codes.NotFound {
		t.Fatalf("missing manifest: %v", err)
	}
}

func TestNotLeaderMapping(t *testing.T) {
	f := fsm.New(fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	conn, _, _ := startServer(t, f, false) // follower
	mem := pb.NewMembershipClient(conn)
	_, err := mem.RegisterNode(context.Background(), &pb.NodeRegistration{NodeId: "v1"})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		t.Fatalf("want FAILED_PRECONDITION, got %v", err)
	}
	found := false
	for _, d := range st.Details() {
		if nl, okd := d.(*pb.NotLeader); okd && nl.GetLeaderAddr() == "leader-addr:7001" {
			found = true
		}
	}
	if !found {
		t.Fatalf("NotLeader detail missing: %v", st.Details())
	}
	// SafeState reads still serve on a follower (bounded staleness, §11.3)
	ss := pb.NewSafeStateClient(conn)
	if _, err := ss.GetSafeWatermark(context.Background(), &pb.GetSafeWatermarkRequest{}); err != nil {
		t.Fatalf("follower read: %v", err)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./server/ 2>&1 | head -5` — Expected: package missing.

- [ ] **Step 3: Implement**

`server/server.go`:

```go
// Package server implements the six Arbiter gRPC services (design §6):
// plaintext in v1 (trusted network; TLS is P3), leader-only writes with
// NotLeader redirection, follower-served SafeState reads. The server never
// signs anything — the authority.Signer belongs to the orchestrator.
package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/raftnode"
	"github.com/sentioxyz/arbiter/wire"
)

type Config struct {
	ApplyTimeout    time.Duration
	MaxStatementAge time.Duration
}

type Deps struct {
	Node        raftnode.ConsensusNode
	FSM         *fsm.FSM
	LeaderAddr  func() string
	Cfg         Config
	Logger      *slog.Logger
	OnSubscribe func()
}

// Server hosts all six services. The registries (Task 14) hold dispatch
// streams; OnLeadershipLost tears them down so clients re-home.
type Server struct {
	d         Deps
	verifiers *verifierRegistry
	snodes    *snodeRegistry
}

func New(d Deps) *Server {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &Server{d: d, verifiers: newVerifierRegistry(), snodes: newSNodeRegistry()}
}

// RegisterAll registers every service implementation on g.
func (s *Server) RegisterAll(g *grpc.Server) {
	pb.RegisterArbiterIngressServer(g, &ingressService{s: s})
	pb.RegisterSourceClaimsServer(g, &claimsService{s: s})
	pb.RegisterVerifierGatewayServer(g, &verifierGatewayService{s: s})
	pb.RegisterPromotionGatewayServer(g, &promotionGatewayService{s: s})
	pb.RegisterSafeStateServer(g, &safeStateService{s: s})
	pb.RegisterMembershipServer(g, &membershipService{s: s})
}

// OnLeadershipLost closes all dispatch streams.
func (s *Server) OnLeadershipLost() {
	s.verifiers.closeAll()
	s.snodes.closeAll()
}

func notLeaderErr(addr string) error {
	st := status.New(codes.FailedPrecondition, "not the leader")
	if d, err := st.WithDetails(&pb.NotLeader{LeaderAddr: addr}); err == nil {
		st = d
	}
	return st.Err()
}

// propose encodes, applies, and maps errors (design §6): NotLeader →
// FAILED_PRECONDITION + detail; other apply errors → UNAVAILABLE;
// fsm.Rejected → INVALID_ARGUMENT with the reason.
func (s *Server) propose(_ context.Context, cmd wire.Command) (any, error) {
	b, err := wire.Encode(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode: %v", err)
	}
	fut := s.d.Node.Apply(b, s.d.Cfg.ApplyTimeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
			return nil, notLeaderErr(s.d.LeaderAddr())
		}
		return nil, status.Errorf(codes.Unavailable, "apply: %v", err)
	}
	res := fut.Response()
	if r, ok := res.(fsm.Rejected); ok {
		return nil, status.Error(codes.InvalidArgument, r.Reason)
	}
	return res, nil
}
```

`server/safestate.go`:

```go
package server

import (
	"context"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter/wire"
)

// safeStateService serves reads from LOCAL fsm state on any node (bounded
// staleness, §11.3); it performs no proposals.
type safeStateService struct {
	pb.UnimplementedSafeStateServer
	s *Server
}

func (x *safeStateService) GetSafeWatermark(context.Context, *pb.GetSafeWatermarkRequest) (*pb.SafeWatermark, error) {
	wm := x.s.d.FSM.SafeWatermarkView()
	return &pb.SafeWatermark{SnapshotId: wm.SnapshotID, SafeBlockSeq: wm.SafeBlockSeq, ManifestRoot: wm.ManifestRoot}, nil
}

func (x *safeStateService) GetManifest(_ context.Context, ref *pb.SnapshotRef) (*pb.SafeSnapshotManifest, error) {
	m, ok := x.s.d.FSM.ManifestByID(ref.GetSnapshotId())
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown snapshot id")
	}
	return wire.ManifestToPB(*m), nil
}

func (x *safeStateService) GetManifestByBlock(_ context.Context, ref *pb.BlockRef) (*pb.SafeSnapshotManifest, error) {
	m, ok := x.s.d.FSM.ManifestBySafeBlock(ref.GetSafeBlockSeq())
	if !ok {
		return nil, status.Error(codes.NotFound, "no manifest at that safe block")
	}
	return wire.ManifestToPB(*m), nil
}
```

`server/membership.go`:

```go
package server

import (
	"context"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter/wire"
)

// membershipService is UNAUTHENTICATED in v1 (trusted network; design §1).
type membershipService struct {
	pb.UnimplementedMembershipServer
	s *Server
}

func (x *membershipService) RegisterNode(ctx context.Context, r *pb.NodeRegistration) (*pb.Ack, error) {
	if _, err := x.s.propose(ctx, wire.Command{RegisterNode: &wire.RegisterNode{Registration: wire.RegistrationFromPB(r)}}); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}

func (x *membershipService) MarkActive(ctx context.Context, r *pb.NodeRef) (*pb.Ack, error) {
	if _, err := x.s.propose(ctx, wire.Command{MarkActive: &wire.MarkActive{NodeID: r.GetNodeId()}}); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}
```

Task-12 placeholders (replaced by Tasks 13-14), in `server/placeholders.go`:

```go
package server

import pb "github.com/sentioxyz/arbiter-proto/gen/pb"

// Placeholder service structs: Tasks 13-14 add the method bodies (until
// then gRPC answers UNIMPLEMENTED via the embedded Unimplemented servers).
type ingressService struct {
	pb.UnimplementedArbiterIngressServer
	s *Server
}

type claimsService struct {
	pb.UnimplementedSourceClaimsServer
	s *Server
}

type verifierGatewayService struct {
	pb.UnimplementedVerifierGatewayServer
	s *Server
}

type promotionGatewayService struct {
	pb.UnimplementedPromotionGatewayServer
	s *Server
}

// Registry placeholders: Task 14 replaces these with the real
// stream registries.
type verifierRegistry struct{}
type snodeRegistry struct{}

func newVerifierRegistry() *verifierRegistry { return &verifierRegistry{} }
func newSNodeRegistry() *snodeRegistry       { return &snodeRegistry{} }
func (r *verifierRegistry) closeAll()        {}
func (r *snodeRegistry) closeAll()           {}
```

Run `go mod tidy` (grpc becomes a direct dependency).

- [ ] **Step 4: Run + commit**

Run: `go mod tidy && go test ./server/ -v 2>&1 | tail -8 && go test ./... 2>&1 | tail -3` — Expected: PASS.

```bash
git add server/ go.mod go.sum
git commit -m "feat(server): grpc core with NotLeader mapping, SafeState + Membership

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 13: server ingress + source claims

**Files:**
- Create: `server/ingress.go`, `server/claims.go`, `server/ingress_test.go`
- Modify: `server/placeholders.go` (remove the ingress/claims placeholder structs — they move to their own files)

**Interfaces:**
- Consumes: Task 12 core (`startServer`, `propose`), `fsm.StatementAck`, `wire.EnvelopeFromPB`/`RCFromPB`.
- Produces: `SubmitStatement` with the ingress freshness gate (the P1a-deferred wall-clock edge) + the §11.3 idempotent re-ack; `RegisterResultClaim`.

Semantics pinned by the design §6:
- Freshness gate: best-effort parse of the user_jws payload `iat` WITHOUT signature verification (Apply re-verifies deterministically). Reject `now-iat > MaxStatementAge + skew(5s)` or `iat-now > skew` with `INVALID_ARGUMENT`. Unparseable JWS → skip the gate; Apply produces the deterministic admission rejection.
- Idempotent re-ack: on `DUPLICATE_CLIENT_SEQ`, consult `StatementAck(flatID)`; stored envelope field-equal to the request's (account-normalized) Go form → return the ORIGINAL `ACCEPTED` ack with the assigned seq; different content → return the duplicate code.

- [ ] **Step 1: Write the failing tests**

Create `server/ingress_test.go`:

```go
package server

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/wire"
)

// signUserJWSAt mirrors the fsm test helper with a controllable iat.
func signUserJWSAt(t *testing.T, key *ecdsa.PrivateKey, sql string, iat int64) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256K","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"iat":%d,"qhash":"0x%x"}`, iat, crypto.Keccak256([]byte(sql)))))
	signingInput := header + "." + payload
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func pbEnvelope(t *testing.T, key *ecdsa.PrivateKey, account string, seq uint64, iat int64) *pb.StatementEnvelopeV2 {
	t.Helper()
	sql := fmt.Sprintf("INSERT INTO db.t VALUES (%d)", seq)
	return &pb.StatementEnvelopeV2{
		StatementId:   &pb.StatementID{ClientAccount: account, ClientSeq: seq, ClientNonce: "n"},
		StatementKind: pb.StatementKind_STATEMENT_KIND_INSERT,
		Sql:           sql, SqlHash: replay.DigestString(sql), TargetTableId: "db.t",
		UserJws: signUserJWSAt(t, key, sql, iat),
	}
}

// mustApplyDirect drives a command through the fake node (test setup path).
func mustApplyDirect(t *testing.T, node *fakeNode, cmd wire.Command) {
	t.Helper()
	b, err := wire.Encode(cmd)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	res := node.Apply(b, time.Second)
	if err := res.Error(); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if r, ok := res.Response().(fsm.Rejected); ok {
		t.Fatalf("setup rejected: %s", r.Reason)
	}
}

func newIngressFixture(t *testing.T) (*ecdsa.PrivateKey, string, pb.ArbiterIngressClient) {
	t.Helper()
	f := fsm.New(fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	conn, _, node := startServer(t, f, true)
	pk := make([]byte, 32)
	pk[0] = 's'
	mustApplyDirect(t, node, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, Ed25519Pubkey: pk}}})
	mustApplyDirect(t, node, wire.Command{MarkActive: &wire.MarkActive{NodeID: "s1"}})
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	account := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	return key, account, pb.NewArbiterIngressClient(conn)
}

func TestSubmitStatement_AcceptAndIdempotentReack(t *testing.T) {
	key, account, client := newIngressFixture(t)
	ctx := context.Background()
	env := pbEnvelope(t, key, account, 1, time.Now().Unix())
	ack1, err := client.SubmitStatement(ctx, env)
	if err != nil || ack1.GetCode() != pb.AdmissionCode_ADMISSION_CODE_ACCEPTED || ack1.GetStatementSeq() != 1 {
		t.Fatalf("first submit: %+v err=%v", ack1, err)
	}
	// byte-identical duplicate → ORIGINAL accepted ack (§11.3 contract)
	ack2, err := client.SubmitStatement(ctx, env)
	if err != nil || ack2.GetCode() != pb.AdmissionCode_ADMISSION_CODE_ACCEPTED || ack2.GetStatementSeq() != 1 {
		t.Fatalf("duplicate re-ack: %+v err=%v", ack2, err)
	}
	// same (account, seq), DIFFERENT content → duplicate code
	changed := pbEnvelope(t, key, account, 1, time.Now().Unix())
	changed.Sql = changed.GetSql() + " -- changed"
	changed.SqlHash = replay.DigestString(changed.GetSql())
	changed.UserJws = signUserJWSAt(t, key, changed.GetSql(), time.Now().Unix())
	ack3, err := client.SubmitStatement(ctx, changed)
	if err != nil || ack3.GetCode() != pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ {
		t.Fatalf("conflicting duplicate: %+v err=%v", ack3, err)
	}
}

func TestSubmitStatement_FreshnessGate(t *testing.T) {
	key, account, client := newIngressFixture(t)
	stale := pbEnvelope(t, key, account, 2, time.Now().Add(-time.Hour).Unix())
	if _, err := client.SubmitStatement(context.Background(), stale); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("stale iat must be edge-rejected: %v", err)
	}
	future := pbEnvelope(t, key, account, 2, time.Now().Add(time.Hour).Unix())
	if _, err := client.SubmitStatement(context.Background(), future); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("future iat must be edge-rejected: %v", err)
	}
	// an unparseable JWS bypasses the gate and gets the DETERMINISTIC
	// admission rejection (INVALID_SIGNATURE code, not a transport error)
	garbage := pbEnvelope(t, key, account, 2, time.Now().Unix())
	garbage.UserJws = "garbage"
	ack, err := client.SubmitStatement(context.Background(), garbage)
	if err != nil || ack.GetCode() != pb.AdmissionCode_ADMISSION_CODE_INVALID_SIGNATURE {
		t.Fatalf("garbage jws: %+v err=%v", ack, err)
	}
}

func TestRegisterResultClaim(t *testing.T) {
	key, account, client := newIngressFixture(t)
	_ = key
	// a claim for an unknown statement PARKS (late binding) → Ack
	// (reuse the ingress fixture's server conn via a second client)
	// then a NUL-id claim is a genuine rejection → INVALID_ARGUMENT.
	// Note: newIngressFixture returns only the ingress client; restructure
	// it to also return the conn so this test can build a SourceClaims
	// client — implementer adjusts the fixture signature accordingly.
	_ = account
	_ = client
	t.Skip("enable after fixture returns conn — see Step 3 note")
}
```

Fixture note for Step 3: change `newIngressFixture` to return `(key, account, conn)` and build clients per test; enable `TestRegisterResultClaim` with: park path (`SourceClaims.RegisterResultClaim` with `source_node:"s1"`, unknown statement → Ack, no error) and rejection path (a `CandidateParts` entry with `TableID: "db.t\x00x"` → `INVALID_ARGUMENT`).

- [ ] **Step 2: Run to verify failure**

Run: `go test ./server/ -run 'TestSubmitStatement|TestRegisterResultClaim' 2>&1 | head -5`
Expected: UNIMPLEMENTED failures.

- [ ] **Step 3: Implement `server/ingress.go` + `server/claims.go`**

`server/ingress.go`:

```go
package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/wire"
)

// ingressClockSkew mirrors the authority/admission skew tolerance.
const ingressClockSkew = 5 * time.Second

type ingressService struct {
	pb.UnimplementedArbiterIngressServer
	s *Server
}

// iatOf best-effort-parses the JWS payload iat WITHOUT verifying the
// signature (Apply re-verifies deterministically). ok=false skips the
// gate; Apply then produces the deterministic admission rejection.
func iatOf(jws string) (int64, bool) {
	parts := strings.Split(jws, ".")
	if len(parts) != 3 {
		return 0, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return 0, false
	}
	var p struct {
		Iat int64 `json:"iat"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.Iat == 0 {
		return 0, false
	}
	return p.Iat, true
}

func (x *ingressService) SubmitStatement(ctx context.Context, env *pb.StatementEnvelopeV2) (*pb.SequencedAck, error) {
	goEnv := wire.EnvelopeFromPB(env)
	// The ingress freshness edge (design §6): wall-clock checks live HERE,
	// never in Apply (P1a red line 1).
	if iat, ok := iatOf(goEnv.UserJWS); ok {
		age := time.Since(time.Unix(iat, 0))
		if age > x.s.d.Cfg.MaxStatementAge+ingressClockSkew {
			return nil, status.Errorf(codes.InvalidArgument, "statement too old: iat age %s exceeds %s", age.Truncate(time.Second), x.s.d.Cfg.MaxStatementAge)
		}
		if age < -ingressClockSkew {
			return nil, status.Error(codes.InvalidArgument, "statement iat is in the future")
		}
	}
	res, err := x.s.propose(ctx, wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: goEnv}})
	if err != nil {
		return nil, err
	}
	sr, ok := res.(fsm.SubmitResult)
	if !ok {
		return nil, status.Errorf(codes.Internal, "unexpected apply result %T", res)
	}
	if sr.Code == arbiter.AdmissionCodeDuplicateClientSeq {
		// §11.3 idempotency: a field-identical duplicate returns the
		// ORIGINAL result, never an error.
		normalized := goEnv
		normalized.StatementID.ClientAccount = strings.ToLower(goEnv.StatementID.ClientAccount)
		if info, found := x.s.d.FSM.StatementAck(normalized.StatementID.Flat()); found && info.Env == normalized {
			return &pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_ACCEPTED, StatementSeq: info.Seq}, nil
		}
	}
	return &pb.SequencedAck{Code: pb.AdmissionCode(sr.Code), StatementSeq: sr.StatementSeq, Message: sr.Message}, nil
}
```

`server/claims.go`:

```go
package server

import (
	"context"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter/wire"
)

type claimsService struct {
	pb.UnimplementedSourceClaimsServer
	s *Server
}

func (x *claimsService) RegisterResultClaim(ctx context.Context, rc *pb.RCRecord) (*pb.Ack, error) {
	// FSM-absorbed duplicates and parked late-bindings return Applied →
	// Ack; genuine rejections (source mismatch, NUL ids, conflicting
	// first-wins) surface as INVALID_ARGUMENT via propose's mapping.
	if _, err := x.s.propose(ctx, wire.Command{RegisterRC: &wire.RegisterRC{RC: wire.RCFromPB(rc)}}); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}
```

Remove the two placeholder structs from `server/placeholders.go`; enable `TestRegisterResultClaim` per the fixture note.

- [ ] **Step 4: Run + commit**

Run: `go test ./server/ -race -v 2>&1 | tail -10 && go test ./... 2>&1 | tail -3` — Expected: PASS.

```bash
git add server/
git commit -m "feat(server): ingress freshness edge + idempotent re-ack; source claims

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 14: server gateways — subscribe registries + evidence/ack RPCs

**Files:**
- Create: `server/registry.go`, `server/gateway.go`, `server/gateway_test.go`
- Modify: `server/server.go` (registry accessors), `server/placeholders.go` (delete entirely — the last two placeholder structs move to gateway.go, the registry placeholders are replaced by registry.go)

**Interfaces:**
- Consumes: Tasks 12-13; the orchestrator directory interfaces (STRUCTURAL match only — server does not import orchestrator).
- Produces: real `*verifierRegistry`/`*snodeRegistry` (generic `streamRegistry[T]` instantiations with `Send(id, msg) error`, `Connected() []string`, `register`, `unregister`, `closeAll`); `func (s *Server) VerifierDirectory() *verifierRegistry` + `func (s *Server) SNodeDirectory() *snodeRegistry` (cmd passes them into `orchestrator.Deps` — they satisfy the orchestrator interfaces structurally); leader-gated `SubscribeVerifierDispatch`/`SubscribePromotions`; `SubmitAttestation`/`SubmitByteSideScan`/`AckPromotion`/`AckCleanup`; `Deps.OnSubscribe` fired on every new subscription.

Registry semantics (design §6): `register(id)` returns a fresh buffered channel (cap 16), closing/replacing any previous stream for the same id; `Send` errors on absent subscriber or full buffer (slow consumer — the orchestrator's retry ticker re-covers); `closeAll` closes every channel and empties the map (leadership loss); stream handlers exit when the channel closes or the client context ends.

- [ ] **Step 1: Write the failing tests**

Create `server/gateway_test.go`:

```go
package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter/fsm"
)

func TestVerifierSubscribe_PushAndLeadershipTeardown(t *testing.T) {
	f := fsm.New(fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	conn, s, _ := startServer(t, f, true)
	client := pb.NewVerifierGatewayClient(conn)
	stream, err := client.SubscribeVerifierDispatch(context.Background(), &pb.VerifierHello{ReplicaId: "v1"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	// registration is async with the RPC returning: poll bounded
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ids := s.VerifierDirectory().Connected(); len(ids) == 1 && ids[0] == "v1" {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ids := s.VerifierDirectory().Connected(); len(ids) != 1 {
		t.Fatalf("connected: %v", ids)
	}
	if err := s.VerifierDirectory().Send("v1", &pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ByteSideScan{ByteSideScan: &pb.ByteSideScanRequest{BlockSeq: 7}}}); err != nil {
		t.Fatalf("send: %v", err)
	}
	msg, err := stream.Recv()
	if err != nil || msg.GetByteSideScan().GetBlockSeq() != 7 {
		t.Fatalf("recv: %+v err=%v", msg, err)
	}
	s.OnLeadershipLost()
	if _, err := stream.Recv(); err == nil {
		t.Fatal("stream must terminate after leadership loss")
	}
	if len(s.VerifierDirectory().Connected()) != 0 {
		t.Fatal("registry must be empty after teardown")
	}
	// Send to a gone subscriber errors
	if err := s.VerifierDirectory().Send("v1", &pb.VerifierDispatch{}); err == nil {
		t.Fatal("send to closed registry must error")
	}
}

func TestSubscribeOnFollowerGetsNotLeader(t *testing.T) {
	f := fsm.New(fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	conn, _, _ := startServer(t, f, false)
	client := pb.NewVerifierGatewayClient(conn)
	stream, err := client.SubscribeVerifierDispatch(context.Background(), &pb.VerifierHello{ReplicaId: "v1"})
	if err == nil {
		_, err = stream.Recv() // server-streaming errors surface on Recv
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("want FAILED_PRECONDITION, got %v", err)
	}
}

func TestOnSubscribeCallbackFires(t *testing.T) {
	f := fsm.New(fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	poked := make(chan struct{}, 4)
	node := &fakeNode{f: f, leader: true}
	s := New(Deps{Node: node, FSM: f, LeaderAddr: func() string { return "x" },
		Cfg: Config{ApplyTimeout: time.Second, MaxStatementAge: time.Minute},
		OnSubscribe: func() { poked <- struct{}{} }})
	// direct handler-level exercise via a bufconn server (reuse startServer
	// wiring is fixture-bound to its own Server; build inline here)
	_ = s
	// Simplest: register through the service path used above — implementer
	// may fold this assertion into TestVerifierSubscribe by constructing
	// startServer with an OnSubscribe hook; keep the assertion: callback
	// fires ≥1 time per new subscription.
	t.Skip("fold into TestVerifierSubscribe via an OnSubscribe-aware fixture — see note")
}

func TestGatewayProposalRPCs(t *testing.T) {
	f := fsm.New(fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	conn, _, _ := startServer(t, f, true)
	vg := pb.NewVerifierGatewayClient(conn)
	// garbage attestation against an unknown block → INVALID_ARGUMENT
	if _, err := vg.SubmitAttestation(context.Background(), &pb.ReplayAttestation{ReplicaId: "v1", Receipt: &pb.ExecutionReceipt{BlockSeq: 42}}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("unknown block attestation: %v", err)
	}
	pg := pb.NewPromotionGatewayClient(conn)
	// unknown-seq cleanup ack is FSM-absorbed as idempotent → Ack
	if _, err := pg.AckCleanup(context.Background(), &pb.CleanupAck{NodeId: "s1", PromotionSeq: 9}); err != nil {
		t.Fatalf("idempotent cleanup ack: %v", err)
	}
}
```

Fixture note for Step 3: extend `startServer` with an optional `OnSubscribe` hook (variadic option or a second constructor) and fold `TestOnSubscribeCallbackFires`' assertion into `TestVerifierSubscribe_PushAndLeadershipTeardown` (assert the callback fired ≥1 time after the subscribe registers). Remove the skipped test once folded.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./server/ -run 'TestVerifierSubscribe|TestSubscribeOnFollower|TestGatewayProposalRPCs' 2>&1 | head -6`
Expected: UNIMPLEMENTED / registry methods missing.

- [ ] **Step 3: Implement**

`server/registry.go`:

```go
package server

import (
	"fmt"
	"sync"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// streamRegistry maps subscriber id → dispatch channel. register replaces
// (and closes) a previous stream for the same id; Send errors on absent
// subscriber or full buffer (slow consumer — the orchestrator retry ticker
// re-covers); closeAll tears everything down on leadership loss so clients
// re-home via NotLeader.
type streamRegistry[T any] struct {
	mu      sync.Mutex
	streams map[string]chan T
}

func newStreamRegistry[T any]() *streamRegistry[T] {
	return &streamRegistry[T]{streams: map[string]chan T{}}
}

func (r *streamRegistry[T]) register(id string) chan T {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.streams[id]; ok {
		close(old)
	}
	ch := make(chan T, 16)
	r.streams[id] = ch
	return ch
}

func (r *streamRegistry[T]) unregister(id string, ch chan T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if cur, ok := r.streams[id]; ok && cur == ch {
		delete(r.streams, id)
		close(cur)
	}
}

func (r *streamRegistry[T]) Send(id string, msg T) error {
	r.mu.Lock()
	ch, ok := r.streams[id]
	r.mu.Unlock()
	if !ok {
		return fmt.Errorf("subscriber %q not connected", id)
	}
	select {
	case ch <- msg:
		return nil
	default:
		return fmt.Errorf("subscriber %q send buffer full", id)
	}
}

func (r *streamRegistry[T]) Connected() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.streams))
	for id := range r.streams {
		out = append(out, id)
	}
	return out
}

func (r *streamRegistry[T]) closeAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, ch := range r.streams {
		close(ch)
		delete(r.streams, id)
	}
}

type verifierRegistry = streamRegistry[*pb.VerifierDispatch]
type snodeRegistry = streamRegistry[*pb.PromotionCommand]

func newVerifierRegistry() *verifierRegistry { return newStreamRegistry[*pb.VerifierDispatch]() }
func newSNodeRegistry() *snodeRegistry       { return newStreamRegistry[*pb.PromotionCommand]() }
```

`server/server.go` additions:

```go
// VerifierDirectory / SNodeDirectory expose the registries for the
// orchestrator's Deps (structural interface match; no package cycle —
// cmd wires them together).
func (s *Server) VerifierDirectory() *verifierRegistry { return s.verifiers }
func (s *Server) SNodeDirectory() *snodeRegistry       { return s.snodes }
```

`server/gateway.go` (unregister-safe double-close note: `unregister` only closes when the registry still owns THIS channel — after `closeAll`, the deferred `unregister(id, ch)` finds `ok=false` or a different channel and does nothing):

```go
package server

import (
	"context"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter/wire"
)

type verifierGatewayService struct {
	pb.UnimplementedVerifierGatewayServer
	s *Server
}

func (x *verifierGatewayService) SubscribeVerifierDispatch(hello *pb.VerifierHello, stream pb.VerifierGateway_SubscribeVerifierDispatchServer) error {
	if err := x.s.d.Node.VerifyLeader(); err != nil {
		return notLeaderErr(x.s.d.LeaderAddr())
	}
	id := hello.GetReplicaId()
	ch := x.s.verifiers.register(id)
	defer x.s.verifiers.unregister(id, ch)
	if x.s.d.OnSubscribe != nil {
		x.s.d.OnSubscribe()
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case msg, ok := <-ch:
			if !ok {
				return notLeaderErr(x.s.d.LeaderAddr()) // leadership lost: re-home
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (x *verifierGatewayService) SubmitAttestation(ctx context.Context, att *pb.ReplayAttestation) (*pb.Ack, error) {
	if _, err := x.s.propose(ctx, wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: wire.AttestationFromPB(att)}}); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}

func (x *verifierGatewayService) SubmitByteSideScan(ctx context.Context, scan *pb.ByteSideScanMsg) (*pb.Ack, error) {
	if _, err := x.s.propose(ctx, wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: wire.ScanFromPB(scan)}}); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}

type promotionGatewayService struct {
	pb.UnimplementedPromotionGatewayServer
	s *Server
}

func (x *promotionGatewayService) SubscribePromotions(hello *pb.SNodeHello, stream pb.PromotionGateway_SubscribePromotionsServer) error {
	if err := x.s.d.Node.VerifyLeader(); err != nil {
		return notLeaderErr(x.s.d.LeaderAddr())
	}
	id := hello.GetNodeId()
	ch := x.s.snodes.register(id)
	defer x.s.snodes.unregister(id, ch)
	if x.s.d.OnSubscribe != nil {
		x.s.d.OnSubscribe()
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case msg, ok := <-ch:
			if !ok {
				return notLeaderErr(x.s.d.LeaderAddr())
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (x *promotionGatewayService) AckPromotion(ctx context.Context, ack *pb.PromotionAck) (*pb.Ack, error) {
	if _, err := x.s.propose(ctx, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: wire.PromotionAckFromPB(ack)}}); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}

func (x *promotionGatewayService) AckCleanup(ctx context.Context, ack *pb.CleanupAck) (*pb.Ack, error) {
	if _, err := x.s.propose(ctx, wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: wire.CleanupAckFromPB(ack)}}); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}
```

Delete `server/placeholders.go` (all four service structs now live in ingress.go/claims.go/gateway.go; registries in registry.go).

- [ ] **Step 4: Run + commit**

Run: `go test ./server/ -race -v 2>&1 | tail -10 && go test ./... 2>&1 | tail -3` — Expected: PASS.

```bash
git add server/
git commit -m "feat(server): leader-gated dispatch streams with teardown-on-loss registries

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 15: cmd/arbiter — assembly, leadership watcher, smoke test

**Files:**
- Create: `cmd/arbiter/main.go`, `cmd/arbiter/main_test.go`, `configs/local.yaml`
- Modify: `go.mod` (+`github.com/hashicorp/raft-boltdb/v2`), `README.md` (P1b section)

**Interfaces:**
- Consumes: everything (config, fsm, raftnode, server, orchestrator, anchor, authority).
- Produces: the binary; `run(ctx, cfg, logger) (*app, error)` + `(*app).Close()` (testable assembly; `main()` stays thin); `configs/local.yaml` single-node bootstrap sample.

Design notes binding this task:
- **Logging refinement:** the binary uses stdlib `slog` (TextHandler + `slog.LevelVar` from `-log-level`) rather than importing housegate `pkg/log` — pkg/log's console handler is housegate-binary-local; the spec's intent (structured logging conventions) is met with the stdlib. Recorded as a plan-level refinement of the spec's "imports housegate pkg/log" line.
- **NotLeader address semantics (v1):** `LeaderAddr` is wired from `node.Raw().LeaderWithID()`'s **ServerID** (the raft node id), NOT a raft/grpc address — cluster nodes know each other's grpc addresses via their own configuration, so clients resolve `leader_addr` (= node id) through their peer map. Documented in main.go and exercised by Task 16's fakes. A richer discovery story is P1c client-SDK work.
- **Leadership watcher:** `LeaderCh` gain → spawn `orch.Run(leaderCtx)`; loss → cancel + `srv.OnLeadershipLost()`. The channel may drop notifications (P1a caveat) — `Run`'s entry `VerifyLeader` and the in-loop guards keep correctness; the watcher is a convenience switch.
- **Shutdown order:** signal → `grpc.GracefulStop` (bounded by 5s then `Stop`) → leader cancel → `raft.Shutdown` → boltdb close.

- [ ] **Step 1: Write the failing smoke test**

Create `cmd/arbiter/main_test.go`:

```go
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter/config"
)

// freePort grabs an ephemeral port (close-then-reuse; adequate for a smoke test).
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestRun_SingleNodeBootsAndShutsDown(t *testing.T) {
	raftPort, grpcPort := freePort(t), freePort(t)
	dir := t.TempDir()
	cfg := config.Config{
		NodeID:     "arb-1",
		GRPCListen: fmt.Sprintf("127.0.0.1:%d", grpcPort),
	}
	cfg.Raft.Listen = fmt.Sprintf("127.0.0.1:%d", raftPort)
	cfg.Raft.Advertise = cfg.Raft.Listen
	cfg.Raft.DataDir = filepath.Join(dir, "raft")
	cfg.Raft.Bootstrap = true
	cfg.Raft.Peers = []config.RaftPeer{{ID: "arb-1", Addr: cfg.Raft.Listen}}
	cfg.Genesis.SchemaSnapshotID = "schema-genesis"
	cfg.Genesis.ExecutorProfileID = "housegate-replay-mvp-v0"
	applyTestDefaults(&cfg) // helper below: fast raft timeouts + required durations

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	app, err := run(ctx, cfg, slog.Default())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	defer app.Close()

	// single bootstrap node must become leader
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if app.node.VerifyLeader() == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("single-node cluster never elected a leader")
}

// applyTestDefaults fills required durations with fast test values.
func applyTestDefaults(c *config.Config) {
	set := func(d *config.Duration, v time.Duration) { d.Duration = v }
	set(&c.Raft.ElectionTimeout, 150*time.Millisecond)
	set(&c.Raft.HeartbeatTimeout, 150*time.Millisecond)
	set(&c.Raft.LeaderLeaseTimeout, 100*time.Millisecond)
	set(&c.Raft.CommitTimeout, 10*time.Millisecond)
	set(&c.Raft.SnapshotInterval, 10*time.Second)
	c.Raft.SnapshotThreshold = 8192
	c.Raft.TrailingLogs = 10240
	set(&c.ApplyTimeout, 2*time.Second)
	c.Seal.MaxStatements = 4
	set(&c.Seal.MaxAge, 200*time.Millisecond)
	set(&c.Ingress.MaxStatementAge, 5*time.Minute)
	set(&c.Dispatch.RetryInterval, 100*time.Millisecond)
	c.Anchor.Backend = "local"
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/arbiter/ 2>&1 | head -4` — Expected: package missing / `run` undefined.

- [ ] **Step 3: Implement `cmd/arbiter/main.go`**

```go
// Command arbiter runs one Sentio Arbiter node: raft consensus + the six
// gRPC services + (on the leader) the orchestrator (design §8).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"google.golang.org/grpc"

	"github.com/sentioxyz/arbiter/anchor"
	"github.com/sentioxyz/arbiter/authority"
	"github.com/sentioxyz/arbiter/config"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/orchestrator"
	"github.com/sentioxyz/arbiter/raftnode"
	"github.com/sentioxyz/arbiter/server"
)

func main() {
	var (
		cfgPath  = flag.String("config", "", "path to arbiter yaml config (required)")
		logLevel = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()
	lvl := new(slog.LevelVar)
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		fmt.Fprintf(os.Stderr, "bad -log-level: %v\n", err)
		os.Exit(2)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "-config is required")
		os.Exit(2)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Error("config", "err", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	app, err := run(ctx, cfg, logger)
	if err != nil {
		logger.Error("startup", "err", err)
		os.Exit(1)
	}
	<-ctx.Done()
	logger.Info("shutting down")
	app.Close()
}

// app owns every started component; Close tears down in reverse order.
type app struct {
	logger     *slog.Logger
	grpcServer *grpc.Server
	leaderStop context.CancelFunc
	node       *raftnode.Node
	bolt       *raftboltdb.BoltStore
}

// run assembles one node (design §8 order). It returns once the gRPC
// listener is serving and the leadership watcher is installed.
func run(ctx context.Context, cfg config.Config, logger *slog.Logger) (*app, error) {
	// fsm with the event channel (leader-local wake hints)
	notify := make(chan fsm.Event, 1024)
	f := fsm.NewWithNotify(fsm.Params{
		SchemaSnapshotID:   cfg.Genesis.SchemaSnapshotID,
		ExecutorProfileID:  cfg.Genesis.ExecutorProfileID,
		AuthorityAddresses: cfg.Authority.AllowedAddresses,
	}, notify)

	// raft storage + transport
	if err := os.MkdirAll(cfg.Raft.DataDir, 0o750); err != nil {
		return nil, fmt.Errorf("data dir: %w", err)
	}
	bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.Raft.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("bolt store: %w", err)
	}
	snaps, err := raft.NewFileSnapshotStore(cfg.Raft.DataDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}
	advAddr, err := net.ResolveTCPAddr("tcp", cfg.Raft.Advertise)
	if err != nil {
		return nil, fmt.Errorf("advertise addr: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.Raft.Listen, advAddr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("raft transport: %w", err)
	}
	rcfg := raft.DefaultConfig()
	rcfg.ElectionTimeout = cfg.Raft.ElectionTimeout.Duration
	rcfg.HeartbeatTimeout = cfg.Raft.HeartbeatTimeout.Duration
	rcfg.LeaderLeaseTimeout = cfg.Raft.LeaderLeaseTimeout.Duration
	rcfg.CommitTimeout = cfg.Raft.CommitTimeout.Duration
	rcfg.SnapshotInterval = cfg.Raft.SnapshotInterval.Duration
	rcfg.SnapshotThreshold = cfg.Raft.SnapshotThreshold
	rcfg.TrailingLogs = cfg.Raft.TrailingLogs
	node, err := raftnode.New(raftnode.Options{
		NodeID: cfg.NodeID, FSM: f,
		LogStore: bolt, StableStore: bolt, SnapshotStore: snaps, Transport: transport,
		RaftConfig: rcfg,
	})
	if err != nil {
		return nil, err
	}
	if cfg.Raft.Bootstrap {
		servers := make([]raft.Server, 0, len(cfg.Raft.Peers))
		for _, p := range cfg.Raft.Peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Addr)})
		}
		if err := node.Bootstrap(servers); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	// authority signer (leader-side use only; every node holds the key, §10.2)
	var signer *authority.Signer
	if cfg.Authority.PrivateKeyHex != "" {
		signer, err = authority.NewSignerFromHex(cfg.Authority.PrivateKeyHex)
		if err != nil {
			return nil, fmt.Errorf("authority signer: %w", err)
		}
	}

	// server + orchestrator (OnSubscribe is late-bound to the orchestrator)
	var orch *orchestrator.Orchestrator
	srv := server.New(server.Deps{
		Node: node, FSM: f,
		// v1 NotLeader semantics: leader_addr carries the raft ServerID;
		// clients resolve it through their own peer configuration.
		LeaderAddr: func() string {
			_, id := node.Raw().LeaderWithID()
			return string(id)
		},
		Cfg:    server.Config{ApplyTimeout: cfg.ApplyTimeout.Duration, MaxStatementAge: cfg.Ingress.MaxStatementAge.Duration},
		Logger: logger,
		OnSubscribe: func() {
			if orch != nil {
				orch.Poke()
			}
		},
	})
	orch = orchestrator.New(orchestrator.Deps{
		Node: node, FSM: f, Events: notify,
		Anchor: anchor.NewLocal(), Signer: signer,
		Verifiers: srv.VerifierDirectory(), SNodes: srv.SNodeDirectory(),
		Cfg: orchestrator.Config{
			ApplyTimeout:      cfg.ApplyTimeout.Duration,
			SealMaxStatements: cfg.Seal.MaxStatements,
			SealMaxAge:        cfg.Seal.MaxAge.Duration,
			RetryInterval:     cfg.Dispatch.RetryInterval.Duration,
		},
		Logger: logger,
	})

	grpcServer := grpc.NewServer()
	srv.RegisterAll(grpcServer)
	lis, err := net.Listen("tcp", cfg.GRPCListen)
	if err != nil {
		return nil, fmt.Errorf("grpc listen: %w", err)
	}
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			logger.Warn("grpc serve ended", "err", err)
		}
	}()

	a := &app{logger: logger, grpcServer: grpcServer, node: node, bolt: bolt}

	// leadership watcher: gain → orchestrator; loss → cancel + stream teardown
	go func() {
		var cancel context.CancelFunc
		for {
			select {
			case <-ctx.Done():
				if cancel != nil {
					cancel()
				}
				return
			case isLeader, ok := <-node.LeaderCh():
				if !ok {
					return
				}
				if isLeader {
					if cancel != nil {
						cancel()
					}
					lctx, c := context.WithCancel(ctx)
					cancel = c
					a.leaderStop = c
					go func() {
						if err := orch.Run(lctx); err != nil && lctx.Err() == nil {
							logger.Warn("orchestrator exited", "err", err)
						}
					}()
				} else {
					if cancel != nil {
						cancel()
						cancel = nil
					}
					srv.OnLeadershipLost()
				}
			}
		}
	}()
	return a, nil
}

// Close shuts everything down: grpc (graceful with a 5s bound) →
// leader loop → raft → bolt.
func (a *app) Close() {
	done := make(chan struct{})
	go func() {
		a.grpcServer.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		a.grpcServer.Stop()
	}
	if a.leaderStop != nil {
		a.leaderStop()
	}
	if err := a.node.Shutdown(); err != nil {
		a.logger.Warn("raft shutdown", "err", err)
	}
	if err := a.bolt.Close(); err != nil {
		a.logger.Warn("bolt close", "err", err)
	}
}
```

Create `configs/local.yaml` (single-node dev sample mirroring the design §7 block with `bootstrap: true`, one self-peer, `data_dir: ./data/arb-1`, 127.0.0.1 ports, no authority key — promotions skip with a warn until one is set).

README: append a short "Running a local node" section (build, `./arbiter -config configs/local.yaml`, GH_MODULES_TOKEN note unchanged). Run `go mod tidy` (raft-boltdb/v2 lands).

- [ ] **Step 4: Run + commit**

Run: `go mod tidy && go test ./cmd/... -v 2>&1 | tail -6 && go build ./... && go test ./... 2>&1 | tail -3` — Expected: PASS.

```bash
git add cmd/ configs/ README.md go.mod go.sum
git commit -m "feat(cmd): arbiter binary with raft-boltdb assembly and leadership watcher

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 16: in-process integration — full pipeline + kill-the-leader (the P1b flagship)

**Files:**
- Create: `integration/fakes_test.go`, `integration/pipeline_test.go`

**Interfaces:**
- Consumes: `cmd/arbiter`'s `run` is NOT importable (package main) — the integration package builds nodes through the SAME component assembly (config→fsm→raftnode(bolt+TCP)→server→orchestrator) via a local `startNode(t, cfg)` helper that mirrors main.go's `run` (keep them aligned; a drift is a finding). Everything else via gRPC only — the test is black-box past assembly.
- Produces: test-only. Proves design §9's flagship: full §3.5 pipeline over the wire on a 3-node cluster, then leader kill mid-pipeline with the new leader finishing the run (§10.2 machine proof).

Scripted fakes (deterministic conventions shared by fake SNode and fake verifiers — both derive everything from the ReplayJob/gRPC data, no side channel):
- Table/partition fixed: `db.t` / `p0`.
- Per-statement part hash: `lthashHex("row:" + flatStatementID)` (raw 2048B accumulator over one element).
- Block claim root: `replay.DigestString("root:" + lastStatement.StatementID)` — the fake SNode sets it as every RC's `SourceClaimRoot`; fake verifiers recompute it from the job's last statement. (The FSM's check 1 uses the LAST RC only, so uniform per-RC roots with the last-statement convention stay consistent for multi-statement blocks.)
- Partition commitment: fake verifiers fold `base ⊕ Σ lthash("row:"+id_i)` over ALL job statements, where base = all-zero for `PrevSafeSnapshotID == ""`, else the partition root fetched from `SafeState.GetManifest(job.PrevSafeSnapshotID)`.
- ed25519 identity: verifier `v<i>` uses `ed25519.NewKeyFromSeed(seed[0]=i)`; registered via Membership before activation.
- NotLeader re-homing: every fake wraps its calls in `withLeaderRetry` — on `FAILED_PRECONDITION`, extract the `pb.NotLeader` detail (= node id per Task 15), switch to that node's conn (the test holds an id→conn map), re-subscribe streams, retry. Bounded attempts (deadline-driven).

- [ ] **Step 1: Write the fakes**

`integration/fakes_test.go` — complete scripted clients (structure; the implementer fills mechanical bodies with exactly these behaviors):

```go
package integration

// cluster: three startNode(t, cfg) apps with fast raft timeouts (Task 15's
// applyTestDefaults values), fixed free ports, node ids arb-1..arb-3,
// bootstrap on arb-1 listing all three raft peers. conns: map[nodeID]*grpc.ClientConn.
//
// leaderConn(t, cluster) *grpc.ClientConn — probes Membership.MarkActive
// with a throwaway op? NO side effects: probe with SafeState + a zero-cost
// write probe is unavoidable — use VerifierGateway.SubmitAttestation with
// an empty attestation and classify: InvalidArgument ⇒ THIS node is leader
// (the proposal reached the FSM and was domain-rejected); FailedPrecondition
// ⇒ follower (NotLeader). Deterministic, state-free leader discovery.
//
// fakeSNode: RegisterNode(role SNODE)+MarkActive via leader; subscribes
// SubscribePromotions (re-homing on NotLeader); AFTER each ingress ack it
// registers the RC for that statement (parts/root per the conventions);
// on PromotionCommand{Promote} → computes post = base ⊕ Σ promote parts
// (base = promote.BasePartitionRoot or zero) → AckPromotion(applied:true,
// post, SafePartMapping{hash, "safe_"+hash[2:10]}); on {Cleanup} → AckCleanup.
//
// fakeVerifier(id): RegisterNode(role VERIFIER, pubkey)+MarkActive;
// subscribes SubscribeVerifierDispatch (re-homing); on ReplayJob → builds
// the receipt per the conventions, ReceiptHash = receipt.Hash(), Signature
// = hex(ed25519.Sign(key, []byte(hash))) → SubmitAttestation; on
// ByteSideScanRequest → PartScan{claimed==scanned} per part, scan_hash =
// replay.CanonicalDigest(arbiter.DomainByteSideScan, body), sign →
// SubmitByteSideScan.
//
// ingress helper: pbEnvelope equivalent (reuse Task 13's shape) + submit
// through the current leader with retry.
```

(Write the real Go — the comment block above is the CONTRACT; the fakes file is ~250 lines of straightforward client code using `pb.New*Client`, `wire`-free — fakes convert to/from pb directly, exercising the true wire path.)

- [ ] **Step 2: Write the pipeline tests**

`integration/pipeline_test.go`:

```go
package integration

import (
	"testing"
	"time"
)

func TestPipeline_EndToEndOverTheWire(t *testing.T) {
	cl := startCluster(t, 3)
	sn := startFakeSNode(t, cl)
	for i := 1; i <= 3; i++ {
		startFakeVerifier(t, cl, i)
	}
	key, account := newAccount(t)
	submitStatement(t, cl, sn, key, account, 1)
	submitStatement(t, cl, sn, key, account, 2)
	waitWatermark(t, cl, 1, 30*time.Second)      // block sealed → verified → anchored → promoted → published
	assertManifestContentAddressed(t, cl)
	waitCleanupAcked(t, sn, 15*time.Second)
	// every node serves the same watermark (replicated reads)
	assertAllNodesWatermark(t, cl, 1)
}

func TestPipeline_LeaderKillMidFlight(t *testing.T) {
	cl := startCluster(t, 3)
	sn := startFakeSNode(t, cl)
	verifiers := make([]*fakeVerifier, 0, 3)
	for i := 1; i <= 3; i++ {
		verifiers = append(verifiers, startFakeVerifier(t, cl, i))
	}
	key, account := newAccount(t)
	submitStatement(t, cl, sn, key, account, 1)
	// wait until at least one verifier has RECEIVED a replay job — the
	// pipeline is provably mid-flight (sealed + marked + dispatched)
	waitAnyJobReceived(t, verifiers, 15*time.Second)
	killCurrentLeader(t, cl) // app.Close() on the leader node
	// fakes re-home via NotLeader; the NEW leader's §10.2 re-entry scan
	// must finish the run without any resubmission
	waitWatermark(t, cl, 1, 45*time.Second)
	assertAllNodesWatermark(t, cl, 1)
}
```

- [ ] **Step 3: Implement + iterate**

Implement the fakes and helpers; run with `-race`. Timing guidance: fast raft (150ms election), dispatch retry 100ms; every wait is `waitFor`-style bounded polling — no bare sleeps as synchronization. **If the pipeline stalls on a REAL product gap (not fixture bugs), STOP and report BLOCKED with the stall point** — that is a finding for the controller (this test exists to catch exactly那 class).

Run: `go test ./integration/ -race -v -timeout 180s 2>&1 | tail -12` (three times; report flakes) then `go test ./... 2>&1 | tail -3`
Expected: both tests PASS ×3.

- [ ] **Step 4: Commit**

```bash
git add integration/
git commit -m "test(integration): wire-level pipeline + kill-the-leader §10.2 proof

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

## Self-Review Notes (spec coverage)

- Design §1 decisions → anchor seam (T7, local backend), plaintext/no-TLS (T12-14, no TLS code), go-modules unchanged, SafeState local reads (T12), seal count+age only (T9). §2 observation shape → T1 events + T9 scan/tickers + T11 event-loss backstop test. §3 fsm extensions → T1 (events), T2-3 (facade incl. wall-clock note realized as leader-local age in T9), T4 (all four roll-forwards + SafeParts spec-completion for manifest ActiveParts). §4 orchestrator → T9 (lifecycle/re-entry/seal), T10 (mark/evidence/challenge), T11 (anchor/promote/manifest/cleanup + re-sign resend §10.2); AwaitingRC observability-only honored (no handler). §5 anchor → T7. §6 server → T12 (core/NotLeader/SafeState/Membership), T13 (ingress freshness + idempotent re-ack, claims), T14 (registries/streams/teardown; OnSubscribe→Poke), wire exports → T6. §7 config → T8 (defaults = Open Q8/Q2 values verbatim; consensus-param warnings). §8 cmd → T15 (assembly order, leadership watcher, shutdown order, raft-boltdb). §9 testing → per-task tests + T16 flagship (kill-the-leader) + T11 event-loss tripwire + T5 rolled-forward hardening tests. §10 tripwires → VerifyLeader-before-side-effect (T9-11 code), no-signer-in-server (grep in final review), no fsm write APIs beyond Apply/Restore (facade is read-only), no new consensus knobs (T8 has none). §11 follow-ups untouched.
- Plan-level refinements recorded (deliberate, review-visible): SafeParts state addition (spec §3 named the need via ManifestInputs; the ack-time accumulation + RC cross-reference is the plan's realization); cmd logging via stdlib slog instead of importing housegate pkg/log; NotLeader `leader_addr` carries the raft ServerID in v1 (clients resolve via peer config; P1c SDK owns richer discovery); Task 3/4 ManifestInputs split (Task 3 lands WorkSet minus ManifestInputs; Task 4 completes it with SafeParts).
- Type-consistency pass: `fsm.Event`/`EventKind` names match between T1 and T9-11 consumers; `WorkSet` field names match T3 ↔ T10/T11 handlers; `BlockAnchor.Ref` added in T11 and consumed there; directory interfaces (T9) structurally match `streamRegistry` methods (T14: `Send(string, T) error` + `Connected() []string`); `server.Config.MaxStatementAge` consumed in T13; `orchestrator.Config` fields consumed in T9/T15; `config.Config` field names match T15's `run` usage; exported wire names (T6) match T12-14 call sites (`EnvelopeFromPB`, `RCFromPB`, `AttestationFromPB`, `ScanFromPB`, `PromotionAckFromPB`, `CleanupAckFromPB`, `ManifestToPB`, `RegistrationFromPB`).
- Placeholder scan: T9's two `handle*Rows` empty bodies are named replaced-by-task placeholders (deliberately no-op, not rejecting); T12's `placeholders.go` is deleted by T14; T13/T14 fixture notes instruct folding two skipped tests — no "TBD/TODO" anywhere.
- Known execution caveats for implementers: pb oneof wrapper spellings (T6/T14/T16), `raftboltdb.NewBoltStore` vs `New(Options)` in raft-boltdb/v2 (use whichever v2 exports; both existed historically), grpc `SupportPackageIsVersion` drift — follow generated code.
