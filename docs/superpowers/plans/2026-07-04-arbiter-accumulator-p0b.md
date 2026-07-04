# Arbiter statement_id Accumulator (P0b) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the frozen `sentio-spent-ids-v1` accumulator — account-granularity SMT, state-transition rules, non-membership proofs, snapshot serialization, JSON test vectors, and the P0a seam amendment — as the `accumulator` package of `github.com/sentioxyz/arbiter`.

**Architecture:** Per [2026-07-04-arbiter-accumulator-design.md](../specs/2026-07-04-arbiter-accumulator-design.md) (the spec; PR housegate#74). One leaf per account in a depth-256 sparse Merkle tree keyed by `BLAKE3(domainKey ‖ 0x00 ‖ account)`; the leaf value commits `(hi_seq, gap ranges)`. The FSM-held implementation (`SpentIDs`) composes three units: the frozen hash profile, the per-account state machine, and a collapsed-trie SMT. Proof and snapshot layers are deterministic hand-rolled binary.

**Tech Stack:** Go 1.26.3, `github.com/zeebo/blake3` v0.2.4 (the repo family's BLAKE3), stdlib only otherwise. TDD throughout; Go native fuzzing.

## Global Constraints

- **Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter` (module `github.com/sentioxyz/arbiter`), branch `main`, commits local until Task 8 pushes. Conventional commits; English code/comments; gofmt-clean.
- **Spec is the normative reference:** `/Users/uranuswch/Dev/housegate/housegate/docs/superpowers/specs/2026-07-04-arbiter-accumulator-design.md` — §3 encodings, §4 transition rules, §5 proof format, §6 API, §8 governance. On any conflict between this plan and the spec, STOP and report (do not silently pick one).
- **Frozen profile constants (verbatim, spec §3):** `ProfileID = "sentio-spent-ids-v1"`; domains `"sentio-spent-ids-v1:key"` / `":leaf"` / `":node"`; hashing = BLAKE3-256 over `domain ‖ 0x00 ‖ payload`; all integers big-endian; empty leaf = 32×0x00; `D[k+1] = node(D[k], D[k])`; `EmptyRoot = D[256]`; **K = MaxOpenGapRanges = 64**; `client_seq = 0` invalid.
- **Frozen test literals** (computed from the spec encoding, verified 2026-07-04 — a mismatch means the implementation deviates from the spec, never "update the literal"):
  - `EmptyRoot = b4aeabf8356fe3d4381cada7f89521990a39ef5378a7f0e7f6b0184360464120`
  - `D[1] = fbd17a4d881cb837b7794a48c4572856df0d5ccbf145eb20f6b9b5f37f7fdca4`
  - `D[128] = 2a6219b5d97396730095f41a4642527f03614ae21e66bb24ab0c85d3b19e88a1`
  - `key("0x1111111111111111111111111111111111111111") = cdbbcc5a415311a9aa4ddf7dc1daef82cd8634480432e6126610ecf37999f54d`
  - `leafval(that account, hi=5, ranges=[[2,3]]) = 7431082351a4b0303a24a3c87841db4239652df7cb16ac5b6b66d4e65014b3ce`
- **No runtime profile configurability** (spec §8): constants in one block, no constructor/config parameter for domains or K.
- **Dependency budget:** add `github.com/zeebo/blake3@v0.2.4` only. No proto dependency in this package.
- **The P0a seam amendment (Task 4) is the only edit outside new files**; `types.go`'s `Proof []byte` already exists in `accumulator/accumulator.go` — do not redefine it.
- Tree levels count leaf-up: leaf = level 0, root = level 256. Bit `i` of a key = bit `7−(i mod 8)` of byte `i/8` (MSB-first). A node at level `L` branches on key bit `256−L`; equivalently the position bit of a level-ℓ value inside its parent is key bit `255−ℓ`.

## File Structure

```text
accumulator/
  accumulator.go        # P0a seam file — Task 4 amends interface
  types.go              # Task 1: SeqRange, Status, sentinel errors
  profile.go            # Task 1: frozen constants, hash helpers, defaults table
  state.go              # Task 2: accountState transitions + canonical validation
  smt.go                # Task 3: collapsed-trie SMT (set/root/path-siblings)
  spentids.go           # Task 4: SpentIDs (the Accumulator implementation)
  proof.go              # Task 5: proof encode/decode/Prove/Verify
  snapshot.go           # Task 6: Snapshot/Restore
  profile_test.go  state_test.go  smt_test.go  spentids_test.go
  proof_test.go  snapshot_test.go
  vectors_test.go       # Task 7: JSON vector replay + -update generator
  testdata/spent_ids_vectors.json
  property_test.go      # Task 8: reference-model properties
  fuzz_test.go          # Task 8: FuzzVerify
```

---

### Task 1: Frozen profile — constants, hash helpers, defaults table

**Files:**
- Create: `accumulator/types.go`, `accumulator/profile.go`
- Test: `accumulator/profile_test.go`
- Modify: `go.mod` (via `go get github.com/zeebo/blake3@v0.2.4`)

**Interfaces:**
- Consumes: nothing (leaf task).
- Produces: `SeqRange{Start, End uint64}`, `Status` (+ 4 constants), `ErrSeqZero`/`ErrSpentDuplicate`/`ErrGapBudgetExceeded`, `ProfileID`, `MaxOpenGapRanges = 64`, `EmptyRoot() []byte`, and unexported helpers every later task uses verbatim: `hashSize = 32`, `defaults [257][32]byte`, `hashDomain(domain string, payload []byte) [32]byte`, `hashKey(account string) [32]byte`, `leafPreimage(account string, hi uint64, ranges []SeqRange) []byte`, `hashLeafVal(account string, hi uint64, ranges []SeqRange) [32]byte`, `hashNode(l, r [32]byte) [32]byte`, `keyBit(key [32]byte, i int) byte`, `foldFrom(start, key [32]byte, toLevel int) [32]byte`.

- [ ] **Step 1: Write the failing test** (`accumulator/profile_test.go`)

```go
package accumulator

import (
	"bytes"
	"encoding/hex"
	"testing"
)

// Frozen profile literals (plan Global Constraints). Recomputing these
// differently means the profile changed and every recorded spent_ids_root
// is invalidated — never "fix" a literal without a coordinated
// profile-version migration (design §8).

func TestFrozenProfileLiterals(t *testing.T) {
	if got := hex.EncodeToString(EmptyRoot()); got != "b4aeabf8356fe3d4381cada7f89521990a39ef5378a7f0e7f6b0184360464120" {
		t.Fatalf("EmptyRoot: got %s", got)
	}
	if got := hex.EncodeToString(defaults[1][:]); got != "fbd17a4d881cb837b7794a48c4572856df0d5ccbf145eb20f6b9b5f37f7fdca4" {
		t.Fatalf("D[1]: got %s", got)
	}
	if got := hex.EncodeToString(defaults[128][:]); got != "2a6219b5d97396730095f41a4642527f03614ae21e66bb24ab0c85d3b19e88a1" {
		t.Fatalf("D[128]: got %s", got)
	}
	const acct = "0x1111111111111111111111111111111111111111"
	k := hashKey(acct)
	if got := hex.EncodeToString(k[:]); got != "cdbbcc5a415311a9aa4ddf7dc1daef82cd8634480432e6126610ecf37999f54d" {
		t.Fatalf("key(acct): got %s", got)
	}
	lv := hashLeafVal(acct, 5, []SeqRange{{Start: 2, End: 3}})
	if got := hex.EncodeToString(lv[:]); got != "7431082351a4b0303a24a3c87841db4239652df7cb16ac5b6b66d4e65014b3ce" {
		t.Fatalf("leafval: got %s", got)
	}
}

func TestKeyBitIsMSBFirst(t *testing.T) {
	var k [hashSize]byte
	k[0] = 0x80
	k[31] = 0x01
	if keyBit(k, 0) != 1 || keyBit(k, 1) != 0 {
		t.Fatal("bit 0 must be the MSB of byte 0")
	}
	if keyBit(k, 255) != 1 || keyBit(k, 254) != 0 {
		t.Fatal("bit 255 must be the LSB of byte 31")
	}
}

func TestFoldFromEmptyLeafReachesEmptyRoot(t *testing.T) {
	// Folding the empty leaf up any key's path with default siblings must
	// reproduce D[256] — the fold and the defaults table must agree.
	key := hashKey("0xabc")
	h := foldFrom(defaults[0], key, 256)
	if h != defaults[256] {
		t.Fatal("foldFrom(empty leaf) != EmptyRoot")
	}
}

func TestLeafPreimageLayout(t *testing.T) {
	p := leafPreimage("ab", 5, []SeqRange{{Start: 2, End: 3}})
	want := []byte{
		0x00, 0x02, 'a', 'b',
		0, 0, 0, 0, 0, 0, 0, 5,
		0, 0, 0, 1,
		0, 0, 0, 0, 0, 0, 0, 2,
		0, 0, 0, 0, 0, 0, 0, 3,
	}
	if !bytes.Equal(p, want) {
		t.Fatalf("preimage layout: got %x want %x", p, want)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/uranuswch/src/sentio_xyz/arbiter && go test ./accumulator/ -run 'TestFrozenProfileLiterals|TestKeyBit|TestFoldFrom|TestLeafPreimage' -v`
Expected: FAIL — compile errors (`undefined: EmptyRoot`, `undefined: defaults`, …).

- [ ] **Step 3: Write `accumulator/types.go`**

```go
package accumulator

import "errors"

// SeqRange is a closed range [Start, End] of unspent client_seq values (an
// open gap in an account's spent set).
type SeqRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

// Status classifies a coordinate against the committed spent state
// (design §4); the read-only admission primitive for the P1a FSM.
type Status uint8

const (
	// StatusFresh: seq is beyond the account's high-water mark (or the
	// account is unseen) — acceptable via the fast path.
	StatusFresh Status = iota + 1
	// StatusGapFillable: seq lies inside an open gap range — acceptable.
	StatusGapFillable
	// StatusSpentDuplicate: the coordinate is already spent (client bug).
	StatusSpentDuplicate
	// StatusSeqZero: client_seq 0 is invalid by profile rule.
	StatusSeqZero
)

var (
	// ErrSeqZero rejects the invalid client_seq 0 (design §3: a
	// strictly-increasing counter starts at 1; hi=0 means "nothing spent").
	ErrSeqZero = errors.New("accumulator: client_seq 0 is invalid")
	// ErrSpentDuplicate rejects a coordinate that is already spent.
	ErrSpentDuplicate = errors.New("accumulator: coordinate already spent")
	// ErrGapBudgetExceeded rejects any operation whose result would hold
	// more than MaxOpenGapRanges open ranges for the account (design §4).
	ErrGapBudgetExceeded = errors.New("accumulator: open gap-range budget exceeded")
)
```

- [ ] **Step 4: Write `accumulator/profile.go`**

```go
package accumulator

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
)

// The frozen sentio-spent-ids-v1 profile (design §3, §8). These constants
// are consensus parameters: two participants disagreeing on any of them
// derive different roots from the same L3 stream. There is deliberately no
// runtime configuration; any change is a new versioned profile with new
// test vectors.
const (
	// ProfileID names the frozen byte profile.
	ProfileID = "sentio-spent-ids-v1"

	domainKey  = "sentio-spent-ids-v1:key"
	domainLeaf = "sentio-spent-ids-v1:leaf"
	domainNode = "sentio-spent-ids-v1:node"

	// MaxOpenGapRanges is the per-account open gap-range budget K
	// (design §4): any operation whose result would exceed it is rejected
	// with ErrGapBudgetExceeded. It bounds both FSM state and the leaf
	// preimage carried in proofs (64 × 16B = 1KB).
	MaxOpenGapRanges = 64

	hashSize = 32
)

// defaults[k] is the hash of an empty subtree at level k (leaf = level 0):
// defaults[0] is the empty leaf (32 zero bytes), defaults[k+1] =
// node(defaults[k], defaults[k]); the empty accumulator root is defaults[256].
var defaults [257][hashSize]byte

func init() {
	for k := 1; k <= 256; k++ {
		defaults[k] = hashNode(defaults[k-1], defaults[k-1])
	}
}

// EmptyRoot returns the 32-byte root of the empty accumulator.
func EmptyRoot() []byte {
	r := defaults[256]
	return r[:]
}

// hashDomain is the profile's one hashing form: BLAKE3-256 over
// domain ‖ 0x00 ‖ payload (NUL-separated domain framing, design §3).
func hashDomain(domain string, payload []byte) [hashSize]byte {
	buf := make([]byte, 0, len(domain)+1+len(payload))
	buf = append(buf, domain...)
	buf = append(buf, 0x00)
	buf = append(buf, payload...)
	return blake3.Sum256(buf)
}

// hashKey derives the SMT position key for an account. The account string
// is opaque bytes here; admission normalizes it (lowercase) before the
// accumulator sees it.
func hashKey(account string) [hashSize]byte {
	return hashDomain(domainKey, []byte(account))
}

// leafPreimage renders the canonical leaf preimage (design §3):
// len(account) u16 ‖ account ‖ hi u64 ‖ n_ranges u32 ‖ (start u64 ‖ end u64)…
// all big-endian. Shared by leaf hashing, proof verification, and snapshot
// validation so the canonical form has exactly one renderer.
func leafPreimage(account string, hi uint64, ranges []SeqRange) []byte {
	p := make([]byte, 0, 2+len(account)+8+4+16*len(ranges))
	p = binary.BigEndian.AppendUint16(p, uint16(len(account)))
	p = append(p, account...)
	p = binary.BigEndian.AppendUint64(p, hi)
	p = binary.BigEndian.AppendUint32(p, uint32(len(ranges)))
	for _, r := range ranges {
		p = binary.BigEndian.AppendUint64(p, r.Start)
		p = binary.BigEndian.AppendUint64(p, r.End)
	}
	return p
}

// hashLeafVal is the level-0 leaf value for an account's spent state.
func hashLeafVal(account string, hi uint64, ranges []SeqRange) [hashSize]byte {
	return hashDomain(domainLeaf, leafPreimage(account, hi, ranges))
}

func hashNode(left, right [hashSize]byte) [hashSize]byte {
	var payload [2 * hashSize]byte
	copy(payload[:hashSize], left[:])
	copy(payload[hashSize:], right[:])
	return hashDomain(domainNode, payload[:])
}

// keyBit returns bit i of key, MSB-first: bit 0 is the most significant
// bit of key[0] (the root's branch bit).
func keyBit(key [hashSize]byte, i int) byte {
	return (key[i/8] >> (7 - i%8)) & 1
}

// foldFrom folds a level-0 value up to toLevel along key's path using
// empty-subtree defaults as siblings. The position bit of the level-ℓ
// value inside its parent is key bit 255−ℓ (0 = left).
func foldFrom(start, key [hashSize]byte, toLevel int) [hashSize]byte {
	h := start
	for l := 0; l < toLevel; l++ {
		if keyBit(key, 255-l) == 0 {
			h = hashNode(h, defaults[l])
		} else {
			h = hashNode(defaults[l], h)
		}
	}
	return h
}
```

- [ ] **Step 5: Add the dependency and run tests**

Run: `go get github.com/zeebo/blake3@v0.2.4 && go mod tidy && go test ./accumulator/ -run 'TestFrozenProfileLiterals|TestKeyBit|TestFoldFrom|TestLeafPreimage' -v`
Expected: 4/4 PASS. If `TestFrozenProfileLiterals` fails, the implementation deviates from the spec encoding — fix the code, never the literal.

- [ ] **Step 6: Full-package check and commit**

Run: `go build ./... && go test ./accumulator/ && go vet ./... && gofmt -l accumulator/` (gofmt output empty)

```bash
git add accumulator/types.go accumulator/profile.go accumulator/profile_test.go go.mod go.sum
git commit -m "feat(accumulator): frozen sentio-spent-ids-v1 profile + hash helpers

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Per-account state machine

**Files:**
- Create: `accumulator/state.go`
- Test: `accumulator/state_test.go`

**Interfaces:**
- Consumes: `SeqRange`, `Status*`, `Err*`, `MaxOpenGapRanges` (Task 1).
- Produces: `accountState{hi uint64; ranges []SeqRange}` with methods `status(seq uint64) Status`, `insert(seq uint64) error` (state unchanged on error), `findRange(seq uint64) (int, bool)`; and `validateCanonicalState(account string, hi uint64, ranges []SeqRange) error` + `validateAccount(account string) error` (reused by proof verification and snapshot restore).

- [ ] **Step 1: Write the failing test** (`accumulator/state_test.go`)

```go
package accumulator

import (
	"errors"
	"testing"
)

func mustInsert(t *testing.T, s *accountState, seqs ...uint64) {
	t.Helper()
	for _, q := range seqs {
		if err := s.insert(q); err != nil {
			t.Fatalf("insert(%d): %v", q, err)
		}
	}
}

func TestInsertTransitions(t *testing.T) {
	t.Run("new account seq 1", func(t *testing.T) {
		var s accountState
		mustInsert(t, &s, 1)
		if s.hi != 1 || len(s.ranges) != 0 {
			t.Fatalf("got hi=%d ranges=%v", s.hi, s.ranges)
		}
	})
	t.Run("new account jump to 5", func(t *testing.T) {
		var s accountState
		mustInsert(t, &s, 5)
		if s.hi != 5 || len(s.ranges) != 1 || s.ranges[0] != (SeqRange{1, 4}) {
			t.Fatalf("got hi=%d ranges=%v", s.hi, s.ranges)
		}
	})
	t.Run("fast path increment", func(t *testing.T) {
		var s accountState
		mustInsert(t, &s, 1, 2, 3)
		if s.hi != 3 || len(s.ranges) != 0 {
			t.Fatalf("got hi=%d ranges=%v", s.hi, s.ranges)
		}
	})
	t.Run("jump then edge fills then elimination", func(t *testing.T) {
		var s accountState
		mustInsert(t, &s, 1, 10) // gap [2,9]
		if s.ranges[0] != (SeqRange{2, 9}) {
			t.Fatalf("ranges=%v", s.ranges)
		}
		mustInsert(t, &s, 2) // start edge -> [3,9]
		if s.ranges[0] != (SeqRange{3, 9}) {
			t.Fatalf("ranges=%v", s.ranges)
		}
		mustInsert(t, &s, 9) // end edge -> [3,8]
		if s.ranges[0] != (SeqRange{3, 8}) {
			t.Fatalf("ranges=%v", s.ranges)
		}
		mustInsert(t, &s, 5) // split -> [3,4],[6,8]
		if len(s.ranges) != 2 || s.ranges[0] != (SeqRange{3, 4}) || s.ranges[1] != (SeqRange{6, 8}) {
			t.Fatalf("ranges=%v", s.ranges)
		}
		mustInsert(t, &s, 3, 4) // eliminates [3,4] via edge then single
		if len(s.ranges) != 1 || s.ranges[0] != (SeqRange{6, 8}) {
			t.Fatalf("ranges=%v", s.ranges)
		}
	})
	t.Run("rejections", func(t *testing.T) {
		var s accountState
		mustInsert(t, &s, 3)
		if err := s.insert(0); !errors.Is(err, ErrSeqZero) {
			t.Fatalf("seq 0: %v", err)
		}
		if err := s.insert(3); !errors.Is(err, ErrSpentDuplicate) {
			t.Fatalf("dup hi: %v", err)
		}
		mustInsert(t, &s, 1)
		if err := s.insert(1); !errors.Is(err, ErrSpentDuplicate) {
			t.Fatalf("dup filled: %v", err)
		}
	})
}

func TestGapBudget(t *testing.T) {
	// Build exactly K open ranges with alternating jumps: inserting
	// 2, 4, 6, ... creates one single-seq gap per jump.
	var s accountState
	for i := 0; i < MaxOpenGapRanges; i++ {
		mustInsert(t, &s, uint64(2*(i+1)))
	}
	if len(s.ranges) != MaxOpenGapRanges {
		t.Fatalf("want %d ranges, got %d", MaxOpenGapRanges, len(s.ranges))
	}
	before := s.hi
	// 65th jump-created range must be rejected, state unchanged.
	if err := s.insert(s.hi + 2); !errors.Is(err, ErrGapBudgetExceeded) {
		t.Fatalf("want budget rejection, got %v", err)
	}
	if s.hi != before || len(s.ranges) != MaxOpenGapRanges {
		t.Fatal("state mutated on rejected insert")
	}
	// hi+1 (no new gap) still fine at full budget.
	mustInsert(t, &s, s.hi+1)
	// Filling a single-seq range (elimination) still fine at full budget.
	mustInsert(t, &s, s.ranges[0].Start)
	if len(s.ranges) != MaxOpenGapRanges-1 {
		t.Fatalf("elimination failed: %d ranges", len(s.ranges))
	}
}

func TestGapBudgetSplitRejected(t *testing.T) {
	// One wide range + (K-1) singles = K total; a mid-range split would
	// make K+1 -> rejected; an edge fill of the wide range is fine.
	var s accountState
	mustInsert(t, &s, 1, 100) // wide range [2,99]
	for i := 0; i < MaxOpenGapRanges-1; i++ {
		mustInsert(t, &s, s.hi+2)
	}
	if len(s.ranges) != MaxOpenGapRanges {
		t.Fatalf("setup: %d ranges", len(s.ranges))
	}
	if err := s.insert(50); !errors.Is(err, ErrGapBudgetExceeded) {
		t.Fatalf("split at full budget: %v", err)
	}
	mustInsert(t, &s, 2) // edge fill never increases the count
	if s.ranges[0] != (SeqRange{3, 99}) {
		t.Fatalf("ranges[0]=%v", s.ranges[0])
	}
}

func TestStatusClassification(t *testing.T) {
	var s accountState
	mustInsert(t, &s, 1, 10)
	cases := []struct {
		seq  uint64
		want Status
	}{
		{0, StatusSeqZero}, {11, StatusFresh}, {5, StatusGapFillable},
		{1, StatusSpentDuplicate}, {10, StatusSpentDuplicate},
	}
	for _, c := range cases {
		if got := s.status(c.seq); got != c.want {
			t.Fatalf("status(%d) = %v, want %v", c.seq, got, c.want)
		}
	}
}

func TestValidateCanonicalState(t *testing.T) {
	ok := func(hi uint64, rs []SeqRange) error {
		return validateCanonicalState("0xa", hi, rs)
	}
	if err := ok(5, []SeqRange{{2, 3}}); err != nil {
		t.Fatalf("canonical state rejected: %v", err)
	}
	bad := []struct {
		name string
		hi   uint64
		rs   []SeqRange
	}{
		{"hi zero", 0, nil},
		{"start zero", 5, []SeqRange{{0, 2}}},
		{"start>end", 5, []SeqRange{{3, 2}}},
		{"end>=hi", 5, []SeqRange{{2, 5}}},
		{"unsorted", 9, []SeqRange{{5, 6}, {2, 3}}},
		{"adjacent", 9, []SeqRange{{2, 3}, {4, 5}}},
		{"overlap", 9, []SeqRange{{2, 5}, {4, 6}}},
	}
	for _, c := range bad {
		if err := ok(c.hi, c.rs); err == nil {
			t.Fatalf("%s: accepted", c.name)
		}
	}
	if err := validateCanonicalState("", 1, nil); err == nil {
		t.Fatal("empty account accepted")
	}
	over := make([]SeqRange, MaxOpenGapRanges+1)
	for i := range over {
		over[i] = SeqRange{Start: uint64(2 + 2*i), End: uint64(2 + 2*i)}
	}
	if err := ok(uint64(2+2*MaxOpenGapRanges+2), over); err == nil {
		t.Fatal("over-budget range list accepted")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./accumulator/ -run 'TestInsertTransitions|TestGapBudget|TestStatus|TestValidateCanonical' -v`
Expected: FAIL — `undefined: accountState`, `undefined: validateCanonicalState`.

- [ ] **Step 3: Write `accumulator/state.go`**

```go
package accumulator

import (
	"fmt"
	"sort"
)

// accountState is one account's complete spent state:
// spent = [1, hi] \ ⋃ ranges (design §3). Canonical-form invariant: ranges
// sorted ascending by Start, pairwise disjoint, non-adjacent, within
// [1, hi−1], Start <= End; hi >= 1 for any existing state.
type accountState struct {
	hi     uint64
	ranges []SeqRange
}

// findRange returns the index of the open range containing seq.
func (s *accountState) findRange(seq uint64) (int, bool) {
	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].End >= seq })
	if i < len(s.ranges) && s.ranges[i].Start <= seq {
		return i, true
	}
	return 0, false
}

// status classifies seq without mutating (absent accounts are the caller's
// concern: absent ≡ hi 0 ≡ everything Fresh except seq 0).
func (s *accountState) status(seq uint64) Status {
	switch {
	case seq == 0:
		return StatusSeqZero
	case seq > s.hi:
		return StatusFresh
	}
	if _, ok := s.findRange(seq); ok {
		return StatusGapFillable
	}
	return StatusSpentDuplicate
}

// insert applies the design §4 transition rules. State is unchanged on
// error; every budget check runs before any mutation.
func (s *accountState) insert(seq uint64) error {
	switch {
	case seq == 0:
		return ErrSeqZero
	case seq == s.hi+1:
		s.hi = seq
		return nil
	case seq > s.hi+1:
		if len(s.ranges)+1 > MaxOpenGapRanges {
			return ErrGapBudgetExceeded
		}
		// The new gap starts past every existing range (all end <= hi−1),
		// so appending preserves canonical order and non-adjacency.
		s.ranges = append(s.ranges, SeqRange{Start: s.hi + 1, End: seq - 1})
		s.hi = seq
		return nil
	}
	// seq <= hi: gap-fill or duplicate.
	i, ok := s.findRange(seq)
	if !ok {
		return ErrSpentDuplicate
	}
	r := s.ranges[i]
	switch {
	case r.Start == r.End: // elimination
		s.ranges = append(s.ranges[:i], s.ranges[i+1:]...)
	case seq == r.Start: // start-edge fill
		s.ranges[i].Start = seq + 1
	case seq == r.End: // end-edge fill
		s.ranges[i].End = seq - 1
	default: // mid-range split
		if len(s.ranges)+1 > MaxOpenGapRanges {
			return ErrGapBudgetExceeded
		}
		tail := make([]SeqRange, len(s.ranges)-i-1)
		copy(tail, s.ranges[i+1:])
		s.ranges = append(s.ranges[:i],
			SeqRange{Start: r.Start, End: seq - 1},
			SeqRange{Start: seq + 1, End: r.End})
		s.ranges = append(s.ranges, tail...)
	}
	return nil
}

// validateAccount checks the opaque account string fits the wire framing.
func validateAccount(account string) error {
	if account == "" {
		return fmt.Errorf("accumulator: account must be non-empty")
	}
	if len(account) > 65535 {
		return fmt.Errorf("accumulator: account exceeds u16 length framing")
	}
	return nil
}

// validateCanonicalState enforces the design §3 canonical form on an
// externally supplied state (proof preimages, snapshot entries).
func validateCanonicalState(account string, hi uint64, ranges []SeqRange) error {
	if err := validateAccount(account); err != nil {
		return err
	}
	if hi == 0 {
		return fmt.Errorf("accumulator: hi_seq must be >= 1 for an existing state")
	}
	if len(ranges) > MaxOpenGapRanges {
		return fmt.Errorf("accumulator: %d ranges exceeds the K=%d budget", len(ranges), MaxOpenGapRanges)
	}
	prevEnd := uint64(0)
	for i, r := range ranges {
		if r.Start == 0 || r.Start > r.End {
			return fmt.Errorf("accumulator: range %d [%d,%d] malformed", i, r.Start, r.End)
		}
		if r.End >= hi {
			return fmt.Errorf("accumulator: range %d [%d,%d] not within [1,%d]", i, r.Start, r.End, hi-1)
		}
		if i > 0 && r.Start <= prevEnd+1 {
			return fmt.Errorf("accumulator: ranges %d,%d not sorted/disjoint/non-adjacent", i-1, i)
		}
		prevEnd = r.End
	}
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./accumulator/ -run 'TestInsertTransitions|TestGapBudget|TestStatus|TestValidateCanonical' -v`
Expected: PASS (all subtests).

- [ ] **Step 5: Commit**

```bash
git add accumulator/state.go accumulator/state_test.go
git commit -m "feat(accumulator): per-account spent-state transitions + K=64 gap budget

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Collapsed-trie SMT

**Files:**
- Create: `accumulator/smt.go`
- Test: `accumulator/smt_test.go`

**Interfaces:**
- Consumes: `defaults`, `hashNode`, `keyBit`, `foldFrom`, `hashSize` (Task 1).
- Produces: `type smt struct{ root *smtNode }` with methods `set(key, val [32]byte)`, `rootHash() [32]byte`, `pathSiblings(key [32]byte) [256][32]byte` (indexed by the sibling's level). Task 4 embeds `smt`; Task 5 consumes `pathSiblings`.

- [ ] **Step 1: Write the failing test** (`accumulator/smt_test.go`)

```go
package accumulator

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

// refRoot recomputes the SMT root from scratch by recursive partition —
// the naive normative definition (design §3), independent of the trie.
func refRoot(leaves map[[hashSize]byte][hashSize]byte) [hashSize]byte {
	keys := make([][hashSize]byte, 0, len(leaves))
	for k := range leaves {
		keys = append(keys, k)
	}
	var rec func(level int, ks [][hashSize]byte) [hashSize]byte
	rec = func(level int, ks [][hashSize]byte) [hashSize]byte {
		if len(ks) == 0 {
			return defaults[level]
		}
		if level == 0 {
			return leaves[ks[0]] // len(ks) == 1: keys are distinct
		}
		var left, right [][hashSize]byte
		for _, k := range ks {
			if keyBit(k, 256-level) == 0 {
				left = append(left, k)
			} else {
				right = append(right, k)
			}
		}
		return hashNode(rec(level-1, left), rec(level-1, right))
	}
	return rec(256, keys)
}

func testKV(i int) ([hashSize]byte, [hashSize]byte) {
	var k, v [hashSize]byte
	binary.BigEndian.PutUint64(k[:8], uint64(i))
	k = hashDomain(domainKey, k[:]) // spread keys pseudo-randomly
	binary.BigEndian.PutUint64(v[:8], uint64(i)+1000)
	return k, v
}

func TestSMTEmptyRoot(t *testing.T) {
	var tr smt
	if tr.rootHash() != defaults[256] {
		t.Fatal("empty trie root != EmptyRoot")
	}
}

func TestSMTSingleLeaf(t *testing.T) {
	var tr smt
	k, v := testKV(1)
	tr.set(k, v)
	if got, want := tr.rootHash(), foldFrom(v, k, 256); got != want {
		t.Fatal("single-leaf root != foldFrom(val)")
	}
}

func TestSMTMatchesReference(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	var tr smt
	leaves := map[[hashSize]byte][hashSize]byte{}
	for n := 0; n < 200; n++ {
		i := rng.Intn(64) // collisions force updates
		k, v := testKV(i)
		v[31] = byte(n) // vary value per round
		tr.set(k, v)
		leaves[k] = v
		if got, want := tr.rootHash(), refRoot(leaves); got != want {
			t.Fatalf("root diverged from reference after %d ops", n+1)
		}
	}
}

func TestSMTUpdateIsIdempotent(t *testing.T) {
	var tr smt
	k, v := testKV(1)
	tr.set(k, v)
	r1 := tr.rootHash()
	tr.set(k, v)
	if tr.rootHash() != r1 {
		t.Fatal("same set changed root")
	}
}

func TestPathSiblingsFoldToRoot(t *testing.T) {
	var tr smt
	leaves := map[[hashSize]byte][hashSize]byte{}
	for i := 0; i < 16; i++ {
		k, v := testKV(i)
		tr.set(k, v)
		leaves[k] = v
	}
	root := tr.rootHash()
	fold := func(start, key [hashSize]byte, sib [256][hashSize]byte) [hashSize]byte {
		h := start
		for l := 0; l < 256; l++ {
			if keyBit(key, 255-l) == 0 {
				h = hashNode(h, sib[l])
			} else {
				h = hashNode(sib[l], h)
			}
		}
		return h
	}
	// Present keys fold from their leaf value.
	for k, v := range leaves {
		if fold(v, k, tr.pathSiblings(k)) != root {
			t.Fatal("present-key path does not fold to root")
		}
	}
	// Absent keys fold from the empty leaf — including keys that share a
	// prefix with a collapsed leaf (the diverged-sibling case).
	for i := 100; i < 132; i++ {
		k, _ := testKV(i)
		if fold(defaults[0], k, tr.pathSiblings(k)) != root {
			t.Fatal("absent-key path does not fold to root")
		}
	}
	// pathSiblings must not disturb cached roots.
	if tr.rootHash() != root {
		t.Fatal("pathSiblings mutated the trie state")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./accumulator/ -run 'TestSMT|TestPathSiblings' -v`
Expected: FAIL — `undefined: smt`.

- [ ] **Step 3: Write `accumulator/smt.go`**

```go
package accumulator

// smt is the in-memory compressed representation of the depth-256 sparse
// Merkle tree (design §3). Only the root derivation is normative; this
// trie collapses single-leaf subtrees and caches subtree hashes. A node at
// level L branches on key bit 256−L; a collapsed leaf's subtree hash at
// level L folds its value up from level 0 with empty-default siblings.
type smt struct {
	root *smtNode
}

type smtNode struct {
	left, right *smtNode      // internal node children (nil = empty subtree)
	key, val    [hashSize]byte // leaf payload (isLeaf only)
	isLeaf      bool
	cache       [hashSize]byte // subtree hash at this node's structural level
	cacheOK     bool
}

// set inserts or updates the leaf at key. Leaves are never deleted
// (design §3: spent state never empties once hi >= 1).
func (t *smt) set(key, val [hashSize]byte) {
	t.root = setNode(t.root, key, val, 256)
}

func setNode(n *smtNode, key, val [hashSize]byte, level int) *smtNode {
	if n == nil {
		return &smtNode{isLeaf: true, key: key, val: val}
	}
	n.cacheOK = false
	if n.isLeaf {
		if n.key == key {
			n.val = val
			return n
		}
		return splitLeaf(n, key, val, level)
	}
	if keyBit(key, 256-level) == 0 {
		n.left = setNode(n.left, key, val, level-1)
	} else {
		n.right = setNode(n.right, key, val, level-1)
	}
	return n
}

// splitLeaf replaces a collapsed leaf at `level` with the internal spine
// down to the first bit where the two (distinct) keys diverge.
func splitLeaf(old *smtNode, key, val [hashSize]byte, level int) *smtNode {
	n := &smtNode{}
	bitOld := keyBit(old.key, 256-level)
	bitNew := keyBit(key, 256-level)
	if bitOld == bitNew {
		child := splitLeaf(old, key, val, level-1)
		if bitOld == 0 {
			n.left = child
		} else {
			n.right = child
		}
		return n
	}
	newLeaf := &smtNode{isLeaf: true, key: key, val: val}
	if bitOld == 0 {
		n.left, n.right = old, newLeaf
	} else {
		n.left, n.right = newLeaf, old
	}
	return n
}

// rootHash returns the SMT root (EmptyRoot when no leaves exist).
func (t *smt) rootHash() [hashSize]byte {
	return hashAt(t.root, 256)
}

// hashAt returns the subtree hash of n evaluated at `level`, caching on
// the node. Caches stay valid because a node's structural level never
// changes except when a leaf is pushed down by splitLeaf, which runs under
// setNode's cache invalidation.
func hashAt(n *smtNode, level int) [hashSize]byte {
	if n == nil {
		return defaults[level]
	}
	if n.cacheOK {
		return n.cache
	}
	var h [hashSize]byte
	if n.isLeaf {
		h = foldFrom(n.val, n.key, level)
	} else {
		h = hashNode(hashAt(n.left, level-1), hashAt(n.right, level-1))
	}
	n.cache, n.cacheOK = h, true
	return h
}

// pathSiblings returns the 256 sibling subtree hashes along key's path,
// indexed by the sibling's level (0..255). Works for absent keys too: when
// the walk meets a collapsed leaf with a different key, that leaf is the
// sibling at the first diverging bit (folded WITHOUT caching — its cache
// slot belongs to its structural level) and every deeper sibling is empty.
func (t *smt) pathSiblings(key [hashSize]byte) (siblings [256][hashSize]byte) {
	n := t.root
	for level := 256; level >= 1; level-- {
		sibLevel := level - 1
		if n == nil {
			siblings[sibLevel] = defaults[sibLevel]
			continue
		}
		if n.isLeaf {
			if n.key == key {
				siblings[sibLevel] = defaults[sibLevel]
				continue
			}
			if keyBit(n.key, 256-level) == keyBit(key, 256-level) {
				// Shared bit: the other leaf lives deeper along our path.
				siblings[sibLevel] = defaults[sibLevel]
				continue
			}
			// Diverged: the other leaf's whole subtree is our sibling here.
			siblings[sibLevel] = foldFrom(n.val, n.key, sibLevel)
			n = nil
			continue
		}
		if keyBit(key, 256-level) == 0 {
			siblings[sibLevel] = hashAt(n.right, sibLevel)
			n = n.left
		} else {
			siblings[sibLevel] = hashAt(n.left, sibLevel)
			n = n.right
		}
	}
	return siblings
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./accumulator/ -run 'TestSMT|TestPathSiblings' -v`
Expected: PASS. `TestSMTMatchesReference` is the load-bearing one (200 random ops vs the naive recursive definition).

- [ ] **Step 5: Commit**

```bash
git add accumulator/smt.go accumulator/smt_test.go
git commit -m "feat(accumulator): collapsed-trie SMT with path-sibling extraction

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Seam amendment + `SpentIDs` implementation

**Files:**
- Modify: `accumulator/accumulator.go` (replace the P0a interface block; keep the package doc comment and `Proof []byte`)
- Create: `accumulator/spentids.go`
- Test: `accumulator/spentids_test.go`

**Interfaces:**
- Consumes: Tasks 1–3 internals; `arbiter.StatementCoord` (root package).
- Produces: the amended `Accumulator` interface (spec §6) and `SpentIDs` with `NewSpentIDs() *SpentIDs`, methods `Root() []byte`, `Insert(c arbiter.StatementCoord) error`, `Status(c arbiter.StatementCoord) Status`, `AccountState(account string) (hi uint64, ranges []SeqRange, ok bool)`. Tasks 5–6 add `ProveNonMembership` / `VerifyNonMembership` / `Snapshot` / `Restore` methods to `SpentIDs` — the interface assertion `var _ Accumulator = (*SpentIDs)(nil)` therefore lands in **Task 6** (after all methods exist); Task 4 must NOT add it.

- [ ] **Step 1: Write the failing test** (`accumulator/spentids_test.go`)

```go
package accumulator

import (
	"bytes"
	"errors"
	"testing"

	"github.com/sentioxyz/arbiter"
)

func coord(account string, seq uint64) arbiter.StatementCoord {
	return arbiter.StatementCoord{Account: account, ClientSeq: seq}
}

func TestSpentIDsEmptyRoot(t *testing.T) {
	a := NewSpentIDs()
	if !bytes.Equal(a.Root(), EmptyRoot()) {
		t.Fatal("fresh accumulator root != EmptyRoot")
	}
}

func TestSpentIDsInsertAdvancesRoot(t *testing.T) {
	a := NewSpentIDs()
	r0 := append([]byte(nil), a.Root()...)
	if err := a.Insert(coord("0xaa", 1)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if bytes.Equal(a.Root(), r0) {
		t.Fatal("root unchanged after insert")
	}
	// The root must equal a single-leaf tree built from the same state.
	var tr smt
	tr.set(hashKey("0xaa"), hashLeafVal("0xaa", 1, nil))
	want := tr.rootHash()
	if !bytes.Equal(a.Root(), want[:]) {
		t.Fatal("root != independently built single-leaf root")
	}
}

func TestSpentIDsRejectionsLeaveRootUnchanged(t *testing.T) {
	a := NewSpentIDs()
	if err := a.Insert(coord("0xaa", 3)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	r := append([]byte(nil), a.Root()...)
	if err := a.Insert(coord("0xaa", 0)); !errors.Is(err, ErrSeqZero) {
		t.Fatalf("seq0: %v", err)
	}
	if err := a.Insert(coord("0xaa", 3)); !errors.Is(err, ErrSpentDuplicate) {
		t.Fatalf("dup: %v", err)
	}
	if err := a.Insert(coord("", 1)); err == nil {
		t.Fatal("empty account accepted")
	}
	if !bytes.Equal(a.Root(), r) {
		t.Fatal("root changed on rejected insert")
	}
}

func TestSpentIDsOrderIndependence(t *testing.T) {
	// Same accepted set in different orders -> same root (design §2).
	build := func(perm []int) []byte {
		a := NewSpentIDs()
		coords := []arbiter.StatementCoord{
			coord("0xaa", 1), coord("0xaa", 5), coord("0xaa", 3),
			coord("0xbb", 2), coord("0xbb", 1), coord("0xcc", 7),
		}
		for _, i := range perm {
			if err := a.Insert(coords[i]); err != nil {
				t.Fatalf("insert %v: %v", coords[i], err)
			}
		}
		return a.Root()
	}
	// Two orders that are both fill-legal: gap-fills happen after jumps.
	r1 := build([]int{0, 1, 2, 3, 4, 5})
	r2 := build([]int{5, 3, 4, 0, 1, 2})
	if !bytes.Equal(r1, r2) {
		t.Fatal("roots differ across insertion orders of the same set")
	}
}

func TestSpentIDsStatusAndAccountState(t *testing.T) {
	a := NewSpentIDs()
	if a.Status(coord("0xzz", 9)) != StatusFresh {
		t.Fatal("unseen account not Fresh")
	}
	if a.Status(coord("0xzz", 0)) != StatusSeqZero {
		t.Fatal("seq0 not classified")
	}
	if err := a.Insert(coord("0xaa", 10)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if a.Status(coord("0xaa", 4)) != StatusGapFillable {
		t.Fatal("gap not classified")
	}
	hi, ranges, ok := a.AccountState("0xaa")
	if !ok || hi != 10 || len(ranges) != 1 || ranges[0] != (SeqRange{1, 9}) {
		t.Fatalf("AccountState: %d %v %v", hi, ranges, ok)
	}
	ranges[0].Start = 999 // caller mutation must not leak in
	if _, r2, _ := a.AccountState("0xaa"); r2[0].Start != 1 {
		t.Fatal("AccountState returned aliased slice")
	}
	if _, _, ok := a.AccountState("0xnone"); ok {
		t.Fatal("absent account reported present")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./accumulator/ -run 'TestSpentIDs' -v`
Expected: FAIL — `undefined: NewSpentIDs`.

- [ ] **Step 3: Amend the seam in `accumulator/accumulator.go`**

Replace the existing `Proof`/`Accumulator` declarations (keep the package doc comment at the top of the file) with:

```go
// Proof is an opaque encoded non-membership proof (design §5 wire format).
// Byte encoding frozen by the sentio-spent-ids-v1 profile; carried in
// SubmitStatementCmd.non_membership_proof for the decentralized phase
// (always empty in v1 — the FSM holds the full dictionary).
type Proof []byte

// Accumulator is the FSM's spent-ids authenticated dictionary (design §6;
// amends the P0a seam: Insert gains an error return because this
// construction rejects SeqZero / SpentDuplicate / GapBudgetExceeded at the
// accumulator layer, and admission/snapshot primitives are added for P1a).
type Accumulator interface {
	// Root returns the 32-byte spent_ids_root (EmptyRoot when empty).
	Root() []byte
	// Insert commits a coordinate per the design §4 transition rules and
	// advances the root. State is unchanged on error.
	Insert(c arbiter.StatementCoord) error
	// Status classifies a coordinate read-only — the §6.3 admission
	// primitive. The gap budget is Insert's concern, not Status's.
	Status(c arbiter.StatementCoord) Status
	// AccountState returns a copy of the account's committed spent state.
	AccountState(account string) (hi uint64, ranges []SeqRange, ok bool)
	// ProveNonMembership builds a proof that c is unspent (leader/audit
	// side; errors for invalid or already-spent coordinates).
	ProveNonMembership(c arbiter.StatementCoord) (Proof, error)
	// VerifyNonMembership checks a proof against the CURRENT root;
	// equivalent to Verify(Root(), c, p).
	VerifyNonMembership(c arbiter.StatementCoord, p Proof) bool
	// Snapshot writes the canonical dictionary dump (design §6); Restore
	// replaces this accumulator's state from one.
	Snapshot(w io.Writer) error
	Restore(r io.Reader) error
}
```

(adjust the file's imports to `"io"` + the existing `"github.com/sentioxyz/arbiter"`)

- [ ] **Step 4: Write `accumulator/spentids.go`**

```go
package accumulator

import (
	"github.com/sentioxyz/arbiter"
)

// SpentIDs is the FSM-held accumulator (design §2): the authenticated
// dictionary of every account's spent state, with the SMT authenticating
// exactly the dictionary the FSM must hold anyway. Not safe for concurrent
// use; the FSM applies commands serially.
type SpentIDs struct {
	accounts map[string]*accountState
	tree     smt
}

// NewSpentIDs returns an empty accumulator (Root() == EmptyRoot()).
func NewSpentIDs() *SpentIDs {
	return &SpentIDs{accounts: make(map[string]*accountState)}
}

// Root returns the 32-byte spent_ids_root.
func (a *SpentIDs) Root() []byte {
	r := a.tree.rootHash()
	return r[:]
}

// Insert commits (account, seq) per the design §4 rules; state is
// unchanged on error.
func (a *SpentIDs) Insert(c arbiter.StatementCoord) error {
	if err := validateAccount(c.Account); err != nil {
		return err
	}
	st, existing := a.accounts[c.Account]
	if !existing {
		if c.ClientSeq == 0 {
			return ErrSeqZero
		}
		st = &accountState{}
	}
	if err := st.insert(c.ClientSeq); err != nil {
		return err
	}
	if !existing {
		a.accounts[c.Account] = st
	}
	a.tree.set(hashKey(c.Account), hashLeafVal(c.Account, st.hi, st.ranges))
	return nil
}

// Status classifies a coordinate against committed state (read-only).
func (a *SpentIDs) Status(c arbiter.StatementCoord) Status {
	if c.ClientSeq == 0 {
		return StatusSeqZero
	}
	st, ok := a.accounts[c.Account]
	if !ok {
		return StatusFresh
	}
	return st.status(c.ClientSeq)
}

// AccountState returns a copy of the account's spent state.
func (a *SpentIDs) AccountState(account string) (uint64, []SeqRange, bool) {
	st, ok := a.accounts[account]
	if !ok {
		return 0, nil, false
	}
	ranges := make([]SeqRange, len(st.ranges))
	copy(ranges, st.ranges)
	return st.hi, ranges, true
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./accumulator/ -run 'TestSpentIDs' -v && go build ./...`
Expected: tests PASS. NOTE: `go build ./...` FAILS at this point only if something outside the package referenced the old `Insert` signature — nothing does (the P0a repo has no `Accumulator` consumers); expect a clean build. The `Accumulator` interface is not yet satisfied by `SpentIDs` (Prove/Verify/Snapshot/Restore land in Tasks 5–6) — that is why the compile-time assertion is deferred to Task 6.

- [ ] **Step 6: Commit**

```bash
git add accumulator/accumulator.go accumulator/spentids.go accumulator/spentids_test.go
git commit -m "feat(accumulator): SpentIDs dictionary + amended P0a seam (design §6)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Non-membership proofs

**Files:**
- Create: `accumulator/proof.go`
- Test: `accumulator/proof_test.go`

**Interfaces:**
- Consumes: `smt.pathSiblings`, profile helpers, `validateCanonicalState`, `SpentIDs` internals.
- Produces: package-level `Verify(root []byte, c arbiter.StatementCoord, p Proof) bool`; methods `(*SpentIDs) ProveNonMembership(c arbiter.StatementCoord) (Proof, error)` and `(*SpentIDs) VerifyNonMembership(c arbiter.StatementCoord, p Proof) bool`.

- [ ] **Step 1: Write the failing test** (`accumulator/proof_test.go`)

```go
package accumulator

import (
	"errors"
	"testing"

	"github.com/sentioxyz/arbiter"
)

func provingFixture(t *testing.T) *SpentIDs {
	t.Helper()
	a := NewSpentIDs()
	for _, c := range []arbiter.StatementCoord{
		coord("0xaa", 1), coord("0xaa", 10), // gap [2,9]
		coord("0xbb", 3),                    // gap [1,2]
		coord("0xcc", 1), coord("0xdd", 2),
	} {
		if err := a.Insert(c); err != nil {
			t.Fatalf("fixture insert %v: %v", c, err)
		}
	}
	return a
}

func TestProveVerifyHonestCases(t *testing.T) {
	a := provingFixture(t)
	root := a.Root()
	cases := []struct {
		name string
		c    arbiter.StatementCoord
	}{
		{"absent account", coord("0xnew", 1)},
		{"fresh beyond hi", coord("0xaa", 11)},
		{"inside gap", coord("0xaa", 5)},
		{"gap other account", coord("0xbb", 2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := a.ProveNonMembership(tc.c)
			if err != nil {
				t.Fatalf("prove: %v", err)
			}
			if !Verify(root, tc.c, p) {
				t.Fatal("honest proof rejected")
			}
			if !a.VerifyNonMembership(tc.c, p) {
				t.Fatal("VerifyNonMembership rejected honest proof")
			}
		})
	}
}

func TestProveSpentCoordinateErrors(t *testing.T) {
	a := provingFixture(t)
	if _, err := a.ProveNonMembership(coord("0xaa", 10)); err == nil {
		t.Fatal("proved a spent coordinate")
	}
	if _, err := a.ProveNonMembership(coord("0xaa", 0)); !errors.Is(err, ErrSeqZero) {
		t.Fatalf("seq0: %v", err)
	}
}

func TestVerifyRejections(t *testing.T) {
	a := provingFixture(t)
	root := a.Root()
	c := coord("0xaa", 5)
	p, err := a.ProveNonMembership(c)
	if err != nil {
		t.Fatalf("prove: %v", err)
	}

	t.Run("spent coordinate under same proof", func(t *testing.T) {
		if Verify(root, coord("0xaa", 10), p) {
			t.Fatal("proof accepted for a spent coordinate")
		}
	})
	t.Run("seq zero", func(t *testing.T) {
		if Verify(root, coord("0xaa", 0), p) {
			t.Fatal("seq 0 accepted")
		}
	})
	t.Run("wrong root", func(t *testing.T) {
		bad := append([]byte(nil), root...)
		bad[0] ^= 1
		if Verify(bad, c, p) {
			t.Fatal("wrong root accepted")
		}
	})
	t.Run("wrong account", func(t *testing.T) {
		if Verify(root, coord("0xbb", 5), p) {
			t.Fatal("proof for 0xaa accepted for 0xbb")
		}
	})
	t.Run("tampered byte", func(t *testing.T) {
		for i := range p {
			bad := append(Proof(nil), p...)
			bad[i] ^= 0x01
			if Verify(root, c, bad) {
				t.Fatalf("tampered byte %d accepted", i)
			}
		}
	})
	t.Run("truncated / trailing", func(t *testing.T) {
		if Verify(root, c, p[:len(p)-1]) {
			t.Fatal("truncated proof accepted")
		}
		if Verify(root, c, append(append(Proof(nil), p...), 0x00)) {
			t.Fatal("trailing byte accepted")
		}
		if Verify(root, c, nil) {
			t.Fatal("nil proof accepted")
		}
	})
}

func TestVerifyRejectsNonCanonicalPreimage(t *testing.T) {
	// Hand-build a kind=2 proof whose range list is adjacent (must merge):
	// even with a root recomputed to match, Verify must reject before the
	// root comparison can pass — validateCanonicalState guards the leaf.
	a := NewSpentIDs()
	if err := a.Insert(coord("0xaa", 10)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	p, err := a.ProveNonMembership(coord("0xaa", 5)) // honest: ranges [[1,9]]
	if err != nil {
		t.Fatalf("prove: %v", err)
	}
	// Honest proof decodes; rebuild one carrying [[1,4],[5,9]] (adjacent,
	// same set) — a non-canonical split of the same gap.
	forged := buildProofForTest(2, "0xaa", 10,
		[]SeqRange{{1, 4}, {5, 9}}, a.tree.pathSiblings(hashKey("0xaa")))
	if Verify(a.Root(), coord("0xaa", 5), forged) {
		t.Fatal("non-canonical range list accepted")
	}
	_ = p
}

func TestVerifyRejectsDefaultSiblingSupplied(t *testing.T) {
	// A proof that explicitly carries a default sibling (bitmap bit set,
	// sibling == D[l]) is non-canonical and must be rejected.
	a := NewSpentIDs()
	if err := a.Insert(coord("0xaa", 1)); err != nil {
		t.Fatalf("insert: %v", err)
	}
	sib := a.tree.pathSiblings(hashKey("0xzz"))
	sib[0] = defaults[0] // force an explicit default into the encoding
	forged := buildProofForTestWithBitmapOverride(1, "0xzz", 0, nil, sib, 0)
	if Verify(a.Root(), coord("0xzz", 3), forged) {
		t.Fatal("explicit default sibling accepted")
	}
}
```

The two `buildProofForTest*` helpers are test-only constructors placed at the bottom of `proof_test.go` (they reuse the same encoder internals):

```go
// buildProofForTest encodes a proof from raw parts, bypassing Prove's
// honesty checks — for negative tests only.
func buildProofForTest(kind byte, account string, hi uint64, ranges []SeqRange, siblings [256][hashSize]byte) Proof {
	return encodeProof(kind, account, hi, ranges, siblings)
}

// buildProofForTestWithBitmapOverride additionally forces the bitmap bit
// at forcedLevel to be set even when the sibling equals the default.
func buildProofForTestWithBitmapOverride(kind byte, account string, hi uint64, ranges []SeqRange, siblings [256][hashSize]byte, forcedLevel int) Proof {
	p := encodeProofForced(kind, account, hi, ranges, siblings, map[int]bool{forcedLevel: true})
	return p
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./accumulator/ -run 'TestProve|TestVerify' -v`
Expected: FAIL — `undefined: Verify`, `undefined: encodeProof`.

- [ ] **Step 3: Write `accumulator/proof.go`**

```go
package accumulator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/bits"

	"github.com/sentioxyz/arbiter"
)

// Proof wire format (design §5), version 1:
//
//	ver(u8=1) ‖ kind(u8: 1=account absent, 2=account present)
//	‖ pathBitmap(32B)                    bit ℓ set ⇒ sibling supplied for level ℓ
//	‖ nSiblings(u16) ‖ siblings(32B ea)  bottom-up, only for set bits
//	‖ [kind=2: len(account) u16 ‖ account ‖ hi u64 ‖ n_ranges u32 ‖ ranges…]
//
// Bitmap bit ℓ is bit 7−(ℓ mod 8) of byte ℓ/8. Strict decoding: exact
// length, nSiblings == popcount(bitmap), no supplied sibling may equal the
// level default, kind=2 preimage must be canonical and byte-equal the
// proof target's account. Malformed or non-canonical proofs verify false.

const (
	proofVersion      = 1
	proofKindAbsent   = 1
	proofKindPresent  = 2
	bitmapBytes       = hashSize // 256 bits
	proofHeaderLen    = 2 + bitmapBytes + 2
)

func bitmapBit(bm []byte, l int) bool {
	return bm[l/8]&(1<<(7-l%8)) != 0
}

func encodeProof(kind byte, account string, hi uint64, ranges []SeqRange, siblings [256][hashSize]byte) Proof {
	return encodeProofForced(kind, account, hi, ranges, siblings, nil)
}

// encodeProofForced exists so tests can force non-canonical bitmap bits;
// production callers pass forced == nil.
func encodeProofForced(kind byte, account string, hi uint64, ranges []SeqRange, siblings [256][hashSize]byte, forced map[int]bool) Proof {
	var bm [bitmapBytes]byte
	var sibs []byte
	n := 0
	for l := 0; l < 256; l++ {
		if siblings[l] != defaults[l] || forced[l] {
			bm[l/8] |= 1 << (7 - l%8)
			sibs = append(sibs, siblings[l][:]...)
			n++
		}
	}
	p := make([]byte, 0, proofHeaderLen+len(sibs)+2+len(account)+12+16*len(ranges))
	p = append(p, proofVersion, kind)
	p = append(p, bm[:]...)
	p = binary.BigEndian.AppendUint16(p, uint16(n))
	p = append(p, sibs...)
	if kind == proofKindPresent {
		p = append(p, leafPreimage(account, hi, ranges)...)
	}
	return Proof(p)
}

type decodedProof struct {
	kind     byte
	bitmap   []byte
	siblings [][hashSize]byte
	account  string
	hi       uint64
	ranges   []SeqRange
}

// decodeProof parses strictly; ok=false on any malformation.
func decodeProof(p Proof) (decodedProof, bool) {
	var d decodedProof
	if len(p) < proofHeaderLen {
		return d, false
	}
	if p[0] != proofVersion {
		return d, false
	}
	d.kind = p[1]
	if d.kind != proofKindAbsent && d.kind != proofKindPresent {
		return d, false
	}
	d.bitmap = p[2 : 2+bitmapBytes]
	pop := 0
	for _, b := range d.bitmap {
		pop += bits.OnesCount8(b)
	}
	n := int(binary.BigEndian.Uint16(p[2+bitmapBytes : proofHeaderLen]))
	if n != pop {
		return d, false
	}
	rest := p[proofHeaderLen:]
	if len(rest) < n*hashSize {
		return d, false
	}
	d.siblings = make([][hashSize]byte, n)
	for i := 0; i < n; i++ {
		copy(d.siblings[i][:], rest[i*hashSize:(i+1)*hashSize])
	}
	rest = rest[n*hashSize:]
	if d.kind == proofKindAbsent {
		return d, len(rest) == 0
	}
	// kind=2 preimage: len u16 ‖ account ‖ hi u64 ‖ nRanges u32 ‖ ranges
	if len(rest) < 2 {
		return d, false
	}
	alen := int(binary.BigEndian.Uint16(rest))
	rest = rest[2:]
	if len(rest) < alen+8+4 {
		return d, false
	}
	d.account = string(rest[:alen])
	rest = rest[alen:]
	d.hi = binary.BigEndian.Uint64(rest)
	rest = rest[8:]
	nr := int(binary.BigEndian.Uint32(rest))
	rest = rest[4:]
	if len(rest) != nr*16 {
		return d, false
	}
	d.ranges = make([]SeqRange, nr)
	for i := 0; i < nr; i++ {
		d.ranges[i].Start = binary.BigEndian.Uint64(rest[i*16:])
		d.ranges[i].End = binary.BigEndian.Uint64(rest[i*16+8:])
	}
	return d, true
}

// Verify checks a non-membership proof against a root (design §5): the
// package-level pure verifier any node can run from the root alone.
func Verify(root []byte, c arbiter.StatementCoord, p Proof) bool {
	if len(root) != hashSize || c.ClientSeq == 0 || validateAccount(c.Account) != nil {
		return false
	}
	d, ok := decodeProof(p)
	if !ok {
		return false
	}
	start := defaults[0] // kind=1: fold from the empty leaf
	if d.kind == proofKindPresent {
		if d.account != c.Account {
			return false
		}
		if validateCanonicalState(d.account, d.hi, d.ranges) != nil {
			return false
		}
		start = hashLeafVal(d.account, d.hi, d.ranges)
	}
	key := hashKey(c.Account)
	h := start
	sibIdx := 0
	for l := 0; l < 256; l++ {
		var sib [hashSize]byte
		if bitmapBit(d.bitmap, l) {
			sib = d.siblings[sibIdx]
			sibIdx++
			if sib == defaults[l] {
				return false // non-canonical: explicit default sibling
			}
		} else {
			sib = defaults[l]
		}
		if keyBit(key, 255-l) == 0 {
			h = hashNode(h, sib)
		} else {
			h = hashNode(sib, h)
		}
	}
	if !bytes.Equal(h[:], root) {
		return false
	}
	if d.kind == proofKindAbsent {
		return true // nothing spent for this account
	}
	if c.ClientSeq > d.hi {
		return true
	}
	for _, r := range d.ranges {
		if r.Start <= c.ClientSeq && c.ClientSeq <= r.End {
			return true
		}
	}
	return false
}

// ProveNonMembership builds the proof that c is unspent under the current
// root (design §5; leader/audit side).
func (a *SpentIDs) ProveNonMembership(c arbiter.StatementCoord) (Proof, error) {
	if c.ClientSeq == 0 {
		return nil, ErrSeqZero
	}
	if err := validateAccount(c.Account); err != nil {
		return nil, err
	}
	st, present := a.accounts[c.Account]
	if present && st.status(c.ClientSeq) == StatusSpentDuplicate {
		return nil, fmt.Errorf("accumulator: cannot prove non-membership of spent coordinate (%s, %d)", c.Account, c.ClientSeq)
	}
	siblings := a.tree.pathSiblings(hashKey(c.Account))
	if !present {
		return encodeProof(proofKindAbsent, "", 0, nil, siblings), nil
	}
	return encodeProof(proofKindPresent, c.Account, st.hi, st.ranges, siblings), nil
}

// VerifyNonMembership checks p against the CURRENT root.
func (a *SpentIDs) VerifyNonMembership(c arbiter.StatementCoord, p Proof) bool {
	return Verify(a.Root(), c, p)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./accumulator/ -run 'TestProve|TestVerify' -v`
Expected: PASS, including the full tampered-byte sweep and both non-canonical rejections.

- [ ] **Step 5: Commit**

```bash
git add accumulator/proof.go accumulator/proof_test.go
git commit -m "feat(accumulator): non-membership proofs (encode/decode/Prove/Verify)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Snapshot serialization + interface assertion

**Files:**
- Create: `accumulator/snapshot.go`
- Test: `accumulator/snapshot_test.go`

**Interfaces:**
- Consumes: `SpentIDs` internals, `leafPreimage` layout rules, `validateCanonicalState`.
- Produces: `(*SpentIDs) Snapshot(w io.Writer) error`, `(*SpentIDs) Restore(r io.Reader) error`, and the deferred compile-time assertion `var _ Accumulator = (*SpentIDs)(nil)` (now that the method set is complete).

- [ ] **Step 1: Write the failing test** (`accumulator/snapshot_test.go`)

```go
package accumulator

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/sentioxyz/arbiter"
)

func snapshotFixture(t *testing.T) *SpentIDs {
	t.Helper()
	a := NewSpentIDs()
	for _, c := range []arbiter.StatementCoord{
		coord("0xbb", 3), coord("0xaa", 1), coord("0xaa", 10), coord("0xcc", 5),
	} {
		if err := a.Insert(c); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	return a
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	a := snapshotFixture(t)
	var buf bytes.Buffer
	if err := a.Snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	b := NewSpentIDs()
	if err := b.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !bytes.Equal(a.Root(), b.Root()) {
		t.Fatal("root changed across snapshot round-trip")
	}
	hi, ranges, ok := b.AccountState("0xaa")
	if !ok || hi != 10 || len(ranges) != 1 || ranges[0] != (SeqRange{2, 9}) {
		t.Fatalf("restored state: %d %v %v", hi, ranges, ok)
	}
	// Restored accumulator keeps working.
	if err := b.Insert(coord("0xaa", 5)); err != nil {
		t.Fatalf("insert after restore: %v", err)
	}
}

func TestSnapshotEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := NewSpentIDs().Snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	b := NewSpentIDs()
	if err := b.Restore(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !bytes.Equal(b.Root(), EmptyRoot()) {
		t.Fatal("empty round-trip root mismatch")
	}
}

func TestSnapshotEntriesSortedByAccountBytes(t *testing.T) {
	a := snapshotFixture(t)
	var buf bytes.Buffer
	if err := a.Snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	raw := buf.Bytes()
	// header: "SIDS" ‖ ver u8 ‖ count u64
	if string(raw[:4]) != "SIDS" || raw[4] != 1 {
		t.Fatalf("header: %x", raw[:5])
	}
	count := binary.BigEndian.Uint64(raw[5:13])
	if count != 3 {
		t.Fatalf("count: %d", count)
	}
	// first entry account must be 0xaa (bytewise ascending)
	alen := binary.BigEndian.Uint16(raw[13:15])
	if string(raw[15:15+int(alen)]) != "0xaa" {
		t.Fatalf("first entry: %q", raw[15:15+int(alen)])
	}
}

func TestRestoreRejections(t *testing.T) {
	a := snapshotFixture(t)
	var buf bytes.Buffer
	if err := a.Snapshot(&buf); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	good := buf.Bytes()

	mutate := func(f func(b []byte) []byte) error {
		b := append([]byte(nil), good...)
		return NewSpentIDs().Restore(bytes.NewReader(f(b)))
	}
	if err := mutate(func(b []byte) []byte { b[0] = 'X'; return b }); err == nil {
		t.Fatal("bad magic accepted")
	}
	if err := mutate(func(b []byte) []byte { b[4] = 9; return b }); err == nil {
		t.Fatal("bad version accepted")
	}
	if err := mutate(func(b []byte) []byte { return append(b, 0x00) }); err == nil {
		t.Fatal("trailing bytes accepted")
	}
	if err := mutate(func(b []byte) []byte { return b[:len(b)-1] }); err == nil {
		t.Fatal("truncation accepted")
	}
	// Unsorted entries: snapshot two accounts, swap their entries.
	two := NewSpentIDs()
	if err := two.Insert(coord("0xaa", 1)); err != nil {
		t.Fatal(err)
	}
	if err := two.Insert(coord("0xbb", 1)); err != nil {
		t.Fatal(err)
	}
	var tb bytes.Buffer
	if err := two.Snapshot(&tb); err != nil {
		t.Fatal(err)
	}
	raw := tb.Bytes()
	// entry = len u16 ‖ account(4) ‖ hi u64 ‖ nRanges u32 = 18 bytes each here
	e1 := append([]byte(nil), raw[13:13+18]...)
	e2 := append([]byte(nil), raw[13+18:13+36]...)
	swapped := append(append(append([]byte(nil), raw[:13]...), e2...), e1...)
	if err := NewSpentIDs().Restore(bytes.NewReader(swapped)); err == nil {
		t.Fatal("unsorted entries accepted")
	}
}

func TestAccumulatorInterfaceSatisfied(t *testing.T) {
	var _ Accumulator = (*SpentIDs)(nil)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./accumulator/ -run 'TestSnapshot|TestRestore|TestAccumulatorInterface' -v`
Expected: FAIL — `SpentIDs` has no `Snapshot`/`Restore` (compile error).

- [ ] **Step 3: Write `accumulator/snapshot.go`**

```go
package accumulator

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// Snapshot wire format (design §6), version 1: the canonical dictionary
// dump — the SMT is derived state and is rebuilt on Restore.
//
//	"SIDS" ‖ ver u8=1 ‖ count u64 ‖ entries…
//	entry = len(account) u16 ‖ account ‖ hi u64 ‖ n_ranges u32 ‖ (start u64 ‖ end u64)…
//
// Entries sorted ascending by account bytes; all integers big-endian.

var snapshotMagic = []byte("SIDS")

const snapshotVersion = 1

// Snapshot writes the canonical dump of the dictionary.
func (a *SpentIDs) Snapshot(w io.Writer) error {
	bw := bufio.NewWriter(w)
	if _, err := bw.Write(snapshotMagic); err != nil {
		return err
	}
	if err := bw.WriteByte(snapshotVersion); err != nil {
		return err
	}
	accounts := make([]string, 0, len(a.accounts))
	for acct := range a.accounts {
		accounts = append(accounts, acct)
	}
	sort.Strings(accounts) // Go string order == bytewise lexicographic
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], uint64(len(accounts)))
	if _, err := bw.Write(num[:]); err != nil {
		return err
	}
	for _, acct := range accounts {
		st := a.accounts[acct]
		if _, err := bw.Write(leafPreimage(acct, st.hi, st.ranges)); err != nil {
			return err
		}
	}
	return bw.Flush()
}

// Restore replaces this accumulator's state from a canonical dump,
// validating strictly (magic, version, entry order, canonical form, exact
// length). On error the receiver is left unchanged.
func (a *SpentIDs) Restore(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("accumulator: read snapshot: %w", err)
	}
	if len(data) < len(snapshotMagic)+1+8 {
		return fmt.Errorf("accumulator: snapshot too short")
	}
	if !bytes.Equal(data[:4], snapshotMagic) {
		return fmt.Errorf("accumulator: bad snapshot magic")
	}
	if data[4] != snapshotVersion {
		return fmt.Errorf("accumulator: unsupported snapshot version %d", data[4])
	}
	count := binary.BigEndian.Uint64(data[5:13])
	rest := data[13:]

	fresh := NewSpentIDs()
	prevAccount := ""
	for i := uint64(0); i < count; i++ {
		if len(rest) < 2 {
			return fmt.Errorf("accumulator: snapshot truncated at entry %d", i)
		}
		alen := int(binary.BigEndian.Uint16(rest))
		rest = rest[2:]
		if len(rest) < alen+8+4 {
			return fmt.Errorf("accumulator: snapshot truncated at entry %d", i)
		}
		acct := string(rest[:alen])
		rest = rest[alen:]
		hi := binary.BigEndian.Uint64(rest)
		rest = rest[8:]
		nr := int(binary.BigEndian.Uint32(rest))
		rest = rest[4:]
		if len(rest) < nr*16 {
			return fmt.Errorf("accumulator: snapshot truncated at entry %d ranges", i)
		}
		ranges := make([]SeqRange, nr)
		for j := 0; j < nr; j++ {
			ranges[j].Start = binary.BigEndian.Uint64(rest[j*16:])
			ranges[j].End = binary.BigEndian.Uint64(rest[j*16+8:])
		}
		rest = rest[nr*16:]
		if i > 0 && acct <= prevAccount {
			return fmt.Errorf("accumulator: snapshot entries not strictly ascending at %q", acct)
		}
		prevAccount = acct
		if err := validateCanonicalState(acct, hi, ranges); err != nil {
			return fmt.Errorf("accumulator: snapshot entry %q: %w", acct, err)
		}
		fresh.accounts[acct] = &accountState{hi: hi, ranges: ranges}
		fresh.tree.set(hashKey(acct), hashLeafVal(acct, hi, ranges))
	}
	if len(rest) != 0 {
		return fmt.Errorf("accumulator: %d trailing bytes in snapshot", len(rest))
	}
	a.accounts = fresh.accounts
	a.tree = fresh.tree
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./accumulator/ -v`
Expected: ALL package tests PASS (Tasks 1–6 combined).

- [ ] **Step 5: Commit**

```bash
git add accumulator/snapshot.go accumulator/snapshot_test.go
git commit -m "feat(accumulator): canonical snapshot serialization + complete Accumulator seam

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: JSON test vectors (`-update` generator + frozen file)

**Files:**
- Create: `accumulator/vectors_test.go`, `accumulator/testdata/spent_ids_vectors.json` (generated then committed)

**Interfaces:**
- Consumes: everything (exported API + unexported hash helpers — the test lives in package `accumulator`).
- Produces: the frozen cross-implementation vector file (design §9); regeneration only via `go test ./accumulator/ -run TestVectors -update` followed by review.

- [ ] **Step 1: Write `accumulator/vectors_test.go`**

```go
package accumulator

import (
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/sentioxyz/arbiter"
)

// The vector file freezes the sentio-spent-ids-v1 profile for future
// implementations (design §9). Regenerate ONLY deliberately:
//
//	go test ./accumulator/ -run TestVectors -update
//
// and treat any diff as a profile change requiring coordinated review.

var update = flag.Bool("update", false, "regenerate testdata/spent_ids_vectors.json")

type vectorFile struct {
	Profile    string          `json:"profile"`
	Primitives vecPrimitives   `json:"primitives"`
	Sequences  []vecSequence   `json:"sequences"`
	Proofs     []vecProof      `json:"proofs"`
}

type vecPrimitives struct {
	EmptyRoot string            `json:"empty_root"`
	Defaults  map[string]string `json:"defaults"` // level -> hex
	Keys      []vecKey          `json:"keys"`
	Leaves    []vecLeaf         `json:"leaves"`
}

type vecKey struct {
	Account string `json:"account"`
	Key     string `json:"key"`
}

type vecLeaf struct {
	Account string     `json:"account"`
	Hi      uint64     `json:"hi"`
	Ranges  []SeqRange `json:"ranges"`
	LeafVal string     `json:"leafval"`
}

type vecOp struct {
	Account string `json:"account"`
	Seq     uint64 `json:"seq"`
	Expect  string `json:"expect"` // ok | seq_zero | duplicate | gap_budget
	Root    string `json:"root"`   // root AFTER this op (unchanged on rejection)
}

type vecSequence struct {
	Name string  `json:"name"`
	Ops  []vecOp `json:"ops"`
}

type vecProof struct {
	Sequence string `json:"sequence"` // sequence whose final root this proves against
	Account  string `json:"account"`
	Seq      uint64 `json:"seq"`
	Proof    string `json:"proof"`
	Valid    bool   `json:"valid"`
}

func outcomeString(err error) string {
	switch err {
	case nil:
		return "ok"
	case ErrSeqZero:
		return "seq_zero"
	case ErrSpentDuplicate:
		return "duplicate"
	case ErrGapBudgetExceeded:
		return "gap_budget"
	}
	return "error:" + err.Error()
}

// buildVectors produces the whole file deterministically (no randomness,
// no clock) so -update is reproducible.
func buildVectors() vectorFile {
	const acctA = "0x1111111111111111111111111111111111111111"
	const acctB = "0x2222222222222222222222222222222222222222"

	var vf vectorFile
	vf.Profile = ProfileID
	vf.Primitives.EmptyRoot = hex.EncodeToString(EmptyRoot())
	vf.Primitives.Defaults = map[string]string{}
	for _, l := range []int{0, 1, 2, 128, 255, 256} {
		vf.Primitives.Defaults[itoa(l)] = hex.EncodeToString(defaults[l][:])
	}
	for _, acct := range []string{acctA, acctB, "0xab"} {
		k := hashKey(acct)
		vf.Primitives.Keys = append(vf.Primitives.Keys, vecKey{Account: acct, Key: hex.EncodeToString(k[:])})
	}
	for _, lf := range []vecLeaf{
		{Account: acctA, Hi: 1, Ranges: nil},
		{Account: acctA, Hi: 5, Ranges: []SeqRange{{2, 3}}},
		{Account: acctB, Hi: 10, Ranges: []SeqRange{{1, 4}, {6, 9}}},
	} {
		v := hashLeafVal(lf.Account, lf.Hi, lf.Ranges)
		lf.LeafVal = hex.EncodeToString(v[:])
		vf.Primitives.Leaves = append(vf.Primitives.Leaves, lf)
	}

	runSeq := func(name string, ops []struct {
		acct string
		seq  uint64
	}) (*SpentIDs, vecSequence) {
		a := NewSpentIDs()
		s := vecSequence{Name: name}
		for _, op := range ops {
			err := a.Insert(arbiter.StatementCoord{Account: op.acct, ClientSeq: op.seq})
			s.Ops = append(s.Ops, vecOp{
				Account: op.acct, Seq: op.seq,
				Expect: outcomeString(err),
				Root:   hex.EncodeToString(a.Root()),
			})
		}
		return a, s
	}

	type op = struct {
		acct string
		seq  uint64
	}
	accA, seqA := runSeq("single-account lifecycle", []op{
		{acctA, 1},        // fresh start
		{acctA, 2},        // fast path
		{acctA, 10},       // jump -> gap [3,9]
		{acctA, 3},        // start-edge fill -> [4,9]
		{acctA, 9},        // end-edge fill -> [4,8]
		{acctA, 6},        // split -> [4,5],[7,8]
		{acctA, 6},        // duplicate (root unchanged)
		{acctA, 0},        // seq zero (root unchanged)
	})
	accM, seqM := runSeq("two accounts, permuted equivalence base", []op{
		{acctA, 1}, {acctB, 5}, {acctA, 3}, {acctB, 2},
	})
	_, seqP := runSeq("two accounts, permuted order (same set)", []op{
		{acctB, 5}, {acctB, 2}, {acctA, 1}, {acctA, 3},
	})
	// gap-budget sequence: alternating jumps to K ranges, then rejection.
	budgetOps := make([]op, 0, MaxOpenGapRanges+1)
	for i := 0; i < MaxOpenGapRanges; i++ {
		budgetOps = append(budgetOps, op{acctA, uint64(2 * (i + 1))})
	}
	budgetOps = append(budgetOps, op{acctA, uint64(2*MaxOpenGapRanges + 3)}) // would create range K+1
	_, seqB := runSeq("gap budget exhaustion", budgetOps)

	vf.Sequences = []vecSequence{seqA, seqM, seqP, seqB}

	mustProve := func(a *SpentIDs, acct string, seq uint64) Proof {
		p, err := a.ProveNonMembership(arbiter.StatementCoord{Account: acct, ClientSeq: seq})
		if err != nil {
			panic(err)
		}
		return p
	}
	pAbsent := mustProve(accA, acctB, 1)  // account absent in seqA's final state
	pFresh := mustProve(accA, acctA, 99)  // beyond hi
	pGap := mustProve(accA, acctA, 4)     // inside [4,5]
	pGapM := mustProve(accM, acctB, 3)    // in acctB's gap under seqM
	tampered := append(Proof(nil), pGap...)
	tampered[len(tampered)-1] ^= 0x01
	vf.Proofs = []vecProof{
		{Sequence: seqA.Name, Account: acctB, Seq: 1, Proof: hex.EncodeToString(pAbsent), Valid: true},
		{Sequence: seqA.Name, Account: acctA, Seq: 99, Proof: hex.EncodeToString(pFresh), Valid: true},
		{Sequence: seqA.Name, Account: acctA, Seq: 4, Proof: hex.EncodeToString(pGap), Valid: true},
		{Sequence: seqA.Name, Account: acctA, Seq: 1, Proof: hex.EncodeToString(pGap), Valid: false},  // spent coord
		{Sequence: seqA.Name, Account: acctA, Seq: 4, Proof: hex.EncodeToString(tampered), Valid: false}, // tampered
		{Sequence: seqM.Name, Account: acctB, Seq: 3, Proof: hex.EncodeToString(pGapM), Valid: true},
	}
	return vf
}

func itoa(i int) string {
	return fmt.Sprintf("%d", i)
}

const vectorPath = "testdata/spent_ids_vectors.json"

func TestVectors(t *testing.T) {
	if *update {
		vf := buildVectors()
		data, err := json.MarshalIndent(vf, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(vectorPath), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(vectorPath, append(data, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(vectorPath)
	if err != nil {
		t.Fatalf("read vectors (run with -update once to generate): %v", err)
	}
	var vf vectorFile
	if err := json.Unmarshal(raw, &vf); err != nil {
		t.Fatal(err)
	}
	if vf.Profile != ProfileID {
		t.Fatalf("vector profile %q != %q", vf.Profile, ProfileID)
	}

	// Primitives.
	if vf.Primitives.EmptyRoot != hex.EncodeToString(EmptyRoot()) {
		t.Fatal("empty_root drifted")
	}
	for lvl, want := range vf.Primitives.Defaults {
		l := mustAtoi(t, lvl)
		if got := hex.EncodeToString(defaults[l][:]); got != want {
			t.Fatalf("defaults[%d] drifted", l)
		}
	}
	for _, kv := range vf.Primitives.Keys {
		k := hashKey(kv.Account)
		if hex.EncodeToString(k[:]) != kv.Key {
			t.Fatalf("key(%q) drifted", kv.Account)
		}
	}
	for _, lf := range vf.Primitives.Leaves {
		v := hashLeafVal(lf.Account, lf.Hi, lf.Ranges)
		if hex.EncodeToString(v[:]) != lf.LeafVal {
			t.Fatalf("leafval(%q) drifted", lf.Account)
		}
	}

	// Sequences replay: outcomes and per-op roots must match exactly.
	finals := map[string]*SpentIDs{}
	for _, seq := range vf.Sequences {
		a := NewSpentIDs()
		for i, op := range seq.Ops {
			err := a.Insert(arbiter.StatementCoord{Account: op.Account, ClientSeq: op.Seq})
			if got := outcomeString(err); got != op.Expect {
				t.Fatalf("%s op %d: outcome %s want %s", seq.Name, i, got, op.Expect)
			}
			if got := hex.EncodeToString(a.Root()); got != op.Root {
				t.Fatalf("%s op %d: root drifted", seq.Name, i)
			}
		}
		finals[seq.Name] = a
	}
	// The permuted pair must land on the same final root.
	m, p := vf.Sequences[1], vf.Sequences[2]
	if m.Ops[len(m.Ops)-1].Root != p.Ops[len(p.Ops)-1].Root {
		t.Fatal("permuted sequences ended on different roots")
	}

	// Proof vectors.
	for i, pv := range vf.Proofs {
		a, ok := finals[pv.Sequence]
		if !ok {
			t.Fatalf("proof %d references unknown sequence %q", i, pv.Sequence)
		}
		proofBytes, err := hex.DecodeString(pv.Proof)
		if err != nil {
			t.Fatalf("proof %d hex: %v", i, err)
		}
		got := Verify(a.Root(), arbiter.StatementCoord{Account: pv.Account, ClientSeq: pv.Seq}, Proof(proofBytes))
		if got != pv.Valid {
			t.Fatalf("proof %d: Verify=%v want %v", i, got, pv.Valid)
		}
	}
}

func mustAtoi(t *testing.T, s string) int {
	t.Helper()
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("bad level key %q", s)
	}
	return n
}
```

- [ ] **Step 2: Generate the vector file, then verify the frozen replay passes**

Run: `go test ./accumulator/ -run TestVectors -update && go test ./accumulator/ -run TestVectors -v`
Expected: first run writes `accumulator/testdata/spent_ids_vectors.json`; second run PASSES against the frozen file. Eyeball the JSON: `empty_root` must equal the Global Constraints literal `b4aeabf8…4120`, and the `key`/`leafval` entries for `0x1111…1111` must match the frozen literals.

- [ ] **Step 3: Commit**

```bash
git add accumulator/vectors_test.go accumulator/testdata/spent_ids_vectors.json
git commit -m "test(accumulator): frozen sentio-spent-ids-v1 JSON vectors + replay harness

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: Property tests, fuzz, README, push

**Files:**
- Create: `accumulator/property_test.go`, `accumulator/fuzz_test.go`
- Modify: `README.md` (the `accumulator/` layout row)

**Interfaces:**
- Consumes: the complete package.
- Produces: the P0b definition-of-done: reference-model equivalence, order-independence at scale, fuzz safety; branch pushed.

- [ ] **Step 1: Write `accumulator/property_test.go`**

```go
package accumulator

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/sentioxyz/arbiter"
)

// refSpent is the naive reference model: explicit spent sets.
type refSpent map[string]map[uint64]bool

func (m refSpent) insert(acct string, seq uint64) string {
	if seq == 0 {
		return "seq_zero"
	}
	s := m[acct]
	if s == nil {
		s = map[uint64]bool{}
		m[acct] = s
	}
	if s[seq] {
		return "duplicate"
	}
	// gap budget is a policy of the real implementation; the reference
	// only decides membership — budget rejections are cross-checked
	// against the real accumulator's state size instead.
	s[seq] = true
	return "ok"
}

func TestPropertyAgainstReferenceModel(t *testing.T) {
	for _, seed := range []int64{1, 2, 3, 4, 5} {
		rng := rand.New(rand.NewSource(seed))
		a := NewSpentIDs()
		ref := refSpent{}
		accounts := []string{"0xa", "0xb", "0xc"}
		for op := 0; op < 3000; op++ {
			acct := accounts[rng.Intn(len(accounts))]
			seq := uint64(rng.Intn(120)) // dense: exercises fills/splits/dups
			err := a.Insert(arbiter.StatementCoord{Account: acct, ClientSeq: seq})
			want := ""
			switch {
			case err == nil:
				want = "ok"
			case err == ErrSeqZero:
				want = "seq_zero"
			case err == ErrSpentDuplicate:
				want = "duplicate"
			case err == ErrGapBudgetExceeded:
				want = "gap_budget"
			default:
				t.Fatalf("unexpected error: %v", err)
			}
			refOut := ref.insert(acct, seq)
			if want == "gap_budget" {
				// The reference has no budget: undo its acceptance and
				// verify the real state genuinely was at the K limit.
				delete(ref[acct], seq)
				if _, ranges, ok := a.AccountState(acct); !ok || len(ranges) < MaxOpenGapRanges-1 {
					t.Fatalf("seed %d op %d: budget rejection with %d ranges", seed, op, len(ranges))
				}
				continue
			}
			if want != refOut {
				t.Fatalf("seed %d op %d (%s,%d): impl=%s ref=%s", seed, op, acct, seq, want, refOut)
			}
			// Membership classification agrees on a random probe.
			probe := uint64(rng.Intn(130))
			st := a.Status(arbiter.StatementCoord{Account: acct, ClientSeq: probe})
			spentInRef := probe != 0 && ref[acct][probe]
			if spentInRef != (st == StatusSpentDuplicate) {
				t.Fatalf("seed %d op %d: status(%d)=%v ref-spent=%v", seed, op, probe, st, spentInRef)
			}
		}
		// Every accepted coordinate re-inserted must reject as duplicate,
		// and a fresh accumulator replaying the accepted set in sorted
		// order (always fill-legal: per-account ascending) matches roots.
		b := NewSpentIDs()
		for _, acct := range accounts {
			seqs := make([]uint64, 0, len(ref[acct]))
			for q := range ref[acct] {
				seqs = append(seqs, q)
			}
			sortUint64(seqs)
			for _, q := range seqs {
				if err := b.Insert(arbiter.StatementCoord{Account: acct, ClientSeq: q}); err != nil {
					t.Fatalf("replay insert: %v", err)
				}
			}
		}
		if !bytes.Equal(a.Root(), b.Root()) {
			t.Fatalf("seed %d: root depends on insertion order", seed)
		}
		// Snapshot round-trip preserves the root.
		var buf bytes.Buffer
		if err := a.Snapshot(&buf); err != nil {
			t.Fatal(err)
		}
		c := NewSpentIDs()
		if err := c.Restore(&buf); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(a.Root(), c.Root()) {
			t.Fatalf("seed %d: snapshot round-trip changed root", seed)
		}
		// Proofs for a sample of unspent coordinates verify; spent don't prove.
		for i := 0; i < 50; i++ {
			acct := accounts[rng.Intn(len(accounts))]
			seq := uint64(1 + rng.Intn(200))
			cd := arbiter.StatementCoord{Account: acct, ClientSeq: seq}
			if ref[acct][seq] {
				if _, err := a.ProveNonMembership(cd); err == nil {
					t.Fatalf("proved spent (%s,%d)", acct, seq)
				}
				continue
			}
			p, err := a.ProveNonMembership(cd)
			if err != nil {
				t.Fatalf("prove (%s,%d): %v", acct, seq, err)
			}
			if !Verify(a.Root(), cd, p) {
				t.Fatalf("honest proof rejected (%s,%d)", acct, seq)
			}
		}
	}
}

func sortUint64(s []uint64) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}
```

- [ ] **Step 2: Write `accumulator/fuzz_test.go`**

```go
package accumulator

import (
	"bytes"
	"testing"

	"github.com/sentioxyz/arbiter"
)

// FuzzVerify: arbitrary proof bytes must never panic and must never verify
// true against an honest root except for the honest proof itself (the
// canonical encoding is unique per coordinate).
func FuzzVerify(f *testing.F) {
	a := NewSpentIDs()
	for _, c := range []arbiter.StatementCoord{
		{Account: "0xaa", ClientSeq: 1},
		{Account: "0xaa", ClientSeq: 10},
		{Account: "0xbb", ClientSeq: 3},
	} {
		if err := a.Insert(c); err != nil {
			f.Fatal(err)
		}
	}
	root := a.Root()
	target := arbiter.StatementCoord{Account: "0xaa", ClientSeq: 5}
	honest, err := a.ProveNonMembership(target)
	if err != nil {
		f.Fatal(err)
	}
	f.Add([]byte(honest))
	f.Add([]byte{})
	f.Add([]byte{1, 1})
	f.Fuzz(func(t *testing.T, data []byte) {
		if Verify(root, target, Proof(data)) && !bytes.Equal(data, honest) {
			t.Fatalf("forged proof accepted: %x", data)
		}
	})
}
```

- [ ] **Step 3: Run the new tests + a bounded fuzz pass**

Run: `go test ./accumulator/ -run 'TestProperty' -v && go test ./accumulator/ -run FuzzVerify -fuzz FuzzVerify -fuzztime 30s`
Expected: property PASS (5 seeds × 3000 ops); fuzz runs 30s with zero failures.

- [ ] **Step 4: Update the README layout row**

In `README.md`, change the `accumulator/` row to:

```markdown
| `accumulator/` | statement_id uniqueness accumulator (§6): frozen `sentio-spent-ids-v1` profile, account-granularity SMT, non-membership proofs, snapshot serialization, JSON test vectors (`testdata/`). |
```

- [ ] **Step 5: Full-module verification**

Run: `go build ./... && go test ./... && go vet ./... && gofmt -l .`
Expected: all green; gofmt output empty.

- [ ] **Step 6: Commit and push**

```bash
git add accumulator/property_test.go accumulator/fuzz_test.go README.md
git commit -m "test(accumulator): reference-model properties + Verify fuzzing

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

Expected: push succeeds; `git ls-remote origin main` matches local HEAD.

---

## Self-Review Notes (spec coverage)

- Spec §2 construction → Tasks 3–4. §3 encodings → Task 1 (+ frozen literals). §4 transitions + K → Task 2. §5 proofs → Task 5. §6 API + snapshot → Tasks 4, 6. §8 governance → Task 1 constant block, no config surface anywhere. §9 vectors/property/fuzz → Tasks 7–8. §10 boundary respected: no FSM wiring, no proto changes, no housegate profile edits.
- The §9 "non-canonical range list must reject" and "explicit default sibling must reject" vector classes are covered as unit tests (Task 5) rather than JSON vectors — encoded forgeries embed implementation-internal path data that would make the JSON brittle; the JSON carries the tampered-byte and spent-coordinate negative cases.

## Follow-ups recorded for P1a (not this plan)

- Wire `SpentIDs` into the FSM (§6.3 admission via `Status` + `Insert`; FSM snapshot via `Snapshot`/`Restore`).
- `ADMISSION_CODE_GAP_BUDGET_EXCEEDED` enum append in arbiter-proto v0.2.0.
- Leader-side proof-serving RPC surface (decentralized phase; `ProveNonMembership` is ready).
