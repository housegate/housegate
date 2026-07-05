# Sentio Arbiter P1a — Replicated Core (FSM + raftnode) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the Arbiter's deterministic replicated core per [2026-07-05-arbiter-p1a-fsm-raftnode-design.md](../specs/2026-07-05-arbiter-p1a-fsm-raftnode-design.md): the full 17-command FSM (`Apply`/`Snapshot`/`Restore`), §6.3 admission over the P0b accumulator primitives, deterministic source/verifier selection, the three-way predicate with closure — plus the `raftnode` assembly behind the frozen `ConsensusNode` seam, arbiter-proto v0.2.0, the arbiter repo CI, and the `authority.Validator` MaxTokenAge fail-closed fix.

**Architecture:** Three-layer structure (design §2): root-package canonical Go mirror types (JSON tags = proto field names), a `wire` package owning ALL pb ⇄ Go conversion (nil/empty normalization frozen there), and a pure `fsm` package that never imports `gen/pb` — the §4.3/§13 determinism boundary is an import-graph property enforced by CI. `raftnode` wraps `hashicorp/raft` with fully-injected stores/transport.

**Tech Stack:** Go 1.26.3, hashicorp/raft v1.7.3, arbiter-proto v0.2.0 (built in Task 1), housegate `pkg/replay` + `pkg/lthash` via module replace, go-ethereum crypto (secp256k1), stdlib `crypto/ed25519`. Tests: Go stdlib testing only (no testify — repo convention).

## Global Constraints

- **Two working repos.** arbiter-proto: `/Users/uranuswch/src/sentio_xyz/arbiter-proto`. arbiter: `/Users/uranuswch/src/sentio_xyz/arbiter`. Every task states its repo. housegate (`/Users/uranuswch/Dev/housegate/housegate`) receives documentation only — zero code change.
- **Commits:** conventional style (`feat:`/`fix:`/`test:`/`build:`/`ci:`), pushed directly to `main` in both sentioxyz repos (P0/P0b precedent). End every commit message with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **English** code/comments/log messages. Markdown docs: no hard line-wrapping.
- **Determinism red lines in `fsm/`** (design §2, §13): no `time.Now`, no `rand`, no network I/O, no output derived from map iteration order (pure AND-folds over maps are fine), and `fsm` must never import `arbiter-proto/gen/pb`. CI enforces the last two mechanically (Task 16).
- **Single hash profile:** every digest goes through `replay.CanonicalDigest` / `replay.DigestString` or the frozen P0b accumulator profile. New frozen domains: `arbiter-l3-header-v1`, `arbiter-byte-side-scan-v1`. Frozen selection-seed prefixes: `arbiter-source-select-v1:`, `arbiter-verifier-select-v1:`.
- **Frozen consensus constants, never config:** verifier quorum = 2, verifier selection = 3, admitted statement kind = INSERT.
- **Rejected-but-committed:** a command that fails validation leaves state unchanged and returns a typed rejection VALUE through the `ApplyFuture` — never a Go error for a domain rejection, so every replica computes the identical result.
- **Wall-clock-free Apply:** all signature verification inside `Apply` skips iat/expiry checks; freshness belongs to the leader ingress edge (P1b) and the SNode-side Validator.
- **nil/empty rule (frozen at the wire seam):** the canonical Go form uses `nil` for an empty repeated field and a `nil` pointer for an absent message.
- **After every task:** `go test ./...` green in the task's repo; `gofmt -l .` (excluding `gen/`) empty.
- ed25519 evidence signatures are lowercase hex WITHOUT `0x` prefix, over the hash string bytes as-is (matches `payloadexec.Ed25519Signer`: `hex.EncodeToString(ed25519.Sign(priv, []byte(hashString)))`). Verification tolerates an optional `0x` prefix via TrimPrefix.
- LtHash scalars (`part_row_lthash`, `new_parts_lthash_sum`, `base_partition_root`, `post_partition_commitment`) are `"0x" + hex` of the raw 2048-byte accumulator.

## File Structure (final state)

```text
arbiter-proto/                          # Task 1
  proto/arbiter.proto                   # +ADMISSION_CODE_GAP_BUDGET_EXCEEDED = 8
  gen/pb/*.pb.go                        # regenerated

arbiter/                                # Tasks 2–15
  authority/validator.go                # Task 2: MaxTokenAge fail-closed
  authority/authority_test.go           # Task 2: zero/negative-age tests
  authority/payload.go                  # Task 12: export PromoteCommandHash/CleanupCommandHash
  types.go                              # Task 3: mirror types + enums + TablePartition TextMarshaler
  types_test.go                         # Task 3: Flat/Coord/Body/TextMarshaler tests
  domains.go                            # Task 3: frozen domain + seed constants
  conformance/arbiter_wire_test.go      # Task 3: mirror-tag + enum-number conformance
  wire/command.go                       # Task 4: Command union + Encode/Decode
  wire/convert.go                       # Task 4: pb ⇄ Go converters + mapSlice normalization
  wire/wire_test.go                     # Task 4: round-trips + normalization pins
  fsm/state.go                          # Task 5: State/Params/lifecycle/header/verdict types
  fsm/fsm.go                            # Task 5: FSM, New, Apply dispatch, results, Summary
  fsm/apply.go                          # Tasks 5,8,9,12,13: non-admission handlers
  fsm/fsm_test.go                       # Task 5: helpers (mustEncode/mkLog/newTestFSM) + membership tests
  fsm/snapshot.go                       # Task 6: container + Persist/Restore
  fsm/snapshot_test.go                  # Task 6: round-trip
  fsm/admission.go                      # Task 7: applySubmitStatement pipeline
  fsm/userjws.go                        # Task 7: wall-clock-free user JWS verification
  fsm/select.go                         # Tasks 7–8: u64FromDigest, selectSource, selectVerifiers
  fsm/admission_test.go                 # Task 7: admission table + selection determinism
  fsm/seal_test.go                      # Task 8: chain + verifier-selection tests
  fsm/rc_test.go                        # Task 9: late binding
  fsm/evidence_test.go                  # Task 10: attestation/scan recording
  fsm/threeway.go                       # Task 11: evaluation + verdict + flip
  fsm/threeway_test.go                  # Task 11: fixtures
  fsm/authorityjws.go                   # Task 12: wall-clock-free authority JWS verification
  fsm/promotion_test.go                 # Tasks 12–13: issuance/ack/closure/cleanup/challenge
  fsm/determinism_test.go               # Task 14: N-FSM equality + midpoint snapshot + mini e2e
  raftnode/node.go                      # Task 15: hashicorp/raft assembly
  raftnode/node_test.go                 # Task 15: 3-node inmem cluster tests
  .github/workflows/ci.yml              # Task 16: build+vet+test+tripwires
  README.md                             # Task 16: fsm/raftnode section
  go.mod                                # Task 3: arbiter-proto → v0.2.0

housegate/docs/superpowers/plans/2026-07-05-arbiter-p1a-fsm-raftnode.md   # this plan (committed with the spec PR)
```

Task order: 1 → 2 → 3 → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13 → 14 → 15 → 16 (strictly sequential; each task's Interfaces block lists what it consumes from earlier tasks).

---

### Task 1: arbiter-proto v0.2.0 — `ADMISSION_CODE_GAP_BUDGET_EXCEEDED` append

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter-proto`

**Files:**
- Modify: `proto/arbiter.proto` (the `AdmissionCode` enum)
- Regenerate: `gen/pb/arbiter.pb.go`

**Interfaces:**
- Consumes: the frozen v0.1.0 `AdmissionCode` enum (values 0–7).
- Produces: `pb.AdmissionCode_ADMISSION_CODE_GAP_BUDGET_EXCEEDED` (= 8) and git tag `v0.2.0`; Task 3 bumps the arbiter repo to it.

- [ ] **Step 1: Append the enum value**

In `proto/arbiter.proto`, find the `AdmissionCode` enum and append after `ADMISSION_CODE_MALFORMED = 7;`:

```protobuf
  // The account's open gap-range budget (K = 64, frozen in the
  // sentio-spent-ids-v1 profile) would be exceeded. A jump adds one open
  // range; a mid-range fill splits one into two. Remedy: fill from a
  // range's EDGES (never increases the count), or continue from the
  // high-water mark. Distinct from DUPLICATE_CLIENT_SEQ: the coordinate is
  // unspent, the account's gap state is what rejects it.
  ADMISSION_CODE_GAP_BUDGET_EXCEEDED = 8;
```

- [ ] **Step 2: Regenerate and verify**

```bash
cd /Users/uranuswch/src/sentio_xyz/arbiter-proto
make tools && make proto
buf lint
buf breaking --against '.git#branch=main'
go build ./... && go test ./...
git diff --stat -- gen/
```

Expected: lint clean; breaking check passes (enum value append is compatible); build/test green; `gen/pb/arbiter.pb.go` shows the new value.

- [ ] **Step 3: Commit, push, tag**

```bash
git add proto/arbiter.proto gen/
git commit -m "feat(proto): append ADMISSION_CODE_GAP_BUDGET_EXCEEDED (P0b K=64 budget)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
git tag v0.2.0 && git push origin v0.2.0
```

Expected: `git ls-remote --tags origin | grep v0.2.0` shows the tag.

---

### Task 2: `authority.Validator` MaxTokenAge fail-closed

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Modify: `authority/validator.go` (the `authorize` method + the `MaxTokenAge` field comment)
- Test: `authority/authority_test.go`

**Interfaces:**
- Consumes: existing `Validator{AllowedAddresses, MaxTokenAge}`, `Signer` from P0a.
- Produces: `authorize()` rejects everything when `MaxTokenAge <= 0`. Task 12's fsm-side verification does NOT use this method (it is wall-clock-free); this fix protects the P1c SNode consumer.

- [ ] **Step 1: Write the failing tests**

Append to `authority/authority_test.go` (reuse the file's existing helper style — it constructs a `Signer` via `NewSignerFromHex` and a `Validator` literal; follow the existing `TestAuthorizePromotion*` tests' fixture setup):

```go
func TestAuthorize_ZeroMaxTokenAgeFailsClosed(t *testing.T) {
	s, v := newSignerAndValidator(t) // if no such helper exists, inline the same setup the existing happy-path test uses
	v.MaxTokenAge = 0
	cmd := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1}
	token, err := s.SignPromotion(cmd)
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	if _, err := v.AuthorizePromotion(cmd, token); err == nil {
		t.Fatal("MaxTokenAge=0 must fail closed (never-expiring tokens are the empty-allowlist zero-value trap)")
	}
}

func TestAuthorize_NegativeMaxTokenAgeFailsClosed(t *testing.T) {
	s, v := newSignerAndValidator(t)
	v.MaxTokenAge = -time.Minute
	cmd := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1}
	token, err := s.SignPromotion(cmd)
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	if _, err := v.AuthorizePromotion(cmd, token); err == nil {
		t.Fatal("negative MaxTokenAge must fail closed")
	}
}
```

If the existing tests build validators inline (no `newSignerAndValidator` helper), add that small helper in the test file returning the signer and a `*Validator` with the signer's address allowlisted and `MaxTokenAge: time.Minute`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/uranuswch/src/sentio_xyz/arbiter && go test ./authority/ -run 'TestAuthorize_.*MaxTokenAge' -v`
Expected: both FAIL (zero currently means "no age check", tokens verify).

- [ ] **Step 3: Implement the fail-closed guard**

In `authority/validator.go`, at the top of `authorize` (right after the empty-allowlist guard):

```go
	if v.MaxTokenAge <= 0 {
		return "", fmt.Errorf("authority validator: MaxTokenAge must be positive (fail-closed; a zero value would mean never-expiring tokens)")
	}
```

And replace the `MaxTokenAge` field comment:

```go
	// MaxTokenAge caps iat age and MUST be positive: a non-positive value
	// fails closed (every token rejected), the same shape as the empty
	// allowlist — a zero value must never silently mean "no expiry".
	// Promotion re-sends after failover re-sign, so short ages are safe
	// (§10.2).
	MaxTokenAge time.Duration
```

The existing age check `if v.MaxTokenAge > 0 && ...` can stay as-is (the guard above makes the condition always true) — simplify it to drop the `> 0` test:

```go
	if now-payload.Iat > int64(v.MaxTokenAge.Seconds())+clockSkewToleranceSeconds {
		return "", fmt.Errorf("authority token expired")
	}
```

- [ ] **Step 4: Run the full authority package**

Run: `go test ./authority/ -v`
Expected: ALL PASS, including the two new tests. If an existing test constructed a Validator without MaxTokenAge (e.g. `&Validator{MaxTokenAge: time.Minute}` at line ~126 sets it, but check for any literal omitting it), give it `MaxTokenAge: time.Minute` — that is the point of the fix.

- [ ] **Step 5: Commit**

```bash
git add authority/
git commit -m "fix(authority): MaxTokenAge <= 0 fails closed in Validator.authorize

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 3: root mirror types, frozen domains, proto bump, conformance

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Modify: `go.mod` (arbiter-proto → v0.2.0), `types.go` (append mirror types)
- Create: `domains.go`, `types_test.go` additions, `conformance/arbiter_wire_test.go`

**Interfaces:**
- Consumes: Task 1's `v0.2.0` tag; existing `StatementCoord`, `TablePartition`, `PartRef`, `PromoteSafePartition`, `UnsafeCleanup`, `StatementIDString`.
- Produces (used by every later task): `StatementID` (+ `Flat()`, `Coord()`), `StatementEnvelope`, `StatementKind` (+ `StatementKindInsert`), `AdmissionCode` (+ constants 0–8), `CandidatePart`, `PartitionLtHashSum`, `RCRecord`, `PartScan`, `ByteSideScanMsg` (+ `Body()`), `ByteSideScanBody`, `AnchorRef`, `NodeRole` (+ constants), `NodeRegistration`, `SafePartMapping`, `PromotionAck`, `CleanupAck`; `TablePartition.MarshalText/UnmarshalText`; constants `DomainL3Header`, `DomainByteSideScan`, `SourceSelectSeedPrefix`, `VerifierSelectSeedPrefix`.
- Note: the design §3 mirror list omitted `PromotionAck`/`SafePartMapping`/`CleanupAck`; they are required by the `RecordPromotionAck`/`RecordCleanupAck` commands and are added here (deliberate plan-level completion, same convention).

- [ ] **Step 1: Bump arbiter-proto**

```bash
cd /Users/uranuswch/src/sentio_xyz/arbiter
go get github.com/sentioxyz/arbiter-proto@v0.2.0 && go mod tidy
go build ./...
```

Expected: go.mod requires `github.com/sentioxyz/arbiter-proto v0.2.0`; build green.

- [ ] **Step 2: Write the failing conformance test**

Create `conformance/arbiter_wire_test.go`:

```go
package conformance

import (
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter"
)

// assertMirror pins a canonical Go mirror struct to its pb message: the set
// of json tags must equal the set of proto field names. This is the same
// freeze discipline as replay_wire_test.go, mechanized.
func assertMirror(t *testing.T, goValue any, msg proto.Message) {
	t.Helper()
	goTags := map[string]bool{}
	rt := reflect.TypeOf(goValue)
	for i := 0; i < rt.NumField(); i++ {
		tag := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			t.Fatalf("%s.%s: every mirror field needs a json tag", rt.Name(), rt.Field(i).Name)
		}
		goTags[tag] = true
	}
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if !goTags[name] {
			t.Errorf("%s: proto field %q has no Go mirror json tag", rt.Name(), name)
		}
		delete(goTags, name)
	}
	for tag := range goTags {
		t.Errorf("%s: Go json tag %q has no proto field", rt.Name(), tag)
	}
}

func TestArbiterMirrorsMatchProto(t *testing.T) {
	assertMirror(t, arbiter.StatementID{}, &pb.StatementID{})
	assertMirror(t, arbiter.StatementEnvelope{}, &pb.StatementEnvelopeV2{})
	assertMirror(t, arbiter.CandidatePart{}, &pb.CandidatePart{})
	assertMirror(t, arbiter.PartitionLtHashSum{}, &pb.PartitionLtHashSum{})
	assertMirror(t, arbiter.RCRecord{}, &pb.RCRecord{})
	assertMirror(t, arbiter.PartScan{}, &pb.PartScan{})
	assertMirror(t, arbiter.ByteSideScanMsg{}, &pb.ByteSideScanMsg{})
	assertMirror(t, arbiter.AnchorRef{}, &pb.AnchorRef{})
	assertMirror(t, arbiter.NodeRegistration{}, &pb.NodeRegistration{})
	assertMirror(t, arbiter.SafePartMapping{}, &pb.SafePartMapping{})
	assertMirror(t, arbiter.PromotionAck{}, &pb.PromotionAck{})
	assertMirror(t, arbiter.CleanupAck{}, &pb.CleanupAck{})
}

func TestEnumNumbersMatchProto(t *testing.T) {
	if int32(arbiter.AdmissionCodeUnspecified) != int32(pb.AdmissionCode_ADMISSION_CODE_UNSPECIFIED) ||
		int32(arbiter.AdmissionCodeAccepted) != int32(pb.AdmissionCode_ADMISSION_CODE_ACCEPTED) ||
		int32(arbiter.AdmissionCodeDuplicateClientSeq) != int32(pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ) ||
		int32(arbiter.AdmissionCodeSchemaNotAllowed) != int32(pb.AdmissionCode_ADMISSION_CODE_SCHEMA_NOT_ALLOWED) ||
		int32(arbiter.AdmissionCodeKindNotAdmitted) != int32(pb.AdmissionCode_ADMISSION_CODE_KIND_NOT_ADMITTED) ||
		int32(arbiter.AdmissionCodeInvalidSignature) != int32(pb.AdmissionCode_ADMISSION_CODE_INVALID_SIGNATURE) ||
		int32(arbiter.AdmissionCodeInvalidProof) != int32(pb.AdmissionCode_ADMISSION_CODE_INVALID_PROOF) ||
		int32(arbiter.AdmissionCodeMalformed) != int32(pb.AdmissionCode_ADMISSION_CODE_MALFORMED) ||
		int32(arbiter.AdmissionCodeGapBudgetExceeded) != int32(pb.AdmissionCode_ADMISSION_CODE_GAP_BUDGET_EXCEEDED) {
		t.Fatal("AdmissionCode Go constants drifted from pb enum numbers")
	}
	if int32(arbiter.NodeRoleUnspecified) != int32(pb.NodeRole_NODE_ROLE_UNSPECIFIED) ||
		int32(arbiter.NodeRoleVerifier) != int32(pb.NodeRole_NODE_ROLE_VERIFIER) ||
		int32(arbiter.NodeRoleSNode) != int32(pb.NodeRole_NODE_ROLE_SNODE) {
		t.Fatal("NodeRole Go constants drifted from pb enum numbers")
	}
	if int32(arbiter.StatementKindUnspecified) != int32(pb.StatementKind_STATEMENT_KIND_UNSPECIFIED) ||
		int32(arbiter.StatementKindInsert) != int32(pb.StatementKind_STATEMENT_KIND_INSERT) {
		t.Fatal("StatementKind Go constants drifted from pb enum numbers")
	}
}
```

- [ ] **Step 3: Run to verify it fails to compile**

Run: `go test ./conformance/ 2>&1 | head -5`
Expected: compile errors — `arbiter.StatementID` etc. undefined.

- [ ] **Step 4: Append the mirror types to `types.go`**

Append to `types.go` (keep the existing header/types untouched; add `"bytes"`, `"fmt"` to imports):

```go
// ---- Canonical Go mirror types (P1a, design §3) ----
//
// JSON tags are byte-equal to the arbiter-proto field names and are pinned
// by conformance/arbiter_wire_test.go. These structs are the canonical
// forms: FSM state, canonical hashing (via replay.CanonicalDigest), and
// authority signing all use them; proto is transport only (§4.3).

// StatementKind mirrors pb.StatementKind. v1 admits INSERT only.
type StatementKind int32

const (
	StatementKindUnspecified StatementKind = 0
	StatementKindInsert      StatementKind = 1
)

// AdmissionCode mirrors pb.AdmissionCode (numbers pinned by conformance).
type AdmissionCode int32

const (
	AdmissionCodeUnspecified        AdmissionCode = 0
	AdmissionCodeAccepted           AdmissionCode = 1
	AdmissionCodeDuplicateClientSeq AdmissionCode = 2
	AdmissionCodeSchemaNotAllowed   AdmissionCode = 3
	AdmissionCodeKindNotAdmitted    AdmissionCode = 4
	AdmissionCodeInvalidSignature   AdmissionCode = 5
	AdmissionCodeInvalidProof       AdmissionCode = 6
	AdmissionCodeMalformed          AdmissionCode = 7
	// AdmissionCodeGapBudgetExceeded: the P0b K=64 open-range budget
	// (arbiter-proto v0.2.0 append).
	AdmissionCodeGapBudgetExceeded AdmissionCode = 8
)

// NodeRole mirrors pb.NodeRole.
type NodeRole int32

const (
	NodeRoleUnspecified NodeRole = 0
	NodeRoleVerifier    NodeRole = 1
	NodeRoleSNode       NodeRole = 2
)

// StatementID is the structured client-assigned statement identity
// (uniqueness key = (client_account, client_seq); nonce is entropy, §6.1).
type StatementID struct {
	ClientAccount string `json:"client_account"`
	ClientSeq     uint64 `json:"client_seq"`
	ClientNonce   string `json:"client_nonce"`
}

// Flat renders the canonical flat statement_id string form.
func (id StatementID) Flat() string {
	return StatementIDString(id.ClientAccount, id.ClientSeq, id.ClientNonce)
}

// Coord is the accumulator uniqueness coordinate (account normalized).
func (id StatementID) Coord() StatementCoord {
	return StatementCoord{Account: strings.ToLower(id.ClientAccount), ClientSeq: id.ClientSeq}
}

// StatementEnvelope is the canonical Go form of pb.StatementEnvelopeV2 (the
// V2 suffix is frozen on the wire only; the Go world drops it).
type StatementEnvelope struct {
	StatementID   StatementID   `json:"statement_id"`
	StatementKind StatementKind `json:"statement_kind"`
	SQL           string        `json:"sql"`
	SQLHash       string        `json:"sql_hash"`
	SettingsHash  string        `json:"settings_hash,omitempty"`
	PayloadRef    string        `json:"payload_ref,omitempty"`
	PayloadHash   string        `json:"payload_hash,omitempty"`
	PayloadLength uint64        `json:"payload_length,omitempty"`
	TargetTableID string        `json:"target_table_id,omitempty"`
	UserJWS       string        `json:"user_jws"`
}

// CandidatePart is one hg_unsafe part the source claims a statement
// produced; part_row_lthash is its identity everywhere downstream.
type CandidatePart struct {
	TableID       string `json:"table_id"`
	PartitionID   string `json:"partition_id"`
	PartName      string `json:"part_name,omitempty"`
	PartRowLtHash string `json:"part_row_lthash"`
	PartPhysHash  string `json:"part_phys_hash,omitempty"`
	RowCount      uint64 `json:"row_count,omitempty"`
	Bytes         uint64 `json:"bytes,omitempty"`
}

// PartitionLtHashSum is the source's claimed per-partition new-part LtHash
// sum — check 2's "claimed" side (§7.3).
type PartitionLtHashSum struct {
	TableID          string `json:"table_id"`
	PartitionID      string `json:"partition_id"`
	NewPartsLtHashSum string `json:"new_parts_lthash_sum"`
}

// RCRecord is the source's result claim (late-bindable by statement_id).
type RCRecord struct {
	StatementID          StatementID          `json:"statement_id"`
	SourceNode           string               `json:"source_node"`
	CandidateParts       []CandidatePart      `json:"candidate_parts"`
	SourceClaimRoot      string               `json:"source_claim_root"`
	PartitionNewPartSums []PartitionLtHashSum `json:"partition_new_part_sums"`
}

// PartScan is one scanned part's byte-side result (check 3).
type PartScan struct {
	TableID              string `json:"table_id"`
	PartitionID          string `json:"partition_id"`
	ClaimedPartRowLtHash string `json:"claimed_part_row_lthash"`
	ScannedPartRowLtHash string `json:"scanned_part_row_lthash"`
	LivePartName         string `json:"live_part_name,omitempty"`
}

// ByteSideScanBody is the canonical hash/sign form of a scan: the message
// minus its own hash and signature. scan_hash = CanonicalDigest(
// DomainByteSideScan, msg.Body()); signature = ed25519 over the scan_hash
// string bytes, hex — the ReplayAttestation convention.
type ByteSideScanBody struct {
	ReplicaID string     `json:"replica_id"`
	BlockSeq  uint64     `json:"block_seq"`
	Parts     []PartScan `json:"parts"`
}

// ByteSideScanMsg mirrors pb.ByteSideScanMsg.
type ByteSideScanMsg struct {
	ReplicaID string     `json:"replica_id"`
	BlockSeq  uint64     `json:"block_seq"`
	Parts     []PartScan `json:"parts"`
	ScanHash  string     `json:"scan_hash"`
	Signature string     `json:"signature"`
}

// Body returns the canonical hash/sign form.
func (m ByteSideScanMsg) Body() ByteSideScanBody {
	return ByteSideScanBody{ReplicaID: m.ReplicaID, BlockSeq: m.BlockSeq, Parts: m.Parts}
}

// AnchorRef references the L2 anchor of one L3 block (§5.2).
type AnchorRef struct {
	L3BlockHash   string `json:"l3_block_hash"`
	StateRoot     string `json:"state_root"`
	L2TxRef       string `json:"l2_tx_ref,omitempty"`
	L2BlockNumber uint64 `json:"l2_block_number,omitempty"`
	DARef         string `json:"da_ref,omitempty"`
}

// NodeRegistration enters a data-plane node into FSM membership.
type NodeRegistration struct {
	NodeID        string     `json:"node_id"`
	Roles         []NodeRole `json:"roles"`
	Ed25519Pubkey []byte     `json:"ed25519_pubkey"`
	DialAddr      string     `json:"dial_addr,omitempty"`
}

// SafePartMapping records where a promoted part landed in hg_safe.
type SafePartMapping struct {
	PartRowLtHash string `json:"part_row_lthash"`
	SafePartName  string `json:"safe_part_name"`
	PartPhysHash  string `json:"part_phys_hash,omitempty"`
}

// PromotionAck reports REPLACE PARTITION completion (§8.3).
type PromotionAck struct {
	NodeID                  string            `json:"node_id"`
	PromotionSeq            uint64            `json:"promotion_seq"`
	TableID                 string            `json:"table_id"`
	PartitionID             string            `json:"partition_id"`
	PostPartitionCommitment string            `json:"post_partition_commitment"`
	Parts                   []SafePartMapping `json:"parts"`
	Applied                 bool              `json:"applied"`
	Detail                  string            `json:"detail,omitempty"`
}

// CleanupAck acknowledges a scheduled unsafe cleanup.
type CleanupAck struct {
	NodeID       string `json:"node_id"`
	PromotionSeq uint64 `json:"promotion_seq"`
	TableID      string `json:"table_id"`
	PartitionID  string `json:"partition_id"`
}

// MarshalText/UnmarshalText let TablePartition key JSON maps in FSM
// snapshots. NUL is the delimiter — table/partition ids never contain it.
func (p TablePartition) MarshalText() ([]byte, error) {
	return []byte(p.TableID + "\x00" + p.PartitionID), nil
}

func (p *TablePartition) UnmarshalText(b []byte) error {
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		return fmt.Errorf("table partition key: missing NUL delimiter")
	}
	p.TableID, p.PartitionID = string(b[:i]), string(b[i+1:])
	return nil
}
```

- [ ] **Step 5: Create `domains.go`**

```go
package arbiter

// Frozen CanonicalDigest domains and deterministic-selection seed prefixes
// (P1a design §3). These are consensus parameters in the P0b §8 sense:
// compile-time constants, no configuration surface, changed only as a new
// versioned value with an explicit migration.
const (
	// DomainL3Header chains L3 block headers: PrevL3Hash =
	// CanonicalDigest(DomainL3Header, header-with-anchor-excluded) (§5).
	DomainL3Header = "arbiter-l3-header-v1"
	// DomainByteSideScan hashes ByteSideScanBody for scan_hash (§7.2).
	DomainByteSideScan = "arbiter-byte-side-scan-v1"
	// SourceSelectSeedPrefix seeds §5.4 deterministic source selection.
	SourceSelectSeedPrefix = "arbiter-source-select-v1:"
	// VerifierSelectSeedPrefix seeds §7.1 deterministic 3-selection.
	VerifierSelectSeedPrefix = "arbiter-verifier-select-v1:"
)
```

- [ ] **Step 6: Add unit tests for the new helpers**

Append to `types_test.go`:

```go
func TestStatementIDFlatAndCoord(t *testing.T) {
	id := StatementID{ClientAccount: "0xABcD", ClientSeq: 7, ClientNonce: "n1"}
	if got, want := id.Flat(), "0xabcd:7:n1"; got != want {
		t.Fatalf("Flat: got %q want %q", got, want)
	}
	if c := id.Coord(); c.Account != "0xabcd" || c.ClientSeq != 7 {
		t.Fatalf("Coord: got %+v", c)
	}
}

func TestByteSideScanBodyExcludesHashAndSignature(t *testing.T) {
	m := ByteSideScanMsg{ReplicaID: "r1", BlockSeq: 3, Parts: []PartScan{{TableID: "db.t"}}, ScanHash: "0xdead", Signature: "beef"}
	b := m.Body()
	if b.ReplicaID != "r1" || b.BlockSeq != 3 || len(b.Parts) != 1 {
		t.Fatalf("Body: got %+v", b)
	}
}

func TestTablePartitionTextRoundTrip(t *testing.T) {
	in := TablePartition{TableID: "db.table", PartitionID: "2026-07"}
	b, err := in.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	var out TablePartition
	if err := out.UnmarshalText(b); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if out != in {
		t.Fatalf("round trip: got %+v want %+v", out, in)
	}
	if err := out.UnmarshalText([]byte("no-delimiter")); err == nil {
		t.Fatal("missing NUL must error")
	}
}
```

- [ ] **Step 7: Run all tests**

Run: `go test ./... 2>&1 | tail -8`
Expected: ALL PASS including `./conformance/`. If `assertMirror` reports a tag/field diff, fix the Go tag (never the proto).

- [ ] **Step 8: Commit**

```bash
git add go.mod go.sum types.go types_test.go domains.go conformance/
git commit -m "feat(types): canonical Go mirrors, frozen domains, arbiter-proto v0.2.0

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 4: `wire` package — pb ⇄ Go seam

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Create: `wire/command.go`, `wire/convert.go`, `wire/wire_test.go`

**Interfaces:**
- Consumes: Task 3 mirror types; `pb.RaftCommand` oneof (17 fields); `replay.ReplayAttestation`/`ExecutionReceipt`/`SafeSnapshotManifest`/`TableManifest`/`PartitionCommitment`/`PartManifestEntry` from housegate.
- Produces (the fsm's ONLY view of the log): `wire.Command` union struct (17 pointer fields, exactly one set), per-command payload structs (`SubmitStatement{Envelope, NonMembershipProof}`, `SealL3Block{}`, `MarkReplaying{BlockSeq}`, `RegisterRC{RC}`, `RecordAttestation{Attestation}`, `RecordByteSideScan{Scan}`, `RecordAnchorFinality{L3BlockSeq, Anchor, FinalityReached, LastMergeableReached}`, `RecordPromotionIssued{Promote, AuthorityJWS}`, `RecordPromotionAck{Ack}`, `PublishSafeSnapshot{Manifest}`, `ScheduleUnsafeCleanup{Cleanup, AuthorityJWS}`, `RecordCleanupAck{Ack}`, `OpenChallenge{BlockSeq, Reason, OpenedBy}`, `ResolveChallenge{BlockSeq, Verdict}`, `RegisterNode{Registration}`, `MarkActive{NodeID}`, `EvictNode{NodeID, Reason}`), `wire.ChallengeVerdict` constants, and `Encode(Command) ([]byte, error)` / `Decode([]byte) (Command, error)`.

- [ ] **Step 1: Write the failing round-trip test**

Create `wire/wire_test.go`:

```go
package wire

import (
	"reflect"
	"testing"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
)

func mustRoundTrip(t *testing.T, in Command) Command {
	t.Helper()
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip diverged:\n in=%+v\nout=%+v", in, out)
	}
	return out
}

func TestRoundTrip_SubmitStatement(t *testing.T) {
	mustRoundTrip(t, Command{SubmitStatement: &SubmitStatement{
		Envelope: arbiter.StatementEnvelope{
			StatementID:   arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"},
			StatementKind: arbiter.StatementKindInsert,
			SQL:           "INSERT INTO t VALUES (1)",
			SQLHash:       "0x11",
			SettingsHash:  "0x22",
			PayloadRef:    "ref",
			PayloadHash:   "0x33",
			PayloadLength: 9,
			TargetTableID: "db.t",
			UserJWS:       "a.b.c",
		},
	}})
}

func TestRoundTrip_AllScalarCommands(t *testing.T) {
	for _, c := range []Command{
		{SealL3Block: &SealL3Block{}},
		{MarkReplaying: &MarkReplaying{BlockSeq: 4}},
		{RecordAnchorFinality: &RecordAnchorFinality{L3BlockSeq: 4, Anchor: arbiter.AnchorRef{L3BlockHash: "0xaa", StateRoot: "0xbb", L2TxRef: "tx", L2BlockNumber: 9}, FinalityReached: true, LastMergeableReached: true}},
		{OpenChallenge: &OpenChallenge{BlockSeq: 4, Reason: "mismatch", OpenedBy: "r2"}},
		{ResolveChallenge: &ResolveChallenge{BlockSeq: 4, Verdict: ChallengeVerdictRejected}},
		{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "n1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier, arbiter.NodeRoleSNode}, Ed25519Pubkey: []byte{1, 2, 3}, DialAddr: "addr"}}},
		{MarkActive: &MarkActive{NodeID: "n1"}},
		{EvictNode: &EvictNode{NodeID: "n1", Reason: "audit"}},
	} {
		mustRoundTrip(t, c)
	}
}

func TestRoundTrip_RCAndEvidence(t *testing.T) {
	rc := arbiter.RCRecord{
		StatementID:     arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"},
		SourceNode:      "s1",
		CandidateParts:  []arbiter.CandidatePart{{TableID: "db.t", PartitionID: "p0", PartName: "all_1_1_0", PartRowLtHash: "0xffee", PartPhysHash: "0x99", RowCount: 2, Bytes: 64}},
		SourceClaimRoot: "0xr00t",
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p0", NewPartsLtHashSum: "0xffee"}},
	}
	mustRoundTrip(t, Command{RegisterRC: &RegisterRC{RC: rc}})

	att := replay.ReplayAttestation{
		ReplicaID: "r1",
		Receipt: replay.ExecutionReceipt{
			BlockSeq: 4, PrevSafeSnapshotID: "s0", PrevStateRoot: "0x0", SchemaSnapshotID: "sch", ExecutorProfileID: "prof",
			StatementRoot: "0xs", PayloadRoot: "0xp", SourceClaimRoot: "0xr00t", ComputedStateRoot: "0xr00t", MatchSourceRoot: true,
			PartitionCommitmentsAfter: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: "0xcc"}},
			AffectedParts:             []replay.PartManifestEntry{{TableID: "db.t", PartitionID: "p0", PartName: "all_1_1_0", PartPhysHash: "0x99", PartRowLtHash: "0xffee", RowCount: 2, Bytes: 64}},
		},
		ReceiptHash: "0xrh", Signature: "aabb", MatchSourceRoot: true,
	}
	mustRoundTrip(t, Command{RecordAttestation: &RecordAttestation{Attestation: att}})

	scan := arbiter.ByteSideScanMsg{ReplicaID: "r1", BlockSeq: 4,
		Parts:    []arbiter.PartScan{{TableID: "db.t", PartitionID: "p0", ClaimedPartRowLtHash: "0xffee", ScannedPartRowLtHash: "0xffee", LivePartName: "all_1_1_0"}},
		ScanHash: "0xsh", Signature: "ccdd"}
	mustRoundTrip(t, Command{RecordByteSideScan: &RecordByteSideScan{Scan: scan}})
}

func TestRoundTrip_PromotionAndManifest(t *testing.T) {
	mustRoundTrip(t, Command{RecordPromotionIssued: &RecordPromotionIssued{
		Promote: arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1, BaseSafeSnapshotID: "s0", BasePartitionRoot: "0x00", CandidateParts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: "0xffee", PartName: "all_1_1_0"}}},
		AuthorityJWS: "x.y.z"}})
	mustRoundTrip(t, Command{RecordPromotionAck: &RecordPromotionAck{Ack: arbiter.PromotionAck{
		NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", PostPartitionCommitment: "0xpost",
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xffee", SafePartName: "all_2_2_0", PartPhysHash: "0x99"}}, Applied: true, Detail: "ok"}}})
	mustRoundTrip(t, Command{ScheduleUnsafeCleanup: &ScheduleUnsafeCleanup{
		Cleanup: arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1, Parts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: "0xffee"}}}, AuthorityJWS: "x.y.z"}})
	mustRoundTrip(t, Command{RecordCleanupAck: &RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0"}}})
	mustRoundTrip(t, Command{PublishSafeSnapshot: &PublishSafeSnapshot{Manifest: replay.SafeSnapshotManifest{
		SnapshotID: "s1", ParentSnapshotID: "s0", SafeBlockSeq: 4, StateRoot: "0xsr", SchemaSnapshotID: "sch", SchemaRoot: "0xschr", ExecutorProfileID: "prof", DataRoot: "0xdr", ManifestRoot: "0xmr",
		Tables: []replay.TableManifest{{TableID: "db.t", SchemaHash: "0xsh",
			PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: "0xcc"}},
			ActiveParts:    []replay.PartManifestEntry{{TableID: "db.t", PartitionID: "p0", PartName: "all_2_2_0", PartPhysHash: "0x99", PartRowLtHash: "0xffee", RowCount: 2, Bytes: 64, StorageRefs: []string{"s3://x"}}}}}}}})
}

// The frozen nil/empty rule: empty repeated fields decode to nil, absent
// messages to nil pointers — the canonical Go form (design §2).
func TestNormalization_EmptyRepeatedDecodesNil(t *testing.T) {
	in := Command{RegisterRC: &RegisterRC{RC: arbiter.RCRecord{
		StatementID:          arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"},
		SourceNode:           "s1",
		CandidateParts:       []arbiter.CandidatePart{},        // empty NON-nil in
		SourceClaimRoot:      "0xr",
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{}}}} // empty NON-nil in
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.RegisterRC.RC.CandidateParts != nil || out.RegisterRC.RC.PartitionNewPartSums != nil {
		t.Fatal("empty repeated fields must normalize to nil on decode")
	}
}

func TestEncode_RejectsZeroOrMultipleCommands(t *testing.T) {
	if _, err := Encode(Command{}); err == nil {
		t.Fatal("empty command must be rejected")
	}
	if _, err := Encode(Command{SealL3Block: &SealL3Block{}, MarkActive: &MarkActive{NodeID: "n"}}); err == nil {
		t.Fatal("two set commands must be rejected")
	}
}

func TestDecode_RejectsGarbageAndEmptyOneof(t *testing.T) {
	if _, err := Decode([]byte{0xff, 0x01, 0x02}); err == nil {
		t.Fatal("garbage bytes must error")
	}
	if _, err := Decode(nil); err == nil {
		t.Fatal("empty RaftCommand (no oneof) must error")
	}
}
```

- [ ] **Step 2: Run to verify compile failure**

Run: `go test ./wire/ 2>&1 | head -5`
Expected: package does not exist / undefined symbols.

- [ ] **Step 3: Implement `wire/command.go`**

```go
// Package wire is the Arbiter's ONLY pb ⇄ Go boundary (design §2). The fsm
// package consumes the decoded Command union and never imports gen/pb —
// canonical hashing runs over the mirror types by construction (§4.3/§13).
//
// Frozen nil/empty rule: the canonical Go form uses nil for an empty
// repeated field and a nil pointer for an absent message. proto3 repeated
// fields have no presence, so decoding yields nil naturally; a producer
// that hashes a non-nil empty slice ([] vs null in canonical JSON) fails
// its own hash recomputation and is rejected — a protocol conformance
// rule, not a lenient normalization.
package wire

import (
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
)

// ChallengeVerdict mirrors pb.ChallengeVerdict.
type ChallengeVerdict int32

const (
	ChallengeVerdictUnspecified ChallengeVerdict = 0
	ChallengeVerdictSafe        ChallengeVerdict = 1
	ChallengeVerdictRejected    ChallengeVerdict = 2
)

type SubmitStatement struct {
	Envelope           arbiter.StatementEnvelope
	NonMembershipProof []byte
}
type SealL3Block struct{}
type MarkReplaying struct{ BlockSeq uint64 }
type RegisterRC struct{ RC arbiter.RCRecord }
type RecordAttestation struct{ Attestation replay.ReplayAttestation }
type RecordByteSideScan struct{ Scan arbiter.ByteSideScanMsg }
type RecordAnchorFinality struct {
	L3BlockSeq           uint64
	Anchor               arbiter.AnchorRef
	FinalityReached      bool
	LastMergeableReached bool
}
type RecordPromotionIssued struct {
	Promote      arbiter.PromoteSafePartition
	AuthorityJWS string
}
type RecordPromotionAck struct{ Ack arbiter.PromotionAck }
type PublishSafeSnapshot struct{ Manifest replay.SafeSnapshotManifest }
type ScheduleUnsafeCleanup struct {
	Cleanup      arbiter.UnsafeCleanup
	AuthorityJWS string
}
type RecordCleanupAck struct{ Ack arbiter.CleanupAck }
type OpenChallenge struct {
	BlockSeq uint64
	Reason   string
	OpenedBy string
}
type ResolveChallenge struct {
	BlockSeq uint64
	Verdict  ChallengeVerdict
}
type RegisterNode struct{ Registration arbiter.NodeRegistration }
type MarkActive struct{ NodeID string }
type EvictNode struct {
	NodeID string
	Reason string
}

// Command is the decoded RaftCommand: exactly one field is non-nil.
type Command struct {
	SubmitStatement       *SubmitStatement
	SealL3Block           *SealL3Block
	MarkReplaying         *MarkReplaying
	RegisterRC            *RegisterRC
	RecordAttestation     *RecordAttestation
	RecordByteSideScan    *RecordByteSideScan
	RecordAnchorFinality  *RecordAnchorFinality
	RecordPromotionIssued *RecordPromotionIssued
	RecordPromotionAck    *RecordPromotionAck
	PublishSafeSnapshot   *PublishSafeSnapshot
	ScheduleUnsafeCleanup *ScheduleUnsafeCleanup
	RecordCleanupAck      *RecordCleanupAck
	OpenChallenge         *OpenChallenge
	ResolveChallenge      *ResolveChallenge
	RegisterNode          *RegisterNode
	MarkActive            *MarkActive
	EvictNode             *EvictNode
}

// Encode marshals a Command into RaftCommand log-entry bytes.
func Encode(c Command) ([]byte, error) {
	out := &pb.RaftCommand{}
	set := 0
	if c.SubmitStatement != nil {
		set++
		out.Cmd = &pb.RaftCommand_SubmitStatement{SubmitStatement: &pb.SubmitStatementCmd{
			Envelope: envelopeToPB(c.SubmitStatement.Envelope), NonMembershipProof: c.SubmitStatement.NonMembershipProof}}
	}
	if c.SealL3Block != nil {
		set++
		out.Cmd = &pb.RaftCommand_SealL3Block{SealL3Block: &pb.SealL3BlockCmd{}}
	}
	if c.MarkReplaying != nil {
		set++
		out.Cmd = &pb.RaftCommand_MarkReplaying{MarkReplaying: &pb.MarkReplayingCmd{BlockSeq: c.MarkReplaying.BlockSeq}}
	}
	if c.RegisterRC != nil {
		set++
		out.Cmd = &pb.RaftCommand_RegisterRc{RegisterRc: &pb.RegisterRCCmd{Rc: rcToPB(c.RegisterRC.RC)}}
	}
	if c.RecordAttestation != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordAttestation{RecordAttestation: &pb.RecordAttestationCmd{Attestation: attestationToPB(c.RecordAttestation.Attestation)}}
	}
	if c.RecordByteSideScan != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordByteSideScan{RecordByteSideScan: &pb.RecordByteSideScanCmd{Scan: scanToPB(c.RecordByteSideScan.Scan)}}
	}
	if c.RecordAnchorFinality != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordAnchorFinality{RecordAnchorFinality: &pb.RecordAnchorFinalityCmd{
			L3BlockSeq: c.RecordAnchorFinality.L3BlockSeq, Anchor: anchorToPB(c.RecordAnchorFinality.Anchor),
			FinalityReached: c.RecordAnchorFinality.FinalityReached, LastMergeableReached: c.RecordAnchorFinality.LastMergeableReached}}
	}
	if c.RecordPromotionIssued != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordPromotionIssued{RecordPromotionIssued: &pb.RecordPromotionIssuedCmd{
			Promote: promoteToPB(c.RecordPromotionIssued.Promote), AuthorityJws: c.RecordPromotionIssued.AuthorityJWS}}
	}
	if c.RecordPromotionAck != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordPromotionAck{RecordPromotionAck: &pb.RecordPromotionAckCmd{Ack: promotionAckToPB(c.RecordPromotionAck.Ack)}}
	}
	if c.PublishSafeSnapshot != nil {
		set++
		out.Cmd = &pb.RaftCommand_PublishSafeSnapshot{PublishSafeSnapshot: &pb.PublishSafeSnapshotCmd{Manifest: manifestToPB(c.PublishSafeSnapshot.Manifest)}}
	}
	if c.ScheduleUnsafeCleanup != nil {
		set++
		out.Cmd = &pb.RaftCommand_ScheduleUnsafeCleanup{ScheduleUnsafeCleanup: &pb.ScheduleUnsafeCleanupCmd{
			Cleanup: cleanupToPB(c.ScheduleUnsafeCleanup.Cleanup), AuthorityJws: c.ScheduleUnsafeCleanup.AuthorityJWS}}
	}
	if c.RecordCleanupAck != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordCleanupAck{RecordCleanupAck: &pb.RecordCleanupAckCmd{Ack: cleanupAckToPB(c.RecordCleanupAck.Ack)}}
	}
	if c.OpenChallenge != nil {
		set++
		out.Cmd = &pb.RaftCommand_OpenChallenge{OpenChallenge: &pb.OpenChallengeCmd{
			BlockSeq: c.OpenChallenge.BlockSeq, Reason: c.OpenChallenge.Reason, OpenedBy: c.OpenChallenge.OpenedBy}}
	}
	if c.ResolveChallenge != nil {
		set++
		out.Cmd = &pb.RaftCommand_ResolveChallenge{ResolveChallenge: &pb.ResolveChallengeCmd{
			BlockSeq: c.ResolveChallenge.BlockSeq, Verdict: pb.ChallengeVerdict(c.ResolveChallenge.Verdict)}}
	}
	if c.RegisterNode != nil {
		set++
		out.Cmd = &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{Registration: registrationToPB(c.RegisterNode.Registration)}}
	}
	if c.MarkActive != nil {
		set++
		out.Cmd = &pb.RaftCommand_MarkActive{MarkActive: &pb.MarkActiveCmd{NodeId: c.MarkActive.NodeID}}
	}
	if c.EvictNode != nil {
		set++
		out.Cmd = &pb.RaftCommand_EvictNode{EvictNode: &pb.EvictNodeCmd{NodeId: c.EvictNode.NodeID, Reason: c.EvictNode.Reason}}
	}
	if set != 1 {
		return nil, fmt.Errorf("wire: exactly one command must be set, got %d", set)
	}
	return proto.Marshal(out)
}

// Decode parses RaftCommand log-entry bytes into the Go union.
func Decode(b []byte) (Command, error) {
	var in pb.RaftCommand
	if err := proto.Unmarshal(b, &in); err != nil {
		return Command{}, fmt.Errorf("wire: unmarshal RaftCommand: %w", err)
	}
	switch cmd := in.GetCmd().(type) {
	case *pb.RaftCommand_SubmitStatement:
		return Command{SubmitStatement: &SubmitStatement{
			Envelope: envelopeFromPB(cmd.SubmitStatement.GetEnvelope()), NonMembershipProof: cmd.SubmitStatement.GetNonMembershipProof()}}, nil
	case *pb.RaftCommand_SealL3Block:
		return Command{SealL3Block: &SealL3Block{}}, nil
	case *pb.RaftCommand_MarkReplaying:
		return Command{MarkReplaying: &MarkReplaying{BlockSeq: cmd.MarkReplaying.GetBlockSeq()}}, nil
	case *pb.RaftCommand_RegisterRc:
		return Command{RegisterRC: &RegisterRC{RC: rcFromPB(cmd.RegisterRc.GetRc())}}, nil
	case *pb.RaftCommand_RecordAttestation:
		return Command{RecordAttestation: &RecordAttestation{Attestation: attestationFromPB(cmd.RecordAttestation.GetAttestation())}}, nil
	case *pb.RaftCommand_RecordByteSideScan:
		return Command{RecordByteSideScan: &RecordByteSideScan{Scan: scanFromPB(cmd.RecordByteSideScan.GetScan())}}, nil
	case *pb.RaftCommand_RecordAnchorFinality:
		return Command{RecordAnchorFinality: &RecordAnchorFinality{
			L3BlockSeq: cmd.RecordAnchorFinality.GetL3BlockSeq(), Anchor: anchorFromPB(cmd.RecordAnchorFinality.GetAnchor()),
			FinalityReached: cmd.RecordAnchorFinality.GetFinalityReached(), LastMergeableReached: cmd.RecordAnchorFinality.GetLastMergeableReached()}}, nil
	case *pb.RaftCommand_RecordPromotionIssued:
		return Command{RecordPromotionIssued: &RecordPromotionIssued{
			Promote: promoteFromPB(cmd.RecordPromotionIssued.GetPromote()), AuthorityJWS: cmd.RecordPromotionIssued.GetAuthorityJws()}}, nil
	case *pb.RaftCommand_RecordPromotionAck:
		return Command{RecordPromotionAck: &RecordPromotionAck{Ack: promotionAckFromPB(cmd.RecordPromotionAck.GetAck())}}, nil
	case *pb.RaftCommand_PublishSafeSnapshot:
		return Command{PublishSafeSnapshot: &PublishSafeSnapshot{Manifest: manifestFromPB(cmd.PublishSafeSnapshot.GetManifest())}}, nil
	case *pb.RaftCommand_ScheduleUnsafeCleanup:
		return Command{ScheduleUnsafeCleanup: &ScheduleUnsafeCleanup{
			Cleanup: cleanupFromPB(cmd.ScheduleUnsafeCleanup.GetCleanup()), AuthorityJWS: cmd.ScheduleUnsafeCleanup.GetAuthorityJws()}}, nil
	case *pb.RaftCommand_RecordCleanupAck:
		return Command{RecordCleanupAck: &RecordCleanupAck{Ack: cleanupAckFromPB(cmd.RecordCleanupAck.GetAck())}}, nil
	case *pb.RaftCommand_OpenChallenge:
		return Command{OpenChallenge: &OpenChallenge{
			BlockSeq: cmd.OpenChallenge.GetBlockSeq(), Reason: cmd.OpenChallenge.GetReason(), OpenedBy: cmd.OpenChallenge.GetOpenedBy()}}, nil
	case *pb.RaftCommand_ResolveChallenge:
		return Command{ResolveChallenge: &ResolveChallenge{
			BlockSeq: cmd.ResolveChallenge.GetBlockSeq(), Verdict: ChallengeVerdict(cmd.ResolveChallenge.GetVerdict())}}, nil
	case *pb.RaftCommand_RegisterNode:
		return Command{RegisterNode: &RegisterNode{Registration: registrationFromPB(cmd.RegisterNode.GetRegistration())}}, nil
	case *pb.RaftCommand_MarkActive:
		return Command{MarkActive: &MarkActive{NodeID: cmd.MarkActive.GetNodeId()}}, nil
	case *pb.RaftCommand_EvictNode:
		return Command{EvictNode: &EvictNode{NodeID: cmd.EvictNode.GetNodeId(), Reason: cmd.EvictNode.GetReason()}}, nil
	default:
		return Command{}, fmt.Errorf("wire: RaftCommand has no command set")
	}
}
```

Note: the generated oneof wrapper for `register_rc` is `pb.RaftCommand_RegisterRc` (protoc capitalization); check `gen/pb/raftlog.pb.go` for the exact identifiers if the compiler disagrees, and follow the generated names.

- [ ] **Step 4: Implement `wire/convert.go`**

```go
package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
)

// mapSlice converts a repeated field; empty input yields nil — THE frozen
// normalization point for the canonical Go form (see package doc).
func mapSlice[I, O any](in []I, f func(I) O) []O {
	if len(in) == 0 {
		return nil
	}
	out := make([]O, len(in))
	for i := range in {
		out[i] = f(in[i])
	}
	return out
}

func statementIDFromPB(m *pb.StatementID) arbiter.StatementID {
	return arbiter.StatementID{ClientAccount: m.GetClientAccount(), ClientSeq: m.GetClientSeq(), ClientNonce: m.GetClientNonce()}
}

func statementIDToPB(v arbiter.StatementID) *pb.StatementID {
	return &pb.StatementID{ClientAccount: v.ClientAccount, ClientSeq: v.ClientSeq, ClientNonce: v.ClientNonce}
}

func envelopeFromPB(m *pb.StatementEnvelopeV2) arbiter.StatementEnvelope {
	return arbiter.StatementEnvelope{
		StatementID:   statementIDFromPB(m.GetStatementId()),
		StatementKind: arbiter.StatementKind(m.GetStatementKind()),
		SQL:           m.GetSql(),
		SQLHash:       m.GetSqlHash(),
		SettingsHash:  m.GetSettingsHash(),
		PayloadRef:    m.GetPayloadRef(),
		PayloadHash:   m.GetPayloadHash(),
		PayloadLength: m.GetPayloadLength(),
		TargetTableID: m.GetTargetTableId(),
		UserJWS:       m.GetUserJws(),
	}
}

func envelopeToPB(v arbiter.StatementEnvelope) *pb.StatementEnvelopeV2 {
	return &pb.StatementEnvelopeV2{
		StatementId:   statementIDToPB(v.StatementID),
		StatementKind: pb.StatementKind(v.StatementKind),
		Sql:           v.SQL,
		SqlHash:       v.SQLHash,
		SettingsHash:  v.SettingsHash,
		PayloadRef:    v.PayloadRef,
		PayloadHash:   v.PayloadHash,
		PayloadLength: v.PayloadLength,
		TargetTableId: v.TargetTableID,
		UserJws:       v.UserJWS,
	}
}

func candidatePartFromPB(m *pb.CandidatePart) arbiter.CandidatePart {
	return arbiter.CandidatePart{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), PartName: m.GetPartName(),
		PartRowLtHash: m.GetPartRowLthash(), PartPhysHash: m.GetPartPhysHash(), RowCount: m.GetRowCount(), Bytes: m.GetBytes()}
}

func candidatePartToPB(v arbiter.CandidatePart) *pb.CandidatePart {
	return &pb.CandidatePart{TableId: v.TableID, PartitionId: v.PartitionID, PartName: v.PartName,
		PartRowLthash: v.PartRowLtHash, PartPhysHash: v.PartPhysHash, RowCount: v.RowCount, Bytes: v.Bytes}
}

func partitionSumFromPB(m *pb.PartitionLtHashSum) arbiter.PartitionLtHashSum {
	return arbiter.PartitionLtHashSum{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), NewPartsLtHashSum: m.GetNewPartsLthashSum()}
}

func partitionSumToPB(v arbiter.PartitionLtHashSum) *pb.PartitionLtHashSum {
	return &pb.PartitionLtHashSum{TableId: v.TableID, PartitionId: v.PartitionID, NewPartsLthashSum: v.NewPartsLtHashSum}
}

func rcFromPB(m *pb.RCRecord) arbiter.RCRecord {
	return arbiter.RCRecord{
		StatementID:          statementIDFromPB(m.GetStatementId()),
		SourceNode:           m.GetSourceNode(),
		CandidateParts:       mapSlice(m.GetCandidateParts(), candidatePartFromPB),
		SourceClaimRoot:      m.GetSourceClaimRoot(),
		PartitionNewPartSums: mapSlice(m.GetPartitionNewPartSums(), partitionSumFromPB),
	}
}

func rcToPB(v arbiter.RCRecord) *pb.RCRecord {
	return &pb.RCRecord{
		StatementId:          statementIDToPB(v.StatementID),
		SourceNode:           v.SourceNode,
		CandidateParts:       mapSlice(v.CandidateParts, candidatePartToPB),
		SourceClaimRoot:      v.SourceClaimRoot,
		PartitionNewPartSums: mapSlice(v.PartitionNewPartSums, partitionSumToPB),
	}
}

func partScanFromPB(m *pb.PartScan) arbiter.PartScan {
	return arbiter.PartScan{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(),
		ClaimedPartRowLtHash: m.GetClaimedPartRowLthash(), ScannedPartRowLtHash: m.GetScannedPartRowLthash(), LivePartName: m.GetLivePartName()}
}

func partScanToPB(v arbiter.PartScan) *pb.PartScan {
	return &pb.PartScan{TableId: v.TableID, PartitionId: v.PartitionID,
		ClaimedPartRowLthash: v.ClaimedPartRowLtHash, ScannedPartRowLthash: v.ScannedPartRowLtHash, LivePartName: v.LivePartName}
}

func scanFromPB(m *pb.ByteSideScanMsg) arbiter.ByteSideScanMsg {
	return arbiter.ByteSideScanMsg{ReplicaID: m.GetReplicaId(), BlockSeq: m.GetBlockSeq(),
		Parts: mapSlice(m.GetParts(), partScanFromPB), ScanHash: m.GetScanHash(), Signature: m.GetSignature()}
}

func scanToPB(v arbiter.ByteSideScanMsg) *pb.ByteSideScanMsg {
	return &pb.ByteSideScanMsg{ReplicaId: v.ReplicaID, BlockSeq: v.BlockSeq,
		Parts: mapSlice(v.Parts, partScanToPB), ScanHash: v.ScanHash, Signature: v.Signature}
}

func anchorFromPB(m *pb.AnchorRef) arbiter.AnchorRef {
	return arbiter.AnchorRef{L3BlockHash: m.GetL3BlockHash(), StateRoot: m.GetStateRoot(),
		L2TxRef: m.GetL2TxRef(), L2BlockNumber: m.GetL2BlockNumber(), DARef: m.GetDaRef()}
}

func anchorToPB(v arbiter.AnchorRef) *pb.AnchorRef {
	return &pb.AnchorRef{L3BlockHash: v.L3BlockHash, StateRoot: v.StateRoot,
		L2TxRef: v.L2TxRef, L2BlockNumber: v.L2BlockNumber, DaRef: v.DARef}
}

func registrationFromPB(m *pb.NodeRegistration) arbiter.NodeRegistration {
	return arbiter.NodeRegistration{NodeID: m.GetNodeId(),
		Roles:         mapSlice(m.GetRoles(), func(r pb.NodeRole) arbiter.NodeRole { return arbiter.NodeRole(r) }),
		Ed25519Pubkey: m.GetEd25519Pubkey(), DialAddr: m.GetDialAddr()}
}

func registrationToPB(v arbiter.NodeRegistration) *pb.NodeRegistration {
	return &pb.NodeRegistration{NodeId: v.NodeID,
		Roles:         mapSlice(v.Roles, func(r arbiter.NodeRole) pb.NodeRole { return pb.NodeRole(r) }),
		Ed25519Pubkey: v.Ed25519Pubkey, DialAddr: v.DialAddr}
}

func partRefFromPB(m *pb.PartRef) arbiter.PartRef {
	return arbiter.PartRef{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(),
		PartRowLtHash: m.GetPartRowLthash(), PartName: m.GetPartName()}
}

func partRefToPB(v arbiter.PartRef) *pb.PartRef {
	return &pb.PartRef{TableId: v.TableID, PartitionId: v.PartitionID,
		PartRowLthash: v.PartRowLtHash, PartName: v.PartName}
}

func promoteFromPB(m *pb.PromoteSafePartition) arbiter.PromoteSafePartition {
	return arbiter.PromoteSafePartition{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), PromotionSeq: m.GetPromotionSeq(),
		BaseSafeSnapshotID: m.GetBaseSafeSnapshotId(), BasePartitionRoot: m.GetBasePartitionRoot(),
		CandidateParts: mapSlice(m.GetCandidateParts(), partRefFromPB)}
}

func promoteToPB(v arbiter.PromoteSafePartition) *pb.PromoteSafePartition {
	return &pb.PromoteSafePartition{TableId: v.TableID, PartitionId: v.PartitionID, PromotionSeq: v.PromotionSeq,
		BaseSafeSnapshotId: v.BaseSafeSnapshotID, BasePartitionRoot: v.BasePartitionRoot,
		CandidateParts: mapSlice(v.CandidateParts, partRefToPB)}
}

func cleanupFromPB(m *pb.UnsafeCleanup) arbiter.UnsafeCleanup {
	return arbiter.UnsafeCleanup{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), PromotionSeq: m.GetPromotionSeq(),
		Parts: mapSlice(m.GetParts(), partRefFromPB)}
}

func cleanupToPB(v arbiter.UnsafeCleanup) *pb.UnsafeCleanup {
	return &pb.UnsafeCleanup{TableId: v.TableID, PartitionId: v.PartitionID, PromotionSeq: v.PromotionSeq,
		Parts: mapSlice(v.Parts, partRefToPB)}
}

func safePartMappingFromPB(m *pb.SafePartMapping) arbiter.SafePartMapping {
	return arbiter.SafePartMapping{PartRowLtHash: m.GetPartRowLthash(), SafePartName: m.GetSafePartName(), PartPhysHash: m.GetPartPhysHash()}
}

func safePartMappingToPB(v arbiter.SafePartMapping) *pb.SafePartMapping {
	return &pb.SafePartMapping{PartRowLthash: v.PartRowLtHash, SafePartName: v.SafePartName, PartPhysHash: v.PartPhysHash}
}

func promotionAckFromPB(m *pb.PromotionAck) arbiter.PromotionAck {
	return arbiter.PromotionAck{NodeID: m.GetNodeId(), PromotionSeq: m.GetPromotionSeq(), TableID: m.GetTableId(), PartitionID: m.GetPartitionId(),
		PostPartitionCommitment: m.GetPostPartitionCommitment(), Parts: mapSlice(m.GetParts(), safePartMappingFromPB),
		Applied: m.GetApplied(), Detail: m.GetDetail()}
}

func promotionAckToPB(v arbiter.PromotionAck) *pb.PromotionAck {
	return &pb.PromotionAck{NodeId: v.NodeID, PromotionSeq: v.PromotionSeq, TableId: v.TableID, PartitionId: v.PartitionID,
		PostPartitionCommitment: v.PostPartitionCommitment, Parts: mapSlice(v.Parts, safePartMappingToPB),
		Applied: v.Applied, Detail: v.Detail}
}

func cleanupAckFromPB(m *pb.CleanupAck) arbiter.CleanupAck {
	return arbiter.CleanupAck{NodeID: m.GetNodeId(), PromotionSeq: m.GetPromotionSeq(), TableID: m.GetTableId(), PartitionID: m.GetPartitionId()}
}

func cleanupAckToPB(v arbiter.CleanupAck) *pb.CleanupAck {
	return &pb.CleanupAck{NodeId: v.NodeID, PromotionSeq: v.PromotionSeq, TableId: v.TableID, PartitionId: v.PartitionID}
}

func partitionCommitmentFromPB(m *pb.PartitionCommitment) replay.PartitionCommitment {
	return replay.PartitionCommitment{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), Root: m.GetRoot()}
}

func partitionCommitmentToPB(v replay.PartitionCommitment) *pb.PartitionCommitment {
	return &pb.PartitionCommitment{TableId: v.TableID, PartitionId: v.PartitionID, Root: v.Root}
}

func partManifestEntryFromPB(m *pb.PartManifestEntry) replay.PartManifestEntry {
	return replay.PartManifestEntry{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), PartName: m.GetPartName(),
		PartPhysHash: m.GetPartPhysHash(), PartRowLtHash: m.GetPartRowLthash(), RowCount: m.GetRowCount(), Bytes: m.GetBytes(),
		StorageRefs: append([]string(nil), m.GetStorageRefs()...)}
}

func partManifestEntryToPB(v replay.PartManifestEntry) *pb.PartManifestEntry {
	return &pb.PartManifestEntry{TableId: v.TableID, PartitionId: v.PartitionID, PartName: v.PartName,
		PartPhysHash: v.PartPhysHash, PartRowLthash: v.PartRowLtHash, RowCount: v.RowCount, Bytes: v.Bytes,
		StorageRefs: v.StorageRefs}
}

func receiptFromPB(m *pb.ExecutionReceipt) replay.ExecutionReceipt {
	return replay.ExecutionReceipt{
		BlockSeq: m.GetBlockSeq(), PrevSafeSnapshotID: m.GetPrevSafeSnapshotId(), PrevStateRoot: m.GetPrevStateRoot(),
		SchemaSnapshotID: m.GetSchemaSnapshotId(), ExecutorProfileID: m.GetExecutorProfileId(),
		StatementRoot: m.GetStatementRoot(), PayloadRoot: m.GetPayloadRoot(), SourceClaimRoot: m.GetSourceClaimRoot(),
		ComputedStateRoot: m.GetComputedStateRoot(), MatchSourceRoot: m.GetMatchSourceRoot(),
		PartitionCommitmentsAfter: mapSlice(m.GetPartitionCommitmentsAfter(), partitionCommitmentFromPB),
		AffectedParts:             mapSlice(m.GetAffectedParts(), partManifestEntryFromPB),
		ReplayLogHash:             m.GetReplayLogHash(),
	}
}

func receiptToPB(v replay.ExecutionReceipt) *pb.ExecutionReceipt {
	return &pb.ExecutionReceipt{
		BlockSeq: v.BlockSeq, PrevSafeSnapshotId: v.PrevSafeSnapshotID, PrevStateRoot: v.PrevStateRoot,
		SchemaSnapshotId: v.SchemaSnapshotID, ExecutorProfileId: v.ExecutorProfileID,
		StatementRoot: v.StatementRoot, PayloadRoot: v.PayloadRoot, SourceClaimRoot: v.SourceClaimRoot,
		ComputedStateRoot: v.ComputedStateRoot, MatchSourceRoot: v.MatchSourceRoot,
		PartitionCommitmentsAfter: mapSlice(v.PartitionCommitmentsAfter, partitionCommitmentToPB),
		AffectedParts:             mapSlice(v.AffectedParts, partManifestEntryToPB),
		ReplayLogHash:             v.ReplayLogHash,
	}
}

func attestationFromPB(m *pb.ReplayAttestation) replay.ReplayAttestation {
	return replay.ReplayAttestation{ReplicaID: m.GetReplicaId(), Receipt: receiptFromPB(m.GetReceipt()),
		ReceiptHash: m.GetReceiptHash(), Signature: m.GetSignature(), MatchSourceRoot: m.GetMatchSourceRoot()}
}

func attestationToPB(v replay.ReplayAttestation) *pb.ReplayAttestation {
	return &pb.ReplayAttestation{ReplicaId: v.ReplicaID, Receipt: receiptToPB(v.Receipt),
		ReceiptHash: v.ReceiptHash, Signature: v.Signature, MatchSourceRoot: v.MatchSourceRoot}
}

func tableManifestFromPB(m *pb.TableManifest) replay.TableManifest {
	return replay.TableManifest{TableID: m.GetTableId(), SchemaHash: m.GetSchemaHash(),
		PartitionRoots: mapSlice(m.GetPartitionRoots(), partitionCommitmentFromPB),
		ActiveParts:    mapSlice(m.GetActiveParts(), partManifestEntryFromPB)}
}

func tableManifestToPB(v replay.TableManifest) *pb.TableManifest {
	return &pb.TableManifest{TableId: v.TableID, SchemaHash: v.SchemaHash,
		PartitionRoots: mapSlice(v.PartitionRoots, partitionCommitmentToPB),
		ActiveParts:    mapSlice(v.ActiveParts, partManifestEntryToPB)}
}

func manifestFromPB(m *pb.SafeSnapshotManifest) replay.SafeSnapshotManifest {
	return replay.SafeSnapshotManifest{SnapshotID: m.GetSnapshotId(), ParentSnapshotID: m.GetParentSnapshotId(),
		SafeBlockSeq: m.GetSafeBlockSeq(), StateRoot: m.GetStateRoot(), SchemaSnapshotID: m.GetSchemaSnapshotId(),
		SchemaRoot: m.GetSchemaRoot(), ExecutorProfileID: m.GetExecutorProfileId(), DataRoot: m.GetDataRoot(),
		ManifestRoot: m.GetManifestRoot(), Tables: mapSlice(m.GetTables(), tableManifestFromPB)}
}

func manifestToPB(v replay.SafeSnapshotManifest) *pb.SafeSnapshotManifest {
	return &pb.SafeSnapshotManifest{SnapshotId: v.SnapshotID, ParentSnapshotId: v.ParentSnapshotID,
		SafeBlockSeq: v.SafeBlockSeq, StateRoot: v.StateRoot, SchemaSnapshotId: v.SchemaSnapshotID,
		SchemaRoot: v.SchemaRoot, ExecutorProfileId: v.ExecutorProfileID, DataRoot: v.DataRoot,
		ManifestRoot: v.ManifestRoot, Tables: mapSlice(v.Tables, tableManifestToPB)}
}
```

Generated-name caveat: pb getter spellings for fields like `part_row_lthash` (`GetPartRowLthash`), `l3_block_seq` (`GetL3BlockSeq`), `da_ref` (`GetDaRef`) follow protoc-gen-go camelization; if the compiler flags one, read the generated `gen/pb/*.pb.go` and use the generated identifier — never rename the proto field. `PartManifestEntry.StorageRefs` uses `append([]string(nil), ...)` because `mapSlice` needs an element converter; the append form both copies and nil-normalizes (`append(nil)` on empty input returns nil).

- [ ] **Step 5: Run the wire tests**

Run: `go test ./wire/ -v 2>&1 | tail -15`
Expected: ALL PASS (round-trips, normalization, encode/decode rejections).

- [ ] **Step 6: Commit**

```bash
git add wire/
git commit -m "feat(wire): pb<->Go command seam with frozen nil/empty normalization

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 5: fsm skeleton — State, results, Apply dispatch, membership

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Create: `fsm/state.go`, `fsm/fsm.go`, `fsm/apply.go`, `fsm/fsm_test.go`

**Interfaces:**
- Consumes: Task 3 mirrors + domains, Task 4 `wire.Decode`/`wire.Command`, `accumulator.NewSpentIDs`, `replay.CanonicalDigest`.
- Produces (used by Tasks 6–15): `fsm.New(Params) *FSM` implementing `raft.FSM`; state types `State`, `StatementState`, `Status*` lifecycle constants, `OpenL3Block`, `L3BlockHeader` (+ `ChainHash()`), `BlockVerification`, `ThreeWayVerdict`/`ReplicaVerdict`, `PartitionState`, `SafeWatermark`, `PendingPromotion`, `NodeInfo`/`NodeStatus`, `Params`; result types `Applied{}`, `Rejected{Reason string}`, `SubmitResult`, `SealResult`; frozen constants `VerifierQuorum = 2`, `VerifierSelectN = 3`; `(*FSM).Summary()`; test helpers `newTestFSM`, `mustApply`, `mkLog`.
- Handler stubs: `Apply` dispatches all 17 commands; the ones built in later tasks return `Rejected{Reason: "not implemented"}` from `apply.go` until their task lands (each later task replaces its stub).

- [ ] **Step 1: Write the failing membership tests**

Create `fsm/fsm_test.go`:

```go
package fsm

import (
	"crypto/ed25519"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

func testParams() Params {
	return Params{
		SchemaSnapshotID:  "schema-genesis",
		ExecutorProfileID: "housegate-replay-mvp-v0",
	}
}

func newTestFSM(t *testing.T) *FSM {
	t.Helper()
	return New(testParams())
}

func mkLog(t *testing.T, c wire.Command) *raft.Log {
	t.Helper()
	b, err := wire.Encode(c)
	if err != nil {
		t.Fatalf("encode command: %v", err)
	}
	return &raft.Log{Data: b}
}

// mustApply applies a command and fails the test on a Rejected result.
func mustApply(t *testing.T, f *FSM, c wire.Command) any {
	t.Helper()
	res := f.Apply(mkLog(t, c))
	if r, ok := res.(Rejected); ok {
		t.Fatalf("unexpected rejection: %s", r.Reason)
	}
	return res
}

func applyExpectReject(t *testing.T, f *FSM, c wire.Command) Rejected {
	t.Helper()
	res := f.Apply(mkLog(t, c))
	r, ok := res.(Rejected)
	if !ok {
		t.Fatalf("expected Rejected, got %T %+v", res, res)
	}
	return r
}

func testPubkey(seed byte) []byte {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return []byte(ed25519.NewKeyFromSeed(s).Public().(ed25519.PublicKey))
}

func registerActive(t *testing.T, f *FSM, id string, roles ...arbiter.NodeRole) {
	t.Helper()
	mustApply(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: id, Roles: roles, Ed25519Pubkey: testPubkey(id[len(id)-1])}}})
	mustApply(t, f, wire.Command{MarkActive: &wire.MarkActive{NodeID: id}})
}

func TestMembershipLifecycle(t *testing.T) {
	f := newTestFSM(t)
	reg := arbiter.NodeRegistration{NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: testPubkey(1)}
	mustApply(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg}})
	if f.st.Nodes["v1"].Status != NodeSyncing {
		t.Fatalf("registered node must start Syncing, got %v", f.st.Nodes["v1"].Status)
	}
	mustApply(t, f, wire.Command{MarkActive: &wire.MarkActive{NodeID: "v1"}})
	if f.st.Nodes["v1"].Status != NodeActive {
		t.Fatal("MarkActive must activate")
	}
	// re-register resets to Syncing (design §9)
	mustApply(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg}})
	if f.st.Nodes["v1"].Status != NodeSyncing {
		t.Fatal("re-registration must reset status to Syncing")
	}
	mustApply(t, f, wire.Command{MarkActive: &wire.MarkActive{NodeID: "v1"}})
	mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: "v1", Reason: "audit"}})
	if n := f.st.Nodes["v1"]; n.Status != NodeEvicted || n.Reason != "audit" {
		t.Fatalf("eviction: got %+v", n)
	}
	// evicted nodes cannot be re-activated without re-registration
	applyExpectReject(t, f, wire.Command{MarkActive: &wire.MarkActive{NodeID: "v1"}})
}

func TestMembershipRejections(t *testing.T) {
	f := newTestFSM(t)
	applyExpectReject(t, f, wire.Command{MarkActive: &wire.MarkActive{NodeID: "ghost"}})
	applyExpectReject(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: "ghost"}})
	// verifier registration requires a 32-byte ed25519 pubkey
	applyExpectReject(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: []byte{1, 2}}}})
	// empty node_id
	applyExpectReject(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}})
}

func TestApplyRejectsGarbageAndUnimplemented(t *testing.T) {
	f := newTestFSM(t)
	if _, ok := f.Apply(&raft.Log{Data: []byte{0xff, 0x00}}).(Rejected); !ok {
		t.Fatal("garbage log data must yield Rejected, not panic")
	}
}

func TestSummaryOnFreshFSM(t *testing.T) {
	f := newTestFSM(t)
	s, err := f.Summary()
	if err != nil {
		t.Fatalf("Summary: %v", err)
	}
	if s.NextStatementSeq != 1 || s.NextL3BlockSeq != 1 || s.StatementCount != 0 {
		t.Fatalf("fresh summary: %+v", s)
	}
	if s.SpentIDsRoot == "" {
		t.Fatal("SpentIDsRoot must render the EmptyRoot")
	}
}
```

- [ ] **Step 2: Run to verify compile failure**

Run: `go test ./fsm/ 2>&1 | head -5`
Expected: package does not exist.

- [ ] **Step 3: Implement `fsm/state.go`**

```go
// Package fsm is the Arbiter's deterministic replicated state machine
// (design §4, P1a spec §4–§9). Apply/Snapshot/Restore are wall-clock-free
// and consume only committed state plus logged bytes; this package never
// imports arbiter-proto/gen/pb (CI-enforced) — wire.Command is its only
// view of the log, and all hashing runs over canonical Go types through
// replay.CanonicalDigest (§4.3 red lines, §13 tripwires).
package fsm

import (
	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/accumulator"
)

// Frozen consensus constants (design §4): values, not configuration.
const (
	// VerifierQuorum is the 2 in "2-of-3" (§7.4).
	VerifierQuorum = 2
	// VerifierSelectN is the deterministic selection size (Open Q3 resolution).
	VerifierSelectN = 3
)

// Status is the §4.4 statement lifecycle.
type Status uint8

const (
	StatusSequenced Status = iota + 1
	StatusUnsafeRegistered
	StatusReplaying
	StatusQuorumVerified
	StatusFinalityWait
	StatusPromotable
	StatusSafe
	StatusChallengeReplay
	StatusRejected
)

// NodeStatus is the membership lifecycle: Syncing → Active → Evicted.
type NodeStatus uint8

const (
	NodeSyncing NodeStatus = iota + 1
	NodeActive
	NodeEvicted
)

// NodeInfo is one registered data-plane node.
type NodeInfo struct {
	Registration arbiter.NodeRegistration `json:"registration"`
	Status       NodeStatus               `json:"status"`
	Reason       string                   `json:"reason,omitempty"`
}

// Params are consensus parameters: every node MUST construct the FSM with
// identical values or the cluster forks. Sourcing them is P1b's config job.
type Params struct {
	SchemaSnapshotID   string   `json:"schema_snapshot_id"`
	ExecutorProfileID  string   `json:"executor_profile_id"`
	AuthorityAddresses []string `json:"authority_addresses,omitempty"`
}

// StatementState is one admitted statement's lifecycle record.
type StatementState struct {
	Env        arbiter.StatementEnvelope `json:"env"`
	Seq        uint64                    `json:"seq"`
	BlockSeq   uint64                    `json:"block_seq,omitempty"` // 0 until sealed
	SourceNode string                    `json:"source_node,omitempty"`
	Status     Status                    `json:"status"`
	RC         *arbiter.RCRecord         `json:"rc,omitempty"`
	// UnpromotedParts tracks the statement's candidate parts (by
	// part_row_lthash) not yet covered by an acked promotion; empty +
	// Promotable → Safe (design §9 RecordPromotionAck).
	UnpromotedParts map[string]bool `json:"unpromoted_parts,omitempty"`
}

// OpenL3Block is the unsealed statement buffer (§5.1).
type OpenL3Block struct {
	StatementSeqStart uint64   `json:"statement_seq_start"`
	StatementSeqs     []uint64 `json:"statement_seqs,omitempty"`
}

// L3BlockHeader is the sealed block header (§5). JSON tags are the frozen
// canonical form hashed by ChainHash.
type L3BlockHeader struct {
	L3BlockSeq         uint64             `json:"l3_block_seq"`
	PrevL3Hash         string             `json:"prev_l3_hash"`
	StatementSeqStart  uint64             `json:"statement_seq_start"`
	StatementCount     uint32             `json:"statement_count"`
	SchemaSnapshotID   string             `json:"schema_snapshot_id"`
	ExecutorProfileID  string             `json:"executor_profile_id"`
	PrevSafeSnapshotID string             `json:"prev_safe_snapshot_id,omitempty"`
	PrevStateRoot      string             `json:"prev_state_root,omitempty"`
	SpentIDsRootAfter  string             `json:"spent_ids_root_after"`
	L2AnchorRef        *arbiter.AnchorRef `json:"l2_anchor_ref,omitempty"`
}

// ChainHash is the frozen chain commitment: the header with the
// back-filled anchor EXCLUDED (§5.2 — sealing fixes the chained content;
// RecordAnchorFinality must not rewrite history).
func (h L3BlockHeader) ChainHash() (string, error) {
	c := h
	c.L2AnchorRef = nil
	return replay.CanonicalDigest(arbiter.DomainL3Header, c)
}

// ReplicaVerdict is one verifier's three-check outcome (§8).
type ReplicaVerdict struct {
	Check1RootMatch      bool `json:"check1_root_match"`
	Check2PartitionMatch bool `json:"check2_partition_match"`
	Check3ByteSideMatch  bool `json:"check3_byte_side_match"`
	Pass                 bool `json:"pass"`
}

// ThreeWayVerdict is the block's recomputable three-way outcome.
type ThreeWayVerdict struct {
	Replicas map[string]ReplicaVerdict `json:"replicas"`
	Quorum   bool                      `json:"quorum"`
}

// BlockVerification is the per-block evidence record (design §8): evidence
// is keyed by block, the verdict projects onto the block's statements.
type BlockVerification struct {
	BlockSeq      uint64                               `json:"block_seq"`
	SourceNodes   []string                             `json:"source_nodes,omitempty"`
	VerifierSet   []string                             `json:"verifier_set,omitempty"`
	Attestations  map[string]*replay.ReplayAttestation `json:"attestations,omitempty"`
	ByteScans     map[string]*arbiter.ByteSideScanMsg  `json:"byte_scans,omitempty"`
	Verdict       *ThreeWayVerdict                     `json:"verdict,omitempty"`
	Anchor        *arbiter.AnchorRef                   `json:"anchor,omitempty"`
	Finality      bool                                 `json:"finality,omitempty"`
	LastMergeable bool                                 `json:"last_mergeable,omitempty"`
}

// PartitionState is the per-partition promotion base (§7.3 check 2 base,
// §8.3 CAS anchor).
type PartitionState struct {
	BaseSafeSnapshotID string `json:"base_safe_snapshot_id,omitempty"`
	// BasePartitionRoot is "0x"+hex of the raw 2048-byte LtHash
	// accumulator; empty means the all-zero base (never promoted).
	BasePartitionRoot string `json:"base_partition_root,omitempty"`
	PublishSeq        uint64 `json:"publish_seq,omitempty"`
}

// SafeWatermark is the published safe-state tip (§8.5).
type SafeWatermark struct {
	SnapshotID   string `json:"snapshot_id,omitempty"`
	SafeBlockSeq uint64 `json:"safe_block_seq,omitempty"`
	ManifestRoot string `json:"manifest_root,omitempty"`
}

// PendingPromotion is an issued-but-unacked promotion (§9).
type PendingPromotion struct {
	Promote       arbiter.PromoteSafePartition `json:"promote"`
	StatementSeqs []uint64                     `json:"statement_seqs,omitempty"`
	Acked         bool                         `json:"acked,omitempty"`
}

// State is the FSM's replicated derived state (§4.2 amended by P0b: the
// accumulator owns HiSeq/Gaps/spent_ids_root).
type State struct {
	NextStatementSeq  uint64
	NextL3BlockSeq    uint64
	OpenBlock         *OpenL3Block
	Blocks            []L3BlockHeader
	SpentIDs          *accumulator.SpentIDs
	Statements        map[uint64]*StatementState
	ByStatementID     map[string]uint64 // derived: rebuilt on Restore
	PendingRC         map[string]*arbiter.RCRecord
	Verifications     map[uint64]*BlockVerification
	PromotionSeq      uint64
	PendingPromotions map[uint64]*PendingPromotion
	PendingCleanups   map[uint64]*arbiter.UnsafeCleanup
	Partitions        map[arbiter.TablePartition]*PartitionState
	SafeWatermark     SafeWatermark
	Manifests         map[string]*replay.SafeSnapshotManifest
	PromotedUnsafe    map[arbiter.TablePartition]map[string]bool
	Nodes             map[string]*NodeInfo
	Params            Params
}

func newState(params Params) *State {
	return &State{
		NextStatementSeq:  1,
		NextL3BlockSeq:    1,
		OpenBlock:         &OpenL3Block{StatementSeqStart: 1},
		SpentIDs:          accumulator.NewSpentIDs(),
		Statements:        map[uint64]*StatementState{},
		ByStatementID:     map[string]uint64{},
		PendingRC:         map[string]*arbiter.RCRecord{},
		Verifications:     map[uint64]*BlockVerification{},
		PendingPromotions: map[uint64]*PendingPromotion{},
		PendingCleanups:   map[uint64]*arbiter.UnsafeCleanup{},
		Partitions:        map[arbiter.TablePartition]*PartitionState{},
		Manifests:         map[string]*replay.SafeSnapshotManifest{},
		PromotedUnsafe:    map[arbiter.TablePartition]map[string]bool{},
		Nodes:             map[string]*NodeInfo{},
		Params:            params,
	}
}
```

- [ ] **Step 4: Implement `fsm/fsm.go`**

```go
package fsm

import (
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/hashicorp/raft"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// Applied acknowledges an accepted non-submit command.
type Applied struct{}

// Rejected is a domain rejection: a committed log entry whose validation
// failed. State is unchanged; every replica returns the identical value
// (rejected-but-committed, design §4).
type Rejected struct{ Reason string }

// SubmitResult is SubmitStatement's admission outcome.
type SubmitResult struct {
	Code         arbiter.AdmissionCode
	StatementSeq uint64
	Message      string
}

// SealResult reports a sealed block.
type SealResult struct {
	BlockSeq  uint64
	ChainHash string
}

// FSM implements raft.FSM (design §4.3). The mutex serializes Apply /
// Snapshot / Restore against read-only Summary calls from other
// goroutines; raft itself never calls Apply concurrently.
type FSM struct {
	mu sync.RWMutex
	st *State
}

var _ raft.FSM = (*FSM)(nil)

// New builds an FSM. Params are consensus parameters — identical on every
// node or the cluster forks.
func New(params Params) *FSM {
	return &FSM{st: newState(params)}
}

// Apply decodes one committed RaftCommand and mutates state
// deterministically. Domain failures return Rejected values, never errors.
func (f *FSM) Apply(l *raft.Log) any {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd, err := wire.Decode(l.Data)
	if err != nil {
		return Rejected{Reason: fmt.Sprintf("decode: %v", err)}
	}
	switch {
	case cmd.SubmitStatement != nil:
		return f.applySubmitStatement(cmd.SubmitStatement)
	case cmd.SealL3Block != nil:
		return f.applySealL3Block()
	case cmd.MarkReplaying != nil:
		return f.applyMarkReplaying(cmd.MarkReplaying)
	case cmd.RegisterRC != nil:
		return f.applyRegisterRC(cmd.RegisterRC)
	case cmd.RecordAttestation != nil:
		return f.applyRecordAttestation(cmd.RecordAttestation)
	case cmd.RecordByteSideScan != nil:
		return f.applyRecordByteSideScan(cmd.RecordByteSideScan)
	case cmd.RecordAnchorFinality != nil:
		return f.applyRecordAnchorFinality(cmd.RecordAnchorFinality)
	case cmd.RecordPromotionIssued != nil:
		return f.applyRecordPromotionIssued(cmd.RecordPromotionIssued)
	case cmd.RecordPromotionAck != nil:
		return f.applyRecordPromotionAck(cmd.RecordPromotionAck)
	case cmd.PublishSafeSnapshot != nil:
		return f.applyPublishSafeSnapshot(cmd.PublishSafeSnapshot)
	case cmd.ScheduleUnsafeCleanup != nil:
		return f.applyScheduleUnsafeCleanup(cmd.ScheduleUnsafeCleanup)
	case cmd.RecordCleanupAck != nil:
		return f.applyRecordCleanupAck(cmd.RecordCleanupAck)
	case cmd.OpenChallenge != nil:
		return f.applyOpenChallenge(cmd.OpenChallenge)
	case cmd.ResolveChallenge != nil:
		return f.applyResolveChallenge(cmd.ResolveChallenge)
	case cmd.RegisterNode != nil:
		return f.applyRegisterNode(cmd.RegisterNode)
	case cmd.MarkActive != nil:
		return f.applyMarkActive(cmd.MarkActive)
	case cmd.EvictNode != nil:
		return f.applyEvictNode(cmd.EvictNode)
	default:
		return Rejected{Reason: "empty command"}
	}
}

// Summary is a cross-package, read-only fingerprint of the state used by
// determinism and cluster tests (and P1b observability).
type Summary struct {
	NextStatementSeq uint64
	NextL3BlockSeq   uint64
	SpentIDsRoot     string
	LastChainHash    string
	SafeSnapshotID   string
	SafeBlockSeq     uint64
	StatementCount   int
	ActiveNodes      int
}

func (f *FSM) Summary() (Summary, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := Summary{
		NextStatementSeq: f.st.NextStatementSeq,
		NextL3BlockSeq:   f.st.NextL3BlockSeq,
		SpentIDsRoot:     "0x" + hex.EncodeToString(f.st.SpentIDs.Root()),
		SafeSnapshotID:   f.st.SafeWatermark.SnapshotID,
		SafeBlockSeq:     f.st.SafeWatermark.SafeBlockSeq,
		StatementCount:   len(f.st.Statements),
	}
	for _, n := range f.st.Nodes {
		if n.Status == NodeActive {
			s.ActiveNodes++
		}
	}
	if len(f.st.Blocks) > 0 {
		h, err := f.st.Blocks[len(f.st.Blocks)-1].ChainHash()
		if err != nil {
			return Summary{}, err
		}
		s.LastChainHash = h
	}
	return s, nil
}
```

- [ ] **Step 5: Implement `fsm/apply.go` (membership + stubs for later tasks)**

```go
package fsm

import (
	"crypto/ed25519"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

func hasRole(roles []arbiter.NodeRole, want arbiter.NodeRole) bool {
	for _, r := range roles {
		if r == want {
			return true
		}
	}
	return false
}

func (f *FSM) applyRegisterNode(c *wire.RegisterNode) any {
	r := c.Registration
	if r.NodeID == "" {
		return Rejected{Reason: "node_id required"}
	}
	if hasRole(r.Roles, arbiter.NodeRoleVerifier) && len(r.Ed25519Pubkey) != ed25519.PublicKeySize {
		return Rejected{Reason: "verifier registration requires a 32-byte ed25519 pubkey"}
	}
	// Re-registration replaces the record and resets to Syncing: a node
	// must re-prove snapshot sync before re-entering selection pools.
	f.st.Nodes[r.NodeID] = &NodeInfo{Registration: r, Status: NodeSyncing}
	return Applied{}
}

func (f *FSM) applyMarkActive(c *wire.MarkActive) any {
	n, ok := f.st.Nodes[c.NodeID]
	if !ok {
		return Rejected{Reason: "unknown node"}
	}
	if n.Status == NodeEvicted {
		return Rejected{Reason: "node is evicted; re-register first"}
	}
	n.Status = NodeActive
	return Applied{}
}

func (f *FSM) applyEvictNode(c *wire.EvictNode) any {
	n, ok := f.st.Nodes[c.NodeID]
	if !ok {
		return Rejected{Reason: "unknown node"}
	}
	n.Status = NodeEvicted
	n.Reason = c.Reason
	return Applied{}
}

// ---- stubs replaced by later tasks (7–13). Each returns a deterministic
// rejection so a premature command is a visible no-op, not a panic. ----

func (f *FSM) applySubmitStatement(c *wire.SubmitStatement) any {
	return Rejected{Reason: "not implemented: SubmitStatement (Task 7)"}
}
func (f *FSM) applySealL3Block() any {
	return Rejected{Reason: "not implemented: SealL3Block (Task 8)"}
}
func (f *FSM) applyMarkReplaying(c *wire.MarkReplaying) any {
	return Rejected{Reason: "not implemented: MarkReplaying (Task 8)"}
}
func (f *FSM) applyRegisterRC(c *wire.RegisterRC) any {
	return Rejected{Reason: "not implemented: RegisterRC (Task 9)"}
}
func (f *FSM) applyRecordAttestation(c *wire.RecordAttestation) any {
	return Rejected{Reason: "not implemented: RecordAttestation (Task 10)"}
}
func (f *FSM) applyRecordByteSideScan(c *wire.RecordByteSideScan) any {
	return Rejected{Reason: "not implemented: RecordByteSideScan (Task 10)"}
}
func (f *FSM) applyRecordAnchorFinality(c *wire.RecordAnchorFinality) any {
	return Rejected{Reason: "not implemented: RecordAnchorFinality (Task 12)"}
}
func (f *FSM) applyRecordPromotionIssued(c *wire.RecordPromotionIssued) any {
	return Rejected{Reason: "not implemented: RecordPromotionIssued (Task 12)"}
}
func (f *FSM) applyRecordPromotionAck(c *wire.RecordPromotionAck) any {
	return Rejected{Reason: "not implemented: RecordPromotionAck (Task 13)"}
}
func (f *FSM) applyPublishSafeSnapshot(c *wire.PublishSafeSnapshot) any {
	return Rejected{Reason: "not implemented: PublishSafeSnapshot (Task 13)"}
}
func (f *FSM) applyScheduleUnsafeCleanup(c *wire.ScheduleUnsafeCleanup) any {
	return Rejected{Reason: "not implemented: ScheduleUnsafeCleanup (Task 13)"}
}
func (f *FSM) applyRecordCleanupAck(c *wire.RecordCleanupAck) any {
	return Rejected{Reason: "not implemented: RecordCleanupAck (Task 13)"}
}
func (f *FSM) applyOpenChallenge(c *wire.OpenChallenge) any {
	return Rejected{Reason: "not implemented: OpenChallenge (Task 12)"}
}
func (f *FSM) applyResolveChallenge(c *wire.ResolveChallenge) any {
	return Rejected{Reason: "not implemented: ResolveChallenge (Task 12)"}
}
```

Snapshot/Restore are required by `raft.FSM`; until Task 6 lands, add to `fsm/fsm.go`:

```go
// Snapshot / Restore are implemented in Task 6 (snapshot.go); these
// temporary bodies satisfy raft.FSM so Task 5 compiles.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return nil, fmt.Errorf("not implemented: Snapshot (Task 6)")
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	rc.Close()
	return fmt.Errorf("not implemented: Restore (Task 6)")
}
```

(add `io` to the imports; Task 6 deletes these two bodies).

- [ ] **Step 6: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -12`
Expected: ALL PASS (membership lifecycle, rejections, garbage decode, fresh summary).

- [ ] **Step 7: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): state skeleton, Apply dispatch, membership handlers

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 6: fsm snapshot container (Snapshot / Restore)

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Create: `fsm/snapshot.go`, `fsm/snapshot_test.go`
- Modify: `fsm/fsm.go` (delete the two temporary Snapshot/Restore bodies)

**Interfaces:**
- Consumes: Task 5 `State`; `accumulator.SpentIDs.Snapshot(w io.Writer) error` / `Restore(r io.Reader) error`; `arbiter.TablePartition` TextMarshaler (Task 3).
- Produces: working `(*FSM).Snapshot() (raft.FSMSnapshot, error)` and `(*FSM).Restore(io.ReadCloser) error`; container format `"AFSM" ‖ ver u8=1 ‖ u64 BE JSON length ‖ JSON(snapshotDoc) ‖ SpentIDs canonical dump`; Task 14/15 rely on round-trip fidelity.

- [ ] **Step 1: Write the failing round-trip test**

Create `fsm/snapshot_test.go`:

```go
package fsm

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// memSink is a minimal raft.SnapshotSink writing into a buffer.
type memSink struct {
	bytes.Buffer
	canceled bool
}

func (s *memSink) ID() string    { return "test" }
func (s *memSink) Cancel() error { s.canceled = true; return nil }
func (s *memSink) Close() error  { return nil }

func snapshotBytes(t *testing.T, f *FSM) []byte {
	t.Helper()
	snap, err := f.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	sink := &memSink{}
	if err := snap.Persist(sink); err != nil {
		t.Fatalf("Persist: %v", err)
	}
	snap.Release()
	return sink.Bytes()
}

func restoreInto(t *testing.T, data []byte) *FSM {
	t.Helper()
	g := New(Params{}) // params come from the snapshot, not the constructor
	if err := g.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	return g
}

func TestSnapshotRoundTrip_MembershipAndPartitions(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	registerActive(t, f, "v1", arbiter.NodeRoleVerifier)
	// populate non-command-reachable fields directly (white-box: same package)
	tp := arbiter.TablePartition{TableID: "db.t", PartitionID: "p0"}
	f.st.Partitions[tp] = &PartitionState{BasePartitionRoot: "0xabcd", PublishSeq: 3}
	f.st.PromotedUnsafe[tp] = map[string]bool{"0xffee": true}
	f.st.SafeWatermark = SafeWatermark{SnapshotID: "s-1", SafeBlockSeq: 9, ManifestRoot: "0xmr"}

	g := restoreInto(t, snapshotBytes(t, f))

	if !reflect.DeepEqual(f.st.Nodes, g.st.Nodes) {
		t.Fatal("Nodes diverged")
	}
	if !reflect.DeepEqual(f.st.Partitions, g.st.Partitions) {
		t.Fatal("Partitions (TablePartition-keyed map) diverged")
	}
	if !reflect.DeepEqual(f.st.PromotedUnsafe, g.st.PromotedUnsafe) {
		t.Fatal("PromotedUnsafe diverged")
	}
	if f.st.SafeWatermark != g.st.SafeWatermark {
		t.Fatal("SafeWatermark diverged")
	}
	if g.st.Params != f.st.Params {
		t.Fatal("Params must travel in the snapshot")
	}
	sf, _ := f.Summary()
	sg, _ := g.Summary()
	if sf != sg {
		t.Fatalf("Summary diverged: %+v vs %+v", sf, sg)
	}
}

func TestSnapshotRoundTrip_RebuildsDerivedIndex(t *testing.T) {
	f := newTestFSM(t)
	// hand-place a statement (SubmitStatement lands in Task 7)
	env := arbiter.StatementEnvelope{StatementID: arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"}, SQL: "INSERT"}
	f.st.Statements[1] = &StatementState{Env: env, Seq: 1, Status: StatusSequenced}
	f.st.ByStatementID[env.StatementID.Flat()] = 1
	f.st.NextStatementSeq = 2

	g := restoreInto(t, snapshotBytes(t, f))
	if got := g.st.ByStatementID[env.StatementID.Flat()]; got != 1 {
		t.Fatalf("ByStatementID must be rebuilt on restore, got %d", got)
	}
	if !reflect.DeepEqual(f.st.Statements, g.st.Statements) {
		t.Fatal("Statements diverged")
	}
}

func TestRestoreRejectsCorruptContainer(t *testing.T) {
	g := New(Params{})
	if err := g.Restore(io.NopCloser(bytes.NewReader([]byte("XXXX")))); err == nil {
		t.Fatal("bad magic must error")
	}
}

var _ raft.FSM = (*FSM)(nil) // still satisfied with the real methods
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run TestSnapshot -v 2>&1 | tail -5`
Expected: FAIL — `Snapshot` returns "not implemented".

- [ ] **Step 3: Implement `fsm/snapshot.go`** (and delete the two temporary bodies + the `io`/`fmt` leftovers they used from `fsm.go`)

```go
package fsm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/hashicorp/raft"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/accumulator"
)

var snapshotMagic = [4]byte{'A', 'F', 'S', 'M'}

const snapshotVersion = 1

// snapshotDoc is State minus the accumulator (appended as its own
// canonical dump) and minus derived indexes (rebuilt on restore).
// Snapshot bytes are never compared across nodes, so encoding/json is
// sufficient (Restore ∘ Snapshot ≡ id is the only requirement).
type snapshotDoc struct {
	NextStatementSeq  uint64                                     `json:"next_statement_seq"`
	NextL3BlockSeq    uint64                                     `json:"next_l3_block_seq"`
	OpenBlock         *OpenL3Block                               `json:"open_block"`
	Blocks            []L3BlockHeader                            `json:"blocks,omitempty"`
	Statements        map[uint64]*StatementState                 `json:"statements,omitempty"`
	PendingRC         map[string]*arbiter.RCRecord               `json:"pending_rc,omitempty"`
	Verifications     map[uint64]*BlockVerification              `json:"verifications,omitempty"`
	PromotionSeq      uint64                                     `json:"promotion_seq,omitempty"`
	PendingPromotions map[uint64]*PendingPromotion               `json:"pending_promotions,omitempty"`
	PendingCleanups   map[uint64]*arbiter.UnsafeCleanup          `json:"pending_cleanups,omitempty"`
	Partitions        map[arbiter.TablePartition]*PartitionState `json:"partitions,omitempty"`
	SafeWatermark     SafeWatermark                              `json:"safe_watermark"`
	Manifests         map[string]*replay.SafeSnapshotManifest    `json:"manifests,omitempty"`
	PromotedUnsafe    map[arbiter.TablePartition]map[string]bool `json:"promoted_unsafe,omitempty"`
	Nodes             map[string]*NodeInfo                       `json:"nodes,omitempty"`
	Params            Params                                     `json:"params"`
}

func writeSnapshot(w io.Writer, st *State) error {
	doc := snapshotDoc{
		NextStatementSeq: st.NextStatementSeq, NextL3BlockSeq: st.NextL3BlockSeq,
		OpenBlock: st.OpenBlock, Blocks: st.Blocks, Statements: st.Statements,
		PendingRC: st.PendingRC, Verifications: st.Verifications,
		PromotionSeq: st.PromotionSeq, PendingPromotions: st.PendingPromotions,
		PendingCleanups: st.PendingCleanups, Partitions: st.Partitions,
		SafeWatermark: st.SafeWatermark, Manifests: st.Manifests,
		PromotedUnsafe: st.PromotedUnsafe, Nodes: st.Nodes, Params: st.Params,
	}
	jb, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal snapshot doc: %w", err)
	}
	if _, err := w.Write(snapshotMagic[:]); err != nil {
		return err
	}
	if _, err := w.Write([]byte{snapshotVersion}); err != nil {
		return err
	}
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(jb)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if _, err := w.Write(jb); err != nil {
		return err
	}
	return st.SpentIDs.Snapshot(w)
}

func readSnapshot(r io.Reader) (*State, error) {
	var magic [4]byte
	if _, err := io.ReadFull(r, magic[:]); err != nil {
		return nil, fmt.Errorf("snapshot magic: %w", err)
	}
	if magic != snapshotMagic {
		return nil, fmt.Errorf("snapshot magic mismatch: %q", magic[:])
	}
	var ver [1]byte
	if _, err := io.ReadFull(r, ver[:]); err != nil {
		return nil, fmt.Errorf("snapshot version: %w", err)
	}
	if ver[0] != snapshotVersion {
		return nil, fmt.Errorf("unsupported snapshot version %d", ver[0])
	}
	var lenBuf [8]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("snapshot doc length: %w", err)
	}
	jb := make([]byte, binary.BigEndian.Uint64(lenBuf[:]))
	if _, err := io.ReadFull(r, jb); err != nil {
		return nil, fmt.Errorf("snapshot doc: %w", err)
	}
	var doc snapshotDoc
	if err := json.Unmarshal(jb, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal snapshot doc: %w", err)
	}
	acc := accumulator.NewSpentIDs()
	if err := acc.Restore(r); err != nil {
		return nil, fmt.Errorf("restore spent-ids: %w", err)
	}
	st := newState(doc.Params)
	st.NextStatementSeq = doc.NextStatementSeq
	st.NextL3BlockSeq = doc.NextL3BlockSeq
	if doc.OpenBlock != nil {
		st.OpenBlock = doc.OpenBlock
	}
	st.Blocks = doc.Blocks
	st.SpentIDs = acc
	if doc.Statements != nil {
		st.Statements = doc.Statements
	}
	if doc.PendingRC != nil {
		st.PendingRC = doc.PendingRC
	}
	if doc.Verifications != nil {
		st.Verifications = doc.Verifications
	}
	st.PromotionSeq = doc.PromotionSeq
	if doc.PendingPromotions != nil {
		st.PendingPromotions = doc.PendingPromotions
	}
	if doc.PendingCleanups != nil {
		st.PendingCleanups = doc.PendingCleanups
	}
	if doc.Partitions != nil {
		st.Partitions = doc.Partitions
	}
	st.SafeWatermark = doc.SafeWatermark
	if doc.Manifests != nil {
		st.Manifests = doc.Manifests
	}
	if doc.PromotedUnsafe != nil {
		st.PromotedUnsafe = doc.PromotedUnsafe
	}
	if doc.Nodes != nil {
		st.Nodes = doc.Nodes
	}
	// rebuild derived indexes
	st.ByStatementID = make(map[string]uint64, len(st.Statements))
	for seq, ss := range st.Statements {
		st.ByStatementID[ss.Env.StatementID.Flat()] = seq
	}
	return st, nil
}

// fsmSnapshot is a point-in-time byte capture. Snapshot() serializes
// synchronously (v1 state is small, design §5); Persist just streams.
type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

// Snapshot implements raft.FSM.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var buf bytes.Buffer
	if err := writeSnapshot(&buf, f.st); err != nil {
		return nil, err
	}
	return &fsmSnapshot{data: buf.Bytes()}, nil
}

// Restore implements raft.FSM: it replaces the whole state.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	st, err := readSnapshot(rc)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.st = st
	f.mu.Unlock()
	return nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -10`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): versioned snapshot container with accumulator dump

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 7: admission — `SubmitStatement` (§6.3 assembly) + source selection

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Create: `fsm/admission.go`, `fsm/userjws.go`, `fsm/select.go`, `fsm/admission_test.go`
- Modify: `fsm/apply.go` (delete the `applySubmitStatement` stub)

**Interfaces:**
- Consumes: `accumulator.SpentIDs.Status/Insert`, `accumulator.Status*` constants, `accumulator.ErrGapBudgetExceeded`, `replay.DigestString`, go-ethereum `crypto` (Keccak256, SigToPub, PubkeyToAddress), Task 3 `arbiter.SourceSelectSeedPrefix`.
- Produces: real `applySubmitStatement` returning `SubmitResult`; `verifyUserJWS(token, sql, clientAccount string) error` (wall-clock-free); `(*FSM).selectSource(flatID string) string`; `u64FromDigest(digest string) uint64`; `(*FSM).activeNodesWithRole(role, excluded) []string`; `(*FSM).bindRC(ss *StatementState, rc *arbiter.RCRecord)` (also used by Task 9); test helper `signUserJWS`.

- [ ] **Step 1: Write the failing admission tests**

Create `fsm/admission_test.go`:

```go
package fsm

import (
	"crypto/ecdsa"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// signUserJWS builds a housegate-shaped user query JWS: payload
// {iat, qhash=keccak256Hex(sql)}, ES256K over keccak256(signingInput).
// iat is a fixed constant — Apply never reads the clock.
func signUserJWS(t *testing.T, key *ecdsa.PrivateKey, sql string) string {
	t.Helper()
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256K","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"iat":1700000000,"qhash":"0x%x"}`, crypto.Keccak256([]byte(sql)))))
	signingInput := header + "." + payload
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func testAccount(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	return key, strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
}

func validEnvelope(t *testing.T, key *ecdsa.PrivateKey, account string, seq uint64) arbiter.StatementEnvelope {
	t.Helper()
	sql := fmt.Sprintf("INSERT INTO db.t VALUES (%d)", seq)
	return arbiter.StatementEnvelope{
		StatementID:   arbiter.StatementID{ClientAccount: account, ClientSeq: seq, ClientNonce: "n"},
		StatementKind: arbiter.StatementKindInsert,
		SQL:           sql,
		SQLHash:       replay.DigestString(sql),
		TargetTableID: "db.t",
		UserJWS:       signUserJWS(t, key, sql),
	}
}

func submit(t *testing.T, f *FSM, env arbiter.StatementEnvelope) SubmitResult {
	t.Helper()
	res := f.Apply(mkLog(t, wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: env}}))
	sr, ok := res.(SubmitResult)
	if !ok {
		t.Fatalf("expected SubmitResult, got %T %+v", res, res)
	}
	return sr
}

func TestAdmission_AcceptAndAssignSeq(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	r1 := submit(t, f, validEnvelope(t, key, account, 1))
	if r1.Code != arbiter.AdmissionCodeAccepted || r1.StatementSeq != 1 {
		t.Fatalf("first: %+v", r1)
	}
	r2 := submit(t, f, validEnvelope(t, key, account, 2))
	if r2.Code != arbiter.AdmissionCodeAccepted || r2.StatementSeq != 2 {
		t.Fatalf("second: %+v", r2)
	}
	ss := f.st.Statements[1]
	if ss.Status != StatusSequenced || ss.SourceNode != "s1" {
		t.Fatalf("statement state: %+v", ss)
	}
	if len(f.st.OpenBlock.StatementSeqs) != 2 {
		t.Fatalf("open block: %+v", f.st.OpenBlock)
	}
}

func TestAdmission_Codes(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	otherKey, _ := testAccount(t)

	base := func(seq uint64) arbiter.StatementEnvelope { return validEnvelope(t, key, account, seq) }

	// duplicate client_seq
	if r := submit(t, f, base(1)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("setup: %+v", r)
	}
	if r := submit(t, f, base(1)); r.Code != arbiter.AdmissionCodeDuplicateClientSeq {
		t.Fatalf("duplicate: %+v", r)
	}

	// malformed: empty sql
	e := base(2)
	e.SQL, e.SQLHash = "", replay.DigestString("")
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeMalformed {
		t.Fatalf("empty sql: %+v", r)
	}
	// malformed: sql_hash does not bind
	e = base(2)
	e.SQLHash = "0xdeadbeef"
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeMalformed {
		t.Fatalf("sql_hash: %+v", r)
	}
	// malformed: client_seq zero
	e = validEnvelope(t, key, account, 0)
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeMalformed {
		t.Fatalf("seq zero: %+v", r)
	}
	// malformed: non-empty proof in v1
	e = base(2)
	res := f.Apply(mkLog(t, wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: e, NonMembershipProof: []byte{1}}}))
	if sr := res.(SubmitResult); sr.Code != arbiter.AdmissionCodeMalformed {
		t.Fatalf("non-empty proof: %+v", sr)
	}

	// kind not admitted
	e = base(2)
	e.StatementKind = arbiter.StatementKindUnspecified
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeKindNotAdmitted {
		t.Fatalf("kind: %+v", r)
	}

	// invalid signature: signed by another key
	e = base(2)
	e.UserJWS = signUserJWS(t, otherKey, e.SQL)
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("wrong signer: %+v", r)
	}
	// invalid signature: JWS binds different sql
	e = base(2)
	e.UserJWS = signUserJWS(t, key, "INSERT INTO db.t VALUES (999)")
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("qhash mismatch: %+v", r)
	}
	// invalid signature: not a JWS
	e = base(2)
	e.UserJWS = "garbage"
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("garbage jws: %+v", r)
	}

	// gap-fill accepted (out-of-order legitimate seq)
	if r := submit(t, f, base(5)); r.Code != arbiter.AdmissionCodeAccepted { // opens gap [2,4]
		t.Fatalf("jump: %+v", r)
	}
	if r := submit(t, f, base(3)); r.Code != arbiter.AdmissionCodeAccepted { // splits the gap
		t.Fatalf("gap fill: %+v", r)
	}
}

func TestAdmission_GapBudgetExceeded(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	// every even seq 2,4,...,128 creates one open range [odd,odd] → 64 ranges = K
	for s := uint64(2); s <= 128; s += 2 {
		if r := submit(t, f, validEnvelope(t, key, account, s)); r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("seq %d: %+v", s, r)
		}
	}
	// the 65th range must be rejected with the distinct code
	if r := submit(t, f, validEnvelope(t, key, account, 130)); r.Code != arbiter.AdmissionCodeGapBudgetExceeded {
		t.Fatalf("want GAP_BUDGET_EXCEEDED, got %+v", r)
	}
	// remedy: continuing from the high-water mark still works
	if r := submit(t, f, validEnvelope(t, key, account, 129)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("hi+1 after budget rejection: %+v", r)
	}
}

func TestSourceSelection_DeterministicOverPool(t *testing.T) {
	build := func() *FSM {
		f := newTestFSM(t)
		for _, id := range []string{"s1", "s2", "s3"} {
			registerActive(t, f, id, arbiter.NodeRoleSNode)
		}
		return f
	}
	f1, f2 := build(), build()
	key, account := testAccount(t)
	for seq := uint64(1); seq <= 8; seq++ {
		e := validEnvelope(t, key, account, seq)
		a := submit(t, f1, e)
		b := submit(t, f2, e)
		if a.Code != arbiter.AdmissionCodeAccepted || b.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("seq %d: %+v / %+v", seq, a, b)
		}
		if f1.st.Statements[a.StatementSeq].SourceNode != f2.st.Statements[b.StatementSeq].SourceNode {
			t.Fatalf("seq %d: source selection diverged", seq)
		}
	}
	// empty pool records ""
	f3 := newTestFSM(t)
	r := submit(t, f3, validEnvelope(t, key, account, 1))
	if r.Code != arbiter.AdmissionCodeAccepted || f3.st.Statements[1].SourceNode != "" {
		t.Fatalf("empty pool: %+v src=%q", r, f3.st.Statements[1].SourceNode)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run 'TestAdmission|TestSourceSelection' 2>&1 | tail -5`
Expected: FAIL — stub returns `Rejected{"not implemented: SubmitStatement (Task 7)"}`, so `submit()` fatals on the type assertion.

- [ ] **Step 3: Implement `fsm/userjws.go`**

```go
package fsm

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
)

// verifyUserJWS re-verifies the user's SQL-binding JWS deterministically:
// alg, qhash = keccak256Hex(sql), and secp256k1 recovery == client_account.
// It deliberately performs NO iat/expiry check — Apply is wall-clock-free
// (§4.3 red line 1); freshness is the leader ingress edge's job (P1b).
func verifyUserJWS(token, sql, clientAccount string) error {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return fmt.Errorf("user_jws: want compact JWS with 3 parts, got %d", len(parts))
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return fmt.Errorf("user_jws header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return fmt.Errorf("user_jws header: %w", err)
	}
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return fmt.Errorf("user_jws: unexpected alg %q", header.Alg)
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return fmt.Errorf("user_jws payload: %w", err)
	}
	var payload struct {
		Iat   int64  `json:"iat"`
		Qhash string `json:"qhash"`
	}
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return fmt.Errorf("user_jws payload: %w", err)
	}
	wantQhash := "0x" + hex.EncodeToString(crypto.Keccak256([]byte(sql)))
	if !strings.EqualFold(payload.Qhash, wantQhash) {
		return fmt.Errorf("user_jws: qhash does not bind sql")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("user_jws signature: %w", err)
	}
	if len(sig) != 65 {
		return fmt.Errorf("user_jws signature: want 65 bytes, got %d", len(sig))
	}
	recSig := make([]byte, 65)
	copy(recSig, sig)
	if recSig[64] >= 27 {
		recSig[64] -= 27
	}
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(parts[0]+"."+parts[1])), recSig)
	if err != nil {
		return fmt.Errorf("user_jws: recover signer: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex())
	if addr != strings.ToLower(clientAccount) {
		return fmt.Errorf("user_jws: recovered %s does not match client_account", addr)
	}
	return nil
}
```

- [ ] **Step 4: Implement `fsm/select.go`**

```go
package fsm

import (
	"sort"
	"strconv"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
)

// u64FromDigest parses digest[2:18] (the first 8 hash bytes of a
// "0x"-prefixed DigestString) as a big-endian uint64 — the frozen index
// derivation for both selections (design §7).
func u64FromDigest(digest string) uint64 {
	v, err := strconv.ParseUint(digest[2:18], 16, 64)
	if err != nil {
		// DigestString output is always 0x + 64 hex chars; anything else
		// is a programming error, not an input condition.
		panic("fsm: malformed digest: " + digest)
	}
	return v
}

// activeNodesWithRole returns the committed Active nodes holding role,
// minus excluded, sorted by NodeID (canonical order, §4.3 red line 1).
func (f *FSM) activeNodesWithRole(role arbiter.NodeRole, excluded map[string]bool) []string {
	var ids []string
	for id, n := range f.st.Nodes {
		if n.Status != NodeActive || excluded[id] {
			continue
		}
		if hasRole(n.Registration.Roles, role) {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

// selectSource picks the statement's source SNode (§5.4): hash-mod over
// the sorted Active writer pool. Empty pool → "" (no RC will ever match;
// zero-writer pools are an operational error, documented not special-cased).
func (f *FSM) selectSource(flatID string) string {
	pool := f.activeNodesWithRole(arbiter.NodeRoleSNode, nil)
	if len(pool) == 0 {
		return ""
	}
	return pool[u64FromDigest(replay.DigestString(arbiter.SourceSelectSeedPrefix+flatID))%uint64(len(pool))]
}

// selectVerifiers picks exactly VerifierSelectN verifiers for a block
// (§7.1 + Open Q3 resolution): the sorted Active verifier ring minus the
// block's sources, 3 consecutive positions from a block-seeded start.
// Returns nil when the pool is smaller than VerifierSelectN — the
// machine-enforced §7.4 pool floor.
func (f *FSM) selectVerifiers(blockSeq uint64, sources []string) []string {
	excluded := map[string]bool{}
	for _, s := range sources {
		excluded[s] = true
	}
	ring := f.activeNodesWithRole(arbiter.NodeRoleVerifier, excluded)
	if len(ring) < VerifierSelectN {
		return nil
	}
	start := u64FromDigest(replay.DigestString(arbiter.VerifierSelectSeedPrefix+strconv.FormatUint(blockSeq, 10))) % uint64(len(ring))
	out := make([]string, 0, VerifierSelectN)
	for i := uint64(0); i < VerifierSelectN; i++ {
		out = append(out, ring[(start+i)%uint64(len(ring))])
	}
	sort.Strings(out)
	return out
}
```

- [ ] **Step 5: Implement `fsm/admission.go`** (and delete the `applySubmitStatement` stub from `apply.go`)

```go
package fsm

import (
	"errors"
	"strings"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/accumulator"
	"github.com/sentioxyz/arbiter/wire"
)

// applySubmitStatement is the §6.3 admission assembly (P1a spec §6). All
// steps read committed state + logged bytes only; no wall clock anywhere.
func (f *FSM) applySubmitStatement(c *wire.SubmitStatement) any {
	env := c.Envelope
	id := env.StatementID

	// 1. shape
	if id.ClientAccount == "" || id.ClientNonce == "" || env.SQL == "" {
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "statement_id/sql incomplete"}
	}
	if len(c.NonMembershipProof) != 0 {
		// v1: the FSM holds the full dictionary; a carried proof is
		// protocol misuse (strict reject; INVALID_PROOF stays reserved
		// for the decentralized phase that actually consumes proofs).
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "non_membership_proof must be empty in v1"}
	}
	if id.ClientSeq == 0 {
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "client_seq 0 is invalid"}
	}
	if env.SQLHash != replay.DigestString(env.SQL) {
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "sql_hash does not bind sql"}
	}

	// 2. kind (frozen: INSERT only in v1)
	if env.StatementKind != arbiter.StatementKindInsert {
		return SubmitResult{Code: arbiter.AdmissionCodeKindNotAdmitted, Message: "v1 admits INSERT only"}
	}

	// 3. signature, wall-clock-free
	acct := strings.ToLower(id.ClientAccount)
	if err := verifyUserJWS(env.UserJWS, env.SQL, acct); err != nil {
		return SubmitResult{Code: arbiter.AdmissionCodeInvalidSignature, Message: err.Error()}
	}

	// 4. schema/settings allowlist: v1 has no DDL/schema registry — the
	// seam accepts everything; SCHEMA_NOT_ALLOWED is reserved for the P2
	// schema-transition lane.

	// 5. dedup admission over the P0b primitives
	coord := arbiter.StatementCoord{Account: acct, ClientSeq: id.ClientSeq}
	if st := f.st.SpentIDs.Status(coord); st == accumulator.StatusSpentDuplicate {
		return SubmitResult{Code: arbiter.AdmissionCodeDuplicateClientSeq, Message: "client_seq already spent (client bug: do not retry with the same seq)"}
	}
	if err := f.st.SpentIDs.Insert(coord); err != nil {
		if errors.Is(err, accumulator.ErrGapBudgetExceeded) {
			return SubmitResult{Code: arbiter.AdmissionCodeGapBudgetExceeded, Message: err.Error()}
		}
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: err.Error()}
	}

	// 6. assign + bind (account normalized into the stored envelope)
	seq := f.st.NextStatementSeq
	f.st.NextStatementSeq++
	norm := env
	norm.StatementID.ClientAccount = acct
	flat := norm.StatementID.Flat()
	ss := &StatementState{Env: norm, Seq: seq, Status: StatusSequenced, SourceNode: f.selectSource(flat)}
	f.st.Statements[seq] = ss
	f.st.ByStatementID[flat] = seq
	if rc, ok := f.st.PendingRC[flat]; ok {
		delete(f.st.PendingRC, flat)
		if rc.SourceNode == ss.SourceNode {
			f.bindRC(ss, rc)
		}
		// source mismatch: the parked claim is discarded (design §6 step 6)
	}
	f.st.OpenBlock.StatementSeqs = append(f.st.OpenBlock.StatementSeqs, seq)
	return SubmitResult{Code: arbiter.AdmissionCodeAccepted, StatementSeq: seq}
}

// bindRC attaches a validated result claim to a statement (§5.5 late
// binding; Task 9's RegisterRC also calls this) and re-evaluates the
// statement's block if it is already sealed and marked.
func (f *FSM) bindRC(ss *StatementState, rc *arbiter.RCRecord) {
	ss.RC = rc
	ss.UnpromotedParts = make(map[string]bool, len(rc.CandidateParts))
	for _, cp := range rc.CandidateParts {
		ss.UnpromotedParts[cp.PartRowLtHash] = true
	}
	if ss.Status == StatusSequenced {
		ss.Status = StatusUnsafeRegistered
	}
	f.reevaluateBlock(ss.BlockSeq)
}

// reevaluateBlock re-runs the three-way evaluation; Task 11 implements it.
// Until then it is a deterministic no-op.
func (f *FSM) reevaluateBlock(blockSeq uint64) {}
```

Note: Task 11 REPLACES the no-op `reevaluateBlock` with the real evaluation in `fsm/threeway.go` (delete the placeholder here when Task 11 lands).

- [ ] **Step 6: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -12`
Expected: ALL PASS (admission table, gap budget at the K=64 boundary, deterministic selection, empty pool).

- [ ] **Step 7: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): SubmitStatement admission over P0b primitives + source selection

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 8: `SealL3Block` + `MarkReplaying` (header chain + verifier selection)

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/apply.go` (replace the two stubs)
- Create: `fsm/seal_test.go`

**Interfaces:**
- Consumes: Task 5 `L3BlockHeader.ChainHash()`, Task 7 `selectVerifiers`, `SpentIDs.Root()`.
- Produces: real `applySealL3Block` (returns `SealResult`) and `applyMarkReplaying`; helper `(*FSM).forEachBlockStatement(blockSeq uint64, fn func(*StatementState))` and `(*FSM).blockStatements(blockSeq uint64) []*StatementState` (statement_seq order — Task 11 consumes both).

- [ ] **Step 1: Write the failing tests**

Create `fsm/seal_test.go`:

```go
package fsm

import (
	"testing"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// sealBlock admits n statements from a fresh account and seals them into
// the next block; returns the SealResult.
func sealBlock(t *testing.T, f *FSM, n int) SealResult {
	t.Helper()
	key, account := testAccount(t)
	for i := 0; i < n; i++ {
		hi, _, _ := f.st.SpentIDs.AccountState(account)
		if r := submit(t, f, validEnvelope(t, key, account, hi+1)); r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("submit %d: %+v", i, r)
		}
	}
	res := f.Apply(mkLog(t, wire.Command{SealL3Block: &wire.SealL3Block{}}))
	sr, ok := res.(SealResult)
	if !ok {
		t.Fatalf("expected SealResult, got %T %+v", res, res)
	}
	return sr
}

func TestSeal_EmptyBlockRejected(t *testing.T) {
	f := newTestFSM(t)
	applyExpectReject(t, f, wire.Command{SealL3Block: &wire.SealL3Block{}})
}

func TestSeal_HeaderChainAndAnchorExclusion(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)

	r1 := sealBlock(t, f, 2)
	if r1.BlockSeq != 1 {
		t.Fatalf("first block seq: %+v", r1)
	}
	h1 := f.st.Blocks[0]
	if h1.PrevL3Hash != "" {
		t.Fatal("genesis block must chain from the empty string")
	}
	if h1.StatementSeqStart != 1 || h1.StatementCount != 2 {
		t.Fatalf("header range: %+v", h1)
	}
	if h1.SchemaSnapshotID != "schema-genesis" || h1.ExecutorProfileID != "housegate-replay-mvp-v0" {
		t.Fatalf("params not stamped: %+v", h1)
	}
	if h1.SpentIDsRootAfter == "" {
		t.Fatal("SpentIDsRootAfter must be stamped")
	}
	// statements got their BlockSeq; verification shell created
	if f.st.Statements[1].BlockSeq != 1 || f.st.Verifications[1] == nil {
		t.Fatal("seal must stamp BlockSeq and create the verification shell")
	}
	if got := f.st.Verifications[1].SourceNodes; len(got) != 1 || got[0] != "s1" {
		t.Fatalf("source union: %v", got)
	}

	r2 := sealBlock(t, f, 1)
	h2 := f.st.Blocks[1]
	want, err := h1.ChainHash()
	if err != nil {
		t.Fatalf("ChainHash: %v", err)
	}
	if h2.PrevL3Hash != want {
		t.Fatal("second header must chain from the first's ChainHash")
	}
	if r2.ChainHash == "" || r2.ChainHash == r1.ChainHash {
		t.Fatalf("seal results must carry distinct chain hashes: %+v %+v", r1, r2)
	}
	// anchor back-fill must not change the chained content
	withAnchor := h1
	withAnchor.L2AnchorRef = &arbiter.AnchorRef{L3BlockHash: "0xaa", StateRoot: "0xbb"}
	got, err := withAnchor.ChainHash()
	if err != nil {
		t.Fatalf("ChainHash: %v", err)
	}
	if got != want {
		t.Fatal("ChainHash must exclude the back-filled anchor")
	}
	// open block reset
	if len(f.st.OpenBlock.StatementSeqs) != 0 || f.st.OpenBlock.StatementSeqStart != f.st.NextStatementSeq {
		t.Fatalf("open block not reset: %+v", f.st.OpenBlock)
	}
}

func TestMarkReplaying_SelectsThreeDeterministically(t *testing.T) {
	build := func() *FSM {
		f := newTestFSM(t)
		registerActive(t, f, "s1", arbiter.NodeRoleSNode)
		for _, id := range []string{"v1", "v2", "v3", "v4", "v5"} {
			registerActive(t, f, id, arbiter.NodeRoleVerifier)
		}
		return f
	}
	f1, f2 := build(), build()
	sealBlock(t, f1, 1)
	sealBlock(t, f2, 1)
	mustApply(t, f1, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	mustApply(t, f2, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	set1, set2 := f1.st.Verifications[1].VerifierSet, f2.st.Verifications[1].VerifierSet
	if len(set1) != 3 {
		t.Fatalf("must select exactly 3, got %v", set1)
	}
	for i := range set1 {
		if set1[i] != set2[i] {
			t.Fatalf("selection diverged: %v vs %v", set1, set2)
		}
	}
	if f1.st.Statements[1].Status != StatusReplaying {
		t.Fatal("statements must move to Replaying")
	}
	// double-mark rejected
	applyExpectReject(t, f1, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
}

func TestMarkReplaying_PoolFloor(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	registerActive(t, f, "v1", arbiter.NodeRoleVerifier)
	registerActive(t, f, "v2", arbiter.NodeRoleVerifier)
	sealBlock(t, f, 1)
	applyExpectReject(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	// unknown block
	applyExpectReject(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 42}})
	// source exclusion: a node that is both SNode and Verifier and IS the
	// block's source must not be selectable
	f2 := newTestFSM(t)
	registerActive(t, f2, "dual", arbiter.NodeRoleSNode, arbiter.NodeRoleVerifier)
	registerActive(t, f2, "v1", arbiter.NodeRoleVerifier)
	registerActive(t, f2, "v2", arbiter.NodeRoleVerifier)
	registerActive(t, f2, "v3", arbiter.NodeRoleVerifier)
	sealBlock(t, f2, 1) // source = "dual" (only SNode)
	mustApply(t, f2, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	for _, id := range f2.st.Verifications[1].VerifierSet {
		if id == "dual" {
			t.Fatal("the block's source must be excluded from the verifier set")
		}
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run 'TestSeal|TestMarkReplaying' 2>&1 | tail -5`
Expected: FAIL on the "not implemented" stubs.

- [ ] **Step 3: Replace the two stubs in `fsm/apply.go`**

Delete the `applySealL3Block` / `applyMarkReplaying` stubs and add (plus `"encoding/hex"` and `"sort"` imports):

```go
// blockStatements returns the block's statements in statement_seq order.
func (f *FSM) blockStatements(blockSeq uint64) []*StatementState {
	idx := int(blockSeq) - 1
	if idx < 0 || idx >= len(f.st.Blocks) {
		return nil
	}
	h := f.st.Blocks[idx]
	out := make([]*StatementState, 0, h.StatementCount)
	for seq := h.StatementSeqStart; seq < h.StatementSeqStart+uint64(h.StatementCount); seq++ {
		if ss, ok := f.st.Statements[seq]; ok {
			out = append(out, ss)
		}
	}
	return out
}

func (f *FSM) forEachBlockStatement(blockSeq uint64, fn func(*StatementState)) {
	for _, ss := range f.blockStatements(blockSeq) {
		fn(ss)
	}
}

func (f *FSM) applySealL3Block() any {
	ob := f.st.OpenBlock
	if len(ob.StatementSeqs) == 0 {
		return Rejected{Reason: "open block is empty"}
	}
	prevHash := ""
	if n := len(f.st.Blocks); n > 0 {
		h, err := f.st.Blocks[n-1].ChainHash()
		if err != nil {
			return Rejected{Reason: "chain hash: " + err.Error()}
		}
		prevHash = h
	}
	prevStateRoot := ""
	if m, ok := f.st.Manifests[f.st.SafeWatermark.SnapshotID]; ok {
		prevStateRoot = m.StateRoot
	}
	hdr := L3BlockHeader{
		L3BlockSeq:         f.st.NextL3BlockSeq,
		PrevL3Hash:         prevHash,
		StatementSeqStart:  ob.StatementSeqStart,
		StatementCount:     uint32(len(ob.StatementSeqs)),
		SchemaSnapshotID:   f.st.Params.SchemaSnapshotID,
		ExecutorProfileID:  f.st.Params.ExecutorProfileID,
		PrevSafeSnapshotID: f.st.SafeWatermark.SnapshotID,
		PrevStateRoot:      prevStateRoot,
		SpentIDsRootAfter:  "0x" + hex.EncodeToString(f.st.SpentIDs.Root()),
	}
	chainHash, err := hdr.ChainHash()
	if err != nil {
		return Rejected{Reason: "chain hash: " + err.Error()}
	}
	// stamp statements + collect the source union (sorted, deduped)
	srcSet := map[string]bool{}
	for _, seq := range ob.StatementSeqs {
		ss := f.st.Statements[seq]
		ss.BlockSeq = hdr.L3BlockSeq
		if ss.SourceNode != "" {
			srcSet[ss.SourceNode] = true
		}
	}
	sources := make([]string, 0, len(srcSet))
	for s := range srcSet {
		sources = append(sources, s)
	}
	sort.Strings(sources)
	f.st.Blocks = append(f.st.Blocks, hdr)
	f.st.Verifications[hdr.L3BlockSeq] = &BlockVerification{
		BlockSeq:     hdr.L3BlockSeq,
		SourceNodes:  sources,
		Attestations: map[string]*replay.ReplayAttestation{},
		ByteScans:    map[string]*arbiter.ByteSideScanMsg{},
	}
	f.st.NextL3BlockSeq++
	f.st.OpenBlock = &OpenL3Block{StatementSeqStart: f.st.NextStatementSeq}
	return SealResult{BlockSeq: hdr.L3BlockSeq, ChainHash: chainHash}
}

func (f *FSM) applyMarkReplaying(c *wire.MarkReplaying) any {
	bv, ok := f.st.Verifications[c.BlockSeq]
	if !ok {
		return Rejected{Reason: "unknown block"}
	}
	if len(bv.VerifierSet) > 0 {
		return Rejected{Reason: "block already marked replaying"}
	}
	set := f.selectVerifiers(c.BlockSeq, bv.SourceNodes)
	if set == nil {
		return Rejected{Reason: "active non-source verifier pool below 3 (§7.4 floor)"}
	}
	bv.VerifierSet = set
	f.forEachBlockStatement(c.BlockSeq, func(ss *StatementState) {
		if ss.Status == StatusSequenced || ss.Status == StatusUnsafeRegistered {
			ss.Status = StatusReplaying
		}
	})
	return Applied{}
}
```

`apply.go` gains imports `"encoding/hex"`, `"sort"`, and `"housegate/housegate/pkg/replay"`.

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -10`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): L3 sealing with anchor-excluded hash chain + deterministic 3-selection

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 9: `RegisterRC` — late binding both directions

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/apply.go` (replace the `applyRegisterRC` stub)
- Create: `fsm/rc_test.go`

**Interfaces:**
- Consumes: Task 7 `bindRC`, `StatementState.SourceNode`, `PendingRC` map.
- Produces: real `applyRegisterRC`; test helper `rcFor(t, f, seq, parts...)` used by Tasks 11–13.

- [ ] **Step 1: Write the failing tests**

Create `fsm/rc_test.go`:

```go
package fsm

import (
	"testing"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// rcFor builds an RCRecord for an admitted statement, claiming the given
// part lthash values in partition "p0" of the statement's target table,
// with a claimed source root.
func rcFor(f *FSM, seq uint64, sourceClaimRoot string, partHashes ...string) arbiter.RCRecord {
	ss := f.st.Statements[seq]
	parts := make([]arbiter.CandidatePart, 0, len(partHashes))
	for i, h := range partHashes {
		parts = append(parts, arbiter.CandidatePart{
			TableID: ss.Env.TargetTableID, PartitionID: "p0",
			PartName: "all_" + string(rune('1'+i)) + "_0", PartRowLtHash: h, RowCount: 1, Bytes: 32})
	}
	return arbiter.RCRecord{
		StatementID:     ss.Env.StatementID,
		SourceNode:      ss.SourceNode,
		CandidateParts:  parts,
		SourceClaimRoot: sourceClaimRoot,
	}
}

func TestRegisterRC_BindAfterSubmit(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	submit(t, f, validEnvelope(t, key, account, 1))

	rc := rcFor(f, 1, "0xr00t", "0xffee")
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	ss := f.st.Statements[1]
	if ss.Status != StatusUnsafeRegistered || ss.RC == nil {
		t.Fatalf("bind failed: %+v", ss)
	}
	if !ss.UnpromotedParts["0xffee"] {
		t.Fatal("UnpromotedParts must track candidate parts")
	}
	// identical duplicate absorbed
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	// conflicting content rejected (first wins)
	conflicting := rcFor(f, 1, "0xother", "0xffee")
	applyExpectReject(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: conflicting}})
	// wrong source rejected
	wrongSrc := rcFor(f, 1, "0xr00t", "0xffee")
	wrongSrc.SourceNode = "impostor"
	applyExpectReject(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: wrongSrc}})
}

func TestRegisterRC_ParkBeforeSubmitThenAdopt(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	env := validEnvelope(t, key, account, 1)

	// the RC arrives FIRST (route A optimistic write, §5.5)
	rc := arbiter.RCRecord{
		StatementID:     arbiter.StatementID{ClientAccount: account, ClientSeq: 1, ClientNonce: "n"},
		SourceNode:      "s1", // the only Active writer — must match selection
		CandidateParts:  []arbiter.CandidatePart{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: "0xffee"}},
		SourceClaimRoot: "0xr00t",
	}
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	if len(f.st.PendingRC) != 1 {
		t.Fatal("RC must park under statement_id before the seq exists")
	}
	// conflicting parked content rejected, identical absorbed
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	conflicting := rc
	conflicting.SourceClaimRoot = "0xother"
	applyExpectReject(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: conflicting}})

	// SubmitStatement adopts the parked claim
	if r := submit(t, f, env); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("submit: %+v", r)
	}
	ss := f.st.Statements[1]
	if ss.Status != StatusUnsafeRegistered || ss.RC == nil {
		t.Fatalf("adoption failed: %+v", ss)
	}
	if len(f.st.PendingRC) != 0 {
		t.Fatal("parked claim must be consumed")
	}
}

func TestRegisterRC_ParkedSourceMismatchDiscardedOnAdopt(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	rc := arbiter.RCRecord{
		StatementID:     arbiter.StatementID{ClientAccount: account, ClientSeq: 1, ClientNonce: "n"},
		SourceNode:      "impostor",
		SourceClaimRoot: "0xr00t",
	}
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	if r := submit(t, f, validEnvelope(t, key, account, 1)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("submit: %+v", r)
	}
	ss := f.st.Statements[1]
	if ss.RC != nil || ss.Status != StatusSequenced {
		t.Fatal("mismatched parked claim must be discarded, not adopted")
	}
	if len(f.st.PendingRC) != 0 {
		t.Fatal("mismatched parked claim must still be consumed (discarded)")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run TestRegisterRC 2>&1 | tail -5`
Expected: FAIL on the stub.

- [ ] **Step 3: Replace the `applyRegisterRC` stub in `fsm/apply.go`**

```go
func (f *FSM) applyRegisterRC(c *wire.RegisterRC) any {
	rc := c.RC
	flat := rc.StatementID.Flat()
	seq, ok := f.st.ByStatementID[flat]
	if !ok {
		// §5.5 late binding: park under statement_id until the seq exists.
		if prev, dup := f.st.PendingRC[flat]; dup {
			if reflect.DeepEqual(*prev, rc) {
				return Applied{} // idempotent re-registration
			}
			return Rejected{Reason: "conflicting RC already parked for this statement (first wins)"}
		}
		parked := rc
		f.st.PendingRC[flat] = &parked
		return Applied{}
	}
	ss := f.st.Statements[seq]
	if rc.SourceNode != ss.SourceNode {
		return Rejected{Reason: "rc source_node does not match the deterministic source selection (§5.4)"}
	}
	if ss.RC != nil {
		if reflect.DeepEqual(*ss.RC, rc) {
			return Applied{} // idempotent re-registration
		}
		return Rejected{Reason: "conflicting RC already bound for this statement (first wins)"}
	}
	bound := rc
	f.bindRC(ss, &bound)
	return Applied{}
}
```

`apply.go` gains the `"reflect"` import.

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -8`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): RegisterRC with two-direction late binding and first-wins dedup

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 10: evidence recording — `RecordAttestation` + `RecordByteSideScan`

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/apply.go` (replace the two stubs; add `verifyNodeSig`)
- Create: `fsm/evidence_test.go`

**Interfaces:**
- Consumes: `replay.ExecutionReceipt.Hash()`, `replay.CanonicalDigest` + `arbiter.DomainByteSideScan` + `ByteSideScanMsg.Body()`, `ed25519.Verify`, `BlockVerification` from Task 8.
- Produces: real `applyRecordAttestation` / `applyRecordByteSideScan`; `(*FSM).verifyNodeSig(nodeID, msg, sigHex string) error`; test helpers `ed25519KeyFor(seed byte)`, `signedAttestation(...)`, `signedScan(...)` reused by Tasks 11 and 14.

- [ ] **Step 1: Write the failing tests**

Create `fsm/evidence_test.go`:

```go
package fsm

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// ed25519KeyFor derives the deterministic test key matching testPubkey(seed)
// (fsm_test.go registers nodes with these pubkeys).
func ed25519KeyFor(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

// signedAttestation builds a fully-signed whole-block attestation. The
// signature convention matches payloadexec.Ed25519Signer: hex, no 0x
// prefix, over the receipt-hash string bytes.
func signedAttestation(t *testing.T, replicaID string, seed byte, receipt replay.ExecutionReceipt) replay.ReplayAttestation {
	t.Helper()
	h, err := receipt.Hash()
	if err != nil {
		t.Fatalf("receipt hash: %v", err)
	}
	sig := ed25519.Sign(ed25519KeyFor(seed), []byte(h))
	return replay.ReplayAttestation{ReplicaID: replicaID, Receipt: receipt, ReceiptHash: h, Signature: hex.EncodeToString(sig)}
}

// signedScan builds a fully-signed byte-side scan.
func signedScan(t *testing.T, replicaID string, seed byte, blockSeq uint64, parts []arbiter.PartScan) arbiter.ByteSideScanMsg {
	t.Helper()
	m := arbiter.ByteSideScanMsg{ReplicaID: replicaID, BlockSeq: blockSeq, Parts: parts}
	h, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, m.Body())
	if err != nil {
		t.Fatalf("scan hash: %v", err)
	}
	m.ScanHash = h
	m.Signature = hex.EncodeToString(ed25519.Sign(ed25519KeyFor(seed), []byte(h)))
	return m
}

// markedBlock builds a sealed, RC-bound, replaying block with sources s1
// and verifiers v1..v3 (seeds = last byte of the id) and returns the
// verifier set.
func markedBlock(t *testing.T, f *FSM) []string {
	t.Helper()
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	for _, id := range []string{"v1", "v2", "v3"} {
		registerActive(t, f, id, arbiter.NodeRoleVerifier)
	}
	sealBlock(t, f, 1)
	rc := rcFor(f, 1, "0xr00t", "0xffee")
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	mustApply(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	return f.st.Verifications[1].VerifierSet
}

func receiptForBlock(f *FSM, computedRoot string) replay.ExecutionReceipt {
	h := f.st.Blocks[0]
	return replay.ExecutionReceipt{
		BlockSeq: 1, PrevSafeSnapshotID: h.PrevSafeSnapshotID, PrevStateRoot: h.PrevStateRoot,
		SchemaSnapshotID: h.SchemaSnapshotID, ExecutorProfileID: h.ExecutorProfileID,
		SourceClaimRoot: "0xr00t", ComputedStateRoot: computedRoot,
	}
}

func TestRecordAttestation_HappyAndGates(t *testing.T) {
	f := newTestFSM(t)
	set := markedBlock(t, f)
	rid := set[0]
	seed := rid[len(rid)-1]

	att := signedAttestation(t, rid, seed, receiptForBlock(f, "0xr00t"))
	mustApply(t, f, wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: att}})
	if f.st.Verifications[1].Attestations[rid] == nil {
		t.Fatal("attestation must be recorded")
	}
	// duplicate: first wins, idempotent Applied
	mustApply(t, f, wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: att}})

	// non-member replica rejected (s1 is not in the verifier set)
	bad := signedAttestation(t, "s1", '1', receiptForBlock(f, "0xr00t"))
	applyExpectReject(t, f, wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: bad}})

	// tampered receipt: recorded hash no longer matches
	tampered := att
	tampered.Receipt.ComputedStateRoot = "0xevil"
	applyExpectReject(t, f, wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: tampered}})

	// wrong key: signature verification fails
	rid2 := set[1]
	wrongKey := signedAttestation(t, rid2, 0x7f, receiptForBlock(f, "0xr00t"))
	applyExpectReject(t, f, wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: wrongKey}})

	// unknown block
	unknown := signedAttestation(t, rid, seed, replay.ExecutionReceipt{BlockSeq: 42})
	applyExpectReject(t, f, wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: unknown}})
}

func TestRecordByteSideScan_HappyAndGates(t *testing.T) {
	f := newTestFSM(t)
	set := markedBlock(t, f)
	rid := set[0]
	seed := rid[len(rid)-1]
	parts := []arbiter.PartScan{{TableID: "db.t", PartitionID: "p0", ClaimedPartRowLtHash: "0xffee", ScannedPartRowLtHash: "0xffee"}}

	scan := signedScan(t, rid, seed, 1, parts)
	mustApply(t, f, wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: scan}})
	if f.st.Verifications[1].ByteScans[rid] == nil {
		t.Fatal("scan must be recorded")
	}
	// duplicate absorbed
	mustApply(t, f, wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: scan}})

	// tampered parts: scan_hash mismatch
	tampered := scan
	tampered.Parts = []arbiter.PartScan{{TableID: "db.t", PartitionID: "p0", ClaimedPartRowLtHash: "0xffee", ScannedPartRowLtHash: "0xevil"}}
	applyExpectReject(t, f, wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: tampered}})

	// non-member rejected
	foreign := signedScan(t, "s1", '1', 1, parts)
	applyExpectReject(t, f, wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: foreign}})

	// block not marked: seal a second block without MarkReplaying
	sealBlock(t, f, 1)
	early := signedScan(t, rid, seed, 2, parts)
	applyExpectReject(t, f, wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: early}})
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run 'TestRecordAttestation|TestRecordByteSideScan' 2>&1 | tail -5`
Expected: FAIL on the stubs.

- [ ] **Step 3: Replace the two stubs in `fsm/apply.go`**

```go
// verifyNodeSig checks a node's ed25519 signature over a hash string
// (evidence convention: hex signature, message = hash string bytes as-is).
func (f *FSM) verifyNodeSig(nodeID, msg, sigHex string) error {
	n, ok := f.st.Nodes[nodeID]
	if !ok {
		return fmt.Errorf("unknown node %q", nodeID)
	}
	sig, err := hex.DecodeString(strings.TrimPrefix(sigHex, "0x"))
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("node %q: malformed signature", nodeID)
	}
	pub := ed25519.PublicKey(n.Registration.Ed25519Pubkey)
	if len(pub) != ed25519.PublicKeySize || !ed25519.Verify(pub, []byte(msg), sig) {
		return fmt.Errorf("node %q: ed25519 verification failed", nodeID)
	}
	return nil
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if v == want {
			return true
		}
	}
	return false
}

func (f *FSM) applyRecordAttestation(c *wire.RecordAttestation) any {
	att := c.Attestation
	bv, ok := f.st.Verifications[att.Receipt.BlockSeq]
	if !ok {
		return Rejected{Reason: "unknown block"}
	}
	if len(bv.VerifierSet) == 0 {
		return Rejected{Reason: "block not marked replaying"}
	}
	if !contains(bv.VerifierSet, att.ReplicaID) {
		return Rejected{Reason: "replica not in the block's verifier set"}
	}
	if _, dup := bv.Attestations[att.ReplicaID]; dup {
		return Applied{} // first wins, idempotent (§10.3)
	}
	h, err := att.Receipt.Hash()
	if err != nil || h != att.ReceiptHash {
		return Rejected{Reason: "receipt_hash does not match the receipt (recomputed verbatim, §4.3)"}
	}
	if err := f.verifyNodeSig(att.ReplicaID, att.ReceiptHash, att.Signature); err != nil {
		return Rejected{Reason: err.Error()}
	}
	rec := att
	bv.Attestations[att.ReplicaID] = &rec
	f.reevaluateBlock(att.Receipt.BlockSeq)
	return Applied{}
}

func (f *FSM) applyRecordByteSideScan(c *wire.RecordByteSideScan) any {
	scan := c.Scan
	bv, ok := f.st.Verifications[scan.BlockSeq]
	if !ok {
		return Rejected{Reason: "unknown block"}
	}
	if len(bv.VerifierSet) == 0 {
		return Rejected{Reason: "block not marked replaying"}
	}
	if !contains(bv.VerifierSet, scan.ReplicaID) {
		return Rejected{Reason: "replica not in the block's verifier set"}
	}
	if _, dup := bv.ByteScans[scan.ReplicaID]; dup {
		return Applied{}
	}
	want, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, scan.Body())
	if err != nil || want != scan.ScanHash {
		return Rejected{Reason: "scan_hash does not match the scan body"}
	}
	if err := f.verifyNodeSig(scan.ReplicaID, scan.ScanHash, scan.Signature); err != nil {
		return Rejected{Reason: err.Error()}
	}
	rec := scan
	bv.ByteScans[scan.ReplicaID] = &rec
	f.reevaluateBlock(scan.BlockSeq)
	return Applied{}
}
```

`apply.go` gains imports `"encoding/hex"` (already there from Task 8), `"fmt"`, `"strings"`.

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -8`
Expected: ALL PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): attested evidence recording with hash recompute + ed25519 gates

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 11: the three-way predicate (`fsm/threeway.go`)

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Create: `fsm/threeway.go`, `fsm/threeway_test.go`
- Modify: `fsm/admission.go` (delete the no-op `reevaluateBlock` placeholder)

**Interfaces:**
- Consumes: `BlockVerification`, `blockStatements` (Task 8), `pkg/lthash` (`New`, `FromBytes`, `AddHash`, `Equal`, `Bytes`), Tasks 9–10 helpers.
- Produces: real `(*FSM).reevaluateBlock(blockSeq uint64)` (evaluability → per-replica conjunction → quorum flip, design §8); `lthashFromHex(s string) (*lthash.Hash, error)` (Task 13 reuses it); tests produce `lthashHex(elements ...string) string` fixture helper (Task 13/14 reuse).

- [ ] **Step 1: Write the failing fixture tests**

Create `fsm/threeway_test.go`:

```go
package fsm

import (
	"encoding/hex"
	"testing"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// lthashHex builds a real LtHash accumulator over the given elements and
// renders the raw-2048-byte "0x" hex form used by every part/partition
// scalar in the protocol.
func lthashHex(elements ...string) string {
	h := lthash.New()
	for _, e := range elements {
		h.Add([]byte(e))
	}
	return "0x" + hex.EncodeToString(h.Bytes())
}

// evidenceBlock: one statement, RC claiming part A (lthash over "rowA")
// with a matching partition sum, sealed + marked. Returns the verifier set
// and the partition commitment a correct replay must report (base is the
// all-zero accumulator, so post == the part hash itself).
func evidenceBlock(t *testing.T, f *FSM) (set []string, partHash string) {
	t.Helper()
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	for _, id := range []string{"v1", "v2", "v3"} {
		registerActive(t, f, id, arbiter.NodeRoleVerifier)
	}
	sealBlock(t, f, 1)
	partHash = lthashHex("rowA")
	rc := rcFor(f, 1, "0xr00t", partHash)
	rc.PartitionNewPartSums = []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p0", NewPartsLtHashSum: partHash}}
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	mustApply(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	return f.st.Verifications[1].VerifierSet, partHash
}

func goodReceipt(f *FSM, partHash string) replay.ExecutionReceipt {
	r := receiptForBlock(f, "0xr00t")
	r.PartitionCommitmentsAfter = []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: partHash}}
	return r
}

func goodScan(partHash string) []arbiter.PartScan {
	return []arbiter.PartScan{{TableID: "db.t", PartitionID: "p0", ClaimedPartRowLtHash: partHash, ScannedPartRowLtHash: partHash}}
}

func attest(t *testing.T, f *FSM, rid string, receipt replay.ExecutionReceipt) {
	t.Helper()
	mustApply(t, f, wire.Command{RecordAttestation: &wire.RecordAttestation{
		Attestation: signedAttestation(t, rid, rid[len(rid)-1], receipt)}})
}

func scanIn(t *testing.T, f *FSM, rid string, parts []arbiter.PartScan) {
	t.Helper()
	mustApply(t, f, wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{
		Scan: signedScan(t, rid, rid[len(rid)-1], 1, parts)}})
}

func TestThreeWay_HonestQuorumFlipsBlock(t *testing.T) {
	f := newTestFSM(t)
	set, partHash := evidenceBlock(t, f)

	// first replica: evidence complete but 1-of-3 — no quorum yet
	attest(t, f, set[0], goodReceipt(f, partHash))
	scanIn(t, f, set[0], goodScan(partHash))
	if v := f.st.Verifications[1].Verdict; v == nil || v.Quorum {
		t.Fatalf("1-of-3 must evaluate without quorum: %+v", v)
	}
	if f.st.Statements[1].Status != StatusReplaying {
		t.Fatal("no premature flip")
	}

	// second replica completes the 2-of-3 quorum
	attest(t, f, set[1], goodReceipt(f, partHash))
	scanIn(t, f, set[1], goodScan(partHash))
	v := f.st.Verifications[1].Verdict
	if v == nil || !v.Quorum {
		t.Fatalf("2-of-3 must reach quorum: %+v", v)
	}
	rv := v.Replicas[set[0]]
	if !rv.Check1RootMatch || !rv.Check2PartitionMatch || !rv.Check3ByteSideMatch || !rv.Pass {
		t.Fatalf("per-replica verdict: %+v", rv)
	}
	if f.st.Statements[1].Status != StatusQuorumVerified {
		t.Fatal("quorum must flip the block's statements to QuorumVerified")
	}
}

func TestThreeWay_EachCheckFailsIndependently(t *testing.T) {
	// check 1: computed root != source claim
	f := newTestFSM(t)
	set, partHash := evidenceBlock(t, f)
	bad1 := goodReceipt(f, partHash)
	bad1.ComputedStateRoot = "0xdifferent"
	attest(t, f, set[0], bad1)
	scanIn(t, f, set[0], goodScan(partHash))
	if rv := f.st.Verifications[1].Verdict.Replicas[set[0]]; rv.Check1RootMatch || rv.Pass {
		t.Fatalf("check1 must fail: %+v", rv)
	}

	// check 2: partition commitment does not equal base + claimed sum
	f2 := newTestFSM(t)
	set2, partHash2 := evidenceBlock(t, f2)
	bad2 := goodReceipt(f2, partHash2)
	bad2.PartitionCommitmentsAfter = []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: lthashHex("rowEvil")}}
	attest(t, f2, set2[0], bad2)
	scanIn(t, f2, set2[0], goodScan(partHash2))
	if rv := f2.st.Verifications[1].Verdict.Replicas[set2[0]]; rv.Check2PartitionMatch || rv.Pass {
		t.Fatalf("check2 must fail: %+v", rv)
	}

	// check 3: scanned bytes disagree with the claimed commitment
	f3 := newTestFSM(t)
	set3, partHash3 := evidenceBlock(t, f3)
	attest(t, f3, set3[0], goodReceipt(f3, partHash3))
	scanIn(t, f3, set3[0], []arbiter.PartScan{{TableID: "db.t", PartitionID: "p0",
		ClaimedPartRowLtHash: partHash3, ScannedPartRowLtHash: lthashHex("rowEvil")}})
	if rv := f3.st.Verifications[1].Verdict.Replicas[set3[0]]; rv.Check3ByteSideMatch || rv.Pass {
		t.Fatalf("check3 must fail: %+v", rv)
	}

	// missing scan coverage of a claimed part also fails check 3
	f4 := newTestFSM(t)
	set4, partHash4 := evidenceBlock(t, f4)
	attest(t, f4, set4[0], goodReceipt(f4, partHash4))
	scanIn(t, f4, set4[0], []arbiter.PartScan{{TableID: "db.t", PartitionID: "p0",
		ClaimedPartRowLtHash: "0xother", ScannedPartRowLtHash: "0xother"}})
	if rv := f4.st.Verifications[1].Verdict.Replicas[set4[0]]; rv.Check3ByteSideMatch {
		t.Fatalf("uncovered claimed part must fail check3: %+v", rv)
	}
}

func TestThreeWay_TwoFailuresBlockQuorum(t *testing.T) {
	f := newTestFSM(t)
	set, partHash := evidenceBlock(t, f)
	bad := goodReceipt(f, partHash)
	bad.ComputedStateRoot = "0xdifferent"
	for _, rid := range set[:2] {
		attest(t, f, rid, bad)
		scanIn(t, f, rid, goodScan(partHash))
	}
	attest(t, f, set[2], goodReceipt(f, partHash))
	scanIn(t, f, set[2], goodScan(partHash))
	v := f.st.Verifications[1].Verdict
	if v.Quorum {
		t.Fatalf("1 honest of 3 must not reach quorum: %+v", v)
	}
	if f.st.Statements[1].Status == StatusQuorumVerified {
		t.Fatal("no flip without quorum")
	}
}

func TestThreeWay_EvaluabilityRequiresAllRCs(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	for _, id := range []string{"v1", "v2", "v3"} {
		registerActive(t, f, id, arbiter.NodeRoleVerifier)
	}
	sealBlock(t, f, 2) // two statements, NO RC bound
	mustApply(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	set := f.st.Verifications[1].VerifierSet
	for _, rid := range set[:2] {
		attest(t, f, rid, receiptForBlock(f, "0xr00t"))
		scanIn(t, f, rid, nil)
	}
	if f.st.Verifications[1].Verdict != nil {
		t.Fatal("evaluation must wait until every statement in the block has a bound RC")
	}
}

func TestThreeWay_MalformedClaimHexFailsCheck2(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	for _, id := range []string{"v1", "v2", "v3"} {
		registerActive(t, f, id, arbiter.NodeRoleVerifier)
	}
	sealBlock(t, f, 1)
	rc := rcFor(f, 1, "0xr00t", "0xffee")
	rc.PartitionNewPartSums = []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p0", NewPartsLtHashSum: "0xnothex"}}
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	mustApply(t, f, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	set := f.st.Verifications[1].VerifierSet
	attest(t, f, set[0], receiptForBlock(f, "0xr00t"))
	scanIn(t, f, set[0], []arbiter.PartScan{{TableID: "db.t", PartitionID: "p0", ClaimedPartRowLtHash: "0xffee", ScannedPartRowLtHash: "0xffee"}})
	rv := f.st.Verifications[1].Verdict.Replicas[set[0]]
	if rv.Check2PartitionMatch {
		t.Fatal("malformed claimed-sum hex must fail check 2 deterministically")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run TestThreeWay 2>&1 | tail -5`
Expected: FAIL — `reevaluateBlock` is still the Task 7 no-op, so `Verdict` stays nil.

- [ ] **Step 3: Implement `fsm/threeway.go`** (delete the placeholder `reevaluateBlock` from `admission.go`)

```go
package fsm

import (
	"encoding/hex"
	"strings"

	"housegate/housegate/pkg/lthash"

	"github.com/sentioxyz/arbiter"
)

// lthashFromHex decodes a "0x"-prefixed raw-2048-byte LtHash accumulator.
func lthashFromHex(s string) (*lthash.Hash, error) {
	b, err := hex.DecodeString(strings.TrimPrefix(s, "0x"))
	if err != nil {
		return nil, err
	}
	return lthash.FromBytes(b)
}

// reevaluateBlock recomputes the three-way verdict (design §8) over logged
// signed evidence. Deterministic: called at the end of every
// evidence-bearing Apply; early or late evaluation yields the same result.
// Quorum, once reached, is final (evidence-append monotone).
func (f *FSM) reevaluateBlock(blockSeq uint64) {
	if blockSeq == 0 {
		return
	}
	bv, ok := f.st.Verifications[blockSeq]
	if !ok || len(bv.VerifierSet) == 0 {
		return
	}
	if bv.Verdict != nil && bv.Verdict.Quorum {
		return
	}
	stmts := f.blockStatements(blockSeq)
	if len(stmts) == 0 {
		return
	}
	for _, ss := range stmts {
		if ss.RC == nil {
			return // evaluability: every statement needs its bound RC (checks 2/3 need all part claims)
		}
	}
	lastRC := stmts[len(stmts)-1].RC

	// Claimed per-partition sums across the block's RCs. A malformed
	// claimed scalar deterministically fails check 2 for every replica.
	claimed := map[arbiter.TablePartition]*lthash.Hash{}
	claimedBad := false
	for _, ss := range stmts {
		for _, ps := range ss.RC.PartitionNewPartSums {
			h, err := lthashFromHex(ps.NewPartsLtHashSum)
			if err != nil {
				claimedBad = true
				continue
			}
			tp := arbiter.TablePartition{TableID: ps.TableID, PartitionID: ps.PartitionID}
			if acc, ok := claimed[tp]; ok {
				acc.AddHash(h)
			} else {
				claimed[tp] = h
			}
		}
	}

	verdict := &ThreeWayVerdict{Replicas: map[string]ReplicaVerdict{}}
	pass := 0
	for _, rid := range bv.VerifierSet {
		att := bv.Attestations[rid]
		scan := bv.ByteScans[rid]
		if att == nil || scan == nil {
			continue // this replica's evidence bundle is incomplete
		}
		rv := ReplicaVerdict{
			// check 1: FSM-recomputed root equality; the advisory
			// MatchSourceRoot flag is deliberately unread (§13).
			Check1RootMatch:      lastRC.SourceClaimRoot != "" && att.Receipt.ComputedStateRoot == lastRC.SourceClaimRoot,
			Check2PartitionMatch: !claimedBad && f.check2(claimed, att),
			Check3ByteSideMatch:  check3(stmts, scan),
		}
		rv.Pass = rv.Check1RootMatch && rv.Check2PartitionMatch && rv.Check3ByteSideMatch
		verdict.Replicas[rid] = rv
		if rv.Pass {
			pass++
		}
	}
	verdict.Quorum = pass >= VerifierQuorum
	bv.Verdict = verdict
	if verdict.Quorum {
		for _, ss := range stmts {
			switch ss.Status {
			case StatusSequenced, StatusUnsafeRegistered, StatusReplaying:
				ss.Status = StatusQuorumVerified
			}
		}
	}
}

// check2: for every touched partition, base ⊕ Σ(claimed) must equal the
// replica's absolute post-commitment (§7.3 check 2, additive form). Map
// iteration order is irrelevant: the fold is a pure conjunction.
func (f *FSM) check2(claimed map[arbiter.TablePartition]*lthash.Hash, att *replay.ReplayAttestation) bool {
	if len(claimed) == 0 {
		return false // an INSERT block with no partition claims can never verify
	}
	after := map[arbiter.TablePartition]string{}
	for _, pc := range att.Receipt.PartitionCommitmentsAfter {
		after[arbiter.TablePartition{TableID: pc.TableID, PartitionID: pc.PartitionID}] = pc.Root
	}
	for tp, sum := range claimed {
		expect := lthash.New()
		if ps, ok := f.st.Partitions[tp]; ok && ps.BasePartitionRoot != "" {
			base, err := lthashFromHex(ps.BasePartitionRoot)
			if err != nil {
				return false
			}
			expect = base
		}
		expect.AddHash(sum)
		gotHex, ok := after[tp]
		if !ok {
			return false
		}
		got, err := lthashFromHex(gotHex)
		if err != nil {
			return false
		}
		if !expect.Equal(got) {
			return false
		}
	}
	return true
}

// check3: every candidate part across the block's RCs must appear in the
// replica's scan with scanned == claimed (§7.3 check 3).
func check3(stmts []*StatementState, scan *arbiter.ByteSideScanMsg) bool {
	scanned := map[string]string{}
	for _, p := range scan.Parts {
		scanned[p.ClaimedPartRowLtHash] = p.ScannedPartRowLtHash
	}
	for _, ss := range stmts {
		for _, cp := range ss.RC.CandidateParts {
			got, ok := scanned[cp.PartRowLtHash]
			if !ok || !strings.EqualFold(got, cp.PartRowLtHash) {
				return false
			}
		}
	}
	return true
}
```

`threeway.go` needs the `replay` import (`"housegate/housegate/pkg/replay"`) for the `att` parameter type.

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -12`
Expected: ALL PASS (honest quorum, three independent failure axes, missing coverage, 1-honest-of-3, evaluability, malformed hex).

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): in-Apply three-way predicate with per-replica conjunction quorum

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 12: authority hash export + anchor / promotion issuance / challenge

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Modify: `authority/payload.go` (export the two hash funcs), `authority/signer.go` + `authority/validator.go` (call sites), `fsm/apply.go` (replace four stubs)
- Create: `fsm/authorityjws.go`, `fsm/promotion_test.go` (first half)

**Interfaces:**
- Consumes: `authority.PromotionPurpose`, `authority.JWSCommandPayload`, `authority.Signer` (tests sign real tokens), Task 11 verdicts.
- Produces: `authority.PromoteCommandHash(arbiter.PromoteSafePartition) (string, error)` + `authority.CleanupCommandHash(arbiter.UnsafeCleanup) (string, error)` (renamed exports; internal callers updated); `verifyAuthorityJWS(token, wantCmdHash string, allowlist []string) error` (wall-clock-free); real `applyRecordAnchorFinality`, `applyRecordPromotionIssued`, `applyOpenChallenge`, `applyResolveChallenge`.

- [ ] **Step 1: Export the authority hash functions**

In `authority/payload.go` rename `promoteCommandHash` → `PromoteCommandHash` and `cleanupCommandHash` → `CleanupCommandHash` (keep bodies; update doc comments to note they are the canonical cmd-hash entry points consumed by the FSM's audit verification). Update the call sites in `authority/signer.go` and `authority/validator.go` (5 references). Run `go test ./authority/` — must stay green.

- [ ] **Step 2: Write the failing tests**

Create `fsm/promotion_test.go`:

```go
package fsm

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/authority"
	"github.com/sentioxyz/arbiter/wire"
)

// authorityFixture returns a signer plus Params carrying its address.
func authorityFixture(t *testing.T) (*authority.Signer, Params) {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	s, err := authority.NewSignerFromHex(strings.TrimPrefix(crypto.EncodeToHex(crypto.FromECDSA(key)), "0x"))
	if err != nil {
		t.Fatalf("signer: %v", err)
	}
	p := testParams()
	p.AuthorityAddresses = []string{s.Address()}
	return s, p
}

// quorumVerifiedBlock drives a block to QuorumVerified and returns the FSM
// and the verified part hash.
func quorumVerifiedBlock(t *testing.T, params Params) (*FSM, string) {
	t.Helper()
	f := New(params)
	set, partHash := evidenceBlock(t, f)
	for _, rid := range set[:2] {
		attest(t, f, rid, goodReceipt(f, partHash))
		scanIn(t, f, rid, goodScan(partHash))
	}
	if !f.st.Verifications[1].Verdict.Quorum {
		t.Fatal("fixture must reach quorum")
	}
	return f, partHash
}

func TestAnchorFinality_GateAndBackfill(t *testing.T) {
	signer, params := authorityFixture(t)
	_ = signer
	f, _ := quorumVerifiedBlock(t, params)

	// anchor without finality → FinalityWait
	anchor := arbiter.AnchorRef{L3BlockHash: "0xaa", StateRoot: "0xbb", L2TxRef: "tx1"}
	mustApply(t, f, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: 1, Anchor: anchor}})
	if f.st.Statements[1].Status != StatusFinalityWait {
		t.Fatalf("anchor without finality: %v", f.st.Statements[1].Status)
	}
	if f.st.Blocks[0].L2AnchorRef == nil || f.st.Blocks[0].L2AnchorRef.L2TxRef != "tx1" {
		t.Fatal("anchor must back-fill the sealed header")
	}
	// finality + last_mergeable → Promotable
	mustApply(t, f, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: 1, Anchor: anchor, FinalityReached: true, LastMergeableReached: true}})
	if f.st.Statements[1].Status != StatusPromotable {
		t.Fatalf("finality: %v", f.st.Statements[1].Status)
	}

	// gate: a block that is not QuorumVerified rejects the anchor
	g := New(params)
	registerActive(t, g, "s1", arbiter.NodeRoleSNode)
	sealBlock(t, g, 1)
	applyExpectReject(t, g, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: 1, Anchor: anchor}})
}

func TestPromotionIssued_AuditVerification(t *testing.T) {
	signer, params := authorityFixture(t)
	f, partHash := quorumVerifiedBlock(t, params)

	promote := arbiter.PromoteSafePartition{
		TableID: "db.t", PartitionID: "p0", PromotionSeq: 1,
		BaseSafeSnapshotID: "", BasePartitionRoot: "",
		CandidateParts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}},
	}
	token, err := signer.SignPromotion(promote)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	mustApply(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote, AuthorityJWS: token}})
	if f.st.PromotionSeq != 1 {
		t.Fatal("PromotionSeq must advance")
	}
	pp := f.st.PendingPromotions[1]
	if pp == nil || len(pp.StatementSeqs) != 1 || pp.StatementSeqs[0] != 1 {
		t.Fatalf("pending promotion must resolve covered statements: %+v", pp)
	}

	// non-monotonic seq
	promote2 := promote
	promote2.PromotionSeq = 5
	token2, _ := signer.SignPromotion(promote2)
	applyExpectReject(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote2, AuthorityJWS: token2}})

	// tampered command: token binds different content
	promote3 := promote
	promote3.PromotionSeq = 2
	tokenForOther, _ := signer.SignPromotion(promote2)
	applyExpectReject(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote3, AuthorityJWS: tokenForOther}})

	// unlisted authority
	rogueKey, _ := crypto.GenerateKey()
	rogue, _ := authority.NewSignerFromHex(strings.TrimPrefix(crypto.EncodeToHex(crypto.FromECDSA(rogueKey)), "0x"))
	promote4 := promote
	promote4.PromotionSeq = 2
	rogueToken, _ := rogue.SignPromotion(promote4)
	applyExpectReject(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote4, AuthorityJWS: rogueToken}})

	// empty allowlist fails closed
	g, _ := quorumVerifiedBlock(t, testParams()) // no AuthorityAddresses
	pg := promote
	tokenG, _ := signer.SignPromotion(pg)
	applyExpectReject(t, g, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: pg, AuthorityJWS: tokenG}})
}

func TestChallenge_Transitions(t *testing.T) {
	_, params := authorityFixture(t)
	f, _ := quorumVerifiedBlock(t, params)
	mustApply(t, f, wire.Command{OpenChallenge: &wire.OpenChallenge{BlockSeq: 1, Reason: "verifier mismatch", OpenedBy: "v9"}})
	if f.st.Statements[1].Status != StatusChallengeReplay {
		t.Fatal("OpenChallenge must move statements to ChallengeReplay")
	}
	mustApply(t, f, wire.Command{ResolveChallenge: &wire.ResolveChallenge{BlockSeq: 1, Verdict: wire.ChallengeVerdictSafe}})
	if f.st.Statements[1].Status != StatusQuorumVerified {
		t.Fatal("SAFE verdict re-enters the pipeline at QuorumVerified")
	}
	mustApply(t, f, wire.Command{OpenChallenge: &wire.OpenChallenge{BlockSeq: 1, Reason: "again", OpenedBy: "v9"}})
	mustApply(t, f, wire.Command{ResolveChallenge: &wire.ResolveChallenge{BlockSeq: 1, Verdict: wire.ChallengeVerdictRejected}})
	if f.st.Statements[1].Status != StatusRejected {
		t.Fatal("REJECTED verdict drops the block's statements")
	}
	applyExpectReject(t, f, wire.Command{ResolveChallenge: &wire.ResolveChallenge{BlockSeq: 1, Verdict: wire.ChallengeVerdictUnspecified}})
	applyExpectReject(t, f, wire.Command{OpenChallenge: &wire.OpenChallenge{BlockSeq: 42}})
}
```

Note: `authority.Signer`'s constructor is `NewSignerFromHex(hex string)`; `crypto.EncodeToHex(crypto.FromECDSA(key))` renders `0x`-prefixed private-key hex, hence the `TrimPrefix`. If `SignPromotion`'s iat handling differs (it takes no iat argument per P0a), the call is just `signer.SignPromotion(cmd)` as shown — check `authority/signer.go` if the compiler disagrees and follow the existing signature.

- [ ] **Step 3: Run to verify failure**

Run: `go test ./fsm/ -run 'TestAnchorFinality|TestPromotionIssued|TestChallenge' 2>&1 | tail -5`
Expected: FAIL on the stubs.

- [ ] **Step 4: Implement `fsm/authorityjws.go`**

```go
package fsm

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter/authority"
)

// verifyAuthorityJWS is the FSM's wall-clock-free audit verification of an
// authority command token (design §9): purpose + cmd-hash binding +
// secp256k1 recovery against Params.AuthorityAddresses. It deliberately
// checks NO iat/expiry — Apply records history deterministically (§4.3 red
// line 1); token freshness is enforced by the SNode-side
// authority.Validator (now MaxTokenAge fail-closed, Task 2).
func verifyAuthorityJWS(token, wantCmdHash string, allowlist []string) error {
	if len(allowlist) == 0 {
		return fmt.Errorf("authority allowlist is empty: refusing to record any authority command")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return fmt.Errorf("authority token: want compact JWS with 3 parts, got %d", len(parts))
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return fmt.Errorf("authority token header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return fmt.Errorf("authority token header: %w", err)
	}
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return fmt.Errorf("authority token: unexpected alg %q", header.Alg)
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return fmt.Errorf("authority token payload: %w", err)
	}
	var payload authority.JWSCommandPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return fmt.Errorf("authority token payload: %w", err)
	}
	if payload.Purpose != authority.PromotionPurpose {
		return fmt.Errorf("authority token: unexpected purpose %q", payload.Purpose)
	}
	if !strings.EqualFold(payload.CmdHash, wantCmdHash) {
		return fmt.Errorf("authority token: command hash mismatch")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("authority token signature: %w", err)
	}
	if len(sig) != 65 {
		return fmt.Errorf("authority token signature: want 65 bytes, got %d", len(sig))
	}
	recSig := make([]byte, 65)
	copy(recSig, sig)
	if recSig[64] >= 27 {
		recSig[64] -= 27
	}
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(parts[0]+"."+parts[1])), recSig)
	if err != nil {
		return fmt.Errorf("recover authority address: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex())
	for _, allowed := range allowlist {
		if strings.EqualFold(allowed, addr) {
			return nil
		}
	}
	return fmt.Errorf("authority token: recovered %s is not in the authority allowlist", addr)
}
```

- [ ] **Step 5: Replace the four stubs in `fsm/apply.go`**

```go
func (f *FSM) applyRecordAnchorFinality(c *wire.RecordAnchorFinality) any {
	bv, ok := f.st.Verifications[c.L3BlockSeq]
	if !ok {
		return Rejected{Reason: "unknown block"}
	}
	if bv.Verdict == nil || !bv.Verdict.Quorum {
		return Rejected{Reason: "block is not quorum-verified"}
	}
	idx := int(c.L3BlockSeq) - 1
	if idx < 0 || idx >= len(f.st.Blocks) {
		return Rejected{Reason: "block header out of range"}
	}
	a := c.Anchor
	bv.Anchor = &a
	bv.Finality = bv.Finality || c.FinalityReached
	bv.LastMergeable = bv.LastMergeable || c.LastMergeableReached
	f.st.Blocks[idx].L2AnchorRef = &a // back-fill; ChainHash excludes it (§5.2)
	target := StatusFinalityWait
	if bv.Finality && bv.LastMergeable {
		target = StatusPromotable
	}
	f.forEachBlockStatement(c.L3BlockSeq, func(ss *StatementState) {
		if ss.Status == StatusQuorumVerified || ss.Status == StatusFinalityWait {
			ss.Status = target
		}
	})
	return Applied{}
}

func (f *FSM) applyRecordPromotionIssued(c *wire.RecordPromotionIssued) any {
	if c.Promote.PromotionSeq != f.st.PromotionSeq+1 {
		return Rejected{Reason: "promotion_seq must be PromotionSeq+1 (frozen monotonic rule)"}
	}
	wantHash, err := authority.PromoteCommandHash(c.Promote)
	if err != nil {
		return Rejected{Reason: "hash promote command: " + err.Error()}
	}
	if err := verifyAuthorityJWS(c.AuthorityJWS, wantHash, f.st.Params.AuthorityAddresses); err != nil {
		return Rejected{Reason: err.Error()}
	}
	// Resolve covered statements by part content commitment (design §9):
	// map iteration is safe — the result set is sorted before storing.
	covered := map[uint64]bool{}
	for _, pr := range c.Promote.CandidateParts {
		for seq, ss := range f.st.Statements {
			if ss.UnpromotedParts[pr.PartRowLtHash] {
				covered[seq] = true
			}
		}
	}
	seqs := make([]uint64, 0, len(covered))
	for seq := range covered {
		seqs = append(seqs, seq)
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	f.st.PromotionSeq = c.Promote.PromotionSeq
	f.st.PendingPromotions[c.Promote.PromotionSeq] = &PendingPromotion{Promote: c.Promote, StatementSeqs: seqs}
	return Applied{}
}

func (f *FSM) applyOpenChallenge(c *wire.OpenChallenge) any {
	if _, ok := f.st.Verifications[c.BlockSeq]; !ok {
		return Rejected{Reason: "unknown block"}
	}
	f.forEachBlockStatement(c.BlockSeq, func(ss *StatementState) {
		if ss.Status != StatusRejected && ss.Status != StatusSafe {
			ss.Status = StatusChallengeReplay
		}
	})
	return Applied{}
}

func (f *FSM) applyResolveChallenge(c *wire.ResolveChallenge) any {
	if _, ok := f.st.Verifications[c.BlockSeq]; !ok {
		return Rejected{Reason: "unknown block"}
	}
	var target Status
	switch c.Verdict {
	case wire.ChallengeVerdictSafe:
		// The claim stands: re-enter the normal pipeline at
		// QuorumVerified (promotion still needs finality + ack; "Safe"
		// in §7.5 names the claim's fate, not StatusSafe directly).
		target = StatusQuorumVerified
	case wire.ChallengeVerdictRejected:
		target = StatusRejected
	default:
		return Rejected{Reason: "unspecified challenge verdict"}
	}
	f.forEachBlockStatement(c.BlockSeq, func(ss *StatementState) {
		if ss.Status == StatusChallengeReplay {
			ss.Status = target
		}
	})
	return Applied{}
}
```

`apply.go` gains the `"github.com/sentioxyz/arbiter/authority"` import (`"sort"` is already there from Task 8).

- [ ] **Step 6: Run the tests**

Run: `go test ./... 2>&1 | tail -8`
Expected: ALL PASS across the repo (authority rename included).

- [ ] **Step 7: Commit**

```bash
git add authority/ fsm/
git commit -m "feat(fsm): anchor finality, audited promotion issuance, challenge transitions

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 13: promotion ack closure + safe snapshot + cleanup registry

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/apply.go` (replace the last four stubs)
- Modify: `fsm/promotion_test.go` (append tests)

**Interfaces:**
- Consumes: Task 11 `lthashFromHex`, Task 12 `verifyAuthorityJWS` + `authority.CleanupCommandHash`, `replay.SafeSnapshotManifest.Validate()` / `.Seal()`.
- Produces: real `applyRecordPromotionAck` (closure equality), `applyPublishSafeSnapshot`, `applyScheduleUnsafeCleanup`, `applyRecordCleanupAck`. After this task the 17-command alphabet is fully implemented.

- [ ] **Step 1: Write the failing tests**

Append to `fsm/promotion_test.go`:

```go
// issuePromotion drives quorumVerifiedBlock through anchor+issuance and
// returns the FSM, signer, verified part hash, and the promote command.
func issuePromotion(t *testing.T) (*FSM, *authority.Signer, string, arbiter.PromoteSafePartition) {
	t.Helper()
	signer, params := authorityFixture(t)
	f, partHash := quorumVerifiedBlock(t, params)
	anchor := arbiter.AnchorRef{L3BlockHash: "0xaa", StateRoot: "0xbb"}
	mustApply(t, f, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: 1, Anchor: anchor, FinalityReached: true, LastMergeableReached: true}})
	promote := arbiter.PromoteSafePartition{
		TableID: "db.t", PartitionID: "p0", PromotionSeq: 1,
		CandidateParts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}},
	}
	token, err := signer.SignPromotion(promote)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	mustApply(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote, AuthorityJWS: token}})
	return f, signer, partHash, promote
}

func TestPromotionAck_ClosureAndSafeFlip(t *testing.T) {
	f, _, partHash, _ := issuePromotion(t)
	tp := arbiter.TablePartition{TableID: "db.t", PartitionID: "p0"}

	// closure: base is all-zero, sum(verified) = partHash → post must equal partHash
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0",
		PostPartitionCommitment: partHash, Applied: true,
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: partHash, SafePartName: "all_9_9_0"}}}
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack}})

	if f.st.Statements[1].Status != StatusSafe {
		t.Fatalf("all parts promoted → Safe, got %v", f.st.Statements[1].Status)
	}
	ps := f.st.Partitions[tp]
	if ps == nil || ps.BasePartitionRoot != partHash || ps.PublishSeq != 1 {
		t.Fatalf("partition base must advance: %+v", ps)
	}
	if !f.st.PromotedUnsafe[tp][partHash] {
		t.Fatal("promoted part must enter the PromotedUnsafe registry")
	}
	// duplicate ack absorbed
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack}})
}

func TestPromotionAck_ClosureFailureBlocks(t *testing.T) {
	f, _, _, _ := issuePromotion(t)
	bad := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0",
		PostPartitionCommitment: lthashHex("rowEvil", "rowExtra"), Applied: true}
	applyExpectReject(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: bad}})
	if f.st.Statements[1].Status == StatusSafe {
		t.Fatal("closure failure must not flip Safe")
	}
	if f.st.Partitions[arbiter.TablePartition{TableID: "db.t", PartitionID: "p0"}] != nil {
		t.Fatal("closure failure must not advance the partition base")
	}
}

func TestPromotionAck_CASFailureRecordedOnly(t *testing.T) {
	f, _, _, _ := issuePromotion(t)
	notApplied := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0",
		Applied: false, Detail: "base CAS failed"}
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: notApplied}})
	if f.st.Statements[1].Status == StatusSafe {
		t.Fatal("applied=false must not flip Safe")
	}
	if !f.st.PendingPromotions[1].Acked {
		t.Fatal("applied=false consumes the pending promotion (orchestrator rebases with a NEW seq)")
	}
	// mismatched partition rejected
	f2, _, partHash2, _ := issuePromotion(t)
	wrong := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.OTHER", PartitionID: "p0",
		PostPartitionCommitment: partHash2, Applied: true}
	applyExpectReject(t, f2, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: wrong}})
	// unknown promotion_seq rejected
	applyExpectReject(t, f2, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: arbiter.PromotionAck{PromotionSeq: 9}}})
}

func TestPublishSafeSnapshot_ValidateAndWatermark(t *testing.T) {
	f, _, partHash, _ := issuePromotion(t)
	m, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq: 1, SchemaSnapshotID: "schema-genesis", SchemaRoot: "0xschr", ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []replay.TableManifest{{TableID: "db.t",
			PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: partHash}}}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	mustApply(t, f, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: m}})
	if f.st.SafeWatermark.SnapshotID != m.SnapshotID || f.st.SafeWatermark.SafeBlockSeq != 1 {
		t.Fatalf("watermark: %+v", f.st.SafeWatermark)
	}
	// tampered manifest rejected
	bad := m
	bad.DataRoot = "0xtampered"
	applyExpectReject(t, f, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: bad}})
	// non-advancing safe_block_seq rejected
	applyExpectReject(t, f, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: m}})
}

func TestCleanup_ScheduleAndAck(t *testing.T) {
	f, signer, partHash, _ := issuePromotion(t)
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0",
		PostPartitionCommitment: partHash, Applied: true}
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack}})
	tp := arbiter.TablePartition{TableID: "db.t", PartitionID: "p0"}

	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1,
		Parts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}}}
	token, err := signer.SignCleanup(cleanup)
	if err != nil {
		t.Fatalf("sign cleanup: %v", err)
	}
	mustApply(t, f, wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: cleanup, AuthorityJWS: token}})
	if f.st.PendingCleanups[1] == nil {
		t.Fatal("cleanup must be scheduled")
	}
	// bad signature rejected
	rogue := cleanup
	rogue.PromotionSeq = 2
	applyExpectReject(t, f, wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: rogue, AuthorityJWS: token}})

	mustApply(t, f, wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0"}}})
	if f.st.PromotedUnsafe[tp][partHash] {
		t.Fatal("cleanup ack must clear the registry entry")
	}
	if f.st.PendingCleanups[1] != nil {
		t.Fatal("cleanup ack must consume the schedule")
	}
	// re-ack idempotent
	mustApply(t, f, wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0"}}})
}
```

`promotion_test.go` gains the `"housegate/housegate/pkg/replay"` import.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./fsm/ -run 'TestPromotionAck|TestPublishSafeSnapshot|TestCleanup' 2>&1 | tail -5`
Expected: FAIL on the stubs.

- [ ] **Step 3: Replace the last four stubs in `fsm/apply.go`**

```go
func (f *FSM) applyRecordPromotionAck(c *wire.RecordPromotionAck) any {
	ack := c.Ack
	pp, ok := f.st.PendingPromotions[ack.PromotionSeq]
	if !ok {
		return Rejected{Reason: "unknown promotion_seq"}
	}
	if pp.Acked {
		return Applied{} // idempotent re-ack (§10.3)
	}
	if ack.TableID != pp.Promote.TableID || ack.PartitionID != pp.Promote.PartitionID {
		return Rejected{Reason: "ack partition does not match the promotion"}
	}
	if !ack.Applied {
		// Base CAS failed on SNode: the promotion is consumed; rebase +
		// re-issue with a NEW promotion_seq is the orchestrator's job
		// (§8.3). Recording is the FSM's whole responsibility here.
		pp.Acked = true
		return Applied{}
	}
	// Closure equality (§7.3): post == base ⊕ Σ(verified new-part lthash).
	tp := arbiter.TablePartition{TableID: ack.TableID, PartitionID: ack.PartitionID}
	expect := lthash.New()
	if ps, ok := f.st.Partitions[tp]; ok && ps.BasePartitionRoot != "" {
		base, err := lthashFromHex(ps.BasePartitionRoot)
		if err != nil {
			return Rejected{Reason: "stored base partition root is malformed"}
		}
		expect = base
	}
	for _, pr := range pp.Promote.CandidateParts {
		h, err := lthashFromHex(pr.PartRowLtHash)
		if err != nil {
			return Rejected{Reason: "promotion candidate part lthash is malformed"}
		}
		expect.AddHash(h)
	}
	post, err := lthashFromHex(ack.PostPartitionCommitment)
	if err != nil {
		return Rejected{Reason: "post_partition_commitment is malformed"}
	}
	if !expect.Equal(post) {
		return Rejected{Reason: "closure equality failed: post != base + sum(verified parts) (§7.3/§8.4 — extra or slipped part)"}
	}
	// advance the partition base + registry + statement lifecycle
	ps := f.st.Partitions[tp]
	if ps == nil {
		ps = &PartitionState{}
		f.st.Partitions[tp] = ps
	}
	ps.BaseSafeSnapshotID = pp.Promote.BaseSafeSnapshotID
	ps.BasePartitionRoot = ack.PostPartitionCommitment
	ps.PublishSeq = ack.PromotionSeq
	pp.Acked = true
	reg := f.st.PromotedUnsafe[tp]
	if reg == nil {
		reg = map[string]bool{}
		f.st.PromotedUnsafe[tp] = reg
	}
	for _, pr := range pp.Promote.CandidateParts {
		reg[pr.PartRowLtHash] = true
	}
	for _, seq := range pp.StatementSeqs {
		ss, ok := f.st.Statements[seq]
		if !ok {
			continue
		}
		for _, pr := range pp.Promote.CandidateParts {
			delete(ss.UnpromotedParts, pr.PartRowLtHash)
		}
		if len(ss.UnpromotedParts) == 0 && ss.Status == StatusPromotable {
			ss.Status = StatusSafe
		}
	}
	return Applied{}
}

func (f *FSM) applyPublishSafeSnapshot(c *wire.PublishSafeSnapshot) any {
	m := c.Manifest
	if err := m.Validate(); err != nil {
		return Rejected{Reason: "manifest validation failed: " + err.Error()}
	}
	if f.st.SafeWatermark.SnapshotID != "" && m.SafeBlockSeq <= f.st.SafeWatermark.SafeBlockSeq {
		return Rejected{Reason: "safe_block_seq must advance the watermark"}
	}
	rec := m
	f.st.Manifests[m.SnapshotID] = &rec
	f.st.SafeWatermark = SafeWatermark{SnapshotID: m.SnapshotID, SafeBlockSeq: m.SafeBlockSeq, ManifestRoot: m.ManifestRoot}
	return Applied{}
}

func (f *FSM) applyScheduleUnsafeCleanup(c *wire.ScheduleUnsafeCleanup) any {
	wantHash, err := authority.CleanupCommandHash(c.Cleanup)
	if err != nil {
		return Rejected{Reason: "hash cleanup command: " + err.Error()}
	}
	if err := verifyAuthorityJWS(c.AuthorityJWS, wantHash, f.st.Params.AuthorityAddresses); err != nil {
		return Rejected{Reason: err.Error()}
	}
	if _, dup := f.st.PendingCleanups[c.Cleanup.PromotionSeq]; dup {
		return Applied{} // idempotent re-schedule
	}
	cl := c.Cleanup
	f.st.PendingCleanups[cl.PromotionSeq] = &cl
	return Applied{}
}

func (f *FSM) applyRecordCleanupAck(c *wire.RecordCleanupAck) any {
	cl, ok := f.st.PendingCleanups[c.Ack.PromotionSeq]
	if !ok {
		return Applied{} // already cleaned or never scheduled: idempotent (§10.3)
	}
	tp := arbiter.TablePartition{TableID: cl.TableID, PartitionID: cl.PartitionID}
	if reg := f.st.PromotedUnsafe[tp]; reg != nil {
		for _, pr := range cl.Parts {
			delete(reg, pr.PartRowLtHash)
		}
		if len(reg) == 0 {
			delete(f.st.PromotedUnsafe, tp)
		}
	}
	delete(f.st.PendingCleanups, c.Ack.PromotionSeq)
	return Applied{}
}
```

`apply.go` gains the `"housegate/housegate/pkg/lthash"` import.

- [ ] **Step 4: Run the tests**

Run: `go test ./fsm/ -v 2>&1 | tail -10`
Expected: ALL PASS. The full 17-command alphabet is now live (grep `not implemented` under `fsm/` must return nothing).

- [ ] **Step 5: Commit**

```bash
git add fsm/
git commit -m "feat(fsm): promotion-ack closure equality, safe snapshot watermark, cleanup registry

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 14: determinism flagship tests + in-process mini e2e

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Create: `fsm/determinism_test.go`

**Interfaces:**
- Consumes: every fixture helper from Tasks 5–13 (`signUserJWS`, `signedAttestation`, `signedScan`, `lthashHex`, `authorityFixture`, `rcFor`); `Snapshot`/`Restore` (Task 6).
- Produces: test-only. This is the design §12 flagship: same log ⇒ same state; midpoint snapshot ⇒ same state; full §3.5 lifecycle as one command stream.

- [ ] **Step 1: Write the tests**

Create `fsm/determinism_test.go`:

```go
package fsm

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/hashicorp/raft"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// lifecycleScript builds the full §3.5 happy path as encoded RaftCommand
// bytes: membership → submit×2 → seal → rc → mark → 2×(attest+scan) →
// anchor+finality → promotion issued → ack → publish manifest → cleanup.
// Every byte is deterministic given the fixed test keys.
func lifecycleScript(t *testing.T) ([][]byte, Params) {
	t.Helper()
	signer, params := authorityFixture(t)

	// A scratch FSM discovers the deterministic values (selection, receipt
	// contents) while we record every encoded command.
	f := New(params)
	var script [][]byte
	apply := func(c wire.Command) any {
		b, err := wire.Encode(c)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		script = append(script, b)
		return f.Apply(&raft.Log{Data: b})
	}
	mustOK := func(c wire.Command) {
		if r, bad := apply(c).(Rejected); bad {
			t.Fatalf("script rejected: %s", r.Reason)
		}
	}

	mustOK(wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, Ed25519Pubkey: testPubkey('1')}}})
	mustOK(wire.Command{MarkActive: &wire.MarkActive{NodeID: "s1"}})
	for _, id := range []string{"v1", "v2", "v3"} {
		mustOK(wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: id, Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: testPubkey(id[1])}}})
		mustOK(wire.Command{MarkActive: &wire.MarkActive{NodeID: id}})
	}

	key, account := testAccount(t)
	for seq := uint64(1); seq <= 2; seq++ {
		mustOK(wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: validEnvelope(t, key, account, seq)}})
	}
	mustOK(wire.Command{SealL3Block: &wire.SealL3Block{}})

	partHash := lthashHex("rowA")
	rc := rcFor(f, 2, "0xr00t", partHash) // RC of the LAST statement carries the block claim
	rc.PartitionNewPartSums = []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p0", NewPartsLtHashSum: partHash}}
	mustOK(wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	// Evaluability requires EVERY statement's RC (design §8), so statement
	// 1 claims its own part too.
	part1 := lthashHex("row1")
	rcS1 := rcFor(f, 1, "0xmid", part1)
	rcS1.PartitionNewPartSums = []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p0", NewPartsLtHashSum: part1}}
	mustOK(wire.Command{RegisterRC: &wire.RegisterRC{RC: rcS1}})

	mustOK(wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: 1}})
	set := f.st.Verifications[1].VerifierSet

	// post commitment = zero base ⊕ part1 ⊕ partA
	post := lthashHex("row1", "rowA")
	receipt := receiptForBlock(f, "0xr00t")
	receipt.PartitionCommitmentsAfter = []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: post}}
	scans := []arbiter.PartScan{
		{TableID: "db.t", PartitionID: "p0", ClaimedPartRowLtHash: part1, ScannedPartRowLtHash: part1},
		{TableID: "db.t", PartitionID: "p0", ClaimedPartRowLtHash: partHash, ScannedPartRowLtHash: partHash},
	}
	for _, rid := range set[:2] {
		mustOK(wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: signedAttestation(t, rid, rid[len(rid)-1], receipt)}})
		mustOK(wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: signedScan(t, rid, rid[len(rid)-1], 1, scans)}})
	}

	mustOK(wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: 1,
		Anchor: arbiter.AnchorRef{L3BlockHash: "0xaa", StateRoot: "0xbb"}, FinalityReached: true, LastMergeableReached: true}})

	promote := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1,
		CandidateParts: []arbiter.PartRef{
			{TableID: "db.t", PartitionID: "p0", PartRowLtHash: part1},
			{TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}}}
	token, err := signer.SignPromotion(promote)
	if err != nil {
		t.Fatalf("sign promotion: %v", err)
	}
	mustOK(wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote, AuthorityJWS: token}})
	mustOK(wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: arbiter.PromotionAck{
		NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", PostPartitionCommitment: post, Applied: true}}})

	manifest, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq: 1, SchemaSnapshotID: params.SchemaSnapshotID, SchemaRoot: "0xschr", ExecutorProfileID: params.ExecutorProfileID,
		Tables: []replay.TableManifest{{TableID: "db.t", PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: post}}}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	mustOK(wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: manifest}})

	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1,
		Parts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: part1}, {TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}}}
	ctoken, err := signer.SignCleanup(cleanup)
	if err != nil {
		t.Fatalf("sign cleanup: %v", err)
	}
	mustOK(wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: cleanup, AuthorityJWS: ctoken}})
	mustOK(wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0"}}})

	// the scripted FSM itself must have finished Safe
	if f.st.Statements[1].Status != StatusSafe || f.st.Statements[2].Status != StatusSafe {
		t.Fatalf("lifecycle fixture must end Safe: %v %v", f.st.Statements[1].Status, f.st.Statements[2].Status)
	}
	return script, params
}

// stateJSON renders a canonical view of the full state for deep equality.
func stateJSON(t *testing.T, f *FSM) string {
	t.Helper()
	var buf bytes.Buffer
	f.mu.RLock()
	err := writeSnapshot(&buf, f.st)
	f.mu.RUnlock()
	if err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
	// strip to the JSON doc for readable diffs (accumulator dump compared raw)
	var doc any
	if e := json.Unmarshal(buf.Bytes()[13:13+jsonLen(buf.Bytes())], &doc); e != nil {
		return buf.String() // fall back to raw bytes
	}
	pretty, _ := json.Marshal(doc)
	return string(pretty) + "|" + buf.String()[len(buf.String())-64:]
}

func jsonLen(b []byte) int {
	n := 0
	for i := 5; i < 13; i++ {
		n = n<<8 | int(b[i])
	}
	return n
}

func TestDeterminism_SameLogSameState(t *testing.T) {
	script, params := lifecycleScript(t)
	replicas := []*FSM{New(params), New(params), New(params)}
	for _, f := range replicas {
		for _, b := range script {
			f.Apply(&raft.Log{Data: b})
		}
	}
	want := stateJSON(t, replicas[0])
	s0, err := replicas[0].Summary()
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	for i, f := range replicas[1:] {
		if got := stateJSON(t, f); got != want {
			t.Fatalf("replica %d state diverged", i+1)
		}
		si, _ := f.Summary()
		if si != s0 {
			t.Fatalf("replica %d summary diverged: %+v vs %+v", i+1, si, s0)
		}
	}
	if s0.SafeBlockSeq != 1 || s0.StatementCount != 2 {
		t.Fatalf("unexpected terminal summary: %+v", s0)
	}
}

func TestDeterminism_MidpointSnapshotEquivalence(t *testing.T) {
	script, params := lifecycleScript(t)
	straight := New(params)
	for _, b := range script {
		straight.Apply(&raft.Log{Data: b})
	}
	want := stateJSON(t, straight)

	// cut at every 3rd index to keep runtime sane while sweeping phases
	for cut := 1; cut < len(script); cut += 3 {
		a := New(params)
		for _, b := range script[:cut] {
			a.Apply(&raft.Log{Data: b})
		}
		b2 := restoreInto(t, snapshotBytes(t, a))
		for _, b := range script[cut:] {
			b2.Apply(&raft.Log{Data: b})
		}
		if got := stateJSON(t, b2); got != want {
			t.Fatalf("snapshot at %d then replay diverged from straight-through", cut)
		}
	}
}
```

The fixture registers TWO RCs: `rcS1` (statement 1, part1) and `rc` (statement 2, partHash) whose `SourceClaimRoot` is the block claim — evaluability requires every statement's RC bound before the three-way evaluates.

- [ ] **Step 2: Run the tests**

Run: `go test ./fsm/ -run TestDeterminism -v 2>&1 | tail -6`
Expected: PASS. If `stateJSON` diverges only in the accumulator tail, a snapshot field was missed in Task 6 — fix `snapshotDoc`, not the test.

- [ ] **Step 3: Run the whole repo + vet**

Run: `go vet ./... && go test ./... 2>&1 | tail -6`
Expected: clean vet; ALL PASS.

- [ ] **Step 4: Commit**

```bash
git add fsm/
git commit -m "test(fsm): N-replica determinism, midpoint-snapshot equivalence, lifecycle e2e

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 15: raftnode — ConsensusNode assembly + cluster tests

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Create: `raftnode/node.go`, `raftnode/node_test.go`

**Interfaces:**
- Consumes: frozen `raftnode.ConsensusNode` seam (unchanged), `fsm.New`/`fsm.Summary`, `wire.Encode`.
- Produces: `raftnode.Options{NodeID, FSM, LogStore, StableStore, SnapshotStore, Transport, RaftConfig}`, `New(Options) (*Node, error)` (`*Node` implements `ConsensusNode`), `(*Node).Bootstrap(servers []raft.Server) error`, `(*Node).Shutdown() error`, `(*Node).Raw() *raft.Raft` (P1b wiring: AddVoter etc.).

- [ ] **Step 1: Write the failing tests**

Create `raftnode/node_test.go`:

```go
package raftnode

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/wire"
)

// testNode bundles a Node with its FSM and inmem transport.
type testNode struct {
	node *Node
	fsm  *fsm.FSM
	addr raft.ServerAddress
	tr   *raft.InmemTransport
	id   string
}

// tuned returns a raft.Config with test-fast timeouts.
func tuned(id string) *raft.Config {
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 50 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond
	cfg.CommitTimeout = 5 * time.Millisecond
	cfg.SnapshotInterval = 500 * time.Millisecond
	cfg.SnapshotThreshold = 8192
	cfg.LogOutput = nil
	return cfg
}

func startNode(t *testing.T, id string) *testNode {
	t.Helper()
	f := fsm.New(fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	addr, tr := raft.NewInmemTransport(raft.ServerAddress(id))
	n, err := New(Options{
		NodeID: id, FSM: f,
		LogStore: raft.NewInmemStore(), StableStore: raft.NewInmemStore(),
		SnapshotStore: raft.NewInmemSnapshotStore(), Transport: tr,
		RaftConfig: tuned(id),
	})
	if err != nil {
		t.Fatalf("New(%s): %v", id, err)
	}
	tn := &testNode{node: n, fsm: f, addr: addr, tr: tr, id: id}
	t.Cleanup(func() { _ = n.Shutdown() })
	return tn
}

func connectAll(nodes ...*testNode) {
	for _, a := range nodes {
		for _, b := range nodes {
			if a != b {
				a.tr.Connect(b.addr, b.tr)
			}
		}
	}
}

// waitFor polls cond with a deadline (raft elections are inherently
// asynchronous; bounded polling is the accepted pattern here).
func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func leaderOf(nodes []*testNode) *testNode {
	for _, n := range nodes {
		if n.node.Raw().State() == raft.Leader {
			return n
		}
	}
	return nil
}

func mustCmd(t *testing.T, c wire.Command) []byte {
	t.Helper()
	b, err := wire.Encode(c)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return b
}

func registerCmds(id string, role arbiter.NodeRole) []wire.Command {
	pk := make([]byte, 32)
	pk[0] = id[len(id)-1]
	return []wire.Command{
		{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: id, Roles: []arbiter.NodeRole{role}, Ed25519Pubkey: pk}}},
		{MarkActive: &wire.MarkActive{NodeID: id}},
	}
}

func TestNew_RequiresInjection(t *testing.T) {
	if _, err := New(Options{NodeID: "n1"}); err == nil {
		t.Fatal("missing stores/transport must error")
	}
	if _, err := New(Options{}); err == nil {
		t.Fatal("missing NodeID must error")
	}
}

func TestCluster_ThreeNodesConverge(t *testing.T) {
	nodes := []*testNode{startNode(t, "n1"), startNode(t, "n2"), startNode(t, "n3")}
	connectAll(nodes...)
	servers := make([]raft.Server, len(nodes))
	for i, n := range nodes {
		servers[i] = raft.Server{ID: raft.ServerID(n.id), Address: n.addr}
	}
	if err := nodes[0].node.Bootstrap(servers); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	waitFor(t, "leader election", 5*time.Second, func() bool { return leaderOf(nodes) != nil })
	leader := leaderOf(nodes)

	if err := leader.node.VerifyLeader(); err != nil {
		t.Fatalf("VerifyLeader on the leader: %v", err)
	}

	var cmds []wire.Command
	for i := 1; i <= 4; i++ {
		cmds = append(cmds, registerCmds(fmt.Sprintf("dp%d", i), arbiter.NodeRoleVerifier)...)
	}
	for _, c := range cmds {
		fut := leader.node.Apply(mustCmd(t, c), 2*time.Second)
		if err := fut.Error(); err != nil {
			t.Fatalf("apply: %v", err)
		}
		if r, bad := fut.Response().(fsm.Rejected); bad {
			t.Fatalf("rejected: %s", r.Reason)
		}
	}
	if err := leader.node.Barrier(2 * time.Second); err != nil {
		t.Fatalf("Barrier: %v", err)
	}
	want, err := leader.fsm.Summary()
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if want.ActiveNodes != 4 {
		t.Fatalf("leader summary: %+v", want)
	}
	waitFor(t, "replica convergence", 5*time.Second, func() bool {
		for _, n := range nodes {
			s, err := n.fsm.Summary()
			if err != nil || s != want {
				return false
			}
		}
		return true
	})
}

func TestCluster_LeaderTransferMidStream(t *testing.T) {
	nodes := []*testNode{startNode(t, "n1"), startNode(t, "n2"), startNode(t, "n3")}
	connectAll(nodes...)
	servers := []raft.Server{}
	for _, n := range nodes {
		servers = append(servers, raft.Server{ID: raft.ServerID(n.id), Address: n.addr})
	}
	if err := nodes[0].node.Bootstrap(servers); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	waitFor(t, "leader", 5*time.Second, func() bool { return leaderOf(nodes) != nil })
	l1 := leaderOf(nodes)
	for _, c := range registerCmds("dp1", arbiter.NodeRoleSNode) {
		if err := l1.node.Apply(mustCmd(t, c), 2*time.Second).Error(); err != nil {
			t.Fatalf("apply: %v", err)
		}
	}
	if err := l1.node.Raw().LeadershipTransfer().Error(); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	waitFor(t, "new leader", 5*time.Second, func() bool {
		l := leaderOf(nodes)
		return l != nil && l != l1
	})
	l2 := leaderOf(nodes)
	for _, c := range registerCmds("dp2", arbiter.NodeRoleSNode) {
		if err := l2.node.Apply(mustCmd(t, c), 2*time.Second).Error(); err != nil {
			t.Fatalf("apply after transfer: %v", err)
		}
	}
	want, _ := l2.fsm.Summary()
	waitFor(t, "convergence after transfer", 5*time.Second, func() bool {
		for _, n := range nodes {
			if s, err := n.fsm.Summary(); err != nil || s != want {
				return false
			}
		}
		return true
	})
}

func TestCluster_LateJoinerRestoresFromSnapshot(t *testing.T) {
	n1 := startNode(t, "n1")
	if err := n1.node.Bootstrap([]raft.Server{{ID: "n1", Address: n1.addr}}); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	waitFor(t, "single-node leadership", 5*time.Second, func() bool { return n1.node.Raw().State() == raft.Leader })
	for i := 1; i <= 6; i++ {
		for _, c := range registerCmds(fmt.Sprintf("dp%d", i), arbiter.NodeRoleVerifier) {
			if err := n1.node.Apply(mustCmd(t, c), 2*time.Second).Error(); err != nil {
				t.Fatalf("apply: %v", err)
			}
		}
	}
	// force a snapshot so the joiner catches up via FSM.Restore
	if err := n1.node.Raw().Snapshot().Error(); err != nil {
		t.Fatalf("manual snapshot: %v", err)
	}
	n2 := startNode(t, "n2")
	n1.tr.Connect(n2.addr, n2.tr)
	n2.tr.Connect(n1.addr, n1.tr)
	if err := n1.node.Raw().AddVoter("n2", n2.addr, 0, 2*time.Second).Error(); err != nil {
		t.Fatalf("AddVoter: %v", err)
	}
	want, _ := n1.fsm.Summary()
	waitFor(t, "late joiner catch-up", 10*time.Second, func() bool {
		s, err := n2.fsm.Summary()
		return err == nil && s == want
	})
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./raftnode/ 2>&1 | head -5`
Expected: compile errors (no `Node`/`New`).

- [ ] **Step 3: Implement `raftnode/node.go`**

```go
package raftnode

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/raft"
)

// Options assembles a Node. Everything is injected: tests use raft's inmem
// store/transport; P1b's cmd supplies raft-boltdb + TCP transport and the
// tuned timeouts (arbiter design Open Question 2). P1a deliberately adds
// no storage dependency and no config surface.
type Options struct {
	NodeID        string
	FSM           raft.FSM
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	Transport     raft.Transport
	// RaftConfig, when nil, defaults to raft.DefaultConfig(); LocalID is
	// always overwritten with NodeID.
	RaftConfig *raft.Config
}

// Node wraps hashicorp/raft behind the frozen ConsensusNode seam.
type Node struct {
	r *raft.Raft
}

var _ ConsensusNode = (*Node)(nil)

// New builds and starts the raft node (it begins as a follower; call
// Bootstrap on the first node of a new cluster).
func New(opts Options) (*Node, error) {
	if opts.NodeID == "" {
		return nil, errors.New("raftnode: NodeID is required")
	}
	if opts.FSM == nil || opts.LogStore == nil || opts.StableStore == nil || opts.SnapshotStore == nil || opts.Transport == nil {
		return nil, errors.New("raftnode: FSM, LogStore, StableStore, SnapshotStore, and Transport must all be injected")
	}
	cfg := opts.RaftConfig
	if cfg == nil {
		cfg = raft.DefaultConfig()
	}
	cfg.LocalID = raft.ServerID(opts.NodeID)
	r, err := raft.NewRaft(cfg, opts.FSM, opts.LogStore, opts.StableStore, opts.SnapshotStore, opts.Transport)
	if err != nil {
		return nil, fmt.Errorf("raftnode: %w", err)
	}
	return &Node{r: r}, nil
}

// Apply proposes an encoded RaftCommand (leader only; followers' futures
// fail with raft.ErrNotLeader).
func (n *Node) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	return n.r.Apply(cmd, timeout)
}

// VerifyLeader confirms leadership before orchestrator side effects (§10.2).
func (n *Node) VerifyLeader() error {
	return n.r.VerifyLeader().Error()
}

// LeaderCh signals leadership changes. hashicorp/raft's channel is
// buffered(1) and MAY DROP notifications under churn — treat it as a
// wake-up hint; correctness rests on VerifyLeader before every side
// effect (the seam contract).
func (n *Node) LeaderCh() <-chan bool {
	return n.r.LeaderCh()
}

// Barrier is the read-index gate for linearizable SafeState reads (§11.3).
func (n *Node) Barrier(timeout time.Duration) error {
	return n.r.Barrier(timeout).Error()
}

// Bootstrap initializes a brand-new cluster with the given voter set. Call
// exactly once, on one node, before any Apply.
func (n *Node) Bootstrap(servers []raft.Server) error {
	return n.r.BootstrapCluster(raft.Configuration{Servers: servers}).Error()
}

// Shutdown stops the node and waits for it to wind down.
func (n *Node) Shutdown() error {
	return n.r.Shutdown().Error()
}

// Raw exposes the underlying raft handle for assembly-time operations
// (AddVoter, LeadershipTransfer, manual Snapshot); the data path goes
// through the ConsensusNode seam only.
func (n *Node) Raw() *raft.Raft {
	return n.r
}
```

- [ ] **Step 4: Run the tests (race on)**

Run: `go test ./raftnode/ -race -v 2>&1 | tail -10`
Expected: ALL PASS (convergence, transfer, snapshot late-join). These are bounded-poll tests; if `late joiner catch-up` flakes, raise its `waitFor` timeout — never add bare sleeps.

- [ ] **Step 5: Commit**

```bash
git add raftnode/
git commit -m "feat(raftnode): hashicorp/raft assembly behind the ConsensusNode seam

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
```

---

### Task 16: arbiter CI + README

**Repo:** `/Users/uranuswch/src/sentio_xyz/arbiter`

**Files:**
- Create: `.github/workflows/ci.yml`
- Modify: `README.md` (add the fsm/raftnode/wire section)

**Interfaces:**
- Consumes: everything; secret `GH_MODULES_TOKEN` (fine-grained PAT with read access to `sentioxyz/arbiter-proto` + `housegate/housegate`) — **user-provisioned in the repo's Actions secrets before merge; the workflow fails without it**.
- Produces: CI on push/PR: build + vet + the two red-line tripwires + tests.

- [ ] **Step 1: Create `.github/workflows/ci.yml`**

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - name: configure private module access
        run: |
          git config --global url."https://x-access-token:${{ secrets.GH_MODULES_TOKEN }}@github.com/".insteadOf "https://github.com/"
          go env -w GOPRIVATE=github.com/sentioxyz,github.com/housegate
      - run: go build ./...
      - run: go vet ./...
      - name: fsm import red line (never gen/pb)
        run: |
          if go list -deps ./fsm | grep -q 'arbiter-proto/gen/pb'; then
            echo "fsm must not import arbiter-proto/gen/pb (design §2 structural boundary)"
            exit 1
          fi
      - name: fsm wall-clock red line (no time.Now)
        run: |
          if grep -rn 'time\.Now' fsm/; then
            echo "fsm must be wall-clock-free (§4.3 red line 1)"
            exit 1
          fi
      - run: go test ./...
```

- [ ] **Step 2: Append the package section to `README.md`**

Add after the existing package list (match the README's current tone):

```markdown
## P1a packages

- `wire/` — the only pb ⇄ Go boundary: `RaftCommand` encode/decode plus per-message converters. The canonical Go form uses nil for empty repeated fields; a producer hashing `[]` instead of `null` fails its own hash recomputation.
- `fsm/` — the deterministic replicated state machine (raft.FSM): §6.3 admission over the accumulator, deterministic source/verifier selection, L3 sealing with an anchor-excluded hash chain, the in-Apply three-way predicate + closure equality, and a versioned snapshot container. Never imports `gen/pb`, never reads the wall clock (both CI-enforced).
- `raftnode/` — hashicorp/raft behind the frozen `ConsensusNode` seam; stores/transport injected (bolt + TCP arrive with P1b's `cmd/arbiter`).

CI needs the `GH_MODULES_TOKEN` Actions secret (fine-grained PAT, read access to `sentioxyz/arbiter-proto` and `housegate/housegate`) for private module fetch.
```

- [ ] **Step 3: Validate locally**

```bash
go list -deps ./fsm | grep -c 'arbiter-proto/gen/pb' || echo "fsm clean"
grep -rn 'time\.Now' fsm/ || echo "wall-clock clean"
go build ./... && go vet ./... && go test ./...
gofmt -l . | grep -v '^gen/' || true
```

Expected: `fsm clean`, `wall-clock clean`, build/vet/test green, no unformatted files.

- [ ] **Step 4: Commit and push; verify the workflow**

```bash
git add .github/ README.md
git commit -m "ci: build+vet+test with fsm red-line tripwires and private-module access

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin main
gh run watch --repo sentioxyz/arbiter --exit-status || echo "REMINDER: provision GH_MODULES_TOKEN in the repo's Actions secrets, then re-run"
```

Expected: the run goes green once `GH_MODULES_TOKEN` exists; if the secret is missing, the private-module step fails — hand the reminder to the user (they provision; decision recorded 2026-07-05).

---

## Self-Review Notes (spec coverage)

- Spec §1 decisions → Tasks 1 (enum), 16 (CI/PAT), 2 (MaxTokenAge), 3 (`StatementEnvelope` naming). §2 layout + import red line → Tasks 3–5 + CI tripwire (16). §3 mirrors/domains → Task 3 (adds `PromotionAck`/`SafePartMapping`/`CleanupAck`, a recorded plan-level completion). §4 State/Params/rejected-but-committed → Task 5. §5 snapshot + chain hash → Tasks 6, 8. §6 admission → Task 7. §7 selection → Tasks 7–8. §8 block-level three-way → Task 11. §9 handler table → Tasks 5, 8, 9, 10, 12, 13 (all 17 commands; the stub table in Task 5 names its replacement task). §10 raftnode → Task 15. §11 ledger todos → Tasks 1, 2, 16. §12 testing → per-task tests + Task 14 flagship + Task 15 cluster. §13 tripwires → CI (16) + `MatchSourceRoot` unread (11) + closure (13). §14 follow-ups: none scheduled here, as specified.
- Type-consistency pass: `wire.Command` field names match `fsm.Apply`'s dispatch; `SubmitResult`/`SealResult`/`Applied`/`Rejected` used consistently in all task tests; `lthashFromHex` defined in Task 11 and consumed in Task 13; evidence helpers (`signedAttestation`/`signedScan`/`lthashHex`/`rcFor`) defined in Tasks 9–11 and reused in 12–14; `authority.PromoteCommandHash`/`CleanupCommandHash` exported in Task 12 before first use.
- Placeholder scan: the only "not implemented" strings are the deliberate Task 5 stubs, each naming the task that replaces it; Task 13 Step 4 greps them away.
- Generated-identifier caveats are marked at their use sites (pb oneof wrapper names, getter camelization, `authority.Signer` constructor) with the instruction to follow the generated/existing code — never to rename frozen surfaces.
