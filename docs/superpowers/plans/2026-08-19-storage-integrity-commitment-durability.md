# Commitment Durability and Admission Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Freeze the anchored commitment's preimage behind golden vectors that fail on any encoding change, stop network binding from degrading to a no-op, make the auditor read surface complete-or-error, make the SNode replay path and the agent deferred-INSERT path as strict as their fresh counterparts, enforce `settings_hash` against the enumerated housegate-owned key set instead of a prefix, and bind `statement_kind` into the signed statement payload before the mutation lane exists.

**Architecture:** Eight decisions across five repos. D1 adds committed golden fixtures (marshalled bytes + exact digest) for `CanonicalDigest(DomainL3Statements, []StatementEnvelope)` and `CanonicalDigest(DomainL3Header, L3BlockHeader)` in arbiter, mirrored for the envelope in arbiter-core so a field reorder fails that repo's own tests first; the digests live as Go source literals so no regenerator can silently re-bless them. D2 moves genesis-identity validation to `fsm.New`/`NewWithNotify`/`Restore`. D3 makes `L3BlockView` return sentinel errors and `GetL3Block` map them. D4 hoists `schema_hash` validation above the SNode cached-result branch. D5 teaches `chproto` to expose the client Data block name so the deferred lane refuses external-table blocks. D6 replaces the `SQL_x_*` prefix test with membership in the six exported owned keys plus `SQL_x_read_mode`. D7 adds `statement_kind` to `JWSStatementPayloadV2` — a signing-payload wire break sequenced exactly like Spec A: arbiter-proto minor bump → housegate (payload + regenerated shared vectors) → arbiter-core → arbiter → sentio-node → coordinated tags. D8 fixes the non-canonical-header taxonomy and the sentio-node `ErrPayloadMismatch` classification.

**Tech Stack:** Go 1.26 (Bazel 9 + Bzlmod in housegate/arbiter-core/arbiter/sentio-node; buf + `go test` in arbiter-proto), secp256k1 JWS (go-ethereum crypto), ClickHouse native TCP protocol via the sentioxyz ch-go v0.73 fork, gRPC/protobuf, hashicorp/raft.

**Spec:** `docs/superpowers/specs/2026-08-19-storage-integrity-commitment-durability-design.md` (Spec K) + roadmap `docs/superpowers/specs/2026-08-19-storage-integrity-remediation-roadmap.md` §4 decision 3, §5. Remediates `docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md` (Spec A), whose plan `docs/superpowers/plans/2026-08-18-storage-integrity-signed-envelope-v2.md` documents what was built.

## Progress and Publication Evidence

Fill this in as each release lands, in the shape Spec A's plan uses: PR number and URL, the merge commit SHA, the release-workflow run URL, the annotated tag object SHA and what it peels to, and for arbiter the published image digest. One line per release: arbiter-proto `v0.6.0` (Task 2), housegate `v0.10.0` (Task 9), arbiter-core `v0.4.0` (Task 13), arbiter `v0.3.0` (Task 21), sentio-node `v0.1.0` (Task 23).

## Global Constraints

- **Code base this plan starts from:** arbiter `71657a8` (v0.2.1), arbiter-core `b669ccd` (v0.3.1), arbiter-proto `bb1823f` (v0.5.0), housegate `621eaab` (v0.9.3), sentio-node `ba136ea` (no tags).
- **Releases this plan produces:** arbiter-proto `v0.6.0`, housegate `v0.10.0`, arbiter-core `v0.4.0`, arbiter `v0.3.0`, sentio-node `v0.1.0` (its first tag — roadmap §5). One release per repo; each repo's dependency-bump task lands the new pins in the same PR that carries its code change.
- **Dependency-bump debt closed by this plan (roadmap §5):** arbiter and arbiter-core move from housegate `v0.9.0` (7 commits behind) to `v0.10.0`; sentio-node moves from housegate `v0.9.2` (4 behind) to `v0.10.0`.
- **D1 is a freeze, not a migration.** `replay.CanonicalDigest` stays `SHA256("housegate-replay-mvp-v0:" ‖ domain ‖ 0x00 ‖ json.Marshal(v))`. RFC-8785 canonical JSON is explicitly rejected for this spec (it changes every historical root) and recorded as debt (spec D1, roadmap §4 decision 3, §6).
- **No task in this plan changes the field set or field order of `arbiter.StatementEnvelope` or `fsm.L3BlockHeader`.** D7 adds a field to `auth.JWSStatementPayloadV2` (a JWS payload, not a `CanonicalDigest` preimage). The golden digests below are therefore stable across the whole plan.
- **Frozen digests (computed against the code base above; paste verbatim, never recompute-and-bless):**
  - `statements_root` golden = `0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921`
  - header `ChainHash` golden = `0x168866c1498647761215bb57494f23658828b6b0073efc61dbccfc7d87ed8bd2`
  - envelope reorder-proof value (`sql_hash` ↔ `settings_hash` declaration swap) = `0x0efa77b50a1f4166e170dc6259a87151e8cd0da9d5b1c504cef1a103504d3117`
  - header reorder-proof value (`spent_ids_root_after` ↔ `statements_root` declaration swap) = `0x31e682a7b97139b43436281ed5c8e63e90fe7b2d27e697c3613fecb35cceeb77`
  - `EmptySettingsHash` (unchanged, Spec A) = `0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006`
- **JWS v2 stays otherwise unchanged:** compact serialization, protected header exactly `{"alg":"ES256K","typ":"JWT"}`, `purpose = "housegate-statement-v2"`, setting key `SQL_x_statement_token`. `statement_kind` is **appended last** to `JWSStatementPayloadV2` (append-only discipline; the committed token in the shared vectors is what freezes the payload byte layout).
- **Arbiter FSM red lines (CI tripwires, `.github/workflows/ci.yml`):** `fsm/` must not import `arbiter-proto/gen/pb` (only through `wire`), and `grep -rn 'time\.Now' fsm/` must find nothing. Every D2/D3/D7 change stays wall-clock-free.
- **arbiter / arbiter-core docker-gated tests** run only with `ARBITER_CH_INTEGRATION=1` and (optionally) `CH_ADDR`; default `127.0.0.1:9000`. `integration/chpipeline` additionally needs a live cluster harness.
- **arbiter / arbiter-core pin scripts:** `bash scripts/update-housegate.sh <tag-or-sha>` and `bash scripts/update-arbiter-core.sh <tag-or-sha>` update both `go.mod` and the Bzlmod `git_override`. A bare `go get` on the wrong module path silently does nothing. sentio-node has no script — edit `go.mod` and `MODULE.bazel` by hand.
- **housegate conventions (CLAUDE.md):** Bazel is the ground truth (`bazel test //...`); after adding packages or deps run `bazel mod tidy && bazel run //:gazelle`; module path `github.com/housegate/housegate`; logging via `pkg/log` (`Infow`/`Warnw`); errors wrapped with `%w`; English identifiers and comments; Markdown never hard-wrapped; docker-bound integration targets are tagged `manual` and must be listed explicitly in `.github/workflows/ci.yml`.
- **Merge order vs. Spec J:** this plan's repos must merge *after* Spec J's CI change lands, so the new tests actually execute in CI on the way in (roadmap §3). If J has not landed when a task is executed, run the named command locally and record the output in the commit body.

---

## File Structure (what is created / modified, by repo)

**arbiter-proto** (`/Users/uranuswch/Dev/sentio_xyz/arbiter-proto`)
- Modify `proto/arbiter.proto` — `StatementEnvelopeV2.statement_kind` comment records that it is bound by `user_jws` from envelope v2.1 onward; no field-number change.
- Regenerate `gen/pb/arbiter.pb.go` (committed).
- Create `conformance/statement_kind_binding_test.go`.

**housegate** (`/Users/uranuswch/Dev/housegate/housegate`)
- Modify `pkg/chproto/compress.go` — add `ClientDataInfo` + `InspectClientDataPacket`; `ClientDataPacketIsEmpty` becomes a wrapper. Test: `pkg/chproto/compress_test.go`.
- Modify `pkg/proxy/relay.go` (`runDeferredInsert`, ~:1264-1298) — refuse named client Data blocks. Test: `pkg/proxy/relay_deferred_test.go`.
- Modify `pkg/storageintegrity/settings.go` — enumerated owned-key set + `ReadModeSettingKey`. Test: `pkg/storageintegrity/settings_test.go`; new guard test `pkg/rewriter/read_mode_key_test.go`.
- Modify `pkg/auth/types.go` — `JWSStatementPayloadV2.StatementKind`, `StatementPayloadV2Mismatch` case, `SharedStatementVectorsSHA256`; create `pkg/auth/shared_vectors.go`.
- Modify `pkg/auth/statement_v2_test.go`, `pkg/auth/statement_v2_vectors_test.go`, `pkg/auth/testdata/statement_jws_v2.json` (regenerated, 17 vectors).
- Modify `pkg/storageintegrity/intake.go` — `StatementKindCode`.
- Modify `pkg/plugins/sistatement/plugin.go` (~:317), `pkg/plugins/storageintegrity/plugin.go` (~:463) and their tests.
- Modify `CLAUDE.md`, `go.mod`, `go.sum`, `MODULE.bazel` (arbiter-proto pin).

**arbiter-core** (`/Users/uranuswch/Dev/sentio_xyz/arbiter-core`)
- Create `conformance/statement_envelope_golden_test.go`, `conformance/testdata/statement_envelope_golden.json`.
- Modify `conformance/BUILD.bazel` (testdata `data` glob; gazelle usually adds it).
- Modify `snode/staged.go` (`PrepareLocalStatement` ~:73-130, new `resolveEnvelopeSchema`), `snode/staged_prepare_test.go`.
- Modify `go.mod`, `go.sum`, `MODULE.bazel` (housegate + arbiter-proto pins).

**arbiter** (`/Users/uranuswch/Dev/sentio_xyz/arbiter`)
- Create `fsm/l3_commitment_golden_test.go`, `fsm/testdata/l3_commitment_golden.json`.
- Modify `fsm/state.go` (`Params.Validate`), `fsm/fsm.go` (`New`), `fsm/watch.go` (`NewWithNotify`), `fsm/snapshot.go` (`readSnapshot`), `fsm/reads.go` (`L3BlockView`), `fsm/userjws.go` (header taxonomy + `statement_kind`), `fsm/userjws_v2_test.go`, `fsm/seal_test.go`, `fsm/fsm_test.go`, `fsm/watch_test.go`, `fsm/snapshot_test.go`.
- Modify `fsm/testdata/statement_jws_v2.json` (re-copied from housegate), create `fsm/shared_vectors_test.go`.
- Modify `server/safestate.go` (`GetL3Block`), `server/safestate_l3block_test.go`, `server/gateway_test.go`, `server/server_test.go`, `server/ingress_test.go`, `raftnode/node_test.go`, `orchestrator/loop_test.go`, `integration/cluster_node_test.go`, `integration/chpipeline/cluster_test.go`.
- Modify `cmd/arbiter/services.go` (`newFSM`), `cmd/arbiter/app.go` (`run`), `cmd/arbiter/main_test.go`.
- Modify `integration/chpipeline/fraud_test.go` (~:113 structured-field decoupling + new `statement_kind` fraud class).
- Modify `go.mod`, `go.sum`, `MODULE.bazel` (housegate + arbiter-core + arbiter-proto pins).

**sentio-node** (`/Users/uranuswch/Dev/sentio_xyz/sentio-node`)
- Modify `storageintegrityadapter/adapter.go` (~:73-77), `storageintegrityadapter/adapter_test.go`.
- Modify `go.mod`, `go.sum`, `MODULE.bazel`.

**docs (housegate repo)**
- Modify `docs/superpowers/specs/2026-08-19-storage-integrity-commitment-durability-design.md` (status), `docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md` (§4.2 field-list correction), `docs/superpowers/specs/2026-08-18-storage-integrity-design-v4-reconciliation.md` (Spec B edit list).

## Cross-task interface summary (names every task must agree on)

```go
// housegate pkg/chproto  (Task 3)
type ClientDataInfo struct {
	BlockName string // external-table name; "" for the INSERT payload stream
	Empty     bool   // zero columns AND zero rows
}
func InspectClientDataPacket(raw []byte, compression proto.Compression) (ClientDataInfo, error)
func ClientDataPacketIsEmpty(raw []byte, compression proto.Compression) (bool, error) // unchanged signature; wraps the above

// housegate pkg/storageintegrity  (Tasks 5, 6)
const ReadModeSettingKey = "SQL_x_read_mode"          // value-equal to rewriter.ReadModeSettingKey
func HousegateOwnedSettingKeys() []string             // the seven owned keys, sorted
func IsHousegateOwnedSettingKey(key string) bool      // set membership, no prefix test
func RejectUserSettings(keys []string) error          // unchanged signature and message shape
const StatementKindCodeInsert uint32 = 1              // mirrors pb.StatementKind_STATEMENT_KIND_INSERT
func StatementKindCode(k Kind) (uint32, error)        // KindInsert -> 1; anything else -> error

// housegate pkg/auth  (Tasks 6, 7)
type JWSStatementPayloadV2 struct { /* …existing 14 fields… */ StatementKind uint32 `json:"statement_kind"` }
func StatementPayloadV2Mismatch(got, want JWSStatementPayloadV2) string // gains a "statement_kind" case
const SharedStatementVectorsSHA256 = "<sha256 hex of pkg/auth/testdata/statement_jws_v2.json>"

// arbiter fsm  (Tasks 15, 16, 17)
func (p Params) Validate() error
var ErrGenesisParams  = errors.New("fsm: genesis params incomplete")
var ErrL3BlockNotFound = errors.New("fsm: no sealed L3 block at that seq")
var ErrL3BlockIncomplete = errors.New("fsm: sealed L3 block is missing statements")
func New(params Params) (*FSM, error)
func NewWithNotify(params Params, notify chan<- Event) (*FSM, error)
func (f *FSM) L3BlockView(seq uint64) (L3BlockHeader, string, []arbiter.StatementEnvelope, error)
const goldenStatementsRoot  = "0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921"
const goldenHeaderChainHash = "0x168866c1498647761215bb57494f23658828b6b0073efc61dbccfc7d87ed8bd2"

// arbiter cmd/arbiter  (Task 16)
func newFSM(cfg config.Config) (*fsm.FSM, <-chan fsm.Event, error)

// arbiter-core snode  (Task 12)
func (r *Role) resolveEnvelopeSchema(env arbiter.StatementEnvelope) (payloadexec.TableSchema, error)
// sentinels unchanged: ErrSchemaUnknown, ErrSchemaHashMismatch, ErrPayloadMismatch

// arbiter-core conformance  (Task 11)
const goldenStatementsRoot = "0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921"
```

---

## Phase 1 — arbiter-proto (spec D7, delivery step 4 head)

**Why the proto gets a minor bump when no field number changes.** `statement_kind` is already field 2 of `StatementEnvelopeV2`. What changes in D7 is that the field becomes *covered by `user_jws`*: a v0.5.0 signer produces tokens a v0.6.0 verifier rejects and vice versa. The proto module version is the only version number the five repos share, so it is the coordination signal. The `.proto` change is therefore a comment plus a conformance test that pins the field number the signing payload now depends on — exactly the discipline Spec A used for the v2 field appends.

### Task 1: Record `statement_kind` as a signed field and pin its number

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-proto`

**Files:**
- Modify: `proto/arbiter.proto` (`StatementEnvelopeV2` at ~:50-74)
- Regenerate: `gen/pb/arbiter.pb.go`
- Test: `conformance/statement_kind_binding_test.go` (new)

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: no new Go symbols. `pb.StatementEnvelopeV2.StatementKind` keeps field number 2 and type `pb.StatementKind`; downstream repos rely on `pb.StatementKind_STATEMENT_KIND_INSERT == 1`.

- [ ] **Step 1: Write the failing conformance test**

Create `conformance/statement_kind_binding_test.go`:

```go
package conformance

import (
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// TestStatementKindIsFieldTwoAndInsertIsOne pins what the envelope-v2.1 user
// JWS binds. The signing payload carries statement_kind as the numeric enum
// value, so both the field number and the INSERT enum number are consensus
// constants, not implementation details.
func TestStatementKindIsFieldTwoAndInsertIsOne(t *testing.T) {
	fd := (&pb.StatementEnvelopeV2{}).ProtoReflect().Descriptor().Fields().ByName(protoreflectName("statement_kind"))
	if fd == nil {
		t.Fatal("StatementEnvelopeV2 is missing statement_kind")
	}
	if fd.Number() != 2 {
		t.Fatalf("statement_kind number = %d, want 2", fd.Number())
	}
	if got := int32(pb.StatementKind_STATEMENT_KIND_INSERT); got != 1 {
		t.Fatalf("STATEMENT_KIND_INSERT = %d, want 1", got)
	}
	if got := int32(pb.StatementKind_STATEMENT_KIND_UNSPECIFIED); got != 0 {
		t.Fatalf("STATEMENT_KIND_UNSPECIFIED = %d, want 0", got)
	}
}

// TestStatementKindDocumentsTheSignedBinding keeps the wire comment honest:
// a reader of the .proto must be able to see that the field is signed.
func TestStatementKindDocumentsTheSignedBinding(t *testing.T) {
	fd := (&pb.StatementEnvelopeV2{}).ProtoReflect().Descriptor().Fields().ByName(protoreflectName("statement_kind"))
	if fd == nil {
		t.Fatal("StatementEnvelopeV2 is missing statement_kind")
	}
	comment := fd.ParentFile().SourceLocations().ByDescriptor(fd).LeadingComments
	if !strings.Contains(comment, "user_jws") {
		t.Fatalf("statement_kind leading comment must say it is bound by user_jws, got %q", comment)
	}
}
```

- [ ] **Step 2: Run it and watch the comment assertion fail**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter-proto && go test ./conformance/ -run TestStatementKind -v`
Expected: `TestStatementKindIsFieldTwoAndInsertIsOne` PASS, `TestStatementKindDocumentsTheSignedBinding` FAIL with `statement_kind leading comment must say it is bound by user_jws, got ""`.

- [ ] **Step 3: Add the comment in `proto/arbiter.proto`**

Replace the bare field line inside `message StatementEnvelopeV2`:

```proto
  StatementKind statement_kind = 2;
```

with:

```proto
  // Bound by user_jws from envelope v2.1 (arbiter-proto v0.6.0) onward: the
  // signed payload carries "statement_kind" as this enum's numeric value, so
  // an operator cannot re-label an INSERT as another kind once the mutation
  // lane ships. v1 still admits INSERT only.
  StatementKind statement_kind = 2;
```

- [ ] **Step 4: Regenerate and re-run**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter-proto && make tools && make proto && make lint && make breaking && go test ./conformance/ -run TestStatementKind -v`
Expected: `buf lint` silent; `buf breaking` silent (a comment is not a breaking change); both tests PASS. `git status` shows `proto/arbiter.proto` and `gen/pb/arbiter.pb.go` modified (the generated file carries the comment in its `rawDesc`).

- [ ] **Step 5: Full repo gate**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter-proto && make test`
Expected: `go build`, `go vet`, `go test ./...` all clean.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-proto
git add proto/arbiter.proto gen/pb/arbiter.pb.go conformance/statement_kind_binding_test.go
git commit -m "feat(proto): record statement_kind as a user_jws-bound field"
```

### Task 2: Tag arbiter-proto v0.6.0

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-proto`

**Files:** none (release only).

**Interfaces:**
- Produces: module version `github.com/sentioxyz/arbiter-proto v0.6.0`, consumed by Tasks 9, 10, 14, 23.

- [ ] **Step 1: Open the PR and merge it green**

Run: `gh pr create --fill --base main` then merge once `ci` is green.

- [ ] **Step 2: Cut the release**

Run: `gh workflow run cut-release.yml -f version=v0.6.0` (from the default branch), then `gh run watch`.
Expected: a non-draft, non-prerelease `v0.6.0` whose annotated tag peels to the merge commit from Step 1.

- [ ] **Step 3: Record the evidence**

Append the merge SHA, the run URL, and the tag object SHA to this plan's "Progress and Publication Evidence" section (create it directly under the plan header if it does not exist yet), in the same shape Spec A's plan uses.

---

## Phase 2 — housegate (spec D5, D6, D7 agent/ingress half)

### Task 3: `chproto` exposes the client Data block name

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/chproto/compress.go:15-54`
- Test: `pkg/chproto/compress_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `chproto.ClientDataInfo{BlockName string; Empty bool}` and `chproto.InspectClientDataPacket(raw []byte, compression proto.Compression) (ClientDataInfo, error)`. `chproto.ClientDataPacketIsEmpty` keeps its exact signature and behaviour (Task 4 and `relay.go:1068` both still call it).

- [ ] **Step 1: Write the failing tests**

Append to `pkg/chproto/compress_test.go`:

```go
// buildNamedClientDataPacket constructs a ClientData packet carrying an
// external temporary table: a non-empty block name with rows.
func buildNamedClientDataPacket(t *testing.T, name string, rows int) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString(name)
	values := make(proto.ColUInt64, rows)
	for i := range values {
		values[i] = uint64(i + 1)
	}
	block := proto.Block{Info: proto.BlockInfo{BucketNum: -1}, Columns: 1, Rows: rows}
	if err := block.EncodeBlock(&buf, 54453, []proto.InputColumn{{Name: "v", Data: &values}}); err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

// buildEmptyNamedClientDataPacket is the zero-row terminator of ONE external
// table: an empty block that still carries that table's name.
func buildEmptyNamedClientDataPacket(t *testing.T, name string) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString(name)
	(proto.BlockInfo{BucketNum: -1}).Encode(&buf)
	buf.PutUVarInt(0)
	buf.PutUVarInt(0)
	return append([]byte(nil), buf.Buf...)
}

func TestInspectClientDataPacketReportsBlockName(t *testing.T) {
	for _, tc := range []struct {
		name      string
		raw       []byte
		wantName  string
		wantEmpty bool
	}{
		{"unnamed terminator", buildEmptyUncompressedDataPacket(t), "", true},
		{"named external table rows", buildNamedClientDataPacket(t, "tmp_ext", 3), "tmp_ext", false},
		{"named external table terminator", buildEmptyNamedClientDataPacket(t, "tmp_ext"), "tmp_ext", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info, err := InspectClientDataPacket(tc.raw, proto.CompressionDisabled)
			if err != nil {
				t.Fatalf("InspectClientDataPacket: %v", err)
			}
			if info.BlockName != tc.wantName || info.Empty != tc.wantEmpty {
				t.Fatalf("info = %+v, want {BlockName:%q Empty:%v}", info, tc.wantName, tc.wantEmpty)
			}
		})
	}
}

func TestClientDataPacketIsEmptyStillIgnoresTheName(t *testing.T) {
	// The non-deferred input path (relay.go's ordinary INSERT loop) has always
	// terminated on any empty block. Keep that behaviour byte-for-byte; only
	// the deferred lane gets stricter.
	empty, err := ClientDataPacketIsEmpty(buildEmptyNamedClientDataPacket(t, "tmp_ext"), proto.CompressionDisabled)
	if err != nil {
		t.Fatalf("ClientDataPacketIsEmpty: %v", err)
	}
	if !empty {
		t.Fatal("ClientDataPacketIsEmpty must keep classifying a named empty block as empty")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //pkg/chproto:chproto_test --test_filter='TestInspectClientDataPacketReportsBlockName|TestClientDataPacketIsEmptyStillIgnoresTheName' --test_output=errors`
Expected: FAIL — `undefined: InspectClientDataPacket`.

- [ ] **Step 3: Implement**

In `pkg/chproto/compress.go`, replace the whole `ClientDataPacketIsEmpty` function (lines 15-54) with:

```go
// ClientDataInfo is what the framing of one client Data packet says about it:
// the block name (empty for the INSERT payload stream, non-empty for an
// external temporary table) and whether the block carries no columns and no
// rows. Callers that need to distinguish the payload stream from external
// tables must read BlockName; Empty alone cannot.
type ClientDataInfo struct {
	BlockName string
	Empty     bool
}

// InspectClientDataPacket decodes the framing of raw far enough to report the
// block name and emptiness without materialising any column data.
func InspectClientDataPacket(raw []byte, compression proto.Compression) (ClientDataInfo, error) {
	r := bytes.NewReader(raw)
	code, err := binary.ReadUvarint(r)
	if err != nil {
		return ClientDataInfo{}, fmt.Errorf("client data packet code: %w", err)
	}
	if code != uint64(proto.ClientCodeData) {
		return ClientDataInfo{}, fmt.Errorf("packet type %d is not ClientData", code)
	}
	nameLen, err := binary.ReadUvarint(r)
	if err != nil {
		return ClientDataInfo{}, fmt.Errorf("client data block name length: %w", err)
	}
	if nameLen > uint64(r.Len()) {
		return ClientDataInfo{}, fmt.Errorf("client data block name length %d exceeds remaining packet bytes %d", nameLen, r.Len())
	}
	name := make([]byte, nameLen)
	if _, err := io.ReadFull(r, name); err != nil {
		return ClientDataInfo{}, fmt.Errorf("client data block name: %w", err)
	}

	var body io.Reader = r
	if compression == proto.CompressionEnabled {
		body = compress.NewReader(r)
	}
	pr := proto.NewReader(body)
	if _, _, err := decodeBlockInfoCompat(pr); err != nil {
		return ClientDataInfo{}, fmt.Errorf("client data BlockInfo: %w", err)
	}
	columns, err := pr.UVarInt()
	if err != nil {
		return ClientDataInfo{}, fmt.Errorf("client data columns: %w", err)
	}
	rows, err := pr.UVarInt()
	if err != nil {
		return ClientDataInfo{}, fmt.Errorf("client data rows: %w", err)
	}
	return ClientDataInfo{BlockName: string(name), Empty: columns == 0 && rows == 0}, nil
}

// ClientDataPacketIsEmpty reports whether raw is a protocol-level empty
// ClientData block. It deliberately ignores the block name: the ordinary
// (non-deferred) input path terminates on any empty block, which is the
// pre-existing behaviour. The signed deferred lane uses
// InspectClientDataPacket instead so it can refuse external tables.
func ClientDataPacketIsEmpty(raw []byte, compression proto.Compression) (bool, error) {
	info, err := InspectClientDataPacket(raw, compression)
	if err != nil {
		return false, err
	}
	return info.Empty, nil
}
```

- [ ] **Step 4: Run to verify it passes**

Run: `bazel test //pkg/chproto:chproto_test --test_output=errors`
Expected: PASS (all `pkg/chproto` tests, including the three pre-existing `ClientDataPacketIsEmpty*` cases).

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/chproto/compress.go pkg/chproto/compress_test.go
git commit -m "feat(chproto): expose the client Data block name"
```

### Task 4: The deferred lane refuses external-table blocks (D5)

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/proxy/relay.go:1264-1298` (the `ClientDataCode` arm of `runDeferredInsert`)
- Test: `pkg/proxy/relay_deferred_test.go`

**Interfaces:**
- Consumes: `chproto.InspectClientDataPacket` / `chproto.ClientDataInfo` (Task 3).
- Produces: no new exported symbols. Behaviour: a client Data packet with a non-empty block name aborts the deferred INSERT with an Exception before any signing, releasing the payload buffer.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/proxy/relay_deferred_test.go`:

```go
// encodeNamedClientData is an external temporary table block: named, with rows.
func encodeNamedClientData(t *testing.T, name string) []byte {
	t.Helper()
	values := proto.ColUInt64{7, 8}
	input := proto.Input{{Name: "v", Data: &values}}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString(name)
	block := proto.Block{Rows: values.Rows(), Columns: len(input)}
	if err := block.EncodeBlock(&buf, deferredTestRev, input); err != nil {
		t.Fatalf("encode named client data block: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

// encodeEmptyNamedClientData is one external table's zero-row terminator.
func encodeEmptyNamedClientData(t *testing.T, name string) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString(name)
	(proto.BlockInfo{BucketNum: -1}).Encode(&buf)
	buf.PutUVarInt(0)
	buf.PutUVarInt(0)
	return append([]byte(nil), buf.Buf...)
}

// assertDeferredNamedBlockRejected drives the deferred lane up to the sample
// block, feeds it one named Data packet, and asserts the fail-closed shape:
// Exception naming external tables, nothing forwarded upstream, strict hooks
// never fired, abort + exactly one complete.
func assertDeferredNamedBlockRejected(t *testing.T, named []byte) {
	t.Helper()
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)

	upstreamBytes := make(chan int, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := h.upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	namedWrite := make(chan error, 1)
	go func() {
		_, err := h.clientProxy.Write(named)
		namedWrite <- err
	}()
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	exc, ok := pkt.Decoded.(*chproto.Exception)
	if !ok || !strings.Contains(exc.Message, "external table") {
		t.Fatalf("client got %#v, want an external-table Exception", pkt.Decoded)
	}
	h.close(t)
	<-namedWrite
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes, want 0", n)
	}
	strictData, strictComplete, _, queryCompletes, aborts := hooks.counts()
	if strictData != 0 || strictComplete != 0 {
		t.Fatalf("strict hooks fired (%d data, %d complete); a rejected block must never reach the signer", strictData, strictComplete)
	}
	if queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts complete/abort = %d/%d, want 1/1", queryCompletes, aborts)
	}
}

func TestRelay_DeferredInsert_RejectsNamedExternalTableBlock(t *testing.T) {
	assertDeferredNamedBlockRejected(t, encodeNamedClientData(t, "tmp_ext"))
}

func TestRelay_DeferredInsert_RejectsEmptyNamedExternalTableTerminator(t *testing.T) {
	// An EMPTY named block is the per-external-table terminator. Accepting it
	// as "the" payload terminator is exactly the confusion 1e describes, so it
	// is refused rather than consumed.
	assertDeferredNamedBlockRejected(t, encodeEmptyNamedClientData(t, "tmp_ext"))
}

func TestRelay_DeferredInsert_RejectsNamedBlockAfterPayload(t *testing.T) {
	// A named block arriving after payload bytes must not be folded into
	// payload_hash either.
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, encodeEmptyClientData(t))
	writeAllConn(t, h.clientProxy, nonEmpty)
	named := encodeNamedClientData(t, "tmp_ext")
	namedWrite := make(chan error, 1)
	go func() {
		_, err := h.clientProxy.Write(named)
		namedWrite <- err
	}()
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	exc, ok := pkt.Decoded.(*chproto.Exception)
	if !ok || !strings.Contains(exc.Message, "external table") {
		t.Fatalf("client got %#v, want an external-table Exception", pkt.Decoded)
	}
	h.close(t)
	<-namedWrite
	if _, strictComplete, _, _, aborts := hooks.counts(); strictComplete != 0 || aborts != 1 {
		t.Fatalf("strictComplete/abort = %d/%d, want 0/1 (nothing may be signed)", strictComplete, aborts)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_DeferredInsert_Rejects.*Named' --test_output=errors`
Expected: FAIL — the first two hang until the 2s read deadline then report `client read: i/o timeout` (today Relay silently folds the named block into the payload or consumes it as a terminator); the third fails on `client got <nil>`.

- [ ] **Step 3: Implement**

In `pkg/proxy/relay.go`, replace the body of the `case uint64(chproto.ClientDataCode):` arm (lines 1265-1298) with:

```go
		case uint64(chproto.ClientDataCode):
			info, err := chproto.InspectClientDataPacket(pkt.Raw, compression)
			if err != nil {
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return fmt.Errorf("classify deferred client data packet: %w", err)
			}
			if info.BlockName != "" {
				// A named block is external-temporary-table data (or that
				// table's own zero-row terminator). Folding it into
				// payload_hash would sign bytes the ingress never stores, and
				// consuming its terminator would strand the real payload, so
				// the signed lane refuses it outright.
				return rejectClose(fmt.Errorf(
					"deferred INSERT %q received external table block %q; external tables are not supported on the storage-integrity signed lane",
					q.ID, info.BlockName))
			}
			switch {
			case info.Empty && !sawPayload && initialEmptyRaw == nil:
				// Protocol state, never a timeout, identifies the first empty
				// packet as the external-tables marker. A standard zero-row
				// producer sends a second empty packet as its terminator.
				initialEmptyRaw = append([]byte(nil), pkt.Raw...)
			case info.Empty:
				terminatorRaw = append([]byte(nil), pkt.Raw...)
			default:
				// The client-side marker is optional. When absent, terminatorRaw is
				// later replayed once before the sample and once after payload.
				sawPayload = true
				if uint64(len(pkt.Raw)) > remaining {
					return rejectClose(fmt.Errorf("deferred INSERT %q payload exceeds limit (remaining %d bytes): %w", q.ID, remaining, chproto.ErrPacketTooLarge))
				}
				if err := r.hooks.OnClientDataStrict(ctx, qctx, pkt.Raw); err != nil {
					return rejectClose(fmt.Errorf("client data strict hook: %w", err))
				}
				if err := r.hooks.OnClientData(ctx, qctx, pkt.Raw); err != nil {
					logger.Warnw("client data hook failed (fail-open)", "raw_len", pkt.RawLen, "err", err)
				}
				bufferedBytes += uint64(len(pkt.Raw))
				if bufferedBytes > plan.MaxPayloadBytes {
					return rejectClose(fmt.Errorf("deferred INSERT %q payload exceeds limit of %d bytes", q.ID, plan.MaxPayloadBytes))
				}
				buffered = append(buffered, append([]byte(nil), pkt.Raw...))
			}
```

`rejectClose` already fires `OnQueryAbort` + `OnQueryComplete`, writes the Exception, and returns the error that closes the connection; `buffered` is a local slice so it is released with the frame.

- [ ] **Step 4: Run to verify it passes**

Run: `bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_DeferredInsert' --test_output=errors`
Expected: PASS — the three new tests plus every pre-existing `TestRelay_DeferredInsert_*` case (the happy path still sends unnamed marker + payload + unnamed terminator).

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/proxy/relay.go pkg/proxy/relay_deferred_test.go
git commit -m "fix(proxy): refuse external table blocks on the deferred INSERT lane"
```

### Task 5: `settings_hash` is enforced against the enumerated owned key set (D6)

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/storageintegrity/settings.go:20-38`
- Test: `pkg/storageintegrity/settings_test.go`; create `pkg/rewriter/read_mode_key_test.go`

**Interfaces:**
- Consumes: `auth.AuthTokenSettingKey`, `auth.StatementTokenSettingKey`, `auth.MaintenanceSettingKey`, `auth.PlatformOperatorSettingKey`, `auth.DriverSettingKey`, `auth.PayerSettingKey` (existing constants in `pkg/auth/types.go:26-35`).
- Produces: `sicore.ReadModeSettingKey`, `sicore.HousegateOwnedSettingKeys() []string`; `IsHousegateOwnedSettingKey` / `RejectUserSettings` keep their signatures and their (good) rejection message.

**Import-graph note.** `pkg/storageintegrity` gains an import of `pkg/auth`, which is acyclic (`pkg/auth` imports only `context` and `pkg/log`). It deliberately does **not** import `pkg/rewriter` for `ReadModeSettingKey` — that would pull grpc + the FFI engine into a pure-data package. Instead `sicore` declares its own `ReadModeSettingKey` and a test in `pkg/rewriter` (which already sits above both) asserts the two constants are equal, so drift is a red test rather than a silent divergence.

- [ ] **Step 1: Write the failing tests**

The two existing tests in `pkg/storageintegrity/settings_test.go` (`TestEmptySettingsHashIsTheCanonicalEmptySetDigest`, `TestRejectUserSettingsStripsHousegateOwnedKeys`) stay exactly as they are — every key they name is in the new owned set, so they keep passing. Append:

```go
func TestHousegateOwnedSettingKeysIsExactlyTheOwnedSet(t *testing.T) {
	want := []string{
		"SQL_sentio_driver",
		"SQL_sentio_maintenance",
		"SQL_sentio_platform_operator",
		"SQL_x_auth_token",
		"SQL_x_payer",
		"SQL_x_read_mode",
		"SQL_x_statement_token",
	}
	got := HousegateOwnedSettingKeys()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HousegateOwnedSettingKeys() = %v, want %v", got, want)
	}
	for _, key := range want {
		if !IsHousegateOwnedSettingKey(key) {
			t.Fatalf("%s must be owned", key)
		}
	}
}

func TestRejectUserSettingsRejectsUnknownPrefixedKeys(t *testing.T) {
	// 1f: the prefix escape hatch let a client attach arbitrary unsigned
	// settings that still reached ClickHouse. Membership closes it.
	for _, key := range []string{
		"SQL_x_whatever",
		"SQL_x_",
		"SQL_sentio_anything",
		"sql_x_auth_token", // case-sensitive: ClickHouse settings are
		"SQL_x_auth_token_2",
		"async_insert",
		"max_threads",
	} {
		err := RejectUserSettings([]string{key})
		if err == nil {
			t.Fatalf("%q must be rejected", key)
		}
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("rejection for %q must name the key, got %v", key, err)
		}
	}
}

func TestRejectUserSettingsAcceptsEveryOwnedKeyTogether(t *testing.T) {
	if err := RejectUserSettings(HousegateOwnedSettingKeys()); err != nil {
		t.Fatalf("the owned set must be admitted whole: %v", err)
	}
}

func TestOwnedSettingKeysTrackTheAuthConstants(t *testing.T) {
	// A rename in pkg/auth must not silently drop a key out of the owned set.
	for _, key := range []string{
		auth.AuthTokenSettingKey, auth.StatementTokenSettingKey, auth.PayerSettingKey,
		auth.DriverSettingKey, auth.MaintenanceSettingKey, auth.PlatformOperatorSettingKey,
	} {
		if !IsHousegateOwnedSettingKey(key) {
			t.Fatalf("pkg/auth constant %q is not in the owned set", key)
		}
	}
}
```

Add `reflect` and `"github.com/housegate/housegate/pkg/auth"` to that file's imports (`strings` and `testing` are already there).

Create `pkg/rewriter/read_mode_key_test.go`:

```go
package rewriter

import (
	"testing"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// TestReadModeSettingKeyMatchesStorageIntegrity keeps the two declarations of
// SQL_x_read_mode in lockstep. pkg/storageintegrity cannot import pkg/rewriter
// (that would pull grpc + the FFI engine into a pure-data package), so the
// equality is asserted from the package that already sits above both.
func TestReadModeSettingKeyMatchesStorageIntegrity(t *testing.T) {
	if ReadModeSettingKey != sicore.ReadModeSettingKey {
		t.Fatalf("rewriter.ReadModeSettingKey = %q, storageintegrity.ReadModeSettingKey = %q", ReadModeSettingKey, sicore.ReadModeSettingKey)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bazel run //:gazelle && bazel test //pkg/storageintegrity:storageintegrity_test //pkg/rewriter:rewriter_test --test_output=errors`
Expected: FAIL — `undefined: HousegateOwnedSettingKeys`, `undefined: sicore.ReadModeSettingKey`, and `TestRejectUserSettingsRejectsUnknownPrefixedKeys` accepting `SQL_x_whatever`.

- [ ] **Step 3: Implement**

Replace `pkg/storageintegrity/settings.go` lines 20-38 with:

```go
// ReadModeSettingKey is the per-query storage-integrity read-mode setting.
// It is declared here as well as in pkg/rewriter because this package must
// stay free of the rewriter's grpc/FFI dependencies; pkg/rewriter's
// read_mode_key_test.go asserts the two constants are equal.
const ReadModeSettingKey = "SQL_x_read_mode"

// housegateOwnedSettingKeys is the complete, enumerable set of ClickHouse
// query settings HouseGate itself attaches. Membership — not a SQL_x_ /
// SQL_sentio_ prefix — is what excludes a key from settings_hash: a prefix
// test let a client smuggle arbitrary unsigned settings through to ClickHouse
// while still hashing to the empty-settings digest (spec K 1f).
var housegateOwnedSettingKeys = map[string]bool{
	auth.AuthTokenSettingKey:         true,
	auth.StatementTokenSettingKey:    true,
	auth.PayerSettingKey:             true,
	auth.DriverSettingKey:            true,
	auth.MaintenanceSettingKey:       true,
	auth.PlatformOperatorSettingKey:  true,
	ReadModeSettingKey:               true,
}

// HousegateOwnedSettingKeys returns the owned key set in sorted order. It
// exists so tests and operator diagnostics can enumerate it without reaching
// into the map.
func HousegateOwnedSettingKeys() []string {
	keys := make([]string, 0, len(housegateOwnedSettingKeys))
	for key := range housegateOwnedSettingKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// IsHousegateOwnedSettingKey reports whether a ClickHouse query setting key is
// one HouseGate attaches itself and is therefore excluded from settings_hash.
func IsHousegateOwnedSettingKey(key string) bool {
	return housegateOwnedSettingKeys[key]
}
```

Change the import block of that file to:

```go
import (
	"fmt"
	"sort"

	"github.com/housegate/housegate/pkg/auth"
)
```

(`strings` is no longer used.) `RejectUserSettings` below is unchanged — its message quality is good and the spec says to keep it.

- [ ] **Step 4: Run to verify it passes**

Run: `bazel mod tidy && bazel run //:gazelle && bazel test //pkg/storageintegrity:storageintegrity_test //pkg/rewriter:rewriter_test //pkg/plugins/storageintegrity:storageintegrity_test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: PASS on all four. The two `RejectUserSettings` call sites (`pkg/plugins/storageintegrity/plugin.go:239,738` and `pkg/plugins/sistatement/plugin.go:143`) are unchanged and keep compiling.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/storageintegrity/settings.go pkg/storageintegrity/settings_test.go pkg/storageintegrity/BUILD.bazel pkg/rewriter/read_mode_key_test.go pkg/rewriter/BUILD.bazel
git commit -m "fix(storageintegrity): enforce settings_hash against the owned key set"
```

### Task 6: `statement_kind` enters the signed payload (D7, housegate core)

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/auth/types.go:153-205`
- Modify: `pkg/storageintegrity/intake.go` (append after the `Kind` block at ~:22-26)
- Test: `pkg/auth/statement_v2_test.go`, `pkg/storageintegrity/intake_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `auth.JWSStatementPayloadV2.StatementKind uint32` (json tag `statement_kind`, **appended last**), a `"statement_kind"` case appended to `auth.StatementPayloadV2Mismatch`, `sicore.StatementKindCodeInsert uint32 = 1`, `sicore.StatementKindCode(k Kind) (uint32, error)`.

**Why appended last.** The JWS payload is signed as `json.Marshal` bytes and verified by unmarshalling into the same struct, so field order does not affect *correctness* — but it does change every token's bytes. Appending keeps the diff to the shared vectors minimal and follows the same append-only discipline the envelope's v2 fields used. The `StatementPayloadV2Mismatch` case is appended for the same reason: it keeps the reported field for every pre-existing mismatch unchanged.

- [ ] **Step 1: Write the failing tests**

Append to `pkg/auth/statement_v2_test.go`:

```go
func TestStatementPayloadV2MismatchReportsStatementKind(t *testing.T) {
	want := statementV2Fixture("0x0000000000000000000000000000000000000001")
	want.Purpose = StatementPurposeV2
	got := want
	got.StatementKind = 7
	if field := StatementPayloadV2Mismatch(got, want); field != "statement_kind" {
		t.Fatalf("mismatch field = %q, want statement_kind", field)
	}
	got = want
	if field := StatementPayloadV2Mismatch(got, want); field != "" {
		t.Fatalf("identical payloads must not mismatch, got %q", field)
	}
}

func TestStatementKindIsSignedAndRoundTrips(t *testing.T) {
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	payload := statementV2Fixture(signer.Address())
	payload.StatementKind = 1
	token, err := signer.SignStatementV2(payload)
	if err != nil {
		t.Fatalf("SignStatementV2: %v", err)
	}
	decoded, err := DecodeStatementV2Payload(token)
	if err != nil {
		t.Fatalf("DecodeStatementV2Payload: %v", err)
	}
	if decoded.StatementKind != 1 {
		t.Fatalf("decoded statement_kind = %d, want 1", decoded.StatementKind)
	}
	validator := NewEthValidator([]string{signer.Address()}, 100*365*24*time.Hour, true, false, "", nil)
	want := payload
	want.StatementKind = 0 // an operator claiming a different kind
	if _, err := validator.ValidateStatementV2(token, want); err == nil {
		t.Fatal("a statement_kind the signer did not sign must be rejected")
	}
}
```

Also change `statementV2Fixture` (`pkg/auth/statement_v2_test.go:15-31`) to add `StatementKind: 1,` as its **last** field so every existing test in that file signs a kind.

Append to `pkg/storageintegrity/intake_test.go`:

```go
func TestStatementKindCode(t *testing.T) {
	code, err := StatementKindCode(KindInsert)
	if err != nil {
		t.Fatalf("StatementKindCode(KindInsert): %v", err)
	}
	if code != StatementKindCodeInsert || code != 1 {
		t.Fatalf("StatementKindCode(KindInsert) = %d, want 1", code)
	}
	if _, err := StatementKindCode(Kind("")); err == nil {
		t.Fatal("an empty kind must not resolve to a signed code")
	}
	if _, err := StatementKindCode(Kind("UPDATE")); err == nil {
		t.Fatal("an unmodelled kind must fail closed rather than sign 0")
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //pkg/auth:auth_test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: FAIL — `unknown field StatementKind in struct literal`, `undefined: StatementKindCode`.

- [ ] **Step 3: Implement**

In `pkg/auth/types.go`, add the field as the last member of `JWSStatementPayloadV2`:

```go
	TargetTableID  string `json:"target_table_id"`
	RowIDProfileID string `json:"row_id_profile_id"`
	// StatementKind is pb.StatementKind's numeric value (1 = INSERT). Bound
	// from envelope v2.1 so an operator cannot re-label a signed statement as
	// another kind once the mutation lane ships. Appended last: the field set
	// grows, the existing field order does not move.
	StatementKind uint32 `json:"statement_kind"`
}
```

and append the case to `StatementPayloadV2Mismatch`, after `row_id_profile_id`:

```go
	case got.RowIDProfileID != want.RowIDProfileID:
		return "row_id_profile_id"
	case got.StatementKind != want.StatementKind:
		return "statement_kind"
	}
```

In `pkg/storageintegrity/intake.go`, directly under the `KindInsert` const block:

```go
// StatementKindCodeInsert mirrors pb.StatementKind_STATEMENT_KIND_INSERT. The
// signed statement payload carries the numeric enum value, and this package
// must not import arbiter-proto, so the number is pinned here and asserted
// against the generated enum by the ingress plugin's tests.
const StatementKindCodeInsert uint32 = 1

// StatementKindCode maps an admitted Kind onto the numeric value the signed
// payload binds. An unmodelled kind fails closed rather than signing 0, which
// is STATEMENT_KIND_UNSPECIFIED and would be admitted by no verifier.
func StatementKindCode(k Kind) (uint32, error) {
	switch k {
	case KindInsert:
		return StatementKindCodeInsert, nil
	default:
		return 0, fmt.Errorf("storageintegrity: statement kind %q has no signed code", k)
	}
}
```

(`fmt` is already imported by `intake.go`.)

- [ ] **Step 4: Run to verify it passes**

Run: `bazel test //pkg/auth:auth_test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: `//pkg/storageintegrity:storageintegrity_test` PASS. `//pkg/auth:auth_test` FAILs **only** on `TestStatementV2Vectors` (the committed vectors still carry no `statement_kind`, so `valid` now mismatches) — that is Task 7's starting red.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/auth/types.go pkg/auth/statement_v2_test.go pkg/storageintegrity/intake.go pkg/storageintegrity/intake_test.go
git commit -m "feat(auth): bind statement_kind in the v2 statement payload"
```

### Task 7: Regenerate the shared JWS vectors and strengthen the reject assertions

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/auth/statement_v2_vectors_test.go`
- Regenerate: `pkg/auth/testdata/statement_jws_v2.json`
- Create: `pkg/auth/shared_vectors.go`

**Interfaces:**
- Consumes: `auth.JWSStatementPayloadV2.StatementKind` (Task 6).
- Produces: the 17-vector file consumed by arbiter Task 19, and `auth.SharedStatementVectorsSHA256` — the build-time link between the two copies.

**The build-time link (spec asks for a decision, here it is).** Today the two files are byte-identical by hand; nothing detects drift. Options considered: (a) a Bazel `filegroup` in `pkg/auth` that arbiter's `fsm_test` consumes as `data` — a real link, but it needs `# keep`-protected hand edits in two gazelle-generated BUILD files and it does not work under plain `go test`; (b) publishing the file's SHA-256 as an **exported constant in `pkg/auth`**, which arbiter already imports. Decision: **(b)**. It works under both build systems with no BUILD surgery, and it goes red exactly when arbiter bumps its housegate pin — which is the coordinated-release moment D7 already requires. Residual gap, accepted and recorded: arbiter does not notice housegate-side regeneration until it bumps the pin.

- [ ] **Step 1: Rewrite the generator and the consumer**

Replace `pkg/auth/statement_v2_vectors_test.go` in full (the SHA in Step 4 depends on this being byte-exact):

```go
package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type statementV2Vector struct {
	Name         string                `json:"name"`
	Expect       string                `json:"expect"`                  // "accept" | "reject"
	RejectReason string                `json:"reject_reason,omitempty"` // "binding" | "purpose" | "signature" | "malformed"
	RejectField  string                `json:"reject_field,omitempty"`  // set iff reject_reason == "binding"
	Payload      JWSStatementPayloadV2 `json:"payload"`
	Token        string                `json:"token"`
}

type statementV2VectorFile struct {
	SignerPrivateKeyHex string              `json:"signer_private_key_hex"`
	SignerAddress       string              `json:"signer_address"`
	Vectors             []statementV2Vector `json:"vectors"`
}

const statementV2VectorPath = "testdata/statement_jws_v2.json"

// TestGenerateStatementV2Vectors rewrites the shared vector file when
// HOUSEGATE_WRITE_VECTORS=1. The file is consumed by this package and copied
// verbatim into arbiter fsm/testdata so both verifiers prove the same set.
func TestGenerateStatementV2Vectors(t *testing.T) {
	if os.Getenv("HOUSEGATE_WRITE_VECTORS") != "1" {
		t.Skip("set HOUSEGATE_WRITE_VECTORS=1 to regenerate testdata/statement_jws_v2.json")
	}
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	other, err := NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	valid := statementV2Fixture(signer.Address()) // Iat fixed at 1755000000
	valid.Purpose = StatementPurposeV2
	validToken, err := signer.SignStatementV2(valid)
	if err != nil {
		t.Fatalf("SignStatementV2: %v", err)
	}
	wrongSignerToken, err := other.SignStatementV2(valid)
	if err != nil {
		t.Fatalf("SignStatementV2(other): %v", err)
	}
	legacyToken, err := signer.SignToken("INSERT INTO db.table FORMAT Native")
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	mutate := func(field string, f func(*JWSStatementPayloadV2)) statementV2Vector {
		p := valid
		f(&p)
		return statementV2Vector{Name: field + "_mismatch", Expect: "reject", RejectReason: "binding", RejectField: field, Payload: p, Token: validToken}
	}
	file := statementV2VectorFile{
		SignerPrivateKeyHex: statementV2TestKey,
		SignerAddress:       signer.Address(),
		Vectors: []statementV2Vector{
			{Name: "valid", Expect: "accept", Payload: valid, Token: validToken},
			mutate("network_id", func(p *JWSStatementPayloadV2) { p.NetworkID = "other-net" }),
			mutate("keeper_shard_id", func(p *JWSStatementPayloadV2) { p.KeeperShardID = 1 }),
			mutate("statement_id", func(p *JWSStatementPayloadV2) { p.StatementID = signer.Address() + ":2:n1" }),
			mutate("sql_hash", func(p *JWSStatementPayloadV2) { p.SQLHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("settings_hash", func(p *JWSStatementPayloadV2) { p.SettingsHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("schema_hash", func(p *JWSStatementPayloadV2) { p.SchemaHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("payload_hash", func(p *JWSStatementPayloadV2) { p.PayloadHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("payload_length", func(p *JWSStatementPayloadV2) { p.PayloadLength = 1 }),
			mutate("payload_format", func(p *JWSStatementPayloadV2) { p.PayloadFormat = "csv-with-names-v1" }),
			mutate("client_revision", func(p *JWSStatementPayloadV2) { p.ClientRevision = 54470 }),
			mutate("target_table_id", func(p *JWSStatementPayloadV2) { p.TargetTableID = "db.other" }),
			mutate("row_id_profile_id", func(p *JWSStatementPayloadV2) { p.RowIDProfileID = "housegate-row-id-v0" }),
			mutate("statement_kind", func(p *JWSStatementPayloadV2) { p.StatementKind = 0 }),
			{Name: "wrong_signer", Expect: "reject", RejectReason: "signature", Payload: valid, Token: wrongSignerToken},
			{Name: "legacy_query_token", Expect: "reject", RejectReason: "purpose", Payload: valid, Token: legacyToken},
			{Name: "garbage_token", Expect: "reject", RejectReason: "malformed", Payload: valid, Token: "not.a.jws"},
		},
	}
	b, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(statementV2VectorPath), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(statementV2VectorPath, append(b, '\n'), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}

// TestStatementV2Vectors proves the committed vectors against this package's
// validator. Every reject vector asserts WHY it was rejected — the field name
// for a binding failure, the class otherwise — so a reordered
// StatementPayloadV2Mismatch or a collapsed error taxonomy is a red test
// rather than a still-green "some error happened".
func TestStatementV2Vectors(t *testing.T) {
	raw, err := os.ReadFile(statementV2VectorPath)
	if err != nil {
		t.Fatalf("read vectors (run TestGenerateStatementV2Vectors with HOUSEGATE_WRITE_VECTORS=1 first): %v", err)
	}
	var file statementV2VectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatalf("decode vectors: %v", err)
	}
	validator := NewEthValidator([]string{file.SignerAddress}, 100*365*24*time.Hour, true, false, "", nil)
	if len(file.Vectors) != 17 {
		t.Fatalf("expected exactly 17 vectors, got %d", len(file.Vectors))
	}
	seenBindingFields := map[string]bool{}
	for _, vec := range file.Vectors {
		t.Run(vec.Name, func(t *testing.T) {
			addr, err := validator.ValidateStatementV2(vec.Token, vec.Payload)
			switch vec.Expect {
			case "accept":
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				if addr != file.SignerAddress {
					t.Fatalf("recovered %s, want %s", addr, file.SignerAddress)
				}
				return
			case "reject":
				if err == nil {
					t.Fatal("expected reject, got accept")
				}
			default:
				t.Fatalf("unknown expect %q", vec.Expect)
			}
			switch vec.RejectReason {
			case "binding":
				seenBindingFields[vec.RejectField] = true
				want := "statement token binding mismatch on " + vec.RejectField
				if err.Error() != want {
					t.Fatalf("error = %q, want exactly %q", err.Error(), want)
				}
			case "purpose":
				if !strings.Contains(err.Error(), "statement token purpose mismatch") {
					t.Fatalf("error = %q, want a purpose mismatch", err.Error())
				}
			case "signature":
				if !strings.Contains(err.Error(), "not in allowlist") && !strings.Contains(err.Error(), "signature verification failed") {
					t.Fatalf("error = %q, want a signer failure", err.Error())
				}
			case "malformed":
				if !strings.Contains(err.Error(), "invalid statement") {
					t.Fatalf("error = %q, want a malformed-token failure", err.Error())
				}
			default:
				t.Fatalf("vector %s has no reject_reason", vec.Name)
			}
		})
	}
	// Every bound field must have its own vector: adding a field to
	// JWSStatementPayloadV2 without a vector is what let statement_kind sit
	// unbound in the first place.
	for _, field := range []string{
		"network_id", "keeper_shard_id", "statement_id", "sql_hash", "settings_hash",
		"schema_hash", "payload_hash", "payload_length", "payload_format",
		"client_revision", "target_table_id", "row_id_profile_id", "statement_kind",
	} {
		if !seenBindingFields[field] {
			t.Fatalf("no reject vector covers bound field %q", field)
		}
	}
}

// TestSharedStatementVectorsSHA256 is the cross-repo link: arbiter asserts its
// copy of testdata/statement_jws_v2.json hashes to the same exported constant.
func TestSharedStatementVectorsSHA256(t *testing.T) {
	raw, err := os.ReadFile(statementV2VectorPath)
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != SharedStatementVectorsSHA256 {
		t.Fatalf("statement_jws_v2.json sha256 = %s, SharedStatementVectorsSHA256 = %s\n"+
			"regenerating the vectors is a coordinated wire change: update the constant, copy the file into arbiter fsm/testdata, and cut both releases together", got, SharedStatementVectorsSHA256)
	}
}
```

- [ ] **Step 2: Add the exported link constant**

Create `pkg/auth/shared_vectors.go`:

```go
package auth

// SharedStatementVectorsSHA256 is the SHA-256 of
// pkg/auth/testdata/statement_jws_v2.json — the statement-JWS conformance
// vectors this repo produces and the Arbiter FSM consumes verbatim.
//
// It is exported so the Arbiter, which already imports this package, can
// assert its committed copy is byte-identical without a Bazel filegroup or a
// module-cache lookup. Regenerating the vectors is therefore a coordinated
// wire change by construction: the constant, the two copies of the file, and
// the two releases move together or the downstream repo goes red the moment it
// bumps its housegate pin.
const SharedStatementVectorsSHA256 = "6af5c9cc34d6b083935d804799138e059ce7da99fb034a2e0332b3c7ce8737bc"
```

- [ ] **Step 3: Regenerate the vector file**

Run: `cd /Users/uranuswch/Dev/housegate/housegate/pkg/auth && HOUSEGATE_WRITE_VECTORS=1 go test . -run TestGenerateStatementV2Vectors -count=1`
Expected: `ok`.

- [ ] **Step 4: Verify the digest reproduces exactly**

Run: `cd /Users/uranuswch/Dev/housegate/housegate && shasum -a 256 pkg/auth/testdata/statement_jws_v2.json && wc -c pkg/auth/testdata/statement_jws_v2.json`
Expected, byte for byte:
```
6af5c9cc34d6b083935d804799138e059ce7da99fb034a2e0332b3c7ce8737bc  pkg/auth/testdata/statement_jws_v2.json
   33120 pkg/auth/testdata/statement_jws_v2.json
```
If the digest differs, the generator above was not reproduced exactly — diff it rather than editing the constant. (The `valid` payload must end with `"row_id_profile_id": "housegate-row-id-v1", "statement_kind": 1` and the file must hold exactly 17 vectors.)

- [ ] **Step 5: Run the consumer**

Run: `bazel run //:gazelle && bazel test //pkg/auth:auth_test --test_output=errors`
Expected: PASS with 17 `TestStatementV2Vectors` subtests plus `TestSharedStatementVectorsSHA256`. Spot-check the reject messages match what the plan asserts:

```
network_id_mismatch        statement token binding mismatch on network_id
statement_kind_mismatch    statement token binding mismatch on statement_kind
wrong_signer               statement signer 0x88f9b82462f6c4bf4a0fb15e5c3971559a316e7f not in allowlist
legacy_query_token         statement token purpose mismatch: expected "housegate-statement-v2", got "housegate-query"
garbage_token              invalid statement header encoding: invalid statement header canonical base64url encoding: illegal base64 data at input byte 2
```

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/auth/statement_v2_vectors_test.go pkg/auth/shared_vectors.go pkg/auth/testdata/statement_jws_v2.json pkg/auth/BUILD.bazel
git commit -m "test(auth): regenerate shared JWS vectors with statement_kind and assert the failing field"
```

### Task 8: Agent and ingress bind `statement_kind`

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/plugins/sistatement/plugin.go:317-330`
- Modify: `pkg/plugins/storageintegrity/plugin.go:463-476`
- Test: `pkg/plugins/sistatement/plugin_test.go`, `pkg/plugins/storageintegrity/plugin_test.go`

**Interfaces:**
- Consumes: `auth.JWSStatementPayloadV2.StatementKind`, `sicore.StatementKindCodeInsert`, `sicore.StatementKindCode` (Task 6).
- Produces: no new symbols. Both sides now put `statement_kind` in the payload they sign / expect.

- [ ] **Step 1: Write the failing tests**

In `pkg/plugins/sistatement/plugin_test.go`, add `StatementKind: sicore.StatementKindCodeInsert,` as the last field of the `want` literal inside `TestPlugin_HappyPathSignsStatementTokenAfterPayload` (~:170-186), and append:

```go
// TestPlugin_SignsInsertKind decodes the token the agent appended and asserts
// the kind is signed, not left at STATEMENT_KIND_UNSPECIFIED.
func TestPlugin_SignsInsertKind(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, _ := newTestPlugin(t, ns, t.TempDir())
	qctx := insertQctx(newSession(9, ""), "INSERT INTO shop.orders FORMAT Native")

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, encodeRows(t)); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	var token string
	for _, s := range qctx.Query.Settings {
		if s.Key == auth.StatementTokenSettingKey {
			token = strings.Trim(s.Value, "'")
		}
	}
	if token == "" {
		t.Fatal("SQL_x_statement_token missing")
	}
	payload, err := auth.DecodeStatementV2Payload(token)
	if err != nil {
		t.Fatalf("DecodeStatementV2Payload: %v", err)
	}
	if payload.StatementKind != sicore.StatementKindCodeInsert {
		t.Fatalf("signed statement_kind = %d, want %d", payload.StatementKind, sicore.StatementKindCodeInsert)
	}
}
```

In `pkg/plugins/storageintegrity/plugin_test.go`, add `StatementKind: sicore.StatementKindCodeInsert,` as the last field of the literal `v2Statement` returns (~:995-1001), and append:

```go
// TestIngressV2_BindsTheKindItClassifiedItself proves the ingress derives
// statement_kind from its OWN classification of the SQL — a token signed with
// any other kind is refused before the payload is uploaded.
func TestIngressV2_BindsTheKindItClassifiedItself(t *testing.T) {
	p, signer, schemaHash := newV2Ingress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}

	qctx := signedQueryContext(t, 41, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{IsStorageIntegrity: true, OriginalDatabase: "tenant", OriginalTable: "events"}}
	unspecified := v2Statement(signer, qctx.Query.ID, sql, schemaHash, payload, 54453)
	unspecified.StatementKind = 0
	withStatementToken(t, qctx, signer, unspecified)

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	_, err := p.ConsumeAdmission(qctx.Session.ID())
	if err == nil || !strings.Contains(err.Error(), "statement_kind") {
		t.Fatalf("ConsumeAdmission err = %v, want a statement_kind binding rejection", err)
	}
}

// TestStatementKindCodeMatchesGeneratedEnum keeps sicore's hand-pinned number
// equal to the wire enum without pkg/storageintegrity importing arbiter-proto
// in a test of its own.
func TestStatementKindCodeMatchesGeneratedEnum(t *testing.T) {
	code, err := sicore.StatementKindCode(sicore.KindInsert)
	if err != nil {
		t.Fatalf("StatementKindCode: %v", err)
	}
	if code != uint32(pb.StatementKind_STATEMENT_KIND_INSERT) {
		t.Fatalf("sicore kind code %d does not match pb.StatementKind_STATEMENT_KIND_INSERT %d", code, pb.StatementKind_STATEMENT_KIND_INSERT)
	}
}
```

Add `pb "github.com/sentioxyz/arbiter-proto/gen/pb"` to that test file's imports (the module is already a dependency through `pkg/storageintegrity`; `bazel run //:gazelle` wires the test rule's dep).

- [ ] **Step 2: Run to verify it fails**

Run: `bazel test //pkg/plugins/sistatement:sistatement_test //pkg/plugins/storageintegrity:storageintegrity_test --test_output=errors`
Expected: FAIL — `TestPlugin_HappyPathSignsStatementTokenAfterPayload` and `TestPlugin_SignsInsertKind` report `signed statement_kind = 0, want 1`; `TestIngressV2_BindsTheKindItClassifiedItself` reports `ConsumeAdmission err = <nil>` (today the ingress does not look at the kind at all); `TestIngressV2_AcceptsTokenBoundToItsOwnCapture` reports a `statement_kind` binding mismatch because `v2Statement` now signs 1 while the ingress still expects 0.

- [ ] **Step 3: Implement**

In `pkg/plugins/sistatement/plugin.go`, inside `OnQueryInputCompleteStrict`, replace the `SignStatementV2` call's argument tail:

```go
		TargetTableID:  st.tableID,
		RowIDProfileID: payloadexec.RowIDProfileID,
		StatementKind:  sicore.StatementKindCodeInsert,
	})
```

The agent's SI lane is INSERT-only by construction (`sicore.InsertPayloadEncoding` gates entry in `OnQuery`), so the constant is correct and no lookup is needed.

In `pkg/plugins/storageintegrity/plugin.go`, inside `admissionFromState`, resolve the kind from the admission's own classification before building `want`:

```go
	kindCode, err := sicore.StatementKindCode(sicore.Kind(admission.Kind))
	if err != nil {
		return Admission{}, fmt.Errorf("storage_integrity admission %s: %w", admission.StatementID, err)
	}
	want := auth.JWSStatementPayloadV2{
		NetworkID:      p.networkID,
		KeeperShardID:  0,
		StatementID:    admission.StatementID,
		SQLHash:        admission.SQLHash,
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     state.schemaHash,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: uint32(state.revision),
		TargetTableID:  admission.TableID,
		RowIDProfileID: payloadexec.RowIDProfileID,
		StatementKind:  kindCode,
	}
```

(`admission.Kind` is the plugin package's `Kind`; `sicore.Kind` has the same underlying string values by design — see the comment on `sicore.Kind` in `pkg/storageintegrity/intake.go:18-24`.)

- [ ] **Step 4: Run to verify it passes**

Run: `bazel test //pkg/plugins/sistatement:sistatement_test //pkg/plugins/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add pkg/plugins/sistatement/plugin.go pkg/plugins/sistatement/plugin_test.go pkg/plugins/storageintegrity/plugin.go pkg/plugins/storageintegrity/plugin_test.go
git commit -m "feat(storageintegrity): sign and verify statement_kind on the SI lane"
```

### Task 9: housegate docs, arbiter-proto re-pin, full suite, tag v0.10.0

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `CLAUDE.md`, `go.mod`, `go.sum`, `MODULE.bazel`

**Interfaces:**
- Consumes: arbiter-proto `v0.6.0` (Task 2).
- Produces: housegate `v0.10.0`, consumed by Tasks 10, 14, 23.

- [ ] **Step 1: Re-pin arbiter-proto**

Run:
```bash
cd /Users/uranuswch/Dev/housegate/housegate
go get github.com/sentioxyz/arbiter-proto@v0.6.0
go mod tidy
bazel mod tidy && bazel run //:gazelle
```
Expected: `go.mod` shows `github.com/sentioxyz/arbiter-proto v0.6.0`; `MODULE.bazel` updated by `bazel mod tidy`.

- [ ] **Step 2: Update CLAUDE.md**

In the `pkg/plugins/` bullet, change the `storageintegrity` sentence so it reads (replace the existing "server-mode ingress validates the v2 statement token against …" clause):

```
`storageintegrity` (server-mode ingress validates the v2 statement token against its own exact Native-byte capture, the network-state `schema_hash`, `settings_hash == EmptySettingsHash`, and the `statement_kind` it classified itself; …
```

In the `pkg/storageintegrity/` bullet, append:

```
`settings.go` enforces `settings_hash` against the **enumerated** owned key set (`SQL_x_auth_token`, `SQL_x_statement_token`, `SQL_x_payer`, `SQL_x_read_mode`, `SQL_sentio_driver`, `SQL_sentio_maintenance`, `SQL_sentio_platform_operator`) — not a `SQL_x_` / `SQL_sentio_` prefix, which let unsigned client settings through to ClickHouse. `pkg/rewriter`'s `read_mode_key_test.go` keeps the duplicated `SQL_x_read_mode` constant in lockstep. `auth.SharedStatementVectorsSHA256` pins `pkg/auth/testdata/statement_jws_v2.json`; the Arbiter asserts its verbatim copy against the same constant, so regenerating the vectors is a coordinated wire change.
```

In the **Packet-level streaming pipeline** section, after the `ClientData packets additionally fire the OnClientData chain` sentence, add:

```
On the deferred-INSERT lane a client `Data` packet whose block **name** is non-empty is an external temporary table and is refused with an Exception before any signing — `chproto.InspectClientDataPacket` reports the name; `ClientDataPacketIsEmpty` keeps ignoring it for the ordinary input path.
```

- [ ] **Step 3: Full Bazel suite**

Run: `bazel test //... --test_output=errors`
Expected: PASS. Docker-bound `//pkg/integration:*` targets are tagged `manual` and are not selected by `//...`.

- [ ] **Step 4: Race + the docker integration suite**

Run:
```bash
bazel test --@rules_go//go/config:race //pkg/proxy:proxy_test //pkg/auth:auth_test //pkg/plugins/sistatement:sistatement_test --test_output=errors
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_output=errors
```
Expected: PASS. If an integration test fails, diff the failing set against a clean `main` build before calling it a regression (CLAUDE.md's main-baseline rule).

- [ ] **Step 5: Commit, merge, release**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add CLAUDE.md go.mod go.sum MODULE.bazel
git commit -m "chore: pin arbiter-proto v0.6.0 and document the commitment-durability changes"
gh pr create --fill --base main
```
Merge once Build, Integration, and Release-tooling are green, then run the official release workflow for `v0.10.0` and record the merge SHA, run URL, and tag object SHA in the plan's evidence section.

---

## Phase 3 — arbiter-core (spec D1 mirror, D4)

### Task 10: Point arbiter-core at housegate v0.10.0 and arbiter-proto v0.6.0

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`

**Interfaces:**
- Consumes: housegate `v0.10.0` (Task 9), arbiter-proto `v0.6.0` (Task 2).
- Produces: a tree where `auth.JWSStatementPayloadV2` has `StatementKind` and `sicore.HousegateOwnedSettingKeys` exists. This closes the roadmap §5 "arbiter-core is 7 commits behind housegate v0.9.0" debt.

- [ ] **Step 1: Bump both pins**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
bash scripts/update-housegate.sh v0.10.0 2>/dev/null || { go get github.com/housegate/housegate@v0.10.0 && go mod tidy; }
go get github.com/sentioxyz/arbiter-proto@v0.6.0
go mod tidy
```
arbiter-core has no `update-housegate.sh` of its own (that script lives in arbiter); if the fallback runs, also update the `housegate` `git_override` commit in `MODULE.bazel` to the `v0.10.0` tag's commit:
```bash
git ls-remote https://github.com/housegate/housegate refs/tags/v0.10.0^{}
```
Expected: `go.mod` shows `github.com/housegate/housegate v0.10.0` and `github.com/sentioxyz/arbiter-proto v0.6.0`; `MODULE.bazel`'s `housegate` override points at the peeled tag commit.

- [ ] **Step 2: Prove the tree still builds and tests green**

Run: `bazel build //... && bazel test --build_tests_only //... --test_output=errors && go test ./... 2>&1 | tail -20`
Expected: PASS. Nothing in arbiter-core reads `JWSStatementPayloadV2` directly, so the new field is additive here.

- [ ] **Step 3: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git add go.mod go.sum MODULE.bazel
git commit -m "chore: pin housegate v0.10.0 and arbiter-proto v0.6.0"
```

### Task 11: Freeze the `StatementEnvelope` preimage in its own repo (D1 mirror)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Create: `conformance/statement_envelope_golden_test.go`
- Create: `conformance/testdata/statement_envelope_golden.json`
- Modify: `conformance/BUILD.bazel` (gazelle adds `data = glob(["testdata/**"])`; add it by hand if it does not)

**Interfaces:**
- Consumes: nothing.
- Produces: the frozen `statements_root` value `0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921` that arbiter Task 15 asserts against the identical fixture.

**Why here as well as in arbiter.** `arbiter.StatementEnvelope` lives in this module; arbiter consumes it as a pinned dependency. A reorder here is green in this repo and only turns red in arbiter after somebody bumps the pin — potentially a release later. The mirror makes the reorder fail in the repo where the edit happens.

**Why no regenerator.** The digest is a Go source constant and the testdata file carries the same digest; both must be edited by hand to re-bless a change. A `-write` mode would let a routine "regenerate the goldens" step silently launder an encoding break, which is the exact failure this task exists to prevent (and the self-mutating-golden pattern Spec J is removing elsewhere).

- [ ] **Step 1: Commit the fixture**

Create `conformance/testdata/statement_envelope_golden.json` exactly:

```json
{
  "hash_profile": "housegate-replay-mvp-v0",
  "domain": "arbiter-l3-statements-v1",
  "digest": "0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921",
  "value": [
    {
      "statement_id": {
        "client_account": "0x00000000000000000000000000000000000000a1",
        "client_seq": 11,
        "client_nonce": "00112233445566778899aabbccddeeff"
      },
      "statement_kind": 1,
      "sql": "INSERT INTO db1.events FORMAT Native",
      "sql_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
      "settings_hash": "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
      "payload_ref": "payload/golden-1",
      "payload_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
      "payload_length": 4096,
      "target_table_id": "db1.events",
      "user_jws": "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjF9.Z29sZGVuLXNpZ25hdHVyZS0x",
      "envelope_version": 2,
      "network_id": "arbiter-golden-net",
      "keeper_shard_id": 0,
      "payload_format": "clickhouse-native-data-v1",
      "client_revision": 54460,
      "schema_hash": "0x3333333333333333333333333333333333333333333333333333333333333333",
      "row_id_profile_id": "housegate-row-id-v1"
    },
    {
      "statement_id": {
        "client_account": "0x00000000000000000000000000000000000000b2",
        "client_seq": 12,
        "client_nonce": "ffeeddccbbaa99887766554433221100"
      },
      "statement_kind": 1,
      "sql": "INSERT INTO db1.metrics FORMAT Native",
      "sql_hash": "0x4444444444444444444444444444444444444444444444444444444444444444",
      "settings_hash": "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
      "payload_ref": "payload/golden-2",
      "payload_hash": "0x5555555555555555555555555555555555555555555555555555555555555555",
      "payload_length": 8192,
      "target_table_id": "db1.metrics",
      "user_jws": "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjJ9.Z29sZGVuLXNpZ25hdHVyZS0y",
      "envelope_version": 2,
      "network_id": "arbiter-golden-net",
      "keeper_shard_id": 0,
      "payload_format": "clickhouse-native-data-v1",
      "client_revision": 54460,
      "schema_hash": "0x6666666666666666666666666666666666666666666666666666666666666666",
      "row_id_profile_id": "housegate-row-id-v1"
    }
  ]
}
```

Both envelopes are fully populated on purpose: `settings_hash`, `payload_ref`, `payload_hash`, `payload_length` and `target_table_id` carry `omitempty`, and an absent field cannot witness a reorder.

- [ ] **Step 2: Write the failing test**

Create `conformance/statement_envelope_golden_test.go`:

```go
package conformance

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// goldenStatementsRoot freezes CanonicalDigest(DomainL3Statements, envelopes)
// for the fixture below. CanonicalDigest is SHA-256 over json.Marshal, and
// encoding/json emits struct fields in DECLARATION order, so this constant is
// a function of arbiter.StatementEnvelope's field order as well as its field
// set. Every historical statements_root — hence every historical L3 ChainHash,
// hence every anchored value — depends on that order, so it is a consensus
// parameter and changing it is a versioned migration, never a refactor.
//
// If this test fails: do NOT paste the new digest in. Revert the struct change.
const goldenStatementsRoot = "0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921"

type statementEnvelopeGolden struct {
	HashProfile string          `json:"hash_profile"`
	Domain      string          `json:"domain"`
	Digest      string          `json:"digest"`
	Value       json.RawMessage `json:"value"`
}

// goldenEnvelopes is the fixture as Go values. It must stay in lockstep with
// testdata/statement_envelope_golden.json; the test compares the marshalled
// bytes so a diff of that file shows WHAT changed, not only that a hash moved.
func goldenEnvelopes() []arbiter.StatementEnvelope {
	return []arbiter.StatementEnvelope{
		{
			StatementID: arbiter.StatementID{
				ClientAccount: "0x00000000000000000000000000000000000000a1",
				ClientSeq:     11,
				ClientNonce:   "00112233445566778899aabbccddeeff",
			},
			StatementKind:   arbiter.StatementKindInsert,
			SQL:             "INSERT INTO db1.events FORMAT Native",
			SQLHash:         "0x1111111111111111111111111111111111111111111111111111111111111111",
			SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
			PayloadRef:      "payload/golden-1",
			PayloadHash:     "0x2222222222222222222222222222222222222222222222222222222222222222",
			PayloadLength:   4096,
			TargetTableID:   "db1.events",
			UserJWS:         "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjF9.Z29sZGVuLXNpZ25hdHVyZS0x",
			EnvelopeVersion: 2,
			NetworkID:       "arbiter-golden-net",
			KeeperShardID:   0,
			PayloadFormat:   "clickhouse-native-data-v1",
			ClientRevision:  54460,
			SchemaHash:      "0x3333333333333333333333333333333333333333333333333333333333333333",
			RowIDProfileID:  "housegate-row-id-v1",
		},
		{
			StatementID: arbiter.StatementID{
				ClientAccount: "0x00000000000000000000000000000000000000b2",
				ClientSeq:     12,
				ClientNonce:   "ffeeddccbbaa99887766554433221100",
			},
			StatementKind:   arbiter.StatementKindInsert,
			SQL:             "INSERT INTO db1.metrics FORMAT Native",
			SQLHash:         "0x4444444444444444444444444444444444444444444444444444444444444444",
			SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
			PayloadRef:      "payload/golden-2",
			PayloadHash:     "0x5555555555555555555555555555555555555555555555555555555555555555",
			PayloadLength:   8192,
			TargetTableID:   "db1.metrics",
			UserJWS:         "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjJ9.Z29sZGVuLXNpZ25hdHVyZS0y",
			EnvelopeVersion: 2,
			NetworkID:       "arbiter-golden-net",
			KeeperShardID:   0,
			PayloadFormat:   "clickhouse-native-data-v1",
			ClientRevision:  54460,
			SchemaHash:      "0x6666666666666666666666666666666666666666666666666666666666666666",
			RowIDProfileID:  "housegate-row-id-v1",
		},
	}
}

func loadStatementEnvelopeGolden(t *testing.T) statementEnvelopeGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/statement_envelope_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var g statementEnvelopeGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return g
}

func TestStatementEnvelopeCanonicalEncodingIsFrozen(t *testing.T) {
	g := loadStatementEnvelopeGolden(t)
	if g.HashProfile != "housegate-replay-mvp-v0" || g.Domain != arbiter.DomainL3Statements {
		t.Fatalf("golden profile/domain = %s/%s", g.HashProfile, g.Domain)
	}
	if g.Digest != goldenStatementsRoot {
		t.Fatalf("testdata digest %s disagrees with the source constant %s; both must move together in a deliberate versioned change", g.Digest, goldenStatementsRoot)
	}

	got, err := json.Marshal(goldenEnvelopes())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var want bytes.Buffer
	if err := json.Compact(&want, g.Value); err != nil {
		t.Fatalf("compact golden value: %v", err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("StatementEnvelope canonical JSON changed.\n got: %s\nwant: %s\n"+
			"encoding/json emits fields in declaration order, so a reorder, rename, retag or new field breaks every historical statements_root. Revert the struct change.", got, want.Bytes())
	}
}

func TestStatementsRootGoldenDigest(t *testing.T) {
	got, err := replay.CanonicalDigest(arbiter.DomainL3Statements, goldenEnvelopes())
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	if got != goldenStatementsRoot {
		t.Fatalf("statements_root golden drift: got %s want %s\n"+
			"this digest is anchored on L2 through L3BlockHeader.ChainHash. Do not re-bless it: revert whatever changed the encoding.", got, goldenStatementsRoot)
	}
}
```

- [ ] **Step 3: Run and confirm green**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core && bazel run //:gazelle && grep -n 'testdata' conformance/BUILD.bazel; bazel test //conformance:conformance_test --test_output=errors`
Expected: `data = glob(["testdata/**"])` present in the `go_test` rule (add `data = glob(["testdata/**"]),` by hand if gazelle did not); PASS.

- [ ] **Step 4: PROVE the vector detects a reorder — swap two fields, observe red, revert**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
# Swap the DECLARATION order of SQLHash and SettingsHash in types.go (tags and
# values are untouched); this is exactly the "someone regroups the struct"
# refactor the golden exists to catch.
python3 - <<'PY'
p='types.go'
s=open(p).read()
s=s.replace('''	SQLHash         string        `json:"sql_hash"`
	SettingsHash    string        `json:"settings_hash,omitempty"`''','''	SettingsHash    string        `json:"settings_hash,omitempty"`
	SQLHash         string        `json:"sql_hash"`''')
open(p,'w').write(s)
PY
go test ./conformance/ -run 'TestStatementEnvelopeCanonicalEncodingIsFrozen|TestStatementsRootGoldenDigest' 2>&1 | tail -20
```
Expected: **FAIL**, both tests. `TestStatementsRootGoldenDigest` must print exactly:
```
statements_root golden drift: got 0x0efa77b50a1f4166e170dc6259a87151e8cd0da9d5b1c504cef1a103504d3117 want 0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921
```
and `TestStatementEnvelopeCanonicalEncodingIsFrozen` must show the `got:` line with `"settings_hash"` preceding `"sql_hash"`. If either stays green, the fixture is not exercising the field — fix it before continuing.

Then revert and re-confirm:
```bash
git checkout -- types.go
go test ./conformance/ -run 'TestStatementEnvelopeCanonicalEncodingIsFrozen|TestStatementsRootGoldenDigest'
```
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git status --porcelain   # must show ONLY the new test + testdata (+ BUILD.bazel)
git add conformance/statement_envelope_golden_test.go conformance/testdata/statement_envelope_golden.json conformance/BUILD.bazel
git commit -m "test(conformance): freeze the StatementEnvelope canonical encoding"
```

### Task 12: Replay and fresh prepare share one validation (D4)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Modify: `snode/staged.go:73-130`
- Test: `snode/staged_prepare_test.go`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `(*Role).resolveEnvelopeSchema(env arbiter.StatementEnvelope) (payloadexec.TableSchema, error)` — wraps `ErrSchemaUnknown` / `ErrSchemaHashMismatch`. Call order in `PrepareLocalStatement` becomes: `validatePrepareBindings` → `resolveEnvelopeSchema` → journal load / cached-result branch → fresh path.

- [ ] **Step 1: Write the failing test**

Append to `snode/staged_prepare_test.go`:

```go
// TestPrepareLocalStatement_ReplayRevalidatesSchemaHash is spec K 1d: the
// cached-result branch used to return the stored result after only an envelope
// DeepEqual, so a role whose table set changed between the original prepare and
// a replay would serve the cached result under the NEW schema. The schema-hash
// check must run above that branch.
func TestPrepareLocalStatement_ReplayRevalidatesSchemaHash(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := nativePayload(t, pv{"p0", 1})
	req := stagedRequest(payload)
	if _, err := role.PrepareLocalStatement(ctx, req, payload); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	partsBefore := countActiveParts(t, conn, role, schema)

	// The role's declared schema moves under the journal: same table id, one
	// more column. The signed envelope still carries the OLD schema_hash.
	evolved := schema
	evolved.Columns = append(append([]lthash.Column(nil), schema.Columns...), lthash.Column{Name: "added", Type: "UInt8"})
	role.cfg.Tables = []payloadexec.TableSchema{evolved}
	role.cfg.SchemaRoot = payloadexec.SchemaRoot(role.cfg.NetworkID, role.cfg.Tables)

	_, err := role.PrepareLocalStatement(ctx, req, payload)
	if !errors.Is(err, ErrSchemaHashMismatch) {
		t.Fatalf("replay under a changed schema must be a terminal schema-hash reject, got %v", err)
	}
	if after := countActiveParts(t, conn, role, schema); after != partsBefore {
		t.Fatalf("a rejected replay must not write: before=%d after=%d", partsBefore, after)
	}
}

// TestPrepareLocalStatement_ReplayRejectsUnknownTable covers the same hoist for
// a table the role no longer declares at all.
func TestPrepareLocalStatement_ReplayRejectsUnknownTable(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := nativePayload(t, pv{"p0", 1})
	req := stagedRequest(payload)
	if _, err := role.PrepareLocalStatement(ctx, req, payload); err != nil {
		t.Fatalf("first prepare: %v", err)
	}
	role.cfg.Tables = nil
	if _, err := role.PrepareLocalStatement(ctx, req, payload); !errors.Is(err, ErrSchemaUnknown) {
		t.Fatalf("replay for an undeclared table must be ErrSchemaUnknown, got %v", err)
	}
}
```

Add `"github.com/housegate/housegate/pkg/lthash"` to that file's imports if it is not already there.

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core && ARBITER_CH_INTEGRATION=1 bazel test //snode:snode_test --test_filter='TestPrepareLocalStatement_Replay(RevalidatesSchemaHash|RejectsUnknownTable)' --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_output=errors`
(with ClickHouse 25.8 listening on `CH_ADDR`, default `127.0.0.1:9000`)
Expected: FAIL — both report the cached result was returned (`got <nil>`), because today the schema lookup sits below the cached-result branch.

- [ ] **Step 3: Implement**

In `snode/staged.go`, add the shared helper immediately above `validatePrepareBindings`:

```go
// resolveEnvelopeSchema is the binding check every prepare attempt must pass —
// fresh, cached, or converged. The role's declared schema can change between
// the original prepare and a replay, and serving a cached result under a new
// schema would silently decouple the stored bytes from the schema the user
// signed, so the check runs above the journal branch, never inside it.
func (r *Role) resolveEnvelopeSchema(env arbiter.StatementEnvelope) (payloadexec.TableSchema, error) {
	schema, err := r.schemaFor(env.TargetTableID)
	if err != nil {
		return payloadexec.TableSchema{}, fmt.Errorf("%v: %w", err, ErrSchemaUnknown)
	}
	if want := payloadexec.TableSchemaHash(r.cfg.NetworkID, schema); env.SchemaHash != want {
		return payloadexec.TableSchema{}, fmt.Errorf("statement %s schema_hash %q, source has %q: %w",
			env.StatementID.Flat(), env.SchemaHash, want, ErrSchemaHashMismatch)
	}
	return schema, nil
}
```

In `PrepareLocalStatement`, insert the call directly after `validatePrepareBindings` (before `flat := …`):

```go
	payloadEncoding, revision, err := validatePrepareBindings(req)
	if err != nil {
		return PreparedLocalResult{}, err
	}
	// Envelope v2: the agent signed the schema hash it encoded against. Verified
	// BEFORE the journal branch so a cached result cannot be served under a
	// schema the user never signed, and before any decode or unsafe write.
	schema, err := r.resolveEnvelopeSchema(req.Envelope)
	if err != nil {
		return PreparedLocalResult{}, err
	}

	flat := req.Envelope.StatementID.Flat()
```

then delete the now-duplicated block from the fresh path (lines 118-127 of the current file), leaving `validatePayloadBinding` and the `r.d.Payloads == nil || r.d.Conn == nil` guard exactly where they are:

```go
	schema, err := r.schemaFor(req.Envelope.TargetTableID)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("%v: %w", err, ErrSchemaUnknown)
	}
	// Envelope v2: the agent signed the schema hash it encoded against; a
	// disagreement with this source's declared schema is a terminal reject,
	// checked BEFORE any decode or unsafe write.
	if want := payloadexec.TableSchemaHash(r.cfg.NetworkID, schema); req.Envelope.SchemaHash != want {
		return PreparedLocalResult{}, fmt.Errorf("statement %s schema_hash %q, source has %q: %w", flat, req.Envelope.SchemaHash, want, ErrSchemaHashMismatch)
	}
```

`schema` is now the hoisted variable, so everything below that reads it (`nativepayload.Decode`, `touchedPartitions`, `CHTableName`, `assembleAfterWrite`) is unchanged.

**Consequence to accept deliberately:** the `LifecyclePreparing` / `LifecycleAbortPending` convergence path now also requires the schema to resolve, so removing a table from the role's config while one of its statements is mid-convergence blocks that convergence instead of silently converging under a schema the user never signed. That is the fail-closed behaviour D4 asks for; the operator fix is to restore the table declaration, not to relax the check.

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core && ARBITER_CH_INTEGRATION=1 bazel test //snode:snode_test --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=900 --test_output=errors`
Expected: PASS — the two new tests plus every pre-existing `TestPrepareLocalStatement_*` case, notably `_IdempotentReplayAtUnsafeWritten`, `_IdempotentReplayAtRCBound`, `_ReplayAtPreparingConverges` (all replay with an unchanged schema, so the hoisted check is a no-op for them) and `_RejectsSchemaHashMismatchBeforeDecode` (now satisfied by the hoisted call).

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git add snode/staged.go snode/staged_prepare_test.go
git commit -m "fix(snode): verify schema_hash before serving a cached prepare"
```

### Task 13: Full arbiter-core gates and release v0.4.0

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:** none (release only).

**Interfaces:**
- Produces: arbiter-core `v0.4.0`, consumed by Tasks 14 and 23.

- [ ] **Step 1: Run every gate CI runs**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
bazel build //...
bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
go test ./... 2>&1 | tail -20
ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1 bazel test \
  //dataplane/ddl:ddl_test //snode:snode_test //verifier:verifier_test \
  --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=ARBITER_CH_REPLICA \
  --test_env=CH_ADDR --test_env=CH_REPLICA_ADDR --test_timeout=900 --test_output=errors
```
Expected: all PASS. The docker-bound block needs two ClickHouse instances (see `.github/workflows/ci.yml`'s ClickHouse step); if the second replica is unavailable locally, run the first three commands and let CI run the fourth.

- [ ] **Step 2: PR, merge, release**

Run: `gh pr create --fill --base main`, merge green, then `gh workflow run cut-release.yml -f version=v0.4.0` and `gh run watch`.
Expected: non-draft, non-prerelease `v0.4.0`; record merge SHA, run URL and tag object SHA in the evidence section.

---

## Phase 4 — arbiter (spec D1, D2, D3, D7 admission, D8a)

### Task 14: Point arbiter at housegate v0.10.0, arbiter-core v0.4.0, arbiter-proto v0.6.0

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`

**Interfaces:**
- Consumes: housegate `v0.10.0` (Task 9), arbiter-core `v0.4.0` (Task 13), arbiter-proto `v0.6.0` (Task 2).
- Produces: a tree where `auth.SharedStatementVectorsSHA256` and `auth.JWSStatementPayloadV2.StatementKind` exist. Closes the roadmap §5 "arbiter is 7 commits behind housegate v0.9.0" debt.

- [ ] **Step 1: Bump all three pins**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bash scripts/update-housegate.sh v0.10.0
bash scripts/update-arbiter-core.sh v0.4.0
go get github.com/sentioxyz/arbiter-proto@v0.6.0
go mod tidy
```
Expected: `go.mod` shows `housegate v0.10.0`, `arbiter-core v0.4.0`, `arbiter-proto v0.6.0`; the scripts also update `MODULE.bazel`'s `git_override` commits.

- [ ] **Step 2: Build and observe the expected break**

Run: `bazel build //... && bazel test --build_tests_only //fsm:fsm_test --test_output=errors`
Expected: build PASS; `//fsm:fsm_test` FAILs on `TestVerifyUserJWSV2_SharedVectors` — housegate v0.10.0's `StatementPayloadV2Mismatch` now compares `statement_kind`, the committed `fsm/testdata/statement_jws_v2.json` is the old 16-vector file, and `verifyUserJWSV2` does not yet populate the field. Task 19 closes it. Note the failure in the commit body so the intermediate state is explicit.

- [ ] **Step 3: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add go.mod go.sum MODULE.bazel
git commit -m "chore: pin housegate v0.10.0, arbiter-core v0.4.0, arbiter-proto v0.6.0"
```

### Task 15: Freeze `statements_root` and the L3 header preimage (D1 — the headline)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Create: `fsm/l3_commitment_golden_test.go`
- Create: `fsm/testdata/l3_commitment_golden.json`
- Modify: `fsm/BUILD.bazel` (the `fsm_test` rule already has `data = glob(["testdata/**"])` for the JWS vectors — verify it still matches)

**Interfaces:**
- Consumes: arbiter-core `v0.4.0` (Task 14) — the same `arbiter.StatementEnvelope` Task 11 froze.
- Produces: `goldenStatementsRoot` / `goldenHeaderChainHash` source constants and `fsm/testdata/l3_commitment_golden.json`.

**What this is protecting.** `L3BlockHeader.ChainHash()` is `CanonicalDigest(DomainL3Header, header-minus-anchor)`, `StatementsRoot` is `CanonicalDigest(DomainL3Statements, envelopes)`, and `CanonicalDigest` is SHA-256 over `json.Marshal`, which emits struct fields in **declaration order**. So every anchored L2 value is a function of the field order of two Go structs, one of which lives in another module. `TestSealL3Block_StatementsRootCommitsEnvelopes` recomputes the root with the same code and therefore cannot see an encoding change; `TestL3BlockHeader_ChainHashGoldenV2` does pin a header digest, but its fixture leaves `prev_safe_snapshot_id` and `prev_state_root` empty, and `omitempty` drops them — so a reorder involving either is invisible to it. This task adds a fully populated fixture for both commitments and keeps the existing tests as they are.

- [ ] **Step 1: Commit the fixture**

Create `fsm/testdata/l3_commitment_golden.json` exactly:

```json
{
  "hash_profile": "housegate-replay-mvp-v0",
  "statements": {
    "domain": "arbiter-l3-statements-v1",
    "digest": "0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921",
    "value": [
      {
        "statement_id": {
          "client_account": "0x00000000000000000000000000000000000000a1",
          "client_seq": 11,
          "client_nonce": "00112233445566778899aabbccddeeff"
        },
        "statement_kind": 1,
        "sql": "INSERT INTO db1.events FORMAT Native",
        "sql_hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
        "settings_hash": "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
        "payload_ref": "payload/golden-1",
        "payload_hash": "0x2222222222222222222222222222222222222222222222222222222222222222",
        "payload_length": 4096,
        "target_table_id": "db1.events",
        "user_jws": "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjF9.Z29sZGVuLXNpZ25hdHVyZS0x",
        "envelope_version": 2,
        "network_id": "arbiter-golden-net",
        "keeper_shard_id": 0,
        "payload_format": "clickhouse-native-data-v1",
        "client_revision": 54460,
        "schema_hash": "0x3333333333333333333333333333333333333333333333333333333333333333",
        "row_id_profile_id": "housegate-row-id-v1"
      },
      {
        "statement_id": {
          "client_account": "0x00000000000000000000000000000000000000b2",
          "client_seq": 12,
          "client_nonce": "ffeeddccbbaa99887766554433221100"
        },
        "statement_kind": 1,
        "sql": "INSERT INTO db1.metrics FORMAT Native",
        "sql_hash": "0x4444444444444444444444444444444444444444444444444444444444444444",
        "settings_hash": "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
        "payload_ref": "payload/golden-2",
        "payload_hash": "0x5555555555555555555555555555555555555555555555555555555555555555",
        "payload_length": 8192,
        "target_table_id": "db1.metrics",
        "user_jws": "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjJ9.Z29sZGVuLXNpZ25hdHVyZS0y",
        "envelope_version": 2,
        "network_id": "arbiter-golden-net",
        "keeper_shard_id": 0,
        "payload_format": "clickhouse-native-data-v1",
        "client_revision": 54460,
        "schema_hash": "0x6666666666666666666666666666666666666666666666666666666666666666",
        "row_id_profile_id": "housegate-row-id-v1"
      }
    ]
  },
  "header": {
    "domain": "arbiter-l3-header-v1",
    "digest": "0x168866c1498647761215bb57494f23658828b6b0073efc61dbccfc7d87ed8bd2",
    "value": {
      "l3_block_seq": 7,
      "prev_l3_hash": "0x7777777777777777777777777777777777777777777777777777777777777777",
      "statement_seq_start": 11,
      "statement_count": 2,
      "schema_snapshot_id": "schema-genesis",
      "executor_profile_id": "housegate-replay-mvp-v0",
      "prev_safe_snapshot_id": "safe-snapshot-6",
      "prev_state_root": "0x8888888888888888888888888888888888888888888888888888888888888888",
      "spent_ids_root_after": "0x9999999999999999999999999999999999999999999999999999999999999999",
      "statements_root": "0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921"
    }
  }
}
```

The `statements` block is byte-identical to arbiter-core's `conformance/testdata/statement_envelope_golden.json` `value`, and the header's `statements_root` is the `statements` digest — so the fixture also proves the two repos agree on the same preimage.

- [ ] **Step 2: Write the failing test**

Create `fsm/l3_commitment_golden_test.go`:

```go
package fsm

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// The L3 commitment preimages are consensus parameters, not implementation
// details. CanonicalDigest is SHA-256 over json.Marshal, and encoding/json
// emits struct fields in DECLARATION order, so these digests are a function of
// the field ORDER of arbiter.StatementEnvelope (another module!) and
// L3BlockHeader, not only of their field sets. statements_root sits inside
// ChainHash, ChainHash is what gets anchored on L2, and every historical
// anchored value must stay recomputable forever.
//
// If one of these tests fails: revert whatever changed the encoding. Changing
// them is a versioned protocol migration with an explicit plan, never a
// refactor, and never a "regenerate the goldens" step — which is why there is
// no regenerator here and why the digests live in Go source as well as in the
// fixture file.
const (
	goldenStatementsRoot  = "0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921"
	goldenHeaderChainHash = "0x168866c1498647761215bb57494f23658828b6b0073efc61dbccfc7d87ed8bd2"
)

type l3CommitmentGolden struct {
	HashProfile string `json:"hash_profile"`
	Statements  struct {
		Domain string          `json:"domain"`
		Digest string          `json:"digest"`
		Value  json.RawMessage `json:"value"`
	} `json:"statements"`
	Header struct {
		Domain string          `json:"domain"`
		Digest string          `json:"digest"`
		Value  json.RawMessage `json:"value"`
	} `json:"header"`
}

func loadL3CommitmentGolden(t *testing.T) l3CommitmentGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/l3_commitment_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var g l3CommitmentGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return g
}

// goldenEnvelopes mirrors testdata/l3_commitment_golden.json's statements.value
// and arbiter-core's conformance/testdata/statement_envelope_golden.json. Every
// field is populated: omitempty drops absent fields, and an absent field cannot
// witness a reorder.
func goldenEnvelopes() []arbiter.StatementEnvelope {
	return []arbiter.StatementEnvelope{
		{
			StatementID: arbiter.StatementID{
				ClientAccount: "0x00000000000000000000000000000000000000a1",
				ClientSeq:     11,
				ClientNonce:   "00112233445566778899aabbccddeeff",
			},
			StatementKind:   arbiter.StatementKindInsert,
			SQL:             "INSERT INTO db1.events FORMAT Native",
			SQLHash:         "0x1111111111111111111111111111111111111111111111111111111111111111",
			SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
			PayloadRef:      "payload/golden-1",
			PayloadHash:     "0x2222222222222222222222222222222222222222222222222222222222222222",
			PayloadLength:   4096,
			TargetTableID:   "db1.events",
			UserJWS:         "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjF9.Z29sZGVuLXNpZ25hdHVyZS0x",
			EnvelopeVersion: 2,
			NetworkID:       "arbiter-golden-net",
			KeeperShardID:   0,
			PayloadFormat:   "clickhouse-native-data-v1",
			ClientRevision:  54460,
			SchemaHash:      "0x3333333333333333333333333333333333333333333333333333333333333333",
			RowIDProfileID:  "housegate-row-id-v1",
		},
		{
			StatementID: arbiter.StatementID{
				ClientAccount: "0x00000000000000000000000000000000000000b2",
				ClientSeq:     12,
				ClientNonce:   "ffeeddccbbaa99887766554433221100",
			},
			StatementKind:   arbiter.StatementKindInsert,
			SQL:             "INSERT INTO db1.metrics FORMAT Native",
			SQLHash:         "0x4444444444444444444444444444444444444444444444444444444444444444",
			SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
			PayloadRef:      "payload/golden-2",
			PayloadHash:     "0x5555555555555555555555555555555555555555555555555555555555555555",
			PayloadLength:   8192,
			TargetTableID:   "db1.metrics",
			UserJWS:         "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjJ9.Z29sZGVuLXNpZ25hdHVyZS0y",
			EnvelopeVersion: 2,
			NetworkID:       "arbiter-golden-net",
			KeeperShardID:   0,
			PayloadFormat:   "clickhouse-native-data-v1",
			ClientRevision:  54460,
			SchemaHash:      "0x6666666666666666666666666666666666666666666666666666666666666666",
			RowIDProfileID:  "housegate-row-id-v1",
		},
	}
}

func goldenHeader() L3BlockHeader {
	return L3BlockHeader{
		L3BlockSeq:         7,
		PrevL3Hash:         "0x7777777777777777777777777777777777777777777777777777777777777777",
		StatementSeqStart:  11,
		StatementCount:     2,
		SchemaSnapshotID:   "schema-genesis",
		ExecutorProfileID:  "housegate-replay-mvp-v0",
		PrevSafeSnapshotID: "safe-snapshot-6",
		PrevStateRoot:      "0x8888888888888888888888888888888888888888888888888888888888888888",
		SpentIDsRootAfter:  "0x9999999999999999999999999999999999999999999999999999999999999999",
		StatementsRoot:     goldenStatementsRoot,
	}
}

func assertMarshalsTo(t *testing.T, what string, v any, fixture json.RawMessage) {
	t.Helper()
	got, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %s: %v", what, err)
	}
	var want bytes.Buffer
	if err := json.Compact(&want, fixture); err != nil {
		t.Fatalf("compact %s fixture: %v", what, err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("%s canonical JSON changed.\n got: %s\nwant: %s\n"+
			"the fixture in testdata/l3_commitment_golden.json is the diffable record of the preimage; revert the struct change.", what, got, want.Bytes())
	}
}

func TestL3CommitmentPreimagesAreFrozen(t *testing.T) {
	g := loadL3CommitmentGolden(t)
	if g.HashProfile != "housegate-replay-mvp-v0" {
		t.Fatalf("golden hash profile = %q", g.HashProfile)
	}
	if g.Statements.Domain != arbiter.DomainL3Statements || g.Header.Domain != arbiter.DomainL3Header {
		t.Fatalf("golden domains = %s / %s", g.Statements.Domain, g.Header.Domain)
	}
	if g.Statements.Digest != goldenStatementsRoot || g.Header.Digest != goldenHeaderChainHash {
		t.Fatalf("testdata digests (%s / %s) disagree with the source constants (%s / %s); both must move together in a deliberate versioned change",
			g.Statements.Digest, g.Header.Digest, goldenStatementsRoot, goldenHeaderChainHash)
	}
	assertMarshalsTo(t, "[]arbiter.StatementEnvelope", goldenEnvelopes(), g.Statements.Value)
	assertMarshalsTo(t, "L3BlockHeader", goldenHeader(), g.Header.Value)
}

func TestStatementsRootGoldenDigest(t *testing.T) {
	got, err := replay.CanonicalDigest(arbiter.DomainL3Statements, goldenEnvelopes())
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	if got != goldenStatementsRoot {
		t.Fatalf("statements_root golden drift: got %s want %s", got, goldenStatementsRoot)
	}
}

func TestL3BlockHeaderChainHashGoldenDigest(t *testing.T) {
	got, err := goldenHeader().ChainHash()
	if err != nil {
		t.Fatalf("ChainHash: %v", err)
	}
	if got != goldenHeaderChainHash {
		t.Fatalf("ChainHash golden drift: got %s want %s", got, goldenHeaderChainHash)
	}
	// The back-filled anchor stays excluded (§5.2): anchoring must not rewrite
	// history, so an anchored copy hashes identically.
	anchored := goldenHeader()
	anchored.L2AnchorRef = &arbiter.AnchorRef{L3BlockHash: "0xdeadbeef"}
	again, err := anchored.ChainHash()
	if err != nil {
		t.Fatalf("ChainHash(anchored): %v", err)
	}
	if again != goldenHeaderChainHash {
		t.Fatalf("anchored ChainHash = %s, want %s", again, goldenHeaderChainHash)
	}
}

// TestSealStampsTheGoldenPreimage closes the loop: applySealL3Block must build
// its statements_root from exactly the encoding the golden pins, so the frozen
// digest is the one production actually produces.
func TestSealStampsTheGoldenPreimage(t *testing.T) {
	root, err := replay.CanonicalDigest(arbiter.DomainL3Statements, goldenEnvelopes())
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	sealBlock(t, f, 2)
	live := f.st.Blocks[0]
	liveEnvs := []arbiter.StatementEnvelope{f.st.Statements[1].Env, f.st.Statements[2].Env}
	liveRoot, err := replay.CanonicalDigest(arbiter.DomainL3Statements, liveEnvs)
	if err != nil {
		t.Fatalf("CanonicalDigest(live): %v", err)
	}
	if live.StatementsRoot != liveRoot {
		t.Fatalf("seal stamped %s, recompute says %s", live.StatementsRoot, liveRoot)
	}
	if root == liveRoot {
		t.Fatal("the golden fixture must not accidentally equal the live test envelopes")
	}
}
```

- [ ] **Step 3: Run and confirm green**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && bazel run //:gazelle && bazel test //fsm:fsm_test --test_filter='TestL3Commitment|TestStatementsRootGolden|TestL3BlockHeaderChainHashGolden|TestSealStampsTheGoldenPreimage' --test_output=errors`
Expected: PASS (4 tests). `//fsm:fsm_test` as a whole is still red on `TestVerifyUserJWSV2_SharedVectors` from Task 14 — use `--test_filter` until Task 19.

- [ ] **Step 4: PROVE the header vector detects a reorder — swap two fields, observe red, revert**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
# Swap the DECLARATION order of SpentIDsRootAfter and StatementsRoot in
# fsm/state.go. Tags, types and values are untouched.
python3 - <<'PY'
p='fsm/state.go'
s=open(p).read()
s=s.replace('''	SpentIDsRootAfter  string `json:"spent_ids_root_after"`
	// StatementsRoot = CanonicalDigest(DomainL3Statements, []StatementEnvelope)
	// over the sealed envelopes in statement_seq order (envelope v2 §4.4): the
	// anchored chain hash pins WHAT was sequenced, not just the seq range.
	StatementsRoot string             `json:"statements_root"`''','''	// StatementsRoot = CanonicalDigest(DomainL3Statements, []StatementEnvelope)
	// over the sealed envelopes in statement_seq order (envelope v2 §4.4): the
	// anchored chain hash pins WHAT was sequenced, not just the seq range.
	StatementsRoot string `json:"statements_root"`
	SpentIDsRootAfter  string `json:"spent_ids_root_after"`''')
open(p,'w').write(s)
PY
gofmt -w fsm/state.go
go test ./fsm/ -run 'TestL3CommitmentPreimagesAreFrozen|TestL3BlockHeaderChainHashGoldenDigest' 2>&1 | tail -20
```
Expected: **FAIL**, both tests. `TestL3BlockHeaderChainHashGoldenDigest` must print exactly:
```
ChainHash golden drift: got 0x31e682a7b97139b43436281ed5c8e63e90fe7b2d27e697c3613fecb35cceeb77 want 0x168866c1498647761215bb57494f23658828b6b0073efc61dbccfc7d87ed8bd2
```
and `TestL3CommitmentPreimagesAreFrozen` must show a `got:` line ending `…"statements_root":"0x7268…","spent_ids_root_after":"0x9999…"}`. Note that the pre-existing `TestL3BlockHeader_ChainHashGoldenV2` **also** goes red here — good, but it would have stayed green for a `prev_safe_snapshot_id`/`prev_state_root` swap, which is why the fully populated fixture exists.

Then revert and re-confirm:
```bash
git checkout -- fsm/state.go
go test ./fsm/ -run 'TestL3CommitmentPreimagesAreFrozen|TestL3BlockHeaderChainHashGoldenDigest'
```
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git status --porcelain   # must show ONLY the new test + testdata
git add fsm/l3_commitment_golden_test.go fsm/testdata/l3_commitment_golden.json fsm/BUILD.bazel
git commit -m "test(fsm): freeze the L3 statements-root and header preimages"
```

### Task 16: Genesis identity is validated at the FSM boundary (D2)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/state.go` (add `Params.Validate` under the `Params` struct at ~:82-89), `fsm/fsm.go:46-50`, `fsm/watch.go:34-40`, `fsm/snapshot.go:109-118`
- Modify: `cmd/arbiter/services.go:38-46`, `cmd/arbiter/app.go:42`
- Modify (call sites): `fsm/fsm_test.go`, `fsm/watch_test.go`, `fsm/snapshot_test.go`, `server/gateway_test.go`, `server/server_test.go`, `server/safestate_l3block_test.go`, `raftnode/node_test.go`, `orchestrator/loop_test.go`, `integration/cluster_node_test.go`, `integration/chpipeline/cluster_test.go`, `cmd/arbiter/main_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `fsm.ErrGenesisParams`, `(Params).Validate() error`, `fsm.New(Params) (*FSM, error)`, `fsm.NewWithNotify(Params, chan<- Event) (*FSM, error)`, `newFSM(config.Config) (*fsm.FSM, <-chan fsm.Event, error)`.

**Spec correction, recorded deliberately.** Spec K 1b says `run(ctx, cfg, logger)` never re-validates and that `cmd/arbiter/main_test.go` proves the bypass. Against `71657a8` that is no longer accurate: `cmd/arbiter/app.go:37` calls `cfg.Validate()`, and `applyTestDefaults` (`cmd/arbiter/main_test.go:239`) sets `Genesis.NetworkID`. The live gap is the one D2 names: **`fsm.New`/`NewWithNotify` accept empty Params**, and `server/gateway_test.go`, `server/server_test.go` and `raftnode/node_test.go` today construct FSMs with `NetworkID: ""` — against which `applySubmitStatement`'s `env.NetworkID != f.st.Params.NetworkID` admits envelopes carrying `network_id: ""`. This task fixes that and adds the `run`-rejects-invalid-config test the spec's acceptance list asks for; it does not duplicate the existing `cfg.Validate()` call.

- [ ] **Step 1: Write the failing tests**

Append to `fsm/fsm_test.go`:

```go
func TestNewRejectsIncompleteGenesisParams(t *testing.T) {
	full := testParams()
	for name, mutate := range map[string]func(*Params){
		"empty network id":         func(p *Params) { p.NetworkID = "" },
		"blank network id":         func(p *Params) { p.NetworkID = "   " },
		"empty schema snapshot id": func(p *Params) { p.SchemaSnapshotID = "" },
		"empty profile id":         func(p *Params) { p.ExecutorProfileID = "" },
	} {
		t.Run(name, func(t *testing.T) {
			p := full
			mutate(&p)
			if _, err := New(p); !errors.Is(err, ErrGenesisParams) {
				t.Fatalf("New(%+v) err = %v, want ErrGenesisParams", p, err)
			}
			if _, err := NewWithNotify(p, nil); !errors.Is(err, ErrGenesisParams) {
				t.Fatalf("NewWithNotify(%+v) err = %v, want ErrGenesisParams", p, err)
			}
		})
	}
	if _, err := New(full); err != nil {
		t.Fatalf("New(valid) err = %v", err)
	}
}

// TestEmptyNetworkIDCannotDegradeCrossNetworkBinding is the property D2 exists
// for: admission compares env.NetworkID against Params.NetworkID, so an FSM
// built with an empty NetworkID would admit envelopes claiming no network.
func TestEmptyNetworkIDCannotDegradeCrossNetworkBinding(t *testing.T) {
	if _, err := New(Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"}); err == nil {
		t.Fatal("an FSM with no NetworkID makes the cross-network binding a no-op and must not be constructible")
	}
}

func TestRestoreRejectsIncompleteGenesisParams(t *testing.T) {
	// A snapshot carrying empty genesis params is as dangerous as a bad New.
	f := newTestFSM(t)
	var buf bytes.Buffer
	st := newState(Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})
	if err := writeSnapshot(&buf, st); err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
	if err := f.Restore(io.NopCloser(bytes.NewReader(buf.Bytes()))); !errors.Is(err, ErrGenesisParams) {
		t.Fatalf("Restore err = %v, want ErrGenesisParams", err)
	}
	// The pre-existing state must survive a rejected restore.
	if f.st.Params.NetworkID != testParams().NetworkID {
		t.Fatalf("a rejected Restore must not replace state: %+v", f.st.Params)
	}
}
```

(`fsm_test.go` needs `bytes`, `errors`, `io` in its imports.)

Append to `cmd/arbiter/main_test.go`:

```go
func TestRun_RejectsConfigWithoutGenesisNetworkID(t *testing.T) {
	dir := t.TempDir()
	cfg := config.Config{NodeID: "arb-1", GRPCListen: "127.0.0.1:0"}
	cfg.Raft.Listen = "127.0.0.1:0"
	cfg.Raft.Advertise = cfg.Raft.Listen
	cfg.Raft.DataDir = filepath.Join(dir, "raft")
	cfg.Raft.Bootstrap = true
	cfg.Raft.Peers = []config.RaftPeer{{ID: "arb-1", Addr: cfg.Raft.Listen}}
	cfg.Genesis.SchemaSnapshotID = "schema-genesis"
	cfg.Genesis.ExecutorProfileID = "housegate-replay-mvp-v0"
	applyTestDefaults(&cfg)
	cfg.Genesis.NetworkID = "" // applyTestDefaults set it; a literal config need not

	_, err := run(t.Context(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("run must reject a config without genesis.network_id")
	}
	if !strings.Contains(err.Error(), "genesis.network_id") {
		t.Fatalf("run err = %v, want it to name genesis.network_id", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && go test ./fsm/ -run 'TestNewRejects|TestEmptyNetworkID|TestRestoreRejects' && go test ./cmd/arbiter/ -run TestRun_RejectsConfigWithoutGenesisNetworkID`
Expected: FAIL — `New(p) (no value) used as value` (New returns one value today) and `undefined: ErrGenesisParams`. The `cmd/arbiter` test may already pass (config validation exists); if it does, keep it — it is the spec's acceptance bullet and it stops a future refactor from dropping the call.

- [ ] **Step 3: Implement**

In `fsm/state.go`, directly under the `Params` struct:

```go
// ErrGenesisParams reports incomplete consensus genesis parameters.
var ErrGenesisParams = errors.New("fsm: genesis params incomplete")

// Validate rejects an FSM identity that would silently disable a consensus
// binding. An empty NetworkID is the sharp one: admission compares
// env.NetworkID against this value, so two empty strings make the cross-network
// binding a no-op. SchemaSnapshotID and ExecutorProfileID have the same shape
// of risk (they are stamped into every sealed header). Config validation stays
// where it is; this is the boundary embedded and library hosts cross.
func (p Params) Validate() error {
	var errs []error
	if strings.TrimSpace(p.NetworkID) == "" {
		errs = append(errs, fmt.Errorf("%w: network_id is required", ErrGenesisParams))
	}
	if strings.TrimSpace(p.SchemaSnapshotID) == "" {
		errs = append(errs, fmt.Errorf("%w: schema_snapshot_id is required", ErrGenesisParams))
	}
	if strings.TrimSpace(p.ExecutorProfileID) == "" {
		errs = append(errs, fmt.Errorf("%w: executor_profile_id is required", ErrGenesisParams))
	}
	return errors.Join(errs...)
}
```

(add `errors`, `fmt`, `strings` to `fsm/state.go`'s imports as needed.)

In `fsm/fsm.go`:

```go
// New builds an FSM. Params are consensus parameters — identical on every
// node or the cluster forks — and are validated here because embedded and
// library hosts never pass through config.Validate.
func New(params Params) (*FSM, error) {
	return NewWithNotify(params, nil)
}
```

In `fsm/watch.go`:

```go
func NewWithNotify(params Params, notify chan<- Event) (*FSM, error) {
	if err := params.Validate(); err != nil {
		return nil, err
	}
	return &FSM{st: newState(params), notify: notify}, nil
}
```

In `fsm/snapshot.go`, inside `readSnapshot`, immediately after `json.Unmarshal(jb, &doc)` succeeds:

```go
	if err := doc.Params.Validate(); err != nil {
		return nil, fmt.Errorf("snapshot genesis params: %w", err)
	}
```

(`Restore` returns that error before touching `f.st`, so a rejected restore leaves state intact.)

In `cmd/arbiter/services.go`:

```go
func newFSM(cfg config.Config) (*fsm.FSM, <-chan fsm.Event, error) {
	events := make(chan fsm.Event, 1024)
	f, err := fsm.NewWithNotify(fsm.Params{
		NetworkID:          cfg.Genesis.NetworkID,
		SchemaSnapshotID:   cfg.Genesis.SchemaSnapshotID,
		ExecutorProfileID:  cfg.Genesis.ExecutorProfileID,
		AuthorityAddresses: cfg.Authority.AllowedAddresses,
	}, events)
	if err != nil {
		return nil, nil, err
	}
	return f, events, nil
}
```

In `cmd/arbiter/app.go:42`:

```go
	arbiterFSM, events, err := newFSM(cfg)
	if err != nil {
		stopApp()
		return nil, err
	}
```

- [ ] **Step 4: Fix every call site**

There are exactly these, all now two-valued:

| File | Fix |
|---|---|
| `fsm/fsm_test.go` `newTestFSM` | `f, err := New(testParams()); if err != nil { t.Fatal(err) }` |
| `fsm/watch_test.go:13,86` | same two-value form with `t.Fatal` on error |
| `orchestrator/loop_test.go:22` | same |
| `raftnode/node_test.go:61` | add `NetworkID: "testnet-raftnode"` and take the error |
| `server/gateway_test.go` (5 sites) | add `NetworkID: testServerNetworkID` (declared in `server/ingress_test.go:29`) and take the error |
| `server/server_test.go` (6 sites) | same |
| `server/safestate_l3block_test.go:25` | already sets `NetworkID`; take the error |
| `integration/cluster_node_test.go:32` | already sets `NetworkID` from cfg; take the error |
| `integration/chpipeline/cluster_test.go:154` | same |

To keep the diff small, add one helper per test package rather than repeating the error check, e.g. in `server/ingress_test.go`:

```go
func mustFSM(t *testing.T, params fsm.Params) *fsm.FSM {
	t.Helper()
	if params.NetworkID == "" {
		params.NetworkID = testServerNetworkID
	}
	f, err := fsm.New(params)
	if err != nil {
		t.Fatalf("fsm.New: %v", err)
	}
	return f
}
```

and rewrite the `server` call sites as `mustFSM(t, fsm.Params{SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})`.

- [ ] **Step 5: Run to verify it passes**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && bazel build //... && bazel test //fsm:fsm_test --test_filter='TestNewRejects|TestEmptyNetworkID|TestRestoreRejects|TestSnapshot' --test_output=errors && bazel test //server:server_test //raftnode:node_test //orchestrator:orchestrator_test //cmd/arbiter:arbiter_test --test_output=errors`
Expected: PASS. `grep -rn 'time\.Now' fsm/` must still print nothing.

- [ ] **Step 6: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add fsm/state.go fsm/fsm.go fsm/watch.go fsm/snapshot.go fsm/fsm_test.go fsm/watch_test.go cmd/arbiter/services.go cmd/arbiter/app.go cmd/arbiter/main_test.go server raftnode orchestrator integration
git commit -m "fix(fsm): validate genesis identity at the FSM boundary"
```

### Task 17: `L3BlockView` is complete or errors (D3)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/reads.go:110-133`
- Modify: `server/safestate.go:48-58`
- Test: `fsm/seal_test.go:178-185`, `server/safestate_l3block_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: `fsm.ErrL3BlockNotFound`, `fsm.ErrL3BlockIncomplete`, `(*FSM).L3BlockView(seq uint64) (L3BlockHeader, string, []arbiter.StatementEnvelope, error)` — the `ok bool` becomes an `error` so "no such block" and "this node's copy is short" are distinguishable.

- [ ] **Step 1: Write the failing tests**

Replace the `L3BlockView` block at the end of `TestSealL3Block_StatementsRootCommitsEnvelopes` (`fsm/seal_test.go:178-185`) with:

```go
	// L3BlockView exposes header + envelopes for auditors.
	hdr, chainHash, got2, err := f.L3BlockView(1)
	if err != nil || hdr.L3BlockSeq != 1 || chainHash != base || len(got2) != 2 || got2[0] != envs[0] || got2[1] != envs[1] {
		t.Fatalf("L3BlockView(1) = %+v %s %d err=%v", hdr, chainHash, len(got2), err)
	}
	if _, _, _, err := f.L3BlockView(9); !errors.Is(err, ErrL3BlockNotFound) {
		t.Fatalf("unknown block err = %v, want ErrL3BlockNotFound", err)
	}
```

and append to `fsm/seal_test.go`:

```go
// TestL3BlockViewRejectsAnIncompleteBlock is the auditor's safety net: a short
// envelope list recomputes to a different statements_root and reads as fraud.
// The read surface must say "incomplete" instead of quietly under-reporting.
func TestL3BlockViewRejectsAnIncompleteBlock(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	sealBlock(t, f, 2)
	if _, _, envs, err := f.L3BlockView(1); err != nil || len(envs) != 2 {
		t.Fatalf("precondition: L3BlockView(1) = %d envs, err=%v", len(envs), err)
	}
	// Synthesise the loss the current code would silently tolerate.
	delete(f.st.Statements, 2)
	_, _, _, err := f.L3BlockView(1)
	if !errors.Is(err, ErrL3BlockIncomplete) {
		t.Fatalf("L3BlockView err = %v, want ErrL3BlockIncomplete", err)
	}
	if !strings.Contains(err.Error(), "1") || !strings.Contains(err.Error(), "2") {
		t.Fatalf("error must name the block seq and the counts, got %v", err)
	}
}
```

Append to `server/safestate_l3block_test.go`:

```go
func TestGetL3BlockMapsIncompleteToDataLoss(t *testing.T) {
	h := newSafeStateTestHarness(t)
	h.state.DeleteStatementForTest(2) // test-only hook, see Step 3
	_, err := h.svc.GetL3Block(h.ctx, &pb.L3BlockRef{L3BlockSeq: 1})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.DataLoss {
		t.Fatalf("GetL3Block err = %v, want codes.DataLoss", err)
	}
	if !strings.Contains(st.Message(), "l3 block 1") {
		t.Fatalf("gRPC message must name the block seq, got %q", st.Message())
	}
}

func TestGetL3BlockUnknownSeqStaysNotFound(t *testing.T) {
	h := newSafeStateTestHarness(t)
	_, err := h.svc.GetL3Block(h.ctx, &pb.L3BlockRef{L3BlockSeq: 99})
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.NotFound {
		t.Fatalf("GetL3Block err = %v, want codes.NotFound", err)
	}
}
```

Also update the existing `hdr, chainHash, envs, ok := h.state.L3BlockView(1)` at `server/safestate_l3block_test.go:74` to the `err` form.

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && go test ./fsm/ -run 'TestSealL3Block_StatementsRoot|TestL3BlockViewRejects' && go test ./server/ -run TestGetL3Block`
Expected: FAIL — `undefined: ErrL3BlockNotFound`, `assignment mismatch`, `undefined: DeleteStatementForTest`.

- [ ] **Step 3: Implement**

In `fsm/reads.go`, replace `L3BlockView` (lines 110-133) with:

```go
// ErrL3BlockNotFound reports that no sealed block exists at that seq.
var ErrL3BlockNotFound = errors.New("fsm: no sealed L3 block at that seq")

// ErrL3BlockIncomplete reports that a sealed block's header and its retained
// statement records disagree. An auditor recomputing statements_root from a
// short list would get a different digest and read it as fraud, so the read
// surface must refuse rather than under-report.
var ErrL3BlockIncomplete = errors.New("fsm: sealed L3 block is missing statements")

// L3BlockView returns a detached copy of one sealed block's header, its chain
// hash, and its envelopes in statement_seq order — the material an auditor
// needs to recompute statements_root and ChainHash (SafeState.GetL3Block).
// The envelope list is complete or the call errors.
func (f *FSM) L3BlockView(seq uint64) (L3BlockHeader, string, []arbiter.StatementEnvelope, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	idx := int(seq) - 1
	if idx < 0 || idx >= len(f.st.Blocks) {
		return L3BlockHeader{}, "", nil, fmt.Errorf("%w: %d", ErrL3BlockNotFound, seq)
	}
	header := f.st.Blocks[idx]
	chainHash, err := header.ChainHash()
	if err != nil {
		return L3BlockHeader{}, "", nil, fmt.Errorf("l3 block %d chain hash: %w", seq, err)
	}
	header.L2AnchorRef = cloneAnchorRef(header.L2AnchorRef)
	envs := make([]arbiter.StatementEnvelope, 0, header.StatementCount)
	for s := header.StatementSeqStart; s < header.StatementSeqStart+uint64(header.StatementCount); s++ {
		if ss := f.st.Statements[s]; ss != nil {
			envs = append(envs, ss.Env)
		}
	}
	if uint32(len(envs)) != header.StatementCount {
		return L3BlockHeader{}, "", nil, fmt.Errorf("%w: l3 block %d header declares %d statements, %d retained",
			ErrL3BlockIncomplete, seq, header.StatementCount, len(envs))
	}
	return header, chainHash, envs, nil
}

// DeleteStatementForTest removes one statement record. It exists only so tests
// can synthesise the incomplete-block state ErrL3BlockIncomplete guards; no
// production path deletes from f.st.Statements.
func (f *FSM) DeleteStatementForTest(seq uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.st.Statements, seq)
}
```

(add `errors` and `fmt` to `fsm/reads.go`'s imports if absent.)

In `server/safestate.go`, replace `GetL3Block`'s first three lines:

```go
func (svc *safeStateService) GetL3Block(_ context.Context, ref *pb.L3BlockRef) (*pb.L3Block, error) {
	hdr, chainHash, envs, err := svc.s.d.FSM.L3BlockView(ref.GetL3BlockSeq())
	switch {
	case errors.Is(err, fsm.ErrL3BlockNotFound):
		return nil, status.Error(codes.NotFound, "no sealed block at that seq")
	case errors.Is(err, fsm.ErrL3BlockIncomplete):
		// DataLoss, not NotFound: the block exists, this node's copy of it is
		// short, and an auditor must not mistake that for the block's content.
		return nil, status.Errorf(codes.DataLoss, "l3 block %d is incomplete on this node: %v", ref.GetL3BlockSeq(), err)
	case err != nil:
		return nil, status.Errorf(codes.Internal, "l3 block %d: %v", ref.GetL3BlockSeq(), err)
	}
	out := &pb.L3Block{Header: l3HeaderToPB(hdr), ChainHash: chainHash}
	for _, env := range envs {
		out.Statements = append(out.Statements, wire.EnvelopeToPB(env))
	}
	return out, nil
}
```

(add `errors` to `server/safestate.go`'s imports.)

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && bazel test //fsm:fsm_test --test_filter='TestSealL3Block|TestL3BlockView' --test_output=errors && bazel test //server:server_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add fsm/reads.go fsm/seal_test.go server/safestate.go server/safestate_l3block_test.go
git commit -m "fix(fsm): fail the L3 audit read surface on an incomplete block"
```

### Task 18: A non-canonical protected header is MALFORMED (D8a)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/userjws.go:51-53`
- Test: `fsm/userjws_v2_test.go:108-138`, `fsm/admission_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces: no new symbols. Behaviour: `verifyUserJWSV2` wraps the canonical-header failure in `errMalformedUserJWS`, so `applySubmitStatement` maps it to `AdmissionCodeMalformed`.

**Why MALFORMED.** PR #17 split structural/UTF-8 failures (`MALFORMED`) from binding and signer failures (`INVALID_SIGNATURE`) but left the exact-canonical-header check on the signature side. A wrong protected header is a structural defect in the token, not evidence about the signer — and `INVALID_SIGNATURE` is the code the fraud story reserves for "this signature does not authorise this envelope". The existing test asserts only `err == nil`, so the misclassification is invisible.

- [ ] **Step 1: Strengthen the tests**

Replace `TestVerifyUserJWSV2_RejectsNonCanonicalProtectedHeader`'s assertion (`fsm/userjws_v2_test.go:133-135`) with:

```go
			err = verifyUserJWSV2(env)
			if err == nil {
				t.Fatalf("expected canonical protected-header rejection for %s", rawHeader)
			}
			if !errors.Is(err, errMalformedUserJWS) {
				t.Fatalf("a non-canonical protected header is a structural defect, not signature evidence: %v", err)
			}
			if !strings.Contains(err.Error(), "protected header") {
				t.Fatalf("error must name the protected header, got %v", err)
			}
```

(add `errors` to that file's imports.)

Append to `fsm/admission_test.go`:

```go
// TestAdmission_NonCanonicalProtectedHeaderIsMalformed pins the taxonomy at the
// admission boundary, which is what the client actually sees. PR #17's split
// left this one case on the INVALID_SIGNATURE side, and the unit test asserted
// only err != nil, so nothing caught it.
func TestAdmission_NonCanonicalProtectedHeaderIsMalformed(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	env := validEnvelope(t, key, account, 1)
	parts := strings.Split(env.UserJWS, ".")
	for name, rawHeader := range map[string]string{
		"reordered":   `{"typ":"JWT","alg":"ES256K"}`,
		"extra field": `{"alg":"ES256K","typ":"JWT","kid":"key-1"}`,
		"legacy alg":  `{"alg":"secp256k1","typ":"JWT"}`,
	} {
		t.Run(name, func(t *testing.T) {
			header := base64.RawURLEncoding.EncodeToString([]byte(rawHeader))
			signingInput := header + "." + parts[1]
			sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), key)
			if err != nil {
				t.Fatal(err)
			}
			e := env
			e.StatementID.ClientSeq = 1
			e.UserJWS = signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
			if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeMalformed {
				t.Fatalf("code=%v msg=%q, want MALFORMED (a non-canonical protected header is a structural defect, not signature evidence)", r.Code, r.Message)
			}
		})
	}
}
```

`testAccount` (`fsm/admission_test.go:71`), `validEnvelope` (:80) and `submit` (:106) are that file's existing helpers; `registerActive` and `newTestFSM` live in `fsm/fsm_test.go`. Each sub-case reuses `client_seq = 1`, which is safe because a MALFORMED rejection never reaches the dedup accumulator.

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && go test ./fsm/ -run 'TestVerifyUserJWSV2_RejectsNonCanonicalProtectedHeader|TestAdmission_NonCanonicalProtectedHeaderIsMalformed' -v`
Expected: FAIL — the sub-tests report `a non-canonical protected header is a structural defect, not signature evidence: user_jws: protected header must be exact canonical {"alg":"ES256K","typ":"JWT"}` and `admission code = 5, want MALFORMED`.

- [ ] **Step 3: Implement**

In `fsm/userjws.go`, line 51-53:

```go
	if string(headerJSON) != canonicalStatementProtectedHeader {
		// Structural, not signature evidence: MALFORMED, like the other
		// encoding failures above it.
		return fmt.Errorf("%w: user_jws: protected header must be exact canonical %s", errMalformedUserJWS, canonicalStatementProtectedHeader)
	}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && bazel test //fsm:fsm_test --test_filter='TestVerifyUserJWSV2|TestAdmission_' --test_output=errors`
Expected: PASS except `TestVerifyUserJWSV2_SharedVectors`, still red from Task 14 until Task 19.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add fsm/userjws.go fsm/userjws_v2_test.go fsm/admission_test.go
git commit -m "fix(fsm): classify a non-canonical protected header as MALFORMED"
```

### Task 19: Admission binds `statement_kind`; shared vectors re-synced and asserted by field (D7)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/userjws.go:69-83` (the `want` construction)
- Modify: `fsm/admission_test.go:38-66` (`signStatementV2`), and add a mismatch case
- Replace: `fsm/testdata/statement_jws_v2.json` (verbatim copy of housegate's)
- Modify: `fsm/userjws_v2_test.go`
- Create: `fsm/shared_vectors_test.go`

**Interfaces:**
- Consumes: `auth.JWSStatementPayloadV2.StatementKind` and `auth.SharedStatementVectorsSHA256` (Tasks 6, 7); housegate `v0.10.0` (Task 14).
- Produces: `verifyUserJWSV2` binding `statement_kind`; the byte-identity gate between the two vector copies.

- [ ] **Step 1: Copy the vectors and add the SHA link test**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
HG=$(go list -m -f '{{.Dir}}' github.com/housegate/housegate)
cp "$HG/pkg/auth/testdata/statement_jws_v2.json" fsm/testdata/statement_jws_v2.json
chmod u+w fsm/testdata/statement_jws_v2.json
shasum -a 256 fsm/testdata/statement_jws_v2.json
```
Expected: `6af5c9cc34d6b083935d804799138e059ce7da99fb034a2e0332b3c7ce8737bc`. (The module cache is read-only, hence the `chmod`.)

Create `fsm/shared_vectors_test.go`:

```go
package fsm

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
)

// TestSharedStatementVectorsAreByteIdenticalToHousegate is the cross-repo link
// for testdata/statement_jws_v2.json. The file is produced by housegate's
// pkg/auth generator and consumed verbatim here so both verifiers prove the
// same set; nothing else keeps the two copies together.
//
// A Bazel filegroup would also work but needs hand-kept edits in two
// gazelle-generated BUILD files and does not fire under plain `go test`. The
// exported digest travels with the module we already import, so the check
// fires exactly when this repo bumps its housegate pin — the coordinated
// release moment where the two copies must be re-synced anyway.
//
// If this fails: re-copy the file from the pinned housegate module, do not
// edit the constant.
func TestSharedStatementVectorsAreByteIdenticalToHousegate(t *testing.T) {
	raw, err := os.ReadFile("testdata/statement_jws_v2.json")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != auth.SharedStatementVectorsSHA256 {
		t.Fatalf("fsm/testdata/statement_jws_v2.json sha256 = %s, housegate publishes %s\n"+
			"re-copy it: cp $(go list -m -f '{{.Dir}}' github.com/housegate/housegate)/pkg/auth/testdata/statement_jws_v2.json fsm/testdata/", got, auth.SharedStatementVectorsSHA256)
	}
}
```

- [ ] **Step 2: Strengthen the vector consumer**

In `fsm/userjws_v2_test.go`, extend the vector struct and the assertions:

```go
type statementV2Vector struct {
	Name         string                     `json:"name"`
	Expect       string                     `json:"expect"`
	RejectReason string                     `json:"reject_reason"`
	RejectField  string                     `json:"reject_field"`
	Payload      auth.JWSStatementPayloadV2 `json:"payload"`
	Token        string                     `json:"token"`
}
```

change `loadStatementV2Vectors`'s guard from `< 16` to:

```go
	if len(file.Vectors) != 17 {
		t.Fatalf("expected exactly 17 vectors, got %d", len(file.Vectors))
	}
```

and in `envelopeFromVector` replace the hard-coded `StatementKind: arbiter.StatementKindInsert,` with `StatementKind: arbiter.StatementKind(v.Payload.StatementKind),`. The vector's `payload` block is the *expectation* the verifier must derive from the envelope, so mirroring it is what makes `statement_kind_mismatch` (expectation 0, token 1) disagree while every other vector agrees on the kind and fails on its own field.

Then replace the body of `TestVerifyUserJWSV2_SharedVectors` with:

```go
func TestVerifyUserJWSV2_SharedVectors(t *testing.T) {
	file := loadStatementV2Vectors(t)
	for _, vec := range file.Vectors {
		t.Run(vec.Name, func(t *testing.T) {
			err := verifyUserJWSV2(envelopeFromVector(t, vec))
			switch vec.Expect {
			case "accept":
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
				return
			case "reject":
				if err == nil {
					t.Fatal("expected reject")
				}
			default:
				t.Fatalf("unknown expect %q", vec.Expect)
			}
			switch vec.RejectReason {
			case "binding":
				want := "user_jws: " + vec.RejectField + " does not bind the envelope"
				if err.Error() != want {
					t.Fatalf("error = %q, want exactly %q", err.Error(), want)
				}
				if errors.Is(err, errMalformedUserJWS) {
					t.Fatal("a binding failure must classify as INVALID_SIGNATURE, not MALFORMED")
				}
			case "purpose":
				if err.Error() != "user_jws: purpose does not bind the envelope" {
					t.Fatalf("error = %q, want the purpose binding failure", err.Error())
				}
			case "signature":
				if !strings.Contains(err.Error(), "does not match client_account") {
					t.Fatalf("error = %q, want a signer failure", err.Error())
				}
				if errors.Is(err, errMalformedUserJWS) {
					t.Fatal("a signer failure must classify as INVALID_SIGNATURE, not MALFORMED")
				}
			case "malformed":
				if !errors.Is(err, errMalformedUserJWS) {
					t.Fatalf("error = %q must classify as MALFORMED", err.Error())
				}
			default:
				t.Fatalf("vector %s has no reject_reason", vec.Name)
			}
		})
	}
}
```

(add `errors` to that file's imports.)

- [ ] **Step 3: Add the admission-level kind-binding test**

Append to `fsm/admission_test.go`:

```go
// TestAdmission_StatementKindIsBound: an operator holding a token signed for
// one kind must not be able to attach it to an envelope of another kind. v1
// admits INSERT only, so the reachable case is a token signed for
// STATEMENT_KIND_UNSPECIFIED on an INSERT envelope; the mutation lane makes
// the general case live, which is why it is bound before that lane ships.
func TestAdmission_StatementKindIsBound(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	env := validEnvelope(t, key, account, 1)

	unsigned := env
	unsigned.StatementKind = arbiter.StatementKindUnspecified
	env.UserJWS = signStatementV2(t, key, unsigned) // token says 0, envelope says INSERT

	if r := submit(t, f, env); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("code=%v msg=%q, want INVALID_SIGNATURE", r.Code, r.Message)
	} else if !strings.Contains(r.Message, "statement_kind") {
		t.Fatalf("message %q must name statement_kind", r.Message)
	}
}
```

- [ ] **Step 4: Run to verify it fails**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && go test ./fsm/ -run 'TestSharedStatementVectors|TestVerifyUserJWSV2|TestAdmission_StatementKindIsBound' -v 2>&1 | tail -30`
Expected: FAIL — `TestSharedStatementVectorsAreByteIdenticalToHousegate` passes (the copy is fresh), `TestVerifyUserJWSV2_SharedVectors/valid` fails with `user_jws: statement_kind does not bind the envelope` (the FSM's `want` still leaves the field zero while the vector token signs 1), and `TestAdmission_StatementKindIsBound` fails because `signStatementV2` does not carry the kind either.

- [ ] **Step 5: Implement**

In `fsm/userjws.go`, add the field to the `want` literal (after `RowIDProfileID`):

```go
		TargetTableID:  env.TargetTableID,
		RowIDProfileID: env.RowIDProfileID,
		StatementKind:  uint32(env.StatementKind),
	}
```

In `fsm/admission_test.go`'s `signStatementV2`, add the same line after `RowIDProfileID`:

```go
		RowIDProfileID: env.RowIDProfileID,
		StatementKind:  uint32(env.StatementKind),
	}
```

- [ ] **Step 6: Run to verify it passes**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && bazel test //fsm:fsm_test --test_output=errors`
Expected: PASS — the whole `fsm` suite, including the 17 vector sub-tests, the golden vectors from Task 15, the genesis validation from Task 16, `L3BlockView` from Task 17, and the taxonomy from Task 18. Also confirm the red lines: `bazel query 'deps(//fsm:fsm, 1)' | grep -c arbiter_proto` prints `0`, and `grep -rn 'time\.Now' fsm/` prints nothing.

- [ ] **Step 7: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add fsm/userjws.go fsm/userjws_v2_test.go fsm/admission_test.go fsm/shared_vectors_test.go fsm/testdata/statement_jws_v2.json
git commit -m "feat(fsm): bind statement_kind and link the shared JWS vectors to housegate"
```

### Task 20: chpipeline — decouple the fraud assertion and add the kind-relabel class

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `integration/chpipeline/harness_ops_test.go:163-173` (`statementV2PayloadAt`)
- Modify: `integration/chpipeline/fraud_test.go:88-125`

**Interfaces:**
- Consumes: `auth.StatementPayloadV2Mismatch`, `auth.DecodeStatementV2Payload` (housegate), `verifyUserJWSV2`'s message shape (Task 19).
- Produces: no new symbols.

**What is being fixed.** `fraud_test.go:113` asserts `strings.Contains(ack.GetMessage(), "payload_hash")`. That message is `"user_jws: <first mismatching field> does not bind the envelope"`, and the field is whichever case `auth.StatementPayloadV2Mismatch` reaches first — a housegate-side switch. Reordering that switch would break an arbiter test with no arbiter change, or worse, keep it green while proving something else. The fix asserts two independent things: the fraud *structurally* changes `payload_hash`, and the message names whatever field the comparator itself reports.

- [ ] **Step 1: Write the failing test**

In `integration/chpipeline/fraud_test.go`, replace lines 112-115 (the `strings.Contains(ack.GetMessage(), "payload_hash")` block) with:

```go
	// The fraud must structurally break payload_hash …
	signed, err := auth.DecodeStatementV2Payload(env.UserJWS)
	if err != nil {
		t.Fatalf("decode signed payload: %v", err)
	}
	expected := statementV2PayloadAt(env, signed.Iat)
	if signed.PayloadHash == expected.PayloadHash {
		t.Fatal("the swapped-payload fraud must change payload_hash")
	}
	// … and the rejection must name the field housegate's own comparator
	// reports, so reordering StatementPayloadV2Mismatch cannot silently change
	// what this test proves.
	field := auth.StatementPayloadV2Mismatch(signed, expected)
	if field == "" {
		t.Fatal("a swapped payload must break at least one binding")
	}
	if want := "user_jws: " + field + " does not bind the envelope"; ack.GetMessage() != want {
		t.Fatalf("rejection message = %q, want exactly %q", ack.GetMessage(), want)
	}
```

and append a new fraud class:

```go
// Fifth fraud class (spec K D7): an operator re-labels a signed statement's
// kind. Today v1 admits INSERT only, so the reachable case is a token signed
// for STATEMENT_KIND_UNSPECIFIED attached to an INSERT envelope; the binding
// is what keeps the attack closed once the mutation lane admits more kinds.
func TestPipeline_FraudRelabelsStatementKind(t *testing.T) {
	conn := requireCH(t)
	h := startHarness(t, conn)

	env, _ := h.statement(t, 1, "p0", 601)
	relabelled := env
	relabelled.StatementKind = arbiter.StatementKindUnspecified
	env.UserJWS = signStatementV2At(h.key, relabelled, time.Now().Unix())

	ctx, cancel := context.WithTimeout(h.ctx, 10*time.Second)
	defer cancel()
	var ack *pb.SequencedAck
	err := h.cluster.withLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		var err error
		ack, err = pb.NewArbiterIngressClient(conn).SubmitStatement(ctx, wire.EnvelopeToPB(env))
		return err
	})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if ack.GetCode() != pb.AdmissionCode_ADMISSION_CODE_INVALID_SIGNATURE {
		t.Fatalf("a relabelled kind must be INVALID_SIGNATURE, got %+v", ack)
	}
	if ack.GetMessage() != "user_jws: statement_kind does not bind the envelope" {
		t.Fatalf("rejection message = %q", ack.GetMessage())
	}
	if h.statementStatus(t, env.StatementID.Flat()).GetFound() {
		t.Fatal("a rejected statement must not be sequenced")
	}
}
```

Add `"github.com/housegate/housegate/pkg/auth"` to `fraud_test.go`'s imports.

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && ARBITER_CH_INTEGRATION=1 bazel test //integration/chpipeline:chpipeline_test --test_filter='TestPipeline_Fraud' --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=900 --test_output=errors`
Expected: FAIL — `TestPipeline_FraudRelabelsStatementKind` gets `user_jws: <some other field> …` because `statementV2PayloadAt` still does not sign the kind, so the token and envelope agree on nothing about it.

- [ ] **Step 3: Implement**

In `integration/chpipeline/harness_ops_test.go`, add the field to `statementV2PayloadAt`:

```go
		TargetTableID: env.TargetTableID, RowIDProfileID: env.RowIDProfileID,
		StatementKind: uint32(env.StatementKind),
	}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter && ARBITER_CH_INTEGRATION=1 bazel test //integration/chpipeline:chpipeline_test --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=900 --test_output=errors`
Expected: PASS — the four pre-existing fraud classes, the new fifth, and `TestSignStatementV2AtMatchesRelaySigner` (which now also proves the harness signer and `RelaySigner.SignStatementV2` agree on the new field).

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
git add integration/chpipeline/fraud_test.go integration/chpipeline/harness_ops_test.go
git commit -m "test(chpipeline): assert the bound field structurally and add the kind-relabel fraud class"
```

### Task 21: Full arbiter gates and release v0.3.0

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:** none (release only).

**Interfaces:**
- Produces: arbiter `v0.3.0`, referenced by Spec M's pilot pins.

- [ ] **Step 1: Run every gate CI runs**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter
bazel build //...
bazel query 'deps(//fsm:fsm, 1)' | grep -q 'com_github_sentioxyz_arbiter_proto' && echo "RED LINE VIOLATED" || echo "fsm import red line ok"
grep -rn 'time\.Now' fsm/ && echo "RED LINE VIOLATED" || echo "fsm wall-clock red line ok"
./scripts/anchor-bindings.sh check
bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
```
Expected: both red-line lines print `ok`; `anchor-bindings.sh check` clean; all tests PASS.

- [ ] **Step 2: Run the docker-bound suites**

Run: `ARBITER_CH_INTEGRATION=1 bazel test //integration/... --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=900 --test_output=errors` with ClickHouse 25.8 reachable on `CH_ADDR`.
Expected: PASS.

- [ ] **Step 3: PR, merge, release**

Run: `gh pr create --fill --base main`, merge green, then `gh workflow run cut-release.yml -f version=v0.3.0` and `gh run watch`.
Expected: non-draft, non-prerelease `v0.3.0`; record merge SHA, run URL, tag object SHA and the published image digest in the evidence section.

---

## Phase 5 — sentio-node (spec D8b, pins)

### Task 22: `ErrPayloadMismatch` is a terminal prepare rejection (D8b)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/sentio-node`

**Files:**
- Modify: `storageintegrityadapter/adapter.go:73-77`
- Test: `storageintegrityadapter/adapter_test.go:142-160`

**Interfaces:**
- Consumes: `snode.ErrPayloadMismatch` (arbiter-core, unchanged), `sicore.ErrPrepareTerminalReject` (housegate, unchanged).
- Produces: no new symbols. Behaviour: a payload/envelope binding violation is classified terminal, like `ErrSchemaHashMismatch` and `ErrEncodingNotSupported`.

**Why.** `validatePrepareBindings` and `validatePayloadBinding` reject before any unsafe write, for reasons that cannot change on retry: the request's encoding/revision contradict the signed envelope, the payload bytes do not hash to `payload_hash`, or the envelope changed across prepare attempts. Leaving it non-terminal makes the HouseGate orchestrator hold the source frontier behind a status lookup for a write that provably cannot exist.

- [ ] **Step 1: Flip the expectation (the failing test)**

In `storageintegrityadapter/adapter_test.go`'s `TestPrepareErrorClassificationPreservesSentinels`, change the `snode.ErrPayloadMismatch` row's `terminal` from `false` to `true` and add the case that reaches it through a wrapped error:

```go
	for _, tc := range []struct {
		injected error
		terminal bool
	}{
		{snode.ErrEncodingNotSupported, true},
		{snode.ErrSchemaHashMismatch, true},
		// A binding violation is decided before any unsafe write and cannot
		// change on retry: terminal, so the orchestrator aborts instead of
		// fencing a retry behind a source lookup.
		{snode.ErrPayloadMismatch, true},
		{fmt.Errorf("statement 0xabc:1:x envelope changed across prepare attempts: %w", snode.ErrPayloadMismatch), true},
		{snode.ErrConvergenceForeignRows, false},
		{snode.ErrBackpressure, false},
		{errors.New("dial tcp: connection refused"), false},
	} {
```

(`snode.ErrBackpressure` takes the structured back-pressure branch above and must stay non-terminal; keep it in the table so a future edit cannot quietly reclassify it.)

- [ ] **Step 2: Run to verify it fails**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/sentio-node && go test ./storageintegrityadapter/ -run TestPrepareErrorClassificationPreservesSentinels -v`
Expected: FAIL — `terminal classification for snode: payload does not match envelope`.

- [ ] **Step 3: Implement**

In `storageintegrityadapter/adapter.go`, extend the terminal branch:

```go
		if errors.Is(err, snode.ErrSchemaHashMismatch) ||
			errors.Is(err, snode.ErrEncodingNotSupported) ||
			errors.Is(err, snode.ErrPayloadMismatch) {
			// Terminal before any unsafe write: let the orchestrator abort
			// instead of fencing a retry behind a source lookup. A binding
			// violation (payload hash/length, encoding, revision, or an
			// envelope that changed across attempts) is as unretriable as a
			// schema-hash mismatch.
			return sicore.PreparedLocalResult{}, fmt.Errorf("%w: %w", sicore.ErrPrepareTerminalReject, err)
		}
```

- [ ] **Step 4: Run to verify it passes**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/sentio-node && bazel test //storageintegrityadapter:storageintegrityadapter_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
git add storageintegrityadapter/adapter.go storageintegrityadapter/adapter_test.go
git commit -m "fix(storageintegrityadapter): classify ErrPayloadMismatch as a terminal prepare reject"
```

### Task 23: sentio-node pins, gates, and its first release tag

Working directory: `/Users/uranuswch/Dev/sentio_xyz/sentio-node`

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`

**Interfaces:**
- Consumes: housegate `v0.10.0` (Task 9), arbiter-core `v0.4.0` (Task 13), arbiter-proto `v0.6.0` (Task 2).
- Produces: sentio-node `v0.1.0` — its first tag, so its commits become addressable (roadmap §5).

- [ ] **Step 1: Bump the pins by hand (sentio-node has no update script)**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
go get github.com/housegate/housegate@v0.10.0
go get github.com/sentioxyz/arbiter-core@v0.4.0
go get github.com/sentioxyz/arbiter-proto@v0.6.0
go mod tidy
git ls-remote https://github.com/housegate/housegate refs/tags/v0.10.0^{}
git ls-remote https://github.com/sentioxyz/arbiter-core refs/tags/v0.4.0^{}
```
Then edit `MODULE.bazel`: set the `housegate` `git_override` commit to the first peeled SHA and the `arbiter_core` override to the second, and update the resolved-version comment next to each (`# Resolved arbiter-core v0.4.0; source is pinned by the commit below.`).

Expected: `go.mod` shows `housegate v0.10.0` (was `v0.9.2`, closing the roadmap §5 4-commit gap), `arbiter-core v0.4.0`, `arbiter-proto v0.6.0`.

- [ ] **Step 2: Full gates**

Run:
```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
bazel mod tidy
bazel build //...
bazel test --build_tests_only //... --test_output=errors
bazel test --build_tests_only --@rules_go//go/config:race //storageintegrityadapter:storageintegrityadapter_test --test_output=errors
go test ./... 2>&1 | tail -20
```
Expected: PASS. sentio-node has no code that constructs `auth.JWSStatementPayloadV2`, so `statement_kind` is transparent here; the adapter simply carries the envelope through.

- [ ] **Step 3: Commit, merge, tag**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
git add go.mod go.sum MODULE.bazel
git commit -m "chore: pin housegate v0.10.0, arbiter-core v0.4.0, arbiter-proto v0.6.0"
gh pr create --fill --base main
```
Merge green, then cut the repo's first tag from the merge commit:
```bash
git fetch origin main && git tag -a v0.1.0 -m "sentio-node v0.1.0" origin/main && git push origin v0.1.0
```
Expected: `git ls-remote --tags origin` lists `v0.1.0`; record the merge SHA and tag object SHA in the evidence section.

---

## Phase 6 — documentation (spec delivery step 5)

### Task 24: Spec A §4.2 correction, the canonicalization debt, and Spec K status

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md` (§4.2 payload listing)
- Modify: `docs/superpowers/specs/2026-08-18-storage-integrity-design-v4-reconciliation.md` (Spec B edit list)
- Modify: `docs/superpowers/specs/2026-08-19-storage-integrity-commitment-durability-design.md` (Status)

- [ ] **Step 1: Correct Spec A §4.2**

In `2026-08-18-storage-integrity-signed-envelope-v2-design.md` §4.2, add `"statement_kind": 1,` to the JSON payload listing immediately after `"row_id_profile_id": "housegate-row-id-v1"` (matching the struct's append-only order), and append to the paragraph that follows it:

```
`statement_kind` was missing from this list in the original draft while §2 goal 1 required the signature to bind every field that affects what is executed; it is bound from arbiter-proto v0.6.0 / housegate v0.10.0 onward (Spec K D7). It carries `pb.StatementKind`'s numeric value (1 = INSERT).
```

- [ ] **Step 2: Add the Spec B edit-list items**

In `2026-08-18-storage-integrity-design-v4-reconciliation.md`, append to the base-design §7 item:

```
- Base design §7 signing payload table: add `statement_kind` (numeric `pb.StatementKind`, 1 = INSERT) to the `user_jws_v2` payload. Source: 2026-08-19-storage-integrity-commitment-durability-design.md D7.
- Base design §4.3 / Arbiter design §4.3: record the canonicalization debt. `replay.CanonicalDigest` is `SHA256("housegate-replay-mvp-v0:" ‖ domain ‖ 0x00 ‖ json.Marshal(v))`, so every root — `statements_root`, `ChainHash`, the data/state/manifest roots — depends on Go struct **field declaration order**, not just on the field set. Structural canonicalization (RFC-8785-style, field-order-independent) is the correct long-term fix and is deliberately deferred: adopting it changes every historical root and is a protocol migration, not a hardening fix. Until then the preimages are frozen by golden vectors with committed marshalled bytes — arbiter `fsm/testdata/l3_commitment_golden.json` (`statements_root` `0x72683a2f…a5e82921`, header `ChainHash` `0x168866c1…87ed8bd2`) and arbiter-core `conformance/testdata/statement_envelope_golden.json` — whose digests also live as Go source constants so they cannot be re-blessed by a regenerator. Source: 2026-08-19-storage-integrity-commitment-durability-design.md D1, roadmap §4 decision 3 and §6.
- Arbiter design §4.1 / §5: `fsm.New`/`NewWithNotify`/`Restore` reject empty `network_id` / `schema_snapshot_id` / `executor_profile_id` (`fsm.ErrGenesisParams`); `SafeState.GetL3Block` returns `NotFound` for an unknown seq and `DataLoss` naming the seq when the node's envelope list is shorter than `statement_count` (`fsm.ErrL3BlockIncomplete`).
- Base design §5.1: `settings_hash` is enforced against the enumerated housegate-owned key set, not a `SQL_x_` / `SQL_sentio_` prefix; the signed deferred-INSERT lane refuses named client Data blocks (external temporary tables); the SNode verifies `schema_hash` against its current schema source before serving a cached prepare result.
```

- [ ] **Step 3: Flip Spec K's status**

In `2026-08-19-storage-integrity-commitment-durability-design.md`, change `**Status:** Proposed` to `**Status:** Implemented (plan docs/superpowers/plans/2026-08-19-storage-integrity-commitment-durability.md)` and update the **Code base** line to the tags this plan produced: `arbiter v0.3.0, arbiter-core v0.4.0, arbiter-proto v0.6.0, housegate v0.10.0, sentio-node v0.1.0`.

- [ ] **Step 4: Commit**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git add docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md docs/superpowers/specs/2026-08-18-storage-integrity-design-v4-reconciliation.md docs/superpowers/specs/2026-08-19-storage-integrity-commitment-durability-design.md
git commit -m "docs(storage-integrity): correct the v2 payload list and record the canonicalization debt"
```

---

## Self-review

**1. Spec coverage** — every decision and every acceptance bullet maps to at least one task (see the map below).

Gaps found and closed while reviewing:

- **The spec's delivery order (§5) implies two arbiter releases and two arbiter-core releases** (step 1 touches both, step 4 touches both again). This plan collapses each repo to a single release by running the D7 wave first in the repos that produce artefacts (arbiter-proto → housegate) and then landing D1/D2/D3/D8a together with the D7 consumption side in arbiter-core and arbiter. That is safe here because **no task in this plan changes the field set or order of `arbiter.StatementEnvelope` or `fsm.L3BlockHeader`**, so the D1 freeze is not racing its own plan; it also matches the D7 sequencing the spec explicitly requires (proto → housegate → arbiter-core → arbiter → sentio-node). Recorded as a deliberate deviation.
- **Spec 1b is partly stale against `71657a8`.** `cmd/arbiter/app.go:37` already calls `cfg.Validate()`, and `applyTestDefaults` sets `Genesis.NetworkID`, so the "`run` never re-validates / `main_test` proves the bypass" framing no longer holds. The live gap is the one D2 actually fixes (`fsm.New`/`NewWithNotify` accept empty Params; `server/gateway_test.go`, `server/server_test.go`, `raftnode/node_test.go` construct FSMs with `NetworkID: ""` today). Task 16 states the correction inline and still adds the `run`-rejects-invalid-config test the acceptance list asks for.
- **The spec's D7 says "bump arbiter-proto's minor" without a proto change to hang it on.** `statement_kind` is already field 2. Task 1 explains the bump as the coordination signal (the module version is the only one the five repos share) and gives the proto a real, testable change: a comment recording the binding, plus a conformance test pinning field number 2 and enum value 1 — the two numbers the signing payload now depends on.
- **The spec asks whether to add a build-time link between the two vector copies.** Decided and justified in Task 7: an exported `auth.SharedStatementVectorsSHA256` rather than a Bazel `filegroup`, because it works under plain `go test` too, needs no hand edits in gazelle-generated BUILD files, and fires at the pin bump — the exact coordinated-release moment D7 requires. The residual gap (arbiter does not see housegate-side regeneration until it bumps the pin) is recorded there.
- **D1's "marshalled bytes so a diff shows what changed"** needed a fixture format decision. Chosen: a pretty-printed `value` block in testdata compared through `json.Compact` against `json.Marshal` of the Go fixture, with the digest duplicated as a Go source constant. No regenerator exists, deliberately — a `-write` mode would let a routine "refresh the goldens" step launder an encoding break, which is the failure the task exists to prevent.
- **D3 needed an error taxonomy the spec left open.** Chosen: `ErrL3BlockNotFound` → `codes.NotFound`, `ErrL3BlockIncomplete` → `codes.DataLoss` naming the seq. `DataLoss` because the block exists and this node's copy of it is short — an auditor must not read that as the block's content. `L3BlockView`'s `ok bool` becomes an `error` so the two are distinguishable; the two call sites and three tests are enumerated in Task 17.
- **D6 needed an import-graph decision** for `SQL_x_read_mode`, whose canonical constant lives in `pkg/rewriter` (grpc + FFI deps). Chosen: duplicate the constant in `pkg/storageintegrity` and assert equality from `pkg/rewriter`, which already sits above both. Justified in Task 5.

**2. Placeholder scan** — no "TBD", "TODO", "implement later", "add appropriate error handling", or "write tests for the above". Every code step carries the code; every digest is a literal computed against the code base named in the header, never an instruction to compute one. The two places an executor must exercise judgment are named explicitly: Task 16's per-package `mustFSM` helper (the shape is given; the exact placement is per test package) and Task 24's Spec B section (append to the existing base-design §7 item, or create it if that document has no such item yet). Task 22's error-table row for `snode.ErrBackpressure` is retained on purpose so a future edit cannot quietly reclassify it.

**3. Type / name consistency (checked across tasks)** — `chproto.ClientDataInfo` / `chproto.InspectClientDataPacket` (Task 3 defines, Task 4 uses; `ClientDataPacketIsEmpty` keeps its signature for `relay.go:1068`). `sicore.ReadModeSettingKey` / `HousegateOwnedSettingKeys` / `IsHousegateOwnedSettingKey` / `RejectUserSettings` (Task 5; the three existing call sites in `pkg/plugins/{storageintegrity,sistatement}` are unchanged). `sicore.StatementKindCodeInsert` / `sicore.StatementKindCode` (Task 6 defines; Task 8 uses both). `auth.JWSStatementPayloadV2.StatementKind` (Task 6 defines; Tasks 7, 8, 19, 20 use). `auth.StatementPayloadV2Mismatch`'s new `"statement_kind"` case (Task 6; asserted in Tasks 7, 19, 20). `auth.SharedStatementVectorsSHA256` = `6af5c9cc34d6b083935d804799138e059ce7da99fb034a2e0332b3c7ce8737bc` (Task 7 defines; Task 19 asserts). `fsm.ErrGenesisParams` / `(Params).Validate` / two-valued `New` / `NewWithNotify` / three-valued `newFSM` (Task 16; call sites enumerated). `fsm.ErrL3BlockNotFound` / `fsm.ErrL3BlockIncomplete` / `L3BlockView(...) (…, error)` / `DeleteStatementForTest` (Task 17 defines; `server/safestate.go` and `server/safestate_l3block_test.go` updated in the same task). `(*Role).resolveEnvelopeSchema` (Task 12, arbiter-core only). `goldenStatementsRoot` = `0x72683a2f…a5e82921` appears in two different packages (arbiter-core `conformance`, arbiter `fsm`) with the same value on purpose — the header fixture's `statements_root` is that value, which is what proves the two repos hash the same preimage. Existing helpers reused rather than renamed: `testAccount` / `validEnvelope` / `submit` / `signStatementV2` (arbiter `fsm`), `newTestFSM` / `registerActive` / `sealBlock` (arbiter `fsm`), `newV2Ingress` / `v2Statement` / `withStatementToken` / `signedQueryContext` (housegate ingress tests), `newTestPlugin` / `insertQctx` / `encodeRows` / `declareSchema` (housegate `sistatement` tests), `newDeferredHarness` / `encodeInsertQuery` / `encodeEmptyClientData` / `encodeNonEmptyClientDataPacket` / `encodeServerSampleDataPacket` / `readExact` / `writeAllConn` (housegate relay tests), `requireCH` / `intakeSchema` / `newIntakeHarness` / `stagedRequest` / `nativePayload` / `countActiveParts` (arbiter-core `snode` tests), `h.statement` / `statementV2PayloadAt` / `signStatementV2At` / `h.statementStatus` (arbiter chpipeline).

**4. Task ordering vs. red/green** — Task 14 (arbiter pin bump) deliberately leaves `//fsm:fsm_test` red on `TestVerifyUserJWSV2_SharedVectors` until Task 19; Tasks 15–18 use `--test_filter` and the plan says so at each step. That is the only intentionally-red interval, and it is inside one repo's single PR.

## Spec coverage map

| Spec section | Requirement | Tasks |
|---|---|---|
| §1 1a / §3 D1 | `statements_root` preimage frozen by golden vectors with marshalled bytes; mirrored in arbiter-core; canonicalization debt recorded | 11, 15, 24 |
| §1 1b / §3 D2 | `Params.NetworkID` (+ `SchemaSnapshotID`, `ExecutorProfileID`) validated at `fsm.New` / `NewWithNotify` / `Restore`; config validation stays | 16 |
| §1 1c / §3 D3 | `L3BlockView` errors on an incomplete block; `GetL3Block` maps it, naming the seq | 17 |
| §1 1d / §3 D4 | replay and fresh prepare share one validation; `schema_hash` checked above the cached-result branch | 12 |
| §1 1e / §3 D5 | chproto exposes the block name; the deferred lane refuses non-empty **and** empty named Data blocks | 3, 4 |
| §1 1f / §3 D6 | `settings_hash` enforced against the enumerated owned key set; rejection names the key | 5 |
| §1 1g / §3 D7 | `statement_kind` in `JWSStatementPayloadV2`, `StatementPayloadV2Mismatch`, and both `want` constructions; proto minor bump; regenerated shared vectors; coordinated releases | 1, 2, 6, 7, 8, 9, 10, 14, 19, 20, 23, 24 |
| §1 last ¶ / §3 D8a | non-canonical protected header wrapped in `errMalformedUserJWS`; test asserts the code, not `err != nil` | 18 |
| §1 last ¶ / §3 D8b | `snode.ErrPayloadMismatch` → `ErrPrepareTerminalReject` in sentio-node's adapter | 22 |
| §2 goal 1 | anchored preimage pinned by a test that fails on any encoding change | 11, 15 |
| §2 goal 2 | network binding cannot degrade to a no-op | 16 |
| §2 goal 3 | audit read surface complete or errors | 17 |
| §2 goal 4 | replay and deferred-input paths as strict as the fresh paths | 4, 12 |
| §2 goal 5 | `settings_hash` enforced against the actual owned key set | 5 |
| §2 goal 6 | `statement_kind` bound before the mutation lane exists | 6, 8, 19 |
| §2 non-goals | `CanonicalDigest` unchanged; row-id/row-element profile unchanged; no mutation lane; `Apply` stays wall-clock-free | Global Constraints; 15 step 3, 16 step 5, 19 step 6 red-line checks |
| §4 bullet 1 | golden vectors fail on a reorder — proven by reordering locally, observing red, reverting | 11 step 4, 15 step 4 |
| §4 bullet 2 | `New`/`Restore` reject empty `NetworkID`; the `run()`-with-literal-config test updated / rejection test added | 16 |
| §4 bullet 3 | `GetL3Block` on a synthetically incomplete block errors and names the seq | 17 |
| §4 bullet 4 | a cached prepare whose role schema changed returns the terminal schema-hash rejection | 12 |
| §4 bullet 5 | deferred INSERT preceded by an external-table block rejected before signing, buffer released | 4 |
| §4 bullet 6 | unsigned `SQL_x_anything` rejected naming the key; the owned keys accepted | 5 |
| §4 bullet 7 | shared vectors regenerated with `statement_kind`; both repos consume them; reject vectors assert the failing field name | 7, 19 |
| §4 bullet 8 | the chpipeline `strings.Contains(msg,"payload_hash")` coupling replaced by a structured field assertion | 20 |
| §5 delivery 1 | arbiter + arbiter-core: D1, D2, D3, D8a | 11, 15, 16, 17, 18 |
| §5 delivery 2 | arbiter-core: D4 (+ the `ErrPayloadMismatch` classification sentio-node consumes) | 12, 22 |
| §5 delivery 3 | housegate: D5, D6 | 3, 4, 5 |
| §5 delivery 4 | arbiter-proto + housegate + arbiter + arbiter-core + sentio-node: D7 as a coordinated minor bump | 1, 2, 6–10, 14, 19, 20, 23 |
| §5 delivery 5 | Spec B edit list: §4.2 field-list correction + canonicalization-debt note | 24 |
| Roadmap §4 decision 3 | golden vector, not a canonical-JSON encoder | 11, 15, 24 |
| Roadmap §5 | arbiter / arbiter-core off housegate v0.9.0; sentio-node off v0.9.2; sentio-node's first tag | 10, 14, 23 |
| Roadmap §3 | K merges after Spec J's CI change so the new tests run in CI | Global Constraints |
