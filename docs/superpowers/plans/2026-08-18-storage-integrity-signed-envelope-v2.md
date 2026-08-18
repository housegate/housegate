# Signed Statement Envelope v2 and Agent-Side Payload Commitment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the user signature bind every executed-and-attributed field of a storage-integrity INSERT (statement id, sql/settings/schema/payload hashes, payload format+length, client revision, target table, network, shard, row-id profile), with the agent-mode HouseGate answering the INSERT sample block itself so it can hash the payload before forwarding; commit sequenced envelopes into the L3 chain hash; verify all bindings deterministically in the Arbiter FSM; and switch the canonical stored payload to the exact ClickHouse Native wire bytes (deleting the CSV bridge).

**Architecture:** arbiter-proto grows the `StatementEnvelopeV2` v2 fields, `replay.Statement.payload_format/client_revision/schema_hash`, and a `SafeState.GetL3Block` RPC (v0.5.0). housegate adds a domain-separated `housegate-statement-v2` JWS in `pkg/auth`, a Relay "deferred-INSERT" mode (`Codec.WriteSampleBlock` + `QueryContext.DeferredInsert`) that lets a QueryPlugin answer the sample block locally, buffer client Data, and only then forward, a new agent-only plugin `pkg/plugins/sistatement` that produces v2 envelopes, and an ingress that validates the v2 token against its own capture (no CSV conversion). arbiter-core mirrors the fields, decodes Native in the SNode, verifies `schema_hash` in the verifier; arbiter admits v2 only, computes `statements_root`, bumps the snapshot version, exposes `GetL3Block`, and proves the "ingress swaps payload after signing" fraud class in chpipeline; sentio-node carries the fields and drops the CSV bridge wiring. Hard cutover, no dual-read.

**Tech Stack:** Go 1.26 (Bazel 9 + Bzlmod in housegate/arbiter-core/arbiter/sentio-node; buf + `go test` in arbiter-proto), ClickHouse native TCP protocol via the sentioxyz ch-go v0.73 fork, secp256k1 JWS (go-ethereum crypto), gRPC/protobuf.

**Spec:** `docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md` (Spec A) + roadmap `docs/superpowers/specs/2026-08-18-storage-integrity-v1-closure-roadmap.md` §4 decisions 1–4. Base specs: `docs/superpowers/specs/housegate-storage-integrity-insert/2026-07-16-housegate-storage-integrity-signed-ingress-design.md`, `.../2026-07-20-housegate-storage-integrity-p1e-runtime-e2e-design.md`.

## Global Constraints

- Hard cutover: envelope v2 replaces v1; arbiter-proto minor bump `v0.4.0 → v0.5.0`; arbiter FSM `snapshotVersion = 2`; no dual-read (spec §1, §7).
- `payload_format` for the SI lane is exactly `clickhouse-native-data-v1`: concatenation, in arrival order, of the de-chunked `Packet.Raw` bytes of every non-empty client `Data` packet of the INSERT; the terminal empty block is excluded; compressed blocks are rejected (spec D1).
- `settings_hash = replay.CanonicalDigest("housegate-settings-v1", []string{})` = `0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006` (constant `storageintegrity.EmptySettingsHash`); after stripping `SQL_x_*` and `SQL_sentio_*` keys the SI lane admits **no** client settings and names the offending setting on rejection (spec D4).
- `schema_hash = payloadexec.TableSchemaHash(networkID, schema)` (Phase B, unchanged); `sql_hash = replay.DigestString(sql)`; `payload_hash = replay.DigestBytes(payload)`; `row_id_profile_id = "housegate-row-id-v1"`; `keeper_shard_id = 0` in v1; `envelope_version = 2` (spec §4.3, §7).
- JWS v2: compact serialization, header `{"alg":"ES256K","typ":"JWT"}`, `purpose = "housegate-statement-v2"`, setting key `SQL_x_statement_token`; the legacy `SQL_x_auth_token` query JWS and `SignToken`/`ValidateQuery` stay untouched (spec D3, §4.2).
- Relay invariants: one query in flight; the client codec's `proto.Reader` is never replaced; cross-codec forwarding uses `WriteRawPacket`, never `Splice`; the deferred buffer is released on every exit path; `DeferredInsert` and `SuppressUpstreamExecution` are mutually exclusive on one query (spec §5.2).
- Arbiter FSM red lines: `fsm/` must not import `arbiter-proto/gen/pb` and must not call `time.Now` (CI tripwires); v2 admission is wall-clock-free — the `iat` window stays at the ingress edge (spec §7).
- housegate conventions (CLAUDE.md): Bazel is the ground truth (`bazel test //...`); after adding packages/deps run `bazel mod tidy && bazel run //:gazelle`; module path `github.com/housegate/housegate`; logging via `pkg/log` (`Infow`/`Warnw`), errors wrapped with `%w`, English identifiers/comments, no hard-wrapped Markdown; docker-bound integration targets are tagged `manual` and must be added to `.github/workflows/ci.yml`'s explicit list.
- Cross-repo pins are bumped to in-progress commits mid-flight (housegate `go get github.com/sentioxyz/arbiter-proto@<sha>`; arbiter-core/arbiter `scripts/update-housegate.sh <sha>` / `scripts/update-arbiter-core.sh <sha>` + `go get` for arbiter-proto; sentio-node manual `go.mod` + `MODULE.bazel` git_override) and re-pinned to tagged releases at the end of each phase.

---

## File Structure (what is created / modified, by repo)

**arbiter-proto** (`/Users/uranuswch/Dev/sentio_xyz/arbiter-proto`)
- Modify `proto/arbiter.proto` — `StatementEnvelopeV2` fields 11–17; new `L3BlockHeader`, `L3BlockRef`, `L3Block` messages; `SafeState.GetL3Block` rpc.
- Modify `proto/replay.proto` — `Statement.payload_format = 11`, `client_revision = 12`, `schema_hash = 13`.
- Regenerate `gen/pb/*.pb.go` (committed).

**housegate** (`/Users/uranuswch/Dev/housegate/housegate`)
- Modify `pkg/auth/types.go`, `relay_signer.go`, `eth_validator.go`, `signer.go` — v2 payload/signer/validator; new `pkg/auth/statement_v2_test.go`, `pkg/auth/testdata/statement_jws_v2.json`.
- Modify `pkg/replay/types.go` (Statement fields), `pkg/replay/verifier.go` (schema-hash source), `pkg/replay/payloadexec/exports.go` (`RowIDProfileID`, `PayloadFormatCSVWithNames`).
- Create `pkg/replay/nativepayload/{native.go,native_test.go}` (moved from `pkg/storageintegrity/native_payload*.go`); modify `pkg/replay/chexec/materializer.go` (format branch).
- Modify `pkg/storageintegrity/{native_payload.go (aliases),sql.go,materializer_select.go,intake.go,arbiter_proto.go}`; create `pkg/storageintegrity/settings.go`; delete `pkg/storageintegrity/csv_payload.go` + test.
- Modify `pkg/chproto/codec.go` (+`WriteSampleBlock`), `pkg/plugin/context.go` (`DeferredInsertPlan`), `pkg/proxy/relay.go` (deferred mode) + tests.
- Create `pkg/plugins/sistatement/{plugin.go,seq.go,sqlparse.go,*_test.go}`; modify `pkg/config/storage_integrity_config.go`, `pkg/config/config.go`, `build.go`, `proxy.go` (agent wiring + `Options.StorageIntegrityTableSchemas`).
- Modify `pkg/plugins/storageintegrity/plugin.go` (v2 validation), `storage_integrity_ingress.go`, `storage_integrity_runtime.go`, `build.go` (ingress wiring), `pkg/integration/storage_integrity_agent_test.go` (new docker test), `.github/workflows/ci.yml` (no change needed — the target is already `//pkg/integration:integration_test`), `CLAUDE.md`.

**arbiter-core** (`/Users/uranuswch/Dev/sentio_xyz/arbiter-core`)
- Modify `types.go`, `domains.go`, `wire/convert.go`, `wire/dispatch.go`, `conformance/*_test.go`, `snode/staged.go` (+tests), `verifier/backends.go` (+tests), `MODULE.bazel`, `go.mod`, root `BUILD.bazel` (gazelle resolve for `pkg/replay/nativepayload` and `pkg/auth`).

**arbiter** (`/Users/uranuswch/Dev/sentio_xyz/arbiter`)
- Modify `fsm/admission.go`, `fsm/userjws.go` (+`verifyUserJWSV2`), `fsm/state.go` (`Params.NetworkID`, `L3BlockHeader.StatementsRoot`), `fsm/apply.go`, `fsm/snapshot.go`, `fsm/reads_dispatch.go`, `fsm/reads.go` (+`L3BlockView`), `server/safestate.go` (+`GetL3Block`), `config/config.go`, `cmd/arbiter/services.go`, `integration/chpipeline/*` (native payloads + fourth fraud class), `fsm/testdata/statement_jws_v2.json` (copied vector), root `BUILD.bazel` (gazelle resolve for `pkg/auth`, `pkg/replay/nativepayload`).

**sentio-node** (`/Users/uranuswch/Dev/sentio_xyz/sentio-node`)
- Modify `storageintegrityadapter/adapter.go` (+test), `standalone/standalone.go`, `config/config.go` (ingress network id passthrough), `go.mod`, `MODULE.bazel`.

## Cross-task interface summary (names every task must agree on)

```go
// pkg/auth
const StatementPurposeV2 = "housegate-statement-v2"
const StatementTokenSettingKey = "SQL_x_statement_token"
type JWSStatementPayloadV2 struct { Purpose string; Iat int64; NetworkID string; KeeperShardID uint32; StatementID string; SQLHash string; SettingsHash string; SchemaHash string; PayloadHash string; PayloadLength uint64; PayloadFormat string; ClientRevision uint32; TargetTableID string; RowIDProfileID string }
func (s *RelaySigner) SignStatementV2(p JWSStatementPayloadV2) (string, error)
func (v *EthValidator) ValidateStatementV2(token string, want JWSStatementPayloadV2) (string, error)
func StatementPayloadV2Mismatch(got, want JWSStatementPayloadV2) string // "" when every bound field is equal (Iat ignored, Purpose compared)
type StatementSignerV2 interface { Address() string; SignStatementV2(JWSStatementPayloadV2) (string, error) }
type StatementValidatorV2 interface { ValidateStatementV2(token string, want JWSStatementPayloadV2) (string, error) }

// pkg/replay
type Statement struct { ...; PayloadFormat string `json:"payload_format,omitempty"`; ClientRevision uint32 `json:"client_revision,omitempty"`; SchemaHash string `json:"schema_hash,omitempty"` }
type SchemaHashSource interface { TableSchemaHash(tableID string) (string, bool) }
Verifier.SchemaHashes SchemaHashSource // optional; mismatch → signed receipt with MatchSourceRoot=false

// pkg/replay/payloadexec
const RowIDProfileID = "housegate-row-id-v1"
const PayloadFormatCSVWithNames = "csv-with-names-v1"

// pkg/replay/nativepayload
const PayloadFormat = "clickhouse-native-data-v1"
var ErrUnsupported error
type Materializer struct { NetworkID string; Revision int }
func Decode(schema payloadexec.TableSchema, revision int, payload []byte) ([]payloadexec.Row, error)
func ValidateDecodable(schema payloadexec.TableSchema, revision int, payload []byte) error

// pkg/storageintegrity (sicore)
const EmptySettingsHash = "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006"
const SettingsHashDomain = "housegate-settings-v1"
const EnvelopeVersionV2 uint32 = 2
func IsHousegateOwnedSettingKey(key string) bool
func RejectUserSettings(keys []string) error
AdmissionRecord / StatementEnvelope: + EnvelopeVersion uint32, NetworkID string, KeeperShardID uint32, SettingsHash string, SchemaHash string, RowIDProfileID string (PayloadEncoding == payload_format, Revision == client_revision)

// pkg/chproto
type SampleColumn struct { Name, Type string }
func (c *Codec) WriteSampleBlock(cols []SampleColumn) error

// pkg/plugin
type DeferredInsertPlan struct { SampleColumns []chproto.SampleColumn; MaxPayloadBytes uint64 }
QueryContext.DeferredInsert *DeferredInsertPlan

// pkg/plugins/sistatement
type Options struct { Signer auth.StatementSignerV2; Schemas registry.TableSchemas; NetworkID string; KeeperShardID uint32; Seq *SeqCounter; MaxPayloadBytes uint64 }
func New(opts Options) (*Plugin, error)
var ErrClientSeqExhausted error
func OpenSeqCounter(stateDir, account string) (*SeqCounter, error); func (c *SeqCounter) Next() (uint64, error); func (c *SeqCounter) AdvanceTo(seq uint64) error; func (c *SeqCounter) Last() uint64

// pkg/config
StorageIntegrityConfig.Agent StorageIntegrityAgentConfig{Enabled bool; NetworkID string; KeeperShardID uint32; StateDir string; MaxPayloadBytes uint64; RequireNetworkState bool}
StorageIntegrityIngressConfig.NetworkID string

// root package
Options.StorageIntegrityTableSchemas registry.TableSchemas // optional override; else type-assert the registry

// arbiter-core
arbiter.StatementEnvelope: + EnvelopeVersion uint32 `json:"envelope_version"`, NetworkID `json:"network_id"`, KeeperShardID uint32 `json:"keeper_shard_id"`, PayloadFormat `json:"payload_format"`, ClientRevision uint32 `json:"client_revision"`, SchemaHash `json:"schema_hash"`, RowIDProfileID `json:"row_id_profile_id"`
const DomainL3Statements = "arbiter-l3-statements-v1"
snode.PrepareRequest.PayloadEncoding == "clickhouse-native-data-v1"; new sentinel snode.ErrSchemaHashMismatch

// arbiter
fsm.Params.NetworkID string `json:"network_id"`; fsm.L3BlockHeader.StatementsRoot string `json:"statements_root"`; snapshotVersion = 2
func verifyUserJWSV2(env arbiter.StatementEnvelope) error
func (f *FSM) L3BlockView(seq uint64) (L3BlockHeader, string, []arbiter.StatementEnvelope, bool)
```

---

## Phase 1 — arbiter-proto (spec §10 step 1)

### Task 1: Envelope v2 fields, replay Statement fields, `GetL3Block` RPC

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-proto`

**Files:**
- Modify: `proto/arbiter.proto` (`StatementEnvelopeV2` at ~:50, `SafeState` service at ~:414)
- Modify: `proto/replay.proto` (`Statement` at ~:22)
- Regenerate: `gen/pb/arbiter.pb.go`, `gen/pb/arbiter_grpc.pb.go`, `gen/pb/replay.pb.go`
- Test: `conformance/envelope_v2_test.go` (new)

**Interfaces:**
- Produces: `pb.StatementEnvelopeV2{EnvelopeVersion uint32; NetworkId string; KeeperShardId uint32; PayloadFormat string; ClientRevision uint32; SchemaHash string; RowIdProfileId string}` (fields 11–17), `pb.Statement{PayloadFormat string; ClientRevision uint32; SchemaHash string}` (fields 11–13), `pb.L3BlockHeader`, `pb.L3BlockRef`, `pb.L3Block`, `SafeStateClient.GetL3Block(ctx, *pb.L3BlockRef) (*pb.L3Block, error)`.

- [x] **Step 1: Write the failing conformance test**

Create `conformance/envelope_v2_test.go`:

```go
package conformance

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// TestStatementEnvelopeV2HasSignedV2Fields pins the field numbers of the
// v2 additions: an accidental renumber would silently break every stored
// envelope, so the numbers are asserted, not just the names.
func TestStatementEnvelopeV2HasSignedV2Fields(t *testing.T) {
	want := map[string]int32{
		"envelope_version":  11,
		"network_id":        12,
		"keeper_shard_id":   13,
		"payload_format":    14,
		"client_revision":   15,
		"schema_hash":       16,
		"row_id_profile_id": 17,
	}
	fields := (&pb.StatementEnvelopeV2{}).ProtoReflect().Descriptor().Fields()
	for name, number := range want {
		fd := fields.ByName(protoreflectName(name))
		if fd == nil {
			t.Fatalf("StatementEnvelopeV2 is missing field %q", name)
		}
		if int32(fd.Number()) != number {
			t.Fatalf("StatementEnvelopeV2.%s number = %d, want %d", name, fd.Number(), number)
		}
	}
}

func TestReplayStatementCarriesFormatRevisionSchemaHash(t *testing.T) {
	want := map[string]int32{"payload_format": 11, "client_revision": 12, "schema_hash": 13}
	fields := (&pb.Statement{}).ProtoReflect().Descriptor().Fields()
	for name, number := range want {
		fd := fields.ByName(protoreflectName(name))
		if fd == nil {
			t.Fatalf("Statement is missing field %q", name)
		}
		if int32(fd.Number()) != number {
			t.Fatalf("Statement.%s number = %d, want %d", name, fd.Number(), number)
		}
	}
}

func TestSafeStateExposesGetL3Block(t *testing.T) {
	svc := pb.File_arbiter_proto.Services().ByName("SafeState")
	if svc == nil {
		t.Fatal("SafeState service missing")
	}
	m := svc.Methods().ByName("GetL3Block")
	if m == nil {
		t.Fatal("SafeState.GetL3Block missing")
	}
	if got := string(m.Input().FullName()); got != "arbiter.L3BlockRef" {
		t.Fatalf("GetL3Block input = %s, want arbiter.L3BlockRef", got)
	}
	if got := string(m.Output().FullName()); got != "arbiter.L3Block" {
		t.Fatalf("GetL3Block output = %s, want arbiter.L3Block", got)
	}
	// L3Block must carry the sealed envelopes so an auditor can recompute
	// statements_root from the same canonical form the FSM hashed.
	var _ proto.Message = &pb.L3Block{}
	if (&pb.L3Block{}).ProtoReflect().Descriptor().Fields().ByName("statements") == nil {
		t.Fatal("L3Block.statements missing")
	}
}
```

Add the tiny helper to the same file (protoreflect name type conversion):

```go
import "google.golang.org/protobuf/reflect/protoreflect"

func protoreflectName(s string) protoreflect.Name { return protoreflect.Name(s) }
```

(Put the `protoreflect` import into the file's import block.)

- [x] **Step 2: Run to verify it fails**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter-proto && go test ./conformance/ -run 'TestStatementEnvelopeV2HasSignedV2Fields|TestReplayStatementCarriesFormatRevisionSchemaHash|TestSafeStateExposesGetL3Block' -count=1`
Expected: compile error `pb.L3Block undefined` (or FAIL "missing field envelope_version").

- [x] **Step 3: Edit the protos**

In `proto/arbiter.proto` replace the `StatementEnvelopeV2` message body with:

```proto
message StatementEnvelopeV2 {
  StatementID statement_id = 1;
  StatementKind statement_kind = 2;
  string sql = 3;
  // DigestString(sql) under the housegate-replay-mvp-v0 profile.
  string sql_hash = 4;
  // v2: CanonicalDigest("housegate-settings-v1", []) — the constant empty
  // user-settings digest; the SI lane admits no client settings in v1.
  string settings_hash = 5;
  string payload_ref = 6;
  string payload_hash = 7;
  uint64 payload_length = 8;
  string target_table_id = 9;
  // The user's signature binding the statement (JWS compact form). v2:
  // purpose "housegate-statement-v2" over every signed field below.
  string user_jws = 10;
  // ---- v2 additions (all signed by user_jws) ----
  uint32 envelope_version = 11;   // must be 2
  string network_id = 12;         // must equal the Arbiter genesis network id
  uint32 keeper_shard_id = 13;    // must be 0 in v1 (Sharder returns group 0)
  string payload_format = 14;     // "clickhouse-native-data-v1"
  uint32 client_revision = 15;    // ClickHouse client protocol revision the Native blocks were encoded for
  string schema_hash = 16;        // Phase-B TableSchemaHash the payload was encoded against
  string row_id_profile_id = 17;  // "housegate-row-id-v1"
}
```

Append after `message BlockRef { ... }`:

```proto
// L3BlockHeader mirrors arbiter fsm.L3BlockHeader (json tags == field
// names) so auditors can recompute ChainHash / statements_root.
message L3BlockHeader {
  uint64 l3_block_seq = 1;
  string prev_l3_hash = 2;
  uint64 statement_seq_start = 3;
  uint32 statement_count = 4;
  string schema_snapshot_id = 5;
  string executor_profile_id = 6;
  string prev_safe_snapshot_id = 7;
  string prev_state_root = 8;
  string spent_ids_root_after = 9;
  // CanonicalDigest("arbiter-l3-statements-v1", []StatementEnvelope) over the
  // sealed block's envelopes in statement_seq order.
  string statements_root = 10;
  AnchorRef l2_anchor_ref = 11;
}

message L3BlockRef {
  uint64 l3_block_seq = 1;
}

message L3Block {
  L3BlockHeader header = 1;
  string chain_hash = 2;
  // Envelopes in statement_seq order (statement_seq_start .. +statement_count).
  repeated StatementEnvelopeV2 statements = 3;
}
```

In the `SafeState` service add:

```proto
  // Sealed L3 block header + envelopes for auditing statements_root / ChainHash.
  rpc GetL3Block (L3BlockRef) returns (L3Block) {}
```

In `proto/replay.proto` append to `message Statement`:

```proto
  // v2 additions — additive, JSON-tag frozen against pkg/replay.Statement.
  string payload_format = 11;
  uint32 client_revision = 12;
  string schema_hash = 13;
```

- [x] **Step 4: Regenerate, lint, breaking-check, test**

Run: `cd /Users/uranuswch/Dev/sentio_xyz/arbiter-proto && make tools && make proto && make lint && make breaking && make test`
Expected: `buf breaking` passes (append-only), `go test ./...` PASS including the three new tests.

- [x] **Step 5: Commit**

```bash
git add proto/arbiter.proto proto/replay.proto gen/pb conformance/envelope_v2_test.go
git commit -m "feat(proto): statement envelope v2 signed fields, replay statement format/revision/schema_hash, SafeState.GetL3Block"
git rev-parse HEAD   # record as ARBITER_PROTO_SHA for Task 3
```

### Task 2: Tag arbiter-proto v0.5.0

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-proto`

- [x] **Step 1: Confirm main is at the Task 1 commit and clean**

Run: `git status --short && git log --oneline -1`
Expected: no output from status; the Task 1 commit on top.

- [x] **Step 2: Tag and push (only after the branch is merged to main; if working on a branch, do this task after merge)**

```bash
git tag -a v0.5.0 -m "arbiter-proto v0.5.0: statement envelope v2, replay statement payload_format/client_revision/schema_hash, SafeState.GetL3Block"
git push origin main --tags
```

- [x] **Step 3: Verify the module resolves**

Run: `cd /tmp && GOFLAGS=-mod=mod go list -m github.com/sentioxyz/arbiter-proto@v0.5.0`
Expected: `github.com/sentioxyz/arbiter-proto v0.5.0`.

---

## Phase 2 — housegate foundations (spec §10 step 2)

### Task 3: Point housegate at the new arbiter-proto

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel.lock`

- [x] **Step 1: Bump the module (commit pseudo-version now; Task 21 re-pins to the tag)**

```bash
go get github.com/sentioxyz/arbiter-proto@$ARBITER_PROTO_SHA   # or @v0.5.0 once Task 2 is done
go mod tidy
bazel mod tidy
bazel run //:gazelle
```

- [x] **Step 2: Verify the new fields are visible to housegate**

Run: `bazel build //pkg/storageintegrity:storageintegrity && go doc github.com/sentioxyz/arbiter-proto/gen/pb.StatementEnvelopeV2 | grep -c 'EnvelopeVersion\|RowIdProfileId'`
Expected: build OK; grep prints `2`.

- [x] **Step 3: Commit**

```bash
git add go.mod go.sum MODULE.bazel.lock
git commit -m "chore(deps): bump arbiter-proto for statement envelope v2"
```

### Task 4: `pkg/auth` v2 statement JWS — payload, signer, validator

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/auth/types.go`, `pkg/auth/relay_signer.go`, `pkg/auth/eth_validator.go`, `pkg/auth/signer.go`
- Test: `pkg/auth/statement_v2_test.go` (new)

**Interfaces:**
- Consumes: existing `keccak256`, `keccak256Hex`, `recoverAddress`, `RelaySigner.privateKey`, `EthValidator.{AllowedAddresses,MaxTokenAge,Enabled}`.
- Produces: `StatementPurposeV2`, `StatementTokenSettingKey`, `JWSStatementPayloadV2`, `StatementPayloadV2Mismatch`, `(*RelaySigner).SignStatementV2`, `(*EthValidator).ValidateStatementV2`, `StatementSignerV2`, `StatementValidatorV2` (used by Tasks 5, 16, 19 and arbiter Task 30).

- [x] **Step 1: Write the failing tests**

Create `pkg/auth/statement_v2_test.go`:

```go
package auth

import (
	"strings"
	"testing"
	"time"
)

const statementV2TestKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func statementV2Fixture(account string) JWSStatementPayloadV2 {
	return JWSStatementPayloadV2{
		Iat:            1755000000,
		NetworkID:      "testnet-v2",
		KeeperShardID:  0,
		StatementID:    account + ":1:n1",
		SQLHash:        "0x" + strings.Repeat("11", 32),
		SettingsHash:   "0x" + strings.Repeat("22", 32),
		SchemaHash:     "0x" + strings.Repeat("33", 32),
		PayloadHash:    "0x" + strings.Repeat("44", 32),
		PayloadLength:  12345,
		PayloadFormat:  "clickhouse-native-data-v1",
		ClientRevision: 54460,
		TargetTableID:  "db.table",
		RowIDProfileID: "housegate-row-id-v1",
	}
}

func TestStatementV2_SignAndValidateRoundTrip(t *testing.T) {
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Unix()
	token, err := signer.SignStatementV2(want)
	if err != nil {
		t.Fatalf("SignStatementV2: %v", err)
	}
	if strings.Count(token, ".") != 2 {
		t.Fatalf("token is not compact JWS: %q", token)
	}
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	got, err := validator.ValidateStatementV2(token, want)
	if err != nil {
		t.Fatalf("ValidateStatementV2: %v", err)
	}
	if got != signer.Address() {
		t.Fatalf("recovered %s, want %s", got, signer.Address())
	}
}

func TestStatementV2_SignerForcesPurposeAndFillsIat(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	p := statementV2Fixture(signer.Address())
	p.Iat = 0
	p.Purpose = "wrong"
	token, err := signer.SignStatementV2(p)
	if err != nil {
		t.Fatalf("SignStatementV2: %v", err)
	}
	decoded, err := DecodeStatementV2Payload(token)
	if err != nil {
		t.Fatalf("DecodeStatementV2Payload: %v", err)
	}
	if decoded.Purpose != StatementPurposeV2 {
		t.Fatalf("purpose = %q, want %q", decoded.Purpose, StatementPurposeV2)
	}
	if decoded.Iat == 0 {
		t.Fatal("iat must be filled when zero")
	}
}

func TestStatementV2_EveryBoundFieldMismatchRejects(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	base := statementV2Fixture(signer.Address())
	base.Iat = time.Now().Unix()
	token, err := signer.SignStatementV2(base)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	mutations := map[string]func(*JWSStatementPayloadV2){
		"network_id":        func(p *JWSStatementPayloadV2) { p.NetworkID = "other" },
		"keeper_shard_id":   func(p *JWSStatementPayloadV2) { p.KeeperShardID = 1 },
		"statement_id":      func(p *JWSStatementPayloadV2) { p.StatementID = signer.Address() + ":2:n1" },
		"sql_hash":          func(p *JWSStatementPayloadV2) { p.SQLHash = "0x" + strings.Repeat("aa", 32) },
		"settings_hash":     func(p *JWSStatementPayloadV2) { p.SettingsHash = "0x" + strings.Repeat("aa", 32) },
		"schema_hash":       func(p *JWSStatementPayloadV2) { p.SchemaHash = "0x" + strings.Repeat("aa", 32) },
		"payload_hash":      func(p *JWSStatementPayloadV2) { p.PayloadHash = "0x" + strings.Repeat("aa", 32) },
		"payload_length":    func(p *JWSStatementPayloadV2) { p.PayloadLength = 1 },
		"payload_format":    func(p *JWSStatementPayloadV2) { p.PayloadFormat = "csv-with-names-v1" },
		"client_revision":   func(p *JWSStatementPayloadV2) { p.ClientRevision = 54470 },
		"target_table_id":   func(p *JWSStatementPayloadV2) { p.TargetTableID = "db.other" },
		"row_id_profile_id": func(p *JWSStatementPayloadV2) { p.RowIDProfileID = "housegate-row-id-v0" },
	}
	for name, mutate := range mutations {
		want := base
		mutate(&want)
		_, err := validator.ValidateStatementV2(token, want)
		if err == nil || !strings.Contains(err.Error(), name) {
			t.Fatalf("mutation %s: err = %v, want rejection naming the field", name, err)
		}
	}
}

func TestStatementV2_PurposeMismatchRejects(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	// A legacy query token in the statement slot must be rejected on purpose.
	legacy, err := signer.SignToken("SELECT 1")
	if err != nil {
		t.Fatalf("SignToken: %v", err)
	}
	if _, err := validator.ValidateStatementV2(legacy, want); err == nil || !strings.Contains(err.Error(), "purpose") {
		t.Fatalf("legacy token accepted as statement token: %v", err)
	}
}

func TestStatementV2_RecoveredAddressNotInAllowlistRejects(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	other, _ := NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	validator := NewEthValidator([]string{other.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Unix()
	token, _ := signer.SignStatementV2(want)
	if _, err := validator.ValidateStatementV2(token, want); err == nil || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("expected allowlist rejection, got %v", err)
	}
}

func TestStatementV2_ExpiredIatRejects(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := statementV2Fixture(signer.Address())
	want.Iat = time.Now().Add(-2 * time.Minute).Unix()
	token, _ := signer.SignStatementV2(want)
	if _, err := validator.ValidateStatementV2(token, want); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expiry rejection, got %v", err)
	}
}

func TestStatementPayloadV2Mismatch_IgnoresIat(t *testing.T) {
	a := statementV2Fixture("0xabc")
	b := a
	b.Iat = a.Iat + 100
	if got := StatementPayloadV2Mismatch(a, b); got != "" {
		t.Fatalf("iat must not count as a mismatch, got %q", got)
	}
	b.PayloadLength++
	if got := StatementPayloadV2Mismatch(a, b); got != "payload_length" {
		t.Fatalf("mismatch = %q, want payload_length", got)
	}
}
```

- [x] **Step 2: Run to verify failure**

Run: `bazel test //pkg/auth:auth_test --test_filter='TestStatementV2|TestStatementPayloadV2Mismatch' --test_output=errors`
Expected: build failure `undefined: JWSStatementPayloadV2` (gazelle will add the new test file automatically only after `bazel run //:gazelle`; run it first).

- [x] **Step 3: Implement**

Append to `pkg/auth/types.go`:

```go
// StatementTokenSettingKey carries the storage-integrity statement JWS
// (purpose StatementPurposeV2). It is deliberately distinct from
// AuthTokenSettingKey so the ordinary auth plugin's purpose-optional legacy
// path can never accept a statement token and vice versa.
const StatementTokenSettingKey = "SQL_x_statement_token"

// StatementPurposeV2 is the JWSStatementPayloadV2.Purpose value.
const StatementPurposeV2 = "housegate-statement-v2"

// JWSStatementPayloadV2 is the signed payload of a storage-integrity
// statement token. Every field except Iat is bound: verifiers derive the
// expected value from the envelope and require exact equality.
type JWSStatementPayloadV2 struct {
	Purpose        string `json:"purpose"`
	Iat            int64  `json:"iat"`
	NetworkID      string `json:"network_id"`
	KeeperShardID  uint32 `json:"keeper_shard_id"`
	StatementID    string `json:"statement_id"`
	SQLHash        string `json:"sql_hash"`
	SettingsHash   string `json:"settings_hash"`
	SchemaHash     string `json:"schema_hash"`
	PayloadHash    string `json:"payload_hash"`
	PayloadLength  uint64 `json:"payload_length"`
	PayloadFormat  string `json:"payload_format"`
	ClientRevision uint32 `json:"client_revision"`
	TargetTableID  string `json:"target_table_id"`
	RowIDProfileID string `json:"row_id_profile_id"`
}

// StatementPayloadV2Mismatch returns the JSON name of the first bound field
// that differs between got and want, or "" when every bound field is equal.
// Iat is not a bound field (freshness is checked separately at the ingress
// edge); Purpose is compared. Deterministic and side-effect free so the
// Arbiter FSM can reuse it inside Apply.
func StatementPayloadV2Mismatch(got, want JWSStatementPayloadV2) string {
	switch {
	case got.Purpose != want.Purpose:
		return "purpose"
	case got.NetworkID != want.NetworkID:
		return "network_id"
	case got.KeeperShardID != want.KeeperShardID:
		return "keeper_shard_id"
	case got.StatementID != want.StatementID:
		return "statement_id"
	case got.SQLHash != want.SQLHash:
		return "sql_hash"
	case got.SettingsHash != want.SettingsHash:
		return "settings_hash"
	case got.SchemaHash != want.SchemaHash:
		return "schema_hash"
	case got.PayloadHash != want.PayloadHash:
		return "payload_hash"
	case got.PayloadLength != want.PayloadLength:
		return "payload_length"
	case got.PayloadFormat != want.PayloadFormat:
		return "payload_format"
	case got.ClientRevision != want.ClientRevision:
		return "client_revision"
	case got.TargetTableID != want.TargetTableID:
		return "target_table_id"
	case got.RowIDProfileID != want.RowIDProfileID:
		return "row_id_profile_id"
	}
	return ""
}
```

Append to `pkg/auth/relay_signer.go`:

```go
// SignStatementV2 produces the storage-integrity statement token. Purpose is
// forced to StatementPurposeV2 and Iat is filled with the current time when
// zero (tests and vector generation pass an explicit Iat for determinism;
// secp256k1 signing is RFC 6979 deterministic so a fixed payload yields a
// fixed token).
func (s *RelaySigner) SignStatementV2(payload JWSStatementPayloadV2) (string, error) {
	payload.Purpose = StatementPurposeV2
	if payload.Iat == 0 {
		payload.Iat = time.Now().Unix()
	}
	header := JWSHeader{Alg: "ES256K", Typ: "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("marshal header: %w", err)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal statement payload: %w", err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	sig, err := crypto.Sign(keccak256([]byte(signingInput)), s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign statement: %w", err)
	}
	sig[64] += 27
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
```

Append to `pkg/auth/eth_validator.go`:

```go
// DecodeStatementV2Payload parses the payload of a compact statement token
// without verifying it. Callers that need trust must use ValidateStatementV2.
func DecodeStatementV2Payload(token string) (JWSStatementPayloadV2, error) {
	parts := strings.Split(strings.Trim(strings.TrimSpace(token), "\"'"), ".")
	if len(parts) != 3 {
		return JWSStatementPayloadV2{}, errors.New("invalid statement JWS format: expected 3 parts")
	}
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return JWSStatementPayloadV2{}, fmt.Errorf("invalid statement payload encoding: %w", err)
	}
	var payload JWSStatementPayloadV2
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return JWSStatementPayloadV2{}, fmt.Errorf("invalid statement payload JSON: %w", err)
	}
	return payload, nil
}

// ValidateStatementV2 verifies a storage-integrity statement token: compact
// serialization only, ES256K/secp256k1, purpose == StatementPurposeV2, iat
// within MaxTokenAge (5s skew), exact equality of every bound field against
// want (StatementPayloadV2Mismatch), then signer recovery and allowlist.
// Returns the recovered lowercase 0x address. Callers compare it against
// statement_id.client_account themselves.
func (v *EthValidator) ValidateStatementV2(token string, want JWSStatementPayloadV2) (string, error) {
	token = strings.Trim(strings.TrimSpace(token), "\"'")
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", errors.New("invalid statement JWS format: expected 3 parts")
	}
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("invalid statement header encoding: %w", err)
	}
	var header JWSHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return "", fmt.Errorf("invalid statement header JSON: %w", err)
	}
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return "", fmt.Errorf("unsupported algorithm: %s", header.Alg)
	}
	payload, err := DecodeStatementV2Payload(token)
	if err != nil {
		return "", err
	}
	if payload.Purpose != StatementPurposeV2 {
		return "", fmt.Errorf("statement token purpose mismatch: expected %q, got %q", StatementPurposeV2, payload.Purpose)
	}
	const clockSkewTolerance int64 = 5
	now := time.Now().Unix()
	tokenAge := now - payload.Iat
	if tokenAge < -clockSkewTolerance {
		return "", errors.New("statement token issued in the future")
	}
	if tokenAge < 0 {
		tokenAge = 0
	}
	if time.Duration(tokenAge)*time.Second > v.MaxTokenAge {
		return "", fmt.Errorf("statement token expired: age %ds exceeds max %s", tokenAge, v.MaxTokenAge)
	}
	want.Purpose = StatementPurposeV2
	if field := StatementPayloadV2Mismatch(payload, want); field != "" {
		return "", fmt.Errorf("statement token binding mismatch on %s", field)
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("invalid statement signature encoding: %w", err)
	}
	recoveredAddr, err := v.recoverAddressFromInput(parts[0]+"."+parts[1], signature)
	if err != nil {
		return "", fmt.Errorf("statement signature verification failed: %w", err)
	}
	if len(v.AllowedAddresses) > 0 && !v.AllowedAddresses[strings.ToLower(recoveredAddr)] {
		return "", fmt.Errorf("statement signer %s not in allowlist", recoveredAddr)
	}
	log.Debugw("statement token validated", "source", "eth_validator", "address", recoveredAddr, "statement_id", payload.StatementID)
	return strings.ToLower(recoveredAddr), nil
}
```

Append to `pkg/auth/signer.go`:

```go
// StatementSignerV2 signs storage-integrity statement tokens (agent side).
type StatementSignerV2 interface {
	Address() string
	SignStatementV2(payload JWSStatementPayloadV2) (string, error)
}

// StatementValidatorV2 verifies storage-integrity statement tokens against
// an envelope-derived expectation (ingress side).
type StatementValidatorV2 interface {
	ValidateStatementV2(token string, want JWSStatementPayloadV2) (string, error)
}

var (
	_ StatementSignerV2    = (*RelaySigner)(nil)
	_ StatementValidatorV2 = (*EthValidator)(nil)
)
```

- [x] **Step 4: Run tests**

Run: `bazel run //:gazelle && bazel test //pkg/auth:auth_test --test_output=errors`
Expected: PASS (all existing + 8 new tests).

- [x] **Step 5: Commit**

```bash
git add pkg/auth
git commit -m "feat(auth): housegate-statement-v2 JWS payload, signer and validator"
```

### Task 5: Shared JWS v2 test vectors (`statement_jws_v2.json`)

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Create: `pkg/auth/testdata/statement_jws_v2.json` (generated once, committed)
- Test: `pkg/auth/statement_v2_vectors_test.go` (generator behind env var + consumer)
- Modify: `pkg/auth/BUILD.bazel` (gazelle adds `data = glob(["testdata/**"])` — verify; add manually if not)

**Interfaces:**
- Produces: the vector file consumed by arbiter Task 30 (`fsm/testdata/statement_jws_v2.json`). Format:

```json
{
  "signer_private_key_hex": "aaaa…aa",
  "signer_address": "0x…",
  "vectors": [
    {"name": "valid", "expect": "accept", "payload": {<JWSStatementPayloadV2 JSON>}, "token": "<compact JWS>"},
    {"name": "payload_hash_mismatch", "expect": "reject", "payload": {...}, "token": "<same token as valid>"},
    ...
  ]
}
```
`payload` is the envelope-derived expectation the verifier compares against; `token` is the JWS. Reject vectors reuse the valid token but alter one expected field, or use a wrong-key/wrong-purpose token.

- [x] **Step 1: Write the generator + consumer test**

Create `pkg/auth/statement_v2_vectors_test.go`:

```go
package auth

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type statementV2Vector struct {
	Name    string                `json:"name"`
	Expect  string                `json:"expect"` // "accept" | "reject"
	Payload JWSStatementPayloadV2 `json:"payload"`
	Token   string                `json:"token"`
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
	mutate := func(name string, f func(*JWSStatementPayloadV2)) statementV2Vector {
		p := valid
		f(&p)
		return statementV2Vector{Name: name, Expect: "reject", Payload: p, Token: validToken}
	}
	file := statementV2VectorFile{
		SignerPrivateKeyHex: statementV2TestKey,
		SignerAddress:       signer.Address(),
		Vectors: []statementV2Vector{
			{Name: "valid", Expect: "accept", Payload: valid, Token: validToken},
			mutate("network_id_mismatch", func(p *JWSStatementPayloadV2) { p.NetworkID = "other-net" }),
			mutate("keeper_shard_id_mismatch", func(p *JWSStatementPayloadV2) { p.KeeperShardID = 1 }),
			mutate("statement_id_mismatch", func(p *JWSStatementPayloadV2) { p.StatementID = signer.Address() + ":2:n1" }),
			mutate("sql_hash_mismatch", func(p *JWSStatementPayloadV2) { p.SQLHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("settings_hash_mismatch", func(p *JWSStatementPayloadV2) { p.SettingsHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("schema_hash_mismatch", func(p *JWSStatementPayloadV2) { p.SchemaHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("payload_hash_mismatch", func(p *JWSStatementPayloadV2) { p.PayloadHash = "0x" + strings.Repeat("ab", 32) }),
			mutate("payload_length_mismatch", func(p *JWSStatementPayloadV2) { p.PayloadLength = 1 }),
			mutate("payload_format_mismatch", func(p *JWSStatementPayloadV2) { p.PayloadFormat = "csv-with-names-v1" }),
			mutate("client_revision_mismatch", func(p *JWSStatementPayloadV2) { p.ClientRevision = 54470 }),
			mutate("target_table_id_mismatch", func(p *JWSStatementPayloadV2) { p.TargetTableID = "db.other" }),
			mutate("row_id_profile_id_mismatch", func(p *JWSStatementPayloadV2) { p.RowIDProfileID = "housegate-row-id-v0" }),
			{Name: "wrong_signer", Expect: "reject", Payload: valid, Token: wrongSignerToken},
			{Name: "legacy_query_token", Expect: "reject", Payload: valid, Token: legacyToken},
			{Name: "garbage_token", Expect: "reject", Payload: valid, Token: "not.a.jws"},
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

// TestStatementV2Vectors proves the committed vectors against this
// package's validator. The validator's iat window is bypassed by pinning
// MaxTokenAge very large: the vectors are fixed at iat=1755000000 and the
// FSM-side consumer is wall-clock-free anyway.
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
	if len(file.Vectors) < 16 {
		t.Fatalf("expected at least 16 vectors, got %d", len(file.Vectors))
	}
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
			case "reject":
				if err == nil {
					t.Fatal("expected reject, got accept")
				}
			default:
				t.Fatalf("unknown expect %q", vec.Expect)
			}
		})
	}
}
```

- [x] **Step 2: Generate the vector file**

Run: `cd pkg/auth && HOUSEGATE_WRITE_VECTORS=1 go test . -run TestGenerateStatementV2Vectors -count=1 && ls -la testdata/statement_jws_v2.json && cd -`
Expected: `ok`, file exists (~6 KB), 16 vectors.

- [x] **Step 3: Run the consumer under Bazel**

Run: `bazel run //:gazelle && grep -n 'testdata' pkg/auth/BUILD.bazel; bazel test //pkg/auth:auth_test --test_filter=TestStatementV2Vectors --test_output=errors`
Expected: `data = glob(["testdata/**"])` present in `go_test(name="auth_test")` (if gazelle did not add it, add `data = glob(["testdata/**"]),` to the `auth_test` rule by hand); PASS with 16 subtests.

- [x] **Step 4: Commit**

```bash
git add pkg/auth/statement_v2_vectors_test.go pkg/auth/testdata/statement_jws_v2.json pkg/auth/BUILD.bazel
git commit -m "test(auth): shared statement JWS v2 test vectors"
```

### Task 6: `replay.Statement` v2 fields, verifier schema-hash source, payloadexec exports

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/replay/types.go:163-174` (Statement), `pkg/replay/verifier.go` (Verifier struct + Verify), `pkg/replay/payloadexec/exports.go`
- Test: `pkg/replay/verifier_test.go` (add), `pkg/replay/payloadexec/exports_test.go` (add)

**Interfaces:**
- Produces: `replay.Statement.{PayloadFormat string; ClientRevision uint32; SchemaHash string}` (json `payload_format`, `client_revision`, `schema_hash`, all `omitempty` — the arbiter-core conformance gate in Task 25 checks these names against the proto), `replay.SchemaHashSource`, `Verifier.SchemaHashes`, `payloadexec.RowIDProfileID`, `payloadexec.PayloadFormatCSVWithNames`.

- [x] **Step 1: Write the failing tests**

Append to `pkg/replay/verifier_test.go`:

```go
type fakeSchemaHashes map[string]string

func (f fakeSchemaHashes) TableSchemaHash(tableID string) (string, bool) {
	h, ok := f[tableID]
	return h, ok
}

func TestVerifierSchemaHashMismatchIsSignedNotErrored(t *testing.T) {
	ctx := context.Background()
	snap := testSnapshot(t)
	payload := []byte("name,balance\nalice,10\n")
	job := testJob(snap, payload, DigestString("source-claim"))
	job.Statements[0].SchemaHash = DigestString("agent-thought-schema")
	exec := &fakeExecutor{result: resultForJob(job, DigestString("source-claim"))}
	signer := &fakeSigner{replicaID: "replica-a", signature: "sig-a"}

	got, err := (&Verifier{
		Snapshots:    fakeSnapshotStore{snap.SnapshotID: snap},
		Payloads:     fakePayloadStore{"payload-1": payload},
		Executor:     exec,
		Signer:       signer,
		SchemaHashes: fakeSchemaHashes{"table-1": DigestString("verifier-schema")},
	}).Verify(ctx, job)
	if err != nil {
		t.Fatalf("schema hash mismatch must be signed evidence, got error: %v", err)
	}
	if got.MatchSourceRoot || got.Receipt.MatchSourceRoot {
		t.Fatal("schema hash mismatch must produce a non-matching receipt")
	}
	if exec.called {
		t.Fatal("executor must not run when the signed schema hash mismatches the verifier's schema source")
	}
	if got.Signature == "" || got.Receipt.ReplayLogHash == "" {
		t.Fatalf("mismatch receipt must be signed and carry a replay log hash: %#v", got)
	}
}

func TestVerifierSchemaHashMatchRunsExecutor(t *testing.T) {
	ctx := context.Background()
	snap := testSnapshot(t)
	payload := []byte("name,balance\nalice,10\n")
	root := DigestString("root-after")
	job := testJob(snap, payload, root)
	job.Statements[0].SchemaHash = DigestString("shared-schema")
	exec := &fakeExecutor{result: resultForJob(job, root)}
	got, err := (&Verifier{
		Snapshots:    fakeSnapshotStore{snap.SnapshotID: snap},
		Payloads:     fakePayloadStore{"payload-1": payload},
		Executor:     exec,
		Signer:       &fakeSigner{replicaID: "replica-a", signature: "sig-a"},
		SchemaHashes: fakeSchemaHashes{"table-1": DigestString("shared-schema")},
	}).Verify(ctx, job)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !exec.called || !got.MatchSourceRoot {
		t.Fatalf("matching schema hash must run the executor and match: called=%v match=%v", exec.called, got.MatchSourceRoot)
	}
}

func TestVerifierUnknownTableInSchemaSourceIsLocalRefusal(t *testing.T) {
	ctx := context.Background()
	snap := testSnapshot(t)
	payload := []byte("name,balance\nalice,10\n")
	job := testJob(snap, payload, DigestString("x"))
	job.Statements[0].SchemaHash = DigestString("shared-schema")
	_, err := (&Verifier{
		Snapshots:    fakeSnapshotStore{snap.SnapshotID: snap},
		Payloads:     fakePayloadStore{"payload-1": payload},
		Executor:     &fakeExecutor{},
		Signer:       &fakeSigner{replicaID: "replica-a", signature: "sig-a"},
		SchemaHashes: fakeSchemaHashes{},
	}).Verify(ctx, job)
	if err == nil {
		t.Fatal("a table the verifier cannot resolve is a local refusal to attest, not signed evidence")
	}
}
```

Append to `pkg/replay/payloadexec/exports_test.go`:

```go
func TestRowIDProfileIDMatchesDomain(t *testing.T) {
	if RowIDProfileID != rowIDDomain || RowIDProfileID != "housegate-row-id-v1" {
		t.Fatalf("RowIDProfileID = %q", RowIDProfileID)
	}
	if PayloadFormatCSVWithNames != "csv-with-names-v1" {
		t.Fatalf("PayloadFormatCSVWithNames = %q", PayloadFormatCSVWithNames)
	}
}
```

- [x] **Step 2: Run to verify failure**

Run: `bazel test //pkg/replay:replay_test //pkg/replay/payloadexec:payloadexec_test --test_output=errors`
Expected: build errors `unknown field SchemaHash`, `undefined: RowIDProfileID`.

- [x] **Step 3: Implement**

`pkg/replay/types.go` — extend `Statement`:

```go
type Statement struct {
	StatementID   string `json:"statement_id"`
	StatementSeq  uint64 `json:"statement_seq"`
	SQL           string `json:"sql"`
	SQLHash       string `json:"sql_hash"`
	SettingsHash  string `json:"settings_hash"`
	PayloadRef    string `json:"payload_ref,omitempty"`
	PayloadHash   string `json:"payload_hash,omitempty"`
	PayloadLength uint64 `json:"payload_length,omitempty"`
	TargetTableID string `json:"target_table_id"`
	UserJWS       string `json:"user_jws,omitempty"`
	// v2 additions (envelope v2): how to decode the payload and which
	// declared schema the signer encoded against. JSON tags are frozen
	// against arbiter-proto replay.proto Statement fields 11-13.
	PayloadFormat  string `json:"payload_format,omitempty"`
	ClientRevision uint32 `json:"client_revision,omitempty"`
	SchemaHash     string `json:"schema_hash,omitempty"`
}
```

`pkg/replay/verifier.go` — add the source interface and field, and the pre-execution check:

```go
// SchemaHashSource resolves the verifier's own Phase-B schema hash for a
// table. When set on Verifier, every statement that carries a non-empty
// SchemaHash is compared against it BEFORE execution: a mismatch is
// challenge evidence (base design C.4) and yields a signed non-matching
// receipt; a table the source cannot resolve is a local refusal to attest.
type SchemaHashSource interface {
	TableSchemaHash(tableID string) (string, bool)
}

type Verifier struct {
	Snapshots    SnapshotStore
	Payloads     PayloadStore
	Executor     Executor
	Signer       Signer
	SchemaHashes SchemaHashSource // optional
}
```

Inside `Verify`, immediately after `prepared, err := v.prepareStatements(...)` succeeds and before `v.Executor.Replay`, insert:

```go
	if v.SchemaHashes != nil {
		for i, st := range job.Statements {
			if st.SchemaHash == "" {
				continue
			}
			local, ok := v.SchemaHashes.TableSchemaHash(st.TargetTableID)
			if !ok {
				return ReplayAttestation{}, fmt.Errorf("statement %d: no local schema for table %q", i, st.TargetTableID)
			}
			if local != st.SchemaHash {
				return v.signSchemaMismatch(ctx, job, prepared, i, st, local)
			}
		}
	}
```

And add the helper after `Verify`:

```go
// signSchemaMismatch signs a non-matching receipt when a statement's signed
// schema_hash differs from this verifier's schema source. Nothing is
// executed; ComputedStateRoot is empty and ReplayLogHash commits to the
// mismatch so the receipt is non-repudiable challenge evidence.
func (v *Verifier) signSchemaMismatch(ctx context.Context, job ReplayJob, prepared []PreparedStatement, index int, st Statement, local string) (ReplayAttestation, error) {
	statementRoot, err := statementRoot(job.Statements)
	if err != nil {
		return ReplayAttestation{}, err
	}
	payloadRoot, err := payloadRoot(prepared)
	if err != nil {
		return ReplayAttestation{}, err
	}
	logHash, err := canonicalDigest("replay-schema-hash-mismatch", struct {
		StatementIndex int    `json:"statement_index"`
		StatementID    string `json:"statement_id"`
		TableID        string `json:"table_id"`
		SignedHash     string `json:"signed_schema_hash"`
		LocalHash      string `json:"local_schema_hash"`
	}{index, st.StatementID, st.TargetTableID, st.SchemaHash, local})
	if err != nil {
		return ReplayAttestation{}, err
	}
	receipt := ExecutionReceipt{
		BlockSeq:           job.BlockSeq,
		PrevSafeSnapshotID: job.PrevSafeSnapshotID,
		PrevStateRoot:      job.PrevStateRoot,
		SchemaSnapshotID:   job.SchemaSnapshotID,
		ExecutorProfileID:  job.ExecutorProfileID,
		StatementRoot:      statementRoot,
		PayloadRoot:        payloadRoot,
		SourceClaimRoot:    job.SourceClaimRoot,
		ComputedStateRoot:  "",
		MatchSourceRoot:    false,
		ReplayLogHash:      logHash,
	}
	receiptHash, err := receipt.Hash()
	if err != nil {
		return ReplayAttestation{}, err
	}
	replicaID, sig, err := v.Signer.SignReplayReceipt(ctx, receiptHash)
	if err != nil {
		return ReplayAttestation{}, fmt.Errorf("sign schema-mismatch receipt: %w", err)
	}
	if replicaID == "" || sig == "" {
		return ReplayAttestation{}, fmt.Errorf("signer returned empty replica id or signature")
	}
	return ReplayAttestation{ReplicaID: replicaID, Receipt: receipt, ReceiptHash: receiptHash, Signature: sig, MatchSourceRoot: false}, nil
}
```

`pkg/replay/payloadexec/exports.go` — append:

```go
// RowIDProfileID names the _hg_row_id derivation profile (§5.2). Envelope v2
// signs it as row_id_profile_id; verifiers reject any other value.
const RowIDProfileID = rowIDDomain

// PayloadFormatCSVWithNames is the legacy/test wire encoding decoded by
// DecodeCSV. Production storage-integrity payloads are Native
// (pkg/replay/nativepayload.PayloadFormat).
const PayloadFormatCSVWithNames = "csv-with-names-v1"
```

- [x] **Step 4: Run tests**

Run: `bazel test //pkg/replay:replay_test //pkg/replay/payloadexec:payloadexec_test --test_output=errors`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add pkg/replay
git commit -m "feat(replay): statement payload_format/client_revision/schema_hash, verifier schema-hash evidence, RowIDProfileID export"
```

### Task 7: Move the Native decoder to `pkg/replay/nativepayload`; `chexec` branches on payload format

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Create: `pkg/replay/nativepayload/native.go` (from `pkg/storageintegrity/native_payload.go`), `pkg/replay/nativepayload/native_test.go` (from `pkg/storageintegrity/native_payload_test.go`)
- Modify: `pkg/storageintegrity/native_payload.go` (becomes thin aliases), `pkg/replay/chexec/materializer.go:52-60`, `pkg/replay/chexec/BUILD.bazel` (gazelle), `pkg/storageintegrity/BUILD.bazel` (gazelle)
- Test: `pkg/replay/chexec/materializer_format_test.go` (new, no docker)

**Interfaces:**
- Produces: `nativepayload.PayloadFormat`, `nativepayload.ErrUnsupported`, `nativepayload.Materializer{NetworkID, Revision}`, `nativepayload.Decode`, `nativepayload.ValidateDecodable`; sicore keeps `PayloadEncodingClickHouseNativeData`, `ErrNativePayloadUnsupported`, `NativeMaterializer`, `DecodeNativePayload`, `ValidateNativePayloadDecodable` as aliases (no caller changes).

- [x] **Step 1: Do the move**

```bash
mkdir -p pkg/replay/nativepayload
git mv pkg/storageintegrity/native_payload.go pkg/replay/nativepayload/native.go
git mv pkg/storageintegrity/native_payload_test.go pkg/replay/nativepayload/native_test.go
```

Edit `pkg/replay/nativepayload/native.go`: change `package storageintegrity` → `package nativepayload`; add a package doc; rename identifiers exactly as follows (use `gofmt -r` or sed, then review):

| old | new |
|---|---|
| `PayloadEncodingClickHouseNativeData` | `PayloadFormat` |
| `ErrNativePayloadUnsupported` | `ErrUnsupported` |
| `NativeMaterializer` | `Materializer` |
| `DecodeNativePayload` | `Decode` |
| `ValidateNativePayloadDecodable` | `ValidateDecodable` |

Package doc to put at the top:

```go
// Package nativepayload decodes ClickHouse Native ClientData wire payloads
// (payload_format "clickhouse-native-data-v1": the concatenated de-chunked
// raw bytes of every non-empty client Data packet of one INSERT) into the
// shared payloadexec row model. It lives under pkg/replay so replay
// executors (chexec, arbiter-core snode/verifier) can import it without
// pulling in the ingress package.
package nativepayload
```

In `Materializer.Materialize`, replace `rows, err := DecodeNativePayload(schema, m.Revision, st.Payload)` with:

```go
	revision := m.Revision
	if st.ClientRevision != 0 {
		revision = int(st.ClientRevision)
	}
	rows, err := Decode(schema, revision, st.Payload)
```

Edit `pkg/replay/nativepayload/native_test.go`: `package nativepayload`; apply the same renames; the tests `TestNativeMaterializerMatchesCSVExecutorProfile` etc. keep their names. Any test that referenced `PayloadEncodingClickHouseNativeData` now references `PayloadFormat`.

Create a NEW `pkg/storageintegrity/native_payload.go` with only aliases:

```go
package storageintegrity

import (
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// PayloadEncodingClickHouseNativeData is the SI lane's canonical payload
// format; the decoder itself lives in pkg/replay/nativepayload.
const PayloadEncodingClickHouseNativeData = nativepayload.PayloadFormat

// ErrNativePayloadUnsupported aliases nativepayload.ErrUnsupported.
var ErrNativePayloadUnsupported = nativepayload.ErrUnsupported

// NativeMaterializer aliases nativepayload.Materializer.
type NativeMaterializer = nativepayload.Materializer

// DecodeNativePayload aliases nativepayload.Decode.
func DecodeNativePayload(schema payloadexec.TableSchema, revision int, payload []byte) ([]payloadexec.Row, error) {
	return nativepayload.Decode(schema, revision, payload)
}

// ValidateNativePayloadDecodable aliases nativepayload.ValidateDecodable.
func ValidateNativePayloadDecodable(schema payloadexec.TableSchema, revision int, payload []byte) error {
	return nativepayload.ValidateDecodable(schema, revision, payload)
}
```

- [x] **Step 2: Write the failing chexec format test**

Create `pkg/replay/chexec/materializer_format_test.go`:

```go
package chexec

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// decodeRows is exercised without ClickHouse: it is the pure format branch
// in front of the scratch-table path.
func TestDecodeRowsBranchesOnPayloadFormat(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	csv := replay.PreparedStatement{Statement: replay.Statement{StatementID: "0xabc:1:n", TargetTableID: "db.t", PayloadFormat: payloadexec.PayloadFormatCSVWithNames}, Payload: []byte("v\n1\n")}
	rows, err := decodeRows(context.Background(), schema, csv)
	if err != nil || len(rows) != 1 {
		t.Fatalf("csv branch: rows=%d err=%v", len(rows), err)
	}
	legacy := csv
	legacy.PayloadFormat = ""
	if _, err := decodeRows(context.Background(), schema, legacy); err != nil {
		t.Fatalf("empty format must decode as csv (legacy tests): %v", err)
	}
	native := csv
	native.PayloadFormat = "clickhouse-native-data-v1"
	native.ClientRevision = 0
	if _, err := decodeRows(context.Background(), schema, native); err == nil || !strings.Contains(err.Error(), "revision") {
		t.Fatalf("native branch without client_revision must fail closed: %v", err)
	}
	unknown := csv
	unknown.PayloadFormat = "future-v9"
	if _, err := decodeRows(context.Background(), schema, unknown); err == nil || !strings.Contains(err.Error(), "future-v9") {
		t.Fatalf("unknown format must fail closed naming the format: %v", err)
	}
}
```

- [x] **Step 3: Implement the chexec branch**

In `pkg/replay/chexec/materializer.go` add the import `"github.com/housegate/housegate/pkg/replay/nativepayload"`, replace the first lines of `Materialize` (`decoded, err := payloadexec.DecodeCSV(st.Payload, schema)`) with `decoded, err := decodeRows(ctx, schema, st)`, and add:

```go
// decodeRows selects the wire decoder from the statement's signed
// payload_format. Native is the production SI lane; CSVWithNames (and the
// empty legacy value) is kept for the in-process executor tests.
func decodeRows(_ context.Context, schema payloadexec.TableSchema, st replay.PreparedStatement) ([]payloadexec.Row, error) {
	switch st.PayloadFormat {
	case nativepayload.PayloadFormat:
		if st.ClientRevision == 0 {
			return nil, fmt.Errorf("statement %s: native payload requires client_revision", st.StatementID)
		}
		return nativepayload.Decode(schema, int(st.ClientRevision), st.Payload)
	case "", payloadexec.PayloadFormatCSVWithNames:
		return payloadexec.DecodeCSV(st.Payload, schema)
	default:
		return nil, fmt.Errorf("statement %s: unsupported payload_format %q", st.StatementID, st.PayloadFormat)
	}
}
```

- [x] **Step 4: Regenerate BUILD files and run**

Run: `bazel run //:gazelle && bazel test //pkg/replay/nativepayload:nativepayload_test //pkg/replay/chexec:chexec_test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS; the 17 moved tests run under the new package name (`bazel test //pkg/replay/nativepayload:nativepayload_test --test_arg=-test.v | grep -c '^--- PASS'` prints 17).

- [x] **Step 5: Docker equivalence test — Native payload through chexec == in-process nativepayload executor**

Append to `pkg/integration/chreplay_test.go` (docker-bound, already in the CI target list):

```go
// nativeReplayPayload encodes one client Data packet for the schema used by
// TestReplayCHExecutorNativePayloadMatchesInProcessRoot at revision 54460.
func nativeReplayPayload(t *testing.T) []byte {
	t.Helper()
	user := proto.ColStr{}
	user.Append("0x123")
	user.Append("0xabc")
	balance := proto.ColUInt64{10, 250}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 2, Columns: 2}).EncodeBlock(&buf, 54460, proto.Input{{Name: "user_id", Data: &user}, {Name: "balance", Data: &balance}}); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

// TestReplayCHExecutorNativePayloadMatchesInProcessRoot: with
// payload_format = clickhouse-native-data-v1 the ClickHouse-backed
// materializer and the in-process nativepayload materializer must produce
// the same state root (executor equivalence, envelope v2 §8).
func TestReplayCHExecutorNativePayloadMatchesInProcessRoot(t *testing.T) {
	conn := openDirectCH(t)
	tableID := uniqueTable(t)
	schema := payloadexec.TableSchema{TableID: tableID, Columns: []lthash.Column{{Name: "user_id", Type: "String"}, {Name: "balance", Type: "UInt64"}}}
	chE := payloadexec.NewWithMaterializer(chReplayNetwork, chexec.NewMaterializer(chReplayNetwork, conn), schema)
	inE := payloadexec.NewWithMaterializer(chReplayNetwork, nativepayload.Materializer{NetworkID: chReplayNetwork}, schema)
	genCH, err := chE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatal(err)
	}
	genIn, err := inE.GenesisSnapshot(0, "schema-1", "ch-26.x")
	if err != nil {
		t.Fatal(err)
	}
	payload := nativeReplayPayload(t)
	job := chReplayJob(genCH, tableID, "stmt-native-1", "probe", payload, "")
	job.Statements[0].SQL = "INSERT INTO " + tableID + " FORMAT Native"
	job.Statements[0].SQLHash = replay.DigestString(job.Statements[0].SQL)
	job.Statements[0].PayloadFormat = nativepayload.PayloadFormat
	job.Statements[0].ClientRevision = 54460
	prepared := []replay.PreparedStatement{{Statement: job.Statements[0], Payload: payload}}
	_, chRes, err := chE.ApplyContext(context.Background(), genCH, job, prepared)
	if err != nil {
		t.Fatalf("chexec native: %v", err)
	}
	_, inRes, err := inE.ApplyContext(context.Background(), genIn, job, prepared)
	if err != nil {
		t.Fatalf("in-process native: %v", err)
	}
	if chRes.ComputedStateRoot != inRes.ComputedStateRoot {
		t.Fatalf("native executor equivalence broken:\n  ch: %s\n  in: %s", chRes.ComputedStateRoot, inRes.ComputedStateRoot)
	}
}
```

(imports: `github.com/ClickHouse/ch-go/proto`, `github.com/housegate/housegate/pkg/replay/nativepayload`.) Run: `bazel run //:gazelle && bazel test //pkg/integration:integration_test --test_filter=TestReplayCHExecutorNativePayloadMatchesInProcessRoot --test_output=errors` — Expected: PASS (docker ClickHouse required).

- [x] **Step 6: Commit**

```bash
git add pkg/replay/nativepayload pkg/replay/chexec pkg/storageintegrity/native_payload.go pkg/storageintegrity/BUILD.bazel pkg/integration/chreplay_test.go pkg/integration/BUILD.bazel
git commit -m "refactor(replay): move Native payload decoder to pkg/replay/nativepayload; chexec branches on payload_format"
```

### Task 8: `sicore` settings commitment, envelope version constant, native-only `InsertPayloadEncoding`

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Create: `pkg/storageintegrity/settings.go`, `pkg/storageintegrity/settings_test.go`
- Modify: `pkg/storageintegrity/sql.go:12-36`, `pkg/storageintegrity/sql_test.go:22-25`, `pkg/storageintegrity/materializer_select.go:9`

**Interfaces:**
- Produces: `sicore.EmptySettingsHash`, `sicore.SettingsHashDomain`, `sicore.EnvelopeVersionV2`, `sicore.IsHousegateOwnedSettingKey`, `sicore.RejectUserSettings`; `InsertPayloadEncoding("... FORMAT CSVWithNames")` now returns `PayloadEncodingClickHouseNativeData` (the wire is Native regardless of the SQL FORMAT); `EncodingCSVWithNames = payloadexec.PayloadFormatCSVWithNames`.

- [x] **Step 1: Write the failing tests**

Create `pkg/storageintegrity/settings_test.go`:

```go
package storageintegrity

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func TestEmptySettingsHashIsTheCanonicalEmptySetDigest(t *testing.T) {
	got, err := replay.CanonicalDigest(SettingsHashDomain, []string{})
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	if got != EmptySettingsHash {
		t.Fatalf("EmptySettingsHash = %s, canonical = %s", EmptySettingsHash, got)
	}
	if EmptySettingsHash != "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006" {
		t.Fatalf("EmptySettingsHash constant drifted: %s", EmptySettingsHash)
	}
}

func TestRejectUserSettingsStripsHousegateOwnedKeys(t *testing.T) {
	if err := RejectUserSettings([]string{"SQL_x_auth_token", "SQL_x_statement_token", "SQL_x_payer", "SQL_sentio_driver"}); err != nil {
		t.Fatalf("housegate-owned keys must be admitted: %v", err)
	}
	err := RejectUserSettings([]string{"SQL_x_auth_token", "async_insert"})
	if err == nil || !strings.Contains(err.Error(), "async_insert") {
		t.Fatalf("expected rejection naming async_insert, got %v", err)
	}
	if !IsHousegateOwnedSettingKey("SQL_sentio_maintenance") || IsHousegateOwnedSettingKey("max_threads") {
		t.Fatal("IsHousegateOwnedSettingKey prefix rules")
	}
}
```

Edit `pkg/storageintegrity/sql_test.go`: change the third case to

```go
		{
			name: "CSVWithNames in SQL still rides the Native wire",
			sql:  "INSERT INTO events FORMAT CSVWithNames",
			want: PayloadEncodingClickHouseNativeData,
		},
```

- [x] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestEmptySettingsHash|TestRejectUserSettings|TestInsertPayloadEncodingAccepts' --test_output=errors`
Expected: build error `undefined: SettingsHashDomain`.

- [x] **Step 3: Implement**

Create `pkg/storageintegrity/settings.go`:

```go
package storageintegrity

import (
	"fmt"
	"strings"
)

// SettingsHashDomain is the CanonicalDigest domain for the signed
// settings_hash. v1 commits to the empty user-settings set (spec D4).
const SettingsHashDomain = "housegate-settings-v1"

// EmptySettingsHash = replay.CanonicalDigest(SettingsHashDomain, []string{}).
// It is a constant so the Arbiter FSM and every housegate compare the same
// literal without recomputing; settings_test.go pins it to the derivation.
const EmptySettingsHash = "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006"

// EnvelopeVersionV2 is the only envelope_version the SI lane emits/admits.
const EnvelopeVersionV2 uint32 = 2

// IsHousegateOwnedSettingKey reports whether a ClickHouse query setting key
// is owned by housegate (auth token, statement token, payer, driver /
// maintenance / operator flags) and therefore excluded from settings_hash.
func IsHousegateOwnedSettingKey(key string) bool {
	return strings.HasPrefix(key, "SQL_x_") || strings.HasPrefix(key, "SQL_sentio_")
}

// RejectUserSettings enforces the v1 empty-user-settings rule: every key that
// is not housegate-owned is a rejection naming the setting so writers learn
// why (e.g. async_insert / input_format_* cannot use the SI lane in v1).
func RejectUserSettings(keys []string) error {
	for _, key := range keys {
		if IsHousegateOwnedSettingKey(key) {
			continue
		}
		return fmt.Errorf("storage_integrity v1 does not admit client query setting %q on the SI lane; remove it or use a non-SI connection", key)
	}
	return nil
}
```

Edit `pkg/storageintegrity/sql.go` `InsertPayloadEncoding`: replace the `case "FORMAT":` block with

```go
	case "FORMAT":
		switch format {
		case "NATIVE", "CSVWITHNAMES":
			// The native TCP client parses CSVWithNames locally and sends
			// Native blocks; the stored payload is the wire capture either way.
			return PayloadEncodingClickHouseNativeData, nil
		}
		if format == "" {
			return "", fmt.Errorf("requires streaming Native INSERT input; FORMAT without Native is not supported")
		}
		return "", fmt.Errorf("requires streaming Native INSERT input; FORMAT %s is not supported", format)
```

Edit `pkg/storageintegrity/materializer_select.go` line 9 to `const EncodingCSVWithNames = payloadexec.PayloadFormatCSVWithNames` and add the import `"github.com/housegate/housegate/pkg/replay/payloadexec"`.

- [x] **Step 4: Run tests**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS. (`TestEnvelopeFromAdmission_AcceptsMaterializedCSVWithNames` in intake_test.go now fails because CSV SQL maps to native — delete that test in this task; the CSV admission path is removed by Task 20 anyway.)

- [x] **Step 5: Commit**

```bash
git add pkg/storageintegrity
git commit -m "feat(storageintegrity): settings commitment constants, envelope v2 version, native-only INSERT payload encoding"
```

---

## Phase 3 — Relay deferred-INSERT mode (spec §10 step 3, §5.2)

### Task 9: `chproto.SampleColumn` + `Codec.WriteSampleBlock`

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/chproto/codec.go` (after `WriteEmptyDataBlock`, ~:657)
- Test: `pkg/chproto/sample_block_test.go` (new)

**Interfaces:**
- Produces: `chproto.SampleColumn{Name, Type string}`, `(*Codec).WriteSampleBlock(cols []SampleColumn) error` — writes ONE server `Data` packet (`ServerCodeData`) holding a 0-row block with the given columns, encoded for `c.Revision()`, through the codec's own writer (chunk-framed when negotiated), LZ4-wrapped when the codec's compression is enabled. Byte-identical to ch-go `proto.Block{Rows:0,Columns:n}.EncodeBlock` for the same revision.

- [x] **Step 1: Write the failing test**

Create `pkg/chproto/sample_block_test.go`:

```go
package chproto

import (
	"bytes"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func chgoSampleBlock(t *testing.T, rev int) []byte {
	t.Helper()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")
	id := proto.ColUInt64{}
	region := proto.ColStr{}
	input := proto.Input{{Name: "id", Data: &id}, {Name: "region", Data: &region}}
	if err := (proto.Block{Rows: 0, Columns: 2}).EncodeBlock(&buf, rev, input); err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

func TestWriteSampleBlock_MatchesChGoEncodingAcrossRevisions(t *testing.T) {
	// 54453 predates FeatureCustomSerialization (54454); 54460 / 54470 carry
	// the per-column "has custom serialization" flag.
	for _, rev := range []int{54453, 54460, 54470} {
		var out bytes.Buffer
		c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &out}, DirFromClient)
		c.SetRevision(rev)
		if err := c.WriteSampleBlock([]SampleColumn{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}); err != nil {
			t.Fatalf("rev %d: WriteSampleBlock: %v", rev, err)
		}
		if want := chgoSampleBlock(t, rev); !bytes.Equal(out.Bytes(), want) {
			t.Fatalf("rev %d: sample block bytes\n got %x\nwant %x", rev, out.Bytes(), want)
		}
	}
}

func TestWriteSampleBlock_DecodesAsZeroRowBlockWithColumns(t *testing.T) {
	var out bytes.Buffer
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &out}, DirFromClient)
	c.SetRevision(54460)
	if err := c.WriteSampleBlock([]SampleColumn{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}); err != nil {
		t.Fatalf("WriteSampleBlock: %v", err)
	}
	r := proto.NewReader(bytes.NewReader(out.Bytes()))
	code, err := r.UVarInt()
	if err != nil || code != uint64(proto.ServerCodeData) {
		t.Fatalf("packet code = %d err=%v, want ServerCodeData", code, err)
	}
	if name, err := r.Str(); err != nil || name != "" {
		t.Fatalf("block name = %q err=%v", name, err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, 54460, results.Auto()); err != nil {
		t.Fatalf("DecodeBlock: %v", err)
	}
	if block.Rows != 0 || block.Columns != 2 || len(results) != 2 {
		t.Fatalf("block rows=%d cols=%d results=%d", block.Rows, block.Columns, len(results))
	}
	if results[0].Name != "id" || results[0].Data.Type() != "UInt64" || results[1].Name != "region" || results[1].Data.Type() != "String" {
		t.Fatalf("columns = %s %s / %s %s", results[0].Name, results[0].Data.Type(), results[1].Name, results[1].Data.Type())
	}
}

func TestWriteSampleBlock_ChunkFramesWhenNegotiated(t *testing.T) {
	var out bytes.Buffer
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &out}, DirFromClient)
	c.SetRevision(54470)
	c.EnableChunked(false, true)
	if err := c.WriteSampleBlock([]SampleColumn{{Name: "v", Type: "UInt64"}}); err != nil {
		t.Fatalf("WriteSampleBlock: %v", err)
	}
	chunk, err := NewChunkedReader(bytes.NewReader(out.Bytes()), true).ReadChunk()
	if err != nil {
		t.Fatalf("ReadChunk: %v", err)
	}
	var want proto.Buffer
	want.PutUVarInt(uint64(proto.ServerCodeData))
	want.PutString("")
	v := proto.ColUInt64{}
	if err := (proto.Block{Rows: 0, Columns: 1}).EncodeBlock(&want, 54470, proto.Input{{Name: "v", Data: &v}}); err != nil {
		t.Fatalf("EncodeBlock: %v", err)
	}
	if !bytes.Equal(chunk, want.Buf) {
		t.Fatalf("chunk payload mismatch\n got %x\nwant %x", chunk, want.Buf)
	}
}

func TestWriteSampleBlock_RejectsUnnamedOrUntypedColumns(t *testing.T) {
	c := NewCodec(&readerWriter{r: &bytes.Buffer{}, w: &bytes.Buffer{}}, DirFromClient)
	c.SetRevision(54460)
	if err := c.WriteSampleBlock([]SampleColumn{{Name: "", Type: "UInt64"}}); err == nil {
		t.Fatal("unnamed column must be rejected")
	}
	if err := c.WriteSampleBlock([]SampleColumn{{Name: "v", Type: ""}}); err == nil {
		t.Fatal("untyped column must be rejected")
	}
	if err := c.WriteSampleBlock(nil); err == nil {
		t.Fatal("a sample block needs at least one column")
	}
}
```

- [x] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/chproto:chproto_test --test_filter=TestWriteSampleBlock --test_output=errors`
Expected: build error `undefined: SampleColumn`.

- [x] **Step 3: Implement**

Append to `pkg/chproto/codec.go`:

```go
// SampleColumn is one column of a synthesized INSERT sample block: the
// ClickHouse column name and its exact type string (e.g. "UInt64",
// "LowCardinality(String)"). Housegate never instantiates a typed ch-go
// column for it — a 0-row block only carries name + type on the wire.
type SampleColumn struct {
	Name string
	Type string
}

// WriteSampleBlock writes the server-side INSERT sample block (a Data packet
// holding a 0-row block with cols) that ClickHouse would send after an INSERT
// Query. Relay uses it in deferred-INSERT mode so the client starts streaming
// its payload before the Query reaches the upstream. The encoding is
// byte-identical to ch-go's proto.Block.EncodeBlock for the negotiated
// revision (BlockInfo when >= 51903, per-column custom-serialization flag when
// >= 54454) and follows WriteEmptyDataBlock's compression handling.
func (c *Codec) WriteSampleBlock(cols []SampleColumn) error {
	if len(cols) == 0 {
		return fmt.Errorf("%w: sample block requires at least one column", ErrMalformed)
	}
	rev := c.Revision()
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ServerCodeData))
	buf.PutString("")

	var payload proto.Buffer
	if proto.FeatureBlockInfo.In(rev) {
		bi := proto.BlockInfo{BucketNum: -1}
		bi.Encode(&payload)
	}
	payload.PutUVarInt(uint64(len(cols)))
	payload.PutUVarInt(0) // rows
	for _, col := range cols {
		if col.Name == "" || col.Type == "" {
			return fmt.Errorf("%w: sample column requires name and type (got %q/%q)", ErrMalformed, col.Name, col.Type)
		}
		payload.PutString(col.Name)
		payload.PutString(col.Type)
		if proto.FeatureCustomSerialization.In(rev) {
			payload.PutBool(false) // no custom serialization
		}
	}

	if c.Compression() == proto.CompressionEnabled {
		w := compress.NewWriter(0, compress.LZ4)
		if err := w.Compress(payload.Buf); err != nil {
			return fmt.Errorf("compress sample block: %w", err)
		}
		buf.Buf = append(buf.Buf, w.Data...)
	} else {
		buf.Buf = append(buf.Buf, payload.Buf...)
	}
	// Single Write: one logical chunk on a chunked leg, one segment for
	// net.Pipe-backed test peers.
	n, err := c.w.Write(buf.Buf)
	if err != nil {
		return err
	}
	if n != len(buf.Buf) {
		return io.ErrShortWrite
	}
	return nil
}
```

- [x] **Step 4: Run tests**

Run: `bazel test //pkg/chproto:chproto_test --test_output=errors`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add pkg/chproto
git commit -m "feat(chproto): Codec.WriteSampleBlock for locally answered INSERT sample blocks"
```

### Task 10: `plugin.DeferredInsertPlan` + Relay deferred-INSERT protocol (happy path)

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/plugin/context.go` (add `DeferredInsertPlan` + `QueryContext.DeferredInsert`)
- Modify: `pkg/proxy/relay.go` (Relay fields ~:42-62; `clientToUpstream` ~:736-760; `upstreamToClient` ~:1076-1166; new `runDeferredInsert`)
- Test: `pkg/proxy/relay_deferred_test.go` (new; harness reused by Task 11)

**Interfaces:**
- Consumes: `chproto.SampleColumn`, `Codec.WriteSampleBlock` (Task 9), `Codec.ReadPacketWithDataLimit`, `chproto.ClientDataPacketIsEmpty`, hooks `OnClientDataStrict/OnClientData/OnQueryInputCompleteStrict/OnQueryInputComplete/OnQueryAbort/OnQueryComplete/ClientDataReadLimit`.
- Produces: `plugin.DeferredInsertPlan{SampleColumns []chproto.SampleColumn; MaxPayloadBytes uint64}`, `QueryContext.DeferredInsert *DeferredInsertPlan` (set by Task 16's plugin); Relay behaviour per spec §5.2 steps 1–4.

- [x] **Step 1: Add the plan type**

Append to `pkg/plugin/context.go` (inside `QueryContext`, after `SuppressUpstreamExecution`):

```go
	// DeferredInsert, when set by a QueryPlugin during OnQuery, switches Relay
	// into deferred-INSERT mode for this query (spec 2026-08-18 signed
	// envelope v2 §5.2): Relay answers the INSERT sample block itself from
	// SampleColumns, buffers every client Data packet (bounded by
	// MaxPayloadBytes) while running the strict data hooks, runs the strict
	// input-complete hook, and only then forwards Query + buffered Data +
	// terminator upstream — so a plugin can sign the payload before the
	// upstream ever sees the Query. Mutually exclusive with
	// SuppressUpstreamExecution; Relay rejects a query that sets both.
	DeferredInsert *DeferredInsertPlan
```

and after the struct:

```go
// DeferredInsertPlan tells Relay how to run the deferred-INSERT protocol.
type DeferredInsertPlan struct {
	// SampleColumns is the 0-row sample block Relay writes to the client in
	// place of the upstream's (name + exact ClickHouse type, table order).
	SampleColumns []chproto.SampleColumn
	// MaxPayloadBytes bounds the buffered on-wire payload; exceeding it
	// aborts the query with an Exception before any byte reaches upstream.
	MaxPayloadBytes uint64
}
```

- [x] **Step 2: Write the failing happy-path test + harness**

Create `pkg/proxy/relay_deferred_test.go`:

```go
package proxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

const deferredTestRev = 54453

// deferredInsertHooks marks every Query as a deferred INSERT and records the
// lifecycle hooks Relay fires.
type deferredInsertHooks struct {
	plugin.NoopHooks
	mu             sync.Mutex
	maxPayload     uint64
	alsoSuppress   bool
	strictDataErr  error
	strictDoneErr  error
	strictDataRaw  [][]byte
	strictComplete int
	inputCompletes int
	queryCompletes int
	queryAborts    int
}

func (h *deferredInsertHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	max := h.maxPayload
	if max == 0 {
		max = 1 << 20
	}
	qctx.DeferredInsert = &plugin.DeferredInsertPlan{
		SampleColumns:   []chproto.SampleColumn{{Name: "v", Type: "UInt64"}},
		MaxPayloadBytes: max,
	}
	qctx.SuppressUpstreamExecution = h.alsoSuppress
	return nil
}

func (h *deferredInsertHooks) OnClientDataStrict(_ context.Context, _ *plugin.QueryContext, raw []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.strictDataErr != nil {
		return h.strictDataErr
	}
	h.strictDataRaw = append(h.strictDataRaw, append([]byte(nil), raw...))
	return nil
}

func (h *deferredInsertHooks) OnQueryInputCompleteStrict(context.Context, *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.strictComplete++
	return h.strictDoneErr
}

func (h *deferredInsertHooks) OnQueryInputComplete(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.inputCompletes++
	h.mu.Unlock()
}

func (h *deferredInsertHooks) OnQueryComplete(context.Context, chsession.Session) {
	h.mu.Lock()
	h.queryCompletes++
	h.mu.Unlock()
}

func (h *deferredInsertHooks) OnQueryAbort(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.queryAborts++
	h.mu.Unlock()
}

func (h *deferredInsertHooks) counts() (strictData, strictComplete, inputCompletes, queryCompletes, aborts int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.strictDataRaw), h.strictComplete, h.inputCompletes, h.queryCompletes, h.queryAborts
}

// deferredHarness wires a Relay between two net.Pipe pairs with codecs at
// revision deferredTestRev, exactly like the staged-input tests above.
type deferredHarness struct {
	clientProxy, proxyClient     net.Conn
	upstreamProxy, proxyUpstream net.Conn
	relay                        *Relay
	loopErr                      chan error
	cancel                       context.CancelFunc
}

func newDeferredHarness(t *testing.T, hooks plugin.Hooks) *deferredHarness {
	t.Helper()
	clientProxy, proxyClient := net.Pipe()
	upstreamProxy, proxyUpstream := net.Pipe()
	sess := chsession.New(1, proxyClient)
	sess.Client().SetRevision(deferredTestRev)
	up := chproto.NewCodec(proxyUpstream, chproto.DirToUpstream)
	up.SetRevision(deferredTestRev)
	if err := sess.BindUpstream(context.Background(), up); err != nil {
		t.Fatalf("BindUpstream: %v", err)
	}
	r := &Relay{sess: sess, hooks: hooks}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	h := &deferredHarness{clientProxy: clientProxy, proxyClient: proxyClient, upstreamProxy: upstreamProxy, proxyUpstream: proxyUpstream, relay: r, loopErr: make(chan error, 2), cancel: cancel}
	go func() { h.loopErr <- r.clientToUpstream(ctx) }()
	go func() { h.loopErr <- r.upstreamToClient(ctx) }()
	t.Cleanup(func() {
		cancel()
		clientProxy.Close()
		upstreamProxy.Close()
	})
	return h
}

// close tears the pipes down and returns the two loop results.
func (h *deferredHarness) close(t *testing.T) []error {
	t.Helper()
	h.clientProxy.Close()
	h.upstreamProxy.Close()
	var errs []error
	for i := 0; i < 2; i++ {
		select {
		case err := <-h.loopErr:
			errs = append(errs, err)
		case <-time.After(3 * time.Second):
			t.Fatal("relay loop did not return after close")
		}
	}
	return errs
}

func encodeInsertQuery(t *testing.T, id, sql string) []byte {
	t.Helper()
	var qb proto.Buffer
	(&proto.Query{
		ID: id, Body: sql,
		Info: proto.ClientInfo{
			ProtocolVersion: deferredTestRev, Major: 24, Minor: 1,
			Interface: proto.InterfaceTCP,
			Query:     proto.ClientQueryInitial,
		},
	}).EncodeAware(&qb, deferredTestRev)
	return append([]byte(nil), qb.Buf...)
}

func encodeEmptyClientData(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	c := chproto.NewCodec(&readWriter{r: &bytes.Buffer{}, w: &buf}, chproto.DirFromClient)
	c.SetCompression(proto.CompressionDisabled)
	if err := c.WriteEmptyDataBlock(); err != nil {
		t.Fatalf("WriteEmptyDataBlock: %v", err)
	}
	return append([]byte(nil), buf.Bytes()...)
}

type readWriter struct {
	r *bytes.Buffer
	w *bytes.Buffer
}

func (rw *readWriter) Read(p []byte) (int, error)  { return rw.r.Read(p) }
func (rw *readWriter) Write(p []byte) (int, error) { return rw.w.Write(p) }

func readExact(t *testing.T, c net.Conn, n int) []byte {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, n)
	if _, err := io.ReadFull(c, buf); err != nil {
		t.Fatalf("read %d bytes: %v", n, err)
	}
	return buf
}

func writeAllConn(t *testing.T, c net.Conn, b []byte) {
	t.Helper()
	_ = c.SetWriteDeadline(time.Now().Add(2 * time.Second))
	if _, err := c.Write(b); err != nil {
		t.Fatalf("write %d bytes: %v", len(b), err)
	}
}

// upstreamAcceptsDeferredInsert plays a ClickHouse that receives Query,
// external-tables terminator, answers the sample block, receives payload +
// terminator, and answers EndOfStream. clientDone must be set by the client
// before the terminator was sent so the test proves the Query was deferred.
func upstreamAcceptsDeferredInsert(t *testing.T, conn net.Conn, clientDone *atomic.Bool, wantPayload []byte, done chan<- error) {
	codec := chproto.NewCodec(conn, chproto.DirFromClient)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
	if err != nil {
		done <- err
		return
	}
	if !clientDone.Load() {
		t.Errorf("upstream received the Query before the client finished sending its payload")
	}
	if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != "INSERT INTO t FORMAT Native" {
		t.Errorf("upstream query = %#v", pkt.Decoded)
	}
	first, err := codec.ReadPacket()
	if err != nil {
		done <- err
		return
	}
	if empty, _ := chproto.ClientDataPacketIsEmpty(first.Raw, proto.CompressionDisabled); !empty {
		t.Errorf("upstream expected the external-tables terminator first, got %x", first.Raw)
	}
	if _, err := conn.Write(encodeServerSampleDataPacket(t, deferredTestRev)); err != nil {
		done <- err
		return
	}
	data, err := codec.ReadPacket()
	if err != nil {
		done <- err
		return
	}
	if !bytes.Equal(data.Raw, wantPayload) {
		t.Errorf("upstream payload = %x, want %x", data.Raw, wantPayload)
	}
	term, err := codec.ReadPacket()
	if err != nil {
		done <- err
		return
	}
	if empty, _ := chproto.ClientDataPacketIsEmpty(term.Raw, proto.CompressionDisabled); !empty {
		t.Errorf("upstream expected terminator, got %x", term.Raw)
	}
	_, err = conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	done <- err
}

func TestRelay_DeferredInsert_HappyPathAnswersSampleLocallyAndForwardsAfterTerminator(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, upDone)

	// net.Pipe writes block until read, so the test client reads the locally
	// answered sample block BEFORE it sends the external-tables marker (a real
	// TCP client sends the marker immediately; the relay accepts either order).
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want %x", got, sample)
	}
	writeAllConn(t, h.clientProxy, empty) // end of external tables
	writeAllConn(t, h.clientProxy, nonEmpty)
	clientDone.Store(true)
	writeAllConn(t, h.clientProxy, empty) // terminator
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got server packet %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	strictData, strictComplete, inputCompletes, queryCompletes, aborts := hooks.counts()
	if strictData != 1 || !bytes.Equal(hooks.strictDataRaw[0], nonEmpty) {
		t.Fatalf("strict data captured %d packets, want exactly the payload", strictData)
	}
	if strictComplete != 1 || inputCompletes != 1 || queryCompletes != 1 || aborts != 0 {
		t.Fatalf("hooks strictComplete/input/complete/abort = %d/%d/%d/%d, want 1/1/1/0", strictComplete, inputCompletes, queryCompletes, aborts)
	}
	for _, err := range h.close(t) {
		if err != nil && !errors.Is(err, io.EOF) {
			t.Logf("relay loop returned: %v", err)
		}
	}
}
```

- [x] **Step 3: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/proxy:proxy_test --test_filter=TestRelay_DeferredInsert_HappyPath --test_output=errors`
Expected: FAIL — the client blocks reading the sample block (Relay forwarded the Query immediately; the test times out at 2s: `read 47 bytes: i/o timeout` or "upstream received the Query before the client finished").

- [x] **Step 4: Implement the Relay side**

Add fields to `Relay` (after `opaqueErr error`):

```go
	// deferredGate coordinates the deferred-INSERT sample step between the
	// two relay goroutines: clientToUpstream arms it right before writing the
	// deferred Query upstream; upstreamToClient settles it when the upstream
	// answers with its own sample Data block (consumed, never forwarded), an
	// Exception (forwarded to the client), or fails.
	deferredMu   sync.Mutex
	deferredGate *deferredSampleGate
```

Add types + helpers after `NewRelay`:

```go
type deferredSampleGate struct {
	queryID string
	done    chan deferredSampleResult // buffered(1): settle never blocks
}

type deferredSampleResult struct {
	exception *chproto.Exception // upstream rejected the Query at the sample step
	err       error              // transport / protocol failure; session closes
}

func (r *Relay) armDeferredSample(queryID string) <-chan deferredSampleResult {
	r.deferredMu.Lock()
	defer r.deferredMu.Unlock()
	g := &deferredSampleGate{queryID: queryID, done: make(chan deferredSampleResult, 1)}
	r.deferredGate = g
	return g.done
}

// settleDeferredSample delivers res to the armed gate (no-op when none) and
// disarms it. Returns whether a gate was armed.
func (r *Relay) settleDeferredSample(res deferredSampleResult) bool {
	r.deferredMu.Lock()
	defer r.deferredMu.Unlock()
	if r.deferredGate == nil {
		return false
	}
	r.deferredGate.done <- res
	r.deferredGate = nil
	return true
}

func (r *Relay) disarmDeferredSample() {
	r.deferredMu.Lock()
	r.deferredGate = nil
	r.deferredMu.Unlock()
}

func (r *Relay) deferredSampleArmed() bool {
	r.deferredMu.Lock()
	defer r.deferredMu.Unlock()
	return r.deferredGate != nil
}
```

In `clientToUpstream`, right after the `AbortWithSuccess` block (`rejectedQctx = qctx; continue }`) and BEFORE `up = r.sess.Upstream()`, insert:

```go
			if qctx.DeferredInsert != nil {
				if qctx.SuppressUpstreamExecution {
					err := fmt.Errorf("query %q: DeferredInsert and SuppressUpstreamExecution are mutually exclusive", q.ID)
					r.writeExceptionToClient(ctx, err)
					r.hooks.OnQueryAbort(ctx, qctx)
					r.hooks.OnQueryComplete(ctx, r.sess)
					rejectedQctx = qctx
					continue
				}
				if err := r.runDeferredInsert(ctx, qctx, clientCompression); err != nil {
					return err
				}
				continue
			}
```

Add `runDeferredInsert` (new function, e.g. after `clientToUpstream`):

```go
// runDeferredInsert drives the deferred-INSERT protocol (spec 2026-08-18
// signed envelope v2 §5.2) for one query whose OnQuery chain set
// qctx.DeferredInsert:
//
//  1. write the plan's 0-row sample block to the client (the Query is NOT
//     forwarded yet);
//  2. read client packets: the first empty Data before any payload is the
//     end-of-external-tables marker (kept for replay upstream); non-empty
//     Data runs OnClientDataStrict/OnClientData and is buffered under
//     MaxPayloadBytes; the empty terminator runs OnQueryInputCompleteStrict;
//     Cancel aborts with a local EndOfStream; anything else is a protocol
//     error;
//  3. forward Query (+ external-tables marker), wait for upstreamToClient to
//     consume the upstream's own sample block (or forward its Exception),
//     then forward every buffered Data packet and the terminator via
//     WriteRawPacket, and fire OnQueryInputComplete. The ordinary
//     upstreamToClient loop delivers the terminal EndOfStream/Exception.
//
// A nil return means the session stays usable (success, upstream Exception,
// or client Cancel); a non-nil return closes the session (fail-closed on
// strict-hook errors, limits, protocol violations, transport errors). The
// buffer is a local variable and is released on every path.
func (r *Relay) runDeferredInsert(ctx context.Context, qctx *plugin.QueryContext, compression proto.Compression) error {
	_, logger := log.FromContext(ctx)
	client := r.sess.Client()
	plan := qctx.DeferredInsert
	q := qctx.Query
	if plan.MaxPayloadBytes == 0 {
		err := fmt.Errorf("query %q: deferred INSERT plan requires MaxPayloadBytes > 0", q.ID)
		r.writeExceptionToClient(ctx, err)
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return err
	}
	rejectClose := func(err error) error {
		r.writeExceptionToClient(ctx, err)
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return err
	}
	if err := client.WriteSampleBlock(plan.SampleColumns); err != nil {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return fmt.Errorf("write deferred sample block: %w", err)
	}

	var (
		buffered        [][]byte
		bufferedBytes   uint64
		initialEmptyRaw []byte
		terminatorRaw   []byte
		sawPayload      bool
	)
	for terminatorRaw == nil {
		remaining := plan.MaxPayloadBytes - bufferedBytes
		if limit, enforce := r.hooks.ClientDataReadLimit(qctx); enforce && limit < remaining {
			remaining = limit
		}
		pkt, decErr := client.ReadPacketWithDataLimit(remaining, uint64(chproto.ClientQueryCode))
		if errors.Is(decErr, chproto.ErrPacketTooLarge) {
			return rejectClose(fmt.Errorf("deferred INSERT %q payload exceeds limit of %d bytes: %w", q.ID, plan.MaxPayloadBytes, decErr))
		}
		if pkt == nil || (decErr != nil && !errors.Is(decErr, chproto.ErrDecode)) {
			r.hooks.OnQueryAbort(ctx, qctx)
			r.hooks.OnQueryComplete(ctx, r.sess)
			if decErr == nil {
				decErr = io.EOF
			}
			return decErr
		}
		if r.obs != nil {
			r.obs.ClientPacket(clientPacketName(pkt.Type))
			if pkt.RawLen > 0 {
				r.obs.BytesTransferred("client_to_upstream", float64(pkt.RawLen))
			}
		}
		switch pkt.Type {
		case uint64(chproto.ClientDataCode):
			empty, err := chproto.ClientDataPacketIsEmpty(pkt.Raw, compression)
			if err != nil {
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return fmt.Errorf("classify deferred client data packet: %w", err)
			}
			switch {
			case empty && !sawPayload && initialEmptyRaw == nil:
				// End-of-external-tables marker every client sends after the
				// Query; replayed upstream so ClickHouse proceeds to its sample.
				initialEmptyRaw = append([]byte(nil), pkt.Raw...)
			case empty:
				terminatorRaw = append([]byte(nil), pkt.Raw...)
			default:
				sawPayload = true
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
		case uint64(chproto.ClientCancelCode):
			// Nothing reached upstream. ClickHouse answers a cancelled query
			// with EndOfStream; do the same locally and drop the buffer.
			r.hooks.OnQueryAbort(ctx, qctx)
			if err := client.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
				r.hooks.OnQueryComplete(ctx, r.sess)
				return fmt.Errorf("write end-of-stream after deferred cancel: %w", err)
			}
			r.hooks.OnQueryComplete(ctx, r.sess)
			logger.Debugw("deferred INSERT cancelled by client before forwarding", "query_id", q.ID)
			return nil
		default:
			return rejectClose(fmt.Errorf("client sent packet type %d (%s) while deferred INSERT %q was collecting its payload", pkt.Type, clientPacketName(pkt.Type), q.ID))
		}
	}
	if err := r.hooks.OnQueryInputCompleteStrict(ctx, qctx); err != nil {
		return rejectClose(fmt.Errorf("query input complete strict hook: %w", err))
	}

	up := r.sess.Upstream()
	if up == nil {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return chsession.ErrNoUpstream
	}
	up.SetCompression(compression)
	if !r.beginActiveQuery(q.ID) {
		return rejectClose(fmt.Errorf("query %q raced with another active upstream query", q.ID))
	}
	gate := r.armDeferredSample(q.ID)
	forwardFail := func(stage string, err error) error {
		r.disarmDeferredSample()
		r.takeActiveQuery()
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return fmt.Errorf("deferred INSERT %q %s to %s: %w", q.ID, stage, upstreamAddr(up), err)
	}
	if err := up.WriteQuery(q); err != nil {
		return forwardFail("forward query", err)
	}
	if initialEmptyRaw != nil {
		if err := up.WriteRawPacket(initialEmptyRaw); err != nil {
			return forwardFail("forward external-tables marker", err)
		}
	}
	select {
	case res := <-gate:
		if res.err != nil {
			r.hooks.OnQueryAbort(ctx, qctx)
			return fmt.Errorf("deferred INSERT %q sample negotiation: %w", q.ID, res.err)
		}
		if res.exception != nil {
			// upstreamToClient already forwarded the Exception and ran the
			// terminal hooks (takeActiveQuery + OnQueryComplete); only the
			// plugin-side buffer state is left to drop.
			logger.Debugw("deferred INSERT rejected by upstream at sample step", "query_id", q.ID, "code", res.exception.Code, "message", res.exception.Message)
			r.hooks.OnQueryAbort(ctx, qctx)
			return nil
		}
	case <-ctx.Done():
		r.disarmDeferredSample()
		r.hooks.OnQueryAbort(ctx, qctx)
		return ctx.Err()
	}
	for _, raw := range buffered {
		if err := up.WriteRawPacket(raw); err != nil {
			return forwardFail("forward payload", err)
		}
	}
	if err := up.WriteRawPacket(terminatorRaw); err != nil {
		return forwardFail("forward terminator", err)
	}
	r.hooks.OnQueryInputComplete(ctx, qctx)
	logger.Debugw("deferred INSERT forwarded", "query_id", q.ID, "packets", len(buffered), "payload_bytes", bufferedBytes)
	return nil
}
```

In `upstreamToClient`, add a defer at the top of the function (right after `client := r.sess.Client()`):

```go
	// A deferred INSERT may be waiting for this loop to consume the upstream
	// sample block; never leave it parked when this loop exits.
	defer r.settleDeferredSample(deferredSampleResult{err: errors.New("upstream relay loop exited")})
```

and after the observer counters (`r.obs.ServerPacket(packetName)` block) and BEFORE `isEndOfStream := ...`, insert:

```go
		if r.deferredSampleArmed() {
			switch pkt.Type {
			case uint64(chproto.ServerDataCode):
				// The upstream's INSERT sample block for a deferred INSERT: the
				// client already received housegate's locally synthesized sample
				// and is past its data phase, so this one is consumed here.
				r.settleDeferredSample(deferredSampleResult{})
				logger.Debugw("deferred INSERT upstream sample block consumed")
				continue
			case uint64(chproto.ServerTableColumnsCode):
				// ClickHouse sends TableColumns ahead of the sample when
				// input_format_defaults_for_omitted_fields is on; the client
				// would treat it as an unexpected packet after its data phase.
				continue
			case uint64(chproto.ServerEndOfStreamCode):
				err := fmt.Errorf("%w: upstream ended a deferred INSERT before its sample block", chproto.ErrMalformed)
				r.settleDeferredSample(deferredSampleResult{err: err})
				if _, active := r.takeActiveQuery(); active {
					r.hooks.OnQueryComplete(ctx, r.sess)
				}
				return err
			case uint64(chproto.ServerExceptionCode):
				if exc, ok := pkt.Decoded.(*chproto.Exception); ok {
					r.settleDeferredSample(deferredSampleResult{exception: exc})
				} else {
					r.settleDeferredSample(deferredSampleResult{err: fmt.Errorf("upstream exception packet decoded as %T", pkt.Decoded)})
				}
				// Fall through: the Exception is forwarded to the client and the
				// terminal hooks run exactly as for any other query.
			}
		}
```

- [x] **Step 5: Run the happy-path test and the whole proxy suite**

Run: `bazel test //pkg/proxy:proxy_test --test_output=errors`
Expected: PASS (existing staged-input / pipelining tests untouched).

- [x] **Step 6: Commit**

```bash
git add pkg/plugin/context.go pkg/proxy/relay.go pkg/proxy/relay_deferred_test.go
git commit -m "feat(relay): deferred-INSERT mode — local sample block, buffered payload, forward after strict input-complete"
```

### Task 11: Relay deferred-INSERT fragmentation / failure matrix

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Test: `pkg/proxy/relay_deferred_test.go` (append)

**Interfaces:**
- Consumes: harness from Task 10 (`newDeferredHarness`, `deferredInsertHooks`, `encodeInsertQuery`, `encodeEmptyClientData`, `readExact`, `writeAllConn`, `upstreamAcceptsDeferredInsert`).

- [x] **Step 1: Write the matrix tests**

Append to `pkg/proxy/relay_deferred_test.go`:

```go
// Query + external-tables marker + payload + terminator arrive in ONE TCP
// segment (net.Pipe delivers one Write as one Read into the codec's bufio).
func TestRelay_DeferredInsert_CoalescedQueryAndPayloadInOneSegment(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	clientDone.Store(true) // everything is on the wire before the relay reads
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, upDone)

	segment := append(append(append(encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"), empty...), nonEmpty...), empty...)
	writeAllConn(t, h.clientProxy, segment)
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want %x", got, sample)
	}
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if strictData, strictComplete, _, _, aborts := hooks.counts(); strictData != 1 || strictComplete != 1 || aborts != 0 {
		t.Fatalf("counts strictData/strictComplete/aborts = %d/%d/%d", strictData, strictComplete, aborts)
	}
	h.close(t)
}

// The terminator (and the payload packet) is split across two Writes.
func TestRelay_DeferredInsert_TerminatorSplitAcrossSegments(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	var clientDone atomic.Bool
	upDone := make(chan error, 1)
	go upstreamAcceptsDeferredInsert(t, h.upstreamProxy, &clientDone, nonEmpty, upDone)

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty[:3])
	time.Sleep(20 * time.Millisecond)
	writeAllConn(t, h.clientProxy, nonEmpty[3:])
	writeAllConn(t, h.clientProxy, empty[:1])
	clientDone.Store(true)
	time.Sleep(20 * time.Millisecond)
	writeAllConn(t, h.clientProxy, empty[1:])
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d, want EndOfStream", got[0])
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	if strictData, _, inputCompletes, _, aborts := hooks.counts(); strictData != 1 || inputCompletes != 1 || aborts != 0 {
		t.Fatalf("counts strictData/input/aborts = %d/%d/%d", strictData, inputCompletes, aborts)
	}
	h.close(t)
}

// Upstream answers the deferred Query with an Exception instead of a sample
// block: the Exception reaches the client, no payload is forwarded, the
// query lifecycle is aborted+completed, and the session stays usable.
func TestRelay_DeferredInsert_UpstreamExceptionAtSampleStep(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	upstreamGotPayload := make(chan struct{}, 1)
	go func() {
		codec := chproto.NewCodec(h.upstreamProxy, chproto.DirFromClient)
		codec.SetRevision(deferredTestRev)
		codec.SetCompression(proto.CompressionDisabled)
		if _, err := codec.ReadPacket(uint64(chproto.ClientQueryCode)); err != nil {
			upDone <- err
			return
		}
		if _, err := codec.ReadPacket(); err != nil { // external-tables marker
			upDone <- err
			return
		}
		if err := codec.WriteException(&chproto.Exception{Code: 60, Name: "DB::Exception", Message: "Table default.t does not exist"}); err != nil {
			upDone <- err
			return
		}
		upDone <- nil
		if _, err := codec.ReadPacket(); err == nil {
			upstreamGotPayload <- struct{}{}
		}
	}()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block mismatch")
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, empty)
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	exc, ok := pkt.Decoded.(*chproto.Exception)
	if !ok || exc.Code != 60 {
		t.Fatalf("client got %#v, want the upstream Exception (code 60)", pkt.Decoded)
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
	select {
	case <-upstreamGotPayload:
		t.Fatal("payload must not be forwarded after the upstream rejected the Query")
	case <-time.After(200 * time.Millisecond):
	}
	if _, _, inputCompletes, queryCompletes, aborts := hooks.counts(); inputCompletes != 0 || queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts input/complete/abort = %d/%d/%d, want 0/1/1", inputCompletes, queryCompletes, aborts)
	}
	// Session still alive: both relay loops must still be running.
	select {
	case err := <-h.loopErr:
		t.Fatalf("relay loop exited after a recoverable upstream Exception: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	h.close(t)
}

// Payload larger than the plan's MaxPayloadBytes: rejected before any byte
// reaches upstream, connection closes fail-closed.
func TestRelay_DeferredInsert_OversizedPayloadIsRejectedBeforeForwarding(t *testing.T) {
	hooks := &deferredInsertHooks{maxPayload: 8}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev) // > 8 bytes
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

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
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || !bytes.Contains([]byte(exc.Message), []byte("exceeds limit")) {
		t.Fatalf("client got %#v, want limit Exception", pkt.Decoded)
	}
	errs := h.close(t)
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes, want 0", n)
	}
	if _, _, _, queryCompletes, aborts := hooks.counts(); queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts complete/abort = %d/%d, want 1/1", queryCompletes, aborts)
	}
	sawLimit := false
	for _, err := range errs {
		if err != nil && errors.Is(err, chproto.ErrPacketTooLarge) {
			sawLimit = true
		}
	}
	if !sawLimit {
		t.Fatalf("clientToUpstream must return the limit error, got %v", errs)
	}
}

// Cancel mid-payload: local EndOfStream, buffer dropped, nothing upstream,
// session usable for the next query.
func TestRelay_DeferredInsert_CancelMidPayloadDropsBufferAndAnswersEndOfStream(t *testing.T) {
	hooks := &deferredInsertHooks{}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)

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
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	writeAllConn(t, h.clientProxy, []byte{byte(chproto.ClientCancelCode)})
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("client got %d after cancel, want EndOfStream", got[0])
	}
	if strictData, _, inputCompletes, queryCompletes, aborts := hooks.counts(); strictData != 1 || inputCompletes != 0 || queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts strictData/input/complete/abort = %d/%d/%d/%d, want 1/0/1/1", strictData, inputCompletes, queryCompletes, aborts)
	}
	select {
	case err := <-h.loopErr:
		t.Fatalf("relay loop exited after a client cancel: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	h.close(t)
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes after cancel, want 0", n)
	}
}

// A plan that also sets SuppressUpstreamExecution is rejected outright.
func TestRelay_DeferredInsert_MutuallyExclusiveWithSuppressUpstreamExecution(t *testing.T) {
	hooks := &deferredInsertHooks{alsoSuppress: true}
	h := newDeferredHarness(t, hooks)
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || !bytes.Contains([]byte(exc.Message), []byte("mutually exclusive")) {
		t.Fatalf("client got %#v, want mutual-exclusion Exception", pkt.Decoded)
	}
	if _, _, _, queryCompletes, aborts := hooks.counts(); queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts complete/abort = %d/%d, want 1/1", queryCompletes, aborts)
	}
	h.close(t)
}

// A strict data hook error aborts before forwarding and closes the session.
func TestRelay_DeferredInsert_StrictDataErrorAbortsBeforeForwarding(t *testing.T) {
	hooks := &deferredInsertHooks{strictDataErr: errors.New("compressed block rejected")}
	h := newDeferredHarness(t, hooks)
	nonEmpty := encodeNonEmptyClientDataPacket(t, deferredTestRev)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)
	empty := encodeEmptyClientData(t)
	upstreamBytes := make(chan int, 1)
	go func() {
		buf := make([]byte, 1024)
		n, _ := h.upstreamProxy.Read(buf)
		upstreamBytes <- n
	}()
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "qid", "INSERT INTO t FORMAT Native"))
	readExact(t, h.clientProxy, len(sample))
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, nonEmpty)
	clientCodec := chproto.NewCodec(h.clientProxy, chproto.DirToUpstream)
	clientCodec.SetRevision(deferredTestRev)
	_ = h.clientProxy.SetReadDeadline(time.Now().Add(2 * time.Second))
	pkt, err := clientCodec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); !ok || !bytes.Contains([]byte(exc.Message), []byte("compressed block rejected")) {
		t.Fatalf("client got %#v, want the strict hook error", pkt.Decoded)
	}
	h.close(t)
	if n := <-upstreamBytes; n != 0 {
		t.Fatalf("upstream received %d bytes, want 0", n)
	}
	if _, _, _, queryCompletes, aborts := hooks.counts(); queryCompletes != 1 || aborts != 1 {
		t.Fatalf("counts complete/abort = %d/%d, want 1/1", queryCompletes, aborts)
	}
}
```

- [x] **Step 2: Run the matrix (and with the race detector)**

Run: `bazel test //pkg/proxy:proxy_test --test_filter='TestRelay_DeferredInsert' --test_output=errors && bazel test //pkg/proxy:proxy_test --@rules_go//go/config:race --test_filter='TestRelay_DeferredInsert' --test_output=errors`
Expected: PASS ×2 (8 tests). If `TerminatorSplitAcrossSegments` flakes on the 20ms sleeps, raise them to 50ms — the assertion is on packet boundaries, not timing.

- [x] **Step 3: Commit**

```bash
git add pkg/proxy/relay_deferred_test.go
git commit -m "test(relay): deferred-INSERT fragmentation, upstream-exception, limit, cancel and exclusion matrix"
```

---

## Phase 4 — Agent-side statement plugin (spec §10 step 4, §5.1)

### Task 12: `storage_integrity.agent` config block

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/config/storage_integrity_config.go`, `pkg/config/config.go:288-308` (ModeAgent case)
- Test: `pkg/config/storage_integrity_agent_config_test.go` (new)

**Interfaces:**
- Produces: `config.StorageIntegrityAgentConfig{Enabled bool; NetworkID string; KeeperShardID uint32; StateDir string; MaxPayloadBytes uint64; RequireNetworkState bool}` at `cfg.StorageIntegrity.Agent` (yaml `storage_integrity.agent.*`); defaults `MaxPayloadBytes: 64<<20`, `RequireNetworkState: true`; `(StorageIntegrityConfig).validateAgent(root *Config) error` — agent-mode-only; a `storage_integrity.agent` block with `enabled: true` in server mode is rejected.

- [x] **Step 1: Write the failing test**

Create `pkg/config/storage_integrity_agent_config_test.go`:

```go
package config

import (
	"strings"
	"testing"
)

func agentSIBase() *Config {
	c := Default()
	c.Listen = "127.0.0.1:0"
	c.Agent.Mode = true
	c.Agent.PrivateKeyHex = "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	c.Agent.Upstream = "127.0.0.1:9000"
	c.NetworkState.Source = "network_state.yaml"
	c.StorageIntegrity.Agent.Enabled = true
	c.StorageIntegrity.Agent.NetworkID = "testnet-v2"
	c.StorageIntegrity.Agent.StateDir = "/tmp/hg-si-agent"
	return &c
}

func TestStorageIntegrityAgentConfig_Defaults(t *testing.T) {
	d := Default()
	if d.StorageIntegrity.Agent.Enabled {
		t.Fatal("agent SI must default off")
	}
	if d.StorageIntegrity.Agent.MaxPayloadBytes != 64<<20 {
		t.Fatalf("default max_payload_bytes = %d", d.StorageIntegrity.Agent.MaxPayloadBytes)
	}
	if !d.StorageIntegrity.Agent.RequireNetworkState {
		t.Fatal("require_network_state must default true")
	}
	if d.StorageIntegrity.Agent.KeeperShardID != 0 {
		t.Fatal("keeper_shard_id must default 0")
	}
}

func TestStorageIntegrityAgentConfig_Validate(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string
	}{
		{"valid", func(*Config) {}, ""},
		{"server mode rejects", func(c *Config) { c.Agent.Mode = false; c.Upstream = "127.0.0.1:9000"; c.CkhManagerConfigPath = "/tmp/x.yaml" }, "agent mode only"},
		{"missing network_id", func(c *Config) { c.StorageIntegrity.Agent.NetworkID = " " }, "network_id"},
		{"non-zero shard", func(c *Config) { c.StorageIntegrity.Agent.KeeperShardID = 3 }, "keeper_shard_id"},
		{"missing state_dir", func(c *Config) { c.StorageIntegrity.Agent.StateDir = "" }, "state_dir"},
		{"zero payload limit", func(c *Config) { c.StorageIntegrity.Agent.MaxPayloadBytes = 0 }, "max_payload_bytes"},
		{"missing network_state.source", func(c *Config) { c.NetworkState.Source = "" }, "network_state.source"},
		{"host-injected state allowed", func(c *Config) { c.NetworkState.Source = ""; c.StorageIntegrity.Agent.RequireNetworkState = false }, ""},
		{"disabled block ignored", func(c *Config) { c.StorageIntegrity.Agent = StorageIntegrityAgentConfig{} }, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := agentSIBase()
			tc.mutate(c)
			err := c.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
		})
	}
}
```

- [x] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/config:config_test --test_filter=TestStorageIntegrityAgentConfig --test_output=errors`
Expected: build error `c.StorageIntegrity.Agent undefined`.

- [x] **Step 3: Implement**

In `pkg/config/storage_integrity_config.go` add the field to `StorageIntegrityConfig`:

```go
type StorageIntegrityConfig struct {
	Ingress    StorageIntegrityIngressConfig    `json:"ingress"     yaml:"ingress"`
	Runtime    StorageIntegrityRuntimeConfig    `json:"runtime"     yaml:"runtime"`
	SafeMerges StorageIntegritySafeMergesConfig `json:"safe_merges" yaml:"safe_merges"`
	Agent      StorageIntegrityAgentConfig      `json:"agent"       yaml:"agent"`
}
```

Add the type:

```go
// StorageIntegrityAgentConfig turns on the agent-mode statement plugin
// (pkg/plugins/sistatement): the agent answers the INSERT sample block from
// the network-state table schema, buffers and hashes the payload, and signs
// the envelope-v2 statement token before forwarding.
type StorageIntegrityAgentConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
	// NetworkID is the Arbiter genesis network id signed into every token.
	NetworkID string `json:"network_id" yaml:"network_id"`
	// KeeperShardID must be 0 in v1.
	KeeperShardID uint32 `json:"keeper_shard_id" yaml:"keeper_shard_id"`
	// StateDir holds <account>.seq, the durable client_seq counter.
	StateDir string `json:"state_dir" yaml:"state_dir"`
	// MaxPayloadBytes bounds one buffered INSERT payload (default 64 MiB).
	MaxPayloadBytes uint64 `json:"max_payload_bytes" yaml:"max_payload_bytes"`
	// RequireNetworkState (default true) makes Validate insist on
	// network_state.source; hosts that inject Options.NetworkState set false.
	RequireNetworkState bool `json:"require_network_state" yaml:"require_network_state"`
}
```

In `defaultStorageIntegrityConfig()` add:

```go
		Agent: StorageIntegrityAgentConfig{
			MaxPayloadBytes:     defaultStorageIntegrityMaxPayloadBytes,
			RequireNetworkState: true,
		},
```

In `validate(mode Mode)` insert as the FIRST statement:

```go
	if c.Agent.Enabled && mode != ModeAgent {
		return errors.New("storage_integrity: storage_integrity.agent is agent mode only")
	}
```

Add the method:

```go
// validateAgent checks the agent-mode statement plugin block. Called from
// Config.Validate in ModeAgent only.
func (c StorageIntegrityConfig) validateAgent(root *Config) error {
	a := c.Agent
	if !a.Enabled {
		return nil
	}
	var errs []error
	if strings.TrimSpace(a.NetworkID) == "" {
		errs = append(errs, errors.New("storage_integrity.agent.network_id is required when storage_integrity.agent.enabled"))
	}
	if a.KeeperShardID != 0 {
		errs = append(errs, fmt.Errorf("storage_integrity.agent.keeper_shard_id must be 0 in v1, got %d", a.KeeperShardID))
	}
	if strings.TrimSpace(a.StateDir) == "" {
		errs = append(errs, errors.New("storage_integrity.agent.state_dir is required when storage_integrity.agent.enabled"))
	}
	if a.MaxPayloadBytes == 0 {
		errs = append(errs, errors.New("storage_integrity.agent.max_payload_bytes must be > 0 when storage_integrity.agent.enabled"))
	}
	if root.Agent.PrivateKeyHex == "" {
		errs = append(errs, errors.New("storage_integrity.agent requires agent.private_key_hex"))
	}
	if a.RequireNetworkState &&
		!root.NetworkState.IsYAMLSource() &&
		!root.NetworkState.IsRpcSource() &&
		root.ResolveRedisAddr(root.NetworkState.Source) == "" {
		errs = append(errs, errors.New("storage_integrity.agent requires network_state.source (or set storage_integrity.agent.require_network_state: false when the host injects Options.NetworkState)"))
	}
	if joined := errors.Join(errs...); joined != nil {
		return fmt.Errorf("storage_integrity.agent: %w", joined)
	}
	return nil
}
```

In `pkg/config/config.go` `case ModeAgent:` (after the Materialize validation) add:

```go
		if err := c.StorageIntegrity.validateAgent(c); err != nil {
			errs = append(errs, err)
		}
```

- [x] **Step 4: Run tests**

Run: `bazel test //pkg/config:config_test --test_output=errors`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add pkg/config
git commit -m "feat(config): storage_integrity.agent block for the agent-mode statement plugin"
```

### Task 13: `sistatement.SeqCounter` — durable per-account `client_seq`

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Create: `pkg/plugins/sistatement/seq.go`, `pkg/plugins/sistatement/seq_test.go`, `pkg/plugins/sistatement/doc.go`

**Interfaces:**
- Produces: `OpenSeqCounter(stateDir, account string) (*SeqCounter, error)`, `(*SeqCounter).Next() (uint64, error)` and `(*SeqCounter).AdvanceTo(seq uint64) error` (both write+fsync BEFORE returning a newly reserved value), `ErrClientSeqExhausted` (checked-increment sentinel; no wraparound), `(*SeqCounter).Last() uint64`, `(*SeqCounter).Path() string`.

- [x] **Step 1: Write the failing tests**

Create `pkg/plugins/sistatement/seq_test.go`:

```go
package sistatement

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSeqCounter_StartsAtOneAndPersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, "0xAbC0000000000000000000000000000000000001")
	if err != nil {
		t.Fatalf("OpenSeqCounter: %v", err)
	}
	for want := uint64(1); want <= 3; want++ {
		got, err := c.Next()
		if err != nil || got != want {
			t.Fatalf("Next = %d err=%v, want %d", got, err, want)
		}
	}
	if c.Last() != 3 {
		t.Fatalf("Last = %d, want 3", c.Last())
	}
	// File name is the lowercase account; content is the last issued seq.
	b, err := os.ReadFile(filepath.Join(dir, "0xabc0000000000000000000000000000000000001.seq"))
	if err != nil || string(b) != "3\n" {
		t.Fatalf("seq file = %q err=%v, want \"3\\n\"", b, err)
	}
	reopened, err := OpenSeqCounter(dir, "0xabc0000000000000000000000000000000000001")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got, _ := reopened.Next(); got != 4 {
		t.Fatalf("after restart Next = %d, want 4 (last+1)", got)
	}
}

func TestSeqCounter_AdvanceToReservesSuppliedSequenceDurably(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := c.Next(); err != nil || got != 1 {
		t.Fatalf("Next = %d err=%v, want 1", got, err)
	}
	if err := c.AdvanceTo(41); err != nil {
		t.Fatalf("AdvanceTo: %v", err)
	}
	if err := c.AdvanceTo(7); err != nil || c.Last() != 41 {
		t.Fatalf("AdvanceTo must not move backwards: last=%d err=%v", c.Last(), err)
	}
	reopened, err := OpenSeqCounter(dir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.Next(); err != nil || got != 42 {
		t.Fatalf("Next after supplied seq = %d err=%v, want 42", got, err)
	}
}

func TestSeqCounter_MaxUint64NeverWrapsOrAdvances(t *testing.T) {
	dir := t.TempDir()
	c, err := OpenSeqCounter(dir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.AdvanceTo(^uint64(0)); !errors.Is(err, ErrClientSeqExhausted) {
		t.Fatalf("AdvanceTo(MaxUint64) = %v, want ErrClientSeqExhausted", err)
	}
	if c.Last() != 0 {
		t.Fatalf("rejected terminal reservation changed last to %d", c.Last())
	}
	if got, err := c.Next(); err != nil || got != 1 {
		t.Fatalf("Next after rejected terminal reservation = %d, %v; want 1", got, err)
	}
	exhaustedDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(exhaustedDir, "0xabc.seq"), []byte("18446744073709551615\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	exhausted, err := OpenSeqCounter(exhaustedDir, "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	if got, err := exhausted.Next(); got != 0 || !errors.Is(err, ErrClientSeqExhausted) {
		t.Fatalf("Next at exhaustion = %d, %v", got, err)
	}
	if exhausted.Last() != ^uint64(0) {
		t.Fatalf("exhaustion changed durable high watermark to %d", exhausted.Last())
	}
}

func TestSeqCounter_RejectsCorruptFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "0xabc.seq"), []byte("not-a-number"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSeqCounter(dir, "0xabc"); err == nil {
		t.Fatal("corrupt seq file must fail closed")
	}
}

func TestSeqCounter_ConcurrentNextIsUnique(t *testing.T) {
	c, err := OpenSeqCounter(t.TempDir(), "0xabc")
	if err != nil {
		t.Fatal(err)
	}
	var (
		mu   sync.Mutex
		seen = map[uint64]bool{}
		wg   sync.WaitGroup
	)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.Next()
			if err != nil {
				t.Errorf("Next: %v", err)
				return
			}
			mu.Lock()
			if seen[v] {
				t.Errorf("duplicate seq %d", v)
			}
			seen[v] = true
			mu.Unlock()
		}()
	}
	wg.Wait()
	if len(seen) != 32 || c.Last() != 32 {
		t.Fatalf("seen=%d last=%d", len(seen), c.Last())
	}
}

func TestSeqCounter_RequiresAccountAndDir(t *testing.T) {
	if _, err := OpenSeqCounter("", "0xabc"); err == nil {
		t.Fatal("empty dir must be rejected")
	}
	if _, err := OpenSeqCounter(t.TempDir(), ""); err == nil {
		t.Fatal("empty account must be rejected")
	}
}
```

- [x] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: build error `undefined: OpenSeqCounter` (create `doc.go` first so the package exists — see Step 3).

- [x] **Step 3: Implement**

Create `pkg/plugins/sistatement/doc.go`:

```go
// Package sistatement is the agent-mode storage-integrity statement plugin
// (spec 2026-08-18 signed envelope v2 §5.1). For every payload-local Native
// INSERT it resolves the target table's declared schema from network state,
// asks Relay to answer the sample block locally (QueryContext.DeferredInsert),
// buffers and hashes the client's Data packets, and signs the
// housegate-statement-v2 token into SQL_x_statement_token before the Query is
// forwarded. It runs after materialize and before the agent auth signer.
package sistatement
```

Create `pkg/plugins/sistatement/seq.go`:

```go
package sistatement

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ErrClientSeqExhausted means the durable uint64 sequence space has no next
// value. Callers must fail closed; the counter never wraps to zero.
var ErrClientSeqExhausted = errors.New("sistatement: client_seq exhausted")

// SeqCounter is the durable per-account client_seq source (spec §5.1 /
// D6). Next and AdvanceTo write and fsync a newly reserved value BEFORE
// returning, so neither an agent-generated nor an SDK-supplied seq can be
// issued again across restarts; a crash between fsync and submission wastes
// one seq (a gap the accumulator's K=64 budget absorbs).
// One process per (state_dir, account) — sharing a key across agents is out
// of scope.
type SeqCounter struct {
	path string
	mu   sync.Mutex
	last uint64
}

// OpenSeqCounter loads <stateDir>/<lowercase account>.seq (0 when absent).
func OpenSeqCounter(stateDir, account string) (*SeqCounter, error) {
	stateDir = strings.TrimSpace(stateDir)
	account = strings.ToLower(strings.TrimSpace(account))
	if stateDir == "" {
		return nil, errors.New("sistatement: state dir is required")
	}
	if account == "" {
		return nil, errors.New("sistatement: account is required")
	}
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("sistatement: create state dir: %w", err)
	}
	c := &SeqCounter{path: filepath.Join(stateDir, account+".seq")}
	b, err := os.ReadFile(c.path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		c.last = 0
	case err != nil:
		return nil, fmt.Errorf("sistatement: read %s: %w", c.path, err)
	default:
		last, perr := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
		if perr != nil {
			return nil, fmt.Errorf("sistatement: corrupt seq file %s: %w", c.path, perr)
		}
		c.last = last
	}
	return c, nil
}

// Path returns the backing file (for logs/tests).
func (c *SeqCounter) Path() string { return c.path }

// Last returns the last issued seq (0 before the first Next).
func (c *SeqCounter) Last() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.last
}

func (c *SeqCounter) persistLocked(next uint64) error {
	tmp := c.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("sistatement: open %s: %w", tmp, err)
	}
	if _, err := f.WriteString(strconv.FormatUint(next, 10) + "\n"); err != nil {
		_ = f.Close()
		return fmt.Errorf("sistatement: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sistatement: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("sistatement: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, c.path); err != nil {
		return fmt.Errorf("sistatement: rename %s: %w", tmp, err)
	}
	if dir, err := os.Open(filepath.Dir(c.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	c.last = next
	return nil
}

// AdvanceTo reserves seq if it is above the current durable high watermark.
// Lower/equal SDK values never move the counter backwards.
func (c *SeqCounter) AdvanceTo(seq uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if seq == ^uint64(0) {
		return ErrClientSeqExhausted
	}
	if seq <= c.last {
		return nil
	}
	return c.persistLocked(seq)
}

// Next issues last+1 after durably recording it (temp file, fsync, rename,
// directory fsync).
func (c *SeqCounter) Next() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.last == ^uint64(0) {
		return 0, ErrClientSeqExhausted
	}
	next := c.last + 1
	if err := c.persistLocked(next); err != nil {
		return 0, err
	}
	return next, nil
}
```

- [x] **Step 4: Run tests**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: PASS (all counter tests, including terminal-value rejection and exhausted-file reopen).

- [x] **Step 5: Commit**

```bash
git add pkg/plugins/sistatement
git commit -m "feat(sistatement): durable per-account client_seq counter"
```

### Task 14: `sistatement` SQL helpers — target table, column list, sample columns, USE

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Create: `pkg/plugins/sistatement/sqlparse.go`, `pkg/plugins/sistatement/sqlparse_test.go`
- Modify: `pkg/storageintegrity/intake.go` (export `ParseFlatStatementID`)

**Interfaces:**
- Produces: `insertTargetPath(sql) (string, bool)`, `resolveTargetTableID(sql, sessionDB string) (tableID string, err error)`, `insertColumnList(sql) ([]string, bool, error)`, `sampleColumnsFor(schema payloadexec.TableSchema, listed []string) ([]chproto.SampleColumn, error)`, `matchUse(sql) (string, bool)`; `sicore.ParseFlatStatementID(id string) (account string, seq uint64, nonce string, err error)`.

- [x] **Step 1: Write the failing tests**

Create `pkg/plugins/sistatement/sqlparse_test.go`:

```go
package sistatement

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func testSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "shop.orders",
		PartitionBy: "region",
		Columns: []lthash.Column{
			{Name: "id", Type: "UInt64"},
			{Name: "region", Type: "String"},
			{Name: "amount", Type: "Float64"},
		},
	}
}

func TestResolveTargetTableID(t *testing.T) {
	cases := []struct {
		sql, sessionDB, want, wantErr string
	}{
		{"INSERT INTO shop.orders FORMAT Native", "", "shop.orders", ""},
		{"INSERT INTO `shop`.`orders` (id, region, amount) FORMAT Native", "", "shop.orders", ""},
		{"insert into orders format Native", "shop", "shop.orders", ""},
		{"INSERT INTO orders FORMAT Native", "", "", "database-qualified"},
		{"SELECT 1", "", "", "not an INSERT"},
	}
	for _, tc := range cases {
		got, err := resolveTargetTableID(tc.sql, tc.sessionDB)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("%q: err=%v want %q", tc.sql, err, tc.wantErr)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Fatalf("%q: got %q err=%v, want %q", tc.sql, got, err, tc.want)
		}
	}
}

func TestInsertColumnList(t *testing.T) {
	cases := []struct {
		sql      string
		want     []string
		explicit bool
		wantErr  bool
	}{
		{"INSERT INTO shop.orders FORMAT Native", nil, false, false},
		{"INSERT INTO shop.orders (id, region, amount) FORMAT Native", []string{"id", "region", "amount"}, true, false},
		{"INSERT INTO shop.orders (`region`, \"id\", amount) FORMAT Native", []string{"region", "id", "amount"}, true, false},
		{"INSERT INTO shop.orders ( id ,region ) FORMAT Native", []string{"id", "region"}, true, false},
		{"INSERT INTO shop.orders (id, ) FORMAT Native", nil, true, true},
		{"INSERT INTO shop.orders (id region) FORMAT Native", nil, true, true},
	}
	for _, tc := range cases {
		got, explicit, err := insertColumnList(tc.sql)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("%q: expected error", tc.sql)
			}
			continue
		}
		if err != nil || explicit != tc.explicit || strings.Join(got, ",") != strings.Join(tc.want, ",") {
			t.Fatalf("%q: got %v explicit=%v err=%v", tc.sql, got, explicit, err)
		}
	}
}

func TestSampleColumnsFor(t *testing.T) {
	schema := testSchema()
	all, err := sampleColumnsFor(schema, nil)
	if err != nil || len(all) != 3 || all[0].Name != "id" || all[0].Type != "UInt64" || all[2].Name != "amount" {
		t.Fatalf("schema order: %v err=%v", all, err)
	}
	perm, err := sampleColumnsFor(schema, []string{"amount", "id", "region"})
	if err != nil || perm[0].Name != "amount" || perm[1].Name != "id" || perm[2].Name != "region" || perm[0].Type != "Float64" {
		t.Fatalf("permutation order: %v err=%v", perm, err)
	}
	if _, err := sampleColumnsFor(schema, []string{"id", "region"}); err == nil || !strings.Contains(err.Error(), "amount") {
		t.Fatalf("subset must be rejected naming the missing column: %v", err)
	}
	if _, err := sampleColumnsFor(schema, []string{"id", "region", "amount", "extra"}); err == nil || !strings.Contains(err.Error(), "extra") {
		t.Fatalf("unknown column must be rejected: %v", err)
	}
	if _, err := sampleColumnsFor(schema, []string{"id", "id", "region"}); err == nil {
		t.Fatal("duplicate column must be rejected")
	}
	if _, err := sampleColumnsFor(payloadexec.TableSchema{TableID: "x.y"}, nil); err == nil {
		t.Fatal("empty schema must be rejected")
	}
}

func TestMatchUse(t *testing.T) {
	if db, ok := matchUse("USE shop"); !ok || db != "shop" {
		t.Fatalf("USE shop → %q %v", db, ok)
	}
	if db, ok := matchUse("  use `shop-2`; "); !ok || db != "shop-2" {
		t.Fatalf("quoted USE → %q %v", db, ok)
	}
	if _, ok := matchUse("USE shop SETTINGS x=1"); ok {
		t.Fatal("USE with SETTINGS must not match")
	}
	if _, ok := matchUse("SELECT 1"); ok {
		t.Fatal("SELECT must not match")
	}
}
```

- [x] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sistatement:sistatement_test --test_filter='TestResolveTargetTableID|TestInsertColumnList|TestSampleColumnsFor|TestMatchUse' --test_output=errors`
Expected: build error `undefined: resolveTargetTableID`.

- [x] **Step 3: Implement**

Create `pkg/plugins/sistatement/sqlparse.go`:

```go
package sistatement

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sqlident"
)

const identifierPath = "(?:`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*)(?:\\s*\\.\\s*(?:`(?:``|[^`])+`|[A-Za-z_][A-Za-z0-9_]*))*"

var (
	// insertTargetPattern mirrors pkg/plugins/storageintegrity's pattern so
	// agent and ingress agree on what the target path is.
	insertTargetPattern = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(` + identifierPath + `)(?:\s|\(|;|$)`)
	// useRegex mirrors pkg/plugins/forward/use_regex.go (standalone USE only).
	useRegex = regexp.MustCompile("(?i)^\\s*USE\\s+(?:`([A-Za-z0-9_-]+)`|\"([A-Za-z0-9_-]+)\"|([A-Za-z0-9_-]+))\\s*;?\\s*$")
)

// insertTargetPath returns the raw target identifier path of an INSERT.
func insertTargetPath(sql string) (string, bool) {
	m := insertTargetPattern.FindStringSubmatch(sql)
	if len(m) != 2 {
		return "", false
	}
	return m[1], true
}

// resolveTargetTableID returns the logical "<db>.<table>" id: the SQL's own
// database when qualified, else sessionDB. Unqualified + no session database
// is an error (the SI lane needs an unambiguous table id to look up the
// declared schema).
func resolveTargetTableID(sql, sessionDB string) (string, error) {
	target, ok := insertTargetPath(sql)
	if !ok {
		return "", errors.New("sistatement: statement is not an INSERT")
	}
	db, table := sqlident.SplitLastPath(target)
	if table == "" {
		return "", fmt.Errorf("sistatement: invalid INSERT target %q", target)
	}
	if db == "" {
		if strings.TrimSpace(sessionDB) == "" {
			return "", fmt.Errorf("sistatement: INSERT target %q must be database-qualified (db.table) or the session must select a database", target)
		}
		db = sqlident.NormalizePath(sqlident.Quote(sessionDB))
		if db == "" {
			return "", fmt.Errorf("sistatement: invalid session database %q", sessionDB)
		}
	}
	return db + "." + table, nil
}

// insertColumnList parses the optional "(c1, c2, ...)" list that follows the
// INSERT target. explicit=false when there is no list.
func insertColumnList(sql string) ([]string, bool, error) {
	loc := insertTargetPattern.FindStringSubmatchIndex(sql)
	if loc == nil {
		return nil, false, errors.New("sistatement: statement is not an INSERT")
	}
	pos := loc[3] // end of the target path
	for pos < len(sql) && (sql[pos] == ' ' || sql[pos] == '\t' || sql[pos] == '\n' || sql[pos] == '\r') {
		pos++
	}
	if pos >= len(sql) || sql[pos] != '(' {
		return nil, false, nil
	}
	pos++
	var (
		cols    []string
		current strings.Builder
		haveTok bool
	)
	flush := func() error {
		if !haveTok {
			return errors.New("sistatement: empty column name in INSERT column list")
		}
		cols = append(cols, current.String())
		current.Reset()
		haveTok = false
		return nil
	}
	for pos < len(sql) {
		ch := sql[pos]
		switch {
		case ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r':
			pos++
		case ch == ',':
			if err := flush(); err != nil {
				return nil, true, err
			}
			pos++
		case ch == ')':
			if err := flush(); err != nil {
				return nil, true, err
			}
			return cols, true, nil
		case ch == '`' || ch == '"':
			if haveTok {
				return nil, true, fmt.Errorf("sistatement: unexpected quoted identifier at offset %d in INSERT column list", pos)
			}
			quote := ch
			pos++
			for {
				if pos >= len(sql) {
					return nil, true, errors.New("sistatement: unterminated quoted identifier in INSERT column list")
				}
				if sql[pos] == quote {
					if pos+1 < len(sql) && sql[pos+1] == quote {
						current.WriteByte(quote)
						pos += 2
						continue
					}
					pos++
					break
				}
				current.WriteByte(sql[pos])
				pos++
			}
			haveTok = true
		case ch == '_' || (ch >= 'A' && ch <= 'Z') || (ch >= 'a' && ch <= 'z'):
			if haveTok {
				return nil, true, fmt.Errorf("sistatement: expected ',' or ')' before %q in INSERT column list", ch)
			}
			start := pos
			for pos < len(sql) && (sql[pos] == '_' || (sql[pos] >= 'A' && sql[pos] <= 'Z') || (sql[pos] >= 'a' && sql[pos] <= 'z') || (sql[pos] >= '0' && sql[pos] <= '9')) {
				pos++
			}
			current.WriteString(sql[start:pos])
			haveTok = true
		default:
			return nil, true, fmt.Errorf("sistatement: unexpected %q in INSERT column list", ch)
		}
	}
	return nil, true, errors.New("sistatement: unterminated INSERT column list")
}

// sampleColumnsFor builds the sample block columns: schema order when the
// INSERT has no column list, else the SQL's order — which must name every
// declared column exactly once (the Native decoder requires the full column
// set, so a subset would only fail later at the ingress).
func sampleColumnsFor(schema payloadexec.TableSchema, listed []string) ([]chproto.SampleColumn, error) {
	byName := make(map[string]string, len(schema.Columns))
	for _, c := range schema.Columns {
		if c.Name == "_hg_row_id" {
			continue
		}
		byName[c.Name] = c.Type
	}
	if len(byName) == 0 {
		return nil, fmt.Errorf("sistatement: declared schema for %s has no columns", schema.TableID)
	}
	if len(listed) == 0 {
		out := make([]chproto.SampleColumn, 0, len(byName))
		for _, c := range schema.Columns {
			if c.Name == "_hg_row_id" {
				continue
			}
			out = append(out, chproto.SampleColumn{Name: c.Name, Type: c.Type})
		}
		return out, nil
	}
	seen := make(map[string]bool, len(listed))
	out := make([]chproto.SampleColumn, 0, len(listed))
	for _, name := range listed {
		typ, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("sistatement: INSERT lists unknown column %q for %s", name, schema.TableID)
		}
		if seen[name] {
			return nil, fmt.Errorf("sistatement: INSERT lists column %q twice", name)
		}
		seen[name] = true
		out = append(out, chproto.SampleColumn{Name: name, Type: typ})
	}
	for _, c := range schema.Columns {
		if c.Name != "_hg_row_id" && !seen[c.Name] {
			return nil, fmt.Errorf("sistatement: SI INSERT into %s must list every declared column (missing %q)", schema.TableID, c.Name)
		}
	}
	return out, nil
}

// matchUse returns (database, true) for a standalone USE statement.
func matchUse(sql string) (string, bool) {
	m := useRegex.FindStringSubmatch(sql)
	if m == nil {
		return "", false
	}
	for _, g := range m[1:] {
		if g != "" {
			return g, true
		}
	}
	return "", false
}
```

Append to `pkg/storageintegrity/intake.go` (after `parseFlatStatementID`):

```go
// ParseFlatStatementID validates the flat "<lowercase 0x account>:<seq>:<nonce>"
// form and returns its parts. Exported for the agent-side plugin so both ends
// apply identical rules.
func ParseFlatStatementID(id string) (account string, seq uint64, nonce string, err error) {
	parsed, err := parseFlatStatementID(id)
	if err != nil {
		return "", 0, "", err
	}
	return parsed.ClientAccount, parsed.ClientSeq, parsed.ClientNonce, nil
}
```

- [x] **Step 4: Run tests**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sistatement:sistatement_test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add pkg/plugins/sistatement pkg/storageintegrity/intake.go
git commit -m "feat(sistatement): INSERT target/column-list/sample-column helpers, USE detection"
```

### Task 15: `sistatement.Plugin` — OnQuery / strict data / strict input-complete / abort / close

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Create: `pkg/plugins/sistatement/plugin.go`, `pkg/plugins/sistatement/plugin_test.go`

**Interfaces:**
- Consumes: Task 4 (`auth.StatementSignerV2`, `auth.JWSStatementPayloadV2`, `auth.StatementTokenSettingKey`), Task 8 (`sicore.RejectUserSettings`, `sicore.EmptySettingsHash`, `sicore.InsertPayloadEncoding`, `sicore.PayloadEncodingClickHouseNativeData`), Task 6 (`payloadexec.RowIDProfileID`), Task 10 (`plugin.DeferredInsertPlan`), Task 13 (`SeqCounter`), Task 14 helpers, `schemaregistry.NewNetworkStateLoader`, `registry.TableSchemas`.
- Produces: `sistatement.Options`, `sistatement.New(opts) (*Plugin, error)`; `*Plugin` implements `plugin.QueryPlugin`, `plugin.StrictDataPlugin`, `plugin.StrictDataLimitPlugin`, `plugin.QueryInputCompleteStrictPlugin`, `plugin.QueryAbortPlugin`, `plugin.ClosePlugin`.

- [x] **Step 1: Write the failing tests**

Create `pkg/plugins/sistatement/plugin_test.go`:

```go
package sistatement

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

const (
	testKey       = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testNetworkID = "testnet-v2"
	testRevision  = 54460
)

type fakeSession struct {
	id    int64
	state *chsession.SessionState
}

func (s *fakeSession) ID() int64                                          { return s.id }
func (s *fakeSession) State() *chsession.SessionState                     { return s.state }
func (s *fakeSession) Client() *chproto.Codec                             { return nil }
func (s *fakeSession) Upstream() *chproto.Codec                           { return nil }
func (s *fakeSession) RemoteAddr() net.Addr                               { return nil }
func (s *fakeSession) Close() error                                       { return nil }
func (s *fakeSession) BindUpstream(context.Context, *chproto.Codec) error { return nil }
func (s *fakeSession) RebindUpstream(context.Context, *chproto.Codec, bool) error {
	return nil
}
func (s *fakeSession) RebindToPeer(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}
func (s *fakeSession) RebindToLocal(context.Context, *chproto.Codec, *chproto.ClientHello) error {
	return nil
}

func newSession(id int64, logicalDB string) *fakeSession {
	st := chsession.NewSessionState()
	st.ClientRevision = testRevision
	if logicalDB != "" {
		st.SetLogicalDatabase(logicalDB)
	}
	return &fakeSession{id: id, state: st}
}

func declareSchema(t *testing.T, ns *network.InMemoryNetworkState, schema payloadexec.TableSchema) string {
	t.Helper()
	db, table, ok := strings.Cut(schema.TableID, ".")
	if !ok {
		t.Fatalf("schema table id %q", schema.TableID)
	}
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	hash := payloadexec.TableSchemaHash(testNetworkID, schema)
	ns.TableSchemas[db+"/"+table+"@1"] = network.TableSchemaInfo{DatabaseId: db, TableId: table, Version: 1, SchemaHash: hash, SchemaJson: string(js)}
	return hash
}

func newTestPlugin(t *testing.T, ns *network.InMemoryNetworkState, stateDir string) (*Plugin, *auth.RelaySigner) {
	t.Helper()
	signer, err := auth.NewRelaySigner(testKey)
	if err != nil {
		t.Fatal(err)
	}
	seq, err := OpenSeqCounter(stateDir, signer.Address())
	if err != nil {
		t.Fatal(err)
	}
	p, err := New(Options{Signer: signer, Schemas: ns, NetworkID: testNetworkID, KeeperShardID: 0, Seq: seq, MaxPayloadBytes: 1 << 20})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return p, signer
}

func insertQctx(sess *fakeSession, sql string) *plugin.QueryContext {
	return &plugin.QueryContext{
		Session:     sess,
		OriginalSQL: sql,
		Query:       &chproto.Query{ID: "client-uuid-1", Body: sql, Compression: proto.CompressionDisabled},
		Values:      map[string]any{},
	}
}

func encodeRows(t *testing.T) []byte {
	t.Helper()
	id := proto.ColUInt64{1, 2}
	region := proto.ColStr{}
	region.Append("eu")
	region.Append("us")
	amount := proto.ColFloat64{1.5, 2.5}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 2, Columns: 3}).EncodeBlock(&buf, testRevision, proto.Input{{Name: "id", Data: &id}, {Name: "region", Data: &region}, {Name: "amount", Data: &amount}}); err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), buf.Buf...)
}

func TestPlugin_HappyPathSignsStatementTokenAfterPayload(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	schemaHash := declareSchema(t, ns, testSchema())
	p, signer := newTestPlugin(t, ns, t.TempDir())
	sess := newSession(7, "")
	sql := "INSERT INTO shop.orders FORMAT Native"
	qctx := insertQctx(sess, sql)

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if qctx.DeferredInsert == nil || len(qctx.DeferredInsert.SampleColumns) != 3 || qctx.DeferredInsert.SampleColumns[1] != (chproto.SampleColumn{Name: "region", Type: "String"}) {
		t.Fatalf("DeferredInsert = %#v", qctx.DeferredInsert)
	}
	if qctx.DeferredInsert.MaxPayloadBytes != 1<<20 {
		t.Fatalf("MaxPayloadBytes = %d", qctx.DeferredInsert.MaxPayloadBytes)
	}
	account, seq, nonce, err := sicore.ParseFlatStatementID(qctx.Query.ID)
	if err != nil || account != signer.Address() || seq != 1 || len(nonce) != 32 {
		t.Fatalf("statement id %q: account=%s seq=%d nonce=%q err=%v", qctx.Query.ID, account, seq, nonce, err)
	}
	payload := encodeRows(t)
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	if limit, enforce := p.ClientDataReadLimit(qctx); !enforce || limit != (1<<20)-uint64(len(payload)) {
		t.Fatalf("ClientDataReadLimit = %d/%v", limit, enforce)
	}
	if err := p.OnQueryInputCompleteStrict(context.Background(), qctx); err != nil {
		t.Fatalf("OnQueryInputCompleteStrict: %v", err)
	}
	var token string
	for _, s := range qctx.Query.Settings {
		if s.Key == auth.StatementTokenSettingKey {
			token = s.Value
			if !s.Custom || !strings.HasPrefix(s.Value, "'") {
				t.Fatalf("statement token setting must be Custom and quoted: %#v", s)
			}
		}
	}
	if token == "" {
		t.Fatal("SQL_x_statement_token missing")
	}
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := auth.JWSStatementPayloadV2{
		NetworkID:      testNetworkID,
		KeeperShardID:  0,
		StatementID:    qctx.Query.ID,
		SQLHash:        replay.DigestString(sql),
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     schemaHash,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: testRevision,
		TargetTableID:  "shop.orders",
		RowIDProfileID: payloadexec.RowIDProfileID,
	}
	if got, err := validator.ValidateStatementV2(token, want); err != nil || got != signer.Address() {
		t.Fatalf("token does not bind the envelope: got=%s err=%v", got, err)
	}
	// After signing the per-session state is released; the next INSERT gets seq 2.
	next := insertQctx(sess, sql)
	if err := p.OnQuery(context.Background(), next); err != nil {
		t.Fatalf("second OnQuery: %v", err)
	}
	if _, seq, _, _ := sicore.ParseFlatStatementID(next.Query.ID); seq != 2 {
		t.Fatalf("second seq = %d, want 2", seq)
	}
}

func TestPlugin_SeqSurvivesRestart(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	dir := t.TempDir()
	p, _ := newTestPlugin(t, ns, dir)
	q := insertQctx(newSession(1, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	p2, _ := newTestPlugin(t, ns, dir)
	q2 := insertQctx(newSession(2, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p2.OnQuery(context.Background(), q2); err != nil {
		t.Fatal(err)
	}
	if _, seq, _, _ := sicore.ParseFlatStatementID(q2.Query.ID); seq != 2 {
		t.Fatalf("seq after restart = %d, want 2", seq)
	}
}

func TestPlugin_ClientSuppliedStatementIDIsKeptOnlyForOwnAccount(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, signer := newTestPlugin(t, ns, t.TempDir())
	own := insertQctx(newSession(1, ""), "INSERT INTO shop.orders FORMAT Native")
	own.Query.ID = "0x" + strings.ToUpper(strings.TrimPrefix(signer.Address(), "0x")) + ":41:sdk-nonce"
	if err := p.OnQuery(context.Background(), own); err != nil || own.Query.ID != signer.Address()+":41:sdk-nonce" {
		t.Fatalf("own-account id must be kept: id=%q err=%v", own.Query.ID, err)
	}
	generated := insertQctx(newSession(2, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), generated); err != nil {
		t.Fatal(err)
	}
	if account, seq, _, err := sicore.ParseFlatStatementID(generated.Query.ID); err != nil || account != signer.Address() || seq != 42 {
		t.Fatalf("generated id after SDK reservation = %q account=%s seq=%d err=%v, want seq 42", generated.Query.ID, account, seq, err)
	}
	foreign := insertQctx(newSession(3, ""), "INSERT INTO shop.orders FORMAT Native")
	foreign.Query.ID = "0x0000000000000000000000000000000000000001:5:n"
	if err := p.OnQuery(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	if account, _, _, _ := sicore.ParseFlatStatementID(foreign.Query.ID); account != signer.Address() {
		t.Fatalf("foreign-account id must be replaced, got %q", foreign.Query.ID)
	}
}

func TestPlugin_ClientSuppliedMaxSequenceIsRejectedWithoutAdvancing(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, signer := newTestPlugin(t, ns, t.TempDir())
	terminal := insertQctx(newSession(1, ""), "INSERT INTO shop.orders FORMAT Native")
	terminal.Query.ID = signer.Address() + ":18446744073709551615:sdk-nonce"
	if err := p.OnQuery(context.Background(), terminal); !errors.Is(err, ErrClientSeqExhausted) {
		t.Fatalf("terminal supplied sequence = %v, want ErrClientSeqExhausted", err)
	}
	generated := insertQctx(newSession(2, ""), "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), generated); err != nil {
		t.Fatal(err)
	}
	if _, seq, _, err := sicore.ParseFlatStatementID(generated.Query.ID); err != nil || seq != 1 {
		t.Fatalf("generated id after rejected terminal reservation = %q seq=%d err=%v, want seq 1", generated.Query.ID, seq, err)
	}
}

func TestPlugin_Rejections(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, _ := newTestPlugin(t, ns, t.TempDir())
	cases := []struct {
		name    string
		mutate  func(*plugin.QueryContext)
		wantErr string
	}{
		{"schema missing", func(q *plugin.QueryContext) { q.Query.Body = "INSERT INTO shop.unknown FORMAT Native" }, "not declared"},
		{"compressed", func(q *plugin.QueryContext) { q.Query.Compression = proto.CompressionEnabled }, "compressed"},
		{"user setting", func(q *plugin.QueryContext) {
			q.Query.Settings = []chproto.Setting{{Key: "SQL_x_payer", Value: "'0xabc'", Custom: true}, {Key: "async_insert", Value: "1"}}
		}, "async_insert"},
		{"unqualified without session db", func(q *plugin.QueryContext) { q.Query.Body = "INSERT INTO orders FORMAT Native" }, "database-qualified"},
		{"column subset", func(q *plugin.QueryContext) { q.Query.Body = "INSERT INTO shop.orders (id, region) FORMAT Native" }, "amount"},
		{"unknown revision", func(q *plugin.QueryContext) { q.Session.State().ClientRevision = 0 }, "revision"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := insertQctx(newSession(int64(10+i), ""), "INSERT INTO shop.orders FORMAT Native")
			tc.mutate(q)
			err := p.OnQuery(context.Background(), q)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
			}
			if q.DeferredInsert != nil {
				t.Fatal("rejected query must not carry a deferred plan")
			}
		})
	}
}

func TestPlugin_NonSILaneStatementsPassThrough(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	p, _ := newTestPlugin(t, ns, t.TempDir())
	for _, sql := range []string{"SELECT 1", "INSERT INTO shop.orders VALUES (1, 'eu', 1.5)", "INSERT INTO shop.orders SELECT * FROM src", "CREATE TABLE x (a UInt8) ENGINE=Memory"} {
		q := insertQctx(newSession(1, ""), sql)
		if err := p.OnQuery(context.Background(), q); err != nil || q.DeferredInsert != nil || q.Query.ID != "client-uuid-1" {
			t.Fatalf("%q: err=%v deferred=%v id=%q", sql, err, q.DeferredInsert, q.Query.ID)
		}
	}
}

func TestPlugin_UseTrackingResolvesUnqualifiedTarget(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	p, _ := newTestPlugin(t, ns, t.TempDir())
	sess := newSession(3, "other")
	if err := p.OnQuery(context.Background(), insertQctx(sess, "USE shop")); err != nil {
		t.Fatal(err)
	}
	q := insertQctx(sess, "INSERT INTO orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatalf("USE-resolved target: %v", err)
	}
	if q.DeferredInsert == nil {
		t.Fatal("expected deferred plan for shop.orders")
	}
}

func TestPlugin_OversizedPayloadAndAbortAndClose(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	declareSchema(t, ns, testSchema())
	signer, _ := auth.NewRelaySigner(testKey)
	seq, _ := OpenSeqCounter(t.TempDir(), signer.Address())
	p, err := New(Options{Signer: signer, Schemas: ns, NetworkID: testNetworkID, Seq: seq, MaxPayloadBytes: 16})
	if err != nil {
		t.Fatal(err)
	}
	sess := newSession(4, "")
	q := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if err := p.OnClientDataStrict(context.Background(), q, encodeRows(t)); err == nil || !strings.Contains(err.Error(), "max_payload_bytes") {
		t.Fatalf("oversized payload must be rejected: %v", err)
	}
	// Rejected capture leaves no state: a new INSERT on the session is accepted.
	q2 := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q2); err != nil {
		t.Fatalf("after oversized rejection: %v", err)
	}
	// A pending INSERT blocks a second one on the same session until abort/close.
	q3 := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q3); err == nil || !strings.Contains(err.Error(), "has not completed") {
		t.Fatalf("second in-flight INSERT: %v", err)
	}
	p.OnQueryAbort(context.Background(), q2)
	if err := p.OnQuery(context.Background(), q3); err != nil {
		t.Fatalf("after abort: %v", err)
	}
	p.OnClose(sess)
	q4 := insertQctx(sess, "INSERT INTO shop.orders FORMAT Native")
	if err := p.OnQuery(context.Background(), q4); err != nil {
		t.Fatalf("after close: %v", err)
	}
	// Input-complete without any payload fails closed.
	if err := p.OnQueryInputCompleteStrict(context.Background(), q4); err == nil || !strings.Contains(err.Error(), "no payload") {
		t.Fatalf("empty payload must be rejected: %v", err)
	}
}
```

- [x] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: build error `undefined: Options`.

- [x] **Step 3: Implement**

Create `pkg/plugins/sistatement/plugin.go`:

```go
package sistatement

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// Options wires the plugin. All fields are required except KeeperShardID
// (must be 0 in v1).
type Options struct {
	Signer          auth.StatementSignerV2
	Schemas         registry.TableSchemas
	NetworkID       string
	KeeperShardID   uint32
	Seq             *SeqCounter
	MaxPayloadBytes uint64
}

// Plugin is the agent-mode storage-integrity statement plugin. See doc.go.
type Plugin struct {
	signer        auth.StatementSignerV2
	account       string // lowercase 0x
	loader        *schemaregistry.NetworkStateLoader
	networkID     string
	keeperShardID uint32
	seq           *SeqCounter
	maxPayload    uint64

	mu      sync.Mutex
	pending map[int64]*pendingStatement // by session id; at most one per session
	useDB   map[int64]string            // last standalone USE per session
}

type pendingStatement struct {
	statementID    string
	tableID        string
	schemaHash     string
	clientRevision uint32
	payload        bytes.Buffer
}

// New validates opts and returns the plugin.
func New(opts Options) (*Plugin, error) {
	var errs []error
	if opts.Signer == nil {
		errs = append(errs, errors.New("signer is required"))
	}
	if opts.Schemas == nil {
		errs = append(errs, errors.New("network-state TableSchemas source is required"))
	}
	if strings.TrimSpace(opts.NetworkID) == "" {
		errs = append(errs, errors.New("network id is required"))
	}
	if opts.KeeperShardID != 0 {
		errs = append(errs, fmt.Errorf("keeper_shard_id must be 0 in v1, got %d", opts.KeeperShardID))
	}
	if opts.Seq == nil {
		errs = append(errs, errors.New("seq counter is required"))
	}
	if opts.MaxPayloadBytes == 0 {
		errs = append(errs, errors.New("max payload bytes must be > 0"))
	}
	if joined := errors.Join(errs...); joined != nil {
		return nil, fmt.Errorf("sistatement: %w", joined)
	}
	return &Plugin{
		signer:        opts.Signer,
		account:       strings.ToLower(opts.Signer.Address()),
		loader:        schemaregistry.NewNetworkStateLoader(opts.Schemas, opts.NetworkID),
		networkID:     opts.NetworkID,
		keeperShardID: opts.KeeperShardID,
		seq:           opts.Seq,
		maxPayload:    opts.MaxPayloadBytes,
		pending:       map[int64]*pendingStatement{},
		useDB:         map[int64]string{},
	}, nil
}

// OnQuery classifies the statement; payload-local Native INSERTs enter the SI
// lane (deferred plan + statement id), everything else passes through.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p == nil || qctx == nil || qctx.Query == nil || qctx.Session == nil {
		return nil
	}
	sql := qctx.Query.Body
	sessID := qctx.Session.ID()
	if db, ok := matchUse(sql); ok {
		p.mu.Lock()
		p.useDB[sessID] = db
		p.mu.Unlock()
		return nil
	}
	if _, err := sicore.InsertPayloadEncoding(sql); err != nil {
		// VALUES / SELECT / non-INSERT: ordinary path.
		return nil
	}
	if qctx.Query.Compression == proto.CompressionEnabled {
		return errors.New("storage_integrity agent rejects compressed INSERT payloads; retry with ClickHouse query compression disabled")
	}
	keys := make([]string, 0, len(qctx.Query.Settings))
	for _, s := range qctx.Query.Settings {
		keys = append(keys, s.Key)
	}
	if err := sicore.RejectUserSettings(keys); err != nil {
		return err
	}
	tableID, err := resolveTargetTableID(sql, p.sessionDatabase(qctx.Session))
	if err != nil {
		return fmt.Errorf("storage_integrity agent: %w", err)
	}
	schema, schemaHash, err := p.loadSchema(ctx, tableID)
	if err != nil {
		return err
	}
	listed, _, err := insertColumnList(sql)
	if err != nil {
		return fmt.Errorf("storage_integrity agent: %w", err)
	}
	cols, err := sampleColumnsFor(schema, listed)
	if err != nil {
		return fmt.Errorf("storage_integrity agent: %w", err)
	}
	revision := qctx.Session.State().ClientRevision
	if revision <= 0 {
		return errors.New("storage_integrity agent: client protocol revision is unknown; cannot sign client_revision")
	}
	statementID, err := p.statementIDFor(qctx.Query.ID)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.pending[sessID]; existing != nil {
		return fmt.Errorf("storage_integrity agent: previous SI INSERT %s on this session has not completed", existing.statementID)
	}
	p.pending[sessID] = &pendingStatement{statementID: statementID, tableID: tableID, schemaHash: schemaHash, clientRevision: uint32(revision)}
	qctx.Query.ID = statementID
	qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: cols, MaxPayloadBytes: p.maxPayload}
	_, logger := log.FromContext(ctx)
	logger.Debugw("sistatement: SI INSERT admitted for deferred signing", "statement_id", statementID, "table_id", tableID, "columns", len(cols))
	return nil
}

func (p *Plugin) sessionDatabase(sess chsession.Session) string {
	p.mu.Lock()
	db := p.useDB[sess.ID()]
	p.mu.Unlock()
	if db != "" {
		return db
	}
	if state := sess.State(); state != nil {
		if logical := state.LogicalDatabaseName(); logical != "" {
			return logical
		}
		return state.PhysicalDatabaseName()
	}
	return ""
}

func (p *Plugin) loadSchema(ctx context.Context, tableID string) (payloadexec.TableSchema, string, error) {
	db, table, ok := strings.Cut(tableID, ".")
	if !ok || db == "" || table == "" {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: table id %q must be db.table", tableID)
	}
	schemas, err := p.loader.Load(ctx, []schemaregistry.TableRef{{TableID: tableID, Database: db, Table: table}})
	if err != nil {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: table %s is not declared in network state (SI INSERT requires a declared, hash-verified schema): %w", tableID, err)
	}
	schema := schemas[0]
	return schema, payloadexec.TableSchemaHash(p.networkID, schema), nil
}

// statementIDFor keeps a client-supplied flat id for this agent's own
// account (SDK path, D6), after durably reserving its seq and canonicalizing
// the account; otherwise it mints <account>:<seq>:<nonce>.
func (p *Plugin) statementIDFor(queryID string) (string, error) {
	if canonical, seq, ok := ownSuppliedStatementID(queryID, p.account); ok {
		if err := p.seq.AdvanceTo(seq); err != nil {
			return "", fmt.Errorf("storage_integrity agent: reserve supplied client_seq: %w", err)
		}
		return canonical, nil
	}
	seq, err := p.seq.Next()
	if err != nil {
		return "", fmt.Errorf("storage_integrity agent: issue client_seq: %w", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("storage_integrity agent: nonce: %w", err)
	}
	return p.account + ":" + strconv.FormatUint(seq, 10) + ":" + hex.EncodeToString(nonce[:]), nil
}

// ownSuppliedStatementID accepts account casing from SDK query ids, but keeps
// the shared parser strict for the sequence and nonce. Only the account segment
// is normalized; the client nonce is preserved byte-for-byte.
func ownSuppliedStatementID(queryID, ownAccount string) (canonical string, seq uint64, ok bool) {
	account, tail, found := strings.Cut(strings.TrimSpace(queryID), ":")
	if !found || !strings.EqualFold(account, ownAccount) {
		return "", 0, false
	}
	account = strings.ToLower(account)
	parsedAccount, parsedSeq, nonce, err := sicore.ParseFlatStatementID(account + ":" + tail)
	if err != nil || parsedAccount != strings.ToLower(ownAccount) {
		return "", 0, false
	}
	return parsedAccount + ":" + strconv.FormatUint(parsedSeq, 10) + ":" + nonce, parsedSeq, true
}

// OnClientDataStrict buffers one raw non-empty Data packet under the budget.
func (p *Plugin) OnClientDataStrict(_ context.Context, qctx *plugin.QueryContext, raw []byte) error {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil || len(raw) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.pending[qctx.Session.ID()]
	if st == nil || st.statementID != qctx.Query.ID {
		return nil
	}
	if next := uint64(st.payload.Len()) + uint64(len(raw)); next > p.maxPayload {
		delete(p.pending, qctx.Session.ID())
		return fmt.Errorf("storage_integrity agent: payload for %s exceeds max_payload_bytes (%d > %d)", st.statementID, next, p.maxPayload)
	}
	_, _ = st.payload.Write(raw)
	return nil
}

// ClientDataReadLimit exposes the remaining budget so Relay rejects an
// oversized packet while reading it.
func (p *Plugin) ClientDataReadLimit(qctx *plugin.QueryContext) (uint64, bool) {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.pending[qctx.Session.ID()]
	if st == nil || st.statementID != qctx.Query.ID {
		return 0, false
	}
	used := uint64(st.payload.Len())
	if used >= p.maxPayload {
		return 0, true
	}
	return p.maxPayload - used, true
}

// OnQueryInputCompleteStrict signs the v2 statement token over the buffered
// payload and appends SQL_x_statement_token; the pending state is released.
func (p *Plugin) OnQueryInputCompleteStrict(ctx context.Context, qctx *plugin.QueryContext) error {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return nil
	}
	p.mu.Lock()
	st := p.pending[qctx.Session.ID()]
	if st == nil || st.statementID != qctx.Query.ID {
		p.mu.Unlock()
		return nil
	}
	delete(p.pending, qctx.Session.ID())
	p.mu.Unlock()
	if st.payload.Len() == 0 {
		return fmt.Errorf("storage_integrity agent: SI INSERT %s carried no payload", st.statementID)
	}
	payload := st.payload.Bytes()
	token, err := p.signer.SignStatementV2(auth.JWSStatementPayloadV2{
		NetworkID:      p.networkID,
		KeeperShardID:  p.keeperShardID,
		StatementID:    st.statementID,
		SQLHash:        replay.DigestString(qctx.Query.Body),
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     st.schemaHash,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: st.clientRevision,
		TargetTableID:  st.tableID,
		RowIDProfileID: payloadexec.RowIDProfileID,
	})
	if err != nil {
		return fmt.Errorf("storage_integrity agent: sign statement %s: %w", st.statementID, err)
	}
	// Same Custom + single-quote wrapping as the auth token (see agent.Plugin).
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + token + "'", Custom: true})
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: statement token signed", "statement_id", st.statementID, "table_id", st.tableID, "payload_bytes", len(payload))
	return nil
}

// OnQueryAbort drops the buffer for the exact query.
func (p *Plugin) OnQueryAbort(_ context.Context, qctx *plugin.QueryContext) {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return
	}
	p.mu.Lock()
	if st := p.pending[qctx.Session.ID()]; st != nil && st.statementID == qctx.Query.ID {
		delete(p.pending, qctx.Session.ID())
	}
	p.mu.Unlock()
}

// OnClose drops all per-session state.
func (p *Plugin) OnClose(sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	delete(p.pending, sess.ID())
	delete(p.useDB, sess.ID())
	p.mu.Unlock()
}

var (
	_ plugin.QueryPlugin                    = (*Plugin)(nil)
	_ plugin.StrictDataPlugin               = (*Plugin)(nil)
	_ plugin.StrictDataLimitPlugin          = (*Plugin)(nil)
	_ plugin.QueryInputCompleteStrictPlugin = (*Plugin)(nil)
	_ plugin.QueryAbortPlugin               = (*Plugin)(nil)
	_ plugin.ClosePlugin                    = (*Plugin)(nil)
)
```

- [x] **Step 4: Run tests**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/sistatement:sistatement_test --test_output=errors`
Expected: PASS (all tests in the package).

- [x] **Step 5: Commit**

```bash
git add pkg/plugins/sistatement
git commit -m "feat(sistatement): agent-side envelope-v2 statement plugin"
```

### Task 16: Wire the plugin into `buildAgent`; `Options.StorageIntegrityTableSchemas`

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `build.go:909-984` (buildAgent), `build.go:1002-1013` (network state resolution), `proxy.go:74-110` (Options)
- Test: `build_test.go` (append `TestBuildAgent_StorageIntegrityAgentWiresPluginChain`, `TestBuildAgent_StorageIntegrityAgentRequiresTableSchemas`)

**Interfaces:**
- Consumes: Task 12 config, Task 15 plugin, `sessionstate.Plugin`.
- Produces: `Options.StorageIntegrityTableSchemas registry.TableSchemas` (optional override, both modes); `agentNetworkState(opts, rf) (registry.Registry, error)`; agent chain order `HelloPlugins: [sessionstate]`, `QueryPlugins: [materialize?, sistatement?, agent.Signer, metrics]`, `StrictDataPlugins/QueryInputCompleteStrictPlugins/QueryAbortPlugins/ClosePlugins: [sistatement]`.

- [x] **Step 1: Write the failing tests**

Append to `build_test.go`:

```go
func agentSICfg(t *testing.T) *config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Listen = "127.0.0.1:0"
	cfg.MetricsListen = ""
	cfg.Agent.Mode = true
	cfg.Agent.PrivateKeyHex = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	cfg.Agent.Upstream = "127.0.0.1:9000"
	cfg.StorageIntegrity.Agent.Enabled = true
	cfg.StorageIntegrity.Agent.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
	cfg.StorageIntegrity.Agent.RequireNetworkState = false
	return &cfg
}

func TestBuildAgent_StorageIntegrityAgentWiresPluginChain(t *testing.T) {
	cfg := agentSICfg(t)
	bs, err := buildAgent(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildAgent: %v", err)
	}
	defer bs.teardown()
	srv := requireProxyServer(t, bs.listeners[0])
	chain, ok := srv.Hooks.(*plugin.PluginChain)
	if !ok {
		t.Fatalf("Hooks type %T", srv.Hooks)
	}
	// sistatement must run BEFORE the agent auth signer so both tokens see the
	// same SQL body and the statement id is final when SQL_x_auth_token is minted.
	siIdx, signerIdx := -1, -1
	for i, p := range chain.QueryPlugins {
		switch p.(type) {
		case *sistatement.Plugin:
			siIdx = i
		case *agent.Plugin:
			signerIdx = i
		}
	}
	if siIdx < 0 || signerIdx < 0 || siIdx > signerIdx {
		t.Fatalf("query plugin order sistatement=%d signer=%d", siIdx, signerIdx)
	}
	if len(chain.StrictDataPlugins) != 1 || len(chain.QueryInputCompleteStrictPlugins) != 1 || len(chain.QueryAbortPlugins) != 1 || len(chain.ClosePlugins) != 1 {
		t.Fatalf("sistatement must be registered on strict-data/input-complete-strict/abort/close chains: %d/%d/%d/%d",
			len(chain.StrictDataPlugins), len(chain.QueryInputCompleteStrictPlugins), len(chain.QueryAbortPlugins), len(chain.ClosePlugins))
	}
	if len(chain.HelloPlugins) != 1 {
		t.Fatalf("agent SI needs the sessionstate hello plugin for LogicalDatabase, got %d hello plugins", len(chain.HelloPlugins))
	}
}

func TestBuildAgent_StorageIntegrityAgentRequiresTableSchemas(t *testing.T) {
	cfg := agentSICfg(t)
	// registryWithoutSchemas satisfies registry.Registry but not TableSchemas.
	_, err := buildAgent(Options{Config: cfg, NetworkState: registryWithoutSchemas{network.NewInMemoryNetworkState()}}, nil)
	if err == nil || !strings.Contains(err.Error(), "TableSchemas") {
		t.Fatalf("expected TableSchemas requirement error, got %v", err)
	}
	// An explicit override satisfies it.
	bs, err := buildAgent(Options{Config: cfg, NetworkState: registryWithoutSchemas{network.NewInMemoryNetworkState()}, StorageIntegrityTableSchemas: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("override: %v", err)
	}
	bs.teardown()
}

// registryWithoutSchemas hides the TableSchemas methods of the embedded state.
type registryWithoutSchemas struct{ *network.InMemoryNetworkState }

func (r registryWithoutSchemas) TableSchema(string, string, uint32) {}
func (r registryWithoutSchemas) LatestTableSchema(string, string) {}
```

(Add imports `"github.com/housegate/housegate/pkg/plugins/agent"` and `"github.com/housegate/housegate/pkg/plugins/sistatement"` to build_test.go. The two shadowing methods have a different signature from `registry.TableSchemas`, so the type assertion in `buildAgent` fails as intended.)

- [x] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //:housegate_test --test_filter='TestBuildAgent_StorageIntegrityAgent' --test_output=errors`
Expected: build error `unknown field StorageIntegrityTableSchemas`.

- [x] **Step 3: Implement**

`proxy.go` — add to `Options` after `StorageIntegrityRuntime`:

```go
	// StorageIntegrityTableSchemas optionally supplies the declared table
	// schema source used by the agent-mode statement plugin
	// (storage_integrity.agent) and the server-mode ingress
	// (storage_integrity.ingress) to resolve schema_hash. When nil, housegate
	// type-asserts the effective registry (Options.NetworkState or the
	// config-loaded one) to registry.TableSchemas; the yaml
	// InMemoryNetworkState and sentio-node's adapter both implement it, the
	// agent RPC backend does not.
	StorageIntegrityTableSchemas registry.TableSchemas
```

`build.go` — add the helper (near `buildAgentDialer`):

```go
// agentNetworkState resolves the agent's registry: opts.NetworkState wins,
// else Config.NetworkState.Source is loaded.
func agentNetworkState(opts Options, rf *redisFactory) (registry.Registry, error) {
	if opts.NetworkState != nil {
		return opts.NetworkState, nil
	}
	return loadNetworkState(opts.Config, rf)
}

// resolveTableSchemas returns the declared-schema source for storage
// integrity: the explicit option, else the registry if it implements
// registry.TableSchemas.
func resolveTableSchemas(opts Options, reg registry.Registry, feature string) (registry.TableSchemas, error) {
	if opts.StorageIntegrityTableSchemas != nil {
		return opts.StorageIntegrityTableSchemas, nil
	}
	if ts, ok := reg.(registry.TableSchemas); ok && ts != nil {
		return ts, nil
	}
	return nil, fmt.Errorf("%s requires a NetworkState that implements registry.TableSchemas (yaml source or host-injected state); set Options.StorageIntegrityTableSchemas explicitly otherwise", feature)
}
```

In `buildAgentDialer`, replace the block that resolves `reg` (`var reg registry.Registry ... loadNetworkState`) with:

```go
	reg, err := agentNetworkState(opts, rf)
	if err != nil {
		return nil, err
	}
```

In `buildAgent`, replace the plugin assembly between `queryPlugins := []plugin.QueryPlugin{}` and `chain := &plugin.PluginChain{...}` so it reads:

```go
	queryPlugins := []plugin.QueryPlugin{}
	var helloPlugins []plugin.HelloPlugin
	var strictDataPlugins []plugin.StrictDataPlugin
	var inputCompleteStrictPlugins []plugin.QueryInputCompleteStrictPlugin
	var abortPlugins []plugin.QueryAbortPlugin
	var closePlugins []plugin.ClosePlugin
	var materializerClose func()
	if cfg.Materialize.Enabled {
		m, err := buildMaterializer(cfg)
		if err != nil {
			return nil, fmt.Errorf("materialize: %w", err) // startup fail-fast
		}
		materializerClose = func() { _ = m.Close() }
		queryPlugins = append(queryPlugins, &materialize.Plugin{Materializer: m, Observer: obs})
		log.Infow("agent materialize enabled", "engine", cfg.Materialize.Engine)
	}
	// storage-integrity statement plugin: after materialize (signs the
	// materialized SQL), before the auth signer (statement id is final and
	// both tokens bind the same body). Startup fail-fast like materialize.
	if cfg.StorageIntegrity.Agent.Enabled {
		stmtSigner, ok := signer.(auth.StatementSignerV2)
		if !ok {
			return nil, fmt.Errorf("storage_integrity.agent: signer %T does not implement auth.StatementSignerV2", signer)
		}
		reg, err := agentNetworkState(opts, rf)
		if err != nil {
			return nil, fmt.Errorf("storage_integrity.agent: %w", err)
		}
		schemas, err := resolveTableSchemas(opts, reg, "storage_integrity.agent")
		if err != nil {
			return nil, err
		}
		seq, err := sistatement.OpenSeqCounter(cfg.StorageIntegrity.Agent.StateDir, signer.Address())
		if err != nil {
			return nil, fmt.Errorf("storage_integrity.agent: %w", err)
		}
		siPlug, err := sistatement.New(sistatement.Options{
			Signer:          stmtSigner,
			Schemas:         schemas,
			NetworkID:       cfg.StorageIntegrity.Agent.NetworkID,
			KeeperShardID:   cfg.StorageIntegrity.Agent.KeeperShardID,
			Seq:             seq,
			MaxPayloadBytes: cfg.StorageIntegrity.Agent.MaxPayloadBytes,
		})
		if err != nil {
			return nil, fmt.Errorf("storage_integrity.agent: %w", err)
		}
		helloPlugins = append(helloPlugins, &sessionstate.Plugin{})
		queryPlugins = append(queryPlugins, siPlug)
		strictDataPlugins = append(strictDataPlugins, siPlug)
		inputCompleteStrictPlugins = append(inputCompleteStrictPlugins, siPlug)
		abortPlugins = append(abortPlugins, siPlug)
		closePlugins = append(closePlugins, siPlug)
		log.Infow("storage_integrity agent statement plugin enabled",
			"network_id", cfg.StorageIntegrity.Agent.NetworkID,
			"state_dir", cfg.StorageIntegrity.Agent.StateDir,
			"seq_last", seq.Last(),
			"max_payload_bytes", cfg.StorageIntegrity.Agent.MaxPayloadBytes)
	}
	queryPlugins = append(queryPlugins,
		&agent.Plugin{Signer: signer, Observer: obs, Owner: cfg.Agent.Owner, IsDriver: cfg.Agent.Driver},
		metrics,
	)

	chain := &plugin.PluginChain{
		ConnLifecyclePlugins:            []plugin.ConnLifecyclePlugin{metrics},
		HelloPlugins:                    helloPlugins,
		HandshakeCompletePlugins:        []plugin.HandshakeCompletePlugin{metrics},
		QueryPlugins:                    queryPlugins,
		StrictDataPlugins:               strictDataPlugins,
		QueryInputCompleteStrictPlugins: inputCompleteStrictPlugins,
		QueryAbortPlugins:               abortPlugins,
		ExceptionPlugins:                []plugin.ExceptionPlugin{metrics},
		ClosePlugins:                    closePlugins,
	}
```

Add import `"github.com/housegate/housegate/pkg/plugins/sistatement"` to build.go.

- [x] **Step 4: Run tests**

Run: `bazel run //:gazelle && bazel test //:housegate_test --test_output=errors`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add build.go proxy.go build_test.go BUILD.bazel
git commit -m "feat(agent): wire the storage-integrity statement plugin into buildAgent"
```

---

## Phase 5 — Ingress v2 and CSV-bridge removal (spec §10 step 5, §6)

### Task 17: `sicore` envelope v2 fields, `EnvelopeFromAdmission` re-verification, proto fill

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/storageintegrity/intake.go:33-47` (AdmissionRecord), `:80-96` (StatementEnvelope), `:102-182` (EnvelopeFromAdmission), `pkg/storageintegrity/arbiter_proto.go:299-338`
- Test: `pkg/storageintegrity/intake_test.go` (append), `pkg/storageintegrity/arbiter_proto_test.go` (append)

**Interfaces:**
- Consumes: Task 8 constants, Task 6 `payloadexec.RowIDProfileID`, Task 3 proto fields.
- Produces: `AdmissionRecord`/`StatementEnvelope` + `EnvelopeVersion uint32; NetworkID string; KeeperShardID uint32; SettingsHash string; SchemaHash string; RowIDProfileID string`; `EnvelopeFromAdmission` enforces the v2 invariants; `ArbiterStatementEnvelopeToProto` fills fields 5, 11–17.

- [x] **Step 1: Write the failing tests**

Append to `pkg/storageintegrity/intake_test.go` (the file already has a helper that builds a valid Native admission — if its name differs, use it; the plan calls it `validNativeAdmission`; if absent add it as below):

```go
func validNativeAdmissionV2(t *testing.T) AdmissionRecord {
	t.Helper()
	sql := "INSERT INTO tenant.events FORMAT Native"
	payload := []byte{byte(2), 0, 0xab, 0xcd}
	return AdmissionRecord{
		StatementID:     "0xabc0000000000000000000000000000000000001:1:n1",
		Kind:            KindInsert,
		TableID:         "tenant.events",
		SQL:             sql,
		SQLHash:         replay.DigestString(sql),
		Signer:          "0xabc0000000000000000000000000000000000001",
		UserJWS:         "h.p.s",
		Payload:         payload,
		PayloadLength:   uint64(len(payload)),
		PayloadHash:     replay.DigestBytes(payload),
		PayloadEncoding: PayloadEncodingClickHouseNativeData,
		Revision:        54460,
		EnvelopeVersion: EnvelopeVersionV2,
		NetworkID:       "testnet-v2",
		KeeperShardID:   0,
		SettingsHash:    EmptySettingsHash,
		SchemaHash:      "0x" + strings.Repeat("33", 32),
		RowIDProfileID:  payloadexec.RowIDProfileID,
	}
}

func TestEnvelopeFromAdmission_CarriesV2Fields(t *testing.T) {
	adm := validNativeAdmissionV2(t)
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatalf("EnvelopeFromAdmission: %v", err)
	}
	if env.EnvelopeVersion != 2 || env.NetworkID != "testnet-v2" || env.KeeperShardID != 0 || env.SettingsHash != EmptySettingsHash || env.SchemaHash != adm.SchemaHash || env.RowIDProfileID != payloadexec.RowIDProfileID || env.PayloadEncoding != PayloadEncodingClickHouseNativeData || env.Revision != 54460 {
		t.Fatalf("envelope v2 fields not carried: %+v", env)
	}
}

func TestEnvelopeFromAdmission_RejectsV2Violations(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*AdmissionRecord)
		want   string
	}{
		{"wrong envelope version", func(a *AdmissionRecord) { a.EnvelopeVersion = 1 }, "envelope_version"},
		{"missing network id", func(a *AdmissionRecord) { a.NetworkID = "" }, "network_id"},
		{"non-zero shard", func(a *AdmissionRecord) { a.KeeperShardID = 1 }, "keeper_shard_id"},
		{"non-empty settings hash", func(a *AdmissionRecord) { a.SettingsHash = replay.DigestString("x") }, "settings_hash"},
		{"missing schema hash", func(a *AdmissionRecord) { a.SchemaHash = "" }, "schema_hash"},
		{"wrong row id profile", func(a *AdmissionRecord) { a.RowIDProfileID = "housegate-row-id-v0" }, "row_id_profile_id"},
		{"csv encoding no longer admitted", func(a *AdmissionRecord) { a.PayloadEncoding = EncodingCSVWithNames }, "payload encoding"},
		{"missing revision", func(a *AdmissionRecord) { a.Revision = 0 }, "revision"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			adm := validNativeAdmissionV2(t)
			tc.mutate(&adm)
			_, err := EnvelopeFromAdmission(adm)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}
```

Append to `pkg/storageintegrity/arbiter_proto_test.go`:

```go
func TestArbiterStatementEnvelopeToProto_FillsEveryV2Field(t *testing.T) {
	adm := validNativeAdmissionV2(t)
	env, err := EnvelopeFromAdmission(adm)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := ArbiterStatementEnvelopeToProto(env)
	if err != nil {
		t.Fatalf("ArbiterStatementEnvelopeToProto: %v", err)
	}
	if msg.GetEnvelopeVersion() != 2 || msg.GetNetworkId() != "testnet-v2" || msg.GetKeeperShardId() != 0 ||
		msg.GetPayloadFormat() != PayloadEncodingClickHouseNativeData || msg.GetClientRevision() != 54460 ||
		msg.GetSchemaHash() != adm.SchemaHash || msg.GetRowIdProfileId() != payloadexec.RowIDProfileID ||
		msg.GetSettingsHash() != EmptySettingsHash {
		t.Fatalf("proto v2 fields: %+v", msg)
	}
	if msg.GetSettingsHash() == "" {
		t.Fatal("settings_hash must no longer be empty")
	}
}
```

- [x] **Step 2: Run to verify failure**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestEnvelopeFromAdmission_CarriesV2Fields|TestEnvelopeFromAdmission_RejectsV2Violations|TestArbiterStatementEnvelopeToProto_FillsEveryV2Field' --test_output=errors`
Expected: build error `unknown field EnvelopeVersion`.

- [x] **Step 3: Implement**

Add to `AdmissionRecord` (after `Revision int`):

```go
	// ---- envelope v2 (all signed by UserJWS) ----
	EnvelopeVersion uint32
	NetworkID       string
	KeeperShardID   uint32
	SettingsHash    string
	SchemaHash      string
	RowIDProfileID  string
```

Add the same six fields to `StatementEnvelope` (after `UserJWS string`), and in `EnvelopeFromAdmission` replace the block from `if adm.PayloadEncoding == "" {` through the `Revision == 0` check with:

```go
	if adm.PayloadEncoding == "" {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s has no payload encoding", adm.StatementID)
	}
	if adm.PayloadEncoding != PayloadEncodingClickHouseNativeData {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s payload encoding %q is not the SI lane's %q", adm.StatementID, adm.PayloadEncoding, PayloadEncodingClickHouseNativeData)
	}
	if adm.PayloadEncoding != sqlEncoding {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s payload encoding %q does not match SQL encoding %q", adm.StatementID, adm.PayloadEncoding, sqlEncoding)
	}
	if adm.EnvelopeVersion != EnvelopeVersionV2 {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s envelope_version %d, want %d", adm.StatementID, adm.EnvelopeVersion, EnvelopeVersionV2)
	}
	if strings.TrimSpace(adm.NetworkID) == "" {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s has no network_id", adm.StatementID)
	}
	if adm.KeeperShardID != 0 {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s keeper_shard_id %d, want 0 in v1", adm.StatementID, adm.KeeperShardID)
	}
	if adm.SettingsHash != EmptySettingsHash {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s settings_hash %q, want the empty-settings digest", adm.StatementID, adm.SettingsHash)
	}
	if adm.SchemaHash == "" {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s has no schema_hash", adm.StatementID)
	}
	if adm.RowIDProfileID != payloadexec.RowIDProfileID {
		return StatementEnvelope{}, fmt.Errorf("intake: admission %s row_id_profile_id %q, want %q", adm.StatementID, adm.RowIDProfileID, payloadexec.RowIDProfileID)
	}
```

keep the existing statement-id / signer / sql-hash / user-jws / payload checks, and replace the old `Revision == 0` check inside the `if adm.Kind == KindInsert {` block with an unconditional one:

```go
		if adm.Revision == 0 {
			return StatementEnvelope{}, fmt.Errorf("intake: INSERT admission %s has no client protocol revision", adm.StatementID)
		}
```

Extend the returned `StatementEnvelope{...}` literal with `EnvelopeVersion: adm.EnvelopeVersion, NetworkID: adm.NetworkID, KeeperShardID: adm.KeeperShardID, SettingsHash: adm.SettingsHash, SchemaHash: adm.SchemaHash, RowIDProfileID: adm.RowIDProfileID`. Add the import `"github.com/housegate/housegate/pkg/replay/payloadexec"` to intake.go.

In `arbiter_proto.go` `ArbiterStatementEnvelopeToProto`, add before the return:

```go
	if env.EnvelopeVersion != EnvelopeVersionV2 || env.NetworkID == "" || env.SettingsHash == "" || env.SchemaHash == "" || env.RowIDProfileID == "" || env.PayloadEncoding == "" || env.Revision == 0 {
		return nil, fmt.Errorf("storageintegrity: envelope %s is missing v2 fields (version=%d network=%q settings=%q schema=%q profile=%q format=%q revision=%d)", env.StatementID, env.EnvelopeVersion, env.NetworkID, env.SettingsHash, env.SchemaHash, env.RowIDProfileID, env.PayloadEncoding, env.Revision)
	}
```

and fill the message:

```go
	return &pb.StatementEnvelopeV2{
		StatementId:     &pb.StatementID{ClientAccount: id.ClientAccount, ClientSeq: id.ClientSeq, ClientNonce: id.ClientNonce},
		StatementKind:   pb.StatementKind_STATEMENT_KIND_INSERT,
		Sql:             env.SQL,
		SqlHash:         env.SQLHash,
		SettingsHash:    env.SettingsHash,
		PayloadRef:      env.PayloadRef,
		PayloadHash:     env.PayloadHash,
		PayloadLength:   env.PayloadLength,
		TargetTableId:   env.TargetTableID,
		UserJws:         env.UserJWS,
		EnvelopeVersion: env.EnvelopeVersion,
		NetworkId:       env.NetworkID,
		KeeperShardId:   env.KeeperShardID,
		PayloadFormat:   env.PayloadEncoding,
		ClientRevision:  uint32(env.Revision),
		SchemaHash:      env.SchemaHash,
		RowIdProfileId:  env.RowIDProfileID,
	}, nil
```

Update every existing test fixture in `pkg/storageintegrity/*_test.go` and `storage_integrity_ingress_test.go` / `build_test.go` that builds an `AdmissionRecord` for the Native path so it sets the six v2 fields (grep `PayloadEncoding: PayloadEncodingClickHouseNativeData` / `storageIntegrityAdmissionForEncoding` and add `EnvelopeVersion: EnvelopeVersionV2, NetworkID: "testnet-v2", SettingsHash: EmptySettingsHash, SchemaHash: "0x…", RowIDProfileID: payloadexec.RowIDProfileID`).

- [x] **Step 3b: Terminal prepare-reject class for the source's `schema_hash` check (spec §8)**

Add to `pkg/storageintegrity/intake.go` (near the other sentinels / after `PreparedStatementLookup`):

```go
// ErrPrepareTerminalReject marks a PrepareLocalStatement failure the source
// classified as terminal BEFORE any unsafe write (envelope v2: schema_hash
// mismatch against the source's declared schema, unsupported payload format).
// Host adapters wrap the source's sentinel with %w AND this error; the
// orchestrator then aborts the intake as OutcomeTerminalReject instead of
// fencing a retry behind a source lookup (nothing was written to fence).
var ErrPrepareTerminalReject = errors.New("intake: source rejected prepare terminally")
```

In `Orchestrator.run`, replace the `if prepareErr != nil {` block with:

```go
			if prepareErr != nil {
				if errors.Is(prepareErr, ErrPrepareTerminalReject) {
					// Nothing durable happened on the source: abort with the exact
					// (empty) candidate set and finish as a terminal reject.
					res := o.resultFor(rec)
					res.Submit = s
					return o.abort(ctx, res, rec, prepareErr.Error())
				}
				// The source may have committed its durable unsafe write before a
				// transport error or cancellation hid the response. Fence every
				// later prepare behind an explicit source lookup.
				o.mu.Lock()
				rec.requirePreparedLookup = true
				o.mu.Unlock()
				return IntakeResult{StatementID: env.StatementID, Submit: s}, fmt.Errorf("intake: prepare failed for %s: %w", env.StatementID, prepareErr)
			}
```

(add `"errors"` to intake.go imports if missing.) Append the test to `pkg/storageintegrity/intake_test.go`:

```go
func TestOrchestrate_TerminalPrepareRejectAbortsWithoutLookupFence(t *testing.T) {
	prep := &recordingPreparer{prepareErr: fmt.Errorf("schema_hash mismatch: %w", ErrPrepareTerminalReject)}
	sub := &recordingSubmitter{outcome: SubmitOutcome{Category: OutcomeAccepted}}
	orch := NewOrchestrator(sub, prep, OrchestratorConfig{ExpectedSource: "snode-A"})
	res, err := orch.Orchestrate(context.Background(), admissionFixture())
	if err != nil {
		t.Fatalf("terminal prepare reject must resolve cleanly, got %v", err)
	}
	if res.Ack2 || res.Lifecycle != LifecycleCleaned {
		t.Fatalf("result = %+v, want cleaned non-ACK2", res)
	}
	if atomic.LoadInt64(&prep.abortAt) == 0 {
		t.Fatal("terminal prepare reject must abort the (empty) candidate set")
	}
	// A retry does not need a source lookup: prepare runs again immediately.
	prep.prepareErr = nil
	before := atomic.LoadInt64(&prep.prepareCount)
	_, _ = orch.Orchestrate(context.Background(), admissionFixture())
	if atomic.LoadInt64(&prep.prepareCount) != before+1 {
		t.Fatal("retry after a terminal prepare reject must re-prepare without a lookup fence")
	}
}
```

(`admissionFixture()` is the existing valid-admission helper in intake_test.go; it must set the six v2 fields after this task's Step 3.)

- [x] **Step 4: Run tests**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test //:housegate_test --test_output=errors`
Expected: PASS (root tests still reference the CSV materializer — that is Task 19; if `//:housegate_test` fails only on `MaterializerCSV`-named tests, proceed and fix them in Task 19).

- [x] **Step 5: Commit**

```bash
git add pkg/storageintegrity storage_integrity_ingress_test.go build_test.go
git commit -m "feat(storageintegrity): envelope v2 fields on AdmissionRecord/StatementEnvelope and the arbiter proto fill"
```

### Task 18: Ingress plugin v2 — statement token validation against the ingress's own capture

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/plugins/storageintegrity/plugin.go` (Config, Admission, admissionState, OnQuery, admissionFromState, new helpers)
- Test: `pkg/plugins/storageintegrity/plugin_test.go` (append)

**Interfaces:**
- Consumes: Task 4 (`auth.StatementValidatorV2`, `auth.JWSStatementPayloadV2`, `auth.StatementTokenSettingKey`, `auth.DecodeStatementV2Payload`), Task 8, Task 6, `registry.TableSchemas`, `schemaregistry.NewNetworkStateLoader`.
- Produces: `Config.TableSchemas registry.TableSchemas`, `Config.NetworkID string`; `Admission.{AuthToken string; EnvelopeVersion uint32; NetworkID string; KeeperShardID uint32; SettingsHash string; SchemaHash string; RowIDProfileID string}` with `Admission.UserJWS` now holding the **v2 statement token**; `Config.PayloadMaterializer` is removed in Task 19 (leave it in place here).

- [x] **Step 1: Write the failing tests**

Append to `pkg/plugins/storageintegrity/plugin_test.go`:

```go
func ingressSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: "tenant.events", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}}
}

func ingressNetworkState(t *testing.T) (*network.InMemoryNetworkState, string) {
	t.Helper()
	ns := network.NewInMemoryNetworkState()
	schema := ingressSchema()
	js, _ := json.Marshal(schema)
	hash := payloadexec.TableSchemaHash("testnet-v2", schema)
	ns.TableSchemas["tenant/events@1"] = network.TableSchemaInfo{DatabaseId: "tenant", TableId: "events", Version: 1, SchemaHash: hash, SchemaJson: string(js)}
	return ns, hash
}

func newV2Ingress(t *testing.T) (*Plugin, *auth.RelaySigner, string) {
	t.Helper()
	ns, hash := ingressNetworkState(t)
	p, signer := newSignedIngressWithConfig(t, Config{TableSchemas: ns, NetworkID: "testnet-v2"})
	return p, signer, hash
}

// v2Statement builds the expected token payload the way the agent does and
// signs it; the ingress must recompute the same expectation from its capture.
func v2Statement(signer *auth.RelaySigner, statementID, sql, schemaHash string, payload []byte, revision uint32) auth.JWSStatementPayloadV2 {
	return auth.JWSStatementPayloadV2{
		NetworkID: "testnet-v2", KeeperShardID: 0, StatementID: statementID,
		SQLHash: replay.DigestString(sql), SettingsHash: sicore.EmptySettingsHash, SchemaHash: schemaHash,
		PayloadHash: replay.DigestBytes(payload), PayloadLength: uint64(len(payload)),
		PayloadFormat: sicore.PayloadEncodingClickHouseNativeData, ClientRevision: revision,
		TargetTableID: "tenant.events", RowIDProfileID: payloadexec.RowIDProfileID,
	}
}

func withStatementToken(t *testing.T, qctx *plugin.QueryContext, signer *auth.RelaySigner, p auth.JWSStatementPayloadV2) {
	t.Helper()
	token, err := signer.SignStatementV2(p)
	if err != nil {
		t.Fatal(err)
	}
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + token + "'", Custom: true})
}

func TestIngressV2_AcceptsTokenBoundToItsOwnCapture(t *testing.T) {
	p, signer, schemaHash := newV2Ingress(t)
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 21, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, sql, schemaHash, payload, 54453))

	if err := p.OnQuery(context.Background(), qctx); err != nil {
		t.Fatalf("OnQuery: %v", err)
	}
	if err := p.OnClientDataStrict(context.Background(), qctx, payload); err != nil {
		t.Fatalf("OnClientDataStrict: %v", err)
	}
	p.OnQueryInputComplete(context.Background(), qctx)
	adm, err := p.ConsumeAdmission(qctx.Session.ID())
	if err != nil {
		t.Fatalf("ConsumeAdmission: %v", err)
	}
	if adm.EnvelopeVersion != 2 || adm.NetworkID != "testnet-v2" || adm.SchemaHash != schemaHash || adm.SettingsHash != sicore.EmptySettingsHash || adm.RowIDProfileID != payloadexec.RowIDProfileID || adm.Payload.Encoding != sicore.PayloadEncodingClickHouseNativeData || adm.Payload.Revision != 54453 {
		t.Fatalf("admission v2 fields: %+v", adm)
	}
	if adm.UserJWS == "" || strings.Count(adm.UserJWS, ".") != 2 || adm.AuthToken == "" {
		t.Fatalf("UserJWS must be the statement token and AuthToken the query token: %+v", adm)
	}
	if adm.UserJWS == adm.AuthToken {
		t.Fatal("statement token and auth token must be different tokens")
	}
}

func TestIngressV2_RejectionMatrix(t *testing.T) {
	sql := "INSERT INTO tenant.events FORMAT Native"
	payload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
	cases := []struct {
		name    string
		prepare func(t *testing.T, p *Plugin, signer *auth.RelaySigner, schemaHash string, qctx *plugin.QueryContext) []byte // returns payload to capture
		atQuery bool
		want    string
	}{
		{"missing statement token", func(t *testing.T, _ *Plugin, _ *auth.RelaySigner, _ string, _ *plugin.QueryContext) []byte { return payload }, true, auth.StatementTokenSettingKey},
		{"user setting present", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, h, payload, 54453))
			q.Query.Settings = append(q.Query.Settings, chproto.Setting{Key: "async_insert", Value: "1"})
			return payload
		}, true, "async_insert"},
		{"payload swapped after signing", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, h, payload, 54453))
			return []byte{byte(chproto.ClientDataCode), 0, 0xff, 0xff}
		}, false, "payload_hash"},
		{"schema hash differs from network state", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, _ string, q *plugin.QueryContext) []byte {
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, "0x"+strings.Repeat("ee", 32), payload, 54453))
			return payload
		}, false, "schema_hash"},
		{"client revision differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			withStatementToken(t, q, signer, v2Statement(signer, q.Query.ID, sql, h, payload, 54470))
			return payload
		}, false, "client_revision"},
		{"network id differs", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			st := v2Statement(signer, q.Query.ID, sql, h, payload, 54453)
			st.NetworkID = "other"
			withStatementToken(t, q, signer, st)
			return payload
		}, false, "network_id"},
		{"statement token signed by other key", func(t *testing.T, _ *Plugin, _ *auth.RelaySigner, h string, q *plugin.QueryContext) []byte {
			other, _ := auth.NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
			withStatementToken(t, q, other, v2Statement(other, q.Query.ID, sql, h, payload, 54453))
			return payload
		}, false, "allowlist"},
		{"legacy query token in statement slot", func(t *testing.T, _ *Plugin, signer *auth.RelaySigner, _ string, q *plugin.QueryContext) []byte {
			legacy, _ := signer.SignToken(sql)
			q.Query.Settings = append(q.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + legacy + "'", Custom: true})
			return payload
		}, false, "purpose"},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, signer, schemaHash := newV2Ingress(t)
			qctx := signedQueryContext(t, int64(30+i), signer, sql, sql, sqlmeta.StatementTypeInsert)
			qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
			captured := tc.prepare(t, p, signer, schemaHash, qctx)
			err := p.OnQuery(context.Background(), qctx)
			if tc.atQuery {
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("OnQuery err = %v, want containing %q", err, tc.want)
				}
				return
			}
			if err != nil {
				t.Fatalf("OnQuery: %v", err)
			}
			if err := p.OnClientDataStrict(context.Background(), qctx, captured); err != nil {
				t.Fatalf("OnClientDataStrict: %v", err)
			}
			p.OnQueryInputComplete(context.Background(), qctx)
			_, err = p.ConsumeAdmission(qctx.Session.ID())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("ConsumeAdmission err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestIngressV2_UndeclaredTableFailsClosedAtQuery(t *testing.T) {
	p, signer := newSignedIngressWithConfig(t, Config{TableSchemas: network.NewInMemoryNetworkState(), NetworkID: "testnet-v2"})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 40, signer, sql, sql, sqlmeta.StatementTypeInsert)
	qctx.AccessedTables = []sqlmeta.AccessedTable{{OriginalDatabase: "tenant", OriginalTable: "events"}}
	withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, sql, "0x00", []byte{2, 0}, 54453))
	if err := p.OnQuery(context.Background(), qctx); err == nil || !strings.Contains(err.Error(), "schema") {
		t.Fatalf("undeclared table must fail closed at OnQuery: %v", err)
	}
}

func TestIngressV2_RequiresTableSchemasAndNetworkID(t *testing.T) {
	p, signer := newSignedIngressWithConfig(t, Config{})
	sql := "INSERT INTO tenant.events FORMAT Native"
	qctx := signedQueryContext(t, 41, signer, sql, sql, sqlmeta.StatementTypeInsert)
	if err := p.OnQuery(context.Background(), qctx); err == nil || !strings.Contains(err.Error(), "TableSchemas") {
		t.Fatalf("ingress without a schema source must reject SI INSERTs: %v", err)
	}
}
```

Add imports to the test file: `"encoding/json"`, `"github.com/housegate/housegate/pkg/lthash"`, `"github.com/housegate/housegate/pkg/network"`, `"github.com/housegate/housegate/pkg/replay/payloadexec"`.

- [x] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/plugins/storageintegrity:storageintegrity_test --test_filter='TestIngressV2' --test_output=errors`
Expected: build error `unknown field TableSchemas in struct literal`.

- [x] **Step 3: Implement**

`Config` gains:

```go
	// TableSchemas resolves the declared network-state schema for the target
	// table so the ingress can compute its own schema_hash expectation
	// (fail closed when unresolvable). NetworkID is the genesis network id
	// every statement token must carry.
	TableSchemas registry.TableSchemas
	NetworkID    string
```

`Plugin` gains `schemaLoader *schemaregistry.NetworkStateLoader` (nil when `cfg.TableSchemas == nil`) and `networkID string`; `New` sets `schemaLoader: schemaregistry.NewNetworkStateLoader(cfg.TableSchemas, cfg.NetworkID)` when non-nil.

`Admission` gains:

```go
	// AuthToken is the SQL_x_auth_token query JWS (audit only). UserJWS is
	// the SQL_x_statement_token envelope-v2 token that is sequenced.
	AuthToken       string
	EnvelopeVersion uint32
	NetworkID       string
	KeeperShardID   uint32
	SettingsHash    string
	SchemaHash      string
	RowIDProfileID  string
```

`admissionState` gains `statementToken string` and `schemaHash string`.

`OnQuery` — after `signer, err := p.authenticate(...)` / `requireStatementIDSigner`, insert:

```go
	if p.schemaLoader == nil || strings.TrimSpace(p.networkID) == "" {
		return errors.New("storage_integrity ingress requires a network-state TableSchemas source and network_id to verify envelope v2 statements")
	}
	statementToken, err := statementTokenFromSettings(querySettings(qctx))
	if err != nil {
		return err
	}
	if err := sicore.RejectUserSettings(settingKeys(qctx)); err != nil {
		return err
	}
	schemaHash, err := p.resolveSchemaHash(ctx, tableID)
	if err != nil {
		return err
	}
```

and set on the state: `admission.UserJWS = statementToken`, `admission.AuthToken = userJWS` (the auth token), `state.statementToken = statementToken`, `state.schemaHash = schemaHash`. Add helpers:

```go
func statementTokenFromSettings(settings map[string]string) (string, error) {
	token := strings.Trim(strings.TrimSpace(settings[auth.StatementTokenSettingKey]), "\"'")
	if token == "" {
		return "", fmt.Errorf("storage_integrity requires %s (envelope v2 statement token)", auth.StatementTokenSettingKey)
	}
	return token, nil
}

func settingKeys(qctx *plugin.QueryContext) []string {
	if qctx == nil || qctx.Query == nil {
		return nil
	}
	keys := make([]string, 0, len(qctx.Query.Settings))
	for _, s := range qctx.Query.Settings {
		keys = append(keys, s.Key)
	}
	return keys
}

// resolveSchemaHash computes this ingress's own expectation of schema_hash
// from the declared, hash-verified network-state schema. Fail closed.
func (p *Plugin) resolveSchemaHash(ctx context.Context, tableID string) (string, error) {
	db, table, ok := strings.Cut(tableID, ".")
	if !ok || db == "" || table == "" {
		return "", fmt.Errorf("storage_integrity target table id %q must be db.table for schema resolution", tableID)
	}
	schemas, err := p.schemaLoader.Load(ctx, []schemaregistry.TableRef{{TableID: tableID, Database: db, Table: table}})
	if err != nil {
		return "", fmt.Errorf("storage_integrity cannot resolve declared schema for %s: %w", tableID, err)
	}
	return payloadexec.TableSchemaHash(p.networkID, schemas[0]), nil
}
```

`admissionFromState` — after the payload bytes are final (`payload := append(...)`; the CSV branch goes away in Task 19) and before the size check, add the v2 verification:

```go
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
	}
	sv, ok := p.authValidator.(auth.StatementValidatorV2)
	if !ok {
		return Admission{}, errors.New("storage_integrity auth validator does not support envelope v2 statement tokens")
	}
	recovered, err := sv.ValidateStatementV2(state.statementToken, want)
	if err != nil {
		return Admission{}, fmt.Errorf("storage_integrity statement token rejected for %s: %w", admission.StatementID, err)
	}
	if !strings.EqualFold(recovered, admission.Signer) {
		return Admission{}, fmt.Errorf("storage_integrity statement token signer %s does not match query signer %s", recovered, admission.Signer)
	}
	if err := requireStatementIDSigner(admission.StatementID, recovered); err != nil {
		return Admission{}, err
	}
	admission.EnvelopeVersion = sicore.EnvelopeVersionV2
	admission.NetworkID = p.networkID
	admission.KeeperShardID = 0
	admission.SettingsHash = sicore.EmptySettingsHash
	admission.SchemaHash = state.schemaHash
	admission.RowIDProfileID = payloadexec.RowIDProfileID
```

Note `state.revision == 0` must already reject: add `if state.revision == 0 { return Admission{}, fmt.Errorf("storage_integrity admission %s has no client protocol revision", admission.StatementID) }` right before building `want`.

Also change the existing `Admission.SHA256` doc if needed (unchanged); imports: `"github.com/housegate/housegate/pkg/registry"`, `"github.com/housegate/housegate/pkg/schemaregistry"`, `"github.com/housegate/housegate/pkg/replay/payloadexec"`.

Every existing plugin test that admits an INSERT (`newSignedIngress` users) now needs a schema source + statement token: change `newSignedIngress` to build the network state from `ingressNetworkState` and set `NetworkID`, and have `signedQueryContext` also append a valid statement token for the empty payload it will capture? Simpler: keep `signedQueryContext` as is and add `withStatementToken(...)` calls where a test drives capture through `OnQueryInputComplete`/`ConsumeAdmission` (the tests listed in `plugin_test.go` that call `ConsumeAdmission` or a consumer). Tests that only exercise `OnQuery` rejections (`TestIngressRejects*`) still need a token present because the missing-token check runs in OnQuery after auth: add the token in `signedQueryContext` for the default payload `[]byte{2,0,0xab,0xcd}` and 54453; tests that capture other bytes override with `withStatementToken` before OnQuery.

- [x] **Step 4: Run tests**

Run: `bazel test //pkg/plugins/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS.

- [x] **Step 5: Commit**

```bash
git add pkg/plugins/storageintegrity
git commit -m "feat(ingress): validate envelope-v2 statement token against the ingress's own capture"
```

### Task 19: Delete the CSV bridge; runtime pins Native; root projection carries v2 fields

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Delete: `pkg/storageintegrity/csv_payload.go`, `pkg/storageintegrity/csv_payload_test.go`
- Modify: `pkg/plugins/storageintegrity/plugin.go` (remove `PayloadMaterializer` config/field/branch), `proxy.go` (remove `Options.StorageIntegrityPayloadMaterializer`), `build.go:605-641` (remove the materializer requirement/wiring), `storage_integrity_runtime.go:133` (`MaterializerNative`), `storage_integrity_ingress.go:130-132,164-189` (drop CSV-revision check; carry v2 fields), `pkg/storageintegrity/materializer_select.go` (doc only), tests: `build_test.go`, `storage_integrity_ingress_test.go`
- Test: `storage_integrity_ingress_test.go` (append `TestAdmissionRecordFromPluginCarriesEnvelopeV2Fields`)

**Interfaces:**
- Produces: `AdmissionRecordFromPlugin` copies `EnvelopeVersion/NetworkID/KeeperShardID/SettingsHash/SchemaHash/RowIDProfileID`; built-in runtime materializer kind = `sicore.MaterializerNative`; `sicore.PayloadMaterializer`, `sicore.TableSchemaResolver`, `sicore.NativeCSVPayloadMaterializer` no longer exist.

- [x] **Step 1: Write the failing test**

Append to `storage_integrity_ingress_test.go`:

```go
func TestAdmissionRecordFromPluginCarriesEnvelopeV2Fields(t *testing.T) {
	adm := siplugin.Admission{
		StatementID: "0xabc0000000000000000000000000000000000001:1:n1", Kind: siplugin.KindInsert, TableID: "tenant.events",
		SQL: "INSERT INTO tenant.events FORMAT Native", SQLHash: replay.DigestString("INSERT INTO tenant.events FORMAT Native"),
		Signer: "0xabc0000000000000000000000000000000000001", UserJWS: "v2.token.x", AuthToken: "v1.token.x",
		Payload:         siplugin.CapturedPayload{Bytes: []byte{2, 0, 1}, Length: 3, Encoding: sicore.PayloadEncodingClickHouseNativeData, Revision: 54460, Complete: true},
		EnvelopeVersion: 2, NetworkID: "testnet-v2", KeeperShardID: 0, SettingsHash: sicore.EmptySettingsHash, SchemaHash: "0x11", RowIDProfileID: payloadexec.RowIDProfileID,
	}
	rec := AdmissionRecordFromPlugin(adm)
	if rec.EnvelopeVersion != 2 || rec.NetworkID != "testnet-v2" || rec.KeeperShardID != 0 || rec.SettingsHash != sicore.EmptySettingsHash || rec.SchemaHash != "0x11" || rec.RowIDProfileID != payloadexec.RowIDProfileID || rec.UserJWS != "v2.token.x" || rec.Revision != 54460 || rec.PayloadEncoding != sicore.PayloadEncodingClickHouseNativeData {
		t.Fatalf("v2 fields not carried: %+v", rec)
	}
}

func TestBuildStorageIntegrityRuntimePinsNativeMaterializer(t *testing.T) {
	// See TestBuild... helper set in this file: construct the runtime as the
	// existing "auto wires status querier" test does and assert matKind.
	ingress, _, err := buildStorageIntegrityRuntimeConsumer(runtimeTestConfig(t), StorageIntegrityRuntimeOptions{
		StatementSubmitter: &rootRecordingSubmitter{}, SourcePreparer: &rootRecordingPreparer{},
		StatusQuerier: rootRecordingStatusQuerier{}, PayloadWriter: &rootRecordingPayloadWriter{}, MergeGuard: &recordingBuildMergeGuard{},
	})
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntimeConsumer: %v", err)
	}
	defer ingress.Close()
	if ingress.matKind != sicore.MaterializerNative {
		t.Fatalf("runtime materializer = %v, want Native", ingress.matKind)
	}
}
```

(`runtimeTestConfig(t)` is whatever helper the existing `TestBuildStorageIntegrityRuntime*` tests use to produce a valid `config.StorageIntegrityRuntimeConfig`; reuse it by name — grep `buildStorageIntegrityRuntimeConsumer(` in `build_test.go` / `storage_integrity_ingress_test.go` and mirror the config construction there.)

- [x] **Step 2: Remove the bridge**

```bash
git rm pkg/storageintegrity/csv_payload.go pkg/storageintegrity/csv_payload_test.go
```

- `pkg/plugins/storageintegrity/plugin.go`: delete `Config.PayloadMaterializer`, `Plugin.payloadMaterializer`, the `payloadEncoding == sicore.EncodingCSVWithNames && p.payloadMaterializer == nil` check in `OnQuery`, and the whole `if payloadEncoding == sicore.EncodingCSVWithNames { ... }` branch in `admissionFromState`.
- `proxy.go`: delete `Options.StorageIntegrityPayloadMaterializer` and its comment.
- `build.go`: delete the `if opts.StorageIntegrityPayloadMaterializer == nil { return ... "required for CSVWithNames" }` check and the `PayloadMaterializer:` line in `storageintegrity.New(...)`.
- `storage_integrity_runtime.go:133`: `NewStorageIntegrityIngressWithPayloadWriter(orch, mergeGuard, sicore.MaterializerNative, payloadWriter)`.
- `storage_integrity_ingress.go`: delete the `if i.matKind == sicore.MaterializerCSV && rec.Revision == 0 {...}` block; in `AdmissionRecordFromPlugin` add `EnvelopeVersion: adm.EnvelopeVersion, NetworkID: adm.NetworkID, KeeperShardID: adm.KeeperShardID, SettingsHash: adm.SettingsHash, SchemaHash: adm.SchemaHash, RowIDProfileID: adm.RowIDProfileID`.
- `pkg/storageintegrity/materializer_select.go`: update the doc comment: "The built-in runtime pins MaterializerNative; MaterializerCSV remains for payloadexec test payloads."
- Tests: delete `TestBuildServer_StorageIntegrityIngressWiresCSVPayloadMaterializer`, `TestBuildServer_StorageIntegrityRuntimeRequiresCSVPayloadMaterializer`, `recordingBuildPayloadMaterializer` (build_test.go); in `storage_integrity_ingress_test.go` change every `sicore.MaterializerCSV` argument to `sicore.MaterializerNative`, delete `TestStorageIntegrityIngressRejectsCSVWithoutRevisionBeforePayloadPut`, and change `TestStorageIntegrityIngressRejectsWrongMaterializerBeforePayloadPut` to feed `sicore.EncodingCSVWithNames` and expect `"requires Native materializer"`; delete `TestIngressRejectsCSVWithNamesWithoutMaterializer` and `TestIngressMaterializesCSVWithNamesAdmissionFromCapturedNativeData` in the plugin tests; in `build_test.go` remove `StorageIntegrityPayloadMaterializer:` from every `Options{...}` literal.

- [x] **Step 3: Run the whole housegate suite**

Run: `bazel run //:gazelle && bazel test //... --test_output=errors`
Expected: PASS (integration targets are `manual` and skipped here).

- [x] **Step 4: Commit**

```bash
git add -A pkg/storageintegrity pkg/plugins/storageintegrity proxy.go build.go build_test.go storage_integrity_ingress.go storage_integrity_ingress_test.go storage_integrity_runtime.go BUILD.bazel
git commit -m "refactor(storageintegrity): remove the CSV bridge; runtime pins the Native materializer; carry envelope v2 through the root projection"
```

### Task 20: `buildServer` ingress wiring + docker integration test (agent → server → ClickHouse)

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `pkg/config/storage_integrity_config.go` (`StorageIntegrityIngressConfig.NetworkID`, required when enabled), `build.go:600-655` (pass `TableSchemas`/`NetworkID`)
- Test: `build_test.go` (append), `pkg/integration/storage_integrity_agent_test.go` (new)

**Interfaces:**
- Consumes: Task 16 `resolveTableSchemas`, Task 18 `Config.TableSchemas/NetworkID`, `testenv.StartServerProxy/StartAgentProxy`, `network.InMemoryNetworkState.TableSchemas`.
- Produces: `storage_integrity.ingress.network_id` (yaml), server-side wiring; the integration test proves stored bytes == signed bytes.

- [x] **Step 1: Write the failing tests**

`pkg/config/storage_integrity_agent_config_test.go` — append:

```go
func TestStorageIntegrityIngressConfig_RequiresNetworkID(t *testing.T) {
	c := Default()
	c.Listen = "127.0.0.1:0"
	c.NetworkState.Source = "ns.yaml"
	c.StorageIntegrity.Ingress.Enabled = true
	c.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x0000000000000000000000000000000000000001"}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "network_id") {
		t.Fatalf("ingress without network_id must be rejected: %v", err)
	}
	c.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	if err := c.Validate(); err != nil {
		t.Fatalf("valid ingress config rejected: %v", err)
	}
}
```

`build_test.go` — append:

```go
func TestBuildServer_StorageIntegrityIngressRequiresTableSchemas(t *testing.T) {
	signer, _ := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	cfg := minimalRouterOnlyCfg(t)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
	cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = time.Second
	cfg.StorageIntegrity.Ingress.MaxPayloadBytes = 1 << 20
	_, err := buildServer(Options{Config: cfg, NetworkState: registryWithoutSchemas{network.NewInMemoryNetworkState()}, StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{}}, nil)
	if err == nil || !strings.Contains(err.Error(), "TableSchemas") {
		t.Fatalf("expected TableSchemas requirement, got %v", err)
	}
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState(), StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{}}, nil)
	if err != nil {
		t.Fatalf("buildServer with in-memory TableSchemas: %v", err)
	}
	bs.teardown()
}
```

`pkg/integration/storage_integrity_agent_test.go` — new:

```go
package integration

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/network"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type capturingConsumer struct {
	mu   sync.Mutex
	seen []siplugin.Admission
}

func (c *capturingConsumer) ConsumeStorageIntegrityAdmission(_ context.Context, adm siplugin.Admission) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = append(c.seen, adm)
	return nil
}

func siAgentSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID: chEnv.Database + ".si_events",
		Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}},
	}
}

func withDeclaredSchema(t *testing.T, networkID string) testenv.ProxyOption {
	t.Helper()
	schema := siAgentSchema()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	return func(_ *config.Config, opts *housegate.Options) {
		ns := opts.NetworkState.(*network.InMemoryNetworkState)
		ns.TableSchemas[chEnv.Database+"/si_events@1"] = network.TableSchemaInfo{
			DatabaseId: chEnv.Database, TableId: "si_events", Version: 1,
			SchemaHash: payloadexec.TableSchemaHash(networkID, schema), SchemaJson: string(js),
		}
	}
}

// TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd runs client → agent
// housegate (storage_integrity.agent) → server housegate (ingress) → CH and
// proves the ingress stored exactly the bytes the agent signed: the v2
// token validates against a payload_hash recomputed from the stored bytes,
// and those bytes decode (Native, at the pinned revision) into the rows the
// client appended.
func TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd(t *testing.T) {
	const networkID = "itest-net"
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS "+chEnv.Database+".si_events (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	consumer := &capturingConsumer{}
	rewriterOpt, _ := testenv.WithRewriterMock(t)
	server := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityAdmissionConsumer = consumer },
	)
	agentProxy := testenv.StartAgentProxy(t, authTestKey1, server.Addr,
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
		}),
	)

	conn := openConnNoCompression(t, agentProxy.Addr)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+chEnv.Database+".si_events")
	if err != nil {
		t.Fatalf("PrepareBatch through agent: %v", err)
	}
	if err := batch.Append(uint64(1), "eu"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(uint64(2), "us"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch.Send through agent → server: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		consumer.mu.Lock()
		n := len(consumer.seen)
		consumer.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.seen) != 1 {
		t.Fatalf("consumer saw %d admissions, want 1", len(consumer.seen))
	}
	adm := consumer.seen[0]
	if adm.EnvelopeVersion != 2 || adm.NetworkID != networkID || adm.Payload.Encoding != sicore.PayloadEncodingClickHouseNativeData || adm.Payload.Revision == 0 {
		t.Fatalf("admission is not envelope v2: %+v", adm)
	}
	if account, seq, _, err := sicore.ParseFlatStatementID(adm.StatementID); err != nil || account != signer.Address() || seq != 1 {
		t.Fatalf("statement id %q: account=%s seq=%d err=%v", adm.StatementID, account, seq, err)
	}
	// The stored bytes are the signed bytes: recompute the expectation from the
	// stored payload and validate the sequenced token against it.
	validator := auth.NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	want := auth.JWSStatementPayloadV2{
		NetworkID: networkID, StatementID: adm.StatementID, SQLHash: replay.DigestString(adm.SQL),
		SettingsHash: sicore.EmptySettingsHash, SchemaHash: adm.SchemaHash,
		PayloadHash: replay.DigestBytes(adm.Payload.Bytes), PayloadLength: uint64(len(adm.Payload.Bytes)),
		PayloadFormat: sicore.PayloadEncodingClickHouseNativeData, ClientRevision: uint32(adm.Payload.Revision),
		TargetTableID: chEnv.Database + ".si_events", RowIDProfileID: payloadexec.RowIDProfileID,
	}
	if got, err := validator.ValidateStatementV2(adm.UserJWS, want); err != nil || got != signer.Address() {
		t.Fatalf("stored bytes do not match the signed envelope: signer=%s err=%v", got, err)
	}
	if adm.SchemaHash != payloadexec.TableSchemaHash(networkID, siAgentSchema()) {
		t.Fatalf("schema_hash %s does not match the declared schema", adm.SchemaHash)
	}
	rows, err := nativepayload.Decode(siAgentSchema(), adm.Payload.Revision, adm.Payload.Bytes)
	if err != nil {
		t.Fatalf("stored bytes are not the client's Native Data packets: %v", err)
	}
	if len(rows) != 2 || rows[0].Values[0] != uint64(1) || rows[0].Values[1] != "eu" || rows[1].Values[0] != uint64(2) || rows[1].Values[1] != "us" {
		t.Fatalf("decoded rows = %+v", rows)
	}
	if !strings.HasPrefix(adm.SQL, "INSERT INTO "+chEnv.Database+".si_events") {
		t.Fatalf("signed SQL = %q", adm.SQL)
	}
}
```

- [x] **Step 2: Run the unit parts to verify failure**

Run: `bazel run //:gazelle && bazel test //pkg/config:config_test //:housegate_test --test_filter='TestStorageIntegrityIngressConfig_RequiresNetworkID|TestBuildServer_StorageIntegrityIngressRequiresTableSchemas' --test_output=errors`
Expected: FAIL (`network_id` not rejected; `TableSchemas` requirement missing).

- [x] **Step 3: Implement**

`pkg/config/storage_integrity_config.go`: add `NetworkID string \`json:"network_id" yaml:"network_id"\`` to `StorageIntegrityIngressConfig` and, in `validate(mode)` next to the allowlist check:

```go
	if strings.TrimSpace(c.Ingress.NetworkID) == "" {
		errs = append(errs, errors.New("storage_integrity.ingress.network_id is required when storage_integrity.ingress.enabled"))
	}
```

`build.go` in the `if cfg.StorageIntegrity.Ingress.Enabled {` block, before `storageIntegrityIngress = storageintegrity.New(...)`:

```go
		ingressSchemas, err := resolveTableSchemas(opts, reg, "storage_integrity.ingress")
		if err != nil {
			return nil, err
		}
```

and add `TableSchemas: ingressSchemas, NetworkID: ingressCfg.NetworkID,` to the `storageintegrity.Config{...}` literal; add `"network_id", ingressCfg.NetworkID` to the `log.Infow("storage_integrity ingress enabled", ...)` call. (`reg` is the `registry.Registry` resolved earlier in `buildServer` at ~:325.)

Every existing `build_test.go` test enabling the ingress must now set `cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"` (grep `Ingress.Enabled = true`); `enableStorageIntegrityRuntimeTestConfig` likewise.

- [x] **Step 4: Run unit + docker integration**

Run: `bazel test //pkg/config:config_test //:housegate_test --test_output=errors && bazel test //pkg/integration:integration_test --test_filter='TestStorageIntegrity_AgentSignsEnvelopeV2EndToEnd|TestAgent_PinnedUpstream_HappyPath' --test_output=errors`
Expected: PASS (docker + `clickhouse/clickhouse-server:25.8` must be available locally, as for every `pkg/integration` target; the target is already in `.github/workflows/ci.yml`'s explicit list, so no CI change is needed).

- [x] **Step 5: Commit**

```bash
git add pkg/config build.go build_test.go pkg/integration/storage_integrity_agent_test.go pkg/integration/BUILD.bazel
git commit -m "feat(server): wire envelope-v2 ingress schema source + network id; agent→server→CH integration test"
```

### Task 21: housegate docs, full suite, re-pin arbiter-proto to v0.5.0, tag housegate

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `CLAUDE.md` (Key Modules bullets), `go.mod`/`go.sum`/`MODULE.bazel.lock`

- [x] **Step 1: Update CLAUDE.md**

In the `pkg/plugins/` bullet list add, after the `storageintegrity` ingress mention (add one if missing):

```
`sistatement` (agent-mode-only, config `storage_integrity.agent`; the envelope-v2 signer: for payload-local Native INSERTs it resolves the declared network-state schema, sets `QueryContext.DeferredInsert` so Relay answers the sample block locally and buffers the payload, then signs `SQL_x_statement_token` (`auth.StatementPurposeV2`) over statement id / sql / settings / schema / payload hashes + format + client revision + target + network + shard + row-id profile; durable `client_seq` in `<state_dir>/<account>.seq`; runs after `materialize`, before the agent `Signer`)
```

In `pkg/plugin/` bullet add: "`QueryContext.DeferredInsert` (`DeferredInsertPlan`) switches Relay into deferred-INSERT mode (see relay.go `runDeferredInsert`): local sample block via `Codec.WriteSampleBlock`, buffered payload under `MaxPayloadBytes`, forward only after `OnQueryInputCompleteStrict`; mutually exclusive with `SuppressUpstreamExecution`."

Add a `pkg/replay/nativepayload/` bullet: "Native `ClientData` payload decoder (`payload_format` `clickhouse-native-data-v1`) shared by chexec / arbiter-core; moved out of `pkg/storageintegrity` (which keeps aliases)."

In the storage-integrity ingress description note: "ingress validates the v2 statement token against its own capture (`payload_hash` of the exact Native bytes, `schema_hash` from network state, `settings_hash == EmptySettingsHash`); the CSV bridge (`NativeCSVPayloadMaterializer`, `StorageIntegrityPayloadMaterializer`) is gone; the runtime pins `MaterializerNative`."

- [x] **Step 2: Re-pin arbiter-proto to the tag (after Task 2)**

```bash
go get github.com/sentioxyz/arbiter-proto@v0.5.0 && go mod tidy && bazel mod tidy && bazel run //:gazelle
git diff --stat go.mod
```

- [x] **Step 3: Full verification**

Run: `bazel build //... && bazel test //... --test_output=errors && bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_output=errors`
Expected: PASS; compare any integration failures against a clean `main` build before calling them regressions (CLAUDE.md rule).

- [ ] **Step 4: Commit, merge, tag**

```bash
git add CLAUDE.md go.mod go.sum MODULE.bazel.lock
git commit -m "docs: envelope v2 / deferred-INSERT / sistatement; pin arbiter-proto v0.5.0"
# after the branch is merged to main via PR (release.yml cuts the tag from a vX.Y.Z tag push):
git tag -a v0.9.0 -m "housegate v0.9.0: storage-integrity signed envelope v2, deferred-INSERT relay, agent statement plugin"
git push origin v0.9.0
git rev-parse HEAD   # HOUSEGATE_SHA for Tasks 22 and 27
```

---

## Phase 6 — arbiter-core (spec §10 step 6, §8)

### Task 22: Point arbiter-core at the new housegate + arbiter-proto

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel` (housegate `bazel_dep` version + `git_override` commit), `MODULE.bazel.lock`, `README.md` (pin comment), root `BUILD.bazel` (gazelle resolve lines)

- [ ] **Step 1: Bump pins**

```bash
# housegate: tag when Task 21 is tagged, else the commit sha
bash scripts/update-housegate.sh v0.9.0        # or: bash scripts/update-housegate.sh $HOUSEGATE_SHA
GOWORK=off go get github.com/sentioxyz/arbiter-proto@v0.5.0
GOWORK=off go mod tidy
bazel mod deps --lockfile_mode=update >/dev/null
```

- [ ] **Step 2: Teach gazelle the two new housegate imports**

In root `BUILD.bazel` add next to the existing `# gazelle:resolve go github.com/housegate/housegate/pkg/replay/payloadexec ...` line:

```
# gazelle:resolve go github.com/housegate/housegate/pkg/replay/nativepayload @housegate//pkg/replay/nativepayload
# gazelle:resolve go github.com/housegate/housegate/pkg/auth @housegate//pkg/auth
```

- [ ] **Step 3: Build; observe the conformance gate now failing (expected until Task 23)**

Run: `bazel run //:gazelle && bazel build //... && bazel test //conformance:conformance_test --test_output=errors`
Expected: build OK; `TestArbiterMirrorsMatchProto` FAILS with `StatementEnvelope: proto field "envelope_version" has no Go mirror json tag` (and the six siblings) — this is Task 23's red test.

- [ ] **Step 4: Commit**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock README.md BUILD.bazel
git commit -m "chore(deps): housegate v0.9.0 + arbiter-proto v0.5.0 (envelope v2)"
```

### Task 23: `arbiter.StatementEnvelope` v2 fields, wire conversion, `DomainL3Statements`

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Modify: `types.go:128-139`, `domains.go`, `wire/convert.go:32-60`, `wire/dispatch.go:15-44`
- Test: `conformance/arbiter_wire_test.go` (already red), `wire/convert_test.go` (append round-trip)

**Interfaces:**
- Produces: `arbiter.StatementEnvelope{...; EnvelopeVersion uint32 \`json:"envelope_version"\`; NetworkID string \`json:"network_id"\`; KeeperShardID uint32 \`json:"keeper_shard_id"\`; PayloadFormat string \`json:"payload_format"\`; ClientRevision uint32 \`json:"client_revision"\`; SchemaHash string \`json:"schema_hash"\`; RowIDProfileID string \`json:"row_id_profile_id"\`}`, `arbiter.DomainL3Statements = "arbiter-l3-statements-v1"`, `wire.EnvelopeFromPB/EnvelopeToPB` and `statementToPB/StatementFromPB` carry the new fields.

- [ ] **Step 1: Write the failing round-trip test**

Append to `wire/convert_test.go` (create the file if it does not exist, package `wire`):

```go
func TestEnvelopeV2RoundTripsEveryField(t *testing.T) {
	in := arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 7, ClientNonce: "n7"},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             "INSERT INTO db.t FORMAT Native",
		SQLHash:         "0x11",
		SettingsHash:    "0x22",
		PayloadRef:      "ref",
		PayloadHash:     "0x33",
		PayloadLength:   9,
		TargetTableID:   "db.t",
		UserJWS:         "a.b.c",
		EnvelopeVersion: 2,
		NetworkID:       "net",
		KeeperShardID:   0,
		PayloadFormat:   "clickhouse-native-data-v1",
		ClientRevision:  54460,
		SchemaHash:      "0x44",
		RowIDProfileID:  "housegate-row-id-v1",
	}
	out := EnvelopeFromPB(EnvelopeToPB(in))
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip lost fields:\n in=%+v\nout=%+v", in, out)
	}
	st := replay.Statement{StatementID: "0xabc:7:n7", StatementSeq: 3, SQL: in.SQL, SQLHash: in.SQLHash, SettingsHash: in.SettingsHash, PayloadRef: "ref", PayloadHash: "0x33", PayloadLength: 9, TargetTableID: "db.t", UserJWS: "a.b.c", PayloadFormat: "clickhouse-native-data-v1", ClientRevision: 54460, SchemaHash: "0x44"}
	if got := StatementFromPB(statementToPB(st)); !reflect.DeepEqual(st, got) {
		t.Fatalf("replay statement round trip lost fields:\n in=%+v\nout=%+v", st, got)
	}
}
```

(imports: `reflect`, `testing`, `github.com/housegate/housegate/pkg/replay`, `github.com/sentioxyz/arbiter-core`.)

- [ ] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //wire:wire_test //conformance:conformance_test --test_output=errors`
Expected: build error `unknown field EnvelopeVersion` / conformance FAIL.

- [ ] **Step 3: Implement**

`types.go` — extend `StatementEnvelope`:

```go
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
	// ---- envelope v2 (spec 2026-08-18 signed envelope v2 §4.1); all signed
	// by UserJWS (purpose housegate-statement-v2) ----
	EnvelopeVersion uint32 `json:"envelope_version"`
	NetworkID       string `json:"network_id"`
	KeeperShardID   uint32 `json:"keeper_shard_id"`
	PayloadFormat   string `json:"payload_format"`
	ClientRevision  uint32 `json:"client_revision"`
	SchemaHash      string `json:"schema_hash"`
	RowIDProfileID  string `json:"row_id_profile_id"`
}
```

`domains.go` — add inside the const block:

```go
	// DomainL3Statements commits the sealed block's envelopes (statement_seq
	// order) into L3BlockHeader.StatementsRoot:
	// CanonicalDigest(DomainL3Statements, []StatementEnvelope).
	DomainL3Statements = "arbiter-l3-statements-v1"
```

`wire/convert.go` — extend both envelope converters:

```go
func EnvelopeFromPB(m *pb.StatementEnvelopeV2) arbiter.StatementEnvelope {
	return arbiter.StatementEnvelope{
		StatementID:     statementIDFromPB(m.GetStatementId()),
		StatementKind:   arbiter.StatementKind(m.GetStatementKind()),
		SQL:             m.GetSql(),
		SQLHash:         m.GetSqlHash(),
		SettingsHash:    m.GetSettingsHash(),
		PayloadRef:      m.GetPayloadRef(),
		PayloadHash:     m.GetPayloadHash(),
		PayloadLength:   m.GetPayloadLength(),
		TargetTableID:   m.GetTargetTableId(),
		UserJWS:         m.GetUserJws(),
		EnvelopeVersion: m.GetEnvelopeVersion(),
		NetworkID:       m.GetNetworkId(),
		KeeperShardID:   m.GetKeeperShardId(),
		PayloadFormat:   m.GetPayloadFormat(),
		ClientRevision:  m.GetClientRevision(),
		SchemaHash:      m.GetSchemaHash(),
		RowIDProfileID:  m.GetRowIdProfileId(),
	}
}

func EnvelopeToPB(v arbiter.StatementEnvelope) *pb.StatementEnvelopeV2 {
	return &pb.StatementEnvelopeV2{
		StatementId:     statementIDToPB(v.StatementID),
		StatementKind:   pb.StatementKind(v.StatementKind),
		Sql:             v.SQL,
		SqlHash:         v.SQLHash,
		SettingsHash:    v.SettingsHash,
		PayloadRef:      v.PayloadRef,
		PayloadHash:     v.PayloadHash,
		PayloadLength:   v.PayloadLength,
		TargetTableId:   v.TargetTableID,
		UserJws:         v.UserJWS,
		EnvelopeVersion: v.EnvelopeVersion,
		NetworkId:       v.NetworkID,
		KeeperShardId:   v.KeeperShardID,
		PayloadFormat:   v.PayloadFormat,
		ClientRevision:  v.ClientRevision,
		SchemaHash:      v.SchemaHash,
		RowIdProfileId:  v.RowIDProfileID,
	}
}
```

`wire/dispatch.go` — add to `statementToPB`: `PayloadFormat: s.PayloadFormat, ClientRevision: s.ClientRevision, SchemaHash: s.SchemaHash,` and to `StatementFromPB`: `PayloadFormat: m.GetPayloadFormat(), ClientRevision: m.GetClientRevision(), SchemaHash: m.GetSchemaHash(),`.

- [ ] **Step 4: Run tests**

Run: `bazel test //wire:wire_test //conformance:conformance_test //:arbiter-core_test --test_output=errors`
Expected: PASS (`TestArbiterMirrorsMatchProto`, `TestReplayWireTypesMirrorPkgReplay` green again).

- [ ] **Step 5: Commit**

```bash
git add types.go domains.go wire conformance
git commit -m "feat: statement envelope v2 fields, DomainL3Statements, replay statement format/revision/schema_hash on the wire"
```

### Task 24: SNode staged prepare decodes Native and verifies `schema_hash`

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Modify: `snode/staged.go:22,45-49,103-125`
- Test: `snode/staged_prepare_test.go` (fixtures → Native), `snode/journal_test.go:20`, `snode/intake_fixtures_test.go` (envelope v2 fields), new `snode/native_fixture_test.go`

**Interfaces:**
- Consumes: `nativepayload.PayloadFormat`, `nativepayload.Decode`, `payloadexec.TableSchemaHash`, `payloadexec.RowIDProfileID`.
- Produces: `snode.ErrSchemaHashMismatch` (terminal reject class; sentio-node maps it to `sicore.OutcomeTerminalReject` in Task 33), `PrepareRequest.PayloadEncoding` must be `clickhouse-native-data-v1`.

- [ ] **Step 1: Write the fixture + failing tests**

Create `snode/native_fixture_test.go`:

```go
package snode

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

const testRevision = 54460

// nativePayload encodes one client Data packet with columns (p String,
// v UInt64) — the wire bytes an agent-side housegate would have signed.
func nativePayload(t *testing.T, rows ...struct {
	P string
	V uint64
}) []byte {
	t.Helper()
	p := proto.ColStr{}
	v := proto.ColUInt64{}
	for _, r := range rows {
		p.Append(r.P)
		v.Append(r.V)
	}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: len(rows), Columns: 2}).EncodeBlock(&buf, testRevision, proto.Input{{Name: "p", Data: &p}, {Name: "v", Data: &v}}); err != nil {
		t.Fatalf("encode native payload: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

type pv = struct {
	P string
	V uint64
}
```

Edit `snode/staged_prepare_test.go`: `const testEncoding = "clickhouse-native-data-v1"`; replace every CSV payload literal (`[]byte("p,v\np0,1\np0,2\n")` etc.) with `nativePayload(t, pv{"p0", 1}, pv{"p0", 2})` (same rows), and every `res.PayloadEncoding != testEncoding` stays valid. Edit `snode/journal_test.go:20` to `PayloadEncoding: "clickhouse-native-data-v1"`. Edit `snode/intake_fixtures_test.go` `intakeEnvelope`:

```go
func intakeEnvelope(payload []byte) arbiter.StatementEnvelope {
	sql := "INSERT INTO db.t FORMAT Native"
	schema := intakeSchema()
	return arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: "0xacct", ClientSeq: 1, ClientNonce: "n"},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             sql,
		SQLHash:         replay.DigestString(sql),
		SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
		PayloadRef:      "payload-1.native",
		PayloadHash:     replay.DigestBytes(payload),
		PayloadLength:   uint64(len(payload)),
		TargetTableID:   "db.t",
		UserJWS:         "x.y.z",
		EnvelopeVersion: 2,
		NetworkID:       "testnet",
		KeeperShardID:   0,
		PayloadFormat:   "clickhouse-native-data-v1",
		ClientRevision:  testRevision,
		SchemaHash:      payloadexec.TableSchemaHash("testnet", schema),
		RowIDProfileID:  payloadexec.RowIDProfileID,
	}
}
```

(`testConfigS(t).NetworkID` must be `"testnet"` — grep it and align the literal if it differs.)

Append to `snode/staged_prepare_test.go`:

```go
func TestPrepareLocalStatement_RejectsCSVEncoding(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{intakeSchema()}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	payload := nativePayload(t, pv{"p0", 1})
	req := stagedRequest(payload)
	req.PayloadEncoding = "csv-with-names-v1"
	_, err := role.PrepareLocalStatement(ctx, req, payload)
	if !errors.Is(err, ErrEncodingNotSupported) {
		t.Fatalf("csv must be rejected with ErrEncodingNotSupported, got %v", err)
	}
}

func TestPrepareLocalStatement_RejectsSchemaHashMismatchBeforeDecode(t *testing.T) {
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
	req.Envelope.SchemaHash = "0x" + strings.Repeat("ee", 32)
	before := countActiveParts(t, conn, role, schema)
	_, err := role.PrepareLocalStatement(ctx, req, payload)
	if !errors.Is(err, ErrSchemaHashMismatch) {
		t.Fatalf("schema hash mismatch must be terminal, got %v", err)
	}
	if after := countActiveParts(t, conn, role, schema); after != before {
		t.Fatalf("schema hash mismatch must not write unsafe parts: before=%d after=%d", before, after)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //snode:snode_test --test_output=errors` (non-docker part) and, with ClickHouse running (`docker run -d --rm --name arbiter-core-ch -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:25.8`): `ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 bazel test //snode:snode_test --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=900 --test_output=errors`
Expected: build error `undefined: ErrSchemaHashMismatch`; after adding only the sentinel, `TestPrepareLocalStatement_CleanPath` fails with `encoding "clickhouse-native-data-v1": snode: payload encoding not supported`.

- [ ] **Step 3: Implement**

`snode/staged.go`: replace `const stagedCSVEncoding = "csv-with-names-v1"` with

```go
// stagedNativeEncoding is the only payload format the SI lane admits
// (envelope v2 D1): the exact ClickHouse Native ClientData wire bytes.
const stagedNativeEncoding = nativepayload.PayloadFormat
```

add the sentinel to the `var (...)` block:

```go
	ErrSchemaHashMismatch = errors.New("snode: envelope schema_hash does not match this source's declared schema")
```

and rewrite the section from `if req.PayloadEncoding != stagedCSVEncoding {` through the `rows[i].RowID = ...` loop as:

```go
	if req.PayloadEncoding != stagedNativeEncoding {
		return PreparedLocalResult{}, fmt.Errorf("encoding %q: %w", req.PayloadEncoding, ErrEncodingNotSupported)
	}
	if req.Revision == 0 {
		return PreparedLocalResult{}, fmt.Errorf("revision must be non-zero: %w", ErrPayloadMismatch)
	}
	if err := validatePayloadBinding(req.Envelope, payload); err != nil {
		return PreparedLocalResult{}, fmt.Errorf("%v: %w", err, ErrPayloadMismatch)
	}
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
	if r.d.Payloads == nil || r.d.Conn == nil {
		return PreparedLocalResult{}, errors.New("snode: payload store and clickhouse connection are required")
	}

	rows, err := nativepayload.Decode(schema, req.Revision, payload)
	if err != nil {
		return PreparedLocalResult{}, fmt.Errorf("decode payload: %v: %w", err, ErrPayloadMismatch)
	}
	for i := range rows {
		rows[i].RowID = payloadexec.RowID(r.cfg.NetworkID, schema.TableID, flat, uint64(i))
	}
```

Add import `"github.com/housegate/housegate/pkg/replay/nativepayload"`.

- [ ] **Step 4: Run tests (unit + docker)**

Run: `bazel run //:gazelle && bazel test //snode:snode_test --test_output=errors && ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 bazel test //snode:snode_test --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=900 --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add snode BUILD.bazel
git commit -m "feat(snode): stage Native payloads and reject schema_hash mismatch before decode"
```

### Task 25: Verifier schema-hash source

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

**Files:**
- Modify: `verifier/backends.go:19-30`
- Test: `verifier/backends_test.go` (append)

**Interfaces:**
- Consumes: Task 6 `replay.Verifier.SchemaHashes` / `replay.SchemaHashSource`.
- Produces: `NewReplayCore` sets `SchemaHashes` from `cfg.Tables` (`payloadexec.TableSchemaHash(cfg.NetworkID, t)`); the Native replay branch is `chexec` (housegate Task 7) — equivalence with the in-process executor is proven by housegate's docker test `TestReplayCHExecutorNativePayloadMatchesInProcessRoot` (added in Task 7 Step 5).

- [ ] **Step 1: Write the failing test**

Append to `verifier/backends_test.go`:

```go
func TestNewReplayCore_WiresSchemaHashSourceFromTables(t *testing.T) {
	cfg := testConfigV()
	core, err := NewReplayCore(cfg, nil, payloadexec.NewMemSnapshotStore(), payloadexec.NewMemPayloadStore())
	if err != nil {
		t.Fatalf("NewReplayCore: %v", err)
	}
	if core.SchemaHashes == nil {
		t.Fatal("verifier must verify signed schema_hash against its own tables")
	}
	got, ok := core.SchemaHashes.TableSchemaHash("db.t")
	if !ok || got != payloadexec.TableSchemaHash(cfg.NetworkID, cfg.Tables[0]) {
		t.Fatalf("schema hash for db.t = %q ok=%v", got, ok)
	}
	if _, ok := core.SchemaHashes.TableSchemaHash("db.unknown"); ok {
		t.Fatal("unknown table must not resolve")
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `bazel test //verifier:verifier_test --test_filter=TestNewReplayCore_WiresSchemaHashSourceFromTables --test_output=errors`
Expected: FAIL `verifier must verify signed schema_hash against its own tables`.

- [ ] **Step 3: Implement**

In `verifier/backends.go`:

```go
// tableSchemaHashes implements replay.SchemaHashSource over the verifier's
// configured tables (Phase-B hashes under this network id).
type tableSchemaHashes struct {
	networkID string
	tables    []payloadexec.TableSchema
}

func (s tableSchemaHashes) TableSchemaHash(tableID string) (string, bool) {
	for _, t := range s.tables {
		if t.TableID == tableID {
			return payloadexec.TableSchemaHash(s.networkID, t), true
		}
	}
	return "", false
}
```

and in `NewReplayCore` add `SchemaHashes: tableSchemaHashes{networkID: cfg.NetworkID, tables: cfg.Tables},` to the `&replay.Verifier{...}` literal.

- [ ] **Step 4: Run, commit**

Run: `bazel test //verifier:verifier_test --test_output=errors && bash scripts/check-public-boundary.sh && bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors`
Expected: PASS.

```bash
git add verifier
git commit -m "feat(verifier): verify signed schema_hash against the verifier's declared tables"
```

### Task 26: Cut arbiter-core release

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`

- [ ] **Step 1: Merge to main, run the "Cut Release" GitHub workflow from `main`** (it validates the module, runs the ClickHouse-backed snode/verifier tests, and creates the annotated tag per the README calendar scheme). Record the resulting tag as `ARBITER_CORE_TAG` and `git rev-parse HEAD` as `ARBITER_CORE_SHA`.

- [ ] **Step 2: Verify**

Run: `cd /tmp && GOFLAGS=-mod=mod go list -m github.com/sentioxyz/arbiter-core@$ARBITER_CORE_TAG`
Expected: prints the module@tag.

---

## Phase 7 — arbiter (spec §10 step 7, §7, §4.4)

### Task 27: Point arbiter at the new arbiter-core / housegate / arbiter-proto

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock`, root `BUILD.bazel` (gazelle resolve for `pkg/auth`, `pkg/replay/nativepayload`)

- [ ] **Step 1: Bump pins**

```bash
bash scripts/update-arbiter-core.sh $ARBITER_CORE_TAG      # or the sha
bash scripts/update-housegate.sh v0.9.0                    # or $HOUSEGATE_SHA
GOWORK=off go get github.com/sentioxyz/arbiter-proto@v0.5.0
GOWORK=off go mod tidy
bazel mod deps --lockfile_mode=update >/dev/null
```

Add to root `BUILD.bazel` next to the existing housegate resolve lines:

```
# gazelle:resolve go github.com/housegate/housegate/pkg/auth @housegate//pkg/auth
# gazelle:resolve go github.com/housegate/housegate/pkg/replay/nativepayload @housegate//pkg/replay/nativepayload
```

- [ ] **Step 2: Build**

Run: `bazel run //:gazelle && bazel build //... && bazel test //fsm:fsm_test //server:server_test //orchestrator:orchestrator_test --test_output=errors`
Expected: build OK; fsm tests still pass (v1 admission still in place — the v2 cutover is Task 28).

- [ ] **Step 3: Commit**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock BUILD.bazel
git commit -m "chore(deps): arbiter-core/housegate/arbiter-proto for statement envelope v2"
```

### Task 28: FSM admission v2 (`verifyUserJWSV2`, `Params.NetworkID`, shared vectors)

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/state.go:82-86` (Params), `fsm/userjws.go` (+`verifyUserJWSV2`), `fsm/admission.go:16-46`, `config/config.go:108-113,171` (`GenesisConfig.NetworkID`), `cmd/arbiter/services.go:40-44`, `integration/chpipeline/cluster_test.go:132-135,152-156`
- Create: `fsm/testdata/statement_jws_v2.json` (copied from housegate `pkg/auth/testdata/statement_jws_v2.json`), `fsm/userjws_v2_test.go`
- Test: `fsm/admission_test.go` (rewrite `signUserJWS`/`validEnvelope` for v2 + new reject cases), `fsm/fsm_test.go:13-18` (`testParams` gains `NetworkID`)

**Interfaces:**
- Consumes: `auth.JWSStatementPayloadV2`, `auth.StatementPurposeV2`, `auth.StatementPayloadV2Mismatch` (pure, wall-clock-free), arbiter-core v2 fields.
- Produces: `fsm.Params.NetworkID string \`json:"network_id"\``; `verifyUserJWSV2(env arbiter.StatementEnvelope) error`; admission order: 1 shape → 1b v2 invariants (`envelope_version==2`, `network_id==Params.NetworkID`, `keeper_shard_id==0`, `payload_format=="clickhouse-native-data-v1"`, `row_id_profile_id=="housegate-row-id-v1"`, `settings_hash==EmptySettingsHash`, `schema_hash!=""`, `client_revision!=0`) → 2 kind → 3 `verifyUserJWSV2` → 5 dedup → 6 bind.

- [ ] **Step 1: Copy the shared vectors and write the failing tests**

```bash
mkdir -p fsm/testdata
cp /Users/uranuswch/Dev/housegate/housegate/pkg/auth/testdata/statement_jws_v2.json fsm/testdata/statement_jws_v2.json
```

Create `fsm/userjws_v2_test.go`:

```go
package fsm

import (
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"

	"github.com/sentioxyz/arbiter-core"
)

type statementV2Vector struct {
	Name    string                     `json:"name"`
	Expect  string                     `json:"expect"`
	Payload auth.JWSStatementPayloadV2 `json:"payload"`
	Token   string                     `json:"token"`
}

type statementV2VectorFile struct {
	SignerAddress string              `json:"signer_address"`
	Vectors       []statementV2Vector `json:"vectors"`
}

// envelopeFromVector rebuilds the envelope the vector's expectation describes;
// the FSM must derive exactly this expectation from an envelope.
func envelopeFromVector(t *testing.T, v statementV2Vector) arbiter.StatementEnvelope {
	t.Helper()
	parts := strings.Split(v.Payload.StatementID, ":")
	if len(parts) != 3 {
		t.Fatalf("vector %s statement id %q", v.Name, v.Payload.StatementID)
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: parts[0], ClientSeq: seq, ClientNonce: parts[2]},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             "irrelevant-for-jws-vector",
		SQLHash:         v.Payload.SQLHash,
		SettingsHash:    v.Payload.SettingsHash,
		PayloadHash:     v.Payload.PayloadHash,
		PayloadLength:   v.Payload.PayloadLength,
		TargetTableID:   v.Payload.TargetTableID,
		UserJWS:         v.Token,
		EnvelopeVersion: 2,
		NetworkID:       v.Payload.NetworkID,
		KeeperShardID:   v.Payload.KeeperShardID,
		PayloadFormat:   v.Payload.PayloadFormat,
		ClientRevision:  v.Payload.ClientRevision,
		SchemaHash:      v.Payload.SchemaHash,
		RowIDProfileID:  v.Payload.RowIDProfileID,
	}
}

func TestVerifyUserJWSV2_SharedVectors(t *testing.T) {
	raw, err := os.ReadFile("testdata/statement_jws_v2.json")
	if err != nil {
		t.Fatalf("read vectors (copy from housegate pkg/auth/testdata): %v", err)
	}
	var file statementV2VectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) < 16 {
		t.Fatalf("expected >= 16 vectors, got %d", len(file.Vectors))
	}
	for _, vec := range file.Vectors {
		t.Run(vec.Name, func(t *testing.T) {
			err := verifyUserJWSV2(envelopeFromVector(t, vec))
			switch vec.Expect {
			case "accept":
				if err != nil {
					t.Fatalf("expected accept, got %v", err)
				}
			case "reject":
				if err == nil {
					t.Fatal("expected reject")
				}
			default:
				t.Fatalf("unknown expect %q", vec.Expect)
			}
		})
	}
}

func TestVerifyUserJWSV2_AccountCaseInsensitive(t *testing.T) {
	raw, _ := os.ReadFile("testdata/statement_jws_v2.json")
	var file statementV2VectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	env := envelopeFromVector(t, file.Vectors[0])
	env.StatementID.ClientAccount = strings.ToUpper(env.StatementID.ClientAccount)
	if err := verifyUserJWSV2(env); err != nil {
		t.Fatalf("admission lowercases client_account before binding: %v", err)
	}
}
```

Rewrite the helpers at the top of `fsm/admission_test.go`:

```go
// signStatementV2 builds the envelope-v2 statement token exactly as the
// agent-side housegate does (auth.RelaySigner.SignStatementV2), with a fixed
// iat — Apply never reads the clock.
func signStatementV2(t *testing.T, key *ecdsa.PrivateKey, env arbiter.StatementEnvelope) string {
	t.Helper()
	payload := auth.JWSStatementPayloadV2{
		Purpose: auth.StatementPurposeV2, Iat: 1700000000,
		NetworkID: env.NetworkID, KeeperShardID: env.KeeperShardID,
		StatementID:    strings.ToLower(env.StatementID.ClientAccount) + ":" + strconv.FormatUint(env.StatementID.ClientSeq, 10) + ":" + env.StatementID.ClientNonce,
		SQLHash:        env.SQLHash,
		SettingsHash:   env.SettingsHash,
		SchemaHash:     env.SchemaHash,
		PayloadHash:    env.PayloadHash,
		PayloadLength:  env.PayloadLength,
		PayloadFormat:  env.PayloadFormat,
		ClientRevision: env.ClientRevision,
		TargetTableID:  env.TargetTableID,
		RowIDProfileID: env.RowIDProfileID,
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256K","typ":"JWT"}`))
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	signingInput := header + "." + base64.RawURLEncoding.EncodeToString(body)
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), key)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}

const testEmptySettingsHash = "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006"

func validEnvelope(t *testing.T, key *ecdsa.PrivateKey, account string, seq uint64) arbiter.StatementEnvelope {
	t.Helper()
	sql := fmt.Sprintf("INSERT INTO db.t FORMAT Native -- %d", seq)
	payload := []byte{2, 0, byte(seq)}
	env := arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: account, ClientSeq: seq, ClientNonce: "n"},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             sql,
		SQLHash:         replay.DigestString(sql),
		SettingsHash:    testEmptySettingsHash,
		PayloadRef:      "ref",
		PayloadHash:     replay.DigestBytes(payload),
		PayloadLength:   uint64(len(payload)),
		TargetTableID:   "db.t",
		EnvelopeVersion: 2,
		NetworkID:       testParams().NetworkID,
		KeeperShardID:   0,
		PayloadFormat:   "clickhouse-native-data-v1",
		ClientRevision:  54460,
		SchemaHash:      "0x" + strings.Repeat("33", 32),
		RowIDProfileID:  "housegate-row-id-v1",
	}
	env.UserJWS = signStatementV2(t, key, env)
	return env
}
```

(imports: add `encoding/json`, `strconv`, `github.com/housegate/housegate/pkg/auth`; delete `signUserJWS` and its `hex` usage if now unused.) In `TestAdmission_Codes` replace the three "invalid signature" cases with:

```go
	// invalid signature: signed by another key
	e = base(2)
	e.UserJWS = signStatementV2(t, otherKey, e)
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("wrong signer: %+v", r)
	}
	// invalid signature: token binds a different payload_hash (ingress swapped the payload after signing)
	e = base(2)
	signed := e
	signed.PayloadHash = replay.DigestBytes([]byte("other"))
	e.UserJWS = signStatementV2(t, key, signed)
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("payload swap: %+v", r)
	}
	// invalid signature: legacy v1 query token
	e = base(2)
	e.UserJWS = signUserJWSLegacy(t, key, e.SQL)
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("legacy token: %+v", r)
	}
	// invalid signature: not a JWS
	e = base(2)
	e.UserJWS = "garbage"
	if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("garbage jws: %+v", r)
	}
```

(keep the old `signUserJWS` renamed to `signUserJWSLegacy` for that case), and add a new test:

```go
func TestAdmission_V2Invariants(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	cases := []struct {
		name   string
		mutate func(*arbiter.StatementEnvelope)
	}{
		{"envelope_version 1", func(e *arbiter.StatementEnvelope) { e.EnvelopeVersion = 1 }},
		{"foreign network", func(e *arbiter.StatementEnvelope) { e.NetworkID = "other-net" }},
		{"non-zero shard", func(e *arbiter.StatementEnvelope) { e.KeeperShardID = 1 }},
		{"csv format", func(e *arbiter.StatementEnvelope) { e.PayloadFormat = "csv-with-names-v1" }},
		{"wrong row profile", func(e *arbiter.StatementEnvelope) { e.RowIDProfileID = "housegate-row-id-v0" }},
		{"non-empty settings", func(e *arbiter.StatementEnvelope) { e.SettingsHash = replay.DigestString("x") }},
		{"missing schema hash", func(e *arbiter.StatementEnvelope) { e.SchemaHash = "" }},
		{"zero revision", func(e *arbiter.StatementEnvelope) { e.ClientRevision = 0 }},
	}
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := validEnvelope(t, key, account, uint64(10+i))
			tc.mutate(&e)
			e.UserJWS = signStatementV2(t, key, e) // re-sign so ONLY the invariant fails, not the binding
			if r := submit(t, f, e); r.Code != arbiter.AdmissionCodeMalformed {
				t.Fatalf("%s: code=%v msg=%q, want MALFORMED", tc.name, r.Code, r.Message)
			}
		})
	}
}
```

Edit `fsm/fsm_test.go` `testParams()` to include `NetworkID: "testnet-fsm"`.

- [ ] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test --test_output=errors`
Expected: build error `undefined: verifyUserJWSV2` / `unknown field NetworkID in Params`.

- [ ] **Step 3: Implement**

`fsm/state.go` `Params`:

```go
type Params struct {
	SchemaSnapshotID   string   `json:"schema_snapshot_id"`
	ExecutorProfileID  string   `json:"executor_profile_id"`
	AuthorityAddresses []string `json:"authority_addresses,omitempty"`
	// NetworkID is the genesis network id every envelope-v2 statement must
	// carry (and sign). Consensus parameter: identical on every node.
	NetworkID string `json:"network_id"`
}
```

`fsm/userjws.go` — append (keep `verifyUserJWS` for now; delete it if nothing else references it after this task):

```go
// Envelope v2 admission constants (spec 2026-08-18 signed envelope v2 §7).
// Duplicated as literals so fsm never imports arbiter-proto or the
// housegate storageintegrity package; the housegate/arbiter-core tests pin
// the same values.
const (
	envelopeVersionV2   = 2
	payloadFormatNative = "clickhouse-native-data-v1"
	rowIDProfileV1      = "housegate-row-id-v1"
	emptySettingsHash   = "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006"
)

// verifyUserJWSV2 re-verifies the envelope-v2 statement token
// deterministically: compact JWS, ES256K, purpose housegate-statement-v2,
// EVERY bound payload field equal to the envelope-derived expectation
// (statement_id flat form with lowercased account), and secp256k1 recovery
// == client_account. No iat/expiry check — Apply is wall-clock-free; the
// ingress edge enforces freshness.
func verifyUserJWSV2(env arbiter.StatementEnvelope) error {
	parts := strings.Split(env.UserJWS, ".")
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
	var payload auth.JWSStatementPayloadV2
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return fmt.Errorf("user_jws payload: %w", err)
	}
	acct := strings.ToLower(env.StatementID.ClientAccount)
	want := auth.JWSStatementPayloadV2{
		Purpose:        auth.StatementPurposeV2,
		NetworkID:      env.NetworkID,
		KeeperShardID:  env.KeeperShardID,
		StatementID:    arbiter.StatementID{ClientAccount: acct, ClientSeq: env.StatementID.ClientSeq, ClientNonce: env.StatementID.ClientNonce}.Flat(),
		SQLHash:        env.SQLHash,
		SettingsHash:   env.SettingsHash,
		SchemaHash:     env.SchemaHash,
		PayloadHash:    env.PayloadHash,
		PayloadLength:  env.PayloadLength,
		PayloadFormat:  env.PayloadFormat,
		ClientRevision: env.ClientRevision,
		TargetTableID:  env.TargetTableID,
		RowIDProfileID: env.RowIDProfileID,
	}
	if field := auth.StatementPayloadV2Mismatch(payload, want); field != "" {
		return fmt.Errorf("user_jws: %s does not bind the envelope", field)
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
	if addr := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()); addr != acct {
		return fmt.Errorf("user_jws: recovered %s does not match client_account", addr)
	}
	return nil
}
```

Add import `"github.com/housegate/housegate/pkg/auth"` and `"github.com/sentioxyz/arbiter-core"` to `fsm/userjws.go`.

`fsm/admission.go` — between step 1 (`sql_hash does not bind sql`) and step 2 insert:

```go
	// 1b. envelope v2 invariants (deterministic, wall-clock-free)
	switch {
	case env.EnvelopeVersion != envelopeVersionV2:
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: fmt.Sprintf("envelope_version %d is not %d", env.EnvelopeVersion, envelopeVersionV2)}
	case env.NetworkID != f.st.Params.NetworkID:
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "network_id does not match this network"}
	case env.KeeperShardID != 0:
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "keeper_shard_id must be 0 in v1"}
	case env.PayloadFormat != payloadFormatNative:
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "payload_format is not clickhouse-native-data-v1"}
	case env.RowIDProfileID != rowIDProfileV1:
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "row_id_profile_id is not housegate-row-id-v1"}
	case env.SettingsHash != emptySettingsHash:
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "settings_hash must be the empty-settings digest in v1"}
	case env.SchemaHash == "":
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "schema_hash is required"}
	case env.ClientRevision == 0:
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "client_revision is required"}
	}
```

and replace step 3 with:

```go
	// 3. signature, wall-clock-free — binds every v2 field, not just sql
	acct := strings.ToLower(id.ClientAccount)
	if err := verifyUserJWSV2(env); err != nil {
		return SubmitResult{Code: arbiter.AdmissionCodeInvalidSignature, Message: err.Error()}
	}
```

(add `"fmt"` import if missing). `config/config.go`: add `NetworkID string \`yaml:"network_id"\`` to `GenesisConfig` and `req(c.Genesis.NetworkID, "genesis.network_id")` next to the other genesis requirements. `cmd/arbiter/services.go:40-44`: add `NetworkID: cfg.Genesis.NetworkID,` to the `fsm.Params{...}` literal. `integration/chpipeline/cluster_test.go`: add `NetworkID: testNetworkID` to the `config.GenesisConfig{...}` literal (~:132) and `NetworkID: cfg.Genesis.NetworkID` to the `fsm.Params{...}` literal in `startNode` (~:153). Update every `configs/*.yaml` example that has a `genesis:` block with `network_id: <value>` and `config/config_test.go` fixtures accordingly (grep `schema_snapshot_id`).

- [ ] **Step 4: Run tests + tripwires**

Run: `bazel test //fsm:fsm_test //config:config_test //server:server_test --test_output=errors && ! (bazel query 'deps(//fsm:fsm, 1)' | grep -q com_github_sentioxyz_arbiter_proto) && ! grep -rn 'time\.Now' fsm/`
Expected: PASS; both tripwire commands succeed (no output).

- [ ] **Step 5: Commit**

```bash
git add fsm config cmd integration/chpipeline/cluster_test.go configs
git commit -m "feat(fsm): envelope v2 admission — invariants, verifyUserJWSV2 shared vectors, genesis network_id"
```

### Task 29: `L3BlockHeader.StatementsRoot`, snapshot v2, `L3BlockView`, replay statements carry v2 fields

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `fsm/state.go:110-130`, `fsm/apply.go:108-165`, `fsm/snapshot.go:21,94`, `fsm/reads_dispatch.go:48-59`, `fsm/reads.go` (+`L3BlockView`)
- Test: `fsm/seal_test.go` (append), `fsm/snapshot_test.go` (append), `fsm/reads_dispatch_test.go` (append)

**Interfaces:**
- Produces: `L3BlockHeader.StatementsRoot string \`json:"statements_root"\`` (included in `ChainHash`), `applySealL3Block` computes it as `replay.CanonicalDigest(arbiter.DomainL3Statements, []arbiter.StatementEnvelope)` in statement_seq order; `snapshotVersion = 2` (v1 refused: `unsupported snapshot version 1`); `(*FSM).L3BlockView(seq uint64) (header L3BlockHeader, chainHash string, envelopes []arbiter.StatementEnvelope, ok bool)`; `BlockDispatchInfo.Statements[i].{PayloadFormat, ClientRevision, SchemaHash}` populated.

- [ ] **Step 1: Write the failing tests**

Append to `fsm/seal_test.go`:

```go
func TestSealL3Block_StatementsRootCommitsEnvelopes(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	sealBlock(t, f, 2)
	h := f.st.Blocks[0]
	if h.StatementsRoot == "" {
		t.Fatal("statements_root must be stamped at seal")
	}
	envs := []arbiter.StatementEnvelope{f.st.Statements[1].Env, f.st.Statements[2].Env}
	want, err := replay.CanonicalDigest(arbiter.DomainL3Statements, envs)
	if err != nil {
		t.Fatal(err)
	}
	if h.StatementsRoot != want {
		t.Fatalf("statements_root = %s, want %s (envelopes in statement_seq order)", h.StatementsRoot, want)
	}
	// ChainHash must include statements_root: mutating it changes the hash.
	base, _ := h.ChainHash()
	mutated := h
	mutated.StatementsRoot = replay.DigestString("evil")
	got, _ := mutated.ChainHash()
	if got == base {
		t.Fatal("ChainHash must commit to statements_root")
	}
	// The anchored copy still hashes identically (anchor excluded).
	withAnchor := h
	withAnchor.L2AnchorRef = &arbiter.AnchorRef{L3BlockHash: "0xaa"}
	if again, _ := withAnchor.ChainHash(); again != base {
		t.Fatal("ChainHash must still exclude the back-filled anchor")
	}
	// L3BlockView exposes header + envelopes for auditors.
	hdr, chainHash, got2, ok := f.L3BlockView(1)
	if !ok || hdr.L3BlockSeq != 1 || chainHash != base || len(got2) != 2 || got2[0] != envs[0] || got2[1] != envs[1] {
		t.Fatalf("L3BlockView(1) = %+v %s %d ok=%v", hdr, chainHash, len(got2), ok)
	}
	if _, _, _, ok := f.L3BlockView(9); ok {
		t.Fatal("unknown block must not resolve")
	}
}

// Golden vector: a fixed header must hash to a fixed value so any future
// change to the header layout is a deliberate, versioned decision.
func TestL3BlockHeader_ChainHashGoldenV2(t *testing.T) {
	h := L3BlockHeader{
		L3BlockSeq: 1, PrevL3Hash: "", StatementSeqStart: 1, StatementCount: 2,
		SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0",
		SpentIDsRootAfter: "0x00", StatementsRoot: "0x11",
	}
	got, err := h.ChainHash()
	if err != nil {
		t.Fatal(err)
	}
	// Recompute independently through the canonical profile so the golden is
	// derived, not hard-coded, yet still catches accidental field drift.
	want, err := replay.CanonicalDigest(arbiter.DomainL3Header, map[string]any{
		"l3_block_seq": 1, "prev_l3_hash": "", "statement_seq_start": 1, "statement_count": 2,
		"schema_snapshot_id": "schema-genesis", "executor_profile_id": "housegate-replay-mvp-v0",
		"spent_ids_root_after": "0x00", "statements_root": "0x11",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("ChainHash golden drift: got %s want %s (json field set/order changed?)", got, want)
	}
}
```

(The map-based recomputation works because `encoding/json` sorts map keys and the struct's json field order — `l3_block_seq, prev_l3_hash, statement_seq_start, statement_count, schema_snapshot_id, executor_profile_id, spent_ids_root_after, statements_root` with `prev_safe_snapshot_id`/`prev_state_root`/`l2_anchor_ref` omitted when empty — must equal the sorted-key order for the golden to hold. If it does not, replace `want` with the literal hex the first run prints and keep it as the golden.)

Append to `fsm/snapshot_test.go`:

```go
func TestSnapshotVersion2_RefusesV1Container(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	sealBlock(t, f, 1)
	data := snapshotBytes(t, f)
	if data[4] != 2 {
		t.Fatalf("snapshot version byte = %d, want 2", data[4])
	}
	g := restoreInto(t, data)
	if !reflect.DeepEqual(g.st.Blocks, f.st.Blocks) {
		t.Fatal("blocks (with statements_root) must round-trip")
	}
	if g.st.Params.NetworkID != f.st.Params.NetworkID || g.st.Params.NetworkID == "" {
		t.Fatal("Params.NetworkID must travel in the snapshot")
	}
	v1 := append([]byte(nil), data...)
	v1[4] = 1
	if err := New(Params{}).Restore(io.NopCloser(bytes.NewReader(v1))); err == nil || !strings.Contains(err.Error(), "unsupported snapshot version 1") {
		t.Fatalf("v1 snapshot must be refused with a clear error, got %v", err)
	}
}
```

Append to `fsm/reads_dispatch_test.go`:

```go
func TestBlockDispatchInfo_StatementsCarryV2Fields(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	sealBlock(t, f, 1)
	info, ok := f.BlockDispatchInfo(1)
	if !ok || len(info.Statements) != 1 {
		t.Fatalf("dispatch info: ok=%v n=%d", ok, len(info.Statements))
	}
	st := info.Statements[0]
	env := f.st.Statements[1].Env
	if st.PayloadFormat != env.PayloadFormat || st.ClientRevision != env.ClientRevision || st.SchemaHash != env.SchemaHash || st.PayloadFormat == "" {
		t.Fatalf("replay statement must carry payload_format/client_revision/schema_hash: %+v", st)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `bazel test //fsm:fsm_test --test_filter='TestSealL3Block_StatementsRoot|TestL3BlockHeader_ChainHashGoldenV2|TestSnapshotVersion2|TestBlockDispatchInfo_StatementsCarryV2Fields' --test_output=errors`
Expected: build error `unknown field StatementsRoot` / `undefined: L3BlockView`.

- [ ] **Step 3: Implement**

`fsm/state.go` — add to `L3BlockHeader` after `SpentIDsRootAfter`:

```go
	// StatementsRoot = CanonicalDigest(DomainL3Statements, []StatementEnvelope)
	// over the sealed envelopes in statement_seq order (envelope v2 §4.4): the
	// anchored chain hash pins WHAT was sequenced, not just the seq range.
	StatementsRoot string `json:"statements_root"`
```

`fsm/apply.go` `applySealL3Block` — before `hdr := L3BlockHeader{`, collect the envelopes and hash them:

```go
	envs := make([]arbiter.StatementEnvelope, 0, len(ob.StatementSeqs))
	for _, seq := range ob.StatementSeqs {
		envs = append(envs, f.st.Statements[seq].Env)
	}
	statementsRoot, err := replay.CanonicalDigest(arbiter.DomainL3Statements, envs)
	if err != nil {
		return Rejected{Reason: "statements root: " + err.Error()}
	}
```

and set `StatementsRoot: statementsRoot,` in the header literal. (`ob.StatementSeqs` are appended in admission order == statement_seq order.)

`fsm/snapshot.go`: `snapshotVersion = 2`. The `Restore` check already produces `unsupported snapshot version %d`.

`fsm/reads_dispatch.go`: add `PayloadFormat: ss.Env.PayloadFormat, ClientRevision: ss.Env.ClientRevision, SchemaHash: ss.Env.SchemaHash,` to the `replay.Statement{...}` literal.

`fsm/reads.go` — append:

```go
// L3BlockView returns a detached copy of one sealed block's header, its
// chain hash, and its envelopes in statement_seq order — the material an
// auditor needs to recompute statements_root and ChainHash (SafeState.GetL3Block).
func (f *FSM) L3BlockView(seq uint64) (L3BlockHeader, string, []arbiter.StatementEnvelope, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	idx := int(seq) - 1
	if idx < 0 || idx >= len(f.st.Blocks) {
		return L3BlockHeader{}, "", nil, false
	}
	header := f.st.Blocks[idx]
	chainHash, err := header.ChainHash()
	if err != nil {
		return L3BlockHeader{}, "", nil, false
	}
	header.L2AnchorRef = cloneAnchorRef(header.L2AnchorRef)
	envs := make([]arbiter.StatementEnvelope, 0, header.StatementCount)
	for s := header.StatementSeqStart; s < header.StatementSeqStart+uint64(header.StatementCount); s++ {
		if ss := f.st.Statements[s]; ss != nil {
			envs = append(envs, ss.Env)
		}
	}
	return header, chainHash, envs, true
}
```

- [ ] **Step 4: Run tests**

Run: `bazel test //fsm:fsm_test --test_output=errors && ! grep -rn 'time\.Now' fsm/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fsm
git commit -m "feat(fsm): statements_root in the L3 chain hash, snapshot v2, L3BlockView, v2 replay statement fields"
```

### Task 30: `SafeState.GetL3Block` RPC

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `server/safestate.go`
- Test: `server/safestate_l3block_test.go` (new)

**Interfaces:**
- Consumes: `pb.L3BlockRef`, `pb.L3Block`, `pb.L3BlockHeader` (Task 1), `wire.EnvelopeToPB`, `wire.AnchorRefToPB`, `fsm.L3BlockView` (Task 29).
- Produces: `safeStateService.GetL3Block(ctx, *pb.L3BlockRef) (*pb.L3Block, error)` (`codes.NotFound` for unknown seq); `l3HeaderToPB(fsm.L3BlockHeader) *pb.L3BlockHeader`.

- [ ] **Step 1: Write the failing tests**

Create `server/safestate_l3block_test.go` (mirror the fixture style of the existing `server/*_test.go` — the FSM under test is built with `fsm.New(fsm.Params{...})` and driven with `wire.Command`s; if a shared server-test harness exists (grep `newTestServer` / `safeStateService{s:` in `server/*_test.go`), use it):

```go
package server

import (
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter/fsm"
)

// TestL3BlockHeaderMirrorsProto pins fsm.L3BlockHeader json tags to
// pb.L3BlockHeader field names, the same freeze discipline arbiter-core's
// conformance package applies to its mirrors.
func TestL3BlockHeaderMirrorsProto(t *testing.T) {
	goTags := map[string]bool{}
	rt := reflect.TypeOf(fsm.L3BlockHeader{})
	for i := 0; i < rt.NumField(); i++ {
		goTags[strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	var msg proto.Message = &pb.L3BlockHeader{}
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if !goTags[name] {
			t.Errorf("proto field %q has no fsm.L3BlockHeader json tag", name)
		}
		delete(goTags, name)
	}
	for tag := range goTags {
		t.Errorf("fsm.L3BlockHeader json tag %q has no proto field", tag)
	}
}

func TestGetL3Block_ReturnsHeaderChainHashAndEnvelopes(t *testing.T) {
	h := newSafeStateTestHarness(t) // existing/added helper: FSM with one sealed block of 2 statements + safeStateService
	got, err := h.svc.GetL3Block(h.ctx, &pb.L3BlockRef{L3BlockSeq: 1})
	if err != nil {
		t.Fatalf("GetL3Block: %v", err)
	}
	hdr, chainHash, envs, ok := h.state.L3BlockView(1)
	if !ok {
		t.Fatal("fsm has no block 1")
	}
	if got.GetChainHash() != chainHash || got.GetHeader().GetStatementsRoot() != hdr.StatementsRoot || got.GetHeader().GetL3BlockSeq() != 1 || len(got.GetStatements()) != len(envs) {
		t.Fatalf("GetL3Block = %+v", got)
	}
	if got.GetStatements()[0].GetEnvelopeVersion() != 2 || got.GetStatements()[0].GetUserJws() != envs[0].UserJWS {
		t.Fatalf("envelopes must be the sequenced v2 envelopes: %+v", got.GetStatements()[0])
	}
	if _, err := h.svc.GetL3Block(h.ctx, &pb.L3BlockRef{L3BlockSeq: 42}); err == nil {
		t.Fatal("unknown block must be NotFound")
	}
}
```

Write `newSafeStateTestHarness` in the same file by copying how the existing safestate/ingress server tests construct `*Server` and `fsm.FSM` (grep `safeStateService{s:` and `fsm.New(` in `server/*_test.go`), sealing one block through `wire.Command{SubmitStatement: ...}` ×2 + `wire.Command{SealL3Block: ...}` (envelopes built like `fsm/admission_test.go`'s `validEnvelope` — copy that helper into the server test package as `serverValidEnvelope`).

- [ ] **Step 2: Run to verify failure**

Run: `bazel run //:gazelle && bazel test //server:server_test --test_filter='TestL3BlockHeaderMirrorsProto|TestGetL3Block' --test_output=errors`
Expected: build error `svc.GetL3Block undefined`.

- [ ] **Step 3: Implement**

Append to `server/safestate.go`:

```go
// GetL3Block serves a sealed block's header, chain hash and envelopes so an
// auditor can recompute statements_root and ChainHash from the same canonical
// forms the FSM hashed (envelope v2 §4.4; consumed by the multi-replica
// safe-set spec's lag replay too).
func (svc *safeStateService) GetL3Block(_ context.Context, ref *pb.L3BlockRef) (*pb.L3Block, error) {
	hdr, chainHash, envs, ok := svc.s.d.FSM.L3BlockView(ref.GetL3BlockSeq())
	if !ok {
		return nil, status.Error(codes.NotFound, "no sealed block at that seq")
	}
	out := &pb.L3Block{Header: l3HeaderToPB(hdr), ChainHash: chainHash}
	for _, env := range envs {
		out.Statements = append(out.Statements, wire.EnvelopeToPB(env))
	}
	return out, nil
}

func l3HeaderToPB(h fsm.L3BlockHeader) *pb.L3BlockHeader {
	out := &pb.L3BlockHeader{
		L3BlockSeq:         h.L3BlockSeq,
		PrevL3Hash:         h.PrevL3Hash,
		StatementSeqStart:  h.StatementSeqStart,
		StatementCount:     h.StatementCount,
		SchemaSnapshotId:   h.SchemaSnapshotID,
		ExecutorProfileId:  h.ExecutorProfileID,
		PrevSafeSnapshotId: h.PrevSafeSnapshotID,
		PrevStateRoot:      h.PrevStateRoot,
		SpentIdsRootAfter:  h.SpentIDsRootAfter,
		StatementsRoot:     h.StatementsRoot,
	}
	if h.L2AnchorRef != nil {
		out.L2AnchorRef = wire.AnchorRefToPB(*h.L2AnchorRef)
	}
	return out
}
```

(imports: `github.com/sentioxyz/arbiter-core/wire`, `github.com/sentioxyz/arbiter/fsm` if not already present.) `pb.RegisterSafeStateServer` in `server/server.go` picks the new method up through the interface — the build fails until it exists, which is the point.

- [ ] **Step 4: Run tests**

Run: `bazel test //server:server_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server
git commit -m "feat(server): SafeState.GetL3Block — header, chain hash and sequenced envelopes for auditors"
```

### Task 31: chpipeline — Native payloads, v2 envelopes, and the fourth fraud class

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

**Files:**
- Modify: `integration/chpipeline/harness_ops_test.go` (`statement`, `signUserJWSAt` → v2), `integration/chpipeline/harness_fraud_test.go:32-45` (decode Native), `integration/chpipeline/staged_test.go:21-22`, `integration/chpipeline/fraud_test.go` (+fourth class)
- Create: `integration/chpipeline/native_payload_test.go` (encoder helper)

**Interfaces:**
- Consumes: `nativepayload.Decode`, `payloadexec.TableSchemaHash`, `payloadexec.RowIDProfileID`, `auth.JWSStatementPayloadV2`/`auth.StatementPurposeV2`; harness fields `h.key`, `h.account`, `h.schema`.

- [ ] **Step 1: Write the fourth fraud class (failing) and adapt the harness**

Create `integration/chpipeline/native_payload_test.go`:

```go
package chpipeline

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

const pipelineClientRevision = 54460

// nativeRows encodes one Native ClientData packet (p String, v UInt64) —
// the exact bytes an agent-side housegate signs and the ingress stores.
func nativeRows(t *testing.T, partition string, v uint64) []byte {
	t.Helper()
	p := proto.ColStr{}
	p.Append(partition)
	vals := proto.ColUInt64{v}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: 1, Columns: 2}).EncodeBlock(&buf, pipelineClientRevision, proto.Input{{Name: "p", Data: &p}, {Name: "v", Data: &vals}}); err != nil {
		t.Fatalf("encode native rows: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}
```

Rewrite `harness_ops_test.go` `statement` + `signUserJWSAt`:

```go
func (h *harness) statement(t *testing.T, seq uint64, partition string, v uint64) (arbiter.StatementEnvelope, []byte) {
	t.Helper()
	payload := nativeRows(t, partition, v)
	sql := "INSERT INTO db.t FORMAT Native"
	payloadRef := h.mintPayloadRef(t, fmt.Sprintf("payload-%d.native", seq), payload)
	env := arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: h.account, ClientSeq: seq, ClientNonce: fmt.Sprintf("n%d", seq)},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             sql,
		SQLHash:         replay.DigestString(sql),
		SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
		PayloadRef:      payloadRef,
		PayloadHash:     replay.DigestBytes(payload),
		PayloadLength:   uint64(len(payload)),
		TargetTableID:   testTableID,
		EnvelopeVersion: 2,
		NetworkID:       testNetworkID,
		KeeperShardID:   0,
		PayloadFormat:   "clickhouse-native-data-v1",
		ClientRevision:  pipelineClientRevision,
		SchemaHash:      payloadexec.TableSchemaHash(testNetworkID, h.schema),
		RowIDProfileID:  payloadexec.RowIDProfileID,
	}
	env.UserJWS = signStatementV2At(h.key, env, time.Now().Unix())
	return env, payload
}

// signStatementV2At is the agent-side envelope-v2 signer (housegate
// auth.RelaySigner.SignStatementV2) reproduced with an explicit iat.
func signStatementV2At(key *ecdsa.PrivateKey, env arbiter.StatementEnvelope, iat int64) string {
	payload := auth.JWSStatementPayloadV2{
		Purpose: auth.StatementPurposeV2, Iat: iat,
		NetworkID: env.NetworkID, KeeperShardID: env.KeeperShardID,
		StatementID:    arbiter.StatementID{ClientAccount: strings.ToLower(env.StatementID.ClientAccount), ClientSeq: env.StatementID.ClientSeq, ClientNonce: env.StatementID.ClientNonce}.Flat(),
		SQLHash:        env.SQLHash,
		SettingsHash:   env.SettingsHash,
		SchemaHash:     env.SchemaHash,
		PayloadHash:    env.PayloadHash,
		PayloadLength:  env.PayloadLength,
		PayloadFormat:  env.PayloadFormat,
		ClientRevision: env.ClientRevision,
		TargetTableID:  env.TargetTableID,
		RowIDProfileID: env.RowIDProfileID,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256K","typ":"JWT"}`))
	signingInput := header + "." + base64.RawURLEncoding.EncodeToString(body)
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), key)
	if err != nil {
		panic(fmt.Sprintf("sign statement v2: %v", err))
	}
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig)
}
```

(imports: `encoding/json`, `github.com/housegate/housegate/pkg/auth`, `github.com/housegate/housegate/pkg/replay/payloadexec`; delete `signUserJWSAt` and its unused imports.) In `harness_fraud_test.go` `submitManualSource` replace `payloadexec.DecodeCSV(payload, h.schema)` with `nativepayload.Decode(h.schema, pipelineClientRevision, payload)` (import `github.com/housegate/housegate/pkg/replay/nativepayload`). In `staged_test.go` set `PayloadEncoding: "clickhouse-native-data-v1", Revision: pipelineClientRevision`.

Append to `fraud_test.go`:

```go
// Fourth fraud class (envelope v2 spec §9): an ingress that swaps the payload
// AFTER the user signed. The stored bytes and the envelope's payload_hash /
// payload_length are internally consistent, but they no longer match what the
// user_jws binds — admission must reject with INVALID_SIGNATURE before the
// statement is ever sequenced.
func TestPipeline_FraudIngressSwapsPayloadAfterSigning(t *testing.T) {
	conn := requireCH(t)
	h := startHarness(t, conn)

	env, _ := h.statement(t, 1, "p0", 501) // signed over the honest payload
	swapped := nativeRows(t, "p0", 999)   // what a dishonest ingress stores instead
	env.PayloadRef = h.mintPayloadRef(t, "payload-1-swapped.native", swapped)
	env.PayloadHash = replay.DigestBytes(swapped)
	env.PayloadLength = uint64(len(swapped))
	// UserJWS deliberately NOT re-signed.

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
		t.Fatalf("swapped payload must be INVALID_SIGNATURE, got %+v", ack)
	}
	if !strings.Contains(ack.GetMessage(), "payload_hash") {
		t.Fatalf("rejection should name payload_hash, got %q", ack.GetMessage())
	}
	statusView := h.statementStatus(t, env.StatementID.Flat())
	if statusView.GetFound() {
		t.Fatal("a rejected statement must not be sequenced")
	}
	// The honest chain is untouched: nothing was sealed, watermark still 0.
	if wm, err := h.leaderWatermark(); err != nil || wm.GetSafeBlockSeq() != 0 {
		t.Fatalf("watermark after rejected fraud = %v err=%v", wm, err)
	}
}
```

(imports in fraud_test.go: `context`, `strings`, `google.golang.org/grpc`, `pb`, `github.com/sentioxyz/arbiter-core/wire`.)

- [ ] **Step 2: Run the docker suite**

Run (ClickHouse on :9000 as in CI): `bazel run //:gazelle && ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 bazel test //integration/chpipeline:chpipeline_test --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=1800 --test_output=errors`
Expected: PASS — the honest, three v1 fraud classes and the fourth class all green; before Task 28's admission changes the fourth class would have been ACCEPTED (that is the property this spec closes).

- [ ] **Step 3: Full arbiter gate + commit**

Run: `bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors && ./scripts/anchor-bindings.sh check`
Expected: PASS.

```bash
git add integration/chpipeline
git commit -m "test(chpipeline): native payloads, envelope v2, and the ingress-payload-swap fraud class"
```

### Task 32: Cut arbiter release

Working directory: `/Users/uranuswch/Dev/sentio_xyz/arbiter`

- [ ] **Step 1: Merge to main; run the arbiter release workflow / tag per the repo's calendar scheme** (`git tag --sort=-v:refname | head -1` shows the last tag; `scripts/next-version.sh` computes the next). Record `ARBITER_TAG`.

- [ ] **Step 2: Deployment note** — devnet2 has no v1 snapshots (`snapshotVersion=2` refuses them); confirm with `configs/*.yaml` that `genesis.network_id` is set before rolling.

- [ ] **Step 3: Verify the tag resolves**

Run: `cd /tmp && GOFLAGS=-mod=mod go list -m github.com/sentioxyz/arbiter@$ARBITER_TAG`
Expected: prints the module@tag (sentio-node does not import arbiter — the tag is for the devnet2 rollout in Spec F).

---

## Phase 8 — sentio-node (spec §10 step 8, §8 last bullet)

### Task 33: Point sentio-node at housegate v0.9.0, the new arbiter-core, arbiter-proto v0.5.0

Working directory: `/Users/uranuswch/Dev/sentio_xyz/sentio-node`

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel` (`bazel_dep(name="housegate", version=...)`, `git_override(module_name="housegate", commit=...)`, same pair for `arbiter_core`), `MODULE.bazel.lock`

- [ ] **Step 1: Bump the Go pins**

```bash
go get github.com/housegate/housegate@v0.9.0 \
       github.com/sentioxyz/arbiter-core@$ARBITER_CORE_TAG \
       github.com/sentioxyz/arbiter-proto@v0.5.0
bazel run @rules_go//go -- mod tidy
```

- [ ] **Step 2: Bump the Bzlmod pins by hand (no update script in this repo)**

In `MODULE.bazel` set `bazel_dep(name = "housegate", version = "0.9.0")` and, in the housegate `git_override`, `commit = "<full 40-hex sha of the v0.9.0 tag>"` with the `# Resolved Housegate v0.9.0; …` comment; do the same for `arbiter_core` (`version = "<go version without v>"`, `commit = "$ARBITER_CORE_SHA"`). Resolve the SHAs with:

```bash
GOWORK=off go list -m -f '{{.Version}} {{with .Origin}}{{.Hash}}{{end}}' github.com/housegate/housegate@v0.9.0
GOWORK=off go list -m -f '{{.Version}} {{with .Origin}}{{.Hash}}{{end}}' github.com/sentioxyz/arbiter-core@$ARBITER_CORE_TAG
bazel mod deps --lockfile_mode=update >/dev/null
```

- [ ] **Step 3: Regenerate BUILD files; observe the expected breakage**

Run: `./scripts/update-bazel-deps.sh && bazel build //... 2>&1 | grep -E 'NativeCSVPayloadMaterializer|StorageIntegrityPayloadMaterializer|TableSchemaResolver' | head`
Expected: compile errors in `standalone/standalone.go` (`sicore.NativeCSVPayloadMaterializer undefined`, `unknown field StorageIntegrityPayloadMaterializer`) and `storageintegrityadapter/adapter.go` (`sicore.TableSchemaResolver undefined`) — fixed by Tasks 34–35.

- [ ] **Step 4: Commit (WIP — the tree does not build until Task 35)**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock
git commit -m "chore(deps): housegate v0.9.0, arbiter-core, arbiter-proto v0.5.0 (statement envelope v2)"
```

### Task 34: `storageintegrityadapter` — carry v2 fields, terminal prepare-reject mapping, drop the schema resolver

Working directory: `/Users/uranuswch/Dev/sentio_xyz/sentio-node`

**Files:**
- Modify: `storageintegrityadapter/adapter.go` (`toArbiterEnvelope` ~:80-108, `PrepareLocalStatement` ~:35-48, delete `schemaResolver`/`NewSchemaResolver` ~:214-228)
- Test: `storageintegrityadapter/adapter_test.go` (`validEnvelope` ~:272, `TestPrepareFieldMappingBothDirections`, `TestPrepareErrorClassificationPreservesSentinels`, delete `TestSchemaResolver`)

**Interfaces:**
- Consumes: `sicore.StatementEnvelope` v2 fields (Task 17), `sicore.ErrPrepareTerminalReject` (Task 17 Step 3b), `snode.ErrSchemaHashMismatch` (Task 24), `arbiter.StatementEnvelope` v2 fields (Task 23).
- Produces: `toArbiterEnvelope` maps `EnvelopeVersion/NetworkID/KeeperShardID/PayloadFormat(=PayloadEncoding)/ClientRevision(=Revision)/SchemaHash/RowIDProfileID`; `PrepareLocalStatement` wraps `snode.ErrSchemaHashMismatch` and `snode.ErrEncodingNotSupported` so `errors.Is(err, sicore.ErrPrepareTerminalReject)` AND the original sentinel both hold.

- [ ] **Step 1: Write the failing tests**

Replace `validEnvelope` in `adapter_test.go`:

```go
func validEnvelope(id string) sicore.StatementEnvelope {
	return sicore.StatementEnvelope{
		StatementID: id, StatementKind: sicore.KindInsert,
		SQL: "INSERT", SQLHash: "0xsql", TargetTableID: "t",
		PayloadRef: "ref", PayloadHash: "0xhash", PayloadLength: 12,
		PayloadEncoding: "clickhouse-native-data-v1", Revision: 54460,
		Signer: "0xabc", UserJWS: "jws",
		EnvelopeVersion: 2, NetworkID: "testnet", KeeperShardID: 0,
		SettingsHash: "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
		SchemaHash: "0xschema", RowIDProfileID: "housegate-row-id-v1",
	}
}
```

In `TestPrepareFieldMappingBothDirections` change the fake's `PayloadEncoding` literals to `"clickhouse-native-data-v1"` and add after the existing envelope assertions:

```go
	require.Equal(t, uint32(2), f.lastReq.Envelope.EnvelopeVersion)
	require.Equal(t, "testnet", f.lastReq.Envelope.NetworkID)
	require.Equal(t, uint32(0), f.lastReq.Envelope.KeeperShardID)
	require.Equal(t, "clickhouse-native-data-v1", f.lastReq.Envelope.PayloadFormat)
	require.Equal(t, uint32(54460), f.lastReq.Envelope.ClientRevision)
	require.Equal(t, "0xschema", f.lastReq.Envelope.SchemaHash)
	require.Equal(t, "housegate-row-id-v1", f.lastReq.Envelope.RowIDProfileID)
	require.Equal(t, env.SettingsHash, f.lastReq.Envelope.SettingsHash)
```

Replace `TestPrepareErrorClassificationPreservesSentinels` with:

```go
func TestPrepareErrorClassificationPreservesSentinels(t *testing.T) {
	for _, tc := range []struct {
		injected error
		terminal bool
	}{
		{snode.ErrEncodingNotSupported, true},
		{snode.ErrSchemaHashMismatch, true},
		{snode.ErrPayloadMismatch, false},
		{snode.ErrConvergenceForeignRows, false},
		{errors.New("dial tcp: connection refused"), false},
	} {
		f := &fakeRole{prepErr: tc.injected}
		_, err := NewSourcePreparer(f).PrepareLocalStatement(t.Context(), validEnvelope("0xabc:1:x"), nil)
		require.Error(t, err)
		require.ErrorIs(t, err, tc.injected, "original sentinel must survive")
		require.Equal(t, tc.terminal, errors.Is(err, sicore.ErrPrepareTerminalReject), "terminal classification for %v", tc.injected)
	}
}
```

Delete `TestSchemaResolver`.

- [ ] **Step 2: Run to verify failure**

Run: `bazel test //storageintegrityadapter:storageintegrityadapter_test --test_output=errors`
Expected: build error `sicore.TableSchemaResolver undefined` (the resolver is deleted in Step 3); once that is removed, `TestPrepareFieldMappingBothDirections` FAILS on the v2 field assertions and `TestPrepareErrorClassificationPreservesSentinels` FAILS on the terminal classification.

- [ ] **Step 3: Implement**

`toArbiterEnvelope` returns:

```go
	return arbiter.StatementEnvelope{
		StatementID:     id,
		StatementKind:   kind,
		SQL:             env.SQL,
		SQLHash:         env.SQLHash,
		SettingsHash:    env.SettingsHash,
		PayloadRef:      env.PayloadRef,
		PayloadHash:     env.PayloadHash,
		PayloadLength:   env.PayloadLength,
		TargetTableID:   env.TargetTableID,
		UserJWS:         env.UserJWS,
		EnvelopeVersion: env.EnvelopeVersion,
		NetworkID:       env.NetworkID,
		KeeperShardID:   env.KeeperShardID,
		PayloadFormat:   env.PayloadEncoding,
		ClientRevision:  uint32(env.Revision),
		SchemaHash:      env.SchemaHash,
		RowIDProfileID:  env.RowIDProfileID,
	}, nil
```

`PrepareLocalStatement` — replace the `if err != nil { return sicore.PreparedLocalResult{}, err }` after `p.role.PrepareLocalStatement(...)` with:

```go
	if err != nil {
		if errors.Is(err, snode.ErrSchemaHashMismatch) || errors.Is(err, snode.ErrEncodingNotSupported) {
			// Terminal before any unsafe write: let the orchestrator abort
			// instead of fencing a retry behind a source lookup.
			return sicore.PreparedLocalResult{}, fmt.Errorf("%w: %w", sicore.ErrPrepareTerminalReject, err)
		}
		return sicore.PreparedLocalResult{}, err
	}
```

(add `"errors"` import.) Delete `type schemaResolver`, `NewSchemaResolver`, and `StorageIntegrityTableSchema`.

- [ ] **Step 4: Run tests**

Run: `./scripts/update-bazel-deps.sh && bazel test //storageintegrityadapter:storageintegrityadapter_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add storageintegrityadapter
git commit -m "feat(storageintegrityadapter): carry envelope v2 fields; classify schema_hash mismatch as terminal prepare reject; drop CSV schema resolver"
```

### Task 35: standalone wiring, ingress `network_id`, smoke test on envelope v2

Working directory: `/Users/uranuswch/Dev/sentio_xyz/sentio-node`

**Files:**
- Modify: `standalone/standalone.go:319-345` (drop `siMaterializer`, set ingress `NetworkID`), `config/config.go` (validation of `housegate.storage_integrity.ingress.network_id` vs `storage_integrity.snode.network_id`), `standalone/storage_integrity_smoke_test.go` (v2 statement token, Native SQL)
- Test: `config/config_test.go` (append), `standalone/storage_integrity_smoke_test.go` (fixture test `TestRunStagedInsertWireOrdersExternalMarkerSamplePayloadAndTerminator` extended)

**Interfaces:**
- Consumes: `housegate.Options` without `StorageIntegrityPayloadMaterializer` (Task 19), `Options.StorageIntegrityTableSchemas` (Task 16; not needed — `networkstate.FromStatecore` implements `registry.TableSchemas`), `auth.RelaySigner.SignStatementV2` (Task 4).
- Produces: standalone passes `cfg.Housegate.StorageIntegrity.Ingress.NetworkID` (defaults to `storage_integrity.snode.network_id` when empty; mismatch is a config error); smoke test signs both tokens in-process from `SENTIO_SI_SIGNER_KEY_HEX` and needs `SENTIO_SI_SCHEMA_HASH`.

- [ ] **Step 1: Write the failing tests**

Append to `config/config_test.go` (mirror the file's existing SI config test builder — grep `StorageIntegrity.SNode.NetworkID` for the fixture used by other tests and reuse it as `validSIConfig(t)`):

```go
func TestStorageIntegrityIngressNetworkIDDefaultsToSNodeNetworkID(t *testing.T) {
	c := validSIConfig(t)
	c.Housegate.StorageIntegrity.Ingress.Enabled = true
	c.Housegate.StorageIntegrity.Ingress.NetworkID = ""
	require.NoError(t, c.Validate())
	require.Equal(t, c.StorageIntegrity.SNode.NetworkID, c.Housegate.StorageIntegrity.Ingress.NetworkID)

	c.Housegate.StorageIntegrity.Ingress.NetworkID = "other-net"
	err := c.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "network_id")
}
```

Extend `TestRunStagedInsertWireOrdersExternalMarkerSamplePayloadAndTerminator` (the in-process fixture that already asserts `SQL_x_auth_token`): after `hasSetting(query.Settings, "SQL_x_auth_token", ...)` add an assertion that the Query carries `SQL_x_statement_token` whose payload (decoded with `auth.DecodeStatementV2Payload`) has `PayloadHash == replay.DigestBytes(<the payload bytes the fixture received>)`, `PayloadFormat == "clickhouse-native-data-v1"`, `ClientRevision == uint32(revision)`, `StatementID == request.StatementID`, `SchemaHash == request.SchemaHash`; the fixture must capture the received Data packet raw bytes (it already reads them to check ordering).

- [ ] **Step 2: Run to verify failure**

Run: `bazel test //config:config_test //standalone:standalone_test --test_filter='TestStorageIntegrityIngressNetworkIDDefaultsToSNodeNetworkID|TestRunStagedInsertWireOrdersExternalMarkerSamplePayloadAndTerminator' --test_output=errors`
Expected: FAIL / build errors (`stagedInsertWireRequest` has no `SchemaHash`, standalone still references the removed materializer).

- [ ] **Step 3: Implement**

`config/config.go` — in `Config.Validate` where `c.StorageIntegrity.Validate(...)` runs (~:394), add before it:

```go
	if c.StorageIntegrity.Enabled && c.Housegate.StorageIntegrity.Ingress.Enabled {
		if c.Housegate.StorageIntegrity.Ingress.NetworkID == "" {
			c.Housegate.StorageIntegrity.Ingress.NetworkID = c.StorageIntegrity.SNode.NetworkID
		}
		if c.Housegate.StorageIntegrity.Ingress.NetworkID != c.StorageIntegrity.SNode.NetworkID {
			errs = append(errs, fmt.Errorf("housegate.storage_integrity.ingress.network_id %q must equal storage_integrity.snode.network_id %q", c.Housegate.StorageIntegrity.Ingress.NetworkID, c.StorageIntegrity.SNode.NetworkID))
		}
	}
```

(`Validate` must therefore have a pointer receiver / mutate the value it validates — follow the file's existing pattern for defaults.)

`standalone/standalone.go` — delete the `siMaterializer` variable, the `siMaterializer = sicore.NativeCSVPayloadMaterializer{...}` block, the `StorageIntegrityPayloadMaterializer: siMaterializer,` option, and the `NewSchemaResolver` import usage. The ingress schema source is the injected `netState` (`networkstate.FromStatecore` implements `registry.TableSchemas` — add the compile-time guard `var _ registry.TableSchemas = (*networkstate.FromStatecore)(nil)` to `standalone/networkstate/networkstate.go` if it is not already there).

`standalone/storage_integrity_smoke_test.go`:
- `stagedInsertWireRequest` gains `SignerKeyHex string`, `NetworkID string`, `SchemaHash string`, `TargetTableID string`, drops `QueryJWS`.
- Refactor `writeStagedInsertPayload(conn, revision, partition, value)` into `encodeStagedInsertPayload(revision int, partition string, value uint64) ([]byte, error)` (same body, returns `buf.Buf`) plus a one-line `conn.Write`.
- In `runStagedInsertWire`, after `revision := min(proto.Version, serverHello.Revision)` and before `codec.WriteQuery`, sign both tokens:

```go
	signer, err := auth.NewRelaySigner(request.SignerKeyHex)
	if err != nil {
		return fmt.Errorf("staged insert signer: %w", err)
	}
	payload, err := encodeStagedInsertPayload(revision, request.Partition, request.Value)
	if err != nil {
		return err
	}
	queryJWS, err := signer.SignToken(request.SQL)
	if err != nil {
		return fmt.Errorf("sign query: %w", err)
	}
	statementJWS, err := signer.SignStatementV2(auth.JWSStatementPayloadV2{
		NetworkID:      request.NetworkID,
		KeeperShardID:  0,
		StatementID:    request.StatementID,
		SQLHash:        replay.DigestString(request.SQL),
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     request.SchemaHash,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: uint32(revision),
		TargetTableID:  request.TargetTableID,
		RowIDProfileID: payloadexec.RowIDProfileID,
	})
	if err != nil {
		return fmt.Errorf("sign statement: %w", err)
	}
```

and set `Settings: []chproto.Setting{{Key: "SQL_x_auth_token", Value: queryJWS, Custom: true}, {Key: auth.StatementTokenSettingKey, Value: "'" + statementJWS + "'", Custom: true}}`; replace the later `writeStagedInsertPayload(...)` call with `conn.Write(payload)`.
- In `TestStorageIntegritySmoke` read `SENTIO_SI_SIGNER_KEY_HEX` (required), `SENTIO_SI_SCHEMA_HASH` (required), `SENTIO_SI_TARGET_TABLE_ID` (default `<database>.orders`), pass `NetworkID: cfg.StorageIntegrity.SNode.NetworkID`, and default `SENTIO_SI_INSERT_SQL` to `INSERT INTO orders (partition, value) FORMAT Native` (CSVWithNames in the SQL still works — the wire is Native — but Native is the documented form). Update the smoke's doc comment listing the env vars.
- In `serveStagedInsertFixture` capture the raw payload packet bytes and assert the statement token as described in Step 1 (fixture signer key: `aaaa…aa` × 32; the fixture's `hasSetting("SQL_x_auth_token", "fixture-jws")` check becomes "present and non-empty").

- [ ] **Step 4: Run tests + full build**

Run: `./scripts/update-bazel-deps.sh && bazel build //... && bazel test //config:config_test //standalone:standalone_test //storageintegrityadapter:storageintegrityadapter_test --test_output=errors`
Expected: PASS (`TestStorageIntegritySmoke` / `TestSchemaRegistryPhaseBSmoke` skip without `SENTIO_SI_E2E=1`).

- [ ] **Step 5: Commit**

```bash
git add standalone config
git commit -m "feat(standalone): envelope v2 — drop CSV bridge wiring, ingress network_id, smoke signs the statement token"
```

---

## Phase 9 — Spec B documentation pointers (spec §10 step 9)

### Task 36: Record the v2 outcome for the design-v4 reconciliation

Working directory: `/Users/uranuswch/Dev/housegate/housegate`

**Files:**
- Modify: `docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md` (Status line), `docs/superpowers/specs/2026-08-18-storage-integrity-design-v4-reconciliation.md` (Spec B — add the items below to its checklist; do NOT rewrite the base design here)

- [ ] **Step 1: Flip Spec A's status and list Spec B's inputs**

In `2026-08-18-storage-integrity-signed-envelope-v2-design.md` change `**Status:** Proposed` to `**Status:** Implemented (plan docs/superpowers/plans/2026-08-18-storage-integrity-signed-envelope-v2.md)`.

In `2026-08-18-storage-integrity-design-v4-reconciliation.md` add under its base-design §7 item (create the section if the doc has no such item yet):

```
- Base design §7 signing payload table: replace `schema_snapshot_id` with `schema_hash` (Phase-B `payloadexec.TableSchemaHash`) and add `client_revision`; `payload_format = clickhouse-native-data-v1`; `settings_hash = CanonicalDigest("housegate-settings-v1", [])` (constant `storageintegrity.EmptySettingsHash`); purpose `housegate-statement-v2`, setting `SQL_x_statement_token`; the legacy `SQL_x_auth_token` query JWS stays for the auth plugin. Source: 2026-08-18-storage-integrity-signed-envelope-v2-design.md §4.
- Base design §5.1 / §6.1: the stored payload is byte-identical to the signed Native `ClientData` bytes; the CSV bridge (`NativeCSVPayloadMaterializer`, `StorageIntegrityPayloadMaterializer`) no longer exists; SNode/verifier decode Native via `pkg/replay/nativepayload`.
- Base design §4.1: the agent-mode HouseGate answers the INSERT sample block from network-state schema (Relay deferred-INSERT mode) so it can sign `payload_hash` before forwarding.
- Arbiter design §5: `L3BlockHeader.statements_root` (`arbiter-l3-statements-v1`) is part of `ChainHash`; `SafeState.GetL3Block` exposes header + envelopes; FSM snapshot version 2; genesis `network_id`.
- housegate CLAUDE.md `pkg/storageintegrity` / `pkg/plugins/{storageintegrity,sistatement}` / `storage_integrity.{ingress,runtime,agent}` section (Task 21 wrote the bullets; B consolidates).
```

- [ ] **Step 2: Commit**

```bash
git add docs/superpowers/specs/2026-08-18-storage-integrity-signed-envelope-v2-design.md docs/superpowers/specs/2026-08-18-storage-integrity-design-v4-reconciliation.md
git commit -m "docs: mark signed envelope v2 implemented; hand the base-design updates to Spec B"
```

---

## Self-review

**1. Spec coverage** — every numbered spec section maps to at least one task (see the map below). Gaps found and fixed while reviewing: (a) spec §8 "verifier verifies schema_hash … mismatch is signed" needed `schema_hash` on the replay wire — added `replay.Statement.SchemaHash` / `pb.Statement.schema_hash = 13` (Tasks 1, 6, 23, 29) as an explicit, documented addition beyond the spec's `payload_format/client_revision`; (b) spec §8 "new PrepareLocalStatement error class the housegate orchestrator maps to TerminalReject" needed a housegate-side sentinel — added `sicore.ErrPrepareTerminalReject` (Task 17 Step 3b) and the sentio-node mapping (Task 34); (c) spec §4.4 `SafeState.GetL3Block` needed proto messages the spec did not spell out — `L3BlockHeader`/`L3BlockRef`/`L3Block` (Task 1) mirrored with a json-tag parity test (Task 30).

**2. Placeholder scan** — no "TBD/TODO/implement later"; every code step carries the code. Deliberate executor-judgment points are named explicitly: Task 19 (`runtimeTestConfig(t)` = the existing runtime-config helper), Task 30 (`newSafeStateTestHarness` copied from the existing server tests), Task 35 (`validSIConfig(t)` = the existing sentio-node config fixture), Task 29 (golden literal fallback if map-key order differs from struct order).

**3. Type/name consistency (checked across tasks)** — `sistatement` (package, `pkg/plugins/sistatement`, Tasks 13–16, 21); `Options.StorageIntegrityTableSchemas` (Task 16 defines, Task 20 uses); `Codec.WriteSampleBlock([]chproto.SampleColumn)` (Task 9 defines, Task 10 uses); `plugin.DeferredInsertPlan{SampleColumns, MaxPayloadBytes}` / `QueryContext.DeferredInsert` (Task 10 defines, Tasks 11, 15 use); `auth.JWSStatementPayloadV2`, `auth.StatementPurposeV2`, `auth.StatementTokenSettingKey`, `auth.StatementPayloadV2Mismatch`, `RelaySigner.SignStatementV2`, `EthValidator.ValidateStatementV2`, `auth.DecodeStatementV2Payload`, `auth.StatementSignerV2`, `auth.StatementValidatorV2` (Task 4 defines; Tasks 5, 15, 16, 18, 20, 28, 31, 35 use); `sicore.EmptySettingsHash`, `sicore.SettingsHashDomain`, `sicore.EnvelopeVersionV2`, `sicore.RejectUserSettings`, `sicore.IsHousegateOwnedSettingKey` (Task 8; used 15, 17, 18, 28 literal, 31 literal); `sicore.ParseFlatStatementID` (Task 14 defines, 15/20 use); `sicore.ErrPrepareTerminalReject` (Task 17 Step 3b, Task 34); `payloadexec.RowIDProfileID`, `payloadexec.PayloadFormatCSVWithNames` (Task 6; used 7, 8, 15, 17–20, 24, 28, 31); `nativepayload.{PayloadFormat, Decode, Materializer, ValidateDecodable, ErrUnsupported}` (Task 7; used 20, 24, 31); `replay.Statement.{PayloadFormat, ClientRevision, SchemaHash}`, `replay.SchemaHashSource`, `Verifier.SchemaHashes` (Task 6; used 7, 23, 25, 29); `arbiter.DomainL3Statements` (Task 23; used 29); `fsm.Params.NetworkID`, `L3BlockHeader.StatementsRoot`, `FSM.L3BlockView`, `verifyUserJWSV2` (Tasks 28–30); `pb.L3Block/L3BlockRef/L3BlockHeader`, `SafeState.GetL3Block` (Tasks 1, 30); `snode.ErrSchemaHashMismatch` (Task 24; Task 34); `config.StorageIntegrityAgentConfig` (Task 12; Task 16, 20 tests); `StorageIntegrityIngressConfig.NetworkID` (Task 20; Task 35). Relay helper names (`runDeferredInsert`, `armDeferredSample`, `settleDeferredSample`, `disarmDeferredSample`, `deferredSampleArmed`) are internal to Task 10 and referenced only there and in Task 21's CLAUDE.md text.

**Deliberate deviations from the spec text (all recorded above):** `WriteSampleBlock` takes `[]chproto.SampleColumn{Name, Type}` instead of `[]proto.ColInput` (a 0-row block needs only name+type; avoids ch-go `ColAuto` type-coverage limits); `settings_hash` uses `replay.CanonicalDigest` (there is no `replay.DigestJSON` in housegate); the sample block honours an INSERT column list that names every declared column (SQL order), rejecting subsets early; the agent's per-session `USE` tracking is done inside `sistatement` (agent mode has no rewriter to mirror it); the ingress keeps `SQL_x_auth_token` in `Admission.AuthToken` and puts the v2 token in `Admission.UserJWS`.

## Spec coverage map

| Spec section | Requirement | Tasks |
|---|---|---|
| §1 / §2 goals 1–5 | signature binds every field; stored bytes == signed bytes; L3 commits envelopes; deterministic FSM admission; agent produces v2 without SDK changes | 4, 5, 10–11, 15–16, 18, 20, 28–29, 31 |
| §3 D1 (Native canonical payload; CSV bridge removed) | 7, 8, 17, 19, 24, 34, 35 |
| §3 D2 (agent answers sample block, defers Query) | 9, 10, 11, 15, 16, 20 |
| §3 D3 (two tokens; auth plugin untouched) | 4, 15, 18 |
| §3 D4 (`settings_hash` = empty set; reject named setting) | 8, 15, 18, 28 |
| §3 D5 (`schema_hash` in envelope) | 6, 15, 18, 24, 25, 28 |
| §3 D6 (agent-generated statement id, durable seq) | 13, 15 |
| §4.1 wire (fields 11–17) | 1, 2, 3, 17, 23 |
| §4.2 `user_jws_v2` payload/signer/validator | 4, 5, 28 (shared vectors) |
| §4.3 hash profiles | 6, 8, 15, 18 |
| §4.4 L3 header `statements_root`, `ChainHash`, `GetL3Block` | 1, 23 (domain), 29, 30 |
| §5.1 agent plugin (`sistatement`, config, hooks, seq durability) | 12, 13, 14, 15, 16 |
| §5.2 Relay deferred-INSERT + `WriteSampleBlock` + fragmentation tests | 9, 10, 11 |
| §6 ingress v2 validation, CSV bridge deletion, `AdmissionRecord`/`StatementEnvelope`/`arbiter_proto.go` fill | 17, 18, 19, 20 |
| §7 admission v2, `verifyUserJWSV2`, `Params.NetworkID`, snapshot v2, replay statements carry v2 | 27, 28, 29 |
| §8 snode Native + `schema_hash` reject; chexec branch; `nativepayload` move; verifier `schema_hash` evidence; sentio-node carry-through | 7, 22, 23, 24, 25, 33, 34, 35 |
| §9 testing: auth vectors, relay matrix, plugin unit tests, ingress matrix, nativepayload move green, agent→server→CH docker test, arbiter admission tests, shared vector file, ChainHash golden, snapshot v2 round-trip, chpipeline fourth fraud class, conformance gates, snode/verifier tests, docs | 4, 5, 7, 11, 15, 18, 20, 23, 24, 25, 28, 29, 31, 36 |
| §10 delivery order + pin/tag tasks | 1–2 (proto), 3, 21 (housegate pins/tag), 22, 26 (arbiter-core), 27, 32 (arbiter), 33 (sentio-node), 36 (Spec B) |
| Roadmap §4 decisions 1–4 | 7/8/19 (1), 10/15/16 (2), 8/15/18/28 (3), 6/15/18/24 (4) |
