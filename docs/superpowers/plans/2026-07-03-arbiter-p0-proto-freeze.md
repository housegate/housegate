# Sentio Arbiter P0 — Proto Contract Freeze & Repo Scaffolding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Freeze the Sentio Arbiter's P0 protocol surfaces — the `arbiter-proto` wire contract (gRPC services + Raft command alphabet), the exported `replay.CanonicalDigest` single hashing profile, the §3.4 interface seams, and the authority promotion-signing payload — as working, tested code across three repos.

**Architecture:** Per [2026-06-30-sentio-arbiter-design.md](../specs/2026-06-30-sentio-arbiter-design.md) §12 P0. The proto module (`github.com/sentioxyz/arbiter-proto`) mirrors the `rewriter-go` pattern: `.proto` + committed generated Go under `gen/pb`, consumed as `pb`. The service repo (`github.com/sentioxyz/arbiter`) hosts the frozen Go seams and the authority signing code, reusing housegate's `pkg/replay` / `pkg/auth` via a Go-module `replace`. One housegate PR exports `replay.CanonicalDigest` so every root/commitment in the integrity layer shares one canonicalization profile.

**Tech Stack:** proto3 + buf v2 (lint BASIC, breaking FILE), protoc-gen-go v1.36.11 + protoc-gen-go-grpc v1.5.1, Go 1.26.3, go-ethereum crypto (secp256k1 JWS), hashicorp/raft (interface seam only in P0).

## Global Constraints

- **Three working directories.** housegate: `/Users/uranuswch/Dev/housegate/housegate`. arbiter-proto: `/Users/uranuswch/src/sentio_xyz/arbiter-proto`. arbiter: `/Users/uranuswch/src/sentio_xyz/arbiter`. Every task states its repo; every commit goes to that repo.
- **Module paths (frozen):** `github.com/sentioxyz/arbiter-proto` and `github.com/sentioxyz/arbiter` (from the repos' git remotes). The design doc's `github.com/housegate/arbiter-go` name is superseded by the user's repo choice — same role, different name. housegate's module path is `housegate/housegate` (NOT a GitHub path); the arbiter repo consumes it via `replace housegate/housegate => github.com/housegate/housegate <pseudo-version>` (valid because housegate's go.mod declares `module housegate/housegate`, which is the left-hand side of the replace).
- **housegate's own replaces must be copied** into the arbiter repo's go.mod (Go replaces don't propagate): `github.com/wasmerio/wasmer-go => github.com/sentioxyz/wasmer-go v1.0.5-0.20250206064014-c65a8b154145`, `github.com/ClickHouse/ch-go => github.com/sentioxyz/ch-go v0.71.0-sentioxyz-20260225`, `github.com/ClickHouse/clickhouse-go/v2 => github.com/sentioxyz/clickhouse-go/v2 v2.41.0-sentioxyz`.
- **Private-repo fetch:** `go env -w GOPRIVATE=github.com/sentioxyz,github.com/housegate` plus git rewrite `git config --global url."git@github.com:".insteadOf "https://github.com/"` (the user's SSH host alias `sentio` also works: `url."git@sentio:".insteadOf` scoped per org). Verify with `go mod download` before assuming CI parity.
- **Go version:** `go 1.26.3` in both new repos (arbiter must be ≥ housegate's declared 1.26.3).
- **Generator pins:** protoc-gen-go `v1.36.11`, protoc-gen-go-grpc `v1.5.1`, installed via `make tools` (pins live in the Makefile; CI runs `make tools` before the drift check).
- **buf config:** v2; `lint: use: [BASIC], except: [PACKAGE_DIRECTORY_MATCH]`; `breaking: use: [FILE]`. Proto package is bare `arbiter` with files at `proto/*.proto` and `go_package = "github.com/sentioxyz/arbiter-proto/gen/pb;pb"` — the exact rewriter-go layout (flat `gen/pb`, import alias `pb`). Verified locally: `uint64 bytes = 7;` (field named `bytes`) parses and lints clean under this config.
- **Wire-name freeze:** every field in `replay.proto` must equal the corresponding `pkg/replay` JSON tag verbatim (`block_seq`, `source_claim_root`, `part_row_lthash`, …). Task 9's conformance test enforces this mechanically; do not "improve" a name.
- **One canonicalization profile (§4.3 / §13):** every hash/root computed in arbiter code goes through `replay.CanonicalDigest` (profile `housegate-replay-mvp-v0`). Never hash re-encoded proto bytes — Raft replicates encoded bytes verbatim so proto is fine as a *transport*, but canonical hashing always happens over the mirrored Go structs.
- **Design deviations locked here** (each deliberate, each flagged for review): (1) `VerifierGateway` subscribe stream carries a `VerifierDispatch` oneof (ReplayJob | ByteSideScanRequest) instead of the spec sketch's bare `stream ReplayJob` — §7.1's two-round flow needs a scan-dispatch direction the sketch omits. (2) Enum values use buf-conformant SCREAMING_SNAKE prefixes (the spec sketches CamelCase). (3) The flat `statement_id` string form is frozen as `"<lowercase account>:<decimal client_seq>:<client_nonce>"` (the accumulator's binary leaf encoding stays a P0b deliverable).
- **Conventions:** English code/comments/log messages; markdown docs with no hard line-wrapping; commit messages in conventional-commit style (`feat:`, `build:`, `docs:`, `test:`). housegate work follows housegate's CLAUDE.md (Bazel is test ground truth there).
- **Out of P0a scope (named follow-up plans):** the mountain-range accumulator implementation + test vectors ("P0b accumulator plan"), the FSM/raftnode/orchestrator/server implementations and `cmd/arbiter` (P1 plans), Bazel wiring for the arbiter repo (decide when org CI integration lands; Go modules are sufficient for P0/P1 development), any SNode/Verifier data-plane code.

## File Structure

```text
arbiter-proto/                        # Task 1–5
  go.mod                              # module github.com/sentioxyz/arbiter-proto
  buf.yaml  buf.gen.yaml  Makefile  .gitignore  README.md
  .github/workflows/ci.yml            # lint + breaking + drift + build
  proto/replay.proto                  # wire mirror of pkg/replay reused types
  proto/arbiter.proto                 # new domain messages + 6 gRPC services
  proto/raftlog.proto                 # Raft command alphabet (§4.1)
  gen/pb/*.pb.go                      # committed generated code

housegate/                            # Task 6 (branch + PR)
  pkg/replay/hash.go                  # + CanonicalDigest export
  pkg/replay/hash_test.go             # new: identity + frozen-vector tests

arbiter/                              # Task 7–9
  go.mod                              # module github.com/sentioxyz/arbiter (+ replaces)
  README.md  .gitignore
  types.go  types_test.go             # StatementCoord, TablePartition, command structs, StatementIDString
  sharder.go  sharder_test.go         # Sharder seam + group-0 impl (§10.6)
  accumulator/accumulator.go          # Accumulator interface + Proof (impl = P0b)
  orchestrator/orchestrator.go        # Orchestrator interface (impl = P1)
  raftnode/consensus.go               # ConsensusNode seam over hashicorp/raft
  authority/payload.go                # JWS promotion payload + purpose claim
  authority/signer.go  authority/validator.go
  authority/authority_test.go         # Task 8 TDD
  conformance/replay_wire_test.go     # Task 9: pb ⇄ pkg/replay field-name equality
```

Task order: 1→2→3→4→5 (arbiter-proto, sequential — same files), 6 (housegate, independent, can run any time before 7), 7→8→9 (arbiter repo; 7 needs the Task 5 tag and the Task 6 merge commit).

---

### Task 1: arbiter-proto scaffolding + `replay.proto` (mirrored wire types)

**Files:**
- Create: `go.mod`, `buf.yaml`, `buf.gen.yaml`, `Makefile`, `.gitignore`, `README.md`, `proto/replay.proto`
- Repo: `/Users/uranuswch/src/sentio_xyz/arbiter-proto`

**Interfaces:**
- Consumes: housegate `pkg/replay` JSON tags (reference only — listed verbatim in the proto below).
- Produces: proto package `arbiter`, messages `Statement`, `ReplayJob`, `PartitionCommitment`, `PartManifestEntry`, `ExecutionReceipt`, `ReplayAttestation`, `TableManifest`, `SafeSnapshotManifest`; Go import `pb "github.com/sentioxyz/arbiter-proto/gen/pb"`. Tasks 2–4 add files to the same proto package; Task 9 asserts field-name parity.

- [ ] **Step 1: Write the scaffolding files**

`go.mod`:

```go
module github.com/sentioxyz/arbiter-proto

go 1.26.3
```

`buf.yaml`:

```yaml
version: v2
modules:
  - path: proto
lint:
  use: [BASIC]
  # Files live flat at proto/*.proto with bare package `arbiter`, mirroring
  # rewriter-go's layout (single gen/pb output imported as `pb`).
  except: [PACKAGE_DIRECTORY_MATCH]
breaking:
  use: [FILE]
```

`buf.gen.yaml`:

```yaml
version: v2
plugins:
  - local: protoc-gen-go
    out: gen/pb
    opt: paths=source_relative
  - local: protoc-gen-go-grpc
    out: gen/pb
    opt: paths=source_relative
```

`Makefile`:

```make
# Generator versions are pinned here; `make tools` is the single source of
# truth for local dev and CI (the drift check depends on identical versions).
PROTOC_GEN_GO_VERSION      := v1.36.11
PROTOC_GEN_GO_GRPC_VERSION := v1.5.1

.PHONY: tools proto lint breaking test tidy

tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)

proto:
	buf generate

lint:
	buf lint

breaking:
	buf breaking --against '.git#branch=main'

test:
	go build ./...
	go vet ./...

tidy:
	go mod tidy
```

`.gitignore`:

```
*.test
*.out
```

`README.md`:

```markdown
# arbiter-proto

Wire contract for the Sentio Arbiter (L3-block sequencer + admission + attestation collector + safe-state publisher). The `.proto` files and the generated Go live here; consumers import `pb "github.com/sentioxyz/arbiter-proto/gen/pb"` — the same pattern as `rewriter-go/gen/pb`.

Design source of truth: housegate `docs/superpowers/specs/2026-06-30-sentio-arbiter-design.md` (§11 services, §4.1 Raft command alphabet).

## Layout

| Path | What |
|---|---|
| `proto/replay.proto` | Wire mirror of housegate `pkg/replay` reused types. Field names are frozen against those types' JSON tags — a conformance test in the `arbiter` repo enforces parity. |
| `proto/arbiter.proto` | Arbiter domain messages + the six gRPC services. |
| `proto/raftlog.proto` | The replicated-FSM command alphabet (Raft log entry payloads). |
| `gen/pb` | Generated Go (committed). |

## Build & regenerate

```bash
make tools    # install pinned protoc-gen-go / protoc-gen-go-grpc
make proto    # regenerate gen/pb (buf)
make lint     # buf lint
make test     # go build + go vet over generated code
```
```

- [ ] **Step 2: Write `proto/replay.proto`**

```proto
syntax = "proto3";

// replay — wire mirror of the housegate pkg/replay types the Arbiter reuses
// verbatim (arbiter design §3.3 "reuse from pkg/replay", §11.2).
//
// FREEZE RULE: every field name below MUST equal the JSON tag of the
// corresponding Go type in housegate pkg/replay/types.go. The Arbiter FSM
// recomputes receipt_hash and manifest roots over those Go types via
// replay.CanonicalDigest after converting from proto, so the conversion must
// be lossless and name-stable. A conformance test in the arbiter repo
// (conformance/replay_wire_test.go) asserts field-set equality; changing a
// name here without changing pkg/replay is a build-breaking contract bug.
//
// Deliberately NOT mirrored: PreparedStatement, ExecutionRequest,
// ExecutionResult (executor-local, never on the Arbiter wire).
package arbiter;
option go_package = "github.com/sentioxyz/arbiter-proto/gen/pb;pb";

// Statement is the replay-relevant projection of a signed statement envelope
// (pkg/replay.Statement). The full signed envelope (StatementEnvelopeV2,
// arbiter.proto) stays Arbiter-side; verifiers receive only this projection.
message Statement {
  // Flat statement id: "<lowercase client_account>:<decimal client_seq>:<client_nonce>".
  string statement_id = 1;
  uint64 statement_seq = 2;
  string sql = 3;
  // DigestString(sql) — "0x" + hex(SHA-256), the pkg/replay hash profile.
  string sql_hash = 4;
  string settings_hash = 5;
  // Payload-store / DA reference; payload bytes are fetched by the verifier
  // and validated against payload_hash + payload_length BEFORE any executor
  // runs (pkg/replay verifier invariant).
  string payload_ref = 6;
  string payload_hash = 7;
  uint64 payload_length = 8;
  string target_table_id = 9;
  string user_jws = 10;
}

// ReplayJob is the verifier input for one sequenced L3 block
// (pkg/replay.ReplayJob). Dispatched leader → verifier on the
// VerifierGateway subscribe stream.
message ReplayJob {
  uint64 block_seq = 1;
  string prev_safe_snapshot_id = 2;
  string prev_state_root = 3;
  string schema_snapshot_id = 4;
  string executor_profile_id = 5;
  // The source's claimed post-state root; check 1 of the three-way predicate
  // compares each verifier's computed_state_root against this (§7.3).
  string source_claim_root = 6;
  repeated Statement statements = 7;
}

// PartitionCommitment is the post-replay commitment for one table partition
// (pkg/replay.PartitionCommitment). root is "0x" + hex of the raw 2048-byte
// LtHash accumulator so deltas stay additive.
message PartitionCommitment {
  string table_id = 1;
  string partition_id = 2;
  string root = 3;
}

// PartManifestEntry identifies an immutable safe part by content, not by a
// trusted storage location (pkg/replay.PartManifestEntry).
message PartManifestEntry {
  string table_id = 1;
  string partition_id = 2;
  string part_name = 3;
  string part_phys_hash = 4;
  // Content commitment of the part's rows: "0x" + hex of the raw 2048-byte
  // LtHash accumulator. This is the part identity used across RCRecord /
  // PromoteSafePartition / byte-side scans.
  string part_row_lthash = 5;
  uint64 row_count = 6;
  uint64 bytes = 7;
  repeated string storage_refs = 8;
}

// ExecutionReceipt is what a verifier signs (pkg/replay.ExecutionReceipt).
// Mismatches are signed too — a signed mismatch is non-repudiable challenge
// evidence, not an error (§base-spec C.4).
message ExecutionReceipt {
  uint64 block_seq = 1;
  string prev_safe_snapshot_id = 2;
  string prev_state_root = 3;
  string schema_snapshot_id = 4;
  string executor_profile_id = 5;
  string statement_root = 6;
  string payload_root = 7;
  string source_claim_root = 8;
  string computed_state_root = 9;
  // Advisory only — the FSM recomputes check 1 as
  // computed_state_root == RCRecord.source_claim_root (§7.3, §13).
  bool match_source_root = 10;
  // ABSOLUTE post-state commitments (not deltas); check 2 is phrased
  // additively: base + Σ claimed new-part lthash == this value.
  repeated PartitionCommitment partition_commitments_after = 11;
  repeated PartManifestEntry affected_parts = 12;
  string replay_log_hash = 13;
}

// ReplayAttestation is the verifier output the Arbiter records
// (pkg/replay.ReplayAttestation). receipt_hash =
// CanonicalDigest("replay-execution-receipt", receipt) over the Go type;
// signature = ed25519 over the receipt_hash string bytes, hex-encoded
// (payloadexec.Ed25519Signer convention). The FSM verifies the signature
// against the replica's RegisterNode pubkey and recomputes receipt_hash over
// the received receipt verbatim (§4.3 red line 2).
message ReplayAttestation {
  string replica_id = 1;
  ExecutionReceipt receipt = 2;
  string receipt_hash = 3;
  string signature = 4;
  bool match_source_root = 5;
}

// TableManifest records the data root for one verified table
// (pkg/replay.TableManifest).
message TableManifest {
  string table_id = 1;
  string schema_hash = 2;
  repeated PartitionCommitment partition_roots = 3;
  repeated PartManifestEntry active_parts = 4;
}

// SafeSnapshotManifest is the published safe watermark
// (pkg/replay.SafeSnapshotManifest). The FSM validates it via pkg/replay
// Seal/Validate before recording (§8.5); ordering is normalized (tables by
// table_id, partition_roots by (table_id, partition_id), active_parts by
// (table_id, partition_id, part_name)) so independent nodes derive the same
// roots.
message SafeSnapshotManifest {
  string snapshot_id = 1;
  string parent_snapshot_id = 2;
  uint64 safe_block_seq = 3;
  string state_root = 4;
  string schema_snapshot_id = 5;
  string schema_root = 6;
  string executor_profile_id = 7;
  string data_root = 8;
  string manifest_root = 9;
  repeated TableManifest tables = 10;
}
```

- [ ] **Step 3: Lint and build the proto**

Run (repo root): `buf lint && buf build -o /dev/null`
Expected: exit 0, no output.

- [ ] **Step 4: Generate and compile**

Run: `make tools && make proto && go mod tidy && make test`
Expected: `gen/pb/replay.pb.go` created; `go build ./...` and `go vet ./...` pass. `go mod tidy` adds `google.golang.org/protobuf` to go.mod.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "feat: scaffold buf module + replay wire mirror of housegate pkg/replay"
```

---

### Task 2: `arbiter.proto` — domain messages

**Files:**
- Create: `proto/arbiter.proto`
- Repo: `/Users/uranuswch/src/sentio_xyz/arbiter-proto`

**Interfaces:**
- Consumes: `replay.proto` messages (same package, `import "replay.proto";`).
- Produces: `StatementID`, `StatementEnvelopeV2`, `SequencedAck`, `AdmissionCode`, `RCRecord`, `CandidatePart`, `PartitionLtHashSum`, `PartRef`, `PromoteSafePartition`, `UnsafeCleanup`, `PromotionCommand`, `ByteSideScanRequest`, `ByteSideScanMsg`, `PartScan`, `AnchorRef`, `PromotionAck`, `SafePartMapping`, `CleanupAck`, plus hello/read/membership/ack messages. Task 3 appends the services to this same file; Task 4 embeds these messages in Raft commands; Task 8's Go signing structs mirror `PromoteSafePartition`/`PartRef`/`UnsafeCleanup` field-for-field.

- [ ] **Step 1: Write `proto/arbiter.proto`** (messages only; Task 3 appends services)

```proto
syntax = "proto3";

// arbiter — gRPC contract for the Sentio Arbiter (design §11): statement
// ingress, source result claims, verifier dispatch/attestation, promotion
// dispatch/ack, safe-state reads, and membership.
//
// Direction convention (§11.1): the data plane always dials the Arbiter.
// Unary calls go client → Arbiter; Arbiter-initiated dispatch rides
// node-initiated server-streaming subscriptions. Write-class RPCs on a
// non-leader fail with gRPC FAILED_PRECONDITION carrying a NotLeader detail;
// subscribe streams are only established against the leader (§11.3).
//
// Idempotency keys (§10.3, §11.3): SubmitStatement (client_account,
// client_seq); RegisterResultClaim statement_id; SubmitAttestation
// (replica_id, block_seq); SubmitByteSideScan (replica_id, block_seq);
// AckPromotion promotion_seq. Duplicate submissions return the original
// result, never an error.
package arbiter;
option go_package = "github.com/sentioxyz/arbiter-proto/gen/pb;pb";

import "replay.proto";

// ============================================================
// Ingress: HouseGate → Arbiter (§5.5 route A / optimistic-forward)
// ============================================================

// StatementID is the structured client-assigned statement identity.
// Uniqueness key is (client_account, client_seq) — client_nonce contributes
// entropy to _hg_row_id but is NOT part of the uniqueness key (§6.1).
// Flat string form (replay projection, RCRecord linkage, row-id derivation):
// "<lowercase client_account>:<decimal client_seq>:<client_nonce>".
message StatementID {
  // Lowercase 0x-prefixed Ethereum address of the signing account.
  string client_account = 1;
  uint64 client_seq = 2;
  string client_nonce = 3;
}

// StatementKind gates admission (§4.1 SubmitStatement). v1 admits INSERT
// only; DDL/mutation kinds arrive with P2+ and extend this enum.
enum StatementKind {
  STATEMENT_KIND_UNSPECIFIED = 0;
  STATEMENT_KIND_INSERT = 1;
}

// StatementEnvelopeV2 is the signed write-statement envelope HouseGate
// submits after validating the user signature and spooling the payload to
// the payload store at ingest (§3.5 step 1-2). The Arbiter re-verifies
// user_jws deterministically in Apply (§4.1).
message StatementEnvelopeV2 {
  StatementID statement_id = 1;
  StatementKind statement_kind = 2;
  string sql = 3;
  // DigestString(sql) under the housegate-replay-mvp-v0 profile.
  string sql_hash = 4;
  string settings_hash = 5;
  string payload_ref = 6;
  string payload_hash = 7;
  uint64 payload_length = 8;
  string target_table_id = 9;
  // The user's signature binding the statement (JWS compact form).
  string user_jws = 10;
}

// AdmissionCode is the application-level admission taxonomy. Transport
// failures are gRPC errors; admission rejections return OK + a code, the
// rewriter-go RewriteCode pattern.
enum AdmissionCode {
  ADMISSION_CODE_UNSPECIFIED = 0;
  ADMISSION_CODE_ACCEPTED = 1;
  // Reused (client_account, client_seq): a CLIENT BUG the SDK must surface,
  // never silently drop (§6.1). Do not retry with the same client_seq.
  ADMISSION_CODE_DUPLICATE_CLIENT_SEQ = 2;
  ADMISSION_CODE_SCHEMA_NOT_ALLOWED = 3;
  ADMISSION_CODE_KIND_NOT_ADMITTED = 4;
  ADMISSION_CODE_INVALID_SIGNATURE = 5;
  // The carried non-membership proof failed verification against
  // spent_ids_root (gap-fill path only, §6.3).
  ADMISSION_CODE_INVALID_PROOF = 6;
  ADMISSION_CODE_MALFORMED = 7;
}

message SequencedAck {
  AdmissionCode code = 1;
  // Human-readable rejection detail; empty on ACCEPTED.
  string message = 2;
  // Assigned global statement sequence; set only when code == ACCEPTED.
  uint64 statement_seq = 3;
}

// ============================================================
// Source result claims: source SNode → Arbiter (§4.1 RegisterRC)
// ============================================================

// CandidatePart is one hg_unsafe part the source claims a statement
// produced. part_row_lthash is the part's content commitment and its
// identity everywhere downstream (part names are unstable after
// attach/replace, base-spec §12.2).
message CandidatePart {
  string table_id = 1;
  string partition_id = 2;
  // Advisory: the hg_unsafe part name at claim time.
  string part_name = 3;
  string part_row_lthash = 4;
  string part_phys_hash = 5;
  uint64 row_count = 6;
  uint64 bytes = 7;
}

// PartitionLtHashSum is the source's claimed per-partition sum of new-part
// LtHash contributions — the "claimed" side of check 2's additive equation
// base + claimed == post (§7.3).
message PartitionLtHashSum {
  string table_id = 1;
  string partition_id = 2;
  // "0x" + hex of the raw 2048-byte LtHash accumulator sum.
  string new_parts_lthash_sum = 3;
}

// RCRecord is the source's result claim (§4.1, §7.3). Late binding: an
// RCRecord may arrive before its statement_seq exists; the FSM parks it
// under statement_id and binds when SubmitStatement assigns the seq (§5.5).
// v1 trusts the gRPC channel for source identity; a source signature slot
// can be added compatibly when P5+ decentralizes.
message RCRecord {
  StatementID statement_id = 1;
  // NodeID of the claiming source SNode; must match the FSM's deterministic
  // source selection for this statement (§5.4).
  string source_node = 2;
  repeated CandidatePart candidate_parts = 3;
  // The source's claimed post-state root (check 1's right-hand side).
  string source_claim_root = 4;
  repeated PartitionLtHashSum partition_new_part_sums = 5;
}

// ============================================================
// Verifier dispatch + evidence: Arbiter ⇄ Verifier (§7.1 two rounds)
// ============================================================

message VerifierHello {
  string replica_id = 1;
}

// VerifierDispatch is the leader→verifier dispatch stream payload. Two
// rounds per block (§7.1): the replay job first, then — after the replay
// attestation lands — the byte-side scan request. Kept as distinct kinds
// for auditability; they may be pipelined.
message VerifierDispatch {
  oneof dispatch {
    ReplayJob replay_job = 1;
    ByteSideScanRequest byte_side_scan = 2;
  }
}

// ByteSideScanRequest asks a verifier to recompute part_row_lthash from the
// real on-disk bytes of the candidate parts (check 3, §7.2). Parts are
// identified by content commitment, never by name — the verifier maps
// commitments to live _part names locally before SELECT ... WHERE _part IN.
message ByteSideScanRequest {
  uint64 block_seq = 1;
  repeated PartRef parts = 2;
}

// PartScan is one scanned part's result.
message PartScan {
  string table_id = 1;
  string partition_id = 2;
  // Identity: the source-claimed content commitment being checked.
  string claimed_part_row_lthash = 3;
  // The verifier's recomputation from on-disk bytes; check 3 compares this
  // recorded scalar against RCRecord.candidate_parts in the FSM (§7.3).
  string scanned_part_row_lthash = 4;
  // Advisory: the live _part name the verifier resolved locally.
  string live_part_name = 5;
}

// ByteSideScanMsg is the verifier's attested byte-side scan (§4.1
// RecordByteSideScan). scan_hash = CanonicalDigest over the canonical Go
// form (domain "arbiter-byte-side-scan-v1", frozen in the arbiter repo);
// signature = ed25519 over the scan_hash string bytes, hex-encoded — the
// same convention as ReplayAttestation.
message ByteSideScanMsg {
  string replica_id = 1;
  uint64 block_seq = 2;
  repeated PartScan parts = 3;
  string scan_hash = 4;
  string signature = 5;
}

// ============================================================
// Promotion: Arbiter → SNode (§8)
// ============================================================

// PartRef identifies a verified part by content commitment (§8.1).
message PartRef {
  string table_id = 1;
  string partition_id = 2;
  string part_row_lthash = 3;
  // Advisory name hint; never authoritative.
  string part_name = 4;
}

// PromoteSafePartition is the authority-signed promotion command (§8.1).
// candidate_parts lists EXACTLY the parts that passed the three-way check —
// SNode builds hg_promote from base + these only (the §7.3 closure rule).
message PromoteSafePartition {
  string table_id = 1;
  string partition_id = 2;
  // Monotonic per Arbiter; SNode persists a last-applied watermark per
  // (table, partition) and rejects seq <= watermark (§8.3 exactly-once).
  uint64 promotion_seq = 3;
  string base_safe_snapshot_id = 4;
  // Base partition's raw LtHash accumulator ("0x" + hex of 2048 bytes) for
  // the SNode-side CAS check (§8.3).
  string base_partition_root = 5;
  repeated PartRef candidate_parts = 6;
}

// UnsafeCleanup schedules the drop of promoted hg_unsafe parts (§8.5).
message UnsafeCleanup {
  string table_id = 1;
  string partition_id = 2;
  uint64 promotion_seq = 3;
  repeated PartRef parts = 4;
}

// PromotionCommand is the leader→SNode dispatch stream payload. authority_jws
// is the Arbiter authority's secp256k1 JWS (compact form) whose payload
// carries purpose "arbiter-promotion" and a CanonicalDigest of the command
// (domains "arbiter-promote-command-v1" / "arbiter-cleanup-command-v1");
// SNode authorizes by address recovery against the authority allowlist
// (§8.1). SNode never promotes without a valid signature (§13).
message PromotionCommand {
  oneof cmd {
    PromoteSafePartition promote = 1;
    UnsafeCleanup cleanup = 2;
  }
  string authority_jws = 15;
}

message SNodeHello {
  string node_id = 1;
}

// SafePartMapping records where a promoted part landed in hg_safe — part
// names change across attach/replace, so the mapping is keyed by content
// commitment (§8.2).
message SafePartMapping {
  string part_row_lthash = 1;
  string safe_part_name = 2;
  string part_phys_hash = 3;
}

// PromotionAck reports REPLACE PARTITION completion. The FSM checks the
// closure equality post_partition_commitment == base + Σ verified new-part
// lthash before advancing the safe watermark (§7.3 closure, §8.4).
message PromotionAck {
  string node_id = 1;
  uint64 promotion_seq = 2;
  string table_id = 3;
  string partition_id = 4;
  // The absolute partition LtHash root SNode observed after promotion.
  string post_partition_commitment = 5;
  repeated SafePartMapping parts = 6;
  // applied=false signals a dropped shadow (base CAS failed); detail says
  // why. The Arbiter rebases and re-issues (§8.3).
  bool applied = 7;
  string detail = 8;
}

message CleanupAck {
  string node_id = 1;
  uint64 promotion_seq = 2;
  string table_id = 3;
  string partition_id = 4;
}

// ============================================================
// Anchoring, safe-state reads, membership
// ============================================================

// AnchorRef references the L2 anchor of one L3 block (§5.2). v1 posts
// commitment only; da_ref is reserved so the decentralized phase can switch
// to commitment + DA reference as configuration, not protocol change.
message AnchorRef {
  string l3_block_hash = 1;
  string state_root = 2;
  // Chain-specific transaction reference of the anchor.
  string l2_tx_ref = 3;
  uint64 l2_block_number = 4;
  string da_ref = 5;
}

message GetSafeWatermarkRequest {}

message SafeWatermark {
  string snapshot_id = 1;
  uint64 safe_block_seq = 2;
  string manifest_root = 3;
}

message SnapshotRef {
  string snapshot_id = 1;
}

// BlockRef selects a manifest by SafeBlockSeq — the as_of_safe time-travel
// read (§8.5; time-travel is manifest-indexed, never a per-row column, §13).
message BlockRef {
  uint64 safe_block_seq = 1;
}

enum NodeRole {
  NODE_ROLE_UNSPECIFIED = 0;
  NODE_ROLE_VERIFIER = 1;
  NODE_ROLE_SNODE = 2;
}

// NodeRegistration enters a data-plane node into FSM membership (§4.1
// RegisterNode). Only MarkActive (snapshot-synced) nodes join the
// deterministic verifier-selection pool (§7.4).
message NodeRegistration {
  string node_id = 1;
  repeated NodeRole roles = 2;
  // ed25519 verify key for attestation/scan signatures (verifier role).
  bytes ed25519_pubkey = 3;
  // Advisory dial address, observability only — dispatch always rides
  // node-initiated subscribe streams (§11.1).
  string dial_addr = 4;
}

message NodeRef {
  string node_id = 1;
}

// Ack acknowledges an idempotent write; duplicates ack identically.
message Ack {}

// NotLeader is attached as a gRPC error detail (FAILED_PRECONDITION) by
// non-leader nodes on write-class RPCs; the client retries against
// leader_addr (§11.3).
message NotLeader {
  string leader_addr = 1;
}
```

- [ ] **Step 2: Lint + generate + build**

Run: `buf lint && make proto && make test`
Expected: exit 0; `gen/pb/arbiter.pb.go` created; build passes.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: arbiter domain messages (envelope, RC, promotion, scan, anchor, membership)"
```

---

### Task 3: `arbiter.proto` — the six gRPC services

**Files:**
- Modify: `proto/arbiter.proto` (append services at end of file)
- Repo: `/Users/uranuswch/src/sentio_xyz/arbiter-proto`

**Interfaces:**
- Consumes: all Task 2 messages.
- Produces: gRPC services `ArbiterIngress`, `SourceClaims`, `VerifierGateway`, `PromotionGateway`, `SafeState`, `Membership` (generated client/server stubs in `gen/pb/arbiter_grpc.pb.go`). P1 server plan implements these; HouseGate/SNode/Verifier clients consume them.

- [ ] **Step 1: Append the service definitions to `proto/arbiter.proto`**

```proto
// ============================================================
// Services (§11.2). Leader-only for all write/dispatch RPCs; SafeState
// reads may be served by followers with bounded staleness or routed via
// read-index for linearizability (§11.3).
// ============================================================

// ArbiterIngress is called by HouseGate.
service ArbiterIngress {
  // Idempotency key: (statement_id.client_account, statement_id.client_seq).
  rpc SubmitStatement (StatementEnvelopeV2) returns (SequencedAck) {}
}

// SourceClaims is called by the source SNode.
service SourceClaims {
  // Idempotency key: statement_id (same statement, same RC → same Ack).
  rpc RegisterResultClaim (RCRecord) returns (Ack) {}
}

// VerifierGateway is called by Verifiers (data plane dials in).
service VerifierGateway {
  // Leader dispatch stream: ReplayJob round, then ByteSideScanRequest round
  // (§7.1). A leader change breaks the stream; the verifier reconnects to
  // the new leader and the orchestrator re-dispatches from FSM state (§10.2).
  rpc SubscribeVerifierDispatch (VerifierHello) returns (stream VerifierDispatch) {}
  // Idempotency key: (replica_id, receipt.block_seq) — one vote per replica
  // per block (§10.3).
  rpc SubmitAttestation (ReplayAttestation) returns (Ack) {}
  // Idempotency key: (replica_id, block_seq).
  rpc SubmitByteSideScan (ByteSideScanMsg) returns (Ack) {}
}

// PromotionGateway is called by SNodes (data plane dials in).
service PromotionGateway {
  rpc SubscribePromotions (SNodeHello) returns (stream PromotionCommand) {}
  // Idempotency key: promotion_seq (per table/partition, §10.3).
  rpc AckPromotion (PromotionAck) returns (Ack) {}
  rpc AckCleanup (CleanupAck) returns (Ack) {}
}

// SafeState serves read-only safe-state queries to HouseGate and clients.
service SafeState {
  rpc GetSafeWatermark (GetSafeWatermarkRequest) returns (SafeWatermark) {}
  rpc GetManifest (SnapshotRef) returns (SafeSnapshotManifest) {}
  // as_of_safe time-travel by SafeBlockSeq (§8.5).
  rpc GetManifestByBlock (BlockRef) returns (SafeSnapshotManifest) {}
}

// Membership is called by SNodes / Verifiers.
service Membership {
  rpc RegisterNode (NodeRegistration) returns (Ack) {}
  // MarkActive is accepted only after snapshot sync; Active nodes enter the
  // deterministic selection pools (§4.1, §7.4).
  rpc MarkActive (NodeRef) returns (Ack) {}
}
```

- [ ] **Step 2: Lint + generate + build**

Run: `buf lint && make proto && go mod tidy && make test`
Expected: `gen/pb/arbiter_grpc.pb.go` created; `go mod tidy` adds `google.golang.org/grpc`; build passes.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: six Arbiter gRPC services (ingress, claims, verifier, promotion, safe-state, membership)"
```

---

### Task 4: `raftlog.proto` — the Raft command alphabet

**Files:**
- Create: `proto/raftlog.proto`
- Repo: `/Users/uranuswch/src/sentio_xyz/arbiter-proto`

**Interfaces:**
- Consumes: Task 1–2 messages (`StatementEnvelopeV2`, `RCRecord`, `ReplayAttestation`, `ByteSideScanMsg`, `PromoteSafePartition`, `UnsafeCleanup`, `SafeSnapshotManifest`, `PromotionAck`, `CleanupAck`, `NodeRegistration`, `AnchorRef`).
- Produces: `RaftCommand` + per-command messages — the P1 FSM decodes exactly these from `raft.Log.Data`.

- [ ] **Step 1: Write `proto/raftlog.proto`**

```proto
syntax = "proto3";

// raftlog — the Arbiter replicated-FSM command alphabet (design §4.1). One
// RaftCommand per Raft log entry; the leader's orchestrator makes every
// timing decision and proposes it as a command at a fixed log index; Apply
// performs only the deterministic state mutation.
//
// Proto is safe as the LOG TRANSPORT because Raft replicates the encoded
// bytes verbatim — every node decodes identical bytes, so non-canonical
// proto serialization cannot fork replicas. Canonical HASHING never runs
// over proto bytes: the FSM converts to the mirrored Go types and hashes
// through replay.CanonicalDigest (§4.3 red lines).
//
// Cluster membership changes (adding/removing Raft nodes) use
// hashicorp/raft's native configuration-change mechanism and are NOT
// commands here (§4.1).
package arbiter;
option go_package = "github.com/sentioxyz/arbiter-proto/gen/pb;pb";

import "arbiter.proto";
import "replay.proto";

// SubmitStatementCmd → Sequenced. Deterministic admission in Apply: verify
// envelope signature, verify the carried non-membership proof against the
// accumulator (proof VERIFIED in Apply, GENERATED leader-side outside Apply
// — §4.3 red line 4), check schema/kind allowlists, assign statement_seq,
// advance accumulator + high-water mark, buffer into the open block.
message SubmitStatementCmd {
  StatementEnvelopeV2 envelope = 1;
  // Opaque accumulator non-membership proof, present only on the
  // out-of-order gap-fill path (client_seq <= hi, §6.3). Byte encoding is
  // frozen by the P0b accumulator plan; this field is the frozen slot.
  bytes non_membership_proof = 2;
}

// SealL3BlockCmd seals the open buffer into an L3 block (§5.1). All header
// content (prev hash, seq range, spent_ids_root_after) derives
// deterministically from FSM state at this log index; the command carries
// nothing.
message SealL3BlockCmd {}

// MarkReplayingCmd records that the leader dispatched the first ReplayJob
// for a block (§4.4 lifecycle). The verifier SET is not carried — it is
// deterministically selected in the FSM from the committed Active set
// (sorted by NodeID, excluding source_node, §7.1).
message MarkReplayingCmd {
  uint64 block_seq = 1;
}

// RegisterRCCmd → UnsafeRegistered (late binding by statement_id, §5.5).
message RegisterRCCmd {
  RCRecord rc = 1;
}

// RecordAttestationCmd: verify ed25519 signature, recompute receipt_hash
// over the receipt verbatim, record, re-evaluate the three-way predicate →
// QuorumVerified on success (§4.1, §7.3).
message RecordAttestationCmd {
  ReplayAttestation attestation = 1;
}

// RecordByteSideScanCmd records the attested per-part scan scalars for
// check 3 (the FSM compares; it never re-runs the scan, §3.2).
message RecordByteSideScanCmd {
  ByteSideScanMsg scan = 1;
}

// RecordAnchorFinalityCmd: QuorumVerified + finality + last_mergeable →
// promotable (§4.1, §4.4).
message RecordAnchorFinalityCmd {
  uint64 l3_block_seq = 1;
  AnchorRef anchor = 2;
  bool finality_reached = 3;
  bool last_mergeable_reached = 4;
}

// RecordPromotionIssuedCmd records the signed promotion for audit — the
// leader's signature is its only privileged act and even that is logged
// (§3.2, §8.1).
message RecordPromotionIssuedCmd {
  PromoteSafePartition promote = 1;
  string authority_jws = 2;
}

// RecordPromotionAckCmd records SNode's REPLACE PARTITION ack; the FSM
// checks the closure equality before advancing the watermark (§7.3, §8.4).
message RecordPromotionAckCmd {
  PromotionAck ack = 1;
}

// PublishSafeSnapshotCmd: validated via pkg/replay Seal/Validate in Apply,
// then recorded as the new safe watermark (§8.5).
message PublishSafeSnapshotCmd {
  SafeSnapshotManifest manifest = 1;
}

// ScheduleUnsafeCleanupCmd marks promoted hg_unsafe parts for cleanup in
// the PromotedUnsafe registry that unsafe_latest filters against (§8.5).
message ScheduleUnsafeCleanupCmd {
  UnsafeCleanup cleanup = 1;
  string authority_jws = 2;
}

// RecordCleanupAckCmd clears PromotedUnsafe entries (idempotent, §10.3).
message RecordCleanupAckCmd {
  CleanupAck ack = 1;
}

message OpenChallengeCmd {
  uint64 block_seq = 1;
  string reason = 2;
  // Who triggered it: a verifier's replica_id or "timeout" (§7.5).
  string opened_by = 3;
}

enum ChallengeVerdict {
  CHALLENGE_VERDICT_UNSPECIFIED = 0;
  CHALLENGE_VERDICT_SAFE = 1;
  CHALLENGE_VERDICT_REJECTED = 2;
}

// ResolveChallengeCmd drives ChallengeReplay → Safe | Rejected using the
// SAME three-way predicate (§7.5).
message ResolveChallengeCmd {
  uint64 block_seq = 1;
  ChallengeVerdict verdict = 2;
}

message RegisterNodeCmd {
  NodeRegistration registration = 1;
}

message MarkActiveCmd {
  string node_id = 1;
}

message EvictNodeCmd {
  string node_id = 1;
  string reason = 2;
}

// RaftCommand is the single log-entry envelope. Field numbers are frozen;
// new commands append new numbers (never renumber/reuse).
message RaftCommand {
  oneof cmd {
    SubmitStatementCmd submit_statement = 1;
    SealL3BlockCmd seal_l3_block = 2;
    MarkReplayingCmd mark_replaying = 3;
    RegisterRCCmd register_rc = 4;
    RecordAttestationCmd record_attestation = 5;
    RecordByteSideScanCmd record_byte_side_scan = 6;
    RecordAnchorFinalityCmd record_anchor_finality = 7;
    RecordPromotionIssuedCmd record_promotion_issued = 8;
    RecordPromotionAckCmd record_promotion_ack = 9;
    PublishSafeSnapshotCmd publish_safe_snapshot = 10;
    ScheduleUnsafeCleanupCmd schedule_unsafe_cleanup = 11;
    RecordCleanupAckCmd record_cleanup_ack = 12;
    OpenChallengeCmd open_challenge = 13;
    ResolveChallengeCmd resolve_challenge = 14;
    RegisterNodeCmd register_node = 15;
    MarkActiveCmd mark_active = 16;
    EvictNodeCmd evict_node = 17;
  }
}
```

- [ ] **Step 2: Lint + generate + build**

Run: `buf lint && make proto && make test`
Expected: `gen/pb/raftlog.pb.go` created; build passes.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "feat: Raft command alphabet (raftlog.proto)"
```

---

### Task 5: arbiter-proto CI + tag v0.1.0

**Files:**
- Create: `.github/workflows/ci.yml`
- Repo: `/Users/uranuswch/src/sentio_xyz/arbiter-proto`

**Interfaces:**
- Produces: git tag `v0.1.0` — the version Task 7's `go.mod` requires.

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
name: ci

on:
  push:
    branches: [main]
  pull_request:

jobs:
  proto:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
      - uses: bufbuild/buf-action@v1
        with:
          setup_only: true
      - run: buf lint
      - name: breaking check (PRs only)
        if: github.event_name == 'pull_request'
        run: buf breaking --against ".git#branch=origin/main"
      - name: generated code is current
        run: |
          make tools
          make proto
          git diff --exit-code -- gen/
      - run: make test
```

- [ ] **Step 2: Verify the CI steps locally**

Run: `buf lint && make tools && make proto && git diff --exit-code -- gen/ && make test`
Expected: all exit 0 (gen/ has no drift because Task 4 committed regenerated output).

- [ ] **Step 3: Commit, push, tag**

```bash
git add -A
git commit -m "build: buf lint/breaking/drift CI"
git push origin main
git tag -a v0.1.0 -m "P0 contract freeze: replay wire mirror, six services, Raft command alphabet"
git push origin v0.1.0
```

Expected: CI green on main. The tag makes `go get github.com/sentioxyz/arbiter-proto@v0.1.0` resolvable (with GOPRIVATE configured).

---

### Task 6: housegate — export `replay.CanonicalDigest` (PR)

**Files:**
- Modify: `pkg/replay/hash.go` (append exported function)
- Create: `pkg/replay/hash_test.go`
- Repo: `/Users/uranuswch/Dev/housegate/housegate` (branch `export-canonical-digest`, PR to `main`)

**Interfaces:**
- Produces: `func CanonicalDigest(domain string, v any) (string, error)` in package `housegate/housegate/pkg/replay`. Task 8's authority package and all later FSM/verifier code hash through this — it is the §4.3 "one canonicalization profile" export.

- [ ] **Step 1: Create the branch**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git checkout main && git pull && git checkout -b export-canonical-digest
```

- [ ] **Step 2: Write the failing test** (`pkg/replay/hash_test.go`)

```go
package replay

import "testing"

// CanonicalDigest is the exported single canonicalization profile the
// integrity layer shares (arbiter design §4.3): a second, parallel hash
// profile anywhere in arbiter/verifier code is a correctness regression.

func TestCanonicalDigestMatchesReceiptHash(t *testing.T) {
	r := ExecutionReceipt{
		BlockSeq:           7,
		PrevSafeSnapshotID: "snap-1",
		ComputedStateRoot:  "0xabc",
	}
	want, err := r.Hash()
	if err != nil {
		t.Fatalf("receipt hash: %v", err)
	}
	got, err := CanonicalDigest("replay-execution-receipt", r)
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	if got != want {
		t.Fatalf("CanonicalDigest diverged from ExecutionReceipt.Hash: got %s want %s", got, want)
	}
}

func TestCanonicalDigestFrozenVector(t *testing.T) {
	// Frozen cross-implementation vector: SHA-256 over
	// "housegate-replay-mvp-v0:" || domain || 0x00 || canonical JSON.
	// If this test breaks, the wire profile changed and every recorded
	// root/receipt in the integrity layer is invalidated — do not "fix" the
	// expected value without a coordinated profile migration.
	got, err := CanonicalDigest("arbiter-p0-vector", struct {
		A string `json:"a"`
		N int    `json:"n"`
	}{A: "x", N: 7})
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	const want = "0xa6002c60260099a674a29f3af4bf87b440fa9933d7ac4ae4c41603d0099db512"
	if got != want {
		t.Fatalf("frozen vector mismatch: got %s want %s", got, want)
	}
}
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `bazel test //pkg/replay:replay_test --test_filter='TestCanonicalDigest' --test_output=errors`
Expected: FAIL — compile error `undefined: CanonicalDigest`. (If Bazel needs the new file registered first, run `bazel run //:gazelle`.)

- [ ] **Step 4: Implement the export** (append to `pkg/replay/hash.go`)

```go
// CanonicalDigest returns the digest of v under the integrity layer's single
// canonical hashing profile ("housegate-replay-mvp-v0"): SHA-256 over the
// domain-separated canonical JSON encoding of v, hex-encoded with a 0x
// prefix. Every root/commitment in the replay/arbiter integrity layer must
// go through this profile with a distinct domain tag per commitment kind —
// never a second, parallel hash profile — so independent nodes derive
// identical roots from the same evidence (arbiter design §4.3).
func CanonicalDigest(domain string, v any) (string, error) {
	return canonicalDigest(domain, v)
}
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //pkg/replay:replay_test --test_output=errors`
Expected: PASS (all pkg/replay tests, not just the new ones).

- [ ] **Step 6: Commit and open the PR**

```bash
git add pkg/replay/hash.go pkg/replay/hash_test.go
git commit -m "feat(replay): export CanonicalDigest as the shared canonicalization profile

The Sentio Arbiter (P0, docs/superpowers/specs/2026-06-30-sentio-arbiter-design.md §4.3)
requires one canonicalization profile across the integrity layer. Export the
existing canonicalDigest unchanged, with a frozen test vector so the profile
cannot silently drift.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push -u origin export-canonical-digest
gh pr create --title "feat(replay): export CanonicalDigest as the shared canonicalization profile" --body "Arbiter P0 deliverable (design §4.3, §12): exports the existing profile verbatim + identity test against ExecutionReceipt.Hash + a frozen cross-implementation vector.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

Expected: PR opens; CI green. Record the merge commit SHA — Task 7 pins the housegate replace to it.

---

### Task 7: arbiter repo bootstrap + frozen §3.4 seams

**Files:**
- Create: `go.mod`, `README.md`, `.gitignore`, `types.go`, `types_test.go`, `sharder.go`, `sharder_test.go`, `accumulator/accumulator.go`, `orchestrator/orchestrator.go`, `raftnode/consensus.go`
- Repo: `/Users/uranuswch/src/sentio_xyz/arbiter`

**Interfaces:**
- Consumes: `pb "github.com/sentioxyz/arbiter-proto/gen/pb"` (Task 5 tag); `housegate/housegate` at the Task 6 merge commit.
- Produces: package `arbiter` (module root): `StatementCoord{Account string; ClientSeq uint64}`, `TablePartition{TableID, PartitionID string}`, `GroupID uint32`, canonical signing structs `PromoteSafePartition` / `PartRef` / `UnsafeCleanup` (JSON tags matching the proto field names), `StatementIDString(account string, seq uint64, nonce string) string`, `Sharder` interface + `SingleGroupSharder`; package `accumulator`: `Accumulator` interface + `Proof []byte`; package `orchestrator`: `Orchestrator` interface; package `raftnode`: `ConsensusNode` interface. Task 8 imports the root command structs; the P0b/P1 plans implement the interfaces.

- [ ] **Step 1: Bootstrap the module**

```bash
cd /Users/uranuswch/src/sentio_xyz/arbiter
go env -w GOPRIVATE=github.com/sentioxyz,github.com/housegate
go mod init github.com/sentioxyz/arbiter
go mod edit -go=1.26.3
# Pin housegate to the Task 6 merge commit (replace resolves to a pseudo-version):
go mod edit -replace 'housegate/housegate=github.com/housegate/housegate@<TASK-6-MERGE-COMMIT-SHA>'
# Copy housegate's own replaces (Go replaces do not propagate to consumers):
go mod edit -replace 'github.com/wasmerio/wasmer-go=github.com/sentioxyz/wasmer-go@v1.0.5-0.20250206064014-c65a8b154145'
go mod edit -replace 'github.com/ClickHouse/ch-go=github.com/sentioxyz/ch-go@v0.71.0-sentioxyz-20260225'
go mod edit -replace 'github.com/ClickHouse/clickhouse-go/v2=github.com/sentioxyz/clickhouse-go/v2@v2.41.0-sentioxyz'
go get github.com/sentioxyz/arbiter-proto@v0.1.0
go get housegate/housegate
go get github.com/hashicorp/raft@latest
```

Expected: `go mod download` succeeds for all three private/replaced modules. If the https→ssh rewrite is missing, configure `git config --global url."git@github.com:".insteadOf "https://github.com/"` (or the `git@sentio:` alias equivalents) and retry.

`.gitignore`:

```
*.test
*.out
```

`README.md`:

```markdown
# arbiter

The Sentio Arbiter: L3-block sequencer + admission + attestation collector + safe-state publisher, built on hashicorp/raft. Control plane only — it sequences, adjudicates, and signs; it never stores user parts or executes user SQL on the hot path.

Design source of truth: housegate `docs/superpowers/specs/2026-06-30-sentio-arbiter-design.md`. Wire contract: `github.com/sentioxyz/arbiter-proto`.

## Layout

| Path | What |
|---|---|
| root package | Frozen §3.4 seams: command/coordinate types, `Sharder` (v1: single group). |
| `accumulator/` | statement_id uniqueness accumulator interface (§6); implementation lands with the P0b plan. |
| `orchestrator/` | Leader-only side-effectful loop interface (§3.2); implementation lands with P1. |
| `raftnode/` | Consensus seam over hashicorp/raft (§3.4). |
| `authority/` | secp256k1 promotion-command signing (JWS, purpose-claim domain-separated). |
| `conformance/` | Wire-compatibility tests against housegate `pkg/replay`. |

## Build & test

```bash
go build ./...
go test ./...
```

housegate is consumed via a `replace` to `github.com/housegate/housegate` (its module path is `housegate/housegate`); private-module fetch needs `GOPRIVATE=github.com/sentioxyz,github.com/housegate` and an https→ssh git rewrite.
```

- [ ] **Step 2: Write the failing tests** (`types_test.go`, `sharder_test.go`)

`types_test.go`:

```go
package arbiter

import "testing"

func TestStatementIDString(t *testing.T) {
	got := StatementIDString("0xAbCd000000000000000000000000000000000001", 42, "n-7")
	want := "0xabcd000000000000000000000000000000000001:42:n-7"
	if got != want {
		t.Fatalf("StatementIDString: got %q want %q", got, want)
	}
}
```

`sharder_test.go`:

```go
package arbiter

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

func TestSingleGroupSharderRoutesEverythingToGroupZero(t *testing.T) {
	var s Sharder = SingleGroupSharder{}
	if g := s.GroupForStatement(&pb.StatementEnvelopeV2{}); g != 0 {
		t.Fatalf("GroupForStatement: got %d want 0", g)
	}
	if g := s.GroupForPartition(TablePartition{TableID: "t", PartitionID: "p"}); g != 0 {
		t.Fatalf("GroupForPartition: got %d want 0", g)
	}
	if g := s.GroupForSchema("schema-1"); g != 0 {
		t.Fatalf("GroupForSchema: got %d want 0", g)
	}
	groups := s.Groups()
	if len(groups) != 1 || groups[0] != 0 {
		t.Fatalf("Groups: got %v want [0]", groups)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./...`
Expected: FAIL — `undefined: StatementIDString`, `undefined: Sharder`, etc.

- [ ] **Step 4: Write `types.go`**

```go
// Package arbiter defines the frozen P0 interface seams and canonical
// command/coordinate types of the Sentio Arbiter (design §3.4). It stays
// dependency-light so every subpackage (fsm, accumulator, orchestrator,
// raftnode, server, authority) can import it without cycles.
package arbiter

import (
	"strconv"
	"strings"
)

// StatementCoord is the statement_id uniqueness coordinate: one statement
// per (account, client_seq); client_nonce is NOT part of the key (§6.1).
type StatementCoord struct {
	Account   string `json:"account"`
	ClientSeq uint64 `json:"client_seq"`
}

// TablePartition addresses one partition of one logical table.
type TablePartition struct {
	TableID     string `json:"table_id"`
	PartitionID string `json:"partition_id"`
}

// StatementIDString renders the canonical flat statement_id used by the
// replay projection (pkg/replay.Statement.StatementID), RCRecord linkage,
// and _hg_row_id derivation: "<lowercase account>:<decimal seq>:<nonce>".
// The accumulator's binary leaf encoding is frozen separately (P0b plan);
// this string form is the cross-component linking id.
func StatementIDString(clientAccount string, clientSeq uint64, clientNonce string) string {
	return strings.ToLower(clientAccount) + ":" + strconv.FormatUint(clientSeq, 10) + ":" + clientNonce
}

// PartRef identifies a verified part by content commitment (design §8.1).
// JSON tags mirror the arbiter-proto PartRef field names: these structs are
// the CANONICAL SIGNING FORM — authority JWS payloads hash them through
// replay.CanonicalDigest, never re-encoded proto bytes (§4.3).
type PartRef struct {
	TableID       string `json:"table_id"`
	PartitionID   string `json:"partition_id"`
	PartRowLtHash string `json:"part_row_lthash"`
	PartName      string `json:"part_name,omitempty"`
}

// PromoteSafePartition is the canonical signing form of the promotion
// command (design §8.1); wire form is pb.PromoteSafePartition.
type PromoteSafePartition struct {
	TableID            string    `json:"table_id"`
	PartitionID        string    `json:"partition_id"`
	PromotionSeq       uint64    `json:"promotion_seq"`
	BaseSafeSnapshotID string    `json:"base_safe_snapshot_id"`
	BasePartitionRoot  string    `json:"base_partition_root"`
	CandidateParts     []PartRef `json:"candidate_parts"`
}

// UnsafeCleanup is the canonical signing form of the promoted-unsafe-part
// cleanup command (design §8.5); wire form is pb.UnsafeCleanup.
type UnsafeCleanup struct {
	TableID      string    `json:"table_id"`
	PartitionID  string    `json:"partition_id"`
	PromotionSeq uint64    `json:"promotion_seq"`
	Parts        []PartRef `json:"parts"`
}
```

- [ ] **Step 5: Write `sharder.go`**

```go
package arbiter

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// GroupID identifies one Raft group. v1 runs a single group (§10.6).
type GroupID uint32

// Sharder is the multi-Raft sharding seam (design §3.4, §10.6): every
// shard-routing decision flows through it so the P5+ multi-group manager is
// a single-interface replacement, not a rewrite. v1 returns group 0 for
// everything.
type Sharder interface {
	GroupForStatement(env *pb.StatementEnvelopeV2) GroupID
	GroupForPartition(p TablePartition) GroupID
	GroupForSchema(schemaSnapshotID string) GroupID
	Groups() []GroupID
}

// SingleGroupSharder is the v1 Sharder: one Raft group, group 0.
type SingleGroupSharder struct{}

var _ Sharder = SingleGroupSharder{}

func (SingleGroupSharder) GroupForStatement(*pb.StatementEnvelopeV2) GroupID { return 0 }
func (SingleGroupSharder) GroupForPartition(TablePartition) GroupID          { return 0 }
func (SingleGroupSharder) GroupForSchema(string) GroupID                     { return 0 }
func (SingleGroupSharder) Groups() []GroupID                                 { return []GroupID{0} }
```

- [ ] **Step 6: Write the seam-only packages**

`accumulator/accumulator.go`:

```go
// Package accumulator defines the statement_id uniqueness accumulator seam
// (design §6): an append-only, permanent commitment to the spent
// (account, client_seq) coordinates, replayable from the L3 stream, with
// O(log n) non-membership proofs. The recommended construction is a
// mountain-range Merkle accumulator; the byte-exact encoding + test vectors
// are the P0b plan's deliverable.
package accumulator

import "github.com/sentioxyz/arbiter"

// Proof is an opaque encoded non-membership proof. Its byte encoding is
// frozen by the P0b plan and carried in SubmitStatementCmd.non_membership_proof.
type Proof []byte

// Accumulator is the FSM's spent-ids authenticator (design §3.4).
// ProveNonMembership needs the full element set and runs on the
// leader/prover side OUTSIDE Apply; Apply only calls VerifyNonMembership
// against the in-FSM root (§4.3 red line 4).
type Accumulator interface {
	// Root returns the current spent_ids_root.
	Root() []byte
	// Insert commits a coordinate and advances the root.
	Insert(c arbiter.StatementCoord)
	// ProveNonMembership builds a proof that c is not yet committed.
	ProveNonMembership(c arbiter.StatementCoord) (Proof, error)
	// VerifyNonMembership deterministically checks a carried proof; the only
	// form used inside Apply.
	VerifyNonMembership(c arbiter.StatementCoord, p Proof) bool
}
```

`orchestrator/orchestrator.go`:

```go
// Package orchestrator defines the leader-only side-effect seam (design
// §3.2, §3.4): the orchestrator watches FSM transitions, performs ALL I/O
// (dispatch ReplayJobs, collect attestations, poll parts, anchor, sign and
// send promotions), and feeds every result back into the Raft log as a
// proposed command. It never mutates FSM state directly and is never the
// promotion authority — the three-way verdict lives inside the FSM.
package orchestrator

import "context"

// Orchestrator runs on the elected leader. On acquiring leadership an
// implementation first scans committed FSM state to rebuild its work set,
// then drains the leader-local transition channel; every side effect must
// be idempotent or gated by FSM-recorded progress (§10.2, §10.3).
type Orchestrator interface {
	Run(ctx context.Context) error
}
```

`raftnode/consensus.go`:

```go
// Package raftnode wires hashicorp/raft. ConsensusNode is the consensus
// seam (design §3.4) so the raft library stays swappable and testable.
package raftnode

import (
	"time"

	"github.com/hashicorp/raft"
)

// ConsensusNode is the narrow consensus surface the Arbiter uses.
type ConsensusNode interface {
	// Apply proposes an encoded RaftCommand.
	Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture
	// VerifyLeader confirms leadership before every orchestrator side effect
	// (defends against an old leader during a partition, §10.2).
	VerifyLeader() error
	// LeaderCh signals leadership acquisition/loss (starts/stops the
	// orchestrator, §10.1).
	LeaderCh() <-chan bool
	// Barrier is the read-index gate for linearizable SafeState reads (§11.3).
	Barrier(timeout time.Duration) error
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `go mod tidy && go build ./... && go test ./... && go vet ./...`
Expected: all PASS/exit 0 (accumulator/orchestrator/raftnode compile; root package tests green).

- [ ] **Step 8: Commit**

```bash
git add -A
git commit -m "feat: module bootstrap + frozen P0 interface seams (design §3.4)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: `authority` — promotion-command signing payload (TDD)

**Files:**
- Create: `authority/payload.go`, `authority/signer.go`, `authority/validator.go`, `authority/authority_test.go`
- Repo: `/Users/uranuswch/src/sentio_xyz/arbiter`

**Interfaces:**
- Consumes: `arbiter.PromoteSafePartition` / `arbiter.UnsafeCleanup` / `arbiter.PartRef` (Task 7); `replay.CanonicalDigest` (Task 6); `github.com/ethereum/go-ethereum/crypto`.
- Produces: `authority.Signer` (`NewSignerFromHex(hex string) (*Signer, error)`, `Address() string`, `SignPromotion(cmd arbiter.PromoteSafePartition) (string, error)`, `SignCleanup(cmd arbiter.UnsafeCleanup) (string, error)`) and `authority.Validator` (`AllowedAddresses map[string]bool`, `MaxTokenAge time.Duration`; `AuthorizePromotion(cmd arbiter.PromoteSafePartition, token string) (addr string, err error)`, `AuthorizeCleanup(cmd arbiter.UnsafeCleanup, token string) (string, error)`). These satisfy the design §3.4 `PromotionSigner`/`PromotionValidator` seams; the P1 orchestrator and the SNode-side executor consume them.

This is the §8.1 two-signature scheme's secp256k1 half: JWS compact serialization, header `{"alg":"ES256K","typ":"JWT"}`, keccak256(signingInput) signed with V+27 recovery — byte-compatible with housegate `pkg/auth`'s scheme but with a new purpose-claim payload (`JWSPayload`/`JWSPeerPayload` are SQL-/peer-login-bound and deliberately not reused).

- [ ] **Step 1: Write the failing tests** (`authority/authority_test.go`)

```go
package authority

import (
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter"
)

// Throwaway test key (never provision): secp256k1.
const testKeyHex = "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"

func testCmd() arbiter.PromoteSafePartition {
	return arbiter.PromoteSafePartition{
		TableID:            "tbl-1",
		PartitionID:        "202607",
		PromotionSeq:       9,
		BaseSafeSnapshotID: "snap-1",
		BasePartitionRoot:  "0xdead",
		CandidateParts: []arbiter.PartRef{
			{TableID: "tbl-1", PartitionID: "202607", PartRowLtHash: "0xbeef"},
		},
	}
}

func newTestPair(t *testing.T) (*Signer, *Validator) {
	t.Helper()
	s, err := NewSignerFromHex(testKeyHex)
	if err != nil {
		t.Fatalf("NewSignerFromHex: %v", err)
	}
	v := &Validator{
		AllowedAddresses: map[string]bool{strings.ToLower(s.Address()): true},
		MaxTokenAge:      time.Minute,
	}
	return s, v
}

func TestPromotionSignAuthorizeRoundTrip(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.SignPromotion(testCmd())
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	addr, err := v.AuthorizePromotion(testCmd(), token)
	if err != nil {
		t.Fatalf("AuthorizePromotion: %v", err)
	}
	if !strings.EqualFold(addr, s.Address()) {
		t.Fatalf("recovered %s, want signer %s", addr, s.Address())
	}
}

func TestTamperedCommandRejected(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.SignPromotion(testCmd())
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	evil := testCmd()
	evil.CandidateParts = append(evil.CandidateParts, arbiter.PartRef{
		TableID: "tbl-1", PartitionID: "202607", PartRowLtHash: "0xevil",
	})
	if _, err := v.AuthorizePromotion(evil, token); err == nil {
		t.Fatal("tampered command accepted")
	}
}

func TestCleanupTokenCannotAuthorizePromotion(t *testing.T) {
	// Domain separation: a cleanup signature over field-identical content
	// must not authorize a promotion (distinct CanonicalDigest domains).
	s, v := newTestPair(t)
	cleanup := arbiter.UnsafeCleanup{
		TableID: "tbl-1", PartitionID: "202607", PromotionSeq: 9,
		Parts: []arbiter.PartRef{{TableID: "tbl-1", PartitionID: "202607", PartRowLtHash: "0xbeef"}},
	}
	token, err := s.SignCleanup(cleanup)
	if err != nil {
		t.Fatalf("SignCleanup: %v", err)
	}
	promote := arbiter.PromoteSafePartition{
		TableID: "tbl-1", PartitionID: "202607", PromotionSeq: 9,
		CandidateParts: []arbiter.PartRef{{TableID: "tbl-1", PartitionID: "202607", PartRowLtHash: "0xbeef"}},
	}
	if _, err := v.AuthorizePromotion(promote, token); err == nil {
		t.Fatal("cleanup token authorized a promotion")
	}
}

func TestNonAllowlistedSignerRejected(t *testing.T) {
	s, _ := newTestPair(t)
	v := &Validator{
		AllowedAddresses: map[string]bool{"0x0000000000000000000000000000000000000001": true},
		MaxTokenAge:      time.Minute,
	}
	token, err := s.SignPromotion(testCmd())
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	if _, err := v.AuthorizePromotion(testCmd(), token); err == nil {
		t.Fatal("non-allowlisted signer accepted")
	}
}

func TestStaleTokenRejected(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.signPromotionAt(testCmd(), time.Now().Add(-10*time.Minute).Unix())
	if err != nil {
		t.Fatalf("signPromotionAt: %v", err)
	}
	if _, err := v.AuthorizePromotion(testCmd(), token); err == nil {
		t.Fatal("stale token accepted")
	}
}

func TestWrongPurposeRejected(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.signWithPurpose(testCmd(), "peer-relay")
	if err != nil {
		t.Fatalf("signWithPurpose: %v", err)
	}
	if _, err := v.AuthorizePromotion(testCmd(), token); err == nil {
		t.Fatal("wrong-purpose token accepted")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./authority/...`
Expected: FAIL — `undefined: NewSignerFromHex` etc.

- [ ] **Step 3: Write `authority/payload.go`**

```go
// Package authority implements the Arbiter's secp256k1 command-signing
// scheme (design §8.1): the single authority key (shared across Raft nodes,
// leader-only use) signs PromoteSafePartition / UnsafeCleanup as a JWS whose
// payload is purpose-claim domain-separated from housegate's query and
// peer-login JWS families; SNode authorizes by address recovery against an
// allowlist (the pkg/auth EthValidator pattern).
package authority

import (
	"fmt"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
)

// PromotionPurpose is the JWSCommandPayload.Purpose value for
// Arbiter-authority command tokens. Domain-separated from housegate's query
// JWS (no purpose claim) and peer-relay JWS (purpose "peer-relay") so no
// token family can be replayed as another.
const PromotionPurpose = "arbiter-promotion"

// CanonicalDigest domains — one per command kind, so a cleanup signature can
// never authorize a promotion over field-identical content.
const (
	promoteCommandDomain = "arbiter-promote-command-v1"
	cleanupCommandDomain = "arbiter-cleanup-command-v1"
)

// JWSCommandPayload is the signed payload. CmdHash binds the token to one
// exact command via replay.CanonicalDigest over the canonical Go struct
// (never re-encoded proto bytes, design §4.3).
type JWSCommandPayload struct {
	Iat     int64  `json:"iat"`
	Purpose string `json:"purpose"`
	CmdHash string `json:"cmd_hash"`
}

func promoteCommandHash(cmd arbiter.PromoteSafePartition) (string, error) {
	h, err := replay.CanonicalDigest(promoteCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash promote command: %w", err)
	}
	return h, nil
}

func cleanupCommandHash(cmd arbiter.UnsafeCleanup) (string, error) {
	h, err := replay.CanonicalDigest(cleanupCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash cleanup command: %w", err)
	}
	return h, nil
}
```

- [ ] **Step 4: Write `authority/signer.go`**

```go
package authority

import (
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter"
)

type jwsHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// Signer holds the Arbiter authority secp256k1 key. The key is provisioned
// to every Raft node but USED only by the verified leader (design §8.1,
// §10.2) — that discipline lives in the orchestrator, not here.
type Signer struct {
	privateKey *ecdsa.PrivateKey
	address    string
}

// NewSignerFromHex loads a 32-byte secp256k1 private key from hex (with or
// without 0x prefix).
func NewSignerFromHex(privateKeyHex string) (*Signer, error) {
	key, err := crypto.HexToECDSA(strings.TrimPrefix(privateKeyHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("parse authority key: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	return &Signer{privateKey: key, address: addr}, nil
}

// Address returns the lowercase 0x-prefixed authority address.
func (s *Signer) Address() string { return s.address }

// SignPromotion produces the JWS compact token for one promotion command.
func (s *Signer) SignPromotion(cmd arbiter.PromoteSafePartition) (string, error) {
	return s.signPromotionAt(cmd, time.Now().Unix())
}

// SignCleanup produces the JWS compact token for one cleanup command.
func (s *Signer) SignCleanup(cmd arbiter.UnsafeCleanup) (string, error) {
	h, err := cleanupCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: time.Now().Unix(), Purpose: PromotionPurpose, CmdHash: h})
}

func (s *Signer) signPromotionAt(cmd arbiter.PromoteSafePartition, iat int64) (string, error) {
	h, err := promoteCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: PromotionPurpose, CmdHash: h})
}

// signWithPurpose exists for negative tests (wrong-purpose tokens).
func (s *Signer) signWithPurpose(cmd arbiter.PromoteSafePartition, purpose string) (string, error) {
	h, err := promoteCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: time.Now().Unix(), Purpose: purpose, CmdHash: h})
}

// signPayload builds the ES256K JWS compact serialization: keccak256 over
// base64url(header) + "." + base64url(payload), signed with the Ethereum
// V+27 recovery convention — byte-compatible with housegate pkg/auth.
func (s *Signer) signPayload(payload JWSCommandPayload) (string, error) {
	headerJSON, err := json.Marshal(jwsHeader{Alg: "ES256K", Typ: "JWT"})
	if err != nil {
		return "", fmt.Errorf("marshal header: %w", err)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign command token: %w", err)
	}
	sig[64] += 27
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
```

- [ ] **Step 5: Write `authority/validator.go`**

```go
package authority

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter"
)

// clockSkewToleranceSeconds mirrors housegate pkg/auth.
const clockSkewToleranceSeconds = 5

// Validator authorizes authority command tokens by secp256k1 address
// recovery against an allowlist (design §8.1). SNode must never act on a
// command that fails Authorize* (§13).
type Validator struct {
	// AllowedAddresses is the lowercase, 0x-prefixed authority allowlist.
	AllowedAddresses map[string]bool
	// MaxTokenAge caps iat age; promotion re-sends after failover re-sign,
	// so short ages are safe (§10.2).
	MaxTokenAge time.Duration
}

// AuthorizePromotion checks token against the received command and returns
// the recovered authority address.
func (v *Validator) AuthorizePromotion(cmd arbiter.PromoteSafePartition, token string) (string, error) {
	h, err := promoteCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return v.authorize(h, token)
}

// AuthorizeCleanup is AuthorizePromotion for cleanup commands.
func (v *Validator) AuthorizeCleanup(cmd arbiter.UnsafeCleanup, token string) (string, error) {
	h, err := cleanupCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return v.authorize(h, token)
}

func (v *Validator) authorize(wantCmdHash, token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("authority token: want JWS compact form with 3 parts, got %d", len(parts))
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("authority token header: %w", err)
	}
	var header jwsHeader
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return "", fmt.Errorf("authority token header: %w", err)
	}
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return "", fmt.Errorf("authority token: unexpected alg %q", header.Alg)
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("authority token payload: %w", err)
	}
	var payload JWSCommandPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return "", fmt.Errorf("authority token payload: %w", err)
	}
	if payload.Purpose != PromotionPurpose {
		return "", fmt.Errorf("authority token: unexpected purpose %q (want %q)", payload.Purpose, PromotionPurpose)
	}
	if !strings.EqualFold(payload.CmdHash, wantCmdHash) {
		return "", fmt.Errorf("authority token: command hash mismatch")
	}
	now := time.Now().Unix()
	if payload.Iat-now > clockSkewToleranceSeconds {
		return "", fmt.Errorf("authority token issued in the future")
	}
	if v.MaxTokenAge > 0 && now-payload.Iat > int64(v.MaxTokenAge.Seconds())+clockSkewToleranceSeconds {
		return "", fmt.Errorf("authority token expired")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("authority token signature: %w", err)
	}
	if len(sig) != 65 {
		return "", fmt.Errorf("authority token signature: want 65 bytes, got %d", len(sig))
	}
	recSig := make([]byte, 65)
	copy(recSig, sig)
	if recSig[64] >= 27 {
		recSig[64] -= 27
	}
	signingInput := parts[0] + "." + parts[1]
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(signingInput)), recSig)
	if err != nil {
		return "", fmt.Errorf("recover authority address: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex())
	if len(v.AllowedAddresses) > 0 && !v.AllowedAddresses[addr] {
		return "", fmt.Errorf("authority address %s not in allowlist", addr)
	}
	return addr, nil
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go mod tidy && go test ./authority/... -v`
Expected: all 6 tests PASS (`go mod tidy` pulls `github.com/ethereum/go-ethereum`).

- [ ] **Step 7: Full-module check + commit**

Run: `go build ./... && go test ./... && go vet ./...`
Expected: exit 0.

```bash
git add -A
git commit -m "feat(authority): secp256k1 promotion-command JWS with purpose-claim domain separation

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: conformance test — proto ⇄ `pkg/replay` wire-name parity

**Files:**
- Create: `conformance/replay_wire_test.go`
- Repo: `/Users/uranuswch/src/sentio_xyz/arbiter`

**Interfaces:**
- Consumes: `housegate/housegate/pkg/replay` Go types; `pb` generated messages (Task 1).
- Produces: the mechanical freeze-guard: if either side renames/adds/drops a mirrored field, this test fails. Run it whenever either module bumps.

- [ ] **Step 1: Write the test** (`conformance/replay_wire_test.go`)

```go
// Package conformance pins the arbiter-proto wire mirror to housegate
// pkg/replay: field sets must stay identical (the FSM converts proto ⇄ Go
// losslessly and hashes the Go form via replay.CanonicalDigest).
package conformance

import (
	"encoding/json"
	"reflect"
	"sort"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func goJSONFieldNames(t *testing.T, v any) []string {
	t.Helper()
	rt := reflect.TypeOf(v)
	names := make([]string, 0, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		tag := rt.Field(i).Tag.Get("json")
		if tag == "" || tag == "-" {
			t.Fatalf("%s.%s has no json tag", rt.Name(), rt.Field(i).Name)
		}
		names = append(names, strings.Split(tag, ",")[0])
	}
	sort.Strings(names)
	return names
}

func protoFieldNames(m proto.Message) []string {
	fields := m.ProtoReflect().Descriptor().Fields()
	names := make([]string, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		names = append(names, string(fields.Get(i).Name()))
	}
	sort.Strings(names)
	return names
}

func TestReplayWireTypesMirrorPkgReplay(t *testing.T) {
	cases := []struct {
		name  string
		goVal any
		msg   proto.Message
	}{
		{"Statement", replay.Statement{}, &pb.Statement{}},
		{"ReplayJob", replay.ReplayJob{}, &pb.ReplayJob{}},
		{"PartitionCommitment", replay.PartitionCommitment{}, &pb.PartitionCommitment{}},
		{"PartManifestEntry", replay.PartManifestEntry{}, &pb.PartManifestEntry{}},
		{"ExecutionReceipt", replay.ExecutionReceipt{}, &pb.ExecutionReceipt{}},
		{"ReplayAttestation", replay.ReplayAttestation{}, &pb.ReplayAttestation{}},
		{"TableManifest", replay.TableManifest{}, &pb.TableManifest{}},
		{"SafeSnapshotManifest", replay.SafeSnapshotManifest{}, &pb.SafeSnapshotManifest{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			goNames := goJSONFieldNames(t, tc.goVal)
			pbNames := protoFieldNames(tc.msg)
			if !reflect.DeepEqual(goNames, pbNames) {
				t.Fatalf("field sets diverged:\n  pkg/replay: %v\n  proto:      %v", goNames, pbNames)
			}
		})
	}
}

func TestProtoAcceptsPkgReplayJSON(t *testing.T) {
	att := replay.ReplayAttestation{
		ReplicaID: "replica-2",
		Receipt: replay.ExecutionReceipt{
			BlockSeq:           7,
			PrevSafeSnapshotID: "snap-1",
			PrevStateRoot:      "0x11",
			SchemaSnapshotID:   "schema-1",
			ExecutorProfileID:  "ch-26.x-pinned",
			StatementRoot:      "0x22",
			PayloadRoot:        "0x33",
			SourceClaimRoot:    "0x44",
			ComputedStateRoot:  "0x44",
			MatchSourceRoot:    true,
			PartitionCommitmentsAfter: []replay.PartitionCommitment{
				{TableID: "tbl-1", PartitionID: "202607", Root: "0x55"},
			},
			AffectedParts: []replay.PartManifestEntry{{
				TableID: "tbl-1", PartitionID: "202607", PartName: "202607-b7-s1",
				PartPhysHash: "0x66", PartRowLtHash: "0x77",
				RowCount: 3, Bytes: 128, StorageRefs: []string{"s3://x"},
			}},
			ReplayLogHash: "0x88",
		},
		ReceiptHash:     "0x99",
		Signature:       "aa",
		MatchSourceRoot: true,
	}
	raw, err := json.Marshal(att)
	if err != nil {
		t.Fatalf("marshal pkg/replay attestation: %v", err)
	}
	var msg pb.ReplayAttestation
	// Default UnmarshalOptions reject unknown fields — a pkg/replay field
	// with no proto counterpart fails here.
	if err := protojson.Unmarshal(raw, &msg); err != nil {
		t.Fatalf("protojson rejected pkg/replay JSON: %v", err)
	}
	if msg.GetReceipt().GetComputedStateRoot() != "0x44" {
		t.Fatalf("computed_state_root: got %q", msg.GetReceipt().GetComputedStateRoot())
	}
	if msg.GetReceipt().GetAffectedParts()[0].GetBytes() != 128 {
		t.Fatalf("affected_parts[0].bytes: got %d", msg.GetReceipt().GetAffectedParts()[0].GetBytes())
	}
	if msg.GetReceipt().GetPartitionCommitmentsAfter()[0].GetRoot() != "0x55" {
		t.Fatalf("partition_commitments_after[0].root: got %q", msg.GetReceipt().GetPartitionCommitmentsAfter()[0].GetRoot())
	}
}
```

- [ ] **Step 2: Run the test**

Run: `go mod tidy && go test ./conformance/... -v`
Expected: PASS. If `TestReplayWireTypesMirrorPkgReplay` fails, the fix is in `arbiter-proto/proto/replay.proto` (rename to match pkg/replay, regenerate, retag) — never in pkg/replay.

- [ ] **Step 3: Commit**

```bash
git add -A
git commit -m "test(conformance): pin proto wire mirror to housegate pkg/replay field names

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push -u origin main
```

---

## Acceptance (§13 tripwires relevant to P0)

- One canonicalization profile: the only hash entry points in the arbiter repo are `replay.CanonicalDigest` (authority command hashes) — grep for `sha256\.` / `blake3\.` in the arbiter repo must return nothing outside vendored deps.
- Promotion is authorized only by secp256k1 recovery against an allowlist (`authority.Validator`); no code path constructs an unsigned `PromotionCommand`.
- No ZooKeeper anywhere; no per-row `_hg_l3_block_seq`/`_hg_statement_seq` column in any proto message (time-travel reads are manifest-indexed: `GetManifestByBlock`).
- `ExecutionReceipt.match_source_root` is documented as advisory in the proto; the FSM comment trail (raftlog.proto `RecordAttestationCmd`) states check 1 is recomputed.

## Follow-up plans (not in this plan)

1. **P0b — accumulator**: mountain-range Merkle accumulator implementation + frozen byte encoding + cross-implementation test vectors (design §6, base-spec §14); fills `accumulator.Accumulator` and the `SubmitStatementCmd.non_membership_proof` encoding.
2. **P1a — fsm + raftnode**: the replicated FSM (Apply/Snapshot/Restore over `RaftCommand`), deterministic source/verifier selection, three-way predicate, hashicorp/raft wiring behind `ConsensusNode`.
3. **P1b — orchestrator + gRPC server + cmd/arbiter**: leader-only loop, subscribe streams, NotLeader handling, config section (design Open Question 8), Bazel decision, and the arbiter repo's CI (a minimal `go test` workflow needs user-provisioned private-module credentials — explicitly deferred from P0 by user decision, 2026-07-03; until then the conformance freeze-guard runs only on manual `go test`).
4. **P1c — data-plane counterparts** (SNode promotion executor, Verifier byte-side scanner) — likely in sentio-node / housegate repos.
