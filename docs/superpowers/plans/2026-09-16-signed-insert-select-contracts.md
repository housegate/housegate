# Signed INSERT ... SELECT Contracts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship disabled-by-default snapshot-query wire, signature, profile and SQL-analysis libraries with frozen cross-repository vectors.

**Architecture:** Introduce separate v3 records and RPCs instead of changing v2 payload interpretation. Rewriter backends own a closed AST profile and acknowledge the reserved profile; Housegate owns canonical records and signature checking. Consumers can test these libraries before any source execution or network activation exists.

**Tech Stack:** Go, ES256K JWS, protobuf, polyglot FFI, ClickHouse C++ AST, Bazel, CMake/gtest.

**Spec:** [Signed INSERT ... SELECT design](../specs/2026-09-16-signed-insert-select-design.md), D1–D6 and D10; [plan index and repository aliases](2026-09-16-signed-insert-select.md).

## Global Constraints

- All [index constraints](2026-09-16-signed-insert-select.md#global-constraints) apply. This plan is proposed, with no implementation steps completed.
- Preserve `housegate-statement-v2`, `clickhouse-native-data-v1`, INSERT numeric `1`, and all legacy JWS/L3/receipt bytes.
- New JWS purpose is `housegate-statement-v3`; discriminator is `snapshot_query`; canonical hashing uses `replay.CanonicalDigest` with the spec's new domains.
- New canonical arrays are explicit `[]`; unknown variants, mixed payload fields, unbound AST nodes and unacknowledged profiles fail closed.
- A reserved `(executor_profile_id, query_profile_id)` pair authorizes final analysis and signing. Installation of an older profile is not admission authority.
- Column/type/row interpretation remains in Housegate; the rewriter analyzes SQL and produces SQL, not Go-evaluated rows.

---

## File map

| Owner | Create | Modify |
|---|---|---|
| HG wire | `pkg/replay/snapshot_query_types.go`, `pkg/replay/snapshot_query_hash.go`, `pkg/replay/snapshot_query_hash_test.go`, `pkg/replay/testdata/snapshot_query_v1.json` | `pkg/replay/BUILD.bazel` |
| AP wire | New messages in existing proto files; `testdata/snapshot_query_v1.json` | `proto/arbiter.proto`, `proto/replay.proto`, `proto/raftlog.proto`, matching `gen/pb` files |
| HG auth | `pkg/auth/statement_v3.go`, `pkg/auth/statement_v3_test.go`, `pkg/auth/statement_v3_vectors_test.go` | `pkg/auth/BUILD.bazel` |
| AC wire | `wire/snapshot_query.go`, `wire/snapshot_query_test.go`, `conformance/snapshot_query_wire_test.go` | `wire/command.go`, `wire/BUILD.bazel`, `conformance/BUILD.bazel` |
| RP | New messages/RPCs in existing proto | `proto/rewriter.proto`, `gen/pb/rewriter.pb.go`, `gen/pb/rewriter_grpc.pb.go` |
| HG A4 identity/tool | `pkg/replay/snapshot_query_build_identity.go`, `pkg/replay/snapshot_query_build_identity_test.go`, `pkg/replay/testdata/snapshot_query_build_identity_v1.json`; `tools/snapshot-query-profile/main.go`, `tools/snapshot-query-profile/main_test.go`, `tools/snapshot-query-profile/README.md`, `tools/snapshot-query-profile/BUILD.bazel` | `pkg/replay/BUILD.bazel` |
| RG | `snapshot_query.go`, `snapshot_query_service_test.go`, `snapshot_query_profiles.go`, `snapshot_query_profiles_test.go`, `internal/engine/snapshot_query.go`, `internal/engine/snapshot_query_nodes.go`, `internal/engine/snapshot_query_test.go`, `internal/harness/snapshot_query_golden_test.go`, `internal/harness/testdata/snapshot_query_cases.json` | `service.go`, `native.go`, dependency pins and build metadata |
| RC | `src/handlers/snapshot_query.h`, `src/handlers/snapshot_query.cc`, `tests/snapshot_query_test.cc`, `tests/testdata/snapshot_query_cases.json` | `src/rewriter-server.h`, `src/rewriter-server.cc`, `src/rewriter-server-main.cc`, `CMakeLists.txt`, `tests/CMakeLists.txt`, `third_party/rewriter-proto` pin |
| HG wrapper | `pkg/rewriter/snapshot_query.go`, `pkg/rewriter/snapshot_query_test.go` | `pkg/rewriter/backend.go`, `pkg/rewriter/backend_test.go`, `pkg/rewriter/types.go`, `pkg/rewriter/BUILD.bazel`, dependency pins |

### Task A1: Freeze separate wire records and canonical bytes

**Files:** HG/AP/AC wire rows above. Commit AP generated messages first, HG canonical records second, AC conversion/conformance third; link their PRs and pin the tested commits.

**Interfaces:** Produce the following `replay` records and functions; the field lists below specify JSON order, snake-case tags and protobuf order for each **new** message. `string`, `uint32` and `uint64` have their usual Go/proto meanings. Reuse existing `PartitionCommitment` without changing it; `SnapshotReadPart` deliberately has no `storage_refs` field.

| New type | Fields in canonical order |
|---|---|
| `SnapshotPin` | `network_id string`, `keeper_shard_id uint32`, `snapshot_id string`, `safe_block_seq uint64`, `manifest_root string`, `state_root string`, `schema_snapshot_id string`, `schema_root string` |
| `SnapshotReadPart` | `table_id string`, `partition_id string`, `part_name string`, `part_phys_hash string`, `part_row_lthash string`, `row_count uint64`, `bytes uint64` |
| `SnapshotReadTable` | `database string`, `table string`, `table_id string`, `schema_hash string`, `partition_roots []PartitionCommitment`, `active_parts []SnapshotReadPart` |
| `SnapshotReadSet` | `read_snapshot SnapshotPin`, `tables []SnapshotReadTable` |
| `SnapshotQueryBinding` | `envelope_version uint32`, `input_kind string`, `client_account string`, `statement_id string`, `statement_kind uint32`, `network_id string`, `keeper_shard_id uint32`, `sql_hash string`, `settings_hash string`, `target_table_id string`, `schema_hash string`, `row_id_profile_id string`, `client_revision uint32`, `read_snapshot SnapshotPin`, `read_set_root string`, `schema_snapshot_id string`, `schema_root string`, `logical_database string`, `query_profile_id string`, `executor_profile_id string`, `reservation_id string`, `fencing_generation uint64` |
| `SnapshotQueryInput` | `binding SnapshotQueryBinding`, `sql string`, `read_set SnapshotReadSet` |
| `SnapshotQueryEnvelope` | `input SnapshotQueryInput`, `input_root string`, `user_jws string` |
| `SnapshotQueryReservation` | `reservation_id string`, `fencing_generation uint64`, `client_account string`, `statement_id string`, `read_snapshot SnapshotPin`, `executor_profile_id string`, `query_profile_id string`, `activation_id string` |
| `SnapshotQueryReservationStatus` | `version uint32`, `found bool`, `state string`, `request_id string`, `client_account string`, `statement_id string`, `fencing_generation uint64`, `reservation *SnapshotQueryReservation`, `block_seq uint64`, `terminal_proof []byte` |
| `SnapshotQueryStatement` | `statement_seq uint64`, `envelope SnapshotQueryEnvelope` |
| `SnapshotQueryJob` | `block_seq uint64`, `prev_safe_snapshot_id string`, `prev_state_root string`, `schema_snapshot_id string`, `executor_profile_id string`, `query_profile_id string`, `reservation SnapshotQueryReservation`, `statement SnapshotQueryStatement`, `source_claim_root string`, `source_claim *SnapshotQueryClaim` (C5) |
| `SnapshotQueryEvidence` | `execution_outcome string`, `output_row_count uint64`, `output_rows_root string` |
| `SnapshotQueryReceipt` | `block_seq uint64`, `statement_root string`, `input_root string`, `read_set_root string`, `read_snapshot SnapshotPin`, `schema_snapshot_id string`, `executor_profile_id string`, `query_profile_id string`, `reservation_id string`, `fencing_generation uint64`, `execution_outcome string`, `abort_record_root string`, `output_row_count uint64`, `output_rows_root string`, `source_claim_root string`, `computed_state_root string`, `match_source_root bool`, `partition_commitments_after []PartitionCommitment`, `affected_parts []PartManifestEntry`, `replay_log_hash string` |
| `SnapshotQueryAttestation` | `replica_id string`, `receipt SnapshotQueryReceipt`, `receipt_hash string`, `signature string` |
| `SnapshotQuerySubmitResult` | `admission_code uint32`, `message string`, `statement_seq uint64`, `block_seq uint64`, `source_node string`, `input_root string`, `reservation SnapshotQueryReservation` |
| `SnapshotQueryStatus` | `version uint32`, `found bool`, `accepted SnapshotQuerySubmitResult`, `lifecycle string`, `execution_outcome string`, `terminal_proof []byte` |

`SnapshotQueryBinding` is nested identically in the canonical input and JWS; this nesting is an implementation choice that becomes frozen in this task. `client_account` must equal the canonical account in `statement_id` and the authorized signer identity. Duplicate network/schema/pin fields must agree. Add validation before normalization so a duplicate table/part cannot disappear during sorting. A well-formed unnormalized input may be canonicalized before signing; reject noncanonical transport bytes when checking a signed input.

Freeze the complete profile, control, artifact-readiness, abort and claim records specified in A4 and C1–C5 in this contract task as well; those tasks implement their behavior. A1's exact artifact/part records, normalization, hash domain and frozen populated/empty vectors are normative in the [artifact-set commitment](../specs/2026-09-16-signed-insert-select-schema-semantics-design.md#artifact-set-commitment); B1/C1 must not redefine them. This prevents B/C consumers from inventing incompatible local records. `SnapshotQueryStatus.version` starts at `1`; `found=false` is an authenticated negative lookup, never inferred from a zero-valued accepted result. `terminal_proof` carries C3's authenticated applied/abort record and retention authorization; it is not included in any user-input hash. A source constructing an in-process execution request before claiming has no `source_claim`; a dispatched verifier job requires the complete committed claim and checks its digest. Do not compare a compound claim digest directly with a computed state root.

`SnapshotQueryReservationStatus.version` also starts at `1`; C2 defines its exact `draining`, `granted`, `consumed`, and `released` states and authenticated `found=false` response. Its optional reservation is absent before grant; an empty grant is never a negative lookup. Release/cancellation tombstones survive restart and prevent a delayed acquire from recreating canceled work. This separate control-result message does not change the ordered `SnapshotQueryReservation` or any signed v3 binding. Add fixtures for every state, absent request, lost acquire/release response and tombstone round trips; the status is not part of the user-input hash.

Freeze the ancillary `GetSnapshotQueryStatusRequest` transport fields as `network_id string = 1`, `keeper_shard_id uint32 = 2`, `client_account string = 3`, `statement_id string = 4`, `expected_input_root string = 5`, and append `expected_user_jws_hash string = 6`, preserving tags 1–5. The required tag-6 value is `replay.DigestString(originalUserJWS)`: `0x`-prefixed SHA-256 over the exact UTF-8 bytes of the original compact JWS, without canonicalization, re-signing or full JWS transport in this lookup. Input roots exclude JWS bytes, so two valid signatures over the same bound input are distinct status identities. C3 compares the requested hash against the committed original JWS hash and rejects mismatch as identity conflict, never as `found=false` or an older accepted result. Add omitted/wrong hash, two-valid-signature, lost-rejection-then-lookup and exact-original-JWS retry fixtures to AP/AC wire conformance; existing v2 and C2 reservation-control status semantics are unchanged.

Ancillary service ownership is explicit: `SourceClaims` owns `RecordSnapshotArtifactReady` and query claim writes; `SafeState` remains read-only. `PublishedSnapshot` transports manifest, activation, artifact-ready submission and `terminal_proof`; historical `QueryPolicyStatus` transports found, activation and `terminal_proof`. Positive published/history responses require authenticated committed-record evidence, not a self-consistent hash, boolean or empty proof. C1 freezes/verifies the proof encoding and fresh authority-read behavior; C1/C3/D3 implement the missing role authentication and runtime credential configuration. The existing authority JWS cryptographic pattern is reusable, but its promotion purpose does not authenticate these new commands.

Produce `ValidateSnapshotQueryInput(in SnapshotQueryInput) error`, `CanonicalSnapshotQueryInput(in SnapshotQueryInput) (SnapshotQueryInput, error)`, `SnapshotQueryInputRoot(in SnapshotQueryInput) (string, error)`, `SnapshotQueryReadSetRoot(in SnapshotReadSet) (string, error)`, `SnapshotQueryStatementRoot(st SnapshotQueryStatement) (string, error)` and `SnapshotQueryReceipt.Hash() (string, error)`. All read-set/input roots exclude transport hints and signatures; the statement root includes `statement_seq`, `input_root`, and the **original** `user_jws`. Receipt `affected_parts` uses a canonical projection without storage hints for hashing, while transport may still carry the existing `PartManifestEntry` hints; freeze that projection in the vectors.

- [ ] **Step 1: Add golden tests and the full fixture schema.** Use a deterministic two-table schema, an explicit empty genesis table, one populated snapshot, a constant query with empty reads, a self-insert, a cross-table query, and duplicate/conflicting part cases. `snapshot_query_v1.json` contains cases with `input`, `canonical_input_json`, `input_root`, `read_set_root`, `statement`, `statement_root`, `receipt`, `receipt_hash`, and either `error_contains` or valid expected bytes. The committed fixture includes a full canonical snapshot and schemas so consumers do not invent hashes. The test reads the committed bytes rather than generating expected values:

```go
func TestSnapshotQueryGolden(t *testing.T) {
    raw, err := os.ReadFile("testdata/snapshot_query_v1.json")
    if err != nil { t.Fatal(err) }
    var cases []struct {
        Name string `json:"name"`
        Input SnapshotQueryInput `json:"input"`
        CanonicalJSON string `json:"canonical_input_json"`
        InputRoot string `json:"input_root"`
        ErrorContains string `json:"error_contains"`
    }
    if err := json.Unmarshal(raw, &cases); err != nil { t.Fatal(err) }
    for _, c := range cases {
        t.Run(c.Name, func(t *testing.T) {
            in, err := CanonicalSnapshotQueryInput(c.Input)
            if c.ErrorContains != "" {
                if err == nil || !strings.Contains(err.Error(), c.ErrorContains) { t.Fatalf("got %v", err) }
                return
            }
            if err != nil { t.Fatal(err) }
            b, err := json.Marshal(in)
            if err != nil { t.Fatal(err) }
            if string(b) != c.CanonicalJSON { t.Fatalf("canonical bytes differ: %s", b) }
            root, err := SnapshotQueryInputRoot(in)
            if err != nil || root != c.InputRoot { t.Fatalf("root=%s err=%v", root, err) }
        })
    }
}
```

Add `encoding/json`, `os`, `strings`, `testing` imports. Extend this test with explicit root assertions for the other fixture fields; add reflection coverage that mutates **each binding field**, preserves all other bytes and requires changed roots/rejection. Include unknown JSON keys and mixed fields with zero/empty values: absence is a presence check, not `value != zero`.

- [ ] **Step 2: Run the failing gate.** HG: `bazel test //pkg/replay:replay_test --test_filter=TestSnapshotQuery`. Expect missing new symbols, then behavioral failures until canonicalization and all vectors exist. AP: `make test`; AC: `bazel test //conformance:conformance_test //wire:wire_test` with the prospective pins.

- [ ] **Step 3: Implement the new records, hashing and append-only wire allocations.** New protobuf messages number their fields from `1` in the table's order. Allocate `STATEMENT_KIND_SNAPSHOT_QUERY = 2` (only `0` and `1` existed at inspection). Leave `StatementEnvelopeV2`, old replay `Statement`, old `ReplayJob` and old `ExecutionReceipt` unchanged. Add separate `SubmitSnapshotQuery` and query replay/attestation RPC records. Keep unknown/new variants rejecting on old paths. Add a manifest-projection validator that checks all descriptor parts/partitions against the selected manifest before hashing.

```go
func SnapshotQueryInputRoot(in SnapshotQueryInput) (string, error) {
    in, err := CanonicalSnapshotQueryInput(in)
    if err != nil { return "", err }
    return CanonicalDigest("snapshot-query-input-v1", in)
}

func SnapshotQueryStatementRoot(st SnapshotQueryStatement) (string, error) {
    root, err := SnapshotQueryInputRoot(st.Envelope.Input)
    if err != nil { return "", err }
    if root != st.Envelope.InputRoot { return "", fmt.Errorf("input_root mismatch") }
    return CanonicalDigest("snapshot-query-statement-root-v1", struct {
        StatementSeq uint64 `json:"statement_seq"`
        InputRoot string `json:"input_root"`
        UserJWS string `json:"user_jws"`
    }{st.StatementSeq, root, st.Envelope.UserJWS})
}
```

The profile/control records in C1–C3 use new messages in AP's existing `proto/arbiter.proto`, `proto/replay.proto` and `proto/raftlog.proto`. Reserve Raft oneof fields `18..25` for `begin_snapshot_query`, `grant_snapshot_query`, `release_snapshot_query`, `submit_snapshot_query`, `abort_snapshot_query`, `activate_query_profile`, `record_snapshot_query_claim`, and `record_snapshot_query_attestation`, respectively; fields `1..17` remain unchanged. Recheck these numbers before implementation and allocate new numbers if intervening commits use them; update this allocation ledger and all linked consumers together. The begin/grant split lets prior work drain without network calls inside apply. Further profile/publication proof records belong to C1, with distinct commands and new numbers; do not reuse an allocated tag.

- [ ] **Step 4: Freeze vectors and verify all readers.** Generate the proposed new vectors once with a temporary development generator that calls the canonical functions, independently check the domain prefix/JSON byte string with SHA-256, then commit the inspected fixture. AP: `make proto`, `make lint`, `make test`. HG: `bazel test //pkg/replay:replay_test //pkg/auth:auth_test`. AC: `bazel test //conformance:conformance_test //wire:wire_test`. Require byte-identical old fixture files and a semantic new-wire round trip, including empty arrays and unknown/present-empty payload fields. Do not treat proto field-name equality as semantic coverage.

- [ ] **Step 5: Commit the contracts.** In each owner, stage only this task's file-map entries and generated bindings. Use `feat(replay): define snapshot query input and commitment contracts` for HG, `feat(proto): add versioned snapshot query records` for AP, and `test(wire): freeze snapshot query conformance` for AC. Record the three commit IDs in the implementation PR dependency table.

### Task A2: Sign v3 bindings and independently verify their identity

**Files:** HG auth row; AC `wire/snapshot_query.go` and `conformance/snapshot_query_wire_test.go` for the consumed contract. Do not change the v2 mismatch rules.

**Interfaces:** Add `auth.JWSStatementPayloadV3` with ordered fields `purpose string`, `iat int64`, `binding replay.SnapshotQueryBinding`, `input_root string`; `(*RelaySigner).SignStatementV3(p JWSStatementPayloadV3) (string, error)`; `(*EthValidator).ValidateStatementV3(token string, want JWSStatementPayloadV3) (string, error)`; `DecodeStatementV3Payload(token string) (JWSStatementPayloadV3, error)`; `StatementPayloadV3Mismatch(got, want JWSStatementPayloadV3) string`. Add pure `VerifyStatementV3Signature(token string, want JWSStatementPayloadV3) (account string, err error)` for deterministic replay/FSM use; it checks canonical JWS and recovered account without consulting the wall clock. Admission freshness and ACL checks remain separate caller responsibilities. This package may depend on `replay`; root `replay` must not import `auth` in return.

- [ ] **Step 1: Add a round-trip and every-field tamper test.** Reuse the existing public test key `statementV2TestKey`, with the A1 valid input fixture as the v3 binding. Set `Iat` explicitly in frozen vectors and use `time.Now()` only in freshness tests. Add a small test showing why valid signatures do not grant current-profile authorization:

```go
func TestStatementPayloadV3MismatchBindsQueryProfile(t *testing.T) {
    want := JWSStatementPayloadV3{Purpose: "housegate-statement-v3",
        Binding: replay.SnapshotQueryBinding{QueryProfileID: "active"}, InputRoot: "root"}
    got := want
    got.Binding.QueryProfileID = "historical"
    if field := StatementPayloadV3Mismatch(got, want); field != "query_profile_id" {
        t.Fatalf("mismatch field=%q", field)
    }
}
```

Test v2/ordinary query/peer tokens in the v3 slot, noncanonical protected JSON/base64, recovered-account mismatch, duplicate JSON fields, unknown fields, altered input/read roots, schema/pin/genesis/generation and either profile. `Iat` is excluded from input-root identity but remains subject to ingress freshness policy. Copy no v2 fixture expectations into regenerated files.

- [ ] **Step 2: Run `bazel test //pkg/auth:auth_test --test_filter='TestStatementV3|TestStatementPayloadV3'`.** Expect missing symbols or an accepted tampered field before the implementation.

- [ ] **Step 3: Implement purpose/domain checking using the existing canonical ES256K machinery.** Refactor only genuinely shared protected-header, base64 and signature primitives; retain v2 output bytes. Derive the expected binding from the validated input, recompute its root, and compare every field before authorizing the recovered account.

```go
want := auth.JWSStatementPayloadV3{
    Purpose: "housegate-statement-v3", Binding: envelope.Input.Binding,
    InputRoot: envelope.InputRoot,
}
account, err := auth.VerifyStatementV3Signature(envelope.UserJWS, want)
if err != nil { return err }
if account != envelope.Input.Binding.ClientAccount {
    return fmt.Errorf("client_account does not match signature")
}
```

This call sequence is the common verifier/FSM binding check, not a replacement for C2's active reservation/policy equality. `ValidateStatementV3` layers the existing allowlist/freshness behavior over the pure signature function; historical replay never applies today's freshness window.

- [ ] **Step 4: Run all auth/replay tests and AC conformance.** Require unchanged old signed vectors, valid v3 round trips, deterministic verification under different local times, and a named refusal for every tamper.

- [ ] **Step 5: Commit `feat(auth): sign and validate snapshot query bindings`.** Include HG build metadata and the linked AC fixture-consumer update.

### Task A3: Define the rewriter's versioned analysis and scratch-preparation RPCs

**Files:** RP row; regenerate bindings and update RG/RC test-only consumers before implementing handlers.

**Interfaces:** Add `AnalyzeSnapshotQuery(AnalyzeSnapshotQueryRequest) returns (AnalyzeSnapshotQueryResponse)` and `PrepareSnapshotQuery(PrepareSnapshotQueryRequest) returns (PrepareSnapshotQueryResponse)` to `RewriterService`. The proposed new message fields are:

| Message | Ordered fields and types |
|---|---|
| `SnapshotQueryCatalogTable` | `database string`, `table string`, `table_id string`, `schema_hash string`, `columns repeated SnapshotQueryColumn`; every column has `name string=1`, `type string=2`, `generation SnapshotQueryColumnGeneration=3`, `default_expression string=4` |
| `AnalyzeSnapshotQueryRequest` | `contract_version uint32`, `query_profile_id string`, `sql string`, `logical_database string`, `catalog repeated SnapshotQueryCatalogTable`, `materialize bool`, `inputs MaterializationInputs` |
| `AnalyzeSnapshotQueryResponse` | `contract_version uint32`, `query_profile_id string`, `code SnapshotQueryCode`, `message string`, `sql_after_materialization string`, `target_table_id string`, `target_columns repeated string`, `read_table_ids repeated string` |
| `SnapshotScratchBinding` | `table_id string`, `scratch_database string`, `scratch_table string` |
| `PrepareSnapshotQueryRequest` | `analysis AnalyzeSnapshotQueryRequest`, `bindings repeated SnapshotScratchBinding` |
| `PrepareSnapshotQueryResponse` | `contract_version uint32`, `query_profile_id string`, `code SnapshotQueryCode`, `message string`, `select_sql string`, `target_table_id string`, `target_columns repeated string`, `read_table_ids repeated string` |

`SnapshotQueryCode` is a new enum: `UNSPECIFIED=0`, `SUCCESS=1`, `UNSUPPORTED=2`, `INVALID_INPUT=3`, `PROFILE_UNAVAILABLE=4`, `MATERIALIZATION_FAILED=5`, `NOT_SNAPSHOT_QUERY=6`. Version is exactly `1`. `NOT_SNAPSHOT_QUERY` is allowed only after successful single-statement AST classification proves an ordinary non-snapshot statement; it requires the exact version/profile acknowledgement and empty executable SQL/target/column/read outputs. Parse failures, unknown statement classes and forbidden snapshot-query shapes never use it. Freeze positive ordinary SELECT/USE/payload-INSERT cases and negative leading-WITH/forbidden-query/parse-error cases so D1/D2 cannot mistake an analysis failure for passthrough permission. `materialize=false` forbids inputs and residual volatility for snapshot queries; `materialize=true` requires complete deterministic replacement, including pool availability. Scratch preparation always validates with `materialize=false` and requires an exact one-to-one binding for the returned read set; it never accepts `NOT_SNAPSHOT_QUERY`. An error response cannot carry executable SQL. Caller-provided catalog entries come from authenticated schema resolution; they cannot invent a supported profile.

Append `SnapshotQueryColumnGeneration` with exact values `UNSPECIFIED=0`, `ORDINARY=1`, `DEFAULT=2`, `MATERIALIZED=3`, `ALIAS=4`, `OTHER=5`. The initial profile accepts only explicit `ORDINARY` with empty `default_expression` for every target user column and every column of actually referenced read tables. Missing old-client fields decode as `UNSPECIFIED`; unknown numeric values remain transportable but refuse at runtime. DEFAULT including a constant, MATERIALIZED, ALIAS, OTHER and ORDINARY with a nonempty expression refuse. An ineligible unrelated U remains in the complete ledger without itself invalidating analysis. The exact authenticated producer contract is the [schema-semantics addendum](../specs/2026-09-16-signed-insert-select-schema-semantics-design.md); these RP fields do not self-authenticate caller data or alter legacy schema hashes.

Analysis returns `target_columns` in the INSERT's positional target-list order. Preparation returns it in the authenticated target schema's order and rewrites the outer projection accordingly: `INSERT INTO t (b,a) SELECT x,y` produces schema-ordered output `(y,x)` when schema order is `(a,b)`. The prepared SELECT performs only profile-admitted operations to those exact types; the scratch scanner checks resulting arity and types. Include this permutation in the golden corpus so B2's `QueryRows(selectSQL, targetSchema)` cannot silently assign the wrong columns. Under `prepare-integer-literal-exact-v1`, Prepare alone may emit `accurateCast('<canonical_exact_decimal>', '<TargetIntType>')` after Analyze and Prepare losslessly prove the original decimal integer token and exact target range. Under `prepare-integer-scalar-null-throw-v1` plus `cast_keep_nullable='0'`, Prepare alone may unwrap a direct scalar subquery whose underlying integer type already equals the target. Unchecked narrowing, user casts/helpers and every other conversion remain refused.

- [ ] **Step 1: Add message round-trip tests and an unsupported-service test.** The initial client test calls the new RPC on an old/fake server and requires `Unimplemented` to be an admission error. Add a fixture request with `contract_version=1`, a known query-profile identifier, `INSERT INTO tenant.copy SELECT value FROM tenant.events`, and catalog entries for exactly `tenant.copy` and `tenant.events`. Freeze enum numbers and field tags/types/order, distinct nonzero generation/expression values in nested catalog columns, old name/type-only bytes decoding to `UNSPECIFIED` plus empty expression, and preservation of an unknown numeric enum without claiming it is accepted.

```proto
service RewriterService {
  // Add alongside the existing methods; retain them verbatim.
  rpc AnalyzeSnapshotQuery(AnalyzeSnapshotQueryRequest) returns (AnalyzeSnapshotQueryResponse);
  rpc PrepareSnapshotQuery(PrepareSnapshotQueryRequest) returns (PrepareSnapshotQueryResponse);
}
```

- [ ] **Step 2: Run `make test` before regeneration.** The new generated-type test fails to compile. After generation it must still fail if any field disappears in the round trip or an unsupported service is treated as success.

- [ ] **Step 3: Add the messages/RPCs with append-only changes and generated Go bindings.** Keep `Rewrite`, `MaterializeSQL`, `RewriteErrorMessage` and `Optimize` signatures unchanged. Append generation tag 3 and default-expression tag 4 after the existing column fields, retaining every old tag and true gRPC `Unimplemented` default. `SnapshotQueryCatalogTable.columns` has complete schema order; read IDs and scratch bindings have sorted unique table-ID order. Copy the exact request/response field names to the cross-engine corpus schema.

- [ ] **Step 4: Run `make proto`, `make lint`, `make test`, then `make verify` after generated files are staged/committed.** `make verify` regenerates and checks `gen/` cleanliness. Check the RP breaking-change gate against the actual base revision. RC consumes the independent RP repository through `third_party/rewriter-proto`; update that pin, not a divergent hand-edited mirror.

- [ ] **Step 5: Commit `feat(proto): add snapshot query analysis contract`.** Publish a real dependency version after checks; do not insert an invented future version into HG/RG/RC pins.

### Task A4: Implement the closed AST profile with native/gRPC parity

**Files:** HG A4 identity/tool, RG and RC rows. HG supplies the canonical witness and offline bundle generator before either engine consumes them. Both engines implement A3's exact RPC contract; the RG corpus is mirrored byte for byte to RC.

**Interfaces:** HG owns `NativeArtifactV1`, `NativeArtifactSetV1`, `QueryProfileEntryV1` and `QueryProfileFileV1`; it exports `ValidateNativeArtifactSetV1`, `EncodeCanonicalNativeArtifactSetV1`, `NativeArtifactSetV1Digest`, `ValidateQueryProfileFileV1`, `EncodeCanonicalQueryProfileFileV1` and `DecodeCanonicalQueryProfileFileV1`. Q remains the unchanged `QueryProfileRecord.Hash()`. The offline `snapshot-query-profile` CLI requires `-recipe`, `-profile-out` and `-provenance-out`, measures final actual artifacts, and writes a canonical profile plus separate unsigned provenance. RG and RC consume independent literal vectors and never import HG. RG adds `(*Service).AnalyzeSnapshotQuery(ctx context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)` and `PrepareSnapshotQuery` with A3's request/response types; the same signatures are exposed on `NativeRewriter`. RC adds matching service overrides and `doAnalyzeSnapshotQuery`/`doPrepareSnapshotQuery`, delegating to `src/handlers/snapshot_query.cc`. Internal functions consume each engine's own AST; no AST format crosses the RPC.

#### Public A4 serial substages

The labels below are the ordered decomposition of this one Task A4. They do not add tasks, checkboxes or acceptance gates. A substage is incomplete until its named evidence passes; an intermediate checkpoint cannot advertise snapshot-query capability.

| Stage | Public deliverable and gate | Mapping to Task A4 steps |
| --- | --- | --- |
| **A4.1a — HG canonical authority** | HG owns `NativeArtifactV1`/`NativeArtifactSetV1`, the strict external-envelope validation and canonical hash rules, and independent valid/malformed literal vectors without changing the A1 record or vectors. The focused replay gate and separate HG commit below must pass first. | First prerequisite portion of Step 1; A4.1b, A4.1c, A4.2 and A4.3 consume it. |
| **A4.1b — HG offline generator** | A bounded offline tool measures final artifact files and emits the external profile bundle plus separate provenance; it is not a runtime analyzer or native artifact-set member. The tool test/build gate and separate HG commit below must pass before any generated bundle is consumed. | Second prerequisite portion of Step 1; supplies A4.1c/A4.2/A4.3 inputs and Step 4's exact build → measure → bundle ordering. |
| **A4.1c — complete mirrored corpus and registered RED** | Freeze the complete RG corpus, copy it byte-for-byte to RC, register both real test targets, and demonstrate the named capability assertions fail against empty handlers. A missing file or uncompiled test is not RED evidence. | Remaining corpus portion of Step 1 and all of Step 2; it freezes expectations but qualifies no behavior. |
| **A4.2 — RG measured loader** | RG independently validates the envelope and Q, filters to its measured executable/retained sealed FFI tuple, and proves Linux load/lifetime plus ordinary-constructor compatibility. | Native-loader portion of Step 3 and its identity/lifecycle cases in Step 4. |
| **A4.3 — RC measured startup** | RC independently validates the same contract, measures the complete service executable/dependency closure, and refuses unsupported Q at actual service startup. | gRPC-loader/startup portion of Step 3 and its identity/startup cases in Step 4. |
| **A4.4 — RG closed implementation** | The real native engine implements the complete closed AST, original-token literal proof, bounded volatility, scalar typing and exact Prepare contract, and passes the full corpus under its measured test Q. | Native implementation portion of Step 3 and native corpus/refusal portion of Step 4. |
| **A4.5 — independent RC implementation** | The real C++ engine independently implements the same contract on its own AST and passes the byte-identical corpus plus service and ordinary regression gates. | C++ implementation portion of Step 3 and C++ corpus/refusal portion of Step 4. |
| **A4.6 — frozen-artifact parity and measured ClickHouse qualification** | Consolidate the reviewed local engine checkpoints, build and measure final artifacts, regenerate Q/bundles, then run those exact unchanged artifacts for native/gRPC parity and exact Prepare SQL metadata/value/error checks on pinned ClickHouse. Any code or binary change invalidates the affected evidence. | Final integrated portion of Step 4. Step 5's local implementation commits must be consolidated before these final measurements; Step 5 is complete only after A4.6 records both engine digests and the RP pin. Publication/push remains a separate controller action. |

Later A5/B/D role executables repeat measurement with their final role bytes and complete their existing gates; A4 does not substitute analyzer test-binary evidence for those later role measurements.

- [ ] **Step 1: Implement the HG identity prerequisites, then write the full positive/negative corpus before handlers.** A4.1a first creates `pkg/replay/snapshot_query_build_identity.go`, its test and `pkg/replay/testdata/snapshot_query_build_identity_v1.json`, and updates `pkg/replay/BUILD.bazel`. Implement strict `NativeArtifactV1`/`NativeArtifactSetV1` and profile-file validation, canonical encoding/decoding and N hashing through the existing Q record without changing old vectors or domains. Run `bazel test //pkg/replay:replay_test --test_filter=TestSnapshotQueryBuildIdentity --test_output=errors`, then the relevant full `//pkg/replay:replay_test` gate when shared replay code or fixtures are affected. Cover exact canonical bytes, strict profile-file shape/order/duplicate/null/digest refusal, independent vectors, field mutations and caller-input immutability. Commit this prerequisite separately as `feat(replay): add native artifact profile witness`.

Next A4.1b creates only `tools/snapshot-query-profile/main.go`, `main_test.go`, `README.md` and `BUILD.bazel`. Its `-recipe`, `-profile-out` and `-provenance-out` CLI opens and hashes final regular artifact files, derives N and unchanged-record Q through the HG authority, writes canonical profile bytes and a separate deterministic unsigned provenance manifest, and never accepts fake/prepopulated measured hashes, silently adds itself as a native member or advertises runtime capability. Run `bazel test //tools/snapshot-query-profile:snapshot-query-profile_test --test_output=errors` and `bazel build //tools/snapshot-query-profile:snapshot-query-profile`. Cover actual-file measurement, strict recipe and independent canonical vectors, artifact mutations, nonregular input refusal without blocking, profile/provenance outputs aliasing each other or any input through direct, symlink, hardlink, case-variant or actual-parent identity, and staged-output failures. Commit this second prerequisite separately as `feat(replay): generate measured snapshot query profiles`. Both HG commits precede A4.1c, A4.2 and A4.3 and are not folded into later engine checkpoint consolidation.

Then write the corpus. Every case carries `name`, `request`, `expected_response`, and optional `prepare_request`/`expected_prepare_response`. Include the following SQL as separate literal cases, plus the spec's entire A1/A2/A7 syntax family:

```sql
INSERT INTO tenant.copy (value) SELECT value FROM tenant.events WHERE value > 0;
INSERT INTO tenant.copy (value) WITH x AS (SELECT value FROM tenant.events) SELECT value FROM x;
WITH x AS (SELECT value FROM tenant.events) INSERT INTO tenant.copy (value) SELECT value FROM x;
INSERT INTO tenant.copy SELECT 7;
INSERT INTO tenant.copy SELECT e.value FROM tenant.events AS e ALL INNER JOIN tenant.ids AS i ON e.value = i.value;
INSERT INTO tenant.copy SELECT value FROM tenant.events UNION ALL SELECT value FROM tenant.ids;
INSERT INTO tenant.copy SELECT value FROM tenant.events WHERE value IN (SELECT value FROM ordinary.secret);
INSERT INTO tenant.copy WITH unused AS (SELECT * FROM remote('host', 'db', 't')) SELECT 7;
INSERT INTO tenant.copy SELECT value FROM (SELECT value FROM tenant.events LIMIT 1);
```

The last three reject: hidden non-SI source, forbidden syntax in an unused CTE, and nested order-dependent row selection. The `SELECT 7` case keeps that exact SQL and uses an authenticated one-column ORDINARY `Int64` target; its Prepare result must be exact `Int64`, not a bare inferred `UInt8`. Add CTE shadowing and free/correlated variables, both WITH placements, quoted names/comments/heredocs, every target column once, reordered full target list, missing/duplicate target columns, user-reserved row-id references, and all rejected table-function/catalog/UDF carriers. Add equal-name/type cases for explicit ORDINARY, omitted/UNSPECIFIED, unknown, DEFAULT including a constant, MATERIALIZED, ALIAS, OTHER and contradictory ORDINARY-plus-expression metadata. A default-bearing target rejects even with a complete explicit target list; an ineligible referenced source rejects, while an unrelated ineligible U does not.

Freeze literal cases for zero, 7, minimum, maximum and immediate one-past values across `Int8/16/32/64` and `UInt8/16/32/64`, negative-to-unsigned, optional sign, parentheses, output alias, long digits, target permutation and two equal values with different target widths. Prove the original lexeme before parser normalization and preserve it through signed logical regeneration; reject `-9223372036854775809` to `Int64`, `18446744073709551616` to `UInt64`, fractions, exponents, hex/binary, user strings, user `CAST`/`::`/`toInt*`/`accurateCast`, arithmetic wrappers and lost token fidelity. Direct final literals may lower only through the compiler template. CTE/UNION/runtime aliases and `(SELECT 7)` do not inherit literal provenance; add explicit derived mismatch negatives while preserving same-type source-column CTE/UNION positives.

Freeze `scalar_projection`, `scalar_nested_projection`, `scalar_zero_row_shape` and `scalar_one_or_multiple_row_shape` for every integer width where source and target are the same type. Prepare expects the generated same-type `accurateCast` unwrap under `cast_keep_nullable='0'`; evaluated one row returns exact nonnullable T, zero rows/NULL throws, and multiple rows preserves the cardinality error, without pretending analysis can know runtime row counts. Add different width/sign, literal-only inferred mismatch, noninteger, arbitrary alias, missing/unknown/ignored/wrong setting and user-helper negatives. Keep scalar and `IN` predicates unwrapped with SQL three-valued NULL behavior; zero external output and an unevaluated scalar do not require eager evaluation. The full transitive referenced closure still includes nested/dead scalar branches, while an unreferenced CTE remains syntax-validated without adding its bases.

The new test profile admits exact-type column/float copy, integer comparison, boolean composition, `prepare-integer-literal-exact-v1` and `prepare-integer-scalar-null-throw-v1`; its settings include exactly `cast_keep_nullable='0'`. Those versioned entries are compiler capabilities, not user allowed-function names. Other integer conversions/arithmetic stay absent until a later explicit checked operation and overflow vectors pass both engines and pinned ClickHouse. Pure final ORDER BY is optional in the design: initially reject it to keep this profile smaller. Before accepting a known snapshot statement, both engines require the addendum's explicit eligibility for every user column of the target and referenced read tables and preserve the existing AST/unused-definition checks.

Initial volatile materialization accepts only zero-argument `rand()`, `rand32()` and `rand64()` with bounded pools and original function-name/AST binding. Add zero, missing, partial, exhausted and wrong-pool cases, each-occurrence exactly-once replacement, `rand32` range refusal, UInt64 exactness and residual-volatility refusal. `random()`, temporal functions, floating random output and UUID output remain unsupported even when ordinary MaterializeSQL recognizes a broader family; ordinary materialization behavior is unchanged.

- [ ] **Step 2: Run the corpus with empty handlers.** RG: `make ffi`, then `make test`. RC: run only on the documented remote build box (`ssh -p 30100 sentio@64.38.131.242`) or its immutable-commit PR CI lane: `./scripts.sh rebuild`, then fail-closed `ninja -C build rewriter_tests`, then `ctest --test-dir build --output-on-failure`. Follow RC's checkout/host-lock procedure; do not mutate a shared remote checkout concurrently. Require the new `SnapshotQuery.*` cases in `build/tests/rewriter_tests` to be compiled and fail; explicitly add the new `.cc` to `tests/CMakeLists.txt` and handler to root `CMakeLists.txt`. Merely creating a test file is insufficient. No local C++ build is supported, and `./scripts.sh test` is prohibited in the persistent worktree because it takes the clean-build path and discards its cache.

- [ ] **Step 3: Implement scope-aware analysis and exact scratch rewriting.** Reuse RG `Engine.ParseOne`, `Generate`, materialization nodes and table-scope helpers; reuse RC's ClickHouse AST and `visitChildrenWithCTEVisibility`. Validate every AST node, including unreferenced CTE bodies, while collecting the base-table closure only through referenced definitions. Bind each column/reference to its lexical scope and authenticated catalog. Traverse expressions in projections, predicates, joins and nested subqueries. Refuse free variables, unknown nodes/functions, non-SI identities, settings and parameters at every depth. Do not assume `CollectSelectTables` alone proves this closure.

```text
AnalyzeSnapshotQuery(request):
  require version == 1 and installed profile digest == request.query_profile_id
  parse exactly one statement; extract INSERT target and SELECT body for either WITH placement
  if AST proves a known ordinary non-snapshot statement: return acknowledged NOT_SNAPSHOT_QUERY
  parse failure/unknown statement/forbidden snapshot shape: reject; never ordinary passthrough
  require explicit ORDINARY plus empty expression for all target and referenced-table user columns
  validate target schema and complete target-column permutation
  if materialize: replace every allowed volatile occurrence using supplied pools or reject
  walk all definitions and expression nodes against the closed profile
  bind referenced CTEs/aliases/columns with lexical scopes; reject free or correlated references
  prove original integer literal lexemes and exact target ranges before regeneration
  collect, sort and deduplicate resolved base table IDs; reject a missing catalog entry
  return regenerated logical SQL and acknowledged version/profile/target/closure

PrepareSnapshotQuery(request):
  run AnalyzeSnapshotQuery with materialize=false; require success
  require exact bijection between read_table_ids and unique scratch bindings
  rewrite bound base-relation AST nodes only; never textual replacement
  reprove direct literal/scalar output eligibility; generate only the admitted typed wrappers
  return SELECT body with explicit output positions and scratch-only base relations
```

Add a `QueryProfileRecord` with ordered fields `version uint32`, `clickhouse_build_digest string`, `platform string`, `native_analyzer_build_digest string`, `grpc_analyzer_build_digest string`, `tzdata_digest string`, `settings []ProfileSetting`, `scalar_operators []string`, `column_profile_id string`, `output_order_id string`, `limits QueryLimits`. `ProfileSetting` is `{name string, value string}`, sorted uniquely by name; operators are sorted unique names. `QueryLimits` contains the unsigned `uint64` fields named below. A record has no self-referential ID. Its ID remains `CanonicalDigest("snapshot-query-profile-v1", record)`; A1 freezes its canonical JSON and independent digest vector. Both analyzer digests belong to the same profile record, so native and gRPC can acknowledge the same profile ID while verifying their own measured build. The new test record contains sorted unique `prepare-integer-literal-exact-v1` and `prepare-integer-scalar-null-throw-v1` operator entries and `cast_keep_nullable='0'`; adding them changes Q under the existing record hash, without changing old A1/A3 bytes, fields or domains. HG owns these Go canonical records; engines independently verify the published canonical-byte/digest vectors without importing HG, which would create a dependency cycle. The exact digest meanings, witness domain and ordered structures are frozen by the [build-identity addendum](../specs/2026-09-16-signed-insert-select-build-identity-design.md).

Create RG `snapshot_query_profiles.go` and tests with `NewServiceWithSnapshotQueryProfiles(libPath, profilePath string) (*Service, error)`; RC adds `--snapshot-query-profile-file` startup loading beside its existing server options. Both strictly load the addendum's version-1 envelope, recompute Q and the native artifact-set witness, retain immutable entries and advertise only profiles supported by the running measured engine. An unknown or merely present-but-unsupported ID refuses. HG adds `Options.SnapshotQueryProfilePath` and A5's native factory invokes the new constructor when creating a snapshot analyzer. Normal `NewService(libPath)` and ordinary rewriter construction stay compatible.

Engine build digests are taken from the final binaries actually used in B/D tests, not guessed release versions. A4 owns the Linux `/proc/self/exe` measurement, sealed retained memory-file loading of the exact FFI bytes, native witness validation and RC complete-executable/dependency-closure check defined by the addendum; there is no production supplied-hash bypass or hash-then-reopen fallback. Build complete test/role executables first, measure final bytes second, generate the external test/production bundles third and run those exact unchanged binaries. Use a deterministic resource-test profile with `max_sql_bytes=65536`, `max_descriptor_bytes=65536`, `max_output_rows=1048576`, `max_output_bytes=268435456`, `max_restore_bytes=1073741824`, `max_sort_memory_bytes=134217728`, `max_spill_bytes=1073741824`, and `max_execution_ms=30000`; these are proposed initial test/canary limits, not production settings. A changed value or native artifact member produces a new profile ID and must traverse C1's active-policy boundary. Pin exact internal settings in the fixture, including single-thread execution and throw-on-limit behavior; resource exhaustion must never return a truncated successful SELECT.

- [ ] **Step 4: Run parity and rejection tests.** A4.1c freezes and runs the shared RED expectations without advertising capability. A4.4 implements and tests the exact literal/scalar/volatility rules on the real native engine; A4.5 independently does so on the real C++ engine. A4.6 runs both engines' exact Prepare SQL on pinned measured ClickHouse, inspects metadata and values for all integer widths plus evaluated scalar zero/one/many, tests wrong-setting and user-helper refusal, and compares paired responses under the final rebuilt measured Q. RG `make test`; RC uses the same remote/CI incremental build, explicit test-target build and `ctest --test-dir build --output-on-failure` sequence from Step 2, with named cases run through `build/tests/rewriter_tests`; RG harness uses `REWRITER_ORACLE_ADDR` pointed at that tested RC service. Require byte-identical copied corpus files, identical canonical responses and real measured native/gRPC equality, including error categories, target order and closure. Add missing/unknown/ineligible/contradictory generation metadata and equal-legacy-hash semantic drift to the shared corpus, plus independent executable/FFI mutations, tuple order/duplicate/platform errors, canonical witness vectors, sealed-source swap/load races, own-build mismatch, parser-depth/SQL-size failures, failed transport, zero/partial materialization pools, wrong profile and no capability acknowledgement. Membership, loader success and standalone ClickHouse probes are not parity evidence, and no response may fall back to ordinary rewrite. B2/B4/D4 retain exact typed readback, canonical bytes/output-root equality, and no successful output/commit after NULL, cardinality, conversion or partial-stream failure.

- [ ] **Step 5: Consolidate and record one implementation in each engine.** A4.1a and A4.1b already have the separate HG prerequisite commits named in Step 1; do not squash them into either engine. Use `feat(rewriter): analyze pinned snapshot queries` for each engine implementation. Consolidate reviewed engine-local checkpoints before A4.6 builds its final artifacts; after A4.6 passes, record both exact engine digests and the RP pin beside the shared corpus. These are reviewable commits and do not themselves publish, push or activate capability. Unsupported checked arithmetic remains refused until it has its own vectors; do not claim that ordinary ClickHouse casts detect overflow.

### Task A5: Add a fail-closed Housegate analysis wrapper and capability probe

**Files:** HG wrapper row, RG/RP dependency pins and root `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` when changed by the repository's dependency workflow.

**Interfaces:** Add `rewriter.SnapshotQueryAnalyzer` with `AnalyzeSnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)`, `ClassifySnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)` and `PrepareSnapshotQuery(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error)`; `NewSnapshotQueryAnalyzer(opts Options) (SnapshotQueryAnalyzer, error)` returns a wrapper with `Close() error` also on the interface. For native mode this factory invokes A4's measured `NewServiceWithSnapshotQueryProfiles` constructor and supports only its locally advertised entries; for gRPC it wraps one configured endpoint and exact-Q acknowledgement. It does not select or launch historical engines. `ClassifySnapshotQuery` uses the same Analyze RPC and accepts only acknowledged `SUCCESS` or structurally empty `NOT_SNAPSHOT_QUERY`; the final Analyze method and Prepare still require `SUCCESS`. Extend the private `backend` and its fake with A3 methods. Add `ProbeSnapshotQuery(ctx context.Context, analyzer SnapshotQueryAnalyzer, profileID string, catalog []*pb.SnapshotQueryCatalogTable) error` using the A4 known positive, hidden-source, nested-LIMIT, missing-generation and default-bearing-target cases plus the positive ordinary classification and malformed NOT_SNAPSHOT_QUERY response cases. The probe checks engine behavior on supplied metadata; callers still authenticate exact-S artifacts independently.

- [ ] **Step 1: Add fake-backend tests for every rejection channel.** In `snapshot_query_test.go`, configure the existing `fakeBackend` with function fields for the two new methods and record requests. Verify an explicit final call after reservation under the returned profile, not acceptance of a preliminary result. The core response gate is:

```go
if resp == nil || resp.GetContractVersion() != 1 ||
    resp.GetQueryProfileId() != req.GetQueryProfileId() ||
    resp.GetCode() != pb.SnapshotQueryCode_SUCCESS {
    return nil, fmt.Errorf("snapshot query analysis did not acknowledge the reserved profile")
}
```

Test nil response, transport error, unknown version, unavailable/older profile, syntactically valid but locally unsupported profile, exact-Q probe failure, missing/unknown/ineligible/contradictory column metadata, success with residual volatile AST case, unsupported mode and prepare with extra/missing scratch binding. Test unchanged ordinary constructor behavior plus idempotent and concurrent `Close`; existing ordinary materialization and empty-SI fail-open tests remain unchanged.

Test `ClassifySnapshotQuery` accepting an exact acknowledged ordinary result with no executable outputs, refusing that code with SQL/target/read data or wrong acknowledgement, and never mapping generic `UNSUPPORTED`/transport/parse errors to ordinary flow. Final reserved-profile Analyze and all Prepare calls reject `NOT_SNAPSHOT_QUERY`; only D1/D2 preliminary classification may use it to resume the unchanged ordinary chain.

- [ ] **Step 2: Run `bazel test //pkg/rewriter:rewriter_test --test_filter=TestSnapshotQuery`.** Expected pre-implementation failure: missing methods or an unacknowledged response accepted.

- [ ] **Step 3: Implement the wrapper, resource bounds and read-only probe.** Route gRPC through the new generated methods; native mode invokes A4's measured constructor and filters the file to the running tuple before exposing support. Check request byte limits, complete column metadata and response identity before/after FFI/RPC; final Analyze/Prepare turn all non-successes into typed errors, while Classify additionally admits only the exact structurally empty NOT_SNAPSHOT_QUERY result specified above. The exact-Q probe validates behavior including eligibility refusal, not membership or a version string alone. It creates no reservation or unsafe data and does not claim the supplied catalog is published. `Close` is idempotent, retains and releases the measured native descriptor/handle correctly, and races return a rejection; do not pretend a canceled native FFI call can be interrupted.

- [ ] **Step 4: Run `bazel test //pkg/rewriter:rewriter_test //pkg/replay:replay_test //pkg/auth:auth_test`.** Exercise a real native library and real gRPC service in the later D4 gate. Verify all new dependency pins, including the separately distributed FFI library used by CI, through the repository's dependency-upgrade procedure.

- [ ] **Step 5: Commit `feat(rewriter): require snapshot query capability acknowledgement`.** Publish the exact API/pins for B, C and D; leave runtime configuration and agent admission disabled.
