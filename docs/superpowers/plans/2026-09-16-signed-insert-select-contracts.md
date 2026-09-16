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
| `SnapshotQueryStatement` | `statement_seq uint64`, `envelope SnapshotQueryEnvelope` |
| `SnapshotQueryJob` | `block_seq uint64`, `prev_safe_snapshot_id string`, `prev_state_root string`, `schema_snapshot_id string`, `executor_profile_id string`, `query_profile_id string`, `reservation SnapshotQueryReservation`, `statement SnapshotQueryStatement`, `source_claim_root string`, `source_claim *SnapshotQueryClaim` (C5) |
| `SnapshotQueryEvidence` | `execution_outcome string`, `output_row_count uint64`, `output_rows_root string` |
| `SnapshotQueryReceipt` | `block_seq uint64`, `statement_root string`, `input_root string`, `read_set_root string`, `read_snapshot SnapshotPin`, `schema_snapshot_id string`, `executor_profile_id string`, `query_profile_id string`, `reservation_id string`, `fencing_generation uint64`, `execution_outcome string`, `abort_record_root string`, `output_row_count uint64`, `output_rows_root string`, `source_claim_root string`, `computed_state_root string`, `match_source_root bool`, `partition_commitments_after []PartitionCommitment`, `affected_parts []PartManifestEntry`, `replay_log_hash string` |
| `SnapshotQueryAttestation` | `replica_id string`, `receipt SnapshotQueryReceipt`, `receipt_hash string`, `signature string` |
| `SnapshotQuerySubmitResult` | `admission_code uint32`, `message string`, `statement_seq uint64`, `block_seq uint64`, `source_node string`, `input_root string`, `reservation SnapshotQueryReservation` |
| `SnapshotQueryStatus` | `version uint32`, `found bool`, `accepted SnapshotQuerySubmitResult`, `lifecycle string`, `execution_outcome string`, `terminal_proof []byte` |

`SnapshotQueryBinding` is nested identically in the canonical input and JWS; this nesting is an implementation choice that becomes frozen in this task. `client_account` must equal the canonical account in `statement_id` and the authorized signer identity. Duplicate network/schema/pin fields must agree. Add validation before normalization so a duplicate table/part cannot disappear during sorting. A well-formed unnormalized input may be canonicalized before signing; reject noncanonical transport bytes when checking a signed input.

Freeze the complete profile, control, artifact-readiness, abort and claim records specified in A4 and C1–C5 in this contract task as well; those tasks implement their behavior. This prevents B/C consumers from inventing incompatible local records. `SnapshotQueryStatus.version` starts at `1`; `found=false` is an authenticated negative lookup, never inferred from a zero-valued accepted result. `terminal_proof` carries C3's authenticated applied/abort record and retention authorization; it is not included in any user-input hash. A source constructing an in-process execution request before claiming has no `source_claim`; a dispatched verifier job requires the complete committed claim and checks its digest. Do not compare a compound claim digest directly with a computed state root.

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
| `SnapshotQueryCatalogTable` | `database string`, `table string`, `table_id string`, `schema_hash string`, `columns repeated SnapshotQueryColumn`; every column has `name string`, `type string` |
| `AnalyzeSnapshotQueryRequest` | `contract_version uint32`, `query_profile_id string`, `sql string`, `logical_database string`, `catalog repeated SnapshotQueryCatalogTable`, `materialize bool`, `inputs MaterializationInputs` |
| `AnalyzeSnapshotQueryResponse` | `contract_version uint32`, `query_profile_id string`, `code SnapshotQueryCode`, `message string`, `sql_after_materialization string`, `target_table_id string`, `target_columns repeated string`, `read_table_ids repeated string` |
| `SnapshotScratchBinding` | `table_id string`, `scratch_database string`, `scratch_table string` |
| `PrepareSnapshotQueryRequest` | `analysis AnalyzeSnapshotQueryRequest`, `bindings repeated SnapshotScratchBinding` |
| `PrepareSnapshotQueryResponse` | `contract_version uint32`, `query_profile_id string`, `code SnapshotQueryCode`, `message string`, `select_sql string`, `target_table_id string`, `target_columns repeated string`, `read_table_ids repeated string` |

`SnapshotQueryCode` is a new enum: `UNSPECIFIED=0`, `SUCCESS=1`, `UNSUPPORTED=2`, `INVALID_INPUT=3`, `PROFILE_UNAVAILABLE=4`, `MATERIALIZATION_FAILED=5`. Version is exactly `1`. `materialize=false` forbids inputs and residual volatility; `materialize=true` requires complete deterministic replacement, including pool availability. Scratch preparation always validates with `materialize=false` and requires an exact one-to-one binding for the returned read set. An error response cannot carry executable SQL. Caller-provided catalog entries come from authenticated schema resolution; they cannot invent a supported profile.

Analysis returns `target_columns` in the INSERT's positional target-list order. Preparation returns it in the authenticated target schema's order and rewrites the outer projection accordingly: `INSERT INTO t (b,a) SELECT x,y` produces schema-ordered output `(y,x)` when schema order is `(a,b)`. The prepared SELECT performs only profile-admitted coercions to those exact types; the scratch scanner checks resulting arity and types. Include this permutation in the golden corpus so B2's `QueryRows(selectSQL, targetSchema)` cannot silently assign the wrong columns. Range-checked integer literals may be rendered in their target type; unchecked narrowing/casts remain refused.

- [ ] **Step 1: Add message round-trip tests and an unsupported-service test.** The initial client test calls the new RPC on an old/fake server and requires `Unimplemented` to be an admission error. Add a fixture request with `contract_version=1`, a known query-profile identifier, `INSERT INTO tenant.copy SELECT value FROM tenant.events`, and catalog entries for exactly `tenant.copy` and `tenant.events`.

```proto
service RewriterService {
  // Add alongside the existing methods; retain them verbatim.
  rpc AnalyzeSnapshotQuery(AnalyzeSnapshotQueryRequest) returns (AnalyzeSnapshotQueryResponse);
  rpc PrepareSnapshotQuery(PrepareSnapshotQueryRequest) returns (PrepareSnapshotQueryResponse);
}
```

- [ ] **Step 2: Run `make test` before regeneration.** The new generated-type test fails to compile. After generation it must still fail if any field disappears in the round trip or an unsupported service is treated as success.

- [ ] **Step 3: Add the messages/RPCs with append-only changes and generated Go bindings.** Keep `Rewrite`, `MaterializeSQL`, `RewriteErrorMessage` and `Optimize` signatures unchanged. `SnapshotQueryCatalogTable.columns` has schema order; read IDs and scratch bindings have sorted unique table-ID order. Copy the exact request/response field names to the cross-engine corpus schema.

- [ ] **Step 4: Run `make proto`, `make lint`, `make test`, then `make verify` after generated files are staged/committed.** `make verify` regenerates and checks `gen/` cleanliness. Check the RP breaking-change gate against the actual base revision. RC consumes the independent RP repository through `third_party/rewriter-proto`; update that pin, not a divergent hand-edited mirror.

- [ ] **Step 5: Commit `feat(proto): add snapshot query analysis contract`.** Publish a real dependency version after checks; do not insert an invented future version into HG/RG/RC pins.

### Task A4: Implement the closed AST profile with native/gRPC parity

**Files:** RG and RC rows. Both engines implement A3's exact RPC contract; the RG corpus is mirrored byte for byte to RC.

**Interfaces:** RG adds `(*Service).AnalyzeSnapshotQuery(ctx context.Context, req *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)` and `PrepareSnapshotQuery` with A3's request/response types; the same signatures are exposed on `NativeRewriter`. RC adds matching service overrides and `doAnalyzeSnapshotQuery`/`doPrepareSnapshotQuery`, delegating to `src/handlers/snapshot_query.cc`. Internal functions consume each engine's own AST; no AST format crosses the RPC.

- [ ] **Step 1: Write the full positive/negative corpus before handlers.** Every case carries `name`, `request`, `expected_response`, and optional `prepare_request`/`expected_prepare_response`. Include the following SQL as separate literal cases, plus the spec's entire A1/A2/A7 syntax family:

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

The last three reject: hidden non-SI source, forbidden syntax in an unused CTE, and nested order-dependent row selection. Add CTE shadowing and free/correlated variables, scalar subqueries returning zero/one/multiple rows, both WITH placements, quoted names/comments/heredocs, every target column once, reordered full target list, missing/duplicate target columns, user-reserved row-id references, materialized/default-bearing columns and all rejected table-function/catalog/UDF carriers. A scalar subquery cardinality error is an execution failure, never permission to pick one row.

The initial profile admits typed column/literal projection, integer comparison and boolean composition. Checked integer conversions/arithmetic are admitted only after explicit checked-expression and overflow vectors pass on both engines and the pinned ClickHouse; keep those entries absent otherwise. Pure final ORDER BY is optional in the design: initially reject it to keep this profile smaller. Copying floats is allowed; computing with them is refused. Supported volatility is materialized once per AST occurrence; reject a UUID-producing expression if its resulting target type is outside `ResolveColumnProfile`.

- [ ] **Step 2: Run the corpus with empty handlers.** RG: `make ffi`, then `make test`. RC: `./scripts.sh rebuild`, `./scripts.sh test`. Require the new `SnapshotQuery.*` cases to be compiled and fail; explicitly add the new `.cc` to `tests/CMakeLists.txt` and handler to root `CMakeLists.txt`. Merely creating a test file is insufficient.

- [ ] **Step 3: Implement scope-aware analysis and exact scratch rewriting.** Reuse RG `Engine.ParseOne`, `Generate`, materialization nodes and table-scope helpers; reuse RC's ClickHouse AST and `visitChildrenWithCTEVisibility`. Validate every AST node, including unreferenced CTE bodies, while collecting the base-table closure only through referenced definitions. Bind each column/reference to its lexical scope and authenticated catalog. Traverse expressions in projections, predicates, joins and nested subqueries. Refuse free variables, unknown nodes/functions, non-SI identities, settings and parameters at every depth. Do not assume `CollectSelectTables` alone proves this closure.

```text
AnalyzeSnapshotQuery(request):
  require version == 1 and installed profile digest == request.query_profile_id
  parse exactly one statement; extract INSERT target and SELECT body for either WITH placement
  validate target schema and complete target-column permutation
  if materialize: replace every allowed volatile occurrence using supplied pools or reject
  walk all definitions and expression nodes against the closed profile
  bind referenced CTEs/aliases/columns with lexical scopes; reject free or correlated references
  collect, sort and deduplicate resolved base table IDs; reject a missing catalog entry
  return regenerated logical SQL and acknowledged version/profile/target/closure

PrepareSnapshotQuery(request):
  run AnalyzeSnapshotQuery with materialize=false; require success
  require exact bijection between read_table_ids and unique scratch bindings
  rewrite bound base-relation AST nodes only; never textual replacement
  return SELECT body with explicit output positions and scratch-only base relations
```

Add a `QueryProfileRecord` with ordered fields `version uint32`, `clickhouse_build_digest string`, `platform string`, `native_analyzer_build_digest string`, `grpc_analyzer_build_digest string`, `tzdata_digest string`, `settings []ProfileSetting`, `scalar_operators []string`, `column_profile_id string`, `output_order_id string`, `limits QueryLimits`. `ProfileSetting` is `{name string, value string}`, sorted uniquely by name; operators are sorted unique names. `QueryLimits` contains the unsigned `uint64` fields named below. A record has no self-referential ID. Its ID is `CanonicalDigest("snapshot-query-profile-v1", record)`; A1 freezes its canonical JSON and independent digest vector. Both analyzer digests belong to the same profile record, so native and gRPC can acknowledge the same profile ID while verifying their own build. HG owns these Go canonical records; engines verify the published canonical-byte/digest vector without importing HG, which would create a dependency cycle.

Create RG `snapshot_query_profiles.go` and tests with `NewServiceWithSnapshotQueryProfiles(libPath, profilePath string) (*Service, error)`; RC adds `--snapshot-query-profile-file` startup loading beside its existing server options. The immutable profile file contains canonical records plus their expected digests. Load/validate it before accepting RPCs; an unknown ID refuses. HG adds `Options.SnapshotQueryProfilePath` and A5's native factory uses the new constructor when creating a snapshot analyzer. Normal rewriter construction stays compatible. Do not compile a binary digest into its own profile ID: build binaries first, produce the immutable profile file from measured digests second.

Engine build digests are taken from the binaries actually used in B/D tests, not guessed release versions. Use a deterministic resource-test profile with `max_sql_bytes=65536`, `max_descriptor_bytes=65536`, `max_output_rows=1048576`, `max_output_bytes=268435456`, `max_restore_bytes=1073741824`, `max_sort_memory_bytes=134217728`, `max_spill_bytes=1073741824`, and `max_execution_ms=30000`; these are proposed initial test/canary limits, not production settings. A changed value produces a new profile ID and must traverse C1's active-policy boundary. Pin exact internal settings in the fixture, including single-thread execution and throw-on-limit behavior; resource exhaustion must never return a truncated successful SELECT.

- [ ] **Step 4: Run parity and rejection tests.** RG `make test`; RC `./scripts.sh test` and `ctest --test-dir build --output-on-failure`; RG harness with `REWRITER_ORACLE_ADDR` pointed at the tested RC service. Require byte-identical copied corpus files and identical canonical responses, including error categories, target order and closure. Add parser-depth/SQL-size failure cases before accepting input. Repeat with failed transport, zero/partial materialization pools, wrong profile and no capability acknowledgement; no response may fall back to ordinary rewrite.

- [ ] **Step 5: Commit one implementation in each engine.** Use `feat(rewriter): analyze pinned snapshot queries`. Record both engine digests and RP pin beside the shared corpus. Unsupported checked arithmetic remains refused until it has its own vectors; do not claim that ordinary ClickHouse casts detect overflow.

### Task A5: Add a fail-closed Housegate analysis wrapper and capability probe

**Files:** HG wrapper row, RG/RP dependency pins and root `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` when changed by the repository's dependency workflow.

**Interfaces:** Add `rewriter.SnapshotQueryAnalyzer` with `AnalyzeSnapshotQuery(context.Context, *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error)` and `PrepareSnapshotQuery(context.Context, *pb.PrepareSnapshotQueryRequest) (*pb.PrepareSnapshotQueryResponse, error)`; `NewSnapshotQueryAnalyzer(opts Options) (SnapshotQueryAnalyzer, error)` returns a wrapper with `Close() error` also on the interface. Extend the private `backend` and its fake with A3 methods. Add `ProbeSnapshotQuery(ctx context.Context, analyzer SnapshotQueryAnalyzer, profileID string, catalog []*pb.SnapshotQueryCatalogTable) error` using the A4 known positive, hidden-source and nested-LIMIT cases.

- [ ] **Step 1: Add fake-backend tests for every rejection channel.** In `snapshot_query_test.go`, configure the existing `fakeBackend` with function fields for the two new methods and record requests. Verify an explicit final call after reservation under the returned profile, not acceptance of a preliminary result. The core response gate is:

```go
if resp == nil || resp.GetContractVersion() != 1 ||
    resp.GetQueryProfileId() != req.GetQueryProfileId() ||
    resp.GetCode() != pb.SnapshotQueryCode_SUCCESS {
    return nil, fmt.Errorf("snapshot query analysis did not acknowledge the reserved profile")
}
```

Test nil response, transport error, unknown version, unavailable/older profile, success with residual volatile AST case, unsupported mode and prepare with extra/missing scratch binding. Existing ordinary materialization and empty-SI fail-open tests remain unchanged.

- [ ] **Step 2: Run `bazel test //pkg/rewriter:rewriter_test --test_filter=TestSnapshotQuery`.** Expected pre-implementation failure: missing methods or an unacknowledged response accepted.

- [ ] **Step 3: Implement the wrapper, resource bounds and read-only probe.** Route gRPC through the new generated methods; native `rewritergo.Service` implements the same interface. Check request byte limits before FFI/RPC, check complete response identity, and turn all non-successes into typed errors. The probe validates behavior, not a version string alone. It creates no reservation or unsafe data. `Close` is idempotent and races return a rejection; do not pretend a canceled native FFI call can be interrupted.

- [ ] **Step 4: Run `bazel test //pkg/rewriter:rewriter_test //pkg/replay:replay_test //pkg/auth:auth_test`.** Exercise a real native library and real gRPC service in the later D4 gate. Verify all new dependency pins, including the separately distributed FFI library used by CI, through the repository's dependency-upgrade procedure.

- [ ] **Step 5: Commit `feat(rewriter): require snapshot query capability acknowledgement`.** Publish the exact API/pins for B, C and D; leave runtime configuration and agent admission disabled.
