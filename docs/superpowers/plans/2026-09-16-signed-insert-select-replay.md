# Signed INSERT ... SELECT Snapshot Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore an authenticated read snapshot and independently derive deterministic query output, complete post-state and versioned replay evidence.

**Architecture:** Housegate defines injected snapshot/row-stream ports and owns canonical execution. Arbiter-core publishes and retains immutable artifacts and supplies a verified scratch handle. A new query executor implements `replay.Executor`; shared append helpers preserve the v2 payload executor and the full predecessor ledger.

**Tech Stack:** Go, ClickHouse native client, immutable part artifacts, canonical row encoding, bounded external merge sort, Bazel and Docker.

**Spec:** [Signed INSERT ... SELECT design](../specs/2026-09-16-signed-insert-select-design.md), D2 and D5–D7; [plan index](2026-09-16-signed-insert-select.md); [contracts plan](2026-09-16-signed-insert-select-contracts.md).

## Global Constraints

- All [index constraints](2026-09-16-signed-insert-select.md#global-constraints) apply; A's wire/analysis interfaces are dependencies, not existing APIs.
- Use exactly the pinned S, complete read tables, all active parts and the entire predecessor ledger. A self-consistent manifest alone is not authenticated publication.
- Sort **full** `lthash.EncodeRow` user-row bytes; preserve duplicates and global ordinals `0..N-1`; keep `housegate-row-id-v1` unchanged.
- Reuse `ResolveColumnProfile`, `PartitionIDForRow` and `RowElementHash`. No new type or expression-evaluation authority is introduced.
- Local inability to validate/restore/execute is a pre-receipt refusal. Valid independent replay that disagrees with a source produces signed mismatch evidence.
- Canonical algorithms belong to HG; artifact I/O and retention implementations belong to AC. Neither package may restore from a live `hg_safe` table by assumption.

---

## File map

| Owner | Create | Modify |
|---|---|---|
| HG ports | `pkg/replay/snapshotquery/ports.go`, `profile.go`, `profile_test.go`, `BUILD.bazel` | None outside build registration |
| AC artifacts | `dataplane/snapshot_artifacts.go`, `snapshot_artifacts_test.go`, `snapshot_retention.go`, `snapshot_retention_test.go` | `dataplane/manifests.go`, `dataplane/BUILD.bazel`; source publication wiring in `snode/promote.go`, `snode/promote_replace.go`, `snode/BUILD.bazel` |
| HG scratch | `pkg/replay/chexec/snapshot.go`, `snapshot_test.go` | `pkg/replay/chexec/BUILD.bazel` |
| AC restore | `dataplane/snapshot_restore.go`, `snapshot_restore_test.go` | `dataplane/BUILD.bazel` |
| HG canonical output | `pkg/replay/snapshotquery/canonical.go`, `canonical_test.go`, `sort.go`, `sort_test.go`, `output.go` | `pkg/replay/snapshotquery/BUILD.bazel` |
| HG shared append | `pkg/replay/payloadexec/row_source.go`, `apply_rows.go`, `apply_rows_test.go` | `pkg/replay/payloadexec/executor.go`, `exports.go`, `BUILD.bazel` |
| HG query replay | `pkg/replay/snapshotquery/executor.go`, `executor_test.go`, `verifier.go`, `verifier_test.go`, `dispatch.go`, `dispatch_test.go` | `pkg/replay/types.go` for in-process extension pointers only, respective BUILD files |
| AC verifier | `verifier/snapshot_query_test.go` | `verifier/backends.go`, `verifier/verifier.go`, `verifier/config.go`, `verifier/BUILD.bazel`, `wire/snapshot_query.go` |

### Task B1: Publish immutable artifacts with durable retention references

**Files:** HG ports and AC artifacts rows. AC currently has a cached `ManifestStore.GetSafeSnapshot` over `SafeState.GetManifest`; it has no complete artifact publication/retention implementation. Build that missing component explicitly.

**Interfaces:** HG `snapshotquery` defines `SnapshotReadStore.Open(ctx context.Context, pin replay.SnapshotPin, reads replay.SnapshotReadSet, referenceID string) (ReadSnapshot, error)`. It also defines:

```go
type Relation struct { TableID, Database, Table string }

type RowStream interface {
    Next(context.Context) ([]any, error) // io.EOF is the only successful end.
    Close() error
}

type ReadSnapshot interface {
    Manifest() replay.SafeSnapshotManifest
    Schemas() []payloadexec.TableSchema
    Relations() []Relation
    QueryRows(context.Context, string, payloadexec.TableSchema) (RowStream, error)
    Close() error
}
```

`QueryRows` is implemented by HG's restricted scratch connection in B2; it returns target-typed values and cannot mutate source relations. Closing the local scratch handle does not drop the durable replay/challenge reference. AC adds `SnapshotArtifacts` with `Publish(ctx, manifest, schemas) error`, `Retain(ctx, referenceID, pin) error`, `Release(ctx, referenceID, terminalProof) error`, and `FetchPart(ctx, pin, part) (io.ReadCloser, error)`. Here `manifest` is `replay.SafeSnapshotManifest`, `schemas` is `[]payloadexec.TableSchema`, `pin` is `replay.SnapshotPin`, `part` is `replay.PartManifestEntry`, and `terminalProof` is the authenticated outcome/retention authorization returned by C3, represented as `[]byte` and verified by an injected control-plane verifier. Release must never trust a caller's boolean.

AC owns an injected `PublishedSnapshotSource.GetPublishedSnapshot(ctx context.Context, pin replay.SnapshotPin) (replay.SafeSnapshotManifest, []payloadexec.TableSchema, error)`. Its implementation checks the authenticated SafeState publication/activation chain added in C1–C3, not an arbitrary artifact server. Test it with a deterministic fake until AR implements that service. Network authentication plus the committed published record is required; `Manifest.Validate()` remains an additional integrity check.

Publish C1's signed `SnapshotArtifactReady` record through `RecordSnapshotArtifactReady` after complete durable upload. Source promotion/publication wiring must export and retain eligible snapshots produced by both payload and query writers. Bootstrap the existing predecessor's artifacts before its executor transition can activate queries. Rewriting a fetch hint means trying another location for the same pinned object; it never mutates the selected manifest or its root.

Make the first storage backend concrete: AC adds `ArtifactBackend` with `Put(ctx context.Context, key string, input io.Reader) (digest string, length uint64, err error)`, `Open(ctx context.Context, key string) (io.ReadCloser, error)` and `Delete(ctx context.Context, key string) error`, plus `NewFilesystemArtifactBackend(rootDir string) (ArtifactBackend, error)`. It uses immutable content keys, atomic/fsynced writes and bounded streaming; validate keys and refuse symlink/path escape. `PartExporter.Export(ctx context.Context, part replay.PartManifestEntry) (io.ReadCloser, error)` supplies an immutable frozen part archive using source data-plane access. `NewSnapshotArtifacts(backend ArtifactBackend, exporter PartExporter, published PublishedSnapshotSource, journalDir string, verifyTerminal func(context.Context, []byte) error) (SnapshotArtifacts, error)` supplies B1's public port. Source and verifier see the same durable artifact namespace through a shared mounted store in the first deployment/test profile; local ephemeral caches alone fail readiness. Remote object-store adapters can implement this port later without changing roots.

- [ ] **Step 1: Write publication/retention tests with a temporary object directory and journal.** Test a manifest whose hash is internally valid but which is absent from `PublishedSnapshotSource`, a declared empty table, missing schema, renamed location serving identical bytes, and reference retention across process reconstruction. Tests must inspect storage after a failed operation, not just a return code:

```text
publish S with complete schemas and two parts -> durable commit marker exists only after both parts verify
retain S using reference "reservation/r1/7" -> restart store -> GC cannot delete either part
retain same reference twice -> one durable reference; different pin under same reference -> reject
release without authenticated terminal proof -> reject and keep both parts
release after scheduling barrier ends but challenge retention remains -> keep both parts
corrupt or omit one upload -> no publication-ready acknowledgement
```

- [ ] **Step 2: Run `bazel test //dataplane:dataplane_test` in AC and the new `bazel test //pkg/replay/snapshotquery:snapshotquery_test` in HG.** Expect missing ports/store or failed retention/publication assertions before implementing them. Add both BUILD declarations in this task.

- [ ] **Step 3: Implement publish-before-safe readiness and idempotent durable references.** Export exact selected parts under content-addressed keys, with the authenticated schema and physical-hash definition. Validate complete upload before acknowledging availability to the safe publisher; AR's final publication gate consumes that acknowledgement. Never delete old parts merely because a MergeTree merge made them inactive locally. Use atomic write/fsync/rename and a durable reference journal; account for reserve, accepted block, replay and challenge references independently.

```text
Publish(manifest, schemas):
  validate full table/schema ledger and canonical roots
  export each selected part from a stable frozen source view
  hash/check every artifact; fsync artifact and schema objects
  write/fsync readiness record binding snapshot_id, manifest_root and all artifact hashes
  acknowledge readiness; only the control plane may publish the safe snapshot

Retain(referenceID, pin):
  verify pin publication; atomically persist referenceID -> exact pin
  reject rebinding an existing referenceID; repeated identical retain succeeds

GC(candidate):
  require no durable references and authenticated retention horizon passed
  recheck under the GC/reference lock, then delete the immutable object
```

Use the existing part physical-hash authority where available; freeze its archive/layout interpretation in a profile vector. The content-addressed object key may be separate from `part_phys_hash` if the archive container has its own digest: verify both and never confuse compressed archive bytes with the authenticated part-content hash. Interrupted uploads are uncommitted temporary objects, never readable parts.

- [ ] **Step 4: Run retention fault injection and cold fetch tests.** Crash between upload, fsync, readiness, retain and release; reconstruct from disk, and verify no referenced object disappears. Require a changed hint to work only for identical authenticated bytes. Run AC's dataplane and snode tests and preserve existing safe publication behavior while the feature is off.

- [ ] **Step 5: Commit `feat(snapshot): retain authenticated replay artifacts`.** Include ports in an HG commit first, then pin it in the AC implementation. No live network is made artifact-capable merely by adding the manifest reader.

### Task B2: Restore and verify read-only scratch relations

**Files:** HG scratch and AC restore rows; use B1's ports and A3/A5 scratch-binding contract.

**Interfaces:** AC `NewSnapshotReadStore(published PublishedSnapshotSource, artifacts SnapshotArtifacts, restorer snapshotquery.ScratchRestorer) snapshotquery.SnapshotReadStore` returns the implementation of HG's interface. `ScratchRestorer.Restore(ctx context.Context, manifest replay.SafeSnapshotManifest, schemas []payloadexec.TableSchema, reads replay.SnapshotReadSet, parts []VerifiedPart) (snapshotquery.ReadSnapshot, error)` uses `VerifiedPart{Entry replay.PartManifestEntry, LocalPath string}` after download verification; HG `chexec.NewSnapshotRestorer(admin clickhouse.Conn, readerFactory func(context.Context, []snapshotquery.Relation) (clickhouse.Conn, error)) snapshotquery.ScratchRestorer` supplies it. The factory must provision/read with grants restricted to the restored relations, not return `admin`. Place the `ScratchRestorer`/`VerifiedPart` port types in HG `snapshotquery/ports.go` so HG does not import AC.

- [ ] **Step 1: Add complete-ledger adversarial tests.** Construct S with read table R, target W and unrelated U. Require exact relation membership; remove/duplicate/add a part, change table/partition/schema/row count/bytes, corrupt physical bytes or row LtHash, and require refusal before `QueryRows`. The empty R case must create an authenticated empty relation; removing R from the manifest must fail.

```text
Open(S, {R}, ref): published proof -> retain -> complete descriptor equality -> fetch all R parts
verify physical hashes -> attach isolated parts -> scan imported rows including _hg_row_id
sum RowElementHash per part -> part LtHash -> complete partition roots -> expose read-only R
QueryRows("SELECT value FROM R_scratch", W_schema): return coerced user values only
attempt production table/catalog/remote read on that reader: permission/profile refusal
Close(): close reader, remove only this operation's scratch relations, retain durable ref
```

- [ ] **Step 2: Run HG `bazel test //pkg/replay/chexec:chexec_test` and AC `bazel test //dataplane:dataplane_test`.** Unit fakes must prove the denial order. Register a Docker case for actual grants/ATTACH/readback in D4; do not claim fake connections prove SQL isolation.

- [ ] **Step 3: Implement authenticated restore and the restricted handle.** Verify the D1 publication record, schema/root/profile identity and all descriptor entries before trusting bytes. Download all active parts of R, not predicate-selected partitions. Validate per-part physical hash, rows, schema and partition; scan with the existing shared row authority and fold every part into its partition commitment. Use an immutable imported copy or a proven immutable hardlink/reflink; do not attach mutable live part paths. Scratch names are random operation-local transport names excluded from commitments.

Use A5's prepare API to rewrite every bound read relation to the handle's exact relations. Return only user columns; `_hg_row_id` is accessible to the internal validator and hidden from user SQL. Target coercion runs on the pinned engine with profile-admitted conversions and is read back via `ResolveColumnProfile`. Unknown types, coercion overflow, multi-row scalar subquery errors and incomplete streams fail the operation. If an output scratch table is needed, a separate internal writer owns it; the user's read connection has no INSERT/DDL privilege.

```go
stream, err := handle.QueryRows(ctx, prepared.GetSelectSql(), targetSchema)
if err != nil { return nil, err }
// A caller must read through io.EOF; error/cancel before EOF cannot publish output.
defer stream.Close()
```

No `remote()` or live catalog is used to find a missing source. A missing artifact produces a typed retryable restore error. Conflicting identities/corrupt bytes produce a typed integrity refusal. Both are pre-receipt failures.

- [ ] **Step 4: Verify cold/warm equivalence and retention cleanup.** Run AC dataplane and HG chexec tests with the same S but different local unsafe rows, physical download locations and cache layouts. The restored user rows must match. Fail a read midstream and confirm that no applied result or unsafe write exists, while durable pin retention remains recoverable.

- [ ] **Step 5: Commit `feat(replay): restore authenticated query snapshots`.** Include the actual grants/ATTACH integration case in the same implementation branch and its CI registration before marking this task done.

### Task B3: Canonicalize and externally sort the complete output

**Files:** HG canonical output row and `pkg/replay/snapshotquery/profile.go`; add `payloadexec.RowSource` in `row_source.go` as the minimal iterator type used by B4.

**Interfaces:** `type Limits = replay.QueryLimits` uses A4's exact canonical profile bounds (`MaxSQLBytes`, `MaxDescriptorBytes`, `MaxOutputRows`, `MaxOutputBytes`, `MaxRestoreBytes`, `MaxSortMemoryBytes`, `MaxSpillBytes`, `MaxExecutionMS`), all `uint64`. Add `Canonicalize(ctx context.Context, networkID, statementID string, schema payloadexec.TableSchema, input RowStream, limits Limits, tempDir string) (CanonicalOutput, error)` and `NormalizeRow(schema payloadexec.TableSchema, values []any) ([]any, error)`.

```go
// package payloadexec
type RowSource interface {
    Next(context.Context) (Row, error)
    Close() error
}

// package snapshotquery
type CanonicalOutput interface {
    RowCount() uint64
    OutputRowsRoot() string
    TouchedPartitionIDs() []string
    OpenRows() (payloadexec.RowSource, error)
    Close() error
}
```

- [ ] **Step 1: Add ordering, multiplicity and limit tests.** Feed the same typed rows in reversed, shuffled and differently chunked orders, including `65536` identical rows and at least two partitions. Independently compute each expected key with `lthash.EncodeRow`, sort via `bytes.Compare`, and assert every assigned `RowID(network, table, statement, ordinal)` without resetting ordinal. Include canonical NaNs, negative/positive zero and equal instants with different timezone representations. Do not use output-root equality alone as proof of multiplicity.

```go
func TestCanonicalKeyOrdersBytesNotDigest(t *testing.T) {
    cols := []lthash.Column{{Name: "value", Type: "UInt64"}}
    var keys [][]byte
    for _, v := range []uint64{256, 1, 0, 65536} {
        b, err := lthash.EncodeRow("tenant.copy", cols, []any{v})
        if err != nil { t.Fatal(err) }
        keys = append(keys, b)
    }
    sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i], keys[j]) < 0 })
    // Little-endian canonical bytes intentionally differ from numeric ordering.
    b, err := lthash.EncodeRow("tenant.copy", cols, []any{uint64(65536)})
    if err != nil { t.Fatal(err) }
    if !bytes.Equal(keys[1], b) { t.Fatal("canonical byte order changed") }
}
```

Add a production-path test that reads `CanonicalOutput.OpenRows()` and compares its keys and IDs to this independent ordering; the snippet above alone tests only the encoding convention. Test zero rows, a canceled stream, `io.ErrUnexpectedEOF`, exact-boundary success and one-over-limit failure for every resource. A partial stream cannot produce `CanonicalOutput`.

- [ ] **Step 2: Run `bazel test //pkg/replay/snapshotquery:snapshotquery_test --test_filter='TestCanonical|TestSort'`.** Expect missing APIs or a wrong order/count on shuffled and spill paths.

- [ ] **Step 3: Implement normalization, sorted runs and a bounded merge.** Use `ResolveColumnProfile` to validate and normalize each value to its exact Go representation; reuse the canonical encoder's NaN/zero/time semantics. Store normalized typed values alongside the full canonical key so the stored output and hashed values agree. Count row bytes, key memory, sort-run overhead, output hashes and spill bytes against the profile budgets. Reject oversized single rows and integer-overflowing counters before allocating.

```text
read typed rows through EOF -> normalize -> encode full user-row key
sort memory-bounded runs by bytes.Compare(key); spill framed key + typed value records
merge runs by full key; preserve every duplicate
for global ordinal in [0,N): derive RowID; derive PartitionIDForRow; append normalized output
collect ordered DigestBytes(key), including duplicates
CanonicalDigest("snapshot-query-output-v1", {target_table_id,schema_hash,row_count,row_hashes})
fsync completed output; return handle only after all streams and counters validate
```

`row_hashes` is an explicit empty array for N=0. Bound its memory as well as row buffers; the profile's row cap prevents an unbounded digest list. If a streaming canonical hash implementation is introduced for larger profiles, prove its bytes against `CanonicalDigest` golden vectors before use. Spill framing has length checks and content checks so corrupt local caches cannot silently alter rows. Cache files are untrusted derived artifacts bound to input/output roots, never replacement user input.

- [ ] **Step 4: Run sort-spill and interruption tests.** Force multiple runs with a small test memory budget, truncate/corrupt one run, alter worker/chunk ordering, and verify identical final output on valid cases and atomic refusal on invalid cases. Confirm scratch/run cleanup after cancellation and restart discovery for durable outputs retained by C4; do not release snapshot retention here.

- [ ] **Step 5: Commit `feat(replay): canonicalize snapshot query output`.** Keep every v2 row-order and row-ID vector unchanged.

### Task B4: Share append/state assembly and build the snapshot-query executor

**Files:** HG shared append row and `snapshotquery/executor.go`, `executor_test.go`; in-process extensions in `pkg/replay/types.go`.

**Interfaces:** Add `payloadexec.StatementRows{StatementID string, StatementSeq uint64, TargetTableID string, Rows RowSource}` and `(*payloadexec.Executor).ApplyRows(ctx context.Context, prev replay.SafeSnapshotManifest, job replay.ReplayJob, batches []StatementRows) (replay.SafeSnapshotManifest, replay.ExecutionResult, error)`. Existing `ApplyContext` decodes/materializes v2 and delegates to the same append core without changing ordering. Add `SnapshotQuery *SnapshotQueryJob` to `replay.ExecutionRequest` and `SnapshotQuery *SnapshotQueryEvidence` to `replay.ExecutionResult`; these are in-process optional extensions, not changes to frozen v2 wire records or hashes.

New `snapshotquery.Executor` receives `NetworkID string`, `Snapshots SnapshotReadStore`, `Analyzer rewriter.SnapshotQueryAnalyzer`, `Profiles ProfileRegistry`, and `Appender *payloadexec.Executor`; it implements `Replay(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error)`. `ProfileRegistry.Lookup(executorID, queryID string) (Profile, bool)` returns `Profile{ExecutorProfileID string, QueryProfileID string, Record replay.QueryProfileRecord}`. Bounds are `profile.Record.Limits`; the remaining record fields pin engines/settings/types/operators as specified in A4. It does not decide admission authorization; C1 does. The executor receives B5's `HistoricalPolicy` port too, so direct source invocation must prove accepted reservation provenance rather than trusting a caller-populated job.

- [ ] **Step 1: Add whole-ledger regression tests before extracting code.** Use the existing payload executor fixtures with tables R, W and U; after appending to W, assert exact preserved R/U table/partition/part entries and parent linkage. Add query cases with R=W, zero output and two sequential safe self-inserts. Drop a table, duplicate a statement ID or mismatch a schema in direct `ApplyRows` input and require refusal.

```text
S: R has 2 rows, W has 1 row, U has 3 rows
query W <- SELECT R: post rows R=2,W=3,U=3; unchanged R/U ledger byte-equal
self-insert R <- SELECT R: new R=4 with two new IDs; old IDs unchanged
next safe self-insert at S2: R=8; an aborted first operation would instead leave R=2
constant SELECT with zero rows: all data/partition roots unchanged, new manifest/block identity
```

- [ ] **Step 2: Run `bazel test //pkg/replay/payloadexec:payloadexec_test //pkg/replay/snapshotquery:snapshotquery_test`.** The new direct append/query tests fail before extraction; old payload vectors establish the baseline.

- [ ] **Step 3: Extract the smallest common append core, then orchestrate query execution.** Move existing ledger verification, deterministic part/delta assembly and complete table preservation into `ApplyRows`. It validates unique statement IDs, known targets and complete predecessor/schema coverage. It must not impose payload-ref requirements on already validated row batches; those remain at the v2 caller. Preserve existing v2 part grouping and byte/accounting formulas, proven by its old vectors.

```text
snapshotquery.Executor.Replay(req):
  require exactly one req.SnapshotQuery; require supported exact executor/query profiles
  verify job/pin/descriptor/schema identity and accepted reservation provenance
  open retained S using statement/block reference; independently analyze signed materialized SQL
  compare full read closure/descriptor; prepare SELECT bound only to restored relations
  stream target-typed rows; Canonicalize through complete EOF
  pass globally ordered rows to Appender.ApplyRows using the job's assigned sequence
  require complete predecessor table set; attach applied output count/root evidence
  return execution result; close scratch/output handles without dropping durable retention
```

The executor is independently reusable by source and verifier; it never calls `prepareAndSubmit`, writes production unsafe tables or re-materializes volatility. A missing `SnapshotQuery` is not inferred from empty payload. The composite dispatcher in B5 invokes the legacy executor explicitly for legacy requests.

- [ ] **Step 4: Run payload regression, query state and separate-instance tests.** HG targeted replay/payloadexec/chexec/snapshotquery tests must pass. D4 supplies two real ClickHouse instances with different local live data and part order; compare canonical output, global IDs, LtHash and complete post-state. Do not claim physical part byte equality across instances is required for equal logical data roots.

- [ ] **Step 5: Commit `feat(replay): execute snapshot queries with shared state assembly`.** Include the full v2 vector-preservation result in the PR; any changed legacy digest blocks the task.

### Task B5: Sign versioned query receipts and wire historical dispatch

**Files:** HG query verifier/dispatch row; AC verifier/wire row. Depends on A2, B4 and C1's authenticated historical-policy lookup port; a deterministic fake can test that port before AR is implemented.

**Interfaces:** `snapshotquery.Verifier` receives the executor, `replay.Signer`, an A2 pure signature validator, and `HistoricalPolicy.VerifyReservation(ctx context.Context, job replay.SnapshotQueryJob) error`. It exposes `Verify(ctx context.Context, job replay.SnapshotQueryJob) (replay.SnapshotQueryAttestation, error)`. `CompositeExecutor` holds `Payload replay.Executor` and `Query replay.Executor`; its `Replay` routes only on the explicit in-process variant and profile. AC `NewSnapshotQueryReplayCore` constructs the query verifier beside the existing `NewReplayCore`, and verifier dispatch gets a separate AP query-job variant and query-attestation reply.

- [ ] **Step 1: Write receipt/refusal and dispatch tests.** Assert a valid result with wrong source root returns a signed `match_source_root=false` receipt; invalid signature/unpublished pin/missing part/unsupported profile/stream failure returns no attestation. Historical replay uses the authenticated original pair after current policy changes. A legacy request invokes only the legacy executor; unknown input/profile never calls either executor.

```go
func (d CompositeExecutor) Replay(ctx context.Context, req replay.ExecutionRequest) (replay.ExecutionResult, error) {
    if req.SnapshotQuery != nil {
        if d.Query == nil { return replay.ExecutionResult{}, fmt.Errorf("snapshot query executor unavailable") }
        return d.Query.Replay(ctx, req)
    }
    if d.Payload == nil { return replay.ExecutionResult{}, fmt.Errorf("payload executor unavailable") }
    return d.Payload.Replay(ctx, req)
}
```

The dispatcher code is intentionally small; the selected executor validates its exact profile and shape. Add zero-row applied receipt and aborted-outcome separation tests: abort evidence is produced from C3's authenticated control transition, never by converting an execution error to applied success.

- [ ] **Step 2: Run HG `bazel test //pkg/replay/snapshotquery:snapshotquery_test` and AC `bazel test //verifier:verifier_test //conformance:conformance_test //wire:wire_test`.** Require missing query dispatch/new receipt support to fail visibly.

- [ ] **Step 3: Implement independent signature/input/descriptor/policy validation before replay and new receipt hashing after replay.** Recompute A1 input/statement/read roots, verify the original JWS without today's freshness rule, then independently analyze SQL and restore S. Require C5's complete committed claim, check `claim.Hash() == job.SourceClaimRoot`, and compare computed state/output count/output root with the claim's corresponding fields. Do not compare computed state to the compound claim digest. A valid replay binds output evidence and full state to `snapshot-query-receipt-v1`; use `replay.Signer.SignReplayReceipt` on that new hash. Preserve old `ExecutionReceipt.Hash()` and the v2 verifier path.

AC's verifier handler dispatches new query jobs separately, signs/returns the new attestation and retains its exact evidence. Keep byte-side scans of exact candidate parts through `chexec.ScanParts` and `RowElementHash`. A correct output hash with different candidate bytes must still fail the AR three-way gate in C5. Source and verifier caches cannot substitute for independent execution.

- [ ] **Step 4: Run differential fraud tests and legacy conformance.** Source executes using altered unsafe/live rows, substitutes one normalized output row, swaps an output cache or candidate part, or drops U from the assembled state. Require signed mismatch/challenge evidence when valid replay disagrees and no promotion in each fraud case. Keep local restore unavailability distinct from malicious-source mismatch.

- [ ] **Step 5: Commit `feat(verifier): attest snapshot query execution evidence`.** Release HG before AC updates its import pins; SN does not instantiate this verifier and must not gain a fictitious verifier constructor in D3.
