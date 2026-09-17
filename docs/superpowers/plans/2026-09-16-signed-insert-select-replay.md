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
| HG schema | `pkg/replay/snapshot_query_schema.go`, `snapshot_query_schema_test.go`, `pkg/replay/testdata/snapshot_query_schema_v1.json` | `pkg/replay/BUILD.bazel` |
| HG schema certificate | `pkg/auth/snapshot_schema_certificate.go`, `snapshot_schema_certificate_test.go` | `pkg/auth/BUILD.bazel` |
| HG ports | `pkg/replay/snapshotquery/ports.go`, `profile.go`, `profile_test.go`, `BUILD.bazel` | None outside build registration |
| AC artifacts | `dataplane/snapshot_artifacts.go`, `snapshot_artifacts_test.go`, `snapshot_retention.go`, `snapshot_retention_test.go`, `snapshot_owners.go`, `snapshot_owners_test.go` | `dataplane/manifests.go`, `dataplane/BUILD.bazel`; source publication wiring in `snode/promote.go`, `snode/promote_replace.go`, `snode/BUILD.bazel` |
| HG scratch | `pkg/replay/chexec/snapshot.go`, `snapshot_test.go` | `pkg/replay/chexec/BUILD.bazel` |
| AC restore | `dataplane/snapshot_restore.go`, `snapshot_restore_test.go` | `dataplane/BUILD.bazel` |
| HG canonical output | `pkg/replay/snapshotquery/canonical.go`, `canonical_test.go`, `sort.go`, `sort_test.go`, `output.go` | `pkg/replay/snapshotquery/BUILD.bazel` |
| HG shared append | `pkg/replay/payloadexec/row_source.go`, `apply_rows.go`, `apply_rows_test.go` | `pkg/replay/payloadexec/executor.go`, `exports.go`, `BUILD.bazel` |
| HG query replay | `pkg/replay/snapshotquery/executor.go`, `executor_test.go`, `verifier.go`, `verifier_test.go`, `dispatch.go`, `dispatch_test.go` | `pkg/replay/types.go` for in-process extension pointers only, respective BUILD files |
| AC verifier | `verifier/snapshot_query_test.go` | `verifier/backends.go`, `verifier/verifier.go`, `verifier/config.go`, `verifier/BUILD.bazel`, `wire/snapshot_query.go` |

### Task B1: Publish immutable artifacts with durable retention references

**Files:** HG schema, HG schema certificate, HG ports and AC artifacts rows. AC currently has a cached `ManifestStore.GetSafeSnapshot` over `SafeState.GetManifest`; it has no complete artifact publication/retention implementation. Build that missing component explicitly.

**Interfaces:** HG `snapshotquery` defines `SnapshotReadStore.Open(ctx context.Context, pin replay.SnapshotPin, reads replay.SnapshotReadSet, referenceID string) (ReadSnapshot, error)`. It also defines:

```go
type Relation struct { TableID, Database, Table string }

type RowStream interface {
    Next(context.Context) ([]any, error) // io.EOF is the only successful end.
    Close() error
}

type ReadSnapshot interface {
    Manifest() replay.SafeSnapshotManifest
    SchemaArtifact() replay.AuthenticatedSnapshotQuerySchemaV1
    Schemas() []payloadexec.TableSchema
    Relations() []Relation
    QueryRows(context.Context, string, payloadexec.TableSchema) (RowStream, error)
    Close() error
}
```

`QueryRows` is implemented by HG's restricted scratch connection in B2; it returns target-typed values and cannot mutate source relations. Closing the local scratch handle does not drop the durable replay/challenge reference. AC adds `SnapshotArtifacts` with `Publish(ctx context.Context, manifest replay.SafeSnapshotManifest, schema replay.AuthenticatedSnapshotQuerySchemaV1) error`, `Retain(ctx, referenceID, pin) error`, `Release(ctx, referenceID, terminalProof) error`, and `FetchPart(ctx, pin, part) (io.ReadCloser, error)`. `SchemaArtifact()` returns the verified typed object while `Schemas()` returns its validated unchanged `[]payloadexec.TableSchema` projection. Here `pin` is `replay.SnapshotPin`, `part` is `replay.PartManifestEntry`, and `terminalProof` is the authenticated outcome/retention authorization returned by C3, represented as `[]byte` and verified by an injected control-plane verifier. Release must never trust a caller's boolean.

AC owns an injected `PublishedSnapshotSource.GetPublishedSnapshot(ctx context.Context, pin replay.SnapshotPin) (replay.SafeSnapshotManifest, replay.AuthenticatedSnapshotQuerySchemaV1, error)`. Its implementation checks the authenticated SafeState publication/activation/readiness association added in C1–C3, not an arbitrary artifact server. It remains published-only and cannot supply current authority admission, ready-but-unpublished/lost-response lookup, publisher authentication or readiness submission; B1 receives those through the addendum's separate local `ArtifactPublicationControl`. Network authentication plus the committed published record is required; `Manifest.Validate()` remains an additional integrity check.

HG root replay owns the strict canonical artifact/outer records and projection validation; auth owns the pure schema-certificate payload plus the existing compact ES256K signing/verification convention. B1 fetches bounded exact outer bytes, requires byte-identical strict re-encoding, verifies inner and outer digests, the [exact A1 artifact-set commitment](../specs/2026-09-16-signed-insert-select-schema-semantics-design.md#artifact-set-commitment), exact S/projection, certificate purpose/signature and dedicated network/shard-scoped schema-authority role before readiness. The authoritative fields, digest order and no-cycle rule are in the same schema-semantics addendum. Publisher authorization remains separate and occurs only after certificate/object/part validation.

Publish C1's signed `SnapshotArtifactReady` record through `RecordSnapshotArtifactReady` after complete durable upload. Source promotion/publication wiring must export and retain eligible snapshots produced by both payload and query writers. Bootstrap the existing predecessor's artifacts before its executor transition can activate queries. Rewriting a fetch hint means trying another location for the same pinned object; it never mutates the selected manifest or its root.

Make the first storage backend concrete: AC adds `ArtifactBackend` with `Put(ctx context.Context, key string, input io.Reader) (digest string, length uint64, err error)`, `Open(ctx context.Context, key string) (io.ReadCloser, error)` and `Delete(ctx context.Context, key string) error`, plus `NewFilesystemArtifactBackend(rootDir string) (ArtifactBackend, error)`. It uses immutable content keys, atomic/fsynced writes and bounded streaming; validate keys and refuse symlink/path escape. `PartExporter.Export(ctx context.Context, part replay.PartManifestEntry) (io.ReadCloser, error)` supplies an immutable frozen part archive using source data-plane access. AC also owns the addendum's exact local `ArtifactPublisher`, `CommittedArtifactReady`, `ArtifactPublicationControl` and protected `ArtifactOwnerRegistry` implementation. `NewSnapshotArtifacts(backend ArtifactBackend, exporter PartExporter, published PublishedSnapshotSource, publication ArtifactPublicationControl, journalDir string, owners ArtifactOwnerRegistry, verifyTerminal func(context.Context, string, replay.SnapshotPin, []byte) error) (SnapshotArtifacts, error)` supplies B1's public port. `owners` is the required explicit dependency immediately before the existing callback; it implements the exact five-class grammar, immutable owner/pin binding, protected acquisition allocation, tracked use/close lifecycle, transfer edges, spent state and conservative recovery defined by the addendum's [local reference ownership](../specs/2026-09-16-signed-insert-select-schema-semantics-design.md#local-reference-ownership). The verifier arguments are `(ctx, expectedReferenceID, expectedPin, terminalProof)`: B1 reads the immutable expected binding from its durable journal and passes it explicitly; no wrapper may discard either expected value or obtain it from mutable closure/context state. Source and verifier share immutable artifact objects but use protected constructor-bound per-principal ownership partitions in the first profile; shared unrestricted writable storage and local ephemeral caches fail enablement. Remote object-store adapters can implement the artifact backend later without changing roots or owner identity.

Both modes require backend, authenticated published source, journal directory, a bound owner registry and terminal-proof verifier. With `publication == nil`, construction is explicitly read-only, exporter may be nil and Publish returns `ErrArtifactPublicationUnavailable` before any exporter/owner-registry/journal/readiness call; Retain/FetchPart/Release remain published-safe operations through the bound registry. Publishing mode requires nonnil publication and exporter. AC does not introspect an opaque control or invent a generic health method: C1/D3's concrete control constructor validates its authority verifier, separately configured publisher credential/authorization, fixed scope, committed reader and ready submitter. A control error fails Publish and never selects read-only behavior.

- [ ] **Step 1: Write publication/retention tests with a temporary object directory and journal.** Freeze A bytes/digest, certificate bytes/recovered address, O bytes/digest, the addendum's exact populated/empty artifact-set canonical bytes/roots and ready association. Exercise unsorted parts, duplicate structured identity, empty `parts:[]`, distinct physical/object digests and the exact tuple sort without inventing local fields or a hash domain. Test read-only construction and immediate Publish refusal with zero exporter, owner-registry, journal, authority, publisher or submit calls; published-safe reads/retention still work without publication secrets. Both modes refuse a missing/unbound owner registry. Test all five exact reference grammars/tails, canonical UTF-8 hex/decimal round-trip, exact segment count, unknown class, malformed namespace/serial/literal enum, noncanonical alternate spelling and use-as-path refusal. Test publishing construction refusing a missing exporter and the concrete control refusing missing authority/publisher/scope/reader/submitter wiring. For lookup, out-of-scope, follower, stale, invalid-proof, timeout and unavailable failures never become absence; cover fresh authenticated absence, identical committed-ready-unpublished retry, conflicting root, lost-response exact-root reconciliation and an old-authority committed retry with independent publisher auth. Publish and independently verify candidate artifacts before safe publication, including durable provisional retention registered after candidate seal and before object exposure; upload must not require prior membership in `PublishedSnapshotSource`. Reject unknown/duplicate/missing/noncanonical object fields, wrong purpose/key/role/network/shard/S, inner/outer digest confusion, high-S/bad-V, equal legacy hashes with different generation metadata and byte substitution before C1. Then test query consumption of an internally valid staged-but-unpublished manifest and require `Retain`/`Open`/`FetchPart` refusal despite artifact readiness. Also test a declared empty table, missing schema object, renamed location serving identical bytes, and reference retention across process reconstruction. Tests must inspect storage after a failed operation, not just a return code:

```text
publish S with complete schemas and two parts -> durable commit marker exists only after both parts verify
before safe publication: provisional reference survives restart; query consumption of S refuses
authenticated safe publication after readiness -> query Retain/Open/FetchPart may consume exact S
registry admits canonical reservation reference -> Retain attaches exact live registration -> restart store -> GC cannot delete either part
retain same reference twice -> one durable reference; different pin under same reference -> reject
same live natural-owner retry -> same acquisition A; new authorized historical replay -> new A; spent A never reopens
publication/reservation/accepted/replay/challenge disposition mismatch -> reject without weakening another class
tracked handle remains open or old process/fence is unproved -> close/release refuses and keeps objects
release without authenticated terminal proof -> reject and keep both parts
two references share one pin; proof for A presented to B -> reject and keep both bindings/parts
release with wrong reference, pin, reservation/generation, assigned block, statement/input/original JWS or predecessor/child -> reject
release verification races a mutation -> recheck the same binding before durable mutation and reject stale authorization
release then restart -> reference identity remains durably spent and cannot be rebound or resurrected
release after scheduling barrier ends but challenge retention remains -> keep both parts
corrupt or omit one upload -> no publication-ready acknowledgement
```

- [ ] **Step 2: Run HG `bazel test //pkg/replay:replay_test //pkg/auth:auth_test //pkg/replay/snapshotquery:snapshotquery_test` and AC `bazel test //dataplane:dataplane_test //snode:snode_test`.** Expect missing schema/certificate/ports/store APIs or failed retention/publication assertions before implementing them. Add the root replay, auth, snapshotquery, dataplane and snode BUILD registrations in this task.

- [ ] **Step 3: Implement the HG schema/certificate prerequisite first, then publish-before-safe readiness and durable references.** In HG root `replay`, implement and freeze A, O, both digest layers, complete legacy projection validation and the exact strict vectors in the [normative schema addendum](../specs/2026-09-16-signed-insert-select-schema-semantics-design.md); do not duplicate or alter its fields. In `auth`, implement the pure exact compact ES256K certificate payload/signing/verification and recovered-address primitive. These root packages must not import `snapshotquery` or `payloadexec` or create an auth/replay cycle; the leaf `snapshotquery` ports adapter maps an independently validated object to the unchanged `payloadexec.TableSchema` projection. Current schema-authority admission, publisher identity and historical-association rules remain in their existing B1/C1 controls, not these pure primitives. Implement AC's protected owner registry and five constructor-selected lifecycle capabilities exactly as the addendum specifies: immutable authenticated registration, fsynced namespace/serial allocation and retry key, tracked use leases, close/release authorization, durable successor handoff, spent tombstones/high-water marks and conservative recovery. Do not freeze the proposal's illustrative method names or expose arbitrary registration/`closed` inputs. Only after those HG contracts and AC ownership prerequisite pass may AC export exact selected candidate parts and O under content-addressed keys. `Publish` locally validates O/signature/digest layers/S/projection, computes the complete actual part/artifact root, and uses `LookupCommittedArtifactReady` to distinguish authenticated fresh absence from an existing association; every lookup error stops. For fresh absence, call `AuthorizeCurrentSchemaAuthority` with only the address recovered from O, finish complete byte/root checks and provisional fsync, call `AuthenticatePublisher`, construct the existing readiness record with that returned publisher/policy, call `RecordSnapshotArtifactReady`, and verify its returned pin/record/evidence. If lookup or lost-response reconciliation returns an association, compare every pin/readiness field and reject conflicts; only an identical root may skip current authority admission, after the same exact O/root/S/projection checks and an `AuthenticatePublisher` result whose publisher ID and retention policy match the committed readiness fields. Readiness cannot be exposed before complete bytes/root validation and provisional ownership, even if lookup precedes heavy export. This flow does not require published-safe membership and breaks the readiness/publication dependency cycle. Publication registration is derived from the sealed candidate and constructor-bound issuer lifecycle, before object exposure; it neither trusts Publish input identity nor calls publisher authentication early. Only a separately authenticated published-safe pin, its committed exact readiness association and an existing exact owner registration authorize query `Retain`, `Open` and `FetchPart`; readiness, a certificate or a well-formed reference string alone does not. Never delete old parts or O merely because a MergeTree merge made them inactive locally. Use atomic write/fsync/rename and a durable reference/owner journal; account for publication, reservation, accepted, replay and challenge ownership independently. Reconcile abandoned provisional uploads against the real control-plane publication/cancellation authority before GC; absent a proven submission-exclusion seam, retain/resume/quarantine rather than free on uploader disconnect or fresh absence.

```text
Publish(manifest, authenticatedSchema):
  validate exact O, both digest layers, certificate role/S and full legacy projection
  export each selected part from a stable frozen source view
  hash/check every artifact; fsync artifact and schema objects
  persist provisional publication reference before exposing readiness
  write/fsync readiness record binding snapshot_id, manifest_root and all artifact hashes
  acknowledge readiness; only the control plane may publish the safe snapshot

Retain(referenceID, pin):
  require existing live authenticated owner registration and verify exact published pin
  atomically attach referenceID -> exact pin and acquire tracked use leases
  reject rebinding an existing or spent referenceID; repeated identical retain succeeds only while live

Release(referenceID, terminalProof):
  load immutable referenceID -> exact pin binding from the durable journal
  require this acquisition closed/quiescent and its exact class disposition; persist release authorization
  verify terminalProof against that expected referenceID and exact pin outside long-held journal/GC locks
  immediately before mutation recheck the same live binding/registry state and persist a spent identity atomically
  release only this ownership; never treat verifier success as object-delete authority

GC(candidate):
  require no live/provisional/uncertain references, active leases, handoffs or authorized-not-spent entries
  require authenticated retention horizon passed
  recheck under the GC/reference lock, then delete the immutable object
```

Repeated Release is idempotent only for the already-spent binding and cannot authorize a recreated owner. Failure, timeout, cancellation, mismatch or a concurrent binding change leaves the live ownership, O and all parts intact. A scheduling/query terminal proof cannot release provisional publication ownership without its own authenticated publication/cancellation disposition, and a scheduling reference cannot be released while it is the only durable protection for an unresolved challenge or cleanup obligation. Object GC remains a separate decision requiring zero durable ownerships plus the authenticated retention horizon.

Use the existing part physical-hash authority where available; freeze its archive/layout interpretation in a profile vector. The content-addressed object key may be separate from `part_phys_hash` if the archive container has its own digest: verify both and never confuse compressed archive bytes with the authenticated part-content hash. Interrupted uploads are uncommitted temporary objects, never readable parts.

- [ ] **Step 4: Run retention fault injection, cold fetch and the complete local consumer gate.** Run HG `bazel test //pkg/replay:replay_test //pkg/auth:auth_test //pkg/replay/snapshotquery:snapshotquery_test` and AC `bazel test //dataplane:dataplane_test //snode:snode_test`. Crash during allocation/registration, O/part upload, fsync, readiness, Retain, handoff, tracked-use close, release authorization, external verification, spent-marker persistence and object GC; reconstruct from disk, and verify no referenced object or original certificate disappears or a namespace/serial/spent identity becomes reusable. Cover every class, the exact callback arguments, verifier refusal/mismatch, A/B references sharing one pin, parallel same-job acquisitions, natural-owner retry versus new historical replay, wrong pin/reference/owner/principal/namespace, other live references, active handle refusal, challenge transfer and verification-versus-GC races. Old journal IDs are explicitly migrated from authenticated origin evidence or quarantined, never guessed. Recovery proves the exact old process/children stopped or an enforced fence before resuming the same A; PID/time/startup/counter evidence refuses. Combine crash immediately after K1 O fsync with a requested K1-to-K2 rotation, and committed readiness with a lost response plus that rotation; require the pre-removal drain or authenticated identical-committed-root branch to finish exact O before K1 removal. Require a changed hint to work only for identical authenticated bytes, and preserve the exact O through authority rotation for historical reads. This closes only B1's local parser/registry/lease/journal/restart/fault/GC mechanics and fail-closed fake-evaluator consumer gate; real class origin/disposition, cancellation/horizon authority and process identity/fencing remain pending C1/C2/C3/B5/C5/D3 gates, repeated through actual process transport in D4. Preserve existing safe publication behavior while the feature is off.

- [ ] **Step 5: Commit HG `feat(snapshot): define authenticated snapshot schema` before AC `feat(snapshot): retain authenticated replay artifacts`.** The HG commit contains schema, certificate and ports prerequisites; pin that commit before AC implementation. No live network is made artifact-capable merely by adding the manifest reader or by passing B1's local fake-verifier gate.

### Task B2: Restore and verify read-only scratch relations

**Files:** HG scratch and AC restore rows; use B1's ports and A3/A5 scratch-binding contract.

**Interfaces:** AC `NewSnapshotReadStore(published PublishedSnapshotSource, artifacts SnapshotArtifacts, restorer snapshotquery.ScratchRestorer) snapshotquery.SnapshotReadStore` returns the implementation of HG's interface. It verifies the selected published manifest's exact authenticated schema object, then calls `ScratchRestorer.Restore(ctx context.Context, manifest replay.SafeSnapshotManifest, schemaArtifact replay.AuthenticatedSnapshotQuerySchemaV1, schemas []payloadexec.TableSchema, reads replay.SnapshotReadSet, parts []VerifiedPart) (snapshotquery.ReadSnapshot, error)` with that exact object and its validated legacy projection. The restorer independently derives/checks the supplied projection, retains an immutable copy of `schemaArtifact` for `ReadSnapshot.SchemaArtifact()`, and never reconstructs O from `schemas`; `Schemas()` returns only the validated unchanged projection. `VerifiedPart{Entry replay.PartManifestEntry, LocalPath string}` follows download verification; HG `chexec.NewSnapshotRestorer(admin clickhouse.Conn, readerFactory func(context.Context, []snapshotquery.Relation) (clickhouse.Conn, error)) snapshotquery.ScratchRestorer` supplies the port. The factory must provision/read with grants restricted to the restored relations, not return `admin`. Place the `ScratchRestorer`/`VerifiedPart` port types in HG `snapshotquery/ports.go` so HG does not import AC.

- [ ] **Step 1: Add complete-ledger adversarial tests.** Construct S with read table R, target W and unrelated U. Require exact relation membership; remove/duplicate/add a part, change table/partition/schema/row count/bytes, corrupt physical bytes or row LtHash, and require refusal before `QueryRows`. Pass two authenticated objects with the same legacy projection but different semantic bytes and prove the returned handle retains only the exact supplied object; projection-only reconstruction or object substitution refuses. The empty R case must create an authenticated empty relation; removing R from the manifest must fail.

```text
Open(S, {R}, ref): published proof -> retain -> complete descriptor equality -> fetch all R parts
verify physical hashes -> attach isolated parts -> scan imported rows including _hg_row_id
sum RowElementHash per part -> part LtHash -> complete partition roots -> expose read-only R
QueryRows("SELECT value FROM R_scratch", W_schema): return coerced user values only
attempt production table/catalog/remote read on that reader: permission/profile refusal
Close(): close reader, remove only this operation's scratch relations, retain durable ref
```

- [ ] **Step 2: Run HG `bazel test //pkg/replay/chexec:chexec_test` and AC `bazel test //dataplane:dataplane_test`.** Unit fakes must prove the denial order. Register a Docker case for actual grants/ATTACH/readback in D4; do not claim fake connections prove SQL isolation.

- [ ] **Step 3: Implement authenticated restore and the restricted handle.** Verify the D1 publication record, committed exact O/readiness association, schema/root/profile identity, complete legacy projection and all descriptor entries before trusting bytes. Copy the supplied typed object immutably onto the handle for B4, derive/check `Schemas()` from it and never reconstruct or choose O through the lossy projection; current live metadata never substitutes. Download all active parts of R, not predicate-selected partitions. Validate per-part physical hash, rows, schema and partition; scan with the existing shared row authority and fold every part into its partition commitment. Use an immutable imported copy or a proven immutable hardlink/reflink; do not attach mutable live part paths. Scratch names are random operation-local transport names excluded from commitments.

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

New `snapshotquery.Executor` receives `NetworkID string`, `Snapshots SnapshotReadStore`, `Analyzer rewriter.SnapshotQueryAnalyzer`, `Profiles ProfileRegistry`, and `Appender *payloadexec.Executor`; it implements `Replay(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error)`. `ProfileRegistry.Lookup(executorID, queryID string) (Profile, bool)` returns `Profile{ExecutorProfileID string, QueryProfileID string, Record replay.QueryProfileRecord}`. Construction resolves and freezes one exact executor/query pair and one analyzer that has probed that Q; this B4 executor is not a historical engine selector. For every run it verifies the exact immutable object carried through `ScratchRestorer.Restore` and returned by `ReadSnapshot.SchemaArtifact()`, plus the committed exact-S publication policy, builds the RP catalog from that authenticated complete metadata, and uses `Schemas()` only as its validated legacy projection for scratch/state assembly. Equal legacy projections never permit substituting or reconstructing the authenticated object. Bounds are `profile.Record.Limits`; the remaining record fields pin engines/settings/types/operators as specified in A4. It does not decide admission authorization; C1 does. The executor receives B5's `HistoricalPolicy` port too, so direct source invocation must prove accepted reservation provenance rather than trusting a caller-populated job.

Add proposed in-process `PreparedQueryExecution{Result replay.ExecutionResult, Output CanonicalOutput}` with `Close() error`, and `(*Executor).Prepare(ctx context.Context, req replay.ExecutionRequest) (*PreparedQueryExecution, error)`. A successful Prepare transfers ownership of the complete canonical output handle to the caller; it closes temporary restore handles but must not close that output. `Replay` wraps Prepare with deferred Close and returns Result, preserving the existing `replay.Executor` interface. C5 uses Prepare and `Output.OpenRows()` to durably copy/reopen the exact canonical rows before closing the handle. This is no wire result expansion, no source-row input to the verifier, and no second SELECT. Caller-owned cache durability/retention remains C5's responsibility.

- [ ] **Step 1: Add whole-ledger regression tests before extracting code.** Use the existing payload executor fixtures with tables R, W and U; after appending to W, assert exact preserved R/U table/partition/part entries and parent linkage. Add query cases with R=W, zero output and two sequential safe self-inserts. Require an ineligible target or referenced R to refuse before analysis while an ineligible untouched U is preserved unchanged. Drop a table, duplicate a statement ID, substitute the schema object or mismatch a legacy projection in direct `ApplyRows` input and require refusal.

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
snapshotquery.Executor.Prepare(req):
  require exactly one req.SnapshotQuery; require supported exact executor/query profiles
  verify job/pin/descriptor/schema identity, exact O and committed original publication provenance
  open retained S using statement/block reference; independently analyze signed materialized SQL
  build catalog from authenticated full metadata; require touched-table eligibility
  compare full read closure/descriptor; prepare SELECT bound only to restored relations
  stream target-typed rows; Canonicalize through complete EOF
  pass globally ordered rows to Appender.ApplyRows using the job's assigned sequence
  require complete predecessor table set; attach applied output count/root evidence
  close scratch handles; return PreparedQueryExecution transferring canonical output ownership
snapshotquery.Executor.Replay(req): Prepare -> defer prepared.Close -> return prepared.Result
```

The executor is independently reusable by source and verifier; it never calls `prepareAndSubmit`, writes production unsafe tables or re-materializes volatility. A missing `SnapshotQuery` is not inferred from empty payload. The composite dispatcher in B5 invokes the legacy executor explicitly for legacy requests.

- [ ] **Step 4: Run payload regression, query state and separate-instance tests.** HG targeted replay/payloadexec/chexec/snapshotquery tests must pass. Include old adapters that drop metadata, current live semantics differing from committed O, equal legacy hash with different generation, and original O restoration after restart. D4 supplies two real ClickHouse instances with different local live data and part order; compare canonical output, global IDs, LtHash and complete post-state. Do not claim physical part byte equality across instances is required for equal logical data roots.

Test Prepare ownership with an output handle that detects premature Close: C5 can read all rows through EOF after Prepare, durably reopen its copied cache after Close/restart and obtain identical global IDs/root/count. Replay closes its output exactly once on success/error; Prepare errors release temporary resources and return no partial handle. A failed cache fsync prevents unsafe writes. Preserve independent verifier Prepare/Replay execution and reject any attempt to feed the source cache as replay input.

- [ ] **Step 5: Commit `feat(replay): execute snapshot queries with shared state assembly`.** Include the full v2 vector-preservation result in the PR; any changed legacy digest blocks the task.

### Task B5: Sign versioned query receipts and wire historical dispatch

**Files:** HG query verifier/dispatch row; AC verifier/wire row. Depends on A2, B4 and C1's authenticated historical-policy lookup port; a deterministic fake can test that port before AR is implemented.

**Interfaces:** `snapshotquery.Verifier` receives B5's dispatcher, `replay.Signer`, an A2 pure signature validator, and `HistoricalPolicy.VerifyReservation(ctx context.Context, job replay.SnapshotQueryJob) error`. It exposes `Verify(ctx context.Context, job replay.SnapshotQueryJob) (replay.SnapshotQueryAttestation, error)`. `CompositeExecutor` holds `Payload replay.Executor` plus a constructor-copied immutable map from `QueryProfileKey{ExecutorProfileID string, QueryProfileID string}` to already-constructed `replay.Executor`. The verifier validates the authenticated original policy before lookup; dispatch then routes only the explicit in-process variant and exact pair. AC `NewSnapshotQueryReplayCore` constructs the query verifier beside the existing `NewReplayCore`, and verifier dispatch gets a separate AP query-job variant and query-attestation reply. Its trusted wrapper also owns one `replay` acquisition for the configured authenticated verifier principal under the addendum's [local reference ownership](../specs/2026-09-16-signed-insert-select-schema-semantics-design.md#local-reference-ownership): register/Retain before restore or execution, track every read/scratch/execution handle, close only after they quiesce, then seek release authorization for that exact A. This is the minimal historical-routing ownership defined by the [build-identity addendum](../specs/2026-09-16-signed-insert-select-build-identity-design.md).

- [ ] **Step 1: Write receipt/refusal and dispatch tests.** Assert a valid result with wrong source root returns a signed `match_source_root=false` receipt; invalid signature/unpublished pin/missing part/schema object/unsupported profile/stream failure returns no attestation. Require authenticated original-policy and exact committed original schema-object association before dispatch. Historical replay uses its retained executor and original O/certificate after current activation or schema-authority changes; old profile JSON without an executable route, a substituted backend/endpoint/object, an unknown pair or a missing route refuses without calling any executor or producing a receipt. Test two verifier attempts for one job as distinct A values, same-live-operation retry recovering A, active A refusing old terminal/close evidence from another acquisition, and a newly authorized historical replay obtaining a new A after the prior one is spent. A legacy request invokes only the legacy executor.

```go
func (d CompositeExecutor) Replay(ctx context.Context, req replay.ExecutionRequest) (replay.ExecutionResult, error) {
    if req.SnapshotQuery != nil {
        key := QueryProfileKey{req.SnapshotQuery.ExecutorProfileID, req.SnapshotQuery.QueryProfileID}
        executor, ok := d.queryByProfile[key]
        if !ok { return replay.ExecutionResult{}, fmt.Errorf("snapshot query profile route unavailable") }
        return executor.Replay(ctx, req)
    }
    if d.Payload == nil { return replay.ExecutionResult{}, fmt.Errorf("payload executor unavailable") }
    return d.Payload.Replay(ctx, req)
}
```

The dispatcher code is intentionally small; its constructor rejects duplicate/invalid keys and copies the map so later caller mutation cannot change routing. The selected B4 executor validates its exact profile and shape. Add zero-row applied receipt and aborted-outcome separation tests: abort evidence is produced from C3's authenticated control transition, never by converting an execution error to applied success.

- [ ] **Step 2: Run HG `bazel test //pkg/replay/snapshotquery:snapshotquery_test` and AC `bazel test //verifier:verifier_test //conformance:conformance_test //wire:wire_test`.** Require missing query dispatch/new receipt support to fail visibly.

- [ ] **Step 3: Implement independent signature/input/descriptor/policy validation before replay and new receipt hashing after replay.** Recompute A1 input/statement/read roots, verify the original JWS without today's freshness rule, validate the authenticated original reservation/activation policy and exact committed original readiness/publication association, then select the immutable exact-pair route, verify retained O/certificate bytes without today's authority allowlist or token-age rule, and independently analyze SQL and restore S. Build the catalog from authenticated metadata and refuse touched-table ineligibility. A missing route/object/proof refuses before execution and produces no receipt. Require C5's complete committed claim, check `claim.Hash() == job.SourceClaimRoot`, and compare computed state/output count/output root with the claim's corresponding fields. Do not compare computed state to the compound claim digest. A valid replay binds output evidence and full state to `snapshot-query-receipt-v1`; use `replay.Signer.SignReplayReceipt` on that new hash. Preserve old `ExecutionReceipt.Hash()` and the v2 verifier path.

AC's verifier handler dispatches new query jobs separately, signs/returns the new attestation and retains its exact evidence. Its lifecycle wrapper derives WORKER from verified/configured credentials, never `ReplicaID` or a process label, and closes its own replay acquisition only after all execution/submission handles join; terminal history cannot close an active acquisition. Keep byte-side scans of exact candidate parts through `chexec.ScanParts` and `RowElementHash`. A correct output hash with different candidate bytes must still fail the AR three-way gate in C5. Source and verifier caches cannot substitute for independent execution.

- [ ] **Step 4: Run differential fraud tests and legacy conformance.** Source executes using altered unsafe/live rows, substitutes one normalized output row, swaps an output cache or candidate part, or drops U from the assembled state. Require signed mismatch/challenge evidence when valid replay disagrees and no promotion in each fraud case. Keep local restore unavailability distinct from malicious-source mismatch.

- [ ] **Step 5: Commit `feat(verifier): attest snapshot query execution evidence`.** Release HG before AC updates its import pins; SN does not instantiate this verifier and must not gain a fictitious verifier constructor in D3.
