# Snapshot-query authenticated schema semantics

**Status:** Normative bounded correction for the future signed `INSERT ... SELECT` capability. It changes no implemented runtime behavior and qualifies no existing schema, snapshot or deployment.

This addendum defines the column-eligibility metadata, authenticated exact-snapshot schema object, committed publication association, five-class reference grammar and automatic trusted issuance required by the [signed `INSERT ... SELECT` design](2026-09-16-signed-insert-select-design.md). The artifact-lifecycle addendum separately governs [archive capacity and safe handoff](2026-09-17-signed-insert-select-artifact-lifecycle-design.md#6-safe-acquisition-extraction-and-immutable-handoff), the [exact disposition protocol](2026-09-17-signed-insert-select-artifact-lifecycle-design.md#7-exact-disposition-protocol-surface) and [candidate/use/obligation/retirement lifetime](2026-09-17-signed-insert-select-artifact-lifecycle-design.md#9-candidate-use-obligation-and-retirement-lifetime). Together they preserve every A1/AP/v2 field and hash, the existing `payloadexec.TableSchema`, all legacy table/schema/state formulas, the reviewed [analyzer build identity](2026-09-16-signed-insert-select-build-identity-design.md), all A1–A14 gates and default-disabled behavior.

## 1. Closed column eligibility

A3 appends the following enum and fields without renumbering or reusing `SnapshotQueryColumn.name = 1`, `type = 2`, any other A3 field or any RPC:

```proto
enum SnapshotQueryColumnGeneration {
  SNAPSHOT_QUERY_COLUMN_GENERATION_UNSPECIFIED = 0;
  SNAPSHOT_QUERY_COLUMN_GENERATION_ORDINARY = 1;
  SNAPSHOT_QUERY_COLUMN_GENERATION_DEFAULT = 2;
  SNAPSHOT_QUERY_COLUMN_GENERATION_MATERIALIZED = 3;
  SNAPSHOT_QUERY_COLUMN_GENERATION_ALIAS = 4;
  SNAPSHOT_QUERY_COLUMN_GENERATION_OTHER = 5;
}
SnapshotQueryColumnGeneration generation = 3;
string default_expression = 4;
```

The initial profile admits only explicit `ORDINARY` with an empty `default_expression`. `UNSPECIFIED`, unknown numeric values, `DEFAULT` including a constant default, `MATERIALIZED`, `ALIAS`, `OTHER`, and `ORDINARY` with a nonempty expression refuse. Missing old-client metadata is `UNSPECIFIED`, never evidence of ordinary semantics.

Every declared user column of the target and every declared user column of each actually referenced read table must pass this rule before analysis; omitting a generated/default column from an explicit target list does not make the table eligible. The protocol-owned row-id remains separate. An unrelated untouched table U may be ineligible and remains preserved in the complete ledger without invalidating the query. Existing AST validation, unused-definition inspection, target-column permutation and `ResolveColumnProfile` rules remain additional gates.

## 2. Canonical schema artifact and certificate

The HG root `replay` package owns these strict canonical records with the exact ordered fields below:

```text
SnapshotQuerySchemaArtifactV1 {
  kind: "snapshot-query-schema-artifact-v1"
  version: uint32 = 1
  network_id: string
  keeper_shard_id: uint32
  snapshot_id: string
  manifest_root: string
  schema_snapshot_id: string
  schema_root: string
  tables: []SnapshotQueryTableSchemaV1
}
SnapshotQueryTableSchemaV1 {
  table_id: string
  schema_hash: string
  partition_by: string
  columns: []SnapshotQuerySchemaColumnV1
}
SnapshotQuerySchemaColumnV1 {
  name: string
  type: string
  generation: uint32
  default_expression: string
}
AuthenticatedSnapshotQuerySchemaV1 {
  artifact: SnapshotQuerySchemaArtifactV1
  authority_jws: string
}
```

Tables are nonempty when the manifest is nonempty, unique and sorted by `table_id`; columns preserve complete schema order and are unique by name. Strict decoding rejects unknown, duplicate, missing or `null` fields, malformed digests, alternate ordering/escaping, incomplete tables/columns and unsupported kind/version. Re-encoding must equal the bounded exact input bytes. Projecting each table to the unchanged legacy `{table_id, partition_by, columns{name,type}}` form must reproduce its `schema_hash`; the complete projection must reproduce S's `schema_root`. Generation/default semantics are authenticated by this new object and do not become part of the legacy hashes.

The HG `auth` package owns the pure certificate payload and compact signing/verification primitives without importing `snapshotquery` or creating an auth/replay cycle. The exact ordered payload is:

```text
SnapshotSchemaCertificatePayloadV1 {
  purpose: "housegate-snapshot-query-schema-v1"
  version: uint32 = 1
  artifact_digest: string
}
```

Let A be the strict canonical JSON bytes of `SnapshotQuerySchemaArtifactV1`. Set `artifact_digest = replay.DigestBytes(A)`. `authority_jws` is the exact compact token over that payload. Let O be the strict canonical JSON bytes of `AuthenticatedSnapshotQuerySchemaV1`. The unchanged `SnapshotArtifactSet.SchemaArtifactDigest` is `replay.DigestBytes(O)`, and the artifact-set root commits that outer digest. The order is A, inner digest, JWS, O, outer digest, artifact-set root, readiness and publication. The certificate never signs the outer digest, so no digest or signature contains itself.

The certificate uses the existing Housegate compact ES256K convention exactly:

```text
protected header bytes = {"alg":"ES256K","typ":"JWT"}
signing input = raw-base64url(header) + "." + raw-base64url(canonical payload)
message hash = Keccak256(UTF-8 signing input)
signature = canonical secp256k1 R || S || V (65 bytes, low-S, V=27/28)
token = signing input + "." + raw-base64url(signature)
```

Verification recovers the lowercase Ethereum `0x` address, which is the key identity. Reject high-S, bad V, alternate headers/algorithms/encoding, `kid`, `jwk`, certificate URLs, claimed roles and caller-supplied authority identities. The payload has no `iat`, `exp` or `aud`, and historical verification applies no token-age rule. The dedicated schema-authority credential is separately configured and network/shard scoped; it never defaults to a user, source, generic publisher or relay key.

### Artifact-set commitment

The existing A1 artifact-set records have exactly these ordered fields and no kind, version, S, path or storage-reference field:

```text
SnapshotArtifactSet {
  parts: []SnapshotArtifactEntry
  schema_artifact_digest: string
}
SnapshotArtifactEntry {
  table_id: string
  partition_id: string
  part_name: string
  part_phys_hash: string
  object_digest: string
  bytes: uint64
}
```

Canonicalization copies `parts`, rejects an empty `schema_artifact_digest`, any empty part-entry string field and duplicate structured `(table_id, partition_id, part_name)` identities, sorts entries by that exact lexicographic tuple, and preserves every entry field. An empty set encodes `parts:[]`, never `null` or omission. Do not sort by either digest, collapse duplicate identities or conflate `part_phys_hash` with `object_digest`. `schema_artifact_digest` is `DigestBytes(O)`, never the certificate's `DigestBytes(A)`.

`ArtifactSetRoot = CanonicalDigest("snapshot-query-artifact-set-v1", normalizedSet)`: `0x` plus lowercase SHA-256 of the UTF-8 bytes `housegate-replay-mvp-v0:` + `snapshot-query-artifact-set-v1` + one NUL byte + compact canonical JSON under the root replay field-order and JSON string/`uint64` profile. Each part `object_digest`/`bytes` commits the exact HGPART v1 transport object defined by the artifact-lifecycle addendum while `part_phys_hash` retains its distinct ClickHouse covered-file role. Object integrity, selected-manifest completeness, sealed complete-tree capture, exact byte counts and the authenticated S/readiness association are separate B1/C1 checks; the ancillary hash alone is not admission authority.

The frozen `constant_empty_reads.contracts` vectors below are synthetic canonical commitment vectors, not valid schema certificates or published snapshots. Equal `part_phys_hash` and `object_digest` values in the populated synthetic case establish no equality rule. The complete fixture is pinned at SHA-256 `3558d62035a23f4e572d09a59f82bc0ebac4f9cae175ccee127b0034600ed137` in [`snapshot_query_v1.json` at `7ed1259a`](https://github.com/housegate/housegate/blob/7ed1259a702f83990149eaa95e9cc43e8b2d9015/pkg/replay/testdata/snapshot_query_v1.json).

```text
artifact_set canonical JSON = {"parts":[{"table_id":"events","partition_id":"1","part_name":"p1a","part_phys_hash":"0x47ea7b3b0757e53b1c1ec281d24bae7216435467d0a6d9622606e56a9a9691ac","object_digest":"0x47ea7b3b0757e53b1c1ec281d24bae7216435467d0a6d9622606e56a9a9691ac","bytes":128},{"table_id":"events","partition_id":"1","part_name":"p1b","part_phys_hash":"0x5ee7c79afddbbf5d9208d40bb07397919db80e9c5ff5cb8d9014b0d4f3b194c7","object_digest":"0x5ee7c79afddbbf5d9208d40bb07397919db80e9c5ff5cb8d9014b0d4f3b194c7","bytes":128},{"table_id":"events","partition_id":"2","part_name":"p2a","part_phys_hash":"0xe9cc5bf143e2041791518df5e5b2b15ee762f401ea92eb30ff8452ea5e6cd232","object_digest":"0xe9cc5bf143e2041791518df5e5b2b15ee762f401ea92eb30ff8452ea5e6cd232","bytes":128}],"schema_artifact_digest":"0xe1902f64876ca35f2dc8f2109c25e606a593c5aa4926194bdb3e02f1352622f8"}
artifact_set root = 0xfa6eadd01f43632dbbf9f7db82e2a8d06e154786398d5ab97b3226e5a0378354
empty_artifact_set canonical JSON = {"parts":[],"schema_artifact_digest":"0xe1902f64876ca35f2dc8f2109c25e606a593c5aa4926194bdb3e02f1352622f8"}
empty_artifact_set root = 0xa84173887aeda7bc80df6133fd86e64b3926ae144c040fe1b6976b47799a7147
```

This preserves the sequence A, inner digest, JWS, O, outer digest, artifact-set root, readiness and publication, and C1's unique committed exact-S association.

## 3. Independent verification and exact-S commitment

B1 and C1 independently fetch bounded exact O bytes from the artifact store, strictly decode and byte-for-byte re-encode them, verify both digest layers and the complete artifact-set root, verify the exact purpose/version/signature and recovered role, match network/shard/snapshot/manifest/schema identities, and recompute the complete legacy projection. No publisher availability signature, current live metadata, old `SchemaJson` hash, boolean, self-consistent manifest or uncommitted certificate supplies semantic or publication proof.

Before first readiness, B1 passes only the address recovered from exact O to its fixed-scope publication control for dedicated current schema-authority authorization, verifies the frozen capture provenance, persists the exact O and all parts, retains provisional ownership and then uses the control's separately authenticated publisher identity to submit readiness. C1 fetches O independently, repeats byte/digest/signature/S/projection checks, authorizes the current schema-authority role for first commitment, separately authenticates the publisher, and only then proposes readiness. Fetch, crypto, capture and role work remain outside Raft apply; deterministic identity/conflict checks occur in apply.

C1 commits at most one immutable `ArtifactSetRoot` for `(network_id, keeper_shard_id, snapshot_id, manifest_root)`. An identical retry succeeds; a conflicting root refuses before or after publication. Signing the same A again into different JWS/O bytes cannot replace the committed root. A location change may serve only the identical O. This unique authenticated committed readiness/publication association lets A1's signed exact S bind the semantic artifact without adding an A1 field.

Historical consumption verifies the retained exact O/certificate, S binding and authenticated committed original readiness/publication association. It does not require the old address in today's allowlist and does not apply current token age. A newly presented, previously uncommitted certificate from a removed key refuses. A retry that discovers the identical root already committed independently revalidates exact O, both digests, artifact-set root, S/projection and authenticated committed readiness evidence, and retains publisher authentication; it reuses that original association without reapplying today's schema-authority admission. This exception applies only to an identical already-committed association, never a first commitment or conflicting root.

Trust-map rotation first pauses new issuance while leaving the old fixed authority map effective for the drain. It drains every candidate with a durably signed O: fsynced-before-upload, uploaded/prevalidated, readiness committed with its response lost, and ready-but-unpublished. Each must publish/commit as specified or reach an authenticated terminal disposition that safely releases its ownership before the old key is removed. The pause must not block this drain, and a consumed query must not wait on its own barrier. An unavailable dependency delays rotation; it does not authorize abandoning, overwriting or re-signing O. Only after the drain completes may participants remove the old key and resume; a K1 object's first commitment then refuses. Preserve original bytes and committed proofs through restart, migration, follower catch-up, rotation, replay and challenge; never re-sign or replace historical O in place.

## 4. New-lane ports and consumers

`SnapshotArtifacts.Publish` receives `replay.AuthenticatedSnapshotQuerySchemaV1` with the candidate manifest instead of treating `[]payloadexec.TableSchema` as semantic authentication. `PublishedSnapshotSource.GetPublishedSnapshot` returns the manifest and verified typed object. `ReadSnapshot.SchemaArtifact()` returns that typed object, while `Schemas()` remains its validated legacy projection for B2/B4 row and state assembly. The new-lane `ScratchRestorer.Restore` receives the typed object and the validated legacy projection as separate arguments, verifies that projection, and retains an immutable copy of the exact object in its returned `ReadSnapshot`; it never reconstructs O from `[]payloadexec.TableSchema`. D1's `SnapshotCatalogSource.Load(pin)` returns the manifest and verified object after checking the exact selected-S publication proof. These changes apply only to new-lane ports and create no HG-to-AC dependency.

A5, D1, D2, B4 and B5 build RP catalogs only from the authenticated object's complete columns, revalidate exact S/projection/publication policy before final signing, host intake, source execution and verifier execution, and transport `generation` plus `default_expression` unchanged. Engine `SUCCESS` validates supplied metadata/profile behavior but never proves publication. B4 still supplies only the validated legacy projection to shared row/state assembly and preserves U in the complete ledger.

### Historical policy and output ownership

HG defines one neutral `snapshotquery` decision port shared by B4 and B5. C1 supplies its authenticated production adapter from fixed network/shard scope, trusted endpoint identity and committed-history proof validation. The port is an in-process contract only: it adds no wire field, proof encoding, certificate, proto tag, hash domain or admission mechanism.

```go
type HistoricalPolicy interface {
    // Success means original accepted assignment, historical activation and
    // exact committed published-ready association have been authenticated.
    // This does not admit an artifact use or release any retained reference.
    VerifyReservation(context.Context, replay.SnapshotQueryJob) (HistoricalDecision, error)
}

// All stored fields contain values only, with no caller-owned slices/pointers.
// The zero value is invalid. The type has no wire representation or hash.
type HistoricalDecision struct {
    reservation          replay.SnapshotQueryReservation
    activation           replay.ActiveQueryPolicy
    blockSeq             uint64
    statementRoot        string
    ready                replay.SnapshotArtifactReady
    schemaArtifactDigest string
}

func (d HistoricalDecision) CheckJob(replay.SnapshotQueryJob) error
func (d HistoricalDecision) Reservation() replay.SnapshotQueryReservation
func (d HistoricalDecision) Activation() replay.ActiveQueryPolicy
func (d HistoricalDecision) Ready() replay.SnapshotArtifactReady
func (d HistoricalDecision) SchemaArtifactDigest() string

// Constructor input for the trusted C1 adapter; not an RPC request or proof.
// The name does not assert that arbitrary caller records are authenticated.
type HistoricalDecisionRecords struct {
    Reservation   replay.SnapshotQueryReservation
    Activation    replay.ActiveQueryPolicy
    BlockSeq      uint64
    StatementRoot string
    Ready         replay.SnapshotArtifactReady
    Artifacts     replay.SnapshotArtifactSet
}

func NewHistoricalDecisionFromVerifiedRecords(
    job replay.SnapshotQueryJob,
    records HistoricalDecisionRecords,
) (HistoricalDecision, error)
```

The constructor is a structural binder after authentication, not an authentication primitive. It rejects incomplete identities, invalid digests, invalid original activation, network/shard/job/reservation/pin/assignment mismatches, readiness identity mismatches and an artifact-set root different from the independently authenticated committed readiness. The decision binds the original reservation ID, fence generation, client and statement; original activation and executor/query pair; exact pin; committed assigned block sequence and statement root; and readiness/artifact-set root, publisher and retention-policy identity. It recomputes the input, read-set and statement roots, binds the original JWS through the statement root, validates the exact predecessor/schema/profile pair and retains immutable scalar copies only. Its accessors return value copies, it owns no resources, and any error returns a zero decision. Its expected outer O digest is `records.Artifacts.SchemaArtifactDigest` only after the complete artifact-set preimage hashes to `records.Ready.ArtifactSetRoot` and C1 has authenticated the original committed readiness/publication association. Computing an expected digest from the returned read object's O would be self-comparison, not independent authority. The decision excludes `SourceClaim` and `SourceClaimRoot`, which do not exist before source execution.

Production code may construct a decision only through the fixed trusted C1 adapter. Explicit deterministic fixtures may exercise consumers, but a fake, structural constructor, profile file, caller-selected endpoint, context value, nonempty proof bytes, current policy, permissive nil or allow-all implementation supplies no authority. Missing, stale, follower, unauthenticated, absent, invalid or out-of-scope evidence refuses. Historical original facts likewise do not admit current use or authorize retention, retirement, challenge or release.

B5 copies and validates the complete job, verifies the original user signature under the existing historical rules, then calls `HistoricalPolicy.VerifyReservation`, calls `decision.CheckJob`, and only afterward selects the immutable executor/query pair. B4 independently repeats the policy call and job check before `SnapshotReadStore.Open`, including direct source invocation. Duplicate authenticated reads are accepted for the first version; no caller-supplied decision bypass exists. B4 compares the returned exact O with `decision.SchemaArtifactDigest()` through the existing authenticated-profile validator before analyzer, query or appender work. B4/B5 use the retained original activation and certificate without substituting today's active policy or allowlist.

B3's prescribed `Canonicalize(ctx, networkID, statementID, schema, input, limits, tempDir)` receives no signed input object or full input root. Its ephemeral runs and output bind the exact network/statement/schema context plus local integrity, count and output identities; each `OpenRows` validates those bindings. B4 authenticates the selected pin/schema/descriptor/signed statement and full input root before supplying the stream. C5 durably copies and reopens the exact canonical output under that authenticated full input root plus output count/root before unsafe writes or restart reuse. Neither B3's internal context hash nor a source cache replaces this composed binding or independent verifier execution.

`ApplyRows` borrows `RowSource`; its caller owns and closes the stream even after errors. The legacy adapter closes streams it creates, B4 closes canonical cursors it opens, and any required owner close failure prevents successful adapter/orchestration completion. Direct `ApplyRows` tests cover complete predecessor/configured-schema/legacy-projection consistency and ledger preservation. O1/O2 equal-projection and certificate substitution tests belong to B4 Prepare/Replay, which receives O. No O, policy argument or synthetic payload reference is added to legacy append APIs.

B4a (shared streaming append/state assembly and unchanged v2 vectors) and B4b (the neutral decision port, request/result plumbing and single-pair Prepare/Replay orchestration) are independent milestones of the existing B4 task. Full B4 still requires real B1/B2/C1 integration and D4 independent execution. These contracts describe pending work and do not establish implementation, deployment or live acceptance.

### Publication control injection

AC dataplane owns these local dependency types; their public Go shape remains local and adds no field or hash. When the disposition capability is enabled, the concrete implementation is bound to one exact candidate and uses the new disposition control service defined by the artifact-lifecycle addendum rather than an unscoped old mutation path:

```go
type ArtifactPublisher struct {
    PublisherID string
    RetentionPolicyID string
}
type CommittedArtifactReady struct {
    Pin replay.SnapshotPin
    Ready replay.SnapshotArtifactReady
    Evidence []byte
}
type ArtifactPublicationControl interface {
    AuthorizeCurrentSchemaAuthority(context.Context, replay.SnapshotPin, string) error
    LookupCommittedArtifactReady(context.Context, replay.SnapshotPin) (*CommittedArtifactReady, error)
    AuthenticatePublisher(context.Context, replay.SnapshotPin) (ArtifactPublisher, error)
    RecordSnapshotArtifactReady(context.Context, replay.SnapshotPin, replay.SnapshotArtifactReady) (CommittedArtifactReady, error)
}
```

`Evidence` is C1's existing authenticated committed-proof encoding. The control is immutably bound to trusted configured network/shard, endpoint, trust map and publisher identity; an out-of-scope pin refuses, and no manifest/caller selects an endpoint or role. `AuthorizeCurrentSchemaAuthority` accepts only the address B1 recovered cryptographically from exact O and checks the dedicated current fixed map. `AuthenticatePublisher` independently derives the configured publisher and retention policy after authentication; Publish input carries neither publisher credentials nor a schema private key.

`LookupCommittedArtifactReady` performs authenticated authority, fresh leader-Barrier and C1-proof validation and can return ready-but-unpublished state. Only authenticated fresh absence returns `(nil, nil)`; transport, auth, proof, scope, timeout, follower, stale or unavailable failures stop Publish. A conflict returns the original complete committed association for exact comparison, never absence. An identical root enables only the existing committed retry exception after B1 rechecks exact O, both digests, complete root, S/projection and publisher authorization; the independently authenticated publisher ID and retention policy must equal the committed readiness fields.

`RecordSnapshotArtifactReady` internally signs only the unchanged readiness record/domain with the separately configured publisher credential and submits it inside `ArtifactDispositionRecordReadyV1` for its constructor-bound `candidate_seq`. It verifies the returned committed association and exposes no arbitrary-byte signer; a lost response is reconciled through the original disposition operation selector. The old direct readiness mutation may remain only when the disposition capability is off; enabled apply refuses or internally routes only an already exact candidate-bound trusted call. The new disposition service is control-plane protocol, while no separate schema-signing or generic issuer RPC is introduced.

The exact constructor is `NewSnapshotArtifacts(backend ArtifactBackend, exporter PartExporter, published PublishedSnapshotSource, publication ArtifactPublicationControl, journalDir string, owners ArtifactOwnerRegistry, verifyTerminal func(context.Context, string, replay.SnapshotPin, []byte) error) (SnapshotArtifacts, error)`. `ArtifactOwnerRegistry` is an AC-local protected registry dependency, immediately before the terminal callback, and is required in both modes. Its responsibilities are immutable authenticated registration, atomic serial/retry allocation, tracked use leases, monotone close/release authorization, exact origin/pin binding, protected class capabilities, durable transfer edges and conservative recovery; its concrete Go helper types, methods and internal record encoding belong to B1 and are not public wire/canonical contracts. No generic arbitrary-owner registration, caller-provided closed boolean, context-selected trust, unbound permissive registry or compatibility callback is allowed.

The verifier arguments remain `(ctx, expectedReferenceID, expectedPin, terminalProof)`: B1 reads the immutable expected reference/pin binding from its durable journal and passes both values explicitly. C1/C3 own the trusted proof verifier and authenticated reference-owner/terminal authorization, using the same fixed registry view; a B1-local parser, nonempty opaque proof, caller string or mutable context/closure cannot establish ownership. Every callback consumer uses both expected values, and unrecognized, unbound or unsupported reference classes refuse release. Verification runs outside long-held journal/GC locks and follows the artifact-lifecycle addendum's strict proof parsing plus mandatory fresh authenticated `GetArtifactDisposition` read. Only after successful exact terminal evaluation may the registry durably commit release authorization; B1 then immediately rechecks the same immutable live binding before the spent mutation. Verifier failure, mismatch, offline authority or a crash before successful evaluation leaves no durable release authorization and keeps the ownership and objects intact. Verifier success authorizes only that ownership release, not object GC. Public `Release`, the callback shape, five-class spelling and every old hash remain unchanged; the new proof/service/domain surface is the explicit reviewed disposition amendment.

Both modes require backend, published source, journal directory, a bound owner registry and terminal-proof verification. `publication == nil` deliberately selects read-only mode; exporter may also be nil, and Publish returns `ErrArtifactPublicationUnavailable` before exporter, owner registry, journal or readiness work. A failed control call never falls back to this mode. Publishing mode requires both nonnil publication control and exporter. The concrete control constructor, not AC reflection or a generic `Validate` method, verifies complete authority, publisher credential/authorization, fixed-scope, reader and submitter wiring. Publication registration derives its candidate/publisher identity from the trusted constructor-bound issuer lifecycle; it does not add caller identity or move `AuthenticatePublisher` earlier than the existing validation order.

### Local reference ownership

The five local journal classes use one exact grammar:

```text
hgsqref/1/<class>/<N>/<K>/<J>/<A>/<class-tail>
```

`class` is exactly `publication`, `reservation`, `accepted`, `replay` or `challenge`. For every capital string token below, `H(s)` is the lowercase hexadecimal encoding of the exact nonempty UTF-8 bytes, with no normalization, trimming, percent escape or alternate spelling. `N=H(network_id)`. `K` is canonical unsigned base-10 `keeper_shard_id`, with no sign or leading zero except `0`. `J=H(registry namespace ID)` comes only from trusted constructor configuration and is durably bound to one local principal/journal; the fixed map also binds its N/K scope, protected directory and allowed class capabilities. A new empty registry obtains a new namespace, restoration keeps its namespace/high-water mark/tombstones, and a namespace is never reused after data loss. `A` is a positive monotonically allocated `uint64` acquisition serial in J. Allocation and registration are one fsynced transaction under a cross-process lock; callers cannot choose A, exhaustion refuses and an interrupted allocation is never reused. Parsers require exact segment count/class, canonical hex/decimal, valid UTF-8 round trip and byte-identical re-encoding, with no trailing slash or ignored suffix. IDs are never filesystem paths, and J resolves only through the constructor-fixed registry map.

`SI` is snapshot ID, `MR` manifest root, `PUB` authenticated publisher principal, `CA` client account, `ST` statement ID, `REQ` request ID, `RID` reservation ID, `SR` the existing `SnapshotQueryStatementRoot`, `POL` the existing retention-policy ID and `WORKER` the verified/configured runtime principal; all are H-encoded. `G`, `B` and `SS` are positive canonical decimal grant generation, block sequence and statement sequence. `ROLE` is literally `source` or `verifier`; `ORIGIN` is literally `publication` or `query`. These local IDs add no root or public record.

| Class | Exact class-tail | Authenticated owner and required immutable binding | Admission | Release disposition |
|---|---|---|---|---|
| `publication` | `<SI>/<MR>/<PUB>` | D3's sealed-candidate/O journal under the actual publication/schema fence; bind full candidate pin/manifest, complete part-export view, configured publisher/retention policy, issuer/capture authority and candidate lifecycle, then append O/digests/readiness association set-once. | Register after the complete candidate is sealed and before any upload object is written/exposed; retry of that live candidate recovers the same A. Work before a complete pin is issuer debt, not a fabricated reference. | Exact safe publication/readiness plus durable transfer to continuing publication/challenge retention, or authenticated fenced cancellation that excludes late readiness/publication commits, settles unknown proposals and preserves cleanup/challenge debt. Ready-but-unpublished, disconnect or fresh absence alone retains. |
| `reservation` | `<CA>/<ST>/<REQ>/<RID>/<G>` | C2 authenticated request-history key and original complete grant; bind exact predecessor pin, activation, executor/query profiles, RID and original G. | AR registers/retains before returning the grant; response-loss retry recovers the same A. Draining/absent requests have no granted artifact reference. | Exact committed unconsumed `released` tombstone for the original request/grant with `block_seq=0`, or its consumed assignment followed by C3's exact applied/aborted chain, settled/fenced writes and surviving cleanup/challenge ownership. Consumption, idle or another request/generation never suffices. |
| `accepted` | `<CA>/<ST>/<B>/<SS>/<SR>` | C3 committed acceptance/assignment plus durable original signed L3 statement and full reservation; bind input/read roots, exact original compact-JWS hash, grant/fence, source assignment, full predecessor pin, profile/activation history and any existing claim identity. | Register/retain during accepted-work recovery before exposing work to execution; reservation ownership covers a commit-before-local-persist window until accepted ownership is established. | Exact matching applied/aborted terminal child for the predecessor/block, with publication/output or abort invariants, old-generation fencing, settled/quarantined writes and durable replay/cleanup/challenge successors; the local accepted operation must also be closed. |
| `replay` | `<CA>/<ST>/<B>/<SS>/<SR>/<ROLE>/<WORKER>` | C1/C3 authenticated committed job/assignment/history and original L3 statement plus this J/A; bind accepted identity, required claim/root, full pin/O association, historical activation/pair and configured principal/role. | The trusted source/verifier wrapper registers and retains before any read/execution. Same live operation retry recovers A; each newly authorized historical replay gets a new A while old spent IDs stay spent. | This exact acquisition is closed/quiescent and its job/retention disposition is authenticated with surviving challenge/cleanup protection. A terminal job fact may support multiple separately closed acquisitions but cannot close an active one or borrow another A's close evidence. |
| `challenge` | `<ORIGIN>/<SI>/<MR>/<POL>` | C1 committed readiness/publication and policy; for query origin also C3 consumed job/terminal/abort/cleanup authority. Bind the exact immutable origin record, full pin/O association and every outstanding challenge/cleanup obligation. | Register/retain the successor before spending the last scheduling/publication/replay protection; persist a transfer edge to the exact live successor, never only a count or pin label. | Authenticated current policy/horizon authority says every challenge, cleanup debt, unknown write and transfer dependency for this origin is resolved, and the local adapter is closed. The disposition must be monotone or protected by an enforced admission fence that requires replacement ownership; scheduler terminal, finality or local time alone never suffices. |

The immutable registry core contains the exact reference bytes/class, full `SnapshotPin`, natural owner key, configured principal/role, authenticated record identities/evidence, J/A, durable operation retry key and lifecycle adapter. Store the original evidence, including original JWS bytes, and append set-once phase facts; conflicts refuse and absent facts remain pending. For public-safe classes, registration and `Retain` both require authenticated exact published S plus original readiness/O association and owner-specific admission. Only constructor-selected lifecycle adapters register or close ownership. Public `Retain` attaches an existing live exact-matching registration; it cannot manufacture one from a string.

The local lifecycle is `registered -> retained/live -> closing -> closed -> release-authorized -> spent`. `closing` forbids new I/O. Managed read, scratch, execution and submission handles take tracked leases and must join or be authoritatively reconciled before `closed`; `ReadSnapshot.Close` closes one handle, not the acquisition. The fixed-trust evaluator requires both the exact class's control-plane disposition and this acquisition's actual closed state, then durably commits release authorization. B1 rechecks the same immutable live binding and commits spent. A crash between those commits retains extra data and retries safely. Natural-owner retry recovers the same live A; identical Retain is idempotent only for that live core. Unknown IDs error. Repeated Release of an already spent binding may return its stored result without reauthorization, but spent IDs, namespaces and serials never reopen, rebind or affect another acquisition. The registry retains namespace high-water state and exact spent bindings/receipts indefinitely under the existing no-new-duration rule; there is no automatic tombstone reuse or reclamation. A rolled-back journal snapshot that lost later allocated/spent IDs cannot mutate that namespace until authoritative complete state is reconciled, otherwise it refuses. Existing experimental free-form IDs such as `reservation/r1/7` are not guessed into this grammar; they require authenticated explicit local migration or remain quarantined.

Challenge/debt transfer is durable before the last prior protection becomes spent, and a successor cannot disappear concurrently with handoff. Recovery preserves live/closing/uncertain ownership. Orphan registration protects or quarantines its known objects; a B1 live reference without authenticated registry state remains protected and cannot release; corruption or missing registry state never means absence. It may resume the same A only when the managing host/container authority proves the exact old process incarnation and all children stopped, or an enforced fence excludes every old operation. PID spelling, elapsed time, startup or a local counter is insufficient; absent proof retains the acquisition and refuses reclaim. GC independently requires no live/provisional/uncertain reference, registration/handoff, active lease or release-authorized-not-spent entry, plus the exact cancelled-candidate or published-RETIRED disposition and settled obligations/debts, followed by a common-lock recheck.

The first filesystem profile uses protected per-principal lifecycle partitions configured separately for AR, each source and each verifier. Only the trusted publisher/GC authority writes shared immutable objects; each lifecycle adapter writes only its own partition, while the verifier/GC authority has configured read views. Shared lock files do not grant directory replacement or cross-owner journal writes. Enablement proves actual mounts, UID/ACL, no-follow/directory ownership checks, durable rename/fsync and cross-process locking. Shared unrestricted writable storage, unreliable locking, missing principal identity or missing old-process fencing refuses enablement; this adds no general supervisor.

The artifact-lifecycle addendum supplies the required protected sealed-candidate lifecycle, late-future fence, committed use/obligation/debt authority and explicit administrative historical-window closure. C1/C2/C3/B5/C5/D3/D4 must implement and test that exact default-disabled protocol; a local cancelled label, request-context cancellation, Finality, LastMergeable, cleanup absence, `AckCleanup` or a claimed closed boolean remains insufficient. Successful cancelled-unpublished reclamation and successful published-noncurrent retirement are mandatory finite paths, while every unknown outcome retains/resumes/quarantines until the exact terminal predicate is freshly proven.

Only the trusted AR publication worker receives publishing mode and the schema/publisher secrets. Source and verifier roles receive read-only mode and retain published-safe read/retention dependencies without publication secrets. B1 may lookup immutable committed state before heavy export, but it cannot expose readiness until full bytes/root checks and provisional fsync complete; first and identical-committed branches both authenticate the publisher.

## 5. Automatic trusted issuance

D3 configures a dedicated schema issuer library inside the trusted AR leader/publication runtime, with separate schema-authority key material, trusted authenticated read-only full-semantic capture access and a durable candidate journal. The concrete boundary is `orchestrator/promotion.go`'s `publishManifest` after the complete candidate is sealed and before `PublishSafeSnapshot`; genesis/bootstrap, executor-profile transition, empty/zero-output, every v2 successor and every query successor use the same readiness-aware path. The source never receives the authority key or chooses the capture endpoint.

The restartable worker follows this order:

```text
derive and seal candidate M -> verify leader and publication/schema fence
pin immutable candidate manifest, schema capture and part-export view
create protected publication reference and persist exact disposition operation key/root
bind and fsync the exact prepaid principal/namespace/ordinal slot before local registration
RegisterCandidate -> authenticate returned C -> append set-once C phase fact and candidate obligation
trusted issuer independently captures complete name/type/position/generation/expression metadata
build A -> sign certificate -> atomically fsync exact O in the candidate journal
B1 validates O, exports/verifies/fsyncs complete HGPART objects under C's provisional ownership
B1 publication control authenticates publisher and submits candidate-bound RecordReady
C1 independently verifies O and commits the unique readiness association for M and C
publication worker submits candidate-bound PublishCandidate(M); FSM requires matching readiness
```

The issuer independently verifies metadata against trusted configured access under the actual candidate/fence; unsigned source JSON and arbitrary endpoints are not certifiable evidence. Work uses bounded attempts, persists progress by candidate identity and holds no Go mutex or Raft apply across capture, signing, artifact I/O or readiness transport. Recheck fence ownership before readiness and publication. Retry, restart, leader rescan and the pre-removal rotation drain reuse an existing candidate's exact fsynced O rather than re-signing a conflicting outer object; an authenticated identical committed-readiness result resumes publication through the committed-root branch above.

The protected slot binding is the artifact-lifecycle capacity profile's private allowance, not a schema certificate, public command field or signing claim. The trusted allocator binds immutable owner/core/reference/A before registration; the first private validation carries positive ordinal field 8 until replicated installation, then later actions derive the installed binding with zero. A recovery coordinator is a caller, not the protected owner. Invalid requests consume only the bounded rejection pool and cannot spend the candidate's ready/cancel/publish/cleanup or eventual retirement completion routes.

A query successor uses the consumed/resolving operation's publication authority and must not wait for its own barrier to become idle or acquire a new query reservation. A v2 successor uses its publication/schema exclusion rather than a synthetic query. Failures keep recoverable work and provisional artifacts; restoring healthy dependencies resumes automatically. Manual/CLI certificate creation may aid diagnostics but cannot satisfy D3 or D4.

No separate issuer/signing RPC is required for the initial topology. AR composes the injected issuer and publishing-mode B1 in process through the shared artifact namespace and C1's candidate-bound local publication-control adapter; the adapter uses `ArtifactDispositionControl` for candidate/readiness/publication state. `GetPublishedSnapshot` is published-only and cannot feed pre-publication issuance, partition ACK is not a certificate, and an old unscoped readiness mutation is neither a sign request nor sufficient as B1's control dependency. A remote issuer, general schema registry, supervisor and production activation remain outside this correction.

## 6. Required evidence and compatibility

Freeze public vectors for A bytes/digest, certificate payload/header/signing input/token/recovered address, O bytes/digest, artifact-set root and ready association. Preserve every legacy/A1 vector. Cover equal legacy projection with different authenticated O and immutable restore carry-through, equal legacy hash with different generation, old/missing/unknown/contradictory metadata, constant DEFAULT, explicit full target lists, ineligible referenced reads versus untouched U, wrong purpose/key/role/network/shard/S, inner/outer digest confusion, high-S/bad-V, byte substitution between B1 and C1, two roots for one S, semantic-only drift, stale/latest/missing capture and fence, and old adapters/engines that drop fields.

Test K1 first commitment, the full-stage drained K1-to-K2 rotation, K1 historical replay after restart and refusal of a new uncommitted K1 object. Combine crash after K1 O fsync with rotation, and K1 readiness commit plus lost response with rotation: both retain exact O, finish under the still-fixed K1 map or the identical committed-root recovery branch, and only then permit key removal. D4 uses distinct disposable authority, publisher and source keys and runs automatic v2 S1, query S2 and v2 S3 publication plus genesis, transition and zero-output successors without manual certificate injection. Crash after signing, O fsync, readiness and before publication must retain/reuse exact O and finish after restart. Missing/fake capture, wrong leader/fence, missing store and unavailable authority capture refuse progress while unhealthy and resume automatically after restoration.

Also test a late old candidate future after cancellation, same-`S` replacement candidate separation, absent-use close racing admission, original AdmitUse result with current closed state, pre-cut work and replay-to-challenge continuation after a cut, current-tip retirement refusal, cancelled-unpublished cleanup, published-retired deletion and shared-object protection. These lifecycle tests use actual authenticated registry/process evidence and never promote absence, `AckCleanup` or a boolean to proof.

Also test allowance binding before the first candidate/use registration, exact retry across lost response/restart, ordinal owner versus recovery caller, one-over bounded attempts and permanent spent slots. Run the complete candidate/manifest at the disposition command/request/Raft limits and prove exact raw-manifest interning reconstructs the original canonical root without changing A, O, manifest, readiness or certificate bytes. Capacity success supplies no missing schema, publication or physical authority.

Old name/type-only schemas continue to support v2 unchanged but never opt into snapshot queries. Do not modify legacy hashes/manifests, infer metadata, certify an unprovable retained snapshot, or claim current code supplies these guarantees. All implementation remains future work behind the existing default-off gate.
