# Snapshot-query authenticated schema semantics

**Status:** Normative bounded correction for the future signed `INSERT ... SELECT` capability. It changes no implemented runtime behavior and qualifies no existing schema, snapshot or deployment.

This addendum defines the column-eligibility metadata, authenticated exact-snapshot schema object, committed publication association and automatic trusted issuance required by the [signed `INSERT ... SELECT` design](2026-09-16-signed-insert-select-design.md). It preserves every A1/AP/v2 field and hash, the existing `payloadexec.TableSchema`, all legacy table/schema/state formulas, the reviewed [analyzer build identity](2026-09-16-signed-insert-select-build-identity-design.md), all A1–A14 gates and default-disabled behavior.

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

## 3. Independent verification and exact-S commitment

B1 and C1 independently fetch bounded exact O bytes from the artifact store, strictly decode and byte-for-byte re-encode them, verify both digest layers and the complete artifact-set root, verify the exact purpose/version/signature and recovered role, match network/shard/snapshot/manifest/schema identities, and recompute the complete legacy projection. No publisher availability signature, current live metadata, old `SchemaJson` hash, boolean, self-consistent manifest or uncommitted certificate supplies semantic or publication proof.

Before readiness, B1 requires the recovered address to be authorized for the dedicated current schema-authority role and scope, verifies the frozen capture provenance, persists the exact O and all parts, retains provisional ownership and then uses the separately authenticated publisher credential to submit readiness. C1 fetches O independently, repeats byte/digest/signature/S/projection checks, authorizes the current schema-authority role for first commitment, separately authenticates the publisher, and only then proposes readiness. Fetch, crypto, capture and role work remain outside Raft apply; deterministic identity/conflict checks occur in apply.

C1 commits at most one immutable `ArtifactSetRoot` for `(network_id, keeper_shard_id, snapshot_id, manifest_root)`. An identical retry succeeds; a conflicting root refuses before or after publication. Signing the same A again into different JWS/O bytes cannot replace the committed root. A location change may serve only the identical O. This unique authenticated committed readiness/publication association lets A1's signed exact S bind the semantic artifact without adding an A1 field.

Historical consumption verifies the retained exact O/certificate, S binding and authenticated committed original readiness/publication association. It does not require the old address in today's allowlist and does not apply current token age. A newly presented, previously uncommitted certificate from a removed key refuses. Trust-map rotation pauses issuance/readiness, drains prevalidated work, updates the participants and resumes; no prevalidated request crosses the change. Preserve original bytes and committed proofs through restart, migration, follower catch-up, rotation, replay and challenge; never re-sign or replace historical O in place.

## 4. New-lane ports and consumers

`SnapshotArtifacts.Publish` receives `replay.AuthenticatedSnapshotQuerySchemaV1` with the candidate manifest instead of treating `[]payloadexec.TableSchema` as semantic authentication. `PublishedSnapshotSource.GetPublishedSnapshot` returns the manifest and verified typed object. `ReadSnapshot.SchemaArtifact()` returns that typed object, while `Schemas()` remains its validated legacy projection for B2/B4 row and state assembly. D1's `SnapshotCatalogSource.Load(pin)` returns the manifest and verified object after checking the exact selected-S publication proof. These changes apply only to new-lane ports and create no HG-to-AC dependency.

A5, D1, D2, B4 and B5 build RP catalogs only from the authenticated object's complete columns, revalidate exact S/projection/publication policy before final signing, host intake, source execution and verifier execution, and transport `generation` plus `default_expression` unchanged. Engine `SUCCESS` validates supplied metadata/profile behavior but never proves publication. B4 still supplies only the validated legacy projection to shared row/state assembly and preserves U in the complete ledger.

## 5. Automatic trusted issuance

D3 configures a dedicated schema issuer library inside the trusted AR leader/publication runtime, with separate schema-authority key material, trusted authenticated read-only full-semantic capture access and a durable candidate journal. The concrete boundary is `orchestrator/promotion.go`'s `publishManifest` after the complete candidate is sealed and before `PublishSafeSnapshot`; genesis/bootstrap, executor-profile transition, empty/zero-output, every v2 successor and every query successor use the same readiness-aware path. The source never receives the authority key or chooses the capture endpoint.

The restartable worker follows this order:

```text
derive and seal candidate M -> verify leader and publication/schema fence
pin immutable candidate manifest, schema capture and part-export view
trusted issuer independently captures complete name/type/position/generation/expression metadata
build A -> sign certificate -> atomically fsync exact O in the candidate journal
B1 validates O, exports/verifies/fsyncs parts and retains provisional ownership
publisher submits existing RecordSnapshotArtifactReady for O's committed outer digest
C1 independently verifies O and commits the unique readiness association for M
publication worker proposes PublishSafeSnapshot(M); FSM requires matching readiness
```

The issuer independently verifies metadata against trusted configured access under the actual candidate/fence; unsigned source JSON and arbitrary endpoints are not certifiable evidence. Work uses bounded attempts, persists progress by candidate identity and holds no Go mutex or Raft apply across capture, signing, artifact I/O or readiness transport. Recheck fence ownership before readiness and publication. Retry, restart, leader rescan and rotation reuse an existing candidate's exact fsynced O rather than re-signing a conflicting outer object.

A query successor uses the consumed/resolving operation's publication authority and must not wait for its own barrier to become idle or acquire a new query reservation. A v2 successor uses its publication/schema exclusion rather than a synthetic query. Failures keep recoverable work and provisional artifacts; restoring healthy dependencies resumes automatically. Manual/CLI certificate creation may aid diagnostics but cannot satisfy D3 or D4.

No new issuer RPC is required for the initial topology. AR composes the injected issuer and B1 publisher in process through the shared artifact namespace; C1 consumes the existing readiness transport. `GetPublishedSnapshot` is published-only and cannot feed pre-publication issuance, partition ACK is not a certificate, and `RecordSnapshotArtifactReady` is not a sign request. A remote issuer, general schema registry, supervisor and production activation remain outside this correction.

## 6. Required evidence and compatibility

Freeze public vectors for A bytes/digest, certificate payload/header/signing input/token/recovered address, O bytes/digest, artifact-set root and ready association. Preserve every legacy/A1 vector. Cover equal legacy hash with different generation, old/missing/unknown/contradictory metadata, constant DEFAULT, explicit full target lists, ineligible referenced reads versus untouched U, wrong purpose/key/role/network/shard/S, inner/outer digest confusion, high-S/bad-V, byte substitution between B1 and C1, two roots for one S, semantic-only drift, stale/latest/missing capture and fence, and old adapters/engines that drop fields.

Test K1 first commitment, drained K1-to-K2 rotation, K1 historical replay after restart and refusal of a new uncommitted K1 object. D4 uses distinct disposable authority, publisher and source keys and runs automatic v2 S1, query S2 and v2 S3 publication plus genesis, transition and zero-output successors without manual certificate injection. Crash after signing, O fsync, readiness and before publication must retain/reuse exact O and finish after restart. Missing/fake capture, wrong leader/fence, missing store and unavailable authority capture refuse progress while unhealthy and resume automatically after restoration.

Old name/type-only schemas continue to support v2 unchanged but never opt into snapshot queries. Do not modify legacy hashes/manifests, infer metadata, certify an unprovable retained snapshot, or claim current code supplies these guarantees. All implementation remains future work behind the existing default-off gate.
