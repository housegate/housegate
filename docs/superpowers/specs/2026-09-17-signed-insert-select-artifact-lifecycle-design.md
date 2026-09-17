# Signed INSERT ... SELECT artifact lifecycle

**Status:** Normative design addendum, 2026-09-17. This contract is implementation work only and remains disabled by default until its named qualification and acceptance gates pass; it authorizes no deployment, production retirement, merge, or deletion.

**Related authority:** The [main design](2026-09-16-signed-insert-select-design.md) defines query semantics, while the schema-semantics addendum remains authoritative for the [artifact-set commitment](2026-09-16-signed-insert-select-schema-semantics-design.md#artifact-set-commitment), [publication-control injection](2026-09-16-signed-insert-select-schema-semantics-design.md#publication-control-injection) and [five-class local reference ownership](2026-09-16-signed-insert-select-schema-semantics-design.md#local-reference-ownership). This addendum governs complete artifact capacity, candidate fencing, historical-use admission and finite cancellation/retirement. Where touched lifecycle prose in the older documents conflicts, this reviewed bounded amendment controls; every old canonical record, token, digest, wire field and reference spelling remains byte-for-byte unchanged.

## 1. Scope and compatibility boundary

The archive profile preserves complete selected snapshot `S`, including untouched `U`, auxiliary files, projection trees and empty directories. It does not apply the selected-read relation `R` column/type eligibility rules to `U`, and it does not apply `Q.max_restore_bytes=1073741824` to whole `S`; that 1 GiB bound remains a B2 selected-`R` restore bound.

The archive format adds no field to `SnapshotPin`, `SafeSnapshotManifest`, `AuthenticatedSnapshotQuerySchemaV1`, `SnapshotArtifactEntry`, `SnapshotArtifactReady`, `ArtifactSetRoot`, the five-class reference grammar, or any existing signature. `SnapshotArtifactEntry.bytes` remains the transport-object length and `ObjectDigest` remains `replay.DigestBytes` of the exact object bytes. A wrong HGPART v1 interpretation therefore requires a new readable archive version and retention of old readers; implementations never rewrite a committed object in place.

The disposition protocol is a reviewed bounded amendment to the prior no-new-RPC/no-new-domain assumption. It adds two RPCs, one Raft oneof variant at tag 28, eleven typed actions, two canonical digest domains and one administrator JWS purpose while preserving all existing A1/v2/user/schema/manifest/readiness/receipt fields, tags, hashes, signatures and canonical bytes.

The first profile uses the existing Arbiter Raft group as replicated candidate/disposition authority and the protected AC-local registry as exact acquisition, handle, child, quiescence, transfer and spent-identity authority. It does not turn the local registry into a distributed leader, add a supervisor, invent a retention duration, or authorize rehydration of a retired identity.

## 2. HGPART v1 complete-tree object

The exact object grammar is:

```text
offset 0:  48 47 50 41 52 54 00 01   # HGPART, NUL, version 1
offset 8:  entry_count:u64le
repeat entry_count times:
    kind:u8                          # 00 = directory, 01 = regular file
    path_length:u32le
    path:path_length raw bytes
    if kind == 01:
        length:u64le
        body:length original bytes
immediate EOF
```

All integers are fixed-width unsigned little-endian. Directories have no length or body; zero-length files have explicit file records. There is no root record, padding, checksum trailer, outer compression, timestamp, owner, permission, link, extended attribute, sparse map or executable metadata. Root is implicit. Empty-tree syntax is the 16-byte magic/count-zero header, although a valid ClickHouse part still must contain its required metadata.

Entries are strictly increasing by unsigned raw UTF-8 path bytes. Every non-root parent directory appears exactly once before its descendants, including empty directories; a path has one kind and one record. A syntax-only three-entry object containing directory `d`, one-byte file `d/x` with body `7a`, and empty file `e` is exactly 53 bytes: `16 + 6 + 17 + 14`.

Paths are unchanged valid UTF-8. No normalization, case folding, percent decoding, trimming or translation is allowed. `%`, spaces, quotes, non-ASCII bytes and backslash are literal accepted filename bytes on the qualified Linux profile; backslash is never a separator. `/` is the only separator. Leading or trailing `/`, empty components, `.` or `..` components and NUL refuse. Invalid UTF-8 refuses rather than replacement-decoding.

Source symlinks, sockets, devices and FIFOs refuse. Hardlinked regular source files may be copied as independent bodies only when the D3 immutable view proves stable bytes. Destination files are fresh regular inodes; no source hardlink is reconstructed or followed. A source inventory or kind change during capture fails the candidate.

The qualified filesystem must be case-sensitive, byte-preserving and distinguish every accepted name. Component length is at most 255 UTF-8 bytes, relative path length at most 4095 UTF-8 bytes and path/projection depth at most 64 components. A platform that aliases accepted names refuses instead of renaming them.

## 3. Complete preservation and proof roles

Every selected top-level part in complete `S`, including untouched `U`, preserves every captured regular file and directory. The archive layer does not call `ResolveColumnProfile`, require `_hg_row_id` in projection children, reject DEFAULT/generated user columns or drop a part/member to fit a narrower parser. Selected-`R` interpretation remains a separate B2 gate under the schema-semantics addendum.

At every checksum-bearing part or projection directory, require the original complete `checksums.txt`, root `columns.txt` and corresponding original physical metadata. Parse the complete bounded checksum map. For every ordinary entry, resolve one exact regular member and independently compare actual body length and physical hash. For a real projection child directory, recursively verify the child, compare its checksum-covered sum and non-reference outer SipHash aggregate, and include that directory edge exactly once in its parent sum and aggregate.

The verifier retains every other captured auxiliary file and directory and compares the entire tree against D3's sealed inventory. An auxiliary absent from the checksum map is not automatically extra and receives no fabricated physical-checksum claim. Unsupported sentinel or checksum proof refuses by name rather than silently skipping it. Known auxiliaries include `checksums.txt`, `columns.txt`, and, when present, `default_compression_codec.txt`, `txn_version.txt`, `metadata_version.txt` and `columns_substreams.txt`; this is not a universal allowlist and covered metadata such as `serialization.json`, counts, partition/minmax/index/mark/TTL/UUID files remains preserved.

The trusted D3 capture binds exact part identity, every relative name/kind/presence and every byte under the real publication/schema fence. It is a protected local journal fact, not a new public proof field. Physical hashes prove covered files/projection structure but do not authenticate unchecked metadata. Object SHA-256 proves exact committed archive bytes but not their origin. The complete tuple-sorted ArtifactSetRoot proves the selected object set. `O` proves complete table semantics but not every physical auxiliary. These proofs are complementary and none substitutes for the others.

The pinned ClickHouse source authority for full checksums and part metadata is commit `46297556f3ac83e1a3ee4a05cc31f4287f5e17ac`. The qualified physical hash implementation uses `github.com/go-faster/city` v1.0.1, successive 2048-byte logical pieces with the previous `CH128Seed` result, and the ClickHouse non-reference zero-key SipHash128 outer aggregation over raw-name-sorted entries. The system.parts spelling is high64 then low64, each 16 lowercase hex digits without `0x`.

For one part tree, `P` is the sum of root checksum-entry physical sizes, counting each projection-child covered sum exactly once; `F` is the sum of all archived regular-file logical lengths recursively; and `T = 16 + sum_dirs(5 + path_bytes) + sum_files(13 + path_bytes + file_bytes)`. `PartManifestEntry.Bytes=P`, `SnapshotArtifactEntry.bytes=T`, and `F` remains local accounting rather than a wire field.

## 4. Checksum compatibility and qualification

The target support set is full checksum versions 2, 3 and 4. Version 4 accepts wrapper methods `NONE 0x02`, `LZ4/LZ4HC 0x82` and `ZSTD 0x90`. Full v1, minimalistic v5 and unknown versions or methods refuse. Minimalistic v5 cannot prove a complete file map.

Version 2 implements this profile's canonical strict subset of the pinned ClickHouse line grammar: names are terminated by LF or TAB and therefore cannot carry LF or TAB verbatim, numbers are unsigned decimal UInt64 with no unmodelled whitespace or escape guessing, and booleans accept only literal `0` or `1` even though the pinned helper accepts additional text/nonzero spellings. Version 3 uses a UInt64 varint count, length-delimited names, physical-size varints, little-endian low/high hash halves, a one-byte 0/1 compressed flag and optional uncompressed size/hash; varints are minimal, at most ten bytes and reject overflow or an invalid tenth byte. Version 3 and its version-4 compressed representation may carry otherwise valid UTF-8 control bytes in length-delimited names. This checksum-encoding restriction does not narrow HGPART's general raw-UTF-8 path policy. All versions require exact decoded EOF.

Version 4 consists of one or more ClickHouse compressed frames whose decoded bytes concatenate into the version-3 payload. Each frame has a separate 16-byte stored checksum, then method byte, compressed-size `u32le` including the nine-byte method/size header, decoded-size `u32le`, and exactly `compressed_size-9` payload bytes. Verify unseeded `city.CH128(method || size fields || payload)` before decoding. `NONE` requires payload length equal to decoded size; LZ4 and ZSTD must consume their exact blocks and produce the declared decoded size. Mixed supported frames are allowed within all cumulative limits. Raw and decoded streams require exact EOF and a zero-output frame loop refuses.

Existing independent evidence qualifies v4/LZ4 Wide, Compact, actual Sparse, projection and UTF-8-name physical behavior, including 126 leaves across 12 read chunkings and independent outer hashes. A preparatory checksum corpus freezes 96 literal inputs with expected results: 95 source-derived cases plus one actual-capture case, 24 positive and 72 negative cases, 1,730 prefix-rejection checks, and six original captured ClickHouse frames with independently checked SHA-256, offsets and stored hash bytes. This freezes parser expectations and actual frame identity but does not complete the B1 archive/restore/runtime gate. In particular, the complete ZSTD 32 MiB workspace bound remains unqualified. Source-derived cases, including the canonical v2 delimiter/boolean subset above, remain labelled source-derived rather than historical captures. Full v2/v3 implementation, v4 NONE/ZSTD/multiframe runtime boundaries, arbitrary metadata semantics and restore still require the named independent gates before support is claimed.

## 5. Finite local and whole-S resource profile

All KiB/MiB/GiB values are powers of 1024. These bounds are frozen trusted local admission policy, not new query/profile/wire fields. Every counter uses checked UInt64 arithmetic before conversion or allocation; every fetch uses limit-plus-one where applicable.

| Unit | Ceiling |
|---|---:|
| Filename component / relative path / depth | 255 bytes / 4095 bytes / 64 components |
| Entries / path bytes in one object | 65,536 / 8 MiB |
| One regular member / raw bodies in one part tree / transport object | 16 GiB / 16 GiB / 16 GiB |
| One full-checksum raw file / decoded payload | 16 MiB / 16 MiB |
| One v4 encoded frame / decoded frame | 2 MiB including its 9-byte compression header / 1 MiB |
| Stored checksum prefix for one v4 frame | separate 16 bytes; maximum total frame is `2 MiB + 16` |
| Frames per checksum file | 1,024 |
| Checksum entries per directory / per object | 65,536 / 65,536 |
| Decoded checksum-name bytes per object | 8 MiB |
| All checksum raw / decoded bytes per object | 32 MiB / 32 MiB |
| Exact canonical `O` / certificate artifact `A` buffers | 8 MiB each |
| Manifest encoding / artifact-set encoding | 32 MiB each |
| Schema tables / columns per table / columns total | 4,096 / 4,096 / 65,536 |
| `S` part records / partition records | 65,536 / 65,536 |
| All regular and directory entries across `S` | 1,048,576 |
| All archive path / decoded checksum-name bytes across `S` | 64 MiB / 64 MiB |
| All raw / decoded checksum bytes across `S` | 256 MiB / 256 MiB |
| `S` raw bodies / sum of transport-object bytes | 64 GiB / 64 GiB, separate checked counters |
| `S` total logical `O` + manifest + artifact set + part objects | 64 GiB + 128 MiB |
| Active B1 export/verification jobs per instance | 1, including read-only instances |
| B1-owned resident allocations | 256 MiB |
| ZSTD window / decoder workspace | 8 MiB / 32 MiB |
| I/O buffer / sort window / live descriptors | 1 MiB / 16 MiB / 128 |
| Metadata/index temporary disk per publication | 1 GiB |
| Per-attempt body bytes read/written/hashed / wall time | 512 GiB / 2 hours |
| Cancellation observation / automatic retries in one B1 call | between at most 1 MiB chunks or one bounded frame / 0 |

The 256 MiB figure is charged B1-owned allocation, not total process RSS. D3/D4 still require measured deployment headroom, CPU/memory isolation, cancellation responsiveness, filesystem capacity and inode evidence. ENOSPC, EDQUOT, inode exhaustion, timeout or lost cancellation responsiveness is a failed recoverable attempt and never readiness, cancellation or release authority.

Disk reservation uses the sealed actual inventory. Let `B` be the qualified filesystem allocation unit in `1..1,048,576`, `roundB(x)=ceil(x/B)*B`, and `mu=16*B`. For part `i`, let `F_i` be its file sizes, `E_i` its file-plus-directory count, `L_i` its path bytes and `T_i` its exact HGPART length. Let `E=sum E_i`, `L=sum L_i`, `P=part count`, and let `M`, `O`, `Aset` be actual canonical manifest, outer-schema and artifact-set byte lengths.

```text
tree(i) = sum_over_files(roundB(file_length))
          + mu * (E_i + 1)
          + roundB(2 * L_i)

C = sum_i tree(i)
Objs = sum_i (roundB(T_i) + mu)
       + roundB(O) + mu
Temp = max_i(roundB(T_i) + mu)
Verify = max_i tree(i)

Iraw = 8 MiB + 512*E + 4*L + 512*P + 2*M + 2*O + 2*Aset
I = roundB(Iraw) + 64*mu
Safety = 64 MiB

D_payload = C + Objs + Temp + Verify + Safety
D_new = D_payload + I
require D_payload <= 192 GiB and I <= 1 GiB
reserve D_new, never the two ceilings unconditionally
```

For `P=0`, both maxima are zero and `I`/Safety charge staging. Reserve at least `sum_i(E_i+1) + (P+1) + max_i(E_i+1) + 65` inodes. Attribute every term to the volume that stores it; never sum free space across volumes. Apply logical whole-`S` bounds before physical reservation and take no dedup, hardlink, compression or sparse-allocation saving. A tiny candidate reserves its conservative actual footprint; 193 GiB is the maximum allowed reservation, not a mandatory free-space minimum.

The common quota/reservation lock checks free blocks, configured quota, inodes and active reservations and durably binds the reservation to the acquisition. If actual allocation would exceed it, extend only under the same checks and ceilings before writing. Exhausted retained storage refuses new admission and never evicts protected history.

## 6. Safe acquisition, extraction and immutable handoff

Phase 1 streams the complete object into a new exclusive private inode under the protected root, charges all per-object/whole-`S` counters, hashes exact bytes, requires exact EOF and fsyncs before use. Reopen by descriptor, require a stable regular inode, parse the complete archive, validate all paths/kinds/parents/counts and build a bounded disk-backed offset index. The publishing path additionally verifies every physical member and the complete trusted capture before `ArtifactBackend.Put`; Put independently recomputes digest and length and refuses a key mismatch.

Phase 2 materializes from that same immutable verified object into a random exclusive private root. Traverse via directory descriptors using `O_DIRECTORY|O_NOFOLLOW`; create leaves with `O_CREAT|O_EXCL|O_NOFOLLOW`, require regular files, copy bounded indexed ranges, fsync files and directories, and seal the tree before handoff. Generic tar extraction, `MkdirAll` through attacker-writable parents, string-prefix path checks, caller-relative paths, existing-leaf reuse and post-hoc symlink checks are forbidden.

Shared artifact objects are read-only to source and verifier. If ClickHouse import mutates files or directories, B2 supplies a separately budgeted private copy/reflink and proves the authenticated object remains unchanged. chmod, a successful precheck or a hardlink into writable ClickHouse storage is not an immutability proof. Pending files, indexes and commit records are not complete objects.

Closing a stream joins its handle lease but does not spend durable ownership. A crash during either phase preserves uncertain protection until exact lifecycle closure. Private uncommitted temporary cleanup is separate from object/reference GC.

## 7. Exact disposition protocol surface

AP creates `proto/artifact_disposition.proto` in package `arbiter`, importing existing `arbiter.proto` and `replay.proto`. `raftlog.proto` imports it and appends `ArtifactDispositionCmd artifact_disposition = 28;`; tags 1 through 27 remain untouched and tag 28 must be collision-checked again immediately before editing AP.

The new service is exactly `ArtifactDispositionControl` with RPCs `ApplyArtifactDisposition` and `GetArtifactDisposition`. The new canonical digest domains are `artifact-disposition-command-v1` and `artifact-disposition-reply-v1`. The only new administrator JWS purpose is `housegate-artifact-disposition-command-v1`. There is no validation-signing purpose, response-signing key, raw-Raft endpoint or sign-arbitrary-bytes RPC.

`ArtifactDispositionCommandRoot` is exactly `CanonicalDigest("artifact-disposition-command-v1", command)`. `ArtifactDispositionReplyRoot` is exactly `CanonicalDigest("artifact-disposition-reply-v1", replyBody)` and excludes the proof. Both therefore retain the existing `housegate-replay-mvp-v0:` prefix, domain, NUL and canonical-JSON construction; no hash is computed over protobuf bytes.

For every declaration below, field order is canonical JSON order and `@n` is the protobuf tag. Ordinary records emit all fields, including zero values, and arrays are non-nil `[]`. Oneofs encode exactly one selected snake-case object member; unknown, duplicate, missing, null, noncanonical, wrong-cardinality or unknown-state/action input refuses. Required proto message presence and unknown envelope fields are checked. Digests are lowercase `0x` plus 64 hex digits; addresses are lowercase `0x` plus 40 hex digits; `request_id` and `reply_nonce` are lowercase 64-character hex without `0x`.

```text
ArtifactDispositionPolicyV1 {
  policy_id: string @1
  kind: string @2
  administrator_addresses: string[] @3
}
```

`kind` is exactly `explicit_historical_window_v1`; administrator addresses are a nonempty bytewise-sorted unique set and cannot bootstrap their caller.

```text
ArtifactDispositionOriginV1 {
  kind: string @1
  parent_snapshot_id: string @2
  safe_block_seq: u64 @3
  activation_id: string @4
  transition_root: string @5
  client_account: string @6
  statement_id: string @7
  request_id: string @8
  reservation_id: string @9
  fencing_generation: u64 @10
  block_seq: u64 @11
  statement_seq: u64 @12
  statement_root: string @13
  input_root: string @14
  user_jws_hash: string @15
  execution_outcome: string @16
  candidate_seq: u64 @17
}
ArtifactDispositionUseV1 {
  reference_id: string @1
  pin: SnapshotPin @2
  principal_id: string @3
  origin: ArtifactDispositionOriginV1 @4
  continuation_obligation_seq: u64 @5
}
```

Origin shapes are exact: `genesis` has `safe_block_seq=0` and every other identity field zero; `v2` has parent and positive safe block; `transition` adds activation ID and transition root; `reservation` has account, statement, C2 request, reservation and positive generation; `query` has parent, assigned block/statement, account/statement, reservation/generation, statement/input roots and original JWS hash, with optional `execution_outcome=applied|aborted`; and `publication` has only positive `candidate_seq`. Candidate registration accepts only genesis/v2/transition/query, and a candidate obligation uses the separate publication origin.

```text
ArtifactDispositionBindPolicyV1 { policy: ArtifactDispositionPolicyV1 @1 }
ArtifactDispositionRegisterCandidateV1 {
  pin: SnapshotPin @1
  manifest: SafeSnapshotManifest @2
  publisher_id: string @3
  retention_policy_id: string @4
  publication_reference_id: string @5
  origin: ArtifactDispositionOriginV1 @6
}
ArtifactDispositionRecordReadyV1 {
  candidate_seq: u64 @1
  submission: SnapshotArtifactReadySubmission @2
}
ArtifactDispositionPublishCandidateV1 {
  candidate_seq: u64 @1
  manifest: SafeSnapshotManifest @2
  transition: ExecutorProfileTransition @3
  transition_receipts: ExecutorProfileTransitionReceipt[] @4
}
ArtifactDispositionCancelCandidateV1 {
  candidate_seq: u64 @1
  reason_code: string @2
}
ArtifactDispositionRetirementTargetV1 {
  pin: SnapshotPin @1
  retention_policy_id: string @2
}
ArtifactDispositionAdmitUseV1 { use: ArtifactDispositionUseV1 @1 }
ArtifactDispositionCloseUseV1 {
  use: ArtifactDispositionUseV1 @1
  expected_registry_revision: u64 @2
}
ArtifactDispositionOpenChallengeV1 {
  pin: SnapshotPin @1
  origin: ArtifactDispositionOriginV1 @2
  attestation: SnapshotQueryAttestation @3
  replay_use: ArtifactDispositionUseV1 @4
}
ArtifactDispositionResolveObligationV1 { obligation_seq: u64 @1 }
ArtifactDispositionActionV1 oneof action {
  bind_policy: ArtifactDispositionBindPolicyV1 @1
  register_candidate: ArtifactDispositionRegisterCandidateV1 @2
  record_ready: ArtifactDispositionRecordReadyV1 @3
  publish_candidate: ArtifactDispositionPublishCandidateV1 @4
  cancel_candidate: ArtifactDispositionCancelCandidateV1 @5
  begin_retirement: ArtifactDispositionRetirementTargetV1 @6
  finish_retirement: ArtifactDispositionRetirementTargetV1 @7
  admit_use: ArtifactDispositionAdmitUseV1 @8
  close_use: ArtifactDispositionCloseUseV1 @9
  open_challenge: ArtifactDispositionOpenChallengeV1 @10
  resolve_obligation: ArtifactDispositionResolveObligationV1 @11
}
ArtifactDispositionCommandV1 {
  version: u32 @1
  network_id: string @2
  keeper_shard_id: u32 @3
  actor_id: string @4
  request_id: string @5
  expected_revision: u64 @6
  action: ArtifactDispositionActionV1 @7
}
```

Version is exactly 1. `reason_code` is `origin_superseded`, `preparation_failed` or `operator_cancelled`. Non-transition publication carries the required zero-value existing transition and `transition_receipts=[]`; transition publication carries the exact approved old records. `ResolveObligation` carries no caller outcome. `OpenChallenge` requires the exact signed mismatch attestation and admitted replay use; it cannot turn restore failure or an unsigned reason into challenge authority.

The administrator payload is `ArtifactDispositionAdminPayloadV1{purpose,version,iat,command}` in that exact order, with purpose `housegate-artifact-disposition-command-v1`, version 1 and positive Unix-seconds `iat`. It uses the existing strict compact ES256K convention, raw base64url, Keccak256 signing input, 65-byte low-S `R||S||V`, `V=27/28`, strict header/payload re-encoding and recovered canonical actor address. Live service admission applies configured freshness; deterministic apply and historical proof verification do not derive time policy from `iat`.

```text
ArtifactDispositionRegistryObservationV1 {
  reference_id: string @1
  pin: SnapshotPin @2
  principal_id: string @3
  registry_revision: u64 @4
  observation: string @5
  record_digest: string @6
}
ArtifactDispositionSourceObservationV1 {
  obligation_seq: u64 @1
  source_kind: string @2
  source_index: u64 @3
  source_identity: ArtifactDispositionOriginV1 @4
  replacement_obligation_seqs: u64[] @5
}
ArtifactDispositionValidationV1 {
  version: u32 @1
  command_root: string @2
  actor_id: string @3
  actor_role: string @4
  administrator_jws_hash: string @5
  registry_observations: ArtifactDispositionRegistryObservationV1[] @6
  source_observations: ArtifactDispositionSourceObservationV1[] @7
}
```

Validation is private typed data produced only by the authenticated AR service after crypto, role, object and protected-registry checks; it is never accepted from Apply callers. Roles are exactly `governance_admin`, `policy_admin`, `publisher`, `source`, `verifier`, `coordinator`. Registry observations are `registered|closed|debt_settled|debt_transferred`; source kinds are `reservation_released|query_terminal|candidate_terminal|challenge_resolved|cleanup_settled`. `record_digest` links immutable observation bytes but does not authorize by being nonempty. Apply rechecks exact identities, revisions and all mutable replicated predicates without performing crypto, filesystem or RPC work.

| Action | Authenticated actor and administrator JWS | Required predicate |
|---|---|---|
| `bind_policy` | configured governance administrator; required | new immutable policy, revision 0, exact kind and sorted administrators; supplied list cannot authorize its own caller |
| `register_candidate` | configured trusted publisher; JWS empty | publisher=actor, exact policy/origin/full sealed `S`, actual registered publication reference, one live origin slot and no RETIRED `S`; allocate C and candidate obligation atomically |
| `record_ready` | candidate publisher; JWS empty | exact live C/pin/publisher/policy/provisional protection plus all existing schema/O/root/readiness checks |
| `publish_candidate` | configured publication coordinator; JWS empty | C ready, exact manifest/transition/receipts, origin terminal checks and local publication ownership; atomically publish old record, C and OPEN retirement state |
| `cancel_candidate` | publisher for `preparation_failed`, origin coordinator for `origin_superseded`, policy administrator with required JWS for `operator_cancelled` | exact nonpublished C/revision and origin permission; preserve readiness and obligations while fencing future readiness/publication |
| `begin_retirement` | committed policy administrator; required | exact OPEN published noncurrent S/policy and valid current successor |
| `finish_retirement` | configured disposition coordinator; JWS empty | exact CLOSING noncurrent S with every use/obligation/debt/unknown/pending dependency and actual process closure resolved |
| `admit_use` | exact authenticated source/verifier/lifecycle principal; JWS empty | actual live registry core and published S/O/history; independent admission only OPEN, continuation only from exact live pre-cut obligation |
| `close_use` | same lifecycle principal or configured recovery coordinator; JWS empty | irreversible actual closed registry observation at requested revision with handles/children/submissions joined or fenced |
| `open_challenge` | authorized verifier; JWS empty | exact job/source claim, signed mismatch attestation and currently admitted replay use; OPEN or exact bounded pre-cut continuation |
| `resolve_obligation` | configured disposition coordinator; JWS empty | no caller outcome; exact authoritative registry/source settlement observations and current parent/children/use/C2/C3 state |

```text
service ArtifactDispositionControl {
  rpc ApplyArtifactDisposition (ApplyArtifactDispositionRequest) returns (ArtifactDispositionReply);
  rpc GetArtifactDisposition (GetArtifactDispositionRequest) returns (ArtifactDispositionReply);
}
message ApplyArtifactDispositionRequest {
  ArtifactDispositionCommandV1 command = 1;
  string administrator_jws = 2;
  string reply_nonce = 3;
}
message ArtifactDispositionCmd {
  ArtifactDispositionCommandV1 command = 1;
  string administrator_jws = 2;
  ArtifactDispositionValidationV1 validation = 3;
}
```

`reply_nonce` binds one fresh RPC response and is not part of command idempotency or Raft state.

## 8. Selectors, state, replies and authenticated proof interpretation

```text
ArtifactDispositionRequestKeyV1 {
  actor_id: string @1
  request_id: string @2
  command_root: string @3
}
ArtifactDispositionObligationOwnerV1 {
  pin: SnapshotPin @1
  kind: string @2
  origin: ArtifactDispositionOriginV1 @3
}
ArtifactDispositionSelectorV1 oneof selector {
  policy_id: string @1
  candidate_seq: u64 @2
  retirement: ArtifactDispositionRetirementTargetV1 @3
  use: ArtifactDispositionUseV1 @4
  obligation_seq: u64 @5
  operation: ArtifactDispositionRequestKeyV1 @6
  obligation_owner: ArtifactDispositionObligationOwnerV1 @7
}
GetArtifactDispositionRequest {
  version: u32 @1
  network_id: string @2
  keeper_shard_id: u32 @3
  reader_id: string @4
  selector: ArtifactDispositionSelectorV1 @5
  reply_nonce: string @6
}
```

The authenticated `reader_id` must equal the request. Reads cross a leader Barrier and return exact current committed state. Operation lookup reconciles a lost response by persisted actor/request/root; a different root conflicts and never becomes absence. `obligation_owner` resolves exact reservation/query/candidate roots and never chooses a latest matching owner.

```text
ArtifactDispositionPolicyRecordV1 {
  policy: ArtifactDispositionPolicyV1 @1
  revision: u64 @2
  commit_index: u64 @3
}
ArtifactDispositionCandidateRecordV1 {
  candidate_seq: u64 @1
  pin: SnapshotPin @2
  publisher_id: string @3
  retention_policy_id: string @4
  publication_reference_id: string @5
  origin: ArtifactDispositionOriginV1 @6
  state: string @7
  revision: u64 @8
  ready: SnapshotArtifactReadySubmission @9
  obligation_seq: u64 @10
  registered_index: u64 @11
  terminal_index: u64 @12
}
ArtifactDispositionRetirementRecordV1 {
  pin: SnapshotPin @1
  retention_policy_id: string @2
  state: string @3
  revision: u64 @4
  published_candidate_seq: u64 @5
  cut_index: u64 @6
  retired_index: u64 @7
  active_use_ids: string[] @8
  unresolved_obligation_seqs: u64[] @9
}
ArtifactDispositionUseRecordV1 {
  use: ArtifactDispositionUseV1 @1
  state: string @2
  revision: u64 @3
  admitted_index: u64 @4
  closed_index: u64 @5
  registry_closed_revision: u64 @6
  closure_command_root: string @7
}
ArtifactDispositionObligationRecordV1 {
  obligation_seq: u64 @1
  pin: SnapshotPin @2
  kind: string @3
  origin: ArtifactDispositionOriginV1 @4
  parent_obligation_seq: u64 @5
  state: string @6
  revision: u64 @7
  admitted_index: u64 @8
  resolved_index: u64 @9
  replacement_obligation_seqs: u64[] @10
  resolution_source_observations: ArtifactDispositionSourceObservationV1[] @11
}
ArtifactDispositionResultV1 {
  actor_id: string @1
  request_id: string @2
  command_root: string @3
  code: string @4
  commit_index: u64 @5
  target: ArtifactDispositionSelectorV1 @6
  target_revision: u64 @7
}
ArtifactDispositionRecordV1 oneof record {
  policy: ArtifactDispositionPolicyRecordV1 @1
  candidate: ArtifactDispositionCandidateRecordV1 @2
  retirement: ArtifactDispositionRetirementRecordV1 @3
  use: ArtifactDispositionUseRecordV1 @4
  obligation: ArtifactDispositionObligationRecordV1 @5
}
ArtifactDispositionReplyBodyV1 {
  version: u32 @1
  network_id: string @2
  keeper_shard_id: u32 @3
  reader_id: string @4
  reply_nonce: string @5
  selector: ArtifactDispositionSelectorV1 @6
  found: bool @7
  read_index: u64 @8
  operation_result: ArtifactDispositionResultV1 @9
  record: ArtifactDispositionRecordV1 @10
}
ArtifactDispositionReadProofV1 {
  version: u32 @1
  reply_root: string @2
  body: ArtifactDispositionReplyBodyV1 @3
}
ArtifactDispositionReply {
  body: ArtifactDispositionReplyBodyV1 @1
  proof: bytes @2
}
```

`operation_result` and `record` are the only optional pointer/message-presence fields in the reply body. States are candidate `preparing|ready|published|cancelled`, retirement `open|closing|retired`, use `admitted|closed|cancelled_absent`, obligation `open|resolved`. Initial revision is 1 and every phase transition increments it with overflow refusal. Result codes are `applied|revision_conflict|identity_conflict|state_conflict|retired|not_found|obligations_open`.

The proof is strict canonical UTF-8 JSON of `ArtifactDispositionReadProofV1{version:1,reply_root:ArtifactDispositionReplyRoot(body),body:body}` and the embedded body must canonically equal the outer body. It is integrity/correlation evidence, not a signature or portable authorization. Immediate clients authenticate the fixed AR service and redirect, require the leader Barrier path, and check exact scope, reader, fresh nonce, selector, identities and indices.

The fixed terminal callback remains exactly `VerifyTerminal(ctx, expectedReferenceID, expectedPin, terminalProof)`. It parses the hint, then performs a fresh `GetArtifactDisposition` with a new nonce against its constructor-fixed authenticated service and checks full pin/reference/principal/origin/policy/candidate/use/obligation identity plus immutable terminal indices. Offline or unavailable authority, registry absence, conflict or nonterminal current state refuses. An old AdmitUse success with a current closed record cannot authorize I/O. Local registry helper names and record encoding remain AC-local implementation choices.

## 9. Candidate, use, obligation and retirement lifetime

Before candidate allocation, the trusted issuer seals the candidate and creates the unchanged publication reference plus persisted operation key `(scope, actor_id, request_id)` in the protected journal. `RegisterCandidate` authenticates that exact core/key/root/manifest and atomically allocates monotone non-reusable candidate `C` plus its candidate obligation. The issuer persists returned `C` as a set-once phase fact; lost responses reconcile the original operation. It never guesses `C` from `S`, allocates a recovery replacement, or rewrites the original local owner into `publication{C}`.

Every readiness/publication operation names exact `C`. Cancellation is permanent and fences late RecordReady/Publish futures; a later authorized same-`S` attempt gets a new reference, operation key, `C` and obligation. Capability-enabled FSM apply refuses old unscoped `RecordSnapshotArtifactReadyCmd`, `PublishSafeSnapshotCmd` and transition-publication mutation paths unless they enter through the exact candidate-bound internal action. Capability-off legacy behavior and frozen vectors remain unchanged.

Operation idempotency is `(scope, authenticated actor_id, request_id)` bound to exact command root and first committed result. A different body/root conflicts and never overwrites or becomes absence. Deterministic state rejection is retained under that key. Expected revision is 0 for BindPolicy/RegisterCandidate/AdmitUse creation/OpenChallenge; exact candidate revision for Ready/Publish/Cancel; retirement revision for Begin/Finish; exact use revision for CloseUse or 0 for conditional absent close; and exact obligation revision for ResolveObligation.

CloseUse at revision 0 still includes the complete exact use and actual closed local registry observation. If admission is absent at apply, it commits a `cancelled_absent` tombstone with revision 1, `admitted_index=0` and positive closure index, preventing any delayed admission. If AdmitUse won, the close conflicts and recovery closes the exact admitted revision after quiescence. Authenticated `found=false` proves only selector absence and is never cancellation, retirement or release.

Obligation kinds are exactly `candidate|reservation|query|challenge|cleanup`. C2 grant and C3 acceptance atomically create reservation/query obligations before work can escape; transfers atomically preserve successor debt. OpenChallenge atomically creates a child obligation before replay protection drops. ResolveObligation accepts no caller verdict and requires exact committed settlement evidence. In particular, physical cleanup needs C3's authenticated typed physical-write/cleanup settlement record and index; `AckCleanup`, empty PendingCleanups, claimed booleans, timeout or absence cannot prove settlement.

The first retirement policy is explicit administrative historical-window closure. Each published `S` has an immutable committed policy and published candidate association. An authenticated policy administrator may begin `OPEN -> CLOSING` only for published noncurrent `S` with a valid current successor. The cut blocks new independent historical admissions while preserving every pre-cut use, assignment, challenge and necessary bounded descendant continuation. Sharing a pin or claimed parent is not continuation authority.

FinishRetirement records `RETIRED` only after every admitted use, obligation, child debt, unknown write, pending publication and required process closure is resolved under exact authenticated evidence; apply rechecks complete current state. The current safe tip cannot retire. `RETIRED` is monotone and retains all metadata, signatures, proofs, high-water marks and tombstones. A retired read returns explicit retired/failed-precondition rather than not-found, corrupt, invalid-signature or fallback. No automatic wall-clock expiry exists.

A never-published cancelled candidate may release its own bytes without a nonexistent retirement record only after fresh exact cancelled-C proof, resolved candidate obligation/dependent debt, actual closed registry with matching set-once C, no uncertainty or writes, and zero remaining owners/leases/debt under the common object-namespace lock. Every new candidate durably registers its own provisional owner/object associations before reuse/write/readiness. If GC wins first, the new candidate reconstructs and verifies the complete object. A published `S` sharing any object protects it until that `S` is RETIRED. Cancelled `C` and all spent identities remain permanent.

Published-safe consumption requires exact published `S`, committed readiness association and committed AdmitUse before I/O. Unpublished candidate protection permits preparation/object writes under its exact candidate obligation and provisional owner but never public-safe reads. Published retirement and cancelled-unpublished cleanup are distinct terminal facts.

AR persists capability version 0/1, fixed scope/trust configuration identity, candidate/obligation high-water marks, policies, full candidate/retirement/use/obligation records, operation roots/results/validation evidence and applied indices. The coordinated new snapshot version includes this state from its first release; recheck the currently free version immediately before implementation and never reinterpret a published version. Existing v2 snapshots migrate with capability 0 and empty new state; no old manifest implies readiness, policy, candidate, use or retirement. Mixed-version enablement refuses and old readers reject the new snapshot format.

## 10. Ownership and required evidence

C1 owns pure HG records/auth/domains, AP messages/service/tag 28, AC lossless conversion/client and AR authenticated validation/proof/FSM/migration/old-path guards. C2/C3 atomically create and settle their exact obligations and debts. B5/C5 integrate AdmitUse, continuation and CloseUse with the existing registry. D3 wires the actual per-candidate issuer, fixed endpoint/admin trust, policy, protected registry, filesystem/process fence and default-disabled capability. D4 supplies transport, Raft failover/restart, cut/continuation/retirement and real deletion evidence.

B1 local gates require independent literal HGPART vectors; all path/count/one-over/whole-`S` bounds; full v2/v3 source-derived parser vectors; independent v4 NONE/ZSTD/multiframe qualification; filesystem block/inode/quota reservation; cold restart; object substitution; writable-descriptor, symlink, parent-replacement and same-key races; and all five registry classes. Existing v4/LZ4 evidence is reused rather than claimed as proof for other wrappers.

C1 gates require complete independent vectors for all eleven actions, both digest domains and the administrator JWS; Go/proto/conformance round trips; fixed endpoint/nonce/full-identity failures; old operation result with current closed state; capability-off/mixed-version/old-unscoped guards; delayed old candidate futures; all absent-close orderings; restart/tombstones; and fresh authenticated proof refusal on forged/offline/nonterminal state. Protocol-shape examples are not frozen literal vectors.

D4 must demonstrate noncurrent/current-tip separation, new historical admission before and after a cut, protected pre-cut work, replay-to-challenge continuation after a cut, actual closed registry/process evidence, lost candidate-result recovery, same-`S` new-candidate versus stale cancelled-C GC in both lock orders, shared-object protection, successful cancelled-never-published cleanup and successful published-noncurrent retirement/deletion. Documentation approval is not runtime acceptance.

All support claims cite durable repository commits and generated/conformance tests. Private host paths, ignored planning artifacts, private binaries and credentials are not normative evidence. The checked-in plan records the exact file/test ownership; implementation reports must distinguish contract publication, code/PR/CI status, disposable acceptance and production activation.
