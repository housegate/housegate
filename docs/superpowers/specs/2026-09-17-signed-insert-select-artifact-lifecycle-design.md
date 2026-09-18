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

The approved ZSTD closure path is AC-only: add private `dataplane/internal/checksumzstd/` containing narrowly adapted copies of the eight production files `bits.go`, `block.go`, `fse.go`, `huff.go`, `literals.go`, `window.go`, `xxhash.go` and `zstd.go` from upstream Go 1.26.3 `src/internal/zstd` at commit `2dc996f71b0ebafb77e64433e58333e049488a3c`, plus one fixed-storage adapter, tests, `BUILD.bazel`, the BSD license and an adjacent provenance manifest with original paths and hashes. Only AC's checksum wrapper imports this leaf. It adds no HG package, public decoder API, external module or dependency-pin change, and ordinary ClickHouse client traffic keeps its existing codec.

One non-copyable decoder owner is initialized once and retained by AC's single B1 worker across idle, cancellation and error. Its fixed typed storage includes an 8 MiB window, separate 128 KiB compressed-block, decoded-block and literal arrays, a 2,048-entry Huffman table, fixed FSE scratch and sequence tables, and per-owner scalar scratch; reset may reslice but cannot allocate, pool, replace backing storage, retain caller buffers or launch another worker while an old synchronous decode can run. The compile-time whole-state guard is at most 9 MiB, but that guard is only an internal structural check and does not prove the existing 32 MiB workspace ceiling.

The private synchronous one-frame decoder refuses windows above 8 MiB, dictionaries, skippable or concatenated frames and trailing bytes; checks every literal, match, final-literal, block and declared-output extension before reslicing; preserves overlap-copy semantics; consumes the optional inner checksum; and requires exact inner EOF and declared length before exposing output. The AC wrapper first validates the separate CH128 checksum and exact encoded frame boundary, then decodes, and publishes no bytes until every frame, checksum and length check passes. Static error/status storage and bounded input/output cursors replace per-frame formatted errors, generic readers, pools and growth paths.

This source choice does not qualify ZSTD by itself. B1 must record the exact AC Bazel compiler/action identities and prove escape, call-graph, stack-growth, static-data, allocation and lifetime bounds on `linux/amd64`, `darwin/arm64` and the actual AC host target; require zero decode/reset/error allocations across valid and hostile repeated frames; carry upstream tests and compare against independent implementations and ClickHouse; and cover raw/RLE/compressed blocks, one/four-stream Huffman, FSE/repeat modes, known/unknown content size, maximum-window tiny output, multiblock history, truncation, reserved/skippable/concatenated/trailing input, checksum and exact-limit failures. The complete 8 MiB window, 32 MiB workspace and 256 MiB B1-owned resident limits remain unchanged, and full archive/restore/runtime acceptance remains open until the whole B1 gate passes.

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

Disk reservation uses the sealed actual inventory. Let `B` be the qualified filesystem allocation unit in `1..1,048,576`, `roundB(x)=ceil(x/B)*B`, and `mu=16*B`. For part `i`, let `F_i` be its file sizes, `E_i` its file-plus-directory count, `L_i` its path bytes and `T_i` its exact HGPART length. Let `E=sum E_i`, `L=sum L_i`, `P=part count`, and let `M`, `O`, `Aset` be actual canonical manifest, outer-schema and artifact-set byte lengths. Compute exact `Aset` length before its hash from exact identities/lengths and fixed-length digest placeholders; placeholder bytes are never committed. If any exact canonical metadata length is not available yet, reserve that field's existing finite maximum until fixed, never a maximum payload-tree size in its place.

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
I = roundB(Iraw) + 64*mu  # at most 32 index/journal spool files plus directories
Safety = 64 MiB

D_payload = C + Objs + Temp + Verify + Safety
D_new = D_payload + I
require D_payload <= 192 GiB and I <= 1 GiB
reserve D_new, never the two ceilings unconditionally
```

For `P=0`, both maxima are zero and `I`/Safety charge staging. The `I` allowance covers bounded fixed-record indices, names, canonical copies and local journal append/spill work; implementations keep the index/journal spool count at no more than 32 files plus their directories and keep encoded record sizes within the formula's allowance, or return a capacity error before growth. Reserve at least `sum_i(E_i+1) + (P+1) + max_i(E_i+1) + 65` inodes. Attribute every term to the volume that stores it; never sum free space across volumes. Apply logical whole-`S` bounds before physical reservation and take no dedup, hardlink, compression or sparse-allocation saving. A tiny candidate reserves its conservative actual footprint; 193 GiB is the maximum allowed reservation, not a mandatory free-space minimum.

`mu` and the name debit are conservative admission charges only for a qualified filesystem profile, not asserted overhead for arbitrary filesystems. D3/D4 measure the supported mount's block, inode and metadata behavior and prove those debits conservative; a mount needing more refuses or requires a separately reviewed profile. Capacity growth is checked under the common reservation lock before the next write and may not exceed either the reserved amount or these allowances.

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

Origin shapes are exact, and every field not named for the selected kind is empty/0. `genesis` has `safe_block_seq=0`; `v2` has parent snapshot ID and positive safe block; `transition` adds activation ID and transition root; `reservation` has client account, statement ID, C2 request ID, original reservation ID and positive original fencing generation. `query` has parent, assigned block/statement sequence, account/statement, reservation/original generation, statement/input roots and exact original JWS hash; its `request_id`, `safe_block_seq` and `candidate_seq` are zero. Its `execution_outcome` is empty for accepted/replay/challenge identity and is `applied|aborted` only for a particular query publication candidate selected by committed C3 resolving authority. `publication` has only positive `candidate_seq`. Candidate registration accepts only genesis/v2/transition/query, requires the selected committed outcome for a query candidate, and creates its obligation with the separate `publication{candidate_seq:C}` origin rather than mutating the candidate's original authorized origin.

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
  capacity_allowance_ordinal: u64 @8
}
```

Validation is private typed data produced only by the authenticated AR service after crypto, role, object and protected-registry checks; it is never accepted from Apply callers. Roles are exactly `governance_admin`, `policy_admin`, `publisher`, `source`, `verifier`, `coordinator` and are validator-selected for the action rather than request-selected. Registry observations sort by exact reference ID and are unique. Source observations sort uniquely by `(obligation_seq, source_kind, source_index)`; every `replacement_obligation_seqs` array is ascending and unique. `administrator_jws_hash` is `DigestString(exact supplied administrator token)` when that action requires the token and is empty otherwise; the exact token remains in immutable audit/replay identity. Registry observations are `registered|closed|debt_settled|debt_transferred`, and `record_digest` is exactly `DigestBytes(exact immutable observation export bytes)` from the trusted registry adapter, never permission inferred from a nonempty digest. Source kinds are `reservation_released|query_terminal|candidate_terminal|challenge_resolved|cleanup_settled`; `source_index` is the positive committed index of the matching authoritative retained record, not a caller-selected index. Apply rechecks exact root, actor, token digest, role, observation identities/revisions and all mutable replicated predicates without performing crypto, filesystem or RPC work.

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
| `open_challenge` | authorized verifier; JWS empty | exact job/source claim, signed mismatch attestation and currently admitted same-job/same-claim/same-principal replay use; OPEN ordinarily; in CLOSING either that exact U was independently admitted before cut or a post-cut U names its still-open pre-cut ancestor and bounded assignment link |
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

`operation_result` and `record` are the only optional pointer/message-presence fields in the reply body. Get of an ordinary record omits `operation_result`; Get of an operation or Apply includes it when the operation is known. Authenticated absence sets `found=false` and omits `record`; a rejected known operation without an existing target may include `operation_result` while `found=false`. When `record` is present its oneof has exactly one member, so absence never encodes as null or a zero record. A PREPARING candidate carries the required zero-valued existing `SnapshotArtifactReadySubmission`; after readiness it carries the exact immutable submission even if later CANCELLED, and the zero value is never readiness.

States are candidate `preparing|ready|published|cancelled`, retirement `open|closing|retired`, use `admitted|closed|cancelled_absent`, obligation `open|resolved`. Initial revision is 1 and every actual phase transition increments it with overflow refusal; policy revision remains 1, and retirement revision changes only on retirement phase transitions. Result codes are `applied|revision_conflict|identity_conflict|state_conflict|retired|not_found|obligations_open`. Every set-valued array in a state reply is sorted and unique: string IDs bytewise, numeric obligation sequences ascending and retained source observations by `(obligation_seq,source_kind,source_index)`. An admitted use has empty `closure_command_root`; closed/cancelled-absent uses have the nonempty exact validated CloseUse command root. An open obligation has empty `resolution_source_observations`; a resolved obligation retains the exact settlement observations, whose source indices do not exceed `resolved_index`.

After operation resolution for Apply, or when serving Get, the actual leader crosses Barrier and then atomically captures the immutable operation result and current selected record under one FSM read lock; `read_index` is that state's last applied committed index. For Apply, `body.selector` is exactly `operation_result.target`. For Get(operation), `body.selector` echoes the requested operation selector while `operation_result.target` names the current record returned; the persisted actor/request/root must match the requested operation. Ordinary Get uses its requested record selector. In every case `read_index` is at least every returned record transition index, result `commit_index` and other returned immutable terminal index.

The proof is strict compact canonical UTF-8 JSON of `ArtifactDispositionReadProofV1{version:1,reply_root:ArtifactDispositionReplyRoot(body),body:body}` and the embedded body must byte-for-byte canonically equal the outer body. It is integrity/correlation evidence, not a signature or portable authorization. Immediate clients authenticate the fixed AR service and any redirect, require the leader Barrier path, and check exact version, scope, reader, fresh nonce, selector/result relationship, full identities and indices.

The fixed terminal callback remains exactly `VerifyTerminal(ctx, expectedReferenceID, expectedPin, terminalProof)`. It validates the hint's exact shape, self-consistency and full expected class-owner identity, then performs a fresh `GetArtifactDisposition` with a new nonce and exact selector against its constructor-fixed authenticated service. The fresh response must retain the same claimed immutable terminal transition/index/origin and command root when present; later monotone phases may advance, so the entire body is not compared byte-for-byte. The verifier follows exact named candidate or retirement association and then exact policy, plus any obligation dependencies required by the predicate; it never chooses latest-by-`S`. Separate reads decide only immutable identity/monotone facts, while predicates depending on simultaneous mutable state remain atomic apply decisions.

I/O and terminal release use different predicates. Current `admitted` plus an actually live local acquisition and the applicable class/continuation predicate authorizes I/O. Current exact `closed` or `cancelled_absent`, irreversible actual local closure and all applicable natural-owner, candidate, retirement, policy and obligation terminal facts are required to authorize release of that acquisition; `closed U` alone is insufficient. An old AdmitUse success with a current closed record cannot authorize I/O, but that exact closed record is the expected use-terminal evidence for release after the remaining facts pass. Unrelated/nonterminal disposition, identity conflict, registry absence or unavailable authority refuses release. Published-object deletion additionally requires RETIRED plus local quiescence; never-published cancelled candidates use their distinct candidate-terminal predicate. Local registry helper names and record encoding remain AC-local implementation choices.

## 9. Candidate, use, obligation and retirement lifetime

Before candidate allocation, the trusted issuer seals the candidate and creates the unchanged publication reference plus persisted operation key `(scope, actor_id, request_id)` in the protected journal. `RegisterCandidate` authenticates that exact core/key/root/manifest and atomically allocates monotone non-reusable candidate `C` plus its candidate obligation. The issuer persists returned `C` as a set-once phase fact; lost responses reconcile the original operation. It never guesses `C` from `S`, allocates a recovery replacement, or rewrites the original local owner into `publication{C}`.

Every readiness/publication operation names exact `C`. Cancellation is permanent and fences late RecordReady/Publish futures; a later authorized same-`S` attempt gets a new reference, operation key, `C` and obligation. Capability-enabled FSM apply refuses old unscoped `RecordSnapshotArtifactReadyCmd`, `PublishSafeSnapshotCmd` and transition-publication mutation paths unless they enter through the exact candidate-bound internal action. Capability-off legacy behavior and frozen vectors remain unchanged.

Operation idempotency is `(scope, authenticated actor_id, request_id)` bound to exact command root and first committed result. Before any fresh-capacity decision, an existing exact key/root returns its immutable first result plus a fresh current record/proof; another root conflicts and never overwrites or becomes absence. A deterministic state rejection is permanent only if apply atomically acquired a bounded rejection-result slot. Shape, authentication, credential, message-limit or capacity failure that acquires no such slot is not admitted, returns the appropriate gRPC error (`ResourceExhausted` for capacity), and creates no result, tombstone or `found=false` success. Expected revision is 0 for BindPolicy/RegisterCandidate/AdmitUse creation/OpenChallenge; exact candidate revision for Ready/Publish/Cancel; retirement revision for Begin/Finish; exact use revision for CloseUse or 0 for conditional absent close; and exact obligation revision for ResolveObligation.

CloseUse at revision 0 still includes the complete exact use and actual closed local registry observation. If admission is absent at apply, it commits a `cancelled_absent` tombstone with revision 1, `admitted_index=0` and positive closure index, preventing any delayed admission. If AdmitUse won, the close conflicts and recovery closes the exact admitted revision after quiescence. Authenticated `found=false` proves only selector absence and is never cancellation, retirement or release.

Obligation kinds are exactly `candidate|reservation|query|challenge|cleanup`. C2 grant and C3 acceptance atomically create reservation/query obligations before work can escape; transfers atomically preserve successor debt. ResolveObligation accepts no caller verdict and requires exact committed settlement evidence. In particular, physical cleanup needs C3's authenticated typed physical-write/cleanup settlement record and index; `AckCleanup`, empty PendingCleanups, claimed booleans, timeout or absence cannot prove settlement.

OpenChallenge validates the exact currently admitted replay use, verifier principal, job, claim and signed mismatch and commits the challenge obligation before any replay close, release or scheduling-reference removal. While CLOSING, a replay use with `admitted_index < cut_index` may open only its exact original job/claim/mismatch challenge. If that U was an independent pre-cut replay, the challenge records `parent_obligation_seq=0` and its immutable OpenChallenge operation retains the exact originating `ArtifactDispositionUseV1`; no invented ancestor is required. A U admitted after cut instead must name a still-open pre-cut ancestor obligation and prove the deterministic bounded assignment link to that same job/claim, and the child records that ancestor as parent. Apply refuses an already closed U. After committed challenge creation, a later I/O continuation acquires its approved local challenge reference and admits a continuation use linked to the challenge obligation.

The first retirement policy is explicit administrative historical-window closure. Each published `S` has an immutable committed policy and published candidate association. An authenticated policy administrator may begin `OPEN -> CLOSING` only for published noncurrent `S` with a valid current successor. The cut blocks new independent historical admissions while preserving every pre-cut use, assignment, challenge and necessary bounded descendant continuation. Sharing a pin or claimed parent is not continuation authority.

FinishRetirement records `RETIRED` only after every admitted use, obligation, child debt, unknown write, pending publication and required process closure is resolved under exact authenticated evidence; apply rechecks complete current state. The current safe tip cannot retire. `RETIRED` is monotone and retains all metadata, signatures, proofs, high-water marks and tombstones. A retired read returns explicit retired/failed-precondition rather than not-found, corrupt, invalid-signature or fallback. No automatic wall-clock expiry exists.

A never-published cancelled candidate may release its own bytes without a nonexistent retirement record only after fresh exact cancelled-C proof, resolved candidate obligation/dependent debt, actual closed registry with matching set-once C, no uncertainty or writes, and zero remaining owners/leases/debt under the common object-namespace lock. Every new candidate durably registers its own provisional owner/object associations before reuse/write/readiness. If GC wins first, the new candidate reconstructs and verifies the complete object. A published `S` sharing any object protects it until that `S` is RETIRED. Cancelled `C` and all spent identities remain permanent.

Published-safe consumption requires exact published `S`, committed readiness association and committed AdmitUse before I/O. Unpublished candidate protection permits preparation/object writes under its exact candidate obligation and provisional owner but never public-safe reads. Published retirement and cancelled-unpublished cleanup are distinct terminal facts.

AR persists capability version 0/1, fixed scope/trust/capacity-profile identity, candidate/obligation high-water marks, policies, full candidate/retirement/use/obligation records, operation roots/results/validation evidence, capacity counters/reservations/allowance bindings and applied indices. The coordinated new snapshot version includes this state from its first release; C1 selects the still-free version from actual source immediately before implementation and never reinterprets a published version. Existing v2 snapshots migrate with capability 0 and no enabled capacity table; no old manifest implies readiness, policy, candidate, use, retirement or completion credit. Mixed-version enablement refuses and old readers reject the new snapshot format.

## 10. Finite disposition capacity and completion credits

All byte units below are powers of 1024, string limits count UTF-8 bytes, and record limits count the exact encoded representation. New wrappers have a maximum nesting depth of 32 and refuse boundary 33 before oversized decode or allocation. Checked UInt64 arithmetic, nesting-depth checks, repeated-count checks and cumulative byte checks occur before conversion, allocation or `proto.Unmarshal`; every inclusive maximum has an exact maximum and maximum-plus-one test. Existing stricter syntax, hash, identity and whole-manifest rules remain conjunctive. No input is truncated, normalized, replaced by a hash or silently reinterpreted, and these enabled-control limits do not change old v2 structs, hashes or canonical bytes.

The scalar and nested ceilings are: existing digest/address/request-ID/nonce spellings retain their exact 66/42/64/64 ASCII lengths; every free-form network, actor, principal, publisher, policy, activation, client, statement, reservation or other control identity is at most 256 bytes; an approved reference ID is at most 1,024 ASCII bytes under its unchanged grammar; every closed-vocabulary kind/state/action/role/reason is at most 64 bytes; administrator compact JWS is at most 48 KiB with at most 32 KiB decoded payload; existing readiness or receipt/attestation signatures are at most 16 KiB each; complete pin, origin and selector encodings are at most 8 KiB, 8 KiB and 24 KiB; complete UseV1 is at most 20 KiB; policy administrators are at most 64 sorted unique canonical addresses; registry and source observations are at most 16 each; replacement obligation IDs are at most 16 per source observation or obligation record; and PublishCandidate carries at most 64 transition receipts totaling at most 256 KiB canonical bytes, with one embedded transition at most 64 KiB. The existing exact canonical manifest remains 32 MiB. These control-envelope limits do not impose the 256-byte free-form ceiling on nested manifest SQL/schema names; their already-adopted complete-manifest limits remain authoritative.

Aggregate ceilings are independent: a small canonical command is at most 256 KiB, a manifest-bearing RegisterCandidate or PublishCandidate command at most 34 MiB, a small Apply protobuf at most 768 KiB, a manifest-bearing Apply protobuf at most 36 MiB, private validation canonical JSON and protobuf at most 256 KiB each, a small complete tag-28 Raft command at most 1 MiB and a manifest-bearing tag-28 command at most 40 MiB. Canonical and protobuf Get requests are each at most 32 KiB; a selected canonical state record is at most 256 KiB; canonical ReplyBodyV1 is at most 384 KiB; canonical ReadProofV1 is at most 385 KiB; and the complete protobuf reply and any local outer JSON reply are each at most 1 MiB. The outer JSON calculation charges exact proof expansion `4*ceil(proof_bytes/3)` plus the outer body, keys and escapes; no two-body approximation proves it. Large actions require an empty administrator JWS. Get returns result plus selected bounded current metadata and never returns the manifest, retained command, validation or audit payload.

Every published pin admits at most 128 actual or reserved future active uses and 256 actual or reserved future unresolved obligations. Returned lists contain only actual IDs, each once in the specified sorted order, while reservations charge future cardinality and worst encoded read growth without fabricating response entries. Before admission, the FSM proves the future complete record, reply body, proof and outer response fit using 20-digit UInt64 values, maximum identities and each actual escaped reference-ID length plus punctuation. Apply refuses a count or encoded-size overflow before side effects; close/resolve converts or removes already-reserved occupancy and never truncates a list. An imported violation refuses enablement rather than pruning history.

The absolute finite ceilings are 16 immutable policies; 128 allocated candidates; 64 published disposition records; 8,192 uses including admitted, closed and cancelled-absent; 8,192 obligations including resolved; 32,768 successfully admitted operation-result keys; 1,024 retained deterministic rejection-result keys; and 16 active independent/root work families, with the per-pin and encoded-read ceilings still binding. The rejection pool is also limited to 16 MiB actual plus reserved bytes. These absolute counts do not create unfunded capacity: the enabled profile admits work only when its complete vector fits every count and byte component.

All admitted results, spent identities and allocated ordinals are permanent. A successfully admitted operation stores its exact root and first result forever. A new operation atomically acquires either its full semantic/completion vector or a bounded rejection cell under the FSM lock; stale preflight is repeated at apply. Insufficient capacity produces internal `NotAdmittedCapacity`, mapped to `ResourceExhausted`, without map, counter or sequence mutation. Fresh IDs for an already-completed semantic action are rejected before proposal or consume only the reject pool; they never spend another family's completion reservation. Retained rejection exhaustion also yields non-admission. Exact old retries remain available when every fresh pool is full.

The coordinated new snapshot representation must intern each distinct exact canonical manifest once as a raw JSON value under its existing digest while comparing length and exact bytes, emit it verbatim without HTML re-escaping or reordering, and recompute canonical form and digest at restore. Candidate, published-manifest map and retained commands reference the same value. A large retained command stores exact canonical prefix and suffix around the manifest as base64 plus lengths/reference and verifies the original command root on reconstruction; small commands/validations/results retain exact raw canonical bytes and tokens retain exact base64 bytes. Interned values are never garbage-collected in this profile. Migration of the existing `Manifests` map requires exact reconstruction and verification or refuses enablement; transient old/new copies are separately qualified. Hash equality alone, lossy reserialization and uncharged duplicate copies are forbidden.

Each small audit cell is at most 640 KiB: 256 KiB command + 256 KiB validation + `4*ceil(49152/3)` = 64 KiB exact base64 administrator token + 32 KiB immutable result + 32 KiB aggregate keys/references/framing. The 32 KiB result bound is independent of the 384 KiB current-record body bound. A large audit cell is at most 4 MiB: its manifest-excluding canonical prefix+suffix is at most 2 MiB raw and `4*ceil(2097152/3)` = 2,796,204 bytes base64, while validation + result + aggregate framing is at most 327,680 bytes; the remaining 1,070,420 bytes are unused ceiling, not a second allowance. The large residual limit is checked directly, and a large action token is empty. These internal limits do not reduce the unchanged 34/36/40 MiB command/request/Raft transport ceilings or the 32 MiB manifest.

The enabled whole-FSM ledger charges lifecycle JSON actual plus reserved completion growth at most 768 MiB, all non-lifecycle JSON actual plus reserved growth at most 240 MiB, and the exact complete JSON document including top-level framing at most 1,008 MiB. Any later-qualified C3 staged/final settlement actual plus reserved growth has a 256 MiB subceiling inside the 768 MiB lifecycle pool, never an additional pool. The existing JSON restore ceiling remains 1,024 MiB; its 16 MiB gap above the writer watermark is not normal admission capacity. A separate canonical SpentIDs tail is at most 64 MiB including its 13-byte header, so the complete new-version serialized snapshot is at most `13 + 1008 MiB + 64 MiB` = 1,124,073,485 bytes, where the outer 13 bytes are 4-byte magic, 1-byte version and 8-byte JSON length. Before `io.ReadAll` or decode, the new-version tail reader is bounded to the 64 MiB limit plus one byte, 67108865 bytes: exactly 64 MiB is allowed subject to the existing structural checks, while observing the extra byte refuses. Tail cost is exactly `13 + sum(2 + len(accountUTF8) + 8 + 4 + 16*rangeCount)`; admission reserves worst nonmerging growth, and restore bounds length/count/arithmetic before allocation then recomputes dictionary length and root. No spent identity or range is pruned.

C1 AR owns one deterministic shared capacity ledger for every mutation in an enabled governed FSM: tag 28, old v2 admission, node, statement, claim, verification, promotion, cleanup, manifest, spent-ID and any other co-resident state. It counts exact persisted encoding including map keys, punctuation, top-level framing and actually persisted indexes; a deterministic counting encoder matches the snapshot writer's field order, escaping and presence rules. Apply derives the exact family, target, role and allowance from authenticated private validation and committed state, builds the tentative mutation, atomically converts owned reservation to actual state, and releases a mutually exclusive unused byte branch only when that branch can no longer occur. Restore recomputes actual encodings and future branches and refuses overflow, deficit, double ownership or inconsistent credits. Default-disabled/version-2 behavior is unchanged; an enabled scope cannot accept an old mutation that bypasses the ledger.

The only protected completion-capacity admission mechanism is the finite prepaid table keyed by `(configured principal_id, approved namespace J, slot_ordinal)`. At coordinated disabled-to-enabled bootstrap, every cell is fully charged for one compiled root-family kind and its complete count/byte/read/continuation vector and is `available|bound|spent`; unbound cells conservatively charge wildcard per-pin occupancy as if they all selected one pin. The protected per-principal allocator binds one cell to exact immutable reference/core/A and fsyncs before local registration or launch. The first tag-28 operation installs that exact binding; lost responses reuse the same ordinal/reference/request, and a distinct A conflicts. No dynamic refill, rootless registration, apply-created ticket, service-local pending ticket, caller-selected credit owner or public reservation action exists.

Private `ArtifactDispositionValidationV1.capacity_allowance_ordinal` is field 8. The inspected AP baseline contains no disposition source/generated type or reserved field 8, and AP rechecks source, generated and reserved tags immediately before creation. The field is never part of the public Apply request, canonical command, JWS, command root, RPC action set or hash domain; callers cannot supply validation. A positive ordinal is used only for the first binding under the immutable protected owner principal/namespace, while zero means that exact slot binding is already installed and is derived from current state. A separately authenticated recovery coordinator may close work owned by the source/verifier principal, but apply keys credit to the immutable owner, not the recovery caller or `validation.actor_id`; an ordinal alone grants no authority.

The first AdmitUse and absent CloseUse share the same positive pre-funded slot until either action installs the binding. If Close wins, it records cancelled-absent, spends the owned cell and excludes delayed Admit; if Admit wins, it creates U and retains its normal Close reservation. An invalid or too-early Admit cannot consume or release the protected Close cell; if retained, its rejection uses the separate rejection pool. Later commands use ordinal zero and the installed exact reference/core/target binding. C2 grant similarly binds a prepaid Q root before returning success, publishes the committed family binding to the protected allocator view, and transfers the union of `grant->release` and `grant->query->applied|aborted` credits atomically on consume; it never releases and reacquires credit between phases.

The supported accepted-query family reserves at most two candidate incarnations, one published child, 32 uses, 24 obligations, 128 successful result cells, obligation depth 4 and at most 8 direct children. The 32 uses are 2 source attempts, 6 verifier attempts (2 for each of three independently authenticated selected verifiers), 12 challenge continuations, 8 cleanup acquisitions and 4 terminal-reconciliation acquisitions. The 24 obligations are 1 reservation + 1 query + 2 candidate roots + 6 verifier mismatch challenges + 2 query cleanup children + 6 challenge cleanup children + 6 descendants of the two query cleanup children. The 128 results are 64 Admit-or-absentClose/normalClose, 6 OpenChallenge, 24 ResolveObligation, 6 candidate registration/readiness/terminal operations including three large cells, 2 child-retirement results and 26 small C2/C3/reconciliation cells. One semantic phase has one winner; descendants inherit the family vector and receive no new envelope.

One maximum Q family reserves exactly 298.125 MiB (312,606,720 bytes): 64 MiB for two distinct 32 MiB interned manifests, 12 MiB for three large audit cells, 78.125 MiB for 125 small audit cells, 14.75 MiB for 59 current records, 1.25 MiB for allocator/lineage/live-list/binding/index/framing and one conditional 128 MiB aggregate C3 retained-settlement contribution. The family without C3 evidence is exactly 170.125 MiB. That last contribution is only a reservation witness inside the same family and 768 MiB lifecycle pool; it does not adopt a C3 encoding, tag 29, staging protocol, physical fence or evidence shape. C3 must still prove that all staged/final JSON copies and the complete 65,536-output/whole-S settlement fit the aggregate before this route can enable.

Each standalone publication P reserves 57.75 MiB: one 32 MiB manifest, two 4 MiB large audit cells, twenty-two 640 KiB small cells and 4 MiB of current records/indexes, with at most four uses, eight obligations and 24 results. It covers candidate, ready, terminal, exact obligation resolution and published Begin/Finish retirement routes, including bounded candidate-origin cleanup descendants. Each independent historical replay H reserves 18 MiB: twenty-four small cells = 15 MiB, use/obligation metadata at most 2 MiB and framing/index at most 1 MiB; it permits one initial U, at most one mismatch challenge for that exact U/job/claim/principal and at most four total uses, four obligations and 24 results. H cannot launch new source writes without separately funded C3 authority. Every published S retains its Begin/Finish retirement capacity. Recovery reuses the still-live acquisition/phase; attempt exhaustion blocks fresh work while its reserved abort/fence/reconcile/cleanup route remains. Credit supplies metadata/result capacity only and never waives OPEN/CLOSING rules, actor authority, actual closure, settlement or physical evidence.

The fixed first bootstrap funds Q1/Q2 at 2 × 298.125 MiB = 596.25 MiB; P0/P1 at 2 × 57.75 MiB = 115.5 MiB; H1/H2 at 2 × 18 MiB = 36 MiB; the rejection pool at 16 MiB; and policy/imported-current-retirement/profile/framing at 4 MiB. Total lifecycle actual plus reserved is exactly 767.75 MiB (805,044,224 bytes), leaving 0.25 MiB unallocated headroom that is not new-work credit. Its permanent maxima are 6 new C, 4 published child/S records, 80 U, 72 obligations, 352 successful results, 3 bootstrap policy/predecessor-retirement results and at most 1,024 rejections. The configured principals are source S0, verifiers V1/V2/V3 with actual selection 3 and quorum 2, publisher P, recovery/disposition coordinator R, governance administrator G and C2 gateway H. Independent acceptance cases use fresh isolated scopes; Q1 then Q2 and other sequential histories remain permanent within one case.

The imported predecessor and every ordinary enabled map/future completion must fit the separate 240 MiB non-lifecycle actual-plus-reserved compiler witness. The selected largest fixture includes a full 32 MiB manifest, 65,536 S parts and 65,536 duplicate output rows across a bounded partition set while retaining the complete ledger; it does not assume every independent scalar maximum or 65,536 maximally escaped partitions simultaneously. A missing exact source-command-to-old-state projection, a scope already over a ceiling or accepted work with no reconstructable completion vector refuses enablement. The conditional C3 evidence shape remains an open 128 MiB gate and cannot borrow the non-lifecycle pool.

Transport sets Apply receive to 36 MiB, Get receive to 32 KiB and reply send/receive to 1 MiB at both server and clients. Method-aware bounded decoding must enforce the smaller Get/action limits before allocation; generic post-unmarshal interception is insufficient, and compression is disabled or bounded in both framed and decoded forms. The enabled service allows at most one bulk encode/proposal, four small writes and four complete response encodes concurrently; large tag-28 entries are limited to the two manifest-bearing actions, and Raft uses `MaxAppendEntries=1`. A reserved completion queue/scheduling lane cannot be occupied by fresh roots or invalid traffic. A 40 MiB cap is not evidence that quorum replication and heartbeat deadlines work; failure requires a separately reviewed internal staging design without reducing manifest or whole-S semantics.

The 1 GiB transient-control allocation budget is a qualification target, not a Go heap proof. Account incoming protobuf, decoded objects, canonical hashing, validation, Raft encoding, reply/proof/base64, library copies, queue buffers, retained FSM heap and snapshot encoding independently; one bulk proposal's four fixed buffers already total 110.25 MiB before decoded/library copies. Qualify the 1,008 MiB JSON watermark with its additional encoded snapshot buffer or implement bounded streaming in the coordinated version. The 1 GiB selected-read limit and 256 MiB B1 allocation limit are separate budgets.

Logical credits reserve deterministic metadata/result/list capacity against competing admissions; they do not reserve physical disk or guarantee availability. This amendment keeps the existing injected Raft LogStore, StableStore and SnapshotStore and adds no 32 GiB-per-voter mandate, 32/16/8/4 GiB layout, ReservedLogStore, ReservedSnapshotStore, DurableQuota, replacement segment store or advertised physical-reservation guarantee. I/O failure, lost quorum, full disk, snapshot/compaction failure or missing host closure may stall completion; retain exact unresolved ownership, pause fresh admission on configured memory/log/storage pressure, preserve the completion lane and resume after healthy storage/consensus returns. Never fabricate successful closure, delete permanent history, lend credits or reinterpret a free-space check as guaranteed allocation. The separate B1 artifact-volume block/inode reservation remains unchanged.

Acceptance covers every scalar, nesting, repeated count and encoded object at exact maximum/+1; complete sorted 128-use/256-obligation reads; count, byte and reject-pool exhaustion; exact retry at full capacity; racing final slots; every reserved Close/Resolve/Cancel/Publish/Retire edge; exact interning/reconstruction/migration and equal-digest/different-byte refusal; 655,360/+1 small cells, 4 MiB/+1 large cells, 2 MiB/+1 residual and 32 MiB/+1 manifests; first Admit/absent Close in both orders across fsync/failover/restart and distinct owner/recovery caller; Q/P/H graph one-over cases; JSON 1,008 MiB/+1, reader 1,024 MiB, tail 64 MiB/+1 and complete snapshot limit; concurrent old/new FSM mutations; 36/40 MiB transport with real quorum/failover; fresh-ID/storage/snapshot/restart pressure; and post-recovery completion. D3 records actual headroom, queue and existing-store pressure policy; D4 measures real heap/GC/Raft/snapshot/latency and injects storage faults. Source/runtime acceptance remains open until the 240 MiB compiler witness, conditional C3 128 MiB shape/fence and complete runtime qualifications pass.

## 11. Ownership and required evidence

C1 owns pure HG records/auth/domains/bounds, AP messages/service/tag 28/private validation field 8, AC lossless conversion/client and AR authenticated validation/proof/FSM/interning/shared capacity ledger/tail bounds/migration/old-path guards. C2/C3 atomically create and settle their exact obligations, debts and owned credit transfers. B5/C5 integrate prepaid allocator binding, AdmitUse, continuation and CloseUse with the existing registry. D3 wires the fixed capacity/bootstrap/principal profile, actual per-candidate issuer, endpoint/admin trust, policy, protected registry, filesystem/process fence, existing-store pressure controls and default-disabled capability. D4 supplies limit/exhaustion/transport/Raft/snapshot/storage-fault qualification, cut/continuation/retirement and real deletion evidence.

B1 local gates require independent literal HGPART vectors; all path/count/one-over/whole-`S` bounds; full v2/v3 source-derived parser vectors; independent v4 NONE/ZSTD/multiframe qualification; the AC-private fixed-storage Go 1.26.3 decoder's provenance, compiler/escape/callgraph/stack/lifetime and zero-allocation proof on every actual target; filesystem block/inode/quota reservation; cold restart; object substitution; writable-descriptor, symlink, parent-replacement and same-key races; and all five registry classes. Existing v4/LZ4 and preparatory corpus evidence is reused only for the exact behavior it established.

C1 gates require complete independent vectors for all eleven actions, both digest domains and the administrator JWS; Go/proto/conformance round trips; exact field/tag/count/byte maxima and +1; interning and original-root reconstruction; finite result/rejection/list/ledger/tail/snapshot exhaustion; fixed endpoint/nonce/full-identity failures; old operation result with current closed state; capability-off/mixed-version/old-unscoped guards; delayed old candidate futures; all prepaid first-Admit/absent-close orderings and owner/caller distinctions; restart/tombstones; and fresh authenticated proof refusal on forged/offline/nonterminal state. Protocol-shape examples are not frozen literal vectors.

D4 must demonstrate noncurrent/current-tip separation, new historical admission before and after a cut, protected pre-cut work, replay-to-challenge continuation after a cut, actual closed registry/process evidence, lost candidate-result recovery, same-`S` new-candidate versus stale cancelled-C GC in both lock orders, shared-object protection, successful cancelled-never-published cleanup and successful published-noncurrent retirement/deletion. Documentation approval is not runtime acceptance.

All support claims cite durable repository commits and generated/conformance tests. Private host paths, ignored planning artifacts, private binaries and credentials are not normative evidence. The checked-in plan records the exact file/test ownership; implementation reports must distinguish contract publication, code/PR/CI status, disposable acceptance and production activation.
