# Snapshot-query analyzer build identity and historical routing

**Status:** Normative bounded clarification for the future signed `INSERT ... SELECT` capability. It changes no implemented runtime behavior and qualifies no current build.

This addendum defines the previously unspecified analyzer-build identity, external profile witness and historical executor routing for the [signed `INSERT ... SELECT` design](2026-09-16-signed-insert-select-design.md). It preserves the ordered `QueryProfileRecord`, the `snapshot-query-profile-v1` domain, every frozen A1 vector and RPC, the active-versus-historical admission rules, all A1–A14 gates and default-disabled runtime behavior.

## 1. Measured analyzer identity

`native_analyzer_build_digest` commits a finite immutable `NativeArtifactSetV1` using `CanonicalDigest("snapshot-native-analyzer-artifact-set-v1", set)`. The canonical ordered JSON fields are:

```text
NativeArtifactSetV1 {
  version: uint32 = 1
  artifacts: []NativeArtifactV1
}
NativeArtifactV1 {
  platform: string
  executable_digest: string
  ffi_digest: string
}
```

`artifacts` is nonempty, contains unique members and is sorted lexicographically by `(platform, executable_digest, ffi_digest)`. Each binary digest is `0x` followed by lowercase SHA-256 of the final actual artifact bytes after every stripping, signing or packaging operation that changes them. Source revisions, version strings, Go build information and caller-supplied stamps are not binary identity. Both digests matter because the native Go snapshot handlers are embedded in each host executable. Adding or rebuilding any member changes `native_analyzer_build_digest`, therefore changes Q, and requires the existing drained profile transition.

`grpc_analyzer_build_digest` remains the digest of the final complete gRPC service executable bytes. Qualification must verify that the executable contains the entire semantic parser and handler dependency closure. If semantic code can be replaced independently, a separately reviewed identity extension must include it before that build can qualify. The existing OS and deployment trust boundary remains; these local measurements do not introduce remote cryptographic attestation.

The unchanged profile identity remains `Q = CanonicalDigest("snapshot-query-profile-v1", record)`. Engines implement the canonical byte rules independently and do not import Housegate; shared literal canonical-JSON and digest vectors prove equality.

The new-profile `scalar_operators` values `prepare-integer-literal-exact-v1` and `prepare-integer-scalar-null-throw-v1`, together with the required `settings` member `cast_keep_nullable='0'`, are semantic commitments under that unchanged record and domain. The first denotes only Prepare's compiler-generated checked conversion of a losslessly proven original decimal integer literal to its authenticated integer target. The second denotes only Prepare's NULL-throw unwrap of a direct scalar subquery whose underlying integer type already equals that target. They are not ClickHouse user-function allowlist entries and add no record, RPC or digest field. Adding them and the setting changes Q through ordinary record hashing; every frozen A1/A3 vector remains byte-for-byte unchanged and new-profile vectors are separate.

## 2. Strict external profile witness

The version-1 external profile file has ordered fields `version`, `profiles`. Each profile entry has ordered fields `query_profile_id`, `record`, `native_artifact_set`. Entries are unique and sorted by `query_profile_id`, which is Q.

Every loader strictly rejects unknown, duplicate, missing, `null`, conflicting or malformed structure and malformed digests. It recomputes `native_analyzer_build_digest` from `native_artifact_set`, recomputes Q from the unchanged ordered `record`, verifies both commitments and retains immutable in-memory entries. A native engine advertises only entries containing its measured `(platform, executable_digest, ffi_digest)` tuple. A gRPC engine advertises only entries whose `grpc_analyzer_build_digest` equals its measured complete executable digest. A valid file entry that the running engine cannot execute is not supported.

Neither `record` nor `native_artifact_set` contains its own Q, a containing profile-bundle/file digest, a path or a release label. This avoids a hash cycle; the `query_profile_id` in the outer entry is the independently recomputed lookup key, while the frozen independent `column_profile_id` and `output_order_id` fields remain in `record`.

`QueryProfileRecord.platform` pins the SQL-execution ClickHouse artifact and environment. `NativeArtifactV1.platform` pins an analyzer host and is checked separately against the running analyzer platform. `clickhouse_build_digest` identifies the SQL-execution artifact; it is not an RC parser revision, native FFI digest or source revision. Initial qualification covers only the concrete Linux measured-runtime path. Native builds on other operating systems retain ordinary functionality but cannot activate snapshot-query capability until they provide equivalent running-image and immutable-load identity and pass cross-engine parity. No current digest or runtime is implicitly qualified by this document.

## 3. Linux native measurement and loading

The initial Linux measured constructor hashes the running executable through `/proc/self/exe`. It opens the requested FFI source once, copies the exact bytes into a fresh sealable memory-file descriptor, seals the object against writes, growth and shrinkage, hashes that sealed descriptor, and passes `/proc/self/fd/<fd>` to the explicit loader. It retains the descriptor and library handle through `Close` so the measured object is the loaded object. It never hashes a path and reopens it, returns to the source path, silently uses default or environment lookup, or accepts production caller-supplied hashes.

The measured constructor refuses unsupported executable-memory-file or dependency environments. A library requiring file-relative or separately replaceable semantic dependencies needs a separately reviewed immutable dependency identity before qualification; there is no permissive fallback. Ordinary `NewService(libPath)` behavior remains unchanged. A4 owns this measured constructor, witness validation and profile support calculation; A5 invokes it and probes exact Q.

## 4. Build and qualification order

Build complete test or release role executables first, measure their final bytes second, generate the corresponding external profile bundle third, then execute those exact unchanged binaries. A rebuilt test binary has a different measured identity and cannot claim the earlier profile. Test executables use their own measured test Q; production executables use a separately generated production Q.

Membership in `native_artifact_set` is necessary but does not establish semantic equivalence. A loader accepting a record, a measured binary containing the record, or an A4.1c RED corpus expectation cannot advertise either new semantic operation. The [public A4 serial substage map](../plans/2026-09-16-signed-insert-select-contracts.md#public-a4-serial-substages) defines A4.1a–A4.6: A4.4 qualifies the real native implementation, A4.5 independently qualifies the real C++ implementation, and A4.6 runs their exact Prepare SQL under the final measured Q on the pinned ClickHouse and compares paired responses. That sequence covers original-token literal proof and lowering, user-helper refusal, all integer bounds and target order, exact volatility family/pools, scalar zero/one/many behavior, wrong-setting refusal, predicates left unwrapped and derived-type mismatches. B2/B4/D4 then retain exact target-type readback and all-or-abort output/commit gates. Every final role member must pass those gates against the pinned gRPC executable and SQL-execution ClickHouse environment before publication. Preserve every published historical bundle and all referenced executable, FFI, gRPC and executor artifacts needed for its retention period.

## 5. Historical executor routing

B5's Housegate query dispatcher owns an immutable injected map from authenticated `(executor_profile_id, query_profile_id)` to an already-constructed `replay.Executor`. It validates the authenticated original historical policy before route lookup. Unknown pairs, missing routes and failed policy checks call no executor and return no receipt. B4 constructs each executor with one analyzer capable of the exact Q it serves; it is not a historical engine selector. A5 wraps one backend and does not launch historical processes or treat every record in a profile file as locally supported.

D3 host configuration and wiring construct and probe the per-pair routes from explicit native constructor inputs or retained gRPC endpoints together with the supported executor implementation. A changed native host cannot regain an old Q by loading old JSON because its executable tuple is absent. Where the executor still supports the original pair, deployment retains the old process/environment or the matching old gRPC executable and endpoint. A missing retained capability refuses without a receipt. Source and verifier roles construct their own required routes; sentio-node does not acquire a fictitious verifier constructor.

The design adds no automatic supervisor, sidecar or general artifact-attestation system. Profile-file presence is availability metadata rather than admission authority. Current admission still uses the committed active pair and drained transition; historical replay still uses the authenticated pair committed for the original operation.

## 6. Required evidence

A4 adds independent executable and FFI mutation tests, tuple ordering/uniqueness/platform tests, canonical witness vectors, sealed-file mutation/load-race tests, native and gRPC own-build mismatch tests, and real measured native/gRPC equality. Its [A4.4/A4.5/A4.6 stages](../plans/2026-09-16-signed-insert-select-contracts.md#public-a4-serial-substages) also prove the two versioned Prepare operations, exact `cast_keep_nullable='0'`, the bounded `rand`/`rand32`/`rand64` materialization surface and all refusals above on final measured artifacts. It verifies the gRPC semantic dependency closure before qualification.

A5 tests locally supported-profile filtering, exact-Q constructor and probe failures, unchanged ordinary constructor behavior, and idempotent/concurrent `Close` behavior.

B5 tests authenticated original-policy validation before dispatch, unknown and missing-route refusal without calling an executor, backend or endpoint substitution refusal, and historical routing to the retained backend after current activation changes.

D3 tests restart reconstruction of the per-pair map, missing retained endpoint or artifact, a current native host alongside retained old gRPC routing, and distinct source/verifier role wiring.

D4 records exact final test and production role-binary/profile manifests, proves native/gRPC parity with the unchanged measured binaries, proves retained historical execution across upgrade and rollback, and separately proves that an old Q cannot obtain new admission. These additions retain all existing A1–A14 obligations and do not mark any runtime step complete.
