# Signed INSERT ... SELECT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add independently verifiable signed `INSERT ... SELECT` over one authenticated safe snapshot, including both placements of `WITH`, without changing the existing v2 payload lane.

**Architecture:** The agent signs a versioned query input after the Arbiter grants a fenced snapshot reservation. Source and verifier restore the same authenticated relations, execute the same restricted SQL profile, sort canonical output rows, and use Housegate's shared commitment implementation. A durable network/shard barrier, explicit abort outcome, and committed profile activation make sequencing and recovery part of the protocol.

**Tech Stack:** Go, protobuf/gRPC, ClickHouse native TCP, rewriter-go's polyglot AST, rewriter-grpc's ClickHouse C++ AST, Raft FSM, Bazel 9.1.0/Bzlmod, Docker integration tests.

**Specs:** [Signed INSERT ... SELECT over an authenticated safe snapshot](../specs/2026-09-16-signed-insert-select-design.md), [authenticated schema semantics](../specs/2026-09-16-signed-insert-select-schema-semantics-design.md), and the [artifact lifecycle](../specs/2026-09-17-signed-insert-select-artifact-lifecycle-design.md).

## Global Constraints

- This is an unexecuted implementation plan for Tier 3 of [issue #153](https://github.com/housegate/housegate/issues/153); merging these documents enables no runtime capability and authorizes no deployment.
- The implementation baseline in the spec is Housegate `2e6633c`, including #152; refresh each repository's exact base before starting its implementation worktree and record any intervening contract changes.
- Use `housegate-statement-v3` and input discriminator `snapshot_query`; preserve `housegate-statement-v2`, `clickhouse-native-data-v1`, and INSERT's existing numeric value `1`.
- New commitment domains are `snapshot-query-read-set-v1`, `snapshot-query-input-v1`, `snapshot-query-output-v1`, `snapshot-query-statement-root-v1`, `snapshot-query-receipt-v1`, and `executor-profile-transition-v1`.
- This plan additionally freezes `snapshot-query-profile-v1`, `snapshot-query-abort-v1`, `snapshot-query-artifact-ready-v1` and `snapshot-query-claim-v1` for the concrete profile/control/claim records below; these are new domains, not changes to old hashes.
- Use `replay.CanonicalDigest`; preserve `safe-snapshot-data-v2`, the existing state-root formula, `housegate-row-id-v1`, and all old JWS/L3/receipt vectors byte for byte.
- Empty collections in the new canonical records serialize as `[]`, never `null` or omitted. Canonical records contain no storage-location hints.
- All source tables and the target belong to the same authenticated snapshot, network, shard and schema. Restore all active parts of every read table; retain the complete table ledger when building post-state.
- Acquire drains earlier SI work through published safe or committed abort, not ACK2. New submissions must match the reservation and committed active `(executor_profile_id, query_profile_id)` pair exactly.
- Historical executable profiles grant replay capability only. New admissions cannot choose an older installed profile, even with a valid user signature.
- Analyzer build identity, the strict external profile witness and executable historical routing follow the [normative build-identity addendum](../specs/2026-09-16-signed-insert-select-build-identity-design.md). Profile JSON alone never proves local executable support.
- Column eligibility, exact-S schema certification, two-layer object digests and automatic trusted issuance follow the [normative schema-semantics addendum](../specs/2026-09-16-signed-insert-select-schema-semantics-design.md). Legacy name/type hashes and live metadata alone never authorize this lane.
- Complete HGPART v1 preservation, finite archive/capacity limits and candidate/use/obligation/retirement control follow the [normative artifact-lifecycle addendum](../specs/2026-09-17-signed-insert-select-artifact-lifecycle-design.md). Keep the disposition capability disabled by default; preserve all old wire/canonical bytes and never apply `R` eligibility or the 1 GiB selected-read restore bound to whole `S`.
- The addendum's finite disposition profile is mandatory when enabled: exact limits and complete-read bounds, immutable manifest/audit reconstruction, shared 768 MiB lifecycle + 240 MiB non-lifecycle + 1,008 MiB JSON ledger, 64 MiB SpentIDs tail, private validation ordinal 8, prepaid Q/P/H completion graph and bounded rejection/non-admission distinction. Preserve two RPCs, tag 28, eleven actions, two domains, one administrator purpose and every old v2 byte. Logical credits do not provide lifecycle/physical authority or guaranteed disk; retain existing Raft stores, pause fresh admission under pressure and keep protected completion scheduling/recovery.
- B1 publication uses the addendum's fixed-scope AC-local control injected only into the trusted AR worker. Source/verifier construction is explicitly read-only and carries no publisher or schema-authority publication secret; published-safe reads and retention remain available.
- Source execution starts after durable singleton sequencing. Exact touched-partition capacity is reserved before unsafe writes. Accepted work survives client disconnects and reservation timers.
- Query-only execution creates no `DeferredInsertPlan`, emits no sample block, and does not wait for a row terminator. An empty external-table marker is protocol input to drain, not an execution trigger.
- Agent `OnQuery` installs an asynchronous preparation plan and returns promptly; the sole client codec reader must observe Cancel/EOF while acquire is drain-blocked and while finalization runs. Only the relay's serialized generation/forward gate may transfer a completed preparation to one upstream Query. A late grant or worker cannot forward a canceled generation; suspended query-auth hooks resume in order exactly once.
- Agent preparation and host `QueryOnlyPlan` have distinct ownership and are mutually exclusive with each other and all payload/local-abort execution plans. Host Submit uses the same cancellation generation through the actual submit-launch gate. Before the forwarding/Submit gate wins, cancellation forbids those side effects and reconciles only proven-unconsumed request/grant state. After a gate win, persist separate ForwardAuthorized/SubmitAuthorized outside the reader/lock before I/O; pre-gate intent cannot authorize recovery replay, and authorization persistence failure/unknown means no launch. Recovered intent without durable authorization only looks up/fences unconsumed work, even after NotFound; accepted/consumed proof retains durable recovery. Authorized retries preserve the exact signed identity, never re-sign/rebase, and later Cancel after the gate win stops delivery only.
- Acquisition recovery uses authenticated request-identity status for draining/granted/consumed/released records, including lost responses and durable cancellation tombstones. Neither a canceled RPC nor one negative lookup proves an in-flight acquire cannot commit. Released identities cannot be silently reacquired; only C3's terminal authority can resolve consumed reservations.
- Query Submit/status recovery additionally binds the exact original compact JWS: `GetSnapshotQueryStatusRequest.expected_user_jws_hash` is required at tag 6 and equals `replay.DigestString` of its unmodified UTF-8 bytes. Input root excludes JWS; a different valid signature with the same input must produce identity conflict, never absence or another signature's accepted result. This does not change v2 or reservation-control status.
- Fresh authority reads and source/publisher/admin credential binding are new C1/C3/D3 implementation prerequisites. New query reads authenticate the serving endpoint and cross the leader `raftnode.ConsensusNode.Barrier(timeout)`; role identities derive from verified credentials and configured authorization, with distinct new-lane authority purposes. Missing credentials, committed evidence or freshness refuses new capability; existing direct FSM reads, self-declared node IDs and old promotion signatures supply none of these guarantees. Legacy behavior remains unchanged while the new capability is disabled.
- `ResolveColumnProfile`, `lthash.EncodeRow`, `payloadexec.RowID`, `PartitionIDForRow`, and `RowElementHash` remain the shared authorities. No Nullable, UUID or Decimal support, floating arithmetic, inline VALUES, or arbitrary SQL is added.
- SQL parsing/transformation stays in rewriter backends. Neither agent nor ingress gets a SELECT regex parser; peer/trust markers do not waive the new executing-host validation.
- Every runtime gate defaults off until the release gate passes. Keep readers/executors and authenticated artifacts for accepted historical operations during rollback.
- Housegate owns canonical replay code; arbiter-core imports it. Housegate must not import arbiter-core or sentio-node. Protobuf mirror changes run arbiter-core's field-name and semantic conformance checks.
- Bazel is Housegate's test authority. Explicitly register Docker/manual integration targets in CI; local macOS commands do not use the Linux-only `--config=ci`.
- Authoritative documentation is English, with one paragraph per line. Use conventional commit subjects and explicitly stage the listed files. Preserve unrelated work and use an isolated worktree per implementation repository.

---

## Plan boundaries and execution order

The design spans independently reviewable libraries, storage/execution, coordination, and transport. Execute these four component plans in dependency order; each task ends in a testable, disabled-by-default deliverable. Read this index and the spec before the component plan. All checkboxes describe future work.

| Plan | Deliverable | Dependencies | Tasks |
|---|---|---|---|
| [A: Contracts and SQL analysis](2026-09-16-signed-insert-select-contracts.md) | Versioned wire/signature vectors and matching native/gRPC analysis; read-only Housegate wrapper | Existing v2 contracts | A1–A5 |
| [B: Snapshot restore and replay](2026-09-16-signed-insert-select-replay.md) | Authenticated artifact restore, bounded canonical output, shared state assembly and signed query receipts | A1–A5; storage ports can be tested before coordination exists | B1–B5 |
| [C: Coordination and recovery](2026-09-16-signed-insert-select-coordination.md) | Profile transition, barrier/reservation/abort FSM, sequence-first journal and source lifecycle | A1–A2; B contracts for execution and promotion | C1–C5 |
| [D: Native transport and activation evidence](2026-09-16-signed-insert-select-integration.md) | Agent/server integration, embedded adapters, adversarial end-to-end gate and operator runbook | A, B and C | D1–D4 |

Land shared contracts before consumers and cut real dependency releases only after their own checks pass. B and C may progress independently against the frozen A records; C5 requires B5, and D requires both. Do not combine every repository into one unreviewable implementation PR. Each task names the repository that owns its commit; coordinated mirror changes need a linked PR in each owner, with dependent PRs pinned to immutable commits/releases.

## Repository and file ownership

Paths in the component plans are repository-relative and prefixed by the aliases below. Existing paths were inspected while preparing the plan; files marked **Create** are proposed. Use the repository's own AGENTS/CLAUDE instructions and an isolated worktree when executing.

| Alias | Repository | Responsibilities and principal files |
|---|---|---|
| HG | `housegate/housegate` | `pkg/replay`, `pkg/replay/payloadexec`, `pkg/replay/chexec`, proposed `pkg/replay/snapshotquery`; `pkg/auth`; `pkg/rewriter`; `pkg/storageintegrity`; agent/ingress plugins; `pkg/proxy/relay.go`; `build.go` |
| AP | `sentioxyz/arbiter-proto` | Protobuf statement, replay, reservation, profile, source, verifier and artifact-disposition RPC records; generated bindings |
| RP | `housegate/rewriter-proto` | AST analysis/materialization/prepare contract and capability acknowledgement |
| RG | `housegate/rewriter-go` | Native AST analysis, restricted profile and exact logical-to-scratch relation binding |
| RC | `housegate/rewriter` (local checkout `rewriter-grpc`) | gRPC implementation with the same corpus and rejection semantics |
| AC | `sentioxyz/arbiter-core` | Typed complete-tree artifact publication, restoration and retention; protected local registry; disposition client/conversion; local publication-control port/read-only mode; AC-private fixed-storage checksum ZSTD leaf; source candidates, verifier and wire adapters |
| AR | `sentioxyz/arbiter` | Deterministic admission, shared whole-FSM capacity ledger, exact manifest/audit interning, prepaid allowance bindings, barrier/reservation log, singleton block, abort, activation, candidate/use/obligation/retirement disposition, trusted automatic schema issuance/publication, concrete publication-control adapter and restart |
| SN | `sentioxyz/sentio-node` | `storageintegrityadapter`, embedded Housegate/core wiring and dependency pins |
| PD | `sentioxyz/production` | Deployment manifests and durable artifact/profile wiring, after separate rollout authorization |

The new replay package owns query-input validation, profile lookup, restore/evaluation ports, canonical sorting and query orchestration. Shared append/state helpers stay beside the existing payload executor. Artifact bytes and process/network integration stay in AC. Control-plane state transitions stay in AR. Keeping these responsibilities separate avoids putting another execution mode into the payload-only `Materializer.Materialize(ctx, schema, statement)` seam.

## Review and verification rules

Each task's first test must fail against the implementation without that task; record the failure and passing rerun. Missing new symbols are an acceptable first failure for a new API, but completion also requires the task's behavioral assertions. Do not replace historical vectors by regenerating expected outputs from the new implementation. Generate new golden records once, inspect their canonical bytes independently, commit them, and run consumers against the frozen bytes.

Commands assume the executor is in the owning repository's isolated worktree. Each plan's commands are repository scripts or shell-neutral commands; no copied command contains credentials. Generated-code changes include their build metadata in the same task. Run a targeted suite during development, then the owning repository's required checks once per candidate commit; broaden testing only for changed behavior or a new failure. Baseline-match any unrelated environmental failure before attributing it to this work.

Protocol-changing tasks must retain binary and JSON fixtures for old readers, new readers and unknown variants. Every active-policy change is tested separately from historical profile dispatch. A test that proves “the new profile is installed” does not establish that a new admission is authorized.

## Acceptance ownership

The IDs below map to the spec's full acceptance matrix, which remains the acceptance authority. D4 assembles evidence across repositories; a unit test alone does not satisfy a real transport, storage or recovery scenario.

| Spec gate | Implementing tasks | Required cross-component gate |
|---|---|---|
| A1 grammar/CTEs/joins/clients | A3–A5, D1–D2 | D4 native and gRPC, official CLI and Go driver |
| A2 hidden sources and escape refusal | A4, B2, D2 | D4 forged descriptors and isolated executor |
| A3 signatures/pins/genesis/profile downgrade | A1–A2, B1, C1–C3, D3 | D4 signed historical-profile rejection, exact original-JWS status identity, schema certificate/digest/unique-S/candidate association, unauthorized/wrong-role/spoofed identity and fixed-endpoint/nonce/full-identity/fresh-read proof failures |
| A4 exact S and serialized self-insert | B2, C2–C5 | D4 two sequential self-inserts with unsafe interference |
| A5 complete authentic restore | B1–B2 | D4 complete-tree/auxiliary/projection preservation, checksum qualification, cold/warm corruption, schema-object substitution and availability cases |
| A6 canonical order and duplicate multiplicity | B3–B5 | D4 separate ClickHouse instances and at least `2^16` duplicates |
| A7 closed SQL/materialization/type failures | A3–A5, B3–B4 | D4 nested operators, column-generation refusal, pool exhaustion and overflow |
| A8 whole ledger and empty applied output | B4–B5, C3 | D4 read/write/unrelated tables and zero-row advancement |
| A9 source fraud and candidate bytes | B5, C5 | D4 independent replay plus delta plus exact byte scan |
| A10 native packet lifecycle | D1–D2, C4 | D4 fragmentation/coalescing, marker, drain-blocked acquire Cancel/EOF, late worker/grant, finalization/forward/Submit races, one-shot auth hooks and next-query races; no upstream Query/no Submit when cancellation wins |
| A11 crash/lost-response recovery | B1, C1–C5, D1–D3 | D4 fault injection at every durable boundary, including prepaid ordinal allocation/binding/transfer, issuer O fsync/candidate/readiness/publication, late old candidate futures, first-Admit/absent-Close races, lost acquire/release responses, four agent/host launch-authorization crash windows and restart with cancel/use/tombstone/capacity state; unknown outcomes remain protected |
| A12 failure/abort/liveness/retention | C3–C5 | D4 committed no-op, late-generation refusal, exact obligation/debt settlement and next-block progress; no absence/AckCleanup/boolean proof |
| A13 versions/migration/activation/rollback | A1–A2, B1, C1–C3, D3 | D4 legacy vectors, capability-off and old-unscoped guards, schema-authority rotation, disposition snapshot migration, restart and historical replay |
| A14 limits/performance | B1–B3, C1–C5, D3 | D4 exact max/+1 disposition encodings, complete lists, Q/P/H/rejection/whole-FSM/tail exhaustion, real 34/36/40 MiB transport and 1,008 MiB snapshot qualification, plus measured archive/checksum fixed-storage ZSTD, capacity/cancellation, restore/sort/spill/latency/barrier occupancy and existing-store pressure/failure recovery |

## Completion boundary

The implementation is ready for a separately authorized deployment only when A1–A14 have evidence against one immutable release manifest containing all binary/image/profile digests. D4 produces that manifest and a runbook; it does not activate a production network. Deployment first installs compatible readers/executors and artifact retention, then canaries on a disposable network. Only a later committed profile transition and active-pair selection can authorize new reservations.

An implementation report must distinguish code/PR completion, CI completion, disposable-network acceptance and production activation. It must name any unfinished gate directly. Inline VALUES, concurrent snapshot-query scheduling, subset read witnesses and extra SQL/type support remain separate projects.
