# HouseGate Storage Integrity P1e Runtime Wiring And E2E Closure

Date: 2026-07-20

Last hardened: 2026-07-28

## Purpose

This change adds the P1e runtime shell that connects the signed-ingress admission plugin to the staged-intake orchestrator: a root-package `StorageIntegrityIngress` that implements the plugin's `AdmissionConsumer` by mapping a completed `Admission` into a core `AdmissionRecord` and driving `Orchestrate` toward ACK2. Alongside it, this slice adds two runtime building blocks that are pure HouseGate-local logic: a `MergeGuard` that re-asserts and verifies `SYSTEM STOP MERGES` on the guarded tables, and a `SelectMaterializerKind` function that chooses the replay materializer (Native vs CSV) from the pinned payload encoding.

A follow-up on the same branch adds the production wiring boundary for this runtime. `storage_integrity.runtime.enabled` asks HouseGate to build the `AdmissionConsumer` itself from host-injected runtime ports instead of requiring the host to provide a fully constructed consumer. The injected ports are the Arbiter submitter (or an `ArbiterIngressClient` that HouseGate adapts), the selected-SNode `SourcePreparer`, the PayloadStore writer (or a `PayloadStoreClient` that HouseGate adapts), an optional status querier, and a ClickHouse `MergeConn` or prebuilt startup merge guard. HouseGate now builds the local durable intake journal, payload spool wrapper, and config-driven merge guard from YAML by default. This keeps protocol ownership honest: HouseGate can wire real local runtime pieces and arbiter-proto clients, but it still does not invent a fake SNode API.

The runtime never constructs a HouseGate-owned Verifier or Promoter: verifier selection, quorum, and manifest publication are Arbiter/SNode responsibilities the orchestrator only drives through ports (design sections 3.6 and 4.1). Storage integrity stays default-off; with the ingress disabled the plugin chain is byte-identical to a non-storage-integrity build.

## Companion Gate Status

This design slice remains C1-gated, on the same basis as the earlier INSERT intake slices. It reuses the existing C1 gate — `CompanionStagedIntakeAvailable` (`pkg/storageintegrity/intake.go`) and `requireCompanionStagedIntake` — and introduces no new protocol gate: this is the INSERT P1e runtime, whose blocker is C1 (the staged-prepare seam), not C2. The companion repos expose no `PrepareLocalStatement` / `RegisterPreparedClaim` / `AbortPreparedStatement` seam (arbiter `9e7c643`, arbiter-proto `3b1b2d3`, re-verified 2026-07-28), so no real `SourcePreparer` / `IntakeStatusQuerier` adapters exist and the ingress consumer cannot actually close a statement to ACK2 in production. HouseGate now has real arbiter-proto adapters for Arbiter `SubmitStatement` and PayloadStore `Put`, plus HouseGate-owned durable journal/spool primitives, but those are not sufficient without the selected-SNode staged seam.

The production runtime builder checks this capability gate directly. An injected interface with the same Go method shape is not proof that the companion protocol exists: while `CompanionStagedIntakeAvailable` is false, `storage_integrity.runtime.enabled=true` fails during build and no listener starts. Pure HouseGate components remain directly constructible for unit and contract tests, but production wiring cannot bypass the missing protocol with a local mock.

Because HouseGate must not fabricate the companion protocol, this slice ships:

1. this scoped spec;
2. the pure HouseGate-local runtime pieces — the ingress adapter and its `Admission` → `AdmissionRecord` projection, durable intake journal, durable payload spool wrapper, config-driven `MergeGuard`, the materializer-selection function, runtime port assembly, and the `safe_merges` config with default-off validation;
3. contract tests. The durable journal resume path, payload spool retention, merge-guard SQL build/verify, materializer selection, admission projection, and config default-off behavior are pure HouseGate logic and run green today. Runtime assembly is tested below the production capability gate; the production builder test proves that enabling the runtime remains rejected while the companion seam is absent. The end-to-end ingress-to-ACK2 orchestration assertion is gated behind `requireCompanionStagedIntake` and skips closed while the companion seam is absent.

When the companion seam lands, the real selected-SNode adapter is injected through `StorageIntegrityRuntimeOptions.SourcePreparer`, the payload encoding/revision contract and selected-source semantics are verified across the same deployed revisions, `CompanionStagedIntakeAvailable` flips to true, and the gated test becomes the executable spec for the real close-to-ACK2 path. No local mock shape is added in the meantime.

## Design Anchors

This contract implements the P1e runtime responsibilities of section 3 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- Section 3.2 server-side ingress timing: a completed admission is driven through the staged prepare/submit/RC path to ACK2.
- The explicit Native-vs-CSV materializer selection: the runtime picks the decoder from the pinned payload encoding rather than declaring support and guessing.
- The `STOP MERGES` requirement: the integrity layer owns the active part inventory, so background merges are stopped at startup, periodically re-asserted, and connected to an admission health latch that fails closed on drift.
- Section 3.6 local-optimization boundary: HouseGate may add local runtime helpers but must not construct a Verifier/Promoter or modify the Arbiter proto / Raft commands / three-way predicate / manifest canonicalization.

## The Runtime Shell

`StorageIntegrityIngress` holds only `{orch, guard, matKind, payloadWriter}` — an orchestrator, an optional merge guard, the selected materializer kind, and the PayloadStore writer wrapper. Its `ConsumeStorageIntegrityAdmission` maps the plugin `Admission` into a core `AdmissionRecord` via the pure `AdmissionRecordFromPlugin` projection, spools and uploads payload bytes when a writer is configured, then drives `Orchestrate`; a non-ACK2 outcome is surfaced as an error so the plugin reports failure to the client rather than a false success, and only a bound ACK2 returns nil. Because the struct has no Verifier/Promoter field and no constructor for one, verifier selection / quorum / manifest publication cannot leak into the HouseGate runtime.

When `storage_integrity.runtime.enabled=true`, server-mode `buildServer` constructs this consumer from `StorageIntegrityRuntimeOptions` and `storage_integrity.runtime`. Hosts may pass already-built core ports (`StatementSubmitter`, `PayloadWriter`) or arbiter-proto clients (`ArbiterIngressClient`, `PayloadStoreClient`) that HouseGate adapts via `NewArbiterStatementSubmitter` and `NewArbiterPayloadStoreWriter`. HouseGate builds a `FileIntakeJournal` from `journal_dir`, wraps the payload writer with a `FilePayloadSpool` from `payload_spool_dir`, and either uses an injected `MergeGuard` or builds one from an injected `MergeConn` plus `merge_guard.tables`. `SourcePreparer` is mandatory and remains a host/companion dependency because the SNode staged prepare seam is not in arbiter-proto today. `ExpectedSource` comes from YAML and is passed into `OrchestratorConfig`, so a committed-source mismatch still fails closed inside the ACK2 path.

## Durable Journal And Payload Spool

`FileIntakeJournal` persists one JSON record per statement id using atomic write+rename and directory fsync. The durable record carries the original envelope/admission binding, highest lifecycle stage, cached prepared candidate parts, cached accepted submit outcome, unknown submit/claim flags, abort reason, terminal result, and an immutable per-source `frontier_ordinal`. A new statement reserves its ordinal under the orchestrator lock and durably writes the initial `Preparing` record before acquiring the source frontier or calling the source. Gaps are allowed; reuse or reordering is not. Recovery orders non-terminal records by `(source, frontier_ordinal)` and initializes the next ordinal from the maximum durable value. Because an older record without an ordinal cannot prove its position, recovery fails closed when such a record could compete with another non-terminal record on the same source.

A restarted orchestrator enumerates and actively resumes the journal before admitting new work. `RecoverPending(ctx)` drives each source's holder in ordinal order through prepared lookup, submit/claim convergence, or exact `AbortPending` cleanup until the record reaches `RCBound` or `Cleaned`; retryable and unknown outcomes use bounded backoff and retain the frontier. Server `preServe` does not return, and therefore listeners do not start, until this recovery drain succeeds or its context is cancelled. Recovery does not depend on the original client retrying.

A terminal transition is committed to memory only after its journal snapshot has been durably saved. A failed terminal save leaves the process-local record non-terminal, keeps the source frontier held, and forces a later attempt to persist or recompute the same transition; it may not return a terminal cache that exists only in memory. If any `PrepareLocalStatement` call returns an error that could follow a committed source write, the record immediately requires `LookupPreparedStatement` before another prepare in the same process. A restart already derives the same requirement from the durable `Preparing` record. Only an explicit `found=false` lookup permits a fresh source write; a missing lookup port or an indeterminate lookup fails closed.

`FilePayloadSpool` writes exact payload bytes and metadata by content hash before the remote PayloadStore put is attempted. `SpoolingPayloadWriter` stores locally first, calls the real remote writer second, and marks the spool record uploaded only after a successful remote result. If the remote put fails, the local bytes remain in `PENDING` state and can be inspected or reused by the next admission retry; the unsafe prepare path is not reached before this write succeeds.

An `UPLOADED` record is reusable only while its ingest lease remains outside the configured refresh window. The lease supervisor runs under the server context, reloads exact bytes from the spool, and repeats the idempotent remote put before `LeaseExpiresUnixMS` until the journal proves `SubmitAccepted` or a terminal pre-submit rejection. Recovery registers every non-terminal pre-submit journal record with the supervisor before attempting submit convergence. A refresh that returns a different non-empty `payload_ref` fails closed because the original statement envelope is already bound to the prior ref. Restart safety comes from the journal plus exact spool bytes, not from an in-memory timer.

## The Merge Guard

`MergeGuard` re-asserts and verifies that ClickHouse background merges are stopped on the guarded storage-integrity tables. It works over a narrow `MergeConn` / `MergeRows` port so it is unit-testable with an in-memory fake and the core package does not depend on the full clickhouse-go connection type; the runtime adapts a real connection to this port at the wiring boundary. `BuildStopMergesStatements` emits one table-scoped, backtick-quoted `SYSTEM STOP MERGES <db>.<table>` per guarded table (never a bare global STOP). `AssertStopMerges` issues every stop statement, then runs a `system.merges` probe scoped to exactly the guarded tables and fails closed with `ErrNativeMergesEnabled` (naming the offending table) if any guarded table still shows an active merge. It is idempotent, so it is safe to re-assert at startup and on reconnect; a failed stop exec surfaces before the verify probe runs.

With runtime wiring enabled, HouseGate calls the resolved merge guard from `preServe` before the cluster manager and metrics collector start. A guard error prevents serving from starting. After the initial assertion, a supervisor re-runs the idempotent STOP/probe operation at the configured interval under the server context. Its health latch starts closed, opens only after a successful assertion, closes on any later exec/query/active-merge failure, and opens again only after a subsequent successful assertion. `StorageIntegrityIngress` checks this latch before payload upload or orchestration, so a ClickHouse restart or reconnect cannot leave admission fail-open between background checks. This is still a host-injected connection boundary: YAML supplies the guarded table set, but the host must pass either a `MergeConn` over its ClickHouse management connection or a prebuilt `MergeGuard`.

## Materializer Selection

`SelectMaterializerKind(encoding)` is a pure, deterministic map from the pinned payload encoding to the replay materializer family: the Native encoding selects `MaterializerNative`, an empty or `csv-with-names-v1` encoding selects `MaterializerCSV`, and any other non-empty encoding fails closed. Failing closed on an unknown encoding is the safety property — a Native payload must never be silently mis-decoded as CSV.

Ingress-side CSVWithNames compatibility is a separate bridge from replay
selection. `StorageIntegrityPayloadMaterializer` converts the captured Native
`ClientData` into final `csv-with-names-v1` bytes before `AdmissionRecord` is
built. After that point the runtime sees an ordinary CSV payload and
`SelectMaterializerKind` selects `MaterializerCSV`. Without the bridge,
`FORMAT CSVWithNames` is rejected before the runtime shell is invoked.

## Config

The `storage_integrity.safe_merges.allow_native_background_merges` toggle governs the merge guard. It defaults false, and enabling it is rejected in v1 (native background merges would mutate the guarded part inventory out from under the integrity layer). The YAML ingress config (enabled/allowlist/timeouts/max-payload) stays default-off; with `ingress.enabled=false` the storage-integrity runtime is not constructed and the plugin chain equals the non-storage-integrity baseline.

`storage_integrity.runtime.enabled` is also default-off. Enabling it requires `storage_integrity.ingress.enabled=true`, non-empty `storage_integrity.runtime.expected_source`, `journal_dir`, `payload_spool_dir`, a positive payload-lease refresh interval/window, a positive merge-guard reassert interval, and at least one `merge_guard.tables[]` entry with non-empty `database` and `table`. `buildServer` then requires the runtime ports (`StatementSubmitter` or `ArbiterIngressClient`, `SourcePreparer`, `PayloadWriter` or `PayloadStoreClient`, and either `MergeGuard` or `MergeConn`) and rejects ambiguous wiring where the host also supplied `StorageIntegrityAdmissionConsumer`. Even complete local configuration remains rejected while `CompanionStagedIntakeAvailable` is false.

The embeddable `housegate.Options` surface adds
`StorageIntegrityPayloadMaterializer`. Hosts that want to accept
`FORMAT CSVWithNames` must provide this bridge because they own pinned schema
resolution. This option does not add a HouseGate-owned Verifier/Promoter and
does not change the Arbiter FSM protocol.

## Non-Scope

This change does not implement the real `SourcePreparer` / `IntakeStatusQuerier` adapters, Arbiter's block-frontier barrier, payload encoding/revision fields in arbiter-proto, selected-source reservation, or duplicate resolution before JWS freshness validation. It does not construct a Verifier/Promoter, modify Arbiter proto/Raft commands, flip `CompanionStagedIntakeAvailable`, add YAML-driven gRPC dialing, open ClickHouse connections automatically, or add a non-INSERT surface. Those missing companion capabilities keep the production runtime build gate closed; HouseGate implements only its local durability, lease, recovery, and merge-health responsibilities.

## Verification

Focused gate:

```bash
go test . ./pkg/config ./pkg/plugins/storageintegrity ./pkg/storageintegrity -count=1
go test -race ./pkg/storageintegrity -count=1
```

Bazel gate:

```bash
bazel test //pkg/storageintegrity:storageintegrity_test //pkg/plugins/storageintegrity:storageintegrity_test
```

The hardening regression gate adds: ambiguous prepare error followed by same-process lookup; terminal journal-save failure followed by retry and restart; immutable A-before-B recovery despite later A updates; startup recovery without an original client retry; payload lease refresh before expiry and restart registration from journal/spool; payload-ref mismatch rejection; periodic merge reassertion, unhealthy admission rejection, and recovery after a successful reassert; and production runtime rejection while the companion capability gate is false.

Existing pure-component and gated C1 tests remain. The full production ingress-to-ACK2 assertion stays skipped closed until deployed companion revisions satisfy the contract; unit tests may construct the HouseGate components directly but may not flip the production builder gate.
