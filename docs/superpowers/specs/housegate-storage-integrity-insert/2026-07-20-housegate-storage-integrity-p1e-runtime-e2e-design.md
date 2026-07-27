# HouseGate Storage Integrity P1e Runtime Wiring And E2E Closure

Date: 2026-07-20

## Purpose

This change adds the P1e runtime shell that connects the signed-ingress admission plugin to the staged-intake orchestrator: a root-package `StorageIntegrityIngress` that implements the plugin's `AdmissionConsumer` by mapping a completed `Admission` into a core `AdmissionRecord` and driving `Orchestrate` toward ACK2. Alongside it, this slice adds two runtime building blocks that are pure HouseGate-local logic: a `MergeGuard` that re-asserts and verifies `SYSTEM STOP MERGES` on the guarded tables, and a `SelectMaterializerKind` function that chooses the replay materializer (Native vs CSV) from the pinned payload encoding.

A follow-up on the same branch adds the first production wiring boundary for this runtime. `storage_integrity.runtime.enabled` asks HouseGate to build the `AdmissionConsumer` itself from host-injected runtime ports instead of requiring the host to provide a fully constructed consumer. The injected ports are the Arbiter submitter (or an `ArbiterIngressClient` that HouseGate adapts), the selected-SNode `SourcePreparer`, the PayloadStore writer (or a `PayloadStoreClient` that HouseGate adapts), an optional status querier, and a startup merge guard. This keeps protocol ownership honest: HouseGate can wire real clients once the companion seam exists, but it still does not invent a fake SNode API.

The runtime never constructs a HouseGate-owned Verifier or Promoter: verifier selection, quorum, and manifest publication are Arbiter/SNode responsibilities the orchestrator only drives through ports (design sections 3.6 and 4.1). Storage integrity stays default-off; with the ingress disabled the plugin chain is byte-identical to a non-storage-integrity build.

## Companion Gate Status

This design slice is a blocked skeleton, on the same basis as the earlier INSERT intake slices. It reuses the existing C1 gate — `CompanionStagedIntakeAvailable` (`pkg/storageintegrity/intake.go`) and `requireCompanionStagedIntake` — and introduces no new gate: this is the INSERT P1e runtime, whose blocker is C1 (the staged-prepare seam), not C2. The companion repos expose no `PrepareLocalStatement` / `RegisterPreparedClaim` / `AbortPreparedStatement` seam (arbiter `03aa035`, arbiter-proto `2fa9263`, re-verified 2026-07-20), so no real `SourcePreparer` / `IntakeStatusQuerier` adapters exist and the ingress consumer cannot actually close a statement to ACK2 in production. HouseGate now has real arbiter-proto adapters for Arbiter `SubmitStatement` and PayloadStore `Put`, but those are not sufficient without the selected-SNode staged seam and durable intake journal.

Because HouseGate must not fabricate the companion protocol, this slice ships:

1. this scoped spec;
2. the pure HouseGate-local runtime pieces — the ingress adapter and its `Admission` → `AdmissionRecord` projection, the `MergeGuard`, the materializer-selection function, runtime port assembly, and the `safe_merges` config with default-off validation;
3. contract tests. The merge-guard SQL build/verify, the materializer selection, the admission projection, the config default-off, and the build-time default-off wiring are pure HouseGate logic and run green today. The single end-to-end ingress-to-ACK2 orchestration assertion is gated behind `requireCompanionStagedIntake` and skips closed while the companion seam is absent.

When the companion seam lands, the real selected-SNode adapter is injected through `StorageIntegrityRuntimeOptions.SourcePreparer`, `CompanionStagedIntakeAvailable` flips to true, and the gated test becomes the executable spec for the real close-to-ACK2 path. No local mock shape is added in the meantime.

## Design Anchors

This contract implements the P1e runtime responsibilities of section 3 of the unified storage-integrity design (`2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md`):

- Section 3.2 server-side ingress timing: a completed admission is driven through the staged prepare/submit/RC path to ACK2.
- The explicit Native-vs-CSV materializer selection: the runtime picks the decoder from the pinned payload encoding rather than declaring support and guessing.
- The `STOP MERGES` requirement: the integrity layer owns the active part inventory, so background merges are stopped and re-asserted at startup, failing closed on drift.
- Section 3.6 local-optimization boundary: HouseGate may add local runtime helpers but must not construct a Verifier/Promoter or modify the Arbiter proto / Raft commands / three-way predicate / manifest canonicalization.

## The Runtime Shell

`StorageIntegrityIngress` holds only `{orch, guard, matKind}` — an orchestrator, an optional merge guard, and the selected materializer kind. Its `ConsumeStorageIntegrityAdmission` maps the plugin `Admission` into a core `AdmissionRecord` via the pure `AdmissionRecordFromPlugin` projection and drives `Orchestrate`; a non-ACK2 outcome is surfaced as an error so the plugin reports failure to the client rather than a false success, and only a bound ACK2 returns nil. Because the struct has no Verifier/Promoter field and no constructor for one, verifier selection / quorum / manifest publication cannot leak into the HouseGate runtime.

When `storage_integrity.runtime.enabled=true`, server-mode `buildServer` constructs this consumer from `StorageIntegrityRuntimeOptions`. Hosts may pass already-built core ports (`StatementSubmitter`, `PayloadWriter`) or arbiter-proto clients (`ArbiterIngressClient`, `PayloadStoreClient`) that HouseGate adapts via `NewArbiterStatementSubmitter` and `NewArbiterPayloadStoreWriter`. `SourcePreparer` is mandatory and remains a host/companion dependency because the SNode staged prepare seam is not in arbiter-proto today. `ExpectedSource` comes from YAML and is passed into `OrchestratorConfig`, so a committed-source mismatch still fails closed inside the ACK2 path.

## The Merge Guard

`MergeGuard` re-asserts and verifies that ClickHouse background merges are stopped on the guarded storage-integrity tables. It works over a narrow `MergeConn` / `MergeRows` port so it is unit-testable with an in-memory fake and the core package does not depend on the full clickhouse-go connection type; the runtime adapts a real connection to this port at the wiring boundary. `BuildStopMergesStatements` emits one table-scoped, backtick-quoted `SYSTEM STOP MERGES <db>.<table>` per guarded table (never a bare global STOP). `AssertStopMerges` issues every stop statement, then runs a `system.merges` probe scoped to exactly the guarded tables and fails closed with `ErrNativeMergesEnabled` (naming the offending table) if any guarded table still shows an active merge. It is idempotent, so it is safe to re-assert at startup and on reconnect; a failed stop exec surfaces before the verify probe runs.

With runtime wiring enabled, HouseGate calls the injected merge guard from `preServe` before the cluster manager and metrics collector start. A guard error prevents serving from starting. This is still a host-injected connection boundary: this slice does not make YAML alone open a ClickHouse connection for the guard.

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

`storage_integrity.runtime.enabled` is also default-off. Enabling it requires `storage_integrity.ingress.enabled=true` and a non-empty `storage_integrity.runtime.expected_source`. `buildServer` then requires the runtime ports (`StatementSubmitter` or `ArbiterIngressClient`, `SourcePreparer`, `PayloadWriter` or `PayloadStoreClient`, and `MergeGuard`) and rejects ambiguous wiring where the host also supplied `StorageIntegrityAdmissionConsumer`.

The embeddable `housegate.Options` surface adds
`StorageIntegrityPayloadMaterializer`. Hosts that want to accept
`FORMAT CSVWithNames` must provide this bridge because they own pinned schema
resolution. This option does not add a HouseGate-owned Verifier/Promoter and
does not change the Arbiter FSM protocol.

## Non-Scope

This change does not implement the real `SourcePreparer` / `IntakeStatusQuerier` adapters (they need the companion staged-prepare seam), the durable intake journal, durable payload spool / lease refresh loop, cross-restart crash recovery, leader failover retry policy beyond the supplied ports, or YAML-driven gRPC dialing. It does not construct a Verifier/Promoter, does not touch the Arbiter proto / Raft commands, does not flip `CompanionStagedIntakeAvailable`, and adds no non-INSERT surface. The crash/retry/frontier/leader-failover E2E behaviors named in the runtime goal are C1-gated: they depend on the durable journal and real adapters that arrive with C1, and are not asserted against fakes here.

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

Green today: `TestSelectMaterializerKind` (encoding → kind, fail-closed on unknown); `TestNativeCSVPayloadMaterializerConvertsCapturedClientData` (captured Native `ClientData` → decodable CSVWithNames); `TestIngressMaterializesCSVWithNamesAdmissionFromCapturedNativeData` and `TestBuildServer_StorageIntegrityIngressWiresCSVPayloadMaterializer` (bridge wired into admission and server assembly); the `TestMergeGuard_*` suite (build stop statements for both hg_safe and hg_unsafe, scoped verify probe, emit-then-verify ordering, fail-closed on active merge, idempotent re-assert, exec-error surfaces); `TestAdmissionRecordFromPlugin_MapsAllFields` and `_MapsKinds` (pure projection); `TestNewStorageIntegrityIngress_RequiresOrchestrator` (nil rejected, no Verifier/Promoter fields); the config default-off and safe-merges-rejection tests; `TestBuildServer_StorageIntegrityRuntimeRequiresPorts`, `TestBuildServer_StorageIntegrityRuntimeBuildsConsumerAndRunsMergeGuard`, and `TestBuildServer_StorageIntegrityRuntimePreServeFailsClosedOnMergeGuardError` (runtime port assembly and startup fail-closed wiring). Gated: `TestIngressDrivesOrchestratorToAck2` (an INSERT admission driven to ACK2/RCBound) skips closed under `requireCompanionStagedIntake` and passes only when `CompanionStagedIntakeAvailable` is temporarily flipped true for wiring verification. The suite is race-clean.
