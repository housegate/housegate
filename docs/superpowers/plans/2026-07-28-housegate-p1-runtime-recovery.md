# HouseGate P1 Runtime Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Make HouseGate's local P1 runtime crash-safe and self-recovering while keeping production disabled until the deployed Arbiter/Proto companion contract exists.

**Status:** Implemented on `fix/housegate-p1-runtime-recovery` on 2026-07-28.
The static companion gate recorded below was superseded on 2026-07-29 by
dynamic `SourcePreparer`/`PreparedStatementLookup`/status-query validation in
the P1e runtime design.

**Architecture:** Harden the existing orchestrator with durable, immutable source ordering and journal-first terminal transitions. Add two focused supervisors: a core payload-lease manager that redrives content-addressed puts from spool bytes, and a root runtime merge-health supervisor that continuously reasserts STOP MERGES and gates admission. Startup synchronously drains durable intake recovery before listeners start.

**Tech Stack:** Go 1.24, standard-library concurrency and atomic file persistence, existing HouseGate storage-integrity ports, table-driven Go tests.

## Global Constraints

- Do not modify `arbiter` or `arbiter-proto`.
- Historical note: the implementation originally retained
  `CompanionStagedIntakeAvailable`; the 2026-07-29 production wiring removed it
  in favor of dynamic capability validation.
- Never repeat `PrepareLocalStatement` after an ambiguous result without a positive source-side `found=false` lookup.
- Never release a source frontier before durable `RCBound` or `Cleaned`.
- Payload lease refresh must preserve the original non-empty `payload_ref`.
- Merge-guard failure must reject admission until a later successful assertion.
- Every production behavior change follows RED-GREEN-REFACTOR.

---

### Task 1: Journal-First Transitions And Immutable Frontier Order

**Files:**
- Modify: `pkg/storageintegrity/intake.go`
- Modify: `pkg/storageintegrity/journal.go`
- Test: `pkg/storageintegrity/intake_journal_test.go`
- Test: `pkg/storageintegrity/journal_test.go`

**Interfaces:**
- Produces: `IntakeJournalRecord.FrontierOrdinal uint64`
- Produces: `intakeRecord.frontierOrdinal uint64`
- Produces: `Orchestrator.nextFrontierOrdinal map[string]uint64`
- Preserves: `IntakeJournal` method set

- [x] **Step 1: Add a failing ambiguous-prepare regression**

Add `TestOrchestrate_AmbiguousPrepareErrorRequiresLookupBeforeRetry`. The fake preparer records a durable prepared result, returns a transport error on the first prepare, implements `LookupPreparedStatement`, and succeeds on lookup. Assert two calls reach ACK2 with exactly one prepare and one lookup.

```go
if got := atomic.LoadInt64(&prep.prepareCount); got != 1 {
    t.Fatalf("prepare count = %d, want 1", got)
}
if got := atomic.LoadInt64(&prep.lookupCount); got != 1 {
    t.Fatalf("lookup count = %d, want 1", got)
}
```

- [x] **Step 2: Run the test and verify RED**

Run:

```bash
go test ./pkg/storageintegrity -run TestOrchestrate_AmbiguousPrepareErrorRequiresLookupBeforeRetry -count=1
```

Expected: FAIL because the second attempt calls prepare again.

- [x] **Step 3: Implement same-process lookup fencing**

When prepare returns an error, set `rec.requirePreparedLookup = true` under `o.mu` before returning. Do not clear it until `LookupPreparedStatement` returns `found=false` or `cachePrepared` durably saves the found result.

- [x] **Step 4: Add terminal-save failure regressions**

Add `TestOrchestrate_TerminalSaveFailureDoesNotPublishMemoryTerminal` and `TestOrchestrate_TerminalSaveFailureRestartsFromDurableStage`. Fail the first `RCBound` save. Assert the next same-process call retries persistence instead of returning the cached ACK2, releases the frontier only after save succeeds, and a fresh orchestrator resumes from the last durable non-terminal stage.

- [x] **Step 5: Run the terminal tests and verify RED**

```bash
go test ./pkg/storageintegrity -run 'TestOrchestrate_TerminalSaveFailure' -count=1
```

Expected: FAIL because `setTerminal` mutates `isTerminal` before the journal save.

- [x] **Step 6: Make terminal transitions journal-first**

Build the terminal snapshot from a cloned record, save it, then copy `stage`, `isTerminal`, and `terminalRes` into the live record under `o.mu`. Keep non-terminal stage helpers unchanged unless their callers also publish externally visible terminal state.

- [x] **Step 7: Add immutable-order tests**

Add `TestFileIntakeJournalOrdersNonTerminalRecordsByFrontierOrdinal` and `TestOrchestratorRecoveryKeepsABeforeBWhenAUpdatedLater`. Persist A with ordinal 1, B with ordinal 2, then update A after B. Assert recovery still installs A as holder.

- [x] **Step 8: Run the order tests and verify RED**

```bash
go test ./pkg/storageintegrity -run 'TestFileIntakeJournalOrdersNonTerminalRecordsByFrontierOrdinal|TestOrchestratorRecoveryKeepsABeforeBWhenAUpdatedLater' -count=1
```

Expected: FAIL because records are ordered by mutable update time.

- [x] **Step 9: Implement ordinals and fail-closed legacy validation**

Allocate a non-zero ordinal under `o.mu` when creating a record, persist it in the initial `Preparing` snapshot, and sort journal enumeration by source then ordinal. During recovery initialize each source counter from the maximum ordinal. Return a recovery error if two or more non-terminal records for one source contain ordinal zero; one legacy record may be assigned the next non-zero ordinal and immediately persisted before recovery continues.

- [x] **Step 10: Run Task 1 tests**

```bash
go test ./pkg/storageintegrity -run 'TestOrchestrate_AmbiguousPrepare|TestOrchestrate_TerminalSaveFailure|TestFileIntakeJournalOrders|TestOrchestratorRecoveryKeeps' -count=1
go test ./pkg/storageintegrity -count=1
```

- [x] **Step 11: Commit**

```bash
git add pkg/storageintegrity/intake.go pkg/storageintegrity/journal.go pkg/storageintegrity/intake_journal_test.go pkg/storageintegrity/journal_test.go
git commit -m "fix(storageintegrity): harden durable intake transitions"
```

---

### Task 2: Startup Recovery Drain

**Files:**
- Create: `pkg/storageintegrity/intake_recovery.go`
- Create: `pkg/storageintegrity/intake_recovery_test.go`
- Modify: `pkg/storageintegrity/intake.go`
- Modify: `storage_integrity_runtime.go`
- Modify: `build.go`
- Test: `build_test.go`

**Interfaces:**
- Produces: `func (o *Orchestrator) RecoverPending(ctx context.Context) error`
- Produces: `OrchestratorConfig.RecoveryRetryInterval time.Duration`
- Consumes: journal ordering and transition semantics from Task 1

- [x] **Step 1: Add failing recovery tests**

Add:

```go
func TestRecoverPendingResumesHolderWithoutClientRetry(t *testing.T)
func TestRecoverPendingDrainsAThenBInOrdinalOrder(t *testing.T)
func TestRecoverPendingRetriesRetryableOutcomeUntilTerminal(t *testing.T)
```

Seed the journal directly. Use fakes that record statement IDs and become accepted on a controlled retry. Assert `RecoverPending` returns only after every seeded record reaches `RCBound` or `Cleaned`.

- [x] **Step 2: Verify RED**

```bash
go test ./pkg/storageintegrity -run TestRecoverPending -count=1
```

Expected: build failure because `RecoverPending` does not exist.

- [x] **Step 3: Implement ordered recovery**

`RecoverPending` calls `ensureJournalRecovered`, snapshots non-terminal records in `(source, ordinal)` order, and invokes the existing orchestration path with each record's original admission. A retryable/unknown result or local transient error waits `RecoveryRetryInterval` and retries the same holder. Context cancellation is returned immediately. Default interval is one second; tests inject one millisecond.

- [x] **Step 4: Add failing startup wiring test**

Add `TestBuildServer_StorageIntegrityRecoveryRunsBeforeOtherPreServeWork`. Construct an orchestrator with a seeded journal and assert the recovery fake is called before the next preServe component starts.

- [x] **Step 5: Verify RED**

```bash
go test . -run TestBuildServer_StorageIntegrityRecoveryRunsBeforeOtherPreServeWork -count=1
```

Expected: FAIL because `preServe` only asserts the merge guard.

- [x] **Step 6: Wire recovery before listeners**

Return the orchestrator as part of the internal runtime assembly result. In `preServe`, run initial merge assertion, then `RecoverPending(ctx)`, then start background supervisors, cluster manager, and metrics. A recovery error is wrapped with `storage_integrity.recovery`.

- [x] **Step 7: Run Task 2 tests and commit**

```bash
go test . ./pkg/storageintegrity -run 'TestRecoverPending|TestBuildServer_StorageIntegrityRecovery' -count=1
git add pkg/storageintegrity/intake.go pkg/storageintegrity/intake_recovery.go pkg/storageintegrity/intake_recovery_test.go storage_integrity_runtime.go build.go build_test.go
git commit -m "feat(storageintegrity): drain durable intake recovery at startup"
```

---

### Task 3: Payload Lease Supervisor

**Files:**
- Modify: `pkg/storageintegrity/payload_spool.go`
- Create: `pkg/storageintegrity/payload_lease.go`
- Create: `pkg/storageintegrity/payload_lease_test.go`
- Modify: `pkg/storageintegrity/intake.go`
- Modify: `storage_integrity_runtime.go`
- Modify: `build.go`
- Modify: `pkg/config/storage_integrity_config.go`
- Modify: `pkg/config/storage_integrity_config_test.go`

**Interfaces:**
- Produces:

```go
type PayloadLeaseManager interface {
    EnsurePayloadLease(context.Context, AdmissionRecord, string) error
    ReleasePayloadLease(payloadHash string)
    Run(context.Context)
}
```

- Produces: `NewPayloadLeaseSupervisor(writer *SpoolingPayloadWriter, refreshInterval, refreshBefore time.Duration) *PayloadLeaseSupervisor`
- Produces: `storage_integrity.runtime.payload_lease.refresh_interval` and `refresh_before`, defaulting to one second and 30 seconds
- Consumes: exact payload bytes from `FilePayloadSpool`

- [x] **Step 1: Add lease-expiry RED tests**

Add `TestSpoolingPayloadWriterRefreshesLeaseNearExpiry` and `TestSpoolingPayloadWriterReusesLeaseOutsideRefreshWindow`. Inject a clock and refresh window. Assert near-expiry calls the remote writer twice while a healthy lease calls it once.

- [x] **Step 2: Verify RED**

```bash
go test ./pkg/storageintegrity -run 'TestSpoolingPayloadWriter.*Lease' -count=1
```

Expected: FAIL because every Uploaded record is reused forever.

- [x] **Step 3: Implement lease-aware spool writes**

Reuse an Uploaded record only when `LeaseExpiresUnixMS` is non-zero and later than `now + refreshBefore`. Otherwise repeat the remote put and atomically replace the uploaded metadata.

- [x] **Step 4: Add supervisor RED tests**

Add:

```go
func TestPayloadLeaseSupervisorRefreshesTrackedPayload(t *testing.T)
func TestPayloadLeaseSupervisorRejectsChangedPayloadRef(t *testing.T)
func TestPayloadLeaseSupervisorReleaseStopsRefresh(t *testing.T)
func TestRecoverPendingRegistersLeaseBeforeSubmit(t *testing.T)
```

Use short injected intervals and channels, not sleeps, to observe refresh and release deterministically.

- [x] **Step 5: Verify RED**

```bash
go test ./pkg/storageintegrity -run 'TestPayloadLeaseSupervisor|TestRecoverPendingRegistersLease' -count=1
```

Expected: build failure because the supervisor API does not exist.

- [x] **Step 6: Implement and wire the lease manager**

Track payload hash, length, bytes, and expected ref. `EnsurePayloadLease` performs an immediate lease-aware put, validates the ref, and installs the tracked item. `Run` refreshes tracked items until context cancellation. Before every pre-submit orchestration attempt call `EnsurePayloadLease`; after durable `SubmitAccepted`, `RCBound`, or terminal pre-submit cleanup call `ReleasePayloadLease`. Recovery therefore registers leases from durable admission records before it resubmits. Add positive config validation and start the supervisor from `preServe` after the initial merge assertion and before `RecoverPending`.

- [x] **Step 7: Run Task 3 tests and commit**

```bash
go test . ./pkg/config ./pkg/storageintegrity -run 'TestSpoolingPayloadWriter|TestPayloadLeaseSupervisor|TestRecoverPendingRegistersLease|PayloadLease' -count=1
go test ./pkg/storageintegrity -count=1
git add pkg/storageintegrity/payload_spool.go pkg/storageintegrity/payload_lease.go pkg/storageintegrity/payload_lease_test.go pkg/storageintegrity/intake.go storage_integrity_runtime.go build.go pkg/config/storage_integrity_config.go pkg/config/storage_integrity_config_test.go
git commit -m "feat(storageintegrity): maintain payload ingest leases"
```

---

### Task 4: Continuous Merge Health Gate

**Files:**
- Create: `storage_integrity_merge_supervisor.go`
- Create: `storage_integrity_merge_supervisor_test.go`
- Modify: `storage_integrity_ingress.go`
- Modify: `storage_integrity_runtime.go`
- Modify: `build.go`
- Modify: `pkg/config/storage_integrity_config.go`
- Modify: `pkg/config/storage_integrity_config_test.go`

**Interfaces:**
- Produces:

```go
type StorageIntegrityMergeHealth interface {
    CheckMergeHealth() error
}

func NewStorageIntegrityMergeSupervisor(
    guard StorageIntegrityMergeGuard,
    interval time.Duration,
) *StorageIntegrityMergeSupervisor
```

- Supervisor implements `StorageIntegrityMergeGuard`, `StorageIntegrityMergeHealth`, and `Run(context.Context)`.

- [x] **Step 1: Add supervisor RED tests**

Add:

```go
func TestMergeSupervisorStartsClosedUntilAssertSucceeds(t *testing.T)
func TestMergeSupervisorClosesHealthAfterPeriodicFailure(t *testing.T)
func TestMergeSupervisorReopensAfterSuccessfulReassert(t *testing.T)
```

- [x] **Step 2: Verify RED**

```bash
go test . -run TestMergeSupervisor -count=1
```

Expected: build failure because the supervisor does not exist.

- [x] **Step 3: Implement supervisor**

Serialize guard calls, atomically publish the last error, run one assertion per interval, and continue after failures so health can recover. `CheckMergeHealth` returns a stable wrapped error while closed.

- [x] **Step 4: Add ingress fail-closed RED test**

Add `TestStorageIntegrityIngressRejectsAdmissionWhenMergeHealthClosed`. Assert neither payload put nor orchestrator submit is called.

- [x] **Step 5: Verify RED**

```bash
go test . -run TestStorageIntegrityIngressRejectsAdmissionWhenMergeHealthClosed -count=1
```

Expected: FAIL because ingress does not inspect merge health.

- [x] **Step 6: Wire health and configuration**

Add positive `storage_integrity.runtime.merge_guard.reassert_interval` validation with a 30-second default. Production assembly wraps the configured guard in the supervisor, performs its first assertion in `preServe`, starts `Run(ctx)`, and passes the same supervisor to ingress. Ingress checks health before payload upload.

- [x] **Step 7: Run Task 4 tests and commit**

```bash
go test . ./pkg/config -run 'TestMergeSupervisor|TestStorageIntegrityIngressRejectsAdmission|StorageIntegrity.*MergeGuard' -count=1
git add storage_integrity_merge_supervisor.go storage_integrity_merge_supervisor_test.go storage_integrity_ingress.go storage_integrity_runtime.go build.go pkg/config/storage_integrity_config.go pkg/config/storage_integrity_config_test.go
git commit -m "feat(storageintegrity): continuously gate merge health"
```

---

### Task 5: Production Companion Capability Gate

> Historical implementation record: this compile-time gate was removed after
> arbiter-core and arbiter-proto published the required capabilities. Production
> now fails closed by validating the injected interfaces.

**Files:**
- Modify: `build.go`
- Modify: `storage_integrity_runtime.go`
- Modify: `build_test.go`
- Modify: `pkg/storageintegrity/intake.go`

**Historical interfaces (superseded):**
- Preserved: `CompanionStagedIntakeAvailable == false`
- Produced: deterministic build error containing `companion staged-intake contract unavailable`

- [x] **Step 1: Add production-gate RED test**

Add `TestBuildServer_StorageIntegrityRuntimeRejectsUnavailableCompanionContract`. Supply every local runtime dependency and assert `buildServer` still rejects runtime enablement.

- [x] **Step 2: Verify RED**

```bash
go test . -run TestBuildServer_StorageIntegrityRuntimeRejectsUnavailableCompanionContract -count=1
```

Expected: FAIL because injected Go interfaces currently bypass the documented capability gate.

- [x] **Step 3: Implement the gate**

Check the capability before runtime consumer construction. Keep lower-level constructors available to pure tests. Update existing build-success tests to exercise internal runtime assembly directly or to expect the production gate; do not add a test-only production bypass.

- [x] **Step 4: Run Task 5 tests and commit**

```bash
go test . -run 'StorageIntegrityRuntime|UnavailableCompanion' -count=1
git add build.go storage_integrity_runtime.go build_test.go pkg/storageintegrity/intake.go
git commit -m "fix(storageintegrity): enforce companion runtime gate"
```

---

### Task 6: Documentation, Race Tests, And Full Verification

**Files:**
- Modify: `docs/superpowers/specs/housegate-storage-integrity-insert/2026-07-20-housegate-storage-integrity-p1e-runtime-e2e-design.md`
- Modify: `docs/superpowers/plans/2026-07-28-housegate-p1-runtime-recovery.md`

- [x] **Step 1: Update verification names and mark plan tasks complete**

Replace planned test names with the exact implemented names and distinguish
HouseGate component tests from production tests that require the host-supplied
capability set.

- [x] **Step 2: Run formatting and focused tests**

```bash
gofmt -w pkg/storageintegrity/*.go storage_integrity_*.go build_test.go pkg/config/storage_integrity_config*.go
go test . ./pkg/config ./pkg/plugins/storageintegrity ./pkg/storageintegrity -count=1
go test -race ./pkg/storageintegrity . -count=1
```

- [x] **Step 3: Run repository-wide verification**

```bash
go test ./... -count=1
git diff --check
git status --short
```

Record Docker-only failures separately; do not describe an unavailable integration environment as a passing suite.

- [x] **Step 4: Review the final diff against the spec**

Confirm production construction validates the full capability set, no terminal
state is visible before journal durability, no zero ordinal can reorder
multiple recovered records, lease refresh cannot replace payload ref, and
merge-health failure blocks admission.

- [x] **Step 5: Commit final docs or cleanup**

```bash
git add docs/superpowers/specs/housegate-storage-integrity-insert/2026-07-20-housegate-storage-integrity-p1e-runtime-e2e-design.md docs/superpowers/plans/2026-07-28-housegate-p1-runtime-recovery.md
git commit -m "docs: record P1 runtime recovery verification"
```
