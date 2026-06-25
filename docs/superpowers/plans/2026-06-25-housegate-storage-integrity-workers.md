# HouseGate Storage Integrity Workers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the HouseGate-side P0 runtime primitives from the storage-integrity design: mock payload storage, mock finality, replay submission, promotion execution, and safe-audit hashing/voting.

**Architecture:** Keep HouseKeeper as the control-plane owner by depending on narrow interfaces. HouseGate owns workers and local adapters only; real Keeper RPC and real DA/L2 can replace the injected interfaces later.

**Tech Stack:** Go, existing `pkg/replay`, existing `pkg/config`, `build.go` server lifecycle.

---

### Task 1: Storage Integrity Package

**Files:**
- Create: `pkg/storageintegrity/types.go`
- Create: `pkg/storageintegrity/mock_payload_store.go`
- Create: `pkg/storageintegrity/mock_finality.go`
- Create: `pkg/storageintegrity/replay_worker.go`
- Create: `pkg/storageintegrity/promotion_worker.go`
- Create: `pkg/storageintegrity/safe_audit_worker.go`
- Test: `pkg/storageintegrity/*_test.go`

- [ ] Write tests first for durable `MockPayloadStore`, immediate/delayed `MockFinalityWatcher`, replay submit/fail behavior, promotion success/failure behavior, and safe-audit row/batch hash behavior.
- [ ] Implement only the APIs needed by those tests.
- [ ] Run `go test ./pkg/storageintegrity -count=1`.

### Task 2: Config

**Files:**
- Create: `pkg/config/storage_integrity_config.go`
- Modify: `pkg/config/config.go`
- Modify: `pkg/config/BUILD.bazel`
- Test: `pkg/config/storage_integrity_config_test.go`

- [ ] Add disabled-by-default `storage_integrity` config.
- [ ] Validate server-only enablement, required mock payload path, positive poll interval, and non-negative mock finality delay.
- [ ] Run `go test ./pkg/config -run StorageIntegrity -count=1`.

### Task 3: Server Lifecycle Wiring

**Files:**
- Modify: `proxy.go`
- Modify: `build.go`
- Test: `build_storage_integrity_test.go`

- [ ] Add background runner support to `builtServer` so worker runtimes share the proxy run context.
- [ ] Add `Options` injection points for storage-integrity control plane, replay dependencies, promotion executor, audit reader, and audit signer.
- [ ] Wire enabled storage-integrity config into a `storageintegrity.Runtime`.
- [ ] Run `go test . -run StorageIntegrity -count=1`.

### Task 4: Verification

- [ ] Run `go test ./pkg/storageintegrity ./pkg/config . -count=1`.
- [ ] Run targeted existing packages touched by lifecycle wiring: `go test ./pkg/replay ./pkg/proxy -count=1`.
