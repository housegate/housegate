# HouseGate Storage Integrity: PR25 — Read-Set Decision TTL Cache and Event Invalidation

Date: 2026-07-21
Branch: `feat/housegate-sentio-pr19-25-safeaudit-quarantine`
Design: `docs/superpowers/specs/2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md` §5.2 (last bullet), §7 `read_set_cache`

## 1. Scope — runnable local layer

A TTL cache in front of one committed `SafeReadGate` for safe-SELECT read decisions. The decision cache is keyed by `(table, requested snapshot, worker, read mode)` and invalidated on a new manifest (safe cut), quarantine, watermark lag, active-set mismatch, or TTL expiry (design §5.2 last bullet). It depends only on HouseGate-side pieces (PR16 `SafeReadGate`, and the PR20/PR21 quarantine/re-entry events feed its invalidation hooks) — **no companion-seam dependency**, so it is fully implemented and **green today**. It is fail-safe: it never serves a stale allow.

## 2. Design (`read_set_cache.go`)

- **`Clock`** — the injected monotonic clock seam; the cache never calls `time.Now()` directly, so its TTL logic is deterministically testable. `SystemClock` is the production impl.
- **`ReadSetCacheKey`** — `{TableID, RequestedSnapshot, WorkerID, ReadMode}`; a decision cached for one tuple is never reused for another.
- **`ReadSetDecisionCache`** — concurrency-safe. `Decide(key)` returns a live cached `GateDecision` or recomputes via `gate.MayServe(worker, snapshot)` and caches the result (when TTL > 0). The cached decision is always the gate's own answer — the cache never invents a verdict. A non-positive TTL disables caching (every `Decide` recomputes), still correct.
- **Invalidation hooks:**
  - `InstallCut(cut)` — rebinds to a new committed safe cut and **flushes all** entries (a new manifest/watermark/read-set is a global eligibility change). Fail-closed on an invalid cut: the old gate is kept and the error returned.
  - `Invalidate(key)` — targeted eviction.
  - `InvalidateWorker(worker)` — drops every entry for a worker (the hook for a quarantine or active-set mismatch, which affects the worker across all tables/snapshots/modes).
  - `InvalidateAll()` — coarse flush for an uncertain blast radius.

## 3. Config

`storage_integrity.read_set_cache` gains `{enabled, ttl}`, default off with a 5s default TTL (matching design §7). Because it is a discardable local layer with no companion dependency, **enabling it is accepted**; a non-positive TTL when enabled is rejected (a zero TTL would cache forever and never invalidate on time).

## 4. Reused types (never redefined)

`SafeReadGate`, `SafeCutView`, `NewSafeReadGate`, `GateDecision`, `GateDenyNotInReadSet`, `GateDenyQuarantined`.

## 5. Tests (all green today, no gate)

- Hit returns the gate's decision (allow + typed deny); TTL expiry recomputes; zero-TTL never caches.
- `InstallCut` flushes all entries and the recomputed decision reflects the new cut (a newly-quarantined worker is denied — no stale allow served); an invalid cut is rejected and the old gate kept.
- Targeted `Invalidate` / `InvalidateWorker` (across snapshots+tables) / `InvalidateAll`.
- Nil clock defaults to `SystemClock`.
- Config default-off-with-positive-ttl, enabling-accepted, non-positive-ttl-rejected.
