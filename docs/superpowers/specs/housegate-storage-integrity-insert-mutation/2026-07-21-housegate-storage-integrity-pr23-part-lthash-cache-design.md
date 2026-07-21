# HouseGate Storage Integrity: PR23 — PartLtHash Cache and Real-Scan Fallback

Date: 2026-07-21
Branch: `feat/housegate-sentio-pr19-25-safeaudit-quarantine`
Design: `docs/superpowers/specs/2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md` §5.1 (pt 5), §7 `part_lthash_cache`

## 1. Scope — runnable local layer

A bounded LRU cache from a part's identity to its computed LtHash, plus a caching scanner that returns a cached hit or falls through to a real scan. This is a **discardable local performance layer** (design §5.1: the cache may pre-check but must not change a vote/scan result) — so it is fully implemented and **green today**, with **no companion-seam dependency**. Dropping the cache only forces real scans; it can never serve a wrong answer.

## 2. Cache key binds all identity (`part_lthash_cache.go`)

`PartCacheKey` binds **all** of: `RowHashVersion`, `TableID`, `SchemaHash`, `PartPhysHash`, `RowCount`, `Bytes`. A hit requires **every** field to match; any drift (a re-materialized part, a schema change, a row-hash profile bump) changes the key and forces a real scan. `RowHashProfileVersion` mirrors the `pkg/lthash` canonical row profile domain (`housegate-row-mvp-v0`) and must be bumped in lockstep — it is in the key so a profile change invalidates every prior entry. `Valid()` fails closed on a key with no physical hash (nothing anchors the content). `PartLtHashCache` is a concurrency-safe bounded LRU; `maxEntries <= 0` means unbounded; the bound is an eviction cap, not a correctness knob.

## 3. Inspector + caching scanner

- **`PartInspector`** derives the `PartCacheKey` from a `replay.PartManifestEntry` + `payloadexec.TableSchema` (schema hash via `payloadexec.TableSchemaHash`), and `ValidateCachedAgainstEntry` guards against a phys-hash collision serving a differently-named part.
- **`PartScanner`** is the scanner seam: `ScanPart(ctx, entry, schema) → PartLtHashResult`. The real implementation wraps `chexec.ScanParts` against a pinned ClickHouse; a fake stands in for tests.
- **`CachingPartScanner`** wraps an inner `PartScanner` + cache: it returns a validated hit (identical to what a real scan would produce), or on a miss / un-keyable part / inconsistent hit falls through to a real scan and populates the cache. A nil cache makes it a correct pass-through. Caching is best-effort — a `Put` error never fails a scan.

## 4. Config

`storage_integrity.part_lthash_cache` gains `{enabled, path, max_entries}`, default off. Because it is a discardable local layer with no companion dependency, **enabling it is accepted** (validated, not v1-rejected) — unlike the mutation/safe_merges toggles.

## 5. Reused types (never redefined)

`replay.PartManifestEntry`, `payloadexec.TableSchema`, `payloadexec.TableSchemaHash`.

## 6. Tests (all green today, no gate)

- `PartCacheKey.Valid` (every binding field required).
- Cache get/put; **all six key fields bind a hit** (any single field change misses); invalid key rejected by `Put`; LRU eviction respecting recency.
- `CachingPartScanner`: a hit returns exactly what the real scan produced and skips the rescan (hit/miss counters); an un-keyable part always rescans; a nil cache is a pass-through; an inconsistent (phys-collision) hit is discarded and rescanned; a real-scan error propagates.
- `PartInspector.KeyFor` rejects a no-phys-hash part.
- Config default-off + enabling-accepted.
