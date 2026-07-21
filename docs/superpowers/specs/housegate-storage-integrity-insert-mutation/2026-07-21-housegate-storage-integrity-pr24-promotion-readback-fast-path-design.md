# HouseGate Storage Integrity: PR24 — Promotion Exact-Parts Readback Fast Path

Date: 2026-07-21
Branch: `feat/housegate-sentio-pr19-25-safeaudit-quarantine`
Design: `docs/superpowers/specs/2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md` §9 acceptance ("promotion readback fast path")

## 1. Scope

A promotion-readback fast path that reduces the row scan but produces the **same** result as the strict path: the same post state root **and** the same exact candidate-to-safe-part mapping (design §9 acceptance matrix: "promotion readback fast path: post root 和 exact `Parts[]` mapping 都与 strict path 一致"). This PR ships the pure equivalence assertion plus the gated C0 SNode readback port.

## 2. Pure logic (`promotion_readback.go`)

- **`PromotionReadbackMapping`** — the readback result: `PostStateRoot` + the exact `[]replay.PartManifestEntry` mapping.
- **`AssertReadbackFastPathEquivalent(strict, fast)`** — the fail-closed predicate that the fast path equals the strict path: identical `PostStateRoot` **and** an order-insensitive exact match of the parts mapping by every field (table/partition/part name/phys hash/row-lthash/rows/bytes, with `StorageRefs` compared order-insensitively). Any divergence is an error — the fast path may only reduce work, never change the result.

## 3. Gated driver (blocked-skeleton, C0)

`SNodeReadbackPort` is the gated **C0** port that reads back the exact active parts a promotion produced from the SNode. It is **absent** from arbiter/arbiter-proto (no exact-parts/readback RPC exists — verified by grep). `PromotionReadback` holds the gated SNode port and a strict-path `PartScanner` (the PR23 seam); `Readback` fails closed while `CompanionMutationConsensusAvailable == false`. No SNode protocol is fabricated.

## 4. Reused types (never redefined)

`replay.PartManifestEntry`, `payloadexec.TableSchema`, `CandidatePart`, `PartScanner` (PR23), `auditPartKey` (PR19), `requireCompanionMutationConsensus`.

## 5. Tests

- **Pure (green today):** `AssertReadbackFastPathEquivalent` identical (with part + storage-ref order-insensitivity) and the full divergence matrix (post root, blank root, missing/extra part, phys-hash / row-lthash / rows / bytes / storage-refs drift).
- **Gated (skip-closed):** `NewPromotionReadback` wiring; `Readback` fails closed while C0 absent; the real SNode fast-path-matches-strict test `t.Skip` then `t.Fatal("unreachable ...")`.
