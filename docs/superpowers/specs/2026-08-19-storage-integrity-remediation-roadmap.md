# Storage Integrity Remediation — Roadmap and Spec Index

**Date:** 2026-08-19 **Status:** Proposed **Source:** the 2026-08-19 implementation review of Specs A/C/G/F (five parallel adversarial reviews; findings reproduced against the live native engine, real ClickHouse 25.8, and rendered Helm output). **Parent:** [2026-08-18 v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) — this document indexes the remediation specs I–M that close its gaps. **Code base:** housegate `621eaab` (v0.9.3), arbiter `71657a8` (v0.2.1), arbiter-core `b669ccd` (v0.3.1), arbiter-proto `bb1823f` (v0.5.0), sentio-node `ba136ea`, rewriter-go `dbac7bc` (v0.7.1), rewriter-grpc `ddc24b9` (v0.12.1), rewriter-proto `1879d30` (v0.2.0), production `dev/uranus/storage-integrity-devnet2` `3509fbae1`. **Source of truth:** English version.

## 1. What the review found

Specs A, C and G shipped across six repos; every repo builds and every local unit test passes; all 354 plan steps are checked. The protocol core is sound: `user_jws` now binds all thirteen envelope fields against ingress-derived expectations, `statements_root` is inside `ChainHash()`, and a genuine payload-swap fraud test proves the attack Spec A existed to close is closed.

Four classes of problem remain.

**The read surface is not fail-closed.** Two ordinary SQL statements let any authenticated user defeat the integrity layer: `SYSTEM START MERGES hg_unsafe.<t>` passes through the engines' catch-all as `Success` and restarts the merges whose absence is the candidate-part boundary; `TRUNCATE DATABASE hg_safe` is rejected by the engine *before* it records an accessed table, so HouseGate's SI-flag-keyed fail-closed gate never fires and the statement is forwarded verbatim.

**Two commitments and one admission path rest on unfrozen or unenforced invariants.** `statements_root`'s preimage is `json.Marshal` over a Go struct, so the anchored chain hash depends on field declaration order in another module, with no golden vector. Column type strings reach executed DDL unescaped and unvalidated. The back-pressure guard runs an unbounded `system.parts` scan on every admission behind a hard-coded 2s timeout with no config key.

**Verification coverage is the weakest part of the work.** HouseGate's CI runs none of its 1130 unit test functions; rewriter-grpc has no CI at all; the byte-identical 178-case corpus that is supposed to be the two engines' parity gate compares generated SQL in exactly one C++ case; Spec C's only end-to-end proof has never executed.

**The devnet2 pilot cannot pass its own acceptance run.** Four deterministic blockers, plus a key-reuse choice that reduces the fraud drill to a mechanism demo.

Full detail, with file:line evidence and reproduction, is in the review artifact; each spec below restates what it needs.

## 2. Spec set

| # | Spec | Repos | Fixes | Urgency |
|---|---|---|---|---|
| **I** | [SI surface fail-closed hardening](2026-08-19-storage-integrity-surface-failclosed-design.md) | rewriter-go, rewriter-grpc, housegate | the two Critical holes, literal escaping, the four rewrite correctness bugs, the peer-trust bypass decision | **Blocker** — ship before any SI deployment |
| **J** | [Verification restoration](2026-08-19-storage-integrity-verification-restoration-design.md) | housegate, rewriter-grpc, rewriter-go, sentio-node, arbiter-core | CI test execution, the corpus parity gate, agent-guidance files, self-mutating goldens | **Blocker** — it is what proves I/K/L are correct |
| **K** | [Commitment durability and admission hardening](2026-08-19-storage-integrity-commitment-durability-design.md) | arbiter, arbiter-core, housegate | `statements_root` golden, `Params.NetworkID`, `L3BlockView` completeness, replay-path `schema_hash`, deferred named blocks, `settings_hash` key set | High |
| **L** | [Protocol table and back-pressure hardening](2026-08-19-storage-integrity-table-backpressure-hardening-design.md) | arbiter-core, arbiter, housegate, sentio-node | column-type validation, mode derivation, bounded pressure reads, reconcile resilience, `hg_promote` lifecycle | High |
| **M** | [devnet2 pilot readiness](2026-08-19-storage-integrity-devnet2-readiness-design.md) | production, arbiter, arbiter-core | the four blockers, key split, `/metrics`, runbook truth, NetworkPolicy | Gates the pilot |

## 3. Order

```
   I (fail-closed)  ─┐
   J (verification) ─┼──► K, L can start immediately and merge behind I/J
   K (commitments)  ─┤
   L (data plane)   ─┘
                      └──► M (devnet2) — last; consumes the tags I/J/K/L produce
```

**I and J first, together.** I closes the holes; J is what stops the next regression from shipping green. They touch disjoint files (I is engine + rewriter adapter logic, J is workflows + test runners + fixtures), so two agents can run in parallel.

**K and L are independent of each other and of I/J** — different repos and files — so all four can be in flight at once. They must merge *after* J's CI change lands, so their tests actually run in CI on the way in.

**M last.** It pins release tags, and several of its fixes (metrics in `cmd/arbiter*`) are code changes in repos I/J/K/L also touch.

## 4. Decisions this roadmap takes (override before executing)

1. **The engines fail closed for SI-configured requests, not just for SI-flagged tables.** Any statement class the engine does not model, when the request carries a non-empty SI table set, becomes a rejection rather than a pass-through (Spec I D1). This is the general fix; enumerating `SYSTEM`/`TRUNCATE DATABASE`/… is the specific one, and I does both — the enumeration for a good error message, the catch-all so the next unmodelled class is safe by default.
2. **Peer-trusted and forwarded sessions keep bypassing SI policy in v1**, and that becomes a recorded, tested decision rather than an accident (Spec I D6). Changing it means peer SQL — already rewritten by the originating proxy — would be re-rewritten, which the peer-trust design forbids. The mitigation is a documented network-isolation requirement plus a startup warning when SI tables are configured and an internal port is open. Maintenance and platform-operator sessions retain their general rewrite bypass but are separately prevented from mentioning the protocol-owned SI databases or row-id column by HouseGate's `sireserved` guard.
3. **`statements_root` gets a golden vector, not a canonical-JSON encoder.** RFC-8785-style canonicalization is the right long-term answer and is out of scope here; a frozen test vector converts a silent break into a red test at zero runtime cost (Spec K D1).
4. **Column types are validated by an allowlist of type expressions, not by a parser.** The SI profile's supported types are already a short whitelist in `payloadexec`; reusing it keeps one source of truth and rejects everything else (Spec L D1).
5. **Back-pressure reads become bounded aggregates scoped to the touched tables**, and the exact-name inventory is kept only where the reservation protocol genuinely needs it (Spec L D3). `refresh_timeout` and `snapshot_ttl` become config keys.
6. **The devnet2 pilot splits the authority and poster keys off the indexer key** (Spec M D2). Without it the fraud drill demonstrates the mechanism but not the property Spec A exists to establish.
7. **HouseGate CI runs the full unit suite; a failing test blocks merge** (Spec J D1). If uncommenting surfaces pre-existing failures, they are fixed or explicitly quarantined with an issue reference — not left disabled.

## 5. Bounded tasks (no spec; do directly)

- `~/.git/.gitignore` line 20's bare `AGENTS.md` rule is a **user environment change, not a repo change** — it silently untracks nested agent-guidance files in every repo. Spec J covers the repo-side fix (correct `pkg/replay/AGENTS.md`, track it); tightening or removing the global rule is the user's call and is listed here so it is not forgotten.
- arbiter / arbiter-core: bump the housegate pin from v0.9.0 to v0.9.3 (7 commits behind); sentio-node from v0.9.2 (4 behind). Do this as each spec's dependency-bump task rather than as a standalone PR.
- sentio-node has zero release tags; cut one so its commits are addressable.
- production: the pre-existing plaintext ClickHouse password / Harbor robot password / sidecar private key remain committed (out of scope for storage integrity, inside M's blast radius).

## 6. Out of scope

Everything the v1 closure roadmap already defers (P2 mutations, P3 SafeAudit, P4 compaction, the DDL lane, sharding, threshold signing, independent challenger dispatch, `INSERT ... SELECT`). Plus, newly recorded from this review: RFC-8785 canonical JSON for every root (K D1 records the debt); a `column_id`-keyed row profile (already tracked as a P2/DDL-lane prerequisite); `ErrorReverseMap` beyond the physical-name scrubbing I adds; and rewriter contract minor-version negotiation beyond the single startup assertion I D5 adds.
