# Storage Integrity Closure — Roadmap and Spec Index (N/O/P)

**Date:** 2026-08-25 **Status:** Proposed **Source:** the 2026-08-25 verification review of Specs I/J/K/L against each repo's latest `origin/main` (four parallel read-only agents; every load-bearing claim re-read at source, the Critical findings reproduced against the shipped scanner and the live v0.9.0 native engine). **Parent:** [2026-08-19 remediation roadmap](2026-08-19-storage-integrity-remediation-roadmap.md) — this document indexes Specs N–P, which close what that round left open plus two bypasses it introduced or missed. **Code base:** housegate `6fd56b8` (v0.11.0) plus open PR #141, arbiter `c1d32f6` (v0.3.0), arbiter-core `32b59a8` (v0.5.1), arbiter-proto `19d90fc` (v0.6.0), sentio-node `58f5e5f`, rewriter-go `23687cc` (v0.9.0), rewriter-grpc `a8ca4e7` (v0.13.0+1). **Source of truth:** English version.

## 1. What the review found

The library work is good and, where it could be tested empirically, it holds. Spec K's `statements_root` goldens genuinely fail on a field reorder and have no regenerator. Spec K D7 derives the `statement_kind` expectation from the ingress's own classification rather than from the client, and its completeness gate requires all thirteen bound fields to be exercised — the same gate that caught the original `statement_kind` omission. Spec L D3's bounded reads and D6's non-closing 252 both have working docker end-to-end proofs. Spec J's CI gates execute 56 real targets with three anti-skip guards on the drift assertion.

Three things cut across all of it.

**The production path is not connected.** Spec I's HouseGate half sits in open PR #141 while `main` still pins `rewriter-go v0.7.1` — the engine from before Spec I. All three of Spec I's defence layers are therefore absent from `main`, and the two Critical statements it exists to close are end-to-end live there. sentio-node, the production host, pins `housegate v0.9.2` / `arbiter-core v0.4.0` / `arbiter-proto v0.5.0` and consumes none of Specs K or L: the "permanent brick" from a malformed column type, the unbounded `system.parts` scan and the connection-tearing 252 are all still live in production.

**Two new bypasses.** `sireserved`'s scanner does not model ClickHouse heredocs, so a `--` inside `$$…$$` blanks the rest of the statement from both scan surfaces and the operator guard sees nothing — on a path where it is the only control. And `SHOW COLUMNS` / `INDEX` / `INDEXES` / `KEYS` carry a database and a table target that the Go engine's SHOW classification assumes away, so any ordinary authenticated user reads the SI physical schema; the C++ engine fails closed on the same input by accident, so the two engines diverge on live input that the shared corpus does not cover.

**Half-closed residue.** The dispatch path never got the completeness assertion the audit path got, so a corrupted reader can produce signed fraud evidence against an honest source. `fullScope()` still reads safe-database part names, moving Spec L D3(b)'s growth cliff to startup rather than closing it. One golden vector still has a `-update` regenerator. Three CI configurations report skips as passes.

Full evidence with file:line and reproduction is in the review artifact; each spec restates what it needs.

## 2. Spec set

| # | Spec | Repos | Fixes | Urgency |
|---|---|---|---|---|
| **N** | [Lexical and SHOW-namespace closure](2026-08-25-storage-integrity-lexical-namespace-closure-design.md) | housegate, rewriter-go, rewriter-grpc | the heredoc bypass, the `SHOW COLUMNS`/`INDEX` family and the Go↔C++ divergence, the cross-engine differential run, connector table functions | **Blocker** — must land before PR #141 merges |
| **O** | [Production rollout of Specs I–L](2026-08-25-storage-integrity-production-rollout-design.md) | rewriter-go, housegate, arbiter, arbiter-core, sentio-node | the pin chain, the #141 merge, the `ErrPayloadMismatch` split, the back-pressure disable sentinel, the duplicate mode derivation, the two missing acceptance tests | **Blocker** — nothing shipped so far is reachable in production without it |
| **P** | [Residual binding and verification closure](2026-08-25-storage-integrity-residual-binding-design.md) | arbiter, housegate, arbiter-core, sentio-node | dispatch-path completeness, the `SET` record, read-mode e2e, the safe-database part scan, the last golden regenerator, CI honesty, bookkeeping | High — independent of N/O |

## 3. Order

```
   N (lexical + SHOW)  ──► rewriter-go v0.9.1 ──┐
                                                 ├──► O (rollout: pins, #141, host)
   P (residual)  ── independent ─────────────────┘        ├─ arbiter-core v0.6.0
                                                          ├─ housegate v0.12.0
                                                          └─ sentio-node v0.1.0 ──► M (devnet2)
```

**N first and alone.** Its heredoc fix goes onto PR #141's own branch, because it closes a hole that PR introduces; #141 must not merge without it. Its engine half cuts the tag O consumes.

**P runs in parallel with N.** Different repos and files (arbiter's FSM, HouseGate's pressure guard, arbiter's accumulator vectors, three CI files). Its one HouseGate code change — `fullScope()` — does not touch anything N touches.

**O is strictly sequential inside itself.** Engine tag → HouseGate pin + merge + tag → arbiter-core tag → sentio-node pins + tag. Each step's failure must be loud and local; nothing is bundled to save a round trip. O's arbiter-core work (the sentinel split, the disable sentinel) has to land before sentio-node's bump, since the bump is what makes it consumable.

**M last**, unchanged from the previous roadmap: it pins the release tags this round produces.

## 4. Decisions this roadmap takes (override before executing)

1. **A `$` outside a quoted span is refused, not copied through** (N D1). The heredoc span is modelled properly; anything else `$` could be is not something the operator guard needs to admit, and copying it through is exactly what produced the bypass.
2. **The SHOW family is classified positively with a fail-closed default** (N D2): rewritten / target-bearing / target-less, and an unrecognised kind is a rejection under an active SI contract. The target-less list is an allowlist justified case by case, not a residue.
3. **Metadata confidentiality is explicitly not an SI v1 property** (N §2). `system.parts` / `system.tables` / `system.merges` expose SI physical names to any authenticated reader, so gating `SHOW MERGES` while leaving `SELECT … FROM system.merges` open would be theatre. Recorded as debt, not closed.
4. **The two engines are proved equal by execution, once, before N merges** (N D3) — the `REWRITER_ORACLE_ADDR` differential over the full corpus. Byte-identical JSON proves the inputs match, not the outputs. Automating it in CI is separate work.
5. **`ErrPayloadMismatch` is split into two sentinels that both still match the original** (O D1), so the change is additive for every existing caller. The previous round's instruction to map it wholesale to terminal-reject was unsound — "not retryable" is not "did not write", and a blanket mapping wedges the retry loop.
6. **Back-pressure disable becomes explicit and doubly-stated** (O D2): `DisableHardParts` plus a validation error if a non-zero limit is also configured. A half-configured disable stops being expressible.
7. **When two functions derive the same thing and disagree, the fail-open one is deleted, not fixed** (O D3), and the test that pinned it is re-pointed at the production predicate.
8. **The dispatch path refuses to dispatch an incomplete block** (P D1) rather than extending `ReplayJob` with `statement_count` / `statements_root`. The wire change is the better long-term answer and is recorded as debt; refusing locally is correct, cheap and does not cross a protocol boundary.
9. **Every new guard ships with a step proving it fails against the unfixed code.** Carried forward from the previous round, where it was the discipline that caught eight self-inflicted defects.

## 5. Bounded tasks (no spec; do directly)

- sentio-node still has zero release tags; O D5 cuts the first one.
- The user-environment `~/.git/.gitignore` bare `AGENTS.md` rule is still the user's call, still listed so it is not forgotten.
- `production`'s committed plaintext ClickHouse password, Harbor robot password and sidecar private key remain out of scope for storage integrity and inside Spec M's blast radius.

## 6. Out of scope

Everything the two previous roadmaps defer (P2 mutations, P3 SafeAudit, P4 compaction, the DDL lane, sharding, threshold signing, independent challenger dispatch, `INSERT ... SELECT`, RFC-8785 canonical JSON, a `column_id`-keyed row profile). Newly recorded here: `system.*` metadata exposure; replacing `sireserved`'s hand-rolled scanner with the polyglot tokenizer; the cross-engine differential as a CI job; `statement_count` / `statements_root` on `ReplayJob`.
