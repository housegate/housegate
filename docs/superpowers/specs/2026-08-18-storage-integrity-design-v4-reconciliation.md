# Storage Integrity Design v4 Reconciliation (docs-only)

**Date:** 2026-08-18 **Status:** Proposed **Roadmap:** [v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) Spec B. **Targets:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) (→ v4, en + zh-CN regenerated), [designs/sento-network/PROGRESS.md](https://github.com/sentioxyz/designs/blob/main/sento-network/PROGRESS.md), housegate `CLAUDE.md`, housegate `pkg/replay/AGENTS.md`, rewriter-proto `proto/rewriter.proto` comments, rewriter-grpc `README.md`, compute-network-contracts `CLAUDE.md`, arbiter `README.md`. **Source of truth:** English version; regenerate zh-CN from it.

## 1. Purpose

Bring the base design and the repo-level guidance documents back in line with what is implemented (as of the 2026-08-18 review) and with what Specs A/C/D/E/G decide. This spec is an edit list, not a redesign: every item names the section, the current text's claim, the truth, and the replacement. Items marked **(after A)** etc. must wait for that spec to merge so the doc describes shipped behaviour; everything else can land now.

## 2. `2026-06-22-storage-integrity-design.md` → v4

Bump header to `Status: Proposed (v4, reconciled with P0–P1e implementation, 2026-08-18)`; add the 2026-08-18 review and the six roadmap specs to §17 References. Section edits:

| § | Current claim | Truth / decision | Edit |
|---|---|---|---|
| §4, §5.1, §6, §9 | The agent injects `_hg_row_id` before signing; HouseGate does not touch reserved columns | `_hg_row_id` is derived deterministically at execution by SNode/verifier from `(network_id, table_id, statement_id, ordinal)` (`payloadexec.RowID`); the ingress rejects payloads that already carry it; the signed payload is row-id-free | Rewrite the four passages: "The row id is a pure function of signed inputs and is injected by the executor (source SNode and every replay), never by the client, HouseGate, or the payload store; a payload carrying `_hg_row_id` is rejected at ingress." Update the §9 sequence diagram note 1 accordingly. |
| §5.2, §7, §15 Q7 | Mountain-range Merkle accumulator + per-account high-water mark, "Resolved" | Account-granularity depth-256 SMT committing `(hi_seq, gap ranges)` per account, K=64 open-range budget, `ADMISSION_CODE_GAP_BUDGET_EXCEEDED`; FSM holds the dictionary; carried proofs rejected in v1 ([2026-07-04](2026-07-04-arbiter-accumulator-design.md)) | Replace the construction paragraph with a two-sentence summary + link; keep the requirements list; add the gap-budget admission outcome to the §11 state machine's `Accepted` entry conditions. |
| §6 | `hg_unsafe.Transfer_<table_id>`; example DDL with `toYYYYMM(block_time)` partition | Naming is `hg_unsafe.<db>__<table>` (`snode.CHTableName`, `table_id` = logical `db.table`); v1 partition key must be a bare `String` column; DDL text = Spec C `BuildDDL` output incl. pinned settings and `ORDER BY (<partition_by>, _hg_row_id)` | **(after C)** replace naming + example DDL; add a "v1 freeze" callout for the partition key with the follow-up (partition expressions need `partition_id`-based promotion, tracked). |
| §7 envelope | Field list; `schema_snapshot_id` per statement; `user_jws_v2` payload | Spec A's field list (`schema_hash` instead of per-statement `schema_snapshot_id`, `client_revision` added), purpose `housegate-statement-v2`, two-token model (`SQL_x_auth_token` legacy + `SQL_x_statement_token`), agent-side deferred INSERT | **(after A)** replace the envelope block and the signing-payload block verbatim from Spec A §4; add one paragraph on why the agent must answer the sample block itself. |
| §7 `settings_hash` | listed, undefined | v1 = digest of the empty set; SI lane rejects client settings | **(after A)** define. |
| §7 L3Block | `statements: [StatementEnvelopeV2]` in the block | Header + `statements_root` (Spec A) — the header commits the envelopes by root, the envelopes are fetched by `SafeState.GetL3Block` | **(after A)** show `L3BlockHeader` as implemented (`arbiter/fsm/state.go`) with `statements_root`. |
| §7 DDL table | v1 admission classes | None implemented; `SCHEMA_NOT_ALLOWED` reserved; no DDL command in the alphabet | Add a status line above the table: "Not implemented in v1 closure; the table is the target for the DDL lane (post-v1)." |
| §8 formulas | BLAKE3 everywhere; `data_root` includes `schema_hash` + `active_parts`; row element `housegate-row-v1` keyed by `column_id`; `schema_hash` includes keys/engine/settings | Roots: SHA-256 over canonical JSON (`housegate-replay-mvp-v0`), `data_root` = `safe-snapshot-data-v2` over `{table_id, partition_roots}` only (parts covered by `manifest_root`); row element `housegate-row-mvp-v0` keyed by column **name**; `schema_hash` = `(network_id, table_id, partition_by, [(name,type)])`; `part_phys_hash` is a placeholder in the in-process executor | Replace the formula block with the implemented profile and add a **"Profile debt"** callout: name-keyed row elements make `RENAME COLUMN` non-neutral (contradicts the §7 DDL table); a `column_id`-keyed `housegate-row-v1` + full `schema_hash` is a prerequisite of the DDL lane / P2 and gets its own spec. Keep the four-level table (it is correct). |
| §9 flow + properties | HouseGate spools payload after computing hash; challenge; three-way | Correct in shape. Add: ACK2 is the staged-intake dual gate (statement accepted **and** RC bound, [P1e](housegate-storage-integrity-insert/2026-07-20-housegate-storage-integrity-p1e-runtime-e2e-design.md)); the byte-side scan request identifies parts by name on the wire while the FSM compares by commitment | Add two bullets; link P1e. |
| §9/§11 challenge | Challenge replay adjudicated by the three-way predicate; may resolve Safe | v1: `QuorumFailed` opens and immediately resolves `Rejected`; no independent challenger, no timeout path | Add "v1 status" note under the challenge paragraph and mark `ChallengeReplay → Safe` as unreachable in v1 in the state diagram caption. |
| §9 quorum | 2-of-3 independent replicas | Implemented as exactly 3 selected from Active non-source verifiers, floor 3 → tolerates 0 malicious verifiers | Add the sentence; reference roadmap decision 7. |
| §11 read modes | default `safe` | Mechanism supports both; policy is `storage_integrity.read.default_mode` (shipped `safe`; devnet2 `unsafe_latest`) via Spec G; `unsafe_latest` dedups by `_part` exclusion from the co-located SNode journal | **(after G)** rewrite the table's "Rewrite" column with the actual SQL shapes; note the PROGRESS 07-08 preference and that it is a config policy. |
| §12.1 | Replication-plane forwarding "reusing `__peer__`" | housegate `pkg/replicationproxy` uses HTTP headers `X-Housegate-Peer-User/-Token`, not the native envelope; not deployed on devnet2 | Fix wording; add deployment note. |
| §12.2 | "Promotion runs locally on every replica" | Today single safe replica; Spec D makes it plural with per-node acks, cleanup gate, lag classes | Add "v1 status" now; **(after D)** rewrite as implemented. |
| §12.2 | `hg_promote.<t>_<snapshot_id>` | persistent `hg_promote.<t> AS hg_safe.<t>` with the partition dropped before/after | Fix the SQL sketch. |
| §12.2 / §12.3 | pinned DDL settings, throttle "mandatory" | Spec C | **(after C)** replace with the pinned table + the throttle description; note SNode mirror. |
| §12.4 | `hg_safe` merges admitted under the ledger equation | v1: `hg_safe` merges stopped and pinned off; growth metric; controlled compaction is P4 | Add "v1 status". |
| §12.5 | "P1 spike" | Spec D | **(after D)** replace with a summary of D3–D7. |
| §13 | mitigations list | none implemented | Add "v1 status: not started (P3)". |
| §14 phases | P0/P1 lists | Mark each bullet done/partial/missing per the review; add "P1 closure" = Specs A/C/D/G + F. |
| §15 | open questions | Q1 resolved (route A, implemented); Q4 resolved (protos frozen in arbiter-proto); Q5 resolved (commitment-only anchor `(l3BlockHash, stateRoot)`); Q6 partially (v1: DA never releases AUDIT pins; retention/custody proof still open); Q8 narrowed to Spec E's frozen set; Q11 resolved but requires Spec E D4 to be enforceable; Q14 → Spec D D6/D8; Q15 v1 HA done (3–5 node raft), sharding seam only; Q16 → Spec G | Update statuses in place. |
| §16 | route B contrast | unchanged | leave. |

## 3. `designs/sento-network/PROGRESS.md`

Snapshot is 07-08. Add a "2026-08-18 review" entry under 最新变化: P1c/P1d/DA client/P1e/schema registry A+B all landed; the honest architecture block (single safe replica today; nothing deployed); the two protocol gaps (JWS binding, L3 header) and their fix (Spec A); the roadmap link and its ordering; the DA storage decision status (network-da: GCS/fs/mem behind `da.proto`, GCS in prod — closes the 07-08 🔴 item as "protocol-first done, GCS chosen for v1"); the read-mode decision as a config policy (Spec G D1) awaiting the product call; update 分工 if changed; move the 06-24 action items about `MOVE PART`/`FETCH` and 双 unsafe 表轮换 to "superseded by `hg_promote` + `REPLACE PARTITION`, implemented".

## 4. housegate `CLAUDE.md`

- Key Modules: add `pkg/storageintegrity` (staged intake orchestrator, ACK2 dual gate, journal + recovery, payload spool + lease, merge guard, back-pressure guard after C, arbiter-proto adapters, `pkg/replay/nativepayload` after A) and `pkg/plugins/storageintegrity` (signed-ingress admission: hook set incl. `StrictQueryDecodePlugin`/`StrictDataPlugin`/`StrictDataLimitPlugin`/`QueryInputCompletePlugin`/`QueryAbortPlugin`, `SuppressUpstreamExecution`, statement-id form, v2 token after A); after A also `pkg/plugins/sistatement` (agent-side deferred INSERT signer) and Relay's deferred-INSERT mode under §3 pipeline.
- Config: `storage_integrity.{ingress, runtime, tables, read, agent, safe_merges}` with the mode gating (ingress/runtime/read server-only; agent agent-only).
- `pkg/plugin/` hook list: add the strict/input-complete/abort hooks.
- Root `Options`: `StorageIntegrityRuntime`, `StorageIntegrityAdmissionConsumer`, `StorageIntegrityReadState` (after G); remove `StorageIntegrityPayloadMaterializer` (after A).
- Dependencies: `github.com/sentioxyz/arbiter-proto` pin.
- Fix line 168's rewriter paragraph: both engines emit the dotted `phys.\`logical.table\`` form (rewriter-grpc PR #37); the dynamic-args field is `upstream_physical_database_in_context`; `DESCRIBE` is not rewritten until Spec E D6.
- Tests/CI: `bazel test //...` re-enabled (roadmap bounded task) — describe what runs where.
- Known Rough Edges: add "hash family split (SHA-256 roots / BLAKE3 row-id, LtHash)", "row profile mvp-v0 (name-keyed)", "single safe replica until Spec D".

## 5. housegate `pkg/replay/AGENTS.md`

Replace the "type aliases forwarding to arbiter-core" text with the truth: `pkg/replay`, `payloadexec`, `chexec` are the canonical implementations consumed by arbiter-core; wire form is mirrored in arbiter-proto `replay.proto` with a field-name freeze enforced by arbiter-core's conformance tests; keep the invariants list (canonical digest profile, root domains, `_hg_row_id` rules).

## 6. rewriter repos and contracts

- rewriter-proto `proto/rewriter.proto:148-178`: dotted naming text + examples (Spec E D-doc); note the trailing separator is a literal `.`.
- rewriter-grpc `README.md:24,99`: same fix; `CLAUDE.md`: profile split (after E).
- compute-network-contracts `CLAUDE.md`: Databases blurb gains schema declarations (`setTableSchema`, `TableSchemaSet`, `Types.TableSchema`, contract-owned version cursor); Shared Types lists `TableSchema`.
- arbiter `README.md`: after A/C/D add the v2 envelope, `--ensure-tables`, safe-replica role and resume semantics; state that `tables` inline mode is removed once 07-30 lands.

## 7. Acceptance

Each edit above is a checkbox in the implementing PR(s); zh-CN regenerated from v4 (no hard wrap); links resolve; the v4 doc contains no statement contradicted by the code at the referenced base commits (reviewer spot-checks §7, §8, §12.2 against arbiter `fsm/state.go`, housegate `pkg/replay/roots.go`, arbiter-core `snode/promote_replace.go`).
