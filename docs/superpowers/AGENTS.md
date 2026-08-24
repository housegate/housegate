# SUPERPOWERS DOCS GUIDE

## OVERVIEW
`docs/superpowers` holds design specs and implementation plans. These docs often preserve historical decisions, so edit them as source-controlled design artifacts, not casual notes.

## STRUCTURE
```
docs/superpowers/
|-- specs/   # design specs, sometimes bilingual English + zh-CN
`-- plans/   # implementation plans and historical execution notes
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Current storage integrity design | `specs/2026-06-22-storage-integrity-design.md` | Promotion shadow table and `REPLACE PARTITION` direction. |
| Multi-replica trust design | `specs/2026-06-10-multi-replica-trust-design.md` | Replay-first semantics, `_hg_row_id`, safe/unsafe boundary. |
| Peer trust and routing | `specs/2026-04-28-peer-trust-design.md`, `specs/2026-04-28-two-port-server-mode.md` | `__route__`, `__peer__`, internal listener posture. |
| ClickHouse packet architecture | `specs/2026-04-21-clickhouse-tcp-conn-interface-design.md` | Codec/relay invariants and legacy migration context. |
| Deprecated docs | `../deprecated/` | Historical context only; do not treat as current behavior. |

## CONVENTIONS
- English is the source of truth for bilingual specs.
- For Chinese updates, regenerate from the English technical meaning instead of patching independently.
- Keep protocol identifiers in English when translation would reduce precision: `statement_id`, `statement_seq`, `_hg_row_id`, `payload_hash`, `state_root`.
- Use one paragraph per line. Do not hard-wrap new English or CJK prose.
- Design claims should match current code or explicitly say they are north-star / future-state.

## ANTI-PATTERNS
- Do not explain safe/unsafe routing as Keeper labels or `_part` filtering inside one mixed table when the intended boundary is physical isolation.
- Do not patch only the Chinese side of a bilingual spec when English semantics changed.
- Do not import plan text into current docs without checking whether the plan was superseded by code.
- Do not turn design-candidate object names into current-code facts unless the repo has adopted them.

## COMMANDS
```bash
git diff --check -- docs/superpowers
rg -n 'statement_id|statement_seq|_hg_row_id|payload_hash|state_root|REPLACE PARTITION|promotion shadow' docs/superpowers/specs
```

## NOTES
- Specs before June 2026 may contain line-wrapped paragraphs; do not imitate that style in new edits.
- The canonical public north-star architecture is in the external `housegate/docs` repo; this repo's `CLAUDE.md` describes the current implementation.
