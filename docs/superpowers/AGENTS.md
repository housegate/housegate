# SUPERPOWERS DOCS GUIDE

## OVERVIEW
Design specs and implementation plans moved to the maintainers' internal design-docs repository (see the root `CLAUDE.md`, "Design docs"). `docs/superpowers/` now holds only the multi-source storage-integrity **stage-3 plan set**, temporarily: Plans S3-A … S3-F are also the `s3-ledger.py` release ledger (each plan's Appendix C), and the ledger reads them from `origin/main` here. The set moves to the internal repository once Plan S3-F lands.

## STRUCTURE
```
docs/superpowers/
|-- specs/   # 2026-10-14 stage-3 database-groups design (English + zh-CN)
`-- plans/   # Plans S3-A … S3-F (ledger files), s3cf scoping/status/names index, stage-3 handoff, decisions (s3c3 member image, D41+)
```

## CONVENTIONS
- New stage-3 plans, decisions and amendments keep going here until the set moves; anything else goes to the internal design-docs repository, never here.
- These files were deliberately left byte-for-byte unchanged by the docs move (the ledger and running executions depend on them), so their links to documents that already moved now dangle: look the target up by file name in the internal repository. They are rewritten when the set moves.
- A ledger row is filled only by the record procedure of the plan that defines it (`s3-ledger.py set`), never by hand.
- English is the source of truth for bilingual specs; regenerate the Chinese side from the English meaning instead of patching it independently.
- Keep protocol identifiers in English: `statement_id`, `statement_seq`, `_hg_row_id`, `payload_hash`, `state_root`.
- Use one paragraph per line. Do not hard-wrap new English or CJK prose.

## COMMANDS
```bash
git diff --check -- docs/superpowers
```
