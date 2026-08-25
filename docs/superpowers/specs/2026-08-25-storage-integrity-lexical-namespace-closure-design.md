# Storage-Integrity Surface: Lexical and SHOW-Namespace Closure

**Date:** 2026-08-25 **Status:** Proposed **Roadmap:** [closure roadmap](2026-08-25-storage-integrity-closure-roadmap.md) Spec N. **Remediates:** [Spec I surface fail-closed](2026-08-19-storage-integrity-surface-failclosed-design.md) — findings from the 2026-08-25 verification review, each reproduced against the shipped scanner and the live v0.9.0 native engine. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.1, §6, §11, §12.2. **Code base:** housegate `6fd56b8` (v0.11.0) plus open PR #141 `feature/si-surface-failclosed-housegate`, rewriter-go `23687cc` (v0.9.0), rewriter-grpc `a8ca4e7` (v0.13.0+1), rewriter-proto `19d90fc` (v0.6.0). **Source of truth:** English version.

## 1. Problem

Spec I built three independent layers: an engine catch-all (D1), HouseGate's unconditional fail-closed on any non-`Success` (D3), and an operator-session guard that refuses on *mention* of a reserved name (D6/D7e, `pkg/plugins/sireserved`). The verification review found that two of the three have a hole, and that the mechanism meant to prove the two engines agree cannot see either hole.

**1a — heredoc literals blank the operator guard (Critical).** `sireserved`'s `scanSQLSurfaces` (`pkg/plugins/sireserved/plugin.go:115-165` on the PR branch) models `'…'`, `` `…` ``, `"…"`, `--`, `#`, `#!`, `//` and nested `/* */`. It does not model ClickHouse's heredoc string literal (`$$…$$`, `$tag$…$tag$`). A `$` falls to the byte-copying `default:` branch, so a `--`, `#` or `//` **inside** a heredoc is treated as a real comment and the rest of the statement is blanked from both scan surfaces. The reviewer extracted the shipped scanner verbatim and ran it, and confirmed against the real polyglot grammar that each statement is valid ClickHouse:

| Statement | Guard verdict | Engine |
|---|---|---|
| `SELECT $$--$$ AS x, count() FROM hg_safe.db1__t` | no violation | valid, accessed `hg_safe.db1__t` |
| `SELECT $$#$$ AS x, _hg_row_id FROM hg_unsafe.db1__t` | no violation | valid |
| `INSERT INTO ordinary.t SELECT $$--$$, a FROM hg_safe.db1__t` | no violation | valid — exports safe state to an ordinary table |
| control: `SELECT count() FROM hg_safe.db1__t` | **refused** (correct) | — |

`sireserved` is the *only* control on this path: maintenance and platform-operator sessions skip rewrite by design (Spec I D6), and the SI ingress short-circuits when the accessed-table set is empty. Refuse-on-mention was adopted in Spec I precisely to escape the parser-gap bypass class; it carries exactly one obligation — a complete lexical model of every span in which a reserved name can hide — and one span is missing.

**1b — `SHOW COLUMNS` / `INDEX` / `INDEXES` / `KEYS` escape the engine namespace gate, and the two engines diverge (High).** `rewriter-go/internal/handlers/dblevel.go:266-278` carries the comment *"The remaining SHOW variants have no database target and must not inherit the current SI context accidentally"*, and pass-throughs everything whose `ShowWhat != "TABLES"` except `DICTIONARIES`. The premise is false: `ShowWhat` is just the first uppercased token after `SHOW` (`internal/engine/dblevel.go:81`), and ClickHouse's `SHOW [EXTENDED] [FULL] COLUMNS {FROM|IN} <table> [{FROM|IN} <database>]` and `SHOW [EXTENDED] INDEX|INDEXES|KEYS {FROM|IN} <table> [{FROM|IN} <database>]` both carry a database *and* a table target. Measured on the v0.9.0 engine with SI configured:

```
Success              SHOW COLUMNS FROM db1__t FROM hg_safe   → SQL unchanged
Success              SHOW INDEX FROM hg_safe.db1__t          → unchanged
Success              SHOW KEYS FROM db1__t FROM hg_safe      → unchanged
control:
UnsupportedStatement SHOW TABLES FROM hg_safe                → "physical database hg_safe is not directly addressable"
```

`statement_type` is `SHOW_TABLES`, so these never reach `native.go:250`'s D1 catch-all. Any ordinary authenticated user — no maintenance or platform-operator marker needed — reads the SI physical schema including the `_hg_row_id` column name. The C++ engine fails closed on the same input because its `ASTShowColumnsQuery` cast fails and it drops to its catch-all, so **the two engines disagree** on live inputs, and the shared corpus has zero coverage of any of these forms (7 SHOW cases out of 210, all `TABLES` / `DICTIONARIES` / `CREATE`).

Two structural causes sit behind the single wrong comment. First, the SHOW-kind classification is a **negative** rule (`!= "TABLES"` → assumed target-less), so every SHOW variant ClickHouse adds lands in the target-less bucket by default. Second, the prefix set is incomplete: `EXTENDED` is not modelled, so `SHOW EXTENDED COLUMNS …` classifies as kind `EXTENDED`.

**1c — the corpus cannot see engine divergence (Medium).** Spec J made all 41 `Success` cases pin their SQL and grew the corpus to 210, which is a large improvement. But byte-identical JSON in two repos only proves the two runners are fed the same inputs; it does not prove the two engines *produce* the same output on anything the corpus omits. `REWRITER_ORACLE_ADDR` — the cross-engine differential harness — exists and is wired into `TestStorageIntegrityGolden`, and 1b is exactly the kind of divergence it would catch. There is no record of it having been run over the full corpus since Spec I.

**1d — external-connector table functions are outside the namespace gate (Medium).** Spec G's namespace decoder covers the local and cluster families thoroughly — `remote`, `remoteSecure`, `cluster`, `clusterAllReplicas`, `merge`, `loop`, `dictionary`, the `timeSeries*` and `prometheus*` families (`internal/engine/nodes.go:277-300`). It does not cover the foreign-connector family whose signature also carries an explicit `(database, table)` pair: `mysql`, `postgresql`, `mongodb`, `sqlite`, `redis`, `jdbc`, `odbc`. Because a `SELECT` *is* modelled, the D1 catch-all never fires for them. ClickHouse ships its own MySQL and PostgreSQL wire listeners (9004 / 9005), so `SELECT * FROM mysql('127.0.0.1:9004', 'hg_safe', 'db1__t', …)` is a loopback into the protected namespace. It is credential-gated — the attacker needs a ClickHouse account on that port — which is why this is Medium rather than Critical, but the gate is supposed to be the thing that does not depend on a second credential boundary.

## 2. Goals / non-goals

**Goals.** Complete `sireserved`'s lexical model so no reserved name can hide in a span the scanner does not understand. Replace the SHOW family's negative classification with an explicit positive one that fails closed on an unknown kind, and gate every SHOW variant that names a database or a table. Prove the two engines agree by executing the differential harness over the whole corpus rather than by asserting that they share a JSON file. Extend the namespace decoder to the foreign-connector table-function family.

**Non-goals.** Metadata confidentiality. `system.parts`, `system.tables`, `system.columns` and `system.merges` expose SI physical database, table and part names to any authenticated reader, and this spec does not change that — the SI property is integrity (nothing mutates a protected namespace, and no read escapes the safe/unsafe rewrite), not secrecy of the physical layout. `SHOW MERGES`, `SHOW CLUSTERS`, `SHOW CACHES` and `SHOW SETTINGS` name no object and stay pass-through for the same reason; gating them while `SELECT … FROM system.merges` stays open would be theatre. §6 records the debt. Also out of scope: replacing `sireserved`'s hand-rolled scanner with a real tokenizer (see §6), and the peer-trust / forwarded-session exemption, which Spec I D6 already recorded as a network-isolation requirement.

## 3. Decisions

### D1 — `sireserved` models ClickHouse heredocs, and refuses a `$` that does not open one

`scanSQLSurfaces` gains a heredoc case ahead of the `default:` branch:

- A `$` opens a heredoc when it is followed by a (possibly empty) tag matching `[A-Za-z_][0-9A-Za-z_]*` and a closing `$`. The span ends at the next occurrence of the identical `$tag$`. An unterminated heredoc is an error, exactly like an unterminated `'…'` or `/* … */`.
- The heredoc body is a **string literal**, so it is blanked from `outsideLiterals` and written verbatim to `withLiterals`. This is not optional: ClickHouse table functions interpret literal arguments as identifiers, so `merge($$hg_safe$$, '^db1__t$')` must still be caught by the `withLiterals` surface. A heredoc body that lands on neither surface would be a new bypass, strictly worse than the one being fixed.
- ClickHouse performs **no escape processing** inside a heredoc: `$$hg\x5Fsafe$$` is literally `hg\x5Fsafe` and is not `hg_safe`. Heredoc bodies therefore do **not** inherit the backslash refusal that `consumeStringLiteral` applies to `'…'`. The implementation must confirm this against the real grammar before relying on it; if the grammar disagrees, refuse backslash-bearing heredocs the same way.
- A `$` that does not open a well-formed heredoc is **refused**, with the same error shape as the existing lexical refusals. Rejecting is correct rather than merely conservative: outside a heredoc opener and a quoted span, `$` is not part of any identifier or operator this guard needs to admit, and the alternative — copying it through — is what created 1a.

### D2 — the SHOW family is classified positively, and every target-bearing variant is gated

`internal/engine/dblevel.go`'s SHOW parsing gains `EXTENDED` as a prefix alongside `FULL` and `TEMPORARY`, in ClickHouse's fixed order (`SHOW [EXTENDED] [FULL] …`), and models the two-target grammar: for the `COLUMNS` / `INDEX` / `INDEXES` / `KEYS` kinds the **first** `FROM|IN` names the *table* and the optional **second** `FROM|IN` names the *database* — the opposite of the `TABLES` / `DICTIONARIES` grammar the current parser assumes. Today `SHOW COLUMNS FROM db1__t FROM hg_safe` binds `DB = "db1__t"`, which is the table.

`internal/handlers/dblevel.go` replaces `if info.ShowWhat != "TABLES"` with an explicit three-way classification:

| Class | Kinds | Behaviour under an active SI contract |
|---|---|---|
| **Rewritten** | `TABLES` | unchanged (existing synthetic `system.tables` enumeration + namespace gate) |
| **Target-bearing pass-through** | `DICTIONARIES`, `COLUMNS`, `INDEX`, `INDEXES`, `KEYS` | namespace gate on the resolved database *and*, for the `COLUMNS`/`INDEX` family, on the resolved `(database, table)` pair; pass through only once both are proved ordinary |
| **Target-less pass-through** | `CLUSTER`, `CLUSTERS`, `SETTINGS`, `MERGES`, `CACHES`, `PROCESSLIST`, `FUNCTIONS`, `GRANTS`, `USERS`, `ROLES`, `ROW`, `QUOTA`, `QUOTAS`, `PROFILES`, `POLICIES`, `ACCESS`, `ENGINES`, `FILESYSTEM` | pass through (they name no database or table) |

`CREATE` keeps its existing separate handling (`dblevel.go:46`). **Any kind in none of the three lists is a rejection** when the request carries a non-empty `storage_integrity.tables` — the same catch-all shape as Spec I D1, so the next SHOW variant ClickHouse adds is safe by default instead of silently target-less. The target-less list is an explicit allowlist that must be justified case by case in review, not a residue.

The gate reuses the existing helper shape and message vocabulary: `recordAccessedDatabase` before rejecting (so HouseGate's SI-flag path sees the object), `StorageIntegrityPhysicalDatabaseRejectMessage` / `StorageIntegrityPhysicalRejectMessage`, and `rejectUnresolvedShowDatabase` for a target that is present but not statically resolvable. Nothing new is invented.

An unqualified `SHOW COLUMNS FROM t` inherits `upstream_physical_database_in_context`, which is checked exactly as `rejectShowDictionariesStorageIntegrityNamespace` already checks it (`dblevel.go:338-347`).

rewriter-grpc implements the identical classification. Its current fail-closed behaviour on `SHOW COLUMNS` is accidental (a failed AST cast), so it changes from *accidentally right* to *deliberately right with the same message*, which is what makes the corpus able to pin it.

### D3 — the cross-engine differential harness is run over the full corpus, and its result is recorded

Before Spec N merges, `TestStorageIntegrityGolden` runs with `REWRITER_ORACLE_ADDR` pointed at a freshly built rewriter-grpc, over all corpus cases including the ones added here. Any divergence is either fixed or recorded as an explicit `allow_sql_divergence` case with a written reason. The run's date, rewriter-grpc commit and case count go into the spec's delivery record, because "the two halves are equal" is currently supported only by a shared JSON file and not by any execution.

This is a one-shot gate for this spec, not a new CI job: rewriter-grpc builds only on the remote box, so wiring it into CI is separate work (recorded in §6).

### D4 — the namespace decoder covers foreign-connector table functions

`decodeNamespaceFunctionRefDetail` gains the connector family whose signature carries an explicit `(database, table)` pair after a connection argument: `mysql`, `postgresql`, `mongodb`, `sqlite`, `redis`, `jdbc`, `odbc`. They decode exactly like `remote` — pair at argument index 1 — and inherit the existing unresolvable-argument rule: an argument that is not a static string literal is a rejection, not a pass (`si_remote_unresolved_namespace_rejected` is the precedent).

`sqlite` and `redis` take `(path|address, table)` rather than a database, so they decode as a single-name reference like `merge`'s one-argument form. The implementation must read each signature from the ClickHouse docs rather than assume, and pin each in the corpus.

Object-storage and file connectors (`s3`, `url`, `hdfs`, `azureBlobStorage`, `file`, `iceberg`, `deltaLake`) are **not** included: they address paths, not `(database, table)` pairs, so there is no reserved name for the gate to match. They remain covered by whatever ClickHouse-level restrictions the deployment sets.

### D5 — every new behaviour is pinned in the shared corpus

The corpus grows by roughly 30 cases: the four heredoc reproductions from 1a plus their controls (housegate-side unit tests, not corpus — `sireserved` is not a rewriter surface), the full `SHOW` matrix (each kind × qualified / two-`FROM` / context-inherited / ordinary-database control), `SHOW EXTENDED` prefix forms, one case per unknown-kind rejection, and one connector table function per D4 name in both the resolvable-physical and unresolvable-argument shapes. The Go copy is authoritative; the C++ copy is produced by `cp` and never hand-edited (Spec G plan D-10). No new schema key is introduced.

## 4. Testing / acceptance

1. **Heredoc bypass regression.** A `sireserved` unit test table containing each 1a statement, asserting refusal, plus the control statement, plus `merge($$hg_safe$$, …)` asserting the body reached `withLiterals`, plus an unterminated `$tag$` asserting an error, plus a bare `$` asserting refusal. Each must **fail against the pre-fix scanner** — the plan states this explicitly per case.
2. **SHOW matrix.** Corpus cases for D2, green in both engines. `SHOW COLUMNS FROM db1__t FROM hg_safe`, `SHOW INDEX FROM hg_safe.db1__t`, `SHOW KEYS FROM db1__t FROM hg_safe` and `SHOW EXTENDED COLUMNS FROM db1__t FROM hg_unsafe` all reject and name the object; an ordinary-database control passes through unchanged.
3. **Unknown-kind catch-all.** A corpus case using a SHOW kind in none of the three lists (e.g. `SHOW SOMETHINGNEW FROM hg_safe`) rejects with the D1 generic message.
4. **Connector functions.** One reject case per D4 name plus one ordinary-database pass-through control.
5. **Cross-engine diff.** `REWRITER_ORACLE_ADDR` run over the full corpus, zero unexplained divergences, recorded per D3.
6. **HouseGate integration.** The existing `pkg/integration/storage_integrity_read_test.go` gains the `SHOW COLUMNS` and heredoc statements against real ClickHouse 25.8, proving the rejection reaches the client as an exception rather than as a result set.

## 5. Delivery

Ships as three PRs. rewriter-go and rewriter-grpc land D2/D4/D5 together with the corpus (they are one behaviour in two repos), then rewriter-go cuts **v0.9.1**. HouseGate lands D1 **onto PR #141's branch** — #141 must not merge without it, since D1 fixes a hole #141 itself introduces. D3's run happens between the two.

Spec O consumes v0.9.1: it is the pin bump plus the #141 merge.

## 6. Out of scope / recorded debt

- **`system.*` metadata exposure.** SI physical database, table and part names are readable through `system.tables` / `system.parts` / `system.columns` / `system.merges` by any authenticated user. Closing it means an allowlist over the `system` database, which breaks ordinary client introspection; it is a confidentiality property the v1 design never claimed. Revisit if SI is ever deployed to a multi-tenant read surface.
- **`sireserved`'s hand-rolled scanner.** D1 completes its lexical model but the structure — a byte loop maintained by hand against a moving SQL dialect — is the same structure that produced 1a. The durable answer is to run the polyglot tokenizer over the statement and scan tokens, which is available in-process wherever the native engine is configured. Blocked on operator sessions being exactly the sessions where the rewriter is deliberately absent; needs its own design.
- **Cross-engine differential in CI.** D3 is a manual gate because rewriter-grpc builds only on the remote box. Automating it needs a published rewriter-grpc image or a remote CI runner.
- **`SHOW MERGES` and friends.** Deliberately pass-through per §2; reconsider only together with the `system.*` decision.
