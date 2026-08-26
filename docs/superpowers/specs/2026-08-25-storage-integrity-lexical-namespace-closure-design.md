# Storage-Integrity Surface: Lexical and SHOW-Namespace Closure

**Date:** 2026-08-25 **Status:** Implemented (Parts A-D; the release tag is Spec O's step 1) **Roadmap:** [closure roadmap](2026-08-25-storage-integrity-closure-roadmap.md) Spec N. **Remediates:** [Spec I surface fail-closed](2026-08-19-storage-integrity-surface-failclosed-design.md) — findings from the 2026-08-25 verification review, each reproduced against the shipped scanner and the live v0.9.0 native engine. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.1, §6, §11, §12.2. **Code base:** housegate `6fd56b8` (v0.11.0) plus open PR #141 `feature/si-surface-failclosed-housegate`, rewriter-go `23687cc` (v0.9.0), rewriter-grpc `a8ca4e7` (v0.13.0+1), rewriter-proto `19d90fc` (v0.6.0). **Source of truth:** English version.

## 1. Problem

Spec I built three independent layers: an engine catch-all (D1), HouseGate's unconditional fail-closed on any non-`Success` (D3), and an operator-session guard that refuses on *mention* of a reserved name (D6/D7e, `pkg/plugins/sireserved`). The verification review found a hole in two of the three, and that the mechanism meant to prove the two engines agree cannot see either. Planning this spec then found a third, more severe than both (1e): the engine's namespace gate itself is defeated by a tagged heredoc, with no privileged session required. All four share one shape — **something reads a value that a later stage interprets differently**, whether that stage is the comment lexer, the SHOW classifier, or the SQL generator.

**1a — heredoc literals blank the operator guard (Critical).** `sireserved`'s `scanSQLSurfaces` (`pkg/plugins/sireserved/plugin.go:115-165` on the PR branch) models `'…'`, `` `…` ``, `"…"`, `--`, `#`, `#!`, `//` and nested `/* */`. It does not model ClickHouse's heredoc string literal (`$$…$$`, `$tag$…$tag$`). A `$` falls to the byte-copying `default:` branch, so a `--`, `#` or `//` **inside** a heredoc is treated as a real comment and the rest of the statement is blanked from both scan surfaces. The reviewer extracted the shipped scanner verbatim and ran it, and confirmed against the real polyglot grammar that each statement is valid ClickHouse:

| Statement | Guard verdict | Engine |
|---|---|---|
| `SELECT $$--$$ AS x, count() FROM hg_safe.db1__t` | no violation | valid, accessed `hg_safe.db1__t` |
| `SELECT $$#$$ AS x, _hg_row_id FROM hg_unsafe.db1__t` | no violation | valid |
| `INSERT INTO ordinary.t SELECT $$--$$, a FROM hg_safe.db1__t` | no violation | valid — exports safe state to an ordinary table |
| `SELECT $tag$//$tag$ AS x, count() FROM hg_safe.db1__t` | no violation | valid — the tagged form bypasses too |
| control: `SELECT count() FROM hg_safe.db1__t` | **refused** (correct) | — |
| control: `SELECT * FROM merge($$hg_safe$$, '^db1__t$')` | **refused** (correct, accidentally — see D1) | — |

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

`ParseDBLevel` measured against the live v0.9.0 engine, which fixes exactly what each part of D2 has to do:

| SQL | `ShowWhat` | `DB` | `DBResolved` |
|---|---|---|---|
| `SHOW COLUMNS FROM db1__t FROM hg_safe` | `COLUMNS` | `db1__t` | true |
| `SHOW COLUMNS FROM hg_safe.db1__t` | `COLUMNS` | `hg_safe` | true |
| `SHOW EXTENDED COLUMNS FROM db1__t FROM hg_safe` | `EXTENDED` | `` | false |
| `SHOW FULL COLUMNS FROM db1__t IN hg_unsafe` | `COLUMNS` | `db1__t` | true |
| `SHOW INDEX FROM hg_safe.db1__t` | `INDEX` | `hg_safe` | true |
| `SHOW INDEXES FROM db1__t FROM hg_safe` | `INDEXES` | `db1__t` | true |
| `SHOW KEYS FROM db1__t FROM hg_safe` | `KEYS` | `db1__t` | true |
| `SHOW MERGES` | `MERGES` | `` | false |
| `SHOW SOMETHINGNEW FROM hg_safe` | `SOMETHINGNEW` | `hg_safe` | true |

Three consequences. The **qualified** form already binds the right database — `SHOW INDEX FROM hg_safe.db1__t` gives `DB = hg_safe` — so for that shape the whole fix is at the handler, and the parser needs no change. The **two-`FROM`** form binds the table into `DB`, so the parser genuinely has to learn the reversed grammar. And an unknown kind parses cleanly with its database bound, so the catch-all in D2 is implementable and would have caught this class.

**1c — the corpus cannot see engine divergence (Medium).** Spec J made all 41 `Success` cases pin their SQL and grew the corpus to 210, which is a large improvement. But byte-identical JSON in two repos only proves the two runners are fed the same inputs; it does not prove the two engines *produce* the same output on anything the corpus omits. `REWRITER_ORACLE_ADDR` — the cross-engine differential harness — exists and is wired into `TestStorageIntegrityGolden`, and 1b is exactly the kind of divergence it would catch. There is no record of it having been run over the full corpus since Spec I.

**1e — a tagged heredoc defeats the engine's namespace gate outright (Critical).** Found while planning this spec, and the most severe item in the round: it is in the engine, not the operator guard, so **no maintenance or platform-operator marker is required — any authenticated user**. Polyglot encodes a heredoc literal as `literal_type: "dollar_string"`, and for the tagged form it packs the tag into the value as `<tag>\x00<body>`. `tableFunctionArgValue` (`internal/engine/nodes.go:1905-1913`) reads `lit["value"]` without ever consulting `literal_type`, so the namespace decoder is handed `tag\x00hg_safe` and matches nothing. `Generate` then re-emits the same literal as an ordinary quoted string. Measured on v0.9.0 with SI configured:

```
Success  SELECT count() FROM merge($tag$hg_safe$tag$, 'db1__t')
         → emitted: SELECT count() FROM merge('hg_safe', 'db1__t')
Success  SELECT count() FROM remote('h', $tag$hg_unsafe$tag$, 'db1__t')
         → emitted: SELECT count() FROM remote('h', 'hg_unsafe', 'db1__t')
controls (all correctly rejected):
RewriteError  merge($$hg_safe$$, 'db1__t')      → "hg_safe.db1__t is not directly addressable"
RewriteError  merge('hg_safe', 'db1__t')        → same
RewriteError  remote('h', 'hg_unsafe', 'db1__t') → "hg_unsafe.db1__t is not directly addressable"
```

The statement that reaches ClickHouse is byte-for-byte the statement the gate would have refused. The **bare** `$$…$$` form encodes as a plain `{"literal_type":"dollar_string","value":"hg_safe"}` with no tag prefix, which is why it is caught — the hole is specific to the tagged form, and would have stayed invisible to any test that only tried `$$`.

The class of defect matters more than the instance: **policy inspects a raw AST field while the generator interprets it differently.** Any future `literal_type` polyglot adds re-opens it the same way.

**And the tagged heredoc was not the only live instance.** Implementing D6 turned up a second one, measured the same way: `e'hg_safe'` encodes as `{"literal_type":"escape_string","value":"e:hg_safe"}` and generates as `E'hg_safe'`. Policy is handed `e:hg_safe`, matches nothing, and ClickHouse receives a literal that *is* `hg_safe`. Ten more literal kinds — `number`, `hex_number`, `hex_string`, `bit_string`, `date`, `time`, `timestamp`, `datetime` among them — resolve to a value ClickHouse never sees as that string. Enumerating the instances was never going to be the fix; the whitelist in D6 is, and it closes all of them at once. This is the second time in this spec that a negative rule ("anything I do not recognise is harmless") turned out to be the bug — the first being §1b's SHOW classification.

**1d — external-connector table functions are outside the namespace gate (Medium).** Spec G's namespace decoder covers the local and cluster families thoroughly — `remote`, `remoteSecure`, `cluster`, `clusterAllReplicas`, `merge`, `loop`, `dictionary`, the `timeSeries*` and `prometheus*` families (`internal/engine/nodes.go:277-300`). It does not cover the foreign-connector family whose signature also carries an explicit `(database, table)` pair: `mysql`, `postgresql`, `mongodb`, `sqlite`, `redis`, `jdbc`, `odbc`. Because a `SELECT` *is* modelled, the D1 catch-all never fires for them. ClickHouse ships its own MySQL and PostgreSQL wire listeners (9004 / 9005), so `SELECT * FROM mysql('127.0.0.1:9004', 'hg_safe', 'db1__t', …)` is a loopback into the protected namespace. It is credential-gated — the attacker needs a ClickHouse account on that port — which is why this is Medium rather than Critical, but the gate is supposed to be the thing that does not depend on a second credential boundary.

## 2. Goals / non-goals

**Goals.** Complete `sireserved`'s lexical model so no reserved name can hide in a span the scanner does not understand. Make the engine's namespace decoder read the value the generator will emit, and refuse a literal kind it does not model. Replace the SHOW family's negative classification with an explicit positive one that fails closed on an unknown kind, and gate every SHOW variant that names a database or a table. Prove the two engines agree by executing the differential harness over the whole corpus rather than by asserting that they share a JSON file. Extend the namespace decoder to the foreign-connector table-function family.

**Non-goals.** Metadata confidentiality. `system.parts`, `system.tables`, `system.columns` and `system.merges` expose SI physical database, table and part names to any authenticated reader, and this spec does not change that — the SI property is integrity (nothing mutates a protected namespace, and no read escapes the safe/unsafe rewrite), not secrecy of the physical layout. `SHOW MERGES`, `SHOW CLUSTERS`, `SHOW CACHES` and `SHOW SETTINGS` name no object and stay pass-through for the same reason; gating them while `SELECT … FROM system.merges` stays open would be theatre. §6 records the debt. Also out of scope: replacing `sireserved`'s hand-rolled scanner with a real tokenizer (see §6), and the peer-trust / forwarded-session exemption, which Spec I D6 already recorded as a network-isolation requirement.

## 3. Decisions

### D1 — `sireserved` models ClickHouse heredocs, and refuses a `$` that does not open one

`scanSQLSurfaces` gains a heredoc case ahead of the `default:` branch:

- The guard recognizes a heredoc opener as a `$` followed by a (possibly empty) tag matching `[A-Za-z_][0-9A-Za-z_]*` and a closing `$`. The span ends at the next occurrence of the identical `$tag$`. An unterminated heredoc is an error, exactly like an unterminated `'…'` or `/* … */`.

  **This charset is a deliberately narrower subset, not the grammar.** An earlier draft of this bullet stated it as ClickHouse's rule; measured against the live engine, the real grammar is wider — `$1t$x$1t$`, `$1$x$1$` and the non-ASCII `$tä$x$tä$` all parse as heredocs and generate as `'x'`. Narrower is the safe direction *only because* of the next bullet: an opener the guard does not recognize falls through to the stray-`$` refusal rather than being copied through. The divergence is pinned by a test, so widening the subset has to be a deliberate act.
- The heredoc body is a **string literal**, so it is blanked from `outsideLiterals` and written verbatim to `withLiterals`. This is not optional, and it is a live regression risk rather than a hypothetical one: `merge($$hg_safe$$, '^db1__t$')` is **refused today**, because the body carries no comment marker and `hg_safe` survives on `withLiterals` as ordinary bytes. A fix that blanks heredoc bodies from both surfaces would turn a currently-caught statement into a bypass — strictly worse than the hole being closed.
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

**Executed. The record:**

```
Spec N D3 cross-engine differential - 2026-08-25
  rewriter-grpc   1faaf96 (feature/si-lexical-namespace-closure)
  rewriter-go     f1e8626 (feature/si-lexical-namespace-closure)
  oracle binary   sha256 5b5c6fc24d97e4d13b19ba32b23c0609c2ab42a91cffb1791d66f9d1b87b4940
  suites          StorageIntegrity 237/237 - Writes 36/36 - Phase4 25/25 -
                  DBLevel 17/17 - Select 15/15 - Errmsg 4/4  = 334 cases, 338 oracle RPCs
  divergences     0
  corpus          sha256 321cd51d515bdebb01228f2223481f3b1e2dc667c3406a4a3652e346b14cef78
                  232837 bytes - 237 cases - fnv1a64 4366038644618079701
  C++ suite       565/565
```

Two things the run established that the spec could not have asserted in advance.

**C++ never had the 1e defect, and the reason is structural.** ClickHouse's lexer materializes a heredoc into a plain `ASTLiteral` String, so its policy already reads the value its formatter emits — which is exactly the invariant D6 makes the Go engine hold. All four heredoc corpus cases were green on the unmodified C++ binary with no code change, and `e'…'` does not exist in ClickHouse's grammar at all (`SyntaxError`). So 1e was a **Go-only** Critical on which the two engines silently disagreed, and nothing but an executed differential could have surfaced it. That is the argument for §6's "automate this in CI" debt, restated as evidence.

**The differential's field set is narrower than "the engines agree."** `harness.Compare` diffs `code`, `statement_type`, `existence_clause`, `storage_integrity_contract_version`, `table_rewrites`, `database_rewrites`, `failed_cte_aliases`, `privileges_deltas` and `sql_after_rewrite`. It does **not** diff `message` or `original_accessed_tables`; those reach parity only through each engine asserting the same corpus `want_message_contains` / `want_accessed` locally, which covers only the cases that set those keys. "Zero divergences" is a statement about the diffed fields, and §6 records the gap.

### D4 — the namespace decoder covers foreign-connector table functions

`decodeNamespaceFunctionRefDetail` gains the connector family whose signature carries an explicit ClickHouse-shaped `(database, table)` pair after a connection argument: **`mysql`, `postgresql`, `mongodb`, `jdbc`, `odbc`**. They inherit the existing unresolvable-argument rule: an argument that is not a static string literal is a rejection, not a pass (`si_remote_unresolved_namespace_rejected` is the precedent).

**`sqlite` and `redis` are excluded**, correcting an earlier draft of this decision. Read from the ClickHouse docs while planning: `redis(host:port, key, structure, …)` — `key` is *a column name*, not a table; `sqlite(db_path, table_name)` — a table inside a SQLite file, not a ClickHouse namespace. Neither names anything the gate protects, and including them would flip `sqlite('/tmp/x.db','db1__t')` from its measured `Success` into a rejection for no security benefit. The corpus pins `si_redis_column_name_allowed` so this cannot be "tidied up" back in later.

**Decoding is by arity, not a flat "pair at index 1"** — also a correction. `mongodb` accepts a `(uri, collection, structure…)` form as well as the database-bearing one; `jdbc` and `odbc` accept both 2- and 3-argument forms; and all five accept a named-collection form whose namespace is not in the argument list at all. Each shape is measured first and pinned, and the task stops rather than guessing if any named-collection form returns `Success` while naming `hg_safe`.

Object-storage and file connectors (`s3`, `url`, `hdfs`, `azureBlobStorage`, `file`, `iceberg`, `deltaLake`) are **not** included: they address paths, not `(database, table)` pairs, so there is no reserved name for the gate to match. They remain covered by whatever ClickHouse-level restrictions the deployment sets.

### D6 — the namespace decoder reads the value the generator will emit, and refuses a literal kind it does not model

Two changes, because the instance and the class need different fixes.

**The instance:** `tableFunctionArgValue` consults `literal_type`. `"string"` is used as today; `"dollar_string"` is decoded by taking everything after the first `\x00` when one is present, and the whole value when it is not. Verified encodings: `$tag$hg_safe$tag$` → `{"literal_type":"dollar_string","value":"tag\u0000hg_safe"}`; `$$hg_safe$$` → `{"literal_type":"dollar_string","value":"hg_safe"}`; `'hg_safe'` → `{"literal_type":"string","value":"hg_safe"}`. And — the second live instance — `e'hg_safe'` → `{"literal_type":"escape_string","value":"e:hg_safe"}`, generated as `E'hg_safe'`.

`escape_string` is deliberately **not** decoded. Its escape grammar is ClickHouse's, and re-implementing it here would rebuild the partial-parser bypass class that D6 exists to escape. It resolves to unknown and is refused, which is both correct and cheap: the measured refusal is `storage-integrity table function namespace is not statically resolvable`.

**The class:** a `literal_type` the decoder does not model is **not** treated as an opaque non-namespace value — that is precisely how 1e happened. Under an active SI contract it resolves to `namespaceValueUnknown`, which the existing machinery already turns into a rejection (the precedent is `si_remote_unresolved_namespace_rejected`, where a non-static argument is refused rather than assumed harmless).

**The proof that the class is closed:** a test that, for every `literal_type` polyglot can emit in a table-function argument position, asserts that the value policy sees equals the value `Generate` emits — or that policy refuses. That equality is the actual invariant; the `dollar_string` decode is just today's instance of it. Without this test the next literal kind reopens the hole silently.

rewriter-grpc uses ClickHouse's own AST rather than polyglot's JSON, so it may not share the defect. The plan measures it on the remote box rather than assuming either way, and the corpus pins the outcome for both engines.

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

Ships as three PRs. rewriter-go and rewriter-grpc land D2/D4/D5/D6 together with the corpus (they are one behaviour in two repos), then rewriter-go cuts a tag. HouseGate lands D1 **onto PR #141's branch** — #141 must not merge without it, since D1 fixes a hole #141 itself introduces. D3's run happens between the two.

**Do not hard-code the rewriter-go version.** "v0.9.1" elsewhere in this spec set is a prediction, not an instruction: `scripts/next-version.sh` bumps the patch component only when the previous tag's date is today in `Asia/Shanghai`, and `v0.9.0` is dated 2026-08-25, so a cut on any later day yields **v0.10.0**. The plan records whatever the release workflow prints, and Spec O consumes that value rather than a literal.

## 6. Out of scope / recorded debt

- **`system.*` metadata exposure.** SI physical database, table and part names are readable through `system.tables` / `system.parts` / `system.columns` / `system.merges` by any authenticated user. Closing it means an allowlist over the `system` database, which breaks ordinary client introspection; it is a confidentiality property the v1 design never claimed. Revisit if SI is ever deployed to a multi-tenant read surface.
- **`sireserved`'s hand-rolled scanner.** D1 completes its lexical model but the structure — a byte loop maintained by hand against a moving SQL dialect — is the same structure that produced 1a. The durable answer is to run the polyglot tokenizer over the statement and scan tokens, which is available in-process wherever the native engine is configured. Blocked on operator sessions being exactly the sessions where the rewriter is deliberately absent; needs its own design.
- **Cross-engine differential in CI.** D3 is a manual gate because rewriter-grpc builds only on the remote box. Automating it needs a published rewriter-grpc image or a remote CI runner. The 2026-08-25 run is the argument for doing it: it is what proved 1e was Go-only, and a shared JSON file could never have shown that.
- **`harness.Compare` does not diff `message` or `original_accessed_tables`.** Two engines could return the same code with different operator-facing text and the differential would stay green. Widening the diff is cheap; deciding what counts as an acceptable message difference is not, which is why it is debt rather than a decision here.
- **`SHOW MERGES` and friends.** Deliberately pass-through per §2; reconsider only together with the `system.*` decision.
