# Schema Registry Phase A (CH-derived loader) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

> **Revision note (2026-07-29):** re-baselined after a wave of merges: sentio-node PR #163 (snode hosting) is on main including its known review findings; housegate merged PR #98 (storageintegrity runtime), #108/#109 (**canonical module path `github.com/housegate/housegate`**, tagged **v0.7.1**), and #110 (P1 production wiring — `GetStatementStatus` client + outcome convergence; no config/tables surface touched); arbiter-core is already on `github.com/housegate/housegate v0.7.1` with **no replace directive** (the public-module replace debt is gone). Task 3 therefore lands on a fresh sentio-node branch off main and fixes the inherited #163 findings there.

**Goal:** Kill hand-written column YAML now, per [schema-registry spec](../specs/2026-07-28-schema-registry-design.md) decision 8 Phase A: storage-integrity consumers keep only a `table_ids` selector and derive column-level `payloadexec.TableSchema` from the local ClickHouse (`system.columns`/`system.tables`), with the genesis `schema_root` anchor check unchanged.

**Architecture:** One new housegate package `pkg/schemaregistry` defines the loader seam (`Loader.Load(ctx, refs)`) that Phase B will re-implement over network state, plus the Phase-A `ClickHouseLoader`. arbiter-core's reference binaries and sentio-node's assembly consume it; `schema_root` validation stays where it is (`snode.Config.validate`), so a wrong derivation still refuses startup. The sentio-node change also structurally retires two findings PR #163 carried into main.

**Tech Stack:** Go 1.26; housegate is Bazel 8.5.1 + Bzlmod (gazelle-managed BUILD files, docker tests in `//pkg/integration:integration_test`) with module path `github.com/housegate/housegate` (canonical since #108 — all import paths in this plan use it); arbiter-core and sentio-node consume housegate as a normal public module (no replace, no GOPRIVATE needed for it).

**Repos & branches:**
- housegate `/Users/uranuswch/Dev/housegate/housegate` — branch `feat/schemaregistry-ch-loader` off origin/main (≥ `81092ae`, includes #98/#108/#110)
- arbiter-core `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` — branch `feat/ch-derived-schemas` off origin/main (≥ `33f7cef`, already on housegate v0.7.1)
- sentio-node `/Users/uranuswch/Dev/sentio_xyz/sentio-node` — branch `feat/schema-registry-phase-a` off origin/main (≥ `516f503`, PR #163 merged)

## Global Constraints

- The consensus anchor is untouched: schemas from any source still flow through `payloadexec.SchemaRoot(networkID, tables)` and must equal the configured genesis `schema_root` (validated in `arbiter-core/snode/config.go:67-71`; verifier likewise) — Phase A changes where columns COME FROM, never what is TRUSTED.
- Hash domains are reused, not invented (spec decision 1): no new digest anywhere in this plan.
- The loader excludes the `_hg_row_id` protocol column — `TableSchema.Columns` is user columns only (matches `chexec` scratch-table construction).
- The loader does NOT validate partition-key shape; `validatePartitionBy` (arbiter-core `snode/config.go:82`, bare-String-column MVP freeze) stays the sole gate and now also catches expression partition keys arriving from CH introspection.
- Physical table naming stays arbiter-core's single source of truth: export `snode.CHTableName` rather than copying `ReplaceAll(id, ".", "__")` into any consumer.
- All housegate imports use the canonical path (`github.com/housegate/housegate/pkg/...`) — the pre-#108 `housegate/housegate` form must not reappear.
- **Config break is declared, not shimmed:** `storage_integrity.snode.tables` (column YAML) is replaced by `table_ids` with no backward-compat alias — the entire storage_integrity stack is default-off and not yet in production, so the break lands while it is free. State the break in the sentio-node PR description.
- housegate rules: Bazel is ground truth (`bazel mod tidy && bazel run //:gazelle` after dep/package changes); new docker-bound tests join the existing `//pkg/integration:integration_test` target (already in ci.yml — do NOT create a new manual target); no hard line-wrapping in docs; `pkg/log` for logging.
- English comments/logs; `fmt.Errorf("context: %w", err)` wrapping.

---

## Phase A file map

```
housegate    pkg/schemaregistry/loader.go        TableRef, Loader, ClickHouseLoader
             pkg/schemaregistry/loader_test.go   unit (arg validation, no CH)
             pkg/integration/chschema_test.go    docker: derive vs real DDL, root stability
arbiter-core snode/parts.go                      export CHTableName
             cmd/arbiter-snode/{config,main}.go  table_ids | tables (exactly one), derive path
             cmd/arbiter-verifier/{config,main}.go  same
sentio-node  config/config.go                    StorageIntegritySNode.Tables → TableIDs
             standalone/standalone.go            derive before snode.New; resolver from derived set
             (new branch off main)               + fixes for the inherited #163 findings this intersects
```

---

### Task 1: housegate `pkg/schemaregistry` — the loader seam + ClickHouse implementation

**Files:**
- Create: `pkg/schemaregistry/loader.go`, `pkg/schemaregistry/loader_test.go`
- Create: `pkg/integration/chschema_test.go`
- Modify: BUILD files via gazelle (mechanical)

**Interfaces:**
- Consumes: `github.com/housegate/housegate/pkg/replay/payloadexec` (`TableSchema`), `github.com/housegate/housegate/pkg/lthash` (`Column`), `clickhouse-go/v2` (existing dep).
- Produces (Tasks 2–3 rely on these exact shapes):

```go
package schemaregistry

// TableRef names one storage-integrity table to load: the logical id that
// feeds TableSchemaHash, and the physical ClickHouse coordinates to
// introspect. Callers own the id→physical mapping (snode.CHTableName).
type TableRef struct {
    TableID  string
    Database string
    Table    string
}

// Loader is the schema-source seam (spec §5). Phase A implements it over
// the local ClickHouse; Phase B re-implements it over network state with
// hash verification — consumers never change.
type Loader interface {
    Load(ctx context.Context, refs []TableRef) ([]payloadexec.TableSchema, error)
}

type ClickHouseLoader struct{ conn clickhouse.Conn }

func NewClickHouseLoader(conn clickhouse.Conn) *ClickHouseLoader
```

- [ ] **Step 1: Write the failing unit test**

```go
package schemaregistry

import (
    "context"
    "strings"
    "testing"
)

func TestLoad_ValidatesRefs(t *testing.T) {
    l := NewClickHouseLoader(nil)
    for name, refs := range map[string][]TableRef{
        "empty set":     {},
        "no table id":   {{Database: "db", Table: "t"}},
        "no database":   {{TableID: "a", Table: "t"}},
        "no table":      {{TableID: "a", Database: "db"}},
        "duplicate ids": {{TableID: "a", Database: "db", Table: "t1"}, {TableID: "a", Database: "db", Table: "t2"}},
    } {
        if _, err := l.Load(context.Background(), refs); err == nil {
            t.Errorf("%s must fail before any query", name)
        }
    }
}

func TestLoad_NilConnFailsClosed(t *testing.T) {
    _, err := NewClickHouseLoader(nil).Load(context.Background(), []TableRef{{TableID: "a", Database: "db", Table: "t"}})
    if err == nil || !strings.Contains(err.Error(), "connection") {
        t.Fatalf("nil conn must fail with a pointed error, got %v", err)
    }
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/schemaregistry/ -v`
Expected: FAIL — package does not exist.

- [ ] **Step 3: Implement `loader.go`**

```go
// Package schemaregistry loads column-level storage-integrity table
// schemas from a source and hands them to consumers as
// payloadexec.TableSchema. The consensus anchor is NOT here: whatever a
// Loader returns still flows through payloadexec.SchemaRoot and must match
// the genesis schema_root, so a wrong source refuses startup instead of
// diverging replay roots (schema-registry design §2/§5).
package schemaregistry

const protocolRowIDColumn = "_hg_row_id"

func (l *ClickHouseLoader) Load(ctx context.Context, refs []TableRef) ([]payloadexec.TableSchema, error) {
    if err := validateRefs(refs); err != nil {
        return nil, err
    }
    if l.conn == nil {
        return nil, fmt.Errorf("schemaregistry: clickhouse connection is required")
    }
    out := make([]payloadexec.TableSchema, 0, len(refs))
    for _, ref := range refs {
        sch, err := l.loadOne(ctx, ref)
        if err != nil {
            return nil, fmt.Errorf("schemaregistry: table %s (%s.%s): %w", ref.TableID, ref.Database, ref.Table, err)
        }
        out = append(out, sch)
    }
    return out, nil
}

func (l *ClickHouseLoader) loadOne(ctx context.Context, ref TableRef) (payloadexec.TableSchema, error) {
    var partitionKey string
    row := l.conn.QueryRow(ctx,
        "SELECT partition_key FROM system.tables WHERE database = @db AND name = @table",
        clickhouse.Named("db", ref.Database), clickhouse.Named("table", ref.Table))
    if err := row.Scan(&partitionKey); err != nil {
        return payloadexec.TableSchema{}, fmt.Errorf("table not found or unreadable: %w", err)
    }
    rows, err := l.conn.Query(ctx,
        "SELECT name, type FROM system.columns WHERE database = @db AND table = @table AND name != @rowid ORDER BY position",
        clickhouse.Named("db", ref.Database), clickhouse.Named("table", ref.Table),
        clickhouse.Named("rowid", protocolRowIDColumn))
    if err != nil {
        return payloadexec.TableSchema{}, fmt.Errorf("list columns: %w", err)
    }
    defer rows.Close()
    var cols []lthash.Column
    for rows.Next() {
        var name, typ string
        if err := rows.Scan(&name, &typ); err != nil {
            return payloadexec.TableSchema{}, fmt.Errorf("scan column: %w", err)
        }
        cols = append(cols, lthash.Column{Name: name, Type: typ})
    }
    if err := rows.Err(); err != nil {
        return payloadexec.TableSchema{}, fmt.Errorf("iterate columns: %w", err)
    }
    if len(cols) == 0 {
        return payloadexec.TableSchema{}, fmt.Errorf("no user columns (does the table exist and carry more than %s?)", protocolRowIDColumn)
    }
    return payloadexec.TableSchema{TableID: ref.TableID, PartitionBy: partitionKey, Columns: cols}, nil
}
```

`validateRefs`: non-empty set; every field non-empty; `TableID` unique across the set (build a `map[string]bool`). Deliberately no partition-key shape check (Global Constraints).

- [ ] **Step 4: Write the docker integration test**

`pkg/integration/chschema_test.go`, following the harness conventions of the neighboring `chscan_test.go` (read it first for the CH connection helper and skip-gate; this file joins the same `//pkg/integration:integration_test` target so gazelle picks it up automatically):

```go
func TestClickHouseLoader_DerivesRealDDL(t *testing.T) {
    conn := requireClickHouse(t) // reuse the file's existing helper name — read chscan_test.go
    db, table := "hg_unsafe_chschema", "orders__t"
    mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
    mustExec(t, conn, "DROP TABLE IF EXISTS "+db+"."+table)
    mustExec(t, conn, `CREATE TABLE `+db+`.`+table+` (
        _hg_row_id FixedString(32),
        p String,
        v UInt64,
        note Nullable(String)
    ) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()`)
    t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db) })

    got, err := schemaregistry.NewClickHouseLoader(conn).Load(context.Background(),
        []schemaregistry.TableRef{{TableID: "orders.t", Database: db, Table: table}})
    if err != nil {
        t.Fatalf("load: %v", err)
    }
    want := payloadexec.TableSchema{TableID: "orders.t", PartitionBy: "p", Columns: []lthash.Column{
        {Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}, {Name: "note", Type: "Nullable(String)"},
    }}
    if !reflect.DeepEqual(got[0], want) {
        t.Fatalf("derived schema:\n got %+v\nwant %+v", got[0], want)
    }
    // The derived schema feeds the same root as a hand-declared one — the
    // anchor property Phase A rides on.
    if payloadexec.SchemaRoot("net", got) != payloadexec.SchemaRoot("net", []payloadexec.TableSchema{want}) {
        t.Fatal("derived and declared schemas must produce identical roots")
    }
    // Missing table fails closed with the ref in the error.
    _, err = schemaregistry.NewClickHouseLoader(conn).Load(context.Background(),
        []schemaregistry.TableRef{{TableID: "ghost", Database: db, Table: "nope"}})
    if err == nil || !strings.Contains(err.Error(), "ghost") {
        t.Fatalf("missing table: %v", err)
    }
}
```

- [ ] **Step 5: Bazel sync + run everything**

```bash
cd /Users/uranuswch/Dev/housegate/housegate
git checkout main && git pull && git checkout -b feat/schemaregistry-ch-loader
bazel mod tidy && bazel run //:gazelle
bazel test //pkg/schemaregistry:schemaregistry_test
docker pull clickhouse/clickhouse-server:25.8 >/dev/null
bazel test //pkg/integration:integration_test --test_filter='TestClickHouseLoader' --test_output=errors
bazel build //...
```

Expected: PASS (integration target needs docker running, matching the repo's existing CI job).

- [ ] **Step 6: Commit + PR**

```bash
git add pkg/schemaregistry/ pkg/integration/chschema_test.go $(git ls-files -mo --exclude-standard '*BUILD.bazel')
git commit -m "feat(schemaregistry): loader seam + ClickHouse-derived table schemas

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin feat/schemaregistry-ch-loader
gh pr create --repo housegate/housegate --base main --title "feat(schemaregistry): CH-derived storage-integrity schema loader (Phase A)" --body "Phase A of docs/superpowers/specs/2026-07-28-schema-registry-design.md (decision 8): the Loader seam Phase B re-implements over network state, plus the ClickHouse implementation. Consumers follow in arbiter-core and sentio-node."
```

After merge, tag the next housegate patch release (the flow that minted v0.7.1) so Tasks 2–3 pin a real version; a pseudo-version of the merge commit works in the interim (the module path is canonical and public — plain `go get`, no replace).

---

### Task 2: arbiter-core — `CHTableName` export + `table_ids` mode in both reference binaries

**Files:**
- Modify: `snode/parts.go:79` (export), call sites of `chTableName` (mechanical rename)
- Modify: `cmd/arbiter-snode/config.go`, `cmd/arbiter-snode/main.go`, `cmd/arbiter-verifier/config.go`, `cmd/arbiter-verifier/main.go`
- Test: `cmd/arbiter-snode/main_test.go`, `cmd/arbiter-verifier/main_test.go`
- Modify: `go.mod` (`go get github.com/housegate/housegate@<Task-1 version>` — plain public module bump)

**Interfaces:**
- Consumes: Task 1's `schemaregistry.NewClickHouseLoader/TableRef/Loader`.
- Produces: `snode.CHTableName(tableID string) string` (Task 3 uses it); config semantics: `tables` XOR `table_ids` (exactly one, enforced), `-print-schema-root` works in both modes (derive mode dials ClickHouse first).

- [ ] **Step 1: Export the physical-name mapping**

`snode/parts.go`: rename `chTableName` → `CHTableName` with a doc comment ("CHTableName maps a logical storage-integrity table id to its physical ClickHouse table name; consumers must use this instead of copying the rule"), rename the ~6 call sites in `snode/`. `go build ./... && go test ./snode/ -count=1` green, commit `refactor(snode): export CHTableName`.

- [ ] **Step 2: Write the failing config tests**

In both `main_test.go` files (they already have `TestConfigValidate`-style tables from the staged-intake work — extend them):

```go
func TestConfigValidate_TableSourceModes(t *testing.T) {
    base := validConfig() // existing fixture with .Tables set
    // table_ids mode: drop Tables, set TableIDs — valid
    cfg := base
    cfg.Tables = nil
    cfg.TableIDs = []string{"orders.t"}
    if err := cfg.validate(false); err != nil {
        t.Fatalf("table_ids mode: %v", err)
    }
    // both set — invalid
    cfg = base
    cfg.TableIDs = []string{"orders.t"}
    if err := cfg.validate(false); err == nil {
        t.Fatal("tables and table_ids together must fail")
    }
    // neither — invalid (existing 'at least one table schema' error keeps firing)
    cfg = base
    cfg.Tables = nil
    if err := cfg.validate(false); err == nil {
        t.Fatal("no table source must fail")
    }
    // schema_root still required in table_ids mode, but root EQUALITY is
    // deferred to post-derive (validate can't compute without columns)
    cfg = base
    cfg.Tables = nil
    cfg.TableIDs = []string{"orders.t"}
    cfg.SchemaRoot = ""
    if err := cfg.validate(false); err == nil {
        t.Fatal("schema_root stays required")
    }
}
```

(Adapt `validate`'s actual signature/fixture names from the file — the current snode config validator takes a bool; verifier's takes none. The assertions are the contract.)

- [ ] **Step 3: Run to verify failure, then implement**

Run: `go test ./cmd/arbiter-snode/ ./cmd/arbiter-verifier/ -run TableSourceModes -v` → FAIL.

Config: add `TableIDs []string \`yaml:"table_ids"\`` next to `Tables`; validation: exactly one of the two non-empty (pointed error naming both keys); in `table_ids` mode skip the per-table column checks and the `computedSchemaRoot` equality (columns unknown until derive) but keep `schema_root` non-empty required. Main assembly in both binaries, before role construction:

```go
tables := cfg.tables()
if len(cfg.TableIDs) > 0 {
    refs := make([]schemaregistry.TableRef, 0, len(cfg.TableIDs))
    for _, id := range cfg.TableIDs {
        refs = append(refs, schemaregistry.TableRef{TableID: id, Database: cfg.UnsafeDatabase, Table: snode.CHTableName(id)})
    }
    tables, err = schemaregistry.NewClickHouseLoader(conn).Load(ctx, refs)
    if err != nil {
        return fmt.Errorf("derive table schemas: %w", err)
    }
}
```

(verifier introspects its own local replica the same way — same `UnsafeDatabase` field; the genesis `schema_root` equality then fires inside `snode.New`/`verifier.New` config validation as today, which is the anchor check.) `-print-schema-root`: in derive mode, dial ClickHouse first (reuse the main path's conn construction), derive, print `payloadexec.SchemaRoot(cfg.NetworkID, tables)`; document in the flag help that derive mode needs a reachable ClickHouse. README: note the verifier-bootstrap limitation (a verifier with no local tables yet cannot derive — keep `tables` YAML for that case until Phase B).

- [ ] **Step 4: Verify + commit + PR**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/arbiter-core
git checkout main && git pull && git checkout -b feat/ch-derived-schemas
go get github.com/housegate/housegate@<task-1-version> && go mod tidy
go build ./... && go vet ./... && go test ./... -count=1
docker run -d --rm --name ac-pa-ch -p 19000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:25.8 && sleep 8
ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:19000 go test ./snode -count=1 -timeout 900s
docker rm -f ac-pa-ch
git add -A && git commit -m "feat(cmd): table_ids mode derives schemas from ClickHouse"
git push origin feat/ch-derived-schemas
gh pr create --repo sentioxyz/arbiter-core --title "feat(cmd): CH-derived table schemas (schema-registry Phase A)" --body "Consumes housegate pkg/schemaregistry; config gains table_ids XOR tables; -print-schema-root works in derive mode. Spec: housegate docs/superpowers/specs/2026-07-28-schema-registry-design.md decision 8."
```

---

### Task 3: sentio-node — config switch on a fresh branch + inherited-findings fixes

PR #163 merged with its review findings intact; this task lands the Phase-A config switch AND retires the inherited findings it structurally intersects, as one coherent follow-up PR.

**Files (branch `feat/schema-registry-phase-a` off main):**
- Modify: `config/config.go` (`StorageIntegritySNode.Tables` → `TableIDs []string`; delete `StorageIntegrityTable`/`StorageIntegrityColumn` and `TableSchemas()`; the `partition_by`-required validation at `config/config.go:104` disappears with the field — that closes inherited finding #4 structurally), `config/config_test.go`
- Modify: `standalone/standalone.go` (derive before `snode.New`; `NewSchemaResolver` fed from the derived set)
- Modify: `go.mod` + `MODULE.bazel`/BUILD via gazelle (bump housegate to the Task-1 version and arbiter-core to the Task-2 version)

**Interfaces:**
- Consumes: Task 1 loader, Task 2's `snode.CHTableName`.
- Produces: final Phase-A config shape (breaking, declared in the PR — the stack is default-off and pre-production):

```yaml
storage_integrity:
  snode:
    table_ids: ["orders.t", "events.e"]   # replaces tables[] — columns derive from clickhouse_dsn
```

- [ ] **Step 1: Write the failing config tests**

Rewrite `TestStorageIntegrityConfigValidation`: `TableIDs` required non-empty when enabled, entries non-empty and unique; the `partition_by` case is deleted with the field. Add the two cross-checks the #163 review flagged, now as main-line fixes: `enabled ⇒ cfg.Housegate.Listen != ""` (inherited finding #14 — today the SI block silently fails to assemble when housegate is disabled, because the assembly sits inside `if cfg.Housegate.Listen != ""`), and every `table_ids` entry resolving into `cfg.Housegate.StorageIntegrity.Runtime.MergeGuard.Tables` via `snode.CHTableName` + the unsafe database (the tables↔merge_guard drift gap):

```go
"si without housegate listen": func(c *Config) { c.Housegate.Listen = "" },          // must fail
"table id not merge-guarded":  func(c *Config) { c.StorageIntegrity.SNode.TableIDs = append(..., "ghost.t") }, // must fail
```

- [ ] **Step 2: Run to verify failure, then implement**

Config: replace the field, move validation, add both cross-checks with pointed errors (`"storage_integrity.snode.table_ids[%d] %q has no merge_guard.tables entry (expected {database: %q, table: %q})"`). Assembly in `standalone.go` — replace the `si.TableSchemas()` calls:

```go
refs := make([]schemaregistry.TableRef, 0, len(si.SNode.TableIDs))
for _, id := range si.SNode.TableIDs {
    refs = append(refs, schemaregistry.TableRef{TableID: id, Database: unsafeDB, Table: snode.CHTableName(id)})
}
tables, err := schemaregistry.NewClickHouseLoader(chConn).Load(ctx, refs)
if err != nil {
    return fmt.Errorf("derive storage-integrity schemas: %w", err)
}
```

(`unsafeDB` — the snode role's unsafe database; take it from wherever main currently sources `snode.Config`'s database fields, keeping one source of truth.) Feed `tables` to both `snode.Config.Tables` and `storageintegrityadapter.NewSchemaResolver(tables)`. The genesis anchor fires unchanged inside `snode.New` — a wrong derivation still refuses startup.

- [ ] **Step 3: Verify + commit + PR**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
git checkout main && git pull && git checkout -b feat/schema-registry-phase-a
go get github.com/housegate/housegate@<task-1-version> github.com/sentioxyz/arbiter-core@<task-2-version>
go mod tidy && bazel mod tidy && bazel run //:gazelle
go build ./... && go test ./config/ ./storageintegrityadapter/ -count=1 && bazel build //...
git add -A && git commit -m "feat(config): derive storage-integrity schemas from ClickHouse (Phase A)

BREAKING (pre-production, default-off stack): storage_integrity.snode.tables
column YAML is replaced by table_ids. Also fixes two findings inherited from
PR #163: the partition_by-required bug goes with the field, and enabled-SI
now cross-checks housegate.listen and merge_guard.tables coverage.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push origin feat/schema-registry-phase-a
gh pr create --repo sentioxyz/sentio-node --title "feat(config): CH-derived storage-integrity schemas (schema-registry Phase A)" --body "Phase A of housegate docs/superpowers/specs/2026-07-28-schema-registry-design.md. Breaking config change (pre-production stack): snode.tables → table_ids, columns derived from the local ClickHouse, genesis schema_root anchor unchanged. Retires two inherited #163 review findings (partition_by-required; SI/housegate-listen + merge_guard cross-checks)."
```

---

### Task 4: Docs + smoke rehearsal

**Files:**
- Modify: sentio-node's storage-integrity doc section (wherever main documents the config; likely README or docs/) + the E2E smoke fixture config
- Modify: housegate `docs/superpowers/specs/2026-07-28-schema-registry-design.md` — mark Phase A as Implemented with the three PR links (on the `docs/schema-registry` branch)

- [ ] **Step 1: Update the smoke fixture + run it**

The gated E2E smoke (`SENTIO_SI_E2E=1`) config drops the `tables` block for `table_ids`; run the smoke against local arbiter + `da-store --dev` + ClickHouse per its runbook and confirm one INSERT reaches ACK2 with derived schemas (this is the end-to-end proof that derive → root → genesis anchor → staged intake all line up). Note: housegate PR #110's production wiring touched the intake/status paths and may have changed the smoke's gates (`CompanionStagedIntakeAvailable` state) — read the smoke's current gates on main first and follow them.

- [ ] **Step 2: Onboarding runbook seed**

In the same doc section, add the Phase-A onboarding recipe (the spec decision-6 runbook, CH-derive edition): create the table in ClickHouse (with `_hg_row_id`) → add its id to `table_ids` + `merge_guard.tables` → run `arbiter-snode -print-schema-root` (derive mode) → governance-update the genesis `schema_root` → rolling restart; every role re-anchors or refuses.

- [ ] **Step 3: Final verification + spec status**

All three repos: full build+test green (housegate via bazel, others via go+bazel as above). Update the spec's Phase A row with the three PR links and commit on `docs/schema-registry`.

---

## Self-review notes (already applied)

- Spec decision 8 Phase A scope check: config→`table_ids` ✅ (Tasks 2–3), columns from `system.columns` ✅ (Task 1), root anchored to genesis unchanged ✅ (explicitly untouched; asserted in Task 1's root-equality test and exercised end-to-end in Task 4).
- The loader seam signature matches spec §5's `Load(ctx, tableIDs)` in spirit but takes `[]TableRef` — the spec's signature elides the physical mapping the CH source needs; Phase B's network-state loader takes the same refs and ignores `Database`/`Table`. Recorded as a spec-precision note rather than a deviation (the seam is unchanged for consumers).
- Verifier bootstrap limitation (spec decision 4's known CH-derive gap) is documented, not solved — Phase B's explicit purpose.
- Re-baseline (2026-07-29): the canonical-module-path migration (housegate #108/#109, arbiter-core #4/#5, sentio-node #164) removed every replace/GOPRIVATE complication this plan previously carried — dependency bumps are now plain public `go get`s against tagged versions (housegate v0.7.1 era). sentio-node work moved from "converge with open PR #163" to "fix inherited findings on a fresh branch off main"; the `partition_by` fix is structural (field deleted), the two cross-checks are explicit test cases in Task 3.
- The #163 findings NOT touched here (`parseStatementID` duplication, slog-adapter review, double `Validate` call, arbiter-core tag hygiene — the last already resolved by the v0.7.1-era release flow) stay on the review backlog — Phase A only claims the ones its config change structurally intersects.
