# Schema Registry Phase A (CH-derived loader) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kill hand-written column YAML now, per [schema-registry spec](../specs/2026-07-28-schema-registry-design.md) decision 8 Phase A: storage-integrity consumers keep only a `table_ids` selector and derive column-level `payloadexec.TableSchema` from the local ClickHouse (`system.columns`/`system.tables`), with the genesis `schema_root` anchor check unchanged.

**Architecture:** One new housegate package `pkg/schemaregistry` defines the loader seam (`Loader.Load(ctx, refs)`) that Phase B will re-implement over network state, plus the Phase-A `ClickHouseLoader`. arbiter-core's reference binaries and sentio-node's assembly consume it; `schema_root` validation stays where it is (`snode.Config.validate`), so a wrong derivation still refuses startup. sentio-node's change lands on the open PR #163 branch and retires two of its review findings in the process.

**Tech Stack:** Go 1.26; housegate is Bazel 8.5.1 + Bzlmod (gazelle-managed BUILD files, docker tests in `//pkg/integration:integration_test`); arbiter-core and sentio-node consume housegate as a module.

**Repos & branches:**
- housegate `/Users/uranuswch/Dev/housegate/housegate` — branch `feat/schemaregistry-ch-loader` off main
- arbiter-core `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` — branch `feat/ch-derived-schemas` off main
- sentio-node `/Users/uranuswch/Dev/sentio_xyz/sentio-node` — continue on PR #163's branch (`git fetch origin 'pull/163/head:...'` — confirm the head branch name via `gh pr view 163 --repo sentioxyz/sentio-node --json headRefName` first)

## Global Constraints

- The consensus anchor is untouched: schemas from any source still flow through `payloadexec.SchemaRoot(networkID, tables)` and must equal the configured genesis `schema_root` (validated in `arbiter-core/snode/config.go:67-71`; verifier likewise) — Phase A changes where columns COME FROM, never what is TRUSTED.
- Hash domains are reused, not invented (spec decision 1): no new digest anywhere in this plan.
- The loader excludes the `_hg_row_id` protocol column — `TableSchema.Columns` is user columns only (matches `chexec` scratch-table construction).
- The loader does NOT validate partition-key shape; `validatePartitionBy` (arbiter-core `snode/config.go:82`, bare-String-column MVP freeze) stays the sole gate and now also catches expression partition keys arriving from CH introspection.
- Physical table naming stays arbiter-core's single source of truth: export `snode.CHTableName` rather than copying `ReplaceAll(id, ".", "__")` into any consumer.
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
             (PR #163 branch)                    + the two review fixes this obsoletes/requires
```

---

### Task 1: housegate `pkg/schemaregistry` — the loader seam + ClickHouse implementation

**Files:**
- Create: `pkg/schemaregistry/loader.go`, `pkg/schemaregistry/loader_test.go`
- Create: `pkg/integration/chschema_test.go`
- Modify: BUILD files via gazelle (mechanical)

**Interfaces:**
- Consumes: `pkg/replay/payloadexec.TableSchema`/`lthash.Column` (existing), `clickhouse-go/v2` (existing dep).
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
git checkout -b feat/schemaregistry-ch-loader origin/main
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

---

### Task 2: arbiter-core — `CHTableName` export + `table_ids` mode in both reference binaries

**Files:**
- Modify: `snode/parts.go:79` (export), call sites of `chTableName` (mechanical rename)
- Modify: `cmd/arbiter-snode/config.go`, `cmd/arbiter-snode/main.go`, `cmd/arbiter-verifier/config.go`, `cmd/arbiter-verifier/main.go`
- Test: `cmd/arbiter-snode/main_test.go`, `cmd/arbiter-verifier/main_test.go`
- Modify: `go.mod` (housegate bump to the commit containing Task 1; pseudo-version until the PR merges, re-bump after)

**Interfaces:**
- Consumes: Task 1's `schemaregistry.NewClickHouseLoader/TableRef/Loader`.
- Produces: `snode.CHTableName(tableID string) string` (Task 3 uses it); config semantics: `tables` XOR `table_ids` (exactly one, enforced), `-print-schema-root` works in both modes (derive mode dials ClickHouse first).

- [ ] **Step 1: Export the physical-name mapping**

`snode/parts.go`: rename `chTableName` → `CHTableName` with a doc comment ("CHTableName maps a logical storage-integrity table id to its physical ClickHouse table name; consumers must use this instead of copying the rule"), keep an unexported alias if call-site churn is large (it is small — just rename the ~6 call sites in `snode/`). `go build ./... && go test ./snode/ -count=1` green, commit `refactor(snode): export CHTableName`.

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
GOPRIVATE=github.com/sentioxyz go get github.com/housegate/housegate@<task-1-commit> && go mod tidy
go build ./... && go vet ./... && go test ./... -count=1
docker run -d --rm --name ac-pa-ch -p 19000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 clickhouse/clickhouse-server:25.8 && sleep 8
ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:19000 go test ./snode -count=1 -timeout 900s
docker rm -f ac-pa-ch
git add -A && git commit -m "feat(cmd): table_ids mode derives schemas from ClickHouse"
git push origin feat/ch-derived-schemas
gh pr create --repo sentioxyz/arbiter-core --title "feat(cmd): CH-derived table schemas (schema-registry Phase A)" --body "Consumes housegate pkg/schemaregistry; config gains table_ids XOR tables; -print-schema-root works in derive mode. Spec: housegate docs/superpowers/specs/2026-07-28-schema-registry-design.md decision 8."
```

---

### Task 3: sentio-node — config switch on the PR #163 branch + review fixes

**Files (on PR #163's head branch):**
- Modify: `config/config.go` (`StorageIntegritySNode.Tables` → `TableIDs []string`; drop `StorageIntegrityTable`/`StorageIntegrityColumn`; `TableSchemas()` deleted), `config/config_test.go`
- Modify: `standalone/standalone.go` (derive before `snode.New`; `NewSchemaResolver` fed from the derived set)
- Modify: `storageintegrityadapter/adapter.go` only if `NewSchemaResolver`'s input type changes (it takes `[]payloadexec.TableSchema` — no change expected)

**Interfaces:**
- Consumes: Task 1 loader, Task 2's `snode.CHTableName` + the bumped module pins (`go.mod`: housegate ≥ Task-1 commit, arbiter-core ≥ Task-2 commit; `bazel mod tidy && bazel run //:gazelle` after).
- Produces: final Phase-A config shape:

```yaml
storage_integrity:
  snode:
    table_ids: ["orders.t", "events.e"]   # replaces tables[] — columns derive from clickhouse_dsn
```

- [ ] **Step 1: Write the failing config tests**

Rewrite the PR's `TestStorageIntegrityConfigValidation` table: `TableIDs` required non-empty when enabled, entries non-empty and unique; **the `partition_by`-required case is deleted with the field** (this closes PR #163 review finding #4 structurally). Add the two cross-checks from the review as cases: `enabled ⇒ cfg.Housegate.Listen != ""` (finding #14), and every `table_ids` entry resolving into `cfg.Housegate.StorageIntegrity.Runtime.MergeGuard.Tables` via `snode.CHTableName` + the unsafe database (the review's tables↔merge_guard gap):

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

(`unsafeDB` — the snode role's unsafe database; take it from wherever the PR currently sources `snode.Config`'s database fields, keeping one source of truth.) Feed `tables` to both `snode.Config.Tables` and `storageintegrityadapter.NewSchemaResolver(tables)`. The genesis anchor fires unchanged inside `snode.New` — a wrong derivation still refuses startup.

- [ ] **Step 3: Verify + commit**

```bash
cd /Users/uranuswch/Dev/sentio_xyz/sentio-node
go mod tidy && bazel mod tidy && bazel run //:gazelle
go build ./... && go test ./config/ ./storageintegrityadapter/ -count=1 && bazel build //...
git add -A && git commit -m "feat(config): derive storage-integrity schemas from ClickHouse (Phase A)

Replaces column YAML with table_ids; closes the partition_by and
merge-guard/listen cross-check review findings.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push
```

Update the PR #163 description: note the config-shape change and the two review findings it resolves.

---

### Task 4: Docs + smoke rehearsal

**Files:**
- Modify: sentio-node's storage-integrity doc section (wherever PR #163 documents the config; likely README or docs/) + the E2E smoke fixture config
- Modify: housegate `docs/superpowers/specs/2026-07-28-schema-registry-design.md` — mark Phase A as Implemented with PR links (on the `docs/schema-registry` branch)

- [ ] **Step 1: Update the smoke fixture + run it**

The gated E2E smoke (`SENTIO_SI_E2E=1`) config drops the `tables` block for `table_ids`; run the smoke against local arbiter + `da-store --dev` + ClickHouse per its runbook and confirm one INSERT reaches ACK2 with derived schemas (this is the end-to-end proof that derive → root → genesis anchor → staged intake all line up).

- [ ] **Step 2: Onboarding runbook seed**

In the same doc section, add the Phase-A onboarding recipe (the spec decision-6 runbook, CH-derive edition): create the table in ClickHouse (with `_hg_row_id`) → add its id to `table_ids` + `merge_guard.tables` → run `arbiter-snode -print-schema-root` (derive mode) → governance-update the genesis `schema_root` → rolling restart; every role re-anchors or refuses.

- [ ] **Step 3: Final verification + spec status**

All three repos: full build+test green (housegate via bazel, others via go+bazel as above). Update the spec's Phase A row with the three PR links and commit on `docs/schema-registry`.

---

## Self-review notes (already applied)

- Spec decision 8 Phase A scope check: config→`table_ids` ✅ (Tasks 2–3), columns from `system.columns` ✅ (Task 1), root anchored to genesis unchanged ✅ (explicitly untouched; asserted in Task 1's root-equality test and exercised end-to-end in Task 4).
- The loader seam signature matches spec §5's `Load(ctx, tableIDs)` in spirit but takes `[]TableRef` — the spec's signature elides the physical mapping the CH source needs; Phase B's network-state loader takes the same refs and ignores `Database`/`Table`. Recorded as a spec-precision note rather than a deviation (the seam is unchanged for consumers).
- Verifier bootstrap limitation (spec decision 4's known CH-derive gap) is documented, not solved — Phase B's explicit purpose.
- sentio-node work deliberately lands on the open PR #163 branch: Phase A deletes the `partition_by` field at the center of review finding #4 and adds the two missing cross-checks (findings #14 + tables↔merge_guard), so the review and Phase A converge instead of conflicting.
