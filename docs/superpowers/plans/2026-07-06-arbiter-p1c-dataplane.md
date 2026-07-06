# Arbiter P1c Data-Plane Roles (Verifier + SNode) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the real Verifier data plane (replay + byte-side scan against real ClickHouse) and the real SNode promotion executor (source write, RC assembly, REPLACE PARTITION, cleanup) as arbiter-repo libraries over a shared dataplane client foundation, prove them with a docker flagship including three fraud classes, and land the housegate content-addressed-root precondition they depend on.

**Architecture:** Spec `docs/superpowers/specs/2026-07-06-arbiter-p1c-dataplane-design.md` (housegate repo, PR #79). Two repos: housegate gets the content-addressed root change + exported same-by-construction helpers (Tasks 1–3, one PR, must merge before Task 4); arbiter gets everything else (Tasks 4–18, direct-to-main). `dataplane/` is the role-agnostic arbiter-client foundation; `verifier/` and `snode/` are thin role loops over it; the anchor seam becomes a non-blocking poll; genesis gains `schema_root` + `manifest_path` consensus params.

**Tech Stack:** Go 1.24+ (go modules, no Bazel in arbiter), hashicorp/raft (existing), grpc + arbiter-proto v0.2.0 (no proto changes), clickhouse-go/v2, housegate `pkg/replay`(+payloadexec/chexec) and `pkg/lthash`, docker `clickhouse/clickhouse-server:25.8` for gated integration tests.

## Global Constraints

- **Repos:** housegate work on a branch + PR (main is PR-protected, Bazel is ground truth: `bazel test //...`); arbiter work commits directly to main (`cd /Users/uranuswch/src/sentio_xyz/arbiter`), conventional commits, every commit ends with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- **Ordering:** Tasks 1–3 (housegate) must be merged to housegate main before Task 4 runs (arbiter pins the new housegate version). Tasks 5+ depend on Task 4.
- **Same-by-construction rule (spec decision 5):** row-id derivation, row canonicalization, per-part row-LtHash, schema-root, and state-root assembly in `verifier/` and `snode/` MUST call the exported payloadexec/chexec/replay helpers. No `blake3.` / `lthash.New(` / local digest reimplementation of those five concerns inside `verifier/` or `snode/` (tripwire grep).
- **Frozen constants (arbiter):** VerifierQuorum=2, VerifierSelectN=3, INSERT-only; fsm red lines unchanged (no `gen/pb` direct import, no `time.Now` under `fsm/`); **no file under `fsm/` may change in this entire plan** — any fsm diff is a stop-and-report.
- **arbiter red lines carried from P1b:** orchestrator is the only holder of `authority.Signer`; every orchestrator side effect preceded by `VerifyLeader()`; `pb.NotLeader.leader_addr` carries the raft ServerID; plaintext gRPC.
- **JWS/signature conventions:** ed25519 signatures are hex WITHOUT `0x`, computed over the hash-string bytes; authority JWS validated via `authority.Validator` (`PromoteCommandHash` / `CleanupCommandHash`).
- **Docker-gated tests:** every test needing ClickHouse is skipped unless `ARBITER_CH_INTEGRATION=1` (arbiter repo) so plain `go test ./...` stays green without docker. housegate's docker tests go into the existing `//pkg/integration:integration_test` Bazel target (tagged `manual`, listed in CI explicitly — if you add a NEW docker-bound Bazel target, you must add it to housegate `.github/workflows/ci.yml`).
- **CH image:** `clickhouse/clickhouse-server:25.8` everywhere.
- **Naming:** `_hg_row_id` FixedString(32) is the reserved row-identity column; scratch DBs prefix `_hg_replay_scratch`; the three SNode databases are `hg_unsafe`, `hg_safe`, `hg_promote`.
- **Markdown docs:** no hard line-wrapping. Code/comments/logs in English.
- **Report files:** implementers write reports to `/Users/uranuswch/Dev/housegate/housegate/.superpowers/sdd/p1c-task-N-report.md` (absolute path; note the `p1c-` prefix).

## Repo/paths cheat sheet

- housegate checkout: `/Users/uranuswch/Dev/housegate/housegate` (currently on branch `docs/arbiter-p1c-design`; housegate CODE tasks use a separate branch `feat/replay-content-roots` cut from `main`).
- arbiter checkout: `/Users/uranuswch/src/sentio_xyz/arbiter` (main, direct push).
- arbiter imports housegate as module `housegate/housegate` (see arbiter `go.mod` for the current pseudo-version pin).

---

### Task 1: housegate — content-addressed DataRoot v2 + `AssembleStateRoot` (semantic change)

**Files:**
- Modify: `pkg/replay/types.go` (ComputeDataRoot, new helper)
- Test: `pkg/replay/types_test.go` (create if absent — check first with `ls pkg/replay/*_test.go`; hash_test.go exists but is for hash.go)

**Interfaces:**
- Consumes: existing `SafeSnapshotManifest`, `canonicalDigest` (pkg-internal), `normalized()`.
- Produces: `ComputeDataRoot()` new semantics (part-name/phys/bytes independent, domain `"safe-snapshot-data-v2"`); `func AssembleStateRoot(schemaSnapshotID, schemaRoot, executorProfileID string, tables []TableManifest) (dataRoot, stateRoot string, err error)` — used by Task 2's executor refactor and by arbiter `snode/` (Task 13).

**Context (why):** today `ComputeDataRoot` hashes `ActiveParts` verbatim, which depends on `PartName` (the replay executor synthesizes `"%s-b%d-s%d"` names; the real source reads ClickHouse names), `PartPhysHash` (executor's is a name-derived placeholder; source's is `system.parts.hash_of_all_files`), and `Bytes` (executor sums payload `RawBytes`; source reads `bytes_on_disk`). Honest source and honest verifier could never agree on check 1. Spec decision 8: `DataRoot` commits logical content only — per table `{TableID, PartitionRoots}`; `ActiveParts` stay covered by `ComputeManifestRoot` alone. The domain string bumps to `safe-snapshot-data-v2` so old and new roots can never be silently compared. `ComputeStateRoot`'s shape is unchanged (it hashes `{SchemaSnapshotID, SchemaRoot, ExecutorProfileID, DataRoot}`) and inherits the fix through DataRoot.

- [ ] **Step 1: Write the failing tests**

Create `pkg/replay/types_test.go` (or append if it exists):

```go
package replay

import "testing"

func manifestFixture() SafeSnapshotManifest {
	return SafeSnapshotManifest{
		SafeBlockSeq:      3,
		SchemaSnapshotID:  "schema-genesis",
		SchemaRoot:        "0xschr",
		ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []TableManifest{{
			TableID:    "db.t",
			SchemaHash: "0xsh",
			PartitionRoots: []PartitionCommitment{
				{TableID: "db.t", PartitionID: "p0", Root: "0xr0"},
			},
			ActiveParts: []PartManifestEntry{{
				TableID: "db.t", PartitionID: "p0", PartName: "all_1_1_0",
				PartPhysHash: "0xphys", PartRowLtHash: "0xrow", RowCount: 7, Bytes: 1234,
			}},
		}},
	}
}

// Decision 8: DataRoot/StateRoot must be independent of part names, phys
// hashes, sizes, and storage refs — only {TableID, PartitionRoots} count.
func TestDataRoot_IgnoresPartIdentityFields(t *testing.T) {
	a, err := manifestFixture().Seal()
	if err != nil {
		t.Fatalf("seal a: %v", err)
	}
	mutated := manifestFixture()
	mutated.Tables[0].ActiveParts[0].PartName = "different_9_9_9"
	mutated.Tables[0].ActiveParts[0].PartPhysHash = "0xotherphys"
	mutated.Tables[0].ActiveParts[0].Bytes = 999999
	mutated.Tables[0].ActiveParts[0].StorageRefs = []string{"s3://x"}
	b, err := mutated.Seal()
	if err != nil {
		t.Fatalf("seal b: %v", err)
	}
	if a.DataRoot != b.DataRoot || a.StateRoot != b.StateRoot {
		t.Fatalf("data/state roots must ignore part identity fields: %s/%s vs %s/%s", a.DataRoot, a.StateRoot, b.DataRoot, b.StateRoot)
	}
	if a.ManifestRoot == b.ManifestRoot {
		t.Fatal("manifest root must still cover ActiveParts (document commitment)")
	}
}

func TestDataRoot_SensitiveToPartitionRoots(t *testing.T) {
	a, _ := manifestFixture().Seal()
	mutated := manifestFixture()
	mutated.Tables[0].PartitionRoots[0].Root = "0xEVIL"
	b, err := mutated.Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if a.DataRoot == b.DataRoot {
		t.Fatal("data root must commit partition roots")
	}
}

func TestAssembleStateRoot_MatchesSeal(t *testing.T) {
	m, err := manifestFixture().Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	dataRoot, stateRoot, err := AssembleStateRoot(m.SchemaSnapshotID, m.SchemaRoot, m.ExecutorProfileID, manifestFixture().Tables)
	if err != nil {
		t.Fatalf("assemble: %v", err)
	}
	if dataRoot != m.DataRoot || stateRoot != m.StateRoot {
		t.Fatalf("assembly must match Seal: %s/%s vs %s/%s", dataRoot, stateRoot, m.DataRoot, m.StateRoot)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/uranuswch/Dev/housegate/housegate && git checkout main && git pull --ff-only && git checkout -b feat/replay-content-roots && go test ./pkg/replay/ -run 'TestDataRoot|TestAssembleStateRoot' -v`
Expected: FAIL — `TestDataRoot_IgnoresPartIdentityFields` (roots differ today) and `TestAssembleStateRoot_MatchesSeal` (undefined: AssembleStateRoot).

- [ ] **Step 3: Implement**

In `pkg/replay/types.go`, replace `ComputeDataRoot` and add `AssembleStateRoot`:

```go
// ComputeDataRoot returns the commitment over active table data. It is
// content-addressed (P1c decision 8): per table only {TableID,
// PartitionRoots} enter the digest — partition roots are LtHash sums of
// row content, so the data root is independent of part names, physical
// hashes, sizes, and storage refs, which differ between the replay
// executor's synthetic parts and real ClickHouse parts. ActiveParts stay
// covered by ComputeManifestRoot (the manifest document commitment). The
// domain is versioned (-v2) so pre-P1c roots can never be compared.
func (m SafeSnapshotManifest) ComputeDataRoot() (string, error) {
	type tableData struct {
		TableID        string                `json:"table_id"`
		PartitionRoots []PartitionCommitment `json:"partition_roots"`
	}
	var tables []tableData
	for _, t := range m.normalized().Tables {
		tables = append(tables, tableData{
			TableID:        t.TableID,
			PartitionRoots: t.PartitionRoots,
		})
	}
	return canonicalDigest("safe-snapshot-data-v2", tables)
}

// AssembleStateRoot derives (DataRoot, StateRoot) for an arbitrary
// table-set view without sealing a manifest. It is THE shared state-root
// assembly (spec decision 5): the replay executor reaches it through
// Seal, and the SNode source reaches it directly to compute
// RCRecord.SourceClaimRoot over its absolute per-partition view, so an
// honest source matches ExecutionReceipt.ComputedStateRoot by
// construction.
func AssembleStateRoot(schemaSnapshotID, schemaRoot, executorProfileID string, tables []TableManifest) (dataRoot, stateRoot string, err error) {
	m := SafeSnapshotManifest{
		SchemaSnapshotID:  schemaSnapshotID,
		SchemaRoot:        schemaRoot,
		ExecutorProfileID: executorProfileID,
		Tables:            tables,
	}
	dataRoot, err = m.ComputeDataRoot()
	if err != nil {
		return "", "", err
	}
	m.DataRoot = dataRoot
	stateRoot, err = m.ComputeStateRoot()
	if err != nil {
		return "", "", err
	}
	return dataRoot, stateRoot, nil
}
```

- [ ] **Step 4: Run the package tests; fix golden-value fallout ONLY**

Run: `go test ./pkg/replay/... 2>&1 | tail -20`
Expected: the three new tests PASS. Pre-existing tests that assert exact root hex values or cross-root relationships may fail — inspect each; a failure caused purely by the new derivation (different digest for same inputs) is expected fallout: update the asserted constant by re-deriving it (print with `t.Log` once, paste, remove the log). A failure that indicates a behavior change beyond DataRoot derivation is a STOP — report it, do not paper over. Then run the Bazel ground truth: `bazel test //pkg/replay/... --test_output=errors`.

- [ ] **Step 5: Commit**

```bash
git add pkg/replay/types.go pkg/replay/types_test.go
git commit -m "feat(replay): content-address DataRoot; export AssembleStateRoot

DataRoot now commits {TableID, PartitionRoots} only (domain
safe-snapshot-data-v2); ActiveParts remain covered by ManifestRoot.
Part names, phys hashes, and byte sizes differ between the replay
executor's synthetic parts and real ClickHouse parts, so a
name-carrying DataRoot made honest check-1 agreement impossible.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: housegate — payloadexec exports (`RowElementHash`, `SchemaRoot`, `TableSchemaHash`)

**Files:**
- Modify: `pkg/replay/payloadexec/executor.go` (export three internals; keep old names as thin delegates)
- Test: `pkg/replay/payloadexec/exports_test.go` (create)

**Interfaces:**
- Consumes: internal `rowElementHash(sch TableSchema, rid []byte, values []any) (*lthash.Hash, error)`, `schemaRoot(networkID string, schemas []TableSchema) string`, `tableSchemaHash(networkID string, t TableSchema) string`.
- Produces (exact signatures, used by Tasks 3/11/13/16):
  - `func RowElementHash(sch TableSchema, rowID []byte, values []any) (*lthash.Hash, error)`
  - `func SchemaRoot(networkID string, schemas []TableSchema) string`
  - `func TableSchemaHash(networkID string, t TableSchema) string`

- [ ] **Step 1: Write the failing test**

Create `pkg/replay/payloadexec/exports_test.go`:

```go
package payloadexec

import "testing"

func TestExportedHelpersDelegate(t *testing.T) {
	sch := TableSchema{TableID: "db.t", PartitionBy: "p", Columns: []Column{{Name: "v", Type: "UInt64"}}}
	rid := RowID("net-1", "db.t", "acct/1/n", 0)

	exp, err := RowElementHash(sch, rid, []any{uint64(42)})
	if err != nil {
		t.Fatalf("RowElementHash: %v", err)
	}
	got, err := rowElementHash(sch, rid, []any{uint64(42)})
	if err != nil {
		t.Fatalf("rowElementHash: %v", err)
	}
	if exp.Sum() != got.Sum() {
		t.Fatal("RowElementHash must delegate to the internal derivation")
	}
	if SchemaRoot("net-1", []TableSchema{sch}) != schemaRoot("net-1", []TableSchema{sch}) {
		t.Fatal("SchemaRoot must delegate")
	}
	if TableSchemaHash("net-1", sch) != tableSchemaHash("net-1", sch) {
		t.Fatal("TableSchemaHash must delegate")
	}
}
```

Caveat for the implementer: `Column` is whatever the field type of `TableSchema.Columns` actually is (check `executor.go:47-58`); `lthash.Hash` comparison — if there is no `Sum()` method, compare via `Bytes()` `bytes.Equal`. Adjust the test to the real API, keeping the delegation assertions.

- [ ] **Step 2: Run to verify failure**

Run: `go test ./pkg/replay/payloadexec/ -run TestExportedHelpersDelegate -v`
Expected: FAIL — `undefined: RowElementHash` etc.

- [ ] **Step 3: Implement**

Append to `pkg/replay/payloadexec/executor.go` (near the internals):

```go
// RowElementHash is the exported canonical per-row LtHash element used by
// every consumer that must agree with the executor bit-for-bit (P1c
// decision 5): the chexec part scanner and the SNode source-side part
// hashing. It delegates to the executor's internal derivation.
func RowElementHash(sch TableSchema, rowID []byte, values []any) (*lthash.Hash, error) {
	return rowElementHash(sch, rowID, values)
}

// SchemaRoot is the exported deployment schema-root derivation. The value
// seeds the arbiter's genesis.schema_root consensus param and is asserted
// at data-plane startup.
func SchemaRoot(networkID string, schemas []TableSchema) string {
	return schemaRoot(networkID, schemas)
}

// TableSchemaHash is the exported per-table schema digest (manifest
// TableManifest.SchemaHash).
func TableSchemaHash(networkID string, t TableSchema) string {
	return tableSchemaHash(networkID, t)
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./pkg/replay/payloadexec/ && bazel test //pkg/replay/payloadexec:all --test_output=errors 2>/dev/null || bazel test //pkg/replay/... --test_output=errors`
Expected: PASS (use whichever Bazel label exists; `bazel query 'tests(//pkg/replay/...)'` lists them).

- [ ] **Step 5: Commit**

```bash
git add pkg/replay/payloadexec/executor.go pkg/replay/payloadexec/exports_test.go
git commit -m "feat(payloadexec): export RowElementHash/SchemaRoot/TableSchemaHash

Same-by-construction helpers for the P1c data-plane roles.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: housegate — chexec `ScanParts` (generalized part scanner) + docker test + PR

**Files:**
- Create: `pkg/replay/chexec/scan.go`
- Test: `pkg/integration/chscan_test.go` (docker-bound; part of the existing `//pkg/integration:integration_test` target — check `pkg/integration/BUILD.bazel` includes new `_test.go` files via glob; if files are listed explicitly, add it)
- Modify (only if needed for the test env): none — reuse `pkg/integration/testenv`.

**Interfaces:**
- Consumes: `payloadexec.RowElementHash` (Task 2), chexec internals `newScanDest`/`derefScan`/`quoteIdent`/`rowIDColumn`, `clickhouse.Conn`.
- Produces (used by arbiter Tasks 11/13/14):
  - `type PartScanResult struct { PartName string; RowLtHash string; RowCount uint64 }`
  - `func ScanParts(ctx context.Context, conn clickhouse.Conn, qualifiedTable string, schema payloadexec.TableSchema, partNames []string) ([]PartScanResult, error)` — scans the named active parts of a REAL table (`hg_unsafe.t` etc.), per-row `RowElementHash` folded into a per-part LtHash; returns results sorted by PartName; errors if any requested part yields zero rows (missing part must be a visible failure, not an empty commitment).

- [ ] **Step 1: Write `scan.go`**

```go
package chexec

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay/payloadexec"
)

// PartScanResult is one part's byte-side content commitment computed from
// rows actually stored on disk.
type PartScanResult struct {
	PartName  string
	RowLtHash string
	RowCount  uint64
}

// ScanParts recomputes per-part row-LtHash commitments from a real
// ClickHouse table (design §7.2 byte-side scan; P1c decision 5). It reads
// `_hg_row_id` plus the schema columns for the named parts and folds each
// row through payloadexec.RowElementHash — the SAME canonical element the
// replay executor uses — so scanner output is comparable to RC claims by
// construction. qualifiedTable is `db.table` (both idents are quoted
// here). A requested part that returns no rows is an error: a missing
// part must surface as refusal, never as an empty commitment.
func ScanParts(ctx context.Context, conn clickhouse.Conn, qualifiedTable string, schema payloadexec.TableSchema, partNames []string) ([]PartScanResult, error) {
	if len(partNames) == 0 {
		return nil, fmt.Errorf("chexec: ScanParts requires at least one part name")
	}
	db, tbl, ok := strings.Cut(qualifiedTable, ".")
	if !ok {
		return nil, fmt.Errorf("chexec: qualifiedTable must be db.table, got %q", qualifiedTable)
	}
	for _, c := range schema.Columns {
		if !supportedColumnType(c.Type) {
			return nil, fmt.Errorf("unsupported column type %q for ClickHouse scan (column %q)", c.Type, c.Name)
		}
	}

	var sel strings.Builder
	sel.WriteString("SELECT _part, ")
	sel.WriteString(rowIDColumn)
	for _, c := range schema.Columns {
		sel.WriteString(", ")
		sel.WriteString(quoteIdent(c.Name))
	}
	fmt.Fprintf(&sel, " FROM %s.%s WHERE _part IN (", quoteIdent(db), quoteIdent(tbl))
	args := make([]any, 0, len(partNames))
	for i, p := range partNames {
		if i > 0 {
			sel.WriteString(", ")
		}
		sel.WriteString("?")
		args = append(args, p)
	}
	sel.WriteString(")")

	rows, err := conn.Query(ctx, sel.String(), args...)
	if err != nil {
		return nil, fmt.Errorf("part scan query: %w", err)
	}
	defer rows.Close()

	type agg struct {
		acc   *lthash.Hash
		count uint64
	}
	byPart := map[string]*agg{}
	for rows.Next() {
		var part string
		var rid []byte
		dests := make([]any, len(schema.Columns)+2)
		dests[0] = &part
		dests[1] = &rid
		holders := make([]any, len(schema.Columns))
		for i, c := range schema.Columns {
			p, err := newScanDest(c.Type)
			if err != nil {
				return nil, err
			}
			dests[i+2] = p
			holders[i] = p
		}
		if err := rows.Scan(dests...); err != nil {
			return nil, fmt.Errorf("part scan row: %w", err)
		}
		values := make([]any, len(schema.Columns))
		for i := range schema.Columns {
			v, err := derefScan(holders[i])
			if err != nil {
				return nil, err
			}
			values[i] = v
		}
		h, err := payloadexec.RowElementHash(schema, append([]byte(nil), rid...), values)
		if err != nil {
			return nil, fmt.Errorf("part %s: %w", part, err)
		}
		a := byPart[part]
		if a == nil {
			a = &agg{acc: lthash.New()}
			byPart[part] = a
		}
		a.acc.AddHash(h)
		a.count++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("part scan rows: %w", err)
	}

	out := make([]PartScanResult, 0, len(partNames))
	for _, name := range partNames {
		a, ok := byPart[name]
		if !ok {
			return nil, fmt.Errorf("part %q not found (or empty) in %s", name, qualifiedTable)
		}
		out = append(out, PartScanResult{
			PartName:  name,
			RowLtHash: "0x" + hex.EncodeToString(a.acc.Bytes()),
			RowCount:  a.count,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PartName < out[j].PartName })
	return out, nil
}
```

Caveats for the implementer: (a) `lthash.Hash` method names — verify `AddHash`/`Bytes` against `pkg/lthash` (they are what `materializer.go`'s callers and `executor.go` use); (b) the `0x`-hex form must match `payloadexec.lthashHex` — check that helper and reuse its format exactly (if it is unexported, keep the inline `"0x"+hex.EncodeToString(...)` but confirm byte source is the same accumulator `Bytes()`); (c) clickhouse-go positional `?` binding — the codebase may use `@name` named args; follow whatever `pkg/integration` CH tests already do.

- [ ] **Step 2: Write the docker test**

Create `pkg/integration/chscan_test.go` — model the setup on the existing `chreplay_test.go` in the same package (reuse its ClickHouse container helper / testenv wiring verbatim; read it first). Test body:

```go
func TestChexecScanParts_MatchesExecutorRowHashing(t *testing.T) {
	// setup: testenv ClickHouse conn (same pattern as chreplay_test.go)
	conn := mustClickHouseConn(t) // whatever helper chreplay_test.go uses

	sch := payloadexec.TableSchema{TableID: "db.scan_t", PartitionBy: "p", Columns: []payloadexec.Column{
		{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"},
	}}
	// create a real MergeTree with _hg_row_id and STOP MERGES semantics
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS hg_scan_test")
	mustExec(t, conn, `CREATE TABLE hg_scan_test.scan_t (_hg_row_id FixedString(32), p String, v UInt64)
		ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
		SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0`)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS hg_scan_test") })

	// one INSERT = one part per touched partition; compute expectation with
	// the executor-side helpers over the same rows
	rid0 := payloadexec.RowID("net-1", "db.scan_t", "acct/1/n", 0)
	rid1 := payloadexec.RowID("net-1", "db.scan_t", "acct/1/n", 1)
	mustExec(t, conn, "INSERT INTO hg_scan_test.scan_t VALUES (?, ?, ?), (?, ?, ?)",
		rid0, "p0", uint64(1), rid1, "p0", uint64(2))

	want := lthash.New()
	h0, _ := payloadexec.RowElementHash(sch, rid0, []any{"p0", uint64(1)})
	h1, _ := payloadexec.RowElementHash(sch, rid1, []any{"p0", uint64(2)})
	want.AddHash(h0)
	want.AddHash(h1)

	// discover the part name via system.parts, then scan it
	partName := activePartNames(t, conn, "hg_scan_test", "scan_t")[0]
	got, err := chexec.ScanParts(context.Background(), conn, "hg_scan_test.scan_t", sch, []string{partName})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if got[0].RowCount != 2 || got[0].RowLtHash != "0x"+hex.EncodeToString(want.Bytes()) {
		t.Fatalf("scan mismatch: %+v", got[0])
	}

	// missing part -> error, not empty commitment
	if _, err := chexec.ScanParts(context.Background(), conn, "hg_scan_test.scan_t", sch, []string{"nope_0_0_0"}); err == nil {
		t.Fatal("missing part must error")
	}
}
```

Write `mustExec` / `activePartNames` (`SELECT name FROM system.parts WHERE database=? AND table=? AND active ORDER BY name`) as small local helpers if the package lacks them.

- [ ] **Step 3: Run the docker test**

Run: `bazel test //pkg/integration:integration_test --test_filter='TestChexecScanParts' --test_output=streamed`
Expected: PASS (docker must be running; the target is tagged `manual` so it does not run in `bazel test //...`).

- [ ] **Step 4: Full housegate gate + PR**

```bash
bazel test //... 2>&1 | tail -5        # docker-less targets all green
git add pkg/replay/chexec/scan.go pkg/integration/chscan_test.go
git commit -m "feat(chexec): ScanParts — generalized on-disk part scanner

Scans named active parts of a real table and folds rows through
payloadexec.RowElementHash, giving the byte-side scanner and the SNode
source the same row->LtHash derivation the replay executor uses.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git push -u origin feat/replay-content-roots
gh pr create --repo housegate/housegate --title "feat(replay): content-addressed roots + P1c data-plane helper exports" --body "Precondition for arbiter P1c (spec PR #79, decision 6/8): content-addressed DataRoot v2, AssembleStateRoot, payloadexec RowElementHash/SchemaRoot/TableSchemaHash exports, chexec ScanParts."
```

**STOP — controller action:** this PR must be reviewed/merged by the user before Task 4 (arbiter pins the merged commit). Report DONE with the PR URL.

---

### Task 4: arbiter — housegate pin bump + wire receive-side exports

**Files (arbiter repo, `/Users/uranuswch/src/sentio_xyz/arbiter`):**
- Modify: `go.mod` (housegate pin), `wire/convert.go` (export two), `wire/dispatch.go` (add FromPB complements)
- Test: `wire/dispatch_from_test.go` (create)
- Possibly modify: `conformance/` + `domains.go` IF they mirror the `safe-snapshot-data` domain or pin manifest-root vectors (check: `grep -rn "safe-snapshot-data" . --include=*.go`) — update the mirror string to `safe-snapshot-data-v2` and re-derive any golden vectors, with a note in the report.

**Interfaces:**
- Consumes: merged housegate commit from Task 3 (`go get housegate/housegate@<merged-commit-sha>`); existing unexported `promoteFromPB`, `cleanupFromPB`, `statementToPB`, `partRefToPB`.
- Produces (used by Tasks 10–15):
  - `func PromoteFromPB(m *pb.PromoteSafePartition) arbiter.PromoteSafePartition` (export existing)
  - `func CleanupFromPB(m *pb.UnsafeCleanup) arbiter.UnsafeCleanup` (export existing)
  - `func StatementFromPB(m *pb.Statement) replay.Statement`
  - `func ReplayJobFromPB(m *pb.ReplayJob) replay.ReplayJob`
  - `func PartRefsFromPB(ms []*pb.PartRef) []arbiter.PartRef`

- [ ] **Step 1: Bump the housegate pin and verify the root change arrived**

```bash
cd /Users/uranuswch/src/sentio_xyz/arbiter
go get housegate/housegate@<MERGED_COMMIT_SHA>   # controller supplies the sha from Task 3's merged PR
go mod tidy && go build ./... && go test ./... 2>&1 | tail -8
```
Expected: build OK. Tests: any arbiter test that asserts a sealed manifest root value derived under the OLD DataRoot may fail (fsm manifest tests use `Seal()` on the fly, so most adapt automatically); `grep -rn "safe-snapshot-data" --include=*.go .` — if `domains.go`/`conformance` mirror the domain string, update to `safe-snapshot-data-v2` and re-derive golden digests (re-run, paste new constants). Any OTHER failure is a STOP.

- [ ] **Step 2: Write the failing converter test**

Create `wire/dispatch_from_test.go`:

```go
package wire

import (
	"testing"

	"housegate/housegate/pkg/replay"
)

func TestReplayJobRoundTrip(t *testing.T) {
	job := replay.ReplayJob{
		BlockSeq: 7, PrevSafeSnapshotID: "0xprev", PrevStateRoot: "0xstate",
		SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0",
		SourceClaimRoot: "0xclaim",
		Statements: []replay.Statement{{
			StatementID: "0xacct/1/n", StatementSeq: 3, SQL: "INSERT INTO db.t VALUES (1)",
			SQLHash: replay.DigestString("INSERT INTO db.t VALUES (1)"), SettingsHash: "0xsettings",
			PayloadRef: "0xpay", PayloadHash: "0xpayh", PayloadLength: 12,
			TargetTableID: "db.t", UserJWS: "a.b.c",
		}},
	}
	got := ReplayJobFromPB(ReplayJobToPB(job))
	if got.BlockSeq != job.BlockSeq || got.PrevSafeSnapshotID != job.PrevSafeSnapshotID ||
		got.PrevStateRoot != job.PrevStateRoot || got.SourceClaimRoot != job.SourceClaimRoot ||
		len(got.Statements) != 1 || got.Statements[0] != job.Statements[0] {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, job)
	}
}
```

- [ ] **Step 3: Run to verify failure** — `go test ./wire/ -run TestReplayJobRoundTrip` → FAIL `undefined: ReplayJobFromPB`.

- [ ] **Step 4: Implement**

In `wire/convert.go`, rename `promoteFromPB` → `PromoteFromPB` and `cleanupFromPB` → `CleanupFromPB` (fix the two `command.go` call sites). Append to `wire/dispatch.go`:

```go
// StatementFromPB is the receive-side complement of statementToPB.
func StatementFromPB(m *pb.Statement) replay.Statement {
	return replay.Statement{
		StatementID:   m.GetStatementId(),
		StatementSeq:  m.GetStatementSeq(),
		SQL:           m.GetSql(),
		SQLHash:       m.GetSqlHash(),
		SettingsHash:  m.GetSettingsHash(),
		PayloadRef:    m.GetPayloadRef(),
		PayloadHash:   m.GetPayloadHash(),
		PayloadLength: m.GetPayloadLength(),
		TargetTableID: m.GetTargetTableId(),
		UserJWS:       m.GetUserJws(),
	}
}

// ReplayJobFromPB is the receive-side complement of ReplayJobToPB.
func ReplayJobFromPB(m *pb.ReplayJob) replay.ReplayJob {
	return replay.ReplayJob{
		BlockSeq:           m.GetBlockSeq(),
		PrevSafeSnapshotID: m.GetPrevSafeSnapshotId(),
		PrevStateRoot:      m.GetPrevStateRoot(),
		SchemaSnapshotID:   m.GetSchemaSnapshotId(),
		ExecutorProfileID:  m.GetExecutorProfileId(),
		SourceClaimRoot:    m.GetSourceClaimRoot(),
		Statements:         mapSlice(m.GetStatements(), StatementFromPB),
	}
}

// PartRefsFromPB converts a ByteSideScanRequest's part list.
func PartRefsFromPB(ms []*pb.PartRef) []arbiter.PartRef {
	return mapSlice(ms, func(m *pb.PartRef) arbiter.PartRef {
		return arbiter.PartRef{
			TableID:       m.GetTableId(),
			PartitionID:   m.GetPartitionId(),
			PartRowLtHash: m.GetPartRowLthash(),
			PartName:      m.GetPartName(),
		}
	})
}
```

Caveat: pb getter spellings (`GetPartRowLthash` vs `GetPartRowLtHash` etc.) — `gen/pb/*.pb.go` is ground truth; mirror whatever `partRefToPB` sets.

- [ ] **Step 5: Run tests + commit**

```bash
go test ./wire/ ./conformance/ && go vet ./...
git add -A && git commit -m "feat(wire): receive-side converters + housegate content-root pin

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: arbiter — anchor `Finality` seam (non-blocking poll) + orchestrator no-spam

**Files:**
- Modify: `anchor/client.go`, `anchor/local.go`, `orchestrator/promotion.go` (anchorBlock), `orchestrator/orchestrator_fakes_test.go` + `orchestrator/promotion_fixtures_test.go` (rename fake methods)
- Test: extend `orchestrator/promotion_test.go`, `anchor/local_test.go`

**Interfaces:**
- Consumes: `fsm.BlockAnchor{BlockSeq, ChainHash, StateRoot, Ref, Anchored, Finality, LastMergeable}` (P1b), `anchor.Local`.
- Produces: `anchor.Client` interface v2 — `Anchor(ctx, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error)`; `Finality(ctx context.Context, ref arbiter.AnchorRef) (finality, lastMergeable bool, err error)`. Consumed by Task 16 (cmd) and P1d.

- [ ] **Step 1: Write the failing tests**

In `anchor/local_test.go` add (adapt to existing test style):

```go
func TestLocal_FinalityIsNonBlockingAndImmediate(t *testing.T) {
	l := NewLocal()
	ref, err := l.Anchor(context.Background(), "0xhash", "0xroot")
	if err != nil {
		t.Fatalf("anchor: %v", err)
	}
	fin, lm, err := l.Finality(context.Background(), ref)
	if err != nil || !fin || !lm {
		t.Fatalf("local finality must be immediate: %v %v %v", fin, lm, err)
	}
}
```

In `orchestrator/promotion_test.go` add a no-spam test: drive a fixture where the block is already `Anchored` with `Finality && LastMergeable` recorded FALSE and a fake anchor whose `Finality` returns `(false, false, nil)`; assert a rescan proposes **zero** `RecordAnchorFinality` commands (count proposals via the existing fake node — the P1b orchestrator fakes count Apply calls; follow `TestPromotion_AnchorRefRecordedBeforeFinalityRetry`'s fixture style). Then flip the fake to `(true, true, nil)` and assert exactly one proposal with both flags.

- [ ] **Step 2: Run to verify failure** — `go test ./anchor/ ./orchestrator/ -run 'Finality|Anchor'` → FAIL (`Finality` undefined; orchestrator still calls `WaitFinality`).

- [ ] **Step 3: Implement**

`anchor/client.go`:

```go
// Client posts one L3 block commitment and reports its finality.
type Client interface {
	Anchor(ctx context.Context, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error)
	// Finality is a non-blocking, point-in-time check for a posted anchor
	// (P1c seam: the orchestrator polls it from the retry scan; a real L2
	// backend returns pending until the chain confirms). It must never
	// block on chain progress.
	Finality(ctx context.Context, ref arbiter.AnchorRef) (finality, lastMergeable bool, err error)
}
```

`anchor/local.go`: rename `WaitFinality` → `Finality` (same body: `return true, true, nil`).

`orchestrator/promotion.go` — replace the tail of `anchorBlock` (from the `WaitFinality` call to the end) with:

```go
	if err := o.d.Node.VerifyLeader(); err != nil {
		return fmt.Errorf("verify leader before anchor finality block %d: %w", ba.BlockSeq, err)
	}
	finality, lastMergeable, err := o.d.Anchor.Finality(ctx, ref)
	if err != nil {
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		o.d.Logger.Warn("anchor finality check failed", "block", ba.BlockSeq, "err", err)
		return nil
	}
	// No-spam: when the poll reports no progress over what the FSM already
	// recorded and the ref is persisted, propose nothing — the retry scan
	// polls again next round without burning a raft log entry per tick.
	if ba.Anchored && finality == ba.Finality && lastMergeable == ba.LastMergeable {
		return nil
	}
	_, err = o.propose(wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{
		L3BlockSeq:           ba.BlockSeq,
		Anchor:               ref,
		FinalityReached:      finality,
		LastMergeableReached: lastMergeable,
	}})
	if err != nil && !errors.Is(err, ErrRejected) {
		return fmt.Errorf("record anchor finality block %d: %w", ba.BlockSeq, err)
	}
	return nil
```

Note the fresh-anchor path (`!ba.Anchored`) still records the ref first (unchanged P1b behavior), so after a crash the new leader skips re-anchoring; the no-spam guard applies only when `ba.Anchored`.

- [ ] **Step 4: Run** — `go test ./anchor/ ./orchestrator/ ./integration/ -count=1` → PASS (integration exercises the local backend end-to-end; behavior is bit-identical).

- [ ] **Step 5: Commit**

```bash
git add anchor/ orchestrator/ && git commit -m "feat(anchor): non-blocking Finality seam; orchestrator polls via retry scan

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: arbiter — `genesis.schema_root` + `genesis.manifest_path` + orchestrator bootstrap publish

**Files:**
- Modify: `config/config.go` + `config/raw.go` (two new Genesis fields), `orchestrator/deps.go` (Genesis manifest + SchemaRoot in Config), `orchestrator/loop.go` (bootstrap publish in rescan), `orchestrator/promotion.go` (publishManifest schema root), `cmd/arbiter/services.go` + `cmd/arbiter/app.go` (load + validate manifest file, wire through), `configs/local.yaml` (document the fields, commented out)
- Test: `config/config_test.go`, `orchestrator/loop_test.go` (bootstrap), `orchestrator/promotion_test.go` (schema-root source), `cmd/arbiter/main_test.go` (manifest load validation)

**Interfaces:**
- Consumes: `replay.SafeSnapshotManifest` (JSON-decodable), `fsm.SafeWatermarkView()`, existing `publishManifest`.
- Produces: `config.GenesisConfig{SchemaSnapshotID, ExecutorProfileID, SchemaRoot string `yaml:"schema_root"`, ManifestPath string `yaml:"manifest_path"`}`; `orchestrator.Config.SchemaRoot string`; `orchestrator.Deps.Genesis *replay.SafeSnapshotManifest`. Task 17's harness seeds genesis through these.

- [ ] **Step 1: Failing tests**

`config/config_test.go` additions: (a) yaml with `genesis: {schema_root: "0xsr", manifest_path: "/tmp/g.json"}` loads both fields; (b) `manifest_path` set with EMPTY `schema_root` → Validate error containing `genesis.schema_root`.

`orchestrator/loop_test.go` addition — bootstrap publish:

```go
func TestLoop_BootstrapPublishesGenesisManifestOnce(t *testing.T) {
	// fixture: fresh FSM (empty watermark), Deps.Genesis = sealed manifest with
	// SafeBlockSeq 0 (build via replay.SafeSnapshotManifest{...}.Seal() with a
	// table set; SchemaRoot arbitrary here). Run one rescan (use the afterRescan
	// hook pattern from the P1b loop tests). Assert:
	//   fsm.SafeWatermarkView().SnapshotID == genesis.SnapshotID
	// Run a second rescan; assert no error and watermark unchanged (the second
	// propose is Rejected by the watermark guard and swallowed as ErrRejected).
}
```

(Write it concretely in the fixture style of the neighboring `TestLoop_*` tests — they construct `Deps` with the real FSM + fake node; the fake node's Apply drives `fsm.Apply` so the watermark really advances.)

`orchestrator/promotion_test.go` addition: with `Cfg.SchemaRoot = "0xconfigured"`, a published (non-genesis) manifest's `SchemaRoot` must be `"0xconfigured"`; with `Cfg.SchemaRoot == ""` it must remain `replay.DigestString("schema:" + schemaID)` (transitional fallback keeping P1b fixtures green — the flagship and sample configs always set it).

- [ ] **Step 2: Run to verify failures** — `go test ./config/ ./orchestrator/ -run 'Genesis|SchemaRoot'` → FAIL (unknown fields).

- [ ] **Step 3: Implement**

`config/config.go` — extend GenesisConfig:

```go
type GenesisConfig struct {
	SchemaSnapshotID  string `yaml:"schema_snapshot_id"`
	ExecutorProfileID string `yaml:"executor_profile_id"`
	// SchemaRoot is the deployment schema-root consensus value, computed
	// offline with payloadexec.SchemaRoot over the table set. Required when
	// manifest_path is set; used verbatim by manifest publishing.
	SchemaRoot string `yaml:"schema_root"`
	// ManifestPath points at the sealed genesis SafeSnapshotManifest JSON
	// (payloadexec GenesisSnapshot output). Optional: without it the
	// orchestrator never bootstrap-publishes (P1b-compatible).
	ManifestPath string `yaml:"manifest_path"`
}
```

Validation (in `Validate`): `if c.Genesis.ManifestPath != "" && c.Genesis.SchemaRoot == "" { errs = append(errs, errors.New("genesis.schema_root is required when genesis.manifest_path is set")) }`. Mirror the two fields through `config/raw.go` (plain strings — no pointer defaulting needed).

`orchestrator/deps.go`: add `Genesis *replay.SafeSnapshotManifest` to `Deps`; add `SchemaRoot string` to `Config`.

`orchestrator/loop.go` — at the TOP of `rescan` (before `WorkSet`):

```go
	if o.d.Genesis != nil && o.d.FSM.SafeWatermarkView().SnapshotID == "" {
		if _, err := o.propose(wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: *o.d.Genesis}}); err != nil && !errors.Is(err, ErrRejected) {
			return fmt.Errorf("bootstrap genesis manifest: %w", err)
		}
	}
```

`orchestrator/promotion.go` — in `publishManifest`, replace the SchemaRoot line:

```go
	schemaRoot := o.d.Cfg.SchemaRoot
	if schemaRoot == "" {
		// Transitional fallback (pre-P1c fixtures); production configs set
		// genesis.schema_root and this branch never runs.
		schemaRoot = replay.DigestString("schema:" + schemaID)
	}
```
and use `SchemaRoot: schemaRoot` in the manifest literal.

`cmd/arbiter/services.go` — load the genesis manifest when configured (new helper in `services.go`):

```go
func loadGenesisManifest(cfg config.Config) (*replay.SafeSnapshotManifest, error) {
	if cfg.Genesis.ManifestPath == "" {
		return nil, nil
	}
	b, err := os.ReadFile(cfg.Genesis.ManifestPath)
	if err != nil {
		return nil, fmt.Errorf("genesis manifest: %w", err)
	}
	var m replay.SafeSnapshotManifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("genesis manifest: %w", err)
	}
	if err := m.Validate(); err != nil {
		return nil, fmt.Errorf("genesis manifest: %w", err)
	}
	if m.SafeBlockSeq != 0 {
		return nil, fmt.Errorf("genesis manifest: safe_block_seq must be 0, got %d", m.SafeBlockSeq)
	}
	if m.SchemaRoot != cfg.Genesis.SchemaRoot {
		return nil, fmt.Errorf("genesis manifest schema_root %s != genesis.schema_root %s", m.SchemaRoot, cfg.Genesis.SchemaRoot)
	}
	if m.SchemaSnapshotID != cfg.Genesis.SchemaSnapshotID {
		return nil, fmt.Errorf("genesis manifest schema_snapshot_id %s != genesis.schema_snapshot_id %s", m.SchemaSnapshotID, cfg.Genesis.SchemaSnapshotID)
	}
	return &m, nil
}
```

Call it from `newServerAndLoop` (return error on failure — startup fail-fast) and pass into `orchestrator.Deps{..., Genesis: gm}` plus `Cfg.SchemaRoot: cfg.Genesis.SchemaRoot`. `cmd/arbiter/main_test.go`: table-test `loadGenesisManifest` (valid file, bad JSON, SafeBlockSeq!=0, schema_root mismatch — build the valid file by sealing a manifest in the test and writing JSON to t.TempDir()).

- [ ] **Step 4: Run** — `go test ./config/ ./orchestrator/ ./cmd/... ./integration/ -count=1` → PASS (P1b integration unaffected: no manifest_path configured).

- [ ] **Step 5: Commit**

```bash
git add config/ orchestrator/ cmd/ configs/ && git commit -m "feat(config,orchestrator): genesis schema_root + manifest bootstrap publish

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: arbiter — `dataplane` Client + `WithLeaderRetry`

**Files:**
- Create: `dataplane/client.go`
- Test: `dataplane/client_test.go`

**Interfaces:**
- Consumes: `pb.NotLeader` detail (P1b server semantics), grpc insecure creds.
- Produces (used by Tasks 8–16):
  - `type Peer struct { ID, GRPCAddr string }`
  - `type Config struct { Peers []Peer; DialTimeout, RetryBackoffMin, RetryBackoffMax time.Duration }` (zero values default to 5s / 100ms / 3s)
  - `type Client struct{ ... }`; `func New(cfg Config) (*Client, error)` (error on empty/duplicate peers); `func (c *Client) Close()`
  - `func (c *Client) WithLeaderRetry(ctx context.Context, fn func(ctx context.Context, conn *grpc.ClientConn) error) error`
  - `func (c *Client) LeaderConn(ctx context.Context) (*grpc.ClientConn, error)` — conn to last-known/discovered leader (used by subscriptions)
  - `func notLeaderHint(err error) (string, bool)` (internal), `func retryableStatus(c codes.Code) bool` (internal)

- [ ] **Step 1: Write the failing test**

Create `dataplane/client_test.go` — an in-process fake: three grpc servers each implementing ONE cheap service (`pb.SafeStateServer` is convenient: implement `GetSafeWatermark`); two return `FAILED_PRECONDITION` + `pb.NotLeader{LeaderAddr: "n3"}`, one (n3) returns success. Test cases:

```go
func TestWithLeaderRetry_FollowsNotLeaderHint(t *testing.T) {
	// start fake peers n1,n2 (not leader, hint n3), n3 (leader)
	// cl := New(Config{Peers: [...3 real listen addrs...]})
	// err := cl.WithLeaderRetry(ctx, func(ctx, conn) error {
	//     _, err := pb.NewSafeStateClient(conn).GetSafeWatermark(ctx, &pb.GetSafeWatermarkRequest{})
	//     return err
	// })
	// assert err == nil AND the n3 fake served exactly 1 success AND at most
	// one non-leader peer was tried before re-homing (hint short-circuits the rotation)
}

func TestWithLeaderRetry_RotatesOnUnavailableAndStopsOnCtx(t *testing.T) {
	// peers: one dead addr (closed listener -> Unavailable), one NotLeader-with-
	// empty-hint, one leader. Assert success. Then: all three dead; assert
	// WithLeaderRetry returns ctx.Err() once the deadline passes (bounded backoff).
}

func TestWithLeaderRetry_NonRetryableStatusPropagates(t *testing.T) {
	// leader fake returns codes.InvalidArgument; assert WithLeaderRetry returns
	// it immediately (no rotation, no retry).
}
```

Write these fully (the P1b `integration/cluster_control_test.go` leader-retry tests are the pattern source — but do NOT import from integration; re-create small local fakes).

- [ ] **Step 2: Run to verify failure** — `go test ./dataplane/ -v` → FAIL (package does not exist).

- [ ] **Step 3: Implement `dataplane/client.go`**

```go
// Package dataplane is the shared arbiter-client foundation for data-plane
// roles (P1c design §3): peer connection management, leader discovery via
// pb.NotLeader ServerID hints, retry loops, and subscription maintenance.
package dataplane

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Peer is one arbiter node. ID must equal the node's raft ServerID: the
// pb.NotLeader detail carries that ID (P1b v1 semantics) and this table is
// how clients resolve it to a dialable address.
type Peer struct {
	ID       string
	GRPCAddr string
}

type Config struct {
	Peers           []Peer
	DialTimeout     time.Duration // default 5s
	RetryBackoffMin time.Duration // default 100ms
	RetryBackoffMax time.Duration // default 3s
}

func (c Config) withDefaults() Config {
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.RetryBackoffMin <= 0 {
		c.RetryBackoffMin = 100 * time.Millisecond
	}
	if c.RetryBackoffMax <= 0 {
		c.RetryBackoffMax = 3 * time.Second
	}
	return c
}

// Client manages one lazily-dialed conn per peer and tracks the last-known
// leader. All methods are safe for concurrent use.
type Client struct {
	cfg   Config
	order []string // peer IDs in config order

	mu         sync.Mutex
	conns      map[string]*grpc.ClientConn
	addrs      map[string]string
	lastLeader string
}

func New(cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("dataplane: at least one peer is required")
	}
	c := &Client{cfg: cfg, conns: map[string]*grpc.ClientConn{}, addrs: map[string]string{}}
	for _, p := range cfg.Peers {
		if p.ID == "" || p.GRPCAddr == "" {
			return nil, fmt.Errorf("dataplane: peer id and addr are required")
		}
		if _, dup := c.addrs[p.ID]; dup {
			return nil, fmt.Errorf("dataplane: duplicate peer id %q", p.ID)
		}
		c.addrs[p.ID] = p.GRPCAddr
		c.order = append(c.order, p.ID)
	}
	return c, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, id)
	}
}

func (c *Client) conn(id string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[id]; ok {
		return conn, nil
	}
	addr, ok := c.addrs[id]
	if !ok {
		return nil, fmt.Errorf("dataplane: unknown peer %q", id)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dataplane: dial %s: %w", addr, err)
	}
	c.conns[id] = conn
	return conn, nil
}

func (c *Client) setLeader(id string) {
	c.mu.Lock()
	c.lastLeader = id
	c.mu.Unlock()
}

// candidateOrder returns peer IDs starting from `preferred` (or the
// last-known leader), then the rest in config order.
func (c *Client) candidateOrder(preferred string) []string {
	c.mu.Lock()
	if preferred == "" {
		preferred = c.lastLeader
	}
	c.mu.Unlock()
	if preferred == "" {
		return c.order
	}
	out := make([]string, 0, len(c.order))
	if _, known := c.addrs[preferred]; known {
		out = append(out, preferred)
	}
	for _, id := range c.order {
		if id != preferred {
			out = append(out, id)
		}
	}
	return out
}

// WithLeaderRetry runs fn against the leader, following pb.NotLeader hints
// and rotating peers on transport-level failures, with capped exponential
// backoff between full rotations. It returns fn's error verbatim when the
// status is not retryable (the caller's real result), and ctx.Err() when
// the context ends first.
func (c *Client) WithLeaderRetry(ctx context.Context, fn func(ctx context.Context, conn *grpc.ClientConn) error) error {
	backoff := c.cfg.RetryBackoffMin
	preferred := ""
	for {
		for _, id := range c.candidateOrder(preferred) {
			if err := ctx.Err(); err != nil {
				return err
			}
			conn, err := c.conn(id)
			if err != nil {
				continue
			}
			err = fn(ctx, conn)
			if err == nil {
				c.setLeader(id)
				return nil
			}
			if hint, ok := notLeaderHint(err); ok {
				if hint != "" {
					preferred = hint
				}
				continue
			}
			if !retryableStatus(status.Code(err)) {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > c.cfg.RetryBackoffMax {
			backoff = c.cfg.RetryBackoffMax
		}
	}
}

// LeaderConn returns a conn to the last-known leader, probing with a
// zero-cost GetSafeWatermark call when unknown. Subscriptions use it to
// pick their stream target; the stream itself re-verifies leadership
// server-side.
func (c *Client) LeaderConn(ctx context.Context) (*grpc.ClientConn, error) {
	var out *grpc.ClientConn
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		// Any leader-gated cheap unary works as a probe; SafeState is served
		// by every node, so probe the subscribe gate instead: a follower
		// answers streams with NotLeader. Use GetSafeWatermark + trust the
		// caller's stream to re-home if the answer is stale.
		_, err := pb.NewSafeStateClient(conn).GetSafeWatermark(ctx, &pb.GetSafeWatermarkRequest{})
		if err != nil {
			return err
		}
		out = conn
		return nil
	})
	return out, err
}

func notLeaderHint(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return "", false
	}
	for _, d := range st.Details() {
		if nl, ok := d.(*pb.NotLeader); ok {
			return nl.GetLeaderAddr(), true
		}
	}
	return "", true
}

func retryableStatus(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Canceled || code == codes.Unknown
}

var _ = errors.Is // keep errors import if unused after edits
```

Caveats: (a) `GetSafeWatermark` is served by FOLLOWERS too (local read) — so `LeaderConn` is a *reachability* probe plus lastLeader affinity, not a strict leader proof; the subscribe RPC itself is leader-gated and returns NotLeader with a hint, which Task 8's loop feeds back via `WithLeaderRetry` semantics. That is the intended design (subscriptions self-correct); keep the comment honest. (b) Remove the `errors` var-keep line if `errors` ends up used/unused appropriately.

- [ ] **Step 4: Run** — `go test ./dataplane/ -race -count=1` → PASS.

- [ ] **Step 5: Commit** — `git add dataplane/ && git commit -m "feat(dataplane): client foundation with NotLeader re-homing retry loop

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"`

---

### Task 8: arbiter — `dataplane` subscriptions

**Files:**
- Create: `dataplane/subscribe.go`
- Test: `dataplane/subscribe_test.go`

**Interfaces:**
- Consumes: Task 7 `Client` (`candidateOrder`/`conn`/`setLeader`/`notLeaderHint` internals OK — same package), `pb.VerifierGatewayClient`, `pb.PromotionGatewayClient`.
- Produces (used by Tasks 10/12/16/17):
  - `func (c *Client) RunVerifierSubscription(ctx context.Context, replicaID string, onDispatch func(*pb.VerifierDispatch) error) error` — blocks until ctx ends; returns ctx.Err().
  - `func (c *Client) RunPromotionSubscription(ctx context.Context, nodeID string, onCommand func(*pb.PromotionCommand) error) error` — same contract.
  - Callback errors are LOGGED-equivalent (returned to an optional hook) but never kill the loop; delivery is sequential.

- [ ] **Step 1: Write the failing test**

`dataplane/subscribe_test.go` with an in-process fake gateway server (implements `pb.VerifierGatewayServer.SubscribeVerifierDispatch` streaming N messages then returning NotLeader; a second fake being the new leader streaming more). Cases: (a) messages delivered in order to the callback; (b) on stream end with NotLeader the loop re-homes to the hinted peer and keeps delivering (assert total received across both fakes); (c) callback error does not stop delivery; (d) ctx cancel returns promptly with ctx.Err(). Mirror for promotions is structural — test one gateway deeply + one smoke for the other.

- [ ] **Step 2: Run to verify failure** — `go test ./dataplane/ -run Subscription` → FAIL (undefined).

- [ ] **Step 3: Implement `dataplane/subscribe.go`**

```go
package dataplane

import (
	"context"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
)

// runSubscription is the generic re-homing stream loop: open the stream on
// a candidate peer, deliver messages sequentially, and on ANY stream end
// (leadership loss closes server-side, NotLeader on subscribe, transport
// error) re-home with capped backoff. Callback errors never kill the loop:
// the arbiter retry ticker re-dispatches work, so dropping one delivery is
// always recoverable (scan-is-truth).
func runSubscription[T any](
	ctx context.Context,
	c *Client,
	open func(ctx context.Context, conn *grpc.ClientConn) (interface{ Recv() (T, error) }, error),
	deliver func(T) error,
) error {
	backoff := c.cfg.RetryBackoffMin
	preferred := ""
	for {
		delivered := false
		for _, id := range c.candidateOrder(preferred) {
			if err := ctx.Err(); err != nil {
				return err
			}
			conn, err := c.conn(id)
			if err != nil {
				continue
			}
			stream, err := open(ctx, conn)
			if err != nil {
				if hint, ok := notLeaderHint(err); ok && hint != "" {
					preferred = hint
				}
				continue
			}
			c.setLeader(id)
			for {
				msg, err := stream.Recv()
				if err != nil {
					if hint, ok := notLeaderHint(err); ok && hint != "" {
						preferred = hint
					}
					break
				}
				delivered = true
				backoff = c.cfg.RetryBackoffMin
				_ = deliver(msg) // callback errors are the role's concern; loop survives
			}
		}
		if !delivered {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff *= 2; backoff > c.cfg.RetryBackoffMax {
				backoff = c.cfg.RetryBackoffMax
			}
		}
	}
}

// RunVerifierSubscription maintains the VerifierGateway dispatch stream for
// replicaID until ctx ends.
func (c *Client) RunVerifierSubscription(ctx context.Context, replicaID string, onDispatch func(*pb.VerifierDispatch) error) error {
	return runSubscription(ctx, c,
		func(ctx context.Context, conn *grpc.ClientConn) (interface{ Recv() (*pb.VerifierDispatch, error) }, error) {
			return pb.NewVerifierGatewayClient(conn).SubscribeVerifierDispatch(ctx, &pb.VerifierHello{ReplicaId: replicaID})
		}, onDispatch)
}

// RunPromotionSubscription maintains the PromotionGateway command stream
// for nodeID until ctx ends.
func (c *Client) RunPromotionSubscription(ctx context.Context, nodeID string, onCommand func(*pb.PromotionCommand) error) error {
	return runSubscription(ctx, c,
		func(ctx context.Context, conn *grpc.ClientConn) (interface{ Recv() (*pb.PromotionCommand, error) }, error) {
			return pb.NewPromotionGatewayClient(conn).SubscribePromotions(ctx, &pb.SNodeHello{NodeId: nodeID})
		}, onCommand)
}
```

Caveats: hello message/field names per `gen/pb` (P1b used `pb.VerifierHello{ReplicaId}` / `pb.SNodeHello{NodeId}` — mirror the server tests); the generic `interface{ Recv() (T, error) }` constraint works on grpc's generated stream types.

- [ ] **Step 4: Run** — `go test ./dataplane/ -race -count=1` → PASS.

- [ ] **Step 5: Commit** — `git add dataplane/ && git commit -m "feat(dataplane): re-homing gateway subscriptions

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"`

---

### Task 9: arbiter — `dataplane.ManifestStore` + `fspayload`

**Files:**
- Create: `dataplane/manifests.go`, `dataplane/fspayload/store.go`
- Test: `dataplane/manifests_test.go`, `dataplane/fspayload/store_test.go`

**Interfaces:**
- Consumes: `replay.SnapshotStore` (`GetSafeSnapshot(ctx, id) (SafeSnapshotManifest, error)`), `replay.PayloadStore` (`GetPayload(ctx, ref) ([]byte, error)`), `wire.ManifestFromPB`, `pb.SafeStateClient`, Task 7 `Client`.
- Produces (used by Tasks 10–16):
  - `func NewManifestStore(c *Client) *ManifestStore`; `var _ replay.SnapshotStore = (*ManifestStore)(nil)` — fetches `SafeState.GetManifest{SnapshotId}` via `WithLeaderRetry`, converts, caches append-only (content-addressed ⇒ never invalidate); rejects empty id with an error (genesis handling lives in the replay core, which never asks for "").
  - `package fspayload`: `func New(dir string) (*Store, error)` (mkdir-all); `func (s *Store) Put(ref string, payload []byte) error` (temp+fsync+rename, idempotent overwrite-same); `func (s *Store) GetPayload(ctx context.Context, ref string) ([]byte, error)`; `var _ replay.PayloadStore = (*Store)(nil)`. Refs are hex digests — reject refs containing `/` or `..` (path traversal guard).

- [ ] **Step 1: Failing tests**

`dataplane/manifests_test.go`: fake `pb.SafeStateServer` returning a sealed manifest for id X and NotFound otherwise; assert (a) fetch converts and `Validate()`s clean, (b) second fetch of X does NOT hit the server (count calls — cache), (c) unknown id error propagates, (d) empty id errors without a server call.
`dataplane/fspayload/store_test.go`: put/get round trip; get missing ref errors; `Put` then `Put` same bytes idempotent; ref `"../evil"` rejected; concurrent `Put`/`GetPayload` race-free (`-race`).

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement**

`dataplane/manifests.go`:

```go
package dataplane

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter/wire"
)

// ManifestStore adapts the arbiter SafeState RPC to replay.SnapshotStore.
// Manifests are content-addressed, so the cache is append-only and never
// invalidates. v1 reads go through the leader-retry loop (follower-served
// manifest reads are a recorded roll-forward).
type ManifestStore struct {
	c  *Client
	mu sync.Mutex
	m  map[string]replay.SafeSnapshotManifest
}

var _ replay.SnapshotStore = (*ManifestStore)(nil)

func NewManifestStore(c *Client) *ManifestStore {
	return &ManifestStore{c: c, m: map[string]replay.SafeSnapshotManifest{}}
}

func (s *ManifestStore) GetSafeSnapshot(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error) {
	if snapshotID == "" {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("manifest store: empty snapshot id")
	}
	s.mu.Lock()
	if m, ok := s.m[snapshotID]; ok {
		s.mu.Unlock()
		return m, nil
	}
	s.mu.Unlock()

	var out replay.SafeSnapshotManifest
	err := s.c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewSafeStateClient(conn).GetManifest(ctx, &pb.SnapshotRef{SnapshotId: snapshotID})
		if err != nil {
			return err
		}
		out = wire.ManifestFromPB(resp)
		return nil
	})
	if err != nil {
		return replay.SafeSnapshotManifest{}, err
	}
	if err := out.Validate(); err != nil {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("manifest %s failed validation: %w", snapshotID, err)
	}
	s.mu.Lock()
	s.m[snapshotID] = out
	s.mu.Unlock()
	return out, nil
}
```

`dataplane/fspayload/store.go`:

```go
// Package fspayload is the filesystem replay.PayloadStore (P1c v1): the
// SNode spools payloads at ingest and co-located verifiers read them back
// by content-addressed ref. A DA-network store is a later backend behind
// the same interface.
package fspayload

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"housegate/housegate/pkg/replay"
)

type Store struct{ dir string }

var _ replay.PayloadStore = (*Store)(nil)

func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("fspayload: %w", err)
	}
	return &Store{dir: dir}, nil
}

func (s *Store) path(ref string) (string, error) {
	if ref == "" || strings.ContainsAny(ref, "/\\") || strings.Contains(ref, "..") {
		return "", fmt.Errorf("fspayload: invalid ref %q", ref)
	}
	return filepath.Join(s.dir, ref), nil
}

// Put spools payload bytes under their content-addressed ref. Writes are
// temp+fsync+rename so a crash never leaves a torn payload; re-putting the
// same ref is idempotent.
func (s *Store) Put(ref string, payload []byte) error {
	p, err := s.path(ref)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(s.dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("fspayload: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(payload); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fspayload: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("fspayload: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("fspayload: %w", err)
	}
	if err := os.Rename(tmp.Name(), p); err != nil {
		return fmt.Errorf("fspayload: %w", err)
	}
	return nil
}

func (s *Store) GetPayload(_ context.Context, ref string) ([]byte, error) {
	p, err := s.path(ref)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("fspayload: payload %s: %w", ref, err)
	}
	return b, nil
}
```

- [ ] **Step 4: Run** — `go test ./dataplane/... -race -count=1` → PASS.

- [ ] **Step 5: Commit** — `git add dataplane/ && git commit -m "feat(dataplane): SafeState-backed ManifestStore + filesystem payload store

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"`

---

### Task 10: arbiter — `verifier` role core (config, registration, dispatch loop; fake replay core)

**Files:**
- Create: `verifier/verifier.go`, `verifier/config.go`
- Test: `verifier/verifier_test.go`

**Interfaces:**
- Consumes: Task 7–9 `dataplane.Client`/subscriptions/`ManifestStore`/`fspayload`, Task 4 `wire.ReplayJobFromPB`/`PartRefsFromPB`, `wire.AttestationToPB`+`ScanToPB` (P1b exports), `replay.Verifier`, `payloadexec.NewEd25519Signer`, `arbiter.ByteSideScanMsg` (+`Body()`, `DomainByteSideScan`, `replay.CanonicalDigest`), ed25519.
- Produces (used by Tasks 16/17):
  - `type Config struct { ReplicaID string; Ed25519Seed []byte; NetworkID string; SchemaSnapshotID string; SchemaRoot string; Tables []payloadexec.TableSchema; UnsafeDatabase string /* default "hg_unsafe" */ }`
  - `type Deps struct { Client *dataplane.Client; Replay replayCore; Scanner scanner; Logger *slog.Logger }` where `type replayCore interface { Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) }` and `type scanner interface { Scan(ctx context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error) }` (both satisfied by Task 11's real impls; fakes in tests)
  - `func New(cfg Config, d Deps) (*Role, error)` — validates config, asserts `payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables) == cfg.SchemaRoot` (startup schema assertion, spec §8)
  - `func (r *Role) Register(ctx context.Context) error` — RegisterNode{roles:[VERIFIER], pubkey} + MarkActive via WithLeaderRetry
  - `func (r *Role) Run(ctx context.Context) error` — subscription loop; dispatch → handleReplayJob / handleScanRequest

- [ ] **Step 1: Failing tests** (`verifier/verifier_test.go`)

Fakes: `fakeReplayCore` (records jobs, returns a canned signed attestation or error), `fakeScanner`, plus an in-process arbiter-ish gRPC fake exposing VerifierGateway (stream + SubmitAttestation/SubmitByteSideScan recording) and Membership + SafeState (for Client). Reuse the fake-server patterns from `dataplane/` tests. Cases:

1. `TestNew_AssertsSchemaRoot` — cfg.SchemaRoot mismatching `payloadexec.SchemaRoot(...)` → error naming `schema_root`.
2. `TestRegister_SendsVerifierRegistration` — fake Membership records `NodeRegistration{NodeId, Roles: [VERIFIER], Ed25519Pubkey == seed-derived public key}` then MarkActive.
3. `TestRun_ReplayJobFlowsToSubmitAttestation` — push a `VerifierDispatch_ReplayJob` through the fake stream; assert fakeReplayCore got the converted `replay.ReplayJob` AND the fake gateway received the attestation from the canned result.
4. `TestRun_ReplayCoreErrorMeansNoAttestation` — core errors → nothing submitted, loop alive (push a second job, it processes).
5. `TestRun_ScanRequestFlowsToSubmitByteSideScan` — push `VerifierDispatch_ByteSideScan`; assert submitted `pb.ByteSideScanMsg` has: parts = fakeScanner output, `ScanHash == replay.CanonicalDigest(arbiter.DomainByteSideScan, body)` recomputed in the test, and the ed25519 signature verifies over the scan-hash string bytes with the config pubkey.
6. `TestRun_ScannerErrorMeansNoScanSubmission`.

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement**

`verifier/config.go`:

```go
package verifier

import (
	"crypto/ed25519"
	"errors"
	"fmt"

	"housegate/housegate/pkg/replay/payloadexec"
)

type Config struct {
	ReplicaID        string
	Ed25519Seed      []byte // 32 bytes
	NetworkID        string
	SchemaSnapshotID string
	SchemaRoot       string // must equal payloadexec.SchemaRoot(NetworkID, Tables)
	Tables           []payloadexec.TableSchema
	UnsafeDatabase   string // default "hg_unsafe"
}

func (c *Config) validate() error {
	var errs []error
	if c.ReplicaID == "" {
		errs = append(errs, errors.New("replica id is required"))
	}
	if len(c.Ed25519Seed) != ed25519.SeedSize {
		errs = append(errs, fmt.Errorf("ed25519 seed must be %d bytes", ed25519.SeedSize))
	}
	if c.NetworkID == "" {
		errs = append(errs, errors.New("network id is required"))
	}
	if c.SchemaSnapshotID == "" {
		errs = append(errs, errors.New("schema snapshot id is required"))
	}
	if len(c.Tables) == 0 {
		errs = append(errs, errors.New("at least one table schema is required"))
	}
	if c.UnsafeDatabase == "" {
		c.UnsafeDatabase = "hg_unsafe"
	}
	if len(errs) == 0 {
		if got := payloadexec.SchemaRoot(c.NetworkID, c.Tables); got != c.SchemaRoot {
			errs = append(errs, fmt.Errorf("schema_root mismatch: configured %s, computed %s (genesis.schema_root must equal the deployment schema root)", c.SchemaRoot, got))
		}
	}
	return errors.Join(errs...)
}
```

`verifier/verifier.go`:

```go
// Package verifier is the P1c Verifier data-plane role: it subscribes to
// the arbiter VerifierGateway, replays dispatched blocks through
// pkg/replay (chexec-backed), byte-scans candidate parts on its local
// replica, and submits signed evidence. Design §4 of the P1c spec.
package verifier

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"log/slog"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/dataplane"
	"github.com/sentioxyz/arbiter/wire"
)

type replayCore interface {
	Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error)
}

type scanner interface {
	Scan(ctx context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error)
}

type Deps struct {
	Client  *dataplane.Client
	Replay  replayCore
	Scanner scanner
	Logger  *slog.Logger
}

type Role struct {
	cfg  Config
	d    Deps
	priv ed25519.PrivateKey
}

func New(cfg Config, d Deps) (*Role, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("verifier config: %w", err)
	}
	if d.Client == nil || d.Replay == nil || d.Scanner == nil {
		return nil, fmt.Errorf("verifier: client, replay core, and scanner are required")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	return &Role{cfg: cfg, d: d, priv: ed25519.NewKeyFromSeed(cfg.Ed25519Seed)}, nil
}

// Register enters this verifier into arbiter membership (v1 trusted
// self-activation; snapshot-sync proof is a later phase).
func (r *Role) Register(ctx context.Context) error {
	pub := r.priv.Public().(ed25519.PublicKey)
	err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).RegisterNode(ctx, &pb.NodeRegistration{
			NodeId: r.cfg.ReplicaID, Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}, Ed25519Pubkey: pub,
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("register verifier: %w", err)
	}
	err = r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).MarkActive(ctx, &pb.NodeRef{NodeId: r.cfg.ReplicaID})
		return err
	})
	if err != nil {
		return fmt.Errorf("mark verifier active: %w", err)
	}
	return nil
}

// Run blocks on the dispatch subscription until ctx ends.
func (r *Role) Run(ctx context.Context) error {
	return r.d.Client.RunVerifierSubscription(ctx, r.cfg.ReplicaID, func(d *pb.VerifierDispatch) error {
		switch msg := d.GetDispatch().(type) {
		case *pb.VerifierDispatch_ReplayJob:
			return r.handleReplayJob(ctx, msg.ReplayJob)
		case *pb.VerifierDispatch_ByteSideScan:
			return r.handleScanRequest(ctx, msg.ByteSideScan)
		default:
			r.d.Logger.Warn("unknown verifier dispatch", "type", fmt.Sprintf("%T", d.GetDispatch()))
			return nil
		}
	})
}

func (r *Role) handleReplayJob(ctx context.Context, m *pb.ReplayJob) error {
	job := wire.ReplayJobFromPB(m)
	att, err := r.d.Replay.Verify(ctx, job)
	if err != nil {
		// Pre-receipt failure: refusal to attest (Appendix C.4). The arbiter
		// retry ticker re-dispatches; a MISMATCH is not an error and flows
		// through as a signed MatchSourceRoot=false attestation.
		r.d.Logger.Warn("replay verify failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitAttestation(ctx, wire.AttestationToPB(att))
		return err
	})
}

func (r *Role) handleScanRequest(ctx context.Context, m *pb.ByteSideScanRequest) error {
	parts := wire.PartRefsFromPB(m.GetParts())
	scans, err := r.d.Scanner.Scan(ctx, parts)
	if err != nil {
		r.d.Logger.Warn("byte-side scan failed; refusing to attest", "block", m.GetBlockSeq(), "err", err)
		return err
	}
	msg := arbiter.ByteSideScanMsg{ReplicaID: r.cfg.ReplicaID, BlockSeq: m.GetBlockSeq(), Parts: scans}
	hash, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, msg.Body())
	if err != nil {
		return fmt.Errorf("scan hash: %w", err)
	}
	msg.ScanHash = hash
	msg.Signature = hex.EncodeToString(ed25519.Sign(r.priv, []byte(hash)))
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitByteSideScan(ctx, wire.ScanToPB(msg))
		return err
	})
}
```

Caveats: exact pb enum/message spellings (`NodeRole_NODE_ROLE_VERIFIER`, `NodeRegistration` field names) and `wire.ScanToPB`'s input type — mirror P1a/P1b usage (`fsm` tests and `server` tests show the working spellings); `replay.CanonicalDigest` signature (string, any) → check `pkg/replay/hash.go` (P1a used it via mirrors — the arbiter `authority`/`fsm` code shows the call shape); the FSM verifies signatures as hex-no-0x over hash-STRING bytes (P1a convention) — the code above matches.

- [ ] **Step 4: Run** — `go test ./verifier/ -race -count=1` → PASS.

- [ ] **Step 5: Commit** — `git add verifier/ && git commit -m "feat(verifier): role core — registration, dispatch loop, signed evidence

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"`

---

### Task 11: arbiter — verifier real backends (replay wiring + byte-side scanner) + docker test

**Files:**
- Create: `verifier/backends.go` (real replayCore + scanner constructors), `verifier/scanner_test.go` (docker-gated)
- Test: `verifier/scanner_test.go`

**Interfaces:**
- Consumes: `chexec.NewMaterializer`, `chexec.ScanParts` (Task 3), `payloadexec.NewWithMaterializer`/`NewEd25519Signer`, `replay.Verifier`, `dataplane.ManifestStore`, `fspayload.Store`, `clickhouse.Conn`.
- Produces (used by Task 16/17):
  - `func NewReplayCore(cfg Config, conn clickhouse.Conn, manifests replay.SnapshotStore, payloads replay.PayloadStore) (*replay.Verifier, error)` — assembles `replay.Verifier{Snapshots, Payloads, Executor: payloadexec.NewWithMaterializer(cfg.NetworkID, chexec.NewMaterializer(cfg.NetworkID, conn), cfg.Tables...), Signer: payloadexec.NewEd25519Signer(cfg.ReplicaID, cfg.Ed25519Seed)}`
  - `func NewScanner(cfg Config, conn clickhouse.Conn) *CHScanner` with `func (s *CHScanner) Scan(ctx, parts []arbiter.PartRef) ([]arbiter.PartScan, error)`

- [ ] **Step 1: Implement `verifier/backends.go`** (constructor code is direct assembly per the Produces block above — write it, including the scanner):

```go
package verifier

import (
	"context"
	"fmt"
	"sort"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/chexec"
	"housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter"
)

// NewReplayCore assembles the real pkg/replay pipeline for this verifier:
// chexec materialization against the verifier's ClickHouse, manifests from
// the arbiter SafeState RPC, payloads from the shared payload store, and
// the role's ed25519 receipt signer.
func NewReplayCore(cfg Config, conn clickhouse.Conn, manifests replay.SnapshotStore, payloads replay.PayloadStore) (*replay.Verifier, error) {
	signer, err := payloadexec.NewEd25519Signer(cfg.ReplicaID, cfg.Ed25519Seed)
	if err != nil {
		return nil, fmt.Errorf("verifier signer: %w", err)
	}
	return &replay.Verifier{
		Snapshots: manifests,
		Payloads:  payloads,
		Executor:  payloadexec.NewWithMaterializer(cfg.NetworkID, chexec.NewMaterializer(cfg.NetworkID, conn), cfg.Tables...),
		Signer:    signer,
	}, nil
}

// CHScanner is the byte-side scanner (spec §4): it recomputes per-part
// row-LtHash from THIS replica's hg_unsafe disk via chexec.ScanParts.
type CHScanner struct {
	cfg  Config
	conn clickhouse.Conn
}

func NewScanner(cfg Config, conn clickhouse.Conn) *CHScanner {
	return &CHScanner{cfg: cfg, conn: conn}
}

func (s *CHScanner) Scan(ctx context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error) {
	// Group requested parts by table; primary resolution is by part name
	// (RMT preserves names across replicas and STOP MERGES prevents local
	// renames). A missing part fails the whole scan: refusal to attest.
	byTable := map[string][]arbiter.PartRef{}
	tableOrder := []string{}
	for _, p := range parts {
		if p.PartName == "" {
			return nil, fmt.Errorf("scan request part without a name (table %s partition %s)", p.TableID, p.PartitionID)
		}
		if _, ok := byTable[p.TableID]; !ok {
			tableOrder = append(tableOrder, p.TableID)
		}
		byTable[p.TableID] = append(byTable[p.TableID], p)
	}
	sort.Strings(tableOrder)

	out := make([]arbiter.PartScan, 0, len(parts))
	for _, tableID := range tableOrder {
		sch, err := s.schemaFor(tableID)
		if err != nil {
			return nil, err
		}
		refs := byTable[tableID]
		names := make([]string, 0, len(refs))
		for _, p := range refs {
			names = append(names, p.PartName)
		}
		results, err := chexec.ScanParts(ctx, s.conn, s.cfg.UnsafeDatabase+"."+chTableName(tableID), sch, names)
		if err != nil {
			return nil, fmt.Errorf("scan table %s: %w", tableID, err)
		}
		byName := map[string]chexec.PartScanResult{}
		for _, r := range results {
			byName[r.PartName] = r
		}
		for _, p := range refs {
			r, ok := byName[p.PartName]
			if !ok {
				return nil, fmt.Errorf("part %s missing from scan results", p.PartName)
			}
			out = append(out, arbiter.PartScan{
				TableID:              p.TableID,
				PartitionID:          p.PartitionID,
				ClaimedPartRowLtHash: p.PartRowLtHash,
				ScannedPartRowLtHash: r.RowLtHash,
				LivePartName:         r.PartName,
			})
		}
	}
	return out, nil
}

func (s *CHScanner) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	for _, t := range s.cfg.Tables {
		if t.TableID == tableID {
			return t, nil
		}
	}
	return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
}

// chTableName maps a logical table id (e.g. "db.t") to the physical table
// name inside the role databases: the P1c convention stores every table as
// hg_unsafe.<flattened>, flattening "." to "__" so one CH database hosts
// all logical tables.
func chTableName(tableID string) string {
	out := make([]rune, 0, len(tableID))
	for _, r := range tableID {
		if r == '.' {
			out = append(out, '_', '_')
			continue
		}
		out = append(out, r)
	}
	return string(out)
}
```

**Convention freeze (used by Tasks 13/14/17 identically):** logical `TableID` "db.t" ↔ physical table `hg_unsafe.db__t` / `hg_safe.db__t` / `hg_promote.db__t` via `chTableName`. SNode duplicates this tiny mapper (or move it to `dataplane` — implementer's choice; if moved, name it `dataplane.CHTableName` and use it from both roles).

- [ ] **Step 2: Docker-gated scanner test** (`verifier/scanner_test.go`)

```go
//go:build !skip

package verifier

// gate helper — every CH-bound arbiter test uses this exact pattern
func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run a ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestCHScanner_MatchesSourceHashing(t *testing.T) {
	conn := requireCH(t)
	// create hg_unsafe.db__t with _hg_row_id + (p String, v UInt64), merges
	// pinned off; INSERT two rows with payloadexec.RowID ids; find the part
	// name via system.parts; Scan with a PartRef claiming lthash "0xLIE";
	// assert ScannedPartRowLtHash equals the payloadexec.RowElementHash-
	// derived sum (recompute in test) and ClaimedPartRowLtHash=="0xLIE"
	// (scanner reports honestly, never fails on mismatch).
	// Then: Scan with PartName "absent_0_0_0" -> error (refusal).
}
```

Write the body fully following Task 3's housegate scan test (same DDL/insert/expectation mechanics — self-contained here, do not import housegate test helpers).

- [ ] **Step 3: Run** — `go test ./verifier/ -count=1` (skips CH test), then with a local CH: `docker run -d --rm --name p1c-ch -p 9000:9000 clickhouse/clickhouse-server:25.8 && sleep 5 && ARBITER_CH_INTEGRATION=1 go test ./verifier/ -run TestCHScanner -v; docker stop p1c-ch` → PASS.

- [ ] **Step 4: Commit** — `git add verifier/ && git commit -m "feat(verifier): real replay core assembly + on-disk byte-side scanner

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"`

---

### Task 12: arbiter — `snode` config, durable local state, registration

**Files:**
- Create: `snode/config.go`, `snode/state.go`, `snode/snode.go` (Role struct + New + Register + Run skeleton)
- Test: `snode/state_test.go`, `snode/snode_test.go`

**Interfaces:**
- Consumes: `dataplane.Client`, `fspayload.Store`, `authority.Validator` (P1a — construction: see `fsm/authorityjws.go` / `authority` package for the validator over allowed addresses), `payloadexec.TableSchema`/`SchemaRoot`, clickhouse.Conn.
- Produces (Tasks 13–15 build on these exact names):
  - `type Config struct { NodeID, NetworkID, SchemaSnapshotID, SchemaRoot string; Tables []payloadexec.TableSchema; StateDir string; UnsafeDatabase, SafeDatabase, PromoteDatabase string /* defaults hg_unsafe/hg_safe/hg_promote */; AuthorityAddresses []string }` + `validate()` (schema-root assertion identical to verifier's)
  - `type Role struct{ ... }`; `func New(cfg Config, d Deps) (*Role, error)`; `type Deps struct { Client *dataplane.Client; Conn clickhouse.Conn; Payloads *fspayload.Store; Logger *slog.Logger }`
  - `func (r *Role) Register(ctx) error` — RegisterNode{roles:[SNODE]} (no pubkey) + MarkActive
  - `func (r *Role) Run(ctx) error` — `RunPromotionSubscription(ctx, cfg.NodeID, r.handleCommand)`; `handleCommand` dispatches promote (Task 14) / cleanup (Task 15); placeholders returning nil until those tasks
  - state.go: `type partitionKey struct{ Table, Partition string }`; `type localState struct { Watermarks map[string]uint64; LastAcks map[string]arbiter.PromotionAck; BaseRoots map[string]string; BaseSnapshotIDs map[string]string; UnpromotedSums map[string]string }` (map key = `table + "\x00" + partition`); `type stateStore struct{ path string; mu sync.Mutex; s localState }`; `func openStateStore(dir string) (*stateStore, error)` (loads `state.json` if present); getters/setters used later: `Watermark(k) uint64`, `LastAck(k) (arbiter.PromotionAck, bool)`, `BaseRoot(k) (string, string)` (root, snapshotID), `UnpromotedSum(k) string`, and mutators `RecordAck(k, seq, ack, newBaseRoot, newBaseSnapshotID string)` / `AddUnpromoted(k, partLtHashHex string) error` / `DrainUnpromoted(k, partLtHashHexes []string) error` — every mutator persists (temp+fsync+rename) before returning; LtHash math via `pkg/lthash` `FromBytes`/`Add`/hex helpers (state stores the ACCUMULATOR hex so ⊕/⊖ stay exact — use lthash add for AddUnpromoted and subtract-equivalent on drain; check `pkg/lthash` for the remove/subtract API name).

- [ ] **Step 1: Failing tests** — `snode/state_test.go`: open→mutate→reopen round trip (watermark, ack, base root, unpromoted sum survive); `AddUnpromoted` twice then `DrainUnpromoted` of one leaves the other's contribution (assert via lthash recompute); corrupt JSON file → open error. `snode/snode_test.go`: `TestNew_AssertsSchemaRoot` (mismatch → error); `TestRegister_SendsSNodeRegistration` (fake Membership records roles=[SNODE], empty pubkey OK).

- [ ] **Step 2: Run to verify failure.**

- [ ] **Step 3: Implement** — `config.go` mirrors Task 10's shape (defaults for the three databases; `AuthorityAddresses` required non-empty). `state.go` per the Produces contract:

```go
package snode

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"housegate/housegate/pkg/lthash"

	"github.com/sentioxyz/arbiter"
)

func key(table, partition string) string { return table + "\x00" + partition }

type localState struct {
	Watermarks      map[string]uint64                `json:"watermarks"`
	LastAcks        map[string]arbiter.PromotionAck  `json:"last_acks"`
	BaseRoots       map[string]string                `json:"base_roots"`
	BaseSnapshotIDs map[string]string                `json:"base_snapshot_ids"`
	UnpromotedSums  map[string]string                `json:"unpromoted_sums"`
}

// stateStore is the SNode's durable local state (spec §5d): promotion
// watermarks + last acks (exactly-once re-ack), per-partition safe base
// roots (CAS anchor), and per-partition unpromoted new-part LtHash sums
// (the absolute-view input for SourceClaimRoot). One JSON document,
// temp+fsync+rename on every mutation. If it is ever lost, the recovery
// procedure is a re-scan of hg_unsafe system.parts (documented, not
// automated in v1).
type stateStore struct {
	path string
	mu   sync.Mutex
	s    localState
}

func openStateStore(dir string) (*stateStore, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("snode state dir: %w", err)
	}
	st := &stateStore{path: filepath.Join(dir, "state.json"), s: localState{
		Watermarks: map[string]uint64{}, LastAcks: map[string]arbiter.PromotionAck{},
		BaseRoots: map[string]string{}, BaseSnapshotIDs: map[string]string{},
		UnpromotedSums: map[string]string{},
	}}
	b, err := os.ReadFile(st.path)
	if os.IsNotExist(err) {
		return st, nil
	}
	if err != nil {
		return nil, fmt.Errorf("snode state: %w", err)
	}
	if err := json.Unmarshal(b, &st.s); err != nil {
		return nil, fmt.Errorf("snode state corrupt: %w", err)
	}
	return st, nil
}

func (st *stateStore) persistLocked() error {
	b, err := json.Marshal(st.s)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(st.path), ".state-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), st.path)
}
```

plus the getters/mutators (all `st.mu.Lock()`-guarded; mutators call `persistLocked` and return its error). `AddUnpromoted`/`DrainUnpromoted` parse the stored accumulator hex (`lthash.New()` when absent), add/remove the part hash (check `pkg/lthash` for the exact add/remove method names — `AddHash` exists; the remove is whatever lthash exposes for lane-wise minus, e.g. `RemoveHash`/`SubHash` — read `pkg/lthash` and use its real API), store back as `0x`-hex.

`snode/snode.go`: `New` (validate config; `openStateStore(cfg.StateDir)`; build `authority.Validator` from `cfg.AuthorityAddresses` — P1a exposes a validator constructor used by fsm's `verifyAuthorityJWS`; find it via `grep -rn "NewEthValidator\|NewValidator" authority/` and use the real name); `Register` mirrors Task 10 with `NODE_ROLE_SNODE` and no pubkey; `Run` subscribes and dispatches:

```go
func (r *Role) Run(ctx context.Context) error {
	return r.d.Client.RunPromotionSubscription(ctx, r.cfg.NodeID, func(cmd *pb.PromotionCommand) error {
		switch m := cmd.GetCmd().(type) {
		case *pb.PromotionCommand_Promote:
			return r.handlePromote(ctx, m.Promote, cmd.GetAuthorityJws())
		case *pb.PromotionCommand_Cleanup:
			return r.handleCleanup(ctx, m.Cleanup, cmd.GetAuthorityJws())
		default:
			r.d.Logger.Warn("unknown promotion command", "type", fmt.Sprintf("%T", cmd.GetCmd()))
			return nil
		}
	})
}
```

with `handlePromote`/`handleCleanup` as logged no-op placeholders (`return nil`) that Tasks 14/15 replace.

- [ ] **Step 4: Run + commit** — `go test ./snode/ -race -count=1` → PASS; commit `feat(snode): role skeleton — config, durable state store, registration`.

---

### Task 13: arbiter — SNode statement intake (source write + RC assembly)

**Files:**
- Create: `snode/intake.go`, `snode/view.go` (absolute-view root assembly), `snode/parts.go` (system.parts helpers shared with Task 14)
- Test: `snode/intake_test.go` (docker-gated; reuse the `requireCH` pattern frozen in Task 11 — copy it into `snode/ch_test.go` as a package-local helper)

**Interfaces:**
- Consumes: `payloadexec.DecodeCSV`/`RowID`/`TableSchemaHash`, `chexec.ScanParts`, `replay.AssembleStateRoot` (Task 1), `wire.RCToPB` (P1b export) + `pb.SourceClaimsClient.RegisterResultClaim`, stateStore (Task 12).
- Produces: `func (r *Role) SubmitLocalStatement(ctx context.Context, env arbiter.StatementEnvelope, payload []byte) error` — THE route-A seam (P1e HouseGate calls it; tests call it directly).
- `snode/parts.go` produces (also used by Task 14): `func activeParts(ctx, conn, db, table string) ([]partInfo, error)` where `type partInfo struct { Name, PhysHash, Path string; Rows, Bytes uint64 }` — `SELECT name, hash_of_all_files, path, rows, bytes_on_disk FROM system.parts WHERE database=? AND table=? AND active ORDER BY name`.

- [ ] **Step 1: Docker-gated failing test** (`snode/intake_test.go`) — full body required; key assertions:

```go
func TestSubmitLocalStatement_WritesSourceAndRegistersRC(t *testing.T) {
	conn := requireCH(t)
	// harness: create hg_unsafe DB + hg_unsafe.db__t (same DDL as Task 11's
	// test, merges pinned); fake gRPC arbiter exposing SourceClaims (records
	// RegisterResultClaim) + SafeState serving a sealed GENESIS manifest for
	// the configured table set (build with a local payloadexec.Executor's
	// GenesisSnapshot — this is also what seeds cfg.SchemaRoot); dataplane
	// client against the fake; Role with StateDir = t.TempDir().
	//
	// env := envelope for statement "0xacct/1/n" targeting "db.t", payload
	// CSVWithNames "p,v\np0,1\np0,2\n" with PayloadHash/Length derived the
	// same way pkg/replay/verifier.go prepareStatements validates them
	// (READ verifier.go:199-240 and mirror the exact digest call).
	//
	// r.SubmitLocalStatement(ctx, env, payload)
	//
	// Assert: (1) hg_unsafe.db__t has 2 rows whose _hg_row_id equal
	// payloadexec.RowID(net, "db.t", env.StatementID.Flat(), 0/1);
	// (2) payload store contains the payload under env.PayloadRef;
	// (3) the recorded RCRecord has: SourceNode == cfg.NodeID; exactly one
	// CandidatePart with PartName == the real system.parts name, RowCount 2,
	// PartPhysHash == system.parts hash_of_all_files, PartRowLtHash == the
	// chexec.ScanParts value; PartitionNewPartSums == that part's hash for
	// partition p0; (4) SourceClaimRoot equals a reference computation:
	// drive a local payloadexec Executor.ApplyContext over the same genesis
	// manifest + a ReplayJob holding this statement, and assert
	// rc.SourceClaimRoot == result.ComputedStateRoot  — the check-1
	// equivalence, asserted end-to-end at the unit level;
	// (5) payload-hash mismatch → error, nothing written (row count still 2).
}
```

Assertion (4) is the heart of the task: it proves the same-by-construction claim without a full cluster.

- [ ] **Step 2: Implement**

`snode/view.go`:

```go
package snode

import (
	"fmt"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/replay/payloadexec"
)

// sourceClaimRoot assembles the source's absolute-view state root (spec
// §5a6): for every configured table, per-partition roots = persisted safe
// base ⊕ unpromoted new-part sum, fed through the SAME
// replay.AssembleStateRoot the replay executor uses. The absolute form is
// manifest-cut-invariant by LtHash associativity, which is what makes
// check 1 well-defined under route A's late binding.
func (r *Role) sourceClaimRoot() (string, error) {
	tables := make([]replay.TableManifest, 0, len(r.cfg.Tables))
	for _, sch := range r.cfg.Tables {
		tm := replay.TableManifest{
			TableID:    sch.TableID,
			SchemaHash: payloadexec.TableSchemaHash(r.cfg.NetworkID, sch),
		}
		for _, pk := range r.state.partitionsOf(sch.TableID) {
			base := r.state.baseRootOr(pk, "")
			unpromoted := r.state.unpromotedSumOr(pk, "")
			root, err := lthashCombineHex(base, unpromoted)
			if err != nil {
				return "", fmt.Errorf("partition %s/%s: %w", sch.TableID, pk.Partition, err)
			}
			tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{
				TableID: sch.TableID, PartitionID: pk.Partition, Root: root,
			})
		}
		tables = append(tables, tm)
	}
	_, stateRoot, err := replay.AssembleStateRoot(r.cfg.SchemaSnapshotID, r.cfg.SchemaRoot, r.executorProfileID(), tables)
	return stateRoot, err
}

// lthashCombineHex adds two accumulator-hex values, treating "" as zero.
func lthashCombineHex(a, b string) (string, error) {
	acc := lthash.New()
	for _, s := range []string{a, b} {
		if s == "" {
			continue
		}
		h, err := lthashFromHexLocal(s)
		if err != nil {
			return "", err
		}
		acc.AddHash(h)
	}
	return lthashHexLocal(acc), nil
}
```

(`lthashFromHexLocal`/`lthashHexLocal`: 6-line local helpers mirroring the fsm/payloadexec hex convention — the FORMAT is shared (`0x`+hex of the 2048-byte accumulator), the arithmetic is `pkg/lthash`; this does not violate the same-by-construction tripwire, which covers row/schema/state derivations, but keep them dumb. `partitionsOf(tableID)` iterates the union of BaseRoots+UnpromotedSums keys for the table, sorted. `executorProfileID()` — carry it in Config? It is a job/genesis field; add `ExecutorProfileID string` to `snode.Config` + validate non-empty, and mirror in Task 10's verifier Config for its own use if not already present — keep both configs symmetric.)

`snode/intake.go`:

```go
package snode

import (
	"context"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"housegate/housegate/pkg/lthash"
	"housegate/housegate/pkg/replay/payloadexec"
	"housegate/housegate/pkg/replay/chexec"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/wire"
)

// SubmitLocalStatement is the route-A intake seam (spec §5a): validate the
// envelope's payload binding, spool the payload, execute the source write
// into hg_unsafe with _hg_row_id injection, derive the statement's
// candidate parts from the system.parts diff, and register the RC. v1
// processes statements sequentially (r.intakeMu).
func (r *Role) SubmitLocalStatement(ctx context.Context, env arbiter.StatementEnvelope, payload []byte) error {
	r.intakeMu.Lock()
	defer r.intakeMu.Unlock()

	if err := validatePayloadBinding(env, payload); err != nil {
		return err
	}
	sch, err := r.schemaFor(env.TargetTableID)
	if err != nil {
		return err
	}
	if err := r.d.Payloads.Put(env.PayloadRef, payload); err != nil {
		return fmt.Errorf("spool payload: %w", err)
	}

	rows, err := payloadexec.DecodeCSV(payload, sch)
	if err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	flatID := env.StatementID.Flat()
	for i := range rows {
		rows[i].RowID = payloadexec.RowID(r.cfg.NetworkID, sch.TableID, flatID, uint64(i))
	}

	table := chTableName(sch.TableID)
	before, err := activeParts(ctx, r.d.Conn, r.cfg.UnsafeDatabase, table)
	if err != nil {
		return err
	}
	if err := r.insertRows(ctx, r.cfg.UnsafeDatabase, table, sch, rows); err != nil {
		return fmt.Errorf("source write: %w", err)
	}
	after, err := activeParts(ctx, r.d.Conn, r.cfg.UnsafeDatabase, table)
	if err != nil {
		return err
	}
	newParts := diffParts(before, after)
	if len(rows) > 0 && len(newParts) == 0 {
		return fmt.Errorf("source write produced no new parts (merges not stopped?)")
	}

	names := make([]string, 0, len(newParts))
	for _, p := range newParts {
		names = append(names, p.Name)
	}
	var scans []chexec.PartScanResult
	if len(names) > 0 {
		scans, err = chexec.ScanParts(ctx, r.d.Conn, r.cfg.UnsafeDatabase+"."+table, sch, names)
		if err != nil {
			return fmt.Errorf("hash new parts: %w", err)
		}
	}

	rc, err := r.assembleRC(ctx, env, sch, newParts, scans)
	if err != nil {
		return err
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewSourceClaimsClient(conn).RegisterResultClaim(ctx, wire.RCToPB(rc))
		return err
	}); err != nil {
		return fmt.Errorf("register rc: %w", err)
	}
	return nil
}
```

`assembleRC` (same file): map each scan result to its partInfo (by name); build `arbiter.CandidatePart{TableID: sch.TableID, PartitionID: partitionOf(scan/part rows), PartName, PartRowLtHash: scan.RowLtHash, PartPhysHash: info.PhysHash, RowCount: scan.RowCount, Bytes: info.Bytes}` — **PartitionID**: derive from the part rows' partition; for the MVP `PARTITION BY p` on a String column the part's partition id is `system.parts.partition_id`... add `PartitionID string` to `partInfo` (`SELECT partition_id` too) and use it directly; then per-partition `AddUnpromoted(key(tableID, partitionID), scan.RowLtHash)` and `PartitionNewPartSums` = per-partition lthash sum of this statement's new part hashes (`arbiter.PartitionLtHashSum` — check its exact fields in `types.go` and fill all); `SourceClaimRoot` = `r.sourceClaimRoot()` computed AFTER the AddUnpromoted calls (the view must include this statement); return the assembled `arbiter.RCRecord{StatementID: env.StatementID, SourceNode: r.cfg.NodeID, ...}`.

`insertRows`: batch insert `(_hg_row_id, cols...)` via `r.d.Conn.PrepareBatch` — mirror `chexec.insertRows` shape (it is unexported; re-write the 15 lines here, same order: row id first, then values). `validatePayloadBinding`: mirror `pkg/replay/verifier.go`'s payload hash+length check EXACTLY (read verifier.go's prepareStatements; use the same digest function and the same error semantics).

- [ ] **Step 3: Run** — skip-gated pass without CH; with CH: `ARBITER_CH_INTEGRATION=1 go test ./snode/ -run TestSubmitLocalStatement -v` → PASS.
- [ ] **Step 4: Commit** — `feat(snode): route-A statement intake — source write, part hashing, RC assembly`.

---

### Task 14: arbiter — SNode promotion execution (hg_promote build + REPLACE PARTITION + exact-Parts ack)

**Files:**
- Create: `snode/promote.go`, `snode/hardlink.go`
- Test: `snode/promote_test.go` (docker-gated)

**Interfaces:**
- Consumes: Task 12 stateStore, Task 13 `parts.go` helpers, `authority.Validator` + `authority.PromoteCommandHash`, `wire.PromoteFromPB`, `wire.PromotionAckToPB`, `pb.PromotionGatewayClient.AckPromotion`, `chexec.ScanParts`.
- Produces: `func (r *Role) handlePromote(ctx context.Context, m *pb.PromoteSafePartition, jws string) error` (replaces Task 12 placeholder).

- [ ] **Step 1: Docker-gated failing test** (`snode/promote_test.go`) — required scenarios, full bodies:

1. `TestHandlePromote_HappyPath`: seed hg_unsafe with one statement's part (reuse Task 13 intake against the fake arbiter), then hand-build a signed `PromoteSafePartition{PromotionSeq: 1, BasePartitionRoot: "" /* genesis-empty */, CandidateParts: [the real part]}` (sign with an `authority.Signer` whose address is in cfg.AuthorityAddresses); call `handlePromote`. Assert: hg_safe.db__t partition p0 now serves exactly the 2 rows; the fake gateway received `PromotionAck{Applied: true, PromotionSeq: 1, PostPartitionCommitment == base⊕candidate (recompute), Parts == exactly one mapping {PartRowLtHash: candidate hash, SafePartName: an ACTUAL hg_safe system.parts name, PartPhysHash: that part's hash_of_all_files}}`; stateStore watermark==1, BaseRoot advanced to post, UnpromotedSums drained for p0; hg_promote's partition is dropped (shadow hygiene).
2. `TestHandlePromote_BadJWSRejected`: JWS signed by an unlisted key → error, no ack sent, no hg_safe change.
3. `TestHandlePromote_StaleSeqResendsPersistedAck`: re-deliver the SAME command after happy path → the fake gateway receives the IDENTICAL persisted ack again; no CH mutation (part set unchanged).
4. `TestHandlePromote_BaseCASMismatchAcksNotApplied`: command with `BasePartitionRoot: "0xWRONG"` → ack `{Applied: false, Detail: contains "base"}`; hg_safe untouched; watermark advanced (the promotion is consumed — emergent rebase happens arbiter-side).

- [ ] **Step 2: Implement `snode/promote.go`**

```go
package snode

import (
	"context"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/authority"
	"github.com/sentioxyz/arbiter/wire"
)

// handlePromote executes one Arbiter-signed promotion (spec §5b): JWS
// check, watermark exactly-once, publish lock + base-CAS, hg_promote
// shadow build (base attach + candidate hardlinks), REPLACE PARTITION,
// content-matched exact-Parts ack.
func (r *Role) handlePromote(ctx context.Context, m *pb.PromoteSafePartition, jws string) error {
	cmd := wire.PromoteFromPB(m)
	hash, err := authority.PromoteCommandHash(cmd)
	if err != nil {
		return fmt.Errorf("promote hash: %w", err)
	}
	if err := r.authority.Validate(jws, hash); err != nil {
		return fmt.Errorf("promote authority: %w", err)
	}

	k := key(cmd.TableID, cmd.PartitionID)
	r.publishMu(k).Lock()
	defer r.publishMu(k).Unlock()

	// Exactly-once across restarts and idle partitions (§8.3): a stale or
	// duplicate promotion re-sends the persisted ack verbatim.
	if cmd.PromotionSeq <= r.state.Watermark(k) {
		if ack, ok := r.state.LastAck(k); ok && ack.PromotionSeq == cmd.PromotionSeq {
			return r.sendAck(ctx, ack)
		}
		r.d.Logger.Warn("stale promotion below watermark with no stored ack; ignoring", "seq", cmd.PromotionSeq)
		return nil
	}

	baseRoot, _ := r.state.BaseRoot(k)
	if cmd.BasePartitionRoot != baseRoot {
		// Base moved (or never matched): consume with Applied=false. Rebase
		// is emergent — the arbiter re-advertises under a fresh seq.
		ack := arbiter.PromotionAck{
			NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
			TableID: cmd.TableID, PartitionID: cmd.PartitionID,
			Applied: false, Detail: fmt.Sprintf("base CAS mismatch: local %s, command %s", baseRoot, cmd.BasePartitionRoot),
		}
		if err := r.state.RecordAck(k, cmd.PromotionSeq, ack, baseRoot, r.state.baseSnapshotIDOr(k, "")); err != nil {
			return err
		}
		return r.sendAck(ctx, ack)
	}

	post, parts, err := r.buildAndReplace(ctx, cmd)
	if err != nil {
		return fmt.Errorf("promotion %d: %w", cmd.PromotionSeq, err)
	}
	ack := arbiter.PromotionAck{
		NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
		TableID: cmd.TableID, PartitionID: cmd.PartitionID,
		PostPartitionCommitment: post, Applied: true, Parts: parts,
	}
	if err := r.state.RecordAck(k, cmd.PromotionSeq, ack, post, cmd.BaseSafeSnapshotID); err != nil {
		return err
	}
	hashes := make([]string, 0, len(cmd.CandidateParts))
	for _, p := range cmd.CandidateParts {
		hashes = append(hashes, p.PartRowLtHash)
	}
	if err := r.state.DrainUnpromoted(k, hashes); err != nil {
		return err
	}
	return r.sendAck(ctx, ack)
}

func (r *Role) sendAck(ctx context.Context, ack arbiter.PromotionAck) error {
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).AckPromotion(ctx, wire.PromotionAckToPB(ack))
		return err
	})
}
```

`buildAndReplace` (same file) — the CH sequence, each step `r.d.Conn.Exec`:

```go
func (r *Role) buildAndReplace(ctx context.Context, cmd arbiter.PromoteSafePartition) (post string, mappings []arbiter.SafePartMapping, err error) {
	sch, err := r.schemaFor(cmd.TableID)
	if err != nil {
		return "", nil, err
	}
	table := chTableName(cmd.TableID)
	safe := r.cfg.SafeDatabase + "." + table
	promote := r.cfg.PromoteDatabase + "." + table

	// 1) shadow table exists, same structure/policy as hg_safe
	if err := r.exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS %s", promote, safe)); err != nil {
		return "", nil, err
	}
	// 2) crash safety: drop any leftover shadow partition first
	if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s", promote, quotePartition(cmd.PartitionID))); err != nil {
		return "", nil, err
	}
	// 3) bring the CAS base in whole (metadata-only hardlink attach)
	if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", promote, quotePartition(cmd.PartitionID), safe)); err != nil {
		return "", nil, err
	}
	// 4) per candidate: hardlink the live hg_unsafe part dir into the
	// shadow's detached/, then attach it by name. Parts are immutable on
	// disk and merges are stopped, so linking live parts is safe.
	safeBefore, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return "", nil, err
	}
	for _, cp := range cmd.CandidateParts {
		src, err := r.unsafePartPath(ctx, table, cp.PartName)
		if err != nil {
			return "", nil, err
		}
		dst, err := r.promoteDetachedPath(ctx, table, cp.PartName)
		if err != nil {
			return "", nil, err
		}
		if err := hardlinkDir(src, dst); err != nil {
			return "", nil, fmt.Errorf("hardlink part %s: %w", cp.PartName, err)
		}
		if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PART '%s'", promote, escapeSQLString(cp.PartName))); err != nil {
			return "", nil, err
		}
	}
	// 5) atomic publish
	if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION %s FROM %s", safe, quotePartition(cmd.PartitionID), promote)); err != nil {
		return "", nil, err
	}
	// 6) shadow hygiene
	if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s", promote, quotePartition(cmd.PartitionID))); err != nil {
		return "", nil, err
	}

	// 7) content-matched exact-Parts mapping over the renamed parts
	safeAfter, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return "", nil, err
	}
	newSafe := diffParts(safeBefore, safeAfter)
	names := make([]string, 0, len(newSafe))
	infoByName := map[string]partInfo{}
	for _, p := range newSafe {
		names = append(names, p.Name)
		infoByName[p.Name] = p
	}
	scans, err := chexec.ScanParts(ctx, r.d.Conn, safe, sch, names)
	if err != nil {
		return "", nil, err
	}
	type ckey struct {
		hash  string
		count uint64
	}
	wantByKey := map[ckey]arbiter.PartRef{}
	for _, cp := range cmd.CandidateParts {
		wantByKey[ckey{cp.PartRowLtHash, candidateRowCount(cp, r)}] = cp
	}
	// Match each new safe part to a candidate by content key (RowLtHash,
	// RowCount) — the exact-Parts contract's physical realization. Base
	// parts re-attached under new names match nothing (their content keys
	// are not in wantByKey) and are skipped; an unmatched candidate at the
	// end is a local build failure (ack Applied=false is produced only by
	// the caller-level CAS path).
	for _, scan := range scans {
		k := ckey{scan.RowLtHash, scan.RowCount}
		cp, ok := wantByKey[k]
		if !ok {
			continue // a base part under its new name
		}
		mappings = append(mappings, arbiter.SafePartMapping{
			PartRowLtHash: cp.PartRowLtHash,
			SafePartName:  scan.PartName,
			PartPhysHash:  infoByName[scan.PartName].PhysHash,
		})
		delete(wantByKey, k)
	}
	if len(wantByKey) != 0 {
		return "", nil, fmt.Errorf("promotion %d: %d candidate part(s) not found in hg_safe after REPLACE (content-match failed)", cmd.PromotionSeq, len(wantByKey))
	}
	post, err = lthashCombineHexAll(cmd.BasePartitionRoot, candidateHashes(cmd))
	return post, mappings, err
}
```

Support helpers to write alongside: `lthashCombineHexAll(baseHex string, partHexes []string) (string, error)` (fold `lthashCombineHex` over the list) and `candidateHashes(cmd) []string`. Note base-part exclusion: REPLACE PARTITION renames the base parts too — they are distinguished from candidates by content key mismatch. `candidateRowCount`: candidates carry RowCount from the RC (Task 13 set it); if zero (older RC), fall back to matching by hash alone with ambiguity → error. `quotePartition`: the MVP partitions by a String column — `PARTITION BY p` → partition id needs quoting as `'p0'` (use `escapeSQLString`); `unsafePartPath`: `SELECT path FROM system.parts WHERE database=? AND table=? AND name=? AND active`; `promoteDetachedPath`: `SELECT data_paths[1] FROM system.tables WHERE database=? AND name=?` + `"detached/" + partName`. `hardlinkDir` in `snode/hardlink.go`: `filepath.WalkDir(src)`, `os.MkdirAll` dirs, `os.Link` files (no symlink following; error if dst exists — leftover detached dirs from a crashed attempt must be removed first: `os.RemoveAll(dst)` before walking).

- [ ] **Step 3: Run with CH** — all four scenarios PASS.
- [ ] **Step 4: Commit** — `feat(snode): promotion executor — shadow build, REPLACE PARTITION, exact-Parts ack`.

---

### Task 15: arbiter — SNode cleanup + intake/promotion interplay hardening

**Files:**
- Create: `snode/cleanup.go`
- Test: `snode/cleanup_test.go` (docker-gated) + extend `snode/promote_test.go`

**Interfaces:**
- Consumes: `authority.CleanupCommandHash`, `wire.CleanupFromPB`, `pb.PromotionGatewayClient.AckCleanup`, `wire.CleanupAckToPB` (check the exact P1b export name via `grep -n "CleanupAck" wire/*.go`).
- Produces: `func (r *Role) handleCleanup(ctx context.Context, m *pb.UnsafeCleanup, jws string) error` (replaces placeholder).

- [ ] **Step 1: Failing tests** — (1) happy: after Task 14's happy path, deliver a signed `UnsafeCleanup{PromotionSeq: 1, Parts: [original unsafe part]}`; assert the part is GONE from hg_unsafe system.parts, hg_safe rows still served, and the fake gateway got `CleanupAck{PromotionSeq: 1, NodeID: cfg.NodeID}` (check the `arbiter.CleanupAck` field set in types.go and fill all fields the FSM's applyRecordCleanupAck expects). (2) idempotent: re-deliver → part already gone → still acks (missing part treated as already dropped). (3) bad JWS → no drop, no ack.

- [ ] **Step 2: Implement `snode/cleanup.go`**

```go
package snode

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/sentioxyz/arbiter"
	"github.com/sentioxyz/arbiter/authority"
	"github.com/sentioxyz/arbiter/wire"
)

// handleCleanup drops promoted hg_unsafe parts (spec §5c). Idempotent: a
// part that no longer exists counts as dropped.
func (r *Role) handleCleanup(ctx context.Context, m *pb.UnsafeCleanup, jws string) error {
	cmd := wire.CleanupFromPB(m)
	hash, err := authority.CleanupCommandHash(cmd)
	if err != nil {
		return fmt.Errorf("cleanup hash: %w", err)
	}
	if err := r.authority.Validate(jws, hash); err != nil {
		return fmt.Errorf("cleanup authority: %w", err)
	}
	table := chTableName(cmd.TableID)
	for _, p := range cmd.Parts {
		err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s DROP PART '%s'",
			r.cfg.UnsafeDatabase, table, escapeSQLString(p.PartName)))
		if err != nil && !isMissingPartErr(err) {
			return fmt.Errorf("drop part %s: %w", p.PartName, err)
		}
	}
	ack := arbiter.CleanupAck{NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
		TableID: cmd.TableID, PartitionID: cmd.PartitionID}
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).AckCleanup(ctx, wire.CleanupAckToPB(ack))
		return err
	})
}

func isMissingPartErr(err error) bool {
	// ClickHouse: "No part <name> in table"/"Part <name> not found" class of
	// errors. Match loosely on the message; verify the actual 25.8 message
	// text in the test and adjust.
	s := err.Error()
	return strings.Contains(s, "No part") || strings.Contains(s, "not found")
}
```

Caveats: `arbiter.CleanupAck` exact fields from `types.go` (fill everything `fsm.applyRecordCleanupAck` reads — check its validation in `fsm/apply.go` WITHOUT modifying fsm); `DROP PART` name quoting; the missing-part error text is empirically pinned by the test.

- [ ] **Step 3: Run with CH** → PASS. **Step 4: Commit** — `feat(snode): authority-gated unsafe cleanup with idempotent drops`.

---

### Task 16: arbiter — reference binaries `cmd/arbiter-verifier` + `cmd/arbiter-snode`

**Files:**
- Create: `cmd/arbiter-verifier/main.go`, `cmd/arbiter-verifier/config.go`, `cmd/arbiter-snode/main.go`, `cmd/arbiter-snode/config.go`, `configs/verifier.local.yaml`, `configs/snode.local.yaml`
- Test: `cmd/arbiter-verifier/main_test.go`, `cmd/arbiter-snode/main_test.go`

**Interfaces:**
- Consumes: `verifier.New/Register/Run` + `NewReplayCore/NewScanner`, `snode.New/Register/Run`, `dataplane.New/NewManifestStore`, `fspayload.New`, `config.Duration` (reuse the arbiter `config` package's Duration type via import — do NOT copy it), clickhouse-go `Open`.
- Produces: two runnable binaries with yaml configs; each `run(ctx, cfg, logger) error` is testable (mirrors `cmd/arbiter`'s pattern).

Each binary's yaml (full schema in the config.go of each cmd; write Load/Validate following `cmd/arbiter`'s style — plain structs, `yaml.v3`, `errors.Join`):

```yaml
# configs/verifier.local.yaml
replica_id: "verifier-1"
network_id: "net-local"
schema_snapshot_id: "schema-genesis"
schema_root: ""            # computed by `arbiter-verifier -print-schema-root` (see below)
executor_profile_id: "housegate-replay-mvp-v0"
clickhouse_addr: "127.0.0.1:9000"
scratch_database: "default"
payload_dir: "/tmp/hg-payloads"
ed25519_seed_hex: ""       # env ARBITER_VERIFIER_ED25519_SEED_HEX overrides
peers:
  - { id: "arb-1", grpc_addr: "127.0.0.1:8081" }
tables:
  - table_id: "db.t"
    partition_by: "p"
    columns:
      - { name: "p", type: "String" }
      - { name: "v", type: "UInt64" }
```

`snode.local.yaml`: same shape minus ed25519, plus `node_id`, `state_dir`, `authority_allowed_addresses: []`, `unsafe_database/safe_database/promote_database` defaults.

Both mains: flag `-config`; a `-print-schema-root` mode that loads tables and prints `payloadexec.SchemaRoot(networkID, tables)` then exits 0 (this is how operators seed `genesis.schema_root` and the roles' `schema_root` — document in the yaml comment); slog text handler; `signal.NotifyContext`; `run()` wires dataplane client → stores → role → `Register` then `Run`; graceful stop on ctx.

Tests: config load/validate table (bad yaml, missing fields, seed hex length), `-print-schema-root` prints a `0x…` digest and exits clean (capture via the run function, not a subprocess), and a `run()` smoke against NO arbiter: `Register` fails → error returned (fail-fast, no hang) — bound with a 2s ctx.

- [ ] Steps: failing tests → implement → `go test ./cmd/... -count=1` PASS → commit `feat(cmd): arbiter-verifier + arbiter-snode reference binaries`.

---

### Task 17: arbiter — docker flagship: real end-to-end happy path

**Files:**
- Create: `integration/chpipeline/pipeline_test.go`, `integration/chpipeline/harness_test.go`
- (New package so the existing fake-based `integration` suite stays intact and CH-free.)

**Interfaces:**
- Consumes: EVERYTHING. The P1b in-process cluster harness (`integration/cluster_*.go` — it is package `integration`, test-only; REPLICATE the small pieces needed — startCluster/withLeaderRetry — into `chpipeline` as local copies; do not export test helpers across packages), `verifier.*`, `snode.*`, `dataplane.*`, `payloadexec.Executor.GenesisSnapshot`.
- Produces: `TestPipeline_RealDataPlaneEndToEnd` — the P1c flagship.

**Harness (`harness_test.go`):**
- `requireCH(t)` (Task 11 pattern; CH_ADDR env or localhost:9000).
- 3-node arbiter cluster: copy the minimal startNode/leader-retry plumbing from `integration/cluster_node_test.go` / `cluster_control_test.go` (trim to what this package needs). Cluster config additions vs P1b: `genesis.schema_root` = `payloadexec.SchemaRoot(net, tables)`, `genesis.manifest_path` = a temp file holding the sealed genesis manifest JSON produced by `payloadexec.New(net, tables...).GenesisSnapshot(0, schemaSnapshotID, profileID)`.
- CH setup: create `hg_unsafe`/`hg_safe`/`hg_promote` databases + `db__t` tables (unsafe with `SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0`; safe/promote same structure) + `SYSTEM STOP MERGES` on the unsafe table; `t.Cleanup` drops all three databases. Use a UNIQUE suffix per test run (e.g. from `t.Name()` hash) in the database names so parallel/failed runs never collide — thread the names through the role configs.
- Roles in-process: 3× `verifier.Role` (distinct ed25519 seeds, ReplicaIDs verifier-1..3; all sharing the single CH conn + one fspayload dir) with REAL `NewReplayCore`+`NewScanner`; 1× `snode.Role` (NodeID snode-1, state dir t.TempDir(), AuthorityAddresses = the cluster's authority address). Register all, then `go Run(ctx)` each.

**The flagship (`pipeline_test.go`):**

```go
func TestPipeline_RealDataPlaneEndToEnd(t *testing.T) {
	conn := requireCH(t)
	h := startHarness(t, conn) // cluster + CH schemas + roles, all registered+running

	// Two statements through BOTH ingress paths (route A): the arbiter
	// SubmitStatement (signed envelope, same JWS conventions as the P1b
	// integration test) AND the SNode local seam.
	env1, payload1 := h.statement(1, "p0", 1) // INSERT ... VALUES via CSVWithNames "p,v\np0,1\n"
	h.submitToArbiter(t, env1)                // ingress SubmitStatement (admission)
	mustNoErr(t, h.snode.SubmitLocalStatement(h.ctx, env1, payload1)) // source write + RC
	env2, payload2 := h.statement(2, "p0", 2)
	h.submitToArbiter(t, env2)
	mustNoErr(t, h.snode.SubmitLocalStatement(h.ctx, env2, payload2))

	// Seal by count (cluster seal.max_statements=2), then the machine runs:
	// RC bind -> MarkReplaying -> real replay x3 (chexec) -> real byte scans
	// -> quorum -> anchor(local) -> promote -> SNode REPLACE PARTITION ->
	// ack -> manifest -> cleanup. Wait for the safe watermark.
	h.waitWatermark(t, 1, 90*time.Second)

	// hg_safe serves the rows
	rows := h.queryAll(t, "SELECT v FROM "+h.safeTable()+" ORDER BY v")
	if len(rows) != 2 || rows[0] != uint64(1) || rows[1] != uint64(2) {
		t.Fatalf("hg_safe rows: %v", rows)
	}
	// manifest reality: ActiveParts name real hg_safe parts and the
	// partition root equals base⊕parts
	m := h.leaderManifest(t)
	h.assertManifestMatchesSystemParts(t, m)
	// all three attestations were MatchSourceRoot=true (fetch block
	// verification via a follower's... NOT exposed via RPC — assert
	// indirectly: watermark advanced means quorum passed with the REAL
	// roots; additionally assert the RC's SourceClaimRoot equals the
	// manifest's StateRoot at safe block 1 (both derive from the same
	// content — the strongest external equality available).
	// cleanup: original unsafe parts are gone
	h.waitUnsafePartsGone(t, 30*time.Second)
	// genesis bootstrap happened exactly once
	if got := h.watermarkManifestParent(t); got != h.genesisID {
		t.Fatalf("manifest parent must be the genesis manifest, got %s", got)
	}
}
```

Write every helper concretely in harness_test.go (they are thin: gRPC calls via the copied leader-retry, CH queries via conn). `statement(seq, partition, v)` builds envelope + payload with the SAME derivations as Task 13's test (payload hash per verifier.go, `RowID` ids, user JWS per the P1b integration signer — copy `signUserJWSAt` from `integration/pipeline_test.go`).

- [ ] Steps: write harness+test → run `ARBITER_CH_INTEGRATION=1 go test ./integration/chpipeline/ -run RealDataPlane -v -timeout 300s` (with the docker CH from Task 11's snippet) → PASS → commit `test(integration): real data-plane end-to-end flagship over docker ClickHouse`.

**Timing note:** first run pulls nothing (roles are in-process); the pipeline is asynchronous — every wait uses the `waitFor` polling pattern from P1b (10ms tick, generous timeout), never sleeps.

---

### Task 18: arbiter — fraud classes (a)(b)(c) + CI docker job

**Files:**
- Create: `integration/chpipeline/fraud_test.go`
- Modify: `.github/workflows/ci.yml` (arbiter repo), `README.md` (P1c section: roles, schema-root tool, docker test howto)

**Interfaces:**
- Consumes: Task 17 harness; `snode.Role` wrapped by a tampering RC path.
- Produces: `TestPipeline_FraudSourceClaimRoot`, `TestPipeline_FraudCandidatePartHash`, `TestPipeline_FraudPartitionNewPartSums`.

**Tampering seam:** the cleanest lie injection is at the RC boundary. Add to the HARNESS (not the product): a `tamperRC func(*pb.RCRecord)` hook on the fake... no — RCs flow SNode→arbiter directly. Two options; use (A): (A) run the fraud tests with a thin `lyingSNode` wrapper that embeds `*snode.Role` but re-implements `SubmitLocalStatement` by calling a copied assembleRC path with a mutation — too much duplication. (B) intercept at the network: the harness dials the SNode's dataplane client through a **tampering gRPC proxy** — also heavy. (C — CHOSEN) add a TEST-ONLY export in `snode`: `func (r *Role) SetRCTamper(f func(*arbiter.RCRecord))` guarded to test builds? Product code with a tamper hook is unacceptable. FINAL CHOICE: the fraud tests do NOT reuse `snode.Role` for the lying source. They drive the source side MANUALLY in the harness: perform the real CH insert + part hashing with the same exported helpers (chexec.ScanParts etc. — ~30 lines in the harness, mirroring intake), then register a MUTATED RC via a direct `RegisterResultClaim` call. This is faithful adversary modeling — a hostile source runs hostile code, not our library — and it keeps the product tamper-free. The honest verifiers + honest arbiter are the code under test.

1. **Fraud (a) — SourceClaimRoot lie (check 1):** manual source write of statement 1, RC with `SourceClaimRoot: replay.DigestString("evil")`, everything else honest. Assert: three attestations arrive with `MatchSourceRoot=false`... (external observability: the watermark must NOT advance; poll the leader's block state via `waitFor` on `GetSafeWatermark` staying 0 for a bounded window is weak — strengthen with the challenge outcome below); the orchestrator's QuorumFailed path fires `OpenChallenge`+`ResolveChallenge(REJECTED)`; terminal assertions: watermark stays 0 for ≥ 3× retry_interval AND `hg_safe` has zero rows AND a follow-up HONEST statement (statement 2 in a NEW block... note: block 1 is rejected; the pipeline for block 2 proceeds — submit statement 2 honestly via the real snode and assert the system still reaches watermark for the later block, proving the fraud poisoned only its own block). NOTE: whether a rejected block 1 blocks the safe prefix forever is FSM semantics — safePrefix requires contiguous safe blocks, and a Rejected statement never becomes Safe, so watermark can NEVER advance past a rejected block: assert instead that watermark stays 0 AND hg_safe stays empty AND the statement status is terminal — do NOT assert later blocks advance. (Read `fsm/reads_work.go` safePrefixLocked to confirm before writing the assertion, and say which behavior you observed in the report.)
2. **Fraud (b) — candidate part hash lie (check 3):** honest write + RC, except one CandidatePart.PartRowLtHash (and the matching PartitionNewPartSums entry, to keep check 2 self-consistent) replaced by `lthash-of("evil-part")`. The REAL scanners report the true disk value → check 3 mismatch. Assert as (a): no quorum, watermark pinned at 0, hg_safe empty.
3. **Fraud (c) — PartitionNewPartSums lie only (check 2):** honest per-part hashes and honest SourceClaimRoot, `PartitionNewPartSums[p0]` replaced by a wrong sum. Check 2's additive equality (`base ⊕ Σ claimed == verifier's PartitionCommitmentsAfter`) trips. Same terminal assertions.

For each fraud test, additionally assert WHICH check tripped if externally observable; the FSM does not expose per-check verdicts over RPC, so encode the discrimination structurally: (a) is the ONLY case where attestations carry MatchSourceRoot=false — but attestation content is also not queryable via RPC. Accept the terminal-state assertions as the acceptance bar (spec §9 requires "assert the specific check that tripped (via block verification state)" — block verification state is FSM-internal; the honest fulfillment at the integration level is: construct each fraud so that ONLY its intended check can fail (a: roots differ but parts+sums honest → checks 2,3 pass; b: root and sums consistent-with-lie... careful: (b) keeps SourceClaimRoot HONEST (computed from true parts) while the RC's claimed part hash lies — then check 2 compares base⊕(lying sums)⊕? — set sums to match the LIE so check 2 passes and only check 3 fails; (c) mirrors with only sums lying). Document the isolation argument in a comment atop each test.

**CI (`.github/workflows/ci.yml`)** — add a second job:

```yaml
  integration-clickhouse:
    runs-on: ubuntu-latest
    services:
      clickhouse:
        image: clickhouse/clickhouse-server:25.8
        ports: ["9000:9000"]
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version-file: go.mod }
      - name: configure private module access
        run: |
          git config --global url."https://x-access-token:${{ secrets.GH_MODULES_TOKEN }}@github.com/".insteadOf "https://github.com/"
          go env -w GOPRIVATE=github.com/sentioxyz,github.com/housegate
      - name: data-plane integration (docker ClickHouse)
        run: ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 go test ./verifier/ ./snode/ ./integration/chpipeline/ -count=1 -timeout 900s
        env: { GOFLAGS: -mod=mod }
```

(Adjust the private-module step to exactly match the existing job's; keep the existing test job untouched. GitHub `services` health: add a small retry loop before tests if the first connection flakes — `for i in $(seq 1 30); do nc -z 127.0.0.1 9000 && break; sleep 1; done`.)

- [ ] Steps: fraud tests red-in-the-right-way check (each fraud test against a DISHONEST assertion first is not required — instead verify each test FAILS if you disable its lie: flip the tamper off and assert the test's terminal condition would not hold, i.e. run the honest variant and confirm watermark ADVANCES — this proves the assertions bite) → implement → `ARBITER_CH_INTEGRATION=1 go test ./integration/chpipeline/ -v -timeout 600s` all PASS → README section → commit `test(integration): fraud rejection classes + docker CI job` → push and confirm the new CI job is green on GitHub.

---

## Execution notes for the controller

- **Model selection:** Tasks 2, 9, 12, 16 are transcription-heavy → haiku-tier; Tasks 1, 4, 5, 6, 7, 8, 10, 15 → sonnet-tier; Tasks 3, 11, 13, 14, 17, 18 (docker + multi-system nuance) → sonnet with opus REVIEWERS; final whole-branch review → fable with opus verification per P1a/P1b convention.
- **Docker availability:** Tasks 3, 11, 13, 14, 15, 17, 18 need a local ClickHouse (`docker run -d --rm --name p1c-ch -p 9000:9000 clickhouse/clickhouse-server:25.8`). The controller starts/stops it around those tasks; implementers only consume `ARBITER_CH_INTEGRATION=1` + `CH_ADDR`.
- **STOP gates:** end of Task 3 (user merges the housegate PR; controller records the merged sha for Task 4). Any fsm/ diff at any point. Any test that can only pass by weakening an assertion.
- **Ledger:** append one line per reviewed task to `/Users/uranuswch/Dev/housegate/housegate/.superpowers/sdd/progress.md` (absolute path), format `P1c Task N: complete (commits <base7>..<head7>, review clean)`.
- **Final review:** `review-package <base-sha> HEAD` per repo (arbiter base = the pre-Task-4 main sha; housegate base = merge-base of the feature branch), tripwire greps from Global Constraints + spec §10, Minor-triage from the ledger.

## Self-Review Notes (plan-level, written after drafting)

1. **Task 14's content-match loop and Task 16's config structs are the two spots closest to "sketch"** — both carry exact contracts (inputs, outputs, error cases, field sources) and their tests pin behavior; implementers transcribe the loop from the stated invariants. Everything else is full code.
2. **`LeaderConn` (Task 7) is intentionally NOT used by subscriptions** (Task 8 re-homes itself via candidateOrder). It exists for Task 16/17 harness probes. If review finds it dead by Task 16, delete it rather than keep speculative API (YAGNI) — noted so the Task 7 reviewer doesn't flag the later deletion as scope creep.
3. **Type-consistency sweep done:** `dataplane.Client` methods (`WithLeaderRetry`, `RunVerifierSubscription`, `RunPromotionSubscription`, `conn/candidateOrder/setLeader` internals) used by Tasks 8–17 match Task 7/8 definitions; `chexec.ScanParts(ctx, conn, "db.table", schema, names) ([]PartScanResult, error)` consistent across Tasks 3/11/13/14; `replay.AssembleStateRoot(schemaSnapshotID, schemaRoot, executorProfileID, tables)` consistent across Tasks 1/13; `chTableName` convention frozen in Task 11 and reused in 13/14/15/17.
4. **Spec coverage check:** decisions 1–8 ✓ (D8 root change = T1; helpers = T2/T3; seam/intake = T13; acceptance = T17/T18); §3 dataplane ✓ T7–9; §4 verifier ✓ T10–11 (local-replica scan + name→content fallback: the fallback branch is NOT in T11's scanner — single-CH v1 never exercises it; ADD the content-fallback to T11's `Scan` only if the reviewer insists, else it stays a recorded follow-up with the multi-replica topology — the spec words it as the production rule; note this consciously in the T11 report); §5 snode ✓ T12–15 (a1–a7, b1–b7, c, d); §6 anchor ✓ T5; §7 cross-repo ✓ T1–4 (+genesis bootstrap T6); §8 configs ✓ T16; §9 testing ✓ T17–18 + per-task units; §10 tripwires → Global Constraints + final review; §11 untouched.
5. **Known deliberate deviations from spec text:** (i) T11 omits the scan name-miss content-fallback (single-CH cannot exercise it; follow-up recorded) — flag to the user at plan review; (ii) fraud (a) terminal assertion acknowledges a rejected block pins the safe prefix forever (FSM semantics) — the spec's fraud table is silent on post-fraud liveness; the test asserts containment, not recovery.
6. **P1b orchestrator schema-root fallback (T6)** keeps old fixtures green while production configs set `genesis.schema_root`; the fallback branch is marked transitional and the flagship uses the real value — revisit removal in P1d.
