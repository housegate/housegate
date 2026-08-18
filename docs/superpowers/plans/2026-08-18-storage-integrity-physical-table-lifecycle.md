# Storage Integrity Physical Table Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every storage-integrity data-plane role derives, creates, and verifies the protocol-owned `hg_unsafe.*` / `hg_safe.*` tables (pinned engine, columns, keys, MergeTree settings) from the schema source, and the ingress HouseGate (plus the SNode prepare as a mirror) refuses INSERTs into a partition whose `hg_unsafe` part count crosses a soft limit with a retryable ClickHouse `TOO_MANY_PARTS` (252) exception.

**Architecture:** A new pure package `arbiter-core/dataplane/ddl` renders the D3-pinned DDL from `payloadexec.TableSchema` (`BuildDDL`, golden-tested), verifies live tables against the same intent (`VerifyProtocolTable`, `system.tables` / `system.columns` / `system.replicas` / `system.merge_tree_settings` + an `engine_full` SETTINGS parser), and exposes `EnsureProtocolTables` that `snode.Role.Register` / `verifier.Role.Register` run before registering with the Arbiter and re-run on a 60s reconcile ticker against the same schema-root-bound startup table slice. The ticker detects deletion/drift; it does not hot-adopt later declarations, which require a controlled restart until the schema-transition lane exists. housegate gains a `PartsPressureGuard` over the existing `MergeConn` port that snapshots `system.parts` grouped by `(database, table, partition)`, a `ClientError` seam so a plugin rejection can carry code 252, config `storage_integrity.runtime.backpressure`, a poller/supervisor with Prometheus gauges, and the ingress check between the merge-health latch and the payload put. sentio-node passes NodeID + schema-source-derived mode, wires the schema resolver into the housegate runtime, and maps the SNode mirror error to the same retryable rejection.

**Tech Stack:** Go 1.26, Bazel 9 + Bzlmod + gazelle in all repos, clickhouse-go v2 (sentioxyz fork), ClickHouse 25.8 docker image (arbiter-core CI service / housegate testcontainers), Prometheus client_golang, slog (arbiter-core) / `pkg/log` (housegate).

**Spec:** `/Users/uranuswch/Dev/housegate/housegate/docs/superpowers/specs/2026-08-18-storage-integrity-physical-table-lifecycle-design.md` (Spec C). Roadmap: `docs/superpowers/specs/2026-08-18-storage-integrity-v1-closure-roadmap.md` §4 decisions 6 (`hg_safe` merges stay stopped) and 5/1 (row profile / Native payload — the ingress guard must decode both `csv-with-names-v1` and `clickhouse-native-data-v1`). Context: `docs/superpowers/specs/2026-07-06-arbiter-p1c-dataplane-design.md`.

## Global Constraints

- Repos (all local, absolute paths): arbiter-core `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` (main `829c44f`), arbiter `/Users/uranuswch/Dev/sentio_xyz/arbiter` (main `edd23c3`), housegate `/Users/uranuswch/Dev/housegate/housegate` (main `c6f7a6d`), sentio-node `/Users/uranuswch/Dev/sentio_xyz/sentio-node` (main `9f12620`). Each task names its repo; branch off that repo's latest `origin/main`. Never push to `main`; open PRs with `gh`.
- Bazel is the ground truth everywhere: after adding Go files or imports run `bazel run //:gazelle` (sentio-node: `./scripts/update-bazel-deps.sh`); after `go.mod` changes run `bazel mod tidy` (housegate/arbiter-core/arbiter) — sentio-node uses `bazel run @rules_go//go -- mod tidy`. Go module paths: `github.com/housegate/housegate`, `github.com/sentioxyz/arbiter-core`, `github.com/sentioxyz/arbiter`, sentio-node `compute-network-node`.
- Docker-gated tests: arbiter-core gates on `ARBITER_CH_INTEGRATION=1` + `CH_ADDR` (default `127.0.0.1:9000`), run via `bazel test //snode:snode_test //verifier:verifier_test --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=900`; this plan adds `//dataplane/ddl:ddl_test` to that list and a keeper-enabled ClickHouse (ReplicatedMergeTree needs Keeper — the stock `clickhouse/clickhouse-server:25.8` service in `ci.yml` has none). housegate docker tests live in `pkg/integration` (`//pkg/integration:integration_test`, tagged `manual`, listed explicitly in `.github/workflows/ci.yml`, shared container `chEnv`, helpers `openDirectCH` / `mustExec` / `uniqueTable`). sentio-node smoke gates on `SENTIO_SI_E2E=1`.
- Naming freeze (spec D2): physical table = `strings.ReplaceAll(tableID, ".", "__")`; zk path = `/sentio/<keeper_shard_id>/unsafe/<physical>`; replica = the node id the role registers with; `keeper_shard_id = 0` in v1.
- Pinned settings (spec D3), verbatim: `hg_unsafe.*` = `ReplicatedMergeTree` + `max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0`; `hg_safe.*` = `MergeTree` + `max_bytes_to_merge_at_max_space_in_pool = 0`; columns `_hg_row_id FixedString(32)` first then declared user columns; `PARTITION BY <partition_by>`; `ORDER BY (<partition_by>, _hg_row_id)` (unpartitioned: `ORDER BY (_hg_row_id)`).
- Back-pressure constants (spec D5): soft = 2400, hard = 2950 per partition; poll interval 2s; rejection = ClickHouse exception code `252` with message prefix `storage_integrity: back-pressure`; never journaled.
- Partition identity (verified on ClickHouse 25.8.28 in docker while writing this plan): for a `String` partition key `system.parts.partition` is the raw value (`p0`), while `system.parts.partition_id` is a 32-hex SipHash that cannot be derived from a row. All partition keys in this plan therefore use the existing logical convention `p_<system.parts.partition>` / `all` (unpartitioned tables show `partition = 'tuple()'`), which is exactly what `payloadexec.PartitionIDForRow`, `payloadexec.DecodeCSV`, `sicore.DecodeNativePayload` (`Row.PartitionID`) and arbiter-core `snode.logicalPartitionID` already produce. Never invent another derivation.
- ClickHouse facts verified in docker (25.8.28): `engine_full` for the pinned RMT is `ReplicatedMergeTree('/sentio/0/unsafe/db__t', 'node-1') PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0, index_granularity = 8192` (ClickHouse appends `index_granularity = 8192`; backtick-quoted identifiers in the CREATE are normalized away; `sorting_key = "p, _hg_row_id"`, `partition_key = "p"`, empty for unpartitioned); `ALTER TABLE … MODIFY SETTING` rewrites the value inside `engine_full`; `CREATE TABLE IF NOT EXISTS` with a different definition is a silent no-op; two RMT tables in different databases on one server sharing a zk path replicate parts; reusing a replica name fails with code 253 `REPLICA_ALREADY_EXISTS`; `DROP TABLE … SYNC` removes the replica's zk metadata; `parts_to_throw_insert = N` rejects the insert that would create part N+1 with code 252 `TOO_MANY_PARTS`.
- English-only code, comments, log and error strings. housegate logging via `github.com/housegate/housegate/pkg/log` (`log.Infow`/`Warnw`/`WarnEveryN`); arbiter-core via the role's `*slog.Logger`. Errors wrapped with `fmt.Errorf("context: %w", err)`; config errors aggregated with `errors.Join`.
- Markdown docs: no hard line-wrapping.
- `Config.Tables` and `SchemaRoot` are one coherent startup snapshot. Protocol-table reconcile intentionally reuses that frozen slice and only detects deletion/drift; hot table/schema onboarding is part of the future authenticated schema-transition lane and requires a role restart in v1.
- Test isolation for RMT: every docker test that creates an RMT must use a per-test unique table id (zk paths are shared across databases and across the parallel `snode_test` / `verifier_test` / `ddl_test` Bazel targets) and drop with `DROP DATABASE … SYNC` in `t.Cleanup`.

## File Structure

arbiter-core (new package `dataplane/ddl`, split by responsibility):
- `dataplane/ddl/naming.go` — `CHTableName`, `ZooKeeperPath`, identifier/literal quoting.
- `dataplane/ddl/pinned.go` — `Pinned`, `PinnedSetting`, `UnsafeSettings()`, `SafeSettings()`, `RowIDColumn`.
- `dataplane/ddl/build.go` — `TableIntent`, `Intents`, `BuildDDL`, `ErrPartitionFreeze` (pure, golden-tested).
- `dataplane/ddl/engine_full.go` — `ParseEngineFullSettings` (pure).
- `dataplane/ddl/verify.go` — `VerifyProtocolTable`, `ErrProtocolTableDrift`, `ErrProtocolTableMissing`.
- `dataplane/ddl/ensure.go` — `Mode`, `ParseMode`, `EnsureProtocolTables`, `DefaultReconcileInterval`.
- `snode/parts.go` (`CHTableName` delegates), `snode/config.go`, `snode/snode.go` (Register/Run wiring), `snode/staged.go` (hard-limit mirror), `verifier/config.go`, `verifier/verifier.go`, `verifier/backends.go`.
- `scripts/ci/clickhouse-keeper.xml`, `.github/workflows/ci.yml`, `.github/workflows/cut-release.yml`, `README.md`.

arbiter: `cmd/arbiter-snode/{main.go,config.go,main_test.go}`, `cmd/arbiter-verifier/{main.go,config.go,main_test.go}`, `README.md`, dependency pins.

housegate:
- `pkg/chproto/client_error.go` — `ClientError` (code-carrying plugin error) + `CodeTooManyParts`.
- `pkg/proxy/relay.go` — `exceptionForPluginError` used by `writeExceptionToClient`.
- `pkg/storageintegrity/physical_table.go` — `PhysicalTableName`.
- `pkg/storageintegrity/payload_partitions.go` — `PayloadPartitionIDs`, `ErrPartitionFreeze`.
- `pkg/storageintegrity/parts_pressure.go` — `PartsKey`, `PartsSnapshot`, `PartsPressureConfig`, `PartsPressureGuard`, `ErrBackpressure`, `BackpressureError`, `LogicalPartitionID`.
- `pkg/config/storage_integrity_config.go` — `StorageIntegrityRuntimeBackpressureConfig`.
- root `storage_integrity_backpressure.go` — `StorageIntegrityPartsPressure` interface, `StorageIntegrityPartsPressureSupervisor`, Prometheus metrics; `storage_integrity_runtime.go`, `storage_integrity_ingress.go`, `build.go` wiring.
- `pkg/integration/storage_backpressure_test.go` — docker test.

sentio-node: `standalone/standalone.go`, `storageintegrityadapter/adapter.go`, `config/config.go`, `standalone/storage_integrity_smoke_test.go`, pins.

---

### Task 1: arbiter-core `dataplane/ddl` — naming, pinned settings, `BuildDDL` (pure, golden-tested)

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` (branch `feat/protocol-tables` off `origin/main`).

**Files:**
- Create: `dataplane/ddl/naming.go`, `dataplane/ddl/pinned.go`, `dataplane/ddl/build.go`, `dataplane/ddl/build_test.go`, `dataplane/ddl/BUILD.bazel` (gazelle)
- Modify: `snode/parts.go:79-83` (`CHTableName` delegates to `ddl.CHTableName`)

**Interfaces:**
- Consumes: `payloadexec.TableSchema{TableID, PartitionBy string; Columns []lthash.Column}`, `lthash.Column{Name, Type string}` (housegate v0.8.1, already a dep).
- Produces (used by Tasks 2–7, 10, 20, 21):
  - `ddl.CHTableName(tableID string) string`
  - `ddl.Pinned{UnsafeDB, SafeDB, PromoteDB, NodeID string; KeeperShardID uint32}`
  - `ddl.ZooKeeperPath(p Pinned, tableID string) string`
  - `ddl.PinnedSetting{Name, Value string}`, `ddl.UnsafeSettings() []PinnedSetting`, `ddl.SafeSettings() []PinnedSetting`, `ddl.RowIDColumn = "_hg_row_id"`, `ddl.RowIDType = "FixedString(32)"`
  - `ddl.TableIntent{Database, Table, Engine, ZooKeeperPath, ReplicaName string; Columns []lthash.Column; PartitionKey string; SortingKey []string; Settings []PinnedSetting}` with `func (t TableIntent) SQL() string`
  - `ddl.Intents(p Pinned, t payloadexec.TableSchema) (unsafe, safe TableIntent, err error)`
  - `ddl.BuildDDL(p Pinned, t payloadexec.TableSchema) (unsafeDDL, safeDDL string, err error)`
  - `ddl.ErrPartitionFreeze` sentinel.

- [ ] **Step 1: Write the failing golden test** `dataplane/ddl/build_test.go`

```go
package ddl

import (
	"errors"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func goldenPinned() Pinned {
	return Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "node-1", KeeperShardID: 0}
}

func goldenSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
}

const goldenUnsafeDDL = "CREATE TABLE IF NOT EXISTS `hg_unsafe`.`db__t` (\n" +
	"    `_hg_row_id` FixedString(32),\n" +
	"    `p` String,\n" +
	"    `v` UInt64\n" +
	") ENGINE = ReplicatedMergeTree('/sentio/0/unsafe/db__t', 'node-1')\n" +
	"PARTITION BY `p`\n" +
	"ORDER BY (`p`, `_hg_row_id`)\n" +
	"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0"

const goldenSafeDDL = "CREATE TABLE IF NOT EXISTS `hg_safe`.`db__t` (\n" +
	"    `_hg_row_id` FixedString(32),\n" +
	"    `p` String,\n" +
	"    `v` UInt64\n" +
	") ENGINE = MergeTree\n" +
	"PARTITION BY `p`\n" +
	"ORDER BY (`p`, `_hg_row_id`)\n" +
	"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0"

func TestBuildDDL_GoldenStringPartitionedTable(t *testing.T) {
	unsafe, safe, err := BuildDDL(goldenPinned(), goldenSchema())
	if err != nil {
		t.Fatalf("BuildDDL: %v", err)
	}
	if unsafe != goldenUnsafeDDL {
		t.Fatalf("unsafe DDL:\n got: %s\nwant: %s", unsafe, goldenUnsafeDDL)
	}
	if safe != goldenSafeDDL {
		t.Fatalf("safe DDL:\n got: %s\nwant: %s", safe, goldenSafeDDL)
	}
}

func TestBuildDDL_UnpartitionedTableOrdersByRowIDOnly(t *testing.T) {
	sch := payloadexec.TableSchema{TableID: "db.u", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	unsafe, safe, err := BuildDDL(goldenPinned(), sch)
	if err != nil {
		t.Fatalf("BuildDDL: %v", err)
	}
	wantUnsafe := "CREATE TABLE IF NOT EXISTS `hg_unsafe`.`db__u` (\n" +
		"    `_hg_row_id` FixedString(32),\n" +
		"    `v` UInt64\n" +
		") ENGINE = ReplicatedMergeTree('/sentio/0/unsafe/db__u', 'node-1')\n" +
		"ORDER BY (`_hg_row_id`)\n" +
		"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0"
	if unsafe != wantUnsafe {
		t.Fatalf("unsafe DDL:\n got: %s\nwant: %s", unsafe, wantUnsafe)
	}
	wantSafe := "CREATE TABLE IF NOT EXISTS `hg_safe`.`db__u` (\n" +
		"    `_hg_row_id` FixedString(32),\n" +
		"    `v` UInt64\n" +
		") ENGINE = MergeTree\n" +
		"ORDER BY (`_hg_row_id`)\n" +
		"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0"
	if safe != wantSafe {
		t.Fatalf("safe DDL:\n got: %s\nwant: %s", safe, wantSafe)
	}
}

func TestBuildDDL_KeeperShardAndNodeIDLandInZKPath(t *testing.T) {
	p := goldenPinned()
	p.KeeperShardID = 3
	p.NodeID = "verifier-a'b"
	unsafe, _, err := BuildDDL(p, goldenSchema())
	if err != nil {
		t.Fatalf("BuildDDL: %v", err)
	}
	if want := "ENGINE = ReplicatedMergeTree('/sentio/3/unsafe/db__t', 'verifier-a\\'b')"; !contains(unsafe, want) {
		t.Fatalf("unsafe DDL missing %q:\n%s", want, unsafe)
	}
	if got := ZooKeeperPath(p, "db.t"); got != "/sentio/3/unsafe/db__t" {
		t.Fatalf("ZooKeeperPath = %q", got)
	}
}

func TestBuildDDL_RejectsPartitionFreezeViolations(t *testing.T) {
	cases := map[string]payloadexec.TableSchema{
		"expression": {TableID: "db.t", PartitionBy: "toYYYYMM(d)", Columns: []lthash.Column{{Name: "d", Type: "Date"}}},
		"non-string": {TableID: "db.t", PartitionBy: "n", Columns: []lthash.Column{{Name: "n", Type: "UInt64"}}},
		"undeclared": {TableID: "db.t", PartitionBy: "x", Columns: []lthash.Column{{Name: "p", Type: "String"}}},
	}
	for name, sch := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := BuildDDL(goldenPinned(), sch)
			if !errors.Is(err, ErrPartitionFreeze) {
				t.Fatalf("err = %v, want ErrPartitionFreeze", err)
			}
		})
	}
}

func TestBuildDDL_RejectsRowIDColumnInDeclaredSchema(t *testing.T) {
	sch := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "_hg_row_id", Type: "FixedString(32)"}}}
	if _, _, err := BuildDDL(goldenPinned(), sch); err == nil {
		t.Fatal("declared _hg_row_id must be rejected")
	}
}

func TestIntents_MatchRenderedDDLShape(t *testing.T) {
	unsafe, safe, err := Intents(goldenPinned(), goldenSchema())
	if err != nil {
		t.Fatalf("Intents: %v", err)
	}
	if unsafe.Engine != "ReplicatedMergeTree" || unsafe.ZooKeeperPath != "/sentio/0/unsafe/db__t" || unsafe.ReplicaName != "node-1" {
		t.Fatalf("unsafe intent: %+v", unsafe)
	}
	if safe.Engine != "MergeTree" || safe.ZooKeeperPath != "" || len(safe.Settings) != 1 {
		t.Fatalf("safe intent: %+v", safe)
	}
	if unsafe.PartitionKey != "p" || len(unsafe.SortingKey) != 2 || unsafe.SortingKey[1] != "_hg_row_id" {
		t.Fatalf("unsafe keys: %+v", unsafe)
	}
	if unsafe.Columns[0].Name != "_hg_row_id" || unsafe.Columns[0].Type != "FixedString(32)" || len(unsafe.Columns) != 3 {
		t.Fatalf("unsafe columns: %+v", unsafe.Columns)
	}
	if unsafe.SQL() != goldenUnsafeDDL || safe.SQL() != goldenSafeDDL {
		t.Fatal("TableIntent.SQL must equal BuildDDL output")
	}
}

func TestCHTableName(t *testing.T) {
	if got := CHTableName("db.t"); got != "db__t" {
		t.Fatalf("CHTableName = %q", got)
	}
	if got := CHTableName("a.b.c"); got != "a__b__c" {
		t.Fatalf("CHTableName = %q", got)
	}
}

func contains(s, sub string) bool { return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0) }

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Run to verify it fails** — `mkdir -p dataplane/ddl && go test ./dataplane/ddl/...` Expected: FAIL (`undefined: BuildDDL` etc.).

- [ ] **Step 3: Implement** — `dataplane/ddl/naming.go`:

```go
// Package ddl renders and verifies the protocol-owned physical tables of the
// storage-integrity data plane (hg_unsafe / hg_safe / hg_promote). It is the
// single source of the D2 naming freeze and the D3 pinned engine settings.
package ddl

import (
	"fmt"
	"strings"
)

// CHTableName maps a logical storage-integrity table id (<database>.<table>)
// to its physical ClickHouse table name (D2 naming freeze). snode.CHTableName
// delegates here; do not copy the rule.
func CHTableName(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}

// ZooKeeperPath is the anchored replication path of an hg_unsafe table:
// /sentio/<keeper_shard_id>/unsafe/<CHTableName>.
func ZooKeeperPath(p Pinned, tableID string) string {
	return fmt.Sprintf("/sentio/%d/unsafe/%s", p.KeeperShardID, CHTableName(tableID))
}

func quoteIdent(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
```

`dataplane/ddl/pinned.go`:

```go
package ddl

const (
	RowIDColumn = "_hg_row_id"
	RowIDType   = "FixedString(32)"

	EngineReplicatedMergeTree = "ReplicatedMergeTree"
	EngineMergeTree           = "MergeTree"
)

// Pinned carries the deployment-specific inputs of the protocol DDL: the three
// database names, the replica name (the node id the role registers with the
// Arbiter) and the keeper shard (0 in v1).
type Pinned struct {
	UnsafeDB, SafeDB, PromoteDB string
	NodeID                      string
	KeeperShardID               uint32
}

// PinnedSetting is one frozen MergeTree setting (spec D3).
type PinnedSetting struct {
	Name  string
	Value string
}

// UnsafeSettings are the hg_unsafe pins, in DDL order.
func UnsafeSettings() []PinnedSetting {
	return []PinnedSetting{
		{Name: "max_bytes_to_merge_at_max_space_in_pool", Value: "0"},
		{Name: "parts_to_delay_insert", Value: "1000"},
		{Name: "parts_to_throw_insert", Value: "3000"},
		{Name: "max_parts_in_total", Value: "100000"},
		{Name: "replicated_deduplication_window", Value: "0"},
	}
}

// SafeSettings are the hg_safe pins (interim: merges stay stopped, spec §6).
func SafeSettings() []PinnedSetting {
	return []PinnedSetting{{Name: "max_bytes_to_merge_at_max_space_in_pool", Value: "0"}}
}
```

`dataplane/ddl/build.go`:

```go
package ddl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrPartitionFreeze is returned for a declared schema outside the P1c
// partition-key freeze: partition_by must be empty or name a declared bare
// String column (no expressions, no other types).
var ErrPartitionFreeze = errors.New("ddl: partition_by must be a bare String column declared in the schema (P1c partition freeze: no expressions, no non-String keys)")

// TableIntent is the structured form of one protocol table. BuildDDL renders
// it and VerifyProtocolTable compares live metadata against it, so both sides
// share one definition.
type TableIntent struct {
	Database      string
	Table         string
	Engine        string
	ZooKeeperPath string // ReplicatedMergeTree only
	ReplicaName   string // ReplicatedMergeTree only
	Columns       []lthash.Column
	PartitionKey  string // "" for unpartitioned
	SortingKey    []string
	Settings      []PinnedSetting
}

// SQL renders the CREATE TABLE IF NOT EXISTS statement for the intent.
func (t TableIntent) SQL() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s.%s (\n", quoteIdent(t.Database), quoteIdent(t.Table))
	for i, c := range t.Columns {
		sep := ","
		if i == len(t.Columns)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "    %s %s%s\n", quoteIdent(c.Name), c.Type, sep)
	}
	b.WriteString(") ENGINE = " + t.Engine)
	if t.Engine == EngineReplicatedMergeTree {
		fmt.Fprintf(&b, "(%s, %s)", quoteLiteral(t.ZooKeeperPath), quoteLiteral(t.ReplicaName))
	}
	b.WriteString("\n")
	if t.PartitionKey != "" {
		fmt.Fprintf(&b, "PARTITION BY %s\n", quoteIdent(t.PartitionKey))
	}
	keys := make([]string, 0, len(t.SortingKey))
	for _, k := range t.SortingKey {
		keys = append(keys, quoteIdent(k))
	}
	fmt.Fprintf(&b, "ORDER BY (%s)\n", strings.Join(keys, ", "))
	settings := make([]string, 0, len(t.Settings))
	for _, s := range t.Settings {
		settings = append(settings, s.Name+" = "+s.Value)
	}
	b.WriteString("SETTINGS " + strings.Join(settings, ", "))
	return b.String()
}

// Intents derives the hg_unsafe and hg_safe intents for one declared schema.
func Intents(p Pinned, t payloadexec.TableSchema) (TableIntent, TableIntent, error) {
	if err := validatePartitionFreeze(t); err != nil {
		return TableIntent{}, TableIntent{}, fmt.Errorf("table %s: %w", t.TableID, err)
	}
	for _, c := range t.Columns {
		if c.Name == RowIDColumn {
			return TableIntent{}, TableIntent{}, fmt.Errorf("table %s: declared schema must not contain %s (the protocol injects it)", t.TableID, RowIDColumn)
		}
	}
	cols := make([]lthash.Column, 0, len(t.Columns)+1)
	cols = append(cols, lthash.Column{Name: RowIDColumn, Type: RowIDType})
	cols = append(cols, t.Columns...)
	var sorting []string
	if t.PartitionBy != "" {
		sorting = append(sorting, t.PartitionBy)
	}
	sorting = append(sorting, RowIDColumn)
	table := CHTableName(t.TableID)
	unsafe := TableIntent{
		Database: p.UnsafeDB, Table: table, Engine: EngineReplicatedMergeTree,
		ZooKeeperPath: ZooKeeperPath(p, t.TableID), ReplicaName: p.NodeID,
		Columns: cols, PartitionKey: t.PartitionBy, SortingKey: sorting, Settings: UnsafeSettings(),
	}
	safe := TableIntent{
		Database: p.SafeDB, Table: table, Engine: EngineMergeTree,
		Columns: cols, PartitionKey: t.PartitionBy, SortingKey: sorting, Settings: SafeSettings(),
	}
	return unsafe, safe, nil
}

// BuildDDL renders the two CREATE TABLE IF NOT EXISTS statements. Pure; golden
// tested. hg_promote is created lazily by the promotion path (AS hg_safe).
func BuildDDL(p Pinned, t payloadexec.TableSchema) (string, string, error) {
	unsafe, safe, err := Intents(p, t)
	if err != nil {
		return "", "", err
	}
	return unsafe.SQL(), safe.SQL(), nil
}

func validatePartitionFreeze(t payloadexec.TableSchema) error {
	if t.PartitionBy == "" {
		return nil
	}
	if strings.ContainsAny(t.PartitionBy, "()") {
		return fmt.Errorf("%w: got expression %q", ErrPartitionFreeze, t.PartitionBy)
	}
	for _, c := range t.Columns {
		if c.Name == t.PartitionBy {
			if c.Type != "String" {
				return fmt.Errorf("%w: column %q has type %s", ErrPartitionFreeze, t.PartitionBy, c.Type)
			}
			return nil
		}
	}
	return fmt.Errorf("%w: %q names no declared column", ErrPartitionFreeze, t.PartitionBy)
}
```

Then in `snode/parts.go` replace the body of `CHTableName` with `return ddl.CHTableName(tableID)`, add import `"github.com/sentioxyz/arbiter-core/dataplane/ddl"`, drop the now-unused `strings` import.

- [ ] **Step 4: Gazelle + run** — `bazel run //:gazelle && go test ./dataplane/ddl/... ./snode/... && bazel test //dataplane/ddl:ddl_test //snode:snode_test --test_output=errors` Expected: PASS (docker-gated snode tests skip). Confirm `dataplane/ddl/BUILD.bazel` deps contain `@housegate//pkg/lthash` and `@housegate//pkg/replay/payloadexec`.

- [ ] **Step 5: Commit**

```bash
git add dataplane/ddl snode/parts.go snode/BUILD.bazel
git commit -m "feat(ddl): pinned protocol-table DDL builder (BuildDDL, D2 naming, D3 settings)"
```

---

### Task 2: arbiter-core `dataplane/ddl` — `engine_full` SETTINGS parser (pure)

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` (same branch).

**Files:**
- Create: `dataplane/ddl/engine_full.go`, `dataplane/ddl/engine_full_test.go`

**Interfaces:**
- Produces: `ddl.ParseEngineFullSettings(engineFull string) (map[string]string, error)` — returns the per-table overrides in the trailing `SETTINGS a = 1, b = 2` clause of `system.tables.engine_full`; empty map when no `SETTINGS` clause. Used by Task 3.

- [ ] **Step 1: Write the failing test** `dataplane/ddl/engine_full_test.go` (strings captured verbatim from ClickHouse 25.8.28)

```go
package ddl

import (
	"reflect"
	"testing"
)

func TestParseEngineFullSettings_ReplicatedMergeTreePins(t *testing.T) {
	in := "ReplicatedMergeTree('/sentio/0/unsafe/db__t', 'node-1') PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0, index_granularity = 8192"
	got, err := ParseEngineFullSettings(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := map[string]string{
		"max_bytes_to_merge_at_max_space_in_pool": "0",
		"parts_to_delay_insert":                   "1000",
		"parts_to_throw_insert":                   "3000",
		"max_parts_in_total":                      "100000",
		"replicated_deduplication_window":         "0",
		"index_granularity":                       "8192",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("settings = %v want %v", got, want)
	}
}

func TestParseEngineFullSettings_MergeTreeAndTamperedValue(t *testing.T) {
	in := "MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, index_granularity = 8192"
	got, err := ParseEngineFullSettings(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got["max_bytes_to_merge_at_max_space_in_pool"] != "0" || got["index_granularity"] != "8192" || len(got) != 2 {
		t.Fatalf("settings = %v", got)
	}
	tampered := "ReplicatedMergeTree('/sentio/0/unsafe/db__t', 'node-1') PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 2999, max_parts_in_total = 100000, replicated_deduplication_window = 0, index_granularity = 8192"
	got, err = ParseEngineFullSettings(tampered)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got["parts_to_throw_insert"] != "2999" {
		t.Fatalf("parts_to_throw_insert = %q want 2999", got["parts_to_throw_insert"])
	}
}

func TestParseEngineFullSettings_NoSettingsClause(t *testing.T) {
	got, err := ParseEngineFullSettings("MergeTree ORDER BY _hg_row_id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("settings = %v want empty", got)
	}
}

func TestParseEngineFullSettings_RejectsMalformedPair(t *testing.T) {
	if _, err := ParseEngineFullSettings("MergeTree ORDER BY x SETTINGS index_granularity"); err == nil {
		t.Fatal("malformed setting pair must error")
	}
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./dataplane/ddl/ -run TestParseEngineFull` Expected: FAIL (`undefined: ParseEngineFullSettings`).

- [ ] **Step 3: Implement** `dataplane/ddl/engine_full.go`

```go
package ddl

import (
	"fmt"
	"strings"
)

const settingsClause = " SETTINGS "

// ParseEngineFullSettings extracts the per-table MergeTree setting overrides
// from system.tables.engine_full. ClickHouse renders them as the trailing
// "SETTINGS name = value, name = value" clause (it always appends
// index_granularity). Values are integers or quoted literals; the pinned
// settings compared by VerifyProtocolTable are all plain integers, so a
// comma-split on ", " is exact for them.
func ParseEngineFullSettings(engineFull string) (map[string]string, error) {
	out := map[string]string{}
	idx := strings.LastIndex(engineFull, settingsClause)
	if idx < 0 {
		return out, nil
	}
	tail := strings.TrimSpace(engineFull[idx+len(settingsClause):])
	if tail == "" {
		return out, nil
	}
	for _, pair := range strings.Split(tail, ", ") {
		name, value, ok := strings.Cut(pair, " = ")
		if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("ddl: malformed engine_full setting %q in %q", pair, engineFull)
		}
		out[strings.TrimSpace(name)] = strings.TrimSpace(value)
	}
	return out, nil
}
```

- [ ] **Step 4: Run** — `go test ./dataplane/ddl/... && bazel run //:gazelle && bazel test //dataplane/ddl:ddl_test --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add dataplane/ddl && git commit -m "feat(ddl): parse per-table settings from system.tables.engine_full"`

---

### Task 3: arbiter-core `dataplane/ddl` — `VerifyProtocolTable`, `EnsureProtocolTables`, keeper-enabled docker tests

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` (same branch).

**Files:**
- Create: `dataplane/ddl/verify.go`, `dataplane/ddl/ensure.go`, `dataplane/ddl/ensure_ch_test.go`, `dataplane/ddl/ch_test.go` (docker gate + keeper gate helpers), `scripts/ci/clickhouse-keeper.xml`
- Modify: `.github/workflows/ci.yml` (job `integration-clickhouse`), `.github/workflows/cut-release.yml` (step `start ClickHouse` + test list), `README.md` (docker command)

**Interfaces:**
- Consumes: Task 1 `Intents`/`TableIntent`/`Pinned`, Task 2 `ParseEngineFullSettings`; `clickhouse.Conn` (`github.com/ClickHouse/clickhouse-go/v2`).
- Produces (used by Tasks 5, 6, 10, 20, 21):
  - `ddl.Mode` (`ModeOff`, `ModeVerifyOnly`, `ModeCreateAndVerify`), `ddl.ParseMode(s string) (Mode, error)` (`"off"|"verify"|"create"`), `func (m Mode) String() string`
  - `ddl.DefaultReconcileInterval = 60 * time.Second`
  - `ddl.ErrProtocolTableDrift`, `ddl.ErrProtocolTableMissing`
  - `ddl.VerifyProtocolTable(ctx context.Context, conn clickhouse.Conn, want TableIntent) error`
  - `ddl.EnsureProtocolTables(ctx context.Context, conn clickhouse.Conn, p Pinned, tables []payloadexec.TableSchema, mode Mode, logger *slog.Logger) error`

- [ ] **Step 1: Keeper config + test gates.** Create `scripts/ci/clickhouse-keeper.xml` (embedded single-node Keeper; identical to housegate's `pkg/integration/storage_promotion_mvp_test.go::writePromotionKeeperConfig`):

```xml
<clickhouse>
  <keeper_server>
    <tcp_port>9181</tcp_port>
    <server_id>1</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    <coordination_settings>
      <operation_timeout_ms>10000</operation_timeout_ms>
      <session_timeout_ms>30000</session_timeout_ms>
    </coordination_settings>
    <raft_configuration>
      <server>
        <id>1</id>
        <hostname>127.0.0.1</hostname>
        <port>9234</port>
      </server>
    </raft_configuration>
  </keeper_server>
  <zookeeper>
    <node index="1">
      <host>127.0.0.1</host>
      <port>9181</port>
    </node>
  </zookeeper>
</clickhouse>
```

Create `dataplane/ddl/ch_test.go`:

```go
package ddl

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// requireCH mirrors snode/ch_test.go: opt-in via ARBITER_CH_INTEGRATION=1.
func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("clickhouse ping: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// requireKeeper skips (or fails under ARBITER_CH_KEEPER=1, which CI sets) when
// the ClickHouse has no Keeper: ReplicatedMergeTree cannot be created without one.
func requireKeeper(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	var n uint64
	err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n)
	if err == nil {
		return
	}
	if os.Getenv("ARBITER_CH_KEEPER") == "1" {
		t.Fatalf("ARBITER_CH_KEEPER=1 but ClickHouse has no Keeper (mount scripts/ci/clickhouse-keeper.xml): %v", err)
	}
	t.Skipf("ClickHouse has no Keeper configured; skipping ReplicatedMergeTree test: %v", err)
}

// uniqueSuffix keeps zk paths and databases unique per test: zk paths are
// shared across databases and across the parallel snode/verifier/ddl targets.
func uniqueSuffix(t *testing.T) string {
	t.Helper()
	sum := sha1.Sum([]byte(t.Name()))
	return hex.EncodeToString(sum[:])[:10]
}

func testPinned(t *testing.T) Pinned {
	t.Helper()
	s := uniqueSuffix(t)
	return Pinned{UnsafeDB: "hg_unsafe_" + s, SafeDB: "hg_safe_" + s, PromoteDB: "hg_promote_" + s, NodeID: "node-" + s}
}

func dropDatabasesSync(t *testing.T, conn clickhouse.Conn, p Pinned) {
	t.Helper()
	t.Cleanup(func() {
		for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
}
```

- [ ] **Step 2: Write the failing docker tests** `dataplane/ddl/ensure_ch_test.go`

```go
package ddl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func ensureSchema(t *testing.T) payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t_" + uniqueSuffix(t),
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
}

func TestEnsureProtocolTables_CreateVerifyTamperDrift(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	tables := []payloadexec.TableSchema{sch}

	if err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	// Idempotent: second run creates nothing and verifies clean.
	if err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	table := CHTableName(sch.TableID)
	var engine, zk, replica string
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", p.UnsafeDB, table).Scan(&engine); err != nil || engine != "ReplicatedMergeTree" {
		t.Fatalf("unsafe engine = %q err=%v", engine, err)
	}
	if err := conn.QueryRow(ctx, "SELECT zookeeper_path, replica_name FROM system.replicas WHERE database = ? AND table = ?", p.UnsafeDB, table).Scan(&zk, &replica); err != nil {
		t.Fatalf("system.replicas: %v", err)
	}
	if zk != ZooKeeperPath(p, sch.TableID) || replica != p.NodeID {
		t.Fatalf("zk=%q replica=%q", zk, replica)
	}
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", p.SafeDB, table).Scan(&engine); err != nil || engine != "MergeTree" {
		t.Fatalf("safe engine = %q err=%v", engine, err)
	}

	// Tamper a pinned setting: verify must fail closed and name table + field.
	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s MODIFY SETTING parts_to_throw_insert = 2999", p.UnsafeDB, table)); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default())
	if !errors.Is(err, ErrProtocolTableDrift) {
		t.Fatalf("err = %v, want ErrProtocolTableDrift", err)
	}
	if !strings.Contains(err.Error(), p.UnsafeDB+"."+table) || !strings.Contains(err.Error(), "parts_to_throw_insert") {
		t.Fatalf("drift error must name table and setting: %v", err)
	}
}

func TestEnsureProtocolTables_VerifyOnlyNeverCreates(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	tables := []payloadexec.TableSchema{ensureSchema(t)}

	err := EnsureProtocolTables(ctx, conn, p, tables, ModeVerifyOnly, slog.Default())
	if !errors.Is(err, ErrProtocolTableMissing) {
		t.Fatalf("err = %v, want ErrProtocolTableMissing", err)
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.databases WHERE name IN (?, ?, ?)", p.UnsafeDB, p.SafeDB, p.PromoteDB).Scan(&n); err != nil {
		t.Fatalf("count databases: %v", err)
	}
	if n != 0 {
		t.Fatalf("verify-only created %d databases", n)
	}
}

func TestEnsureProtocolTables_DetectsEngineAndColumnDrift(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	table := CHTableName(sch.TableID)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + p.UnsafeDB,
		"CREATE DATABASE IF NOT EXISTS " + p.SafeDB,
		// Hand-created legacy shape: MergeTree, ORDER BY tuple(), no pins.
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()", p.UnsafeDB, table),
		// Missing the v column.
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", p.SafeDB, table),
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{sch}, ModeVerifyOnly, slog.Default())
	if !errors.Is(err, ErrProtocolTableDrift) {
		t.Fatalf("err = %v, want ErrProtocolTableDrift", err)
	}
	msg := err.Error()
	for _, want := range []string{"engine", "columns", p.UnsafeDB + "." + table, p.SafeDB + "." + table} {
		if !strings.Contains(msg, want) {
			t.Fatalf("drift error missing %q: %s", want, msg)
		}
	}
}

func TestEnsureProtocolTables_SkipsFreezeViolationWithWarning(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	good := ensureSchema(t)
	bad := payloadexec.TableSchema{TableID: "db.bad_" + uniqueSuffix(t), PartitionBy: "n", Columns: []lthash.Column{{Name: "n", Type: "UInt64"}}}

	if err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{bad, good}, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("one bad declaration must not stop the ensure: %v", err)
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", p.UnsafeDB, CHTableName(bad.TableID)).Scan(&n); err != nil || n != 0 {
		t.Fatalf("freeze-violating table must be skipped, count=%d err=%v", n, err)
	}
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", p.UnsafeDB, CHTableName(good.TableID)).Scan(&n); err != nil || n != 1 {
		t.Fatalf("good table must be created, count=%d err=%v", n, err)
	}
}

func TestEnsureProtocolTables_TwoReplicasSameZKPathReplicate(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	a := testPinned(t)
	dropDatabasesSync(t, conn, a)
	b := Pinned{UnsafeDB: a.UnsafeDB + "_b", SafeDB: a.SafeDB + "_b", PromoteDB: a.PromoteDB + "_b", NodeID: a.NodeID + "-b"}
	dropDatabasesSync(t, conn, b)
	sch := ensureSchema(t)
	tables := []payloadexec.TableSchema{sch}
	for _, p := range []Pinned{a, b} {
		if err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default()); err != nil {
			t.Fatalf("ensure %s: %v", p.NodeID, err)
		}
	}
	table := CHTableName(sch.TableID)
	if err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p0', 1)", a.UnsafeDB, table, 1)); err != nil {
		t.Fatalf("insert into replica a: %v", err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf("SYSTEM SYNC REPLICA %s.%s", b.UnsafeDB, table)); err != nil {
		t.Fatalf("sync replica b: %v", err)
	}
	var rows uint64
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s", b.UnsafeDB, table)).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("replica b rows = %d err=%v, want 1 (same zk path %s)", rows, err, ZooKeeperPath(a, sch.TableID))
	}
}

func TestParseMode(t *testing.T) {
	for in, want := range map[string]Mode{"off": ModeOff, "verify": ModeVerifyOnly, "create": ModeCreateAndVerify} {
		got, err := ParseMode(in)
		if err != nil || got != want {
			t.Fatalf("ParseMode(%q) = %v, %v", in, got, err)
		}
		if got.String() != in {
			t.Fatalf("String() = %q want %q", got.String(), in)
		}
	}
	if _, err := ParseMode("maybe"); err == nil {
		t.Fatal("unknown mode must error")
	}
}
```

- [ ] **Step 3: Run to verify it fails** — `go test ./dataplane/ddl/ -run 'TestParseMode|TestEnsure'` Expected: FAIL to compile (`undefined: EnsureProtocolTables`, `ModeOff`, …).

- [ ] **Step 4: Implement** `dataplane/ddl/verify.go`

```go
package ddl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
)

var (
	// ErrProtocolTableDrift: an existing table differs from the pinned intent
	// (engine, zk path/replica, columns, keys, or a pinned setting). Fail-closed
	// (spec D4): the role must not start; there is no auto-ALTER in v1.
	ErrProtocolTableDrift = errors.New("ddl: protocol table drift")
	// ErrProtocolTableMissing: the table does not exist (VerifyOnly mode).
	ErrProtocolTableMissing = errors.New("ddl: protocol table missing")
)

// VerifyProtocolTable compares the live table against the intent using
// system.tables (engine, engine_full, sorting_key, partition_key),
// system.columns (name/type by position), system.replicas (zk path + replica
// for ReplicatedMergeTree) and system.merge_tree_settings (global defaults for
// pinned settings not overridden in engine_full). Every mismatch is reported
// in one joined error naming <db>.<table> and the field.
func VerifyProtocolTable(ctx context.Context, conn clickhouse.Conn, want TableIntent) error {
	qualified := want.Database + "." + want.Table
	var engine, engineFull, sortingKey, partitionKey string
	err := conn.QueryRow(ctx, `
		SELECT engine, engine_full, sorting_key, partition_key
		FROM system.tables WHERE database = ? AND name = ?`, want.Database, want.Table,
	).Scan(&engine, &engineFull, &sortingKey, &partitionKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrProtocolTableMissing, qualified)
		}
		return fmt.Errorf("ddl: read system.tables for %s: %w", qualified, err)
	}
	var drifts []error
	drift := func(field, got, exp string) {
		drifts = append(drifts, fmt.Errorf("%w: %s %s: got %q want %q", ErrProtocolTableDrift, qualified, field, got, exp))
	}
	if engine != want.Engine {
		drift("engine", engine, want.Engine)
	}
	if wantSK := strings.Join(want.SortingKey, ", "); sortingKey != wantSK {
		drift("sorting_key", sortingKey, wantSK)
	}
	if partitionKey != want.PartitionKey {
		drift("partition_key", partitionKey, want.PartitionKey)
	}

	cols, err := readColumns(ctx, conn, want.Database, want.Table)
	if err != nil {
		return err
	}
	if !equalColumns(cols, want.Columns) {
		drift("columns", formatColumns(cols), formatColumns(want.Columns))
	}

	overrides, err := ParseEngineFullSettings(engineFull)
	if err != nil {
		return fmt.Errorf("ddl: %s: %w", qualified, err)
	}
	globals, err := readGlobalMergeTreeSettings(ctx, conn, want.Settings)
	if err != nil {
		return err
	}
	for _, s := range want.Settings {
		got, ok := overrides[s.Name]
		if !ok {
			got = globals[s.Name]
		}
		if got != s.Value {
			drift("setting "+s.Name, got, s.Value)
		}
	}

	if want.Engine == EngineReplicatedMergeTree && engine == EngineReplicatedMergeTree {
		var zk, replica string
		err := conn.QueryRow(ctx, `SELECT zookeeper_path, replica_name FROM system.replicas WHERE database = ? AND table = ?`, want.Database, want.Table).Scan(&zk, &replica)
		if err != nil {
			return fmt.Errorf("ddl: read system.replicas for %s: %w", qualified, err)
		}
		if zk != want.ZooKeeperPath {
			drift("zookeeper_path", zk, want.ZooKeeperPath)
		}
		if replica != want.ReplicaName {
			drift("replica_name", replica, want.ReplicaName)
		}
	}
	return errors.Join(drifts...)
}

func readColumns(ctx context.Context, conn clickhouse.Conn, db, table string) ([]lthash.Column, error) {
	rows, err := conn.Query(ctx, `SELECT name, type FROM system.columns WHERE database = ? AND table = ? ORDER BY position`, db, table)
	if err != nil {
		return nil, fmt.Errorf("ddl: read system.columns for %s.%s: %w", db, table, err)
	}
	defer rows.Close()
	var out []lthash.Column
	for rows.Next() {
		var c lthash.Column
		if err := rows.Scan(&c.Name, &c.Type); err != nil {
			return nil, fmt.Errorf("ddl: scan system.columns: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func readGlobalMergeTreeSettings(ctx context.Context, conn clickhouse.Conn, pinned []PinnedSetting) (map[string]string, error) {
	names := make([]string, 0, len(pinned))
	for _, s := range pinned {
		names = append(names, s.Name)
	}
	rows, err := conn.Query(ctx, `SELECT name, value FROM system.merge_tree_settings WHERE name IN (?)`, names)
	if err != nil {
		return nil, fmt.Errorf("ddl: read system.merge_tree_settings: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return nil, fmt.Errorf("ddl: scan system.merge_tree_settings: %w", err)
		}
		out[name] = value
	}
	return out, rows.Err()
}

func equalColumns(a, b []lthash.Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Type != b[i].Type {
			return false
		}
	}
	return true
}

func formatColumns(cols []lthash.Column) string {
	parts := make([]string, 0, len(cols))
	for _, c := range cols {
		parts = append(parts, c.Name+" "+c.Type)
	}
	return strings.Join(parts, ", ")
}
```

`dataplane/ddl/ensure.go`:

```go
package ddl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Mode selects what EnsureProtocolTables may do (spec D1).
type Mode int

const (
	// ModeOff: the host owns the DDL (test harnesses that pre-create tables);
	// EnsureProtocolTables is a no-op. Never used by production wiring.
	ModeOff Mode = iota
	// ModeVerifyOnly: schema source is the local ClickHouse; creating from what
	// we read would be circular, so only verify.
	ModeVerifyOnly
	// ModeCreateAndVerify: schema source is network_state or chain; CREATE IF
	// NOT EXISTS then verify.
	ModeCreateAndVerify
)

// DefaultReconcileInterval is the periodic re-run cadence used by the roles.
const DefaultReconcileInterval = 60 * time.Second

func (m Mode) String() string {
	switch m {
	case ModeOff:
		return "off"
	case ModeVerifyOnly:
		return "verify"
	case ModeCreateAndVerify:
		return "create"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// ParseMode parses "off" | "verify" | "create".
func ParseMode(s string) (Mode, error) {
	switch s {
	case "off":
		return ModeOff, nil
	case "verify":
		return ModeVerifyOnly, nil
	case "create":
		return ModeCreateAndVerify, nil
	default:
		return ModeOff, fmt.Errorf("ddl: unknown ensure-tables mode %q (want off|verify|create)", s)
	}
}

// EnsureProtocolTables creates (mode permitting) and verifies the hg_unsafe /
// hg_safe tables for every declared schema. A declaration outside the P1c
// partition freeze is skipped with a warning so one bad declaration does not
// stop the node; drift or a missing table (VerifyOnly) is fail-closed. All
// per-table errors are joined so operators see every drift at once.
func EnsureProtocolTables(ctx context.Context, conn clickhouse.Conn, p Pinned, tables []payloadexec.TableSchema, mode Mode, logger *slog.Logger) error {
	if mode == ModeOff {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	if conn == nil {
		return errors.New("ddl: clickhouse connection is required")
	}
	if p.UnsafeDB == "" || p.SafeDB == "" || p.PromoteDB == "" || p.NodeID == "" {
		return errors.New("ddl: Pinned needs UnsafeDB, SafeDB, PromoteDB and NodeID")
	}
	if mode == ModeCreateAndVerify {
		for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
			if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(db)); err != nil {
				return fmt.Errorf("ddl: create database %s: %w", db, err)
			}
		}
	}
	var errs []error
	for _, t := range tables {
		unsafe, safe, err := Intents(p, t)
		if err != nil {
			if errors.Is(err, ErrPartitionFreeze) {
				logger.Warn("skipping protocol tables for declaration outside the partition freeze", "table_id", t.TableID, "err", err)
				continue
			}
			errs = append(errs, err)
			continue
		}
		for _, intent := range []TableIntent{unsafe, safe} {
			if mode == ModeCreateAndVerify {
				if err := conn.Exec(ctx, intent.SQL()); err != nil {
					errs = append(errs, fmt.Errorf("ddl: create %s.%s: %w", intent.Database, intent.Table, err))
					continue
				}
			}
			if err := VerifyProtocolTable(ctx, conn, intent); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	logger.Info("protocol tables ensured", "mode", mode.String(), "tables", len(tables), "unsafe_db", p.UnsafeDB, "safe_db", p.SafeDB, "node_id", p.NodeID)
	return nil
}
```

Note on `readGlobalMergeTreeSettings`: clickhouse-go expands a `[]string` bound to `IN (?)`; if the pinned fork rejects it, fall back to building the `IN ('a', 'b')` list with `quoteLiteral` — do not silently drop the query.

- [ ] **Step 5: CI + README.** In `.github/workflows/ci.yml` replace the `services:` block of `integration-clickhouse` with a docker-run step after checkout (mirrors `cut-release.yml`), and add `//dataplane/ddl:ddl_test` + `ARBITER_CH_KEEPER`:

```yaml
  integration-clickhouse:
    if: >-
      github.event_name != 'pull_request' ||
      github.event.pull_request.head.repo.full_name == github.repository
    runs-on: [self-hosted]
    steps:
      - uses: actions/checkout@v4
      - uses: bazelbuild/setup-bazelisk@v3
      - name: start ClickHouse (embedded Keeper for ReplicatedMergeTree)
        run: |
          docker rm -f arbiter-core-ci-clickhouse >/dev/null 2>&1 || true
          docker run -d --rm \
            --name arbiter-core-ci-clickhouse \
            -p 127.0.0.1::9000 \
            -e CLICKHOUSE_SKIP_USER_SETUP=1 \
            -v "$PWD/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
            clickhouse/clickhouse-server:25.8
          host_port="$(docker inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' arbiter-core-ci-clickhouse)"
          echo "CH_ADDR=127.0.0.1:${host_port}" >> "$GITHUB_ENV"
          echo "CH_PORT=${host_port}" >> "$GITHUB_ENV"
      - name: wait for ClickHouse
        run: |
          for _ in $(seq 1 30); do
            timeout 1 bash -c "</dev/tcp/127.0.0.1/${CH_PORT}" && exit 0
            sleep 1
          done
          docker logs arbiter-core-ci-clickhouse
          exit 1
      - name: data-plane integration
        env:
          ARBITER_CH_INTEGRATION: "1"
          ARBITER_CH_KEEPER: "1"
        run: |
          bazel test \
            //dataplane/ddl:ddl_test \
            //snode:snode_test \
            //verifier:verifier_test \
            --test_env=ARBITER_CH_INTEGRATION \
            --test_env=ARBITER_CH_KEEPER \
            --test_env=CH_ADDR \
            --test_timeout=900 \
            --test_output=errors
      - name: stop ClickHouse
        if: always()
        run: docker rm -f arbiter-core-ci-clickhouse >/dev/null 2>&1 || true
```

Apply the same three edits to `cut-release.yml`'s `start ClickHouse` step (add the `-v …keeper.xml` mount), its test step (add `//dataplane/ddl:ddl_test`, `ARBITER_CH_KEEPER: "1"`, `--test_env=ARBITER_CH_KEEPER`). In `README.md` replace the docker run example with the mounted-keeper form and add `//dataplane/ddl:ddl_test` + `ARBITER_CH_KEEPER=1` to the bazel test example.

- [ ] **Step 6: Run locally against a keeper-enabled ClickHouse**

```bash
docker rm -f arbiter-core-ch >/dev/null 2>&1 || true
docker run -d --rm --name arbiter-core-ch -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" clickhouse/clickhouse-server:25.8
sleep 8
bazel run //:gazelle
ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 CH_ADDR=127.0.0.1:9000 \
  bazel test //dataplane/ddl:ddl_test --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=CH_ADDR --test_output=errors
```
Expected: PASS (all six tests). Also `bazel test //dataplane/ddl:ddl_test` without env: PASS with the docker tests skipped.

- [ ] **Step 7: Commit** — `git add dataplane/ddl scripts/ci .github README.md && git commit -m "feat(ddl): EnsureProtocolTables + VerifyProtocolTable; keeper-enabled ClickHouse in CI"`

---

### Task 4: arbiter-core `snode` — run `EnsureProtocolTables` in `Register`, reconcile in `Run`

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` (same branch).

**Files:**
- Modify: `snode/config.go` (new fields + defaults), `snode/snode.go` (`Register`, `Run`, `ensureProtocolTables`, `pinned`, `reconcileProtocolTables`)
- Create: `snode/protocol_tables_test.go`

**Interfaces:**
- Consumes: Task 3 `ddl.EnsureProtocolTables`, `ddl.Mode`, `ddl.DefaultReconcileInterval`, `ddl.Pinned`, `ddl.ErrProtocolTableDrift`.
- Produces (used by Tasks 8, 20): `snode.Config.ProtocolTables ddl.Mode` (zero value `ddl.ModeOff` keeps existing harnesses that pre-create MergeTree tables working — production wiring always sets it), `snode.Config.ProtocolTablesReconcile time.Duration` (0 → `ddl.DefaultReconcileInterval`), `snode.Config.KeeperShardID uint32` (0 in v1). `Register(ctx)` now ensures tables before `RegisterNode`; `Run(ctx)` starts the reconcile goroutine against the frozen `cfg.Tables` snapshot (drift detection only, never dynamic schema discovery).

- [ ] **Step 1: Write the failing test** `snode/protocol_tables_test.go`

```go
package snode

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

func requireKeeperS(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but no Keeper: %v", err)
		}
		t.Skipf("no Keeper configured: %v", err)
	}
}

func TestRegister_EnsuresProtocolTablesThenFailsClosedOnDrift(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	server := &snodeFakeServer{}
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)

	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	schema := intakeSchema()
	schema.TableID = "db.t_" + suffix
	cfg := testConfigS(t)
	cfg.NodeID = "snode-" + suffix
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	setUniqueDatabases(t, &cfg)
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	role, err := New(cfg, Deps{Client: client, Conn: conn})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}

	if err := role.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	table := CHTableName(schema.TableID)
	var engine string
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", role.cfg.UnsafeDatabase, table).Scan(&engine); err != nil || engine != "ReplicatedMergeTree" {
		t.Fatalf("hg_unsafe engine = %q err=%v", engine, err)
	}
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", role.cfg.SafeDatabase, table).Scan(&engine); err != nil || engine != "MergeTree" {
		t.Fatalf("hg_safe engine = %q err=%v", engine, err)
	}
	if regs, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("registration must still happen after ensure: %+v", regs)
	}

	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1", role.cfg.SafeDatabase, table)); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	err = role.Register(ctx)
	if !errors.Is(err, ddl.ErrProtocolTableDrift) {
		t.Fatalf("drift must fail closed before re-registration, got %v", err)
	}
	if regs, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("drifted role must not register again: %+v", regs)
	}
}

func TestRegister_ProtocolTablesModeRequiresConn(t *testing.T) {
	cfg := testConfigS(t)
	cfg.ProtocolTables = ddl.ModeVerifyOnly
	server := &snodeFakeServer{}
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	role, err := New(cfg, Deps{Client: client})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}
	if err := role.Register(context.Background()); err == nil {
		t.Fatal("ensure mode without a ClickHouse connection must fail")
	}
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./snode/ -run 'TestRegister_ProtocolTables|TestRegister_Ensures'` Expected: FAIL (`cfg.ProtocolTables undefined`).

- [ ] **Step 3: Implement.** `snode/config.go`: add to `Config`

```go
	// ProtocolTables selects whether Register creates/verifies the pinned
	// hg_unsafe/hg_safe DDL (spec C §4). ddl.ModeOff (zero value) leaves DDL to
	// the host; production wiring passes ddl.ModeCreateAndVerify for
	// network_state/chain schema sources and ddl.ModeVerifyOnly for clickhouse.
	ProtocolTables ddl.Mode
	// ProtocolTablesReconcile is the periodic re-run cadence (0 = 60s).
	ProtocolTablesReconcile time.Duration
	// KeeperShardID feeds the anchored zk path /sentio/<shard>/unsafe/<t>; 0 in v1.
	KeeperShardID uint32
	// HardPartsPerPartition is added by Task 6.
```

(imports: `"time"`, `"github.com/sentioxyz/arbiter-core/dataplane/ddl"`). In `validate()` add `if c.ProtocolTablesReconcile == 0 { c.ProtocolTablesReconcile = ddl.DefaultReconcileInterval }` next to the database defaults.

`snode/snode.go`:

```go
func (r *Role) pinned() ddl.Pinned {
	return ddl.Pinned{
		UnsafeDB: r.cfg.UnsafeDatabase, SafeDB: r.cfg.SafeDatabase, PromoteDB: r.cfg.PromoteDatabase,
		NodeID: r.cfg.NodeID, KeeperShardID: r.cfg.KeeperShardID,
	}
}

func (r *Role) ensureProtocolTables(ctx context.Context) error {
	if r.cfg.ProtocolTables == ddl.ModeOff {
		return nil
	}
	if r.d.Conn == nil {
		return errors.New("snode: clickhouse connection is required to ensure protocol tables")
	}
	if err := ddl.EnsureProtocolTables(ctx, r.d.Conn, r.pinned(), r.cfg.Tables, r.cfg.ProtocolTables, r.d.Logger); err != nil {
		return fmt.Errorf("snode: ensure protocol tables: %w", err)
	}
	return nil
}

func (r *Role) reconcileProtocolTables(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.ProtocolTablesReconcile)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.ensureProtocolTables(ctx); err != nil {
				r.d.Logger.Error("protocol table reconcile failed", "err", err)
			}
		}
	}
}
```

The `r.cfg.Tables` argument is deliberate: it is the schema-root-validated startup snapshot used by the rest of the role. Do not re-read a mutable schema source in this ticker and thereby mix schema snapshots; a newly declared table is admitted after a controlled role restart in v1.

Modify `Register`: first statement `if err := r.ensureProtocolTables(ctx); err != nil { return err }`. Modify `Run`: after `convergeStartup` succeeds, `if r.cfg.ProtocolTables != ddl.ModeOff { go r.reconcileProtocolTables(ctx) }`. Add `"errors"` to imports.

- [ ] **Step 4: Run** — `bazel run //:gazelle && go test ./snode/... && ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 CH_ADDR=127.0.0.1:9000 bazel test //snode:snode_test --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=CH_ADDR --test_output=errors` Expected: PASS (existing harness tests still pre-create MergeTree tables under `ModeOff`).

- [ ] **Step 5: Commit** — `git add snode && git commit -m "feat(snode): ensure protocol tables in Register and reconcile every 60s"`

---

### Task 5: arbiter-core `verifier` — same wiring + `ddl.CHTableName`

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` (same branch).

**Files:**
- Modify: `verifier/config.go`, `verifier/verifier.go`, `verifier/backends.go` (drop private `chTableName`, closes roadmap §5 bounded task)
- Create: `verifier/protocol_tables_test.go`

**Interfaces:**
- Produces (used by Tasks 8, 20): `verifier.Config.SafeDatabase`, `verifier.Config.PromoteDatabase` (defaults `hg_safe` / `hg_promote`), `verifier.Config.ProtocolTables ddl.Mode`, `verifier.Config.ProtocolTablesReconcile time.Duration`, `verifier.Config.KeeperShardID uint32`; `verifier.Deps.Conn clickhouse.Conn` (required when `ProtocolTables != ddl.ModeOff`, validated in `New`). As in SNode, reconcile uses the schema-root-validated startup `cfg.Tables` slice and only detects drift.

- [ ] **Step 1: Write the failing test** `verifier/protocol_tables_test.go`

```go
package verifier

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

func TestNew_ProtocolTablesModeRequiresConn(t *testing.T) {
	cfg := testConfigV()
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	defer client.Close()
	if _, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}}); err == nil {
		t.Fatal("ensure mode without a ClickHouse connection must fail at construction")
	}
}

func TestRegister_EnsuresProtocolTablesOnVerifier(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but no Keeper: %v", err)
		}
		t.Skipf("no Keeper configured: %v", err)
	}
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	server := newVerifierFakeServer()
	addr := startVerifierFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	cfg := testConfigV()
	cfg.ReplicaID = "verifier-" + suffix
	sch := scanTableSchema()
	sch.TableID = "db.t_" + suffix
	cfg.Tables = []payloadexec.TableSchema{sch}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.UnsafeDatabase = "hg_unsafe_" + suffix
	cfg.SafeDatabase = "hg_safe_" + suffix
	cfg.PromoteDatabase = "hg_promote_" + suffix
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	role, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}, Conn: conn})
	if err != nil {
		t.Fatalf("new role: %v", err)
	}
	if err := role.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	var replica string
	if err := conn.QueryRow(ctx, "SELECT replica_name FROM system.replicas WHERE database = ? AND table = ?", cfg.UnsafeDatabase, ddl.CHTableName(sch.TableID)).Scan(&replica); err != nil || replica != cfg.ReplicaID {
		t.Fatalf("replica_name = %q err=%v want %q", replica, err, cfg.ReplicaID)
	}
	if regs, _, _, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("registration after ensure: %+v", regs)
	}
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./verifier/ -run 'ProtocolTables'` Expected: FAIL (`cfg.ProtocolTables undefined`).

- [ ] **Step 3: Implement.** `verifier/config.go`: add fields `SafeDatabase`, `PromoteDatabase string`, `ProtocolTables ddl.Mode`, `ProtocolTablesReconcile time.Duration`, `KeeperShardID uint32`; constants `defaultSafeDatabase = "hg_safe"`, `defaultPromoteDatabase = "hg_promote"`; in `validate()` default the two databases and `ProtocolTablesReconcile` like snode. `verifier/verifier.go`: add `Conn clickhouse.Conn` to `Deps`; in `New`, after the existing required-deps check: `if cfg.ProtocolTables != ddl.ModeOff && d.Conn == nil { return nil, fmt.Errorf("verifier: clickhouse connection is required when protocol tables are ensured") }`; add

```go
func (r *Role) ensureProtocolTables(ctx context.Context) error {
	if r.cfg.ProtocolTables == ddl.ModeOff {
		return nil
	}
	p := ddl.Pinned{
		UnsafeDB: r.cfg.UnsafeDatabase, SafeDB: r.cfg.SafeDatabase, PromoteDB: r.cfg.PromoteDatabase,
		NodeID: r.cfg.ReplicaID, KeeperShardID: r.cfg.KeeperShardID,
	}
	if err := ddl.EnsureProtocolTables(ctx, r.d.Conn, p, r.cfg.Tables, r.cfg.ProtocolTables, r.d.Logger); err != nil {
		return fmt.Errorf("verifier: ensure protocol tables: %w", err)
	}
	return nil
}
```

plus a `reconcileProtocolTables(ctx)` ticker identical in shape to snode's. `Register` calls `ensureProtocolTables` first; `Run` starts `go r.reconcileProtocolTables(ctx)` before `RunVerifierSubscription` when mode != Off. `verifier/backends.go`: `qualified := s.cfg.UnsafeDatabase + "." + ddl.CHTableName(tableID)`, delete `chTableName` and the `strings` import; import `"github.com/sentioxyz/arbiter-core/dataplane/ddl"`; the `scanner_test.go` call `chTableName("db.t")` becomes `ddl.CHTableName("db.t")`.

- [ ] **Step 4: Run** — `bazel run //:gazelle && go test ./verifier/... && ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 CH_ADDR=127.0.0.1:9000 bazel test //verifier:verifier_test --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=CH_ADDR --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add verifier && git commit -m "feat(verifier): ensure protocol tables in Register; use ddl.CHTableName"`

---

### Task 6: arbiter-core `snode` — prepare hard-limit mirror (`ErrBackpressure`)

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` (same branch).

**Files:**
- Modify: `snode/config.go` (`HardPartsPerPartition`), `snode/staged.go` (`ErrBackpressure`, check before the journal write)
- Create: `snode/staged_backpressure_test.go`

**Interfaces:**
- Consumes: existing `activeParts`, `partNamesForPartition`, `touchedPartitions` in `snode/staged.go` (the pre-write `before` inventory is already the per-partition part list — no extra query).
- Produces (used by Task 20): `snode.ErrBackpressure` sentinel (`errors.Is`-able), `snode.Config.HardPartsPerPartition int` (0 → `snode.DefaultHardPartsPerPartition = 2950`).

- [ ] **Step 1: Write the failing docker test** `snode/staged_backpressure_test.go`

```go
package snode

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func TestPrepareLocalStatement_RefusesAboveHardPartsLimitBeforeWriting(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.HardPartsPerPartition = 2
	role, claims := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)
	qualified := role.cfg.UnsafeDatabase + "." + CHTableName(schema.TableID)
	// Two single-row inserts = two active parts in partition p0 (merges stopped).
	for i := 1; i <= 2; i++ {
		mustExecIntake(t, conn, fmt.Sprintf("INSERT INTO %s VALUES (unhex('%064x'), 'p0', %d)", qualified, i, i))
	}
	if got := countActiveParts(t, conn, role, schema); got != 2 {
		t.Fatalf("seed parts = %d want 2", got)
	}

	payload := []byte("p,v\np0,3\n")
	req := stagedRequest(payload)
	_, err := role.PrepareLocalStatement(ctx, req, payload)
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}
	if got := countActiveParts(t, conn, role, schema); got != 2 {
		t.Fatalf("refused prepare must not write: parts = %d", got)
	}
	if _, ok, jerr := role.journal.load(req.Envelope.StatementID.Flat()); jerr != nil || ok {
		t.Fatalf("refused prepare must not journal: ok=%v err=%v", ok, jerr)
	}
	if claims.count() != 0 {
		t.Fatal("no claim may be registered")
	}

	// A different partition below the limit still prepares.
	other := []byte("p,v\np1,1\n")
	env := intakeEnvelope(other)
	env.StatementID = arbiter.StatementID{ClientAccount: "0xacct", ClientSeq: 2, ClientNonce: "n"}
	if _, err := role.PrepareLocalStatement(ctx, PrepareRequest{Envelope: env, PayloadEncoding: testEncoding, Revision: 54460}, other); err != nil {
		t.Fatalf("prepare into p1: %v", err)
	}
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./snode/ -run TestPrepareLocalStatement_RefusesAboveHardPartsLimit` Expected: FAIL (`ErrBackpressure`, `HardPartsPerPartition` undefined; the docker body skips without `ARBITER_CH_INTEGRATION`).

- [ ] **Step 3: Implement.** `snode/config.go`: `const DefaultHardPartsPerPartition = 2950` (spec D5: `parts_to_throw_insert - 50`), field `HardPartsPerPartition int` with doc "SNode-side mirror of the ingress back-pressure hard stop; a prepare that would land at or above this many active parts in any touched hg_unsafe partition is refused before touching ClickHouse", default applied in `validate()` when 0. `snode/staged.go`: add

```go
	// ErrBackpressure: a touched hg_unsafe partition is at or above the hard
	// parts limit; nothing was journaled or written. Hosts map it to a
	// retryable client rejection (housegate: Exception 252, prefix
	// "storage_integrity: back-pressure").
	ErrBackpressure = errors.New("snode: back-pressure: hg_unsafe partition at hard parts limit")
```

to the `var (...)` block, and in `PrepareLocalStatement` right after the `inventory` map is filled (before `rec = intakeRecord{...}` / `r.journal.save`):

```go
	for _, partitionID := range touched {
		if n := len(inventory[partitionID]); n >= r.cfg.HardPartsPerPartition {
			return PreparedLocalResult{}, fmt.Errorf("%w: %s.%s partition %s has %d active parts (hard limit %d)",
				ErrBackpressure, r.cfg.UnsafeDatabase, table, partitionID, n, r.cfg.HardPartsPerPartition)
		}
	}
```

- [ ] **Step 4: Run** — `ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 bazel test //snode:snode_test --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit + PR** — `git add snode && git commit -m "feat(snode): refuse prepare above the hard parts-per-partition limit (ErrBackpressure)"`, then `git push -u origin feat/protocol-tables && gh pr create --title "feat: protocol-owned physical tables + prepare back-pressure mirror (Spec C)" --body "Tasks 1-6 of docs/superpowers/plans/2026-08-18-storage-integrity-physical-table-lifecycle.md (housegate repo). BuildDDL golden, EnsureProtocolTables/VerifyProtocolTable, snode/verifier Register wiring + reconcile, ErrBackpressure mirror, keeper-enabled CI."`. After merge, either run the **Cut Release** workflow (`gh workflow run cut-release.yml`) to get a `vX.Y.Z` tag, or use the merge commit SHA in Task 7.

---

### Task 7: arbiter — bump arbiter-core to the Task 1–6 revision

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter` (branch `feat/ensure-tables-flag` off `origin/main`).

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (all via the script)

**Interfaces:**
- Produces: `github.com/sentioxyz/arbiter-core/dataplane/ddl` resolvable as `@arbiter_core//dataplane/ddl`; `snode.Config.ProtocolTables`, `verifier.Config.ProtocolTables`, `verifier.Deps.Conn` available to Task 8.

- [ ] **Step 1: Bump** — `bash scripts/update-arbiter-core.sh <tag-or-40-char-sha-of-the-merged-Task-6-commit>` (the script resolves the pseudo-version, runs `go get` + `go mod tidy`, rewrites the `bazel_dep` version and `git_override` commit in `MODULE.bazel`, and re-syncs housegate via `scripts/update-housegate.sh`; it prints `Updated arbiter-core dependency:` with the resolved version and commit).
- [ ] **Step 2: Verify** — `go doc github.com/sentioxyz/arbiter-core/dataplane/ddl.EnsureProtocolTables` prints the signature; `bazel mod tidy && bazel build //... && bazel test //cmd/... --test_output=errors` Expected: PASS.
- [ ] **Step 3: Commit** — `git add go.mod go.sum MODULE.bazel MODULE.bazel.lock && git commit -m "chore(deps): upgrade arbiter-core for protocol-table DDL"`

---

### Task 8: arbiter reference binaries — `-ensure-tables=off|verify|create` + `-print-ddl`

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter` (same branch).

**Files:**
- Modify: `cmd/arbiter-snode/main.go`, `cmd/arbiter-snode/config.go` (`toRoleConfig`), `cmd/arbiter-snode/main_test.go`, `cmd/arbiter-verifier/main.go`, `cmd/arbiter-verifier/config.go` (`toRoleConfig`), `cmd/arbiter-verifier/main_test.go`, `README.md` (P1c section)

**Interfaces:**
- Consumes: Task 7 pins; `ddl.ParseMode`, `ddl.BuildDDL`, `ddl.Pinned`, `snode.Config.ProtocolTables`, `verifier.Config.{ProtocolTables,SafeDatabase,PromoteDatabase}`, `verifier.Deps.Conn`.
- Produces: `defaultEnsureMode(cfg Config) string` in each cmd (`"create"` when inline `tables`, `"verify"` when `table_ids`), `printDDL(ctx, cfg, w io.Writer) error`.

- [ ] **Step 1: Write the failing tests** (append to `cmd/arbiter-snode/main_test.go`; mirror in `cmd/arbiter-verifier/main_test.go` with `validVerifierConfig`/`writeVerifierConfig` and `replica_id: verifier-1`)

```go
func TestDefaultEnsureMode_FollowsSchemaSource(t *testing.T) {
	cfg, err := loadConfig(writeSNodeConfig(t, validSNodeConfig(t)))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if got := defaultEnsureMode(cfg); got != "create" {
		t.Fatalf("inline tables → %q, want create", got)
	}
	cfg.Tables = nil
	cfg.TableIDs = []string{"db.t"}
	if got := defaultEnsureMode(cfg); got != "verify" {
		t.Fatalf("table_ids → %q, want verify", got)
	}
}

func TestPrintDDL_PrintsPinnedStatements(t *testing.T) {
	cfg, err := loadConfig(writeSNodeConfig(t, validSNodeConfig(t)))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	var out bytes.Buffer
	if err := printDDL(context.Background(), cfg, &out); err != nil {
		t.Fatalf("printDDL: %v", err)
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `hg_unsafe`.`db__t`",
		"ENGINE = ReplicatedMergeTree('/sentio/0/unsafe/db__t', 'snode-1')",
		"CREATE TABLE IF NOT EXISTS `hg_safe`.`db__t`",
		"replicated_deduplication_window = 0",
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("print-ddl output missing %q:\n%s", want, out.String())
		}
	}
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./cmd/arbiter-snode/ -run 'TestDefaultEnsureMode|TestPrintDDL'` Expected: FAIL (`undefined: defaultEnsureMode`, `printDDL`).

- [ ] **Step 3: Implement (`cmd/arbiter-snode`).** `main.go`: new flags

```go
	ensureTables := flag.String("ensure-tables", "", "protocol table DDL mode: off|verify|create (default: create with inline tables, verify with table_ids)")
	printDDLFlag := flag.Bool("print-ddl", false, "print the pinned hg_unsafe/hg_safe DDL for the configured tables and exit")
```

After config load: `if *printDDLFlag { if err := printDDL(ctx0, cfg, os.Stdout); err != nil {...}; return }`. `run` gains a `mode ddl.Mode` parameter (resolved in `main` via `ddl.ParseMode(orDefault(*ensureTables, defaultEnsureMode(cfg)))`, exit 2 on parse error) and passes it: `role, err := snode.New(cfg.toRoleConfig(tables, mode), ...)`. Helpers:

```go
func defaultEnsureMode(cfg Config) string {
	if len(cfg.TableIDs) > 0 {
		return "verify" // schema derived from local ClickHouse: creating from it would be circular
	}
	return "create"
}

func printDDL(ctx context.Context, cfg Config, w io.Writer) error {
	tables := cfg.tables()
	if len(cfg.TableIDs) > 0 {
		conn, err := openClickHouse(cfg)
		if err != nil {
			return err
		}
		defer conn.Close()
		if tables, err = loadTableSchemas(ctx, cfg, conn); err != nil {
			return err
		}
	}
	p := ddl.Pinned{UnsafeDB: orDefault(cfg.UnsafeDatabase, "hg_unsafe"), SafeDB: orDefault(cfg.SafeDatabase, "hg_safe"), PromoteDB: orDefault(cfg.PromoteDatabase, "hg_promote"), NodeID: cfg.NodeID}
	for _, t := range tables {
		unsafe, safe, err := ddl.BuildDDL(p, t)
		if err != nil {
			return fmt.Errorf("table %s: %w", t.TableID, err)
		}
		if _, err := fmt.Fprintf(w, "%s;\n\n%s;\n\n", unsafe, safe); err != nil {
			return err
		}
	}
	return nil
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
```

`config.go`: `toRoleConfig(tables []payloadexec.TableSchema, mode ddl.Mode) snode.Config` sets `ProtocolTables: mode`. Existing `TestRun_NoArbiterFailsFast` passes `ddl.ModeOff` to `run` (it has no ClickHouse). Verifier cmd: identical flags/helpers with `NodeID: cfg.ReplicaID`, `verifier.Config{..., SafeDatabase: cfg.SafeDatabase, PromoteDatabase: cfg.PromoteDatabase, ProtocolTables: mode}` — add `safe_database`/`promote_database` yaml keys to the verifier `Config` — and `verifier.Deps{..., Conn: conn}`.

- [ ] **Step 4: Run** — `bazel run //:gazelle && go test ./cmd/... && bazel test //cmd/... --test_output=errors && bazel run //cmd/arbiter-snode -- -config configs/snode.local.yaml -print-ddl` (needs a reachable ClickHouse because the sample uses `table_ids`; with `configs/verifier.local.yaml` the print is offline). Expected: PASS; DDL printed.

- [ ] **Step 5: README** — in `README.md` "P1c data-plane roles": add the two flags, state that both binaries create/verify the pinned `hg_unsafe`/`hg_safe` DDL on start (`--ensure-tables=create` with inline `tables`, `verify` with `table_ids`, `off` only for harnesses that own DDL), replace the "physical tables must already exist" onboarding sentence with "print with `-print-ddl` or let the role create them", and note that `table_ids` bootstrap still needs the tables (verify-only). Also mention `docker run … -v scripts/ci/clickhouse-keeper.xml…` is required for RMT.

- [ ] **Step 6: Commit + PR** — `git add cmd README.md && git commit -m "feat(cmd): --ensure-tables and -print-ddl for snode/verifier reference binaries" && git push -u origin feat/ensure-tables-flag && gh pr create --title "feat(cmd): --ensure-tables / -print-ddl (Spec C)" --body "Bumps arbiter-core; reference binaries create/verify pinned protocol DDL."`

---

### Task 9: housegate `pkg/chproto` + `pkg/proxy` — code-carrying plugin rejection (`ClientError`)

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (branch `feat/si-backpressure` off `origin/main`).

**Files:**
- Create: `pkg/chproto/client_error.go`, `pkg/chproto/client_error_test.go`, `pkg/proxy/relay_exception_test.go`
- Modify: `pkg/proxy/relay.go:1270-1283` (`writeExceptionToClient` → uses `exceptionForPluginError`)

**Interfaces:**
- Produces (used by Task 14): `chproto.ClientError{Code int32; Message string; Err error}` with `Error()` / `Unwrap()`; `chproto.CodeTooManyParts int32 = 252`; `proxy.exceptionForPluginError(err error) *chproto.Exception` (unexported, tested in-package). Relay behavior: an error chain containing a `*chproto.ClientError` is written with that code and exactly `Message`; every other plugin error keeps today's `403` + `err.Error()`.

- [ ] **Step 1: Write the failing tests** — `pkg/chproto/client_error_test.go`

```go
package chproto

import (
	"errors"
	"fmt"
	"testing"
)

func TestClientError_UnwrapsAndFormats(t *testing.T) {
	cause := errors.New("partition p_2026 has 2400 active parts")
	ce := &ClientError{Code: CodeTooManyParts, Message: "storage_integrity: back-pressure: retry later", Err: cause}
	wrapped := fmt.Errorf("storage_integrity admission rejected for s1: %w", ce)
	var got *ClientError
	if !errors.As(wrapped, &got) || got.Code != 252 {
		t.Fatalf("errors.As failed or code=%d", got.Code)
	}
	if !errors.Is(wrapped, cause) {
		t.Fatal("ClientError must unwrap to its cause")
	}
	if ce.Error() != "storage_integrity: back-pressure: retry later: partition p_2026 has 2400 active parts" {
		t.Fatalf("Error() = %q", ce.Error())
	}
	if (&ClientError{Code: 1, Message: "m"}).Error() != "m" {
		t.Fatal("Error() without cause must be the message alone")
	}
}
```

`pkg/proxy/relay_exception_test.go`

```go
package proxy

import (
	"errors"
	"fmt"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
)

func TestExceptionForPluginError_DefaultsTo403(t *testing.T) {
	exc := exceptionForPluginError(errors.New("jws invalid"))
	if exc.Code != 403 || exc.Message != "jws invalid" || exc.Name != "DB::Exception" {
		t.Fatalf("exception = %+v", exc)
	}
}

func TestExceptionForPluginError_HonorsClientError(t *testing.T) {
	ce := &chproto.ClientError{Code: chproto.CodeTooManyParts, Message: "storage_integrity: back-pressure: hg_unsafe.db__t partition p_a has 2400 active parts (soft limit 2400); retry later", Err: errors.New("cause")}
	exc := exceptionForPluginError(fmt.Errorf("query input complete strict hook: %w", fmt.Errorf("storage_integrity admission rejected for s1: %w", ce)))
	if exc.Code != 252 {
		t.Fatalf("code = %d want 252", exc.Code)
	}
	if exc.Message != ce.Message {
		t.Fatalf("message = %q want the ClientError message verbatim", exc.Message)
	}
}
```

- [ ] **Step 2: Run to verify they fail** — `go test ./pkg/chproto/ -run ClientError; go test ./pkg/proxy/ -run ExceptionForPluginError` Expected: FAIL (`undefined: ClientError`, `exceptionForPluginError`).

- [ ] **Step 3: Implement** `pkg/chproto/client_error.go`

```go
package chproto

// CodeTooManyParts is ClickHouse error code 252 (TOO_MANY_PARTS); clients
// already treat it as retryable, which is why the storage-integrity
// back-pressure rejection reuses it (spec C D5).
const CodeTooManyParts int32 = 252

// ClientError lets a plugin choose the ClickHouse exception code and the exact
// client-facing message Relay writes when the plugin chain rejects a query.
// Errors that do not carry a ClientError keep the generic 403 plugin-reject.
// Err is the server-side cause (logged, unwrapped) and is not sent verbatim.
type ClientError struct {
	Code    int32
	Message string
	Err     error
}

func (e *ClientError) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *ClientError) Unwrap() error { return e.Err }
```

`pkg/proxy/relay.go`: replace `writeExceptionToClient` with

```go
// exceptionForPluginError maps a plugin error to the synthetic Exception the
// client sees: a *chproto.ClientError anywhere in the chain sets code + message
// (e.g. storage-integrity back-pressure → 252); anything else is the generic
// 403 plugin-reject carrying the error text.
func exceptionForPluginError(pluginErr error) *chproto.Exception {
	var ce *chproto.ClientError
	if errors.As(pluginErr, &ce) {
		return &chproto.Exception{Code: proto.Error(ce.Code), Name: "DB::Exception", Message: ce.Message}
	}
	return &chproto.Exception{
		Code:    403, // ClickHouse AUTHENTICATION_FAILED; generic plugin-reject
		Name:    "DB::Exception",
		Message: pluginErr.Error(),
	}
}

func (r *Relay) writeExceptionToClient(ctx context.Context, pluginErr error) {
	if err := r.sess.Client().WriteException(exceptionForPluginError(pluginErr)); err != nil {
		_, logger := log.FromContext(ctx)
		logger.Warne(err, "failed to write plugin-error exception")
	}
}
```

(`relay.go` already imports `errors` and `github.com/ClickHouse/ch-go/proto` — check; add if missing.) Behavior note for the plan reader: the strict end-of-input path still closes the connection after writing the exception (pre-existing; unchanged here).

- [ ] **Step 4: Run** — `bazel run //:gazelle && bazel test //pkg/chproto:chproto_test //pkg/proxy:proxy_test --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add pkg/chproto pkg/proxy && git commit -m "feat(proxy): plugins can reject with an explicit ClickHouse exception code (ClientError)"`

---

### Task 10: housegate `pkg/storageintegrity` — `PhysicalTableName` + `PayloadPartitionIDs`

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (same branch).

**Files:**
- Create: `pkg/storageintegrity/physical_table.go`, `pkg/storageintegrity/physical_table_test.go`, `pkg/storageintegrity/payload_partitions.go`, `pkg/storageintegrity/payload_partitions_test.go`

**Interfaces:**
- Consumes: `payloadexec.DecodeCSV(payload []byte, sch TableSchema) ([]Row, error)` (Row.PartitionID = `p_<raw csv token>`/`all`), `DecodeNativePayload(schema, revision, payload) ([]payloadexec.Row, error)` (Row.PartitionID from `PartitionIDForRow`), constants `EncodingCSVWithNames`, `PayloadEncodingClickHouseNativeData`.
- Produces (used by Tasks 11, 14, 20): `sicore.PhysicalTableName(tableID string) string` (D2 rule, mirror of arbiter-core `ddl.CHTableName`), `sicore.ErrPartitionFreeze`, `sicore.PayloadPartitionIDs(schema payloadexec.TableSchema, encoding string, revision int, payload []byte) ([]string, error)` (sorted, unique logical partition ids).

- [ ] **Step 1: Write the failing tests** — `pkg/storageintegrity/physical_table_test.go`

```go
package storageintegrity

import "testing"

func TestPhysicalTableName_D2Freeze(t *testing.T) {
	for in, want := range map[string]string{"db.t": "db__t", "orders.events": "orders__events", "a.b.c": "a__b__c", "plain": "plain"} {
		if got := PhysicalTableName(in); got != want {
			t.Fatalf("PhysicalTableName(%q) = %q want %q", in, got, want)
		}
	}
}
```

`pkg/storageintegrity/payload_partitions_test.go`

```go
package storageintegrity

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func partitionsSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
}

func TestPayloadPartitionIDs_CSVGroupsSortsAndDedups(t *testing.T) {
	got, err := PayloadPartitionIDs(partitionsSchema(), EncodingCSVWithNames, 0, []byte("p,v\nzeta,1\nalpha,2\nzeta,3\n"))
	if err != nil {
		t.Fatalf("PayloadPartitionIDs: %v", err)
	}
	if want := []string{"p_alpha", "p_zeta"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partitions = %v want %v", got, want)
	}
}

func TestPayloadPartitionIDs_NativeUsesDecodedRows(t *testing.T) {
	// encodeNativePayload / newColStr / nativePayloadTestRevision are the
	// existing helpers in native_payload_test.go (same package).
	payload := encodeNativePayload(t, proto.Input{
		{Name: "p", Data: newColStr("b", "a", "b")},
		{Name: "v", Data: &proto.ColUInt64{1, 2, 3}},
	})
	got, err := PayloadPartitionIDs(partitionsSchema(), PayloadEncodingClickHouseNativeData, nativePayloadTestRevision, payload)
	if err != nil {
		t.Fatalf("PayloadPartitionIDs: %v", err)
	}
	if want := []string{"p_a", "p_b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("partitions = %v want %v", got, want)
	}
}

func TestPayloadPartitionIDs_UnpartitionedIsAll(t *testing.T) {
	sch := payloadexec.TableSchema{TableID: "db.u", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	got, err := PayloadPartitionIDs(sch, EncodingCSVWithNames, 0, []byte("v\n1\n2\n"))
	if err != nil || !reflect.DeepEqual(got, []string{"all"}) {
		t.Fatalf("partitions = %v err=%v want [all]", got, err)
	}
}

func TestPayloadPartitionIDs_RejectsFreezeViolationAndUnknownEncoding(t *testing.T) {
	bad := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "n", Columns: []lthash.Column{{Name: "n", Type: "UInt64"}}}
	if _, err := PayloadPartitionIDs(bad, EncodingCSVWithNames, 0, []byte("n\n1\n")); !errors.Is(err, ErrPartitionFreeze) {
		t.Fatalf("err = %v want ErrPartitionFreeze", err)
	}
	if _, err := PayloadPartitionIDs(partitionsSchema(), "future-encoding-v2", 0, []byte("x")); err == nil {
		t.Fatal("unknown encoding must error")
	}
}
```

(The test file imports `"github.com/ClickHouse/ch-go/proto"` for `proto.Input` / `proto.ColUInt64`, already a test dep of the package.)

- [ ] **Step 2: Run to verify they fail** — `go test ./pkg/storageintegrity/ -run 'PhysicalTableName|PayloadPartitionIDs'` Expected: FAIL (`undefined: PhysicalTableName`, `PayloadPartitionIDs`).

- [ ] **Step 3: Implement** — `pkg/storageintegrity/physical_table.go`

```go
package storageintegrity

import "strings"

// PhysicalTableName maps a logical storage-integrity table id
// (<database>.<table>) to its physical hg_unsafe/hg_safe table name. It is the
// D2 naming freeze; arbiter-core's ddl.CHTableName / snode.CHTableName apply
// the identical rule (sentio-node cross-checks the two at config time).
func PhysicalTableName(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}
```

`pkg/storageintegrity/payload_partitions.go`

```go
package storageintegrity

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrPartitionFreeze mirrors arbiter-core's P1c freeze at the ingress: a
// storage-integrity table's partition_by must be empty or a declared bare
// String column. Admission of such a table is refused with a clear message.
var ErrPartitionFreeze = errors.New("storage_integrity: table violates the P1c partition freeze (partition_by must be a bare String column)")

// PayloadPartitionIDs decodes the admitted payload under the pinned schema and
// returns the sorted, unique logical partition ids it touches — the same
// "p_<value>" / "all" convention payloadexec.PartitionIDForRow, DecodeCSV and
// DecodeNativePayload already produce and that system.parts.partition maps to
// (see LogicalPartitionID). The ingress checks back-pressure per partition
// before any payload put.
func PayloadPartitionIDs(schema payloadexec.TableSchema, encoding string, revision int, payload []byte) ([]string, error) {
	if err := validatePartitionFreeze(schema); err != nil {
		return nil, err
	}
	var rows []payloadexec.Row
	var err error
	switch encoding {
	case EncodingCSVWithNames:
		rows, err = payloadexec.DecodeCSV(payload, schema)
	case PayloadEncodingClickHouseNativeData:
		rows, err = DecodeNativePayload(schema, revision, payload)
	default:
		return nil, fmt.Errorf("storage_integrity: cannot derive partitions for payload encoding %q", encoding)
	}
	if err != nil {
		return nil, fmt.Errorf("storage_integrity: decode payload for partition check: %w", err)
	}
	seen := map[string]bool{}
	out := make([]string, 0, 1)
	for _, r := range rows {
		if !seen[r.PartitionID] {
			seen[r.PartitionID] = true
			out = append(out, r.PartitionID)
		}
	}
	sort.Strings(out)
	return out, nil
}

func validatePartitionFreeze(schema payloadexec.TableSchema) error {
	if schema.PartitionBy == "" {
		return nil
	}
	if strings.ContainsAny(schema.PartitionBy, "()") {
		return fmt.Errorf("%w: table %s partition_by %q is an expression", ErrPartitionFreeze, schema.TableID, schema.PartitionBy)
	}
	for _, c := range schema.Columns {
		if c.Name == schema.PartitionBy {
			if c.Type != "String" {
				return fmt.Errorf("%w: table %s partition column %q has type %s", ErrPartitionFreeze, schema.TableID, c.Name, c.Type)
			}
			return nil
		}
	}
	return fmt.Errorf("%w: table %s partition_by %q names no declared column", ErrPartitionFreeze, schema.TableID, schema.PartitionBy)
}
```

- [ ] **Step 4: Run** — `bazel run //:gazelle && bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add pkg/storageintegrity && git commit -m "feat(storageintegrity): PhysicalTableName and per-payload partition derivation"`

---

### Task 11: housegate `pkg/storageintegrity` — `PartsPressureGuard` over `MergeConn` (fake-conn tests)

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (same branch).

**Files:**
- Create: `pkg/storageintegrity/parts_pressure.go`, `pkg/storageintegrity/parts_pressure_test.go`

**Interfaces:**
- Consumes: `MergeConn` / `MergeRows` (merge_guard.go), `quoteMergeString`.
- Produces (used by Tasks 13, 14, 15, 20):
  - `sicore.ErrBackpressure` sentinel; `sicore.BackpressureError{Database, Table, Partition string; Parts, Limit int; Kind string}` (`Kind` ∈ `"soft"`, `"hard"`, `"unavailable"`), `Error()` starts with `storage_integrity: back-pressure`, `Unwrap()` → `ErrBackpressure`.
  - `sicore.LogicalPartitionID(partitionText string) string` (`"tuple()"` → `"all"`, else `"p_"+text`).
  - `sicore.PartsKey{Database, Table, Partition string}`, `sicore.PartsSnapshot map[PartsKey]int`.
  - `sicore.PartsPressureConfig{UnsafeDatabase, SafeDatabase string; SoftPartsPerPartition, HardPartsPerPartition int}`, `sicore.DefaultSoftPartsPerPartition = 2400`, `sicore.DefaultHardPartsPerPartition = 2950`.
  - `sicore.NewPartsPressureGuard(conn MergeConn, cfg PartsPressureConfig) *PartsPressureGuard`; methods `BuildSnapshotQuery() string`, `Refresh(ctx) (PartsSnapshot, error)` (queries + stores), `Snapshot() (PartsSnapshot, bool)` (copy of last good snapshot), `Allow(table, partitionID string) error` (nil / `*BackpressureError`), `Invalidate()`, `Invalidated() <-chan struct{}`.

- [ ] **Step 1: Write the failing tests** `pkg/storageintegrity/parts_pressure_test.go`

```go
package storageintegrity

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakePartsRow struct {
	db, table, partition string
	n                    uint64
}

type fakePartsConn struct {
	rows     []fakePartsRow
	queryErr error
	queries  []string
}

func (c *fakePartsConn) Exec(context.Context, string, ...any) error { return nil }

func (c *fakePartsConn) Query(_ context.Context, q string, _ ...any) (MergeRows, error) {
	c.queries = append(c.queries, q)
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	return &fakePartsRows{rows: c.rows}, nil
}

type fakePartsRows struct {
	rows []fakePartsRow
	i    int
}

func (r *fakePartsRows) Next() bool { return r.i < len(r.rows) }
func (r *fakePartsRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	*(dest[0].(*string)) = row.db
	*(dest[1].(*string)) = row.table
	*(dest[2].(*string)) = row.partition
	*(dest[3].(*uint64)) = row.n
	return nil
}
func (r *fakePartsRows) Err() error   { return nil }
func (r *fakePartsRows) Close() error { return nil }

func pressureFixture(rows ...fakePartsRow) (*PartsPressureGuard, *fakePartsConn) {
	conn := &fakePartsConn{rows: rows}
	g := NewPartsPressureGuard(conn, PartsPressureConfig{
		UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe",
		SoftPartsPerPartition: 3, HardPartsPerPartition: 5,
	})
	return g, conn
}

func TestPartsPressureGuard_BuildSnapshotQuery(t *testing.T) {
	g, _ := pressureFixture()
	q := g.BuildSnapshotQuery()
	for _, want := range []string{"system.parts", "active", "GROUP BY database, table, partition", "'hg_unsafe'", "'hg_safe'"} {
		if !strings.Contains(q, want) {
			t.Fatalf("query %q missing %q", q, want)
		}
	}
	if strings.Contains(q, "partition_id") {
		t.Fatal("must group by partition text, not partition_id (SipHash for String keys)")
	}
}

func TestPartsPressureGuard_RefreshMapsPartitionsToLogicalIDs(t *testing.T) {
	g, _ := pressureFixture(
		fakePartsRow{"hg_unsafe", "db__t", "a", 3},
		fakePartsRow{"hg_unsafe", "db__u", "tuple()", 1},
		fakePartsRow{"hg_safe", "db__t", "a", 7},
	)
	snap, err := g.Refresh(context.Background())
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if snap[PartsKey{"hg_unsafe", "db__t", "p_a"}] != 3 || snap[PartsKey{"hg_unsafe", "db__u", "all"}] != 1 || snap[PartsKey{"hg_safe", "db__t", "p_a"}] != 7 {
		t.Fatalf("snapshot = %v", snap)
	}
	if got, ok := g.Snapshot(); !ok || len(got) != 3 {
		t.Fatalf("Snapshot() = %v %v", got, ok)
	}
}

func TestPartsPressureGuard_AllowBelowAtAboveSoftAndHard(t *testing.T) {
	g, _ := pressureFixture(
		fakePartsRow{"hg_unsafe", "db__t", "below", 2},
		fakePartsRow{"hg_unsafe", "db__t", "at_soft", 3},
		fakePartsRow{"hg_unsafe", "db__t", "above_soft", 4},
		fakePartsRow{"hg_unsafe", "db__t", "at_hard", 5},
	)
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if err := g.Allow("db__t", "p_below"); err != nil {
		t.Fatalf("below soft must be allowed: %v", err)
	}
	if err := g.Allow("db__t", "p_never_seen"); err != nil {
		t.Fatalf("unknown partition (0 parts) must be allowed: %v", err)
	}
	for part, kind := range map[string]string{"p_at_soft": "soft", "p_above_soft": "soft", "p_at_hard": "hard"} {
		err := g.Allow("db__t", part)
		var be *BackpressureError
		if !errors.As(err, &be) || !errors.Is(err, ErrBackpressure) {
			t.Fatalf("%s: err = %v want BackpressureError", part, err)
		}
		if be.Kind != kind || be.Table != "db__t" || be.Partition != part {
			t.Fatalf("%s: %+v", part, be)
		}
		if !strings.HasPrefix(err.Error(), "storage_integrity: back-pressure") {
			t.Fatalf("message prefix: %q", err.Error())
		}
	}
	err := g.Allow("db__t", "p_at_soft")
	if !strings.Contains(err.Error(), "hg_unsafe.db__t") || !strings.Contains(err.Error(), "3 active parts") || !strings.Contains(err.Error(), "soft limit 3") {
		t.Fatalf("message must name table, count and limit: %q", err.Error())
	}
}

func TestPartsPressureGuard_AllowWithoutSnapshotIsUnavailable(t *testing.T) {
	g, _ := pressureFixture()
	err := g.Allow("db__t", "p_a")
	var be *BackpressureError
	if !errors.As(err, &be) || be.Kind != "unavailable" {
		t.Fatalf("err = %v want unavailable BackpressureError", err)
	}
}

func TestPartsPressureGuard_RefreshErrorKeepsLastGoodSnapshot(t *testing.T) {
	g, conn := pressureFixture(fakePartsRow{"hg_unsafe", "db__t", "a", 1})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	conn.queryErr = errors.New("connection reset")
	if _, err := g.Refresh(context.Background()); err == nil {
		t.Fatal("refresh error must surface")
	}
	if err := g.Allow("db__t", "p_a"); err != nil {
		t.Fatalf("last good snapshot must remain usable: %v", err)
	}
}

func TestPartsPressureGuard_InvalidateSignalsOnce(t *testing.T) {
	g, _ := pressureFixture()
	g.Invalidate()
	g.Invalidate() // coalesced, must not block
	select {
	case <-g.Invalidated():
	default:
		t.Fatal("Invalidate must signal the poller")
	}
	select {
	case <-g.Invalidated():
		t.Fatal("second signal must be coalesced")
	default:
	}
}

func TestLogicalPartitionID(t *testing.T) {
	if LogicalPartitionID("tuple()") != "all" || LogicalPartitionID("2026") != "p_2026" || LogicalPartitionID("") != "p_" {
		t.Fatal("LogicalPartitionID mapping")
	}
}
```

- [ ] **Step 2: Run to verify they fail** — `go test ./pkg/storageintegrity/ -run 'PartsPressureGuard|LogicalPartitionID'` Expected: FAIL (`undefined: NewPartsPressureGuard`).

- [ ] **Step 3: Implement** `pkg/storageintegrity/parts_pressure.go`

```go
package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultSoftPartsPerPartition = 0.8 × parts_to_throw_insert (3000).
	DefaultSoftPartsPerPartition = 2400
	// DefaultHardPartsPerPartition = parts_to_throw_insert - 50.
	DefaultHardPartsPerPartition = 2950
)

// ErrBackpressure is the sentinel every back-pressure refusal unwraps to. The
// ingress maps it to a ClickHouse Exception 252 (TOO_MANY_PARTS); it is never
// journaled as a source write.
var ErrBackpressure = errors.New("storage_integrity: back-pressure")

// BackpressureError names the refused table/partition and the limit that
// tripped: Kind "soft" (ingress soft limit), "hard" (hard stop), or
// "unavailable" (no part inventory yet — fail closed but retryable).
type BackpressureError struct {
	Database  string
	Table     string
	Partition string
	Parts     int
	Limit     int
	Kind      string
}

func (e *BackpressureError) Error() string {
	if e.Kind == "unavailable" {
		return fmt.Sprintf("storage_integrity: back-pressure: part inventory unavailable for %s.%s; retry later", e.Database, e.Table)
	}
	return fmt.Sprintf("storage_integrity: back-pressure: %s.%s partition %s has %d active parts (%s limit %d); retry later",
		e.Database, e.Table, e.Partition, e.Parts, e.Kind, e.Limit)
}

func (e *BackpressureError) Unwrap() error { return ErrBackpressure }

// LogicalPartitionID maps system.parts.partition (text) to the logical
// partition id used everywhere else in the storage-integrity code
// (payloadexec.PartitionIDForRow, arbiter-core snode.logicalPartitionID):
// "tuple()" (unpartitioned) → "all", otherwise "p_" + value. For a bare
// String key the text is the raw value, so this is exact.
func LogicalPartitionID(partitionText string) string {
	if partitionText == "tuple()" {
		return "all"
	}
	return "p_" + partitionText
}

// PartsKey identifies one (database, physical table, logical partition).
type PartsKey struct {
	Database  string
	Table     string
	Partition string
}

// PartsSnapshot is the active-part count per key at one poll.
type PartsSnapshot map[PartsKey]int

// PartsPressureConfig pins the guarded databases and per-partition limits.
type PartsPressureConfig struct {
	UnsafeDatabase        string
	SafeDatabase          string
	SoftPartsPerPartition int
	HardPartsPerPartition int
}

// PartsPressureGuard is the ingress-side back-pressure source of truth (spec C
// §5): it snapshots the co-located ClickHouse's system.parts for hg_unsafe
// (admission decisions) and hg_safe (growth signal), and answers Allow from
// the last good snapshot. Polling and metrics are the host's job
// (housegate root StorageIntegrityPartsPressureSupervisor).
type PartsPressureGuard struct {
	conn MergeConn
	cfg  PartsPressureConfig

	mu       sync.RWMutex
	snap     PartsSnapshot
	takenAt  time.Time
	haveSnap bool

	invalidated chan struct{}
}

func NewPartsPressureGuard(conn MergeConn, cfg PartsPressureConfig) *PartsPressureGuard {
	if cfg.SoftPartsPerPartition <= 0 {
		cfg.SoftPartsPerPartition = DefaultSoftPartsPerPartition
	}
	if cfg.HardPartsPerPartition <= 0 {
		cfg.HardPartsPerPartition = DefaultHardPartsPerPartition
	}
	return &PartsPressureGuard{conn: conn, cfg: cfg, invalidated: make(chan struct{}, 1)}
}

// BuildSnapshotQuery groups active parts by (database, table, partition
// text). It deliberately does not use partition_id: for a String partition
// key that is a SipHash that no row-side derivation can reproduce.
func (g *PartsPressureGuard) BuildSnapshotQuery() string {
	dbs := []string{quoteMergeString(g.cfg.UnsafeDatabase)}
	if g.cfg.SafeDatabase != "" {
		dbs = append(dbs, quoteMergeString(g.cfg.SafeDatabase))
	}
	return "SELECT database, table, partition, count() FROM system.parts WHERE database IN (" +
		strings.Join(dbs, ", ") + ") AND active GROUP BY database, table, partition"
}

// Refresh polls system.parts and replaces the cached snapshot on success. On
// error the last good snapshot is kept (the merge-health latch already fails
// ingress closed when ClickHouse is unreachable).
func (g *PartsPressureGuard) Refresh(ctx context.Context) (PartsSnapshot, error) {
	rows, err := g.conn.Query(ctx, g.BuildSnapshotQuery())
	if err != nil {
		return nil, fmt.Errorf("storage_integrity: parts snapshot query failed: %w", err)
	}
	defer rows.Close()
	snap := PartsSnapshot{}
	for rows.Next() {
		var db, table, partition string
		var n uint64
		if err := rows.Scan(&db, &table, &partition, &n); err != nil {
			return nil, fmt.Errorf("storage_integrity: scan parts snapshot: %w", err)
		}
		snap[PartsKey{Database: db, Table: table, Partition: LogicalPartitionID(partition)}] = int(n)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage_integrity: read parts snapshot: %w", err)
	}
	g.mu.Lock()
	g.snap = snap
	g.takenAt = time.Now()
	g.haveSnap = true
	g.mu.Unlock()
	return g.copySnapshot(), nil
}

// Snapshot returns a copy of the last good snapshot and whether one exists.
func (g *PartsPressureGuard) Snapshot() (PartsSnapshot, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.haveSnap {
		return nil, false
	}
	out := make(PartsSnapshot, len(g.snap))
	for k, v := range g.snap {
		out[k] = v
	}
	return out, true
}

func (g *PartsPressureGuard) copySnapshot() PartsSnapshot {
	s, _ := g.Snapshot()
	return s
}

// Allow decides admission for one (physical table, logical partition) of the
// unsafe database: nil below the soft limit; *BackpressureError at/above soft
// ("soft"), at/above hard ("hard"), or when no snapshot exists ("unavailable").
func (g *PartsPressureGuard) Allow(table, partitionID string) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.haveSnap {
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Kind: "unavailable"}
	}
	n := g.snap[PartsKey{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID}]
	switch {
	case n >= g.cfg.HardPartsPerPartition:
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Parts: n, Limit: g.cfg.HardPartsPerPartition, Kind: "hard"}
	case n >= g.cfg.SoftPartsPerPartition:
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Parts: n, Limit: g.cfg.SoftPartsPerPartition, Kind: "soft"}
	}
	return nil
}

// Invalidate asks the poller for a prompt refresh (cheap, coalesced); the
// ingress calls it after every ACK2 so a burst tightens quickly.
func (g *PartsPressureGuard) Invalidate() {
	select {
	case g.invalidated <- struct{}{}:
	default:
	}
}

// Invalidated is the poller-side signal channel.
func (g *PartsPressureGuard) Invalidated() <-chan struct{} { return g.invalidated }
```

- [ ] **Step 4: Run** — `bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add pkg/storageintegrity && git commit -m "feat(storageintegrity): PartsPressureGuard over system.parts with soft/hard per-partition limits"`

---

### Task 12: housegate `pkg/config` — `storage_integrity.runtime.backpressure`

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (same branch).

**Files:**
- Modify: `pkg/config/storage_integrity_config.go` (struct, defaults, validate), `pkg/config/storage_integrity_config_test.go`

**Interfaces:**
- Produces (used by Tasks 14, 20): `config.StorageIntegrityRuntimeConfig.Backpressure StorageIntegrityRuntimeBackpressureConfig` with fields `Enabled bool` (default true), `UnsafeDatabase string` (default `"hg_unsafe"`), `SafeDatabase string` (default `"hg_safe"`), `PollInterval Duration` (default 2s), `SoftPartsPerPartition int` (2400), `HardPartsPerPartition int` (2950); yaml/json keys `enabled`, `unsafe_database`, `safe_database`, `poll_interval`, `soft_parts_per_partition`, `hard_parts_per_partition`.

- [ ] **Step 1: Write the failing tests** (append to `pkg/config/storage_integrity_config_test.go`)

```go
func TestStorageIntegrityBackpressureDefaults(t *testing.T) {
	bp := Default().StorageIntegrity.Runtime.Backpressure
	if !bp.Enabled || bp.UnsafeDatabase != "hg_unsafe" || bp.SafeDatabase != "hg_safe" {
		t.Fatalf("defaults = %+v", bp)
	}
	if bp.PollInterval.Duration != 2*time.Second || bp.SoftPartsPerPartition != 2400 || bp.HardPartsPerPartition != 2950 {
		t.Fatalf("defaults = %+v", bp)
	}
}

func TestConfigValidateStorageIntegrityBackpressure(t *testing.T) {
	valid := func(t *testing.T) *Config {
		cfg := minimalServerConfig(t)
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
		cfg.StorageIntegrity.Ingress.MaxTokenAge.Duration = time.Minute
		cfg.StorageIntegrity.Ingress.RequestTimeout.Duration = 5 * time.Second
		cfg.StorageIntegrity.Ingress.MaxPayloadBytes = defaultStorageIntegrityMaxPayloadBytes
		cfg.StorageIntegrity.Runtime.Enabled = true
		cfg.StorageIntegrity.Runtime.ExpectedSource = "snode-A"
		cfg.StorageIntegrity.Runtime.JournalDir = "/var/lib/hg/journal"
		cfg.StorageIntegrity.Runtime.PayloadSpoolDir = "/var/lib/hg/spool"
		cfg.StorageIntegrity.Runtime.MergeGuard.Tables = []StorageIntegrityRuntimeMergeTableConfig{{Database: "hg_unsafe", Table: "db__t"}}
		return cfg
	}
	if err := valid(t).Validate(); err != nil {
		t.Fatalf("valid runtime config: %v", err)
	}
	cases := map[string]struct {
		mutate func(*Config)
		want   string
	}{
		"soft must be positive": {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.SoftPartsPerPartition = 0 }, "backpressure.soft_parts_per_partition"},
		"hard must exceed soft": {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.HardPartsPerPartition = 2400 }, "backpressure.hard_parts_per_partition"},
		"hard below pinned throw": {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.HardPartsPerPartition = 3000 }, "backpressure.hard_parts_per_partition"},
		"poll interval positive": {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.PollInterval.Duration = 0 }, "backpressure.poll_interval"},
		"unsafe database required": {func(c *Config) { c.StorageIntegrity.Runtime.Backpressure.UnsafeDatabase = " " }, "backpressure.unsafe_database"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := valid(t)
			tc.mutate(cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate err = %v, want %q", err, tc.want)
			}
		})
	}
	t.Run("disabled skips limit validation", func(t *testing.T) {
		cfg := valid(t)
		cfg.StorageIntegrity.Runtime.Backpressure.Enabled = false
		cfg.StorageIntegrity.Runtime.Backpressure.SoftPartsPerPartition = 0
		if err := cfg.Validate(); err != nil {
			t.Fatalf("disabled backpressure must not validate limits: %v", err)
		}
	})
}
```

- [ ] **Step 2: Run to verify it fails** — `go test ./pkg/config/ -run 'Backpressure'` Expected: FAIL (`Backpressure` undefined).

- [ ] **Step 3: Implement.** In `storage_integrity_config.go` add to `StorageIntegrityRuntimeConfig`: `Backpressure StorageIntegrityRuntimeBackpressureConfig \`json:"backpressure" yaml:"backpressure"\``, and

```go
// StorageIntegrityRuntimeBackpressureConfig governs the ingress part-count
// throttle (spec C §5 / D5): the runtime polls system.parts of the co-located
// ClickHouse every poll_interval and refuses SI INSERTs into an hg_unsafe
// partition at or above soft_parts_per_partition with a retryable ClickHouse
// exception 252 (TOO_MANY_PARTS). hard_parts_per_partition is the SNode
// prepare mirror; both must stay below the pinned parts_to_throw_insert = 3000.
type StorageIntegrityRuntimeBackpressureConfig struct {
	Enabled               bool     `json:"enabled"                  yaml:"enabled"`
	UnsafeDatabase        string   `json:"unsafe_database"          yaml:"unsafe_database"`
	SafeDatabase          string   `json:"safe_database"            yaml:"safe_database"`
	PollInterval          Duration `json:"poll_interval"            yaml:"poll_interval"`
	SoftPartsPerPartition int      `json:"soft_parts_per_partition" yaml:"soft_parts_per_partition"`
	HardPartsPerPartition int      `json:"hard_parts_per_partition" yaml:"hard_parts_per_partition"`
}

const pinnedPartsToThrowInsert = 3000
```

Defaults in `defaultStorageIntegrityConfig()` → `Backpressure: StorageIntegrityRuntimeBackpressureConfig{Enabled: true, UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe", PollInterval: Duration{Duration: 2 * time.Second}, SoftPartsPerPartition: 2400, HardPartsPerPartition: 2950}`. In `validate` inside the `if c.Runtime.Enabled {` block:

```go
		if bp := c.Runtime.Backpressure; bp.Enabled {
			if strings.TrimSpace(bp.UnsafeDatabase) == "" {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.unsafe_database is required when backpressure is enabled"))
			}
			if bp.PollInterval.Duration <= 0 {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.poll_interval must be > 0"))
			}
			if bp.SoftPartsPerPartition <= 0 {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.soft_parts_per_partition must be > 0"))
			}
			if bp.HardPartsPerPartition <= bp.SoftPartsPerPartition || bp.HardPartsPerPartition >= pinnedPartsToThrowInsert {
				errs = append(errs, fmt.Errorf("storage_integrity.runtime.backpressure.hard_parts_per_partition must be > soft_parts_per_partition and < the pinned parts_to_throw_insert (%d)", pinnedPartsToThrowInsert))
			}
		}
```

- [ ] **Step 4: Run** — `bazel test //pkg/config:config_test --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add pkg/config && git commit -m "feat(config): storage_integrity.runtime.backpressure block"`

---

### Task 13: housegate root — parts-pressure supervisor + Prometheus metrics

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (same branch).

**Files:**
- Create: `storage_integrity_backpressure.go`, `storage_integrity_backpressure_test.go`

**Interfaces:**
- Consumes: Task 11 `sicore.PartsPressureGuard` (`Refresh`, `Snapshot`, `Allow`, `Invalidate`, `Invalidated`), `sicore.PartsKey`.
- Produces (used by Task 14):
  - `housegate.StorageIntegrityPartsPressure` interface `{ Allow(table, partitionID string) error; Invalidate() }` (satisfied by `*sicore.PartsPressureGuard` and by the supervisor).
  - `housegate.StorageIntegrityPartsPressureSupervisor` — `NewStorageIntegrityPartsPressureSupervisor(guard *sicore.PartsPressureGuard, interval time.Duration, unsafeDB, safeDB string) *StorageIntegrityPartsPressureSupervisor`; methods `Refresh(ctx) error` (poll + gauges), `Run(ctx)` (ticker + invalidation channel), `Allow`, `Invalidate` (delegate).
  - Prometheus (init()-registered globals, names verbatim from spec D6/§6): `storage_integrity_unsafe_parts{table,partition}` gauge, `storage_integrity_safe_parts{table,partition}` gauge, `storage_integrity_backpressure_total{table}` counter; exported helper `storageIntegrityBackpressureTotal.WithLabelValues(table).Inc()` used by the ingress (same package).

- [ ] **Step 1: Write the failing tests** `storage_integrity_backpressure_test.go`

```go
package housegate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type rootPartsRow struct {
	db, table, partition string
	n                    uint64
}

type rootPartsConn struct {
	mu      sync.Mutex
	rows    []rootPartsRow
	queries atomic.Int64
	err     error
}

func (c *rootPartsConn) setRows(rows []rootPartsRow) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rows = rows
}

func (c *rootPartsConn) Exec(context.Context, string, ...any) error { return nil }
func (c *rootPartsConn) Query(context.Context, string, ...any) (sicore.MergeRows, error) {
	c.queries.Add(1)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	return &rootPartsRows{rows: c.rows}, nil
}

type rootPartsRows struct {
	rows []rootPartsRow
	i    int
}

func (r *rootPartsRows) Next() bool { return r.i < len(r.rows) }
func (r *rootPartsRows) Scan(dest ...any) error {
	row := r.rows[r.i]
	r.i++
	*(dest[0].(*string)) = row.db
	*(dest[1].(*string)) = row.table
	*(dest[2].(*string)) = row.partition
	*(dest[3].(*uint64)) = row.n
	return nil
}
func (r *rootPartsRows) Err() error   { return nil }
func (r *rootPartsRows) Close() error { return nil }

func TestPartsPressureSupervisor_RefreshSetsGauges(t *testing.T) {
	conn := &rootPartsConn{rows: []rootPartsRow{
		{"hg_unsafe", "db__t", "a", 5},
		{"hg_safe", "db__t", "a", 9},
	}}
	guard := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe", SoftPartsPerPartition: 3, HardPartsPerPartition: 4})
	sup := NewStorageIntegrityPartsPressureSupervisor(guard, time.Second, "hg_unsafe", "hg_safe")
	if err := sup.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := testutil.ToFloat64(storageIntegrityUnsafeParts.WithLabelValues("db__t", "p_a")); got != 5 {
		t.Fatalf("unsafe gauge = %v want 5", got)
	}
	if got := testutil.ToFloat64(storageIntegritySafeParts.WithLabelValues("db__t", "p_a")); got != 9 {
		t.Fatalf("safe gauge = %v want 9", got)
	}
	if err := sup.Allow("db__t", "p_a"); !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("supervisor must delegate Allow: %v", err)
	}
	// A partition that disappears (cleaned) must not keep a stale gauge.
	conn.setRows([]rootPartsRow{{"hg_safe", "db__t", "a", 9}})
	if err := sup.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if got := testutil.ToFloat64(storageIntegrityUnsafeParts.WithLabelValues("db__t", "p_a")); got != 0 {
		t.Fatalf("stale unsafe gauge = %v want 0 after reset", got)
	}
}

func TestPartsPressureSupervisor_RunRefreshesOnTickAndInvalidate(t *testing.T) {
	conn := &rootPartsConn{}
	guard := sicore.NewPartsPressureGuard(conn, sicore.PartsPressureConfig{UnsafeDatabase: "hg_unsafe"})
	sup := NewStorageIntegrityPartsPressureSupervisor(guard, 20*time.Millisecond, "hg_unsafe", "")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go sup.Run(ctx)
	deadline := time.Now().Add(2 * time.Second)
	for conn.queries.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if conn.queries.Load() < 2 {
		t.Fatalf("ticker refresh count = %d, want >= 2", conn.queries.Load())
	}
	before := conn.queries.Load()
	sup.Invalidate()
	deadline = time.Now().Add(2 * time.Second)
	for conn.queries.Load() == before && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if conn.queries.Load() == before {
		t.Fatal("Invalidate must trigger a prompt refresh")
	}
}

func TestStorageIntegrityBackpressureMetricsRegisteredOnce(t *testing.T) {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	found := map[string]bool{}
	for _, mf := range mfs {
		found[mf.GetName()] = true
	}
	for _, name := range []string{"storage_integrity_unsafe_parts", "storage_integrity_safe_parts", "storage_integrity_backpressure_total"} {
		if !found[name] {
			// Gauges/counters without observed label sets are omitted from Gather;
			// touching a label set makes them visible.
			t.Fatalf("metric %s not registered on the default registry (touch a label set first if this is a fresh process)", name)
		}
	}
}
```

(In `TestStorageIntegrityBackpressureMetricsRegisteredOnce`, touch `storageIntegrityUnsafeParts.WithLabelValues("t", "p").Set(0)`, `storageIntegritySafeParts.WithLabelValues("t", "p").Set(0)`, `storageIntegrityBackpressureTotal.WithLabelValues("t").Add(0)` at the top so all three families gather; a duplicate `MustRegister` would panic at package init, which is the "registered once" property.)

- [ ] **Step 2: Run to verify it fails** — `go test . -run 'PartsPressureSupervisor|BackpressureMetrics'` Expected: FAIL (`undefined: NewStorageIntegrityPartsPressureSupervisor`).

- [ ] **Step 3: Implement** `storage_integrity_backpressure.go`

```go
package housegate

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/log"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

var (
	storageIntegrityUnsafeParts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "storage_integrity_unsafe_parts",
		Help: "Active parts per hg_unsafe table and logical partition (back-pressure input; cleanup is the only drain).",
	}, []string{"table", "partition"})
	storageIntegritySafeParts = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "storage_integrity_safe_parts",
		Help: "Active parts per hg_safe table and logical partition (grows by candidate parts per promotion; P4 compaction trigger).",
	}, []string{"table", "partition"})
	storageIntegrityBackpressureTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "storage_integrity_backpressure_total",
		Help: "Storage-integrity INSERT admissions refused with back-pressure (ClickHouse exception 252).",
	}, []string{"table"})
)

func init() {
	prometheus.MustRegister(storageIntegrityUnsafeParts)
	prometheus.MustRegister(storageIntegritySafeParts)
	prometheus.MustRegister(storageIntegrityBackpressureTotal)
}

// StorageIntegrityPartsPressure is the ingress-facing admission port; the
// supervisor and *sicore.PartsPressureGuard both satisfy it.
type StorageIntegrityPartsPressure interface {
	Allow(table, partitionID string) error
	Invalidate()
}

// StorageIntegrityPartsPressureSupervisor polls the guard on an interval (and
// promptly on Invalidate) and mirrors each snapshot into the parts gauges. It
// mirrors StorageIntegrityMergeSupervisor: startup Refresh is fail-fast, later
// poll errors are logged and the last good snapshot stays in force.
type StorageIntegrityPartsPressureSupervisor struct {
	guard    *sicore.PartsPressureGuard
	interval time.Duration
	unsafeDB string
	safeDB   string
}

func NewStorageIntegrityPartsPressureSupervisor(guard *sicore.PartsPressureGuard, interval time.Duration, unsafeDB, safeDB string) *StorageIntegrityPartsPressureSupervisor {
	if interval <= 0 {
		interval = 2 * time.Second
	}
	return &StorageIntegrityPartsPressureSupervisor{guard: guard, interval: interval, unsafeDB: unsafeDB, safeDB: safeDB}
}

func (s *StorageIntegrityPartsPressureSupervisor) Refresh(ctx context.Context) error {
	if s == nil || s.guard == nil {
		return errors.New("storage_integrity: parts pressure guard is required")
	}
	snap, err := s.guard.Refresh(ctx)
	if err != nil {
		return err
	}
	storageIntegrityUnsafeParts.Reset()
	storageIntegritySafeParts.Reset()
	for k, n := range snap {
		switch k.Database {
		case s.unsafeDB:
			storageIntegrityUnsafeParts.WithLabelValues(k.Table, k.Partition).Set(float64(n))
		case s.safeDB:
			storageIntegritySafeParts.WithLabelValues(k.Table, k.Partition).Set(float64(n))
		}
	}
	return nil
}

func (s *StorageIntegrityPartsPressureSupervisor) Run(ctx context.Context) {
	if s == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.guard.Invalidated():
		}
		if err := s.Refresh(ctx); err != nil && ctx.Err() == nil {
			log.WarnEveryN("storage_integrity.parts_pressure.refresh", 30, "storage_integrity parts snapshot failed; keeping last snapshot", "err", err)
		}
	}
}

func (s *StorageIntegrityPartsPressureSupervisor) Allow(table, partitionID string) error {
	return s.guard.Allow(table, partitionID)
}

func (s *StorageIntegrityPartsPressureSupervisor) Invalidate() { s.guard.Invalidate() }
```

(`log.WarnEveryN(id string, n int64, msg string, kv ...any)` is the existing helper in `pkg/log/log.go:476`.)

- [ ] **Step 4: Run** — `bazel run //:gazelle && bazel test //:housegate_test --test_filter='PartsPressureSupervisor|BackpressureMetrics' --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add storage_integrity_backpressure.go storage_integrity_backpressure_test.go BUILD.bazel && git commit -m "feat(storage-integrity): parts-pressure poller with unsafe/safe parts gauges and back-pressure counter"`

---

### Task 14: housegate root — wire the guard into the runtime and consult it in the ingress

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (same branch).

**Files:**
- Modify: `storage_integrity_runtime.go` (`StorageIntegrityRuntimeOptions` + `buildStorageIntegrityRuntimeConsumer` + `startStorageIntegrityRuntime`), `storage_integrity_ingress.go` (`ConsumeStorageIntegrityAdmission`, new fields, `StartBackground`), `build_test.go` (`enableStorageIntegrityRuntimeTestConfig`), `storage_integrity_ingress_test.go`
- Create: `storage_integrity_backpressure_ingress_test.go`

**Interfaces:**
- Consumes: Tasks 9–13.
- Produces (used by Task 20): `StorageIntegrityRuntimeOptions.SchemaResolver sicore.TableSchemaResolver` (required when `backpressure.enabled`), `StorageIntegrityRuntimeOptions.PartsPressure StorageIntegrityPartsPressure` (optional injection; when nil and enabled, built from `MergeConn` + config), `startStorageIntegrityRuntime` performs the first `Refresh` fail-fast. Ingress behavior: after the merge-health latch and before the payload put, for every partition of the payload `Allow(PhysicalTableName(TableID), partition)`; refusal → `storageIntegrityBackpressureTotal.Inc()` and `&chproto.ClientError{Code: chproto.CodeTooManyParts, Message: <BackpressureError text>, Err: err}`; an orchestrate error wrapping `sicore.ErrBackpressure` (SNode mirror via host adapter) → same `ClientError`; after a bound ACK2 → `Invalidate()`.

- [ ] **Step 1: Write the failing tests** `storage_integrity_backpressure_ingress_test.go`

```go
package housegate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type fakePartsPressure struct {
	refuse      map[string]error // key "<table>/<partition>"
	allowCalls  []string
	invalidated int
}

func (f *fakePartsPressure) Allow(table, partitionID string) error {
	f.allowCalls = append(f.allowCalls, table+"/"+partitionID)
	return f.refuse[table+"/"+partitionID]
}
func (f *fakePartsPressure) Invalidate() { f.invalidated++ }

func bpSchemaResolver() sicore.TableSchemaResolver {
	return sicore.TableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		if tableID != "net1.events" {
			return payloadexec.TableSchema{}, false
		}
		return payloadexec.TableSchema{TableID: "net1.events", PartitionBy: "region", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}}}, true
	})
}

func bpAdmission() siplugin.Admission {
	sql := "INSERT INTO events FORMAT CSVWithNames"
	payload := []byte("id,region\n1,eu\n2,us\n3,eu\n")
	return siplugin.Admission{
		StatementID: "0xabc:1:n1", Kind: siplugin.KindInsert, TableID: "net1.events",
		SQL: sql, SQLHash: replay.DigestString(sql), Signer: "0xabc", UserJWS: "jws",
		Payload: siplugin.CapturedPayload{Bytes: payload, Length: uint64(len(payload)), Encoding: sicore.EncodingCSVWithNames, Revision: 54465, Complete: true},
	}
}

func newBackpressureIngress(t *testing.T, pressure *fakePartsPressure) (*StorageIntegrityIngress, *rootRecordingPayloadWriter, *rootRecordingSubmitter, *rootRecordingPreparer) {
	t.Helper()
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	preparer := &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A"})
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}
	ing.WithPartsPressure(pressure, bpSchemaResolver())
	return ing, writer, submitter, preparer
}

func TestIngress_BackpressureRefusesWithException252BeforePayloadPut(t *testing.T) {
	pressure := &fakePartsPressure{refuse: map[string]error{
		"net1__events/p_us": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_us", Parts: 2400, Limit: 2400, Kind: "soft"},
	}}
	ing, writer, submitter, preparer := newBackpressureIngress(t, pressure)

	err := ing.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeTooManyParts {
		t.Fatalf("err = %v, want ClientError 252", err)
	}
	if !strings.HasPrefix(ce.Message, "storage_integrity: back-pressure") || !strings.Contains(ce.Message, "p_us") {
		t.Fatalf("client message = %q", ce.Message)
	}
	if !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatal("must unwrap to ErrBackpressure")
	}
	if writer.calls != 0 || submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("writer/submit/prepare calls = %d/%d/%d, want 0/0/0 (no payload put, no journal)", writer.calls, submitter.calls, preparer.prepareCalls)
	}
	// Both partitions in the payload are checked (sorted); the guard sees the physical table name.
	if len(pressure.allowCalls) == 0 || pressure.allowCalls[0] != "net1__events/p_eu" {
		t.Fatalf("allow calls = %v", pressure.allowCalls)
	}
}

func TestIngress_BackpressureAllowsAndInvalidatesAfterAck2(t *testing.T) {
	pressure := &fakePartsPressure{}
	ing, writer, _, _ := newBackpressureIngress(t, pressure)
	if err := ing.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("ConsumeStorageIntegrityAdmission: %v", err)
	}
	if writer.calls != 1 {
		t.Fatalf("payload writer calls = %d want 1", writer.calls)
	}
	if got := strings.Join(pressure.allowCalls, ","); got != "net1__events/p_eu,net1__events/p_us" {
		t.Fatalf("allow calls = %q", got)
	}
	if pressure.invalidated != 1 {
		t.Fatalf("invalidate after ACK2 = %d want 1", pressure.invalidated)
	}
}

func TestIngress_MapsSNodeMirrorBackpressureToException252(t *testing.T) {
	pressure := &fakePartsPressure{}
	ing, _, _, preparer := newBackpressureIngress(t, pressure)
	preparer.err = &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_eu", Parts: 2950, Limit: 2950, Kind: "hard"}
	err := ing.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != 252 || !strings.Contains(ce.Message, "hard limit 2950") {
		t.Fatalf("err = %v, want ClientError 252 from the SNode mirror", err)
	}
	if pressure.invalidated != 0 {
		t.Fatal("a refused admission must not invalidate")
	}
}

func TestIngress_UnknownSchemaOrFreezeViolationRefusedWithoutPut(t *testing.T) {
	ing, writer, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	adm := bpAdmission()
	adm.TableID = "net1.unknown"
	err := ing.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	if err == nil || !strings.Contains(err.Error(), "no pinned schema") || writer.calls != 0 {
		t.Fatalf("err = %v writer calls = %d", err, writer.calls)
	}
}
```

Also append to `build_test.go` a construction test:

```go
func TestBuildStorageIntegrityRuntimeBackpressureRequiresConnAndResolver(t *testing.T) {
	signer, err := auth.NewRelaySigner("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	cfg := minimalRouterOnlyCfg(t)
	enableStorageIntegrityRuntimeTestConfig(t, cfg, signer)
	cfg.StorageIntegrity.Runtime.Backpressure.Enabled = true
	base := StorageIntegrityRuntimeOptions{
		StatementSubmitter: &rootRecordingSubmitter{}, SourcePreparer: &rootRecordingPreparer{},
		StatusQuerier: rootRecordingStatusQuerier{}, PayloadWriter: &rootRecordingPayloadWriter{},
		MergeGuard: &recordingBuildMergeGuard{},
	}
	if _, _, err := buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, base); err == nil || !strings.Contains(err.Error(), "backpressure") {
		t.Fatalf("enabled backpressure without merge_conn/schema_resolver must fail: %v", err)
	}
	withPorts := base
	withPorts.MergeConn = &recordingBuildMergeConn{}
	withPorts.SchemaResolver = bpSchemaResolver()
	ingress, _, err := buildStorageIntegrityRuntimeConsumer(cfg.StorageIntegrity.Runtime, withPorts)
	if err != nil {
		t.Fatalf("buildStorageIntegrityRuntimeConsumer: %v", err)
	}
	if ingress.pressure == nil || ingress.pressureRunner == nil {
		t.Fatal("runtime must construct the parts-pressure guard and its supervisor")
	}
}
```

- [ ] **Step 2: Run to verify they fail** — `go test . -run 'TestIngress_|BackpressureRequires'` Expected: FAIL (`WithPartsPressure` undefined, `SchemaResolver` field missing).

- [ ] **Step 3: Implement.** `storage_integrity_ingress.go` — new fields on `StorageIntegrityIngress`: `pressure StorageIntegrityPartsPressure`, `schemas sicore.TableSchemaResolver`, `pressureRunner *StorageIntegrityPartsPressureSupervisor`; add

```go
// WithPartsPressure enables the ingress back-pressure check (spec C §5). Both
// ports are required together; the resolver supplies the pinned schema used to
// decode the payload's partitions.
func (i *StorageIntegrityIngress) WithPartsPressure(pressure StorageIntegrityPartsPressure, schemas sicore.TableSchemaResolver) {
	i.pressure = pressure
	i.schemas = schemas
}

func (i *StorageIntegrityIngress) checkPartsPressure(rec sicore.AdmissionRecord) error {
	if i.pressure == nil {
		return nil
	}
	if i.schemas == nil {
		return fmt.Errorf("storage_integrity ingress: back-pressure requires a table schema resolver")
	}
	schema, ok := i.schemas.StorageIntegrityTableSchema(rec.TableID)
	if !ok {
		return fmt.Errorf("storage_integrity ingress: no pinned schema for table %q", rec.TableID)
	}
	partitions, err := sicore.PayloadPartitionIDs(schema, rec.PayloadEncoding, rec.Revision, rec.Payload)
	if err != nil {
		return fmt.Errorf("storage_integrity ingress: %w", err)
	}
	table := sicore.PhysicalTableName(rec.TableID)
	for _, partition := range partitions {
		if err := i.pressure.Allow(table, partition); err != nil {
			return backpressureClientError(table, err)
		}
	}
	return nil
}

// backpressureClientError surfaces a back-pressure refusal as ClickHouse
// exception 252 (TOO_MANY_PARTS) so existing client retry logic works
// unchanged; the message keeps the "storage_integrity: back-pressure" prefix.
func backpressureClientError(table string, err error) error {
	storageIntegrityBackpressureTotal.WithLabelValues(table).Inc()
	var be *sicore.BackpressureError
	msg := "storage_integrity: back-pressure: retry later"
	if errors.As(err, &be) {
		msg = be.Error()
	}
	return &chproto.ClientError{Code: chproto.CodeTooManyParts, Message: msg, Err: err}
}
```

In `ConsumeStorageIntegrityAdmission`: after the merge-health block and the materializer/revision checks, before `if i.payloadWriter != nil {`, insert `if err := i.checkPartsPressure(rec); err != nil { return err }`. Around `i.orch.Orchestrate`:

```go
	res, err := i.orch.Orchestrate(ctx, rec)
	if err != nil {
		if errors.Is(err, sicore.ErrBackpressure) {
			return backpressureClientError(sicore.PhysicalTableName(rec.TableID), err)
		}
		return fmt.Errorf("storage_integrity ingress: orchestrate %s: %w", rec.StatementID, err)
	}
	if !res.Ack2 { ...unchanged... }
	if i.pressure != nil {
		i.pressure.Invalidate()
	}
	return nil
```

`StartBackground`: also `if i.pressureRunner != nil { go i.pressureRunner.Run(runCtx) }` and include `i.pressureRunner == nil` in the early-return guard. Imports: add `"errors"`, `"github.com/housegate/housegate/pkg/chproto"`.

`storage_integrity_runtime.go`: add `SchemaResolver sicore.TableSchemaResolver` and `PartsPressure StorageIntegrityPartsPressure` to `StorageIntegrityRuntimeOptions`. In `buildStorageIntegrityRuntimeConsumer`, after the ingress is built:

```go
	if bp := runtimeCfg.Backpressure; bp.Enabled {
		pressure := opts.PartsPressure
		if pressure == nil {
			if opts.MergeConn == nil {
				return nil, nil, errors.New("storage_integrity.runtime.backpressure requires merge_conn (or set storage_integrity.runtime.backpressure.enabled: false)")
			}
			guard := sicore.NewPartsPressureGuard(opts.MergeConn, sicore.PartsPressureConfig{
				UnsafeDatabase: strings.TrimSpace(bp.UnsafeDatabase), SafeDatabase: strings.TrimSpace(bp.SafeDatabase),
				SoftPartsPerPartition: bp.SoftPartsPerPartition, HardPartsPerPartition: bp.HardPartsPerPartition,
			})
			sup := NewStorageIntegrityPartsPressureSupervisor(guard, bp.PollInterval.Duration, strings.TrimSpace(bp.UnsafeDatabase), strings.TrimSpace(bp.SafeDatabase))
			ingress.pressureRunner = sup
			pressure = sup
		}
		if opts.SchemaResolver == nil {
			return nil, nil, errors.New("storage_integrity.runtime.backpressure requires a table schema resolver (StorageIntegrityRuntimeOptions.SchemaResolver)")
		}
		ingress.WithPartsPressure(pressure, opts.SchemaResolver)
	}
```

In `startStorageIntegrityRuntime`, after the merge guard assert and before `runtime.StartBackground(ctx)`: `if runtime.pressureRunner != nil { if err := runtime.pressureRunner.Refresh(ctx); err != nil { return fmt.Errorf("storage_integrity.backpressure: initial parts snapshot: %w", err) } }`.

`build_test.go::enableStorageIntegrityRuntimeTestConfig`: add `cfg.StorageIntegrity.Runtime.Backpressure.Enabled = false // existing runtime tests inject a MergeGuard without a MergeConn; back-pressure has its own tests` so the untouched tests keep passing.

- [ ] **Step 4: Run** — `bazel run //:gazelle && bazel test //:housegate_test //pkg/proxy:proxy_test --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add storage_integrity_ingress.go storage_integrity_runtime.go storage_integrity_backpressure_ingress_test.go build_test.go BUILD.bazel && git commit -m "feat(storage-integrity): ingress back-pressure (exception 252) wired between merge-health latch and payload put"`

---

### Task 15: housegate docker test — `PartsPressureGuard` against real `system.parts`

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (same branch).

**Files:**
- Create: `pkg/integration/storage_backpressure_test.go`
- Modify: `pkg/integration/BUILD.bazel` (add src + `//pkg/storageintegrity` dep — gazelle does it)

**Interfaces:**
- Consumes: `openDirectCH`, `mustExec`, `uniqueTable` (existing helpers), Task 11 guard.

- [ ] **Step 1: Write the test**

```go
package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// chMergeConn adapts clickhouse-go to the narrow sicore.MergeConn port the
// same way sentio-node's storageintegrityadapter.NewMergeConn does.
type chMergeConn struct{ conn clickhouse.Conn }

func (c chMergeConn) Exec(ctx context.Context, q string, args ...any) error { return c.conn.Exec(ctx, q, args...) }
func (c chMergeConn) Query(ctx context.Context, q string, args ...any) (sicore.MergeRows, error) {
	return c.conn.Query(ctx, q, args...)
}

func TestPartsPressureGuard_AgainstRealSystemParts(t *testing.T) {
	ctx := context.Background()
	conn := openDirectCH(t)
	unsafeDB := "hg_unsafe_bp_" + uniqueTable(t)
	safeDB := "hg_safe_bp_" + uniqueTable(t)
	table := "db__t"
	unpartitioned := "db__u"
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + unsafeDB,
		"CREATE DATABASE IF NOT EXISTS " + safeDB,
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", unsafeDB, table),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), v UInt64) ENGINE = MergeTree ORDER BY (_hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", unsafeDB, unpartitioned),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", safeDB, table),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, table),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, unpartitioned),
	} {
		mustExec(t, conn, q)
	}
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+unsafeDB)
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+safeDB)
	})
	// Three single-row inserts into p0 = three active parts; one into p1; one into the unpartitioned table.
	for i := 1; i <= 3; i++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p0', %d)", unsafeDB, table, i, i))
	}
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p1', 9)", unsafeDB, table, 9))
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 7)", unsafeDB, unpartitioned, 7))
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p0', 1)", safeDB, table, 1))

	guard := sicore.NewPartsPressureGuard(chMergeConn{conn}, sicore.PartsPressureConfig{
		UnsafeDatabase: unsafeDB, SafeDatabase: safeDB, SoftPartsPerPartition: 3, HardPartsPerPartition: 5,
	})
	snap, err := guard.Refresh(ctx)
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if snap[sicore.PartsKey{Database: unsafeDB, Table: table, Partition: "p_p0"}] != 3 ||
		snap[sicore.PartsKey{Database: unsafeDB, Table: table, Partition: "p_p1"}] != 1 ||
		snap[sicore.PartsKey{Database: unsafeDB, Table: unpartitioned, Partition: "all"}] != 1 ||
		snap[sicore.PartsKey{Database: safeDB, Table: table, Partition: "p_p0"}] != 1 {
		t.Fatalf("snapshot = %v", snap)
	}
	if err := guard.Allow(table, "p_p0"); !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatalf("p_p0 at soft limit must be refused: %v", err)
	}
	if err := guard.Allow(table, "p_p1"); err != nil {
		t.Fatalf("p_p1 below soft must be allowed: %v", err)
	}
	if err := guard.Allow(unpartitioned, "all"); err != nil {
		t.Fatalf("unpartitioned below soft must be allowed: %v", err)
	}
	// The logical id the guard keys on must be exactly what the row-side derivation yields.
	rows, err := sicore.PayloadPartitionIDs(bpTableSchema(), sicore.EncodingCSVWithNames, 0, []byte("p,v\np0,1\np1,2\n"))
	if err != nil || len(rows) != 2 || rows[0] != "p_p0" || rows[1] != "p_p1" {
		t.Fatalf("PayloadPartitionIDs = %v err=%v", rows, err)
	}
}
```

with `bpTableSchema()` returning `payloadexec.TableSchema{TableID: "db.t", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}` (imports `pkg/lthash`, `pkg/replay/payloadexec`).

- [ ] **Step 2: Run** — `bazel run //:gazelle && bazel test //pkg/integration:integration_test --test_filter=TestPartsPressureGuard_AgainstRealSystemParts --test_output=errors` (docker required; the target is already in `ci.yml`'s explicit list, so no CI edit). Expected: PASS.

- [ ] **Step 3: Commit + PR** — `git add pkg/integration && git commit -m "test(integration): PartsPressureGuard against real system.parts partitions" && git push -u origin feat/si-backpressure && gh pr create --title "feat(storage-integrity): ingress back-pressure + exception-code plugin rejections (Spec C)" --body "Tasks 9-15 of docs/superpowers/plans/2026-08-18-storage-integrity-physical-table-lifecycle.md."`

---

### Task 16: sentio-node — bump housegate and arbiter-core

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/sentio-node` (branch `feat/si-protocol-tables` off `origin/main`).

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel` (`bazel_dep` versions + `git_override` commits for `housegate` and `arbiter_core`), `MODULE.bazel.lock`

**Interfaces:**
- Produces: `sicore.PhysicalTableName`, `sicore.ErrBackpressure`, `sicore.BackpressureError`, `housegate.StorageIntegrityRuntimeOptions.SchemaResolver`, `config.StorageIntegrityRuntimeBackpressureConfig` (Tasks 10–14) and `ddl.Mode`, `snode.Config.{ProtocolTables,KeeperShardID,HardPartsPerPartition}`, `snode.ErrBackpressure` (Tasks 3–6) available to Tasks 17–18.

- [ ] **Step 1: Bump Go pins** — with `HG_SHA` = merge commit of the housegate PR (Task 15) and `AC_SHA` = merge commit of the arbiter-core PR (Task 6):

```bash
go get github.com/housegate/housegate@${HG_SHA} github.com/sentioxyz/arbiter-core@${AC_SHA}
bazel run @rules_go//go -- mod tidy
go list -m github.com/housegate/housegate github.com/sentioxyz/arbiter-core   # note both pseudo-versions
```

- [ ] **Step 2: Bump Bzlmod pins** — in `MODULE.bazel` set `bazel_dep(name = "housegate", version = "<housegate pseudo-version without leading v>")` and `bazel_dep(name = "arbiter_core", version = "<arbiter-core pseudo-version without leading v>")`, and in the two `git_override` blocks set `commit = "<full 40-char sha>"` and refresh the `# Resolved … ; source is pinned by the commit below.` comment lines (same shape as the current lines 30-40). sentio-node has no updater script for these two modules; edit by hand exactly as `0c9a37a` did.
- [ ] **Step 3: Verify** — `./scripts/update-bazel-deps.sh && bazel build //... && bazel test //config/... //storageintegrityadapter/... //standalone/... --test_output=errors` Expected: PASS (`go doc github.com/housegate/housegate/pkg/storageintegrity.PartsPressureGuard` and `go doc github.com/sentioxyz/arbiter-core/dataplane/ddl.Mode` print).
- [ ] **Step 4: Commit** — `git add go.mod go.sum MODULE.bazel MODULE.bazel.lock && git commit -m "chore(deps): upgrade housegate and arbiter-core for protocol tables + back-pressure"`

---

### Task 17: sentio-node — wire mode/NodeID/hard-limit/schema resolver, map the SNode mirror error, config cross-checks

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/sentio-node` (same branch).

**Files:**
- Modify: `standalone/standalone.go:255-330`, `storageintegrityadapter/adapter.go` (`PrepareLocalStatement`), `storageintegrityadapter/adapter_test.go`, `config/config.go` (`validateStorageIntegrity`), `config/config_test.go`

**Interfaces:**
- Consumes: Task 16 pins.
- Produces: `storageintegrityadapter.ProtocolTablesMode(schemaSource string) ddl.Mode` (`"network_state"` → `ddl.ModeCreateAndVerify`; `""`/`"clickhouse"` → `ddl.ModeVerifyOnly`); `SourcePreparer.PrepareLocalStatement` translates `snode.ErrBackpressure` into a `*sicore.BackpressureError` (Kind `"hard"`, `Unwrap` → `sicore.ErrBackpressure`) so housegate's ingress surfaces exception 252; config validation: `housegate.storage_integrity.runtime.backpressure.unsafe_database` must equal `config.StorageIntegrityUnsafeDatabase`, and `snode.CHTableName(id) == sicore.PhysicalTableName(id)` for every configured table id (cross-repo D2 tripwire).

- [ ] **Step 1: Write the failing tests.** `storageintegrityadapter/adapter_test.go`:

```go
func TestPrepareMapsSNodeBackpressureToHousegateBackpressure(t *testing.T) {
	f := &fakeRole{prepErr: fmt.Errorf("%w: hg_unsafe.orders__t partition p_a has 2950 active parts (hard limit 2950)", snode.ErrBackpressure)}
	_, err := NewSourcePreparer(f).PrepareLocalStatement(t.Context(), validEnvelope("0xabc:1:x"), nil)
	require.ErrorIs(t, err, sicore.ErrBackpressure)
	var be *sicore.BackpressureError
	require.ErrorAs(t, err, &be)
	require.Equal(t, "hard", be.Kind)
	require.Contains(t, err.Error(), "storage_integrity: back-pressure")
	require.ErrorIs(t, err, snode.ErrBackpressure, "the arbiter-core sentinel must stay in the chain for logs")
}

func TestProtocolTablesModeFollowsSchemaSource(t *testing.T) {
	require.Equal(t, ddl.ModeCreateAndVerify, ProtocolTablesMode("network_state"))
	require.Equal(t, ddl.ModeVerifyOnly, ProtocolTablesMode("clickhouse"))
	require.Equal(t, ddl.ModeVerifyOnly, ProtocolTablesMode(""))
}

func TestPhysicalTableNameMatchesArbiterCore(t *testing.T) {
	for _, id := range []string{"orders.t", "a.b.c", "plain"} {
		require.Equal(t, snode.CHTableName(id), sicore.PhysicalTableName(id))
	}
}
```

(imports: `"fmt"`, `"github.com/sentioxyz/arbiter-core/dataplane/ddl"`). `config/config_test.go` (inside `TestConfigValidate_StorageIntegrityAssembly`):

```go
	t.Run("backpressure unsafe database must match", func(t *testing.T) {
		cfg := base
		cfg.Housegate.StorageIntegrity.Runtime.Backpressure.Enabled = true
		cfg.Housegate.StorageIntegrity.Runtime.Backpressure.UnsafeDatabase = "hg_unsafe_other"
		err := cfg.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "backpressure.unsafe_database")
	})
```

(`indexerConfig(t)` builds the housegate config by hand, so `Backpressure` is zero-valued there; the subtest sets `Enabled` explicitly.)

- [ ] **Step 2: Run to verify they fail** — `go test ./storageintegrityadapter/ ./config/ -run 'Backpressure|ProtocolTablesMode|PhysicalTableName'` Expected: FAIL (`undefined: ProtocolTablesMode`; the adapter returns the raw snode error; config passes).

- [ ] **Step 3: Implement.** `storageintegrityadapter/adapter.go`:

```go
// ProtocolTablesMode maps the configured schema source to the DDL mode (spec
// C D1): network_state may create; clickhouse can only verify (creating from
// what it reads would be circular).
func ProtocolTablesMode(schemaSource string) ddl.Mode {
	if schemaSource == "network_state" {
		return ddl.ModeCreateAndVerify
	}
	return ddl.ModeVerifyOnly
}
```

and in `PrepareLocalStatement`, replace `if err != nil { return sicore.PreparedLocalResult{}, err }` after the role call with:

```go
	if err != nil {
		if errors.Is(err, snode.ErrBackpressure) {
			return sicore.PreparedLocalResult{}, &prepareBackpressureError{
				BackpressureError: sicore.BackpressureError{
					Database:  config.StorageIntegrityUnsafeDatabase,
					Table:     sicore.PhysicalTableName(env.TargetTableID),
					Partition: "*",
					Kind:      "hard",
				},
				cause: err,
			}
		}
		return sicore.PreparedLocalResult{}, err
	}
```

with

```go
// prepareBackpressureError carries both sentinels: housegate matches
// sicore.ErrBackpressure (exception 252), operators still see snode's cause.
type prepareBackpressureError struct {
	sicore.BackpressureError
	cause error
}

func (e *prepareBackpressureError) Error() string { return e.BackpressureError.Error() + ": " + e.cause.Error() }
func (e *prepareBackpressureError) Unwrap() []error {
	return []error{&e.BackpressureError, e.cause}
}
```

(`errors.As(err, &be)` for `*sicore.BackpressureError` works through the multi-`Unwrap`; add imports `"errors"`, `"compute-network-node/config"`, `"github.com/sentioxyz/arbiter-core/dataplane/ddl"` — if importing `config` from the adapter creates a cycle, take the unsafe database name as a `NewSourcePreparer` option instead: `NewSourcePreparerWithUnsafeDatabase(role, db)`; check `go build ./...` first.)

`config/config.go::validateStorageIntegrity`, after the merge-guard loop:

```go
	if bp := c.Housegate.StorageIntegrity.Runtime.Backpressure; bp.Enabled && bp.UnsafeDatabase != StorageIntegrityUnsafeDatabase {
		return fmt.Errorf("housegate.storage_integrity.runtime.backpressure.unsafe_database (%q) must equal %q", bp.UnsafeDatabase, StorageIntegrityUnsafeDatabase)
	}
	for i, tableID := range c.StorageIntegrity.SNode.TableIDs {
		if snode.CHTableName(tableID) != sicore.PhysicalTableName(tableID) {
			return fmt.Errorf("storage_integrity.snode.table_ids[%d] %q: arbiter-core and housegate disagree on the physical table name (D2 freeze broken)", i, tableID)
		}
	}
```

`standalone/standalone.go`: in the `snode.New(snode.Config{...})` literal add `SafeDatabase: "hg_safe", PromoteDatabase: "hg_promote", ProtocolTables: storageintegrityadapter.ProtocolTablesMode(si.SNode.SchemaSource), KeeperShardID: 0, HardPartsPerPartition: cfg.Housegate.StorageIntegrity.Runtime.Backpressure.HardPartsPerPartition,` (NodeID stays `si.SNode.NodeID` — the id the role registers with, per spec §4). Build the resolver once — `siResolver := storageintegrityadapter.NewSchemaResolver(tables)` — and use it in both `siRuntime` (`SchemaResolver: siResolver`) and `siMaterializer` (`SchemaResolver: siResolver`).

- [ ] **Step 4: Run** — `./scripts/update-bazel-deps.sh && bazel test //config/... //storageintegrityadapter/... //standalone/... --test_output=errors` Expected: PASS.

- [ ] **Step 5: Commit** — `git add standalone storageintegrityadapter config && git commit -m "feat(storage-integrity): ensure protocol tables by schema source; map SNode back-pressure; wire schema resolver"`

---

### Task 18: sentio-node — Phase-B smoke: pinned protocol tables exist after boot, drift fails startup

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/sentio-node` (same branch).

**Files:**
- Modify: `standalone/storage_integrity_smoke_test.go` (`TestSchemaRegistryPhaseBSmoke` after phase 2), `README.md` (smoke section), `docs/schema-registry-phase-b-runbook.md` (one paragraph)

**Interfaces:**
- Consumes: `ddl.VerifyProtocolTable`, `ddl.Intents`, `ddl.Pinned`, `ddl.ErrProtocolTableDrift`; existing `startSmokeStandalone`, `waitForListener`, `smokeStandalone.stop`.

- [ ] **Step 1: Extend the smoke.** In `TestSchemaRegistryPhaseBSmoke`, after `waitForStatementRCBound(t, phase2.ctx, cfg, statementID)` append:

```go
	// Spec C: with schema_source network_state the SNode creates + verifies the
	// pinned protocol tables for every configured id, including the table
	// declared for the first time in this run.
	pinned := ddl.Pinned{
		UnsafeDB: config.StorageIntegrityUnsafeDatabase, SafeDB: "hg_safe", PromoteDB: "hg_promote",
		NodeID: cfg.StorageIntegrity.SNode.NodeID,
	}
	unsafeIntent, safeIntent, err := ddl.Intents(pinned, declared)
	require.NoError(t, err)
	require.NoError(t, ddl.VerifyProtocolTable(t.Context(), chConn, unsafeIntent), "hg_unsafe must carry the pinned RMT DDL")
	require.NoError(t, ddl.VerifyProtocolTable(t.Context(), chConn, safeIntent), "hg_safe must carry the pinned MergeTree DDL")
	phase2.stop(t)

	// Drift is fail-closed: a tampered pinned setting must stop the node.
	physical := snode.CHTableName(logicalID)
	require.NoError(t, chConn.Exec(t.Context(), fmt.Sprintf(
		"ALTER TABLE %s.%s MODIFY SETTING parts_to_throw_insert = 2999", config.StorageIntegrityUnsafeDatabase, physical)))
	t.Cleanup(func() {
		_ = chConn.Exec(context.Background(), fmt.Sprintf(
			"ALTER TABLE %s.%s MODIFY SETTING parts_to_throw_insert = 3000", config.StorageIntegrityUnsafeDatabase, physical))
	})
	cfg.StorageIntegrity.SNode.StateDir = t.TempDir()
	phase3 := startSmokeStandalone(t, cfg)
	defer phase3.stop(t)
	select {
	case runErr := <-phase3.runErr:
		require.Error(t, runErr)
		require.ErrorIs(t, runErr, ddl.ErrProtocolTableDrift)
		require.Contains(t, runErr.Error(), "parts_to_throw_insert")
	case <-time.After(60 * time.Second):
		t.Fatal("drifted protocol table must fail startup within 60s")
	}
```

(imports: `"github.com/sentioxyz/arbiter-core/dataplane/ddl"`; `declared` is the `payloadexec.TableSchema` unmarshalled earlier in the same test.) If `Run` wraps the snode error with `%v` instead of `%w`, change that `fmt.Errorf` in `standalone.go` to `%w` so `ErrorIs` holds.

- [ ] **Step 2: Docs.** README smoke section: state that the smoke ClickHouse must have Keeper (`config.d` `<zookeeper>`/`<keeper_server>`, e.g. the `scripts/ci/clickhouse-keeper.xml` from arbiter-core), that Phase-1 tables must already conform to the pinned DDL (`hg_unsafe.*` ReplicatedMergeTree at `/sentio/0/unsafe/<t>` replica `<snode.node_id>`, `hg_safe.*` MergeTree, both with the pinned settings — print them with `arbiter-snode -print-ddl` or let a `network_state` boot create them), that Phase 2 now creates the acceptance table's protocol tables and Phase 3 proves drift refusal, and that between runs the operator drops `hg_unsafe.<t> SYNC` / `hg_safe.<t>` for the acceptance table. Onboarding step 2: replace "add the corresponding `snode.CHTableName(id)` physical table to `merge_guard.tables`" with the same sentence plus "the role creates `hg_unsafe.<t>`/`hg_safe.<t>` itself when `schema_source: network_state`; with `clickhouse` they must pre-exist with the pinned DDL". Runbook: one paragraph under the source-flip step noting that flipping to `network_state` also enables protocol-table creation and that startup now fails on DDL drift.

- [ ] **Step 3: Run** — `bazel test //standalone:standalone_test --test_output=errors` (gated tests skip; the file must compile) and, when the operator environment is available: `SENTIO_SI_E2E=1 SENTIO_SI_CONFIG=… <the README env> go test ./standalone -run TestSchemaRegistryPhaseBSmoke -count=1 -timeout=15m` Expected: PASS.

- [ ] **Step 4: Commit + PR** — `git add standalone README.md docs && git commit -m "test(standalone): smoke proves pinned protocol tables and drift refusal" && git push -u origin feat/si-protocol-tables && gh pr create --title "feat(storage-integrity): protocol tables + back-pressure wiring (Spec C)" --body "Tasks 16-18 of housegate docs/superpowers/plans/2026-08-18-storage-integrity-physical-table-lifecycle.md."`

---

### Task 19: Docs — housegate CLAUDE.md + arbiter-core README + operator notes

**Repo / cwd:** housegate `/Users/uranuswch/Dev/housegate/housegate` (branch `feat/si-backpressure` or a follow-up `docs/si-backpressure`), arbiter-core (already covered by Task 3 Step 5; verify), arbiter (Task 8 Step 5; verify).

**Files:**
- Modify: housegate `CLAUDE.md` (Key Modules bullets for `pkg/storageintegrity` / root runtime; Known Rough Edges), housegate `docs/superpowers/specs/2026-08-18-storage-integrity-physical-table-lifecycle-design.md` (status line only)

- [ ] **Step 1: CLAUDE.md** — under Key Modules add a bullet (one paragraph, no hard wraps): `pkg/storageintegrity` back-pressure: `PartsPressureGuard` (snapshot of `system.parts` grouped by `(database, table, partition)`; keys use `LogicalPartitionID` = `p_<partition text>`/`all`, never `partition_id`, which is a SipHash for String keys), `PayloadPartitionIDs` (CSV + Native), `PhysicalTableName` (D2 mirror of arbiter-core `ddl.CHTableName`), `ErrBackpressure`/`BackpressureError`; root `StorageIntegrityPartsPressureSupervisor` polls every `storage_integrity.runtime.backpressure.poll_interval` (2s), first refresh is startup fail-fast, gauges `storage_integrity_unsafe_parts` / `storage_integrity_safe_parts` and counter `storage_integrity_backpressure_total`; ingress refuses with `chproto.ClientError{Code: 252}` between the merge-health latch and the payload put; the plugin chain can now reject with an explicit code via `chproto.ClientError` (relay `exceptionForPluginError`, default stays 403). Under Known Rough Edges add: `hg_safe` part counts only grow in v1 (merges stay stopped, `allow_native_background_merges` still rejected) — a `hg_safe` partition approaching `parts_to_throw_insert` is the P4 controlled-compaction prerequisite, not an incident to fix by enabling merges; the strict-input rejection path still closes the client connection after the exception. Mark the spec status `Proposed` → `Implemented (see plan)` once all PRs merge.
- [ ] **Step 2: Verify the two READMEs** — arbiter-core `README.md` shows the keeper mount + `//dataplane/ddl:ddl_test` (Task 3); arbiter `README.md` documents `--ensure-tables` / `-print-ddl` and no longer says tables "must already exist" for inline-tables roles (Task 8). Base-design §6 replacement (naming/DDL example = `BuildDDL` golden output) is Spec B's job — leave it.
- [ ] **Step 3: Commit** — `git add CLAUDE.md docs && git commit -m "docs: storage-integrity back-pressure and protocol-table lifecycle notes"`

---

## Self-review

**1. Spec coverage** — see the map below; every numbered spec section maps to at least one task. Two deliberate deviations, both recorded in the tasks: (a) `ddl.Mode` has a third value `ModeOff` (zero value) so existing test harnesses in arbiter-core and the arbiter `integration/chpipeline` suite (which pre-create plain MergeTree tables and run without Keeper) keep working across the arbiter-core bump; production wiring (arbiter cmds, sentio-node) always resolves `verify`/`create` from the schema source. (b) `BuildDDL` returns an `error` (the spec signature has none) because the spec itself requires it to enforce the partition freeze "with an error that says exactly what the freeze is". Also: the spec's D5 phrase "grouped by `partition_id`" is implemented as grouped by `partition` (text) → `p_<text>`, because `partition_id` for a String key is a SipHash (verified) — recorded in Global Constraints and in `BuildSnapshotQuery`'s comment/test.

**2. Placeholder scan** — the only open values are the merge-commit SHAs / tags used by the dependency-bump tasks (Tasks 7, 16), which cannot exist before the upstream PRs merge; each states exactly which commit to use. No "TBD"/"similar to task N"; every code step carries the code.

**3. Type consistency** — checked: `ddl.Pinned{UnsafeDB, SafeDB, PromoteDB, NodeID, KeeperShardID}` (Tasks 1, 3, 4, 5, 8, 18); `ddl.EnsureProtocolTables(ctx, conn, p, tables, mode, logger)` (Tasks 3, 4, 5); `ddl.Mode` values `ModeOff|ModeVerifyOnly|ModeCreateAndVerify` and `ParseMode("off"|"verify"|"create")` (Tasks 3, 4, 5, 8, 17); `snode.Config.{ProtocolTables, ProtocolTablesReconcile, KeeperShardID, HardPartsPerPartition}` (Tasks 4, 6, 8, 17); `snode.ErrBackpressure` (Tasks 6, 17); `verifier.Deps.Conn` + `verifier.Config.{SafeDatabase, PromoteDatabase, ProtocolTables}` (Tasks 5, 8); `chproto.ClientError{Code int32, Message, Err}` + `chproto.CodeTooManyParts` (Tasks 9, 14); `sicore.PhysicalTableName`, `sicore.PayloadPartitionIDs(schema, encoding, revision, payload)`, `sicore.ErrPartitionFreeze` (Tasks 10, 14, 15, 17); `sicore.PartsPressureGuard` methods `BuildSnapshotQuery/Refresh/Snapshot/Allow/Invalidate/Invalidated`, `sicore.PartsKey{Database, Table, Partition}`, `sicore.PartsPressureConfig`, `sicore.BackpressureError{Database, Table, Partition, Parts, Limit, Kind}`, `sicore.LogicalPartitionID` (Tasks 11, 13, 14, 15, 17); `config.StorageIntegrityRuntimeBackpressureConfig{Enabled, UnsafeDatabase, SafeDatabase, PollInterval, SoftPartsPerPartition, HardPartsPerPartition}` (Tasks 12, 14, 17); root `StorageIntegrityPartsPressure` interface `{Allow, Invalidate}`, `NewStorageIntegrityPartsPressureSupervisor(guard, interval, unsafeDB, safeDB)`, `StorageIntegrityIngress.WithPartsPressure(pressure, schemas)`, fields `pressure`/`pressureRunner`, `StorageIntegrityRuntimeOptions.{SchemaResolver, PartsPressure}` (Tasks 13, 14, 17); metric names `storage_integrity_unsafe_parts{table,partition}`, `storage_integrity_safe_parts{table,partition}`, `storage_integrity_backpressure_total{table}` (Tasks 13, 14, 19).

## Spec coverage map

| Spec section | Requirement | Tasks |
|---|---|---|
| §1 problem 1 / §2 goal 1 | roles create/verify protocol tables idempotently from the schema source | 3, 4, 5, 8, 17 |
| §1 problem 2 / §2 goal 2 / D3 | pinned settings in DDL, verified on startup, fail-closed on drift | 1, 2, 3, 4, 5, 18 |
| §1 problem 3 / §2 goal 3 / D5 | ingress soft-limit refusal with retryable 252 + `storage_integrity: back-pressure` prefix; SNode hard-limit mirror; never journaled | 6, 9, 11, 12, 13, 14, 15, 17 |
| §2 goal 4 / §6 / D6 | `hg_safe` growth signal, `storage_integrity_unsafe_parts` / `storage_integrity_safe_parts` / `storage_integrity_backpressure_total`, runbook note | 13, 14, 19 |
| D1 | mode by schema source: `network_state|chain` create, `clickhouse` verify-only | 3, 8, 17 |
| D2 | naming freeze `hg_unsafe.<CHTableName>` , zk path `/sentio/0/unsafe/<t>`, replica = node id | 1, 5 (verifier uses `ddl.CHTableName`), 10, 17 (cross-repo tripwire) |
| D4 | drift fail-closed naming table + field, no auto-ALTER | 3, 4, 5, 18 |
| D5 partition derivation | from decoded payload rows before prepare; multi-partition payload checked per partition | 10, 14, 15 |
| §4 `Pinned` / `BuildDDL` / `EnsureProtocolTables` / verify parser | | 1, 2, 3 |
| §4 callers: `snode.NewRole` / `verifier.NewRole` before Arbiter registration; 60s reconcile | | 4, 5 |
| §4 reference binaries `--ensure-tables=verify|create` | (+ `-print-ddl`) | 7, 8 |
| §4 sentio-node passes NodeID + schema source | | 17 |
| §4 partition freeze: skip with warning at ensure; refuse at ingress | | 1, 3, 10, 14 |
| §5 `PartsPressureGuard` over `MergeConn`, `Snapshot`/`Allow`, config block, ingress placement after merge-health and before payload put | | 11, 12, 13, 14 |
| §5 SNode mirror `ErrBackpressure` mapped to Retryable/252 by housegate | | 6, 14, 17 |
| §7 acceptance: BuildDDL golden; create→verify→tamper→drift; verify-only never creates; reconcile idempotent; two replicas replicate | | 1, 3, 4 |
| §7 acceptance: fake-conn guard tests (below/at/above soft, hard, multi-partition); ingress 252 without payload put/journal; metrics registered once | | 11, 13, 14, 15 |
| §7 acceptance: sentio-node smoke (tables exist with pinned settings; drift → startup failure) | | 18 |
| §8 delivery order + dependency bumps | | 7 (arbiter ← arbiter-core), 16 (sentio-node ← housegate + arbiter-core), 19 (docs) |
| Roadmap §4.6 (`hg_safe` merges stay stopped) | | 1 (`SafeSettings`), 19 |
| Roadmap §5 bounded task: verifier private `chTableName` → shared export | | 1, 5 |
