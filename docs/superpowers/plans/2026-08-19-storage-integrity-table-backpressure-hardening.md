# Protocol Table and Back-Pressure Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A hostile or merely wrong table declaration can no longer produce unparseable DDL or an unrecoverable node; the protocol-table lifecycle mode is derived from the schema source instead of being a fail-open config field; part-pressure reads become bounded and operator-tunable so back-pressure cannot degrade into permanent refusal; transient ClickHouse errors stop killing roles; `hg_promote` joins the lifecycle; and a back-pressure refusal throttles a client without tearing down its connection.

**Architecture:** Four repos, one dependency chain. HouseGate exports the storage-integrity column-type whitelist that already lives in `pkg/replay/payloadexec` (`parseValue`), and arbiter-core calls it from `ddl.Intents` so `BuildDDL`, `EnsureProtocolTables` and both roles' `validate()` share one gate that rejects before any `CREATE`. arbiter-core replaces the `ProtocolTables ddl.Mode` config field with a `SchemaSource` string that `validate()` maps to a mode (no reachable `ModeOff`), renders and verifies `hg_promote` alongside `hg_unsafe`/`hg_safe`, and gives the reconcile loop a drift-vs-transient split with bounded backoff plus a startup ensure. HouseGate splits `PartsPressureGuard`'s single unbounded `system.parts` scan into a bounded per-key exact read on the admission path and an aggregate `count()` poll for gauges and untouched keys, tracks per-key freshness/generation so a partial read never implies absence, scopes the admission mutex and the cleanup-proof fence per table, and adds a non-closing rejection class so exception 252 ends the query instead of the session. sentio-node bumps both pins, aligns its name-pin validator, and stops feeding the SNode mirror limit when back-pressure is disabled.

**Tech Stack:** Go 1.26, Bazel 9 + Bzlmod + gazelle in all four repos, clickhouse-go v2 (sentioxyz fork v2.47.0-sentioxyz-20260629), ch-go (sentioxyz fork v0.73.0-sentioxyz-20260629), ClickHouse 25.8 docker image, Prometheus client_golang, `log/slog` (arbiter-core) / `github.com/housegate/housegate/pkg/log` (housegate).

**Spec:** `/Users/uranuswch/Dev/housegate/housegate/docs/superpowers/specs/2026-08-19-storage-integrity-table-backpressure-hardening-design.md` (Spec L). Roadmap: `docs/superpowers/specs/2026-08-19-storage-integrity-remediation-roadmap.md` (§4 decisions 4 and 5 are this plan's D1 and D3). Remediates Spec C `docs/superpowers/specs/2026-08-18-storage-integrity-physical-table-lifecycle-design.md`, whose implementation record (what actually shipped, and why it deviates from its own plan text) is in `docs/superpowers/plans/2026-08-18-storage-integrity-physical-table-lifecycle.md` §"Progress and Evidence-Backed Deviations" — read that section before touching `parts_pressure.go`.

## Global Constraints

- Repos (all local, absolute paths): housegate `/Users/uranuswch/Dev/housegate/housegate` (main `621eaab`, v0.9.3), arbiter-core `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` (main `b669ccd`, v0.3.1), arbiter `/Users/uranuswch/Dev/sentio_xyz/arbiter` (main `71657a8`, v0.2.1), sentio-node `/Users/uranuswch/Dev/sentio_xyz/sentio-node` (main `ba136ea`, no tags yet). Each task names its repo and branch; branch off that repo's latest `origin/main`. Never push to `main`; open PRs with `gh pr create`.
- Go module paths: `github.com/housegate/housegate`, `github.com/sentioxyz/arbiter-core`, `github.com/sentioxyz/arbiter`, sentio-node `compute-network-node`.
- Bazel is ground truth everywhere. After adding Go files or imports: `bazel run //:gazelle` (sentio-node: `./scripts/update-bazel-deps.sh`). After `go.mod` changes: `bazel mod tidy` (housegate / arbiter-core / arbiter); sentio-node uses `bazel run @rules_go//go -- mod tidy`.
- Docker-gated tests. arbiter-core: `ARBITER_CH_INTEGRATION=1` + `CH_ADDR` (default `127.0.0.1:9000`), and for the Keeper/replica suites `ARBITER_CH_KEEPER=1` / `ARBITER_CH_REPLICA=1` + `CH_REPLICA_ADDR`; its `.github/workflows/ci.yml` starts a two-node shared-Keeper ClickHouse. housegate: `//pkg/integration:integration_test` is tagged `manual` and listed explicitly in `.github/workflows/ci.yml`; helpers `openDirectCH(t)`, `mustExec(t, conn, sql)`, `uniqueTable(t)`, `testenv.ClickHouseCLI(t)`, `testenv.RunCLICompressedMultiquery(...)`. sentio-node: `SENTIO_SI_E2E=1`.
- Naming freeze (Spec C D2, unchanged): physical table = `strings.ReplaceAll(tableID, ".", "__")`; zk path = `/sentio/<keeper_shard_id>/unsafe/<physical>`; replica name = the node id the role registers with; `keeper_shard_id = 0` in v1. Databases are pinned to exactly `hg_unsafe`, `hg_safe`, `hg_promote`.
- Pinned settings (Spec C D3, unchanged): `hg_unsafe.*` = `ReplicatedMergeTree` + `max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0`; `hg_safe.*` and `hg_promote.*` = `MergeTree` + `max_bytes_to_merge_at_max_space_in_pool = 0`; columns `_hg_row_id FixedString(32)` first then the declared user columns in declared order; `PARTITION BY <partition_by>`; `ORDER BY (<partition_by>, _hg_row_id)` (unpartitioned: `ORDER BY (_hg_row_id)`).
- Back-pressure constants (Spec C D5, unchanged): soft = 2400, hard = 2950 per partition, poll interval 2s; rejection = ClickHouse exception code `252` with message prefix `storage_integrity: back-pressure`; never journaled. New config keys added by this plan keep the current code defaults: `refresh_timeout: 2s`, `snapshot_ttl: 6s`.
- SI column-type whitelist (Spec L D1, verbatim from the spec): `String`, `FixedString(N)`, `Bool`, `Float32`, `Float64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`. Nothing else. Temporal types (`Date` / `DateTime` / `DateTime64`) are **deliberately excluded** even though `pkg/replay/nativepayload` and `pkg/replay/chexec` can decode them, because `payloadexec.parseValue` — the pinned in-process executor's row materializer — cannot, and a column the pinned executor cannot materialize is exactly the "silent divergence" D1 rejects. Do not widen the set in this plan.
- Partition identity (unchanged): logical partition id is `p_<system.parts.partition>`, or `all` when and only when `system.tables.partition_key` is empty. A partitioned `String` column whose value is literally `tuple()` stays `p_tuple()`. Never infer unpartitioned state from the partition text alone.
- English-only code, comments, log and error strings. housegate logging via `github.com/housegate/housegate/pkg/log` (`log.Infow` / `Warnw` / `WarnEveryN`); arbiter-core via the role's `*slog.Logger`. Wrap errors with `fmt.Errorf("context: %w", err)`; aggregate config errors with `errors.Join`.
- Markdown docs: no hard line-wrapping (one paragraph per line).
- Release tags are **computed by each repo's release workflow, not chosen by you**. housegate's `.github/workflows/release.yml` is `workflow_dispatch` with a `bump` input (`auto|major|minor|patch`) and prints the tag it cut; arbiter-core / arbiter use their own Cut Release workflow. Every dependency-bump task in this plan says "the tag the previous release task published" — read it from the workflow run output and record it in the PR body. Spec C's plan burned a whole correction on assuming a version number; do not repeat that.
- Do not change the reservation/cleanup-proof protocol's *semantics* anywhere in this plan. Tasks 13–19 change **which rows a query returns and which keys a refresh is authoritative for**; every ownership, coverage, rebase, and fail-closed rule stays exactly as it is. The 49 `TestPartsPressureGuard_*` unit tests in `pkg/storageintegrity/parts_pressure_test.go` are the regression net: they must all pass unmodified after each task except where a task explicitly says otherwise.

## File Structure

housegate (`/Users/uranuswch/Dev/housegate/housegate`):
- `pkg/replay/payloadexec/column_types.go` *(new)* — `SupportedColumnType`, `ValidateColumnType`, `ValidateTableSchemaColumns`, `ErrUnsupportedColumnType`. One switch, shared with `parseValue`.
- `pkg/storageintegrity/parts_pressure.go` — split reads: `BuildAggregateSnapshotQuery`, `BuildExactPartsQuery`, per-key `partsKeyState`, scoped `refresh`, per-table `admissionMu`, per-table cleanup-proof fence.
- `pkg/storageintegrity/parts_pressure_scope.go` *(new)* — `PartsScope` and its query/predicate builders, kept out of the 1300-line guard file.
- `pkg/config/storage_integrity_config.go` — `refresh_timeout` / `snapshot_ttl` keys; one `validateStorageIntegrityDatabaseNames` that runs whenever any SI feature is enabled; `PromoteDatabase` pin.
- `pkg/chproto/client_error.go` — `ClientError.KeepSession`.
- `pkg/proxy/relay.go` — pending-rejection terminal swap (`rejectActiveQueryTerminal` / `takePendingRejection`), non-closing strict-hook rejection in both the `SuppressUpstreamExecution` path and `runDeferredInsert`.
- `storage_integrity_backpressure.go` — supervisor drives the aggregate poll + the exact poll for live keys; `Invalidate` marks stale instead of forcing a scan.
- `storage_integrity_runtime.go` — feeds the two new config keys into `PartsPressureConfig`.
- Tests: `pkg/replay/payloadexec/column_types_test.go` *(new)*, `pkg/storageintegrity/parts_pressure_test.go`, `pkg/storageintegrity/parts_pressure_scope_test.go` *(new)*, `pkg/config/storage_integrity_config_test.go`, `pkg/proxy/relay_reject_test.go` *(new)*, `pkg/integration/storage_backpressure_test.go`, `pkg/integration/storage_backpressure_bounded_test.go` *(new)*, `pkg/integration/storage_backpressure_session_test.go` *(new)*.

arbiter-core (`/Users/uranuswch/Dev/sentio_xyz/arbiter-core`):
- `dataplane/ddl/build.go` — column-type gate in `Intents`; `hg_promote` intent; `Intents` returns three intents.
- `dataplane/ddl/ensure.go` — `SchemaSource`-derived `Mode`, `ModeFromSchemaSource`, promote creation/verification, `ErrColumnType` propagation.
- `dataplane/ddl/reconcile.go` *(new)* — `ClassifyReconcileError` + the bounded-backoff policy shared by both roles.
- `snode/config.go`, `verifier/config.go` — `SchemaSource string`, derived mode, partition-freeze parity in the verifier.
- `snode/snode.go`, `verifier/verifier.go` — startup ensure inside `Run`, drift-vs-transient reconcile, subscription-cancellation error fidelity.
- `snode/promote_replace.go` — stop creating `hg_promote` ad hoc; fail with a clear error when it is absent.
- Tests: `dataplane/ddl/build_test.go`, `dataplane/ddl/ensure_ch_test.go`, `dataplane/ddl/reconcile_test.go` *(new)*, `snode/config_test.go`, `snode/protocol_tables_test.go`, `snode/promote_test.go`, `verifier/config_test.go`, `verifier/protocol_tables_test.go`.

arbiter (`/Users/uranuswch/Dev/sentio_xyz/arbiter`):
- `cmd/arbiter-snode/{main.go,config.go}`, `cmd/arbiter-verifier/{main.go,config.go}` — `-ensure-tables` becomes a cross-check of the derived mode; `README.md`.

sentio-node (`/Users/uranuswch/Dev/sentio_xyz/sentio-node`):
- `config/config.go` — name pins run whenever any SI feature is enabled; mirror-limit gating.
- `standalone/standalone.go` — feed `HardPartsPerPartition` only when back-pressure is enabled.
- `storageintegrityadapter/adapter.go` — `ProtocolTablesMode` follows arbiter-core's derived-mode API.
- Tests: `config/config_test.go`, `standalone/storage_integrity_smoke_test.go`.

---

## Phase A — HouseGate exports the column-type whitelist (unblocks arbiter-core D1)

### Task 1: housegate — export the SI column-type whitelist from `payloadexec`

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (branch `feat/si-column-type-export` off `origin/main`).

**Files:**
- Create: `pkg/replay/payloadexec/column_types.go`
- Create: `pkg/replay/payloadexec/column_types_test.go`
- Modify: `pkg/replay/payloadexec/executor.go:472-513` (`parseValue`'s `default` branch delegates to the new validator)

**Interfaces:**
- Consumes: `payloadexec.TableSchema{TableID, PartitionBy string; Columns []lthash.Column}`, `lthash.Column{Name, Type string}` (both already exist).
- Produces (used by Tasks 4, 5, 6 in arbiter-core):
  - `payloadexec.ErrUnsupportedColumnType` — sentinel every rejection unwraps to.
  - `payloadexec.SupportedColumnType(typeName string) bool`
  - `payloadexec.ValidateColumnType(typeName string) error`
  - `payloadexec.ValidateTableSchemaColumns(t TableSchema) error` — per-column validation naming the table and column; returns the joined error for all bad columns.

- [ ] **Step 1: Write the failing test** `pkg/replay/payloadexec/column_types_test.go`

```go
package payloadexec

import (
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
)

// supportedTypeMatrix is the MVP whitelist from the storage-integrity design
// (§5.3) and Spec L D1. It is duplicated here on purpose: the test is the
// frozen statement of the set, so a change to the implementation switch alone
// cannot silently widen or narrow it.
var supportedTypeMatrix = []string{
	"String", "FixedString(1)", "FixedString(32)", "FixedString(255)",
	"Bool", "Float32", "Float64",
	"UInt8", "UInt16", "UInt32", "UInt64",
	"Int8", "Int16", "Int32", "Int64",
}

var rejectedTypeMatrix = []string{
	"", " String", "string", "Nullable(String)", "Array(UInt64)",
	"LowCardinality(String)", "Decimal(9, 2)", "UUID", "IPv4",
	"Date", "DateTime", "DateTime64(3)", "Enum8('a' = 1)",
	"Int128", "UInt256", "FixedString(0)", "FixedString(-1)", "FixedString(x)",
	"Map(String, String)", "Tuple(UInt64, String)", "AggregateFunction(sum, UInt64)",
	"String, extra UInt64", "String) ENGINE = MergeTree", "String'",
}

func TestValidateColumnType_AcceptsExactlyTheMVPWhitelist(t *testing.T) {
	for _, name := range supportedTypeMatrix {
		if !SupportedColumnType(name) {
			t.Errorf("SupportedColumnType(%q) = false, want true", name)
		}
		if err := ValidateColumnType(name); err != nil {
			t.Errorf("ValidateColumnType(%q) = %v, want nil", name, err)
		}
	}
	for _, name := range rejectedTypeMatrix {
		if SupportedColumnType(name) {
			t.Errorf("SupportedColumnType(%q) = true, want false", name)
		}
		err := ValidateColumnType(name)
		if !errors.Is(err, ErrUnsupportedColumnType) {
			t.Errorf("ValidateColumnType(%q) = %v, want ErrUnsupportedColumnType", name, err)
		}
		if !strings.Contains(err.Error(), name) && name != "" {
			t.Errorf("ValidateColumnType(%q) error %q does not name the offending type", name, err)
		}
	}
}

// parseValue is the pinned executor's row materializer. The validator must
// accept exactly what it can parse: a wider validator would admit a column the
// replay executor cannot materialize (silent divergence), a narrower one would
// reject a table that already works.
func TestValidateColumnType_AgreesWithParseValue(t *testing.T) {
	for _, name := range append(append([]string{}, supportedTypeMatrix...), rejectedTypeMatrix...) {
		_, parseErr := parseValue(name, "1")
		parseRejectsType := errors.Is(parseErr, ErrUnsupportedColumnType)
		if got := ValidateColumnType(name) != nil; got != parseRejectsType {
			t.Errorf("type %q: ValidateColumnType rejects=%v, parseValue rejects-type=%v (parseErr=%v)", name, got, parseRejectsType, parseErr)
		}
	}
}

func TestValidateTableSchemaColumns_NamesTableAndColumn(t *testing.T) {
	schema := TableSchema{
		TableID: "db.t",
		Columns: []lthash.Column{
			{Name: "ok", Type: "UInt64"},
			{Name: "bad", Type: "Nullable(String)"},
		},
	}
	err := ValidateTableSchemaColumns(schema)
	if !errors.Is(err, ErrUnsupportedColumnType) {
		t.Fatalf("ValidateTableSchemaColumns = %v, want ErrUnsupportedColumnType", err)
	}
	for _, want := range []string{"db.t", "bad", "Nullable(String)"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}
	if err := ValidateTableSchemaColumns(TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "ok", Type: "UInt64"}}}); err != nil {
		t.Fatalf("clean schema rejected: %v", err)
	}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `bazel test //pkg/replay/payloadexec:payloadexec_test --test_output=errors`
Expected: FAIL — compile error `undefined: SupportedColumnType`, `undefined: ValidateColumnType`, `undefined: ValidateTableSchemaColumns`, `undefined: ErrUnsupportedColumnType`.

- [ ] **Step 3: Write `pkg/replay/payloadexec/column_types.go`**

```go
package payloadexec

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// ErrUnsupportedColumnType is the sentinel every column-type rejection unwraps
// to. Spec L D1: the storage-integrity profile validates declared column types
// against the set this package's pinned executor can actually materialize,
// rather than against ClickHouse's grammar, because a type ClickHouse accepts
// but the executor cannot replay is a worse failure (silent divergence) than a
// refusal to create the table.
var ErrUnsupportedColumnType = errors.New("payloadexec: unsupported column type")

// SupportedColumnType reports whether a declared ClickHouse type is inside the
// MVP whitelist (§5.3): String, FixedString(N) with N > 0, Bool, Float32/64 and
// [U]Int8/16/32/64. It is the single source of truth shared by parseValue and
// by every caller that validates a declaration before executing DDL.
func SupportedColumnType(typeName string) bool {
	switch typeName {
	case "String", "Bool", "Float32", "Float64",
		"UInt8", "UInt16", "UInt32", "UInt64",
		"Int8", "Int16", "Int32", "Int64":
		return true
	}
	if !strings.HasPrefix(typeName, "FixedString(") || !strings.HasSuffix(typeName, ")") {
		return false
	}
	inner := typeName[len("FixedString(") : len(typeName)-1]
	width, err := strconv.Atoi(inner)
	return err == nil && width > 0
}

// ValidateColumnType returns ErrUnsupportedColumnType naming the offending
// string when the type is outside the whitelist.
func ValidateColumnType(typeName string) error {
	if SupportedColumnType(typeName) {
		return nil
	}
	return fmt.Errorf("%w %q (whitelist: String, FixedString(N), Bool, Float32/64, [U]Int8/16/32/64)", ErrUnsupportedColumnType, typeName)
}

// ValidateTableSchemaColumns validates every declared column of one table and
// joins the failures, so an operator sees all offending columns at once.
func ValidateTableSchemaColumns(t TableSchema) error {
	var errs []error
	for _, column := range t.Columns {
		if err := ValidateColumnType(column.Type); err != nil {
			errs = append(errs, fmt.Errorf("table %s column %q: %w", t.TableID, column.Name, err))
		}
	}
	return errors.Join(errs...)
}
```

- [ ] **Step 4: Make `parseValue` delegate its default branch**

In `pkg/replay/payloadexec/executor.go`, replace the `default` branch of `parseValue` (currently `executor.go:510-512`):

```go
	default:
		return nil, ValidateColumnType(typeName)
	}
```

Leave every other branch untouched. `parseFixedString` keeps its own `invalid FixedString type` error for malformed widths that reach it; `ValidateColumnType` rejects those before the parse in every caller that validates first.

- [ ] **Step 5: Run the tests**

Run: `bazel test //pkg/replay/payloadexec:payloadexec_test --test_output=errors`
Expected: PASS (the pre-existing executor tests still pass — `parseValue`'s accepted set did not change; only the error value for unsupported types did, and it still contains the type name).

- [ ] **Step 6: Re-run the packages that decode payloads**

Run: `bazel test //pkg/replay/... //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS, all targets.

- [ ] **Step 7: Gazelle + commit**

```bash
bazel run //:gazelle
git add pkg/replay/payloadexec/column_types.go pkg/replay/payloadexec/column_types_test.go pkg/replay/payloadexec/executor.go pkg/replay/payloadexec/BUILD.bazel
git commit -m "feat(payloadexec): export the storage-integrity column-type whitelist"
```

- [ ] **Step 8: Open the PR**

```bash
gh pr create --title "feat(payloadexec): export the storage-integrity column-type whitelist" \
  --body "Spec L D1 needs one source of truth for the SI column-type whitelist so arbiter-core's ddl.Intents can reject a hostile or wrong declaration before any CREATE runs. Exports SupportedColumnType / ValidateColumnType / ValidateTableSchemaColumns from the set parseValue already enumerates, and makes parseValue's default branch return the same error so the two cannot drift."
```

### Task 2: housegate — cut the release that arbiter-core will pin

**Repo / cwd:** `/Users/uranuswch/Dev/housegate/housegate` (on `main`, after Task 1's PR merged).

**Files:** none (workflow run only).

**Interfaces:**
- Produces (used by Task 3): `HOUSEGATE_TAG_A` = the tag the release run published, and `HOUSEGATE_COMMIT_A` = the commit that annotated tag peels to.

- [ ] **Step 1: Confirm main is green**

Run: `git fetch origin && git log --oneline -1 origin/main`
Expected: the merge commit of Task 1's PR.

- [ ] **Step 2: Run the release workflow**

```bash
gh workflow run release.yml --ref main -f bump=patch
gh run watch "$(gh run list --workflow=release.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
```
Expected: the run succeeds and the job summary names the tag it cut (`v0.9.x`).

- [ ] **Step 3: Record the exact tag and commit**

```bash
git fetch --tags
TAG=$(git describe --tags --abbrev=0 origin/main)
echo "HOUSEGATE_TAG_A=$TAG"
echo "HOUSEGATE_COMMIT_A=$(git rev-list -n 1 "$TAG")"
```
Expected: a non-draft, non-prerelease tag whose commit equals Task 1's merge commit. Write both values into the plan-execution notes; Task 3 consumes them verbatim. Do **not** assume a version number — use what the workflow printed.

---

## Phase B — arbiter-core: D1 type validation, D2 derived mode, D5 `hg_promote`, D4 reconcile resilience, D7 verifier parity

All Phase B tasks share one branch: `feat/protocol-table-hardening` off `origin/main` in `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`.

### Task 3: arbiter-core — bump the housegate pin to Task 2's tag

**Files:**
- Modify: `go.mod:29` (`github.com/housegate/housegate v0.9.0`)
- Modify: `MODULE.bazel:10-23` (`bazel_dep(name = "housegate", version = ...)` and the `git_override` commit)

**Interfaces:**
- Consumes: `HOUSEGATE_TAG_A` / `HOUSEGATE_COMMIT_A` from Task 2.
- Produces: `payloadexec.ValidateColumnType` / `ValidateTableSchemaColumns` / `ErrUnsupportedColumnType` are resolvable in this repo (used by Tasks 4–6).

- [ ] **Step 1: Bump the Go module pin**

```bash
go get github.com/housegate/housegate@"$HOUSEGATE_TAG_A"
```
Expected: `go.mod`'s `github.com/housegate/housegate` line now reads the new tag.

- [ ] **Step 2: Bump both Bazel pins**

In `MODULE.bazel`, set the `bazel_dep(name = "housegate", version = "X.Y.Z")` to the tag **without** the leading `v`, and set the `git_override(module_name = "housegate", commit = "...")` to `HOUSEGATE_COMMIT_A`, updating the comment line to name the same version:

```python
bazel_dep(
    name = "housegate",
    version = "0.9.4",
)

git_override(
    module_name = "housegate",
    # Resolved Housegate v0.9.4; source is pinned by the commit below.
    commit = "<HOUSEGATE_COMMIT_A>",
    remote = "https://github.com/housegate/housegate",
)
```

- [ ] **Step 3: Re-resolve and verify the symbol exists**

```bash
bazel mod tidy
bazel run //:gazelle
bazel build //...
```
Expected: all three succeed. Then confirm the new API is visible:

```bash
bazel run -- @rules_go//go doc github.com/housegate/housegate/pkg/replay/payloadexec ValidateTableSchemaColumns
```
Expected: the doc comment from Task 1 prints.

- [ ] **Step 4: Run the non-docker suite**

Run: `bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors`
Expected: PASS (docker-gated tests skip without `ARBITER_CH_INTEGRATION=1`).

- [ ] **Step 5: Commit**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock
git commit -m "build: pin housegate $HOUSEGATE_TAG_A for the SI column-type validator"
```

### Task 4: arbiter-core — D1 column-type gate inside `ddl.Intents`

**Files:**
- Modify: `dataplane/ddl/build.go:64-93` (`Intents`)
- Modify: `dataplane/ddl/build_test.go`

**Interfaces:**
- Consumes: `payloadexec.ValidateTableSchemaColumns(t) error`, `payloadexec.ErrUnsupportedColumnType` (Task 1).
- Produces (used by Tasks 5, 6, 8): `ddl.Intents` and `ddl.BuildDDL` reject a declaration whose column types are outside the SI whitelist, with the error unwrapping to `payloadexec.ErrUnsupportedColumnType`. Ordering inside `Intents` is frozen: partition-freeze check first, then reserved-column check, then column types.

- [ ] **Step 1: Write the failing test** — append to `dataplane/ddl/build_test.go`

```go
func TestIntents_RejectsColumnTypeOutsideTheSIWhitelist(t *testing.T) {
	for name, schema := range map[string]payloadexec.TableSchema{
		"nullable":     {TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "Nullable(UInt64)"}}},
		"array":        {TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "Array(String)"}}},
		"temporal":     {TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "DateTime"}}},
		"ddl_injection": {TableID: "db.t", Columns: []lthash.Column{
			{Name: "v", Type: "String, injected UInt64"},
		}},
		"closes_column_list": {TableID: "db.t", Columns: []lthash.Column{
			{Name: "v", Type: "String) ENGINE = MergeTree ORDER BY tuple() --"},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := Intents(testPinnedStatic(), schema); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("Intents error = %v, want ErrUnsupportedColumnType", err)
			}
			if _, _, _, err := BuildDDL(testPinnedStatic(), schema); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("BuildDDL error = %v, want ErrUnsupportedColumnType", err)
			}
		})
	}
}

// The partition freeze is checked before column types so an expression
// partition key keeps reporting ErrPartitionFreeze, which callers (ensure)
// treat as skip-with-warning rather than as a hard failure.
func TestIntents_PartitionFreezeOutranksColumnTypeRejection(t *testing.T) {
	schema := payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "toYYYYMM(d)",
		Columns:     []lthash.Column{{Name: "d", Type: "Date"}},
	}
	_, _, _, err := Intents(testPinnedStatic(), schema)
	if !errors.Is(err, ErrPartitionFreeze) {
		t.Fatalf("Intents error = %v, want ErrPartitionFreeze", err)
	}
}

func TestIntents_AcceptsEveryWhitelistedType(t *testing.T) {
	schema := payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"}, {Name: "f", Type: "FixedString(8)"},
			{Name: "b", Type: "Bool"}, {Name: "f32", Type: "Float32"}, {Name: "f64", Type: "Float64"},
			{Name: "u8", Type: "UInt8"}, {Name: "u16", Type: "UInt16"}, {Name: "u32", Type: "UInt32"}, {Name: "u64", Type: "UInt64"},
			{Name: "i8", Type: "Int8"}, {Name: "i16", Type: "Int16"}, {Name: "i32", Type: "Int32"}, {Name: "i64", Type: "Int64"},
		},
	}
	if _, _, _, err := Intents(testPinnedStatic(), schema); err != nil {
		t.Fatalf("Intents rejected the full whitelist: %v", err)
	}
}
```

Add the helper (the existing docker helper `testPinned(t)` derives unique names per test; this one is the static pure-test pin) next to the other helpers in `build_test.go`:

```go
func testPinnedStatic() Pinned {
	return Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "node-1", KeeperShardID: 0}
}
```

- [ ] **Step 2: Run it and watch it fail**

Run: `bazel test //dataplane/ddl:ddl_test --test_output=errors`
Expected: FAIL — `Intents` returns 3 values, not 4 (the three-intent signature lands in Task 8; write the test against the final shape now and let Step 3 introduce it).

- [ ] **Step 3: Change `Intents` to return three intents and add the type gate**

Replace `dataplane/ddl/build.go:64-103` with:

```go
// Intents derives the hg_unsafe, hg_safe and hg_promote intents for one
// declared schema. Validation order is frozen: partition freeze (callers may
// skip such a declaration), then the reserved row-id column, then the column
// types. Every check runs before any caller can issue DDL.
func Intents(p Pinned, t payloadexec.TableSchema) (TableIntent, TableIntent, TableIntent, error) {
	if err := validatePartitionFreeze(t); err != nil {
		return TableIntent{}, TableIntent{}, TableIntent{}, fmt.Errorf("table %s: %w", t.TableID, err)
	}
	for _, c := range t.Columns {
		if c.Name == RowIDColumn {
			return TableIntent{}, TableIntent{}, TableIntent{}, fmt.Errorf("table %s: declared schema must not contain %s (the protocol injects it)", t.TableID, RowIDColumn)
		}
	}
	// Spec L D1: a declared type outside the storage-integrity profile's own
	// supported set is rejected before any CREATE. A type ClickHouse would
	// accept but the pinned replay executor cannot materialize diverges
	// silently; a type string that closes the column list would add a column
	// and permanently brick the role, because drift is fail-closed and
	// CREATE TABLE IF NOT EXISTS is a no-op against the existing table.
	if err := payloadexec.ValidateTableSchemaColumns(t); err != nil {
		return TableIntent{}, TableIntent{}, TableIntent{}, err
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
	// Spec L D5: hg_promote is the promotion shadow. It was created ad hoc as
	// `CREATE TABLE ... AS hg_safe.<t>`, i.e. structurally identical to hg_safe
	// but in the promote database, so the intent is exactly that — now rendered
	// and verified like the other two instead of appearing at first promotion.
	promote := safe
	promote.Database = p.PromoteDB
	return unsafe, safe, promote, nil
}

// BuildDDL renders the three CREATE TABLE IF NOT EXISTS statements. Pure;
// golden tested.
func BuildDDL(p Pinned, t payloadexec.TableSchema) (string, string, string, error) {
	unsafe, safe, promote, err := Intents(p, t)
	if err != nil {
		return "", "", "", err
	}
	return unsafe.SQL(), safe.SQL(), promote.SQL(), nil
}
```

Add `"github.com/housegate/housegate/pkg/replay/payloadexec"` to the import block if gazelle has not already (it is already imported for `TableSchema`).

- [ ] **Step 4: Update the existing golden tests to the new arity**

Every existing call in `dataplane/ddl/build_test.go` of the form `unsafe, safe, err := BuildDDL(...)` becomes `unsafe, safe, promote, err := BuildDDL(...)`; the two `Intents` callers likewise. Add the promote golden to `TestBuildDDL_GoldenStringPartitionedTable`:

```go
	wantPromote := "CREATE TABLE IF NOT EXISTS `hg_promote`.`db__t` (\n" +
		"    `_hg_row_id` FixedString(32),\n" +
		"    `p` String,\n" +
		"    `v` UInt64\n" +
		") ENGINE = MergeTree\n" +
		"PARTITION BY `p`\n" +
		"ORDER BY (`p`, `_hg_row_id`)\n" +
		"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0"
	if promote != wantPromote {
		t.Fatalf("promote DDL =\n%s\nwant\n%s", promote, wantPromote)
	}
```

- [ ] **Step 5: Fix the in-repo callers of the old arity**

Run: `bazel build //... 2>&1 | head -40`
Expected: the only breakages are `dataplane/ddl/ensure.go:86` (`unsafe, safe, err := Intents(...)`). Change it to `unsafe, safe, promote, err := Intents(...)` and extend the intent loop in the same edit:

```go
		for _, intent := range []TableIntent{unsafe, safe, promote} {
```

- [ ] **Step 6: Run the package tests**

Run: `bazel test //dataplane/ddl:ddl_test //snode:snode_test //verifier:verifier_test --test_output=errors`
Expected: PASS (docker tests skip).

- [ ] **Step 7: Commit**

```bash
git add dataplane/ddl/build.go dataplane/ddl/build_test.go dataplane/ddl/ensure.go
git commit -m "feat(ddl): reject column types outside the storage-integrity whitelist and render hg_promote"
```

### Task 5: arbiter-core — D1/D7 role-config parity: type gate and partition freeze in both roles' `validate()`

**Files:**
- Modify: `snode/config.go:47-105` (`validate`)
- Modify: `verifier/config.go:36-79` (`validate`)
- Modify: `snode/protocol_tables_test.go` (new cases), `verifier/protocol_tables_test.go` (new cases)

**Interfaces:**
- Consumes: `payloadexec.ValidateTableSchemaColumns` (Task 1), `snode.validatePartitionBy` (already in `snode/config.go:113-129`).
- Produces (used by Tasks 7, 13, 28): both `snode.New` and `verifier.New` fail construction for a declaration outside the SI type whitelist **or** outside the P1c partition freeze, with identical wording.

- [ ] **Step 1: Write the failing tests**

Append to `snode/protocol_tables_test.go`:

```go
func TestConfigRejectsColumnTypeOutsideWhitelist(t *testing.T) {
	cfg := testConfigS(t)
	schema := intakeSchema()
	schema.Columns = append(schema.Columns, lthash.Column{Name: "bad", Type: "Nullable(String)"})
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	if err := cfg.validate(); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
		t.Fatalf("validate = %v, want ErrUnsupportedColumnType", err)
	}
}
```

Append to `verifier/protocol_tables_test.go` (this is the D7 parity gap — the verifier has neither check today):

```go
func TestConfigRejectsColumnTypeOutsideWhitelist(t *testing.T) {
	cfg := testConfigV()
	sch := scanTableSchema()
	sch.Columns = append(sch.Columns, lthash.Column{Name: "bad", Type: "Array(UInt64)"})
	cfg.Tables = []payloadexec.TableSchema{sch}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	if err := cfg.validate(); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
		t.Fatalf("validate = %v, want ErrUnsupportedColumnType", err)
	}
}

// Spec L D7: a freeze-violating declaration must fail BOTH roles identically.
// Before this change the SNode refused to start while the verifier started
// happily without protocol tables for that table — the exact half-silent
// failure Spec C §1 set out to eliminate.
func TestConfigRejectsPartitionFreezeViolation(t *testing.T) {
	for name, mutate := range map[string]func(*payloadexec.TableSchema){
		"expression":   func(s *payloadexec.TableSchema) { s.PartitionBy = "toYYYYMM(d)" },
		"non_string":   func(s *payloadexec.TableSchema) { s.Columns[0].Type = "UInt64"; s.PartitionBy = s.Columns[0].Name },
		"undeclared":   func(s *payloadexec.TableSchema) { s.PartitionBy = "nope" },
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfigV()
			sch := scanTableSchema()
			mutate(&sch)
			cfg.Tables = []payloadexec.TableSchema{sch}
			cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
			if err := cfg.validate(); err == nil {
				t.Fatal("verifier accepted a declaration outside the P1c partition freeze")
			}
		})
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `bazel test //snode:snode_test //verifier:verifier_test --test_output=errors`
Expected: FAIL — `validate` returns nil for both new verifier cases and for the snode type case.

- [ ] **Step 3: Move `validatePartitionBy` into `ddl` and call it from both roles**

`snode/config.go`'s `validatePartitionBy` and `ddl`'s `validatePartitionFreeze` are the same rule with different wording. Delete `snode/config.go:113-129` and export the `ddl` one instead. In `dataplane/ddl/build.go`, add above `validatePartitionFreeze`:

```go
// ValidatePartitionFreeze is the role-config entry point for the P1c freeze
// (partition_by must be empty or name a declared bare String column). Roles
// call it so a freeze violation fails both of them identically at startup,
// rather than bricking one and leaving the other silently without protocol
// tables for that table.
func ValidatePartitionFreeze(t payloadexec.TableSchema) error { return validatePartitionFreeze(t) }
```

In `snode/config.go`, replace the per-table loop body with both checks:

```go
	for i, tbl := range c.Tables {
		if err := ddl.ValidatePartitionFreeze(tbl); err != nil {
			errs = append(errs, fmt.Errorf("tables[%d] (%s): %w", i, tbl.TableID, err))
		}
		if err := payloadexec.ValidateTableSchemaColumns(tbl); err != nil {
			errs = append(errs, fmt.Errorf("tables[%d]: %w", i, err))
		}
	}
```

Delete the now-unused `validatePartitionBy` function and the `strings` import if nothing else uses it (`bazel build //snode:snode` will tell you).

In `verifier/config.go`, add the same loop immediately after the `ddl.ValidatePhysicalTableNames(c.Tables)` call:

```go
	for i, tbl := range c.Tables {
		if err := ddl.ValidatePartitionFreeze(tbl); err != nil {
			errs = append(errs, fmt.Errorf("tables[%d] (%s): %w", i, tbl.TableID, err))
		}
		if err := payloadexec.ValidateTableSchemaColumns(tbl); err != nil {
			errs = append(errs, fmt.Errorf("tables[%d]: %w", i, err))
		}
	}
```

- [ ] **Step 4: Run the tests**

Run: `bazel test //snode:snode_test //verifier:verifier_test //dataplane/ddl:ddl_test --test_output=errors`
Expected: PASS. If an existing fixture used a non-whitelisted column type, fix the fixture — do not widen the whitelist.

- [ ] **Step 5: Commit**

```bash
bazel run //:gazelle
git add snode/config.go verifier/config.go dataplane/ddl/build.go snode/protocol_tables_test.go verifier/protocol_tables_test.go
git commit -m "feat(roles): validate SI column types and the partition freeze in both role configs"
```

### Task 6: arbiter-core — D1 docker acceptance: a bad type creates nothing

**Files:**
- Modify: `dataplane/ddl/ensure_ch_test.go`

**Interfaces:**
- Consumes: `EnsureProtocolTables(ctx, conn, pinned, tables, mode, logger)`, `requireCH(t)`, `requireKeeper(t, conn)`, `testPinned(t)`, `dropDatabasesSync(t, conn, p)` (all already in `dataplane/ddl/ch_test.go`).
- Produces: the "permanent brick" regression proof — nothing exists in ClickHouse after a rejected declaration.

- [ ] **Step 1: Write the failing docker test**

```go
// A declared type outside the SI whitelist must be refused BEFORE any DDL
// runs. This is the permanent-brick scenario from Spec L §1a: a type string
// that closes the column list would add a column, VerifyProtocolTable would
// then report drift forever, and CREATE TABLE IF NOT EXISTS is a silent no-op
// against the existing table, so the node could not recover without an
// operator DROP.
func TestEnsureProtocolTables_RejectsBadColumnTypeBeforeCreatingAnything(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	sch.Columns = append(sch.Columns, lthash.Column{
		Name: "evil",
		Type: "String) ENGINE = MergeTree ORDER BY tuple() --",
	})
	err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{sch}, ModeCreateAndVerify, slog.Default())
	if !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
		t.Fatalf("EnsureProtocolTables = %v, want ErrUnsupportedColumnType", err)
	}
	table := CHTableName(sch.TableID)
	for _, database := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
		var n uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM system.tables WHERE database = ? AND name = ?", database, table,
		).Scan(&n); err != nil {
			t.Fatalf("count tables in %s: %v", database, err)
		}
		if n != 0 {
			t.Fatalf("%s.%s exists after a rejected declaration; the role is now permanently bricked", database, table)
		}
	}
}
```

- [ ] **Step 2: Start a Keeper-enabled ClickHouse and run it**

```bash
docker run -d --rm --name hardening-ch -p 9000:9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  clickhouse/clickhouse-server:25.8
ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 CH_ADDR=127.0.0.1:9000 \
  bazel test //dataplane/ddl:ddl_test \
    --test_filter=TestEnsureProtocolTables_RejectsBadColumnTypeBeforeCreatingAnything \
    --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=CH_ADDR \
    --test_timeout=900 --test_output=all
```
Expected after Task 4: PASS. (If you run this before Task 4 is merged into your branch, it fails with `EnsureProtocolTables = <nil>` and the created table — that is the red state this test exists to pin.)

- [ ] **Step 3: Run the whole docker suite for the package**

```bash
ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 CH_ADDR=127.0.0.1:9000 \
  bazel test //dataplane/ddl:ddl_test \
    --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=CH_ADDR \
    --test_timeout=900 --test_output=errors
```
Expected: PASS, every test in the package.

- [ ] **Step 4: Commit**

```bash
git add dataplane/ddl/ensure_ch_test.go
git commit -m "test(ddl): prove a rejected column type creates no ClickHouse table"
```

### Task 7: arbiter-core — D2 schema-source-derived mode with no fail-open zero value

**Files:**
- Modify: `dataplane/ddl/ensure.go:15-56` (`Mode`, add `SchemaSource` + `ModeFromSchemaSource`)
- Modify: `snode/config.go:22-46` and its `validate()`
- Modify: `verifier/config.go:21-34` and its `validate()`
- Modify: `snode/snode.go:120-150`, `verifier/verifier.go:44-130` (read the derived mode)
- Modify: `snode/protocol_tables_test.go`, `verifier/protocol_tables_test.go`, and every in-package test fixture that sets `ProtocolTables`

**Interfaces:**
- Produces (used by Tasks 13, 28):
  - `ddl.SchemaSource` (string type) with constants `ddl.SchemaSourceNetworkState = "network_state"`, `ddl.SchemaSourceChain = "chain"`, `ddl.SchemaSourceClickHouse = "clickhouse"`, `ddl.SchemaSourceUnmanaged = "unmanaged"`.
  - `ddl.ModeFromSchemaSource(source SchemaSource) (Mode, error)`.
  - `snode.Config.SchemaSource ddl.SchemaSource` and `verifier.Config.SchemaSource ddl.SchemaSource` replace the exported `ProtocolTables ddl.Mode` field; the derived mode lives in the unexported `protocolTables` field that `validate()` sets.
  - `snode.Config.ProtocolTablesMode() ddl.Mode` / `verifier.Config.ProtocolTablesMode() ddl.Mode` — read-only accessors for hosts and tests.

- [ ] **Step 1: Write the failing tests**

Append to `dataplane/ddl/ensure_ch_test.go`'s pure-test neighbour `dataplane/ddl/build_test.go` (no ClickHouse needed):

```go
func TestModeFromSchemaSource(t *testing.T) {
	for source, want := range map[SchemaSource]Mode{
		SchemaSourceNetworkState: ModeCreateAndVerify,
		SchemaSourceChain:        ModeCreateAndVerify,
		SchemaSourceClickHouse:   ModeVerifyOnly,
		SchemaSourceUnmanaged:    ModeOff,
	} {
		got, err := ModeFromSchemaSource(source)
		if err != nil || got != want {
			t.Fatalf("ModeFromSchemaSource(%q) = %v, %v; want %v, nil", source, got, err, want)
		}
	}
	for _, bad := range []SchemaSource{"", "CREATE", "network-state", "off"} {
		if _, err := ModeFromSchemaSource(bad); err == nil {
			t.Fatalf("ModeFromSchemaSource(%q) accepted an unknown schema source", bad)
		}
	}
}
```

Append to `snode/protocol_tables_test.go`:

```go
func TestConfigRequiresSchemaSource(t *testing.T) {
	cfg := testConfigS(t)
	cfg.SchemaSource = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("an unset schema_source must be rejected; the old zero value silently disabled the lifecycle")
	}
}

func TestConfigDerivesProtocolTableMode(t *testing.T) {
	cfg := testConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceClickHouse
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got := cfg.ProtocolTablesMode(); got != ddl.ModeVerifyOnly {
		t.Fatalf("clickhouse schema source derived %v, want verify", got)
	}
	cfg = testConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got := cfg.ProtocolTablesMode(); got != ddl.ModeCreateAndVerify {
		t.Fatalf("network_state schema source derived %v, want create", got)
	}
}
```

Append the same two tests to `verifier/protocol_tables_test.go` using `testConfigV()` (no `t` argument) and `cfg.ProtocolTablesMode()`.

- [ ] **Step 2: Run and watch them fail**

Run: `bazel test //dataplane/ddl:ddl_test //snode:snode_test //verifier:verifier_test --test_output=errors`
Expected: FAIL — `undefined: SchemaSource`, `undefined: ModeFromSchemaSource`, `cfg.SchemaSource undefined`.

- [ ] **Step 3: Add the schema-source type to `ddl`**

In `dataplane/ddl/ensure.go`, after the `Mode` block:

```go
// SchemaSource names where a role's authoritative table schemas come from.
// Spec L D2: the protocol-table mode is DERIVED from it, so a deployment can
// never silently end up with the lifecycle disabled by omitting a field.
type SchemaSource string

const (
	// SchemaSourceNetworkState resolves schemas from the network-state
	// registry; the role may create protocol tables.
	SchemaSourceNetworkState SchemaSource = "network_state"
	// SchemaSourceChain resolves schemas from the on-chain declaration; the
	// role may create protocol tables.
	SchemaSourceChain SchemaSource = "chain"
	// SchemaSourceClickHouse derives schemas from the local ClickHouse, so the
	// role can only verify: creating from what it reads would be circular.
	SchemaSourceClickHouse SchemaSource = "clickhouse"
	// SchemaSourceUnmanaged is TEST/HARNESS ONLY: the host owns protocol DDL.
	// Production config loaders must reject it; it exists so in-package tests
	// that create their own tables keep a way to express that intent
	// explicitly instead of relying on a fail-open zero value.
	SchemaSourceUnmanaged SchemaSource = "unmanaged"
)

// ModeFromSchemaSource is the only supported way to obtain a Mode for a role.
func ModeFromSchemaSource(source SchemaSource) (Mode, error) {
	switch source {
	case SchemaSourceNetworkState, SchemaSourceChain:
		return ModeCreateAndVerify, nil
	case SchemaSourceClickHouse:
		return ModeVerifyOnly, nil
	case SchemaSourceUnmanaged:
		return ModeOff, nil
	default:
		return ModeOff, fmt.Errorf("ddl: unknown schema source %q (want network_state|chain|clickhouse, or unmanaged in tests)", source)
	}
}
```

- [ ] **Step 4: Replace the config field in both roles**

In `snode/config.go`, delete the `ProtocolTables ddl.Mode` field and add:

```go
	// SchemaSource names where Tables came from. It derives the protocol-table
	// mode (Spec L D2); there is no configurable mode and no fail-open zero.
	SchemaSource ddl.SchemaSource
	// protocolTables is the derived mode; validate() sets it.
	protocolTables ddl.Mode
```

In `validate()`, add (before the `SchemaRoot` cross-check):

```go
	mode, modeErr := ddl.ModeFromSchemaSource(c.SchemaSource)
	if modeErr != nil {
		errs = append(errs, modeErr)
	} else {
		c.protocolTables = mode
	}
```

and add the accessor:

```go
// ProtocolTablesMode returns the mode derived from SchemaSource. Valid only
// after validate() has run (New does that before anything else).
func (c Config) ProtocolTablesMode() ddl.Mode { return c.protocolTables }
```

Apply the identical change to `verifier/config.go`.

- [ ] **Step 5: Read the derived mode everywhere the field was read**

- `snode/snode.go:127` `ensureProtocolTables`: `return r.ensureProtocolTablesMode(ctx, r.cfg.protocolTables)`.
- `snode/snode.go:120` `Run`: `if r.cfg.protocolTables == ddl.ModeOff {`.
- `verifier/verifier.go:53` `New`: `if cfg.ProtocolTablesMode() != ddl.ModeOff && d.Conn == nil {` — note `New` calls `cfg.validate()` first, and `validate` has a pointer receiver, so the derived value is present. Keep that ordering.
- `verifier/verifier.go:110,118` likewise.

- [ ] **Step 6: Update in-package test fixtures**

Every `cfg.ProtocolTables = ddl.ModeCreateAndVerify` becomes `cfg.SchemaSource = ddl.SchemaSourceNetworkState`; `= ddl.ModeVerifyOnly` becomes `= ddl.SchemaSourceClickHouse`. Every fixture that previously relied on the zero value (`testConfigS`, `testConfigV`, and the intake/promote/converge test configs) gets `SchemaSource: ddl.SchemaSourceUnmanaged` so its intent is explicit.

Run: `bazel build //... 2>&1 | grep -n "ProtocolTables" | head -20` and fix every hit.

- [ ] **Step 7: Run the tests**

Run: `bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add dataplane/ddl/ensure.go dataplane/ddl/build_test.go snode verifier
git commit -m "feat(roles): derive the protocol-table mode from the schema source"
```

### Task 8: arbiter-core — D5 `hg_promote` creation, verification and promotion-path failure

**Files:**
- Modify: `dataplane/ddl/ensure.go:84-106` (already extended in Task 4 Step 5 — this task adds the docker proof and the promotion-path change)
- Modify: `snode/promote_replace.go:231-241` (`prepareShadow`)
- Modify: `snode/promote_test.go`, `dataplane/ddl/ensure_ch_test.go`

**Interfaces:**
- Consumes: `ddl.Intents` three-intent form (Task 4).
- Produces: `snode.ErrPromoteTableMissing` — returned by `prepareShadow` when `hg_promote.<t>` is absent.

- [ ] **Step 1: Write the failing tests**

In `dataplane/ddl/ensure_ch_test.go`:

```go
func TestEnsureProtocolTables_CreatesAndVerifiesPromoteTable(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	tables := []payloadexec.TableSchema{sch}
	if err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("ensure: %v", err)
	}
	table := CHTableName(sch.TableID)
	var engine string
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", p.PromoteDB, table).Scan(&engine); err != nil {
		t.Fatalf("hg_promote table missing after create: %v", err)
	}
	if engine != EngineMergeTree {
		t.Fatalf("hg_promote engine = %q, want MergeTree", engine)
	}
	// D5: promote drift is detected at startup, not at first promotion.
	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1", p.PromoteDB, table)); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default())
	if !errors.Is(err, ErrProtocolTableDrift) || !strings.Contains(err.Error(), p.PromoteDB) {
		t.Fatalf("ensure after promote tamper = %v, want drift naming %s", err, p.PromoteDB)
	}
}
```

In `snode/promote_test.go`:

```go
func TestPrepareShadow_FailsClearlyWhenPromoteTableIsAbsent(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	r, cmd, sch, table := newPromoteFixtureWithoutPromoteTable(t, conn)
	safe := r.cfg.SafeDatabase + "." + table
	promote := r.cfg.PromoteDatabase + "." + table
	partition, err := quotePartition(sch, cmd.PartitionID)
	if err != nil {
		t.Fatal(err)
	}
	err = r.prepareShadow(ctx, cmd, sch, table, safe, promote, partition)
	if !errors.Is(err, ErrPromoteTableMissing) {
		t.Fatalf("prepareShadow = %v, want ErrPromoteTableMissing", err)
	}
	if !strings.Contains(err.Error(), promote) {
		t.Fatalf("error %q does not name the missing table", err)
	}
}
```

`newPromoteFixtureWithoutPromoteTable` builds the same fixture the existing promote docker tests use (see `snode/promote_fixtures_test.go`) but runs `EnsureProtocolTables` with only the unsafe/safe intents — simplest implementation: call the existing fixture builder, then `DROP TABLE <promote> SYNC` before the assertion.

- [ ] **Step 2: Run and watch them fail**

```bash
ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 CH_ADDR=127.0.0.1:9000 \
  bazel test //dataplane/ddl:ddl_test //snode:snode_test \
    --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=CH_ADDR \
    --test_timeout=900 --test_output=errors
```
Expected: FAIL — `undefined: ErrPromoteTableMissing`; the promote table exists only because `prepareShadow` creates it.

- [ ] **Step 3: Stop creating `hg_promote` ad hoc**

In `snode/promote_replace.go`, add the sentinel next to the other snode errors (`snode/staged.go:47-57` holds the rest; keep this one with the promotion code):

```go
// ErrPromoteTableMissing means the protocol-owned hg_promote table does not
// exist. Spec L D5 moved it into EnsureProtocolTables, so its absence is a
// startup-detectable condition rather than a first-promotion surprise; the
// promotion path no longer creates it, because an ad-hoc CREATE would bypass
// the pinned DDL and its drift detection.
var ErrPromoteTableMissing = errors.New("snode: hg_promote table is missing; run the role with a create-capable schema source so EnsureProtocolTables can build it")
```

Replace the first statement of `prepareShadow`:

```go
func (r *Role) prepareShadow(ctx context.Context, cmd arbiter.PromoteSafePartition, sch payloadexec.TableSchema, table, safe, promote, partition string) error {
	var exists uint64
	if err := r.d.Conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = ? AND name = ?", r.cfg.PromoteDatabase, table,
	).Scan(&exists); err != nil {
		return fmt.Errorf("snode: check %s: %w", promote, err)
	}
	if exists == 0 {
		return fmt.Errorf("%w: %s", ErrPromoteTableMissing, promote)
	}
	if err := r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partition); err != nil {
		return err
	}
	if cmd.BasePartitionRoot == "" {
		return nil
	}
	return r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", promote, partition, safe))
}
```

- [ ] **Step 4: Make the existing promote fixtures create the promote table through `ddl`**

Every promote docker fixture that relied on the lazy CREATE must now run `ddl.EnsureProtocolTables(..., ddl.ModeCreateAndVerify, ...)` (or create the three databases and call it) during setup. Grep for fixtures that create `hg_safe`/`hg_unsafe` by hand:

Run: `grep -rn "CREATE TABLE" snode/*_test.go | head -20`
Expected: a handful of fixture builders; switch each to `EnsureProtocolTables` so fixture and production DDL cannot drift.

- [ ] **Step 5: Run the docker suite**

```bash
ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1 \
  CH_ADDR=127.0.0.1:9000 CH_REPLICA_ADDR=127.0.0.1:9001 \
  bazel test //dataplane/ddl:ddl_test //snode:snode_test //verifier:verifier_test \
    --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=ARBITER_CH_REPLICA \
    --test_env=CH_ADDR --test_env=CH_REPLICA_ADDR --test_timeout=900 --test_output=errors
```
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dataplane/ddl snode
git commit -m "feat(snode): move hg_promote into the protocol-table lifecycle"
```

### Task 9: arbiter-core — D4 reconcile distinguishes drift from transient failure

**Files:**
- Create: `dataplane/ddl/reconcile.go`, `dataplane/ddl/reconcile_test.go`
- Modify: `snode/snode.go:103-196`, `verifier/verifier.go:86-170`
- Modify: `snode/config.go`, `verifier/config.go` (one new field)
- Modify: `snode/protocol_tables_test.go`, `verifier/protocol_tables_test.go`

**Interfaces:**
- Produces (used by Task 11):
  - `ddl.FatalReconcileError(err error) bool` — true for `ErrProtocolTableDrift`, `ErrProtocolTableMissing`, `ErrPhysicalTableNameCollision`, `ErrPartitionFreeze` and `payloadexec.ErrUnsupportedColumnType`.
  - `ddl.ReconcileBackoff(consecutiveFailures int, interval time.Duration) time.Duration`.
  - `ddl.DefaultReconcileMaxFailures = 5`.
  - `snode.Config.ProtocolTablesMaxFailures int` / `verifier.Config.ProtocolTablesMaxFailures int` (0 = default).

- [ ] **Step 1: Write the failing unit test** `dataplane/ddl/reconcile_test.go`

```go
package ddl

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestFatalReconcileError(t *testing.T) {
	fatal := []error{
		fmt.Errorf("wrapped: %w", ErrProtocolTableDrift),
		fmt.Errorf("wrapped: %w", ErrProtocolTableMissing),
		fmt.Errorf("wrapped: %w", ErrPhysicalTableNameCollision),
		fmt.Errorf("wrapped: %w", ErrPartitionFreeze),
		fmt.Errorf("wrapped: %w", payloadexec.ErrUnsupportedColumnType),
	}
	for _, err := range fatal {
		if !FatalReconcileError(err) {
			t.Errorf("FatalReconcileError(%v) = false, want true", err)
		}
	}
	transient := []error{
		errors.New("read tcp 127.0.0.1:9000: connection reset by peer"),
		fmt.Errorf("ddl: read system.tables for hg_unsafe.db__t: %w", errors.New("EOF")),
		errors.New("code: 210, message: Connection refused"),
	}
	for _, err := range transient {
		if FatalReconcileError(err) {
			t.Errorf("FatalReconcileError(%v) = true, want false", err)
		}
	}
}

func TestReconcileBackoff_IsBoundedByTheInterval(t *testing.T) {
	interval := 60 * time.Second
	got := []time.Duration{
		ReconcileBackoff(1, interval),
		ReconcileBackoff(2, interval),
		ReconcileBackoff(3, interval),
		ReconcileBackoff(20, interval),
	}
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, interval}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ReconcileBackoff(%d) = %s, want %s", i+1, got[i], want[i])
		}
	}
	if d := ReconcileBackoff(20, 500*time.Millisecond); d != 500*time.Millisecond {
		t.Fatalf("backoff must never exceed the interval, got %s", d)
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `bazel test //dataplane/ddl:ddl_test --test_output=errors`
Expected: FAIL — `undefined: FatalReconcileError`, `undefined: ReconcileBackoff`.

- [ ] **Step 3: Write `dataplane/ddl/reconcile.go`**

```go
package ddl

import (
	"errors"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// DefaultReconcileMaxFailures bounds consecutive transient reconcile failures
// before the role gives up and exits.
const DefaultReconcileMaxFailures = 5

// reconcileBackoffMin is the first retry delay after a transient failure.
const reconcileBackoffMin = time.Second

// FatalReconcileError reports whether a reconcile error means the deployment
// is wrong (Spec L D4) rather than temporarily unreachable. Drift, a missing
// table, a naming collision, a freeze violation and an unsupported column type
// are all facts about the declaration or the database: retrying cannot fix
// them and the role must fail closed. Everything else — a ClickHouse restart,
// a dropped connection, a timeout — is retried with bounded backoff.
func FatalReconcileError(err error) bool {
	return errors.Is(err, ErrProtocolTableDrift) ||
		errors.Is(err, ErrProtocolTableMissing) ||
		errors.Is(err, ErrPhysicalTableNameCollision) ||
		errors.Is(err, ErrPartitionFreeze) ||
		errors.Is(err, payloadexec.ErrUnsupportedColumnType)
}

// ReconcileBackoff is the delay before retry number consecutiveFailures. It
// doubles from one second and is capped by the reconcile interval, so a
// retrying role never polls faster than its steady-state cadence for long.
func ReconcileBackoff(consecutiveFailures int, interval time.Duration) time.Duration {
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}
	backoff := reconcileBackoffMin
	for i := 1; i < consecutiveFailures; i++ {
		backoff *= 2
		if backoff >= interval {
			return interval
		}
	}
	if backoff > interval {
		return interval
	}
	return backoff
}
```

- [ ] **Step 4: Write the failing role tests**

Append to `snode/protocol_tables_test.go`:

```go
// A ClickHouse blip during reconcile must not kill the role; real drift must.
func TestReconcile_RetriesTransientErrorsAndDiesOnDrift(t *testing.T) {
	role, conn := newReconcileFixture(t) // fake metadata conn, see Step 5
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.reconcileProtocolTables(ctx) }()

	conn.failNext(errors.New("read tcp 127.0.0.1:9000: connection reset by peer"))
	conn.waitForAttempts(t, 2) // the retry proves the loop survived
	select {
	case err := <-done:
		t.Fatalf("role died on a transient error: %v", err)
	default:
	}

	conn.failNext(fmt.Errorf("wrapped: %w", ddl.ErrProtocolTableDrift))
	select {
	case err := <-done:
		if !errors.Is(err, ddl.ErrProtocolTableDrift) {
			t.Fatalf("reconcile returned %v, want drift", err)
		}
	case <-ctx.Done():
		t.Fatal("role survived real drift")
	}
}

func TestReconcile_GivesUpAfterMaxConsecutiveTransientFailures(t *testing.T) {
	role, conn := newReconcileFixture(t)
	role.cfg.ProtocolTablesMaxFailures = 2
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn.failAlways(errors.New("connection refused"))
	err := role.reconcileProtocolTables(ctx)
	if err == nil || errors.Is(err, ddl.ErrProtocolTableDrift) {
		t.Fatalf("reconcile = %v, want a transient-exhaustion error", err)
	}
	if got := conn.attempts(); got != 2 {
		t.Fatalf("attempts = %d, want exactly ProtocolTablesMaxFailures", got)
	}
}
```

- [ ] **Step 5: Implement the loop and its fixture**

`newReconcileFixture(t)` builds a `*Role` whose `ensureProtocolTablesMode` is driven by an injectable function so the test needs no ClickHouse. Add to `snode/snode.go` an unexported seam used by the loop (production value set in `New`):

```go
type Role struct {
	// ... existing fields ...
	// ensureFn is the reconcile seam. Production wiring is
	// ensureProtocolTablesMode; tests inject a scripted failure sequence.
	ensureFn func(context.Context, ddl.Mode) error
}
```

In `New`, after building the role: `r.ensureFn = r.ensureProtocolTablesMode`.

Replace `reconcileProtocolTables` in both roles with the shared shape (snode version shown; the verifier version is identical with `verifier:` in the messages):

```go
func (r *Role) reconcileProtocolTables(ctx context.Context) error {
	interval := r.cfg.ProtocolTablesReconcile
	maxFailures := r.cfg.ProtocolTablesMaxFailures
	if maxFailures <= 0 {
		maxFailures = ddl.DefaultReconcileMaxFailures
	}
	consecutive := 0
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		err := r.ensureFn(ctx, ddl.ModeVerifyOnly)
		switch {
		case err == nil:
			consecutive = 0
			timer.Reset(interval)
		case ctx.Err() != nil:
			return ctx.Err()
		case ddl.FatalReconcileError(err):
			return fmt.Errorf("snode: reconcile protocol tables: %w", err)
		default:
			consecutive++
			if consecutive >= maxFailures {
				return fmt.Errorf("snode: reconcile protocol tables failed %d consecutive times: %w", consecutive, err)
			}
			backoff := ddl.ReconcileBackoff(consecutive, interval)
			r.d.Logger.Warn("protocol table reconcile failed; retrying",
				"consecutive_failures", consecutive, "max_failures", maxFailures, "retry_in", backoff, "err", err)
			timer.Reset(backoff)
		}
	}
}
```

Add `ProtocolTablesMaxFailures int` to both configs, validated as `>= 0` next to `ProtocolTablesReconcile`.

- [ ] **Step 6: `Run` ensures before entering the loop**

In `snode/snode.go`'s `Run`, immediately after `convergeStartup` succeeds and before `runSubscription` is launched:

```go
	// Spec L D4: the ticker first fires at t+interval, so a Run without a
	// preceding Register — or a re-Run after a transient failure — would spend
	// a whole interval unverified while convergeStartup already touched
	// ClickHouse. Ensure once here; it is idempotent with Register's call.
	if err := r.ensureProtocolTables(ctx); err != nil {
		return err
	}
```

Place it **before** `convergeStartup(runCtx)` so no ClickHouse work precedes verification, and adjust the existing call order accordingly. Apply the same to `verifier.Role.Run` (which has no converge step; ensure first, then subscribe).

- [ ] **Step 7: Fix the subscription-cancellation error fidelity**

In both roles' `runWithProtocolTableReconcile`, the subscription-first branch must always return the subscription error:

```go
	case subscriptionErr := <-subscriptionDone:
		cancel()
		// We cancelled the reconcile ourselves; whatever it reports now is an
		// artifact of that cancellation, not a diagnosis. Log it and return the
		// real cause. (The reconcile-first branch below is where a reconcile
		// error is authoritative.)
		if reconcileErr := <-reconcileDone; reconcileErr != nil && !errors.Is(reconcileErr, context.Canceled) {
			r.d.Logger.Warn("protocol table reconcile stopped while the subscription was failing", "err", reconcileErr)
		}
		return subscriptionErr
```

- [ ] **Step 8: Run the tests**

Run: `bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors`
Expected: PASS, including the existing `TestRun_SubscriptionFailureJoinsReconcileBeforeReturn` and `TestRun_ReconcileIsVerifyOnlyAndDroppedTableFailsClosed`.

- [ ] **Step 9: Commit**

```bash
bazel run //:gazelle
git add dataplane/ddl/reconcile.go dataplane/ddl/reconcile_test.go snode verifier
git commit -m "feat(roles): retry transient reconcile failures and ensure protocol tables at Run"
```

### Task 10: arbiter-core — docker acceptance for D4 + D5 startup drift

**Files:**
- Modify: `snode/protocol_tables_test.go`

**Interfaces:**
- Consumes: everything from Tasks 7–9.

- [ ] **Step 1: Write the docker test**

```go
// D5 acceptance: hg_promote drift is a startup failure, not a first-promotion
// surprise. D4 acceptance: the role that survives it is the one whose
// reconcile can tell drift from a blip.
func TestRegister_FailsClosedOnPromoteTableDrift(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	role, cfg := newRegisteredRoleFixture(t, conn) // creates all three tables
	table := ddl.CHTableName(cfg.Tables[0].TableID)
	if err := conn.Exec(ctx, fmt.Sprintf(
		"ALTER TABLE %s.%s MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1", cfg.PromoteDatabase, table,
	)); err != nil {
		t.Fatalf("tamper promote: %v", err)
	}
	err := role.Register(ctx)
	if !errors.Is(err, ddl.ErrProtocolTableDrift) {
		t.Fatalf("Register after promote drift = %v, want ErrProtocolTableDrift", err)
	}
	if !strings.Contains(err.Error(), cfg.PromoteDatabase) {
		t.Fatalf("drift error %q does not name the promote database", err)
	}
}
```

`newRegisteredRoleFixture` reuses the existing `TestRegister_EnsuresProtocolTablesThenFailsClosedOnDrift` setup verbatim (fake gRPC server + `setUniqueDatabases` + `cfg.SchemaSource = ddl.SchemaSourceNetworkState`), returning the role and its config after one successful `Register`.

- [ ] **Step 2: Run it**

```bash
ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 CH_ADDR=127.0.0.1:9000 \
  bazel test //snode:snode_test --test_filter=TestRegister_FailsClosedOnPromoteTableDrift \
    --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=CH_ADDR \
    --test_timeout=900 --test_output=all
```
Expected: PASS.

- [ ] **Step 3: Full docker suite, then commit**

```bash
ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1 \
  CH_ADDR=127.0.0.1:9000 CH_REPLICA_ADDR=127.0.0.1:9001 \
  bazel test //dataplane/ddl:ddl_test //snode:snode_test //verifier:verifier_test \
    --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=ARBITER_CH_REPLICA \
    --test_env=CH_ADDR --test_env=CH_REPLICA_ADDR --test_timeout=900 --test_output=errors
git add snode/protocol_tables_test.go
git commit -m "test(snode): prove hg_promote drift fails closed at startup"
```

### Task 11: arbiter-core — README, PR and release

**Files:**
- Modify: `README.md`
- No CI change: `//dataplane/ddl:ddl_test`, `//snode:snode_test`, `//verifier:verifier_test` are already in `.github/workflows/ci.yml`'s explicit list.

**Interfaces:**
- Produces (used by Tasks 12, 28): `ARBITER_CORE_TAG_B` and `ARBITER_CORE_COMMIT_B`.

- [ ] **Step 1: Document the three behaviour changes in `README.md`**

Add one paragraph each (no hard wrapping): declared column types are validated against the storage-integrity whitelist before any DDL and a violation fails role construction; the protocol-table mode is derived from `schema_source` (`network_state`/`chain` create, `clickhouse` verifies) and there is no configurable mode; `hg_promote` is now created and verified with the other two tables, so a verify-only node that was never bootstrapped in create mode fails at startup instead of at first promotion.

- [ ] **Step 2: Full local gate**

```bash
bazel build //...
bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
```
Expected: PASS both.

- [ ] **Step 3: Open the PR**

```bash
git push -u origin feat/protocol-table-hardening
gh pr create --title "feat(storage-integrity): protocol table hardening (Spec L D1/D2/D4/D5/D7)" \
  --body "$(cat <<'EOF'
Spec L: docs/superpowers/specs/2026-08-19-storage-integrity-table-backpressure-hardening-design.md (housegate repo).

- D1: ddl.Intents validates declared column types against payloadexec's SI whitelist before any CREATE; both roles' validate() do the same. Docker test proves no table is created for a rejected declaration.
- D2: ProtocolTables ddl.Mode is replaced by SchemaSource; the mode is derived in validate() and ModeOff is only reachable through the explicit test-only "unmanaged" source.
- D4: reconcile retries transient ClickHouse errors with bounded backoff and dies only on drift/collision/freeze/type errors; Run ensures before entering its loop; the subscription-first branch returns the subscription error.
- D5: hg_promote is rendered, created and verified with hg_unsafe/hg_safe; promote_replace no longer creates it and fails with ErrPromoteTableMissing.
- D7: the verifier now enforces the P1c partition freeze exactly like the SNode.

Operator note: an hg_promote table created by the old ad-hoc path is now verified. If a deployment has one whose settings differ from the hg_safe pins, drop it before upgrading; the role will recreate it.
EOF
)"
```

- [ ] **Step 4: Merge, then cut the release**

```bash
gh workflow run cut-release.yml --ref main
gh run watch "$(gh run list --workflow=cut-release.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
git fetch --tags
TAG=$(git describe --tags --abbrev=0 origin/main)
echo "ARBITER_CORE_TAG_B=$TAG"
echo "ARBITER_CORE_COMMIT_B=$(git rev-list -n 1 "$TAG")"
```
Expected: a non-draft, non-prerelease tag whose annotated tag peels to the merge commit. Record both values.

---

## Phase C — arbiter reference binaries: `-ensure-tables` becomes a check

### Task 12: arbiter — consume the derived mode and turn `-ensure-tables` into a cross-check

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/arbiter` (branch `feat/ensure-tables-check` off `origin/main`).

**Files:**
- Modify: `go.mod:20` (arbiter-core pin), `MODULE.bazel:9-32` (`bazel_dep` version + `git_override` commit)
- Modify: `cmd/arbiter-snode/main.go:20,59-77,100`, `cmd/arbiter-snode/config.go:214-223`
- Modify: `cmd/arbiter-verifier/main.go:30,63-79,100`, `cmd/arbiter-verifier/config.go:228-238`
- Modify: `cmd/arbiter-snode/main_test.go:133-146`, `cmd/arbiter-verifier/main_test.go:137-150`
- Modify: `README.md:104-120`

**Interfaces:**
- Consumes: `ddl.SchemaSource*`, `ddl.ModeFromSchemaSource`, `snode.Config.SchemaSource`, `verifier.Config.SchemaSource`, three-return `ddl.BuildDDL` (Tasks 4, 7).
- Produces (used by Task 13): both binaries refuse to start when `-ensure-tables` disagrees with the config's schema source.

- [ ] **Step 1: Bump the arbiter-core pin**

```bash
go get github.com/sentioxyz/arbiter-core@"$ARBITER_CORE_TAG_B"
```
Then set `MODULE.bazel`'s `bazel_dep(name = "arbiter_core", version = "X.Y.Z")` and its `git_override` commit to `ARBITER_CORE_COMMIT_B`, updating the comment. Also bump the `housegate` `bazel_dep`/`git_override`/`go.mod` entries to `HOUSEGATE_TAG_A` / `HOUSEGATE_COMMIT_A` (arbiter still pins v0.9.0, and arbiter-core now requires the newer one — a stale pin would resolve to two housegate versions).

Run: `bazel mod tidy && bazel run //:gazelle && bazel build //...`
Expected: PASS.

- [ ] **Step 2: Write the failing tests**

Replace `TestDefaultEnsureMode_FollowsSchemaSource` in **both** `main_test.go` files with:

```go
func TestSchemaSourceForConfig(t *testing.T) {
	cfg, err := loadConfig(writeSNodeConfig(t, validSNodeConfig(t)))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if got := schemaSourceFor(cfg); got != ddl.SchemaSourceNetworkState {
		t.Fatalf("inline tables schema source = %q, want network_state", got)
	}
	cfg.Tables = nil
	cfg.TableIDs = []string{"db.t"}
	if got := schemaSourceFor(cfg); got != ddl.SchemaSourceClickHouse {
		t.Fatalf("table_ids schema source = %q, want clickhouse", got)
	}
}

// Spec L D2: -ensure-tables is a CHECK, never an override. A flag that
// disagrees with the schema source names both and refuses to start.
func TestResolveEnsureMode_RejectsDisagreementWithSchemaSource(t *testing.T) {
	cfg, err := loadConfig(writeSNodeConfig(t, validSNodeConfig(t)))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	cfg.Tables = nil
	cfg.TableIDs = []string{"db.t"}

	if _, err := resolveEnsureMode(cfg, ""); err != nil {
		t.Fatalf("empty flag must accept the derived mode: %v", err)
	}
	if _, err := resolveEnsureMode(cfg, "verify"); err != nil {
		t.Fatalf("agreeing flag must be accepted: %v", err)
	}
	err = func() error { _, err := resolveEnsureMode(cfg, "create"); return err }()
	if err == nil {
		t.Fatal("-ensure-tables=create with a table_ids config must be refused")
	}
	for _, want := range []string{"create", "clickhouse", "table_ids"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
	if _, err := resolveEnsureMode(cfg, "off"); err == nil {
		t.Fatal("-ensure-tables=off must be refused in a reference binary")
	}
}
```

(The verifier copy is identical with `writeVerifierConfig` / `validVerifierConfig`.)

- [ ] **Step 3: Run and watch them fail**

Run: `bazel test //cmd/arbiter-snode:arbiter-snode_test //cmd/arbiter-verifier:arbiter-verifier_test --test_output=errors`
Expected: FAIL — `undefined: schemaSourceFor`, `undefined: resolveEnsureMode`.

- [ ] **Step 4: Implement in both `main.go` files**

Replace `defaultEnsureMode` with:

```go
// schemaSourceFor maps the config's single schema source onto the arbiter-core
// enum. Inline `tables` is an authoritative declaration the role may create
// from; `table_ids` derives columns from the local ClickHouse, so creation
// would be circular and the role can only verify.
func schemaSourceFor(cfg Config) ddl.SchemaSource {
	if len(cfg.TableIDs) > 0 {
		return ddl.SchemaSourceClickHouse
	}
	return ddl.SchemaSourceNetworkState
}

// resolveEnsureMode derives the protocol-table mode from the schema source and
// treats -ensure-tables as a cross-check (Spec L D2). A flag that disagrees
// with the config refuses to start and names both; "off" is never accepted in
// a reference binary, because a silently disabled lifecycle is exactly the
// fail-open the derivation exists to remove.
func resolveEnsureMode(cfg Config, flagValue string) (ddl.Mode, error) {
	source := schemaSourceFor(cfg)
	derived, err := ddl.ModeFromSchemaSource(source)
	if err != nil {
		return ddl.ModeOff, err
	}
	if flagValue == "" {
		return derived, nil
	}
	requested, err := ddl.ParseMode(flagValue)
	if err != nil {
		return ddl.ModeOff, err
	}
	if requested != derived {
		return ddl.ModeOff, fmt.Errorf(
			"-ensure-tables=%s disagrees with the configured schema source %q (%s config implies -ensure-tables=%s); remove the flag or fix the config",
			flagValue, source, schemaSourceConfigKey(cfg), derived,
		)
	}
	return derived, nil
}

func schemaSourceConfigKey(cfg Config) string {
	if len(cfg.TableIDs) > 0 {
		return "table_ids"
	}
	return "tables"
}
```

Change the flag's help text and its use site:

```go
	ensureTables := flag.String("ensure-tables", "", "cross-check the derived protocol table DDL mode: verify|create (must agree with the config's schema source)")
	...
	mode, err := resolveEnsureMode(cfg, *ensureTables)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(2)
	}
```

- [ ] **Step 5: Pass the schema source into the role config**

In `cmd/arbiter-snode/config.go`'s `toRoleConfig`, replace `ProtocolTables: mode` with `SchemaSource: source` and change the signature to `func (c Config) toRoleConfig(tables []payloadexec.TableSchema, source ddl.SchemaSource) snode.Config`. Update the single call site in `run(...)`. Do the same in `cmd/arbiter-verifier/config.go`. `mode` is still needed by nothing else in `run` — delete the parameter threading if it becomes unused.

- [ ] **Step 6: Fix `printDDL` for the three-statement `BuildDDL`**

In both `main.go` files:

```go
	for _, table := range tables {
		unsafe, safe, promote, err := ddl.BuildDDL(pinned, table)
		if err != nil {
			return fmt.Errorf("table %s: %w", table.TableID, err)
		}
		if _, err := fmt.Fprintf(w, "%s;\n\n%s;\n\n%s;\n\n", unsafe, safe, promote); err != nil {
			return err
		}
	}
```

Extend `TestPrintDDL_PrintsPinnedStatements` in both packages with one more expected substring — the promote CREATE, written exactly like the two the test already checks:

```go
		"CREATE TABLE IF NOT EXISTS `hg_promote`.`db__t`",
```

- [ ] **Step 7: Run the tests**

Run: `bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors`
Expected: PASS.

- [ ] **Step 8: Update `README.md:104-120`**

Rewrite the `-ensure-tables` paragraph: the mode is derived from the schema source (`tables` → create, `table_ids` → verify); `-ensure-tables` only cross-checks and refuses to start on disagreement; `off` is no longer accepted; `-print-ddl` now prints three statements per table.

- [ ] **Step 9: Commit, PR and release**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock cmd README.md
git commit -m "feat(cmd): derive the protocol table mode and make -ensure-tables a check"
git push -u origin feat/ensure-tables-check
gh pr create --title "feat(cmd): derive protocol-table mode from the schema source (Spec L D2)" --body "Consumes arbiter-core $ARBITER_CORE_TAG_B. -ensure-tables no longer overrides the config; it cross-checks the derived mode and refuses to start on disagreement, naming both. -print-ddl now emits hg_promote too."
```
After merge, cut the release the same way as Task 11 Step 4 and record `ARBITER_TAG_C` / `ARBITER_COMMIT_C`.

---

## Phase D — HouseGate: D3 bounded pressure reads, D6 non-closing rejection, D7 name pinning

Phase D is independent of Phases B and C (different repo, different files) and may run in parallel with them once Task 1 has merged. All Phase D tasks share one branch: `feat/pressure-bounded-reads` off `origin/main` in `/Users/uranuswch/Dev/housegate/housegate`.

**Read this before Task 13.** `pkg/storageintegrity/parts_pressure.go` implements a reservation/cleanup-proof protocol whose correctness rests on one property: **absence of an exact part name in `activeParts` is proof that the part is gone.** That is only true for keys the last read actually covered. Every task below preserves that property by tracking, per key, whether the current data came from an exact read and how fresh it is. The four places that consume exact names are:

1. `newReservationLocked` (`:580-582`) captures `baselineParts` / `initialParts` from `g.activeParts[key]` at admission time.
2. `bindCandidatePartsLocked` (`:923`) marks a claim observed when the candidate name is active.
3. `reconcileCandidateClaimsLocked` (`:965-989`) retires a finalized claim only after active → absent.
4. `rebaseLiveReservationsAfterCleanupLocked` (`:1264-1272`) advances a queued reservation only when its captured baseline contained a proven-removed name.

(1) and (4) are why the admission path cannot fall back to aggregate counts: a reservation created against a stale name set would rebase incorrectly. The fix is therefore "read exactly, but only the keys this statement touches", not "read approximately".

### Task 13: housegate — per-key freshness and generation (pure refactor, no query changes)

**Files:**
- Modify: `pkg/storageintegrity/parts_pressure.go`
- Modify: `pkg/storageintegrity/parts_pressure_test.go` (three new tests only; the 49 existing ones must pass unmodified)

**Interfaces:**
- Produces (used by Tasks 14–17):
  - `PartsPressureGuard.keyGeneration map[PartsKey]uint64`, `countFreshAt map[PartsKey]time.Time`, `namesFreshAt map[PartsKey]time.Time` (unexported fields).
  - `func (g *PartsPressureGuard) generationForLocked(key PartsKey) uint64`
  - `func (g *PartsPressureGuard) namesFreshLocked(key PartsKey) bool`
  - `func (g *PartsPressureGuard) invalidateKeysLocked(match func(PartsKey) bool)`
  - `func (g *PartsPressureGuard) checkTableAvailableLocked(table string) error`
  - `partsReservation.visibleAfter` continues to hold a generation per key, now the **per-key** generation.

- [ ] **Step 1: Write the failing tests** — append to `pkg/storageintegrity/parts_pressure_test.go`

```go
func TestPartsPressureGuard_PerKeyGenerationOnlyAdvancesForCoveredKeys(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__a", partition: "p0", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__b", partition: "p0", partitionKey: "p", number: 1},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	keyA := PartsKey{Database: "hg_unsafe", Table: "db__a", Partition: "p_p0"}
	keyB := PartsKey{Database: "hg_unsafe", Table: "db__b", Partition: "p_p0"}
	g.mu.RLock()
	genA, genB := g.generationForLocked(keyA), g.generationForLocked(keyB)
	g.mu.RUnlock()
	if genA == 0 || genB == 0 {
		t.Fatalf("full refresh must advance both keys: a=%d b=%d", genA, genB)
	}
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("second Refresh: %v", err)
	}
	g.mu.RLock()
	gen2A, gen2B := g.generationForLocked(keyA), g.generationForLocked(keyB)
	g.mu.RUnlock()
	if gen2A != genA+1 || gen2B != genB+1 {
		t.Fatalf("generations = %d/%d, want %d/%d", gen2A, gen2B, genA+1, genB+1)
	}
}

func TestPartsPressureGuard_ExpiredKeyFailsClosedPerKey(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 1})
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SnapshotTTL: time.Second})
	now := time.Now()
	g.now = func() time.Time { return now }
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	if err := g.Allow("db__t", "p_p0"); err != nil {
		t.Fatalf("fresh key must be allowed: %v", err)
	}
	now = now.Add(2 * time.Second)
	if err := g.Allow("db__t", "p_p0"); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("expired key must fail closed, got %v", err)
	}
}

func TestPartsPressureGuard_InvalidateKeysDropsOnlyMatchingFreshness(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__a", partition: "p0", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__b", partition: "p0", partitionKey: "p", number: 1},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe"})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	g.mu.Lock()
	g.invalidateKeysLocked(func(key PartsKey) bool { return key.Table == "db__a" })
	g.mu.Unlock()
	if err := g.Allow("db__a", "p_p0"); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("invalidated key must fail closed, got %v", err)
	}
	if err := g.Allow("db__b", "p_p0"); err != nil {
		t.Fatalf("untouched key must stay available: %v", err)
	}
}
```

- [ ] **Step 2: Run and watch them fail**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: FAIL — `g.generationForLocked undefined`, `g.invalidateKeysLocked undefined`, and `TestPartsPressureGuard_ExpiredKeyFailsClosedPerKey` still passes only by accident of the global `takenAt`.

- [ ] **Step 3: Replace the global freshness/generation state**

In the `PartsPressureGuard` struct, delete `snapshotGeneration uint64`, `takenAt time.Time` and `haveSnap bool`; add:

```go
	// Per-key bookkeeping. A read is authoritative only for the keys its scope
	// covered, so "this name is absent" is proof for those keys and silence for
	// every other key. keyGeneration is the visibility barrier reservations
	// compare against; it advances only when a read actually covered the key.
	keyGeneration map[PartsKey]uint64
	countFreshAt  map[PartsKey]time.Time
	namesFreshAt  map[PartsKey]time.Time
	// lastFullOK/lastFullAt describe the last successful full-database pass and
	// feed Snapshot() and the metrics supervisor only.
	lastFullOK bool
	lastFullAt time.Time
```

Initialise the three maps in `NewPartsPressureGuard`.

- [ ] **Step 4: Add the accessors**

```go
func (g *PartsPressureGuard) generationForLocked(key PartsKey) uint64 { return g.keyGeneration[key] }

func (g *PartsPressureGuard) namesFreshLocked(key PartsKey) bool {
	at, ok := g.namesFreshAt[key]
	return ok && g.now().Sub(at) <= g.cfg.SnapshotTTL
}

// invalidateKeysLocked drops freshness for every matching key, making
// admission for those keys fail closed until the next successful read that
// covers them. Counts and names stay in place: they remain the best available
// evidence for reconciliation, they are simply no longer admissible.
func (g *PartsPressureGuard) invalidateKeysLocked(match func(PartsKey) bool) {
	for key := range g.countFreshAt {
		if match(key) {
			delete(g.countFreshAt, key)
		}
	}
	for key := range g.namesFreshAt {
		if match(key) {
			delete(g.namesFreshAt, key)
		}
	}
	if match(PartsKey{Database: g.cfg.UnsafeDatabase}) {
		g.lastFullOK = false
	}
}

// checkTableAvailableLocked holds the fences that are not per key: a failed
// durable projection blocks everything, and a stuck cleanup proof blocks its
// own table (Spec L D7 scoped this from global to per table).
func (g *PartsPressureGuard) checkTableAvailableLocked(table string) error {
	if g.restoreBlocked {
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Kind: "unavailable"}
	}
	if g.pendingCleanupProofForTableLocked(table) {
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Kind: "unavailable"}
	}
	return nil
}

func (g *PartsPressureGuard) pendingCleanupProofForTableLocked(table string) bool {
	for _, reservation := range g.liveReservations {
		if !reservation.cleanupProofPending {
			continue
		}
		for _, key := range reservation.keys {
			if key.Table == table {
				return true
			}
		}
	}
	return false
}
```

Delete `hasPendingCleanupProofLocked`.

- [ ] **Step 5: Rewrite `checkAvailableLocked` per key**

```go
func (g *PartsPressureGuard) checkAvailableLocked(table, partitionID string) error {
	if err := g.checkTableAvailableLocked(table); err != nil {
		return err
	}
	if partitionID == "" {
		return nil
	}
	if !g.namesFreshLocked(PartsKey{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID}) {
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Kind: "unavailable"}
	}
	return nil
}
```

In `newReservationLocked`, the pre-loop `checkAvailableLocked(table, "")` stays (it is now the table-level fence) and the per-partition loop gains the per-key availability check next to the capacity check:

```go
		if enforceLimits {
			if err := g.checkAvailableLocked(table, partitionID); err != nil {
				return nil, err
			}
			if err := g.checkCapacityLocked(table, partitionID); err != nil {
				return nil, err
			}
		}
```

and `visibleAfterGenerations = append(visibleAfterGenerations, g.generationForLocked(key))` replaces the single captured `generation` (move the append below the `key :=` line).

- [ ] **Step 6: Rewrite `Refresh` around a scope-applying helper**

Keep `Refresh`'s external behaviour (full exact inventory of both configured databases) and route it through the new helper so Tasks 14–15 only have to supply a different scope:

```go
// fullScope covers every key in both configured databases.
func (g *PartsPressureGuard) fullScope() PartsScope {
	return PartsScope{Database: g.cfg.UnsafeDatabase, IncludeSafeDatabase: true}
}

// Refresh replaces the cached inventory for every key in both configured
// databases, exactly. It is the startup / RestoreBatch path.
func (g *PartsPressureGuard) Refresh(ctx context.Context) (PartsSnapshot, error) {
	return g.refreshScope(ctx, g.fullScope())
}

// refreshScope reads one scope and installs it as authoritative for exactly the
// keys that scope covers. Keys outside the scope keep their previous counts,
// names, generation and freshness: this read says nothing about them.
func (g *PartsPressureGuard) refreshScope(ctx context.Context, scope PartsScope) (PartsSnapshot, error) {
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	g.commitMu.Lock()
	defer g.commitMu.Unlock()
	refreshCtx, cancel := context.WithTimeout(ctx, g.cfg.RefreshTimeout)
	defer cancel()
	query, args := g.BuildExactPartsQuery(scope)
	rows, err := g.conn.Query(refreshCtx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("storage_integrity: parts snapshot query failed: %w", err)
	}
	defer rows.Close()
	snapshot := PartsSnapshot{}
	inventory := partsInventory{}
	for rows.Next() {
		var database, table, partition, partitionKey, partName string
		if err := rows.Scan(&database, &table, &partition, &partitionKey, &partName); err != nil {
			return nil, fmt.Errorf("storage_integrity: scan parts snapshot: %w", err)
		}
		key := PartsKey{Database: database, Table: table, Partition: LogicalPartitionID(partition, partitionKey == "")}
		snapshot[key]++
		if inventory[key] == nil {
			inventory[key] = map[string]struct{}{}
		}
		inventory[key][partName] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage_integrity: read parts snapshot: %w", err)
	}
	g.mu.Lock()
	g.applyExactScopeLocked(scope, snapshot, inventory)
	g.reconcileCandidateClaimsLocked(scope)
	g.reconcileScopeLocked(scope)
	g.invalidatePendingObservationKeysLocked()
	g.mu.Unlock()
	if err := g.flushCandidateObservations(ctx); err != nil {
		g.mu.Lock()
		g.invalidateKeysLocked(scope.Covers)
		g.mu.Unlock()
		return nil, fmt.Errorf("storage_integrity: persist observed candidate: %w", err)
	}
	g.mu.Lock()
	// A prior inventory may have observed a candidate whose durable marker was
	// only just persisted. Reconcile again so an already-absent finalized claim
	// can retire without waiting for another poll.
	g.reconcileCandidateClaimsLocked(scope)
	g.reconcileScopeLocked(scope)
	g.invalidatePendingObservationKeysLocked()
	result := copyPartsSnapshot(g.snapshot)
	g.mu.Unlock()
	return result, nil
}

// invalidatePendingObservationKeysLocked keeps the pre-existing fail-closed
// rule that the old global haveSnap encoded as len(pendingObserved) == 0: an
// exact candidate whose local observation is not durable yet cannot back an
// admission, because a later restart could not tell delayed visibility from
// historical cleanup. Only the affected keys are fenced.
func (g *PartsPressureGuard) invalidatePendingObservationKeysLocked() {
	if len(g.pendingObserved) == 0 {
		return
	}
	pending := make(map[PartsKey]struct{}, len(g.pendingObserved))
	for _, observation := range g.pendingObserved {
		pending[PartsKey{
			Database:  g.cfg.UnsafeDatabase,
			Table:     PhysicalTableName(observation.candidate.TableID),
			Partition: observation.candidate.PartitionID,
		}] = struct{}{}
	}
	g.invalidateKeysLocked(func(key PartsKey) bool {
		_, ok := pending[key]
		return ok
	})
}

// applyExactScopeLocked installs exact names + counts for the covered keys and
// zeroes covered keys the read did not return (absence within a covered scope
// is proof). Freshness, generation and the pending-proof fences are updated
// only for those keys.
func (g *PartsPressureGuard) applyExactScopeLocked(scope PartsScope, snapshot PartsSnapshot, inventory partsInventory) {
	now := g.now()
	for key := range g.snapshot {
		if scope.Covers(key) {
			if _, present := snapshot[key]; !present {
				delete(g.snapshot, key)
			}
		}
	}
	for key := range g.activeParts {
		if scope.Covers(key) {
			if _, present := inventory[key]; !present {
				delete(g.activeParts, key)
			}
		}
	}
	covered := make(map[PartsKey]struct{}, len(snapshot))
	for key, count := range snapshot {
		g.snapshot[key] = count
		covered[key] = struct{}{}
	}
	for key, names := range inventory {
		g.activeParts[key] = names
		covered[key] = struct{}{}
	}
	for key := range g.countFreshAt {
		if scope.Covers(key) {
			covered[key] = struct{}{}
		}
	}
	for key := range g.namesFreshAt {
		if scope.Covers(key) {
			covered[key] = struct{}{}
		}
	}
	for key := range covered {
		g.keyGeneration[key]++
		g.countFreshAt[key] = now
		g.namesFreshAt[key] = now
	}
	if scope.IsFull(g.cfg) {
		g.lastFullOK = !g.restoreBlocked
		g.lastFullAt = now
	}
}

func (g *PartsPressureGuard) reconcileScopeLocked(scope PartsScope) {
	for key := range g.committedReservations {
		if scope.Covers(key) {
			g.reconcileCommittedKeyLocked(key)
		}
	}
}
```

Note the `covered` set deliberately includes keys that already had freshness inside the scope even when this read returned no rows for them, so a partition that drained to zero still advances its generation and stays admissible.

- [ ] **Step 7: Thread the scope through the remaining `haveSnap` sites**

Replace every `g.haveSnap = false` / `g.haveSnap = ...` assignment with a scoped invalidation:

- `Refresh`'s old post-flush assignments → handled by Step 6.
- `PrepareCleanupProof` (`:741`, `:750`, `:757`, `:763`, `:777`): `r.guard.invalidateKeysLocked(r.tableScope().Covers)`.
- `ReleaseCleaned` (`:804`, `:816`, `:820`, `:827`): same; the success branch instead re-marks the reservation's keys fresh, which the `Refresh` inside `ReleaseCleaned` already did.
- `RestoreBatch` (`:371`, `:375`, `:399`): keep the global semantics — `g.restoreBlocked = true` plus `g.invalidateKeysLocked(func(PartsKey) bool { return true })`. Its success tail (`:529-530`) becomes `g.restoreBlocked = false` followed by `g.invalidatePendingObservationKeysLocked()`; the cleanup-proof half of the old expression is now evaluated per table inside `checkTableAvailableLocked`, so no global flag survives.
- `Snapshot()`: `if !g.lastFullOK || g.now().Sub(g.lastFullAt) > g.cfg.SnapshotTTL { return nil, false }`.

Add the reservation helper:

```go
// tableScope is the reservation's own table. Cleanup-proof fences and their
// invalidations are scoped to it (Spec L D7): one stuck proof must not fence
// SI ingress for every other table in the deployment.
func (r *partsReservation) tableScope() PartsScope {
	table := ""
	if len(r.keys) > 0 {
		table = r.keys[0].Table
	}
	return PartsScope{Database: r.guard.cfg.UnsafeDatabase, Table: table}
}
```

- [ ] **Step 8: Add `PartsScope`** — create `pkg/storageintegrity/parts_pressure_scope.go`

```go
package storageintegrity

import (
	"fmt"
	"strings"
)

// PartsScope names the keys one inventory read is authoritative for. A read
// proves both presence and absence inside its scope and says nothing outside
// it, which is what lets the admission path read a few partitions instead of
// every active part in the deployment.
type PartsScope struct {
	// Database is the primary database; always the unsafe database in
	// admission paths.
	Database string
	// IncludeSafeDatabase widens the scope to the safe database as well. Only
	// full passes do this, and only for counts.
	IncludeSafeDatabase bool
	// SafeDatabase is the second database covered when IncludeSafeDatabase.
	SafeDatabase string
	// Table limits the scope to one physical table ("" = every table).
	Table string
	// Partitions limits the scope to these logical partition ids
	// (empty = every partition of Table). Ignored when Table is "".
	Partitions []string
}

// Covers reports whether this read is authoritative for key.
func (s PartsScope) Covers(key PartsKey) bool {
	switch key.Database {
	case s.Database:
	case s.SafeDatabase:
		if !s.IncludeSafeDatabase || s.SafeDatabase == "" {
			return false
		}
		// The safe database is only ever covered as a whole; admission never
		// scopes into it.
		return s.Table == ""
	default:
		return false
	}
	if s.Table != "" && key.Table != s.Table {
		return false
	}
	if s.Table == "" || len(s.Partitions) == 0 {
		return true
	}
	for _, partition := range s.Partitions {
		if partition == key.Partition {
			return true
		}
	}
	return false
}

// IsFull reports whether the scope covers every key of both configured
// databases.
func (s PartsScope) IsFull(cfg PartsPressureConfig) bool {
	return s.Table == "" && s.Database == cfg.UnsafeDatabase &&
		(cfg.SafeDatabase == "" || (s.IncludeSafeDatabase && s.SafeDatabase == cfg.SafeDatabase))
}

// partitionTexts maps logical partition ids back to system.parts.partition
// text. "all" means the table is unpartitioned, so the scope degenerates to
// the whole table; mixing it with p_-prefixed ids is a caller bug and fails
// closed rather than silently reading a wider or narrower set.
func partitionTexts(ids []string) ([]string, bool, error) {
	if len(ids) == 0 {
		return nil, true, nil
	}
	sawAll := false
	texts := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "all" {
			sawAll = true
			continue
		}
		if !strings.HasPrefix(id, "p_") {
			return nil, false, fmt.Errorf("storage_integrity: partition id %q is neither \"all\" nor p_-prefixed", id)
		}
		texts = append(texts, strings.TrimPrefix(id, "p_"))
	}
	if sawAll && len(texts) > 0 {
		return nil, false, fmt.Errorf("storage_integrity: partition id set mixes \"all\" with partitioned ids: %v", ids)
	}
	if sawAll {
		return nil, true, nil
	}
	return texts, false, nil
}
```

- [ ] **Step 9: Add `BuildExactPartsQuery`** — in `parts_pressure.go`, replacing `BuildSnapshotQuery`

```go
// BuildExactPartsQuery reads every active part NAME in the scope, together
// with the partition text and the table's partition key, so an unpartitioned
// table can be distinguished from a partitioned String value whose bytes are
// "tuple()". Aggregation happens in Go because the reservation/cleanup-proof
// protocol needs exact names. Row count is bounded by the scope: a
// statement-scoped read returns at most the parts of the partitions it
// touches, which ClickHouse itself caps at parts_to_throw_insert.
func (g *PartsPressureGuard) BuildExactPartsQuery(scope PartsScope) (string, []any) {
	var b strings.Builder
	args := []any{}
	b.WriteString("SELECT parts.database, parts.table, parts.partition, tables.partition_key, parts.name " +
		"FROM system.parts AS parts INNER JOIN system.tables AS tables " +
		"ON parts.database = tables.database AND parts.table = tables.name WHERE ")
	if scope.IncludeSafeDatabase && scope.SafeDatabase != "" {
		b.WriteString("parts.database IN (?, ?)")
		args = append(args, scope.Database, scope.SafeDatabase)
	} else {
		b.WriteString("parts.database = ?")
		args = append(args, scope.Database)
	}
	b.WriteString(" AND parts.active")
	if scope.Table != "" {
		b.WriteString(" AND parts.table = ?")
		args = append(args, scope.Table)
		if texts, whole, err := partitionTexts(scope.Partitions); err == nil && !whole {
			b.WriteString(" AND parts.partition IN (")
			for i, text := range texts {
				if i > 0 {
					b.WriteString(", ")
				}
				b.WriteString("?")
				args = append(args, text)
			}
			b.WriteString(")")
		}
	}
	b.WriteString(" ORDER BY parts.database, parts.table, parts.partition, parts.name")
	return b.String(), args
}
```

`partitionTexts`'s error is deliberately swallowed here (a malformed id widens the read to the whole table, which is still correct, just less bounded); the caller validates it explicitly in Task 15 and fails closed there.

- [ ] **Step 10: Teach the fake conn to honour the scope**

In `parts_pressure_test.go`'s `fakePartsConn.Query`, capture the args and filter:

```go
func (c *fakePartsConn) Query(ctx context.Context, query string, args ...any) (MergeRows, error) {
	c.mu.Lock()
	c.queries = append(c.queries, query)
	c.args = append(c.args, append([]any(nil), args...))
	// ... existing capture of rows/inventory/queryErr/blockUntilContext ...
	c.mu.Unlock()
	// ... existing queryStarted / blockUntilContext / queryErr handling ...
	// ... existing rows -> inventory expansion ...
	return &fakePartsRows{rows: filterFakeInventory(query, args, inventory)}, nil
}

// filterFakeInventory applies the same predicate the real query does, so a
// scoped read in a test sees exactly what ClickHouse would return.
func filterFakeInventory(query string, args []any, inventory []fakePartInventoryRow) []fakePartInventoryRow {
	databases := map[string]bool{}
	table := ""
	partitions := map[string]bool{}
	rest := args
	if strings.Contains(query, "parts.database IN (?, ?)") {
		databases[rest[0].(string)], databases[rest[1].(string)] = true, true
		rest = rest[2:]
	} else {
		databases[rest[0].(string)] = true
		rest = rest[1:]
	}
	if strings.Contains(query, "parts.table = ?") && len(rest) > 0 {
		table = rest[0].(string)
		rest = rest[1:]
		for _, value := range rest {
			partitions[value.(string)] = true
		}
	}
	out := make([]fakePartInventoryRow, 0, len(inventory))
	for _, row := range inventory {
		if !databases[row.database] {
			continue
		}
		if table != "" && row.table != table {
			continue
		}
		if len(partitions) > 0 && !partitions[row.partition] {
			continue
		}
		out = append(out, row)
	}
	return out
}
```

Add `args [][]any` to `fakePartsConn` and a `lastArgs()` accessor for Task 15's assertions.

- [ ] **Step 11: Run every existing test**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS — all 49 pre-existing `TestPartsPressureGuard_*` plus the three new ones. Any failure here is a real semantic regression in the reservation protocol; fix the refactor, never the test.

- [ ] **Step 12: Run the docker test unchanged**

Run: `bazel test //pkg/integration:integration_test --test_filter=TestPartsPressureGuard_AgainstRealSystemParts --test_output=errors`
Expected: PASS.

- [ ] **Step 13: Commit**

```bash
bazel run //:gazelle
git add pkg/storageintegrity/parts_pressure.go pkg/storageintegrity/parts_pressure_scope.go pkg/storageintegrity/parts_pressure_test.go pkg/storageintegrity/BUILD.bazel
git commit -m "refactor(storageintegrity): make part inventory freshness and generation per key"
```

### Task 14: housegate — aggregate counts for the poller; `hg_safe` never read by name

**Files:**
- Modify: `pkg/storageintegrity/parts_pressure.go`
- Create: `pkg/storageintegrity/parts_pressure_scope_test.go`
- Modify: `storage_integrity_backpressure.go`

**Interfaces:**
- Produces (used by Tasks 15, 17):
  - `func (g *PartsPressureGuard) BuildAggregateSnapshotQuery() (string, []any)`
  - `func (g *PartsPressureGuard) RefreshCounts(ctx context.Context) (PartsSnapshot, error)` — aggregate pass over both databases; updates counts, `countFreshAt` and generation, never names.
  - `func (g *PartsPressureGuard) RefreshLiveKeys(ctx context.Context) error` — exact pass over just the keys that currently hold reservations or candidate claims.
  - `StorageIntegrityPartsPressureSupervisor.Refresh` now runs `RefreshCounts` followed by `RefreshLiveKeys`.

- [ ] **Step 1: Write the failing tests** — `pkg/storageintegrity/parts_pressure_scope_test.go`

```go
package storageintegrity

import (
	"context"
	"strings"
	"testing"
)

func TestBuildAggregateSnapshotQuery_IsGroupedAndBoundToBothDatabases(t *testing.T) {
	g := NewPartsPressureGuard(&fakePartsConn{}, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	query, args := g.BuildAggregateSnapshotQuery()
	for _, want := range []string{"count()", "GROUP BY", "parts.database IN (?, ?)", "parts.active"} {
		if !strings.Contains(query, want) {
			t.Fatalf("aggregate query %q missing %q", query, want)
		}
	}
	if strings.Contains(query, "parts.name") {
		t.Fatalf("aggregate query must not read part names: %q", query)
	}
	if len(args) != 2 || args[0] != "hg_unsafe" || args[1] != "hg_safe" {
		t.Fatalf("aggregate args = %v", args)
	}
}

// hg_safe part counts only grow in v1 (merges are pinned off), so reading its
// exact names is the unbounded cost Spec L §1c describes. Counts are all the
// gauges need; nothing in the reservation protocol keys on the safe database.
func TestRefreshCounts_NeverReadsSafeDatabasePartNames(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 2},
		fakePartsRow{database: "hg_safe", table: "db__t", partition: "p0", partitionKey: "p", number: 5},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	snapshot, err := g.RefreshCounts(context.Background())
	if err != nil {
		t.Fatalf("RefreshCounts: %v", err)
	}
	if snapshot[PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_p0"}] != 5 {
		t.Fatalf("safe count missing: %v", snapshot)
	}
	g.mu.RLock()
	names := g.activeParts[PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_p0"}]
	g.mu.RUnlock()
	if len(names) != 0 {
		t.Fatalf("safe database part names were read: %v", names)
	}
	for _, query := range conn.recordedQueries() {
		if strings.Contains(query, "parts.name") && strings.Contains(query, "hg_safe") {
			t.Fatalf("a query read safe-database part names: %q", query)
		}
	}
}

func TestRefreshLiveKeys_ReadsOnlyKeysWithLiveOwnership(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__a", partition: "p0", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__b", partition: "p0", partitionKey: "p", number: 1},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	reservation, err := g.Reserve(context.Background(), "db__a", []string{"p_p0"})
	if err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	defer reservation.Release()
	conn.resetQueries()
	if err := g.RefreshLiveKeys(context.Background()); err != nil {
		t.Fatalf("RefreshLiveKeys: %v", err)
	}
	queries := conn.recordedQueries()
	if len(queries) != 1 {
		t.Fatalf("RefreshLiveKeys issued %d queries, want 1", len(queries))
	}
	args := conn.lastArgs()
	if len(args) != 3 || args[1] != "db__a" || args[2] != "p0" {
		t.Fatalf("live-key read args = %v, want the reserved table and partition only", args)
	}
}
```

Add `recordedQueries()` and `resetQueries()` helpers to `fakePartsConn` alongside `lastArgs()`.

- [ ] **Step 2: Run and watch them fail**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: FAIL — `BuildAggregateSnapshotQuery`, `RefreshCounts`, `RefreshLiveKeys` undefined.

- [ ] **Step 3: Implement the aggregate query and pass**

```go
// BuildAggregateSnapshotQuery counts active parts per (database, table,
// partition) across the configured databases. Its row count is bounded by
// tables x partitions rather than by parts, which is what makes the interval
// poll affordable while hg_safe grows without bound (Spec L D3).
func (g *PartsPressureGuard) BuildAggregateSnapshotQuery() (string, []any) {
	args := []any{g.cfg.UnsafeDatabase}
	predicate := "parts.database = ?"
	if g.cfg.SafeDatabase != "" {
		predicate = "parts.database IN (?, ?)"
		args = append(args, g.cfg.SafeDatabase)
	}
	return "SELECT parts.database, parts.table, parts.partition, tables.partition_key, count() " +
		"FROM system.parts AS parts INNER JOIN system.tables AS tables " +
		"ON parts.database = tables.database AND parts.table = tables.name " +
		"WHERE " + predicate + " AND parts.active " +
		"GROUP BY parts.database, parts.table, parts.partition, tables.partition_key " +
		"ORDER BY parts.database, parts.table, parts.partition", args
}

// RefreshCounts runs the bounded aggregate pass. It updates counts, freshness
// and generation for every key of both databases, and deliberately does not
// touch exact names: a count is not proof that a specific part is gone, so
// name-level reconciliation stays with the exact passes.
func (g *PartsPressureGuard) RefreshCounts(ctx context.Context) (PartsSnapshot, error) {
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	g.commitMu.Lock()
	defer g.commitMu.Unlock()
	refreshCtx, cancel := context.WithTimeout(ctx, g.cfg.RefreshTimeout)
	defer cancel()
	query, args := g.BuildAggregateSnapshotQuery()
	rows, err := g.conn.Query(refreshCtx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("storage_integrity: parts count query failed: %w", err)
	}
	defer rows.Close()
	counts := PartsSnapshot{}
	for rows.Next() {
		var database, table, partition, partitionKey string
		var number uint64
		if err := rows.Scan(&database, &table, &partition, &partitionKey, &number); err != nil {
			return nil, fmt.Errorf("storage_integrity: scan parts counts: %w", err)
		}
		counts[PartsKey{Database: database, Table: table, Partition: LogicalPartitionID(partition, partitionKey == "")}] = int(number)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage_integrity: read parts counts: %w", err)
	}
	scope := g.fullScope()
	g.mu.Lock()
	defer g.mu.Unlock()
	g.applyCountScopeLocked(scope, counts)
	g.reconcileScopeLocked(scope)
	return copyPartsSnapshot(g.snapshot), nil
}

// applyCountScopeLocked installs counts for the covered keys. Keys whose names
// were previously exact keep those names but lose name freshness, because a
// count pass cannot prove a specific part is still there.
func (g *PartsPressureGuard) applyCountScopeLocked(scope PartsScope, counts PartsSnapshot) {
	now := g.now()
	for key := range g.snapshot {
		if scope.Covers(key) {
			if _, present := counts[key]; !present {
				delete(g.snapshot, key)
			}
		}
	}
	covered := make(map[PartsKey]struct{}, len(counts))
	for key, count := range counts {
		g.snapshot[key] = count
		covered[key] = struct{}{}
	}
	for key := range g.countFreshAt {
		if scope.Covers(key) {
			covered[key] = struct{}{}
		}
	}
	for key := range covered {
		g.keyGeneration[key]++
		g.countFreshAt[key] = now
	}
	if scope.IsFull(g.cfg) {
		g.lastFullOK = !g.restoreBlocked
		g.lastFullAt = now
	}
}
```

Note the count pass advances the generation. That is deliberate and matches the pre-existing semantics: `coveredReservationSlotsLocked` already treats aggregate growth as coverage for slots with no exact candidate, and refuses it for slots that do have one.

- [ ] **Step 4: Implement `RefreshLiveKeys`**

```go
// RefreshLiveKeys runs one exact pass per table that currently holds a live
// reservation or an unretired candidate claim. Name-level evidence is only
// needed where ownership exists, so the poller's exact cost scales with
// in-flight statements rather than with the size of the deployment.
func (g *PartsPressureGuard) RefreshLiveKeys(ctx context.Context) error {
	g.mu.RLock()
	tables := map[string]map[string]struct{}{}
	add := func(key PartsKey) {
		if key.Database != g.cfg.UnsafeDatabase {
			return
		}
		if tables[key.Table] == nil {
			tables[key.Table] = map[string]struct{}{}
		}
		tables[key.Table][key.Partition] = struct{}{}
	}
	for _, reservation := range g.liveReservations {
		for _, key := range reservation.keys {
			add(key)
		}
	}
	for key := range g.candidateClaims {
		add(key)
	}
	g.mu.RUnlock()
	var errs []error
	for table, partitionSet := range tables {
		partitions := make([]string, 0, len(partitionSet))
		for partition := range partitionSet {
			partitions = append(partitions, partition)
		}
		sort.Strings(partitions)
		scope := PartsScope{Database: g.cfg.UnsafeDatabase, Table: table, Partitions: partitions}
		if _, err := g.refreshScope(ctx, scope); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
```

- [ ] **Step 5: Point the supervisor at the two passes**

In `storage_integrity_backpressure.go`, `(*StorageIntegrityPartsPressureSupervisor).Refresh`:

```go
func (s *StorageIntegrityPartsPressureSupervisor) Refresh(ctx context.Context) error {
	if s == nil || s.guard == nil {
		return errors.New("storage_integrity: parts pressure guard is required")
	}
	// Counts first: they are the gauge input and the capacity input for every
	// key, and the query is bounded by tables x partitions. Then one exact pass
	// per table that actually holds ownership, so cleanup proof and claim
	// retirement keep their name-level evidence.
	snap, err := s.guard.RefreshCounts(ctx)
	if err != nil {
		return err
	}
	if err := s.guard.RefreshLiveKeys(ctx); err != nil {
		return err
	}
	storageIntegrityUnsafeParts.Reset()
	storageIntegritySafeParts.Reset()
	for key, parts := range snap {
		switch key.Database {
		case s.unsafeDB:
			storageIntegrityUnsafeParts.WithLabelValues(key.Table, key.Partition).Set(float64(parts))
		case s.safeDB:
			storageIntegritySafeParts.WithLabelValues(key.Table, key.Partition).Set(float64(parts))
		}
	}
	return nil
}
```

The startup fail-fast path (`storage_integrity_runtime.go`'s `startStorageIntegrityRuntime` → `pressureRunner.Refresh(ctx)`) keeps calling this, so startup still fails closed when ClickHouse is unreachable.

- [ ] **Step 6: Run the tests**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test //:housegate_test --test_output=errors`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/storageintegrity storage_integrity_backpressure.go
git commit -m "perf(storageintegrity): poll part counts as a bounded aggregate and read names only where ownership exists"
```

### Task 15: housegate — the admission path reads only the partitions the statement touches

**Files:**
- Modify: `pkg/storageintegrity/parts_pressure.go` (`reserve`, `PrepareCleanupProof`, `ReleaseCleaned`)
- Modify: `pkg/storageintegrity/parts_pressure_scope_test.go`

**Interfaces:**
- Produces (used by Task 19): `reserve` issues exactly one query whose bound arguments are the unsafe database, the physical table and the statement's partition texts; `PrepareCleanupProof` / `ReleaseCleaned` do the same for their reservation's table.

- [ ] **Step 1: Write the failing test**

```go
// Spec L D3(a): the hot path reads only the (table, partition) pairs the
// statement touches. The whole point is that this query's cost is independent
// of how many parts every other table holds.
func TestReserve_ReadsOnlyTheStatementsPartitions(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p1", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__other", partition: "p0", partitionKey: "p", number: 900},
		fakePartsRow{database: "hg_safe", table: "db__t", partition: "p0", partitionKey: "p", number: 5000},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("seed Refresh: %v", err)
	}
	conn.resetQueries()
	reservation, err := g.ReserveStatement(context.Background(), "0xabc:1:n", "db__t", []string{"p_p0"})
	if err != nil {
		t.Fatalf("ReserveStatement: %v", err)
	}
	defer reservation.Release()
	queries := conn.recordedQueries()
	if len(queries) != 1 {
		t.Fatalf("Reserve issued %d queries, want 1", len(queries))
	}
	if strings.Contains(queries[0], "hg_safe") {
		t.Fatalf("hot-path query touched the safe database: %q", queries[0])
	}
	args := conn.lastArgs()
	if len(args) != 3 || args[0] != "hg_unsafe" || args[1] != "db__t" || args[2] != "p0" {
		t.Fatalf("hot-path args = %v, want [hg_unsafe db__t p0]", args)
	}
	// The untouched keys keep the values the earlier full pass established.
	g.mu.RLock()
	other := g.snapshot[PartsKey{Database: "hg_unsafe", Table: "db__other", Partition: "p_p0"}]
	safe := g.snapshot[PartsKey{Database: "hg_safe", Table: "db__t", Partition: "p_p0"}]
	g.mu.RUnlock()
	if other != 900 || safe != 5000 {
		t.Fatalf("scoped read clobbered untouched keys: other=%d safe=%d", other, safe)
	}
}

func TestReserve_RejectsMalformedPartitionIDs(t *testing.T) {
	conn := &fakePartsConn{}
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe"})
	if _, err := g.ReserveStatement(context.Background(), "0xabc:1:n", "db__t", []string{"p_p0", "all"}); err == nil {
		t.Fatal("mixing all with partitioned ids must fail closed")
	}
	if _, err := g.ReserveStatement(context.Background(), "0xabc:1:n", "db__t", []string{"bogus"}); err == nil {
		t.Fatal("a partition id that is neither all nor p_-prefixed must fail closed")
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter=TestReserve_ --test_output=errors`
Expected: FAIL — `Reserve issued 1 queries` passes but `hot-path args = [hg_unsafe hg_safe]` (still the full scope), and the malformed-id case is accepted.

- [ ] **Step 3: Scope the reservation read**

```go
func (g *PartsPressureGuard) reserve(ctx context.Context, statementID, table string, partitionIDs []string) (PartsReservation, error) {
	if _, _, err := partitionTexts(partitionIDs); err != nil {
		// A partition id that cannot be mapped back to system.parts text would
		// silently widen or narrow the read; refuse instead.
		return nil, fmt.Errorf("%w: %w", ErrBackpressure, err)
	}
	unlock := g.lockTable(table)
	defer unlock()
	scope := PartsScope{Database: g.cfg.UnsafeDatabase, Table: table, Partitions: partitionIDs}
	if _, err := g.refreshScope(ctx, scope); err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		unavailable := &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Kind: "unavailable"}
		return nil, fmt.Errorf("%w: refresh parts snapshot: %w", unavailable, err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	reservation, err := g.newReservationLocked(table, partitionIDs, true)
	if err != nil {
		return nil, err
	}
	reservation.statementID = statementID
	return reservation, nil
}
```

(`lockTable` arrives in Task 16; until then keep `g.admissionMu.Lock()` here and swap it in that task.)

- [ ] **Step 4: Scope the two cleanup-proof reads**

In `PrepareCleanupProof` and `ReleaseCleaned`, replace `r.guard.Refresh(ctx)` with:

```go
	_, refreshErr := r.guard.refreshScope(ctx, r.reservationScope())
```

and add:

```go
// reservationScope is this reservation's exact table and partitions. Cleanup
// proof only needs name-level evidence for the parts it is proving absent.
func (r *partsReservation) reservationScope() PartsScope {
	scope := PartsScope{Database: r.guard.cfg.UnsafeDatabase}
	for _, key := range r.keys {
		scope.Table = key.Table
		scope.Partitions = append(scope.Partitions, key.Partition)
	}
	return scope
}
```

A reservation's keys always share one table (`newReservationLocked` builds them from a single `table` argument), so the last assignment is the table; keep the loop shape so a future multi-table reservation trips the `Covers` check rather than silently reading the wrong scope.

- [ ] **Step 5: Run every guard test**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS — including all 49 pre-existing tests. Pay attention to `TestPartsPressureGuard_InFlightCleanupRebasesQueuedReplacement`, `TestPartsPressureGuard_CleanupRebasesOlderReservationQueuedBehind` and `TestPartsPressureGuard_ExactCleanupDistinguishesOffsettingVisibleWrite`: they are the rebase paths that depend on the admission-time name capture.

- [ ] **Step 6: Run the docker guard test**

Run: `bazel test //pkg/integration:integration_test --test_filter=TestPartsPressureGuard_AgainstRealSystemParts --test_output=errors`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/storageintegrity
git commit -m "perf(storageintegrity): scope the admission part read to the statement's partitions"
```

### Task 16: housegate — per-table admission serialization (D3) and per-table cleanup fence (D7)

**Files:**
- Modify: `pkg/storageintegrity/parts_pressure.go`
- Modify: `pkg/storageintegrity/parts_pressure_test.go`

**Interfaces:**
- Produces: `func (g *PartsPressureGuard) lockTable(table string) func()` and `func (g *PartsPressureGuard) lockAllTables() func()`; `admissionMu` becomes `admissionGate sync.RWMutex` + `tableLocks map[string]*sync.Mutex`.

- [ ] **Step 1: Write the failing tests**

```go
// One table's slow inventory read must not serialize another table's
// admissions (Spec L §1c: admissionMu serialized all admissions behind one
// query).
func TestPartsPressureGuard_AdmissionsOnDifferentTablesDoNotSerialize(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(
		fakePartsRow{database: "hg_unsafe", table: "db__slow", partition: "p0", partitionKey: "p", number: 1},
		fakePartsRow{database: "hg_unsafe", table: "db__fast", partition: "p0", partitionKey: "p", number: 1},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe"})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	release := conn.blockTable("db__slow") // gate the scoped read for that table
	slowDone := make(chan struct{})
	go func() {
		defer close(slowDone)
		if r, err := g.Reserve(context.Background(), "db__slow", []string{"p_p0"}); err == nil {
			r.Release()
		}
	}()
	conn.waitForBlocked(t)
	fast, err := g.Reserve(context.Background(), "db__fast", []string{"p_p0"})
	if err != nil {
		t.Fatalf("admission on another table blocked behind a slow read: %v", err)
	}
	fast.Release()
	close(release)
	<-slowDone
}

// Spec L D7: a stuck cleanup proof fences its own table, not the deployment.
func TestPartsPressureGuard_StuckCleanupProofFencesOnlyItsTable(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setInventory(
		fakePartInventoryRow{database: "hg_unsafe", table: "db__a", partition: "p0", partitionKey: "p", partName: "a_1_1_0"},
		fakePartInventoryRow{database: "hg_unsafe", table: "db__b", partition: "p0", partitionKey: "p", partName: "b_1_1_0"},
	)
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe"})
	if _, err := g.Refresh(context.Background()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	cleaner, err := g.ReserveStatement(context.Background(), "0xabc:1:n", "db__a", []string{"p_p0"})
	if err != nil {
		t.Fatalf("ReserveStatement: %v", err)
	}
	if err := cleaner.Commit(CandidatePart{TableID: "db.a", PartitionID: "p_p0", PartName: "a_1_1_0"}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if err := cleaner.PrepareCleanupProof(context.Background(), []CandidatePart{{TableID: "db.a", PartitionID: "p_p0", PartName: "a_1_1_0"}}); err != nil {
		t.Fatalf("PrepareCleanupProof: %v", err)
	}
	// The proof is now armed and pending: db__a is fenced ...
	if _, err := g.Reserve(context.Background(), "db__a", []string{"p_p0"}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("the proving table must be fenced, got %v", err)
	}
	// ... and db__b is not.
	other, err := g.Reserve(context.Background(), "db__b", []string{"p_p0"})
	if err != nil {
		t.Fatalf("an unrelated table was fenced by another table's cleanup proof: %v", err)
	}
	other.Release()
}
```

Add `blockTable(table string) chan struct{}` and `waitForBlocked(t)` to `fakePartsConn`: when a query's args name the blocked table, wait on the returned channel before answering.

- [ ] **Step 2: Run and watch them fail**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --test_filter='TestPartsPressureGuard_(AdmissionsOnDifferentTables|StuckCleanupProof)' --test_output=errors`
Expected: FAIL — the fast admission blocks (single `admissionMu`), and the unrelated table is fenced.

- [ ] **Step 3: Replace `admissionMu` with a gate plus per-table locks**

```go
	// admissionGate is held for reading by per-table admission paths and for
	// writing by whole-guard operations (RestoreBatch), so a durable-projection
	// rebuild still excludes every table while one table's admissions no longer
	// exclude another's (Spec L D3).
	admissionGate sync.RWMutex
	tableLocksMu  sync.Mutex
	tableLocks    map[string]*sync.Mutex
```

```go
// lockTable serializes admission and lifecycle transitions for one physical
// table. Lock order is admissionGate -> table -> refreshMu -> commitMu -> mu.
func (g *PartsPressureGuard) lockTable(table string) func() {
	g.admissionGate.RLock()
	g.tableLocksMu.Lock()
	if g.tableLocks == nil {
		g.tableLocks = map[string]*sync.Mutex{}
	}
	lock := g.tableLocks[table]
	if lock == nil {
		lock = &sync.Mutex{}
		g.tableLocks[table] = lock
	}
	g.tableLocksMu.Unlock()
	lock.Lock()
	return func() {
		lock.Unlock()
		g.admissionGate.RUnlock()
	}
}

// lockAllTables excludes every per-table admission path.
func (g *PartsPressureGuard) lockAllTables() func() {
	g.admissionGate.Lock()
	return g.admissionGate.Unlock
}
```

Replace the `g.admissionMu.Lock(); defer g.admissionMu.Unlock()` pairs:

- `reserve` → `unlock := g.lockTable(table); defer unlock()` (already written in Task 15 Step 3).
- `partsReservation.Commit`, `CommitIndeterminate`, `release`, `Finalize`, `PrepareCleanupProof`, `ReleaseCleaned` → `unlock := r.guard.lockTable(r.tableScope().Table); defer unlock()`. A reservation with no keys (`len(r.keys) == 0`, the zero-capacity recovery handle) locks the empty-string table, which is fine: it excludes only other zero-key handles.
- `RestoreBatch` → `unlock := g.lockAllTables(); defer unlock()`.

Delete the `admissionMu` field.

- [ ] **Step 4: Run every guard test, with race**

Run: `bazel test //pkg/storageintegrity:storageintegrity_test --@rules_go//go/config:race --test_output=errors`
Expected: PASS with no race reports. `TestPartsPressureGuard_ReservePreventsConcurrentOversubscription` reserves the same table from several goroutines and is the proof that per-table serialization is still enough.

- [ ] **Step 5: Commit**

```bash
git add pkg/storageintegrity
git commit -m "perf(storageintegrity): serialize admission per table and scope the cleanup fence to its table"
```

### Task 17: housegate — invalidation marks the snapshot stale instead of forcing a scan

**Files:**
- Modify: `pkg/storageintegrity/parts_pressure.go` (`Invalidate`)
- Modify: `storage_integrity_backpressure.go` (supervisor loop)
- Modify: `pkg/storageintegrity/parts_pressure_test.go` (`TestPartsPressureGuard_InvalidateSignalsOnce`)

**Interfaces:**
- Produces: `Invalidate()` sets a stale flag and wakes the poller at most once per interval; `Invalidated()` keeps its signature.

- [ ] **Step 1: Write the failing test**

```go
// Spec L §1c: the ingress calls Invalidate() after every transition, and the
// poller answered each one with a full scan, so one admission cost two reads.
// Invalidation now marks the cache stale; the poller picks it up on its own
// cadence, coalescing a burst into one pass.
func TestSupervisor_CoalescesInvalidationsIntoOnePass(t *testing.T) {
	conn := &fakePartsConn{}
	conn.setRows(fakePartsRow{database: "hg_unsafe", table: "db__t", partition: "p0", partitionKey: "p", number: 1})
	g := NewPartsPressureGuard(conn, PartsPressureConfig{UnsafeDatabase: "hg_unsafe", SafeDatabase: "hg_safe"})
	supervisor := NewStorageIntegrityPartsPressureSupervisor(g, 50*time.Millisecond, "hg_unsafe", "hg_safe")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go supervisor.Run(ctx)
	for i := 0; i < 50; i++ {
		g.Invalidate()
	}
	time.Sleep(120 * time.Millisecond)
	if got := len(conn.recordedQueries()); got > 8 {
		t.Fatalf("50 invalidations produced %d queries; they must coalesce into the poll cadence", got)
	}
}
```

This test lives in the root package (it uses the supervisor), so put it in a new `storage_integrity_backpressure_test.go` and export what it needs via the existing `sicore` alias pattern used by the other root tests.

- [ ] **Step 2: Run and watch it fail**

Run: `bazel test //:housegate_test --test_filter=TestSupervisor_CoalescesInvalidationsIntoOnePass --test_output=errors`
Expected: FAIL — each invalidation wakes the loop and runs a full pass.

- [ ] **Step 3: Make invalidation a stale flag**

In `parts_pressure.go`:

```go
// Invalidate marks the cached inventory stale. It does NOT trigger a read: the
// admission path refreshes its own scope synchronously, so the only consumer
// of a post-transition refresh is the metrics poller, and forcing a full scan
// per admission is exactly the cost Spec L D3 removes.
func (g *PartsPressureGuard) Invalidate() {
	g.mu.Lock()
	g.stale = true
	g.mu.Unlock()
	select {
	case g.invalidated <- struct{}{}:
	default:
	}
}

// TakeStale reports and clears the stale flag.
func (g *PartsPressureGuard) TakeStale() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	stale := g.stale
	g.stale = false
	return stale
}
```

Add `stale bool` to the guard struct.

- [ ] **Step 4: Make the supervisor coalesce**

```go
func (s *StorageIntegrityPartsPressureSupervisor) Run(ctx context.Context) {
	if s == nil || s.guard == nil {
		return
	}
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	// Invalidations are coalesced: the channel only wakes the loop so a stale
	// cache is picked up promptly, and the pass itself runs at most once per
	// tick. A burst of admissions therefore costs one refresh, not one each.
	pending := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.guard.Invalidated():
			if pending {
				continue
			}
			pending = true
			continue
		}
		pending = false
		s.guard.TakeStale()
		if err := s.Refresh(ctx); err != nil && ctx.Err() == nil {
			log.WarnEveryN("storage_integrity.parts_pressure.refresh", 30, "storage_integrity parts snapshot failed; keeping last snapshot", "err", err)
		}
	}
}
```

- [ ] **Step 5: Run the tests**

Run: `bazel test //:housegate_test //pkg/storageintegrity:storageintegrity_test --test_output=errors`
Expected: PASS, including the existing `TestPartsPressureGuard_InvalidateSignalsOnce`.

- [ ] **Step 6: Commit**

```bash
git add pkg/storageintegrity storage_integrity_backpressure.go storage_integrity_backpressure_test.go
git commit -m "perf(storageintegrity): coalesce pressure invalidations into the poll cadence"
```

### Task 18: housegate — `refresh_timeout` and `snapshot_ttl` config keys

**Files:**
- Modify: `pkg/config/storage_integrity_config.go:152-159` (struct), `:179-186` (defaults), `:284-308` (validation)
- Modify: `storage_integrity_runtime.go:169-174` (wiring)
- Modify: `pkg/config/storage_integrity_config_test.go`

**Interfaces:**
- Produces (used by Task 26): `StorageIntegrityRuntimeBackpressureConfig.RefreshTimeout Duration` (`refresh_timeout`, default `2s`) and `.SnapshotTTL Duration` (`snapshot_ttl`, default `6s`), both validated `> 0` and `snapshot_ttl > refresh_timeout`.

- [ ] **Step 1: Write the failing test** — append to `pkg/config/storage_integrity_config_test.go`

```go
func TestStorageIntegrityBackpressure_RefreshTimeoutAndSnapshotTTL(t *testing.T) {
	base := func() Config {
		cfg := Default()
		cfg.StorageIntegrity.Tables = []string{"db.t"}
		cfg.StorageIntegrity.Ingress.Enabled = true
		cfg.StorageIntegrity.Ingress.NetworkID = "net"
		cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1"}
		cfg.StorageIntegrity.Runtime.Enabled = true
		cfg.StorageIntegrity.Runtime.ExpectedSource = "node-1"
		cfg.StorageIntegrity.Runtime.JournalDir = "/tmp/j"
		cfg.StorageIntegrity.Runtime.PayloadSpoolDir = "/tmp/p"
		return cfg
	}
	if got := Default().StorageIntegrity.Runtime.Backpressure.RefreshTimeout.Duration; got != 2*time.Second {
		t.Fatalf("default refresh_timeout = %s, want 2s", got)
	}
	if got := Default().StorageIntegrity.Runtime.Backpressure.SnapshotTTL.Duration; got != 6*time.Second {
		t.Fatalf("default snapshot_ttl = %s, want 6s", got)
	}
	cfg := base()
	cfg.StorageIntegrity.Runtime.Backpressure.RefreshTimeout = Duration{}
	if err := cfg.StorageIntegrity.validate(ModeServer); err == nil ||
		!strings.Contains(err.Error(), "refresh_timeout") {
		t.Fatalf("zero refresh_timeout error = %v", err)
	}
	cfg = base()
	cfg.StorageIntegrity.Runtime.Backpressure.SnapshotTTL = Duration{Duration: time.Second}
	if err := cfg.StorageIntegrity.validate(ModeServer); err == nil ||
		!strings.Contains(err.Error(), "snapshot_ttl") {
		t.Fatalf("snapshot_ttl below refresh_timeout error = %v", err)
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `bazel test //pkg/config:config_test --test_filter=TestStorageIntegrityBackpressure_RefreshTimeoutAndSnapshotTTL --test_output=errors`
Expected: FAIL — `RefreshTimeout` / `SnapshotTTL` undefined.

- [ ] **Step 3: Add the fields, defaults and validation**

Struct (`pkg/config/storage_integrity_config.go:152-159`):

```go
type StorageIntegrityRuntimeBackpressureConfig struct {
	Enabled               bool     `json:"enabled"                  yaml:"enabled"`
	UnsafeDatabase        string   `json:"unsafe_database"          yaml:"unsafe_database"`
	SafeDatabase          string   `json:"safe_database"            yaml:"safe_database"`
	PollInterval          Duration `json:"poll_interval"            yaml:"poll_interval"`
	// RefreshTimeout bounds one system.parts read. Spec L D3 made it a config
	// key: the value used to be hard-coded, so a deployment whose inventory
	// query grew past it refused every SI INSERT with no operator knob.
	RefreshTimeout Duration `json:"refresh_timeout" yaml:"refresh_timeout"`
	// SnapshotTTL is how long a key's inventory stays admissible after the read
	// that produced it. Must exceed RefreshTimeout, otherwise a read that
	// finishes just inside its own deadline is already expired.
	SnapshotTTL           Duration `json:"snapshot_ttl"             yaml:"snapshot_ttl"`
	SoftPartsPerPartition int      `json:"soft_parts_per_partition" yaml:"soft_parts_per_partition"`
	HardPartsPerPartition int      `json:"hard_parts_per_partition" yaml:"hard_parts_per_partition"`
}
```

Defaults (`:179-186`): add `RefreshTimeout: Duration{Duration: 2 * time.Second}`, `SnapshotTTL: Duration{Duration: 6 * time.Second}`.

Validation, inside the existing `if bp := c.Runtime.Backpressure; bp.Enabled {` block:

```go
			if bp.RefreshTimeout.Duration <= 0 {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.refresh_timeout must be > 0"))
			}
			if bp.SnapshotTTL.Duration <= 0 {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.snapshot_ttl must be > 0"))
			} else if bp.RefreshTimeout.Duration > 0 && bp.SnapshotTTL.Duration <= bp.RefreshTimeout.Duration {
				errs = append(errs, errors.New("storage_integrity.runtime.backpressure.snapshot_ttl must be greater than refresh_timeout"))
			}
```

- [ ] **Step 4: Wire them into the guard**

`storage_integrity_runtime.go:169-174`:

```go
			guard := sicore.NewPartsPressureGuard(opts.MergeConn, sicore.PartsPressureConfig{
				UnsafeDatabase:        unsafeDatabase,
				SafeDatabase:          safeDatabase,
				SoftPartsPerPartition: backpressure.SoftPartsPerPartition,
				HardPartsPerPartition: backpressure.HardPartsPerPartition,
				RefreshTimeout:        backpressure.RefreshTimeout.Duration,
				SnapshotTTL:           backpressure.SnapshotTTL.Duration,
			})
```

- [ ] **Step 5: Run the tests**

Run: `bazel test //pkg/config:config_test //:housegate_test --test_output=errors`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/config storage_integrity_runtime.go
git commit -m "feat(config): make the part-pressure refresh timeout and snapshot TTL configurable"
```

### Task 19: housegate — docker proof that the hot-path read is bounded

**Files:**
- Create: `pkg/integration/storage_backpressure_bounded_test.go`

**Interfaces:**
- Consumes: `openDirectCH(t)`, `mustExec(t, conn, sql)`, `uniqueTable(t)`, `chMergeConn` (already in `pkg/integration/storage_backpressure_test.go`), `sicore.NewPartsPressureGuard`, `BuildExactPartsQuery`, `BuildAggregateSnapshotQuery`.

- [ ] **Step 1: Write the docker test**

```go
package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// Spec L §1c has a cliff: hg_safe merges are pinned off, so its part count only
// grows, and the old admission query returned one row per active part across
// both databases. Once that crossed the 2s deadline every SI INSERT failed
// closed permanently. This test builds a table with several thousand parts and
// proves the admission read stays bounded and fast.
func TestPartsPressure_HotPathReadStaysBoundedWithManyParts(t *testing.T) {
	const (
		noisyParts = 3000
		partitions = 4
	)
	ctx := context.Background()
	conn := openDirectCH(t)
	suffix := uniqueTable(t)
	unsafeDB := "hg_unsafe_bounded_" + suffix
	safeDB := "hg_safe_bounded_" + suffix
	hot, noisy := "db__hot", "db__noisy"
	for _, query := range []string{
		"CREATE DATABASE IF NOT EXISTS " + unsafeDB,
		"CREATE DATABASE IF NOT EXISTS " + safeDB,
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_throw_insert = 100000, parts_to_delay_insert = 100000, max_parts_in_total = 1000000", unsafeDB, hot),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_throw_insert = 100000, parts_to_delay_insert = 100000, max_parts_in_total = 1000000", safeDB, noisy),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", unsafeDB, hot),
		fmt.Sprintf("SYSTEM STOP MERGES %s.%s", safeDB, noisy),
	} {
		mustExec(t, conn, query)
	}
	t.Cleanup(func() {
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+unsafeDB+" SYNC")
		_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+safeDB+" SYNC")
	})

	// hg_safe is the unbounded one in production; give it the bulk of the parts.
	for i := 0; i < noisyParts; i++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p%d', %d)", safeDB, noisy, i, i%partitions, i))
	}
	for i := 0; i < 200; i++ {
		mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p%d', %d)", unsafeDB, hot, i, i%partitions, i))
	}

	guard := sicore.NewPartsPressureGuard(chMergeConn{conn: conn}, sicore.PartsPressureConfig{
		UnsafeDatabase: unsafeDB, SafeDatabase: safeDB,
		SoftPartsPerPartition: 2400, HardPartsPerPartition: 2950,
		RefreshTimeout: 2 * time.Second, SnapshotTTL: 6 * time.Second,
	})

	// Count the rows the two production queries actually return.
	countRows := func(query string, args ...any) int {
		rows, err := conn.Query(ctx, query, args...)
		if err != nil {
			t.Fatalf("run pressure query: %v\n%s", err, query)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("read pressure query: %v", err)
		}
		return n
	}
	aggQuery, aggArgs := guard.BuildAggregateSnapshotQuery()
	if got := countRows(aggQuery, aggArgs...); got > 4*partitions {
		t.Fatalf("aggregate poll returned %d rows; it must be bounded by tables x partitions", got)
	}
	exactQuery, exactArgs := guard.BuildExactPartsQuery(sicore.PartsScope{
		Database: unsafeDB, Table: hot, Partitions: []string{"p_p0"},
	})
	exactRows := countRows(exactQuery, exactArgs...)
	if exactRows == 0 || exactRows > 200 {
		t.Fatalf("scoped admission read returned %d rows; want only the touched partition's parts", exactRows)
	}

	// And the admission itself completes well inside the refresh deadline.
	if _, err := guard.Refresh(ctx); err != nil {
		t.Fatalf("seed refresh: %v", err)
	}
	start := time.Now()
	reservation, err := guard.ReserveStatement(ctx, "0xabc:1:n", hot, []string{"p_p0"})
	if err != nil {
		t.Fatalf("ReserveStatement with %d noisy parts: %v", noisyParts, err)
	}
	elapsed := time.Since(start)
	reservation.Release()
	if elapsed > time.Second {
		t.Fatalf("admission took %s with %d parts in hg_safe; the hot path is not bounded", elapsed, noisyParts)
	}
}
```

- [ ] **Step 2: Run it**

Run: `bazel test //pkg/integration:integration_test --test_filter=TestPartsPressure_HotPathReadStaysBoundedWithManyParts --test_output=all --test_timeout=900`
Expected: PASS. The 3200 single-row inserts take a couple of minutes on a cold container; that is why the target's timeout is raised here.

- [ ] **Step 3: Confirm CI already runs it**

The new file lives in the existing `//pkg/integration:integration_test` target, which `.github/workflows/ci.yml` already lists. Run `bazel query 'tests(//pkg/integration:integration_test)' | head` to confirm the file was picked up after `bazel run //:gazelle`; no workflow edit is needed.

- [ ] **Step 4: Commit**

```bash
bazel run //:gazelle
git add pkg/integration/storage_backpressure_bounded_test.go pkg/integration/BUILD.bazel
git commit -m "test(integration): prove the admission part read stays bounded with thousands of parts"
```

### Task 20: housegate — a rejection class that keeps the session

**Files:**
- Modify: `pkg/chproto/client_error.go`
- Create: `pkg/chproto/client_error_test.go`
- Modify: `storage_integrity_ingress.go:491-499` (`backpressureClientError`)

**Interfaces:**
- Produces (used by Tasks 21–23):
  - `chproto.ClientError.KeepSession bool`
  - `func chproto.KeepsSession(err error) bool` — true when any `*ClientError` in the chain sets `KeepSession`.
  - `backpressureClientError` returns a `KeepSession: true` error.

- [ ] **Step 1: Write the failing test** — `pkg/chproto/client_error_test.go`

```go
package chproto

import (
	"errors"
	"fmt"
	"testing"
)

func TestKeepsSession(t *testing.T) {
	throttle := &ClientError{Code: CodeTooManyParts, Message: "storage_integrity: back-pressure: retry later", KeepSession: true}
	if !KeepsSession(fmt.Errorf("wrapped: %w", throttle)) {
		t.Fatal("a wrapped KeepSession ClientError must be recognised")
	}
	if KeepsSession(&ClientError{Code: 403, Message: "denied"}) {
		t.Fatal("an ordinary ClientError must not keep the session")
	}
	if KeepsSession(errors.New("boom")) {
		t.Fatal("a plain error must not keep the session")
	}
	if KeepsSession(nil) {
		t.Fatal("nil must not keep the session")
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `bazel test //pkg/chproto:chproto_test --test_output=errors`
Expected: FAIL — `unknown field KeepSession`, `undefined: KeepsSession`.

- [ ] **Step 3: Implement**

```go
// ClientError lets a plugin choose the ClickHouse exception code and exact
// client-facing message. Err remains the server-side cause and is not sent.
type ClientError struct {
	Code    int32
	Message string
	Err     error
	// KeepSession marks a rejection that ends the QUERY without ending the
	// CONNECTION. Spec L D6: back-pressure fires precisely when connection
	// churn is most expensive, and real ClickHouse answers TOO_MANY_PARTS with
	// an Exception on a session that stays usable. Relay honours this only at
	// points where the client's input stream is at a clean packet boundary; at
	// any other point the session is still closed, because the framing state is
	// unknown.
	KeepSession bool
}

// KeepsSession reports whether err is (or wraps) a ClientError that must not
// close the session.
func KeepsSession(err error) bool {
	var clientErr *ClientError
	return errors.As(err, &clientErr) && clientErr.KeepSession
}
```

Add the `errors` import.

- [ ] **Step 4: Mark the back-pressure rejection**

`storage_integrity_ingress.go:498`:

```go
	return &chproto.ClientError{
		Code:        chproto.CodeTooManyParts,
		Message:     message,
		Err:         err,
		KeepSession: true,
	}
```

- [ ] **Step 5: Run the tests, then commit**

Run: `bazel test //pkg/chproto:chproto_test //:housegate_test --test_output=errors`
Expected: PASS.

```bash
git add pkg/chproto storage_integrity_ingress.go
git commit -m "feat(chproto): add a rejection class that ends the query without ending the session"
```

### Task 21: housegate — non-closing rejection on the staged (`SuppressUpstreamExecution`) path

**Files:**
- Modify: `pkg/proxy/relay.go` (struct fields near `:41-62`, `clientToUpstream:1110-1170`, `upstreamToClient:1969-1980`)
- Create: `pkg/proxy/relay_reject_test.go`

**Interfaces:**
- Produces (used by Tasks 22, 23):
  - `func (r *Relay) rejectActiveQueryTerminal(queryID string, exc *chproto.Exception) bool`
  - `func (r *Relay) takePendingRejection(queryID string) *chproto.Exception`

**Why this path and not `runDeferredInsert`.** Spec L §1f names `runDeferredInsert`, but the server-side ingress plugin sets `qctx.SuppressUpstreamExecution = true` (`pkg/plugins/storageintegrity/plugin.go:280`), not `DeferredInsert` — `DeferredInsert` is the *agent*-side `sistatement` plugin. Back-pressure therefore surfaces from `clientToUpstream`'s strict hook at `relay.go:1119`. Task 22 covers the deferred path too, so both are safe; this task is the one that fixes the production symptom.

- [ ] **Step 1: Write the failing test** — `pkg/proxy/relay_reject_test.go`

```go
package proxy

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

// stagedRejectHooks mimics the storage-integrity ingress plugin: it stages the
// INSERT (payload withheld from upstream) and rejects the first statement at
// the end-of-input boundary with a retryable, session-preserving 252.
type stagedRejectHooks struct {
	plugin.NoopHooks
	mu        sync.Mutex
	queries   []string
	rejectOne bool
	aborts    int
	completes int
	successes int
}

func (h *stagedRejectHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queries = append(h.queries, qctx.Query.Body)
	qctx.SuppressUpstreamExecution = len(h.queries) == 1
	return nil
}

func (h *stagedRejectHooks) OnQueryInputCompleteStrict(context.Context, *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.rejectOne {
		return nil
	}
	h.rejectOne = false
	return &chproto.ClientError{
		Code:        chproto.CodeTooManyParts,
		Message:     "storage_integrity: back-pressure: hg_unsafe.db__t partition p_p0 has 2400 active parts (soft limit 2400); retry later",
		KeepSession: true,
	}
}

func (h *stagedRejectHooks) OnQueryAbort(context.Context, *plugin.QueryContext) {
	h.mu.Lock()
	h.aborts++
	h.mu.Unlock()
}

func (h *stagedRejectHooks) OnQueryComplete(context.Context, chsession.Session) {
	h.mu.Lock()
	h.completes++
	h.mu.Unlock()
}

func (h *stagedRejectHooks) OnQuerySuccess(context.Context, chsession.Session, string) {
	h.mu.Lock()
	h.successes++
	h.mu.Unlock()
}

func (h *stagedRejectHooks) counts() (aborts, completes, successes int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.aborts, h.completes, h.successes
}

// Spec L D6 acceptance: the client receives Exception 252 AND the connection
// remains usable for a subsequent query.
func TestRelay_StagedRejection_KeepsSessionAndServesNextQuery(t *testing.T) {
	hooks := &stagedRejectHooks{rejectOne: true}
	h := newDeferredHarness(t, hooks)
	empty := encodeEmptyClientData(t)

	upDone := make(chan error, 1)
	go func() { upDone <- serveStagedRejectUpstream(t, h.upstreamProxy) }()

	// Statement 1: staged INSERT that back-pressure refuses.
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q1", "INSERT INTO db.t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(encodeServerSampleDataPacket(t, deferredTestRev))); len(got) == 0 {
		t.Fatal("client never received the upstream sample block")
	}
	writeAllConn(t, h.clientProxy, empty)                                     // external-tables marker
	writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev)) // payload (withheld)
	writeAllConn(t, h.clientProxy, empty)                                     // terminator

	exc := readServerException(t, h.clientProxy)
	if exc.Code != proto.Error(chproto.CodeTooManyParts) {
		t.Fatalf("exception code = %d, want 252", exc.Code)
	}
	if !bytes.Contains([]byte(exc.Message), []byte("back-pressure")) {
		t.Fatalf("exception message = %q", exc.Message)
	}
	if aborts, completes, successes := hooks.counts(); aborts != 1 || completes != 1 || successes != 0 {
		t.Fatalf("lifecycle abort/complete/success = %d/%d/%d, want 1/1/0", aborts, completes, successes)
	}

	// Statement 2 on the SAME connection must be served normally.
	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q2", "SELECT 1"))
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("second query terminal = %d, want EndOfStream", got[0])
	}
	select {
	case err := <-h.loopErr:
		t.Fatalf("a relay loop exited after the rejection: %v", err)
	default:
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
}

// serveStagedRejectUpstream plays ClickHouse for both statements: the staged
// INSERT (Query, marker, terminator -> sample block, EndOfStream) and the
// following SELECT (Query, marker -> EndOfStream). It asserts the payload was
// never forwarded.
func serveStagedRejectUpstream(t *testing.T, conn net.Conn) error {
	t.Helper()
	codec := chproto.NewCodec(conn, chproto.DirFromClient)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	for _, want := range []string{"INSERT INTO db.t FORMAT Native", "SELECT 1"} {
		pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
		if err != nil {
			return err
		}
		q, ok := pkt.Decoded.(*chproto.Query)
		if !ok || q.Body != want {
			t.Errorf("upstream query = %#v, want %q", pkt.Decoded, want)
		}
		if want == "INSERT INTO db.t FORMAT Native" {
			marker, err := codec.ReadPacket()
			if err != nil {
				return err
			}
			if empty, _ := chproto.ClientDataPacketIsEmpty(marker.Raw, proto.CompressionDisabled); !empty {
				t.Error("upstream received a non-empty Data packet; staged payload must be withheld")
			}
			if _, err := conn.Write(encodeServerSampleDataPacket(t, deferredTestRev)); err != nil {
				return err
			}
		}
		term, err := codec.ReadPacket()
		if err != nil {
			return err
		}
		if empty, _ := chproto.ClientDataPacketIsEmpty(term.Raw, proto.CompressionDisabled); !empty {
			t.Error("upstream received a non-empty Data packet; staged payload must be withheld")
		}
		if _, err := conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			return err
		}
	}
	return nil
}

// readServerException frames one server packet from the client side and
// decodes it as an Exception.
func readServerException(t *testing.T, conn net.Conn) *chproto.Exception {
	t.Helper()
	codec := chproto.NewCodec(conn, chproto.DirFromUpstream)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	pkt, err := codec.ReadPacket(uint64(chproto.ServerExceptionCode))
	if err != nil {
		t.Fatalf("read server exception: %v", err)
	}
	exc, ok := pkt.Decoded.(*chproto.Exception)
	if !ok {
		t.Fatalf("server packet %d is not an Exception", pkt.Type)
	}
	return exc
}
```

`readServerException` reads from the raw client pipe with its own codec; if the harness's pipe semantics make a second codec awkward, read the exception bytes with `readExact` and decode with `chproto`'s exported Exception decoder instead — the assertion that matters is code 252 followed by a working second query.

- [ ] **Step 2: Run and watch it fail**

Run: `bazel test //pkg/proxy:proxy_test --test_filter=TestRelay_StagedRejection_KeepsSessionAndServesNextQuery --test_output=all`
Expected: FAIL — the relay writes the Exception and then returns, so the loops exit and the second query is never served.

- [ ] **Step 3: Add the pending-rejection state to `Relay`**

Next to `activeQuery` / `activeQueryID` / `queryCanceled` (guarded by `queryMu`):

```go
	// pendingRejection replaces the terminal packet of a query that Housegate
	// rejected locally after its input was complete. The upstream still has to
	// finish (its zero-row INSERT commits nothing), but its EndOfStream is
	// bookkeeping: the client must see exactly one terminal packet, and it must
	// be our Exception. Guarded by queryMu.
	pendingRejectionID  string
	pendingRejectionExc *chproto.Exception
```

```go
// rejectActiveQueryTerminal arms the terminal swap for queryID. It returns
// false when that query is no longer the active one, in which case the caller
// must fall back to closing the session.
func (r *Relay) rejectActiveQueryTerminal(queryID string, exc *chproto.Exception) bool {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if !r.activeQuery || r.activeQueryID != queryID || exc == nil {
		return false
	}
	r.pendingRejectionID = queryID
	r.pendingRejectionExc = exc
	return true
}

// takePendingRejection consumes the armed rejection for queryID, if any.
func (r *Relay) takePendingRejection(queryID string) *chproto.Exception {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if r.pendingRejectionExc == nil || r.pendingRejectionID != queryID {
		return nil
	}
	exc := r.pendingRejectionExc
	r.pendingRejectionID, r.pendingRejectionExc = "", nil
	return exc
}
```

- [ ] **Step 4: Arm it from the strict hook**

Declare `curQctxRejected := false` next to `curQctxSawPayload` in `clientToUpstream`, and replace the strict-hook block at `relay.go:1118-1125`:

```go
		if inputComplete && curQctx != nil {
			if err := r.hooks.OnQueryInputCompleteStrict(ctx, curQctx); err != nil {
				if chproto.KeepsSession(err) && curQctx.SuppressUpstreamExecution &&
					r.rejectActiveQueryTerminal(curQctx.Query.ID, exceptionForPluginError(err)) {
					// Spec L D6: throttling must not cost the client its
					// connection. A staged INSERT withheld its payload, so
					// forwarding the terminator below only closes the upstream's
					// zero-row negotiation; upstreamToClient then swaps that
					// terminal packet for this Exception, and the client sees
					// exactly one terminal packet on a session that stays usable.
					r.hooks.OnQueryAbort(ctx, curQctx)
					curQctxRejected = true
					logger.Infow("query rejected without closing the session",
						"query_id", curQctx.Query.ID, "err", err)
				} else {
					r.writeExceptionToClient(ctx, err)
					r.hooks.OnQueryAbort(ctx, curQctx)
					r.hooks.OnQueryComplete(ctx, r.sess)
					return fmt.Errorf("query input complete strict hook: %w", err)
				}
			}
		}
```

and the post-splice block at `relay.go:1163-1168`:

```go
			if inputComplete {
				if !curQctxRejected {
					r.hooks.OnQueryInputComplete(ctx, curQctx)
				}
				curQctx = nil
				curQctxRejected = false
				curQctxSawInitialEmpty = false
				curQctxSawPayload = false
			}
```

`OnQueryComplete` is deliberately not fired here: `upstreamToClient` fires it exactly once when the upstream terminal arrives.

- [ ] **Step 5: Swap the terminal packet**

In `upstreamToClient`, replace the terminal block at `relay.go:1969-1980`:

```go
		if isEndOfStream || isException {
			r.sess.State().ClearActiveRewrite()
			queryID, canceled, active := r.takeActiveQueryState()
			rejection := r.takePendingRejection(queryID)
			if active && isEndOfStream && !canceled && rejection == nil {
				r.hooks.OnQuerySuccess(ctx, r.sess, queryID)
			}
			if deferredTerminal != nil {
				deferredTerminal.complete(ctx, r.hooks, r.sess)
			} else if active {
				r.hooks.OnQueryComplete(ctx, r.sess)
			}
			if rejection != nil {
				if rewrittenException != nil {
					logger.Debugw("upstream exception superseded by a local rejection",
						"query_id", queryID, "upstream_code", rewrittenException.Code)
				}
				rewrittenException = rejection
			}
		}
```

The existing `if rewrittenException != nil { client.WriteException(...); continue }` below then writes our Exception and the loop continues — the session survives.

- [ ] **Step 6: Run the test**

Run: `bazel test //pkg/proxy:proxy_test --test_filter=TestRelay_StagedRejection_KeepsSessionAndServesNextQuery --test_output=all`
Expected: PASS.

- [ ] **Step 7: Run the whole proxy suite (twice, for the flaky lifecycle test)**

Run: `bazel test //pkg/proxy:proxy_test --test_output=errors --runs_per_test=2`
Expected: PASS. `TestServer_ConnLifecycleHooks_FireOnDialFailure` is the known plain-`go test` flake; under Bazel it passes.

- [ ] **Step 8: Commit**

```bash
bazel run //:gazelle
git add pkg/proxy
git commit -m "feat(proxy): let a retryable rejection end the query without ending the session"
```

### Task 22: housegate — the same rejection class on the deferred-INSERT path

**Files:**
- Modify: `pkg/proxy/relay.go` (`clientToUpstream:1005-1008`, `runDeferredInsert:1198-1315`)
- Modify: `pkg/proxy/relay_reject_test.go`

**Interfaces:**
- Produces: `errQueryRejectedResume` (unexported sentinel) — `runDeferredInsert` returns it wrapped when it rejected without closing; `clientToUpstream` continues its loop on it.

- [ ] **Step 1: Write the failing test** — append to `pkg/proxy/relay_reject_test.go`

```go
// deferredRejectHooks is the agent-side shape: Relay answers the sample block
// locally, buffers the payload, and the strict hook refuses with a retryable
// 252. Nothing was forwarded upstream, so the session must survive.
type deferredRejectHooks struct {
	stagedRejectHooks
}

func (h *deferredRejectHooks) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.queries = append(h.queries, qctx.Query.Body)
	if len(h.queries) == 1 {
		qctx.DeferredInsert = &plugin.DeferredInsertPlan{
			SampleColumns:   []chproto.SampleColumn{{Name: "v", Type: "UInt64"}},
			MaxPayloadBytes: 1 << 20,
		}
	}
	return nil
}

func TestRelay_DeferredRejection_KeepsSessionAndServesNextQuery(t *testing.T) {
	hooks := &deferredRejectHooks{stagedRejectHooks{rejectOne: true}}
	h := newDeferredHarness(t, hooks)
	empty := encodeEmptyClientData(t)
	sample := encodeServerSampleDataPacket(t, deferredTestRev)

	upDone := make(chan error, 1)
	go func() { upDone <- serveSecondQueryOnlyUpstream(t, h.upstreamProxy) }()

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q1", "INSERT INTO db.t FORMAT Native"))
	if got := readExact(t, h.clientProxy, len(sample)); !bytes.Equal(got, sample) {
		t.Fatalf("client sample block = %x, want the locally synthesized one", got)
	}
	writeAllConn(t, h.clientProxy, empty)
	writeAllConn(t, h.clientProxy, encodeNonEmptyClientDataPacket(t, deferredTestRev))
	writeAllConn(t, h.clientProxy, empty)

	exc := readServerException(t, h.clientProxy)
	if exc.Code != proto.Error(chproto.CodeTooManyParts) {
		t.Fatalf("exception code = %d, want 252", exc.Code)
	}
	if aborts, completes, successes := hooks.counts(); aborts != 1 || completes != 1 || successes != 0 {
		t.Fatalf("lifecycle abort/complete/success = %d/%d/%d, want 1/1/0", aborts, completes, successes)
	}

	writeAllConn(t, h.clientProxy, encodeInsertQuery(t, "q2", "SELECT 1"))
	writeAllConn(t, h.clientProxy, empty)
	if got := readExact(t, h.clientProxy, 1); got[0] != byte(chproto.ServerEndOfStreamCode) {
		t.Fatalf("second query terminal = %d, want EndOfStream", got[0])
	}
	select {
	case err := <-h.loopErr:
		t.Fatalf("a relay loop exited after the deferred rejection: %v", err)
	default:
	}
	if err := <-upDone; err != nil {
		t.Fatalf("upstream flow: %v", err)
	}
}

// serveSecondQueryOnlyUpstream proves the rejected deferred INSERT never
// reached upstream: the first packet it sees is the SELECT.
func serveSecondQueryOnlyUpstream(t *testing.T, conn net.Conn) error {
	t.Helper()
	codec := chproto.NewCodec(conn, chproto.DirFromClient)
	codec.SetRevision(deferredTestRev)
	codec.SetCompression(proto.CompressionDisabled)
	pkt, err := codec.ReadPacket(uint64(chproto.ClientQueryCode))
	if err != nil {
		return err
	}
	if q, ok := pkt.Decoded.(*chproto.Query); !ok || q.Body != "SELECT 1" {
		t.Errorf("upstream first query = %#v, want the SELECT (the rejected INSERT must not be forwarded)", pkt.Decoded)
	}
	if _, err := codec.ReadPacket(); err != nil {
		return err
	}
	_, err = conn.Write([]byte{byte(chproto.ServerEndOfStreamCode)})
	return err
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `bazel test //pkg/proxy:proxy_test --test_filter=TestRelay_DeferredRejection_KeepsSessionAndServesNextQuery --test_output=all`
Expected: FAIL — `runDeferredInsert` returns the error, `clientToUpstream` returns it, the session closes.

- [ ] **Step 3: Add the resume sentinel and the resume path**

Next to `errOpaqueConnectionNotReusable` (`relay.go:88-90`):

```go
// errQueryRejectedResume marks a deferred INSERT that Housegate rejected after
// consuming the client's complete input. Nothing reached upstream, so the
// session stays usable and clientToUpstream continues its loop.
var errQueryRejectedResume = errors.New("query rejected without closing the session")
```

In `runDeferredInsert`, next to `rejectClose` (`:1210-1215`):

```go
	// rejectResume ends the query without ending the session. It is only valid
	// here: the terminator has been consumed, so the client's input stream is at
	// a clean boundary, and nothing has been written upstream, so this goroutine
	// owns the client writer (exactly as rejectClose already assumes).
	rejectResume := func(err error) error {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.writeExceptionToClient(ctx, err)
		return fmt.Errorf("%w: %w", errQueryRejectedResume, err)
	}
```

and replace `:1313-1315`:

```go
	if err := r.hooks.OnQueryInputCompleteStrict(ctx, qctx); err != nil {
		if chproto.KeepsSession(err) {
			return rejectResume(fmt.Errorf("query input complete strict hook: %w", err))
		}
		return rejectClose(fmt.Errorf("query input complete strict hook: %w", err))
	}
```

In `clientToUpstream` (`:1005-1008`):

```go
				if err := r.runDeferredInsert(ctx, qctx, clientCompression); err != nil {
					if errors.Is(err, errQueryRejectedResume) {
						logger.Infow("deferred INSERT rejected without closing the session",
							"query_id", q.ID, "err", err)
						continue
					}
					return err
				}
```

Do **not** set `rejectedQctx`: the deferred path already consumed the terminator, so there is no leftover input to drain, and setting it would reject the client's next Query at `relay.go:948`.

- [ ] **Step 4: Run the tests**

Run: `bazel test //pkg/proxy:proxy_test --test_output=errors --runs_per_test=2`
Expected: PASS, including every pre-existing `TestRelay_DeferredInsert_*`.

- [ ] **Step 5: Commit**

```bash
git add pkg/proxy
git commit -m "feat(proxy): keep the session on a retryable deferred-INSERT rejection"
```

### Task 23: housegate — end-to-end proof through a real client on one connection

**Files:**
- Create: `pkg/integration/storage_backpressure_session_test.go`
- Modify: `pkg/integration/testenv/cli.go` (one new helper)

**Interfaces:**
- Consumes: `testenv.StartServerProxy` / `StartAgentProxy` / `WithRewriterMock` / `WithConfigMutator` / `withDeclaredSchema` / `authProxyConfig` / `siAgentSchema` (all in `pkg/integration/storage_integrity_agent_test.go`), `testenv.ClickHouseCLI`.
- Produces: `testenv.RunCLIMultiqueryIgnoreError(t, bin, proxyAddr, database, query) (string, error)` — uncompressed, `--multiquery --ignore-error`, so the CLI keeps its connection after a server exception.

**Why the CLI and not clickhouse-go.** `clickhouse-go`'s `(*clickhouse).release` closes the TCP connection whenever `batch.Send()` returns any error, including a server Exception (fork `v2.47.0-sentioxyz-20260629`, `clickhouse.go:385-388`). A Go-driver test therefore cannot observe session survival no matter what the proxy does. The official client keeps the connection and continues with `--ignore-error`, so it can.

- [ ] **Step 1: Add the CLI helper** — `pkg/integration/testenv/cli.go`

```go
// RunCLIMultiqueryIgnoreError runs several semicolon-separated statements on
// ONE client connection and keeps going after a server exception. Compression
// stays off because the storage-integrity lanes capture raw Native blocks.
func RunCLIMultiqueryIgnoreError(t *testing.T, bin, proxyAddr, database, query string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return runCLIContext(ctx, t, bin, proxyAddr, database, query, "--multiquery", "--ignore-error")
}
```

- [ ] **Step 2: Write the failing test** — `pkg/integration/storage_backpressure_session_test.go`

```go
package integration

import (
	"context"
	"strings"
	"sync"
	"testing"

	housegate "github.com/housegate/housegate"
	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/registry"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

// throttlingConsumer refuses the first admission the way the real ingress does
// under back-pressure, then accepts.
type throttlingConsumer struct {
	mu   sync.Mutex
	seen int
}

func (c *throttlingConsumer) ConsumeStorageIntegrityAdmission(_ context.Context, _ siplugin.Admission) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen++
	if c.seen > 1 {
		return nil
	}
	return &chproto.ClientError{
		Code:        chproto.CodeTooManyParts,
		Message:     "storage_integrity: back-pressure: hg_unsafe.db__si_events partition p_eu has 2400 active parts (soft limit 2400); retry later",
		Err:         sicore.ErrBackpressure,
		KeepSession: true,
	}
}

// Spec L D6 acceptance: the client receives Exception 252 and the connection
// remains usable for a subsequent query.
func TestStorageIntegrity_BackpressureKeepsTheClientSession(t *testing.T) {
	const networkID = "itest-net"
	bin := testenv.ClickHouseCLI(t)
	signer, err := auth.NewRelaySigner(authTestKey1)
	if err != nil {
		t.Fatal(err)
	}
	ch := openConn(t, chEnv.Addr)
	if err := ch.Exec(context.Background(),
		"CREATE TABLE IF NOT EXISTS "+chEnv.Database+".si_events (id UInt64, region String) ENGINE = MergeTree ORDER BY id"); err != nil {
		t.Fatalf("create table: %v", err)
	}
	consumer := &throttlingConsumer{}
	rewriterOpt, rewriterMock := testenv.WithRewriterMock(t)
	rewriterMock.SetAccessedTables("INSERT INTO "+chEnv.Database+".si_events", []*pb.AccessedTable{{
		OriginalDatabase:   chEnv.Database,
		OriginalTable:      "si_events",
		LogicalDatabase:    chEnv.Database,
		PhysicalDatabase:   chEnv.Database,
		IsStorageIntegrity: true,
	}})
	server := testenv.StartServerProxy(t, chEnv.Addr,
		rewriterOpt,
		authProxyConfig([]string{signer.Address()}, false),
		testenv.WithDatabasePermission(signer.Address(), chEnv.Database, registry.DbAuthWrite),
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Ingress.Enabled = true
			cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{signer.Address()}
			cfg.StorageIntegrity.Ingress.NetworkID = networkID
		}),
		func(_ *config.Config, opts *housegate.Options) {
			opts.StorageIntegrityAdmissionConsumer = consumer
		},
	)
	agentProxy := testenv.StartAgentProxy(t, authTestKey1, server.Addr,
		withDeclaredSchema(t, networkID),
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.StorageIntegrity.Agent.Enabled = true
			cfg.StorageIntegrity.Agent.NetworkID = networkID
			cfg.StorageIntegrity.Agent.StateDir = t.TempDir()
			cfg.StorageIntegrity.Agent.RequireNetworkState = false
		}),
	)

	out, err := testenv.RunCLIMultiqueryIgnoreError(t, bin, agentProxy.Addr, chEnv.Database,
		"INSERT INTO "+chEnv.Database+".si_events VALUES (1, 'eu'); SELECT 42")
	if err == nil {
		t.Fatalf("the throttled INSERT must surface an error; out: %s", out)
	}
	if !strings.Contains(out, "252") && !strings.Contains(out, "TOO_MANY_PARTS") {
		t.Fatalf("client did not see exception 252:\n%s", out)
	}
	if !strings.Contains(out, "back-pressure") {
		t.Fatalf("client did not see the back-pressure message:\n%s", out)
	}
	// The decisive assertion: the SELECT ran on the same client connection
	// after the rejection.
	if !strings.Contains(out, "42") {
		t.Fatalf("the connection did not survive the throttle; the follow-up query never ran:\n%s", out)
	}
	consumer.mu.Lock()
	seen := consumer.seen
	consumer.mu.Unlock()
	if seen != 1 {
		t.Fatalf("consumer saw %d admissions, want exactly the refused one", seen)
	}
}
```

- [ ] **Step 3: Run it**

Run: `bazel test //pkg/integration:integration_test --test_filter=TestStorageIntegrity_BackpressureKeepsTheClientSession --test_output=all`
Expected: PASS. If the CLI is absent the test skips (`ClickHouseCLI` calls `t.Skip`); install it as CI does (`curl -sSL https://clickhouse.com/install.sh | sh && mv clickhouse tests/bin/`).

If the CLI turns out to reconnect between multiquery statements in this ClickHouse build, the assertion weakens rather than breaks — in that case add `--max_client_network_bandwidth` style noise? No: instead assert on the proxy side that no session was torn down, by capturing `clickhouse_proxy_active_connections` before and after via `prometheus.DefaultGatherer` in the same test. Prefer the CLI assertion; use the gauge only as a fallback and say so in a comment.

- [ ] **Step 4: Commit**

```bash
bazel run //:gazelle
git add pkg/integration pkg/integration/testenv
git commit -m "test(integration): prove a throttled SI INSERT leaves the client session usable"
```

### Task 24: housegate — D7 database-name pinning that runs whenever any SI feature is enabled

**Files:**
- Modify: `pkg/config/storage_integrity_config.go:12-18` (add the promote constant), `:195-311` (validation)
- Modify: `pkg/config/storage_integrity_config_test.go`

**Interfaces:**
- Produces (used by Task 26): `config.StorageIntegrityPromoteDatabase = "hg_promote"`; `StorageIntegrityConfig.validate` enforces the unsafe/safe pins whenever they are set **or** back-pressure is enabled, independently of `Runtime.Enabled`.

- [ ] **Step 1: Write the failing test**

```go
// Spec L §1g: the name pins lived inside `if Runtime.Enabled { if bp.Enabled {`,
// so a config with runtime disabled and backpressure enabled passed validation
// with a wrong database name.
func TestStorageIntegrity_DatabaseNamesArePinnedWithoutRuntime(t *testing.T) {
	cfg := Default()
	cfg.StorageIntegrity.Tables = []string{"db.t"}
	cfg.StorageIntegrity.Runtime.Enabled = false
	cfg.StorageIntegrity.Runtime.Backpressure.Enabled = true
	cfg.StorageIntegrity.Runtime.Backpressure.UnsafeDatabase = "hg_unsafe_typo"
	err := cfg.StorageIntegrity.validate(ModeServer)
	if err == nil || !strings.Contains(err.Error(), "unsafe_database") {
		t.Fatalf("validate = %v, want a pinned-name rejection", err)
	}

	cfg = Default()
	cfg.StorageIntegrity.Tables = []string{"db.t"}
	cfg.StorageIntegrity.Runtime.Backpressure.SafeDatabase = "hg_safe_typo"
	if err := cfg.StorageIntegrity.validate(ModeServer); err == nil ||
		!strings.Contains(err.Error(), "safe_database") {
		t.Fatalf("validate = %v, want a pinned-name rejection even with backpressure disabled", err)
	}
}

func TestStorageIntegrityPromoteDatabaseIsPinned(t *testing.T) {
	if StorageIntegrityPromoteDatabase != "hg_promote" {
		t.Fatalf("promote database pin = %q", StorageIntegrityPromoteDatabase)
	}
}
```

- [ ] **Step 2: Run and watch it fail**

Run: `bazel test //pkg/config:config_test --test_filter='TestStorageIntegrity(_DatabaseNamesArePinnedWithoutRuntime|PromoteDatabaseIsPinned)' --test_output=errors`
Expected: FAIL — both configs validate cleanly; `StorageIntegrityPromoteDatabase` undefined.

- [ ] **Step 3: Implement**

Add the constant next to the other two (`:12-18`):

```go
	// StorageIntegrityPromoteDatabase is the promotion shadow database. Housegate
	// never reads it, but it is part of the D2 naming freeze and hosts
	// (sentio-node, arbiter-core roles) pin against this constant so the three
	// names have one definition.
	StorageIntegrityPromoteDatabase = "hg_promote"
```

Add the shared validator and call it from `validate()` **before** the `if !c.Ingress.Enabled { ... return }` short-circuit:

```go
// validateDatabaseNames pins the physical storage-integrity database names.
// Spec L D7: this must not be nested inside the runtime/back-pressure enable
// checks, or a config that only enables one of them slips a wrong database
// name past both validators.
func (c StorageIntegrityConfig) validateDatabaseNames() []error {
	bp := c.Runtime.Backpressure
	unsafeDatabase := strings.TrimSpace(bp.UnsafeDatabase)
	safeDatabase := strings.TrimSpace(bp.SafeDatabase)
	var errs []error
	if bp.Enabled {
		if unsafeDatabase == "" {
			errs = append(errs, errors.New("storage_integrity.runtime.backpressure.unsafe_database is required when backpressure is enabled"))
		}
		if safeDatabase == "" {
			errs = append(errs, errors.New("storage_integrity.runtime.backpressure.safe_database is required when backpressure is enabled"))
		}
	}
	if unsafeDatabase != "" && unsafeDatabase != StorageIntegrityUnsafeDatabase {
		errs = append(errs, fmt.Errorf("storage_integrity.runtime.backpressure.unsafe_database must be %q", StorageIntegrityUnsafeDatabase))
	}
	if safeDatabase != "" && safeDatabase != StorageIntegritySafeDatabase {
		errs = append(errs, fmt.Errorf("storage_integrity.runtime.backpressure.safe_database must be %q", StorageIntegritySafeDatabase))
	}
	if unsafeDatabase != "" && unsafeDatabase == safeDatabase {
		errs = append(errs, errors.New("storage_integrity.runtime.backpressure.safe_database must differ from unsafe_database"))
	}
	return errs
}
```

In `validate()`, immediately after the `c.Tables` loop:

```go
	errs = append(errs, c.validateDatabaseNames()...)
```

and delete the four name checks now living inside the `if bp := c.Runtime.Backpressure; bp.Enabled {` block (the limit and interval checks stay there).

- [ ] **Step 4: Run the tests**

Run: `bazel test //pkg/config:config_test //:housegate_test --test_output=errors`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/config
git commit -m "fix(config): pin the storage-integrity database names outside the runtime enable check"
```

### Task 25: housegate — docs, PR and release

**Files:**
- Modify: `CLAUDE.md` (the `pkg/storageintegrity/` bullet and the `pkg/config` back-pressure description)
- Modify: `docs/` operator notes if the repo has a back-pressure runbook section (grep for `back-pressure` under `docs/`)

**Interfaces:**
- Produces (used by Task 26): `HOUSEGATE_TAG_D`, `HOUSEGATE_COMMIT_D`.

- [ ] **Step 1: Update `CLAUDE.md`**

In the `pkg/storageintegrity/` bullet, replace the sentence describing `PartsPressureGuard`'s snapshot with: the guard now keeps per-key freshness and generation; the interval poll is a bounded `count()` aggregate over both databases plus one exact per-table read for keys that hold reservations or claims; admission reads only the `(table, partition)` pairs the statement touches; `hg_safe` part names are never read; admission is serialized per table; a stuck cleanup proof fences only its own table; `refresh_timeout` and `snapshot_ttl` are config keys. In the relay/`chproto` section, note that `ClientError.KeepSession` makes a rejection end the query rather than the session, and that back-pressure (252) uses it on both the staged and deferred INSERT paths.

- [ ] **Step 2: Full local gate**

```bash
bazel build //...
bazel test //... --test_output=errors
bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_output=errors
```
Expected: PASS. Compare any failure against a clean `main` build before calling it a regression.

- [ ] **Step 3: PR**

```bash
git push -u origin feat/pressure-bounded-reads
gh pr create --title "feat(storage-integrity): bounded pressure reads and a non-closing 252 (Spec L D3/D6/D7)" \
  --body "$(cat <<'EOF'
Spec L: docs/superpowers/specs/2026-08-19-storage-integrity-table-backpressure-hardening-design.md

- D3: the admission path reads only the (table, partition) pairs the statement touches; the interval poll is a bounded count() aggregate plus one exact per-table read for keys that hold ownership; hg_safe part names (the unbounded set, since its merges are pinned off) are never read; per-key freshness/generation replace the global snapshot flag so a scoped read never implies absence outside its scope; admission is serialized per table; refresh_timeout and snapshot_ttl are config keys. Docker test builds a table with thousands of parts and proves the hot-path query stays bounded.
- D6: chproto.ClientError.KeepSession ends the query without ending the session. On the staged (SuppressUpstreamExecution) path the upstream's terminal packet is swapped for our Exception so the client sees exactly one terminal; on the deferred path nothing reached upstream and the loop simply resumes. Note: Spec L §1f names runDeferredInsert, but the server ingress plugin uses SuppressUpstreamExecution (pkg/plugins/storageintegrity/plugin.go:280) — both paths are covered.
- D7: the hg_unsafe/hg_safe pins now run whenever the names are set or back-pressure is enabled, not only under runtime.enabled; hg_promote is pinned as a constant; a stuck cleanup proof fences only its own table.
EOF
)"
```

- [ ] **Step 4: Merge and release**

```bash
gh workflow run release.yml --ref main -f bump=minor
gh run watch "$(gh run list --workflow=release.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
git fetch --tags
TAG=$(git describe --tags --abbrev=0 origin/main)
echo "HOUSEGATE_TAG_D=$TAG"
echo "HOUSEGATE_COMMIT_D=$(git rev-list -n 1 "$TAG")"
```
Expected: a non-draft, non-prerelease tag. `bump=minor` because the config surface and `ddl`-facing API both changed.

---

## Phase E — sentio-node: pins, D7 alignment, mirror gating, smoke

### Task 26: sentio-node — bump pins, align the name validator, gate the mirror limit

**Repo / cwd:** `/Users/uranuswch/Dev/sentio_xyz/sentio-node` (branch `feat/si-hardening-pins` off `origin/main`).

**Files:**
- Modify: `go.mod:9,15` (housegate, arbiter-core), `MODULE.bazel` (both `bazel_dep` versions + `git_override` commits)
- Modify: `config/config.go:30-32` (constants), `:427-476` (`validateStorageIntegrity`), `:56-67` (`StorageIntegritySNode`)
- Modify: `standalone/standalone.go:281-304` (mirror limit + schema source)
- Modify: `storageintegrityadapter/adapter.go:36-44` (`ProtocolTablesMode`)
- Modify: `config/config_test.go`

**Interfaces:**
- Consumes: `HOUSEGATE_TAG_D` / `HOUSEGATE_COMMIT_D` (Task 25), `ARBITER_CORE_TAG_B` / `ARBITER_CORE_COMMIT_B` (Task 11), `config.StorageIntegrityPromoteDatabase` (Task 24), `ddl.SchemaSource*` (Task 7), `snode.Config.SchemaSource` (Task 7).
- Produces: `storageintegrityadapter.ProtocolSchemaSource(schemaSource string) ddl.SchemaSource` replaces `ProtocolTablesMode`.

- [ ] **Step 1: Bump both pins**

```bash
go get github.com/housegate/housegate@"$HOUSEGATE_TAG_D"
go get github.com/sentioxyz/arbiter-core@"$ARBITER_CORE_TAG_B"
```
Then update `MODULE.bazel`'s `bazel_dep` versions and `git_override` commits for both modules, and run:

```bash
bazel run @rules_go//go -- mod tidy
./scripts/update-bazel-deps.sh
bazel build //...
```
Expected: PASS.

- [ ] **Step 2: Write the failing tests** — `config/config_test.go`

```go
func TestValidateStorageIntegrity_PinsDatabaseNamesWithoutBackpressure(t *testing.T) {
	cfg := base // the existing valid fixture in this file
	cfg.Housegate.StorageIntegrity.Runtime.Backpressure = housegateConfig.Default().StorageIntegrity.Runtime.Backpressure
	cfg.Housegate.StorageIntegrity.Runtime.Backpressure.Enabled = false
	cfg.Housegate.StorageIntegrity.Runtime.Backpressure.UnsafeDatabase = "hg_unsafe_other"
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsafe_database")
}

func TestValidateStorageIntegrity_RequiresSchemaSource(t *testing.T) {
	cfg := base
	cfg.StorageIntegrity.SNode.SchemaSource = ""
	err := cfg.Validate()
	require.Error(t, err)
	require.Contains(t, err.Error(), "schema_source")
}

func TestValidateStorageIntegrity_PinsPromoteDatabase(t *testing.T) {
	require.Equal(t, housegateConfig.StorageIntegrityPromoteDatabase, StorageIntegrityPromoteDatabase)
}
```

And in `standalone/storage_integrity_schemas_test.go` (or a new `standalone/storage_integrity_mirror_test.go`):

```go
// Spec L §1g: the SNode mirror limit was fed unconditionally, so disabling
// ingress back-pressure with a zero value silently reverted the source-side
// mirror to its 2950 default.
func TestMirrorHardPartsLimit_IsZeroWhenBackpressureIsDisabled(t *testing.T) {
	cfg := validStorageIntegrityConfig(t)
	cfg.Housegate.StorageIntegrity.Runtime.Backpressure.Enabled = false
	require.Equal(t, 0, mirrorHardPartsPerPartition(cfg))

	cfg.Housegate.StorageIntegrity.Runtime.Backpressure.Enabled = true
	cfg.Housegate.StorageIntegrity.Runtime.Backpressure.HardPartsPerPartition = 2950
	require.Equal(t, 2950, mirrorHardPartsPerPartition(cfg))
}
```

- [ ] **Step 3: Run and watch them fail**

Run: `bazel test //config:config_test //standalone:standalone_test --test_output=errors`
Expected: FAIL — the disabled-back-pressure name check passes today, `schema_source` is optional (`config.go:113-119` accepts `""`), and `mirrorHardPartsPerPartition` does not exist.

- [ ] **Step 4: Require the schema source and pin the names**

In `config/config.go`'s `StorageIntegrityConfig.Validate`, replace the permissive switch:

```go
	switch c.SNode.SchemaSource {
	case "clickhouse", "network_state":
	default:
		errs = append(errs, fmt.Errorf(
			"storage_integrity.snode.schema_source must be clickhouse or network_state, got %q (it derives the protocol-table mode; there is no default)",
			c.SNode.SchemaSource,
		))
	}
```

In `validateStorageIntegrity`, replace the two `backpressure.Enabled &&` guarded name checks with unconditional ones (housegate's own validator now covers the same ground; keeping sentio-node's copy is deliberate defence in depth, and it must not disagree):

```go
	backpressure := c.Housegate.StorageIntegrity.Runtime.Backpressure
	if name := strings.TrimSpace(backpressure.UnsafeDatabase); name != "" && name != StorageIntegrityUnsafeDatabase {
		return fmt.Errorf(
			"housegate.storage_integrity.runtime.backpressure.unsafe_database (%q) must equal %q",
			backpressure.UnsafeDatabase, StorageIntegrityUnsafeDatabase,
		)
	}
	if name := strings.TrimSpace(backpressure.SafeDatabase); name != "" && name != StorageIntegritySafeDatabase {
		return fmt.Errorf(
			"housegate.storage_integrity.runtime.backpressure.safe_database (%q) must equal %q",
			backpressure.SafeDatabase, StorageIntegritySafeDatabase,
		)
	}
```

Point the three local constants at housegate's so the names have one definition:

```go
const (
	StorageIntegrityUnsafeDatabase  = housegateConfig.StorageIntegrityUnsafeDatabase
	StorageIntegritySafeDatabase    = housegateConfig.StorageIntegritySafeDatabase
	StorageIntegrityPromoteDatabase = housegateConfig.StorageIntegrityPromoteDatabase
)
```

- [ ] **Step 5: Gate the mirror limit**

In `standalone/standalone.go`, add next to the other helpers:

```go
// mirrorHardPartsPerPartition feeds the SNode's defence-in-depth hard limit
// ONLY when ingress back-pressure is enabled. Spec L §1g: feeding it
// unconditionally meant that disabling back-pressure left the source-side
// mirror silently running at the 2950 default, so the two halves of the
// throttle disagreed about whether it existed. Zero means "no mirror".
func mirrorHardPartsPerPartition(cfg *config.Config) int {
	backpressure := cfg.Housegate.StorageIntegrity.Runtime.Backpressure
	if !backpressure.Enabled {
		return 0
	}
	return backpressure.HardPartsPerPartition
}
```

and use it in the `snode.New(snode.Config{...})` literal (`:297-298`):

```go
				HardPartsPerPartition: mirrorHardPartsPerPartition(cfg),
```

Note `snode.Config.validate()` turns 0 into `DefaultHardPartsPerPartition`. To make "no mirror" real, the arbiter-core side must treat a *disabled* mirror explicitly — pass the sentinel the role understands: keep 0 here and, in the same PR against arbiter-core if you are the same author, or as a follow-up issue if not, document that 0 means default. **For this plan: 0 keeps today's arbiter-core behaviour, and the fix that matters is that the number is now tied to the same enable flag as the ingress half.** State that explicitly in the PR body.

- [ ] **Step 6: Follow arbiter-core's derived-mode API**

In `storageintegrityadapter/adapter.go`, replace `ProtocolTablesMode`:

```go
// ProtocolSchemaSource maps sentio-node's configured schema source onto
// arbiter-core's enum. arbiter-core derives the protocol-table mode from it
// (Spec L D2); the mode is no longer something a host chooses.
func ProtocolSchemaSource(schemaSource string) ddl.SchemaSource {
	if schemaSource == "network_state" {
		return ddl.SchemaSourceNetworkState
	}
	return ddl.SchemaSourceClickHouse
}
```

In `standalone/standalone.go`, replace `protocolTablesMode := storageintegrityadapter.ProtocolTablesMode(si.SNode.SchemaSource)` with `protocolSource := storageintegrityadapter.ProtocolSchemaSource(si.SNode.SchemaSource)`, pass `SchemaSource: protocolSource` in the `snode.Config` literal instead of `ProtocolTables: protocolTablesMode`, and change the two later uses:

- the bootstrap branch condition becomes `if protocolSource == ddl.SchemaSourceNetworkState {`
- the `ddl.EnsureProtocolTables(...)` call needs a mode: `mode, err := ddl.ModeFromSchemaSource(protocolSource)` right above the closure, returning the error if any.

- [ ] **Step 7: Run the tests**

Run: `bazel test //... --test_output=errors`
Expected: PASS (the `SENTIO_SI_E2E` smoke stays skipped but must still compile).

- [ ] **Step 8: Commit and PR**

```bash
git add go.mod go.sum MODULE.bazel MODULE.bazel.lock config standalone storageintegrityadapter
git commit -m "feat(storage-integrity): follow the derived protocol-table mode and gate the mirror limit"
git push -u origin feat/si-hardening-pins
gh pr create --title "feat(storage-integrity): Spec L pin bumps, name pinning and mirror gating" --body "Pins housegate $HOUSEGATE_TAG_D and arbiter-core $ARBITER_CORE_TAG_B. schema_source is now required (it derives the protocol-table mode). The hg_unsafe/hg_safe name pins no longer hide behind backpressure.enabled, and the three names delegate to housegate's constants. The SNode mirror hard limit is fed only when ingress back-pressure is enabled; note that arbiter-core still substitutes its 2950 default for 0, so 'disabled' currently means 'source-side default' — the change here is that the two halves are finally driven by one flag."
```

### Task 27: sentio-node — smoke coverage for the type reject and the survivable throttle, then the first tag

**Files:**
- Modify: `standalone/storage_integrity_smoke_test.go`

**Interfaces:**
- Consumes: everything above; `ddl.EnsureProtocolTables`, `payloadexec.ErrUnsupportedColumnType`, `chproto`'s exception decoding already used by `runStagedInsertWire`.

- [ ] **Step 1: Add the type-validation smoke case**

Append to `standalone/storage_integrity_smoke_test.go` (inside the `SENTIO_SI_E2E`-gated test, after the existing protocol-table verification at `:301-312`):

```go
	// Spec L D1 acceptance in a live node: a declaration whose column type is
	// outside the storage-integrity whitelist is refused before any DDL runs.
	badSchema := unsafeIntentSchema // the schema this smoke already declares
	badSchema.TableID = badSchema.TableID + "_badtype"
	badSchema.Columns = append(badSchema.Columns, lthash.Column{Name: "bad", Type: "Nullable(String)"})
	err = ddl.EnsureProtocolTables(t.Context(), chConn, pinned, []payloadexec.TableSchema{badSchema}, ddl.ModeCreateAndVerify, newSlogLogger(logger))
	require.ErrorIs(t, err, payloadexec.ErrUnsupportedColumnType)
	var created uint64
	require.NoError(t, chConn.QueryRow(t.Context(),
		"SELECT count() FROM system.tables WHERE database IN (?, ?, ?) AND name = ?",
		config.StorageIntegrityUnsafeDatabase, config.StorageIntegritySafeDatabase, config.StorageIntegrityPromoteDatabase,
		snode.CHTableName(badSchema.TableID),
	).Scan(&created))
	require.Zero(t, created, "a rejected declaration must not create any protocol table")
```

- [ ] **Step 2: Add the survivable-throttle smoke case**

The smoke already drives a staged INSERT over the wire with `runStagedInsertWire` and reads server packets with `readUntilServerPacket`. Add a follow-up on the **same** `net.Conn`:

```go
	// Spec L D6 acceptance in a live node: a back-pressure refusal ends the
	// statement, not the connection. Drive a second statement on the same wire
	// after a refused one and require a normal EndOfStream.
	conn, err := net.Dial("tcp", listenAddr)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, runStagedInsertWire(t.Context(), conn, throttledInsertRequest(t, cfg)))
	require.NoError(t, runSimpleQueryWire(t.Context(), conn, simpleQueryWireRequest{Query: "SELECT 42"}),
		"the session must stay usable after a back-pressure refusal")
```

`throttledInsertRequest` builds the same signed request the smoke already builds, against a partition that has been pushed over the configured soft limit by the preceding inserts; if the environment cannot reach the limit, set `storage_integrity.runtime.backpressure.soft_parts_per_partition: 1` in the smoke's config fixture so the second INSERT is refused deterministically.

- [ ] **Step 3: Compile and (where the operator environment exists) run**

Run: `bazel test //standalone:standalone_test --test_output=errors`
Expected: PASS — the smoke skips without `SENTIO_SI_E2E=1` but must compile.

With the operator environment:

```bash
SENTIO_SI_E2E=1 SENTIO_SI_CONFIG=... SENTIO_SI_CLICKHOUSE_USER=... \
  SENTIO_SI_STATEMENT_ID=... SENTIO_SI_SIGNER_KEY_HEX=... SENTIO_SI_SCHEMA_HASH=... \
  bazel test //standalone:standalone_test --test_filter=TestStorageIntegritySmoke \
    --test_env=SENTIO_SI_E2E --test_env=SENTIO_SI_CONFIG --test_env=SENTIO_SI_CLICKHOUSE_USER \
    --test_env=SENTIO_SI_STATEMENT_ID --test_env=SENTIO_SI_SIGNER_KEY_HEX --test_env=SENTIO_SI_SCHEMA_HASH \
    --test_output=all
```
Expected: PASS. If the environment is unavailable, record that the compile/skip gate passed and no live pass is claimed — exactly as the Spec C plan's publication note did.

- [ ] **Step 4: Commit, PR, and cut sentio-node's first tag**

```bash
git add standalone/storage_integrity_smoke_test.go
git commit -m "test(smoke): cover the SI type reject and the survivable throttle"
git push
gh pr create --title "test(storage-integrity): Spec L smoke coverage" --body "Adds the D1 type-reject assertion (no protocol table is created) and the D6 survivable-throttle assertion (a second statement succeeds on the same wire after a 252)."
```

After merge, cut the repo's first tag so its commits become addressable from the other repos (the roadmap's §5 bounded task):

```bash
git fetch origin && git checkout main && git pull
git tag -a v0.1.0 -m "sentio-node v0.1.0"
git push origin v0.1.0
```

---

## Self-Review

**1. Spec coverage.** See the map below; every decision D1–D7 and every bullet of Spec L §4 has at least one task, and §5's four delivery steps map to Phases B/C/D/E.

**2. Placeholder scan.** No `TBD`, no "add error handling", no "similar to Task N". Every code step carries the code. Two deliberately parameterised values remain and are parameterised *because they are outputs of a workflow run, not choices*: the release tags (`HOUSEGATE_TAG_A/D`, `ARBITER_CORE_TAG_B`, `ARBITER_TAG_C`) and their commits. Each is produced by an explicit step that prints it.

**3. Type consistency.** Cross-checked names used across tasks: `payloadexec.SupportedColumnType` / `ValidateColumnType` / `ValidateTableSchemaColumns` / `ErrUnsupportedColumnType` (Task 1 → 4, 5, 6, 9, 27); `ddl.Intents` four-return and `ddl.BuildDDL` four-return (Task 4 → 8, 12); `ddl.SchemaSource*` / `ModeFromSchemaSource` (Task 7 → 12, 26); `ddl.ValidatePartitionFreeze` (Task 5 → 9's fatal set); `ddl.FatalReconcileError` / `ReconcileBackoff` / `DefaultReconcileMaxFailures` (Task 9); `snode.ErrPromoteTableMissing` (Task 8 → 10); `PartsScope` with `Covers` / `IsFull` (Task 13 → 14, 15, 19); `refreshScope` / `RefreshCounts` / `RefreshLiveKeys` / `BuildExactPartsQuery` / `BuildAggregateSnapshotQuery` (Tasks 13–15 → 17, 19); `lockTable` / `lockAllTables` (Task 16, used by Task 15's `reserve`, which is why Task 15 says to keep `admissionMu` until Task 16 swaps it); `chproto.ClientError.KeepSession` / `chproto.KeepsSession` (Task 20 → 21, 22, 23); `rejectActiveQueryTerminal` / `takePendingRejection` (Task 21); `errQueryRejectedResume` (Task 22); `config.StorageIntegrityPromoteDatabase` (Task 24 → 26); `mirrorHardPartsPerPartition` (Task 26); `storageintegrityadapter.ProtocolSchemaSource` (Task 26).

**4. Two findings that contradict the spec's own wording, resolved here rather than silently.**

- **§1f names `runDeferredInsert`, but production back-pressure surfaces on the staged path.** `pkg/plugins/storageintegrity/plugin.go:280` sets `SuppressUpstreamExecution`, so the strict-hook rejection is `relay.go:1119`, not `relay.go:1313`. Tasks 21 and 22 fix both; Task 21 is the one that changes production behaviour.
- **D3(a) says "an aggregate `count()` grouped query" for the hot path.** The reservation protocol captures each reservation's exact baseline part names at admission (`parts_pressure.go:580-582`) and rebases queued reservations by name after cleanup (`:1264-1272`), so an aggregate hot-path read would break `TestPartsPressureGuard_InFlightCleanupRebasesQueuedReplacement` and friends. The hot path is therefore an **exact read scoped to the statement's `(table, partition)` pairs** — bounded by the touched partitions (ClickHouse caps a partition at `parts_to_throw_insert`) instead of by the whole deployment — and the aggregate `count()` form is what the interval poller uses for gauges and untouched keys. That satisfies D3's stated goal ("pressure reads are bounded and operator-tunable, and cannot degrade into permanent refusal") and its D3(b) requirement that the exact-name inventory survive where the protocol needs it. Record this as an evidence-backed deviation in the plan's progress notes when Phase D merges.

## Spec Coverage Map

| Spec L | Requirement | Tasks |
|---|---|---|
| D1 | Export `payloadexec`'s type whitelist and call it from `Intents`; `BuildDDL`, `EnsureProtocolTables` and both roles' `validate()` covered by one gate; reject before any `CREATE`, naming table/column/type | 1, 3, 4, 5, 6 |
| D2 | `SchemaSource` in both role configs; mode derived in `validate()`; no reachable fail-open zero; `-ensure-tables` becomes a check that names both on disagreement; collision validation stays ahead of every short-circuit | 7, 12 |
| D3(a) | Hot path reads only the statement's `(table, partition)` pairs, bound as parameters | 15, 19 |
| D3(b) | Exact-name inventory kept where the reservation/cleanup protocol needs it, on the poller, scoped per table | 13, 14 |
| D3 | `refresh_timeout` / `snapshot_ttl` become config keys with today's values as defaults, validated positive | 18 |
| D3 | Drop the per-admission `Invalidate()`-triggered second scan; invalidation marks stale for the poller | 17 |
| D3 | `admissionMu` scoped per table | 16 |
| D3 | Fail-closed posture on genuinely unavailable inventory unchanged | 13 (per-key `checkAvailableLocked`), 16 (per-table fence), 18 (TTL) |
| D4 | `errors.Is(ErrProtocolTableDrift)` (and D1's type errors) kill the role; anything else retried with bounded backoff, escalating after a configurable count | 9 |
| D4 | `Run` ensures before its loop; subscription-cancellation path returns the original error | 9 |
| D5 | `hg_promote` rendered by `ddl`, created under create mode, verified by `VerifyProtocolTable` | 4, 8 |
| D5 | `promote_replace.go` stops creating it and fails clearly when absent | 8, 10 |
| D6 | 252 returns on a session that stays usable; strict-hook contract gains a rejection class that writes the Exception, aborts the query and resumes | 20, 21, 22, 23 |
| D7 | One validator pins `hg_safe`/`hg_unsafe`/`hg_promote` whenever any SI feature is enabled, in housegate and sentio-node | 24, 26 |
| D7 | SNode mirror limit fed only when back-pressure is enabled | 26 |
| D7 | A stuck cleanup proof fences only the affected table | 13, 16 |
| D7 | Verifier enforces the partition freeze at config time exactly as the SNode does | 5 |
| §4 | Type outside the whitelist rejected by `BuildDDL`, `EnsureProtocolTables` and both `validate()`s; docker test proves no table is created | 4, 5, 6 |
| §4 | Config without a schema source fails role construction; `-ensure-tables=create` with `table_ids` fails naming both | 7, 12 |
| §4 | Docker test with several thousand parts shows a bounded hot-path row count inside the timeout; unit test proves the touched-pairs query is parameterized by exactly the statement's partitions; `refresh_timeout`/`snapshot_ttl` honoured from config | 15, 18, 19 |
| §4 | Transient reconcile error retried and the role survives; real drift kills it; both asserted distinctly | 9 |
| §4 | `hg_promote` drift detected at startup; `promote_replace` errors clearly when absent | 8, 10 |
| §4 | Back-pressure refusal: Exception 252 **and** the connection remains usable | 21, 22, 23, 27 |
| §4 | sentio-node SI smoke covers the type-validation reject and the survivable throttle | 27 |
| §5.1 | arbiter-core delivery → tag | 3–11 |
| §5.2 | arbiter delivery → tag | 12 |
| §5.3 | housegate delivery → tag | 1, 2, 13–25 |
| §5.4 | sentio-node pins, D7 alignment, mirror gating, smoke | 26, 27 |
| Roadmap §5 | Bump the housegate pin in arbiter/arbiter-core/sentio-node as each spec's dependency task; cut sentio-node's first tag | 3, 12, 26, 27 |
