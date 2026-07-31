# Arbiter Chain Schema Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `cmd/arbiter-verifier` and `cmd/arbiter-snode` gain `schema_source: chain` — table set enumerated from the `Databases` contract, content verified through housegate's `NetworkStateLoader` — and the inline `tables` config mode is deleted.

**Architecture:** A new internal `chainschema` package in the arbiter repo implements housegate's `registry.TableSchemas` over four `eth_call` view functions (abigen caller bindings, sha256-pinned like the anchor bindings), enumerates the declared table set from the contract, and feeds the existing `NetworkStateLoader` hash-verification ladder. Both role cmds call one shared `chainschema.LoadTables` entrypoint in their chain branch; the clickhouse branch is untouched. Spec: housegate repo `docs/superpowers/specs/2026-07-30-arbiter-chain-schema-source-design.md` (read it first).

**Tech Stack:** Go, go-ethereum v1.17.2 (`ethclient`, `bind`, `tool abigen` — all already in arbiter's go.mod), housegate `pkg/schemaregistry` + `pkg/registry` + `pkg/replay/payloadexec`, Bazel + gazelle, `gopkg.in/yaml.v3`.

## Global Constraints

- Repos: arbiter at `~/src/sentio_xyz/arbiter` (branch `feat/chain-schema-source` off latest `origin/main`, ≥ d84735d), arbiter-core at `~/src/sentio_xyz/arbiter-core` (branch `chore/chtablename-dedup` off latest `origin/main`, ≥ b277f57). Do NOT touch housegate, sentio-node, sentio-core, or compute-network-contracts.
- Bazel is the arbiter build/test ground truth: `make build` / `make test` delegate to `bazel build //...` / `bazel test //...`. Use `go test ./<pkg>/...` for fast iteration, but every task's final green must also hold under Bazel at the end (Task 11). After changing go.mod or adding packages: `bazel mod tidy && bazel run //:gazelle`.
- Never push to any repo's `main`. PRs via `gh`; if the current gh account lacks permission on sentioxyz repos, switch to the `axiom147` account (`gh auth switch -u axiom147`) and switch back afterwards.
- Never read or print private-key environment variables. The devnet smoke uses only RPC-endpoint/contract-address/network-id env vars.
- English-only code comments and error strings; wrap errors with `fmt.Errorf("context: %w", err)`; aggregate config errors via the existing `errs []error` + `errors.Join` pattern.
- Canonical hash domains: never reimplement `payloadexec.TableSchemaHash` / `payloadexec.SchemaRoot` / the `"0x"+hex` hash encoding — call them. `replay.DigestString` returns `"0x"+hex(sha256)`, so `TableSchemaHash` output string-compares against `"0x" + hex.EncodeToString(schemaHash[:])` of the contract's `bytes32`.
- Generated bindings type names (`TypesDatabase`, `TypesTable`, `TypesTableSchema`, `IDatabasesViewsCaller`) are abigen's output; if the actual generated names differ (e.g. from `internalType` quirks), adjust the hand-written interface/code to match the generated file — never hand-edit the generated file.

---

### Task 1: arbiter-core — replace the private `chTableName` copy

**Files:**
- Modify: `~/src/sentio_xyz/arbiter-core/verifier/backends.go` (private `chTableName` at bottom, call site `qualified := s.cfg.UnsafeDatabase + "." + chTableName(tableID)`)

**Interfaces:**
- Consumes: `snode.CHTableName(tableID string) string` (exported in arbiter-core `snode/parts.go`, replaces `.` with `__`). `snode` does not import `verifier`, so no import cycle.
- Produces: nothing new — behavior-preserving dedup.

- [ ] **Step 1: Baseline** — `cd ~/src/sentio_xyz/arbiter-core && git checkout -b chore/chtablename-dedup origin/main && go test ./verifier/...` Expected: PASS (record any pre-existing failures; there should be none).
- [ ] **Step 2: Edit** — in `verifier/backends.go`: add `"github.com/sentioxyz/arbiter-core/snode"` to imports; change the call site to `qualified := s.cfg.UnsafeDatabase + "." + snode.CHTableName(tableID)`; delete the private `func chTableName(tableID string) string { return strings.ReplaceAll(tableID, ".", "__") }` and drop the `strings` import if now unused.
- [ ] **Step 3: Verify** — `go test ./verifier/... && go build ./...` Expected: PASS (existing backend tests cover the mapping).
- [ ] **Step 4: Commit + PR** —
```bash
git add verifier/backends.go && git commit -m "refactor(verifier): use snode.CHTableName instead of private copy"
git push -u origin chore/chtablename-dedup && gh pr create --title "refactor(verifier): use snode.CHTableName instead of private copy" --body "The export in #6 existed precisely to kill such copies. Behavior-preserving."
```

---

### Task 2: arbiter — bump housegate to a NetworkStateLoader-bearing version

**Files:**
- Modify: `~/src/sentio_xyz/arbiter/go.mod`, `go.sum`
- Modify: `~/src/sentio_xyz/arbiter/BUILD.bazel` (root — `# gazelle:resolve` directives)

**Interfaces:**
- Produces: `schemaregistry.NewNetworkStateLoader(schemas registry.TableSchemas, networkID string) *NetworkStateLoader`, `(*NetworkStateLoader).WithClickHouseCrossCheck(conn clickhouse.Conn)`, `(*NetworkStateLoader).Load(ctx, refs []TableRef) ([]payloadexec.TableSchema, error)`, sentinels `schemaregistry.ErrSchemaContentMissing` / `ErrSchemaHashMismatch` / `ErrClickHouseDrift`, and `registry.TableSchema{DatabaseId, TableId string; Version uint32; SchemaHash, SchemaJson string}` + `registry.TableSchemas` interface — all used by Tasks 4–5.

- [ ] **Step 1: Branch** — `cd ~/src/sentio_xyz/arbiter && git checkout -b feat/chain-schema-source origin/main`
- [ ] **Step 2: Bump** — `go get github.com/housegate/housegate@main && go mod tidy`. The current pin is `v0.7.2-0.20260729115133-bc3782c68838` (Phase A only); the new pseudo-version must resolve to housegate main commit ≥ `88a120b` (PR #113). Verify: `go list -m github.com/housegate/housegate` and `go doc github.com/housegate/housegate/pkg/schemaregistry.NewNetworkStateLoader` prints the symbol.
- [ ] **Step 3: Bazel sync** — root `BUILD.bazel` carries `# gazelle:resolve go github.com/housegate/housegate/pkg/... @housegate//pkg/...` directives; add one for `pkg/registry` in the same pattern if missing (`pkg/schemaregistry` is already consumed by the cmds — check whether it has a directive and mirror whatever the repo does for it). Then `bazel mod tidy && bazel run //:gazelle`.
- [ ] **Step 4: Verify + commit** — `bazel build //... && go build ./...` Expected: PASS.
```bash
git add go.mod go.sum BUILD.bazel && git commit -m "chore(deps): housegate @ main for NetworkStateLoader"
```
(If gazelle rewrote other BUILD files, include them.)

---

### Task 3: arbiter — vendored IDatabases views ABI + pinned caller bindings

**Files:**
- Create: `~/src/sentio_xyz/arbiter/contracts/IDatabasesViews.abi.json` (vendored 4-function ABI)
- Create: `~/src/sentio_xyz/arbiter/scripts/databases-bindings.sh` (gen/check, modeled on `scripts/anchor-bindings.sh`)
- Create (generated): `~/src/sentio_xyz/arbiter/chainschema/bindings/idatabases_views.go` + `IDatabasesViews.abi.json.sha256` + `idatabases_views.go.sha256`
- Modify: `~/src/sentio_xyz/arbiter/Makefile` (two targets), the CI workflow step that runs `scripts/anchor-bindings.sh check` (add the sibling check)

**Interfaces:**
- Produces: package `bindings` (`github.com/sentioxyz/arbiter/chainschema/bindings`) with `NewIDatabasesViewsCaller(address common.Address, caller bind.ContractCaller) (*IDatabasesViewsCaller, error)` and methods `GetDatabases(opts *bind.CallOpts) ([]TypesDatabase, error)`, `GetDatabaseTables(opts *bind.CallOpts, databaseId string) ([]TypesTable, error)`, `LatestTableSchemaVersion(opts *bind.CallOpts, databaseId, tableId string) (uint32, error)`, `GetTableSchema(opts *bind.CallOpts, databaseId, tableId string, version uint32) (TypesTableSchema, error)`; structs `TypesDatabase{Id string; Active bool; ...; PendingDelete bool}`, `TypesTable{Id string; Active bool; DatabaseId string; TableType string}`, `TypesTableSchema{SchemaHash [32]byte; SchemaJson string}`.

- [ ] **Step 1: Extract the ABI from sentio-node's checked-in bindings** (sentio-node vendors the full IDatabases ABI from the contracts repo; filtering it mechanically avoids any hand transcription):
```bash
cd ~/src/sentio_xyz/arbiter && mkdir -p contracts chainschema/bindings
python3 - > contracts/IDatabasesViews.abi.json <<'EOF'
import json, re, pathlib
src = pathlib.Path("/Users/uranuswch/src/sentio_xyz/sentio-node/bindings/bindings.go").read_text()
m = re.search(r'IDatabasesMetaData\s*=\s*&bind\.MetaData\{\s*ABI:\s*"((?:[^"\\]|\\.)*)"', src)
assert m, "IDatabasesMetaData ABI not found"
abi = json.loads(json.loads('"%s"' % m.group(1)))  # first loads undoes Go string escapes
keep = {"getDatabases", "getDatabaseTables", "latestTableSchemaVersion", "getTableSchema"}
out = [e for e in abi if e.get("type") == "function" and e.get("name") in keep]
assert {e["name"] for e in out} == keep, sorted(e["name"] for e in out)
print(json.dumps(out, indent=1))
EOF
```
(If the variable is named differently in sentio-node's bindings.go, grep for `getTableSchema` there and adapt the regex — the assert catches a bad match.)
- [ ] **Step 2: Write `scripts/databases-bindings.sh`** — copy the structure of `scripts/anchor-bindings.sh` (portable shell, `gen`/`check` entry points, `sha256()` helper, header stamp) minus the solc step, since the input is already an ABI:
```bash
#!/usr/bin/env bash
#
# Generates and verifies the checked-in IDatabases view-caller bindings.
# The ABI is vendored from the compute-network-contracts IDatabases interface
# (via sentio-node's checked-in copy); only the four schema-registry view
# functions are kept. Same gen/check contract as anchor-bindings.sh.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${repo_root}"
readonly SOURCE="contracts/IDatabasesViews.abi.json"
readonly BINDING="chainschema/bindings/idatabases_views.go"
readonly SOURCE_SUM="chainschema/bindings/IDatabasesViews.abi.json.sha256"
readonly BINDING_SUM="chainschema/bindings/idatabases_views.go.sha256"
readonly HEADER_PREFIX="// IDatabasesViews.abi.json sha256: "
sha256() {
	if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | awk '{print $1}'
	else shasum -a 256 "$1" | awk '{print $1}'; fi
}
header_hash() { sed -n "1s#^${HEADER_PREFIX}##p" "${BINDING}"; }
gen() {
	local abigen_args=(
		--abi "${repo_root}/${SOURCE}"
		--pkg bindings
		--type IDatabasesViews
		--out "${repo_root}/${BINDING}"
	)
	if command -v bazel >/dev/null 2>&1; then
		bazel run @com_github_ethereum_go_ethereum//cmd/abigen -- "${abigen_args[@]}"
	else
		go tool abigen "${abigen_args[@]}"
	fi
	local stamped="${BINDING}.tmp"
	printf '%s%s\n' "${HEADER_PREFIX}" "$(sha256 "${SOURCE}")" >"${stamped}"
	cat "${BINDING}" >>"${stamped}"
	mv "${stamped}" "${BINDING}"
	sha256 "${SOURCE}" >"${SOURCE_SUM}"
	sha256 "${BINDING}" >"${BINDING_SUM}"
}
check() {
	local source_hash binding_hash failed=0
	source_hash="$(sha256 "${SOURCE}")"
	binding_hash="$(sha256 "${BINDING}")"
	if [ "${source_hash}" != "$(cat "${SOURCE_SUM}")" ]; then
		echo "${SOURCE} does not match ${SOURCE_SUM}" >&2; failed=1; fi
	if [ "${source_hash}" != "$(header_hash)" ]; then
		echo "${SOURCE} does not match the hash stamped into ${BINDING}" >&2; failed=1; fi
	if [ "${binding_hash}" != "$(cat "${BINDING_SUM}")" ]; then
		echo "${BINDING} does not match ${BINDING_SUM}" >&2; failed=1; fi
	if [ "${failed}" -ne 0 ]; then
		echo "databases bindings drift: run 'make gen-databases-bindings' and commit" >&2
		exit 1
	fi
}
case "${1-}" in
gen) gen ;;
check) check ;;
*) echo "usage: $0 {gen|check}" >&2; exit 2 ;;
esac
```
`chmod +x scripts/databases-bindings.sh`
- [ ] **Step 3: Generate and inspect** — `./scripts/databases-bindings.sh gen`, then `grep -n "func.*IDatabasesViewsCaller\|type Types" chainschema/bindings/idatabases_views.go`. Expected: the four caller methods and the three `Types*` structs from the Interfaces block. If abigen emitted different struct names, note the actual names — Tasks 4–5 must use them.
- [ ] **Step 4: Wire Makefile + CI** — append to `Makefile` (same shape as the anchor pair):
```makefile
gen-databases-bindings:
	./scripts/databases-bindings.sh gen

check-databases-bindings:
	./scripts/databases-bindings.sh check
```
Find the CI step: `grep -rn "anchor-bindings.sh check" .github/workflows/` and add `./scripts/databases-bindings.sh check` immediately after it in the same job.
- [ ] **Step 5: Verify + commit** — `./scripts/databases-bindings.sh check && bazel run //:gazelle && bazel build //chainschema/...` Expected: check passes, gazelle writes `chainschema/bindings/BUILD.bazel`, build green.
```bash
git add contracts/ scripts/databases-bindings.sh chainschema/ Makefile .github/
git commit -m "feat(chainschema): pinned IDatabases view-caller bindings"
```

---

### Task 4: arbiter — `ContractTableSchemas` (registry.TableSchemas over eth_call)

**Files:**
- Create: `~/src/sentio_xyz/arbiter/chainschema/schemas.go`
- Test: `~/src/sentio_xyz/arbiter/chainschema/schemas_test.go` (also creates the shared `fakeCaller` used by Task 5's tests)

**Interfaces:**
- Consumes: `bindings` package from Task 3; `registry.TableSchema` / `registry.TableSchemas` from housegate.
- Produces: `type DatabasesCaller interface` (the four methods, exact signatures below); `NewContractTableSchemas(ctx context.Context, caller DatabasesCaller) *ContractTableSchemas`; methods `LatestTableSchema(databaseId, tableId string) (registry.TableSchema, bool)`, `TableSchema(databaseId, tableId string, version uint32) (registry.TableSchema, bool)`, `LastError() error`. Task 5 consumes all of these plus `fakeCaller`.

- [ ] **Step 1: Write the failing test** — `chainschema/schemas_test.go`:
```go
package chainschema

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter/chainschema/bindings"
)

// fakeCaller implements DatabasesCaller in memory. Shared with loadtables_test.go.
type fakeCaller struct {
	databases []bindings.TypesDatabase
	tables    map[string][]bindings.TypesTable     // databaseId -> tables
	versions  map[string]uint32                    // vkey -> latest version
	schemas   map[string]bindings.TypesTableSchema // skey -> record
	err       error                                // non-nil: every call fails with it
	schemaErr error                                // non-nil: only GetTableSchema fails — models
	//                                                "enumeration succeeded, content fetch hit RPC trouble"
}

func vkey(db, tbl string) string { return db + "\x00" + tbl }
func skey(db, tbl string, v uint32) string {
	return fmt.Sprintf("%s\x00%s\x00%d", db, tbl, v)
}

func (f *fakeCaller) GetDatabases(_ *bind.CallOpts) ([]bindings.TypesDatabase, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.databases, nil
}
func (f *fakeCaller) GetDatabaseTables(_ *bind.CallOpts, databaseId string) ([]bindings.TypesTable, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.tables[databaseId], nil
}
func (f *fakeCaller) LatestTableSchemaVersion(_ *bind.CallOpts, databaseId, tableId string) (uint32, error) {
	if f.err != nil {
		return 0, f.err
	}
	return f.versions[vkey(databaseId, tableId)], nil
}
func (f *fakeCaller) GetTableSchema(_ *bind.CallOpts, databaseId, tableId string, version uint32) (bindings.TypesTableSchema, error) {
	if f.err != nil {
		return bindings.TypesTableSchema{}, f.err
	}
	if f.schemaErr != nil {
		return bindings.TypesTableSchema{}, f.schemaErr
	}
	return f.schemas[skey(databaseId, tableId, version)], nil
}

// declaredFixture returns a fake with one declared table db.t whose on-chain
// hash is the real payloadexec.TableSchemaHash of its schema_json — the honest
// declarer behavior.
func declaredFixture(t *testing.T, networkID string) (*fakeCaller, payloadexec.TableSchema) {
	t.Helper()
	schema := payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	hashHex := payloadexec.TableSchemaHash(networkID, schema) // "0x" + 64 hex chars
	raw, err := hex.DecodeString(strings.TrimPrefix(hashHex, "0x"))
	if err != nil || len(raw) != 32 {
		t.Fatalf("unexpected TableSchemaHash form %q: %v", hashHex, err)
	}
	var h32 [32]byte
	copy(h32[:], raw)
	return &fakeCaller{
		databases: []bindings.TypesDatabase{{Id: "db", Active: true}},
		tables:    map[string][]bindings.TypesTable{"db": {{Id: "t", Active: true, DatabaseId: "db"}}},
		versions:  map[string]uint32{vkey("db", "t"): 1},
		schemas: map[string]bindings.TypesTableSchema{
			skey("db", "t", 1): {SchemaHash: h32, SchemaJson: string(schemaJSON)},
		},
	}, schema
}

func TestContractTableSchemas_LatestTableSchema(t *testing.T) {
	fake, _ := declaredFixture(t, "net-test")
	cts := NewContractTableSchemas(context.Background(), fake)
	got, ok := cts.LatestTableSchema("db", "t")
	if !ok {
		t.Fatal("want ok")
	}
	if got.Version != 1 || got.DatabaseId != "db" || got.TableId != "t" {
		t.Fatalf("bad identity: %+v", got)
	}
	if !strings.HasPrefix(got.SchemaHash, "0x") || len(got.SchemaHash) != 66 {
		t.Fatalf("bad hash encoding: %q", got.SchemaHash)
	}
	if got.SchemaJson == "" {
		t.Fatal("want schema json")
	}
	if err := cts.LastError(); err != nil {
		t.Fatalf("unexpected LastError: %v", err)
	}
}

func TestContractTableSchemas_UndeclaredIsMissNotError(t *testing.T) {
	fake, _ := declaredFixture(t, "net-test")
	cts := NewContractTableSchemas(context.Background(), fake)
	if _, ok := cts.LatestTableSchema("db", "nope"); ok {
		t.Fatal("want miss for undeclared table")
	}
	if err := cts.LastError(); err != nil {
		t.Fatalf("undeclared must not record an RPC error, got %v", err)
	}
}

func TestContractTableSchemas_RPCErrorRecordsLastError(t *testing.T) {
	rpcErr := errors.New("connection refused")
	cts := NewContractTableSchemas(context.Background(), &fakeCaller{err: rpcErr})
	if _, ok := cts.LatestTableSchema("db", "t"); ok {
		t.Fatal("want miss on RPC error")
	}
	if !errors.Is(cts.LastError(), rpcErr) {
		t.Fatalf("want recorded rpc error, got %v", cts.LastError())
	}
}
```
- [ ] **Step 2: Run to verify failure** — `go test ./chainschema/ -run TestContractTableSchemas -v` Expected: FAIL (undefined: `NewContractTableSchemas`, `DatabasesCaller`).
- [ ] **Step 3: Implement** — `chainschema/schemas.go`:
```go
// Package chainschema loads the declared storage-integrity table set and
// schema content from the Databases contract's view functions, feeding
// housegate's NetworkStateLoader verification ladder. It is the arbiter-side
// "chain" schema source (spec: 2026-07-30-arbiter-chain-schema-source-design).
package chainschema

import (
	"context"
	"encoding/hex"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"

	"github.com/housegate/housegate/pkg/registry"

	"github.com/sentioxyz/arbiter/chainschema/bindings"
)

// DatabasesCaller is the narrow view-only surface of the Databases contract
// this package needs; the production implementation is the abigen-generated
// IDatabasesViewsCaller, tests supply a fake.
type DatabasesCaller interface {
	GetDatabases(opts *bind.CallOpts) ([]bindings.TypesDatabase, error)
	GetDatabaseTables(opts *bind.CallOpts, databaseId string) ([]bindings.TypesTable, error)
	LatestTableSchemaVersion(opts *bind.CallOpts, databaseId, tableId string) (uint32, error)
	GetTableSchema(opts *bind.CallOpts, databaseId, tableId string, version uint32) (bindings.TypesTableSchema, error)
}

// ContractTableSchemas implements housegate registry.TableSchemas over
// eth_call. The interface returns (value, ok) without an error channel, so
// RPC failures are recorded in lastErr and surfaced as misses; LoadTables
// checks LastError after the load to keep "chain unreachable" distinct from
// "table not declared".
type ContractTableSchemas struct {
	ctx    context.Context
	caller DatabasesCaller

	mu      sync.Mutex
	lastErr error
}

// NewContractTableSchemas wraps caller for lookups executed under ctx (the
// registry.TableSchemas methods carry no context of their own).
func NewContractTableSchemas(ctx context.Context, caller DatabasesCaller) *ContractTableSchemas {
	return &ContractTableSchemas{ctx: ctx, caller: caller}
}

func (c *ContractTableSchemas) setErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.lastErr == nil {
		c.lastErr = err
	}
}

// LastError reports the first RPC failure observed by any lookup, nil if all
// lookups were clean.
func (c *ContractTableSchemas) LastError() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr
}

// LatestTableSchema resolves the contract's version cursor and fetches that
// version. version == 0 means never declared — a genuine miss.
func (c *ContractTableSchemas) LatestTableSchema(databaseId, tableId string) (registry.TableSchema, bool) {
	version, err := c.caller.LatestTableSchemaVersion(&bind.CallOpts{Context: c.ctx}, databaseId, tableId)
	if err != nil {
		c.setErr(err)
		return registry.TableSchema{}, false
	}
	if version == 0 {
		return registry.TableSchema{}, false
	}
	return c.TableSchema(databaseId, tableId, version)
}

// TableSchema fetches one declared version. The hash is re-encoded to the
// canonical "0x"+hex string form NetworkStateLoader compares against
// payloadexec.TableSchemaHash output.
func (c *ContractTableSchemas) TableSchema(databaseId, tableId string, version uint32) (registry.TableSchema, bool) {
	s, err := c.caller.GetTableSchema(&bind.CallOpts{Context: c.ctx}, databaseId, tableId, version)
	if err != nil {
		c.setErr(err)
		return registry.TableSchema{}, false
	}
	return registry.TableSchema{
		DatabaseId: databaseId,
		TableId:    tableId,
		Version:    version,
		SchemaHash: "0x" + hex.EncodeToString(s.SchemaHash[:]),
		SchemaJson: s.SchemaJson,
	}, true
}
```
- [ ] **Step 4: Run to verify pass** — `go test ./chainschema/ -run TestContractTableSchemas -v` Expected: PASS.
- [ ] **Step 5: Commit** —
```bash
bazel run //:gazelle && git add chainschema/ && git commit -m "feat(chainschema): ContractTableSchemas over Databases view calls"
```

---

### Task 5: arbiter — enumeration, `LoadTables` entrypoint, `DialCaller`

**Files:**
- Create: `~/src/sentio_xyz/arbiter/chainschema/loadtables.go`
- Test: `~/src/sentio_xyz/arbiter/chainschema/loadtables_test.go` (reuses `fakeCaller` from Task 4's test file — same package)

**Interfaces:**
- Consumes: Task 4's `DatabasesCaller`, `NewContractTableSchemas`; housegate `schemaregistry.NewNetworkStateLoader` / `TableRef` / sentinels; `snode.CHTableName` from arbiter-core; `payloadexec.SchemaRoot` (tests only).
- Produces: `LoadTables(ctx context.Context, opts LoadOptions) ([]payloadexec.TableSchema, error)`; `type LoadOptions struct { Caller DatabasesCaller; NetworkID string; UnsafeDatabase string; CrossCheck clickhouse.Conn; Timeout time.Duration }`; `DialCaller(rpcURL string, contractAddr common.Address) (DatabasesCaller, func(), error)`. Tasks 7–8 consume all three.

- [ ] **Step 1: Write the failing tests** — `chainschema/loadtables_test.go`:
```go
package chainschema

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/schemaregistry"

	"github.com/sentioxyz/arbiter/chainschema/bindings"
)

func TestLoadTables_HonestDeclarationLoads(t *testing.T) {
	fake, want := declaredFixture(t, "net-test")
	tables, err := LoadTables(context.Background(), LoadOptions{
		Caller: fake, NetworkID: "net-test", UnsafeDatabase: "hg_unsafe",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 || tables[0].TableID != want.TableID || tables[0].PartitionBy != want.PartitionBy {
		t.Fatalf("bad load: %+v", tables)
	}
}

func TestLoadTables_EnumerationFiltersAndSorts(t *testing.T) {
	fake, _ := declaredFixture(t, "net-test")
	// A second declared table that sorts before db.t, plus every excluded kind.
	addDeclared(t, fake, "net-test", "adb", "z")
	fake.databases = append(fake.databases,
		bindings.TypesDatabase{Id: "inactive", Active: false},
		bindings.TypesDatabase{Id: "deleting", Active: true, PendingDelete: true},
	)
	fake.tables["db"] = append(fake.tables["db"],
		bindings.TypesTable{Id: "dead", Active: false, DatabaseId: "db"},
		bindings.TypesTable{Id: "undeclared", Active: true, DatabaseId: "db"},
	)
	refs, err := enumerateDeclaredTables(context.Background(), fake, "hg_unsafe")
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 || refs[0].TableID != "adb.z" || refs[1].TableID != "db.t" {
		t.Fatalf("bad enumeration: %+v", refs)
	}
	if refs[1].Database != "hg_unsafe" || refs[1].Table != "db__t" {
		t.Fatalf("bad ref mapping: %+v", refs[1])
	}
}

func TestLoadTables_EmptyEnumerationIsDedicatedError(t *testing.T) {
	fake := &fakeCaller{databases: []bindings.TypesDatabase{{Id: "db", Active: true}}}
	_, err := LoadTables(context.Background(), LoadOptions{
		Caller: fake, NetworkID: "net-test", UnsafeDatabase: "hg_unsafe",
	})
	if err == nil || !strings.Contains(err.Error(), "no declared storage-integrity tables") {
		t.Fatalf("want dedicated empty-set error, got %v", err)
	}
}

func TestLoadTables_RPCErrorIsUnavailableNotMissing(t *testing.T) {
	rpcErr := errors.New("connection refused")
	_, err := LoadTables(context.Background(), LoadOptions{
		Caller: &fakeCaller{err: rpcErr}, NetworkID: "net-test", UnsafeDatabase: "hg_unsafe",
	})
	if err == nil || !errors.Is(err, rpcErr) {
		t.Fatalf("want wrapped rpc error, got %v", err)
	}
	if errors.Is(err, schemaregistry.ErrSchemaContentMissing) {
		t.Fatalf("rpc failure must not classify as missing declaration: %v", err)
	}
}

func TestLoadTables_SchemaFetchRPCErrorIsUnavailable(t *testing.T) {
	// Enumeration succeeds (GetDatabases/GetDatabaseTables/LatestTableSchemaVersion
	// are healthy), but the loader's content fetch hits RPC trouble. This is the
	// decision-7 path: the ErrSchemaContentMissing the loader reports must be
	// reclassified as "unavailable", never as a missing declaration.
	fake, _ := declaredFixture(t, "net-test")
	fake.schemaErr = errors.New("rpc timeout")
	_, err := LoadTables(context.Background(), LoadOptions{
		Caller: fake, NetworkID: "net-test", UnsafeDatabase: "hg_unsafe",
	})
	if err == nil || !strings.Contains(err.Error(), "chain schema source unavailable") {
		t.Fatalf("want unavailable classification, got %v", err)
	}
	if !errors.Is(err, fake.schemaErr) {
		t.Fatalf("want wrapped rpc error, got %v", err)
	}
}

func TestLoadTables_DeclaredHashMismatchRejected(t *testing.T) {
	fake, _ := declaredFixture(t, "net-test")
	rec := fake.schemas[skey("db", "t", 1)]
	rec.SchemaHash[0] ^= 0xff // corrupt the declared commitment
	fake.schemas[skey("db", "t", 1)] = rec
	_, err := LoadTables(context.Background(), LoadOptions{
		Caller: fake, NetworkID: "net-test", UnsafeDatabase: "hg_unsafe",
	})
	if !errors.Is(err, schemaregistry.ErrSchemaHashMismatch) {
		t.Fatalf("want ErrSchemaHashMismatch, got %v", err)
	}
}
```
Also append the `addDeclared` helper to `schemas_test.go` (it mirrors `declaredFixture` for a second table):
```go
func addDeclared(t *testing.T, f *fakeCaller, networkID, db, tbl string) {
	t.Helper()
	schema := payloadexec.TableSchema{
		TableID:     db + "." + tbl,
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}},
	}
	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	hashHex := payloadexec.TableSchemaHash(networkID, schema)
	raw, _ := hex.DecodeString(strings.TrimPrefix(hashHex, "0x"))
	var h32 [32]byte
	copy(h32[:], raw)
	found := false
	for _, d := range f.databases {
		if d.Id == db {
			found = true
		}
	}
	if !found {
		f.databases = append(f.databases, bindings.TypesDatabase{Id: db, Active: true})
	}
	if f.tables == nil {
		f.tables = map[string][]bindings.TypesTable{}
	}
	f.tables[db] = append(f.tables[db], bindings.TypesTable{Id: tbl, Active: true, DatabaseId: db})
	f.versions[vkey(db, tbl)] = 1
	f.schemas[skey(db, tbl, 1)] = bindings.TypesTableSchema{SchemaHash: h32, SchemaJson: string(schemaJSON)}
}
```
- [ ] **Step 2: Run to verify failure** — `go test ./chainschema/ -run TestLoadTables -v` Expected: FAIL (undefined: `LoadTables`, `LoadOptions`, `enumerateDeclaredTables`).
- [ ] **Step 3: Implement** — `chainschema/loadtables.go`:
```go
package chainschema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"

	"github.com/sentioxyz/arbiter-core/snode"
	"github.com/sentioxyz/arbiter/chainschema/bindings"
)

// DefaultTimeout bounds the whole enumerate+load sequence when LoadOptions
// leaves Timeout zero.
const DefaultTimeout = 60 * time.Second

// LoadOptions parameterizes LoadTables.
type LoadOptions struct {
	Caller         DatabasesCaller
	NetworkID      string
	UnsafeDatabase string
	CrossCheck     clickhouse.Conn // nil = no ClickHouse cross-check (verifier); snode passes its conn
	Timeout        time.Duration   // zero = DefaultTimeout
}

// LoadTables enumerates the declared storage-integrity table set from the
// Databases contract and loads + verifies every schema through housegate's
// NetworkStateLoader. It is the single chain-branch entrypoint for both role
// cmds.
func LoadTables(ctx context.Context, opts LoadOptions) ([]payloadexec.TableSchema, error) {
	timeout := opts.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	refs, err := enumerateDeclaredTables(ctx, opts.Caller, opts.UnsafeDatabase)
	if err != nil {
		return nil, fmt.Errorf("enumerate declared tables: %w", err)
	}
	if len(refs) == 0 {
		return nil, errors.New("no declared storage-integrity tables found on chain")
	}

	cts := NewContractTableSchemas(ctx, opts.Caller)
	loader := schemaregistry.NewNetworkStateLoader(cts, opts.NetworkID)
	if opts.CrossCheck != nil {
		loader = loader.WithClickHouseCrossCheck(opts.CrossCheck)
	}
	tables, err := loader.Load(ctx, refs)
	if err != nil {
		if rpcErr := cts.LastError(); rpcErr != nil && errors.Is(err, schemaregistry.ErrSchemaContentMissing) {
			return nil, fmt.Errorf("chain schema source unavailable: %w", rpcErr)
		}
		return nil, fmt.Errorf("load schemas from chain: %w", err)
	}
	return tables, nil
}

// enumerateDeclaredTables walks active databases and tables and keeps every
// table with a non-zero schema-version cursor — declaration is the SI-set
// membership marker (a table is declared only after the source-side operator
// admits it). Sorted by TableID for deterministic logs and root printing
// (payloadexec.SchemaRoot canonicalizes order itself).
func enumerateDeclaredTables(ctx context.Context, caller DatabasesCaller, unsafeDatabase string) ([]schemaregistry.TableRef, error) {
	opts := &bind.CallOpts{Context: ctx}
	dbs, err := caller.GetDatabases(opts)
	if err != nil {
		return nil, fmt.Errorf("getDatabases: %w", err)
	}
	var refs []schemaregistry.TableRef
	for _, db := range dbs {
		if !db.Active || db.PendingDelete {
			continue
		}
		tables, err := caller.GetDatabaseTables(opts, db.Id)
		if err != nil {
			return nil, fmt.Errorf("getDatabaseTables(%s): %w", db.Id, err)
		}
		for _, t := range tables {
			if !t.Active {
				continue
			}
			version, err := caller.LatestTableSchemaVersion(opts, db.Id, t.Id)
			if err != nil {
				return nil, fmt.Errorf("latestTableSchemaVersion(%s.%s): %w", db.Id, t.Id, err)
			}
			if version == 0 {
				continue
			}
			logical := db.Id + "." + t.Id
			refs = append(refs, schemaregistry.TableRef{
				TableID:  logical,
				Database: unsafeDatabase,
				Table:    snode.CHTableName(logical),
			})
		}
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].TableID < refs[j].TableID })
	return refs, nil
}

// DialCaller connects an ethclient and binds the IDatabasesViews caller.
// The returned func closes the client.
func DialCaller(rpcURL string, contractAddr common.Address) (DatabasesCaller, func(), error) {
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		return nil, nil, fmt.Errorf("dial chain rpc: %w", err)
	}
	caller, err := bindings.NewIDatabasesViewsCaller(contractAddr, client)
	if err != nil {
		client.Close()
		return nil, nil, fmt.Errorf("bind databases contract: %w", err)
	}
	return caller, client.Close, nil
}
```
- [ ] **Step 4: Run to verify pass** — `go test ./chainschema/... -v` Expected: PASS (all Task 4 + Task 5 tests).
- [ ] **Step 5: Commit** —
```bash
bazel run //:gazelle && git add chainschema/ && git commit -m "feat(chainschema): declared-set enumeration and LoadTables entrypoint"
```

---

### Task 6: arbiter — verifier config convergence

**Files:**
- Modify: `~/src/sentio_xyz/arbiter/cmd/arbiter-verifier/config.go`
- Test: `~/src/sentio_xyz/arbiter/cmd/arbiter-verifier/main_test.go` (rewrite `TestConfigValidate_TableSourceModes`)

**Interfaces:**
- Produces: `Config.SchemaSource string`, `Config.Chain ChainConfig{RPCURL, DatabasesContractAddress string; Timeout arbconfig.Duration}`, `validateTableIDs(tableIDs []string, errs *[]error)`; `Config.UnsafeDatabase` defaulted to `"hg_unsafe"` in `loadConfig`. Removes: `Config.Tables`, `TableConfig`, `ColumnConfig`, `validateTables`, `validateTableSource`, `Config.tables()`, `Config.computedSchemaRoot()`, the validate-time `schema_root mismatch` block. Task 7 consumes `SchemaSource`/`Chain`; `toRoleConfig(tables)` keeps its signature.

- [ ] **Step 1: Rewrite the mode test** — replace `TestConfigValidate_TableSourceModes` in `main_test.go` (read the file first: if it already has an equivalent valid-config helper, reuse it instead of adding this one). New cases:
```go
func validVerifierConfig() Config {
	return Config{
		ReplicaID:         "v1",
		NetworkID:         "net",
		SchemaSnapshotID:  "snap",
		ExecutorProfileID: "prof",
		ClickHouseAddr:    "127.0.0.1:9000",
		PayloadDir:        "/tmp/p",
		UnsafeDatabase:    "hg_unsafe",
		Peers:             []PeerConfig{{ID: "arb-1", GRPCAddr: "127.0.0.1:7080"}},
		TableIDs:          []string{"db.t"},
	}
}

func TestConfigValidate_SchemaSourceModes(t *testing.T) {
	base := validVerifierConfig()

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr string // empty = valid
	}{
		{"clickhouse default ok", func(c *Config) { c.SchemaSource = "" }, ""},
		{"clickhouse explicit ok", func(c *Config) { c.SchemaSource = "clickhouse" }, ""},
		{"clickhouse requires table_ids", func(c *Config) { c.TableIDs = nil }, "table_ids is required"},
		{"table_ids entries validated", func(c *Config) { c.TableIDs = []string{"db.t", "db.t"} }, "duplicates"},
		{"chain block without chain mode rejected", func(c *Config) {
			c.Chain.RPCURL = "http://127.0.0.1:8545"
		}, "chain block requires schema_source: chain"},
		{"unknown source rejected", func(c *Config) { c.SchemaSource = "redis" }, "schema_source must be clickhouse or chain"},
		{"chain ok", func(c *Config) {
			c.SchemaSource = "chain"
			c.TableIDs = nil
			c.Chain = ChainConfig{RPCURL: "http://127.0.0.1:8545", DatabasesContractAddress: "0xabcdef0123456789abcdef0123456789abcdef01"}
		}, ""},
		{"chain rejects table_ids", func(c *Config) {
			c.SchemaSource = "chain"
			c.Chain = ChainConfig{RPCURL: "http://127.0.0.1:8545", DatabasesContractAddress: "0xabcdef0123456789abcdef0123456789abcdef01"}
		}, "table_ids must be empty when schema_source is chain"},
		{"chain requires rpc_url", func(c *Config) {
			c.SchemaSource = "chain"
			c.TableIDs = nil
			c.Chain = ChainConfig{DatabasesContractAddress: "0xabcdef0123456789abcdef0123456789abcdef01"}
		}, "chain.rpc_url is required"},
		{"chain requires contract address", func(c *Config) {
			c.SchemaSource = "chain"
			c.TableIDs = nil
			c.Chain = ChainConfig{RPCURL: "http://127.0.0.1:8545"}
		}, "chain.databases_contract_address is required"},
		{"chain address must be hex", func(c *Config) {
			c.SchemaSource = "chain"
			c.TableIDs = nil
			c.Chain = ChainConfig{RPCURL: "http://127.0.0.1:8545", DatabasesContractAddress: "nope"}
		}, "must be an Ethereum address"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mutate(&cfg)
			err := cfg.validate(false)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("want valid, got %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("want %q, got %v", tc.wantErr, err)
			}
		})
	}
}
```
Also add a defaulting test (self-contained, no ClickHouse or chain needed):
```go
func TestLoadConfig_DefaultsUnsafeDatabase(t *testing.T) {
	t.Setenv(envVerifierSeedHex, "") // keep an ambient seed env from interfering
	path := filepath.Join(t.TempDir(), "cfg.yaml")
	yaml := `replica_id: v1
network_id: net
schema_snapshot_id: snap
executor_profile_id: prof
clickhouse_addr: 127.0.0.1:9000
payload_dir: /tmp/p
peers:
  - { id: arb-1, grpc_addr: 127.0.0.1:7080 }
table_ids: [db.t]
`
	if err := os.WriteFile(path, []byte(yaml), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.UnsafeDatabase != "hg_unsafe" {
		t.Fatalf("want hg_unsafe default, got %q", cfg.UnsafeDatabase)
	}
}
```
(imports: `os`, `path/filepath`; the snode port in Task 8 drops the `t.Setenv` line — the snode cmd has no seed env override — and swaps the identity fields for `node_id`/`state_dir`/`authority_allowed_addresses`.)
- [ ] **Step 2: Run to verify failure** — `go test ./cmd/arbiter-verifier/ -run 'TestConfigValidate_SchemaSourceModes|TestLoadConfig_DefaultsUnsafeDatabase' -v` Expected: FAIL (undefined `ChainConfig`, `SchemaSource`; old `TestConfigValidate_TableSourceModes` must be deleted in the same edit or it fails compilation referencing `Tables`).
- [ ] **Step 3: Edit `config.go`** —
  - Delete: `Tables []TableConfig` field, `TableConfig`, `ColumnConfig`, `validateTables`, `validateTableSource`, `tables()`, `computedSchemaRoot()`, and the `if c.SchemaRoot != "" && len(c.Tables) != 0 ...` block in `validate`.
  - Add to `Config`: `SchemaSource string \`yaml:"schema_source"\`` and `Chain ChainConfig \`yaml:"chain"\``. Add:
```go
// ChainConfig points the chain schema source at a Databases contract.
type ChainConfig struct {
	RPCURL                   string             `yaml:"rpc_url"`
	DatabasesContractAddress string             `yaml:"databases_contract_address"`
	Timeout                  arbconfig.Duration `yaml:"timeout"`
}
```
  - In `loadConfig`, after unmarshal (before validate): `if cfg.UnsafeDatabase == "" { cfg.UnsafeDatabase = "hg_unsafe" }`.
  - In `validate`, where `validateTableSource` was called, substitute:
```go
	req(c.UnsafeDatabase, "unsafe_database")
	switch c.SchemaSource {
	case "", "clickhouse":
		if c.Chain != (ChainConfig{}) {
			errs = append(errs, errors.New("chain block requires schema_source: chain"))
		}
		validateTableIDs(c.TableIDs, &errs)
	case "chain":
		if len(c.TableIDs) > 0 {
			errs = append(errs, errors.New("table_ids must be empty when schema_source is chain: the table set is enumerated from the contract"))
		}
		req(c.Chain.RPCURL, "chain.rpc_url")
		req(c.Chain.DatabasesContractAddress, "chain.databases_contract_address")
		if c.Chain.DatabasesContractAddress != "" && !ethcommon.IsHexAddress(c.Chain.DatabasesContractAddress) {
			errs = append(errs, errors.New("chain.databases_contract_address must be an Ethereum address"))
		}
	default:
		errs = append(errs, fmt.Errorf("schema_source must be clickhouse or chain, got %q", c.SchemaSource))
	}
```
with import `ethcommon "github.com/ethereum/go-ethereum/common"` (new in the verifier config; the snode config already imports it un-aliased — keep each file internally consistent). Add:
```go
func validateTableIDs(tableIDs []string, errs *[]error) {
	if len(tableIDs) == 0 {
		*errs = append(*errs, errors.New("table_ids is required when schema_source is clickhouse"))
		return
	}
	seen := make(map[string]bool, len(tableIDs))
	for i, tableID := range tableIDs {
		if tableID == "" {
			*errs = append(*errs, fmt.Errorf("table_ids[%d] is required", i))
		}
		if tableID != "" && seen[tableID] {
			*errs = append(*errs, fmt.Errorf("table_ids[%d] duplicates %q", i, tableID))
		}
		seen[tableID] = true
	}
}
```
  - Delete the old `TestConfigValidate_TableSourceModes` and fix any other test in `main_test.go` that sets `Tables:` (grep the file; the sample-config test is handled in Task 9).
- [ ] **Step 4: Run to verify pass** — `go test ./cmd/arbiter-verifier/ -v` Expected: PASS except tests that touch `configs/verifier.local.yaml` (still `tables:` mode until Task 9 — if `TestVerifierSampleConfig_LoadsForSchemaRoot` fails on the not-yet-migrated sample, note it and proceed; it must be green after Task 9).
- [ ] **Step 5: Commit** —
```bash
git add cmd/arbiter-verifier/ && git commit -m "feat(verifier): schema_source config, drop inline tables mode"
```

---

### Task 7: arbiter — verifier main wiring

**Files:**
- Modify: `~/src/sentio_xyz/arbiter/cmd/arbiter-verifier/main.go` (`loadTableSchemas`, `printSchemaRoot`, the `-print-schema-root` flag help)

**Interfaces:**
- Consumes: Task 5's `chainschema.LoadTables` / `LoadOptions` / `DialCaller`; Task 6's `cfg.SchemaSource` / `cfg.Chain`.
- Produces: `loadTableSchemas(ctx, cfg, conn)` handling both sources (verifier passes `CrossCheck: nil` always — its scratch ClickHouse does not hold the source tables).

- [ ] **Step 1: Edit `main.go`** —
  - Flag text: `"print schema root for the configured schema source and exit (clickhouse mode requires reachable ClickHouse; chain mode requires reachable chain RPC)"`.
  - Replace `loadTableSchemas`:
```go
func loadTableSchemas(ctx context.Context, cfg Config, conn clickhouse.Conn) ([]payloadexec.TableSchema, error) {
	if cfg.SchemaSource == "chain" {
		caller, closeCaller, err := chainschema.DialCaller(cfg.Chain.RPCURL, ethcommon.HexToAddress(cfg.Chain.DatabasesContractAddress))
		if err != nil {
			return nil, err
		}
		defer closeCaller()
		tables, err := chainschema.LoadTables(ctx, chainschema.LoadOptions{
			Caller:         caller,
			NetworkID:      cfg.NetworkID,
			UnsafeDatabase: cfg.UnsafeDatabase,
			CrossCheck:     nil, // verifier ClickHouse is a scratch database without the source tables
			Timeout:        cfg.Chain.Timeout.Duration,
		})
		if err != nil {
			return nil, fmt.Errorf("load table schemas from chain: %w", err)
		}
		return tables, nil
	}
	refs := make([]schemaregistry.TableRef, 0, len(cfg.TableIDs))
	for _, id := range cfg.TableIDs {
		refs = append(refs, schemaregistry.TableRef{
			TableID:  id,
			Database: cfg.UnsafeDatabase,
			Table:    snode.CHTableName(id),
		})
	}
	tables, err := schemaregistry.NewClickHouseLoader(conn).Load(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("derive table schemas: %w", err)
	}
	return tables, nil
}
```
(imports gain `ethcommon "github.com/ethereum/go-ethereum/common"` and `"github.com/sentioxyz/arbiter/chainschema"`; the `len(cfg.TableIDs) == 0 → cfg.tables()` early return is gone with the mode.)
  - Replace `printSchemaRoot` (chain mode must not require ClickHouse):
```go
func printSchemaRoot(ctx context.Context, cfg Config, w io.Writer) error {
	var (
		tables []payloadexec.TableSchema
		err    error
	)
	if cfg.SchemaSource == "chain" {
		tables, err = loadTableSchemas(ctx, cfg, nil)
	} else {
		var conn clickhouse.Conn
		conn, err = openClickHouse(cfg)
		if err != nil {
			return err
		}
		defer conn.Close()
		tables, err = loadTableSchemas(ctx, cfg, conn)
	}
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, payloadexec.SchemaRoot(cfg.NetworkID, tables))
	return err
}
```
  - `run()` is unchanged (it already opens ClickHouse unconditionally — chexec needs it in every mode — and calls `loadTableSchemas(ctx, cfg, conn)`).
- [ ] **Step 2: Verify** — `go test ./cmd/arbiter-verifier/ -v && go build ./cmd/arbiter-verifier` Expected: PASS/green (same Task-9 caveat for the sample-config test).
- [ ] **Step 3: Commit** —
```bash
bazel run //:gazelle && git add cmd/arbiter-verifier/ && git commit -m "feat(verifier): chain schema source wiring"
```

---

### Task 8: arbiter — snode config + main (symmetric)

**Files:**
- Modify: `~/src/sentio_xyz/arbiter/cmd/arbiter-snode/config.go`, `~/src/sentio_xyz/arbiter/cmd/arbiter-snode/main.go`
- Test: `~/src/sentio_xyz/arbiter/cmd/arbiter-snode/main_test.go`

**Interfaces:**
- Consumes: same as Tasks 6–7. The snode config already imports `"github.com/ethereum/go-ethereum/common"` un-aliased — use `common.IsHexAddress` / `common.HexToAddress` there.
- Produces: identical `SchemaSource`/`ChainConfig`/`validateTableIDs` surface in the snode cmd; snode's chain branch passes `CrossCheck: conn` (the source tables exist in its local ClickHouse) while `printSchemaRoot`'s chain path passes a nil conn (pure chain read, consistent with the verifier).

- [ ] **Step 1: Apply Task 6 to `cmd/arbiter-snode/config.go`** — the file is a near-copy: same deletions (`Tables`/`TableConfig`/`ColumnConfig`/`validateTables`/`validateTableSource`/`tables()`/`computedSchemaRoot()`/the validate-time root check), same additions (`SchemaSource`, `ChainConfig`, `validateTableIDs`, `req(c.UnsafeDatabase, ...)` + `hg_unsafe` default in `loadConfig`, the same `switch c.SchemaSource` block using the file's existing un-aliased `common` import). Keep snode-specific validation (authority addresses etc.) untouched. Note the snode sample already sets `unsafe_database: hg_unsafe` explicitly.
- [ ] **Step 2: Apply Task 7 to `cmd/arbiter-snode/main.go`** — same flag text; same `loadTableSchemas` shape but with the cross-check difference:
```go
	if cfg.SchemaSource == "chain" {
		caller, closeCaller, err := chainschema.DialCaller(cfg.Chain.RPCURL, common.HexToAddress(cfg.Chain.DatabasesContractAddress))
		if err != nil {
			return nil, err
		}
		defer closeCaller()
		tables, err := chainschema.LoadTables(ctx, chainschema.LoadOptions{
			Caller:         caller,
			NetworkID:      cfg.NetworkID,
			UnsafeDatabase: cfg.UnsafeDatabase,
			CrossCheck:     conn, // source tables exist locally; nil when called from printSchemaRoot
			Timeout:        cfg.Chain.Timeout.Duration,
		})
		if err != nil {
			return nil, fmt.Errorf("load table schemas from chain: %w", err)
		}
		return tables, nil
	}
```
and the same `printSchemaRoot` rewrite (chain path calls `loadTableSchemas(ctx, cfg, nil)` — with a nil conn the `CrossCheck: conn` field is nil, disabling the cross-check for the offline root-printing tool).
- [ ] **Step 3: Mirror the tests** — port `TestConfigValidate_SchemaSourceModes` + `TestLoadConfig_DefaultsUnsafeDatabase` from Task 6 into `cmd/arbiter-snode/main_test.go` (adapting the valid-config helper), delete the old `TestConfigValidate_TableSourceModes`, and fix any `Tables:` literals.
- [ ] **Step 4: Verify** — `go test ./cmd/arbiter-snode/ -v && go build ./cmd/arbiter-snode` Expected: PASS (snode's sample-config test should stay green — its sample is already `table_ids` mode).
- [ ] **Step 5: Commit** —
```bash
bazel run //:gazelle && git add cmd/arbiter-snode/ && git commit -m "feat(snode): schema_source config and chain wiring"
```

---

### Task 9: arbiter — sample configs + README

**Files:**
- Modify: `~/src/sentio_xyz/arbiter/configs/verifier.local.yaml` (currently `tables:` mode), `~/src/sentio_xyz/arbiter/configs/snode.local.yaml` (already `table_ids` — add the commented chain example + `schema_source`), `~/src/sentio_xyz/arbiter/README.md` (the `tables`/`table_ids`/bootstrapping-verifier section), `cmd/arbiter-verifier/main_test.go` (`TestVerifierSampleConfig_LoadsForSchemaRoot` if it needs adjusting)

- [ ] **Step 1: Migrate `configs/verifier.local.yaml`** — replace the `tables:` block with:
```yaml
unsafe_database: hg_unsafe
table_ids: [db.t]
schema_source: clickhouse
# Chain mode needs no table list — the declared set is enumerated from the
# Databases contract and verified against schema_root:
# schema_source: chain
# table_ids: []
# chain:
#   rpc_url: "http://127.0.0.1:8545"
#   databases_contract_address: "0x0000000000000000000000000000000000000000"
#   timeout: 60s
```
Add the same commented `schema_source: chain` block to `configs/snode.local.yaml` and an explicit `schema_source: clickhouse` line next to its `table_ids`.
- [ ] **Step 2: Fix the sample-config tests** — read `TestVerifierSampleConfig_LoadsForSchemaRoot` / `TestSNodeSampleConfig_LoadsForSchemaRoot`; they must keep asserting that `loadConfig(configs/*.local.yaml)` succeeds without contacting ClickHouse or a chain (if a test currently exercises the offline `tables`-mode `printSchemaRoot` path, reduce it to the loadConfig assertion — the offline path no longer exists).
- [ ] **Step 3: Rewrite the README section** — replace the `tables` bullets and the bootstrapping-verifier paragraph with the two-mode story: `table_ids` + `schema_source: clickhouse` derives from local `system.columns` (source-side binaries, co-located deployments); `schema_source: chain` enumerates the declared set from the `Databases` contract, verifies every `schema_json` against its on-chain `schema_hash`, and is the path for a bootstrapping verifier with no local tables (no `tables` YAML anymore). Document `-print-schema-root` per mode (ClickHouse reachable vs chain RPC reachable) and its governance role (print the new root after a declaration for the genesis update), and the window semantics one-liner: a chain-mode start between a new declaration and the genesis `schema_root` update fails closed with `schema_root mismatch` — complete governance before restarting.
- [ ] **Step 4: Verify + commit** — `go test ./cmd/... -v` Expected: PASS including both sample-config tests.
```bash
git add configs/ README.md cmd/ && git commit -m "docs(configs): migrate samples to schema_source, document chain mode"
```

---

### Task 10: arbiter — env-gated devnet smoke

**Files:**
- Create: `~/src/sentio_xyz/arbiter/chainschema/devnet_smoke_test.go`

- [ ] **Step 1: Write the test** —
```go
package chainschema

import (
	"context"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// TestDevnetChainSchemaSmoke runs the full enumerate+load pipeline against a
// real deployment. Skipped unless the devnet coordinates are supplied:
//
//	ARBITER_CHAIN_SCHEMA_RPC=https://... \
//	ARBITER_CHAIN_SCHEMA_CONTRACT=0x... \
//	ARBITER_CHAIN_SCHEMA_NETWORK_ID=net-... \
//	go test ./chainschema/ -run TestDevnetChainSchemaSmoke -v
func TestDevnetChainSchemaSmoke(t *testing.T) {
	rpc := os.Getenv("ARBITER_CHAIN_SCHEMA_RPC")
	contract := os.Getenv("ARBITER_CHAIN_SCHEMA_CONTRACT")
	networkID := os.Getenv("ARBITER_CHAIN_SCHEMA_NETWORK_ID")
	if rpc == "" || contract == "" || networkID == "" {
		t.Skip("set ARBITER_CHAIN_SCHEMA_RPC, ARBITER_CHAIN_SCHEMA_CONTRACT, ARBITER_CHAIN_SCHEMA_NETWORK_ID to run")
	}
	caller, closeCaller, err := DialCaller(rpc, common.HexToAddress(contract))
	if err != nil {
		t.Fatal(err)
	}
	defer closeCaller()
	load := func() []payloadexec.TableSchema {
		tables, err := LoadTables(context.Background(), LoadOptions{
			Caller: caller, NetworkID: networkID, UnsafeDatabase: "hg_unsafe",
		})
		if err != nil {
			t.Fatal(err)
		}
		if len(tables) == 0 {
			t.Fatal("devnet has no declared tables")
		}
		return tables
	}
	first := payloadexec.SchemaRoot(networkID, load())
	second := payloadexec.SchemaRoot(networkID, load())
	if first != second {
		t.Fatalf("schema root unstable across runs: %s vs %s", first, second)
	}
	t.Logf("devnet schema root: %s", first)
}
```
- [ ] **Step 2: Verify skip-by-default** — `go test ./chainschema/ -run TestDevnetChainSchemaSmoke -v` Expected: SKIP. If devnet coordinates are available in the execution environment, run once with them and record the printed root in the PR description; otherwise state in the PR that the smoke was verified skip-only.
- [ ] **Step 3: Commit** —
```bash
bazel run //:gazelle && git add chainschema/ && git commit -m "test(chainschema): env-gated devnet smoke"
```

---

### Task 11: Full verification + PR

- [ ] **Step 1: Full local gates** —
```bash
cd ~/src/sentio_xyz/arbiter
./scripts/databases-bindings.sh check && ./scripts/anchor-bindings.sh check
bazel mod tidy && bazel run //:gazelle && git diff --exit-code   # no un-committed BUILD drift
bazel build //... && bazel test //...
```
Expected: all green. Compare any Bazel test failures against a clean `origin/main` baseline before attributing them to this change.
- [ ] **Step 2: PR** —
```bash
git push -u origin feat/chain-schema-source
gh pr create --title "feat: chain schema source for verifier and snode cmds" --body "..."
```
PR body: link the spec (housegate `docs/superpowers/specs/2026-07-30-arbiter-chain-schema-source-design.md`), summarize the decision table (chain-direct only; tables mode deleted; chain mode enumerates the declared set — zero per-table config; startup-frozen + fail-closed window semantics), list the test evidence (unit suites, config matrices, devnet smoke result or skip note), and note the arbiter-core dedup PR from Task 1 as a sibling.

## Execution Notes

- Task order: 1 is independent (can land any time); 2 → 3 → 4 → 5 are strictly sequential; 6 → 7 and 8 depend on 5; 9 depends on 6–8; 10 depends on 5; 11 last.
- If abigen's generated struct/type names differ from the plan's (`TypesDatabase` etc.), fix the plan's hand-written code to the generated names at first use (Task 4 Step 3, Task 5 Step 3) — the generated file is authoritative.
- If `TestVerifierSampleConfig_LoadsForSchemaRoot` fails between Tasks 6 and 9, that is the expected mid-flight state (sample not yet migrated); it must be green by the end of Task 9.
