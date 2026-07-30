# Arbiter chain schema source — design

**Status:** Proposed. **Parent:** [schema-registry design](2026-07-28-schema-registry-design.md) (decisions 2/4/5 frozen there govern hash/verification semantics) and [Phase B design](2026-07-29-schema-registry-phase-b-design.md) (the contract + declaration path this spec consumes). **Base:** arbiter `origin/main` d84735d (`table_ids` CLI mode, #12), arbiter-core `origin/main` b277f57 (`snode.CHTableName`, #6), housegate ≥ #113 (`pkg/schemaregistry.NetworkStateLoader`, `pkg/registry.TableSchemas`), compute-network-contracts ≥ #75 (`setTableSchema` + view getters), sentio-node ≥ #167 (declaration hardened, allowlist-gated). Facts below cite the 2026-07-30 exploration of arbiter `cmd/`, arbiter-core `verifier/`+`snode/`, and the sentio-node bindings ABI.

## 1. Problem

Phase B distributes declared table schemas through network state with an on-chain commitment, but arbiter never consumed it: `cmd/arbiter-verifier` and `cmd/arbiter-snode` still choose between an inline full-schema `tables` block and a `table_ids` list derived from live ClickHouse. A bootstrapping verifier — no local copy of the source tables, ClickHouse is a per-statement scratch environment (`chexec` creates and drops throwaway tables) — can use neither, so it must hand-transcribe the `tables` YAML that Phase A eliminated everywhere else (arbiter README spells this out: keep the YAML "until the network-state registry in Phase B provides schema content independently of local storage"). This spec closes that gap: the two role binaries gain `schema_source: chain`, reading both the table **set** and the table **content** from the `Databases` contract's view functions, verified through the existing housegate ladder. Config carries no per-table state at all in chain mode; the genesis `schema_root` remains the operator's only schema input.

## 2. Decisions

| # | Decision | Rationale |
|---|---|---|
| 1 | **Chain-direct only; no redis mirror channel in arbiter.** The `registry.TableSchemas` seam is implemented over `eth_call` against the `Databases` contract. sentio-node keeps its private statemirror read leg; the two coexist at the system level. | The verifier's root of trust is the chain and an eth RPC endpoint is already a hard dependency (anchor). Schema loading is a one-shot startup read — tens of tables, ~2 calls each — so the mirror's high-frequency-read optimization buys nothing. Arbiter already ships go-ethereum v1.17.2 + ethclient + the abigen toolchain (anchor backend): zero new dependencies. A mirror channel can be added later behind the same seam if internal fleets ever outgrow direct RPC. |
| 2 | **The inline `tables` mode is deleted.** Config converges on the sentio-node model: `schema_source ∈ {"", "clickhouse", "chain"}` selects the content source; `TableConfig`/`ColumnConfig`, `validateTables`, `validateTableSource`, and the cmd-layer `computedSchemaRoot` all go. | Its only justification was the bootstrapping verifier this spec unblocks. Deleting it also dissolves the known asymmetry where the cmd-layer `schema_root` cross-check ran in `tables` mode only — root validation collapses to the single post-derivation check in the role layer (`arbiter-core verifier/config.go:48`, `snode/config.go:69`). |
| 3 | **Chain mode derives the table set from the contract; `table_ids` is rejected there.** Enumeration: `getDatabases()` → keep `active && !pendingDelete` → per DB `getDatabaseTables()` → keep `active` → per table `latestTableSchemaVersion() > 0` → include, sorted by TableID. Configuring `table_ids` alongside `schema_source: chain` is a validation error. | For a consumer-side role the covered set is a chain-derivable **fact**, not an intent input: a table is in the SI set iff it was declared, and declarations only happen after the source-side operator admits the table to its allowlist (sentio-node #167). A second hand-maintained list is a two-source drift generator (list updated but root not, or vice versa). `SchemaRoot` is order-canonical over its input set, but the enumeration sorts by TableID anyway so logs and `-print-schema-root` output are deterministic. sentio-node keeps `table_ids` because the source side **is** the intent input (what to declare, what to STOP MERGES). |
| 4 | **`clickhouse` mode keeps `table_ids` (required, non-empty, deduped).** | Local ClickHouse has no authoritative marker for "which tables are SI-covered" — the unsafe database may legitimately hold non-SI tables — so the covered set must be an input. This mode's realistic users are `cmd/arbiter-snode` (source-side reference binary, tables exist locally), integration harnesses, and co-located deployments; with `tables` deleted, a clean verifier's only real path is `chain`. |
| 5 | **The new enum value is named `chain`, not `network_state`.** | sentio-node's `network_state` reads the availability layer (redis mirror); arbiter reads the authoritative layer (the chain itself), and its config block wants an eth RPC URL + contract address, not a redis address. Naming the value after what it dials keeps operator errors diagnosable. Both are the same registry content and verify through the same ladder. |
| 6 | **Startup-frozen table set; no runtime refresh, no event subscription.** One enumeration + load at process start; `TableSchemaSet` watching is out of scope. | Refresh is unusable by construction: a newly discovered table changes `SchemaRoot(networkID, tables)` away from the operator's configured genesis root, so the role-layer check would (correctly) refuse it. Honoring it would mean auto-accepting whatever the chain currently says — bypassing governance and destroying the property that `schema_root` is the operator's independent trust input. The whole pipeline is startup-frozen anyway (sentio-node `snode.Config.Tables`, housegate `SchemaResolver`, merge guard). Protocol-level set evolution is Phase C (`schema_snapshot_id`); this enumeration is its content-channel groundwork. See §6 for the full lifecycle walkthrough. |
| 7 | **RPC failure and "not declared" are distinguished without changing the housegate interface.** `registry.TableSchemas` methods return `(TableSchema, bool)` with no error. The contract-backed implementation records the last RPC error internally; the load entrypoint checks it after `Load` returns and converts a content-missing failure with a pending RPC error into a "chain schema source unavailable" startup error. | `ErrSchemaContentMissing` must mean "the network really has no declaration for this table", never "the RPC timed out" — misdiagnosing an outage as a missing declaration sends the operator down the wrong runbook. Widening the housegate interface with an error-returning variant would touch a public seam shared with sentio-node for strictly local benefit. |
| 8 | **Tests: fake-caller unit tests + an env-gated devnet smoke; no simulated-backend or anvil deployment path.** | `Databases` is a UUPS proxy whose deployment drags in the forge toolchain; devnet already runs the real deployment (Phase B upgraded it). This differs from anchor's sim tests, where the contract is simple and its bindings self-deploy. The fake covers all decision logic; the smoke proves the real ABI/hex/hash pipeline end-to-end when an RPC is available. |

## 3. Config surface

Both role binaries change identically (their config packages are historically duplicated copies; this spec does not merge them — see §7 for how new logic avoids a third copy).

Removed: `tables` (`TableConfig`, `ColumnConfig`, `validateTables`, `validateTableSource`, `computedSchemaRoot` and its `tables`-mode-only `schema_root` pre-check). `configs/verifier.local.yaml` migrates from `tables:` to `table_ids:` + `schema_source: clickhouse`, with a commented `chain` example alongside.

Added / changed:

```yaml
schema_source: chain        # "" | "clickhouse" | "chain"; "" falls back to clickhouse (matches sentio-node)
table_ids: []               # required non-empty + deduped in clickhouse mode; MUST be absent in chain mode
unsafe_database: hg_unsafe  # now defaulted to hg_unsafe and validated non-empty (previously undefaulted; the shipped
                            # verifier sample omitted it, so table_ids mode failed with "database is required")
chain:                      # required iff schema_source: chain; rejected otherwise
  rpc_url: "https://..."                    # eth JSON-RPC endpoint; independent field, deliberately not shared
                                            # with anchor config (registry chain and anchor chain may differ)
  databases_contract_address: "0x..."       # Databases (IDatabases) proxy address
  timeout: 60s                              # optional; one deadline over the whole enumerate+load sequence
```

Validation matrix (aggregated via the existing `errs` pattern): `schema_source` outside the allowlist → error; `chain` block present without `schema_source: chain` → error; chain mode with `table_ids` set → error ("table_ids must be empty when schema_source is chain: the table set is enumerated from the contract"); chain mode missing `rpc_url` or `databases_contract_address` → error. `network_id` is the existing required role field and doubles as the `TableSchemaHash` domain — no new field.

`-print-schema-root` works in every mode through the same load path (clickhouse mode needs reachable ClickHouse, chain mode needs a reachable RPC; flag help text updated accordingly). In chain mode it closes the governance loop: after a new table's declaration lands, one invocation prints the root that becomes the genesis-update proposal input.

## 4. `chainschema` package (arbiter repo, internal)

One new package shared by both cmds; arbiter is private, so no public-API surface is created.

```go
// DatabasesCaller is the narrow view-only surface of the Databases contract
// this package needs; the production implementation is the abigen-generated
// IDatabasesCaller, tests supply a fake.
type DatabasesCaller interface {
    GetDatabases(opts *bind.CallOpts) ([]TypesDatabase, error)
    GetDatabaseTables(opts *bind.CallOpts, databaseId string) ([]TypesTable, error)
    LatestTableSchemaVersion(opts *bind.CallOpts, databaseId, tableId string) (uint32, error)
    GetTableSchema(opts *bind.CallOpts, databaseId, tableId string, version uint32) (TypesTableSchema, error)
}

// ContractTableSchemas implements housegate registry.TableSchemas over eth_call.
type ContractTableSchemas struct { /* caller, ctx, mu, lastErr */ }
func (c *ContractTableSchemas) LatestTableSchema(databaseId, tableId string) (registry.TableSchema, bool)
func (c *ContractTableSchemas) TableSchema(databaseId, tableId string, version uint32) (registry.TableSchema, bool)
func (c *ContractTableSchemas) LastError() error

// LoadTables is the single entrypoint both cmds call in the chain branch of
// loadTableSchemas: enumerate → sort → NetworkStateLoader.Load → error classing.
func LoadTables(ctx context.Context, opts LoadOptions) ([]payloadexec.TableSchema, error)
type LoadOptions struct {
    Caller         DatabasesCaller
    NetworkID      string
    UnsafeDatabase string
    CrossCheck     clickhouse.Conn // nil = off; snode passes its conn, verifier passes nil
    Timeout        time.Duration   // default 60s
}
```

Contracts and conventions the implementation must hold:

- **Hash encoding** is `"0x" + hex.EncodeToString(schemaHash[:])`, byte-for-byte the sentio-node handler convention (`handlers/database_event.go`), because `NetworkStateLoader` compares the declared hash as a string against recomputed `payloadexec.TableSchemaHash`.
- **TableID join/split** is the established `<databaseId>.<tableId>` convention shared with the declarer's backfill split and housegate's `logicalTableCoordinates`. Enumeration builds `schemaregistry.TableRef{TableID: db + "." + table, Database: opts.UnsafeDatabase, Table: snode.CHTableName(id)}`.
- **`LatestTableSchema`** = `latestTableSchemaVersion` (0 → not found) then `getTableSchema`; the returned `registry.TableSchema.Version` is the cursor value (the getter does not echo it). RPC errors record into `lastErr` and return `(zero, false)`.
- **Bindings** live under `chainschema/bindings/`, caller-only, generated for the four views above, with the same drift protection as anchor: a generation script (extend `scripts/anchor-bindings.sh` or add a sibling), `.sha256` pins for source and output, a Makefile `gen`/`check` pair, and the check wired into CI.

housegate is untouched: enumeration sits **in front of** the `Loader` seam, `registry.TableSchemas` keeps its two lookup methods, and `NetworkStateLoader`'s verification ladder is reused verbatim.

## 5. Data flow (chain mode, both binaries)

```
parse config → ethclient.Dial(chain.rpc_url) → IDatabasesCaller(chain.databases_contract_address)
  → chainschema.LoadTables(ctx, opts)                       [one chain.timeout deadline over all of it]
      1. enumerate: getDatabases() → active && !pendingDelete
           → per DB getDatabaseTables() → active
           → per table latestTableSchemaVersion() > 0 → include
           → TableRefs, sorted by TableID
      2. load+verify: NewNetworkStateLoader(contractSchemas, networkID)
           [.WithClickHouseCrossCheck(conn) — snode only]
           .Load(ctx, refs)   // per table: getTableSchema → decode schema_json
                              // → recompute TableSchemaHash → compare declared hash
      3. classify errors (decision 7; §7)
  → role config {Tables, SchemaRoot, NetworkID, ...}
  → role-layer check: payloadexec.SchemaRoot(networkID, tables) == cfg.SchemaRoot, else refuse to start
```

The verifier runs no ClickHouse cross-check (its scratch CH does not hold the source tables); `cmd/arbiter-snode` does (tables exist locally — the same belt-and-suspenders sentio-node applies in `network_state` mode).

## 6. Table lifecycle and refresh semantics

The set is frozen at startup by design; this section pins the runtime story so nobody "fixes" it later.

- **In-flight new tables produce no claims.** A table only generates SI claims after the source side admits it (sentio-node config + restart), and the source pipeline is itself startup-frozen. Between on-chain declaration and the governance root update, a running chain-mode verifier simply never sees the table in any block.
- **The governance restart is where the new table appears.** A set change means a new `schema_root`, and every verifier must restart with the new root anyway — it is a consensus startup parameter. Chain mode collapses that restart's config delta to exactly one value (the root); there is no per-verifier table list to forget to update.
- **Window behavior is fail-closed and intended.** A chain-mode verifier (re)started after a new declaration but before the governance update enumerates the larger set, computes a root that differs from its configured genesis root, and refuses to start with `schema_root mismatch`. Correct: a set governance has not ratified must not participate in replay. Operational rule: complete the governance update promptly after declaring, and avoid restarting chain-mode verifiers inside the window. The declaration cadence is operator-controlled (declarations only fire for allowlisted tables on the source side).
- **Out-of-set statements are rejected, not absorbed.** If a block ever carries a statement for a table outside the frozen set (source bug or malice), the executor fails with "no pinned schema for table" and the verifier refuses to attest — the existing fail-closed path.
- **Phase C is the successor, not a patch.** DDL-barrier-minted `schema_snapshot_id`s make set evolution a protocol object; statements bind to snapshots and verifiers resolve the set per job. This spec's enumeration + content channel is deliberately the substrate that survives that transition (only the anchoring moves).

## 7. Error handling and incidental fixes

Startup is fail-fast with no internal retries (process managers own restart policy, matching arbiter's style). Distinct failures get distinct errors: RPC unreachable / call errors → "chain schema source unavailable: …" (via `LastError()`, never conflated with a missing declaration); zero declared tables → "no declared storage-integrity tables found on chain" (clearer than the root mismatch an empty set would eventually produce); `ErrSchemaHashMismatch` (declared json vs declared hash — declarer bug or corruption) → refuse to start; `ErrClickHouseDrift` (snode only) → refuse to start; role-layer `schema_root mismatch` → refuse to start (§6 window semantics).

Incidental fixes riding along: `unsafe_database` gains the `hg_unsafe` default + non-empty validation (§3); the cmd-layer `tables`-mode residue is deleted wholesale (§2 decision 2); and in **arbiter-core**, `verifier/backends.go`'s private `chTableName` duplicate is replaced by the exported `snode.CHTableName` it predates (a one-line independent PR — the export in #6 existed precisely to kill such copies).

New shared logic lives in `chainschema` so the two cmds' historically duplicated config packages do not gain a third copy: each cmd's `loadTableSchemas` keeps its clickhouse branch as-is and calls `chainschema.LoadTables` in the chain branch. No wholesale config-package merge in this change.

## 8. Testing

- **Fake-caller unit tests** (`DatabasesCaller` fake): enumeration filtering (inactive/pendingDelete databases, inactive tables, `version == 0`), TableID sort determinism, hash hex encoding against a known vector shared with the housegate loader fixtures (byte-for-byte the sentio-node handler encoding), `LastError` classification (RPC error vs genuinely undeclared), declared-hash mismatch → `ErrSchemaHashMismatch` surfaces, empty enumeration → the dedicated error.
- **Config validation tests** (both cmds): the §3 matrix — chain×`table_ids` rejection, clickhouse×missing-`table_ids` rejection, `chain` block presence/absence rules, `unsafe_database` defaulting.
- **Env-gated devnet smoke**: with an RPC + contract address supplied via env (skip otherwise), run the full `LoadTables` against the real devnet deployment and assert the returned set is non-empty, hashes verify, and `SchemaRoot` is stable across two runs. No sim/anvil deployment path (decision 8).
- **Existing suites as guards**: chpipeline integration constructs role configs directly and must stay green; arbiter-core's verifier backend tests cover the `CHTableName` swap.

## 9. Non-goals

Redis-mirror channel in arbiter (decision 1 — sentio-node keeps its own); runtime refresh or `TableSchemaSet` event subscription (decision 6); any sequencer (`cmd/arbiter`) change — it holds only the opaque genesis root string; wholesale merge of the two cmds' duplicated config packages; `ALTER`/schema-evolution semantics; Phase C (`schema_snapshot_id`, DDL barrier); changes to housegate or sentio-node (both are consumed as-is).

## 10. Delivery boundaries

| Step | Repo | Delivers |
|---|---|---|
| 1 | arbiter-core | `verifier/backends.go` `chTableName` → `snode.CHTableName` (independent, can land first) |
| 2 | arbiter | `chainschema` package + bindings + generation/check scripts; config convergence (delete `tables`, add `schema_source`/`chain` block, `unsafe_database` default) in both role cmds; sample config + README updates; unit/config/smoke tests |

Step 2 depends on nothing in step 1 (the duplicate removal is hygiene, not a prerequisite). No housegate, sentio-node, sentio-core, or contracts changes.
