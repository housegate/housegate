# Storage-Integrity Schema Registry Design (network-state distributed, consensus-anchored)

**Status:** Proposed. **Base:** arbiter-core main (staged intake seam landed; `snode.Config.Tables` is static column-level config), sentio-node PR #163 (static `storage_integrity.snode.tables` YAML), housegate PR #98 (`sicore.NativeCSVPayloadMaterializer` + `SchemaResolver`), sentio-core network state (`TableInfo{TableId, TableType}` — no column data today). **Parent designs:** [staged intake seam](2026-07-28-arbiter-snode-staged-intake-design.md) (the consumers), [P1c data plane](2026-07-06-arbiter-p1c-dataplane-design.md) decision 8 (`schema_root` as a genesis consensus param), [master arbiter design](2026-06-30-sentio-arbiter-design.md) §5.3 (schema barrier / `schema_snapshot_id`, the P2 lane this registry feeds), da.proto's hash-silent trust model (the pattern this design transplants).

## 1. Problem

Storage-integrity data-plane roles need column-level table schemas (`payloadexec.TableSchema`: ordered `{name, type}` columns + partition key) — they feed row encoding, row-ids, LtHash, partition attribution, and the `schema_root` consensus check byte-for-byte. Today those schemas are hand-written YAML in three places (sentio-node `storage_integrity.snode.tables`, housegate `merge_guard.tables`, the real ClickHouse DDL) with no cross-check, and the network state carries only `{TableId, TableType}` — nothing column-level. Sentio tables are created dynamically by user processors, so a static-YAML model cannot follow the platform; but the naive fix — "roles read the latest schema from mutable state" — would silently break the integrity model: source and verifiers must hold an *identical, committed* schema or replay roots diverge undetectably.

## 2. Design principle (the invariant everything else follows from)

**Network state is the schema's availability layer, never its integrity layer** — exactly the da.proto stance transplanted: payload bytes live in the DA store but `payload_hash` lives in the signed envelope, and readers re-verify. Likewise:

```
schema CONTENT     → network state (distributable: versioned, globally readable, reaches roles with no local table)
schema COMMITMENT  → consensus anchor (v1.5: genesis schema_root; P2: the block-carried schema_snapshot_id)
every consumer     → fetch content → recompute hash → compare to the anchor → mismatch = fail closed
```

A consumer never acts on network-state schema it has not re-hashed against a consensus-anchored commitment. Redis lag, poisoning, or partial propagation therefore degrade to a loud startup/refresh failure — never to divergent replay roots.

## 3. Decisions

| # | Decision | Rationale |
|---|---|---|
| 1 | **Hash domains are reused, not invented:** per-table commitment = `payloadexec.TableSchemaHash(networkID, schema)`, set commitment = `payloadexec.SchemaRoot(networkID, schemas)` ([exports.go](../../pkg/replay/payloadexec/exports.go) — exported precisely for arbiter genesis checks). | One canonical derivation across housegate, arbiter-core, and this registry; a second schema-hash domain would be a fork risk for zero benefit. |
| 2 | **On-chain records are commitment-only:** the registry contract carries `(table_id, schema_version, schema_hash)` per table (extending the existing `Types.Table` struct or a companion `setTableSchema(dbId, tableId, version, schemaHash)` method + event — final shape belongs to the contracts repo). Column content never goes on-chain. | Chain storage is expensive and the chain's job here is ordering + commitment; content rides the same event→state pipeline the registry already has. Precedent: sentio-core already versions processor schemas (`EntitySchemaVersion`). |
| 3 | **Content lives in network state as its own collection, commitment pointers ride `TableInfo`.** `TableInfo` (sentio-core → sentio-node convert → housegate `pkg/registry.Table`) grows only `SchemaVersion uint64` + `SchemaHash string`. Column content goes into a NEW statemirror collection — `statemirror:v1:TableSchemas`, hash field `<databaseId>/<tableId>@<version>`, value `{columns: [{name, type}], partition_by, schema_hash}` — written by the same syncer that mirrors the chain event, write-once per version key. Precedent: `ProcessorInfo.EntitySchema`/`EntitySchemaVersion` already carries full schema content through this pipeline. | Today `UpsertDatabaseTable` read-modify-writes the ENTIRE `DatabaseInfo` JSON (tables are embedded in one Redis hash field per database); inlining columns there would balloon that value and its write contention. A separate write-once-per-version collection keeps the Databases value thin, is naturally race-free, and gives P2's barrier switch the old/new version coexistence it needs for free. |
| 4 | **Consumer loading sequence** (snode, verifier, and housegate's `SchemaResolver` — all three go through one shared loader): resolve the SI table set → fetch `{version, hash, columns, partition_by}` from network state → recompute `TableSchemaHash` per table and reject on mismatch → assemble the set → recompute `SchemaRoot` and compare to the genesis `schema_root` → any failure refuses startup. Static column YAML (`storage_integrity.snode.tables[].columns`) is deleted; config keeps only the table-set selector. | The anchor check is what makes network state safe to trust for delivery (principle §2). Roles with no local tables yet (a bootstrapping verifier) can now load schemas at all — the CH-derive alternative cannot serve them. |
| 5 | **Local ClickHouse cross-check:** after loading, snode (and verifier where tables exist locally) diffs the registry schema against `system.columns`/`system.tables` for each SI table; declared-vs-actual mismatch refuses startup with a pointed error. | Catches "table was created wrong" at boot instead of surfacing as an unattributable replay hash mismatch — the failure users hit today has no good diagnosis path. |
| 6 | **v1.5 table-set governance stays frozen-set + runbook:** the SI table set and its `schema_root` remain genesis parameters; newly created tables are NOT in the SI lane by default. Onboarding a batch = declare schemas on-chain → wait for state propagation → derive the new root with the upgraded `-print-schema-root` (now registry-fed) → governance-update genesis → rolling restart, every role re-anchoring against the new root. | Changing a consensus parameter must stay a deliberate, observable operation until P2 gives DDL a protocol-level ordering. The runbook makes it operable; the registry makes it mechanical instead of hand-edited YAML. |
| 7 | **P2 hand-off is designed-in, not implemented:** `schema_snapshot_id` becomes the content hash of a versioned registry snapshot (the exact `SchemaRoot` of a pinned `(table_id → version)` map); the DDL lane (master design §5.3: exclusive block, snapshot minting, barrier switch) orders *when* each snapshot takes effect, and statements/manifests already carry `SchemaSnapshotID` fields pointing at it. This registry is that mechanism's storage layer, built early. | Phase-B work is not throwaway: P2 reuses the distribution pipeline, the hash domains, and the loader verbatim, swapping only the anchor (genesis constant → block-carried snapshot id). |
| 8 | **Phasing:** **Phase A** (immediate, no data-model change): CH-derived loader in sentio-node/arbiter-core — config drops column YAML for `table_ids`, columns read from local `system.columns`, root still anchored to genesis. **Phase B** (this spec's main body): the four-layer registry + shared network-state loader + cross-check, replacing Phase A's CH source. **Phase C** (out of scope, P2): DDL barrier + snapshot minting on top. | A ships in days and kills the YAML-drift problem now; B is a 4-repo change that lands behind it without conflicting (the loader seam is shared — only the source swaps); C is protocol work with its own spec. |
| 9 | **Trust posture of the SI schema writer:** schema declarations enter the chain through the same authenticated path as `EnsureTable` today (`database_registry` server, indexer-signed). v1.5 makes no attempt to verify that the declared schema matches the creator's intent — the CH cross-check (decision 5) and the replay quorum remain the behavioral backstops. | Same trust envelope as the existing table registry; tightening authorship is a governance concern, not a data-plane one. |

## 4. Data model (per layer)

```
contract     Types.Table                 + schemaVersion uint64, schemaHash string   (contracts repo)
             event TableSchemaSet(dbId, tableId, version, schemaHash)               (contracts repo)
sentio-core  network/state/types.go      TableInfo + SchemaVersion, SchemaHash (pointers only)
             NEW collection MappingTableSchemas ("TableSchemas"): field
             <databaseId>/<tableId>@<version> → {columns, partition_by, schema_hash}
             state.go: UpsertTableSchema/GetTableSchema; StateMirrored codec for it
sentio-node  standalone/networkstate     redis reader for the new collection +
             convert.go projection widened with the two pointer fields
housegate    pkg/registry.Table          + SchemaVersion, SchemaHash; registry gains
             TableSchema(databaseId, tableId, version) content lookup
             pkg/network yaml source     + same fields/content block (local fixtures only —
             the redis path lives in sentio-node since the sentio-core decoupling)
```

Content propagation: the chain event carries only the commitment; the column payload is submitted alongside the registration call and mirrored into the `TableSchemas` collection by the existing syncer path, write-once per `(table_id, version)` key. A consumer that finds the commitment but not the content treats it as unavailable (retry/fail closed) — never trusts a bare hash, never serves without verification. `ColumnDef.Type` is the exact ClickHouse type string that feeds `TableSchemaHash` — no normalization layer in v1.5 (declare exactly what the DDL says; the cross-check enforces agreement).

## 5. Consumption (one shared loader, three call sites)

A new `schemaregistry.Loader` (housegate repo, next to `pkg/registry` — it is a consumer-side concern) implements: `Load(ctx, tableIDs) ([]payloadexec.TableSchema, error)` running the decision-4 sequence, plus `CrossCheckClickHouse(ctx, conn, schemas) error` for decision 5. Call sites: arbiter-core `snode.Config` gains a constructor path from the loader (sentio-node assembly calls it before `snode.New`); the verifier binary/assembly likewise; housegate's `SchemaResolver` for the Native→CSV materializer wraps the same loaded set. Failure taxonomy: `ErrSchemaContentMissing` (commitment present, content absent — retryable), `ErrSchemaHashMismatch` (content ≠ commitment — fail closed, integrity incident), `ErrSchemaRootMismatch` (set ≠ genesis anchor — fail closed, wrong table set or un-onboarded change), `ErrClickHouseDrift` (declared ≠ actual DDL — fail closed, ops error). All four refuse startup; none is retried silently.

## 6. Non-goals

The DDL barrier / snapshot-minting protocol (Phase C, P2 — gets its own spec); `ALTER` semantics of any kind (v1.5 versions are write-once per table onboarding); multi-network schema namespacing; automated genesis updates (the runbook is deliberate); contract-side authorization changes; migrating the `merge_guard.tables` config (it stays `{database, table}`-level — but gains a cross-check that every SI table id resolves into it, closing the PR #163 review gap).

## 7. Delivery boundaries

| Repo | Delivers | Phase |
|---|---|---|
| contracts (external team) | `Types.Table` extension or `setTableSchema` + event | B |
| sentio-core | state types + observer mirroring of schema content | B |
| sentio-node | convert.go projection; assembly switches `snode.Config.Tables` to the loader; config drops column YAML (A: `table_ids` + CH-derive; B: registry source) | A + B |
| housegate | `pkg/registry.Table`/`pkg/network.TableInfo` widening; `schemaregistry.Loader`; `SchemaResolver` rewire | B |
| arbiter-core | `snode`/`verifier` config accept loader-built schemas; `-print-schema-root` upgraded to registry-fed derivation | A + B |

## 8. Testing sketch

Loader unit tests against a fake registry (hash mismatch / missing content / root mismatch / drift each mapped to its sentinel); a golden-vector test pinning `TableSchemaHash`/`SchemaRoot` over a known registry payload (guards the no-new-domain claim); sentio-node assembly test proving YAML-column configs are rejected after the switch; an end-to-end onboarding rehearsal in the chpipeline harness (declare → propagate via fake state → load → anchor-check → run one staged INSERT to Safe); cross-check test with a deliberately drifted CH table.
