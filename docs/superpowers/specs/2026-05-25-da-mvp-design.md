# DA-Layer Data Sync MVP — Design Spec

**Date:** 2026-05-25
**Status:** Draft
**Authors:** poetry, Claude

> Chinese translation: [`2026-05-25-da-mvp-design.zh-CN.md`](./2026-05-25-da-mvp-design.zh-CN.md).
> Supersedes the L1 (S3-backed MergeTree) approach in
> [`2026-05-12-data-reliability-design.md`](./2026-05-12-data-reliability-design.md)
> as the candidate for the durability layer, pending the measurements
> defined in §7 of this document.

## 1. Goal

Deliver an **end-to-end prototype**, in two weeks of a single engineer's
time, that exercises the data-availability (DA) layer as a sync substrate
for housegate's ClickHouse data: writes on a source CH instance get
published to a DA layer, and a fresh ClickHouse instance can be
rebuilt from DA blobs alone. The MVP exists to **collect three
measurements** — sustained throughput, end-to-end latency, and $/GB cost
— that drive a go / no-go decision on whether DA can serve as the
data-plane durability mechanism (replacing the S3-backed approach in the
prior reliability spec), as a commitment-only auxiliary layer, or as
neither.

**The MVP is not production-ready software.** It is a measurement rig.
The reason to build it is that the existing public information on DA
throughput, latency, and effective $/GB at sustained load is too sparse
to decide the reliability architecture by analysis alone — we need
numbers from a real ClickHouse-shaped workload.

## 2. Non-goals

To keep the two-week budget honest, the following are explicitly out of
scope for the MVP. Each is a real concern that becomes in-scope only
**after** the MVP's go / no-go decision is taken:

- **No real-time sync.** Publisher polls `system.parts` on a 60-second
  cycle. Stream-from-write hook is post-MVP.
- **No housegate plugin integration.** Publisher and rebuilder are
  standalone CLIs (sidecars). Wiring into the proxy's plugin chain is
  post-MVP.
- **No DDL / schema evolution.** Schema is fixed at publisher start and
  any drift aborts. `ALTER TABLE`, `DROP COLUMN`, etc. are post-MVP.
- **No multi-table / multi-DB coordination.** One table, one publisher
  process. Fan-out is post-MVP.
- **No publisher high availability.** Single-process, single-checkpoint
  file. Lease-based or quorum publisher is post-MVP.
- **No Byzantine fraud-proof.** Integrity is a blake3 hash per blob,
  written to the anchor contract; Merkle proofs over part contents are
  post-MVP.
- **No economic / staking model.** Who pays the DA gas, who slashes
  publisher misbehavior — all post-MVP.
- **No data confidentiality.** Celestia blobs are public. We assume the
  indexed on-chain data is already public; see §10 Open Q1.

## 3. Key design decisions

Each row is the choice plus the one-line reason. Alternatives that lost
are listed in §11.

| Decision | Choice | Rationale |
|---|---|---|
| DA layer | **Celestia Mocha-4 testnet** | Mature Go SDK; stable PayForBlobs spec; testnet TIA is free; can re-run on EigenDA / Avail later for comparison |
| Integration | **Sidecar CLI process (not a housegate plugin)** | MVP measures DA, not integration. Avoids changing the proxy's hot path and lets the team iterate independently |
| Data format | **Parquet** via `SELECT … FORMAT Parquet` | CH-native both directions, stable across CH versions, compressed, language-agnostic |
| Code location | `tools/da-mvp/` in the housegate repo. **Two separate binaries** built under `tools/da-mvp/cmd/da-publisher/` and `tools/da-mvp/cmd/da-rebuilder/` — not added as subcommands to the `housegate` binary | Keeps experimental code out of the production binary; still reuses Bazel build / CI / project conventions. If the MVP graduates, the binaries can move to `cmd/` and the library to `pkg/da/` |
| Anchor chain | **Local anvil + a single Solidity contract** | MVP does not need a real chain. The contract interface is designed so it can be re-deployed to the production housegate registry chain unchanged |
| Sync semantics | **Async batch, 60 s polling cycle** | MVP measures throughput and cost, not latency floor. Batching amortizes PFB gas |
| Integrity | **blake3 hash per blob, recorded in the anchor** | Lets the rebuilder verify what it read from DA. Merkle commitment is post-MVP |

## 4. Architecture

Five components: source CH, publisher, Celestia testnet, anchor contract
on a local anvil EVM, rebuilder, target CH. The dataflow is purposefully
one-way — no feedback from rebuilder back to source.

```
┌────────────────┐                              ┌────────────────────────┐
│   Source CH    │                              │  Celestia Mocha-4 DA   │
│   (writes)     │                              └────────────┬───────────┘
└────────┬───────┘                                           ▲
         │ ① SELECT … FORMAT Parquet                         │ ③ PFB(blob)
         ▼                                                   │
   ┌─────────────┐                                           │
   │ da-publisher│───────────────────────────────────────────┤
   │  (sidecar)  │                                           │
   └─────┬───────┘                                           │ ⑤ GetBlob
         │ ④ Anchor.publish(commit, hash, seq, …)            │
         ▼                                                   │
   ┌──────────────────────┐                                  │
   │  anvil + DAAnchor.sol │◀── ⑥ queryFilter(Published) ────┤
   └──────────────────────┘                                  │
                                                  ┌──────────┴──────┐
                                                  │  da-rebuilder   │
                                                  │     (CLI)       │
                                                  └────────┬────────┘
                                                           │ ⑦ INSERT … FORMAT Parquet
                                                           ▼
                                                  ┌─────────────────┐
                                                  │    Target CH    │
                                                  │   (rebuild)     │
                                                  └─────────────────┘
```

Ordering invariant: the publisher writes the Celestia PFB **before** the
anchor `publish()` call, so any anchor visible on chain has a
corresponding blob already committed to DA. The rebuilder consumes
anchors in `partSeq` order and is therefore deterministic.

## 5. Components

### 5.1 `da-publisher`

Standalone binary built from `tools/da-mvp/cmd/da-publisher/main.go`,
using cobra for flag parsing (matching the style of the existing
`housegate secret-*` subcommands but as its own binary). Invocation:
`da-publisher --source-ch … …`. Flags / config:

```
--source-ch <DSN>              # tcp://host:9000?username=…&password=…
--database <name>              # source database
--table <name>                 # source table
--celestia-rpc <URL>           # local light-node RPC, e.g. http://localhost:26658
--celestia-token <token>       # auth token from light node
--celestia-namespace <hex>     # 10-byte namespace, see §6
--anchor-rpc <URL>             # anvil JSON-RPC
--anchor-contract <addr>       # deployed DAAnchor address
--anchor-private-key <hex>     # publisher signing key for anchor tx
--interval 60s                 # poll cycle
--checkpoint-file ./pub.state  # JSON file: { last_modification_time, last_part_seq }
--metrics-listen :9100         # Prometheus /metrics
```

Main loop (target: < 300 lines of Go):

```
checkpoint := load(checkpointFile)              // { lastModTime, lastPartSeq }
schemaHash := blake3(canonical(SHOW CREATE TABLE db.tbl))
for {
    parts := query(`
        SELECT name, modification_time, bytes_on_disk
        FROM system.parts
        WHERE database=? AND table=? AND active=1
              AND modification_time > ?
        ORDER BY modification_time, name`,
        database, table, checkpoint.LastModTime)

    for _, p := range parts {
        parquet := exportPart(sourceCH, database, table, p.Name)   // SELECT WHERE _part=… FORMAT Parquet
        hash := blake3(parquet)
        chunks := split(parquet, maxBlobSize)                       // see §6
        var partSeq uint64

        for i, c := range chunks {
            height, commitment := celestia.SubmitPayForBlob(namespace, c)
            if i == 0 {
                // First chunk allocates partSeq from the contract.
                partSeq, _ = anchor.publish(dbId, tableId,
                    height, commitment, len(c), hash, schemaHash,
                    uint8(0), uint8(len(chunks)))
            } else {
                // Subsequent chunks reuse the same partSeq.
                anchor.publishChunk(dbId, tableId, partSeq,
                    height, commitment, len(c), hash, schemaHash,
                    uint8(i), uint8(len(chunks)))
            }
        }

        checkpoint.LastModTime = p.ModificationTime
        save(checkpointFile, checkpoint)
    }
    sleep(interval)
}
```

**Prometheus metrics** (registered under `housegate_da_publisher_…`):

| Metric | Type | Notes |
|---|---|---|
| `parts_published_total` | counter | per part successfully anchored |
| `blobs_published_total` | counter | per Celestia PFB (chunks ≥ parts) |
| `bytes_published_total` | counter | uncompressed Parquet bytes |
| `publish_lag_seconds` | gauge | `now() − checkpoint.LastModTime` |
| `celestia_submit_seconds` | histogram | PFB latency |
| `anchor_submit_seconds` | histogram | anchor tx confirmation latency |
| `celestia_errors_total{kind}` | counter | submit errors by error class |

**Failure / restart**: linear backoff on Celestia or anvil errors (1 s →
30 s cap). Process crash resumes from `checkpoint.LastModTime` —
re-publishing a part already on DA is safe because `partSeq` is
allocated by the anchor contract, so the rebuilder always sees a
consistent monotonic sequence.

**One-publisher-per-table contract.** The MVP assumes a single
publisher process per `(database, table)`. Two publishers racing would
emit duplicate anchors with different `partSeq` values for the same
source data. Out of scope: lease / leader election.

### 5.2 `da-rebuilder`

Standalone binary built from `tools/da-mvp/cmd/da-rebuilder/main.go`.
Flags mirror the publisher:

```
--target-ch <DSN>
--database <name>
--table <name>
--celestia-rpc <URL>
--celestia-token <token>
--celestia-namespace <hex>
--anchor-rpc <URL>
--anchor-contract <addr>
--since-seq 0
--verify-after { count | sample | full }
```

Main loop:

```
expectedSchemaHash := blake3(canonical(SHOW CREATE TABLE db.tbl))
since := flag.SinceSeq
for {
    anchors := anchor.QueryFilterPublished(dbId, tableId, since)
    if len(anchors) == 0 { break }       // or sleep + poll if --follow

    groups := groupByPartSeq(anchors)    // chunks of one part are contiguous in seq?
                                         // No — they share partSeq but differ by chunkIdx.
                                         // Group by (partSeq) and verify chunkCount complete.

    for _, g := range groups in seq order {
        if g.SchemaHash != expectedSchemaHash {
            abort("schema drift detected at partSeq=%d", g.PartSeq)
        }
        blob := assemble(fetchChunks(g))                 // one Celestia GetBlob per chunk
        if blake3(blob) != g.BlobHash { abort("blob hash mismatch") }
        exec(targetCH, "INSERT INTO db.tbl FORMAT Parquet", blob)
        since = g.PartSeq + 1
    }
}
verify(targetCH, sourceCH, mode=flag.VerifyAfter)
```

**Verification modes** (post-rebuild correctness check):

- `count`: `SELECT count() FROM db.tbl` matches on both sides.
- `sample`: `SELECT * FROM db.tbl ORDER BY <pk> LIMIT 1000 OFFSET 0/halfway/end` matches.
- `full`: `SELECT cityHash64(toString(t)) FROM db.tbl ORDER BY <pk>` aggregated — full row-level hash.

`full` only feasible on small tables; `sample` is the default.

### 5.3 `DAAnchor.sol`

Single contract, ~50 lines including imports:

```solidity
// SPDX-License-Identifier: MIT
pragma solidity ^0.8.20;

contract DAAnchor {
    event Published(
        bytes32 indexed dbId,
        bytes32 indexed tableId,
        uint64  partSeq,
        uint64  daHeight,
        bytes32 daCommitment,
        uint32  blobSize,
        bytes32 blobHash,
        bytes32 schemaHash,
        uint8   chunkIdx,
        uint8   chunkCount
    );

    // Per-(dbId, tableId) monotonic sequence allocator.
    mapping(bytes32 => uint64) private _nextSeq;

    function publish(
        bytes32 dbId,
        bytes32 tableId,
        uint64  daHeight,
        bytes32 daCommitment,
        uint32  blobSize,
        bytes32 blobHash,
        bytes32 schemaHash,
        uint8   chunkIdx,
        uint8   chunkCount
    ) external returns (uint64 seq) {
        bytes32 key = keccak256(abi.encode(dbId, tableId));
        // Chunks of the same part share partSeq; only the first chunk increments.
        if (chunkIdx == 0) {
            seq = _nextSeq[key]++;
        } else {
            // Caller must pass the seq it received from chunkIdx=0.
            // For MVP simplicity we expose this via a separate helper:
            revert("MVP: use publishChunk(seq, …) for chunkIdx > 0");
        }
        emit Published(
            dbId, tableId, seq, daHeight, daCommitment,
            blobSize, blobHash, schemaHash, chunkIdx, chunkCount
        );
    }

    function publishChunk(
        bytes32 dbId, bytes32 tableId, uint64 seq,
        uint64 daHeight, bytes32 daCommitment,
        uint32 blobSize, bytes32 blobHash, bytes32 schemaHash,
        uint8 chunkIdx, uint8 chunkCount
    ) external {
        emit Published(
            dbId, tableId, seq, daHeight, daCommitment,
            blobSize, blobHash, schemaHash, chunkIdx, chunkCount
        );
    }
}
```

Deployment artifact: a Foundry / Hardhat project under
`tools/da-mvp/contracts/`. The compiled ABI is checked in as JSON so the
Go publisher / rebuilder can load it without a build dependency on
solc.

No access control in the MVP — anvil is local, and the production
version will use the housegate registry's existing auth.

### 5.4 Schema handling (minimum viable)

Both publisher and rebuilder compute
`schemaHash = blake3(canonicalize(SHOW CREATE TABLE db.tbl))` at
startup. `canonicalize` strips whitespace, comments, and engine settings
that don't affect column layout (`storage_policy`, `ttl`, etc.) — only
the column list and types are hashed.

- Publisher: if `schemaHash` changes mid-run (rare — would require
  online ALTER), publisher aborts and demands a fresh checkpoint.
- Rebuilder: if `schemaHash` in an anchor differs from local
  `expectedSchemaHash`, rebuilder aborts at that anchor and reports the
  partSeq.

DDL replay across schema changes is **post-MVP**; the MVP can only
rebuild over a schema-stable window.

## 6. DA layer specifics — Celestia Mocha-4

- **Single PFB blob upper bound:** ~1.97 MiB. MVP targets **1.5 MiB**
  per chunk to leave headroom for header overhead.
- **Block time:** ~12 seconds.
- **Block capacity:** ~1.4 MiB on mainnet (Mocha-4 testnet is more
  permissive but the mainnet number is what we extrapolate against).
- **Network shared throughput:** ~117 KB/s aggregate on mainnet —
  shared across **all** Celestia users. This is the headline number
  that throughput Experiment A will test against.
- **Namespace:** 10 bytes. Allocation scheme for MVP:
  `0x68676d76 || <6 bytes of blake3(dbId||tableId)>` — `0x68676d76` is
  ASCII `"hgmv"` (housegate MVP).
- **Light node:** `celestia light start --p2p.network mocha --core.ip
  <consensus RPC> --rpc.skip-auth=false`. Token retrieved from
  `~/.celestia-light-mocha-4/keys/auth.token` after first start.
- **Go SDK:** `github.com/celestiaorg/celestia-node/api/rpc/client` —
  `Blob.Submit(ctx, []*blob.Blob{…}, …)` returns `(height uint64, error)`.
  `Blob.Get(ctx, height, namespace, commitment)` returns the blob.

## 7. Measurement plan — the actual MVP deliverable

The point of building this prototype is to populate three tables. Each
experiment runs ≥ 2 hours per data point to smooth out block-time noise
and gas-price variance.

### 7.1 Experiment A — Throughput

Goal: find the steady-state INSERT rate at which `publish_lag_seconds`
stops growing.

- Workload: a `loadgen` tool emits `INSERT INTO db.tbl VALUES (…)` to
  the source CH at rates `{100, 1k, 10k, 100k}` rows/s. Row schema is
  representative of a typical indexer table: ~12 columns mixing
  `String`, `UInt64`, `Decimal128`, `DateTime64`.
- Measure:
  - `publish_lag_seconds` over time at each rate.
  - `bytes_published_total` derivative → MB/s.
  - Celestia block occupancy: (our blob bytes) / (block capacity).
- Output: rate-vs-lag chart, MB/s-vs-rate chart, % of Mocha block
  capacity at each rate.

### 7.2 Experiment B — Latency

Goal: end-to-end latency from source `INSERT` visible to target
`SELECT` visible.

- Inject rows with a `inject_ts DateTime64(6)` column = wall clock at
  insert time. Run rebuilder in `--follow` mode.
- Measure on target: `now64() - inject_ts` median / P50 / P95 / P99.
- Decompose by stage:
  - source CH part flush delay
  - publisher poll cycle
  - Celestia commit (≈ 12 s + finality)
  - rebuilder fetch + INSERT
- Output: stacked latency-breakdown chart.

### 7.3 Experiment C — Cost

Goal: $/GB of published data, extrapolated from Mocha testnet TIA
consumption to mainnet TIA pricing.

- Publish 100 GB of representative data over 24+ hours.
- Tally: testnet TIA spent, anvil gas spent (irrelevant for cost but
  shows tx overhead).
- Extrapolate: mainnet TIA spot price × tally → $/GB.
- Compare against:
  - S3: $0.023/GB-month + PUT cost.
  - Hypothetical Foundation-run Keeper cluster: $X/month estimated
    operator cost / total network GB.

### 7.4 Output artifact

A follow-up document
`docs/superpowers/specs/2026-XX-XX-da-mvp-report.md` containing the
three experiments' tables and a clear go/no-go recommendation
referencing §8 below.

## 8. Go / no-go decision tree

The decisions taken in the follow-up report will be guided by:

```
Q1: At a typical indexer load (e.g. 100k rows/s sustained),
    does publish_lag_seconds stabilize?
  ├── yes → Q2
  └── no  → DA cannot be the data plane.
           ├── Fallback A: DA serves only L4 commitments (Merkle roots).
           └── Fallback B: Drop DA entirely; pursue Keeper + RMT
                          (see prior reliability spec, this becomes the L1
                          replacement).

Q2: Is extrapolated mainnet $/GB within 10× of S3 ($0.023/GB-month)?
  ├── yes → Q3
  └── no  → same fallbacks as Q1.

Q3: Is P99 end-to-end latency under 5 minutes?
  ├── yes → DA is a candidate primary data plane.
  └── no  → DA is suitable as commitment + hot-DB full publish only;
           cold DBs stay on Keeper+RMT or the existing S3 path.
```

A single negative on any of Q1/Q2/Q3 funnels into a specific fallback.
The MVP cannot answer all questions a production deployment would —
this tree is intentionally narrow.

## 9. Implementation timeline (2 weeks, 1 engineer)

| Day | Task | Deliverable |
|---|---|---|
| 1 | Celestia light node up; PFB submit/get demo in Go | demo binary commits a blob and reads it back |
| 2–3 | `da-publisher` main loop + checkpoint | single-part publish end-to-end |
| 4 | blob chunking + anchor `publishChunk` wiring | parts > 1.5 MiB split correctly |
| 5–6 | `da-rebuilder` | one table fully rebuilt on target |
| 7 | `DAAnchor.sol`, Foundry project, anvil integration in CI | ABI JSON checked in; integration test green |
| 8–9 | `loadgen` + Experiment A | throughput table |
| 10 | Experiment B + Experiment C | latency + cost tables |
| 11–12 | report writing + go/no-go proposal | follow-up spec committed |

Day budget is tight; the explicit out-of-scope items in §2 are how this
fits in two weeks.

## 10. Beyond the MVP (not in scope; signposted)

If the go/no-go is positive (Q1+Q2+Q3 all yes), the production path
extends along the following axes. None of these are blocking the MVP.

- **Publisher embedding in housegate.** Move publisher into a
  `daPublisher` plugin that hooks the Data-block stream after rewrite.
  Removes the polling cycle; achievable latency floor drops to "as soon
  as the part is sealed."
- **Multi-indexer coordination.** Decide whether each indexer publishes
  independently (wasteful but trustless), or whether one elected
  publisher publishes per (database, table) with the others verifying.
- **DDL replay.** Anchor schema-change events alongside Published
  events; rebuilder applies DDL on the target before resuming data
  replay.
- **Fraud-proof / Merkle commitments.** Add a per-blob Merkle root over
  rows; allow cheap challenges against incorrect rebuilder output.
- **Staking / slashing for publishers.** Bond publishers; slash on
  proven misbehavior.
- **Anchor contract migration.** Re-deploy `DAAnchor.sol` to the
  production housegate registry chain; integrate with `commitgate` so
  the proxy can refuse writes whose anchors fall too far behind.
- **DA selection.** If Mocha numbers disqualify Celestia, re-run the
  same harness against EigenDA and Avail. The MVP harness is
  intentionally DA-agnostic at the interface level.

## 11. Alternatives considered (and why not, for the MVP)

- **Embed publisher as a housegate plugin from day 1.** Tighter latency
  but couples DA validation to plugin-chain churn. MVP measures DA, not
  integration; sidecar wins.
- **Publish raw INSERT SQL strings instead of Parquet.** Smaller blobs
  for INSERT-heavy workloads, but non-deterministic functions
  (`now()`, `generateUUIDv4()`) make replay incorrect. Parquet captures
  *materialized* row state and is replay-safe.
- **Publish raw CH part files (.bin / .mrk / etc.).** Most efficient
  storage-wise but ties replay to specific CH versions and engine
  internals; brittle for a measurement rig.
- **Use Avail or EigenDA in the MVP instead of Celestia.** Both are
  viable; Celestia is the most mature OSS path with the lowest setup
  cost. Comparing DA layers is post-MVP.
- **Deploy the anchor contract directly on a public testnet.** Doable,
  but anvil is faster, deterministic, and isolates the experiment from
  public-testnet flakiness.
- **Encrypt blobs with operator-held keys.** Adds key-management
  complexity. Indexer data is assumed public (§2, §10 Q1). Confirm
  with the team before this assumption is locked.

## 12. Open questions

1. **Data confidentiality.** Celestia blobs are public — anyone can
   read them. housegate indexes on-chain data, which is itself public,
   so this should be fine — but it should be **explicitly** confirmed
   before the MVP starts. If even one private dataset is in scope, the
   MVP needs an AEAD wrapper layer.
2. **DA layer comparison cadence.** After Mocha-4 numbers land, do we
   re-run the MVP against EigenDA and Avail? Recommendation: only if
   the Mocha-4 result is "close to viable but not quite" — running all
   three is a week of additional work.
3. **Single-publisher assumption.** The MVP runs one publisher per
   table. Production likely needs N indexers all able to publish (for
   independence) — but then duplicate blobs balloon the cost
   measurement. The Experiment C cost number will need to be re-stated
   per-publisher and per-network-total once that model is chosen.
4. **Anchor contract gas under sustained load.** Anvil is free, but on
   the production chain each anchor `publish()` costs gas. Estimating
   that is mechanical and post-MVP, but the MVP should record the
   anchor tx count so the extrapolation is clean.

---

**Decision-driving question for the team:** Once §7 produces numbers,
which fallback (Fallback A: commitment-only DA; Fallback B: Keeper +
RMT) becomes the default if any of Q1/Q2/Q3 is "no"? Answering this
**before** the MVP completes saves a round-trip and lets the report's
recommendation land cleanly.
