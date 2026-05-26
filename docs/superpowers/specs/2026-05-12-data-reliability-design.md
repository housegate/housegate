# Data Reliability for Single-Instance Indexers — Design Spec

**Date:** 2026-05-12
**Status:** Proposed
**Authors:** poetry, Claude

## 1. Goal

In the decentralized indexer network, each indexer node runs exactly **one
housegate + one ClickHouse instance**. There is no in-process replication,
no `ReplicatedMergeTree` quorum, no failover replica. When the host machine
dies, the disk is lost, or the indexer goes offline for any other reason,
the user's indexed data must remain **durable** (recoverable bit-for-bit)
and the network must remain **available** (queries that reference that
data can still be served, within a bounded recovery window).

This spec defines the layered architecture that delivers those two
properties without introducing a centralized SPOF (such as a Kafka-style
write-ahead log) and without requiring operators to coordinate quorum
membership across organizational boundaries.

The core idea is to push **durability into S3-compatible object storage**
(per-operator buckets, owned and paid for by the operator running the
indexer), and to push **ownership / handoff coordination onto the
existing on-chain registry** that housegate already trusts for permissions
and DDL. No new MQ, no new consensus system.

## 2. Non-goals

- **Not building a write-ahead log or message queue.** Object storage is
  itself a durable log; layering an MQ on top is redundant and reintroduces
  the SPOF this design exists to eliminate.
- **Not changing the proxy's stateless contract.** housegate continues to
  persist nothing locally. All durability shifts to the CH instance's
  storage policy and to the on-chain registry.
- **Not requiring cross-operator ClickHouse Keeper quorums.** Operators in
  a decentralized network do not share trust boundaries with each other;
  running a single Keeper cluster across operators contradicts the network
  model. This design uses object storage and on-chain coordination instead.
- **Not adding Byzantine-fault tolerance against malicious operators in
  this iteration.** Per-part hash manifests (L4) lay the groundwork, but
  fraud-proof verification and slashing are explicitly future work.
- **Not replacing existing per-bucket CH replication features.** Operators
  who want stronger local HA may run `ReplicatedMergeTree` with their own
  Keeper inside their boundary; that is orthogonal to this spec and
  unchanged by it.

## 3. Background: what data lives where today

Before designing for reliability, we must be precise about what is at
risk. A survey of the current code yields:

| Data | Location | Survives indexer host loss? |
|---|---|---|
| User MergeTree tables (the indexed data) | Local disk on the CH instance | **No** — this is the gap. |
| Schema (CREATE TABLE / DATABASE) | Local CH metadata + on-chain DDL records via commitgate | Partially — chain has the DDL events; local CH metadata is lost. |
| Permissions / ownership | On-chain, mirrored to Redis via `statemirror` | Yes. |
| Indexer connection info (address, port, ID) | On-chain, mirrored to Redis | Yes. |
| housegate session state | Process memory | N/A — proxy is stateless by design. |
| Usage / billing | Delegated to sentio-node RPC | Yes — external system. |
| Concurrency limiter state | Redis sorted sets | Ephemeral by design. |

**The single gap is the user's MergeTree data on local disk.** Every other
class of state already has a durability story. This spec is therefore
narrowly scoped: keep the user data safe and reachable.

## 4. The four-layer model

Reliability is not one property; it is several, and conflating them
produces muddled designs. We separate concerns into four layers, each
solved by an independent mechanism:

```
┌─────────────────────────────────────────────────────────────┐
│ L4  Integrity     │  Periodic part-hash manifests on-chain   │
├─────────────────────────────────────────────────────────────┤
│ L3  Metadata      │  On-chain registry: db → indexer →       │
│                   │  bucket URI → signing pubkey             │
├─────────────────────────────────────────────────────────────┤
│ L2  Availability  │  Bucket-takeover protocol +              │
│                   │  cross-indexer read fallback             │
├─────────────────────────────────────────────────────────────┤
│ L1  Durability    │  S3-backed MergeTree; local disk is      │
│                   │  cache only                              │
└─────────────────────────────────────────────────────────────┘
```

The layers are independent: L1 alone gives durability against host loss;
L1+L3 gives durability + a way to find the data again; L1+L2+L3 gives
bounded-RTO availability; L4 adds tamper-evidence on top. Operators can
adopt L1 in isolation and benefit immediately; later layers compose on
top without re-architecting.

## 5. Layer L1 — Durability via S3-backed MergeTree

### 5.1 Mechanism

ClickHouse has supported S3-backed `MergeTree` since 21.x. Configure a
`storage_policy` with an `s3` disk type and tables created against that
policy store parts in the configured bucket. Local disk becomes a read
cache. fsync-on-insert and `cache_on_write` semantics are configurable.

Each indexer operator provisions their own S3-compatible bucket. The
bucket may be AWS S3, MinIO running on operator hardware, Backblaze B2,
Wasabi, Cloudflare R2, or any S3-API-compatible store. Credentials never
leave the operator's environment; housegate is not aware of bucket
credentials.

### 5.2 Indexer-level configuration

ClickHouse storage policy lives in `config.xml` (the CH instance's own
config, not housegate's). A representative configuration:

```xml
<storage_configuration>
  <disks>
    <s3_main>
      <type>s3</type>
      <endpoint>https://s3.us-east-1.amazonaws.com/housegate-indexer-7/</endpoint>
      <access_key_id from_env="HG_S3_ACCESS_KEY"/>
      <secret_access_key from_env="HG_S3_SECRET_KEY"/>
      <cache_enabled>true</cache_enabled>
      <cache_path>/var/lib/clickhouse/s3_cache/</cache_path>
      <cache_size>200Gi</cache_size>
    </s3_main>
  </disks>
  <policies>
    <s3_main_policy>
      <volumes><main><disk>s3_main</disk></main></volumes>
    </s3_main_policy>
  </policies>
</storage_configuration>
```

Tables created by users are stamped with `SETTINGS storage_policy =
's3_main_policy'`. The rewriter does **not** need to know about storage
policy; this is purely a CH-side concern. The only housegate change is
that the commitgate observer, when validating a `CREATE TABLE`, may
optionally require the storage policy clause to be present (enforced by
DDL inspection at the rewriter layer — discussed in §8).

### 5.3 Performance considerations

S3-backed MergeTree adds latency on cold cache reads. Mitigation:
- Configure `cache_size` to fit the working set; reads stay local.
- `cache_on_write = true` so freshly-inserted parts are immediately
  cached locally and the first read after INSERT is hot.
- Heavy analytics scans across cold data pay S3 GET latency. This is
  acceptable for the indexed-blockchain-data use case where access
  patterns are heavily skewed to recent data.

### 5.4 Cost model

Object storage costs the operator: storage GB-month + PUT/GET requests.
For typical indexer workloads (write-heavy, append-mostly MergeTree with
batched parts) the GET cost is dominated by cache misses on cold reads
and is bounded by `cache_size`. PUT cost scales with merge granularity;
operators tune `min_bytes_for_wide_part` and merge settings on their own
CH instance. None of this is housegate's concern.

### 5.5 What L1 buys

After L1 alone:
- Host machine dies → all user data parts remain in the operator's
  bucket. Operator spins up a new CH instance pointing at the same
  bucket, attaches existing parts, and resumes service. No data loss.
- The bucket itself is the durability boundary; bucket-level durability
  (eleven-nines on AWS S3, configurable on MinIO via erasure coding) is
  delegated to the storage provider.

L1 does **not** give availability — there is still a recovery window
during which the indexer is offline. That is L2.

## 6. Layer L2 — Availability via bucket-takeover protocol

### 6.1 Goal

When an indexer is offline (host down, network partition, operator
maintenance), queries targeting databases hosted on that indexer must
either (a) be served by a different indexer that has temporarily attached
the same bucket, or (b) fail with a clear "in recovery" signal rather
than silently mis-routing. The recovery window should be bounded; the
target RTO is on the order of minutes, not hours.

### 6.2 Two flavors of takeover

We distinguish two cases. The protocol is the same; the trigger differs.

**Operator-initiated migration (planned, common).** The owning operator
wants to move their indexer to new hardware, rotate keys, or change
hosting provider. They sign a `transfer(database_id, new_indexer_id,
new_bucket_uri)` transaction. The on-chain registry updates the mapping
atomically; statemirror propagates the new pointer; housegate
network-state lookups now resolve queries to the new indexer. The old
indexer can be torn down once Redis converges.

**Network-initiated takeover (unplanned, rare).** The owning operator is
unresponsive for longer than a configured liveness threshold. A `claim`
transaction allows the network to reassign the database to a standby
indexer. The claim must be authorized by either (a) the original
operator's pre-signed delegation (warm standby), or (b) network
governance (a multisig, a slashing condition, etc. — out of scope for
this spec; the contract interface must support it but the policy is set
by the deploying network).

In both cases, the bucket itself must be **accessible to the new
indexer**. Two sub-patterns:

- **Shared-bucket pattern.** The operator pre-grants read access on
  their bucket to a designated standby operator's IAM identity. The
  standby attaches as a read-only replica when claim fires. Suitable for
  warm-standby pairs within a single operator's organization.
- **Bucket-snapshot pattern.** The original bucket remains the system of
  record. Standby indexers maintain an asynchronous mirror (S3
  replication, `aws s3 sync`, MinIO bucket replication). On claim, the
  standby promotes its mirror to primary and re-registers its own
  bucket URI on-chain. Suitable across operator boundaries.

The choice is per-deployment; the on-chain protocol is the same.

### 6.3 On-chain registry extensions

The existing on-chain DB registry (already used by commitgate /
statemirror) is extended with:

```
Database {
  id:              DatabaseID
  owner:           Address           // existing
  indexer_id:      IndexerID         // existing
  bucket_uri:      string            // NEW: s3://… or s3-compatible URI
  signing_pubkey:  bytes             // NEW: which key may sign data
                                     //      manifests for this DB
  standby_indexers: [IndexerID]      // NEW: pre-authorized takeover set
  takeover_policy:  enum             // NEW: { OwnerOnly,
                                     //        StandbySigned,
                                     //        Governance }
  generation:      uint64            // NEW: increments on each takeover
                                     //      to fence stale writers
}

Operations:
  transfer(db_id, new_indexer_id, new_bucket_uri) -> signed by owner
  claim   (db_id, new_indexer_id, new_bucket_uri) -> signed per policy
  heartbeat(indexer_id) -> signed by indexer (periodic liveness)
```

`generation` is the fencing mechanism. The new indexer increments
generation on takeover; housegate's commitgate observer rejects any
write attempt from an indexer whose locally-cached generation is stale
relative to the chain. This prevents a "zombie" indexer that briefly
recovered network connectivity from corrupting data the takeover
already committed elsewhere.

### 6.4 housegate-side changes

The rewriter and forward plugins already resolve `(database) →
(indexer_id, address)` via NetworkState. Two changes:

1. **Add `bucket_uri` and `generation` to the resolved indexer info.**
   The rewriter consumes `bucket_uri` only as opaque metadata (used by
   L4 manifest publication); proxies do not access buckets directly.
   `generation` is used by commitgate to fence stale writers.
2. **Surface a "in-recovery" status.** When NetworkState reports an
   indexer with stale heartbeat and no claim yet, queries to its
   databases fail with `Code: 999. DB::Exception: housegate: database
   '<name>' in recovery, please retry`. This is preferable to silently
   timing out at the TCP layer.

The takeover transaction itself is initiated by operators or governance
tooling, not by housegate. housegate is a consumer of the registry, not
a writer.

### 6.5 Optional: cross-indexer read fallback

For deployments that want better read availability than RTO-bounded
takeover, the standby pattern can keep a hot read-replica:

- Operator A's housegate, on schedule, exports recent parts to a shared
  bucket prefix readable by operator B's indexer.
- Operator B's CH attaches those parts as read-only (`ATTACH PART`).
- The on-chain registry advertises operator B's indexer as a read-fallback
  for that database (new `read_replicas: [IndexerID]` field).
- The rewriter's existing cross-indexer routing already supports remote
  reads; it gains a fallback list to try when the primary is offline.

This is **optional** and adds steady-state cost (storage duplication +
network egress); it is appropriate only for high-value databases. The
core spec does not require it; it lists it as a supported extension.

## 7. Layer L3 — Metadata via on-chain registry

This is mostly already built (commitgate, statemirror, NetworkState).
This spec extends the schema with the fields listed in §6.3 and adds:

- **DDL idempotency on takeover.** When a new indexer attaches a bucket
  with existing parts, the schema must already exist or be re-applied.
  CH metadata files (`.sql` files in `metadata/`) are part of the bucket
  if `storage_policy` covers metadata too; otherwise, the on-chain DDL
  history is replayed. The commitgate observer already records DDL
  events on-chain; the takeover handler reads those events in order and
  replays them against the fresh CH instance before opening the indexer
  to traffic.
- **Permission re-derivation.** Permissions are already on-chain; no
  change needed. The new indexer reads the same statemirror Redis and
  enforces the same bitmaps.

## 8. Layer L4 — Integrity via part-hash manifests

### 8.1 Goal

In a decentralized network, an operator could in principle corrupt parts
in their own bucket and serve falsified query results. L4 makes such
corruption detectable by **committing periodic content hashes of parts
to chain**, which any party can later verify against the bucket
contents.

This is groundwork; full fraud-proof verification and economic slashing
are out of scope for this spec.

### 8.2 Mechanism

A new housegate plugin (`manifest`) runs server-mode-only. Periodically
(e.g., every N minutes or every M committed merges, configurable):

1. Query the local CH `system.parts` table for `(database, table,
   part_name, hash, bytes_on_disk, modification_time)` since the last
   manifest.
2. Build a Merkle root over the new parts (`merkle(sorted by part_name,
   leaf = blake3(part_name || part_hash))`).
3. Sign the Merkle root with the indexer's signing key (the one
   registered in the on-chain registry per §6.3).
4. Submit a `commit_manifest(database_id, generation, since_seq,
   to_seq, merkle_root, sig)` transaction.

The Merkle root and per-leaf manifest contents are also published to the
bucket under a `_manifests/` prefix so verifiers do not have to scan all
parts to recompute the root.

### 8.3 Verifier role

A verifier (a network watchdog, a user-side check, or governance
tooling) can:
- Read the bucket's `_manifests/` prefix and the chain's `commit_manifest`
  events.
- Recompute the Merkle root and confirm it matches the chain commitment.
- Spot-check individual parts by fetching them and recomputing leaf
  hashes.

If a manifest fails verification, the verifier raises an alert. The
slashing policy (if any) is governance-defined and orthogonal to this
spec.

### 8.4 Cost

Manifest commits are infrequent (per-minute granularity or coarser) and
the on-chain payload is small (a hash + sig). Chain cost is
operator-paid and bounded.

## 9. Failure mode walkthrough

To validate the design, trace each failure scenario end-to-end.

### 9.1 indexer host disk fails

- L1: parts are in S3, intact.
- Operator spins up a new CH instance on new hardware, points
  `storage_configuration` at the same bucket, ATTACHes existing parts.
- Operator submits `transfer(db_id, new_indexer_id, same_bucket_uri)`
  (only `indexer_id` changes; bucket stays).
- statemirror propagates new indexer address; rewriter re-resolves on
  next query. RTO ≈ time to provision new host + propagate state.

### 9.2 indexer network-partitioned for hours

- L2 heartbeat threshold elapses; chain shows stale heartbeat.
- Network-initiated `claim` per policy; standby attaches via either
  shared-bucket or snapshot pattern.
- New indexer increments generation. If old indexer recovers and tries
  to write, commitgate observer reads generation from chain, finds
  local generation stale, rejects writes with a "fenced" error.
- Eventual reconciliation: old indexer's bucket is either abandoned
  (snapshot pattern) or reattached as a read-only mirror (shared
  pattern). Manual cleanup; spec does not automate reconciliation.

### 9.3 operator-initiated graceful migration

- Operator submits `transfer` with downtime window.
- statemirror converges; rewriter routes to new indexer.
- Old indexer is torn down after a drain period (configurable; default
  60s sliding window for in-flight queries to complete).

### 9.4 transient indexer restart (most common)

- housegate stops, CH stops, both restart in seconds.
- No takeover triggered (heartbeat threshold not exceeded).
- Existing client connections see TCP RST and reconnect.
- L1 ensures no part is lost across restart; CH replays its own WAL on
  startup as it always does.

### 9.5 silent bucket corruption (Byzantine operator)

- L4 manifest commits make corruption detectable.
- Verifier identifies divergence; raises alert.
- Network governance handles slashing / reassignment per policy
  (out-of-spec).

## 10. Implementation plan

This spec sequences into four roughly-independent milestones. Each
milestone delivers a usable property on its own.

### Milestone M1: L1 enablement

- Document operator runbook for configuring `storage_policy` with
  S3/MinIO on their CH instance.
- Optionally: a commitgate observer that requires `CREATE TABLE` to
  carry `SETTINGS storage_policy = 's3_main_policy'` (or whatever policy
  name the deployment standardizes on). This is enforced post-rewrite
  on `qctx.RawSQL` inspection.
- No proxy code changes are strictly required for M1; CH does all the
  work. The optional commitgate observer is a small addition.
- **Deliverable:** operators can self-host MergeTree on S3; data
  survives host loss.

### Milestone M2: L3 registry schema extensions

- Extend on-chain Database struct with `bucket_uri`, `signing_pubkey`,
  `standby_indexers`, `takeover_policy`, `generation`.
- Extend statemirror to surface new fields.
- Extend NetworkState consumers (rewriter, forward plugin) to consume
  `generation` for fencing.
- Add commitgate observer that rejects writes when local generation is
  stale.
- **Deliverable:** the protocol surface for takeover exists, even if
  takeover is still manual.

### Milestone M3: L2 takeover protocol

- Implement `transfer` / `claim` / `heartbeat` on-chain entry points
  (contract work, outside housegate repo).
- Implement housegate-side "in-recovery" error surface.
- Implement standby-indexer DDL replay tooling (a CLI or a startup hook
  that reads chain DDL history and applies it to fresh CH).
- **Deliverable:** indexer failover with bounded RTO.

### Milestone M4: L4 manifests

- Add `manifest` plugin (server-mode only).
- Define on-chain `commit_manifest` entry point.
- Document verifier tooling.
- **Deliverable:** tamper-evidence; basis for future fraud-proof work.

## 11. Open questions

These are items I'd like a second pass on before locking the design:

1. **Bucket credential rotation.** Operators rotate S3 keys periodically.
   CH supports `from_env` for keys; reload semantics on rotation need a
   CH restart in current versions. Acceptable, or do we need
   hot-rotation?
2. **DDL replay determinism.** Replaying DDL from chain to a fresh CH
   instance assumes ordered, idempotent application. Are there CH DDL
   operations in current use that are *not* idempotent under
   `IF NOT EXISTS` / `IF EXISTS` rewriting? (Most are; ALTER TABLE on
   non-existent column comes to mind.)
3. **Manifest cadence.** Per-minute is a starting point; needs benchmark
   to confirm that part-count growth doesn't make manifest size
   prohibitive. May need streaming Merkle vs. snapshot Merkle.
4. **Read-fallback ownership.** If operator B serves a read query for
   operator A's data, who pays for the S3 GET egress? Probably operator
   B charges back via the existing usage/billing path, but the policy
   needs explicit specification.
5. **Bucket-region failure.** S3 region-wide outage is rare but real.
   Should the design encourage multi-region bucket replication at the
   storage layer, or is single-region acceptable for v1?

## 12. Out of scope (explicit future work)

- Fraud-proof verification protocol and slashing policy (L4 builds the
  substrate; the game-theoretic layer is a separate spec).
- Cross-operator ClickHouse Keeper quorums (architectural mismatch with
  decentralization, as discussed in §2).
- Automatic reconciliation of forked buckets when a fenced indexer
  recovers (manual operator action per §9.2).
- housegate-keeper-as-MQ: this design replaces it; not a follow-up.
- Encrypted-at-rest parts with operator-held keys (CH supports this
  independently; orthogonal to reliability).

## 13. Why not the alternatives

For completeness, the three options the prompt considered, with the
reason each is rejected as the primary mechanism:

- **housegate-keeper as MQ.** A central MQ becomes a SPOF; making it HA
  re-creates the consensus problem the decentralized model exists to
  avoid. Object storage is already a durable log; an additional MQ on
  top is redundant.
- **On-chain data synchronization.** Chain throughput is 3-4 orders of
  magnitude below CH write throughput. Suitable for metadata and
  manifests (and this spec uses it for both), unsuitable for data plane.
- **S3 / MinIO alone, no L2/L3/L4.** Durability without availability is
  insufficient; without a coordination layer, a dead indexer's data is
  safe but inaccessible. L2 + L3 close that gap.

The accepted design is **S3 for durability + chain for coordination**,
which uses each substrate for what it is good at.
