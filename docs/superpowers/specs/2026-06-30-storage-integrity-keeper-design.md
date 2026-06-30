# Sentio Sequencer — Design Spec (Go coordinator for the storage integrity layer)

**Date:** 2026-06-30 **Status:** Proposed **Source of truth:** English version; regenerate the Chinese version from this file when changing protocol semantics. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.2 + §12 + §15 Q15, which define the role's responsibilities but not its architecture. This spec fills that gap. **Depends on:** [`pkg/replay`](../../../pkg/replay) (verifier core + manifest/receipt/attestation types), [`pkg/lthash`](../../../pkg/lthash), [`pkg/auth`](../../../pkg/auth) (secp256k1 relay signing).

**Naming note.** The storage design's §5.2 originally called this role "Keeper". This spec renames it **Sentio Sequencer**, because (a) its core trust-root function is `statement_seq` assignment / L3 sequencing, (b) the rollup-sequencer framing matches its actual shape (sequencing + attestation aggregation + publication), and (c) it removes a persistent confusion with the unrelated **ClickHouse Keeper** ([§1.1](#11-clickhouse-keeper-not-us-not-go)). The storage design's text still reads "Keeper" in places; both refer to the same role specified here.

This is a **design + Go API spec**, not an implementation. It freezes the component boundaries, the state model, the signing plan, and the package map so that the P0/P1 implementation in [§11](#11-delivery-phases) can proceed against a fixed target. No production Go code is written in this document.

---

## 1. Positioning and terminology

### 1.1 ClickHouse Keeper (not us, not Go)

**ClickHouse Keeper** is the C++ ZooKeeper-compatible coordination service that ships *inside the ClickHouse repo* ([`ClickHouse/ClickHouse`](https://github.com/ClickHouse/ClickHouse), under `programs/keeper/` and `src/Coordination/`). It is the Raft-based replacement for Apache ZooKeeper that ClickHouse uses to coordinate `ReplicatedMergeTree` replication. In this design's [§12.1](2026-06-22-storage-integrity-design.md) engine split, ClickHouse Keeper backs the `hg_unsafe` ReplicatedMergeTree tables and nothing else.

The Sentio Sequencer has **no code relationship and no protocol relationship** with ClickHouse Keeper:

- We do **not** fork, patch, embed, or reimplement it. ClickHouse Keeper remains an external C++ dependency, deployed as-is.
- We do **not** speak the ZooKeeper wire protocol from Go. ClickHouse Keeper is treated as an opaque coordination service that ClickHouse talks to; the integrity layer never reads its znodes as the source of truth for safe-state semantics.
- The [`pkg/replicationproxy`](../../../pkg/replicationproxy) `KeeperServer` in this repo is a **L4 TCP passthrough** that optionally forwards the ClickHouse↔Keeper connection for network isolation (so a ClickHouse instance only opens TCP to its co-located HouseGate). It moves bytes; it does not interpret them, and it is explicitly **not an integrity gate** ([storage design §16](2026-06-22-storage-integrity-design.md)). That component is about ClickHouse Keeper, not about the Sequencer.

There is therefore no "rewrite clickhouse-keeper in Go" project here. The C++ code stays where it is. The name "Sentio Sequencer" was chosen in part to keep this boundary obvious: a sequencer sequences L3 blocks, ClickHouse Keeper coordinates ReplicatedMergeTree, and never the two shall be conflated.

### 1.2 Sentio Sequencer (us, Go)

The **Sentio Sequencer** is the integrity layer's own component — a **rollup sequencer + attestation aggregator + safe-state publisher**. Its job, inherited verbatim from [storage design §5.2](2026-06-22-storage-integrity-design.md), is to be the protocol's sequencer, validator, registry, replay orchestrator, attestation collector, safe-state publisher, and promotion issuer. "Sequencer" names the role's trust-root function (`statement_seq` assignment, L3 block construction, ordering), with the understanding that — as with rollup sequencers generally — it also owns the adjacent attestation/publication machinery that turns ordering into finalized safe state.

We implement it in **Go**, for three concrete reasons that are not matters of taste:

1. **It stands on [`pkg/replay`](../../../pkg/replay).** The verifier core (`replay.Verifier`), the manifest/receipt/attestation types (`SafeSnapshotManifest`, `ExecutionReceipt`, `ReplayAttestation`, `ReplayJob`), and the in-process executor (`pkg/replay/payloadexec`) are already Go and already define the integrity layer's data model. A Sequencer that consumed them from another language would pay a cross-language boundary tax for no benefit. See [§4](#4-state-model) for the reuse map.
2. **It shares the deployment and operator surface with HouseGate.** Same Go toolchain, same [`pkg/log`](../../../pkg/log), same config/secrets loaders, same observability stack. One binary class, one set of runbooks.
3. **Forking ClickHouse Keeper would couple safety protocol to unstable internals.** The storage design [§16](2026-06-22-storage-integrity-design.md) already rejects "HouseGate/keeper as a ClickHouse-to-Keeper reverse proxy that *gates* merges" because gating requires parsing `ReplicationLogEntry` serialization (not a stable API across versions) and part bytes flow over interserver HTTP, not Keeper. Implementing the Sequencer's sequencing/attestation logic *inside* ClickHouse Keeper would inherit exactly that coupling. Keeping them separate is what lets ClickHouse "run unmodified" — storage design's stated v1 goal.

The two roles are tabulated once, for the last time, in [§12.1 of the storage design](2026-06-22-storage-integrity-design.md): ClickHouse Keeper coordinates ClickHouse's ReplicatedMergeTree; the Sentio Sequencer sequences the integrity log.

---

## 2. Goals and Non-Goals

Goals (all in service of the [storage design §2](2026-06-22-storage-integrity-design.md) goals; this spec is a decomposition, not a re-scoping):

1. Give the §5.2 responsibility list a concrete architecture: components, interfaces, durable state, consensus boundary, and signing plan.
2. Freeze the Go package map and the key interface signatures so P0/P1 work proceeds against a fixed target.
3. Preserve the storage design's safety predicates **unchanged** — especially the [§9](2026-06-22-storage-integrity-design.md) three-way promotion predicate (replay quorum **AND** partition-delta **AND** byte-side `part_row_lthash`), which must not degenerate to root equality.
4. Treat Sequencer HA as a v1-critical concern, because the storage design proves ([§12.3 consequence 4](2026-06-22-storage-integrity-design.md)) that v1 write availability is `Sequencer_liveness × promotion_throughput ≥ ingest_rate`.
5. Reuse [`pkg/replay`](../../../pkg/replay) objects by name rather than reinventing them.

Non-goals (specific to this sub-design; the storage design's own non-goals still apply):

1. No production Go implementation in this document. This stops at interface signatures and field-level type sketches.
2. No forking, patching, or reimplementation of ClickHouse Keeper, and no Go code that speaks the ZooKeeper wire protocol.
3. No claim that the Sentio Sequencer solves malicious safe-query serving. That remains the separate [§13](2026-06-22-storage-integrity-design.md) serving-integrity problem (probabilistic audit, query attestation).
4. No multi-Raft in v1. See [§9](#9-consensus-and-ha) for why "before production v1" multi-Raft is over-scoped and how the spec phases it instead.
5. No new on-chain/economic slashing game. The Sentio Sequencer produces *evidence* (signed attestations, mismatch receipts); the economic layer that consumes it is out of scope here.

---

## 3. Component map

Sentio Sequencer is one process containing seven logical subsystems. The first six are pure functions over replicated state; the seventh owns the replication itself.

```text
                         ┌─────────────────────────────────────────────────────────┐
                         │                Sentio Sequencer process                 │
   HouseGate ─submit────▶│  1. Sequencer      ──▶  2. Dedup (accumulator + hi-seq)  │
   (signed envelope)      │        │                       │ (non-membership proof) │
                         │        ▼                       ▼                         │
                         │  3. Claim registry ◀── RCRecord from Source SNode       │
                         │        │                                                 │
                         │        ▼                                                 │
                         │  4. Replay orchestrator ──▶ ReplayJob ──▶ Verifier pool │
                         │        │                       ◀── ReplayAttestation ── │
                         │        ▼                       (Ed25519-signed)         │
                         │  5. Promotion publisher (§9 three-way predicate)        │
                         │        │                                                 │
                         │        ▼                                                 │
                         │  ─▶ PromotionCommand (secp256k1) ──▶ SNode REPLACEMENT  │
                         │  ─▶ SafeSnapshotManifest         ──▶ publish + L2 anchor│
                         │                                                         │
                         │  6. Schema-transition lane  ◀── DDL admission          │
                         │  6. Membership authority    ◀── replica lifecycle       │
                         │                                                         │
                         │  7. Consensus core (single Raft group, etcd/raft + bbolt)│
                         │     replicates the durable state machine that 1–6 read   │
                         └─────────────────────────────────────────────────────────┘
```

Subsystems and what each owns:

| # | Subsystem | Owns | Talks to |
|---|---|---|---|
| 1 | **Sequencer** | `statement_seq` assignment, L3 block construction, `statement_id → statement_seq` anchoring | HouseGate (submit), subsystem 2 |
| 2 | **Dedup** | mountain-range Merkle accumulator (`spent_ids_root`), per-account high-water mark, non-membership proof validation | Sequencer |
| 3 | **Claim registry** | `RCRecord` storage + validation front (linkage, schema, payload, part-arithmetic checks) | Source SNode (claim submission) |
| 4 | **Replay orchestrator** | `ReplayJob` construction from L3 + prev safe snapshot + source claim, dispatch to verifier replicas, attestation collection, challenge opening | Verifier pool / SNode replicas |
| 5 | **Promotion publisher** | the [§9](2026-06-22-storage-integrity-design.md) three-way predicate, `PromotionCommand` signing + issuance, `SafeSnapshotManifest` publication, safe watermark advance, L2 anchoring | SNode (promote), L2 anchor |
| 6a | **Schema-transition lane** | singleton/block-boundary DDL, schema barriers, anchored `schema_snapshot_id` / `schema_root` | HouseGate (DDL submit), SNode (observed `schema_hash`) |
| 6b | **Membership authority** | `ReplicaStatus`, source-node selection, Active read-set admission, lagging-replica promotion replay, cold-bootstrap gating | SNode replicas |
| 7 | **Consensus core** | the replicated state machine backing 1–6; the only subsystem allowed to *commit* state | Raft peers (other Sequencer nodes) |

**Boundary invariants the map implies:**

- **HouseGate submits, never decides.** The ingress HouseGate validates the signed envelope's signature, computes `payload_hash`, and spools the payload, then submits a `StatementEnvelopeV2` to the Sequencer. It does not assign `statement_seq`, does not promote, and is explicitly not "the final judge of correctness" ([storage design §5.1](2026-06-22-storage-integrity-design.md)).
- **Verifier replicas attest, never promote.** A verifier replica runs [`replay.Verifier.Verify`](../../../pkg/replay/verifier.go) and returns a `ReplayAttestation`. Promotion is the publisher's call, gated on a quorum of attestations plus the [§9](2026-06-22-storage-integrity-design.md) three-way predicate. The verifier types are reused unchanged (see [§4](#4-state-model)).
- **ClickHouse Keeper is off this diagram entirely.** It coordinates ClickHouse's own `hg_unsafe` ReplicatedMergeTree and is invisible to subsystems 1–6. HouseGate's optional L4 forwarding of the ClickHouse↔Keeper connection lives in [`pkg/replicationproxy`](../../../pkg/replicationproxy) and is network isolation only.

---

## 4. State model

The state model is split into two categories: **types we reuse as-is from [`pkg/replay`](../../../pkg/replay)**, and **types this design introduces**. Reuse is non-negotiable: the integrity layer's correctness argument is built on those exact types, and inventing aliases drifts the contract.

### 4.1 Reused from `pkg/replay` (do not redefine)

These are the load-bearing types. Field lists below are summaries; the canonical definition is the linked source.

- **`replay.SafeSnapshotManifest`** — [`pkg/replay/types.go:11`](../../../pkg/replay/types.go). The published safe state. Fields: `SnapshotID, ParentSnapshotID, SafeBlockSeq uint64, SchemaSnapshotID, SchemaRoot, ExecutorProfileID, DataRoot, StateRoot, ManifestRoot, Tables []TableManifest`. Has value-receiver `Seal()`, `Validate()`, and `Compute*Root()` methods that the publisher calls; `SnapshotID` defaults to `ManifestRoot` when unset.
- **`replay.ReplayJob`** — [`pkg/replay/types.go:212`](../../../pkg/replay/types.go). The verifier input for one sequenced block: `BlockSeq, PrevSafeSnapshotID, PrevStateRoot, SchemaSnapshotID, ExecutorProfileID, SourceClaimRoot, Statements []Statement`. The orchestrator builds this from `L3Block + RCRecord + prev manifest`.
- **`replay.Statement`** — [`pkg/replay/types.go:223`](../../../pkg/replay/types.go). The replay-relevant projection of a signed envelope: `StatementID, StatementSeq uint64, SQL, SQLHash, SettingsHash, PayloadRef, PayloadHash, PayloadLength uint64, TargetTableID, UserJWS`. Note `StatementSeq` is already present and non-empty by the time it reaches the verifier — the Sequencer assigned it.
- **`replay.ExecutionReceipt`** — [`pkg/replay/types.go:265`](../../../pkg/replay/types.go). What a verifier signs: block metadata + `StatementRoot, PayloadRoot, SourceClaimRoot, ComputedStateRoot, MatchSourceRoot bool, PartitionCommitmentsAfter, AffectedParts, ReplayLogHash`. Has `Hash() (string, error)` via `canonicalDigest("replay-execution-receipt", r)`. Mismatches (`MatchSourceRoot=false`) are signed too — that is the challenge-evidence path.
- **`replay.ReplayAttestation`** — [`pkg/replay/types.go:286`](../../../pkg/replay/types.go). The verifier output the publisher collects: `ReplicaID, Receipt ExecutionReceipt, ReceiptHash, Signature, MatchSourceRoot bool`.
- **`replay.Verifier`** — [`pkg/replay/verifier.go:31`](../../../pkg/replay/verifier.go) (a struct, not an interface): `Snapshots SnapshotStore, Payloads PayloadStore, Executor Executor, Signer Signer`. Entry point `Verify(ctx, ReplayJob) (ReplayAttestation, error)`. It already enforces: snapshot load + `Validate`, `block_seq > snap.SafeBlockSeq`, payload `hash`/`length` match *before* execution, executor output shape, and receipt signing. The Sequencer does **not** re-implement any of this.
- **Interfaces** — `replay.SnapshotStore` ([`:9`](../../../pkg/replay/verifier.go)), `replay.PayloadStore` ([`:14`](../../../pkg/replay/verifier.go)), `replay.Executor` ([`:19`](../../../pkg/replay/verifier.go)), `replay.Signer` ([`:25`](../../../pkg/replay/verifier.go)). The Sequencer supplies production `SnapshotStore`/`PayloadStore` implementations backed by the consensus core's replicated state (or external DA for payloads — open question [§12 Q2](#12-open-questions)).
- **Hash primitives** — `replay.DigestBytes` / `DigestString` ([`pkg/replay/hash.go:15`](../../../pkg/replay/hash.go)) and the unexported `canonicalDigest(domain, v)` (`"housegate-replay-mvp-v0:<domain>\x00<body"` → sha256, `0x`-prefixed hex). **All new Sequencer-side commitments reuse `canonicalDigest` with a new domain tag** (see [§4.3](#43-hashing-and-canonicalization)) rather than introducing a parallel hash profile.

### 4.2 New types introduced by this design

All new types are JSON-serialized Go structs with the same `json` tag conventions as `pkg/replay`, so they flow through `canonicalDigest` identically. Field-level sketches below freeze the shape; the exact wire schema is P0 work (open question [§12 Q1](#12-open-questions)).

```go
// StatementEnvelopeV2 is what HouseGate submits after validating the user JWS.
// It carries Phase-1 (agent-materialized) SQL; Phase-2 physical rewrite is NOT
// signed and is recomputed by the executor during replay (storage design §7).
StatementEnvelopeV2 {
  envelope_version    int      `json:"envelope_version"`
  network_id          uint64   `json:"network_id"`
  keeper_shard_id     uint32   `json:"keeper_shard_id"`
  client_account      string   `json:"client_account"`
  statement_id        string   `json:"statement_id"`     // signed; feeds _hg_row_id
  statement_kind      string   `json:"statement_kind"`   // insert|mutation|ddl
  virtual_table_id    string   `json:"virtual_table_id"`
  rewritten_sql       string   `json:"rewritten_sql"`    // Phase-1 output
  sql_hash            string   `json:"sql_hash"`         // H(rewritten_sql)
  settings_hash       string   `json:"settings_hash"`
  schema_snapshot_id  string   `json:"schema_snapshot_id"`
  payload_ref         string   `json:"payload_ref,omitempty"`
  payload_hash        string   `json:"payload_hash,omitempty"`
  payload_length      uint64   `json:"payload_length,omitempty"`
  payload_format      string   `json:"payload_format,omitempty"`
  row_id_profile_id   string   `json:"row_id_profile_id"`
  user_jws_v2         string   `json:"user_jws_v2"`
}

// L3Block is the sequencer's committed batch. statement_seq is assigned here,
// not in the envelope. spent_ids_root_after is the L3-derived mountain-range
// accumulator root after folding this block's statement_ids (storage design §7).
L3Block {
  l3_block_seq        uint64                  `json:"l3_block_seq"`
  prev_l3_hash        string                  `json:"prev_l3_hash"`
  l2_anchor_ref       string                  `json:"l2_anchor_ref,omitempty"`
  statement_seq_start uint64                  `json:"statement_seq_start"`
  statements          []StatementEnvelopeV2   `json:"statements"`
  schema_snapshot_id  string                  `json:"schema_snapshot_id"`
  executor_profile_id string                  `json:"executor_profile_id"`
  prev_safe_snapshot_id string                `json:"prev_safe_snapshot_id"`
  prev_state_root     string                  `json:"prev_state_root"`
  spent_ids_root_after string                 `json:"spent_ids_root_after"`
}

// RCRecord is the source SNode's result claim. The claim registry accepts it
// only after the validation front (§5) passes.
RCRecord {
  l3_block_seq           uint64           `json:"l3_block_seq"`
  statement_seq          uint64           `json:"statement_seq"`
  source_node            string           `json:"source_node"`
  unsafe_table           string           `json:"unsafe_table"`
  candidate_parts        []CandidatePart  `json:"candidate_parts"`
  partition_deltas       []PartitionDelta `json:"partition_deltas"`
  source_claim_state_root string          `json:"source_claim_state_root"`
}

CandidatePart {
  part_name        string `json:"part_name"`
  partition_id     string `json:"partition_id"`
  part_phys_hash   string `json:"part_phys_hash"`
  part_row_lthash  string `json:"part_row_lthash"`   // 0x-prefixed hex of lthash.Hash.Bytes()
  row_count        uint64 `json:"row_count"`
  bytes            uint64 `json:"bytes"`
}

PartitionDelta {
  table_id         string `json:"table_id"`
  partition_id     string `json:"partition_id"`
  delta            string `json:"delta"`             // 0x-prefixed hex; sum(new) - sum(old) LtHash
}

// PromotionCommand is what the publisher issues after the §9 three-way
// predicate passes. It is secp256k1-signed by the Sequencer identity (§6.3) and
// drives the local REPLACE PARTITION on each attesting SNode (storage §12.2).
PromotionCommand {
  command_kind          string   `json:"command_kind"`            // insert|mutation|merge|drop_unsafe
  table_id              string   `json:"table_id"`
  partition_id          string   `json:"partition_id"`
  base_safe_snapshot_id string   `json:"base_safe_snapshot_id"`  // CAS base for the publish lock
  base_partition_root   string   `json:"base_partition_root"`    // CAS base for the partition
  promotion_seq         uint64   `json:"promotion_seq"`          // per-(table,partition_id) monotonic
  target_snapshot_id    string   `json:"target_snapshot_id"`     // resulting SafeSnapshotManifest
  target_state_root     string   `json:"target_state_root"`
  promoted_part_hashes  []string `json:"promoted_part_hashes"`   // candidate part_phys_hash set
  issued_at_unix        int64    `json:"issued_at_unix"`
  expires_at_unix       int64    `json:"expires_at_unix"`        // SNode must publish before this
  sequencer_address     string   `json:"sequencer_address"`      // secp256k1 signer address
  sequencer_signature   string   `json:"sequencer_signature"`    // populated by Sign()
}

// SchemaTransition is a singleton/block-boundary DDL record on the
// schema-transition lane (§7). It mints a new schema_snapshot_id.
SchemaTransition {
  transition_seq        uint64   `json:"transition_seq"`
  prev_schema_snapshot_id string `json:"prev_schema_snapshot_id"`
  new_schema_snapshot_id  string `json:"new_schema_snapshot_id"`
  new_schema_root        string  `json:"new_schema_root"`
  ddl_envelope           StatementEnvelopeV2 `json:"ddl_envelope"`
  observed_schema_hashes map[string]string   `json:"observed_schema_hashes,omitempty"` // table_id -> SNode-reported
}

// ReplicaStatus tracks one SNode's lifecycle (§8).
ReplicaStatus struct {
  replica_id            string    `json:"replica_id"`
  indexer_address       string    `json:"indexer_address"`
  state                 string    `json:"state"`           // joining|catching_up|active|suspended|leaving
  last_safe_snapshot_id string    `json:"last_safe_snapshot_id"`
  last_seen_unix        int64     `json:"last_seen_unix"`
  active_read_set       bool      `json:"active_read_set"`
}
```

`SafeSnapshotManifest` publication, `ReplayJob` construction, and `ReplayAttestation` collection are **not** new types — they are operations the publisher and orchestrator perform on the reused `pkg/replay` types. The storage design already names them; this design reuses the Go structs.

### 4.3 Hashing and canonicalization

Every new commitment the Sequencer mints (L3 block hash, accumulator root, `PromotionCommand` signing payload, `SchemaTransition` root) goes through `pkg/replay.canonicalDigest` with a new domain tag:

```text
canonicalDigest("sequencer-l3-block",            l3BlockJSON)
canonicalDigest("sequencer-spent-ids",           accumulatorStateJSON)
canonicalDigest("sequencer-promotion-command",   promotionCommandCanonicalJSON)   // minus sequencer_signature
canonicalDigest("sequencer-schema-transition",   schemaTransitionJSON)
```

`canonicalDigest` is currently unexported. P0 will either export a thin `replay.CanonicalDigest(domain, v)` wrapper or add a `keeper` package helper that calls it — the goal is one canonicalization profile across the integrity layer, not two. This is listed as a P0 task in [§11](#11-delivery-phases).

---

## 5. Sequencing and dedup

This section maps the [storage design §7](2026-06-22-storage-integrity-design.md) sequencing rules onto the components in [§3](#3-component-map). No rule is changed; the value here is specifying *where* each rule is enforced.

### 5.1 `statement_seq` vs `statement_id` — enforced by different components

| | `statement_id` | `statement_seq` |
|---|---|---|
| Assigned by | client/agent (before signing) | **Sequencer** (subsystem 1, after submission) |
| Signed? | **yes** (in `user_jws_v2`) | **no** |
| Validated by | HouseGate (signature), Sequencer subsystem (shape) | n/a — the Sequencer mints it |
| Dedup-enforced by | **Dedup** (subsystem 2): accumulator + high-water | n/a |
| Role | identity / dedup / feeds `_hg_row_id` | ordering / part attribution |

The split is load-bearing: `statement_seq` cannot be signed because [the signer cannot know its position at signing time](2026-06-22-storage-integrity-design.md) (storage design §7). The Sequencer therefore accepts only `statement_id` from the envelope and anchors the `statement_id → statement_seq` binding in the L3 block, making the mapping auditable.

### 5.2 Dedup: accumulator + high-water mark (subsystem 2)

Adopts [storage design §7](2026-06-22-storage-integrity-design.md) verbatim:

- A **mountain-range Merkle accumulator** commits `spent_ids_root` in each L3 block. It is a pure function of sequenced `statement_id`s — any honest node replaying the L3 stream reconstructs it identically, so decentralizing the Sequencer does not change the dedup fact.
- Acceptance requires a **non-membership proof** that the `statement_id` is not under the previous `spent_ids_root`.
- A **per-account high-water mark** `hi_seq[account]` gives well-behaved traffic O(1) acceptance: a new `client_seq > hi_seq` needs no non-membership proof; only out-of-order `client_seq ≤ hi_seq` falls back to the accumulator proof. This bounds dedup state to one integer per active account plus a gap set, and shards cleanly by `client_account`.
- The accumulator is append-only and permanent; scope is **per-account-global**.

The mountain-range construction and its non-membership proof test vectors are storage-design P0 deliverables; the Sequencer's job in P1 is to wire them into subsystem 2 behind the `Accumulator` interface in [§10.2](#102-dedup).

### 5.3 L3 block construction (subsystem 1)

Once dedup passes, the Sequencer:

1. Assigns a monotonic `statement_seq` to each accepted `statement_id` (the block's `statement_seq_start` plus offset).
2. Batches accepted envelopes into an `L3Block` carrying one `schema_snapshot_id` and one `executor_profile_id` for the whole block (the v1 block-level schema scoping rule, [storage design §7](2026-06-22-storage-integrity-design.md)).
3. Folds the block's `statement_id`s into the accumulator and records `spent_ids_root_after`.
4. Commits the block through the consensus core (subsystem 7). The block is not durable until Raft commits it.

### 5.4 Source-claim validation front (subsystem 3)

The claim registry accepts a `RCRecord` from the source SNode only after these checks pass, in this order (cheap→expensive, fail-closed):

1. **Linkage** — `RCRecord.l3_block_seq` and `RCRecord.statement_seq` reference a committed L3 block; `source_node` matches the Sequencer's source assignment for that statement.
2. **Schema/settings** — the candidate parts' table references resolve under the block's `schema_snapshot_id`.
3. **Payload availability** — `payload_ref` is retrievable from the payload store; `payload_hash`/`payload_length` match ([storage design §5.4](2026-06-22-storage-integrity-design.md) loads bytes only after this check).
4. **Part arithmetic** — `Σ candidate_parts.part_row_lthash` per partition is internally consistent with `partition_deltas`, and `partition_deltas` fold into `source_claim_state_root` under the same LtHash arithmetic the executor uses ([§8](2026-06-22-storage-integrity-design.md)).

A `RCRecord` that fails the front is rejected; the orchestrator never sees it. This is *registration validation*, not promotion — promotion still requires the [§6](#6-replay-orchestration-and-promotion) three-way predicate. The two are distinct: a self-consistent `RCRecord` over `bytes_evil` passes the front but fails check 2/3 of promotion.

---

## 6. Replay orchestration and promotion

This is the safety-critical core. The [storage design §9](2026-06-22-storage-integrity-design.md) three-way promotion predicate is reproduced here unchanged and named explicitly so no implementation can quietly weaken it.

### 6.1 Replay orchestration (subsystem 4)

For each committed L3 block with an accepted `RCRecord`, the orchestrator:

1. Builds a [`replay.ReplayJob`](../../../pkg/replay/types.go) from `L3Block + RCRecord.source_claim_state_root + the previous SafeSnapshotManifest identity`. The mapping is mechanical — every `StatementEnvelopeV2` becomes a `replay.Statement` (already the replay-relevant projection), `L3Block.l3_block_seq → ReplayJob.BlockSeq`, `prev_safe_snapshot_id/prev_state_root → ReplayJob.Prev*`, etc.
2. Dispatches the `ReplayJob` to ≥3 independent verifier replicas (storage design P0 freeze: **promote requires ≥2 of 3**; the source's self-attestation does not count).
3. Each replica runs [`replay.Verifier.Verify`](../../../pkg/replay/verifier.go), which loads the prev safe snapshot, validates payload hash/length *before* execution, runs the pinned executor, and returns a signed `ReplayAttestation` carrying `ComputedStateRoot` and `MatchSourceRoot`.
4. On timeout or disagreement, the orchestrator opens a **challenge replay** ([§6.4](#64-challenge-replay)).

The verifier replicas are *separate* from the Sequencer process (they hold the pinned ClickHouse build + scratch executor); the Sequencer only constructs jobs and collects attestations.

### 6.2 The three-way promotion predicate (subsystem 5)

Promotion is **not** root equality. It is the conjunction of three checks, every one load-bearing, exactly as in [storage design §9](2026-06-22-storage-integrity-design.md):

1. **Replay check** — a quorum (≥2 of 3) of replicas independently replays the signed L3 payload and produces the same `computed_state_root` as `RCRecord.source_claim_state_root`. *Proves the payload's correct execution yields this root.*
2. **Partition-delta check** — for each touched partition, `Σ(candidate_parts.part_row_lthash)` reported by the source equals the partition delta the replicas computed during replay. *Binds the source's per-part claims to the replay root; defeats a colluding source that would otherwise report per-part hashes summing to the correct delta for evil rows — infeasible without a collision, which per-row `_hg_row_id` rules out.*
3. **Byte-side part-lthash check** — each attesting replica reads the part bytes it actually fetched (`SELECT ... WHERE _part IN (...)`), recomputes `part_row_lthash`, and confirms it equals `RCRecord.candidate_parts`. *Binds the source's reported per-part hashes to the actual bytes on disk; the only one of the three that touches the source's actual part bytes.*

**Promotion refuses if any of the three fails.** A root match without checks 2 and 3 is explicitly *not* promotion, and 2 and 3 are complementary, not redundant: check 2 closes the "report a correct-looking `part_row_lthash` for `bytes_evil`" half; check 3 closes the "report a hash for `bytes_evil` but store divergent bytes" half. The promotion chain is `root —check 2→ Σ source per-part claims —check 3→ actual disk bytes`; every link is needed.

> **Spec guardrail.** Any code change that lets a part enter `hg_safe` on root equality alone is a correctness regression, not an optimization. The P0 freeze on this predicate is what the [acceptance grep](#verification) checks for.

### 6.3 INSERT vs mutation paths

The third check differs by statement class ([storage design §10](2026-06-22-storage-integrity-design.md)):

- **INSERT path** — check 3 is a fetched-byte scan over the shared replicated `hg_unsafe` candidate parts (every replica fetches the same bytes).
- **Mutation path** — there is no shared fetched-byte object (each replica regenerates mutated parts in its own scratch). Check 3 becomes a **recomputed-commitment match**: each replica recomputes the post-mutation per-partition `partition_commitment` from its own locally materialized post-state and confirms it equals the safe pre-state commitment plus the claimed `partition_deltas`. The comparison is absolute-against-absolute (`partition_commitment` is an absolute LtHash accumulator; `partition_deltas` is `Σ new − Σ old`; additivity makes `post = pre + delta` exact).

Both paths share checks 1 and 2 unchanged. Mutation-class statements (`ALTER … UPDATE/DELETE`) are admitted only under bounded profiles in v1 ([storage design §10](2026-06-22-storage-integrity-design.md)); `INSERT … SELECT` is rejected at admission.

### 6.4 Challenge replay

A mismatch or timeout opens a challenge replay. **Challenge adjudication uses the same three-way predicate as promotion** ([storage design §11](2026-06-22-storage-integrity-design.md)) — it does *not* resolve on reproduced-root equality alone, because that is exactly the `bytes_evil`-with-truthful-root case the predicate exists to reject. A signed mismatch attestation (`MatchSourceRoot=false`, still signed by `replay.Verifier`) is non-repudiable challenge evidence. In v1 the centralized Sequencer arbitrates immediately (no challenge window); the challenge-window safety model is the decentralized-phase ([§9](#9-consensus-and-ha)) concern.

### 6.5 Promotion command issuance and CAS

When the predicate passes, the publisher (subsystem 5):

1. Seals the resulting `SafeSnapshotManifest` (`replay.SafeSnapshotManifest.Seal()`).
2. Mints a `PromotionCommand` carrying the **CAS base** — `base_safe_snapshot_id` + `base_partition_root` + a monotonic `promotion_seq` for `(table_id, partition_id)`. The CAS base is what implements the [storage design §12.2](2026-06-22-storage-integrity-design.md) publish-time-base rule: SNode takes its local publish lock, checks the current active `hg_safe` partition still matches the base, and only then runs the Sequencer-signed `REPLACE PARTITION` from `hg_promote`. If another promotion has already advanced the partition, this command is refused and rebased.
3. Signs the command with the **Sequencer identity (secp256k1)** — see [§10.5](#105-promotion). This is a *different* signature from the attestation signatures.
4. Publishes the manifest, advances the safe watermark, anchors the L3 block hash + state root to L2.

Concurrent INSERT promotions into the same partition are serialized at `(table_id, partition_id)` by the `promotion_seq` ordering, the INSERT-path analogue of the [storage design §10](2026-06-22-storage-integrity-design.md) mutation barrier.

---

## 7. Schema-transition lane (subsystem 6a)

Adopts [storage design §7](2026-06-22-storage-integrity-design.md) schema-transition rules. In v1 every admitted schema change is sequenced as a **singleton block or a block-boundary transition** on a separate lane, not through unsafe-part promotion. The lane:

1. Installs a **table/database-level schema barrier**, stops admitting new writes under the old schema, and drains or rejects outstanding old-schema unsafe writes.
2. Mints a new `schema_snapshot_id` and `schema_root` (the lane emits a `SchemaTransition` record, [§4.2](#42-new-types-introduced-by-this-design)).
3. Issues the Sequencer-signed DDL to all protocol-owned physical surfaces (`hg_safe`, `hg_unsafe`, `hg_promote` templates, mutation scratch templates, replay scratch templates).
4. Accepts SNode-reported `schema_hash` observations and compares to the anchored root. **Source-side `system.columns` is an observation, not authority** — verifiers derive the schema exclusively from the anchored DDL/settings log. Normal writes resume only after the local schema matches the anchored root.

DDL admission classes for v1 (reproduced from [storage design §7](2026-06-22-storage-integrity-design.md)):

| Statement class | v1 route |
|---|---|
| `CREATE TABLE` | Admit only if engine, partition key, order key, primary key, storage policy, defaults/materialized expressions, and types are on the verified whitelist. The Sequencer allocates stable `table_id`/`column_id` and injects reserved columns (`_hg_row_id`). |
| `ADD COLUMN` | Metadata-only only for non-key, non-reserved columns with deterministic immutable `DEFAULT`/`NULL` semantics and stable `column_id`. Commitment-neutral only if the profile defines how old sealed parts canonicalize a missing column. |
| `RENAME COLUMN` | Metadata-only: commitments bind `column_id`, not display names. Reserved columns never renamable. |
| `MODIFY DEFAULT` | Rejected unless the profile proves it affects only future inserts and not read-time values for old sealed parts. |
| `DROP COLUMN` / `MODIFY COLUMN` type | Rejected in v1 by default; a later admitted form must be mutation-class rehash. |
| `TRUNCATE` / `DROP PARTITION` | Mutation-class but cheap: delta is `-partition_commitment`. |
| Partition/order/primary key, engine, storage policy, TTL, projection/index changes | Rejected in v1. |
| `_hg_row_id` and other protocol columns | Never user-modifiable. |

---

## 8. Replica lifecycle and membership (subsystem 6b)

The membership authority owns the [storage design §11](2026-06-22-storage-integrity-design.md) state machine and the §12.5 bootstrap paths. State transitions (reproduced):

```text
[*] → Accepted → Sequenced → UnsafeExecuting → UnsafeRegistered
                                   → Replaying → QuorumVerified → FinalityWait → Safe
                                                → ChallengeReplay → Safe | Rejected → Dropped
```

Responsibilities:

- **Source-node selection** — picks the optimistic-execution source for a statement (the storage design's route A default).
- **Replay-quorum membership** — selects the ≥3 verifier replicas for a `ReplayJob` and counts attestations; the source's self-attestation does not count toward the 2-of-3.
- **Lagging-replica promotion replay** — a replica that has not yet fetched candidate parts via ReplicatedMergeTree is a liveness issue, not safety (it did not attest). When it catches up, it **replays the recorded per-`(table_id, partition_id)` promotion sequence in order**, resolving each promotion's CAS base against that promotion's recorded `base_safe_snapshot_id`/`base_partition_root`, not against its present watermark ([storage design §12.5](2026-06-22-storage-integrity-design.md)). This is what lets a late replica still reproduce each step.
- **Cold bootstrap** — a fresh/long-offline node starts empty. Two recovery paths, both off the hot path: (1) **replay from the L3 stream** (reconstruct `hg_safe` by replaying signed payloads from genesis or the oldest retained safe snapshot); (2) **copy from a peer's `hg_safe`** (fetch safe parts, verify each part's `part_phys_hash`/`part_row_lthash` against the published manifest, attach). A new node may not enter the **Active read set** until it produces a `SafeSnapshotManifest` matching the network's current safe watermark ([storage design §12.5 open question 14](2026-06-22-storage-integrity-design.md)).
- **Audit hooks** — feeds the [storage design §13](2026-06-22-storage-integrity-design.md) safe-serving audit (periodic safe-part scans, query sampling, cross-node comparison); audit failures drop a replica from the read set.

---

## 9. Consensus and HA

This section is where this design **diverges from the `.omo/plans` draft**, which called for "multi-Raft groups before production v1." That scope is wrong for v1, and the reason is technical, not schedule-driven.

### 9.1 v1 = single Raft group (the chosen path)

v1 Sentio Sequencer runs **one Raft group** across a small (3-node) ensemble, replicating the entire durable state machine (subsystems 1–6's committed state). The library is [`go.etcd.io/raft/v3`](https://github.com/etcd-io/raft) — the same library etcd, TiKV, and CockroachDB build on. The Sequencer provides:

- A `StateMachine` implementation (the `Apply`/`Snapshot`/`Restore` triple, [§10.3](#103-state)) that wraps subsystems 1–6.
- A `raft.Storage` implementation backed by **bbolt** (`go.etcd.io/bbolt`) for the WAL/log store, plus periodic `StateMachine.Snapshot()` for log compaction.
- A transport for Raft messages between Sequencer peers (gRPC or the raw etcd-raft `Transport` interface — open question [§12 Q4](#12-open-questions)).

Everything the sequencer/publisher decides is committed through this one group before it is authoritative. Reads that need linearizable semantics go through `raft.Node.Status()`/`ReadIndex` (most Sequencer reads are of committed state and can be served locally by followers with a lease).

### 9.2 Why multi-Raft is not v1 (correcting the draft)

**Multi-Raft is not a native capability of `go.etcd.io/raft/v3`.** The library is built around a single `raft.Node` per group. The [etcd-dev MultiRaft discussion](https://groups.google.com/g/etcd-dev/c/cq88rpcxvm8) confirms MultiRaft was never merged back into etcd's raft library. Systems that need multi-Raft on this library — **TiKV** ([design writeup](https://www.pingcap.com/blog/design-and-implementation-of-multi-raft/)) and **CockroachDB** ([scaling-raft post](https://www.cockroachlabs.com/blog/scaling-raft/)) — each built their own region/group routing, per-group `raft.Node` fan-out, heartbeat coalescing, and snapshot management on top. That is a multi-quarter subsystem in its own right, with its own correctness hazards (cross-group transaction ordering, region split/merge, batched heartbeats).

Bundling that into "v1, before production" would make multi-Raft — not sequencing, not the three-way predicate, not the promote data plane — the dominant implementation risk and the longest pole. It directly contradicts the storage design's stated v1 goal of "implementable without forking ClickHouse" extended to its natural analogue: *implementable without inventing a distributed database*.

So the phasing is:

- **v1 (P1):** single Raft group, 3 nodes, full sequencer + INSERT promote. The `StateMachine` and a `Sharder` interface (below) are defined but the `Sharder` returns one group for every key.
- **P5+:** multi-Raft + sharding by table/database/account, the production horizontal-scale path. Reopens [storage design §15 Q15](2026-06-22-storage-integrity-design.md) "multi-Raft group layout, shard routing by table/database, cross-Sequencer L2 height clock." The `Sharder` interface is what makes this a *replacement* of one function rather than a rewrite.

The Sequencer-facing interface that future-proofs this:

```go
// Sharder maps a state key to the Raft group responsible for it. v1 has one
// group (group 0) for every key; P5+ introduces per-table/account groups.
type Sharder interface {
    GroupForStatement(env StatementEnvelopeV2) (groupID uint64, err error)
    GroupForPartition(tableID, partitionID string) (groupID uint64, err error)
    GroupForSchema() (groupID uint64, err error) // schema lane
    Groups() []uint64
}
```

`Sharder` is the *only* multi-Raft seam v1 commits to; the consensus core calls it and gets `0` back for everything.

### 9.3 Sequencer liveness is a write-availability single point (acknowledged, mitigated, not pretended away)

[Storage design §12.3 consequence 4](2026-06-22-storage-integrity-design.md) proves v1 write availability is `Sequencer_liveness × promotion_throughput ≥ ingest_rate`: with `hg_unsafe` under `STOP MERGES` and a pinned `parts_to_throw_insert`, the only thing draining `hg_unsafe` is promotion, and promotion requires the Sequencer. A Sequencer outage stops promotion, parts accumulate, a partition hits the ceiling, and writes get refused network-wide with **no local escape valve** (no `hg_unsafe` merge — that's the safety invariant; no local promote — the Sequencer owns the command).

**v1 mitigation is HA, not decentralization.** The 3-node Raft group tolerates one node failure with no promotion interruption. But "trusted" ([storage design §4](2026-06-22-storage-integrity-design.md)) is not "highly available": trusting the Sequencer ensemble for *safety* does not guarantee its *liveness* for writes. Two concrete consequences this design accepts and names:

1. The Raft ensemble must be operationally treated as write-critical infra (not coordination infra). Its quorum is on the write path.
2. The documented escape hatches — replay from the L3 stream / copy from a peer's `hg_safe` ([storage design §12.5](2026-06-22-storage-integrity-design.md)) — recover `hg_safe` *after* a Sequencer outage, but they do **not** drain `hg_unsafe` *while* the Sequencer is still down. There is no v1 fix for this; it is the cost of the engine split.

P5+ decentralization changes *who checks and who bears economic consequences*, not this liveness arithmetic.

---

## 10. Go package and API map

This section freezes the package layout and the key interface signatures. It is **signatures only** — no implementations. Naming follows the repo's leaf-package convention (each subpackage gets a distinct Go identifier to avoid collisions, as in `pkg/plugins/*`).

### 10.1 Layout

```text
pkg/sequencer/
  types/         — new JSON types (§4.2) + type aliases re-exporting pkg/replay types
  state/         — replicated StateMachine: Apply/Snapshot/Restore over sequencer entries
  dedup/         — mountain-range accumulator + per-account high-water mark
  replay/        — ReplayJob construction, dispatch, attestation collection, three-way check
  promotion/     — PromotionCommand construction + secp256k1 signing + CAS-base minting
  schema/        — schema-transition lane, barriers, DDL admission
  membership/    — ReplicaStatus lifecycle, Active read-set admission, bootstrap gating
  consensus/     — etcd/raft Node wrapper + bbolt-backed raft.Storage + Sharder
  signing/       — PromotionSigner (secp256k1) + AttestationSigner adapter (Ed25519)
  internal/      — protobuf-free JSON canonicalization helper (wraps replay.canonicalDigest)
cmd/sentio-sequencer/ — binary: config, Raft bootstrap, gRPC/HTTP admin surface, Run(ctx)
```

### 10.2 `dedup`

```go
package dedup

// Accumulator is the L3-derived mountain-range Merkle accumulator over
// statement_ids (storage design §7). Append-only and permanent; per-account-global scope.
type Accumulator interface {
    // Root returns the current spent_ids_root.
    Root() string
    // ProveNonMembership returns a non-membership proof that id is not yet spent.
    ProveNonMembership(id string) (NonMembershipProof, error)
    // VerifyNonMembership validates a proof against a stated root.
    VerifyNonMembership(root string, id string, p NonMembershipProof) error
    // Add folds id into the accumulator. Called only by the state machine on commit.
    Add(id string) error
}

// HighWaterMark is the per-account O(1) fast path: a new client_seq > hi_seq[account]
// needs no non-membership proof; only out-of-order client_seq ≤ hi_seq falls back
// to the accumulator proof.
type HighWaterMark interface {
    Get(account string) (clientSeq uint64, ok bool)
    Advance(account string, clientSeq uint64) error
    Gap(account string) []uint64 // the out-of-order set still requiring accumulator proofs
}
```

### 10.3 `state`

```go
package state

// Entry is one committed unit of Sequencer state. The consensus core commits a
// []byte-encoded Entry via raft; StateMachine.Apply decodes and applies it.
// Variants: SequencerEntry (new L3 block), ClaimEntry (accepted RCRecord),
// PromotionEntry (issued PromotionCommand + new SafeSnapshotManifest),
// SchemaEntry (SchemaTransition), MembershipEntry (ReplicaStatus change).
type Entry struct {
    Seq   uint64      `json:"seq"`
    Kind  string      `json:"kind"`
    // payload is one of the Entry variants above, JSON-encoded.
    Payload json.RawMessage `json:"payload"`
}

// StateMachine is the application of committed Entries to in-memory state.
// It is the only thing allowed to mutate subsystems 1–6's committed view.
// Snapshot/Restore are how the consensus core compacts the Raft log.
type StateMachine interface {
    Apply(ctx context.Context, e Entry) error
    Snapshot() (Snapshot, error)
    Restore(s Snapshot) error

    // Read-only views used by the API surface (served from committed state):
    L3Block(seq uint64) (types.L3Block, bool)
    RCRecord(blockSeq, statementSeq uint64) (types.RCRecord, bool)
    CurrentSafeSnapshot() (replay.SafeSnapshotManifest, error)
    Replica(replicaID string) (types.ReplicaStatus, bool)
}
```

### 10.4 `replay` (orchestrator)

```go
package replay // pkg/sequencer/replay, distinct import path from pkg/replay

// Orchestrator builds ReplayJobs, dispatches them, collects attestations, and
// evaluates the storage-design §9 three-way predicate. It does NOT sign
// promotion commands — that is pkg/sequencer/promotion's job.
type Orchestrator interface {
    // BuildJob maps a committed L3Block + accepted RCRecord + prev manifest
    // identity into a replay.ReplayJob. Mechanical mapping (§6.1).
    BuildJob(block types.L3Block, claim types.RCRecord, prev replay.SafeSnapshotManifest) (replay.ReplayJob, error)

    // Dispatch sends the job to ≥3 verifier replicas and collects attestations.
    // The source's self-attestation does not count toward quorum.
    Dispatch(ctx context.Context, job replay.ReplayJob, replicas []string) ([]replay.ReplayAttestation, error)

    // ThreeWayPromote evaluates the §9 predicate over collected attestations
    // and the source RCRecord. Returns (ok, evidence). MUST NOT reduce to
    // root-equality: all three checks are enforced (spec guardrail §6.2).
    ThreeWayPromote(atts []replay.ReplayAttestation, claim types.RCRecord) (PromotionDecision, error)
}

type PromotionDecision struct {
    Ok              bool
    Root            string             // computed_state_root (quorum-agreed)
    PartitionDeltas []types.PartitionDelta
    ByteSideOK      map[string]bool    // part_phys_hash -> recompute matched
    FailureReason   string             // populated when !Ok, for challenge evidence
}
```

### 10.5 `promotion` and the signing plan

**Two distinct signature schemes, two distinct identities.** This is the second place this design sharpens the `.omo` draft, which left signing ambiguous.

| Object signed | Scheme | Identity | Interface | Reuse |
|---|---|---|---|---|
| `ExecutionReceipt` → `ReplayAttestation.Signature` | **Ed25519** | verifier replica (`replica_id`) | `replay.Signer` ([`pkg/replay/verifier.go:25`](../../../pkg/replay/verifier.go)) | `payloadexec.Ed25519Signer` ([`pkg/replay/payloadexec/signer.go:14`](../../../pkg/replay/payloadexec/signer.go)) already satisfies it |
| `PromotionCommand.sequencer_signature` | **secp256k1** | Sequencer ensemble (one address) | new `signing.PromotionSigner` | backed by `auth.RelaySigner` ([`pkg/auth/relay_signer.go:23`](../../../pkg/auth/relay_signer.go)) |

The two-identity split is deliberate and maps to the trust model: verifier replicas are independent witnesses that must not share a key (a colluding quorum is the threat), so they get per-replica Ed25519 keys; the Sequencer is the single trust root that issues *commands SNodes act on*, and it reuses the deployment's existing secp256k1 relay identity (same key HouseGate uses for peer trust) so SNodes verify promotion commands with an allowlist they already maintain.

```go
package signing

// PromotionSigner signs a PromotionCommand with the Sequencer's secp256k1 identity.
// Backed by *auth.RelaySigner. The signing payload is
// canonicalDigest("sequencer-promotion-command", cmd_canonical_json_minus_signature).
type PromotionSigner interface {
    Sign(cmd types.PromotionCommand) (types.PromotionCommand, error) // fills sequencer_address + sequencer_signature
    Address() string
}

// PromotionValidator runs on SNode. Verifies sequencer_signature against an allowlist.
type PromotionValidator interface {
    Validate(cmd types.PromotionCommand, allowedSequencerAddresses []string) error
}

// AttestationSigner adapts a verifier replica's Ed25519 key to replay.Signer.
// This is the seam that lets a Sequencer-side verifier reuse payloadexec.Ed25519Signer
// or a KMS-backed Ed25519 key without touching pkg/replay.
type AttestationSigner interface {
    replay.Signer
}
```

`pkg/auth`'s `RelaySigner` already exposes `Address()` and a private secp256k1 key; the `PromotionSigner` implementation is a thin adapter that builds the canonical signing payload, signs it with the relay key, and stamps `sequencer_address`. It does **not** reuse `SignToken`/`SignPeerLogin` (those bind SQL/audience respectively) — promotion has its own payload schema.

### 10.6 `consensus`

```go
package consensus

// Node wraps go.etcd.io/raft/v3's raft.Node + a bbolt-backed raft.Storage +
// the transport. v1 runs one group (group 0); the Sharder is the P5+ seam.
type Node interface {
    // Propose submits an Entry to the Raft group returned by the Sharder.
    // Blocks until Raft commits (or ctx cancels). Linearizable.
    Propose(ctx context.Context, e state.Entry) error
    // ReadIndex-served linearizable read of committed state.
    Read(ctx context.Context, fn func(state.StateMachine) error) error
    // Status exposes Raft leader/term/health for the admin surface.
    Status() Status
    Run(ctx context.Context) error // drives the Ready loop until ctx cancels
}

type Sharder interface { /* as in §9.2 */ }
```

### 10.7 Binary

`cmd/sentio-sequencer/` mirrors the structure of `cmd/housegate/`: flag parsing, age-encrypted config via [`pkg/secretsload`](../../../pkg/secretsload), signal-context wiring, a `/metrics` server via [`pkg/metricshttp`](../../../pkg/metricshttp), structured logging via [`pkg/log`](../../../pkg/log), then `sequencer.New(opts).Run(ctx)`. No domain logic in the binary.

---

## 11. Delivery phases

These are scoped against the storage design's P0–P4 ([§14](2026-06-22-storage-integrity-design.md)); this design adds the Sequencer-specific decomposition.

**P0 — freeze protocol surfaces.**
- Freeze the [§4.2](#42-new-types-introduced-by-this-design) JSON wire schemas and `json` tags.
- Export `replay.CanonicalDigest(domain, v)` (or add the `pkg/sequencer/internal` helper) so new commitments share one canonicalization profile ([§4.3](#43-hashing-and-canonicalization)).
- Freeze the two-signature plan ([§10.5](#105-promotion-and-the-signing-plan)): `PromotionCommand` signing payload canonicalization + `PromotionValidator` test vectors.
- Mountain-range accumulator construction + non-membership proof test vectors (storage-design P0 deliverable).

**P1 — sequencer + single Raft + INSERT promote.**
- `pkg/sequencer/state` `StateMachine` with the entry variants in [§10.3](#103-state).
- `pkg/sequencer/consensus` single-group `Node` over etcd-raft + bbolt, `Sharder` returning group 0.
- `pkg/sequencer/dedup` accumulator + high-water mark wired into the Sequencer subsystem.
- `pkg/sequencer/replay` orchestrator: `BuildJob` + `Dispatch` + `ThreeWayPromote` over reused `replay.Verifier`/`Executor`.
- `pkg/sequencer/promotion` secp256k1 `PromotionSigner` + CAS-base minting.
- INSERT-path end-to-end: submit → sequence → claim → replay → three-way → promote → `SafeSnapshotManifest` publish.
- Tests: unit (dedup, three-way predicate positive/negative, CAS-base replay), integration (3-node Raft, one-node failure preserves promotion throughput).

**P2 — bounded UPDATE/DELETE (mutation path).**
- Mutation barriers, affected safe-part discovery, scratch clone.
- Mutation third-check (recomputed-commitment match, [§6.3](#63-insert-vs-mutation-paths)).
- Admission caps for touched data.

**P3 — harden safe serving.**
- `pkg/sequencer/membership` audit hooks feeding the storage-design §13 audit.
- Replica read-set health scoring.

**P4+ — multi-Raft sharding and decentralization.**
- Replace the v1 `Sharder` with per-table/account group routing ([§9.2](#92-why-multi-raft-is-not-v1-correcting-the-draft)).
- The decentralized-phase challenge window ([storage design §11](2026-06-22-storage-integrity-design.md)).

---

## 12. Open questions

1. **L3/RC JSON wire fields** ([storage design §15 Q4](2026-06-22-storage-integrity-design.md), narrowed): the [§4.2](#42-new-types-introduced-by-this-design) sketches freeze the shape; the exact JSON field set, optional-vs-required, and `omitempty` policy are P0.
2. **Payload DA** ([storage design §15 Q6](2026-06-22-storage-integrity-design.md)): is the `PayloadStore` implementation backing `replay.Verifier` the consensus core's replicated state, an external DA layer, or both with a fallback? Affects `pkg/sequencer/replay` dispatch.
3. **Chain commitment** ([storage design §15 Q5](2026-06-22-storage-integrity-design.md)): does the L2 anchor store the full L3 block payload, a DA reference, or only a block/root commitment? Affects `promotion.Publish`.
4. **Raft transport** ([§9.1](#91-v1--single-raft-group-the-chosen-path)): gRPC transport for inter-Sequencer Raft messages, or the raw etcd-raft `Transport`? P1 decision.
5. **Snapshot frequency** ([§9.1](#91-v1--single-raft-group-the-chosen-path)): bbolt log-compaction cadence for the `StateMachine.Snapshot()`. Trades Raft log length against snapshot cost.
6. **zh-CN mirror** ([convention](#)): per repo bilingual-spec convention, generate `2026-06-30-storage-integrity-keeper-design.zh-CN.md` from this English source after P0 field freeze, keeping identifiers in English. Optional; does not affect the English source of truth.

---

## Verification

This document is self-checking against the requirements in the approved plan. The acceptance greps:

- **Must contain** — `Sentio Sequencer`, `ClickHouse Keeper`, `Ed25519`, `secp256k1`, `REPLACE PARTITION`, `three-way`, `multi-Raft`, `pkg/sequencer`, `statement_seq`, `statement_id`, `SafeSnapshotManifest`, `PromotionCommand` (all present above).
- **Must not contain** — `fork ClickHouse`, `root equality alone`, or any phrase collapsing `ClickHouse Keeper` into `PromotionCommand` authority (none present; the two Keeper roles are separated in [§1](#1-positioning-and-terminology) and never re-conflated).

## 13. References

- [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) — the parent spec; this document decomposes its §5.2 Sequencer role (called "Keeper" in that document's earlier wording; see the [naming note](#) at the top).
- [2026-06-10 multi-replica trust design](2026-06-10-multi-replica-trust-design.md) — row-id profile, ledger equation, accumulator background.
- [`pkg/replay`](../../../pkg/replay) — verifier core + manifest/receipt/attestation types reused verbatim ([§4.1](#41-reused-from-pkgreplay-do-not-redefine)).
- [`pkg/lthash`](../../../pkg/lthash) — the LtHash arithmetic `part_row_lthash` and `partition_commitment` use.
- [`pkg/auth`](../../../pkg/auth) — `RelaySigner` backs the `PromotionSigner` ([§10.5](#105-promotion-and-the-signing-plan)).
- [`pkg/replicationproxy`](../../../pkg/replicationproxy) — the optional L4 ClickHouse-Keeper passthrough; network isolation, not integrity.
- [`etcd-io/raft`](https://github.com/etcd-io/raft) — the Raft library v1 builds on ([§9](#9-consensus-and-ha)).
- [etcd-dev MultiRaft discussion](https://groups.google.com/g/etcd-dev/c/cq88rpcxvm8) — why multi-Raft is not native to the library ([§9.2](#92-why-multi-raft-is-not-v1-correcting-the-draft)).
- [TiKV multi-Raft design](https://www.pingcap.com/blog/design-and-implementation-of-multi-raft/) and [CockroachDB scaling-raft](https://www.cockroachlabs.com/blog/scaling-raft/) — prior art for the multi-Raft work deferred to P5+.
- [ClickHouse Keeper docs](https://clickhouse.com/docs/guides/sre/keeper/clickhouse-keeper) — the unrelated C++ coordination service ([§1.1](#11-clickhouse-keeper-not-us-not-go)).
