# Multi-source storage-integrity network — Design

**Status:** design, approved section by section on 2026-10-10; umbrella spec. Each sub-project (§14) gets its own implementation plan in the repository that owns it.

**Repositories:** arbiter-proto, arbiter-core, arbiter (private), housegate, sentio-node, production.

**Related:** [2026-06-22-storage-integrity-design.md](2026-06-22-storage-integrity-design.md) (route A, the Keeper/Arbiter role), [2026-06-30-sentio-arbiter-design.md](2026-06-30-sentio-arbiter-design.md) (source selection §5.4, the `Sharder` seam §3.4/§10.6), [2026-09-23-dynamic-si-table-set-design.md](2026-09-23-dynamic-si-table-set-design.md) (the table registry; its D1 is replaced here), [2026-10-09-user-agent-si-writes-design.md](2026-10-09-user-agent-si-writes-design.md) (agent discovery D18, upstream switch D19, client lanes).

**Revisions read:** housegate `origin/main` 2959e4a, arbiter 5ea60c8, arbiter-core 050490c, arbiter-proto 2eb3917, sentio-node ff336f1, production 4ad1968cd. Cross-repository references are written `repo:path:line` at those refs; bare paths are housegate. Live facts were read from devnet2 on 2026-10-09.

**Terminology.** A *track* is the sequence of L3 blocks owned by one SI indexer (§5). The design discussion called it a lane; this document says track so it cannot be confused with the client lanes of the 2026-10-09 spec.

## 1. Goal

Every indexer of a Sentio network can join that network's storage-integrity (SI) layer, so that a table created on any SI indexer after the indexer's activation point is signed, sequenced, verified and promoted like the tables of devnet2's indexer-a today. One L2 network (devnet2, testnet, mainnet) keeps exactly one L3 chain and one arbiter Raft group, and the agent keeps working as it does today. A stuck, slow or fraudulent indexer must not stop the safe progress of the others. The first deployment target is enrolling devnet2's indexer-b; the end state admits third-party indexers.

## 2. Measured facts this design rests on

- **Sources are interchangeable today.** The FSM picks a statement's source by hash over the Active SNODE pool, independent of the target table (`arbiter:fsm/select.go:41-51`). An RC from any other node is refused (`arbiter:fsm/apply.go:392-395`) and a parked RC with another source is discarded at admission (`arbiter:fsm/admission.go:146-152`). HouseGate always prepares on its co-located SNode in parallel with the submit and requires the bound source to be that SNode (`pkg/storageintegrity/intake.go:1247`); its frontier is keyed by the single configured `expected_source` (`intake.go:727-732`). With N SNodes about (N−1)/N of every indexer's statements would be refused, and their blocks would wait for an RC forever.
- **One writer.** `max_writers` caps non-evicted SNODE registrations; values above one "do not implement multi-source routing or multi-writer state-root semantics" (`arbiter:fsm/state.go:94-98`, gate at `arbiter:fsm/apply.go:51-64`).
- **One SI indexer.** `TableRegistryParams.SIIndexerID` is set once (`arbiter-core:table_registry.go:17-25`) and no consensus update may change the registry parameters (`arbiter:fsm/consensus_updates.go:120-130`). A `TableCreated` is SI iff its database's indexer at the event block equals that id (`arbiter:tableregistry/chain.go:77-101`), and the Legacy seed is one-shot at `activation_block` (`chain.go:107-131`). Incarnations record no owner.
- **Safe progress is one contiguous prefix.** It stops at the first block holding any non-Safe statement, and Rejected is not Safe (`arbiter:fsm/reads_work.go:42-75`). Promotions are issued only up to the prefix + 1 and never while a manifest is owed (`arbiter:fsm/apply.go:583-598`). At most one table-set transition block may be unpublished, because every block pins the published watermark (`arbiter:fsm/table_set_transition.go:232-249`). A consensus update needs every promotion acknowledged, no pending cleanup and no promoted unsafe part (`arbiter:fsm/consensus_updates.go:52-66`).
- **Verification is already base-relative.** Every block pins the published manifest at seal time and is judged against it alone; claims are per (table, partition) and LtHash is additive (`arbiter:fsm/threeway.go:35-112`), so blocks of different tables do not interfere arithmetically.
- **Promotion and membership trust every caller.** Promotions are sent to every connected SNode (`arbiter:orchestrator/promotion.go:168-191`). The FSM does not check which node acknowledged, and an `Applied:false` acknowledgement from any node consumes the promotion (`arbiter:fsm/apply.go:605-635`). The gRPC server is a plain `grpc.NewServer()` (`arbiter:cmd/arbiter/grpc.go:47`); RCs and acknowledgements are unsigned, and any caller may register a node or mark any node active (`arbiter:server/membership.go`).
- **Authority work is re-signed at dispatch.** The FSM verifies a promotion's or cleanup's authority JWS against the current authority when the work is issued (`arbiter:fsm/apply.go:567`, `:738`) and records it. Every resend is signed again by the leader with the current {network id, genesis snapshot id, epoch} (`arbiter:orchestrator/promotion.go` `streamPromotion`, `arbiter:orchestrator/cleanup.go` `streamCleanup`, `arbiter:orchestrator/signer.go:25-39`). Acknowledgements are not epoch-checked.
- **Every node holds every table.** The table-set reconciler materialises every registry incarnation on every node (`arbiter-core:dataplane/tableset/reconciler.go:383`) at Keeper path `/sentio/<keeper_shard_id>/unsafe/<db>__<table>`, replica name = node id (`arbiter-core:dataplane/ddl/naming.go:42-46`). The SNode attributes candidate parts by diffing the table's active parts before and after its own write (`arbiter-core:snode/staged.go:160-199, 319-326`), which is sound only while nothing else adds parts to that table. Verifiers byte-scan their own replica (`arbiter-core:verifier/backends.go:93-143`).
- **The agent is already multi-host.** It resolves a database's hosting indexer and asks that indexer for table status and network id (`pkg/network/rpc.go:284-326`), keeps one seq/lane store per network and signer, and switches the session upstream to the hosting indexer (2026-10-09 spec D18/D19). The accumulator allows 64 open gap ranges per subject (`arbiter:accumulator/profile.go:22-26`), so statement-id uniqueness must stay network-wide in one accumulator.
- **sentio-node governs one indexer.** A database is governed iff `IndexerID == SIIndexerID` and it is not a processor database, and a key the registry records is answered from the registry whatever its host (`sentio-node:storageintegrityadapter/tablestate/snapshot.go:64-92`); unrecorded tables of a governed database are Pending (`snapshot.go:149-154, 186-193`). Its config requires a non-empty genesis `table_ids` and `node_id == expected_source` (`sentio-node:config/config.go:100-120`), and an SNode registration failure rolls back the startup transaction (`sentio-node:standalone/standalone.go:1026-1028`).
- **Databases do not move.** `createUserDatabase` returns the indexer the contract chose and `systemCreateProcessorDatabase` takes one; the `IDatabases` interface has no call that moves a database (`sentio-node:bindings/bindings.go:1905`).
- **Anchoring is one transaction per block:** `anchor(l3BlockHash, stateRoot)` (`arbiter:orchestrator/promotion.go:52-108`, `arbiter:contracts/AnchorRegistry.sol`), with a poster allowlist.
- **Manifest consumers.** The verifier replays a job only if its block is above the base manifest's `safe_block_seq` (`pkg/replay/verifier.go:101`); the manifest root covers `safe_block_seq` and the tables (`pkg/replay/roots.go:65-87`).
- **devnet2 (2026-10-09).** Epoch 2; `max_writers` 1; `table_registry` {chain 7892301, contract 0x5557d2cc…3614, `si_indexer_id` 0, `activation_block` 5508931, `safe`}; `client_lanes` {256}; one active SNode (`snode-1`, indexer-a) and three verifiers; safe watermark = sealed tip. On chain, `devnet101`, `devuser2` and `devuser3` belong to indexer 0 (indexer-a) and `devuser1`, `devuser3b` to indexer 1 (indexer-b, signer 0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3, storage RPC port 33003). indexer-a answers `ordinary` for indexer-b's tables. indexer-b runs sentio-node 36b5c0a (housegate v0.13.0, arbiter-core v0.6.0), sidecar v0.12.0 and rewriter 0.14.0, has no SI RPC, and its ClickHouse holds only `devuser1.t` (0 rows) and two views; its state mirror has no address(0) grant.

## 3. Scope

In scope: enrolling any number of indexers into one network's SI layer; table ownership; owner-bound sources; authenticated SNodes and verifiers; an owner-scoped data plane; per-indexer liveness isolation (tracks) with quarantine; the devnet2 rollout through indexer-b.

Out of scope, with the requirement recorded in §17: cross-operator Keeper and interserver transport; DA write authentication and quotas; per-indexer admission rate limits; verifier sharding; TLS on arbiter endpoints. Also out of scope: cross-indexer per-clause SI reads (they stay refused, §9); de-enrolment; moving a database between indexers; making the snapshot-query lane track-aware.

## 4. Decisions

**D1 — One L2 network, one L3 chain, one arbiter Raft group.** All SI indexers of a network share one chain, one accumulator, one governance, one AnchorRegistry, one DA and one Keeper root. Rejected: one Raft group per indexer through the `Sharder` seam with `keeper_shard_id` as group id — isolation would come by construction, but per-group accumulators break network-wide statement-id uniqueness for an agent writing several hosts through one lane (the 64-range gap budget), and governance, anchoring and membership would multiply per group. Rejected: one SI network per indexer, for the same cost.

**D2 — The agent is unchanged.** `keeper_shard_id` stays 0; discovery, signing, lanes and the upstream switch work as in the 2026-10-09 spec.

**D3 — Enrolment by governance.** A new consensus parameter `si_indexers` is an append-only list of `{indexer_id, activation_block, signer, snode_node_id}`, changed only by authority-signed consensus updates. Each indexer has its own activation block. The founding indexer of an existing registry is restated as entry 0, so `table_registry` stays unchanged.

**D4 — A table's owner is its hosting indexer.** The owner is the database's `indexerId` at the `TableCreated` block; it is recorded on every incarnation and never changes.

**D5 — A statement's source is its owner's SNode.** This replaces hash selection once `si_indexers` is set. Because an indexer's ingress only admits its own Active tables, the selected source is always the co-located SNode, and route A in HouseGate is unchanged.

**D6 — An SNode speaks with its indexer's on-chain signer key.** Every SNode message the FSM accepts (registration, activation, RC, promotion and cleanup acknowledgements, purge reports) is signed with the secp256k1 key the indexer already holds, domain-separated by purpose, network id and genesis snapshot id. No new key is introduced.

**D7 — The verifier set is governed.** A consensus parameter `verifiers` lists `{node_id, ed25519_pubkey}`; only listed verifiers may register, and their registration and activation are signed.

**D8 — The data plane is owner-scoped.** An SNode materialises, promotes and cleans only its own tables; promotions and cleanups go only to the owner's SNode and are acknowledged only by it. Verifiers keep a replica of every SI table (v1).

**D9 — Tracks isolate liveness (stage 2).** Every block belongs to exactly one owner's track, by sealing the open block when the next admitted statement has a different owner. The safe prefix, the promotion frontier and transition publication are computed per track, and the manifest records each track's frontier.

**D10 — Quarantine.** Authority-signed commands quarantine and release an indexer. A Rejected block quarantines its track automatically; release must acknowledge the rejected blocks explicitly before the track can pass them.

**D11 — Naming and identities are unchanged.** Keeper paths stay `/sentio/0/unsafe/<db>__<table>`, physical names stay `hg_*.<db>__<table>`, and row-id derivation, the statement-id grammar, `network_id` and `keeper_shard_id = 0` stay as they are. Database ids are globally unique, so table paths of different owners never collide.

**D12 — Two stages.** Stage 1 (§6, §8–§11) makes multi-source correct: D3–D8. Stage 2 (§7) adds tracks and quarantine: D9–D10. Between them liveness is coupled — one stuck indexer stalls the safe progress of all — so only self-operated indexers may be enrolled before stage 2. Third-party enrolment additionally requires the gate in §17.

## 5. Concepts

- **SI indexer:** an indexer with an entry in `si_indexers`. It is *active* from its `activation_block` on.
- **Owner:** the SI indexer hosting a table's database, recorded on the incarnation (D4).
- **Source:** the owner's SNode, named by the entry's `snode_node_id` (D5). Exactly one per SI indexer.
- **Track (stage 2):** the blocks whose header names one owner, in chain order (D9). Table-set transition blocks belong to the track of the tables they change.

A statement's path is the one route A defines, with the owner fixed by the target table: the agent asks the hosting indexer for status, signs, and switches its session to that indexer; the indexer's ingress prepares on its own SNode while submitting; the FSM admits the statement and binds the owner's SNode as source; the SNode registers a signed RC; verifiers replay and byte-scan the owner's parts from their replicas; after quorum and anchor finality the FSM promotes on the owner's SNode only.

## 6. Stage 1: consensus layer (arbiter, arbiter-core, arbiter-proto)

### 6.1 Parameters

- `si_indexers`: nil until first set; then a list sorted by `indexer_id`. Entry fields: `indexer_id` (uint64), `activation_block` (uint64, ≥ 1), `signer` (lowercase 0x address of the indexer's IndexerRegistry signer), `snode_node_id` (non-empty).
- The first update that sets it requires `table_registry` to be set (earlier or in the same update) and requires entry 0 to equal `{table_registry.si_indexer_id, table_registry.activation_block}`.
- Later updates may append entries and may change an existing entry's `signer` only. `indexer_id`, `activation_block` and `snode_node_id` of an existing entry never change, and entries are never removed.
- `verifiers`: nil until first set; then a list of `{node_id, ed25519_pubkey}` that updates may add to and remove from. Removing an entry evicts that node.
- `max_writers` must be at least `len(si_indexers)`; the existing writer-capacity check stays as a second guard.
- Every update carries both lists completely, like the authority set and `client_lanes` today. Their digests join the params digest only once set, so the digest of a network that never sets them is unchanged.

### 6.2 Enrolment

An update that appends an entry is an enrolment. It is accepted only if:

- the entry's `indexer_id` is new, and its `snode_node_id` is unused by every other SNode entry, every verifier entry and every registered node;
- `activation_block` is greater than the registry's committed L2 cursor block, so every event an auditor will later judge under the new entry was not yet consumed when the entry was committed (§6.3);
- the update carries an *SNode enrolment statement* — a JWS (ES256K) with purpose `arbiter-snode-enrollment-v1` over `{network_id, genesis_snapshot_id, indexer_id, snode_node_id}` — whose signer recovers to the entry's `signer`. It proves the operator of the on-chain indexer consents to, and controls, this SNode identity.

A `signer` rotation carries a new enrolment statement signed by the new signer. Enrolment is a consensus update, so in stage 1 it needs the existing drained-authority-work moment (§2); stage 2 narrows that (§7.4). `arbiter-admin consensus update` gains repeatable `--si-indexer` and `--verifier` flags and an enrolment-statement input, and its preflight (not consensus) probes the new SNode's capability endpoint and refuses an `activation_block` less than about one hour of L2 blocks past the safe head.

### 6.3 Table registry

- `AddTable` carries `owner_indexer_id`, resolved by the leader from the database's indexer at the `TableCreated` block (the watcher's existing resolver already caches it) and re-derived by every auditor. Apply refuses an `AddTable` whose owner has no entry or whose `TableCreated` block is below the owner's `activation_block`.
- Membership generalises dynamic-table-set D3: a `TableCreated` opens an SI incarnation iff its database's indexer at the event block has an entry and the event block is at or after that entry's `activation_block`. Because entries are append-only and every new `activation_block` lies beyond the committed cursor, deriving with the current list reproduces every past decision.
- The Legacy seed becomes per indexer: `SeedLegacyTables` names an `indexer_id` and records that indexer's tables active at `activation_block − 1`; the registry tracks seeded indexers as a set. Apply refuses a second seed for an indexer and refuses any `AddTable`, `RetireTables` or cursor advance at or beyond an entry's `activation_block` while that entry is unseeded, so the watcher seeds before it consumes any event the seed must precede.
- Every incarnation that exists when `si_indexers` is first set gets the founding indexer as owner; on devnet2 that is every incarnation, all of which belong to indexer 0's databases.
- If the watcher observes a database whose indexer differs from the one it resolved earlier, it halts with a new reason; ownership never changes silently.
- The registry views served to followers (`GetTableRegistry`, `WatchTableRegistry`) carry the owner of each incarnation, the `si_indexers` list and the seeded set.

### 6.4 Admission and source binding

Once `si_indexers` is set, step 6 of admission binds `SourceNode = si_indexers[owner].snode_node_id`, where `owner` is the target's live incarnation, which step 4 already loads. Hash selection remains only for replaying log entries committed before the first `si_indexers` update; on devnet2 both rules name `snode-1`, so the switch changes no outcome.

If the owner's SNode is not registered and Active in committed state, admission returns a new code `SOURCE_UNAVAILABLE` and changes nothing; HouseGate treats it as a retryable, session-preserving refusal and removes its already-prepared parts through its existing terminal-submit-rejection path. This replaces today's behaviour of sequencing a statement whose RC can never arrive.

### 6.5 Signed SNode messages and membership

- After activation (§6.7) the FSM accepts an SNode message only with a valid signature by the owning entry's `signer`, carried beside the message in the Raft command and computed over the message's canonical digest with a per-kind purpose plus `network_id` and the genesis snapshot id. This covers `RegisterNode` and `MarkActive` for the SNODE role, `RegisterRC` (including parked RCs, which are verified when parked), `RecordPromotionAck`, `RecordCleanupAck` and `SubmitTablePurged`. A forged or replayed RC can no longer occupy a statement's first-wins slot.
- Registration and activation requests carry a `registration_seq` that must exceed the last one applied for that node, so a replayed registration cannot push a running node back to Syncing.
- An SNODE registration must use an enrolled `snode_node_id`. The grandfathered `snode-1` registration on devnet2 stays valid; its later messages must be signed.
- Once `verifiers` is set, a VERIFIER registration must match a listed `{node_id, ed25519_pubkey}` and is signed by that key with a `registration_seq`, as are its `MarkActive` and its purge reports. Evidence signatures are unchanged (they are already ed25519).
- `EvictNode` gets an authority-signed entry point in the server and `arbiter-admin`.
- The selection floor of three active non-source verifiers is unchanged.

### 6.6 Promotion, cleanup and purge

- The orchestrator sends a promotion or cleanup only to the owner's SNode.
- Apply accepts a `RecordPromotionAck` or `RecordCleanupAck` only from the owner's SNode; an `Applied:false` from any other node no longer consumes the promotion.
- A purge completes when the owner's SNode and every non-evicted verifier have reported, instead of every non-evicted SNode and verifier.
- Promotion and manifest publication otherwise stay as they are in stage 1, including the global safe prefix.

### 6.7 Snapshot v19 and activation

Snapshot v19 adds `si_indexers`, `verifiers`, the incarnation owner, the seeded set and per-node registration sequences. As with v18, it is written only once `si_indexers` is committed, so a voter can roll back until then.

Activation is one authority-signed update that sets `si_indexers` (on devnet2: `[{0, 5508931, 0x22a67f498e669cbde18a0eecfff74a72c48331f4, snode-1}]`) and `verifiers` (the three running verifiers' ids and keys). It is irreversible. The leader refuses it unless every voter passes the capability probe and every registered data-plane node advertises the new feature `signed_claims_v1`, with the same leader-local feature book the client-lanes gate uses. From its commit on, unsigned or unauthorised SNode and verifier messages are refused.

## 7. Stage 2: tracks and quarantine

### 7.1 One owner per block

- The open block records the owner of its statements. When admission accepts a statement whose owner differs from a non-empty open block's, Apply first seals the open block exactly as `SealL3Block` would, then opens a new one for the new owner. The seal happens after every admission check has passed (including the gap-budget check) and before the statement's spent-id insert, so the sealed header's `spent_ids_root_after` excludes the new statement.
- Blocks stay contiguous statement-seq ranges, so the verifier, `ReplayJob`, the L3 block views and the offline state-digest tool need no change.
- The header gains `track: {indexer_id}`, a pointer so that indexer 0 is not dropped by `omitempty`; it is part of the chain-hash preimage for every block sealed after activation and absent from earlier ones. A table-set transition block carries the track of the tables it changes and contains changes of one owner only; the sealer seals one transition block per owner with pending changes.
- Interleaved traffic from several indexers makes blocks smaller, and each block costs one anchor transaction plus three replays. The leader therefore groups submissions it receives within a short window (default 50 ms, configurable, not a consensus parameter) by owner before proposing them. If block count becomes the bottleneck, a later change may give each owner its own open block with an explicit statement list in the header; track semantics would not change.

### 7.2 Per-track progress

- The safe prefix of track T is the last T block such that every T block up to it is safe; a statement block is safe when all its statements are Safe, a transition block when its own quorum, finality and last-mergeable are latched, and an acknowledged Rejected block (§7.5) counts as passed.
- The global `safe_block_seq` remains the contiguous prefix over all blocks and is a conservative value: everything up to it is safe, but tracks may be ahead of it.
- The promotion frontier is per track: a statement is promotable only if its block is at most the first T block after T's prefix.
- The manifest-debt gate is per track: while T's prefix is ahead of T's frontier in the latest published manifest, no new promotion is issued for T. Other tracks keep promoting.
- At most one transition block per track may be unpublished. Pending→Active and Retiring→Purging advance when the published manifest's frontier for the table's track covers the transition block.
- Tables and partitions of different owners are disjoint, so tracks never contend for a partition.

### 7.3 Manifest

- `replay.SafeSnapshotManifest` gains `track_frontiers: [{indexer_id, safe_block_seq}]`, sorted and unique. `ComputeManifestRoot` includes it only when non-empty, so the roots of existing manifests do not change. Validation requires each track's frontier to be at least that track's last block at or below `safe_block_seq`.
- A manifest contains exactly the effects of every block up to `safe_block_seq` and of every track's blocks up to its frontier.
- The leader still publishes one manifest chain. A publication starts from the parent manifest and replaces the tables and frontier of every track whose prefix advanced since the parent with that track's current state. Every other track keeps its tables and frontier from the parent.
  - The per-track debt gate (§7.2) guarantees that a track with a newly advanced prefix has no promotion beyond it, so its current state is exactly its new prefix.
  - Carrying the other tracks over from the parent keeps the manifest exact even while one of them has a block partly promoted (some partitions acknowledged, others not). Taking the live state of such a track would publish its partial effects under the old frontier.
- The verifier's check that a job's block is above the base's `safe_block_seq` still holds, because a block is always sealed above the global prefix of the manifest it pins.
- The manifest format change is a housegate `pkg/replay` change; every verifier and SNode must run it before activation (§7.6).

### 7.4 Governance and quarantine

- `QuarantineIndexer{indexer_id, expected_generation, reason}` and `ReleaseIndexer{indexer_id, expected_generation, acknowledged_rejected_blocks}` are authority-signed commands outside `ConsensusParamsUpdate`, so the drain gate cannot block them. The authority JWS covers every field plus the current authority context (network id, genesis snapshot id, epoch).
- Each SI indexer has a durable `quarantine_generation`, starting at 0. Every applied quarantine, release and automatic quarantine (§7.5) increments it.
  - Apply refuses a command whose `expected_generation` differs from the current generation. A release must therefore name the generation of the quarantine it ends, and a quarantine the generation of the state it changes.
  - Epoch binding alone is not enough: neither command advances the consensus epoch, so without the generation a signed command could be replayed within the same epoch — an old quarantine after a release, or an old release against a later quarantine.
- While an indexer is quarantined:
  - admission refuses its tables with a new non-retryable code `INDEXER_QUARANTINED`;
  - its pending promotions and cleanups stay recorded but are not dispatched (cancelling them could leave the SNode's local base ahead of the FSM's if an acknowledgement was lost);
  - its track is excluded from the consensus-update drain gate.
- The drain gate applies to every other track as today. In-flight promotions of healthy tracks last seconds, so updates go through between them.
- On release the orchestrator re-dispatches both kinds of suspended work, promotions and cleanups, through its existing resend paths. Those paths re-sign every dispatch with the current authority context (`arbiter:orchestrator/promotion.go` `streamPromotion`, `arbiter:orchestrator/cleanup.go` `streamCleanup`, context from `arbiter:orchestrator/signer.go:25-39`).
  - The FSM records the authority JWS only when work is issued and does not check an epoch when it is acknowledged. Suspended work therefore survives an authority rotation without a new command.
  - Re-sent work is idempotent. The SNode applies a re-sent promotion through its per-partition watermark and last-acknowledgement record. A re-sent cleanup only drops parts, and dropping an already-dropped part is tolerated (`arbiter-core:snode/cleanup.go:27-31`); the FSM accepts an acknowledgement for a cleanup it has already cleared as a no-op.

### 7.5 Rejected blocks

When a challenge resolves a block of track T as Rejected, Apply quarantines T with the block as reason and increments T's `quarantine_generation`. A release signed before that point names an older generation and is refused, so the operator must review the new rejection before releasing. The track cannot pass the block until a `ReleaseIndexer` lists it in `acknowledged_rejected_blocks`. Release then:

- counts the block as passed for T's prefix;
- schedules cleanup of its statements' candidate parts on T's SNode.

Rejected data never reached safe, so passing it is sound. Today a single rejected block stops the safe progress of the whole network.

### 7.6 Snapshot v20 and activation

Snapshot v20 adds the open block's owner, the header track, `tracks_enabled`, the quarantine state with each indexer's `quarantine_generation`, and the acknowledged rejected blocks; it is written only once `tracks_enabled` is committed. `tracks_enabled` is a raise-only parameter set by one authority-signed update. The update requires:

- a drained chain: the open block is empty and the global safe prefix equals the sealed tip;
- every voter passing the capability probe and every verifier and SNode advertising `track_manifests_v1`.

Every block sealed after it carries a track.

### 7.7 What stays global

- The table-set transition admission fence: it lasts only until the leader seals the transition.
- The snapshot-query reservation barrier: default off and not wired; it keeps waiting on the global prefix and so on every track.
- Verifier liveness: with two of three verifiers down nothing verifies.
- Raft log, DA and verifier compute are shared, so per-indexer quotas are needed before third parties join (§17).

## 8. Data plane (arbiter-core)

- **Owner filter.** The SNode role knows its `indexer_id`, and its reconciler processes only incarnations owned by it. Its genesis table set is the genesis tables it owns, possibly empty. Verifiers reconcile every incarnation.
- **Own tables only.** The SNode's source-claim root, prepared-statement lookup, promoted-unsafe reads and decommissioned-replica sweep cover only its own tables. Each table's replica set is therefore "its owner's SNode plus the verifiers", and the before/after part diff stays sound (§2).
- **Signing.** The SNode signs every message of §6.5 with the indexer key handed in by sentio-node, and keeps a persisted `registration_seq`.
- **Source ClickHouse profile.** Every SI indexer's ClickHouse must satisfy it:
  - membership of the network's Keeper ensemble under the network's Keeper root;
  - interserver (port 9009) reachable from every verifier ClickHouse;
  - the `hg_safe` / `hg_unsafe` / `hg_promote` databases with their DDL-pinned settings.
  The network chart no longer owns an indexer's ClickHouse (§12).
- **Verifiers.** v1 keeps three verifiers holding a replica of every SI table, fetching parts from each owner's ClickHouse; storage grows with the network's SI data. Verifier sharding is recorded in §17.
- **DA.** It stays one network DA, written by each indexer's ingress and SNode and read by verifiers, with the current channel trust while all indexers are self-operated.

## 9. housegate

- Route A is unchanged: the ingress admits only the host's own Active tables, so the FSM's source is the co-located SNode and `expected_source` stays the local node id.
- The ingress maps `SOURCE_UNAVAILABLE` to a retryable, session-preserving refusal (stage 1) and `INDEXER_QUARANTINED` to the non-retryable code 392 (stage 2).
- Every SI host runs `sipeerguard`, so a peer-trusted statement that names the ordinary physical table of a governed table is refused on every SI host alike. Cross-indexer per-clause reads and writes of SI tables stay refused; whole-session forwarding (`--database` / `USE`) remains the supported path.
- `pkg/sitable` documentation: Ordinary means "not governed on this host", which now includes tables owned by another SI indexer.
- `pkg/replay` gains `track_frontiers` (§7.3) with new root test vectors.

## 10. sentio-node

- **Configuration.** Per indexer: the SNode node id from its entry, state directory, its own source ClickHouse, and the indexer private key it already holds for signing. Network-wide: network id, schema snapshot id, executor profile, authority, arbiter peers and DA. `table_ids` becomes the genesis tables this indexer owns and may be empty.
- **Governance predicate.** A database is governed iff it is hosted by this indexer, this indexer has an entry in `si_indexers`, and it is not a processor database. Before this indexer is seeded, a table created before its `activation_block` stays Ordinary, as the founding indexer's do today. A recorded registry key is answered from the registry only when its owner is this indexer; tables owned by another SI indexer are Ordinary here. Answering them from the registry would let this host's rewriter turn reads of a peer's SI table into reads of its own empty `hg_safe`. Default deny and the Legacy fallback read this indexer's own seeded flag and activation block.
- **Enrolment.** A CLI subcommand signs and prints the SNode enrolment statement with the indexer key. A node configured for SI that starts before its enrolment commits keeps retrying registration instead of failing startup; until then its tables are Ordinary and writes take the ordinary path.
- **RPC.** `sentio_getStorageIntegrityInfo` reports `si_indexer_id` as this indexer's id when it has an entry, else null, and adds `enrolled`. Stage 2 adds `quarantined`, and the host reports its own Active tables as Refused while quarantined.
- **Schema network id.** On a node of an SI network, the schema declarer's network id must equal the arbiter network id; startup refuses a mismatch instead of relying on `physical_database` happening to match.

## 11. Agent

No change (D2). An agent writing several hosts opens one session per host, as today. Each indexer's driver sidecar is configured as indexer-a's is: v0.15.0 or later, `networkStateSource` pointing at the host's own storage RPC.

## 12. Production

- Split the per-network chart from the per-indexer source ClickHouse.
  - The network release keeps the arbiter, verifiers with their ClickHouses, DA and the Keeper root.
  - Each indexer's ClickHouse receives the source profile (§8).
  - indexer-a keeps its dedicated source ClickHouse.
  - For indexer-b, the runbook decides between applying the profile to `clickhouse-devnet2-node-b` and creating a new source ClickHouse; its data is negligible either way.
- Generalise the sentio-node SI overlay so each indexer's overlay differs only in its own values: node id, ports, storage RPC, ClickHouse, state paths.
- Render tests check unique node ids across overlays, owner-specific genesis sets, and that every overlay targets the same network release.
- SNode node ids are unique, stable and never reused. The runbook fixes indexer-b's.

## 13. devnet2 rollout

Stage 1:
1. Release and pin the stage-1 builds. Replay the live voters' logs offline with the state-digest tool under the old and new builds; they must be identical.
2. Upgrade verifiers, then replace voters one at a time (followers first, leader last). Behaviour is unchanged before activation.
3. Upgrade indexer-a's sentio-node; it starts advertising `signed_claims_v1`. Behaviour is unchanged.
4. Commit the activation update (§6.7) and verify that indexer-a's signed INSERTs still reach safe.
5. Prepare indexer-b:
   - source ClickHouse profile;
   - images aligned with indexer-a (sentio-node, rewriter, sidecar), after the read-only address(0) check, which was already empty on 2026-10-09;
   - SI overlay;
   - signed enrolment statement.
6. Commit the enrolment update: append indexer 1 with `activation_block` about 1800 L2 blocks past the safe head, signer 0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3, and the SNode node id the runbook fixes (§12); set `max_writers` to 2. indexer-b's SNode registers and activates.
7. At indexer-b's activation block the watcher seeds its Legacy tables (`devuser1.t` among them).
8. End to end:
   - create a table in `devuser1`; it is Pending, then Active;
   - a signed INSERT through an agent switched to indexer-b reaches safe and reads back;
   - DROP, retire and purge it;
   - indexer-a's promotions go only to `snode-1`;
   - indexer-a's writes are unaffected.

Between stages, a stuck indexer-b stalls every indexer's safe progress (D12); the runbook records it.

Stage 2:
1. Upgrade the data plane (verifiers, then SNodes), then the voters.
2. Drain the chain and commit `tracks_enabled`.
3. Stop indexer-b's sentio-node and check that indexer-a's writes still reach safe.
4. Exercise quarantine and release.
5. Exercise the rejected-block path only in the integration environment, with fault injection.

## 14. Sub-projects and order

Stage 1:
1. **arbiter-proto** — `si_indexers`, `verifiers`, the enrolment statement, signatures and registration sequences on the commands of §6.5, `owner_indexer_id` on `AddTable`, the registry view additions, `SOURCE_UNAVAILABLE`, the `signed_claims_v1` feature, and the authority-signed eviction.
2. **arbiter-core** — wire converters and normalisation, SNode signing and `registration_seq`, the owner filter in the reconciler, and owner-scoped SNode reads.
3. **arbiter** — the FSM changes of §6, snapshot v19, orchestrator routing, server authentication, the watcher and auditor changes, and `arbiter-admin`.
4. **housegate** — the `SOURCE_UNAVAILABLE` mapping and the `sitable` documentation.
5. **sentio-node** — §10, stage-1 items.
6. **production** — the chart split, the overlay generalisation, indexer-b's values, render tests, and the stage-1 runbook.

Stage 2:
7. **housegate** — `track_frontiers` with root vectors; the `INDEXER_QUARANTINED` mapping.
8. **arbiter-proto / arbiter-core** — header `track`, the quarantine commands with their generation, and `track_manifests_v1`.
9. **arbiter** — the FSM changes of §7, snapshot v20, the grouping window, and `arbiter-admin`.
10. **sentio-node** — quarantine reporting.
11. **production** — the stage-2 runbook.

## 15. Testing

- **FSM, stage 1:**
  - owner-bound selection and `SOURCE_UNAVAILABLE`;
  - signatures: wrong key, wrong network, replay, parked RC;
  - `registration_seq` replay;
  - acknowledgement origin and `Applied:false` from a non-owner;
  - enrolment constraints: cursor, statement, uniqueness, signer rotation;
  - per-indexer seed ordering;
  - the owner-halt reason;
  - v19 restore and its refusal by an older binary;
  - a state-digest replay of devnet2's log that is unchanged before activation.
- **FSM, stage 2:**
  - track independence: a track with an RC-less block does not stop another track's promotions, manifests or transitions;
  - determinism of owner-switch sealing, including `spent_ids_root_after`;
  - manifest exactness under interleaved tracks, including a publication triggered by one track while another has a block partly promoted;
  - per-track transition limits;
  - quarantine, automatic quarantine on Rejected, and release with acknowledgement;
  - replay of the quarantine commands:
    - an old `QuarantineIndexer` replayed after a release is refused;
    - an old `ReleaseIndexer` replayed after a later quarantine is refused;
    - a release signed before an automatic quarantine is refused;
  - an authority rotation while a quarantined track holds both a pending promotion and a pending cleanup: after release both are re-dispatched under the new authority and acknowledged;
  - the drain gate with a quarantined track;
  - v20.
- **housegate:** `track_frontiers` root vectors and manifest validation.
- **Integration** (`chpipeline` harness):
  - two SNodes with two source ClickHouses and three verifiers sharing one Keeper;
  - concurrent writes to both owners' tables;
  - stopping one SNode stalls both (stage 1) or only its own track (stage 2).
- **sentio-node:** governance predicate, owner filtering of recorded keys, degraded start before enrolment.
- **production:** render tests of §12.

## 16. Risks

- Two irreversible consensus upgrades (v19, v20). The strict decoder requires every voter to upgrade before any new command is proposed.
- Stage 1 couples liveness across indexers (D12).
- Verifier storage and replay load grow with the network's SI data until verifier sharding lands.
- All indexers share one Keeper ensemble while self-operated.
- Interleaved writes make smaller blocks and more anchor transactions (§7.1).
- An indexer's hot key signs its SNode messages. A leaked key lets an attacker forge that indexer's claims and acknowledgements, but forged data still fails verification, and the damage stays within its track once stage 2 is live.
- Immutable database ownership is an assumption about the contract; the watcher halts if it is ever violated (§6.3).

## 17. Third-party enrolment gate

A third-party indexer may be enrolled only after all of these hold:

- stage 2 is active;
- arbiter endpoints serve TLS;
- the cross-operator replication transport exists, giving a source ClickHouse authenticated, isolated Keeper access that can write only its own tables' paths, and giving verifiers authenticated interserver fetches;
- DA writes are signed by the indexer signer and subject to per-indexer quotas;
- per-indexer admission rate limits exist;
- verifier capacity, or verifier sharding, is sized for the added data.

Each is a separate design.

## 18. Changes to earlier decisions

- Dynamic SI table set D1 and §3 ("one SI indexer per network", "more than one SI indexer per network" out of scope) are replaced by D3 here. Its D10 (default deny) applies per enrolled indexer after that indexer's activation block, and its Legacy seed becomes per indexer.
- Arbiter design §5.4 (hash selection over the writer pool) applies only before `si_indexers` is set. The single-writer gate becomes "`max_writers` ≥ enrolled indexers".
- The data-plane rule that every node holds every SI table now applies to verifiers only. Promotion broadcast becomes owner-only.
- The contiguous network-wide safe prefix becomes per track in stage 2. The global `safe_block_seq` keeps its conservative meaning.
