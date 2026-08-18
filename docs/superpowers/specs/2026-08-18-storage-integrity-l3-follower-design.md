# L3 Block Publication and the L3 Follower Node

**Date:** 2026-08-18 **Status:** Proposed **Roadmap:** [v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) Spec H. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §5.2, §7 (L3Block, "replayable from the L3 stream"), §12.5 (replay from the L3 stream), §15 Q5/Q6; [2026-06-30 Arbiter design](2026-06-30-sentio-arbiter-design.md) §3.5 (L1/L2/L3 layering), §5.2 (chain commitment), §12 ("on-chain DA reference via the reserved `AnchorRef` field" for P5+); [2026-07-15 P1d EVM anchor](2026-07-15-arbiter-p1d-evm-anchor-design.md) §10 (contract v2 = new contract + config swap); [2026-07-27 DA client](2026-07-27-arbiter-da-client-design.md) (custody chain, `PinPurpose`); designs PROGRESS.md 2026-07-01 ("Sequencer 就是在实现 L3；L3 Node 自研轻量（隐式共识、block 链表）"; "SQL 全文上链"). **Depends on:** Spec A (`statements_root` in `L3BlockHeader`, `SafeState.GetL3Block`, `DomainL3Statements`). **Code base:** arbiter `edd23c3` (`fsm/state.go`, `orchestrator/anchor*.go`, `accumulator/`), arbiter-core `829c44f`, arbiter-proto `v0.4.0` (`AnchorRef.da_ref` reserved, `PinPurpose`), arbiter `contracts/AnchorRegistry.sol`, network-da `8b67059`. **Source of truth:** English version.

## 1. Problem

The design's L3 has two halves. The **producer** half exists: the Arbiter's Raft FSM sequences statements, seals `L3BlockHeader`s into a hash chain (`ChainHash`), maintains `spent_ids_root`, and anchors `(l3BlockHash, stateRoot)` on the L2 `AnchorRegistry`. That is the "self-built light L3 node with implicit consensus" the 2026-07-01 sync described. The **follower** half does not exist, and the data it would need is not published:

1. L3 block content (the envelopes, hence the SQL text) lives only in the Arbiter's Raft log and FSM snapshots. The DA store holds INSERT payload bytes, not envelopes; the L2 anchor holds two hashes. "SQL 全文上链" (07-01) is in practice "SQL text inside the Arbiter". Loss of the Raft data loses the L3 stream; the base design's "replay from the L3 stream from genesis" (§7, §12.5) and the recomputability argument (§5.2 "a single honest verifier with the signed log refutes any number of colluding replicas") have no data source outside the trusted Arbiter.
2. There is no read API for blocks (Spec A adds `SafeState.GetL3Block`, served by the Arbiter — still a trusted RPC).
3. `AnchorRef.da_ref` is reserved and empty; the anchor gives an observer no way to locate a block's bytes.
4. The `statement_id` accumulator lives in the private `arbiter` repo, so no external party can recompute `spent_ids_root_after` even if it had the blocks.
5. Nobody consumes the anchor stream: there is no process that walks `Anchored` events, fetches blocks, verifies the chain, and re-derives L3 state independently of the Arbiter.

v1 is a centralized Arbiter, so none of this blocks v1 writes. It does block: (a) the recomputability property being real rather than theoretical, (b) Spec D's cold bootstrap trusting anything but the Arbiter's RPC, (c) any P5+ decentralization (independent verifiers, challengers, auditors all need an untrusted-source L3 stream).

## 2. Goals / non-goals

Goals: (1) every sealed L3 block is published as a content-addressed object in the DA store before it is anchored, and the anchor carries the object's DA reference; (2) an **L3 follower** can, from an L2 RPC + the anchor contract address + a DA endpoint, download the whole L3 stream, verify it (hash chain, `statements_root`, `spent_ids_root_after` recomputation, every `user_jws_v2`), store it locally, and serve the same read API the Arbiter serves (`GetL3Block`, latest anchored block); (3) the follower is a public library + reference binary so third parties can run it; (4) Spec D's replica bootstrap can use a follower as its L3 source.

Non-goals: P2P/gossip between followers (L2 events + DA are the transport); state-root verification by the follower (that is replay — the verifier's job; the follower verifies *sequencing*, not *execution*); serving as an ingress or influencing the Arbiter; L3 forks (single Arbiter, linear chain).

## 3. Decisions

**D1 — Block object = canonical JSON `{header, statements}`**, where `header` is `L3BlockHeader` with `L2AnchorRef` cleared (the anchor is back-filled and cannot be part of what is anchored) and `statements` are the block's `StatementEnvelope`s in `statement_seq` order (arbiter-core JSON tags == proto names). Its digest under `replay.CanonicalDigest("arbiter-l3-block-object-v1", obj)` is `l3_block_object_hash`; DA stores the raw bytes with `payload_hash = "0x"+hex(SHA-256(bytes))` (the DA put header profile) and `payload_length`. Both hashes are recorded. Rejected: anchoring the object hash instead of `ChainHash` — `ChainHash` stays the chain commitment (already frozen, Spec A adds `statements_root` to it); the object is a *carrier* whose integrity the follower checks against the header it contains.

**D2 — Publish before anchor; anchor carries the DA ref.** The orchestrator, on `SealL3Block`, puts the block object to the DA store (`PutPayloadInline` under `max_inline_bytes`, streamed `PutPayload` above), pins it with a new `PIN_PURPOSE_L3_BLOCK` (never released), proposes `RecordL3BlockPublishedCmd{block_seq, da_ref, object_hash, object_length}`, and only then anchors. `AnchorRegistry` v2 `anchor(bytes32 l3BlockHash, bytes32 stateRoot, string daRef)`; `AnchorRef.da_ref` filled. Rejected: deriving the ref from the hash — `payload_ref` is opaque by the DA backend-swap rule (`da.proto` header) and the fetch API is ref-keyed. Rejected: anchoring without a ref and letting followers ask the Arbiter — that is the trust we are removing.

**D3 — Accumulator moves to arbiter-core.** `arbiter/accumulator` (SMT, profile `sentio-spent-ids-v1`, proofs, snapshot, vectors) becomes `arbiter-core/accumulator` verbatim; `arbiter` imports it. The follower recomputes `spent_ids_root_after` block by block from genesis. `check-public-boundary.sh` still holds (arbiter-core gains no private import).

**D4 — Follower verifies sequencing, stores, serves; it never trusts the Arbiter.** Inputs: L2 RPC URL, `AnchorRegistry` address, DA data address, genesis `Params` (network id, schema snapshot id, executor profile id — the same values the Arbiter runs with; a follower configured with different genesis rejects block 1). Per block: fetch object by `daRef` from the `Anchored` event → `SHA-256(bytes) == payload_hash` (the follower is hash-checking a read, which the DA API is silent about by design) → decode → `header.ChainHash() == anchored l3BlockHash` → `header.prev_l3_hash == stored ChainHash(seq-1)` (`""`/genesis rule for seq 1 mirrors the FSM) → `header.statements_root == CanonicalDigest(DomainL3Statements, statements)` → `header.statement_seq_start/count` continuous with the previous block → every envelope: `verifyUserJWSV2` (moved to a public package, see §4), `sql_hash` binds `sql`, `envelope_version == 2` → accumulator: insert each `(account, client_seq)` in seq order, `Root() == header.spent_ids_root_after` (a duplicate or a gap-budget violation is a chain-invalid block) → store. `state_root` from the anchor is stored as claimed, not verified. Any failure halts the follower at that height with a loud error (it is either a corrupt/withheld DA object, a mis-anchor, or an Arbiter bug — all of which are exactly what a follower exists to surface).

**D5 — Serving surface = the same `SafeState.GetL3Block` shape** plus `GetL3Head{last_verified_seq, last_anchored_seq, l3_block_hash}`; read-only gRPC. Spec D's `acquireCandidate` / manifest catch-up take an `L3Source` port satisfied by either the Arbiter or a follower.

**D6 — Public library, reference binary in arbiter for v1.** `arbiter-core/l3/{object.go, verify.go, follower.go, store.go}` (public), `arbiter/cmd/arbiter-l3node` (binary; a public binary is a P5+ move like the other reference cmds). Store = a bolt file keyed by seq (same library the Raft log uses).

## 4. Changes

### 4.1 arbiter-proto (minor bump)

- `da.proto`: `PIN_PURPOSE_L3_BLOCK` (held forever; comment references this spec).
- `raftlog.proto`: `RecordL3BlockPublishedCmd{uint64 l3_block_seq; string da_ref; string object_hash; uint64 object_length;}` in `RaftCommand`.
- `arbiter.proto`: `L3BlockHeader` view (Spec A) gains `da_ref`, `object_hash`, `object_length`; new `SafeState.GetL3Head`; `AnchorRef.da_ref` semantics documented (set once published).

### 4.2 arbiter

- FSM: `L3BlockHeader` keeps `L2AnchorRef` excluded from `ChainHash`; new derived fields `DARef/ObjectHash/ObjectLength` set by `applyRecordL3BlockPublished` (idempotent per seq; must precede `RecordAnchorFinality` — anchoring a block without a recorded ref is rejected in Apply). Snapshot version bump (fold into A's if landed together).
- Orchestrator: `publishL3Block(seq)` step between seal and anchor (retry with backoff; failure blocks anchoring for that seq only); `anchor/evm` gains the v2 ABI (`anchor(bytes32,bytes32,string)`), bindings regenerated with the existing provenance job; contract `contracts/AnchorRegistryV2.sol` (idempotent on identical tuple; `StateRootMismatch`/`DARefMismatch` reverts; `Anchored(bytes32 indexed l3BlockHash, bytes32 stateRoot, string daRef, uint64 l2BlockNumber)`); `cmd/arbiter-anchor` deploys v2. Config `anchor.evm.abi_version: 2` (v1 kept only for the local backend / tests).
- Move `accumulator/` to arbiter-core (D3); update imports; the CI tripwire "fsm must not import arbiter-proto" is unaffected.
- `fsm/userjws.go` v2 verifier moves to a public `arbiter-core/envelope` package (`VerifyUserJWSV2(env)`); the FSM calls it.
- `cmd/arbiter-l3node`: config `{l2_rpc_url, anchor_contract, da_data_addr, genesis{network_id, schema_snapshot_id, executor_profile_id}, store_dir, grpc_listen, metrics_listen, start_from_l2_block}`; runs `l3.Follower`.

### 4.3 arbiter-core

- `accumulator/` (moved), `envelope/` (JWS v2 verify), `l3/object.go` (encode/decode/hash), `l3/verify.go` (the D4 predicate as a pure function `VerifyNext(prev *Head, obj Object, anchored Anchor, acc *accumulator.SpentIDs) error`), `l3/follower.go` (event subscription over `bind.ContractFilterer`, DA fetch through `dataplane/dastore`, store, gRPC), `l3/store.go`.
- `dataplane/dastore`: `PutObject`/`FetchObject` helpers over the existing put/fetch with the `L3_BLOCK` pin.

### 4.4 network-da

Accept the new `PinPurpose` (enum passthrough; add to the conformance suite that a `L3_BLOCK` pin is never released by `ReleasePins` policy in v1 — same rule as `AUDIT`).

## 5. Testing / acceptance

- arbiter-core `l3`: golden object encoding; `VerifyNext` table — good chain; wrong prev hash; tampered statement (statements_root); duplicate `client_seq` (accumulator root mismatch); bad JWS; non-continuous seq range; genesis mismatch; object hash ≠ anchored ref hash. Accumulator tests/vectors move unchanged.
- arbiter: FSM `applyRecordL3BlockPublished` idempotency + "anchor before publish is rejected"; orchestrator publishes then anchors (fake DA + fake anchor); v2 contract simulated-backend tests + anvil e2e (`TestAnvilE2E` extended to assert `daRef` in the event); `integration/chpipeline`: after N statements, run `l3.Follower` against the harness's anvil + da-store → follower head == Arbiter head, all blocks verified; **fraud drill**: mutate one stored DA object → follower halts at that height with a `statements_root`/hash error.
- Spec D integration: replica bootstrap configured with the follower as `L3Source` reaches the same `hg_safe` roots.

## 6. Delivery

1. arbiter-proto: pin purpose + command + header fields + `GetL3Head` → tag.
2. arbiter-core: accumulator + envelope moves (pure moves, tests green), `l3` package + tests → tag.
3. arbiter: FSM/orchestrator publication, contract v2 + bindings + `cmd/arbiter-anchor`, `cmd/arbiter-l3node`, chpipeline follower test → release.
4. network-da: pin purpose passthrough + conformance.
5. Spec D: `L3Source` port accepts a follower (one small PR once D lands).
6. Spec B: base design §7/§12.5/§15 Q5–Q6 updated ("SQL is on DA, ref is on chain, followers verify").
