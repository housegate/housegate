# Sentio Arbiter — P1a Replicated Core: FSM + raftnode

**Date:** 2026-07-05 **Status:** Proposed (v1) **Base:** [2026-06-30 Sentio Arbiter design](2026-06-30-sentio-arbiter-design.md) §4–§7 + [2026-07-04 accumulator design](2026-07-04-arbiter-accumulator-design.md) (P0b). **Source of truth:** English version.

This document designs the P1a phase frozen by the P0 plan's follow-up list: the replicated FSM (`Apply`/`Snapshot`/`Restore` over the full 17-command `RaftCommand` alphabet), the §6.3 admission assembly over the P0b accumulator primitives, deterministic source/verifier selection, the three-way predicate inside the FSM, and the `hashicorp/raft` wiring behind the frozen `ConsensusNode` seam — plus the three ledger todos carried into P1a (arbiter-proto v0.2.0 enum append, arbiter repo CI, `authority.Validator` MaxTokenAge fail-closed) and the P0-review carry-over on pb→Go nil/empty-slice normalization.

## 1. Scope, decisions, and deliverable boundary

**Decisions resolved for this phase** (with the user, 2026-07-05):

1. **Verifier selection (arbiter design Open Question 3, required before the P1 freeze): deterministic 3-selection.** When the Active non-source verifier pool exceeds 3, the FSM selects exactly 3 by a frozen deterministic function (§7 below); quorum stays a strict 2-of-3, so the "tolerates 1 malicious verifier" claim does not drift with pool size and per-block replay cost is bounded. A pool of exactly 3 degenerates to selecting the whole pool.
2. **Arbiter repo CI credentials: fine-grained PAT** with read access to `sentioxyz/arbiter-proto` and `housegate/housegate`, stored as the arbiter repo Actions secret `GH_MODULES_TOKEN` (user-provisioned). The workflow wires it via a git `insteadOf` rewrite + `GOPRIVATE`.
3. **MaxTokenAge enforcement: fail-closed inside `authority.Validator`.** `authorize()` rejects every token when `MaxTokenAge <= 0`, the same shape as the existing empty-allowlist fail-closed — the zero-value trap (a never-expiring token) dies at the root rather than at each construction site.
4. **Go canonical type naming: `StatementEnvelope`, no V2 suffix.** The pb message name `StatementEnvelopeV2` is frozen with arbiter-proto v0.1.0 (renaming is a buf breaking change); the `wire` package bridges `pb.StatementEnvelopeV2 ⇄ arbiter.StatementEnvelope`, and the Go world uses only `StatementEnvelope`.

**In P1a:** the `fsm` and `wire` packages and the root-package canonical mirror types in `github.com/sentioxyz/arbiter`; the `raftnode` implementation of the frozen `ConsensusNode` seam; arbiter-proto v0.2.0 (`ADMISSION_CODE_GAP_BUDGET_EXCEEDED` append); the arbiter repo CI workflow; the `authority.Validator` fail-closed fix.

**Not P1a:** the orchestrator implementation, the gRPC server, `cmd/arbiter`, the `pkg/config` Arbiter section, raft-boltdb/TCP-transport assembly (all P1b); SNode promotion executor and Verifier byte-side scanner (P1c); leader-side proof-serving RPC (decentralized phase); any housegate code change — housegate already exports everything the FSM needs (`replay.CanonicalDigest`, `replay.DigestString`, `ExecutionReceipt.Hash()`, `Seal`/`Validate`, `pkg/lthash`), so housegate receives only this design/plan documentation.

**Key P0b input honored throughout:** in v1 the FSM holds the full SpentIDs dictionary; `SubmitStatementCmd.non_membership_proof` is always empty, admission is `Status()` + `Insert()` with no proof verification in `Apply`, and the arbiter-design §4.2 `HiSeq`/`Gaps` maps are absorbed by the accumulator (`AccountState` exposes them).

## 2. Package layout and the structural determinism boundary

```text
arbiter/                       # github.com/sentioxyz/arbiter
  types.go                     # + canonical Go mirror types (§3) + AdmissionCode mirror
  domains.go                   # + frozen CanonicalDigest domain constants (§3)
  wire/                        # NEW: the ONLY pb ⇄ Go conversion layer
    command.go                 #   DecodeRaftCommand([]byte) (Command, error) — Go union
    convert.go                 #   per-message converters + nil/empty normalization
  fsm/                         # NEW: the deterministic replicated core (never imports gen/pb)
    fsm.go state.go apply.go admission.go select.go threeway.go snapshot.go
  raftnode/
    consensus.go               #   frozen seam (unchanged)
    node.go                    # NEW: hashicorp/raft assembly implementing ConsensusNode
  authority/validator.go       #   MaxTokenAge fail-closed (ledger todo 3)
  .github/workflows/ci.yml     # NEW: build + vet + test + tripwires (ledger todo 2)

arbiter-proto/                 # v0.2.0: ADMISSION_CODE_GAP_BUDGET_EXCEEDED = 8 (ledger todo 1)
housegate/                     # documentation only; zero code change
```

**Import red line (the structural form of §4.3 red lines / §13 tripwire).** `fsm` imports only the root `arbiter` package, `accumulator`, `wire` (for the decoded Go command union), `housegate/housegate/pkg/replay` + `pkg/lthash`, and `hashicorp/raft` (for the `raft.FSM`/`raft.Log` interface types). `gen/pb` may appear only in `wire`'s imports. Canonical hashing therefore cannot run over proto types by construction — the property is import-graph-checkable, and CI enforces it mechanically (§11).

**nil/empty normalization (P0-review carry-over, frozen at the wire seam).** The canonical Go form uses `nil` for an empty repeated field and a `nil` pointer for an absent message. proto3 repeated fields have no presence, so decoding naturally yields `nil`; the rule's force is protocol-side: a verifier that builds a receipt with a non-nil empty slice hashes `[]` where the FSM's recomputation (over the round-tripped struct) hashes `null`, so its `receipt_hash` fails and the attestation is rejected. This is a protocol conformance rule, stated in the `wire` package documentation and pinned by tests.

## 3. Canonical Go mirror types and frozen constants

The root package gains Go mirrors of the arbiter-proto messages the FSM stores or hashes, following the existing `PromoteSafePartition` convention: plain structs, JSON tags byte-equal to the proto field names, conformance-tested against `gen/pb` (extending the P0 `conformance/replay_wire_test.go` pattern):

- `StatementID` (structured form: `client_account`, `client_seq`, `client_nonce`; the flat string form stays `StatementIDString`).
- `StatementEnvelope` (mirrors `pb.StatementEnvelopeV2`; see decision 4).
- `CandidatePart`, `PartitionLtHashSum`, `RCRecord`.
- `PartScan`, `ByteSideScanMsg`.
- `AnchorRef`.
- `NodeRegistration` + `NodeRole` (Go constants mirroring the pb enum numbers).
- `AdmissionCode` (Go constants mirroring pb enum numbers 0–8, including the new `AdmissionCodeGapBudgetExceeded = 8`; a conformance test pins every number).

`ReplayJob`, `ReplayAttestation`, `ExecutionReceipt`, `SafeSnapshotManifest`, `PartitionCommitment`, `PartManifestEntry` are reused verbatim from `pkg/replay` (already canonical Go with JSON tags; the pb mirrors were field-name-frozen against them in P0).

**Frozen domain constants** (`domains.go`; all hashing through `replay.CanonicalDigest`, per the §13 single-profile tripwire):

```go
const (
    DomainL3Header     = "arbiter-l3-header-v1"      // PrevL3Hash chaining (§5)
    DomainByteSideScan = "arbiter-byte-side-scan-v1" // scan_hash (already promised by the proto comment)
    // already frozen in P0a (authority package): arbiter-promote-command-v1, arbiter-cleanup-command-v1
)
```

**Frozen selection-seed prefixes** (§7): `"arbiter-source-select-v1:"` and `"arbiter-verifier-select-v1:"`. These are consensus parameters in the P0b §8 sense: compile-time constants, no configuration surface.

## 4. FSM state

```go
// fsm/state.go — all fields serialize into the snapshot except derived indexes.
type State struct {
    NextStatementSeq uint64                                    // starts at 1
    NextL3BlockSeq   uint64                                    // starts at 1
    OpenBlock        *OpenL3Block                              // admitted statements since last seal
    Blocks           []L3BlockHeader                           // sealed headers (small: ~10^4 scale)
    SpentIDs         *accumulator.SpentIDs                     // owns HiSeq/Gaps/spent_ids_root (P0b)
    Statements       map[uint64]*StatementState                // key = statement_seq
    ByStatementID    map[string]uint64                         // flat id → seq (derived; rebuilt on Restore)
    PendingRC        map[string]*arbiter.RCRecord              // §5.5 late-binding park (RC before seq)
    Verifications    map[uint64]*BlockVerification             // key = l3_block_seq (§8)
    PromotionSeq     uint64                                    // last issued promotion_seq
    PendingPromotions map[uint64]*PendingPromotion             // promotion_seq → verified part set awaiting ack
    Partitions       map[arbiter.TablePartition]*PartitionState
    SafeWatermark    SafeWatermark                             // snapshot_id, safe_block_seq, manifest_root
    Manifests        map[string]*replay.SafeSnapshotManifest   // by snapshot_id (SafeBlockSeq secondary index derived)
    PromotedUnsafe   map[arbiter.TablePartition]map[string]bool // part_row_lthash registry (unsafe_latest filter)
    Nodes            map[string]*NodeInfo                      // role set, ed25519 pubkey, Active|Syncing|Evicted
    Params           Params
}

type Params struct { // consensus parameters: every node MUST construct the FSM with identical values
    SchemaSnapshotID   string   // v1 genesis schema id (no DDL command exists in the v1 alphabet)
    ExecutorProfileID  string
    AuthorityAddresses []string // lowercase 0x-prefixed; audit verification of RecordPromotionIssued
}
```

`Params` is supplied identically to every node or the cluster forks; sourcing it is P1b's config job, P1a freezes the struct and documents the obligation. **Quorum = 2, verifier-select = 3, admitted kind = INSERT-only are frozen compile-time constants, not Params fields** — the P0b §8 profile-governance philosophy: no knobs for consensus-critical values.

`StatementState` carries `Env arbiter.StatementEnvelope`, `Seq`, `SourceNode`, `Status` (the §4.4 lifecycle enum: `Sequenced`, `UnsafeRegistered`, `Replaying`, `QuorumVerified`, `FinalityWait`, `Promotable`, `Safe`, `ChallengeReplay`, `Rejected`), `RC *arbiter.RCRecord`, and `BlockSeq` (0 until sealed into a block).

**No pruning in P1a.** The arbiter design §4.2 gives `Statements` no eviction semantics; P1a copies that literally (state grows with cumulative statements) and records the pruning/compaction policy as a P1b/P2 follow-up (§14) rather than silently deviating.

**Rejected-but-committed semantics.** Raft commits a command before the FSM sees it, so a command that fails validation still occupies a log index: `Apply` leaves state unchanged and returns a typed rejection result (e.g. `SubmitResult{Code: AdmissionCodeDuplicateClientSeq}`) through the `ApplyFuture` — identical on every replica. Rejection is a result, never a Go error that could differ across nodes.

## 5. Snapshot format and L3 header hashing

**Snapshot container:** `magic "AFSM" ‖ ver u8 = 1 ‖ u64 big-endian JSON length ‖ JSON(snapshotDoc) ‖ SpentIDs canonical dump ("SIDS"…, the P0b §6 format)`. `snapshotDoc` is `State` minus `SpentIDs` and minus derived indexes (`ByStatementID`, the manifest SafeBlockSeq index), which `Restore` rebuilds. Snapshot bytes are never compared across nodes (each node writes its own), so only `Restore ∘ Snapshot ≡ id` is required — Go's `encoding/json` (sorted map keys) is sufficient.

**Concurrency shape:** `Snapshot()` serializes synchronously into an in-memory buffer and returns an `raft.FSMSnapshot` whose `Persist` streams that buffer; at v1 state sizes (few MB) this is cheap and sidesteps copy-on-write complexity.

**L3 header hashing:** `PrevL3Hash = replay.CanonicalDigest(DomainL3Header, header)` over the Go `L3BlockHeader` with its JSON tags. The first sealed block carries `PrevL3Hash = ""` (frozen genesis rule). At seal time `SpentIDsRootAfter = "0x" + hex(SpentIDs.Root())`, `SchemaSnapshotID`/`ExecutorProfileID` come from `Params`, `PrevSafeSnapshotID`/`PrevStateRoot` from the safe watermark, and the seq range from `OpenBlock`. Sealing an empty `OpenBlock` is a deterministic rejection (no empty blocks).

## 6. Admission: the §6.3 assembly (SubmitStatement Apply)

All steps are wall-clock-free and read only committed state + the logged command bytes:

```text
1. shape checks     statement_id complete, account normalized lowercase (before the accumulator
                    sees it, per P0b §3), sql non-empty, sql_hash == replay.DigestString(sql)
                    (binds the replay projection)                            → MALFORMED
                    non_membership_proof MUST be empty in v1 (FSM holds the dictionary;
                    a non-empty proof is protocol misuse — strict reject)    → MALFORMED
2. kind check       statement_kind != INSERT (frozen constant)              → KIND_NOT_ADMITTED
3. signature        parse user_jws (compact JWS; alg ES256K/secp256k1; payload =
   (wall-clock-free) housegate auth JWSPayload{iat, qhash}):
                    qhash == keccak256Hex(sql) AND recover(signingInput) == client_account
                                                                             → INVALID_SIGNATURE
                    iat/expiry are NOT checked in Apply (§4.3 red line 1); freshness is the
                    leader ingress edge's job before proposing (P1b)
4. schema/settings  v1 has no DDL/schema registry: the seam exists (pure function over State)
                    with implementation = accept; SCHEMA_NOT_ALLOWED reserved for the P2 lane
5. dedup (§6.3)     st := SpentIDs.Status(coord)
                    StatusSpentDuplicate                       → DUPLICATE_CLIENT_SEQ
                    StatusSeqZero                              → MALFORMED (folded; no distinct code)
                    StatusFresh | StatusGapFillable → SpentIDs.Insert(coord)
                      ErrGapBudgetExceeded                     → GAP_BUDGET_EXCEEDED (v0.2.0)
6. assign + bind    seq = NextStatementSeq++; Statements[seq] created (Status = Sequenced);
                    ByStatementID[flatID] = seq; deterministic source selection (§7) recorded in
                    StatementState.SourceNode; if PendingRC[flatID] exists: adopt when its
                    source_node matches the selected source (→ UnsafeRegistered), else discard
                    the parked claim; append statement to OpenBlock
7. result           SubmitResult{Code: ACCEPTED, StatementSeq: seq}
```

`INVALID_PROOF` (code 6) stays reserved for the decentralized phase that actually consumes carried proofs; v1's strict-empty rule reports `MALFORMED`.

## 7. Deterministic selection (source §5.4, verifier §7.1 + Open Q3 resolution)

Both selections are pure functions over committed state; all hashing goes through `replay.DigestString` (single profile, no parallel hash). The index derivation: `u64(digest)` parses `digest[2:18]` of the `"0x"`-prefixed digest string (the first 8 hash bytes) as a big-endian uint64.

- **Source selection (at SubmitStatement):** pool = nodes whose committed status is Active and whose role set contains SNODE, sorted by NodeID. `idx = u64(DigestString("arbiter-source-select-v1:" + flatStatementID)) mod len(pool)`. Recorded in `StatementState.SourceNode`; a later `RegisterRC.source_node` must equal it or the claim is rejected (§5.4 linkage). An empty pool records `SourceNode = ""` (no RC can ever match; the statement stalls) — in a sane bringup `RegisterNode`/`MarkActive` are the first log entries, so a zero-writer pool is an operational error, documented not special-cased.
- **Verifier selection (at MarkReplaying, per decision 1):** ring = nodes Active with role VERIFIER, NodeID not in the block's source set, sorted by NodeID. `start = u64(DigestString("arbiter-verifier-select-v1:" + decimal(block_seq))) mod n`; take 3 consecutive ring positions. **n < 3 → Apply(MarkReplaying) deterministically rejects** (the block stays sealed-but-undispatched; the orchestrator retries later) — the machine-enforced form of §7.4's "non-source pool >= 3". The selected set is recorded in `BlockVerification.VerifierSet`; subsequent evidence commands accept only members.

**Route-A interface obligation (recorded, not built here):** the data-plane router (HouseGate → source SNode) must implement the same frozen source-selection function over the same membership view, or RCs will be rejected on source mismatch and statements stall. v1 single-writer deployments agree trivially; multi-writer alignment is a P1b/P1c responsibility. P1a's job is only to pin the FSM-side rule.

## 8. Block-level evidence and the three-way predicate

**The alignment refinement.** Evidence is per block — a verifier replays a whole `ReplayJob` (one sealed L3 block) and its receipt's `ComputedStateRoot` is the post-block root; the idempotency key is `(replica_id, block_seq)` (§10.3). `RCRecord` is per statement. The arbiter design §4.2 hangs attestations off `StatementState`; this design normalizes them onto a per-block record and projects the verdict back onto statements:

```go
type BlockVerification struct {
    BlockSeq     uint64
    SourceNodes  []string                              // union of the block's statement sources (usually one)
    VerifierSet  []string                              // the 3 selected at MarkReplaying
    Attestations map[string]*replay.ReplayAttestation  // replica → whole-block receipt (first wins)
    ByteScans    map[string]*arbiter.ByteSideScanMsg   // replica → scan (first wins)
    Verdict      *ThreeWayVerdict                      // per-replica three-check detail; recomputable
}
```

- **Check 1's right-hand side:** the block's claimed root = the `RC.source_claim_root` of the block's **last** statement (by statement_seq) — the source executes S1..Sn in order, so R_n is the post-block root, the same semantic layer as the verifier's `ComputedStateRoot`.
- **Evaluability condition:** block sealed ∧ every statement in the block has a bound RC (checks 2/3 need all part claims) ∧ evidence present. Every evidence-bearing command (`RegisterRC`, `RecordAttestation`, `RecordByteSideScan`) re-runs the evaluation at the end of its Apply — deterministic, so evaluating early or late yields the same verdict.
- **Per-replica conjunction, quorum over passing replicas** (§7.3 check 3's "for each verifier in the quorum" reading). Replica r passes iff all three hold:
  1. `r.Receipt.ComputedStateRoot == lastRC.SourceClaimRoot` — recomputed by the FSM from logged evidence, never read from the advisory `MatchSourceRoot` flag;
  2. for every partition p the block touches: `BasePartitionRoot(p) ⊕ Σ(partition_new_part_sums[p] across the block's RCs) == r.Receipt.PartitionCommitmentsAfter[p]`, computed with `pkg/lthash` addition over raw 2048-byte accumulators (hex-decoded); a never-promoted partition's base is the all-zero accumulator;
  3. every candidate part across the block's RCs appears in r's `ByteScans` with `scanned_part_row_lthash == claimed`.
- **≥ 2 of the 3 `VerifierSet` members pass → the block is QuorumVerified and every statement in it flips.** The `Verdict` records per-replica, per-check outcomes so any node or auditor recomputes the same conclusion from the log.

## 9. Command handler semantics (the full alphabet)

All handlers are wall-clock-free; idempotency keys follow §10.3; every rejection is a typed result with state unchanged.

| Command | Apply semantics |
|---|---|
| `SubmitStatement` | §6 admission pipeline |
| `SealL3Block` | reject if `OpenBlock` empty; derive header (§5), append to `Blocks`, create `Verifications[blockSeq]` shell, stamp `StatementState.BlockSeq`, open a fresh buffer |
| `MarkReplaying` | block sealed ∧ not yet marked; select `VerifierSet` (§7; pool < 3 → deterministic reject); statements → Replaying |
| `RegisterRC` | seq exists → bind + require `source_node == recorded SourceNode`; seq absent → park in `PendingRC` (§5.5); duplicate for a bound statement: byte-identical content is absorbed idempotently, different content is rejected (first wins); → UnsafeRegistered; re-evaluate §8 |
| `RecordAttestation` | replica ∈ `VerifierSet`; `(replica, block_seq)` first-wins; recompute `receipt.Hash() == ReceiptHash` (exported by pkg/replay); ed25519-verify signature over the receipt-hash string bytes with the pubkey from `Nodes`; record; re-evaluate §8 |
| `RecordByteSideScan` | same shape; recompute `scan_hash = CanonicalDigest(DomainByteSideScan, canonical Go form)` then ed25519-verify; record; re-evaluate §8 |
| `RecordAnchorFinality` | block must be QuorumVerified; back-fill `AnchorRef` into the sealed header (§5.2); anchor recorded without both flags → statements → FinalityWait; `finality_reached ∧ last_mergeable_reached` → Promotable |
| `RecordPromotionIssued` | wall-clock-free audit verification: purpose claim + cmd-hash binding (authority package hash fns) + secp256k1 recovery ∈ `Params.AuthorityAddresses`, **no iat check in Apply** (expiry is the SNode-side Validator's job); `promote.promotion_seq` must equal `State.PromotionSeq + 1` (frozen monotonic rule) → `State.PromotionSeq` advances; record the verified part set in `PendingPromotions` |
| `RecordPromotionAck` | `applied=true` → closure equality `post_partition_commitment == BasePartitionRoot ⊕ Σ(verified new-part lthash)` (§7.3 closure): pass → update `Partitions[p]` (base root = post, `PublishSeq`), statements → Safe, parts enter `PromotedUnsafe`; fail → record the mismatch, watermark does not advance (challenge path); `applied=false` (base CAS failed) → record only; rebase/re-issue is the P1b orchestrator's job |
| `PublishSafeSnapshot` | `replay.Validate` the manifest in Apply; record in `Manifests`, advance `SafeWatermark` |
| `ScheduleUnsafeCleanup` | same audit verification shape as RecordPromotionIssued (cleanup purpose/domain); mark registry entries for cleanup |
| `RecordCleanupAck` | idempotently clear `PromotedUnsafe` entries keyed by promotion_seq |
| `OpenChallenge` | mark the block's statements → ChallengeReplay |
| `ResolveChallenge` | verdict SAFE → Safe; REJECTED → Rejected (v1 immediate adjudication, §7.5) |
| `RegisterNode` | record roles + ed25519 pubkey; re-registering an existing node_id replaces the registration and resets status → Syncing |
| `MarkActive` | requires registered; status → Active (joins selection pools) |
| `EvictNode` | status → Evicted with reason; **not retroactive**: in-flight `VerifierSet`s are pinned in the log, already-logged evidence stands; only future selections see the shrunken pool |

## 10. raftnode: the ConsensusNode assembly

```go
// raftnode/node.go
type Options struct {
    NodeID        string
    FSM           raft.FSM            // inject fsm.FSM
    LogStore      raft.LogStore       // all injected: tests use raft.NewInmemStore()
    StableStore   raft.StableStore    //   P1b's cmd supplies raft-boltdb + TCP transport
    SnapshotStore raft.SnapshotStore
    Transport     raft.Transport
    RaftConfig    *raft.Config        // nil → raft.DefaultConfig() with LocalID = NodeID
}
func New(opts Options) (*Node, error)                  // *Node implements ConsensusNode
func (n *Node) Bootstrap(servers []raft.Server) error  // first-node cluster bootstrap
func (n *Node) Shutdown() error
```

`Apply`/`VerifyLeader`/`LeaderCh`/`Barrier` delegate to `*raft.Raft` directly. `LeaderCh` inherits hashicorp/raft's caveat (buffered, may drop notifications) — copied into the doc comment; the orchestrator contract already requires `VerifyLeader` before every side effect, so the channel is a wake-up hint, not a correctness input. P1a deliberately adds **no raft-boltdb dependency and no config surface**: storage and transport are injected, election/heartbeat tuning values are P1b's Open Question 2. A compile-time guard pins `*Node` to `raftnode.ConsensusNode` and `*fsm.FSM` to `raft.FSM`.

## 11. The three ledger todos

1. **arbiter-proto v0.2.0.** Append `ADMISSION_CODE_GAP_BUDGET_EXCEEDED = 8;` to `AdmissionCode` (compatible value append; passes buf breaking), with a comment mirroring the P0b §4 semantics (K = 64 open-range budget; filling from range edges never increases the count — the client's remedy). Regenerate, tag v0.2.0. The arbiter repo bumps its require and the root `AdmissionCode` mirror + conformance test pin the new number.
2. **Arbiter repo CI** (`.github/workflows/ci.yml`): checkout → setup-go (`go-version-file: go.mod`) → private-module access (`git config --global url."https://x-access-token:${{ secrets.GH_MODULES_TOKEN }}@github.com/".insteadOf "https://github.com/"` + `GOPRIVATE=github.com/sentioxyz,github.com/housegate`) → `go build ./... && go vet ./... && go test ./...`. Two cheap tripwires ride along: `go list -deps ./fsm` must not contain `arbiter-proto/gen/pb` (the §2 import red line), and a grep for `time.Now` under `fsm/` must be empty (red line 1's mechanical sentinel). Secret `GH_MODULES_TOKEN` is user-provisioned.
3. **MaxTokenAge fail-closed.** `authority.Validator.authorize()` gains a leading check: `MaxTokenAge <= 0` → reject every token with an explicit fail-closed error, mirroring the empty-allowlist behavior; field comment updated; zero-value and negative-value tests added. Existing tests all set `MaxTokenAge` explicitly and are unaffected.

## 12. Testing

- **Determinism flagship tests:** (a) the same command log applied to N fresh FSMs yields deep-equal state, equal L3 header hashes, and equal `spent_ids_root`; (b) `Snapshot`+`Restore` at random midpoints followed by the remaining log ≡ straight-through apply (catches snapshot field omissions).
- **Three-way fixtures:** hand-built evidence with real ed25519 keys, real `pkg/lthash` addition, real `receipt.Hash()`: honest pass; check-1-only failure (root mismatch); check-2 failure (lying partition sum); check-3 failure (scanned ≠ claimed); 1-of-3 insufficient; evidence from a non-`VerifierSet` replica rejected; closure equality both sides.
- **Admission table tests:** every AdmissionCode value reachable at least once (incl. GAP_BUDGET_EXCEEDED at the K = 64 boundary, reusing P0b vector thinking); wall-clock-free signature checks (forged JWS, qhash mismatch, recover ≠ client_account); strict non-empty-proof rejection.
- **Wire tests:** pb ⇄ Go round-trips + nil/empty normalization pinned; mirror JSON tags == proto field names; `AdmissionCode`/`NodeRole` numbers == pb enums (conformance pattern).
- **raftnode integration** (InmemTransport, no docker): 3-node cluster convergence (three FSMs, equal state roots); leader transfer mid-stream; forced snapshot then a new node joins via restore and catches up.
- **In-process mini e2e:** the full §3.5 lifecycle as a pure command stream (RegisterNode×4 → MarkActive → Submit → Seal → RegisterRC → MarkReplaying → 2×attest + 2×scan → AnchorFinality → PromotionIssued → PromotionAck → PublishSafeSnapshot → CleanupAck), asserting every Status transition and the final watermark.

## 13. Acceptance tripwires (P1a-specific, extending arbiter design §13)

Must hold: the verdict is computed inside `Apply` over logged signed evidence (never a leader-side decision); check 1 recomputed (advisory flag unread); the three-way never degenerates to root-equality alone; closure equality enforced at `RecordPromotionAck`; `fsm` does not import `gen/pb` (CI-checked); no `time.Now`/`rand`/map-iteration-order dependence in `fsm` (CI grep + review); every root/commitment hashes through `replay.CanonicalDigest` or a P0b-style frozen authenticator profile — never a third path; quorum/select/kind are constants, not config.

## 14. Recorded follow-ups (not P1a)

- **Statement-state pruning/compaction** after Safe + cleanup-ack (state currently grows with cumulative statements) — P1b/P2 decision.
- **Route-A router alignment**: HouseGate-side deterministic source routing implementing §7's frozen function — P1b/P1c.
- **Raft tuning values** (election/heartbeat/snapshot thresholds) — arbiter design Open Question 2, P1b config.
- **Leader-side proof-serving RPC** (`ProveNonMembership` is ready) — decentralized phase.
- **Challenge-window semantics beyond v1 immediate adjudication** — P5+.

## 15. References

- [2026-06-30 Sentio Arbiter design](2026-06-30-sentio-arbiter-design.md) — §4 FSM, §5 sequencing, §6.3 admission, §7 three-way, §10 HA, §13 tripwires, Open Questions 2/3
- [2026-07-04 accumulator design](2026-07-04-arbiter-accumulator-design.md) — P0b construction, `Status`/`Insert` primitives, profile governance
- [2026-07-03 P0 proto-freeze plan](../plans/2026-07-03-arbiter-p0-proto-freeze.md) — frozen alphabet, seams, P1a follow-up definition
- `github.com/sentioxyz/arbiter` / `github.com/sentioxyz/arbiter-proto` — landing repos
- housegate `pkg/replay` (`CanonicalDigest`, `DigestString`, `ExecutionReceipt.Hash`, `Seal`/`Validate`), `pkg/lthash`, `pkg/auth` — reused surfaces
