# Sentio Arbiter — P1b: Orchestrator, gRPC Server, cmd/arbiter, Config

**Date:** 2026-07-05 **Status:** Proposed (v1) **Base:** [2026-06-30 Sentio Arbiter design](2026-06-30-sentio-arbiter-design.md) §3.2/§10/§11 + [2026-07-05 P1a design](2026-07-05-arbiter-p1a-fsm-raftnode-design.md) (delivered: 17-command FSM, raftnode, wire seam). **Source of truth:** English version.

This document designs P1b: the leader-only orchestrator with failover re-entry (§10.2), the six gRPC services with subscribe streams and NotLeader handling (§11), the `cmd/arbiter` binary and its config section (resolving base-design Open Question 8), raft tuning defaults (resolving Open Question 2), the L2-anchoring seam with a local v1 backend, and the P1a ledger roll-forwards. Everything lands in `github.com/sentioxyz/arbiter`; housegate receives documentation only.

## 1. Scope, decisions, and boundary

**Decisions resolved with the user (2026-07-05):**

1. **Anchoring: seam + local backend.** P1b freezes an `anchor.Client` interface and ships only a `local` implementation (deterministic refs, immediate finality + last_mergeable) so the pipeline runs end-to-end in dev/test; a real Ethereum L2 backend is a follow-up plan that plugs the same seam behind a config switch — no protocol change.

**Decisions made in this design (overridable at spec review):** v1 gRPC is plaintext on a trusted network — §11 does not mandate TLS and the v1 trust model (base §4) covers the control plane; TLS/mTLS is recorded as P3 hardening. Membership RPCs carry no authentication in v1 (same trust model; documented). The arbiter repo stays on plain Go modules (CI is green; Bazel only if/when org-wide integration demands it — carries forward from P0). `SafeState` reads serve local FSM state on any node (bounded staleness per §11.3); linearizable read-index routing is deferred. Seal triggering uses statement-count and age only; a bytes trigger would need `OpenBlock` to accumulate sizes and is recorded as a follow-up, not built.

**In P1b:** `fsm` additions (transition-event hook, read facade, four roll-forward changes); new packages `orchestrator` (implements the frozen §3.4 `Orchestrator` seam), `anchor`, `server`, `config`; `cmd/arbiter`; raft-boltdb + TCP transport assembly (P1a's injection points); the rolled-forward hardening tests.

**Not P1b (P1c and later):** SNode promotion executor and Verifier data-plane process (replay + byte-side scanner); a real L2 anchor backend; HouseGate-side ingress client wiring; TLS; Bazel.

## 2. Observation mechanism (the §3.2 shape, made concrete)

Base §3.2 prescribes the hybrid: `Apply` appends transition events to a **leader-local, bounded, in-memory channel**; on acquiring leadership the orchestrator first **scans committed FSM state** to rebuild its work set, then drains the channel. This design makes the division of trust explicit: **the scan is the source of truth; the channel is a latency optimization.** Events carry no payloads, may be dropped when the channel is full, and are never required for correctness — a periodic rescan ticker inside the orchestrator recovers anything a dropped event would have signaled. Alternatives considered and rejected: payload-carrying events with no rescan (violates §10.2 re-entry, unbounded event sizes); polling-only with no channel (adds latency to every hop and contradicts §3.2; its useful core — the rescan backstop — is absorbed into the chosen shape).

## 3. fsm extensions

All new read methods take `f.mu.RLock`; the event hook is the only addition to the write path.

**Events (`fsm/watch.go`).** `New` gains an options form (`NewWithOptions(params Params, opts Options)` or an `Options` field — plan pins the exact constructor; `New(params)` keeps its P1a behavior with a nil channel):

```go
type EventKind uint8 // Admitted, BlockSealed, RCBound, EvidenceRecorded, QuorumReached,
                     // AnchorFinal, PromotionIssued, PromotionAcked, ManifestPublished,
                     // CleanupScheduled, CleanupAcked, MembershipChanged

type Event struct {
    Kind         EventKind
    StatementSeq uint64 // 0 when not statement-scoped
    BlockSeq     uint64 // 0 when not block-scoped
    PromotionSeq uint64 // 0 when not promotion-scoped
}
```

`Apply` emits the matching event at the end of each successful state mutation via **non-blocking send — full channel drops the event** (comment: wake hint; truth lives in the scan). Followers run with the same channel wired but the orchestrator only consumes on the leader; the notification path never feeds back into `Apply` (§3.2).

**Read facade (`fsm/reads.go`), consumed by orchestrator + server:**

```go
type WorkSet struct {                    // the §10.2 re-entry inventory
    OpenBlock          OpenBlockStats    // count + oldest-statement age source (see below)
    SealedUnmarked     []uint64          // sealed, all RCs bound, MarkReplaying not applied
    AwaitingRC         []uint64          // sealed, ≥1 statement without a bound RC
    EvidenceIncomplete []BlockEvidence   // marked; per-replica attestation/scan presence + VerifierSet
    QuorumFailed       []uint64          // evidence complete (3/3 bundles), Verdict != quorum, unchallenged
    UnanchoredVerified []BlockAnchor     // QuorumVerified, finality/last_mergeable not both recorded
    PromotablePending  []PromotionWork   // Promotable statements grouped by partition, no live pending promotion
    IssuedUnacked      []PendingPromotionInfo
    AckedUnpublished   []ManifestWork    // acked promotions not yet covered by a published manifest
    PublishedUncleaned []CleanupWork     // acked + published, cleanup not scheduled
}
func (f *FSM) WorkSet() WorkSet
func (f *FSM) OpenBlockStats() OpenBlockStats          // Count, StatementSeqStart; age computed by the CALLER
func (f *FSM) StatementAck(flatID string) (StatementAckInfo, bool)  // seq + stored envelope (idempotent re-ack)
func (f *FSM) SafeWatermarkView() SafeWatermark
func (f *FSM) ManifestByID(id string) (*replay.SafeSnapshotManifest, bool)
func (f *FSM) ManifestBySafeBlock(seq uint64) (*replay.SafeSnapshotManifest, bool)
func (f *FSM) BlockDispatchInfo(blockSeq uint64) (BlockDispatchInfo, bool) // ReplayJob material: header fields,
                                                       // statement projections, SourceClaimRoot (last RC),
                                                       // candidate PartRefs, VerifierSet, recorded evidence per replica
```

Exact struct fields are pinned in the plan; every method returns copies, never interior pointers. **Wall-clock note:** the FSM stores no timestamps (determinism red line), so statement AGE cannot come from state. The orchestrator computes age from its own leader-local clock: it timestamps the first `Admitted` event / first scan sighting of the current open block — age resets on failover, which only delays a seal by at most `seal.max_age`; acceptable and documented.

**Roll-forward changes (from the P1a ledger):**
- `applyPublishSafeSnapshot` asserts `manifest.SnapshotID == manifest.ManifestRoot` (content-addressed ids; load-bearing for check 2's manifest pinning) — reject otherwise.
- `applyRegisterNode` rejects a registration whose ed25519 pubkey already belongs to a DIFFERENT node_id (duplicate-key hygiene).
- `applyRecordAnchorFinality` gains a comment documenting last-wins anchor-content semantics (flags OR-latch; content overwrite is deliberate — the log is the audit trail).
- `readSnapshot` caps the JSON length prefix with a sanity bound (1 GiB constant) before allocating.

## 4. Orchestrator (`orchestrator/`)

**Lifecycle.** `cmd` owns the leadership watch: on `LeaderCh` gain it starts `Run(leaderCtx)` (the frozen §3.4 seam); on loss or shutdown it cancels. Inside the loop, every side effect is preceded by `VerifyLeader()` (§10.2 contract; the channel is only a hint). `Run` is a single-goroutine select loop; blocking I/O (anchor waits, stream sends) runs in short-lived child goroutines that report back to the loop.

**Re-entry sequence (§10.2):** `VerifyLeader → Barrier(apply_timeout) → fsm.WorkSet() → seed queues → select {events, seal ticker, retry ticker, rescan ticker, ctx.Done}`. The rescan ticker (same period as `dispatch.retry_interval`) re-runs `WorkSet()` and merges — the drop-safety backstop.

**Work state machine** (each row: a WorkSet inventory class → the idempotent action; §10.3 keys make retries safe. `AwaitingRC` deliberately has no row — the missing RC comes from the source SNode, so the class is observability-only until the RC lands):

| Inventory | Action |
|---|---|
| Open block hits the seal trigger (count ≥ `seal.max_statements` OR leader-observed age ≥ `seal.max_age`) | propose `SealL3Block` |
| SealedUnmarked | propose `MarkReplaying` |
| EvidenceIncomplete | send `ReplayJob` over the VerifierGateway stream to each connected VerifierSet member missing an attestation; after a replica's attestation lands (event), send it the `ByteSideScanRequest` (§7.1 two rounds); retry ticker re-sends to connected-but-silent members |
| QuorumFailed (3/3 evidence bundles, no quorum, unchallenged) | propose `OpenChallenge`, then `ResolveChallenge(REJECTED)` — v1 immediate adjudication (§7.5); the SAFE resolution is produced by an external adjudicator flow, never by this loop |
| UnanchoredVerified | `anchor.Client.Anchor(ctx, chainHash, stateRoot=lastRC.SourceClaimRoot)` → `WaitFinality` → propose `RecordAnchorFinality`; check-before-anchor via the recorded anchor state in WorkSet (§10.3 idempotency) |
| PromotablePending (per partition, no live pending promotion for it) | aggregate verified `PartRef`s → `PromoteSafePartition{PromotionSeq: current+1}` → `authority.Signer.SignPromotion` → propose `RecordPromotionIssued` → send `PromotionCommand` over the PromotionGateway stream. FSM rejects non-monotonic seq → re-read and retry (single-threaded loop makes races failover-only) |
| IssuedUnacked | re-send on SNode subscribe/reconnect and on the retry ticker (SNode dedups by promotion_seq watermark, §8.3) |
| AckedUnpublished | assemble `SafeSnapshotManifest` (base = pinned previous manifest, updated partition roots from acks) → `Seal()` → propose `PublishSafeSnapshot` |
| PublishedUncleaned | `authority.Signer.SignCleanup` → propose `ScheduleUnsafeCleanup` → send over the stream; `RecordCleanupAck` arrives via the gateway |

**Failure semantics.** A `Rejected` result on the orchestrator's own proposal means the state moved underneath it (typically a failover race): log a warning and trigger an immediate rescan; never blind-retry the same bytes. Anchor/stream errors: log + leave the inventory entry for the retry ticker. Timeouts do not abandon work in v1 (the §10.4 coupling makes promotion progress the priority); persistent failures surface via logs/metrics.

## 5. Anchor seam (`anchor/`)

```go
type Client interface {
    Anchor(ctx context.Context, l3BlockHash, stateRoot string) (arbiter.AnchorRef, error)
    WaitFinality(ctx context.Context, ref arbiter.AnchorRef) (finality, lastMergeable bool, err error)
}
```

P1b ships `local.Client`: `AnchorRef{L3BlockHash, StateRoot, L2TxRef: "local:" + l3BlockHash, L2BlockNumber: process-local monotonic}`, `WaitFinality` returns `(true, true)` immediately. Anchoring is leader-side I/O — determinism constraints apply only to the recorded `RecordAnchorFinality` command, not to the client. The config `anchor.backend` allowlist (`local` in P1b) selects the implementation; a real L2 backend is additive.

## 6. gRPC server (`server/`)

**Structure.** One `Server` holding: the `ConsensusNode` (proposals), the fsm read facade, the two stream registries, and an injected `LeaderAddr() string` (cmd wires it from `node.Raw().LeaderWithID()`; the frozen `ConsensusNode` seam is untouched). All six services register on one plaintext `grpc.Server`. One proposal helper: `propose(ctx, wire.Command)` → `wire.Encode` → `Apply(apply_timeout)` → error mapping: `raft.ErrNotLeader`/`ErrLeadershipLost` → `FAILED_PRECONDITION` with a `pb.NotLeader{leader_addr}` status detail (§11.3); other apply errors → `UNAVAILABLE`.

**wire exports.** The server needs the P1a-internal per-message converters at the RPC boundary; P1b exports them (mechanical rename: `EnvelopeFromPB/ToPB`, `RCFromPB/ToPB`, `AttestationFromPB/ToPB`, `ScanFromPB/ToPB`, `PromotionAckFromPB/ToPB`, `CleanupAckFromPB/ToPB`, `ManifestFromPB/ToPB`, `AnchorRefToPB`, `RegistrationFromPB`, plus the dispatch-side `ToPB` builders for `ReplayJob`/`ByteSideScanRequest`/`PromotionCommand`). Conformance tests unchanged.

**Service semantics:**
- **ArbiterIngress.SubmitStatement** — the ingress freshness edge deferred by P1a: parse the user_jws `iat` and reject `now - iat > ingress.max_statement_age` (+ clock-skew tolerance) with `INVALID_ARGUMENT` before proposing (wall-clock checks live HERE, never in Apply). Then propose. Result mapping: `ACCEPTED` → ack; `DUPLICATE_CLIENT_SEQ` → consult `fsm.StatementAck(flatID)`: if the stored envelope equals the request field-for-field, return the ORIGINAL Accepted ack (the proto idempotency contract); otherwise return the duplicate code.
- **SourceClaims.RegisterResultClaim** — propose `RegisterRC`; FSM-absorbed duplicates → `Ack`; genuine rejections (source mismatch / NUL ids / conflicting first-wins) → `INVALID_ARGUMENT` carrying the reason.
- **VerifierGateway** — `SubscribeVerifierDispatch(hello)`: leader-only (else NotLeader); registers `replica_id → send channel` in the verifier registry; the stream goroutine forwards `VerifierDispatch` messages until client disconnect or leadership loss. Subscription is itself a signal: the orchestrator re-dispatches that replica's pending jobs on connect. `SubmitAttestation`/`SubmitByteSideScan` → propose the Record commands; the FSM gates are the adjudication.
- **PromotionGateway** — same registry pattern keyed by `node_id`; `AckPromotion`/`AckCleanup` → propose `RecordPromotionAck`/`RecordCleanupAck`.
- **SafeState** — `GetSafeWatermark`/`GetManifest`/`GetManifestByBlock` from the read facade, served from local state on any node (bounded staleness, §11.3).
- **Membership** — propose `RegisterNode`/`MarkActive` (v1: unauthenticated on the trusted network, documented; the FSM's new duplicate-pubkey rejection applies).

**Stream lifecycle.** On leadership loss the server closes all dispatch streams and clears both registries; clients reconnect against the new leader (subscribe on a follower gets NotLeader). The `authority.Signer` is constructed in cmd and handed to the orchestrator only — the server never signs.

## 7. Config (`config/` — resolves base-design Open Question 8)

The arbiter repo gets its own small yaml config package (housegate's `pkg/config` is proxy-specific; only its conventions are copied — a local `Duration` accepting `"5s"` strings with a `< 1s` warning, `Validate()` aggregating via `errors.Join`). Logging imports housegate `pkg/log` directly (dependency already present).

```yaml
node_id: "arb-1"                       # required; raft LocalID + identity
raft:
  listen: "0.0.0.0:7000"               # required; TCP transport bind
  advertise: ""                        # optional; defaults to listen
  data_dir: "/var/lib/arbiter"         # required; raft-boltdb + snapshots
  bootstrap: false                     # first node of a NEW cluster only
  peers: [{id: "...", addr: "..."}]    # required (non-empty) when bootstrap: true
  election_timeout: "1s"               # Open Q2 resolution: hashicorp default family →
  heartbeat_timeout: "1s"              #   typical failover < 2s, safe against false elections;
  leader_lease_timeout: "500ms"        #   all exposed for staging tuning
  commit_timeout: "50ms"
  snapshot_interval: "120s"
  snapshot_threshold: 8192
  trailing_logs: 10240
grpc_listen: "0.0.0.0:7001"            # required
apply_timeout: "5s"                    # proposal + barrier timeout
seal: { max_statements: 256, max_age: "2s" }
ingress: { max_statement_age: "5m" }
dispatch: { retry_interval: "5s" }     # also the rescan-backstop period
anchor: { backend: "local" }           # allowlist-validated
authority:
  private_key_hex: "..."               # env ARBITER_AUTHORITY_PRIVATE_KEY_HEX overrides
  allowed_addresses: ["0x..."]         # → fsm Params.AuthorityAddresses
genesis:
  schema_snapshot_id: "schema-genesis"     # → fsm Params
  executor_profile_id: "housegate-replay-mvp-v0"
```

**Consensus parameters** (`genesis.*`, `authority.allowed_addresses`) feed `fsm.Params` and MUST be identical on every node or the cluster forks — stated in the config doc comments AND enforced socially via `Validate`'s warning text (mechanical cross-node checks are a P3 idea, not v1). Validation: required fields, positive durations, `bootstrap ⇒ peers non-empty`, key hex parses to a secp256k1 key, addresses lowercased.

## 8. cmd/arbiter

`flags (-config, -log-level) → config load + Validate → housegate pkg/log setup → fsm.New(Params{genesis, authority allowlist}, Notify: bounded chan (cap 1024)) → raftnode.New with raft-boltdb/v2 store + raft.NewFileSnapshotStore + raft.NewTCPTransport (new dependency: github.com/hashicorp/raft-boltdb/v2) → server on grpc_listen → leadership watcher goroutine (LeaderCh gain ⇒ orchestrator.Run(leaderCtx); loss ⇒ cancel; VerifyLeader remains the in-loop guard against dropped notifications) → optional BootstrapCluster (bootstrap: true, first boot only — idempotent-guarded by raft's ErrCantBootstrap) → signal-cancelled context → graceful shutdown order: grpc GracefulStop → orchestrator cancel → raft Shutdown → bolt close.`

## 9. Testing

- **Orchestrator unit tests:** fake `ConsensusNode` (applies encoded commands straight to a real FSM), fake `anchor.Client`, fake stream registries — table-driven per work-state-machine row; **failover re-entry** (pre-populate FSM state mid-pipeline → `Run` → assert the catch-up proposals); **event-loss backstop** (drop all events → rescan ticker still drives to completion).
- **Server unit tests:** bufconn per service; NotLeader detail mapping; idempotent SubmitStatement re-ack (field-equal duplicate → original ack; different content → duplicate code); subscribe stream push + teardown on leadership loss; ingress staleness rejection.
- **In-process integration (no docker — the P1b flagship):** 3-node real raft (inmem transport or loopback TCP) + real FSM + real servers + `local` anchor + scripted fake Verifier/SNode gRPC clients driving the FULL §3.5 pipeline through the wire: submit → seal → RC → dispatch → attest+scan → quorum → anchor → promote → ack → manifest → cleanup; then **kill the leader mid-pipeline and assert the new leader's orchestrator finishes the run** — the machine proof of §10.2.
- **cmd tests:** config `Validate` table; start/stop smoke with a temp data_dir.
- **Rolled-forward hardening tests (fsm):** JWS v=27/28 normalization + malformed-encoding table; gap-split-triggers-budget path; 2-pass+1-fail quorum boundary.

## 10. Acceptance tripwires (P1b-specific)

Must hold: every orchestrator side effect is preceded by `VerifyLeader`; the orchestrator never mutates FSM state except by proposing commands; correctness never depends on a delivered event (kill-all-events test passes); wall-clock reads live only in orchestrator/server/anchor/cmd — the fsm red lines and CI tripwires from P1a stay green; promotion signing happens only in the orchestrator path (grep: no `authority.Signer` reference in `server/`); `SafeState` handlers perform no proposals. Must NOT appear: a second hash profile; any fsm write API beyond `Apply`/`Restore`; config knobs for the frozen consensus constants (quorum/select/kind).

## 11. Recorded follow-ups (not P1b)

Real L2 anchor backend (seam-compatible); TLS/mTLS + membership authentication (P3); bytes-based seal trigger (`OpenBlock` size accumulation); linearizable SafeState reads via read-index routing; O(parts×statements) reverse index if promotion volume grows; cross-node consensus-param verification; Bazel migration decision.

## 12. References

- [2026-06-30 Sentio Arbiter design](2026-06-30-sentio-arbiter-design.md) — §3.2 pattern, §3.4 seams, §5.1/§5.6 sealing/dispatch, §7.1/§7.5, §8, §10, §11, Open Questions 2/5/8
- [2026-07-05 P1a design](2026-07-05-arbiter-p1a-fsm-raftnode-design.md) + errata — delivered FSM/raftnode/wire surfaces this phase consumes
- `.superpowers/sdd/progress.md` P1a section — roll-forward inventory and P1b preconditions
- `github.com/sentioxyz/arbiter` — landing repo (main = 79955f1 at design time)
- housegate `pkg/log`, `pkg/replay` (`Seal`/`Validate`, job/attestation types) — reused surfaces
