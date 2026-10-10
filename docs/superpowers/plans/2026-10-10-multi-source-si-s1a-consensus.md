# Multi-source SI network — Plan S1-A: consensus layer of stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let one arbiter network accept a source SNode per storage-integrity indexer: governed enrolment of SI indexers, table ownership, owner-bound source selection, SNode and verifier messages signed and checked in consensus, a governed verifier set, owner-routed promotions and cleanups — all switched on by one authority-signed update (the *signed-claims activation*), with every pre-activation behaviour byte-identical.

**Architecture:** arbiter-proto gains additive messages and fields (Task 1). arbiter-core gains the canonical types, their normalisation and digests, the signing and deterministic verification primitives on top of the existing authority JWS machinery, the request ⇄ command converters with a strip helper for pre-activation proposals, and an owner-scoped data plane — a signing SNode that serves only its own indexer's tables and a signing verifier (Tasks 2–4). arbiter enforces the rules in the FSM after the activation, binds each statement's source to its table owner's SNode, routes promotions and cleanups to that owner, writes snapshot v19, and gates the activation on every voter's and data-plane node's capability (Tasks 5–12). Stage 2 (tracks, quarantine) is not in this plan.

**Tech Stack:** Go 1.26; protobuf + buf (arbiter-proto); Bazel 9.1.0 + Bzlmod + gazelle (arbiter-core, arbiter); hashicorp/raft + raft-boltdb; ES256K compact JWS (the existing `authority` package) and ed25519 (verifier evidence convention).

**Spec:** housegate `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` (main `534a8b4`) — binding. This plan implements §6 (stage-1 consensus layer) and the arbiter-core part of §8, i.e. decisions D3–D8, D11 and the stage-1 half of D12, and §14 sub-projects 1–3. Plan S1-B (`2026-10-10-multi-source-si-s1b-hosts-rollout.md`) implements §9, §10, §12 and the stage-1 rollout of §13 on top of this plan's releases.

## Global Constraints

- **Activation model.** Stage 1 starts with the first committed `UpdateConsensusParams` that sets `si_indexers` (entry for the founding indexer restated from `table_registry`) together with `verifiers`: the *signed-claims activation*, `(*FSM).signedClaimsActiveLocked() == len(Params.SIIndexers) > 0`. Before it every behaviour, command byte and snapshot byte is exactly today's; after it the rules below are mandatory. The activation is irreversible.
- **Strict decoder / fork safety.** The arbiter-core Raft decoder refuses unknown fields, so before the activation no proposed Raft command may carry any new field. Every pre-activation proposal built from a request passes through `wire.StripSignedClaims`; the activation update itself is the first command with new fields. `wire.StripSignedClaims` also clears `SeedLegacyTables.IndexerID`, so it must never be applied after the activation. Data-plane nodes may be upgraded first and send the new fields early; the server drops them.
- **Request-only signatures.** Signatures never become fields of the canonical `arbiter.RCRecord`, `arbiter.PromotionAck`, `arbiter.CleanupAck` or of `arbiter.NodeRegistration`; they ride beside the body in the wire command (`RegisterRC.SourceJWS`, `RecordPromotionAck.SourceJWS`, `RecordCleanupAck.SourceJWS`, `RegisterNode.SignerJWS/Ed25519Signature`, `MarkActive.SignerJWS/Ed25519Signature`, `RecordTablePurged.SignerJWS/Ed25519Signature`, `EvictNode.AuthorityJWS`). Commands are built only with the `wire.*FromRequest` converters; `wire.Decode` refuses a signature inside a command's request-only copy. `NodeRegistration.RegistrationSeq` is canonical and signed.
- **arbiter-proto numbers (verbatim, package `arbiter`).** `SIIndexerEntry{indexer_id=1, activation_block=2, signer=3, snode_node_id=4, enrollment_jws=5}` in `table_registry.proto`; `VerifierEntry{node_id=1, ed25519_pubkey=2}` and `EvictNodeRequest{node_id=1, expected_registration_seq=2, reason=3, authority_jws=4}` in `consensus.proto`; `ConsensusParamsUpdate.si_indexers=11, verifiers=12`; `ConsensusMutableParams.si_indexers=6, verifiers=7`; `TableRegistrySnapshot.si_indexers=7, seeded_indexers=8`; `optional uint64 TableIncarnation.owner_indexer_id=18`, `AddTableCmd.owner_indexer_id=8`, `SeedLegacyTablesCmd.indexer_id=3`; `RecordTablePurgedCmd.signer_jws=3, ed25519_signature=4` (also the `SubmitTablePurged` request); `NodeRegistration.registration_seq=6, signer_jws=7, ed25519_signature=8`; `NodeRef.registration_seq=2, signer_jws=3, ed25519_signature=4`; `RCRecord.source_jws=6`; `PromotionAck.source_jws=10`; `CleanupAck.source_jws=5`; `RegisterRCCmd.source_jws=2`; `RecordPromotionAckCmd.source_jws=2`; `RecordCleanupAckCmd.source_jws=2`; `RegisterNodeCmd.signer_jws=2, ed25519_signature=3`; `MarkActiveCmd.registration_seq=2, signer_jws=3, ed25519_signature=4`; `EvictNodeCmd.expected_registration_seq=3, authority_jws=4`; `AdmissionCode.ADMISSION_CODE_SOURCE_UNAVAILABLE=10`; `rpc ConsensusAdmin.EvictNode(EvictNodeRequest) returns (Ack)`.
- **Indexer id 0 is valid** (devnet2's founding indexer). The three single indexer-id fields are proto3 `optional` and `*uint64` in Go; nil (absent) and `&0` round-trip distinctly; read the proto field, never `GetOwnerIndexerId()`, and resolve a nil owner to the founding id with `wire.TableRegistrySnapshot.Owner`.
- **Feature string** `signed_claims_v1` (`arbiter.SignedClaimsFeature`); `arbiter.LocalNodeFeatures()` returns `[client_lanes_v1 signed_claims_v1]`. An SNode advertises it only with a claim signer; verifiers always. Because voters return `LocalNodeFeatures()` from `GetProtocolInfo`, **no arbiter release may be cut between Task 5 (the `CORE_TAG` pin) and Task 12.**
- **Signing domains (verbatim).** Enrolment statement `{network_id, genesis_snapshot_id, indexer_id, snode_node_id}`, purpose `arbiter-snode-enrollment-v1`, digest domain `arbiter-snode-enrollment-statement-v1`, ES256K by the entry's `signer`. SNode messages: purpose `arbiter-snode-message-v1`, kinds `registration`, `mark_active`, `result_claim`, `promotion_ack`, `cleanup_ack`, `table_purged`, digest domain `arbiter-snode-message-body-v1` over `{kind, {network_id, genesis_snapshot_id}, body}`, ES256K by the owning entry's `signer`. Verifier messages: kinds `registration`, `mark_active`, `table_purged`, digest domain `arbiter-verifier-message-body-v1`, lowercase-hex ed25519 over the ASCII hash string by the listed key. Eviction: purpose `arbiter-evict-node-v1`, digest domain `arbiter-evict-node-command-v1`, authority JWS bound to the exact `{network_id, genesis_snapshot_id, authority_epoch}`. Tokens are not idempotency keys: compare canonical bodies, never JWS bytes.
- **Consensus-update rules.** (1) The first `si_indexers` needs `table_registry` (earlier or same update), sets `verifiers` (≥ 3) in the same update and restates the founding entry (`indexer_id == table_registry.si_indexer_id`, `activation_block == table_registry.activation_block`). Its two lists are authoritative: in the same Apply every registered non-evicted SNODE that no entry names and every registered non-evicted VERIFIER that is not listed is evicted (reason `"not named by the signed-claims activation"`), while a listed verifier registered under another key, or a named SNode registered with another role, refuses the update; the writer gate counts writers after those evictions. `arbiter-admin` lists the nodes the activation would evict and submits only with `--evict-unnamed`, forwarded to the leader as gRPC metadata `arbiter-evict-unnamed` naming exactly those nodes; the leader-local gate accepts only that exact set. (2) Afterwards every update carries both lists; entries are never removed; `indexer_id`, `activation_block`, `snode_node_id` never change; `signer` changes only with a new `enrollment_jws` verifying under the new signer; an unchanged entry's `enrollment_jws` stays byte-identical. (3) A new entry's `activation_block > max(cursor.block_number, founding activation_block − 1)`. (4) Every new or rotated entry's enrolment statement verifies. (5) `snode_node_id` is unique across entries and differs from every verifier id and every other registered node. (6) Verifiers may be added and removed; a removed verifier's node is evicted in the same Apply (`"removed from verifiers"`); fewer than 3 non-evicted listed verifiers is refused. (7) The existing drain gate is unchanged. (8) The first set gives every existing incarnation `OwnerIndexerID = &founding` and sets `SeededIndexers = [founding]` iff `Seeded`. Every committed change of `si_indexers` and every seed calls `registryChangedLocked` exactly once. `max_writers ≥ len(si_indexers)`.
- **Registry rules.** After activation `AddTable` carries an enrolled owner and `created.block ≥ entry.activation_block`; before it the owner is absent. `SeedLegacyTables{IndexerID: nil}` is the founding seed (after activation it stamps `&founding`); `{IndexerID: &x}` needs entry x, unseeded, cursor exactly at `entry.activation_block − 1`. Guard: no `AddTable`, `RetireTables` or `AdvanceL2Cursor` at or beyond an unseeded entry's activation block. Membership: a `TableCreated` is SI iff its database's indexer at the event block has an entry and the block ≥ that entry's activation block. New watcher halt reason `HaltOwnerChanged`.
- **Admission.** After activation the source is `entry(owner(live incarnation)).snode_node_id`; if that node is not registered and `NodeActive`, answer `AdmissionCodeSourceUnavailable` with state unchanged (client_seq unspent). Before activation hash selection is unchanged. The snapshot-query lane keeps hash selection and must not be enabled on a network with more than one enrolled indexer.
- **Signed messages after activation.** SNODE registrations only for an entry's node id with roles exactly `[SNODE]`; VERIFIER registrations only for a listed `(node_id, pubkey)`; `registration_seq > RegistrationSeqs[node]`, except an exact duplicate (same seq, same canonical registration, valid signature), which is an idempotent no-op with no status change; `MarkActive` at the current seq, signed. RCs (parked or bound), promotion and cleanup acknowledgements and purge reports must verify under the owning entry's signer (verifier purge reports under the listed key). The acknowledging node must be the bound `SourceNode` of the statements the promotion covers; a non-owner `Applied:false` consumes nothing. `EvictNode` is accepted only through the authority-signed RPC with `expected_registration_seq == RegistrationSeqs[node]`, verified with `Validator.VerifyEvictNode` in Apply and `AuthorizeEvictNode` at the RPC; the RPC answers `FailedPrecondition` before the activation.
- **Purge.** `GetPurgeNodeSet` keeps returning every registered non-evicted SNODE and VERIFIER; completing a purge after activation needs reports from the incarnation owner's SNode and every non-evicted verifier, and a report from a non-owner SNode is refused.
- **Routing.** After activation promotions and cleanups are streamed only to the expected source node; before it they are broadcast as today.
- **Snapshot v19** is written only when `len(Params.SIIndexers) > 0` and carries the new params, `RegistrationSeqs`, incarnation owners and `SeededIndexers`; restore refuses any of them under a lower version byte.
- **`registration_seq`.** Signing SNodes and verifiers send `next := max(persisted+1, uint64(now().UnixMilli()))`, persisted durably (temp file + fsync + rename + directory fsync) before the first `RegisterNode` leaves, reused for every retry of that call and for the following `MarkActive`. Verifiers keep it in `<StateDir>/registration.json`.
- **Never edit existing golden or vector files:** `arbiter:fsm/testdata/*golden*.json`, `arbiter:accumulator/testdata/spent_ids_vectors.json`, `arbiter:fsm/testdata/statement_jws_v2*.json`, `arbiter-core:conformance/*golden*`, `arbiter-proto:conformance/testdata/*.binpb`. New frozen fixtures (`arbiter:fsm/testdata/pre_signed_claims_golden.json`, `arbiter-core:wire/signed_claims_golden_test.go`, `arbiter-core:authority/authoritytest` vectors) are consensus constants once committed.
- **Repository commands.** arbiter-proto: `make tools && make proto && make lint && make breaking && make test` (also `buf breaking --against '.git#tag=v0.9.0'`). arbiter-core, arbiter: `bazel build //... && bazel test //...`; after dependency changes `bazel mod tidy && bazel run //:gazelle`. arbiter-core pins arbiter-proto with `GOWORK=off go get github.com/sentioxyz/arbiter-proto@$PROTO_TAG && GOWORK=off go mod tidy && bazel mod tidy && bazel run //:gazelle`; arbiter pins arbiter-core with `bash scripts/update-arbiter-core.sh $CORE_TAG`.
- **Worktrees.** One URWT/Worktrunk worktree per repository at `~/src/remotesrc/claude/<repo>/multi-source-s1`, branch `urwt/claude/multi-source-s1`, created with the URWT command (never `git worktree add`, never `--yes`/`--clobber`). In agent-run commands `$W` is that worktree's absolute path; the Bash tool keeps no variables between calls, so prefix each command with `W=<path>;`.
- **Commits.** Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Code comments are English. Markdown is never hard-wrapped.
- **Remote and live actions** — pushes, PRs, merges, release workflows, tags — require explicit user confirmation in chat; commands handed to the user are fish.
- **References.** `contract §N` / `CONTRACT §N` in the tasks point to Appendix A (the binding cross-task contract, also binding for Plan S1-B); references to a task group's "Contract conflicts" or "Facts" point to Appendix B.

## Review Focus

1. **A data-plane node or the registry watcher sends new fields before the activation.** Expected: the leader proposes byte-identical legacy commands and old voters keep applying them; a command that still carries a new field is refused like an old decoder refuses it. Tests: Task 3 `TestStripSignedClaimsRestoresThePreActivationBytes`; Task 6 `TestWatcherProposesNoStageOneFieldBeforeActivation`; Task 8 `TestServerDropsSignedFieldsBeforeActivation`, `TestSignedFieldsAreRefusedBeforeActivation`; Task 9 `TestAcknowledgementsBeforeActivationAreUnchanged`, `TestPromotionIsBroadcastBeforeActivation`.
2. **The founding indexer has id 0.** Expected: `&0` survives every converter, view, snapshot and restore and is never confused with "no owner". Tests: Task 1 `TestOptionalIndexerIDsKeepZeroDistinctFromAbsent`; Task 2 `TestTableRegistrySnapshotCarriesSIIndexersAndOwners`; Task 6 `TestTableRegistryViewCarriesOwnership`, `TestValidateRegistryOwnership`; Task 10 `TestSnapshotV19AfterActivationRoundTrips` (devnet2-shaped fixtures with founding indexer 0).
3. **A database is deleted and recreated with the same id on another indexer.** Expected: no owner-change halt; the new incarnation gets the new owner; acknowledgements for the old incarnation's promotions are still checked against the old source because the expected source is the statements' bound source. Moving a live database without `DatabaseDeleted` halts the watcher. Tests: Task 6 `TestRecreatedDatabaseOnAnotherIndexerGetsTheNewOwner`, `TestWatcherHaltsWhenADatabaseChangesOwner`; Task 9 `TestSourceAcknowledgementFollowsTheStatementsIncarnation`.
4. **An SNode or verifier loses its state directory, or its registration is retried.** Expected: the next registration still carries a larger `registration_seq` (clock floor) and is accepted without operator action; an identical retried registration is a no-op that does not reset the node to Syncing. Tests: Task 4 `TestRegister_SeqStaysAboveAnEarlierRegistrationAfterTheStateFileIsLost`, `TestRegister_VerifierSeqStaysAboveAfterTheStateFileIsLost`, `TestRegister_RetriesReuseOneSeqAndSignature`; Task 8 `TestSNodeRegistrationAfterActivation`, `TestVerifierRegistrationAfterActivation`.
5. **Enrolment racing the watcher.** Expected: an activation block at or below the cursor is refused; the seed happens exactly at `activation_block − 1` even across a leader change; no event at the activation block is consumed before the seed. Tests: Task 5 `TestEnrolmentActivatesAfterTheRegistryCursor`; Task 6 `TestWatcherSeedsEachIndexerAtItsActivation`, `TestUnseededIndexerGuard`, `TestEndToEndTwoIndexersLeaderChangeAtEveryProposal`.

## Task order and dependencies

1 (arbiter-proto, `PROTO_TAG`) → 2 → 3 → 4 (arbiter-core, `CORE_TAG`) → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 (arbiter, `ARBITER_TAG` / `ARBITER_DIGEST`). Tasks 5–12 land on one arbiter branch and are released together (no intermediate release, see the feature-string constraint).

## Release ledger (fill in at cut time; later tasks read these names)

| Name | Meaning | Resolved by |
|---|---|---|
| `PROTO_TAG` | arbiter-proto release containing Task 1 | Task 1 Step 13 |
| `CORE_TAG` | arbiter-core release containing Tasks 2–4 | Task 4, release step |
| `ARBITER_TAG` / `ARBITER_DIGEST` | arbiter release containing Tasks 5–12 and its image index digest | Task 12, release step |

Plan S1-B consumes all three.

## File Structure

Line anchors in the tasks refer to arbiter-proto `2eb3917` (v0.9.0), arbiter-core `050490c` (v0.12.1) and arbiter `5ea60c8`.

**arbiter-proto** (Task 1)
- `proto/table_registry.proto` — `SIIndexerEntry`; owner and seed indexer ids (`optional`); registry snapshot lists; purge-report signatures.
- `proto/consensus.proto` — `VerifierEntry`, `EvictNodeRequest`, both lists on `ConsensusParamsUpdate` / `ConsensusMutableParams`, `ConsensusAdmin.EvictNode`.
- `proto/arbiter.proto`, `proto/raftlog.proto` — request-only signature carriers, `registration_seq`, Raft command signature fields, `ADMISSION_CODE_SOURCE_UNAVAILABLE`.
- `conformance/signed_claims_test.go` (new) and the existing conformance pins.

**arbiter-core** (Tasks 2–4)
- `consensus.go`, `types.go` — `SIIndexerEntry`, `VerifierEntry`, `EvictNodeCommand`, `SignedClaimsFeature`, `AdmissionCodeSourceUnavailable`, `NodeRegistration.RegistrationSeq`.
- `authority/consensus.go` — normalisation of the two lists. `authority/snode_messages.go`, `authority/evict_node.go` (new) — enrolment statement, SNode and verifier message signing/verification, authority-signed eviction. `authority/authoritytest/` (new) — fixed keys, fixtures and consensus test vectors.
- `wire/consensus.go`, `wire/convert.go`, `wire/table_registry.go`, `wire/command.go` — converters and command fields. `wire/requests.go`, `wire/signed_claims.go` (new) — `*FromRequest` / `*ToRequest`, `StripSignedClaims`. `wire/signed_claims_golden_test.go` (new) — frozen pre-activation bytes.
- `dataplane/tableset/reconciler.go` — `Config.Owner`. `dataplane/table_purge.go`, `dataplane/genesis.go` (new) — signed purge reports, `GenesisSnapshotID`.
- `snode/config.go`, `snode/state.go`, `snode/snode.go`, `snode/claims.go` (new) — signing SNode, persisted `registration_seq`, owner scoping.
- `verifier/config.go`, `verifier/verifier.go`, `verifier/claims.go`, `verifier/registration_state.go` (new) — signing verifier, `StateDir`.

**arbiter** (Tasks 5–12)
- `fsm/signed_claims.go` (new) — activation predicate, entry lookups, transition rules, activation evictions, message verification helpers, expected-source and purge-report rules, v19 validation.
- `fsm/state.go`, `fsm/params_identity.go`, `fsm/consensus_reads.go`, `fsm/consensus_updates.go`, `fsm/consensus_history.go`, `fsm/watch.go` — parameters and their rules.
- `fsm/table_registry.go`, `fsm/apply_table_registry.go`, `fsm/table_registry_validation.go` — owners, per-indexer seeds, the seed guard, owner-scoped purge completion.
- `fsm/admission.go`, `fsm/select.go` — owner-bound source and `SOURCE_UNAVAILABLE`.
- `fsm/apply.go` — signed registration, activation, RC, acknowledgements, eviction.
- `fsm/snapshot.go` — v19.
- `tableregistry/membership.go` (new), `chain.go`, `derive.go`, `watcher.go`, `auditor.go` — per-indexer membership, owners, seeds, `HaltOwnerChanged`.
- `server/membership.go`, `server/claims.go`, `server/gateway.go`, `server/table_purge.go`, `server/signed_messages.go` (new) — request conversion and pre-activation stripping. `server/consensus_admin.go`, `server/signed_claims_gate.go` (new) — the eviction RPC and the activation gate. `server/table_registry.go`, `server/ingress.go`.
- `orchestrator/promotion.go`, `orchestrator/cleanup.go` — routing to the expected source.
- `cmd/arbiter-admin/main.go` — `--si-indexer`, `--verifier`, `--evict-unnamed`, `node evict`, `consensus show`.
- `cmd/arbiter-verifier/`, `cmd/arbiter-snode/` — `state_dir`, `genesis_snapshot_id`, `indexer_id`, `ARBITER_SNODE_CLAIM_KEY_HEX`.
- `metrics/metrics.go` — `owner_changed` halt label.
- `README.md` — multi-source section.
- `fsm/testdata/pre_signed_claims_golden.json` (new, frozen once committed).

## Spec coverage map

| Spec item | Tasks |
|---|---|
| §4 D3 enrolment by governance, `si_indexers` | 1, 2, 5, 11 |
| §4 D4 owner per incarnation | 1, 2, 6 |
| §4 D5 owner-bound source | 7 |
| §4 D6 SNode identity = indexer signer, signed messages | 3, 4, 8 |
| §4 D7 governed verifier set | 1, 2, 5, 8, 11 |
| §4 D8 owner-scoped data plane | 4, 9 |
| §4 D11 naming unchanged | Global Constraints; frozen goldens in 2, 5, 12 |
| §4 D12 stage 1 only, liveness coupled | 12 (README); Plan S1-B runbook |
| §6.1 parameters, `max_writers` | 1, 2, 5 |
| §6.2 enrolment constraints, statement, signer rotation, `arbiter-admin` | 3, 5, 11 |
| §6.3 registry: owner, membership, per-indexer seed, guard, owner halt, views | 1, 2, 6 |
| §6.4 admission, `SOURCE_UNAVAILABLE` | 1, 2, 7 |
| §6.5 signed messages, `registration_seq`, verifier list, authority eviction | 1, 3, 4, 8, 11 |
| §6.6 owner routing, source acknowledgement, purge completion, Spec D compatibility | 4, 9 |
| §6.7 snapshot v19, activation gate | 10, 11 |
| §8 owner filter, own tables, signing, genesis snapshot id, verifier state | 4, 8 |
| §15 stage-1 FSM tests | 5–10 (each rule has a named test); replay determinism 5, 7, 12 |
| §16 risks | 12 (README) |
| §9, §10, §11, §12, §13 | Plan S1-B |

---

## Task 1: arbiter-proto — enrolment, verifier set, signatures, owner, SOURCE_UNAVAILABLE, EvictNode (S1)

**Files:**
- Modify: `proto/table_registry.proto` — new `SIIndexerEntry` after `ClientLaneParams` (lines 22-28); `SeedLegacyTablesCmd` (55-59); `AddTableCmd` (61-70); `RecordTablePurgedCmd` (86-90); `TableIncarnation` (124-149); `TableRegistrySnapshot` (151-162); the `PurgeNodeSet` comment (164-167)
- Modify: `proto/consensus.proto` — `ConsensusMutableParams` (14-27), `ConsensusParamsUpdate` (29-49), the `ProtocolInfo.features` comment (91-93), new `VerifierEntry` and `EvictNodeRequest` after `NodeFeatures` (120-126), `service ConsensusAdmin` (128-149)
- Modify: `proto/arbiter.proto` — `AdmissionCode` (89-116), `RCRecord` (155-169), `PromotionAck` (290-312), `CleanupAck` (314-319), `NodeRegistration` (413-430), `NodeRef` (432-434), `service PromotionGateway` (510-525), `service Membership` (565-571)
- Modify: `proto/raftlog.proto` — `RegisterRCCmd` (53-56), `RecordPromotionAckCmd` (88-92), `RecordCleanupAckCmd` (107-110), `RegisterNodeCmd` / `MarkActiveCmd` / `EvictNodeCmd` (132-143)
- Regenerate: `gen/pb/arbiter.pb.go`, `gen/pb/arbiter_grpc.pb.go`, `gen/pb/consensus.pb.go`, `gen/pb/consensus_grpc.pb.go`, `gen/pb/raftlog.pb.go`, `gen/pb/table_registry.pb.go`
- Create: `conformance/signed_claims_test.go`
- Modify: `conformance/consensus_test.go`, `conformance/client_lanes_test.go`, `conformance/table_registry_test.go`, `conformance/table_registry_read_test.go`, `conformance/table_purge_test.go`, `conformance/snapshot_query_test.go`
- Never touch: `conformance/testdata/*.binpb` (independently exported baselines, hash-pinned by `TestSnapshotQueryDescriptorFixtureIdentities`) or anything under `gen/` by hand.

**Interfaces:**
- Consumes: nothing. Base is arbiter-proto `v0.9.0` (`2eb3917`, equal to `origin/main` on 2026-10-10).
- Produces (Go package `pb`, imported as `pb "github.com/sentioxyz/arbiter-proto/gen/pb"`; the proto package is `arbiter`, so full names are `arbiter.<Name>`):
  - `pb.SIIndexerEntry{IndexerId uint64; ActivationBlock uint64; Signer string; SnodeNodeId string; EnrollmentJws string}` (defined in `table_registry.proto`), `pb.VerifierEntry{NodeId string; Ed25519Pubkey []byte}`, `pb.EvictNodeRequest{NodeId string; ExpectedRegistrationSeq uint64; Reason string; AuthorityJws string}` (both in `consensus.proto`).
  - `pb.ConsensusParamsUpdate.SiIndexers []*pb.SIIndexerEntry` (11) / `.Verifiers []*pb.VerifierEntry` (12); `pb.ConsensusMutableParams.SiIndexers` (6) / `.Verifiers` (7); `pb.TableRegistrySnapshot.SiIndexers` (7) / `.SeededIndexers []uint64` (8).
  - proto3 `optional`, Go `*uint64`: `pb.TableIncarnation.OwnerIndexerId` (18), `pb.AddTableCmd.OwnerIndexerId` (8), `pb.SeedLegacyTablesCmd.IndexerId` (3).
  - `pb.RecordTablePurgedCmd.SignerJws` (3) / `.Ed25519Signature` (4); `pb.NodeRegistration.RegistrationSeq uint64` (6) / `.SignerJws` (7) / `.Ed25519Signature` (8); `pb.NodeRef.RegistrationSeq uint64` (2) / `.SignerJws` (3) / `.Ed25519Signature` (4); `pb.RCRecord.SourceJws` (6); `pb.PromotionAck.SourceJws` (10); `pb.CleanupAck.SourceJws` (5).
  - Raft carriers: `pb.RegisterRCCmd.SourceJws` (2); `pb.RecordPromotionAckCmd.SourceJws` (2); `pb.RecordCleanupAckCmd.SourceJws` (2); `pb.RegisterNodeCmd.SignerJws` (2) / `.Ed25519Signature` (3); `pb.MarkActiveCmd.RegistrationSeq uint64` (2) / `.SignerJws` (3) / `.Ed25519Signature` (4); `pb.EvictNodeCmd.ExpectedRegistrationSeq uint64` (3) / `.AuthorityJws` (4).
  - `pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE` (10).
  - `pb.ConsensusAdminClient.EvictNode(ctx context.Context, in *pb.EvictNodeRequest, opts ...grpc.CallOption) (*pb.Ack, error)`, `pb.ConsensusAdminServer.EvictNode(context.Context, *pb.EvictNodeRequest) (*pb.Ack, error)`, `pb.UnimplementedConsensusAdminServer.EvictNode` (answers `codes.Unimplemented`), `pb.ConsensusAdmin_EvictNode_FullMethodName` = `"/arbiter.ConsensusAdmin/EvictNode"`.
  - The feature string `"signed_claims_v1"` appears here only in the `ProtocolInfo.features` and `NodeRegistration.features` comments; the Go constant `arbiter.SignedClaimsFeature` is Task 2's.
  - Release ledger: `PROTO_TAG` (Step 13).

**Semantics the new comments carry** (Tasks 2–12 implement them; the proto only documents them):
- *Strict-decoder rule.* Every new field reachable from a `RaftCommand` — `RegisterRCCmd.source_jws`, `RecordPromotionAckCmd.source_jws`, `RecordCleanupAckCmd.source_jws`, `RegisterNodeCmd.signer_jws` / `ed25519_signature` and `registration.registration_seq`, `MarkActiveCmd` 2–4, `EvictNodeCmd` 3–4, `AddTableCmd.owner_indexer_id`, `SeedLegacyTablesCmd.indexer_id`, `RecordTablePurgedCmd` 3–4, `UpdateConsensusParamsCmd.update.si_indexers` / `verifiers` — stays zero, empty or absent in every command proposed before the signed-claims activation. The activation update is the first command that carries any of them.
- *Request-only carriers.* `RCRecord.source_jws`, `PromotionAck.source_jws`, `CleanupAck.source_jws`, `NodeRegistration.signer_jws` and `NodeRegistration.ed25519_signature` exist so the unchanged RPCs can carry a signature over a body that excludes it. The server moves each into its Raft command's sibling field, and an embedded message inside a `RaftCommand` never carries it (the `NodeRegistration.features` rule). `NodeRef` is never embedded; `MarkActiveCmd` gets copies. `RecordTablePurgedCmd` is both the `SubmitTablePurged` request and the Raft command, so its fields 3–4 serve both.
- *Signature domains.* SNode messages: ES256K compact JWS by the `signer` of the `si_indexers` entry whose `snode_node_id` is the node, purpose `arbiter-snode-message-v1`, kinds `registration`, `mark_active`, `result_claim`, `promotion_ack`, `cleanup_ack`, `table_purged`, bound to {network id, genesis snapshot id}. Verifier messages: hex ed25519 signature by the listed key over the verifier-message hash (kinds `registration`, `mark_active`, `table_purged`). Enrolment statement: purpose `arbiter-snode-enrollment-v1`. Eviction: authority JWS, purpose `arbiter-evict-node-v1`, bound to {network id, genesis snapshot id, authority epoch}.
- *`optional` only where one scalar names an indexer* (`owner_indexer_id` twice, `SeedLegacyTablesCmd.indexer_id`): indexer 0 is valid (devnet2's founding indexer), so absent (before activation, or the founding seed) must stay distinct from an explicit 0. `SIIndexerEntry.indexer_id` is plain: an entry exists because it is listed.
- *Empty repeated means unset.* `si_indexers` and `verifiers` are never legitimately set to an empty list (activation needs at least one indexer and three verifiers, and neither list can shrink to empty), so proto3's absent-equals-empty is safe and matches the Go mirrors' nil rule.

- [ ] **Step 1: Create the worktree**

```bash
git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-proto fetch origin --tags
bash -c '
repo=$1 runtime=$2 name=$3 branch=${4:-urwt/$2/$3} base=${5:-origin/main}
case $name in ""|[._-]*|*[!A-Za-z0-9._-]*) echo "urwt: invalid task name: $name" >&2; exit 2;; esac
if [[ $branch == pr:* || $branch == https://* ]] ||
   git -C "$repo" show-ref --quiet --verify "refs/heads/$branch" ||
   git -C "$repo" show-ref --quiet --verify "refs/remotes/origin/$branch"; then
  create=()
else
  create=(--create --base "$base")
fi
command wt -C "$repo" \
  --config-set "worktree-path = \"~/src/remotesrc/$runtime/{{ repo }}/$name\"" \
  switch "${create[@]}" "$branch" --no-cd --format=json
' urwt /Users/uranuswch/Dev/sentio_xyz/arbiter-proto claude multi-source-s1
```

Expected: one JSON object with `"action": "created"` whose `path` is `~/src/remotesrc/claude/arbiter-proto/multi-source-s1` (expanded; `~/src/remotesrc` is a symlink to `/Volumes/SamsungSSD/src`). `W` below is that path; the Bash tool keeps no variables between calls, so prefix every command with `W=<that path>;`. Then:

Run: `git -C "$W" rev-parse HEAD && git -C "$W" describe --tags`
Expected: `2eb391711d85c308edf7d1388942d8c49bbb10c7` and `v0.9.0`. If `origin/main` has moved past `v0.9.0`, continue only if Step 2 still prints exactly the lines below.

- [ ] **Step 2: Prove every contract number is free at the base, and audit `NodeRef`**

Run (before any edit):

```bash
cd "$W" && buf build -o '-#format=json' | python3 -I -c '
import json, sys
img = json.load(sys.stdin)
msgs, enums, svcs = {}, {}, {}
for f in img["file"]:
    for m in f.get("messageType", []):
        msgs[m["name"]] = (f["name"], m)
    for e in f.get("enumType", []):
        enums[e["name"]] = (f["name"], e)
    for s in f.get("service", []):
        svcs[s["name"]] = [x["name"] for x in s.get("method", [])]
for name in ["ConsensusMutableParams", "ConsensusParamsUpdate", "TableRegistrySnapshot", "TableIncarnation", "AddTableCmd", "SeedLegacyTablesCmd", "RecordTablePurgedCmd", "NodeRegistration", "NodeRef", "RCRecord", "PromotionAck", "CleanupAck", "RegisterRCCmd", "RecordPromotionAckCmd", "RecordCleanupAckCmd", "RegisterNodeCmd", "MarkActiveCmd", "EvictNodeCmd"]:
    file, m = msgs[name]
    nums = sorted(x["number"] for x in m.get("field", []))
    reserved = m.get("reservedRange", []) + m.get("reservedName", [])
    print("%s %s max=%d contiguous=%s reserved=%s" % (file, name, nums[-1], nums == list(range(1, nums[-1] + 1)), reserved or None))
file, e = enums["AdmissionCode"]
print("%s AdmissionCode max=%d reserved=%s" % (file, max(v.get("number", 0) for v in e["value"]), e.get("reservedRange") or None))
for name in ["SIIndexerEntry", "VerifierEntry", "EvictNodeRequest"]:
    print("%s %s" % (name, "taken" if name in msgs else "free"))
print("ConsensusAdmin.EvictNode %s" % ("taken" if "EvictNode" in svcs["ConsensusAdmin"] else "free"))
'
```

Expected, verbatim:

```
consensus.proto ConsensusMutableParams max=5 contiguous=True reserved=None
consensus.proto ConsensusParamsUpdate max=10 contiguous=True reserved=None
table_registry.proto TableRegistrySnapshot max=6 contiguous=True reserved=None
table_registry.proto TableIncarnation max=17 contiguous=True reserved=None
table_registry.proto AddTableCmd max=7 contiguous=True reserved=None
table_registry.proto SeedLegacyTablesCmd max=2 contiguous=True reserved=None
table_registry.proto RecordTablePurgedCmd max=2 contiguous=True reserved=None
arbiter.proto NodeRegistration max=5 contiguous=True reserved=None
arbiter.proto NodeRef max=1 contiguous=True reserved=None
arbiter.proto RCRecord max=5 contiguous=True reserved=None
arbiter.proto PromotionAck max=9 contiguous=True reserved=None
arbiter.proto CleanupAck max=4 contiguous=True reserved=None
raftlog.proto RegisterRCCmd max=1 contiguous=True reserved=None
raftlog.proto RecordPromotionAckCmd max=1 contiguous=True reserved=None
raftlog.proto RecordCleanupAckCmd max=1 contiguous=True reserved=None
raftlog.proto RegisterNodeCmd max=1 contiguous=True reserved=None
raftlog.proto MarkActiveCmd max=1 contiguous=True reserved=None
raftlog.proto EvictNodeCmd max=2 contiguous=True reserved=None
arbiter.proto AdmissionCode max=9 reserved=None
SIIndexerEntry free
VerifierEntry free
EvictNodeRequest free
ConsensusAdmin.EvictNode free
```

So every contract number is the next free one: `ConsensusMutableParams` 6–7, `ConsensusParamsUpdate` 11–12, `TableRegistrySnapshot` 7–8, `TableIncarnation` 18, `AddTableCmd` 8, `SeedLegacyTablesCmd` 3, `RecordTablePurgedCmd` 3–4, `NodeRegistration` 6–8, `NodeRef` 2–4, `RCRecord` 6, `PromotionAck` 10, `CleanupAck` 5, `RegisterRCCmd` / `RecordPromotionAckCmd` / `RecordCleanupAckCmd` 2, `RegisterNodeCmd` 2–3, `MarkActiveCmd` 2–4, `EvictNodeCmd` 3–4, `AdmissionCode` 10. If any line differs, stop and report a contract conflict; never pick another number.

Run: `cd "$W" && grep -n 'NodeRef' proto/*.proto`
Expected:

```
proto/arbiter.proto:432:message NodeRef {
proto/arbiter.proto:570:  rpc MarkActive (NodeRef) returns (Ack) {}
```

`NodeRef`'s only proto user is the `Membership.MarkActive` request; no message and no Raft command embeds it (`MarkActiveCmd` has its own `node_id`). Its Go users (audited at arbiter-core `050490c`, arbiter `5ea60c8`) are keyed literals or handlers that read only `GetNodeId()`: arbiter-core `snode/snode.go:161` and `verifier/verifier.go:135` (clients) with fakes `snode/snode_test.go:192`, `verifier/verifier_fakes_test.go:163`; arbiter `server/membership.go:54-62` (copies only `GetNodeId()` into `wire.MarkActive`), `server/server_test.go:1065,1068`, `integration/fakes_test.go:117,129`, `cmd/internal/startup/role_fixtures_test.go:302`; none in housegate or sentio-node. Fields 2–4 are therefore backward compatible both ways: an old client's request decodes with zero values (the pre-activation path), an old server keeps the new bytes as unknown fields and never proposes them, and keyed literals keep compiling.

- [ ] **Step 3: Write the failing conformance test**

Create `conformance/signed_claims_test.go` (style of `conformance/client_lanes_test.go`; the Unimplemented-stub check mirrors `TestSnapshotQueryServicesRemainUnimplemented`):

```go
package conformance

import (
	"bytes"
	"context"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestSignedClaimsFieldNumbers pins the stage-1 multi-source SI additions
// (housegate spec 2026-10-10 §6; Plan S1-A contract §1). Each number is a
// consensus constant: arbiter-core's strict Raft command decoder refuses any
// field its descriptor does not know, so a voter that disagrees on one of
// these numbers forks on the first command that carries it.
func TestSignedClaimsFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		optional bool // proto3 `optional`: an explicit 0 stays distinct from absent
		msgType  protoreflect.FullName
	}{
		{&pb.SIIndexerEntry{}, "indexer_id", 1, protoreflect.Uint64Kind, false, false, ""},
		{&pb.SIIndexerEntry{}, "activation_block", 2, protoreflect.Uint64Kind, false, false, ""},
		{&pb.SIIndexerEntry{}, "signer", 3, protoreflect.StringKind, false, false, ""},
		{&pb.SIIndexerEntry{}, "snode_node_id", 4, protoreflect.StringKind, false, false, ""},
		{&pb.SIIndexerEntry{}, "enrollment_jws", 5, protoreflect.StringKind, false, false, ""},
		{&pb.VerifierEntry{}, "node_id", 1, protoreflect.StringKind, false, false, ""},
		{&pb.VerifierEntry{}, "ed25519_pubkey", 2, protoreflect.BytesKind, false, false, ""},
		{&pb.ConsensusParamsUpdate{}, "si_indexers", 11, protoreflect.MessageKind, true, false, "arbiter.SIIndexerEntry"},
		{&pb.ConsensusParamsUpdate{}, "verifiers", 12, protoreflect.MessageKind, true, false, "arbiter.VerifierEntry"},
		{&pb.ConsensusMutableParams{}, "si_indexers", 6, protoreflect.MessageKind, true, false, "arbiter.SIIndexerEntry"},
		{&pb.ConsensusMutableParams{}, "verifiers", 7, protoreflect.MessageKind, true, false, "arbiter.VerifierEntry"},
		{&pb.EvictNodeRequest{}, "node_id", 1, protoreflect.StringKind, false, false, ""},
		{&pb.EvictNodeRequest{}, "expected_registration_seq", 2, protoreflect.Uint64Kind, false, false, ""},
		{&pb.EvictNodeRequest{}, "reason", 3, protoreflect.StringKind, false, false, ""},
		{&pb.EvictNodeRequest{}, "authority_jws", 4, protoreflect.StringKind, false, false, ""},
		{&pb.TableRegistrySnapshot{}, "si_indexers", 7, protoreflect.MessageKind, true, false, "arbiter.SIIndexerEntry"},
		{&pb.TableRegistrySnapshot{}, "seeded_indexers", 8, protoreflect.Uint64Kind, true, false, ""},
		{&pb.TableIncarnation{}, "owner_indexer_id", 18, protoreflect.Uint64Kind, false, true, ""},
		{&pb.AddTableCmd{}, "owner_indexer_id", 8, protoreflect.Uint64Kind, false, true, ""},
		{&pb.SeedLegacyTablesCmd{}, "indexer_id", 3, protoreflect.Uint64Kind, false, true, ""},
		{&pb.RecordTablePurgedCmd{}, "signer_jws", 3, protoreflect.StringKind, false, false, ""},
		{&pb.RecordTablePurgedCmd{}, "ed25519_signature", 4, protoreflect.StringKind, false, false, ""},
		{&pb.NodeRegistration{}, "registration_seq", 6, protoreflect.Uint64Kind, false, false, ""},
		{&pb.NodeRegistration{}, "signer_jws", 7, protoreflect.StringKind, false, false, ""},
		{&pb.NodeRegistration{}, "ed25519_signature", 8, protoreflect.StringKind, false, false, ""},
		{&pb.NodeRef{}, "registration_seq", 2, protoreflect.Uint64Kind, false, false, ""},
		{&pb.NodeRef{}, "signer_jws", 3, protoreflect.StringKind, false, false, ""},
		{&pb.NodeRef{}, "ed25519_signature", 4, protoreflect.StringKind, false, false, ""},
		{&pb.RCRecord{}, "source_jws", 6, protoreflect.StringKind, false, false, ""},
		{&pb.PromotionAck{}, "source_jws", 10, protoreflect.StringKind, false, false, ""},
		{&pb.CleanupAck{}, "source_jws", 5, protoreflect.StringKind, false, false, ""},
		{&pb.RegisterRCCmd{}, "source_jws", 2, protoreflect.StringKind, false, false, ""},
		{&pb.RecordPromotionAckCmd{}, "source_jws", 2, protoreflect.StringKind, false, false, ""},
		{&pb.RecordCleanupAckCmd{}, "source_jws", 2, protoreflect.StringKind, false, false, ""},
		{&pb.RegisterNodeCmd{}, "signer_jws", 2, protoreflect.StringKind, false, false, ""},
		{&pb.RegisterNodeCmd{}, "ed25519_signature", 3, protoreflect.StringKind, false, false, ""},
		{&pb.MarkActiveCmd{}, "registration_seq", 2, protoreflect.Uint64Kind, false, false, ""},
		{&pb.MarkActiveCmd{}, "signer_jws", 3, protoreflect.StringKind, false, false, ""},
		{&pb.MarkActiveCmd{}, "ed25519_signature", 4, protoreflect.StringKind, false, false, ""},
		{&pb.EvictNodeCmd{}, "expected_registration_seq", 3, protoreflect.Uint64Kind, false, false, ""},
		{&pb.EvictNodeCmd{}, "authority_jws", 4, protoreflect.StringKind, false, false, ""},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated || f.HasOptionalKeyword() != tc.optional {
				t.Fatalf("field = %v, want number %d kind %s repeated %v optional %v", f, tc.number, tc.kind, tc.repeated, tc.optional)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	for msg, want := range map[proto.Message]int{
		&pb.SIIndexerEntry{}: 5, &pb.VerifierEntry{}: 2, &pb.EvictNodeRequest{}: 4,
		&pb.ConsensusParamsUpdate{}: 12, &pb.ConsensusMutableParams{}: 7,
		&pb.TableRegistrySnapshot{}: 8, &pb.TableIncarnation{}: 18, &pb.AddTableCmd{}: 8,
		&pb.SeedLegacyTablesCmd{}: 3, &pb.RecordTablePurgedCmd{}: 4,
		&pb.NodeRegistration{}: 8, &pb.NodeRef{}: 4, &pb.RCRecord{}: 6, &pb.PromotionAck{}: 10, &pb.CleanupAck{}: 5,
		&pb.RegisterRCCmd{}: 2, &pb.RecordPromotionAckCmd{}: 2, &pb.RecordCleanupAckCmd{}: 2,
		&pb.RegisterNodeCmd{}: 3, &pb.MarkActiveCmd{}: 4, &pb.EvictNodeCmd{}: 4,
	} {
		if got := msg.ProtoReflect().Descriptor().Fields().Len(); got != want {
			t.Fatalf("%s field count = %d, want %d", msg.ProtoReflect().Descriptor().Name(), got, want)
		}
	}
	// SIIndexerEntry lives in table_registry.proto: consensus.proto imports
	// that file, so defining the entry in consensus.proto would make
	// TableRegistrySnapshot.si_indexers an import cycle.
	for _, tc := range []struct {
		file protoreflect.FileDescriptor
		name protoreflect.Name
	}{
		{pb.File_table_registry_proto, "SIIndexerEntry"},
		{pb.File_consensus_proto, "VerifierEntry"},
		{pb.File_consensus_proto, "EvictNodeRequest"},
	} {
		if tc.file.Messages().ByName(tc.name) == nil {
			t.Fatalf("%s is not defined in %s", tc.name, tc.file.Path())
		}
	}
}

func TestSourceUnavailableAdmissionCodeIsTen(t *testing.T) {
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE); got != 10 {
		t.Fatalf("ADMISSION_CODE_SOURCE_UNAVAILABLE = %d, want 10", got)
	}
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED); got != 9 {
		t.Fatalf("ADMISSION_CODE_LANE_BUDGET_EXCEEDED moved to %d", got)
	}
	if n := pb.AdmissionCode(0).Descriptor().Values().Len(); n != 11 {
		t.Fatalf("AdmissionCode has %d values, want 11", n)
	}
}

// TestSignedClaimsRPCSignatures pins the authority-signed eviction RPC and the
// request messages that carry the new signature fields. The data-plane RPCs
// keep their request types: the signatures ride inside NodeRegistration,
// NodeRef, RCRecord, PromotionAck and CleanupAck, and SubmitTablePurged keeps
// taking the replicated RecordTablePurgedCmd, whose signer_jws and
// ed25519_signature are therefore the RPC's fields too.
func TestSignedClaimsRPCSignatures(t *testing.T) {
	for _, tc := range []struct {
		file          protoreflect.FileDescriptor
		service       protoreflect.Name
		method        protoreflect.Name
		input, output protoreflect.FullName
	}{
		{pb.File_consensus_proto, "ConsensusAdmin", "EvictNode", "arbiter.EvictNodeRequest", "arbiter.Ack"},
		{pb.File_arbiter_proto, "Membership", "RegisterNode", "arbiter.NodeRegistration", "arbiter.Ack"},
		{pb.File_arbiter_proto, "Membership", "MarkActive", "arbiter.NodeRef", "arbiter.Ack"},
		{pb.File_arbiter_proto, "SourceClaims", "RegisterResultClaim", "arbiter.RCRecord", "arbiter.Ack"},
		{pb.File_arbiter_proto, "PromotionGateway", "AckPromotion", "arbiter.PromotionAck", "arbiter.Ack"},
		{pb.File_arbiter_proto, "PromotionGateway", "AckCleanup", "arbiter.CleanupAck", "arbiter.Ack"},
		{pb.File_arbiter_proto, "PromotionGateway", "SubmitTablePurged", "arbiter.RecordTablePurgedCmd", "arbiter.Ack"},
	} {
		svc := tc.file.Services().ByName(tc.service)
		if svc == nil {
			t.Fatalf("service %s missing", tc.service)
		}
		m := svc.Methods().ByName(tc.method)
		if m == nil || m.Input().FullName() != tc.input || m.Output().FullName() != tc.output || m.IsStreamingClient() || m.IsStreamingServer() {
			t.Fatalf("%s.%s = %v, want unary %s -> %s", tc.service, tc.method, m, tc.input, tc.output)
		}
	}
	// Servers that embed the generated stub answer Unimplemented until the
	// arbiter implements the authority check.
	if _, err := (pb.UnimplementedConsensusAdminServer{}).EvictNode(context.Background(), &pb.EvictNodeRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("UnimplementedConsensusAdminServer.EvictNode = %v, want Unimplemented", err)
	}
}

// TestOptionalIndexerIDsKeepZeroDistinctFromAbsent pins why the three single
// indexer-id fields are proto3 optional: indexer 0 is a valid owner (devnet2's
// founding indexer), so an explicit 0 must survive the wire and differ from
// absent, and an absent id must leave the encoding exactly as a pre-activation
// command encodes it.
func TestOptionalIndexerIDsKeepZeroDistinctFromAbsent(t *testing.T) {
	for _, tc := range []struct {
		name         string
		absent, zero proto.Message
		number       protowire.Number
	}{
		{"AddTableCmd.owner_indexer_id",
			&pb.AddTableCmd{DatabaseId: "db", TableId: "t", SchemaVersion: 1},
			&pb.AddTableCmd{DatabaseId: "db", TableId: "t", SchemaVersion: 1, OwnerIndexerId: proto.Uint64(0)}, 8},
		{"SeedLegacyTablesCmd.indexer_id",
			&pb.SeedLegacyTablesCmd{AtBlock: &pb.L2BlockRef{Number: 5508930}},
			&pb.SeedLegacyTablesCmd{AtBlock: &pb.L2BlockRef{Number: 5508930}, IndexerId: proto.Uint64(0)}, 3},
		{"TableIncarnation.owner_indexer_id",
			&pb.TableIncarnation{Seq: 1, DatabaseId: "db", TableId: "t"},
			&pb.TableIncarnation{Seq: 1, DatabaseId: "db", TableId: "t", OwnerIndexerId: proto.Uint64(0)}, 18},
	} {
		t.Run(tc.name, func(t *testing.T) {
			absent, err := proto.Marshal(tc.absent)
			if err != nil {
				t.Fatal(err)
			}
			zero, err := proto.Marshal(tc.zero)
			if err != nil {
				t.Fatal(err)
			}
			// The new field has the message's highest number, so an explicit 0
			// is the absent encoding plus one tag and a zero varint.
			want := protowire.AppendVarint(protowire.AppendTag(bytes.Clone(absent), tc.number, protowire.VarintType), 0)
			if !bytes.Equal(zero, want) {
				t.Fatalf("explicit 0 encodes as %x, want %x", zero, want)
			}
			fd := tc.zero.ProtoReflect().Descriptor().Fields().ByNumber(tc.number)
			for _, c := range []struct {
				raw     []byte
				present bool
			}{{zero, true}, {absent, false}} {
				back := tc.zero.ProtoReflect().Type().New()
				if err := proto.Unmarshal(c.raw, back.Interface()); err != nil {
					t.Fatal(err)
				}
				if back.Has(fd) != c.present {
					t.Fatalf("presence after decoding %x = %v, want %v", c.raw, back.Has(fd), c.present)
				}
			}
		})
	}
}
```

- [ ] **Step 4: Run it and watch it fail**

Run: `cd "$W" && go test ./conformance/ -run 'TestSignedClaims|TestSourceUnavailable|TestOptionalIndexerIDs' -v`
Expected: `FAIL	github.com/sentioxyz/arbiter-proto/conformance [build failed]`, starting with `conformance/signed_claims_test.go:…: undefined: pb.SIIndexerEntry` (then `undefined: pb.VerifierEntry`, `undefined: pb.EvictNodeRequest`, …, `too many errors`).

- [ ] **Step 5: Edit `proto/table_registry.proto`**

After `message ClientLaneParams { … }`, add `SIIndexerEntry`. It must live in this file, not in `consensus.proto`: `consensus.proto` already imports `table_registry.proto` and `TableRegistrySnapshot` carries the list, so the reverse import would be a cycle (the `ClientLaneParams` precedent). Its full name stays `arbiter.SIIndexerEntry`.

```proto
message ClientLaneParams {
  uint32 max_lanes_per_account = 1;
}

// SIIndexerEntry enrols one indexer in the network's storage-integrity layer
// (housegate spec 2026-10-10 D3, §6.1-§6.2). The si_indexers consensus
// parameter lists these entries sorted by indexer_id and is append-only:
// indexer_id, activation_block and snode_node_id never change, and signer
// changes only together with a new enrollment_jws signed by the new signer.
// The founding indexer of an existing registry is restated as the entry whose
// indexer_id and activation_block equal TableRegistryParams.si_indexer_id and
// TableRegistryParams.activation_block. Defined here because consensus.proto
// imports this file and TableRegistrySnapshot carries the list.
message SIIndexerEntry {
  // IndexerRegistry id. 0 is a valid id (devnet2's founding indexer): an
  // entry exists because it is listed, never because its id is nonzero.
  uint64 indexer_id = 1;
  // First L2 block whose TableCreated events open SI incarnations for this
  // indexer; at least 1. An appended entry's value must exceed the registry's
  // committed L2 cursor block. Its Legacy seed is read at activation_block - 1.
  uint64 activation_block = 2;
  // Lowercase 0x + 40 hex: the indexer's IndexerRegistry signer. Its
  // secp256k1 key signs every message of the indexer's SNode (D6).
  string signer = 3;
  // Node id of the indexer's SNode, the source of every statement on the
  // indexer's tables (D5). Unique across entries and verifier entries, and
  // never the id of another registered node.
  string snode_node_id = 4;
  // ES256K compact JWS by signer with purpose "arbiter-snode-enrollment-v1"
  // over {network_id, genesis_snapshot_id, indexer_id, snode_node_id}: the
  // indexer operator consents to, and controls, this SNode identity (§6.2).
  // Byte-identical in every later update unless signer rotates.
  string enrollment_jws = 5;
}
```

Replace `SeedLegacyTablesCmd` and its comment with:

```proto
// Once per SI indexer, at that indexer's activation_block - 1: every table
// active on the indexer.
message SeedLegacyTablesCmd {
  L2BlockRef at_block = 1;
  repeated LegacyTable tables = 2;
  // The seeded indexer (housegate spec 2026-10-10 §6.3). Absent: the founding
  // indexer's seed at TableRegistryParams.activation_block - 1, the only form
  // before the signed-claims activation. Present only after the activation,
  // naming an unseeded si_indexers entry whose activation_block - 1 is
  // at_block.number. optional because indexer 0 is a valid id; never proposed
  // before the activation, when a voter whose decoder lacks this field would
  // refuse the command.
  optional uint64 indexer_id = 3;
}
```

Append to `AddTableCmd`, after `schema_json = 7`:

```proto
  string schema_json = 7;
  // The owning indexer (housegate spec 2026-10-10 D4): the database's indexer
  // at the created block, resolved by the leader and re-derived by every
  // auditor. Required after the signed-claims activation, where it names an
  // si_indexers entry whose activation_block <= created.block_number; absent
  // before it. Apply refuses either violation. optional because indexer 0 is
  // a valid owner.
  optional uint64 owner_indexer_id = 8;
}
```

Replace `RecordTablePurgedCmd` and its comment with the following. It is also the request of `PromotionGateway.SubmitTablePurged` (pinned by `conformance/table_purge_test.go:20-23`), so these two fields are the purge-report RPC's fields too and no other message changes for the purge report:

```proto
// A data-plane role dropped its hg_* tables for this incarnation. Also the
// request of PromotionGateway.SubmitTablePurged, so the signature fields below
// are carried by the RPC and the Raft command alike.
message RecordTablePurgedCmd {
  string node_id = 1;
  uint64 incarnation_seq = 2;
  // SNODE reporter, after the signed-claims activation (housegate spec
  // 2026-10-10 §6.5): ES256K compact JWS by the signer of the si_indexers entry
  // whose snode_node_id is node_id, purpose "arbiter-snode-message-v1", kind
  // "table_purged", body {node_id, incarnation_seq}, bound to the network id
  // and genesis snapshot id. Empty for a verifier.
  string signer_jws = 3;
  // VERIFIER reporter, after the signed-claims activation: hex ed25519
  // signature by the node's verifiers-entry key over the "table_purged"
  // verifier-message hash of the same body and context. Empty for an SNode.
  // Before the activation the leader proposes neither signature.
  string ed25519_signature = 4;
}
```

Append to `TableIncarnation`, after `purged_by = 17`:

```proto
  repeated string purged_by = 17;
  // The owning SI indexer (housegate spec 2026-10-10 D4): the database's
  // indexer at the TableCreated block; never changes. Absent before the
  // signed-claims activation, when every incarnation belongs to
  // TableRegistryParams.si_indexer_id; the activation sets it on every
  // existing incarnation and every later one is recorded with it. optional
  // because indexer 0 is a valid owner.
  optional uint64 owner_indexer_id = 18;
}
```

Append to `TableRegistrySnapshot`, after `client_lanes = 6`:

```proto
  ClientLaneParams client_lanes = 6;
  // The committed si_indexers parameter, sorted by indexer_id, so data-plane
  // nodes and hosts learn enrolments through the registry follower (housegate
  // spec 2026-10-10 §6.3). Empty before the signed-claims activation. Every
  // committed change bumps version.
  repeated SIIndexerEntry si_indexers = 7;
  // Ids of the indexers whose Legacy seed is recorded, ascending and unique.
  // Empty before the signed-claims activation, when seeded alone describes the
  // founding indexer; from the activation on it holds the founding indexer iff
  // seeded is true.
  repeated uint64 seeded_indexers = 8;
}
```

Replace the comment above `message PurgeNodeSet` with:

```proto
// PurgeNodeSet is the committed set of data-plane nodes whose
// SubmitTablePurged completes a purge: every registered, non-evicted SNode and
// verifier, sorted ascending. After the signed-claims activation one
// incarnation's purge waits only on its owner's SNode and every non-evicted
// verifier (housegate spec 2026-10-10 §6.6). A replica under a table's Keeper
// path whose name is not in the set belongs to a decommissioned node.
```

- [ ] **Step 6: Edit `proto/consensus.proto`**

Append to `ConsensusMutableParams`, after `client_lanes = 5`:

```proto
  // Set once by a signed update, then carried by every update; raise-only.
  ClientLaneParams client_lanes = 5;
  // Set once by the signed-claims activation, then carried complete by every
  // update: append-only, sorted by indexer_id (housegate spec 2026-10-10 D3).
  repeated SIIndexerEntry si_indexers = 6;
  // Set by the signed-claims activation together with si_indexers, then
  // carried complete by every update; entries may be added and removed (D7).
  repeated VerifierEntry verifiers = 7;
}
```

Append to `ConsensusParamsUpdate`, after `client_lanes = 10`:

```proto
  ClientLaneParams client_lanes = 10;
  // Enrolled SI indexers (housegate spec 2026-10-10 D3, §6.1), sorted by
  // indexer_id. Empty keeps signed claims inactive. The update that first sets
  // it is the irreversible signed-claims activation (§6.7): it needs
  // table_registry (earlier or in the same update) and verifiers in the same
  // update, and its entry for table_registry.si_indexer_id restates the
  // founding indexer with table_registry.activation_block. In the same Apply
  // that first sets it, every registered, non-evicted SNODE whose node id no
  // entry names is evicted (reason "not named by the signed-claims
  // activation"), and a named SNode registered with any role other than
  // exactly SNODE refuses the update (contract §3c). Afterwards every update
  // resends the complete list: entries are only appended (an enrolment,
  // §6.2), and an entry's signer changes only with a new enrollment_jws.
  // max_writers must be at least its length. It joins the params digest only
  // once set, so earlier digests are unchanged.
  repeated SIIndexerEntry si_indexers = 11;
  // The governed verifier set (D7), sorted by node_id. Empty until the
  // signed-claims activation, which lists at least three verifiers. In the same
  // Apply, every registered, non-evicted VERIFIER it does not list is evicted
  // (reason "not named by the signed-claims activation"), and a listed verifier
  // registered under a different ed25519 key refuses the update. Afterwards
  // every update resends the complete list; entries may be added and removed,
  // a removed verifier is evicted, and no update may leave fewer than three
  // non-evicted listed verifiers. It joins the params digest only once set.
  repeated VerifierEntry verifiers = 12;
}
```

Replace the `ProtocolInfo.features` comment with:

```proto
  // Capability strings of this binary ("client_lanes_v1", "signed_claims_v1");
  // a static local property, read by arbiter-admin's activation gates on every
  // voter.
  repeated string features = 4;
```

After `message NodeFeatures { … }` and before `service ConsensusAdmin`, add:

```proto
message NodeFeatures {
  repeated NodeFeatureEntry entries = 1;
  repeated string raft_voter_ids = 2;
}

// VerifierEntry is one governed verifier (housegate spec 2026-10-10 D7).
// ed25519_pubkey is the raw 32-byte key the verifier registers as
// NodeRegistration.ed25519_pubkey and signs its evidence with; after the
// signed-claims activation it also signs the node's registration, activation
// and purge reports.
message VerifierEntry {
  string node_id = 1;
  bytes ed25519_pubkey = 2;
}

// EvictNodeRequest is the authority-signed eviction of one data-plane node
// (housegate spec 2026-10-10 §6.5). authority_jws is an ES256K JWS by a
// current authority address with purpose "arbiter-evict-node-v1" over the
// canonical digest of {node_id, expected_registration_seq, reason}, bound to
// the network id, genesis snapshot id and authority epoch.
// expected_registration_seq must equal the node's last applied
// registration_seq, so an eviction signed against one registration never
// evicts a later one.
message EvictNodeRequest {
  string node_id = 1;
  uint64 expected_registration_seq = 2;
  string reason = 3;
  string authority_jws = 4;
}
```

Append `EvictNode` to `service ConsensusAdmin`, after `GetNodeFeatures`; every existing RPC stays:

```proto
  rpc GetNodeFeatures (google.protobuf.Empty) returns (NodeFeatures) {}
  // Leader-only, authority-authenticated eviction (housegate spec 2026-10-10
  // §6.5): proposes EvictNodeCmd carrying expected_registration_seq and
  // authority_jws. Refused with FAILED_PRECONDITION until the signed-claims
  // activation has committed, because no earlier command may carry those
  // fields. Ack means committed success.
  rpc EvictNode (EvictNodeRequest) returns (Ack) {}
}
```

- [ ] **Step 7: Edit `proto/arbiter.proto`**

Append to `enum AdmissionCode`, after `ADMISSION_CODE_LANE_BUDGET_EXCEEDED = 9`:

```proto
  ADMISSION_CODE_LANE_BUDGET_EXCEEDED = 9;
  // The statement's source — the SNode of its target table's owner,
  // si_indexers[owner].snode_node_id — is not registered and Active in
  // committed state (housegate spec 2026-10-10 §6.4). Returned only after the
  // signed-claims activation. Nothing changed: the coordinate is unspent, and
  // HouseGate answers a retryable, session-preserving refusal.
  ADMISSION_CODE_SOURCE_UNAVAILABLE = 10;
}
```

Replace `RCRecord` and its comment (the old comment promised exactly this signature slot):

```proto
// RCRecord is the source's result claim (§4.1, §7.3). Late binding: an
// RCRecord may arrive before its statement_seq exists; the FSM parks it
// under statement_id and binds when SubmitStatement assigns the seq (§5.5).
// Before the signed-claims activation (housegate spec 2026-10-10 §6.7) the
// FSM trusts the gRPC channel for source identity; after it every claim,
// parked or bound, must carry a valid source_jws.
message RCRecord {
  StatementID statement_id = 1;
  // NodeID of the claiming source SNode. It must equal the statement's bound
  // source: the deterministic hash selection (§5.4) before the signed-claims
  // activation, the owner's SNode (si_indexers[owner].snode_node_id) after it.
  string source_node = 2;
  repeated CandidatePart candidate_parts = 3;
  // The source's claimed post-state root (check 1's right-hand side).
  string source_claim_root = 4;
  repeated PartitionLtHashSum partition_new_part_sums = 5;
  // After the signed-claims activation: ES256K compact JWS by the signer of
  // the si_indexers entry whose snode_node_id is source_node, purpose
  // "arbiter-snode-message-v1", kind "result_claim", over this record without
  // source_jws, bound to the network id and genesis snapshot id. Request-only:
  // the server moves it into RegisterRCCmd.source_jws, and an RCRecord inside
  // a RaftCommand never carries it. Dropped before the activation.
  string source_jws = 6;
}
```

Append to `PromotionAck`, after `safe_partition_parts = 9`:

```proto
  repeated SafePartMapping safe_partition_parts = 9;
  // After the signed-claims activation (housegate spec 2026-10-10 §6.6):
  // ES256K compact JWS by the signer of the si_indexers entry whose
  // snode_node_id is node_id, purpose "arbiter-snode-message-v1", kind
  // "promotion_ack", over this ack without source_jws, bound to the network id
  // and genesis snapshot id. Only the promotion's expected source (the owner's
  // SNode) may acknowledge it, and only its signed ack, applied=false
  // included, changes state. Request-only: the server moves it into
  // RecordPromotionAckCmd.source_jws. Dropped before the activation.
  string source_jws = 10;
}
```

Replace `CleanupAck` with:

```proto
message CleanupAck {
  string node_id = 1;
  uint64 promotion_seq = 2;
  string table_id = 3;
  string partition_id = 4;
  // After the signed-claims activation: signed like PromotionAck.source_jws,
  // with kind "cleanup_ack"; only the cleanup's expected source (the owner's
  // SNode) may clear it. Request-only: the server moves it into
  // RecordCleanupAckCmd.source_jws. Dropped before the activation.
  string source_jws = 5;
}
```

Replace the tail of `NodeRegistration` (from the `features` comment) and `NodeRef` with:

```proto
  // Capability strings of the registering binary ("client_lanes_v1",
  // "signed_claims_v1"). Request-only: the leader records them in a
  // non-replicated feature book and never copies them into a RaftCommand or
  // replicated state (housegate spec 2026-10-09 §5.6), because a voter that
  // does not know this field refuses every command that carries it.
  repeated string features = 5;
  // The node's registration sequence (housegate spec 2026-10-10 §6.5): the
  // node persists it and raises it before every RegisterNode. After the
  // signed-claims activation it must exceed the last registration_seq applied
  // for node_id, so a replayed registration cannot push a running node back to
  // Syncing. Unlike features it is replicated, inside
  // RegisterNodeCmd.registration, and covered by the signature; the server
  // proposes 0 before the activation.
  uint64 registration_seq = 6;
  // SNODE role, after the signed-claims activation: ES256K compact JWS by the
  // signer of the si_indexers entry whose snode_node_id is node_id, purpose
  // "arbiter-snode-message-v1", kind "registration", over this registration
  // without features, signer_jws and ed25519_signature, bound to the network
  // id and genesis snapshot id. Request-only: the server moves it into
  // RegisterNodeCmd.signer_jws. Dropped before the activation.
  string signer_jws = 7;
  // VERIFIER role, after the signed-claims activation: hex ed25519 signature
  // by the node's verifiers-entry key (equal to ed25519_pubkey) over the
  // "registration" verifier-message hash of the same body and context.
  // Request-only: the server moves it into RegisterNodeCmd.ed25519_signature.
  // Dropped before the activation.
  string ed25519_signature = 8;
}

// NodeRef names one data-plane node. It is the MarkActive request and is
// never embedded in a RaftCommand.
message NodeRef {
  string node_id = 1;
  // After the signed-claims activation it must equal the last
  // registration_seq applied for node_id, binding this activation to that
  // registration (housegate spec 2026-10-10 §6.5). Copied into MarkActiveCmd;
  // 0 before the activation.
  uint64 registration_seq = 2;
  // SNODE, after the activation: ES256K compact JWS by the entry's signer,
  // purpose "arbiter-snode-message-v1", kind "mark_active", body {node_id,
  // registration_seq}. Copied into MarkActiveCmd.signer_jws.
  string signer_jws = 3;
  // VERIFIER, after the activation: hex ed25519 signature by the
  // verifiers-entry key over the "mark_active" verifier-message hash of the
  // same body. Copied into MarkActiveCmd.ed25519_signature. Both signatures
  // are dropped before the activation.
  string ed25519_signature = 4;
}
```

In `service PromotionGateway`, give `SubscribePromotions` a comment:

```proto
service PromotionGateway {
  // Before the signed-claims activation every connected SNode receives every
  // promotion and cleanup; after it only the table owner's SNode does
  // (housegate spec 2026-10-10 §6.6).
  rpc SubscribePromotions (SNodeHello) returns (stream PromotionCommand) {}
```

In the comment above `rpc SubmitTablePurged`, replace the lines from `dynamic SI table registry. Authenticated like AckCleanup` through `never reads it as success.` with:

```proto
  // dynamic SI table registry. Authenticated like AckCleanup: the node is named
  // by node_id and the FSM accepts only a registered, non-evicted SNode or
  // verifier; after the signed-claims activation the report also carries the
  // reporter's signature (RecordTablePurgedCmd.signer_jws for an SNode,
  // ed25519_signature for a verifier). Idempotent: an already-recorded node,
  // or an incarnation that is already Purged, returns Ack. FAILED_PRECONDITION
  // without a NotLeader detail means the incarnation is not Purging yet; the
  // caller retries and never reads it as success.
```

Replace `service Membership` (keep its leading comment) with:

```proto
service Membership {
  // After the signed-claims activation (housegate spec 2026-10-10 §6.5) only
  // an enrolled SNode (some si_indexers snode_node_id) or a listed verifier
  // may register, signed and with a rising registration_seq.
  rpc RegisterNode (NodeRegistration) returns (Ack) {}
  // MarkActive is accepted only after snapshot sync; Active nodes enter the
  // deterministic selection pools (§4.1, §7.4). After the signed-claims
  // activation it names the node's current registration_seq and is signed.
  rpc MarkActive (NodeRef) returns (Ack) {}
}
```

- [ ] **Step 8: Edit `proto/raftlog.proto`**

Replace `RegisterRCCmd`, `RecordPromotionAckCmd` and `RecordCleanupAckCmd` (each keeps its leading comment) with, respectively:

```proto
message RegisterRCCmd {
  // rc.source_jws is always empty here; the signature rides in source_jws.
  RCRecord rc = 1;
  // RCRecord.source_jws, moved out of rc (housegate spec 2026-10-10 §6.5). Set
  // only after the signed-claims activation, when Apply verifies it before
  // parking or binding the claim. Never proposed before it: a voter whose
  // decoder lacks this field would refuse the command.
  string source_jws = 2;
}
```

```proto
message RecordPromotionAckCmd {
  // ack.source_jws is always empty here; the signature rides in source_jws.
  PromotionAck ack = 1;
  // PromotionAck.source_jws, moved out of ack. Set only after the
  // signed-claims activation, when Apply takes the ack only from the
  // promotion's expected source with a valid signature; never proposed before
  // it.
  string source_jws = 2;
}
```

```proto
message RecordCleanupAckCmd {
  // ack.source_jws is always empty here; the signature rides in source_jws.
  CleanupAck ack = 1;
  // CleanupAck.source_jws, moved out of ack; the same activation rule as
  // RecordPromotionAckCmd.source_jws.
  string source_jws = 2;
}
```

Replace `RegisterNodeCmd`, `MarkActiveCmd` and `EvictNodeCmd` with:

```proto
message RegisterNodeCmd {
  // registration.registration_seq is replicated and signed;
  // registration.features, signer_jws and ed25519_signature are request-only
  // and always empty here.
  NodeRegistration registration = 1;
  // NodeRegistration.signer_jws (SNODE role), moved out of registration.
  string signer_jws = 2;
  // NodeRegistration.ed25519_signature (VERIFIER role), moved out of
  // registration. Both signatures and registration.registration_seq are set
  // only after the signed-claims activation; never proposed before it.
  string ed25519_signature = 3;
}

message MarkActiveCmd {
  string node_id = 1;
  // Copied from NodeRef. All three are set only after the signed-claims
  // activation, when registration_seq must equal the node's last applied
  // registration_seq and the signature must match the node's role; never
  // proposed before it.
  uint64 registration_seq = 2;
  string signer_jws = 3;
  string ed25519_signature = 4;
}

message EvictNodeCmd {
  string node_id = 1;
  string reason = 2;
  // From EvictNodeRequest (ConsensusAdmin.EvictNode), set only after the
  // signed-claims activation: Apply then requires expected_registration_seq
  // to equal the node's last applied registration_seq and authority_jws to
  // verify against the current authority set and epoch. Before the activation
  // no RPC proposes EvictNodeCmd and both stay empty.
  uint64 expected_registration_seq = 3;
  string authority_jws = 4;
}
```

`RaftCommand` gets no new tag: stage 1 reuses tags 4, 9, 12, 15, 16, 17, 18, 31, 32 and 35.

- [ ] **Step 9: Regenerate, run the new test, and see the old pins fail**

Run: `cd "$W" && make tools && make proto && git status --porcelain -- gen/`
Expected, exactly six files (`table_registry_grpc.pb.go` does not change: its service is untouched):

```
 M gen/pb/arbiter.pb.go
 M gen/pb/arbiter_grpc.pb.go
 M gen/pb/consensus.pb.go
 M gen/pb/consensus_grpc.pb.go
 M gen/pb/raftlog.pb.go
 M gen/pb/table_registry.pb.go
```

Run: `cd "$W" && go test ./conformance/ -run 'TestSignedClaims|TestSourceUnavailable|TestOptionalIndexerIDs' -v`
Expected: PASS — `--- PASS: TestSignedClaimsFieldNumbers` with 41 passing subtests, `--- PASS: TestSourceUnavailableAdmissionCodeIsTen`, `--- PASS: TestSignedClaimsRPCSignatures`, `--- PASS: TestOptionalIndexerIDsKeepZeroDistinctFromAbsent` with 3 passing subtests, then `ok  	github.com/sentioxyz/arbiter-proto/conformance`.

Run: `cd "$W" && go test ./conformance/ 2>&1 | grep -E '^--- FAIL'`
Expected, exactly these nine pre-existing pins (they count fields or compare old descriptors, so they must learn the additions):

```
--- FAIL: TestClientLanesFieldNumbers
--- FAIL: TestLaneBudgetAdmissionCodeIsNine
--- FAIL: TestConsensusUpdateContract
--- FAIL: TestConsensusAdminRPCSignatures
--- FAIL: TestSnapshotQueryOldDescriptorsUnchanged
--- FAIL: TestSnapshotQueryMainDescriptorsUnchanged
--- FAIL: TestTablePurgeContract
--- FAIL: TestTableRegistryReadContract
--- FAIL: TestTableRegistryContract
```

The two `TestSnapshotQuery…DescriptorsUnchanged` failures come from `assertSnapshotBaselineDescriptors`, which compares every message of the baselines `pre_snapshot_query_descriptor.binpb` (AP commit `19d90fc`) and `main_f7d9f070_descriptor.binpb` (`f7d9f07`, which already has `consensus.proto`) with the current descriptor and allows only listed trailing additions: `RCRecord` changes, the `PromotionAck` inventory allowance expects exactly one extra field, and the `clientLaneFields` exemption expects `features = 5` to be `NodeRegistration`'s last field.

- [ ] **Step 10: Update the existing conformance pins**

Each pin keeps its exact count and gains the new names; nothing is loosened and the hash-pinned baselines stay untouched.

`conformance/consensus_test.go` — in `TestConsensusUpdateContract`, replace the `ConsensusParamsUpdate` and `ConsensusMutableParams` rows with:

```go
		{&pb.ConsensusParamsUpdate{},
			[]protoreflect.Name{"network_id", "genesis_snapshot_id", "expected_epoch", "previous_params_digest", "authority_addresses", "max_writers", "expected_promotion_seq", "artifact_disposition_capability", "table_registry", "client_lanes", "si_indexers", "verifiers"},
			[]protoreflect.Kind{protoreflect.StringKind, protoreflect.StringKind, protoreflect.Uint64Kind, protoreflect.StringKind, protoreflect.StringKind, protoreflect.Uint64Kind, protoreflect.Uint64Kind, protoreflect.Uint32Kind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.MessageKind}},
```

```go
		{&pb.ConsensusMutableParams{},
			[]protoreflect.Name{"authority_addresses", "max_writers", "artifact_disposition_capability", "table_registry", "client_lanes", "si_indexers", "verifiers"},
			[]protoreflect.Kind{protoreflect.StringKind, protoreflect.Uint64Kind, protoreflect.Uint32Kind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.MessageKind, protoreflect.MessageKind}},
```

replace the repeated-field check with:

```go
				if f.IsList() != (name == "authority_addresses" || name == "features" || name == "si_indexers" || name == "verifiers") {
```

extend the message-type loop at the end of the test:

```go
		f = m.ProtoReflect().Descriptor().Fields().ByName("client_lanes")
		if got := f.Message().FullName(); got != "arbiter.ClientLaneParams" {
			t.Fatalf("%s.client_lanes type = %s", m.ProtoReflect().Descriptor().Name(), got)
		}
		for name, want := range map[protoreflect.Name]protoreflect.FullName{"si_indexers": "arbiter.SIIndexerEntry", "verifiers": "arbiter.VerifierEntry"} {
			if got := m.ProtoReflect().Descriptor().Fields().ByName(name).Message().FullName(); got != want {
				t.Fatalf("%s.%s type = %s, want %s", m.ProtoReflect().Descriptor().Name(), name, got, want)
			}
		}
	}
```

and in `TestConsensusAdminRPCSignatures` raise the method count and add the row:

```go
	if service == nil || service.Methods().Len() != 8 {
```

```go
		{"GetNodeFeatures", "google.protobuf.Empty", "arbiter.NodeFeatures"},
		{"EvictNode", "arbiter.EvictNodeRequest", "arbiter.Ack"},
	} {
```

`conformance/client_lanes_test.go` — `NodeRegistration` now has 8 fields and `AdmissionCode` 11 values:

```go
		&pb.StatementID{}: 4, &pb.NodeRegistration{}: 8, &pb.ClientLaneParams{}: 1,
```

```go
	if n := pb.AdmissionCode(0).Descriptor().Values().Len(); n != 11 {
		t.Fatalf("AdmissionCode has %d values, want 11", n)
	}
```

`conformance/table_registry_test.go` — the fields are listed in number order (`number == index + 1`):

```go
		{&pb.SeedLegacyTablesCmd{}, []field{{"at_block", protoreflect.MessageKind, false}, {"tables", protoreflect.MessageKind, true}, {"indexer_id", protoreflect.Uint64Kind, false}}},
		{&pb.AddTableCmd{}, []field{{"database_id", protoreflect.StringKind, false}, {"table_id", protoreflect.StringKind, false}, {"created", protoreflect.MessageKind, false}, {"schema", protoreflect.MessageKind, false}, {"schema_version", protoreflect.Uint32Kind, false}, {"schema_hash", protoreflect.StringKind, false}, {"schema_json", protoreflect.StringKind, false}, {"owner_indexer_id", protoreflect.Uint64Kind, false}}},
```

```go
		{&pb.RecordTablePurgedCmd{}, []field{{"node_id", protoreflect.StringKind, false}, {"incarnation_seq", protoreflect.Uint64Kind, false}, {"signer_jws", protoreflect.StringKind, false}, {"ed25519_signature", protoreflect.StringKind, false}}},
```

`conformance/table_registry_read_test.go`:

```go
			{"retire_block_seq", 16, protoreflect.Uint64Kind, false}, {"purged_by", 17, protoreflect.StringKind, true},
			{"owner_indexer_id", 18, protoreflect.Uint64Kind, false},
		}},
```

```go
		{&pb.TableRegistrySnapshot{}, []field{{"params", 1, protoreflect.MessageKind, false}, {"version", 2, protoreflect.Uint64Kind, false}, {"seeded", 3, protoreflect.BoolKind, false}, {"cursor", 4, protoreflect.MessageKind, false}, {"incarnations", 5, protoreflect.MessageKind, true}, {"client_lanes", 6, protoreflect.MessageKind, false}, {"si_indexers", 7, protoreflect.MessageKind, true}, {"seeded_indexers", 8, protoreflect.Uint64Kind, true}}},
```

`conformance/table_purge_test.go`:

```go
	if cmd.Len() != 4 {
		t.Fatalf("RecordTablePurgedCmd has %d fields, want 4", cmd.Len())
	}
```

`conformance/snapshot_query_test.go` — add the exemption list and its two helpers directly above `func assertSnapshotBaselineDescriptors` (the descriptor shapes were checked against `buf build` output: plain appended fields carry `name`, `json_name`, `number`, `label`, `type` and, for messages, `type_name`):

```go
// signedClaimsFields pins the exact trailing fields the multi-source SI stage-1
// work (housegate spec 2026-10-10 §6; Plan S1-A contract §1) appended to
// messages that exist in the baselines. They are the last fields of every
// message they touch, so they are stripped first, before every older
// exemption below runs. Every existing baseline predates them, so the
// exemption applies unconditionally.
var signedClaimsFields = map[string][]*descriptorpb.FieldDescriptorProto{
	"RCRecord":     {signedClaimsField("source_jws", "sourceJws", 6, signedClaimsString)},
	"PromotionAck": {signedClaimsField("source_jws", "sourceJws", 10, signedClaimsString)},
	"CleanupAck":   {signedClaimsField("source_jws", "sourceJws", 5, signedClaimsString)},
	"NodeRegistration": {
		signedClaimsField("registration_seq", "registrationSeq", 6, signedClaimsUint64),
		signedClaimsField("signer_jws", "signerJws", 7, signedClaimsString),
		signedClaimsField("ed25519_signature", "ed25519Signature", 8, signedClaimsString),
	},
	"NodeRef": {
		signedClaimsField("registration_seq", "registrationSeq", 2, signedClaimsUint64),
		signedClaimsField("signer_jws", "signerJws", 3, signedClaimsString),
		signedClaimsField("ed25519_signature", "ed25519Signature", 4, signedClaimsString),
	},
	"ConsensusMutableParams": {
		signedClaimsList("si_indexers", "siIndexers", 6, ".arbiter.SIIndexerEntry"),
		signedClaimsList("verifiers", "verifiers", 7, ".arbiter.VerifierEntry"),
	},
	"ConsensusParamsUpdate": {
		signedClaimsList("si_indexers", "siIndexers", 11, ".arbiter.SIIndexerEntry"),
		signedClaimsList("verifiers", "verifiers", 12, ".arbiter.VerifierEntry"),
	},
	"RegisterRCCmd":         {signedClaimsField("source_jws", "sourceJws", 2, signedClaimsString)},
	"RecordPromotionAckCmd": {signedClaimsField("source_jws", "sourceJws", 2, signedClaimsString)},
	"RecordCleanupAckCmd":   {signedClaimsField("source_jws", "sourceJws", 2, signedClaimsString)},
	"RegisterNodeCmd": {
		signedClaimsField("signer_jws", "signerJws", 2, signedClaimsString),
		signedClaimsField("ed25519_signature", "ed25519Signature", 3, signedClaimsString),
	},
	"MarkActiveCmd": {
		signedClaimsField("registration_seq", "registrationSeq", 2, signedClaimsUint64),
		signedClaimsField("signer_jws", "signerJws", 3, signedClaimsString),
		signedClaimsField("ed25519_signature", "ed25519Signature", 4, signedClaimsString),
	},
	"EvictNodeCmd": {
		signedClaimsField("expected_registration_seq", "expectedRegistrationSeq", 3, signedClaimsUint64),
		signedClaimsField("authority_jws", "authorityJws", 4, signedClaimsString),
	},
}

const (
	signedClaimsString = descriptorpb.FieldDescriptorProto_TYPE_STRING
	signedClaimsUint64 = descriptorpb.FieldDescriptorProto_TYPE_UINT64
)

// signedClaimsField is the descriptor protodesc reports for a plain appended
// proto3 scalar: no options, no optional keyword and no oneof.
func signedClaimsField(name, jsonName string, number int32, typ descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name: proto.String(name), JsonName: proto.String(jsonName), Number: proto.Int32(number),
		Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(), Type: typ.Enum(),
	}
}

// signedClaimsList is signedClaimsField for an appended repeated message field.
func signedClaimsList(name, jsonName string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
	f := signedClaimsField(name, jsonName, number, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE)
	f.Label, f.TypeName = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(), proto.String(typeName)
	return f
}

func assertSnapshotBaselineDescriptors(t *testing.T, baseline *descriptorpb.FileDescriptorSet, allowPromotionInventory bool) {
```

and strip the additions first inside `assertSnapshotBaselineDescriptors`, directly after the `if got == nil { … }` check and before the `RaftCommand` truncation. They are the last fields of every message they touch, and the older exemptions (`PromotionAck` inventory, `clientLaneFields`, `tableRegistryFieldNumbers`, `artifactDispositionCapabilityFieldNumbers`) each inspect the then-last field:

```go
			if got == nil {
				t.Fatalf("old message removed: %s", m.GetName())
			}
			if want, ok := signedClaimsFields[m.GetName()]; ok {
				if len(got.Field) < len(want) {
					t.Fatalf("%s lacks its signed-claims fields: %v", m.GetName(), got)
				}
				tail := got.Field[len(got.Field)-len(want):]
				for i := range want {
					if !proto.Equal(tail[i], want[i]) {
						t.Fatalf("%s signed-claims addition must be exactly %s = %d: %v", m.GetName(), want[i].GetName(), want[i].GetNumber(), got)
					}
				}
				got.Field = got.Field[:len(got.Field)-len(want)]
			}
```

No entry is needed in `conformance/snapshot_query_fields_test.go`: `TestSnapshotQueryLedgerCoversEveryNewMessage` covers only new messages of `arbiter.proto`, `replay.proto` and `raftlog.proto`, and this task adds messages only to `consensus.proto` and `table_registry.proto`.

- [ ] **Step 11: Run every gate**

Run: `cd "$W" && gofmt -l conformance/ && make lint`
Expected: `gofmt` prints nothing; `make lint` echoes `buf lint` and exits 0.

Run: `cd "$W" && make breaking && buf breaking --against '.git#tag=v0.9.0'; echo "exit=$?"`
Expected: `make breaking` echoes `buf breaking --against '.git#branch=main'` and passes; the explicit run against the release prints nothing and `exit=0`. `make breaking` compares with the local `main` branch, which in `/Users/uranuswch/Dev/sentio_xyz/arbiter-proto` is stale (`bb1823f`, 14 commits behind `v0.9.0`), so the run against the tag is the authoritative baseline; buf reads `.git#tag=…` from a worktree's `.git` file (checked), and CI repeats the check against `origin/main`.

Run: `cd "$W" && make test`
Expected: `go build ./...` and `go vet ./...` print nothing; `go test ./...` prints `ok  	github.com/sentioxyz/arbiter-proto/conformance` and `?   	github.com/sentioxyz/arbiter-proto/gen/pb	[no test files]`.

Run: `cd "$W" && git add -A proto gen conformance && make proto && git diff --exit-code -- gen/; echo "drift exit=$?"`
Expected: `drift exit=0` (regeneration is idempotent, which is CI's `generated code is current` step).

- [ ] **Step 12: Commit**

```bash
cd "$W" && git add proto gen conformance && git commit -m "feat(proto): signed claims, SI indexer enrolment, verifier set and authority eviction

SIIndexerEntry (table_registry.proto, which consensus.proto imports) and
VerifierEntry; si_indexers / verifiers on ConsensusParamsUpdate (11, 12)
and ConsensusMutableParams (6, 7); si_indexers / seeded_indexers on
TableRegistrySnapshot (7, 8); optional owner_indexer_id on
TableIncarnation (18) and AddTableCmd (8) and optional
SeedLegacyTablesCmd.indexer_id (3), because indexer 0 is valid;
signer_jws / ed25519_signature on RecordTablePurgedCmd (3, 4, also the
SubmitTablePurged request), NodeRegistration (6-8 with registration_seq)
and NodeRef (2-4); source_jws on RCRecord (6), PromotionAck (10),
CleanupAck (5) and RegisterRCCmd / RecordPromotionAckCmd /
RecordCleanupAckCmd (2); RegisterNodeCmd (2, 3), MarkActiveCmd (2-4),
EvictNodeCmd (3, 4); ADMISSION_CODE_SOURCE_UNAVAILABLE (10);
ConsensusAdmin.EvictNode (housegate spec 2026-10-10 §6, D3-D8).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 13: PR, merge and release (requires explicit user confirmation)**

Ask the user before each remote action (push, PR, merge, each workflow run). After approval:

```bash
cd "$W" && git push -u origin urwt/claude/multi-source-s1
gh pr create --repo sentioxyz/arbiter-proto --base main --head urwt/claude/multi-source-s1 \
  --title 'feat(proto): signed claims, SI indexer enrolment, verifier set and authority eviction' \
  --body "$(cat <<'EOF'
Stage-1 wire contract of the multi-source storage-integrity network (housegate spec `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` §6, D3-D8). Additive only; every number is the next free one at v0.9.0.

- `SIIndexerEntry` (in `table_registry.proto`, which `consensus.proto` imports) and `VerifierEntry`; `si_indexers` / `verifiers` on `ConsensusParamsUpdate` (11, 12) and `ConsensusMutableParams` (6, 7); `si_indexers` / `seeded_indexers` on `TableRegistrySnapshot` (7, 8).
- proto3 `optional` owner ids, because indexer 0 is valid: `TableIncarnation.owner_indexer_id` (18), `AddTableCmd.owner_indexer_id` (8), `SeedLegacyTablesCmd.indexer_id` (3).
- SNode and verifier signatures: `RecordTablePurgedCmd` (3, 4; also the `SubmitTablePurged` request), `NodeRegistration` (6-8 with `registration_seq`), `NodeRef` (2-4), `RCRecord.source_jws` (6), `PromotionAck.source_jws` (10), `CleanupAck.source_jws` (5), and the Raft carriers `RegisterRCCmd` / `RecordPromotionAckCmd` / `RecordCleanupAckCmd` (2), `RegisterNodeCmd` (2, 3), `MarkActiveCmd` (2-4), `EvictNodeCmd` (3, 4).
- `ADMISSION_CODE_SOURCE_UNAVAILABLE = 10`; `ConsensusAdmin.EvictNode(EvictNodeRequest)`.

arbiter-core's strict Raft decoder refuses unknown fields, so no new field may enter a proposed command before the signed-claims activation; the field comments say so. New `conformance/signed_claims_test.go`; the field-count pins and the baseline-descriptor exemptions (`signedClaimsFields`) learn the additions; the hash-pinned baselines are unchanged.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Wait for CI (`gh pr checks <n> --repo sentioxyz/arbiter-proto --watch`; the `proto` job runs `buf lint`, `buf breaking` against `origin/main`, the generated-code drift check and `make test`) and review, then merge with the repository's squash convention: `gh pr merge <n> --repo sentioxyz/arbiter-proto --squash`. Then cut the release from `main`, previewing first:

```bash
gh workflow run cut-release.yml --repo sentioxyz/arbiter-proto --ref main -f dry_run=true
gh workflow run cut-release.yml --repo sentioxyz/arbiter-proto --ref main
gh run watch --repo sentioxyz/arbiter-proto "$(gh run list --repo sentioxyz/arbiter-proto --workflow cut-release.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
```

The workflow tags `vX.Y.Z` (Y+1 on the first cut of a new UTC day, else Z+1; `v0.9.0` was cut on 2026-10-09 UTC), so a cut on any later UTC day yields `v0.10.0` — record what the run's summary prints, not this prediction. Verify the tag by content (substitute the tag for `<tag>`):

```bash
T=<tag>; R=/Users/uranuswch/Dev/sentio_xyz/arbiter-proto
git -C "$R" fetch origin --tags
git -C "$R" show "$T":proto/consensus.proto | grep -c 'repeated SIIndexerEntry si_indexers = 11;'
git -C "$R" show "$T":proto/arbiter.proto | grep -c 'ADMISSION_CODE_SOURCE_UNAVAILABLE = 10;'
git -C "$R" show "$T":proto/table_registry.proto | grep -c 'optional uint64 owner_indexer_id = 18;'
git -C "$R" ls-remote --tags origin "$T"
```

Expected: `1`, `1`, `1`, then the tag's remote refs (`refs/tags/<tag>` and `refs/tags/<tag>^{}`). Record the tag as `PROTO_TAG` in the release ledger.

---

## Task 2: arbiter-core — enrolment and verifier parameters, owner fields, SOURCE_UNAVAILABLE (S1)

**Files:**
- Create: `wire/signed_claims_golden_test.go` (pre-activation command bytes, verified on unchanged code in Step 2)
- Modify: `authority/consensus_golden_test.go` (frozen digest of a lanes-era update)
- Modify: `go.mod`, `go.sum` (arbiter-proto `PROTO_TAG`), BUILD files via gazelle; `MODULE.bazel.lock` only if `bazel mod tidy` rewrites it
- Modify: `conformance/arbiter_wire_test.go:51-94` (request-only exemptions, new mirrors, AdmissionCode 10); Create: `conformance/signed_claims_test.go` (field-number pins)
- Modify: `consensus.go:10-16` (`SignedClaimsFeature`, `LocalNodeFeatures`), `consensus.go:41-65` (`ConsensusParamsUpdate`), new `SIIndexerEntry` / `VerifierEntry`
- Modify: `types.go:125-139` (`AdmissionCodeSourceUnavailable`), `types.go:278-284` (`NodeRegistration.RegistrationSeq`)
- Modify: `consensus_test.go:42-51` (`TestLocalNodeFeatures`) and new tests
- Modify: `authority/consensus.go:21-69` (`NormalizeConsensusParamsUpdate`); Create: `authority/consensus_signed_claims_test.go`
- Modify: `wire/consensus.go:12-42` (update converters) plus new list converters, `wire/convert.go:144-154` (`RegistrationFromPB` / `RegistrationToPB`), `wire/table_registry.go:18-31` (`SeedLegacyTables`, `AddTable`), `wire/command.go:250-261` and `:334-341` (encode / decode of seed and add), `wire/table_registry_snapshot.go:61-79, 104-114, 150-222`
- Create: `wire/signed_claims_params_test.go`

**Interfaces:**
- Consumes (Task 1; Go package `pb`, proto package `arbiter`): `pb.SIIndexerEntry{IndexerId, ActivationBlock uint64; Signer, SnodeNodeId, EnrollmentJws string}` (declared in `table_registry.proto`), `pb.VerifierEntry{NodeId string; Ed25519Pubkey []byte}`, `pb.EvictNodeRequest`, `pb.ConsensusParamsUpdate.SiIndexers` / `.Verifiers`, `pb.ConsensusMutableParams.SiIndexers` / `.Verifiers`, `pb.TableRegistrySnapshot.SiIndexers` / `.SeededIndexers []uint64`, the proto3 `optional` fields `pb.TableIncarnation.OwnerIndexerId`, `pb.AddTableCmd.OwnerIndexerId`, `pb.SeedLegacyTablesCmd.IndexerId` (all `*uint64`), `pb.NodeRegistration.RegistrationSeq`, `pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE`, and every transport-only signature field (pinned in Step 3, used by Task 3).
- Produces (package `arbiter`):
  - `const SignedClaimsFeature = "signed_claims_v1"`; `func LocalNodeFeatures() []string` now returns `[client_lanes_v1 signed_claims_v1]` in a fresh slice
  - `const AdmissionCodeSourceUnavailable AdmissionCode = 10`
  - `type SIIndexerEntry struct{ IndexerID uint64; ActivationBlock uint64; Signer string; SNodeNodeID string; EnrollmentJWS string }` with JSON tags `indexer_id`, `activation_block`, `signer`, `snode_node_id`, `enrollment_jws`; `func (e SIIndexerEntry) Validate() error`
  - `type VerifierEntry struct{ NodeID string; Ed25519Pubkey []byte }` with JSON tags `node_id`, `ed25519_pubkey`; `func (e VerifierEntry) Validate() error`
  - `ConsensusParamsUpdate.SIIndexers []SIIndexerEntry` (JSON `si_indexers,omitempty`), `ConsensusParamsUpdate.Verifiers []VerifierEntry` (JSON `verifiers,omitempty`)
  - `NodeRegistration.RegistrationSeq uint64` (JSON `registration_seq,omitempty`; canonical and signed, rides inside `NodeRegistration` in the Raft command)
- Produces (package `authority`): `NormalizeConsensusParamsUpdate` (same signature) also returns `SIIndexers` sorted by `IndexerID` with lowercase signers, every entry `Validate()`d, no duplicate `IndexerID` or `SNodeNodeID`, and refuses `len(SIIndexers) > MaxWriters`; `Verifiers` sorted by `NodeID`, deep-copied, every entry `Validate()`d, no duplicate `NodeID` or key; an absent or empty list normalises to nil (never `[]`). `ConsensusParamsUpdateHash` is byte-identical for every update without the new fields.
- Produces (package `wire`):
  - `func SIIndexerEntriesFromPB(ms []*pb.SIIndexerEntry) []arbiter.SIIndexerEntry`, `func SIIndexerEntriesToPB(v []arbiter.SIIndexerEntry) []*pb.SIIndexerEntry`
  - `func VerifierEntriesFromPB(ms []*pb.VerifierEntry) []arbiter.VerifierEntry`, `func VerifierEntriesToPB(v []arbiter.VerifierEntry) []*pb.VerifierEntry` (key bytes copied; empty list and empty key become nil). arbiter's `server/consensus_admin.go:69-80` builds `pb.ConsensusMutableParams` with these two.
  - `ConsensusParamsUpdateFromPB` / `ToPB` carry both lists; `RegistrationFromPB` / `RegistrationToPB` carry `registration_seq`
  - `AddTable.OwnerIndexerID *uint64`, `SeedLegacyTables.IndexerID *uint64` — nil and `&0` round-trip distinctly (indexer 0 is devnet2's founding indexer)
  - `TableRegistrySnapshot.SIIndexers []arbiter.SIIndexerEntry`, `TableRegistrySnapshot.SeededIndexers []uint64`, `TableIncarnation.OwnerIndexerID *uint64`
  - `func (s TableRegistrySnapshot) SignedClaimsActive() bool`, `func (s TableRegistrySnapshot) SIIndexer(indexerID uint64) (arbiter.SIIndexerEntry, bool)`, `func (s TableRegistrySnapshot) Owner(inc TableIncarnation) uint64` (nil owner = `Params.SIIndexerID`)
  - `TableRegistrySnapshotFromPB` refuses an inconsistent signed-claims view: an invalid or unsorted entry, a seeded id without an entry, an incarnation owner without an entry

- [ ] **Step 1: Create the worktree**

Run the URWT command of Task 1 Step 1 with `/Users/uranuswch/Dev/sentio_xyz/arbiter-core` in place of the arbiter-proto path (after `git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core fetch origin --tags`). Expected: one JSON object whose `path` is `~/src/remotesrc/claude/arbiter-core/multi-source-s1` on branch `urwt/claude/multi-source-s1`; `W` below is that path (prefix every command with `W=<path>;`, the Bash tool keeps no variables).

Run: `git -C "$W" rev-parse HEAD && git -C "$W" describe --tags`
Expected: `050490c5af42c531a80f2ef1d354e7a2bf7906f5` and `v0.12.1`. If `origin/main` moved, continue only if Step 2 passes unchanged.

- [ ] **Step 2: Freeze the pre-activation command bytes and a lanes-era update digest (on unchanged code)**

Every command that the signed-claims stage extends must encode, before activation, exactly as today (CONTRACT §0 strict decoder). The hex below was computed from arbiter-proto `2eb3917` with `protoc --encode=arbiter.RaftCommand` (the same tool reproduces the existing `TestLegacyCommandBytesAreFrozen` value `22440a42…` byte for byte), and the digest with the `replay.CanonicalDigest` preimage `housegate-replay-mvp-v0:<domain>\x00<json>` (the same pipeline reproduces the existing frozen `0x9a2297ef…`).

Create `wire/signed_claims_golden_test.go`:

```go
package wire

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

type signedClaimsGolden struct {
	name string
	cmd  Command
	want string
}

func goldenClaim() arbiter.RCRecord {
	return arbiter.RCRecord{
		StatementID: legacyGoldenEnvelope().StatementID, SourceNode: "snode-1",
		CandidateParts: []arbiter.CandidatePart{{TableID: "db1.t", PartitionID: "all", PartName: "all_1_1_0",
			PartRowLtHash: "0xaa", PartPhysHash: "0xbb", RowCount: 3, Bytes: 512}},
		SourceClaimRoot:      "0xroot",
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: "db1.t", PartitionID: "all", NewPartsLtHashSum: "0xaa"}},
	}
}

func goldenPromotionAck() arbiter.PromotionAck {
	return arbiter.PromotionAck{NodeID: "snode-1", PromotionSeq: 7, TableID: "db1.t", PartitionID: "all",
		PostPartitionCommitment: "0xpost", Applied: true,
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xaa", SafePartName: "all_7_7_0", PartPhysHash: "0xbb"}}}
}

// preSignedClaimsGolden lists, for every Raft command the signed-claims stage
// extends, the shape a pre-activation leader proposes and its exact bytes,
// computed from arbiter-proto 2eb3917 (protoc --encode arbiter.RaftCommand).
func preSignedClaimsGolden() []signedClaimsGolden {
	lanes := legacyGoldenUpdate()
	lanes.ExpectedEpoch = 2
	lanes.ArtifactDispositionCapability = 1
	lanes.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	ev := func(block, logIndex uint64, hash, tx string) arbiter.L2EventRef {
		return arbiter.L2EventRef{BlockNumber: block, BlockHash: hash, LogIndex: logIndex, TxHash: tx}
	}
	return []signedClaimsGolden{
		{"register_rc_with_parts", Command{RegisterRC: &RegisterRC{RC: goldenClaim()}},
			"2288010a85010a340a2a307830303030303030303030303030303030303030303030303030303030303030303030303030306131102a1a04396631631207736e6f64652d311a280a056462312e741203616c6c1a09616c6c5f315f315f302204307861612a0430786262300338800422063078726f6f742a120a056462312e741203616c6c1a0430786161"},
		{"record_promotion_ack", Command{RecordPromotionAck: &RecordPromotionAck{Ack: goldenPromotionAck()}},
			"4a3c0a3a0a07736e6f64652d3110071a056462312e742203616c6c2a063078706f737432170a04307861611209616c6c5f375f375f301a04307862623801"},
		{"record_cleanup_ack", Command{RecordCleanupAck: &RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "snode-1", PromotionSeq: 7, TableID: "db1.t", PartitionID: "all"}}},
			"62190a170a07736e6f64652d3110071a056462312e742203616c6c"},
		{"register_snode", Command{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}},
			"7a090a070a027331120102"},
		{"register_verifier", Command{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier},
			Ed25519Pubkey: bytes.Repeat([]byte{7}, 32), DialAddr: "v1:7080"}}},
			"7a340a320a0276311201011a200707070707070707070707070707070707070707070707070707070707070707220776313a37303830"},
		{"mark_active", Command{MarkActive: &MarkActive{NodeID: "snode-1"}}, "8201090a07736e6f64652d31"},
		{"evict_node", Command{EvictNode: &EvictNode{NodeID: "snode-1", Reason: "operator"}}, "8a01130a07736e6f64652d3112086f70657261746f72"},
		{"record_table_purged", Command{RecordTablePurged: &RecordTablePurged{NodeID: "verifier-1", IncarnationSeq: 5}}, "9a020e0a0a76657269666965722d311005"},
		{"add_table", Command{AddTable: &AddTable{DatabaseID: "db1", TableID: "t",
			Created: ev(5508940, 2, "0xb1", "0xt1"), Schema: ev(5508941, 0, "0xb2", "0xt2"),
			SchemaVersion: 1, SchemaHash: "0xh", SchemaJSON: `{"table_id":"db1.t"}`}},
			"82024d0a036462311201741a1308cc9ed0021204307862311802220430787431221108cd9ed002120430786232220430787432280132033078683a147b227461626c655f6964223a226462312e74227d"},
		{"seed_legacy_tables", Command{SeedLegacyTables: &SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 5508930, Hash: "0xb0"},
			Tables: []arbiter.LegacyTable{{DatabaseID: "db1", TableID: "old", Created: ev(100, 1, "0xc", "0xd")}}}},
			"fa01290a0b08c29ed002120430786230121a0a0364623112036f6c641a0e0864120330786318012203307864"},
		{"update_consensus_params_with_lanes", Command{UpdateConsensusParams: &UpdateConsensusParams{Update: lanes, AuthorityJWS: "h.p.s"}},
			"920198010a8e010a036e65741209307867656e657369731802220830786469676573742a2a3078303030303030303030303030303030303030303030303030303030303030303030303030303062323001380740014a3908cddae103122a30783030303030303030303030303030303030303030303030303030303030303030303030303030643120642a047361666552030880021205682e702e73"},
	}
}

// TestPreSignedClaimsCommandBytesAreFrozen pins the RaftCommand bytes of every
// command kind the signed-claims stage extends, in its pre-activation shape
// (housegate spec 2026-10-10 §16; plan S1-A CONTRACT §0). Until the
// activation every voter, upgraded or not, must decode exactly these bytes: a
// new field leaking into one of them forks an un-upgraded voter.
// Never regenerate these values: a diff here is a consensus break.
func TestPreSignedClaimsCommandBytesAreFrozen(t *testing.T) {
	for _, tc := range preSignedClaimsGolden() {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Encode(tc.cmd)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if hex.EncodeToString(got) != tc.want {
				t.Fatalf("encoded bytes changed:\n got %s\nwant %s", hex.EncodeToString(got), tc.want)
			}
			if _, err := Decode(got); err != nil {
				t.Fatalf("Decode(own bytes): %v", err)
			}
		})
	}
}
```

Append to `authority/consensus_golden_test.go`:

```go
// TestConsensusParamsUpdateWithLanesHashIsFrozen pins the digest of the update
// shape a network commits after client lanes (artifact disposition and lanes
// set, no signed-claims field). si_indexers and verifiers are omitted while
// absent, so adding them must leave it unchanged. Never regenerate this value.
func TestConsensusParamsUpdateWithLanesHashIsFrozen(t *testing.T) {
	update := arbiter.ConsensusParamsUpdate{
		NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", ExpectedEpoch: 2, PreviousParamsDigest: "0xdigest",
		AuthorityAddresses: []string{"0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705"}, MaxWriters: 1, ExpectedPromotionSeq: 3,
		ArtifactDispositionCapability: 1,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000D1",
			SIIndexerID: 0, ActivationBlock: 5508931, Confirmation: arbiter.TableRegistryConfirmationSafe},
		ClientLanes: &arbiter.ClientLaneParams{MaxLanesPerAccount: 256},
	}
	got, err := ConsensusParamsUpdateHash(update)
	if err != nil {
		t.Fatal(err)
	}
	const want = "0xebfe1e7715581d067ce52f54e561d7b401a7250084695d4b96432a94f70fdb42"
	if got != want {
		t.Fatalf("ConsensusParamsUpdateHash = %s, want %s", got, want)
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && bazel test //wire:wire_test //authority:authority_test --test_filter='TestPreSignedClaimsCommandBytesAreFrozen|TestConsensusParamsUpdateWithLanesHashIsFrozen|TestLegacyCommandBytesAreFrozen|TestConsensusParamsUpdateHashIsFrozen' --test_output=errors`
Expected: both targets PASS on unchanged `origin/main` (that is what makes these "the previous release's bytes"). If one case FAILS here, the plan's literal is wrong: replace it with the printed `got` (on unchanged code it is by definition the previous release's encoding), say so in the commit message, and never touch it again.

```bash
cd "$W" && git add wire/signed_claims_golden_test.go authority/consensus_golden_test.go wire/BUILD.bazel && git commit -m "test(wire): freeze pre-signed-claims command bytes and a lanes-era update digest

Captured on the release before multi-source SI stage 1 so a signed-claims
field that leaks into a pre-activation command fails here instead of
forking an un-upgraded voter (housegate spec 2026-10-10 §16).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 3: Pin `PROTO_TAG` together with the canonical mirrors (one commit)**

arbiter-core has no update script for arbiter-proto (`scripts/update-housegate.sh` is Housegate-only). arbiter-proto is a plain `go.mod` require (`go.mod:14`) that Bazel resolves through `go_deps.from_file` (`MODULE.bazel:29`, `use_repo` entry `com_github_sentioxyz_arbiter_proto`), so the pin is `go get` + `go mod tidy` + `bazel mod tidy` + gazelle, with no `MODULE.bazel` or README edit (the housegate `upgrade-dependency` skill, Step 3). `TestArbiterMirrorsMatchProto` fails the moment the new proto lands unless the same commit teaches it the transport-only fields and adds the canonical ones (CONTRACT §3a): `source_jws` on `RCRecord` / `PromotionAck` / `CleanupAck` and `signer_jws` / `ed25519_signature` on `NodeRegistration` are request-only and must never become fields of the canonical Go structs; `NodeRegistration.registration_seq` is canonical (signed).

First the failing test. In `conformance/arbiter_wire_test.go` replace `TestArbiterMirrorsMatchProto` (lines 51-70) with:

```go
func TestArbiterMirrorsMatchProto(t *testing.T) {
	assertMirror(t, arbiter.ConsensusParamsUpdate{}, &pb.ConsensusParamsUpdate{})
	assertMirror(t, arbiter.StatementID{}, &pb.StatementID{})
	assertMirror(t, arbiter.StatementEnvelope{}, &pb.StatementEnvelopeV2{})
	assertMirror(t, arbiter.CandidatePart{}, &pb.CandidatePart{})
	assertMirror(t, arbiter.PartitionLtHashSum{}, &pb.PartitionLtHashSum{})
	// source_jws signs the canonical body; it rides beside it in RegisterRCCmd.
	assertMirror(t, arbiter.RCRecord{}, &pb.RCRecord{}, "source_jws")
	assertMirror(t, arbiter.PartScan{}, &pb.PartScan{})
	assertMirror(t, arbiter.ByteSideScanMsg{}, &pb.ByteSideScanMsg{})
	assertMirror(t, arbiter.AnchorRef{}, &pb.AnchorRef{})
	// registration_seq is canonical (signed); the signatures and features are not.
	assertMirror(t, arbiter.NodeRegistration{}, &pb.NodeRegistration{}, "features", "signer_jws", "ed25519_signature")
	assertMirror(t, arbiter.SafePartMapping{}, &pb.SafePartMapping{})
	assertMirror(t, arbiter.PromotionAck{}, &pb.PromotionAck{}, "source_jws")
	assertMirror(t, arbiter.CleanupAck{}, &pb.CleanupAck{}, "source_jws")
	assertMirror(t, arbiter.TableRegistryParams{}, &pb.TableRegistryParams{})
	assertMirror(t, arbiter.ClientLaneParams{}, &pb.ClientLaneParams{})
	assertMirror(t, arbiter.SIIndexerEntry{}, &pb.SIIndexerEntry{})
	assertMirror(t, arbiter.VerifierEntry{}, &pb.VerifierEntry{})
	assertMirror(t, arbiter.L2BlockRef{}, &pb.L2BlockRef{})
	assertMirror(t, arbiter.L2EventRef{}, &pb.L2EventRef{})
	assertMirror(t, arbiter.LegacyTable{}, &pb.LegacyTable{})
}
```

and in `TestEnumNumbersMatchProto` (line 82) extend the chain:

```go
		int32(arbiter.AdmissionCodeLaneBudgetExceeded) != int32(pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED) ||
		int32(arbiter.AdmissionCodeSourceUnavailable) != int32(pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE) {
```

Create `conformance/signed_claims_test.go`:

```go
package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestSignedClaimsFieldNumbers pins, on the side that encodes and strictly
// decodes them, every arbiter-proto field the signed-claims stage adds
// (housegate spec 2026-10-10 §6; plan S1-A CONTRACT §1, §3a). wire.Decode
// refuses unknown fields, so a voter built against other numbers would refuse
// or misread the first command that carries one of them.
func TestSignedClaimsFieldNumbers(t *testing.T) {
	const (
		kStr = protoreflect.StringKind
		kU64 = protoreflect.Uint64Kind
		kMsg = protoreflect.MessageKind
	)
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		optional bool
		msgType  protoreflect.FullName
	}{
		{&pb.SIIndexerEntry{}, "indexer_id", 1, kU64, false, false, ""},
		{&pb.SIIndexerEntry{}, "activation_block", 2, kU64, false, false, ""},
		{&pb.SIIndexerEntry{}, "signer", 3, kStr, false, false, ""},
		{&pb.SIIndexerEntry{}, "snode_node_id", 4, kStr, false, false, ""},
		{&pb.SIIndexerEntry{}, "enrollment_jws", 5, kStr, false, false, ""},
		{&pb.VerifierEntry{}, "node_id", 1, kStr, false, false, ""},
		{&pb.VerifierEntry{}, "ed25519_pubkey", 2, protoreflect.BytesKind, false, false, ""},
		{&pb.ConsensusParamsUpdate{}, "si_indexers", 11, kMsg, true, false, "arbiter.SIIndexerEntry"},
		{&pb.ConsensusParamsUpdate{}, "verifiers", 12, kMsg, true, false, "arbiter.VerifierEntry"},
		{&pb.ConsensusMutableParams{}, "si_indexers", 6, kMsg, true, false, "arbiter.SIIndexerEntry"},
		{&pb.ConsensusMutableParams{}, "verifiers", 7, kMsg, true, false, "arbiter.VerifierEntry"},
		{&pb.EvictNodeRequest{}, "node_id", 1, kStr, false, false, ""},
		{&pb.EvictNodeRequest{}, "expected_registration_seq", 2, kU64, false, false, ""},
		{&pb.EvictNodeRequest{}, "reason", 3, kStr, false, false, ""},
		{&pb.EvictNodeRequest{}, "authority_jws", 4, kStr, false, false, ""},
		{&pb.TableRegistrySnapshot{}, "si_indexers", 7, kMsg, true, false, "arbiter.SIIndexerEntry"},
		{&pb.TableRegistrySnapshot{}, "seeded_indexers", 8, kU64, true, false, ""},
		{&pb.TableIncarnation{}, "owner_indexer_id", 18, kU64, false, true, ""},
		{&pb.AddTableCmd{}, "owner_indexer_id", 8, kU64, false, true, ""},
		{&pb.SeedLegacyTablesCmd{}, "indexer_id", 3, kU64, false, true, ""},
		{&pb.RecordTablePurgedCmd{}, "signer_jws", 3, kStr, false, false, ""},
		{&pb.RecordTablePurgedCmd{}, "ed25519_signature", 4, kStr, false, false, ""},
		{&pb.NodeRegistration{}, "registration_seq", 6, kU64, false, false, ""},
		{&pb.NodeRegistration{}, "signer_jws", 7, kStr, false, false, ""},
		{&pb.NodeRegistration{}, "ed25519_signature", 8, kStr, false, false, ""},
		{&pb.NodeRef{}, "registration_seq", 2, kU64, false, false, ""},
		{&pb.NodeRef{}, "signer_jws", 3, kStr, false, false, ""},
		{&pb.NodeRef{}, "ed25519_signature", 4, kStr, false, false, ""},
		{&pb.RCRecord{}, "source_jws", 6, kStr, false, false, ""},
		{&pb.PromotionAck{}, "source_jws", 10, kStr, false, false, ""},
		{&pb.CleanupAck{}, "source_jws", 5, kStr, false, false, ""},
		{&pb.RegisterRCCmd{}, "source_jws", 2, kStr, false, false, ""},
		{&pb.RecordPromotionAckCmd{}, "source_jws", 2, kStr, false, false, ""},
		{&pb.RecordCleanupAckCmd{}, "source_jws", 2, kStr, false, false, ""},
		{&pb.RegisterNodeCmd{}, "signer_jws", 2, kStr, false, false, ""},
		{&pb.RegisterNodeCmd{}, "ed25519_signature", 3, kStr, false, false, ""},
		{&pb.MarkActiveCmd{}, "registration_seq", 2, kU64, false, false, ""},
		{&pb.MarkActiveCmd{}, "signer_jws", 3, kStr, false, false, ""},
		{&pb.MarkActiveCmd{}, "ed25519_signature", 4, kStr, false, false, ""},
		{&pb.EvictNodeCmd{}, "expected_registration_seq", 3, kU64, false, false, ""},
		{&pb.EvictNodeCmd{}, "authority_jws", 4, kStr, false, false, ""},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated || f.HasOptionalKeyword() != tc.optional {
				t.Fatalf("field = %v, want number %d kind %s repeated %v optional %v", f, tc.number, tc.kind, tc.repeated, tc.optional)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE); got != 10 {
		t.Fatalf("ADMISSION_CODE_SOURCE_UNAVAILABLE = %d, want 10", got)
	}
	m := pb.File_consensus_proto.Services().ByName("ConsensusAdmin").Methods().ByName("EvictNode")
	if m == nil || m.Input().FullName() != "arbiter.EvictNodeRequest" || m.Output().FullName() != "arbiter.Ack" || m.IsStreamingClient() || m.IsStreamingServer() {
		t.Fatalf("ConsensusAdmin.EvictNode = %v, want unary arbiter.EvictNodeRequest -> arbiter.Ack", m)
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && bazel test //conformance:conformance_test --test_output=errors`
Expected: build failure, `undefined: pb.SIIndexerEntry` (the module still resolves arbiter-proto v0.9.0) and `undefined: arbiter.SIIndexerEntry`.

Now pin and add the canonical mirrors:

```bash
cd "$W" && GOWORK=off go get "github.com/sentioxyz/arbiter-proto@$PROTO_TAG" && GOWORK=off go mod tidy && bazel mod tidy && bazel run //:gazelle
```

Expected: `go.mod:14` reads `github.com/sentioxyz/arbiter-proto $PROTO_TAG`; `MODULE.bazel` unchanged.

In `consensus.go` add after `ClientLaneParams.Validate` (line 31):

```go
// SIIndexerEntry enrols one indexer into the network's storage-integrity layer
// (housegate spec 2026-10-10 D3, §6.1-§6.2). The list is append-only: an
// entry's IndexerID, ActivationBlock and SNodeNodeID never change, and its
// Signer changes only together with a new EnrollmentJWS by the new signer.
type SIIndexerEntry struct {
	IndexerID       uint64 `json:"indexer_id"`
	ActivationBlock uint64 `json:"activation_block"`
	// Signer is the indexer's IndexerRegistry signer, lowercase 0x + 40 hex;
	// it signs every message of the entry's SNode (spec D6).
	Signer string `json:"signer"`
	// SNodeNodeID names the indexer's one SNode, the source of every
	// statement on the indexer's tables (spec D5).
	SNodeNodeID string `json:"snode_node_id"`
	// EnrollmentJWS is Signer's ES256K enrolment statement (purpose
	// arbiter-snode-enrollment-v1) over {network_id, genesis_snapshot_id,
	// indexer_id, snode_node_id}; the FSM verifies it.
	EnrollmentJWS string `json:"enrollment_jws"`
}

// VerifierEntry lists one verifier that may register (spec D7): its node id
// and the ed25519 key that signs its registration, activation, purge reports
// and evidence.
type VerifierEntry struct {
	NodeID        string `json:"node_id"`
	Ed25519Pubkey []byte `json:"ed25519_pubkey"`
}
```

and append to `ConsensusParamsUpdate` (after `ClientLanes`, line 64):

```go
	// SIIndexers is the enrolled SI indexer list (housegate spec 2026-10-10
	// §6.1), sorted by IndexerID. Absent (nil) until the signed-claims
	// activation and omitted from the canonical form, so every earlier digest
	// is unchanged; once set every update carries the complete list.
	SIIndexers []SIIndexerEntry `json:"si_indexers,omitempty"`
	// Verifiers is the governed verifier set (spec D7), sorted by NodeID.
	// Absent until set with SIIndexers; then carried by every update.
	Verifiers []VerifierEntry `json:"verifiers,omitempty"`
```

In `types.go` append to the `AdmissionCode` block (after line 138):

```go
	// AdmissionCodeSourceUnavailable: the target's owner SNode is not registered and Active (housegate spec 2026-10-10 §6.4); nothing changed, retryable.
	AdmissionCodeSourceUnavailable AdmissionCode = 10
```

and append to `NodeRegistration` (after `DialAddr`, line 283):

```go
	// RegistrationSeq orders one node's registrations once signed claims are
	// active (housegate spec 2026-10-10 §6.5): Apply accepts only a value
	// above the last one it applied for the node. It is part of the signed
	// body. Zero (omitted) keeps every pre-activation registration and its
	// canonical form byte-identical.
	RegistrationSeq uint64 `json:"registration_seq,omitempty"`
```

Run: `cd "$W" && bazel build //... && bazel test //conformance:conformance_test //wire:wire_test //authority:authority_test //:arbiter-core_test --test_output=errors`
Expected: all PASS — `TestArbiterMirrorsMatchProto`, `TestEnumNumbersMatchProto`, `TestSignedClaimsFieldNumbers`, and the frozen tests of Step 2 and of `wire/legacy_command_golden_test.go` unchanged (no new field is set yet, and nil or zero encodes nothing).

```bash
cd "$W" && git add go.mod go.sum MODULE.bazel.lock consensus.go types.go conformance && git commit -m "chore(deps): pin arbiter-proto $PROTO_TAG and mirror the signed-claims fields

SIIndexerEntry, VerifierEntry, ConsensusParamsUpdate.SIIndexers/Verifiers,
NodeRegistration.RegistrationSeq and AdmissionCodeSourceUnavailable (10).
The signature fields on RCRecord, PromotionAck, CleanupAck and
NodeRegistration are request-only and stay out of the canonical structs
(housegate spec 2026-10-10 §6; plan S1-A CONTRACT §3a).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

(If `MODULE.bazel.lock` did not change, drop it from `git add`.)

- [ ] **Step 4: Write the failing validation and normalisation tests**

In `consensus_test.go` replace `TestLocalNodeFeatures` (lines 42-51), add `"slices"` and `"strings"` to the imports, and append:

```go
func TestLocalNodeFeatures(t *testing.T) {
	got := LocalNodeFeatures()
	if !slices.Equal(got, []string{"client_lanes_v1", "signed_claims_v1"}) || ClientLanesFeature != "client_lanes_v1" || SignedClaimsFeature != "signed_claims_v1" {
		t.Fatalf("LocalNodeFeatures = %v", got)
	}
	got[0] = "mutated"
	if LocalNodeFeatures()[0] != ClientLanesFeature {
		t.Fatal("LocalNodeFeatures must return a fresh slice")
	}
}

func validSIIndexerEntry() SIIndexerEntry {
	return SIIndexerEntry{IndexerID: 1, ActivationBlock: 5_510_731, Signer: "0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3",
		SNodeNodeID: "snode-2", EnrollmentJWS: "h.p.s"}
}

func TestSIIndexerEntryValidate(t *testing.T) {
	if err := validSIIndexerEntry().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SIIndexerEntry){
		"zero activation block": func(e *SIIndexerEntry) { e.ActivationBlock = 0 },
		"uppercase signer":      func(e *SIIndexerEntry) { e.Signer = "0x20C87974E9AD8113BC6C71F3B6ADB2B472A616F3" },
		"short signer":          func(e *SIIndexerEntry) { e.Signer = "0x20c87974" },
		"unprefixed signer":     func(e *SIIndexerEntry) { e.Signer = "20c87974e9ad8113bc6c71f3b6adb2b472a616f3aa" },
		"zero signer":           func(e *SIIndexerEntry) { e.Signer = "0x0000000000000000000000000000000000000000" },
		"empty snode node id":   func(e *SIIndexerEntry) { e.SNodeNodeID = "" },
		"snode node id space":   func(e *SIIndexerEntry) { e.SNodeNodeID = "snode 2" },
		"snode node id newline": func(e *SIIndexerEntry) { e.SNodeNodeID = "snode-2\n" },
		"snode node id NUL":     func(e *SIIndexerEntry) { e.SNodeNodeID = "snode\x002" },
		"blank enrolment":       func(e *SIIndexerEntry) { e.EnrollmentJWS = " " },
	} {
		t.Run(name, func(t *testing.T) {
			e := validSIIndexerEntry()
			mutate(&e)
			if err := e.Validate(); err == nil || !strings.Contains(err.Error(), "si_indexers") {
				t.Fatalf("%+v: err = %v, want an si_indexers refusal", e, err)
			}
		})
	}
}

func TestVerifierEntryValidate(t *testing.T) {
	if err := (VerifierEntry{NodeID: "verifier-1", Ed25519Pubkey: make([]byte, 32)}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []VerifierEntry{
		{Ed25519Pubkey: make([]byte, 32)}, {NodeID: " ", Ed25519Pubkey: make([]byte, 32)},
		{NodeID: "verifier-1"}, {NodeID: "verifier-1", Ed25519Pubkey: make([]byte, 31)}, {NodeID: "verifier-1", Ed25519Pubkey: make([]byte, 33)},
	} {
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "verifiers") {
			t.Fatalf("%+v: err = %v, want a verifiers refusal", bad, err)
		}
	}
}

func TestSignedClaimsFieldsAreOmittedWhileAbsent(t *testing.T) {
	b, err := json.Marshal(ConsensusParamsUpdate{NetworkID: "n", SIIndexers: []SIIndexerEntry{}, Verifiers: []VerifierEntry{}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "si_indexers") || strings.Contains(string(b), "verifiers") {
		t.Fatalf("absent signed-claims lists leaked into the canonical form: %s", b)
	}
	b, err = json.Marshal(NodeRegistration{NodeID: "s1", Roles: []NodeRole{NodeRoleSNode}})
	if err != nil || string(b) != `{"node_id":"s1","roles":[2],"ed25519_pubkey":null}` {
		t.Fatalf("pre-activation registration JSON = %s (%v): a zero registration_seq must stay omitted", b, err)
	}
	b, _ = json.Marshal(NodeRegistration{NodeID: "s1", Roles: []NodeRole{NodeRoleSNode}, RegistrationSeq: 7})
	if string(b) != `{"node_id":"s1","roles":[2],"ed25519_pubkey":null,"registration_seq":7}` {
		t.Fatalf("registration JSON = %s", b)
	}
}
```

Create `authority/consensus_signed_claims_test.go` (it reuses `testConsensusUpdate`, `authority/consensus_test.go:16`, whose `MaxWriters` is 2):

```go
package authority

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func signedClaimsTestUpdate() arbiter.ConsensusParamsUpdate {
	u := testConsensusUpdate()
	u.SIIndexers = []arbiter.SIIndexerEntry{
		{IndexerID: 1, ActivationBlock: 200, Signer: "0x89FD7C610AC4AA2E17C3B15A2C386A4B215F96D9", SNodeNodeID: "snode-2", EnrollmentJWS: "h.p.s"},
		{IndexerID: 0, ActivationBlock: 100, Signer: "0x563Bd9e11d18b6eA60c2f159F8D3062d30E8039e", SNodeNodeID: "snode-1", EnrollmentJWS: "h.p.s"},
	}
	u.Verifiers = []arbiter.VerifierEntry{
		{NodeID: "verifier-3", Ed25519Pubkey: bytes.Repeat([]byte{3}, 32)},
		{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{1}, 32)},
		{NodeID: "verifier-2", Ed25519Pubkey: bytes.Repeat([]byte{2}, 32)},
	}
	return u
}

func TestNormalizeConsensusParamsUpdateSignedClaims(t *testing.T) {
	in := signedClaimsTestUpdate()
	callerEntries := append([]arbiter.SIIndexerEntry(nil), in.SIIndexers...)
	got, err := NormalizeConsensusParamsUpdate(in)
	if err != nil {
		t.Fatal(err)
	}
	if got.SIIndexers[0].IndexerID != 0 || got.SIIndexers[1].IndexerID != 1 ||
		got.SIIndexers[0].Signer != "0x563bd9e11d18b6ea60c2f159f8d3062d30e8039e" || got.SIIndexers[1].Signer != "0x89fd7c610ac4aa2e17c3b15a2c386a4b215f96d9" {
		t.Fatalf("si_indexers = %+v: want sorted by indexer_id with lowercase signers", got.SIIndexers)
	}
	if !reflect.DeepEqual(in.SIIndexers, callerEntries) {
		t.Fatal("normalization mutated the caller's si_indexers")
	}
	if got.Verifiers[0].NodeID != "verifier-1" || got.Verifiers[1].NodeID != "verifier-2" || got.Verifiers[2].NodeID != "verifier-3" {
		t.Fatalf("verifiers = %+v: want sorted by node_id", got.Verifiers)
	}
	got.Verifiers[0].Ed25519Pubkey[0] = 0xff
	if in.Verifiers[1].Ed25519Pubkey[0] != 1 {
		t.Fatal("normalized verifier keys alias the caller's bytes")
	}
	for name, mutate := range map[string]func(*arbiter.ConsensusParamsUpdate){
		"zero activation block":       func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].ActivationBlock = 0 },
		"short signer":                func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].Signer = "0x89fd" },
		"zero signer":                 func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].Signer = "0x0000000000000000000000000000000000000000" },
		"empty snode node id":         func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].SNodeNodeID = "" },
		"snode node id with a space":  func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].SNodeNodeID = "snode 2" },
		"missing enrolment":           func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].EnrollmentJWS = "" },
		"duplicate indexer":           func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].IndexerID = 0 },
		"duplicate snode node id":     func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].SNodeNodeID = "snode-1" },
		"fewer writers than indexers": func(u *arbiter.ConsensusParamsUpdate) { u.MaxWriters = 1 },
		"empty verifier node id":      func(u *arbiter.ConsensusParamsUpdate) { u.Verifiers[0].NodeID = "" },
		"short verifier key":          func(u *arbiter.ConsensusParamsUpdate) { u.Verifiers[0].Ed25519Pubkey = make([]byte, 31) },
		"duplicate verifier":          func(u *arbiter.ConsensusParamsUpdate) { u.Verifiers[0].NodeID = "verifier-1" },
		"shared verifier key":         func(u *arbiter.ConsensusParamsUpdate) { u.Verifiers[0].Ed25519Pubkey = bytes.Repeat([]byte{1}, 32) },
	} {
		t.Run(name, func(t *testing.T) {
			u := signedClaimsTestUpdate()
			mutate(&u)
			_, err := NormalizeConsensusParamsUpdate(u)
			if err == nil || (!strings.Contains(err.Error(), "si_indexers") && !strings.Contains(err.Error(), "verifiers")) {
				t.Fatalf("err = %v, want a refusal naming the list", err)
			}
		})
	}
}

func TestNormalizeConsensusParamsUpdateKeepsAbsentListsNil(t *testing.T) {
	u := testConsensusUpdate()
	u.SIIndexers, u.Verifiers = []arbiter.SIIndexerEntry{}, []arbiter.VerifierEntry{}
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || got.SIIndexers != nil || got.Verifiers != nil {
		t.Fatalf("empty lists normalised to %#v / %#v (%v): want nil, never []", got.SIIndexers, got.Verifiers, err)
	}
	a, errA := ConsensusParamsUpdateHash(testConsensusUpdate())
	b, errB := ConsensusParamsUpdateHash(u)
	if errA != nil || errB != nil || a != b {
		t.Fatalf("an empty list changed the digest: %s vs %s (%v, %v)", a, b, errA, errB)
	}
}

func TestConsensusParamsUpdateHashBindsSignedClaims(t *testing.T) {
	hash := func(u arbiter.ConsensusParamsUpdate) string {
		t.Helper()
		h, err := ConsensusParamsUpdateHash(u)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	full := hash(signedClaimsTestUpdate())
	rotated := signedClaimsTestUpdate()
	rotated.SIIndexers[0].EnrollmentJWS = "other.p.s"
	fewer := signedClaimsTestUpdate()
	fewer.Verifiers = fewer.Verifiers[:2]
	if hash(testConsensusUpdate()) == full || hash(rotated) == full || hash(fewer) == full {
		t.Fatal("si_indexers and verifiers must be bound by the update hash")
	}
	shuffled := signedClaimsTestUpdate()
	shuffled.SIIndexers[0], shuffled.SIIndexers[1] = shuffled.SIIndexers[1], shuffled.SIIndexers[0]
	if hash(shuffled) != full {
		t.Fatal("entry order must not change the digest: normalisation sorts")
	}
}
```

- [ ] **Step 5: Run them and watch them fail**

Run: `cd "$W" && bazel run //:gazelle && bazel test //:arbiter-core_test //authority:authority_test --test_output=errors`
Expected: build failure in both targets: `e.Validate undefined (type SIIndexerEntry has no field or method Validate)` and `undefined: SignedClaimsFeature`.

- [ ] **Step 6: Implement validation, the feature and normalisation**

In `consensus.go` set the imports to `"crypto/ed25519"`, `"fmt"`, `"regexp"`, `"strings"`, `"unicode"`, replace `LocalNodeFeatures` (line 16) and add the constants and methods:

```go
// SignedClaimsFeature is the capability string of a binary that signs and
// verifies SNode and verifier messages (housegate spec 2026-10-10 §6.5,
// §6.7). The signed-claims activation gate requires it from every voter and
// every registered data-plane node.
const SignedClaimsFeature = "signed_claims_v1"

// LocalNodeFeatures returns this binary's capability strings in a fresh slice.
func LocalNodeFeatures() []string { return []string{ClientLanesFeature, SignedClaimsFeature} }

// signerAddressPattern is a normalized (lowercase) 20-byte address.
var signerAddressPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

const zeroAddress = "0x0000000000000000000000000000000000000000"

// Validate checks one normalized entry (lowercase Signer). Whether its
// enrolment statement verifies, and every rule over committed state, is the
// FSM's.
func (e SIIndexerEntry) Validate() error {
	switch {
	case e.ActivationBlock == 0:
		return fmt.Errorf("si_indexers: indexer %d: activation_block must be at least 1", e.IndexerID)
	case !signerAddressPattern.MatchString(e.Signer) || e.Signer == zeroAddress:
		return fmt.Errorf("si_indexers: indexer %d: signer must be a non-zero lowercase 0x-prefixed 20-byte address", e.IndexerID)
	case !validNodeID(e.SNodeNodeID):
		return fmt.Errorf("si_indexers: indexer %d: snode_node_id must be non-empty and free of whitespace and control characters", e.IndexerID)
	case strings.TrimSpace(e.EnrollmentJWS) == "":
		return fmt.Errorf("si_indexers: indexer %d: enrollment_jws is required", e.IndexerID)
	}
	return nil
}

// Validate checks one verifier entry.
func (e VerifierEntry) Validate() error {
	switch {
	case strings.TrimSpace(e.NodeID) == "":
		return fmt.Errorf("verifiers: node_id is required")
	case len(e.Ed25519Pubkey) != ed25519.PublicKeySize:
		return fmt.Errorf("verifiers: %s: ed25519_pubkey must be %d bytes, got %d", e.NodeID, ed25519.PublicKeySize, len(e.Ed25519Pubkey))
	}
	return nil
}

func validNodeID(id string) bool {
	return id != "" && !strings.ContainsFunc(id, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
}
```

In `authority/consensus.go` add `"bytes"` and `"cmp"` to the imports and replace the tail of `NormalizeConsensusParamsUpdate` (the final `return cmd, nil`, line 68) with:

```go
	siIndexers, err := normalizeSIIndexers(cmd.SIIndexers, cmd.MaxWriters)
	if err != nil {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: %w", err)
	}
	verifiers, err := normalizeVerifiers(cmd.Verifiers)
	if err != nil {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: %w", err)
	}
	cmd.SIIndexers, cmd.Verifiers = siIndexers, verifiers
	return cmd, nil
}

// normalizeSIIndexers returns a validated copy sorted by indexer id with
// lowercase signers. An absent or empty list stays nil (never []), so it is
// omitted from the canonical form.
func normalizeSIIndexers(in []arbiter.SIIndexerEntry, maxWriters uint64) ([]arbiter.SIIndexerEntry, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]arbiter.SIIndexerEntry, len(in))
	for i, e := range in {
		e.Signer = strings.ToLower(e.Signer)
		if err := e.Validate(); err != nil {
			return nil, err
		}
		out[i] = e
	}
	slices.SortFunc(out, func(a, b arbiter.SIIndexerEntry) int { return cmp.Compare(a.IndexerID, b.IndexerID) })
	nodes := make(map[string]bool, len(out))
	for i, e := range out {
		if i > 0 && out[i-1].IndexerID == e.IndexerID {
			return nil, fmt.Errorf("si_indexers: duplicate indexer_id %d", e.IndexerID)
		}
		if nodes[e.SNodeNodeID] {
			return nil, fmt.Errorf("si_indexers: snode_node_id %q is enrolled twice", e.SNodeNodeID)
		}
		nodes[e.SNodeNodeID] = true
	}
	if maxWriters < uint64(len(out)) {
		return nil, fmt.Errorf("si_indexers: max_writers %d is below the %d enrolled indexers", maxWriters, len(out))
	}
	return out, nil
}

// normalizeVerifiers returns a validated deep copy sorted by node id. An
// absent or empty list stays nil.
func normalizeVerifiers(in []arbiter.VerifierEntry) ([]arbiter.VerifierEntry, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]arbiter.VerifierEntry, len(in))
	for i, v := range in {
		if err := v.Validate(); err != nil {
			return nil, err
		}
		out[i] = arbiter.VerifierEntry{NodeID: v.NodeID, Ed25519Pubkey: bytes.Clone(v.Ed25519Pubkey)}
	}
	slices.SortFunc(out, func(a, b arbiter.VerifierEntry) int { return strings.Compare(a.NodeID, b.NodeID) })
	keys := make(map[string]bool, len(out))
	for i, v := range out {
		if i > 0 && out[i-1].NodeID == v.NodeID {
			return nil, fmt.Errorf("verifiers: duplicate node_id %q", v.NodeID)
		}
		if keys[string(v.Ed25519Pubkey)] {
			return nil, fmt.Errorf("verifiers: %s reuses another verifier's ed25519_pubkey", v.NodeID)
		}
		keys[string(v.Ed25519Pubkey)] = true
	}
	return out, nil
}
```

(The cross-list rule "an SNode node id is no verifier's node id" depends on registered nodes too and is the FSM's, CONTRACT §3 rule 5.)

- [ ] **Step 7: Run and commit**

Run: `cd "$W" && bazel test //:arbiter-core_test //authority:authority_test //conformance:conformance_test --test_output=errors`
Expected: PASS, including `TestConsensusParamsUpdateHashIsFrozen` (`0x9a2297ef…`), `TestConsensusParamsUpdateWithLanesHashIsFrozen` (`0xebfe1e77…`) and `TestConsensusUpdateHashCanonicalizesAuthoritySet` (`0x895b8cb1…`) unchanged.

```bash
cd "$W" && git add consensus.go consensus_test.go authority && git commit -m "feat(consensus): validate and normalise si_indexers and verifiers

SIIndexerEntry/VerifierEntry validation, sorted deep-copied lists, unique
indexer ids, SNode node ids and verifier keys, max_writers >= enrolled
indexers, nil for absent lists so every earlier digest is unchanged;
LocalNodeFeatures advertises signed_claims_v1 (housegate spec 2026-10-10
§6.1).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 8: Write the failing wire tests**

Create `wire/signed_claims_params_test.go` (it reuses `legacyGoldenUpdate`, `mustRoundTrip` from `wire/wire_test.go:12` and `registrySnapshotFixture` from `wire/table_registry_snapshot_test.go:18`, whose founding indexer is 1):

```go
package wire

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func siEntryW(id, activation uint64, node string) arbiter.SIIndexerEntry {
	return arbiter.SIIndexerEntry{IndexerID: id, ActivationBlock: activation, Signer: fmt.Sprintf("0x%040x", id+1),
		SNodeNodeID: node, EnrollmentJWS: "h.p.s"}
}

func TestSignedClaimsParamsWire(t *testing.T) {
	u := legacyGoldenUpdate()
	u.MaxWriters = 2
	u.SIIndexers = []arbiter.SIIndexerEntry{siEntryW(0, 100, "snode-1"), siEntryW(1, 200, "snode-2")}
	u.Verifiers = []arbiter.VerifierEntry{{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{7}, 32)}}
	if got := ConsensusParamsUpdateFromPB(ConsensusParamsUpdateToPB(u)); !reflect.DeepEqual(got, u) {
		t.Fatalf("update round trip\n got %+v\nwant %+v", got, u)
	}
	m := ConsensusParamsUpdateToPB(u)
	m.Verifiers[0].Ed25519Pubkey[0] = 9
	if u.Verifiers[0].Ed25519Pubkey[0] != 7 {
		t.Fatal("ToPB shares the caller's verifier key bytes")
	}
	empty := ConsensusParamsUpdateFromPB(&pb.ConsensusParamsUpdate{SiIndexers: []*pb.SIIndexerEntry{}, Verifiers: []*pb.VerifierEntry{}})
	if empty.SIIndexers != nil || empty.Verifiers != nil {
		t.Fatal("empty repeated fields must decode to nil")
	}
	// arbiter's ConsensusAdmin read builds ConsensusMutableParams with the list converters.
	mutable := &pb.ConsensusMutableParams{SiIndexers: SIIndexerEntriesToPB(u.SIIndexers), Verifiers: VerifierEntriesToPB(u.Verifiers)}
	if !reflect.DeepEqual(SIIndexerEntriesFromPB(mutable.GetSiIndexers()), u.SIIndexers) || !reflect.DeepEqual(VerifierEntriesFromPB(mutable.GetVerifiers()), u.Verifiers) {
		t.Fatal("ConsensusMutableParams list round trip diverged")
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
}

func TestRegistrationSeqRidesInsideTheRegistration(t *testing.T) {
	reg := arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 1760054400000}
	if got := RegistrationFromPB(RegistrationToPB(reg)); !reflect.DeepEqual(got, reg) {
		t.Fatalf("registration round trip = %+v", got)
	}
	mustRoundTrip(t, Command{RegisterNode: &RegisterNode{Registration: reg}})
}

// TestRegistryCommandsKeepTheirIndexer: indexer 0 is devnet2's founding
// indexer, so an absent owner (nil, before the activation) and an explicit 0
// must stay distinct on the wire and after decoding.
func TestRegistryCommandsKeepTheirIndexer(t *testing.T) {
	zero := uint64(0)
	ev := arbiter.L2EventRef{BlockNumber: 10, BlockHash: "0xb", LogIndex: 2, TxHash: "0xt"}
	add := Command{AddTable: &AddTable{DatabaseID: "db", TableID: "t", Created: ev, Schema: ev, SchemaVersion: 1,
		SchemaHash: "0xh", SchemaJSON: "{}", OwnerIndexerID: &zero}}
	got := mustRoundTrip(t, add)
	if got.AddTable.OwnerIndexerID == nil || *got.AddTable.OwnerIndexerID != 0 || got.AddTable.OwnerIndexerID == add.AddTable.OwnerIndexerID {
		t.Fatalf("owner = %v: indexer 0 must survive as a present, independent value", got.AddTable.OwnerIndexerID)
	}
	unowned := *add.AddTable
	unowned.OwnerIndexerID = nil
	if got := mustRoundTrip(t, Command{AddTable: &unowned}); got.AddTable.OwnerIndexerID != nil {
		t.Fatal("an absent owner must decode as nil")
	}
	seedFounding := Command{SeedLegacyTables: &SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 99, Hash: "0x9"}}}
	seedZero := Command{SeedLegacyTables: &SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 99, Hash: "0x9"}, IndexerID: &zero}}
	a, err := Encode(seedFounding)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encode(seedZero)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(a) == hex.EncodeToString(b) {
		t.Fatal("the founding seed (nil) and an explicit indexer 0 seed must encode differently")
	}
	if got := mustRoundTrip(t, seedFounding); got.SeedLegacyTables.IndexerID != nil {
		t.Fatal("the founding seed must decode with a nil indexer")
	}
	if got := mustRoundTrip(t, seedZero); got.SeedLegacyTables.IndexerID == nil || *got.SeedLegacyTables.IndexerID != 0 {
		t.Fatal("an explicit indexer 0 seed must decode as &0")
	}
}

func TestTableRegistrySnapshotCarriesSIIndexersAndOwners(t *testing.T) {
	s := registrySnapshotFixture(t) // founding indexer 1
	got, err := TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || got.SIIndexers != nil || got.SeededIndexers != nil || got.Incarnations[0].OwnerIndexerID != nil {
		t.Fatalf("pre-activation snapshot: %+v, %v", got, err)
	}
	if got.SignedClaimsActive() || got.Owner(got.Incarnations[3]) != 1 {
		t.Fatal("before the activation every incarnation belongs to the founding indexer")
	}
	one, two := uint64(1), uint64(2)
	s.SIIndexers = []arbiter.SIIndexerEntry{siEntryW(1, 100, "snode-1"), siEntryW(2, 300, "snode-2")}
	s.SeededIndexers = []uint64{1}
	for i := range s.Incarnations {
		s.Incarnations[i].OwnerIndexerID = &one
	}
	s.Incarnations[3].OwnerIndexerID = &two
	got, err = TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip (%v)\n got %+v\nwant %+v", err, got, s)
	}
	if !got.SignedClaimsActive() || got.Owner(got.Incarnations[3]) != 2 || got.Owner(got.Incarnations[0]) != 1 {
		t.Fatal("owners lost")
	}
	if e, ok := got.SIIndexer(2); !ok || e.SNodeNodeID != "snode-2" {
		t.Fatalf("SIIndexer(2) = %+v, %v", e, ok)
	}
	if _, ok := got.SIIndexer(3); ok {
		t.Fatal("an indexer without an entry must not resolve")
	}
	for name, mutate := range map[string]func(*pb.TableRegistrySnapshot){
		"unsorted entries":      func(m *pb.TableRegistrySnapshot) { m.SiIndexers[0], m.SiIndexers[1] = m.SiIndexers[1], m.SiIndexers[0] },
		"duplicate entry":       func(m *pb.TableRegistrySnapshot) { m.SiIndexers[1].IndexerId = 1 },
		"invalid entry":         func(m *pb.TableRegistrySnapshot) { m.SiIndexers[0].ActivationBlock = 0 },
		"seeded without entry":  func(m *pb.TableRegistrySnapshot) { m.SeededIndexers = []uint64{1, 9} },
		"unsorted seeded":       func(m *pb.TableRegistrySnapshot) { m.SeededIndexers = []uint64{2, 1} },
		"owner without entry":   func(m *pb.TableRegistrySnapshot) { nine := uint64(9); m.Incarnations[0].OwnerIndexerId = &nine },
		"owners without a list": func(m *pb.TableRegistrySnapshot) { m.SiIndexers, m.SeededIndexers = nil, nil },
	} {
		t.Run(name, func(t *testing.T) {
			m := TableRegistrySnapshotToPB(s)
			mutate(m)
			if _, err := TableRegistrySnapshotFromPB(m); err == nil {
				t.Fatal("the decoder accepted an inconsistent signed-claims view")
			}
		})
	}
}
```

- [ ] **Step 9: Run them and watch them fail**

Run: `cd "$W" && bazel run //:gazelle && bazel test //wire:wire_test --test_output=errors`
Expected: build failure: `undefined: SIIndexerEntriesToPB`, `unknown field OwnerIndexerID in struct literal of type AddTable`, `unknown field IndexerID in struct literal of type SeedLegacyTables`, `s.SIIndexers undefined`.

- [ ] **Step 10: Implement the wire changes**

In `wire/consensus.go` add `SIIndexers: SIIndexerEntriesFromPB(m.GetSiIndexers()), Verifiers: VerifierEntriesFromPB(m.GetVerifiers()),` to the `ConsensusParamsUpdateFromPB` literal (after `ClientLanes`, line 23), `SiIndexers: SIIndexerEntriesToPB(v.SIIndexers), Verifiers: VerifierEntriesToPB(v.Verifiers),` to `ConsensusParamsUpdateToPB` (after line 40), and append:

```go
// SIIndexerEntriesFromPB copies a repeated SIIndexerEntry; validation belongs to
// authority normalisation. An empty list decodes to nil.
func SIIndexerEntriesFromPB(ms []*pb.SIIndexerEntry) []arbiter.SIIndexerEntry {
	return mapSlice(ms, func(m *pb.SIIndexerEntry) arbiter.SIIndexerEntry {
		return arbiter.SIIndexerEntry{IndexerID: m.GetIndexerId(), ActivationBlock: m.GetActivationBlock(),
			Signer: m.GetSigner(), SNodeNodeID: m.GetSnodeNodeId(), EnrollmentJWS: m.GetEnrollmentJws()}
	})
}

// SIIndexerEntriesToPB returns independent transport messages (nil for an empty list).
func SIIndexerEntriesToPB(v []arbiter.SIIndexerEntry) []*pb.SIIndexerEntry {
	return mapSlice(v, func(e arbiter.SIIndexerEntry) *pb.SIIndexerEntry {
		return &pb.SIIndexerEntry{IndexerId: e.IndexerID, ActivationBlock: e.ActivationBlock,
			Signer: e.Signer, SnodeNodeId: e.SNodeNodeID, EnrollmentJws: e.EnrollmentJWS}
	})
}

// VerifierEntriesFromPB copies a repeated VerifierEntry including each key's bytes;
// an empty list and an empty key decode to nil.
func VerifierEntriesFromPB(ms []*pb.VerifierEntry) []arbiter.VerifierEntry {
	return mapSlice(ms, func(m *pb.VerifierEntry) arbiter.VerifierEntry {
		return arbiter.VerifierEntry{NodeID: m.GetNodeId(), Ed25519Pubkey: cloneBytes(m.GetEd25519Pubkey())}
	})
}

// VerifierEntriesToPB returns independent transport messages.
func VerifierEntriesToPB(v []arbiter.VerifierEntry) []*pb.VerifierEntry {
	return mapSlice(v, func(e arbiter.VerifierEntry) *pb.VerifierEntry {
		return &pb.VerifierEntry{NodeId: e.NodeID, Ed25519Pubkey: cloneBytes(e.Ed25519Pubkey)}
	})
}

// cloneBytes copies b; an empty slice becomes nil, the canonical empty form.
func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
```

In `wire/convert.go` add `RegistrationSeq: m.GetRegistrationSeq()` to `RegistrationFromPB` (line 147) and `RegistrationSeq: v.RegistrationSeq` to `RegistrationToPB` (line 153). Never copy `features`, `signer_jws` or `ed25519_signature` here: they are request-only (Task 3).

In `wire/table_registry.go` replace `SeedLegacyTables` and `AddTable` (lines 18-31) and add the helper:

```go
// SeedLegacyTables mirrors pb.SeedLegacyTablesCmd.
type SeedLegacyTables struct {
	AtBlock arbiter.L2BlockRef
	Tables  []arbiter.LegacyTable
	// IndexerID names the SI indexer whose Legacy tables the seed records
	// (housegate spec 2026-10-10 §6.3). Nil is the founding indexer's seed,
	// the only form before the signed-claims activation; &0 is indexer 0.
	IndexerID *uint64
}

// AddTable mirrors pb.AddTableCmd.
type AddTable struct {
	DatabaseID, TableID string
	Created, Schema     arbiter.L2EventRef
	SchemaVersion       uint32
	SchemaHash          string
	SchemaJSON          string
	// OwnerIndexerID is the SI indexer hosting the database at the
	// TableCreated block (spec D4): nil before the signed-claims activation,
	// required after it.
	OwnerIndexerID *uint64
}

// cloneUint64 copies an optional value; nil stays nil. Converters read the
// generated pointer field, never its getter, which answers 0 for absent.
func cloneUint64(p *uint64) *uint64 {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
```

In `wire/command.go` replace the `SeedLegacyTables` and `AddTable` encode blocks (lines 250-261):

```go
	if c.SeedLegacyTables != nil {
		set++
		out.Cmd = &pb.RaftCommand_SeedLegacyTables{SeedLegacyTables: &pb.SeedLegacyTablesCmd{
			AtBlock: L2BlockRefToPB(&c.SeedLegacyTables.AtBlock), Tables: legacyTablesToPB(c.SeedLegacyTables.Tables),
			IndexerId: cloneUint64(c.SeedLegacyTables.IndexerID)}}
	}
	if c.AddTable != nil {
		set++
		a := c.AddTable
		out.Cmd = &pb.RaftCommand_AddTable{AddTable: &pb.AddTableCmd{DatabaseId: a.DatabaseID, TableId: a.TableID,
			Created: L2EventRefToPB(&a.Created), Schema: L2EventRefToPB(&a.Schema), SchemaVersion: a.SchemaVersion,
			SchemaHash: a.SchemaHash, SchemaJson: a.SchemaJSON, OwnerIndexerId: cloneUint64(a.OwnerIndexerID)}}
	}
```

and the two decode cases (lines 334-341):

```go
	case *pb.RaftCommand_SeedLegacyTables:
		m := cmd.SeedLegacyTables
		return Command{SeedLegacyTables: &SeedLegacyTables{AtBlock: l2BlockRefValue(m.GetAtBlock()),
			Tables: legacyTablesFromPB(m.GetTables()), IndexerID: cloneUint64(m.IndexerId)}}, nil
	case *pb.RaftCommand_AddTable:
		m := cmd.AddTable
		return Command{AddTable: &AddTable{DatabaseID: m.GetDatabaseId(), TableID: m.GetTableId(),
			Created: l2EventRefValue(m.GetCreated()), Schema: l2EventRefValue(m.GetSchema()),
			SchemaVersion: m.GetSchemaVersion(), SchemaHash: m.GetSchemaHash(), SchemaJSON: m.GetSchemaJson(),
			OwnerIndexerID: cloneUint64(m.OwnerIndexerId)}}, nil
```

In `wire/table_registry_snapshot.go` add `"slices"` to the imports; append to `TableIncarnation` (after `PurgedBy`, line 78):

```go
	// OwnerIndexerID is the SI indexer owning the incarnation (housegate spec
	// 2026-10-10 D4); nil for one recorded before the signed-claims
	// activation, whose owner is the founding indexer (see Owner).
	OwnerIndexerID *uint64
```

append to `TableRegistrySnapshot` (after `ClientLanes`, line 113):

```go
	// SIIndexers is the committed si_indexers list sorted by IndexerID; nil
	// until the signed-claims activation.
	SIIndexers []arbiter.SIIndexerEntry
	// SeededIndexers lists, ascending, the SI indexers whose Legacy seed is
	// recorded; nil before the activation (Seeded keeps the founding flag).
	SeededIndexers []uint64
```

add after `ActiveTables` (line 145):

```go
// SignedClaimsActive reports whether the snapshot carries si_indexers, i.e.
// the signed-claims activation is committed (housegate spec 2026-10-10 §6.7).
func (s TableRegistrySnapshot) SignedClaimsActive() bool { return len(s.SIIndexers) > 0 }

// SIIndexer returns indexerID's enrolled entry.
func (s TableRegistrySnapshot) SIIndexer(indexerID uint64) (arbiter.SIIndexerEntry, bool) {
	for _, e := range s.SIIndexers {
		if e.IndexerID == indexerID {
			return e, true
		}
	}
	return arbiter.SIIndexerEntry{}, false
}

// Owner returns the SI indexer owning inc: its recorded OwnerIndexerID, or,
// for an incarnation recorded before the signed-claims activation (which
// names no owner), the founding indexer Params.SIIndexerID.
func (s TableRegistrySnapshot) Owner(inc TableIncarnation) uint64 {
	if inc.OwnerIndexerID != nil {
		return *inc.OwnerIndexerID
	}
	return s.Params.SIIndexerID
}

// signedClaimsFromPB decodes si_indexers and seeded_indexers. The arbiter
// keeps entries sorted and every seeded id enrolled; anything else is a
// corrupt view, refused like an unknown status.
func signedClaimsFromPB(m *pb.TableRegistrySnapshot) ([]arbiter.SIIndexerEntry, []uint64, error) {
	entries := SIIndexerEntriesFromPB(m.GetSiIndexers())
	for i, e := range entries {
		if err := e.Validate(); err != nil {
			return nil, nil, fmt.Errorf("table registry snapshot: %w", err)
		}
		if i > 0 && entries[i-1].IndexerID >= e.IndexerID {
			return nil, nil, fmt.Errorf("table registry snapshot: si_indexers must be sorted by indexer_id without duplicates")
		}
	}
	seeded := mapSlice(m.GetSeededIndexers(), func(id uint64) uint64 { return id })
	for i, id := range seeded {
		if i > 0 && seeded[i-1] >= id {
			return nil, nil, fmt.Errorf("table registry snapshot: seeded_indexers must be ascending without duplicates")
		}
		if !slices.ContainsFunc(entries, func(e arbiter.SIIndexerEntry) bool { return e.IndexerID == id }) {
			return nil, nil, fmt.Errorf("table registry snapshot: seeded indexer %d has no si_indexers entry", id)
		}
	}
	return entries, seeded, nil
}
```

In `TableRegistrySnapshotFromPB` insert after the client-lanes block (line 166):

```go
	entries, seeded, err := signedClaimsFromPB(m)
	if err != nil {
		return TableRegistrySnapshot{}, err
	}
	out.SIIndexers, out.SeededIndexers = entries, seeded
```

at the top of the incarnation loop body (after the seq check, line 170):

```go
		owner := cloneUint64(inc.OwnerIndexerId)
		if owner != nil {
			if _, ok := out.SIIndexer(*owner); !ok {
				return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d is owned by indexer %d, which has no si_indexers entry", inc.GetSeq(), *owner)
			}
		}
```

and `OwnerIndexerID: owner,` to the `TableIncarnation` literal (after `PurgedBy`, line 193). In `TableRegistrySnapshotToPB` add `SiIndexers: SIIndexerEntriesToPB(s.SIIndexers), SeededIndexers: mapSlice(s.SeededIndexers, func(id uint64) uint64 { return id }),` to the snapshot literal (after line 207) and `OwnerIndexerId: cloneUint64(inc.OwnerIndexerID),` to the incarnation literal (after line 218).

- [ ] **Step 11: Run the full suite and commit**

Run: `cd "$W" && bazel build //... && bazel test //... --test_output=errors`
Expected: every target PASSES (ClickHouse-gated tests SKIP); `TestPreSignedClaimsCommandBytesAreFrozen`, `TestLegacyCommandBytesAreFrozen` and `TestTableRegistrySnapshotRoundTrip` unchanged.

```bash
cd "$W" && git add wire && git commit -m "feat(wire): si_indexers, verifiers, owners and the seeded set on the wire

List converters for SIIndexerEntry/VerifierEntry (update and mutable
params), registration_seq inside NodeRegistration, optional AddTable owner
and seed indexer (nil and &0 stay distinct), and the registry snapshot's
si_indexers, seeded_indexers and incarnation owner with Owner/SIIndexer
helpers; an inconsistent signed-claims view is refused (housegate spec
2026-10-10 §6.1, §6.3).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 3: arbiter-core — SNode enrolment statement, signed SNode and verifier messages, authority-signed eviction, signature-bearing commands (S1)

**Files:**
- Modify: `consensus.go` (new `EvictNodeCommand`, the canonical signing form; the command types live in package `arbiter` like `PromoteSafePartition` / `UnsafeCleanup`, `types.go:87-105`, while their hashes live in `authority`, `authority/payload.go:52-75`)
- Create: `authority/snode_messages.go` (enrolment statement, SNode and verifier messages), `authority/evict_node.go`
- Modify: `authority/context.go:52-60` (`signWithContext` delegates to a purpose-taking helper; behaviour unchanged)
- Create: `authority/snode_messages_test.go`, `authority/evict_node_test.go`
- Create: `authority/authoritytest/fixtures.go`, `authority/authoritytest/vectors.go`, `authority/authoritytest/vectors_test.go` (test-support package, the `dataplane/dastore/dastoretest` precedent)
- Modify: `wire/command.go:40-74` (command structs), `:136-197` and `:273-277` (encode), `:349-398` (decode); `wire/table_registry.go:44-48` (`RecordTablePurged`)
- Create: `wire/requests.go` (request converters), `wire/signed_claims.go` (`StripSignedClaims`), `wire/signed_claims_test.go`
- Modify: `conformance/arbiter_wire_test.go` (`EvictNodeCommand` mirror)

**Interfaces:**
- Consumes: Task 2 (`arbiter.NodeRegistration.RegistrationSeq`, `wire.SeedLegacyTables.IndexerID`, `wire.AddTable.OwnerIndexerID`, `preSignedClaimsGolden` in `wire/signed_claims_golden_test.go`); Task 1 transport fields (`pb.RegisterRCCmd.SourceJws`, `pb.RecordPromotionAckCmd.SourceJws`, `pb.RecordCleanupAckCmd.SourceJws`, `pb.RegisterNodeCmd.SignerJws` / `.Ed25519Signature`, `pb.MarkActiveCmd.RegistrationSeq` / `.SignerJws` / `.Ed25519Signature`, `pb.EvictNodeCmd.ExpectedRegistrationSeq` / `.AuthorityJws`, `pb.RecordTablePurgedCmd.SignerJws` / `.Ed25519Signature`, the request-only `pb.RCRecord.SourceJws`, `pb.PromotionAck.SourceJws`, `pb.CleanupAck.SourceJws`, `pb.NodeRegistration.SignerJws` / `.Ed25519Signature`, `pb.NodeRef.RegistrationSeq` / `.SignerJws` / `.Ed25519Signature`, `pb.EvictNodeRequest`).
- Produces (package `arbiter`): `type EvictNodeCommand struct{ NodeID string; ExpectedRegistrationSeq uint64; Reason string }` (JSON `node_id`, `expected_registration_seq`, `reason`).
- Produces (package `authority`):
  - `const SNodeEnrollmentPurpose = "arbiter-snode-enrollment-v1"`; `type SNodeEnrollmentStatement struct{ NetworkID, GenesisSnapshotID string; IndexerID uint64; SNodeNodeID string }` (JSON `network_id`, `genesis_snapshot_id`, `indexer_id`, `snode_node_id`); `func SNodeEnrollmentHash(stmt SNodeEnrollmentStatement) (string, error)` = `CanonicalDigest("arbiter-snode-enrollment-statement-v1", stmt)`; `func (s *Signer) SignSNodeEnrollment(stmt SNodeEnrollmentStatement) (string, error)`; `func (s *Signer) SignSNodeEnrollmentAt(stmt SNodeEnrollmentStatement, iat int64) (string, error)`; `func VerifySNodeEnrollment(stmt SNodeEnrollmentStatement, jws, signer string) error`
  - `const SNodeMessagePurpose = "arbiter-snode-message-v1"`; `type SNodeMessageKind string` with `SNodeMessageRegistration` (`registration`), `SNodeMessageMarkActive` (`mark_active`), `SNodeMessageResultClaim` (`result_claim`), `SNodeMessagePromotionAck` (`promotion_ack`), `SNodeMessageCleanupAck` (`cleanup_ack`), `SNodeMessageTablePurged` (`table_purged`)
  - `type MessageContext struct{ NetworkID, GenesisSnapshotID string }` (JSON `network_id`, `genesis_snapshot_id`); `type MarkActiveBody struct{ NodeID string; RegistrationSeq uint64 }` (JSON `node_id`, `registration_seq`); `type TablePurgedBody struct{ NodeID string; IncarnationSeq uint64 }` (JSON `node_id`, `incarnation_seq`)
  - `func SNodeMessageHash(kind SNodeMessageKind, ctx MessageContext, body any) (string, error)` = `CanonicalDigest("arbiter-snode-message-body-v1", {kind, context, body})`; kind → body type: `registration` → `arbiter.NodeRegistration`, `mark_active` → `MarkActiveBody`, `result_claim` → `arbiter.RCRecord`, `promotion_ack` → `arbiter.PromotionAck`, `cleanup_ack` → `arbiter.CleanupAck`, `table_purged` → `TablePurgedBody` (values, not pointers); empty repeated fields hash as nil
  - `func (s *Signer) SignSNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any) (string, error)`; `func (s *Signer) SignSNodeMessageAt(kind SNodeMessageKind, ctx MessageContext, body any, iat int64) (string, error)`; `func VerifySNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any, jws, signer string) error`
  - `type VerifierMessageKind string` with `VerifierMessageRegistration`, `VerifierMessageMarkActive`, `VerifierMessageTablePurged`; `func VerifierMessageHash(kind VerifierMessageKind, ctx MessageContext, body any) (string, error)` (domain `arbiter-verifier-message-body-v1`); `func SignVerifierMessage(priv ed25519.PrivateKey, kind VerifierMessageKind, ctx MessageContext, body any) (string, error)` (lowercase hex ed25519 over the hash string's bytes, the `ByteSideScanMsg` convention); `func VerifyVerifierMessage(pub ed25519.PublicKey, kind VerifierMessageKind, ctx MessageContext, body any, sigHex string) error`
  - `const EvictNodePurpose = "arbiter-evict-node-v1"`; `func EvictNodeHash(cmd arbiter.EvictNodeCommand) (string, error)` (domain `arbiter-evict-node-command-v1`); `func (s *Signer) SignEvictNodeWithContext(cmd arbiter.EvictNodeCommand, ctx ConsensusContext) (string, error)`; `func (s *Signer) SignEvictNodeWithContextAt(cmd arbiter.EvictNodeCommand, ctx ConsensusContext, iat int64) (string, error)`; `func (v *Validator) VerifyEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error)` (deterministic, no clock; the token must carry exactly `ctx`); `func (v *Validator) AuthorizeEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error)` (plus token age, API boundary only)
- Produces (package `authority/authoritytest`, import `github.com/sentioxyz/arbiter-core/authority/authoritytest`; names used verbatim by arbiter Tasks 5–12): `IndexerKeyHex0`, `IndexerAddr0`, `IndexerKeyHex1`, `IndexerAddr1`, `AuthorityKeyHex`, `AuthorityAddr`, `StrangerKeyHex`, `StrangerAddr`, `NetworkID` (`devnet2`), `GenesisSnapshotID` (`0xgenesis`), `Iat`, `RegistrationSeq`, `SNodeNodeID0` (`snode-1`), `SNodeNodeID1` (`snode-2`), `ActivationBlock0`; `func MustSigner(t testing.TB, keyHex string) *authority.Signer`; `type SNodeEnrollmentVector struct{ Statement authority.SNodeEnrollmentStatement; KeyHex, Signer string; Iat int64; Hash, JWS string }`; `var SNodeEnrollmentVectors []SNodeEnrollmentVector` (index = indexer id 0, 1); `func Context() authority.MessageContext`; `func ConsensusContext(epoch uint64) authority.ConsensusContext`; `func VerifierNodeID(i int) string`; `func VerifierKey(i int) ed25519.PrivateKey`; `func VerifierEntries() []arbiter.VerifierEntry`; `func SIIndexerEntry0() arbiter.SIIndexerEntry`; `func SIIndexerEntry1(activationBlock uint64) arbiter.SIIndexerEntry`; `func ActivationUpdate() arbiter.ConsensusParamsUpdate`; body fixtures `SNodeRegistration`, `SNodeMarkActive`, `ResultClaim`, `PromotionAck`, `CleanupAck`, `SNodeTablePurged`, `VerifierRegistration`, `VerifierMarkActive`, `VerifierTablePurged`, `EvictCommand`; and the pinned vectors (`SNodeRegistrationJWS` … `ActivationUpdateHash`, listed in Step 1).
- Produces (package `wire`):
  - command fields: `RegisterRC.SourceJWS`, `RecordPromotionAck.SourceJWS`, `RecordCleanupAck.SourceJWS`, `RegisterNode.SignerJWS` / `.Ed25519Signature`, `MarkActive.RegistrationSeq` / `.SignerJWS` / `.Ed25519Signature`, `EvictNode.ExpectedRegistrationSeq` / `.AuthorityJWS`, `RecordTablePurged.SignerJWS` / `.Ed25519Signature`; `func (c EvictNode) Canonical() arbiter.EvictNodeCommand`
  - request converters: `RegisterRCFromRequest(*pb.RCRecord) RegisterRC`, `RegisterRCToRequest(RegisterRC) *pb.RCRecord`, `RecordPromotionAckFromRequest(*pb.PromotionAck) RecordPromotionAck`, `RecordPromotionAckToRequest(RecordPromotionAck) *pb.PromotionAck`, `RecordCleanupAckFromRequest(*pb.CleanupAck) RecordCleanupAck`, `RecordCleanupAckToRequest(RecordCleanupAck) *pb.CleanupAck`, `RegisterNodeFromRequest(*pb.NodeRegistration) RegisterNode`, `RegisterNodeToRequest(c RegisterNode, features []string) *pb.NodeRegistration`, `MarkActiveFromRequest(*pb.NodeRef) MarkActive`, `MarkActiveToRequest(MarkActive) *pb.NodeRef`, `RecordTablePurgedFromRequest(*pb.RecordTablePurgedCmd) RecordTablePurged`, `RecordTablePurgedToRequest(RecordTablePurged) *pb.RecordTablePurgedCmd`, `EvictNodeFromRequest(*pb.EvictNodeRequest) EvictNode`, `EvictNodeToRequest(EvictNode) *pb.EvictNodeRequest`
  - `func StripSignedClaims(c Command) Command` — the pre-activation proposal filter (CONTRACT §0); `UpdateConsensusParams` passes unchanged
  - `Decode` refuses a signature in the request-only copy (`RegisterRCCmd.rc.source_jws`, `RecordPromotionAckCmd.ack.source_jws`, `RecordCleanupAckCmd.ack.source_jws`, `RegisterNodeCmd.registration.signer_jws` / `ed25519_signature`), with an error containing `request-only`

**How deterministic verification works (no new JWS format).** Every ES256K token here is the existing `JWSCommandPayload` (`authority/payload.go:40-50`) produced by `(*Signer).signPayload` (`authority/signer.go:78-94`): header `{"alg":"ES256K","typ":"JWT"}`, payload `{"iat":…,"purpose":…,"cmd_hash":…}` (plus the three context fields for eviction, `authority/context.go:52-60`), keccak256 over `header.payload`, recovery byte V+27. `VerifySNodeEnrollment` / `VerifySNodeMessage` recompute the hash and hand the token to the existing `Validator.verify` (`authority/validator.go:56-126`) with a one-address allowlist `{lowercase(signer)}` and `enforceAge=false`: purpose and `cmd_hash` are compared, the age block (`validator.go:95-103`) is skipped, and `crypto.SigToPub` over `keccak256(header.payload)` recovers the signer, which must be the allowlisted address. Nothing reads a clock, so Raft Apply and snapshot restore get one answer on every voter. ES256K (RFC 6979) and ed25519 (RFC 8032) signing are deterministic, so with a fixed `iat` each token is a single exact string: the vectors below were computed independently of Go (canonical JSON preimage + `shasum`, `cast keccak` / `cast wallet sign --no-hash`, `openssl pkeyutl -rawin` for ed25519; the same pipeline reproduces housegate's `statement_jws_v2.json` signature and RFC 8032 test 2) and the implementation must reproduce them.

- [ ] **Step 1: Write the failing authority tests and the `authoritytest` fixtures**

Create `authority/authoritytest/fixtures.go`:

```go
// Package authoritytest holds deterministic signed-claims fixtures for the
// multi-source storage-integrity network (housegate spec 2026-10-10 §6.2,
// §6.5): throwaway keys (never provision them), one message context, one
// issue time, one body per message kind, and the exact hashes, tokens and
// signatures arbiter-core produces for them. arbiter's FSM and server tests
// import it so both repositories sign and verify the same bytes. The vector
// constants are consensus constants: never regenerate them.
package authoritytest

import (
	"crypto/ed25519"
	"fmt"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
)

const (
	// NetworkID and GenesisSnapshotID form the fixture MessageContext.
	NetworkID         = "devnet2"
	GenesisSnapshotID = "0xgenesis"
	// Iat is every fixture token's issue time (2025-10-10T00:00:00Z).
	Iat int64 = 1760054400
	// RegistrationSeq is Iat in milliseconds: the clock floor a data-plane
	// node reserves when its clock reads Iat.
	RegistrationSeq uint64 = 1760054400000
	// ActivationBlock0 is the founding indexer's activation block.
	ActivationBlock0 uint64 = 5508931

	AuthorityKeyHex = "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"
	AuthorityAddr   = "0x970e8128ab834e8eac17ab8e3812f010678cf791"
	IndexerKeyHex0  = "0000000000000000000000000000000000000000000000000000000000000a11"
	IndexerAddr0    = "0x563bd9e11d18b6ea60c2f159f8d3062d30e8039e"
	IndexerKeyHex1  = "0000000000000000000000000000000000000000000000000000000000000b22"
	IndexerAddr1    = "0x89fd7c610ac4aa2e17c3b15a2c386a4b215f96d9"
	StrangerKeyHex  = "0000000000000000000000000000000000000000000000000000000000000c33"
	StrangerAddr    = "0x009cfebab1cc20d08e23eb7ea89dcefab3349e45"

	// SNodeNodeID0 and SNodeNodeID1 are the SNodes of indexers 0 and 1.
	SNodeNodeID0 = "snode-1"
	SNodeNodeID1 = "snode-2"
)

// MustSigner loads a fixture key.
func MustSigner(t testing.TB, keyHex string) *authority.Signer {
	t.Helper()
	s, err := authority.NewSignerFromHex(keyHex)
	if err != nil {
		t.Fatalf("authoritytest: load key: %v", err)
	}
	return s
}

// Context is the fixture message context.
func Context() authority.MessageContext {
	return authority.MessageContext{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID}
}

// ConsensusContext is the fixture authority context at epoch.
func ConsensusContext(epoch uint64) authority.ConsensusContext {
	return authority.ConsensusContext{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, AuthorityEpoch: epoch}
}

// VerifierNodeID names fixture verifier i (1-based).
func VerifierNodeID(i int) string { return fmt.Sprintf("verifier-%d", i) }

// VerifierKey is fixture verifier i's ed25519 key: seed byte 0 is 6+i.
func VerifierKey(i int) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = byte(6 + i)
	return ed25519.NewKeyFromSeed(seed)
}

// VerifierEntries lists verifiers 1-3, sorted by node id.
func VerifierEntries() []arbiter.VerifierEntry {
	out := make([]arbiter.VerifierEntry, 0, 3)
	for i := 1; i <= 3; i++ {
		out = append(out, arbiter.VerifierEntry{NodeID: VerifierNodeID(i), Ed25519Pubkey: VerifierKey(i).Public().(ed25519.PublicKey)})
	}
	return out
}

// SIIndexerEntry0 is the founding indexer restated as si_indexers entry 0.
func SIIndexerEntry0() arbiter.SIIndexerEntry {
	return arbiter.SIIndexerEntry{IndexerID: 0, ActivationBlock: ActivationBlock0, Signer: IndexerAddr0,
		SNodeNodeID: SNodeNodeID0, EnrollmentJWS: SNodeEnrollmentVectors[0].JWS}
}

// SIIndexerEntry1 enrols indexer 1 at activationBlock (its enrolment
// statement does not cover the block).
func SIIndexerEntry1(activationBlock uint64) arbiter.SIIndexerEntry {
	return arbiter.SIIndexerEntry{IndexerID: 1, ActivationBlock: activationBlock, Signer: IndexerAddr1,
		SNodeNodeID: SNodeNodeID1, EnrollmentJWS: SNodeEnrollmentVectors[1].JWS}
}

// ActivationUpdate is a signed-claims activation update in normalized form.
func ActivationUpdate() arbiter.ConsensusParamsUpdate {
	return arbiter.ConsensusParamsUpdate{
		NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, ExpectedEpoch: 2, PreviousParamsDigest: "0xdigest",
		AuthorityAddresses: []string{AuthorityAddr}, MaxWriters: 1, ExpectedPromotionSeq: 3,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1",
			SIIndexerID: 0, ActivationBlock: ActivationBlock0, Confirmation: arbiter.TableRegistryConfirmationSafe},
		ClientLanes: &arbiter.ClientLaneParams{MaxLanesPerAccount: 256},
		SIIndexers:  []arbiter.SIIndexerEntry{SIIndexerEntry0()},
		Verifiers:   VerifierEntries(),
	}
}

// SNodeRegistration is indexer 1's SNode registration at RegistrationSeq.
func SNodeRegistration() arbiter.NodeRegistration {
	return arbiter.NodeRegistration{NodeID: SNodeNodeID1, Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: RegistrationSeq}
}

// SNodeMarkActive is the activation following SNodeRegistration.
func SNodeMarkActive() authority.MarkActiveBody {
	return authority.MarkActiveBody{NodeID: SNodeNodeID1, RegistrationSeq: RegistrationSeq}
}

// ResultClaim is a one-part RC of indexer 1's SNode.
func ResultClaim() arbiter.RCRecord {
	return arbiter.RCRecord{
		StatementID: arbiter.StatementID{ClientAccount: "0x00000000000000000000000000000000000000a1", ClientSeq: 42, ClientNonce: "9f1c"},
		SourceNode:  SNodeNodeID1,
		CandidateParts: []arbiter.CandidatePart{{TableID: "db1.t", PartitionID: "all", PartName: "all_1_1_0",
			PartRowLtHash: "0xaa", PartPhysHash: "0xbb", RowCount: 3, Bytes: 512}},
		SourceClaimRoot:      "0xroot",
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: "db1.t", PartitionID: "all", NewPartsLtHashSum: "0xaa"}},
	}
}

// PromotionAck is an applied acknowledgement of indexer 1's SNode.
func PromotionAck() arbiter.PromotionAck {
	return arbiter.PromotionAck{NodeID: SNodeNodeID1, PromotionSeq: 7, TableID: "db1.t", PartitionID: "all",
		PostPartitionCommitment: "0xpost", Applied: true,
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xaa", SafePartName: "all_7_7_0", PartPhysHash: "0xbb"}}}
}

// CleanupAck is the cleanup acknowledgement of the same promotion.
func CleanupAck() arbiter.CleanupAck {
	return arbiter.CleanupAck{NodeID: SNodeNodeID1, PromotionSeq: 7, TableID: "db1.t", PartitionID: "all"}
}

// SNodeTablePurged is indexer 1's SNode reporting incarnation 5 purged.
func SNodeTablePurged() authority.TablePurgedBody {
	return authority.TablePurgedBody{NodeID: SNodeNodeID1, IncarnationSeq: 5}
}

// VerifierRegistration is verifier 1's registration at RegistrationSeq.
func VerifierRegistration() arbiter.NodeRegistration {
	return arbiter.NodeRegistration{NodeID: VerifierNodeID(1), Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier},
		Ed25519Pubkey: VerifierKey(1).Public().(ed25519.PublicKey), RegistrationSeq: RegistrationSeq}
}

// VerifierMarkActive is the activation following VerifierRegistration.
func VerifierMarkActive() authority.MarkActiveBody {
	return authority.MarkActiveBody{NodeID: VerifierNodeID(1), RegistrationSeq: RegistrationSeq}
}

// VerifierTablePurged is verifier 1 reporting incarnation 5 purged.
func VerifierTablePurged() authority.TablePurgedBody {
	return authority.TablePurgedBody{NodeID: VerifierNodeID(1), IncarnationSeq: 5}
}

// EvictCommand evicts indexer 1's SNode registered at RegistrationSeq.
func EvictCommand() arbiter.EvictNodeCommand {
	return arbiter.EvictNodeCommand{NodeID: SNodeNodeID1, ExpectedRegistrationSeq: RegistrationSeq, Reason: "key compromised"}
}
```

Create `authority/authoritytest/vectors.go`:

```go
package authoritytest

import "github.com/sentioxyz/arbiter-core/authority"

// SNodeEnrollmentVector is one pinned enrolment statement and the exact token
// its indexer key signs at Iat.
type SNodeEnrollmentVector struct {
	Statement authority.SNodeEnrollmentStatement
	KeyHex    string // the indexer's signer key
	Signer    string // lowercase address of KeyHex
	Iat       int64
	Hash      string // authority.SNodeEnrollmentHash(Statement)
	JWS       string // (*authority.Signer).SignSNodeEnrollmentAt(Statement, Iat)
}

// SNodeEnrollmentVectors pins the enrolment of indexer 0 (snode-1) and
// indexer 1 (snode-2) in the fixture context; the index is the indexer id.
// Read-only.
var SNodeEnrollmentVectors = []SNodeEnrollmentVector{
	{
		Statement: authority.SNodeEnrollmentStatement{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, IndexerID: 0, SNodeNodeID: SNodeNodeID0},
		KeyHex:    IndexerKeyHex0, Signer: IndexerAddr0, Iat: Iat,
		Hash: "0x7a1d3da069ee39c5e027b470d948820a6d55f67cc5c1bfcc98ff2720fc16f72c",
		JWS:  "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLWVucm9sbG1lbnQtdjEiLCJjbWRfaGFzaCI6IjB4N2ExZDNkYTA2OWVlMzljNWUwMjdiNDcwZDk0ODgyMGE2ZDU1ZjY3Y2M1YzFiZmNjOThmZjI3MjBmYzE2ZjcyYyJ9.N2WHVC8OWAhSbNwIP8MMDWm494vmCGQHI_dJwGf6BC8G991JtMnUCqSu3MtsuYwtnYtfBDVImzhlZEjvw4KjHBs",
	},
	{
		Statement: authority.SNodeEnrollmentStatement{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, IndexerID: 1, SNodeNodeID: SNodeNodeID1},
		KeyHex:    IndexerKeyHex1, Signer: IndexerAddr1, Iat: Iat,
		Hash: "0x8098ef999e50781463741c7cdfab3823f687621d71b60a26cf925b717a73b1a4",
		JWS:  "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLWVucm9sbG1lbnQtdjEiLCJjbWRfaGFzaCI6IjB4ODA5OGVmOTk5ZTUwNzgxNDYzNzQxYzdjZGZhYjM4MjNmNjg3NjIxZDcxYjYwYTI2Y2Y5MjViNzE3YTczYjFhNCJ9.FmhPQIellYlN6Lmx6kgf2glCEXXZKGzXpSmxRpg1ViJx3LDAEY4htCLM63nSfd0JioZJpMdKSImACt0SPPU7Gxs",
	},
}

// Pinned message vectors in Context() at Iat. SNode tokens are signed by
// IndexerKeyHex1, verifier signatures by VerifierKey(1), the eviction by
// AuthorityKeyHex in ConsensusContext(2). Example preimages (canonical JSON,
// hashed as sha256("housegate-replay-mvp-v0:" + domain + "\x00" + json)):
//
//	arbiter-snode-message-body-v1 / registration:
//	{"kind":"registration","context":{"network_id":"devnet2","genesis_snapshot_id":"0xgenesis"},"body":{"node_id":"snode-2","roles":[2],"ed25519_pubkey":null,"registration_seq":1760054400000}}
//	arbiter-evict-node-command-v1:
//	{"node_id":"snode-2","expected_registration_seq":1760054400000,"reason":"key compromised"}
const (
	SNodeRegistrationHash = "0xa80e9679bf23e8ce71e78b62c4f5f99b381209130fc8f7b16fb23c700e126a8a"
	SNodeRegistrationJWS  = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4YTgwZTk2NzliZjIzZThjZTcxZTc4YjYyYzRmNWY5OWIzODEyMDkxMzBmYzhmN2IxNmZiMjNjNzAwZTEyNmE4YSJ9.sS370ECHzchEzoRQqfBHUtPcanBs9GGM17DincxZDm0owV0YQe1ypKZaZzLksK9j1ABm4m3pCP0qOzQDfB9e7Rs"
	SNodeMarkActiveHash   = "0x495bf58dfde1beb415989b651a850dbb72591aa786956c6abf5c3e0513ec8cce"
	SNodeMarkActiveJWS    = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4NDk1YmY1OGRmZGUxYmViNDE1OTg5YjY1MWE4NTBkYmI3MjU5MWFhNzg2OTU2YzZhYmY1YzNlMDUxM2VjOGNjZSJ9.tEUyTjbye9FazkI8s89YLhIlIlltYP9r4OwiC8f4Jmkz5M5LwlDLybr21ofo7uU81b338STn9TfUkue6r1vvQxs"
	ResultClaimHash       = "0x5f8db74da70ea02661866db4a80d60abad909270e9f4a6a7d8776539d2c62a3d"
	ResultClaimJWS        = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4NWY4ZGI3NGRhNzBlYTAyNjYxODY2ZGI0YTgwZDYwYWJhZDkwOTI3MGU5ZjRhNmE3ZDg3NzY1MzlkMmM2MmEzZCJ9.wZpc1F7itQGVU-4bpujTt-7MjW_YUUE4fmxjnQQaj8NpNjY4tXqpP3brvXxKBCpVPd7HE3dJdprEImSuOFB0_Bs"
	PromotionAckHash      = "0xbbe81abcea1d1fbdaffff0dd6bb0219ff8106978f834111909e86faed5db89a4"
	PromotionAckJWS       = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4YmJlODFhYmNlYTFkMWZiZGFmZmZmMGRkNmJiMDIxOWZmODEwNjk3OGY4MzQxMTE5MDllODZmYWVkNWRiODlhNCJ9.55r2DfkpLQIwi52thDEzqKJmQacWKCCz9Sb3KWLF28ANvXwYjy6jJ46Pr3-pSgmXRPD-NtTfVBiY_X-hhl3OLBw"
	CleanupAckHash        = "0x76511e207b60f6bf118347a3c785f2ed26628febf4c6d4a01820a0d90d25f1f0"
	CleanupAckJWS         = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4NzY1MTFlMjA3YjYwZjZiZjExODM0N2EzYzc4NWYyZWQyNjYyOGZlYmY0YzZkNGEwMTgyMGEwZDkwZDI1ZjFmMCJ9.jmjzHHNpG7ccoLo__C-se3uWy1mrLDSK-njulYFwIgt_dS-_FfdqfQN12BUGyJP68bz1oNmeU4hn1EA9wV1wXRw"
	SNodeTablePurgedHash  = "0xbd37aff0b5f31faf0b861cd086dff802d5e2cad0e1bbe65885b14adfeebf056e"
	SNodeTablePurgedJWS   = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXNub2RlLW1lc3NhZ2UtdjEiLCJjbWRfaGFzaCI6IjB4YmQzN2FmZjBiNWYzMWZhZjBiODYxY2QwODZkZmY4MDJkNWUyY2FkMGUxYmJlNjU4ODViMTRhZGZlZWJmMDU2ZSJ9.Mr9RWbG8Lng0Mm5tnOAKsWQ7wzFM6e--3TWiphDj_rRAL0S-ArFjEkz5l1KKzsJ8iVMArJmFDleo3SuBq6ARYRs"

	VerifierRegistrationHash      = "0xeeb0507360da71f21586f2b95518264c339d9dcd5ceb25ccba9591ca73949596"
	VerifierRegistrationSignature = "39768c731608bb017cdb6837fa87c983280b3b49566b07723939a6825aa60cf80c9bfbbc7c1ae1521240775cebe6538c8ea84d17644d25d776c04c3be7de780f"
	VerifierMarkActiveHash        = "0xec58905ad4220979fb531e650abb2bc55d583083501dc61ae66f676fb049052d"
	VerifierMarkActiveSignature   = "063de5cace918a78f19d597a2bd819e89710f1ef205048a26d943e0f458c039552abecdfad265eeaeb72a6d543111a3582ea820ef457febeb353f8d20a170100"
	VerifierTablePurgedHash       = "0xe3999386c18ac0dfa54a90c6015c68d77c17e1c7f607b0a33bcf97b61133a975"
	VerifierTablePurgedSignature  = "8b8fe8da362f1cec849e1b3304aa1ffe4d11917d9c0501df8720a1b76a315d2bb49a887f60d57b71261e3f44e225c2d979ddc17623362a3ec877c47c288df001"

	EvictHash = "0xb1ccb963e614b551e33fcc03ea92b591c9dbc2ef2cbc2782eb6b30dca1c17fc9"
	EvictJWS  = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLWV2aWN0LW5vZGUtdjEiLCJjbWRfaGFzaCI6IjB4YjFjY2I5NjNlNjE0YjU1MWUzM2ZjYzAzZWE5MmI1OTFjOWRiYzJlZjJjYmMyNzgyZWI2YjMwZGNhMWMxN2ZjOSIsIm5ldHdvcmtfaWQiOiJkZXZuZXQyIiwiZ2VuZXNpc19zbmFwc2hvdF9pZCI6IjB4Z2VuZXNpcyIsImF1dGhvcml0eV9lcG9jaCI6Mn0.XXSz5PeptJCrIyPgtv3TNOfoJ55k1KumHZa-BNJI6_olrIjNQ3jMFlhr2CiBtp5AcdmzjbiC1rscwamEhx2SOBw"

	// ActivationUpdateHash is authority.ConsensusParamsUpdateHash(ActivationUpdate()).
	ActivationUpdateHash = "0xf3a4e05aa1109b05ef7e9b9b1d0775c8f529699965f6d8578c7155a7c94a098d"

	// Verifier1PubkeyHex..Verifier3PubkeyHex are VerifierKey(1..3)'s public keys.
	Verifier1PubkeyHex = "a2fa2f4a355ba2e907a53009e9e37caddf7ac7e66a08ba07631f553072b3f24c"
	Verifier2PubkeyHex = "d4c5061b81c4682b27a0cfc6459cd9d7892eb60a43f73dd1060b6c478aa7c3d8"
	Verifier3PubkeyHex = "bb5c672482b0dcca91a21a4ed63b15afde8aa1378da72cd01b349589d6e7dd6a"
)
```

Create `authority/authoritytest/vectors_test.go`:

```go
package authoritytest_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
)

func TestFixtureKeysMatchTheirAddresses(t *testing.T) {
	for key, want := range map[string]string{
		authoritytest.AuthorityKeyHex: authoritytest.AuthorityAddr,
		authoritytest.IndexerKeyHex0:  authoritytest.IndexerAddr0,
		authoritytest.IndexerKeyHex1:  authoritytest.IndexerAddr1,
		authoritytest.StrangerKeyHex:  authoritytest.StrangerAddr,
	} {
		if got := authoritytest.MustSigner(t, key).Address(); got != want {
			t.Fatalf("address of %s = %s, want %s", key, got, want)
		}
	}
	for i, want := range []string{authoritytest.Verifier1PubkeyHex, authoritytest.Verifier2PubkeyHex, authoritytest.Verifier3PubkeyHex} {
		if got := hex.EncodeToString(authoritytest.VerifierKey(i + 1).Public().(ed25519.PublicKey)); got != want {
			t.Fatalf("verifier %d public key = %s, want %s", i+1, got, want)
		}
	}
}

func TestSNodeEnrollmentVectors(t *testing.T) {
	for i, v := range authoritytest.SNodeEnrollmentVectors {
		if v.Statement.IndexerID != uint64(i) {
			t.Fatalf("vector %d is indexer %d", i, v.Statement.IndexerID)
		}
		if h, err := authority.SNodeEnrollmentHash(v.Statement); err != nil || h != v.Hash {
			t.Fatalf("indexer %d hash = %s, %v; want %s", i, h, err, v.Hash)
		}
		if got, err := authoritytest.MustSigner(t, v.KeyHex).SignSNodeEnrollmentAt(v.Statement, v.Iat); err != nil || got != v.JWS {
			t.Fatalf("indexer %d token = %s, %v; want the pinned token", i, got, err)
		}
		if err := authority.VerifySNodeEnrollment(v.Statement, v.JWS, v.Signer); err != nil {
			t.Fatalf("indexer %d: %v", i, err)
		}
		if err := authority.VerifySNodeEnrollment(v.Statement, v.JWS, authoritytest.StrangerAddr); err == nil {
			t.Fatalf("indexer %d verified under a stranger", i)
		}
	}
}

func TestSNodeMessageVectors(t *testing.T) {
	signer := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
	for _, tc := range []struct {
		kind      authority.SNodeMessageKind
		body      any
		hash, jws string
	}{
		{authority.SNodeMessageRegistration, authoritytest.SNodeRegistration(), authoritytest.SNodeRegistrationHash, authoritytest.SNodeRegistrationJWS},
		{authority.SNodeMessageMarkActive, authoritytest.SNodeMarkActive(), authoritytest.SNodeMarkActiveHash, authoritytest.SNodeMarkActiveJWS},
		{authority.SNodeMessageResultClaim, authoritytest.ResultClaim(), authoritytest.ResultClaimHash, authoritytest.ResultClaimJWS},
		{authority.SNodeMessagePromotionAck, authoritytest.PromotionAck(), authoritytest.PromotionAckHash, authoritytest.PromotionAckJWS},
		{authority.SNodeMessageCleanupAck, authoritytest.CleanupAck(), authoritytest.CleanupAckHash, authoritytest.CleanupAckJWS},
		{authority.SNodeMessageTablePurged, authoritytest.SNodeTablePurged(), authoritytest.SNodeTablePurgedHash, authoritytest.SNodeTablePurgedJWS},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			if h, err := authority.SNodeMessageHash(tc.kind, authoritytest.Context(), tc.body); err != nil || h != tc.hash {
				t.Fatalf("hash = %s, %v; want %s", h, err, tc.hash)
			}
			if got, err := signer.SignSNodeMessageAt(tc.kind, authoritytest.Context(), tc.body, authoritytest.Iat); err != nil || got != tc.jws {
				t.Fatalf("token = %s, %v; want the pinned token", got, err)
			}
			if err := authority.VerifySNodeMessage(tc.kind, authoritytest.Context(), tc.body, tc.jws, authoritytest.IndexerAddr1); err != nil {
				t.Fatal(err)
			}
			if err := authority.VerifySNodeMessage(tc.kind, authoritytest.Context(), tc.body, tc.jws, authoritytest.IndexerAddr0); err == nil {
				t.Fatal("verified under indexer 0's signer")
			}
		})
	}
}

func TestVerifierMessageVectors(t *testing.T) {
	priv := authoritytest.VerifierKey(1)
	pub := priv.Public().(ed25519.PublicKey)
	for _, tc := range []struct {
		kind      authority.VerifierMessageKind
		body      any
		hash, sig string
	}{
		{authority.VerifierMessageRegistration, authoritytest.VerifierRegistration(), authoritytest.VerifierRegistrationHash, authoritytest.VerifierRegistrationSignature},
		{authority.VerifierMessageMarkActive, authoritytest.VerifierMarkActive(), authoritytest.VerifierMarkActiveHash, authoritytest.VerifierMarkActiveSignature},
		{authority.VerifierMessageTablePurged, authoritytest.VerifierTablePurged(), authoritytest.VerifierTablePurgedHash, authoritytest.VerifierTablePurgedSignature},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			if h, err := authority.VerifierMessageHash(tc.kind, authoritytest.Context(), tc.body); err != nil || h != tc.hash {
				t.Fatalf("hash = %s, %v; want %s", h, err, tc.hash)
			}
			if got, err := authority.SignVerifierMessage(priv, tc.kind, authoritytest.Context(), tc.body); err != nil || got != tc.sig {
				t.Fatalf("signature = %s, %v; want the pinned signature", got, err)
			}
			if err := authority.VerifyVerifierMessage(pub, tc.kind, authoritytest.Context(), tc.body, tc.sig); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEvictNodeVector(t *testing.T) {
	if h, err := authority.EvictNodeHash(authoritytest.EvictCommand()); err != nil || h != authoritytest.EvictHash {
		t.Fatalf("hash = %s, %v", h, err)
	}
	s := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	if got, err := s.SignEvictNodeWithContextAt(authoritytest.EvictCommand(), authoritytest.ConsensusContext(2), authoritytest.Iat); err != nil || got != authoritytest.EvictJWS {
		t.Fatalf("token = %s, %v; want the pinned token", got, err)
	}
	v := &authority.Validator{AllowedAddresses: map[string]bool{authoritytest.AuthorityAddr: true}}
	if addr, err := v.VerifyEvictNode(authoritytest.EvictCommand(), authoritytest.EvictJWS, authoritytest.ConsensusContext(2)); err != nil || addr != authoritytest.AuthorityAddr {
		t.Fatalf("verify = %s, %v", addr, err)
	}
}

func TestActivationUpdateVector(t *testing.T) {
	u := authoritytest.ActivationUpdate()
	if h, err := authority.ConsensusParamsUpdateHash(u); err != nil || h != authoritytest.ActivationUpdateHash {
		t.Fatalf("hash = %s, %v; want %s", h, err, authoritytest.ActivationUpdateHash)
	}
	if got, err := authority.NormalizeConsensusParamsUpdate(u); err != nil || !reflect.DeepEqual(got, u) {
		t.Fatalf("ActivationUpdate is not in normalized form: %+v, %v", got, err)
	}
	e0, e1 := authoritytest.SIIndexerEntry0(), authoritytest.SIIndexerEntry1(6_000_000)
	if err := e0.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := e1.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, e := range []struct {
		stmt        authority.SNodeEnrollmentStatement
		jws, signer string
	}{{authoritytest.SNodeEnrollmentVectors[0].Statement, e0.EnrollmentJWS, e0.Signer}, {authoritytest.SNodeEnrollmentVectors[1].Statement, e1.EnrollmentJWS, e1.Signer}} {
		if err := authority.VerifySNodeEnrollment(e.stmt, e.jws, e.signer); err != nil {
			t.Fatal(err)
		}
	}
}
```

Create `authority/snode_messages_test.go` (internal package; reuses `newTestPair` from `authority/authority_test.go:27`):

```go
package authority

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
)

// Throwaway keys (never provision): an indexer signer and a stranger.
const (
	indexerTestKeyHex  = "0000000000000000000000000000000000000000000000000000000000000b22"
	strangerTestKeyHex = "0000000000000000000000000000000000000000000000000000000000000c33"
)

func testMessageContext() MessageContext {
	return MessageContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis"}
}

func mustTestSigner(t *testing.T, keyHex string) *Signer {
	t.Helper()
	s, err := NewSignerFromHex(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testRegistration() arbiter.NodeRegistration {
	return arbiter.NodeRegistration{NodeID: "snode-2", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 1760054400000}
}

func TestSNodeEnrollmentSignVerify(t *testing.T) {
	s := mustTestSigner(t, indexerTestKeyHex)
	stmt := SNodeEnrollmentStatement{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", IndexerID: 1, SNodeNodeID: "snode-2"}
	token, err := s.SignSNodeEnrollment(stmt)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySNodeEnrollment(stmt, token, "0x"+strings.ToUpper(s.Address()[2:])); err != nil {
		t.Fatalf("verify with a mixed-case signer: %v", err)
	}
	for name, mutate := range map[string]func(*SNodeEnrollmentStatement){
		"other network":       func(s *SNodeEnrollmentStatement) { s.NetworkID = "mainnet" },
		"other genesis":       func(s *SNodeEnrollmentStatement) { s.GenesisSnapshotID = "0xother" },
		"other indexer":       func(s *SNodeEnrollmentStatement) { s.IndexerID = 2 },
		"other snode node id": func(s *SNodeEnrollmentStatement) { s.SNodeNodeID = "snode-3" },
	} {
		t.Run(name, func(t *testing.T) {
			other := stmt
			mutate(&other)
			if err := VerifySNodeEnrollment(other, token, s.Address()); err == nil {
				t.Fatal("a statement signed for another identity verified")
			}
		})
	}
	if err := VerifySNodeEnrollment(stmt, token, mustTestSigner(t, strangerTestKeyHex).Address()); err == nil {
		t.Fatal("verified under a signer that did not sign")
	}
	for _, bad := range []string{"", "0x1234", "0x0000000000000000000000000000000000000000", "0xzz00000000000000000000000000000000000000"} {
		if err := VerifySNodeEnrollment(stmt, token, bad); err == nil {
			t.Fatalf("verified under malformed signer %q", bad)
		}
	}
	hash, err := SNodeEnrollmentHash(stmt)
	if err != nil {
		t.Fatal(err)
	}
	asMessage, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: SNodeMessagePurpose, CmdHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySNodeEnrollment(stmt, asMessage, s.Address()); err == nil {
		t.Fatal("a token of another purpose verified as an enrolment")
	}
	for _, missing := range []SNodeEnrollmentStatement{{GenesisSnapshotID: "g", SNodeNodeID: "s"}, {NetworkID: "n", SNodeNodeID: "s"}, {NetworkID: "n", GenesisSnapshotID: "g"}} {
		if _, err := s.SignSNodeEnrollment(missing); err == nil {
			t.Fatalf("signed an enrolment without its identity: %+v", missing)
		}
	}
}

// TestSignedMessageVerificationReadsNoClock is the FSM's verification path:
// VerifySNodeMessage hands the token to Validator.verify with a one-address
// allowlist and enforceAge=false, so the ES256K recovery alone decides,
// whatever the token's iat.
func TestSignedMessageVerificationReadsNoClock(t *testing.T) {
	s := mustTestSigner(t, indexerTestKeyHex)
	for _, iat := range []int64{1, time.Now().Add(24 * time.Hour).Unix(), time.Now().Add(-365 * 24 * time.Hour).Unix()} {
		token, err := s.SignSNodeMessageAt(SNodeMessageRegistration, testMessageContext(), testRegistration(), iat)
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifySNodeMessage(SNodeMessageRegistration, testMessageContext(), testRegistration(), token, s.Address()); err != nil {
			t.Fatalf("iat %d: %v", iat, err)
		}
	}
}

func TestSNodeMessageBindsKindContextBodyAndSigner(t *testing.T) {
	s := mustTestSigner(t, indexerTestKeyHex)
	ctx, reg := testMessageContext(), testRegistration()
	token, err := s.SignSNodeMessage(SNodeMessageRegistration, ctx, reg)
	if err != nil {
		t.Fatal(err)
	}
	replayed := reg
	replayed.RegistrationSeq--
	otherNetwork, otherGenesis := ctx, ctx
	otherNetwork.NetworkID = "mainnet"
	otherGenesis.GenesisSnapshotID = "0xother"
	stranger := mustTestSigner(t, strangerTestKeyHex).Address()
	for name, err := range map[string]error{
		"another registration_seq": VerifySNodeMessage(SNodeMessageRegistration, ctx, replayed, token, s.Address()),
		"another network":          VerifySNodeMessage(SNodeMessageRegistration, otherNetwork, reg, token, s.Address()),
		"another genesis":          VerifySNodeMessage(SNodeMessageRegistration, otherGenesis, reg, token, s.Address()),
		"another signer":           VerifySNodeMessage(SNodeMessageRegistration, ctx, reg, token, stranger),
		"another kind":             VerifySNodeMessage(SNodeMessageMarkActive, ctx, MarkActiveBody{NodeID: reg.NodeID, RegistrationSeq: reg.RegistrationSeq}, token, s.Address()),
		"tampered signature":       VerifySNodeMessage(SNodeMessageRegistration, ctx, reg, token[:len(token)-4]+"AAAA", s.Address()),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	if _, err := SNodeMessageHash(SNodeMessageMarkActive, ctx, reg); err == nil {
		t.Fatal("a registration body was accepted as mark_active")
	}
	if _, err := SNodeMessageHash(SNodeMessageResultClaim, ctx, &arbiter.RCRecord{}); err == nil {
		t.Fatal("a pointer body was accepted")
	}
	if _, err := SNodeMessageHash("result_claims", ctx, arbiter.RCRecord{}); err == nil {
		t.Fatal("an unknown kind was accepted")
	}
	if _, err := SNodeMessageHash(SNodeMessageCleanupAck, MessageContext{NetworkID: "devnet2"}, arbiter.CleanupAck{}); err == nil {
		t.Fatal("a message without a genesis snapshot id was hashed")
	}
}

// TestSNodeMessageHashCanonicalizesEmptyLists: wire converters decode every
// empty repeated field as nil, so the FSM hashes what it decodes; a sender
// that built [] must sign the same digest (the ByteSideScanMsg.Body rule,
// types.go:249-267).
func TestSNodeMessageHashCanonicalizesEmptyLists(t *testing.T) {
	ctx := testMessageContext()
	for _, tc := range []struct {
		kind          SNodeMessageKind
		nilBody, empty any
	}{
		{SNodeMessageRegistration, arbiter.NodeRegistration{NodeID: "s"}, arbiter.NodeRegistration{NodeID: "s", Roles: []arbiter.NodeRole{}, Ed25519Pubkey: []byte{}}},
		{SNodeMessageResultClaim, arbiter.RCRecord{SourceNode: "s"}, arbiter.RCRecord{SourceNode: "s", CandidateParts: []arbiter.CandidatePart{}, PartitionNewPartSums: []arbiter.PartitionLtHashSum{}}},
		{SNodeMessagePromotionAck, arbiter.PromotionAck{NodeID: "s"}, arbiter.PromotionAck{NodeID: "s", Parts: []arbiter.SafePartMapping{}, SafePartitionParts: []arbiter.SafePartMapping{}}},
	} {
		a, errA := SNodeMessageHash(tc.kind, ctx, tc.nilBody)
		b, errB := SNodeMessageHash(tc.kind, ctx, tc.empty)
		if errA != nil || errB != nil || a != b {
			t.Fatalf("%s: nil and [] hash differently (%s vs %s; %v, %v)", tc.kind, a, b, errA, errB)
		}
	}
}

func TestVerifierMessageSignVerify(t *testing.T) {
	seed := func(b byte) []byte { s := make([]byte, ed25519.SeedSize); s[0] = b; return s }
	priv := ed25519.NewKeyFromSeed(seed(7))
	pub := priv.Public().(ed25519.PublicKey)
	ctx := testMessageContext()
	reg := arbiter.NodeRegistration{NodeID: "verifier-1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: pub, RegistrationSeq: 1760054400000}
	sig, err := SignVerifierMessage(priv, VerifierMessageRegistration, ctx, reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyVerifierMessage(pub, VerifierMessageRegistration, ctx, reg, sig); err != nil {
		t.Fatal(err)
	}
	other := ed25519.NewKeyFromSeed(seed(8)).Public().(ed25519.PublicKey)
	replayed := reg
	replayed.RegistrationSeq--
	otherNetwork := ctx
	otherNetwork.NetworkID = "mainnet"
	for name, err := range map[string]error{
		"another key":      VerifyVerifierMessage(other, VerifierMessageRegistration, ctx, reg, sig),
		"another seq":      VerifyVerifierMessage(pub, VerifierMessageRegistration, ctx, replayed, sig),
		"another network":  VerifyVerifierMessage(pub, VerifierMessageRegistration, otherNetwork, reg, sig),
		"another kind":     VerifyVerifierMessage(pub, VerifierMessageMarkActive, ctx, MarkActiveBody{NodeID: reg.NodeID, RegistrationSeq: reg.RegistrationSeq}, sig),
		"not hex":          VerifyVerifierMessage(pub, VerifierMessageRegistration, ctx, reg, "zz"+sig[2:]),
		"short signature":  VerifyVerifierMessage(pub, VerifierMessageRegistration, ctx, reg, sig[:126]),
		"short public key": VerifyVerifierMessage(pub[:31], VerifierMessageRegistration, ctx, reg, sig),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	if _, err := SignVerifierMessage(priv[:32], VerifierMessageRegistration, ctx, reg); err == nil {
		t.Fatal("signed with a malformed private key instead of refusing")
	}
	snodeHash, _ := SNodeMessageHash(SNodeMessageRegistration, ctx, reg)
	verifierHash, _ := VerifierMessageHash(VerifierMessageRegistration, ctx, reg)
	if snodeHash == verifierHash {
		t.Fatal("SNode and verifier messages must use separate digest domains")
	}
}
```

Create `authority/evict_node_test.go`:

```go
package authority

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func testEvictCommand() arbiter.EvictNodeCommand {
	return arbiter.EvictNodeCommand{NodeID: "snode-2", ExpectedRegistrationSeq: 1760054400000, Reason: "key compromised"}
}

func TestEvictNodeIsContextBoundAndPurposeSeparated(t *testing.T) {
	s, v := newTestPair(t)
	ctx := ConsensusContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", AuthorityEpoch: 2}
	cmd := testEvictCommand()
	old, err := s.SignEvictNodeWithContextAt(cmd, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if addr, err := v.VerifyEvictNode(cmd, old, ctx); err != nil || addr != s.Address() {
		t.Fatalf("deterministic verify: %q, %v", addr, err)
	}
	if _, err := v.AuthorizeEvictNode(cmd, old, ctx); err == nil {
		t.Fatal("the API boundary accepted a stale eviction token")
	}
	fresh, err := s.SignEvictNodeWithContext(cmd, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeEvictNode(cmd, fresh, ctx); err != nil {
		t.Fatalf("fresh token: %v", err)
	}
	promotion, err := s.SignPromotionWithContextAt(testCmd(), ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	evictHash, err := EvictNodeHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	contextless, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: EvictNodePurpose, CmdHash: evictHash})
	if err != nil {
		t.Fatal(err)
	}
	nextEpoch, otherNetwork, otherGenesis := ctx, ctx, ctx
	nextEpoch.AuthorityEpoch = 3
	otherNetwork.NetworkID = "mainnet"
	otherGenesis.GenesisSnapshotID = "0xother"
	otherSeq, otherNode, otherReason := cmd, cmd, cmd
	otherSeq.ExpectedRegistrationSeq++
	otherNode.NodeID = "snode-1"
	otherReason.Reason = "other"
	unlisted := &Validator{AllowedAddresses: map[string]bool{"0x0000000000000000000000000000000000000001": true}}
	verify := func(c arbiter.EvictNodeCommand, token string, cc ConsensusContext) error {
		_, err := v.VerifyEvictNode(c, token, cc)
		return err
	}
	_, unlistedErr := unlisted.VerifyEvictNode(cmd, old, ctx)
	for name, err := range map[string]error{
		"next authority epoch":        verify(cmd, old, nextEpoch),
		"other network":               verify(cmd, old, otherNetwork),
		"other genesis":               verify(cmd, old, otherGenesis),
		"other expected seq":          verify(otherSeq, old, ctx),
		"other node":                  verify(otherNode, old, ctx),
		"other reason":                verify(otherReason, old, ctx),
		"unlisted signer":             unlistedErr,
		"a promotion token":           verify(cmd, promotion, ctx),
		"a token without its context": verify(cmd, contextless, ctx),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	if _, err := v.AuthorizePromotion(testCmd(), fresh); err == nil {
		t.Fatal("an eviction token authorized a promotion")
	}
	for _, bad := range []arbiter.EvictNodeCommand{{Reason: "r"}, {NodeID: "n"}, {NodeID: " ", Reason: "r"}} {
		if _, err := EvictNodeHash(bad); err == nil {
			t.Fatalf("hashed %+v", bad)
		}
	}
}
```

- [ ] **Step 2: Run them and watch them fail**

Run: `cd "$W" && bazel run //:gazelle && bazel test //authority:authority_test //authority/authoritytest:authoritytest_test --test_output=errors`
Expected: build failure: `undefined: MessageContext`, `undefined: SNodeEnrollmentStatement`, `undefined: arbiter.EvictNodeCommand`, `undefined: authority.SNodeMessageRegistration`.

- [ ] **Step 3: Implement the eviction command and the `authority` signing family**

In `consensus.go` append:

```go
// EvictNodeCommand is the canonical signing form of an authority-signed
// eviction (housegate spec 2026-10-10 §6.5); its wire form is
// pb.EvictNodeRequest minus authority_jws. ExpectedRegistrationSeq is a
// compare-and-swap on the node's last applied registration_seq, so a token
// can never evict a later registration of the same node.
type EvictNodeCommand struct {
	NodeID                  string `json:"node_id"`
	ExpectedRegistrationSeq uint64 `json:"expected_registration_seq"`
	Reason                  string `json:"reason"`
}
```

In `authority/context.go` replace `signWithContext` (lines 52-60):

```go
func (s *Signer) signWithContext(hash string, context ConsensusContext, iat int64) (string, error) {
	return s.signWithContextPurpose(hash, PromotionPurpose, context, iat)
}

// signWithContextPurpose signs hash under purpose with the three consensus
// context fields every context-bound authority token carries.
func (s *Signer) signWithContextPurpose(hash, purpose string, context ConsensusContext, iat int64) (string, error) {
	if strings.TrimSpace(context.NetworkID) == "" || strings.TrimSpace(context.GenesisSnapshotID) == "" {
		return "", fmt.Errorf("authority context: network ID and genesis snapshot ID must be non-empty")
	}
	return s.signPayload(JWSCommandPayload{
		Iat: iat, Purpose: purpose, CmdHash: hash,
		NetworkID: context.NetworkID, GenesisSnapshotID: context.GenesisSnapshotID, AuthorityEpoch: &context.AuthorityEpoch,
	})
}
```

Create `authority/snode_messages.go`:

```go
package authority

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// SNodeEnrollmentPurpose is the JWSCommandPayload.Purpose of an SNode
// enrolment statement (housegate spec 2026-10-10 §6.2): the indexer's
// on-chain signer consents to, and controls, one SNode identity of one network.
const SNodeEnrollmentPurpose = "arbiter-snode-enrollment-v1"

// SNodeMessagePurpose is the purpose of every message an SNode signs with its
// indexer's key (spec D6, §6.5); the hash binds the message kind.
const SNodeMessagePurpose = "arbiter-snode-message-v1"

const (
	snodeEnrollmentDomain = "arbiter-snode-enrollment-statement-v1"
	snodeMessageDomain    = "arbiter-snode-message-body-v1"
	verifierMessageDomain = "arbiter-verifier-message-body-v1"
)

// SNodeEnrollmentStatement is what an enrolment JWS signs.
type SNodeEnrollmentStatement struct {
	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
	IndexerID         uint64 `json:"indexer_id"`
	SNodeNodeID       string `json:"snode_node_id"`
}

// SNodeEnrollmentHash is the statement's command hash.
func SNodeEnrollmentHash(stmt SNodeEnrollmentStatement) (string, error) {
	if err := (MessageContext{NetworkID: stmt.NetworkID, GenesisSnapshotID: stmt.GenesisSnapshotID}).validate(); err != nil {
		return "", fmt.Errorf("snode enrollment: %w", err)
	}
	if strings.TrimSpace(stmt.SNodeNodeID) == "" {
		return "", fmt.Errorf("snode enrollment: snode node ID must be non-empty")
	}
	h, err := replay.CanonicalDigest(snodeEnrollmentDomain, stmt)
	if err != nil {
		return "", fmt.Errorf("hash snode enrollment: %w", err)
	}
	return h, nil
}

// SignSNodeEnrollment signs stmt at the current time.
func (s *Signer) SignSNodeEnrollment(stmt SNodeEnrollmentStatement) (string, error) {
	return s.SignSNodeEnrollmentAt(stmt, time.Now().Unix())
}

// SignSNodeEnrollmentAt signs stmt with an explicit issue time (fixtures and
// CLIs that want reproducible output); iat is not a replay guard.
func (s *Signer) SignSNodeEnrollmentAt(stmt SNodeEnrollmentStatement, iat int64) (string, error) {
	h, err := SNodeEnrollmentHash(stmt)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: SNodeEnrollmentPurpose, CmdHash: h})
}

// VerifySNodeEnrollment checks that jws is signer's enrolment of stmt. It is
// deterministic (no clock) and therefore usable in Raft Apply.
func VerifySNodeEnrollment(stmt SNodeEnrollmentStatement, jws, signer string) error {
	h, err := SNodeEnrollmentHash(stmt)
	if err != nil {
		return err
	}
	return verifySignedBy(h, SNodeEnrollmentPurpose, jws, signer)
}

// SNodeMessageKind names one signed SNode message.
type SNodeMessageKind string

const (
	SNodeMessageRegistration SNodeMessageKind = "registration"
	SNodeMessageMarkActive   SNodeMessageKind = "mark_active"
	SNodeMessageResultClaim  SNodeMessageKind = "result_claim"
	SNodeMessagePromotionAck SNodeMessageKind = "promotion_ack"
	SNodeMessageCleanupAck   SNodeMessageKind = "cleanup_ack"
	SNodeMessageTablePurged  SNodeMessageKind = "table_purged"
)

// VerifierMessageKind names one signed verifier message.
type VerifierMessageKind string

const (
	VerifierMessageRegistration VerifierMessageKind = "registration"
	VerifierMessageMarkActive   VerifierMessageKind = "mark_active"
	VerifierMessageTablePurged  VerifierMessageKind = "table_purged"
)

// MessageContext binds a signed data-plane message to one network
// incarnation (spec §6.5).
type MessageContext struct {
	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
}

func (c MessageContext) validate() error {
	if strings.TrimSpace(c.NetworkID) == "" || strings.TrimSpace(c.GenesisSnapshotID) == "" {
		return fmt.Errorf("message context: network ID and genesis snapshot ID must be non-empty")
	}
	return nil
}

// MarkActiveBody is the signed body of a MarkActive request.
type MarkActiveBody struct {
	NodeID          string `json:"node_id"`
	RegistrationSeq uint64 `json:"registration_seq"`
}

// TablePurgedBody is the signed body of a SubmitTablePurged report.
type TablePurgedBody struct {
	NodeID         string `json:"node_id"`
	IncarnationSeq uint64 `json:"incarnation_seq"`
}

type messagePreimage struct {
	Kind    string         `json:"kind"`
	Context MessageContext `json:"context"`
	Body    any            `json:"body"`
}

// SNodeMessageHash is the command hash of one SNode message.
func SNodeMessageHash(kind SNodeMessageKind, ctx MessageContext, body any) (string, error) {
	canonical, err := canonicalSNodeBody(kind, body)
	if err != nil {
		return "", err
	}
	return messageHash(snodeMessageDomain, string(kind), ctx, canonical)
}

// SignSNodeMessage signs one SNode message at the current time.
func (s *Signer) SignSNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any) (string, error) {
	return s.SignSNodeMessageAt(kind, ctx, body, time.Now().Unix())
}

// SignSNodeMessageAt signs with an explicit issue time; iat is informational.
func (s *Signer) SignSNodeMessageAt(kind SNodeMessageKind, ctx MessageContext, body any, iat int64) (string, error) {
	h, err := SNodeMessageHash(kind, ctx, body)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: SNodeMessagePurpose, CmdHash: h})
}

// VerifySNodeMessage checks that jws is signer's signature of the message.
// Deterministic: no clock, usable in Raft Apply and snapshot restore.
func VerifySNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any, jws, signer string) error {
	h, err := SNodeMessageHash(kind, ctx, body)
	if err != nil {
		return err
	}
	return verifySignedBy(h, SNodeMessagePurpose, jws, signer)
}

// VerifierMessageHash is the hash a verifier message's ed25519 signature
// covers; its digest domain is separate from the SNode messages'.
func VerifierMessageHash(kind VerifierMessageKind, ctx MessageContext, body any) (string, error) {
	canonical, err := canonicalVerifierBody(kind, body)
	if err != nil {
		return "", err
	}
	return messageHash(verifierMessageDomain, string(kind), ctx, canonical)
}

// SignVerifierMessage signs the message hash's string bytes with priv and
// returns lowercase hex: the convention of ByteSideScanMsg and attestations.
func SignVerifierMessage(priv ed25519.PrivateKey, kind VerifierMessageKind, ctx MessageContext, body any) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("verifier message: ed25519 private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	h, err := VerifierMessageHash(kind, ctx, body)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(ed25519.Sign(priv, []byte(h))), nil
}

// VerifyVerifierMessage checks a verifier message signature. Deterministic.
func VerifyVerifierMessage(pub ed25519.PublicKey, kind VerifierMessageKind, ctx MessageContext, body any, sigHex string) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("verifier message: ed25519 public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	h, err := VerifierMessageHash(kind, ctx, body)
	if err != nil {
		return err
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("verifier message: signature must be %d bytes of hex", ed25519.SignatureSize)
	}
	if !ed25519.Verify(pub, []byte(h), sig) {
		return fmt.Errorf("verifier message: %s signature does not verify", kind)
	}
	return nil
}

func messageHash(domain, kind string, ctx MessageContext, body any) (string, error) {
	if err := ctx.validate(); err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(domain, messagePreimage{Kind: kind, Context: ctx, Body: body})
	if err != nil {
		return "", fmt.Errorf("hash %s message: %w", kind, err)
	}
	return h, nil
}

// canonicalSNodeBody pairs each kind with its one body type (a value, never a
// pointer) and returns the body as the FSM decodes it: wire converters turn
// every empty repeated field into nil, so [] hashes as null here too.
func canonicalSNodeBody(kind SNodeMessageKind, body any) (any, error) {
	switch kind {
	case SNodeMessageRegistration:
		if v, ok := body.(arbiter.NodeRegistration); ok {
			return canonicalRegistration(v), nil
		}
	case SNodeMessageMarkActive:
		if v, ok := body.(MarkActiveBody); ok {
			return v, nil
		}
	case SNodeMessageResultClaim:
		if v, ok := body.(arbiter.RCRecord); ok {
			v.CandidateParts = nilIfEmpty(v.CandidateParts)
			v.PartitionNewPartSums = nilIfEmpty(v.PartitionNewPartSums)
			return v, nil
		}
	case SNodeMessagePromotionAck:
		if v, ok := body.(arbiter.PromotionAck); ok {
			v.Parts = nilIfEmpty(v.Parts)
			v.SafePartitionParts = nilIfEmpty(v.SafePartitionParts)
			return v, nil
		}
	case SNodeMessageCleanupAck:
		if v, ok := body.(arbiter.CleanupAck); ok {
			return v, nil
		}
	case SNodeMessageTablePurged:
		if v, ok := body.(TablePurgedBody); ok {
			return v, nil
		}
	default:
		return nil, fmt.Errorf("snode message: unknown kind %q", kind)
	}
	return nil, fmt.Errorf("snode message: kind %q does not take a %T body", kind, body)
}

func canonicalVerifierBody(kind VerifierMessageKind, body any) (any, error) {
	switch kind {
	case VerifierMessageRegistration:
		if v, ok := body.(arbiter.NodeRegistration); ok {
			return canonicalRegistration(v), nil
		}
	case VerifierMessageMarkActive:
		if v, ok := body.(MarkActiveBody); ok {
			return v, nil
		}
	case VerifierMessageTablePurged:
		if v, ok := body.(TablePurgedBody); ok {
			return v, nil
		}
	default:
		return nil, fmt.Errorf("verifier message: unknown kind %q", kind)
	}
	return nil, fmt.Errorf("verifier message: kind %q does not take a %T body", kind, body)
}

func canonicalRegistration(r arbiter.NodeRegistration) arbiter.NodeRegistration {
	r.Roles = nilIfEmpty(r.Roles)
	r.Ed25519Pubkey = nilIfEmpty(r.Ed25519Pubkey)
	return r
}

func nilIfEmpty[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	return s
}

// verifySignedBy is the deterministic single-signer form of Validator.verify:
// the same ES256K compact JWS, purpose and command-hash checks, no token-age
// check, and the recovered address must be signer (compared lowercase).
func verifySignedBy(wantHash, purpose, token, signer string) error {
	want, err := normalizeSignerAddress(signer)
	if err != nil {
		return err
	}
	v := Validator{AllowedAddresses: map[string]bool{want: true}}
	if _, err := v.verify(wantHash, purpose, token, false); err != nil {
		return fmt.Errorf("%s: %w", purpose, err)
	}
	return nil
}

func normalizeSignerAddress(signer string) (string, error) {
	s := strings.ToLower(signer)
	if len(s) != 42 || !strings.HasPrefix(s, "0x") {
		return "", fmt.Errorf("signer %q must be a 0x-prefixed 20-byte address", signer)
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil || !slices.ContainsFunc(b, func(x byte) bool { return x != 0 }) {
		return "", fmt.Errorf("signer %q must be a non-zero hex address", signer)
	}
	return s, nil
}
```

Create `authority/evict_node.go`:

```go
package authority

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// EvictNodePurpose is the token family of an authority-signed node eviction
// (housegate spec 2026-10-10 §6.5). A promotion, cleanup or consensus-update
// token cannot evict a node, and an eviction token authorizes nothing else.
const EvictNodePurpose = "arbiter-evict-node-v1"

const evictNodeCommandDomain = "arbiter-evict-node-command-v1"

// EvictNodeHash is the eviction's command hash.
func EvictNodeHash(cmd arbiter.EvictNodeCommand) (string, error) {
	if strings.TrimSpace(cmd.NodeID) == "" || strings.TrimSpace(cmd.Reason) == "" {
		return "", fmt.Errorf("evict node: node ID and reason must be non-empty")
	}
	h, err := replay.CanonicalDigest(evictNodeCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash evict node command: %w", err)
	}
	return h, nil
}

// SignEvictNodeWithContext signs an eviction bound to one network incarnation
// and authority epoch, like SignPromotionWithContext.
func (s *Signer) SignEvictNodeWithContext(cmd arbiter.EvictNodeCommand, ctx ConsensusContext) (string, error) {
	return s.SignEvictNodeWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignEvictNodeWithContextAt signs with an explicit issue time.
func (s *Signer) SignEvictNodeWithContextAt(cmd arbiter.EvictNodeCommand, ctx ConsensusContext, iat int64) (string, error) {
	h, err := EvictNodeHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, EvictNodePurpose, ctx, iat)
}

// VerifyEvictNode checks signature, purpose, command hash, the allowlist and
// that the token carries exactly ctx (all three context fields; unlike legacy
// promotion tokens there is no context-less form). It reads no clock: Raft
// Apply uses it with the current authority set and context.
func (v *Validator) VerifyEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyEvictNode(cmd, token, ctx, false)
}

// AuthorizeEvictNode additionally enforces token age for the EvictNode RPC.
// It must not be used inside replicated Apply or snapshot replay.
func (v *Validator) AuthorizeEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyEvictNode(cmd, token, ctx, true)
}

func (v *Validator) verifyEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	h, err := EvictNodeHash(cmd)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(ctx.NetworkID) == "" || strings.TrimSpace(ctx.GenesisSnapshotID) == "" {
		return "", fmt.Errorf("evict node: network ID and genesis snapshot ID must be non-empty")
	}
	addr, err := v.verify(h, EvictNodePurpose, token, enforceAge)
	if err != nil {
		return "", err
	}
	// verify authenticated these payload bytes; read the context they carry.
	data, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	if err != nil {
		return "", fmt.Errorf("evict node token payload: %w", err)
	}
	var payload JWSCommandPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("evict node token payload: %w", err)
	}
	if payload.NetworkID != ctx.NetworkID || payload.GenesisSnapshotID != ctx.GenesisSnapshotID ||
		payload.AuthorityEpoch == nil || *payload.AuthorityEpoch != ctx.AuthorityEpoch {
		return "", fmt.Errorf("evict node token: consensus context mismatch")
	}
	return addr, nil
}
```

- [ ] **Step 4: Run and commit**

Run: `cd "$W" && bazel run //:gazelle && bazel test //authority:authority_test //authority/authoritytest:authoritytest_test //:arbiter-core_test --test_output=errors`
Expected: PASS. Every vector in `authoritytest` is reproduced exactly; `TestContextSigningPreservesSNodeCompatibility` and `TestContextSigningExplicitTimeAndLegacyPayload` (the refactored `signWithContext`) are unchanged. If a vector differs, fix the implementation, never the constant: the constants were computed outside Go from the canonical form written above (kind/context/body preimage, field order and `omitempty` as declared).

```bash
cd "$W" && git add consensus.go authority && git commit -m "feat(authority): SNode enrolment, signed SNode and verifier messages, context-bound eviction

ES256K enrolment statements and SNode messages (purposes
arbiter-snode-enrollment-v1 / arbiter-snode-message-v1) verified
deterministically through Validator.verify with a one-address allowlist;
ed25519 verifier messages; authority eviction tokens (arbiter-evict-node-v1)
bound to network, genesis and epoch; authoritytest pins every vector
(housegate spec 2026-10-10 §6.2, §6.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Write the failing wire tests**

In `conformance/arbiter_wire_test.go` add to `TestArbiterMirrorsMatchProto`:

```go
	// EvictNodeCommand is the signed body of ConsensusAdmin.EvictNode.
	assertMirror(t, arbiter.EvictNodeCommand{}, &pb.EvictNodeRequest{}, "authority_jws")
```

Create `wire/signed_claims_test.go` (reuses `preSignedClaimsGolden`, `goldenClaim` from Step 2 of Task 2 and `mustRoundTrip`):

```go
package wire

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// withEverySignedClaimsField sets every field the signed-claims stage adds to
// the one command c carries (UpdateConsensusParams is left alone).
func withEverySignedClaimsField(c Command) Command {
	const seq, jws, sig = uint64(1760054400000), "h.p.s", "ab"
	owner := uint64(1)
	out := c
	switch {
	case c.RegisterRC != nil:
		v := *c.RegisterRC
		v.SourceJWS = jws
		out.RegisterRC = &v
	case c.RecordPromotionAck != nil:
		v := *c.RecordPromotionAck
		v.SourceJWS = jws
		out.RecordPromotionAck = &v
	case c.RecordCleanupAck != nil:
		v := *c.RecordCleanupAck
		v.SourceJWS = jws
		out.RecordCleanupAck = &v
	case c.RegisterNode != nil:
		v := *c.RegisterNode
		v.Registration.RegistrationSeq, v.SignerJWS, v.Ed25519Signature = seq, jws, sig
		out.RegisterNode = &v
	case c.MarkActive != nil:
		v := *c.MarkActive
		v.RegistrationSeq, v.SignerJWS, v.Ed25519Signature = seq, jws, sig
		out.MarkActive = &v
	case c.EvictNode != nil:
		v := *c.EvictNode
		v.ExpectedRegistrationSeq, v.AuthorityJWS = seq, jws
		out.EvictNode = &v
	case c.RecordTablePurged != nil:
		v := *c.RecordTablePurged
		v.SignerJWS, v.Ed25519Signature = jws, sig
		out.RecordTablePurged = &v
	case c.AddTable != nil:
		v := *c.AddTable
		v.OwnerIndexerID = &owner
		out.AddTable = &v
	case c.SeedLegacyTables != nil:
		v := *c.SeedLegacyTables
		v.IndexerID = &owner
		out.SeedLegacyTables = &v
	}
	return out
}

func TestSignedClaimsCommandsRoundTrip(t *testing.T) {
	for _, tc := range preSignedClaimsGolden() {
		t.Run(tc.name, func(t *testing.T) { mustRoundTrip(t, withEverySignedClaimsField(tc.cmd)) })
	}
}

// TestStripSignedClaimsRestoresThePreActivationBytes: whatever an upgraded
// SNode, verifier or watcher supplies, a pre-activation proposal encodes to
// the previous release's bytes (plan S1-A CONTRACT §0).
func TestStripSignedClaimsRestoresThePreActivationBytes(t *testing.T) {
	for _, tc := range preSignedClaimsGolden() {
		t.Run(tc.name, func(t *testing.T) {
			full := withEverySignedClaimsField(tc.cmd)
			decorated, err := Encode(full)
			if err != nil {
				t.Fatal(err)
			}
			if tc.cmd.UpdateConsensusParams == nil && hex.EncodeToString(decorated) == tc.want {
				t.Fatal("the decorated command must differ from the pre-activation bytes")
			}
			stripped, err := Encode(StripSignedClaims(full))
			if err != nil {
				t.Fatal(err)
			}
			if hex.EncodeToString(stripped) != tc.want {
				t.Fatalf("stripped bytes:\n got %s\nwant %s", hex.EncodeToString(stripped), tc.want)
			}
			if !reflect.DeepEqual(full, withEverySignedClaimsField(tc.cmd)) {
				t.Fatal("StripSignedClaims modified its argument")
			}
		})
	}
}

// TestDecodeRefusesSignaturesInRequestOnlyCopies makes a buggy encoder fail on
// every voter alike: the Raft command carries each signature in its own field.
func TestDecodeRefusesSignaturesInRequestOnlyCopies(t *testing.T) {
	for name, cmd := range map[string]*pb.RaftCommand{
		"RCRecord.source_jws": {Cmd: &pb.RaftCommand_RegisterRc{RegisterRc: &pb.RegisterRCCmd{
			Rc: &pb.RCRecord{SourceNode: "s1", SourceJws: "h.p.s"}}}},
		"PromotionAck.source_jws": {Cmd: &pb.RaftCommand_RecordPromotionAck{RecordPromotionAck: &pb.RecordPromotionAckCmd{
			Ack: &pb.PromotionAck{NodeId: "s1", SourceJws: "h.p.s"}}}},
		"CleanupAck.source_jws": {Cmd: &pb.RaftCommand_RecordCleanupAck{RecordCleanupAck: &pb.RecordCleanupAckCmd{
			Ack: &pb.CleanupAck{NodeId: "s1", SourceJws: "h.p.s"}}}},
		"NodeRegistration.signer_jws": {Cmd: &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{
			Registration: &pb.NodeRegistration{NodeId: "s1", SignerJws: "h.p.s"}}}},
		"NodeRegistration.ed25519_signature": {Cmd: &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{
			Registration: &pb.NodeRegistration{NodeId: "v1", Ed25519Signature: "ab"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := proto.Marshal(cmd)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Decode(b); err == nil || !strings.Contains(err.Error(), "request-only") {
				t.Fatalf("Decode = %v, want a request-only refusal", err)
			}
		})
	}
}

func TestRequestConvertersCarrySignatures(t *testing.T) {
	rc := RegisterRC{RC: goldenClaim(), SourceJWS: "h.p.s"}
	if got := RegisterRCFromRequest(RegisterRCToRequest(rc)); !reflect.DeepEqual(got, rc) {
		t.Fatalf("RC request round trip = %+v", got)
	}
	if RCToPB(rc.RC).GetSourceJws() != "" {
		t.Fatal("the canonical RC converter must never set the request-only signature")
	}
	pack := RecordPromotionAck{Ack: goldenPromotionAck(), SourceJWS: "h.p.s"}
	if got := RecordPromotionAckFromRequest(RecordPromotionAckToRequest(pack)); !reflect.DeepEqual(got, pack) {
		t.Fatalf("promotion ack request round trip = %+v", got)
	}
	cack := RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 7, TableID: "db1.t", PartitionID: "all"}, SourceJWS: "h.p.s"}
	if got := RecordCleanupAckFromRequest(RecordCleanupAckToRequest(cack)); !reflect.DeepEqual(got, cack) {
		t.Fatalf("cleanup ack request round trip = %+v", got)
	}
	reg := RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 7}, SignerJWS: "h.p.s"}
	req := RegisterNodeToRequest(reg, []string{arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature})
	if req.GetRegistrationSeq() != 7 || req.GetSignerJws() != "h.p.s" || len(req.GetFeatures()) != 2 {
		t.Fatalf("registration request = %v", req)
	}
	if got := RegisterNodeFromRequest(req); !reflect.DeepEqual(got, reg) {
		t.Fatalf("registration request round trip = %+v", got)
	}
	b, err := Encode(Command{RegisterNode: &reg})
	if err != nil {
		t.Fatal(err)
	}
	var m pb.RaftCommand
	if err := proto.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if r := m.GetRegisterNode().GetRegistration(); r.GetSignerJws() != "" || len(r.GetFeatures()) != 0 || r.GetRegistrationSeq() != 7 || m.GetRegisterNode().GetSignerJws() != "h.p.s" {
		t.Fatalf("RegisterNodeCmd = %v: the signature rides beside the registration, the seq inside it", &m)
	}
	mark := MarkActive{NodeID: "s1", RegistrationSeq: 7, SignerJWS: "h.p.s"}
	if got := MarkActiveFromRequest(MarkActiveToRequest(mark)); got != mark {
		t.Fatalf("mark-active request round trip = %+v", got)
	}
	purged := RecordTablePurged{NodeID: "v1", IncarnationSeq: 5, Ed25519Signature: "ab"}
	if got := RecordTablePurgedFromRequest(RecordTablePurgedToRequest(purged)); got != purged {
		t.Fatalf("purge report round trip = %+v", got)
	}
	evict := EvictNode{NodeID: "snode-2", Reason: "key compromised", ExpectedRegistrationSeq: 9, AuthorityJWS: "h.p.s"}
	if got := EvictNodeFromRequest(EvictNodeToRequest(evict)); got != evict {
		t.Fatalf("eviction request round trip = %+v", got)
	}
	if evict.Canonical() != (arbiter.EvictNodeCommand{NodeID: "snode-2", ExpectedRegistrationSeq: 9, Reason: "key compromised"}) {
		t.Fatalf("Canonical = %+v", evict.Canonical())
	}
}
```

- [ ] **Step 6: Run them and watch them fail**

Run: `cd "$W" && bazel run //:gazelle && bazel test //wire:wire_test //conformance:conformance_test --test_output=errors`
Expected: build failure: `unknown field SourceJWS in struct literal of type RegisterRC`, `undefined: StripSignedClaims`, `undefined: RegisterRCToRequest`, `evict.Canonical undefined`.

- [ ] **Step 7: Implement the signature-bearing commands**

In `wire/command.go` replace the command structs at lines 40, 53, 59 and 69-74:

```go
// RegisterRC mirrors pb.RegisterRCCmd. SourceJWS is the bound source's
// ES256K signature over RC (authority.SNodeMessageResultClaim); empty before
// the signed-claims activation.
type RegisterRC struct {
	RC        arbiter.RCRecord
	SourceJWS string
}

// RecordPromotionAck mirrors pb.RecordPromotionAckCmd.
type RecordPromotionAck struct {
	Ack       arbiter.PromotionAck
	SourceJWS string
}

// RecordCleanupAck mirrors pb.RecordCleanupAckCmd.
type RecordCleanupAck struct {
	Ack       arbiter.CleanupAck
	SourceJWS string
}

// RegisterNode mirrors pb.RegisterNodeCmd. An SNODE registration carries
// SignerJWS, a VERIFIER registration Ed25519Signature; Registration.
// RegistrationSeq rides inside the registration. All three stay empty before
// the signed-claims activation.
type RegisterNode struct {
	Registration     arbiter.NodeRegistration
	SignerJWS        string
	Ed25519Signature string
}

// MarkActive mirrors pb.MarkActiveCmd.
type MarkActive struct {
	NodeID           string
	RegistrationSeq  uint64
	SignerJWS        string
	Ed25519Signature string
}

// EvictNode mirrors pb.EvictNodeCmd. ExpectedRegistrationSeq and AuthorityJWS
// are set only by the authority-signed EvictNode RPC after the activation.
type EvictNode struct {
	NodeID                  string
	Reason                  string
	ExpectedRegistrationSeq uint64
	AuthorityJWS            string
}

// Canonical is the eviction's signing form (authority.EvictNodeHash).
func (c EvictNode) Canonical() arbiter.EvictNodeCommand {
	return arbiter.EvictNodeCommand{NodeID: c.NodeID, ExpectedRegistrationSeq: c.ExpectedRegistrationSeq, Reason: c.Reason}
}
```

In `wire/table_registry.go` replace `RecordTablePurged` (lines 44-48):

```go
// RecordTablePurged mirrors pb.RecordTablePurgedCmd, which is also the
// SubmitTablePurged request. An SNODE reporter signs SignerJWS, a VERIFIER
// reporter Ed25519Signature; both stay empty before the activation.
type RecordTablePurged struct {
	NodeID           string
	IncarnationSeq   uint64
	SignerJWS        string
	Ed25519Signature string
}
```

In `Encode` replace the `RegisterRC` (lines 136-139), `RecordPromotionAck` (159-162), `RecordCleanupAck` (172-175), `RegisterNode` / `MarkActive` / `EvictNode` (186-197) and `RecordTablePurged` (273-277) blocks:

```go
	if c.RegisterRC != nil {
		set++
		out.Cmd = &pb.RaftCommand_RegisterRc{RegisterRc: &pb.RegisterRCCmd{Rc: RCToPB(c.RegisterRC.RC), SourceJws: c.RegisterRC.SourceJWS}}
	}
```

```go
	if c.RecordPromotionAck != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordPromotionAck{RecordPromotionAck: &pb.RecordPromotionAckCmd{
			Ack: PromotionAckToPB(c.RecordPromotionAck.Ack), SourceJws: c.RecordPromotionAck.SourceJWS}}
	}
```

```go
	if c.RecordCleanupAck != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordCleanupAck{RecordCleanupAck: &pb.RecordCleanupAckCmd{
			Ack: CleanupAckToPB(c.RecordCleanupAck.Ack), SourceJws: c.RecordCleanupAck.SourceJWS}}
	}
```

```go
	if c.RegisterNode != nil {
		set++
		out.Cmd = &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{
			Registration: RegistrationToPB(c.RegisterNode.Registration),
			SignerJws:    c.RegisterNode.SignerJWS, Ed25519Signature: c.RegisterNode.Ed25519Signature}}
	}
	if c.MarkActive != nil {
		set++
		m := c.MarkActive
		out.Cmd = &pb.RaftCommand_MarkActive{MarkActive: &pb.MarkActiveCmd{
			NodeId: m.NodeID, RegistrationSeq: m.RegistrationSeq, SignerJws: m.SignerJWS, Ed25519Signature: m.Ed25519Signature}}
	}
	if c.EvictNode != nil {
		set++
		e := c.EvictNode
		out.Cmd = &pb.RaftCommand_EvictNode{EvictNode: &pb.EvictNodeCmd{NodeId: e.NodeID, Reason: e.Reason,
			ExpectedRegistrationSeq: e.ExpectedRegistrationSeq, AuthorityJws: e.AuthorityJWS}}
	}
```

```go
	if c.RecordTablePurged != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordTablePurged{RecordTablePurged: RecordTablePurgedToRequest(*c.RecordTablePurged)}
	}
```

In `Decode` replace the `RecordTablePurged` case (lines 349-351), the `RegisterRc` case (360-361), `RecordPromotionAck` (373-374), `RecordCleanupAck` (380-381) and `RegisterNode` / `MarkActive` / `EvictNode` (388-398):

```go
	case *pb.RaftCommand_RecordTablePurged:
		v := RecordTablePurgedFromRequest(cmd.RecordTablePurged)
		return Command{RecordTablePurged: &v}, nil
```

```go
	case *pb.RaftCommand_RegisterRc:
		rc := cmd.RegisterRc.GetRc()
		if rc.GetSourceJws() != "" {
			return Command{}, errRequestOnly("RCRecord.source_jws", "RegisterRCCmd.source_jws")
		}
		return Command{RegisterRC: &RegisterRC{RC: RCFromPB(rc), SourceJWS: cmd.RegisterRc.GetSourceJws()}}, nil
```

```go
	case *pb.RaftCommand_RecordPromotionAck:
		ack := cmd.RecordPromotionAck.GetAck()
		if ack.GetSourceJws() != "" {
			return Command{}, errRequestOnly("PromotionAck.source_jws", "RecordPromotionAckCmd.source_jws")
		}
		return Command{RecordPromotionAck: &RecordPromotionAck{Ack: PromotionAckFromPB(ack), SourceJWS: cmd.RecordPromotionAck.GetSourceJws()}}, nil
```

```go
	case *pb.RaftCommand_RecordCleanupAck:
		ack := cmd.RecordCleanupAck.GetAck()
		if ack.GetSourceJws() != "" {
			return Command{}, errRequestOnly("CleanupAck.source_jws", "RecordCleanupAckCmd.source_jws")
		}
		return Command{RecordCleanupAck: &RecordCleanupAck{Ack: CleanupAckFromPB(ack), SourceJWS: cmd.RecordCleanupAck.GetSourceJws()}}, nil
```

```go
	case *pb.RaftCommand_RegisterNode:
		reg := cmd.RegisterNode.GetRegistration()
		// NodeRegistration.features is request-only (housegate spec 2026-10-09
		// §5.6): refuse it here so a buggy encoder fails on every voter alike.
		if len(reg.GetFeatures()) != 0 {
			return Command{}, fmt.Errorf("wire: NodeRegistration.features is request-only and never part of a RaftCommand")
		}
		if reg.GetSignerJws() != "" || reg.GetEd25519Signature() != "" {
			return Command{}, errRequestOnly("NodeRegistration.signer_jws / ed25519_signature", "RegisterNodeCmd.signer_jws / ed25519_signature")
		}
		return Command{RegisterNode: &RegisterNode{Registration: RegistrationFromPB(reg),
			SignerJWS: cmd.RegisterNode.GetSignerJws(), Ed25519Signature: cmd.RegisterNode.GetEd25519Signature()}}, nil
	case *pb.RaftCommand_MarkActive:
		m := cmd.MarkActive
		return Command{MarkActive: &MarkActive{NodeID: m.GetNodeId(), RegistrationSeq: m.GetRegistrationSeq(),
			SignerJWS: m.GetSignerJws(), Ed25519Signature: m.GetEd25519Signature()}}, nil
	case *pb.RaftCommand_EvictNode:
		e := cmd.EvictNode
		return Command{EvictNode: &EvictNode{NodeID: e.GetNodeId(), Reason: e.GetReason(),
			ExpectedRegistrationSeq: e.GetExpectedRegistrationSeq(), AuthorityJWS: e.GetAuthorityJws()}}, nil
```

and add after `Decode`:

```go
// errRequestOnly refuses a signature inside the request-only copy of a
// command's payload: the Raft command carries it in its own field, so a buggy
// encoder fails on every voter instead of being applied by some.
func errRequestOnly(field, carrier string) error {
	return fmt.Errorf("wire: %s is request-only; the Raft command carries it in %s", field, carrier)
}
```

Create `wire/requests.go`:

```go
package wire

import (
	"slices"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// Request converters (housegate spec 2026-10-10 §6.5). A signed RPC keeps its
// request message: the signature rides in a request-only field beside a body
// that excludes it, and the Raft command carries it in a field of its own.
// Servers decode a request with *FromRequest and propose the result (through
// StripSignedClaims before the signed-claims activation); data-plane clients
// build requests with *ToRequest.

// RegisterRCFromRequest decodes a SourceClaims.RegisterResultClaim request.
func RegisterRCFromRequest(m *pb.RCRecord) RegisterRC {
	return RegisterRC{RC: RCFromPB(m), SourceJWS: m.GetSourceJws()}
}

// RegisterRCToRequest builds a RegisterResultClaim request.
func RegisterRCToRequest(c RegisterRC) *pb.RCRecord {
	m := RCToPB(c.RC)
	m.SourceJws = c.SourceJWS
	return m
}

// RecordPromotionAckFromRequest decodes a PromotionGateway.AckPromotion request.
func RecordPromotionAckFromRequest(m *pb.PromotionAck) RecordPromotionAck {
	return RecordPromotionAck{Ack: PromotionAckFromPB(m), SourceJWS: m.GetSourceJws()}
}

// RecordPromotionAckToRequest builds an AckPromotion request.
func RecordPromotionAckToRequest(c RecordPromotionAck) *pb.PromotionAck {
	m := PromotionAckToPB(c.Ack)
	m.SourceJws = c.SourceJWS
	return m
}

// RecordCleanupAckFromRequest decodes a PromotionGateway.AckCleanup request.
func RecordCleanupAckFromRequest(m *pb.CleanupAck) RecordCleanupAck {
	return RecordCleanupAck{Ack: CleanupAckFromPB(m), SourceJWS: m.GetSourceJws()}
}

// RecordCleanupAckToRequest builds an AckCleanup request.
func RecordCleanupAckToRequest(c RecordCleanupAck) *pb.CleanupAck {
	m := CleanupAckToPB(c.Ack)
	m.SourceJws = c.SourceJWS
	return m
}

// RegisterNodeFromRequest decodes a Membership.RegisterNode request. The
// request-only features stay out; the leader keeps them in its feature book.
func RegisterNodeFromRequest(m *pb.NodeRegistration) RegisterNode {
	return RegisterNode{Registration: RegistrationFromPB(m), SignerJWS: m.GetSignerJws(), Ed25519Signature: m.GetEd25519Signature()}
}

// RegisterNodeToRequest builds a RegisterNode request advertising features.
func RegisterNodeToRequest(c RegisterNode, features []string) *pb.NodeRegistration {
	m := RegistrationToPB(c.Registration)
	m.SignerJws, m.Ed25519Signature = c.SignerJWS, c.Ed25519Signature
	m.Features = slices.Clone(features)
	return m
}

// MarkActiveFromRequest decodes a Membership.MarkActive request.
func MarkActiveFromRequest(m *pb.NodeRef) MarkActive {
	return MarkActive{NodeID: m.GetNodeId(), RegistrationSeq: m.GetRegistrationSeq(),
		SignerJWS: m.GetSignerJws(), Ed25519Signature: m.GetEd25519Signature()}
}

// MarkActiveToRequest builds a MarkActive request.
func MarkActiveToRequest(c MarkActive) *pb.NodeRef {
	return &pb.NodeRef{NodeId: c.NodeID, RegistrationSeq: c.RegistrationSeq, SignerJws: c.SignerJWS, Ed25519Signature: c.Ed25519Signature}
}

// RecordTablePurgedFromRequest decodes a SubmitTablePurged request, which is
// the RecordTablePurgedCmd itself.
func RecordTablePurgedFromRequest(m *pb.RecordTablePurgedCmd) RecordTablePurged {
	return RecordTablePurged{NodeID: m.GetNodeId(), IncarnationSeq: m.GetIncarnationSeq(),
		SignerJWS: m.GetSignerJws(), Ed25519Signature: m.GetEd25519Signature()}
}

// RecordTablePurgedToRequest builds the SubmitTablePurged request (and the
// Raft command body).
func RecordTablePurgedToRequest(c RecordTablePurged) *pb.RecordTablePurgedCmd {
	return &pb.RecordTablePurgedCmd{NodeId: c.NodeID, IncarnationSeq: c.IncarnationSeq,
		SignerJws: c.SignerJWS, Ed25519Signature: c.Ed25519Signature}
}

// EvictNodeFromRequest decodes a ConsensusAdmin.EvictNode request.
func EvictNodeFromRequest(m *pb.EvictNodeRequest) EvictNode {
	return EvictNode{NodeID: m.GetNodeId(), Reason: m.GetReason(),
		ExpectedRegistrationSeq: m.GetExpectedRegistrationSeq(), AuthorityJWS: m.GetAuthorityJws()}
}

// EvictNodeToRequest builds an EvictNode request (arbiter-admin).
func EvictNodeToRequest(c EvictNode) *pb.EvictNodeRequest {
	return &pb.EvictNodeRequest{NodeId: c.NodeID, ExpectedRegistrationSeq: c.ExpectedRegistrationSeq,
		Reason: c.Reason, AuthorityJws: c.AuthorityJWS}
}
```

Create `wire/signed_claims.go`:

```go
package wire

// StripSignedClaims returns c without any field the signed-claims stage
// introduced (housegate spec 2026-10-10 §6.5; plan S1-A CONTRACT §0):
// signatures, registration sequences, AddTable's owner and a seed's indexer.
// arbiter-core's strict decoder refuses unknown fields, so until the
// activation every voter must receive exactly the commands the previous
// release encodes; every pre-activation proposal path passes its command
// through this function. UpdateConsensusParams is returned unchanged: the
// activation update is the first command allowed to carry new fields. c
// itself is not modified.
func StripSignedClaims(c Command) Command {
	out := c
	if c.RegisterRC != nil {
		v := *c.RegisterRC
		v.SourceJWS = ""
		out.RegisterRC = &v
	}
	if c.RecordPromotionAck != nil {
		v := *c.RecordPromotionAck
		v.SourceJWS = ""
		out.RecordPromotionAck = &v
	}
	if c.RecordCleanupAck != nil {
		v := *c.RecordCleanupAck
		v.SourceJWS = ""
		out.RecordCleanupAck = &v
	}
	if c.RegisterNode != nil {
		v := *c.RegisterNode
		v.Registration.RegistrationSeq, v.SignerJWS, v.Ed25519Signature = 0, "", ""
		out.RegisterNode = &v
	}
	if c.MarkActive != nil {
		v := *c.MarkActive
		v.RegistrationSeq, v.SignerJWS, v.Ed25519Signature = 0, "", ""
		out.MarkActive = &v
	}
	if c.EvictNode != nil {
		v := *c.EvictNode
		v.ExpectedRegistrationSeq, v.AuthorityJWS = 0, ""
		out.EvictNode = &v
	}
	if c.RecordTablePurged != nil {
		v := *c.RecordTablePurged
		v.SignerJWS, v.Ed25519Signature = "", ""
		out.RecordTablePurged = &v
	}
	if c.AddTable != nil {
		v := *c.AddTable
		v.OwnerIndexerID = nil
		out.AddTable = &v
	}
	if c.SeedLegacyTables != nil {
		v := *c.SeedLegacyTables
		v.IndexerID = nil
		out.SeedLegacyTables = &v
	}
	return out
}
```

- [ ] **Step 8: Run the full suite and commit**

Run: `cd "$W" && bazel run //:gazelle && bazel build //... && bazel test //... --test_output=errors`
Expected: every target PASSES; `TestPreSignedClaimsCommandBytesAreFrozen`, `TestLegacyCommandBytesAreFrozen`, `TestRegistrationFeaturesNeverEnterARaftCommand`, `TestDecodeRefusesARegistrationCommandCarryingFeatures` and `TestSnapshotQueryPreservesLegacyCommandJSONAndBytes` unchanged (an empty signature field encodes nothing, and the nil-pointer JSON of `Command` is unchanged).

```bash
cd "$W" && git add wire conformance && git commit -m "feat(wire): signature-bearing commands, request converters and StripSignedClaims

RegisterRC/RecordPromotionAck/RecordCleanupAck source_jws, RegisterNode and
MarkActive signatures with registration_seq, authority-signed EvictNode and
signed purge reports; request converters for every signed RPC; Decode
refuses a signature in a request-only copy; StripSignedClaims restores the
pre-activation bytes (housegate spec 2026-10-10 §6.5; CONTRACT §0).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 4: arbiter-core — signing SNode, owner-scoped data plane, signing verifier, release CORE_TAG (S1)

**Files:**
- Modify: `dataplane/tableset/reconciler.go:58-74` (`Config.Owner`), `:139-168` (`New` copies it), `:376-420` (owner filter in `reconcileRegistry`); Create: `dataplane/tableset/owner_test.go`, `dataplane/tableset/owner_ch_test.go`
- Modify: `dataplane/table_purge.go:17-37` (`SubmitTablePurgedReport`, `PurgeReporter`); Create: `dataplane/genesis.go`, `dataplane/table_purge_test.go`
- Modify: `snode/config.go:22-79` (`IndexerID`, `GenesisSnapshotID`; the empty-genesis check moves to `New`), `snode/state.go:50-67` and `:449-468` (`RegistrationSeq`, `cloneLocalState`, `NextRegistrationSeq`), `snode/snode.go:28-37, 78-167, 174-176` (`Deps`, `Role`, `New`, `Register`, `PromotedUnsafeParts`), `snode/registry.go:20-85` (`ErrTableNotOwned`, owner refusal), `snode/view.go:46-63` (owner-scoped claim root), `snode/staged.go:393-396` (signed RC), `snode/converge.go:13-35` (owner-scoped lookup), `snode/promote.go:14-23, 129-134`, `snode/cleanup.go:18-22, 48-53`
- Create: `snode/claims.go`, `snode/claims_test.go`; Modify: `snode/registration_test.go`, `snode/state_test.go`
- Modify: `verifier/config.go:20-51` (`StateDir`, `GenesisSnapshotID`), `verifier/verifier.go:50-141` (`Deps.Now`, `Role`, `New`, `Register`)
- Create: `verifier/claims.go`, `verifier/registration_state.go`, `verifier/claims_test.go`; Modify: `verifier/registration_test.go`
- Modify: `README.md` (§ Compatibility)

**Interfaces:**
- Consumes: Task 2 (`wire.TableRegistrySnapshot.Owner` / `.SIIndexer` / `.SignedClaimsActive`, `TableIncarnation.OwnerIndexerID`, `arbiter.SignedClaimsFeature`, `arbiter.LocalNodeFeatures`); Task 3 (`authority.SignSNodeMessageAt`, `VerifySNodeMessage`, `SignVerifierMessage`, `VerifyVerifierMessage`, `MessageContext`, `MarkActiveBody`, `TablePurgedBody`, the `wire.*ToRequest` converters, `wire.RecordTablePurged` signature fields, `authority/authoritytest`).
- Produces (package `dataplane/tableset`): `Config.Owner *uint64` — non-nil restricts creation, verification, purging, purge reports and the decommissioned-replica sweep to keys whose live incarnation `wire.TableRegistrySnapshot.Owner` assigns to it (nil owner = founding indexer); nil reconciles every incarnation (verifiers, legacy SNodes). (CONTRACT §2 calls it `tableset.Options.Owner`; the reconciler's option struct is `Config`, see "Contract conflicts".)
- Produces (package `dataplane`):
  - `func (c *Client) SubmitTablePurgedReport(ctx context.Context, report wire.RecordTablePurged) error` (`SubmitTablePurged` becomes its unsigned wrapper, unchanged API)
  - `type PurgeReporter struct{ Client *Client; Sign func(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error) }` with `func (p PurgeReporter) SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error` and `func (p PurgeReporter) PurgeNodeSet(ctx context.Context) ([]string, error)` (a `tableset.Arbiter`; nil `Sign` reports unsigned)
  - `func GenesisSnapshotID(networkID, schemaSnapshotID, executorProfileID string, genesis []payloadexec.TableSchema) (string, error)` — the arbiter's derivation (`arbiter:cmd/arbiter/genesis.go:16-24`)
- Produces (package `snode`):
  - `Config.IndexerID uint64` (the SI indexer this SNode serves); `Config.GenesisSnapshotID string` (bound into every signed message; empty derives it from `Tables` with `dataplane.GenesisSnapshotID`, which only the founding indexer's SNode can; a signing SNode with no genesis table must set it)
  - `Deps.ClaimSigner *authority.Signer` (the indexer key; nil = the legacy unsigned SNode, byte for byte, which also owns every incarnation); `Deps.Now func() time.Time` (clock of the `registration_seq` floor and of token `iat`; nil = `time.Now`) — the clock injection point
  - `var ErrTableNotOwned` (wraps `ErrSchemaUnknown`)
  - `state.json` gains `registration_seq` (`localState.RegistrationSeq uint64`, JSON `registration_seq,omitempty`). Before each `Register` call the SNode computes `next := max(persisted+1, uint64(now().UnixMilli()))` (amended rule, CONTRACT §2/§3a: a node whose state directory was lost still moves forward without manual intervention), persists `next` durably (temp file + fsync + rename + directory fsync, `persistStateLocked`, `snode/state.go:413-447`) before the first `RegisterNode` leaves, and uses that one value for every retry of the registration and for the following `MarkActive`
  - with a claim signer: RegisterNode, MarkActive, RCs, promotion and cleanup acknowledgements and purge reports are signed (`SignSNodeMessageAt` at `Deps.Now`), registration advertises `signed_claims_v1`, and the reconciler, source-claim root, fresh intake, prepared lookup, promoted-unsafe reads and promotion/cleanup handling cover only `IndexerID`'s tables; an empty genesis table set is allowed when the SNode also follows the registry
- Produces (package `verifier`): `Config.StateDir string` (holds `registration.json`; the verifier had no on-disk state, so this is new; empty keeps the sequence in memory, still clock-floored), `Config.GenesisSnapshotID string` (empty derives it from `Tables`), `Deps.Now func() time.Time`; RegisterNode, MarkActive and purge reports are always signed with the evidence key (`SignVerifierMessage`) and registration advertises `signed_claims_v1`; same `registration_seq` rule as the SNode.
- Release ledger: `CORE_TAG` (Step 17).

- [ ] **Step 1: Write the failing reconciler owner-filter tests**

Create `dataplane/tableset/owner_test.go` (pure, no ClickHouse):

```go
package tableset

import (
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// nopConnT satisfies clickhouse.Conn for constructor tests; it is never called.
type nopConnT struct{ clickhouse.Conn }

func TestOwnsFollowsTheRegistryOwner(t *testing.T) {
	zero, one := uint64(0), uint64(1)
	snap := wire.TableRegistrySnapshot{Params: arbiter.TableRegistryParams{SIIndexerID: 0}}
	unowned := wire.TableIncarnation{Seq: 1, DatabaseID: "db", TableID: "a"}
	founding := wire.TableIncarnation{Seq: 2, DatabaseID: "db", TableID: "b", OwnerIndexerID: &zero}
	ones := wire.TableIncarnation{Seq: 3, DatabaseID: "db", TableID: "c", OwnerIndexerID: &one}
	for _, tc := range []struct {
		name  string
		owner *uint64
		inc   wire.TableIncarnation
		want  bool
	}{
		{"no filter (verifier, legacy SNode) keeps another indexer's key", nil, ones, true},
		{"no filter keeps a key recorded before the activation", nil, unowned, true},
		{"the founding indexer owns a key recorded before the activation", &zero, unowned, true},
		{"the founding indexer owns its recorded key", &zero, founding, true},
		{"the founding indexer skips indexer 1's key", &zero, ones, false},
		{"indexer 1 owns its key", &one, ones, true},
		{"indexer 1 skips a key recorded before the activation", &one, unowned, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &Reconciler{cfg: Config{Owner: tc.owner}}
			if got := r.owns(snap, tc.inc); got != tc.want {
				t.Fatalf("owns = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestNewCopiesTheOwner(t *testing.T) {
	owner := uint64(1)
	r, err := New(Config{Pinned: ddl.Pinned{UnsafeDB: "u", SafeDB: "s", PromoteDB: "p", NodeID: "n"}, Owner: &owner}, Deps{Conn: nopConnT{}})
	if err != nil {
		t.Fatal(err)
	}
	owner = 2
	if *r.cfg.Owner != 1 {
		t.Fatal("the reconciler aliases the caller's owner")
	}
}
```

Create `dataplane/tableset/owner_ch_test.go` (ClickHouse-gated like `reconciler_ch_test.go`; reuses `requireCH`, `requireReplicaCH`, `testPinned`, `newFakeView`, `fakeArbiter`, `newReconciler`, `schemaFor`, `chainInc`, `localComments`, `mustReconcile`, `stateCount`):

```go
package tableset

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// setSnapshot installs s, with its owners, as the next version.
func (v *fakeView) setSnapshot(s wire.TableRegistrySnapshot) {
	v.mu.Lock()
	defer v.mu.Unlock()
	s.Version = v.snap.Version + 1
	v.snap, v.enabled = s, true
	close(v.changed)
	v.changed = make(chan struct{})
}

func TestReconciler_OwnerFilterMaterialisesAndPurgesOnlyOwnedKeys(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	one, two := uint64(1), uint64(2)
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.Owner = &one })
	mine, theirs, theirsPurging := schemaFor(t, "mine"), schemaFor(t, "theirs"), schemaFor(t, "gone")
	incMine := chainInc(t, 1, mine, wire.TableStatusActive)
	incMine.OwnerIndexerID = &one
	incTheirs := chainInc(t, 2, theirs, wire.TableStatusActive)
	incTheirs.OwnerIndexerID = &two
	incGone := chainInc(t, 3, theirsPurging, wire.TableStatusPurging)
	incGone.OwnerIndexerID = &two
	view.setSnapshot(wire.TableRegistrySnapshot{Seeded: true, Incarnations: []wire.TableIncarnation{incMine, incTheirs, incGone}})
	mustReconcile(t, r)
	if !r.Ready(mine.TableID) || len(localComments(t, conn, p, mine.TableID)) != 3 {
		t.Fatal("the owned table must be created and verified")
	}
	if r.Ready(theirs.TableID) || len(localComments(t, conn, p, theirs.TableID)) != 0 {
		t.Fatal("another indexer's table must never be created on this SNode")
	}
	if got := arb.reported(); len(got) != 0 {
		t.Fatalf("reported %v: another indexer's purge is never this SNode's to report", got)
	}
	if st := r.Stats(); stateCount(st) != 1 || st.States[StateReady] != 1 {
		t.Fatalf("stats = %+v: only the owned key has a state", st)
	}
}

// TestReconciler_OwnerFilterNeverSweepsAnotherOwnersKeeperPath: the Keeper
// root is shared by every owner (spec D11), so a source's sweep of
// decommissioned replicas must stay on its own keys.
func TestReconciler_OwnerFilterNeverSweepsAnotherOwnersKeeperPath(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t, conn, replicaConn)
	view, arb := newFakeView(), &fakeArbiter{}
	one, two := uint64(1), uint64(2)
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned, c.Owner = true, &one })
	theirs := schemaFor(t, "theirs")
	other := p
	other.UnsafeDB, other.SafeDB, other.PromoteDB, other.NodeID = p.UnsafeDB+"_o", p.SafeDB+"_o", p.PromoteDB+"_o", "snode-other"
	t.Cleanup(func() {
		for _, db := range []string{other.UnsafeDB, other.SafeDB, other.PromoteDB} {
			_ = replicaConn.Exec(ctx, "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	if err := ddl.EnsureTable(ctx, replicaConn, other, theirs, 1, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, fmt.Sprintf("DETACH TABLE %s.%s", other.UnsafeDB, ddl.CHTableName(theirs.TableID))); err != nil {
		t.Fatal(err)
	}
	inc := chainInc(t, 1, theirs, wire.TableStatusPurged)
	inc.OwnerIndexerID = &two
	view.setSnapshot(wire.TableRegistrySnapshot{Seeded: true, Incarnations: []wire.TableIncarnation{inc}})
	mustReconcile(t, r)
	replicas, err := ddl.KeeperReplicas(ctx, conn, p, theirs.TableID)
	if err != nil || !slices.Equal(replicas, []string{"snode-other"}) {
		t.Fatalf("replicas = %v, %v: another owner's Keeper path must be left alone", replicas, err)
	}
}
```

(Without the owner filter the second test fails: `sweepPurgedKeeperPaths` would treat the detached `snode-other` replica of the Purged key as decommissioned, the mechanism `TestReconciler_SweepsDecommissionedReplicas` relies on.)

- [ ] **Step 2: Run them and watch them fail**

Run: `cd "$W" && bazel run //:gazelle && bazel test //dataplane/tableset:tableset_test --test_output=errors`
Expected: build failure: `unknown field Owner in struct literal of type Config`, `r.owns undefined`.

- [ ] **Step 3: Implement the owner filter**

In `dataplane/tableset/reconciler.go` append to `Config` (after `SweepDecommissioned`, line 73):

```go
	// Owner, when set, restricts this reconciler to the incarnations owned
	// by that SI indexer (housegate spec 2026-10-10 §8): an SNode creates,
	// verifies, purges, reports and sweeps only its own tables. An
	// incarnation recorded before the signed-claims activation names no
	// owner and belongs to the founding indexer (TableRegistryParams.
	// SIIndexerID). Nil reconciles every incarnation: verifiers, and SNodes
	// without a claim signer. It has no effect while the registry is disabled.
	Owner *uint64
```

in `New` copy it before the struct literal (after the interval default, line 155):

```go
	if cfg.Owner != nil {
		owner := *cfg.Owner
		cfg.Owner = &owner
	}
```

replace the head of the classification loop in `reconcileRegistry` (lines 381-390):

```go
	seen := map[string]bool{}
	notOwned := map[string]bool{}
	var fatal, genesisErrs []error
	for _, inc := range snap.Incarnations {
		key := inc.Key()
		if seen[key] || notOwned[key] {
			continue
		}
		live := snap.Live(key)
		if !r.owns(snap, *live) {
			// Another SI indexer's table (spec §8): never created, verified,
			// purged, reported or swept here; forgetAbsent drops any status
			// and its local tables, if any, are reported as unknown.
			notOwned[key] = true
			continue
		}
		seen[key] = true
		physical := ddl.CHTableName(key)
```

and add after `incarnation` (line 642):

```go
// owns reports whether this reconciler serves inc's key (Config.Owner).
func (r *Reconciler) owns(snap wire.TableRegistrySnapshot, inc wire.TableIncarnation) bool {
	return r.cfg.Owner == nil || snap.Owner(inc) == *r.cfg.Owner
}
```

Because `history` (Purged / Refused / Legacy keys) is built inside that loop, `sweepPurgedKeeperPaths` and `dropLeftover` only ever see owned keys, and `purge` (with `sweep`) only runs for owned Purging keys.

- [ ] **Step 4: Run and commit**

Run: `cd "$W" && bazel test //dataplane/tableset:tableset_test --test_output=errors`
Expected: PASS (`TestOwnsFollowsTheRegistryOwner`, `TestNewCopiesTheOwner`; the ClickHouse tests SKIP without `ARBITER_CH_INTEGRATION`). With the docker pair of README § Build and test running: `ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1 CH_ADDR=127.0.0.1:9000 CH_REPLICA_ADDR=127.0.0.1:9001 bazel test //dataplane/tableset:tableset_test --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=ARBITER_CH_REPLICA --test_env=CH_ADDR --test_env=CH_REPLICA_ADDR --test_timeout=900 --test_output=errors` passes both owner tests and every existing reconciler test (CI's `integration-clickhouse` job runs exactly this target).

```bash
cd "$W" && git add dataplane/tableset && git commit -m "feat(tableset): owner filter for SNode reconcilers

Config.Owner restricts creation, verification, purge reports and the
decommissioned-replica sweep to one SI indexer's tables; an unowned
(pre-activation) incarnation belongs to the founding indexer; nil keeps
every incarnation for verifiers and legacy SNodes (housegate spec
2026-10-10 §8).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Write the failing data-plane client tests**

Create `dataplane/table_purge_test.go` (reuses `newFakeRegistry`, `startRegistryPeer`, `newTestClient` from `dataplane/registry_follower_test.go`):

```go
package dataplane

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/wire"
)

func TestSubmitTablePurgedReportCarriesSignatures(t *testing.T) {
	r := newFakeRegistry()
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.SubmitTablePurgedReport(ctx, wire.RecordTablePurged{NodeID: "s1", IncarnationSeq: 4, SignerJWS: "h.p.s"}); err != nil {
		t.Fatal(err)
	}
	signed := PurgeReporter{Client: c, Sign: func(node string, seq uint64) (wire.RecordTablePurged, error) {
		return wire.RecordTablePurged{NodeID: node, IncarnationSeq: seq, Ed25519Signature: "ab"}, nil
	}}
	if err := signed.SubmitTablePurged(ctx, "v1", 9); err != nil {
		t.Fatal(err)
	}
	if err := (PurgeReporter{Client: c}).SubmitTablePurged(ctx, "v2", 9); err != nil {
		t.Fatal(err)
	}
	failing := PurgeReporter{Client: c, Sign: func(string, uint64) (wire.RecordTablePurged, error) {
		return wire.RecordTablePurged{}, errors.New("no key")
	}}
	if err := failing.SubmitTablePurged(ctx, "v1", 9); err == nil {
		t.Fatal("a signing failure must not send an unsigned report")
	}
	lying := PurgeReporter{Client: c, Sign: func(string, uint64) (wire.RecordTablePurged, error) {
		return wire.RecordTablePurged{NodeID: "other", IncarnationSeq: 9}, nil
	}}
	if err := lying.SubmitTablePurged(ctx, "v1", 9); err == nil {
		t.Fatal("a signed report naming another node must be refused")
	}
	r.mu.Lock()
	purges := append([]*pb.RecordTablePurgedCmd(nil), r.purges...)
	r.mu.Unlock()
	if len(purges) != 3 || purges[0].GetSignerJws() != "h.p.s" || purges[1].GetNodeId() != "v1" || purges[1].GetEd25519Signature() != "ab" ||
		purges[2].GetSignerJws() != "" || purges[2].GetEd25519Signature() != "" {
		t.Fatalf("purges = %v", purges)
	}
}

func TestGenesisSnapshotIDIsTheArbitersDerivation(t *testing.T) {
	tables := []payloadexec.TableSchema{{TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}}
	id, err := GenesisSnapshotID("devnet2", "schema-genesis", "housegate-replay-mvp-v0", tables)
	if err != nil || id == "" {
		t.Fatalf("GenesisSnapshotID = %q, %v", id, err)
	}
	// arbiter cmd/arbiter/genesis.go loadGenesisManifest derives exactly this.
	m, err := payloadexec.New("devnet2", tables...).GenesisSnapshot(0, "schema-genesis", "housegate-replay-mvp-v0")
	if err != nil || m.SnapshotID != id {
		t.Fatalf("arbiter derivation = %q (%v), helper = %q", m.SnapshotID, err, id)
	}
	if other, _ := GenesisSnapshotID("devnet2", "schema-genesis", "housegate-replay-mvp-v0", nil); other == id {
		t.Fatal("another genesis table set must derive another id")
	}
}
```

- [ ] **Step 6: Implement them**

In `dataplane/table_purge.go` add `"github.com/sentioxyz/arbiter-core/wire"` to the imports and replace `SubmitTablePurged` (lines 17-37):

```go
// SubmitTablePurged reports, unsigned, that nodeID dropped its hg_* tables
// for one Purging incarnation. See SubmitTablePurgedReport for the answers.
func (c *Client) SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error {
	return c.SubmitTablePurgedReport(ctx, wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq})
}

// SubmitTablePurgedReport submits a purge report that may carry the
// reporter's signature (housegate spec 2026-10-10 §6.5). It returns nil on
// Ack (including the idempotent already-recorded and already-Purged
// answers), an error wrapping ErrTableNotPurging on the leader's
// FAILED_PRECONDITION, and the gRPC error otherwise (an arbiter without the
// RPC answers Unimplemented; the caller retries it like a transport error).
func (c *Client) SubmitTablePurgedReport(ctx context.Context, report wire.RecordTablePurged) error {
	var precondition error
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).SubmitTablePurged(ctx, wire.RecordTablePurgedToRequest(report))
		if st, ok := leaderPrecondition(err); ok {
			precondition = fmt.Errorf("%w: %s", ErrTableNotPurging, st.Message())
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return precondition
}

// PurgeReporter is the tableset.Arbiter of a signing role: it signs every
// purge report before submitting it. A nil Sign reports unsigned, exactly
// like Client.
type PurgeReporter struct {
	Client *Client
	Sign   func(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error)
}

// SubmitTablePurged signs and submits one purge report.
func (p PurgeReporter) SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error {
	report := wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq}
	if p.Sign != nil {
		signed, err := p.Sign(nodeID, incarnationSeq)
		if err != nil {
			return fmt.Errorf("sign table purged report: %w", err)
		}
		if signed.NodeID != nodeID || signed.IncarnationSeq != incarnationSeq {
			return fmt.Errorf("sign table purged report: signed %s/%d, want %s/%d", signed.NodeID, signed.IncarnationSeq, nodeID, incarnationSeq)
		}
		report = signed
	}
	return p.Client.SubmitTablePurgedReport(ctx, report)
}

// PurgeNodeSet is Client.PurgeNodeSet.
func (p PurgeReporter) PurgeNodeSet(ctx context.Context) ([]string, error) {
	return p.Client.PurgeNodeSet(ctx)
}
```

Create `dataplane/genesis.go`:

```go
package dataplane

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// GenesisSnapshotID derives the network's genesis snapshot id the way the
// arbiter does (arbiter cmd/arbiter/genesis.go loadGenesisManifest): the
// SnapshotID of the sealed empty block-0 snapshot over the genesis table set.
// Signed data-plane messages bind it (housegate spec 2026-10-10 §6.5). Only a
// node holding the network's whole genesis set (verifiers, the founding
// indexer's SNode) can derive it; any other node must be configured with it.
func GenesisSnapshotID(networkID, schemaSnapshotID, executorProfileID string, genesis []payloadexec.TableSchema) (string, error) {
	m, err := payloadexec.New(networkID, genesis...).GenesisSnapshot(0, schemaSnapshotID, executorProfileID)
	if err != nil {
		return "", fmt.Errorf("derive genesis snapshot id: %w", err)
	}
	return m.SnapshotID, nil
}
```

- [ ] **Step 7: Run and commit**

Run: `cd "$W" && bazel run //:gazelle && bazel test //dataplane:dataplane_test --test_output=errors`
Expected: PASS, including the unchanged `TestSubmitTablePurged_PreconditionIsNotSuccessAndNotALeaderMiss`, `TestSubmitTablePurged_FollowsNotLeader` and `TestSubmitTablePurged_UnimplementedIsReturned`.

```bash
cd "$W" && git add dataplane && git commit -m "feat(dataplane): signed purge reports and the genesis snapshot id

SubmitTablePurgedReport carries a reporter signature, PurgeReporter signs
each report for a tableset reconciler, GenesisSnapshotID reproduces the
arbiter's genesis derivation for the message context (housegate spec
2026-10-10 §6.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 8: Write the failing SNode tests**

Append to `snode/state_test.go` (style of `TestStateStore_RoundTripsDurableState`):

```go
func TestStateStore_RegistrationSeqIsDurableAndSurvivesEveryTransition(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := st.NextRegistrationSeq(100); err != nil || got != 100 {
		t.Fatalf("first = %d, %v: want the clock floor", got, err)
	}
	if got, err := st.NextRegistrationSeq(50); err != nil || got != 101 {
		t.Fatalf("second = %d, %v: a slower clock must still advance", got, err)
	}
	// RecordAck rebuilds the whole state through cloneLocalState: the seq
	// must survive it.
	k := partitionKey{Table: "db.t", Partition: "p0"}
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 7, TableID: "db.t", PartitionID: "p0", Applied: true}
	if err := st.RecordAck(k, 7, ack, "0xbase", "snap-7"); err != nil {
		t.Fatal(err)
	}
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := reopened.NextRegistrationSeq(0); err != nil || got != 102 {
		t.Fatalf("after reopen = %d, %v: want 102", got, err)
	}
}
```

Replace `snode/registration_test.go`:

```go
package snode

import (
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

// An SNode without a claim signer sends the legacy requests; it advertises
// client_lanes_v1 but never signed_claims_v1, so the activation gate (spec
// §6.7) cannot count an SNode that cannot sign.
func TestSNodeRegistrationWithoutAClaimSigner(t *testing.T) {
	req, ref, err := (&Role{cfg: Config{NodeID: "s1"}}).registrationRequests()
	if err != nil {
		t.Fatal(err)
	}
	if req.GetNodeId() != "s1" || !slices.Equal(req.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}) || ref.GetNodeId() != "s1" {
		t.Fatalf("registration = %v, activation = %v", req, ref)
	}
	if !slices.Contains(req.GetFeatures(), arbiter.ClientLanesFeature) || slices.Contains(req.GetFeatures(), arbiter.SignedClaimsFeature) {
		t.Fatalf("features = %v", req.GetFeatures())
	}
	if req.GetRegistrationSeq() != 0 || req.GetSignerJws() != "" || ref.GetRegistrationSeq() != 0 || ref.GetSignerJws() != "" {
		t.Fatalf("legacy requests carry signed-claims fields: %v / %v", req, ref)
	}
}
```

Create `snode/claims_test.go` (reuses `testConfigS`, `intakeEnvelope`, `intakeSchema`, `testRevision`, `testRecord`, `newFakeRegistryS`, `chainSchemaS`, `chainIncarnationS`, `genesisIncarnationS`, `nopConnS`, `openStateStore`, `openIntakeJournal`):

```go
package snode

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// claimsFakeS records every request an SNode sends to the arbiter.
// failRegistrations makes the first RegisterNode calls answer Unavailable
// after recording them (an uncertain commit).
type claimsFakeS struct {
	pb.UnimplementedMembershipServer
	pb.UnimplementedSourceClaimsServer
	pb.UnimplementedPromotionGatewayServer

	mu                sync.Mutex
	failRegistrations int
	regs              []*pb.NodeRegistration
	marks             []*pb.NodeRef
	claims            []*pb.RCRecord
	acks              []*pb.PromotionAck
	cleanups          []*pb.CleanupAck
}

func (f *claimsFakeS) RegisterNode(_ context.Context, m *pb.NodeRegistration) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.regs = append(f.regs, m)
	if f.failRegistrations > 0 {
		f.failRegistrations--
		return nil, status.Error(codes.Unavailable, "leader lost before the reply")
	}
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) MarkActive(_ context.Context, m *pb.NodeRef) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, m)
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) RegisterResultClaim(_ context.Context, m *pb.RCRecord) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, m)
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) AckPromotion(_ context.Context, m *pb.PromotionAck) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, m)
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) AckCleanup(_ context.Context, m *pb.CleanupAck) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleanups = append(f.cleanups, m)
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) registrations() ([]*pb.NodeRegistration, []*pb.NodeRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.regs), slices.Clone(f.marks)
}

func (f *claimsFakeS) sent() ([]*pb.RCRecord, []*pb.PromotionAck, []*pb.CleanupAck) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.claims), slices.Clone(f.acks), slices.Clone(f.cleanups)
}

func startClaimsFakeS(t *testing.T, f *claimsFakeS) *dataplane.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterMembershipServer(srv, f)
	pb.RegisterSourceClaimsServer(srv, f)
	pb.RegisterPromotionGatewayServer(srv, f)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// fixtureMillis is authoritytest.Iat in milliseconds.
const fixtureMillis = int64(1_760_054_400_000)

func clockS(ms *int64) func() time.Time { return func() time.Time { return time.UnixMilli(*ms) } }

// signingConfigS is testConfigS as indexer 1's SNode in the authoritytest
// context.
func signingConfigS(t *testing.T) Config {
	cfg := testConfigS(t)
	cfg.NodeID = authoritytest.SNodeNodeID1
	cfg.NetworkID = authoritytest.NetworkID
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.IndexerID = 1
	cfg.GenesisSnapshotID = authoritytest.GenesisSnapshotID
	cfg.AuthorityAddresses = []string{authoritytest.AuthorityAddr}
	return cfg
}

// setSnapshot installs s, with owners and si_indexers, as the next version.
func (v *fakeRegistryS) setSnapshot(s wire.TableRegistrySnapshot) {
	v.mu.Lock()
	defer v.mu.Unlock()
	s.Version = v.snap.Version + 1
	v.snap, v.enabled = s, true
	close(v.changed)
	v.changed = make(chan struct{})
}

func TestRegister_SignsRegistrationAndMarkActiveWithTheIndexerKey(t *testing.T) {
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if len(regs) != 1 || len(marks) != 1 {
		t.Fatalf("requests: %d registrations, %d activations", len(regs), len(marks))
	}
	reg, mark := regs[0], marks[0]
	// Same body, seq (the clock floor), context and iat as the pinned vectors.
	if reg.GetRegistrationSeq() != authoritytest.RegistrationSeq || mark.GetRegistrationSeq() != authoritytest.RegistrationSeq ||
		reg.GetSignerJws() != authoritytest.SNodeRegistrationJWS || mark.GetSignerJws() != authoritytest.SNodeMarkActiveJWS {
		t.Fatalf("registration %v / activation %v differ from the authoritytest vectors", reg, mark)
	}
	// The FSM verifies the registration it decodes from the Raft command.
	if err := authority.VerifySNodeMessage(authority.SNodeMessageRegistration, authoritytest.Context(), wire.RegistrationFromPB(reg),
		reg.GetSignerJws(), authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(reg.GetFeatures(), arbiter.SignedClaimsFeature) || reg.GetEd25519Signature() != "" {
		t.Fatalf("features = %v: a signing SNode advertises signed_claims_v1", reg.GetFeatures())
	}
	reopened, err := openStateStore(role.cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if next, err := reopened.NextRegistrationSeq(0); err != nil || next != authoritytest.RegistrationSeq+1 {
		t.Fatalf("the registration_seq was not durable: next = %d, %v", next, err)
	}
}

// TestRegister_SeqStaysAboveAnEarlierRegistrationAfterTheStateFileIsLost is
// the amended rule (CONTRACT §3a): next = max(persisted+1, now in ms), so a
// node whose state.json was lost still registers above its last seq.
func TestRegister_SeqStaysAboveAnEarlierRegistrationAfterTheStateFileIsLost(t *testing.T) {
	fake := &claimsFakeS{}
	client := startClaimsFakeS(t, fake)
	cfg := signingConfigS(t)
	signer := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
	now := fixtureMillis
	first, err := New(cfg, Deps{Client: client, ClaimSigner: signer, Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(cfg.StateDir, "state.json")); err != nil {
		t.Fatal(err)
	}
	now = fixtureMillis + 5 // the restart took five milliseconds
	second, err := New(cfg, Deps{Client: client, ClaimSigner: signer, Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if len(regs) != 2 || regs[0].GetRegistrationSeq() != uint64(fixtureMillis) || regs[1].GetRegistrationSeq() != uint64(fixtureMillis+5) ||
		marks[1].GetRegistrationSeq() != regs[1].GetRegistrationSeq() {
		t.Fatalf("seqs = %v / %v: the second registration must carry a larger seq", regs, marks)
	}
}

func TestRegister_SeqNeverGoesBackWithTheClock(t *testing.T) {
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = fixtureMillis - 60_000 // the clock stepped back a minute
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, _ := fake.registrations()
	if len(regs) != 2 || regs[1].GetRegistrationSeq() != uint64(fixtureMillis)+1 {
		t.Fatalf("seqs = %v: want persisted+1 when the clock is behind", regs)
	}
}

// TestRegister_RetriesReuseOneSeqAndSignature documents why the FSM must
// treat an exact duplicate registration as idempotent (see Contract
// conflicts): WithLeaderRetry resends the identical request after an
// uncertain commit.
func TestRegister_RetriesReuseOneSeqAndSignature(t *testing.T) {
	fake := &claimsFakeS{failRegistrations: 1}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if len(regs) != 2 || regs[0].GetRegistrationSeq() != regs[1].GetRegistrationSeq() || regs[0].GetSignerJws() != regs[1].GetSignerJws() ||
		len(marks) != 1 || marks[0].GetRegistrationSeq() != regs[1].GetRegistrationSeq() {
		t.Fatalf("registrations %v, activations %v: one Register call uses one seq", regs, marks)
	}
}

func TestRegister_WithoutAClaimSignerSendsTheLegacyRequests(t *testing.T) {
	fake := &claimsFakeS{}
	role, err := New(testConfigS(t), Deps{Client: startClaimsFakeS(t, fake)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if regs[0].GetRegistrationSeq() != 0 || regs[0].GetSignerJws() != "" || marks[0].GetRegistrationSeq() != 0 || marks[0].GetSignerJws() != "" ||
		slices.Contains(regs[0].GetFeatures(), arbiter.SignedClaimsFeature) {
		t.Fatalf("legacy SNode sent %v / %v", regs[0], marks[0])
	}
	if _, err := os.Stat(filepath.Join(role.cfg.StateDir, "state.json")); !os.IsNotExist(err) {
		t.Fatalf("a legacy registration must not write state: %v", err)
	}
}

func TestRegisterPreparedClaim_SignsTheResultClaim(t *testing.T) {
	fake := &claimsFakeS{}
	cfg := signingConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{intakeSchema()}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, err := New(cfg, Deps{Client: startClaimsFakeS(t, fake), ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	env := intakeEnvelope([]byte("native"))
	env.NetworkID = cfg.NetworkID
	env.SchemaHash = payloadexec.TableSchemaHash(cfg.NetworkID, intakeSchema())
	rc := arbiter.RCRecord{StatementID: env.StatementID, SourceNode: cfg.NodeID, CandidateParts: []arbiter.CandidatePart{}, SourceClaimRoot: "0xroot"}
	rec := intakeRecord{StatementID: env.StatementID.Flat(), Lifecycle: LifecyclePreparing, Envelope: env,
		PayloadEncoding: env.PayloadFormat, Revision: testRevision, ExpectedRowCount: 1}
	if err := role.journal.save(rec); err != nil {
		t.Fatal(err)
	}
	rec.Lifecycle, rec.RC = LifecycleUnsafeWritten, &rc
	rec.Result = &PreparedLocalResult{StatementID: rec.StatementID, SourceNode: cfg.NodeID, Lifecycle: LifecycleUnsafeWritten}
	if err := role.journal.save(rec); err != nil {
		t.Fatal(err)
	}
	if out, err := role.RegisterPreparedClaim(context.Background(), rec.StatementID); err != nil || out.Category != ClaimAccepted {
		t.Fatalf("register: %+v, %v", out, err)
	}
	claims, _, _ := fake.sent()
	// The FSM verifies the claim it decodes: [] candidate parts decode as nil.
	if len(claims) != 1 || authority.VerifySNodeMessage(authority.SNodeMessageResultClaim, authoritytest.Context(),
		wire.RCFromPB(claims[0]), claims[0].GetSourceJws(), authoritytest.IndexerAddr1) != nil {
		t.Fatalf("claims = %v: want one RC signed by indexer 1's key", claims)
	}
}

func TestPromotionAndCleanupAcksAreSigned(t *testing.T) {
	fake := &claimsFakeS{}
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake), ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	authoritySigner := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	// A base the SNode does not hold: the base-CAS refusal acknowledges
	// Applied:false without touching ClickHouse. It must be signed too, since
	// the FSM consumes a promotion on it (spec §6.6).
	promote := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "all", PromotionSeq: 7, BaseSafeSnapshotID: "snap", BasePartitionRoot: "0xnot-the-local-base"}
	jws, err := authoritySigner.SignPromotion(promote)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handlePromote(context.Background(), wire.PromoteToPB(promote), jws); err != nil {
		t.Fatal(err)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 7}
	cleanupJWS, err := authoritySigner.SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), cleanupJWS); err != nil {
		t.Fatal(err)
	}
	_, acks, cleanups := fake.sent()
	if len(acks) != 1 || acks[0].GetApplied() || len(cleanups) != 1 {
		t.Fatalf("acks %v, cleanups %v", acks, cleanups)
	}
	ctx := authoritytest.Context()
	if err := authority.VerifySNodeMessage(authority.SNodeMessagePromotionAck, ctx, wire.PromotionAckFromPB(acks[0]), acks[0].GetSourceJws(), authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
	if err := authority.VerifySNodeMessage(authority.SNodeMessageCleanupAck, ctx, wire.CleanupAckFromPB(cleanups[0]), cleanups[0].GetSourceJws(), authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
}

func TestTablePurgedReportIsSignedWithTheIndexerKey(t *testing.T) {
	now := fixtureMillis
	r := &Role{cfg: signingConfigS(t), genesisID: authoritytest.GenesisSnapshotID,
		d: Deps{ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)}}
	report, err := r.signTablePurged(authoritytest.SNodeNodeID1, 5)
	if err != nil || report != (wire.RecordTablePurged{NodeID: authoritytest.SNodeNodeID1, IncarnationSeq: 5, SignerJWS: authoritytest.SNodeTablePurgedJWS}) {
		t.Fatalf("report = %+v, %v: want the authoritytest vector", report, err)
	}
	if _, ok := r.purgeArbiter().(dataplane.PurgeReporter); !ok {
		t.Fatal("a signing SNode must report purges through the signing reporter")
	}
	r.d.ClaimSigner = nil
	if _, ok := r.purgeArbiter().(*dataplane.Client); !ok {
		t.Fatal("a legacy SNode keeps reporting through the plain client")
	}
}

func TestNew_SigningSNodeGenesisSnapshotID(t *testing.T) {
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	signer := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
	derived := signingConfigS(t)
	derived.GenesisSnapshotID = ""
	role, err := New(derived, Deps{Client: client, ClaimSigner: signer})
	if err != nil {
		t.Fatal(err)
	}
	want, err := dataplane.GenesisSnapshotID(derived.NetworkID, derived.SchemaSnapshotID, derived.ExecutorProfileID, derived.Tables)
	if err != nil || role.genesisID != want {
		t.Fatalf("genesis id = %q, want the arbiter's derivation %q (%v)", role.genesisID, want, err)
	}
	// An SNode owning no genesis table (a non-founding indexer) may run with
	// an empty genesis set only as a signing registry follower, and must be
	// told the genesis snapshot id.
	empty := signingConfigS(t)
	empty.Tables, empty.SchemaRoot = nil, payloadexec.SchemaRoot(empty.NetworkID, nil)
	empty.SchemaSource = ddl.SchemaSourceNetworkState
	follower := Deps{Client: client, Conn: nopConnS{}, Registry: newFakeRegistryS(), ClaimSigner: signer}
	if _, err := New(empty, follower); err != nil {
		t.Fatalf("a signing registry follower may own no genesis table: %v", err)
	}
	for name, d := range map[string]Deps{
		"no claim signer": {Client: client, Conn: nopConnS{}, Registry: newFakeRegistryS()},
		"no registry":     {Client: client, ClaimSigner: signer},
	} {
		if _, err := New(empty, d); err == nil || !strings.Contains(err.Error(), "at least one table schema") {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	noID := empty
	noID.GenesisSnapshotID = ""
	if _, err := New(noID, follower); err == nil || !strings.Contains(err.Error(), "genesis snapshot id") {
		t.Fatalf("missing genesis snapshot id: err = %v", err)
	}
}

// ownedSnapshotS: db.t (genesis) and db.theirs belong to indexer 0, db.mine
// to indexer 1, after the signed-claims activation.
func ownedSnapshotS(t *testing.T, cfg Config) wire.TableRegistrySnapshot {
	zero, one := uint64(0), uint64(1)
	genesis := genesisIncarnationS(1, cfg.Tables[0])
	genesis.OwnerIndexerID = &zero
	mine := chainIncarnationS(t, 2, chainSchemaS("db.mine"), wire.TableStatusActive)
	mine.OwnerIndexerID = &one
	theirs := chainIncarnationS(t, 3, chainSchemaS("db.theirs"), wire.TableStatusActive)
	theirs.OwnerIndexerID = &zero
	return wire.TableRegistrySnapshot{Seeded: true, SeededIndexers: []uint64{0},
		SIIndexers:   []arbiter.SIIndexerEntry{authoritytest.SIIndexerEntry0(), authoritytest.SIIndexerEntry1(200)},
		Incarnations: []wire.TableIncarnation{genesis, mine, theirs}}
}

func TestOwnerScopedReads(t *testing.T) {
	cfg := signingConfigS(t)
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	view := newFakeRegistryS()
	view.setSnapshot(ownedSnapshotS(t, cfg))
	r := &Role{cfg: cfg, state: st, journal: journal, d: Deps{Registry: view, ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)}}

	hash := payloadexec.TableSchemaHash("testnet", chainSchemaS("db.mine"))
	_, want, err := replay.AssembleStateRoot(cfg.SchemaSnapshotID, payloadexec.SchemaRootFromHashes(map[string]string{"db.mine": hash}),
		cfg.ExecutorProfileID, []replay.TableManifest{{TableID: "db.mine", SchemaHash: hash}})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := r.sourceClaimRoot()
	if err != nil || owned != want {
		t.Fatalf("source claim root = %s, want %s over db.mine only (%v)", owned, want, err)
	}
	theirsHash := payloadexec.TableSchemaHash("testnet", chainSchemaS("db.theirs"))
	for name, err := range map[string]error{
		"fresh intake":        r.requireAdmissible("db.theirs", theirsHash),
		"genesis of indexer 0": r.requireAdmissible("db.t", payloadexec.TableSchemaHash("testnet", cfg.Tables[0])),
	} {
		if !errors.Is(err, ErrTableNotOwned) || !errors.Is(err, ErrSchemaUnknown) {
			t.Fatalf("%s: err = %v, want ErrTableNotOwned (an ErrSchemaUnknown)", name, err)
		}
	}
	if _, err := r.PromotedUnsafeParts("db.theirs"); !errors.Is(err, ErrTableNotOwned) {
		t.Fatalf("promoted-unsafe read of another owner's table: %v", err)
	}
	if got, err := r.PromotedUnsafeParts("db.mine"); err != nil || got != nil {
		t.Fatalf("own table: %v, %v", got, err)
	}
	rec := testRecord("0xabc:7:n")
	rec.Envelope.TargetTableID = "db.theirs"
	if err := journal.save(rec); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.LookupPreparedStatement(context.Background(), rec.StatementID); !errors.Is(err, ErrTableNotOwned) {
		t.Fatalf("prepared lookup of another owner's table: %v", err)
	}
	// A legacy SNode (no claim signer) still serves every table.
	r.d.ClaimSigner = nil
	if _, err := r.PromotedUnsafeParts("db.theirs"); err != nil {
		t.Fatalf("legacy SNode: %v", err)
	}
	if legacy, err := r.sourceClaimRoot(); err != nil || legacy == owned {
		t.Fatalf("legacy root %s must cover every Active table (%v)", legacy, err)
	}
}

func TestPromotionOfAnotherOwnersTableIsDroppedWithoutAck(t *testing.T) {
	fake := &claimsFakeS{}
	cfg := signingConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	view := newFakeRegistryS()
	view.setSnapshot(ownedSnapshotS(t, cfg))
	role, err := New(cfg, Deps{Client: startClaimsFakeS(t, fake), Conn: nopConnS{}, Registry: view,
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	authoritySigner := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	promote := arbiter.PromoteSafePartition{TableID: "db.theirs", PartitionID: "all", PromotionSeq: 9, BasePartitionRoot: "0xbase"}
	jws, err := authoritySigner.SignPromotion(promote)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handlePromote(context.Background(), wire.PromoteToPB(promote), jws); err != nil {
		t.Fatal(err)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.theirs", PartitionID: "all", PromotionSeq: 9}
	cleanupJWS, err := authoritySigner.SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), cleanupJWS); err != nil {
		t.Fatal(err)
	}
	if _, acks, cleanups := fake.sent(); len(acks) != 0 || len(cleanups) != 0 {
		t.Fatalf("acknowledged another owner's work: %v / %v", acks, cleanups)
	}
}
```

- [ ] **Step 9: Run them and watch them fail**

Run: `cd "$W" && bazel run //:gazelle && bazel test //snode:snode_test --test_output=errors`
Expected: build failure: `unknown field ClaimSigner in struct literal of type Deps`, `unknown field IndexerID in struct literal of type Config`, `st.NextRegistrationSeq undefined`, `undefined: ErrTableNotOwned`, `r.registrationRequests undefined`.

- [ ] **Step 10: Implement the signing, owner-scoped SNode**

In `snode/config.go` delete the unconditional genesis check (lines 77-79, `"at least one table schema is required"`; `New` now decides it with the deps) and append to `Config` (after `DisableHardParts`, line 60):

```go
	// IndexerID is the SI indexer this SNode serves (housegate spec
	// 2026-10-10 D5, §8). With Deps.ClaimSigner it is the owner filter of the
	// reconciler and of every owner-scoped read; without one it is unused.
	IndexerID uint64
	// GenesisSnapshotID is the network's genesis snapshot id, bound into every
	// signed message (spec §6.5). Empty derives it from Tables with
	// dataplane.GenesisSnapshotID, which only the founding indexer's SNode
	// can; a signing SNode without genesis tables must set it.
	GenesisSnapshotID string
```

In `snode/state.go` append to `localState` (after `PromotionIntents`, line 66):

```go
	// RegistrationSeq is the last registration_seq this SNode reserved
	// (housegate spec 2026-10-10 §6.5); see NextRegistrationSeq.
	RegistrationSeq uint64 `json:"registration_seq,omitempty"`
```

add `RegistrationSeq: s.RegistrationSeq,` to the `cloneLocalState` literal (line 450-459; every critical transition persists a clone, so a missing copy would reset the sequence), and add:

```go
// NextRegistrationSeq reserves the registration_seq of the next Register
// call: max(last reserved + 1, nowMillis). It is durable (temp file, fsync,
// rename, directory fsync) before it is returned, so a crash after sending
// never reuses it, and the clock floor keeps it above every earlier
// registration even when state.json was lost (CONTRACT §3a).
func (st *stateStore) NextRegistrationSeq(nowMillis uint64) (uint64, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	next := max(st.s.RegistrationSeq+1, nowMillis)
	n := cloneLocalState(st.s)
	n.RegistrationSeq = next
	if err := st.persistStateLocked(n); err != nil {
		return 0, fmt.Errorf("persist registration_seq: %w", err)
	}
	return next, nil
}
```

In `snode/snode.go` append to `Deps` (after `Registry`, line 36):

```go
	// ClaimSigner is the indexer's on-chain signer key (housegate spec
	// 2026-10-10 D6). Set, the SNode signs RegisterNode, MarkActive, RCs,
	// promotion and cleanup acknowledgements and purge reports, keeps a
	// durable registration_seq, advertises signed_claims_v1, and serves only
	// Config.IndexerID's tables (spec §8). Nil is the legacy unsigned SNode,
	// byte for byte, which serves every table.
	ClaimSigner *authority.Signer
	// Now is the clock of the registration_seq floor and of token issue
	// times; nil is time.Now. Tests inject it.
	Now func() time.Time
```

append `genesisID string` (comment: `// genesisID is the genesis snapshot id signed messages bind; empty without a claim signer.`) to `Role` (after `tables`, line 91); delete `registrationRequest` (lines 140-148); and replace `New` and `Register` (lines 94-138, 150-167):

```go
func New(cfg Config, d Deps) (*Role, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("snode config: %w", err)
	}
	if len(cfg.Tables) == 0 && (d.Registry == nil || d.ClaimSigner == nil) {
		return nil, errors.New("snode config: at least one table schema is required; only a signing SNode that follows the table registry may own no genesis table")
	}
	if d.Client == nil {
		return nil, fmt.Errorf("snode: dataplane client is required")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	genesisID, err := claimGenesisSnapshotID(cfg, d.ClaimSigner != nil)
	if err != nil {
		return nil, fmt.Errorf("snode config: %w", err)
	}
	st, err := openStateStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	journal, err := openIntakeJournal(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	r := &Role{
		cfg:       cfg,
		d:         d,
		state:     st,
		journal:   journal,
		authority: authorityValidator(cfg.AuthorityAddresses),
		genesisID: genesisID,
	}
	r.ensureFn = r.ensureProtocolTablesMode
	if cfg.protocolTables != ddl.ModeOff && d.Conn != nil {
		tables, err := tableset.New(tableset.Config{
			Pinned: r.pinned(), Genesis: cfg.Tables, Interval: cfg.ProtocolTablesReconcile, SweepDecommissioned: true,
			Owner: r.ownerFilter(),
		}, tableset.Deps{
			Conn: d.Conn, Registry: d.Registry, Arbiter: r.purgeArbiter(),
			Quiescent: r.tableQuiescent, Dropped: r.forgetDroppedTable, Logger: d.Logger,
		})
		if err != nil {
			return nil, fmt.Errorf("snode: %w", err)
		}
		r.tables = tables
	} else if d.Registry != nil {
		if cfg.protocolTables == ddl.ModeOff {
			return nil, errors.New("snode: following the table registry requires managed protocol tables")
		}
		return nil, errors.New("snode: following the table registry requires a clickhouse connection")
	}
	if d.ClaimSigner != nil {
		d.Logger.Info("snode signs its arbiter messages with the indexer key",
			"node_id", cfg.NodeID, "indexer_id", cfg.IndexerID, "signer", d.ClaimSigner.Address(),
			"network_id", cfg.NetworkID, "genesis_snapshot_id", genesisID)
	}
	return r, nil
}

// Register registers this SNode and marks it active. With a claim signer both
// requests carry one durable registration_seq and the indexer key's
// signatures; WithLeaderRetry resends the identical requests, so a retry
// after an uncertain commit repeats the same seq.
func (r *Role) Register(ctx context.Context) error {
	if err := r.ensureProtocolTables(ctx); err != nil {
		return err
	}
	reg, mark, err := r.registrationRequests()
	if err != nil {
		return fmt.Errorf("register snode: %w", err)
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).RegisterNode(ctx, reg)
		return err
	}); err != nil {
		return fmt.Errorf("register snode: %w", err)
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).MarkActive(ctx, mark)
		return err
	}); err != nil {
		return fmt.Errorf("mark snode active: %w", err)
	}
	return nil
}
```

Deleting `registrationRequest` leaves the root-package import `github.com/sentioxyz/arbiter-core` of `snode/snode.go` unused (its only use was `arbiter.LocalNodeFeatures()` at line 146): remove it, or the package does not compile. Then make `PromotedUnsafeParts` (lines 174-176) owner-scoped:

```go
func (r *Role) PromotedUnsafeParts(tableID string) ([]string, error) {
	if err := r.requireOwned(tableID); err != nil {
		return nil, err
	}
	return r.state.PromotedUnsafeParts(tableID)
}
```

Create `snode/claims.go`:

```go
package snode

import (
	"errors"
	"fmt"
	"slices"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// Signed claims and owner scope (housegate spec 2026-10-10 D6, D8, §6.5, §8).
// With Deps.ClaimSigner the SNode signs every message the arbiter accepts
// from it and serves only Config.IndexerID's tables; without one it is the
// legacy SNode, byte for byte.

func (r *Role) now() time.Time {
	if r.d.Now != nil {
		return r.d.Now()
	}
	return time.Now()
}

// nowMillis is the registration_seq clock floor: Unix milliseconds (0 before 1970).
func (r *Role) nowMillis() uint64 {
	if ms := r.now().UnixMilli(); ms > 0 {
		return uint64(ms)
	}
	return 0
}

func (r *Role) messageContext() authority.MessageContext {
	return authority.MessageContext{NetworkID: r.cfg.NetworkID, GenesisSnapshotID: r.genesisID}
}

// sign signs one SNode message with the indexer key at the role's clock.
func (r *Role) sign(kind authority.SNodeMessageKind, body any) (string, error) {
	jws, err := r.d.ClaimSigner.SignSNodeMessageAt(kind, r.messageContext(), body, r.now().Unix())
	if err != nil {
		return "", fmt.Errorf("sign %s: %w", kind, err)
	}
	return jws, nil
}

// nodeFeatures is what RegisterNode advertises: signed_claims_v1 only with a
// claim signer, so the activation gate never counts an SNode that cannot sign.
func (r *Role) nodeFeatures() []string {
	features := arbiter.LocalNodeFeatures()
	if r.d.ClaimSigner != nil {
		return features
	}
	return slices.DeleteFunc(features, func(f string) bool { return f == arbiter.SignedClaimsFeature })
}

// registrationRequests builds the RegisterNode and MarkActive requests of one
// Register call. With a claim signer both carry one fresh registration_seq,
// durable before either is sent: max(persisted + 1, now in Unix ms).
func (r *Role) registrationRequests() (*pb.NodeRegistration, *pb.NodeRef, error) {
	reg := wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: r.cfg.NodeID, Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}
	mark := wire.MarkActive{NodeID: r.cfg.NodeID}
	if r.d.ClaimSigner != nil {
		seq, err := r.state.NextRegistrationSeq(r.nowMillis())
		if err != nil {
			return nil, nil, err
		}
		reg.Registration.RegistrationSeq, mark.RegistrationSeq = seq, seq
		if reg.SignerJWS, err = r.sign(authority.SNodeMessageRegistration, reg.Registration); err != nil {
			return nil, nil, err
		}
		if mark.SignerJWS, err = r.sign(authority.SNodeMessageMarkActive, authority.MarkActiveBody{NodeID: mark.NodeID, RegistrationSeq: seq}); err != nil {
			return nil, nil, err
		}
	}
	return wire.RegisterNodeToRequest(reg, r.nodeFeatures()), wire.MarkActiveToRequest(mark), nil
}

// resultClaimRequest is the RegisterResultClaim request for rc.
func (r *Role) resultClaimRequest(rc arbiter.RCRecord) (*pb.RCRecord, error) {
	c := wire.RegisterRC{RC: rc}
	if r.d.ClaimSigner != nil {
		jws, err := r.sign(authority.SNodeMessageResultClaim, rc)
		if err != nil {
			return nil, err
		}
		c.SourceJWS = jws
	}
	return wire.RegisterRCToRequest(c), nil
}

// promotionAckRequest is the AckPromotion request for ack.
func (r *Role) promotionAckRequest(ack arbiter.PromotionAck) (*pb.PromotionAck, error) {
	c := wire.RecordPromotionAck{Ack: ack}
	if r.d.ClaimSigner != nil {
		jws, err := r.sign(authority.SNodeMessagePromotionAck, ack)
		if err != nil {
			return nil, err
		}
		c.SourceJWS = jws
	}
	return wire.RecordPromotionAckToRequest(c), nil
}

// cleanupAckRequest is the AckCleanup request for ack.
func (r *Role) cleanupAckRequest(ack arbiter.CleanupAck) (*pb.CleanupAck, error) {
	c := wire.RecordCleanupAck{Ack: ack}
	if r.d.ClaimSigner != nil {
		jws, err := r.sign(authority.SNodeMessageCleanupAck, ack)
		if err != nil {
			return nil, err
		}
		c.SourceJWS = jws
	}
	return wire.RecordCleanupAckToRequest(c), nil
}

// signTablePurged is the reconciler's purge-report signer.
func (r *Role) signTablePurged(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error) {
	jws, err := r.sign(authority.SNodeMessageTablePurged, authority.TablePurgedBody{NodeID: nodeID, IncarnationSeq: incarnationSeq})
	if err != nil {
		return wire.RecordTablePurged{}, err
	}
	return wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq, SignerJWS: jws}, nil
}

// purgeArbiter is the reconciler's arbiter: signing with a claim signer.
func (r *Role) purgeArbiter() tableset.Arbiter {
	if r.d.ClaimSigner == nil {
		return r.d.Client
	}
	return dataplane.PurgeReporter{Client: r.d.Client, Sign: r.signTablePurged}
}

// ownerFilter is the reconciler's owner filter: this SNode's indexer when it
// signs its claims, none for a legacy SNode.
func (r *Role) ownerFilter() *uint64 {
	if r.d.ClaimSigner == nil {
		return nil
	}
	id := r.cfg.IndexerID
	return &id
}

// owns reports whether this SNode serves inc (spec D4, D8).
func (r *Role) owns(snap wire.TableRegistrySnapshot, inc wire.TableIncarnation) bool {
	return r.d.ClaimSigner == nil || snap.Owner(inc) == r.cfg.IndexerID
}

// requireOwned refuses tableID when the followed registry assigns its live
// incarnation to another SI indexer. Without an enabled registry, or for a
// key the registry does not know, ownership is not decided here.
func (r *Role) requireOwned(tableID string) error {
	snap, enabled := r.registryView()
	if !enabled {
		return nil
	}
	live := snap.Live(tableID)
	if live == nil || r.owns(snap, *live) {
		return nil
	}
	return fmt.Errorf("table %s is owned by SI indexer %d, not by this source's indexer %d: %w",
		tableID, snap.Owner(*live), r.cfg.IndexerID, ErrTableNotOwned)
}

// claimGenesisSnapshotID is the genesis snapshot id signed messages bind: the
// configured one, else the arbiter's derivation over the genesis tables.
func claimGenesisSnapshotID(cfg Config, signs bool) (string, error) {
	switch {
	case !signs:
		return "", nil
	case cfg.GenesisSnapshotID != "":
		return cfg.GenesisSnapshotID, nil
	case len(cfg.Tables) == 0:
		return "", errors.New("genesis snapshot id is required for a signing SNode that holds no genesis table")
	}
	return dataplane.GenesisSnapshotID(cfg.NetworkID, cfg.SchemaSnapshotID, cfg.ExecutorProfileID, cfg.Tables)
}
```

In `snode/registry.go` add after `ErrTableNotReady` (line 24):

```go
// ErrTableNotOwned refuses work on a table the registry assigns to another SI
// indexer (housegate spec 2026-10-10 D8): this SNode never materialised it.
// It wraps ErrSchemaUnknown, so callers that treat an unknown target as
// terminal keep doing so.
var ErrTableNotOwned = fmt.Errorf("%w (owned by another SI indexer)", ErrSchemaUnknown)
```

and in `requireAdmissible` insert a case right after `case live == nil:` (line 73-74):

```go
	case !r.owns(snap, *live):
		return fmt.Errorf("table %s is owned by SI indexer %d, not by this source's indexer %d: %w",
			tableID, snap.Owner(*live), r.cfg.IndexerID, ErrTableNotOwned)
```

In `snode/view.go` make the registry branch of `stateRootTables` (lines 57-61) owner-scoped and fix its comment ("covers this SNode's own Active and Retiring incarnations"):

```go
	for _, inc := range snap.Incarnations {
		if (inc.Status == wire.TableStatusActive || inc.Status == wire.TableStatusRetiring) && r.owns(snap, inc) {
			hashes[inc.Key()] = inc.SchemaHash
		}
	}
```

In `snode/staged.go` replace the send in `RegisterPreparedClaim` (lines 393-396):

```go
	req, err := r.resultClaimRequest(*rec.RC)
	if err != nil {
		return ClaimOutcome{}, err
	}
	err = r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewSourceClaimsClient(conn).RegisterResultClaim(ctx, req)
		return err
	})
```

In `snode/converge.go` insert in `LookupPreparedStatement` after the `LifecycleCleaned` early return (line 28):

```go
	if err := r.requireOwned(rec.Envelope.TargetTableID); err != nil {
		return PreparedLocalResult{}, false, err
	}
```

In `snode/promote.go` insert after the authority check of `handlePromote` (line 18):

```go
	if err := r.requireOwned(cmd.TableID); err != nil {
		// A leader that has not committed the signed-claims activation
		// broadcasts to every SNode; only the owner acts, so a base-CAS
		// refusal from this node can never consume another owner's promotion.
		r.d.Logger.Warn("ignoring a promotion for a table this SNode does not own",
			"promotion_seq", cmd.PromotionSeq, "table", cmd.TableID, "err", err)
		return nil
	}
```

and replace `sendAck` (lines 129-134):

```go
func (r *Role) sendAck(ctx context.Context, ack arbiter.PromotionAck) error {
	req, err := r.promotionAckRequest(ack)
	if err != nil {
		return err
	}
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).AckPromotion(ctx, req)
		return err
	})
}
```

In `snode/cleanup.go` insert after the authority check of `handleCleanup` (line 22):

```go
	if err := r.requireOwned(cmd.TableID); err != nil {
		r.d.Logger.Warn("ignoring a cleanup for a table this SNode does not own",
			"promotion_seq", cmd.PromotionSeq, "table", cmd.TableID, "err", err)
		return nil
	}
```

and replace `sendCleanupAck` (lines 48-53):

```go
func (r *Role) sendCleanupAck(ctx context.Context, ack arbiter.CleanupAck) error {
	req, err := r.cleanupAckRequest(ack)
	if err != nil {
		return err
	}
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).AckCleanup(ctx, req)
		return err
	})
}
```

(`wire` stays imported by `promote.go` / `cleanup.go` for `PromoteFromPB` / `CleanupFromPB`.)

- [ ] **Step 11: Run and commit**

Run: `cd "$W" && bazel run //:gazelle && bazel test //snode:snode_test --test_output=errors`
Expected: PASS — the new tests, and unchanged `TestRegister_SendsSNodeRegistration`, `TestRequireAdmissible_GatesFreshIntake`, `TestSourceClaimRoot_CoversActiveAndRetiringRegistryTables`, `TestNew_FollowingTheRegistryNeedsManagedTablesAndAConnection` and every state test (ClickHouse-gated tests SKIP).

```bash
cd "$W" && git add snode && git commit -m "feat(snode): signed claims with a durable registration_seq, owner-scoped SNode

With a ClaimSigner the SNode signs RegisterNode, MarkActive, RCs, acks and
purge reports with its indexer key, reserves registration_seq as
max(persisted+1, now ms) before registering, advertises signed_claims_v1,
and serves only its indexer's tables (reconciler, claim root, intake,
lookup, promoted-unsafe reads, promotions, cleanups); without one it is
unchanged (housegate spec 2026-10-10 D6, D8, §6.5, §8).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 12: Write the failing verifier tests**

Replace `verifier/registration_test.go`:

```go
package verifier

import (
	"crypto/ed25519"
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func TestVerifierRegistrationAdvertisesSignedClaims(t *testing.T) {
	role, _ := newRoleHarnessV(t, &fakeReplayCore{}, &fakeScanner{})
	pub := ed25519.NewKeyFromSeed(testSeedV()).Public().(ed25519.PublicKey)
	req, ref, err := role.registrationRequests()
	if err != nil {
		t.Fatal(err)
	}
	if req.GetNodeId() != "v1" || !slices.Equal(req.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}) ||
		!slices.Equal(req.GetEd25519Pubkey(), []byte(pub)) || ref.GetNodeId() != "v1" {
		t.Fatalf("registration = %v, activation = %v", req, ref)
	}
	if !slices.Contains(req.GetFeatures(), arbiter.ClientLanesFeature) || !slices.Contains(req.GetFeatures(), arbiter.SignedClaimsFeature) {
		t.Fatalf("features = %v, want %s and %s", req.GetFeatures(), arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature)
	}
}
```

Create `verifier/claims_test.go` (reuses `testConfigV`, `fakeReplayCore`, `fakeScanner`):

```go
package verifier

import (
	"context"
	"crypto/ed25519"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// membershipFakeV records RegisterNode and MarkActive requests.
type membershipFakeV struct {
	pb.UnimplementedMembershipServer
	mu    sync.Mutex
	regs  []*pb.NodeRegistration
	marks []*pb.NodeRef
}

func (f *membershipFakeV) RegisterNode(_ context.Context, m *pb.NodeRegistration) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.regs = append(f.regs, m)
	return &pb.Ack{}, nil
}

func (f *membershipFakeV) MarkActive(_ context.Context, m *pb.NodeRef) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, m)
	return &pb.Ack{}, nil
}

func (f *membershipFakeV) snapshot() ([]*pb.NodeRegistration, []*pb.NodeRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.regs), slices.Clone(f.marks)
}

func startMembershipFakeV(t *testing.T, f *membershipFakeV) *dataplane.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterMembershipServer(srv, f)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

const fixtureMillisV = int64(1_760_054_400_000) // authoritytest.Iat in ms

// signingConfigV is verifier 1 of the authoritytest fixtures.
func signingConfigV(t *testing.T) Config {
	cfg := testConfigV()
	cfg.ReplicaID = authoritytest.VerifierNodeID(1)
	cfg.Ed25519Seed = authoritytest.VerifierKey(1).Seed()
	cfg.NetworkID = authoritytest.NetworkID
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.GenesisSnapshotID = authoritytest.GenesisSnapshotID
	cfg.StateDir = t.TempDir()
	return cfg
}

func newSigningVerifier(t *testing.T, cfg Config, client *dataplane.Client, nowMs *int64) *Role {
	t.Helper()
	role, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{},
		Now: func() time.Time { return time.UnixMilli(*nowMs) }})
	if err != nil {
		t.Fatal(err)
	}
	return role
}

func TestRegister_VerifierSignsWithItsEvidenceKey(t *testing.T) {
	fake := &membershipFakeV{}
	now := fixtureMillisV
	role := newSigningVerifier(t, signingConfigV(t), startMembershipFakeV(t, fake), &now)
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.snapshot()
	if len(regs) != 1 || len(marks) != 1 {
		t.Fatalf("%d registrations, %d activations", len(regs), len(marks))
	}
	reg, mark := regs[0], marks[0]
	// ed25519 is deterministic: the requests reproduce the pinned vectors.
	if reg.GetRegistrationSeq() != authoritytest.RegistrationSeq || mark.GetRegistrationSeq() != authoritytest.RegistrationSeq ||
		reg.GetEd25519Signature() != authoritytest.VerifierRegistrationSignature || mark.GetEd25519Signature() != authoritytest.VerifierMarkActiveSignature {
		t.Fatalf("registration %v / activation %v differ from the authoritytest vectors", reg, mark)
	}
	pub := authoritytest.VerifierKey(1).Public().(ed25519.PublicKey)
	if err := authority.VerifyVerifierMessage(pub, authority.VerifierMessageRegistration, authoritytest.Context(),
		wire.RegistrationFromPB(reg), reg.GetEd25519Signature()); err != nil {
		t.Fatal(err)
	}
	if reg.GetSignerJws() != "" || !slices.Contains(reg.GetFeatures(), arbiter.SignedClaimsFeature) {
		t.Fatalf("registration = %v", reg)
	}
}

// TestRegister_VerifierSeqStaysAboveAfterTheStateFileIsLost is the amended
// rule (CONTRACT §3a) for the verifier's registration.json.
func TestRegister_VerifierSeqStaysAboveAfterTheStateFileIsLost(t *testing.T) {
	fake := &membershipFakeV{}
	client := startMembershipFakeV(t, fake)
	cfg := signingConfigV(t)
	now := fixtureMillisV
	if err := newSigningVerifier(t, cfg, client, &now).Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(cfg.StateDir, "registration.json")); err != nil {
		t.Fatal(err)
	}
	now = fixtureMillisV + 5
	if err := newSigningVerifier(t, cfg, client, &now).Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.snapshot()
	if len(regs) != 2 || regs[1].GetRegistrationSeq() != uint64(fixtureMillisV+5) || marks[1].GetRegistrationSeq() != regs[1].GetRegistrationSeq() {
		t.Fatalf("seqs = %v / %v: the second registration must carry a larger seq", regs, marks)
	}
}

func TestRegister_VerifierSeqNeverGoesBack(t *testing.T) {
	for _, stateDir := range []bool{true, false} {
		fake := &membershipFakeV{}
		cfg := signingConfigV(t)
		if !stateDir {
			cfg.StateDir = "" // in memory, still clock-floored
		}
		now := fixtureMillisV
		role := newSigningVerifier(t, cfg, startMembershipFakeV(t, fake), &now)
		if err := role.Register(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = fixtureMillisV - 60_000
		if err := role.Register(context.Background()); err != nil {
			t.Fatal(err)
		}
		if regs, _ := fake.snapshot(); len(regs) != 2 || regs[1].GetRegistrationSeq() != uint64(fixtureMillisV)+1 {
			t.Fatalf("state dir %v: seqs = %v, want persisted+1", stateDir, regs)
		}
	}
}

func TestVerifierPurgeReportIsSigned(t *testing.T) {
	now := fixtureMillisV
	role := newSigningVerifier(t, signingConfigV(t), startMembershipFakeV(t, &membershipFakeV{}), &now)
	report, err := role.signTablePurged(authoritytest.VerifierNodeID(1), 5)
	if err != nil || report != (wire.RecordTablePurged{NodeID: authoritytest.VerifierNodeID(1), IncarnationSeq: 5, Ed25519Signature: authoritytest.VerifierTablePurgedSignature}) {
		t.Fatalf("report = %+v, %v: want the authoritytest vector", report, err)
	}
}

func TestNew_VerifierDerivesTheGenesisSnapshotID(t *testing.T) {
	cfg := signingConfigV(t)
	cfg.GenesisSnapshotID = ""
	now := fixtureMillisV
	role := newSigningVerifier(t, cfg, startMembershipFakeV(t, &membershipFakeV{}), &now)
	want, err := dataplane.GenesisSnapshotID(cfg.NetworkID, cfg.SchemaSnapshotID, cfg.ExecutorProfileID, cfg.Tables)
	if err != nil || role.genesisID != want {
		t.Fatalf("genesis id = %q, want %q (%v)", role.genesisID, want, err)
	}
}
```

- [ ] **Step 13: Implement the signing verifier**

In `verifier/config.go` append to `Config` (after `AddTransitionReadyWait`, line 50):

```go
	// StateDir holds registration.json, the verifier's last registration_seq
	// (housegate spec 2026-10-10 §6.5). The verifier kept no on-disk state
	// before this; empty keeps the sequence in memory, still clock-floored.
	StateDir string
	// GenesisSnapshotID is bound into every signed message. Empty derives it
	// from Tables, the network's genesis table set every verifier holds.
	GenesisSnapshotID string
```

In `verifier/verifier.go` add `"sync"` to the imports; append `Now func() time.Time` to `Deps` (comment: `// Now is the registration_seq clock; nil is time.Now.`); append to `Role`:

```go
	// genesisID is the genesis snapshot id signed messages bind.
	genesisID string
	// seqMu guards lastSeq, the last registration_seq this process reserved.
	seqMu   sync.Mutex
	lastSeq uint64
```

in `New` replace the role construction and the reconciler call (lines 90-95; the error check and `r.tables = tables` that follow stay):

```go
	genesisID := cfg.GenesisSnapshotID
	if genesisID == "" {
		var err error
		if genesisID, err = dataplane.GenesisSnapshotID(cfg.NetworkID, cfg.SchemaSnapshotID, cfg.ExecutorProfileID, cfg.Tables); err != nil {
			return nil, fmt.Errorf("verifier: %w", err)
		}
	}
	r := &Role{cfg: cfg, d: d, priv: ed25519.NewKeyFromSeed(cfg.Ed25519Seed), genesisID: genesisID}
	r.ensureFn = r.ensureProtocolTablesMode
	if cfg.protocolTables != ddl.ModeOff {
		tables, err := tableset.New(tableset.Config{
			Pinned: r.pinned(), Genesis: cfg.Tables, Interval: cfg.ProtocolTablesReconcile,
		}, tableset.Deps{Conn: d.Conn, Registry: d.Registry,
			Arbiter: dataplane.PurgeReporter{Client: d.Client, Sign: r.signTablePurged}, Logger: d.Logger})
```

delete `registrationRequest` (lines 113-121) and replace `Register` (lines 123-141):

```go
func (r *Role) Register(ctx context.Context) error {
	if err := r.ensureProtocolTables(ctx); err != nil {
		return err
	}
	reg, mark, err := r.registrationRequests()
	if err != nil {
		return fmt.Errorf("register verifier: %w", err)
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).RegisterNode(ctx, reg)
		return err
	}); err != nil {
		return fmt.Errorf("register verifier: %w", err)
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).MarkActive(ctx, mark)
		return err
	}); err != nil {
		return fmt.Errorf("mark verifier active: %w", err)
	}
	return nil
}
```

Create `verifier/claims.go`:

```go
package verifier

import (
	"crypto/ed25519"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// Signed verifier messages (housegate spec 2026-10-10 D7, §6.5): the verifier
// signs its registration, activation and purge reports with the ed25519 key
// that already signs its evidence. The arbiter drops the signatures before
// the signed-claims activation and requires them after it.

func (r *Role) now() time.Time {
	if r.d.Now != nil {
		return r.d.Now()
	}
	return time.Now()
}

func (r *Role) nowMillis() uint64 {
	if ms := r.now().UnixMilli(); ms > 0 {
		return uint64(ms)
	}
	return 0
}

func (r *Role) messageContext() authority.MessageContext {
	return authority.MessageContext{NetworkID: r.cfg.NetworkID, GenesisSnapshotID: r.genesisID}
}

// registrationRequests builds the signed RegisterNode and MarkActive requests
// of one Register call; both carry one fresh registration_seq.
func (r *Role) registrationRequests() (*pb.NodeRegistration, *pb.NodeRef, error) {
	seq, err := r.nextRegistrationSeq()
	if err != nil {
		return nil, nil, err
	}
	ctx := r.messageContext()
	reg := wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: r.cfg.ReplicaID,
		Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: r.priv.Public().(ed25519.PublicKey), RegistrationSeq: seq}}
	if reg.Ed25519Signature, err = authority.SignVerifierMessage(r.priv, authority.VerifierMessageRegistration, ctx, reg.Registration); err != nil {
		return nil, nil, err
	}
	mark := wire.MarkActive{NodeID: r.cfg.ReplicaID, RegistrationSeq: seq}
	if mark.Ed25519Signature, err = authority.SignVerifierMessage(r.priv, authority.VerifierMessageMarkActive, ctx,
		authority.MarkActiveBody{NodeID: mark.NodeID, RegistrationSeq: seq}); err != nil {
		return nil, nil, err
	}
	return wire.RegisterNodeToRequest(reg, arbiter.LocalNodeFeatures()), wire.MarkActiveToRequest(mark), nil
}

// signTablePurged is the reconciler's purge-report signer.
func (r *Role) signTablePurged(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error) {
	sig, err := authority.SignVerifierMessage(r.priv, authority.VerifierMessageTablePurged, r.messageContext(),
		authority.TablePurgedBody{NodeID: nodeID, IncarnationSeq: incarnationSeq})
	if err != nil {
		return wire.RecordTablePurged{}, err
	}
	return wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq, Ed25519Signature: sig}, nil
}
```

Create `verifier/registration_state.go`:

```go
package verifier

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// registrationStateFile is the verifier's only on-disk state.
const registrationStateFile = "registration.json"

type registrationState struct {
	RegistrationSeq uint64 `json:"registration_seq"`
}

// nextRegistrationSeq reserves the registration_seq of the next Register
// call: max(last + 1, now in Unix ms), where last is the larger of this
// process's and the persisted value. With a StateDir it is durable before it
// is returned (CONTRACT §3a); a lost file only drops the persisted floor.
func (r *Role) nextRegistrationSeq() (uint64, error) {
	r.seqMu.Lock()
	defer r.seqMu.Unlock()
	last := r.lastSeq
	if r.cfg.StateDir != "" {
		persisted, err := readRegistrationSeq(r.cfg.StateDir)
		if err != nil {
			return 0, err
		}
		last = max(last, persisted)
	}
	next := max(last+1, r.nowMillis())
	if r.cfg.StateDir != "" {
		if err := writeRegistrationSeq(r.cfg.StateDir, next); err != nil {
			return 0, err
		}
	}
	r.lastSeq = next
	return next, nil
}

func readRegistrationSeq(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, registrationStateFile))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("verifier registration state: %w", err)
	}
	var st registrationState
	if err := json.Unmarshal(b, &st); err != nil {
		return 0, fmt.Errorf("verifier registration state corrupt: %w", err)
	}
	return st.RegistrationSeq, nil
}

// writeRegistrationSeq persists seq like the SNode's state.json
// (snode/state.go persistStateLocked): temp file, fsync, rename, directory fsync.
func writeRegistrationSeq(dir string, seq uint64) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("verifier state dir: %w", err)
	}
	b, err := json.Marshal(registrationState{RegistrationSeq: seq})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".registration-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, registrationStateFile)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
```

- [ ] **Step 14: Run and commit**

Run: `cd "$W" && bazel run //:gazelle && bazel test //verifier:verifier_test --test_output=errors`
Expected: PASS — the new tests and the unchanged `TestRegister_SendsVerifierRegistration` (node id, role, key, one activation of `v1`).

```bash
cd "$W" && git add verifier && git commit -m "feat(verifier): signed registration, activation and purge reports

The verifier signs RegisterNode, MarkActive and purge reports with its
evidence key, keeps registration_seq (max(persisted+1, now ms)) in
<state_dir>/registration.json, and binds the derived genesis snapshot id
(housegate spec 2026-10-10 D7, §6.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 15: Document, run the full verification and commit**

Append one paragraph (no hard wraps) to `README.md` § Compatibility:

```markdown
Multi-source storage integrity, stage 1 (housegate spec 2026-10-10), adds `ConsensusParamsUpdate.SIIndexers` / `Verifiers`, `NodeRegistration.RegistrationSeq`, the table registry's owner fields (`AddTable.OwnerIndexerID`, `SeedLegacyTables.IndexerID`, incarnation `OwnerIndexerID`, snapshot `si_indexers` / `seeded_indexers`), `AdmissionCodeSourceUnavailable` and the signature fields of `RegisterRC`, `RecordPromotionAck`, `RecordCleanupAck`, `RegisterNode`, `MarkActive`, `EvictNode` and `RecordTablePurged`. All are absent before the signed-claims activation, so earlier digests and Raft command bytes are unchanged (pinned by `wire/signed_claims_golden_test.go` and `authority/consensus_golden_test.go`); `wire.StripSignedClaims` is the pre-activation proposal filter, and `wire.Decode` refuses a signature in a request-only copy. `authority` signs and verifies SNode enrolment statements and messages (ES256K, purposes `arbiter-snode-enrollment-v1` / `arbiter-snode-message-v1`), verifier messages (ed25519) and authority evictions (`arbiter-evict-node-v1`); `authority/authoritytest` pins their vectors. An SNode given `Deps.ClaimSigner` signs what it sends, keeps a durable `registration_seq`, advertises `signed_claims_v1` and serves only `Config.IndexerID`'s tables; without one it is unchanged. Verifiers always sign with their evidence key and keep `registration_seq` in `Config.StateDir`.
```

Run:

```bash
cd "$W" && bash scripts/check-public-boundary.sh && bazel run //:gazelle && git diff --exit-code -- '*.bazel' && bazel build //... && bazel test //... --test_output=errors && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors
```

Expected: no BUILD drift; every target PASSES, with and without the race detector (CI's `test` job runs the race line); the ClickHouse-gated tests SKIP. Then, with the two-node docker pair of README § Build and test (CI's `integration-clickhouse` job runs the same command on the PR):

```bash
cd "$W" && ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1 CH_ADDR=127.0.0.1:9000 CH_REPLICA_ADDR=127.0.0.1:9001 \
  bazel test //dataplane/ddl:ddl_test //dataplane/tableset:tableset_test //snode:snode_test //verifier:verifier_test \
  --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=ARBITER_CH_REPLICA \
  --test_env=CH_ADDR --test_env=CH_REPLICA_ADDR --test_timeout=900 --test_output=errors
```

Expected: PASS, including `TestReconciler_OwnerFilterMaterialisesAndPurgesOnlyOwnedKeys` and `TestReconciler_OwnerFilterNeverSweepsAnotherOwnersKeeperPath`.

```bash
cd "$W" && git add README.md && git commit -m "docs: multi-source SI stage 1 compatibility notes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 16: PR, review, merge (each remote action requires explicit user confirmation)**

After approval:

```bash
cd "$W" && git push -u origin urwt/claude/multi-source-s1
gh pr create --repo sentioxyz/arbiter-core --base main --head urwt/claude/multi-source-s1 \
  --title 'feat: signed claims and owner-scoped data plane (multi-source SI stage 1)' \
  --body "$(cat <<'BODY'
Stage 1 of the multi-source storage-integrity network (housegate spec `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` §6, §8, D3-D8), on arbiter-proto `PROTO_TAG`.

- `SIIndexerEntry` / `VerifierEntry` on the consensus update with normalisation; `NodeRegistration.RegistrationSeq`; `AdmissionCodeSourceUnavailable`; registry owners, seeded set and per-indexer seed on the wire.
- `authority`: SNode enrolment statements, signed SNode messages (ES256K) and verifier messages (ed25519), authority-signed eviction bound to network, genesis and epoch; `authority/authoritytest` vectors.
- `wire`: signature-bearing commands, request converters, `StripSignedClaims` for pre-activation proposals, refusal of signatures in request-only copies.
- Data plane: signing SNode with a durable `registration_seq` and owner-scoped reconciler and reads; signing verifier.

Pre-activation bytes and digests are pinned unchanged (`wire/signed_claims_golden_test.go`, `authority/consensus_golden_test.go`).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
BODY
)"
gh pr checks <n> --repo sentioxyz/arbiter-core --watch
gh pr merge <n> --repo sentioxyz/arbiter-core --squash
```

Expected: both CI jobs (`test`, `integration-clickhouse`) green before the merge.

- [ ] **Step 17: Release `CORE_TAG` (requires explicit user confirmation)**

Releases are annotated `vX.Y.Z` tags cut by the **Cut Release** workflow from `main` (`git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core tag --sort=-v:refname | head` shows `v0.12.1`, `v0.12.0`, `v0.11.1`, …; `scripts/next-version.sh` bumps Y on the first cut of a new UTC day, else Z). Never predict the tag. After approval:

```bash
gh workflow run cut-release.yml --repo sentioxyz/arbiter-core --ref main
gh run watch --repo sentioxyz/arbiter-core "$(gh run list --repo sentioxyz/arbiter-core --workflow cut-release.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
```

The workflow validates, pushes the annotated tag and creates the GitHub release; its step summary reads `### Cutting vX.Y.Z` (`.github/workflows/cut-release.yml:130-157`). Read the tag back with `gh release list --repo sentioxyz/arbiter-core --limit 1 --json tagName --jq '.[0].tagName'`, record it as `CORE_TAG` in the release ledger, then verify it by content (substitute it for `<tag>`):

```bash
T=<tag>; R=/Users/uranuswch/Dev/sentio_xyz/arbiter-core
git -C "$R" fetch origin --tags
git -C "$R" show "$T":authority/snode_messages.go | grep -c 'SNodeMessagePurpose = "arbiter-snode-message-v1"'
git -C "$R" show "$T":wire/signed_claims.go | grep -c '^func StripSignedClaims'
git -C "$R" show "$T":go.mod | grep -c "github.com/sentioxyz/arbiter-proto $PROTO_TAG\$"
git -C "$R" rev-parse "$T^{commit}"
```

Expected: `1`, `1`, `1`, then the commit (sentio-node's `git_override` pins it; arbiter pins the tag with `bash scripts/update-arbiter-core.sh <tag>`).

---

## Task 5: arbiter — pin CORE_TAG; si_indexers and verifiers parameters and their transition rules (S1)

**Files:**
- Create: `fsm/pre_signed_claims_golden_test.go`, `fsm/testdata/pre_signed_claims_golden.json` (captured on the unchanged tree, before the pin)
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock`, BUILD files (via `scripts/update-arbiter-core.sh`, `bazel mod tidy`, `bazel run //:gazelle`)
- Modify: `fsm/table_registry.go:35-57` (`TableIncarnation.OwnerIndexerID`, in the pin commit), `:131-137` (`TableRegistryState.SeededIndexers`), `:302-312` (owner copy in `TableRegistryView()`, in the pin commit)
- Modify: `fsm/state.go:94-113` (`Params`), `fsm/params_identity.go:44-64` (`normalizeConsensusParams`), `fsm/consensus_reads.go:77-88` (`cloneParams`), `fsm/watch.go:92-96` (`NewWithNotify`), `fsm/consensus_history.go:23-25`, `fsm/consensus_updates.go:19-159` (`applyUpdateConsensusParams`, `verifyConsensusTransition`)
- Create: `fsm/signed_claims.go`, `fsm/signed_claims_params_test.go`
- Modify: `server/consensus_admin.go:69-81` (`consensusMutableParams`)
- Create: `server/signed_claims_test.go`

**Interfaces:**
- Consumes: `PROTO_TAG` (Task 1) through `CORE_TAG`: `pb.ConsensusMutableParams.SiIndexers` / `.Verifiers`, `pb.SIIndexerEntry` (`GetIndexerId`, `GetActivationBlock`, `GetSigner`, `GetSnodeNodeId`, `GetEnrollmentJws`), `pb.VerifierEntry` (`GetNodeId`, `GetEd25519Pubkey`), `pb.TableIncarnation.OwnerIndexerId`. `CORE_TAG` (arbiter-core Tasks 2–4, CONTRACT §2): `arbiter.SIIndexerEntry`, `arbiter.VerifierEntry`, `arbiter.ConsensusParamsUpdate.SIIndexers` / `.Verifiers`, `authority.NormalizeConsensusParamsUpdate` (sorts both lists, lowercases signers, refuses duplicates and `max_writers < len(si_indexers)`, keeps nil nil), `authority.SNodeEnrollmentStatement`, `(*authority.Signer).SignSNodeEnrollment`, `authority.VerifySNodeEnrollment`, `authority.MessageContext`, `wire.SIIndexerEntriesToPB`, `wire.VerifierEntriesToPB`, and the Task 3 fixture package `github.com/sentioxyz/arbiter-core/authority/authoritytest` (`MustSigner(t testing.TB, keyHex string) *authority.Signer`, `IndexerKeyHex0` / `IndexerAddr0`, `IndexerKeyHex1` / `IndexerAddr1`, `StrangerAddr`, `NetworkID`, `GenesisSnapshotID`, `SNodeEnrollmentVectors` of `SNodeEnrollmentVector{Statement, KeyHex, Signer, Iat, Hash, JWS}`, `SIIndexerEntry0()`, `SIIndexerEntry1(activationBlock)`).
- Produces: `fsm.Params.SIIndexers []arbiter.SIIndexerEntry` (`si_indexers,omitempty`), `fsm.Params.Verifiers []arbiter.VerifierEntry` (`verifiers,omitempty`); `fsm.TableIncarnation.OwnerIndexerID *uint64` (`owner_indexer_id,omitempty`); `fsm.TableRegistryState.SeededIndexers []uint64` (`seeded_indexers,omitempty`); in `fsm/signed_claims.go`: `removedFromVerifiersReason`, `notNamedByActivationReason`, `(f *FSM) signedClaimsActiveLocked() bool`, `(f *FSM) SignedClaimsActive() bool`, `(f *FSM) siIndexerLocked(id uint64) (arbiter.SIIndexerEntry, bool)`, `(f *FSM) siIndexerBySNodeLocked(nodeID string) (arbiter.SIIndexerEntry, bool)`, `(f *FSM) messageContextLocked() (authority.MessageContext, error)`, `findSIIndexer`, `findSIIndexerBySNode`, `cloneSIIndexers`, `cloneVerifiers`, `listsVerifier`, `verifySIIndexerEnrollment`, `verifySIIndexersTransition`, `verifyVerifiersTransition`, `(f *FSM) signedClaimsMembershipLocked(next Params) (evict []string, reason string, err error)`, `(f *FSM) registryEffectiveCursorLocked(*arbiter.TableRegistryParams) uint64`, `(f *FSM) migrateRegistryOwnersLocked()`, `(f *FSM) evictNodesLocked(ids []string, reason string) bool`; the frozen golden `fsm/testdata/pre_signed_claims_golden.json` with `runPreSignedClaimsHistory` and `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`; the tests other tasks re-run, `TestActivationEvictsUnnamedNodes` and `TestVerifierRemovalCompletesAPurgeWithOneRegistryBump`; test helpers in `fsm/signed_claims_params_test.go`: `indexerSigner1(t testing.TB)`, `indexerSigner2(t testing.TB)`, `enrollment`, `siEntry`, `foundingEntry`, `testVerifiers`, `claimsUpdate`, `claimsFSM`, `activateClaims`.

The rules split along the line the code already draws (`fsm/consensus_updates.go:91-93`): `verifyConsensusTransition` is replayed by restore over `Params` alone (`fsm/consensus_history.go:43-57`), so it gets only the signed-history rules (1, 2, 4, the pure halves of 5 and 6); the rules that read committed state (3, the node-record half of 5, the eviction and floor half of 6, 8, and CONTRACT §3c's authoritative activation lists, which evict every running node they do not name) run in `applyUpdateConsensusParams` only, like the writer and drain gates. They run just ahead of the writer gate, which at the activation counts only the writers the activation keeps; every other update meets the gates exactly as today.

- [ ] **Step 1: Create the worktree, freeze a pre-activation history on the unchanged tree, then pin `CORE_TAG`**

Create the arbiter worktree with the URWT command of Task 1 Step 1 for `/Users/uranuswch/Dev/sentio_xyz/arbiter` (`W` is the JSON `path`). Do not touch `go.mod` yet: the golden below is captured with the arbiter-core the devnet2 voters run today, so it also proves the pin byte-neutral.

Create `fsm/pre_signed_claims_golden_test.go` (it reuses `legacyGolden`, `describeApply`, `legacyGoldenUserKeyHex` and `legacyGoldenAuthorityHex` from `fsm/legacy_lane_golden_test.go`, and the existing helpers `mustNewFSM`, `mkLog`, `validEnvelope`, `registryEnvelope`, `laneEnvelope`, `rcFor`, `addCmd`, `retireCmd`, `ev`, `genesisSchemaHash`, `consensusUpdate`, `signedConsensusUpdate`, `testRegistryParams`, `testPubkey`, `snapshotBytes`):

```go
package fsm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

const (
	preSignedClaimsGoldenPath = "testdata/pre_signed_claims_golden.json"
	// preSignedClaimsRotatedAuthorityHex is the fixed key the history rotates
	// the authority to.
	preSignedClaimsRotatedAuthorityHex = "8a1f9a8f95be41cd7ccb6168179afb4504aefe388d1e14474d32c45c72ce7b7a"
)

// runPreSignedClaimsHistory replays one fixed history that never sets
// si_indexers but crosses every path stage 1 of the multi-source network
// changes (housegate spec 2026-10-10 §6): a two-writer pool whose sources are
// hash-selected, a bound RC, an RC from the other writer, a parked RC, the
// table registry's enabling update, seed, adds, cursor advance, retirements, a
// purge completed by an eviction, admission behind a table-set fence and
// against registered and refused targets, an authority rotation and the
// client-lanes activation. It was captured from the release before stage 1;
// never recapture it: a difference means stage 1 replays pre-activation
// history differently.
func runPreSignedClaimsHistory(t *testing.T) legacyGolden {
	t.Helper()
	signer, err := authority.NewSignerFromHex(legacyGoldenAuthorityHex)
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := authority.NewSignerFromHex(preSignedClaimsRotatedAuthorityHex)
	if err != nil {
		t.Fatal(err)
	}
	params := testParams()
	params.AuthorityAddresses = []string{signer.Address()}
	params.MaxWriters = 2
	f := mustNewFSM(t, params)
	key, err := crypto.HexToECDSA(legacyGoldenUserKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	account := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	var out legacyGolden
	apply := func(c wire.Command) { out.Results = append(out.Results, describeApply(f.Apply(mkLog(t, c)))) }
	submitEnv := func(env arbiter.StatementEnvelope) {
		apply(wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: env}})
	}
	register := func(id string, role arbiter.NodeRole) {
		reg := arbiter.NodeRegistration{NodeID: id, Roles: []arbiter.NodeRole{role}}
		if role == arbiter.NodeRoleVerifier {
			reg.Ed25519Pubkey = testPubkey(id[len(id)-1])
		}
		apply(wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg}})
		apply(wire.Command{MarkActive: &wire.MarkActive{NodeID: id}})
	}

	// A two-writer pool: every source below is hash-selected (design §5.4).
	register("s1", arbiter.NodeRoleSNode)
	register("s2", arbiter.NodeRoleSNode)
	for _, id := range []string{"v1", "v2", "v3", "v4"} {
		register(id, arbiter.NodeRoleVerifier)
	}
	for seq := uint64(1); seq <= 6; seq++ {
		submitEnv(validEnvelope(t, key, account, seq))
	}
	apply(wire.Command{RegisterRC: &wire.RegisterRC{RC: rcFor(f, 1, "0xr1", "0xaa01")}})
	wrong := rcFor(f, 2, "0xr2", "0xaa02")
	wrong.SourceNode = map[string]string{"s1": "s2", "s2": "s1"}[wrong.SourceNode]
	apply(wire.Command{RegisterRC: &wire.RegisterRC{RC: wrong}})
	seventh := validEnvelope(t, key, account, 7)
	apply(wire.Command{RegisterRC: &wire.RegisterRC{RC: arbiter.RCRecord{StatementID: seventh.StatementID, SourceNode: "s1", SourceClaimRoot: "0xpark",
		CandidateParts: []arbiter.CandidatePart{{TableID: "db.t", PartitionID: "p0", PartName: "all_1_0", PartRowLtHash: "0xaa07", RowCount: 1, Bytes: 32}}}}})
	submitEnv(seventh)
	apply(wire.Command{SealL3Block: &wire.SealL3Block{}})

	// The table registry: enable, seed, add, advance, retire, purge.
	enable := consensusUpdate(t, f, params.AuthorityAddresses, 2)
	enable.TableRegistry = testRegistryParams()
	apply(signedConsensusUpdate(t, signer, enable))
	apply(wire.Command{SeedLegacyTables: &wire.SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 99, Hash: "0x99"},
		Tables: []arbiter.LegacyTable{{DatabaseID: "db", TableID: "old", Created: ev(5, "0x5", 0)}}}})
	apply(addCmd(t, f, "db", "n", ev(100, "0x100", 0), ev(101, "0x101", 0), 1))
	apply(wire.Command{AdvanceL2Cursor: &wire.AdvanceL2Cursor{To: arbiter.L2BlockRef{Number: 105, Hash: "0x105"}}})
	apply(retireCmd("db", wire.TableRetireReasonTableDeleted, ev(106, "0x106", 0), "old"))
	apply(addCmd(t, f, "db", "p", ev(107, "0x107", 0), ev(108, "0x108", 0), 1))
	apply(retireCmd("db", wire.TableRetireReasonTableDeleted, ev(109, "0x109", 0), "p"))
	purging := f.st.TableRegistry.Live("db.p").Seq
	for _, node := range []string{"s1", "v1", "v2", "v3"} {
		apply(wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: node, IncarnationSeq: purging}})
	}
	apply(wire.Command{EvictNode: &wire.EvictNode{NodeID: "v4", Reason: "offline"}})
	apply(wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: "s2", IncarnationSeq: purging}})

	// Admission against the registry: fenced while db.n's add is unsealed,
	// then the transition block, then registered and refused targets.
	genesisHash := genesisSchemaHash(t, f)
	submitEnv(registryEnvelope(t, key, account, 8, "db.t", genesisHash))
	apply(wire.Command{SealL3Block: &wire.SealL3Block{}})
	for seq := uint64(8); seq <= 11; seq++ {
		submitEnv(registryEnvelope(t, key, account, seq, "db.t", genesisHash))
	}
	submitEnv(registryEnvelope(t, key, account, 12, "db.n", f.st.TableRegistry.Live("db.n").SchemaHash))
	submitEnv(registryEnvelope(t, key, account, 12, "db.unknown", genesisHash))

	// An authority rotation, then client lanes under the new authority.
	rotate := consensusUpdate(t, f, []string{rotated.Address()}, 2)
	rotate.TableRegistry = testRegistryParams()
	apply(signedConsensusUpdate(t, signer, rotate))
	lanes := consensusUpdate(t, f, []string{rotated.Address()}, 2)
	lanes.TableRegistry = testRegistryParams()
	lanes.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	apply(signedConsensusUpdate(t, rotated, lanes))
	submitEnv(laneEnvelope(t, f, key, account, testClientLaneA, 1))
	apply(wire.Command{SealL3Block: &wire.SealL3Block{}})

	for _, h := range f.st.Blocks {
		hash, err := h.ChainHash()
		if err != nil {
			t.Fatal(err)
		}
		out.ChainHashes = append(out.ChainHashes, hash)
	}
	out.SpentIDsRoot = "0x" + hex.EncodeToString(f.st.SpentIDs.Root())
	b := snapshotBytes(t, f)
	out.SnapshotVersion = b[4]
	sum := sha256.Sum256(b)
	out.SnapshotSHA256 = hex.EncodeToString(sum[:])
	return out
}

func TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims(t *testing.T) {
	got := runPreSignedClaimsHistory(t)
	if os.Getenv("ARBITER_CAPTURE_PRE_SIGNED_CLAIMS_GOLDEN") == "1" {
		if _, err := os.Stat(preSignedClaimsGoldenPath); err == nil {
			t.Fatalf("%s exists; it is captured once from the release before stage 1 and never rewritten", preSignedClaimsGoldenPath)
		}
		b, _ := json.MarshalIndent(got, "", "  ")
		if err := os.WriteFile(preSignedClaimsGoldenPath, append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Skip("captured; commit the file")
	}
	raw, err := os.ReadFile(preSignedClaimsGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var want legacyGolden
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		gb, _ := json.MarshalIndent(got, "", "  ")
		t.Fatalf("pre-activation history replays differently from the release before stage 1:\n%s", gb)
	}
	if len(got.ChainHashes) != 3 || got.SnapshotVersion != snapshotVersionV18 {
		t.Fatalf("the history must seal three blocks and write v18, got %d blocks and v%d", len(got.ChainHashes), got.SnapshotVersion)
	}
}
```

Capture it with Go on the unmodified tree (Bazel's sandbox cannot write into the source tree):

Run: `cd "$W" && ARBITER_CAPTURE_PRE_SIGNED_CLAIMS_GOLDEN=1 go test ./fsm/ -run TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims -v && go test ./fsm/ -run TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims -v && cat fsm/testdata/pre_signed_claims_golden.json`
Expected: first run SKIP ("captured"), second run PASS; the file lists 48 results, among them `rejected:rc source_node does not match the deterministic source selection (§5.4)`, `rejected:table-set transition is pending; retry after it is sealed`, two `submit:3:0:…` (SCHEMA_NOT_ALLOWED) answers and three `fsm.SealResult`; three `chain_hashes`; `snapshot_version` 18. If it does not, stop and investigate before any change. `fsm/BUILD.bazel` already ships `data = glob(["testdata/**"])` (`fsm/BUILD.bazel:192`); run `bazel test //fsm:fsm_test --test_filter=TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`.

```bash
cd "$W" && git add fsm/pre_signed_claims_golden_test.go fsm/testdata/pre_signed_claims_golden.json && git commit -m "test(fsm): freeze a pre-activation history before multi-source stage 1

Command answers, chain hashes, spent_ids_root and the exact snapshot
container of a fixed history that never sets si_indexers but crosses
every path stage 1 changes; the stage-1 release must replay it byte for
byte (housegate spec 2026-10-10 §6.7, §13 step 1).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Now pin arbiter-core. `server/table_registry_test.go:26-49` (`TestTableRegistryMirrorsProto`) pins every `pb.TableIncarnation` field to an `fsm.TableIncarnation` JSON tag, and `PROTO_TAG` adds `owner_indexer_id = 18`, so the pin commit also adds the still inert Go field. In `fsm/table_registry.go`, append to `TableIncarnation` after `PurgedBy` (line 56):

```go
	// OwnerIndexerID is the SI indexer whose database held the table at its
	// TableCreated block (spec 2026-10-10 D4). Every incarnation carries it
	// once si_indexers is committed and it never changes; nil before, which
	// keeps every pre-activation snapshot byte-identical.
	OwnerIndexerID *uint64 `json:"owner_indexer_id,omitempty"`
```

and in `TableRegistryView()` (`:302-312`), directly after `c.PurgedBy = slices.Clone(inc.PurgedBy)`, so the view never aliases committed state:

```go
		if inc.OwnerIndexerID != nil {
			owner := *inc.OwnerIndexerID
			c.OwnerIndexerID = &owner
		}
```

Run: `cd "$W" && bash scripts/update-arbiter-core.sh "$CORE_TAG" && bazel mod tidy && bazel run //:gazelle && go list -m github.com/sentioxyz/arbiter-proto && bazel build //... && bazel test //...`
Expected: `go list` prints `github.com/sentioxyz/arbiter-proto $PROTO_TAG` (arbiter-core raises it through minimal version selection); the build succeeds (new RPCs are covered by the embedded `Unimplemented*Server` stubs); everything PASSES, in particular `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims` (the pin is byte-neutral), `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, `TestTableRegistryMirrorsProto`, every `TestDeterminism_*` and the L3 goldens. From this commit on `GetProtocolInfo.features` reports `arbiter.LocalNodeFeatures()`, which now includes `signed_claims_v1` (`server/consensus_admin.go:39`); no image is cut before Task 12, so no voter ever runs a build that advertises the feature without implementing it.

```bash
cd "$W" && git add go.mod go.sum MODULE.bazel MODULE.bazel.lock fsm/table_registry.go $(git ls-files -m '*BUILD.bazel') && git commit -m "chore(deps): pin arbiter-core $CORE_TAG

Brings arbiter-proto $PROTO_TAG. fsm.TableIncarnation gains the inert
owner_indexer_id field TestTableRegistryMirrorsProto pins to the proto.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 2: Write the failing tests**

Create `fsm/signed_claims_params_test.go`. It reuses `authorityFixture` (`fsm/promotion_test.go:16`), `mustNewFSM`, `mustApply`, `testPubkey`, `registerActive` (`fsm/fsm_test.go`), `consensusUpdate`, `signedConsensusUpdate`, `rejectConsensusUnchanged`, `registryUpdate`, `testRegistryParams` (`fsm/consensus_updates_test.go`), `seed`, `ev`, `addCmd`, `retireCmd` (`fsm/apply_table_registry_test.go`), `baseTransitionOn` (`fsm/table_set_transition_seal_test.go:43`), `mustGenesisID`, `enabledRegistryFSM` (`fsm/table_registry_test.go`), `activateLanes` (`fsm/client_lanes_params_test.go:16`), `rotatedConsensusHistory`, `configuredConsensusFSM` (`fsm/consensus_history_test.go`) and `snapshotBytes`:

```go
package fsm

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// indexerSigner1 and indexerSigner2 load the indexer keys arbiter-core
// publishes for enrolment fixtures (authoritytest, Task 3; their addresses
// are authoritytest.IndexerAddr0 and IndexerAddr1). testRegistryParams'
// founding indexer 1 signs with key 0; every other indexer in these tests
// signs with key 1.
func indexerSigner1(t testing.TB) *authority.Signer {
	return authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
}

func indexerSigner2(t testing.TB) *authority.Signer {
	return authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
}

// enrollment is the enrolment statement signer signs for (indexer, node) on
// f's network and genesis.
func enrollment(t *testing.T, f *FSM, signer *authority.Signer, indexer uint64, node string) string {
	t.Helper()
	jws, err := signer.SignSNodeEnrollment(authority.SNodeEnrollmentStatement{NetworkID: f.st.Params.NetworkID,
		GenesisSnapshotID: mustGenesisID(t, f), IndexerID: indexer, SNodeNodeID: node})
	if err != nil {
		t.Fatal(err)
	}
	return jws
}

// siEntry is the si_indexers entry of indexer on f, enrolled by signer.
func siEntry(t *testing.T, f *FSM, signer *authority.Signer, indexer, activation uint64, node string) arbiter.SIIndexerEntry {
	t.Helper()
	return arbiter.SIIndexerEntry{IndexerID: indexer, ActivationBlock: activation, Signer: signer.Address(),
		SNodeNodeID: node, EnrollmentJWS: enrollment(t, f, signer, indexer, node)}
}

// foundingEntry restates testRegistryParams' founding indexer 1 (activation
// block 100) with its SNode s1.
func foundingEntry(t *testing.T, f *FSM) arbiter.SIIndexerEntry {
	t.Helper()
	return siEntry(t, f, indexerSigner1(t), 1, 100, "s1")
}

// testVerifiers lists ids under the keys registerActive registers them with.
func testVerifiers(ids ...string) []arbiter.VerifierEntry {
	out := make([]arbiter.VerifierEntry, 0, len(ids))
	for _, id := range ids {
		out = append(out, arbiter.VerifierEntry{NodeID: id, Ed25519Pubkey: testPubkey(id[len(id)-1])})
	}
	return out
}

// claimsUpdate is f's next update: its committed authority set, table
// registry and client lanes carried forward, the given lists, and a
// max_writers that covers every entry.
func claimsUpdate(t *testing.T, f *FSM, si []arbiter.SIIndexerEntry, verifiers []arbiter.VerifierEntry) arbiter.ConsensusParamsUpdate {
	t.Helper()
	u := consensusUpdate(t, f, slices.Clone(f.st.Params.AuthorityAddresses), max(f.st.Params.maxWriters(), uint64(len(si))))
	u.TableRegistry = testRegistryParams()
	if lanes := f.st.Params.ClientLanes; lanes != nil {
		c := *lanes
		u.ClientLanes = &c
	}
	u.SIIndexers, u.Verifiers = si, verifiers
	return u
}

// claimsFSM is everything the signed-claims activation needs: a seeded
// registry (founding indexer 1, activation block 100, Legacy db.old) with
// max_writers 2, and source s1 and verifiers v1..v3 registered and Active.
func claimsFSM(t *testing.T) (*FSM, *authority.Signer) {
	t.Helper()
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	u := registryUpdate(t, f, params, testRegistryParams())
	u.MaxWriters = 2
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	seed(t, f, arbiter.LegacyTable{DatabaseID: "db", TableID: "old", Created: ev(5, "0x5", 0)})
	baseTransitionOn(t, f)
	return f, a
}

// activateClaims commits the signed-claims activation on claimsFSM: the
// founding entry plus extra, and verifiers v1..v3.
func activateClaims(t *testing.T, f *FSM, a *authority.Signer, extra ...arbiter.SIIndexerEntry) {
	t.Helper()
	si := append([]arbiter.SIIndexerEntry{foundingEntry(t, f)}, extra...)
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, si, testVerifiers("v1", "v2", "v3"))))
	if !f.SignedClaimsActive() {
		t.Fatal("the activation did not commit si_indexers")
	}
}

// Rule 1: the first update that sets si_indexers.
func TestSignedClaimsFirstSet(t *testing.T) {
	verifiers := testVerifiers("v1", "v2", "v3")
	for _, tc := range []struct {
		name   string
		update func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate
		reason string
	}{
		{"without verifiers", func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			return claimsUpdate(t, f, []arbiter.SIIndexerEntry{foundingEntry(t, f)}, nil)
		}, "must set the verifiers"},
		{"with two verifiers", func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			return claimsUpdate(t, f, []arbiter.SIIndexerEntry{foundingEntry(t, f)}, testVerifiers("v1", "v2"))
		}, "at least 3 verifiers"},
		{"verifiers alone", func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			return claimsUpdate(t, f, nil, verifiers)
		}, "only together with the first si_indexers"},
		{"without the founding indexer", func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			return claimsUpdate(t, f, []arbiter.SIIndexerEntry{siEntry(t, f, indexerSigner2(t), 2, 150, "s2")}, verifiers)
		}, "founding indexer 1"},
		{"the founding activation restated wrongly", func(t *testing.T, f *FSM) arbiter.ConsensusParamsUpdate {
			return claimsUpdate(t, f, []arbiter.SIIndexerEntry{siEntry(t, f, indexerSigner1(t), 1, 101, "s1")}, verifiers)
		}, "founding indexer 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := claimsFSM(t)
			rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, tc.update(t, f)), tc.reason)
			if f.SignedClaimsActive() {
				t.Fatal("a refused update activated signed claims")
			}
		})
	}
	t.Run("before the table registry", func(t *testing.T) {
		a, params := authorityFixture(t)
		f := mustNewFSM(t, params)
		u := consensusUpdate(t, f, params.AuthorityAddresses, 1)
		u.SIIndexers, u.Verifiers = []arbiter.SIIndexerEntry{foundingEntry(t, f)}, verifiers
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, u), "before the table registry")
	})
	t.Run("together with the table registry", func(t *testing.T) {
		a, params := authorityFixture(t)
		f := mustNewFSM(t, params)
		u := registryUpdate(t, f, params, testRegistryParams())
		u.SIIndexers, u.Verifiers = []arbiter.SIIndexerEntry{foundingEntry(t, f)}, verifiers
		mustApply(t, f, signedConsensusUpdate(t, a, u))
		reg := f.st.TableRegistry
		if inc := reg.Live("db.t"); inc == nil || inc.OwnerIndexerID == nil || *inc.OwnerIndexerID != 1 {
			t.Fatalf("genesis incarnation = %+v, want owner 1", inc)
		}
		if reg.Seeded || reg.SeededIndexers != nil {
			t.Fatalf("an unseeded registry recorded seeded indexers %v", reg.SeededIndexers)
		}
	})
	t.Run("the founding indexer need not have the lowest id", func(t *testing.T) {
		f, a := claimsFSM(t)
		activateClaims(t, f, a, siEntry(t, f, indexerSigner2(t), 0, 150, "s0"))
		if got := f.st.Params.SIIndexers; len(got) != 2 || got[0].IndexerID != 0 || got[1].IndexerID != 1 {
			t.Fatalf("si_indexers = %+v, want entries 0 and 1 sorted by id", got)
		}
	})
	t.Run("max_writers covers the entries", func(t *testing.T) {
		f, a := claimsFSM(t)
		valid := signedConsensusUpdate(t, a, claimsUpdate(t, f, []arbiter.SIIndexerEntry{foundingEntry(t, f),
			siEntry(t, f, indexerSigner2(t), 2, 150, "s2"), siEntry(t, f, indexerSigner2(t), 3, 160, "s3")}, verifiers))
		invalid := *valid.UpdateConsensusParams
		invalid.Update.MaxWriters = 2
		rejectConsensusUnchanged(t, f, wire.Command{UpdateConsensusParams: &invalid}, "max_writers")
	})
}

// Rule 2: once set, carried, append-only, immutable but for a signer rotation.
func TestSIIndexersAreAppendOnly(t *testing.T) {
	f, a := claimsFSM(t)
	second := siEntry(t, f, indexerSigner2(t), 2, 150, "s2")
	activateClaims(t, f, a, second)
	founding := f.st.Params.SIIndexers[0]
	verifiers := testVerifiers("v1", "v2", "v3")
	b, _ := authorityFixture(t)
	changed := func(mutate func(*arbiter.SIIndexerEntry)) arbiter.SIIndexerEntry {
		e := founding
		mutate(&e)
		return e
	}
	for _, tc := range []struct {
		name   string
		si     []arbiter.SIIndexerEntry
		reason string
	}{
		{"omitted", nil, "must carry the si_indexers"},
		{"an entry removed", []arbiter.SIIndexerEntry{founding}, "cannot remove si_indexers entry 2"},
		{"activation block changed", []arbiter.SIIndexerEntry{changed(func(e *arbiter.SIIndexerEntry) { e.ActivationBlock = 101 }), second}, "cannot change si_indexers entry 1"},
		{"snode changed", []arbiter.SIIndexerEntry{changed(func(e *arbiter.SIIndexerEntry) { e.SNodeNodeID = "s9" }), second}, "cannot change si_indexers entry 1"},
		{"statement changed without a rotation", []arbiter.SIIndexerEntry{changed(func(e *arbiter.SIIndexerEntry) { e.EnrollmentJWS += "x" }), second}, "without rotating its signer"},
		{"a rotation that keeps the old statement", []arbiter.SIIndexerEntry{changed(func(e *arbiter.SIIndexerEntry) { e.Signer = authoritytest.IndexerAddr1 }), second}, "enrolment statement"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			u := claimsUpdate(t, f, tc.si, verifiers)
			// An authority change too, so only the list rule can refuse it.
			u.AuthorityAddresses = []string{b.Address()}
			rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, u), tc.reason)
		})
	}
	carried := claimsUpdate(t, f, []arbiter.SIIndexerEntry{founding, second}, verifiers)
	carried.AuthorityAddresses = []string{b.Address()}
	version := f.st.TableRegistry.Version
	mustApply(t, f, signedConsensusUpdate(t, a, carried))
	if !f.AuthorityAllowed(b.Address()) || f.st.TableRegistry.Version != version {
		t.Fatal("an update carrying unchanged lists must rotate the authority without a registry bump")
	}
	rotated := changed(func(e *arbiter.SIIndexerEntry) {
		e.Signer = authoritytest.IndexerAddr1
		e.EnrollmentJWS = enrollment(t, f, indexerSigner2(t), 1, "s1")
	})
	mustApply(t, f, signedConsensusUpdate(t, b, claimsUpdate(t, f, []arbiter.SIIndexerEntry{rotated, second}, verifiers)))
	if e, _ := f.siIndexerLocked(1); e.Signer != authoritytest.IndexerAddr1 || f.st.TableRegistry.Version != version+1 {
		t.Fatalf("rotated entry %+v, registry version %d -> %d; want the new signer and exactly one bump", e, version, f.st.TableRegistry.Version)
	}
}

// Rule 3: an added entry activates after the registry cursor.
func TestEnrolmentActivatesAfterTheRegistryCursor(t *testing.T) {
	verifiers := testVerifiers("v1", "v2", "v3")
	f, a := claimsFSM(t)
	mustApply(t, f, wire.Command{AdvanceL2Cursor: &wire.AdvanceL2Cursor{To: arbiter.L2BlockRef{Number: 120, Hash: "0x120"}}})
	// The founding entry restates activation block 100, far behind the
	// cursor: the rule judges only the entries an update adds.
	activateClaims(t, f, a)
	founding := f.st.Params.SIIndexers[0]
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
		[]arbiter.SIIndexerEntry{founding, siEntry(t, f, indexerSigner2(t), 2, 120, "s2")}, verifiers)), "after the registry cursor block 120")
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
		[]arbiter.SIIndexerEntry{founding, siEntry(t, f, indexerSigner2(t), 2, 121, "s2")}, verifiers)))

	// Before the founding seed the cursor that counts is the block that seed
	// commits, activation_block - 1.
	ga, gparams := authorityFixture(t)
	g := mustNewFSM(t, gparams)
	mustApply(t, g, signedConsensusUpdate(t, ga, registryUpdate(t, g, gparams, testRegistryParams())))
	rejectConsensusUnchanged(t, g, signedConsensusUpdate(t, ga, claimsUpdate(t, g,
		[]arbiter.SIIndexerEntry{foundingEntry(t, g), siEntry(t, g, indexerSigner2(t), 2, 99, "s2")}, verifiers)), "after the registry cursor block 99")
	mustApply(t, g, signedConsensusUpdate(t, ga, claimsUpdate(t, g,
		[]arbiter.SIIndexerEntry{foundingEntry(t, g), siEntry(t, g, indexerSigner2(t), 2, 100, "s2")}, verifiers)))
}

// Rule 4: every new or rotated entry carries a statement its signer signed.
func TestEnrolmentStatementIsVerified(t *testing.T) {
	f, a := claimsFSM(t)
	activateClaims(t, f, a)
	founding, verifiers := f.st.Params.SIIndexers[0], testVerifiers("v1", "v2", "v3")
	good := authority.SNodeEnrollmentStatement{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: mustGenesisID(t, f), IndexerID: 2, SNodeNodeID: "s2"}
	entry := func(t *testing.T, signer *authority.Signer, stmt authority.SNodeEnrollmentStatement) arbiter.SIIndexerEntry {
		t.Helper()
		jws, err := signer.SignSNodeEnrollment(stmt)
		if err != nil {
			t.Fatal(err)
		}
		return arbiter.SIIndexerEntry{IndexerID: 2, ActivationBlock: 150, Signer: authoritytest.IndexerAddr1, SNodeNodeID: "s2", EnrollmentJWS: jws}
	}
	for _, tc := range []struct {
		name   string
		signer *authority.Signer
		edit   func(*authority.SNodeEnrollmentStatement)
	}{
		{"signed by another key", indexerSigner1(t), func(*authority.SNodeEnrollmentStatement) {}},
		{"another network", indexerSigner2(t), func(s *authority.SNodeEnrollmentStatement) { s.NetworkID = "another-network" }},
		{"another genesis", indexerSigner2(t), func(s *authority.SNodeEnrollmentStatement) { s.GenesisSnapshotID = "0xanother" }},
		{"another indexer", indexerSigner2(t), func(s *authority.SNodeEnrollmentStatement) { s.IndexerID = 3 }},
		{"another snode", indexerSigner2(t), func(s *authority.SNodeEnrollmentStatement) { s.SNodeNodeID = "s3" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt := good
			tc.edit(&stmt)
			rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
				[]arbiter.SIIndexerEntry{founding, entry(t, tc.signer, stmt)}, verifiers)), "enrolment statement")
		})
	}
	// arbiter-core's frozen fixtures (Task 3) through the FSM's own rule: both
	// published statements and the two fixture entries verify in their fixture
	// context (network authoritytest.NetworkID, genesis
	// authoritytest.GenesisSnapshotID), and no field they do not sign for does.
	t.Run("arbiter-core fixtures", func(t *testing.T) {
		if len(authoritytest.SNodeEnrollmentVectors) != 2 {
			t.Fatalf("arbiter-core published %d enrolment vectors, want those of indexers 0 and 1", len(authoritytest.SNodeEnrollmentVectors))
		}
		for i, v := range authoritytest.SNodeEnrollmentVectors {
			e := arbiter.SIIndexerEntry{IndexerID: v.Statement.IndexerID, ActivationBlock: 1, Signer: v.Signer, SNodeNodeID: v.Statement.SNodeNodeID, EnrollmentJWS: v.JWS}
			if err := verifySIIndexerEnrollment(v.Statement.NetworkID, v.Statement.GenesisSnapshotID, e); err != nil {
				t.Fatalf("vector %d: %v", i, err)
			}
			for _, broken := range []struct {
				name, network string
				edit          func(*arbiter.SIIndexerEntry)
			}{
				{"another signer", v.Statement.NetworkID, func(e *arbiter.SIIndexerEntry) { e.Signer = authoritytest.StrangerAddr }},
				{"another indexer", v.Statement.NetworkID, func(e *arbiter.SIIndexerEntry) { e.IndexerID += 7 }},
				{"another snode", v.Statement.NetworkID, func(e *arbiter.SIIndexerEntry) { e.SNodeNodeID += "-x" }},
				{"another network", "another-network", func(*arbiter.SIIndexerEntry) {}},
			} {
				be := e
				broken.edit(&be)
				if err := verifySIIndexerEnrollment(broken.network, v.Statement.GenesisSnapshotID, be); err == nil {
					t.Fatalf("vector %d with %s verified", i, broken.name)
				}
			}
		}
		for _, e := range []arbiter.SIIndexerEntry{authoritytest.SIIndexerEntry0(), authoritytest.SIIndexerEntry1(150)} {
			if err := verifySIIndexerEnrollment(authoritytest.NetworkID, authoritytest.GenesisSnapshotID, e); err != nil {
				t.Fatalf("authoritytest entry of indexer %d: %v", e.IndexerID, err)
			}
		}
	})
	cmd := signedConsensusUpdate(t, a, claimsUpdate(t, f, []arbiter.SIIndexerEntry{founding, entry(t, indexerSigner2(t), good)}, verifiers))
	previous, genesisID := f.st.Params, mustGenesisID(t, f)
	mustApply(t, f, cmd)
	// verifyConsensusTransition is the restore-time replay path: it reaches
	// the same answer from Params alone, without a clock.
	if _, err := verifyConsensusTransition(previous, genesisID, cmd.UpdateConsensusParams.Update, cmd.UpdateConsensusParams.AuthorityJWS); err != nil {
		t.Fatalf("replaying the enrolment = %v", err)
	}
}

// Rule 5: SNode ids are unique, never a verifier's, never reused.
func TestSNodeNodeIDs(t *testing.T) {
	verifiers := testVerifiers("v1", "v2", "v3")
	t.Run("unique across entries", func(t *testing.T) {
		f, a := claimsFSM(t)
		valid := signedConsensusUpdate(t, a, claimsUpdate(t, f, []arbiter.SIIndexerEntry{foundingEntry(t, f), siEntry(t, f, indexerSigner2(t), 2, 150, "s2")}, verifiers))
		invalid := *valid.UpdateConsensusParams
		invalid.Update.SIIndexers = []arbiter.SIIndexerEntry{invalid.Update.SIIndexers[0], siEntry(t, f, indexerSigner2(t), 2, 150, "s1")}
		rejectConsensusUnchanged(t, f, wire.Command{UpdateConsensusParams: &invalid}, "snode_node_id")
	})
	t.Run("never a verifier's id", func(t *testing.T) {
		f, a := claimsFSM(t)
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
			[]arbiter.SIIndexerEntry{foundingEntry(t, f), siEntry(t, f, indexerSigner2(t), 2, 150, "v1")}, verifiers)), "is also a verifier")
	})
	t.Run("a registered node only as the SNode it is, only at the activation", func(t *testing.T) {
		f, a := claimsFSM(t)
		registerActive(t, f, "v4", arbiter.NodeRoleVerifier)
		mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: "v4", Reason: "retired"}})
		registerActive(t, f, "s7", arbiter.NodeRoleSNode)
		mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: "s7", Reason: "retired"}})
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
			[]arbiter.SIIndexerEntry{siEntry(t, f, indexerSigner1(t), 1, 100, "v4")}, verifiers)), "not as an SNode")
		activateClaims(t, f, a) // s1, the grandfathered SNODE registration, is the founding entry's SNode
		founding := f.st.Params.SIIndexers[0]
		for _, node := range []string{"v4", "s7"} {
			rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
				[]arbiter.SIIndexerEntry{founding, siEntry(t, f, indexerSigner2(t), 2, 150, node)}, verifiers)), "already used by a registered node")
		}
		mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
			[]arbiter.SIIndexerEntry{founding, siEntry(t, f, indexerSigner2(t), 2, 150, "s2")}, verifiers)))
	})
	t.Run("a running node with both roles is not an SNode", func(t *testing.T) {
		f, a := claimsFSM(t)
		f.st.Nodes["s1"].Registration.Roles = []arbiter.NodeRole{arbiter.NodeRoleSNode, arbiter.NodeRoleVerifier}
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
			[]arbiter.SIIndexerEntry{foundingEntry(t, f)}, verifiers)), "not as an SNode")
	})
}

// CONTRACT §3c: the activation's lists are authoritative. Every running SNode
// no entry names and every running verifier the list leaves out is evicted by
// the activation itself (nothing could evict them before it), with one
// registry bump for the activation, its migration and any purge the
// evictions complete.
func TestActivationEvictsUnnamedNodes(t *testing.T) {
	f, a := claimsFSM(t)
	registerActive(t, f, "s7", arbiter.NodeRoleSNode)    // a stale second writer
	registerActive(t, f, "v4", arbiter.NodeRoleVerifier) // a stale verifier
	mustApply(t, f, addCmd(t, f, "db", "p", ev(100, "0x100", 0), ev(101, "0x101", 0), 1))
	mustApply(t, f, retireCmd("db", wire.TableRetireReasonTableDeleted, ev(102, "0x102", 0), "p"))
	seq := f.st.TableRegistry.Live("db.p").Seq
	for _, node := range []string{"s1", "v1", "v2", "v3"} {
		mustApply(t, f, wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: node, IncarnationSeq: seq}})
	}
	if inc := f.st.TableRegistry.incarnation(seq); inc.Status != TableStatusPurging {
		t.Fatalf("db.p must wait for s7 and v4, status %s", inc.Status)
	}
	version := f.st.TableRegistry.Version
	activateClaims(t, f, a) // names s1 and lists v1..v3 only
	for _, id := range []string{"s7", "v4"} {
		if n := f.st.Nodes[id]; n.Status != NodeEvicted || n.Reason != "not named by the signed-claims activation" {
			t.Fatalf("%s = %+v, want evicted with reason %q", id, n, "not named by the signed-claims activation")
		}
	}
	for _, id := range []string{"s1", "v1", "v2", "v3"} {
		if f.st.Nodes[id].Status != NodeActive {
			t.Fatalf("%s lost its status", id)
		}
	}
	if inc := f.st.TableRegistry.incarnation(seq); inc.Status != TableStatusPurged {
		t.Fatalf("evicting the stale nodes must complete the purge, status %s", inc.Status)
	}
	if f.st.TableRegistry.Version != version+1 {
		t.Fatalf("registry version %d -> %d, want exactly one bump", version, f.st.TableRegistry.Version)
	}
}

// CONTRACT §3c: the writer gate counts only the writers the activation keeps.
// A stale second SNode the activation evicts does not need room under
// max_writers, and the activation leaves exactly one running writer. An
// update that is not the activation still counts every running writer.
func TestActivationWriterGateExcludesItsEvictions(t *testing.T) {
	// claimsFSM allows two writers: s1, the founding entry's SNode, and s7, a
	// stale second writer no entry names.
	f, a := claimsFSM(t)
	registerActive(t, f, "s7", arbiter.NodeRoleSNode)
	plain := claimsUpdate(t, f, nil, nil)
	plain.MaxWriters = 1
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, plain), "max_writers 1 is below 2 non-evicted writers")
	update := claimsUpdate(t, f, []arbiter.SIIndexerEntry{foundingEntry(t, f)}, testVerifiers("v1", "v2", "v3"))
	update.MaxWriters = 1
	mustApply(t, f, signedConsensusUpdate(t, a, update))
	if n := f.st.Nodes["s7"]; n.Status != NodeEvicted || n.Reason != notNamedByActivationReason {
		t.Fatalf("s7 = %+v, want evicted by the activation", n)
	}
	var writers []string
	for id, n := range f.st.Nodes {
		if n.Status != NodeEvicted && hasRole(n.Registration.Roles, arbiter.NodeRoleSNode) {
			writers = append(writers, id)
		}
	}
	slices.Sort(writers)
	if !slices.Equal(writers, []string{"s1"}) || f.st.Params.MaxWriters != 1 {
		t.Fatalf("running writers %v under max_writers %d, want [s1] under 1", writers, f.st.Params.MaxWriters)
	}
}

// Rule 6 at the activation (CONTRACT §3c): a running verifier the activation
// lists must be listed with the key it registered; an evicted one need not be
// listed and keeps its earlier eviction.
func TestVerifiersAtActivation(t *testing.T) {
	t.Run("a listed verifier registered under another key is refused", func(t *testing.T) {
		f, a := claimsFSM(t)
		verifiers := testVerifiers("v1", "v2", "v3")
		verifiers[0].Ed25519Pubkey = testPubkey('9')
		rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
			[]arbiter.SIIndexerEntry{foundingEntry(t, f)}, verifiers)), `lists verifier "v1" with a key other than the one it registered`)
	})
	t.Run("an evicted verifier need not be listed", func(t *testing.T) {
		f, a := claimsFSM(t)
		registerActive(t, f, "v4", arbiter.NodeRoleVerifier)
		mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: "v4", Reason: "retired"}})
		activateClaims(t, f, a)
		if n := f.st.Nodes["v4"]; n.Status != NodeEvicted || n.Reason != "retired" {
			t.Fatalf("v4 = %+v, want its earlier eviction untouched", n)
		}
	})
}

// Rule 6 afterwards: carried, at least three listed, removal evicts.
func TestVerifiersAreGoverned(t *testing.T) {
	f, a := claimsFSM(t)
	registerActive(t, f, "v4", arbiter.NodeRoleVerifier)
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, []arbiter.SIIndexerEntry{foundingEntry(t, f)}, testVerifiers("v1", "v2", "v3", "v4"))))
	si := slices.Clone(f.st.Params.SIIndexers) // never hand committed state to an update
	b, _ := authorityFixture(t)
	omitted := claimsUpdate(t, f, si, nil)
	omitted.AuthorityAddresses = []string{b.Address()}
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, omitted), "must carry the verifiers")
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, si, testVerifiers("v1", "v2"))), "at least 3 verifiers")
	// Removing v4 evicts its node in the same Apply.
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, si, testVerifiers("v1", "v2", "v3"))))
	if n := f.st.Nodes["v4"]; n.Status != NodeEvicted || n.Reason != "removed from verifiers" {
		t.Fatalf("v4 = %+v, want evicted with reason %q", n, "removed from verifiers")
	}
	for _, id := range []string{"v1", "v2", "v3"} {
		if f.st.Nodes[id].Status != NodeActive {
			t.Fatalf("%s lost its status", id)
		}
	}
	// A listed verifier whose node is evicted does not count toward the floor.
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, si, testVerifiers("v1", "v2", "v3", "v4"))))
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, si, testVerifiers("v1", "v2", "v4"))), "would leave 2 non-evicted listed verifiers")
	// A listed verifier that has not registered yet does.
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, si, testVerifiers("v1", "v2", "v4", "v5"))))
	if f.st.Nodes["v3"].Status != NodeEvicted {
		t.Fatal("v3 was removed and must be evicted")
	}
}

// Rule 6 eviction and contract §3a: one registry bump per Apply.
func TestVerifierRemovalCompletesAPurgeWithOneRegistryBump(t *testing.T) {
	f, a := claimsFSM(t)
	registerActive(t, f, "v4", arbiter.NodeRoleVerifier)
	mustApply(t, f, addCmd(t, f, "db", "p", ev(100, "0x100", 0), ev(101, "0x101", 0), 1))
	mustApply(t, f, retireCmd("db", wire.TableRetireReasonTableDeleted, ev(102, "0x102", 0), "p"))
	seq := f.st.TableRegistry.Live("db.p").Seq
	for _, node := range []string{"s1", "v1", "v2", "v3"} {
		mustApply(t, f, wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: node, IncarnationSeq: seq}})
	}
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, []arbiter.SIIndexerEntry{foundingEntry(t, f)}, testVerifiers("v1", "v2", "v3", "v4"))))
	if inc := f.st.TableRegistry.incarnation(seq); inc.Status != TableStatusPurging {
		t.Fatalf("db.p must still wait for v4, status %s", inc.Status)
	}
	version := f.st.TableRegistry.Version
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, slices.Clone(f.st.Params.SIIndexers), testVerifiers("v1", "v2", "v3"))))
	if inc := f.st.TableRegistry.incarnation(seq); inc.Status != TableStatusPurged {
		t.Fatalf("removing the last missing verifier must complete the purge, status %s", inc.Status)
	}
	if f.st.TableRegistry.Version != version+1 {
		t.Fatalf("registry version %d -> %d, want exactly one bump", version, f.st.TableRegistry.Version)
	}
}

// Rule 7: the drain gate applies to the activation unchanged.
func TestSignedClaimsActivationRequiresDrainedAuthorityWork(t *testing.T) {
	for _, tc := range []struct {
		name   string
		seed   func(*State)
		reason string
	}{
		{"promotion", func(st *State) { st.PendingPromotions[1] = &PendingPromotion{} }, "promotions"},
		{"cleanup", func(st *State) { st.PendingCleanups[1] = &arbiter.UnsafeCleanup{} }, "cleanup commands"},
		{"unsafe parts", func(st *State) {
			st.PromotedUnsafe[arbiter.TablePartition{TableID: "db.t", PartitionID: "p0"}] = map[string]bool{"part": true}
		}, "unsafe parts"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, a := claimsFSM(t)
			tc.seed(f.st)
			rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
				[]arbiter.SIIndexerEntry{foundingEntry(t, f)}, testVerifiers("v1", "v2", "v3"))), tc.reason)
		})
	}
}

// Rule 8 and contract §3a: the migration, and one bump per si_indexers change.
func TestSignedClaimsActivationMigratesOwners(t *testing.T) {
	f, a := claimsFSM(t)
	mustApply(t, f, addCmd(t, f, "db", "n", ev(100, "0x100", 0), ev(101, "0x101", 0), 1))
	reg := f.st.TableRegistry
	version := reg.Version
	changed := f.RegistryChanged()
	activateClaims(t, f, a)
	if len(reg.Incarnations) != 3 {
		t.Fatalf("incarnations = %d, want db.t, db.old and db.n", len(reg.Incarnations))
	}
	for _, inc := range reg.Incarnations {
		if inc.OwnerIndexerID == nil || *inc.OwnerIndexerID != 1 {
			t.Fatalf("incarnation %d (%s) owner = %v, want the founding indexer 1", inc.Seq, inc.Key(), inc.OwnerIndexerID)
		}
	}
	if reg.Incarnations[0].OwnerIndexerID == reg.Incarnations[1].OwnerIndexerID {
		t.Fatal("incarnations share one owner pointer")
	}
	if !slices.Equal(reg.SeededIndexers, []uint64{1}) || reg.Version != version+1 {
		t.Fatalf("seeded %v, registry version %d -> %d; want [1] and exactly one bump", reg.SeededIndexers, version, reg.Version)
	}
	select {
	case <-changed:
	default:
		t.Fatal("registry watchers were not woken")
	}
	// An enrolment reaches registry followers with exactly one more bump.
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f,
		append(slices.Clone(f.st.Params.SIIndexers), siEntry(t, f, indexerSigner2(t), 2, 150, "s2")), testVerifiers("v1", "v2", "v3"))))
	if reg.Version != version+2 || !slices.Equal(reg.SeededIndexers, []uint64{1}) {
		t.Fatalf("after the enrolment: version %d, seeded %v", reg.Version, reg.SeededIndexers)
	}
}

func TestSignedClaimsAreNeverGenesisParameters(t *testing.T) {
	for field, mutate := range map[string]func(*Params){
		"si_indexers": func(p *Params) {
			p.SIIndexers = []arbiter.SIIndexerEntry{{IndexerID: 1, ActivationBlock: 100, Signer: authoritytest.IndexerAddr0, SNodeNodeID: "s1", EnrollmentJWS: "x"}}
		},
		"verifiers": func(p *Params) { p.Verifiers = testVerifiers("v1", "v2", "v3") },
	} {
		params := testParams()
		mutate(&params)
		if _, err := New(params); !errors.Is(err, ErrGenesisParams) || !strings.Contains(err.Error(), field) {
			t.Fatalf("New with %s = %v, want ErrGenesisParams naming it", field, err)
		}
	}
}

func TestSignedClaimsRestoreRefusesBootstrapLists(t *testing.T) {
	candidate, params, genesis := rotatedConsensusHistory(t)
	candidate.st.BootstrapParams.SIIndexers = []arbiter.SIIndexerEntry{{IndexerID: 1, ActivationBlock: 100,
		Signer: authoritytest.IndexerAddr0, SNodeNodeID: "s1", EnrollmentJWS: "x"}}
	target := configuredConsensusFSM(t, params, genesis)
	before, original := snapshotBytes(t, target), target.st
	err := target.Restore(io.NopCloser(bytes.NewReader(snapshotBytes(t, candidate))))
	if err == nil || !strings.Contains(err.Error(), "si_indexers") {
		t.Fatalf("Restore error = %v, want one naming si_indexers", err)
	}
	if target.st != original || !bytes.Equal(before, snapshotBytes(t, target)) {
		t.Fatal("a rejected container replaced live state")
	}
}

func TestParamsDigestUnchangedWithoutSignedClaims(t *testing.T) {
	params := testParams()
	params.TableRegistry = testRegistryParams()
	params.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	before, err := params.ConsensusDigest()
	if err != nil {
		t.Fatal(err)
	}
	params.SIIndexers, params.Verifiers = []arbiter.SIIndexerEntry{}, []arbiter.VerifierEntry{}
	if after, _ := params.ConsensusDigest(); after != before {
		t.Fatal("empty si_indexers and verifiers changed the params digest")
	}
	params.SIIndexers = []arbiter.SIIndexerEntry{{IndexerID: 1, ActivationBlock: 100, Signer: authoritytest.IndexerAddr0, SNodeNodeID: "s1", EnrollmentJWS: "x"}}
	withSI, _ := params.ConsensusDigest()
	params.SIIndexers, params.Verifiers = nil, testVerifiers("v1", "v2", "v3")
	withVerifiers, _ := params.ConsensusDigest()
	if withSI == before || withVerifiers == before || withSI == withVerifiers {
		t.Fatal("si_indexers and verifiers must each be bound by the params digest")
	}
}

// TestPreActivationParamsDigestIsTheLegacyDigest pins the digest a
// pre-activation update binds in previous_params_digest to the canonical form
// Params had before stage 1: both new fields are omitempty, so a network that
// never sets them keeps every digest it ever signed.
func TestPreActivationParamsDigestIsTheLegacyDigest(t *testing.T) {
	// legacyParams is fsm.Params before stage 1, field for field in
	// declaration order, the order CanonicalDigest's json.Marshal emits.
	type legacyParams struct {
		SchemaSnapshotID              string                       `json:"schema_snapshot_id"`
		ExecutorProfileID             string                       `json:"executor_profile_id"`
		AuthorityAddresses            []string                     `json:"authority_addresses,omitempty"`
		NetworkID                     string                       `json:"network_id"`
		MaxWriters                    uint64                       `json:"max_writers,omitempty"`
		ArtifactDispositionCapability uint32                       `json:"artifact_disposition_capability,omitempty"`
		TableRegistry                 *arbiter.TableRegistryParams `json:"table_registry,omitempty"`
		ClientLanes                   *arbiter.ClientLaneParams    `json:"client_lanes,omitempty"`
	}
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	activateLanes(t, f, a, params, 256)
	view, err := f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	p := view.Current
	want, err := replay.CanonicalDigest("arbiter/consensus-params/v1", legacyParams{
		SchemaSnapshotID: p.SchemaSnapshotID, ExecutorProfileID: p.ExecutorProfileID, AuthorityAddresses: p.AuthorityAddresses,
		NetworkID: p.NetworkID, MaxWriters: p.MaxWriters, ArtifactDispositionCapability: p.ArtifactDispositionCapability,
		TableRegistry: p.TableRegistry, ClientLanes: p.ClientLanes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if view.ParamsDigest != want {
		t.Fatalf("pre-activation params digest %s, want the pre-stage-1 digest %s", view.ParamsDigest, want)
	}
}

func TestConsensusParamsViewCarriesSignedClaims(t *testing.T) {
	f, a := claimsFSM(t)
	activateClaims(t, f, a)
	view, err := f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	if len(view.Current.SIIndexers) != 1 || view.Current.SIIndexers[0].SNodeNodeID != "s1" || len(view.Current.Verifiers) != 3 {
		t.Fatalf("view = %+v / %+v", view.Current.SIIndexers, view.Current.Verifiers)
	}
	if digest, _ := f.st.Params.ConsensusDigest(); view.ParamsDigest != digest {
		t.Fatal("the view's digest is not the committed params digest")
	}
	view.Current.SIIndexers[0].SNodeNodeID = "forged"
	view.Current.Verifiers[0].Ed25519Pubkey[0] ^= 0xff
	if f.st.Params.SIIndexers[0].SNodeNodeID != "s1" || !bytes.Equal(f.st.Params.Verifiers[0].Ed25519Pubkey, testPubkey('1')) {
		t.Fatal("ConsensusParamsView leaked the signed-claims lists")
	}
}

func TestSignedClaimsLookups(t *testing.T) {
	f, a := claimsFSM(t)
	if f.signedClaimsActiveLocked() {
		t.Fatal("signed claims active before the activation")
	}
	if _, ok := f.siIndexerLocked(1); ok {
		t.Fatal("an entry before the activation")
	}
	activateClaims(t, f, a, siEntry(t, f, indexerSigner2(t), 2, 150, "s2"))
	if e, ok := f.siIndexerLocked(2); !ok || e.SNodeNodeID != "s2" {
		t.Fatalf("siIndexerLocked(2) = %+v, %v", e, ok)
	}
	if e, ok := f.siIndexerBySNodeLocked("s1"); !ok || e.IndexerID != 1 {
		t.Fatalf("siIndexerBySNodeLocked(s1) = %+v, %v", e, ok)
	}
	if _, ok := f.siIndexerBySNodeLocked("v1"); ok {
		t.Fatal("a verifier resolved to an si_indexers entry")
	}
	ctx, err := f.messageContextLocked()
	if err != nil || ctx != (authority.MessageContext{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: mustGenesisID(t, f)}) {
		t.Fatalf("messageContextLocked = %+v, %v", ctx, err)
	}
}
```

Create `server/signed_claims_test.go`:

```go
package server

import (
	"bytes"
	"testing"

	"github.com/sentioxyz/arbiter-core"

	"github.com/sentioxyz/arbiter/fsm"
)

func TestConsensusMutableParamsCarrySignedClaims(t *testing.T) {
	if got := consensusMutableParams(fsm.Params{}); got.GetSiIndexers() != nil || got.GetVerifiers() != nil {
		t.Fatalf("pre-activation params rendered %v / %v", got.GetSiIndexers(), got.GetVerifiers())
	}
	key := bytes.Repeat([]byte{7}, 32)
	got := consensusMutableParams(fsm.Params{
		SIIndexers: []arbiter.SIIndexerEntry{{IndexerID: 0, ActivationBlock: 5508931, Signer: "0x22a67f498e669cbde18a0eecfff74a72c48331f4",
			SNodeNodeID: "snode-1", EnrollmentJWS: "a.b.c"}},
		Verifiers: []arbiter.VerifierEntry{{NodeID: "verifier-1", Ed25519Pubkey: key}},
	})
	if e := got.GetSiIndexers(); len(e) != 1 || e[0].GetIndexerId() != 0 || e[0].GetActivationBlock() != 5508931 ||
		e[0].GetSigner() != "0x22a67f498e669cbde18a0eecfff74a72c48331f4" || e[0].GetSnodeNodeId() != "snode-1" || e[0].GetEnrollmentJws() != "a.b.c" {
		t.Fatalf("si_indexers = %v", e)
	}
	if v := got.GetVerifiers(); len(v) != 1 || v[0].GetNodeId() != "verifier-1" || !bytes.Equal(v[0].GetEd25519Pubkey(), key) {
		t.Fatalf("verifiers = %v", v)
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && go test ./fsm/ ./server/ -run 'SignedClaims|SIIndexers|Enrolment|SNodeNodeIDs|ActivationEvictsUnnamedNodes|ActivationWriterGate|Verifier|ParamsDigest|ConsensusParamsViewCarries|ConsensusMutableParamsCarrySignedClaims' -v`
Expected: build failure (`unknown field SIIndexers in struct literal of type fsm.Params`, `f.SignedClaimsActive undefined`, `undefined: verifySIIndexerEnrollment`).

- [ ] **Step 3: Implement the parameters and their rules**

`fsm/state.go`: in `Params`, replace the `MaxWriters` comment (lines 94-97) and append the two lists after `ClientLanes` (line 112):

```go
	// MaxWriters caps non-evicted SNODE registrations. Zero means one for
	// embedded callers and snapshots written before this parameter existed.
	// Before the signed-claims activation values above one only opt into a
	// larger membership pool (sources stay hash-selected); afterwards it must
	// cover every si_indexers entry, each of which owns one source.
	MaxWriters uint64 `json:"max_writers,omitempty"`
```

```go
	// SIIndexers is the enrolled storage-integrity indexers (spec 2026-10-10
	// D3, §6.1): nil until the signed-claims activation sets it together with
	// Verifiers; then carried by every update, sorted by indexer id, appended
	// to and never shrunk, and an existing entry may only rotate its signer.
	SIIndexers []arbiter.SIIndexerEntry `json:"si_indexers,omitempty"`
	// Verifiers is the governed verifier set (spec 2026-10-10 D7): nil until
	// the activation, then carried by every update; a verifier node whose
	// entry an update removes is evicted by that update.
	Verifiers []arbiter.VerifierEntry `json:"verifiers,omitempty"`
```

`fsm/table_registry.go`: replace `TableRegistryState` (lines 131-137):

```go
// TableRegistryState is the replicated dynamic SI table registry.
type TableRegistryState struct {
	Version uint64 `json:"version"`
	Seeded  bool   `json:"seeded"`
	// SeededIndexers is the ascending set of SI indexers whose Legacy seed is
	// committed (spec 2026-10-10 §6.3). It is empty before the signed-claims
	// activation; afterwards it holds the founding indexer exactly when
	// Seeded, which keeps meaning "the founding indexer is seeded".
	SeededIndexers []uint64            `json:"seeded_indexers,omitempty"`
	Cursor         L2Position          `json:"cursor"`
	Incarnations   []*TableIncarnation `json:"incarnations"`
	live           map[string][]int    // derived: key -> indexes of its incarnations, oldest first
}
```

`fsm/params_identity.go`, in `normalizeConsensusParams` before `return p`, and `fsm/consensus_reads.go`, in `cloneParams` before `return params` (`params` instead of `p`):

```go
	p.SIIndexers = cloneSIIndexers(p.SIIndexers)
	p.Verifiers = cloneVerifiers(p.Verifiers)
```

`fsm/watch.go`, in `NewWithNotify` after the `client_lanes` refusal (line 96):

```go
	// si_indexers and verifiers are installed only by the signed-claims
	// activation, a signed update after the table registry.
	if len(params.SIIndexers) != 0 {
		return nil, fmt.Errorf("%w: si_indexers is set only by a consensus update, not at genesis", ErrGenesisParams)
	}
	if len(params.Verifiers) != 0 {
		return nil, fmt.Errorf("%w: verifiers is set only by a consensus update, not at genesis", ErrGenesisParams)
	}
```

`fsm/consensus_history.go`, after the bootstrap `client_lanes` refusal (line 25):

```go
	if len(st.BootstrapParams.SIIndexers) != 0 || len(st.BootstrapParams.Verifiers) != 0 {
		return fmt.Errorf("snapshot bootstrap consensus params: si_indexers and verifiers are set only by a consensus update, not at genesis")
	}
```

Create `fsm/signed_claims.go`:

```go
package fsm

import (
	"bytes"
	"fmt"
	"maps"
	"slices"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
)

// Stage 1 of the multi-source storage-integrity network (housegate spec
// 2026-10-10 §6) starts with one authority-signed update that sets si_indexers
// and verifiers: the signed-claims activation. Before it every behaviour is
// byte for byte today's.

// Eviction reasons a consensus update records (spec 2026-10-10 §6.1, CONTRACT
// §3c): a verifier node whose (node_id, ed25519_pubkey) a later update no
// longer lists, and a registered node the activation's lists do not name.
const (
	removedFromVerifiersReason = "removed from verifiers"
	notNamedByActivationReason = "not named by the signed-claims activation"
)

// signedClaimsActiveLocked reports whether the signed-claims activation has
// committed. The caller holds f.mu.
func (f *FSM) signedClaimsActiveLocked() bool { return len(f.st.Params.SIIndexers) > 0 }

// SignedClaimsActive is signedClaimsActiveLocked for readers outside Apply,
// such as gateways that must drop stage-1 fields before the activation.
func (f *FSM) SignedClaimsActive() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.signedClaimsActiveLocked()
}

// siIndexerLocked returns the si_indexers entry of indexer id.
func (f *FSM) siIndexerLocked(id uint64) (arbiter.SIIndexerEntry, bool) {
	return findSIIndexer(f.st.Params.SIIndexers, id)
}

// siIndexerBySNodeLocked returns the si_indexers entry whose SNode is nodeID.
func (f *FSM) siIndexerBySNodeLocked(nodeID string) (arbiter.SIIndexerEntry, bool) {
	return findSIIndexerBySNode(f.st.Params.SIIndexers, nodeID)
}

// messageContextLocked is the domain every signed SNode and verifier message
// binds (spec 2026-10-10 D6): this network and its genesis snapshot.
func (f *FSM) messageContextLocked() (authority.MessageContext, error) {
	genesisID, err := f.genesisSnapshotIDLocked()
	if err != nil {
		return authority.MessageContext{}, err
	}
	if genesisID == "" {
		return authority.MessageContext{}, fmt.Errorf("signed claims require a published genesis")
	}
	return authority.MessageContext{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: genesisID}, nil
}

func findSIIndexer(entries []arbiter.SIIndexerEntry, id uint64) (arbiter.SIIndexerEntry, bool) {
	for _, e := range entries {
		if e.IndexerID == id {
			return e, true
		}
	}
	return arbiter.SIIndexerEntry{}, false
}

func findSIIndexerBySNode(entries []arbiter.SIIndexerEntry, nodeID string) (arbiter.SIIndexerEntry, bool) {
	for _, e := range entries {
		if e.SNodeNodeID == nodeID {
			return e, true
		}
	}
	return arbiter.SIIndexerEntry{}, false
}

// cloneSIIndexers detaches a list; empty becomes nil so equality and digests
// never tell an absent list from an empty one.
func cloneSIIndexers(in []arbiter.SIIndexerEntry) []arbiter.SIIndexerEntry {
	if len(in) == 0 {
		return nil
	}
	return slices.Clone(in)
}

// cloneVerifiers detaches a list including its key bytes; empty becomes nil.
func cloneVerifiers(in []arbiter.VerifierEntry) []arbiter.VerifierEntry {
	if len(in) == 0 {
		return nil
	}
	out := make([]arbiter.VerifierEntry, len(in))
	for i, v := range in {
		out[i] = arbiter.VerifierEntry{NodeID: v.NodeID, Ed25519Pubkey: bytes.Clone(v.Ed25519Pubkey)}
	}
	return out
}

// listsVerifier reports whether entries list nodeID under pubkey.
func listsVerifier(entries []arbiter.VerifierEntry, nodeID string, pubkey []byte) bool {
	return slices.ContainsFunc(entries, func(v arbiter.VerifierEntry) bool {
		return v.NodeID == nodeID && bytes.Equal(v.Ed25519Pubkey, pubkey)
	})
}

// verifySIIndexerEnrollment checks entry e's enrolment statement (spec
// 2026-10-10 §6.2): an ES256K JWS by e.Signer, purpose
// arbiter-snode-enrollment-v1, over this network, its genesis snapshot,
// e.IndexerID and e.SNodeNodeID. It reads no clock, so Apply and restore
// decide alike.
func verifySIIndexerEnrollment(networkID, genesisID string, e arbiter.SIIndexerEntry) error {
	stmt := authority.SNodeEnrollmentStatement{NetworkID: networkID, GenesisSnapshotID: genesisID, IndexerID: e.IndexerID, SNodeNodeID: e.SNodeNodeID}
	if err := authority.VerifySNodeEnrollment(stmt, e.EnrollmentJWS, e.Signer); err != nil {
		return fmt.Errorf("consensus update si_indexers entry %d enrolment statement: %w", e.IndexerID, err)
	}
	return nil
}

// verifySIIndexersTransition is the signed-history half of the si_indexers
// rules (spec 2026-10-10 §6.1-§6.2); restore replays it over Params alone.
//   - The first update that sets the list needs the table registry, set
//     earlier or by the same update, and restates the founding indexer: the
//     entry whose indexer_id is table_registry.si_indexer_id (entry 0 of the
//     id-sorted list only when that id is the lowest) with activation_block
//     equal to table_registry.activation_block.
//   - Every later update carries the list. No entry is removed; an entry's
//     indexer_id, activation_block and snode_node_id never change; its
//     enrollment_jws stays byte-identical unless its signer rotates.
//   - Every new entry and every rotated one carries an enrolment statement
//     that verifies under its signer.
func verifySIIndexersTransition(previous Params, registry *arbiter.TableRegistryParams, genesisID string, update []arbiter.SIIndexerEntry) ([]arbiter.SIIndexerEntry, error) {
	if len(previous.SIIndexers) == 0 {
		if len(update) == 0 {
			return nil, nil
		}
		if registry == nil {
			return nil, fmt.Errorf("consensus update cannot set si_indexers before the table registry")
		}
		founding, ok := findSIIndexer(update, registry.SIIndexerID)
		if !ok || founding.ActivationBlock != registry.ActivationBlock {
			return nil, fmt.Errorf("consensus update si_indexers must restate the founding indexer %d with activation_block %d", registry.SIIndexerID, registry.ActivationBlock)
		}
		for _, e := range update {
			if err := verifySIIndexerEnrollment(previous.NetworkID, genesisID, e); err != nil {
				return nil, err
			}
		}
		return cloneSIIndexers(update), nil
	}
	if len(update) == 0 {
		return nil, fmt.Errorf("consensus update must carry the si_indexers entries")
	}
	for _, old := range previous.SIIndexers {
		e, ok := findSIIndexer(update, old.IndexerID)
		switch {
		case !ok:
			return nil, fmt.Errorf("consensus update cannot remove si_indexers entry %d", old.IndexerID)
		case e.ActivationBlock != old.ActivationBlock || e.SNodeNodeID != old.SNodeNodeID:
			return nil, fmt.Errorf("consensus update cannot change si_indexers entry %d activation_block or snode_node_id", old.IndexerID)
		case e.Signer == old.Signer && e.EnrollmentJWS != old.EnrollmentJWS:
			return nil, fmt.Errorf("consensus update changes si_indexers entry %d enrollment_jws without rotating its signer", old.IndexerID)
		}
	}
	for _, e := range update {
		if old, existed := findSIIndexer(previous.SIIndexers, e.IndexerID); existed && old.Signer == e.Signer {
			continue
		}
		if err := verifySIIndexerEnrollment(previous.NetworkID, genesisID, e); err != nil {
			return nil, err
		}
	}
	return cloneSIIndexers(update), nil
}

// verifyVerifiersTransition is the signed-history half of the verifiers rules
// (spec 2026-10-10 D7, §6.1): the list is first set by the update that first
// sets si_indexers, then carried by every update with at least
// VerifierSelectN entries, and no listed verifier shares an SNode's id.
// Eviction of removed verifiers and the non-evicted floor read node records,
// so they run in signedClaimsMembershipLocked.
func verifyVerifiersTransition(previous Params, nextSI []arbiter.SIIndexerEntry, update []arbiter.VerifierEntry) ([]arbiter.VerifierEntry, error) {
	firstSI := len(previous.SIIndexers) == 0 && len(nextSI) > 0
	switch {
	case len(previous.Verifiers) == 0 && len(update) == 0:
		if firstSI {
			return nil, fmt.Errorf("consensus update setting si_indexers must set the verifiers")
		}
		return nil, nil
	case len(previous.Verifiers) == 0 && !firstSI:
		return nil, fmt.Errorf("consensus update sets verifiers only together with the first si_indexers")
	case len(update) == 0:
		return nil, fmt.Errorf("consensus update must carry the verifiers")
	case len(update) < VerifierSelectN:
		return nil, fmt.Errorf("consensus update must list at least %d verifiers, got %d", VerifierSelectN, len(update))
	}
	for _, e := range nextSI {
		if slices.ContainsFunc(update, func(v arbiter.VerifierEntry) bool { return v.NodeID == e.SNodeNodeID }) {
			return nil, fmt.Errorf("consensus update si_indexers entry %d snode_node_id %q is also a verifier", e.IndexerID, e.SNodeNodeID)
		}
	}
	return cloneVerifiers(update), nil
}

// signedClaimsMembershipLocked is the half of the si_indexers and verifiers
// rules that reads committed state (spec 2026-10-10 §6.2, §6.5, CONTRACT
// §3c): the registry cursor, node records and verifier liveness. Like the
// writer and drain gates it runs only in Apply, never on restore, because that
// state may legitimately change after the transition. It returns the nodes
// the update evicts and the reason Apply records for them: at the activation
// every running node its lists do not name, afterwards every verifier the
// update removes. Apply calls it ahead of the writer gate, which leaves the
// activation's evictions out of its count. Nodes are visited in sorted order
// so every replica names the same one.
func (f *FSM) signedClaimsMembershipLocked(next Params) (evict []string, reason string, err error) {
	previous := f.st.Params
	first := len(previous.SIIndexers) == 0 && len(next.SIIndexers) > 0
	nodeIDs := slices.Sorted(maps.Keys(f.st.Nodes))
	if len(next.SIIndexers) > 0 {
		cursor := f.registryEffectiveCursorLocked(next.TableRegistry)
		for _, e := range next.SIIndexers {
			if _, existed := findSIIndexer(previous.SIIndexers, e.IndexerID); existed {
				continue
			}
			// Rule 3: an added entry activates after every event the registry
			// consumed, so deriving with the longer list reproduces every past
			// decision. The founding entry of the first update restates the
			// registry's own activation block instead (rule 1).
			if !(first && e.IndexerID == next.TableRegistry.SIIndexerID) && e.ActivationBlock <= cursor {
				return nil, "", fmt.Errorf("consensus update si_indexers entry %d activation_block %d must be after the registry cursor block %d", e.IndexerID, e.ActivationBlock, cursor)
			}
			// Rule 5: an SNode id is never reused. Only the first update may
			// name a registered node, and only an SNODE registration: the
			// grandfathered source that indexer already runs (a named SNode
			// registered with any other role is refused, CONTRACT §3c).
			if n := f.st.Nodes[e.SNodeNodeID]; n != nil {
				if !first {
					return nil, "", fmt.Errorf("consensus update si_indexers entry %d snode_node_id %q is already used by a registered node", e.IndexerID, e.SNodeNodeID)
				}
				if !slices.Equal(n.Registration.Roles, []arbiter.NodeRole{arbiter.NodeRoleSNode}) {
					return nil, "", fmt.Errorf("consensus update si_indexers entry %d snode_node_id %q is registered with roles %v, not as an SNode", e.IndexerID, e.SNodeNodeID, n.Registration.Roles)
				}
			}
		}
	}
	reason = removedFromVerifiersReason
	if first {
		// CONTRACT §3c: the activation's lists are the authoritative node
		// sets. A running verifier they list under another key than the one
		// it registered is an operator error and refuses the update; every
		// other running node they do not name is evicted below, because
		// nothing could evict it before the activation (no eviction command
		// may be proposed then) and refusing would let one stale
		// registration block the activation for good.
		reason = notNamedByActivationReason
		for _, id := range nodeIDs {
			n := f.st.Nodes[id]
			if n == nil || n.Status == NodeEvicted || !hasRole(n.Registration.Roles, arbiter.NodeRoleVerifier) {
				continue
			}
			listed := slices.ContainsFunc(next.Verifiers, func(v arbiter.VerifierEntry) bool { return v.NodeID == id })
			if listed && !listsVerifier(next.Verifiers, id, n.Registration.Ed25519Pubkey) {
				return nil, "", fmt.Errorf("consensus update lists verifier %q with a key other than the one it registered", id)
			}
		}
		for _, id := range nodeIDs {
			n := f.st.Nodes[id]
			if n == nil || n.Status == NodeEvicted {
				continue
			}
			_, named := findSIIndexerBySNode(next.SIIndexers, id)
			if hasRole(n.Registration.Roles, arbiter.NodeRoleSNode) && !named {
				evict = append(evict, id)
			}
		}
	}
	if len(next.Verifiers) == 0 {
		return evict, reason, nil
	}
	// Rule 6: a running verifier whose (node_id, key) the list does not hold
	// is evicted (at the activation: one the list leaves out; afterwards: one
	// the update removes), and the update keeps at least VerifierSelectN
	// listed verifiers that are not evicted (one not registered yet counts).
	for _, id := range nodeIDs {
		n := f.st.Nodes[id]
		if n != nil && n.Status != NodeEvicted && hasRole(n.Registration.Roles, arbiter.NodeRoleVerifier) &&
			!listsVerifier(next.Verifiers, id, n.Registration.Ed25519Pubkey) && !slices.Contains(evict, id) {
			evict = append(evict, id)
		}
	}
	slices.Sort(evict)
	live := 0
	for _, v := range next.Verifiers {
		if n := f.st.Nodes[v.NodeID]; n == nil || (n.Status != NodeEvicted && !slices.Contains(evict, v.NodeID)) {
			live++
		}
	}
	if live < VerifierSelectN {
		return nil, "", fmt.Errorf("consensus update would leave %d non-evicted listed verifiers; at least %d are required", live, VerifierSelectN)
	}
	return evict, reason, nil
}

// registryEffectiveCursorLocked is the registry block an added si_indexers
// entry must activate after: the committed cursor block, or, while the
// founding indexer is not seeded yet, the block its seed commits
// (activation_block - 1). No entry can then activate inside the founding
// seed's history, where its own seed could never be placed.
func (f *FSM) registryEffectiveCursorLocked(registry *arbiter.TableRegistryParams) uint64 {
	cursor := registry.ActivationBlock - 1
	if reg := f.st.TableRegistry; reg != nil && reg.Cursor.BlockNumber > cursor {
		cursor = reg.Cursor.BlockNumber
	}
	return cursor
}

// migrateRegistryOwnersLocked runs once, when the first si_indexers update
// applies (spec 2026-10-10 §6.3, rule 8): every incarnation that exists
// belonged to the founding indexer, the only SI indexer so far, and the
// founding indexer is seeded exactly when the registry is. Rule 1 requires
// the registry, enabled earlier or by this Apply, so State.TableRegistry is
// non-nil here.
func (f *FSM) migrateRegistryOwnersLocked() {
	reg := f.st.TableRegistry
	founding := f.st.Params.TableRegistry.SIIndexerID
	for _, inc := range reg.Incarnations {
		owner := founding
		inc.OwnerIndexerID = &owner
	}
	if reg.Seeded {
		reg.SeededIndexers = []uint64{founding}
	}
}

// evictNodesLocked evicts the nodes a consensus update leaves out (spec
// 2026-10-10 §6.1, CONTRACT §3c) under reason and reports whether that
// completed a purge, which the caller folds into its single registry bump.
// RegistrationSeqs is left as it is, so a later registration still needs a
// higher seq.
func (f *FSM) evictNodesLocked(ids []string, reason string) bool {
	if len(ids) == 0 {
		return false
	}
	for _, id := range ids {
		n := f.st.Nodes[id]
		n.Status, n.Reason = NodeEvicted, reason
	}
	completed := false
	if reg := f.st.TableRegistry; reg != nil {
		before := purgedCount(reg)
		f.completePurgesLocked()
		completed = purgedCount(reg) != before
	}
	f.emit(Event{Kind: EventMembershipChanged})
	return completed
}
```

`fsm/consensus_updates.go`, in `verifyConsensusTransition` directly after the client-lanes defence in depth (line 154), before the `consensusParamsEqual` check:

```go
	// si_indexers and verifiers (spec 2026-10-10 §6.1, D3, D7) are first set
	// together by the signed-claims activation and carried by every later
	// update. Only their signed-history rules run here, because restore
	// replays this function over Params alone; applyUpdateConsensusParams
	// adds the rules that read committed state (signedClaimsMembershipLocked).
	si, err := verifySIIndexersTransition(previous, next.TableRegistry, genesisID, normalized.SIIndexers)
	if err != nil {
		return Params{}, err
	}
	verifiers, err := verifyVerifiersTransition(previous, si, normalized.Verifiers)
	if err != nil {
		return Params{}, err
	}
	next.SIIndexers, next.Verifiers = si, verifiers
	// Defence in depth: arbiter-core's normalizer already refuses a writer
	// limit below the number of enrolled indexers.
	if uint64(len(next.SIIndexers)) > next.maxWriters() {
		return Params{}, fmt.Errorf("consensus update max_writers %d is below its %d si_indexers entries", next.maxWriters(), len(next.SIIndexers))
	}
```

and in `applyUpdateConsensusParams`, replace the writer gate (lines 43-51, from `var writers uint64` through its `Rejected`) with the state-reading rules followed by a writer gate that leaves out the writers the activation evicts:

```go
	// The half of the si_indexers and verifiers rules that reads committed
	// state (spec 2026-10-10 §6.2, §6.5, CONTRACT §3c);
	// verifyConsensusTransition checked the signed-history half above. It
	// runs ahead of the writer gate because the signed-claims activation
	// evicts, in this same Apply, every running SNode it does not name.
	// Before the activation it refuses and evicts nothing, so every earlier
	// update meets the writer and drain gates exactly as today.
	evict, evictReason, err := f.signedClaimsMembershipLocked(next)
	if err != nil {
		return Rejected{Reason: err.Error()}
	}
	activation := len(f.st.Params.SIIndexers) == 0 && len(next.SIIndexers) > 0
	var writers uint64
	for id, node := range f.st.Nodes {
		if node == nil || node.Status == NodeEvicted || !hasRole(node.Registration.Roles, arbiter.NodeRoleSNode) {
			continue
		}
		// A writer the activation evicts is not a writer it keeps (CONTRACT
		// §3c). Every other update counts every running writer, as before.
		if activation && slices.Contains(evict, id) {
			continue
		}
		writers++
	}
	if next.MaxWriters < writers {
		return Rejected{Reason: fmt.Sprintf("consensus update max_writers %d is below %d non-evicted writers; evict excess writers first", next.MaxWriters, writers)}
	}
```

The drain gate (lines 52-66) stays where it is, after the writer gate. Then replace lines 67-84 of the unchanged file (from `wasEnabled := …` through the lanes `registryChangedLocked` call) with:

```go
	wasEnabled := f.st.Params.TableRegistry != nil
	previousLanes := f.st.Params.ClientLanes
	previousSI := f.st.Params.SIIndexers
	f.st.Params = next
	f.mirrorArtifactDispositionCapabilityLocked()
	if !wasEnabled && f.st.Params.TableRegistry != nil {
		f.enableTableRegistryLocked(genesisID)
	}
	// Setting or raising client lanes is delivered to data-plane nodes through
	// the registry follower, which only accepts a strictly newer version. The
	// bump is a deterministic function of this applied update.
	// Invariant: Params.TableRegistry != nil implies State.TableRegistry !=
	// nil. enableTableRegistryLocked above runs in the same Apply that sets
	// the parameter, restore refuses one without the other
	// (validateTableRegistry), and verifyConsensusTransition refuses lanes
	// and si_indexers without the registry, so f.st.TableRegistry is non-nil
	// whenever registryChanged is set below.
	registryChanged := false
	if lanes := f.st.Params.ClientLanes; lanes != nil && (previousLanes == nil || lanes.MaxLanesPerAccount != previousLanes.MaxLanesPerAccount) {
		registryChanged = true
	}
	// The signed-claims activation gives every incarnation the founding
	// indexer as owner (rule 8). Every committed si_indexers change, the
	// activation, an enrolment or a signer rotation, reaches registry
	// followers the same way (contract §3a).
	if activation {
		f.migrateRegistryOwnersLocked()
		registryChanged = true
	} else if !slices.Equal(previousSI, next.SIIndexers) {
		registryChanged = true
	}
	// The nodes the update leaves out are evicted now (rule 6, CONTRACT §3c),
	// after the new params are in place, which may complete a purge that
	// waited only for them.
	if f.evictNodesLocked(evict, evictReason) {
		registryChanged = true
	}
	// One bump per Apply, however many of the reasons above hold.
	if registryChanged {
		f.registryChangedLocked(f.st.TableRegistry)
	}
```

(keep the rest of the function: `ConsensusEpoch++`, the history append and the event.) Add `"slices"` to the imports.

`server/consensus_admin.go` `consensusMutableParams`: add to the composite literal

```go
		SiIndexers:                    wire.SIIndexerEntriesToPB(p.SIIndexers),
		Verifiers:                     wire.VerifierEntriesToPB(p.Verifiers),
```

- [ ] **Step 4: Run the tests**

Run: `cd "$W" && bazel run //:gazelle && bazel test //fsm:all //server:all`
Expected: all PASS, including `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims` and `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` (nothing before the activation moved), every `TestConsensusUpdate*` (the writer gate counts as before outside the activation), `TestActivationEvictsUnnamedNodes`, `TestActivationWriterGateExcludesItsEvictions`, every `TestClientLanes*`, `TestTableRegistryMirrorsProto` and every determinism test.

- [ ] **Step 5: Commit**

```bash
cd "$W" && git add fsm server && git commit -m "feat(fsm): si_indexers and verifiers consensus parameters

The signed-claims activation sets both lists in one authority-signed
update after the table registry: the founding indexer is restated, every
enrolment statement verifies under its signer, entries are append-only
and activate after the registry cursor, SNode ids are never reused, the
verifier set keeps at least three non-evicted entries and removing one
evicts its node. The activation's lists are authoritative: it evicts
every running node they do not name, its writer gate counts only the
writers it keeps, and it migrates every incarnation to the founding
indexer; every si_indexers change bumps the registry
version once (housegate spec 2026-10-10 D3, D7, §6.1-§6.2, §6.7;
CONTRACT §3c).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 6: arbiter — table registry ownership, per-indexer seed and the seed guard; watcher and auditor (S1)

**Files:**
- Modify: `fsm/apply_table_registry.go:34-71` (`applySeedLegacyTables`), `:73-92` (`applyAdvanceL2Cursor`), `:94-139` (`applyAddTable`), `:174-227` (`applyRetireTables`)
- Modify: `fsm/table_registry.go:260-269` (`TableRegistryView` struct), `:293-318` (`TableRegistryView()`)
- Modify: `fsm/table_registry_validation.go:30-95` (`validateTableRegistry`)
- Create: `fsm/table_registry_owners_test.go`
- Modify: `server/table_registry.go:99-123` (`tableRegistryToPB`), `server/signed_claims_test.go`
- Create: `tableregistry/membership.go`
- Modify: `tableregistry/chain.go:60-142`, `tableregistry/derive.go:21-37`, `:86-146`, `:186-233`, `tableregistry/watcher.go:19-43`, `:76-89`, `:138-139`, `:171-363`, `tableregistry/auditor.go:16-26`, `:58-68`, `:120-354`
- Modify: `tableregistry/derive_test.go:67-75`, `:124`, `:128`, `:138`, `:142`, `:215`, `:218`
- Create: `tableregistry/membership_test.go`, `tableregistry/multi_source_harness_test.go`, `tableregistry/multi_source_test.go`
- Modify: `metrics/metrics.go:57`, `metrics/table_registry_test.go`

**Interfaces:**
- Consumes: Task 5 `fsm.TableIncarnation.OwnerIndexerID`, `fsm.TableRegistryState.SeededIndexers`, `signedClaimsActiveLocked`, `siIndexerLocked`, `findSIIndexer`, `cloneSIIndexers`, the migration and the per-change registry bump, and the test helpers `claimsFSM`, `activateClaims`, `siEntry`, `foundingEntry`, `testVerifiers`, `indexerSigner2(t)`; `CORE_TAG`: `wire.AddTable.OwnerIndexerID *uint64`, `wire.SeedLegacyTables.IndexerID *uint64`, `wire.SIIndexerEntriesToPB`, `authority.SNodeEnrollmentStatement`, `(*authority.Signer).SignSNodeEnrollment`, `authoritytest.MustSigner(t, keyHex)`, `authoritytest.IndexerKeyHex0` / `IndexerKeyHex1`, `authoritytest.SNodeNodeID0` / `SNodeNodeID1`, `authoritytest.VerifierEntries()`, `authoritytest.VerifierNodeID(i)`, `authoritytest.VerifierKey(i)`, `authority.MessageContext`, `authority.TablePurgedBody`, `(*authority.Signer).SignSNodeMessage` with `authority.SNodeMessageTablePurged`, `authority.SignVerifierMessage` with `authority.VerifierMessageTablePurged`, `wire.RecordTablePurged.SignerJWS` / `.Ed25519Signature`; existing FSM reads `L3BlockView`, `ManifestByID`, `BlockDispatchInfo`, `WorkSet`, `ManifestInputs`, `GenesisParams`; `PROTO_TAG`: `pb.TableRegistrySnapshot.SiIndexers` / `.SeededIndexers`, `pb.TableIncarnation.OwnerIndexerId`.
- Produces: fsm: `TableRegistryView.SIIndexers []arbiter.SIIndexerEntry`, `TableRegistryView.SeededIndexers []uint64`; `(f *FSM) applySeedIndexerTables(*wire.SeedLegacyTables) any`, `legacySeedRefusal(reg, tables, activation) string`, `insertSortedUnique([]uint64, uint64) []uint64`, `(f *FSM) addTableOwnerRefusalLocked(*wire.AddTable) string`, `(f *FSM) unseededIndexerGuardLocked(reg, block uint64) string` (message `table registry: indexer <id> must be seeded before block <n>`), `legacyActivation(Params, *TableIncarnation) uint64`, `validateRegistryOwnership(Params, *TableRegistryState) error`; fsm test helpers `twoIndexerFSM`, `ownedAddCmd`, `seedIndexerCmd`, `advanceTo`. tableregistry: `membership`, `membershipOf`, `(membership).signedClaims/activation/owner/ownerField`, `findEntry`, `nextUnseeded`, `sameIndexer`, `isIndexer`, `siCreate{SI, Owner}`, `pendingCreate{Created, Owner}`, `errOwnerChanged`, `(*indexerResolver).checkSeedOwners`, `readIndexerSeedInput`, `deriveIndexerSeed`, `HaltOwnerChanged = "owner_changed"`, `ownerChangedHalt`, `registeredIn`, `(*Watcher).seedIndexer`, `AuditOwnerChanged = "owner_changed"`, `(*Auditor).auditIndexerSeeds`, `(*Auditor).auditIndexerSeed`, `heldAtSeed`, `registeredBefore`, `sameOwner`; harness helpers `testFoundingIndexer`, `testSecondIndexer`, `testSecondActivation`, `multiParams`, `harnessGenesisID`, `harnessEntry(t, genesisID, keyHex, indexer, activation, snode)`, `multiEntries`, `commitUpdate`, `newMultiNode`, `activateMultiSource`, `harnessVerifierKeys`, `newMultiNodeWithDataPlane`, `publishTableSetTransition`, `reportPurged`, `multiChain`, `ownership`, `wantMultiRegistry`, `checkOwnership`, `commandBlock`, `testEntries`, `testMembership`. metrics: the `owner_changed` halt label.

The guard judges the block a command moves the cursor to: `AddTable.Schema`, `RetireTables.Deleted`, `AdvanceL2Cursor.To`. A per-indexer seed needs the committed cursor to have just completed block `activation_block - 1` (Contract conflicts 6), and the watcher seeds exactly there.

- [ ] **Step 1: Write the failing FSM tests**

Create `fsm/table_registry_owners_test.go` (reuses `seededRegistryFSM`, `enabledRegistryFSM`, `addCmd`, `retireCmd`, `ev`, `seed`, `rejectRegistryUnchanged`, `restoredGenesisManifest` and the Task 5 helpers):

```go
package fsm

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// twoIndexerFSM is claimsFSM after the signed-claims activation with the
// founding indexer 1 and indexer 2 (activation block 150, SNode s2). Indexer
// 2 is not seeded and the committed cursor completes block 99.
func twoIndexerFSM(t *testing.T) *FSM {
	t.Helper()
	f, a := claimsFSM(t)
	activateClaims(t, f, a, siEntry(t, f, indexerSigner2(t), 2, 150, "s2"))
	return f
}

// ownedAddCmd is addCmd naming owner.
func ownedAddCmd(t *testing.T, f *FSM, db, table string, created, schemaEv arbiter.L2EventRef, owner uint64) wire.Command {
	t.Helper()
	cmd := addCmd(t, f, db, table, created, schemaEv, 1)
	cmd.AddTable.OwnerIndexerID = &owner
	return cmd
}

// seedIndexerCmd is indexer's Legacy seed at block (hash "0x<block>").
func seedIndexerCmd(indexer, block uint64, tables ...arbiter.LegacyTable) wire.Command {
	return wire.Command{SeedLegacyTables: &wire.SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: block, Hash: fmt.Sprintf("0x%d", block)},
		Tables: tables, IndexerID: &indexer}}
}

// advanceTo completes block (hash "0x<block>").
func advanceTo(block uint64) wire.Command {
	return wire.Command{AdvanceL2Cursor: &wire.AdvanceL2Cursor{To: arbiter.L2BlockRef{Number: block, Hash: fmt.Sprintf("0x%d", block)}}}
}

func TestAddTableOwnerIsAbsentBeforeActivation(t *testing.T) {
	f := seededRegistryFSM(t)
	rejectRegistryUnchanged(t, f, ownedAddCmd(t, f, "db", "n", ev(100, "0x100", 0), ev(101, "0x101", 0), 1), "owner_indexer_id must be empty before si_indexers is set")
	mustApply(t, f, addCmd(t, f, "db", "n", ev(100, "0x100", 0), ev(101, "0x101", 0), 1))
	if inc := f.st.TableRegistry.Live("db.n"); inc.OwnerIndexerID != nil {
		t.Fatalf("a pre-activation incarnation has owner %d", *inc.OwnerIndexerID)
	}
}

func TestAddTableOwnerAfterActivation(t *testing.T) {
	f := twoIndexerFSM(t)
	mustApply(t, f, advanceTo(149))
	mustApply(t, f, seedIndexerCmd(2, 149))
	created, schema := ev(150, "0x150", 0), ev(151, "0x151", 0)
	rejectRegistryUnchanged(t, f, addCmd(t, f, "db2", "x", created, schema, 1), "owner_indexer_id is required once si_indexers is set")
	rejectRegistryUnchanged(t, f, ownedAddCmd(t, f, "db2", "x", created, schema, 9), "owner indexer 9 has no si_indexers entry")
	rejectRegistryUnchanged(t, f, ownedAddCmd(t, f, "db2", "x", ev(149, "0x149", 3), schema, 2), "created before its owner indexer 2 activated at block 150")
	mustApply(t, f, ownedAddCmd(t, f, "db2", "x", created, schema, 2))
	mustApply(t, f, ownedAddCmd(t, f, "db", "y", ev(152, "0x152", 0), ev(153, "0x153", 0), 1))
	for key, want := range map[string]uint64{"db2.x": 2, "db.y": 1} {
		if inc := f.st.TableRegistry.Live(key); inc == nil || inc.OwnerIndexerID == nil || *inc.OwnerIndexerID != want {
			t.Fatalf("%s = %+v, want owner %d", key, inc, want)
		}
	}
}

func TestUnseededIndexerGuard(t *testing.T) {
	f := twoIndexerFSM(t)
	const want = "table registry: indexer 2 must be seeded before block 150"
	rejectRegistryUnchanged(t, f, advanceTo(150), want)
	rejectRegistryUnchanged(t, f, ownedAddCmd(t, f, "db", "q", ev(149, "0x149", 0), ev(150, "0x150", 0), 1), want)
	rejectRegistryUnchanged(t, f, retireCmd("db", wire.TableRetireReasonTableDeleted, ev(150, "0x150", 0), "old"), want)
	// Block 149 is still before the activation.
	mustApply(t, f, ownedAddCmd(t, f, "db", "q", ev(148, "0x148", 0), ev(149, "0x149", 0), 1))
	mustApply(t, f, advanceTo(149))
	mustApply(t, f, seedIndexerCmd(2, 149))
	mustApply(t, f, advanceTo(150))
	mustApply(t, f, retireCmd("db", wire.TableRetireReasonTableDeleted, ev(151, "0x151", 0), "old"))
}

func TestSeedIndexerLegacyTables(t *testing.T) {
	pre := seededRegistryFSM(t)
	rejectRegistryUnchanged(t, pre, seedIndexerCmd(2, 149), "indexer_id is set only once si_indexers is committed")
	f := twoIndexerFSM(t)
	rejectRegistryUnchanged(t, f, seedIndexerCmd(1, 99), "the founding indexer is seeded without indexer_id")
	rejectRegistryUnchanged(t, f, seedIndexerCmd(3, 149), "indexer 3 has no si_indexers entry")
	rejectRegistryUnchanged(t, f, seedIndexerCmd(2, 148), "activation_block - 1 (149)")
	rejectRegistryUnchanged(t, f, seedIndexerCmd(2, 149), "needs the cursor to complete block 149 first")
	mustApply(t, f, advanceTo(149))
	rejectRegistryUnchanged(t, f, seedIndexerCmd(2, 149, arbiter.LegacyTable{DatabaseID: "db2", TableID: "late", Created: ev(150, "0x150", 0)}), "must be created before activation")
	rejectRegistryUnchanged(t, f, seedIndexerCmd(2, 149, arbiter.LegacyTable{DatabaseID: "db", TableID: "old", Created: ev(5, "0x5", 0)}), "already registered")
	a := arbiter.LegacyTable{DatabaseID: "db2", TableID: "a", Created: ev(120, "0x120", 0)}
	b := arbiter.LegacyTable{DatabaseID: "db2", TableID: "b", Created: ev(130, "0x130", 1)}
	rejectRegistryUnchanged(t, f, seedIndexerCmd(2, 149, b, a), "sorted")
	reg := f.st.TableRegistry
	version, cursor := reg.Version, reg.Cursor
	mustApply(t, f, seedIndexerCmd(2, 149, a, b))
	for _, key := range []string{"db2.a", "db2.b"} {
		if inc := reg.Live(key); inc == nil || inc.Origin != TableOriginLegacy || inc.Status != TableStatusLegacy || inc.OwnerIndexerID == nil || *inc.OwnerIndexerID != 2 {
			t.Fatalf("%s = %+v, want a Legacy incarnation of indexer 2", key, inc)
		}
	}
	if reg.Live("db2.a").OwnerIndexerID == reg.Live("db2.b").OwnerIndexerID {
		t.Fatal("incarnations share one owner pointer")
	}
	if !slices.Equal(reg.SeededIndexers, []uint64{1, 2}) || reg.Cursor != cursor || reg.Version != version+1 {
		t.Fatalf("seeded %v cursor %+v version %d -> %d; want [1 2], an unmoved cursor and one bump", reg.SeededIndexers, reg.Cursor, version, reg.Version)
	}
	rejectRegistryUnchanged(t, f, seedIndexerCmd(2, 149), "indexer 2 is already seeded")
}

// TestFoundingSeedAfterActivationRecordsItsOwner is contract §3a: a founding
// seed committed after the signed-claims activation (the activation update
// itself enabled the registry) gives its Legacy tables the founding owner and
// seeds the founding indexer, with exactly one registry bump.
func TestFoundingSeedAfterActivationRecordsItsOwner(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	u := registryUpdate(t, f, params, testRegistryParams())
	u.SIIndexers, u.Verifiers = []arbiter.SIIndexerEntry{foundingEntry(t, f)}, testVerifiers("v1", "v2", "v3")
	mustApply(t, f, signedConsensusUpdate(t, a, u))
	reg := f.st.TableRegistry
	if reg.Seeded || reg.SeededIndexers != nil {
		t.Fatalf("the activation seeded an unseeded registry: %v", reg.SeededIndexers)
	}
	version := reg.Version
	seed(t, f, arbiter.LegacyTable{DatabaseID: "db", TableID: "old", Created: ev(5, "0x5", 0)})
	if inc := reg.Live("db.old"); inc == nil || inc.OwnerIndexerID == nil || *inc.OwnerIndexerID != 1 {
		t.Fatalf("founding Legacy incarnation = %+v, want owner 1", inc)
	}
	if !reg.Seeded || !slices.Equal(reg.SeededIndexers, []uint64{1}) || reg.Version != version+1 {
		t.Fatalf("seeded %v %v, version %d -> %d", reg.Seeded, reg.SeededIndexers, version, reg.Version)
	}
}

func TestTableRegistryViewCarriesOwnership(t *testing.T) {
	f := twoIndexerFSM(t)
	view, ok := f.TableRegistryView()
	if !ok || len(view.SIIndexers) != 2 || !slices.Equal(view.SeededIndexers, []uint64{1}) {
		t.Fatalf("view si_indexers %+v seeded %v", view.SIIndexers, view.SeededIndexers)
	}
	if inc := view.Live("db.old"); inc == nil || inc.OwnerIndexerID == nil || *inc.OwnerIndexerID != 1 {
		t.Fatalf("db.old in the view = %+v", inc)
	}
	*view.Live("db.old").OwnerIndexerID = 7
	view.SIIndexers[0].SNodeNodeID = "forged"
	view.SeededIndexers[0] = 7
	if *f.st.TableRegistry.Live("db.old").OwnerIndexerID != 1 || f.st.Params.SIIndexers[0].SNodeNodeID != "s1" || f.st.TableRegistry.SeededIndexers[0] != 1 {
		t.Fatal("TableRegistryView leaked ownership state")
	}
	pre, _ := enabledRegistryFSM(t).TableRegistryView()
	if pre.SIIndexers != nil || pre.SeededIndexers != nil || pre.Incarnations[0].OwnerIndexerID != nil {
		t.Fatalf("a pre-activation view carries ownership: %+v", pre)
	}
}

func TestValidateRegistryOwnership(t *testing.T) {
	// twoOwners is twoIndexerFSM with indexer 2 seeded (Legacy db2.a) and
	// owning the chain table db2.x.
	twoOwners := func(t *testing.T) *FSM {
		t.Helper()
		f := twoIndexerFSM(t)
		mustApply(t, f, advanceTo(149))
		mustApply(t, f, seedIndexerCmd(2, 149, arbiter.LegacyTable{DatabaseID: "db2", TableID: "a", Created: ev(120, "0x120", 0)}))
		mustApply(t, f, ownedAddCmd(t, f, "db2", "x", ev(150, "0x150", 0), ev(151, "0x151", 0), 2))
		return f
	}
	validate := func(f *FSM) error { return validateTableRegistry(f.st, restoredGenesisManifest(f.st)) }
	if err := validate(twoOwners(t)); err != nil {
		t.Fatalf("an honest two-owner registry is refused: %v", err)
	}
	if err := validate(enabledRegistryFSM(t)); err != nil {
		t.Fatalf("a pre-activation registry is refused: %v", err)
	}
	two, nine := uint64(2), uint64(9)
	for _, tc := range []struct {
		name   string
		mutate func(*State)
		want   string
	}{
		{"an incarnation without owner", func(st *State) { st.TableRegistry.Live("db2.x").OwnerIndexerID = nil }, "has no owner"},
		{"an unenrolled owner", func(st *State) { st.TableRegistry.Live("db2.x").OwnerIndexerID = &nine }, "names owner 9 without an si_indexers entry"},
		{"a genesis table of another indexer", func(st *State) { st.TableRegistry.Live("db.t").OwnerIndexerID = &two }, "must be owned by the founding indexer 1"},
		{"a legacy table created at its owner's activation", func(st *State) { st.TableRegistry.Live("db2.a").Created.BlockNumber = 150 }, "has no pre-activation creation"},
		{"a legacy table of an unseeded indexer", func(st *State) { st.TableRegistry.SeededIndexers = []uint64{1} }, "belongs to unseeded indexer 2"},
		{"a chain table created before its owner's activation", func(st *State) { st.TableRegistry.Live("db2.x").Created.BlockNumber = 140 }, "before its owner indexer 2 activated at block 150"},
		{"seeded_indexers out of order", func(st *State) { st.TableRegistry.SeededIndexers = []uint64{2, 1} }, "strictly ascending"},
		{"seeded_indexers names an unenrolled indexer", func(st *State) { st.TableRegistry.SeededIndexers = []uint64{1, 2, 9} }, "names indexer 9"},
		{"the seeded founding indexer missing from seeded_indexers", func(st *State) { st.TableRegistry.SeededIndexers = []uint64{2} }, "disagrees with the seeded flag"},
		{"owners without si_indexers", func(st *State) {
			st.Params.SIIndexers = nil
			st.TableRegistry.SeededIndexers = nil
			st.TableRegistry.Incarnations = st.TableRegistry.Incarnations[:2] // db.t and db.old, both founding-owned
		}, "has an owner without si_indexers"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := twoOwners(t)
			tc.mutate(f.st)
			f.st.TableRegistry.rebuildLive()
			if err := validate(f); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateTableRegistry = %v, want %q", err, tc.want)
			}
		})
	}
}
```

Append to `server/signed_claims_test.go` (add `"slices"` to its imports):

```go
func TestTableRegistryToPBCarriesOwnership(t *testing.T) {
	owner := uint64(1)
	view := fsm.TableRegistryView{Version: 9, Seeded: true,
		SIIndexers: []arbiter.SIIndexerEntry{{IndexerID: 0, ActivationBlock: 100, Signer: "0x22a67f498e669cbde18a0eecfff74a72c48331f4", SNodeNodeID: "snode-1", EnrollmentJWS: "a.b.c"},
			{IndexerID: 1, ActivationBlock: 120, Signer: "0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3", SNodeNodeID: "snode-b", EnrollmentJWS: "d.e.f"}},
		SeededIndexers: []uint64{0, 1},
		Incarnations:   []fsm.TableIncarnation{{Seq: 1, DatabaseID: "dbb", TableID: "n", Origin: fsm.TableOriginChain, Status: fsm.TableStatusPending, OwnerIndexerID: &owner}},
	}
	got := tableRegistryToPB(view)
	if len(got.GetSiIndexers()) != 2 || got.GetSiIndexers()[1].GetSnodeNodeId() != "snode-b" || !slices.Equal(got.GetSeededIndexers(), []uint64{0, 1}) {
		t.Fatalf("snapshot ownership = %v / %v", got.GetSiIndexers(), got.GetSeededIndexers())
	}
	if inc := got.GetIncarnations()[0]; inc.OwnerIndexerId == nil || inc.GetOwnerIndexerId() != 1 {
		t.Fatalf("incarnation owner = %v", inc.OwnerIndexerId)
	}
	pre := tableRegistryToPB(fsm.TableRegistryView{Incarnations: []fsm.TableIncarnation{{Seq: 1}}})
	if pre.GetSiIndexers() != nil || pre.GetSeededIndexers() != nil || pre.GetIncarnations()[0].OwnerIndexerId != nil {
		t.Fatal("a pre-activation snapshot must carry no ownership field")
	}
}
```

Run: `cd "$W" && go test ./fsm/ ./server/ -run 'AddTableOwner|UnseededIndexerGuard|SeedIndexerLegacyTables|FoundingSeedAfterActivation|TableRegistryViewCarriesOwnership|ValidateRegistryOwnership|TableRegistryToPBCarriesOwnership' -v`
Expected: build failure (`view.SIIndexers undefined (type TableRegistryView has no field or method SIIndexers)`); with only the view fields added, the command tests FAIL (owners are ignored, the per-indexer seed is taken as a second founding seed: `table registry is already seeded`).

- [ ] **Step 2: Implement ownership, the per-indexer seed and the guard in the FSM**

`fsm/apply_table_registry.go`: replace `applySeedLegacyTables` (lines 34-71) with:

```go
func (f *FSM) applySeedLegacyTables(c *wire.SeedLegacyTables) any {
	if c.IndexerID != nil {
		return f.applySeedIndexerTables(c)
	}
	reg, params, reason := f.registryLocked(false)
	if reason != "" {
		return Rejected{Reason: reason}
	}
	if reg.Seeded {
		return Rejected{Reason: "table registry is already seeded"}
	}
	if c.AtBlock.Number != params.ActivationBlock-1 || c.AtBlock.Hash == "" {
		return Rejected{Reason: fmt.Sprintf("table registry seed must name block activation_block - 1 (%d) with its hash", params.ActivationBlock-1)}
	}
	if reason := legacySeedRefusal(reg, c.Tables, params.ActivationBlock); reason != "" {
		return Rejected{Reason: reason}
	}
	// After the signed-claims activation the founding seed records its owner
	// like every other incarnation and seeds the founding indexer (contract
	// §3a); before it, both stay absent.
	active := f.signedClaimsActiveLocked()
	for _, lt := range c.Tables {
		created := lt.Created
		inc := &TableIncarnation{DatabaseID: lt.DatabaseID, TableID: lt.TableID, Origin: TableOriginLegacy,
			Status: TableStatusLegacy, Created: &created}
		if active {
			owner := params.SIIndexerID
			inc.OwnerIndexerID = &owner
		}
		reg.appendIncarnation(inc)
	}
	reg.Seeded = true
	if active {
		reg.SeededIndexers = insertSortedUnique(reg.SeededIndexers, params.SIIndexerID)
	}
	reg.Cursor = L2Position{BlockNumber: c.AtBlock.Number, BlockHash: c.AtBlock.Hash, BlockComplete: true}
	f.registryChangedLocked(reg)
	return Applied{}
}

// applySeedIndexerTables records the Legacy tables of enrolled indexer x
// (spec 2026-10-10 §6.3): the tables x hosted at its activation_block - 1,
// each owned by x. It applies only once signed claims are active and the
// founding indexer is seeded, once per indexer, and only while the committed
// cursor has just completed block activation_block - 1: the guard keeps
// every later block out until then, so the seed sits exactly between the last
// block before x's activation and the first after it. It leaves the cursor
// where it is.
func (f *FSM) applySeedIndexerTables(c *wire.SeedLegacyTables) any {
	reg, params, reason := f.registryLocked(true)
	if reason != "" {
		return Rejected{Reason: reason}
	}
	x := *c.IndexerID
	if !f.signedClaimsActiveLocked() {
		return Rejected{Reason: "table registry seed: indexer_id is set only once si_indexers is committed"}
	}
	if x == params.SIIndexerID {
		return Rejected{Reason: "table registry seed: the founding indexer is seeded without indexer_id"}
	}
	entry, ok := f.siIndexerLocked(x)
	if !ok {
		return Rejected{Reason: fmt.Sprintf("table registry seed: indexer %d has no si_indexers entry", x)}
	}
	if slices.Contains(reg.SeededIndexers, x) {
		return Rejected{Reason: fmt.Sprintf("table registry seed: indexer %d is already seeded", x)}
	}
	at := entry.ActivationBlock - 1
	if c.AtBlock.Number != at || c.AtBlock.Hash == "" {
		return Rejected{Reason: fmt.Sprintf("table registry seed of indexer %d must name block activation_block - 1 (%d) with its hash", x, at)}
	}
	if reg.Cursor != (L2Position{BlockNumber: at, BlockHash: c.AtBlock.Hash, BlockComplete: true}) {
		return Rejected{Reason: fmt.Sprintf("table registry seed of indexer %d needs the cursor to complete block %d first", x, at)}
	}
	if reason := legacySeedRefusal(reg, c.Tables, entry.ActivationBlock); reason != "" {
		return Rejected{Reason: reason}
	}
	for _, lt := range c.Tables {
		created, owner := lt.Created, x
		reg.appendIncarnation(&TableIncarnation{DatabaseID: lt.DatabaseID, TableID: lt.TableID, Origin: TableOriginLegacy,
			Status: TableStatusLegacy, Created: &created, OwnerIndexerID: &owner})
	}
	reg.SeededIndexers = insertSortedUnique(reg.SeededIndexers, x)
	f.registryChangedLocked(reg)
	return Applied{}
}

// legacySeedRefusal checks a seed's tables (ruling R8): complete ids, keys
// sorted and unique, each created before activation and not registered yet.
// "" admits. The messages are the founding seed's, unchanged.
func legacySeedRefusal(reg *TableRegistryState, tables []arbiter.LegacyTable, activation uint64) string {
	previous := ""
	for _, lt := range tables {
		key := arbiter.TableKey(lt.DatabaseID, lt.TableID)
		if lt.DatabaseID == "" || lt.TableID == "" {
			return "table registry seed: legacy table id is incomplete"
		}
		if key <= previous {
			return "table registry seed: legacy tables must be sorted by key and unique"
		}
		previous = key
		if lt.Created.BlockHash == "" || lt.Created.BlockNumber >= activation {
			return fmt.Sprintf("table registry seed: legacy table %s must be created before activation", key)
		}
		if live := reg.Live(key); live != nil && live.Status != TableStatusPurged {
			return fmt.Sprintf("table registry seed: %s is already registered", key)
		}
	}
	return ""
}

// insertSortedUnique returns s with v inserted in order, unchanged when v is
// already present. s is never modified in place.
func insertSortedUnique(s []uint64, v uint64) []uint64 {
	i, found := slices.BinarySearch(s, v)
	if found {
		return s
	}
	return slices.Insert(slices.Clone(s), i, v)
}

// unseededIndexerGuardLocked refuses a registry command whose L2 block is at
// or beyond the activation block of an enrolled indexer whose Legacy seed is
// not committed yet (spec 2026-10-10 §6.3): the watcher seeds it first, at the
// block before. Entries are sorted by indexer id, so every replica names the
// same indexer. The founding entry never trips it: these commands need the
// founding seed, which seeds the founding indexer.
func (f *FSM) unseededIndexerGuardLocked(reg *TableRegistryState, block uint64) string {
	for _, e := range f.st.Params.SIIndexers {
		if block >= e.ActivationBlock && !slices.Contains(reg.SeededIndexers, e.IndexerID) {
			return fmt.Sprintf("table registry: indexer %d must be seeded before block %d", e.IndexerID, e.ActivationBlock)
		}
	}
	return ""
}

// addTableOwnerRefusalLocked is the ownership rule of AddTable (spec
// 2026-10-10 D4, §6.3): once si_indexers is committed the command names its
// owner, an enrolled indexer whose activation block the table's creation
// reached; before, the field is absent (no pre-activation command carries a
// stage-1 field, CONTRACT §0).
func (f *FSM) addTableOwnerRefusalLocked(c *wire.AddTable) string {
	if !f.signedClaimsActiveLocked() {
		if c.OwnerIndexerID != nil {
			return "add table: owner_indexer_id must be empty before si_indexers is set"
		}
		return ""
	}
	if c.OwnerIndexerID == nil {
		return "add table: owner_indexer_id is required once si_indexers is set"
	}
	owner := *c.OwnerIndexerID
	entry, ok := f.siIndexerLocked(owner)
	switch {
	case !ok:
		return fmt.Sprintf("add table: owner indexer %d has no si_indexers entry", owner)
	case c.Created.BlockNumber < entry.ActivationBlock:
		return fmt.Sprintf("add table: table was created before its owner indexer %d activated at block %d", owner, entry.ActivationBlock)
	}
	return ""
}
```

In `applyAdvanceL2Cursor`, between the `switch` (ends line 88) and the cursor assignment (line 89):

```go
	if reason := f.unseededIndexerGuardLocked(reg, to.Number); reason != "" {
		return Rejected{Reason: reason}
	}
```

In `applyAddTable`, after the duplicate-incarnation loop (line 126) and before the incarnation is built:

```go
	if reason := f.addTableOwnerRefusalLocked(c); reason != "" {
		return Rejected{Reason: reason}
	}
	if reason := f.unseededIndexerGuardLocked(reg, c.Schema.BlockNumber); reason != "" {
		return Rejected{Reason: reason}
	}
```

and after the `inc := &TableIncarnation{…}` literal (line 129):

```go
	if c.OwnerIndexerID != nil {
		owner := *c.OwnerIndexerID
		inc.OwnerIndexerID = &owner
	}
```

In `applyRetireTables`, directly after the `admitEvent(c.Deleted)` check (line 189):

```go
	if reason := f.unseededIndexerGuardLocked(reg, c.Deleted.BlockNumber); reason != "" {
		return Rejected{Reason: reason}
	}
```

`fsm/table_registry.go`: add to the `TableRegistryView` struct after `ClientLanes`:

```go
	// SIIndexers is the committed si_indexers list (nil before the
	// signed-claims activation) and SeededIndexers the indexers whose Legacy
	// seed is committed (spec 2026-10-10 §6.3); the registry follower is how
	// data-plane nodes and sentio-node learn enrolment and ownership.
	SIIndexers     []arbiter.SIIndexerEntry
	SeededIndexers []uint64
```

and in `TableRegistryView()` before `return out, true`:

```go
	out.SIIndexers = cloneSIIndexers(f.st.Params.SIIndexers)
	out.SeededIndexers = slices.Clone(reg.SeededIndexers)
```

`fsm/table_registry_validation.go`: in `validateTableRegistry`, change the Legacy case (lines 64-67) to judge the owner's activation block, and validate ownership after the incarnation loop (before the `Seeded` check at line 91); add `"slices"` to the imports:

```go
		case TableOriginLegacy:
			if inc.Created == nil || inc.Created.BlockNumber >= legacyActivation(st.Params, inc) {
				return fmt.Errorf("table registry: legacy incarnation %d has no pre-activation creation", inc.Seq)
			}
```

```go
	if err := validateRegistryOwnership(st.Params, reg); err != nil {
		return err
	}
```

```go
// legacyActivation is the activation block a Legacy incarnation was created
// before: its owner's once owners are recorded (spec 2026-10-10 §6.3), the
// founding indexer's otherwise. The caller checked Params.TableRegistry.
func legacyActivation(params Params, inc *TableIncarnation) uint64 {
	if inc.OwnerIndexerID != nil {
		if e, ok := findSIIndexer(params.SIIndexers, *inc.OwnerIndexerID); ok {
			return e.ActivationBlock
		}
	}
	return params.TableRegistry.ActivationBlock
}

// validateRegistryOwnership checks the stage-1 ownership fields against the
// committed si_indexers (spec 2026-10-10 §6.3). Before the signed-claims
// activation neither exists. Afterwards every incarnation names an enrolled
// owner (a genesis one the founding indexer, a Legacy one a seeded owner, a
// chain one an owner whose activation block its creation reached), and
// seeded_indexers is strictly ascending, names enrolled indexers only and
// holds the founding indexer exactly when the registry is seeded. Restore
// needs it so it accepts exactly what Apply produces; the snapshot task adds
// the version gate.
func validateRegistryOwnership(params Params, reg *TableRegistryState) error {
	entries := params.SIIndexers
	if len(entries) == 0 {
		if len(reg.SeededIndexers) != 0 {
			return fmt.Errorf("table registry: seeded_indexers without si_indexers")
		}
		for _, inc := range reg.Incarnations {
			if inc.OwnerIndexerID != nil {
				return fmt.Errorf("table registry: incarnation %d has an owner without si_indexers", inc.Seq)
			}
		}
		return nil
	}
	founding := params.TableRegistry.SIIndexerID
	for i, id := range reg.SeededIndexers {
		if i > 0 && id <= reg.SeededIndexers[i-1] {
			return fmt.Errorf("table registry: seeded_indexers must be strictly ascending")
		}
		if _, ok := findSIIndexer(entries, id); !ok {
			return fmt.Errorf("table registry: seeded_indexers names indexer %d without an si_indexers entry", id)
		}
	}
	if slices.Contains(reg.SeededIndexers, founding) != reg.Seeded {
		return fmt.Errorf("table registry: seeded_indexers disagrees with the seeded flag")
	}
	for _, inc := range reg.Incarnations {
		if inc.OwnerIndexerID == nil {
			return fmt.Errorf("table registry: incarnation %d has no owner", inc.Seq)
		}
		owner := *inc.OwnerIndexerID
		entry, ok := findSIIndexer(entries, owner)
		if !ok {
			return fmt.Errorf("table registry: incarnation %d names owner %d without an si_indexers entry", inc.Seq, owner)
		}
		switch inc.Origin {
		case TableOriginGenesis:
			if owner != founding {
				return fmt.Errorf("table registry: genesis incarnation %d must be owned by the founding indexer %d", inc.Seq, founding)
			}
		case TableOriginLegacy:
			if !slices.Contains(reg.SeededIndexers, owner) {
				return fmt.Errorf("table registry: legacy incarnation %d belongs to unseeded indexer %d", inc.Seq, owner)
			}
		case TableOriginChain:
			if inc.Created.BlockNumber < entry.ActivationBlock {
				return fmt.Errorf("table registry: chain incarnation %d was created at block %d, before its owner indexer %d activated at block %d", inc.Seq, inc.Created.BlockNumber, owner, entry.ActivationBlock)
			}
		}
	}
	return nil
}
```

`server/table_registry.go` `tableRegistryToPB`: add to the snapshot literal

```go
		SiIndexers:     wire.SIIndexerEntriesToPB(v.SIIndexers),
		SeededIndexers: slices.Clone(v.SeededIndexers),
```

and to each `pb.TableIncarnation` literal `OwnerIndexerId: inc.OwnerIndexerID,` (the view is already a detached copy); add `"slices"` to the imports.

- [ ] **Step 3: Run the FSM and server tests**

Run: `cd "$W" && bazel run //:gazelle && bazel test //fsm:all //server:all`
Expected: all PASS, including both history goldens, every `TestAddTable*`, `TestSeedLegacyTables`, `TestRetireTables*`, `TestRecordTablePurged`, the registry restore tests and `TestTableRegistryMirrorsProto`.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add fsm server && git commit -m "feat(fsm): table ownership, per-indexer Legacy seeds and the seed guard

Once si_indexers is committed every AddTable names an enrolled owner
whose activation its creation reached; an enrolled indexer's Legacy
tables are seeded exactly when the cursor completes the block before its
activation, and no registry command passes that block before; a founding
seed after the activation records its owner too. The registry view and
snapshot carry si_indexers, seeded_indexers and owners, and restore
checks them (housegate spec 2026-10-10 D4, §6.3).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Write the failing tableregistry tests**

Make the mechanical edits `derive_test.go` needs for the new derive shape: at lines 67-75 the `si` helper becomes

```go
	si := func(events ...databases.Event) siCreates {
		out := siCreates{}
		for _, ev := range events {
			if ev.Kind == databases.TableCreated {
				out[ev.Ref] = siCreate{SI: ev.DatabaseID != "other" && ev.DatabaseID != "a.b", Owner: testSIIndexer}
			}
		}
		return out
	}
```

at line 215 `resolved[ev.Ref] = siCreate{SI: sim.indexerAt[ev.Ref] == testSIIndexer, Owner: sim.indexerAt[ev.Ref]}`, at lines 124, 128, 138, 142 and 218 every `derive(…, testParams(), …)` becomes `derive(…, testMembership(), …)`, and add

```go
// testMembership is the pre-activation membership of testParams: the
// founding indexer alone.
func testMembership() membership { return membership{params: testParams()} }
```

Create `tableregistry/membership_test.go`:

```go
package tableregistry

import (
	"errors"
	"math/big"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/l2/databases"
)

// testEntries is a bare si_indexers list for the pure membership checks.
func testEntries() []arbiter.SIIndexerEntry {
	return []arbiter.SIIndexerEntry{{IndexerID: testFoundingIndexer, ActivationBlock: testActivation},
		{IndexerID: testSecondIndexer, ActivationBlock: testSecondActivation}}
}

func TestMembershipJudgesEachIndexerByItsActivation(t *testing.T) {
	pre := membership{params: multiParams()}
	post := membership{params: multiParams(), entries: testEntries()}
	for _, tc := range []struct {
		name    string
		m       membership
		indexer int64
		block   uint64
		si      bool
	}{
		{"the founding indexer at its activation", pre, testFoundingIndexer, testActivation, true},
		{"the founding indexer before its activation", pre, testFoundingIndexer, testActivation - 1, false},
		{"another indexer before the signed-claims activation", pre, testSecondIndexer, 200, false},
		{"an enrolled indexer before its activation", post, testSecondIndexer, testSecondActivation - 1, false},
		{"an enrolled indexer at its activation", post, testSecondIndexer, testSecondActivation, true},
		{"an indexer without an entry", post, 2, 200, false},
		{"the founding indexer after the signed-claims activation", post, testFoundingIndexer, testActivation, true},
	} {
		owner, si := tc.m.owner(big.NewInt(tc.indexer), tc.block)
		if si != tc.si || owner != uint64(tc.indexer) {
			t.Errorf("%s: owner %d si %v, want owner %d si %v", tc.name, owner, si, tc.indexer, tc.si)
		}
	}
	if _, si := post.owner(nil, 200); si {
		t.Error("an inactive database must not open an SI incarnation")
	}
	if _, si := post.owner(new(big.Int).Lsh(big.NewInt(1), 70), 200); si {
		t.Error("an indexer id wider than 64 bits must not open an SI incarnation")
	}
}

func TestDeriveCarriesTheOwnerOnlyOnceSignedClaimsAreActive(t *testing.T) {
	events := []databases.Event{evAt(databases.TableCreated, 121, 0, "dbb", "n"), evAt(databases.TableSchemaSet, 122, 0, "dbb", "n")}
	si := siCreates{events[0].Ref: {SI: true, Owner: testSecondIndexer}}
	pre, err := derive(pending{}, events, membership{params: multiParams()}, nil, si)
	if err != nil || len(pre) != 1 || pre[0].Command.AddTable.OwnerIndexerID != nil {
		t.Fatalf("before the activation derive = %+v, %v; want an AddTable without owner", pre, err)
	}
	post, err := derive(pending{}, events, membership{params: multiParams(), entries: testEntries()}, nil, si)
	if err != nil || len(post) != 1 || post[0].Command.AddTable.OwnerIndexerID == nil || *post[0].Command.AddTable.OwnerIndexerID != testSecondIndexer {
		t.Fatalf("after the activation derive = %+v, %v; want owner %d", post, err, testSecondIndexer)
	}
}

func TestNextUnseededFollowsActivationOrder(t *testing.T) {
	v := fsm.TableRegistryView{Params: multiParams(), SIIndexers: []arbiter.SIIndexerEntry{
		{IndexerID: 0, ActivationBlock: 100}, {IndexerID: 1, ActivationBlock: 130}, {IndexerID: 2, ActivationBlock: 120}, {IndexerID: 3, ActivationBlock: 120}}}
	for _, step := range []struct {
		seeded []uint64
		want   uint64
		ok     bool
	}{
		{nil, 2, true}, // the founding indexer is never this seed's; 2 and 3 tie, the lower id first
		{[]uint64{0, 2}, 3, true},
		{[]uint64{0, 2, 3}, 1, true},
		{[]uint64{0, 1, 2, 3}, 0, false},
	} {
		v.SeededIndexers = step.seeded
		if got, ok := nextUnseeded(v); ok != step.ok || (ok && got.IndexerID != step.want) {
			t.Fatalf("seeded %v: next = %+v, %v; want %d, %v", step.seeded, got, ok, step.want, step.ok)
		}
	}
}

func TestResolverReportsAnOwnerChange(t *testing.T) {
	r := newIndexerResolver(nil, membership{params: multiParams()})
	created := func(block uint64, indexer int64) databases.Event {
		ev := evAt(databases.DatabaseCreated, block, 0, "dbx", "")
		ev.IndexerID = big.NewInt(indexer)
		return ev
	}
	if _, err := r.observe(t.Context(), created(110, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.observe(t.Context(), created(111, 1)); err != nil {
		t.Fatalf("a repeated DatabaseCreated naming the same indexer = %v", err)
	}
	if _, err := r.observe(t.Context(), created(112, 2)); !errors.Is(err, errOwnerChanged) {
		t.Fatalf("a DatabaseCreated naming another indexer = %v, want errOwnerChanged", err)
	}
	if got := r.indexers["dbx"]; got.Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("the resolver must record the indexer it saw, got %v", got)
	}
	if _, err := r.observe(t.Context(), evAt(databases.DatabaseDeleted, 113, 0, "dbx", "")); err != nil {
		t.Fatal(err)
	}
	if _, err := r.observe(t.Context(), created(114, 3)); err != nil {
		t.Fatalf("a DatabaseCreated after a DatabaseDeleted = %v", err)
	}
}

func TestCheckSeedOwners(t *testing.T) {
	r := newIndexerResolver(nil, membership{params: multiParams()})
	r.indexers["dbb"] = big.NewInt(1)
	in := seedInput{At: databases.Header{Number: 119}, Databases: []databases.Database{
		{ID: "dbb", Active: true, IndexerID: big.NewInt(1)}, {ID: "fresh", Active: true, IndexerID: big.NewInt(3)}}}
	if err := r.checkSeedOwners(in); err != nil {
		t.Fatalf("an unchanged owner = %v", err)
	}
	in.Databases[0].IndexerID = big.NewInt(2)
	if err := r.checkSeedOwners(in); !errors.Is(err, errOwnerChanged) {
		t.Fatalf("a moved database = %v, want errOwnerChanged", err)
	}
}
```

Create `tableregistry/multi_source_harness_test.go` (it builds on `newNode`, `mustPropose`, `testAuthority`, `testParams`, `testNetworkID`, `testActivation` from `fsm_harness_test.go` and `newFakeChain`, `blockHash` from `chain_harness_test.go`):

```go
package tableregistry

import (
	"crypto/ed25519"
	"encoding/hex"
	"maps"
	"reflect"
	"slices"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/l2/databases"
)

// The multi-source harness numbers indexers as devnet2 does: the founding
// indexer 0 (indexer-a) and the enrolled indexer 1 (indexer-b), whose
// activation block is 120. Indexer 2 never enrols.
const (
	testFoundingIndexer  = 0
	testSecondIndexer    = 1
	testSecondActivation = 120
)

// multiParams is testParams with the founding indexer 0.
func multiParams() arbiter.TableRegistryParams {
	p := testParams()
	p.SIIndexerID = testFoundingIndexer
	return p
}

// harnessGenesisID is the genesis snapshot id every harness node publishes.
func harnessGenesisID(t *testing.T) string {
	t.Helper()
	view, err := newNode(t, false).f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	return view.GenesisSnapshotID
}

// harnessEntry is an si_indexers entry on the harness network, enrolled
// with keyHex, one of the indexer keys arbiter-core's authoritytest publishes
// (Task 3): IndexerKeyHex0 for the founding indexer, IndexerKeyHex1 for
// indexer 1.
func harnessEntry(t *testing.T, genesisID, keyHex string, indexer, activation uint64, snode string) arbiter.SIIndexerEntry {
	t.Helper()
	signer := authoritytest.MustSigner(t, keyHex)
	jws, err := signer.SignSNodeEnrollment(authority.SNodeEnrollmentStatement{NetworkID: testNetworkID,
		GenesisSnapshotID: genesisID, IndexerID: indexer, SNodeNodeID: snode})
	if err != nil {
		t.Fatal(err)
	}
	return arbiter.SIIndexerEntry{IndexerID: indexer, ActivationBlock: activation, Signer: signer.Address(), SNodeNodeID: snode, EnrollmentJWS: jws}
}

// multiEntries is the founding entry (activation block 100,
// authoritytest.SNodeNodeID0) and, when second, indexer 1's (activation
// block 120, authoritytest.SNodeNodeID1), signed once per test so every
// replica commits the same bytes.
func multiEntries(t *testing.T, second bool) []arbiter.SIIndexerEntry {
	t.Helper()
	genesisID := harnessGenesisID(t)
	entries := []arbiter.SIIndexerEntry{harnessEntry(t, genesisID, authoritytest.IndexerKeyHex0, testFoundingIndexer, testActivation, authoritytest.SNodeNodeID0)}
	if second {
		entries = append(entries, harnessEntry(t, genesisID, authoritytest.IndexerKeyHex1, testSecondIndexer, testSecondActivation, authoritytest.SNodeNodeID1))
	}
	return entries
}

// commitUpdate commits n's next consensus update: every committed parameter
// carried forward, then mutate.
func commitUpdate(t *testing.T, n *node, mutate func(*arbiter.ConsensusParamsUpdate)) {
	t.Helper()
	view, err := n.f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	c := view.Current
	u := arbiter.ConsensusParamsUpdate{NetworkID: c.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID,
		PreviousParamsDigest: view.ParamsDigest, ExpectedEpoch: view.Epoch, ExpectedPromotionSeq: view.PromotionSeq,
		AuthorityAddresses: c.AuthorityAddresses, MaxWriters: c.MaxWriters, TableRegistry: c.TableRegistry,
		ClientLanes: c.ClientLanes, SIIndexers: c.SIIndexers, Verifiers: c.Verifiers}
	mutate(&u)
	jws, err := testAuthority.SignConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	mustPropose(t, n, wire.Command{UpdateConsensusParams: &wire.UpdateConsensusParams{Update: u, AuthorityJWS: jws}})
}

// newMultiNode is newNode whose one consensus update enables the registry of
// founding indexer 0 and commits the signed-claims activation with entries
// and arbiter-core's fixture verifiers verifier-1..3 (authoritytest
// VerifierEntries), which it does not register (spec 2026-10-10 §6.7;
// newMultiNodeWithDataPlane does).
func newMultiNode(t *testing.T, entries []arbiter.SIIndexerEntry) *node {
	t.Helper()
	n := newNode(t, false)
	activateMultiSource(t, n, entries)
	return n
}

// activateMultiSource commits newMultiNode's one consensus update on n.
func activateMultiSource(t *testing.T, n *node, entries []arbiter.SIIndexerEntry) {
	t.Helper()
	commitUpdate(t, n, func(u *arbiter.ConsensusParamsUpdate) {
		registry := multiParams()
		u.TableRegistry = &registry
		u.MaxWriters = uint64(max(1, len(entries)))
		u.SIIndexers, u.Verifiers = entries, authoritytest.VerifierEntries()
	})
}

// harnessVerifierKeys are the keys of arbiter-core's fixture verifiers
// verifier-1..3, the verifiers every harness activation lists.
func harnessVerifierKeys() map[string]ed25519.PrivateKey {
	keys := map[string]ed25519.PrivateKey{}
	for i := 1; i <= 3; i++ {
		keys[authoritytest.VerifierNodeID(i)] = authoritytest.VerifierKey(i)
	}
	return keys
}

// newMultiNodeWithDataPlane is newMultiNode with a data plane registered and
// Active before the activation, while unsigned registrations are still the
// rule: indexer 1's SNode authoritytest.SNodeNodeID1 and the fixture
// verifiers under the keys the activation lists, so the activation names or
// lists each of them and evicts none (CONTRACT §3c). With it, transition
// blocks can be verified and purges completed.
func newMultiNodeWithDataPlane(t *testing.T, entries []arbiter.SIIndexerEntry) *node {
	t.Helper()
	n := newNode(t, false)
	register := func(id string, role arbiter.NodeRole, pubkey []byte) {
		mustPropose(t, n, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
			NodeID: id, Roles: []arbiter.NodeRole{role}, Ed25519Pubkey: pubkey}}})
		mustPropose(t, n, wire.Command{MarkActive: &wire.MarkActive{NodeID: id}})
	}
	register(authoritytest.SNodeNodeID1, arbiter.NodeRoleSNode, nil)
	keys := harnessVerifierKeys()
	for _, id := range slices.Sorted(maps.Keys(keys)) {
		register(id, arbiter.NodeRoleVerifier, keys[id].Public().(ed25519.PublicKey))
	}
	activateMultiSource(t, n, entries)
	return n
}

// publishTableSetTransition carries n's pending table-set change through a
// transition block the way the orchestrator, two fixture verifiers and the
// anchor do (fsm's verifyTransition, anchorFinal and publishManifestInputs,
// through the FSM's exported reads): seal, mark replaying, two matching
// attestations with empty byte-side scans, a final and last-mergeable anchor,
// then the manifest. Its adds become Active and its retires Purging.
func publishTableSetTransition(t *testing.T, n *node) {
	t.Helper()
	res, err := n.propose(wire.Command{SealL3Block: &wire.SealL3Block{}})
	sealed, ok := res.(fsm.SealResult)
	if err != nil || !ok {
		t.Fatalf("seal the transition block = %+v, %v", res, err)
	}
	seq := sealed.BlockSeq
	mustPropose(t, n, wire.Command{MarkReplaying: &wire.MarkReplaying{BlockSeq: seq}})
	header, _, _, err := n.f.L3BlockView(seq)
	if err != nil || header.TableSetTransition == nil {
		t.Fatalf("block %d = %+v, %v; want a transition block", seq, header, err)
	}
	base, ok := n.f.ManifestByID(header.PrevSafeSnapshotID)
	if !ok {
		t.Fatalf("block %d pins an unknown snapshot %s", seq, header.PrevSafeSnapshotID)
	}
	// The replayed root of a transition block: its base with the retires
	// removed and the adds present without data (fsm's expectedTransitionRoot).
	tables := map[string]replay.TableManifest{}
	for _, tm := range base.Tables {
		tables[tm.TableID] = replay.TableManifest{TableID: tm.TableID, SchemaHash: tm.SchemaHash, PartitionRoots: tm.PartitionRoots}
	}
	for _, id := range header.TableSetTransition.Retires {
		delete(tables, id)
	}
	for _, add := range header.TableSetTransition.Adds {
		tables[add.TableID] = replay.TableManifest{TableID: add.TableID, SchemaHash: add.SchemaHash}
	}
	_, root, err := replay.AssembleStateRoot(header.SchemaSnapshotID, header.TableSetTransition.NewSchemaRoot, header.ExecutorProfileID,
		slices.Collect(maps.Values(tables)))
	if err != nil {
		t.Fatal(err)
	}
	receipt := replay.ExecutionReceipt{BlockSeq: seq, PrevSafeSnapshotID: header.PrevSafeSnapshotID, PrevStateRoot: header.PrevStateRoot,
		SchemaSnapshotID: header.SchemaSnapshotID, ExecutorProfileID: header.ExecutorProfileID, ComputedStateRoot: root}
	receiptHash, err := receipt.Hash()
	if err != nil {
		t.Fatal(err)
	}
	info, ok := n.f.BlockDispatchInfo(seq)
	if !ok || len(info.VerifierSet) < 2 {
		t.Fatalf("block %d verifier set = %v", seq, info.VerifierSet)
	}
	keys := harnessVerifierKeys()
	for _, rid := range info.VerifierSet[:2] {
		key := keys[rid]
		mustPropose(t, n, wire.Command{RecordAttestation: &wire.RecordAttestation{Attestation: replay.ReplayAttestation{ReplicaID: rid,
			Receipt: receipt, ReceiptHash: receiptHash, Signature: hex.EncodeToString(ed25519.Sign(key, []byte(receiptHash)))}}})
		scan := arbiter.ByteSideScanMsg{ReplicaID: rid, BlockSeq: seq}
		scanHash, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, scan.Body())
		if err != nil {
			t.Fatal(err)
		}
		scan.ScanHash, scan.Signature = scanHash, hex.EncodeToString(ed25519.Sign(key, []byte(scanHash)))
		mustPropose(t, n, wire.Command{RecordByteSideScan: &wire.RecordByteSideScan{Scan: scan}})
	}
	ws, err := n.f.WorkSet()
	if err != nil {
		t.Fatal(err)
	}
	at := slices.IndexFunc(ws.UnanchoredVerified, func(a fsm.BlockAnchor) bool { return a.BlockSeq == seq })
	if at < 0 || ws.UnanchoredVerified[at].StateRoot != root {
		t.Fatalf("block %d did not reach quorum on root %s: %+v", seq, root, ws.UnanchoredVerified)
	}
	mustPropose(t, n, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: seq,
		Anchor:          arbiter.AnchorRef{L3BlockHash: ws.UnanchoredVerified[at].ChainHash, StateRoot: root},
		FinalityReached: true, LastMergeableReached: true}})
	mi, ok := n.f.ManifestInputs()
	if !ok || mi.SafeBlockSeq != seq {
		t.Fatalf("manifest inputs = %+v, %v; want block %d", mi, ok, seq)
	}
	schemaID, profileID := n.f.GenesisParams()
	manifest, err := (replay.SafeSnapshotManifest{ParentSnapshotID: mi.ParentSnapshotID, SafeBlockSeq: mi.SafeBlockSeq,
		SchemaSnapshotID: schemaID, SchemaRoot: mi.SchemaRoot, ExecutorProfileID: profileID, Tables: mi.Tables}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	mustPropose(t, n, wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: manifest}})
}

// reportPurged records every purge report incarnation seq needs once the
// signed-claims activation is committed: its owner's SNode (always indexer
// 1's here, authoritytest.SNodeNodeID1) and each fixture verifier, which is
// the purge set under both the pre-activation rule (every running SNode and
// verifier) and the owner rule of CONTRACT §3. Each report is signed the way
// CONTRACT §3 "Signed messages in Apply" verifies it, under indexer 1's key or
// the verifier's, so the test keeps passing once Apply checks the signatures.
func reportPurged(t *testing.T, n *node, seq uint64) {
	t.Helper()
	view, err := n.f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	ctx := authority.MessageContext{NetworkID: testNetworkID, GenesisSnapshotID: view.GenesisSnapshotID}
	snode := authoritytest.SNodeNodeID1
	jws, err := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1).SignSNodeMessage(authority.SNodeMessageTablePurged, ctx,
		authority.TablePurgedBody{NodeID: snode, IncarnationSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	mustPropose(t, n, wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: snode, IncarnationSeq: seq, SignerJWS: jws}})
	keys := harnessVerifierKeys()
	for _, id := range slices.Sorted(maps.Keys(keys)) {
		sig, err := authority.SignVerifierMessage(keys[id], authority.VerifierMessageTablePurged, ctx,
			authority.TablePurgedBody{NodeID: id, IncarnationSeq: seq})
		if err != nil {
			t.Fatal(err)
		}
		mustPropose(t, n, wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: id, IncarnationSeq: seq, Ed25519Signature: sig}})
	}
}

// multiChain is an L2 with three indexers' databases: db (indexer 0, the
// genesis table t and the legacy table old), dbb (indexer 1: keep and gone
// before 120, early created at 105, before indexer 1's activation, and gone
// deleted at 110) and other (indexer 2, never enrolled). After 120: a chain
// table of each enrolled owner, a non-enrolled table, the deletion of
// indexer 1's Legacy keep, and a database dbd that indexer 1 creates.
func multiChain() *fakeChain {
	c := newFakeChain()
	c.preexisting["db"] = testFoundingIndexer
	c.preexisting["dbb"] = testSecondIndexer
	c.preexisting["other"] = 2
	c.add(databases.TableCreated, 40, 0, "db", "t")
	c.add(databases.TableCreated, 50, 0, "db", "old")
	c.add(databases.TableCreated, 60, 0, "dbb", "keep")
	c.add(databases.TableCreated, 61, 0, "dbb", "gone")
	c.add(databases.TableCreated, 70, 0, "other", "x")
	c.add(databases.TableCreated, 101, 0, "db", "n")
	c.add(databases.TableSchemaSet, 102, 0, "db", "n")
	c.add(databases.TableCreated, 105, 0, "dbb", "early")
	c.add(databases.TableSchemaSet, 106, 0, "dbb", "early")
	c.add(databases.TableDeleted, 110, 0, "dbb", "gone")
	c.add(databases.TableCreated, 121, 0, "dbb", "n")
	c.add(databases.TableSchemaSet, 122, 0, "dbb", "n")
	c.add(databases.TableCreated, 123, 0, "db", "m")
	c.add(databases.TableSchemaSet, 124, 0, "db", "m")
	c.add(databases.TableCreated, 125, 0, "other", "y")
	c.add(databases.TableSchemaSet, 126, 0, "other", "y")
	c.add(databases.TableDeleted, 130, 0, "dbb", "keep")
	c.dbCreated(131, 0, "dbd", testSecondIndexer)
	c.add(databases.TableCreated, 132, 0, "dbd", "z")
	c.add(databases.TableSchemaSet, 133, 0, "dbd", "z")
	c.setHead(150)
	return c
}

// ownership is one incarnation's owner, origin and status.
type ownership struct {
	owner  uint64
	origin fsm.TableOrigin
	status fsm.TableIncarnationStatus
}

// wantMultiRegistry is the registry multiChain implies once both indexers
// are enrolled (any activation of indexer 1 in (110, 121] gives the same).
func wantMultiRegistry() map[string]ownership {
	return map[string]ownership{
		"db.t":      {testFoundingIndexer, fsm.TableOriginGenesis, fsm.TableStatusActive},
		"db.old":    {testFoundingIndexer, fsm.TableOriginLegacy, fsm.TableStatusLegacy},
		"db.n":      {testFoundingIndexer, fsm.TableOriginChain, fsm.TableStatusPending},
		"dbb.early": {testSecondIndexer, fsm.TableOriginLegacy, fsm.TableStatusLegacy},
		"dbb.keep":  {testSecondIndexer, fsm.TableOriginLegacy, fsm.TableStatusPurged},
		"dbb.n":     {testSecondIndexer, fsm.TableOriginChain, fsm.TableStatusPending},
		"db.m":      {testFoundingIndexer, fsm.TableOriginChain, fsm.TableStatusPending},
		"dbd.z":     {testSecondIndexer, fsm.TableOriginChain, fsm.TableStatusPending},
	}
}

// checkOwnership requires the view to hold exactly want, every incarnation
// owned, and both indexers seeded.
func checkOwnership(t *testing.T, v fsm.TableRegistryView, want map[string]ownership) {
	t.Helper()
	got := map[string]ownership{}
	for _, inc := range v.Incarnations {
		if inc.OwnerIndexerID == nil {
			t.Fatalf("incarnation %d (%s) has no owner", inc.Seq, inc.Key())
		}
		got[inc.Key()] = ownership{*inc.OwnerIndexerID, inc.Origin, inc.Status}
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("registry = %+v\nwant %+v", got, want)
	}
	if !slices.Equal(v.SeededIndexers, []uint64{testFoundingIndexer, testSecondIndexer}) {
		t.Fatalf("seeded indexers = %v", v.SeededIndexers)
	}
}

// commandBlock is the L2 block a registry command consumes.
func commandBlock(c wire.Command) uint64 {
	switch {
	case c.AddTable != nil:
		return c.AddTable.Schema.BlockNumber
	case c.RetireTables != nil:
		return c.RetireTables.Deleted.BlockNumber
	case c.AdvanceL2Cursor != nil:
		return c.AdvanceL2Cursor.To.Number
	case c.SeedLegacyTables != nil:
		return c.SeedLegacyTables.AtBlock.Number
	}
	return 0
}
```

Create `tableregistry/multi_source_test.go` (it reuses `cluster`, `settle`, `registryState` from `end_to_end_test.go`, `newTestWatcher`, `newFakeClock`, `fixedChain` from `chain_harness_test.go`, `haltRecorder`, `seededChain` from `watcher_test.go`, `newCountingMetrics`, `newTestAuditor`, `auditUntilCurrent`, `incarnationsOf` from `auditor_test.go`, `schemaFor`, `mustView`, `mustPropose`):

```go
package tableregistry

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/l2/databases"
)

// CONTRACT §0: before the activation no registry command carries a stage-1
// field, whatever the watcher derives.
func TestWatcherProposesNoStageOneFieldBeforeActivation(t *testing.T) {
	n := newNode(t, true)
	chain := seededChain()
	chain.add(databases.TableCreated, 101, 0, "db", "n")
	chain.add(databases.TableSchemaSet, 102, 0, "db", "n")
	chain.setHead(110)
	clock := newFakeClock()
	settle(t, newTestWatcher(n, chain, clock), clock, &cluster{replicas: []*node{n}, leaderAlive: true})
	for _, c := range n.registryCommands() {
		if (c.AddTable != nil && c.AddTable.OwnerIndexerID != nil) || (c.SeedLegacyTables != nil && c.SeedLegacyTables.IndexerID != nil) {
			t.Fatalf("a pre-activation registry command carries a stage-1 field: %+v", c)
		}
	}
	if v := mustView(t, n); v.Live("db.n") == nil || v.Live("db.n").OwnerIndexerID != nil || v.SeededIndexers != nil {
		t.Fatalf("pre-activation registry = %+v", v)
	}
}

// Spec 2026-10-10 §6.3: indexer 1's seed sits exactly between block 119 and
// block 120, and every command derives its owner.
func TestWatcherSeedsEachIndexerAtItsActivation(t *testing.T) {
	n := newMultiNode(t, multiEntries(t, true))
	chain := multiChain()
	clock := newFakeClock()
	settle(t, newTestWatcher(n, chain, clock), clock, &cluster{replicas: []*node{n}, leaderAlive: true})
	view := mustView(t, n)
	checkOwnership(t, view, wantMultiRegistry())
	if view.Cursor != (fsm.L2Position{BlockNumber: 150, BlockHash: blockHash(150), BlockComplete: true}) {
		t.Fatalf("cursor = %+v", view.Cursor)
	}
	cmds := n.registryCommands()
	at := slices.IndexFunc(cmds, func(c wire.Command) bool { return c.SeedLegacyTables != nil && c.SeedLegacyTables.IndexerID != nil })
	if at < 1 || *cmds[at].SeedLegacyTables.IndexerID != testSecondIndexer || cmds[at].SeedLegacyTables.AtBlock.Number != testSecondActivation-1 {
		t.Fatalf("indexer 1's seed at %d: %+v", at, cmds)
	}
	if prev := cmds[at-1].AdvanceL2Cursor; prev == nil || prev.To.Number != testSecondActivation-1 {
		t.Fatalf("the command before the seed = %+v, want the cursor completing block %d", cmds[at-1], testSecondActivation-1)
	}
	for _, c := range cmds[:at] {
		if b := commandBlock(c); b >= testSecondActivation {
			t.Fatalf("command %+v at block %d precedes indexer 1's seed", c, b)
		}
	}
}

// The two-indexer variant of TestEndToEndLeaderChangeAtEveryProposal: moving
// leadership at every proposal, before it applies or after it applied with
// the outcome lost, still converges to the registry an uninterrupted leader
// commits, the replicas agree, and the new leader's auditor flags nothing.
func TestEndToEndTwoIndexersLeaderChangeAtEveryProposal(t *testing.T) {
	entries := multiEntries(t, true)
	refNode := newMultiNode(t, entries)
	ref := &cluster{replicas: []*node{refNode}, leaderAlive: true}
	refClock := newFakeClock()
	refWatcher := newTestWatcher(refNode, multiChain(), refClock)
	refWatcher.d.Cfg.MaxLogRange = 7
	settle(t, refWatcher, refClock, ref)
	wantCursor, want := registryState(t, refNode)
	checkOwnership(t, mustView(t, refNode), wantMultiRegistry())
	for _, applyLost := range []bool{false, true} {
		for cut := 1; cut <= ref.count; cut++ {
			t.Run(fmt.Sprintf("cut %d apply lost %v", cut, applyLost), func(t *testing.T) {
				chain := multiChain()
				a, b := newMultiNode(t, entries), newMultiNode(t, entries)
				c := &cluster{replicas: []*node{a, b}, loseAt: cut, applyLost: applyLost, leaderAlive: true}
				clock := newFakeClock()
				first := newTestWatcher(a, chain, clock)
				first.d.Cfg.MaxLogRange = 7
				settle(t, first, clock, c)
				c.leaderAlive = true // replica b takes over
				c.replicas = []*node{b, a}
				second := newTestWatcher(b, chain, clock)
				second.d.Cfg.MaxLogRange = 7
				settle(t, second, clock, c)
				gotCursor, got := registryState(t, b)
				if gotCursor != wantCursor || !reflect.DeepEqual(got, want) {
					t.Fatalf("converged to cursor %+v\n%+v\nwant cursor %+v\n%+v", gotCursor, got, wantCursor, want)
				}
				if _, other := registryState(t, a); !reflect.DeepEqual(other, got) {
					t.Fatal("replicas diverged")
				}
				m := newCountingMetrics()
				auditUntilCurrent(t, newTestAuditor(b, chain, m))
				if len(m.mismatches) != 0 {
					t.Fatalf("audit mismatches = %v", m.mismatches)
				}
			})
		}
	}
}

// An enrolment whose activation block lies behind the watcher's scan (but
// after the committed cursor) must make the watcher rebuild under the longer
// list: no proposal may be rejected (settle fails on any), and the result is
// what the chain implies.
func TestWatcherRebuildsWhenAnIndexerEnrolsBehindItsScan(t *testing.T) {
	genesisID := harnessGenesisID(t)
	n := newMultiNode(t, []arbiter.SIIndexerEntry{harnessEntry(t, genesisID, authoritytest.IndexerKeyHex0, testFoundingIndexer, testActivation, authoritytest.SNodeNodeID0)})
	chain := multiChain()
	chain.setHead(110)
	clock := newFakeClock()
	w := newTestWatcher(n, chain, clock)
	c := &cluster{replicas: []*node{n}, leaderAlive: true}
	settle(t, w, clock, c)
	m := newCountingMetrics()
	incremental := newTestAuditor(n, chain, m)
	auditUntilCurrent(t, incremental)
	// The scan reaches 118 while the throttle holds the committed cursor at
	// 110: settle's last advance was one clock minute ago, which no longer
	// throttles, so pin it to now.
	chain.setHead(118)
	w.lastAdvance = clock.Now()
	if _, err := w.pass(t.Context(), c.propose); err != nil {
		t.Fatal(err)
	}
	if cur := mustView(t, n).Cursor; cur.BlockNumber != 110 || w.scanned.BlockNumber != 118 {
		t.Fatalf("cursor %+v scanned %+v; the fixture needs the scan ahead of the cursor", cur, w.scanned)
	}
	// Indexer 1 enrols with activation block 115: after the cursor, behind the scan.
	commitUpdate(t, n, func(u *arbiter.ConsensusParamsUpdate) {
		u.MaxWriters = 2
		u.SIIndexers = append(u.SIIndexers, harnessEntry(t, genesisID, authoritytest.IndexerKeyHex1, testSecondIndexer, 115, authoritytest.SNodeNodeID1))
	})
	chain.setHead(150)
	settle(t, w, clock, c)
	checkOwnership(t, mustView(t, n), wantMultiRegistry())
	auditUntilCurrent(t, incremental)
	fresh := newCountingMetrics()
	auditUntilCurrent(t, newTestAuditor(n, chain, fresh))
	if len(m.mismatches) != 0 || len(fresh.mismatches) != 0 {
		t.Fatalf("audit mismatches: incremental %v, fresh %v", m.mismatches, fresh.mismatches)
	}
}

// Review focus 3 (spec 2026-10-10 §6.3): a database deleted and recreated
// with the same id on another indexer is a new database, not an ownership
// change. dbx is created on indexer 1 and its table t becomes Active
// (incarnation A, owner 1); the database is deleted, A retires with it and is
// purged as usual, and dbx is recreated on indexer 0, whose table t is a new
// incarnation B owned by indexer 0. settle fails on any pass error, an
// owner_changed halt included, and the auditor, incremental and from
// scratch, flags nothing.
func TestRecreatedDatabaseOnAnotherIndexerGetsTheNewOwner(t *testing.T) {
	n := newMultiNodeWithDataPlane(t, multiEntries(t, true))
	chain := multiChain()
	chain.dbCreated(140, 0, "dbx", testSecondIndexer)
	chain.add(databases.TableCreated, 141, 0, "dbx", "t")
	chain.add(databases.TableSchemaSet, 142, 0, "dbx", "t")
	chain.setHead(145)
	clock := newFakeClock()
	w := newTestWatcher(n, chain, clock)
	c := &cluster{replicas: []*node{n}, leaderAlive: true}
	m := newCountingMetrics()
	incremental := newTestAuditor(n, chain, m)
	settle(t, w, clock, c)
	publishTableSetTransition(t, n) // every Pending add of multiChain and dbx.t becomes Active
	first := incarnationsOf(t, n, "dbx", "t")
	if len(first) != 1 || first[0].Status != fsm.TableStatusActive || first[0].OwnerIndexerID == nil || *first[0].OwnerIndexerID != testSecondIndexer {
		t.Fatalf("dbx.t = %+v, want incarnation A Active and owned by indexer %d", first, testSecondIndexer)
	}
	seqA := first[0].Seq
	auditUntilCurrent(t, incremental)

	// dbx is deleted the way the Databases contract cascades it: A retires
	// with the database, the retirement is published, and A's owner SNode
	// and every verifier purge it.
	chain.add(databases.DatabasePendingDelete, 150, 0, "dbx", "")
	chain.add(databases.TableDeleted, 151, 0, "dbx", "t")
	chain.add(databases.DatabaseDeleted, 151, 1, "dbx", "")
	chain.setHead(155)
	settle(t, w, clock, c)
	if got := incarnationsOf(t, n, "dbx", "t"); len(got) != 1 || got[0].Status != fsm.TableStatusRetiring {
		t.Fatalf("dbx.t after the deletion = %+v, want A Retiring", got)
	}
	publishTableSetTransition(t, n)
	reportPurged(t, n, seqA)
	auditUntilCurrent(t, incremental)

	// dbx is recreated on indexer 0 with a table of the same name.
	chain.dbCreated(160, 0, "dbx", testFoundingIndexer)
	chain.add(databases.TableCreated, 161, 0, "dbx", "t")
	chain.add(databases.TableSchemaSet, 162, 0, "dbx", "t")
	chain.setHead(170)
	settle(t, w, clock, c)
	got := incarnationsOf(t, n, "dbx", "t")
	if len(got) != 2 {
		t.Fatalf("dbx.t = %+v, want incarnations A and B", got)
	}
	incA, incB := got[0], got[1]
	if incA.Seq != seqA || incA.Status != fsm.TableStatusPurged || incA.RetireReason != fsm.RetireReasonDatabaseDeleted ||
		incA.OwnerIndexerID == nil || *incA.OwnerIndexerID != testSecondIndexer {
		t.Fatalf("incarnation A = %+v, want it Purged by the database deletion and still owned by indexer %d", incA, testSecondIndexer)
	}
	if incB.Origin != fsm.TableOriginChain || incB.Status != fsm.TableStatusPending || incB.Created == nil || incB.Created.BlockNumber != 161 ||
		incB.OwnerIndexerID == nil || *incB.OwnerIndexerID != testFoundingIndexer {
		t.Fatalf("incarnation B = %+v, want a new Pending incarnation owned by indexer %d", incB, testFoundingIndexer)
	}
	if live := mustView(t, n).Live("dbx.t"); live == nil || live.Seq != incB.Seq {
		t.Fatalf("live dbx.t = %+v, want incarnation B", live)
	}
	auditUntilCurrent(t, incremental)
	fresh := newCountingMetrics()
	auditUntilCurrent(t, newTestAuditor(n, chain, fresh))
	if len(m.mismatches) != 0 || len(fresh.mismatches) != 0 {
		t.Fatalf("audit mismatches: incremental %v, fresh %v", m.mismatches, fresh.mismatches)
	}
}

// Spec 2026-10-10 §6.3: ownership never changes silently.
func TestWatcherHaltsWhenADatabaseChangesOwner(t *testing.T) {
	n := newMultiNode(t, multiEntries(t, true))
	chain := multiChain()
	chain.dbCreated(115, 0, "dbb", 2) // dbb moves to indexer 2 without a DatabaseDeleted
	metrics := &haltRecorder{}
	w := NewWatcher(Deps{Registry: n.f, Chain: fixedChain(chain), Now: newFakeClock().Now, Metrics: metrics,
		Cfg: Config{PollInterval: time.Millisecond, MaxLogRange: 1000}})
	var halt *HaltError
	if err := w.Run(t.Context(), n.propose); !errors.As(err, &halt) || halt.Reason != HaltOwnerChanged || !errors.Is(err, errOwnerChanged) {
		t.Fatalf("Run = %v, want an owner_changed halt", err)
	}
	if strings.Join(metrics.halted, ",") != HaltOwnerChanged {
		t.Fatalf("halt metrics = %v", metrics.halted)
	}
	if c := mustView(t, n).Cursor; c.BlockNumber >= 115 {
		t.Fatalf("cursor %+v passed the ownership change", c)
	}
}

func TestAuditorReportsAnOwnerChange(t *testing.T) {
	n := newMultiNode(t, multiEntries(t, false))
	chain := multiChain()
	chain.dbCreated(115, 0, "dbb", 2)
	w := newTestWatcher(n, chain, newFakeClock())
	var halt *HaltError
	for i := 0; i < 50 && halt == nil; i++ {
		if _, err := w.pass(t.Context(), n.propose); err != nil && !errors.As(err, &halt) {
			t.Fatal(err)
		}
	}
	if halt == nil || halt.Reason != HaltOwnerChanged {
		t.Fatalf("the watcher did not halt: %v", halt)
	}
	// A leader that ignored the halt commits past the change anyway.
	mustPropose(t, n, wire.Command{AdvanceL2Cursor: &wire.AdvanceL2Cursor{To: arbiter.L2BlockRef{Number: 120, Hash: blockHash(120)}}})
	m := newCountingMetrics()
	auditUntilCurrent(t, newTestAuditor(n, chain, m))
	if len(m.mismatches) != 1 || m.mismatches[AuditOwnerChanged] != 1 || m.audited != 120 {
		t.Fatalf("mismatches = %v, audited = %d; want one %s", m.mismatches, m.audited, AuditOwnerChanged)
	}
}

func TestAuditorComparesEachIndexerSeed(t *testing.T) {
	created := func(block uint64) arbiter.L2EventRef {
		return arbiter.L2EventRef{BlockNumber: block, BlockHash: blockHash(block), TxHash: "0xtx"}
	}
	for _, tc := range []struct {
		name   string
		tables []arbiter.LegacyTable
		want   int
	}{
		{"honest", []arbiter.LegacyTable{{DatabaseID: "dbb", TableID: "early", Created: created(105)}, {DatabaseID: "dbb", TableID: "keep", Created: created(60)}}, 0},
		{"a forged table and a missing one", []arbiter.LegacyTable{{DatabaseID: "dbb", TableID: "early", Created: created(105)}, {DatabaseID: "dbb", TableID: "fake", Created: created(70)}}, 2},
		{"other creation evidence", []arbiter.LegacyTable{{DatabaseID: "dbb", TableID: "early", Created: created(104)}, {DatabaseID: "dbb", TableID: "keep", Created: created(60)}}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := newMultiNode(t, multiEntries(t, true))
			chain := multiChain()
			chain.setHead(118)
			clock := newFakeClock()
			settle(t, newTestWatcher(n, chain, clock), clock, &cluster{replicas: []*node{n}, leaderAlive: true})
			mustPropose(t, n, wire.Command{AdvanceL2Cursor: &wire.AdvanceL2Cursor{To: arbiter.L2BlockRef{Number: 119, Hash: blockHash(119)}}})
			id := uint64(testSecondIndexer)
			mustPropose(t, n, wire.Command{SeedLegacyTables: &wire.SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 119, Hash: blockHash(119)}, Tables: tc.tables, IndexerID: &id}})
			chain.setHead(119)
			m := newCountingMetrics()
			auditUntilCurrent(t, newTestAuditor(n, chain, m))
			if m.mismatches[AuditSeed] != tc.want || len(m.mismatches) > min(tc.want, 1) {
				t.Fatalf("mismatches = %v, want %d %s", m.mismatches, tc.want, AuditSeed)
			}
		})
	}
}

func TestAuditorFlagsAnAddTableWithAnotherOwner(t *testing.T) {
	n := newMultiNode(t, multiEntries(t, true))
	chain := multiChain()
	chain.setHead(120)
	clock := newFakeClock()
	settle(t, newTestWatcher(n, chain, clock), clock, &cluster{replicas: []*node{n}, leaderAlive: true})
	hash, js := schemaFor("dbb.n")
	wrong := uint64(testFoundingIndexer) // the chain says indexer 1
	mustPropose(t, n, wire.Command{AddTable: &wire.AddTable{DatabaseID: "dbb", TableID: "n",
		Created:       arbiter.L2EventRef{BlockNumber: 121, BlockHash: blockHash(121), TxHash: "0xtx"},
		Schema:        arbiter.L2EventRef{BlockNumber: 122, BlockHash: blockHash(122), TxHash: "0xtx"},
		SchemaVersion: 1, SchemaHash: hash, SchemaJSON: js, OwnerIndexerID: &wrong}})
	mustPropose(t, n, wire.Command{AdvanceL2Cursor: &wire.AdvanceL2Cursor{To: arbiter.L2BlockRef{Number: 122, Hash: blockHash(122)}}})
	chain.setHead(125)
	m := newCountingMetrics()
	auditUntilCurrent(t, newTestAuditor(n, chain, m))
	if len(m.mismatches) != 1 || m.mismatches[AuditAddMismatch] != 1 {
		t.Fatalf("mismatches = %v, want one %s", m.mismatches, AuditAddMismatch)
	}
}
```

In `metrics/table_registry_test.go` `TestTableRegistryMetrics`, add `m.WatcherHalted("owner_changed")` after `m.WatcherHalted("reorg")` and `` `arbiter_table_registry_watcher_halts_total{reason="owner_changed"} 1`, `` to the expected lines.

Run: `cd "$W" && bazel run //:gazelle && go test ./tableregistry/ ./metrics/ -v`
Expected: build failure (`undefined: membership`, `undefined: siCreate`, `undefined: HaltOwnerChanged`, `undefined: AuditOwnerChanged`).

- [ ] **Step 6: Implement per-indexer membership in the watcher and the auditor**

Create `tableregistry/membership.go`:

```go
package tableregistry

import (
	"math/big"
	"slices"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter/fsm"
)

// membership is who opens SI incarnations (spec 2026-10-10 §6.3): a
// TableCreated is SI iff the indexer hosting its database at the event block
// has an si_indexers entry and the block is at or after that entry's
// activation block. Before the signed-claims activation the founding indexer
// of TableRegistryParams is the only entry, today's predicate. Entries are
// append-only and each new activation block lies beyond the cursor committed
// when it was enrolled, so judging an already consumed event with a longer
// list gives the answer the shorter one gave.
type membership struct {
	params  arbiter.TableRegistryParams
	entries []arbiter.SIIndexerEntry // the committed si_indexers; nil before the activation
}

// membershipOf is the membership a committed registry view implies.
func membershipOf(v fsm.TableRegistryView) membership {
	return membership{params: v.Params, entries: v.SIIndexers}
}

// signedClaims reports whether si_indexers is committed.
func (m membership) signedClaims() bool { return len(m.entries) > 0 }

// activation returns indexer id's activation block; ok is false when it has
// no entry.
func (m membership) activation(id uint64) (uint64, bool) {
	if !m.signedClaims() {
		return m.params.ActivationBlock, id == m.params.SIIndexerID
	}
	e, ok := findEntry(m.entries, id)
	return e.ActivationBlock, ok
}

// owner judges a TableCreated at block in a database the chain says indexer
// hosts: the owner is that indexer (spec D4), and the event opens an SI
// incarnation iff the indexer has an entry whose activation block it reached.
// A nil indexer (the database was not active) or one wider than 64 bits never
// does.
func (m membership) owner(indexer *big.Int, block uint64) (uint64, bool) {
	if indexer == nil || !indexer.IsUint64() {
		return 0, false
	}
	id := indexer.Uint64()
	activation, ok := m.activation(id)
	return id, ok && block >= activation
}

// ownerField is AddTable.OwnerIndexerID for owner: set once signed claims are
// active, nil before, so no pre-activation command carries a field a strict
// decoder refuses (CONTRACT §0).
func (m membership) ownerField(owner uint64) *uint64 {
	if !m.signedClaims() {
		return nil
	}
	return &owner
}

func findEntry(entries []arbiter.SIIndexerEntry, id uint64) (arbiter.SIIndexerEntry, bool) {
	for _, e := range entries {
		if e.IndexerID == id {
			return e, true
		}
	}
	return arbiter.SIIndexerEntry{}, false
}

// nextUnseeded returns the enrolled indexer, other than the founding one (the
// seed without indexer_id covers it), whose Legacy seed the registry needs
// next: the unseeded entry with the smallest activation block, the lowest id
// on a tie (entries are sorted by id).
func nextUnseeded(v fsm.TableRegistryView) (arbiter.SIIndexerEntry, bool) {
	var next arbiter.SIIndexerEntry
	found := false
	for _, e := range v.SIIndexers {
		if e.IndexerID == v.Params.SIIndexerID || slices.Contains(v.SeededIndexers, e.IndexerID) {
			continue
		}
		if !found || e.ActivationBlock < next.ActivationBlock {
			next, found = e, true
		}
	}
	return next, found
}
```

`tableregistry/derive.go`: replace the `pending` and `siCreates` declarations (lines 21-32) with

```go
// pendingCreate is one open SI create: its TableCreated and the owner the
// resolver recorded for it (spec 2026-10-10 D4: the database's indexer at the
// event block).
type pendingCreate struct {
	Created databases.Event
	Owner   uint64
}

// pending is the derivation state carried from one event to the next: for
// each (database, table) pair, the TableCreated event of an SI incarnation
// whose first schema declaration has not been seen yet (spec D3). It is keyed
// by the exact pair, not the joined TableKey: with dotted ids (a, b.c) and
// (a.b, c) share a key but are different tables on chain.
type pending map[tableRef]pendingCreate

// siCreate is one TableCreated's membership: whether it opens an SI
// incarnation and, if it does, its owner.
type siCreate struct {
	SI    bool
	Owner uint64
}

// siCreates answers, for a TableCreated event (by its ref), its membership
// under the committed si_indexers. The caller resolves it before derive runs,
// so derive itself does no I/O.
type siCreates map[arbiter.L2EventRef]siCreate
```

change `derive`'s signature to `func derive(p pending, events []databases.Event, m membership, view *fsm.TableRegistryView, si siCreates) ([]derived, error)`, its doc's first bullet to "TableCreated at or after its owner's activation_block, in a database of an SI indexer, opens a pending create for its pair (replacing any earlier one); any other TableCreated closes the pair's pending create.", and its two first cases to

```go
		case databases.TableCreated:
			var c siCreate
			// No entry activates before the founding one (every later entry
			// activates after the cursor the founding seed committed), so an
			// earlier create needs no resolution.
			if ev.Ref.BlockNumber >= m.params.ActivationBlock {
				var ok bool
				if c, ok = si[ev.Ref]; !ok {
					return nil, fmt.Errorf("%w: %s at block %d log %d", errUnresolvedIndexer, key, ev.Ref.BlockNumber, ev.Ref.LogIndex)
				}
			}
			if c.SI {
				p[pair] = pendingCreate{Created: ev, Owner: c.Owner}
			} else {
				delete(p, pair)
			}
		case databases.TableSchemaSet:
			created, ok := p[pair]
			if !ok {
				continue
			}
			delete(p, pair)
			if emit {
				out = append(out, derived{Event: ev, Command: wire.Command{AddTable: &wire.AddTable{
					DatabaseID: ev.DatabaseID, TableID: ev.TableID, Created: created.Created.Ref, Schema: ev.Ref,
					SchemaVersion: ev.SchemaVersion, SchemaHash: ev.SchemaHash, SchemaJSON: ev.SchemaJSON,
					OwnerIndexerID: m.ownerField(created.Owner),
				}}})
			}
```

replace `isSIIndexer` (lines 189-192) with

```go
// isIndexer reports whether a uint256 indexer id is want.
func isIndexer(id *big.Int, want uint64) bool {
	return id != nil && id.IsUint64() && id.Uint64() == want
}
```

and split `deriveSeed` (lines 194-233): keep its doc and make it

```go
func deriveSeed(params arbiter.TableRegistryParams, registered func(key string) bool, in seedInput) (wire.SeedLegacyTables, error) {
	return deriveIndexerSeed(params.SIIndexerID, registered, in)
}

// deriveIndexerSeed is deriveSeed for the databases of one indexer (spec
// 2026-10-10 §6.3). It leaves IndexerID unset: the founding seed never
// carries it, and the watcher sets it for every other indexer.
func deriveIndexerSeed(indexer uint64, registered func(key string) bool, in seedInput) (wire.SeedLegacyTables, error) {
	// the former deriveSeed body, with the database filter
	// !db.Active || db.PendingDelete || !isIndexer(db.IndexerID, indexer)
}
```

`tableregistry/chain.go`: `indexerResolver` keeps a `members membership` instead of `params` (`newIndexerResolver(chain Chain, members membership)`), and `observe`, `readSeedInput` become:

```go
// errOwnerChanged is a database whose indexer changed without a
// DatabaseDeleted in between (spec 2026-10-10 §6.3). Owners never change, so
// only an operator can decide what such a chain means.
var errOwnerChanged = errors.New("table registry: a database's indexer changed without a DatabaseDeleted")

// observe folds one event into the cache and, for a TableCreated at or after
// the founding activation, returns its membership for derive. A
// DatabaseCreated naming another indexer than the cached one returns
// errOwnerChanged after recording the new indexer, so an auditor can report it
// and fold on.
func (r *indexerResolver) observe(ctx context.Context, ev databases.Event) (siCreates, error) {
	switch ev.Kind {
	case databases.DatabaseCreated:
		cached, ok := r.indexers[ev.DatabaseID]
		r.indexers[ev.DatabaseID] = ev.IndexerID
		if ok && !sameIndexer(cached, ev.IndexerID) {
			return nil, fmt.Errorf("%w: database %s was indexer %v, DatabaseCreated at block %d log %d names indexer %v",
				errOwnerChanged, ev.DatabaseID, cached, ev.Ref.BlockNumber, ev.Ref.LogIndex, ev.IndexerID)
		}
	case databases.DatabaseDeleted:
		delete(r.indexers, ev.DatabaseID)
	case databases.TableCreated:
		if ev.Ref.BlockNumber < r.members.params.ActivationBlock {
			return nil, nil
		}
		id, ok := r.indexers[ev.DatabaseID]
		if !ok {
			db, err := r.chain.DatabaseAt(ctx, ev.DatabaseID, ev.Ref.BlockNumber)
			if err != nil {
				return nil, err
			}
			if db.Active {
				id = db.IndexerID
				r.indexers[ev.DatabaseID] = id
			}
		}
		owner, si := r.members.owner(id, ev.Ref.BlockNumber)
		return siCreates{ev.Ref: {SI: si, Owner: owner}}, nil
	}
	return nil, nil
}

// checkSeedOwners compares a seed's database list with every database the
// fold state resolved: both describe the chain at the seed block, so another
// indexer is an ownership change.
func (r *indexerResolver) checkSeedOwners(in seedInput) error {
	for _, db := range in.Databases {
		if cached, ok := r.indexers[db.ID]; ok && db.Active && !sameIndexer(cached, db.IndexerID) {
			return fmt.Errorf("%w: database %s was indexer %v, the seed at block %d reads indexer %v", errOwnerChanged, db.ID, cached, in.At.Number, db.IndexerID)
		}
	}
	return nil
}

// sameIndexer compares two uint256 indexer ids; nil equals only nil.
func sameIndexer(a, b *big.Int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Cmp(b) == 0
}

// readSeedInput is the founding indexer's seed input at activation_block - 1
// (ruling R8).
func readSeedInput(ctx context.Context, chain Chain, params arbiter.TableRegistryParams, deployBlock uint64) (seedInput, error) {
	return readIndexerSeedInput(ctx, chain, params.SIIndexerID, params.ActivationBlock-1, deployBlock)
}

// readIndexerSeedInput reads one indexer's seed input at block at: the
// header, the database list, the tables of each active database the indexer
// hosts, and the latest TableCreated per table from a forward scan of
// [deployBlock, at].
func readIndexerSeedInput(ctx context.Context, chain Chain, indexer, at, deployBlock uint64) (seedInput, error) {
	if deployBlock > at {
		return seedInput{}, fmt.Errorf("%w: deploy_block %d is after activation_block - 1 (%d)", errSeedDeployBlock, deployBlock, at)
	}
	// the former readSeedInput body from HeaderAt on, with the database
	// filter !db.Active || db.PendingDelete || !isIndexer(db.IndexerID, indexer)
}
```

`tableregistry/watcher.go`:
- add the halt reason (and extend the `HaltError` doc with ", or a database's indexer changed without a DatabaseDeleted (ownership never changes silently, spec 2026-10-10 §6.3)"):

```go
	// HaltOwnerChanged: a database's indexer changed without a
	// DatabaseDeleted (spec 2026-10-10 §6.3).
	HaltOwnerChanged = "owner_changed"
```

- `Watcher` gains `members []arbiter.SIIndexerEntry // the si_indexers the fold state was built with`; `Run` resets it with the other per-term fields (`w.members = nil`).
- in `pass`, replace the rebuild condition, the `scan` call and its error check (lines 216-224) with:

```go
	// An enrolment changes what an event means from its activation block on,
	// so the fold state is rebuilt under the new list from the committed
	// cursor (which every new activation block lies beyond).
	if w.pending == nil || positionAfter(cursor, w.scanned) || !slices.Equal(w.members, view.SIIndexers) {
		if err := w.rebuild(ctx, view); err != nil {
			return false, err
		}
	}
	// An enrolled indexer is seeded exactly when the committed cursor
	// completes the block before its activation (spec 2026-10-10 §6.3); until
	// then the scan stops at that block, because Apply refuses every registry
	// command at or beyond it.
	limit := head.Number
	if entry, ok := nextUnseeded(view); ok {
		at := entry.ActivationBlock - 1
		if positionAfter(cursor, fsm.L2Position{BlockNumber: at, BlockComplete: true}) {
			return false, &HaltError{Reason: HaltSeed, Err: fmt.Errorf("the registry cursor %+v passed block %d before indexer %d was seeded", cursor, at, entry.IndexerID)}
		}
		if cursor.BlockNumber == at && cursor.BlockComplete {
			return true, w.seedIndexer(ctx, propose, view, entry)
		}
		limit = min(limit, at)
	}
	proposed, err := w.scan(ctx, propose, &view, limit)
	if err != nil {
		return false, err
	}
```

- `scan(ctx context.Context, propose Proposer, view *fsm.TableRegistryView, limit uint64)` bounds the range by `limit` instead of `head` (`if from > limit { return false, nil }; to := limit`); its observe error becomes `return false, ownerChangedHalt(err)` (after `w.pending = nil`), its derive call `derive(w.pending, []databases.Event{ev}, membershipOf(*view), view, si)`, and after `*view = next`:

```go
			// An enrolment committed mid-scan: the rest of the range was folded
			// with the shorter list, so rebuild before going on.
			if !slices.Equal(view.SIIndexers, w.members) {
				w.pending = nil
				return true, nil
			}
```

- `rebuild` builds `members := membershipOf(view)`, uses `newIndexerResolver(w.chain, members)` and `derive(p, []databases.Event{ev}, members, nil, si)`, returns `ownerChangedHalt(err)` for an observe error, and ends with `w.pending, w.resolver, w.scanned, w.members = p, resolver, cursor, slices.Clone(view.SIIndexers)`.
- `seed` (founding) derives with `deriveSeed(params, registeredIn(view), in)`; add:

```go
// registeredIn reports, for a seed, whether the view's registry holds key: a
// live incarnation that is not Purged (one registered key rejects a whole
// seed in Apply).
func registeredIn(view fsm.TableRegistryView) func(string) bool {
	return func(key string) bool {
		live := view.Live(key)
		return live != nil && live.Status != fsm.TableStatusPurged
	}
}

// seedIndexer proposes the Legacy seed of enrolled indexer entry (spec
// 2026-10-10 §6.3) once the committed cursor has completed the block before
// its activation; pass guarantees the fold state sits at that block, so every
// database it resolved must still have the indexer the seed reads.
func (w *Watcher) seedIndexer(ctx context.Context, propose Proposer, view fsm.TableRegistryView, entry arbiter.SIIndexerEntry) error {
	in, err := readIndexerSeedInput(ctx, w.chain, entry.IndexerID, entry.ActivationBlock-1, w.d.Cfg.DeployBlock)
	if err != nil {
		if errors.Is(err, errSeedDeployBlock) {
			return &HaltError{Reason: HaltSeed, Err: err}
		}
		return w.nonCanonical(err)
	}
	if err := w.resolver.checkSeedOwners(in); err != nil {
		w.pending = nil
		return &HaltError{Reason: HaltOwnerChanged, Err: err}
	}
	cmd, err := deriveIndexerSeed(entry.IndexerID, registeredIn(view), in)
	if err != nil {
		return &HaltError{Reason: HaltSeed, Err: err}
	}
	id := entry.IndexerID
	cmd.IndexerID = &id
	return w.propose(propose, wire.Command{SeedLegacyTables: &cmd})
}

// ownerChangedHalt turns an ownership change into a halt; other errors pass.
func ownerChangedHalt(err error) error {
	if errors.Is(err, errOwnerChanged) {
		return &HaltError{Reason: HaltOwnerChanged, Err: err}
	}
	return err
}
```

(add `"slices"` to the imports.)

`tableregistry/auditor.go`:
- add `AuditOwnerChanged = "owner_changed"` to the audit checks and `seedsAudited map[uint64]bool // per-indexer seeds already compared` to `Auditor`;
- in `step`, compute `members := membershipOf(view)` after reading the view; right after the founding-seed block call `if err := a.auditIndexerSeeds(ctx, view, head.Number); err != nil { return err }`; fold with the committed list and report ownership changes once the fold succeeded:

```go
	// Fold into copies: a failure part-way must leave the fold state at the
	// audited position so the next step retries the same range. The resolver
	// judges the range with the committed list: an indexer enrolled since the
	// last step activates after the cursor committed with its enrolment, which
	// is at or after the audited position.
	p, resolver := clonePending(a.pending), a.resolver.clone()
	resolver.members = members
	var adds []*wire.AddTable
	var moved []databases.Event // DatabaseCreated logs that changed a database's indexer
	for _, ev := range events {
		si, err := resolver.observe(ctx, ev)
		if errors.Is(err, errOwnerChanged) {
			// observe recorded the new indexer: fold on, report it below.
			moved, err = append(moved, ev), nil
		}
		if err != nil {
			return err
		}
		out, err := derive(p, []databases.Event{ev}, members, nil, si)
		if err != nil {
			return err
		}
		for _, d := range out {
			adds = append(adds, d.Command.AddTable)
		}
	}
	if canonical.Hash != target.BlockHash {
		a.mismatch(AuditCursorHash, "committed cursor block is not canonical", "block", target.BlockNumber, "committed", target.BlockHash, "canonical", canonical.Hash)
	}
	for _, ev := range moved {
		a.mismatch(AuditOwnerChanged, "a database's indexer changed without a DatabaseDeleted", "database", ev.DatabaseID, "block", ev.Ref.BlockNumber, "log", ev.Ref.LogIndex)
	}
	a.compareAdds(view, adds, *a.audited, target)
	a.checkRetires(view, members, events, *a.audited, target)
```

- `clone` uses `newIndexerResolver(r.chain, r.members)`; `auditSeed` resets the resolver with `newIndexerResolver(a.chain, membershipOf(view))` and counts only the founding indexer's Legacy incarnations as its seed (`inc.Origin == fsm.TableOriginLegacy && (inc.OwnerIndexerID == nil || *inc.OwnerIndexerID == params.SIIndexerID)`);
- `compareAdds` adds `|| !sameOwner(inc.OwnerIndexerID, add.OwnerIndexerID)` to its mismatch condition; `checkRetires(view fsm.TableRegistryView, m membership, events …)` computes `registered := registeredBefore(inc, ev.Ref, m)`; add:

```go
// auditIndexerSeeds compares every per-indexer Legacy seed this auditor has
// not compared yet (spec 2026-10-10 §6.3), once its own RPC confirms the seed
// block. A seed needs no fold state: it is the chain at one block.
func (a *Auditor) auditIndexerSeeds(ctx context.Context, view fsm.TableRegistryView, head uint64) error {
	if a.seedsAudited == nil {
		a.seedsAudited = map[uint64]bool{}
	}
	for _, id := range view.SeededIndexers {
		if id == view.Params.SIIndexerID || a.seedsAudited[id] {
			continue
		}
		entry, ok := findEntry(view.SIIndexers, id)
		if !ok {
			a.mismatch(AuditSeed, "a seeded indexer has no si_indexers entry", "indexer", id)
			a.seedsAudited[id] = true
			continue
		}
		if head < entry.ActivationBlock-1 {
			continue // this RPC has not confirmed the seed block yet
		}
		if err := a.auditIndexerSeed(ctx, view, entry); err != nil {
			return err
		}
		a.seedsAudited[id] = true
	}
	return nil
}

// auditIndexerSeed compares indexer entry's committed Legacy incarnations with
// the chain at its activation_block - 1, pair by pair. Each one must be an
// active table of an active database the indexer hosted then, with its latest
// TableCreated; each such table must be recorded unless its key was already
// held at the seed (the watcher skips registered keys). Whether that holder
// was still unpurged then is not in the view, so the auditor accepts the skip
// rather than report a leader that followed the rule.
func (a *Auditor) auditIndexerSeed(ctx context.Context, view fsm.TableRegistryView, entry arbiter.SIIndexerEntry) error {
	at := entry.ActivationBlock - 1
	in, err := readIndexerSeedInput(ctx, a.chain, entry.IndexerID, at, a.d.Cfg.DeployBlock)
	if err != nil {
		return err
	}
	committed := map[tableRef]arbiter.L2EventRef{}
	held := map[string]bool{}
	for _, inc := range view.Incarnations {
		if inc.Origin == fsm.TableOriginLegacy && inc.OwnerIndexerID != nil && *inc.OwnerIndexerID == entry.IndexerID && inc.Created != nil {
			committed[tableRef{inc.DatabaseID, inc.TableID}] = *inc.Created
		}
		if heldAtSeed(inc, view, at) {
			held[inc.Key()] = true
		}
	}
	for _, db := range in.Databases {
		if !db.Active || db.PendingDelete || !isIndexer(db.IndexerID, entry.IndexerID) {
			continue
		}
		for _, tb := range in.Tables[db.ID] {
			if !tb.Active {
				continue
			}
			pair := tableRef{db.ID, tb.ID}
			created, onChain := in.Created[pair]
			got, recorded := committed[pair]
			delete(committed, pair)
			switch {
			case !onChain:
				a.mismatch(AuditSeed, "an active table has no TableCreated log", "indexer", entry.IndexerID, "database", db.ID, "table", tb.ID)
			case recorded && got != created, !recorded && !held[arbiter.TableKey(db.ID, tb.ID)]:
				a.mismatch(AuditSeed, "legacy table missing or with other evidence", "indexer", entry.IndexerID, "database", db.ID, "table", tb.ID)
			}
		}
	}
	for pair := range committed {
		a.mismatch(AuditSeed, "legacy table the chain does not imply", "indexer", entry.IndexerID, "database", pair.Database, "table", pair.Table)
	}
	return nil
}

// heldAtSeed reports whether inc held its key when the seed at block at
// applied: a genesis incarnation; a Legacy one seeded no later (its owner
// activated by at + 1); a chain one whose schema event is at or before at.
func heldAtSeed(inc fsm.TableIncarnation, view fsm.TableRegistryView, at uint64) bool {
	switch inc.Origin {
	case fsm.TableOriginGenesis:
		return true
	case fsm.TableOriginLegacy:
		owner := view.Params.SIIndexerID
		if inc.OwnerIndexerID != nil {
			owner = *inc.OwnerIndexerID
		}
		e, ok := findEntry(view.SIIndexers, owner)
		return !ok || e.ActivationBlock-1 <= at
	default:
		return inc.SchemaRef != nil && inc.SchemaRef.BlockNumber <= at
	}
}

// registeredBefore reports whether inc was registered before the deletion log
// at ref: a genesis incarnation always, a Legacy one from its owner's
// activation block (its seed sits right before it), a chain one once its
// schema event preceded ref.
func registeredBefore(inc fsm.TableIncarnation, ref arbiter.L2EventRef, m membership) bool {
	switch inc.Origin {
	case fsm.TableOriginChain:
		return inc.SchemaRef != nil && eventBefore(*inc.SchemaRef, ref)
	case fsm.TableOriginLegacy:
		owner := m.params.SIIndexerID
		if inc.OwnerIndexerID != nil {
			owner = *inc.OwnerIndexerID
		}
		activation, ok := m.activation(owner)
		return !ok || ref.BlockNumber >= activation
	default:
		return true
	}
}

// sameOwner compares two optional owners.
func sameOwner(a, b *uint64) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
```

(add `"errors"` to the imports.)

`metrics/metrics.go:57`: the halt help text becomes `"Table registry watcher halts on this node by reason (chain_id, reorg, repeated_rejection, seed, encode, panic, owner_changed); each needs an operator."`.

- [ ] **Step 7: Run the tests**

Run: `cd "$W" && bazel run //:gazelle && bazel test //tableregistry:all //metrics:all //fsm:all //server:all //cmd/...`
Expected: all PASS, including the untouched `TestEndToEndLeaderChangeAtEveryProposal`, `TestDeriveCommandsAreAlwaysAccepted`, every `TestAuditor*` and `TestWatcher*`, and the new two-indexer tests, `TestRecreatedDatabaseOnAnotherIndexerGetsTheNewOwner` among them (review focus 3: no halt, A Purged under owner 1, B Pending under owner 0, no audit mismatch).

- [ ] **Step 8: Commit**

```bash
cd "$W" && git add tableregistry metrics && git commit -m "feat(tableregistry): per-indexer membership, owners, seeds and HaltOwnerChanged

A TableCreated is SI iff its database's indexer has an si_indexers entry
whose activation block it reached; AddTable carries the owner once
signed claims are active. The watcher stops at the block before an
unseeded indexer's activation, seeds that indexer exactly there, rebuilds
when the list grows and halts with owner_changed when a database changes
indexer; the auditor re-derives owners and every indexer's seed (housegate
spec 2026-10-10 D4, §6.3).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 7: arbiter — owner-bound source selection and SOURCE_UNAVAILABLE (S1)

**Files:**
- Modify: `fsm/admission.go:78-92` (step 4 keeps the live incarnation), `:110-113` (owner-bound source before the lane budget), `:139` (step 6 binds it)
- Modify: `fsm/select.go:41-50` (doc of `selectSource`; new `ownerSourceLocked`)
- Create: `fsm/admission_source_test.go`, `fsm/determinism_source_test.go`
- Modify: `server/ingress.go:109-112` (operator log)
- Create: `server/source_unavailable_test.go`

**Interfaces:**
- Consumes: Task 5 `signedClaimsActiveLocked`, `siIndexerLocked`, `claimsFSM`, `activateClaims`, `claimsUpdate`, `foundingEntry`, `siEntry`, `testVerifiers`, `indexerSigner2(t)`, and the golden `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`; Task 6 `advanceTo`, `seedIndexerCmd`, `ownedAddCmd`; existing `mustSeal`, `verifyTransition`, `anchorFinal`, `publishManifestInputs`, `registryEnvelope`, `genesisSchemaHash`, `submit`, `registerActive`, `snapshotBytes`, `legacyGoldenUserKeyHex`, `legacyGoldenAuthorityHex`; `CORE_TAG` `arbiter.AdmissionCodeSourceUnavailable`; `PROTO_TAG` `pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE`.
- Produces: `(f *FSM) ownerSourceLocked(inc *TableIncarnation) (source, unavailable string)`; the admission answer `SubmitResult{Code: arbiter.AdmissionCodeSourceUnavailable, Message: "storage-integrity source <node> of indexer <id> is not active"}` with state unchanged; gateway label `ADMISSION_CODE_SOURCE_UNAVAILABLE` and a WARN log; test helpers `fixedClient`, `ownerFSM`, `sourceBindingScript`.

Order of the answers after this task: shape, kind, signature, schema allowlist, duplicate, snapshot-query fence, table-set fence, then SOURCE_UNAVAILABLE, then the lane budget, then the accumulator insert. A spent client_seq is therefore still DUPLICATE (not retryable) and a transient fence still `Unavailable`; SOURCE_UNAVAILABLE outranks LANE_BUDGET_EXCEEDED so a client does not rotate lanes for a statement no source can take. The gateway maps codes numerically (`server/ingress.go:168-174` `submitResultToPB`), so code 10 reaches `SequencedAck` without a mapping change.

- [ ] **Step 1: Write the failing tests**

Create `fsm/admission_source_test.go`:

```go
package fsm

import (
	"bytes"
	"crypto/ecdsa"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter/accumulator"
)

// fixedClient is the lane golden's fixed client key, so the hash selection
// these tests compare with is the same on every run.
func fixedClient(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	key, err := crypto.HexToECDSA(legacyGoldenUserKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	return key, strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
}

// ownerFSM has two owners: claimsFSM with a second SNode s2 registered and
// Active before the signed-claims activation, which enrols indexer 2
// (activation block 150, SNode s2). Indexer 2 is seeded at block 149 and owns
// db2.x, added at block 150 and made Active through its transition block.
func ownerFSM(t *testing.T) *FSM {
	t.Helper()
	f, a := claimsFSM(t)
	registerActive(t, f, "s2", arbiter.NodeRoleSNode)
	activateClaims(t, f, a, siEntry(t, f, indexerSigner2(t), 2, 150, "s2"))
	mustApply(t, f, advanceTo(149))
	mustApply(t, f, seedIndexerCmd(2, 149))
	mustApply(t, f, ownedAddCmd(t, f, "db2", "x", ev(150, "0x150", 0), ev(151, "0x151", 0), 2))
	seq := mustSeal(t, f).BlockSeq
	verifyTransition(t, f, seq)
	anchorFinal(t, f, seq)
	publishManifestInputs(t, f)
	if inc := f.st.TableRegistry.Live("db2.x"); inc == nil || inc.Status != TableStatusActive {
		t.Fatalf("db2.x = %+v, want Active", inc)
	}
	return f
}

// Spec 2026-10-10 D5, §6.4: after the activation a statement's source is its
// target's owner's SNode, whatever the hash selection says.
func TestAdmissionBindsTheOwnersSNode(t *testing.T) {
	f := ownerFSM(t)
	key, account := fixedClient(t)
	dbt, db2x := genesisSchemaHash(t, f), f.st.TableRegistry.Live("db2.x").SchemaHash
	hashDisagrees := 0
	for seq := uint64(1); seq <= 16; seq++ {
		target, hash, want := "db.t", dbt, "s1"
		if seq%2 == 0 {
			target, hash, want = "db2.x", db2x, "s2"
		}
		r := submit(t, f, registryEnvelope(t, key, account, seq, target, hash))
		if r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("statement %d (%s) = %+v", seq, target, r)
		}
		ss := f.st.Statements[r.StatementSeq]
		if ss.SourceNode != want {
			t.Fatalf("statement %d (%s) bound %q, want the owner's SNode %q", seq, target, ss.SourceNode, want)
		}
		if f.selectSource(ss.Env.StatementID.Flat()) != want {
			hashDisagrees++
		}
	}
	if hashDisagrees == 0 {
		t.Fatal("hash selection agreed with the owner for every statement; the fixture cannot tell the two rules apart")
	}
}

// Spec 2026-10-10 §6.4: an owner SNode that is not registered and Active
// answers SOURCE_UNAVAILABLE before the only state mutation, so the
// client_seq stays unspent and the same statement is admitted once the
// source is back.
func TestAdmissionRefusesAnUnavailableSourceWithStateUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name     string
		down, up func(f *FSM)
	}{
		{"syncing", func(f *FSM) { f.st.Nodes["s2"].Status = NodeSyncing }, func(f *FSM) { f.st.Nodes["s2"].Status = NodeActive }},
		{"evicted", func(f *FSM) { f.st.Nodes["s2"].Status = NodeEvicted }, func(f *FSM) { f.st.Nodes["s2"].Status = NodeActive }},
		{"unregistered", func(f *FSM) { delete(f.st.Nodes, "s2") }, func(f *FSM) {
			f.st.Nodes["s2"] = &NodeInfo{Registration: arbiter.NodeRegistration{NodeID: "s2", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}, Status: NodeActive}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := ownerFSM(t)
			key, account := fixedClient(t)
			env := registryEnvelope(t, key, account, 1, "db2.x", f.st.TableRegistry.Live("db2.x").SchemaHash)
			tc.down(f)
			coord := env.StatementID.Coord()
			root, next := f.st.SpentIDs.Root(), f.st.NextStatementSeq
			before := snapshotBytes(t, f)
			r := submit(t, f, env)
			if r.Code != arbiter.AdmissionCodeSourceUnavailable || r.Message != "storage-integrity source s2 of indexer 2 is not active" || r.StatementSeq != 0 {
				t.Fatalf("submit = %+v, want SOURCE_UNAVAILABLE", r)
			}
			if !bytes.Equal(before, snapshotBytes(t, f)) {
				t.Fatal("SOURCE_UNAVAILABLE changed replicated state")
			}
			if !bytes.Equal(root, f.st.SpentIDs.Root()) || f.st.SpentIDs.Status(coord) != accumulator.StatusFresh || f.st.NextStatementSeq != next {
				t.Fatal("SOURCE_UNAVAILABLE spent the client_seq")
			}
			if _, indexed := f.st.ByStatementID[env.StatementID.Flat()]; indexed {
				t.Fatal("a refused statement was indexed")
			}
			// The other owner's source is unaffected.
			if r := submit(t, f, registryEnvelope(t, key, account, 2, "db.t", genesisSchemaHash(t, f))); r.Code != arbiter.AdmissionCodeAccepted {
				t.Fatalf("db.t while s2 is down = %+v", r)
			}
			tc.up(f)
			if r := submit(t, f, env); r.Code != arbiter.AdmissionCodeAccepted || f.st.Statements[r.StatementSeq].SourceNode != "s2" {
				t.Fatalf("the same statement once s2 is Active = %+v", r)
			}
		})
	}
}

func TestSourceUnavailableRanksAfterEarlierRefusals(t *testing.T) {
	f := ownerFSM(t)
	key, account := fixedClient(t)
	hash := f.st.TableRegistry.Live("db2.x").SchemaHash
	if r := submit(t, f, registryEnvelope(t, key, account, 1, "db2.x", hash)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("first statement = %+v", r)
	}
	f.st.Nodes["s2"].Status = NodeSyncing
	for _, tc := range []struct {
		name string
		env  arbiter.StatementEnvelope
		want arbiter.AdmissionCode
	}{
		{"a spent client_seq is a duplicate", registryEnvelope(t, key, account, 1, "db2.x", hash), arbiter.AdmissionCodeDuplicateClientSeq},
		{"a schema mismatch", registryEnvelope(t, key, account, 2, "db2.x", "0xother"), arbiter.AdmissionCodeSchemaNotAllowed},
		{"an unregistered target", registryEnvelope(t, key, account, 2, "db2.unknown", hash), arbiter.AdmissionCodeSchemaNotAllowed},
		{"the owner's source is down", registryEnvelope(t, key, account, 2, "db2.x", hash), arbiter.AdmissionCodeSourceUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if r := submit(t, f, tc.env); r.Code != tc.want {
				t.Fatalf("submit = %+v, want code %d", r, tc.want)
			}
		})
	}
}

// Design §5.4 stays in force until the activation, over the whole writer pool.
func TestPreActivationSourceIsHashSelected(t *testing.T) {
	f, _ := claimsFSM(t)
	registerActive(t, f, "s2", arbiter.NodeRoleSNode)
	key, account := fixedClient(t)
	seen := map[string]bool{}
	for seq := uint64(1); seq <= 16; seq++ {
		r := submit(t, f, registryEnvelope(t, key, account, seq, "db.t", genesisSchemaHash(t, f)))
		if r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("statement %d = %+v", seq, r)
		}
		ss := f.st.Statements[r.StatementSeq]
		if want := f.selectSource(ss.Env.StatementID.Flat()); ss.SourceNode != want {
			t.Fatalf("statement %d bound %q, want the hash selection %q", seq, ss.SourceNode, want)
		}
		seen[ss.SourceNode] = true
	}
	if !seen["s1"] || !seen["s2"] {
		t.Fatalf("pre-activation sources %v; the fixed key must reach both writers", seen)
	}
}
```

Create `server/source_unavailable_test.go` (reuses `fakeNode`, `newServerTestFSM`, `publishServerGenesis`, `mustDirectApply`, `serverTestAuthority`, `recordingAdmissionMetrics`, `testAccount`, `signEnvelope`, `signStatementEnvelopeAt`, `pbEnvelope`):

```go
package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// TestSubmitStatementAnswersSourceUnavailable: the FSM's committed
// SOURCE_UNAVAILABLE reaches the client as code 10 with its message, is
// counted under its code name, and leaves no statement behind.
func TestSubmitStatementAnswersSourceUnavailable(t *testing.T) {
	node := &fakeNode{f: newServerTestFSM(t), leader: true}
	publishServerGenesis(t, node)
	// The founding indexer's SNode registered before the activation and has
	// not reached Active.
	mustDirectApply(t, node, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}})
	view, err := node.f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	indexer := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	jws, err := indexer.SignSNodeEnrollment(authority.SNodeEnrollmentStatement{NetworkID: view.Current.NetworkID,
		GenesisSnapshotID: view.GenesisSnapshotID, IndexerID: 1, SNodeNodeID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	// arbiter-core's fixture verifiers verifier-1..3, which no node here registers.
	verifiers := authoritytest.VerifierEntries()
	update := arbiter.ConsensusParamsUpdate{
		NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID, PreviousParamsDigest: view.ParamsDigest,
		ExpectedEpoch: view.Epoch, ExpectedPromotionSeq: view.PromotionSeq, AuthorityAddresses: view.Current.AuthorityAddresses, MaxWriters: 1,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1",
			SIIndexerID: 1, ActivationBlock: 100, Confirmation: arbiter.TableRegistryConfirmationSafe},
		SIIndexers: []arbiter.SIIndexerEntry{{IndexerID: 1, ActivationBlock: 100, Signer: indexer.Address(), SNodeNodeID: "s1", EnrollmentJWS: jws}},
		Verifiers:  verifiers,
	}
	token, err := serverTestAuthority.SignConsensusParamsUpdateAt(update, 1)
	if err != nil {
		t.Fatal(err)
	}
	mustDirectApply(t, node, wire.Command{UpdateConsensusParams: &wire.UpdateConsensusParams{Update: update, AuthorityJWS: token}})

	rec := &recordingAdmissionMetrics{}
	s := New(Deps{Node: node, FSM: node.f, AdmissionMetrics: rec, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second, MaxStatementAge: time.Hour}})
	genesis, ok := node.f.ManifestBySafeBlock(0)
	if !ok {
		t.Fatal("genesis manifest missing")
	}
	key, account := testAccount(t)
	iat := time.Now().Unix()
	env := signEnvelope(t, key, account, 1, iat)
	env.SchemaHash = genesis.Tables[0].SchemaHash
	env.UserJWS = signStatementEnvelopeAt(t, key, env, iat)
	before := node.appliedCount()
	ack, err := (&ingressService{s: s}).SubmitStatement(context.Background(), pbEnvelope(env))
	if err != nil || ack.GetCode() != pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE ||
		ack.GetMessage() != "storage-integrity source s1 of indexer 1 is not active" || ack.GetStatementSeq() != 0 {
		t.Fatalf("ack = %v, err = %v", ack, err)
	}
	if node.appliedCount() != before+1 {
		t.Fatal("SOURCE_UNAVAILABLE is the FSM's committed answer: the statement is proposed exactly once")
	}
	if !slices.Equal(rec.codes, []string{"ADMISSION_CODE_SOURCE_UNAVAILABLE"}) {
		t.Fatalf("admission metric labels = %v", rec.codes)
	}
	// testAccount mixes case on purpose; the FSM indexes the lowercased account.
	if _, found := node.f.StatementAck(strings.ToLower(account) + ":1:n"); found {
		t.Fatal("a refused statement was sequenced")
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && go test ./fsm/ ./server/ -run 'OwnersSNode|UnavailableSource|SourceUnavailable|PreActivationSourceIsHashSelected' -v`
Expected: `TestPreActivationSourceIsHashSelected` PASSES (nothing changed before the activation); `TestAdmissionBindsTheOwnersSNode` FAILS (`statement … bound "s2", want the owner's SNode "s1"`: hash selection still decides); the SOURCE_UNAVAILABLE tests FAIL with `ACCEPTED` answers.

- [ ] **Step 2: Bind the owner's SNode in admission**

`fsm/admission.go`, step 4 (lines 78-92) keeps the live incarnation for the source:

```go
	// 4. schema allowlist: once the dynamic SI table registry is enabled, only
	// an Active incarnation with the signed schema hash is admitted. The gate
	// keys on the committed parameter, so replaying pre-enablement logs is
	// unchanged. live is kept for the owner-bound source below.
	var live *TableIncarnation
	if f.st.Params.TableRegistry != nil {
		live = f.st.TableRegistry.Live(env.TargetTableID)
		switch {
		case live == nil:
			return SubmitResult{Code: arbiter.AdmissionCodeSchemaNotAllowed, Message: fmt.Sprintf("target table %q is not a registered storage-integrity table", env.TargetTableID)}
		case live.Status != TableStatusActive:
			return SubmitResult{Code: arbiter.AdmissionCodeSchemaNotAllowed, Message: fmt.Sprintf("target table %q is %s, not active", env.TargetTableID, live.Status)}
		case live.SchemaHash != env.SchemaHash:
			return SubmitResult{Code: arbiter.AdmissionCodeSchemaNotAllowed, Message: fmt.Sprintf("schema_hash does not match the registered schema of %q", env.TargetTableID)}
		}
	}
```

between the table-set fence (ends line 112) and the lane budget (line 113):

```go
	// 5a. Owner-bound source (spec 2026-10-10 D5, §6.4): once si_indexers is
	// committed the source is the SNode of the live incarnation's owner
	// (rule 1 makes the registry, and so live, present). When that SNode is
	// not registered and Active the statement is refused before the only
	// state mutation, so its client_seq stays unspent and the client retries;
	// sequencing it would leave its block waiting for an RC that cannot
	// arrive. A duplicate, a refused target and both fences answer first, the
	// lane budget after.
	var source string
	if f.signedClaimsActiveLocked() {
		var unavailable string
		if source, unavailable = f.ownerSourceLocked(live); unavailable != "" {
			return SubmitResult{Code: arbiter.AdmissionCodeSourceUnavailable, Message: unavailable}
		}
	}
```

and in step 6 replace the `ss := &StatementState{…}` line (139) with:

```go
	if !f.signedClaimsActiveLocked() {
		// Hash selection over the Active writer pool (design §5.4) binds every
		// statement committed before the signed-claims activation, so a log
		// replayed across the upgrade picks the same sources.
		source = f.selectSource(flat)
	}
	ss := &StatementState{Env: norm, Seq: seq, Status: StatusSequenced, SourceNode: source}
```

`fsm/select.go`: the `selectSource` doc becomes "selectSource picks the statement's source SNode before the signed-claims activation (§5.4): hash-mod over the sorted Active writer pool. … After the activation admission binds ownerSourceLocked instead; the snapshot-query lane still calls this." Add (with `"fmt"` imported):

```go
// ownerSourceLocked is a statement's source once si_indexers is committed
// (spec 2026-10-10 D5, §6.4): the SNode of the entry that owns the target's
// live incarnation. unavailable is the SOURCE_UNAVAILABLE message when that
// SNode is not registered and Active. An owner without an entry cannot occur
// (AddTable, the seeds and the migration only record enrolled owners); it is
// refused the same way rather than bound to nothing.
func (f *FSM) ownerSourceLocked(inc *TableIncarnation) (source, unavailable string) {
	owner := f.st.Params.TableRegistry.SIIndexerID
	if inc != nil && inc.OwnerIndexerID != nil {
		owner = *inc.OwnerIndexerID
	}
	entry, ok := f.siIndexerLocked(owner)
	if !ok {
		return "", fmt.Sprintf("storage-integrity source of indexer %d is not enrolled", owner)
	}
	if node := f.st.Nodes[entry.SNodeNodeID]; node == nil || node.Status != NodeActive {
		return "", fmt.Sprintf("storage-integrity source %s of indexer %d is not active", entry.SNodeNodeID, owner)
	}
	return entry.SNodeNodeID, ""
}
```

`server/ingress.go`, extend the refusal switch (lines 109-112):

```go
	case arbiter.AdmissionCodeSourceUnavailable:
		// The owner's SNode is down or not yet Active: an operator signal, and
		// a retryable answer for the client (spec 2026-10-10 §6.4).
		svc.s.d.Logger.Warn("statement admission refused: storage-integrity source unavailable", "target", env.TargetTableID, "message", submit.Message, "account", env.StatementID.ClientAccount, "seq", env.StatementID.ClientSeq)
```

- [ ] **Step 3: Prove pre-activation replay byte-identical and cross-activation replay deterministic**

The repository has no committed state-digest tool (the client-lanes rollout's `statedigest` was a throwaway, never committed); the in-repository byte-identity proof is the capture-once golden pattern of `fsm/legacy_lane_golden_test.go`, which Task 5 Step 1 applied to a pre-activation history (`fsm/testdata/pre_signed_claims_golden.json`, captured before any stage-1 change and never edited). Add the replica determinism check across the activation in `fsm/determinism_source_test.go`, in the style of `lanesScript` (`fsm/determinism_lanes_test.go:22`):

```go
package fsm

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/hashicorp/raft"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// sourceBindingScript is a history across the signed-claims activation as
// encoded RaftCommand bytes: a two-writer pool, the seeded table registry,
// eight statements to db.t under hash selection, a seal, the activation
// naming both running SNodes (s1 for the founding indexer 1, s2 for indexer
// 2), eight more statements to db.t and a seal.
func sourceBindingScript(t *testing.T) ([][]byte, Params) {
	t.Helper()
	signer, err := authority.NewSignerFromHex(legacyGoldenAuthorityHex)
	if err != nil {
		t.Fatal(err)
	}
	params := testParams()
	params.AuthorityAddresses = []string{signer.Address()}
	f := mustNewFSM(t, params)
	var script [][]byte
	mustOK := func(c wire.Command) {
		t.Helper()
		b, err := wire.Encode(c)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		script = append(script, b)
		if r, bad := f.Apply(&raft.Log{Data: b}).(Rejected); bad {
			t.Fatalf("script rejected: %s", r.Reason)
		}
	}
	enable := registryUpdate(t, f, params, testRegistryParams())
	enable.MaxWriters = 2
	mustOK(signedConsensusUpdate(t, signer, enable))
	mustOK(wire.Command{SeedLegacyTables: &wire.SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 99, Hash: "0x99"}}})
	for _, id := range []string{"s1", "s2", "v1", "v2", "v3"} {
		reg := arbiter.NodeRegistration{NodeID: id, Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}
		if id[0] == 'v' {
			reg.Roles, reg.Ed25519Pubkey = []arbiter.NodeRole{arbiter.NodeRoleVerifier}, testPubkey(id[1])
		}
		mustOK(wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg}})
		mustOK(wire.Command{MarkActive: &wire.MarkActive{NodeID: id}})
	}
	key, account := fixedClient(t)
	hash := genesisSchemaHash(t, f)
	for seq := uint64(1); seq <= 8; seq++ {
		mustOK(wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: registryEnvelope(t, key, account, seq, "db.t", hash)}})
	}
	mustOK(wire.Command{SealL3Block: &wire.SealL3Block{}})
	mustOK(signedConsensusUpdate(t, signer, claimsUpdate(t, f,
		[]arbiter.SIIndexerEntry{foundingEntry(t, f), siEntry(t, f, indexerSigner2(t), 2, 150, "s2")}, testVerifiers("v1", "v2", "v3"))))
	for seq := uint64(9); seq <= 16; seq++ {
		mustOK(wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: registryEnvelope(t, key, account, seq, "db.t", hash)}})
	}
	mustOK(wire.Command{SealL3Block: &wire.SealL3Block{}})
	return script, params
}

// TestDeterminism_SourceBindingAcrossActivation: two replicas applying the
// same log across the activation answer and end byte for byte alike; every
// statement before the activation is bound by hash selection and every one
// after by db.t's owner, the founding indexer's s1.
func TestDeterminism_SourceBindingAcrossActivation(t *testing.T) {
	script, params := sourceBindingScript(t)
	a, b := mustNewFSM(t, params), mustNewFSM(t, params)
	for i, cmd := range script {
		if ra, rb := a.Apply(&raft.Log{Data: cmd}), b.Apply(&raft.Log{Data: cmd}); !reflect.DeepEqual(ra, rb) {
			t.Fatalf("command %d: replicas answered %+v and %+v", i, ra, rb)
		}
	}
	if !bytes.Equal(snapshotBytes(t, a), snapshotBytes(t, b)) {
		t.Fatal("replicas replaying the same log across the activation diverged")
	}
	hashedToS2 := false
	for seq := uint64(1); seq <= 16; seq++ {
		ss := a.st.Statements[seq]
		if ss == nil {
			t.Fatalf("statement %d was not sequenced", seq)
		}
		want := "s1" // after the activation: db.t's owner is the founding indexer 1
		if seq <= 8 {
			want = a.selectSource(ss.Env.StatementID.Flat()) // before it: design §5.4
			hashedToS2 = hashedToS2 || want == "s2"
		}
		if ss.SourceNode != want {
			t.Fatalf("statement %d bound %q, want %q", seq, ss.SourceNode, want)
		}
	}
	if !hashedToS2 {
		t.Fatal("no pre-activation statement hashed to s2; the script cannot tell the two rules apart")
	}
}
```

Run: `cd "$W" && go test ./fsm/ -run 'TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims|TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease|TestDeterminism_' -v && git diff --exit-code -- fsm/testdata`
Expected: all PASS and `git diff` reports no change under `fsm/testdata` (no golden was touched).

- [ ] **Step 4: Run everything**

Run: `cd "$W" && bazel run //:gazelle && bazel build //... && bazel test //...`
Expected: all PASS: the two history goldens, every `TestDeterminism_*`, the L3 goldens, `TestAdmission_*`, every lane admission test, the table registry and table-set suites and the server ingress tests.

- [ ] **Step 5: Commit**

```bash
cd "$W" && git add fsm server && git commit -m "feat(admission): owner-bound sources and SOURCE_UNAVAILABLE

After the signed-claims activation a statement's source is the SNode of
its target's owner; when that SNode is not registered and Active the
statement is refused with SOURCE_UNAVAILABLE before its client_seq is
spent. Hash selection stays for every statement before the activation,
which a frozen pre-activation golden and a cross-activation replica
replay pin (housegate spec 2026-10-10 D5, §6.4).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 8: arbiter — signed SNode and verifier messages, registration sequences, authority-signed eviction (S1)

**Files:**
- Modify: `fsm/state.go` (`State` 300-345: `RegistrationSeqs` after `Nodes` at 327; `newState` 898-934: initialized after `Nodes` at 916)
- Modify: `fsm/signed_claims.go` (created by Task 5): the message-verification helpers below
- Modify: `fsm/apply.go` (`applyRegisterNode` 43-76, `applyMarkActive` 78-89, `applyEvictNode` 91-107, `applyRegisterRC` 354-405 with its lane refusal at 375-377)
- Modify: `fsm/apply_table_registry.go` (`applyRecordTablePurged` 229-250)
- Create: `fsm/signed_messages_fixture_test.go`, `fsm/signed_messages_test.go`
- Modify: `server/membership.go` (`RegisterNode` 19-52, `MarkActive` 54-62), `server/claims.go` (`RegisterResultClaim` 20-49), `server/gateway.go` (`AckPromotion` 112-123, `AckCleanup` 132-143), `server/table_purge.go` (`SubmitTablePurged` 23-56)
- Create: `server/signed_messages.go`, `server/signed_messages_test.go` (Task 5 owns `server/signed_claims_test.go`)
- Modify: `cmd/arbiter-verifier/config.go` (`Config` 28-48, `toRoleConfig` 232-241), `cmd/arbiter-verifier/main.go` (`run`, before `verifier.New` at 213), `cmd/arbiter-verifier/main_test.go` (contract §3b.9)
- Modify: `cmd/arbiter-snode/config.go` (`Config` 24-44, `toRoleConfig` 217-226), `cmd/arbiter-snode/main.go` (`run` 179-213: the signer after `validate` at 183-185, `snode.Deps` at 206-208), `cmd/arbiter-snode/main_test.go` (contract §3c)

**Interfaces:**
- Consumes:
  - `PROTO_TAG` (Task 1): `pb.NodeRegistration.{RegistrationSeq,SignerJws,Ed25519Signature}`, `pb.NodeRef.{RegistrationSeq,SignerJws,Ed25519Signature}`, `pb.RCRecord.SourceJws`, `pb.PromotionAck.SourceJws`, `pb.CleanupAck.SourceJws`, `pb.RecordTablePurgedCmd.{SignerJws,Ed25519Signature}` (the `SubmitTablePurged` input is the Raft command itself, contract §3a), `pb.EvictNodeRequest`, `pb.ConsensusAdminServer.EvictNode`.
  - `CORE_TAG` (Tasks 2–3): `arbiter.NodeRegistration.RegistrationSeq`, `arbiter.SignedClaimsFeature`, `arbiter.LocalNodeFeatures()`, `arbiter.EvictNodeCommand`; `authority.{MessageContext, ConsensusContext, MarkActiveBody, TablePurgedBody, SNodeMessageRegistration, SNodeMessageMarkActive, SNodeMessageResultClaim, SNodeMessageTablePurged, VerifierMessageRegistration, VerifierMessageMarkActive, VerifierMessageTablePurged, VerifySNodeMessage, SignVerifierMessage, VerifyVerifierMessage}`, `(*authority.Signer).{SignSNodeMessage, SignEvictNodeWithContext}`, `(*authority.Validator).{VerifyEvictNode, AuthorizeEvictNode}`; the `wire` command fields of CONTRACT §2, `(wire.EvictNode).Canonical()`, `wire.StripSignedClaims`, `wire.{RegisterNodeFromRequest, MarkActiveFromRequest, RegisterRCFromRequest, RecordPromotionAckFromRequest, RecordCleanupAckFromRequest, RecordTablePurgedFromRequest, EvictNodeFromRequest}`, `wire.{RegistrationFromPB, RegistrationToPB, RCFromPB, RCToPB}` (mapping `registration_seq`); `authoritytest.{MustSigner, IndexerKeyHex0, IndexerKeyHex1}` (`MustSigner(t testing.TB, keyHex string)`); `verifier.Config.{StateDir, GenesisSnapshotID}` (Task 4: `StateDir` holds `registration.json`, empty keeps the sequence in memory; `GenesisSnapshotID` empty derives it from the genesis tables); `snode.Config.{IndexerID, GenesisSnapshotID}` and `snode.Deps.ClaimSigner` (Task 4; nil keeps a legacy unsigned SNode, §3b.6), `authority.NewSignerFromHex`, `authoritytest.{IndexerKeyHex1, IndexerAddr1}`.
  - Tasks 5–7: `Params.SIIndexers` / `.Verifiers`, `signedClaimsActiveLocked`, `(*FSM).SignedClaimsActive`, `siIndexerBySNodeLocked`, `messageContextLocked`, `listsVerifier`, the activation rules as amended by contract §3c (the activation evicts every registered SNode it does not name and every registered verifier it does not list), `ownedAddCmd`, the test helpers `claimsFSM`, `activateClaims`, `siEntry`, `indexerSigner1`, `indexerSigner2`, and the frozen golden `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`; existing `authorityAddressSet` (`fsm/consensus_updates.go:161`, `server/consensus_admin.go:409`), `genesisSnapshotIDLocked`, `consensusLeaderCheck` (`server/consensus_admin.go:420`), `proposeTracked`, `isDataPlaneRegistration` (`server/features.go:237`).
- Produces:
  - fsm: `State.RegistrationSeqs map[string]uint64` (initialized empty in `newState`, never written before the activation); `const signedFieldsBeforeActivationReason`; `(f *FSM) snodeSignatureLocked`, `(f *FSM) verifierSignatureLocked`, `(f *FSM) signedRegistrationLocked(c) (reason string, idempotent bool)`, `sameRegistration(a, b arbiter.NodeRegistration) bool`, `(f *FSM) signedMarkActiveRefusalLocked`, `(f *FSM) resultClaimRefusalLocked`, `(f *FSM) purgeReportRefusalLocked(c, node)` (Task 9 adds `inc`), `(f *FSM) evictNodeRefusalLocked`.
  - cmd/arbiter-verifier: YAML `state_dir` and `genesis_snapshot_id` (`Config.StateDir`, `Config.GenesisSnapshotID`) passed through `toRoleConfig`; a startup warning when `state_dir` is empty.
  - cmd/arbiter-snode (contract §3c): `const envSNodeClaimKeyHex = "ARBITER_SNODE_CLAIM_KEY_HEX"` (environment only, never argv) → `snode.Deps.ClaimSigner`; YAML `indexer_id` (`Config.IndexerID *uint64`, required with the key; 0 is a valid indexer) → `snode.Config.IndexerID`; YAML `genesis_snapshot_id` (`Config.GenesisSnapshotID`) → `snode.Config.GenesisSnapshotID`; `func (c Config) claimSigner() (*authority.Signer, error)`; without the key a startup warning and a legacy unsigned SNode.
  - server: `(s *Server) signedClaimsCommand(wire.Command) wire.Command`; `(*consensusAdminService).EvictNode`; refusals `FailedPrecondition "signed claims are active; this binary does not sign its messages"` (RegisterNode) and `FailedPrecondition "eviction requires the signed-claims activation"` (EvictNode, §3a).
  - fsm test helpers (`fsm/signed_messages_fixture_test.go`): `signedClaimsFSM`, `messageContextFor`, `authorityContextFor`, `snodeRegistration`, `snodeMarkActive`, `verifierRegistration`, `verifierMarkActive`, `snodePurged`, `verifierPurged`, `purgingIncarnation`, `rejectSignedUnchanged`.
  - server test helpers (`server/signed_messages_test.go`): `recordingNode`, `newRecordingNode`, `(*recordingNode).proposals`, `lastProposalIs`, `serverVerifierKey`, `serverMessageContext`, `serverAuthorityContext`, `serverEnrolledEntry`, `serverRegistryParams`, `applyServerUpdate`, `enableServerTableRegistry`, `seedServerRegistry`, `activateServerSignedClaims`, `serverPurgingIncarnation`, `evictRequest`, `evictionAdmin`.

Rules (contract §0, §3 "Signed messages in Apply", §3a):
- **Before activation** every new field (`registration_seq`, `signer_jws`, `ed25519_signature`, `source_jws`, `expected_registration_seq`, `authority_jws`) makes the command `Rejected{signedFieldsBeforeActivationReason}` with no state change, checked first. A voter that predates the fields refuses the same bytes at decode (wire's strict decoder), so an upgraded voter must refuse too: "ignoring" the field would leave the two kinds of voter in different states (Contract conflicts C4). The leader never proposes such a command (`wire.StripSignedClaims`), so a healthy network never reaches the refusal; without the fields every command applies exactly as today, which `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims` pins.
- **After activation:** a `RegisterNode` carries exactly one data-plane role (a role-less registration under an enrolled id would otherwise replace that node's record unsigned, C6); an SNODE must be an entry's `snode_node_id` and sign `registration` with that entry's signer, a VERIFIER must be listed as `(node_id, ed25519_pubkey)` and sign with that key; `registration_seq > RegistrationSeqs[node]` replaces the record and records the sequence (producers floor it to the wall clock in milliseconds, §3a, so nothing here assumes `+1` steps). The last applied sequence again, with the identical canonical registration and a valid signature, is the dataplane client's `WithLeaderRetry` resending an identical request: `Applied{}` with no change at all, never a reset to Syncing (§3b.4); an equal sequence with any other body, and every lower one, is refused. Signatures are verified, never compared as bytes (every token carries a fresh `iat`). `MarkActive` names `RegistrationSeqs[node]` (0 for a node registered before the activation and not since) and carries the role's signature over `MarkActiveBody`; repeating it at the current sequence is idempotent. `RegisterRC` is verified (`result_claim` under the source entry's signer) before it parks or binds. `RecordTablePurged` is signed by the reporter's role (`TablePurgedBody`); Task 9 adds the owner check. `EvictNode` needs `expected_registration_seq == RegistrationSeqs[node]` and an `arbiter-evict-node-v1` token that arbiter-core's `Validator.VerifyEvictNode` accepts under the current {network, genesis, epoch}, built in Apply exactly like `VerifyConsensusParamsUpdate` (`fsm/consensus_updates.go:105-106`) and `VerifyQueryProfileActivation` (`fsm/apply_activate_query_profile.go:29-30`); the legacy unauthenticated shape is refused.
- **Gateway:** every handler decodes its request with the Task 3 converter and proposes through `signedClaimsCommand`, which applies `wire.StripSignedClaims` until this leader has applied the activation (§0). After the activation a data-plane registration that does not advertise `signed_claims_v1` is refused before proposing. `ConsensusAdmin.EvictNode` is refused before the activation (§3a); afterwards it follows the `ActivateQueryProfile` precedent (`server/consensus_admin.go:115-146`): admission switch, shape, `consensusLeaderCheck`, `Validator.AuthorizeEvictNode` (token age, the only clock read), then the proposal, which Apply re-verifies.

- [ ] **Step 1: Write the FSM fixtures**

Create `fsm/signed_messages_fixture_test.go`. It builds on Task 5's `claimsFSM` / `activateClaims` / `siEntry` and its signer fixtures `indexerSigner1(t)` (authoritytest key 0: the founding indexer 1, SNode s1) and `indexerSigner2(t)` (key 1: every other indexer in these tests), on Task 6's `ownedAddCmd`, and reuses `mustApply`, `testPubkey` (`fsm/fsm_test.go`), `ed25519KeyFor` (`fsm/evidence_test.go:16`, the private half of `testPubkey`), `mustGenesisID` (`fsm/table_registry_test.go:32`), `rejectConsensusUnchanged` (`fsm/consensus_updates_test.go:35`), `ev` and `retireCmd` (`fsm/apply_table_registry_test.go`):

```go
package fsm

import (
	"crypto/ed25519"
	"maps"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// signedClaimsFSM is Task 5's claimsFSM (seeded registry with Legacy db.old,
// cursor at block 99, max_writers 2, s1 and v1..v3 registered and Active
// before the activation) after the signed-claims activation naming the
// founding indexer 1 (SNode s1) and indexer 2 (activation block 150, SNode
// s2, not registered): Task 6's twoIndexerFSM, also returning the network
// authority.
func signedClaimsFSM(t *testing.T) (*FSM, *authority.Signer) {
	t.Helper()
	f, a := claimsFSM(t)
	activateClaims(t, f, a, siEntry(t, f, indexerSigner2(t), 2, 150, "s2"))
	return f, a
}

// messageContextFor is f's signed-message context (spec 2026-10-10 D6): its
// network id and genesis snapshot id.
func messageContextFor(t *testing.T, f *FSM) authority.MessageContext {
	t.Helper()
	ctx, err := f.messageContextLocked()
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}

// authorityContextFor is the {network, genesis, epoch} an authority token
// signed now must carry.
func authorityContextFor(t *testing.T, f *FSM) authority.ConsensusContext {
	t.Helper()
	return authority.ConsensusContext{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: mustGenesisID(t, f), AuthorityEpoch: f.st.ConsensusEpoch}
}

// snodeRegistration is SNode node's RegisterNode at seq, signed by signer over
// the registration exactly as Apply decodes it from the log (wire turns an
// empty repeated field into nil, which hashes differently from an empty
// slice, so the body is round-tripped before signing).
func snodeRegistration(t *testing.T, f *FSM, signer *authority.Signer, node string, seq uint64) wire.Command {
	t.Helper()
	reg := wire.RegistrationFromPB(wire.RegistrationToPB(arbiter.NodeRegistration{
		NodeID: node, Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: seq}))
	jws, err := signer.SignSNodeMessage(authority.SNodeMessageRegistration, messageContextFor(t, f), reg)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg, SignerJWS: jws}}
}

// snodeMarkActive is SNode node's MarkActive at seq, signed by signer.
func snodeMarkActive(t *testing.T, f *FSM, signer *authority.Signer, node string, seq uint64) wire.Command {
	t.Helper()
	jws, err := signer.SignSNodeMessage(authority.SNodeMessageMarkActive, messageContextFor(t, f),
		authority.MarkActiveBody{NodeID: node, RegistrationSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{MarkActive: &wire.MarkActive{NodeID: node, RegistrationSeq: seq, SignerJWS: jws}}
}

// verifierRegistration is verifier node's RegisterNode at seq with the key
// ed25519KeyFor(seed), signed by that key (registerActive registers v<n> with
// seed '<n>').
func verifierRegistration(t *testing.T, f *FSM, node string, seed byte, seq uint64) wire.Command {
	t.Helper()
	key := ed25519KeyFor(seed)
	reg := wire.RegistrationFromPB(wire.RegistrationToPB(arbiter.NodeRegistration{NodeID: node,
		Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: key.Public().(ed25519.PublicKey), RegistrationSeq: seq}))
	sig, err := authority.SignVerifierMessage(key, authority.VerifierMessageRegistration, messageContextFor(t, f), reg)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg, Ed25519Signature: sig}}
}

// verifierMarkActive is verifier node's MarkActive at seq signed with
// ed25519KeyFor(seed).
func verifierMarkActive(t *testing.T, f *FSM, node string, seed byte, seq uint64) wire.Command {
	t.Helper()
	sig, err := authority.SignVerifierMessage(ed25519KeyFor(seed), authority.VerifierMessageMarkActive, messageContextFor(t, f),
		authority.MarkActiveBody{NodeID: node, RegistrationSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{MarkActive: &wire.MarkActive{NodeID: node, RegistrationSeq: seq, Ed25519Signature: sig}}
}

// snodePurged is SNode node's purge report for incarnation seq, signed by signer.
func snodePurged(t *testing.T, f *FSM, signer *authority.Signer, node string, seq uint64) wire.Command {
	t.Helper()
	jws, err := signer.SignSNodeMessage(authority.SNodeMessageTablePurged, messageContextFor(t, f),
		authority.TablePurgedBody{NodeID: node, IncarnationSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: node, IncarnationSeq: seq, SignerJWS: jws}}
}

// verifierPurged is verifier node's purge report for incarnation seq, signed
// with ed25519KeyFor(seed).
func verifierPurged(t *testing.T, f *FSM, node string, seed byte, seq uint64) wire.Command {
	t.Helper()
	sig, err := authority.SignVerifierMessage(ed25519KeyFor(seed), authority.VerifierMessageTablePurged, messageContextFor(t, f),
		authority.TablePurgedBody{NodeID: node, IncarnationSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: node, IncarnationSeq: seq, Ed25519Signature: sig}}
}

// purgingIncarnation adds db.p owned by indexer owner (created at block 100,
// schema at 101) and retires it at block 110: a Pending incarnation without a
// sealed add transition retires straight to Purging. Every block stays below
// indexer 2's activation block 150, as the unseeded-indexer guard requires.
// It returns the incarnation's seq.
func purgingIncarnation(t *testing.T, f *FSM, owner uint64) uint64 {
	t.Helper()
	mustApply(t, f, ownedAddCmd(t, f, "db", "p", ev(100, "0x100", 0), ev(101, "0x101", 0), owner))
	mustApply(t, f, retireCmd("db", wire.TableRetireReasonTableDeleted, ev(110, "0x110", 0), "p"))
	inc := f.st.TableRegistry.Live("db.p")
	if inc == nil || inc.Status != TableStatusPurging {
		t.Fatalf("db.p = %+v, want Purging", inc)
	}
	return inc.Seq
}

// rejectSignedUnchanged is rejectConsensusUnchanged that also compares the
// registration sequences, which only Task 10 adds to the snapshot document.
func rejectSignedUnchanged(t *testing.T, f *FSM, cmd wire.Command, reason string) {
	t.Helper()
	seqs := maps.Clone(f.st.RegistrationSeqs)
	rejectConsensusUnchanged(t, f, cmd, reason)
	if !maps.Equal(seqs, f.st.RegistrationSeqs) {
		t.Fatalf("a rejected command changed registration_seqs: %v -> %v", seqs, f.st.RegistrationSeqs)
	}
}
```

- [ ] **Step 2: Write the failing FSM tests**

Create `fsm/signed_messages_test.go` (reuses `newTestFSM`, `registerActive`, `testAccount` / `validEnvelope` / `submit` from `fsm/admission_test.go`, `rcFor` from `fsm/rc_test.go:13`, `registryEnvelope` / `genesisSchemaHash` from `fsm/admission_table_registry_test.go`):

```go
package fsm

import (
	"crypto/ed25519"
	"encoding/hex"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// TestSignedFieldsAreRefusedBeforeActivation: before the signed-claims
// activation every new field is refused with no state change, the way a voter
// that predates the fields refuses the command at decode; without them the
// same commands apply exactly as before.
func TestSignedFieldsAreRefusedBeforeActivation(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	registerActive(t, f, "v1", arbiter.NodeRoleVerifier)
	key, account := testAccount(t)
	submit(t, f, validEnvelope(t, key, account, 1))
	rc := rcFor(f, 1, "0xr00t", "0xffee")
	snode := arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}
	sequenced := snode
	sequenced.RegistrationSeq = 1760000000000
	for name, cmd := range map[string]wire.Command{
		"registration_seq":     {RegisterNode: &wire.RegisterNode{Registration: sequenced}},
		"registration signer":  {RegisterNode: &wire.RegisterNode{Registration: snode, SignerJWS: "h.p.s"}},
		"registration ed25519": {RegisterNode: &wire.RegisterNode{Registration: snode, Ed25519Signature: "00"}},
		"mark active seq":      {MarkActive: &wire.MarkActive{NodeID: "s1", RegistrationSeq: 1}},
		"mark active signer":   {MarkActive: &wire.MarkActive{NodeID: "s1", SignerJWS: "h.p.s"}},
		"mark active ed25519":  {MarkActive: &wire.MarkActive{NodeID: "v1", Ed25519Signature: "00"}},
		"rc source_jws":        {RegisterRC: &wire.RegisterRC{RC: rc, SourceJWS: "h.p.s"}},
		"purge signer":         {RecordTablePurged: &wire.RecordTablePurged{NodeID: "s1", IncarnationSeq: 1, SignerJWS: "h.p.s"}},
		"purge ed25519":        {RecordTablePurged: &wire.RecordTablePurged{NodeID: "v1", IncarnationSeq: 1, Ed25519Signature: "00"}},
		"evict expected seq":   {EvictNode: &wire.EvictNode{NodeID: "v1", Reason: "r", ExpectedRegistrationSeq: 1}},
		"evict authority_jws":  {EvictNode: &wire.EvictNode{NodeID: "v1", Reason: "r", AuthorityJWS: "h.p.s"}},
	} {
		t.Run(name, func(t *testing.T) { rejectSignedUnchanged(t, f, cmd, signedFieldsBeforeActivationReason) })
	}
	mustApply(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: snode}})
	mustApply(t, f, wire.Command{MarkActive: &wire.MarkActive{NodeID: "s1"}})
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}})
	mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: "v1", Reason: "r"}})
	if len(f.st.RegistrationSeqs) != 0 {
		t.Fatalf("a pre-activation command recorded registration_seqs %v", f.st.RegistrationSeqs)
	}
}

// TestSNodeRegistrationAfterActivation: an SNODE registration names an
// enrolled snode_node_id, carries one data-plane role, is signed by that
// entry's signer and raises the node's registration_seq; a replay cannot push
// a running node back to Syncing.
func TestSNodeRegistrationAfterActivation(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	founder, second := indexerSigner1(t), indexerSigner2(t)
	unsigned := wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "s2", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 1}}}
	rejectSignedUnchanged(t, f, unsigned, "snode signature")
	rejectSignedUnchanged(t, f, snodeRegistration(t, f, founder, "s2", 1), "snode signature") // indexer 1's key for indexer 2's SNode
	rejectSignedUnchanged(t, f, snodeRegistration(t, f, second, "s9", 1), "s9 is not the snode_node_id of any si_indexers entry")
	rejectSignedUnchanged(t, f, snodeRegistration(t, f, second, "s2", 0), "registration_seq 0 must exceed the last applied 0")
	both := snodeRegistration(t, f, second, "s2", 1)
	both.RegisterNode.Registration.Roles = append(both.RegisterNode.Registration.Roles, arbiter.NodeRoleVerifier)
	both.RegisterNode.Registration.Ed25519Pubkey = testPubkey(9)
	rejectSignedUnchanged(t, f, both, "exactly one of the SNODE and VERIFIER roles")
	// A role-less registration would replace s1's record unsigned.
	roleless := wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", RegistrationSeq: 7}}}
	rejectSignedUnchanged(t, f, roleless, "exactly one of the SNODE and VERIFIER roles")

	mustApply(t, f, snodeRegistration(t, f, second, "s2", 1))
	if f.st.RegistrationSeqs["s2"] != 1 || f.st.Nodes["s2"].Status != NodeSyncing {
		t.Fatalf("seqs %v, node %+v", f.st.RegistrationSeqs, f.st.Nodes["s2"])
	}
	mustApply(t, f, snodeMarkActive(t, f, second, "s2", 1))
	// WithLeaderRetry resends an identical request: the same sequence and
	// body under a fresh, valid signature is a no-op that never pushes a
	// running node back to Syncing (contract §3b.4).
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 1))
	if f.st.Nodes["s2"].Status != NodeActive || f.st.RegistrationSeqs["s2"] != 1 {
		t.Fatalf("an identical resend changed s2: %+v, seq %d", f.st.Nodes["s2"], f.st.RegistrationSeqs["s2"])
	}
	moved := arbiter.NodeRegistration{NodeID: "s2", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, DialAddr: "10.0.0.9:7000", RegistrationSeq: 1}
	movedJWS, err := second.SignSNodeMessage(authority.SNodeMessageRegistration, messageContextFor(t, f), moved)
	if err != nil {
		t.Fatal(err)
	}
	rejectSignedUnchanged(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: moved, SignerJWS: movedJWS}},
		"registration_seq 1 is the last applied one but the registration differs")
	unsignedResend := snodeRegistration(t, f, second, "s2", 1)
	unsignedResend.RegisterNode.SignerJWS = ""
	rejectSignedUnchanged(t, f, unsignedResend, "snode signature")
	// The grandfathered s1 (registered before the activation, no recorded
	// sequence) re-registers from any sequence above zero; producers floor
	// the sequence to the wall clock in milliseconds (contract §3a).
	mustApply(t, f, snodeRegistration(t, f, founder, "s1", 1760000000123))
	if f.st.RegistrationSeqs["s1"] != 1760000000123 {
		t.Fatalf("s1 seq = %d", f.st.RegistrationSeqs["s1"])
	}
}

// TestSignedMessagesBindNetworkGenesisAndKind: a valid signature over another
// network, another genesis or another message kind is refused.
func TestSignedMessagesBindNetworkGenesisAndKind(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	second := indexerSigner2(t)
	reg := wire.RegistrationFromPB(wire.RegistrationToPB(arbiter.NodeRegistration{
		NodeID: "s2", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 1}))
	ctx := messageContextFor(t, f)
	for name, tc := range map[string]struct {
		kind authority.SNodeMessageKind
		ctx  authority.MessageContext
	}{
		"network": {authority.SNodeMessageRegistration, authority.MessageContext{NetworkID: "another-network", GenesisSnapshotID: ctx.GenesisSnapshotID}},
		"genesis": {authority.SNodeMessageRegistration, authority.MessageContext{NetworkID: ctx.NetworkID, GenesisSnapshotID: "0x" + strings.Repeat("ab", 32)}},
		"kind":    {authority.SNodeMessageMarkActive, ctx},
	} {
		jws, err := second.SignSNodeMessage(tc.kind, tc.ctx, reg)
		if err != nil {
			t.Fatal(err)
		}
		t.Run(name, func(t *testing.T) {
			rejectSignedUnchanged(t, f, wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg, SignerJWS: jws}}, "snode signature")
		})
	}
}

// TestMarkActiveNeedsTheCurrentRegistrationSeq: a lower registration is
// refused; MarkActive names the node's current registration, signed by its
// role's key, and repeats idempotently; a stale one is refused.
func TestMarkActiveNeedsTheCurrentRegistrationSeq(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	founder, second := indexerSigner1(t), indexerSigner2(t)
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 1))
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 2))
	rejectSignedUnchanged(t, f, snodeRegistration(t, f, second, "s2", 1), "registration_seq 1 is below the last applied 2")
	rejectSignedUnchanged(t, f, snodeMarkActive(t, f, second, "s2", 1), "registration_seq 1 is not the node's current 2")
	rejectSignedUnchanged(t, f, snodeMarkActive(t, f, founder, "s2", 2), "snode signature")
	rejectSignedUnchanged(t, f, wire.Command{MarkActive: &wire.MarkActive{NodeID: "s2", RegistrationSeq: 2}}, "snode signature")
	mustApply(t, f, snodeMarkActive(t, f, second, "s2", 2))
	mustApply(t, f, snodeMarkActive(t, f, second, "s2", 2)) // a retried MarkActive at the current sequence
	if f.st.Nodes["s2"].Status != NodeActive {
		t.Fatalf("s2 = %+v", f.st.Nodes["s2"])
	}
	// The grandfathered s1 has no recorded sequence: its current one is 0.
	mustApply(t, f, snodeMarkActive(t, f, founder, "s1", 0))
}

// TestVerifierRegistrationAfterActivation: only a listed (node_id, key) pair
// may register, signed by that key; its activation is signed too.
func TestVerifierRegistrationAfterActivation(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	rejectSignedUnchanged(t, f, verifierRegistration(t, f, "v4", '4', 1), "(v4, ed25519_pubkey) is not a verifiers entry")
	rejectSignedUnchanged(t, f, verifierRegistration(t, f, "v1", '9', 1), "(v1, ed25519_pubkey) is not a verifiers entry")
	forged := verifierRegistration(t, f, "v1", '1', 1)
	forged.RegisterNode.Ed25519Signature = hex.EncodeToString(ed25519.Sign(ed25519KeyFor('2'), []byte("not the registration")))
	rejectSignedUnchanged(t, f, forged, "verifier signature")
	mustApply(t, f, verifierRegistration(t, f, "v1", '1', 1))
	rejectSignedUnchanged(t, f, verifierMarkActive(t, f, "v1", '2', 1), "verifier signature")
	mustApply(t, f, verifierMarkActive(t, f, "v1", '1', 1))
	if f.st.Nodes["v1"].Status != NodeActive || f.st.RegistrationSeqs["v1"] != 1 {
		t.Fatalf("v1 = %+v, seqs %v", f.st.Nodes["v1"], f.st.RegistrationSeqs)
	}
	mustApply(t, f, verifierRegistration(t, f, "v1", '1', 1)) // an identical resend
	if f.st.Nodes["v1"].Status != NodeActive {
		t.Fatal("an identical verifier resend pushed v1 back to Syncing")
	}
}

// TestSameRegistrationCoversEveryField: the idempotent-resend comparison sees
// every field of arbiter.NodeRegistration and treats a nil and an empty list
// or key alike, as the signed digest and a snapshot round trip do.
func TestSameRegistrationCoversEveryField(t *testing.T) {
	if n := reflect.TypeOf(arbiter.NodeRegistration{}).NumField(); n != 5 {
		t.Fatalf("arbiter.NodeRegistration has %d fields; extend sameRegistration", n)
	}
	a := arbiter.NodeRegistration{NodeID: "s2", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 1}
	b := a
	b.Ed25519Pubkey, b.Roles = []byte{}, slices.Clone(a.Roles)
	if !sameRegistration(a, b) {
		t.Fatal("a nil and an empty key must compare equal")
	}
	b.DialAddr = "10.0.0.9:7000"
	if sameRegistration(a, b) {
		t.Fatal("a different dial address must differ")
	}
}

// TestResultClaimsAreVerifiedBeforeTheyParkOrBind: a forged or wrongly signed
// RC can no longer occupy a statement's first-wins slot (spec 2026-10-10
// §6.5), parked or bound.
func TestResultClaimsAreVerifiedBeforeTheyParkOrBind(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	founder, second := indexerSigner1(t), indexerSigner2(t)
	key, account := testAccount(t)
	env := registryEnvelope(t, key, account, 1, "db.t", genesisSchemaHash(t, f))
	rc := wire.RCFromPB(wire.RCToPB(arbiter.RCRecord{StatementID: env.StatementID, SourceNode: "s1", SourceClaimRoot: "0xr00t",
		CandidateParts: []arbiter.CandidatePart{{TableID: "db.t", PartitionID: "p0", PartName: "all_1_0", PartRowLtHash: "0xffee", RowCount: 1, Bytes: 32}}}))
	signed := func(signer *authority.Signer, rc arbiter.RCRecord) wire.Command {
		t.Helper()
		jws, err := signer.SignSNodeMessage(authority.SNodeMessageResultClaim, messageContextFor(t, f), rc)
		if err != nil {
			t.Fatal(err)
		}
		return wire.Command{RegisterRC: &wire.RegisterRC{RC: rc, SourceJWS: jws}}
	}
	impostor := rc
	impostor.SourceNode = "impostor"
	rejectSignedUnchanged(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}}, "rc source snode signature")
	rejectSignedUnchanged(t, f, signed(second, rc), "rc source snode signature")
	rejectSignedUnchanged(t, f, signed(founder, impostor), "rc source impostor is not the snode_node_id")
	if len(f.st.PendingRC) != 0 {
		t.Fatal("a refused RC was parked")
	}
	mustApply(t, f, signed(founder, rc))
	if f.st.PendingRC[env.StatementID.Flat()] == nil {
		t.Fatal("the signed RC did not park")
	}
	// Task 7 binds db.t's owner (the founding indexer's s1), so admission
	// adopts the parked RC.
	r := submit(t, f, env)
	if r.Code != arbiter.AdmissionCodeAccepted || f.st.Statements[r.StatementSeq].RC == nil {
		t.Fatalf("admission did not bind the parked signed RC: %+v", r)
	}
	// A re-sent bound RC is idempotent only when signed.
	rejectSignedUnchanged(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rc}}, "rc source snode signature")
	mustApply(t, f, signed(founder, rc))
}

// TestPurgeReportsAreSignedAfterActivation: each reporter signs with its
// role's key.
func TestPurgeReportsAreSignedAfterActivation(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	seq := purgingIncarnation(t, f, 1)
	unsigned := func(node string) wire.Command {
		return wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: node, IncarnationSeq: seq}}
	}
	rejectSignedUnchanged(t, f, unsigned("v1"), "v1: verifier signature")
	rejectSignedUnchanged(t, f, unsigned("s1"), "s1: snode signature")
	rejectSignedUnchanged(t, f, verifierPurged(t, f, "v1", '2', seq), "v1: verifier signature")
	rejectSignedUnchanged(t, f, snodePurged(t, f, indexerSigner2(t), "s1", seq), "s1: snode signature")
	mustApply(t, f, verifierPurged(t, f, "v1", '1', seq))
	mustApply(t, f, snodePurged(t, f, indexerSigner1(t), "s1", seq))
	if inc := f.st.TableRegistry.incarnation(seq); !slices.Contains(inc.PurgedBy, "v1") || !slices.Contains(inc.PurgedBy, "s1") {
		t.Fatalf("purged_by = %v", inc.PurgedBy)
	}
}

// TestEvictNodeAfterActivationNeedsTheAuthorityAndTheCurrentSeq: eviction is
// an authority command bound to the node's current registration and to the
// current consensus context; the refusal names the committed sequence.
func TestEvictNodeAfterActivationNeedsTheAuthorityAndTheCurrentSeq(t *testing.T) {
	f, a := signedClaimsFSM(t)
	second := indexerSigner2(t)
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 1))
	ctx := authorityContextFor(t, f)
	evict := func(signer *authority.Signer, cmd arbiter.EvictNodeCommand, ctx authority.ConsensusContext) wire.Command {
		t.Helper()
		token, err := signer.SignEvictNodeWithContext(cmd, ctx)
		if err != nil {
			t.Fatal(err)
		}
		return wire.Command{EvictNode: &wire.EvictNode{NodeID: cmd.NodeID, Reason: cmd.Reason,
			ExpectedRegistrationSeq: cmd.ExpectedRegistrationSeq, AuthorityJWS: token}}
	}
	cmd := arbiter.EvictNodeCommand{NodeID: "s2", ExpectedRegistrationSeq: 1, Reason: "decommissioned"}
	rejectSignedUnchanged(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: "s2", Reason: "decommissioned", ExpectedRegistrationSeq: 1}}, "evict node s2: authority")
	stale := cmd
	stale.ExpectedRegistrationSeq = 0
	rejectSignedUnchanged(t, f, evict(a, stale, ctx), "expected_registration_seq 0 is not the node's current 1")
	rejectSignedUnchanged(t, f, evict(indexerSigner1(t), cmd, ctx), "evict node s2: authority")
	previous := ctx
	previous.AuthorityEpoch--
	rejectSignedUnchanged(t, f, evict(a, cmd, previous), "evict node s2: authority")
	other := evict(a, arbiter.EvictNodeCommand{NodeID: "s1", ExpectedRegistrationSeq: 1, Reason: "decommissioned"}, ctx)
	other.EvictNode.NodeID = "s2"
	rejectSignedUnchanged(t, f, other, "evict node s2: authority")
	mustApply(t, f, evict(a, cmd, ctx))
	if n := f.st.Nodes["s2"]; n.Status != NodeEvicted || n.Reason != "decommissioned" {
		t.Fatalf("s2 = %+v", n)
	}
	// Eviction keeps the sequence: an identical resend changes nothing (the
	// node stays evicted), and only a higher sequence brings it back.
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 1))
	if f.st.Nodes["s2"].Status != NodeEvicted {
		t.Fatal("an identical resend revived an evicted node")
	}
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 2))
	if f.st.Nodes["s2"].Status != NodeSyncing {
		t.Fatalf("s2 = %+v", f.st.Nodes["s2"])
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && go test ./fsm/ -run 'SignedFieldsAreRefused|SNodeRegistrationAfterActivation|BindNetworkGenesisAndKind|MarkActiveNeedsTheCurrent|VerifierRegistrationAfterActivation|SameRegistrationCoversEveryField|ResultClaimsAreVerified|PurgeReportsAreSigned|EvictNodeAfterActivation' -v`
Expected: build failure (`f.st.RegistrationSeqs undefined`, `undefined: signedFieldsBeforeActivationReason`).

- [ ] **Step 3: Implement the FSM rules**

`fsm/state.go` — in `State`, after `Nodes map[string]*NodeInfo` (line 327):

```go
	// RegistrationSeqs is the last registration_seq applied per node after the
	// signed-claims activation (spec 2026-10-10 §6.5); before it nothing is
	// ever written. A registration needs a strictly greater sequence, and
	// MarkActive and an authority eviction the equal one. Persisted as
	// registration_seqs in a v19 snapshot.
	RegistrationSeqs map[string]uint64
```

and in `newState`, after `Nodes: map[string]*NodeInfo{},` (line 916): `RegistrationSeqs: map[string]uint64{},`.

Append to `fsm/signed_claims.go` (Task 5's file; add `"crypto/ed25519"` and `"github.com/sentioxyz/arbiter-core/wire"` to its imports):

```go
// signedFieldsBeforeActivationReason refuses, before the signed-claims
// activation (spec 2026-10-10 §6.7), every command that carries a field only
// the activation enables. A voter built before those fields existed refuses
// such a command at decode (wire's strict decoder), so an upgraded voter must
// refuse it too, with no state change, instead of applying it. A leader
// proposes through wire.StripSignedClaims until the activation, so a healthy
// network never reaches this.
const signedFieldsBeforeActivationReason = "signed-claims fields are not accepted before the signed-claims activation"

// snodeSignatureLocked verifies jws as nodeID's signature over body: nodeID
// must be an si_indexers entry's snode_node_id, and jws must recover to that
// entry's signer under this network's message context.
func (f *FSM) snodeSignatureLocked(kind authority.SNodeMessageKind, nodeID string, body any, jws string) error {
	entry, ok := f.siIndexerBySNodeLocked(nodeID)
	if !ok {
		return fmt.Errorf("%s is not the snode_node_id of any si_indexers entry", nodeID)
	}
	ctx, err := f.messageContextLocked()
	if err != nil {
		return err
	}
	if err := authority.VerifySNodeMessage(kind, ctx, body, jws, entry.Signer); err != nil {
		return fmt.Errorf("snode signature: %w", err)
	}
	return nil
}

// verifierSignatureLocked verifies sigHex as verifier nodeID's signature over
// body. registered is the key the node registered with; it must be the key the
// verifiers list names for nodeID, so a verifier speaks only with its listed
// key.
func (f *FSM) verifierSignatureLocked(kind authority.VerifierMessageKind, nodeID string, registered []byte, body any, sigHex string) error {
	if !listsVerifier(f.st.Params.Verifiers, nodeID, registered) {
		return fmt.Errorf("(%s, ed25519_pubkey) is not a verifiers entry", nodeID)
	}
	ctx, err := f.messageContextLocked()
	if err != nil {
		return err
	}
	if err := authority.VerifyVerifierMessage(ed25519.PublicKey(registered), kind, ctx, body, sigHex); err != nil {
		return fmt.Errorf("verifier signature: %w", err)
	}
	return nil
}

// signedRegistrationLocked judges RegisterNode after the activation (spec
// 2026-10-10 §6.5, contract §3b.4). A registration carries exactly one
// data-plane role: without one it could replace an enrolled node's record
// unsigned. An SNODE signs with its entry's signer, a VERIFIER with its listed
// key. A registration_seq above the last one applied replaces the record. The
// last one applied again, with the identical canonical registration and a
// valid signature, is the dataplane client's WithLeaderRetry resending an
// identical request: idempotent is true and Apply changes nothing, so a
// running node is never pushed back to Syncing. Any other equal sequence, and
// every lower one, is refused. The signature is verified on both paths and
// never compared as bytes: every token carries a fresh iat.
func (f *FSM) signedRegistrationLocked(c *wire.RegisterNode) (reason string, idempotent bool) {
	r := c.Registration
	snode, verifier := hasRole(r.Roles, arbiter.NodeRoleSNode), hasRole(r.Roles, arbiter.NodeRoleVerifier)
	if len(r.Roles) != 1 || snode == verifier {
		return fmt.Sprintf("register node %s: after the signed-claims activation a registration carries exactly one of the SNODE and VERIFIER roles", r.NodeID), false
	}
	last := f.st.RegistrationSeqs[r.NodeID]
	switch {
	case r.RegistrationSeq < last:
		return fmt.Sprintf("register node %s: registration_seq %d is below the last applied %d", r.NodeID, r.RegistrationSeq, last), false
	case r.RegistrationSeq == last:
		n := f.st.Nodes[r.NodeID]
		if n == nil {
			return fmt.Sprintf("register node %s: registration_seq %d must exceed the last applied %d", r.NodeID, r.RegistrationSeq, last), false
		}
		if !sameRegistration(n.Registration, r) {
			return fmt.Sprintf("register node %s: registration_seq %d is the last applied one but the registration differs", r.NodeID, r.RegistrationSeq), false
		}
		idempotent = true
	}
	var err error
	if snode {
		err = f.snodeSignatureLocked(authority.SNodeMessageRegistration, r.NodeID, r, c.SignerJWS)
	} else {
		err = f.verifierSignatureLocked(authority.VerifierMessageRegistration, r.NodeID, r.Ed25519Pubkey, r, c.Ed25519Signature)
	}
	if err != nil {
		return fmt.Sprintf("register node %s: %v", r.NodeID, err), false
	}
	return "", idempotent
}

// sameRegistration compares two registrations as canonical values: a nil and
// an empty list or key are the same, as they are for the signed digest and
// after a snapshot round trip. It names every field of
// arbiter.NodeRegistration (TestSameRegistrationCoversEveryField).
func sameRegistration(a, b arbiter.NodeRegistration) bool {
	return a.NodeID == b.NodeID && slices.Equal(a.Roles, b.Roles) && bytes.Equal(a.Ed25519Pubkey, b.Ed25519Pubkey) &&
		a.DialAddr == b.DialAddr && a.RegistrationSeq == b.RegistrationSeq
}

// signedMarkActiveRefusalLocked is MarkActive's rule after the activation: the
// node's current registration_seq (0 for a node registered before the
// activation and not since), signed with its role's key.
func (f *FSM) signedMarkActiveRefusalLocked(c *wire.MarkActive, n *NodeInfo) string {
	if current := f.st.RegistrationSeqs[c.NodeID]; c.RegistrationSeq != current {
		return fmt.Sprintf("mark active %s: registration_seq %d is not the node's current %d", c.NodeID, c.RegistrationSeq, current)
	}
	body := authority.MarkActiveBody{NodeID: c.NodeID, RegistrationSeq: c.RegistrationSeq}
	roles := n.Registration.Roles
	snode, verifier := hasRole(roles, arbiter.NodeRoleSNode), hasRole(roles, arbiter.NodeRoleVerifier)
	var err error
	switch {
	case snode == verifier:
		return fmt.Sprintf("mark active %s: after the signed-claims activation only a node with exactly one data-plane role can be activated; re-register it", c.NodeID)
	case snode:
		err = f.snodeSignatureLocked(authority.SNodeMessageMarkActive, c.NodeID, body, c.SignerJWS)
	default:
		err = f.verifierSignatureLocked(authority.VerifierMessageMarkActive, c.NodeID, n.Registration.Ed25519Pubkey, body, c.Ed25519Signature)
	}
	if err != nil {
		return fmt.Sprintf("mark active %s: %v", c.NodeID, err)
	}
	return ""
}

// resultClaimRefusalLocked authenticates an RC after the activation, before it
// parks or binds: its source_node is an enrolled SNode and source_jws is that
// entry's signature over the RC.
func (f *FSM) resultClaimRefusalLocked(c *wire.RegisterRC) string {
	if err := f.snodeSignatureLocked(authority.SNodeMessageResultClaim, c.RC.SourceNode, c.RC, c.SourceJWS); err != nil {
		return "rc source " + err.Error()
	}
	return ""
}

// purgeReportRefusalLocked authenticates a purge report after the activation:
// an SNODE reporter signs with its entry's signer, a VERIFIER with its listed
// key.
func (f *FSM) purgeReportRefusalLocked(c *wire.RecordTablePurged, node *NodeInfo) string {
	body := authority.TablePurgedBody{NodeID: c.NodeID, IncarnationSeq: c.IncarnationSeq}
	roles := node.Registration.Roles
	snode, verifier := hasRole(roles, arbiter.NodeRoleSNode), hasRole(roles, arbiter.NodeRoleVerifier)
	var err error
	switch {
	case snode && verifier:
		return fmt.Sprintf("record table purged: %s holds both data-plane roles; re-register it with one", c.NodeID)
	case snode:
		err = f.snodeSignatureLocked(authority.SNodeMessageTablePurged, c.NodeID, body, c.SignerJWS)
	default:
		err = f.verifierSignatureLocked(authority.VerifierMessageTablePurged, c.NodeID, node.Registration.Ed25519Pubkey, body, c.Ed25519Signature)
	}
	if err != nil {
		return fmt.Sprintf("record table purged: %s: %v", c.NodeID, err)
	}
	return ""
}

// evictNodeRefusalLocked is EvictNode's rule after the activation (spec
// 2026-10-10 §6.5): the node's current registration_seq, and an authority
// token of the eviction family over exactly this command, bound to the
// current {network, genesis, epoch}. arbiter-core's VerifyEvictNode reads no
// clock, so it runs in Apply like VerifyConsensusParamsUpdate; the token age
// is decided by the EvictNode RPC and nowhere else.
func (f *FSM) evictNodeRefusalLocked(c *wire.EvictNode) string {
	if current := f.st.RegistrationSeqs[c.NodeID]; c.ExpectedRegistrationSeq != current {
		return fmt.Sprintf("evict node %s: expected_registration_seq %d is not the node's current %d", c.NodeID, c.ExpectedRegistrationSeq, current)
	}
	genesisID, err := f.genesisSnapshotIDLocked()
	if err != nil {
		return fmt.Sprintf("evict node %s: %v", c.NodeID, err)
	}
	validator := authority.Validator{AllowedAddresses: authorityAddressSet(f.st.Params.AuthorityAddresses)}
	ctx := authority.ConsensusContext{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: genesisID, AuthorityEpoch: f.st.ConsensusEpoch}
	if _, err := validator.VerifyEvictNode(c.Canonical(), c.AuthorityJWS, ctx); err != nil {
		return fmt.Sprintf("evict node %s: authority: %v", c.NodeID, err)
	}
	return ""
}
```

`fsm/apply.go` — `applyRegisterNode` (43-76) becomes:

```go
func (f *FSM) applyRegisterNode(c *wire.RegisterNode) any {
	r := c.Registration
	active := f.signedClaimsActiveLocked()
	if !active && (r.RegistrationSeq != 0 || c.SignerJWS != "" || c.Ed25519Signature != "") {
		return Rejected{Reason: signedFieldsBeforeActivationReason}
	}
	if r.NodeID == "" {
		return Rejected{Reason: "node_id required"}
	}
	if hasRole(r.Roles, arbiter.NodeRoleVerifier) && len(r.Ed25519Pubkey) != ed25519.PublicKeySize {
		return Rejected{Reason: "verifier registration requires a 32-byte ed25519 pubkey"}
	}
	// After the signed-claims activation a registration proves its identity
	// before any membership rule looks at it (spec 2026-10-10 §6.5).
	if active {
		reason, idempotent := f.signedRegistrationLocked(c)
		if reason != "" {
			return Rejected{Reason: reason}
		}
		if idempotent {
			// An identical resend (contract §3b.4): nothing changes, and a
			// running node is never pushed back to Syncing.
			return Applied{}
		}
	}
	// ... the writer gate (lines 51-65) and the pubkey uniqueness loop (66-70)
	// stay exactly as they are ...
	// Re-registration replaces the record and resets to Syncing: a node
	// must re-prove snapshot sync before re-entering selection pools.
	f.st.Nodes[r.NodeID] = &NodeInfo{Registration: r, Status: NodeSyncing}
	if active {
		f.st.RegistrationSeqs[r.NodeID] = r.RegistrationSeq
	}
	f.emit(Event{Kind: EventMembershipChanged})
	return Applied{}
}
```

`applyMarkActive` (78-89):

```go
func (f *FSM) applyMarkActive(c *wire.MarkActive) any {
	active := f.signedClaimsActiveLocked()
	if !active && (c.RegistrationSeq != 0 || c.SignerJWS != "" || c.Ed25519Signature != "") {
		return Rejected{Reason: signedFieldsBeforeActivationReason}
	}
	n, ok := f.st.Nodes[c.NodeID]
	if !ok {
		return Rejected{Reason: "unknown node"}
	}
	if n.Status == NodeEvicted {
		return Rejected{Reason: "node is evicted; re-register first"}
	}
	if active {
		if reason := f.signedMarkActiveRefusalLocked(c, n); reason != "" {
			return Rejected{Reason: reason}
		}
	}
	n.Status = NodeActive
	f.emit(Event{Kind: EventMembershipChanged})
	return Applied{}
}
```

`applyEvictNode` (91-107) keeps its body (status, reason, purge completion with its single registry bump, membership event); only the two checks are added:

```go
func (f *FSM) applyEvictNode(c *wire.EvictNode) any {
	active := f.signedClaimsActiveLocked()
	if !active && (c.ExpectedRegistrationSeq != 0 || c.AuthorityJWS != "") {
		return Rejected{Reason: signedFieldsBeforeActivationReason}
	}
	n, ok := f.st.Nodes[c.NodeID]
	if !ok {
		return Rejected{Reason: "unknown node"}
	}
	// After the activation an eviction is an authority command bound to the
	// node's current registration (spec 2026-10-10 §6.5).
	if active {
		if reason := f.evictNodeRefusalLocked(c); reason != "" {
			return Rejected{Reason: reason}
		}
	}
	// ... lines 96-106 unchanged ...
}
```

`applyRegisterRC` (354-405): insert as its first statements

```go
	active := f.signedClaimsActiveLocked()
	if !active && c.SourceJWS != "" {
		return Rejected{Reason: signedFieldsBeforeActivationReason}
	}
```

and directly after the client-lane refusal (375-377):

```go
	// After the activation an RC is authenticated before it may park or bind
	// (spec 2026-10-10 §6.5): a forged or replayed claim can no longer take a
	// statement's first-wins slot.
	if active {
		if reason := f.resultClaimRefusalLocked(c); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

`fsm/apply_table_registry.go` `applyRecordTablePurged` (229-250): insert as its first statements

```go
	active := f.signedClaimsActiveLocked()
	if !active && (c.SignerJWS != "" || c.Ed25519Signature != "") {
		return Rejected{Reason: signedFieldsBeforeActivationReason}
	}
```

and between the node check (238-241) and the idempotency check (242-244):

```go
	if active {
		if reason := f.purgeReportRefusalLocked(c, node); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

- [ ] **Step 4: Run the FSM suite**

Run: `cd "$W" && go test ./fsm/ -run 'SignedFieldsAreRefused|SNodeRegistrationAfterActivation|BindNetworkGenesisAndKind|MarkActiveNeedsTheCurrent|VerifierRegistrationAfterActivation|SameRegistrationCoversEveryField|ResultClaimsAreVerified|PurgeReportsAreSigned|EvictNodeAfterActivation' -v && bazel test //fsm:all && git diff --exit-code -- fsm/testdata`
Expected: all PASS, including the untouched `TestMembershipLifecycle`, `TestMembershipRejections`, `TestRegisterNode_*` (`fsm/membership_test.go`), `TestRecordTablePurged`, `TestEvictionCompletesPurge`, `TestPromotionIssued_AuditVerification`, Task 5–7's `TestSignedClaimsFirstSet` and `TestDeterminism_SourceBindingAcrossActivation`, and both frozen goldens, `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims` and `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` (a history without the new fields replays to the same bytes); no file under `fsm/testdata` changes.

- [ ] **Step 5: Commit**

```bash
cd "$W" && git add fsm && git commit -m "feat(fsm): signed SNode and verifier messages, registration sequences, authority eviction

After the signed-claims activation RegisterNode, MarkActive, RegisterRC
(parked or bound) and RecordTablePurged must carry the owning entry's
SNode signature or the listed verifier key's signature over a
network- and genesis-bound digest, registrations must raise a per-node
registration_seq (an identical resend is an idempotent no-op), and
EvictNode needs an arbiter-evict-node-v1 authority
token bound to the current context and the node's current sequence.
Before the activation every new field is refused like an older voter's
decoder refuses it (housegate spec 2026-10-10 §6.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Write the failing server tests**

Create `server/signed_messages_test.go` (reuses `fakeNode`, `newServerTestFSM`, `publishServerGenesis`, `serverTestAuthority` from `server/server_test.go`, `mustDirectApply` and `testServerNetworkID` from `server/ingress_test.go`, `enableRegistryWithPendingAdd` from `server/table_set_transition_test.go:59`, `verifierRequest` from `server/features_test.go:55`):

```go
package server

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
)

// recordingNode is fakeNode that also keeps every proposal as the decoded
// RaftCommand, so a test can assert which proto fields reached the log.
// Setup through mustDirectApply(t, rec.fakeNode, ...) is not recorded.
type recordingNode struct {
	*fakeNode
	mu       sync.Mutex
	proposed []*pb.RaftCommand
}

func (n *recordingNode) Apply(cmd []byte, timeout time.Duration) raft.ApplyFuture {
	var decoded pb.RaftCommand
	if err := proto.Unmarshal(cmd, &decoded); err == nil {
		n.mu.Lock()
		n.proposed = append(n.proposed, &decoded)
		n.mu.Unlock()
	}
	return n.fakeNode.Apply(cmd, timeout)
}

func (n *recordingNode) proposals() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.proposed)
}

func newRecordingNode(t *testing.T) *recordingNode {
	t.Helper()
	node := &fakeNode{f: newServerTestFSM(t), leader: true}
	publishServerGenesis(t, node)
	return &recordingNode{fakeNode: node}
}

// lastProposalIs requires the latest proposal to equal cmd's encoding.
// proto.Equal sees every set field, so a signed-claims field that leaked into
// a pre-activation proposal fails here, not only where the FSM refuses it.
func lastProposalIs(t *testing.T, n *recordingNode, cmd wire.Command) {
	t.Helper()
	encoded, err := wire.Encode(cmd)
	if err != nil {
		t.Fatal(err)
	}
	var want pb.RaftCommand
	if err := proto.Unmarshal(encoded, &want); err != nil {
		t.Fatal(err)
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.proposed) == 0 {
		t.Fatal("nothing was proposed")
	}
	if got := n.proposed[len(n.proposed)-1]; !proto.Equal(got, &want) {
		t.Fatalf("proposed %v\nwant     %v", got, &want)
	}
}

func serverVerifierKey(seed byte) ed25519.PrivateKey {
	s := make([]byte, ed25519.SeedSize)
	s[0] = seed
	return ed25519.NewKeyFromSeed(s)
}

func serverMessageContext(t *testing.T, f *fsm.FSM) authority.MessageContext {
	t.Helper()
	view, err := f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	return authority.MessageContext{NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID}
}

func serverAuthorityContext(t *testing.T, f *fsm.FSM) authority.ConsensusContext {
	t.Helper()
	view, err := f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	return authority.ConsensusContext{NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID, AuthorityEpoch: view.Epoch}
}

func serverEnrolledEntry(t *testing.T, f *fsm.FSM, signer *authority.Signer, indexer, activation uint64, node string) arbiter.SIIndexerEntry {
	t.Helper()
	ctx := serverMessageContext(t, f)
	jws, err := signer.SignSNodeEnrollment(authority.SNodeEnrollmentStatement{
		NetworkID: ctx.NetworkID, GenesisSnapshotID: ctx.GenesisSnapshotID, IndexerID: indexer, SNodeNodeID: node})
	if err != nil {
		t.Fatal(err)
	}
	return arbiter.SIIndexerEntry{IndexerID: indexer, ActivationBlock: activation, Signer: signer.Address(), SNodeNodeID: node, EnrollmentJWS: jws}
}

func serverRegistryParams() *arbiter.TableRegistryParams {
	return &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1",
		SIIndexerID: 1, ActivationBlock: 100, Confirmation: arbiter.TableRegistryConfirmationSafe}
}

// applyServerUpdate commits a consensus update built from node's current view
// with serverRegistryParams and max_writers 1, edited by mutate and signed by
// serverTestAuthority, bypassing the admin handler.
func applyServerUpdate(t *testing.T, node *fakeNode, mutate func(*arbiter.ConsensusParamsUpdate)) {
	t.Helper()
	view, err := node.f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	u := arbiter.ConsensusParamsUpdate{
		NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID, ExpectedEpoch: view.Epoch,
		PreviousParamsDigest: view.ParamsDigest, AuthorityAddresses: view.Current.AuthorityAddresses, MaxWriters: 1,
		ExpectedPromotionSeq: view.PromotionSeq, TableRegistry: serverRegistryParams(),
	}
	mutate(&u)
	token, err := serverTestAuthority.SignConsensusParamsUpdateAt(u, 1)
	if err != nil {
		t.Fatal(err)
	}
	mustDirectApply(t, node, wire.Command{UpdateConsensusParams: &wire.UpdateConsensusParams{Update: u, AuthorityJWS: token}})
}

func enableServerTableRegistry(t *testing.T, node *fakeNode) {
	t.Helper()
	applyServerUpdate(t, node, func(*arbiter.ConsensusParamsUpdate) {})
}

func seedServerRegistry(t *testing.T, node *fakeNode) {
	t.Helper()
	mustDirectApply(t, node, wire.Command{SeedLegacyTables: &wire.SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 99, Hash: "0x99"}}})
}

// activateServerSignedClaims registers s1 (SNODE) and v1..v3 (VERIFIER, keys
// serverVerifierKey(1..3)) through the pre-activation path, enables and seeds
// the registry, and commits the signed-claims activation naming the founding
// indexer 1 (SNode s1, signed by founder) and v1..v3, all bypassing the
// gateway.
func activateServerSignedClaims(t *testing.T, node *fakeNode, founder *authority.Signer) {
	t.Helper()
	mustDirectApply(t, node, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}})
	mustDirectApply(t, node, wire.Command{MarkActive: &wire.MarkActive{NodeID: "s1"}})
	var verifiers []arbiter.VerifierEntry
	for i, id := range []string{"v1", "v2", "v3"} {
		pub := serverVerifierKey(byte(i + 1)).Public().(ed25519.PublicKey)
		mustDirectApply(t, node, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
			NodeID: id, Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: pub}}})
		mustDirectApply(t, node, wire.Command{MarkActive: &wire.MarkActive{NodeID: id}})
		verifiers = append(verifiers, arbiter.VerifierEntry{NodeID: id, Ed25519Pubkey: pub})
	}
	enableServerTableRegistry(t, node)
	seedServerRegistry(t, node)
	applyServerUpdate(t, node, func(u *arbiter.ConsensusParamsUpdate) {
		u.SIIndexers = []arbiter.SIIndexerEntry{serverEnrolledEntry(t, node.f, founder, 1, 100, "s1")}
		u.Verifiers = verifiers
	})
	if !node.f.SignedClaimsActive() {
		t.Fatal("signed claims not active")
	}
}

// serverPurgingIncarnation adds db.p (a declared Int64 table created at block
// 100, schema at 101, owned by owner, which must be nil before the activation)
// to a seeded registry and retires it at block 110, which takes a Pending
// incarnation without a sealed add transition straight to Purging. It returns
// the incarnation's seq.
func serverPurgingIncarnation(t *testing.T, node *fakeNode, owner *uint64) uint64 {
	t.Helper()
	schema := payloadexec.TableSchema{TableID: "db.p", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}}
	declared, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	at := func(block uint64) arbiter.L2EventRef {
		return arbiter.L2EventRef{BlockNumber: block, BlockHash: fmt.Sprintf("0x%d", block), TxHash: "0xtx"}
	}
	mustDirectApply(t, node, wire.Command{AddTable: &wire.AddTable{DatabaseID: "db", TableID: "p", Created: at(100), Schema: at(101),
		SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash(testServerNetworkID, schema), SchemaJSON: string(declared), OwnerIndexerID: owner}})
	mustDirectApply(t, node, wire.Command{RetireTables: &wire.RetireTables{DatabaseID: "db", TableIDs: []string{"p"}, Deleted: at(110),
		Reason: wire.TableRetireReasonTableDeleted}})
	view, ok := node.f.TableRegistryView()
	inc := view.Live("db.p")
	if !ok || inc == nil || inc.Status != fsm.TableStatusPurging {
		t.Fatalf("db.p = %+v, want Purging", inc)
	}
	return inc.Seq
}

func evictRequest(t *testing.T, f *fsm.FSM, signer *authority.Signer, cmd arbiter.EvictNodeCommand) *pb.EvictNodeRequest {
	t.Helper()
	token, err := signer.SignEvictNodeWithContext(cmd, serverAuthorityContext(t, f))
	if err != nil {
		t.Fatal(err)
	}
	return &pb.EvictNodeRequest{NodeId: cmd.NodeID, ExpectedRegistrationSeq: cmd.ExpectedRegistrationSeq, Reason: cmd.Reason, AuthorityJws: token}
}

func evictionAdmin(rec *recordingNode, maxAge time.Duration) *consensusAdminService {
	return &consensusAdminService{s: New(Deps{Node: rec, FSM: rec.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second,
		ConsensusUpdatesEnabled: true, ConsensusAdminMaxTokenAge: maxAge}})}
}

// TestServerDropsSignedFieldsBeforeActivation: an upgraded node signs from its
// first start, but until the activation commits the leader proposes today's
// command bytes exactly (contract §0), and the FSM applies them.
func TestServerDropsSignedFieldsBeforeActivation(t *testing.T) {
	rec := newRecordingNode(t)
	enableRegistryWithPendingAdd(t, rec.f, rec.fakeNode)
	mustDirectApply(t, rec.fakeNode, wire.Command{RetireTables: &wire.RetireTables{DatabaseID: "db", TableIDs: []string{"n"},
		Deleted: arbiter.L2EventRef{BlockNumber: 110, BlockHash: "0x110", TxHash: "0xtx"}, Reason: wire.TableRetireReasonTableDeleted}})
	view, _ := rec.f.TableRegistryView()
	purging := view.Live("db.n").Seq
	s := New(Deps{Node: rec, FSM: rec.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second, MaxStatementAge: time.Hour}})
	ctx := context.Background()
	m := &membershipService{s: s}
	if _, err := m.RegisterNode(ctx, &pb.NodeRegistration{NodeId: "s1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE},
		Features: arbiter.LocalNodeFeatures(), RegistrationSeq: 1760000000000, SignerJws: "h.p.s"}); err != nil {
		t.Fatalf("RegisterNode = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}})
	if _, err := m.MarkActive(ctx, &pb.NodeRef{NodeId: "s1", RegistrationSeq: 1760000000000, SignerJws: "h.p.s"}); err != nil {
		t.Fatalf("MarkActive = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{MarkActive: &wire.MarkActive{NodeID: "s1"}})
	rc := &pb.RCRecord{StatementId: &pb.StatementID{ClientAccount: "0xaa", ClientSeq: 1, ClientNonce: "n"}, SourceNode: "s1", SourceJws: "h.p.s"}
	_, _ = (&claimsService{s: s}).RegisterResultClaim(ctx, rc) // the FSM's answer is not the point here
	lastProposalIs(t, rec, wire.Command{RegisterRC: &wire.RegisterRC{RC: wire.RCFromPB(rc)}})
	gw := &promotionGatewayService{s: s}
	_, _ = gw.AckPromotion(ctx, &pb.PromotionAck{NodeId: "s1", PromotionSeq: 9, SourceJws: "h.p.s"}) // no promotion 9: refused
	lastProposalIs(t, rec, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 9}}})
	if _, err := gw.AckCleanup(ctx, &pb.CleanupAck{NodeId: "s1", PromotionSeq: 9, SourceJws: "h.p.s"}); err != nil {
		t.Fatalf("AckCleanup = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 9}}})
	if _, err := gw.SubmitTablePurged(ctx, &pb.RecordTablePurgedCmd{NodeId: "s1", IncarnationSeq: purging, SignerJws: "h.p.s", Ed25519Signature: "00"}); err != nil {
		t.Fatalf("SubmitTablePurged = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: "s1", IncarnationSeq: purging}})
}

// TestServerForwardsSignedFieldsAfterActivation: after the activation the
// leader moves every signature and sequence into the command, and the FSM
// judges them.
func TestServerForwardsSignedFieldsAfterActivation(t *testing.T) {
	rec := newRecordingNode(t)
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	activateServerSignedClaims(t, rec.fakeNode, founder)
	s := New(Deps{Node: rec, FSM: rec.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second, MaxStatementAge: time.Hour}})
	ctx := context.Background()
	mctx := serverMessageContext(t, rec.f)
	m := &membershipService{s: s}
	req := wire.RegistrationToPB(arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 1760000000000})
	jws, err := founder.SignSNodeMessage(authority.SNodeMessageRegistration, mctx, wire.RegistrationFromPB(req))
	if err != nil {
		t.Fatal(err)
	}
	req.SignerJws, req.Features = jws, arbiter.LocalNodeFeatures()
	if _, err := m.RegisterNode(ctx, req); err != nil {
		t.Fatalf("signed registration = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{RegisterNode: &wire.RegisterNode{Registration: wire.RegistrationFromPB(req), SignerJWS: jws}})
	markJWS, err := founder.SignSNodeMessage(authority.SNodeMessageMarkActive, mctx, authority.MarkActiveBody{NodeID: "s1", RegistrationSeq: 1760000000000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.MarkActive(ctx, &pb.NodeRef{NodeId: "s1", RegistrationSeq: 1760000000000, SignerJws: markJWS}); err != nil {
		t.Fatalf("signed MarkActive = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{MarkActive: &wire.MarkActive{NodeID: "s1", RegistrationSeq: 1760000000000, SignerJWS: markJWS}})
	rc := &pb.RCRecord{StatementId: &pb.StatementID{ClientAccount: "0xaa", ClientSeq: 1, ClientNonce: "n"}, SourceNode: "s1", SourceJws: "h.p.s"}
	if _, err := (&claimsService{s: s}).RegisterResultClaim(ctx, rc); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "rc source snode signature") {
		t.Fatalf("a badly signed RC after activation = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{RegisterRC: &wire.RegisterRC{RC: wire.RCFromPB(rc), SourceJWS: "h.p.s"}})
	gw := &promotionGatewayService{s: s}
	_, _ = gw.AckPromotion(ctx, &pb.PromotionAck{NodeId: "s1", PromotionSeq: 9, SourceJws: "h.p.s"})
	lastProposalIs(t, rec, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 9}, SourceJWS: "h.p.s"}})
	if _, err := gw.AckCleanup(ctx, &pb.CleanupAck{NodeId: "s1", PromotionSeq: 9, SourceJws: "h.p.s"}); err != nil {
		t.Fatalf("AckCleanup of an unknown cleanup stays idempotent = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 9}, SourceJWS: "h.p.s"}})
	owner := uint64(1)
	seq := serverPurgingIncarnation(t, rec.fakeNode, &owner)
	sig, err := authority.SignVerifierMessage(serverVerifierKey(1), authority.VerifierMessageTablePurged, mctx,
		authority.TablePurgedBody{NodeID: "v1", IncarnationSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gw.SubmitTablePurged(ctx, &pb.RecordTablePurgedCmd{NodeId: "v1", IncarnationSeq: seq, Ed25519Signature: sig}); err != nil {
		t.Fatalf("signed purge report = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{RecordTablePurged: &wire.RecordTablePurged{NodeID: "v1", IncarnationSeq: seq, Ed25519Signature: sig}})
}

// TestRegistrationThatCannotSignIsRefusedAfterActivation: a data-plane binary
// that does not advertise signed_claims_v1 is refused before proposing.
func TestRegistrationThatCannotSignIsRefusedAfterActivation(t *testing.T) {
	rec := newRecordingNode(t)
	activateServerSignedClaims(t, rec.fakeNode, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0))
	s := New(Deps{Node: rec, FSM: rec.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second}})
	_, err := (&membershipService{s: s}).RegisterNode(context.Background(), verifierRequest("v1", 1, arbiter.ClientLanesFeature))
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "signed claims are active; this binary does not sign its messages" {
		t.Fatalf("err = %v", err)
	}
	if rec.proposals() != 0 {
		t.Fatal("the refused registration was proposed")
	}
}

// TestEvictNodeRPCIsRefusedBeforeActivation: contract §3a. The legacy
// unauthenticated EvictNode Apply path stays unreachable from any RPC.
func TestEvictNodeRPCIsRefusedBeforeActivation(t *testing.T) {
	rec := newRecordingNode(t)
	mustDirectApply(t, rec.fakeNode, wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{
		NodeID: "v9", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: serverVerifierKey(9).Public().(ed25519.PublicKey)}}})
	req := evictRequest(t, rec.f, serverTestAuthority, arbiter.EvictNodeCommand{NodeID: "v9", Reason: "host retired"})
	_, err := evictionAdmin(rec, 5*time.Minute).EvictNode(context.Background(), req)
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "eviction requires the signed-claims activation" {
		t.Fatalf("err = %v", err)
	}
	if rec.proposals() != 0 || !slices.Contains(rec.f.PurgeNodeSet(), "v9") {
		t.Fatal("a pre-activation eviction was proposed")
	}
}

// TestEvictNodeRPCAfterActivation: refusals before proposing, then the full
// command; the registration sequence is the FSM's decision.
func TestEvictNodeRPCAfterActivation(t *testing.T) {
	rec := newRecordingNode(t)
	activateServerSignedClaims(t, rec.fakeNode, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0))
	admin := evictionAdmin(rec, 5*time.Minute)
	ctx := context.Background()
	cmd := arbiter.EvictNodeCommand{NodeID: "v3", Reason: "rotated"}
	if _, err := admin.EvictNode(ctx, evictRequest(t, rec.f, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), cmd)); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a non-authority key = %v", err)
	}
	other := evictRequest(t, rec.f, serverTestAuthority, arbiter.EvictNodeCommand{NodeID: "v3", Reason: "another reason"})
	other.Reason = cmd.Reason
	if _, err := admin.EvictNode(ctx, other); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("a token for another command = %v", err)
	}
	if _, err := admin.EvictNode(ctx, &pb.EvictNodeRequest{NodeId: "v3", Reason: "rotated", AuthorityJws: "not-a-jws"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("a malformed token = %v", err)
	}
	if _, err := admin.EvictNode(ctx, &pb.EvictNodeRequest{NodeId: "v3", AuthorityJws: "a.b.c"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("an empty reason = %v", err)
	}
	if rec.proposals() != 0 {
		t.Fatal("a refused eviction was proposed")
	}
	req := evictRequest(t, rec.f, serverTestAuthority, cmd)
	if _, err := admin.EvictNode(ctx, req); err != nil {
		t.Fatalf("authority eviction = %v", err)
	}
	lastProposalIs(t, rec, wire.Command{EvictNode: &wire.EvictNode{NodeID: "v3", Reason: "rotated", AuthorityJWS: req.GetAuthorityJws()}})
	if slices.Contains(rec.f.PurgeNodeSet(), "v3") {
		t.Fatal("v3 was not evicted")
	}
	stale := evictRequest(t, rec.f, serverTestAuthority, arbiter.EvictNodeCommand{NodeID: "v2", ExpectedRegistrationSeq: 4, Reason: "rotated"})
	if _, err := admin.EvictNode(ctx, stale); status.Code(err) != codes.InvalidArgument || !strings.Contains(err.Error(), "expected_registration_seq 4 is not the node's current 0") {
		t.Fatalf("stale expected sequence = %v", err)
	}
}

// TestEvictNodeRPCNeedsUpdatesEnabledAndATokenAge: the admission switch and
// the fail-closed token age both apply to eviction.
func TestEvictNodeRPCNeedsUpdatesEnabledAndATokenAge(t *testing.T) {
	rec := newRecordingNode(t)
	activateServerSignedClaims(t, rec.fakeNode, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0))
	req := evictRequest(t, rec.f, serverTestAuthority, arbiter.EvictNodeCommand{NodeID: "v3", Reason: "rotated"})
	disabled := &consensusAdminService{s: New(Deps{Node: rec, FSM: rec.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second}})}
	if _, err := disabled.EvictNode(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("updates disabled = %v", err)
	}
	if _, err := evictionAdmin(rec, 0).EvictNode(context.Background(), req); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("non-positive max token age = %v", err)
	}
	if rec.proposals() != 0 {
		t.Fatal("a refused eviction was proposed")
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && go test ./server/ -run 'SignedFields|ForwardsSignedFields|CannotSign|EvictNodeRPC' -v`
Expected: FAIL — the pre-activation registration, MarkActive and purge proposals carry the new fields (the handlers still propose what the converters decoded, and the FSM refuses them), and `EvictNode` answers `Unimplemented`.

- [ ] **Step 7: Implement the server handlers**

Create `server/signed_messages.go`:

```go
package server

import (
	"context"
	"strings"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// signedClaimsCommand is cmd as this leader may propose it (housegate spec
// 2026-10-10 §6.5, §6.7). Before the signed-claims activation every stage-1
// field is dropped (wire.StripSignedClaims): a voter that predates them
// refuses any command carrying one at decode, so until the activation the
// leader proposes exactly what the previous release proposed, whatever an
// upgraded node sent. Afterwards cmd is proposed as received and Apply judges
// the signatures. The view is this leader's applied state: the activation is
// irreversible and applied implies committed, so a true answer never goes
// stale, and a false one that lags the activation's own commit only makes the
// FSM refuse the stripped command as unsigned, and the node retries.
func (s *Server) signedClaimsCommand(cmd wire.Command) wire.Command {
	if s.d.FSM.SignedClaimsActive() {
		return cmd
	}
	return wire.StripSignedClaims(cmd)
}

// EvictNode evicts one data-plane node on an authority-signed request (spec
// 2026-10-10 §6.5). It exists only after the signed-claims activation: before
// it the command could carry neither the token nor the expected sequence
// (strict decoder), and an unauthenticated eviction RPC does not exist
// (contract §3a). Like ActivateQueryProfile, nothing on this path writes to
// the log before the checks: it verifies leadership rather than taking a
// barrier, decides the token age here because Apply may not read the clock,
// and checks the token against this leader's committed authority set and
// context. Apply re-verifies the token and the node's registration sequence
// atomically.
func (svc *consensusAdminService) EvictNode(ctx context.Context, req *pb.EvictNodeRequest) (*pb.Ack, error) {
	if !svc.s.d.Cfg.ConsensusUpdatesEnabled {
		return nil, status.Error(codes.FailedPrecondition, "consensus parameter updates are disabled; verify every voter's protocol capability before setting consensus_updates_enabled: true")
	}
	if !svc.s.d.FSM.SignedClaimsActive() {
		return nil, status.Error(codes.FailedPrecondition, "eviction requires the signed-claims activation")
	}
	evict := wire.EvictNodeFromRequest(req)
	switch {
	case evict.NodeID == "" || strings.TrimSpace(evict.NodeID) != evict.NodeID:
		return nil, status.Error(codes.InvalidArgument, "node_id is required and must not carry surrounding whitespace")
	case strings.TrimSpace(evict.Reason) == "":
		return nil, status.Error(codes.InvalidArgument, "reason is required")
	case len(strings.Split(evict.AuthorityJWS, ".")) != 3:
		// A token that is not a JWS is a malformed request, not a failed
		// authorization.
		return nil, status.Error(codes.InvalidArgument, "authority_jws must be a JWS compact serialization with 3 dot-separated parts")
	}
	if err := svc.s.consensusLeaderCheck(ctx); err != nil {
		return nil, err
	}
	view, err := svc.s.d.FSM.ConsensusParamsView()
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "consensus parameters unavailable: %v", err)
	}
	validator := authority.Validator{AllowedAddresses: authorityAddressSet(view.Current.AuthorityAddresses), MaxTokenAge: svc.s.d.Cfg.ConsensusAdminMaxTokenAge}
	consensus := authority.ConsensusContext{NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID, AuthorityEpoch: view.Epoch}
	if _, err := validator.AuthorizeEvictNode(evict.Canonical(), evict.AuthorityJWS, consensus); err != nil {
		// The validator names the recovered address or the failed check and
		// never the token itself; keep it that way.
		return nil, status.Errorf(codes.PermissionDenied, "node eviction authority: %v", err)
	}
	if _, err := svc.s.propose(ctx, wire.Command{EvictNode: &evict}); err != nil {
		return nil, err // propose maps fsm.Rejected to InvalidArgument with the FSM reason
	}
	return &pb.Ack{}, nil
}
```

`server/membership.go` (add `"slices"` and `"github.com/sentioxyz/arbiter-core"` to its imports) — in `RegisterNode`, after the client-lanes refusal (lines 26-28):

```go
	// After the signed-claims activation (housegate spec 2026-10-10 §6.5) the
	// FSM refuses an unsigned registration; refusing a binary that cannot sign
	// here costs no log entry per retry. Read from applied state like the
	// lanes refusal above, so it cannot diverge.
	if isDataPlaneRegistration(req) && svc.s.d.FSM.SignedClaimsActive() && !slices.Contains(req.GetFeatures(), arbiter.SignedClaimsFeature) {
		return nil, status.Error(codes.FailedPrecondition, "signed claims are active; this binary does not sign its messages")
	}
```

and replace the proposal (lines 41-43) with:

```go
	register := wire.RegisterNodeFromRequest(req)
	_, p, err := svc.s.proposeTracked(ctx, svc.s.signedClaimsCommand(wire.Command{RegisterNode: &register}))
```

`MarkActive` (54-62):

```go
func (svc *membershipService) MarkActive(ctx context.Context, req *pb.NodeRef) (*pb.Ack, error) {
	mark := wire.MarkActiveFromRequest(req)
	if _, err := svc.s.propose(ctx, svc.s.signedClaimsCommand(wire.Command{MarkActive: &mark})); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}
```

`server/claims.go` `RegisterResultClaim` (20-49): replace `rc := wire.RCFromPB(req)` (line 21) with

```go
	register := wire.RegisterRCFromRequest(req)
	rc := register.RC
```

and the proposal (lines 39-41) with `res, err := svc.s.propose(ctx, svc.s.signedClaimsCommand(wire.Command{RegisterRC: &register}))`.

`server/gateway.go` — `AckPromotion` (112-123) and `AckCleanup` (132-143) propose:

```go
	record := wire.RecordPromotionAckFromRequest(ack)
	res, err := svc.s.propose(ctx, svc.s.signedClaimsCommand(wire.Command{RecordPromotionAck: &record}))
```

```go
	record := wire.RecordCleanupAckFromRequest(ack)
	res, err := svc.s.propose(ctx, svc.s.signedClaimsCommand(wire.Command{RecordCleanupAck: &record}))
```

`server/table_purge.go` `SubmitTablePurged` (23-56): the request message is the Raft command itself (§3a), so replace the proposal (lines 39-41) with

```go
	report := wire.RecordTablePurgedFromRequest(req)
	res, err := svc.s.propose(ctx, svc.s.signedClaimsCommand(wire.Command{RecordTablePurged: &report}))
```

- [ ] **Step 8: Run the tests**

Run: `cd "$W" && bazel test //server:all //fsm:all //cmd/arbiter:all //cmd/arbiter-admin:all`
Expected: all PASS, including `TestRegisterNodeRecordsFeaturesOutsideReplicatedState`, `TestRegistrationWithoutLaneSupportIsRefusedAfterActivation`, the feature-book tests in `server/features_test.go`, `TestTableRegistryMirrorsProto`, Task 5's `TestConsensusMutableParamsCarrySignedClaims` and the existing `SubmitTablePurged` tests.

- [ ] **Step 9: Commit**

```bash
cd "$W" && git add server && git commit -m "feat(server): strip signed-claims fields before activation, authority EvictNode RPC

Every data-plane handler decodes its request with the arbiter-core
request converter and proposes through wire.StripSignedClaims until the
leader has applied the signed-claims activation (strict decoder);
afterwards signatures and sequences reach the command and a data-plane
registration that cannot sign is refused. ConsensusAdmin.EvictNode is
refused before the activation and afterwards checks shape, leadership
and the authority token with its age before proposing (housegate spec
2026-10-10 §6.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 10: Write the failing verifier-host test**

A verifier now signs RegisterNode, MarkActive and purge reports and keeps its `registration_seq` in `<StateDir>/registration.json` (Task 4); the host must expose the directory (contract §3b.9). Append to `cmd/arbiter-verifier/main_test.go` (reuses `writeVerifierConfig`, `validVerifierConfig`, `schemaSourceFor`):

```go
// TestToRoleConfig_CarriesStateDirAndGenesisSnapshotID: both optional fields
// reach verifier.Config unchanged, and unset they stay empty.
func TestToRoleConfig_CarriesStateDirAndGenesisSnapshotID(t *testing.T) {
	stateDir := filepath.ToSlash(t.TempDir())
	body := strings.TrimRight(validVerifierConfig(t), "\n") + "\nstate_dir: \"" + stateDir + "\"\ngenesis_snapshot_id: \"0xgenesis\"\n"
	cfg, err := loadConfig(writeVerifierConfig(t, body))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if role := cfg.toRoleConfig(cfg.tables(), schemaSourceFor(cfg)); role.StateDir != stateDir || role.GenesisSnapshotID != "0xgenesis" {
		t.Fatalf("role config = %+v", role)
	}
	plain, err := loadConfig(writeVerifierConfig(t, validVerifierConfig(t)))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if role := plain.toRoleConfig(plain.tables(), schemaSourceFor(plain)); role.StateDir != "" || role.GenesisSnapshotID != "" {
		t.Fatalf("unset fields must stay empty: %+v", role)
	}
}
```

Run: `cd "$W" && go test ./cmd/arbiter-verifier/ -run TestToRoleConfig_CarriesStateDirAndGenesisSnapshotID -v`
Expected: FAIL (`role config = …` with an empty `StateDir`: `loadConfig` ignores the unknown keys).

- [ ] **Step 11: Wire the fields**

`cmd/arbiter-verifier/config.go` — in `Config`, after `PayloadDir` (line 36):

```go
	// StateDir holds the verifier's registration.json, its last
	// registration_seq (arbiter-core verifier.Config.StateDir). Production
	// verifiers set it to a persistent path; empty keeps the sequence in
	// memory, still clock-floored, so after a restart that follows a
	// backwards clock step the arbiter refuses registrations until the clock
	// passes the last applied sequence.
	StateDir string `yaml:"state_dir"`
	// GenesisSnapshotID is the genesis snapshot id every signed message
	// binds. Empty derives it from the genesis tables, which a verifier always
	// holds; set it only to pin the expected value.
	GenesisSnapshotID string `yaml:"genesis_snapshot_id"`
```

and in `toRoleConfig` (232-241) add `StateDir: c.StateDir, GenesisSnapshotID: c.GenesisSnapshotID,` to the `verifier.Config` literal. `cmd/arbiter-verifier/main.go` — in `run`, before `verifier.New` (line 213):

```go
	if cfg.StateDir == "" {
		logger.Warn("state_dir is not set; the registration sequence is kept in memory",
			"remedy", "set state_dir to a persistent path (it holds registration.json)")
	}
```

- [ ] **Step 12: Run and commit**

Run: `cd "$W" && bazel test //cmd/arbiter-verifier:all`
Expected: all PASS, including `TestVerifierSampleConfig_LoadsForSchemaRoot` (the sample config sets neither field).

```bash
cd "$W" && git add cmd/arbiter-verifier && git commit -m "feat(arbiter-verifier): state_dir and genesis_snapshot_id for the signing verifier

The verifier keeps its registration_seq in <state_dir>/registration.json
(arbiter-core verifier.Config.StateDir); the host exposes it and the
optional genesis snapshot id, and warns when state_dir is unset (housegate
spec 2026-10-10 §6.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 13: Write the failing arbiter-snode host test**

The standalone SNode binary (local configs, the chpipeline integration suite) needs the claim-signer inputs too, or it cannot register after the activation (contract §3c). Append to `cmd/arbiter-snode/main_test.go` (reuses `writeSNodeConfig`, `validSNodeConfig`, `schemaSourceFor`; add the import `github.com/sentioxyz/arbiter-core/authority/authoritytest`):

```go
// TestClaimSignerAndRoleConfig: the claim key comes only from
// ARBITER_SNODE_CLAIM_KEY_HEX; with it indexer_id is required (0, devnet2's
// founding indexer, is a valid value) and the identity fields reach
// snode.Config; without it the SNode stays legacy unsigned.
func TestClaimSignerAndRoleConfig(t *testing.T) {
	withIdentity := func(indexer string) Config {
		t.Helper()
		body := strings.TrimRight(validSNodeConfig(t), "\n") + "\nindexer_id: " + indexer + "\ngenesis_snapshot_id: \"0xgenesis\"\n"
		cfg, err := loadConfig(writeSNodeConfig(t, body))
		if err != nil {
			t.Fatalf("loadConfig: %v", err)
		}
		return cfg
	}
	cfg := withIdentity("1")
	t.Setenv(envSNodeClaimKeyHex, "")
	if signer, err := cfg.claimSigner(); signer != nil || err != nil {
		t.Fatalf("without the key: signer %v, err %v; want a legacy unsigned SNode", signer, err)
	}
	t.Setenv(envSNodeClaimKeyHex, authoritytest.IndexerKeyHex1)
	signer, err := cfg.claimSigner()
	if err != nil || signer == nil || signer.Address() != authoritytest.IndexerAddr1 {
		t.Fatalf("signer = %v, %v", signer, err)
	}
	if role := cfg.toRoleConfig(cfg.tables(), schemaSourceFor(cfg)); role.IndexerID != 1 || role.GenesisSnapshotID != "0xgenesis" {
		t.Fatalf("role config = %+v", role)
	}
	if _, err := withIdentity("0").claimSigner(); err != nil {
		t.Fatalf("indexer_id 0 is a valid indexer: %v", err)
	}
	plain, err := loadConfig(writeSNodeConfig(t, validSNodeConfig(t)))
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if _, err := plain.claimSigner(); err == nil || !strings.Contains(err.Error(), "indexer_id is required") {
		t.Fatalf("a claim key without indexer_id = %v", err)
	}
	t.Setenv(envSNodeClaimKeyHex, "not-a-key")
	if _, err := cfg.claimSigner(); err == nil || strings.Contains(err.Error(), "not-a-key") {
		t.Fatalf("an invalid key must be refused without echoing it: %v", err)
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && go test ./cmd/arbiter-snode/ -run TestClaimSignerAndRoleConfig -v`
Expected: build failure (`undefined: envSNodeClaimKeyHex`, `cfg.claimSigner undefined`).

- [ ] **Step 14: Wire the claim signer**

`cmd/arbiter-snode/config.go` (add the import `github.com/sentioxyz/arbiter-core/authority`) — above `Config`:

```go
// envSNodeClaimKeyHex holds the indexer's IndexerRegistry signer key (hex
// secp256k1; its address is this indexer's si_indexers signer). Set, the SNode
// signs everything it sends and serves only indexer_id's tables (arbiter-core
// snode.Deps.ClaimSigner, housegate spec 2026-10-10 §6.5); unset, it stays a
// legacy unsigned SNode, which the arbiter refuses after the signed-claims
// activation. The key is read from the environment only, never from argv or
// the config file.
const envSNodeClaimKeyHex = "ARBITER_SNODE_CLAIM_KEY_HEX"
```

in `Config`, after `StateDir` (line 32):

```go
	// IndexerID is the indexer this SNode serves (snode.Config.IndexerID),
	// required together with envSNodeClaimKeyHex. A pointer, because 0 is a
	// valid indexer (devnet2's founding indexer).
	IndexerID *uint64 `yaml:"indexer_id"`
	// GenesisSnapshotID is the genesis snapshot id every signed message binds
	// (snode.Config.GenesisSnapshotID). Empty derives it from the genesis
	// tables; a host whose SNode holds no genesis table must set it.
	GenesisSnapshotID string `yaml:"genesis_snapshot_id"`
```

after `computedSchemaRoot` (213-215):

```go
// claimSigner is the SNode's claim signer from envSNodeClaimKeyHex: nil
// without it (a legacy unsigned SNode); with it, indexer_id is required.
func (c Config) claimSigner() (*authority.Signer, error) {
	keyHex := os.Getenv(envSNodeClaimKeyHex)
	if keyHex == "" {
		return nil, nil
	}
	signer, err := authority.NewSignerFromHex(keyHex)
	if err != nil {
		// Never echo key material or parser fragments.
		return nil, fmt.Errorf("%s is invalid: expected a 32-byte secp256k1 private key in hex", envSNodeClaimKeyHex)
	}
	if c.IndexerID == nil {
		return nil, fmt.Errorf("indexer_id is required when %s is set", envSNodeClaimKeyHex)
	}
	return signer, nil
}
```

and in `toRoleConfig` (217-226) compute `var indexerID uint64; if c.IndexerID != nil { indexerID = *c.IndexerID }` and add `IndexerID: indexerID, GenesisSnapshotID: c.GenesisSnapshotID,` to the `snode.Config` literal. `cmd/arbiter-snode/main.go` — in `run`, directly after `cfg.validate(true)` (183-185), before ClickHouse is opened:

```go
	signer, err := cfg.claimSigner()
	if err != nil {
		return fmt.Errorf("validate config: %w", err)
	}
	if signer == nil {
		logger.Warn("no claim signer: this SNode runs unsigned, and after the signed-claims activation the arbiter refuses it",
			"remedy", "set "+envSNodeClaimKeyHex+" to the indexer's IndexerRegistry signer key and indexer_id in the config")
	}
```

and add `ClaimSigner: signer,` to the `snode.Deps` literal (206-208).

- [ ] **Step 15: Run and commit**

Run: `cd "$W" && bazel test //cmd/arbiter-snode:all`
Expected: all PASS, including `TestSNodeSampleConfig_LoadsForSchemaRoot` and the `TestRun_*` cases (neither sets the key).

```bash
cd "$W" && git add cmd/arbiter-snode && git commit -m "feat(arbiter-snode): claim signer from ARBITER_SNODE_CLAIM_KEY_HEX, indexer_id and genesis_snapshot_id

With the key in the environment the standalone SNode signs what it sends
and serves only indexer_id's tables (arbiter-core snode.Deps.ClaimSigner);
without it the binary stays a legacy unsigned SNode and warns at startup
(housegate spec 2026-10-10 §6.5; contract §3c).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 9: arbiter — owner-routed promotions and cleanups, source acknowledgements, owner-scoped purge completion (S1)

**Files:**
- Modify: `fsm/signed_claims.go`: add `expectedSourceLocked`, `sourceAckRefusalLocked`, `PromotionSource`; `purgeReportRefusalLocked` gains `inc` and the owner check
- Modify: `fsm/apply.go` (`applyRecordPromotionAck` 605-707, after the `Acked` check at 611-613; `applyScheduleUnsafeCleanup` 725-756, after the authority check at 734-740; `applyRecordCleanupAck` 758-776)
- Modify: `fsm/apply_table_registry.go` (the `purgeReportRefusalLocked` call in `applyRecordTablePurged`; `completePurgesLocked` 278-302; new `incarnationOwnerSNodeLocked`, `purgeRequiredLocked`). `purgeNodeIDsLocked` (256-267) and `PurgeNodeSet` (269-276) stay unchanged (contract §3b.5).
- Create: `fsm/source_ack_test.go`, `fsm/purge_owner_test.go`
- Modify: `orchestrator/promotion.go` (`streamPromotionSigned` 168-191; new `promotionTargets`), `orchestrator/cleanup.go` (`streamCleanupSigned` 63-86)
- Modify: `orchestrator/orchestrator_fakes_test.go` (`fakeSNodes` 160-171 records the addressee)
- Create: `orchestrator/signed_claims_routing_test.go`

**Interfaces:**
- Consumes: Task 8 `snodeSignatureLocked`, `signedFieldsBeforeActivationReason` and the fixtures `signedClaimsFSM`, `messageContextFor`, `authorityContextFor`, `snodeRegistration`, `snodeMarkActive`, `snodePurged`, `verifierPurged`, `purgingIncarnation`, `rejectSignedUnchanged`; Tasks 5–7 `siIndexerLocked`, `TableIncarnation.OwnerIndexerID`, `claimsUpdate`, `activateClaims`, `siEntry`, Task 6 `ownedAddCmd`, `advanceTo`, `seedIndexerCmd` and the owner-bound admission of Task 7; existing `retireCmd`, `ev`, `mustSeal` (`fsm/table_set_transition_seal_test.go:54`), `verifyTransition`, `anchorFinal` (`fsm/table_set_transition_pipeline_test.go:47-60`), `publishManifestInputs` (`fsm/table_set_transition_manifest_test.go:98`), `lthashHex` (`fsm/threeway_test.go:18`), `testAccount`, `registryEnvelope`, `submit`; Task 3 `authority.{SNodeMessagePromotionAck, SNodeMessageCleanupAck}`, `wire.{PromotionAckFromPB, PromotionAckToPB, CleanupAckFromPB, CleanupAckToPB}`, `authoritytest.{MustSigner, IndexerKeyHex0, IndexerKeyHex1}`; existing `registryUpdate` (`fsm/consensus_updates_test.go:167`), `issuePromotion` (`fsm/promotion_test.go:203`), `quorumVerifiedBlock` (`:33`; its `evidenceBlock` registers s1 and v1..v3), `authorityFixture`, `anchorForBlock`, orchestrator `newHarness` (`orchestrator/loop_test.go:19`), `runLoop`, `seedGenesisWriterAndVerifiers` (s1 and v1..v3 under `testPubkeyO` keys), `enableRegistryViaNode` (`orchestrator/table_set_transition_test.go:63`), `driveToQuorum`, `hasSignedPromotion`, `hasCleanup`, `setSNodeConnections`, `waitFor`, `mustApplyViaNode`, `testPubkeyO`, `authorityParamsO`, `postCommitmentForPromotion`.
- Produces: `func (f *FSM) expectedSourceLocked(promotionSeq uint64) (node, reason string)`; `func (f *FSM) sourceAckRefusalLocked(kind authority.SNodeMessageKind, nodeID string, promotionSeq uint64, body any, jws string) string`; `func (f *FSM) PromotionSource(promotionSeq uint64) (node string, routed bool, err error)`; `func (f *FSM) incarnationOwnerSNodeLocked(inc *TableIncarnation) (string, bool)`; `func (f *FSM) purgeRequiredLocked(inc *TableIncarnation) []string`; `purgeReportRefusalLocked(c, node, inc)`; `func (o *Loop) promotionTargets(promotionSeq uint64) []string`; test helpers `promotedClaims`, `appliedAck`, `sourceAck`, `sourceCleanupAck` (fsm), `fakeSNodes.to`, `sentTargets`, `messageContextO`, `enrolledEntryO`, `activateSignedClaimsO`, `registerSignedSNodeO`, `ackSignedPromotionViaNode` (orchestrator).

**Retention check (contract §3, cleanup expected source).** Confirmed: `PendingPromotions` entries are never deleted (`git grep -n 'delete(f.st.PendingPromotions' fsm/` is empty; `fsm/reads_work.go:233-285` keeps reading acknowledged ones to derive `PublishedUncleaned`; restore keeps and validates them, `fsm/consensus_history.go:64-91`), and statements are never pruned (the only `delete(f.st.Statements` is the snapshot-query seal rollback at `fsm/apply_snapshot_query_submit.go:184`, which removes a statement created in the same Apply). So a cleanup's expected source is always derivable from `PendingPromotions[cleanup.PromotionSeq].StatementSeqs`, and no state is added. What today's code does not guarantee is that a cleanup names a promotion at all: `applyScheduleUnsafeCleanup` (`fsm/apply.go:725-756`) accepts any authority-signed cleanup. After the activation it now refuses one whose promotion has no resolvable source, since no node could ever acknowledge it and the drain gate would then block every later consensus update.

Rules (contract §3, §3a, §3b.5):
- **Expected source** of promotion `p` (and of the cleanup naming `p`): the bound `SourceNode` shared by every statement in `PendingPromotions[p].StatementSeqs`; an unknown promotion, an empty list, a missing or unbound statement, or two different sources is a refusal.
- **Acknowledgements after activation:** an unknown promotion is still refused and an already acknowledged one still answers `Applied{}` with no state change (§10.3 idempotency, signed or not); otherwise the acknowledgement must name the expected source and carry that node's entry signature over the decoded body, or it is `Rejected` with no state change, `Applied:false` included. A cleanup acknowledgement for a cleanup that is not pending stays an idempotent `Applied{}`; for a pending one it must match the cleanup's partition and pass the same source check. Before activation acknowledgements are unchanged, and a `source_jws` is refused like every new field.
- **Routing:** before activation promotions and cleanups go to every connected SNode (unchanged); after it only to the expected source, connected or not (a failed send is retried on the next pass); an unresolvable source sends nothing and logs.
- **Purge (§3b.5):** `GetPurgeNodeSet` keeps returning every registered, non-evicted SNode and verifier before and after the activation, because the data plane reads it as live replica membership (`arbiter-core dataplane/tableset/reconciler.go:696-768`: `sweep` drops, and `clearDecommissioned` refuses to create over, any replica missing from it). After the activation the completion of one incarnation's purge needs only its owner's SNode (when registered and not evicted) and every non-evicted verifier, and a report from any other SNode is refused. A node finds its own purge obligations in the registry it follows: the Purging incarnations it materialised (with Task 4's `tableset.Config.Owner` filter, its own tables for an SNode and every SI table for a verifier).
- **Spec D (spec §6.6):** the routing restriction and the source-acknowledgement rule apply among SNodes only. Verifiers receive no promotions or cleanups today (both streams address connected SNodes only) and this task keeps it that way; when Spec D (Proposed) makes verifiers safe replicas, their acknowledgements are recorded as replica acknowledgements under Spec D's own rules, never through `sourceAckRefusalLocked` as the source acknowledgement.

- [ ] **Step 1: Write the failing FSM tests**

Create `fsm/source_ack_test.go`:

```go
package fsm

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// promotedClaims drives statement 1 of db.t (bound to s1, the only SNode, by
// hash selection before the activation) to quorum, enables the registry with
// max_writers 2, commits the signed-claims activation (indexer 1: s1;
// indexer 2: activation block 150, SNode s2), registers s2 Active, anchors
// block 1 and issues promotion 1 under the post-activation authority context.
// It returns the FSM, the network authority and the promoted part hash.
func promotedClaims(t *testing.T) (*FSM, *authority.Signer, string) {
	t.Helper()
	a, params := authorityFixture(t)
	f, partHash := quorumVerifiedBlock(t, params)
	enable := registryUpdate(t, f, params, testRegistryParams())
	enable.MaxWriters = 2
	mustApply(t, f, signedConsensusUpdate(t, a, enable))
	second := indexerSigner2(t)
	activateClaims(t, f, a, siEntry(t, f, second, 2, 150, "s2"))
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 1))
	mustApply(t, f, snodeMarkActive(t, f, second, "s2", 1))
	mustApply(t, f, wire.Command{RecordAnchorFinality: &wire.RecordAnchorFinality{L3BlockSeq: 1, Anchor: anchorForBlock(t, f, 1),
		FinalityReached: true, LastMergeableReached: true}})
	promote := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1,
		CandidateParts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}}}
	token, err := a.SignPromotionWithContext(promote, authorityContextFor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote, AuthorityJWS: token}})
	return f, a, partHash
}

func appliedAck(node, partHash string) arbiter.PromotionAck {
	return arbiter.PromotionAck{NodeID: node, PromotionSeq: 1, TableID: "db.t", PartitionID: "p0",
		PostPartitionCommitment: partHash, Applied: true,
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: partHash, SafePartName: "all_9_9_0", PartPhysHash: "fixture-physical"}}}
}

// sourceAck is ack signed by signer over the value Apply decodes.
func sourceAck(t *testing.T, f *FSM, signer *authority.Signer, ack arbiter.PromotionAck) wire.Command {
	t.Helper()
	ack = wire.PromotionAckFromPB(wire.PromotionAckToPB(ack))
	jws, err := signer.SignSNodeMessage(authority.SNodeMessagePromotionAck, messageContextFor(t, f), ack)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack, SourceJWS: jws}}
}

// sourceCleanupAck is sourceAck for a cleanup acknowledgement.
func sourceCleanupAck(t *testing.T, f *FSM, signer *authority.Signer, ack arbiter.CleanupAck) wire.Command {
	t.Helper()
	ack = wire.CleanupAckFromPB(wire.CleanupAckToPB(ack))
	jws, err := signer.SignSNodeMessage(authority.SNodeMessageCleanupAck, messageContextFor(t, f), ack)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: ack, SourceJWS: jws}}
}

// TestAcknowledgementsBeforeActivationAreUnchanged: before the activation no
// node is checked and nothing is routed; only the new field is refused.
func TestAcknowledgementsBeforeActivationAreUnchanged(t *testing.T) {
	f, _, partHash, _ := issuePromotion(t)
	ack := appliedAck("any-node", partHash)
	rejectSignedUnchanged(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack, SourceJWS: "h.p.s"}}, signedFieldsBeforeActivationReason)
	rejectSignedUnchanged(t, f, wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{
		Ack: arbiter.CleanupAck{NodeID: "any-node", PromotionSeq: 1}, SourceJWS: "h.p.s"}}, signedFieldsBeforeActivationReason)
	if node, routed, err := f.PromotionSource(1); node != "" || routed || err != nil {
		t.Fatalf("PromotionSource before activation = %q %v %v, want broadcast", node, routed, err)
	}
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack}})
	if f.st.Statements[1].Status != StatusSafe {
		t.Fatalf("status = %v", f.st.Statements[1].Status)
	}
}

// TestNonOwnerAcknowledgementNeverConsumesAPromotion: spec §15 "an
// acknowledgement (including Applied:false) from any node other than the
// owner's SNode is never taken as the source acknowledgement".
func TestNonOwnerAcknowledgementNeverConsumesAPromotion(t *testing.T) {
	f, _, partHash := promotedClaims(t)
	founder, second := indexerSigner1(t), indexerSigner2(t)
	if node, routed, err := f.PromotionSource(1); node != "s1" || !routed || err != nil {
		t.Fatalf("PromotionSource = %q %v %v, want s1", node, routed, err)
	}
	notApplied := arbiter.PromotionAck{NodeID: "s2", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", Applied: false, Detail: "base CAS failed"}
	rejectSignedUnchanged(t, f, sourceAck(t, f, second, notApplied), "only the source s1 may acknowledge promotion 1")
	rejectSignedUnchanged(t, f, sourceAck(t, f, second, appliedAck("s2", partHash)), "only the source s1 may acknowledge promotion 1")
	if f.st.PendingPromotions[1].Acked || f.st.Statements[1].Status == StatusSafe {
		t.Fatal("an acknowledgement from a node other than the source consumed the promotion")
	}
	// The source's own Applied:false still consumes it (the orchestrator
	// re-issues under a new sequence), exactly as before the activation.
	notApplied.NodeID = "s1"
	mustApply(t, f, sourceAck(t, f, founder, notApplied))
	if !f.st.PendingPromotions[1].Acked || f.st.Statements[1].Status == StatusSafe {
		t.Fatalf("source Applied:false: acked=%v status=%v", f.st.PendingPromotions[1].Acked, f.st.Statements[1].Status)
	}
}

// TestSourceAcknowledgementMustBeSignedByTheOwner: unsigned, wrongly signed
// or edited-after-signing acknowledgements are refused; the signed one
// advances the partition.
func TestSourceAcknowledgementMustBeSignedByTheOwner(t *testing.T) {
	f, _, partHash := promotedClaims(t)
	founder, second := indexerSigner1(t), indexerSigner2(t)
	ack := appliedAck("s1", partHash)
	rejectSignedUnchanged(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack}}, "snode signature")
	rejectSignedUnchanged(t, f, sourceAck(t, f, second, ack), "snode signature")
	edited := sourceAck(t, f, founder, ack)
	edited.RecordPromotionAck.Ack.Detail = "edited after signing"
	rejectSignedUnchanged(t, f, edited, "snode signature")
	mustApply(t, f, sourceAck(t, f, founder, ack))
	if f.st.Statements[1].Status != StatusSafe || !f.st.PendingPromotions[1].Acked {
		t.Fatalf("status %v acked %v", f.st.Statements[1].Status, f.st.PendingPromotions[1].Acked)
	}
	// An already-acknowledged promotion answers any re-ack idempotently and
	// changes nothing (§10.3).
	mustApply(t, f, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack}})
}

// TestCleanupAcknowledgementComesFromTheSource: only the source's signed
// acknowledgement of the cleanup's own partition clears it.
func TestCleanupAcknowledgementComesFromTheSource(t *testing.T) {
	f, a, partHash := promotedClaims(t)
	founder := indexerSigner1(t)
	mustApply(t, f, sourceAck(t, f, founder, appliedAck("s1", partHash)))
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1,
		Parts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}}}
	token, err := a.SignCleanupWithContext(cleanup, authorityContextFor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: cleanup, AuthorityJWS: token}})
	ack := arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0"}
	other := ack
	other.NodeID = "s2"
	rejectSignedUnchanged(t, f, sourceCleanupAck(t, f, indexerSigner2(t), other), "only the source s1 may acknowledge promotion 1")
	rejectSignedUnchanged(t, f, wire.Command{RecordCleanupAck: &wire.RecordCleanupAck{Ack: ack}}, "snode signature")
	wrong := ack
	wrong.PartitionID = "p9"
	rejectSignedUnchanged(t, f, sourceCleanupAck(t, f, founder, wrong), "cleanup ack partition does not match the cleanup")
	if f.st.PendingCleanups[1] == nil {
		t.Fatal("a refused acknowledgement cleared the cleanup")
	}
	mustApply(t, f, sourceCleanupAck(t, f, founder, ack))
	if len(f.st.PendingCleanups) != 0 || len(f.st.PromotedUnsafe) != 0 {
		t.Fatalf("pending %v promoted-unsafe %v", f.st.PendingCleanups, f.st.PromotedUnsafe)
	}
}

// TestCleanupWithoutAPromotionSourceIsRefusedAfterActivation: no node could
// acknowledge it, and an unacknowledgeable cleanup would block every later
// consensus update.
func TestCleanupWithoutAPromotionSourceIsRefusedAfterActivation(t *testing.T) {
	f, a, partHash := promotedClaims(t)
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p0", PromotionSeq: 7,
		Parts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: partHash}}}
	token, err := a.SignCleanupWithContext(cleanup, authorityContextFor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	rejectSignedUnchanged(t, f, wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: cleanup, AuthorityJWS: token}},
		"schedule cleanup: promotion 7 is unknown")
}

// TestExpectedSourceRefusesGoneOrDisagreeingStatements: contract §3 "if the
// covered statements are gone or disagree, refuse".
func TestExpectedSourceRefusesGoneOrDisagreeingStatements(t *testing.T) {
	f, _, partHash := promotedClaims(t)
	founder := indexerSigner1(t)
	// Synthesized corruption: a covered statement bound to another source.
	f.st.Statements[99] = &StatementState{Seq: 99, SourceNode: "s2", Status: StatusPromotable}
	f.st.PendingPromotions[1].StatementSeqs = append(f.st.PendingPromotions[1].StatementSeqs, 99)
	rejectSignedUnchanged(t, f, sourceAck(t, f, founder, appliedAck("s1", partHash)), "bound to different sources (s1, s2)")
	if _, _, err := f.PromotionSource(1); err == nil {
		t.Fatal("PromotionSource named a source for disagreeing statements")
	}
	delete(f.st.Statements, 99)
	rejectSignedUnchanged(t, f, sourceAck(t, f, founder, appliedAck("s1", partHash)), "covers statement 99, which is gone")
}

// TestSourceAcknowledgementFollowsTheStatementsIncarnation is the review case
// "a database deleted and recreated with the same id on another indexer"
// (spec 2026-10-10 §15). dbx.t lives first as incarnation A, owned by indexer
// 2 (SNode s2), and is then re-created as incarnation B, owned by the founding
// indexer 1 (SNode s1); these are the review case's indexers 1 and 0. The
// expected source of a promotion and of its cleanup is the bound source of the
// statements it covers, not the key's current owner: A's work goes to and is
// acknowledged by s2 only, B's by s1 only.
//
// Setup. The key's history uses the registry commands Task 6 uses. A is added,
// then retired before its add transition is sealed, so it retires straight to
// Purging. Its owner's SNode and the verifiers report the purge. B is
// admissible only once A is Purged (spec D9); it is added and made Active
// through its transition block, as Task 7's ownerFSM activates db2.x.
// Statement B is admitted for real (Task 7 binds B's owner's SNode, s1), its
// signed RC is bound and its block sealed. Statement A is constructed as
// Task 7's admission bound it while A was live, because no real history
// reaches this state: an Active A is purged only after a removal transition
// that the safe prefix holds behind the block of A's statements until their
// promotions complete, and the replay receipts that block needs exist for
// db.t only (testStateRoot, receiptForBlock). The expected source reads
// nothing but the covered statements' bound SourceNode, which the
// construction sets. Both promotions and A's cleanup then run through the
// real commands. A's cleanup is the piece a real history does keep pending
// across the re-creation.
func TestSourceAcknowledgementFollowsTheStatementsIncarnation(t *testing.T) {
	f, a := signedClaimsFSM(t)
	founder, second := indexerSigner1(t), indexerSigner2(t)
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 1))
	mustApply(t, f, snodeMarkActive(t, f, second, "s2", 1))
	mustApply(t, f, advanceTo(149))
	mustApply(t, f, seedIndexerCmd(2, 149))

	// Incarnation A: indexer 2's dbx.t, purged by its owner's SNode and the
	// verifiers.
	mustApply(t, f, ownedAddCmd(t, f, "dbx", "t", ev(150, "0x150", 0), ev(151, "0x151", 0), 2))
	incA := f.st.TableRegistry.Live("dbx.t")
	mustApply(t, f, retireCmd("dbx", wire.TableRetireReasonTableDeleted, ev(152, "0x152", 0), "t"))
	for _, v := range []string{"v1", "v2", "v3"} {
		mustApply(t, f, verifierPurged(t, f, v, v[len(v)-1], incA.Seq))
	}
	mustApply(t, f, snodePurged(t, f, second, "s2", incA.Seq))
	if incA.Status != TableStatusPurged {
		t.Fatalf("incarnation A = %s, want Purged", incA.Status)
	}

	// Incarnation B: the same key re-created under the founding indexer 1.
	mustApply(t, f, ownedAddCmd(t, f, "dbx", "t", ev(153, "0x153", 0), ev(154, "0x154", 0), 1))
	transition := mustSeal(t, f).BlockSeq
	verifyTransition(t, f, transition)
	anchorFinal(t, f, transition)
	publishManifestInputs(t, f)
	incB := f.st.TableRegistry.Live("dbx.t")
	if incB == nil || incB.Seq == incA.Seq || incB.Status != TableStatusActive || incB.OwnerIndexerID == nil || *incB.OwnerIndexerID != 1 {
		t.Fatalf("incarnation B = %+v, want an Active incarnation owned by indexer 1", incB)
	}

	// Statement B, admitted against B: bound to s1, RC signed by s1, sealed.
	key, account := testAccount(t)
	envB := registryEnvelope(t, key, account, 1, "dbx.t", incB.SchemaHash)
	rB := submit(t, f, envB)
	if rB.Code != arbiter.AdmissionCodeAccepted || f.st.Statements[rB.StatementSeq].SourceNode != "s1" {
		t.Fatalf("statement B = %+v", rB)
	}
	partA, partB := lthashHex("rowA"), lthashHex("rowB")
	rcB := wire.RCFromPB(wire.RCToPB(arbiter.RCRecord{StatementID: envB.StatementID, SourceNode: "s1", SourceClaimRoot: "0xrb",
		CandidateParts: []arbiter.CandidatePart{{TableID: "dbx.t", PartitionID: "p0", PartName: "all_2_0", PartRowLtHash: partB, RowCount: 1, Bytes: 32}}}))
	rcJWS, err := founder.SignSNodeMessage(authority.SNodeMessageResultClaim, messageContextFor(t, f), rcB)
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{RegisterRC: &wire.RegisterRC{RC: rcB, SourceJWS: rcJWS}})
	mustSeal(t, f)

	// Statement A, as Task 7 bound it while A was live (see the setup note).
	envA := registryEnvelope(t, key, account, 2, "dbx.t", incA.SchemaHash)
	rcA := wire.RCFromPB(wire.RCToPB(arbiter.RCRecord{StatementID: envA.StatementID, SourceNode: "s2", SourceClaimRoot: "0xra",
		CandidateParts: []arbiter.CandidatePart{{TableID: "dbx.t", PartitionID: "p1", PartName: "all_1_0", PartRowLtHash: partA, RowCount: 1, Bytes: 32}}}))
	f.st.Statements[99] = &StatementState{Env: envA, Seq: 99, SourceNode: "s2", Status: StatusPromotable, RC: &rcA,
		UnpromotedParts: map[string]bool{partA: true}}

	issue := func(partition, part string) uint64 {
		t.Helper()
		promote := arbiter.PromoteSafePartition{TableID: "dbx.t", PartitionID: partition, PromotionSeq: f.st.PromotionSeq + 1,
			CandidateParts: []arbiter.PartRef{{TableID: "dbx.t", PartitionID: partition, PartRowLtHash: part}}}
		token, err := a.SignPromotionWithContext(promote, authorityContextFor(t, f))
		if err != nil {
			t.Fatal(err)
		}
		mustApply(t, f, wire.Command{RecordPromotionIssued: &wire.RecordPromotionIssued{Promote: promote, AuthorityJWS: token}})
		return promote.PromotionSeq
	}
	promoteA, promoteB := issue("p1", partA), issue("p0", partB)
	for p, want := range map[uint64]string{promoteA: "s2", promoteB: "s1"} {
		if node, routed, err := f.PromotionSource(p); node != want || !routed || err != nil {
			t.Fatalf("PromotionSource(%d) = %q %v %v, want %s (the orchestrator streams to it alone)", p, node, routed, err, want)
		}
	}

	ack := func(node string, promotion uint64, partition, part string, applied bool) arbiter.PromotionAck {
		out := arbiter.PromotionAck{NodeID: node, PromotionSeq: promotion, TableID: "dbx.t", PartitionID: partition, Applied: applied}
		if applied {
			out.PostPartitionCommitment = part
			out.Parts = []arbiter.SafePartMapping{{PartRowLtHash: part, SafePartName: "all_9_9_0", PartPhysHash: "fixture-physical"}}
		} else {
			out.Detail = "base CAS failed"
		}
		return out
	}
	// dbx.t now belongs to indexer 1, yet s1 cannot take A's promotion, not
	// even with Applied:false, and s2 cannot take B's.
	rejectSignedUnchanged(t, f, sourceAck(t, f, founder, ack("s1", promoteA, "p1", partA, false)), "only the source s2 may acknowledge promotion")
	rejectSignedUnchanged(t, f, sourceAck(t, f, second, ack("s2", promoteB, "p0", partB, false)), "only the source s1 may acknowledge promotion")
	if f.st.PendingPromotions[promoteA].Acked || f.st.PendingPromotions[promoteB].Acked {
		t.Fatal("an acknowledgement from the other incarnation's source consumed a promotion")
	}
	mustApply(t, f, sourceAck(t, f, second, ack("s2", promoteA, "p1", partA, true)))
	mustApply(t, f, sourceAck(t, f, founder, ack("s1", promoteB, "p0", partB, false)))
	if !f.st.PendingPromotions[promoteA].Acked || !f.st.PendingPromotions[promoteB].Acked || f.st.Statements[99].Status != StatusSafe {
		t.Fatalf("acked A %v B %v, statement A %v", f.st.PendingPromotions[promoteA].Acked, f.st.PendingPromotions[promoteB].Acked, f.st.Statements[99].Status)
	}

	// A's cleanup, which a real history can still owe after the re-creation,
	// follows A's statements too.
	cleanup := arbiter.UnsafeCleanup{TableID: "dbx.t", PartitionID: "p1", PromotionSeq: promoteA,
		Parts: []arbiter.PartRef{{TableID: "dbx.t", PartitionID: "p1", PartRowLtHash: partA}}}
	token, err := a.SignCleanupWithContext(cleanup, authorityContextFor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{ScheduleUnsafeCleanup: &wire.ScheduleUnsafeCleanup{Cleanup: cleanup, AuthorityJWS: token}})
	cleanupAck := arbiter.CleanupAck{NodeID: "s1", PromotionSeq: promoteA, TableID: "dbx.t", PartitionID: "p1"}
	rejectSignedUnchanged(t, f, sourceCleanupAck(t, f, founder, cleanupAck), "only the source s2 may acknowledge promotion")
	cleanupAck.NodeID = "s2"
	mustApply(t, f, sourceCleanupAck(t, f, second, cleanupAck))
	if f.st.PendingCleanups[promoteA] != nil || len(f.st.PromotedUnsafe) != 0 {
		t.Fatalf("pending cleanups %v, promoted-unsafe %v", f.st.PendingCleanups, f.st.PromotedUnsafe)
	}
}
```

Create `fsm/purge_owner_test.go`:

```go
package fsm

import (
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// TestPurgeAfterActivationNeedsTheOwnerSNodeAndTheVerifiers: spec §6.6 and
// contract §3b.5. s2 is registered and Active but holds no replica of
// indexer 1's table: its report is refused and the purge completes without
// it.
func TestPurgeAfterActivationNeedsTheOwnerSNodeAndTheVerifiers(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	second := indexerSigner2(t)
	mustApply(t, f, snodeRegistration(t, f, second, "s2", 1))
	mustApply(t, f, snodeMarkActive(t, f, second, "s2", 1))
	seq := purgingIncarnation(t, f, 1)
	rejectSignedUnchanged(t, f, snodePurged(t, f, second, "s2", seq), "s2 is not the SNode of incarnation")
	for _, v := range []string{"v1", "v2", "v3"} {
		mustApply(t, f, verifierPurged(t, f, v, v[len(v)-1], seq))
	}
	inc := f.st.TableRegistry.incarnation(seq)
	if inc.Status != TableStatusPurging {
		t.Fatalf("the purge completed without the owner's SNode: %s", inc.Status)
	}
	mustApply(t, f, snodePurged(t, f, indexerSigner1(t), "s1", seq))
	if inc.Status != TableStatusPurged {
		t.Fatalf("owner and verifiers reported, status = %s", inc.Status)
	}
}

// TestEvictingTheOwnerSNodeCompletesThePurge: an evicted node is never
// awaited, the owner's SNode included.
func TestEvictingTheOwnerSNodeCompletesThePurge(t *testing.T) {
	f, a := signedClaimsFSM(t)
	seq := purgingIncarnation(t, f, 1)
	for _, v := range []string{"v1", "v2", "v3"} {
		mustApply(t, f, verifierPurged(t, f, v, v[len(v)-1], seq))
	}
	cmd := arbiter.EvictNodeCommand{NodeID: "s1", Reason: "host lost"} // grandfathered: current sequence 0
	token, err := a.SignEvictNodeWithContext(cmd, authorityContextFor(t, f))
	if err != nil {
		t.Fatal(err)
	}
	mustApply(t, f, wire.Command{EvictNode: &wire.EvictNode{NodeID: "s1", Reason: "host lost", AuthorityJWS: token}})
	if inc := f.st.TableRegistry.incarnation(seq); inc.Status != TableStatusPurged {
		t.Fatalf("status = %s", inc.Status)
	}
}

// TestPurgeNodeSetIsUnchangedByTheActivation pins contract §3b.5:
// GetPurgeNodeSet keeps listing every registered, non-evicted SNode and
// verifier after the activation, including an SNode that owns no table yet,
// because the data plane reads it as live replica membership; only purge
// completion is owner-scoped.
func TestPurgeNodeSetIsUnchangedByTheActivation(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	if got := f.PurgeNodeSet(); !slices.Equal(got, []string{"s1", "v1", "v2", "v3"}) {
		t.Fatalf("after activation = %v", got)
	}
	mustApply(t, f, snodeRegistration(t, f, indexerSigner2(t), "s2", 1))
	if got := f.PurgeNodeSet(); !slices.Equal(got, []string{"s1", "s2", "v1", "v2", "v3"}) {
		t.Fatalf("s2, which owns no table yet, must be listed: %v", got)
	}
	seq := purgingIncarnation(t, f, 1)
	for _, v := range []string{"v1", "v2", "v3"} {
		mustApply(t, f, verifierPurged(t, f, v, v[len(v)-1], seq))
	}
	mustApply(t, f, snodePurged(t, f, indexerSigner1(t), "s1", seq))
	if got := f.PurgeNodeSet(); !slices.Equal(got, []string{"s1", "s2", "v1", "v2", "v3"}) {
		t.Fatalf("a completed purge changed the purge node set: %v", got)
	}
}
```

Run: `cd "$W" && go test ./fsm/ -run 'AcknowledgementsBeforeActivation|NonOwnerAcknowledgement|SourceAcknowledgementMustBeSigned|CleanupAcknowledgementComesFromTheSource|CleanupWithoutAPromotionSource|ExpectedSourceRefuses|SourceAcknowledgementFollows|PurgeAfterActivation|EvictingTheOwnerSNode|PurgeNodeSetIsUnchanged' -v`
Expected: build failure (`f.PromotionSource undefined`); with it stubbed, FAIL on the non-owner `Applied:false` (it consumes the promotion today), on the other incarnation's source acknowledging (`TestSourceAcknowledgementFollowsTheStatementsIncarnation`) and on the purge that completes without the owner.

- [ ] **Step 2: Implement the FSM rules**

Append to `fsm/signed_claims.go` (add `"errors"` to its imports):

```go
// expectedSourceLocked is the node whose acknowledgement is the source
// acknowledgement of promotion promotionSeq and of the cleanup that names it
// (spec 2026-10-10 §6.6): the bound SourceNode every statement the promotion
// covers shares. Pending promotions are never deleted and statements are
// never pruned, so a cleanup finds its promotion's statements however long
// after the promotion it runs. reason is non-empty when the promotion is
// unknown or covers no statement, or a covered statement is gone, unbound, or
// bound to a different source; the caller refuses then.
func (f *FSM) expectedSourceLocked(promotionSeq uint64) (node, reason string) {
	pp := f.st.PendingPromotions[promotionSeq]
	if pp == nil {
		return "", fmt.Sprintf("promotion %d is unknown", promotionSeq)
	}
	if len(pp.StatementSeqs) == 0 {
		return "", fmt.Sprintf("promotion %d covers no statement", promotionSeq)
	}
	for _, seq := range pp.StatementSeqs {
		ss := f.st.Statements[seq]
		switch {
		case ss == nil || ss.SourceNode == "":
			return "", fmt.Sprintf("promotion %d covers statement %d, which is gone or has no bound source", promotionSeq, seq)
		case node == "":
			node = ss.SourceNode
		case ss.SourceNode != node:
			return "", fmt.Sprintf("promotion %d covers statements bound to different sources (%s, %s)", promotionSeq, node, ss.SourceNode)
		}
	}
	return node, ""
}

// sourceAckRefusalLocked admits only the source acknowledgement of promotion
// promotionSeq after the activation (spec 2026-10-10 §6.6): it names the
// expected source and carries that node's entry signature over the
// acknowledgement. Every other node's acknowledgement, Applied:false
// included, is refused with no state change.
func (f *FSM) sourceAckRefusalLocked(kind authority.SNodeMessageKind, nodeID string, promotionSeq uint64, body any, jws string) string {
	expected, reason := f.expectedSourceLocked(promotionSeq)
	if reason != "" {
		return fmt.Sprintf("%s: %s", kind, reason)
	}
	if nodeID != expected {
		return fmt.Sprintf("%s from %s: only the source %s may acknowledge promotion %d", kind, nodeID, expected, promotionSeq)
	}
	if err := f.snodeSignatureLocked(kind, expected, body, jws); err != nil {
		return fmt.Sprintf("%s from %s: %v", kind, nodeID, err)
	}
	return ""
}

// PromotionSource is where the orchestrator sends promotion promotionSeq and
// its cleanup (spec 2026-10-10 §6.6). Before the signed-claims activation
// routed is false and every connected SNode receives them, as before. After
// it node is the expected source, or err says why there is none.
func (f *FSM) PromotionSource(promotionSeq uint64) (node string, routed bool, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.signedClaimsActiveLocked() {
		return "", false, nil
	}
	node, reason := f.expectedSourceLocked(promotionSeq)
	if reason != "" {
		return "", true, errors.New(reason)
	}
	return node, true, nil
}
```

and give `purgeReportRefusalLocked` (Task 8) the incarnation and the owner check, then pass `inc` at its call site in `applyRecordTablePurged`:

```go
func (f *FSM) purgeReportRefusalLocked(c *wire.RecordTablePurged, node *NodeInfo, inc *TableIncarnation) string {
	body := authority.TablePurgedBody{NodeID: c.NodeID, IncarnationSeq: c.IncarnationSeq}
	roles := node.Registration.Roles
	snode, verifier := hasRole(roles, arbiter.NodeRoleSNode), hasRole(roles, arbiter.NodeRoleVerifier)
	var err error
	switch {
	case snode && verifier:
		return fmt.Sprintf("record table purged: %s holds both data-plane roles; re-register it with one", c.NodeID)
	case snode:
		// Only the owner's SNode holds the table (spec 2026-10-10 D8).
		if owner, ok := f.incarnationOwnerSNodeLocked(inc); !ok || owner != c.NodeID {
			return fmt.Sprintf("record table purged: %s is not the SNode of incarnation %d's owner", c.NodeID, c.IncarnationSeq)
		}
		err = f.snodeSignatureLocked(authority.SNodeMessageTablePurged, c.NodeID, body, c.SignerJWS)
	default:
		err = f.verifierSignatureLocked(authority.VerifierMessageTablePurged, c.NodeID, node.Registration.Ed25519Pubkey, body, c.Ed25519Signature)
	}
	if err != nil {
		return fmt.Sprintf("record table purged: %s: %v", c.NodeID, err)
	}
	return ""
}
```

`fsm/apply.go` — `applyRecordPromotionAck` (605-707): make its first statements

```go
	ack := c.Ack
	active := f.signedClaimsActiveLocked()
	if !active && c.SourceJWS != "" {
		return Rejected{Reason: signedFieldsBeforeActivationReason}
	}
```

and directly after `if pp.Acked { return Applied{} }` (611-613):

```go
	// Only the source acknowledgement may advance or consume the promotion
	// after the activation (spec 2026-10-10 §6.6), Applied:false included.
	if active {
		if reason := f.sourceAckRefusalLocked(authority.SNodeMessagePromotionAck, ack.NodeID, ack.PromotionSeq, ack, c.SourceJWS); reason != "" {
			return Rejected{Reason: reason}
		}
	}
```

`applyScheduleUnsafeCleanup` (725-756), after the authority verification (734-740):

```go
	// After the activation only the promotion's source may acknowledge a
	// cleanup, so a cleanup whose promotion has no resolvable source could
	// never be acknowledged and would block every later consensus update.
	if f.signedClaimsActiveLocked() {
		if _, reason := f.expectedSourceLocked(c.Cleanup.PromotionSeq); reason != "" {
			return Rejected{Reason: "schedule cleanup: " + reason}
		}
	}
```

`applyRecordCleanupAck` (758-776):

```go
func (f *FSM) applyRecordCleanupAck(c *wire.RecordCleanupAck) any {
	active := f.signedClaimsActiveLocked()
	if !active && c.SourceJWS != "" {
		return Rejected{Reason: signedFieldsBeforeActivationReason}
	}
	cl, ok := f.st.PendingCleanups[c.Ack.PromotionSeq]
	if !ok {
		return Applied{} // already cleaned or never scheduled: idempotent (§10.3)
	}
	if active {
		if c.Ack.TableID != cl.TableID || c.Ack.PartitionID != cl.PartitionID {
			return Rejected{Reason: "cleanup ack partition does not match the cleanup"}
		}
		if reason := f.sourceAckRefusalLocked(authority.SNodeMessageCleanupAck, c.Ack.NodeID, c.Ack.PromotionSeq, c.Ack, c.SourceJWS); reason != "" {
			return Rejected{Reason: reason}
		}
	}
	// ... the rest (lines 763-775) unchanged ...
}
```

`fsm/apply_table_registry.go` — leave `purgeNodeIDsLocked` and `PurgeNodeSet` as they are, and replace `completePurgesLocked` (278-302) with:

```go
// incarnationOwnerSNodeLocked is the snode_node_id of the si_indexers entry
// owning inc. An incarnation without a recorded owner belongs to the founding
// indexer (TableRegistryParams.SIIndexerID); ok is false when the owner has
// no entry.
func (f *FSM) incarnationOwnerSNodeLocked(inc *TableIncarnation) (string, bool) {
	params := f.st.Params.TableRegistry
	if inc == nil || params == nil {
		return "", false
	}
	owner := params.SIIndexerID
	if inc.OwnerIndexerID != nil {
		owner = *inc.OwnerIndexerID
	}
	entry, ok := f.siIndexerLocked(owner)
	if !ok {
		return "", false
	}
	return entry.SNodeNodeID, true
}

// purgeRequiredLocked is the set of nodes whose reports complete the purge of
// inc after the signed-claims activation (spec 2026-10-10 §6.6, contract
// §3b.5), sorted: every non-evicted verifier, plus the owner's SNode when
// that node is registered and not evicted. Only those nodes hold the table.
// GetPurgeNodeSet is unaffected: it stays every registered, non-evicted
// SNode and verifier.
func (f *FSM) purgeRequiredLocked(inc *TableIncarnation) []string {
	owner, ownerOK := f.incarnationOwnerSNodeLocked(inc)
	var required []string
	for id, node := range f.st.Nodes {
		if node == nil || node.Status == NodeEvicted {
			continue
		}
		roles := node.Registration.Roles
		if hasRole(roles, arbiter.NodeRoleVerifier) || (ownerOK && id == owner && hasRole(roles, arbiter.NodeRoleSNode)) {
			required = append(required, id)
		}
	}
	slices.Sort(required)
	return required
}

// completePurgesLocked marks Purging incarnations Purged once every node
// whose report completes them has reported: before the signed-claims
// activation every current non-evicted SNode and verifier, after it
// purgeRequiredLocked. It reads committed state only, so re-running it after
// membership changes is deterministic.
func (f *FSM) completePurgesLocked() {
	reg := f.st.TableRegistry
	if reg == nil {
		return
	}
	active := f.signedClaimsActiveLocked()
	required := f.purgeNodeIDsLocked()
	for _, inc := range reg.Incarnations {
		if inc.Status != TableStatusPurging {
			continue
		}
		if active {
			required = f.purgeRequiredLocked(inc)
		}
		done := true
		for _, id := range required {
			if !slices.Contains(inc.PurgedBy, id) {
				done = false
				break
			}
		}
		if done && len(required) > 0 {
			inc.Status = TableStatusPurged
		}
	}
}
```

- [ ] **Step 3: Run the FSM suite**

Run: `cd "$W" && go test ./fsm/ -run 'AcknowledgementsBeforeActivation|NonOwnerAcknowledgement|SourceAcknowledgementMustBeSigned|CleanupAcknowledgementComesFromTheSource|CleanupWithoutAPromotionSource|ExpectedSourceRefuses|SourceAcknowledgementFollows|PurgeAfterActivation|EvictingTheOwnerSNode|PurgeNodeSetIsUnchanged' -v && bazel test //fsm:all //server:all && git diff --exit-code -- fsm/testdata`
Expected: all PASS, including `TestPromotionAck_*`, `TestCleanup_ScheduleAndAck`, `TestRecordTablePurged`, `TestEvictionCompletesPurge`, Task 5's `TestVerifierRemovalCompletesAPurgeWithOneRegistryBump`, the feature-book tests in `server/features_test.go` (they read `PurgeNodeSet`, which is unchanged) and both frozen goldens.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add fsm && git commit -m "feat(fsm): source acknowledgements and owner-scoped purge completion

After the signed-claims activation only the bound source of a
promotion's statements may acknowledge it or its cleanup, signed with
its entry's key; another node's Applied:false no longer consumes a
promotion. A purge completes with the owner's SNode and every
non-evicted verifier, and a report from any other SNode is refused;
GetPurgeNodeSet keeps listing every registered non-evicted data-plane
node (housegate spec 2026-10-10 §6.6).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Write the failing orchestrator tests**

In `orchestrator/orchestrator_fakes_test.go` let `fakeSNodes` (160-171) record the addressee:

```go
type fakeSNodes struct {
	mu        sync.Mutex
	connected []string
	sent      []*pb.PromotionCommand
	// to[i] is the node sent[i] was addressed to.
	to []string
}

func (s *fakeSNodes) Send(id string, msg *pb.PromotionCommand) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = append(s.sent, msg)
	s.to = append(s.to, id)
	return nil
}
```

Create `orchestrator/signed_claims_routing_test.go`:

```go
package orchestrator

import (
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/sentioxyz/arbiter/fsm"
)

func messageContextO(t *testing.T, f *fsm.FSM) authority.MessageContext {
	t.Helper()
	view, err := f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	return authority.MessageContext{NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID}
}

func enrolledEntryO(t *testing.T, f *fsm.FSM, signer *authority.Signer, indexer, activation uint64, node string) arbiter.SIIndexerEntry {
	t.Helper()
	ctx := messageContextO(t, f)
	jws, err := signer.SignSNodeEnrollment(authority.SNodeEnrollmentStatement{
		NetworkID: ctx.NetworkID, GenesisSnapshotID: ctx.GenesisSnapshotID, IndexerID: indexer, SNodeNodeID: node})
	if err != nil {
		t.Fatal(err)
	}
	return arbiter.SIIndexerEntry{IndexerID: indexer, ActivationBlock: activation, Signer: signer.Address(), SNodeNodeID: node, EnrollmentJWS: jws}
}

// activateSignedClaimsO commits the signed-claims activation on a node whose
// table registry enableRegistryViaNode enabled and seeded (founding indexer 1,
// activation block 100): indexer 1 (SNode s1, authoritytest key 0), indexer 2
// (activation block 150, SNode s2, key 1) and the verifiers
// seedGenesisWriterAndVerifiers registered, with max_writers 2, signed by a.
func activateSignedClaimsO(t *testing.T, node *fakeNode, f *fsm.FSM, a *authority.Signer) {
	t.Helper()
	view, err := f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	u := arbiter.ConsensusParamsUpdate{NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID,
		ExpectedEpoch: view.Epoch, PreviousParamsDigest: view.ParamsDigest, AuthorityAddresses: view.Current.AuthorityAddresses,
		MaxWriters: 2, ExpectedPromotionSeq: view.PromotionSeq, TableRegistry: view.Current.TableRegistry,
		SIIndexers: []arbiter.SIIndexerEntry{
			enrolledEntryO(t, f, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0), 1, 100, "s1"),
			enrolledEntryO(t, f, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), 2, 150, "s2"),
		},
		Verifiers: []arbiter.VerifierEntry{{NodeID: "v1", Ed25519Pubkey: testPubkeyO('1')}, {NodeID: "v2", Ed25519Pubkey: testPubkeyO('2')},
			{NodeID: "v3", Ed25519Pubkey: testPubkeyO('3')}},
	}
	token, err := a.SignConsensusParamsUpdateAt(u, 1)
	if err != nil {
		t.Fatal(err)
	}
	mustApplyViaNode(t, node, wire.Command{UpdateConsensusParams: &wire.UpdateConsensusParams{Update: u, AuthorityJWS: token}})
	if !f.SignedClaimsActive() {
		t.Fatal("signed claims not active")
	}
}

// registerSignedSNodeO registers and activates SNode id at seq, signed by its
// entry's signer.
func registerSignedSNodeO(t *testing.T, node *fakeNode, f *fsm.FSM, signer *authority.Signer, id string, seq uint64) {
	t.Helper()
	ctx := messageContextO(t, f)
	reg := wire.RegistrationFromPB(wire.RegistrationToPB(arbiter.NodeRegistration{NodeID: id, Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: seq}))
	jws, err := signer.SignSNodeMessage(authority.SNodeMessageRegistration, ctx, reg)
	if err != nil {
		t.Fatal(err)
	}
	mustApplyViaNode(t, node, wire.Command{RegisterNode: &wire.RegisterNode{Registration: reg, SignerJWS: jws}})
	markJWS, err := signer.SignSNodeMessage(authority.SNodeMessageMarkActive, ctx, authority.MarkActiveBody{NodeID: id, RegistrationSeq: seq})
	if err != nil {
		t.Fatal(err)
	}
	mustApplyViaNode(t, node, wire.Command{MarkActive: &wire.MarkActive{NodeID: id, RegistrationSeq: seq, SignerJWS: markJWS}})
}

// ackSignedPromotionViaNode is ackPromotionViaNode
// (orchestrator/promotion_fixtures_test.go:77) for the source's signed
// acknowledgement after the activation.
func ackSignedPromotionViaNode(t *testing.T, node *fakeNode, f *fsm.FSM, signer *authority.Signer, nodeID string, promotionSeq uint64) {
	t.Helper()
	ws, err := f.WorkSet()
	if err != nil {
		t.Fatalf("workset: %v", err)
	}
	for _, row := range ws.IssuedUnacked {
		if row.Promote.PromotionSeq != promotionSeq {
			continue
		}
		parts := make([]arbiter.SafePartMapping, 0, len(row.Promote.CandidateParts))
		for idx, part := range row.Promote.CandidateParts {
			parts = append(parts, arbiter.SafePartMapping{PartRowLtHash: part.PartRowLtHash, SafePartName: "all_safe_" + string(rune('a'+idx)), PartPhysHash: "0xphys"})
		}
		ack := wire.PromotionAckFromPB(wire.PromotionAckToPB(arbiter.PromotionAck{NodeID: nodeID, PromotionSeq: row.Promote.PromotionSeq,
			TableID: row.Promote.TableID, PartitionID: row.Promote.PartitionID, PostPartitionCommitment: postCommitmentForPromotion(t, row.Promote),
			Parts: parts, Applied: true}))
		jws, err := signer.SignSNodeMessage(authority.SNodeMessagePromotionAck, messageContextO(t, f), ack)
		if err != nil {
			t.Fatal(err)
		}
		mustApplyViaNode(t, node, wire.Command{RecordPromotionAck: &wire.RecordPromotionAck{Ack: ack, SourceJWS: jws}})
		return
	}
	t.Fatalf("promotion %d not issued: %+v", promotionSeq, ws.IssuedUnacked)
}

// sentTargets lists the distinct nodes promotions (cleanup=false) or cleanups
// (cleanup=true) were addressed to.
func sentTargets(fsn *fakeSNodes, cleanup bool) []string {
	fsn.mu.Lock()
	defer fsn.mu.Unlock()
	seen := map[string]bool{}
	for i, pc := range fsn.sent {
		if (cleanup && pc.GetCleanup() != nil) || (!cleanup && pc.GetPromote() != nil) {
			seen[fsn.to[i]] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// TestPromotionIsBroadcastBeforeActivation: unchanged behaviour.
func TestPromotionIsBroadcastBeforeActivation(t *testing.T) {
	params, signer := authorityParamsO(t)
	f, node, _, fsn, _, o, _ := newHarness(t, params, signer)
	o.d.Events = nil
	o.d.Cfg.RetryInterval = 25 * time.Millisecond
	seedGenesisWriterAndVerifiers(t, node)
	driveToQuorum(t, node, f)
	setSNodeConnections(fsn, []string{"s1", "s2"})
	runLoop(t, o)
	waitFor(t, "promotion sent to both SNodes", 3*time.Second, func() bool {
		return slices.Equal(sentTargets(fsn, false), []string{"s1", "s2"})
	})
}

// TestPromotionAndCleanupGoToTheExpectedSourceOnly: with two enrolled,
// connected SNodes, indexer 1's promotion and cleanup reach s1 only (spec §15
// "indexer-a's promotions go only to snode-1").
func TestPromotionAndCleanupGoToTheExpectedSourceOnly(t *testing.T) {
	params, signer := authorityParamsO(t)
	f, node, _, fsn, _, o, _ := newHarness(t, params, signer)
	o.d.Events = nil
	o.d.Cfg.RetryInterval = 25 * time.Millisecond
	seedGenesisWriterAndVerifiers(t, node)
	driveToQuorum(t, node, f) // statement 1 is bound to s1, the only SNode
	enableRegistryViaNode(t, node, f, params, signer)
	activateSignedClaimsO(t, node, f, signer)
	registerSignedSNodeO(t, node, f, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), "s2", 1)
	setSNodeConnections(fsn, []string{"s1", "s2"})
	runLoop(t, o)
	waitFor(t, "promotion streamed", 3*time.Second, func() bool { return hasSignedPromotion(fsn) })
	if got := sentTargets(fsn, false); !slices.Equal(got, []string{"s1"}) {
		t.Fatalf("promotion sent to %v, want only s1", got)
	}
	ackSignedPromotionViaNode(t, node, f, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0), "s1", 1)
	o.Poke()
	waitFor(t, "cleanup streamed", 3*time.Second, func() bool { return hasCleanup(fsn) })
	if got := sentTargets(fsn, true); !slices.Equal(got, []string{"s1"}) {
		t.Fatalf("cleanup sent to %v, want only s1", got)
	}
	if got := sentTargets(fsn, false); !slices.Equal(got, []string{"s1"}) {
		t.Fatalf("promotion resent to %v, want only s1", got)
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && go test ./orchestrator/ -run 'BroadcastBeforeActivation|ExpectedSourceOnly' -v`
Expected: `TestPromotionIsBroadcastBeforeActivation` PASSES (unchanged behaviour); `TestPromotionAndCleanupGoToTheExpectedSourceOnly` FAILS (`promotion sent to [s1 s2], want only s1`).

- [ ] **Step 6: Route promotions and cleanups**

In `orchestrator/promotion.go`, add after `streamPromotionSigned`:

```go
// promotionTargets lists the SNodes a promotion or cleanup for promotionSeq is
// sent to (spec 2026-10-10 §6.6). Before the signed-claims activation it is
// every connected SNode, as before. After it, only the expected source (the
// bound source of the statements the promotion covers), whether or not it is
// connected: no other SNode holds the table, and a send to a disconnected
// source fails and is retried on the next pass. When the FSM cannot name the
// source nothing is sent.
func (o *Loop) promotionTargets(promotionSeq uint64) []string {
	node, routed, err := o.d.FSM.PromotionSource(promotionSeq)
	switch {
	case err != nil:
		o.d.Logger.Warn("promotion work not sent: no expected source", "promotion_seq", promotionSeq, "err", err)
		return nil
	case !routed:
		return o.d.SNodes.Connected()
	default:
		return []string{node}
	}
}
```

and replace `for _, id := range o.d.SNodes.Connected() {` with `for _, id := range o.promotionTargets(cmd.PromotionSeq) {` in `streamPromotionSigned` (`orchestrator/promotion.go:179`) and in `streamCleanupSigned` (`orchestrator/cleanup.go:74`).

- [ ] **Step 7: Run the tests**

Run: `cd "$W" && bazel test //orchestrator:all //fsm:all`
Expected: all PASS, including `TestPromotion_FullChainToCleanup`, `TestPromotion_ResendIssuedUnacked` and `TestPromotion_ResendsScheduledCleanupAfterMissedStream` (pre-activation broadcast).

- [ ] **Step 8: Commit**

```bash
cd "$W" && git add orchestrator && git commit -m "feat(orchestrator): send promotions and cleanups to the expected source only

After the signed-claims activation a promotion and its cleanup go only
to the bound source of the statements the promotion covers; before it
every connected SNode still receives them (housegate spec 2026-10-10
§6.6).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 10: arbiter — snapshot v19 (S1)

**Files:**
- Modify: `fsm/snapshot.go` (version comment 20-33 and constants 34-53; `snapshotDoc` 60-91; `snapshotWriteVersion` 174-195; `snapshotDocument` 197-219; the four version chains in `readSnapshot` at 248-249, 266, 275 and 382; a restore block after the lanes block 419-432; new helpers after `tableRegistryHasV17Fields` 583-595)
- Modify: `fsm/signed_claims.go` (`validateSignedClaimsState`)
- Modify: `fsm/snapshot_test.go:255` (`snapshotVersion != 18` becomes `!= 19`), `fsm/artifact_capacity_test.go:207` (add `snapshotVersionV18` to the version list)
- Create: `fsm/snapshot_signed_claims_test.go`

**Interfaces:**
- Consumes: Task 8 `State.RegistrationSeqs` and the fixtures `signedClaimsFSM`, `snodeRegistration`, and Task 5's `indexerSigner2`; Tasks 5–6 `Params.SIIndexers` / `.Verifiers` (deep-copied by `cloneParams` / `normalizeConsensusParams`), `TableIncarnation.OwnerIndexerID`, `TableRegistryState.SeededIndexers`, the owner migration (rule 8), the bootstrap refusal Task 5 adds to `fsm/consensus_history.go` (si_indexers and verifiers "are set only by a consensus update, not at genesis"), and Task 6's `validateRegistryOwnership`, which `validateTableRegistry` runs on every restore (owners name enrolled indexers, `seeded_indexers` sorted, unique and consistent with `Seeded`); existing `rewriteSnapshotDocument` (`fsm/apply_artifact_disposition_test.go:721`), `keepSnapshotDocument` (`fsm/snapshot_lanes_test.go:18`), `restoreErr`, `restoreInto` (`fsm/snapshot_test.go:44`), `restoreConfigured` (`fsm/table_set_transition_restore_test.go:31`), `snapshotBytes`, `seededRegistryFSM`, `lanesFSM`, `registerActive`, `CountSnapshotEncoding` (it encodes `snapshotDocument`, so the new key is counted without a change there).
- Produces: `snapshotVersionV19 = 19` (`snapshotVersion = 19`); snapshot key `registration_seqs`; `func tableRegistryHasV19Fields(reg *TableRegistryState) bool`; `func signedClaimsFieldsPresent(doc *snapshotDoc, reg *TableRegistryState) bool`; `func validateSignedClaimsState(st *State) error`; restore refusals `signed-claims state is present in a pre-v19 container`, `verifiers are set without si_indexers`, `registration_seqs are recorded before the signed-claims activation`, `registration_seqs[...] is zero`, `registration_seqs names unregistered node ...`, `registration_seqs[...] = ... differs from the node's registration_seq ...`, `node ... carries registration_seq ... that registration_seqs does not record`; test helpers `registryDoc`, `firstIncarnation`, `signedClaimsKeys`.

Notes:
- **Bytes before activation.** Every new member is `omitempty` (`Params.si_indexers` / `verifiers`, `registration_seqs`, `owner_indexer_id`, `seeded_indexers`, `NodeRegistration.registration_seq`) and nothing writes them before the activation, so a pre-activation document is byte-identical and the writer keeps v14–v18. Task 5's frozen golden (`TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `fsm/testdata/pre_signed_claims_golden.json`, which also pins `snapshot_version` 18), `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` and `TestLaneFreeContainersKeepTheirVersionAndRestore` must pass unchanged.
- **Older-binary refusal.** A pre-v19 binary's acceptance chain (`readSnapshot` line 248 of that release) ends at 18, so it refuses a v19 container before parsing it ("unsupported snapshot version 19"). `TestSnapshotRefusesTheNextVersion` pins the mechanism one version up, and Task 12 Step 5 runs the real previous release against a real v19 container.
- **No committed state-digest tool.** arbiter main (`5ea60c8`, `cmd/` holds only `arbiter`, `arbiter-admin`, `arbiter-anchor`, `arbiter-snode`, `arbiter-verifier`, `internal`) has no `cmd/statedigest`; this task's in-repo proof of pre-activation byte identity is Task 5's golden plus `TestSnapshotStaysPreV19UntilActivation`. The offline replay check of the live voters' logs uses the throwaway `statedigest` that Task 12 builds exactly as the runbook does.
- **Before Task 10** an activated state is written as v18 with the new members (Tasks 5–9 only run in tests, and no arbiter release may be cut before Task 12); after it, v19. Only the pre-activation golden asserts a written version, and it stays v18.

- [ ] **Step 1: Write the failing tests**

Create `fsm/snapshot_signed_claims_test.go`:

```go
package fsm

import (
	"bytes"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func registryDoc(doc map[string]any) map[string]any { return doc["table_registry"].(map[string]any) }

func firstIncarnation(doc map[string]any) map[string]any {
	return registryDoc(doc)["incarnations"].([]any)[0].(map[string]any)
}

// signedClaimsKeys are the JSON members only a v19 container may carry.
var signedClaimsKeys = []string{`"registration_seqs"`, `"si_indexers"`, `"verifiers"`, `"owner_indexer_id"`, `"seeded_indexers"`, `"registration_seq"`}

// TestSnapshotStaysPreV19UntilActivation: until si_indexers is committed the
// writer keeps v14..v18 byte for byte, with none of the new members, and each
// of those containers restores to the identical bytes.
func TestSnapshotStaysPreV19UntilActivation(t *testing.T) {
	lanes, _, _ := lanesFSM(t, 256)
	for name, tc := range map[string]struct {
		f    *FSM
		want byte
	}{
		"registry disabled": {newTestFSM(t), snapshotVersionV14},
		"registry seeded":   {seededRegistryFSM(t), snapshotVersionV15},
		"lanes active":      {lanes, snapshotVersionV18},
	} {
		b := snapshotBytes(t, tc.f)
		if b[4] != tc.want {
			t.Fatalf("%s: version byte = %d, want %d", name, b[4], tc.want)
		}
		for _, key := range signedClaimsKeys {
			if bytes.Contains(b, []byte(key)) {
				t.Fatalf("%s: a pre-activation container carries %s", name, key)
			}
		}
		if got := snapshotBytes(t, restoreInto(t, b)); !bytes.Equal(got, b) {
			t.Fatalf("%s: restore ∘ snapshot is not the identity", name)
		}
	}
}

// TestSnapshotV19AfterActivationRoundTrips: the activation alone selects v19;
// the container carries the lists, the owners, the seeded set and the
// sequences, restores (bare and through the configured production path,
// which re-verifies the signed activation) and re-encodes identically; the
// capacity counter mirrors it.
func TestSnapshotV19AfterActivationRoundTrips(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	mustApply(t, f, snodeRegistration(t, f, indexerSigner2(t), "s2", 1))
	b := snapshotBytes(t, f)
	if b[4] != snapshotVersionV19 {
		t.Fatalf("version byte = %d, want 19", b[4])
	}
	restored := restoreInto(t, b)
	if restored.st.RegistrationSeqs["s2"] != 1 || !restored.SignedClaimsActive() || len(restored.st.Params.Verifiers) != 3 ||
		restored.st.TableRegistry.Live("db.old").OwnerIndexerID == nil {
		t.Fatalf("restored seqs %v params %+v", restored.st.RegistrationSeqs, restored.st.Params)
	}
	if !bytes.Equal(snapshotBytes(t, restored), b) {
		t.Fatal("restore ∘ snapshot is not the identity")
	}
	if _, err := restoreConfigured(t, f, b); err != nil {
		t.Fatalf("configured restore: %v", err)
	}
	dims, err := CountSnapshotEncoding(f.st)
	if err != nil {
		t.Fatal(err)
	}
	if n := binary.BigEndian.Uint64(b[5:13]); dims.JSONBytes != n {
		t.Fatalf("counted %d document bytes, container holds %d", dims.JSONBytes, n)
	}
}

// TestPreV19ContainerCarryingSignedClaimsIsRefused: any v19 member under an
// older version byte is a forged field, not a migration, each one alone and
// even when empty.
func TestPreV19ContainerCarryingSignedClaimsIsRefused(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	forged := rewriteSnapshotDocument(t, snapshotBytes(t, f), snapshotVersionV18, keepSnapshotDocument)
	if err := restoreErr(t, forged); err == nil || !strings.Contains(err.Error(), "pre-v19") {
		t.Fatalf("v19 relabelled v18: %v", err)
	}
	base := seededRegistryFSM(t)
	registerActive(t, base, "s1", arbiter.NodeRoleSNode)
	plain := snapshotBytes(t, base)
	for name, mutate := range map[string]func(map[string]any){
		"empty registration_seqs": func(doc map[string]any) { doc["registration_seqs"] = map[string]any{} },
		"si_indexers":             func(doc map[string]any) { doc["params"].(map[string]any)["si_indexers"] = []any{} },
		"verifiers":               func(doc map[string]any) { doc["params"].(map[string]any)["verifiers"] = []any{} },
		"bootstrap verifiers":     func(doc map[string]any) { doc["bootstrap_params"].(map[string]any)["verifiers"] = []any{} },
		"seeded_indexers":         func(doc map[string]any) { registryDoc(doc)["seeded_indexers"] = []any{1} },
		"owner_indexer_id":        func(doc map[string]any) { firstIncarnation(doc)["owner_indexer_id"] = 1 },
		"node registration_seq": func(doc map[string]any) {
			doc["nodes"].(map[string]any)["s1"].(map[string]any)["registration"].(map[string]any)["registration_seq"] = 7
		},
	} {
		for _, version := range []byte{snapshotVersionV15, snapshotVersionV17, snapshotVersionV18} {
			forged := rewriteSnapshotDocument(t, plain, version, mutate)
			if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), "pre-v19") {
				t.Fatalf("%s under v%d: readSnapshot = %v, want a pre-v19 refusal", name, version, err)
			}
		}
	}
}

// TestSignedClaimsStateNeedsTheActivation: under the v19 byte too, no
// signed-claims state may exist without si_indexers.
func TestSignedClaimsStateNeedsTheActivation(t *testing.T) {
	base := seededRegistryFSM(t)
	registerActive(t, base, "s1", arbiter.NodeRoleSNode)
	plain := snapshotBytes(t, base)
	for name, tc := range map[string]struct {
		mutate func(map[string]any)
		want   string
	}{
		"registration_seqs": {func(doc map[string]any) {
			doc["registration_seqs"] = map[string]any{"s1": 7}
			doc["nodes"].(map[string]any)["s1"].(map[string]any)["registration"].(map[string]any)["registration_seq"] = 7
		}, "registration_seqs are recorded before the signed-claims activation"},
		"verifiers": {func(doc map[string]any) {
			doc["params"].(map[string]any)["verifiers"] = []any{map[string]any{"node_id": "v1", "ed25519_pubkey": "AQ=="}}
		}, "verifiers are set without si_indexers"},
	} {
		forged := rewriteSnapshotDocument(t, plain, snapshotVersionV19, tc.mutate)
		if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("%s: readSnapshot = %v, want %q", name, err, tc.want)
		}
	}
}

// TestRegistrationSeqsMustAgreeWithTheNodes: registration_seqs is redundant
// with the registrations it records and checked against them exactly.
func TestRegistrationSeqsMustAgreeWithTheNodes(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	mustApply(t, f, snodeRegistration(t, f, indexerSigner2(t), "s2", 1))
	b := snapshotBytes(t, f)
	for name, mutate := range map[string]func(map[string]any){
		"inflated": func(doc map[string]any) { doc["registration_seqs"].(map[string]any)["s2"] = 2 },
		"zero":     func(doc map[string]any) { doc["registration_seqs"].(map[string]any)["s2"] = 0 },
		"stranger": func(doc map[string]any) { doc["registration_seqs"].(map[string]any)["s7"] = 1 },
		"missing":  func(doc map[string]any) { delete(doc, "registration_seqs") },
	} {
		forged := rewriteSnapshotDocument(t, b, snapshotVersionV19, mutate)
		if err := restoreErr(t, forged); err == nil || !strings.Contains(err.Error(), "registration_seq") {
			t.Fatalf("%s: restore = %v, want a registration_seq refusal", name, err)
		}
	}
}

// TestBootstrapParamsNeverCarrySignedClaims: Task 5's bootstrap refusal holds
// on the restore path of a v19 container too.
func TestBootstrapParamsNeverCarrySignedClaims(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	forged := rewriteSnapshotDocument(t, snapshotBytes(t, f), snapshotVersionV19, func(doc map[string]any) {
		doc["bootstrap_params"].(map[string]any)["verifiers"] = doc["params"].(map[string]any)["verifiers"]
	})
	if err := restoreErr(t, forged); err == nil || !strings.Contains(err.Error(), "not at genesis") {
		t.Fatalf("restore = %v", err)
	}
}

// TestSnapshotVersionChainsCoverV18AndV19: every version chain in readSnapshot
// names v18 and v19 now that snapshotVersion moved on.
func TestSnapshotVersionChainsCoverV18AndV19(t *testing.T) {
	lanes, _, _ := lanesFSM(t, 256)
	activated, _ := signedClaimsFSM(t)
	for _, tc := range []struct {
		version byte
		data    []byte
	}{
		{snapshotVersionV18, snapshotBytes(t, lanes)},
		{snapshotVersionV19, snapshotBytes(t, activated)},
	} {
		forged := rewriteSnapshotDocument(t, tc.data, tc.version, func(doc map[string]any) { delete(doc, "bootstrap_params") })
		if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), "missing bootstrap consensus params") {
			t.Fatalf("v%d without bootstrap_params: readSnapshot = %v", tc.version, err)
		}
	}
}

// TestSnapshotRefusesTheNextVersion: the mechanism every older binary applies
// to a v19 container, one version up (Task 12 Step 5 runs the real previous
// release against a v19 container).
func TestSnapshotRefusesTheNextVersion(t *testing.T) {
	f, _ := signedClaimsFSM(t)
	next := rewriteSnapshotDocument(t, snapshotBytes(t, f), snapshotVersionV19+1, keepSnapshotDocument)
	if err := restoreErr(t, next); err == nil || !strings.Contains(err.Error(), "unsupported snapshot version 20") {
		t.Fatalf("restore = %v", err)
	}
}
```

Run: `cd "$W" && go test ./fsm/ -run 'PreV19|V19|SignedClaimsStateNeeds|RegistrationSeqsMustAgree|BootstrapParamsNeverCarry|ChainsCoverV18AndV19|RefusesTheNextVersion' -v`
Expected: build failure (`undefined: snapshotVersionV19`).

- [ ] **Step 2: Implement v19**

`fsm/snapshot.go`:
- Constants: `snapshotVersion = 19`, and add `snapshotVersionV19 = 19` above `snapshotVersionV18 = 18`. Rewrite the first sentence of the `snapshotVersion` comment as "writeSnapshot writes it only once Params.SIIndexers is committed (the signed-claims activation, housegate spec 2026-10-10 §6.7); before that it writes snapshotVersionV18 once Params.ClientLanes is committed (client_seq lanes, housegate spec 2026-10-09 §5.4), snapshotVersionV17 once …", and add "or v19's si_indexers, verifiers, registration_seqs, incarnation owners and seeded indexers" to the list of fields an older binary would drop.
- `snapshotDoc` (after `LaneCounts`):

```go
	// RegistrationSeqs is State.RegistrationSeqs (v19): the last
	// registration_seq applied per node after the signed-claims activation.
	// It is redundant with the registrations and checked against them.
	RegistrationSeqs map[string]uint64 `json:"registration_seqs,omitempty"`
```

- `snapshotDocument`: add `RegistrationSeqs: st.RegistrationSeqs,`.
- `snapshotWriteVersion` gets a new first branch:

```go
// snapshotWriteVersion is snapshotVersion (v19) once si_indexers is committed,
// snapshotVersionV18 once client lanes are committed, snapshotVersionV17 once
// any incarnation carries a v17 field, snapshotVersionV16 once any sealed
// block carries a table-set transition, snapshotVersionV15 while the table
// registry is enabled without either, and snapshotVersionV14 while it is
// disabled; see snapshotVersion.
func snapshotWriteVersion(st *State) byte {
	if len(st.Params.SIIndexers) > 0 {
		return snapshotVersionV19
	}
	if st.Params.ClientLanes != nil {
		return snapshotVersionV18
	}
	// ... unchanged ...
```

- `readSnapshot`: `git grep -n 'ver\[0\] == snapshotVersion ||' fsm/snapshot.go` lists the four chains (the acceptance check at 248, the allowance-object check at 266, the bootstrap-params check at 275, the artifact-disposition restore at 382). Add `ver[0] == snapshotVersionV18 ||` right after `ver[0] == snapshotVersion ||` in each (in the acceptance check the inequality form `ver[0] != snapshotVersionV18 &&` after `ver[0] != snapshotVersion &&`), and add `snapshotVersionV18` before `snapshotVersion` in the acceptance error's argument list with one more `%d`. A v18 container keeps restoring exactly as before (`TestSnapshotV18AfterActivationRoundTrips`, `TestSnapshotVersionChainsCoverV17AndV18`).
- After `validateReplicatedStatementIDLanes` (lines 430-432), before the v16 block:

```go
	// Signed claims (multi-source SI, spec 2026-10-10 §6.7) were added in v19,
	// written only once si_indexers is committed. An older container cannot
	// have committed any of it, and a pre-v19 binary would drop it on its next
	// snapshot, so any of it under an older version byte is a forged field,
	// not a migration. A present registration_seqs member, even an empty one,
	// is refused: no pre-v19 writer emits the key. This runs before
	// validateRestoredState, so the refusal names the version, not an owner.
	if ver[0] < snapshotVersionV19 && signedClaimsFieldsPresent(&doc, st.TableRegistry) {
		return nil, fmt.Errorf("signed-claims state is present in a pre-v19 container")
	}
	if doc.RegistrationSeqs != nil {
		st.RegistrationSeqs = doc.RegistrationSeqs
	}
	if err := validateSignedClaimsState(st); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
```

(`st` comes from `newState(doc.Params)` at line 296, so `RegistrationSeqs` is never nil when the key is absent.)

- After `tableRegistryHasV17Fields` (583-595):

```go
// tableRegistryHasV19Fields reports whether the registry carries a field only
// a v19 container may hold: an incarnation owner or the seeded-indexer set.
func tableRegistryHasV19Fields(reg *TableRegistryState) bool {
	if reg == nil {
		return false
	}
	if reg.SeededIndexers != nil {
		return true
	}
	for _, inc := range reg.Incarnations {
		if inc != nil && inc.OwnerIndexerID != nil {
			return true
		}
	}
	return false
}

// signedClaimsFieldsPresent reports any member only a v19 container may hold.
func signedClaimsFieldsPresent(doc *snapshotDoc, reg *TableRegistryState) bool {
	if doc.Params.SIIndexers != nil || doc.Params.Verifiers != nil || doc.RegistrationSeqs != nil {
		return true
	}
	if b := doc.BootstrapParams; b != nil && (b.SIIndexers != nil || b.Verifiers != nil) {
		return true
	}
	for _, node := range doc.Nodes {
		if node != nil && node.Registration.RegistrationSeq != 0 {
			return true
		}
	}
	return tableRegistryHasV19Fields(reg)
}
```

Append to `fsm/signed_claims.go`:

```go
// validateSignedClaimsState checks the signed-claims state a restored
// container carries outside the table registry (Task 6's
// validateRegistryOwnership owns incarnation owners and seeded indexers) and
// outside the consensus history (Task 5 replays the lists' transitions and
// refuses them in the bootstrap params): before the activation neither
// verifiers nor registration sequences exist, and afterwards
// registration_seqs agrees exactly with the registrations it records. Keys
// are visited in sorted order so every replica names the same corruption.
func validateSignedClaimsState(st *State) error {
	if len(st.Params.SIIndexers) == 0 {
		switch {
		case st.Params.Verifiers != nil:
			return fmt.Errorf("verifiers are set without si_indexers")
		case len(st.RegistrationSeqs) != 0:
			return fmt.Errorf("registration_seqs are recorded before the signed-claims activation")
		}
	}
	for _, id := range slices.Sorted(maps.Keys(st.RegistrationSeqs)) {
		seq, node := st.RegistrationSeqs[id], st.Nodes[id]
		switch {
		case seq == 0:
			return fmt.Errorf("registration_seqs[%s] is zero", id)
		case node == nil:
			return fmt.Errorf("registration_seqs names unregistered node %s", id)
		case node.Registration.RegistrationSeq != seq:
			return fmt.Errorf("registration_seqs[%s] = %d differs from the node's registration_seq %d", id, seq, node.Registration.RegistrationSeq)
		}
	}
	for _, id := range slices.Sorted(maps.Keys(st.Nodes)) {
		node := st.Nodes[id]
		if node != nil && node.Registration.RegistrationSeq != 0 && st.RegistrationSeqs[id] != node.Registration.RegistrationSeq {
			return fmt.Errorf("node %s carries registration_seq %d that registration_seqs does not record", id, node.Registration.RegistrationSeq)
		}
	}
	return nil
}
```

Existing assertions that pin the current version: `fsm/snapshot_test.go:255` becomes `if data[4] != snapshotVersionV14 || snapshotVersion != 19 {`; `fsm/artifact_capacity_test.go:207` lists `snapshotVersionV17, snapshotVersionV18, snapshotVersion` so v18 keeps its duplicate-allowance coverage.

- [ ] **Step 3: Run the suite**

Run: `cd "$W" && bazel test //fsm:all //server:all //cmd/... && git diff --exit-code -- fsm/testdata`
Expected: all PASS; `TestDeterminism_SameLogSameState` still asserts `snapshotVersionV17` for its script; Task 7's `TestDeterminism_SourceBindingAcrossActivation` (both replicas now write v19) and Task 6's `TestValidateRegistryOwnership` pass; both frozen goldens pass and no file under `fsm/testdata` changes; every `snapshot_lanes_test.go` case passes.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add fsm && git commit -m "feat(snapshot): v19 container only after the signed-claims activation

Carries si_indexers, verifiers, registration_seqs, incarnation owners
and the seeded-indexer set; until si_indexers is committed the writer
keeps v14-v18 byte for byte. Restore refuses any of the new state under
an older version byte, verifiers or registration sequences without
si_indexers, and registration sequences that disagree with the
registrations (housegate spec 2026-10-10 §6.7).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 11: arbiter — signed-claims activation gate and arbiter-admin (S1)

**Files:**
- Modify: `fsm/signed_claims.go` (`ActivationEvictions`); Create: `fsm/activation_evictions_test.go`
- Create: `server/signed_claims_gate.go`, `server/signed_claims_gate_test.go`
- Modify: `server/consensus_admin.go` (`UpdateConsensusParams` 83-102: the gate after `authorizeConsensusParamsUpdate` at 91-93; `consensusMutableParams` already carries both lists, Task 5)
- Modify: `cmd/arbiter-admin/main.go` (usage 42-76; `run` 95: the `node` group before the `consensus` check at 100; flags 122-136; the `Set` bools at 170 and the `--voter` rule at 201-203; the update path between the client-lanes block (ends 319) and `NormalizeConsensusParamsUpdate` (320-327), the re-check at 336-342 and the post-commit warning at 371-373; the gate functions 429-650)
- Create: `cmd/arbiter-admin/signed_claims_test.go`

**Interfaces:**
- Consumes: `PROTO_TAG` `pb.ConsensusMutableParams.{SiIndexers,Verifiers}`, `pb.SIIndexerEntry`, `pb.VerifierEntry`, `pb.EvictNodeRequest`, `pb.ConsensusAdminClient.EvictNode`; `CORE_TAG` `arbiter.{SignedClaimsFeature, LocalNodeFeatures, SIIndexerEntry.Validate, VerifierEntry.Validate, EvictNodeCommand}`, `authority.{NormalizeConsensusParamsUpdate, VerifySNodeEnrollment, SNodeEnrollmentStatement, EvictNodePurpose, EvictNodeHash, JWSCommandPayload, ConsensusContext}`, `(*authority.Signer).{SignSNodeEnrollment, SignEvictNodeWithContext}`, `wire.{SIIndexerEntriesFromPB, VerifierEntriesFromPB, EvictNodeToRequest}`, `(wire.EvictNode).Canonical()`, `authoritytest.{MustSigner, IndexerKeyHex0, IndexerKeyHex1}`; Task 5 `consensusMutableParams` with both lists, the test helpers `claimsFSM`, `claimsUpdate`, `foundingEntry`, `testVerifiers`, and the activation rules as amended by contract §3c (the activation's lists are authoritative: its own Apply evicts every registered non-evicted SNODE it does not name and every registered non-evicted VERIFIER it does not list, reason `not named by the signed-claims activation`; a listed verifier registered under another key, or a named SNode registered with another role, refuses the update); Task 8 `SignedClaimsActive`, the `EvictNode` RPC and the server fixtures `serverEnrolledEntry`, `applyServerUpdate`, `enableServerTableRegistry`, `serverTestAuthority`; existing `featureBook.answer` (`server/features.go:138`), `raftMembership` / `termReader` (`:172-180`), `voterNode` / `newVoterNode` (`server/features_test.go:24-45`), `verifierRequest` (`:55`); arbiter-admin `fakeAdmin`, `serveAdmin`, `refused`, `laneUpdateArgs`, `voterArgs`, `gateTestKey` (`cmd/arbiter-admin/client_lanes_test.go`), `adminKey`, `startAdminServer`, `adminTestServer`, `testConsensusState` (`cmd/arbiter-admin/main_test.go`), `captureStderr`, `lanesIntegrationNode`, `integrationConsensusNode`.
- Produces: fsm `func (f *FSM) ActivationEvictions(entries []arbiter.SIIndexerEntry, verifiers []arbiter.VerifierEntry) []string`; server `const EvictUnnamedMetadataKey = "arbiter-evict-unnamed"`, `func evictUnnamedConsent(ctx context.Context) []string`, `func (s *Server) signedClaimsGate(ctx context.Context, update arbiter.ConsensusParamsUpdate) error`; arbiter-admin `type activationGate struct{ feature, name, aware string; bound func(nodeID string) bool; evictUnnamed bool }`, `clientLanesActivation`, `signedClaimsGateFor(entries, verifiers, evictUnnamed) activationGate`, `runActivationGate`, `recheckActivationGate`, `warnGateAfterCommit`, `observeGate` (renamed `observeClientLanes`), `(*gateObservation).problems(g activationGate)`, `(*gateObservation).unnamed(g activationGate) []string`, `const evictUnnamedMetadataKey`, `type siIndexerFlags []arbiter.SIIndexerEntry`, `type verifierFlags []arbiter.VerifierEntry`, `parseSIIndexerFlag`, `keyValues`, `decodeVerifierPubkey`, `checkSIIndexers`, `runNode`; CLI `consensus update … [--si-indexer 'id=N,activation=BLOCK,signer=0x…,node=ID,enrollment=PATH' …] [--verifier 'node=ID,pubkey=HEX' …] [--evict-unnamed] [--voter …]` and `node evict --address LEADER --node ID --expected-registration-seq N --reason TEXT [--timeout 10s]`; test helpers `gateVerifierPub`, `gateServer`, `serverActivationRequest`, `consentContext` (server), `claimsGateFixture`, `claimsUpdateArgs`, `writeEnrollment`, `serveAdminImpl`, `consentAdmin`, `evictAdmin`, `decodeJWSPayload`, `evictArgs` (arbiter-admin).

Rules:
- **The gate (contract §3, §3a, §3c) runs in two places, as the client-lanes gate does.** arbiter-admin probes every voter's `GetProtocolInfo` directly and reads the leader's feature book; the leader refuses, in `UpdateConsensusParams`, an update that first sets `si_indexers` unless (1) the Raft configuration has no non-voter, (2) the operator accepted exactly the evictions the activation would make, and (3) every registered, non-evicted SNode and verifier that stays (the purge node set minus those evictions) advertised `signed_claims_v1` to this leader in the current term. For (2) the leader computes the eviction set with `fsm.ActivationEvictions` (every registered non-evicted SNODE the update does not name and every registered non-evicted VERIFIER it does not list, the rule Task 5's Apply evicts by, `TestActivationEvictionsMatchApply`); when it is non-empty the request must carry gRPC metadata `arbiter-evict-unnamed` naming exactly that set, which `arbiter-admin --evict-unnamed` sends. A changed set is refused, so the operator accepts the nodes they reviewed and no others. The metadata is an operator interlock, not authority: the authority token signs the update, which alone determines the evictions in Apply; no proto field is added. The leader cannot probe other voters (it knows their Raft ids and transport addresses, not their administration endpoints), so the voter half stays in the CLI (Contract conflicts C2). Both halves are leader-local and never replicated, and an update that does not first set `si_indexers` skips both.
- **CLI lists:** `--si-indexer` and `--verifier` give the complete lists; omitted, the committed lists are carried (the FSM refuses an update that drops them). Before submitting, the CLI verifies every enrolment statement locally against this network and genesis, requires the activation to restate the founding indexer (`table_registry.si_indexer_id`, `table_registry.activation_block`) and to list at least three verifiers, refuses `--verifier` while no `si_indexers` exist and none are given, and prints a stderr reminder for every appended entry: the arbiter requires its `activation_block` to exceed `max(registry cursor, founding activation_block - 1)` and the watcher seeds it when the cursor completes `activation_block - 1` (contract §3a), so choose at least 1800 L2 blocks past the L2 safe head. arbiter-admin has no L2 endpoint, and with `consensus_admin.listen` set its address carries ConsensusAdmin alone (`cmd/arbiter/grpc.go:20-38`), so it cannot read the registry cursor either: the bound is enforced by the FSM and the margin is a documented manual check (README, Task 12; runbook `docs/storage-integrity-devnet2-multi-source.md`, Plan S1-B Task 16). `pubkey=` takes 64 hex characters (contract) or the standard base64 that `consensus show` prints for a bytes field. For the activation the CLI lists every data-plane node in the leader's feature book that the update neither names nor lists, i.e. the nodes its Apply would evict (§3c), refuses to submit unless `--evict-unnamed` is given when that list is non-empty, and then sends the list as the consent metadata. The book carries ids, not roles; in the rare case where the leader's role-aware set differs (a node registered with both roles, say), the leader refuses and names its exact set, and the operator adjusts the lists. `--evict-unnamed` is accepted only together with `--si-indexer`; the nodes it evicts need not run a signed-claims binary.
- **`node evict`:** the key comes from `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` only (there is no key flag); the authority context comes from the leader's barriered `GetConsensusParams`; the server refuses it before the activation (Task 8). `--expected-registration-seq` is the node's committed sequence, a millisecond-floored counter (§3a), 0 for a node that has not registered since the activation; the FSM's refusal names the committed value.

- [ ] **Step 1: Write the failing FSM and server tests**

Create `fsm/activation_evictions_test.go` (Task 5's `claimsFSM`, `claimsUpdate`, `foundingEntry`, `testVerifiers`, and the existing `registerActive`, `signedConsensusUpdate`, `mustApply`):

```go
package fsm

import (
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestActivationEvictionsMatchApply: the gate's read names exactly the nodes
// the activation's own Apply evicts (contract §3c), so the operator accepts
// the evictions that happen and no others.
func TestActivationEvictionsMatchApply(t *testing.T) {
	f, a := claimsFSM(t)
	registerActive(t, f, "s9", arbiter.NodeRoleSNode)    // a stale SNode no indexer names
	registerActive(t, f, "v4", arbiter.NodeRoleVerifier) // a verifier the update does not list
	entries, verifiers := []arbiter.SIIndexerEntry{foundingEntry(t, f)}, testVerifiers("v1", "v2", "v3")
	want := f.ActivationEvictions(entries, verifiers)
	if !slices.Equal(want, []string{"s9", "v4"}) {
		t.Fatalf("ActivationEvictions = %v, want [s9 v4]", want)
	}
	mustApply(t, f, signedConsensusUpdate(t, a, claimsUpdate(t, f, entries, verifiers)))
	var evicted []string
	for id, n := range f.st.Nodes {
		if n.Status != NodeEvicted {
			continue
		}
		evicted = append(evicted, id)
		if n.Reason != "not named by the signed-claims activation" {
			t.Fatalf("%s evicted with reason %q", id, n.Reason)
		}
	}
	slices.Sort(evicted)
	if !slices.Equal(evicted, want) {
		t.Fatalf("Apply evicted %v, the gate predicted %v", evicted, want)
	}
	if got := f.ActivationEvictions(entries, verifiers); len(got) != 0 {
		t.Fatalf("after the activation nothing is left to evict, got %v", got)
	}
}
```

Create `server/signed_claims_gate_test.go`:

```go
package server

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// consentContext is a request context carrying arbiter-admin's
// --evict-unnamed consent for ids.
func consentContext(ids string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(EvictUnnamedMetadataKey, ids))
}

// gateVerifierPub is the key verifierRequest registers for seed.
func gateVerifierPub(seed byte) []byte {
	pub := make([]byte, 32)
	pub[0] = seed
	return pub
}

func gateServer(vn voterNode) *Server {
	return New(Deps{Node: vn, FSM: vn.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second,
		ConsensusUpdatesEnabled: true, ConsensusAdminMaxTokenAge: 5 * time.Minute}})
}

// serverActivationRequest is the signed-claims activation over node's current
// view: founding indexer 1 (SNode s1, signed by founder) and v1..v3 with the
// keys verifierRequest registers, carrying the committed writer limit, signed
// now by serverTestAuthority.
func serverActivationRequest(t *testing.T, node *fakeNode, founder *authority.Signer) *pb.UpdateConsensusParamsCmd {
	t.Helper()
	view, err := node.f.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	u, err := authority.NormalizeConsensusParamsUpdate(arbiter.ConsensusParamsUpdate{
		NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID, ExpectedEpoch: view.Epoch,
		PreviousParamsDigest: view.ParamsDigest, AuthorityAddresses: view.Current.AuthorityAddresses, MaxWriters: max(view.Current.MaxWriters, 1),
		ExpectedPromotionSeq: view.PromotionSeq, TableRegistry: view.Current.TableRegistry,
		SIIndexers: []arbiter.SIIndexerEntry{serverEnrolledEntry(t, node.f, founder, 1, 100, "s1")},
		Verifiers: []arbiter.VerifierEntry{{NodeID: "v1", Ed25519Pubkey: gateVerifierPub(1)}, {NodeID: "v2", Ed25519Pubkey: gateVerifierPub(2)},
			{NodeID: "v3", Ed25519Pubkey: gateVerifierPub(3)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := serverTestAuthority.SignConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.UpdateConsensusParamsCmd{Update: wire.ConsensusParamsUpdateToPB(u), AuthorityJws: token}
}

// TestSignedClaimsActivationNeedsEveryDataPlaneNodeInTheFeatureBook: the
// leader-local half of the gate refuses before proposing while a registered
// data-plane node has not advertised signed_claims_v1 to it.
func TestSignedClaimsActivationNeedsEveryDataPlaneNodeInTheFeatureBook(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	s := gateServer(vn)
	enableServerTableRegistry(t, vn.fakeNode)
	ctx := context.Background()
	m := &membershipService{s: s}
	register := func(req *pb.NodeRegistration) {
		t.Helper()
		if _, err := m.RegisterNode(ctx, req); err != nil {
			t.Fatalf("register %s: %v", req.GetNodeId(), err)
		}
	}
	both := arbiter.LocalNodeFeatures()
	register(&pb.NodeRegistration{NodeId: "s1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}, Features: both})
	register(verifierRequest("v1", 1, both...))
	register(verifierRequest("v2", 2, both...))
	register(verifierRequest("v3", 3, arbiter.ClientLanesFeature)) // still a pre-S1 verifier
	admin := &consensusAdminService{s: s}
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	before := vn.appliedCount()
	_, err := admin.UpdateConsensusParams(ctx, serverActivationRequest(t, vn.fakeNode, founder))
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "v3") || !strings.Contains(err.Error(), arbiter.SignedClaimsFeature) {
		t.Fatalf("activation with a pre-S1 verifier = %v", err)
	}
	if vn.appliedCount() != before || vn.f.SignedClaimsActive() {
		t.Fatal("the refused activation was proposed")
	}
	register(verifierRequest("v3", 3, both...))
	if _, err := admin.UpdateConsensusParams(ctx, serverActivationRequest(t, vn.fakeNode, founder)); err != nil {
		t.Fatalf("activation once every node advertises signed_claims_v1 = %v", err)
	}
	if !vn.f.SignedClaimsActive() {
		t.Fatal("signed claims not active")
	}
}

// TestSignedClaimsGateNeedsConsentToEvictUnnamedNodes: contract §3c. The
// activation evicts the registered SNode s9 it does not name; the leader
// refuses before proposing unless the request accepts exactly that eviction,
// and s9, which is being evicted, need not advertise signed_claims_v1.
func TestSignedClaimsGateNeedsConsentToEvictUnnamedNodes(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	s := gateServer(vn)
	applyServerUpdate(t, vn.fakeNode, func(u *arbiter.ConsensusParamsUpdate) { u.MaxWriters = 2 })
	m := &membershipService{s: s}
	both := arbiter.LocalNodeFeatures()
	for _, req := range []*pb.NodeRegistration{
		{NodeId: "s1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}, Features: both},
		{NodeId: "s9", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}, Features: []string{arbiter.ClientLanesFeature}}, // a stale pre-S1 SNode
		verifierRequest("v1", 1, both...), verifierRequest("v2", 2, both...), verifierRequest("v3", 3, both...),
	} {
		if _, err := m.RegisterNode(context.Background(), req); err != nil {
			t.Fatalf("register %s: %v", req.GetNodeId(), err)
		}
	}
	admin := &consensusAdminService{s: s}
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	before := vn.appliedCount()
	for name, ctx := range map[string]context.Context{"no consent": context.Background(), "another set": consentContext("s8")} {
		_, err := admin.UpdateConsensusParams(ctx, serverActivationRequest(t, vn.fakeNode, founder))
		if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "would evict [s9]") || !strings.Contains(err.Error(), "--evict-unnamed") {
			t.Fatalf("%s: activation evicting s9 = %v", name, err)
		}
	}
	if vn.appliedCount() != before {
		t.Fatal("a refused activation was proposed")
	}
	if _, err := admin.UpdateConsensusParams(consentContext("s9"), serverActivationRequest(t, vn.fakeNode, founder)); err != nil {
		t.Fatalf("activation with the exact consent = %v", err)
	}
	if !vn.f.SignedClaimsActive() || slices.Contains(vn.f.PurgeNodeSet(), "s9") {
		t.Fatalf("active %v, purge node set %v; want s9 evicted", vn.f.SignedClaimsActive(), vn.f.PurgeNodeSet())
	}
}

// TestSignedClaimsGateRefusesWhileANonVoterExists: a non-voter applies every
// command but cannot be proven signed-claims aware.
func TestSignedClaimsGateRefusesWhileANonVoterExists(t *testing.T) {
	vn := newVoterNode(t, "arb-0")
	vn.nonvoters = []string{"arb-7"}
	s := gateServer(vn)
	enableServerTableRegistry(t, vn.fakeNode)
	_, err := (&consensusAdminService{s: s}).UpdateConsensusParams(context.Background(),
		serverActivationRequest(t, vn.fakeNode, authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)))
	if status.Code(err) != codes.FailedPrecondition || !strings.Contains(err.Error(), "arb-7") {
		t.Fatalf("err = %v", err)
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && go test ./fsm/ -run TestActivationEvictionsMatchApply -v; go test ./server/ -run 'SignedClaimsActivationNeeds|SignedClaimsGateNeedsConsent|SignedClaimsGateRefuses' -v`
Expected: build failures (`f.ActivationEvictions undefined`, `undefined: EvictUnnamedMetadataKey`); with those stubbed, FAIL — the activation with a pre-S1 verifier and the one evicting `s9` without consent reach the FSM and commit.

- [ ] **Step 2: Implement the FSM read and the server half**

Append to `fsm/signed_claims.go`:

```go
// ActivationEvictions lists, sorted, the registered non-evicted nodes the
// signed-claims activation naming entries and listing verifiers would evict
// in its own Apply (contract §3c): every SNODE whose id is no entry's
// snode_node_id and every VERIFIER whose id verifiers does not list. It reads
// committed state only and is an admission aid for the leader's activation
// gate; Apply decides the same set again (TestActivationEvictionsMatchApply).
func (f *FSM) ActivationEvictions(entries []arbiter.SIIndexerEntry, verifiers []arbiter.VerifierEntry) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	var out []string
	for id, n := range f.st.Nodes {
		if n == nil || n.Status == NodeEvicted {
			continue
		}
		roles := n.Registration.Roles
		named := slices.ContainsFunc(entries, func(e arbiter.SIIndexerEntry) bool { return e.SNodeNodeID == id })
		listed := slices.ContainsFunc(verifiers, func(v arbiter.VerifierEntry) bool { return v.NodeID == id })
		if (hasRole(roles, arbiter.NodeRoleSNode) && !named) || (hasRole(roles, arbiter.NodeRoleVerifier) && !listed) {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}
```

Create `server/signed_claims_gate.go`:

```go
package server

import (
	"context"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
)

// EvictUnnamedMetadataKey is the gRPC metadata key under which arbiter-admin
// --evict-unnamed names, comma-separated, the nodes the operator accepts the
// signed-claims activation evicting (contract §3c). It is an operator
// interlock, not authority: the authority token signs the update, and the
// update alone determines the evictions in Apply.
const EvictUnnamedMetadataKey = "arbiter-evict-unnamed"

// evictUnnamedConsent is the sorted node set the request accepts to evict.
func evictUnnamedConsent(ctx context.Context) []string {
	md, _ := metadata.FromIncomingContext(ctx)
	var ids []string
	for _, v := range md.Get(EvictUnnamedMetadataKey) {
		for _, id := range strings.Split(v, ",") {
			if id = strings.TrimSpace(id); id != "" {
				ids = append(ids, id)
			}
		}
	}
	slices.Sort(ids)
	return slices.Compact(ids)
}

// signedClaimsGate is the leader-local half of the signed-claims activation
// gate (spec 2026-10-10 §6.7, contract §3c; the client-lanes feature book,
// reused). An update that first sets si_indexers is refused unless the Raft
// configuration has no non-voter, which no gate could vouch for; the request
// accepts exactly the nodes the activation's Apply would evict (registered
// but not named by it, fsm.ActivationEvictions); and every other registered,
// non-evicted SNode and verifier advertised signed_claims_v1 in a
// registration applied through this leader in the current term. A node being
// evicted need not: it may be a stale registration whose host is gone. The
// voter half, every voter reporting signed_claims_v1 through its own
// GetProtocolInfo, runs in arbiter-admin: a leader knows its peers' Raft
// addresses, not their administration endpoints. Node features and the
// consent are never replicated, so this is an admission decision only; Apply
// cannot see it.
func (s *Server) signedClaimsGate(ctx context.Context, update arbiter.ConsensusParamsUpdate) error {
	if len(update.SIIndexers) == 0 || s.d.FSM.SignedClaimsActive() {
		return nil
	}
	membership, ok := s.d.Node.(raftMembership)
	if !ok {
		return status.Error(codes.FailedPrecondition, "signed claims gate refused: raft configuration is unavailable on this node")
	}
	terms, ok := s.d.Node.(termReader)
	if !ok {
		return status.Error(codes.FailedPrecondition, "signed claims gate refused: raft term is unavailable on this node")
	}
	_, nonvoters, err := membership.RaftServerIDs()
	if err != nil {
		return status.Errorf(codes.Unavailable, "signed claims gate: read raft configuration: %v", err)
	}
	if len(nonvoters) > 0 {
		return status.Errorf(codes.FailedPrecondition, "signed claims gate refused: raft configuration has non-voting servers %s; promote or remove them first", strings.Join(nonvoters, ", "))
	}
	evict := s.d.FSM.ActivationEvictions(update.SIIndexers, update.Verifiers)
	if consent := evictUnnamedConsent(ctx); !slices.Equal(evict, consent) {
		return status.Errorf(codes.FailedPrecondition, "signed claims gate refused: the activation would evict [%s] (registered but not named by it), and --evict-unnamed accepted [%s]; review the list and re-run arbiter-admin with --evict-unnamed",
			strings.Join(evict, " "), strings.Join(consent, " "))
	}
	term := terms.CurrentTerm()
	var missing []string
	for _, id := range s.d.FSM.PurgeNodeSet() {
		if slices.Contains(evict, id) {
			continue // evicted by the activation itself
		}
		if e, ok := s.features.answer(id, term); !ok || !slices.Contains(e.features, arbiter.SignedClaimsFeature) {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return status.Errorf(codes.FailedPrecondition, "signed claims gate refused: data-plane nodes %s have not registered with this leader advertising %s; restart them on a signed-claims release",
			strings.Join(missing, ", "), arbiter.SignedClaimsFeature)
	}
	return nil
}
```

(`PurgeNodeSet` is every registered, non-evicted SNode and verifier, sorted, before and after the activation, contract §3b.5.)

`server/consensus_admin.go` — in `UpdateConsensusParams`, after `authorizeConsensusParamsUpdate` (91-93):

```go
	if err := svc.s.signedClaimsGate(ctx, update); err != nil {
		return nil, err
	}
```

Run: `cd "$W" && go test ./fsm/ -run TestActivationEvictionsMatchApply -v && bazel test //fsm:all //server:all`
Expected: all PASS. `TestActivationEvictionsMatchApply` also checks Task 5's §3c Apply (the evicted set and its reason).

- [ ] **Step 3: Write the failing arbiter-admin tests**

Create `cmd/arbiter-admin/signed_claims_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"

	"github.com/sentioxyz/arbiter/fsm"
	"github.com/sentioxyz/arbiter/server"
)

// writeEnrollment writes signer's enrolment statement for (network, genesis,
// indexer, node) to a file, newline-terminated as an operator's shell would.
func writeEnrollment(t *testing.T, signer *authority.Signer, network, genesis string, indexer uint64, node string) string {
	t.Helper()
	jws, err := signer.SignSNodeEnrollment(authority.SNodeEnrollmentStatement{NetworkID: network, GenesisSnapshotID: genesis, IndexerID: indexer, SNodeNodeID: node})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), fmt.Sprintf("enrollment-%d-%s.jws", indexer, node))
	if err := os.WriteFile(path, []byte(jws+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// claimsGateFixture is gateFixture for the signed-claims activation: three
// voters and a feature book (s1, v1..v3) reporting both features, a table
// registry founded by indexer 0 at block 100 (devnet2's shape) and no lists
// yet, plus an enrolment file for indexer 0's SNode s1 signed with
// authoritytest key 0.
func claimsGateFixture(t *testing.T) (leader *fakeAdmin, leaderAddr string, voterAddrs []string, founder *authority.Signer, enrollment string) {
	t.Helper()
	t.Setenv(authorityPrivateKeyEnv, gateTestKey)
	both := []string{arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature}
	state := &pb.ConsensusParamsState{
		ProtocolVersion: arbiter.ConsensusAdminProtocolVersion, NetworkId: "devnet2", GenesisSnapshotId: "0xgenesis",
		Epoch: 1, ParamsDigest: "0xdigest", PromotionSeq: 3,
		Current: &pb.ConsensusMutableParams{AuthorityAddresses: []string{"0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705"}, MaxWriters: 1,
			TableRegistry: &pb.TableRegistryParams{ChainId: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1", ActivationBlock: 100, Confirmation: "safe"}},
	}
	book := &pb.NodeFeatures{RaftVoterIds: []string{"arb-0", "arb-1", "arb-2"}}
	for _, id := range []string{"s1", "v1", "v2", "v3"} {
		book.Entries = append(book.Entries, &pb.NodeFeatureEntry{NodeId: id, Features: both, RegisteredUnix: 1})
	}
	for i, id := range []string{"arb-0", "arb-1", "arb-2"} {
		f := &fakeAdmin{nodeID: id, features: both, state: state, book: book}
		addr := serveAdmin(t, f)
		voterAddrs = append(voterAddrs, addr)
		if i == 0 {
			leader, leaderAddr = f, addr
		}
	}
	founder = authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	return leader, leaderAddr, voterAddrs, founder, writeEnrollment(t, founder, "devnet2", "0xgenesis", 0, "s1")
}

func claimsUpdateArgs(addr string, founder *authority.Signer, enrollment string, voters []string) []string {
	args := laneUpdateArgs(addr,
		"--si-indexer", fmt.Sprintf("id=0,activation=100,signer=%s,node=s1,enrollment=%s", founder.Address(), enrollment),
		"--verifier", "node=v1,pubkey="+strings.Repeat("01", 32),
		"--verifier", "node=v2,pubkey="+strings.Repeat("02", 32),
		"--verifier", "node=v3,pubkey="+base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{3}, 32)))
	return append(args, voterArgs(voters)...)
}

func TestSignedClaimsActivationPassesTheGate(t *testing.T) {
	leader, addr, voters, founder, enrollment := claimsGateFixture(t)
	if err := run(context.Background(), claimsUpdateArgs(addr, founder, enrollment, voters), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	update := leader.updates[0].GetUpdate()
	entries := update.GetSiIndexers()
	if len(entries) != 1 || entries[0].GetSnodeNodeId() != "s1" || entries[0].GetSigner() != founder.Address() ||
		entries[0].GetEnrollmentJws() == "" || strings.ContainsAny(entries[0].GetEnrollmentJws(), "\n ") || len(update.GetVerifiers()) != 3 {
		t.Fatalf("submitted update = %v", update)
	}
	if got := update.GetVerifiers()[2].GetEd25519Pubkey(); !bytes.Equal(got, bytes.Repeat([]byte{3}, 32)) {
		t.Fatalf("base64 pubkey decoded to %x", got)
	}
}

func TestSignedClaimsGateRefusesAVoterWithoutTheFeature(t *testing.T) {
	leader, addr, voters, founder, enrollment := claimsGateFixture(t)
	voters[2] = serveAdmin(t, &fakeAdmin{nodeID: "arb-2", features: []string{arbiter.ClientLanesFeature}, state: leader.state, book: leader.book})
	refused(t, leader, claimsUpdateArgs(addr, founder, enrollment, voters), "signed claims gate refused", "arb-2", arbiter.SignedClaimsFeature)
}

func TestSignedClaimsGateRefusesADataPlaneNodeWithoutTheFeature(t *testing.T) {
	leader, addr, voters, founder, enrollment := claimsGateFixture(t)
	leader.book.Entries[3].Features = []string{arbiter.ClientLanesFeature} // v3 still on a pre-S1 release
	refused(t, leader, claimsUpdateArgs(addr, founder, enrollment, voters), "restart: v3", "signed-claims-aware")
}

// TestSignedClaimsGateRefusesToEvictWithoutConsent: contract §3c, the
// activation evicts every registered node it does not name; without
// --evict-unnamed the CLI names the node and submits nothing.
func TestSignedClaimsGateRefusesToEvictWithoutConsent(t *testing.T) {
	leader, addr, voters, founder, enrollment := claimsGateFixture(t)
	leader.book.Entries = append(leader.book.Entries, &pb.NodeFeatureEntry{NodeId: "s9",
		Features: []string{arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature}, RegisteredUnix: 1})
	refused(t, leader, claimsUpdateArgs(addr, founder, enrollment, voters), "data-plane node s9 is neither an --si-indexer node= nor a --verifier node=", "--evict-unnamed")
}

// consentAdmin is fakeAdmin that records the --evict-unnamed consent each
// UpdateConsensusParams request carries.
type consentAdmin struct {
	*fakeAdmin
	consents []string
}

func (f *consentAdmin) UpdateConsensusParams(ctx context.Context, req *pb.UpdateConsensusParamsCmd) (*pb.Ack, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	f.mu.Lock()
	f.consents = append(f.consents, strings.Join(md.Get(evictUnnamedMetadataKey), ","))
	f.mu.Unlock()
	return f.fakeAdmin.UpdateConsensusParams(ctx, req)
}

// TestSignedClaimsActivationEvictsUnnamedNodesWithConsent: with
// --evict-unnamed the CLI lists the node the activation will evict, does not
// require it to run a signed-claims binary, and sends exactly that list as the
// leader's consent metadata.
func TestSignedClaimsActivationEvictsUnnamedNodesWithConsent(t *testing.T) {
	leader, _, voters, founder, enrollment := claimsGateFixture(t)
	leader.book.Entries = append(leader.book.Entries, &pb.NodeFeatureEntry{NodeId: "s9",
		Features: []string{arbiter.ClientLanesFeature}, RegisteredUnix: 1}) // a stale pre-S1 SNode
	consent := &consentAdmin{fakeAdmin: leader}
	addr := serveAdminImpl(t, consent)
	notices := captureStderr(t)
	if err := run(context.Background(), append(claimsUpdateArgs(addr, founder, enrollment, voters), "--evict-unnamed"), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if len(consent.consents) != 1 || consent.consents[0] != "s9" {
		t.Fatalf("consent metadata = %q, want [s9]", consent.consents)
	}
	if !strings.Contains(notices.String(), "will evict s9") {
		t.Fatalf("stderr = %q", notices.String())
	}
}

func TestEvictUnnamedMetadataKeyMatchesTheServer(t *testing.T) {
	if evictUnnamedMetadataKey != server.EvictUnnamedMetadataKey {
		t.Fatalf("CLI sends %q, the leader reads %q", evictUnnamedMetadataKey, server.EvictUnnamedMetadataKey)
	}
}

func TestSignedClaimsActivationNeedsVoterAddresses(t *testing.T) {
	leader, addr, _, founder, enrollment := claimsGateFixture(t)
	refused(t, leader, claimsUpdateArgs(addr, founder, enrollment, nil), "--voter")
}

// TestUpdateCarriesSIIndexersAndVerifiersForward: an unrelated update after
// the activation carries both lists unchanged and runs no gate; the FSM
// refuses an update that drops them.
func TestUpdateCarriesSIIndexersAndVerifiersForward(t *testing.T) {
	leader, addr, _, founder, enrollment := claimsGateFixture(t)
	jws, err := os.ReadFile(enrollment)
	if err != nil {
		t.Fatal(err)
	}
	leader.state.Current.SiIndexers = []*pb.SIIndexerEntry{{IndexerId: 0, ActivationBlock: 100, Signer: founder.Address(), SnodeNodeId: "s1", EnrollmentJws: strings.TrimSpace(string(jws))}}
	leader.state.Current.Verifiers = []*pb.VerifierEntry{{NodeId: "v1", Ed25519Pubkey: bytes.Repeat([]byte{1}, 32)},
		{NodeId: "v2", Ed25519Pubkey: bytes.Repeat([]byte{2}, 32)}, {NodeId: "v3", Ed25519Pubkey: bytes.Repeat([]byte{3}, 32)}}
	if err := run(context.Background(), laneUpdateArgs(addr), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	update := leader.updates[0].GetUpdate()
	if len(update.GetSiIndexers()) != 1 || update.GetSiIndexers()[0].GetEnrollmentJws() != strings.TrimSpace(string(jws)) || len(update.GetVerifiers()) != 3 {
		t.Fatalf("an update without --si-indexer/--verifier dropped the lists: %v", update)
	}
	if leader.bookReads.Load() != 0 {
		t.Fatal("a carry consulted the feature book")
	}
}

// TestAppendingAnIndexerWarnsAboutTheActivationBlock: enrolment runs no
// feature gate, and the CLI can see neither the L2 safe head nor the registry
// cursor, so it reminds.
func TestAppendingAnIndexerWarnsAboutTheActivationBlock(t *testing.T) {
	leader, addr, _, founder, enrollment := claimsGateFixture(t)
	jws, _ := os.ReadFile(enrollment)
	leader.state.Current.SiIndexers = []*pb.SIIndexerEntry{{IndexerId: 0, ActivationBlock: 100, Signer: founder.Address(), SnodeNodeId: "s1", EnrollmentJws: strings.TrimSpace(string(jws))}}
	leader.state.Current.Verifiers = []*pb.VerifierEntry{{NodeId: "v1", Ed25519Pubkey: bytes.Repeat([]byte{1}, 32)},
		{NodeId: "v2", Ed25519Pubkey: bytes.Repeat([]byte{2}, 32)}, {NodeId: "v3", Ed25519Pubkey: bytes.Repeat([]byte{3}, 32)}}
	second := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
	warnings := captureStderr(t)
	args := laneUpdateArgs(addr, "--max-writers", "2",
		"--si-indexer", fmt.Sprintf("id=0,activation=100,signer=%s,node=s1,enrollment=%s", founder.Address(), enrollment),
		"--si-indexer", fmt.Sprintf("id=1,activation=5600000,signer=%s,node=snode-b,enrollment=%s", second.Address(), writeEnrollment(t, second, "devnet2", "0xgenesis", 1, "snode-b")))
	if err := run(context.Background(), args, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if got := len(leader.updates[0].GetUpdate().GetSiIndexers()); got != 2 {
		t.Fatalf("submitted %d entries", got)
	}
	if !strings.Contains(warnings.String(), "indexer 1 activates at L2 block 5600000") || !strings.Contains(warnings.String(), "1800") || strings.Contains(warnings.String(), "indexer 0") {
		t.Fatalf("warnings = %q", warnings.String())
	}
	if leader.bookReads.Load() != 0 {
		t.Fatal("an enrolment ran the activation gate")
	}
}

// TestSIIndexerAndVerifierFlagsAreCheckedLocally: every refusal happens
// before anything is submitted.
func TestSIIndexerAndVerifierFlagsAreCheckedLocally(t *testing.T) {
	leader, addr, voters, founder, enrollment := claimsGateFixture(t)
	other := writeEnrollment(t, founder, "devnet2", "0xgenesis", 0, "s9")
	entry := func(fields string) []string { return []string{"--si-indexer", fields} }
	good := fmt.Sprintf("signer=%s,node=s1,enrollment=%s", founder.Address(), enrollment)
	verifiers := []string{"--verifier", "node=v1,pubkey=" + strings.Repeat("01", 32), "--verifier", "node=v2,pubkey=" + strings.Repeat("02", 32),
		"--verifier", "node=v3,pubkey=" + strings.Repeat("03", 32)}
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"unknown key":   {laneUpdateArgs(addr, entry("id=0,activation=100,color=red,"+good)...), "unknown key \"color\""},
		"repeated key":  {laneUpdateArgs(addr, entry("id=0,id=1,activation=100,"+good)...), "key \"id\" is given twice"},
		"missing key":   {laneUpdateArgs(addr, entry(fmt.Sprintf("id=0,activation=100,node=s1,enrollment=%s", enrollment))...), "missing signer="},
		"bad id":        {laneUpdateArgs(addr, entry("id=x,activation=100,"+good)...), "id \"x\" is not an unsigned integer"},
		"activation 0":  {laneUpdateArgs(addr, entry("id=0,activation=0,"+good)...), "--si-indexer"},
		"bad signer":    {laneUpdateArgs(addr, entry(fmt.Sprintf("id=0,activation=100,signer=0xABC,node=s1,enrollment=%s", enrollment))...), "--si-indexer"},
		"absent file":   {laneUpdateArgs(addr, entry(fmt.Sprintf("id=0,activation=100,signer=%s,node=s1,enrollment=/nonexistent", founder.Address()))...), "enrollment"},
		"bad pubkey":    {laneUpdateArgs(addr, "--verifier", "node=v1,pubkey=zz"), "64 hex characters or base64 of 32 bytes"},
		"verifier only": {laneUpdateArgs(addr, verifiers...), "--verifier needs si_indexers"},
		"statement for another node": {append(laneUpdateArgs(addr, append(entry(fmt.Sprintf("id=0,activation=100,signer=%s,node=s1,enrollment=%s", founder.Address(), other)), verifiers...)...), voterArgs(voters)...),
			"enrolment statement does not verify"},
		"no founding entry": {append(laneUpdateArgs(addr, append(entry("id=5,activation=100,"+good), verifiers...)...), voterArgs(voters)...),
			"must restate the founding indexer"},
		"two verifiers": {append(laneUpdateArgs(addr, append(entry("id=0,activation=100,"+good), verifiers[:4]...)...), voterArgs(voters)...),
			"at least 3"},
		"voter without a list": {laneUpdateArgs(addr, voterArgs(voters)...), "--voter is only used with --client-lanes-max-per-account or --si-indexer"},
		"evict without a list": {laneUpdateArgs(addr, "--evict-unnamed"), "--evict-unnamed is only used with --si-indexer"},
	} {
		t.Run(name, func(t *testing.T) { refused(t, leader, tc.args, tc.want) })
	}
}

func TestConsensusShowPrintsSIIndexersAndVerifiers(t *testing.T) {
	t.Setenv(authorityPrivateKeyEnv, "")
	state := testConsensusState()
	state.Current.SiIndexers = []*pb.SIIndexerEntry{{IndexerId: 0, ActivationBlock: 5508931, Signer: "0x22a67f498e669cbde18a0eecfff74a72c48331f4", SnodeNodeId: "snode-1", EnrollmentJws: "h.p.s"}}
	state.Current.Verifiers = []*pb.VerifierEntry{{NodeId: "v1", Ed25519Pubkey: bytes.Repeat([]byte{1}, 32)}}
	addr := startAdminServer(t, &adminTestServer{get: func(context.Context) (*pb.ConsensusParamsState, error) { return state, nil }})
	var out bytes.Buffer
	if err := run(context.Background(), []string{"consensus", "show", "--address", addr}, &out); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"si_indexers"`, `"snode-1"`, `"5508931"`, `"verifiers"`, `"v1"`} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("show output lacks %s:\n%s", want, out.String())
		}
	}
}

type evictAdmin struct {
	*fakeAdmin
	evicts []*pb.EvictNodeRequest
	err    error
}

func (f *evictAdmin) EvictNode(_ context.Context, req *pb.EvictNodeRequest) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.evicts = append(f.evicts, req)
	if f.err != nil {
		return nil, f.err
	}
	return &pb.Ack{}, nil
}

// serveAdminImpl is serveAdmin for any ConsensusAdmin implementation.
func serveAdminImpl(t *testing.T, impl pb.ConsensusAdminServer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	pb.RegisterConsensusAdminServer(g, impl)
	go func() { _ = g.Serve(ln) }()
	t.Cleanup(g.Stop)
	return ln.Addr().String()
}

func decodeJWSPayload(t *testing.T, token string) authority.JWSCommandPayload {
	t.Helper()
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d parts", len(parts))
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatal(err)
	}
	var payload authority.JWSCommandPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func evictArgs(addr string, extra ...string) []string {
	return append([]string{"node", "evict", "--address", addr, "--node", "v3", "--expected-registration-seq", "1760000000123", "--reason", "host retired"}, extra...)
}

func TestNodeEvictSignsTheCommandForTheCurrentEpoch(t *testing.T) {
	adminKey(t)
	admin := &evictAdmin{fakeAdmin: &fakeAdmin{nodeID: "arb-0", state: testConsensusState()}}
	addr := serveAdminImpl(t, admin)
	var out bytes.Buffer
	if err := run(context.Background(), evictArgs(addr), &out); err != nil {
		t.Fatal(err)
	}
	req := admin.evicts[0]
	if req.GetNodeId() != "v3" || req.GetExpectedRegistrationSeq() != 1760000000123 || req.GetReason() != "host retired" {
		t.Fatalf("request = %v", req)
	}
	hash, err := authority.EvictNodeHash(arbiter.EvictNodeCommand{NodeID: "v3", ExpectedRegistrationSeq: 1760000000123, Reason: "host retired"})
	if err != nil {
		t.Fatal(err)
	}
	payload := decodeJWSPayload(t, req.GetAuthorityJws())
	if payload.Purpose != authority.EvictNodePurpose || payload.CmdHash != hash || payload.NetworkID != "testnet" ||
		payload.GenesisSnapshotID != "genesis-committed" || payload.AuthorityEpoch == nil || *payload.AuthorityEpoch != 7 {
		t.Fatalf("token payload = %+v", payload)
	}
	if !strings.Contains(out.String(), `"status": "committed"`) || strings.Contains(out.String(), os.Getenv(authorityPrivateKeyEnv)) {
		t.Fatalf("output = %s", out.String())
	}
}

func TestNodeEvictNeedsItsFlagsAndTheEnvironmentKey(t *testing.T) {
	admin := &evictAdmin{fakeAdmin: &fakeAdmin{nodeID: "arb-0", state: testConsensusState()}}
	addr := serveAdminImpl(t, admin)
	adminKey(t)
	for name, tc := range map[string]struct {
		args []string
		want string
	}{
		"no node":      {[]string{"node", "evict", "--address", addr, "--expected-registration-seq", "0", "--reason", "r"}, "--node is required"},
		"no seq":       {[]string{"node", "evict", "--address", addr, "--node", "v3", "--reason", "r"}, "--expected-registration-seq is required"},
		"no reason":    {[]string{"node", "evict", "--address", addr, "--node", "v3", "--expected-registration-seq", "0"}, "--reason is required"},
		"padded node":  {[]string{"node", "evict", "--address", addr, "--node", " v3", "--expected-registration-seq", "0", "--reason", "r"}, "--node"},
		"key flag":     {evictArgs(addr, "--key", "00"), "flag provided but not defined"},
		"no operation": {[]string{"node"}, "usage"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := run(context.Background(), tc.args, &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	t.Setenv(authorityPrivateKeyEnv, "")
	if err := run(context.Background(), evictArgs(addr), &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), authorityPrivateKeyEnv+" is required") {
		t.Fatalf("err = %v", err)
	}
	if len(admin.evicts) != 0 {
		t.Fatal("a refused eviction reached the server")
	}
}

func TestNodeEvictReportsTheServerRefusalAndUnknownOutcomes(t *testing.T) {
	adminKey(t)
	admin := &evictAdmin{fakeAdmin: &fakeAdmin{nodeID: "arb-0", state: testConsensusState()},
		err: status.Error(codes.FailedPrecondition, "eviction requires the signed-claims activation")}
	addr := serveAdminImpl(t, admin)
	if err := run(context.Background(), evictArgs(addr), &bytes.Buffer{}); status.Code(err) != codes.FailedPrecondition || strings.Contains(err.Error(), "outcome may be unknown") {
		t.Fatalf("err = %v", err)
	}
	admin.err = status.Error(codes.Unavailable, "apply: timeout")
	if err := run(context.Background(), evictArgs(addr), &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "outcome may be unknown") {
		t.Fatalf("err = %v", err)
	}
}

// TestCLISignedClaimsActivationThroughServerAndFSM drives the activation
// against the real server (feature book, GetProtocolInfo, RegisterNode, both
// gate halves, the consent metadata) and the production FSM, mirroring
// TestCLIClientLanesActivationThroughServerAndFSM: a pre-S1 verifier and an
// unlisted stale verifier refuse the gate with state untouched; once the
// verifier re-registers, the activation with --evict-unnamed commits and
// evicts the stale one; a later update without the list flags carries both
// lists.
func TestCLISignedClaimsActivationThroughServerAndFSM(t *testing.T) {
	a := adminKey(t)
	params := fsm.Params{NetworkID: "cli-claims-network", SchemaSnapshotID: "cli-schema", ExecutorProfileID: "cli-profile",
		AuthorityAddresses: []string{a.Address()}, MaxWriters: 1}
	state, err := fsm.New(params)
	if err != nil {
		t.Fatal(err)
	}
	executor := payloadexec.New(params.NetworkID, payloadexec.TableSchema{TableID: "db.admin", Columns: []lthash.Column{{Name: "value", Type: "UInt64"}}})
	genesis, err := executor.GenesisSnapshot(0, params.SchemaSnapshotID, params.ExecutorProfileID)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.ConfigureGenesis(genesis); err != nil {
		t.Fatal(err)
	}
	encodedGenesis, err := wire.Encode(wire.Command{PublishSafeSnapshot: &wire.PublishSafeSnapshot{Manifest: genesis}})
	if err != nil {
		t.Fatal(err)
	}
	if result := state.Apply(&raft.Log{Index: 1, Term: 1, Type: raft.LogCommand, Data: encodedGenesis}); result != (fsm.Applied{}) {
		t.Fatalf("publish genesis: %v", result)
	}
	node := &lanesIntegrationNode{integrationConsensusNode: &integrationConsensusNode{state: state, index: 1}}
	node.leader.Store(true)
	node.setVoters("arb-0")
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	transport := grpc.NewServer()
	svc := server.New(server.Deps{Node: node, FSM: state, LeaderAddr: func() string { return "" }, Cfg: server.Config{
		NodeID: "arb-0", ApplyTimeout: time.Second, ConsensusUpdatesEnabled: true, ConsensusAdminMaxTokenAge: 5 * time.Minute,
	}})
	svc.RegisterAll(transport)
	go func() { _ = transport.Serve(lis) }()
	t.Cleanup(func() {
		svc.CloseStreams()
		transport.Stop()
	})
	address := lis.Addr().String()
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	update := func(epoch string, extra ...string) error {
		t.Helper()
		args := append([]string{"consensus", "update", "--address", address, "--expected-epoch", epoch,
			"--authority", a.Address(), "--max-writers", "1"}, extra...)
		return run(context.Background(), args, &bytes.Buffer{})
	}
	if err := update("0", "--table-registry-chain-id", "7892301", "--table-registry-contract", "0x00000000000000000000000000000000000000d1",
		"--table-registry-si-indexer-id", "1", "--table-registry-activation-block", "100", "--table-registry-confirmation", "safe"); err != nil {
		t.Fatalf("enable the registry: %v", err)
	}
	both := arbiter.LocalNodeFeatures()
	register := func(req *pb.NodeRegistration) {
		t.Helper()
		if _, err := pb.NewMembershipClient(conn).RegisterNode(context.Background(), req); err != nil {
			t.Fatalf("register %s: %v", req.GetNodeId(), err)
		}
	}
	register(&pb.NodeRegistration{NodeId: "s1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}, Features: both})
	pubs := map[string][]byte{}
	for i, id := range []string{"v1", "v2", "v3"} {
		pubs[id] = bytes.Repeat([]byte{byte(i + 1)}, 32)
		features := both
		if id == "v3" {
			features = []string{arbiter.ClientLanesFeature}
		}
		register(&pb.NodeRegistration{NodeId: id, Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}, Ed25519Pubkey: pubs[id], Features: features})
	}
	// A stale verifier the activation does not list: it is evicted, so it need
	// not run a signed-claims binary.
	register(&pb.NodeRegistration{NodeId: "v9", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}, Ed25519Pubkey: bytes.Repeat([]byte{9}, 32),
		Features: []string{arbiter.ClientLanesFeature}})
	view, err := state.ConsensusParamsView()
	if err != nil {
		t.Fatal(err)
	}
	founder := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex0)
	enrollment := writeEnrollment(t, founder, view.Current.NetworkID, view.GenesisSnapshotID, 1, "s1")
	activation := []string{"--si-indexer", fmt.Sprintf("id=1,activation=100,signer=%s,node=s1,enrollment=%s", founder.Address(), enrollment),
		"--verifier", "node=v1,pubkey=" + hex.EncodeToString(pubs["v1"]), "--verifier", "node=v2,pubkey=" + hex.EncodeToString(pubs["v2"]),
		"--verifier", "node=v3,pubkey=" + hex.EncodeToString(pubs["v3"]), "--voter", address}
	applies := node.applies.Load()
	if err := update("1", activation...); err == nil || !strings.Contains(err.Error(), "v3") || !strings.Contains(err.Error(), "v9") {
		t.Fatalf("gate must refuse the pre-S1 verifier v3 and the unconsented eviction of v9: %v", err)
	}
	if node.applies.Load() != applies || state.SignedClaimsActive() {
		t.Fatal("a refused gate changed committed state")
	}
	register(&pb.NodeRegistration{NodeId: "v3", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}, Ed25519Pubkey: pubs["v3"], Features: both})
	if err := update("1", append(activation, "--evict-unnamed")...); err != nil {
		t.Fatalf("activation: %v", err)
	}
	if !state.SignedClaimsActive() || slices.Contains(state.PurgeNodeSet(), "v9") {
		t.Fatalf("active %v, purge node set %v; want v9 evicted", state.SignedClaimsActive(), state.PurgeNodeSet())
	}
	if err := update("2", "--max-writers", "2"); err != nil {
		t.Fatalf("a writer-limit update after activation must carry both lists: %v", err)
	}
	if view, err = state.ConsensusParamsView(); err != nil || len(view.Current.SIIndexers) != 1 || len(view.Current.Verifiers) != 3 || view.Current.MaxWriters != 2 {
		t.Fatalf("after the carrying update = %+v, %v", view.Current, err)
	}
}
```

Run: `cd "$W" && bazel run //:gazelle && go test ./cmd/arbiter-admin/ -run 'SignedClaims|EvictUnnamedMetadataKey|CarriesSIIndexers|AppendingAnIndexer|FlagsAreCheckedLocally|ShowPrintsSIIndexers|NodeEvict|CLISignedClaims' -v`
Expected: build failure (`undefined: evictUnnamedMetadataKey`); with it stubbed, FAIL (`flag provided but not defined: -si-indexer`; `node` answers the usage).

- [ ] **Step 4: Implement arbiter-admin**

`cmd/arbiter-admin/main.go`:

Usage (42-76): replace the `update` line and add the `node evict` line:

```
  arbiter-admin consensus update --address LEADER_GRPC --expected-epoch N --authority 0x... [--authority 0x...] --max-writers N [--artifact-disposition-capability 0|1] [--table-registry-chain-id N --table-registry-contract 0x... --table-registry-si-indexer-id N --table-registry-activation-block N --table-registry-confirmation finalized|safe] [--client-lanes-max-per-account N] [--si-indexer 'id=N,activation=BLOCK,signer=0x...,node=NODE_ID,enrollment=PATH' ...] [--verifier 'node=NODE_ID,pubkey=HEX' ...] [--evict-unnamed] [--voter VOTER_GRPC ...] [--timeout 10s]
  arbiter-admin node evict --address LEADER_GRPC --node NODE_ID --expected-registration-seq N --reason TEXT [--timeout 10s]
```

and before "Signing key for update and activate-query-profile" insert:

```
--si-indexer and --verifier give the complete si_indexers and verifiers lists,
one flag per entry; omitted, the committed lists are carried (once set, every
update must carry both). The first update that sets si_indexers is the
irreversible signed-claims activation: it restates the table registry's
founding indexer and activation block, lists every verifier that stays with
its registered key (at least three), and requires --voter for every Raft
voter; every voter must report signed_claims_v1 and every registered verifier
and SNode that stays must have advertised it to the current leader. The
activation evicts every registered SNode it does not name and every
registered verifier it does not list; the CLI lists them and submits only
with --evict-unnamed. enrollment= names a file holding the
indexer signer's arbiter-snode-enrollment-v1 JWS; the CLI verifies it for this
network and genesis before submitting. The arbiter requires a new entry's
activation block to exceed max(registry cursor, founding activation block - 1);
choose one at least 1800 L2 blocks past the L2 safe head. This CLI reads
neither the L2 chain nor the registry cursor, so check that before running the
update. pubkey= accepts 64 hex characters or the base64 consensus show prints.
Node evict signs an authority eviction of one node (signed-claims networks
only). --expected-registration-seq is the node's committed registration_seq
(0 if it has not registered since the activation); a mismatch is refused and
the refusal names the committed value.
```

and change "Signing key for update and activate-query-profile" to "Signing key for update, activate-query-profile and node evict" and "Both tokens are signed" to "Every token is signed".

`run` (95): first statements, before `if len(args) < 2 || args[0] != "consensus"` at 100:

```go
	if len(args) >= 1 && args[0] == "node" {
		return runNode(ctx, args[1:], out)
	}
```

Flags (122-136): next to `var voters authorities` declare `var siIndexers siIndexerFlags`, `var verifiers verifierFlags` and `var evictUnnamed bool`; in the `update` block register

```go
		fs.Var(&siIndexers, "si-indexer", "si_indexers entry id=N,activation=BLOCK,signer=0x...,node=NODE_ID,enrollment=PATH; repeat for the complete list (omitted keeps the committed list)")
		fs.Var(&verifiers, "verifier", "verifiers entry node=NODE_ID,pubkey=HEX; repeat for the complete list (omitted keeps the committed list)")
		fs.BoolVar(&evictUnnamed, "evict-unnamed", false, "accept that the signed-claims activation evicts every registered SNode and verifier it does not name (the CLI lists them)")
```

and change the `--voter` help to "direct gRPC address of a Raft voter; repeat for every voter (required to set or raise client lanes and for the signed-claims activation)". At 170 add `siIndexersSet, verifiersSet` to the `Set` bools; in the validation block set `siIndexersSet, verifiersSet = present["si-indexer"], present["verifier"]` and replace 201-203 with

```go
		if len(voters) != 0 && !clientLanesSet && !siIndexersSet {
			return errors.New("--voter is only used with --client-lanes-max-per-account or --si-indexer")
		}
		if evictUnnamed && !siIndexersSet {
			return errors.New("--evict-unnamed is only used with --si-indexer")
		}
```

Update path: after the client-lanes block (ends at 319), insert

```go
	// Absent, carry the committed lists unchanged: once set, the FSM refuses an
	// update that omits either (spec 2026-10-10 §6.1).
	currentSIIndexers := wire.SIIndexerEntriesFromPB(state.GetCurrent().GetSiIndexers())
	siIndexersTarget, verifiersTarget := currentSIIndexers, wire.VerifierEntriesFromPB(state.GetCurrent().GetVerifiers())
	if siIndexersSet {
		siIndexersTarget = []arbiter.SIIndexerEntry(siIndexers)
	}
	if verifiersSet {
		verifiersTarget = []arbiter.VerifierEntry(verifiers)
	}
	// claimsGate and claimsSeen are set exactly when this update is the
	// signed-claims activation; the observation is re-checked before submit
	// and after commit.
	var claimsGate activationGate
	var claimsSeen *gateObservation
	switch {
	case siIndexersSet:
		if err := checkSIIndexers(state, currentSIIndexers, siIndexersTarget, verifiersTarget, tableRegistryTarget); err != nil {
			return err
		}
		if len(currentSIIndexers) == 0 {
			claimsGate = signedClaimsGateFor(siIndexersTarget, verifiersTarget, evictUnnamed)
			if claimsSeen, err = runActivationGate(ctx, claimsGate, client, []string(voters), dialAdmin); err != nil {
				return err
			}
		}
	case verifiersSet && len(currentSIIndexers) == 0:
		return errors.New("--verifier needs si_indexers: give --si-indexer in the same update (the signed-claims activation) or set them first")
	}
```

pass `SIIndexers: siIndexersTarget, Verifiers: verifiersTarget,` to `authority.NormalizeConsensusParamsUpdate` (320-327); after the client-lanes re-check (336-342) add

```go
	if claimsSeen != nil {
		if claimsSeen, err = recheckActivationGate(ctx, claimsGate, client, []string(voters), dialAdmin, claimsSeen); err != nil {
			return err
		}
		// The re-check passed, so these are the nodes the operator accepted
		// with --evict-unnamed; the leader requires exactly this set
		// (contract §3c) and refuses a different one.
		if unnamed := claimsSeen.unnamed(claimsGate); len(unnamed) > 0 {
			fmt.Fprintf(stderr, "the signed-claims activation will evict %s (registered but not named by it)\n", strings.Join(unnamed, ", "))
			ctx = metadata.AppendToOutgoingContext(ctx, evictUnnamedMetadataKey, strings.Join(unnamed, ","))
		}
	}
```

with, next to `authorityPrivateKeyEnv` (and the import `google.golang.org/grpc/metadata`):

```go
// evictUnnamedMetadataKey carries the --evict-unnamed consent to the leader;
// it equals server.EvictUnnamedMetadataKey
// (TestEvictUnnamedMetadataKeyMatchesTheServer).
const evictUnnamedMetadataKey = "arbiter-evict-unnamed"
```

and after the client-lanes post-commit warning (371-373)

```go
	if claimsSeen != nil {
		warnGateAfterCommit(ctx, *timeout, claimsGate, client, []string(voters), dialAdmin, claimsSeen)
	}
```

Gate refactor (429-650): introduce the parameter and keep the lane wrappers, so every existing lane message and `client_lanes_test.go` (which calls `runClientLanesGate` and `warnAfterCommit`) stay unchanged:

```go
// activationGate names one feature-gated, irreversible consensus activation.
// The client-lanes gate (housegate spec 2026-10-09 §5.6) and the
// signed-claims gate (spec 2026-10-10 §6.7) run the same checks; only the
// feature, the wording and the signed-claims eviction check differ.
type activationGate struct {
	feature string // capability every voter and data-plane node must report
	name    string // prefixes every refusal and warning, e.g. "client lanes"
	aware   string // describes a binary that reports feature, e.g. "lane-aware"
	// bound, when set, reports whether the update names a data-plane node of
	// the leader's feature book (every registered, non-evicted SNode and
	// verifier); the activation evicts every node it does not name
	// (contract §3c).
	bound func(nodeID string) bool
	// evictUnnamed is --evict-unnamed: the operator accepts those evictions.
	evictUnnamed bool
}

var clientLanesActivation = activationGate{feature: arbiter.ClientLanesFeature, name: "client lanes", aware: "lane-aware"}

// signedClaimsGateFor is the signed-claims activation gate for one update.
// Besides the feature checks it lists every data-plane node of the leader's
// feature book that the update neither names as an si_indexers snode_node_id
// nor lists as a verifier: the activation's own Apply evicts those nodes
// (contract §3c), so they need not run a signed-claims binary, and without
// evictUnnamed the gate refuses. The book carries ids, not roles or keys, so
// the leader re-checks the exact set (fsm.ActivationEvictions) and a verifier
// listed under a key other than the one it registered stays the FSM's
// refusal.
func signedClaimsGateFor(entries []arbiter.SIIndexerEntry, verifiers []arbiter.VerifierEntry, evictUnnamed bool) activationGate {
	return activationGate{feature: arbiter.SignedClaimsFeature, name: "signed claims", aware: "signed-claims-aware", evictUnnamed: evictUnnamed,
		bound: func(id string) bool {
			return slices.ContainsFunc(entries, func(e arbiter.SIIndexerEntry) bool { return e.SNodeNodeID == id }) ||
				slices.ContainsFunc(verifiers, func(v arbiter.VerifierEntry) bool { return v.NodeID == id })
		}}
}

// unnamed lists, sorted, the data-plane nodes of the observation the gate's
// update does not name: the nodes the signed-claims activation evicts.
func (o *gateObservation) unnamed(g activationGate) []string {
	if g.bound == nil {
		return nil
	}
	var out []string
	for _, e := range o.entries {
		if !g.bound(e.GetNodeId()) {
			out = append(out, e.GetNodeId())
		}
	}
	slices.Sort(out)
	return out
}

func clientLanesGate(ctx context.Context, leader pb.ConsensusAdminClient, voters []string, dial func(string) (pb.ConsensusAdminClient, func(), error)) error {
	_, err := runActivationGate(ctx, clientLanesActivation, leader, voters, dial)
	return err
}

func runClientLanesGate(ctx context.Context, leader pb.ConsensusAdminClient, voters []string, dial dialFunc) (*gateObservation, error) {
	return runActivationGate(ctx, clientLanesActivation, leader, voters, dial)
}

func recheckClientLanesGate(ctx context.Context, leader pb.ConsensusAdminClient, voters []string, dial dialFunc, gate *gateObservation) (*gateObservation, error) {
	return recheckActivationGate(ctx, clientLanesActivation, leader, voters, dial, gate)
}

func warnAfterCommit(ctx context.Context, timeout time.Duration, leader pb.ConsensusAdminClient, voters []string, dial dialFunc, submitted *gateObservation) {
	warnGateAfterCommit(ctx, timeout, clientLanesActivation, leader, voters, dial, submitted)
}
```

then turn the bodies of today's `runClientLanesGate` (451-465), `recheckClientLanesGate` (467-479) and `warnAfterCommit` (485-505) into `runActivationGate(ctx, g, leader, voters, dial)`, `recheckActivationGate(ctx, g, leader, voters, dial, gate)` and `warnGateAfterCommit(ctx, timeout, g, leader, voters, dial, submitted)`, replacing every literal `client lanes` in their strings with `g.name` (`fmt.Errorf("%s gate refused: pass --voter with the direct gRPC address of every Raft voter", g.name)`, `g.name+" gate refused; nothing was submitted:"`, `fmt.Errorf("%s re-check before submit refused; nothing was submitted: %w", g.name, err)`, `g.name+" re-check before submit refused; the cluster changed since the gate ran; nothing was submitted:"`, `"WARNING: the "+g.name+" update committed (irreversible), …"`, `"info: the "+g.name+" post-commit re-check differs …"`) and `lane-aware` with `g.aware`; rename `observeClientLanes` (535-554) to `observeGate` (its body is feature-independent); and give `problems` (556-608) the gate:

```go
// problems evaluates the gate's voter and data-plane conditions on one
// observation.
func (o *gateObservation) problems(g activationGate) (problems, restart []string) {
	if len(o.configured) == 0 {
		return []string{"the leader reported no Raft voters, so no voter can be proven " + g.aware}, nil
	}
	// ... unchanged, except the voter feature check:
		if !slices.Contains(p.features, g.feature) {
			problems = append(problems, fmt.Sprintf("voter %s (%s) lacks %s; replace it with a %s release", id, p.address, g.feature, g.aware))
			restart = append(restart, id)
		}
	// ...
	for _, e := range o.entries {
		id := e.GetNodeId()
		if g.bound != nil && !g.bound(id) {
			if !g.evictUnnamed {
				problems = append(problems, "data-plane node "+id+" is neither an --si-indexer node= nor a --verifier node=; the activation would evict it: review the list and pass --evict-unnamed to accept")
			}
			continue // evicted by the activation; it need not run a signed-claims binary
		}
		if !slices.Contains(e.GetFeatures(), g.feature) {
			problems = append(problems, "data-plane node "+id+" has not registered with the current leader as "+g.aware+"; restart it on a "+g.aware+" release")
			restart = append(restart, id)
		}
	}
	return problems, restart
}
```

New flag types and checks (after `type authorities`; new imports `encoding/base64`, `encoding/hex`):

```go
// siIndexerFlags collects repeated --si-indexer values in command-line order.
type siIndexerFlags []arbiter.SIIndexerEntry

func (s *siIndexerFlags) String() string { return fmt.Sprint(len(*s)) }
func (s *siIndexerFlags) Set(value string) error {
	entry, err := parseSIIndexerFlag(value)
	if err != nil {
		return err
	}
	*s = append(*s, entry)
	return nil
}

// verifierFlags collects repeated --verifier values in command-line order.
type verifierFlags []arbiter.VerifierEntry

func (v *verifierFlags) String() string { return fmt.Sprint(len(*v)) }
func (v *verifierFlags) Set(value string) error {
	fields, err := keyValues(value, "node", "pubkey")
	if err != nil {
		return fmt.Errorf("--verifier: %w", err)
	}
	pub, err := decodeVerifierPubkey(fields["pubkey"])
	if err != nil {
		return fmt.Errorf("--verifier %s: %w", fields["node"], err)
	}
	entry := arbiter.VerifierEntry{NodeID: fields["node"], Ed25519Pubkey: pub}
	if err := entry.Validate(); err != nil {
		return fmt.Errorf("--verifier %s: %w", fields["node"], err)
	}
	*v = append(*v, entry)
	return nil
}

// parseSIIndexerFlag reads id=N,activation=BLOCK,signer=0x...,node=ID,
// enrollment=PATH (each key exactly once; enrollment names a file holding the
// JWS, surrounding whitespace trimmed).
func parseSIIndexerFlag(value string) (arbiter.SIIndexerEntry, error) {
	fields, err := keyValues(value, "id", "activation", "signer", "node", "enrollment")
	if err != nil {
		return arbiter.SIIndexerEntry{}, fmt.Errorf("--si-indexer: %w", err)
	}
	id, err := strconv.ParseUint(fields["id"], 10, 64)
	if err != nil {
		return arbiter.SIIndexerEntry{}, fmt.Errorf("--si-indexer id %q is not an unsigned integer", fields["id"])
	}
	activation, err := strconv.ParseUint(fields["activation"], 10, 64)
	if err != nil {
		return arbiter.SIIndexerEntry{}, fmt.Errorf("--si-indexer activation %q is not an unsigned integer", fields["activation"])
	}
	jws, err := os.ReadFile(fields["enrollment"])
	if err != nil {
		return arbiter.SIIndexerEntry{}, fmt.Errorf("--si-indexer enrollment: %w", err)
	}
	entry := arbiter.SIIndexerEntry{IndexerID: id, ActivationBlock: activation, Signer: strings.ToLower(fields["signer"]),
		SNodeNodeID: fields["node"], EnrollmentJWS: strings.TrimSpace(string(jws))}
	if err := entry.Validate(); err != nil {
		return arbiter.SIIndexerEntry{}, fmt.Errorf("--si-indexer %s: %w", fields["node"], err)
	}
	return entry, nil
}

// keyValues splits "k=v,k=v" and requires exactly the keys named, once each.
func keyValues(value string, keys ...string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(value, ",") {
		k, v, ok := strings.Cut(part, "=")
		k, v = strings.TrimSpace(k), strings.TrimSpace(v)
		if !ok || k == "" {
			return nil, fmt.Errorf("%q is not key=value", part)
		}
		if !slices.Contains(keys, k) {
			return nil, fmt.Errorf("unknown key %q (want %s)", k, strings.Join(keys, ", "))
		}
		if _, dup := out[k]; dup {
			return nil, fmt.Errorf("key %q is given twice", k)
		}
		out[k] = v
	}
	for _, k := range keys {
		if out[k] == "" {
			return nil, fmt.Errorf("missing %s=", k)
		}
	}
	return out, nil
}

// decodeVerifierPubkey accepts 64 hex characters (optionally 0x-prefixed) or
// the standard base64 that consensus show prints for a bytes field.
func decodeVerifierPubkey(s string) ([]byte, error) {
	if b, err := hex.DecodeString(strings.TrimPrefix(s, "0x")); err == nil && len(b) == 32 {
		return b, nil
	}
	if b, err := base64.StdEncoding.DecodeString(s); err == nil && len(b) == 32 {
		return b, nil
	}
	return nil, errors.New("pubkey must be 64 hex characters or base64 of 32 bytes")
}

// checkSIIndexers is the CLI's early refusal of an si_indexers list the FSM
// would refuse (spec 2026-10-10 §6.1, §6.2): the activation restates the table
// registry's founding indexer and lists at least three verifiers, and every
// enrolment statement verifies under its entry's signer for this network and
// genesis. For every appended entry it reminds the operator of the activation
// block rule, which needs the L2 safe head and the registry cursor, neither
// of which this CLI can read. The FSM re-checks every rule; this only saves a
// refused round.
func checkSIIndexers(state *pb.ConsensusParamsState, current, target []arbiter.SIIndexerEntry, verifiers []arbiter.VerifierEntry, registry *arbiter.TableRegistryParams) error {
	activation := len(current) == 0
	if activation {
		if registry == nil {
			return errors.New("si_indexers require the table registry; enable it first (or in the same update)")
		}
		if !slices.ContainsFunc(target, func(e arbiter.SIIndexerEntry) bool {
			return e.IndexerID == registry.SIIndexerID && e.ActivationBlock == registry.ActivationBlock
		}) {
			return fmt.Errorf("the signed-claims activation must restate the founding indexer: --si-indexer id=%d,activation=%d,...", registry.SIIndexerID, registry.ActivationBlock)
		}
		if len(verifiers) < 3 {
			return fmt.Errorf("the signed-claims activation must list every registered verifier with --verifier (at least 3, got %d)", len(verifiers))
		}
	}
	known := map[uint64]bool{}
	for _, e := range current {
		known[e.IndexerID] = true
	}
	for _, e := range target {
		stmt := authority.SNodeEnrollmentStatement{NetworkID: state.GetNetworkId(), GenesisSnapshotID: state.GetGenesisSnapshotId(), IndexerID: e.IndexerID, SNodeNodeID: e.SNodeNodeID}
		if err := authority.VerifySNodeEnrollment(stmt, e.EnrollmentJWS, e.Signer); err != nil {
			return fmt.Errorf("--si-indexer id=%d: enrolment statement does not verify under signer %s for network %s: %w", e.IndexerID, e.Signer, state.GetNetworkId(), err)
		}
		founding := activation && registry != nil && e.IndexerID == registry.SIIndexerID
		if !known[e.IndexerID] && !founding {
			fmt.Fprintf(stderr, "WARNING: indexer %d activates at L2 block %d. The arbiter requires it to exceed max(registry cursor, founding activation_block - 1), and the watcher seeds the indexer when the cursor completes block %d. Before submitting, confirm the block is at least 1800 L2 blocks past the L2 safe head; this CLI reads neither the L2 chain nor the registry cursor.\n",
				e.IndexerID, e.ActivationBlock, e.ActivationBlock-1)
		}
	}
	return nil
}
```

`runNode` (after `activateQueryProfile`):

```go
// runNode is the `node` command group: authority-signed membership
// operations. `node evict` is its only operation.
func runNode(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "evict" {
		return errors.New(usage)
	}
	fs := flag.NewFlagSet("node evict", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	address := fs.String("address", "", "leader gRPC host:port")
	timeout := fs.Duration("timeout", 10*time.Second, "timeout for the entire operation")
	node := fs.String("node", "", "required node id to evict")
	expected := fs.Uint64("expected-registration-seq", 0, "required: the node's committed registration_seq (0 if it has not registered since the activation)")
	reason := fs.String("reason", "", "required reason, recorded in replicated state")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			if _, err := fmt.Fprintln(out, usage); err != nil {
				return err
			}
			fs.SetOutput(out)
			fs.PrintDefaults()
			return nil
		}
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	present := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { present[f.Name] = true })
	for _, name := range []string{"address", "node", "expected-registration-seq", "reason"} {
		if !present[name] {
			return fmt.Errorf("--%s is required", name)
		}
	}
	if host, port, err := net.SplitHostPort(*address); err != nil || host == "" || port == "" {
		return errors.New("--address must be a direct node host:port")
	}
	if *node == "" || strings.TrimSpace(*node) != *node {
		return errors.New("--node must be a node id without surrounding whitespace")
	}
	if strings.TrimSpace(*reason) == "" {
		return errors.New("--reason must not be empty")
	}
	if *timeout <= 0 {
		return errors.New("--timeout must be positive")
	}
	keyHex := os.Getenv(authorityPrivateKeyEnv)
	if keyHex == "" {
		return fmt.Errorf("%s is required", authorityPrivateKeyEnv)
	}
	signer, err := authority.NewSignerFromHex(keyHex)
	if err != nil {
		// Do not copy key material or parser fragments into operator logs.
		return fmt.Errorf("%s is invalid: expected a 32-byte secp256k1 private key in hex", authorityPrivateKeyEnv)
	}
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()
	conn, err := grpc.NewClient("passthrough:///"+*address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", *address, err)
	}
	defer conn.Close()
	client := pb.NewConsensusAdminClient(conn)
	// The authority context comes from the leader's barriered view: Apply
	// refuses a token bound to any other network, genesis or epoch.
	state, err := client.GetConsensusParams(ctx, &emptypb.Empty{})
	if err != nil {
		return rpcError("read consensus parameters", *address, err)
	}
	if state.GetProtocolVersion() != arbiter.ConsensusAdminProtocolVersion {
		return fmt.Errorf("unsupported consensus admin protocol %d (this CLI supports %d)", state.GetProtocolVersion(), arbiter.ConsensusAdminProtocolVersion)
	}
	evict := wire.EvictNode{NodeID: *node, Reason: *reason, ExpectedRegistrationSeq: *expected}
	token, err := signer.SignEvictNodeWithContext(evict.Canonical(), authority.ConsensusContext{
		NetworkID: state.GetNetworkId(), GenesisSnapshotID: state.GetGenesisSnapshotId(), AuthorityEpoch: state.GetEpoch()})
	if err != nil {
		return fmt.Errorf("sign node eviction: %w", err)
	}
	evict.AuthorityJWS = token
	if _, err := client.EvictNode(ctx, wire.EvictNodeToRequest(evict)); err != nil {
		wrapped := rpcError("evict node", *address, err)
		if _, ok := leaderHint(err); ok {
			// The server also uses NotLeader for raft.ErrLeadershipLost,
			// whose Apply outcome can already have committed.
			return fmt.Errorf("%w; commit outcome may be unknown: run consensus features against the current leader before retrying", wrapped)
		}
		switch status.Code(err) {
		case codes.DeadlineExceeded, codes.Canceled, codes.Unavailable, codes.Unknown, codes.Internal:
			return fmt.Errorf("%w; commit outcome may be unknown: run consensus features against the current leader before retrying", wrapped)
		default:
			return wrapped
		}
	}
	result := struct {
		Status                  string `json:"status"`
		NodeID                  string `json:"node_id"`
		ExpectedRegistrationSeq string `json:"expected_registration_seq"`
		Reason                  string `json:"reason"`
	}{"committed", *node, strconv.FormatUint(*expected, 10), *reason}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(result)
}
```

`consensus show` prints the whole `ConsensusParamsState` with `EmitUnpopulated`, so once Task 5's `consensusMutableParams` copies the lists, `current.si_indexers` and `current.verifiers` appear without CLI changes; `TestConsensusShowPrintsSIIndexersAndVerifiers` pins it.

- [ ] **Step 5: Run the tests**

Run: `cd "$W" && bazel test //cmd/arbiter-admin:all //server:all //fsm:all`
Expected: all PASS, including `TestActivationEvictionsMatchApply`, every `client_lanes_test.go` case with its unchanged messages, `TestCLIClientLanesActivationThroughServerAndFSM`, `main_test.go` and `consensus_integration_test.go`. If gazelle reports missing deps for the new test imports, run `bazel run //:gazelle` and re-run.

- [ ] **Step 6: Commit**

```bash
cd "$W" && git add fsm server cmd/arbiter-admin && git commit -m "feat(arbiter-admin): signed-claims activation gate, si_indexers/verifiers flags, node evict

consensus update takes the complete si_indexers and verifiers lists
(carried when omitted), verifies enrolment statements locally, and gates
the first si_indexers update on every voter and every data-plane node
that stays reporting signed_claims_v1 (the client-lanes gate,
parameterized); nodes the activation does not name are listed and
evicted by it only with --evict-unnamed, whose exact set the leader
re-checks through gRPC metadata. node evict signs an authority eviction
from the environment key; consensus show prints both lists (housegate
spec 2026-10-10 §6.2, §6.5, §6.7; contract §3c).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 12: arbiter — documentation, full verification, release ARBITER_TAG and image digest (S1)

**Files:**
- Modify: `README.md` (new section after "## Client lanes" (326-346), before "## Artifact disposition admission" (348); extend the "Snapshot versions" sentence at line 324)
- Local only, never committed, with `R="$HOME/si-devnet2-multisource-rehearsal"`: `$R/statedigest/main.go` (the lanes rollout's tool, copied verbatim), `$R/v19check/main.go`, the source exports `$R/src/{old,main,new}` (`git archive`, no worktree, as the lanes rollout did), the binaries `$R/bin/{old,main,new}/{statedigest,v19check}`, `$R/sample/` and `$R/v19.container`

**Interfaces:**
- Consumes: Tasks 5–11; the Cut Release workflow `.github/workflows/cut-release.yml` (workflow_dispatch on `main`; computes `vX.Y.Z` with `scripts/next-version.sh`; pushes an annotated tag; chains `.github/workflows/docker-push.yml`, which builds the five binaries, smoke-tests them in the image and pushes `ghcr.io/sentioxyz/arbiter:<tag>`, `:sha-<short>` and `:latest` for `linux/amd64` with OCI labels `org.opencontainers.image.version` and `.revision`); the throwaway `statedigest` of production `docs/storage-integrity-devnet2-dynamic-table-set.md` §2.6 / §7.2, whose source is `$HOME/si-devnet2-lanes-rehearsal/statedigest/main.go` (sha256 `c5168a19360228da9be001633cb5cf1c2f5a0f49c45c8f90df3dc176c37ac7e4`, byte-identical to the copies the lanes rollout built from in `$HOME/si-devnet2-lanes-rehearsal/{src,live/src}/{old,new}/cmd/statedigest/`); the devnet2 arbiter pin comment in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` (line 70: `# arbiter v0.10.0 (76ca023d)` on 2026-10-10).
- Produces: `ARBITER_TAG`, `ARBITER_DIGEST` (release ledger, contract §4); `$R/bin/old/statedigest` (the voters' live arbiter release) and `$R/bin/new/statedigest` (`ARBITER_TAG`, never `main`), each beside a `v19check`, for Plan S1-B Task 17's offline replay check; `$R/v19.container`.

**`cmd/statedigest` is not on arbiter main.** `git ls-tree --name-only origin/main cmd/` (origin/main = GitHub main = `5ea60c8` on 2026-10-10) lists only `arbiter`, `arbiter-admin`, `arbiter-anchor`, `arbiter-snode`, `arbiter-verifier` and `internal`; the runbook builds the tool per release and never commits it. It is the minimal program the client_seq lanes plan's Task 12 built: it loads a voter's own `arbiter.yaml` exactly as `cmd/arbiter` does (params, then the genesis manifest derived from the configured tables and checked against `genesis.schema_root`), restores the newest Raft snapshot of a copy of that voter's `raft` directory, applies every logged command, prints every rejection (`apply_rejected`, `apply_error`, `rejected_commands`; the FSM returns rejections, not errors, so an old-equals-new comparison alone could pass while both builds reject the log), and prints `last_index`, `applied_commands`, two `live_check` lines (consensus view; next sealed and safe block), `written_version`, a sha256 per top-level key of the written snapshot document, the spent-ids sha256 and the whole container's sha256. This task adds no option to it: the multi-source runbook keeps the lanes runbook's exact procedure and expectations. The older-binary refusal of a v19 container is checked with a separate 30-line throwaway, `v19check`.

- [ ] **Step 1: README**

Insert after the "Client lanes" section (one paragraph per line, never hard-wrapped):

```markdown
## Multi-source storage integrity (signed claims)

Every indexer of a network can join its storage-integrity layer (housegate spec 2026-10-10). Two consensus parameters govern it: `si_indexers`, an append-only list of `{indexer_id, activation_block, signer, snode_node_id, enrollment_jws}` sorted by indexer id, and `verifiers`, a list of `{node_id, ed25519_pubkey}`. Both are absent until an authority-signed update sets them; afterwards every update carries both lists completely. An entry's `indexer_id`, `activation_block` and `snode_node_id` never change and entries are never removed; `signer` rotates only together with a new enrolment statement signed by the new signer. Verifiers may be added and removed, and removing one evicts its node in the same update. `max_writers` must be at least the number of entries.

**Activation.** The first update that sets `si_indexers` is the signed-claims activation, and it is irreversible. It restates the table registry's founding indexer (`si_indexer_id`, `activation_block`) as an entry and lists at least three verifiers. Its two lists are authoritative: in its own Apply every registered SNode it does not name and every registered verifier it does not list is evicted (reason `not named by the signed-claims activation`), while a listed verifier registered under another key, or a named SNode registered with another role, refuses the update. Nothing else can evict a node before the activation, so this is how stale registrations leave; `arbiter-admin` lists the nodes the update would evict and submits only with `--evict-unnamed`, and the leader accepts exactly that list. From its commit on: a statement's source is its table owner's SNode (hash selection only replays earlier entries), and admission answers `SOURCE_UNAVAILABLE` (10) with nothing changed while that SNode is not registered and Active; every SNode message (registration, activation, result claim, promotion and cleanup acknowledgement, purge report) must be signed with the owning indexer's IndexerRegistry signer key over `{kind, network_id, genesis_snapshot_id, body}`, and every verifier registration, activation and purge report with its listed ed25519 key; a registration carries a `registration_seq` (a millisecond-floored counter) that must exceed the last one applied for the node, except that an identical resend at that sequence is a no-op, and an activation names the current one; only the owner's SNode receives a table's promotions and cleanups, and only its acknowledgement advances state (an `Applied:false` from any other node is refused); a purge completes once the owner's SNode and every non-evicted verifier reported, and a purge report from any other SNode is refused. `GetPurgeNodeSet` keeps listing every registered, non-evicted SNode and verifier before and after the activation: the data plane reads it as the live replica membership of every table.

**Enrolment.** An update that appends an entry enrols an indexer. Its `activation_block` must exceed `max(registry cursor, founding activation_block - 1)`; choose one at least 1800 L2 blocks (about one hour) past the L2 safe head and check it by hand before submitting, because `arbiter-admin` reads neither the L2 chain nor the registry cursor. `enrollment_jws` is an ES256K JWS with purpose `arbiter-snode-enrollment-v1` over `{network_id, genesis_snapshot_id, indexer_id, snode_node_id}`, signed by the indexer's IndexerRegistry signer (sentio-node prints it with its enrolment subcommand). Like every consensus update, enrolment needs drained authority work. The watcher seeds the new indexer's Legacy tables when the committed cursor completes block `activation_block - 1`, exactly there, and until that seed the registry refuses table additions, retirements and cursor advances at or past `activation_block`.

**CLI.** `arbiter-admin consensus update ... --si-indexer 'id=N,activation=BLOCK,signer=0x...,node=NODE,enrollment=PATH' ... --verifier 'node=NODE,pubkey=HEX' ...` gives the complete lists; omitted, the committed lists are carried. The CLI verifies each enrolment statement locally and prints a reminder of the activation-block check for every appended entry. For the activation it lists the registered nodes the update would evict and submits only with `--evict-unnamed`, which it forwards to the leader as gRPC metadata naming exactly those nodes. `consensus show` prints both lists. `arbiter-admin node evict --address LEADER --node NODE --expected-registration-seq N --reason TEXT` signs an authority eviction (key from `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` only); it is refused before the activation, and afterwards the arbiter requires the node's current registration sequence and the current authority context. A mismatch is refused, and the refusal names the committed sequence.

**Verifiers and standalone SNodes.** A verifier signs with its evidence key and keeps its `registration_seq` in `<state_dir>/registration.json`. Set `state_dir` in `arbiter-verifier`'s config to a persistent path; left empty, the sequence lives in memory and the verifier logs a warning at startup. The standalone `arbiter-snode` signs once `ARBITER_SNODE_CLAIM_KEY_HEX` holds its indexer's IndexerRegistry signer key (environment only), with `indexer_id` (required with the key) and, for a host without genesis tables, `genesis_snapshot_id` in its config; without the key it stays a legacy unsigned SNode, which the arbiter refuses after the activation.

**Snapshot-query lane.** The signed `INSERT ... SELECT` snapshot-query lane (default off, not wired) still hash-selects its source after the activation. Do not enable it on a network with more than one enrolled indexer until it is made owner-aware.

**Activation order (spec 2026-10-10 §13, stage 1).** 1. Replay every voter's Raft log offline under the voters' live arbiter release and the new one with the throwaway `statedigest` tool (built per release, never committed; production runbook `docs/storage-integrity-devnet2-dynamic-table-set.md` §2.6 and §7.2); the outputs must be identical. 2. Upgrade the verifiers (with `state_dir` set), then replace the voters one at a time, followers first and the leader last. Nothing changes before the activation: the leader strips every new field from what it proposes, and a command carrying one is refused exactly as an older voter's decoder refuses it. 3. Upgrade every SNode (indexer-a's sentio-node, with its claim signer) so it advertises `signed_claims_v1`, and let every verifier and SNode re-register with the current leader. 4. Drain, then run `consensus update` with `--si-indexer` for the founding indexer, `--verifier` for every verifier that stays and `--voter` for every Raft voter, plus `--evict-unnamed` once the listed evictions are the intended ones. The CLI refuses unless every voter reports `signed_claims_v1` and every registered verifier and SNode that stays advertised it to the current leader, and the leader re-checks its own feature book and the eviction list; after a leader change the book is empty, so restart the data plane before the gate. 5. Enrol further indexers with later updates. The runbook is production `docs/storage-integrity-devnet2-multi-source.md`.

**Snapshot v19.** The container moves to v19 once `si_indexers` is committed, adding `registration_seqs`, the two lists, incarnation owners and the seeded-indexer set; until then the writer keeps v14 to v18 byte for byte. A pre-v19 binary refuses a v19 container before parsing it, and v19 state under an older version byte is refused as forged.

**No downgrade.** After the activation no voter, verifier or SNode may run a binary without `signed_claims_v1`; the leader refuses such a data-plane registration before proposing it. Until stage 2 (tracks) one stuck indexer stalls every indexer's safe progress, so enrol only self-operated indexers.
```

and extend the "Snapshot versions" sentence (line 324) with ", and **v19** once `si_indexers` is committed".

- [ ] **Step 2: Full verification**

Run: `cd "$W" && bazel build //... && bazel test --build_tests_only --@rules_go//go/config:race //... --test_output=errors && git diff --exit-code --diff-filter=MDR origin/main -- fsm/testdata accumulator/testdata`
Expected: all PASS (the CI `test` job's exact invocation), in particular both frozen goldens (`TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`), `TestDeterminism_*`, the L3 goldens, `TestTableRegistryMirrorsProto`, `TestSharedStatementVectorsAreByteIdenticalToHousegate`, every `snapshot_lanes_test.go` and `snapshot_signed_claims_test.go` case, `TestCLIClientLanesActivationThroughServerAndFSM` and `TestCLISignedClaimsActivationThroughServerAndFSM`; the `git diff` (modified, deleted or renamed files only) is empty, so no existing golden or vector file changed and the only new one is Task 5's. Also run CI's red lines: `bazel query 'deps(//fsm:fsm, 1)' | grep -c com_github_sentioxyz_arbiter_proto` prints `0`, and `grep -rn 'time\.Now' fsm/` prints nothing. If docker and a ClickHouse on `127.0.0.1:9000` are available, run the CI integration job too: `ARBITER_CH_INTEGRATION=1 CH_ADDR=127.0.0.1:9000 bazel test //integration/chpipeline:chpipeline_test //cmd/internal/startup:startup_test --test_env=ARBITER_CH_INTEGRATION --test_env=CH_ADDR --test_timeout=900 --test_output=errors` (pre-activation pipeline; it must stay green).

- [ ] **Step 3: Commit**

```bash
cd "$W" && git add README.md && git commit -m "docs(readme): multi-source signed claims, enrolment, activation order, snapshot v19

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: PR and merge (requires explicit user confirmation)**

Push and `gh pr create --repo sentioxyz/arbiter --fill`; the PR description lists the stage-1 order of spec §13, the strict-decoder evidence (Task 8 `TestSignedFieldsAreRefusedBeforeActivation`, `TestServerDropsSignedFieldsBeforeActivation`; arbiter-core's frozen command bytes), the frozen pre-activation golden (Task 5) and the v19 gating (Task 10), and ends with the attribution line `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Merge after CI and review.

- [ ] **Step 5: Rehearse the offline replay check on the merged main (local only)**

The procedure is the runbook's (§2.6, §7.2): two source trees exported from the arbiter repository with `git archive` (the lanes rollout's `$HOME/si-devnet2-lanes-rehearsal/src/{old,new}` have no `.git`; the runbook calls them worktrees), the throwaway tool copied into `cmd/statedigest` of each, one binary per tree. Nothing is committed and no worktree is created.

```bash
R="$HOME/si-devnet2-multisource-rehearsal"; A=/Users/uranuswch/Dev/sentio_xyz/arbiter
mkdir -p "$R/statedigest" "$R/v19check" "$R/sample"
cp "$HOME/si-devnet2-lanes-rehearsal/statedigest/main.go" "$R/statedigest/main.go"
shasum -a 256 "$R/statedigest/main.go"   # must print c5168a19360228da9be001633cb5cf1c2f5a0f49c45c8f90df3dc176c37ac7e4
git -C "$A" fetch -q --tags origin
OLD_TAG="$(sed -n 's/.*# arbiter \(v[0-9][0-9.]*\) (.*/\1/p' /Users/uranuswch/Dev/sentio_xyz/production/k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml | head -1)"; echo "OLD_TAG=$OLD_TAG"
export_build() { # $1 = old|main|new, $2 = git ref
  rm -rf "$R/src/$1" "$R/bin/$1" && mkdir -p "$R/src/$1" "$R/bin/$1"
  git -C "$A" archive "$2" | tar -x -C "$R/src/$1"
  for tool in statedigest v19check; do
    mkdir -p "$R/src/$1/cmd/$tool" && cp "$R/$tool/main.go" "$R/src/$1/cmd/$tool/main.go"
  done
  (cd "$R/src/$1" && go build -o "$R/bin/$1/" ./cmd/statedigest ./cmd/v19check)
}
```

`OLD_TAG` is the release the production pin comment names; the runbook's baseline is the voters' live image at rollout time, which Plan S1-B Task 17 re-measures with `si_images` (a stale `old` binary compares the wrong build and proves nothing, runbook F10). Only if `$HOME/si-devnet2-lanes-rehearsal/statedigest/main.go` is gone, write `$R/statedigest/main.go` from this listing of it (then the checksum above does not apply):

```go
// Throwaway offline state digest for the client_seq lane rollout. It loads a
// voter's own arbiter.yaml exactly as cmd/arbiter does (params and genesis),
// restores the newest Raft snapshot of a COPY of that voter's raft data dir,
// applies every logged command, reports every rejection, and prints a sha256
// per top-level key of the resulting snapshot document plus live_check lines.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter/config"
	"github.com/sentioxyz/arbiter/fsm"
)

type sink struct{ bytes.Buffer }

func (s *sink) ID() string    { return "digest" }
func (s *sink) Cancel() error { return nil }
func (s *sink) Close() error  { return nil }

func main() {
	dir := flag.String("data-dir", "", "copy of one voter's raft data dir")
	cfgPath := flag.String("config", "", "that voter's arbiter.yaml")
	flag.Parse()
	if err := run(*dir, *cfgPath); err != nil {
		fmt.Fprintln(os.Stderr, "statedigest:", err)
		os.Exit(1)
	}
}

func genesis(cfg config.Config) (*replay.SafeSnapshotManifest, error) {
	derived, err := payloadexec.New(cfg.Genesis.NetworkID, cfg.TableSchemas()...).GenesisSnapshot(0, cfg.Genesis.SchemaSnapshotID, cfg.Genesis.ExecutorProfileID)
	if err != nil {
		return nil, err
	}
	if derived.SchemaRoot != cfg.Genesis.SchemaRoot {
		return nil, fmt.Errorf("schema_root %s != genesis.schema_root %s", derived.SchemaRoot, cfg.Genesis.SchemaRoot)
	}
	return &derived, nil
}

func run(dir, cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return err
	}
	state, err := fsm.NewWithNotify(fsm.Params{
		NetworkID: cfg.Genesis.NetworkID, MaxWriters: cfg.Genesis.MaxWriters, SchemaSnapshotID: cfg.Genesis.SchemaSnapshotID,
		ExecutorProfileID: cfg.Genesis.ExecutorProfileID, AuthorityAddresses: cfg.Authority.AllowedAddresses,
		ArtifactDispositionCapability: cfg.Genesis.ArtifactDispositionCapability,
	}, nil)
	if err != nil {
		return err
	}
	g, err := genesis(cfg)
	if err != nil {
		return err
	}
	if err := state.ConfigureGenesis(*g); err != nil {
		return err
	}
	fmt.Printf("genesis_configured %s\n", g.SnapshotID)
	snaps, err := raft.NewFileSnapshotStore(dir, 2, io.Discard)
	if err != nil {
		return err
	}
	metas, err := snaps.List()
	if err != nil {
		return err
	}
	var from uint64
	if len(metas) > 0 {
		_, rc, err := snaps.Open(metas[0].ID)
		if err != nil {
			return err
		}
		if err := state.Restore(rc); err != nil {
			return fmt.Errorf("restore %s: %w", metas[0].ID, err)
		}
		from = metas[0].Index
		fmt.Printf("snapshot_index %d\n", from)
	}
	store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
	if err != nil {
		return fmt.Errorf("open raft.db (re-copy it if this is a torn copy): %w", err)
	}
	defer store.Close()
	first, _ := store.FirstIndex()
	last, err := store.LastIndex()
	if err != nil {
		return err
	}
	if from+1 < first && from < last {
		return fmt.Errorf("log gap: snapshot %d, first log %d", from, first)
	}
	applied, rejected := 0, 0
	for i := from + 1; i <= last; i++ {
		var entry raft.Log
		if err := store.GetLog(i, &entry); err != nil {
			return fmt.Errorf("log %d: %w", i, err)
		}
		if entry.Type != raft.LogCommand {
			continue
		}
		switch res := state.Apply(&entry).(type) {
		case fsm.Rejected:
			rejected++
			fmt.Printf("apply_rejected %d %s\n", i, res.Reason)
		case fsm.SubmitResult:
			if res.Code != 1 {
				fmt.Printf("apply_rejected %d code=%d %s\n", i, res.Code, res.Message)
			}
		case error:
			fmt.Printf("apply_error %d %v\n", i, res)
		}
		applied++
	}
	fmt.Printf("last_index %d\napplied_commands %d\nrejected_commands %d\n", last, applied, rejected)
	view, err := state.ConsensusParamsView()
	if err != nil {
		return err
	}
	fmt.Printf("live_check network_id=%s genesis_snapshot_id=%s epoch=%d params_digest=%s promotion_seq=%d\n",
		view.Current.NetworkID, view.GenesisSnapshotID, view.Epoch, view.ParamsDigest, view.PromotionSeq)
	m := state.MetricsSnapshot()
	fmt.Printf("live_check next_l3_block_seq=%d safe_block_seq=%d \n", m.SealedBlockSeq+1, m.SafeBlockSeq)
	snap, err := state.Snapshot()
	if err != nil {
		return err
	}
	var out sink
	if err := snap.Persist(&out); err != nil {
		return err
	}
	raw := out.Bytes()
	n := binary.BigEndian.Uint64(raw[5:13])
	fmt.Printf("written_version %d\n", raw[4])
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw[13:13+n], &doc); err != nil {
		return err
	}
	keys := make([]string, 0, len(doc))
	for k := range doc {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		var v any
		_ = json.Unmarshal(doc[k], &v)
		canonical, _ := json.Marshal(v)
		fmt.Printf("key %s %x\n", k, sha256.Sum256(canonical))
	}
	fmt.Printf("spent_ids %x\n", sha256.Sum256(raw[13+n:]))
	fmt.Printf("snapshot_sha256 %x\n", sha256.Sum256(raw))
	return nil
}
```

Write `$R/v19check/main.go`:

```go
// Throwaway v19 container check for the multi-source signed-claims rollout
// (never committed). It restores one raw snapshot container into this build's
// FSM with no configured genesis (the embedded-host restore path) and prints
// restore_ok or restore_refused with the container's version byte; a build
// older than the container must refuse it before parsing.
package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/sentioxyz/arbiter/fsm"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: v19check CONTAINER")
		os.Exit(2)
	}
	data, err := os.ReadFile(os.Args[1])
	if err != nil || len(data) < 5 {
		fmt.Fprintln(os.Stderr, "v19check: not a snapshot container:", err)
		os.Exit(1)
	}
	state, err := fsm.New(fsm.Params{NetworkID: "rehearsal", SchemaSnapshotID: "rehearsal", ExecutorProfileID: "rehearsal"})
	if err != nil {
		fmt.Fprintln(os.Stderr, "v19check:", err)
		os.Exit(1)
	}
	if err := state.Restore(io.NopCloser(bytes.NewReader(data))); err != nil {
		fmt.Printf("restore_refused version %d: %v\n", data[4], err)
		return
	}
	fmt.Printf("restore_ok version %d\n", data[4])
}
```

Build: `export_build old "$OLD_TAG" && export_build main origin/main`. If `config.Config` in `OLD_TAG` names a field differently (`TableSchemas()`, `Genesis.*`), fix the copy in `$R/src/old/cmd/statedigest` only, mirroring `cmd/arbiter/genesis.go` and `cmd/arbiter/services.go:configuredFSMParams` of that tag. Expected: both trees build both tools.

Rehearse three properties:

1. **Identical replay before the activation.** Use the lanes rehearsal's local sample (`cp -R "$HOME/si-devnet2-lanes-rehearsal/sample/raft" "$HOME/si-devnet2-lanes-rehearsal/sample/local.yaml" "$R/sample/"`), or produce one with a short local single-node run of `bazel run //cmd/arbiter -- -config configs/local.yaml`, then:

```bash
for v in old main; do
  "$R/bin/$v/statedigest" -data-dir "$R/sample/raft" -config "$R/sample/local.yaml" > "$R/sample/out.$v.txt" 2> "$R/sample/out.$v.err" || { cat "$R/sample/out.$v.err"; exit 1; }
  grep -E '^(snapshot_index|last_index|applied_commands|rejected_commands|written_version|genesis_configured|live_check|apply_error|apply_rejected|key|spent_ids|snapshot_sha256)' "$R/sample/out.$v.txt" > "$R/sample/out.$v.cmp"
done
cmp "$R/sample/out.old.cmp" "$R/sample/out.main.cmp" && ! grep -q '^key registration_seqs ' "$R/sample/out.main.cmp" && echo "sample replays identically"
```

Expected: `sample replays identically`, `applied_commands` above zero, no `apply_error`.

2. **A real v19 container.** In the `main` export only, write the throwaway test and dump the container:

```bash
cat > "$R/src/main/fsm/zz_rehearsal_v19_test.go" <<'EOF'
package fsm

import (
	"os"
	"testing"
)

// Throwaway, never committed: writes a v19 container for the multi-source
// rehearsal (Task 12 Step 5).
func TestRehearsalDumpV19Container(t *testing.T) {
	out := os.Getenv("ARBITER_REHEARSAL_V19_OUT")
	if out == "" {
		t.Skip("ARBITER_REHEARSAL_V19_OUT not set")
	}
	f, _ := signedClaimsFSM(t)
	mustApply(t, f, snodeRegistration(t, f, indexerSigner2(t), "s2", 1))
	data := snapshotBytes(t, f)
	if data[4] != snapshotVersionV19 {
		t.Fatalf("version byte %d", data[4])
	}
	if err := os.WriteFile(out, data, 0o644); err != nil {
		t.Fatal(err)
	}
}
EOF
(cd "$R/src/main" && ARBITER_REHEARSAL_V19_OUT="$R/v19.container" go test ./fsm -run TestRehearsalDumpV19Container -count=1)
```

3. **The previous release refuses it, the new code restores it.** `"$R/bin/old/v19check" "$R/v19.container"` prints `restore_refused version 19: unsupported snapshot version 19 …`; `"$R/bin/main/v19check" "$R/v19.container"` prints `restore_ok version 19`.

- [ ] **Step 6: Release `ARBITER_TAG` (requires explicit user confirmation)**

No arbiter release may have been cut between Task 5's pin commit and this step (contract §3b.7): from that commit `GetProtocolInfo` returns `arbiter.LocalNodeFeatures()`, which includes `signed_claims_v1`, so an image cut earlier would advertise a capability it does not implement and pass the activation gate. Check first:

```bash
A=/Users/uranuswch/Dev/sentio_xyz/arbiter
git -C "$A" fetch -q --tags origin
PIN="$(git -C "$A" log --format=%H -1 --grep='pin arbiter-core' origin/main)"; echo "pin commit $PIN"
git -C "$A" tag --contains "$PIN"   # must print nothing
```

If it prints a tag, stop and tell the user: that release must never be deployed, and the rollout needs a decision first. Then run the Cut Release workflow, `gh workflow run cut-release.yml --repo sentioxyz/arbiter --ref main`, and wait for its `docker` job (`gh run watch --repo sentioxyz/arbiter <run id>`). Record `ARBITER_TAG` from the run summary ("Cutting vX.Y.Z") or `git -C "$A" fetch -q --tags origin && git -C "$A" tag --sort=-v:refname | head -1`; never predict it. Record `ARBITER_DIGEST` as the `Digest:` line of `docker buildx imagetools inspect ghcr.io/sentioxyz/arbiter:$ARBITER_TAG` (an OCI index when build-push-action attaches provenance; use exactly that value), and check that `docker buildx imagetools inspect ghcr.io/sentioxyz/arbiter:$ARBITER_TAG --format '{{json .Image}}' | jq -r '.config.Labels["org.opencontainers.image.revision"]'` equals `git -C "$A" rev-parse "$ARBITER_TAG^{commit}"`. Write both values into the release ledger.

- [ ] **Step 7: Build the release's replay-check binaries (local only)**

Exactly as runbook §7.2: `old` from the arbiter release the voters run when the check runs, `new` from `ARBITER_TAG`, never from `main`. With `R`, `A` and `export_build` from Step 5:

```bash
export_build new "$ARBITER_TAG"
export_build old "$OLD_TAG"   # re-measure OLD_TAG first: the voters' live image, not a version fixed in advance
```

Re-run Step 5's checks 1 and 3 with `new` in place of `main` (re-dump the container from `$R/src/new` with the same throwaway test if `ARBITER_TAG^{commit}` differs from the `main` of Step 5). Expected: identical replay lines, `restore_refused` from `old` and `restore_ok` from `new`. Hand `$R/bin/old/statedigest` and `$R/bin/new/statedigest` to Plan S1-B Task 17, which rebuilds `old` whenever the voters' live image differs from `OLD_TAG`.

---

---

## Appendix A: binding cross-task contract

The tasks of Plans S1-A and S1-B refer to this appendix as `contract §N` (or `CONTRACT §N`). It fixes the names, field numbers, JSON tags, signing purposes and consensus semantics every task must use verbatim. The amendment sections §3a–§3c record decisions taken while the tasks were written; the main sections below already state the amended rule and point to the amendment that introduced it. Spec: housegate `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` (main `534a8b4`); stage 1 = spec §6, the data-plane parts of §8, D3–D8, D11 and the stage-1 half of D12. Base revisions: arbiter `5ea60c8`, arbiter-core `050490c`, arbiter-proto `2eb3917`, housegate `2959e4a`, sentio-node `ff336f1`.

### 0. Activation model (read first)

- Stage 1 activates with ONE authority-signed `UpdateConsensusParams` that first sets `si_indexers` (entry 0 = the founding indexer restated from `table_registry`) and `verifiers`. Call this the *signed-claims activation*. In the FSM the predicate is `f.signedClaimsActiveLocked() bool { return len(f.st.Params.SIIndexers) > 0 }`.
- Before activation every behaviour is byte-for-byte today's (hash source selection, unsigned messages, broadcast promotions, purge set = every non-evicted SNode and verifier, snapshot version unchanged).
- **Strict-decoder rule (fork safety):** the arbiter-core Raft decoder refuses unknown fields, so no new field may enter a proposed Raft command before activation. Before activation the arbiter server and orchestrator MUST drop every new field (signatures, `registration_seq`, `owner_indexer_id`, seed `indexer_id`) from the commands they propose, even if an upgraded SNode/verifier/watcher supplies them. The activation update itself is the first command carrying new fields; the activation gate (Task 11) guarantees every voter decodes it.
- After activation the new fields are mandatory where the rules below say so.

### 1. arbiter-proto (Task 1) — additive only, verbatim

All in proto package `arbiter`. Every number was verified free at `2eb3917` (Task 1 Step 2). `SIIndexerEntry` is declared in `table_registry.proto` because `consensus.proto` imports that file (§3a).

table_registry.proto (`SIIndexerEntry`) and consensus.proto (everything else in this block):
```
message SIIndexerEntry {
  uint64 indexer_id = 1;
  uint64 activation_block = 2;
  string signer = 3;          // lowercase 0x + 40 hex, the indexer's IndexerRegistry signer
  string snode_node_id = 4;
  string enrollment_jws = 5;  // ES256K JWS by `signer`, purpose arbiter-snode-enrollment-v1
}
message VerifierEntry {
  string node_id = 1;
  bytes ed25519_pubkey = 2;   // 32 bytes
}
ConsensusParamsUpdate:  repeated SIIndexerEntry si_indexers = 11;  repeated VerifierEntry verifiers = 12;
ConsensusMutableParams: repeated SIIndexerEntry si_indexers = 6;   repeated VerifierEntry verifiers = 7;
message EvictNodeRequest { string node_id = 1; uint64 expected_registration_seq = 2; string reason = 3; string authority_jws = 4; }
service ConsensusAdmin { ... rpc EvictNode (EvictNodeRequest) returns (Ack) {} }   // append, keep existing rpcs
```
table_registry.proto:
```
TableRegistrySnapshot: repeated SIIndexerEntry si_indexers = 7; repeated uint64 seeded_indexers = 8;
TableIncarnation:      optional uint64 owner_indexer_id = 18;
AddTableCmd:           optional uint64 owner_indexer_id = 8;
SeedLegacyTablesCmd:   optional uint64 indexer_id = 3;       // absent = the founding indexer's legacy seed
RecordTablePurgedCmd:  string signer_jws = 3; string ed25519_signature = 4;
// SubmitTablePurged takes RecordTablePurgedCmd itself, so fields 3-4 are also the RPC's fields (§3a)
```
arbiter.proto:
```
NodeRegistration: uint64 registration_seq = 6; string signer_jws = 7; string ed25519_signature = 8;
NodeRef (MarkActive request): uint64 registration_seq = 2; string signer_jws = 3; string ed25519_signature = 4;   // NodeRef's only proto user is MarkActive
RCRecord:     string source_jws = 6;
PromotionAck: string source_jws = 10;
CleanupAck:   string source_jws = 5;
enum AdmissionCode: ADMISSION_CODE_SOURCE_UNAVAILABLE = 10;
```
raftlog.proto:
```
RegisterRCCmd:         string source_jws = 2;
RecordPromotionAckCmd: string source_jws = 2;
RecordCleanupAckCmd:   string source_jws = 2;
RegisterNodeCmd:       string signer_jws = 2; string ed25519_signature = 3;   // registration_seq rides inside NodeRegistration
MarkActiveCmd:         uint64 registration_seq = 2; string signer_jws = 3; string ed25519_signature = 4;
EvictNodeCmd:          uint64 expected_registration_seq = 3; string authority_jws = 4;
```
Feature string: `"signed_claims_v1"`.

### 2. arbiter-core canonical Go (Tasks 2–4)

Package `arbiter` (repo root):
```go
const SignedClaimsFeature = "signed_claims_v1"
func LocalNodeFeatures() []string { return []string{ClientLanesFeature, SignedClaimsFeature} }

const AdmissionCodeSourceUnavailable AdmissionCode = 10

// The canonical signing form of an authority-signed eviction lives beside the
// other command types (types.go); its hash lives in package authority.
type EvictNodeCommand struct {
	NodeID                  string `json:"node_id"`
	ExpectedRegistrationSeq uint64 `json:"expected_registration_seq"`
	Reason                  string `json:"reason"`
}

type SIIndexerEntry struct {
	IndexerID       uint64 `json:"indexer_id"`
	ActivationBlock uint64 `json:"activation_block"`
	Signer          string `json:"signer"`
	SNodeNodeID     string `json:"snode_node_id"`
	EnrollmentJWS   string `json:"enrollment_jws"`
}
func (e SIIndexerEntry) Validate() error // activation_block >= 1; signer lowercase 0x+40 hex non-zero; snode_node_id non-empty, no whitespace; enrollment_jws non-empty

type VerifierEntry struct {
	NodeID        string `json:"node_id"`
	Ed25519Pubkey []byte `json:"ed25519_pubkey"`
}
func (e VerifierEntry) Validate() error // node_id non-empty; pubkey exactly 32 bytes

// ConsensusParamsUpdate gains (both omitempty so every earlier digest is unchanged):
	SIIndexers []SIIndexerEntry `json:"si_indexers,omitempty"`
	Verifiers  []VerifierEntry  `json:"verifiers,omitempty"`

// NodeRegistration gains (omitempty keeps every pre-activation registration byte-identical):
	RegistrationSeq uint64 `json:"registration_seq,omitempty"`
```
`authority.NormalizeConsensusParamsUpdate`: sort `SIIndexers` by `IndexerID`, lowercase `Signer`, refuse duplicate `IndexerID` and duplicate `SNodeNodeID`, `Validate()` each; sort `Verifiers` by `NodeID`, refuse duplicate `NodeID`/pubkey, `Validate()` each; refuse `len(SIIndexers) > 0 && MaxWriters < uint64(len(SIIndexers))`. A nil slice stays nil (never `[]`).

Package `authority` (new file `authority/snode_messages.go` plus `authority/evict_node.go`):
```go
const SNodeEnrollmentPurpose = "arbiter-snode-enrollment-v1"
type SNodeEnrollmentStatement struct {
	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
	IndexerID         uint64 `json:"indexer_id"`
	SNodeNodeID       string `json:"snode_node_id"`
}
func SNodeEnrollmentHash(stmt SNodeEnrollmentStatement) (string, error)              // CanonicalDigest("arbiter-snode-enrollment-statement-v1", stmt)
func (s *Signer) SignSNodeEnrollment(stmt SNodeEnrollmentStatement) (string, error)  // JWSCommandPayload{Iat: now, Purpose: SNodeEnrollmentPurpose, CmdHash: hash}
func VerifySNodeEnrollment(stmt SNodeEnrollmentStatement, jws, signer string) error  // deterministic, no clock; recovers the ES256K signer and requires == signer (case-insensitive)

const SNodeMessagePurpose = "arbiter-snode-message-v1"
type SNodeMessageKind string
const (
	SNodeMessageRegistration SNodeMessageKind = "registration"
	SNodeMessageMarkActive   SNodeMessageKind = "mark_active"
	SNodeMessageResultClaim  SNodeMessageKind = "result_claim"
	SNodeMessagePromotionAck SNodeMessageKind = "promotion_ack"
	SNodeMessageCleanupAck   SNodeMessageKind = "cleanup_ack"
	SNodeMessageTablePurged  SNodeMessageKind = "table_purged"
)
type MessageContext struct {
	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
}
// body is the canonical Go value: arbiter.NodeRegistration, MarkActiveBody, arbiter.RCRecord, arbiter.PromotionAck, arbiter.CleanupAck, TablePurgedBody
type MarkActiveBody struct { NodeID string `json:"node_id"`; RegistrationSeq uint64 `json:"registration_seq"` }
type TablePurgedBody struct { NodeID string `json:"node_id"`; IncarnationSeq uint64 `json:"incarnation_seq"` }
func SNodeMessageHash(kind SNodeMessageKind, ctx MessageContext, body any) (string, error) // CanonicalDigest("arbiter-snode-message-body-v1", struct{Kind; Context; Body})
func (s *Signer) SignSNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any) (string, error)
func VerifySNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any, jws, signer string) error   // deterministic

type VerifierMessageKind string
const (
	VerifierMessageRegistration VerifierMessageKind = "registration"
	VerifierMessageMarkActive   VerifierMessageKind = "mark_active"
	VerifierMessageTablePurged  VerifierMessageKind = "table_purged"
)
func VerifierMessageHash(kind VerifierMessageKind, ctx MessageContext, body any) (string, error) // CanonicalDigest("arbiter-verifier-message-body-v1", ...)
func SignVerifierMessage(priv ed25519.PrivateKey, kind VerifierMessageKind, ctx MessageContext, body any) (string, error) // hex ed25519 over the hash string bytes (evidence convention)
func VerifyVerifierMessage(pub ed25519.PublicKey, kind VerifierMessageKind, ctx MessageContext, body any, sigHex string) error

const EvictNodePurpose = "arbiter-evict-node-v1"
func EvictNodeHash(cmd arbiter.EvictNodeCommand) (string, error)   // CanonicalDigest("arbiter-evict-node-command-v1", cmd)
func (s *Signer) SignEvictNodeWithContext(cmd arbiter.EvictNodeCommand, ctx ConsensusContext) (string, error)
func (v *Validator) VerifyEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error)    // Apply and restore (§3b.2)
func (v *Validator) AuthorizeEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error) // RPC, enforces MaxTokenAge
```
Follow the existing authority helpers (`PromoteCommandHash`, `CleanupCommandHash`, `SignPromotionWithContext`, `verifyAuthorityContext`) for the exact JWS/payload/context mechanics; do not invent a second JWS format.

Package `wire` (Go mirror of Raft commands; field names verbatim):
```go
type RegisterRC struct { RC arbiter.RCRecord; SourceJWS string }
type RecordPromotionAck struct { Ack arbiter.PromotionAck; SourceJWS string }
type RecordCleanupAck struct { Ack arbiter.CleanupAck; SourceJWS string }
type RegisterNode struct { Registration arbiter.NodeRegistration; SignerJWS string; Ed25519Signature string }
type MarkActive struct { NodeID string; RegistrationSeq uint64; SignerJWS string; Ed25519Signature string }
type EvictNode struct { NodeID string; Reason string; ExpectedRegistrationSeq uint64; AuthorityJWS string }
type RecordTablePurged struct { /* existing fields */ SignerJWS string; Ed25519Signature string }
type AddTable struct { /* existing fields */ OwnerIndexerID *uint64 }
type SeedLegacyTables struct { /* existing fields */ IndexerID *uint64 }
```
Converters: proto ⇄ Go for every new field; consensus params (`ConsensusParamsUpdate`, `ConsensusMutableParams`) and the registry snapshot/view converters carry `SIIndexers`, `Verifiers`, `SeededIndexers`, incarnation `OwnerIndexerID`. Nil/empty rule: empty repeated → nil; absent optional → nil pointer.

arbiter-core data plane (Task 4):
- `snode.Config` gains `IndexerID uint64` (the owning indexer, used as the reconciler owner filter) and the role gains a signer: `snode.Deps.ClaimSigner *authority.Signer` (the indexer key; nil = legacy unsigned mode). The SNode persists `RegistrationSeq uint64` in its `state.json` (`json:"registration_seq,omitempty"`). Before each `RegisterNode` it computes `next := max(persisted+1, uint64(now().UnixMilli()))` (so a node whose state directory was lost still moves forward without manual intervention), persists `next` durably, and uses it for the registration and the following MarkActive; and signs RegisterNode, MarkActive, RC, PromotionAck, CleanupAck and table-purged reports with `SignSNodeMessage` when `ClaimSigner != nil`. The context comes from the role's configured network id and `Config.GenesisSnapshotID` (§3b.3).
- `tableset` reconciler gains an owner filter: `tableset.Config.Owner *uint64` (§3b.1) — when non-nil, only incarnations whose `OwnerIndexerID` equals it (and genesis incarnations owned by it) are materialised, verified, swept or purged; `nil` keeps today's "every incarnation" behaviour (verifiers). Incarnations whose owner is unknown (nil `OwnerIndexerID`, pre-activation) are treated as owned by the founding indexer (`TableRegistryParams.SIIndexerID`).
- Owner-scoped SNode reads: source-claim root, prepared-statement lookup, promoted-unsafe reads and the decommissioned-replica sweep cover only owned tables.
- Verifier role: signs RegisterNode / MarkActive / table-purged reports with its evidence key through `SignVerifierMessage` and persists its own `registration_seq` in `<Config.StateDir>/registration.json` (§3b.9).
- `GetPurgeNodeSet` is unchanged and keeps the full membership (§3b.5).

### 3. arbiter FSM and services (Tasks 5–12)

`fsm.Params` gains:
```go
	SIIndexers []arbiter.SIIndexerEntry `json:"si_indexers,omitempty"`
	Verifiers  []arbiter.VerifierEntry  `json:"verifiers,omitempty"`
```
`fsm.State` gains `RegistrationSeqs map[string]uint64` (snapshot key `registration_seqs`, v19 only).
`fsm.TableIncarnation` gains `OwnerIndexerID *uint64 `json:"owner_indexer_id,omitempty"``.
`fsm.TableRegistryState` gains `SeededIndexers []uint64 `json:"seeded_indexers,omitempty"`` (sorted, unique). `Seeded bool` stays and always equals "the founding indexer is seeded".
`fsm.TableRegistryView` gains `SIIndexers []arbiter.SIIndexerEntry` and `SeededIndexers []uint64`; incarnations carry `OwnerIndexerID`.
Helpers (names verbatim): `(f *FSM) signedClaimsActiveLocked() bool`, `(f *FSM) siIndexerLocked(id uint64) (arbiter.SIIndexerEntry, bool)`, `(f *FSM) siIndexerBySNodeLocked(nodeID string) (arbiter.SIIndexerEntry, bool)`, `(f *FSM) messageContextLocked() (authority.MessageContext, error)` (network id + genesis snapshot id).

Consensus-update rules (extend `verifyConsensusTransition` / `applyUpdateConsensusParams` in `fsm/consensus_updates.go`):
1. First set of `SIIndexers` requires `Params.TableRegistry != nil` (earlier or same update), requires `Verifiers` set in the same update with at least 3 entries, and requires the entry whose `IndexerID == TableRegistry.SIIndexerID` to have `ActivationBlock == TableRegistry.ActivationBlock`. The activation's two lists are authoritative for the registered data plane (§3c), and the writer gate counts writers after its evictions.
2. Once set, every update carries `SIIndexers`; entries are never removed; an existing entry's `IndexerID`, `ActivationBlock`, `SNodeNodeID` never change; `Signer` may change only together with a new `EnrollmentJWS` that verifies under the new signer; `EnrollmentJWS` of an unchanged entry must be byte-identical.
3. A new entry's `ActivationBlock` must be `> max(Cursor.BlockNumber, foundingEntry.ActivationBlock - 1)` (§3a).
4. Every new or changed entry's `EnrollmentJWS` must pass `authority.VerifySNodeEnrollment(stmt{network_id, genesis_snapshot_id, indexer_id, snode_node_id}, jws, entry.Signer)`.
5. `SNodeNodeID` must be unique across entries and must not equal any verifier entry's `NodeID` nor the id of any registered node that is not that entry's SNode.
6. `Verifiers`: once set, carried by every update; entries may be added and removed; a removed verifier's node is evicted in the same Apply (status `NodeEvicted`, reason `"removed from verifiers"`). The update is refused if it would leave fewer than 3 non-evicted listed verifiers. At first set, unlisted registered verifiers are evicted and a listed verifier registered under another key refuses the update (§3c).
7. The existing drain gate (all promotions acknowledged, no pending cleanup, no promoted-unsafe part) applies unchanged.
8. When the first `SIIndexers` update applies: every existing incarnation gets `OwnerIndexerID = &foundingID`; `SeededIndexers = [foundingID]` iff `Seeded`; `registryChangedLocked` is called once.

Registry rules:
- After activation `AddTable.OwnerIndexerID` is required, must name an entry, and `Created.BlockNumber >= entry.ActivationBlock`; before activation it must be nil.
- `SeedLegacyTables` with `IndexerID == nil` is the founding seed (today's path, unchanged). With `IndexerID = &x` (after activation only): entry x must exist and be unseeded; `AtBlock.Number == entry.ActivationBlock - 1`; tables recorded Legacy with `OwnerIndexerID = &x`; x appended to `SeededIndexers`.
- Guard: after activation, `AddTable`, `RetireTables` and `AdvanceL2Cursor` whose block is `>= e.ActivationBlock` for any unseeded entry e are refused (`"table registry: indexer <id> must be seeded before block <n>"`).
- Membership predicate (tableregistry/derive.go, chain.go): a `TableCreated` is SI iff the database's indexer at the event block has an entry (founding entry from `TableRegistryParams` before activation) and the event block `>= entry.ActivationBlock`. `derive` gets the entries; `AddTable` carries the owner (after activation only). The seed scan reads one indexer's databases at `ActivationBlock - 1`. New halt reason `HaltOwnerChanged` when a re-read database indexer differs from the cached one without an intervening `DatabaseDeleted`.

Admission (fsm/admission.go step 6): after activation `SourceNode = entry(owner(live incarnation)).SNodeNodeID`; if that node is not registered with status `NodeActive`, return `SubmitResult{Code: arbiter.AdmissionCodeSourceUnavailable, Message: "storage-integrity source <node> of indexer <id> is not active"}` with state unchanged (check before `SpentIDs.Insert`). Before activation `selectSource` (hash) unchanged.

Signed messages in Apply (after activation; before activation the new fields must be empty and are ignored):
- `RegisterNode` SNODE role: `NodeID` must equal some entry's `SNodeNodeID`; roles exactly `[SNODE]`; `RegistrationSeq > RegistrationSeqs[NodeID]`, except that an exact duplicate (same seq, same canonical registration, valid signature) is an idempotent no-op with no status change (§3b.4); `SignerJWS` verifies via `VerifySNodeMessage(SNodeMessageRegistration, ctx, registration, jws, entry.Signer)`. VERIFIER role: `(NodeID, Ed25519Pubkey)` must match a `Verifiers` entry; seq rule; `Ed25519Signature` verifies via `VerifyVerifierMessage`. On success `RegistrationSeqs[NodeID] = seq`. A node with both roles is refused after activation.
- `MarkActive`: `RegistrationSeq == RegistrationSeqs[NodeID]` and the matching signature kind for the node's role.
- `RegisterRC` (parked or bound): `rc.SourceNode` must be an entry's `SNodeNodeID` and `SourceJWS` must verify (`SNodeMessageResultClaim`, body = the RC) under that entry's signer; verify before parking.
- `RecordPromotionAck` / `RecordCleanupAck`: the *expected source* is the bound `SourceNode` shared by the statements the promotion covers (`PendingPromotions[seq].StatementSeqs`); for a cleanup, of the promotion with the same `PromotionSeq`. `ack.NodeID` must equal it and `SourceJWS` must verify under that node's entry signer; otherwise `Rejected{...}` with no state change — including `Applied:false`. If the covered statements are gone or disagree, refuse. `PendingPromotions` entries are never deleted and statements are never pruned, so a cleanup's expected source is always derivable from `PendingPromotions[cleanup.PromotionSeq].StatementSeqs` and no state is added; after the activation `ScheduleUnsafeCleanup` refuses a cleanup whose promotion has no resolvable source, since no node could acknowledge it (Task 9).
- `RecordTablePurged`: SNODE reporter signs `SNodeMessageTablePurged` (body `TablePurgedBody`), VERIFIER reporter signs `VerifierMessageTablePurged`.
- `EvictNode`: proposed only through the new authority-signed admin RPC (refused before the activation, §3a); Apply requires `ExpectedRegistrationSeq == RegistrationSeqs[NodeID]` and `Validator.VerifyEvictNode` with the exact current context (§3b.2). (Before activation the legacy unauthenticated EvictNode Apply path is unchanged — it has no RPC today.)

Purge set (fsm/apply_table_registry.go): after activation, completing a purge needs reports from the incarnation owner's SNode (`entry(owner).SNodeNodeID`) and every non-evicted VERIFIER, and a report from a non-owner SNode is refused; `GetPurgeNodeSet` keeps returning the full membership (§3b.5).

Orchestrator: after activation `streamPromotionSigned` / `streamCleanupSigned` send only to the expected source node (same rule as the ack check); before activation broadcast unchanged.

Snapshot v19 (fsm/snapshot.go): new version constant 19, written only when `len(Params.SIIndexers) > 0`; carries Params (with the new fields), `RegistrationSeqs`, incarnation owners and `SeededIndexers`; restore refuses any of them under a version byte < 19 (same pattern as v18 / lanes). Nothing changes in the bytes written before activation.

Activation gate (server, arbiter-admin): the leader refuses an `UpdateConsensusParams` that first sets `SIIndexers` unless every voter's `GetProtocolInfo.features` contains `signed_claims_v1` and every registered non-evicted SNODE/VERIFIER that the activation keeps advertised `signed_claims_v1` in the leader's feature book (the client-lanes gate code path; leader-local, not replicated). The nodes the activation would evict are listed by `arbiter-admin` and must be accepted with `--evict-unnamed` (§3c).

arbiter-admin:
- `arbiter-admin consensus update ... --si-indexer 'id=<n>,activation=<block>,signer=<0x..>,node=<id>,enrollment=<path-to-jws-file>'` (repeatable, the complete list) and `--verifier 'node=<id>,pubkey=<64 hex>'` (repeatable, the complete list). Omitting both keeps the current values (like `--client-lanes-max-per-account`).
- `arbiter-admin node evict --address <leader> --node <id> --expected-registration-seq <n> --reason <text>` (authority key from `ARBITER_AUTHORITY_PRIVATE_KEY_HEX`, never argv).
- `arbiter-admin consensus show` prints `si_indexers` and `verifiers`.

### 3a. Amendments after Task 1 drafting (binding)

- `SIIndexerEntry` is declared in `table_registry.proto` (consensus.proto already imports it; the reverse would be an import cycle). Names and numbers unchanged. The proto package is `arbiter`.
- The purge-report RPC `SubmitTablePurged` takes `RecordTablePurgedCmd` itself, so `RecordTablePurgedCmd.signer_jws = 3` / `ed25519_signature = 4` cover both the RPC and the Raft command.
- `ConsensusAdmin.EvictNode` is refused before the signed-claims activation (`FailedPrecondition`, "eviction requires the signed-claims activation"); the legacy `EvictNode` Apply path stays unreachable from RPC.
- Every committed change of `SIIndexers` (first set, append, signer rotation) and every `SeedLegacyTables` call `registryChangedLocked` exactly once, so the registry `Version` advances and followers observe the new entries.
- A founding seed (`SeedLegacyTables.IndexerID == nil`) applied after activation records `OwnerIndexerID = &foundingID` on its Legacy incarnations and adds the founding id to `SeededIndexers`; before activation it is unchanged.
- (superseded by §3b.5) `GetPurgeNodeSet` keeps the full membership; the owner rule applies to purge completion only.
- SNode/verifier `registration_seq`: `next := max(persisted+1, uint64(now().UnixMilli()))`, persisted durably before `RegisterNode` (Task 4); the FSM only requires `seq > RegistrationSeqs[node]`.
- (from Tasks 5–7 drafting; superseded by §3c) The activation's handling of registered SNodes that no entry names is defined in §3c: they are evicted, not refused.
- (from Tasks 5–7) A per-indexer seed `SeedLegacyTables{IndexerID: &x}` is accepted only when the committed cursor is exactly at `entry(x).ActivationBlock - 1` (same rule as the founding seed).
- (from Tasks 5–7) Rule 3 reads: a new entry's `ActivationBlock` must be `> max(Cursor.BlockNumber, foundingEntry.ActivationBlock - 1)`.
- (from Tasks 5–7) The snapshot-query lane (default off, unwired) keeps hash source selection after activation; it must not be enabled on a network with more than one enrolled indexer until it is made owner-aware. Document this in README (Task 12).
- (from Tasks 5–7) Test-support names Task 3 exports: package `authoritytest` with `IndexerKeyHex0/1`, `IndexerAddr0/1`, `MustSigner(t, keyHex)`, `SNodeEnrollmentVectors`; Task 2 converters `wire.SIIndexerEntriesToPB/FromPB`, `wire.VerifierEntriesToPB/FromPB`; the normalizer copies its input and its errors contain `max_writers` / `snode_node_id`.

### 3b. Amendments after Tasks 2–4 drafting (binding)

1. The reconciler owner filter is `tableset.Config.Owner *uint64` (there is no `tableset.Options`). Ownership resolves through `wire.TableRegistrySnapshot.Owner(inc)` (nil `OwnerIndexerID` = founding `Params.SIIndexerID`).
2. Signed eviction: Apply verifies with `(&authority.Validator{AllowedAddresses: <current authority set>}).VerifyEvictNode(cmd, jws, authority.ConsensusContext{NetworkID, GenesisSnapshotID, AuthorityEpoch: f.st.ConsensusEpoch})` — exact context, no epoch-0 exception (`verifyCurrentAuthority` only accepts the promotion purpose and stays for promotions/cleanups). The RPC calls `AuthorizeEvictNode` with a positive `MaxTokenAge` before proposing.
3. Both data-plane roles gain `Config.GenesisSnapshotID`; empty = derived with `dataplane.GenesisSnapshotID` from the complete genesis set (founding indexer only). A host whose SNode holds no genesis table must configure it explicitly (sentio-node: new config `storage_integrity.snode.genesis_snapshot_id`, required when `table_ids` is empty; devnet2-si-v2 value `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06`).
4. Registration retries: after activation, a `RegisterNode` whose `RegistrationSeq == RegistrationSeqs[NodeID]`, whose canonical registration equals the stored one and whose signature verifies is an idempotent no-op (`Applied{}`, NO status change — never reset to Syncing); an equal seq with any other body, and every lower seq, is refused. `MarkActive` at the current seq is idempotent.
5. `GetPurgeNodeSet` keeps returning every registered non-evicted SNODE and VERIFIER before and after activation (the reconciler uses it as live replica membership). The per-incarnation completion rule (owner SNode + every non-evicted verifier) lives only in the FSM's purge-report recording, which refuses a report from a non-owner SNode.
6. The SNode owner filter and owner-scoped reads apply only when `Deps.ClaimSigner != nil`; hosts set `IndexerID` together with `ClaimSigner`.
7. The SNode advertises `signed_claims_v1` in RegisterNode features only when `ClaimSigner != nil`; the verifier always advertises it. Because voters return `arbiter.LocalNodeFeatures()` from `GetProtocolInfo`, arbiter must pin `CORE_TAG` only in the release that also contains Tasks 5–11: no arbiter release may be cut between Task 5 and Task 12.
8. `snode` allows an empty genesis `Tables` only with `Deps.Registry != nil` and `Deps.ClaimSigner != nil`; such a host configures `SchemaRoot = 0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59` (`payloadexec.SchemaRoot(net, nil)`).
9. `verifier.Config.StateDir` holds `registration.json`; arbiter `cmd/arbiter-verifier` must expose it (flag/config) and production verifiers must set it to a persistent path.

### 3c. Amendment after Tasks 8–12 drafting (binding, supersedes §3a's "activation must bind every registered SNODE" and rule 1's "every registered verifier must be listed")

- The activation update's `si_indexers` and `verifiers` are the authoritative node sets. In the same Apply that first sets them, every registered non-evicted SNODE whose id is not some entry's `snode_node_id`, and every registered non-evicted VERIFIER that is not listed, is evicted (reason `"not named by the signed-claims activation"`). A listed verifier whose registered pubkey differs from its entry is refused (operator error), as is a named SNode registered with a role other than SNODE. Rationale: before the activation no eviction can be proposed (strict-decoder rule), so refusing would let one stale registration block the activation forever.
- `arbiter-admin consensus update` preflight (and the leader-local gate) lists the nodes the activation would evict and refuses to submit unless `--evict-unnamed` is given when that list is non-empty.
- `cmd/arbiter-snode` (standalone SNode binary) gains the claim-signer inputs: env `ARBITER_SNODE_CLAIM_KEY_HEX` (never argv) → `snode.Deps.ClaimSigner`, config `indexer_id` → `snode.Config.IndexerID`, config `genesis_snapshot_id` → `snode.Config.GenesisSnapshotID`; without the env it stays a legacy unsigned SNode.

### 4. Release ledger names (fill at cut time)

`PROTO_TAG` (Task 1), `CORE_TAG` (Task 4), `ARBITER_TAG` / `ARBITER_DIGEST` (Task 12).

### 5. Commands and conventions

- arbiter-proto: `make tools && make proto && make lint && make breaking && make test`.
- arbiter-core, arbiter: `bazel build //... && bazel test //...`; after dependency changes `bazel mod tidy && bazel run //:gazelle`; arbiter pins arbiter-core with `bash scripts/update-arbiter-core.sh <tag>`.
- One URWT worktree per repo at `~/src/remotesrc/claude/<repo>/multi-source-s1`, branch `urwt/claude/multi-source-s1` (agents: `$W` = that worktree).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Code comments in English. Markdown never hard-wrapped.
- Never edit existing golden/vector files: `arbiter:fsm/testdata/*golden*.json`, `arbiter:accumulator/testdata/spent_ids_vectors.json`, `arbiter:fsm/testdata/statement_jws_v2*.json`, `arbiter-core:conformance/*golden*`.
- TDD: every task step is failing test → run (fail) → minimal implementation → run (pass) → commit, in each repository's existing test style.

---

## Appendix B: drafting decisions and cross-task facts

Each task group was drafted against the binding contract (Appendix A). This appendix keeps, per group, how each contract question was resolved and the facts later tasks rely on. A "Proposal" in these notes was adopted into Appendix A (§3a–§3c) unless the note says otherwise. Where a note here disagrees with the Global Constraints or with Appendix A, those win.

### Task 1: contract conflicts (resolved)

1. **`SIIndexerEntry` cannot be defined in `consensus.proto`.** `TableRegistrySnapshot.si_indexers = 7` (in `table_registry.proto`) needs the type, and `consensus.proto` already imports `table_registry.proto` (`consensus.proto:12`); making `table_registry.proto` import `consensus.proto` is an import cycle, which buf rejects. Resolution used above: define `SIIndexerEntry` in `table_registry.proto` (the `ClientLaneParams` precedent, `table_registry.proto:22-28`). Its proto full name `arbiter.SIIndexerEntry`, Go name `pb.SIIndexerEntry`, fields and numbers are exactly the contract's; `consensus.proto` uses it through its existing import. `VerifierEntry` stays in `consensus.proto` as specified.
2. **There is no separate purge-report request message.** The purge-report RPC is `PromotionGateway.SubmitTablePurged (RecordTablePurgedCmd) returns (Ack)` (`arbiter.proto:524`); it takes the replicated `RecordTablePurgedCmd` itself (the `UpdateConsensusParamsCmd` precedent, pinned by `conformance/table_purge_test.go:20-23`). `RecordTablePurgedCmd.signer_jws = 3` / `ed25519_signature = 4` therefore serve the RPC and the Raft command; nothing else is added.
3. **Package name.** The proto package is `arbiter` (`option go_package = "github.com/sentioxyz/arbiter-proto/gen/pb;pb"`), not `sentio.arbiter.v1`; full names are `arbiter.SIIndexerEntry`, `arbiter.VerifierEntry`, `arbiter.EvictNodeRequest`.

No field-number conflict: Step 2 shows every contract number is the next free one at `v0.9.0`, with no `reserved` ranges anywhere.

Gaps where the contract is silent; the proto comments above state the reading, and the owning arbiter tasks must implement it (or amend the comment in a later release):
- **`EvictNode` before activation.** The RPC must answer `FAILED_PRECONDITION` until the signed-claims activation has committed: before it, an `EvictNodeCmd` may carry neither `expected_registration_seq` nor `authority_jws` (strict-decoder rule), and dropping them would create an unauthenticated eviction RPC that does not exist today.
- **Registry version on every `si_indexers` change.** Contract §3 rule 8 calls `registryChangedLocked` only when `si_indexers` is first set. `WatchTableRegistry` sends a snapshot only when the version rises, so each later enrolment and signer rotation must also bump the version once, or hosts and SNodes never learn the new entry; the `TableRegistrySnapshot.si_indexers` comment says "Every committed change bumps version".
- **Founding seed after activation.** If the founding indexer is still unseeded at activation (a new network, not devnet2), a `SeedLegacyTables` with `indexer_id` absent must still record `OwnerIndexerID = &founding` on its Legacy incarnations and add the founding id to `SeededIndexers` (or Apply must require `indexer_id = founding` after activation); otherwise an incarnation created after activation lacks an owner.
- **`PurgeNodeSet` content after activation.** The comment keeps "every registered, non-evicted SNode and verifier" as the set and adds that one incarnation's purge waits only on its owner's SNode and every non-evicted verifier. If the arbiter task that makes `GetPurgeNodeSet` "reflect" the new rule changes the set's content, it must amend this comment in the next proto release.

### Task 1: facts for other tasks

**Generated Go details.**
- protoc-gen-go camel-cases `si_indexers` to `SiIndexers` (lowercase `i`), `snode_node_id` to `SnodeNodeId`, `enrollment_jws` to `EnrollmentJws`, `ed25519_signature` to `Ed25519Signature`; the canonical arbiter-core mirrors use `SIIndexers`, `SNodeNodeID`, `EnrollmentJWS`, `Ed25519Signature` with the contract's JSON tags.
- The three `optional` fields are this repository's first proto3 `optional` fields: Go type `*uint64`, set with `proto.Uint64(x)`, presence via `m.OwnerIndexerId != nil` (or `m.ProtoReflect().Has(fd)`); `GetOwnerIndexerId()` returns 0 when absent, so converters must never test the getter. An explicit 0 is on the wire (`AddTableCmd` field 8 = `0x40 0x00`), so a pre-activation command must leave the pointer nil. The descriptor gets synthetic oneofs `_owner_indexer_id` / `_indexer_id`; arbiter-core's `validateCommandBytes` (`wire/snapshot_query.go:1327-1379`) walks by field number and is unaffected.
- `pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE.String()` is `"ADMISSION_CODE_SOURCE_UNAVAILABLE"` (arbiter's `reportAdmission` metric label).
- Every downstream gRPC server embeds the Unimplemented stub (arbiter `server/consensus_admin.go:25`, `server/membership.go:15`, `server/claims.go:16`, `server/gateway.go:78`; arbiter-admin fakes `cmd/arbiter-admin/main_test.go:31`, `client_lanes_test.go:35`), so `EvictNode` compiles everywhere before it is implemented.
- `EvictNodeRequest` numbering (`node_id` 1, `expected_registration_seq` 2, `reason` 3, `authority_jws` 4) differs from `EvictNodeCmd` (`node_id` 1, `reason` 2, `expected_registration_seq` 3, `authority_jws` 4), both as the contract specifies; unlike `UpdateConsensusParams` / `AbortSnapshotQuery` / `ActivateQueryProfile`, the request does not mirror the command, so convert by name.

**Proto anchors after this task** (for references in later tasks):

- `proto/table_registry.proto`: `SIIndexerEntry` 39-59; `SeedLegacyTablesCmd.indexer_id` 98; `AddTableCmd.owner_indexer_id` 116; `RecordTablePurgedCmd.signer_jws` 144; `RecordTablePurgedCmd.ed25519_signature` 149; `TableIncarnation.owner_indexer_id` 215; `TableRegistrySnapshot.si_indexers` 233; `TableRegistrySnapshot.seeded_indexers` 238.
- `proto/consensus.proto`: `ConsensusMutableParams.si_indexers` 29; `ConsensusMutableParams.verifiers` 32; `ConsensusParamsUpdate.si_indexers` 65; `ConsensusParamsUpdate.verifiers` 72; `VerifierEntry` 158-161; `EvictNodeRequest` 171-176; `ConsensusAdmin.EvictNode` 204.
- `proto/arbiter.proto`: `AdmissionCode.ADMISSION_CODE_SOURCE_UNAVAILABLE` 121; `RCRecord.source_jws` 183; `PromotionAck.source_jws` 335; `CleanupAck.source_jws` 347; `NodeRegistration.registration_seq` 466; `NodeRegistration.signer_jws` 473; `NodeRegistration.ed25519_signature` 479; `NodeRef.registration_seq` 490; `NodeRef.signer_jws` 494; `NodeRef.ed25519_signature` 499; `PromotionGateway.SubscribePromotions` 581; `PromotionGateway.SubmitTablePurged` 595; `Membership.RegisterNode` 641; `Membership.MarkActive` 645.
- `proto/raftlog.proto`: `RegisterRCCmd.source_jws` 61; `RecordPromotionAckCmd.source_jws` 103; `RecordCleanupAckCmd.source_jws` 125; `RegisterNodeCmd.signer_jws` 154; `RegisterNodeCmd.ed25519_signature` 158; `MarkActiveCmd.registration_seq` 167; `MarkActiveCmd.signer_jws` 168; `MarkActiveCmd.ed25519_signature` 169; `EvictNodeCmd.expected_registration_seq` 180; `EvictNodeCmd.authority_jws` 181.

**arbiter-core (Tasks 2–4) when it pins `PROTO_TAG`.**
- Pins: arbiter-core `go.mod:14`, arbiter `go.mod:21`, housegate `go.mod:15` and sentio-node `go.mod:15` all require `github.com/sentioxyz/arbiter-proto v0.9.0`; Bazel resolves it through `go_deps.from_file` (arbiter-core `MODULE.bazel:29`, `use_repo` entry `com_github_sentioxyz_arbiter_proto` at `:53`), so the bump is `go get github.com/sentioxyz/arbiter-proto@$PROTO_TAG && go mod tidy && bazel mod tidy && bazel run //:gazelle`.
- `conformance/arbiter_wire_test.go:51-70` `TestArbiterMirrorsMatchProto` fails immediately after the bump unless, in the same change: `arbiter.ConsensusParamsUpdate` gains the `si_indexers` / `verifiers` tags; `arbiter.NodeRegistration` gains `registration_seq` and the call becomes `assertMirror(t, arbiter.NodeRegistration{}, &pb.NodeRegistration{}, "features", "signer_jws", "ed25519_signature")`; `RCRecord`, `PromotionAck` and `CleanupAck` pass `"source_jws"` as request-only; and new rows `assertMirror(t, arbiter.SIIndexerEntry{}, &pb.SIIndexerEntry{})`, `assertMirror(t, arbiter.VerifierEntry{}, &pb.VerifierEntry{})` are added. `TestEnumNumbersMatchProto` (`:72-94`) gains `arbiter.AdmissionCodeSourceUnavailable` vs `pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE`.
- `wire/legacy_command_golden_test.go` `TestLegacyCommandBytesAreFrozen` must pass unchanged (zero or nil new fields encode nothing); never regenerate its hex.
- `wire/command.go:388-393` refuses `NodeRegistration.features` inside a `RaftCommand`; extend the same refusal to the embedded request-only fields (`RCRecord.source_jws` at `:360-361`, `PromotionAck.source_jws` at `:373-374`, `CleanupAck.source_jws` at `:380-381`, `NodeRegistration.signer_jws` / `ed25519_signature`), with tests in the style of `wire/registration_request_only_test.go`. Encode sites to extend: `wire/command.go:136-138` (RegisterRC), `:159-161`, `:172-174`, `:186-196` (RegisterNode, MarkActive, EvictNode), `:250-260` (SeedLegacyTables, AddTable), `:273-276` (RecordTablePurged); decode sites `:334-351`, `:360-398`.
- Converters to extend: `wire/consensus.go:12-42` (`ConsensusParamsUpdateFromPB` / `ToPB`), `wire/convert.go:94-112` (RC), `:144-154` (registration), `:203-223` (acks), `wire/table_registry_snapshot.go:150-222` (snapshot, incarnation `OwnerIndexerID`, `SIIndexers`, `SeededIndexers`).

**arbiter (Tasks 5–12).**
- `server/table_registry_test.go:26-49` `TestTableRegistryMirrorsProto` fails as soon as arbiter resolves `PROTO_TAG` (directly or through `CORE_TAG`) until `fsm.TableIncarnation` gains `OwnerIndexerID *uint64` with tag `owner_indexer_id,omitempty`; land both in one commit.
- `server/table_registry.go:99-123` (`tableRegistryToPB`) builds `pb.TableRegistrySnapshot` / `pb.TableIncarnation` and `server/consensus_admin.go:74` builds `pb.ConsensusMutableParams`; both must copy the new fields.
- `server/membership.go:19-62`: `RegisterNode` proposes `wire.RegistrationFromPB(req)` and `MarkActive` copies only `GetNodeId()`. After the bump the registration converter carries `registration_seq`, so before activation the handler must zero it and drop the signatures (contract §0); after activation it moves them into the wrapper fields.

**housegate.** `pkg/storageintegrity/arbiter_proto.go:389-412` `SubmitOutcomeFromSequencedAck` maps an unknown code to `OutcomeUnknown` (the `default` branch), so a housegate still on `v0.9.0` sees code 10 as unknown (status-probe convergence, no prompt cleanup of its prepared parts), not as the retryable, session-preserving refusal of spec §6.4 / §9. The arbiter returns code 10 only after activation, so the housegate mapping (spec §14 sub-project 4) has to be in every SI host's image before the activation update commits.

**Gotchas.**
- `conformance/snapshot_query_test.go` freezes every message of two old baselines and admits only listed trailing additions; stage 2 (track and quarantine fields on old messages) must extend `signedClaimsFields`'s pattern again and strip its own fields first. A new message in `arbiter.proto`, `replay.proto` or `raftlog.proto` (for example a stage-2 Raft command) must also be listed in `conformance/snapshot_query_fields_test.go`.
- `make breaking` compares with the local `main` branch; run `buf breaking --against '.git#tag=<last release>'` for the real baseline.
- `make tools` installs the pinned `protoc-gen-go` v1.36.11 and `protoc-gen-go-grpc` v1.5.1 into `~/go/bin`; always run it before `make proto` (the module cache also holds `protoc-gen-go-grpc` v1.6.2, whose output would fail CI's drift check).

### Tasks 2–4: contract conflicts (resolved)

Line anchors: arbiter-core `050490c` (= `v0.12.1` = origin/main), arbiter `5ea60c8` (both identical to the read-only snapshots).

1. **`tableset.Options.Owner` (CONTRACT §2) does not exist.** The reconciler is configured by `tableset.Config` (`dataplane/tableset/reconciler.go:58-74`, consumed by `tableset.New(cfg Config, d Deps)`); there is no `Options` type. Proposal, used by Task 4: `tableset.Config.Owner *uint64` with the CONTRACT's semantics. Ownership is resolved by `wire.TableRegistrySnapshot.Owner(inc)` (nil `OwnerIndexerID` = `Params.SIIndexerID`), so genesis and pre-activation incarnations belong to the founding indexer both before and after CONTRACT §3 rule 8 stamps `&foundingID` on them.

2. **A signed eviction cannot be verified with `verifyCurrentAuthority` (CONTRACT §2 `EvictNodeHash` comment, §3 `EvictNode` bullet).** `verifyAuthorityJWS` refuses every purpose but `authority.PromotionPurpose` (arbiter `fsm/authorityjws.go:49`), so a token with the CONTRACT's own purpose `arbiter-evict-node-v1` is always refused; and `verifyAuthorityContext` (`fsm/authorityjws.go:83-114`) accepts a token without context at epoch 0, which would let an eviction signed for one network be replayed on another network sharing the authority key. Proposal: the signed `EvictNode` Apply calls `(&authority.Validator{AllowedAddresses: <lowercase set of f.st.Params.AuthorityAddresses>}).VerifyEvictNode(c.Canonical(), c.AuthorityJWS, authority.ConsensusContext{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: <f.genesisSnapshotIDLocked()>, AuthorityEpoch: f.st.ConsensusEpoch})` (Task 3, `authority/evict_node.go`: deterministic, no clock, exact context, no epoch-0 exception; `MaxTokenAge` is ignored when age is not enforced, `authority/validator.go:56-62`); the `ConsensusAdmin.EvictNode` RPC calls `AuthorizeEvictNode` with a positive `MaxTokenAge` before proposing. `verifyCurrentAuthority` stays as is for promotions and cleanups.

3. **Neither data-plane role loads a genesis snapshot id (CONTRACT §2 "the genesis snapshot id it already loads").** `snode.Config` (`snode/config.go:22-60`) and `verifier.Config` (`verifier/config.go:20-51`) carry only `NetworkID`, `SchemaSnapshotID`, `ExecutorProfileID` and `Tables`; the arbiter derives the id in `cmd/arbiter/genesis.go:16-24` and the FSM reads it through `genesisSnapshotIDLocked` (`fsm/consensus_reads.go:61-75`). Proposal, used by Task 4: `Config.GenesisSnapshotID` on both roles, an empty value derived with the new `dataplane.GenesisSnapshotID` (the arbiter's derivation byte for byte, valid only over the network's complete genesis set); a signing SNode with no genesis table must configure it or `New` fails. A non-founding indexer's SNode never holds genesis tables, so its host supplies the id from configuration or from `ConsensusAdmin.GetConsensusParams().genesis_snapshot_id` (unauthenticated read on the management listener, `server/consensus_admin.go:49-66`).

4. **A strict `RegistrationSeq > RegistrationSeqs[NodeID]` (CONTRACT §3 signed messages, §3a) rejects the client's own retry.** `dataplane.(*Client).WithLeaderRetry` (`dataplane/client.go:168`) resends the identical `RegisterNode` request after an uncertain outcome (leader lost after commit, deadline after commit); under a strict `>` the resend is `Rejected`, `Register` returns an error and the role fails startup although its registration committed. Proposal for arbiter Tasks 5–7: after activation, accept `seq == RegistrationSeqs[NodeID]` when the canonical registration equals the stored `Nodes[NodeID].Registration` and its signature verifies, as an idempotent no-op (`Applied{}` with no status change, never a reset to Syncing, unlike today's re-registration at `fsm/apply.go:71-75`; adopted as contract §3b.4); refuse an equal seq with any other body and every lower seq. `MarkActive` at the equal seq is already idempotent. Task 4 `TestRegister_RetriesReuseOneSeqAndSignature` pins the client side: one `Register` call sends one seq and one token, however often it is retried.

5. **`GetPurgeNodeSet` after activation (CONTRACT §3 "Purge set", §3a).** The RPC takes `google.protobuf.Empty` (`dataplane/table_purge.go:41-52`): one network-wide set, which the reconciler uses as live replica membership. `sweep` (`dataplane/tableset/reconciler.go:698-718`) drops every Keeper replica outside it, and `clearDecommissioned` (`:729-762`) refuses to create an incarnation while a replica outside it exists (verifiers never sweep, so they wait). With "the owner SNode of each purging incarnation plus every verifier", a key owned by an SNode with no incarnation in Purging lists that SNode's live replica as decommissioned: verifiers would refuse to re-create such a key forever, and only the owner filter would stand between an SNode sweep and a live replica. Proposal (the §3a "otherwise document how the client picks its own membership" branch): `GetPurgeNodeSet` keeps returning every registered non-evicted SNODE and VERIFIER, before and after activation; the per-incarnation completion rule (`entry(owner).SNodeNodeID` plus every non-evicted verifier) lives only in the FSM's report recording and purge completion, which refuse a report from a non-owner SNode. The client needs no change: with a claim signer the SNode purges, reports and sweeps only owned keys.

6. **`snode.Config.IndexerID` as an unconditional owner filter (CONTRACT §2).** `uint64` zero is a valid indexer id (devnet2's founding indexer is 0), so a host that never sets it would silently scope a legacy SNode to indexer 0, which is wrong on any network whose founding id is not 0 and is a pre-activation behaviour change. Proposal, used by Task 4: the owner filter and every owner-scoped read apply only when `Deps.ClaimSigner != nil` (`(*Role).ownerFilter` / `owns` in `snode/claims.go`); without a signer the SNode serves every incarnation as today. Hosts set `IndexerID` together with `ClaimSigner`.

7. **`signed_claims_v1` advertisement (CONTRACT §2 `LocalNodeFeatures`, §3 activation gate).** Both roles send `arbiter.LocalNodeFeatures()` in RegisterNode today (`snode/snode.go:146`, `verifier/verifier.go:119`), so an SNode built from `CORE_TAG` but run without a claim signer would satisfy the gate and then be unable to register after activation. Proposal, used by Task 4: the SNode removes `signed_claims_v1` from RegisterNode's features when `ClaimSigner == nil`; the verifier always holds its evidence key, always signs and always advertises it. Corollary: voters return `arbiter.LocalNodeFeatures()` from `GetProtocolInfo` (`server/consensus_admin.go:39`), so the first arbiter build pinning `CORE_TAG` advertises `signed_claims_v1` whether or not its FSM implements the rules; arbiter must pin `CORE_TAG` in the same release as Tasks 5–11 (or filter the feature in `GetProtocolInfo` until they land), never in an intermediate release.

8. **`snode.Config` refuses an empty genesis table set (`snode/config.go:77-79`) while Plan S1-B lets a non-founding indexer's `table_ids` be empty.** Proposal, used by Task 4: the check moves to `New`, where empty `Tables` is allowed only with `Deps.Registry != nil` and `Deps.ClaimSigner != nil`. The schema-root comparison (`snode/config.go:131-134`) still runs, so such a host configures `SchemaRoot = payloadexec.SchemaRoot(NetworkID, nil)` = `0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59` (sha256 of `schema-root\x00`, the same for every network id).

9. **The verifier keeps no on-disk state (CONTRACT §2 "if it has none, add a small JSON file ... and say so").** It has none (`verifier/config.go:20-51`), so Task 4 adds `verifier.Config.StateDir` holding `registration.json` (`{"registration_seq":N}`, temp file + fsync + rename + directory fsync). Empty `StateDir` keeps the sequence in memory: still clock-floored, but a restart after a backwards clock step can reuse a lower value, which the FSM then refuses until the clock passes it. Production verifiers should set it (arbiter `cmd/arbiter-verifier/main.go:207`, `toRoleConfig`).

### Tasks 2–4: facts for other tasks

**Repository, pins and release.**
- arbiter-core base `050490c5af42c531a80f2ef1d354e7a2bf7906f5` = `v0.12.1` = origin/main; worktree `~/src/remotesrc/claude/arbiter-core/multi-source-s1`, branch `urwt/claude/multi-source-s1` (URWT). Bazel targets touched: `//:arbiter-core_test`, `//authority:authority_test`, `//authority/authoritytest:authoritytest_test` (new), `//wire:wire_test`, `//conformance:conformance_test`, `//dataplane:dataplane_test`, `//dataplane/tableset:tableset_test`, `//snode:snode_test`, `//verifier:verifier_test`.
- `PROTO_TAG` pin in arbiter-core: plain `go.mod` require (`go.mod:14`) resolved by `go_deps.from_file` (`MODULE.bazel:29`, `use_repo` `com_github_sentioxyz_arbiter_proto`): `GOWORK=off go get github.com/sentioxyz/arbiter-proto@$PROTO_TAG && GOWORK=off go mod tidy && bazel mod tidy && bazel run //:gazelle`; no script, no `MODULE.bazel` edit. The pin and the conformance mirror update (`conformance/arbiter_wire_test.go`) must be one commit (Task 2 Step 3), or `TestArbiterMirrorsMatchProto` is red in between.
- `CORE_TAG`: Cut Release workflow (`gh workflow run cut-release.yml --repo sentioxyz/arbiter-core --ref main`), annotated `vX.Y.Z`, never predicted; verified by content (Task 4 Step 17). arbiter pins it with `bash scripts/update-arbiter-core.sh <tag>`; sentio-node pins `go.mod` plus `MODULE.bazel` `bazel_dep` version and `git_override` commit `git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core rev-parse "<tag>^{commit}"`.

**Exported identifiers (all names verbatim).**
- package `arbiter`: `const SignedClaimsFeature = "signed_claims_v1"`; `func LocalNodeFeatures() []string` → `[client_lanes_v1 signed_claims_v1]` (fresh slice per call); `const AdmissionCodeSourceUnavailable AdmissionCode = 10`; `type SIIndexerEntry struct{ IndexerID uint64; ActivationBlock uint64; Signer string; SNodeNodeID string; EnrollmentJWS string }` (JSON `indexer_id`, `activation_block`, `signer`, `snode_node_id`, `enrollment_jws`) and `func (e SIIndexerEntry) Validate() error`; `type VerifierEntry struct{ NodeID string; Ed25519Pubkey []byte }` (JSON `node_id`, `ed25519_pubkey`) and `func (e VerifierEntry) Validate() error`; `ConsensusParamsUpdate.SIIndexers []SIIndexerEntry` (`si_indexers,omitempty`), `ConsensusParamsUpdate.Verifiers []VerifierEntry` (`verifiers,omitempty`); `NodeRegistration.RegistrationSeq uint64` (`registration_seq,omitempty`, canonical, signed, rides inside `NodeRegistration` in the Raft command); `type EvictNodeCommand struct{ NodeID string; ExpectedRegistrationSeq uint64; Reason string }` (JSON `node_id`, `expected_registration_seq`, `reason`).
- package `authority`:
  - enrolment: `const SNodeEnrollmentPurpose = "arbiter-snode-enrollment-v1"`; `type SNodeEnrollmentStatement struct{ NetworkID string; GenesisSnapshotID string; IndexerID uint64; SNodeNodeID string }` (JSON `network_id`, `genesis_snapshot_id`, `indexer_id`, `snode_node_id`); `func SNodeEnrollmentHash(stmt SNodeEnrollmentStatement) (string, error)` (domain `arbiter-snode-enrollment-statement-v1`); `func (s *Signer) SignSNodeEnrollment(stmt SNodeEnrollmentStatement) (string, error)` (iat = now); `func (s *Signer) SignSNodeEnrollmentAt(stmt SNodeEnrollmentStatement, iat int64) (string, error)`; `func VerifySNodeEnrollment(stmt SNodeEnrollmentStatement, jws, signer string) error`.
  - SNode messages: `const SNodeMessagePurpose = "arbiter-snode-message-v1"`; `type SNodeMessageKind string` with `SNodeMessageRegistration` `"registration"`, `SNodeMessageMarkActive` `"mark_active"`, `SNodeMessageResultClaim` `"result_claim"`, `SNodeMessagePromotionAck` `"promotion_ack"`, `SNodeMessageCleanupAck` `"cleanup_ack"`, `SNodeMessageTablePurged` `"table_purged"`; `type MessageContext struct{ NetworkID string; GenesisSnapshotID string }`; `type MarkActiveBody struct{ NodeID string; RegistrationSeq uint64 }`; `type TablePurgedBody struct{ NodeID string; IncarnationSeq uint64 }`; `func SNodeMessageHash(kind SNodeMessageKind, ctx MessageContext, body any) (string, error)` (domain `arbiter-snode-message-body-v1`); `func (s *Signer) SignSNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any) (string, error)`; `func (s *Signer) SignSNodeMessageAt(kind SNodeMessageKind, ctx MessageContext, body any, iat int64) (string, error)`; `func VerifySNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any, jws, signer string) error`.
  - verifier messages: `type VerifierMessageKind string` with `VerifierMessageRegistration` `"registration"`, `VerifierMessageMarkActive` `"mark_active"`, `VerifierMessageTablePurged` `"table_purged"`; `func VerifierMessageHash(kind VerifierMessageKind, ctx MessageContext, body any) (string, error)` (domain `arbiter-verifier-message-body-v1`); `func SignVerifierMessage(priv ed25519.PrivateKey, kind VerifierMessageKind, ctx MessageContext, body any) (string, error)`; `func VerifyVerifierMessage(pub ed25519.PublicKey, kind VerifierMessageKind, ctx MessageContext, body any, sigHex string) error`.
  - eviction: `const EvictNodePurpose = "arbiter-evict-node-v1"`; `func EvictNodeHash(cmd arbiter.EvictNodeCommand) (string, error)` (domain `arbiter-evict-node-command-v1`; empty node id or reason is an error); `func (s *Signer) SignEvictNodeWithContext(cmd arbiter.EvictNodeCommand, ctx ConsensusContext) (string, error)`; `func (s *Signer) SignEvictNodeWithContextAt(cmd arbiter.EvictNodeCommand, ctx ConsensusContext, iat int64) (string, error)`; `func (v *Validator) VerifyEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error)` (Apply/restore); `func (v *Validator) AuthorizeEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error)` (RPC, enforces `MaxTokenAge`). Both return the recovered lowercase authority address.
  - `NormalizeConsensusParamsUpdate` keeps its signature, copies its input (never sorts the caller's slices), returns `SIIndexers` sorted by id with lowercase signers and `Verifiers` sorted by node id with cloned keys, nil for absent or empty lists; error texts: `si_indexers: indexer <id>: activation_block must be at least 1`, `… signer must be a non-zero lowercase 0x-prefixed 20-byte address`, `… snode_node_id must be non-empty and free of whitespace and control characters`, `… enrollment_jws is required`, `si_indexers: duplicate indexer_id <id>`, `si_indexers: snode_node_id "<id>" is enrolled twice`, `si_indexers: max_writers <n> is below the <m> enrolled indexers`, `verifiers: node_id is required`, `verifiers: <id>: ed25519_pubkey must be 32 bytes, got <n>`, `verifiers: duplicate node_id "<id>"`, `verifiers: <id> reuses another verifier's ed25519_pubkey`, each prefixed `consensus params update: `.
- package `authority/authoritytest` (import `github.com/sentioxyz/arbiter-core/authority/authoritytest`, test support, never provision its keys): constants `NetworkID` = `devnet2`, `GenesisSnapshotID` = `0xgenesis` (a fixture, not devnet2's real genesis id), `Iat` = `1760054400`, `RegistrationSeq` = `1760054400000`, `ActivationBlock0` = `5508931`, `AuthorityKeyHex`/`AuthorityAddr` (`0x970e8128ab834e8eac17ab8e3812f010678cf791`), `IndexerKeyHex0`/`IndexerAddr0` (`0x563bd9e11d18b6ea60c2f159f8d3062d30e8039e`), `IndexerKeyHex1`/`IndexerAddr1` (`0x89fd7c610ac4aa2e17c3b15a2c386a4b215f96d9`), `StrangerKeyHex`/`StrangerAddr` (`0x009cfebab1cc20d08e23eb7ea89dcefab3349e45`), `SNodeNodeID0` = `snode-1`, `SNodeNodeID1` = `snode-2`; `func MustSigner(t testing.TB, keyHex string) *authority.Signer`; `type SNodeEnrollmentVector struct{ Statement authority.SNodeEnrollmentStatement; KeyHex, Signer string; Iat int64; Hash, JWS string }`; `var SNodeEnrollmentVectors []SNodeEnrollmentVector` (index = indexer id 0 and 1, both in the fixture context); `func Context() authority.MessageContext`; `func ConsensusContext(epoch uint64) authority.ConsensusContext`; `func VerifierNodeID(i int) string` (`verifier-<i>`); `func VerifierKey(i int) ed25519.PrivateKey` (seed byte 0 = 6+i); `func VerifierEntries() []arbiter.VerifierEntry` (verifiers 1–3); `func SIIndexerEntry0() arbiter.SIIndexerEntry`; `func SIIndexerEntry1(activationBlock uint64) arbiter.SIIndexerEntry`; `func ActivationUpdate() arbiter.ConsensusParamsUpdate`; body fixtures (all `func … ()`): `SNodeRegistration() arbiter.NodeRegistration`, `SNodeMarkActive() authority.MarkActiveBody`, `ResultClaim() arbiter.RCRecord`, `PromotionAck() arbiter.PromotionAck`, `CleanupAck() arbiter.CleanupAck`, `SNodeTablePurged() authority.TablePurgedBody`, `VerifierRegistration() arbiter.NodeRegistration`, `VerifierMarkActive() authority.MarkActiveBody`, `VerifierTablePurged() authority.TablePurgedBody`, `EvictCommand() arbiter.EvictNodeCommand`; vector constants `SNodeRegistrationHash`/`JWS`, `SNodeMarkActiveHash`/`JWS`, `ResultClaimHash`/`JWS`, `PromotionAckHash`/`JWS`, `CleanupAckHash`/`JWS`, `SNodeTablePurgedHash`/`JWS` (all signed by `IndexerKeyHex1` for `snode-2`), `VerifierRegistrationHash`/`Signature`, `VerifierMarkActiveHash`/`Signature`, `VerifierTablePurgedHash`/`Signature` (by `VerifierKey(1)`), `EvictHash`/`EvictJWS` (by `AuthorityKeyHex` in `ConsensusContext(2)`), `ActivationUpdateHash` = `0xf3a4e05aa1109b05ef7e9b9b1d0775c8f529699965f6d8578c7155a7c94a098d`, `Verifier1PubkeyHex`..`Verifier3PubkeyHex`. These are consensus constants: never regenerate them.
- package `wire`:
  - list converters `func SIIndexerEntriesFromPB(ms []*pb.SIIndexerEntry) []arbiter.SIIndexerEntry`, `func SIIndexerEntriesToPB(v []arbiter.SIIndexerEntry) []*pb.SIIndexerEntry`, `func VerifierEntriesFromPB(ms []*pb.VerifierEntry) []arbiter.VerifierEntry`, `func VerifierEntriesToPB(v []arbiter.VerifierEntry) []*pb.VerifierEntry`; `ConsensusParamsUpdateFromPB`/`ToPB` carry both lists; `RegistrationFromPB`/`RegistrationToPB` carry `registration_seq` (never `features`, `signer_jws`, `ed25519_signature`).
  - fields: `AddTable.OwnerIndexerID *uint64`, `SeedLegacyTables.IndexerID *uint64`, `TableIncarnation.OwnerIndexerID *uint64`, `TableRegistrySnapshot.SIIndexers []arbiter.SIIndexerEntry`, `TableRegistrySnapshot.SeededIndexers []uint64`; `RegisterRC.SourceJWS`, `RecordPromotionAck.SourceJWS`, `RecordCleanupAck.SourceJWS`, `RegisterNode.SignerJWS`/`.Ed25519Signature`, `MarkActive.RegistrationSeq`/`.SignerJWS`/`.Ed25519Signature`, `EvictNode.ExpectedRegistrationSeq`/`.AuthorityJWS`, `RecordTablePurged.SignerJWS`/`.Ed25519Signature`.
  - helpers: `func (s TableRegistrySnapshot) SignedClaimsActive() bool`, `func (s TableRegistrySnapshot) SIIndexer(indexerID uint64) (arbiter.SIIndexerEntry, bool)`, `func (s TableRegistrySnapshot) Owner(inc TableIncarnation) uint64`, `func (c EvictNode) Canonical() arbiter.EvictNodeCommand`, `func StripSignedClaims(c Command) Command`.
  - request converters: `RegisterRCFromRequest(*pb.RCRecord) RegisterRC`, `RegisterRCToRequest(RegisterRC) *pb.RCRecord`, `RecordPromotionAckFromRequest(*pb.PromotionAck) RecordPromotionAck`, `RecordPromotionAckToRequest(RecordPromotionAck) *pb.PromotionAck`, `RecordCleanupAckFromRequest(*pb.CleanupAck) RecordCleanupAck`, `RecordCleanupAckToRequest(RecordCleanupAck) *pb.CleanupAck`, `RegisterNodeFromRequest(*pb.NodeRegistration) RegisterNode`, `RegisterNodeToRequest(c RegisterNode, features []string) *pb.NodeRegistration`, `MarkActiveFromRequest(*pb.NodeRef) MarkActive`, `MarkActiveToRequest(MarkActive) *pb.NodeRef`, `RecordTablePurgedFromRequest(*pb.RecordTablePurgedCmd) RecordTablePurged`, `RecordTablePurgedToRequest(RecordTablePurged) *pb.RecordTablePurgedCmd`, `EvictNodeFromRequest(*pb.EvictNodeRequest) EvictNode`, `EvictNodeToRequest(EvictNode) *pb.EvictNodeRequest`.
- package `dataplane`: `func (c *Client) SubmitTablePurgedReport(ctx context.Context, report wire.RecordTablePurged) error` (`SubmitTablePurged` is now its unsigned wrapper); `type PurgeReporter struct{ Client *Client; Sign func(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error) }` with `SubmitTablePurged(ctx, nodeID, incarnationSeq) error` (refuses a signed report naming another node or seq) and `PurgeNodeSet(ctx) ([]string, error)`, a `tableset.Arbiter`; `func GenesisSnapshotID(networkID, schemaSnapshotID, executorProfileID string, genesis []payloadexec.TableSchema) (string, error)`.
- package `dataplane/tableset`: `Config.Owner *uint64`.
- package `snode`: `Config.IndexerID uint64`, `Config.GenesisSnapshotID string`, `Deps.ClaimSigner *authority.Signer`, `Deps.Now func() time.Time` (the clock injection point for the `registration_seq` floor and every token's `iat`; nil = `time.Now`), `var ErrTableNotOwned` (wraps `ErrSchemaUnknown`, so `errors.Is(err, snode.ErrSchemaUnknown)` stays true); `state.json` gains `registration_seq`.
- package `verifier`: `Config.StateDir string` (`registration.json`), `Config.GenesisSnapshotID string`, `Deps.Now func() time.Time`.

**Verification call shapes for the arbiter FSM and server (Tasks 5–12).** `ctx := authority.MessageContext{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: genesisID}` with `genesisID, err := f.genesisSnapshotIDLocked()` (`fsm/consensus_reads.go:61`; refuse when it errors or is empty). Every verify is deterministic (no clock, no token age), so it is safe in Apply and on snapshot restore.
- Enrolment (consensus rule 4): `authority.VerifySNodeEnrollment(authority.SNodeEnrollmentStatement{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: genesisID, IndexerID: e.IndexerID, SNodeNodeID: e.SNodeNodeID}, e.EnrollmentJWS, e.Signer)`.
- `RegisterNode`, SNODE: `authority.VerifySNodeMessage(authority.SNodeMessageRegistration, ctx, c.Registration, c.SignerJWS, entry.Signer)`; VERIFIER: `authority.VerifyVerifierMessage(ed25519.PublicKey(entry.Ed25519Pubkey), authority.VerifierMessageRegistration, ctx, c.Registration, c.Ed25519Signature)`. `c.Registration` is the decoded `arbiter.NodeRegistration` including `RegistrationSeq` and `DialAddr`; features are never signed.
- `MarkActive`: body `authority.MarkActiveBody{NodeID: c.NodeID, RegistrationSeq: c.RegistrationSeq}` with `SNodeMessageMarkActive` / `VerifierMessageMarkActive`.
- `RegisterRC`: `authority.VerifySNodeMessage(authority.SNodeMessageResultClaim, ctx, c.RC, c.SourceJWS, entry.Signer)`; `RecordPromotionAck`: `(authority.SNodeMessagePromotionAck, ctx, c.Ack, c.SourceJWS, signer)`; `RecordCleanupAck`: `(authority.SNodeMessageCleanupAck, ctx, c.Ack, c.SourceJWS, signer)`.
- `RecordTablePurged`: SNODE `authority.VerifySNodeMessage(authority.SNodeMessageTablePurged, ctx, authority.TablePurgedBody{NodeID: c.NodeID, IncarnationSeq: c.IncarnationSeq}, c.SignerJWS, signer)`; VERIFIER `authority.VerifyVerifierMessage(pub, authority.VerifierMessageTablePurged, ctx, authority.TablePurgedBody{…}, c.Ed25519Signature)`.
- `EvictNode`: see Contract conflict 2 (`VerifyEvictNode` in Apply, `AuthorizeEvictNode` at the RPC).
- Server request decoding: `RegisterNode` → `wire.RegisterNodeFromRequest(req)` (features stay request-only; the feature book keeps reading `req.GetFeatures()`), `MarkActive` → `wire.MarkActiveFromRequest(req)`, `RegisterResultClaim` → `wire.RegisterRCFromRequest(req)`, `AckPromotion` / `AckCleanup` → `wire.RecordPromotionAckFromRequest` / `wire.RecordCleanupAckFromRequest`, `SubmitTablePurged` → `wire.RecordTablePurgedFromRequest(req)`, `EvictNode` → `wire.EvictNodeFromRequest(req)`; then `wire.StripSignedClaims(cmd)` while the signed-claims activation is not committed. Call sites in arbiter `5ea60c8`: `server/membership.go:42` (RegisterNode), `:56` (MarkActive), `server/claims.go:21,40` (RegisterRC), `server/gateway.go:114` (promotion ack), `:134` (cleanup ack), `server/table_purge.go:39` (purge report), `tableregistry/derive.go:116` (AddTable), `:201-232` (SeedLegacyTables). Registry and params read-backs: `server/table_registry.go:99-123` (`tableRegistryToPB` must add `SiIndexers: wire.SIIndexerEntriesToPB(v.SIIndexers)`, `SeededIndexers` and each incarnation's `OwnerIndexerId`) and `server/consensus_admin.go:69-80` (`consensusMutableParams` must add `SiIndexers: wire.SIIndexerEntriesToPB(p.SIIndexers), Verifiers: wire.VerifierEntriesToPB(p.Verifiers)`). Hosts: `cmd/arbiter-snode/config.go:217` + `main.go:206` (`IndexerID`, `GenesisSnapshotID`, `snode.Deps.ClaimSigner`), `cmd/arbiter-verifier/config.go:232` + `main.go:207-216` (`StateDir`, `GenesisSnapshotID`).

**Gotchas.**
- **Fork safety.** `wire.RegistrationFromPB` now copies the canonical `registration_seq`, so an unchanged server path (`server/membership.go:42`) would put it in a Raft command that v0.12.1 voters refuse (strict decoder). Every pre-activation proposal must pass through `wire.StripSignedClaims`; `UpdateConsensusParams` passes through it unchanged. `StripSignedClaims` also nils `SeedLegacyTables.IndexerID`, which would turn a per-indexer seed into a founding seed: call it only on pre-activation proposals (no per-indexer seed can exist then), never after activation.
- `wire.Decode` refuses a signature inside the Raft command's request-only copy (`RegisterRCCmd.rc.source_jws`, `RecordPromotionAckCmd.ack.source_jws`, `RecordCleanupAckCmd.ack.source_jws`, `RegisterNodeCmd.registration.signer_jws` / `ed25519_signature`; error contains `request-only`): always build commands with the `*FromRequest` converters, which move the signature into the command field.
- **nil versus `&0`.** `AddTable.OwnerIndexerID`, `SeedLegacyTables.IndexerID` and `TableIncarnation.OwnerIndexerID` are `*uint64` (proto3 `optional`); nil (absent) and `&0` (indexer 0, devnet2's founding indexer) round-trip distinctly. Read the proto field (`m.OwnerIndexerId`), never `GetOwnerIndexerId()`, which answers 0 for absent; copy with `cloneUint64`. `TableRegistrySnapshot.Owner(inc)` maps nil to `Params.SIIndexerID`.
- **Empty versus nil.** Absent or empty lists normalise to nil everywhere (`NormalizeConsensusParamsUpdate`, the four list converters, `SeededIndexers`); message hashing canonicalises top-level repeated fields and an empty pubkey to nil before `CanonicalDigest`, so `[]` and nil sign the same bytes. The pre-activation canonical forms are pinned by `wire/signed_claims_golden_test.go` (`TestPreSignedClaimsCommandBytesAreFrozen`, `TestConsensusParamsUpdateWithLanesHashIsFrozen`).
- **`registration_seq` rule (binding amendment).** Before each `Register` call a signing SNode, and every verifier, computes `next := max(persisted+1, uint64(now().UnixMilli()))` with the injected `Deps.Now`, persists `next` durably (temp file + fsync + rename + directory fsync: SNode `state.json` via `persistStateLocked`, verifier `<StateDir>/registration.json`) before the first `RegisterNode` leaves, and uses that one value for every retry of the registration and for the following `MarkActive`. A lost state file still moves forward (clock floor); a clock stepping back still moves forward (persisted + 1). A legacy SNode (no claim signer) sends 0 and persists nothing. The FSM only checks `seq > RegistrationSeqs[node]` (plus the exact-duplicate case of Contract conflict 4).
- **Tokens are not idempotency keys.** Every SNode token carries `iat = Deps.Now()`; a message rebuilt later (RC re-registration after restart, an acknowledgement re-sent from a later stream) carries a different JWS for the same body. The FSM verifies the token and compares canonical bodies only; it must not compare or key on token bytes. Within one call `WithLeaderRetry` resends identical bytes.
- **Error mapping.** An FSM `Rejected` reaches clients as gRPC `InvalidArgument` with the reason (`server/server.go:255-257`), which `WithLeaderRetry` does not retry (it retries `Unavailable`, `DeadlineExceeded`, `Canceled`, `Unknown`, `dataplane/client.go:242-244`): a refused registration surfaces from `(*snode.Role).Register` as `register snode: rpc error: code = InvalidArgument desc = <reason>`.
- **Owner scoping only with a signer.** With `Deps.ClaimSigner` set the SNode materialises, verifies, purges, reports and sweeps only `IndexerID`'s keys, refuses intake, prepared lookup and promoted-unsafe reads for other owners with `ErrTableNotOwned`, builds its source-claim root from owned Active/Retiring incarnations only, and drops a promotion or cleanup for a foreign table without acknowledging it. Before activation every incarnation resolves to the founding indexer, so the founding SNode behaves exactly as today.
- **Compatibility of signing nodes.** A signing SNode or verifier works against a v0.12.1-based arbiter (unknown request fields are ignored, `RegistrationFromPB` there copies nothing new) and against a new arbiter before activation (the server strips), so data-plane nodes can be upgraded before the arbiter.
- **Genesis snapshot id.** `dataplane.GenesisSnapshotID` reproduces the arbiter's id only over the network's complete genesis table set with the same `NetworkID`, `SchemaSnapshotID` and `ExecutorProfileID` (`snode/config.go:131-134` and the arbiter's `cmd/arbiter/genesis.go:26` both pin `SchemaRoot`); a wrong id fails every signed message at the FSM, loudly. A signing SNode without genesis tables needs `GenesisSnapshotID` and `SchemaRoot = 0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59`.
- **Signature encodings.** SNode and authority tokens are ES256K compact JWS (`JWSCommandPayload`, V+27); verifier signatures are lowercase hex (128 chars) of ed25519 over the ASCII bytes of the `0x…` hash string. `SNodeMessageHash` / `VerifierMessageHash` take the body by value; a pointer or a body of the wrong type for the kind is an error.
- **Fixtures.** `authoritytest.ActivationUpdate()` (epoch 2, `PreviousParamsDigest "0xdigest"`, `ExpectedPromotionSeq 3`, chain 7892301, `ClientLanes{256}`) is for hashing and normalisation tests, not devnet2's live update. The SNode message vectors are indexer 1 / `snode-2`; use `SNodeEnrollmentVectors[0]` with `SIIndexerEntry0()` for the founding indexer.

**For Plan S1-B.**
- Task 13 (housegate): the arbiter-core constant is `arbiter.AdmissionCodeSourceUnavailable` (= 10), proto `pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE`; `snode.ErrTableNotOwned` wraps `snode.ErrSchemaUnknown`, so existing unknown-schema handling stays terminal.
- Task 14 (sentio-node): extend `snode.Config` at `standalone/standalone.go:317-337` with `IndexerID: <self indexer id>` and `GenesisSnapshotID: <configured, or derived only on the founding indexer>`, and `snode.Deps` with `ClaimSigner: <authority.NewSignerFromHex(<indexer key hex>)>` (lowercase `(*Signer).Address()` must equal the `si_indexers` entry's `signer`); with empty `table_ids` also set `SchemaRoot` to the empty-set value above. Treat `InvalidArgument` from `Register` as "not enrolled yet" and retry with backoff; each attempt reserves a fresh `registration_seq`, which is harmless. The enrolment CLI signs `authority.SNodeEnrollmentStatement{NetworkID, GenesisSnapshotID, IndexerID, SNodeNodeID}` with `(*authority.Signer).SignSNodeEnrollment` and prints the token; the genesis id comes from configuration or from `ConsensusAdmin.GetConsensusParams().genesis_snapshot_id`.
- Task 15 (sentio-node table state): read `wire.TableRegistrySnapshot.SIIndexers`, `.SeededIndexers`, `.SignedClaimsActive()`, `.SIIndexer(id)` and `.Owner(inc)` from the registry follower's snapshot; before activation `SIIndexers` and `SeededIndexers` are nil and `Seeded` is the founding flag.
- Tasks 16–17 (production, devnet2): an SNode's configured node id must equal its `si_indexers` entry's `snode_node_id`; `max_writers` must be at least the number of entries; a verifier entry's `pubkey` is the public key of `ed25519.NewKeyFromSeed(<verifier ed25519 seed>)` (64 hex). Verifier pods need a persistent `StateDir` (Contract conflict 9).

### Tasks 5–7: contract conflicts (resolved)

1. **Rule 3 (enrolment cursor) — two clarifications.** (a) It applies to every entry an update adds except the founding entry of the first set, whose `activation_block` rule 1 pins to `table_registry.activation_block` (on devnet2 5508931, long behind the committed cursor). (b) The cursor is the *effective* one, `max(TableRegistry.Cursor.BlockNumber, table_registry.activation_block - 1)`: at the first set `State.TableRegistry` may be nil (registry enabled by the same update) or unseeded (cursor 0), and an entry activating at or before the founding seed's block could never be seeded, because the per-indexer seed needs the cursor at its own `activation_block - 1` and the founding seed jumps the cursor to the founding `activation_block - 1`. Proposal: adopt this wording. Consequence relied on by Task 6: every entry's `activation_block >= table_registry.activation_block`, so the founding activation is the earliest one.
2. **Rule 5 (SNode ids) and the activation's node sets (CONTRACT §3c, applied).** "Any registered node that is not that entry's SNode" is implemented as: at the first set a node record under an entry's `snode_node_id` must be registered with roles exactly `[SNODE]` (the grandfathered source, any status; a named SNode registered with another or an additional role is refused); for an entry added later any node record under that id (evicted included) refuses the update, because ids are never reused (spec §6.2, §12). As §3c requires, the activation evicts every running (non-evicted) SNODE no entry names and every running VERIFIER it does not list, with reason `not named by the signed-claims activation`, in the same Apply and after the new params are in place (`TestActivationEvictsUnnamedNodes`). The writer gate (`fsm/consensus_updates.go:43-51`) now runs after `signedClaimsMembershipLocked` and, at the activation only, leaves the SNodes the activation evicts out of its count, so `max_writers` need cover only the writers the activation keeps, all of which are SNodes it names (`TestActivationWriterGateExcludesItsEvictions`: `max_writers 1` with the named s1 and a stale s7 commits and leaves one running writer). Every other update counts every running writer, as today; before the activation the membership check refuses and evicts nothing, so the gates keep today's order and texts (`TestConsensusUpdateWriterLimitCountsSyncingAndExcludesEvicted` unchanged).
3. **Rule 6 (verifiers) — precise semantics.** At the activation (CONTRACT §3c) a running verifier listed under another key than the one it registered refuses the update, and an evicted node record is neither required in the list nor evicted again. After it, removal is judged on `(node_id, ed25519_pubkey)`: a running VERIFIER whose registered key is no longer listed under its id is evicted with reason `removed from verifiers`, so a later key change evicts the node registered with the old key (it re-registers with the new one). The floor counts listed entries whose node is unregistered or still running after the update's evictions. `verifiers` can be first set only by the update that first sets `si_indexers` (the activation sets both, §6.7).
4. **Registry version.** Implemented exactly as amended in CONTRACT §3a: one `registryChangedLocked` per Apply when client lanes are set or raised, `si_indexers` is first set or changed (append, signer rotation), or an eviction of the update (activation or verifier removal) completed a purge; never twice for one update.
5. **Founding seed after the activation.** As amended in CONTRACT §3a; additionally a `SeedLegacyTables` with `IndexerID = &founding` is refused ("the founding indexer is seeded without indexer_id"), so exactly one path seeds the founding indexer.
6. **Per-indexer seed placement (addition).** Besides `AtBlock.Number == entry.ActivationBlock - 1`, Apply requires the committed cursor to equal `(AtBlock.Number, AtBlock.Hash, complete)` and the founding seed to exist. The guard lets the cursor stop anywhere below `activation_block - 1`; a seed committed earlier would let a later deletion log at a block `<= activation_block - 1` (a table deleted and re-created before the activation) retire the seeded re-creation. The watcher seeds exactly at that position. Proposal: add this to the contract's registry rules.
7. **SOURCE_UNAVAILABLE position.** The contract says only "before `SpentIDs.Insert`"; implemented after the duplicate check and both fences and before the lane budget (Task 7 rationale). An owner without an entry (unreachable) answers SOURCE_UNAVAILABLE with "storage-integrity source of indexer <id> is not enrolled".
8. **Restore-side ownership validation placed in Task 6.** `validateRegistryOwnership` (and the owner-aware Legacy creation check) lands with the commands that produce the fields, so restore accepts exactly what Apply writes even before the snapshot task; the snapshot v19 task adds only the version gate and the writer bump.
9. **Snapshot-query lane not covered.** `fsm/apply_snapshot_query_submit.go:173` still hash-selects its source after the activation; the contract is silent. The lane is default-off in stage 1 (spec §3 leaves it track-unaware). Proposal: a later stage-1 task either binds it to the owner's SNode or refuses snapshot queries once signed claims are active. Resolved by contract §3a: the lane keeps hash selection and must not be enabled on a network with more than one enrolled indexer until it is made owner-aware; the README says so (Task 12).
10. **Added public read** `(*FSM).SignedClaimsActive()`, not in the contract, for gateways that must drop stage-1 fields before the activation (the `ClientLanesEnabled` precedent).

### Tasks 5–7: facts for other tasks

- **arbiter-core names used, exactly as Tasks 2–4 export them (Appendix B, Tasks 2–4 facts):** from `github.com/sentioxyz/arbiter-core/authority/authoritytest`: `MustSigner(t testing.TB, keyHex string) *authority.Signer` (always called with the test's `t`; arbiter defines no package-level signer), `IndexerKeyHex0` / `IndexerAddr0`, `IndexerKeyHex1` / `IndexerAddr1`, `StrangerAddr`, `NetworkID`, `GenesisSnapshotID`, `SNodeNodeID0` / `SNodeNodeID1`, `SNodeEnrollmentVectors` (two valid vectors, indexers 0 and 1; fields `Statement`, `KeyHex`, `Signer`, `Iat`, `Hash`, `JWS`; negative cases are derived by editing an entry), `SIIndexerEntry0()`, `SIIndexerEntry1(activationBlock)`, `VerifierEntries()` (verifier-1..3). From `wire`: `SIIndexerEntriesToPB` / `SIIndexerEntriesFromPB`, `VerifierEntriesToPB` / `VerifierEntriesFromPB` (nil in, nil out). `authority.NormalizeConsensusParamsUpdate` copies its input and its refusals read `consensus params update: si_indexers: max_writers <n> is below the <m> enrolled indexers` and `consensus params update: si_indexers: snode_node_id "<id>" is enrolled twice`; `TestSignedClaimsFirstSet` and `TestSNodeNodeIDs` assert the substrings `max_writers` and `snode_node_id`. The fixture context (`authoritytest.NetworkID` `devnet2`, `GenesisSnapshotID` `0xgenesis`) differs from the arbiter test FSMs', so FSM entries are signed on the fly with `IndexerKeyHex0` / `IndexerKeyHex1`; the frozen vectors only feed `verifySIIndexerEnrollment` directly.
- **Pin commit coupling:** `TestTableRegistryMirrorsProto` (`server/table_registry_test.go:26-49`) fails as soon as arbiter resolves `PROTO_TAG`, so Task 5 Step 1 adds `fsm.TableIncarnation.OwnerIndexerID` in the pin commit. From that commit `GetProtocolInfo.features` advertises `signed_claims_v1` (`server/consensus_admin.go:39` returns `arbiter.LocalNodeFeatures()`); no intermediate image may be deployed, and Task 11's gate trusts that list.
- **Strict-decoder obligations left to the gateway task:** after the pin `server/membership.go:42` proposes `wire.RegistrationFromPB(req)`, which now carries `registration_seq` from an upgraded node; the gateway must pass every pre-activation proposal through `wire.StripSignedClaims` (Task 3; never after the activation, where it would turn a per-indexer seed into a founding seed). Use `fsm.(*FSM).SignedClaimsActive()` (Task 5) to decide. The watcher never emits `AddTable.OwnerIndexerID` or `SeedLegacyTables.IndexerID` before the activation (`TestWatcherProposesNoStageOneFieldBeforeActivation`).
- **Signed-message task:** Tasks 5–7 tests register, activate and evict nodes only before the activation (legacy unsigned commands), or flip `f.st.Nodes[...].Status` directly in FSM tests; none depends on `registration_seq` values (CONTRACT §3a: only `seq > last`). The one post-activation data-plane message, the `RecordTablePurged` reports of Task 6's `reportPurged` (used by `TestRecreatedDatabaseOnAnotherIndexerGetsTheNewOwner`), already carries the signatures CONTRACT §3 prescribes: `SignerJWS` from `(*authority.Signer).SignSNodeMessage(authority.SNodeMessageTablePurged, ctx, authority.TablePurgedBody{…})` under indexer 1's key `authoritytest.IndexerKeyHex1` for `authoritytest.SNodeNodeID1`, `Ed25519Signature` from `authority.SignVerifierMessage(authoritytest.VerifierKey(i), authority.VerifierMessageTablePurged, ctx, …)` for each verifier, with `ctx = {testNetworkID, ConsensusParamsView().GenesisSnapshotID}`; Apply's verification must accept exactly these, or that test must change with it. Available helpers: `siIndexerLocked`, `siIndexerBySNodeLocked`, `messageContextLocked`, `findSIIndexer`, `findSIIndexerBySNode`, `removedFromVerifiersReason`, `notNamedByActivationReason`, `evictNodesLocked`. A node a consensus update evicts keeps its `RegistrationSeqs` entry, so it re-registers only with a higher seq.
- **Purge-set task:** `evictNodesLocked` (consensus update: activation and verifier removal) calls `completePurgesLocked` after the new params are in place, so the post-activation completion rule applies there too; `TestVerifierRemovalCompletesAPurgeWithOneRegistryBump` and `TestActivationEvictsUnnamedNodes` hold under both the old and the new rule (s1 is the owner's SNode). `TestRecreatedDatabaseOnAnotherIndexerGetsTheNewOwner` registers only the owner's SNode (`snode-2`, indexer 1) and the three verifiers and reports from exactly those, so it also holds under both rules, including the new refusal of a report from a non-owner SNode.
- **Snapshot v19 task:** new JSON keys already serialized with `omitempty`: `params.si_indexers`, `params.verifiers` (and the same in `bootstrap_params` and each `consensus_updates[].update`), `table_registry.seeded_indexers`, `table_registry.incarnations[].owner_indexer_id`. Before that task the writer still emits v18 for an activated state; the task adds the v19 constant, the writer bump on `len(Params.SIIndexers) > 0`, and the `< v19` refusals. `validateRegistryOwnership` and `legacyActivation` (Task 6) already make restore structurally owner-aware; `validateConsensusHistory` refuses bootstrap lists and replays the enrolment verification deterministically.
- **arbiter-admin / activation gate (Tasks 10–11):** after the activation every update must carry `si_indexers` and `verifiers` completely (FSM: "consensus update must carry the si_indexers entries" / "must carry the verifiers"); `max_writers >= len(si_indexers)`; the activation evicts every running SNODE no entry names and every running VERIFIER it does not list (reason `not named by the signed-claims activation`) and refuses a listed verifier registered under another key or a named SNode registered with another role, which is what the `--evict-unnamed` preflight of CONTRACT §3c must predict; the writer gate counts only the writers the activation keeps (the SNodes it names), so `max_writers` need not cover a stale writer it evicts, while every later update counts every running writer as today; enrolments need `activation_block > max(cursor, table_registry.activation_block - 1)`; devnet2's founding entry is indexer 0, restated with `activation_block` 5508931 and `snode-1`.
- **tableregistry API changes:** `derive` takes a `membership` instead of `arbiter.TableRegistryParams`; `siCreates` values are `siCreate{SI, Owner}`; `pending` values are `pendingCreate{Created, Owner}`; `newIndexerResolver(chain, membership)`. Halt reason and audit check label `owner_changed` (`HaltOwnerChanged`, `AuditOwnerChanged`). The watcher halts `HaltSeed` if the committed cursor ever passed an unseeded indexer's seed block.
- **Admission answer for housegate (spec §9):** code 10, message `storage-integrity source <node> of indexer <id> is not active`, `StatementSeq` 0, client_seq unspent; counted as `arbiter_admission_rejects_total{code="ADMISSION_CODE_SOURCE_UNAVAILABLE"}`.
- **Frozen artifacts added:** `fsm/testdata/pre_signed_claims_golden.json` (`TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims`, capture env `ARBITER_CAPTURE_PRE_SIGNED_CLAIMS_GOLDEN=1`); it matches the `*golden*.json` rule and is never edited. It and `fsm/testdata/legacy_lane_golden.json` must stay green through Task 12.
- **Reusable test helpers:** fsm `claimsFSM` (seeded registry, founding indexer 1 at activation block 100, `max_writers` 2, s1 + v1..v3 Active, Legacy db.old), `activateClaims`, `claimsUpdate`, `siEntry`, `foundingEntry`, `enrollment`, `testVerifiers`, `indexerSigner1(t)`, `indexerSigner2(t)`, `twoIndexerFSM`, `ownedAddCmd`, `seedIndexerCmd`, `advanceTo`, `ownerFSM` (two owners, db2.x Active owned by indexer 2 / s2), `fixedClient`; tableregistry `newMultiNode`, `multiEntries`, `multiChain`, `commitUpdate`, `harnessEntry(t, genesisID, keyHex, …)`, `checkOwnership`.

### Tasks 8–12: contract conflicts (resolved)

- **C1 (resolved by §3c). A stale registration blocked the activation; §3c makes the activation evict unnamed nodes.** Task 5's Apply evicts them; Tasks 11–12 implement the rest. `fsm.ActivationEvictions` predicts the set, and `TestActivationEvictionsMatchApply` asserts that Task 5's Apply evicts exactly that set with reason `not named by the signed-claims activation`. The leader's gate refuses unless the request accepts exactly that set, and nodes being evicted need not advertise `signed_claims_v1`. arbiter-admin lists the nodes from the feature book and submits only with `--evict-unnamed`. Open point for review, the consent transport: §3c says the leader-local gate refuses without `--evict-unnamed`, but a leader cannot see a CLI flag and no proto field may be added, so the consent rides in gRPC metadata (`arbiter-evict-unnamed`, the exact comma-separated node ids, `server.EvictUnnamedMetadataKey`). It is an operator interlock, not authority: the authority token signs the update, which alone determines the evictions in Apply. Because it names the exact set, a node that appears between the operator's review and the leader's check is refused, not silently evicted. Proposal: adopt it, or add a proto field in a later release. The book carries ids, not roles; for a node registered with both roles the leader's role-aware set can differ, the leader then names its exact set, and the operator adjusts the lists.
- **C2. The leader cannot run the voter half of the activation gate.** It knows its peers' Raft ids and transport addresses, not their administration endpoints, so it cannot call their `GetProtocolInfo`. As with the client-lanes gate (`cmd/arbiter-admin/main.go:429-650`), the voter probe stays in arbiter-admin; the leader (`signedClaimsGate`) checks its own feature book and the eviction consent, and refuses while a non-voter exists.
- **C3. No RPC exposes a node's committed `registration_seq`** for `node evict --expected-registration-seq`. Fallback implemented: the FSM refusal names the committed value ("expected_registration_seq N is not the node's current M"), and the node's own state file records it. Proposal for a later proto release: `NodeFeatureEntry.registration_seq = 4`, filled by `GetNodeFeatures`.
- **C4. "Before activation the new fields must be empty and are ignored."** Implemented as "refused with no state change" (`signedFieldsBeforeActivationReason`), checked first: an older voter refuses the same bytes at decode, so an upgraded voter that ignored the field and applied the command would hold different state. The gateway never proposes such a command (`wire.StripSignedClaims`).
- **C5. Pre-activation parked RCs stay adoptable.** RCs parked before the activation were accepted unsigned; admission after the activation still binds one whose source matches (`fsm/admission.go:146-152`). Discarding them at the activation could strand an honest claim whose statement lands right after it (the SNode treats a parked RC as registered), and `PendingRC` can hold orphans forever, so it cannot join the drain gate. Left as an accepted residual for Task 7 and reviewers.
- **C6. Role-less registrations after the activation.** The contract states the SNODE and VERIFIER rules and refuses a two-role node, and is silent on a registration with neither role. Task 8 refuses it after the activation ("exactly one of the SNODE and VERIFIER roles"): otherwise an unsigned role-less `RegisterNode` under an enrolled id would replace that node's record and drop it out of every role.
- **C7 (resolved). `authoritytest.MustSigner` signature.** The Tasks 5–7 draft now defines `indexerSigner1(t testing.TB)` and `indexerSigner2(t testing.TB)` over Task 3's `MustSigner(t, keyHex)`; Tasks 8–12's FSM tests consume those two functions (the server, orchestrator and arbiter-admin tests call `authoritytest.MustSigner(t, …)` directly).

Resolved since the first draft of these tasks: signed eviction verifies with `Validator.VerifyEvictNode` in Apply and `AuthorizeEvictNode` at the RPC (§3b.2), with `EvictNodeCommand` in package `arbiter` (Task 3); the list converters are `wire.SIIndexerEntriesToPB/FromPB` and `wire.VerifierEntriesToPB/FromPB`; `GetPurgeNodeSet` keeps every registered non-evicted SNode and verifier (§3b.5); the identical-resend rule (§3b.4); the founding-entry exemption of rule 3 and the refusal of verifiers without `si_indexers` (Task 5); stale registrations at the activation (C1, §3c); the standalone `cmd/arbiter-snode` claim signer (§3c, Task 8 Steps 13–15).

### Tasks 8–12: facts for other tasks

- **Task 5:** Tasks 8–11 append to `fsm/signed_claims.go` (`signedFieldsBeforeActivationReason`, the signature helpers, `signedRegistrationLocked`, `sameRegistration`, `evictNodeRefusalLocked`, `expectedSourceLocked`, `sourceAckRefusalLocked`, `PromotionSource`, `validateSignedClaimsState`, `ActivationEvictions`) and never touch `fsm/signed_claims_params_test.go` or `server/signed_claims_test.go`; their own tests live in `fsm/signed_messages_fixture_test.go`, `fsm/signed_messages_test.go`, `fsm/source_ack_test.go`, `fsm/purge_owner_test.go`, `fsm/snapshot_signed_claims_test.go`, `fsm/activation_evictions_test.go`, `server/signed_messages_test.go` and `server/signed_claims_gate_test.go`. They consume `claimsFSM`, `activateClaims`, `siEntry`, `foundingEntry`, `testVerifiers`, `claimsUpdate`, `indexerSigner1`, `indexerSigner2` and `registryUpdate` as drafted, `TestTableRegistryMirrorsProto` is handled by Task 5's pin commit, and `consensusMutableParams` carries both lists from Task 5. For §3c, Task 5's activation Apply must evict exactly the nodes `ActivationEvictions` names (every registered non-evicted SNODE the update does not name and every registered non-evicted VERIFIER it does not list), with reason `not named by the signed-claims activation`; `TestActivationEvictionsMatchApply` (Task 11) holds it to that, so Task 5 may call the same rule rather than restate it. Task 9's `TestSourceAcknowledgementFollowsTheStatementsIncarnation` also relies on the D9 re-creation rule (an AddTable for a key whose earlier incarnation is not Purged is recorded Refused) and on the transition-to-Active path of Task 7's `ownerFSM`.
- **Task 6:** Task 10 relies on `validateRegistryOwnership` (run by `validateTableRegistry` on every restore) and does not duplicate it; Task 8's `purgingIncarnation` uses `ownedAddCmd` with blocks 100/101/110, below the second indexer's activation block 150.
- **Task 7:** `TestDeterminism_SourceBindingAcrossActivation` keeps passing through Tasks 8–10 (its script adds no membership command after the activation, and both replicas write v19 after Task 10). Task 9's `expectedSourceLocked` reads the bound `SourceNode`, so it agrees with Task 7's owner binding by construction. The snapshot-query lane keeps hash source selection (README, Task 12).
- **Tasks 2–4:** Task 8 uses the `*FromRequest` converters and `wire.StripSignedClaims` exactly as the core facts describe, and strips only while the leader has not applied the activation (never a per-indexer seed). An FSM refusal reaches clients as `InvalidArgument`, which `WithLeaderRetry` does not retry; an identical resend at the stored sequence is `Applied` with no change (§3b.4), and `MarkActive` at the current sequence repeats harmlessly.
- **Data-plane hosts in arbiter:** Task 8 Steps 10–12 wire `cmd/arbiter-verifier` (`state_dir`, `genesis_snapshot_id`) and Steps 13–15 wire `cmd/arbiter-snode` (`ARBITER_SNODE_CLAIM_KEY_HEX`, environment only → `snode.Deps.ClaimSigner`; `indexer_id` → `snode.Config.IndexerID`, required with the key; `genesis_snapshot_id` → `snode.Config.GenesisSnapshotID`); without the key the standalone SNode stays legacy unsigned and warns. Spec §15's two-SNode integration test (two source ClickHouses, three verifiers, concurrent writes) is still not part of Tasks 8–12.
- **arbiter-admin limits:** it reads neither the L2 chain nor the registry cursor (with `consensus_admin.listen` set its address serves ConsensusAdmin alone, `cmd/arbiter/grpc.go:20-38`), so the enrolment block rule is enforced by the FSM and the 1800-block margin is a manual preflight; a later proto release could add the registry cursor to `ConsensusParamsState`.
- **Plan S1-B Task 16:** the README links the runbook `docs/storage-integrity-devnet2-multi-source.md`. Verifier pods need `state_dir` (Task 8 Step 11's YAML key) on a persistent volume. The 1800-block preflight is manual, e.g. in fish `set head (cast block safe --rpc-url $L2_RPC --json | jq -r .number | xargs printf '%d'); math $head + 1800`. `GetPurgeNodeSet` is unchanged by the activation.
- **Plan S1-B Task 17:** consume `$HOME/si-devnet2-multisource-rehearsal/bin/{old,new}/{statedigest,v19check}` (Task 12 Step 7; rebuild `old` whenever the voters' live image differs from `OLD_TAG`) and compare exactly the runbook §7.2 line set; before the activation both builds must print identical lines and no `key registration_seqs`. Before the activation update, review the eviction list `arbiter-admin consensus update` prints: every registered SNode the update does not name and every registered verifier it does not list, expected empty on devnet2 unless stale registrations exist (`arbiter-admin consensus features` shows them beforehand). Pass `--evict-unnamed` only when every listed node is meant to go; the leader accepts exactly the listed set (§3c). Every verifier and SNode that stays must re-register with the current leader advertising `signed_claims_v1` (an SNode advertises it only with its claim signer configured, §3b.7; the feature book is cleared on leadership loss and the gate refuses otherwise). `arbiter-admin node evict` works only after the activation, with the node's committed `registration_seq` (0 for `snode-1` until it re-registers after the activation). After the activation a downgraded data-plane binary is refused at registration (`signed claims are active; this binary does not sign its messages`), and promotions for indexer-a's tables go only to `snode-1` (`TestPromotionAndCleanupGoToTheExpectedSourceOnly`).
- **Release discipline:** no arbiter image between Task 5's pin commit and Task 12 (§3b.7); Task 12 Step 6 checks `git tag --contains <pin commit>` before cutting.
- **Pre-activation byte identity evidence:** Task 5's `TestPreActivationHistoryIsByteIdenticalAcrossSignedClaims` and the existing `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` (frozen containers), `TestSnapshotStaysPreV19UntilActivation`, `TestSignedFieldsAreRefusedBeforeActivation` and `TestServerDropsSignedFieldsBeforeActivation`; existing test assertions are edited only where they pin the current snapshot version (`fsm/snapshot_test.go:255`, `fsm/artifact_capacity_test.go:207`); no golden or vector file is touched.
