# Signed INSERT ... SELECT — Wave 1a-3b: C1 Service Surface (Authenticated Reads, Authority Activation, the Reachable Admission Pipeline) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give arbiter's C1 lane its service surface: the two `SafeState` reads that exist only as proto today (`GetPublishedSnapshot`, `GetQueryPolicy`) served after a leader read barrier with a canonical-JSON proof, barriers on the four existing `SafeState` reads, an authority-signed query-profile activation (Raft tag 23) that replaces the test-only `TestingActivateQueryPolicy` hook and is proposable through `ConsensusAdmin`, the capability-scoped refusals the addendum's line 474 requires (tags 10/26/27 while the capability is enabled) with explicit arms for tags 24–27, candidate-origin validation and candidate restore rules, and an `ApplyArtifactDisposition` pipeline that actually authorizes, observes, proposes and answers — with per-role principal allowlists, an FSM-backed settlement observer and an exported constructor — while `cmd/arbiter` still installs no admission dependencies, so no tag-28 command is proposable in production (Tier 3).

**Architecture:** Nothing new on the proto surface except one `ConsensusAdmin` RPC (`ActivateQueryProfile`, the proposer tag 23 never had). The two reads keep their existing messages; their opaque `terminal_proof` carries compact JSON of an arbiter-core-owned proof `{version, reply_root, body}` over a JSON-tagged body (domains `published-snapshot-reply-v1` / `query-policy-reply-v1`), the same construction as `artifact-disposition-reply-v1`, so the future client verifies with shared code. Activation records gain a kind (`publication` from `PublishCandidate`, `authority` from tag 23); snapshot format moves to v14. The admission pipeline is the shipped `prepareArtifactDispositionAdmission` made reachable, with completeness judged per action, roles aligned to the FSM vocabulary and principals configured per role. Tag 26/27 apply, `SourceClaims.RecordSnapshotArtifactReady`, the arbiter-core reply mirror/client/vectors and housegate's historical-policy export are 1a-3b'; capacity charging is 1a-3c.

**Tech Stack:** Go 1.26, Bazel 9.1.0, hashicorp/raft, arbiter-core `authority` + `wire`, arbiter-proto (`buf generate`), housegate `pkg/replay` (`CanonicalDigest`, `ActiveQueryPolicy`, `SafeSnapshotManifest`, `SnapshotArtifactReadySubmission`).

**Spec:** [design spec](../specs/2026-09-16-signed-insert-select-design.md) line 39 (authenticated authority reads: serving-endpoint authentication and `Barrier` on the leader; followers refuse or redirect); [coordination plan Task C1](2026-09-16-signed-insert-select-coordination.md) lines 57 (`GetPublishedSnapshot` retrieves authenticated committed records plus the manifest; tag 26), 59 (reads cross `Barrier`; `claims.go` accepts a self-declared SourceNode), 61 (tag 27 and publisher authorization), 69 (capability-enabled guards on the old paths), 87 (the authentication test matrix); [artifact-lifecycle addendum](../specs/2026-09-17-signed-insert-select-artifact-lifecycle-design.md) lines 154–158 (reply domain and root), 297 (validation is private, roles validator-selected), 299–311 (actor/predicate table), 364 (reads cross a leader Barrier), 462–464 (atomic capture, `read_index`, the proof shape), 474 (capability-enabled refusal of the unscoped paths), 546 (vectors — deferred to 1a-3b'); [TODO plan T1.1 / T2.2](2026-09-20-signed-insert-select-todo.md); the [Wave 1a-3 plan](2026-09-21-signed-insert-select-wave1a-3-c1-control.md) (Deferred section and execution amendments).

## Global Constraints

- Tier 3 of [issue #153](https://github.com/housegate/housegate/issues/153): merging enables no runtime capability. `cmd/arbiter` installs no `ArtifactDisposition` admission dependencies (config `artifact_disposition.enabled: true` refuses startup in this build), genesis keeps `Capability == 0`, and tag 23 activation is an explicit operator action behind `consensus_updates_enabled` with an authority signature. Nothing here changes a network that never enables the capability except that `SafeState` followers now answer `NotLeader` (the data-plane client already follows the leader hint).
- Evidence only from committed state and the leader barrier: every read crosses `consensusReadBarrier` before touching the FSM; every proof is `CanonicalDigest(domain, body)` over a JSON body captured under one FSM read lock with `read_index` = the disposition lane's `LastAppliedIndex` (the same convention as `GetArtifactDisposition`); no proof is signed (the addendum's proof is "integrity/correlation evidence, not a signature"); authenticated absence is `found=false` with a proof, never an error and never a negative fact.
- Capability-enabled refusals (addendum line 474, coordination line 69): while `ArtifactDisposition.Capability != 0`, `PublishSafeSnapshot` (tag 10), `PublishExecutorProfileTransition` (26), `RecordSnapshotArtifactReady` (27) and `ActivateQueryProfile` (23) refuse before any write with reasons that name the candidate-bound action; capability-off behavior of tag 10 and every frozen vector are unchanged.
- Roles are validator-selected (addendum line 297): the server derives the role from the action (`governance_admin`, `policy_admin`, `publisher`, `coordinator`, `source`, `verifier`, `gateway`), the recovered administrator address must be a configured principal of that role, and `Validation` is never accepted from a caller. Every action requires the administrator JWS in this wave (design decision 8).
- Frozen bytes: `wire.ArtifactDispositionCommandV1`, its command root, `L3BlockHeader`, `StatementState`, the consensus digest and every existing vector are untouched; the two read messages and `ConsensusAdmin`'s five existing methods keep their descriptors (only a sixth method is appended, with an exact conformance row).
- Snapshot format: current v13; this plan bumps to v14 by the recipe (constants, accept list + error text, predicates, per-field validators); a pre-v14 container carrying an `authority` activation is refused; a v13 activation record without a kind restores as `publication` (only `PublishCandidate` could have written it).
- Cross-repo pins move by commit: arbiter-core via `bash scripts/update-arbiter-core.sh <sha>`, arbiter-proto via `go get github.com/sentioxyz/arbiter-proto@<sha> && go mod tidy && bazel mod tidy`; `scripts/update_dependency_test.go` stays green.
- Tests: arbiter-core `bazel test //... && bash scripts/check-public-boundary.sh`; arbiter-proto `make proto && make lint && make test && go test ./...`; arbiter `bazel test //fsm:fsm_test //server:server_test --jobs=4` per task and `bazel test //... --jobs=4` (19 targets) before each PR; every refusal asserted to leave byte-identical snapshots; `reflect.DeepEqual` for successes; server tests count barriers with the `fakeNode.barrier` hook.
- Conventions: English comments; conventional commit subjects; commits end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; PR bodies end with `Refs https://github.com/housegate/housegate/issues/153`, `Design: https://github.com/housegate/housegate/pull/154` and `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. One isolated worktree per repository (`fix/153-wave1a-3b-c1`), never the main checkouts; merge only on green CI, squash with branch deletion; task review precedes every merge; single serial lane.

## Baseline facts (arbiter `origin/main` 71357d9 after PR #110; arbiter-core 86e9747; arbiter-proto 4656854; housegate 30777c8)

- **SafeState today.** `safeStateService` (`server/safestate.go:15-49`) implements `GetSafeWatermark` (:20), `GetManifest` (:29), `GetManifestByBlock` (:37), `GetL3Block` (:49) — none crosses a barrier; `GetPublishedSnapshot` / `GetQueryPolicy` fall through to `pb.UnimplementedSafeStateServer`. Barrier helpers are `*Server` methods `consensusLeaderCheck` (`server/consensus_admin.go:370-395`, `VerifyLeader` only) and `consensusReadBarrier` (`:396-428`, `VerifyLeader` + `Node.Barrier(timeout)`), both returning `notLeaderErr(s.leaderAddr())` (`server/server.go:154`) on a follower; arbiter-core's `dataplane.Client` retries on the `pb.NotLeader` detail (`dataplane/client.go:211`). `GetArtifactDisposition` (`server/artifact_disposition.go:275-323`) is the read precedent: shape → barrier → `FSM.ArtifactDispositionRead` → network check → `pb.ArtifactDispositionReplyBodyV1{…, ReadIndex: view.ReadIndex}` → `artifactDispositionProof` (`:473-483`: `replay.CanonicalDigest("artifact-disposition-reply-v1", body)` over the pb struct, `json.Marshal` of `pb.ArtifactDispositionReadProofV1{Version, ReplyRoot, Body}`, unsigned); `view.ReadIndex` is `ArtifactDispositionState.LastAppliedIndex` (`fsm/artifact_disposition_reads.go:107`). The `terminal_proof` precedent for the abort lane is compact JSON of the committed record plus its authority JWS (`fsm/apply_snapshot_query_abort.go:34-40`).
- **The two read messages exist** (`arbiter-proto proto/arbiter.proto:586-620`): `GetPublishedSnapshotRequest{network_id, keeper_shard_id, snapshot_id}` → `PublishedSnapshot{manifest, activation, artifact_ready, terminal_proof bytes}`; `GetQueryPolicyRequest{network_id, keeper_shard_id, activation_id, block_seq}` → `QueryPolicyStatus{found, activation, terminal_proof}`; no `reader_id`, `reply_nonce`, `read_index`, reply root or signature. Go mirrors and converters exist in arbiter-core `wire/snapshot_query.go:965-1069` (`PublishedSnapshotToPB/FromPB`, `QueryPolicyStatusToPB/FromPB`, …) with no caller. `TestSnapshotQueryServicesRemainUnimplemented` tests the generated `pb.Unimplemented*Server{}` stubs, so an arbiter implementation does not touch it; `assertSnapshotBaselineDescriptors` allows new service methods but freezes old messages byte-exactly.
- **State that the reads project.** `ArtifactDispositionCandidateState{CandidateSeq, Pin, Manifest, PublisherID, RetentionPolicyID, PublicationReferenceID, Origin, State, Revision, Ready replay.SnapshotArtifactReadySubmission, ObligationSeq, RegisteredIndex, TerminalIndex}` (`fsm/state.go:514-527`; `applyPublishCandidate` writes `State = published, TerminalIndex = index`); `ArtifactDispositionQueryPolicyReadiness{CandidateSeq, CommitIndex, TransitionRoot, Policy replay.ActiveQueryPolicy, Transition replay.ExecutorProfileTransition}` (`:531-543`, one non-test writer `newQueryPolicyReadiness` under `applyPublishCandidate`, only when the publication carries a transition; validated at restore by `validateArtifactDispositionQueryPolicyReadiness`, `fsm/artifact_query_policy_readiness.go:13-45`, which requires candidate seq / commit index / transition root and the named published candidate); `FSM.QueryPolicyReadiness()` (`fsm/artifact_disposition_reads.go:63-88`) returns a detached view and has no non-test caller; `SafeWatermark{SnapshotID, SafeBlockSeq, ManifestRoot}` and `Manifests map[string]*replay.SafeSnapshotManifest` are written by `applyPublishSafeSnapshot` (`fsm/apply.go:688-697`, no capability check). `TestingActivateQueryPolicy` (`fsm/apply_snapshot_query_reservation.go:268-279`) installs a bare `QueryPolicyReadiness{Policy}` under the write lock; its only wrapper is `activateServerTestQueryPolicy(t, f)` (`server/snapshot_query_reservations_test.go:42-47`) with eight call sites in `server/*_test.go`.
- **Raft tags.** `wire.Command` carries tags 23 `ActivateQueryProfile{Activation replay.ActiveQueryPolicy, AuthorityJWS}`, 24 `RecordSnapshotQueryClaim{Claim}`, 25 `RecordSnapshotQueryAttestation{Attestation}`, 26 `PublishExecutorProfileTransition{Transition, Manifest, Receipts, AuthorityJWS}`, 27 `RecordSnapshotArtifactReady{Submission}` (`arbiter-core wire/snapshot_query.go:1215-1305`) with encode/decode arms, but `FSM.Apply` (`fsm/fsm.go:60-122`) has no arm for any of them: each falls to `default:` → `Rejected{Reason: "empty command"}`. arbiter-core `authority` has four purposes (`arbiter-consensus-params-update-v1`, `housegate-artifact-disposition-command-v1`, `arbiter-promotion`, `housegate-snapshot-query-abort-v1`); none for tags 23/26/27. `authority/snapshot_query_abort.go` is the template (`ValidateSnapshotQueryAbortRecord`, `SnapshotQueryAbortHash`, `SignSnapshotQueryAbort`/`…At`, `VerifySnapshotQueryAbort` (timeless), `AuthorizeSnapshotQueryAbort` (MaxTokenAge)). `ConsensusAdmin` (`arbiter-proto proto/consensus.proto:89-…`) has five methods frozen by `TestConsensusAdminRPCSignatures` (`conformance/consensus_test.go:73-93`, `Len() != 5`); the abort RPC precedent is `AbortSnapshotQueryRequest{SnapshotQueryAbortRecord record = 1; string authority_jws = 2;}` (`:61-64`) handled by `consensusAdminService.AbortSnapshotQuery` (`server/snapshot_query_abort.go:93-…`: shape → `authority.ValidateSnapshotQueryAbortRecord` → gate → leader check → authorize with `Cfg.ConsensusAdminMaxTokenAge` → propose).
- **The admission pipeline.** `ApplyArtifactDisposition` (`server/artifact_disposition.go:191-200`) has two unconditional `FailedPrecondition` returns and never calls `prepareArtifactDispositionAdmission` (`:211-273`: deps present → command root → `authorizer.Authorize` → `artifactDispositionRole` → `consensusLeaderCheck` → `registry.Observe` + `completeArtifactDispositionRegistryObservations` → `source.Observe` + `completeArtifactDispositionSourceObservations` → `allocator.Allocate` (`ordinal != 0`) → `wire.ArtifactDispositionValidationV1{Version, CommandRoot, ActorID, ActorRole, AdministratorJWSHash, RegistryObservations, SourceObservations, CapacityAllowanceOrdinal}`). `artifactDispositionAdmissionDeps{enabled, bootstrapped, authorizer, registry, source, allocator, control}` (`:132-144`) and its four interfaces (`:100-130`) are unexported; the only production adapter is `strictArtifactDispositionAuthorizer` (`:146-163`, `authority.Validator.VerifyArtifactDispositionCommand` + role derivation, one allowlist for every role, never constructed outside tests); registry/source/allocator have test fakes only (`server/server_test.go:489-…`). `artifactDispositionRole` (`:165-189`) maps `PublishCandidate` → `publisher` and every `CancelCandidate` → `publisher`, but the FSM requires `coordinator` for `PublishCandidate` (`fsm/apply_artifact_disposition.go:349`) and for `CancelCandidate` with `origin_superseded` (`:323`) and `policy_admin` for `operator_cancelled` (`:327`); `ResolveObligation` maps to `source` while the FSM accepts `source|coordinator`. `completeArtifactDispositionSourceObservations` (`:66-81`) requires every identity to pass `validArtifactDispositionSourceIdentity` (`:58-64`), i.e. the exact-shape `validArtifactDispositionOrigin` (`fsm/apply_artifact_disposition.go:485-502`), which has no arm accepting `ExecutionOutcome` — while `settlementObservationMatchesLocked`'s `query_terminal` arm requires it. `server.Deps` (`server/server.go:44-60`) carries `ArtifactDisposition *artifactDispositionAdmissionDeps`; `cmd/arbiter/services.go:89-114` never sets it; no config key exists. `Server.propose` maps `fsm.Rejected` to a gRPC error (see `server/claims.go` and the abort handler for the mapping).
- **Candidates and origins.** `applyRegisterCandidate` (`:274-295`) requires only `a.Origin.Kind != ""` and copies the origin into the candidate and its obligation; `validArtifactDispositionOrigin` is called only through `ValidArtifactDispositionUseIdentity` (`:477-483`); its `publication` arm requires `CandidateSeq != 0`, and shipped register fixtures use `{Kind: "publication"}` (no seq) and `{Kind: "transition"}` (bare). No restore validator covers `d.Candidates` (`validateArtifactDispositionTerminalRecords`, `fsm/genesis_validation.go:377-449`, covers retirements, obligations, uses). Origin kinds in use: `genesis`, `v2`, `transition`, `reservation`, `query`, `publication`.
- **Capability gating.** `ArtifactDisposition.Capability` is read in exactly one place (`applyArtifactDisposition`, `fsm/apply_artifact_disposition.go:20`); every `ApplyServerOwned*` seam, `TestingActivateQueryPolicy`, tags 10/19/20/21/22/30 and `validatePublishTransition`'s only caller (`applyPublishCandidate`) are outside it. `legacyMutationGuard` (`fsm/artifact_capacity.go:458-463`) gates on `Capacity.Enabled`, not the capability.
- **Tests/helpers.** Server: `startServerWithConfig{f, leader, onSubscribe, custody, nodeID, updatesEnabled, artifactDisposition, snapshotQueryReservations, maxTokenAge, configureNode}` (`server/server_test.go:141-213`), `newServerTestFSM` (`:58-69`, `fsm.New(fsm.Params{NetworkID: testServerNetworkID, SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "prof"})`), `publishServerGenesis(t, node)` (`:71-82`), `fakeNode{…, verify func() error, barrier func(time.Duration) error, …}` (`:40-56`), `mustDirectApply(t, node, wire.Command)` (`server/ingress_test.go:373-388`), `TestSnapshotQueryControlRPCsCrossExactlyOneBarrierEach` (`server/snapshot_query_reservations_test.go:230`) for the barrier-count pattern, the disposition-deps tests (`server/server_test.go:504-712`) with `recordingArtifactDisposition*` fakes and `artifactDispositionTestRegistryObservations()`. FSM: `publicationFixture(t, configured) (*FSM, replay.SafeSnapshotManifest, *authority.Signer)`, `readyDispositionCandidate`, `openPublishedCandidate`, `publishedHistoricalCandidate`, `pendingTransitionPublish`, `publishedTransitionCandidate`, `signedTransitionReceipts`, `enableArtifactDispositionForTest`, `capabilityUpdate`, `consensusUpdate`/`signedConsensusUpdate`/`authorityFixture`/`mustNewFSM`, `registerActive`, `ed25519KeyFor`, `snapshotBytes`/`restoreInto`/`restoreErr`/`rewriteSnapshotDocument(t, data, version byte, mutate)`, `tamperQueryPolicyReadinessSnapshot`, `sequencedQuery`/`signedAbort`/`grantedReservation`. Snapshot bump touch points: `fsm/snapshot.go:19-32` constants, `:137` write, `:200-201` accept list + error, predicates `:218`/`:227`/`:334`, pre-vN blocks `:339-364` and `:415-430` (the v13 block is the template), `snapshotDoc` (`:40-66`).

## Design decisions (rulings carried into the tasks)

1. **Scope split.** 1a-3b (this plan) = the arbiter service surface: proofs' domains/types and the activation purpose in arbiter-core (Task 1), the `ConsensusAdmin.ActivateQueryProfile` RPC in arbiter-proto (Task 2), authority activation + record kinds + v14 (Task 3), barriers + the two reads (Task 4), the line-474 guards, explicit arms for tags 24–27, candidate origins and candidate restore rules (Task 5), the reachable admission pipeline with per-role principals, per-action completeness, the FSM-backed settlement observer and the exported constructor (Task 6), docs (Task 7). 1a-3b' = arbiter-core's reply/proof Go mirror (byte-equivalent to the pb-struct JSON arbiter hashes), the `ArtifactDispositionControl` + reads client with proof verification and leader redirect, literal root vectors for every action (twelve) plus the reply domains and the admin JWS, housegate's T2.2 export (`newAuthenticatedHistoricalPolicy` and its source interface) and the adapter over these reads, and — if the consumer needs it — `reader_id`/`reply_nonce` on the two read messages (a proto change with exact-field allowances). Tag 26/27 apply and `SourceClaims.RecordSnapshotArtifactReady` wait for publisher credentials: `arbiter.NodeRole` has no publisher role and the readiness signature scheme belongs to B1's publisher, so an FSM cannot verify a readiness signature today; 1a-3b leaves them as explicit refusals. 1a-3c = capacity charging (the allocator).
2. **Read proofs without a proto change.** `terminal_proof` = compact JSON of `wire.PublishedSnapshotReadProofV1{version:1, reply_root, body}` / `wire.QueryPolicyReadProofV1{…}` where `reply_root = CanonicalDigest(domain, body)` and the bodies are JSON-tagged Go records owned by arbiter-core (`wire/safe_state_reads.go`), so the 1a-3b' client decodes and re-derives with the same code. The body binds the request selector (`snapshot_id` / `activation_id` + `block_seq`), `network_id`, `keeper_shard_id`, `found`, `read_index` and the committed records; freshness is the leader barrier; the outer proto records are copies of the body's records and a client must compare them. Reader/nonce binding needs request fields the messages lack and is deferred with the client.
3. **Barriers on the four existing reads are unconditional.** `GetSafeWatermark`, `GetManifest`, `GetManifestByBlock`, `GetL3Block` cross `consensusReadBarrier`; a follower answers `NotLeader` with the leader hint, which arbiter-core's `dataplane.Client` already follows. The reads stay unauthenticated views (no proof); design line 39's "authenticate the serving endpoint" is transport work outside this wave.
4. **Tag 23 is the capability-off activation path.** `ActivateQueryProfile{Activation, AuthorityJWS}` is authority-signed under the new purpose `housegate-query-profile-activation-v1` (arbiter-core, mirroring the abort purpose), proposed through `ConsensusAdmin.ActivateQueryProfile` behind `consensus_updates_enabled` with `ConsensusAdminMaxTokenAge`, and applied only while `Capability == 0`: it installs `QueryPolicyReadiness{Kind: authority, ActivationIndex, AuthorityJWSHash, Policy}` for the current safe tip's executor profile (a different executor profile is a transition, which stays with `PublishCandidate`/tag 26); the same policy re-proposed is idempotent; activation block is monotone and never beyond the safe watermark. While the capability is enabled, activation belongs to the candidate-bound publication (`newQueryPolicyReadiness`), so tag 23 refuses. `TestingActivateQueryPolicy` is deleted; server tests activate through the real command.
5. **Line-474 guards and explicit arms.** `applyPublishSafeSnapshot` refuses while the capability is enabled ("safe snapshot publication must enter through the candidate-bound PublishCandidate action …"); tags 24 and 25 refuse in both modes as server-owned seams; tags 26 and 27 refuse with the candidate-bound wording while enabled and with "not implemented in this wave" while disabled. No frozen vector changes; capability-off tag 10 is untouched.
6. **Candidate origins and candidate restore rules.** `applyRegisterCandidate` validates its origin with a candidate-specific `validCandidateOrigin` (`genesis` exact; `v2` parent + block; `transition` parent + block with activation id / transition root optional at registration; `publication` with `CandidateSeq` 0 (no parent) or naming an existing candidate; every other field zero). Use origins keep `validArtifactDispositionOrigin`. Restore validates every candidate: state ∈ {preparing, ready, published, cancelled}; `ObligationSeq` names an existing `candidate`-kind obligation; `RegisteredIndex != 0 && ≤ LastAppliedIndex`; `TerminalIndex != 0 && ≤ LastAppliedIndex` iff published/cancelled, and `≥ RegisteredIndex`; `Ready.Signature != ""` iff ready/published.
7. **The admission pipeline is the shipped one, made reachable.** `ApplyArtifactDisposition` runs `prepareArtifactDispositionAdmission`, proposes tag 28, then (after `consensusReadBarrier`) captures the operation result and current record under one read lock and answers with the same body/proof shape as `GetArtifactDisposition` with `body.selector = operation_result.target` (addendum line 462). `fsm.Rejected` keeps `Server.propose`'s existing mapping to `InvalidArgument` carrying the reason (rejection-result slots are 1a-3c's ledger work). Completeness is per action: registry observations are required for `AdmitUse`/`CloseUse`, source observations for `ResolveObligation`, the allocator binding for every action (so production stays refused until 1a-3c); the role map follows the FSM (`coordinator` for `PublishCandidate`, `ResolveObligation` and `CancelCandidate(origin_superseded)`, `policy_admin` for `CancelCandidate(operator_cancelled)`); `strictArtifactDispositionAuthorizer` checks the recovered address against a per-role allowlist (`config artifact_disposition.principals`); the source observer is FSM-backed (`FSM.SettlementObservationsRead`), and `validArtifactDispositionSourceIdentity` becomes settlement-kind-specific so the observer's identities pass; the observer interfaces, observation constructors and `NewArtifactDispositionAdmission` are exported for hosts and tests while `cmd/arbiter` keeps the deps nil.
8. **Every action requires the administrator JWS.** The addendum's table marks most rows "JWS empty" with a configured principal authenticated by credentials that arbiter does not have (no mTLS identity binding; `RegisterResultClaim` accepts a self-declared node). The JWS is the only cryptographic actor binding available, so this wave binds every action to it under a per-role allowlist; the divergence is recorded for the credential-binding slice.
9. **No proto change beyond the one RPC.** The two reads reuse their messages (decision 2); `ActiveQueryPolicy` is imported by `consensus.proto` already, so `ActivateQueryProfileRequest{activation, authority_jws}` mirrors `AbortSnapshotQueryRequest`; `TestConsensusAdminRPCSignatures` moves from 5 to 6 methods with an exact row (the 1a-2b precedent).

Execution order: Task 1 (arbiter-core) → Task 2 (arbiter-proto) → Task 3 (arbiter: pins, activation, v14) → Task 4 (reads) → Task 5 (guards, origins, candidate rules) → Task 6 (admission pipeline) → Task 7 (docs). One PR per task.

---

### Task 1: Read-proof records, reply domains and the activation purpose (arbiter-core)

**Files:**
- Create: `wire/safe_state_reads.go`, `wire/safe_state_reads_test.go`, `authority/query_profile_activation.go`, `authority/query_profile_activation_test.go`
- Modify: `wire/BUILD.bazel`, `authority/BUILD.bazel` (gazelle), `docs/compatibility/snapshot-query-raft-allocation.md` (one line per new domain/purpose)

**Interfaces:**
- Produces (wire): `SafeStateReadProofVersion uint32 = 1`; `PublishedSnapshotReplyDomain = "published-snapshot-reply-v1"`; `QueryPolicyReplyDomain = "query-policy-reply-v1"`; `PublishedSnapshotReplyBodyV1`, `QueryPolicyReplyBodyV1`, `PublishedSnapshotReadProofV1`, `QueryPolicyReadProofV1`; `PublishedSnapshotReplyRoot(body) (string, error)`, `QueryPolicyReplyRoot(body) (string, error)`; `EncodePublishedSnapshotReadProof(body) ([]byte, error)`, `DecodePublishedSnapshotReadProof([]byte) (PublishedSnapshotReadProofV1, error)`, `EncodeQueryPolicyReadProof`, `DecodeQueryPolicyReadProof`.
- Produces (authority): `QueryProfileActivationPurpose = "housegate-query-profile-activation-v1"`, `QueryProfileActivationVersion uint32 = 1`, `QueryProfileActivationPayloadV1{Purpose, Version, Iat, Activation replay.ActiveQueryPolicy}`, `ValidateQueryProfileActivation(p replay.ActiveQueryPolicy) error`, `QueryProfileActivationHash(p) (string, error)` (domain `arbiter-query-profile-activation-command-v1`), `(*Signer).SignQueryProfileActivation(p)`, `(*Signer).SignQueryProfileActivationAt(p, iat)`, `(*Validator).VerifyQueryProfileActivation(p, token) (string, error)` (timeless), `(*Validator).AuthorizeQueryProfileActivation(p, token) (string, error)` (MaxTokenAge).

- [ ] **Step 1: Write the failing tests**

`wire/safe_state_reads_test.go`:

```go
package wire

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func testPublishedSnapshotBody() PublishedSnapshotReplyBodyV1 {
	manifest := replay.SafeSnapshotManifest{SnapshotID: "snap-2", ParentSnapshotID: "snap-1", SafeBlockSeq: 7, ManifestRoot: "0xmanifest", ExecutorProfileID: "prof"}
	ready := replay.SnapshotArtifactReadySubmission{Record: replay.SnapshotArtifactReady{SnapshotID: "snap-2", ManifestRoot: "0xmanifest", SchemaRoot: "0xschema", ArtifactSetRoot: "0xset", PublisherID: "pub-1", RetentionPolicyID: "p1"}, Signature: "0xsig"}
	policy := replay.ActiveQueryPolicy{ActivationID: "act-1", NetworkID: "net", ActivationBlockSeq: 7, ExecutorProfileID: "prof", QueryProfileID: "q1", Enabled: true}
	return PublishedSnapshotReplyBodyV1{Version: 1, NetworkID: "net", SnapshotID: "snap-2", Found: true, ReadIndex: 91, CandidateSeq: 1, PublishedIndex: 91,
		Manifest: &manifest, ArtifactReady: &ready, Activation: &policy, ActivationCommitIndex: 91}
}

func TestPublishedSnapshotReadProofRoundTripsAndBindsEveryField(t *testing.T) {
	body := testPublishedSnapshotBody()
	proof, err := EncodePublishedSnapshotReadProof(body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePublishedSnapshotReadProof(proof)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := PublishedSnapshotReplyRoot(body)
	if decoded.Version != SafeStateReadProofVersion || decoded.ReplyRoot != want || decoded.Body.SnapshotID != "snap-2" || decoded.Body.Manifest == nil || decoded.Body.Manifest.SnapshotID != "snap-2" {
		t.Fatalf("decoded = %+v", decoded)
	}
	absent := PublishedSnapshotReplyBodyV1{Version: 1, NetworkID: "net", SnapshotID: "snap-2", ReadIndex: 91}
	if got, _ := PublishedSnapshotReplyRoot(absent); got == want {
		t.Fatal("absence and presence must not share a root")
	}
	for name, mutate := range map[string]func(*PublishedSnapshotReplyBodyV1){
		"network":   func(b *PublishedSnapshotReplyBodyV1) { b.NetworkID = "other" },
		"snapshot":  func(b *PublishedSnapshotReplyBodyV1) { b.SnapshotID = "snap-3" },
		"found":     func(b *PublishedSnapshotReplyBodyV1) { b.Found = false },
		"index":     func(b *PublishedSnapshotReplyBodyV1) { b.ReadIndex++ },
		"candidate": func(b *PublishedSnapshotReplyBodyV1) { b.CandidateSeq++ },
		"manifest":  func(b *PublishedSnapshotReplyBodyV1) { m := *b.Manifest; m.ManifestRoot = "0xother"; b.Manifest = &m },
		"ready":     func(b *PublishedSnapshotReplyBodyV1) { r := *b.ArtifactReady; r.Signature = "0xother"; b.ArtifactReady = &r },
		"policy":    func(b *PublishedSnapshotReplyBodyV1) { p := *b.Activation; p.QueryProfileID = "q2"; b.Activation = &p },
	} {
		mutated := testPublishedSnapshotBody()
		mutate(&mutated)
		if got, _ := PublishedSnapshotReplyRoot(mutated); got == want {
			t.Fatalf("%s did not change the reply root", name)
		}
	}
}

func TestPublishedSnapshotReadProofRefusesTampering(t *testing.T) {
	proof, _ := EncodePublishedSnapshotReadProof(testPublishedSnapshotBody())
	for name, tamper := range map[string]func(string) string{
		"root":     func(s string) string { return strings.Replace(s, `"reply_root":"0x`, `"reply_root":"0x0`, 1)[:len(s)] },
		"body":     func(s string) string { return strings.Replace(s, `"snapshot_id":"snap-2"`, `"snapshot_id":"snap-3"`, 1) },
		"version":  func(s string) string { return strings.Replace(s, `"version":1,"reply_root"`, `"version":2,"reply_root"`, 1) },
		"unknown":  func(s string) string { return strings.Replace(s, `"version":1,"reply_root"`, `"version":1,"extra":true,"reply_root"`, 1) },
		"trailing": func(s string) string { return s + "{}" },
	} {
		if _, err := DecodePublishedSnapshotReadProof([]byte(tamper(string(proof)))); err == nil {
			t.Fatalf("%s tamper accepted", name)
		}
	}
}

func TestQueryPolicyReadProofRoundTripsAndBindsKind(t *testing.T) {
	policy := replay.ActiveQueryPolicy{ActivationID: "act-1", NetworkID: "net", ActivationBlockSeq: 7, ExecutorProfileID: "prof", QueryProfileID: "q1", Enabled: true}
	body := QueryPolicyReplyBodyV1{Version: 1, NetworkID: "net", ActivationID: "act-1", Found: true, ReadIndex: 40, ActivationKind: "authority", Activation: &policy}
	proof, err := EncodeQueryPolicyReadProof(body)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeQueryPolicyReadProof(proof)
	if err != nil || decoded.Body.ActivationKind != "authority" || decoded.Body.Activation == nil || decoded.Body.Activation.ActivationID != "act-1" {
		t.Fatalf("decoded = %+v (%v)", decoded, err)
	}
	want, _ := QueryPolicyReplyRoot(body)
	publication := body
	publication.ActivationKind, publication.CandidateSeq, publication.CommitIndex, publication.TransitionRoot = "publication", 3, 91, "0xtransition"
	if got, _ := QueryPolicyReplyRoot(publication); got == want {
		t.Fatal("activation kind and candidate binding must change the root")
	}
	absent := QueryPolicyReplyBodyV1{Version: 1, NetworkID: "net", ActivationID: "act-9", ReadIndex: 40}
	if got, _ := QueryPolicyReplyRoot(absent); got == want {
		t.Fatal("absence must not share the root")
	}
}

func TestSafeStateReadDomainsAreDistinct(t *testing.T) {
	if PublishedSnapshotReplyDomain == QueryPolicyReplyDomain || PublishedSnapshotReplyDomain == artifactDispositionCommandDomain {
		t.Fatal("read domains must be distinct from each other and from the command domain")
	}
}
```

`authority/query_profile_activation_test.go` mirrors `authority/snapshot_query_abort_test.go` case for case (valid sign/verify with the recovered address, allowlist refusal, tampered signature by rewriting the first signature character, wrong purpose — an abort token over the same bytes must not verify as an activation and vice versa, `Authorize…` refusing an `iat` older than `MaxTokenAge`, `ValidateQueryProfileActivation` refusing an empty `ActivationID`, `NetworkID`, `ExecutorProfileID`, `QueryProfileID`, a non-zero `KeeperShardID`, `Enabled == false` and a NUL byte in any string; `ActivationBlockSeq` 0 is allowed). Add one golden: `QueryProfileActivationHash` of `replay.ActiveQueryPolicy{ActivationID: "act-1", NetworkID: "net", ActivationBlockSeq: 7, ExecutorProfileID: "prof", QueryProfileID: "q1", Enabled: true}` frozen as a literal (compute once, record the literal, and leave a comment that it is a frozen vector).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //wire:wire_test //authority:authority_test --test_output=errors`
Expected: build FAILS on the undefined types and functions.

- [ ] **Step 3: Implement**

`wire/safe_state_reads.go`:

```go
package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
)

const (
	// SafeStateReadProofVersion is the version of both SafeState read proofs.
	SafeStateReadProofVersion uint32 = 1
	// PublishedSnapshotReplyDomain and QueryPolicyReplyDomain are the canonical
	// digest domains of the two authenticated SafeState reads. Like
	// artifact-disposition-reply-v1 the root is CanonicalDigest over the reply
	// body and excludes the proof; the proof is integrity and correlation
	// evidence for a leader-barrier read, not a signature.
	PublishedSnapshotReplyDomain = "published-snapshot-reply-v1"
	QueryPolicyReplyDomain       = "query-policy-reply-v1"
)

// PublishedSnapshotReplyBodyV1 is the committed state a leader captured under
// one FSM read lock for GetPublishedSnapshot. The records are present only when
// snapshot_id names a published candidate; activation is present only when
// that candidate's publication activated the policy. read_index is the
// artifact-disposition lane's last applied index at capture.
type PublishedSnapshotReplyBodyV1 struct {
	Version               uint32                                  `json:"version"`
	NetworkID             string                                  `json:"network_id"`
	KeeperShardID         uint32                                  `json:"keeper_shard_id"`
	SnapshotID            string                                  `json:"snapshot_id"`
	Found                 bool                                    `json:"found"`
	ReadIndex             uint64                                  `json:"read_index"`
	CandidateSeq          uint64                                  `json:"candidate_seq"`
	PublishedIndex        uint64                                  `json:"published_index"`
	Manifest              *replay.SafeSnapshotManifest            `json:"manifest,omitempty"`
	ArtifactReady         *replay.SnapshotArtifactReadySubmission `json:"artifact_ready,omitempty"`
	Activation            *replay.ActiveQueryPolicy               `json:"activation,omitempty"`
	ActivationCommitIndex uint64                                  `json:"activation_commit_index"`
}

// QueryPolicyReplyBodyV1 is the committed activation record for GetQueryPolicy.
// activation_kind is "publication" (installed by a candidate publication and
// bound to candidate_seq / commit_index / transition_root) or "authority"
// (installed by an authority-signed ActivateQueryProfile command).
type QueryPolicyReplyBodyV1 struct {
	Version        uint32                    `json:"version"`
	NetworkID      string                    `json:"network_id"`
	KeeperShardID  uint32                    `json:"keeper_shard_id"`
	ActivationID   string                    `json:"activation_id"`
	BlockSeq       uint64                    `json:"block_seq"`
	Found          bool                      `json:"found"`
	ReadIndex      uint64                    `json:"read_index"`
	ActivationKind string                    `json:"activation_kind"`
	CandidateSeq   uint64                    `json:"candidate_seq"`
	CommitIndex    uint64                    `json:"commit_index"`
	TransitionRoot string                    `json:"transition_root"`
	Activation     *replay.ActiveQueryPolicy `json:"activation,omitempty"`
}

type PublishedSnapshotReadProofV1 struct {
	Version   uint32                       `json:"version"`
	ReplyRoot string                       `json:"reply_root"`
	Body      PublishedSnapshotReplyBodyV1 `json:"body"`
}

type QueryPolicyReadProofV1 struct {
	Version   uint32                 `json:"version"`
	ReplyRoot string                 `json:"reply_root"`
	Body      QueryPolicyReplyBodyV1 `json:"body"`
}

func PublishedSnapshotReplyRoot(body PublishedSnapshotReplyBodyV1) (string, error) {
	return replay.CanonicalDigest(PublishedSnapshotReplyDomain, body)
}

func QueryPolicyReplyRoot(body QueryPolicyReplyBodyV1) (string, error) {
	return replay.CanonicalDigest(QueryPolicyReplyDomain, body)
}

func EncodePublishedSnapshotReadProof(body PublishedSnapshotReplyBodyV1) ([]byte, error) {
	root, err := PublishedSnapshotReplyRoot(body)
	if err != nil {
		return nil, err
	}
	return json.Marshal(PublishedSnapshotReadProofV1{Version: SafeStateReadProofVersion, ReplyRoot: root, Body: body})
}

// DecodePublishedSnapshotReadProof parses a proof strictly (no unknown fields,
// no trailing data) and re-derives the reply root before returning it.
func DecodePublishedSnapshotReadProof(proof []byte) (PublishedSnapshotReadProofV1, error) {
	var p PublishedSnapshotReadProofV1
	if err := decodeStrictJSON(proof, &p); err != nil {
		return PublishedSnapshotReadProofV1{}, fmt.Errorf("published snapshot read proof: %w", err)
	}
	if p.Version != SafeStateReadProofVersion {
		return PublishedSnapshotReadProofV1{}, fmt.Errorf("published snapshot read proof: version %d is unsupported", p.Version)
	}
	root, err := PublishedSnapshotReplyRoot(p.Body)
	if err != nil || root != p.ReplyRoot {
		return PublishedSnapshotReadProofV1{}, errors.New("published snapshot read proof: reply root mismatch")
	}
	return p, nil
}

func EncodeQueryPolicyReadProof(body QueryPolicyReplyBodyV1) ([]byte, error) {
	root, err := QueryPolicyReplyRoot(body)
	if err != nil {
		return nil, err
	}
	return json.Marshal(QueryPolicyReadProofV1{Version: SafeStateReadProofVersion, ReplyRoot: root, Body: body})
}

func DecodeQueryPolicyReadProof(proof []byte) (QueryPolicyReadProofV1, error) {
	var p QueryPolicyReadProofV1
	if err := decodeStrictJSON(proof, &p); err != nil {
		return QueryPolicyReadProofV1{}, fmt.Errorf("query policy read proof: %w", err)
	}
	if p.Version != SafeStateReadProofVersion {
		return QueryPolicyReadProofV1{}, fmt.Errorf("query policy read proof: version %d is unsupported", p.Version)
	}
	root, err := QueryPolicyReplyRoot(p.Body)
	if err != nil || root != p.ReplyRoot {
		return QueryPolicyReadProofV1{}, errors.New("query policy read proof: reply root mismatch")
	}
	return p, nil
}

func decodeStrictJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data")
	}
	return nil
}
```

If the package already has a strict JSON decoder helper (grep `DisallowUnknownFields` in `wire/`), reuse it instead of adding `decodeStrictJSON`.

`authority/query_profile_activation.go`: copy `authority/snapshot_query_abort.go` structurally — constants `QueryProfileActivationPurpose = "housegate-query-profile-activation-v1"`, `QueryProfileActivationVersion uint32 = 1`, the domain literal `arbiter-query-profile-activation-command-v1` inside `QueryProfileActivationHash`, `QueryProfileActivationPayloadV1{Purpose string json:"purpose"; Version uint32 json:"version"; Iat int64 json:"iat"; Activation replay.ActiveQueryPolicy json:"activation"}`, `ValidateQueryProfileActivation` (fields listed in the Interfaces block, using the same NUL/empty helpers the abort validator uses), `SignQueryProfileActivation`/`…At`, `VerifyQueryProfileActivation` (timeless: purpose, version, strict header/payload re-encoding, recovered address in the allowlist), `AuthorizeQueryProfileActivation` (adds the `MaxTokenAge` check the way the abort authorizer does). Keep the abort file's comment discipline: the verifier is deterministic for replicated apply; freshness belongs to the admission boundary. Add the two doc lines to `docs/compatibility/snapshot-query-raft-allocation.md` (the read domains; the activation purpose as tag 23's proposer purpose).

- [ ] **Step 4: Run the tests, commit**

Run: `bazel run //:gazelle && bazel test //... --test_output=errors && bash scripts/check-public-boundary.sh`
Expected: PASS.

```bash
git add wire/safe_state_reads.go wire/safe_state_reads_test.go wire/BUILD.bazel authority/query_profile_activation.go authority/query_profile_activation_test.go authority/BUILD.bazel docs/compatibility/snapshot-query-raft-allocation.md
git commit -m "feat(wire): add the SafeState read proofs and the query profile activation purpose"
```

---

### Task 2: `ConsensusAdmin.ActivateQueryProfile` (arbiter-proto)

**Files:**
- Modify: `proto/consensus.proto` (`ActivateQueryProfileRequest`, the RPC), `gen/pb/*` (regenerated), `conformance/consensus_test.go` (`TestConsensusAdminRPCSignatures` 5 → 6), `docs/compatibility/snapshot-query-raft-allocation.md` (tag 23 gains its proposer; no new tag)

**Interfaces:**
- Produces: `message ActivateQueryProfileRequest { ActiveQueryPolicy activation = 1; string authority_jws = 2; }` and `rpc ActivateQueryProfile (ActivateQueryProfileRequest) returns (Ack) {}` on `ConsensusAdmin`, after `AbortSnapshotQuery`.

- [ ] **Step 1: Proto change and the conformance row**

In `proto/consensus.proto`, next to `AbortSnapshotQueryRequest` (`:61-64`):

```protobuf
// Authority-signed activation of a query profile for the current executor
// profile (Raft tag 23). Leader-only, explicitly enabled; refused by the FSM
// while the artifact disposition capability is enabled, where activation
// belongs to the candidate-bound publication.
message ActivateQueryProfileRequest {
  ActiveQueryPolicy activation = 1;
  string authority_jws = 2;
}
```

and in `service ConsensusAdmin` after the abort RPC:

```protobuf
  // Leader-only, explicitly enabled mutation: proposes Raft tag 23.
  rpc ActivateQueryProfile (ActivateQueryProfileRequest) returns (Ack) {}
```

`conformance/consensus_test.go::TestConsensusAdminRPCSignatures`: the count becomes 6 and a row `{"ActivateQueryProfile", "arbiter.ActivateQueryProfileRequest", "arbiter.Ack"}` is appended in the existing style. Never re-pin a descriptor baseline (services may gain methods under `assertSnapshotBaselineDescriptors`). Add one sentence to the compatibility doc.

- [ ] **Step 2: Regenerate, test, commit**

Run: `make proto && make lint && make test && go test ./...`
Expected: PASS.

```bash
git add proto/consensus.proto gen/pb conformance/consensus_test.go docs/compatibility/snapshot-query-raft-allocation.md
git commit -m "feat(consensus): add the ConsensusAdmin ActivateQueryProfile RPC"
```

Open the PR, merge on green, record the SHA (Task 3 pins it).

---

### Task 3: Authority activation (Raft tag 23), activation record kinds, snapshot v14 (arbiter)

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (pins), `fsm/state.go` (`ArtifactDispositionQueryPolicyReadiness` kind fields, constants), `fsm/fsm.go` (Apply arm), `fsm/artifact_query_policy_readiness.go` (validator branches, v13 migration), `fsm/snapshot.go` (v14), `fsm/apply_snapshot_query_reservation.go` (delete `TestingActivateQueryPolicy`), `server/consensus_admin.go` (the RPC), `server/consensus_admin_test.go`, `server/server_test.go` (`newServerTestFSM` authority address), `server/snapshot_query_reservations_test.go` (`activateServerTestQueryPolicy` through the real command; the eight call sites), `cmd/arbiter-admin/main.go` (`consensus activate-query-profile`), `cmd/arbiter/storage_protocol_test.go` (current-version literal 13 → 14)
- Create: `fsm/apply_activate_query_profile.go`, `fsm/apply_activate_query_profile_test.go`

**Interfaces:**
- Consumes: Task 1's `authority.ValidateQueryProfileActivation` / `VerifyQueryProfileActivation` / `AuthorizeQueryProfileActivation`, Task 2's `pb.ActivateQueryProfileRequest`, `wire.ActivateQueryProfile{Activation, AuthorityJWS}` (arbiter-core `wire/snapshot_query.go:1215-1218`), `authorityAddressSet` (`fsm/consensus_updates.go:105`), `replay.DigestString`.
- Produces: `const QueryPolicyActivationPublication = "publication"`, `QueryPolicyActivationAuthority = "authority"`; `ArtifactDispositionQueryPolicyReadiness.Kind string json:"kind,omitempty"`, `.ActivationIndex uint64 json:"activation_index,omitempty"`, `.AuthorityJWSHash string json:"authority_jws_hash,omitempty"`; `func (f *FSM) applyActivateQueryProfile(c *wire.ActivateQueryProfile, index uint64) any`; `func (f *FSM) currentExecutorProfileIDLocked() string`; `snapshotVersion = 14`, `snapshotVersionV14`; `ConsensusAdmin.ActivateQueryProfile`; test helper `activateServerTestQueryPolicy(t, node *fakeNode)` and the package-level `serverTestAuthority *authority.Signer`.

- [ ] **Step 1: Bump the pins**

```bash
bash scripts/update-arbiter-core.sh <Task 1 arbiter-core merge SHA>
go get github.com/sentioxyz/arbiter-proto@<Task 2 arbiter-proto merge SHA> && go mod tidy && bazel mod tidy
bazel test //scripts/... //fsm:fsm_test //server:server_test --jobs=4 --test_output=errors
```

Commit `chore(deps): consume the SafeState read proofs, the activation purpose and the ActivateQueryProfile RPC`.

- [ ] **Step 2: Write the failing tests**

`fsm/apply_activate_query_profile_test.go`:

```go
package fsm

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

func activationCommand(t *testing.T, signer *authority.Signer, p replay.ActiveQueryPolicy) wire.Command {
	t.Helper()
	jws, err := signer.SignQueryProfileActivationAt(p, 1)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{ActivateQueryProfile: &wire.ActivateQueryProfile{Activation: p, AuthorityJWS: jws}}
}

func testActivation(f *FSM, id string, block uint64) replay.ActiveQueryPolicy {
	return replay.ActiveQueryPolicy{ActivationID: id, NetworkID: f.st.Params.NetworkID, ActivationBlockSeq: block, ExecutorProfileID: f.st.Params.ExecutorProfileID, QueryProfileID: "q1", Enabled: true}
}

func TestActivateQueryProfileInstallsAnAuthorityActivationAndRoundTrips(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params) // seeded genesis: the safe watermark is block 0
	got := applyCmd(t, f, 41, activationCommand(t, a, testActivation(f, "act-1", 0)))
	if !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("activate = %#v", got)
	}
	r := f.st.ArtifactDisposition.QueryPolicyReadiness
	if r == nil || r.Kind != QueryPolicyActivationAuthority || r.ActivationIndex != 41 || r.AuthorityJWSHash == "" || r.CandidateSeq != 0 || r.Policy.ActivationID != "act-1" {
		t.Fatalf("readiness = %+v", r)
	}
	// The same policy re-proposed (fresh signature) is idempotent.
	before := snapshotBytes(t, f)
	if got := applyCmd(t, f, 42, activationCommand(t, a, testActivation(f, "act-1", 0))); !reflect.DeepEqual(got, Applied{}) || !bytes.Equal(before, snapshotBytes(t, f)) {
		t.Fatalf("replay = %#v", got)
	}
	data := snapshotBytes(t, f)
	if data[4] != snapshotVersionV14 {
		t.Fatalf("version byte = %d, want 14", data[4])
	}
	g := restoreInto(t, data)
	if !reflect.DeepEqual(g.st.ArtifactDisposition.QueryPolicyReadiness, r) {
		t.Fatal("authority activation did not survive restore")
	}
	view, found, err := g.QueryPolicyReadiness()
	if err != nil || !found || view.Policy.ActivationID != "act-1" {
		t.Fatalf("readiness view = %+v %v %v", view, found, err)
	}
	if err := restoreErr(t, rewriteSnapshotDocument(t, data, snapshotVersionV13, func(map[string]any) {})); err == nil || !strings.Contains(err.Error(), "authority activation") {
		t.Fatalf("pre-v14 container with an authority activation restored: %v", err)
	}
	tampered := rewriteSnapshotDocument(t, data, snapshotVersionV14, func(doc map[string]any) {
		doc["artifact_disposition"].(map[string]any)["query_policy_readiness"].(map[string]any)["kind"] = "publication"
	})
	if err := restoreErr(t, tampered); err == nil || !strings.Contains(err.Error(), "activation identity is incomplete") {
		t.Fatalf("publication kind without candidate binding restored: %v", err)
	}
}

func TestActivateQueryProfileRefusalsLeaveStateUntouched(t *testing.T) {
	a, params := authorityFixture(t)
	b, _ := authorityFixture(t)
	f := mustNewFSM(t, params)
	mustApply(t, f, activationCommand(t, a, testActivation(f, "act-1", 0)))
	before := snapshotBytes(t, f)
	for name, cmd := range map[string]wire.Command{
		"unknown authority":      activationCommand(t, b, testActivation(f, "act-2", 0)),
		"wrong network":          activationCommand(t, a, func() replay.ActiveQueryPolicy { p := testActivation(f, "act-2", 0); p.NetworkID = "other"; return p }()),
		"other executor profile": activationCommand(t, a, func() replay.ActiveQueryPolicy { p := testActivation(f, "act-2", 0); p.ExecutorProfileID = "prof-2"; return p }()),
		"beyond the watermark":   activationCommand(t, a, testActivation(f, "act-2", 5)),
		"disabled policy":        activationCommand(t, a, func() replay.ActiveQueryPolicy { p := testActivation(f, "act-2", 0); p.Enabled = false; return p }()),
		"reused activation id":   activationCommand(t, a, func() replay.ActiveQueryPolicy { p := testActivation(f, "act-1", 0); p.QueryProfileID = "q2"; return p }()),
		"tampered signature": func() wire.Command {
			c := activationCommand(t, a, testActivation(f, "act-2", 0))
			jws := c.ActivateQueryProfile.AuthorityJWS
			i := strings.LastIndex(jws, ".") + 1
			first := "A"
			if jws[i] == 'A' {
				first = "B"
			}
			c.ActivateQueryProfile.AuthorityJWS = jws[:i] + first + jws[i+1:]
			return c
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			rejected := applyExpectReject(t, f, cmd)
			if !strings.Contains(rejected.Reason, "query profile activation") {
				t.Fatalf("reason = %q", rejected.Reason)
			}
			if !bytes.Equal(before, snapshotBytes(t, f)) {
				t.Fatal("refusal changed replicated state")
			}
		})
	}
}

func TestActivateQueryProfileRefusesWhileTheCapabilityIsEnabled(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	enableArtifactDispositionForTest(f)
	before := snapshotBytes(t, f)
	rejected := applyExpectReject(t, f, activationCommand(t, a, testActivation(f, "act-1", 0)))
	if !strings.Contains(rejected.Reason, "candidate-bound publication") || !bytes.Equal(before, snapshotBytes(t, f)) {
		t.Fatalf("enabled-capability activation = %q", rejected.Reason)
	}
}

func TestPublicationActivationRestoresWithAKindFromAV13Container(t *testing.T) {
	f, _ := publishedTransitionCandidate(t)
	data := snapshotBytes(t, f)
	r := f.st.ArtifactDisposition.QueryPolicyReadiness
	if r == nil || r.Kind != QueryPolicyActivationPublication {
		t.Fatalf("publication readiness = %+v", r)
	}
	// A v13 document never carried a kind: rewriting the current document
	// without one under the v13 byte must restore as a publication activation.
	legacy := rewriteSnapshotDocument(t, data, snapshotVersionV13, func(doc map[string]any) {
		delete(doc["artifact_disposition"].(map[string]any)["query_policy_readiness"].(map[string]any), "kind")
	})
	if g := restoreInto(t, legacy); g.st.ArtifactDisposition.QueryPolicyReadiness.Kind != QueryPolicyActivationPublication {
		t.Fatal("v13 readiness did not migrate to the publication kind")
	}
	missing := rewriteSnapshotDocument(t, data, snapshotVersionV14, func(doc map[string]any) {
		delete(doc["artifact_disposition"].(map[string]any)["query_policy_readiness"].(map[string]any), "kind")
	})
	if err := restoreErr(t, missing); err == nil || !strings.Contains(err.Error(), "kind") {
		t.Fatalf("v14 readiness without a kind restored: %v", err)
	}
}
```

`mustNewFSM` seeds a genesis manifest (`seedTestGenesis`); if its safe watermark is not block 0, read the seeded `SafeWatermark.SafeBlockSeq` for the activation block and the "beyond the watermark" case. If `publishedTransitionCandidate`'s fixture has `Capability` enabled and the test needs a v13 readiness record with a kind absent, keep the deletion as written (the capability field is independent).

`server/consensus_admin_test.go`: an end-to-end case through `pb.NewConsensusAdminClient`: `updatesEnabled: false` → `FailedPrecondition`; `updatesEnabled: true` on a follower → `NotLeader` details; leader + a token by an unknown key → `PermissionDenied`; leader + a valid token → `Ack` and `FSM.QueryPolicyReadiness()` reports the policy; the same request again → `Ack` (idempotent); a token older than `maxTokenAge` → `PermissionDenied`. Build the FSM with `AuthorityAddresses: []string{signer.Address()}` the way `server/consensus_admin_test.go:32-38` does.

`server/snapshot_query_reservations_test.go`: `activateServerTestQueryPolicy(t, node *fakeNode)` now signs `replay.ActiveQueryPolicy{ActivationID: "activation-1", NetworkID: testServerNetworkID, ExecutorProfileID: "executor-1", QueryProfileID: "query-1", Enabled: true}` with `serverTestAuthority` and applies it through `mustDirectApply(t, node, wire.Command{ActivateQueryProfile: …})`; because the FSM requires the current executor profile, the policy's `ExecutorProfileID` must equal the genesis profile `newServerTestFSM` configures — change the fixture policy to `"prof"` (the value `newServerTestFSM` uses) and audit the eight call sites for assertions that pin `"executor-1"`; `newServerTestFSM` adds `AuthorityAddresses: []string{serverTestAuthority.Address()}` where `serverTestAuthority` is built once from a fixed test key (reuse the hex the consensus-admin tests use). Delete `fsm.TestingActivateQueryPolicy`.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestActivateQueryProfile|TestPublicationActivationRestores' --jobs=4 --test_output=errors`
Expected: FAIL — `Rejected{"empty command"}` from the `default:` arm / undefined constants.

- [ ] **Step 4: Implement**

`fsm/state.go`, in `ArtifactDispositionQueryPolicyReadiness`:

```go
	// Kind is QueryPolicyActivationPublication for a record written by a
	// candidate publication (bound to CandidateSeq/CommitIndex/TransitionRoot)
	// or QueryPolicyActivationAuthority for an authority-signed activation
	// (bound to ActivationIndex/AuthorityJWSHash). A v13 container carried only
	// publication records; restore fills the kind in.
	Kind             string `json:"kind,omitempty"`
	ActivationIndex  uint64 `json:"activation_index,omitempty"`
	AuthorityJWSHash string `json:"authority_jws_hash,omitempty"`
```

with `const ( QueryPolicyActivationPublication = "publication"; QueryPolicyActivationAuthority = "authority" )` next to the lifecycle constants; `newQueryPolicyReadiness` sets `Kind: QueryPolicyActivationPublication`.

`fsm/apply_activate_query_profile.go`:

```go
package fsm

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// applyActivateQueryProfile installs an authority-signed query policy for the
// current executor profile while the artifact disposition capability is off.
// Once the capability is enabled, activation is a consequence of the
// candidate-bound publication (newQueryPolicyReadiness) and this path refuses.
func (f *FSM) applyActivateQueryProfile(c *wire.ActivateQueryProfile, index uint64) any {
	if c == nil {
		return Rejected{Reason: "query profile activation is required"}
	}
	d := &f.st.ArtifactDisposition
	if d.Capability != 0 {
		return Rejected{Reason: "query profile activation must enter through the candidate-bound publication while the artifact disposition capability is enabled"}
	}
	p := c.Activation
	if err := authority.ValidateQueryProfileActivation(p); err != nil {
		return Rejected{Reason: "query profile activation: " + err.Error()}
	}
	validator := authority.Validator{AllowedAddresses: authorityAddressSet(f.st.Params.AuthorityAddresses)}
	if _, err := validator.VerifyQueryProfileActivation(p, c.AuthorityJWS); err != nil {
		return Rejected{Reason: fmt.Sprintf("query profile activation authority: %v", err)}
	}
	if p.NetworkID != f.st.Params.NetworkID || p.KeeperShardID != 0 {
		return Rejected{Reason: "query profile activation scope mismatch"}
	}
	if p.ExecutorProfileID != f.currentExecutorProfileIDLocked() {
		return Rejected{Reason: "query profile activation names an executor profile other than the current safe tip's; a profile change is a transition publication"}
	}
	if p.ActivationBlockSeq > f.st.SafeWatermark.SafeBlockSeq {
		return Rejected{Reason: "query profile activation block is beyond the safe watermark"}
	}
	if r := d.QueryPolicyReadiness; r != nil {
		if r.Policy == p {
			return Applied{}
		}
		if r.Policy.ActivationID == p.ActivationID {
			return Rejected{Reason: "query profile activation id is already active with a different policy"}
		}
		if p.ActivationBlockSeq < r.Policy.ActivationBlockSeq {
			return Rejected{Reason: "query profile activation block precedes the active policy"}
		}
	}
	d.QueryPolicyReadiness = &ArtifactDispositionQueryPolicyReadiness{Kind: QueryPolicyActivationAuthority, ActivationIndex: index, AuthorityJWSHash: replay.DigestString(c.AuthorityJWS), Policy: p}
	return Applied{}
}

// currentExecutorProfileIDLocked is the executor profile of the current safe
// tip, or the genesis profile before any publication.
func (f *FSM) currentExecutorProfileIDLocked() string {
	if m := f.st.Manifests[f.st.SafeWatermark.SnapshotID]; m != nil {
		return m.ExecutorProfileID
	}
	return f.st.Params.ExecutorProfileID
}
```

`fsm/fsm.go`: add `case cmd.ActivateQueryProfile != nil: return f.applyActivateQueryProfile(cmd.ActivateQueryProfile, l.Index)` after the abort arm. `fsm/artifact_query_policy_readiness.go`: branch on `r.Kind` — `QueryPolicyActivationAuthority` requires `ActivationIndex != 0`, `AuthorityJWSHash != ""`, `CandidateSeq == 0`, `CommitIndex == 0`, `TransitionRoot == ""`, a zero `Transition`, and the policy scope (`NetworkID == Params.NetworkID`, shard 0, `Enabled`, non-empty `ActivationID`/`QueryProfileID`/`ExecutorProfileID`); `QueryPolicyActivationPublication` runs the existing checks and additionally requires `ActivationIndex == 0 && AuthorityJWSHash == ""`; any other kind is refused ("query policy readiness kind %q is unknown"). `fsm/snapshot.go`: `snapshotVersion = 14`, `snapshotVersionV14`, accept list + error text, the three predicates; in the restore path before the validators: under `ver[0] < snapshotVersionV14`, a readiness record with `Kind == QueryPolicyActivationAuthority` (or any non-empty kind other than publication) is refused ("query policy readiness carries an authority activation in a pre-v14 container"), and an empty kind is set to `QueryPolicyActivationPublication`; under v14 an empty kind is refused ("query policy readiness kind is required in a v14 container"). Move the existing version-byte assertions to v14 (fixtures deliberately built at older versions stay); `cmd/arbiter/storage_protocol_test.go`'s current-version literal moves to 14.

`server/consensus_admin.go`:

```go
func (svc *consensusAdminService) ActivateQueryProfile(ctx context.Context, req *pb.ActivateQueryProfileRequest) (*pb.Ack, error) {
	if !svc.s.d.Cfg.ConsensusUpdatesEnabled {
		return nil, status.Error(codes.FailedPrecondition, "consensus parameter updates are disabled; verify every voter's protocol capability before setting consensus_updates_enabled: true")
	}
	if req.GetActivation() == nil {
		return nil, status.Error(codes.InvalidArgument, "query profile activation is required")
	}
	p := wire.ActiveQueryPolicyFromPB(req.GetActivation())
	if err := authority.ValidateQueryProfileActivation(p); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := svc.s.consensusLeaderCheck(ctx); err != nil {
		return nil, err
	}
	view, err := svc.s.d.FSM.ConsensusParamsView()
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "consensus parameters unavailable: %v", err)
	}
	validator := authority.Validator{AllowedAddresses: authorityAddressSet(view.Current.AuthorityAddresses), MaxTokenAge: svc.s.d.Cfg.ConsensusAdminMaxTokenAge}
	if _, err := validator.AuthorizeQueryProfileActivation(p, req.GetAuthorityJws()); err != nil {
		return nil, status.Errorf(codes.PermissionDenied, "query profile activation authority: %v", err)
	}
	if _, err := svc.s.propose(ctx, wire.Command{ActivateQueryProfile: &wire.ActivateQueryProfile{Activation: p, AuthorityJWS: req.GetAuthorityJws()}}); err != nil {
		return nil, err // propose maps fsm.Rejected to InvalidArgument with the FSM reason
	}
	return &pb.Ack{}, nil
}
```

(`wire.ActiveQueryPolicyFromPB` lives in arbiter-core `wire/snapshot_query.go:483`.) `cmd/arbiter-admin/main.go`: `consensus activate-query-profile --address --activation-id --query-profile-id --executor-profile-id --activation-block-seq [--timeout]` signs with the authority key the `update` operation uses and calls the RPC; document it in the usage text.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test //server:server_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS; no test references `TestingActivateQueryPolicy`.

- [ ] **Step 6: Commit**

```bash
git add fsm/state.go fsm/fsm.go fsm/apply_activate_query_profile.go fsm/apply_activate_query_profile_test.go fsm/artifact_query_policy_readiness.go fsm/snapshot.go fsm/apply_snapshot_query_reservation.go fsm/*_test.go server/consensus_admin.go server/consensus_admin_test.go server/server_test.go server/*_test.go cmd/arbiter-admin/main.go cmd/arbiter/storage_protocol_test.go
git commit -m "feat(fsm): apply authority-signed query profile activation (tag 23); snapshot format v14"
```

---

### Task 4: Barriers on `SafeState` and the two authenticated reads (arbiter)

**Files:**
- Modify: `server/safestate.go` (barriers; `GetPublishedSnapshot`, `GetQueryPolicy`), `server/safestate_test.go` (create if absent)
- Create: `fsm/safe_state_reads.go`, `fsm/safe_state_reads_test.go`

**Interfaces:**
- Consumes: Task 1's `wire.PublishedSnapshotReplyBodyV1` / `QueryPolicyReplyBodyV1` / `Encode…ReadProof` / `Decode…ReadProof`; Task 3's `Kind` on the readiness record; `consensusReadBarrier`; `cloneManifest` (`fsm/apply.go`).
- Produces: `type PublishedSnapshotView struct{NetworkID string; ReadIndex uint64; Found, Ambiguous bool; CandidateSeq, PublishedIndex uint64; Manifest *replay.SafeSnapshotManifest; ArtifactReady *replay.SnapshotArtifactReadySubmission; Activation *replay.ActiveQueryPolicy; ActivationCommitIndex uint64}`; `func (f *FSM) PublishedSnapshotRead(snapshotID string) PublishedSnapshotView`; `type QueryPolicyView struct{NetworkID string; ReadIndex uint64; Found bool; Kind string; CandidateSeq, CommitIndex uint64; TransitionRoot string; Policy replay.ActiveQueryPolicy}`; `func (f *FSM) QueryPolicyRead(activationID string, blockSeq uint64) QueryPolicyView`.

- [ ] **Step 1: Write the failing tests**

`fsm/safe_state_reads_test.go`: (a) from `publishedTransitionCandidate` (a published candidate whose publication activated a policy): `PublishedSnapshotRead(candidate.Manifest.SnapshotID)` → `Found`, `CandidateSeq`, `PublishedIndex == candidate.TerminalIndex`, `Manifest` deep-equal to the candidate manifest, `ArtifactReady` equal to `candidate.Ready`, `Activation` equal to the readiness policy with `ActivationCommitIndex == readiness.CommitIndex`, `ReadIndex == LastAppliedIndex`; mutating the returned manifest/ready/activation leaves replicated state unchanged (detached); an unknown id → `Found == false`, no records; a candidate that is `ready` but not published → `Found == false`; (b) from `openPublishedCandidate` (a same-profile publication) → `Found` with `Activation == nil`; (c) `QueryPolicyRead("act", 0)` / `(“”, block)` / both agreeing → `Found` with `Kind == publication`, `CandidateSeq`, `CommitIndex`, `TransitionRoot`, `Policy`; disagreeing pair → not found; no readiness → not found; after a Task 3 authority activation → `Kind == authority`, zero candidate fields.

`server/safestate_test.go`: (a) `TestSafeStateReadsCrossExactlyOneBarrierEach`: with `configureNode` counting `barrier` calls, each of the six reads on a leader crosses exactly one barrier; on a follower (`leader: false`) each returns `NotLeader` details before any FSM read; (b) `GetPublishedSnapshot`: `InvalidArgument` for an empty `network_id`/`snapshot_id` or a non-zero `keeper_shard_id`; `FailedPrecondition` for another network; unknown snapshot → `found == false` in the decoded proof, nil records, `proof` decodes with `wire.DecodePublishedSnapshotReadProof` and its body echoes the selector, network and `read_index`; one found path (see below) → the outer `manifest`/`artifact_ready`/`activation` equal the decoded body's records and `reply_root` re-derives; (c) `GetQueryPolicy`: `InvalidArgument` when both selectors are empty; after `activateServerTestQueryPolicy(t, node)` → `found`, `activation` equal to the body's, `activation_kind == "authority"`; a wrong `activation_id` → `found == false` with a proof; the barrier count is one.

For the server found path of `GetPublishedSnapshot`, drive a published candidate through wire commands on the server FSM: enable the capability with a signed consensus update (`serverTestAuthority`), then `BindPolicy` → `RegisterCandidate` → `RecordReady` → `PublishCandidate`, copying the command shapes from `fsm/apply_artifact_disposition_test.go`'s `bindPolicyCommand`/`readyDispositionCandidate`/`publishCandidateCommand` into a server-test helper `publishServerCandidate(t, node) (replay.SnapshotPin, uint64)`. The candidate manifest must be a sealed child of the server genesis: build it the way `fsm`'s `publicationFixture` does (promotion pipeline) — copy those sealing steps into the helper. If that helper exceeds roughly eighty lines, keep it anyway and say so in the report; do not weaken the assertion to the not-found path only.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestPublishedSnapshotRead|TestQueryPolicyRead' --jobs=4 --test_output=errors`
Expected: build FAILS on the undefined reads.

- [ ] **Step 3: Implement**

`fsm/safe_state_reads.go`:

```go
package fsm

import "github.com/housegate/housegate/pkg/replay"

// PublishedSnapshotView is the detached capture GetPublishedSnapshot serves
// after a leader barrier: the published candidate that carries snapshot_id,
// its readiness, and the activation its publication installed, if any.
type PublishedSnapshotView struct {
	NetworkID             string
	ReadIndex             uint64
	Found                 bool
	Ambiguous             bool
	CandidateSeq          uint64
	PublishedIndex        uint64
	Manifest              *replay.SafeSnapshotManifest
	ArtifactReady         *replay.SnapshotArtifactReadySubmission
	Activation            *replay.ActiveQueryPolicy
	ActivationCommitIndex uint64
}

func (f *FSM) PublishedSnapshotRead(snapshotID string) PublishedSnapshotView {
	f.mu.RLock()
	defer f.mu.RUnlock()
	d := &f.st.ArtifactDisposition
	view := PublishedSnapshotView{NetworkID: f.st.Params.NetworkID, ReadIndex: d.LastAppliedIndex}
	var found *ArtifactDispositionCandidateState
	for _, c := range d.Candidates {
		if c == nil || c.State != ArtifactCandidatePublished || c.Manifest.SnapshotID != snapshotID {
			continue
		}
		if found != nil {
			view.Ambiguous = true
			return view
		}
		found = c
	}
	if found == nil {
		return view
	}
	ready := found.Ready
	view.Found, view.CandidateSeq, view.PublishedIndex = true, found.CandidateSeq, found.TerminalIndex
	view.Manifest, view.ArtifactReady = cloneManifest(&found.Manifest), &ready
	if r := d.QueryPolicyReadiness; r != nil && r.Kind == QueryPolicyActivationPublication && r.CandidateSeq == found.CandidateSeq {
		policy := r.Policy
		view.Activation, view.ActivationCommitIndex = &policy, r.CommitIndex
	}
	return view
}

// QueryPolicyView is the detached capture GetQueryPolicy serves. Only the
// current activation is retained; an activation id or block that is not the
// current one is authenticated absence, never a fallback to the latest policy.
type QueryPolicyView struct {
	NetworkID      string
	ReadIndex      uint64
	Found          bool
	Kind           string
	CandidateSeq   uint64
	CommitIndex    uint64
	TransitionRoot string
	Policy         replay.ActiveQueryPolicy
}

func (f *FSM) QueryPolicyRead(activationID string, blockSeq uint64) QueryPolicyView {
	f.mu.RLock()
	defer f.mu.RUnlock()
	d := &f.st.ArtifactDisposition
	view := QueryPolicyView{NetworkID: f.st.Params.NetworkID, ReadIndex: d.LastAppliedIndex}
	r := d.QueryPolicyReadiness
	if r == nil || (activationID == "" && blockSeq == 0) {
		return view
	}
	if activationID != "" && r.Policy.ActivationID != activationID {
		return view
	}
	if blockSeq != 0 && r.Policy.ActivationBlockSeq != blockSeq {
		return view
	}
	view.Found, view.Kind, view.CandidateSeq, view.CommitIndex, view.TransitionRoot, view.Policy = true, r.Kind, r.CandidateSeq, r.CommitIndex, r.TransitionRoot, r.Policy
	return view
}
```

`server/safestate.go`: each of the four existing reads gains `if err := svc.s.consensusReadBarrier(ctx); err != nil { return nil, err }` as its first statement (the `_ context.Context` parameters become `ctx`). Then:

```go
func (svc *safeStateService) GetPublishedSnapshot(ctx context.Context, req *pb.GetPublishedSnapshotRequest) (*pb.PublishedSnapshot, error) {
	if req.GetNetworkId() == "" || req.GetSnapshotId() == "" {
		return nil, status.Error(codes.InvalidArgument, "published snapshot network_id and snapshot_id are required")
	}
	if req.GetKeeperShardId() != 0 {
		return nil, status.Error(codes.InvalidArgument, "published snapshot keeper_shard_id must be 0 in v1")
	}
	if err := svc.s.consensusReadBarrier(ctx); err != nil {
		return nil, err
	}
	view := svc.s.d.FSM.PublishedSnapshotRead(req.GetSnapshotId())
	if req.GetNetworkId() != view.NetworkID {
		return nil, status.Errorf(codes.FailedPrecondition, "published snapshot network_id %q does not match the current network %q", req.GetNetworkId(), view.NetworkID)
	}
	if view.Ambiguous {
		return nil, status.Errorf(codes.FailedPrecondition, "published snapshot %q has ambiguous published candidates", req.GetSnapshotId())
	}
	body := wire.PublishedSnapshotReplyBodyV1{Version: wire.SafeStateReadProofVersion, NetworkID: view.NetworkID, SnapshotID: req.GetSnapshotId(), Found: view.Found, ReadIndex: view.ReadIndex,
		CandidateSeq: view.CandidateSeq, PublishedIndex: view.PublishedIndex, Manifest: view.Manifest, ArtifactReady: view.ArtifactReady, Activation: view.Activation, ActivationCommitIndex: view.ActivationCommitIndex}
	proof, err := wire.EncodePublishedSnapshotReadProof(body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "published snapshot proof: %v", err)
	}
	reply := &pb.PublishedSnapshot{TerminalProof: proof}
	if view.Found {
		reply.Manifest = wire.ManifestToPB(*view.Manifest)
		reply.ArtifactReady = wire.SnapshotArtifactReadySubmissionToPB(*view.ArtifactReady)
	}
	if view.Activation != nil {
		reply.Activation = wire.ActiveQueryPolicyToPB(*view.Activation)
	}
	return reply, nil
}

func (svc *safeStateService) GetQueryPolicy(ctx context.Context, req *pb.GetQueryPolicyRequest) (*pb.QueryPolicyStatus, error) {
	if req.GetNetworkId() == "" {
		return nil, status.Error(codes.InvalidArgument, "query policy network_id is required")
	}
	if req.GetKeeperShardId() != 0 {
		return nil, status.Error(codes.InvalidArgument, "query policy keeper_shard_id must be 0 in v1")
	}
	if req.GetActivationId() == "" && req.GetBlockSeq() == 0 {
		return nil, status.Error(codes.InvalidArgument, "query policy activation_id or block_seq is required; there is no latest-policy fallback")
	}
	if err := svc.s.consensusReadBarrier(ctx); err != nil {
		return nil, err
	}
	view := svc.s.d.FSM.QueryPolicyRead(req.GetActivationId(), req.GetBlockSeq())
	if req.GetNetworkId() != view.NetworkID {
		return nil, status.Errorf(codes.FailedPrecondition, "query policy network_id %q does not match the current network %q", req.GetNetworkId(), view.NetworkID)
	}
	body := wire.QueryPolicyReplyBodyV1{Version: wire.SafeStateReadProofVersion, NetworkID: view.NetworkID, ActivationID: req.GetActivationId(), BlockSeq: req.GetBlockSeq(), Found: view.Found, ReadIndex: view.ReadIndex,
		ActivationKind: view.Kind, CandidateSeq: view.CandidateSeq, CommitIndex: view.CommitIndex, TransitionRoot: view.TransitionRoot}
	if view.Found {
		policy := view.Policy
		body.Activation = &policy
	}
	proof, err := wire.EncodeQueryPolicyReadProof(body)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "query policy proof: %v", err)
	}
	reply := &pb.QueryPolicyStatus{Found: view.Found, TerminalProof: proof}
	if view.Found {
		reply.Activation = wire.ActiveQueryPolicyToPB(view.Policy)
	}
	return reply, nil
}
```

`wire.ManifestToPB` (arbiter-core `wire/convert.go:298`) is what `GetManifest` already uses; `wire.ActiveQueryPolicyToPB` is at `wire/snapshot_query.go:470`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test //server:server_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS, including the existing `cmd/arbiter` listener-separation tests that call `SafeState.GetSafeWatermark` (they run against a leader; if one runs against a follower, its expectation becomes `NotLeader` and the report says so).

- [ ] **Step 5: Commit**

```bash
git add fsm/safe_state_reads.go fsm/safe_state_reads_test.go server/safestate.go server/safestate_test.go server/server_test.go
git commit -m "feat(server): serve GetPublishedSnapshot and GetQueryPolicy after a leader barrier with read proofs; barrier every SafeState read"
```

---

### Task 5: Capability-scoped refusals, explicit arms for tags 24–27, candidate origins and candidate restore rules (arbiter fsm)

**Files:**
- Modify: `fsm/apply.go` (`applyPublishSafeSnapshot` guard), `fsm/fsm.go` (arms for tags 24–27), `fsm/apply_artifact_disposition.go` (`validCandidateOrigin` at `applyRegisterCandidate`), `fsm/genesis_validation.go` (`validateArtifactDispositionCandidateRecords`), `fsm/apply_artifact_disposition_test.go` (`pendingTransitionPublish`'s bare transition origin becomes a full one; `readyDispositionCandidate`'s `{Kind: "publication"}` stays valid as a first publication)
- Create: `fsm/apply_unscoped_paths.go`, `fsm/apply_unscoped_paths_test.go`, `fsm/artifact_candidate_origin_test.go`

**Interfaces:**
- Produces: origin-kind constants `ArtifactOriginKindGenesis/V2/Transition/Reservation/Query/Publication`; `func validCandidateOrigin(d *ArtifactDispositionState, origin wire.ArtifactDispositionOriginV1) bool`; `func validateArtifactDispositionCandidateRecords(d *ArtifactDispositionState) error` (called from `validateArtifactDispositionTerminalRecords`); refusal reasons listed below.

- [ ] **Step 1: Write the failing tests**

`fsm/apply_unscoped_paths_test.go`: (a) `PublishSafeSnapshot` on an FSM with the capability enabled (`enableArtifactDispositionForTest`) → `Rejected` containing "candidate-bound PublishCandidate", byte-identical snapshot; the same command with the capability off → `Applied` (the existing behavior — reuse the manifest a shipped publish test uses); (b) tags 24/25 (`wire.Command{RecordSnapshotQueryClaim: &wire.RecordSnapshotQueryClaim{}}`, `{RecordSnapshotQueryAttestation: …}`) → `Rejected` containing "server-owned seam" in both modes; (c) tags 26/27 → with the capability enabled `Rejected` containing "candidate-bound" (26: "PublishCandidate", 27: "RecordReady"); with it disabled `Rejected` containing "not implemented"; every case byte-identical. Encode the commands through `mkLog(t, cmd)`; `wire.Encode` requires exactly one set field, so a zero-valued inner struct is enough.

`fsm/artifact_candidate_origin_test.go`: `RegisterCandidate` with each accepted origin (`genesis`; `v2` with parent + block; `transition` with parent + block only; `transition` with activation id + root as well; `publication` with `CandidateSeq: 0`; `publication` naming an existing candidate) → `Applied`; refused, byte-identical: `publication` naming a missing candidate, `transition` without a parent, `v2` with an activation id, `reservation` / `query` kinds (use origins are not candidate origins), an unknown kind, an origin carrying `ExecutionOutcome`; reason contains "candidate origin". Restore: from `openPublishedCandidate`, tamper a v14 document's candidate — state `"weird"`; `obligation_seq` naming a missing obligation; `obligation_seq` naming a `query`-kind obligation (author one); `registered_index` 0 or above `last_applied_index`; `terminal_index` set on a `preparing` candidate; `terminal_index` 0 on a `published` one; `terminal_index` below `registered_index`; `ready.signature` empty on a `ready` candidate — each refused with a reason starting "artifact disposition candidate"; the untampered document restores.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestUnscopedPaths|TestCandidateOrigin|TestCandidateRestore' --jobs=4 --test_output=errors`
Expected: FAIL (tag 10 applies under the enabled capability; tags 24–27 report "empty command"; invalid origins register; tampered candidates restore).

- [ ] **Step 3: Implement**

`fsm/apply.go`, first statement of `applyPublishSafeSnapshot`:

```go
	if f.st.ArtifactDisposition.Capability != 0 {
		return Rejected{Reason: "safe snapshot publication must enter through the candidate-bound PublishCandidate action while the artifact disposition capability is enabled"}
	}
```

`fsm/apply_unscoped_paths.go`:

```go
package fsm

// The public Raft tags 24–27 decode but must never mutate replicated state on
// their own. Tags 24 and 25 are the server-owned attestation/claim seams
// (ApplyServerOwned*); tags 26 and 27 are the unscoped transition-publication
// and readiness paths the artifact-lifecycle addendum refuses once the
// capability is enabled, and that have no capability-off implementation in
// this wave. Explicit arms replace the misleading "empty command" default.
func (f *FSM) applyRecordSnapshotQueryClaim() any {
	return Rejected{Reason: "record snapshot query claim is a server-owned seam; no public Raft entry may apply it"}
}

func (f *FSM) applyRecordSnapshotQueryAttestation() any {
	return Rejected{Reason: "record snapshot query attestation is a server-owned seam; no public Raft entry may apply it"}
}

func (f *FSM) applyPublishExecutorProfileTransition() any {
	if f.st.ArtifactDisposition.Capability != 0 {
		return Rejected{Reason: "executor profile transition publication must enter through the candidate-bound PublishCandidate action while the artifact disposition capability is enabled"}
	}
	return Rejected{Reason: "authority-published executor profile transition is not implemented in this wave"}
}

func (f *FSM) applyRecordSnapshotArtifactReady() any {
	if f.st.ArtifactDisposition.Capability != 0 {
		return Rejected{Reason: "snapshot artifact readiness must enter through the candidate-bound RecordReady action while the artifact disposition capability is enabled"}
	}
	return Rejected{Reason: "unscoped snapshot artifact readiness is not implemented in this wave"}
}
```

with four arms in `FSM.Apply` before `default:`: `case cmd.RecordSnapshotQueryClaim != nil: return f.applyRecordSnapshotQueryClaim()`, `case cmd.RecordSnapshotQueryAttestation != nil: return f.applyRecordSnapshotQueryAttestation()`, `case cmd.PublishExecutorProfileTransition != nil: return f.applyPublishExecutorProfileTransition()`, `case cmd.RecordSnapshotArtifactReady != nil: return f.applyRecordSnapshotArtifactReady()`.

`fsm/apply_artifact_disposition.go`: in `applyRegisterCandidate`, replace the `a.Origin.Kind == ""` clause with `!validCandidateOrigin(d, a.Origin)` and the reason "candidate origin is invalid" (keep "candidate identity or manifest mismatch" for the other two clauses), and add:

```go
// validCandidateOrigin admits the origins a candidate registration may carry.
// Unlike validArtifactDispositionOrigin (use identities), a transition origin
// may omit the activation id and transition root at registration — they are
// bound at publication — and a publication origin may carry candidate 0 (no
// parent) or name an existing candidate.
func validCandidateOrigin(d *ArtifactDispositionState, origin wire.ArtifactDispositionOriginV1) bool {
	switch origin.Kind {
	case "genesis":
		return reflect.DeepEqual(origin, wire.ArtifactDispositionOriginV1{Kind: "genesis"})
	case "v2":
		return origin.ParentSnapshotID != "" && origin.SafeBlockSeq != 0 && reflect.DeepEqual(origin, wire.ArtifactDispositionOriginV1{Kind: "v2", ParentSnapshotID: origin.ParentSnapshotID, SafeBlockSeq: origin.SafeBlockSeq})
	case "transition":
		return origin.ParentSnapshotID != "" && origin.SafeBlockSeq != 0 && reflect.DeepEqual(origin, wire.ArtifactDispositionOriginV1{Kind: "transition", ParentSnapshotID: origin.ParentSnapshotID, SafeBlockSeq: origin.SafeBlockSeq, ActivationID: origin.ActivationID, TransitionRoot: origin.TransitionRoot})
	case "publication":
		if origin.CandidateSeq != 0 && d.Candidates[origin.CandidateSeq] == nil {
			return false
		}
		return reflect.DeepEqual(origin, wire.ArtifactDispositionOriginV1{Kind: "publication", CandidateSeq: origin.CandidateSeq})
	default:
		return false
	}
}
```

Introduce origin-kind constants in `fsm/state.go` — `ArtifactOriginKindGenesis = "genesis"`, `ArtifactOriginKindV2 = "v2"`, `ArtifactOriginKindTransition = "transition"`, `ArtifactOriginKindReservation = "reservation"`, `ArtifactOriginKindQuery = "query"`, `ArtifactOriginKindPublication = "publication"` — and use them in `validCandidateOrigin`, `validArtifactDispositionOrigin`, the query-transfer origin literals, `applyOpenDispositionChallenge`'s origin-kind check (which today reuses `ArtifactObligationKindQuery`) and the settlement arms; the string values are unchanged (the Wave 1a-3 carried item). `fsm/genesis_validation.go`: `validateArtifactDispositionCandidateRecords(d)` called first inside `validateArtifactDispositionTerminalRecords`: for every candidate — state ∈ {`preparing`, `ready`, `published`, `cancelled`} else "artifact disposition candidate %d has unknown state %q"; `ObligationSeq == 0` or no obligation or `Kind != candidate` → "… names obligation %d, which is not its candidate obligation"; `RegisteredIndex == 0 || > LastAppliedIndex` → "… is registered at index %d, outside the applied history"; published/cancelled ⇒ `TerminalIndex != 0 && ≤ LastAppliedIndex && ≥ RegisteredIndex` else "… is %s at index %d, outside the applied history" / "… without a registration"; preparing/ready ⇒ `TerminalIndex == 0` else "… carries a terminal index"; ready/published ⇒ `Ready.Signature != ""` else "… is %s without readiness". Update `pendingTransitionPublish`'s origin to `{Kind: "transition", ParentSnapshotID: <parent id>, SafeBlockSeq: <parent block>}` (values the fixture already has), and any other bare-transition fixture.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS; the lifecycle test (`fsm/artifact_disposition_lifecycle_test.go`) still passes with `{Kind: "publication"}`.

- [ ] **Step 5: Commit**

```bash
git add fsm/apply.go fsm/fsm.go fsm/apply_unscoped_paths.go fsm/apply_unscoped_paths_test.go fsm/apply_artifact_disposition.go fsm/artifact_candidate_origin_test.go fsm/genesis_validation.go fsm/apply_artifact_disposition_test.go
git commit -m "feat(fsm): refuse the unscoped publication paths under the capability, validate candidate origins, validate candidates at restore"
```

---

### Task 6: The reachable admission pipeline — per-role principals, per-action completeness, the FSM-backed settlement observer, the exported constructor (arbiter server + fsm + config)

**Files:**
- Modify: `server/artifact_disposition.go` (`ApplyArtifactDisposition`, `prepareArtifactDispositionAdmission` completeness, `artifactDispositionRole`, `strictArtifactDispositionAuthorizer` per-role allowlist, `validArtifactDispositionSourceIdentity` per settlement kind, exported interfaces and constructors), `server/server.go` (doc on `Deps.ArtifactDisposition`), `server/server_test.go` (the deps tests follow the renames; a new end-to-end Apply test), `config/config.go` (`ArtifactDispositionConfig`), `config/config_test.go`, `cmd/arbiter/services.go` (refuse `artifact_disposition.enabled: true`), `fsm/artifact_disposition_reads.go` (`SettlementObservationsRead`), `fsm/artifact_obligation_resolve_test.go` (the read's tests)
- Create: `server/artifact_disposition_settlement_observer.go`, `server/artifact_disposition_apply_test.go`

**Interfaces:**
- Produces (server, exported): `type ArtifactDispositionPrincipal struct{ id, role string }` with `ID()`/`Role()`; `type ArtifactDispositionRegistryObserver interface{ Observe(context.Context, wire.ArtifactDispositionCommandV1, ArtifactDispositionPrincipal) (ArtifactDispositionRegistryObservation, error) }`, `ArtifactDispositionSourceObserver`, `ArtifactDispositionCapacityAllocator` (the three existing interfaces, exported, with the exported principal); `NewArtifactDispositionRegistryObservation([]wire.ArtifactDispositionRegistryObservationV1) ArtifactDispositionRegistryObservation`, `NewArtifactDispositionSourceObservation([]wire.ArtifactDispositionSourceObservationV1) ArtifactDispositionSourceObservation`, `NewArtifactDispositionCapacityBinding(ordinal uint64, reservation []byte) ArtifactDispositionCapacityBinding`; `type ArtifactDispositionAdmissionConfig struct{ Principals map[string][]string }`; `func NewArtifactDispositionAdmission(cfg ArtifactDispositionAdmissionConfig, authorityAddresses []string, registry ArtifactDispositionRegistryObserver, source ArtifactDispositionSourceObserver, allocator ArtifactDispositionCapacityAllocator, control snapshotQueryControlAuthorizer) (*artifactDispositionAdmissionDeps, error)` (the deps type stays unexported; the constructor sets `enabled` and `bootstrapped` when every dependency is non-nil and refuses otherwise); `func NewFSMSettlementObserver(f *fsm.FSM) ArtifactDispositionSourceObserver`.
- Produces (fsm): `func (f *FSM) SettlementObservationsRead(obligationSeq uint64) ([]wire.ArtifactDispositionSourceObservationV1, bool)`.
- Produces (config): `Config.ArtifactDisposition ArtifactDispositionConfig{Enabled bool yaml:"enabled"; Principals map[string][]string yaml:"principals"}`.

- [ ] **Step 1: Write the failing tests**

`fsm/artifact_obligation_resolve_test.go` (append): `SettlementObservationsRead` on the lifecycle fixture after `PublishCandidate` returns one `candidate_terminal` observation with `SourceIndex == candidate.TerminalIndex` and identity `{Kind: "publication", CandidateSeq}`, and `applyResolveObligation` with exactly that observation is `Applied`; on `abortedQueryObligations` it returns a `query_terminal` observation for the query obligation and a `reservation_released` observation for the reservation obligation, each accepted by the handler; an open, unsettled obligation returns `(nil, false)`; an unknown seq returns `(nil, false)`; a resolved obligation returns `(nil, false)`.

`server/artifact_disposition_apply_test.go`: with `startServerWithConfig{artifactDisposition: deps}` built through `NewArtifactDispositionAdmission` from a `Principals` map that puts the test signer under `governance_admin` and `coordinator` only, the recording fakes for registry/allocator and `NewFSMSettlementObserver(f)` as the source: (a) `BindPolicy` signed by the signer → `Ack`-equivalent reply: `body.operation_result.code == "applied"`, `body.selector` deep-equals `operation_result.target`, `body.found`, `body.record` is the policy record, the proof decodes and its root re-derives, `body.reply_nonce` echoes the request and `body.reader_id` is the authenticated actor; (b) the same request again → the same `operation_result` (idempotent through the operation map) and byte-identical FSM state; (c) `RegisterCandidate` signed by the same signer → `PermissionDenied` "not a configured publisher principal"; (d) `PublishCandidate` role derives `coordinator` (assert through the recorded principal the fakes captured); (e) a follower → `NotLeader`; (f) `ResolveObligation` on a settled candidate obligation (drive `RegisterCandidate` … `PublishCandidate` with a signer configured as `publisher` and `coordinator`) → `Applied` with the observation the FSM-backed observer produced, and `ResolveObligation` on an unsettled obligation → `FailedPrecondition` "source observation is incomplete"; (g) `AdmitUse` with the registry fake returning nothing → `FailedPrecondition` "registry observation is incomplete", and `BindPolicy` with the registry fake returning nothing → accepted (per-action completeness); (h) an allocator fake returning ordinal 0 → `FailedPrecondition` "capacity binding is incomplete"; (i) an FSM rejection (e.g. `BeginRetirement` with a wrong revision) → `InvalidArgument` carrying the FSM reason and no operation record.

`config/config_test.go`: `artifact_disposition.principals` with an unknown role, a malformed address, a duplicate address → validation errors; `artifact_disposition.enabled: true` → `cmd/arbiter`'s `newFSM`/services builder refuses with "artifact disposition admission dependencies are not available in this build" (test in `cmd/arbiter` if a services test exists, else in `config`).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestSettlementObservationsRead' //server:server_test --test_filter='TestApplyArtifactDisposition' --jobs=4 --test_output=errors`
Expected: build FAILS on the undefined read/constructors; the RPC returns the unconditional refusal.

- [ ] **Step 3: Implement**

`fsm/artifact_disposition_reads.go`:

```go
// SettlementObservationsRead derives the settlement observations committed
// state can prove for one open obligation: the observation shape
// applyResolveObligation accepts, built from the same records
// settlementObservationMatchesLocked matches. It never guesses: an obligation
// whose settling record has not been committed yields nothing.
func (f *FSM) SettlementObservationsRead(obligationSeq uint64) ([]wire.ArtifactDispositionSourceObservationV1, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	d := &f.st.ArtifactDisposition
	obligation := d.Obligations[obligationSeq]
	if obligation == nil || obligation.State != ArtifactObligationOpen {
		return nil, false
	}
	switch obligation.Kind {
	case ArtifactObligationKindCandidate:
		for _, c := range d.Candidates {
			if c != nil && c.ObligationSeq == obligationSeq && (c.State == ArtifactCandidateCancelled || c.State == ArtifactCandidatePublished) && c.TerminalIndex != 0 {
				return []wire.ArtifactDispositionSourceObservationV1{{ObligationSeq: obligationSeq, SourceKind: "candidate_terminal", SourceIndex: c.TerminalIndex,
					SourceIdentity: wire.ArtifactDispositionOriginV1{Kind: "publication", CandidateSeq: c.CandidateSeq}}}, true
			}
		}
	case ArtifactObligationKindQuery:
		for _, a := range d.QueryAdmissions {
			if a != nil && a.Lifecycle == SnapshotQueryLifecycleTerminal && a.Abort != nil && a.ClientAccount == obligation.Origin.ClientAccount && a.StatementID == obligation.Origin.StatementID && a.InputRoot == obligation.Origin.InputRoot {
				return []wire.ArtifactDispositionSourceObservationV1{{ObligationSeq: obligationSeq, SourceKind: "query_terminal", SourceIndex: a.Abort.CommitIndex,
					SourceIdentity: wire.ArtifactDispositionOriginV1{Kind: "query", ClientAccount: a.ClientAccount, StatementID: a.StatementID, InputRoot: a.InputRoot, ExecutionOutcome: a.ExecutionOutcome}}}, true
			}
		}
	case ArtifactObligationKindReservation:
		for _, t := range d.ReservationTombstones {
			if t != nil && t.Reservation != nil && t.Reservation.ReservationID == obligation.Origin.ReservationID && t.TerminalIndex != 0 {
				return []wire.ArtifactDispositionSourceObservationV1{{ObligationSeq: obligationSeq, SourceKind: "reservation_released", SourceIndex: t.TerminalIndex,
					SourceIdentity: wire.ArtifactDispositionOriginV1{Kind: "reservation", ClientAccount: t.ClientAccount, StatementID: t.StatementID, ReservationID: t.Reservation.ReservationID}}}, true
			}
		}
	}
	return nil, false
}
```

Each identity must be exactly what `settlementObservationMatchesLocked`'s arm compares (read the arms; if the `query_terminal` arm compares more origin fields than listed here, carry them). `server/artifact_disposition.go`:

- `validArtifactDispositionSourceIdentity(kind string, identity wire.ArtifactDispositionOriginV1) bool` becomes settlement-kind-specific: `candidate_terminal` ⇒ `{Kind: "publication", CandidateSeq != 0}` exact; `query_terminal` ⇒ `Kind == "query"` with non-empty `ClientAccount`, `StatementID`, `InputRoot`, `ExecutionOutcome` (other fields free); `reservation_released` ⇒ `Kind == "reservation"` with non-empty `ClientAccount`, `StatementID`, `ReservationID`; anything else false. `completeArtifactDispositionSourceObservations` passes the kind.
- `prepareArtifactDispositionAdmission`: registry completeness is required only when `command.Action.AdmitUse != nil || command.Action.CloseUse != nil`; source completeness only when `command.Action.ResolveObligation != nil` (other actions may carry empty observation lists; non-empty lists are still shape-checked); the allocator binding is required for every action.
- `artifactDispositionRole`: `PublishCandidate` → `coordinator`; `CancelCandidate` by `ReasonCode` (`preparation_failed` → `publisher`, `origin_superseded` → `coordinator`, `operator_cancelled` → `policy_admin`, else error); `ResolveObligation` → `coordinator`; `AdmitUse`/`CloseUse` → `source`; the rest unchanged.
- `strictArtifactDispositionAuthorizer{validator authority.Validator; principals map[string]map[string]bool}`: after `VerifyArtifactDispositionCommand` and role derivation, `if !a.principals[role][actor] { return …, fmt.Errorf("actor %s is not a configured %s principal", actor, role) }`; the validator's allowlist is the union of every role's addresses.
- `ApplyArtifactDisposition`: `prepared, err := svc.prepareArtifactDispositionAdmission(ctx, req)`; propose `wire.Command{ArtifactDisposition: &wire.ArtifactDispositionCmd{Command: prepared.command, AdministratorJWS: req.GetAdministratorJws(), Validation: prepared.validation}}`; on a proposal error return it (`Server.propose` maps `fsm.Rejected` to `InvalidArgument` carrying the reason — keep that mapping); then `consensusReadBarrier`; `ArtifactDispositionRead(ArtifactDispositionReadSelector{Kind: ArtifactDispositionReadOperation, ActorID: prepared.principal.id, RequestID: prepared.command.RequestID, CommandRoot: prepared.validation.CommandRoot})`; build the body exactly as `GetArtifactDisposition` does with `Selector = artifactDispositionOperationTarget(view.Operation)`, `ReaderId = prepared.principal.id` (the authenticated actor — `ApplyArtifactDispositionRequest{command, administrator_jws, reply_nonce}` carries no reader id) and `ReplyNonce = req.GetReplyNonce()`; proof; reply.
- Exports: rename the four interfaces and the principal/observation/binding types as listed under Interfaces, add the observation/binding constructors and `NewArtifactDispositionAdmission` (validates `cfg.Principals` keys against the role vocabulary and addresses as lowercase `0x` + 40 hex, builds the authorizer from the union allowlist, refuses a nil observer), `NewFSMSettlementObserver` in the new file (for `ResolveObligation` it returns `SettlementObservationsRead(a.ObligationSeq)`; for every other action an empty observation).
- `config`: `ArtifactDispositionConfig{Enabled bool; Principals map[string][]string}` under `artifact_disposition`, validated (role vocabulary, address shape, no duplicates); `cmd/arbiter`: `Enabled == true` refuses startup with "artifact disposition admission dependencies are not available in this build" (registry observer and allocator have no production implementation); `Principals` is parsed and validated but not consumed by `cmd/arbiter` yet.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test //server:server_test //config:config_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS; the existing disposition-deps tests pass after the renames.

- [ ] **Step 5: Commit**

```bash
git add server/artifact_disposition.go server/artifact_disposition_settlement_observer.go server/artifact_disposition_apply_test.go server/server.go server/server_test.go config/config.go config/config_test.go cmd/arbiter/services.go fsm/artifact_disposition_reads.go fsm/artifact_obligation_resolve_test.go
git commit -m "feat(server): make ApplyArtifactDisposition propose and answer with per-role principals and an FSM-backed settlement observer"
```

---

### Task 7: Documentation (arbiter + housegate amendments)

**Files:**
- Modify: `docs/snapshot-query-reservations.md` (the "Disposition capability and the terminal actions" section gains "Authenticated reads", "Query profile activation", "Unscoped paths under the capability", "Admission pipeline" paragraphs; the deferred list is rewritten for 1a-3b'/1a-3c), `README.md` (SafeState follower behavior; `consensus activate-query-profile`; `artifact_disposition.principals`), `docs/specs/2026-09-17-consensus-parameter-updates.md` (only if it describes `ConsensusAdmin`'s method set)

- [ ] **Step 1: Write**

One paragraph per line, every identifier verified against the code: the two reads (selectors, `found`, the proof shape and domains, `read_index` semantics, `Ambiguous`, follower `NotLeader`), the barrier on the four existing reads, tag 23 (purpose, the RPC, the refusals, idempotency, the record kinds, v14 and the v13 migration), the line-474 refusals and the explicit arms, candidate origins and the candidate restore rules, the admission pipeline (order of checks, per-action completeness, the role map, per-role principals, the FSM-backed settlement observer, why `cmd/arbiter` still refuses `enabled: true`), and the deferred list (tag 26/27 apply and `SourceClaims.RecordSnapshotArtifactReady` with publisher credentials; arbiter-core reply mirror/client/vectors; housegate T2.2 export and the adapter; reader/nonce on the reads; policy history; the registry observer; the allocator — 1a-3c).

- [ ] **Step 2: Commit**

```bash
git add docs/snapshot-query-reservations.md README.md docs/specs
git commit -m "docs: describe the authenticated SafeState reads, query profile activation and the admission pipeline"
```

---

## Deferred (recorded, not placeholders)

- **1a-3b' — client side.** arbiter-core: a Go mirror of `ArtifactDispositionReplyBodyV1`/`ReadProofV1`/`ResultV1`/the five records whose `encoding/json` bytes equal the pb-struct bytes arbiter hashes (a conformance test marshals both over a fixture set), `ArtifactDispositionReplyRoot` with the `artifact-disposition-reply-v1` constant, a client for `ArtifactDispositionControl` and the two reads (proof decode, root re-derivation, outer-vs-body record equality, network/reader/nonce checks, leader redirect through `dataplane.Client`), literal root vectors for all twelve actions plus both reply domains and the administrator JWS (`conformance/testdata/artifact_disposition_v1.json`, cross-repo SHA pin as `SharedStatementVectorsSHA256`); housegate: export `newAuthenticatedHistoricalPolicy`, `historicalDecisionSource`, `historicalDecisionRequest` and `loadAuthenticatedHistoricalDecision` (T2.2) and the adapter that turns `GetPublishedSnapshot`/`GetQueryPolicy` into `HistoricalDecisionRecords` — which still needs `Reservation`, `BlockSeq`, `StatementRoot` and `Artifacts` from a read that does not exist; `reader_id`/`reply_nonce` on the two read messages (proto change with exact-field allowances).
- **Publisher credentials.** Tag 27 apply, `SourceClaims.RecordSnapshotArtifactReady`, readiness-signature verification in `applyRecordReady` and a configured publisher set need a publisher identity arbiter can verify (`arbiter.NodeRole` has no publisher role; B1's publisher owns the `snapshot-query-artifact-ready-v1` signature scheme); tag 26's capability-off transition publication with a non-candidate activation record.
- **Registry and capacity.** A production `ArtifactDispositionRegistryObserver` (the artifact owner registry lives outside arbiter; `RegistryClosedRevision` stays a recorded caller assertion), the prepaid allocator and the shared capacity ledger (1a-3c), rejection-result slots for deterministic FSM rejections, the addendum's "JWS empty" rows once transport credential binding exists.
- **History.** `GetQueryPolicy` over past activations (a policy history keyed by activation id/block); binding `OpenChallenge` to a committed source claim (tag 24); `challenge_resolved`/`cleanup_settled` settlement records.

## Subsequent plans

| Plan | Scope | Written when |
|---|---|---|
| 1a-3b' — client side | arbiter-core reply mirror + client + vectors; housegate T2.2 export + adapter; reader/nonce proto fields | after this plan merges |
| 1a-3c — capacity charging | charge vector, C2 Q binding, submit/abort through the charged path, the allocator | after 1a-3b' |
| Wave 2 / 3 | source execution and the applied terminal (C5/B5); housegate sequencer client (T3.1) | per the TODO plan |
