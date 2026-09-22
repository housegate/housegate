# Signed INSERT ... SELECT — Wave 1a-3: Disposition Capability Governance and the Four Terminal Actions (C1, part 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the C1 artifact-disposition lane its governance switch and its terminal half: an authority-signed, epoch-fenced way to enable `ArtifactDisposition.Capability` (genesis stays off), and the four undispatched public actions — `FinishRetirement`, `CloseUse` (with the conditional absent close), `ResolveObligation` (against committed settlement records only) and `OpenChallenge` (the challenge obligation) — each restorable under snapshot format v13, all still unreachable in production because no proposer exists for tag 28.

**Architecture:** The capability rides the existing `UpdateConsensusParams` command (Raft tag 18, authority-signed, epoch- and digest-fenced) as one new mutable consensus parameter `artifact_disposition_capability` (arbiter-core struct field, arbiter-proto field, `fsm.Params` field, genesis config default 0); Apply mirrors it into the persisted `ArtifactDisposition.Capability` monotonically (0 → 1 only) and restore refuses any divergence. The four actions follow the shipped handler style (reject before the first write, in place), draw every "authenticated evidence" from committed state or the private `Validation.SourceObservations` — never from a caller boolean — and gain their operation targets, read kinds and reply records. Snapshot format moves to v13 for the new terminal fields. Authenticated barrier reads, Raft tags 23/26/27 with publisher authentication, the `ApplyArtifactDisposition` admission dependencies and candidate-bound capacity charging are 1a-3b and 1a-3c.

**Tech Stack:** Go 1.26, Bazel 9.1.0, hashicorp/raft, arbiter-core `authority` + `wire` + root `arbiter` package, arbiter-proto (`buf generate`), housegate `pkg/replay` (`CanonicalDigest`, `SnapshotQueryAttestation`, `SnapshotPin`).

**Spec:** [coordination plan Task C1](2026-09-16-signed-insert-select-coordination.md) (lines 45–89: activation, capability-enabled guards, snapshot migration); [artifact-lifecycle addendum](../specs/2026-09-17-signed-insert-select-artifact-lifecycle-design.md) (lines 299–311 the per-action actor/predicate table, 460 terminal evidence, 474 the capability rule, 476 expected revisions, 478 CloseUse, 480 ResolveObligation, 482 OpenChallenge, 484–486 retirement, 492 snapshot versions); [TODO plan T1.1](2026-09-20-signed-insert-select-todo.md); the Wave 1a-1 / 1a-2 / 1a-2b plans and their execution amendments (the C2/C3 state this builds on).

## Global Constraints

- Tier 3 of [issue #153](https://github.com/housegate/housegate/issues/153): merging enables no runtime capability. Genesis and every restored pre-v13 container have `Capability == 0`; `cmd/arbiter` sets neither `server.Deps.ArtifactDisposition` nor `SnapshotQueryReservations`; `ApplyArtifactDisposition` keeps its hard `FailedPrecondition` refusal, so no tag-28 command can be proposed in production even after an operator enables the capability.
- Capability enablement is an authority-signed consensus transition: `arbiter.ConsensusParamsUpdate.ArtifactDispositionCapability uint32` (`json:"artifact_disposition_capability,omitempty"`, proto field 8), copied by `verifyConsensusTransition` like `MaxWriters`; only the values 0 and 1 are valid; 1 → 0 is refused ("mixed-version enablement refuses", addendum line 492); zero is absent in canonical JSON, so every frozen consensus-update digest and vector stays byte-identical.
- The four actions: roles are validator-selected (`policy_admin` for `FinishRetirement`, `source` for `CloseUse`/`ResolveObligation`, `verifier` for `OpenChallenge`); expected revisions per addendum line 476 (retirement revision for Finish; exact use revision for CloseUse or 0 for the conditional absent close; exact obligation revision for ResolveObligation; 0 for OpenChallenge); `ResolveObligation` accepts no caller outcome — every `Validation.SourceObservations` entry must name a committed record at its `SourceIndex`, and `challenge_resolved` / `cleanup_settled` observations are refused until their records exist; `OpenChallenge` refuses an already closed use and commits the challenge obligation in the same entry; "the current safe tip cannot retire"; a retired read stays `retired`, never `not_found`.
- Frozen bytes: `L3BlockHeader`, `StatementState`, `wire.ArtifactDispositionCommandV1` and its command root are untouched; `Params` gains only an `omitempty` field, so `ConsensusDigest` of an unchanged network is unchanged.
- Snapshot format: current v12; this plan bumps to v13 by the recipe (constants, accept list + error text, predicates, per-field validators); a pre-v13 container carrying a non-zero capability, a closed/cancelled-absent use, a retired retirement, a resolved or challenge obligation is refused (only test-authored snapshots could have had them).
- Cross-repo pins move by commit: arbiter-core via `bash scripts/update-arbiter-core.sh <sha>`, arbiter-proto via `go get github.com/sentioxyz/arbiter-proto@<sha> && go mod tidy && bazel mod tidy`; `scripts/update_dependency_test.go` stays green.
- Tests: arbiter-core `bazel test //...`; arbiter-proto `make proto && make lint && make test && go test ./...`; arbiter `bazel test //fsm:fsm_test //server:server_test --jobs=4` per task and `bazel test //... --jobs=4` (19 targets) before each PR; every refusal asserted to leave byte-identical snapshots; `reflect.DeepEqual` for successes.
- Conventions: English comments; conventional commit subjects; commits end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`; PR bodies end with `Refs https://github.com/housegate/housegate/issues/153`, `Design: https://github.com/housegate/housegate/pull/154` and `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. One isolated worktree per repository (`fix/153-wave1a-3-c1`), never the main checkouts; merge only on green CI, squash with branch deletion; task review precedes every merge; single serial lane.

## Baseline facts (arbiter `origin/main` c2ee3cf after PR #105; arbiter-core d9df8e1a; arbiter-proto 312b8c9b; housegate 48ccc08a)

- **Dispatch.** `applyArtifactDisposition` (`fsm/apply_artifact_disposition.go:16-71`) guards in order: nil command; `Capability == 0` → "artifact disposition is disabled"; `Capacity != nil && Capacity.Enabled` → "…cannot admit work before complete mutation charging is installed"; `validDispositionEnvelope`; `ensureDispositionMaps`; operation idempotency on `ActorID+"\x00"+RequestID` (same root → `Applied{}`, different → identity conflict). The switch (`:39-56`) dispatches `BindPolicy`, `RegisterCandidate`, `RecordReady`, `PublishCandidate`, `CancelCandidate`, `BeginRetirement`, `AdmitUse`; the `default:` arm rejects `FinishRetirement`, `CloseUse`, `OpenChallenge`, `ResolveObligation` and `GrantReservation` with "artifact disposition action requires the enabled capacity and service-validation slice" (`GrantReservation` stays private: C2's `ApplyServerOwnedReservationGrant`). The tail (`:57-70`) bumps `LastAppliedIndex`, derives `dispositionOperationTarget` (`:102-129`, arms for the seven only — a missing arm lands `TargetKind: ""`, which `ArtifactDispositionRead` reports as `found=false`) and writes `d.Operations[key]`. Handlers mutate `f.st.ArtifactDisposition` in place and reject before the first write: `applyBeginRetirement` (`:374-401`: `admin(c, "policy_admin")`, `policyAdministrator`, `publishedRetirementCandidate`, `hasCommittedCurrentSuccessor`, exact `ExpectedRevision`, `increment(&existing.Revision, …)`, `State, CutIndex = ArtifactRetirementClosing, index`), `applyAdmitUse` (`:409-435`: `ExpectedRevision == 0`, `ContinuationObligationSeq == 0`, `ValidArtifactDispositionUseIdentity`, `ActorID == use.PrincipalID`, role `source` or `verifier`, `publishedOpenUseTarget`, writes `Uses[ReferenceID] = {State: admitted, Revision: 1, AdmittedIndex: index}`). Helpers: `admin` (`:200`), `increment` (`:208`), `policyAdministrator` (`:491`), `retirementKey`, `candidateOperationTarget`, `useOperationTarget`, `retirementOperationPin`.
- **State the four must write** (`fsm/state.go`): constants `ArtifactRetirementOpen/Closing/Retired`, `ArtifactUseAdmitted/Closed/CancelledAbsent` (`"cancelled_absent"`), `ArtifactObligationOpen/Resolved` (`:484-500`); `ArtifactDispositionRetirementState{Pin, RetentionPolicyID, State, Revision, PublishedCandidateSeq, CutIndex, RetiredIndex}` (`:631-639`); `ArtifactDispositionUseState{Use, State, Revision, AdmittedIndex, ClosedIndex, RegistryClosedRevision, ClosureCommandRoot}` (`:641-649`); `ArtifactDispositionObligationState{ObligationSeq, Pin, Kind, Origin, ParentObligationSeq, State, Revision, AdmittedIndex, ResolvedIndex, ReplacementObligationSeqs, ResolutionSourceObservations}` (`:651-663`). `RetiredIndex`, `ClosedIndex`, `RegistryClosedRevision`, `ClosureCommandRoot`, `ResolvedIndex`, `ReplacementObligationSeqs`, `ResolutionSourceObservations`, `ParentObligationSeq` and the constants `closed`, `cancelled_absent`, `retired`, `resolved` have no writer. Obligation kinds written today: `"candidate"` (`:256`) and `"query"` (`artifact_reservation_query_transfer.go:116`); no `challenge` kind and no challenge type exist. Wire payloads: `ArtifactDispositionRetirementTargetV1{Pin, RetentionPolicyID}` (shared by Begin/Finish), `ArtifactDispositionCloseUseV1{Use, ExpectedRegistryRevision}`, `ArtifactDispositionOpenChallengeV1{Pin, Origin, Attestation replay.SnapshotQueryAttestation, ReplayUse}`, `ArtifactDispositionResolveObligationV1{ObligationSeq}`, `ArtifactDispositionSourceObservationV1{ObligationSeq, SourceKind, SourceIndex, SourceIdentity ArtifactDispositionOriginV1, ReplacementObligationSeqs}` (arbiter-core `wire/artifact_disposition.go:331-354, 434-440`). `replay.SnapshotQueryAttestation{ReplicaID, Receipt, ReceiptHash, Signature}`; the shipped attestation verifier is `fsm/artifact_snapshot_query_attestation.go` (`validateSnapshotQueryAttestationRequest`, active-verifier signature over the receipt root — the helper to reuse). Reads: `ArtifactDispositionReadKind` has `policy/candidate/retirement/use/operation` (`fsm/artifact_disposition_reads.go:19-25`); `populateRetirementReadView` already projects `ActiveRetirementUseIDs` (admitted uses at the pin) and `UnresolvedRetirementObligationSeqs` (open obligations at the pin), sorted (`:214-232`); the server's `artifactDispositionRecord` (`server/artifact_disposition.go:403-436`) has arms for policy/candidate/retirement/use; arbiter-proto already has `ArtifactDispositionObligationRecordV1` and selector `obligation_seq = 5`; role mapping `artifactDispositionRole` (`server/artifact_disposition.go:165-184`) derives `source` for `AdmitUse`/`CloseUse`/`OpenChallenge`/`ResolveObligation` and `policy_admin` for Begin/Finish. The only coverage of the four is the negative table `TestAdmitUseRefusesUnimplementedActionsWithoutMutation` (`fsm/artifact_use_test.go:141-166`).
- **Capability.** `ArtifactDispositionState.Capability uint32 json:"capability,omitempty"` (`fsm/state.go:313`): one production reader, no writer; 17 test sites set `f.st.ArtifactDisposition.Capability = 1` directly; `TestApplyBindPolicyWhenEnabledRoundTripsSnapshot` pins that 1 survives a round trip; v3/v4 restore as 0. `fsm.Params{SchemaSnapshotID, ExecutorProfileID, AuthorityAddresses, NetworkID, MaxWriters}` (`:87-99`); `normalizeConsensusParams` (`fsm/params_identity.go:40-52`) touches only `MaxWriters`/`AuthorityAddresses`; `ConsensusDigest` = `CanonicalDigest("arbiter/consensus-params/v1", normalized)`; `ImmutableConsensusDigest` = network/schema/executor only; `verifyConsensusTransition` (`fsm/consensus_updates.go:72-98`) copies `AuthorityAddresses` and `MaxWriters` and refuses a no-op; `applyUpdateConsensusParams` (`:19-67`) requires a published genesis, current epoch and promotion seq, drained promotions/cleanups, then sets `f.st.Params = next`, `ConsensusEpoch++`, appends `ConsensusUpdates`; `validateConsensusHistory` re-derives the chain at restore. `config.GenesisConfig{NetworkID, SchemaSnapshotID, ExecutorProfileID, SchemaRoot, ManifestPath, MaxWriters}` → `configuredFSMParams` (`cmd/arbiter/services.go:35-43`). arbiter-core `arbiter.ConsensusParamsUpdate{NetworkID, GenesisSnapshotID, ExpectedEpoch, PreviousParamsDigest, AuthorityAddresses, MaxWriters, ExpectedPromotionSeq}` with snake-case JSON tags (`consensus.go:16-24`); `authority.NormalizeConsensusParamsUpdate` / `ConsensusParamsUpdateHash` (domain `arbiter-consensus-params-update-command-v1`); arbiter-proto `ConsensusParamsUpdate` fields 1–7, `ConsensusMutableParams{authority_addresses = 1, max_writers = 2}`, `ConsensusParamsState{…, bootstrap = 4, current = 5, …}`; server `UpdateConsensusParams` (`server/consensus_admin.go:79-98`) + `authorizeConsensusParamsUpdate` (`:116-145`); `cmd/arbiter-admin` `consensus show/update`.
- **Snapshot.** `snapshotVersion = 12` (`fsm/snapshot.go:20-33`); accept list `:199-200`; predicates `:217`, `:226`, `:333`; scrubs `:338-365`; validators `:369-414` (capacity → `ensureDispositionMaps` → readiness → attestations → frontiers → query statements → the unknown-lifecycle / pre-v12 gate → admissions); version-byte tests listed in the 1a-2b plan plus `fsm/apply_snapshot_query_abort_test.go`.
- **Tests/helpers.** `dispositionCommand(actor, request, root, action)` (role `publisher`), `bindPolicyCommand()`, `publishCandidateCommand`, `readyDispositionCandidate`, `publishedTransitionCandidate`, `signedTransitionReceipts`, `isDispositionRejected`, `rewriteSnapshotDocument` (`fsm/apply_artifact_disposition_test.go`); `openPublishedCandidate`, `initialPublishedUse(pin)`, `admitUseCommand(actor, request, root, use)`, `snapshotPinForUse` (`fsm/artifact_use_test.go`); `beginRetirementCommand`, `publishedHistoricalCandidate` (`fsm/artifact_retirement_test.go`); `mustApply`, `applyExpectReject`, `registerActive`, `ed25519KeyFor`, `snapshotBytes`, `restoreInto`; C2/C3: `grantedReservation`, `sequencedQuery`, `signedAbort` (a terminal admission and a released tombstone are producible in tests via the 1a-2b abort); server: `startServerWithConfig{f, leader, updatesEnabled, maxTokenAge}`, `newServerTestFSM`, `publishServerGenesis`, `pb.NewConsensusAdminClient`, `server/consensus_admin_test.go:32-38` (an FSM built with `AuthorityAddresses: []string{signer.Address()}`), `fakeNode`.

## Design decisions (rulings carried into the tasks)

1. **Scope split.** 1a-3 (this plan) = the capability governance switch + the four terminal actions + their reads: FSM-local, unblocks every later slice, restorable. 1a-3b = authenticated leader-barrier reads (`GetPublishedSnapshot`, `GetQueryPolicy`, retrofitting the four existing `SafeState` reads), Raft tags 23/26/27 with publisher authentication and `SourceClaims.RecordSnapshotArtifactReady`, the `ApplyArtifactDisposition` admission dependencies (authorizer/registry/source/allocator observers) that turn tag 28 into a proposable command, arbiter-core's reply/proof Go mirror and the per-action root vectors. 1a-3c = candidate-bound capacity charging (narrowing the whole-document charge vector or adding a non-lifecycle family, the C2 grant's Q binding, `SubmitSnapshotQuery` through the query transfer, abort settling the allowance). Cost: three plans instead of one; each ships green on its own.
2. **Capability enablement rides tag 18.** A new mutable consensus parameter, not a new Raft tag, not a disposition action (which the `Capability == 0` guard would refuse) and not genesis-only (a running network must be enableable). Authority signature, epoch and previous-digest fencing, drained-work preconditions and restore-time history validation come for free. Apply mirrors the parameter into `ArtifactDisposition.Capability` so the existing guard and its 17 tests keep reading one field; restore asserts equality.
3. **Evidence only from committed state.** `ResolveObligation`'s observations are matched to records by `SourceIndex`: `reservation_released` → a reservation tombstone with `TerminalIndex == SourceIndex` and matching identity; `query_terminal` → a terminal query admission with `Abort.CommitIndex == SourceIndex` (C5's applied terminal joins later) and matching identity; `candidate_terminal` → a cancelled or published candidate with `TerminalIndex == SourceIndex`; `challenge_resolved` and `cleanup_settled` are refused (no record exists yet — "C3 must add real authenticated physical cleanup settlement rather than weakening ResolveObligation"). A `query` obligation whose origin names an admission is resolvable only by that admission's terminal.
4. **CloseUse's conditional absent close** commits `cancelled_absent` with `Revision: 1`, `AdmittedIndex: 0`, `ClosedIndex: index`, `ClosureCommandRoot`; if an admission exists, revision 0 conflicts (addendum line 478); the exact close requires the same principal, `ExpectedRevision == stored.Revision`, `admitted` state, and records `RegistryClosedRevision = a.ExpectedRegistryRevision`.
5. **OpenChallenge is structural this wave.** It binds an admitted replay use by the verifier principal at the pin, a mismatch attestation signed by an active verifier (the `artifact_snapshot_query_attestation.go` verification extracted into a helper, `Receipt.MatchSourceRoot == false`, `ReceiptHash == Receipt.Hash()`), a `query` origin, the CLOSING-cut parent rule, and creates obligation `Kind: "challenge"`; binding the attestation to a committed source claim (tag 24 has no path) is deferred. The server's role mapping for `OpenChallenge` becomes `verifier` (the natural actor per the addendum table).
6. **Snapshot v13** carries no new container; the new record fields are refused in pre-v13 containers and validated causally in v13 (`ClosedIndex`/`RetiredIndex`/`ResolvedIndex` ≤ `LastAppliedIndex`, observation `SourceIndex ≤ ResolvedIndex`, `ClosureCommandRoot` non-empty iff closed/cancelled-absent, a `challenge` obligation's `ParentObligationSeq` names an existing obligation or is 0, `Capability == Params.ArtifactDispositionCapability`).
7. **Test helper.** `enableArtifactDispositionForTest(f *FSM)` sets `f.st.Params.ArtifactDispositionCapability = 1`, `f.st.BootstrapParams.ArtifactDispositionCapability = 1` and `f.st.ArtifactDisposition.Capability = 1` (bootstrap too, so `validateConsensusHistory` still holds with zero updates); every direct `Capability = 1` write in tests moves to it.

Execution order: Task 1 (arbiter-core) → Task 2 (arbiter-proto) → Task 3 (arbiter: pins, capability, v13) → Task 4 (retirement + use) → Task 5 (obligation + challenge + reads) → Task 6 (docs). One PR per task (Tasks 4 and 5 may combine).

---

### Task 1: `ArtifactDispositionCapability` on the consensus-params update (arbiter-core)

**Files:**
- Modify: `consensus.go` (`ConsensusParamsUpdate` field), `authority/consensus.go` (`NormalizeConsensusParamsUpdate` range check), `authority/consensus_test.go` (vectors)

**Interfaces:**
- Produces: `arbiter.ConsensusParamsUpdate.ArtifactDispositionCapability uint32 \`json:"artifact_disposition_capability,omitempty"\`` (after `ExpectedPromotionSeq`); `NormalizeConsensusParamsUpdate` refuses values above 1 with "consensus params update: artifact disposition capability must be 0 or 1".

- [ ] **Step 1: Write the failing tests**

Append to `authority/consensus_test.go`:

```go
func TestConsensusUpdateHashIgnoresAnAbsentCapability(t *testing.T) {
	base := testConsensusUpdate()
	want, err := ConsensusParamsUpdateHash(base)
	if err != nil {
		t.Fatal(err)
	}
	zero := base
	zero.ArtifactDispositionCapability = 0
	if got, _ := ConsensusParamsUpdateHash(zero); got != want {
		t.Fatalf("zero capability changed the digest: %s != %s", got, want)
	}
	enabled := base
	enabled.ArtifactDispositionCapability = 1
	if got, _ := ConsensusParamsUpdateHash(enabled); got == want {
		t.Fatal("capability 1 must change the digest")
	}
	invalid := base
	invalid.ArtifactDispositionCapability = 2
	if _, err := NormalizeConsensusParamsUpdate(invalid); err == nil || !strings.Contains(err.Error(), "capability") {
		t.Fatalf("capability 2 accepted: %v", err)
	}
}
```

If a frozen consensus-update digest vector exists in `consensus_test.go` (grep `0x` literals), leave it untouched — it must keep passing because zero is absent.

- [ ] **Step 2: Run the test to verify it fails**

Run: `bazel test //authority:authority_test --test_filter='TestConsensusUpdateHashIgnoresAnAbsentCapability' --test_output=errors`
Expected: build FAILS on the undefined field.

- [ ] **Step 3: Implement**

In `consensus.go`, after `ExpectedPromotionSeq`:

```go
	// ArtifactDispositionCapability is the C1 artifact-disposition kill switch
	// as a governed consensus parameter: 0 keeps every tag-28 command refused,
	// 1 enables the lane. It is absent from the canonical form when zero, so
	// every previously signed update keeps its digest; the FSM refuses 1 -> 0.
	ArtifactDispositionCapability uint32 `json:"artifact_disposition_capability,omitempty"`
```

In `authority/consensus.go`'s `NormalizeConsensusParamsUpdate`, after the `MaxWriters` check: `if cmd.ArtifactDispositionCapability > 1 { return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: artifact disposition capability must be 0 or 1") }`. The wire converter for `ConsensusParamsUpdate` (grep `ConsensusParamsUpdateToPB` in `wire/`) copies the new field to/from the proto field Task 2 adds — implement the Go side now against the field name `ArtifactDispositionCapability` and, since the proto field does not exist yet in the pinned arbiter-proto, gate the conversion behind Task 2's pin bump: land Task 1 with the struct + normalize + tests, and add the two converter lines in Task 2's arbiter-core follow-up commit (see Task 2 Step 4).

- [ ] **Step 4: Run the tests, commit, open the PR**

Run: `bazel test //... --test_output=errors && bash scripts/check-public-boundary.sh`.

```bash
git add consensus.go authority/consensus.go authority/consensus_test.go
git commit -m "feat(consensus): govern the artifact disposition capability as a consensus parameter"
```

---

### Task 2: Proto field and the arbiter-core converter (arbiter-proto, then arbiter-core)

**Files:**
- arbiter-proto — Modify: `proto/consensus.proto` (`ConsensusParamsUpdate.artifact_disposition_capability = 8`, `ConsensusMutableParams.artifact_disposition_capability = 3`), `gen/pb/*`, `conformance/consensus_test.go` (if it pins field counts), `docs/compatibility/snapshot-query-raft-allocation.md` (one line: tag 18's message gains field 8, no new tag)
- arbiter-core — Modify: `wire/consensus.go` (or wherever `ConsensusParamsUpdateToPB`/`FromPB` and the mutable-params converters live): copy the field both ways; `go.mod` pin bump to the merged arbiter-proto SHA

- [ ] **Step 1: Proto change and regeneration**

In `proto/consensus.proto`:

```protobuf
message ConsensusMutableParams {
  repeated string authority_addresses = 1;
  uint64 max_writers = 2;
  // 0 keeps artifact disposition (Raft tag 28) refused; 1 enables it. Monotone.
  uint32 artifact_disposition_capability = 3;
}
message ConsensusParamsUpdate {
  ...existing fields 1-7...
  // Governs the C1 artifact-disposition lane; 0 or 1, never lowered.
  uint32 artifact_disposition_capability = 8;
}
```

Run `make tools && make proto && make lint && make test && go test ./...`; extend any conformance test that freezes the `ConsensusParamsUpdate`/`ConsensusMutableParams` field sets the way `TestConsensusAdminRPCSignatures` was extended in Wave 1a-2b (record what you changed; never re-pin the historical descriptor baselines). Commit `feat(consensus): carry the artifact disposition capability in consensus params`, open the PR, merge on green, record the SHA.

- [ ] **Step 2: arbiter-core converter**

In the arbiter-core worktree: `go get github.com/sentioxyz/arbiter-proto@<merged SHA> && go mod tidy && bazel mod tidy` (follow the repo's own pin script if one exists — `ls scripts/`), then in the `ConsensusParamsUpdate` ToPB/FromPB and the `ConsensusMutableParams` converters copy `ArtifactDispositionCapability` ↔ `ArtifactDispositionCapability` (the generated getter is `GetArtifactDispositionCapability()`); add a round-trip test case with the field set to 1. Run `bazel test //...`. Commit `feat(wire): convert the artifact disposition capability consensus field`, open the PR, merge on green, record the SHA (Task 3 pins it).

---

### Task 3: Governed capability in the FSM, genesis stays off, snapshot v13 (arbiter)

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (pins), `fsm/state.go` (`Params.ArtifactDispositionCapability`), `fsm/params_identity.go` (no change to normalization; document that the field is mutable), `fsm/consensus_updates.go` (transition copies the field, refuses 1 → 0, mirrors into `ArtifactDisposition.Capability`), `config/config.go` (`GenesisConfig.ArtifactDispositionCapability`, validated ≤ 1, default 0), `cmd/arbiter/services.go` (`configuredFSMParams`), `cmd/arbiter-admin` (`consensus show` prints the field; `consensus update` accepts `--artifact-disposition-capability`), `server/consensus_admin.go` (`ConsensusMutableParams` conversion carries the field), `fsm/snapshot.go` (v13), `fsm/genesis_validation.go` (capability equality), the 17 test sites → `enableArtifactDispositionForTest`
- Create: `fsm/artifact_disposition_capability_test.go`, `fsm/artifact_disposition_capability.go` (the helper `enableArtifactDispositionForTest` lives in a `_test.go` file; the production file holds only the mirror function)

**Interfaces:**
- Produces: `Params.ArtifactDispositionCapability uint32 \`json:"artifact_disposition_capability,omitempty"\``; `func (f *FSM) mirrorArtifactDispositionCapabilityLocked()`; `snapshotVersion = 13`, `snapshotVersionV13`; `func validateArtifactDispositionCapability(st *State) error`; test helper `enableArtifactDispositionForTest(f *FSM)`.

- [ ] **Step 1: Bump the pins**

```bash
bash scripts/update-arbiter-core.sh <Task 2 arbiter-core merge SHA>
go get github.com/sentioxyz/arbiter-proto@<Task 2 arbiter-proto merge SHA> && go mod tidy && bazel mod tidy
bazel test //scripts/... //fsm:fsm_test //server:server_test --jobs=4 --test_output=errors
```

Commit `chore(deps): consume the artifact disposition capability consensus field`.

- [ ] **Step 2: Write the failing tests**

Create `fsm/artifact_disposition_capability_test.go`:

```go
package fsm

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// enableArtifactDispositionForTest installs the enabled capability the way a
// committed consensus update would leave it, on both the bootstrap and the
// current params so validateConsensusHistory holds with zero updates.
func enableArtifactDispositionForTest(f *FSM) {
	f.st.Params.ArtifactDispositionCapability = 1
	f.st.BootstrapParams.ArtifactDispositionCapability = 1
	f.st.ArtifactDisposition.Capability = 1
}

func capabilityUpdate(t *testing.T, f *FSM, a *authority.Signer, capability uint32) wire.Command {
	t.Helper()
	genesisID, err := f.genesisSnapshotIDLocked()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := f.st.Params.ConsensusDigest()
	if err != nil {
		t.Fatal(err)
	}
	update := arbiter.ConsensusParamsUpdate{NetworkID: f.st.Params.NetworkID, GenesisSnapshotID: genesisID, ExpectedEpoch: f.st.ConsensusEpoch, PreviousParamsDigest: digest,
		AuthorityAddresses: f.st.Params.AuthorityAddresses, MaxWriters: f.st.Params.maxWriters(), ExpectedPromotionSeq: f.st.PromotionSeq, ArtifactDispositionCapability: capability}
	jws, err := a.SignConsensusParamsUpdate(update)
	if err != nil {
		t.Fatal(err)
	}
	return wire.Command{UpdateConsensusParams: &wire.UpdateConsensusParams{Update: update, AuthorityJWS: jws}}
}

func TestGenesisStartsWithArtifactDispositionDisabled(t *testing.T) {
	f := newTestFSM(t)
	if f.st.Params.ArtifactDispositionCapability != 0 || f.st.ArtifactDisposition.Capability != 0 {
		t.Fatalf("genesis capability = %d/%d", f.st.Params.ArtifactDispositionCapability, f.st.ArtifactDisposition.Capability)
	}
	if got := f.applyArtifactDisposition(bindPolicyCommand(), 7); !isDispositionRejected(got, "disabled") {
		t.Fatalf("tag 28 at genesis = %#v", got)
	}
	// A pre-v13 container restores disabled too.
	legacy := snapshotBytes(t, f)
	legacy[4] = snapshotVersionV12
	if g := restoreInto(t, legacy); g.st.ArtifactDisposition.Capability != 0 {
		t.Fatal("v12 restore enabled the capability")
	}
}

func TestConsensusUpdateEnablesArtifactDispositionMonotonically(t *testing.T) {
	f := newTestFSM(t)
	a, _ := authority.NewSignerFromHex(testAbortAuthorityKey)
	f.st.Params.AuthorityAddresses = []string{a.Address()}
	f.st.BootstrapParams.AuthorityAddresses = []string{a.Address()}
	if got := applyCmd(t, f, 41, capabilityUpdate(t, f, a, 1)); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("enable = %#v", got)
	}
	if f.st.Params.ArtifactDispositionCapability != 1 || f.st.ArtifactDisposition.Capability != 1 || f.st.ConsensusEpoch != 1 {
		t.Fatalf("after enable: params=%d disposition=%d epoch=%d", f.st.Params.ArtifactDispositionCapability, f.st.ArtifactDisposition.Capability, f.st.ConsensusEpoch)
	}
	if got := f.applyArtifactDisposition(bindPolicyCommand(), 42); !reflect.DeepEqual(got, Applied{}) {
		t.Fatalf("tag 28 after enable = %#v", got)
	}
	before := snapshotBytes(t, f)
	if got, ok := applyCmd(t, f, 43, capabilityUpdate(t, f, a, 0)).(Rejected); !ok || !strings.Contains(got.Reason, "capability") || string(snapshotBytes(t, f)) != string(before) {
		t.Fatalf("disable = %#v", got)
	}
	// Restore keeps both fields equal; a divergent container is refused.
	data := snapshotBytes(t, f)
	if data[4] != snapshotVersionV13 {
		t.Fatalf("version byte = %d, want 13", data[4])
	}
	g := restoreInto(t, data)
	if g.st.Params.ArtifactDispositionCapability != 1 || g.st.ArtifactDisposition.Capability != 1 {
		t.Fatal("capability did not survive restore")
	}
	tampered := rewriteSnapshotDocument(t, data, snapshotVersionV13, func(doc map[string]any) { doc["artifact_disposition"].(map[string]any)["capability"] = float64(0) })
	if err := restoreErr(t, tampered); err == nil || !strings.Contains(err.Error(), "capability") {
		t.Fatalf("divergent capability restored: %v", err)
	}
	pre := rewriteSnapshotDocument(t, data, snapshotVersionV12, func(map[string]any) {})
	if err := restoreErr(t, pre); err == nil || !strings.Contains(err.Error(), "capability") {
		t.Fatalf("pre-v13 container with capability restored: %v", err)
	}
}
```

(`restoreErr` exists in `fsm/apply_snapshot_query_abort_test.go`; `applyCmd` and `testAbortAuthorityKey` likewise.) Also add to `config/config_test.go` a case that `genesis.artifact_disposition_capability: 2` fails validation and the omitted field defaults to 0, and to `server/consensus_admin_test.go` an end-to-end case: `UpdateConsensusParams` through the real client with `ArtifactDispositionCapability: 1` → `Ack`; `GetConsensusParams` shows `current.artifact_disposition_capability == 1`; a second update lowering it → `InvalidArgument` carrying the FSM's reason.

- [ ] **Step 3: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestGenesisStartsWithArtifactDispositionDisabled|TestConsensusUpdateEnablesArtifactDisposition' --jobs=4 --test_output=errors`
Expected: build FAILS on the undefined `Params.ArtifactDispositionCapability`.

- [ ] **Step 4: Implement**

`fsm/state.go`, in `Params` after `MaxWriters`:

```go
	// ArtifactDispositionCapability is the governed C1 kill switch (0 or 1).
	// It is mutable through UpdateConsensusParams, never lowered, and Apply
	// mirrors it into ArtifactDisposition.Capability so the tag-28 guard and
	// the persisted lane state read one value; restore asserts they agree.
	ArtifactDispositionCapability uint32 `json:"artifact_disposition_capability,omitempty"`
```

`fsm/consensus_updates.go`, in `verifyConsensusTransition` after `next.MaxWriters = normalized.MaxWriters`:

```go
	if normalized.ArtifactDispositionCapability < previous.ArtifactDispositionCapability {
		return Params{}, fmt.Errorf("consensus update cannot lower the artifact disposition capability")
	}
	next.ArtifactDispositionCapability = normalized.ArtifactDispositionCapability
```

and in `applyUpdateConsensusParams` right after `f.st.Params = next`: `f.mirrorArtifactDispositionCapabilityLocked()`. Create `fsm/artifact_disposition_capability.go`:

```go
package fsm

// mirrorArtifactDispositionCapabilityLocked copies the governed consensus
// parameter into the persisted lane switch. It is the only writer of
// ArtifactDisposition.Capability outside tests and runs inside Apply.
func (f *FSM) mirrorArtifactDispositionCapabilityLocked() {
	f.st.ArtifactDisposition.Capability = f.st.Params.ArtifactDispositionCapability
}

// validateArtifactDispositionCapability refuses a container whose persisted
// lane switch disagrees with the governed parameter. Pre-v13 containers had
// no governed parameter, so any non-zero switch there is test-authored.
func validateArtifactDispositionCapability(st *State) error {
	if st.ArtifactDisposition.Capability > 1 || st.Params.ArtifactDispositionCapability > 1 {
		return fmt.Errorf("artifact disposition capability must be 0 or 1")
	}
	if st.ArtifactDisposition.Capability != st.Params.ArtifactDispositionCapability {
		return fmt.Errorf("artifact disposition capability %d disagrees with the consensus parameter %d", st.ArtifactDisposition.Capability, st.Params.ArtifactDispositionCapability)
	}
	return nil
}
```

`fsm/snapshot.go`: `snapshotVersion = 13`, `snapshotVersionV13 = 13`, keep `snapshotVersionV12`; extend the accept list + error text and the three predicates with v12; add, next to the pre-v12 admission gate, `if ver[0] < snapshotVersionV13 && st.ArtifactDisposition.Capability != 0 { return nil, fmt.Errorf("artifact disposition capability is set in a pre-v13 container") }`; call `validateArtifactDispositionCapability(st)` after `validateConsensusHistory` (so `Params` is already validated). `config/config.go`: `GenesisConfig.ArtifactDispositionCapability uint32 \`yaml:"artifact_disposition_capability"\`` with the doc "Bootstrap value of the governed C1 kill switch; omitted or 0 keeps artifact disposition refused. Prefer enabling at runtime through consensus update." and a validation error for values above 1; `configuredFSMParams` copies it. `server/consensus_admin.go`: the `ConsensusMutableParams` conversion (grep `MaxWriters:` in that file) carries the field both ways; `authorizeConsensusParamsUpdate`'s preconditions need no change. `cmd/arbiter-admin`: `consensus show` prints `artifact_disposition_capability`; `consensus update` gains `--artifact-disposition-capability` (default: keep current). Replace the 17 test writes of `Capability = 1` with `enableArtifactDispositionForTest(f)`; update the version-byte assertions to v13 (fixtures deliberately built at older versions stay).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test //server:server_test //config:config_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add fsm/state.go fsm/consensus_updates.go fsm/artifact_disposition_capability.go fsm/artifact_disposition_capability_test.go fsm/snapshot.go fsm/genesis_validation.go config/config.go config/config_test.go cmd/arbiter/services.go cmd/arbiter-admin server/consensus_admin.go server/consensus_admin_test.go fsm/*_test.go cmd/arbiter/storage_protocol_test.go
git commit -m "feat(fsm): govern the artifact disposition capability through consensus updates; snapshot format v13"
```

---

### Task 4: `FinishRetirement` and `CloseUse` (arbiter fsm)

**Files:**
- Modify: `fsm/apply_artifact_disposition.go` (two switch arms, `dispositionOperationTarget` arms), `fsm/genesis_validation.go` (restore validators for retired retirements and closed / cancelled-absent uses, pre-v13 refusal), `fsm/snapshot.go` (the pre-v13 gate extended)
- Create: `fsm/apply_artifact_disposition_terminal.go`, `fsm/artifact_retirement_finish_test.go`, `fsm/artifact_use_close_test.go`

**Interfaces:**
- Consumes: `admin`, `policyAdministrator`, `publishedRetirementCandidate`, `retirementKey`, `increment`, `ValidArtifactDispositionUseIdentity`, `publishedOpenUseTarget`, `populateRetirementReadView`'s predicates (re-derived locally), `f.st.SafeWatermark`.
- Produces: `func (f *FSM) applyFinishRetirement(c *wire.ArtifactDispositionCmd, a *wire.ArtifactDispositionRetirementTargetV1, index uint64) any`; `func (f *FSM) applyCloseUse(c *wire.ArtifactDispositionCmd, a *wire.ArtifactDispositionCloseUseV1, index uint64) any`; `func retirementHasOpenWork(d *ArtifactDispositionState, pin replay.SnapshotPin) (uses int, obligations int)`; `func validateArtifactDispositionTerminalRecords(st *State) error`.

- [ ] **Step 1: Write the failing tests**

`fsm/artifact_retirement_finish_test.go`: from `publishedHistoricalCandidate`/`beginRetirementCommand` (a CLOSING retirement), assert: (a) finish with the exact revision by the policy administrator → `Applied`, `State == retired`, `RetiredIndex == index`, revision incremented, operation `TargetKind == "retirement"` with the new revision, `ArtifactDispositionRead` of the retirement selector reports `retired` (not not-found); (b) refusals, each with a byte-identical snapshot: wrong role (`publisher`), wrong administrator, `OPEN` (not yet cut) state, wrong `ExpectedRevision`, an admitted use at the pin ("obligations_open"-class reason containing "use"), an open obligation at the pin (containing "obligation"), the current tip (the candidate whose manifest is the watermark → reason containing "current"); (c) idempotent retry of the same command root → `Applied`, no change; (d) a second finish → revision conflict.

`fsm/artifact_use_close_test.go`: from `openPublishedCandidate` + `admitUseCommand`: (a) exact close by the same principal at `ExpectedRevision == 1` with `ExpectedRegistryRevision: 7` → `Applied`, `State == closed`, `Revision == 2`, `ClosedIndex == index`, `RegistryClosedRevision == 7`, `ClosureCommandRoot == c.Validation.CommandRoot`, operation `TargetKind == "use"`; (b) conditional absent close (`ExpectedRevision 0`, no admission) → `Applied`, `Uses[ref] = {State: cancelled_absent, Revision: 1, AdmittedIndex: 0, ClosedIndex: index, ClosureCommandRoot}`; a later `AdmitUse` for that reference is refused ("already bound"); (c) refusals with byte-identical snapshots: revision 0 when an admission exists ("conflict"), wrong revision, another principal, already closed, `ContinuationObligationSeq != 0` on the wire use, role `publisher`; (d) restore round trip after (a) and (b); a pre-v13 container carrying a closed use is refused; a v13 container whose closed use has an empty `closure_command_root` is refused.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestFinishRetirement|TestCloseUse' --jobs=4 --test_output=errors`
Expected: FAIL with the `default:` arm's "requires the enabled capacity and service-validation slice" reason.

- [ ] **Step 3: Implement**

Create `fsm/apply_artifact_disposition_terminal.go`:

```go
package fsm

import (
	"reflect"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/wire"
)

// retirementHasOpenWork counts the admitted uses and open obligations pinned
// to the retirement's snapshot: the same predicates the retirement read view
// projects as active_use_ids / unresolved_obligation_seqs.
func retirementHasOpenWork(d *ArtifactDispositionState, pin replay.SnapshotPin) (uses int, obligations int) {
	for _, use := range d.Uses {
		if use != nil && use.State == ArtifactUseAdmitted && reflect.DeepEqual(use.Use.Pin, pin) {
			uses++
		}
	}
	for _, obligation := range d.Obligations {
		if obligation != nil && obligation.State == ArtifactObligationOpen && reflect.DeepEqual(obligation.Pin, pin) {
			obligations++
		}
	}
	return uses, obligations
}

// applyFinishRetirement records RETIRED for a CLOSING noncurrent snapshot once
// every admitted use and open obligation at its pin is gone. Apply rechecks
// the complete current state; a caller boolean proves nothing. The current
// safe tip cannot retire.
func (f *FSM) applyFinishRetirement(c *wire.ArtifactDispositionCmd, a *wire.ArtifactDispositionRetirementTargetV1, index uint64) any {
	d := &f.st.ArtifactDisposition
	if d.Policy == nil || d.Policy.PolicyID != a.RetentionPolicyID || !admin(c, "policy_admin") || !policyAdministrator(d.Policy, c.Command.ActorID) {
		return Rejected{Reason: "retirement policy administrator authorization mismatch"}
	}
	candidate, reason := publishedRetirementCandidate(d, a)
	if reason != "" {
		return Rejected{Reason: reason}
	}
	if candidate.Manifest.SnapshotID == f.st.SafeWatermark.SnapshotID {
		return Rejected{Reason: "retirement target is the current safe tip"}
	}
	existing := d.Retirements[retirementKey(a.Pin, a.RetentionPolicyID)]
	if existing == nil {
		return Rejected{Reason: "retirement record is required"}
	}
	if existing.State == ArtifactRetirementRetired {
		return Rejected{Reason: "retirement is already retired"}
	}
	if existing.State != ArtifactRetirementClosing || c.Command.ExpectedRevision != existing.Revision {
		return Rejected{Reason: "retirement state or expected_revision mismatch"}
	}
	if !reflect.DeepEqual(existing.Pin, a.Pin) || existing.RetentionPolicyID != a.RetentionPolicyID || existing.PublishedCandidateSeq != candidate.CandidateSeq {
		return Rejected{Reason: "retirement target mismatch"}
	}
	if uses, obligations := retirementHasOpenWork(d, a.Pin); uses != 0 || obligations != 0 {
		return Rejected{Reason: "retirement has open work: admitted use or unresolved obligation at the pin"}
	}
	if err := increment(&existing.Revision, "retirement revision"); err != nil {
		return Rejected{Reason: err.Error()}
	}
	existing.State, existing.RetiredIndex = ArtifactRetirementRetired, index
	return Applied{}
}

// applyCloseUse closes an admitted use at its exact revision, or — the
// conditional absent close at revision 0 — commits a cancelled_absent
// tombstone that fences any delayed admission of the same reference.
func (f *FSM) applyCloseUse(c *wire.ArtifactDispositionCmd, a *wire.ArtifactDispositionCloseUseV1, index uint64) any {
	if a == nil {
		return Rejected{Reason: "close use is required"}
	}
	d, use := &f.st.ArtifactDisposition, a.Use
	if !ValidArtifactDispositionUseIdentity(use) || c.Command.ActorID != use.PrincipalID || (c.Validation.ActorRole != "source" && c.Validation.ActorRole != "verifier") {
		return Rejected{Reason: "close use identity mismatch"}
	}
	if use.ContinuationObligationSeq != 0 {
		return Rejected{Reason: "close use continuation is unavailable"}
	}
	stored := d.Uses[use.ReferenceID]
	if c.Command.ExpectedRevision == 0 {
		if stored != nil {
			return Rejected{Reason: "close use revision conflict: the reference is admitted; close its exact revision"}
		}
		d.Uses[use.ReferenceID] = &ArtifactDispositionUseState{Use: use, State: ArtifactUseCancelledAbsent, Revision: 1, AdmittedIndex: 0,
			ClosedIndex: index, RegistryClosedRevision: a.ExpectedRegistryRevision, ClosureCommandRoot: c.Validation.CommandRoot}
		return Applied{}
	}
	if stored == nil {
		return Rejected{Reason: "close use reference is not admitted"}
	}
	if stored.State != ArtifactUseAdmitted {
		return Rejected{Reason: "close use reference is already terminal"}
	}
	if stored.Revision != c.Command.ExpectedRevision || !reflect.DeepEqual(stored.Use, use) {
		return Rejected{Reason: "close use revision or identity mismatch"}
	}
	if err := increment(&stored.Revision, "use revision"); err != nil {
		return Rejected{Reason: err.Error()}
	}
	stored.State, stored.ClosedIndex, stored.RegistryClosedRevision, stored.ClosureCommandRoot = ArtifactUseClosed, index, a.ExpectedRegistryRevision, c.Validation.CommandRoot
	return Applied{}
}
```

In `applyArtifactDisposition`'s switch add `case a.FinishRetirement != nil: result = f.applyFinishRetirement(c, a.FinishRetirement, index)` and `case a.CloseUse != nil: result = f.applyCloseUse(c, a.CloseUse, index)` (match the existing arms' exact form). In `dispositionOperationTarget` add `case action.FinishRetirement != nil:` mirroring the `BeginRetirement` arm, and `case action.CloseUse != nil:` mirroring `AdmitUse` (the stored use's revision after the write). `applyAdmitUse` already refuses a reference that exists in `Uses`, which is what fences a delayed admission after an absent close — pin it with the test. Restore: `validateArtifactDispositionTerminalRecords(st)` — for every retirement in `retired`: `RetiredIndex != 0 && RetiredIndex <= ArtifactDisposition.LastAppliedIndex` and `CutIndex != 0 && CutIndex < RetiredIndex`; for every use: `closed` ⇒ `AdmittedIndex != 0 && ClosedIndex > AdmittedIndex && ClosureCommandRoot != ""`, `cancelled_absent` ⇒ `AdmittedIndex == 0 && ClosedIndex != 0 && Revision == 1 && ClosureCommandRoot != ""`, `admitted` ⇒ `ClosedIndex == 0 && ClosureCommandRoot == ""`; unknown states refused. Extend the pre-v13 gate: a pre-v13 container with any retired retirement or closed / cancelled-absent use is refused. Error strings start with `artifact disposition`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS, including `TestAdmitUseRefusesUnimplementedActionsWithoutMutation` amended to cover only `OpenChallenge`/`ResolveObligation` (until Task 5 removes those two as well).

- [ ] **Step 5: Commit**

```bash
git add fsm/apply_artifact_disposition.go fsm/apply_artifact_disposition_terminal.go fsm/artifact_retirement_finish_test.go fsm/artifact_use_close_test.go fsm/artifact_use_test.go fsm/genesis_validation.go fsm/snapshot.go
git commit -m "feat(fsm): finish retirements and close uses, including the conditional absent close"
```

---

### Task 5: `ResolveObligation`, `OpenChallenge`, the obligation read (arbiter fsm + server)

**Files:**
- Modify: `fsm/apply_artifact_disposition.go` (two arms, targets), `fsm/apply_artifact_disposition_terminal.go` (the two handlers), `fsm/artifact_disposition_reads.go` (`ArtifactDispositionReadObligation` kind + selector + view), `fsm/artifact_snapshot_query_attestation.go` (extract `verifySnapshotQueryAttestationSignatureLocked`), `fsm/genesis_validation.go` (obligation validators), `server/artifact_disposition.go` (obligation record arm; `OpenChallenge` role → `verifier`), `server/server_test.go` (obligation selector test)
- Create: `fsm/artifact_obligation_resolve_test.go`, `fsm/artifact_challenge_open_test.go`

**Interfaces:**
- Produces: `func (f *FSM) applyResolveObligation(c *wire.ArtifactDispositionCmd, a *wire.ArtifactDispositionResolveObligationV1, index uint64) any`; `func (f *FSM) applyOpenChallenge(c *wire.ArtifactDispositionCmd, a *wire.ArtifactDispositionOpenChallengeV1, index uint64) any`; `const ArtifactObligationKindChallenge = "challenge"` (plus `ArtifactObligationKindQuery = "query"`, `ArtifactObligationKindCandidate = "candidate"` replacing the literals); `func (f *FSM) settlementObservationMatchesLocked(o wire.ArtifactDispositionSourceObservationV1, obligation *ArtifactDispositionObligationState) string` (empty = matches); `ArtifactDispositionReadObligation ArtifactDispositionReadKind = "obligation"`, `ArtifactDispositionReadSelector.ObligationSeq`, `ArtifactDispositionReadView.Obligation *ArtifactDispositionObligationState`.

- [ ] **Step 1: Write the failing tests**

`fsm/artifact_obligation_resolve_test.go`: obligations are produced by (i) `RegisterCandidate` (`Kind candidate`) and (ii) the 1a-2 test path `sequencedQuery` + Task 4's `enableArtifactDispositionForTest` (a `query` origin is only written by the charged transfer, so build the `query` case by inserting an obligation `{Kind: "query", Origin: {Kind: "query", ClientAccount, StatementID, InputRoot, BlockSeq, StatementSeq}}` directly and then aborting through `signedAbort`, which produces the terminal admission at a known index). Cases: (a) `candidate_terminal`: cancel the candidate (`CancelCandidate`, note its apply index T) → resolve with `Validation.SourceObservations = [{ObligationSeq, SourceKind: "candidate_terminal", SourceIndex: T, SourceIdentity: {Kind: "publication"/the candidate's origin, CandidateSeq}}]` and `ExpectedRevision == obligation.Revision` → `Applied`, `State == resolved`, `ResolvedIndex == index`, `ResolutionSourceObservations` stored, operation `TargetKind == "obligation"`; (b) `query_terminal` after an abort at index A: observation `{SourceKind: "query_terminal", SourceIndex: A, SourceIdentity: {Kind: "query", ClientAccount, StatementID, InputRoot, ExecutionOutcome: "aborted"}}` → `Applied`; (c) `reservation_released` with `SourceIndex` = the tombstone's `TerminalIndex` → `Applied` for a `reservation`-kind obligation (insert one directly); (d) refusals with byte-identical snapshots: no observations, an observation whose `SourceIndex` names no record, wrong identity, `challenge_resolved`/`cleanup_settled` kinds ("not available"), wrong revision, already resolved, wrong role, observation for a different `ObligationSeq`, a `ReplacementObligationSeqs` entry that is not an open obligation with `ParentObligationSeq == this`; (e) restore round trip; a pre-v13 container with a resolved obligation is refused; a v13 resolved obligation with empty observations or `SourceIndex > ResolvedIndex` is refused.

`fsm/artifact_challenge_open_test.go`: from `openPublishedCandidate` admit a replay use by a verifier principal (`registerActive(t, f, "verifier-1", verifier role)`; the use's `PrincipalID` = that node id; role `verifier`) then open a challenge with a mismatch attestation signed by that active verifier (build `replay.SnapshotQueryAttestation{ReplicaID, Receipt{... MatchSourceRoot: false ...}, ReceiptHash: receipt.Hash(), Signature: ed25519 over the receipt hash the way `artifact_snapshot_query_attestation_test.go` signs}`) → `Applied`, a new obligation `{Kind: "challenge", Pin, Origin: a.Origin, ParentObligationSeq: 0, State: open, Revision: 1, AdmittedIndex: index}`, `NextObligationSeq` incremented, operation `TargetKind == "obligation"`; refusals with byte-identical snapshots: `ExpectedRevision != 0`, replay use not admitted / closed (Task 4's close) / another principal / another pin, attestation by a non-active replica, bad signature, `MatchSourceRoot == true`, `ReceiptHash` mismatch, origin kind not `query`, role `source`; CLOSING rule: after `BeginRetirement` cut at C, a use admitted after C (AdmittedIndex ≥ CutIndex) with `ParentObligationSeq` requirement — the challenge's `Origin`… (since the wire carries no parent field, the parent is derived: `ReplayUse.ContinuationObligationSeq`; a post-cut use with 0 → refused "post-cut replay must name its pre-cut ancestor"); restore round trip; a pre-v13 container with a challenge obligation is refused.

Server (`server/server_test.go`): `GetArtifactDisposition` with the `obligation_seq` selector returns the obligation record with all fields; `artifactDispositionRole` maps `OpenChallenge` to `verifier`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bazel test //fsm:fsm_test --test_filter='TestResolveObligation|TestOpenChallenge' --jobs=4 --test_output=errors`
Expected: FAIL on the `default:` arm.

- [ ] **Step 3: Implement**

Append to `fsm/apply_artifact_disposition_terminal.go`:

```go
const (
	ArtifactObligationKindCandidate = "candidate"
	ArtifactObligationKindQuery     = "query"
	ArtifactObligationKindChallenge = "challenge"
	ArtifactObligationKindReservation = "reservation"
)

// settlementObservationMatchesLocked returns "" when the observation names a
// committed record at exactly SourceIndex whose identity settles the
// obligation, else the refusal reason. Kinds without a committed record yet
// (challenge_resolved, cleanup_settled) are refused: settlement is never
// inferred from absence, a boolean or a caller-selected index.
func (f *FSM) settlementObservationMatchesLocked(o wire.ArtifactDispositionSourceObservationV1, obligation *ArtifactDispositionObligationState) string {
	d := &f.st.ArtifactDisposition
	if o.ObligationSeq != obligation.ObligationSeq || o.SourceIndex == 0 {
		return "settlement observation does not name this obligation at a committed index"
	}
	switch o.SourceKind {
	case "reservation_released":
		for _, t := range d.ReservationTombstones {
			if t != nil && t.TerminalIndex == o.SourceIndex && t.ClientAccount == o.SourceIdentity.ClientAccount && t.StatementID == o.SourceIdentity.StatementID &&
				t.Reservation != nil && t.Reservation.ReservationID == o.SourceIdentity.ReservationID && t.Reservation.ReservationID == obligation.Origin.ReservationID {
				return ""
			}
		}
		return "settlement observation names no released reservation at its index"
	case "query_terminal":
		for _, a := range d.QueryAdmissions {
			if a != nil && a.Lifecycle == SnapshotQueryLifecycleTerminal && a.Abort != nil && a.Abort.CommitIndex == o.SourceIndex &&
				a.ClientAccount == o.SourceIdentity.ClientAccount && a.StatementID == o.SourceIdentity.StatementID && a.InputRoot == o.SourceIdentity.InputRoot &&
				a.ExecutionOutcome == o.SourceIdentity.ExecutionOutcome && a.StatementID == obligation.Origin.StatementID && a.InputRoot == obligation.Origin.InputRoot {
				return ""
			}
		}
		return "settlement observation names no terminal query admission at its index"
	case "candidate_terminal":
		for _, c := range d.Candidates {
			if c != nil && (c.State == ArtifactCandidateCancelled || c.State == ArtifactCandidatePublished) && c.TerminalIndex == o.SourceIndex &&
				c.CandidateSeq == o.SourceIdentity.CandidateSeq && obligation.Origin.CandidateSeq == c.CandidateSeq {
				return ""
			}
		}
		return "settlement observation names no terminal candidate at its index"
	case "challenge_resolved", "cleanup_settled":
		return "settlement kind " + o.SourceKind + " has no committed record yet"
	default:
		return "settlement observation kind is unknown"
	}
}

// applyResolveObligation settles one open obligation from committed
// settlement records only: the caller supplies no outcome, and every
// observation is re-derived against exact indices and identities.
func (f *FSM) applyResolveObligation(c *wire.ArtifactDispositionCmd, a *wire.ArtifactDispositionResolveObligationV1, index uint64) any {
	if a == nil || a.ObligationSeq == 0 {
		return Rejected{Reason: "resolve obligation requires an obligation sequence"}
	}
	d := &f.st.ArtifactDisposition
	if c.Validation.ActorRole != "source" && c.Validation.ActorRole != "coordinator" {
		return Rejected{Reason: "resolve obligation role mismatch"}
	}
	obligation := d.Obligations[a.ObligationSeq]
	if obligation == nil {
		return Rejected{Reason: "obligation is not found"}
	}
	if obligation.State != ArtifactObligationOpen {
		return Rejected{Reason: "obligation is already resolved"}
	}
	if c.Command.ExpectedRevision != obligation.Revision {
		return Rejected{Reason: "obligation expected_revision mismatch"}
	}
	if len(c.Validation.SourceObservations) == 0 {
		return Rejected{Reason: "resolve obligation requires committed settlement observations"}
	}
	for _, o := range c.Validation.SourceObservations {
		if reason := f.settlementObservationMatchesLocked(o, obligation); reason != "" {
			return Rejected{Reason: reason}
		}
		for _, seq := range o.ReplacementObligationSeqs {
			child := d.Obligations[seq]
			if child == nil || child.State != ArtifactObligationOpen || child.ParentObligationSeq != obligation.ObligationSeq {
				return Rejected{Reason: "replacement obligation is not an open child of this obligation"}
			}
		}
	}
	if err := increment(&obligation.Revision, "obligation revision"); err != nil {
		return Rejected{Reason: err.Error()}
	}
	obligation.State, obligation.ResolvedIndex = ArtifactObligationResolved, index
	obligation.ResolutionSourceObservations = append([]wire.ArtifactDispositionSourceObservationV1(nil), c.Validation.SourceObservations...)
	obligation.ReplacementObligationSeqs = nil
	for _, o := range c.Validation.SourceObservations {
		obligation.ReplacementObligationSeqs = append(obligation.ReplacementObligationSeqs, o.ReplacementObligationSeqs...)
	}
	return Applied{}
}

// applyOpenChallenge commits the challenge obligation for an admitted replay
// use and a verifier-signed mismatch attestation before any close or
// release can drop replay protection. It refuses a closed use, an inactive
// or unsigned replica, and — while the retirement is CLOSING — a post-cut
// replay that names no still-open pre-cut ancestor.
func (f *FSM) applyOpenChallenge(c *wire.ArtifactDispositionCmd, a *wire.ArtifactDispositionOpenChallengeV1, index uint64) any {
	if a == nil {
		return Rejected{Reason: "open challenge is required"}
	}
	d := &f.st.ArtifactDisposition
	if c.Command.ExpectedRevision != 0 || c.Validation.ActorRole != "verifier" {
		return Rejected{Reason: "open challenge expected_revision must be zero and the actor must be a verifier"}
	}
	use := d.Uses[a.ReplayUse.ReferenceID]
	if use == nil || use.State != ArtifactUseAdmitted || !reflect.DeepEqual(use.Use, a.ReplayUse) || use.Use.PrincipalID != c.Command.ActorID || !reflect.DeepEqual(use.Use.Pin, a.Pin) {
		return Rejected{Reason: "open challenge replay use is not the actor's admitted use at the pin"}
	}
	if a.Origin.Kind != ArtifactObligationKindQuery {
		return Rejected{Reason: "open challenge origin must be a query"}
	}
	if a.Attestation.Receipt.MatchSourceRoot {
		return Rejected{Reason: "open challenge requires a mismatch attestation"}
	}
	if reason := f.verifySnapshotQueryAttestationSignatureLocked(a.Attestation); reason != "" {
		return Rejected{Reason: "open challenge attestation: " + reason}
	}
	parent := a.ReplayUse.ContinuationObligationSeq
	if retirement := f.retirementAtPinLocked(a.Pin); retirement != nil && retirement.State == ArtifactRetirementClosing && use.AdmittedIndex >= retirement.CutIndex {
		ancestor := d.Obligations[parent]
		if parent == 0 || ancestor == nil || ancestor.State != ArtifactObligationOpen || ancestor.AdmittedIndex >= retirement.CutIndex {
			return Rejected{Reason: "post-cut replay must name a still-open pre-cut ancestor obligation"}
		}
	} else if parent != 0 {
		if ancestor := d.Obligations[parent]; ancestor == nil || ancestor.State != ArtifactObligationOpen {
			return Rejected{Reason: "challenge ancestor obligation is not open"}
		}
	}
	if d.NextObligationSeq == 0 || d.NextObligationSeq == ^uint64(0) || d.Obligations[d.NextObligationSeq] != nil {
		return Rejected{Reason: "obligation sequence overflow"}
	}
	seq := d.NextObligationSeq
	d.Obligations[seq] = &ArtifactDispositionObligationState{ObligationSeq: seq, Pin: a.Pin, Kind: ArtifactObligationKindChallenge, Origin: a.Origin,
		ParentObligationSeq: parent, State: ArtifactObligationOpen, Revision: 1, AdmittedIndex: index}
	d.NextObligationSeq++
	return Applied{}
}
```

`verifySnapshotQueryAttestationSignatureLocked(att replay.SnapshotQueryAttestation) string` is extracted from `artifact_snapshot_query_attestation.go`'s existing validation (active verifier `f.activeVerifier(att.ReplicaID, true)`, `att.ReceiptHash == att.Receipt.Hash()`, node signature over the receipt hash via the shipped verifier) so both paths share one body; `retirementAtPinLocked(pin)` returns the unique retirement record whose `Pin` deep-equals (nil if none). Add the switch arms, the `dispositionOperationTarget` arms (`ResolveObligation` → `{kind: "obligation", obligationSeq, revision: obligation.Revision}`; `OpenChallenge` → the new obligation `NextObligationSeq-1`), and the target struct field `obligationSeq` + `TargetObligationSeq` on `ArtifactDispositionOperation` (json `target_obligation_seq,omitempty`). Reads: `ArtifactDispositionReadObligation`, selector `ObligationSeq`, view `Obligation *ArtifactDispositionObligationState` (detached copy); server `artifactDispositionRecord` gains the `Obligation` arm mapping to `pb.ArtifactDispositionObligationRecordV1` (all eleven fields; `resolution_source_observations` through the existing `wire` converter for `ArtifactDispositionSourceObservationV1`), the selector switch accepts `obligation_seq`, and `artifactDispositionRole` maps `OpenChallenge` to `verifier` (keep the other three on `source`). Restore (`validateArtifactDispositionTerminalRecords`): `resolved` ⇒ `ResolvedIndex != 0 && ResolvedIndex <= LastAppliedIndex`, non-empty observations each with `SourceIndex <= ResolvedIndex` and `ObligationSeq == this`; `open` ⇒ zero `ResolvedIndex`, empty observations; `Kind` ∈ {candidate, reservation, query, challenge}; a `challenge` obligation's `ParentObligationSeq` is 0 or names an existing obligation; pre-v13 containers with a resolved or challenge obligation are refused.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bazel run //:gazelle && bazel test //fsm:fsm_test //server:server_test --jobs=4 --test_output=errors` then `bazel test //... --jobs=4 --test_output=errors`.
Expected: PASS; delete `TestAdmitUseRefusesUnimplementedActionsWithoutMutation` (nothing is unimplemented; `GrantReservation`'s public refusal keeps its own test if one exists, else add one line asserting the `default:` arm still refuses it).

- [ ] **Step 5: Commit**

```bash
git add fsm/apply_artifact_disposition.go fsm/apply_artifact_disposition_terminal.go fsm/artifact_snapshot_query_attestation.go fsm/artifact_disposition_reads.go fsm/state.go fsm/genesis_validation.go fsm/artifact_obligation_resolve_test.go fsm/artifact_challenge_open_test.go fsm/artifact_use_test.go server/artifact_disposition.go server/server_test.go
git commit -m "feat(fsm): resolve obligations from committed settlements and open challenge obligations"
```

---

### Task 6: Documentation (arbiter + housegate amendments)

**Files:**
- Modify: `docs/snapshot-query-reservations.md` (a new section "Disposition capability and the terminal actions"), `README.md` (the consensus-admin paragraph mentions the capability), `docs/specs/2026-09-17-consensus-parameter-updates.md` (if that spec enumerates mutable parameters, add the capability with its monotone rule)

- [ ] **Step 1: Write**

One paragraph per line, verified against the code: the governed capability (consensus parameter, monotone, mirrored, restore equality, genesis 0, `consensus update --artifact-disposition-capability 1`), what enabling does and does not do (tag 28 accepted by Apply; still no proposer because `ApplyArtifactDisposition` refuses without the admission dependencies — 1a-3b), the four actions with their actors, revisions, predicates, evidence sources and refusal reasons, the obligation kinds and the `challenge` obligation, the obligation read selector, snapshot v13 and the pre-v13 refusals, and the deferred list (1a-3b: barrier reads, tags 23/26/27, publisher authentication, admission dependencies, reply Go mirror and per-action vectors; 1a-3c: capacity charging, the C2 Q binding, submit/abort through the charged path; `challenge_resolved`/`cleanup_settled` settlement records; binding a challenge to a committed source claim).

- [ ] **Step 2: Commit**

```bash
git add docs/snapshot-query-reservations.md README.md docs/specs
git commit -m "docs: describe the governed disposition capability and the terminal disposition actions"
```

---

## Deferred (recorded, not placeholders)

- **1a-3b — C1 service surface.** Authenticated leader-barrier reads `SafeState.GetPublishedSnapshot` / `GetQueryPolicy` (proof = canonical JSON reply root over the committed readiness/activation records, the same shape as `ArtifactDispositionReadProofV1`) and barriers on the four existing `SafeState` reads; Raft tags 23 (`ActivateQueryProfile`, an authority purpose of its own), 26 (`PublishExecutorProfileTransition`, reusing `validatePublishTransition`'s quorum), 27 (`RecordSnapshotArtifactReady`) with `SourceClaims.RecordSnapshotArtifactReady` and a configured publisher set; capability-enabled refusal of the unscoped tag 26/27 paths (addendum line 474); the `ApplyArtifactDisposition` admission dependencies (`artifactDispositionAdmissionDeps`) so tag 28 becomes proposable; arbiter-core's reply/proof Go mirror and client; literal root vectors for all eleven actions; replacing `TestingActivateQueryPolicy` with the real transition path in the C2/C3 tests.
- **1a-3c — capacity charging.** Narrow `CountSnapshotEncoding`'s charge vector to the lifecycle dimensions (or add a non-lifecycle family), give the planner/settler their callers, flip the six `Capacity.Enabled` refusals in dependency order, bind a Q allowance at the C2 grant (`applyGrantReservationWithCapacityBinding`), route `SubmitSnapshotQuery` through `ApplyServerOwnedReservationQueryTransfer` and settle the allowance on abort, negative-delta accounting for retirement/resolution.
- `challenge_resolved` and `cleanup_settled` settlement records (C3's authenticated physical-write/cleanup settlement), binding `OpenChallenge` to a committed source claim (tag 24), continuation uses linked to a challenge obligation, and B4/B5's `HistoricalDecisionRecords` transport (needs reservation/block/statement-root/artifact-set fields beyond `GetQueryPolicy`).
- housegate's `newAuthenticatedHistoricalPolicy` export (B4, TODO T2.2) and the `HistoricalPolicy` adapter over arbiter's reads.

## Subsequent plans

| Plan | Scope | Written when |
|---|---|---|
| 1a-3b — C1 service surface | barrier reads, tags 23/26/27, publisher auth, admission dependencies, reply mirror, vectors | after this plan merges |
| 1a-3c — capacity charging | charge vector, C2 Q binding, submit/abort through the charged path | after 1a-3b |
| Wave 2 / 3 | source execution and the applied terminal (C5/B5); housegate sequencer client (T3.1) | per the TODO plan |

## Execution amendments (2026-09-22)

Recorded during subagent-driven execution (arbiter-proto #11 Task 2 first, arbiter-core #36 Tasks 1 + 2, arbiter #106 Task 3, arbiter #107 Task 4, arbiter #108 Task 5, arbiter #110 Task 6 docs, arbiter #109 follow-up). Each item is a ruling made against the plan text above; the merged code is authoritative where the two differ.

- **Execution order (Tasks 1–2).** arbiter-core's `conformance/arbiter_wire_test.go::TestArbiterMirrorsMatchProto` requires every json tag of `arbiter.ConsensusParamsUpdate` to name a field of the pinned proto message and vice versa, so neither the Go field nor the pin bump is green alone: the proto change landed first (arbiter-proto #11) and one arbiter-core commit carried the struct field, the range check, the converter in both directions and the pin bump (arbiter-core #36). The brief's "Task 1 then Task 2" is therefore "Task 2 Step 1, then Task 1 + Task 2 Step 2 as one PR".
- **Task 2 (arbiter-proto).** `conformance/snapshot_query_test.go::assertSnapshotBaselineDescriptors` freezes every baseline message byte-exactly (only `RaftCommand`/`VerifierDispatch` are append-only, and `PromotionAck` has one exact-field allowance). The two new fields got the same treatment: an exact `FieldDescriptorProto` allowance for `artifact_disposition_capability` (number 3 on `ConsensusMutableParams`, 8 on `ConsensusParamsUpdate`, `LABEL_OPTIONAL`, `TYPE_UINT32`) applied to every baseline call, the `.binpb` fixtures untouched; `TestConsensusUpdateContract` gained the 8th and 3rd entries.
- **Task 1 (arbiter-core).** `ConsensusParamsUpdateHash` marshals the whole normalized struct, so `omitempty` alone keeps every frozen digest; the review's one finding — `TestConsensusUpdateBindsEveryField` enumerates the fields by hand — was closed by adding the capability entry.
- **Task 3 (arbiter).** The brief's `capabilityUpdate` builder and direct `Params.AuthorityAddresses` writes were replaced by the existing `authorityFixture`/`mustNewFSM`/`consensusUpdate`/`signedConsensusUpdate` fixtures; `bindPolicyCommand()` returns `wire.Command`, so the tests go through `applyCmd`. Three fixtures needed more than mechanical substitution because a `Capability = 1` container downgraded to a pre-v13 version is exactly what the new gate refuses: `TestV5V6SnapshotsRetainDispositionAndDropCapacity` dropped its capability angle (covered by the new monotonic test), `TestV7SnapshotDropsQueryPolicyReadinessProjection` resets the three capability fields before its v7 downgrade, `TestV3V4SnapshotsRestoreDispositionDisabled` keeps one raw write because v3/v4 never assign the disposition document; `cmd/arbiter/storage_protocol_test.go`'s "current version" literal moved to 13. The review found the genesis path unmirrored (`genesis.artifact_disposition_capability: 1` passed validation and then failed `ConfigureGenesis` with "capability 0 disagrees with the consensus parameter 1"): the mirror now also runs once in `NewWithNotify`, deliberately not in `newState`, which `readSnapshot` shares — a restored container must still fail the divergence check — with a boot-and-round-trip test, two divergence cases pinned to the validator rather than the gate, and a narrowed gate comment (only v5–v12 documents can carry the disposition).
- **Task 4 (arbiter).** `retirementHasOpenWork` shares `retirementUseActiveAt`/`retirementObligationOpenAt` with `populateRetirementReadView` instead of re-deriving them; the four retirement/use operation-target arms share two helpers. Brief case (d) "a second finish → revision conflict" asserts "retirement is already retired" because the brief's own handler ordering puts that arm first; the finish fixture authors the candidate obligation as `resolved` because no public command resolves it before Task 5. The review found `ClosedIndex` unbounded at restore — the brief's per-use enumeration omitted the plan's "`ClosedIndex`/`RetiredIndex` ≤ `LastAppliedIndex`" — closed in both terminal-use arms with tampered-container cases, plus a forged-`Pin` case for the target-mismatch arm; the safe-tip check was judged sufficient because it equals the first clause of `hasCommittedCurrentSuccessor` and the ancestry proof already ran at cut time.
- **Task 5 (arbiter).** The brief's `applyOpenChallenge` collides with the block-level fraud-challenge handler in `fsm/apply.go`; the disposition handler is `applyOpenDispositionChallenge`. `ArtifactDispositionRead`'s operation switch and the server's `artifactDispositionOperationTarget` gained an `obligation` arm (the persisted `TargetKind` was otherwise unreadable); restore also refuses `SourceIndex == 0` and an obligation filed under a sequence other than its own; `retirementAtPinLocked` is a free function returning `(record, ambiguous)`, and an ambiguous pin refuses the challenge ("open challenge pin has ambiguous retirement records") instead of silently skipping the post-cut rule — the post-cut cases author the use directly because `AdmitUse` admits only while the retirement is OPEN. The implementer's own observation became a ruling: settlement kinds are gated by the obligation's kind (`reservation_released` ⇔ `reservation`, `query_terminal` ⇔ `query`, `candidate_terminal` ⇔ `candidate`, every kind refused for a `challenge` obligation), because the `query` and `reservation` obligations of one request share a reservation id and the plan's design decision 3 reserves a query obligation for its admission's terminal. The server maps `OpenChallenge` to `verifier`; the FSM keeps the brief's role supersets (`source|verifier` for `CloseUse`, `source|coordinator` for `ResolveObligation`) because the role is validator-selected.
- **Task 6 (docs).** The docs review verified about one hundred and fifty identifiers, messages, proto fields and JSON tags against the code and the pinned modules with no error; its three minor notes (a non-literal backticked map expression, whole-paragraph rewrapping of the touched paragraphs, two counts carried from the plan's deferred list) were closed in the fix round together with the follow-up's semantics and three additions from the whole-branch review: `genesis.artifact_disposition_capability` is bootstrap-only (adding it after a runtime enable makes every node refuse to restore against its configured genesis), an open challenge pins the snapshot's retirement open until challenge settlement ships, and `RegistryClosedRevision` is recorded from the closing command rather than derived from committed evidence. The consensus-parameter spec enumerates the mutable parameters and now lists the capability with its monotone rule and range.
- **Final whole-branch review (arbiter #109).** Two seams no per-task review could see: the `candidate_terminal` settlement bound `obligation.Origin.CandidateSeq` — a caller-supplied field on `RegisterCandidate` — to the FSM-allocated candidate sequence, which no read exposes, so every naturally registered candidate's obligation was unsettleable and `FinishRetirement` unreachable through public commands (the plan's own Task 5 sketch is the source; the fixtures self-named or authored the obligation); and `OpenChallenge` verified only the attestation's authorship, so any active verifier with an admitted use could block a pin's retirement forever with any signed mismatch receipt. The follow-up binds the settlement through the candidate's `ObligationSeq` back-pointer, binds the challenge locally to the pin and the replay use's query identity (binding to a committed source claim stays deferred), adds the end-to-end lifecycle test through public commands only, range-checks the capability in `Params.Validate`, binds `ClientAccount` on the `query_terminal` arm and hardens the obligation restore rules (replacement children, `AdmittedIndex` bound, parent only on challenges).
- **Carried to later slices.** Binding a challenge obligation's `Origin` to the replay use's origin and the receipt's `StatementRoot`/`InputRoot`/`ReservationID` (and to a committed source claim once tag 24 has a path); constants for origin kinds (the origin-kind check reuses `ArtifactObligationKindQuery` today); `challenge_resolved` and `cleanup_settled` settlement records; a CLI-level test for `--artifact-disposition-capability`; the 1a-3b and 1a-3c scopes as listed under Deferred.
