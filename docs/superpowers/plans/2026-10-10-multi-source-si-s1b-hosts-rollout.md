# Multi-source SI network — Plan S1-B: hosts and devnet2 rollout of stage 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Put the stage-1 consensus layer (Plan S1-A) to work on real hosts: HouseGate answers the new `SOURCE_UNAVAILABLE` refusal correctly, sentio-node runs a signing, owner-scoped SNode that can start before its indexer is enrolled and reports owner-aware table state, production renders a second SI indexer, and devnet2 is upgraded, activated and enrols indexer-b.

**Architecture:** housegate maps arbiter admission code 10 to a retryable, session-preserving, client_seq-unspent refusal (Task 13). sentio-node wires the indexer key as the SNode claim signer together with its on-chain indexer id, accepts an empty genesis table set with a configured genesis snapshot id, defers SNode registration until its enrolment commits, signs the enrolment statement from a CLI, and answers table state and `sentio_getStorageIntegrityInfo` from its own enrolment and ownership (Tasks 14–15). production lets the network release carry more than one writer, renders indexer-b's SI overlay and source ClickHouse, and documents the stage-1 runbook (Task 16). The live rollout replays the voters' logs offline, upgrades verifiers then voters, upgrades indexer-a, commits the signed-claims activation, prepares and enrols indexer-b, and runs the end-to-end check (Task 17).

**Tech Stack:** Go 1.26 + Bazel 9.1.0/Bzlmod (housegate, sentio-node); Helm charts with `test_render.py`, helmfile and the Altinity ClickHouse operator (production); `kubectl`, `arbiter-admin` and the devnet2 runbook helpers (live steps).

**Spec:** housegate `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` (main `534a8b4`) — binding: §9, §10, §11, §12, §13 stage 1, §15–§16, plus the host parts of §6. **Prerequisite:** Plan S1-A (`2026-10-10-multi-source-si-s1a-consensus.md`) — its Global Constraints apply here unchanged, and Tasks 13–17 consume its release ledger (`PROTO_TAG`, `CORE_TAG`, `ARBITER_TAG` / `ARBITER_DIGEST`) and its exported names verbatim.

## Global Constraints

- Every Global Constraint of Plan S1-A applies (activation model, strict-decoder rule, request-only signatures, indexer id 0, feature string, signing domains, consensus/registry/admission/purge/routing rules, `registration_seq`, golden files, worktrees, commits, remote actions).
- **Client refusal for code 10 (verbatim):** ClickHouse code 733 with message `storage_integrity: the source of table <table_id> is not active yet; retry shortly (retryable)`, `KeepSession`, client_seq marked unspent; the deferred lane recognises the exact message.
- **sentio-node config:** new key `storage_integrity.snode.genesis_snapshot_id` (lowercase `0x` + 64 hex), required when `storage_integrity.snode.table_ids` is empty and set on every SI indexer (the enrolment CLI requires it); an empty `table_ids` requires `schema_root = 0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59`; `table_ids` lists only genesis tables this indexer owns. devnet2-si-v2 genesis snapshot id: `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06`.
- **SNode identity:** `snode.Config.IndexerID` is the on-chain indexer id sentio-node already resolves (`self_indexer_id`); `snode.Deps.ClaimSigner` is built from the indexer private key sentio-node already loads (`PRIVATE_KEY`) — never a new secret; both are always set together. The SNode's node id equals its `si_indexers` entry's `snode_node_id` and `housegate.storage_integrity.runtime.expected_source`.
- **Enrolment CLI (verbatim):** `sentio-node --config-path <config> storage-integrity enrollment-statement` prints only the compact JWS on stdout (logs on stderr) and never contacts the arbiter.
- **Deferred registration (verbatim log lines):** `storage-integrity SNode registration deferred: this indexer is not enrolled in the arbiter's si_indexers yet; its tables answer ordinary until the enrolment commits` and `storage-integrity SNode registered after its enrolment committed`.
- **Ordering:** every SI host image carries `HG_TAG` before the activation update commits; indexer-b's SI overlay is applied before its activation block.
- **Pins:** housegate pins arbiter-proto in `go.mod` only; sentio-node pins arbiter-core and housegate three times each (`go.mod` require, `MODULE.bazel` `bazel_dep` version, `git_override` commit verified with `git -C <dep> rev-parse <tag>^{commit}`) and arbiter-proto in `go.mod` only.
- **production rule:** chart and runbook changes go through a production PR; single-service values changes (image digests, one overlay value) go directly to `main` after the PR merges. Every live mutation (`helmfile sync`, pod deletion, `arbiter-admin consensus update`, pushes to production `main`) requires explicit user confirmation in chat; the activation update is irreversible.

## Release ledger (consumed and produced)

| Name | Meaning | Resolved by |
|---|---|---|
| `PROTO_TAG`, `CORE_TAG`, `ARBITER_TAG` / `ARBITER_DIGEST` | Plan S1-A releases | Plan S1-A |
| `HG_TAG` | housegate release containing Task 13 | Task 13, release step |
| `SN_COMMIT` / `SN_DIGEST` | sentio-node main commit containing Tasks 14–15 and its image index digest | Task 15, release step |

References written `contract §N` or `CONTRACT §N` point to Appendix A of Plan S1-A (the binding cross-task contract); references to "Contract conflicts" or "Facts" of a task group point to Appendix B of the plan that contains that task.

## Review Focus

1. **indexer-b runs its SI overlay before its enrolment commits** (the normal rollout order). Expected: it starts, every table answers `ordinary`, the SNode registration is deferred without rolling back the startup, and once the enrolment commits the SNode registers without a restart; an enrolment naming another node id or signer stops the deferred task and the next start refuses. Tests: Task 14 `TestStorageIntegrityDeferredRegistrationWaitsForTheEnrollment`, `TestStorageIntegrityListenerPermitDefersRegistrationBeforeEnrollment`, `TestStorageIntegrityDeferredRegistrationStopsOnAMismatchedEnrollment`, `TestStorageIntegrityListenerPermitRollsBackAMismatchedEnrollment`; Task 15 `TestLookupIdleNonEnrolledHostIsOrdinaryEverywhere`.
2. **A client hits `SOURCE_UNAVAILABLE` on the staged or the deferred lane.** Expected: a retryable 733 refusal, the session stays usable for the next query, the client_seq is unspent, the prepared parts are removed. Hosts still running an older HouseGate would classify code 10 as unknown, so every SI host must carry `HG_TAG` before the activation. Tests: Task 13 `TestSourceUnavailableReachesTheClientAsARetryableRefusal`, `TestRelay_StagedSourceUnavailableRejection_KeepsSessionAndServesNextQuery`, `TestRelay_DeferredUpstreamSourceUnavailable_KeepsSessionAndServesNextQuery`; Task 17's image guard before the activation and before the enrolment.
3. **A wrong genesis snapshot id is configured.** Expected: on an SNode with genesis tables startup refuses the mismatch with both values named; without genesis tables the wrong id surfaces when the arbiter refuses the enrolment statement, and the CLI refuses before any read when the id is missing. Tests: Task 14 `TestCheckStorageIntegrityGenesisSnapshotID`, `TestPrintStorageIntegrityEnrollmentStatementRefusesBeforeAnyRead`; Plan S1-A Task 5 enrolment-statement refusal.
4. **The same registry seen from each host.** Expected: a host answers from the registry only for tables it owns (a nil owner is the founding indexer, never confused with `&0`), other SI indexers' tables are `ordinary`, and each SNode lists only genesis tables its indexer owns. Tests: Task 15 `TestLookupTwoIndexerRegistryFromEachHost`, `TestLookupNilOwnerIsTheFoundingIndexer`; Task 16 `test_each_snode_lists_only_the_genesis_tables_its_indexer_owns`, `test_si_indexers_never_share_a_source_clickhouse`.
5. **Schema declarations under the wrong network id, or a verifier losing its registration state.** Expected: an SI node whose declarer would hash under another network id refuses startup; verifiers keep `registration.json` on a per-verifier persistent volume that survives restarts and the one-time StatefulSet recreation. Tests: Task 15 `TestCheckStorageIntegritySchemaNetworkID`, `TestRunChecksTheSchemaDeclarerNetworkID`; Task 16 `test_enabled_state_is_one_persistent_volume_per_verifier`, `test_devnet2_verifier_state_follows_the_rollout`.

## Task order and dependencies

Plan S1-A complete (`PROTO_TAG`, `CORE_TAG`, `ARBITER_TAG` / `ARBITER_DIGEST` recorded) → 13 (housegate, `HG_TAG`) → 14 → 15 (sentio-node, `SN_COMMIT` / `SN_DIGEST`) → 16 (production PR) → 17 (devnet2 live). Task 13 may start as soon as `PROTO_TAG` exists.

## File Structure

**housegate** (Task 13): `go.mod`, `go.sum` (arbiter-proto pin); `pkg/storageintegrity/arbiter_proto.go` (code 10 as a coded terminal reject); `pkg/chproto/client_error.go` (`SourceUnavailableMessage`, `IsSourceUnavailableMessage`); `storage_integrity_ingress.go` (733 + `KeepSession` + unspent); `pkg/proxy/relay.go` (deferred lane recognises the message); `pkg/sitable/sitable.go`, `README.md`, `CLAUDE.md` (docs); tests beside each.

**sentio-node** (Tasks 14–15): `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (pins); `config/config.go` (`genesis_snapshot_id`, empty-genesis rules); `standalone/standalone.go`, `standalone/storage_integrity_genesis.go`, `standalone/storage_integrity_contract_snapshot.go`, `standalone/storage_integrity_enrollment.go` (new, deferred registration), `standalone/storage_integrity_enrollment_statement.go` (new), `commands/storage_integrity.go` (new CLI), `main.go`; `storageintegrityadapter/tablestate/model.go`, `arbitercore.go`, `snapshot.go` (owner-aware state); `standalone/storage_integrity_table_state.go`, `standalone/storage_integrity_info.go`, `rpc/storage_integrity_info.go` (`enrolled`); `standalone/schema_registry.go` (network-id check); `README.md`.

**production** (Task 16): `charts/sentio-node/templates/node.yaml`, `charts/sentio-node/values.yaml` (`genesisSnapshotID`, empty `tableIDs` guard); `charts/storage-integrity/templates/verifier.yaml`, `templates/validate.yaml`, `values.yaml`, `values.schema.json` (`verifier.state` PVC, bootstrap `maxWriters` documentation); `charts/storage-integrity/tests/test_render.py`, `tests/validate_source_binding.py`; `k8s-sea/sentio-network-devnet2/` indexer-b overlay, source-image pin, dedicated source CHI and helmfile release, `genesisSnapshotID` on indexer-a's overlay; `.github/workflows/storage-integrity-chart.yml`; `docs/storage-integrity-devnet2-multi-source.md` (new runbook), `docs/storage-integrity-devnet2-dynamic-table-set.md` (stage table).

**devnet2** (Task 17): no repository files besides the single-service values commits the runbook prescribes; local evidence under `$HOME/si-devnet2-multisource*`.

## Spec coverage map

| Spec item | Tasks |
|---|---|
| §9 housegate: route A unchanged, code-10 mapping, `sipeerguard` on every SI host, `sitable` docs | 13 (mapping, docs); `sipeerguard` is enabled by the SI overlay (16) |
| §10 sentio-node: configuration, governance predicate, enrolment CLI and deferred start, RPC, schema network id | 14, 15 |
| §11 agent unchanged; driver sidecars configured like indexer-a's | 16 (indexer-b images), 17 |
| §12 production: source ClickHouse per indexer, overlay generalisation, render tests, node ids | 16 |
| §13 stage-1 rollout steps 1–8 | 17 |
| §15 sentio-node and production tests | 14, 15, 16 |
| §16 stage-1 coupling risk | 16 (runbook), 17 |

## Follow-ups (not in this plan)

- Expose each registered node's `ed25519_pubkey` and `registration_seq` in `NodeFeatureEntry` (arbiter-proto `NodeFeatureEntry.ed25519_pubkey = 4`, `registration_seq = 5`) and print them in `arbiter-admin consensus features`, so the activation's verifier list can be read from the leader instead of derived from the seeds (Task 17 derives them; a wrong key is refused atomically by the FSM).
- Stage 2 (tracks, quarantine) — spec §7 — gets its own plans.

---

## Task 13: housegate — SOURCE_UNAVAILABLE refusal and table-state docs, release HG_TAG (S1)

Spec §6.4 and §9: after the signed-claims activation the arbiter answers `ADMISSION_CODE_SOURCE_UNAVAILABLE` (code 10) when the owner's SNode of the target table is not registered and Active, with FSM state unchanged (CONTRACT §3 "Admission", checked before `SpentIDs.Insert`). HouseGate must treat it as a coded terminal reject (so the existing terminal-submit-rejection path removes the prepared parts and the coordinate is marked unspent, exactly as `LANE_BUDGET_EXCEEDED` / `GAP_BUDGET_EXCEEDED` are classified today) and surface it like the other session-preserving storage-integrity refusals (`SCHEMA_NOT_ALLOWED` → 392 and the merge-latch 733): a `chproto.ClientError` with `KeepSession` and `SeqUnspent`, code 733 (`chproto.CodeTableIsBeingRestarted`, ClickHouse `TABLE_IS_BEING_RESTARTED`, `pkg/chproto/client_error.go:23`), and an exact message the relay recognises on the agent's deferred lane. Before activation the arbiter never sends code 10, so nothing changes; a housegate still on arbiter-proto `v0.9.0` would map code 10 to `OutcomeUnknown` (`pkg/storageintegrity/arbiter_proto.go:409-410`), which is why `HG_TAG` must be in every SI host image before the activation update.

**Files:**
- Modify: `go.mod` (line 15 `github.com/sentioxyz/arbiter-proto v0.9.0`), `go.sum`. `MODULE.bazel` does not pin arbiter-proto (it resolves through `go_deps.from_file`, `use_repo` entry `com_github_sentioxyz_arbiter_proto` at `MODULE.bazel:24`), so neither it nor `MODULE.bazel.lock` changes.
- Modify: `pkg/storageintegrity/arbiter_proto.go` (admission-code vars `:367-385`; terminal-reject case list of `SubmitOutcomeFromSequencedAck` `:400-408`), `pkg/storageintegrity/admission_code_test.go` (append after `:48`)
- Modify: `pkg/chproto/client_error.go` (session-preserving refusal messages `:30-79`), `pkg/chproto/client_error_test.go` (append)
- Modify: `storage_integrity_ingress.go` (`ConsumeStorageIntegrityAdmission`, the `!res.Ack2` block `:824-848`), `storage_integrity_ingress_unspent_test.go` (`TestConsumeAdmission_PostOrchestrateMarking` table `:89-104`), `storage_integrity_table_state_runtime_test.go` (append after `TestSchemaNotAllowedReachesTheClientAsNoLongerAcceptsWrites`, `:108-126`)
- Modify: `pkg/proxy/relay.go` (`isSessionPreservingIngressException` `:2297-2325`), `pkg/proxy/relay_reject_test.go` (`:157-168`, `:378-397`, `TestSessionPreservingIngressException` `:479-512`), `pkg/proxy/relay_unspent_test.go` (`TestSessionPreservingIngressException_AcceptsMarker` `:68-78`)
- Modify: `pkg/sitable/sitable.go` (status docs `:19-23`)
- Modify: `README.md` (`:195`, "Dynamic table state"), `CLAUDE.md` (pipeline `KeepSession` lines `:108-114`, `pkg/sitable` bullet `:160`, `pkg/storageintegrity` bullet `:174`)

**Interfaces:**
- Consumes: Task 1 `PROTO_TAG` with `pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE` (= 10, `.String()` = `"ADMISSION_CODE_SOURCE_UNAVAILABLE"`). Existing: `SubmitOutcome{Category, Reason, AdmissionCode}`, `OutcomeTerminalReject` (`RequiresAbort()` → `Orchestrator.abort` → `AbortPreparedStatement`, `pkg/storageintegrity/intake.go:1140-1143`, `:1298-1341`), `chproto.ClientError{Code, Message, Err, KeepSession, SeqUnspent}`, `chproto.CodeTableIsBeingRestarted`, `chproto.TrimSeqUnspentSuffix`, `exceptionForPluginError` (renders the ` [client_seq unspent]` suffix once), test helpers `newBackpressureIngress`, `bpAdmission`, `bpPreparedCandidates`, `rootRecordingPreparer.abortFn` / `.abortCalls`, `testStagedSessionPreservingRejection`, `testDeferredUpstreamSessionPreservingRejection`.
- Produces:
  - `var storageintegrity.AdmissionCodeSourceUnavailable = pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE.String()`; `SubmitOutcomeFromSequencedAck` maps code 10 to `SubmitOutcome{Category: OutcomeTerminalReject, AdmissionCode: AdmissionCodeSourceUnavailable, Reason: <arbiter message>}`.
  - `func chproto.SourceUnavailableMessage(tableID string) string`, `func chproto.IsSourceUnavailableMessage(message string) bool`; unexported `sourceUnavailablePrefix`, `sourceUnavailableSuffix`, `isRefusalMessage(message, prefix, suffix string) bool` (the old `isTableRefusalMessage` delegates to it).
  - Client refusal (verbatim): code 733, message `storage_integrity: the source of table <table_id> is not active yet; retry shortly (retryable)`, `KeepSession: true`, `SeqUnspent: true`, `Err` = `arbiter ADMISSION_CODE_SOURCE_UNAVAILABLE: <arbiter reason>` (server-side only), warn log `storage_integrity statement refused: the table's source is not active`.
  - `isSessionPreservingIngressException` accepts code 733 with that exact message (with or without the unspent suffix).
  - `pkg/sitable` doc: `Ordinary` also means "owned by another SI indexer".
  - Release `HG_TAG`.

- [ ] **Step 1: Worktree and base check**

Create the worktree with the URWT command of Task 1 Step 1, substituting the repository `/Users/uranuswch/Dev/housegate/housegate` (path `~/src/remotesrc/claude/housegate/multi-source-s1`, branch `urwt/claude/multi-source-s1`). Then:

Run: `W=<worktree path>; PROTO_TAG=<ledger>; git -C "$W" log --oneline -1 && git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-proto fetch -q --tags && git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-proto show "$PROTO_TAG":proto/arbiter.proto | grep -c 'ADMISSION_CODE_SOURCE_UNAVAILABLE = 10;'`
Expected: HEAD is `534a8b4` or a descendant (code identical to `2959e4a`), and `1`. If `PROTO_TAG` is not in the ledger yet, stop: Task 13 depends on Task 1 only.

- [ ] **Step 2: Pin arbiter-proto `PROTO_TAG`** (recipe `.claude/skills/upgrade-dependency/SKILL.md`, Steps 2–3)

```bash
W=<worktree path>; PROTO_TAG=<ledger>
cd "$W" && go get "github.com/sentioxyz/arbiter-proto@$PROTO_TAG" && go mod tidy && bazel mod tidy && bazel run //:gazelle
git -C "$W" status --short
```

Expected: only `go.mod` and `go.sum` changed (the `require` line reads `github.com/sentioxyz/arbiter-proto <PROTO_TAG>`); no `MODULE.bazel*` or `BUILD.bazel` diff.

Run: `cd "$W" && bazel build //... && bazel test //pkg/storageintegrity:storageintegrity_test //pkg/chproto:chproto_test //pkg/proxy:proxy_test //:housegate_test`
Expected: PASS (the proto change is additive).

```bash
cd "$W" && git add go.mod go.sum && git commit -m "chore(deps): pin arbiter-proto $PROTO_TAG

The multi-source SI release of arbiter-proto adds
ADMISSION_CODE_SOURCE_UNAVAILABLE and the signed-claims fields
(housegate spec 2026-10-10 §6); additive only.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 3: Write the failing tests for the outcome and the message**

Append to `pkg/storageintegrity/admission_code_test.go`:

```go
// TestSourceUnavailableIsATerminalRejectWithItsCode pins spec 2026-10-10 §6.4:
// the arbiter changed nothing, so the outcome is a coded terminal reject. Its
// category removes the prepared parts through the terminal-submit path, and
// its code tells the ingress which client refusal to send.
func TestSourceUnavailableIsATerminalRejectWithItsCode(t *testing.T) {
	const reason = "storage-integrity source snode-b of indexer 1 is not active"
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE, Message: reason})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeSourceUnavailable ||
		got.AdmissionCode != "ADMISSION_CODE_SOURCE_UNAVAILABLE" || got.Reason != reason {
		t.Fatalf("outcome = %+v", got)
	}
	if !got.Category.RequiresAbort() {
		t.Fatal("a source-unavailable refusal must remove the prepared parts through the terminal-submit path")
	}
}
```

Append to `pkg/chproto/client_error_test.go` (same style as `TestTableActivatingMessage`, `:44-67`):

```go
// TestSourceUnavailableMessage pins the exact spec 2026-10-10 §6.4 / §9 text;
// the relay's deferred-lane classifier depends on it.
func TestSourceUnavailableMessage(t *testing.T) {
	if got := SourceUnavailableMessage("net1.events"); got != "storage_integrity: the source of table net1.events is not active yet; retry shortly (retryable)" {
		t.Fatalf("SourceUnavailableMessage = %q", got)
	}
	if !IsSourceUnavailableMessage(SourceUnavailableMessage("net1.events")) {
		t.Fatal("IsSourceUnavailableMessage rejected its own message")
	}
	if !IsSourceUnavailableMessage(SourceUnavailableMessage("net1.events") + SeqUnspentSuffix) {
		t.Fatal("IsSourceUnavailableMessage must accept the marker suffix")
	}
	for _, msg := range []string{
		SourceUnavailableMessage(""),
		SourceUnavailableMessage("net1 events"),
		SourceUnavailableMessage("net1.events\t"),
		TableActivatingMessage("net1.events"),
		TableNoLongerAcceptsWritesMessage("net1.events"),
		"storage_integrity: the source of table net1.events is not active yet",
	} {
		if IsSourceUnavailableMessage(msg) {
			t.Fatalf("IsSourceUnavailableMessage(%q) = true, want false", msg)
		}
	}
	if IsTableActivatingMessage(SourceUnavailableMessage("net1.events")) || IsTableNoLongerAcceptsWritesMessage(SourceUnavailableMessage("net1.events")) {
		t.Fatal("the source-unavailable refusal must not match the table refusals")
	}
}
```

Run: `cd "$W" && go test ./pkg/storageintegrity/ -run TestSourceUnavailableIsATerminalRejectWithItsCode -v; go test ./pkg/chproto/ -run TestSourceUnavailableMessage -v`
Expected: both FAIL to compile — `undefined: AdmissionCodeSourceUnavailable` and `undefined: SourceUnavailableMessage` / `IsSourceUnavailableMessage`.

- [ ] **Step 4: Implement the outcome mapping and the message**

In `pkg/storageintegrity/arbiter_proto.go`, after `AdmissionCodeGapBudgetExceeded` (`:382-385`) add:

```go
// AdmissionCodeSourceUnavailable is the arbiter's refusal of a statement whose
// table's owning indexer has no registered, Active SNode yet (spec 2026-10-10
// §6.4). The FSM changed nothing, so the coordinate is unspent; the ingress
// removes the prepared parts through the terminal-submit path and answers the
// client with a retryable, session-preserving refusal.
var AdmissionCodeSourceUnavailable = pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE.String()
```

and extend the terminal-reject case of `SubmitOutcomeFromSequencedAck` (`:400-408`) so its last two lines read:

```go
		pb.AdmissionCode_ADMISSION_CODE_GAP_BUDGET_EXCEEDED,
		pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED,
		pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE:
		return SubmitOutcome{Category: OutcomeTerminalReject, Reason: firstNonEmpty(reason, ack.GetCode().String()), AdmissionCode: ack.GetCode().String()}
```

In `pkg/chproto/client_error.go` extend the comment list above the constants (`:30-40`) with

```go
//   - Source unavailable (spec 2026-10-10 §6.4, §9): the arbiter refused the
//     statement because its table's owning indexer has no registered, Active
//     SNode yet; refused retryably with CodeTableIsBeingRestarted.
```

replace the constant block and the helpers (`:41-79`) with:

```go
const (
	tableRefusalPrefix         = "storage_integrity: table "
	tableActivatingSuffix      = " is being activated; retry shortly (retryable)"
	tableNoLongerAcceptsSuffix = " no longer accepts writes"
	sourceUnavailablePrefix    = "storage_integrity: the source of table "
	sourceUnavailableSuffix    = " is not active yet; retry shortly (retryable)"
)

// TableActivatingMessage is the client-facing message of the retryable
// table-activation refusal for tableID.
func TableActivatingMessage(tableID string) string {
	return tableRefusalPrefix + tableID + tableActivatingSuffix
}

// IsTableActivatingMessage reports whether message is exactly a
// table-activation refusal for a non-empty, whitespace-free table id.
func IsTableActivatingMessage(message string) bool {
	return isTableRefusalMessage(message, tableActivatingSuffix)
}

// TableNoLongerAcceptsWritesMessage is the client-facing message of the
// non-retryable spec §9.6 refusal for tableID.
func TableNoLongerAcceptsWritesMessage(tableID string) string {
	return tableRefusalPrefix + tableID + tableNoLongerAcceptsSuffix
}

// IsTableNoLongerAcceptsWritesMessage reports whether message is exactly a
// §9.6 refusal for a non-empty, whitespace-free table id.
func IsTableNoLongerAcceptsWritesMessage(message string) bool {
	return isTableRefusalMessage(message, tableNoLongerAcceptsSuffix)
}

// SourceUnavailableMessage is the client-facing message of the retryable
// refusal of a statement whose table's source SNode is not Active yet (the
// arbiter's SOURCE_UNAVAILABLE, spec 2026-10-10 §6.4, §9).
func SourceUnavailableMessage(tableID string) string {
	return sourceUnavailablePrefix + tableID + sourceUnavailableSuffix
}

// IsSourceUnavailableMessage reports whether message is exactly a
// source-unavailable refusal for a non-empty, whitespace-free table id.
func IsSourceUnavailableMessage(message string) bool {
	return isRefusalMessage(message, sourceUnavailablePrefix, sourceUnavailableSuffix)
}

func isTableRefusalMessage(message, suffix string) bool {
	return isRefusalMessage(message, tableRefusalPrefix, suffix)
}

func isRefusalMessage(message, prefix, suffix string) bool {
	message = TrimSeqUnspentSuffix(message)
	if !strings.HasPrefix(message, prefix) || !strings.HasSuffix(message, suffix) ||
		len(message) <= len(prefix)+len(suffix) {
		return false
	}
	id := message[len(prefix) : len(message)-len(suffix)]
	return strings.IndexFunc(id, unicode.IsSpace) < 0
}
```

Run: `cd "$W" && go test ./pkg/storageintegrity/ ./pkg/chproto/ -run 'SourceUnavailable|TableActivating|NoLongerAcceptsWrites|SeqUnspent|SubmitOutcome|LaneBudget' -v`
Expected: PASS, including the unchanged `TestTableActivatingMessage`, `TestTableNoLongerAcceptsWritesMessage` and `TestSeqUnspentSuffixHelpers`.

- [ ] **Step 5: Write the failing ingress and relay tests**

In `storage_integrity_ingress_unspent_test.go`, add this row to the `TestConsumeAdmission_PostOrchestrateMarking` table (`:94-103`):

```go
		"source unavailable": {outcome: sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject, Reason: "storage-integrity source snode-b of indexer 1 is not active", AdmissionCode: sicore.AdmissionCodeSourceUnavailable}, wantMarked: true, wantText: "the source of table net1.events is not active yet"},
```

Append to `storage_integrity_table_state_runtime_test.go`, after `TestSchemaNotAllowedReachesTheClientAsNoLongerAcceptsWrites` (add `"slices"` to its imports):

```go
// TestSourceUnavailableReachesTheClientAsARetryableRefusal is spec 2026-10-10
// §6.4 / §9: the arbiter refused admission because the owner's SNode of the
// table is not registered and Active. The refusal is the retryable 733, ends
// only the query, leaves the coordinate unspent, keeps the arbiter's code and
// reason server-side, and the terminal-submit path removed the prepared parts.
func TestSourceUnavailableReachesTheClientAsARetryableRefusal(t *testing.T) {
	ingress, _, submitter, preparer := newBackpressureIngress(t, &fakePartsPressure{})
	var aborted []sicore.CandidatePart
	preparer.abortFn = func(parts []sicore.CandidatePart) { aborted = parts }
	submitter.outcome = sicore.SubmitOutcome{Category: sicore.OutcomeTerminalReject,
		Reason: "storage-integrity source snode-b of indexer 1 is not active", AdmissionCode: sicore.AdmissionCodeSourceUnavailable}
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var ce *chproto.ClientError
	if !errors.As(err, &ce) || ce.Code != chproto.CodeTableIsBeingRestarted ||
		ce.Message != "storage_integrity: the source of table net1.events is not active yet; retry shortly (retryable)" {
		t.Fatalf("err = %v, want the retryable source-unavailable refusal", err)
	}
	if !ce.KeepSession || !chproto.KeepsSession(fmt.Errorf("wrapped by the plugin: %w", err)) {
		t.Fatalf("err = %+v, want KeepSession through the plugin's wrapping", ce)
	}
	if !chproto.IsSeqUnspent(err) {
		t.Fatalf("err = %v, want the coordinate marked unspent: the arbiter changed nothing", err)
	}
	if ce.Err == nil || ce.Err.Error() != "arbiter ADMISSION_CODE_SOURCE_UNAVAILABLE: storage-integrity source snode-b of indexer 1 is not active" {
		t.Fatalf("Err = %v, want the arbiter code and reason", ce.Err)
	}
	if preparer.abortCalls != 1 || !slices.Equal(aborted, bpPreparedCandidates()) {
		t.Fatalf("abort calls = %d with %v, want the prepared candidates removed exactly once", preparer.abortCalls, aborted)
	}
}
```

In `pkg/proxy/relay_reject_test.go`, after `TestRelay_StagedNoLongerAcceptsWritesRejection_KeepsSessionAndServesNextQuery` (`:165-168`) add:

```go
// Spec 2026-10-10 §6.4, §9: a statement whose table's owning indexer has no
// Active SNode yet is refused retryably with 733, and the session survives.
func TestRelay_StagedSourceUnavailableRejection_KeepsSessionAndServesNextQuery(t *testing.T) {
	testStagedSessionPreservingRejection(t, chproto.CodeTableIsBeingRestarted, chproto.SourceUnavailableMessage("db1.t"))
}
```

after `TestRelay_DeferredUpstreamNoLongerAcceptsWrites_KeepsSessionAndServesNextQuery` (`:389-397`) add:

```go
// The server-mode Housegate answers a statement whose table's source is not
// Active yet with the session-preserving 733 after it consumed the complete
// staged input; the agent must keep its session too.
func TestRelay_DeferredUpstreamSourceUnavailable_KeepsSessionAndServesNextQuery(t *testing.T) {
	testDeferredUpstreamSessionPreservingRejection(t, &chproto.Exception{
		Code:    proto.Error(chproto.CodeTableIsBeingRestarted),
		Name:    "DB::Exception",
		Message: chproto.SourceUnavailableMessage("db1.t"),
	})
}
```

and add these rows to the `TestSessionPreservingIngressException` table (`:485-502`):

```go
		{"source unavailable", chproto.CodeTableIsBeingRestarted, chproto.SourceUnavailableMessage("net1.events"), true},
		{"source unavailable under 392", chproto.CodeQueryIsProhibited, chproto.SourceUnavailableMessage("net1.events"), false},
		{"source unavailable under 252", chproto.CodeTooManyParts, chproto.SourceUnavailableMessage("net1.events"), false},
		{"source unavailable without a table id", chproto.CodeTableIsBeingRestarted, chproto.SourceUnavailableMessage(""), false},
```

In `pkg/proxy/relay_unspent_test.go` add to the `TestSessionPreservingIngressException_AcceptsMarker` list (`:69-73`):

```go
		{Code: proto.Error(chproto.CodeTableIsBeingRestarted), Message: chproto.SourceUnavailableMessage("db1.t") + chproto.SeqUnspentSuffix},
```

Run: `cd "$W" && go test . -run 'TestSourceUnavailableReachesTheClientAsARetryableRefusal|TestConsumeAdmission_PostOrchestrateMarking' -v; go test ./pkg/proxy/ -run 'TestSessionPreservingIngressException|SourceUnavailable' -v`
Expected: FAIL — the root tests get the generic coded reject `storage_integrity: statement 0xabc:1:n1 rejected by the arbiter: ADMISSION_CODE_SOURCE_UNAVAILABLE` (no `ClientError`, so no 733 and no `KeepSession`); `isSessionPreservingIngressException(733, "storage_integrity: the source of table …") = false, want true`, the marker case fails, and `TestRelay_DeferredUpstreamSourceUnavailable_KeepsSessionAndServesNextQuery` fails because the deferred lane closes the session. `TestRelay_StagedSourceUnavailableRejection_KeepsSessionAndServesNextQuery` already passes (the staged lane honours `KeepSession` generically); it pins the staged half of the contract.

- [ ] **Step 6: Implement the ingress mapping and the relay recognition**

In `storage_integrity_ingress.go`, inside `if !res.Ack2 {`, right after the `AdmissionCodeSchemaNotAllowed` branch (`:825-836`) and before the generic coded-reject branch (`:837-846`), insert:

```go
		if res.Submit.AdmissionCode == sicore.AdmissionCodeSourceUnavailable {
			// The table's owning indexer has no registered, Active SNode yet
			// (spec 2026-10-10 §6.4, §9). The arbiter changed nothing, so the
			// coordinate is unspent, and the terminal-submit path has already
			// removed the prepared parts. The refusal is retryable and ends only
			// the query; the relay recognises the exact message on the wire.
			log.Warnw("storage_integrity statement refused: the table's source is not active",
				"statement_id", rec.StatementID, "table", rec.TableID, "reason", res.Submit.Reason)
			return &chproto.ClientError{Code: chproto.CodeTableIsBeingRestarted,
				Message:     chproto.SourceUnavailableMessage(rec.TableID),
				Err:         fmt.Errorf("arbiter %s: %s", res.Submit.AdmissionCode, res.Submit.Reason),
				KeepSession: true,
				SeqUnspent:  true}
		}
```

In `pkg/proxy/relay.go` change the 733 case of `isSessionPreservingIngressException` (`:2318-2319`) to

```go
	case chproto.CodeTableIsBeingRestarted:
		return chproto.IsTableActivatingMessage(message) || chproto.IsSourceUnavailableMessage(message)
```

and in its doc comment (`:2297-2308`) replace "code 733 with the exact table-activation message (a newly Active table whose merge latch is not asserted yet)," with "code 733 with the exact table-activation message (a newly Active table whose merge latch is not asserted yet) or the exact source-unavailable message (the arbiter's SOURCE_UNAVAILABLE: the table's owning indexer has no registered, Active SNode yet, spec 2026-10-10 §6.4),".

Run: `cd "$W" && go test . -run 'SourceUnavailable|PostOrchestrateMarking|SchemaNotAllowed' -v && go test ./pkg/proxy/ -run 'SessionPreserving|SourceUnavailable|TableActivating|NoLongerAcceptsWrites' -v`
Expected: PASS.

- [ ] **Step 7: Documentation**

In `pkg/sitable/sitable.go` the comments of `Ordinary` and `Pending` (`:20-23`) become (the other constants are unchanged):

```go
	// Ordinary is not governed on this host: a table another indexer hosts —
	// including one another SI indexer of the network owns (spec 2026-10-10
	// §9, §10) — a Legacy table, or any table while the registry is not
	// enabled. Its reads and writes are plain ClickHouse here; the owning
	// host, if any, governs it.
	Ordinary Status = iota
	// Pending includes default deny: a table of a database this host's
	// enrolled SI indexer hosts that the registry has not recorded.
	Pending
```

`README.md:195` (one paragraph, keep it on one line): replace "`Ordinary` (not governed; plain ClickHouse behavior)" with "`Ordinary` (not governed on this host — a table another indexer hosts, including one another SI indexer of the network owns; plain ClickHouse behavior)"; and in the retryable 733 list replace "`storage_integrity: table <id> is still being purged; retry CREATE later (retryable)` and `storage_integrity: table <id> requires a signed INSERT; the client's table state is stale (retryable)`" with "`storage_integrity: table <id> is still being purged; retry CREATE later (retryable)`, `storage_integrity: table <id> requires a signed INSERT; the client's table state is stale (retryable)` and `storage_integrity: the source of table <id> is not active yet; retry shortly (retryable)` (the arbiter's `SOURCE_UNAVAILABLE`: the table's owning indexer has no registered, Active SNode yet; spec 2026-10-10 §6.4)".

`CLAUDE.md`:
- Pipeline block (`:108-114`, a code block, so its wrapping stays): replace "back-pressure uses this for code 252, a table whose merge latch / is not asserted yet for code 733, and a table that retired before" with "back-pressure uses this for code 252, a table whose merge latch / is not asserted yet and a statement whose table's source SNode is / not Active yet (arbiter SOURCE_UNAVAILABLE) for code 733, and a / table that retired before", re-wrapped to the block's indentation.
- `pkg/sitable` bullet (`:160`): replace "`Ordinary` = Legacy or ungoverned" with "`Ordinary` = Legacy or not governed on this host, including a table another SI indexer of the network owns (spec 2026-10-10 §9)", and "A Purged name is reported unrecorded, i.e. Pending on the SI indexer." with "A Purged name is reported unrecorded, i.e. Pending on its owning SI indexer."
- `pkg/storageintegrity` bullet (`:174`): after the sentence that ends "…where `isSessionPreservingIngressException` recognises the exact message." insert: "An arbiter `ADMISSION_CODE_SOURCE_UNAVAILABLE` (spec 2026-10-10 §6.4: the table's owning indexer has no registered, Active SNode; the FSM changed nothing) is a coded terminal reject, so the terminal-submit path removes the prepared parts; it reaches the client as the retryable code-733 `storage_integrity: the source of table <id> is not active yet; retry shortly (retryable)` (`chproto.SourceUnavailableMessage`), marked unspent and carrying `KeepSession`, which `isSessionPreservingIngressException` also recognises on the agent's deferred lane."

- [ ] **Step 8: Full verification**

Run: `cd "$W" && bazel run //:gazelle && bazel build //... && bazel test //...`
Expected: all PASS. The integration targets are `manual`; nothing in them changes, and the PR's `Integration (ClickHouse)` job runs them.

- [ ] **Step 9: Commit**

```bash
cd "$W" && git add -A && git commit -m "feat(storageintegrity): map SOURCE_UNAVAILABLE to a retryable, session-preserving refusal

The arbiter refuses a statement whose table's owning indexer has no
registered, Active SNode with ADMISSION_CODE_SOURCE_UNAVAILABLE and
changes nothing (spec 2026-10-10 §6.4). It is a coded terminal reject,
so the terminal-submit path removes the prepared parts; the client sees
733 'storage_integrity: the source of table <id> is not active yet;
retry shortly (retryable)', marked unspent, and keeps its session on
the staged and the agent's deferred lane. sitable documents that
Ordinary also covers tables another SI indexer owns (§9).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 10: PR and merge (requires explicit user confirmation)**

Ask before each remote action. Then:

```bash
cd "$W" && git push -u origin urwt/claude/multi-source-s1
gh pr create --repo housegate/housegate --base main --head urwt/claude/multi-source-s1 \
  --title 'feat(storageintegrity): SOURCE_UNAVAILABLE refusal for multi-source SI networks' \
  --body "$(cat <<'EOF'
Stage 1 of the multi-source storage-integrity network (spec `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` §6.4, §9, §14 sub-project 4).

- Pins arbiter-proto at the release that adds `ADMISSION_CODE_SOURCE_UNAVAILABLE` (10).
- `SubmitOutcomeFromSequencedAck` classifies it as a coded terminal reject, so the terminal-submit path removes the prepared parts.
- The ingress answers 733 `storage_integrity: the source of table <id> is not active yet; retry shortly (retryable)` with `KeepSession` and the unspent marker; the relay recognises the exact message on the deferred lane.
- `pkg/sitable`, README and CLAUDE.md: `Ordinary` also covers tables another SI indexer owns.

The arbiter answers code 10 only after the signed-claims activation, so behaviour before it is unchanged. Every SI host must run this release before the activation update.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Wait for the `Build` and `Integration (ClickHouse)` jobs (`gh pr checks <n> --repo housegate/housegate --watch`); judge any integration failure against a clean `main` run (main-baseline rule) before calling it a regression. Merge with the repository's squash convention: `gh pr merge <n> --repo housegate/housegate --squash`.

- [ ] **Step 11: Release `HG_TAG` (requires explicit user confirmation)**

```bash
gh workflow run release.yml --repo housegate/housegate --ref main -f bump=auto
gh run watch --repo housegate/housegate "$(gh run list --repo housegate/housegate --workflow release.yml --limit 1 --json databaseId --jq '.[0].databaseId')"
```

`bump=auto` cuts a minor version on the first cut of a UTC day and a patch version otherwise (`.github/workflows/release.yml:11-15`); record what the run prints, never a prediction. Verify by content:

```bash
R=/Users/uranuswch/Dev/housegate/housegate; HG_TAG=<tag>; PROTO_TAG=<ledger>
git -C "$R" fetch -q --tags
git -C "$R" show "$HG_TAG":pkg/chproto/client_error.go | grep -c 'is not active yet; retry shortly (retryable)'
git -C "$R" show "$HG_TAG":pkg/storageintegrity/arbiter_proto.go | grep -c 'ADMISSION_CODE_SOURCE_UNAVAILABLE'
git -C "$R" show "$HG_TAG":go.mod | grep 'sentioxyz/arbiter-proto'
```

Expected: `1`, `2`, and `github.com/sentioxyz/arbiter-proto <PROTO_TAG>`. Record `HG_TAG` in the release ledger. If only the Homebrew job fails, re-run that job alone (re-running the whole workflow cuts another tag).

---

## Task 14: sentio-node — signing SNode, owner filter, degraded start before enrolment, enrolment-statement CLI (S1)

The embedded SNode gets the two inputs arbiter-core's stage-1 role needs (CONTRACT §2, Task 4): `snode.Config.IndexerID` (the reconciler's owner filter and owner-scoped reads) from this node's on-chain indexer id, and `snode.Deps.ClaimSigner` built from the indexer key sentio-node already holds (`PRIVATE_KEY`, `loadPrivateKeyFromEnv`, `config/config.go:624-634`, the key `standalone.go:169` already turns into HouseGate's relay signer). Signed SNode messages bind the network's genesis snapshot id (CONTRACT §3b.3): the founding indexer's SNode derives it from its genesis set, any other indexer configures it in the new key `storage_integrity.snode.genesis_snapshot_id` (passed as `snode.Config.GenesisSnapshotID`), and a configured value is checked at startup against the derivation wherever genesis tables exist. An indexer that owns no genesis table may run with an empty `storage_integrity.snode.table_ids`; config validation then requires `genesis_snapshot_id` and the empty-set `schema_root` (CONTRACT §3b.8). A node that starts before its enrolment commits no longer rolls back its startup transaction: it decides from the registry follower's view, not from the arbiter's refusal, because the arbiter answers a refused `RegisterNode` with `codes.InvalidArgument` (`arbiter:server/server.go:256-260`), which `dataplane.Client.WithLeaderRetry` does not retry (`arbiter-core:dataplane/client.go:196-198, 242-244`), and starts with the SNode unregistered; an owned background task registers it when the registry version that carries its entry arrives. A new CLI subcommand prints the signed enrolment statement from the node config alone (no arbiter connection).

Pre-activation behaviour is unchanged: on devnet2 indexer-a's resolved id is 0, the founding `TableRegistryParams.SIIndexerID`, so the owner filter keeps every incarnation (nil owners count as the founding indexer's); before `si_indexers` exists the founding indexer counts as enrolled, so it registers synchronously exactly as today; an old or pre-activation arbiter ignores or strips the new signature fields (CONTRACT §0). Every SI deployment is an indexer node; an SI-enabled observer node, which never had a key to sign with, now refuses startup with a named error.

**Files:**
- Modify: `go.mod` (`:9` housegate `v0.17.2`, `:14` arbiter-core `v0.12.1`, `:15` arbiter-proto `v0.9.0`), `go.sum`, `MODULE.bazel` (`bazel_dep` `:12-19`, `git_override` `:29-41`), `MODULE.bazel.lock`
- Modify: `config/config.go` (`StorageIntegritySNode` `:59-69`: `TableIDs` doc, new `GenesisSnapshotID`; `StorageIntegrityConfig.Validate` `:71-125`: the empty-set error `:99-101` becomes the empty-genesis rules), `config/config_test.go` (`TestStorageIntegrityConfigValidation` `:207-240`, `TestConfigValidate_StorageIntegrityAssembly` `:260-326`)
- Modify: `standalone/storage_integrity_genesis.go` (`planStorageIntegrityGenesis` `:136-142`), `standalone/storage_integrity_contract_snapshot.go` (`:85-87`), `standalone/storage_integrity_genesis_test.go` (`:593`, `:595`; append), `standalone/storage_integrity_contract_snapshot_test.go` (append)
- Modify: `storageintegrityadapter/tablestate/model.go` (`Registry` `:48-58`; new `SIIndexer`, `IndexerView`, `Registry.Indexer`), `storageintegrityadapter/tablestate/arbitercore.go` (`followerSource.View` `:29-58`), `storageintegrityadapter/tablestate/arbitercore_test.go` (append), `storageintegrityadapter/tablestate/state_test.go` (append)
- Create: `standalone/storage_integrity_enrollment.go`, `standalone/storage_integrity_enrollment_test.go`
- Modify: `standalone/standalone.go` (imports; Run's SI block `:311-360`; `storageIntegrityRoleRegistration` `:646-649`; `snodeStorageIntegrityRoleLifecycle` `:709-727`; `newStorageIntegrityBootDeps` `:729-761`; `validateFor` `:783-795`; `start` `:1025-1029`)
- Modify: `standalone/storage_integrity_bootstrap_test.go` (recorder `:139-181`; `TestStorageIntegrityBootDepsValidateMandatoryDependenciesByMode` `:272-337`; `:667`; the `registration` literal `:740-744`; `:1317-1326`), `standalone/storage_integrity_genesis_ch_test.go:117`, `standalone/storage_integrity_acceptance_ch_test.go:136-146`, `standalone/storage_integrity_drift_ch_test.go:125-135` (tenth constructor argument)
- Create: `standalone/storage_integrity_enrollment_statement.go`, `standalone/storage_integrity_enrollment_statement_test.go`, `commands/storage_integrity.go`, `commands/storage_integrity_test.go`
- Modify: `main.go` (`rootCmd.AddCommand` `:44-54`); BUILD files through gazelle (`standalone/`, `commands/` gains a `go_test`, `storageintegrityadapter/tablestate/`)
- Modify: `README.md` (`:88`, `:90`; new "Storage-integrity enrolment" subsection before `:98`)

**Interfaces:**
- Consumes: `CORE_TAG` (names verbatim from CONTRACT §2/§3b and the Task 4 "Facts for other tasks") — `snode.Config.IndexerID uint64`, `snode.Config.GenesisSnapshotID string` (empty = derived with `dataplane.GenesisSnapshotID` from `Config.Tables`, founding indexer only), `snode.Deps.ClaimSigner *authority.Signer` (owner filtering, signing and the `signed_claims_v1` feature happen only with it; set together with `IndexerID`), `snode.Deps.Now` (left nil = `time.Now`), `snode.New`'s rule that an empty `Tables` needs `Deps.Registry` and `Deps.ClaimSigner` plus `SchemaRoot = payloadexec.SchemaRoot(net, nil)`, `func dataplane.GenesisSnapshotID(networkID, schemaSnapshotID, executorProfileID string, genesis []payloadexec.TableSchema) (string, error)`, `authority.NewSignerFromHex` (existing, `arbiter-core:authority/signer.go:32`), `(*authority.Signer).Address()` (lowercase), `authority.SNodeEnrollmentStatement{NetworkID, GenesisSnapshotID string; IndexerID uint64; SNodeNodeID string}`, `(*authority.Signer).SignSNodeEnrollment`, `authority.VerifySNodeEnrollment`, `arbiter.SIIndexerEntry{IndexerID, ActivationBlock, Signer, SNodeNodeID, EnrollmentJWS}`, `wire.TableRegistrySnapshot.SIIndexers []arbiter.SIIndexerEntry` and `.SeededIndexers []uint64`. `HG_TAG` (Task 13) and `PROTO_TAG` (Task 1) as pins. Existing: `common.IndexerRuntime.ResolvedIndexerID`, `IndexerRegistry.GetIndexerBySigner`, test helpers `recordingStorageIntegrityBootDependencies`, `testStorageIntegrityBootDeps`, `newScriptedRegistryFollower`, `activeTenantRegistry`, `parseStandaloneRun`, `readStandaloneSource`, `identifierCalls`, `identifierExpression`, `selectorExpression`, `selectorCall`, `expressionSelectorPath`, `compositeFieldValues`, `numberedRegistry`, `genesisIncarnation`, `fakeStorageIntegrityHeaderReader`, `fakeStorageIntegrityDatabasesCaller`, `staticFollower`, `validStorageIntegrityConfig`, `cloneStorageIntegrityConfig`, `indexerConfig`.
- Produces:
  - Config: `StorageIntegritySNode.GenesisSnapshotID string` (yaml `genesis_snapshot_id`, a `0x`-prefixed 64-digit lowercase hex digest when set); `storage_integrity.snode.table_ids` may be empty, and then `genesis_snapshot_id` is required and `schema_root` must equal the empty-set root; startup refuses an empty set while the arbiter's table registry is disabled. `node_id == housegate.storage_integrity.runtime.expected_source` is unchanged. Unexported `storageIntegrityGenesisSnapshotIDPattern`.
  - `tablestate.SIIndexer{IndexerID, ActivationBlock uint64; Signer, SNodeNodeID string}`; `tablestate.Registry.SIIndexers []SIIndexer`, `tablestate.Registry.SeededIndexers []uint64`; `tablestate.IndexerView{Enrolled bool; ActivationBlock uint64; Seeded bool; Signer, SNodeNodeID string}`; `func (r tablestate.Registry) Indexer(id uint64) tablestate.IndexerView`.
  - `standalone`: `storageIntegrityIndexerID(*common.IndexerRuntime) (uint64, error)`; `storageIntegrityClaimSigner(*ecdsa.PrivateKey) (*authority.Signer, error)`; `checkStorageIntegrityGenesisSnapshotID(snode config.StorageIntegritySNode, genesis []payloadexec.TableSchema) error`; `type storageIntegrityEnrollment interface{ enrolled() (bool, error); changed() <-chan struct{} }`; `type followerStorageIntegrityEnrollment struct{ registry tablestate.RegistrySource; indexerID uint64; nodeID, signer string }`; `type storageIntegrityDeferredRegistration struct{ enrollment storageIntegrityEnrollment; register func(context.Context) error; logger *slog.Logger; poll, retryMin, retryMax time.Duration }` with `run(context.Context) error`; constants `storageIntegrityEnrollmentPoll` (30 s), `storageIntegrityRegistrationRetryMin` (1 s), `storageIntegrityRegistrationRetryMax` (1 min); `storageIntegrityRoleRegistration` gains `deferred() (bool, error)` and `registerWhenEnrolled(context.Context) error`; `snodeStorageIntegrityRoleLifecycle` gains fields `enrollment`, `logger` and those two methods; `newStorageIntegrityBootDeps` gains a tenth parameter `enrollment storageIntegrityEnrollment`; `validateFor` no longer requires the create-mode preflight dependencies when `skipPreflight`; startup bootstrap step names `check-enrollment` and `defer-register-role`; owned task `storage integrity snode registration`.
  - Log lines (verbatim, for the runbook): `storage-integrity SNode registration deferred: this indexer is not enrolled in the arbiter's si_indexers yet; its tables answer ordinary until the enrolment commits` (warn) and `storage-integrity SNode registered after its enrolment committed` (info).
  - CLI (no arbiter connection): `sentio-node --config-path <path> storage-integrity enrollment-statement` → `commands.NewStorageIntegrityCommand(configPath *string) *cobra.Command`, unexported `newEnrollmentStatementCommand`; `standalone.PrintStorageIntegrityEnrollmentStatement(ctx context.Context, cfg *config.Config, out io.Writer) error`; `writeStorageIntegrityEnrollmentStatement(out io.Writer, signer *authority.Signer, stmt authority.SNodeEnrollmentStatement) error`.
  - Verified constant for overlays without genesis tables: `payloadexec.SchemaRoot(<any network>, nil)` = `0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59`.

- [ ] **Step 1: Worktree and contract check**

Create the worktree with the URWT command of Task 1 Step 1 for `/Users/uranuswch/Dev/sentio_xyz/sentio-node` (path `~/src/remotesrc/claude/sentio-node/multi-source-s1`). Then:

```bash
C=/Users/uranuswch/Dev/sentio_xyz/arbiter-core; H=/Users/uranuswch/Dev/housegate/housegate
CORE_TAG=<ledger>; HG_TAG=<ledger>
git -C "$C" fetch -q --tags && git -C "$H" fetch -q --tags
git -C "$C" grep -nE 'IndexerID +uint64|GenesisSnapshotID +string|ClaimSigner +\*authority.Signer|Now +func\(\) time.Time' "$CORE_TAG" -- snode/config.go snode/snode.go
git -C "$C" grep -n 'may own no genesis table' "$CORE_TAG" -- snode/snode.go
git -C "$C" grep -n 'func GenesisSnapshotID' "$CORE_TAG" -- dataplane/
git -C "$C" grep -nE 'SNodeEnrollmentStatement struct|func \(s \*Signer\) SignSNodeEnrollment|func VerifySNodeEnrollment' "$CORE_TAG" -- authority/
git -C "$C" grep -nE 'type SIIndexerEntry|SignedClaimsFeature' "$CORE_TAG" -- '*.go' ':!*_test.go'
git -C "$C" grep -nE 'SIIndexers|SeededIndexers|OwnerIndexerID' "$CORE_TAG" -- wire/table_registry_snapshot.go
git -C "$H" show "$HG_TAG":pkg/chproto/client_error.go | grep -c 'func SourceUnavailableMessage'
```

Expected: every grep prints at least one line — `IndexerID`, `GenesisSnapshotID`, `ClaimSigner` and `Now` on the SNode (CONTRACT §3a registration_seq, §3b.3, §3b.6), `snode.New`'s refusal of an empty genesis set unless the role follows the registry with a claim signer (§3b.8), `dataplane.GenesisSnapshotID`, the enrolment helpers, `SIIndexerEntry`, `SignedClaimsFeature` and the three registry-view fields; the last command prints `1`. If a name differs, stop and reconcile with the Task 4 plan instead of improvising.

- [ ] **Step 2: Pins**

```bash
W=<worktree path>; HG_TAG=<ledger>; CORE_TAG=<ledger>; PROTO_TAG=<ledger>
HG_SHA="$(git -C /Users/uranuswch/Dev/housegate/housegate rev-parse "$HG_TAG^{commit}")"
CORE_SHA="$(git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core rev-parse "$CORE_TAG^{commit}")"
cd "$W" && go get "github.com/housegate/housegate@$HG_TAG" "github.com/sentioxyz/arbiter-core@$CORE_TAG" "github.com/sentioxyz/arbiter-proto@$PROTO_TAG" && go mod tidy
```

Edit `MODULE.bazel`: `bazel_dep(name = "arbiter_core", version = "<CORE_TAG without v>")` (`:12-15`), `bazel_dep(name = "housegate", version = "<HG_TAG without v>")` (`:16-19`), the `git_override` commits (`:32` housegate, `:39` arbiter_core) to `$HG_SHA` / `$CORE_SHA`, and their `# Resolved … ; source is pinned by the commit below.` comments. arbiter-proto has no `bazel_dep`/`git_override` here (go_deps resolves it from `go.mod`, `use_repo` entry `com_github_sentioxyz_arbiter_proto` at `:76`), so `go.mod` is its only pin.

Run: `cd "$W" && bazel run @rules_go//go -- mod tidy && bazel mod tidy && bazel run //:gazelle && bazel build //... && grep -n "$HG_SHA\|$CORE_SHA" MODULE.bazel`
Expected: the build succeeds and `grep` prints both `commit = …` lines (the silent-failure check of the upgrade-dependency skill: a stale `git_override` compiles the old sources). Then `bazel test //...` — Expected: PASS.

```bash
cd "$W" && git status --short && git add -A && git commit -m "chore(deps): pin housegate $HG_TAG, arbiter-core $CORE_TAG and arbiter-proto $PROTO_TAG

Stage-1 multi-source SI builds (housegate spec 2026-10-10 §14):
housegate maps SOURCE_UNAVAILABLE, arbiter-core signs SNode messages and
filters the SNode's tables by owner, arbiter-proto carries the new
fields. housegate and arbiter-core are pinned in go.mod, bazel_dep and
git_override ($HG_SHA, $CORE_SHA); arbiter-proto only in go.mod.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 3: Write the failing tests for an empty genesis set**

In `config/config_test.go`, delete the `"no table ids"` row of `TestStorageIntegrityConfigValidation` (`:225`), add after `require.NoError(t, (StorageIntegrityConfig{}).Validate(""))` (`:210`):

```go
	// An indexer that owns no genesis table (housegate spec 2026-10-10 §10)
	// configures the network's genesis snapshot id and the empty-set
	// schema_root (CONTRACT §3b.3, §3b.8).
	empty := emptyGenesisStorageIntegrityConfig()
	require.NoError(t, empty.Validate("snode-1"))
	for name, mutate := range map[string]func(*StorageIntegrityConfig){
		"no genesis snapshot id": func(c *StorageIntegrityConfig) { c.SNode.GenesisSnapshotID = "" },
		"genesis schema root":    func(c *StorageIntegrityConfig) { c.SNode.SchemaRoot = "0xroot" },
	} {
		cfg := cloneStorageIntegrityConfig(empty)
		mutate(&cfg)
		require.Error(t, cfg.Validate("snode-1"), name)
	}
	require.ErrorContains(t, func() error {
		cfg := cloneStorageIntegrityConfig(empty)
		cfg.SNode.GenesisSnapshotID = ""
		return cfg.Validate("snode-1")
	}(), "storage_integrity.snode.genesis_snapshot_id is required when storage_integrity.snode.table_ids is empty")
	require.ErrorContains(t, func() error {
		cfg := cloneStorageIntegrityConfig(empty)
		cfg.SNode.SchemaRoot = "0xroot"
		return cfg.Validate("snode-1")
	}(), "0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59")
	for _, bad := range []string{"0xgenesis", "0x63700CF134CE037961D9ACF4AB5FF120DDE6C6B1D2BA258FA321D581139D1C06", " " + devnet2GenesisSnapshotID} {
		cfg := cloneStorageIntegrityConfig(base) // a founding indexer may configure it too
		cfg.SNode.GenesisSnapshotID = bad
		require.ErrorContains(t, cfg.Validate("snode-1"), "must be a 0x-prefixed 64-digit lowercase hex digest", bad)
	}
	founding := cloneStorageIntegrityConfig(base)
	founding.SNode.GenesisSnapshotID = devnet2GenesisSnapshotID
	require.NoError(t, founding.Validate("snode-1"))
```

and append to the helpers after `cloneStorageIntegrityConfig` (`:428-434`):

```go
// devnet2GenesisSnapshotID is devnet2-si-v2's genesis snapshot id (CONTRACT
// §3b.3).
const devnet2GenesisSnapshotID = "0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06"

// emptyGenesisStorageIntegrityConfig is a valid config of an indexer that owns
// no genesis table.
func emptyGenesisStorageIntegrityConfig() StorageIntegrityConfig {
	cfg := validStorageIntegrityConfig()
	cfg.SNode.TableIDs = nil
	cfg.SNode.SchemaRoot = "0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59"
	cfg.SNode.GenesisSnapshotID = devnet2GenesisSnapshotID
	return cfg
}
```

and add to `TestConfigValidate_StorageIntegrityAssembly` (after the `"genesis table ids need no housegate table list"` subtest, `:276-281`):

```go
	t.Run("an empty genesis set needs no housegate table list", func(t *testing.T) {
		cfg := base
		cfg.Housegate.StorageIntegrity.Tables = nil
		cfg.StorageIntegrity = emptyGenesisStorageIntegrityConfig()
		require.NoError(t, cfg.Validate())
	})

	t.Run("a legacy housegate list with an empty genesis set is an error", func(t *testing.T) {
		cfg := base
		cfg.StorageIntegrity = emptyGenesisStorageIntegrityConfig()
		cfg.Housegate.StorageIntegrity.Tables = []string{"orders.t"}
		require.ErrorContains(t, cfg.Validate(), "differs from storage_integrity.snode.table_ids")
	})
```

Append to `standalone/storage_integrity_genesis_test.go`:

```go
// An indexer that owns no genesis table takes every table from the registry
// (housegate spec 2026-10-10 §10), which must therefore be enabled.
func TestPlanStorageIntegrityGenesisRefusesAnEmptySetWithoutTheRegistry(t *testing.T) {
	_, err := planStorageIntegrityGenesis(nil, wire.TableRegistrySnapshot{}, false)
	require.ErrorContains(t, err, "storage_integrity.snode.table_ids is empty, but the arbiter's table registry is disabled")
	plan, err := planStorageIntegrityGenesis(nil, numberedRegistry(genesisIncarnation("t", wire.TableStatusActive)), true)
	require.NoError(t, err)
	require.Empty(t, plan, "an indexer that owns no genesis table plans nothing")
}

// TestEmptyGenesisSchemaRoot pins the schema_root an indexer without genesis
// tables configures: the root of the empty set, which no network id changes.
func TestEmptyGenesisSchemaRoot(t *testing.T) {
	const want = "0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59"
	for _, networkID := range []string{"devnet2", "testnet", "mainnet"} {
		require.Equal(t, want, payloadexec.SchemaRoot(networkID, nil), networkID)
	}
}
```

Append to `standalone/storage_integrity_contract_snapshot_test.go`:

```go
// An indexer that owns no genesis table has no declaration to materialize:
// the snapshot is empty and no finalized header is read.
func TestLoadStorageIntegrityContractSnapshotOfAnEmptyGenesisSetReadsNothing(t *testing.T) {
	headers := &fakeStorageIntegrityHeaderReader{err: errors.New("must not be read")}
	caller := &fakeStorageIntegrityDatabasesCaller{}
	snapshot, err := loadStorageIntegrityContractSnapshot(t.Context(), headers, caller, nil)
	require.NoError(t, err)
	require.NotNil(t, snapshot)
	require.Empty(t, headers.requested)
	require.Empty(t, caller.calls)
	_, ok := snapshot.LatestTableSchema("orders", "events")
	require.False(t, ok)
}
```

Append to `TestStorageIntegrityBootDepsValidateMandatoryDependenciesByMode` in `standalone/storage_integrity_bootstrap_test.go` (after the closing `require.NoError` at `:331-336`):

```go
	require.NoError(t, (&storageIntegrityBootDeps{
		mode:          ddl.ModeCreateAndVerify,
		skipPreflight: true,
		rolePreparer:  recorder,
		roleRunner:    recorder,
		role:          recorder,
	}).validateFor(ddl.ModeCreateAndVerify),
		"with no preflight table (an empty genesis set) nothing calls the create-mode preflight dependencies")
```

Run: `cd "$W" && go test ./config/ -run 'TestStorageIntegrityConfigValidation|TestConfigValidate_StorageIntegrityAssembly' -v; go test ./standalone/ -run 'EmptySet|EmptyGenesis|ValidateMandatoryDependencies' -v`
Expected: FAIL — the config tests do not compile (`unknown field GenesisSnapshotID`), and once the field exists `storage_integrity.snode.table_ids is required` refuses the empty config cases while nothing requires the genesis snapshot id or the empty-set root; `planStorageIntegrityGenesis` returns an empty plan with no error for the disabled registry; the contract snapshot errors `storage-integrity configured tables are required`; `validateFor` answers `storage-integrity protocol-table dependency is required`. `TestEmptyGenesisSchemaRoot` already passes; it pins the overlay value.

- [ ] **Step 4: Implement the empty genesis set**

`config/config.go`: in `StorageIntegritySNode` (`:59-69`) document `TableIDs` and add the genesis snapshot id:

```go
	// TableIDs is the genesis table set this indexer owns (housegate spec
	// 2026-10-10 §10). It is empty for an indexer that owns no genesis
	// table, which then configures GenesisSnapshotID and the empty-set
	// SchemaRoot, and needs the arbiter's table registry to be enabled.
	TableIDs []string `yaml:"table_ids"`
	// GenesisSnapshotID is the network's genesis snapshot id, which every
	// signed SNode message and the enrolment statement bind (CONTRACT
	// §3b.3). Empty lets the SNode derive it from the genesis tables, which
	// only the founding indexer holds; when set alongside genesis tables,
	// startup checks it against that derivation.
	GenesisSnapshotID string `yaml:"genesis_snapshot_id"`
```

(gofmt realigns the struct). Replace the empty-set error in `Validate` (`:99-101`)

```go
	if len(c.SNode.TableIDs) == 0 {
		errs = append(errs, errors.New("storage_integrity.snode.table_ids is required"))
	}
```

with

```go
	if len(c.SNode.TableIDs) == 0 {
		// An indexer that owns no genesis table cannot derive the network's
		// genesis snapshot id, and its schema_root covers the empty genesis
		// set (CONTRACT §3b.3, §3b.8).
		if strings.TrimSpace(c.SNode.GenesisSnapshotID) == "" {
			errs = append(errs, errors.New("storage_integrity.snode.genesis_snapshot_id is required when storage_integrity.snode.table_ids is empty: an indexer without genesis tables cannot derive the network's genesis snapshot id"))
		}
		if want := payloadexec.SchemaRoot(c.SNode.NetworkID, nil); c.SNode.SchemaRoot != want {
			errs = append(errs, fmt.Errorf("storage_integrity.snode.schema_root must be the empty genesis set's root %s when storage_integrity.snode.table_ids is empty, got %q", want, c.SNode.SchemaRoot))
		}
	}
	if id := c.SNode.GenesisSnapshotID; id != "" && !storageIntegrityGenesisSnapshotIDPattern.MatchString(id) {
		errs = append(errs, fmt.Errorf("storage_integrity.snode.genesis_snapshot_id %q must be a 0x-prefixed 64-digit lowercase hex digest", id))
	}
```

and add, after the database-name constants (`:31-35`):

```go
// storageIntegrityGenesisSnapshotIDPattern is a sealed manifest's snapshot id:
// replay.DigestString's 0x-prefixed lowercase SHA-256.
var storageIntegrityGenesisSnapshotIDPattern = regexp.MustCompile(`^0x[0-9a-f]{64}$`)
```

(imports `regexp` and `github.com/housegate/housegate/pkg/replay/payloadexec`).

`standalone/storage_integrity_genesis.go`, first statement of `planStorageIntegrityGenesis` (`:141`):

```go
	if !enabled && len(ids) == 0 {
		// While the registry is disabled the genesis set is the whole table
		// set; an empty one is valid only for an indexer that owns no genesis
		// table and takes every table from the registry (spec 2026-10-10 §10).
		return nil, errors.New("storage_integrity.snode.table_ids is empty, but the arbiter's table registry is disabled: an indexer without genesis tables needs the registry")
	}
```

(add `"errors"` to the imports). `standalone/storage_integrity_contract_snapshot.go` (`:85-87`):

```go
	if len(tableIDs) == 0 {
		// An indexer that owns no genesis table (spec 2026-10-10 §10) has no
		// declaration to materialize; the registry supplies every schema.
		return &storageIntegrityContractSnapshot{byIdentity: map[storageIntegritySchemaIdentity]registry.TableSchema{}}, nil
	}
```

`standalone/standalone.go` `validateFor` (`:786-795`), after the unsupported-mode check:

```go
	if d.skipPreflight {
		// No genesis table takes part in the create-mode preflight (an empty
		// genesis set, or every genesis table retiring): nothing calls the
		// protocol-table or cross-check dependency.
		return nil
	}
```

Run: `cd "$W" && go test ./config/ ./standalone/ -run 'StorageIntegrityConfigValidation|StorageIntegrityAssembly|EmptySet|EmptyGenesis|ValidateMandatoryDependencies|SkipsAnEmptyPreflight|PlanStorageIntegrityGenesis|LoadStorageIntegrityContractSnapshot' -v`
Expected: PASS.

```bash
cd "$W" && git add -A && git commit -m "feat(storage-integrity): allow an empty genesis set for an indexer that owns no genesis table

storage_integrity.snode.table_ids lists the genesis tables this indexer
owns and may be empty (housegate spec 2026-10-10 §10). Startup refuses
an empty set only while the arbiter's table registry is disabled; the
network_state snapshot of an empty set reads nothing, and the
create-mode preflight dependencies are not required when no table takes
part in the preflight. Its schema_root is the empty-set root
0x3aaa1430…7e59.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: Write the failing tests for the enrolment view of the registry**

Append to `storageintegrityadapter/tablestate/arbitercore_test.go`:

```go
// TestFromFollowerMapsTheEnrollment covers housegate spec 2026-10-10 §6.1:
// the view carries the committed si_indexers and the seeded set.
func TestFromFollowerMapsTheEnrollment(t *testing.T) {
	view, enabled := FromFollower(staticFollower{enabled: true, snapshot: wire.TableRegistrySnapshot{
		Params:  arbiter.TableRegistryParams{ChainID: 1, SIIndexerID: 0, ActivationBlock: 5508931, Confirmation: "safe"},
		Version: 30,
		SIIndexers: []arbiter.SIIndexerEntry{
			{IndexerID: 0, ActivationBlock: 5508931, Signer: "0x22a67f498e669cbde18a0eecfff74a72c48331f4", SNodeNodeID: "snode-1", EnrollmentJWS: "a.b.c"},
			{IndexerID: 1, ActivationBlock: 5600000, Signer: "0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3", SNodeNodeID: "snode-b", EnrollmentJWS: "d.e.f"},
		},
		SeededIndexers: []uint64{0},
	}}).View()
	require.True(t, enabled)
	require.Equal(t, []SIIndexer{
		{IndexerID: 0, ActivationBlock: 5508931, Signer: "0x22a67f498e669cbde18a0eecfff74a72c48331f4", SNodeNodeID: "snode-1"},
		{IndexerID: 1, ActivationBlock: 5600000, Signer: "0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3", SNodeNodeID: "snode-b"},
	}, view.SIIndexers)
	require.Equal(t, []uint64{0}, view.SeededIndexers)

	before, _ := FromFollower(staticFollower{enabled: true, snapshot: wire.TableRegistrySnapshot{Version: 3}}).View()
	require.Nil(t, before.SIIndexers, "nil until the signed-claims activation")
	require.Nil(t, before.SeededIndexers)
}
```

Append to `storageintegrityadapter/tablestate/state_test.go`:

```go
// TestRegistryIndexerView is housegate spec 2026-10-10 §10's per-indexer view:
// before the signed-claims activation the founding indexer is the only
// enrolled one, with the registry's own seed flag and activation block;
// afterwards each entry has its own activation block and seed.
func TestRegistryIndexerView(t *testing.T) {
	before := Registry{SIIndexerID: siIndexer, ActivationBlock: activation, Seeded: true}
	require.Equal(t, IndexerView{Enrolled: true, ActivationBlock: activation, Seeded: true}, before.Indexer(siIndexer))
	require.Equal(t, IndexerView{}, before.Indexer(otherIndexer))

	after := Registry{
		SIIndexerID: siIndexer, ActivationBlock: activation, Seeded: true,
		SIIndexers: []SIIndexer{
			{IndexerID: siIndexer, ActivationBlock: activation, Signer: "0xaa", SNodeNodeID: "snode-1"},
			{IndexerID: otherIndexer, ActivationBlock: 2000, Signer: "0xbb", SNodeNodeID: "snode-b"},
		},
		SeededIndexers: []uint64{siIndexer},
	}
	require.Equal(t, IndexerView{Enrolled: true, ActivationBlock: activation, Seeded: true, Signer: "0xaa", SNodeNodeID: "snode-1"}, after.Indexer(siIndexer))
	require.Equal(t, IndexerView{Enrolled: true, ActivationBlock: 2000, Signer: "0xbb", SNodeNodeID: "snode-b"}, after.Indexer(otherIndexer),
		"an enrolled indexer stays unseeded until its own seed commits")
	after.SeededIndexers = []uint64{siIndexer, otherIndexer}
	require.True(t, after.Indexer(otherIndexer).Seeded)
	require.Equal(t, IndexerView{}, after.Indexer(42), "an indexer without an entry is not enrolled")
}
```

Run: `cd "$W" && go test ./storageintegrityadapter/tablestate/ -run 'TestFromFollowerMapsTheEnrollment|TestRegistryIndexerView' -v`
Expected: FAIL to compile — `undefined: SIIndexer`, `IndexerView`, `Registry.Indexer`, unknown fields `SIIndexers`, `SeededIndexers`.

- [ ] **Step 6: Implement the enrolment view**

In `storageintegrityadapter/tablestate/model.go` add to `Registry` (after `Incarnations`, `:54`):

```go
	// SIIndexers is the committed si_indexers list, sorted by indexer id; nil
	// until the signed-claims activation (housegate spec 2026-10-10 §6.1).
	SIIndexers []SIIndexer
	// SeededIndexers lists the indexers whose Legacy seed committed; nil until
	// the activation, before which the founding indexer's seed is Seeded.
	SeededIndexers []uint64
```

and after the `Registry` type:

```go
// SIIndexer is the part of one si_indexers entry the host reads.
type SIIndexer struct {
	IndexerID       uint64
	ActivationBlock uint64
	// Signer is the lowercase 0x address of the indexer's IndexerRegistry
	// signer, which signs its SNode's messages (spec 2026-10-10 D6).
	Signer      string
	SNodeNodeID string
}

// IndexerView is what one registry view says about one indexer's place in
// the storage-integrity network (spec 2026-10-10 §10).
type IndexerView struct {
	// Enrolled: the indexer has an si_indexers entry. Before the signed-claims
	// activation (no entries yet) only the founding indexer,
	// TableRegistryParams.SIIndexerID, is enrolled.
	Enrolled bool
	// ActivationBlock is the entry's activation_block, the founding
	// TableRegistryParams.ActivationBlock before the activation, and zero
	// when the indexer is not enrolled.
	ActivationBlock uint64
	// Seeded: the indexer's Legacy seed committed (SeededIndexers; the
	// founding Seeded flag before the activation).
	Seeded bool
	// Signer and SNodeNodeID are the entry's; empty before the activation.
	Signer      string
	SNodeNodeID string
}

// Indexer answers IndexerView for indexer id.
func (r Registry) Indexer(id uint64) IndexerView {
	if len(r.SIIndexers) == 0 {
		if id != r.SIIndexerID {
			return IndexerView{}
		}
		return IndexerView{Enrolled: true, ActivationBlock: r.ActivationBlock, Seeded: r.Seeded}
	}
	for _, entry := range r.SIIndexers {
		if entry.IndexerID == id {
			return IndexerView{
				Enrolled:        true,
				ActivationBlock: entry.ActivationBlock,
				Seeded:          slices.Contains(r.SeededIndexers, id),
				Signer:          entry.Signer,
				SNodeNodeID:     entry.SNodeNodeID,
			}
		}
	}
	return IndexerView{}
}
```

(add `"slices"` to the imports of `model.go`). In `storageintegrityadapter/tablestate/arbitercore.go`, after the incarnation loop of `followerSource.View` (`:53`):

```go
	for _, entry := range snapshot.SIIndexers {
		out.SIIndexers = append(out.SIIndexers, SIIndexer{
			IndexerID:       entry.IndexerID,
			ActivationBlock: entry.ActivationBlock,
			Signer:          entry.Signer,
			SNodeNodeID:     entry.SNodeNodeID,
		})
	}
	out.SeededIndexers = append([]uint64(nil), snapshot.SeededIndexers...)
```

Run: `cd "$W" && go test ./storageintegrityadapter/tablestate/ -v`
Expected: PASS, including the unchanged `TestFromFollowerMapsEveryField` (its snapshot carries neither field, and `append([]uint64(nil))` of nil stays nil).

- [ ] **Step 7: Write the failing tests for the signing identity and the deferred registration**

Create `standalone/storage_integrity_enrollment_test.go`:

```go
package standalone

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"go/ast"
	"log/slog"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"compute-network-node/common"
	"compute-network-node/config"
	"compute-network-node/storageintegrityadapter/tablestate"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStorageIntegrityIndexerIDRequiresTheResolvedID(t *testing.T) {
	for name, runtime := range map[string]*common.IndexerRuntime{
		"observer node":       nil,
		"unresolved sentinel": {IndexerId: big.NewInt(-1)},
	} {
		_, err := storageIntegrityIndexerID(runtime)
		require.ErrorContains(t, err, "resolved on-chain indexer id", name)
	}
	id, err := storageIntegrityIndexerID(&common.IndexerRuntime{IndexerId: big.NewInt(0)})
	require.NoError(t, err)
	require.Zero(t, id, "indexer 0 is devnet2's founding indexer, a valid id")
}

func TestStorageIntegrityClaimSignerUsesTheIndexerKey(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer, err := storageIntegrityClaimSigner(key)
	require.NoError(t, err)
	require.Equal(t, strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex()), signer.Address(),
		"D6: the SNode speaks with the indexer's on-chain signer key; no new key")
	for name, key := range map[string]*ecdsa.PrivateKey{"nil": nil, "zero": {}} {
		_, err := storageIntegrityClaimSigner(key)
		require.ErrorContains(t, err, "PRIVATE_KEY", name)
	}
}

// TestCheckStorageIntegrityGenesisSnapshotID covers CONTRACT §3b.3: the SNode
// signs with a configured genesis snapshot id as given, so wherever this node
// holds genesis tables (the founding indexer, whose genesis set is the
// arbiter's) a configured id must be the arbiter's derivation.
func TestCheckStorageIntegrityGenesisSnapshotID(t *testing.T) {
	genesis := []payloadexec.TableSchema{{TableID: "orders.t", Columns: []lthash.Column{{Name: "value", Type: "Int64"}}}}
	sn := config.StorageIntegritySNode{NetworkID: "devnet2", SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0"}
	derived, err := dataplane.GenesisSnapshotID(sn.NetworkID, sn.SchemaSnapshotID, sn.ExecutorProfileID, genesis)
	require.NoError(t, err)

	require.NoError(t, checkStorageIntegrityGenesisSnapshotID(sn, genesis), "unset: the SNode derives it")
	sn.GenesisSnapshotID = derived
	require.NoError(t, checkStorageIntegrityGenesisSnapshotID(sn, genesis))
	sn.GenesisSnapshotID = "0x" + strings.Repeat("0", 64)
	require.NoError(t, checkStorageIntegrityGenesisSnapshotID(sn, nil),
		"no genesis table: nothing to derive it from, the configured id is taken as given")
	err = checkStorageIntegrityGenesisSnapshotID(sn, genesis)
	require.ErrorContains(t, err, "storage_integrity.snode.genesis_snapshot_id")
	require.ErrorContains(t, err, sn.GenesisSnapshotID)
	require.ErrorContains(t, err, derived)
}

func TestFollowerStorageIntegrityEnrollmentReadsTheRegistry(t *testing.T) {
	const signer = "0x00000000000000000000000000000000000000bb"
	entry := func(id, activation uint64, signer, node string) arbiter.SIIndexerEntry {
		return arbiter.SIIndexerEntry{IndexerID: id, ActivationBlock: activation, Signer: signer, SNodeNodeID: node, EnrollmentJWS: "jws"}
	}
	activated := func(entries ...arbiter.SIIndexerEntry) wire.TableRegistrySnapshot {
		snapshot := activeTenantRegistry(t) // founding indexer 7
		snapshot.SIIndexers = entries
		return snapshot
	}
	founding := entry(7, 100, "0x00000000000000000000000000000000000000aa", "snode-1")
	for name, tc := range map[string]struct {
		snapshot  wire.TableRegistrySnapshot
		enabled   bool
		indexerID uint64
		want      bool
		wantErr   string
	}{
		"registry disabled registers as before":  {indexerID: 8, want: true},
		"founding indexer before the activation": {snapshot: activeTenantRegistry(t), enabled: true, indexerID: 7, want: true},
		"another indexer before the activation":  {snapshot: activeTenantRegistry(t), enabled: true, indexerID: 8},
		"not enrolled after the activation":      {snapshot: activated(founding), enabled: true, indexerID: 8},
		"enrolled":                               {snapshot: activated(founding, entry(8, 200, signer, "snode-b")), enabled: true, indexerID: 8, want: true},
		"enrolled under another node id": {snapshot: activated(founding, entry(8, 200, signer, "snode-c")), enabled: true, indexerID: 8,
			wantErr: `indexer 8 is enrolled with snode_node_id "snode-c", but storage_integrity.snode.node_id is "snode-b"`},
		"enrolled under another signer": {snapshot: activated(founding, entry(8, 200, "0x00000000000000000000000000000000000000cc", "snode-b")), enabled: true, indexerID: 8,
			wantErr: "indexer 8 is enrolled with signer 0x00000000000000000000000000000000000000cc, but this node's indexer key is " + signer},
	} {
		t.Run(name, func(t *testing.T) {
			follower := newScriptedRegistryFollower(tc.snapshot, tc.enabled, true)
			got, err := followerStorageIntegrityEnrollment{
				registry: tablestate.FromFollower(follower), indexerID: tc.indexerID, nodeID: "snode-b", signer: signer,
			}.enrolled()
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	follower := newScriptedRegistryFollower(activeTenantRegistry(t), true, true)
	gate := followerStorageIntegrityEnrollment{registry: tablestate.FromFollower(follower)}
	require.Equal(t, follower.Changed(), gate.changed(), "a registry version change wakes a deferred registration")
}

type scriptedStorageIntegrityEnrollment struct {
	mu      sync.Mutex
	ok      bool
	err     error
	changes chan struct{}
}

func newScriptedStorageIntegrityEnrollment() *scriptedStorageIntegrityEnrollment {
	return &scriptedStorageIntegrityEnrollment{changes: make(chan struct{})}
}

func (e *scriptedStorageIntegrityEnrollment) enrolled() (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ok, e.err
}

func (e *scriptedStorageIntegrityEnrollment) changed() <-chan struct{} {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.changes
}

// commit lands the enrolment as the registry does: a new version wakes every
// waiter. A non-nil err is an enrolment naming another node.
func (e *scriptedStorageIntegrityEnrollment) commit(err error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.ok, e.err = err == nil, err
	close(e.changes)
	e.changes = make(chan struct{})
}

func TestStorageIntegrityDeferredRegistrationWaitsForTheEnrollment(t *testing.T) {
	enrollment := newScriptedStorageIntegrityEnrollment()
	var calls atomic.Int32
	registered := make(chan struct{})
	deferred := storageIntegrityDeferredRegistration{
		enrollment: enrollment,
		register: func(context.Context) error {
			if calls.Add(1) == 1 {
				// The follower saw the version before the leader that answers.
				return status.Error(codes.InvalidArgument, "snode-b is not an enrolled SNode")
			}
			close(registered)
			return nil
		},
		logger:   slog.New(slog.DiscardHandler),
		poll:     time.Hour, // only the registry change may wake it
		retryMin: time.Millisecond,
		retryMax: time.Millisecond,
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- deferred.run(ctx) }()

	require.Never(t, func() bool { return calls.Load() > 0 }, 100*time.Millisecond, 5*time.Millisecond,
		"no registration before the enrolment commits")
	enrollment.commit(nil)
	select {
	case <-registered:
	case <-time.After(5 * time.Second):
		t.Fatal("the SNode did not register after its enrolment committed")
	}
	require.EqualValues(t, 2, calls.Load(), "a refused registration after the enrolment is retried")
	select {
	case err := <-done:
		t.Fatalf("an owned startup task returned before shutdown: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestStorageIntegrityDeferredRegistrationStopsOnAMismatchedEnrollment(t *testing.T) {
	enrollment := newScriptedStorageIntegrityEnrollment()
	deferred := storageIntegrityDeferredRegistration{
		enrollment: enrollment,
		register: func(context.Context) error {
			t.Error("a mismatched enrolment must never register")
			return nil
		},
		logger: slog.New(slog.DiscardHandler),
		poll:   time.Hour,
	}
	done := make(chan error, 1)
	go func() { done <- deferred.run(t.Context()) }()
	enrollment.commit(errors.New(`indexer 1 is enrolled with snode_node_id "snode-c", but storage_integrity.snode.node_id is "snode-b"`))
	select {
	case err := <-done:
		require.ErrorContains(t, err, `"snode-c"`)
	case <-time.After(5 * time.Second):
		t.Fatal("a mismatched enrolment must fail the owned task, and with it the runtime")
	}
}

// Spec 2026-10-10 §10: a node that starts before its enrolment commits does
// not roll back; registration moves to an owned task.
func TestStorageIntegrityListenerPermitDefersRegistrationBeforeEnrollment(t *testing.T) {
	recorder := &recordingStorageIntegrityBootDependencies{deferRegistration: true}
	permit, err := prepareStorageIntegrityListenerPermit(t.Context(), config.StorageIntegrityConfig{
		Enabled: true,
		SNode:   config.StorageIntegritySNode{SchemaSource: "clickhouse"},
	}, &storageIntegrityBootDeps{mode: ddl.ModeVerifyOnly, rolePreparer: recorder, roleRunner: recorder, role: recorder})
	require.NoError(t, err)
	runtime, err := permit.start(t.Context(), func(*startupTransaction) error { return nil })
	require.NoError(t, err, "an indexer that is not enrolled yet starts instead of rolling back")
	runtime.stopAndWait()
	require.Equal(t, []string{"prepare", "run-ready", "register-when-enrolled"}, recorder.calls,
		"nothing registers synchronously; the owned task ran and ended with the runtime")
}

func TestStorageIntegrityListenerPermitRollsBackAMismatchedEnrollment(t *testing.T) {
	want := errors.New(`indexer 1 is enrolled with snode_node_id "snode-c", but storage_integrity.snode.node_id is "snode-b"`)
	recorder := &recordingStorageIntegrityBootDependencies{enrollmentErr: want}
	permit, err := prepareStorageIntegrityListenerPermit(t.Context(), config.StorageIntegrityConfig{
		Enabled: true,
		SNode:   config.StorageIntegritySNode{SchemaSource: "clickhouse"},
	}, &storageIntegrityBootDeps{mode: ddl.ModeVerifyOnly, rolePreparer: recorder, roleRunner: recorder, role: recorder})
	require.NoError(t, err)
	runtime, err := permit.start(t.Context(), func(*startupTransaction) error { return nil })
	require.ErrorIs(t, err, want)
	require.ErrorContains(t, err, "check-enrollment")
	require.Nil(t, runtime)
	require.Equal(t, []string{"prepare", "run-ready"}, recorder.calls)
}

// TestRunSignsTheSNodeWithTheIndexerIdentity pins spec 2026-10-10 D6 and §8:
// Run hands the SNode its own on-chain indexer id (the reconciler's owner
// filter), a claim signer built from the indexer key, each derived once, and
// the configured genesis snapshot id after checking it (CONTRACT §3b.3).
func TestRunSignsTheSNodeWithTheIndexerIdentity(t *testing.T) {
	_, runDecl := parseStandaloneRun(t, readStandaloneSource(t))
	require.Len(t, identifierCalls(runDecl, "storageIntegrityIndexerID"), 1)
	require.Len(t, identifierCalls(runDecl, "storageIntegrityClaimSigner"), 1)
	genesisChecks := identifierCalls(runDecl, "checkStorageIntegrityGenesisSnapshotID")
	require.Len(t, genesisChecks, 1)
	require.Equal(t, "si.SNode", expressionSelectorPath(genesisChecks[0].Args[0]))
	require.Equal(t, "schemaSets.snode", expressionSelectorPath(genesisChecks[0].Args[1]),
		"the derivation covers every genesis schema, retired ones included, as Config.Tables does")
	fieldsOf := func(pkg, typ string) map[string]ast.Expr {
		var literals []*ast.CompositeLit
		ast.Inspect(runDecl.Body, func(node ast.Node) bool {
			if literal, ok := node.(*ast.CompositeLit); ok {
				if selector, ok := literal.Type.(*ast.SelectorExpr); ok && selector.Sel.Name == typ && identifierExpression(selector.X, pkg) {
					literals = append(literals, literal)
				}
			}
			return true
		})
		require.Len(t, literals, 1, "%s.%s", pkg, typ)
		fields := map[string]ast.Expr{}
		for _, element := range literals[0].Elts {
			if kv, ok := element.(*ast.KeyValueExpr); ok {
				if key, ok := kv.Key.(*ast.Ident); ok {
					fields[key.Name] = kv.Value
				}
			}
		}
		return fields
	}
	snodeConfig := fieldsOf("snode", "Config")
	require.True(t, identifierExpression(snodeConfig["IndexerID"], "siIndexerID"))
	require.Equal(t, "si.SNode.GenesisSnapshotID", expressionSelectorPath(snodeConfig["GenesisSnapshotID"]))
	require.True(t, identifierExpression(fieldsOf("snode", "Deps")["ClaimSigner"], "claimSigner"))
}

// TestRunGatesTheSNodeRegistrationOnItsOwnEnrollment pins spec 2026-10-10
// §10: the gate reads this node's own indexer id, node id and claim signer
// from the one registry follower the SNode and the table state share.
func TestRunGatesTheSNodeRegistrationOnItsOwnEnrollment(t *testing.T) {
	_, runDecl := parseStandaloneRun(t, readStandaloneSource(t))
	var literals []*ast.CompositeLit
	ast.Inspect(runDecl.Body, func(node ast.Node) bool {
		if literal, ok := node.(*ast.CompositeLit); ok && identifierExpression(literal.Type, "followerStorageIntegrityEnrollment") {
			literals = append(literals, literal)
		}
		return true
	})
	require.Len(t, literals, 1)
	field := func(name string) ast.Expr {
		values := compositeFieldValues(literals[0], name)
		require.Len(t, values, 1, name)
		return values[0]
	}
	registry, ok := field("registry").(*ast.CallExpr)
	require.True(t, ok)
	require.True(t, selectorExpression(registry.Fun, "tablestate", "FromFollower"))
	require.True(t, identifierExpression(registry.Args[0], "siRegistry"), "the follower the SNode and the table state read")
	require.True(t, identifierExpression(field("indexerID"), "siIndexerID"))
	require.Equal(t, "si.SNode.NodeID", expressionSelectorPath(field("nodeID")))
	signer, ok := field("signer").(*ast.CallExpr)
	require.True(t, ok)
	require.True(t, selectorCall(signer, "claimSigner", "Address"))
	bootDeps := identifierCalls(runDecl, "newStorageIntegrityBootDeps")
	require.Len(t, bootDeps, 1)
	require.True(t, identifierExpression(bootDeps[0].Args[9], "siEnrollment"))
}
```

Extend the recorder in `standalone/storage_integrity_bootstrap_test.go` (`:139-143`, methods after `register` at `:179-181`) so it implements the new registration methods (they record nothing until the implementation calls them):

```go
type recordingStorageIntegrityBootDependencies struct {
	calls  []string
	failAt string
	err    error
	// deferRegistration and enrollmentErr script deferred(): an indexer that
	// is not enrolled yet, or an enrolment that names another node.
	deferRegistration bool
	enrollmentErr     error
}
```

```go
func (d *recordingStorageIntegrityBootDependencies) deferred() (bool, error) {
	return d.deferRegistration, d.enrollmentErr
}

func (d *recordingStorageIntegrityBootDependencies) registerWhenEnrolled(ctx context.Context) error {
	if err := d.record("register-when-enrolled"); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}
```

Run: `cd "$W" && go test ./standalone/ -run 'IndexerID|ClaimSigner|GenesisSnapshotID|StorageIntegrityEnrollment|DeferredRegistration|DefersRegistration|MismatchedEnrollment|SignsTheSNode|GatesTheSNodeRegistration' -v`
Expected: FAIL to compile — `undefined: storageIntegrityIndexerID`, `storageIntegrityClaimSigner`, `checkStorageIntegrityGenesisSnapshotID`, `followerStorageIntegrityEnrollment`, `storageIntegrityDeferredRegistration`. (With the helpers stubbed, the permit tests would record `register` instead of `register-when-enrolled`, and the two Run audits find no `IndexerID`, `GenesisSnapshotID`, `ClaimSigner`, genesis check or gate literal.)

- [ ] **Step 8: Implement the signing identity, the gate and the deferred registration**

Create `standalone/storage_integrity_enrollment.go`:

```go
package standalone

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"compute-network-node/common"
	"compute-network-node/config"
	"compute-network-node/storageintegrityadapter/tablestate"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
)

// storageIntegrityIndexerID is the SNode's owning indexer (snode.Config.IndexerID,
// the reconciler's owner filter, housegate spec 2026-10-10 §8): this node's
// on-chain id, the one sentio_getStorageIntegrityInfo reports as
// self_indexer_id. Standalone resolves it with EnsureRegistered before any
// storage-integrity wiring.
func storageIntegrityIndexerID(runtime *common.IndexerRuntime) (uint64, error) {
	id, ok := runtime.ResolvedIndexerID()
	if !ok {
		return 0, errors.New("storage integrity requires an indexer node with a resolved on-chain indexer id (node.node_type: indexer): the SNode serves that indexer's tables and signs with its key")
	}
	return id, nil
}

// storageIntegrityClaimSigner builds the SNode's message signer from the
// indexer key the node already holds (PRIVATE_KEY): spec 2026-10-10 D6
// introduces no new key.
func storageIntegrityClaimSigner(key *ecdsa.PrivateKey) (*authority.Signer, error) {
	if key == nil || key.D == nil || key.D.Sign() == 0 {
		return nil, errors.New("storage integrity requires the indexer private key (PRIVATE_KEY): the SNode signs its messages with it")
	}
	signer, err := authority.NewSignerFromHex(hex.EncodeToString(crypto.FromECDSA(key)))
	if err != nil {
		return nil, fmt.Errorf("storage integrity claim signer from the indexer key: %w", err)
	}
	return signer, nil
}

// checkStorageIntegrityGenesisSnapshotID refuses a configured genesis snapshot
// id that is not the arbiter's derivation over this node's genesis tables
// (CONTRACT §3b.3). The SNode signs every message with a configured id as
// given, so a wrong one would only show as refused claims after the
// activation. A node without genesis tables (not the founding indexer) cannot
// derive it and must be configured with it; config validation requires that.
func checkStorageIntegrityGenesisSnapshotID(snode config.StorageIntegritySNode, genesis []payloadexec.TableSchema) error {
	if snode.GenesisSnapshotID == "" || len(genesis) == 0 {
		return nil
	}
	derived, err := dataplane.GenesisSnapshotID(snode.NetworkID, snode.SchemaSnapshotID, snode.ExecutorProfileID, genesis)
	if err != nil {
		return fmt.Errorf("storage integrity genesis snapshot id: %w", err)
	}
	if derived != snode.GenesisSnapshotID {
		return fmt.Errorf("storage_integrity.snode.genesis_snapshot_id %s is not the network's genesis snapshot id %s derived from this node's genesis tables", snode.GenesisSnapshotID, derived)
	}
	return nil
}

// storageIntegrityEnrollment is the SNode registration's view of this
// indexer's enrolment in the arbiter's committed si_indexers (spec 2026-10-10
// §6.2, §10).
type storageIntegrityEnrollment interface {
	// enrolled reports whether the SNode may register now: true while the
	// table registry is disabled (no enrolment exists then; registration is
	// as before), and afterwards once the registry enrols this indexer. It
	// refuses an enrolment whose snode_node_id or signer is not this node's.
	enrolled() (bool, error)
	// changed is closed at the next accepted registry version after the call.
	changed() <-chan struct{}
}

// followerStorageIntegrityEnrollment reads the enrolment from the registry
// follower the SNode and the table state share.
type followerStorageIntegrityEnrollment struct {
	registry  tablestate.RegistrySource
	indexerID uint64
	nodeID    string
	// signer is the lowercase 0x address of the claim signer.
	signer string
}

func (e followerStorageIntegrityEnrollment) enrolled() (bool, error) {
	view, enabled := e.registry.View()
	if !enabled {
		return true, nil
	}
	self := view.Indexer(e.indexerID)
	switch {
	case !self.Enrolled:
		return false, nil
	case self.SNodeNodeID != "" && self.SNodeNodeID != e.nodeID:
		return false, fmt.Errorf("indexer %d is enrolled with snode_node_id %q, but storage_integrity.snode.node_id is %q", e.indexerID, self.SNodeNodeID, e.nodeID)
	case self.Signer != "" && !strings.EqualFold(self.Signer, e.signer):
		return false, fmt.Errorf("indexer %d is enrolled with signer %s, but this node's indexer key is %s", e.indexerID, self.Signer, e.signer)
	}
	return true, nil
}

func (e followerStorageIntegrityEnrollment) changed() <-chan struct{} { return e.registry.Changed() }

// Bounds of a deferred SNode registration. An enrolment bumps the registry
// version, which wakes the wait at once; the poll is a backstop for a missed
// wake-up.
const (
	storageIntegrityEnrollmentPoll       = 30 * time.Second
	storageIntegrityRegistrationRetryMin = time.Second
	storageIntegrityRegistrationRetryMax = time.Minute
)

// storageIntegrityDeferredRegistration registers the SNode of an indexer that
// was not enrolled when it started (spec 2026-10-10 §10): it waits for the
// enrolment, registers with exponential backoff, then holds until shutdown,
// because an owned startup task that returns fails the runtime.
type storageIntegrityDeferredRegistration struct {
	enrollment storageIntegrityEnrollment
	register   func(context.Context) error
	logger     *slog.Logger
	poll       time.Duration
	retryMin   time.Duration
	retryMax   time.Duration
}

func (d storageIntegrityDeferredRegistration) run(ctx context.Context) error {
	logger := d.logger
	if logger == nil {
		logger = slog.Default()
	}
	poll, retry, retryMax := d.poll, d.retryMin, d.retryMax
	if poll <= 0 {
		poll = storageIntegrityEnrollmentPoll
	}
	if retry <= 0 {
		retry = storageIntegrityRegistrationRetryMin
	}
	if retryMax <= 0 {
		retryMax = storageIntegrityRegistrationRetryMax
	}
	logger.Warn("storage-integrity SNode registration deferred: this indexer is not enrolled in the arbiter's si_indexers yet; its tables answer ordinary until the enrolment commits")
	for {
		changed := d.enrollment.changed()
		enrolled, err := d.enrollment.enrolled()
		if err != nil {
			return fmt.Errorf("storage-integrity enrolment: %w", err)
		}
		if !enrolled {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-changed:
			case <-time.After(poll):
			}
			continue
		}
		err = d.register(ctx)
		if err == nil {
			logger.Info("storage-integrity SNode registered after its enrolment committed")
			<-ctx.Done()
			return ctx.Err()
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		logger.Warn("storage-integrity SNode registration after the enrolment failed; retrying", "error", err, "retry_in", retry)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retry):
		}
		retry = min(retry*2, retryMax)
	}
}
```

In `standalone/standalone.go`:

1. `storageIntegrityRoleRegistration` (`:646-649`):

```go
type storageIntegrityRoleRegistration interface {
	valid() bool
	register(context.Context) error
	// deferred reports whether this indexer is not enrolled in the arbiter's
	// committed si_indexers yet (housegate spec 2026-10-10 §10). It is read
	// once, after the registry follower's first answer; an error is an
	// enrolment that names another node id or signer than this node's.
	deferred() (bool, error)
	// registerWhenEnrolled waits for the enrolment, registers with backoff,
	// then holds until ctx ends; it runs as an owned startup task.
	registerWhenEnrolled(context.Context) error
}
```

2. `snodeStorageIntegrityRoleLifecycle` (`:709-711`) gains

```go
type snodeStorageIntegrityRoleLifecycle struct {
	role *snode.Role
	// enrollment gates registration on this indexer's enrolment; nil
	// registers at once, as before multi-source networks.
	enrollment storageIntegrityEnrollment
	logger     *slog.Logger
}
```

and after `register` (`:725-727`) — the registration still goes through `register`, so `r.role.Register` keeps its single reference that `TestStorageIntegrityProductionAdaptersCallConcreteDependencies` audits:

```go
func (r *snodeStorageIntegrityRoleLifecycle) deferred() (bool, error) {
	if r.enrollment == nil {
		return false, nil
	}
	enrolled, err := r.enrollment.enrolled()
	return !enrolled, err
}

func (r *snodeStorageIntegrityRoleLifecycle) registerWhenEnrolled(ctx context.Context) error {
	return storageIntegrityDeferredRegistration{enrollment: r.enrollment, register: r.register, logger: r.logger}.run(ctx)
}
```

3. `newStorageIntegrityBootDeps` (`:729-740`): add the parameter `enrollment storageIntegrityEnrollment` after `logger *slog.Logger`, and build `roleLifecycle := &snodeStorageIntegrityRoleLifecycle{role: role, enrollment: enrollment, logger: logger}`.

4. `start` (`:1025-1029`) becomes:

```go
	// Final activation. An indexer that is not enrolled yet starts without
	// its SNode registered (housegate spec 2026-10-10 §10): its tables answer
	// ordinary until the enrolment commits, so an owned task registers it
	// then instead of failing startup.
	if p.authorization.activation != nil {
		deferRegistration, err := p.authorization.activation.deferred()
		if err != nil {
			return rollback(fmt.Errorf("storage-integrity bootstrap step %q: %w", "check-enrollment", err))
		}
		if deferRegistration {
			if err := tx.addTask("storage integrity snode registration", p.authorization.activation.registerWhenEnrolled); err != nil {
				return rollback(fmt.Errorf("storage-integrity bootstrap step %q: %w", "defer-register-role", err))
			}
		} else if err := p.authorization.activation.register(tx.Context()); err != nil {
			return rollback(fmt.Errorf("storage-integrity bootstrap step %q: %w", "register-role", err))
		}
	}
```

`tx.addTask` runs the task from commit with the producer context, so shutdown cancels and joins it; `storageIntegrityPermitLifecyclePositions` still finds exactly one `p.authorization.activation.register` call, before `tx.commit`. `deferred()` reads a real answer, never the follower's pre-Ready zero view (which reports the registry disabled and would register at once): `start` calls it after `runner(tx)`, inside which `siTableState.begin` (`standalone.go:514-518`) started the follower and waited for its first answer (`storage_integrity_table_state.go:161-176`, `dataplane.WaitReady`); every SI-enabled Run builds `siTableState`.

5. Run's SI block. Before `roleLogger := newSlogLogger(logger)` (`:311`):

```go
			// The SNode serves this indexer's tables and speaks with its key
			// (housegate spec 2026-10-10 D6, §8): the on-chain id the storage RPC
			// reports as self_indexer_id and the PRIVATE_KEY the node already
			// signs its transactions with.
			siIndexerID, err := storageIntegrityIndexerID(env.IndexerRuntime)
			if err != nil {
				return err
			}
			claimSigner, err := storageIntegrityClaimSigner(&cfg.Node.PrivateKey)
			if err != nil {
				return err
			}
```

after the `schemaSets` error check (`:293-295`):

```go
			// The SNode signs with a configured genesis snapshot id as given;
			// where genesis tables exist it must be the arbiter's derivation
			// (CONTRACT §3b.3).
			if err := checkStorageIntegrityGenesisSnapshotID(si.SNode, schemaSets.snode); err != nil {
				return err
			}
```

in the `snode.Config` literal (`:317-332`) add `IndexerID: siIndexerID,` after `NodeID` and `GenesisSnapshotID: si.SNode.GenesisSnapshotID,` after `NetworkID` (empty on the founding indexer lets the SNode derive it; CONTRACT §3b.3), and in the `snode.Deps` literal (`:333-339`) add `ClaimSigner: claimSigner,` — `IndexerID` and `ClaimSigner` are always set together (CONTRACT §3b.6, §3b.7: owner filtering, owner-scoped reads, signing and `signed_claims_v1` happen only with the signer); before `bootDeps := newStorageIntegrityBootDeps(` (`:350`):

```go
			// A node that starts before its enrolment commits keeps running and
			// registers once it is enrolled (spec 2026-10-10 §10).
			siEnrollment := followerStorageIntegrityEnrollment{
				registry:  tablestate.FromFollower(siRegistry),
				indexerID: siIndexerID,
				nodeID:    si.SNode.NodeID,
				signer:    claimSigner.Address(),
			}
```

and pass `siEnrollment,` as the tenth argument after `roleLogger,` (`:359`). Add the import `"compute-network-node/storageintegrityadapter/tablestate"`.

6. Update the source audits and the other constructor callers in the same change:
- `standalone/storage_integrity_bootstrap_test.go:667` and `:1317`: `require.Len(…Args, 9)` → `10`; after `:1326` add `require.True(t, identifierExpression(bootDepsCall.Args[9], "siEnrollment"))`.
- `TestStorageIntegrityListenerPermitStartOrdersRunnerReadinessActivationAndCommit` (`:740-744`): the `registration` literal becomes the new block, byte for byte:

```go
	registration := []byte("\tif p.authorization.activation != nil {\n" +
		"\t\tdeferRegistration, err := p.authorization.activation.deferred()\n" +
		"\t\tif err != nil {\n" +
		"\t\t\treturn rollback(fmt.Errorf(\"storage-integrity bootstrap step %q: %w\", \"check-enrollment\", err))\n" +
		"\t\t}\n" +
		"\t\tif deferRegistration {\n" +
		"\t\t\tif err := tx.addTask(\"storage integrity snode registration\", p.authorization.activation.registerWhenEnrolled); err != nil {\n" +
		"\t\t\t\treturn rollback(fmt.Errorf(\"storage-integrity bootstrap step %q: %w\", \"defer-register-role\", err))\n" +
		"\t\t\t}\n" +
		"\t\t} else if err := p.authorization.activation.register(tx.Context()); err != nil {\n" +
		"\t\t\treturn rollback(fmt.Errorf(\"storage-integrity bootstrap step %q: %w\", \"register-role\", err))\n" +
		"\t\t}\n" +
		"\t}\n")
```

- Tenth argument `nil` at `standalone/storage_integrity_genesis_test.go:593` and `:595`, `standalone/storage_integrity_genesis_ch_test.go:117`, `standalone/storage_integrity_acceptance_ch_test.go:145` and `standalone/storage_integrity_drift_ch_test.go:134` (the CH tests compile into `//standalone:standalone_test` even when skipped). A nil enrollment registers at once, as those tests expect.

If another source audit in `storage_integrity_bootstrap_test.go` fails on the new statements, change only the count or literal it names, and say why in the commit message.

Run: `cd "$W" && go test ./standalone/ -v 2>&1 | tail -40`
Expected: PASS, including every `TestRun…` / `TestStorageIntegrity…Permit…` audit and the unchanged `TestPrepareStorageIntegrityListenerPermitUsesRealConfigPredicate` (`["prepare", "run-ready", "register"]`: the recorder's `deferred()` answers false by default).

- [ ] **Step 9: Commit**

```bash
cd "$W" && bazel run //:gazelle && bazel build //... && bazel test //standalone:standalone_test //storageintegrityadapter/tablestate:tablestate_test //config:config_test
cd "$W" && git add -A && git commit -m "feat(storage-integrity): sign SNode messages with the indexer key and defer registration until enrolment

The embedded SNode gets this node's on-chain indexer id (its owner
filter), a claim signer built from PRIVATE_KEY (housegate spec
2026-10-10 D6, §8) and the configured genesis snapshot id, which startup
checks against the arbiter's derivation wherever genesis tables exist
(CONTRACT §3b.3). A node whose indexer is not in the arbiter's
committed si_indexers yet no longer rolls back its startup transaction:
it starts with the SNode unregistered and an owned task registers it
once the registry carries its entry, retrying with backoff (§10). An
enrolment naming another node id or signer refuses startup. Before
si_indexers exists the founding indexer counts as enrolled, so devnet2's
indexer-a registers synchronously as before.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 10: Write the failing tests for the enrolment-statement CLI**

The command never connects to the arbiter (CONTRACT §3b.3): `network_id`, `snode_node_id` and `genesis_snapshot_id` come from `storage_integrity.snode`, and the indexer id from a read-only `IndexerRegistry.getIndexerBySigner` call. A wrong configured genesis snapshot id fails closed later, because the arbiter verifies the statement against its own genesis snapshot id when the enrolment update is applied.

Create `standalone/storage_integrity_enrollment_statement_test.go`:

```go
package standalone

import (
	"bytes"
	"crypto/ecdsa"
	"strings"
	"testing"

	"compute-network-node/config"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/stretchr/testify/require"
)

func TestWriteStorageIntegrityEnrollmentStatementPrintsOnlyTheJWS(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	signer, err := storageIntegrityClaimSigner(key)
	require.NoError(t, err)
	stmt := authority.SNodeEnrollmentStatement{
		NetworkID:         "devnet2",
		GenesisSnapshotID: "0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06",
		IndexerID:         1,
		SNodeNodeID:       "snode-b",
	}
	var out bytes.Buffer
	require.NoError(t, writeStorageIntegrityEnrollmentStatement(&out, signer, stmt))
	jws, ok := strings.CutSuffix(out.String(), "\n")
	require.True(t, ok, "the JWS ends with one newline")
	require.NotContains(t, jws, "\n", "nothing but the JWS reaches stdout")
	require.Len(t, strings.Split(jws, "."), 3, "a compact JWS")
	require.NoError(t, authority.VerifySNodeEnrollment(stmt, jws, signer.Address()))
	other := stmt
	other.IndexerID = 0
	require.Error(t, authority.VerifySNodeEnrollment(other, jws, signer.Address()), "the statement binds the indexer id")
	other = stmt
	other.GenesisSnapshotID = "0x" + strings.Repeat("0", 64)
	require.Error(t, authority.VerifySNodeEnrollment(other, jws, signer.Address()), "the statement binds the genesis snapshot id")
}

// Every refusal comes before the node env dials anything: the statement needs
// the indexer key, the SNode's identity and the configured genesis snapshot
// id, never the arbiter (CONTRACT §3b.3).
func TestPrintStorageIntegrityEnrollmentStatementRefusesBeforeAnyRead(t *testing.T) {
	key, err := crypto.GenerateKey()
	require.NoError(t, err)
	indexer := func(genesisSnapshotID string, key ecdsa.PrivateKey) *config.Config {
		return &config.Config{
			// An RPC nothing listens on: reaching NewNodeEnv would fail with a
			// dial error that names none of the expected fields.
			Node: config.NodeConfig{NodeType: string(config.NodeTypeIndexer), EthereumRPC: "http://127.0.0.1:1", PrivateKey: key},
			StorageIntegrity: config.StorageIntegrityConfig{Enabled: true, SNode: config.StorageIntegritySNode{
				NodeID: "snode-b", NetworkID: "devnet2", GenesisSnapshotID: genesisSnapshotID,
			}},
		}
	}
	const genesis = "0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06"
	for name, tc := range map[string]struct {
		cfg  *config.Config
		want string
	}{
		"no config":              {nil, "requires a config"},
		"observer":               {&config.Config{Node: config.NodeConfig{NodeType: string(config.NodeTypeObserver)}, StorageIntegrity: config.StorageIntegrityConfig{Enabled: true}}, "node.node_type: indexer"},
		"SI disabled":            {&config.Config{Node: config.NodeConfig{NodeType: string(config.NodeTypeIndexer)}}, "storage_integrity.enabled"},
		"no genesis snapshot id": {indexer("", *key), "storage_integrity.snode.genesis_snapshot_id"},
		"no indexer key":         {indexer(genesis, ecdsa.PrivateKey{}), "PRIVATE_KEY"},
	} {
		var out bytes.Buffer
		require.ErrorContains(t, PrintStorageIntegrityEnrollmentStatement(t.Context(), tc.cfg, &out), tc.want, name)
		require.Empty(t, out.String(), "%s: a refusal writes nothing to stdout", name)
	}
}
```

Create `commands/storage_integrity_test.go`:

```go
package commands

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStorageIntegrityCommandOffersTheEnrollmentStatement(t *testing.T) {
	path := "sentio-node.yaml"
	cmd := NewStorageIntegrityCommand(&path)
	require.Equal(t, "storage-integrity", cmd.Name())
	sub, rest, err := cmd.Find([]string{"enrollment-statement"})
	require.NoError(t, err)
	require.Empty(t, rest)
	require.Equal(t, "enrollment-statement", sub.Name())
	require.Error(t, sub.Args(sub, []string{"extra"}), "the statement takes no positional arguments")
	require.NoError(t, sub.Args(sub, nil))
}
```

Run: `cd "$W" && go test ./standalone/ -run 'EnrollmentStatement' -v; go test ./commands/ -v`
Expected: FAIL to compile — `undefined: writeStorageIntegrityEnrollmentStatement`, `PrintStorageIntegrityEnrollmentStatement`, `NewStorageIntegrityCommand`.

- [ ] **Step 11: Implement the CLI**

Create `standalone/storage_integrity_enrollment_statement.go`:

```go
package standalone

import (
	"context"
	"errors"
	"fmt"
	"io"

	"compute-network-node/common"
	"compute-network-node/config"
	"sentioxyz/sentio-core/common/log"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/sentioxyz/arbiter-core/authority"
)

// PrintStorageIntegrityEnrollmentStatement is `sentio-node storage-integrity
// enrollment-statement`: it signs this indexer's SNode enrolment statement
// (housegate spec 2026-10-10 §6.2, §10) with the indexer key and writes only
// the compact JWS and a newline to out; logs go to stderr. It never connects
// to the arbiter: network_id, snode_node_id and genesis_snapshot_id come from
// storage_integrity.snode (CONTRACT §3b.3), and the indexer id from a
// read-only IndexerRegistry.getIndexerBySigner call (the command never
// registers an indexer, as EnsureRegistered would).
func PrintStorageIntegrityEnrollmentStatement(ctx context.Context, cfg *config.Config, out io.Writer) error {
	switch {
	case cfg == nil:
		return errors.New("enrollment-statement requires a config")
	case !cfg.Node.IsIndexerNode():
		return errors.New("enrollment-statement requires an indexer node config (node.node_type: indexer): the statement is signed with the indexer key")
	case !cfg.StorageIntegrity.Enabled:
		return errors.New("enrollment-statement requires storage_integrity.enabled: the statement binds storage_integrity.snode.node_id and network_id")
	case cfg.StorageIntegrity.SNode.GenesisSnapshotID == "":
		return errors.New("enrollment-statement requires storage_integrity.snode.genesis_snapshot_id: the statement binds the network's genesis snapshot id (arbiter-admin consensus show prints it as genesis_snapshot_id)")
	}
	si := cfg.StorageIntegrity
	signer, err := storageIntegrityClaimSigner(&cfg.Node.PrivateKey)
	if err != nil {
		return err
	}
	ctx, logger := log.FromContext(ctx, "component", "storage-integrity-enrollment")
	env, err := common.NewNodeEnv(ctx, &cfg.Node)
	if err != nil {
		return fmt.Errorf("node env: %w", err)
	}
	defer env.Finalize()
	indexer, err := env.IndexerRegistryContract.GetIndexerBySigner(&bind.CallOpts{Context: ctx}, env.IndexerRuntime.SignerAddress)
	if err != nil {
		return fmt.Errorf("indexer of signer %s: %w", env.IndexerRuntime.SignerAddress.Hex(), common.DecodeContractError(err))
	}
	if indexer.Id == nil || !indexer.Id.IsUint64() {
		return fmt.Errorf("indexer of signer %s has id %v, not a uint64", env.IndexerRuntime.SignerAddress.Hex(), indexer.Id)
	}
	stmt := authority.SNodeEnrollmentStatement{
		NetworkID:         si.SNode.NetworkID,
		GenesisSnapshotID: si.SNode.GenesisSnapshotID,
		IndexerID:         indexer.Id.Uint64(),
		SNodeNodeID:       si.SNode.NodeID,
	}
	logger.Infow("signing the SNode enrolment statement",
		"network_id", stmt.NetworkID, "genesis_snapshot_id", stmt.GenesisSnapshotID,
		"indexer_id", stmt.IndexerID, "snode_node_id", stmt.SNodeNodeID, "signer", signer.Address())
	return writeStorageIntegrityEnrollmentStatement(out, signer, stmt)
}

// writeStorageIntegrityEnrollmentStatement signs stmt, checks that it verifies
// under the signer's address as the arbiter will check it, and writes the
// compact JWS and a newline to out.
func writeStorageIntegrityEnrollmentStatement(out io.Writer, signer *authority.Signer, stmt authority.SNodeEnrollmentStatement) error {
	jws, err := signer.SignSNodeEnrollment(stmt)
	if err != nil {
		return fmt.Errorf("sign the SNode enrolment statement: %w", err)
	}
	if err := authority.VerifySNodeEnrollment(stmt, jws, signer.Address()); err != nil {
		return fmt.Errorf("the signed SNode enrolment statement does not verify under %s: %w", signer.Address(), err)
	}
	_, err = fmt.Fprintln(out, jws)
	return err
}
```

`config.LoadConfig` already runs `StorageIntegrityConfig.Validate` (`config/config.go:342`), so a loaded SI config has `network_id` and `node_id` and a well-formed genesis snapshot id when it is set; `SignSNodeEnrollment` refuses an empty field as a backstop. `NewNodeEnv` dials the Ethereum RPC, resolves the AddressBook and pings Redis (`common/env.go:89-233`) but submits nothing.

Create `commands/storage_integrity.go`:

```go
package commands

import (
	"context"

	"compute-network-node/config"
	"compute-network-node/standalone"

	"github.com/spf13/cobra"
)

// NewStorageIntegrityCommand groups the storage-integrity operator commands.
func NewStorageIntegrityCommand(configPath *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "storage-integrity",
		Short: "Storage-integrity operator commands",
	}
	cmd.AddCommand(newEnrollmentStatementCommand(configPath))
	return cmd
}

// newEnrollmentStatementCommand prints this indexer's signed SNode enrolment
// statement (housegate spec 2026-10-10 §6.2, §10): the compact JWS alone on
// stdout, for arbiter-admin consensus update --si-indexer enrollment=<file>.
func newEnrollmentStatementCommand(configPath *string) *cobra.Command {
	return &cobra.Command{
		Use:   "enrollment-statement",
		Short: "Sign this indexer's SNode enrolment statement with the indexer key and print the JWS",
		Long: "Signs {network_id, genesis_snapshot_id, indexer_id, snode_node_id} under purpose " +
			"arbiter-snode-enrollment-v1 with the indexer key (PRIVATE_KEY). network_id, snode_node_id and " +
			"genesis_snapshot_id come from storage_integrity.snode, indexer_id from the IndexerRegistry entry " +
			"of the key's signer; the arbiter is never contacted. Only the JWS is written to stdout; logs go " +
			"to stderr.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			cfg, err := config.LoadConfig(*configPath)
			if err != nil {
				return err
			}
			return standalone.PrintStorageIntegrityEnrollmentStatement(ctx, cfg, cmd.OutOrStdout())
		},
	}
}
```

In `main.go` add `commands.NewStorageIntegrityCommand(&configPath),` after `commands.NewDeclareTableSchemasCommand(&configPath),` (`:52`). The command inherits the root's persistent `--config-path` flag (`main.go:42`), like every other subcommand.

Run: `cd "$W" && go test ./standalone/ -run 'EnrollmentStatement' -v && go test ./commands/ -v && bazel run //:gazelle && bazel run //:compute-network-node -- storage-integrity enrollment-statement --help`
Expected: PASS; the help text shows `Usage: sentio-node storage-integrity enrollment-statement [flags]` and the global `--config-path` flag.

```bash
cd "$W" && git add -A && git commit -m "feat(storage-integrity): enrollment-statement CLI

sentio-node --config-path <config> storage-integrity enrollment-statement
signs {network_id, genesis_snapshot_id, indexer_id, snode_node_id} with
the indexer key under purpose arbiter-snode-enrollment-v1 and prints the
compact JWS alone on stdout (housegate spec 2026-10-10 §6.2, §10). The
genesis snapshot id comes from storage_integrity.snode.genesis_snapshot_id
and the indexer id from IndexerRegistry.getIndexerBySigner (read-only);
the command never contacts the arbiter.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 12: README and full verification**

`README.md`:
- `:84`: after `    table_ids: ["orders.t"]` add `    genesis_snapshot_id: "0x..."  # the network's genesis snapshot id; required when table_ids is empty`.
- `:88`: replace the opening "`storage_integrity.snode.table_ids` is the genesis table set: the tables served while the arbiter's table registry is disabled, and the only ones whose schemas startup loads." with "`storage_integrity.snode.table_ids` is the genesis table set this indexer owns: the tables served while the arbiter's table registry is disabled, and the only ones whose schemas startup loads. It is empty for an indexer that owns no genesis table (every SI indexer of a multi-source network but the founding one, housegate spec 2026-10-10 §10); such an indexer sets `genesis_snapshot_id` to the network's genesis snapshot id (`arbiter-admin consensus show` prints it as `genesis_snapshot_id`) and `schema_root` to the empty-set root `0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59`, and an empty set with a disabled registry refuses startup. The SNode binds `genesis_snapshot_id` into every signed message; the founding indexer may leave it empty (the SNode derives it from the genesis tables) or set it, in which case startup checks it against that derivation."
- `:90`: replace "`Role.Register` remains the final fallible activation step." with "`Role.Register` remains the final fallible activation step, except before this indexer's enrolment: once the arbiter has committed `si_indexers`, an indexer without an entry starts with its SNode unregistered (its tables answer `ordinary`), logs `storage-integrity SNode registration deferred: …`, and an owned task registers it when the registry version carrying its entry arrives, retrying with backoff and logging `storage-integrity SNode registered after its enrolment committed`. An enrolment that names another `snode_node_id` or signer than this node's refuses startup. The SNode signs its messages with the indexer key (`PRIVATE_KEY`) and serves only this indexer's tables."
- Before `### Dynamic storage-integrity table set` (`:98`) add a subsection at the same level (four-backtick fence because it contains a code block):

````markdown
### Storage-integrity enrolment

An indexer joins a network's storage-integrity layer through an authority-signed consensus update that appends its `si_indexers` entry; the update carries the SNode enrolment statement, signed by the indexer key (housegate spec 2026-10-10 §6.2). Print it from the indexer's own config:

```bash
/sentio-node --config-path <config> storage-integrity enrollment-statement > enrollment.jws
```

The command never contacts the arbiter. It reads this indexer's id from `IndexerRegistry.getIndexerBySigner` (read-only; it never registers an indexer) and takes `network_id`, `node_id` and `genesis_snapshot_id` from `storage_integrity.snode`, signs `{network_id, genesis_snapshot_id, indexer_id, snode_node_id}` with purpose `arbiter-snode-enrollment-v1`, checks that it verifies under the key's address, and writes only the compact JWS and a newline to stdout; logs go to stderr. It needs `PRIVATE_KEY`, the node's Ethereum RPC, Redis and a configured `storage_integrity.snode.genesis_snapshot_id`, which the founding indexer must set too before it prints its own statement. The arbiter checks the statement against its own network id and genesis snapshot id when it applies the enrolment update, so a statement signed over a wrong value is refused there.
````

Run: `cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go'))" && bazel run //:gazelle && bazel build //... && bazel test //... --test_output=errors`
Expected: no unformatted file; all PASS (the CH-bound tests skip without `SENTIO_SI_CH_E2E`; the PR's `integration-clickhouse` job runs them).

```bash
cd "$W" && git add -A && git commit -m "docs(readme): enrolment, genesis snapshot id, empty genesis sets and deferred SNode registration

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 15: sentio-node — owner-aware table state, enrolment in the info RPC, schema network-id check, release SN_COMMIT (S1)

The table state becomes owner-aware (spec §10 "Governance predicate"): a database is governed iff this indexer hosts it, this indexer is enrolled and it is not a processor database; a recorded registry key is answered from the registry only when its incarnation's owner is this indexer (a nil owner is the founding indexer's); every other recorded key is Ordinary; default deny and the Legacy fallback read this indexer's own seed and activation block. `sentio_getStorageIntegrityInfo` reports `si_indexer_id` as this indexer's id while it is enrolled (else null) and a new `enrolled` flag; the declaration-compensation loop uses this indexer's own enrolment and activation block. An SI node refuses to start when its schema declarer would hash under another network id than the arbiter's. Then the sentio-node release (`SN_COMMIT`, `SN_DIGEST`) that carries Tasks 14 and 15.

Pre-activation behaviour is unchanged: with no `si_indexers`, `IndexerView` makes the founding indexer (and only it) enrolled with the registry's own `Seeded` and `ActivationBlock`, every nil owner is the founding indexer's, so devnet2's indexer-a (id 0 = founding) answers exactly as before, `si_indexer_id` stays the founding id there, and the compensator keeps the registry's activation block.

**Files:**
- Modify: `storageintegrityadapter/tablestate/model.go` (`Incarnation` `:32-43`, `Inputs` `:111-125`, new `Registry.Owner`, `Inputs.self`), `storageintegrityadapter/tablestate/arbitercore.go` (incarnation mapping `:41-53`), `storageintegrityadapter/tablestate/snapshot.go` (`build` `:40-115`, `evaluate` `:117-155`), `storageintegrityadapter/tablestate/state.go` (`Config` `:43-56`, `New` `:83-128`, `Refresh` `:160-166`), `storageintegrityadapter/tablestate/arbitercore_test.go`, `storageintegrityadapter/tablestate/state_test.go`
- Modify: `standalone/storage_integrity_table_state.go` (deps `:32-52`, runtime `:57-66`, constructor `:68-105`, `registryActivation` `:107-120`), `standalone/storage_integrity_info.go` (`:49-69`), `standalone/storage_integrity_table_state_test.go` (`testTableStateDeps` `:94-103`, `TestRegistryActivationReadsTheFollowerView` `:153-163`, `TestStorageIntegrityTableStateServesTheInfoMethod` `:247-304`)
- Modify: `rpc/storage_integrity_info.go` (`:8-24`), `rpc/storage_integrity_info_test.go` (`:45-77`)
- Modify: `standalone/schema_registry.go` (append), `standalone/schema_registry_test.go` (append), `standalone/standalone.go` (Run: the SI block `:233-236`; the `storageIntegrityTableStateDeps` literal `:387-399`)
- Modify: `README.md` (`:96`, `:99-104`, `:108`)

**Interfaces:**
- Consumes: Task 14 `tablestate.SIIndexer`, `Registry.SIIndexers`, `Registry.SeededIndexers`, `IndexerView`, `Registry.Indexer`, Run's `siIndexerID`; `CORE_TAG` `wire.TableIncarnation.OwnerIndexerID *uint64`. Existing: `tablestate.Registry.liveByKey`, `content.fallback`, fakes `fakeRegistry`, `fakeReadiness`, `fakeChain`, helpers `chainInc`, `schemaFor`, `schemaJSON`, `newTestState`, `testLogger`, `staticFollower`, `newScriptedRegistryFollower`, `activeTenantRegistry`, `testTableStateDeps`, `fixedStorageIntegrityInfo`, `getInfo`, `schemaDeclarationConfigs`.
- Produces:
  - `tablestate.Incarnation.OwnerIndexerID *uint64`; `func (r tablestate.Registry) Owner(inc tablestate.Incarnation) uint64`; `tablestate.Inputs.Self *uint64`; `tablestate.Config.Self *uint64` (nil serves the founding indexer); unexported `func (in Inputs) self() uint64`, `cloneOwner(*uint64) *uint64`; `evaluate` gains a `me IndexerView` parameter.
  - `rpc.StorageIntegrityInfo.Enrolled bool` (`json:"enrolled"`); `si_indexer_id` = self while enrolled, else null.
  - `standalone`: `storageIntegrityTableStateDeps.self uint64`, `storageIntegrityTableStateRuntime.self uint64`, `registryActivation(registry tablestate.RegistrySource, self uint64) func() (bool, uint64)` (changed signature); `checkStorageIntegritySchemaNetworkID(networkID string, declarations []database_registry.SchemaDeclarationConfig) error` with the startup refusal `storage integrity: the schema declarer hashes table schemas under network id %q, but the arbiter network id (storage_integrity.snode.network_id) is %q; …`.
  - Release `SN_COMMIT`, `SN_DIGEST`.

- [ ] **Step 1: Contract check**

Run: `cd "$W" && git log --oneline -6 && go doc github.com/sentioxyz/arbiter-core/wire TableIncarnation | grep -n OwnerIndexerID`
Expected: Task 14's commits on top of `origin/main`, and `OwnerIndexerID *uint64`. Stop otherwise.

- [ ] **Step 2: Write the failing table-state tests**

Append to `storageintegrityadapter/tablestate/arbitercore_test.go`:

```go
// TestFromFollowerMapsTheIncarnationOwner covers housegate spec 2026-10-10 D4:
// the view carries each incarnation's owner, nil for one recorded before the
// signed-claims activation, and never aliases the follower's snapshot.
func TestFromFollowerMapsTheIncarnationOwner(t *testing.T) {
	owner := uint64(1)
	snapshot := wire.TableRegistrySnapshot{Version: 5, Incarnations: []wire.TableIncarnation{
		{Seq: 1, DatabaseID: "devnet101", TableID: "t", Origin: wire.TableOriginChain, Status: wire.TableStatusActive},
		{Seq: 2, DatabaseID: "devuser1", TableID: "t", Origin: wire.TableOriginChain, Status: wire.TableStatusActive, OwnerIndexerID: &owner},
	}}
	view, enabled := FromFollower(staticFollower{enabled: true, snapshot: snapshot}).View()
	require.True(t, enabled)
	require.Nil(t, view.Incarnations[0].OwnerIndexerID)
	require.NotNil(t, view.Incarnations[1].OwnerIndexerID)
	require.EqualValues(t, 1, *view.Incarnations[1].OwnerIndexerID)
	owner = 9
	require.EqualValues(t, 1, *view.Incarnations[1].OwnerIndexerID, "the view must not alias the follower's shared snapshot")
}
```

Append to `storageintegrityadapter/tablestate/state_test.go`:

```go
func stateFor(t *testing.T, self uint64, reg Registry, ready *fakeReadiness, chain *fakeChain) sitable.Snapshot {
	t.Helper()
	state, err := New(Config{Registry: newFakeRegistry(true, reg), Readiness: ready, Chain: chain, NetworkID: testNetwork, Self: &self, Logger: testLogger(t)})
	require.NoError(t, err)
	require.NoError(t, state.Refresh(context.Background()))
	return state.Current()
}

func activeIDs(snap sitable.Snapshot) []string {
	var ids []string
	for _, table := range snap.Active() {
		ids = append(ids, table.ID)
	}
	return ids
}

// Spec 2026-10-10 §10: two enrolled indexers share one registry. Each host
// governs only its own databases and answers from the registry only the
// incarnations it owns; the peer's tables are Ordinary, so its rewriter never
// turns a read of a peer's SI table into a read of its own empty hg_safe.
func TestLookupTwoIndexerRegistryFromEachHost(t *testing.T) {
	const (
		indexerA, indexerB       = uint64(0), uint64(1)
		aDatabase, bDatabase     = "devnet101", "devuser1"
		activationA, activationB = uint64(1000), uint64(2000)
	)
	owned := func(seq uint64, database, table string, status IncarnationStatus, owner uint64) Incarnation {
		schema := schemaFor(database + "." + table)
		return Incarnation{Seq: seq, DatabaseID: database, TableID: table, Origin: OriginChain, Status: status,
			SchemaJSON: schemaJSON(t, schema), SchemaHash: payloadexec.TableSchemaHash(testNetwork, schema), OwnerIndexerID: &owner}
	}
	registry := func(seeded ...uint64) Registry {
		return Registry{
			Version: 40, Seeded: true, SIIndexerID: indexerA, ActivationBlock: activationA,
			SIIndexers: []SIIndexer{
				{IndexerID: indexerA, ActivationBlock: activationA, Signer: "0xaa", SNodeNodeID: "snode-1"},
				{IndexerID: indexerB, ActivationBlock: activationB, Signer: "0xbb", SNodeNodeID: "snode-b"},
			},
			SeededIndexers: seeded,
			Incarnations: []Incarnation{
				owned(1, aDatabase, "a_active", IncarnationActive, indexerA),
				owned(2, aDatabase, "a_pending", IncarnationPending, indexerA),
				owned(3, bDatabase, "b_active", IncarnationActive, indexerB),
				owned(4, bDatabase, "b_pending", IncarnationPending, indexerB),
			},
		}
	}
	chain := &fakeChain{dbs: map[string]ChainDatabase{
		aDatabase: {IndexerID: indexerA, Tables: map[string]ChainTable{"a_active": {CreatedBlock: 1500}, "a_pending": {CreatedBlock: 1500}, "a_new": {CreatedBlock: 1500}}},
		bDatabase: {IndexerID: indexerB, Tables: map[string]ChainTable{"b_active": {CreatedBlock: 2500}, "b_pending": {CreatedBlock: 2500}, "b_new": {CreatedBlock: 2500}, "b_old": {CreatedBlock: 1500}}},
	}}
	ready := &fakeReadiness{}
	ready.set(aDatabase+".a_active", true)
	ready.set(bDatabase+".b_active", true)
	check := func(host string, snap sitable.Snapshot, want map[string]sitable.Status) {
		for id, status := range want {
			database, table, _ := strings.Cut(id, ".")
			require.Equalf(t, status, snap.Lookup(database, table).Status, "host %s, %s", host, id)
		}
	}

	fromA := stateFor(t, indexerA, registry(indexerA), ready, chain)
	check("A", fromA, map[string]sitable.Status{
		aDatabase + ".a_active": sitable.Active, aDatabase + ".a_pending": sitable.Pending, aDatabase + ".a_new": sitable.Pending,
		bDatabase + ".b_active": sitable.Ordinary, bDatabase + ".b_pending": sitable.Ordinary,
		bDatabase + ".b_new": sitable.Ordinary, bDatabase + ".b_old": sitable.Ordinary,
	})
	require.Equal(t, []string{aDatabase + ".a_active"}, activeIDs(fromA))

	fromB := stateFor(t, indexerB, registry(indexerA), ready, chain)
	check("B", fromB, map[string]sitable.Status{
		aDatabase + ".a_active": sitable.Ordinary, aDatabase + ".a_pending": sitable.Ordinary, aDatabase + ".a_new": sitable.Ordinary,
		bDatabase + ".b_active": sitable.Active, bDatabase + ".b_pending": sitable.Pending, bDatabase + ".b_new": sitable.Pending,
		bDatabase + ".b_old": sitable.Ordinary, // before B's own activation block, B's seed not committed
	})
	require.Equal(t, []string{bDatabase + ".b_active"}, activeIDs(fromB))

	seededB := stateFor(t, indexerB, registry(indexerA, indexerB), ready, chain)
	require.Equal(t, sitable.Pending, seededB.Lookup(bDatabase, "b_old").Status, "B's own seed, not A's, ends B's Legacy window")
}

// The founding indexer owns every incarnation recorded before the signed-
// claims activation: a nil owner and an explicit owner 0 are one owner, on its
// host and on every other host.
func TestLookupNilOwnerIsTheFoundingIndexer(t *testing.T) {
	const founding, peer = uint64(0), uint64(1)
	nilOwner := chainInc(t, 1, "t", IncarnationActive)
	explicit := nilOwner
	zero := founding
	explicit.OwnerIndexerID = &zero
	ready := &fakeReadiness{}
	ready.set(siDatabase+".t", true)
	chain := &fakeChain{dbs: map[string]ChainDatabase{siDatabase: {IndexerID: founding, Tables: map[string]ChainTable{"t": {CreatedBlock: 1500}}}}}
	for name, reg := range map[string]Registry{
		"before the activation, nil owner": {Version: 1, Seeded: true, SIIndexerID: founding, ActivationBlock: activation, Incarnations: []Incarnation{nilOwner}},
		"after the activation, owner 0": {Version: 2, Seeded: true, SIIndexerID: founding, ActivationBlock: activation,
			SIIndexers: []SIIndexer{
				{IndexerID: founding, ActivationBlock: activation, Signer: "0xaa", SNodeNodeID: "snode-1"},
				{IndexerID: peer, ActivationBlock: 2000, Signer: "0xbb", SNodeNodeID: "snode-b"},
			},
			SeededIndexers: []uint64{founding}, Incarnations: []Incarnation{explicit}},
	} {
		require.Equal(t, sitable.Active, stateFor(t, founding, reg, ready, chain).Lookup(siDatabase, "t").Status, name)
		require.Equal(t, sitable.Ordinary, stateFor(t, peer, reg, ready, chain).Lookup(siDatabase, "t").Status, name)
		state := newTestState(t, newFakeRegistry(true, reg), ready, chain)
		require.NoError(t, state.Refresh(context.Background()))
		require.Equal(t, sitable.Active, state.Current().Lookup(siDatabase, "t").Status, "%s: a nil Self serves the founding indexer", name)
	}
}

// An indexer that is not enrolled governs nothing: every table, recorded or
// not, its own or a peer's, is Ordinary, and nothing is Active, before and
// after the signed-claims activation.
func TestLookupIdleNonEnrolledHostIsOrdinaryEverywhere(t *testing.T) {
	const founding, idle = uint64(0), uint64(1)
	incs := []Incarnation{chainInc(t, 1, "t", IncarnationActive), chainInc(t, 2, "p", IncarnationPending)}
	chain := &fakeChain{dbs: map[string]ChainDatabase{
		siDatabase: {IndexerID: founding, Tables: map[string]ChainTable{"t": {CreatedBlock: 1500}, "p": {CreatedBlock: 1500}}},
		"devuser1": {IndexerID: idle, Tables: map[string]ChainTable{"t": {CreatedBlock: 5000}}},
	}}
	ready := &fakeReadiness{}
	ready.set(siDatabase+".t", true)
	for name, reg := range map[string]Registry{
		"before the activation": {Version: 1, Seeded: true, SIIndexerID: founding, ActivationBlock: activation, Incarnations: incs},
		"after the activation, not enrolled": {Version: 2, Seeded: true, SIIndexerID: founding, ActivationBlock: activation,
			SIIndexers:     []SIIndexer{{IndexerID: founding, ActivationBlock: activation, Signer: "0xaa", SNodeNodeID: "snode-1"}},
			SeededIndexers: []uint64{founding}, Incarnations: incs},
	} {
		snap := stateFor(t, idle, reg, ready, chain)
		for _, key := range [][2]string{{siDatabase, "t"}, {siDatabase, "p"}, {"devuser1", "t"}, {"devuser1", "never_synced"}, {"unknown", "x"}} {
			require.Equalf(t, sitable.Ordinary, snap.Lookup(key[0], key[1]).Status, "%s: %s.%s", name, key[0], key[1])
		}
		require.Empty(t, snap.Active(), name)
	}
}
```

Run: `cd "$W" && go test ./storageintegrityadapter/tablestate/ -run 'IncarnationOwner|TwoIndexer|NilOwner|IdleNonEnrolled' -v`
Expected: FAIL to compile — unknown fields `OwnerIndexerID` (on `Incarnation`) and `Self` (on `Config`).

- [ ] **Step 3: Implement the owner-aware table state**

`model.go` — add to `Incarnation` (after `RefusedReason`, `:42`):

```go
	// OwnerIndexerID is the indexer hosting the table's database at its
	// TableCreated block (housegate spec 2026-10-10 D4); nil on an incarnation
	// recorded before the signed-claims activation, which the founding indexer
	// owns.
	OwnerIndexerID *uint64
```

after `Registry.Indexer` (Task 14):

```go
// Owner is the indexer owning inc: its OwnerIndexerID, or the founding
// indexer for an incarnation recorded before the signed-claims activation.
func (r Registry) Owner(inc Incarnation) uint64 {
	if inc.OwnerIndexerID != nil {
		return *inc.OwnerIndexerID
	}
	return r.SIIndexerID
}
```

add to `Inputs` (after `NetworkID`, `:124`):

```go
	// Self is the indexer this host serves; nil serves the founding indexer
	// (Registry.SIIndexerID), the single-indexer behaviour.
	Self *uint64
```

and after the `Inputs` type:

```go
// self is the indexer this host serves.
func (in Inputs) self() uint64 {
	if in.Self != nil {
		return *in.Self
	}
	return in.Registry.SIIndexerID
}
```

`arbitercore.go` — the incarnation literal (`:42-52`) gains `OwnerIndexerID: cloneOwner(inc.OwnerIndexerID),`, plus:

```go
// cloneOwner copies an owner so the view never aliases the follower's shared
// snapshot.
func cloneOwner(owner *uint64) *uint64 {
	if owner == nil {
		return nil
	}
	v := *owner
	return &v
}
```

`snapshot.go` — replace `build` and its doc comment (`:40-115`) with (the registry-disabled step 1 is unchanged):

```go
// build evaluates design §9.2's Lookup order for this host's indexer
// (housegate spec 2026-10-10 §10): every table of a governed database the
// registry or the chain state names, and every registry key this indexer owns
// whose live incarnation is neither Legacy nor Purged, wherever its database
// is. A key another SI indexer owns is Ordinary here. Tables it does not list
// answer the snapshot's fallback: Pending in a governed database, Ordinary
// everywhere else.
func build(in Inputs) (content, evaluated, []Issue) {
	c := content{governed: map[string]struct{}{}, tables: map[tableKey]sitable.Table{}}
	seen := evaluated{}
	if !in.RegistryEnabled {
		// Step 1: the genesis set is Active and everything else is Ordinary.
		for id, schema := range in.Genesis {
			database, table, _ := strings.Cut(id, ".")
			c.tables[tableKey{database, table}] = sitable.Table{
				ID:         id,
				Status:     sitable.Active,
				Schema:     schema,
				SchemaHash: payloadexec.TableSchemaHash(in.NetworkID, schema),
			}
			seen[id] = sitable.Active
		}
		return c, seen, nil
	}
	c.enabled = true
	self := in.self()
	me := in.Registry.Indexer(self)
	for database, info := range in.Chain {
		// A database is governed iff this indexer hosts it, this indexer is
		// enrolled, and it is not a PROCESSOR database (spec 2026-10-10 §10).
		// The driver creates a processor's tables without declaring a schema,
		// so default deny would stall the processor (design 5a H1); a key the
		// registry records there is still answered from the registry below (H2).
		if me.Enrolled && info.IndexerID == self && !info.Processor {
			c.governed[database] = struct{}{}
		}
	}
	live := in.Registry.liveByKey()
	keys := map[tableKey]*Incarnation{}
	foreign := map[tableKey]struct{}{}
	for _, inc := range live {
		key := tableKey{inc.DatabaseID, inc.TableID}
		if in.Registry.Owner(*inc) != self {
			// Another SI indexer owns it. Answering it from the registry would
			// let this host's rewriter turn a read of a peer's SI table into a
			// read of its own empty hg_safe (spec 2026-10-10 §10).
			foreign[key] = struct{}{}
			continue
		}
		if _, ok := c.governed[inc.DatabaseID]; ok {
			keys[key] = inc
			continue
		}
		// A key this indexer owns is answered from the registry whatever the
		// chain state says about its database: a mirror that is empty, not
		// synced yet or missing the database must not turn a governed table
		// Ordinary (and its writes unsigned), and after DatabaseDeleted drops
		// the database from the mirror its Retiring and Purging tables stay
		// Gone with the schema HouseGate's journal recovery resolves. Only
		// Legacy (Ordinary either way) and Purged (unrecorded, which the chain
		// state decides) are left to the fallback.
		if inc.Status != IncarnationLegacy && inc.Status != IncarnationPurged {
			keys[key] = inc
		}
	}
	for database := range c.governed {
		for table := range in.Chain[database].Tables {
			key := tableKey{database, table}
			if _, ok := keys[key]; ok {
				continue
			}
			if _, ok := foreign[key]; ok {
				continue
			}
			keys[key] = nil // no incarnation at all: step 4
		}
	}
	var issues []Issue
	for key, inc := range keys {
		table, issue := evaluate(in, me, key, inc)
		seen[table.ID] = table.Status
		if table.Status != c.fallback(key.database) {
			c.tables[key] = table
		}
		if issue != nil {
			issues = append(issues, *issue)
		}
	}
	for key := range foreign {
		if _, governed := c.governed[key.database]; !governed {
			continue // the Ordinary fallback answers it
		}
		// Databases never move (D4), so a governed database holds only this
		// indexer's tables; a registry that disagrees is answered Ordinary like
		// every table another SI indexer owns, never default-denied.
		id := sitable.TableID(key.database, key.table)
		c.tables[key] = sitable.Table{ID: id, Status: sitable.Ordinary}
		seen[id] = sitable.Ordinary
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].TableID < issues[j].TableID })
	return c, seen, issues
}
```

and in `evaluate` (`:117-155`) change the signature to `func evaluate(in Inputs, me IndexerView, key tableKey, inc *Incarnation) (sitable.Table, *Issue)` (doc: "…given its live incarnation (nil when the registry has none) and what the registry says about this indexer") and step 4 to:

```go
	// Step 4: no live incarnation (none at all, or the newest is Purged).
	// Default deny and the Legacy fallback read this indexer's own seed and
	// activation block (spec 2026-10-10 §10).
	chainTable, onChain := in.Chain[key.database].Tables[key.table]
	if !me.Seeded && onChain && chainTable.CreatedBlock < me.ActivationBlock {
		return sitable.Table{ID: id, Status: sitable.Ordinary}, nil
	}
	return sitable.Table{ID: id, Status: sitable.Pending}, nil
```

`state.go` — add to `Config` (after `NetworkID`, `:51`):

```go
	// Self is this host's on-chain indexer id (housegate spec 2026-10-10
	// §10): the state governs only the databases it hosts while it is
	// enrolled and answers from the registry only the incarnations it owns.
	// Nil serves the founding indexer (TableRegistryParams.SIIndexerID), the
	// behaviour every host had before multi-source networks; standalone
	// always sets it.
	Self *uint64
```

in `New`, before `initial, _, _ := build(…)` (`:119`), copy it so the state shares nothing with the caller:

```go
	if cfg.Self != nil {
		self := *cfg.Self
		cfg.Self = &self
	}
```

and in `Refresh` add `Self: s.cfg.Self,` to the `Inputs` literal (`:160-166`).

Run: `cd "$W" && go test ./storageintegrityadapter/tablestate/ -v`
Expected: PASS — the new tests and every existing one (`TestLookupMatrix`, `TestLookupRegistryKeysOutsideTheChainState` and the metrics tests run with a nil `Self`, which serves the founding indexer that owns their nil-owner incarnations).

- [ ] **Step 4: Write the failing runtime, RPC and compensator tests**

In `standalone/storage_integrity_table_state_test.go`: add `self: 7,` to `testTableStateDeps` (`:94-103`; indexer 7 is `activeTenantRegistry`'s founding indexer, so every existing expectation holds); add `Enrolled: true` to the `want` of the three registry-enabled cases of `TestStorageIntegrityTableStateServesTheInfoMethod` (`:262-281`); replace `TestRegistryActivationReadsTheFollowerView` (`:153-163`) with:

```go
// TestRegistryActivationReadsThisIndexersEnrollment covers the FB-I1 wiring
// for multi-source networks (housegate spec 2026-10-10 §10): the compensation
// loop runs only while this indexer is enrolled, from its own activation
// block; a follower that has not answered yet reads as disabled.
func TestRegistryActivationReadsThisIndexersEnrollment(t *testing.T) {
	from := func(snapshot wire.TableRegistrySnapshot, enabled bool, self uint64) (bool, uint64) {
		return registryActivation(tablestate.FromFollower(newScriptedRegistryFollower(snapshot, enabled, enabled)), self)()
	}
	enabled, block := from(activeTenantRegistry(t), true, 7)
	require.True(t, enabled)
	require.EqualValues(t, 100, block)
	enabled, block = from(activeTenantRegistry(t), false, 7)
	require.False(t, enabled)
	require.Zero(t, block)
	enabled, _ = from(activeTenantRegistry(t), true, 8)
	require.False(t, enabled, "an indexer that is not enrolled has no SI table to declare")

	activated := activeTenantRegistry(t)
	activated.SIIndexers = []arbiter.SIIndexerEntry{
		{IndexerID: 7, ActivationBlock: 100, Signer: "0x00000000000000000000000000000000000000aa", SNodeNodeID: "snode-1", EnrollmentJWS: "x"},
		{IndexerID: 8, ActivationBlock: 250, Signer: "0x00000000000000000000000000000000000000bb", SNodeNodeID: "snode-b", EnrollmentJWS: "y"},
	}
	enabled, block = from(activated, true, 8)
	require.True(t, enabled)
	require.EqualValues(t, 250, block, "this indexer's own activation block")
}
```

and append:

```go
// TestStorageIntegrityInfoReportsThisIndexersEnrollment pins housegate spec
// 2026-10-10 §10: si_indexer_id is this indexer's id while it is enrolled,
// else null, and enrolled says the same.
func TestStorageIntegrityInfoReportsThisIndexersEnrollment(t *testing.T) {
	founding, other := uint64(7), uint64(8)
	entry := func(id, activation uint64, node string) arbiter.SIIndexerEntry {
		return arbiter.SIIndexerEntry{IndexerID: id, ActivationBlock: activation, Signer: "0x00000000000000000000000000000000000000aa", SNodeNodeID: node, EnrollmentJWS: "x"}
	}
	activated := func(entries ...arbiter.SIIndexerEntry) wire.TableRegistrySnapshot {
		snapshot := activeTenantRegistry(t)
		snapshot.SIIndexers = entries
		return snapshot
	}
	for name, tc := range map[string]struct {
		snapshot     wire.TableRegistrySnapshot
		enabled      bool
		self         uint64
		wantID       *uint64
		wantEnrolled bool
	}{
		"registry disabled":                      {self: 7},
		"founding indexer before the activation": {snapshot: activeTenantRegistry(t), enabled: true, self: 7, wantID: &founding, wantEnrolled: true},
		"another indexer before the activation":  {snapshot: activeTenantRegistry(t), enabled: true, self: 8},
		"not enrolled after the activation":      {snapshot: activated(entry(7, 100, "snode-1")), enabled: true, self: 8},
		"enrolled after the activation":          {snapshot: activated(entry(7, 100, "snode-1"), entry(8, 200, "snode-b")), enabled: true, self: 8, wantID: &other, wantEnrolled: true},
	} {
		t.Run(name, func(t *testing.T) {
			deps := testTableStateDeps(newScriptedRegistryFollower(tc.snapshot, tc.enabled, true))
			deps.self = tc.self
			runtime, err := newStorageIntegrityTableStateRuntime(deps)
			require.NoError(t, err)
			got := runtime.StorageIntegrityInfo()
			require.Equal(t, tc.wantID, got.SIIndexerID)
			require.Equal(t, tc.wantEnrolled, got.Enrolled)
		})
	}
}

// TestRunPassesTheSelfIndexerToTheTableState pins that the table state
// governs the indexer the SNode serves.
func TestRunPassesTheSelfIndexerToTheTableState(t *testing.T) {
	_, runDecl := parseStandaloneRun(t, readStandaloneSource(t))
	var self []ast.Expr
	ast.Inspect(runDecl.Body, func(node ast.Node) bool {
		if literal, ok := node.(*ast.CompositeLit); ok && identifierExpression(literal.Type, "storageIntegrityTableStateDeps") {
			self = append(self, compositeFieldValues(literal, "self")...)
		}
		return true
	})
	require.Len(t, self, 1)
	require.True(t, identifierExpression(self[0], "siIndexerID"))
}
```

(add `"compute-network-node/storageintegrityadapter/tablestate"` to that file's imports). In `rpc/storage_integrity_info_test.go`: add `Enrolled: true,` to the fixed source of `TestStorageIntegrityInfoWireShape` (`:48-52`) and `"enrolled": true,` to its expected map (`:59-65`); add `"enrolled": false,` to the expected map of `TestStorageIntegrityInfoWithoutStorageIntegrity` (`:70-76`).

Run: `cd "$W" && go test ./standalone/ -run 'InfoMethod|ThisIndexersEnrollment|SelfIndexer|CompensatorFollowsTheRegistry' -v; go test ./rpc/ -run 'StorageIntegrityInfo' -v`
Expected: FAIL to compile — unknown field `self` in `storageIntegrityTableStateDeps`, unknown field `Enrolled` in `rpc.StorageIntegrityInfo`, too many arguments to `registryActivation`.

- [ ] **Step 5: Implement the runtime, the RPC field and the compensator**

`rpc/storage_integrity_info.go` (`:12-24`): add after `SelfIndexerID`

```go
	// Enrolled reports whether this indexer has an entry in the arbiter's
	// committed si_indexers (before the signed-claims activation: whether it
	// is the founding indexer). SIIndexerID is this indexer's id exactly when
	// Enrolled (housegate spec 2026-10-10 §10).
	Enrolled bool `json:"enrolled"`
```

and update the type comment: "SIIndexerID is this node's indexer id while it is enrolled, else null."

`standalone/storage_integrity_table_state.go`:
- `storageIntegrityTableStateDeps` (`:32-38`) gains `self uint64 // this node's on-chain indexer id (housegate spec 2026-10-10 §10)`; `storageIntegrityTableStateRuntime` (`:57-66`) gains `self uint64`.
- In `newStorageIntegrityTableStateRuntime` (`:73-81`): `self := deps.self` and `Self: &self,` in the `tablestate.Config` literal; `runtime := &storageIntegrityTableStateRuntime{…, self: deps.self}`; `RegistryActivation: registryActivation(tablestate.FromFollower(deps.follower), deps.self),` (`:97`).
- `registryActivation` (`:107-120`):

```go
// registryActivation reads, for the declaration compensation loop (FB-I1),
// whether this indexer is enrolled and its own activation block (housegate
// spec 2026-10-10 §10): the loop compensates only while it is enrolled, and
// only tables created at or after its activation block. Before the
// follower's first answer View reports the registry disabled, so the loop
// waits for it.
func registryActivation(registry tablestate.RegistrySource, self uint64) func() (bool, uint64) {
	return func() (bool, uint64) {
		view, enabled := registry.View()
		if !enabled {
			return false, 0
		}
		me := view.Indexer(self)
		if !me.Enrolled {
			return false, 0
		}
		return true, me.ActivationBlock
	}
}
```

`standalone/storage_integrity_info.go` (`:63-67`) becomes:

```go
	if view, enabled := tablestate.FromFollower(r.follower).View(); enabled {
		info.RegistryVersion = view.Version
		// si_indexer_id names this indexer while it is enrolled (housegate
		// spec 2026-10-10 §10); a host that is not enrolled serves no SI table.
		if view.Indexer(r.self).Enrolled {
			id := r.self
			info.SIIndexerID = &id
			info.Enrolled = true
		}
	}
```

(add `"compute-network-node/storageintegrityadapter/tablestate"` to its imports). `standalone/standalone.go`: add `self: siIndexerID,` to the `storageIntegrityTableStateDeps` literal (`:387-399`).

Run: `cd "$W" && go test ./standalone/ ./rpc/ -run 'StorageIntegrityTableState|StorageIntegrityInfo|RegistryActivation|SelfIndexer|ClientLanes' -v`
Expected: PASS.

- [ ] **Step 6: Write the failing schema network-id tests**

Append to `standalone/schema_registry_test.go` (add `"compute-network-node/database_registry"`, `"go/ast"` to its imports):

```go
// Housegate spec 2026-10-10 §10: on an SI node the schema declarer must hash
// under the arbiter network id; startup refuses a mismatch instead of relying
// on physical_database happening to match it.
func TestCheckStorageIntegritySchemaNetworkID(t *testing.T) {
	require.NoError(t, checkStorageIntegritySchemaNetworkID("devnet2", nil), "a router-only node declares nothing")
	require.NoError(t, checkStorageIntegritySchemaNetworkID("devnet2", []database_registry.SchemaDeclarationConfig{{NetworkID: "devnet2"}}))
	err := checkStorageIntegritySchemaNetworkID(" devnet2", []database_registry.SchemaDeclarationConfig{{NetworkID: "devnet2"}})
	require.ErrorContains(t, err, `network id "devnet2"`)
	require.ErrorContains(t, err, `(storage_integrity.snode.network_id) is " devnet2"`)
}

func TestStorageIntegritySchemaDeclarationsUseTheArbiterNetworkID(t *testing.T) {
	credentialPath := filepath.Join(t.TempDir(), "clickhouse.yaml")
	require.NoError(t, os.WriteFile(credentialPath, []byte("credential:\n  subgraph:\n    username: schema-user\n"), 0o600))
	cfg := &config.Config{}
	cfg.Node.PhysicalDatabase = "testnet"
	cfg.Housegate.Upstream = "clickhouse.local:9000"
	cfg.Housegate.CkhManagerConfigPath = credentialPath
	cfg.StorageIntegrity.Enabled = true
	cfg.StorageIntegrity.SNode.NetworkID = "devnet2"
	declarations, err := schemaDeclarationConfigs(cfg)
	require.NoError(t, err)
	require.NoError(t, checkStorageIntegritySchemaNetworkID(cfg.StorageIntegrity.SNode.NetworkID, declarations),
		"an SI node declares under the arbiter network id, not physical_database")

	cfg.StorageIntegrity.SNode.NetworkID = "devnet2 " // the declarer trims; the SNode and the ingress do not
	declarations, err = schemaDeclarationConfigs(cfg)
	require.NoError(t, err)
	require.ErrorContains(t, checkStorageIntegritySchemaNetworkID(cfg.StorageIntegrity.SNode.NetworkID, declarations), `"devnet2 "`)
}

func TestRunChecksTheSchemaDeclarerNetworkID(t *testing.T) {
	_, runDecl := parseStandaloneRun(t, readStandaloneSource(t))
	checks := identifierCalls(runDecl, "checkStorageIntegritySchemaNetworkID")
	require.Len(t, checks, 1)
	require.Equal(t, "si.SNode.NetworkID", expressionSelectorPath(checks[0].Args[0]))
	require.True(t, identifierExpression(checks[0].Args[1], "schemaRegistryConfigs"))
	declarations := identifierCalls(runDecl, "schemaDeclarationConfigs")
	require.Len(t, declarations, 1)
	require.Less(t, int(declarations[0].Pos()), int(checks[0].Pos()), "the check reads the resolved declarations")
	inEnabledBranch := false
	ast.Inspect(runDecl.Body, func(node ast.Node) bool {
		if statement, ok := node.(*ast.IfStmt); ok && selectorExpression(statement.Cond, "si", "Enabled") && nodeContains(statement.Body, checks[0]) {
			inEnabledBranch = true
		}
		return true
	})
	require.True(t, inEnabledBranch, "only an SI node has an arbiter network id")
}
```

Run: `cd "$W" && go test ./standalone/ -run 'SchemaNetworkID|ArbiterNetworkID|SchemaDeclarerNetworkID' -v`
Expected: FAIL to compile — `undefined: checkStorageIntegritySchemaNetworkID`.

- [ ] **Step 7: Implement the check**

Append to `standalone/schema_registry.go`:

```go
// checkStorageIntegritySchemaNetworkID refuses a storage-integrity node whose
// schema declarations would hash under another network id than the arbiter's
// (housegate spec 2026-10-10 §10). The arbiter's watcher checks every table
// this node declares with TableSchemaHash(network_id, schema), so a declarer
// that hashed under another name (physical_database, or a network id with
// surrounding whitespace) would get the table Refused.
func checkStorageIntegritySchemaNetworkID(networkID string, declarations []database_registry.SchemaDeclarationConfig) error {
	for _, declaration := range declarations {
		if declaration.NetworkID != networkID {
			return fmt.Errorf(
				"storage integrity: the schema declarer hashes table schemas under network id %q, but the arbiter network id (storage_integrity.snode.network_id) is %q; tables this node declares would not match the arbiter's schema hash",
				declaration.NetworkID, networkID)
		}
	}
	return nil
}
```

In `standalone/standalone.go`, at the top of the storage-integrity block `if si.Enabled {` (`:233`, before the schema-source warning):

```go
			if err := checkStorageIntegritySchemaNetworkID(si.SNode.NetworkID, schemaRegistryConfigs); err != nil {
				return err
			}
```

It reads `schemaRegistryConfigs`, resolved at `:204-209`, and adds no bare `si` reference and no `si.Enabled` predicate, so the binding audits keep their counts.

Run: `cd "$W" && go test ./standalone/ -run 'SchemaNetworkID|ArbiterNetworkID|SchemaDeclarerNetworkID|ResolveSchemaRegistry|SchemaDeclarationConfigs|TestRun' -v`
Expected: PASS.

- [ ] **Step 8: README**

`README.md`:
- `:96`: replace "re-declares any table of this indexer's databases that was created at or after the arbiter table registry's activation block" with "re-declares any table of this indexer's databases that was created at or after this indexer's activation block (its `si_indexers` entry; the registry's activation block for the founding indexer before the signed-claims activation)", and "The loop runs only while the registry follower reports the registry enabled" with "The loop runs only while the registry follower reports the registry enabled and this indexer enrolled".
- `:99`: replace "A table the arbiter registry records — a live incarnation Pending, Refused, Active, Retiring or Purging — is answered from the registry whatever the chain state mirror says about its database;" with "A table the arbiter registry records for this indexer — a live incarnation this indexer owns (its database's indexer at the `TableCreated` block; an incarnation recorded before the signed-claims activation belongs to the founding indexer) that is Pending, Refused, Active, Retiring or Purging — is answered from the registry whatever the chain state mirror says about its database; a table another SI indexer owns is Ordinary here, so this host's rewriter never reads a peer's SI table from its own empty `hg_safe` (housegate spec 2026-10-10 §10);".
- `:102`: replace "**Ordinary** while the registry is disabled (only the genesis set is Active), for databases on other indexers or unknown to the chain state," with "**Ordinary** while the registry is disabled (only the genesis set is Active), for every table while this indexer has no `si_indexers` entry (before the signed-claims activation: unless it is the founding indexer), for every table another SI indexer owns, for databases on other indexers or unknown to the chain state,", and "before the seed commits, for tables whose `TableCreated` block precedes `activation_block`" with "before this indexer's seed commits, for tables whose `TableCreated` block precedes its `activation_block`".
- `:103`: replace "for a table of any other database on the SI indexer that the registry does not record" with "for a table of any other database this enrolled indexer hosts that the registry does not record".
- `:108`: after "a node without storage integrity answers `ordinary`." insert "`sentio_getStorageIntegrityInfo` reports `si_indexer_id` as this indexer's id while it is enrolled, else null, and `enrolled`. On a storage-integrity node, startup refuses a schema declarer whose network id differs from `storage_integrity.snode.network_id`."

- [ ] **Step 9: Full verification and commit**

Run: `cd "$W" && test -z "$(gofmt -l $(git ls-files '*.go'))" && bazel run //:gazelle && bazel build //... && bazel test //... --test_output=errors`
Expected: all PASS.

```bash
cd "$W" && git add -A && git commit -m "feat(storage-integrity): owner-aware table state and enrolment reporting

A database is governed iff this indexer hosts it, is enrolled in the
arbiter's si_indexers and it is not a processor database; a recorded
registry key is answered from the registry only when this indexer owns
its incarnation (nil owner = the founding indexer), every other one is
Ordinary; default deny and the Legacy fallback read this indexer's own
seed and activation block (housegate spec 2026-10-10 §10).
sentio_getStorageIntegrityInfo reports si_indexer_id while enrolled and
a new enrolled flag; declaration compensation follows this indexer's
enrolment and activation block; an SI node refuses a schema declarer
network id that differs from storage_integrity.snode.network_id. Before
the signed-claims activation every answer on the founding host is
unchanged.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 10: PR and merge (requires explicit user confirmation)**

sentio-node always goes through a PR (`CLAUDE.md` "Pull Request Workflow"). Ask before each remote action. Then:

```bash
cd "$W" && git push -u origin urwt/claude/multi-source-s1
gh pr create --repo sentioxyz/sentio-node --base main --head urwt/claude/multi-source-s1 \
  --title 'feat(storage-integrity): multi-source SI stage 1 host side' \
  --body "$(cat <<'EOF'
Host side of stage 1 of the multi-source storage-integrity network (housegate spec `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md` §10).

- Pins housegate `HG_TAG`, arbiter-core `CORE_TAG`, arbiter-proto `PROTO_TAG` (go.mod, and bazel_dep + git_override for housegate and arbiter-core).
- The embedded SNode gets this node's indexer id (owner filter) and a claim signer from `PRIVATE_KEY`; it advertises `signed_claims_v1` through arbiter-core.
- `storage_integrity.snode.table_ids` may be empty for an indexer that owns no genesis table.
- A node that is not enrolled yet starts with its SNode unregistered and registers once its `si_indexers` entry commits.
- `sentio-node storage-integrity enrollment-statement` prints the signed enrolment statement.
- Owner-aware table state; `sentio_getStorageIntegrityInfo` reports `enrolled`; compensation follows this indexer's enrolment; startup refuses a schema network id that differs from the arbiter's.

Behaviour before the signed-claims activation is unchanged on the founding indexer.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Wait for `build` and `integration-clickhouse` (`gh pr checks <n> --repo sentioxyz/sentio-node --watch`), then merge after review with the repository's convention (`gh pr merge <n> --repo sentioxyz/sentio-node --squash`).

- [ ] **Step 11: Record `SN_COMMIT` and `SN_DIGEST`**

`SN_COMMIT` is the merge commit on `main` (`gh pr view <n> --repo sentioxyz/sentio-node --json mergeCommit --jq .mergeCommit.oid`, 40 hex). Wait for that push's `docker-push` job (`gh run list --repo sentioxyz/sentio-node --branch main --limit 3`; `gh run watch <id> --repo sentioxyz/sentio-node`), which tags `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>` (`.github/workflows/ci.yml:216-221`, `type=sha,format=long`). Then:

```bash
SN_COMMIT=<sha>
docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT" | sed -n '1,4p'
docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT" --format '{{json .Image}}' \
  | jq -r 'if has("config") then .config.Labels["org.opencontainers.image.revision"] else (to_entries[0].value.config.Labels["org.opencontainers.image.revision"]) end'
```

Expected: the first prints `Name:`, `MediaType:` and `Digest: sha256:…` — record that digest as `SN_DIGEST`; the second prints exactly `SN_COMMIT`. Record both in the release ledger; Task 16 pins `sha-$SN_COMMIT@$SN_DIGEST`.


---

## Task 16: production — multi-writer chart values, indexer-b SI overlay and source ClickHouse, render tests, stage-1 runbook (S1, PR)

**Files** (anchors are `origin/main` of `sentioxyz/production` at `db2109be7`; the storage-integrity files are unchanged since `4ad1968cd`):
- Modify: `charts/sentio-node/templates/node.yaml` (SNode guard 51-53; `snode:` block 325-336)
- Modify: `charts/sentio-node/values.yaml` (`storageIntegrity.snode`, 193-195)
- Modify: `charts/storage-integrity/templates/verifier.yaml` (config 21-24, container mounts 130-132, end of the StatefulSet 142), `charts/storage-integrity/templates/validate.yaml` (legacy branch 72-74)
- Modify: `charts/storage-integrity/values.yaml` (`genesis.maxWriters` comment 15-18, `verifier` 167-171) and `charts/storage-integrity/values.schema.json` (`maxWriters.description` 13, new `verifier` property after 76)
- Modify: `charts/storage-integrity/tests/validate_source_binding.py` (constants 12-14, `validate_binding` 58-178, `main` 181-194)
- Modify: `charts/storage-integrity/tests/test_render.py` (import 15, constants 29-75, helpers before `class DeploymentContractTest` at 266, 308-311, new `VerifierStateTest` before `class LegacyCompatibilityTest` at 587, `FreshSourceBindingTest.validate` 810-816, after 993, `ActualFreshNetworkTest.setUpClass` 1126, 1280, after 1439)
- Create: `k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2.yaml`, `k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml`, `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2-indexer-b-source.yaml`
- Modify: `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml` (header 9-13, new release after 38), `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml` (comment 4; `genesisSnapshotID` after 97)
- Modify: `.github/workflows/storage-integrity-chart.yml` (paths, 13 and 24)
- Create: `docs/storage-integrity-devnet2-multi-source.md`
- Modify: `docs/storage-integrity-devnet2-dynamic-table-set.md` (stage table after 15, §8 after 941)

**Interfaces:**
- Consumes: release ledger `PROTO_TAG`, `CORE_TAG`, `ARBITER_TAG`, `ARBITER_DIGEST` (S1-A Tasks 1, 4, 12), `HG_TAG` (Task 13), `SN_COMMIT`, `SN_DIGEST` (Task 15). CONTRACT §3b: the SNode's `Config.GenesisSnapshotID` behind sentio-node's `storage_integrity.snode.genesis_snapshot_id` (lowercase `0x` + 64 hex; required with an empty `table_ids`; named on every SI indexer because the enrolment subcommand requires it; on an SNode with genesis tables sentio-node checks it against them at startup), the empty-set schema root `0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59`, `verifier.Config.StateDir` (`registration.json`) exposed by `cmd/arbiter-verifier` (this task renders it as the YAML key `state_dir`, see Contract conflicts C8). CONTRACT §3c: the activation's `si_indexers` / `verifiers` are authoritative, its Apply evicts every registered node they do not name, and `arbiter-admin` refuses to submit such an activation without `--evict-unnamed`. S1-A Task 11's CLI (`consensus update … --si-indexer 'id=N,activation=BLOCK,signer=0x…,node=ID,enrollment=PATH' … --verifier 'node=ID,pubkey=HEX' … [--evict-unnamed] --voter …`, `consensus show` printing `current.si_indexers` / `current.verifiers`), S1-A Task 12 Step 7's `$HOME/si-devnet2-multisource-rehearsal/bin/{old,new}/{statedigest,v19check}`, Task 14's `sentio-node --config-path <path> storage-integrity enrollment-statement` (stdout only the JWS; needs `PRIVATE_KEY`, the Ethereum RPC, Redis and the configured genesis snapshot id; never contacts the arbiter) and its deferred-registration log lines, Task 15's `sentio_getStorageIntegrityInfo` fields `si_indexer_id` / `self_indexer_id` / `enrolled` and its owner-aware table answers.
- Produces: the PR; the runbook Task 17 executes; sentio-node chart value `storageIntegrity.snode.genesisSnapshotID`; storage-integrity chart values `verifier.state.{enabled,storageClassName,size}`; `validate_binding(control, source, image_values, namespace, source_clickhouse=None)`, `validate_source_binding.EMPTY_SCHEMA_ROOT` and `--source-clickhouse`; test constants `DEVNET2_VERIFIER_STATE_ENABLED`, `DEVNET2_GENESIS_SNAPSHOT_ID`, `INDEXER_B_CHI`, `INDEXER_B_SOURCE_IMAGE`, `DEVNET2_SNODE_IDS` and helpers `host_config(docs)`, `indexer_b_source_clickhouse()`; indexer-b's SNode node id `snode-2`; indexer-b's source ClickHouse Service `storage-integrity-si-v2-indexer-b-source:9000` (pod `storage-integrity-si-v2-indexer-b-source-0-0-0`, interserver `storage-integrity-si-v2-indexer-b-source-replica-0:9009`).

**Decisions fixed by this task** (evidence read on 2026-10-10, read-only):

- **D16-1. indexer-b gets a dedicated source ClickHouse (option b), CHI `storage-integrity-si-v2-indexer-b` with one cluster `source`, not the source profile on `clickhouse-devnet2-node-b` (option a).**
  - The SNode opens its ClickHouse connection without credentials: `clickhouse.Open(&clickhouse.Options{Addr: []string{si.ClickhouseDSN}})` (sentio-node `standalone/standalone.go:211-213`), i.e. user `default`, empty password. CHI `clickhouse-devnet2` sets `default/password_sha256_hex` (`k8s-sea/clickhouse/sentio-devnet2-clickhouse.yaml:133`), and CHI users are installation-wide (rendered once into `chi-clickhouse-devnet2-common-usersd`, shared by node-a and node-b). Option (a) therefore cannot work without changing node-a's users too.
  - `clickhouse-devnet2` is not Helm-managed and still serves node-a: `sentio-node-devnet2-observer`'s ClickHouse configuration names `clickhouse-devnet2-node-a:9000`, and indexer-a's rollback surfaces (`helmfile.yaml`, `storage-integrity.helmfile.yaml`) point at it. Its live spec equals the repository file (generation 3, status Completed, namespace operator `altinity-clickhouse-operator` 0.27.2), so any edit is a real reconcile of both hosts.
  - node-b's data is negligible (spec §2: `devuser1.t`, 0 rows, plus two views; re-measured credential-free in runbook §6.1), so the move costs one DDL replay without rows (runbook §6.3), the same move indexer-a made to its bundled source.
  - The new installation copies the network installation's source profile exactly, and `test_render.py` keeps the two equal: Keeper ensemble `clickhouse-keeper-extra` with root `/storage-integrity/devnet2/devnet2-si-v2` (which already exists; the operator writes it per host into `chop-generated-zookeeper.xml`, as live on `storage-integrity-si-v2-source`), the operator-advertised `interserver_http_host` = its replica Service, which exposes 9009, the passwordless default user, and the same server build as the verifier replicas. The namespace has no NetworkPolicy, so the verifier ClickHouses reach it. The ReplicatedMergeTree replica name is the literal SNode node id in the DDL (`arbiter-core:dataplane/ddl/build.go:47-48`, `table.go:110-127`), so no `{replica}` macro is involved (Contract conflicts C3).
- **D16-2. indexer-b's SNode node id is `snode-2`.** SNode ids are allocated from a monotonic counter recorded in the runbook's §1.2 ledger (`snode-1` = indexer 0 since devnet2-si-v2 genesis, `snode-2` = indexer 1, next free `snode-3`). A counter can never hand out an id twice, unlike a name derived from an indexer label that could be registered again. It differs from every verifier id (`verifier-1..3`, live StatefulSet init script `replica_id="verifier-${replica_num}"`) and every voter id (`storage-integrity-si-v2-arbiter-0..2`, the pod hostname). The arbiter refuses an enrolment whose id has any node record, evicted included (S1-A Task 5 rule 5), and the id is the Keeper replica name of every table indexer-b owns, so it must stay stable. S1-A Task 2's fixtures already use `snode-2` for indexer 1.
- **D16-3. SNode genesis values (CONTRACT §3b.3, §3b.8).** indexer-b: `tableIDs: []`, `schemaRoot: 0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59` (`payloadexec.SchemaRoot(net, nil)` = `0x` + sha256 of `schema-root\0`: no table hash enters it, so it is the same for every network; verified locally), `genesisSnapshotID: 0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06` (devnet2-si-v2's, from `consensus show` at epoch 2); never indexer-a's `devnet101.swap_new2` in its `tableIDs`, which would make its SNode create that table's `hg_*` replicas. indexer-a keeps its genesis table and the network root and names the same `genesisSnapshotID`: the enrolment subcommand refuses to run without it, and the stage-1 image checks it against indexer-a's genesis tables at startup (`storage_integrity.snode.genesis_snapshot_id <configured> is not the network's genesis snapshot id <derived> derived from this node's genesis tables`). indexer-a's live image ff336f1 decodes its config with yaml.v3's lenient `yaml.Unmarshal` (sentio-node `config/config.go:359` at ff336f1) and ignores the key until Task 17 moves it to the stage-1 image, so the PR sets it on both overlays. The sentio-node chart gains `storageIntegrity.snode.genesisSnapshotID`, rendered as `genesis_snapshot_id`, quoted like `schema_root` and right after it, only when set; it refuses any value that is not lowercase `0x` + 64 hex, and an empty `tableIDs` except in the registry-driven shape with the empty-set root and a genesis snapshot id.
- **D16-4. No chart value for the SNode's indexer id.** sentio-node derives it from its on-chain registration (`rpc/storage_integrity_info.go:47-53`, `env.IndexerRuntime.ResolvedIndexerID()`, which Task 14 reuses for `snode.Config.IndexerID`). A render test asserts no `indexer_id` key is rendered.
- **D16-5. Verifier state (CONTRACT §3b.9): a per-verifier PVC, `verifier.state`, off by default.** The verifier StatefulSet has no persistent volume today (only the `config` emptyDir and the seeds Secret; live and `templates/verifier.yaml:135-142`). An emptyDir loses `registration.json` whenever the pod is recreated, a hostPath does not follow a pod that moves, and the verifier's ClickHouse volume belongs to another pod, so the chart adds a `volumeClaimTemplates` entry `state` (1Gi, `local-path` like the voters' volumes), mounts it at `/state` and renders `state_dir: /state` into `verifier.yaml`. The arbiter-verifier config is parsed leniently (`yaml.Unmarshal`, arbiter `cmd/arbiter-verifier/config.go:77`), so an older verifier ignores the key. A StatefulSet's volume claim templates are immutable, so devnet2 turns it on together with the stage-1 arbiter digest and deletes the verifier StatefulSet with `--cascade=orphan` right before that sync (runbook §3.1; the pods keep running and the recreated StatefulSet rolls them one at a time). The legacy profile refuses it.
- **D16-6. `genesis.maxWriters` stays the immutable bootstrap cap 1.** The runtime limit becomes 2 through the enrolment consensus update (Task 17). See Contract conflicts C1 for why the contract's "allow maxWriters > 1" is not implemented.
- **D16-7. The PR is inert.** Merging it syncs and applies nothing: the new helmfile release is synced only with `--selector name=sentio-node-devnet2-indexer-b`, the CHI file is applied only with `kubectl apply`, and `verifier.state` stays off for devnet2 until Task 17. Step 5 measures it: the network release, the legacy release and every sentio-node release of `helmfile.yaml` and `storage-integrity.helmfile.yaml` render byte-identically to `origin/main`; indexer-a's SI release differs only by its new `genesis_snapshot_id` line and, through it, the `checksum/config` annotation, so a sync of indexer-a before Task 17 would restart it once on ff336f1, which ignores the key.

- [ ] **Step 1: Worktree and ledger inputs**

Requires Task 15 to have recorded `SN_COMMIT` / `SN_DIGEST` (indexer-b's pin and `WRITER_AUTHORIZATION_IMAGES` need them) and Task 12 `ARBITER_TAG` / `ARBITER_DIGEST` (the runbook's pins). Stop if any is missing.

```bash
git -C /Users/uranuswch/Dev/sentio_xyz/production fetch origin
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
' urwt /Users/uranuswch/Dev/sentio_xyz/production claude multi-source-s1
```

Expected: one JSON object with `"action": "created"` whose `path` is `~/src/remotesrc/claude/production/multi-source-s1` (expanded). `W` below is that path; the Bash tool keeps no variables between calls, so prefix every command with `W=<that path>;` and the ledger values (`ARBITER_TAG=…; ARBITER_DIGEST=sha256:…; PROTO_TAG=…; CORE_TAG=…; HG_TAG=…; SN_COMMIT=<40 hex>; SN_DIGEST=sha256:…;`).

```bash
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT" | sed -n 's/^Digest: *//p'
docker buildx imagetools inspect "ghcr.io/sentioxyz/arbiter:$ARBITER_TAG" | sed -n 's/^Digest: *//p'
```

Expected: `Ran 65 tests` and `OK`; the two digests equal `SN_DIGEST` and `ARBITER_DIGEST`.

- [ ] **Step 2: Failing tests for owner-specific genesis sets, per-indexer sources and verifier state**

In `charts/storage-integrity/tests/test_render.py`:

Line 15 becomes:

```python
from validate_source_binding import EMPTY_SCHEMA_ROOT, LEGACY_SOURCE_DIGEST, addresses, validate_binding
```

Insert before `class DeploymentContractTest(unittest.TestCase):` (line 266):

```python
def host_config(docs):
    """The standalone.yaml one sentio-node release renders."""
    return yaml.safe_load(next(
        doc for doc in docs.values() if "standalone.yaml" in doc.get("data", {})
    )["data"]["standalone.yaml"])


```

In `test_only_single_writer_is_supported_by_pilot` (308-311), before the `for` line:

```python
        # genesis.maxWriters is the immutable bootstrap cap; multi-source
        # networks raise the runtime limit by consensus update (spec 2026-10-10 §6.1).
```

Insert before `class LegacyCompatibilityTest(unittest.TestCase):` (line 587; reuses `render`, `render_legacy`, `documents`, `config_for`, `arbiter_config`, `pod_template`):

```python
class VerifierStateTest(unittest.TestCase):
    """housegate spec 2026-10-10: each verifier keeps registration.json, its last
    registration_seq, in a persistent state directory (verifier.Config.StateDir)."""

    @classmethod
    def setUpClass(cls):
        cls.docs = documents(render())
        cls.stateful = documents(render({"verifier": {"state": {"enabled": True}}}))

    def assert_rejected(self, overrides, error):
        result = render(overrides)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(error, result.stderr)

    def test_default_renders_no_verifier_state(self):
        self.assertNotIn("state_dir", config_for(self.docs, "verifier"))
        spec = self.docs[("StatefulSet", "storage-integrity-verifier")]["spec"]
        self.assertNotIn("volumeClaimTemplates", spec)
        self.assertNotIn("state", [mount["name"] for mount in spec["template"]["spec"]["containers"][0]["volumeMounts"]])

    def test_enabled_state_is_one_persistent_volume_per_verifier(self):
        self.assertEqual(config_for(self.stateful, "verifier")["state_dir"], "/state")
        spec = self.stateful[("StatefulSet", "storage-integrity-verifier")]["spec"]
        self.assertEqual(spec["volumeClaimTemplates"], [{
            "metadata": {"name": "state"},
            "spec": {
                "accessModes": ["ReadWriteOnce"],
                "storageClassName": "local-path",
                "resources": {"requests": {"storage": "1Gi"}},
            },
        }])
        self.assertIn({"name": "state", "mountPath": "/state"}, spec["template"]["spec"]["containers"][0]["volumeMounts"])

    def test_state_changes_neither_identity_nor_arbiter_config(self):
        self.assertEqual(arbiter_config(self.stateful), arbiter_config(self.docs))
        for role in ("arbiter", "verifier"):
            self.assertEqual(
                pod_template(self.stateful, role)["metadata"]["annotations"]["checksum/config"],
                pod_template(self.docs, role)["metadata"]["annotations"]["checksum/config"],
            )

    def test_invalid_state_settings_fail_closed(self):
        for state, error in (
            ({"enabled": "true"}, "enabled"),
            ({"enabled": True, "size": ""}, "size"),
            ({"enabled": True, "storageClassName": ""}, "storageClassName"),
        ):
            with self.subTest(state=state):
                self.assert_rejected({"verifier": {"state": state}}, error)

    def test_legacy_profile_rejects_verifier_state(self):
        result = render_legacy({"verifier": {"state": {"enabled": True}}})
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("verifier.state requires pinned-genesis-v1", result.stderr)


```

Replace `FreshSourceBindingTest.validate` (810-816) with:

```python
    def validate(self, source=None, image=None, namespace=RENDER_NAMESPACE, control=None, source_clickhouse=None):
        return validate_binding(
            list((control if control is not None else self.control).values()),
            list((source if source is not None else self.source).values()),
            image if image is not None else self.image_values,
            namespace,
            source_clickhouse=source_clickhouse,
        )
```

Append after the last line of `test_registry_shape_chart_guards` (`self.assertEqual(not_local.returncode, 0, not_local.stderr)`, line 993), inside `FreshSourceBindingTest` (reuses `source_values`, `validate`, `control`, `source`, `image_values` and the module's `documents`, `config_for`, `arbiter_config`, `with_config`, `set_path`):

```python

    def test_an_snode_may_own_no_genesis_table_only_in_the_registry_shape(self):
        # housegate spec 2026-10-10 §10: tableIDs lists the genesis tables this
        # indexer owns, none for an SI indexer enrolled after genesis. Such an
        # SNode uses the empty set's schema root and must name the network's
        # genesis snapshot id, which it cannot derive from an empty set.
        registry_values = self.source_values(self.control, RENDER_NAMESPACE, registry=True)

        def render_with(overrides):
            with tempfile.TemporaryDirectory() as directory:
                base = Path(directory) / "base.yaml"
                extra = Path(directory) / "extra.yaml"
                base.write_text(yaml.safe_dump(registry_values))
                extra.write_text(yaml.safe_dump(overrides))
                return subprocess.run([
                    "helm", "template", "fresh-source", str(ROOT / "charts/sentio-node"),
                    "--namespace", RENDER_NAMESPACE, "-f", str(base), "-f", str(extra),
                ], capture_output=True, text=True, check=False)

        genesis_id = "0x" + "6" * 64
        owns_none = {"tableIDs": [], "schemaRoot": EMPTY_SCHEMA_ROOT, "genesisSnapshotID": genesis_id}
        host = host_config(documents(render_with({"storageIntegrity": {"snode": owns_none}})))
        snode = host["storage_integrity"]["snode"]
        self.assertEqual((snode["table_ids"], snode["schema_root"], snode["genesis_snapshot_id"]), ([], EMPTY_SCHEMA_ROOT, genesis_id))
        self.assertIs(host["housegate"]["storage_integrity"]["enabled"], True)
        self.assertNotIn("tables", host["housegate"]["storage_integrity"])
        # An SNode with genesis tables names the genesis snapshot id too (its
        # enrolment statement binds it; the SNode checks it against its genesis
        # tables at startup). Unset, nothing is rendered.
        founding = host_config(documents(render_with({"storageIntegrity": {"snode": {"genesisSnapshotID": genesis_id}}})))
        self.assertEqual(founding["storage_integrity"]["snode"]["genesis_snapshot_id"], genesis_id)
        self.assertNotIn("genesis_snapshot_id", host_config(documents(render_with({})))["storage_integrity"]["snode"])
        network_root = arbiter_config(self.control)["genesis"]["schema_root"]
        malformed = "storageIntegrity.snode.genesisSnapshotID must be lowercase 0x-prefixed 32-byte hex"
        for overrides, error in (
            ({"storageIntegrity": {"snode": owns_none}, "housegate": {"storageIntegrity": {"tables": ["devnet101.swap_new2"]}}},
             "tableIDs may be empty only in the registry-driven shape"),
            ({"storageIntegrity": {"snode": {**owns_none, "schemaRoot": network_root}}},
             "the schema root of the empty genesis set"),
            ({"storageIntegrity": {"snode": {**owns_none, "genesisSnapshotID": ""}}},
             "storageIntegrity.snode.genesisSnapshotID is required when tableIDs is empty"),
            ({"storageIntegrity": {"snode": {**owns_none, "genesisSnapshotID": "0x" + "A" * 64}}}, malformed),
            ({"storageIntegrity": {"snode": {"genesisSnapshotID": "0x" + "6" * 63}}}, malformed),
            ({"storageIntegrity": {"snode": {"nodeID": ""}}},
             "storageIntegrity.snode identity and authorityAddresses are required"),
        ):
            with self.subTest(overrides=overrides):
                result = render_with(overrides)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(error, result.stderr)

    def test_a_later_indexer_binds_with_its_own_source_and_no_genesis_table(self):
        own = "indexer-two-source:9000"
        genesis_id = "0x" + "6" * 64
        values = self.source_values(self.control, RENDER_NAMESPACE, registry=True)
        values["clickhouse"] = {"address": own}
        values["storageIntegrity"]["clickhouseDSN"] = own
        values["storageIntegrity"]["snode"].update(tableIDs=[], schemaRoot=EMPTY_SCHEMA_ROOT, genesisSnapshotID=genesis_id)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "source.yaml"
            path.write_text(yaml.safe_dump(values))
            later = documents(subprocess.run([
                "helm", "template", "fresh-source", str(ROOT / "charts/sentio-node"),
                "--namespace", RENDER_NAMESPACE, "-f", str(path),
            ], capture_output=True, text=True, check=False))
        self.assertEqual(self.validate(source=later, source_clickhouse=own), FRESH_SOURCE_IMAGE)
        bundled = config_for(self.control, "bootstrap")["clickhouse_addr"]
        with self.assertRaisesRegex(ValueError, "bundled source"):
            self.validate(source=later, source_clickhouse=bundled)
        # Without its own source an SNode is the founding one and lists every genesis table.
        with self.assertRaisesRegex(ValueError, "genesis table"):
            self.validate(source=later)
        # With its own source an SNode owns no genesis table.
        with self.assertRaisesRegex(ValueError, "genesis table"):
            self.validate(source_clickhouse=own)
        for field, value in (
            ("schema_root", arbiter_config(self.control)["genesis"]["schema_root"]),
            ("genesis_snapshot_id", ""),
            ("genesis_snapshot_id", None),
        ):
            with self.subTest(field=field, value=value), self.assertRaisesRegex(ValueError, field):
                self.validate(
                    source=with_config(later, "standalone.yaml", set_path(("storage_integrity", "snode", field), value)),
                    source_clickhouse=own,
                )
        # The founding indexer's SNode may name the genesis snapshot id (its
        # enrolment statement binds it), but only in the canonical spelling.
        named = set_path(("storage_integrity", "snode", "genesis_snapshot_id"), genesis_id)
        self.assertEqual(self.validate(source=with_config(self.source, "standalone.yaml", named)), FRESH_SOURCE_IMAGE)
        with self.assertRaisesRegex(ValueError, "genesis_snapshot_id must be lowercase"):
            self.validate(source=with_config(
                self.source, "standalone.yaml", set_path(("storage_integrity", "snode", "genesis_snapshot_id"), "0x" + "6" * 63),
            ))
```

Run: `cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -2`
Expected: the module does not import: `ImportError: cannot import name 'EMPTY_SCHEMA_ROOT' from 'validate_source_binding'` (measured on a copy of `origin/main`).

- [ ] **Step 3: Chart guards, verifier state and binding validator**

`charts/sentio-node/templates/node.yaml`:

```diff
@@ -48,8 +48,31 @@
 {{- if and .Values.storageIntegrity.enabled (or (empty .Values.storageIntegrity.payloadStoreDataAddr) (empty .Values.storageIntegrity.clickhouseDSN)) -}}
 {{- fail "storageIntegrity payloadStoreDataAddr and clickhouseDSN are required" -}}
 {{- end -}}
-{{- if and .Values.storageIntegrity.enabled (or (empty .Values.storageIntegrity.snode.nodeID) (empty .Values.storageIntegrity.snode.networkID) (empty .Values.storageIntegrity.snode.schemaSnapshotID) (empty .Values.storageIntegrity.snode.executorProfileID) (empty .Values.storageIntegrity.snode.schemaRoot) (empty .Values.storageIntegrity.snode.authorityAddresses) (empty .Values.storageIntegrity.snode.tableIDs)) -}}
-{{- fail "storageIntegrity.snode identity, authorityAddresses, and tableIDs are required" -}}
+{{- if and .Values.storageIntegrity.enabled (or (empty .Values.storageIntegrity.snode.nodeID) (empty .Values.storageIntegrity.snode.networkID) (empty .Values.storageIntegrity.snode.schemaSnapshotID) (empty .Values.storageIntegrity.snode.executorProfileID) (empty .Values.storageIntegrity.snode.schemaRoot) (empty .Values.storageIntegrity.snode.authorityAddresses)) -}}
+{{- fail "storageIntegrity.snode identity and authorityAddresses are required" -}}
+{{- end -}}
+{{- /* storageIntegrity.snode.tableIDs lists the genesis tables this indexer
+owns (housegate spec 2026-10-10 §10): the founding SI indexer lists every
+genesis table, an indexer enrolled later none. Only an SNode that follows the
+table registry (the registry-driven shape) may own none, and it then uses the
+empty set's schema root. genesisSnapshotID is the network's genesis snapshot
+id, which every SI indexer's enrolment statement binds: an SNode with genesis
+tables checks it against them at startup, one without cannot derive it. */ -}}
+{{- $snode := .Values.storageIntegrity.snode -}}
+{{- $emptySchemaRoot := "0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59" -}}
+{{- if and .Values.storageIntegrity.enabled $snode.genesisSnapshotID (not (regexMatch "^0x[0-9a-f]{64}$" (toString $snode.genesisSnapshotID))) -}}
+{{- fail "storageIntegrity.snode.genesisSnapshotID must be lowercase 0x-prefixed 32-byte hex: the genesis_snapshot_id that arbiter-admin consensus show prints" -}}
+{{- end -}}
+{{- if and .Values.storageIntegrity.enabled (empty $snode.tableIDs) -}}
+{{- if not (and .Values.housegate.storageIntegrity.enabled (empty .Values.housegate.storageIntegrity.tables)) -}}
+{{- fail "storageIntegrity.snode.tableIDs may be empty only in the registry-driven shape (housegate.storageIntegrity.enabled with no tables): it lists the genesis tables this indexer owns" -}}
+{{- end -}}
+{{- if ne (lower (toString $snode.schemaRoot)) $emptySchemaRoot -}}
+{{- fail (printf "storageIntegrity.snode.schemaRoot must be %s, the schema root of the empty genesis set, when tableIDs is empty" $emptySchemaRoot) -}}
+{{- end -}}
+{{- if empty $snode.genesisSnapshotID -}}
+{{- fail "storageIntegrity.snode.genesisSnapshotID is required when tableIDs is empty: an SNode without genesis tables cannot derive the network's genesis snapshot id" -}}
+{{- end -}}
 {{- end -}}
 {{- if and .Values.housegate.storageIntegrity.enabled (eq .Values.housegate.storageIntegrity.read.defaultMode "unsafe_latest") (not .Values.storageIntegrity.enabled) -}}
 {{- fail "housegate unsafe_latest requires the in-process storageIntegrity read-state port" -}}
@@ -329,6 +352,9 @@
         schema_snapshot_id: {{ .Values.storageIntegrity.snode.schemaSnapshotID | quote }}
         executor_profile_id: {{ .Values.storageIntegrity.snode.executorProfileID | quote }}
         schema_root: {{ .Values.storageIntegrity.snode.schemaRoot | quote }}
+        {{- with .Values.storageIntegrity.snode.genesisSnapshotID }}
+        genesis_snapshot_id: {{ . | quote }}
+        {{- end }}
         authority_addresses:
           {{- toYaml .Values.storageIntegrity.snode.authorityAddresses | nindent 10 }}
         table_ids:
```

`charts/sentio-node/values.yaml` (the new key stays commented out so releases without it render byte-identically, the chart's convention for `deniedAddresses`):

```diff
@@ -191,7 +191,19 @@
     executorProfileID: ""
     schemaRoot: ""
     authorityAddresses: []
+    # -- The genesis tables this indexer owns (housegate spec 2026-10-10 §10):
+    # the founding SI indexer lists every genesis table, an indexer enrolled
+    # later lists none. Empty only in the registry-driven shape (no
+    # housegate.storageIntegrity.tables), and then schemaRoot must be the empty
+    # set's root, 0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59,
+    # and genesisSnapshotID must be set.
     tableIDs: []
+    # -- (optional) The network's genesis snapshot id (arbiter-admin consensus
+    # show: genesis_snapshot_id), rendered as genesis_snapshot_id. Every SI
+    # indexer names it, because its enrolment statement binds it: an SNode with
+    # genesis tables checks it against them at startup, and an empty tableIDs
+    # requires it. Not set by default, so releases without it render unchanged.
+    # genesisSnapshotID: "0x..."
     schemaSource: clickhouse
```

`charts/storage-integrity/templates/verifier.yaml`:

```diff
@@ -21,6 +21,9 @@
     unsafe_database: hg_unsafe
     safe_database: hg_safe
     promote_database: hg_promote
+    {{- if .Values.verifier.state.enabled }}
+    state_dir: /state
+    {{- end }}
     tables:
 {{ include "storage-integrity.tables" . | indent 6 }}
 ---
@@ -130,6 +133,9 @@
           args: [-config, /config/verifier.yaml, -ensure-tables=create]
           volumeMounts:
             - {name: config, mountPath: /config, readOnly: true}
+            {{- if .Values.verifier.state.enabled }}
+            - {name: state, mountPath: /state}
+            {{- end }}
           resources:
             {{- toYaml .Values.verifier.resources | nindent 12 }}
       volumes:
@@ -140,3 +146,13 @@
           secret:
             secretName: {{ .Values.verifier.seedsSecret.name }}
         - {name: config, emptyDir: {}}
+  {{- if .Values.verifier.state.enabled }}
+  volumeClaimTemplates:
+    - metadata: {name: state}
+      spec:
+        accessModes: [ReadWriteOnce]
+        storageClassName: {{ .Values.verifier.state.storageClassName | quote }}
+        resources:
+          requests:
+            storage: {{ .Values.verifier.state.size }}
+  {{- end }}
```

`charts/storage-integrity/templates/validate.yaml` (inside the `legacy` branch):

```diff
@@ -72,6 +72,9 @@
 {{- if not (empty .Values.arbiter.anchor.posterSecret.name) }}
 {{- fail "arbiter.anchor.posterSecret requires pinned-genesis-v1: legacy retains its existing authoritySecret wiring" }}
 {{- end }}
+{{- if .Values.verifier.state.enabled }}
+{{- fail "verifier.state requires pinned-genesis-v1: the legacy v0.6.1 verifier has no state directory" }}
+{{- end }}
 {{- else }}
```

`charts/storage-integrity/values.yaml`:

```diff
@@ -13,8 +13,13 @@
 authorityAddress: ""
 
 genesis:
-  # Immutable bootstrap cap; retain it after consensus administration updates.
-  # This pilot does not support independent concurrent SNode writers.
+  # Immutable bootstrap cap (genesis.max_writers); retain it after consensus
+  # administration updates. Every voter compares it with its snapshot's
+  # bootstrap parameters on restore, so it never follows the runtime limit. The
+  # runtime limit rises only with the authority-signed consensus update that
+  # enrols an SI indexer (max_writers >= len(si_indexers), housegate spec
+  # 2026-10-10 §6.1): a network starts with one writer and hash source
+  # selection until si_indexers is set.
   maxWriters: 1
 
 table:
@@ -169,6 +174,17 @@
   seedsSecret:
     name: ""
     keyPrefix: verifier-
+  # Persistent state of each verifier (housegate spec 2026-10-10, stage 1):
+  # state_dir holds registration.json, the verifier's last registration_seq
+  # (arbiter-core verifier.Config.StateDir; an arbiter that predates it ignores
+  # the key). Off by default. The volume claim template of an existing
+  # StatefulSet is immutable, so turning this on or off there needs the
+  # StatefulSet deleted with --cascade=orphan right before the sync
+  # (docs/storage-integrity-devnet2-multi-source.md §3.1).
+  state:
+    enabled: false
+    storageClassName: local-path
+    size: 1Gi
   resources:
     requests:
       cpu: 500m
```

`charts/storage-integrity/values.schema.json`:

```diff
@@ -10,7 +10,7 @@
         "maxWriters": {
           "type": "integer",
           "enum": [1],
-          "description": "The pilot supports exactly one SNode writer."
+          "description": "Immutable bootstrap writer cap. The runtime limit rises only by the consensus update that enrols an SI indexer (max_writers >= len(si_indexers)); before si_indexers is set a network has one writer."
         }
       }
     },
@@ -73,6 +73,20 @@
           }
         }
       }
+    },
+    "verifier": {
+      "type": "object",
+      "properties": {
+        "state": {
+          "type": "object",
+          "required": ["enabled", "storageClassName", "size"],
+          "properties": {
+            "enabled": {"type": "boolean"},
+            "storageClassName": {"type": "string", "minLength": 1},
+            "size": {"type": "string", "pattern": "^[1-9][0-9]*(Mi|Gi)$"}
+          }
+        }
+      }
     }
   },
   "definitions": {
```

`charts/storage-integrity/tests/validate_source_binding.py`:

```diff
@@ -12,6 +12,9 @@
 LEGACY_SOURCE_DIGEST = "sha256:56cf2d16d031bf9a9e4996f35c96bec7978c329e3a63dbfe18e72d879f4f195e"
 LEGACY_NAMESPACE = "sentio-network-devnet2"
 LEGACY_GENESIS = ("devnet2", "devnet2-si-v1")
+# payloadexec.SchemaRoot(<any network>, nil): the schema root of an SNode that
+# owns no genesis table (housegate spec 2026-10-10 §8).
+EMPTY_SCHEMA_ROOT = "0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59"
 # Resources of release storage-integrity, which holds the legacy history.
 LEGACY_RELEASE_RESOURCES = {
     ("StatefulSet", "storage-integrity-arbiter"),
@@ -55,11 +58,19 @@
     return actual in (local, f"{host}.{namespace}.svc.cluster.local:{port}")
 
 
-def validate_binding(control, source, image_values, namespace):
+def validate_binding(control, source, image_values, namespace, source_clickhouse=None):
     """Validate initial bootstrap identity, routing, and the selected host image.
 
     Both releases deploy into namespace. In the devnet2 namespace the control
     plane must be a separate release beside the legacy storage-integrity one.
+
+    source_clickhouse is the host:port of the SI indexer's own source
+    ClickHouse. None selects the control render's bundled source, which belongs
+    to the founding SI indexer alone: that indexer owns every genesis table,
+    while an indexer enrolled later brings its own source ClickHouse, owns no
+    genesis table, uses the empty set's schema root and must name the genesis
+    snapshot id, which it cannot derive (housegate spec 2026-10-10 §6.3, §8,
+    §12).
     """
     if not namespace:
         raise ValueError("a namespace is required")
@@ -82,7 +93,12 @@
     arbiter = config_data(control, "arbiter.yaml.tmpl", "Arbiter config")
     if "consensus_updates_enabled" not in arbiter or not arbiter.get("tables"):
         raise ValueError("control render must use the fresh administration-capable profile")
-    source_clickhouse = config_data(control, "schema.yaml", "bootstrap schema config")["clickhouse_addr"]
+    bundled_clickhouse = config_data(control, "schema.yaml", "bootstrap schema config")["clickhouse_addr"]
+    founding = source_clickhouse is None
+    if founding:
+        source_clickhouse = bundled_clickhouse
+    elif routes_to(source_clickhouse, bundled_clickhouse, namespace):
+        raise ValueError("an SI indexer enrolled after genesis must not use the network's bundled source ClickHouse: it belongs to the founding indexer")
     host = config_data(source, "standalone.yaml", "source standalone config")
     containers = [
         container
@@ -113,15 +129,34 @@
         raise ValueError("HouseGate ingress network differs from source SNode network")
     if f".{namespace}.svc." not in host["shared"]["driver_config"]["processor_service"]:
         raise ValueError("source rendered namespace differs from the selected namespace")
-    for key in ("network_id", "schema_snapshot_id", "executor_profile_id", "schema_root"):
+    for key in ("network_id", "schema_snapshot_id", "executor_profile_id"):
         if snode[key] != arbiter["genesis"][key]:
             raise ValueError(f"source/control {key} mismatch")
     # networkID stays the L2 registry's schema-hash domain; the schema snapshot
     # ID is what separates a re-genesis from the legacy devnet2 history.
     if (snode["network_id"], snode["schema_snapshot_id"]) == LEGACY_GENESIS:
         raise ValueError("fresh source must not use the legacy devnet2 genesis identity")
-    if snode["table_ids"] != [table["table_id"] for table in arbiter["tables"]]:
-        raise ValueError("source/control table identities mismatch")
+    genesis_tables = [table["table_id"] for table in arbiter["tables"]]
+    # Every SI indexer's enrolment statement binds the network's genesis
+    # snapshot id: an SNode with genesis tables checks a configured one against
+    # them at startup, one without cannot derive it.
+    genesis_snapshot_id = snode.get("genesis_snapshot_id")
+    if genesis_snapshot_id is not None and not (
+        isinstance(genesis_snapshot_id, str) and re.fullmatch(r"0x[0-9a-f]{64}", genesis_snapshot_id)
+    ):
+        raise ValueError("source genesis_snapshot_id must be lowercase 0x-prefixed 32-byte hex")
+    if founding:
+        if snode["table_ids"] != genesis_tables:
+            raise ValueError("source/control table identities mismatch: the founding indexer's SNode lists every genesis table")
+        expected_schema_root = arbiter["genesis"]["schema_root"]
+    else:
+        if snode["table_ids"]:
+            raise ValueError("source/control table identities mismatch: an SI indexer enrolled after genesis owns no genesis table")
+        if genesis_snapshot_id is None:
+            raise ValueError("an SNode without genesis tables needs the network's genesis_snapshot_id")
+        expected_schema_root = EMPTY_SCHEMA_ROOT
+    if snode["schema_root"] != expected_schema_root:
+        raise ValueError("source/control schema_root mismatch: an SNode's root covers the genesis tables it owns")
     if addresses(snode["authority_addresses"]) != addresses(arbiter["authority"]["allowed_addresses"]):
         raise ValueError("initial source authorities must match the bootstrap trust root")
     expected_peers = [
@@ -184,11 +219,15 @@
     parser.add_argument("--source-render", type=Path, required=True)
     parser.add_argument("--source-image-values", type=Path, required=True)
     parser.add_argument("--namespace", required=True, help="namespace used for both Helm renders and later deployment")
+    parser.add_argument("--source-clickhouse", help="host:port of the SI indexer's own source ClickHouse; omit for the founding indexer, which uses the control render's bundled source")
     args = parser.parse_args()
     try:
         control = [doc for doc in yaml.safe_load_all(args.control_render.read_text()) if doc]
         source = [doc for doc in yaml.safe_load_all(args.source_render.read_text()) if doc]
-        image = validate_binding(control, source, yaml.safe_load(args.source_image_values.read_text()), args.namespace)
+        image = validate_binding(
+            control, source, yaml.safe_load(args.source_image_values.read_text()), args.namespace,
+            source_clickhouse=args.source_clickhouse,
+        )
     except (KeyError, TypeError, ValueError, yaml.YAMLError) as error:
```

The later routing checks (`routes_to(si["clickhouse_dsn"], source_clickhouse, …)`, `routes_to(host["housegate"].get("upstream"), source_clickhouse, …)`) are unchanged and now compare with the indexer's own source.

Run: `cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3`
Expected: `Ran 72 tests` and `OK` (measured on a copy of `origin/main`).

```bash
cd "$W" && git add charts/sentio-node charts/storage-integrity && git commit -m "feat(storage-integrity): owner-specific SNode genesis sets, per-indexer sources, verifier state

An SI indexer enrolled after genesis owns no genesis table (housegate spec
2026-10-10 §6.3, §8, §10): the sentio-node chart accepts an empty
storageIntegrity.snode.tableIDs in the registry-driven shape with the empty
set's schema root, and renders the new genesisSnapshotID, which every SI
indexer names (its enrolment statement binds it) and an SNode without
genesis tables requires. validate_binding takes the indexer's own source
ClickHouse (the bundled one stays the founding indexer's). verifier.state
gives each verifier a persistent state_dir for registration.json.
genesis.maxWriters stays the immutable bootstrap cap.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: Failing devnet2 tests**

In `charts/storage-integrity/tests/test_render.py`:

After `DEVNET2_CONSENSUS_UPDATES_ENABLED = True` (line 33):

```python
# verifier.state.enabled on devnet2-si-v2; stage 1 of
# docs/storage-integrity-devnet2-multi-source.md (§3.1) changes it together
# with DEVNET2_ARBITER_DIGEST.
DEVNET2_VERIFIER_STATE_ENABLED = False
```

Append to `WRITER_AUTHORIZATION_IMAGES` (before its closing `)` at line 55; the image carries D17 because it is built on `HG_TAG`, which contains housegate v0.17.0's writer authorization):

```python
    "ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>"
    "@<SN_DIGEST>",
```

Append to `FRESH_FILES` (before its closing `]` at line 67):

```python
    DEVNET2 / "sentio-node-indexer-b-storage-integrity-si-v2.yaml",
    DEVNET2 / "sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml",
    DEVNET2 / "storage-integrity-si-v2-indexer-b-source.yaml",
```

Insert after `DEVNET2_TABLE_SCHEMA_HASH = …` (line 75):

```python
# devnet2-si-v2's genesis snapshot id (arbiter-admin consensus show). Both SI
# indexers' SNodes name it: their enrolment statements bind it, indexer-a's SNode
# checks it against its genesis tables, and indexer-b's owns none to derive it from.
DEVNET2_GENESIS_SNAPSHOT_ID = "0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06"
# indexer-b's own source ClickHouse (not Helm-managed; runbook
# docs/storage-integrity-devnet2-multi-source.md §6.2).
INDEXER_B_CHI = DEVNET2 / "storage-integrity-si-v2-indexer-b-source.yaml"
# The source image of indexer-b's SI release, the multi-source stage-1 build
# (signed SNode messages, signed_claims_v1). Mirrors
# sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml; indexer-a
# moves to the same image in stage 1 (runbook §4).
INDEXER_B_SOURCE_IMAGE = (
    "ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>"
    "@<SN_DIGEST>"
)
# SNode node ids of devnet2-si-v2 by sentio-node release. Append only: an id is
# never reused, even after its indexer leaves (housegate spec 2026-10-10 §12;
# the arbiter refuses an enrolment whose id ever registered).
DEVNET2_SNODE_IDS = {
    "sentio-node-devnet2-indexer-a": "snode-1",
    "sentio-node-devnet2-indexer-b": "snode-2",
}
```

Insert after `host_config` (Step 2):

```python
def indexer_b_source_clickhouse():
    """indexer-b's own source ClickHouse: the cluster Service {chi}-{cluster}."""
    chi = yaml.safe_load(INDEXER_B_CHI.read_text())
    (cluster,) = chi["spec"]["configuration"]["clusters"]
    return f'{chi["metadata"]["name"]}-{cluster["name"]}:9000'


```

In `ActualFreshNetworkTest.setUpClass`, after `cls.source = documents(cls.render_release(cls.source_release))` (line 1126):

```python
        cls.indexer_b_release = cls.releases[(LEGACY_NAMESPACE, "sentio-node-devnet2-indexer-b")]
        cls.indexer_b = documents(cls.render_release(cls.indexer_b_release))
```

In `test_writer_authorization_needs_a_reviewed_image`, after line 1280:

```python
        self.assertIn(f"{FRESH_HELMFILE.relative_to(ROOT)}:sentio-node-devnet2-indexer-b", checked)
```

Append at the end of `ActualFreshNetworkTest` (after `test_operator_inputs_are_filled_with_distinct_addresses`, line 1439; reuses `release_values`, `render_release`, `agent_config`, `arbiter_config`, `config_for`, `pod_template`, `init_rendered_arbiter_config`, `validate_binding`, `addresses`):

```python

    def test_indexer_b_extends_its_ordinary_release_and_merges_the_source_pin_last(self):
        ordinary = {
            release["name"]: release
            for release in yaml.safe_load((DEVNET2 / "helmfile.yaml").read_text())["releases"]
        }["sentio-node-devnet2-indexer-b"]
        release = self.indexer_b_release
        self.assertEqual((release["chart"], release["set"]), (ordinary["chart"], ordinary["set"]))
        self.assertEqual(release["values"][:3], ordinary["values"][:3])
        self.assertEqual(release["values"][3], "./sentio-node-indexer-b-storage-integrity-si-v2.yaml")
        self.assertEqual(release["values"][4], ordinary["values"][3])
        self.assertEqual(release["values"][-1], "./sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml")
        self.assertEqual(release["needs"], [f"{LEGACY_NAMESPACE}/{FRESH_RELEASE}"])
        pin = yaml.safe_load((FRESH_HELMFILE.parent / release["values"][-1]).read_text())["node"]["image"]
        self.assertEqual(f'{pin["repository"]}:{pin["tag"]}', INDEXER_B_SOURCE_IMAGE)
        self.assertIn(INDEXER_B_SOURCE_IMAGE, WRITER_AUTHORIZATION_IMAGES)

    def test_indexer_b_binds_to_the_network_with_its_own_source_clickhouse(self):
        image_values = yaml.safe_load((FRESH_HELMFILE.parent / self.indexer_b_release["values"][-1]).read_text())
        control, source = list(self.control.values()), list(self.indexer_b.values())
        self.assertEqual(
            validate_binding(control, source, image_values, LEGACY_NAMESPACE, source_clickhouse=indexer_b_source_clickhouse()),
            INDEXER_B_SOURCE_IMAGE,
        )
        bundled = config_for(self.control, "bootstrap", FRESH_RELEASE)["clickhouse_addr"]
        with self.assertRaisesRegex(ValueError, "bundled source"):
            validate_binding(control, source, image_values, LEGACY_NAMESPACE, source_clickhouse=bundled)
        with self.assertRaisesRegex(ValueError, "genesis table"):
            validate_binding(control, source, image_values, LEGACY_NAMESPACE)
        host = host_config(self.indexer_b)
        self.assertEqual(host["node"]["storage_node_rpc_port"], 33003)
        self.assertEqual(self.agent_config(self.indexer_b)["network_state"], {"source": "http://localhost:33003"})
        self.assertEqual(addresses(host["housegate"]["storage_integrity"]["ingress"]["allowed_addresses"]), [])

    def test_si_indexers_target_one_network_release(self):
        """Both SI overlays join one control plane, DA, genesis and authority."""
        def network(docs):
            host = host_config(docs)
            si = host["storage_integrity"]
            return {
                "arbiter_peers": si["arbiter_peers"],
                "payload_store_data_addr": si["payload_store_data_addr"],
                "snode": {key: si["snode"][key] for key in (
                    "network_id", "schema_snapshot_id", "executor_profile_id",
                    "genesis_snapshot_id", "authority_addresses", "schema_source",
                )},
                "ingress_network_id": host["housegate"]["storage_integrity"]["ingress"]["network_id"],
                "agent_network_id": self.agent_config(docs)["storage_integrity"]["agent"]["network_id"],
            }

        self.assertEqual(network(self.indexer_b), network(self.source))
        for release in (self.source_release, self.indexer_b_release):
            self.assertEqual(release["needs"], [f"{LEGACY_NAMESPACE}/{FRESH_RELEASE}"])

    def test_snode_node_ids_are_unique_and_never_a_verifier_or_voter_id(self):
        rendered = {"sentio-node-devnet2-indexer-a": self.source, "sentio-node-devnet2-indexer-b": self.indexer_b}
        self.assertEqual(
            {name for (_, name), release in self.releases.items()
             if (FRESH_HELMFILE.parent / release["chart"]).resolve() == SENTIO_NODE_CHART},
            set(rendered),
            "every SI indexer release of the network is checked here",
        )
        ids = {}
        for name, docs in rendered.items():
            host = host_config(docs)
            ids[name] = host["storage_integrity"]["snode"]["node_id"]
            self.assertEqual(host["housegate"]["storage_integrity"]["runtime"]["expected_source"], ids[name])
        self.assertEqual(ids, DEVNET2_SNODE_IDS)
        self.assertEqual(len(set(ids.values())), len(ids))
        script = pod_template(self.control, "verifier", FRESH_RELEASE)["spec"]["initContainers"][0]["args"][0]
        self.assertIn('replica_id="verifier-${replica_num}"', script)
        verifiers = self.control[("StatefulSet", f"{FRESH_RELEASE}-verifier")]["spec"]["replicas"]
        voters = self.control[("StatefulSet", f"{FRESH_RELEASE}-arbiter")]["spec"]["replicas"]
        taken = {f"verifier-{n}" for n in range(1, verifiers + 1)} | {
            init_rendered_arbiter_config(self.control, n, FRESH_RELEASE)["node_id"] for n in range(voters)
        }
        self.assertEqual(set(ids.values()) & taken, set())

    def test_each_snode_lists_only_the_genesis_tables_its_indexer_owns(self):
        arbiter = arbiter_config(self.control, FRESH_RELEASE)
        genesis = [table["table_id"] for table in arbiter["tables"]]
        a = host_config(self.source)["storage_integrity"]["snode"]
        b = host_config(self.indexer_b)["storage_integrity"]["snode"]
        # si_indexers entry 0 restates the founding indexer, which owns every
        # genesis table; an indexer enrolled later owns none and uses the empty
        # set's schema root (spec §6.3, §10). Both name the network's genesis
        # snapshot id, which their enrolment statements bind.
        self.assertEqual(
            (a["table_ids"], a["schema_root"], a.get("genesis_snapshot_id")),
            (genesis, arbiter["genesis"]["schema_root"], DEVNET2_GENESIS_SNAPSHOT_ID),
        )
        self.assertEqual(
            (b["table_ids"], b["schema_root"], b.get("genesis_snapshot_id")),
            ([], EMPTY_SCHEMA_ROOT, DEVNET2_GENESIS_SNAPSHOT_ID),
        )
        for docs in (self.source, self.indexer_b):
            self.assertNotIn("tables", host_config(docs)["housegate"]["storage_integrity"])
            # sentio-node derives the SNode's indexer id from its on-chain
            # registration; no chart value may override it.
            self.assertNotIn("indexer_id", host_config(docs)["storage_integrity"]["snode"])

    def test_si_indexers_never_share_a_source_clickhouse(self):
        sources = {}
        for name, docs in (("indexer-a", self.source), ("indexer-b", self.indexer_b)):
            host = host_config(docs)
            # The signed lane round-trips through the ordinary table on
            # HouseGate's upstream, which must be the ClickHouse its own SNode reads.
            self.assertEqual(host["housegate"]["upstream"], host["storage_integrity"]["clickhouse_dsn"])
            sources[name] = host["storage_integrity"]["clickhouse_dsn"]
        self.assertEqual(sources, {
            "indexer-a": config_for(self.control, "bootstrap", FRESH_RELEASE)["clickhouse_addr"],
            "indexer-b": indexer_b_source_clickhouse(),
        })

    def test_indexer_b_source_clickhouse_carries_the_network_source_profile(self):
        chi = yaml.safe_load(INDEXER_B_CHI.read_text())
        network = self.control[("ClickHouseInstallation", FRESH_RELEASE)]
        self.assertEqual((chi["kind"], chi["metadata"]["namespace"]), ("ClickHouseInstallation", LEGACY_NAMESPACE))
        self.assertNotIn(chi["metadata"]["name"], {FRESH_RELEASE, "storage-integrity", "clickhouse-devnet2"})
        # Not part of the network's Helm release, so no selector of it matches these pods.
        self.assertNotIn("app.kubernetes.io/instance", chi["metadata"].get("labels") or {})
        configuration = chi["spec"]["configuration"]
        # Keeper: the network's ensemble, timeouts and root (spec §8).
        self.assertEqual(configuration["zookeeper"], network["spec"]["configuration"]["zookeeper"])
        self.assertEqual(configuration["zookeeper"]["root"], "/storage-integrity/devnet2/devnet2-si-v2")
        self.assertEqual(configuration["clusters"], [{"name": "source", "layout": {"shardsCount": 1, "replicasCount": 1}}])
        # The network installation's server build, settings, users and
        # templates: the verifier replicas fetch from it, and the SNode opens
        # its ClickHouse connection as the passwordless default user.
        for key in ("settings", "files", "users", "profiles"):
            self.assertEqual(configuration[key], network["spec"]["configuration"][key], key)
        self.assertEqual(chi["spec"]["defaults"], network["spec"]["defaults"])
        self.assertEqual(chi["spec"]["templates"], network["spec"]["templates"])
        replica = next(
            template for template in chi["spec"]["templates"]["serviceTemplates"]
            if template["name"] == chi["spec"]["defaults"]["templates"]["replicaServiceTemplate"]
        )
        self.assertIn({"name": "interserver", "port": 9009}, replica["spec"]["ports"])

    def test_devnet2_genesis_writer_cap_stays_one(self):
        # genesis.max_writers is the immutable bootstrap value every voter checks
        # on restore (arbiter README; fsm/consensus_history.go). Stage 1 raises
        # the runtime limit to len(si_indexers) with the enrolment update
        # (arbiter-admin consensus update --max-writers 2), never here.
        self.assertEqual(arbiter_config(self.control, FRESH_RELEASE)["genesis"]["max_writers"], 1)
        self.assertNotIn("genesis", yaml.safe_load((DEVNET2 / "storage-integrity-si-v2.yaml").read_text()))

    def test_devnet2_verifier_state_follows_the_rollout(self):
        # docs/storage-integrity-devnet2-multi-source.md §3.1 turns it on together
        # with the stage-1 arbiter (DEVNET2_VERIFIER_STATE_ENABLED).
        verifier = config_for(self.control, "verifier", FRESH_RELEASE)
        spec = self.control[("StatefulSet", f"{FRESH_RELEASE}-verifier")]["spec"]
        if DEVNET2_VERIFIER_STATE_ENABLED:
            self.assertEqual(verifier["state_dir"], "/state")
            self.assertEqual([template["metadata"]["name"] for template in spec["volumeClaimTemplates"]], ["state"])
        else:
            self.assertNotIn("state_dir", verifier)
            self.assertNotIn("volumeClaimTemplates", spec)
```

Fill the ledger values into the test file (`sed -i '' -e "s/<SN_COMMIT>/$SN_COMMIT/g" -e "s/<SN_DIGEST>/$SN_DIGEST/g" charts/storage-integrity/tests/test_render.py`), then run: `cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py ActualFreshNetworkTest 2>&1 | grep -E 'KeyError|^Ran|FAILED'`
Expected: `KeyError: ('sentio-network-devnet2', 'sentio-node-devnet2-indexer-b')` from `setUpClass`, `Ran 0 tests`, `FAILED (errors=1)`.

- [ ] **Step 5: indexer-b overlay, source pin, source ClickHouse, helmfile release, CI paths**

Create `k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2.yaml`:

```yaml
# Opt-in overlay selected only by storage-integrity-si-v2.helmfile.yaml. It binds
# indexer-b (on-chain indexer 1), with its unchanged on-chain registration,
# gateway ports 33001-33003 and PVC, to the devnet2-si-v2 storage-integrity
# network as its second SI indexer (housegate spec 2026-10-10, stage 1;
# docs/storage-integrity-devnet2-multi-source.md). It differs from indexer-a's
# overlay only in indexer-b's own values: SNode node id snode-2, its own source
# ClickHouse storage-integrity-si-v2-indexer-b-source
# (storage-integrity-si-v2-indexer-b-source.yaml), storage RPC
# http://localhost:33003, and no genesis tables (every devnet2-si-v2 genesis
# table belongs to the founding indexer, indexer-a), hence the empty set's
# schema root. Like indexer-a's, it names the network's genesis snapshot id.
# Ports, signer key and state directories come from indexer-b's own values and
# PVC.
#
# Until indexer-b's enrolment commits, sentio-node starts with its SNode
# registration deferred (it registers once the table registry carries its
# si_indexers entry) and answers ordinary for every table.
# Until then helmfile.yaml (ClickHouse clickhouse-devnet2-node-b, no storage
# integrity) stays indexer-b's rollback surface; whichever file last synced
# indexer-b decides which ClickHouse and network it uses.
#
# Every route names storage-integrity-si-v2-*: the legacy devnet2-si-v1 release
# keeps its storage-integrity-* Services in this namespace. The source image pin
# is merged last from sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml.

clickhouse:
  address: storage-integrity-si-v2-indexer-b-source:9000
  username: default
  password: ""

rewriter:
  image:
    repository: us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter
    # rewriter-grpc v0.17.0 (b56ae37), as on indexer-a: the embedded HouseGate
    # probes the table-reference policy at startup and refuses an older
    # rewriter, so this pin moves together with the source image pin.
    tag: "0.17.0@sha256:b3b227fa4a28bd9dcd5d3d6aa172890ff66d7bf60e1c7039428efe4dabebb50a"
    pullPolicy: IfNotPresent

housegate:
  storageIntegrity:
    # No tables list: sentio-node injects HouseGate's table state from the
    # embedded SNode and the table registry.
    enabled: true
    read:
      defaultMode: unsafe_latest
    ingress:
      enabled: true
      networkID: devnet2
      # The source image carries HouseGate's contract writer authorization
      # (housegate spec 2026-10-09 D17); test_render.py pins it.
      writerAuthorization: true
      # Open to every database writer (contract isDatabaseWriter), as on indexer-a.
      allowedAddresses: []
      deniedAddresses: []
      maxTokenAge: 1m
      requestTimeout: 30s
      maxPayloadBytes: 67108864
    runtime:
      enabled: true
      expectedSource: snode-2
      # All SI state of this network lives under /data/storage-integrity-devnet2-si-v2
      # on indexer-b's own PVC.
      journalDir: /data/storage-integrity-devnet2-si-v2/housegate-journal
      payloadSpoolDir: /data/storage-integrity-devnet2-si-v2/housegate-spool
      payloadLease:
        refreshInterval: 5s
        refreshBefore: 30s
      mergeGuard:
        reassertInterval: 30s
      backpressure:
        enabled: true
        unsafeDatabase: hg_unsafe
        safeDatabase: hg_safe
        pollInterval: 2s
        softPartsPerPartition: 2400
        hardPartsPerPartition: 2950

storageIntegrity:
  enabled: true
  arbiterPeers:
    - id: storage-integrity-si-v2-arbiter-0
      addr: storage-integrity-si-v2-arbiter-0.storage-integrity-si-v2-arbiter-raft.sentio-network-devnet2.svc.cluster.local:7080
    - id: storage-integrity-si-v2-arbiter-1
      addr: storage-integrity-si-v2-arbiter-1.storage-integrity-si-v2-arbiter-raft.sentio-network-devnet2.svc.cluster.local:7080
    - id: storage-integrity-si-v2-arbiter-2
      addr: storage-integrity-si-v2-arbiter-2.storage-integrity-si-v2-arbiter-raft.sentio-network-devnet2.svc.cluster.local:7080
  payloadStoreDataAddr: storage-integrity-si-v2-da:9001
  clickhouseDSN: storage-integrity-si-v2-indexer-b-source:9000
  snode:
    # The si_indexers entry of indexer 1 names this id, and it is the replica
    # name of every hg_unsafe table indexer-b owns. Never reuse it (runbook §1.2).
    nodeID: snode-2
    stateDir: /data/storage-integrity-devnet2-si-v2/snode
    # Must equal storage-integrity-si-v2.yaml.
    networkID: devnet2
    schemaSnapshotID: devnet2-si-v2
    executorProfileID: housegate-replay-mvp-v0
    # The schema root covers the genesis tables this SNode owns: none, so it is
    # payloadexec.SchemaRoot(<any network>, nil), not the network's root.
    schemaRoot: "0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59"
    # The network's genesis snapshot id (arbiter-admin consensus show:
    # genesis_snapshot_id), as on indexer-a: the enrolment statement binds it,
    # and an SNode without genesis tables cannot derive it.
    genesisSnapshotID: "0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06"
    authorityAddresses:
      - "0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705"
    # The genesis tables indexer-b owns: none. Its tables enter storage
    # integrity through the registry from its activation block on.
    tableIDs: []
    # Verifies the on-chain declaration, which is bound to network devnet2.
    schemaSource: network_state

housegateSidecar:
  image:
    repository: ghcr.io/housegate/housegate
    # housegate v0.15.0, as on indexer-a: the agent asks this pod's storage RPC
    # for each table's storage-integrity status and signs only Active tables.
    tag: "v0.15.0@sha256:1b51de76b7b7a15a8006d0ac06febb10012c74c92d4950d07e4e5a5b77b3570f"
    pullPolicy: IfNotPresent
  # The SI ingress rejects compressed INSERT payloads.
  dsnParams: ""
  # Required by inlineValues.
  materialize:
    enabled: true
  storageIntegrity:
    enabled: true
    networkID: devnet2
    keeperShardID: 0
    # A new, empty client sequence for this network on indexer-b's PVC.
    stateDir: /data/storage-integrity-devnet2-si-v2/agent
    maxPayloadBytes: 67108864
    # This pod's storage-node JSON-RPC (node.ports.storage_rpc of indexer-b).
    networkStateSource: http://localhost:33003
    inlineValues:
      enabled: true
```

Create `k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml`:

```yaml
# Merged LAST into indexer-b by storage-integrity-si-v2.helmfile.yaml: the
# multi-source stage-1 build (housegate spec 2026-10-10 §13), sentio-node
# <SN_COMMIT> on housegate <HG_TAG> (SOURCE_UNAVAILABLE as a retryable,
# session-preserving refusal, on top of v0.17.2's lanes, writer authorization,
# denylist and sipeerguard) and arbiter-core <CORE_TAG> (SNode messages signed
# with the indexer key, owner-scoped data plane, signed_claims_v1, deferred
# registration before enrolment). indexer-a moves to the same image in stage 1
# (docs/storage-integrity-devnet2-multi-source.md §4). test_render.py mirrors
# this pin as INDEXER_B_SOURCE_IMAGE and lists it in WRITER_AUTHORIZATION_IMAGES.
# Its startup probe needs rewriter-grpc v0.17.0 or later, pinned in
# sentio-node-indexer-b-storage-integrity-si-v2.yaml.
# Way back: until indexer-b's enrolment commits, sync indexer-b from helmfile.yaml;
# afterwards never run an image without signed_claims_v1 on this node.
node:
  image:
    repository: ghcr.io/sentioxyz/sentio-node
    tag: "sha-<SN_COMMIT>@<SN_DIGEST>"
    pullPolicy: IfNotPresent
```

Create `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2-indexer-b-source.yaml` (every value under `spec` equals the rendered network installation `storage-integrity-si-v2` except the single `source` cluster; the test compares them):

```yaml
# Source ClickHouse of devnet2 indexer-b (on-chain indexer 1) in the
# devnet2-si-v2 storage-integrity network (housegate spec 2026-10-10 §8, §12;
# docs/storage-integrity-devnet2-multi-source.md §6.2). Not Helm-managed: apply
# it with kubectl. Release storage-integrity-si-v2 keeps the arbiter, the
# verifiers with their ClickHouses, DA and indexer-a's bundled source; this
# installation is indexer-b's own, so no two SNodes ever write one ClickHouse.
#
# The source profile of spec §8, mirrored from charts/storage-integrity's
# installation (test_render.py compares the two):
# - Keeper: the network's clickhouse-keeper-extra ensemble under the network
#   root /storage-integrity/devnet2/devnet2-si-v2, which already exists (the
#   network's own installation uses it). indexer-b's SNode replicates its
#   hg_unsafe tables at /sentio/0/unsafe/<db>__<table> below it, with replica
#   name snode-2.
# - Interserver: the operator advertises the replica Service
#   storage-integrity-si-v2-indexer-b-source-replica-0 as interserver_http_host
#   and the Service exposes 9009, so the verifier ClickHouses in this namespace
#   fetch indexer-b's parts from it.
# - hg_safe / hg_unsafe / hg_promote: indexer-b's SNode creates them with the
#   DDL-pinned settings; nothing here pre-creates them.
# - The default user has no password and is reachable in-cluster, as on the
#   network installation: the SNode opens its ClickHouse connection without
#   credentials (sentio-node standalone/standalone.go, clickhouse.Options{Addr}).
#   That is why clickhouse-devnet2-node-b could not take this profile without a
#   user change shared with node-a (users are installation-wide).
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: storage-integrity-si-v2-indexer-b
  namespace: sentio-network-devnet2
spec:
  defaults:
    templates:
      podTemplate: clickhouse
      dataVolumeClaimTemplate: data-volume-claim
      logVolumeClaimTemplate: log-volume-claim
      clusterServiceTemplate: cluster-service
      replicaServiceTemplate: replica-service
    storageManagement:
      reclaimPolicy: Retain
  configuration:
    zookeeper:
      nodes:
        - host: clickhouse-keeper-extra-0.clickhouse.svc.cluster.local
          port: 2181
        - host: clickhouse-keeper-extra-1.clickhouse.svc.cluster.local
          port: 2181
        - host: clickhouse-keeper-extra-2.clickhouse.svc.cluster.local
          port: 2181
      session_timeout_ms: 100000
      operation_timeout_ms: 10000
      root: /storage-integrity/devnet2/devnet2-si-v2
    clusters:
      - name: source
        layout: {shardsCount: 1, replicasCount: 1}
    settings:
      logger/level: warning
      logger/console: "true"
      prometheus/endpoint: /metrics
      prometheus/port: 9363
      prometheus/metrics: true
      prometheus/events: true
      prometheus/asynchronous_metrics: true
      prometheus/status_info: true
      max_concurrent_queries: 500
      max_server_memory_usage_to_ram_ratio: "0.85"
    files:
      zz-pool-settings.xml: |
        <clickhouse>
          <background_pool_size>16</background_pool_size>
          <background_fetches_pool_size>8</background_fetches_pool_size>
        </clickhouse>
    users:
      default/password: ""
      default/networks/ip:
        - 0.0.0.0/0
      default/access_management: "1"
    profiles:
      default/query_cache_nondeterministic_function_handling: ignore
      default/enable_json_type: "1"
  templates:
    podTemplates:
      - name: clickhouse
        generateName: "{chi}-{cluster}-{host}"
        podDistribution:
          - type: ClusterAffinity
        metadata:
          annotations:
            prometheus.io/scrape: "true"
            prometheus.io/path: /metrics
            prometheus.io/port: "9363"
        spec:
          automountServiceAccountToken: false
          nodeSelector:
            sentio.xyz/general: "true"
          imagePullSecrets:
            - name: google-image-secret
          containers:
            - name: clickhouse
              image: us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:26.3-lts-decimal512-release-2026-09-16
              ports:
                - name: metrics
                  containerPort: 9363
              volumeMounts:
                - name: data-volume-claim
                  mountPath: /var/lib/clickhouse
              resources:
                requests:
                  cpu: "2"
                  memory: 4Gi
                limits:
                  cpu: "8"
                  memory: 16Gi
    volumeClaimTemplates:
      - name: data-volume-claim
        spec:
          accessModes: [ReadWriteOnce]
          storageClassName: local-path
          resources:
            requests:
              storage: 10Gi
      - name: log-volume-claim
        spec:
          accessModes: [ReadWriteOnce]
          storageClassName: local-path
          resources:
            requests:
              storage: 1Gi
    serviceTemplates:
      - name: cluster-service
        generateName: "{chi}-{cluster}"
        spec:
          ports:
            - {name: http, port: 8123}
            - {name: tcp, port: 9000}
          type: ClusterIP
      - name: replica-service
        generateName: "{chi}-{cluster}-replica-{replica}"
        spec:
          ports:
            - {name: http, port: 8123}
            - {name: tcp, port: 9000}
            - {name: interserver, port: 9009}
          type: ClusterIP
```

`k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml`:

```diff
@@ -10,7 +10,10 @@
 # storage-integrity release in this namespace. For indexer-a,
 # storage-integrity.helmfile.yaml (devnet2-si-v1) and helmfile.yaml (no storage
 # integrity) remain the rollback surfaces; whichever file last synced indexer-a
-# decides which network it writes to.
+# decides which network it writes to. indexer-b joins as the second SI indexer
+# (docs/storage-integrity-devnet2-multi-source.md); helmfile.yaml stays its
+# rollback surface only until its enrolment commits. Always sync one release
+# with --selector name=<release>.
 releases:
   - name: storage-integrity-si-v2
     namespace: sentio-network-devnet2
@@ -36,3 +39,23 @@
       - nodeSelector:
           kubernetes.io/hostname: "sea-g-3"
       - ./sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml
+
+  # The second SI indexer (housegate spec 2026-10-10, stage 1): indexer-b with
+  # its own source ClickHouse (storage-integrity-si-v2-indexer-b-source.yaml,
+  # applied with kubectl before the first sync of this release).
+  - name: sentio-node-devnet2-indexer-b
+    namespace: sentio-network-devnet2
+    chart: ../../charts/sentio-node
+    needs:
+      - sentio-network-devnet2/storage-integrity-si-v2
+    set:
+      - name: service.chainsConfig
+        file: ../../charts/sentio-node/chains-config.json
+    values:
+      - ./common/sentio-node.yaml
+      - ./common/sentio-node-indexer.yaml
+      - ./sentio-node-indexer-b.yaml
+      - ./sentio-node-indexer-b-storage-integrity-si-v2.yaml
+      - nodeSelector:
+          kubernetes.io/hostname: "sea-g-4"
+      - ./sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml
```

`k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2.yaml` (the genesis snapshot id is needed by indexer-a's enrolment statement, runbook §5.2, and checked by the stage-1 image at startup; ff336f1 ignores it):

```diff
@@ -1,7 +1,8 @@
 # Opt-in overlay selected only by storage-integrity-si-v2.helmfile.yaml. It binds
 # indexer-a, with its unchanged on-chain registration, gateway ports and PVC, to
 # the devnet2-si-v2 control plane: release storage-integrity-si-v2 in this
-# namespace. Indexer-b receives no overlay.
+# namespace. indexer-b has its own overlay,
+# sentio-node-indexer-b-storage-integrity-si-v2.yaml.
 #
 # Every route names storage-integrity-si-v2-*. The legacy devnet2-si-v1 release
 # keeps its storage-integrity-* Services in this namespace, so a name without
@@ -94,6 +95,11 @@
     schemaSnapshotID: devnet2-si-v2
     executorProfileID: housegate-replay-mvp-v0
     schemaRoot: "0x4604a800560a07d08516b13b7084e8ee00436055fdc8698912f8b60937eb2732"
+    # The network's genesis snapshot id (arbiter-admin consensus show:
+    # genesis_snapshot_id), which this indexer's enrolment statement binds.
+    # From the multi-source stage-1 image on, the SNode checks it against its
+    # genesis tables at startup; sentio-node ff336f1 ignores the key.
+    genesisSnapshotID: "0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06"
     authorityAddresses:
       - "0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705"
     tableIDs:
```

`.github/workflows/storage-integrity-chart.yml`: in both the `pull_request.paths` and the `push.paths` lists, after `- k8s-sea/sentio-network-devnet2/sentio-node-indexer-a*.yaml` add

```yaml
      - k8s-sea/sentio-network-devnet2/sentio-node-indexer-b*.yaml
```

(The CHI file is already covered by `k8s-sea/sentio-network-devnet2/storage-integrity*`.)

Fill the ledger values and check nothing is left:

```bash
cd "$W" && sed -i '' -e "s/<SN_COMMIT>/$SN_COMMIT/g" -e "s/<SN_DIGEST>/$SN_DIGEST/g" -e "s/<HG_TAG>/$HG_TAG/g" -e "s/<CORE_TAG>/$CORE_TAG/g" \
  k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml charts/storage-integrity/tests/test_render.py
! git grep -nE '<(SN_COMMIT|SN_DIGEST|HG_TAG|CORE_TAG)>' -- k8s-sea charts
```

Run the tests, the CI lints, the indexer-b render, and the inertness proof (every existing release renders byte-identically to `origin/main` except indexer-a's SI release, which gains only its `genesis_snapshot_id`):

```bash
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -3
helm lint charts/storage-integrity -f k8s-sea/sentio-network-devnet2/storage-integrity.yaml --namespace sentio-network-devnet2 --strict
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --strict
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --set arbiter.consensusUpdatesEnabled=true --set arbiter.authoritySigningEnabled=false --strict
helm lint charts/storage-integrity -f charts/storage-integrity/tests/fresh-values.yaml --namespace storage-integrity-render-test --set verifier.state.enabled=true --strict
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml template --selector name=sentio-node-devnet2-indexer-b --skip-deps | python3 -I -c '
import sys, yaml
for d in yaml.safe_load_all(sys.stdin):
    if d and d["kind"] == "ConfigMap" and "standalone.yaml" in d.get("data", {}):
        h = yaml.safe_load(d["data"]["standalone.yaml"])
        s = h["storage_integrity"]["snode"]
        print(h["housegate"]["upstream"], h["storage_integrity"]["clickhouse_dsn"], s["node_id"], s["table_ids"], s["schema_root"], s["genesis_snapshot_id"], h["node"]["storage_node_rpc_port"])
    if d and d["kind"] == "StatefulSet":
        print([c["image"].split("/")[-1] for c in d["spec"]["template"]["spec"]["containers"]])'
base="$(mktemp -d)"; git -C "$W" archive origin/main charts k8s-sea/sentio-network-devnet2 | tar -x -C "$base"
render() { (cd "$1" && helmfile -f "k8s-sea/sentio-network-devnet2/$2" template --selector "name=$3" --skip-deps 2>/dev/null); }
for pair in storage-integrity-si-v2.helmfile.yaml:storage-integrity-si-v2 helmfile.yaml:sentio-node-devnet2-indexer-a helmfile.yaml:sentio-node-devnet2-indexer-b helmfile.yaml:sentio-node-devnet2-observer storage-integrity.helmfile.yaml:sentio-node-devnet2-indexer-a; do
  render "$base" "${pair%%:*}" "${pair#*:}" > "$base/old.yaml"; render "$W" "${pair%%:*}" "${pair#*:}" > "$base/new.yaml"
  if [ -s "$base/new.yaml" ] && cmp -s "$base/old.yaml" "$base/new.yaml"; then echo "$pair renders unchanged"; else echo "$pair DIFFERS"; fi
done
diff <(render "$base" storage-integrity-si-v2.helmfile.yaml sentio-node-devnet2-indexer-a) <(render "$W" storage-integrity-si-v2.helmfile.yaml sentio-node-devnet2-indexer-a) || true
cmp <(cd "$base" && helm template storage-integrity charts/storage-integrity -n sentio-network-devnet2 -f k8s-sea/sentio-network-devnet2/storage-integrity.yaml) \
    <(helm template storage-integrity charts/storage-integrity -n sentio-network-devnet2 -f k8s-sea/sentio-network-devnet2/storage-integrity.yaml) && echo "legacy release renders unchanged"
rm -rf "$base"
```

Expected (measured on a copy of `origin/main` with stand-in pins): `Ran 81 tests` and `OK`; the four lints print `1 chart(s) linted, 0 chart(s) failed`; the indexer-b render prints only `Templating release=sentio-node-devnet2-indexer-b`, then `storage-integrity-si-v2-indexer-b-source:9000 storage-integrity-si-v2-indexer-b-source:9000 snode-2 [] 0x3aaa1430…7e59 0x63700cf1…1c06 33003` and the images `housegate-rewriter:0.17.0@sha256:b3b227fa…`, `housegate:v0.15.0@sha256:1b51de76…`, `sentio-node:sha-<SN_COMMIT>@<SN_DIGEST>`, then `redis:7.4-alpine`; five `… renders unchanged` lines; indexer-a's SI release differs by exactly two lines, the new `genesis_snapshot_id` in `standalone.yaml` and the StatefulSet's `checksum/config`:

```text
215a216
>         genesis_snapshot_id: "0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06"
553c554
<         checksum/config: <origin/main checksum>
---
>         checksum/config: <new checksum>
```

and `legacy release renders unchanged`.

```bash
cd "$W" && git add .github k8s-sea/sentio-network-devnet2 charts/storage-integrity/tests && git commit -m "feat(devnet2): indexer-b SI overlay, source ClickHouse and source pin for multi-source stage 1

indexer-b joins devnet2-si-v2 as the second SI indexer with SNode snode-2,
its own source ClickHouse storage-integrity-si-v2-indexer-b (the network
installation's source profile: Keeper root, interserver, passwordless
default user, server build), no genesis tables, the empty set's schema root
and the network's genesis snapshot id. indexer-a's overlay names the same
genesis snapshot id, which its enrolment statement needs and its stage-1
image checks at startup; sentio-node ff336f1 ignores the key. Inert until
the runbook applies the installation and syncs the new release by selector.
Render tests: one network release with one genesis snapshot id, unique SNode
ids, owner-specific genesis sets, no shared source ClickHouse, the
installation equal to the network's profile, the immutable genesis writer
cap and the devnet2 verifier state switch.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: Runbook**

Create `docs/storage-integrity-devnet2-multi-source.md` with the content below, then fill the ledger values and check:

```bash
cd "$W" && sed -i '' -e "s/<ARBITER_TAG>/$ARBITER_TAG/g" -e "s/<ARBITER_DIGEST>/$ARBITER_DIGEST/g" -e "s/<PROTO_TAG>/$PROTO_TAG/g" -e "s/<CORE_TAG>/$CORE_TAG/g" \
  -e "s/<HG_TAG>/$HG_TAG/g" -e "s/<SN_COMMIT>/$SN_COMMIT/g" -e "s/<SN_DIGEST>/$SN_DIGEST/g" \
  docs/storage-integrity-devnet2-multi-source.md docs/storage-integrity-devnet2-dynamic-table-set.md
! git grep -nE '<(ARBITER_TAG|ARBITER_DIGEST|PROTO_TAG|CORE_TAG|HG_TAG|SN_COMMIT|SN_DIGEST)>' -- docs
python3 -I - docs/storage-integrity-devnet2-multi-source.md <<'EOF'
import re, sys
text = open(sys.argv[1]).read()
section = text.split("## 0. Session helpers", 1)[1].split("\n## ", 1)[0]
assert re.findall(r"```bash\n(.*?)```", section, re.S), "no helper block"
print("helpers block found")
EOF
```

Expected: no placeholder left; `helpers block found` (Task 17 extracts it the same way).

````markdown
# Storage integrity devnet2: multi-source stage 1

This runbook moves devnet2-si-v2 (releases `storage-integrity-si-v2`, `sentio-node-devnet2-indexer-a` and `sentio-node-devnet2-indexer-b` in `sentio-network-devnet2`) onto the stage-1 builds of the multi-source storage-integrity design, turns signed claims on with indexer-a as the founding SI indexer, and enrols indexer-b (on-chain indexer 1) as the second SI indexer. Design: housegate `docs/superpowers/specs/2026-10-10-multi-source-si-network-design.md`, stage 1 (§6, §8 to §13). It continues [storage-integrity-devnet2-dynamic-table-set.md](storage-integrity-devnet2-dynamic-table-set.md), whose stage 5 (client lanes) is done.

| Stage | Change | Way back |
| --- | --- | --- |
| 0 | PR: indexer-b's SI overlay, source pin and source ClickHouse manifest, owner-specific SNode genesis sets and `genesisSnapshotID` in the sentio-node chart (named on both SI overlays), `verifier.state`, render tests, this runbook; inert until synced or applied | Revert the PR |
| 1 | Offline replay check; verifiers (with their new state volumes), then voters one at a time to arbiter `<ARBITER_TAG>` (§2, §3) | Before stage 3: revert the commit, sync, replace the voters back (§10.1) |
| 2 | indexer-a: sentio-node `<SN_COMMIT>` (HouseGate `<HG_TAG>`), which checks the configured genesis snapshot id and advertises `signed_claims_v1` (§4) | Before stage 3: revert the commit and sync (§10.1) |
| 3 | Signed-claims activation: `si_indexers` = indexer 0 with `snode-1`, `verifiers` = the three running verifiers; these lists are authoritative, so §5.1 first proves that no other node is registered (§5) | None: irreversible (§10.2) |
| 4 | indexer-b: its own source ClickHouse, its ordinary objects, the stage-1 images and its SI overlay; its SNode registration is deferred and every table answers ordinary (§6) | Until stage 5: sync indexer-b from `helmfile.yaml` (§10.3) |
| 5 | Enrolment: indexer 1 with `snode-2`, activation block about 1800 L2 blocks past the safe head; `max_writers` 2 (§7) | None: entries are never removed (§10.4) |
| 6 | indexer-b's activation block: the watcher seeds its Legacy tables (§8) | None needed |
| 7 | End to end (§9) | None needed |

Before stage 3 nothing changes for any table: the leader drops every new field from the commands it proposes, and the voters keep writing v18 snapshots. From stage 5 on, until stage 2 of the design (tracks) is live, a stuck indexer-b stalls the safe progress of every indexer, indexer-a included (design D12).

Every command block is bash. Run them from the root of a production checkout at `origin/main`, in order, in one dedicated `bash` session started with `set -eo pipefail` (from fish, run `bash` first). Every value a later block needs is written under `$si_backup`, so a block also runs in a new session once §0 is defined. Never run two stages in one window. From §3.2 until §5.4 has passed, freeze every other change to `storage-integrity-si-v2`: no other sync of it and no pod deletion outside this runbook.

## 0. Session helpers

Define the helpers of §0 of [storage-integrity-devnet2-dynamic-table-set.md](storage-integrity-devnet2-dynamic-table-set.md) unchanged, then these:

```bash
si_backup="$HOME/si-devnet2-multisource"
R="$HOME/si-devnet2-multisource-rehearsal"
si_indexer_b() { si_kubectl exec sentio-node-devnet2-indexer-b-0 -c sentio-node -- "$@"; }
si_rpc_b() { si_indexer_b wget -qO- --header 'Content-Type: application/json' --post-data "$1" http://localhost:33003; }
si_rpc_on() { # $1 a|b, $2 JSON-RPC body: that indexer's storage RPC
  if [ "$1" = b ]; then si_rpc_b "$2"; else si_rpc "$2"; fi
}
si_info() { si_rpc_on "$1" '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityInfo","params":[]}' | jq -c '.result | del(.server_unix_time)'; }
si_status() { # $1 a|b, $2 database, $3 table: that indexer's answer for the table
  si_rpc_on "$1" "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sentio_getStorageIntegrityTableStatus\",\"params\":[\"$2\",\"$3\"]}" | jq -c '.result | {status, schema_hash, registry_version}'
}
si_source_ch() { # $1 a|b, $2 query: that indexer's source ClickHouse, as its passwordless default user
  local pod=storage-integrity-si-v2-source-0-0-0
  if [ "$1" = b ]; then pod=storage-integrity-si-v2-indexer-b-source-0-0-0; fi
  si_kubectl exec "$pod" -c clickhouse -- clickhouse-client --query "$2"
}
si_verifier_ch() { si_kubectl exec "storage-integrity-si-v2-verifier-$1-0-0-0" -c clickhouse -- clickhouse-client --query "$2"; }
si_safe() { si_metrics 0 | sed -n 's/^arbiter_safe_watermark_block_seq //p'; }
si_safe_head() { # the L2 safe head, decimal, from archive node op-reth-0 through voter 0
  printf '%d\n' "$(si_arbiter 0 wget -qO- --header 'Content-Type: application/json' --post-data '{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByNumber","params":["safe",false]}' http://sentio-node-op-reth-0:8545 | jq -r .result.number)"
}
si_features() { # the current leader's feature book: voter ids, and whether each data-plane node advertised signed_claims_v1
  local l
  l="$(si_leader)"
  si_arbiter "$l" /usr/local/bin/arbiter-admin consensus features --address "$(si_voter "$l")" |
    jq -c '{voters: .raft_voter_ids, nodes: [.entries[] | {id: .node_id, claims: ((.features // []) | index("signed_claims_v1") != null)}]}'
}
si_put_voters() { # $1 local file, $2 absolute path to write in every voter's arbiter container
  local i
  for i in 0 1 2; do si_kubectl exec -i "storage-integrity-si-v2-arbiter-$i" -c arbiter -- sh -c "cat > $2" < "$1"; done
}
si_replace_voter() { # $1 ordinal: delete one voter pod (OnDelete), wait until it is Ready and the sealed tips agree
  local tips t j
  si_kubectl delete pod "storage-integrity-si-v2-arbiter-$1" --wait
  until si_kubectl get pod "storage-integrity-si-v2-arbiter-$1" >/dev/null 2>&1; do sleep 2; done
  si_kubectl wait --for=condition=Ready "pod/storage-integrity-si-v2-arbiter-$1" --timeout=15m
  si_capability "$1"
  for t in $(seq 1 60); do
    tips="$(for j in 0 1 2; do si_metrics "$j" | sed -n 's/^arbiter_sealed_block_seq //p'; done | sort -u | wc -l)"
    [ "$tips" -eq 1 ] && break
    sleep 5
  done
  for j in 0 1 2; do si_metrics "$j" | grep sealed_block_seq; done
  [ "$tips" -eq 1 ] || { echo "sealed tips still differ after 5 minutes; stop"; return 1; }
}
si_accept_a() { # $1 value: one signed Native INSERT into devnet101.swap_new2 through indexer-a's sidecar; waits for safe and reads it back
  local base n
  base="$(si_safe)"
  si_kubectl delete pod si-ms-acceptance --ignore-not-found
  si_kubectl run si-ms-acceptance -i --restart=Never \
    --image=us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:25.8-lts-decimal512-2026-03-27@sha256:89839a322a59489fa14ca12e9d00f53d860b08c5486808f652a9484d7c247797 \
    --overrides='{"apiVersion":"v1","spec":{"imagePullSecrets":[{"name":"google-image-secret"}]}}' \
    --command -- sh -ec "
      clickhouse-local --query 'SELECT toInt64($1) AS value FORMAT Native' > /tmp/row.native
      clickhouse-client --compression=0 --host sentio-node-devnet2-indexer-a --port 9011 \
        --query 'INSERT INTO devnet101.swap_new2 (value) FORMAT Native' < /tmp/row.native"
  si_kubectl delete pod si-ms-acceptance
  for n in $(seq 1 48); do [ "$(si_safe)" -gt "$base" ] && break; sleep 10; done
  for n in 0 1 2; do si_metrics "$n"; done
  si_ch "SELECT count() FROM devnet101.swap_new2 WHERE value = $1 SETTINGS SQL_x_read_mode='safe'"
}
si_jws() { # $1 file: header and payload of a compact JWS
  python3 -I -c 'import base64, json, sys
token = open(sys.argv[1]).read().strip()
parts = token.split(".")
assert len(parts) == 3, "not a compact JWS"
dec = lambda s: json.loads(base64.urlsafe_b64decode(s + "=" * (-len(s) % 4)))
print(json.dumps({"header": dec(parts[0]), "payload": dec(parts[1])}))' "$1"
}
si_node_image() { # $1 a|b: the image of that indexer's sentio-node container
  si_kubectl get pod "sentio-node-devnet2-indexer-$1-0" -o jsonpath='{.spec.containers[?(@.name=="sentio-node")].image}'
}
si_stage1_image() { # the stage-1 sentio-node image recorded in §1.1
  printf 'ghcr.io/sentioxyz/sentio-node:sha-%s@%s\n' "$(sed -n 3p "$si_backup/pins")" "$(sed -n 4p "$si_backup/pins")"
}
```

## 1. Preparation

### 1.1 Pins

| Component | Pin | Evidence |
| --- | --- | --- |
| Arbiter voters and verifiers | `ghcr.io/sentioxyz/arbiter:<ARBITER_TAG>@<ARBITER_DIGEST>` (arbiter-core `<CORE_TAG>`, arbiter-proto `<PROTO_TAG>`) | arbiter release workflow; `docker buildx imagetools inspect`: OCI revision equals the tag's commit |
| sentio-node, indexer-a and indexer-b | `ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>@<SN_DIGEST>` (housegate `<HG_TAG>`, arbiter-core `<CORE_TAG>`) | main CI `docker-push`; OCI revision equals `<SN_COMMIT>`; index digest from `docker buildx imagetools inspect` |
| In-pod rewriter, indexer-b | `us-west1-docker.pkg.dev/sentio-352722/sentio/housegate-rewriter:0.17.0@sha256:b3b227fa4a28bd9dcd5d3d6aa172890ff66d7bf60e1c7039428efe4dabebb50a` | indexer-a's pin (dynamic-table-set runbook §1.1) |
| Agent sidecar, indexer-b | `ghcr.io/housegate/housegate:v0.15.0@sha256:1b51de76b7b7a15a8006d0ac06febb10012c74c92d4950d07e4e5a5b77b3570f` | indexer-a's pin |
| indexer-b source ClickHouse | `us-west1-docker.pkg.dev/sentio-352722/sentio/clickhouse-server:26.3-lts-decimal512-release-2026-09-16` | the network installation's server build (`charts/storage-integrity/values.yaml`); `test_render.py` keeps them equal |

Before this upgrade the voters and verifiers run arbiter `v0.10.0` (`sha256:54dda9db84d14d60b81900e4a61af06fac31f47460ae80ccd2c7af0d7aad5b29`) and indexer-a runs sentio-node `ff336f1` (`sha256:29ca9da403559f68fba64900b7739ecf92f2818b55e38b5e130a3e07a25673a8`); those are the rollback targets of stages 1 and 2. indexer-b runs sentio-node `36b5c0a` (`sha256:56cf2d16d031bf9a9e4996f35c96bec7978c329e3a63dbfe18e72d879f4f195e`), rewriter 0.14.0 and sidecar v0.12.0 on `clickhouse-devnet2-node-b` until §6.4. HouseGate `<HG_TAG>` raises no rewriter engine floor above rewriter-grpc v0.17.0 (check its release notes; otherwise the in-pod rewriter moves together with the sentio-node pin, as in stage 2c of the dynamic-table-set runbook).

Every SI host must run the stage-1 sentio-node image, which embeds HouseGate `<HG_TAG>`, before the activation of §5.3 commits: an older embedded HouseGate maps the arbiter's `SOURCE_UNAVAILABLE` (code 10) to an unknown outcome instead of the retryable, session-preserving 733. §5.3 checks indexer-a, and indexer-b becomes an SI host only in §6.4, on the same image. Both agent sidecars stay on housegate v0.15.0: on a `SOURCE_UNAVAILABLE` refusal they close the client session, and the refusal stays retryable; a sidecar built on `<HG_TAG>` would keep the session, which is not part of this rollout.

```bash
mkdir -p -m 700 "$si_backup"
ARBITER_TAG='<ARBITER_TAG>'; ARBITER_DIGEST='<ARBITER_DIGEST>'; SN_COMMIT='<SN_COMMIT>'; SN_DIGEST='<SN_DIGEST>'
case "$ARBITER_TAG$ARBITER_DIGEST$SN_COMMIT$SN_DIGEST" in *'<'*) echo "pins are still placeholders; stop"; exit 1;; esac
[ "$(docker buildx imagetools inspect "ghcr.io/sentioxyz/arbiter:$ARBITER_TAG" | sed -n 's/^Digest: *//p')" = "$ARBITER_DIGEST" ]
[ "$(docker buildx imagetools inspect "ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT" | sed -n 's/^Digest: *//p')" = "$SN_DIGEST" ]
grep -qxF "    tag: \"sha-$SN_COMMIT@$SN_DIGEST\"" k8s-sea/sentio-network-devnet2/sentio-node-indexer-b-storage-integrity-si-v2-source-image.yaml
printf '%s\n' "$ARBITER_TAG" "$ARBITER_DIGEST" "$SN_COMMIT" "$SN_DIGEST" > "$si_backup/pins"
echo "pins resolve"
```

### 1.2 Identities

| Identity | Value |
| --- | --- |
| Network | `devnet2`, schema snapshot `devnet2-si-v2`, genesis snapshot id `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06`, schema root `0x4604a800560a07d08516b13b7084e8ee00436055fdc8698912f8b60937eb2732` |
| Founding SI indexer (entry 0) | indexer 0 = indexer-a, signer `0x22a67f498e669cbde18a0eecfff74a72c48331f4`, activation block `5508931` (= `table_registry.activation_block`), SNode `snode-1` with every genesis table (genesis snapshot id configured since stage 0 and checked against its genesis table from §4 on), source `storage-integrity-si-v2-source:9000` |
| Second SI indexer (entry 1) | indexer 1 = indexer-b, signer `0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3`, SNode `snode-2` with no genesis table (schema root `0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59`, the empty set's; genesis snapshot id configured), source `storage-integrity-si-v2-indexer-b-source:9000`, activation block chosen in §7 |
| Verifiers | `verifier-1`, `verifier-2`, `verifier-3` (pod ordinal + 1); keys derived from `storage-integrity-devnet2-si-v2-verifier-seeds`; state in `/state` (PVC `state-storage-integrity-si-v2-verifier-<ordinal>`) from §3.1 |
| Voters | `storage-integrity-si-v2-arbiter-0`, `-1`, `-2` (`node_id` is the pod hostname) |
| Authority | `0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705`; key in `storage-integrity-devnet2-si-v2-authority`, present in every voter as `ARBITER_AUTHORITY_PRIVATE_KEY_HEX` |

SNode node ids are allocated once, from this ledger, and never reused: the arbiter refuses an enrolment whose id ever had a node record, and the id is the Keeper replica name of every table its indexer owns. A counter cannot hand out an id twice, so ids are numbered rather than named after an indexer.

| SNode | SI indexer | Since |
| --- | --- | --- |
| `snode-1` | 0 (indexer-a) | devnet2-si-v2 genesis |
| `snode-2` | 1 (indexer-b) | §7 of this runbook |

Next free id: `snode-3`.

```bash
for id in 0 1; do si_rpc "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"sentio_getIndexerInfoById\",\"params\":[$id]}" | jq -c '.result | {indexerId, signer: (.signer | ascii_downcase), clickhouseProxyPort, storageNodeRpcPort}'; done
si_rpc '{"jsonrpc":"2.0","id":1,"method":"sentio_getDatabaseInfoById","params":["devuser1"]}' | jq -c '.result | {databaseId, indexerId, dbType}'
si_kubectl get statefulset storage-integrity-si-v2-verifier -o jsonpath='{.spec.template.spec.initContainers[0].args[0]}' | grep -F 'replica_id="verifier-${replica_num}"'
```

Expected: `{"indexerId":0,"signer":"0x22a67f498e669cbde18a0eecfff74a72c48331f4","clickhouseProxyPort":32001,"storageNodeRpcPort":32003}`, `{"indexerId":1,"signer":"0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3","clickhouseProxyPort":33001,"storageNodeRpcPort":33003}`, `{"databaseId":"devuser1","indexerId":1,"dbType":0}` and the verifier naming line. A different signer or owner: stop.

### 1.3 Preconditions and baseline (read-only)

- Stage 5 of the dynamic-table-set runbook is done: `si_show | jq -c .current.client_lanes` prints `{"max_lanes_per_account":256}`.
- The stage 0 PR is merged and the checkout is at `origin/main`.
- `$R/bin/{old,new}/{statedigest,v19check}` exist (arbiter plan, release task, Step 7): `old` built from the voters' live release, `v0.10.0`, and `new` from `<ARBITER_TAG>`; if the voters' live image differs from `v0.10.0`, rebuild `old` from that tag first.
- No SI writer runs during §2, §5 and §7 except this runbook's acceptance INSERTs.

```bash
si_show | tee "$si_backup/show-before.json" | jq -c '{genesis_snapshot_id, epoch, params_digest, promotion_seq, max_writers: .current.max_writers, table_registry: .current.table_registry, client_lanes: .current.client_lanes, si_indexers: .current.si_indexers, verifiers: .current.verifiers}'
g="$(jq -r .genesis_snapshot_id "$si_backup/show-before.json")"
for x in a b; do grep -qxF "    genesisSnapshotID: \"$g\"" "k8s-sea/sentio-network-devnet2/sentio-node-indexer-$x-storage-integrity-si-v2.yaml" && echo "indexer-$x overlay names $g" || { echo "indexer-$x overlay does not name $g; stop"; exit 1; }; done
for i in 0 1 2; do si_metrics "$i"; si_capability "$i"; done | tee "$si_backup/voters-before.txt"
si_images | tee "$si_backup/images-before.txt"
si_info a | tee "$si_backup/info-a-before.json"
si_kubectl get pod sentio-node-devnet2-indexer-b-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}' | tee "$si_backup/indexer-b-images-before.txt"
```

Expected: genesis snapshot id `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06`, named by both SI overlays; epoch `"2"`, `max_writers` `"1"`, `table_registry` `{"chain_id":"7892301","databases_contract":"0x5557d2cc6ec8138407a69019d05399ebda7b3614","si_indexer_id":"0","activation_block":"5508931","confirmation":"safe"}`, `client_lanes` `{"max_lanes_per_account":256}`, `si_indexers` and `verifiers` `null` (the v0.10.0 admin does not know them); every voter with the same sealed tip, safe equal to sealed, one active SNode and three active verifiers, and `features` `["client_lanes_v1"]`; all six arbiter pods on `sha256:54dda9db…`.

## 2. Offline replay check

The throwaway `statedigest` tool of §2.6 and §7.2 of the dynamic-table-set runbook restores a copy of each voter's `raft` directory into one arbiter build and hashes the state after applying every logged command. `old` is the voters' live build, `new` is `<ARBITER_TAG>`. The copy is taken while the voter runs, so a `raft.db` that fails to open is a torn copy: rerun that voter's iteration.

```bash
for v in old new; do [ -x "$R/bin/$v/statedigest" ] || { echo "missing $R/bin/$v/statedigest"; exit 1; }; done
si_images
si_show | tee "$si_backup/show-replay.json" | jq -c '{epoch, params_digest, promotion_seq}'
for i in 0 1 2; do
  si_kubectl exec "storage-integrity-si-v2-arbiter-$i" -c arbiter -- tar -C /data -czf - raft > "$si_backup/arbiter-$i-raft.tgz"
  si_kubectl exec "storage-integrity-si-v2-arbiter-$i" -c arbiter -- cat /config/arbiter.yaml > "$si_backup/arbiter-$i.yaml"
  work="$(mktemp -d)"
  tar -C "$work" -xzf "$si_backup/arbiter-$i-raft.tgz"
  for v in old new; do
    out="$si_backup/digest-$i-$v"
    "$R/bin/$v/statedigest" -data-dir "$work/raft" -config "$si_backup/arbiter-$i.yaml" > "$out.txt" 2> "$out.err" || { echo "statedigest $v failed for arbiter-$i (a raft.db that fails to open is a torn copy; re-copy):"; cat "$out.err"; exit 1; }
    grep -E '^(snapshot_index|last_index|applied_commands|rejected_commands|written_version|genesis_configured|live_check|apply_error|apply_rejected|key|spent_ids|snapshot_sha256)' "$out.txt" > "$out.cmp"
    if grep -q '^apply_error ' "$out.txt"; then echo "arbiter-$i $v: apply_error"; exit 1; fi
    if grep -q '^key registration_seqs ' "$out.txt"; then echo "arbiter-$i $v: registration_seqs before the activation"; exit 1; fi
  done
  if cmp -s "$si_backup/digest-$i-old.cmp" "$si_backup/digest-$i-new.cmp"; then
    echo "arbiter-$i: old and new replay identically"
  else
    echo "arbiter-$i: DIFFERENT; stop"; diff "$si_backup/digest-$i-old.cmp" "$si_backup/digest-$i-new.cmp"; exit 1
  fi
  grep -E '^(applied_commands|written_version|live_check)' "$si_backup/digest-$i-new.txt"
  rm -rf "$work"
done
```

Expected: three `old and new replay identically` lines; for each voter `applied_commands` above zero, `written_version 18` (v18 since the client-lanes activation; the new build writes v19 only once `si_indexers` is committed), no `key registration_seqs` line, no `apply_error`, and `live_check network_id=devnet2 genesis_snapshot_id=0x63700cf1… epoch=2 params_digest=<show-replay.json> promotion_seq=<show-replay.json>`. Any difference: stop before §3.

## 3. Verifiers and voters

### 3.1 Verifiers

One commit in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` and `charts/storage-integrity/tests/test_render.py`: `arbiter.image.digest` set to `<ARBITER_DIGEST>` (the version comment names `<ARBITER_TAG>` and the previous pin `v0.10.0 sha256:54dda9db…`) with its mirror `DEVNET2_ARBITER_DIGEST`, and `verifier.state.enabled: true` with its mirror `DEVNET2_VERIFIER_STATE_ENABLED = True`: every verifier gets a 1Gi `local-path` volume at `/state` for `registration.json`. The voters share the digest but are `OnDelete`, so the sync rolls only the verifiers. A StatefulSet's volume claim templates are immutable, so the verifier StatefulSet is deleted with `--cascade=orphan` right before the sync: its pods keep running, the sync recreates the StatefulSet, which adopts them and replaces them one at a time.

```bash
python3 -B charts/storage-integrity/tests/test_render.py
[ "$(git diff --name-only HEAD~1 HEAD | sort | tr '\n' ' ')" = "charts/storage-integrity/tests/test_render.py k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml " ] || { echo "HEAD is not the verifier commit"; exit 1; }
git rev-parse HEAD > "$si_backup/stage1-verifiers-commit"
git push origin HEAD:main
si_kubectl delete statefulset storage-integrity-si-v2-verifier --cascade=orphan
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
si_kubectl get pvc -o name | grep -E '/state-storage-integrity-si-v2-verifier-[0-2]$'
si_images
for i in 0 1 2; do si_metrics "$i"; done
for n in 0 1 2; do si_kubectl exec "storage-integrity-si-v2-verifier-$n" -c verifier -- cat /state/registration.json; echo; done
```

Expected: the three verifiers run `<ARBITER_DIGEST>` (`RollingUpdate`), the voters keep `sha256:54dda9db…`; three `state-storage-integrity-si-v2-verifier-<n>` claims; every voter reports `arbiter_nodes{role="verifier",status="active"} 3` again within a few minutes, and the safe watermark keeps following the sealed tip; each `registration.json` holds `{"registration_seq":<13-digit millisecond time>}`: the verifiers already sign their registrations, which the v0.10.0 leader receives with the unknown fields ignored. Do not delete a voter in this step.

### 3.2 Voters

Followers first, the leader last, one pod at a time, so two voters always form a quorum; the sealed tip must agree on all three before the next deletion.

```bash
leader="$(si_leader)"
for i in $(printf '0\n1\n2\n' | grep -vx "$leader") "$leader"; do si_replace_voter "$i"; done
si_images
for i in 0 1 2; do si_capability "$i"; done
```

Expected: every capability line names its own `node_id` with `protocol_version` `1`, `updates_enabled` `true` and `features` `["client_lanes_v1","signed_claims_v1"]`, and `si_images` shows the three voters on `<ARBITER_DIGEST>`. The replaced voters still write v18 snapshots and the leader drops every new field, so until §5.3 the previous image can take a voter back the same way (§10.1).

## 4. indexer-a

One commit: sentio-node `sha-<SN_COMMIT>@<SN_DIGEST>` in `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, its mirrors `docs/examples/storage-integrity-fresh-source-image.yaml` and `FRESH_SOURCE_IMAGE` in `test_render.py`, with the header comments naming `<SN_COMMIT>` (housegate `<HG_TAG>`, arbiter-core `<CORE_TAG>`) and the stage-2 rollback target `sha-ff336f146993ebe76a39184bc0f820fa5de036d6@sha256:29ca9da403559f68fba64900b7739ecf92f2818b55e38b5e130a3e07a25673a8`. `WRITER_AUTHORIZATION_IMAGES` already lists the image (stage 0, for indexer-b). The sidecar and the in-pod rewriter do not change. The overlay already names the network's genesis snapshot id (stage 0); the new image checks it against indexer-a's genesis table at startup, and the enrolment statement of §5.2 binds it.

```bash
python3 -B charts/storage-integrity/tests/test_render.py
[ "$(git diff --name-only HEAD~1 HEAD | sort | tr '\n' ' ')" = "charts/storage-integrity/tests/test_render.py docs/examples/storage-integrity-fresh-source-image.yaml k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml " ] || { echo "HEAD is not the indexer-a image commit"; exit 1; }
git rev-parse HEAD > "$si_backup/stage1-indexer-a-commit"
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
si_kubectl get pod sentio-node-devnet2-indexer-a-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}'
[ "$(si_node_image a)" = "$(si_stage1_image)" ] && echo "indexer-a runs the stage-1 image"
si_info a
si_indexer cat /data/storage-integrity-devnet2-si-v2/snode/state.json | jq -c '{registration_seq}'
si_features
si_accept_a 801
```

Expected: `sentio-node` runs `…:sha-<SN_COMMIT>@<SN_DIGEST>` (`indexer-a runs the stage-1 image`), the other two containers are unchanged; the pod became Ready, so the configured genesis snapshot id matched the one derived from its genesis table (a mismatch stops startup with `storage_integrity.snode.genesis_snapshot_id <configured> is not the network's genesis snapshot id <derived> derived from this node's genesis tables`: revert as in §10.1); the info answer carries `"si_indexer_id":0,"self_indexer_id":0,"enrolled":true` and `registry_version` equal to §1.3's; `registration_seq` is a millisecond timestamp (13 digits), persisted before the registration although the leader still drops it; `si_features` lists `snode-1` with `claims: true` — an SNode advertises `signed_claims_v1` only when sentio-node handed it the indexer key as its claim signer, so this proves the signer is wired; the safe read returns `1`.

## 5. Signed-claims activation

### 5.1 Re-register the data plane and drain

A new leader starts with an empty feature book, the gate accepts only features advertised to it in its current term, and §3.2 replaced the leader last. Restart the verifiers so that they re-register with the current leader; indexer-a did in §4. If `snode-1` is missing below, a new term has cleared the book since §4: restart indexer-a as well (`si_kubectl rollout restart statefulset/sentio-node-devnet2-indexer-a`, then its `rollout status`) and repeat the check.

```bash
si_kubectl rollout restart statefulset/storage-integrity-si-v2-verifier
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
si_features | tee "$si_backup/features-before-activation.json"
si_show | jq -c '{epoch, promotion_seq}'
for i in 0 1 2; do si_metrics "$i"; done
```

Expected: the three voter ids and exactly the nodes `snode-1`, `verifier-1`, `verifier-2`, `verifier-3`, each `claims: true` (`snode-1` `false` means indexer-a runs without a claim signer: stop). Any other entry must be explained before §5.3: the activation's `si_indexers` and `verifiers` are authoritative, so its own Apply would evict every registered SNode or verifier they do not name, and `arbiter-admin` then refuses to submit unless `--evict-unnamed` is given, which this runbook never passes; a listed verifier registered under another key refuses the update. `GetNodeFeatures` refuses while the Raft configuration holds a non-voter; inspect it first. Drained means every voter reports `arbiter_open_block_statements 0`, `arbiter_unanchored_verified_blocks 0` and a safe watermark equal to the sealed tip; an undrained update is refused atomically with `InvalidArgument` and changes nothing.

Residual risk: a second process configured with a voter's `node_id` defeats the voter-identity check, because the gate asks each node who it is. Confirm that nothing but the three voters runs `arbiter` with their ids.

### 5.2 Activation inputs

indexer-a's enrolment statement, signed inside its pod with the key sentio-node already holds (`PRIVATE_KEY`). The subcommand reads the chain and Redis as the node does, never contacts the arbiter and changes nothing; it refuses to run without the configured genesis snapshot id. stdout carries only the JWS, logs go to stderr:

```bash
si_indexer /sentio-node --config-path /config/standalone.yaml storage-integrity enrollment-statement > "$si_backup/enroll-indexer-0.jws"
si_jws "$si_backup/enroll-indexer-0.jws"
```

Expected: header `{"alg":"ES256K","typ":"JWT"}`, payload with `"purpose":"arbiter-snode-enrollment-v1"`, a `cmd_hash` and the current `iat`. The statement binds `devnet2`, the genesis snapshot id of §1.2, indexer 0 and `snode-1`; `arbiter-admin` verifies it against the signer before submitting, so a wrong statement submits nothing.

The verifiers' registered public keys, derived from their seeds (OpenSSL 3 needed; the first line checks the pipeline against RFC 8032 test 1 and must print `d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a`). The seeds stay in this shell; only public keys reach the file:

```bash
printf '302e020100300506032b657004220420%s' 9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60 | xxd -r -p | openssl pkey -inform DER -pubout -outform DER | tail -c 32 | xxd -p -c 64
for n in 1 2 3; do
  seed="$(si_kubectl get secret storage-integrity-devnet2-si-v2-verifier-seeds -o "jsonpath={.data.verifier-$n}" | base64 -d | tr -d '\r\n')"
  printf 'verifier-%s %s\n' "$n" "$(printf '302e020100300506032b657004220420%s' "$seed" | xxd -r -p | openssl pkey -inform DER -pubout -outform DER | tail -c 32 | xxd -p -c 64)"
  unset seed
done > "$si_backup/verifier-pubkeys.txt"
cat "$si_backup/verifier-pubkeys.txt"
```

Expected: three lines `verifier-N <64 hex>`, all different. A key that is not the registered one is refused by the activation with nothing changed.

### 5.3 Activate

Stop here and obtain explicit confirmation. The update is **irreversible**: once it commits, signed claims can never be turned off, the voters write v19 snapshots that no older build reads, every SNode and verifier message must be signed, and no voter, verifier or SNode may run an older image again. The rollbacks of §10.1 stop being a way back.

```bash
[ "$(si_node_image a)" = "$(si_stage1_image)" ] || { echo "indexer-a must run the stage-1 image (HouseGate <HG_TAG>) before the activation; stop"; exit 1; }
v1="$(awk '$1 == "verifier-1" {print $2}' "$si_backup/verifier-pubkeys.txt")"
v2="$(awk '$1 == "verifier-2" {print $2}' "$si_backup/verifier-pubkeys.txt")"
v3="$(awk '$1 == "verifier-3" {print $2}' "$si_backup/verifier-pubkeys.txt")"
for v in "$v1" "$v2" "$v3"; do [[ "$v" =~ ^[0-9a-f]{64}$ ]] || { echo "verifier key missing or malformed: $v"; exit 1; }; done
si_put_voters "$si_backup/enroll-indexer-0.jws" /tmp/enroll-indexer-0.jws
leader="$(si_leader)"
epoch="$(si_show | jq -r .epoch)"
si_arbiter "$leader" /usr/local/bin/arbiter-admin consensus update --address "$(si_voter "$leader")" \
  --expected-epoch "$epoch" --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 1 \
  --si-indexer "id=0,activation=5508931,signer=0x22a67f498e669cbde18a0eecfff74a72c48331f4,node=snode-1,enrollment=/tmp/enroll-indexer-0.jws" \
  --verifier "node=verifier-1,pubkey=$v1" --verifier "node=verifier-2,pubkey=$v2" --verifier "node=verifier-3,pubkey=$v3" \
  --voter "$(si_voter 0)" --voter "$(si_voter 1)" --voter "$(si_voter 2)" --timeout 20s | tee "$si_backup/activate.out"
```

The table registry and client-lane parameters are carried from the leader. A gate refusal lists the nodes to fix (restart them on the new images) and submits nothing. A refusal saying the activation `would evict` some nodes and asking for `--evict-unnamed` names registrations that §5.1 did not show: never add the flag here, stop and report them. `InvalidArgument` means the FSM refused (undrained work, a raced epoch, a mis-keyed verifier, a named node registered with another role): run `si_show`, fix and retry. On "commit outcome may be unknown", run `si_show` before anything else: epoch 3 with `si_indexers` set means it committed.

### 5.4 Verify

```bash
si_show | tee "$si_backup/show-after-activation.json" | jq -c '{epoch, max_writers: .current.max_writers, si_indexers: [.current.si_indexers[] | {indexer_id, activation_block, signer, snode_node_id}], verifiers: [.current.verifiers[].node_id]}'
jq -r '.current.verifiers[] | "\(.node_id) \(.ed25519_pubkey)"' "$si_backup/show-after-activation.json" |
  while read -r id key; do printf '%s %s\n' "$id" "$(printf '%s' "$key" | base64 -d | xxd -p -c 64)"; done | diff - "$si_backup/verifier-pubkeys.txt" && echo "verifier keys committed as derived"
si_info a
for i in 0 1 2; do si_arbiter "$i" wget -qO- http://127.0.0.1:9090/metrics | grep -E '^arbiter_admission_rejects_total' || true; done
si_accept_a 802
```

Expected: epoch `"3"`, `max_writers` `"1"`, `si_indexers` `[{"indexer_id":"0","activation_block":"5508931","signer":"0x22a67f498e669cbde18a0eecfff74a72c48331f4","snode_node_id":"snode-1"}]`, verifiers `["verifier-1","verifier-2","verifier-3"]`, `verifier keys committed as derived`; `si_info a` still `enrolled: true` with `registry_version` one above §1.3's (the activation bumps it once); the acceptance INSERT reaches safe and reads `1`: its result claim and promotion acknowledgement are now signed with indexer-a's key.

Then prove that both node kinds re-register under signed claims, while the only impact is a retryable `SOURCE_UNAVAILABLE` window for indexer-a's tables:

```bash
si_kubectl rollout restart statefulset/storage-integrity-si-v2-verifier
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
si_kubectl rollout restart statefulset/sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
si_features
for n in 0 1 2; do si_kubectl exec "storage-integrity-si-v2-verifier-$n" -c verifier -- cat /state/registration.json; echo; done
si_accept_a 803
```

Expected: `si_features` lists the four nodes with `claims: true`, every voter reports one active SNode and three active verifiers, each `registration.json` holds a higher `registration_seq` than before the restart, and the acceptance INSERT reaches safe. A node that cannot re-register now is an incident: the network keeps running until that node restarts, so fix it before §6.

After the next Raft snapshot (the first comes only after the log passes `snapshot_threshold`), every voter's newest `state.bin` (the raw snapshot container) carries version byte 19, which the previous release refuses:

```bash
for i in 0 1 2; do si_arbiter "$i" sh -c 'for d in $(ls -dt /data/raft/snapshots/*/); do echo "$d $(head -c 5 "$d/state.bin" | od -An -tu1 | awk "{print \$5}")"; done'; done
si_arbiter 0 sh -c 'cat "$(ls -dt /data/raft/snapshots/*/ | head -1)state.bin"' > "$si_backup/v19-state.bin"
"$R/bin/old/v19check" "$si_backup/v19-state.bin"
"$R/bin/new/v19check" "$si_backup/v19-state.bin"
```

Expected: the first line of each voter (its newest snapshot) ends in `19`; `old` prints `restore_refused version 19: unsupported snapshot version 19 …`, `new` prints `restore_ok version 19`. Until a snapshot exists the loop prints only older versions; repeat it later.

## 6. Prepare indexer-b

indexer-b gets a dedicated source ClickHouse, `storage-integrity-si-v2-indexer-b` (`k8s-sea/sentio-network-devnet2/storage-integrity-si-v2-indexer-b-source.yaml`), rather than the source profile on `clickhouse-devnet2-node-b`:

- The SNode connects to its source as `default` without a password (sentio-node `standalone/standalone.go`, `clickhouse.Options{Addr}`), while `clickhouse-devnet2`'s `default` user has one; users are installation-wide, so the profile on node-b would change node-a too.
- `clickhouse-devnet2` is not Helm-managed and serves node-a, which the observer's ClickHouse configuration still names and which indexer-a's rollback surfaces point at; any edit reconciles both hosts.
- node-b holds only `devuser1.t` (0 rows) and two views (spec, 2026-10-09; re-measured in §6.1), which §6.3 recreates, without rows, on the new source.
- The new installation carries the network installation's source profile (Keeper ensemble and root, interserver Service on 9009, passwordless default user, server build), as indexer-a's bundled source does, and keeps one SNode per source ClickHouse.

### 6.1 Read-only checks

```bash
si_kubectl exec sentio-node-devnet2-indexer-b-redis-0 -c redis -- redis-cli HGET statemirror:v1:DatabasePermissions 0x0000000000000000000000000000000000000000
si_indexer_b wget -qO- http://sentio-node-devnet2-indexer-b-dind:2375/tasks | jq '[.[] | select(.Status.State=="running")] | length'
mkdir -p "$si_backup/node-b"
si_kubectl exec clickhouse-devnet2-node-b-0-0-0 -c clickhouse -- sh -c '
  cd /var/lib/clickhouse/metadata
  for db in *; do
    [ -d "$db" ] || continue
    case "$db" in system|information_schema|INFORMATION_SCHEMA) continue ;; esac
    for f in "$db"/*.sql; do [ -e "$f" ] && echo "$f"; done
  done' | tee "$si_backup/node-b/objects.txt"
while read -r f; do
  mkdir -p "$si_backup/node-b/$(dirname "$f")"
  si_kubectl exec clickhouse-devnet2-node-b-0-0-0 -c clickhouse -- cat "/var/lib/clickhouse/metadata/$f" < /dev/null > "$si_backup/node-b/$f"
done < "$si_backup/node-b/objects.txt"
si_kubectl exec clickhouse-devnet2-node-b-0-0-0 -c clickhouse -- sh -c '
  cd /var/lib/clickhouse/data
  for db in *; do
    case "$db" in system|information_schema|INFORMATION_SCHEMA) continue ;; esac
    for t in "$db"/*; do [ -d "$t" ] && echo "$t parts=$(find "$t/" -mindepth 1 -maxdepth 1 -type d ! -name detached | wc -l)"; done
  done'
si_status a devuser1 t
```

Expected: an empty `HGET` answer (no address(0) grant, so the contract writer predicate of the new image changes no ordinary write on indexer-b; otherwise follow §6.1 of the dynamic-table-set runbook before §6.4); `0` running driver tasks; `devnet2/devuser1%2Et.sql` and the two view files (a view has no data directory); every listed data directory `parts=0`; indexer-a answers `ordinary` for indexer-b's table. Stop on a running task or any part: indexer-b's data would stay behind on node-b.

### 6.2 Source ClickHouse

The Keeper root `/storage-integrity/devnet2/devnet2-si-v2` exists already (the network installation uses it), so nothing is created in Keeper.

```bash
if si_kubectl get chi storage-integrity-si-v2-indexer-b >/dev/null 2>&1; then echo "installation exists already"; exit 1; fi
si_kubectl apply --dry-run=server -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2-indexer-b-source.yaml -o json | jq -c '{root: .spec.configuration.zookeeper.root, clusters: [.spec.configuration.clusters[].name]}'
si_kubectl apply -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2-indexer-b-source.yaml
until si_kubectl get pod storage-integrity-si-v2-indexer-b-source-0-0-0 >/dev/null 2>&1; do sleep 5; done
si_kubectl wait --for=condition=Ready pod/storage-integrity-si-v2-indexer-b-source-0-0-0 --timeout=15m
for n in $(seq 1 60); do [ "$(si_kubectl get chi storage-integrity-si-v2-indexer-b -o jsonpath='{.status.status}')" = Completed ] && break; sleep 10; done
si_kubectl get chi storage-integrity-si-v2-indexer-b
si_kubectl get configmap chi-storage-integrity-si-v2-indexer-b-deploy-confd-source-0-0 -o json | jq -r '.data["chop-generated-zookeeper.xml"] + .data["chop-generated-hostname-ports.xml"]' | grep -E '<root>|<interserver_http_host>'
si_source_ch b "SELECT version()"
si_source_ch b "SELECT name FROM system.zookeeper WHERE path = '/sentio/0/unsafe' ORDER BY name"
for n in 1 2 3; do si_verifier_ch "$n" "SELECT * FROM url('http://storage-integrity-si-v2-indexer-b-source-replica-0:9009/ping', 'LineAsString')"; done
```

Expected: the dry run shows root `/storage-integrity/devnet2/devnet2-si-v2` and clusters `["source"]`; status `Completed`; `<root>/storage-integrity/devnet2/devnet2-si-v2</root>` and `<interserver_http_host>storage-integrity-si-v2-indexer-b-source-replica-0</interserver_http_host>`; a 26.3 version; the network's existing replicated table paths (`devnet101__swap_new2` among them; an empty answer means a wrong root: stop); `Ok.` from every verifier ClickHouse.

### 6.3 Ordinary objects

Recreate node-b's objects, without rows, from the metadata copied in §6.1. Review `create.sql` before applying it.

```bash
python3 -I - "$si_backup/node-b" > "$si_backup/node-b/create.sql" <<'EOF'
import pathlib, re, sys, urllib.parse
root = pathlib.Path(sys.argv[1])
quote = lambda s: "`" + s.replace("`", "``") + "`"
tables, views, databases = [], [], set()
for path in sorted(root.glob("*/*.sql")):
    database, name = path.parent.name, urllib.parse.unquote(path.stem)
    text = path.read_text()
    match = re.match(r"ATTACH (TABLE|VIEW|MATERIALIZED VIEW|DICTIONARY) _(?:\s+UUID '[0-9a-f-]+')?\s*", text)
    if not match:
        sys.exit(f"{path}: not an ATTACH statement; recreate it by hand")
    databases.add(database)
    statement = f"CREATE {match.group(1)} {quote(database)}.{quote(name)}\n{text[match.end():].strip().rstrip(';')};"
    (tables if match.group(1) == "TABLE" else views).append(statement)
for database in sorted(databases):
    print(f"CREATE DATABASE IF NOT EXISTS {quote(database)};")
print("\n".join(tables + views))
EOF
cat "$si_backup/node-b/create.sql"
si_kubectl exec -i storage-integrity-si-v2-indexer-b-source-0-0-0 -c clickhouse -- clickhouse-client --multiquery < "$si_backup/node-b/create.sql"
si_source_ch b "SELECT database, name, engine FROM system.tables WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema') ORDER BY database, name"
```

Expected: `create.sql` holds `CREATE DATABASE IF NOT EXISTS` lines, the `devnet2`.`devuser1.t` table and then the two views; the new source lists exactly the objects of `objects.txt` and nothing in `hg_*` (indexer-b's SNode creates those).

### 6.4 Sync

The release `sentio-node-devnet2-indexer-b` of `storage-integrity-si-v2.helmfile.yaml` (stage 0) moves indexer-b to the stage-1 images and its SI overlay, on the new source. Its SNode has no genesis table: it runs with the empty set's schema root and the configured genesis snapshot id, and defers its registration until the table registry carries its `si_indexers` entry. The overlay must be live before the activation block §7 chooses, so that every table created from that block on is declared under the arbiter's network id; sentio-node refuses to start when its schema declarer would hash under another network id than `storage_integrity.snode.network_id` (both are `devnet2` here).

```bash
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-b
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-b --timeout=15m
si_kubectl get pod sentio-node-devnet2-indexer-b-0 -o jsonpath='{range .spec.containers[*]}{.name}{"\t"}{.image}{"\n"}{end}'
[ "$(si_node_image b)" = "$(si_stage1_image)" ] && echo "indexer-b runs the stage-1 image"
si_info b
si_status b devuser1 t
si_status a devuser1 t
si_kubectl logs sentio-node-devnet2-indexer-b-0 -c sentio-node --since=10m | grep -c "storage-integrity SNode registration deferred: this indexer is not enrolled in the arbiter's si_indexers yet" || true
si_source_ch b 'SELECT count() FROM devnet2.`devuser1.t`'
si_indexer_b wget -qO- http://sentio-node-devnet2-indexer-b-dind:2375/tasks | jq '[.[] | select(.Status.State=="running")] | length'
```

Expected: `housegate-rewriter` `…:0.17.0@sha256:b3b227fa…`, `housegate-sidecar` `…:v0.15.0@sha256:1b51de76…`, `sentio-node` `…:sha-<SN_COMMIT>@<SN_DIGEST>` (`indexer-b runs the stage-1 image`), and the pod is Ready; the info answer has `"si_indexer_id":null,"self_indexer_id":1,"enrolled":false`; both indexers answer `ordinary` for `devuser1.t`; the deferred-registration warning is logged (count at least `1`) while the node serves; `0` rows; `0` running tasks.

### 6.5 Enrolment statement

```bash
si_indexer_b /sentio-node --config-path /config/standalone.yaml storage-integrity enrollment-statement > "$si_backup/enroll-indexer-1.jws"
si_jws "$si_backup/enroll-indexer-1.jws"
```

Expected: as in §5.2, signed by indexer-b's key over `devnet2`, the configured genesis snapshot id, indexer 1 and `snode-2`.

## 7. Enrolment

A new entry's activation block must exceed both the registry's committed L2 cursor and the founding seed block `5508930`, and should lie about one hour (1800 blocks at 2 s) past the L2 safe head; `arbiter-admin` has no L2 endpoint, so the check is here. The cursor follows `confirmation: safe` and never passes the safe head. Every unchanged entry must be restated byte-identically, so entry 0's statement is taken from `consensus show`, not regenerated. Both SI indexers must already run the stage-1 image, and indexer-b its SI overlay (§6.4), before this update picks the activation block; the block's first lines refuse otherwise.

Stop here and obtain explicit confirmation. The update is **irreversible**: entries are never removed, `snode-2` is bound to indexer 1 for good, and from indexer-b's activation block on a stuck indexer-b stalls the safe progress of every indexer until stage 2.

```bash
[ "$(si_node_image a)" = "$(si_stage1_image)" ] && [ "$(si_node_image b)" = "$(si_stage1_image)" ] || { echo "both SI indexers must run the stage-1 image before the enrolment; stop"; exit 1; }
[ "$(si_info b | jq -r '"\(.self_indexer_id) \(.enrolled)"')" = "1 false" ] || { echo "indexer-b must run its SI overlay, not yet enrolled, before the enrolment; stop"; exit 1; }
head="$(si_safe_head)"
activation=$((head + 1800))
if [ "$activation" -le 5508930 ] || [ "$activation" -le "$head" ]; then echo "activation block $activation is not past the safe head and the founding seed"; exit 1; fi
echo "$activation" > "$si_backup/indexer-b-activation-block"
si_show > "$si_backup/show-before-enrolment.json"
jq -r '.current.si_indexers[] | select(.indexer_id == "0") | .enrollment_jws' "$si_backup/show-before-enrolment.json" > "$si_backup/enroll-indexer-0.committed.jws"
[ -s "$si_backup/enroll-indexer-0.committed.jws" ]
si_put_voters "$si_backup/enroll-indexer-0.committed.jws" /tmp/enroll-indexer-0.jws
si_put_voters "$si_backup/enroll-indexer-1.jws" /tmp/enroll-indexer-1.jws
for i in 0 1 2; do si_metrics "$i"; done
leader="$(si_leader)"
epoch="$(jq -r .epoch "$si_backup/show-before-enrolment.json")"
si_arbiter "$leader" /usr/local/bin/arbiter-admin consensus update --address "$(si_voter "$leader")" \
  --expected-epoch "$epoch" --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 2 \
  --si-indexer "id=0,activation=5508931,signer=0x22a67f498e669cbde18a0eecfff74a72c48331f4,node=snode-1,enrollment=/tmp/enroll-indexer-0.jws" \
  --si-indexer "id=1,activation=$activation,signer=0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3,node=snode-2,enrollment=/tmp/enroll-indexer-1.jws" \
  --timeout 20s | tee "$si_backup/enrol.out"
```

`--verifier` is omitted, so the committed verifiers are carried; the CLI prints a reminder of the 1800-block check for the appended entry. The same refusal handling as §5.3 applies.

```bash
si_show | tee "$si_backup/show-after-enrolment.json" | jq -c '{epoch, max_writers: .current.max_writers, si_indexers: [.current.si_indexers[] | {indexer_id, activation_block, snode_node_id}]}'
for t in $(seq 1 60); do [ "$(si_metrics 0 | sed -n 's/^arbiter_nodes{role="snode",status="active"} //p')" = 2 ] && break; sleep 10; done
for i in 0 1 2; do si_metrics "$i"; done
si_kubectl logs sentio-node-devnet2-indexer-b-0 -c sentio-node --since=15m | grep -E 'storage-integrity SNode registered after its enrolment committed|indexer 1 is enrolled with' || true
si_features
si_info b
si_indexer_b cat /data/storage-integrity-devnet2-si-v2/snode/state.json | jq -c '{registration_seq}'
for ref in devuser1/t devuser3b/s1_e2e; do si_status a "${ref%/*}" "${ref#*/}"; done
si_status b devnet101 swap_new2
si_status b devuser1 t
```

Expected: epoch one above §5.4's, `max_writers` `"2"`, entries `0` (`snode-1`) and `1` (`snode-2`, the recorded activation block); within a minute (the node wakes on the registry change, or its 30 s poll) indexer-b logs `storage-integrity SNode registered after its enrolment committed` and every voter reports `arbiter_nodes{role="snode",status="active"} 2`; `snode-2` appears with `claims: true`; indexer-b reports `"si_indexer_id":1,"enrolled":true`; its `registration_seq` is a millisecond timestamp. A log line `indexer 1 is enrolled with snode_node_id …` or `… signer …` means the entry and the overlay or key disagree, and the restarted pod would refuse startup with the same message: stop. The four table answers are `ordinary`: indexer-a never governs indexer 1's databases (`devuser1`, `devuser3b`, whether or not the table exists), indexer-b never governs indexer 0's (`devnet101`), and `devuser1.t` predates indexer-b's activation block.

## 8. Seed at indexer-b's activation block

The watcher seeds indexer-b's Legacy tables at `activation_block - 1` and the registry refuses to move past that block before the seed. A table created on indexer-b before the activation block stays Legacy (ordinary) for good, so §9 waits for the safe head to pass it (about one hour).

```bash
activation="$(cat "$si_backup/indexer-b-activation-block")"
until [ "$(si_safe_head)" -ge "$activation" ]; do echo "$(date -u +%H:%M:%S) safe head $(si_safe_head) of $activation"; sleep 120; done
sleep 120
si_arbiter "$(si_leader)" wget -qO- http://127.0.0.1:9090/metrics | grep -E '^arbiter_table_registry_(watcher_proposals_total|watcher_halts_total|audit_mismatches_total)' || true
si_indexer_b wget -qO- http://localhost:9091/metrics | grep -E '^sentio_node_storage_integrity_registry_version'
si_status b devuser1 t
```

Expected: the leader's `arbiter_table_registry_watcher_proposals_total{command="seed",outcome="applied"}` has counted the seed (it counts on the proposing leader only; after a leader change rely on the registry version and §9), no `watcher_halts_total` (in particular no `owner_changed` or `seed`), no audit mismatches; the registry version one above §7's; `devuser1.t` answers `ordinary` (Legacy).

## 9. End to end

The writes come from the account that owns `devuser1`, through its own agent (housegate `<HG_TAG>` or any release with the 2026-10-09 agent), with its key only in `HOUSEGATE_AGENT_KEY`. The checks between them only read.

```bash
key_file=/path/to/devuser1-owner.key
HOUSEGATE_AGENT_KEY="$(cat "$key_file")" housegate > "$si_backup/e2e-agent.log" 2>&1 &
agent_pid=$!
sleep 5
ch() { clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 "$@"; }
ch --database devuser1 --query 'CREATE TABLE s1_e2e (id UInt64, v String) ENGINE = MergeTree ORDER BY id'
for n in $(seq 1 60); do s="$(si_status b devuser1 s1_e2e | jq -r .status)"; echo "$(date -u +%H:%M:%S) $s"; [ "$s" = active ] && break; sleep 15; done
si_status a devuser1 s1_e2e
si_status b devnet101 swap_new2
base="$(si_safe)"
printf '1,a\n2,b\n' | ch --database devuser1 --query 'INSERT INTO s1_e2e FORMAT CSV'
printf '3,c\n' | ch --query 'INSERT INTO devuser1.s1_e2e FORMAT CSV'
grep -iE 'switch|upstream|indexer' "$si_backup/e2e-agent.log" | tail -n 10 || true
for n in $(seq 1 48); do [ "$(si_safe)" -gt "$base" ] && [ "$(si_source_ch b 'SELECT count() FROM hg_safe.devuser1__s1_e2e')" = 3 ] && break; sleep 10; done
ch --database devuser1 --query "SELECT count() FROM s1_e2e SETTINGS SQL_x_read_mode='safe'"
for n in 1 2 3; do si_verifier_ch "$n" 'SELECT count() FROM hg_unsafe.devuser1__s1_e2e'; done
si_source_ch b "SELECT database, name FROM system.tables WHERE database IN ('hg_safe', 'hg_unsafe', 'hg_promote') ORDER BY database, name"
si_source_ch a "SELECT database, name FROM system.tables WHERE database IN ('hg_safe', 'hg_unsafe', 'hg_promote') AND startsWith(name, 'devuser1__')"
si_accept_a 901
ch --database devuser1 --query 'DROP TABLE s1_e2e'
for n in $(seq 1 90); do s="$(si_status b devuser1 s1_e2e | jq -r .status)"; echo "$(date -u +%H:%M:%S) $s"; [ "$s" = pending ] && break; sleep 20; done
si_source_ch b "SELECT database, name FROM system.tables WHERE name = 'devuser1__s1_e2e'"
for n in 1 2 3; do si_verifier_ch "$n" "SELECT database, name FROM system.tables WHERE name = 'devuser1__s1_e2e'"; done
for i in 0 1 2; do si_arbiter "$i" wget -qO- http://127.0.0.1:9090/metrics | grep -E '^arbiter_admission_rejects_total' || true; done
kill "$agent_pid"
```

Expected:
- the new table answers `pending` on indexer-b until its SNode has it ready, then `active`, while indexer-a answers `ordinary` (owned by another SI indexer), as indexer-b does for indexer-a's `devnet101.swap_new2`. An `ordinary` answer on indexer-b means the table was created before the activation block: drop it and repeat after §8;
- both INSERTs are accepted; the second runs from a session without `--database`, and the agent log names indexer-b as its upstream (selected, or switched from indexer-a when the account also has a database there);
- `hg_safe.devuser1__s1_e2e` on indexer-b's source holds 3 rows, the safe read returns `3`, and every verifier's `hg_unsafe` replica holds 3 rows (fetched from indexer-b's source over interserver);
- indexer-b's source holds `hg_*` tables of `devuser1__s1_e2e` only, and indexer-a's source none of them: each SNode holds and is promoted only its owner's tables, so indexer-a's promotions go only to `snode-1`;
- indexer-a's acceptance INSERT reaches safe and reads `1`;
- after the DROP the table answers `gone`, then `pending` once purged (a purged name is unrecorded, and indexer-b governs `devuser1`), and no `devuser1__s1_e2e` table is left on indexer-b's source or any verifier;
- `arbiter_admission_rejects_total` shows no `ADMISSION_CODE_SOURCE_UNAVAILABLE` beyond the §5.4 restart window.

## 10. Rollback

### 10.1 Before §5.3

Revert in reverse order: the indexer-a commit, the verifier commit, then the voters. Until the activation the voters write v18 snapshots and no command carries a new field, so the previous release reads everything they wrote. Reverting the verifier commit also removes the state volume template, so the verifier StatefulSet is deleted with `--cascade=orphan` again before that sync; the `state-…` claims stay (deleting them is a separate operator decision).

```bash
git fetch origin main
git merge --ff-only origin/main
c="$(cat "$si_backup/stage1-indexer-a-commit")"; git show --stat "$c"
git revert --no-edit "$c"
python3 -B charts/storage-integrity/tests/test_render.py
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
c="$(cat "$si_backup/stage1-verifiers-commit")"; git show --stat "$c"
git revert --no-edit "$c"
python3 -B charts/storage-integrity/tests/test_render.py
git push origin HEAD:main
si_kubectl delete statefulset storage-integrity-si-v2-verifier --cascade=orphan
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
leader="$(si_leader)"
for i in $(printf '0\n1\n2\n' | grep -vx "$leader") "$leader"; do si_replace_voter "$i"; done
si_images
```

The reverted pin is the voters' template again, so each deleted voter returns on `v0.10.0`. The SNode's persisted `registration_seq` and the `genesisSnapshotID` that indexer-a's overlay keeps from stage 0 are both ignored by the previous image.

### 10.2 After §5.3

There is no rollback. The committed update, the v19 snapshots and the signed-message rules cannot be undone; a voter, verifier or SNode without `signed_claims_v1` is refused at registration (`signed claims are active; this binary does not sign its messages`). Fix forward.

### 10.3 indexer-b before §7

Sync indexer-b from its ordinary release: it returns to `clickhouse-devnet2-node-b` with its previous images and no storage integrity. No node record exists for `snode-2`. Ordinary writes made on the new source since §6.4 stay there.

```bash
helmfile -f k8s-sea/sentio-network-devnet2/helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-b
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-b --timeout=15m
```

The new installation can stay unused. Removing it (`si_kubectl delete chi storage-integrity-si-v2-indexer-b`) keeps its volumes (`reclaimPolicy: Retain`); deleting those volumes destroys data and is a separate, explicit operator decision.

### 10.4 After §7

There is no way back: the entry is never removed and `snode-2` never changes. Keep indexer-b's SNode running on its PVC. Before indexer-b's activation block nothing is bound to `snode-2`; afterwards admission answers `SOURCE_UNAVAILABLE` (retryable) for its tables while `snode-2` is not Active, and any statement already sequenced for `snode-2` stalls every indexer's safe progress until indexer-b returns. Never sync indexer-b from `helmfile.yaml` after §7: that release has no SNode.

## 11. Operating rules

- After §5.3, never run a voter, verifier or SNode below `<ARBITER_TAG>` / `<SN_COMMIT>`.
- SNode node ids come from the §1.2 ledger (next: `snode-3`) and are never reused, not even for the same indexer after its state is lost.
- Every SI indexer's overlay names the network's `genesisSnapshotID` (the enrolment statement binds it; an SNode with genesis tables checks it at startup). An SNode without genesis tables runs with `tableIDs: []` and the empty set's schema root `0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59`; the chart refuses an empty `tableIDs` in any other combination. Never list another indexer's genesis table in `tableIDs`.
- Every SI host runs a sentio-node image that embeds HouseGate `<HG_TAG>` or later, and an SI overlay goes live before its indexer's activation block.
- Every later consensus update carries both lists; `arbiter-admin consensus update` carries them when `--si-indexer` and `--verifier` are omitted. When an update must list entries (an append, a signer rotation), take each unchanged entry's `enrollment_jws` from `consensus show`: it must be byte-identical.
- A new entry's activation block must exceed the registry's committed cursor and the founding seed block (`5508930`); use the L2 safe head plus 1800 and check it by hand.
- One SNode per source ClickHouse: never point an SNode at another indexer's source or at a verifier ClickHouse, and keep `clickhouse.address` equal to `storageIntegrity.clickhouseDSN`. The render tests enforce it for the overlays in this repository.
- Verifiers keep `registration.json` on their `state-…` volumes; turning `verifier.state` on or off needs the verifier StatefulSet deleted with `--cascade=orphan` right before the sync.
- Stopping indexer-b's SNode after its tables hold SI writes stalls every indexer's safe progress until it returns (design D12). Evicting a node needs `arbiter-admin node evict --expected-registration-seq` with its committed `registration_seq` (its `state.json` or `registration.json`; 0 for a node that has not registered since the activation), and never frees its id.
- After any leader change, restart the verifiers and SNodes before an update that runs a gate: the feature book starts empty. Never pass `--evict-unnamed` without first identifying every node it names.
- Keep the snapshot-query lane off while more than one indexer is enrolled: it still hash-selects its source.
- Agent keys go only through `HOUSEGATE_AGENT_KEY`, never on argv. Verifier seeds leave their Secret only for the public-key derivation of §5.2.
- Enrol only self-operated indexers before stage 2 (design §17).
````

`docs/storage-integrity-devnet2-dynamic-table-set.md`: after the stage-5 row of the stage table (line 15) add

```markdown
| 6 | Multi-source stage 1: indexer-b joins as the second SI indexer; see [storage-integrity-devnet2-multi-source.md](storage-integrity-devnet2-multi-source.md) | See that runbook (§10) |
```

and append to §8 Operating rules (after line 941):

```markdown
- Multi-source stage 1 ([storage-integrity-devnet2-multi-source.md](storage-integrity-devnet2-multi-source.md) §11): once signed claims are active, never run a voter, verifier or SNode below `<ARBITER_TAG>` / `<SN_COMMIT>`; SNode node ids are never reused.
```

Fill the placeholders (command at the top of this step), then:

```bash
cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -2
git add docs && git commit -m "docs(storage-integrity): devnet2 multi-source stage 1 runbook

Offline replay check, verifiers (with persistent state) then voters on
$ARBITER_TAG, indexer-a on sentio-node $SN_COMMIT, the irreversible
signed-claims activation, indexer-b's own source ClickHouse and SI
overlay, its irreversible enrolment with snode-2, the seed and the
end-to-end checks (housegate spec 2026-10-10 §13 stage 1).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `OK`.

- [ ] **Step 7: PR (requires explicit user confirmation)**

`charts/`, the new devnet2 resources and `docs/` go through a PR on `sentioxyz/production` (the user's rule: shared charts and new services need a PR):

```bash
cd "$W" && git push -u origin HEAD
gh pr create --repo sentioxyz/production --base main --title "storage-integrity: devnet2 multi-source stage 1 (indexer-b overlay, source ClickHouse, verifier state, render tests, runbook)" --body "$(cat <<'EOF'
## Summary
- sentio-node chart: new `storageIntegrity.snode.genesisSnapshotID`, rendered as `genesis_snapshot_id` (every SI indexer names it: its enrolment statement binds it); `tableIDs` may be empty in the registry-driven shape, with the empty set's schema root and a genesis snapshot id (an SI indexer enrolled after genesis owns no genesis table).
- storage-integrity chart: `verifier.state` gives each verifier a persistent `state_dir` for `registration.json` (off by default; legacy refuses it).
- `validate_source_binding.py`: per-indexer source ClickHouse; the bundled source and every genesis table stay the founding indexer's.
- devnet2: indexer-b's SI overlay (`snode-2`, no genesis tables, storage RPC 33003), its source pin (stage-1 sentio-node), its own source ClickHouse `storage-integrity-si-v2-indexer-b` (the network installation's source profile), and its release in `storage-integrity-si-v2.helmfile.yaml`; indexer-a's SI overlay names the same genesis snapshot id (its live image ff336f1 ignores the key).
- Render tests: one network release with one genesis snapshot id, unique SNode ids, owner-specific genesis sets, no shared source ClickHouse, the installation equal to the network's profile, verifier state, the immutable genesis writer cap.
- Runbook `docs/storage-integrity-devnet2-multi-source.md` (housegate spec 2026-10-10 §13, stage 1).

Inert on merge: nothing is synced or applied until the runbook does it release by release. The network release, the legacy release and every sentio-node release of `helmfile.yaml` and `storage-integrity.helmfile.yaml` render byte-identically; indexer-a's SI release differs only by `genesis_snapshot_id` and its config checksum.

## Test plan
- [x] `python3 -B charts/storage-integrity/tests/test_render.py` (81 tests)
- [x] the CI `helm lint --strict` runs, plus `--set verifier.state.enabled=true`
- [x] `helmfile ... template --selector name=sentio-node-devnet2-indexer-b`
- [x] renders against `origin/main`: byte-identical except indexer-a's SI release (`genesis_snapshot_id` and its config checksum)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Expected: the PR URL; the `Storage-integrity chart validation` workflow passes. Merge only after review and an explicit user yes.

---

## Task 17: devnet2 — stage-1 rollout: replay check, arbiter, indexer-a, activation, indexer-b enrolment, E2E (S1, live)

**Interfaces:**
- Consumes: the merged Task 16 PR (runbook `docs/storage-integrity-devnet2-multi-source.md`, indexer-b release, CHI file, `verifier.state`, `genesisSnapshotID` on both SI overlays, tests); `ARBITER_TAG` / `ARBITER_DIGEST` and `$HOME/si-devnet2-multisource-rehearsal/bin/{old,new}/{statedigest,v19check}` (S1-A Task 12 Step 7, `old` built from `v0.10.0`); `HG_TAG`; `SN_COMMIT` / `SN_DIGEST` (Task 15); Task 14's `kubectl exec <pod> -c sentio-node -- /sentio-node --config-path /config/standalone.yaml storage-integrity enrollment-statement` (stdout only the JWS; needs `PRIVATE_KEY`, the Ethereum RPC, Redis and the configured genesis snapshot id; never contacts the arbiter), its startup checks and its deferred-registration log lines; Task 15's owner-aware table answers; CONTRACT §3c (the activation evicts every registered node it does not name; `--evict-unnamed` consent).
- Produces: signed claims active on devnet2 (epoch 3), indexer-b enrolled (epoch 4, `max_writers` 2, `snode-2`), persistent verifier state, the ledger values `IB_ACTIVATION_BLOCK` (`$HOME/si-devnet2-multisource/indexer-b-activation-block`), `ACTIVATION_EPOCH`, `ENROLMENT_EPOCH`, and the outcome record in the runbook.

**Conventions for this task.**
- The agent runs every bash block with the Bash tool exactly as

  ```bash
  bash -eo pipefail <<'EOF'
  source "$HOME/si-devnet2-multisource/helpers.sh"
  cd "$P"
  <the block>
  EOF
  ```

  `helpers.sh` (Step 1) holds both §0 helper blocks and `P`, the live production worktree; every cross-block value lives in `$si_backup` (`$HOME/si-devnet2-multisource`). A block that may wait longer than 10 minutes runs with `run_in_background: true`; read its output before the next step.
- Every mutation waits for an explicit yes in chat, given for that step only: pushes to production `main`, `helmfile sync`, StatefulSet orphan deletions, pod deletions and `rollout restart`, `kubectl apply`, DDL on a ClickHouse, signing an enrolment statement, `arbiter-admin consensus update`, the acceptance INSERTs, PR creation and merge. Before asking, the agent states exactly what will change. Read-only steps need no confirmation.
- The user runs the two fish blocks below (verifier keys, end-to-end writes) in their own terminal; keys and seeds never enter the chat or argv.
- **IRREVERSIBLE POINT 1 is Step 10 (activation); IRREVERSIBLE POINT 2 is Step 17 (enrolment).** Before Step 10 the way back is runbook §10.1; between Steps 10 and 17, §10.3 for indexer-b only; after Step 17, none.
- Stop on any unexpected output and report it; never improvise a fix on the cluster.

- [ ] **Step 1: Live worktree and helpers (local only)**

After the Task 16 PR has merged:

```bash
git -C /Users/uranuswch/Dev/sentio_xyz/production fetch origin
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
' urwt /Users/uranuswch/Dev/sentio_xyz/production claude multi-source-s1-live
```

With `P=<path from the JSON>`:

```bash
P='<path from the JSON>'   # replace with the "path" field printed above
case "$P" in '<'*) echo "set P first"; exit 1;; esac
mkdir -p -m 700 "$HOME/si-devnet2-multisource"
{ python3 -I - "$P/docs/storage-integrity-devnet2-dynamic-table-set.md" "$P/docs/storage-integrity-devnet2-multi-source.md" <<'PY'
import re, sys
for path in sys.argv[1:]:
    section = open(path).read().split("## 0. Session helpers", 1)[1].split("\n## ", 1)[0]
    blocks = re.findall(r"```bash\n(.*?)```", section, re.S)
    if not blocks:
        sys.exit(f"{path}: no bash block in section 0")
    print(blocks[0])
PY
  printf 'P=%q\n' "$P"; } > "$HOME/si-devnet2-multisource/helpers.sh"
bash -c 'source "$HOME/si-devnet2-multisource/helpers.sh" && type si_show si_replace_voter si_accept_a si_features si_node_image si_stage1_image >/dev/null && echo "helpers ok: si_backup=$si_backup R=$R P=$P"'
git -C "$P" log --oneline -1 && git -C "$P" grep -c 'sentio-node-devnet2-indexer-b' -- k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml
```

Expected: `helpers ok: si_backup=$HOME/si-devnet2-multisource R=$HOME/si-devnet2-multisource-rehearsal P=…/multi-source-s1-live`; the head is `origin/main` with the merged PR; count `1`. Before every later commit: `git -C "$P" fetch origin && git -C "$P" merge --ff-only origin/main`.

- [ ] **Step 2: Pins, identities, baseline (read-only)** — runbook §1.1, §1.2, §1.3 blocks.

Expected: `pins resolve`; the identities of §1.2 exactly (indexer 0 signer `0x22a6…31f4` / 32001 / 32003, indexer 1 signer `0x20c8…16f3` / 33001 / 33003, `devuser1` → indexer 1); genesis snapshot id `0x63700cf1…1c06`, named by both SI overlays (`indexer-a overlay names …`, `indexer-b overlay names …`); baseline epoch `"2"`, `max_writers` `"1"`, `client_lanes` 256, `si_indexers` / `verifiers` `null`, six arbiter pods on `sha256:54dda9db…` (live on 2026-10-10), voters' `features` `["client_lanes_v1"]`. Record the leader (`si_leader`) and `registry_version`.

- [ ] **Step 3: Offline replay check (read-only on the cluster; local CPU)** — runbook §2.

Confirm first that `$R/bin/old/{statedigest,v19check}` were built from the release in `si_images` (`v0.10.0` unless it changed; otherwise rebuild `old` from that tag as S1-A Task 12 Step 7 does). Expected: three `old and new replay identically`, `written_version 18` for both builds, no `key registration_seqs`, `live_check … epoch=2 …` equal to `show-replay.json`. Any difference: stop; nothing has changed yet.

- [ ] **Step 4: Verifiers to `ARBITER_TAG` with persistent state (requires explicit user confirmation)**

In `$P`, one commit. In `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml`, lines 70-79 become (fill `<ARBITER_COMMIT_SHORT>` from `git -C /Users/uranuswch/Dev/sentio_xyz/arbiter rev-parse --short=8 "$ARBITER_TAG^{commit}"` and `<DATE>` with Step 3's date):

```yaml
    # arbiter <ARBITER_TAG> (<ARBITER_COMMIT_SHORT>), pinned here rather than in the
    # chart default so a later chart pin cannot change this network's consensus
    # runtime unreviewed. It carries arbiter-core <CORE_TAG> and the multi-source
    # stage-1 FSM (si_indexers, verifiers, signed SNode and verifier messages,
    # owner-bound sources, snapshot v19), inert until the authority-signed
    # signed-claims activation. The live voters' Raft logs replay identically
    # under v0.10.0 and <ARBITER_TAG> (offline statedigest check, <DATE>). The
    # verifiers (RollingUpdate) move on sync while the voters (OnDelete) keep
    # running v0.10.0 until they are replaced one at a time; see
    # docs/storage-integrity-devnet2-multi-source.md §3.
    # Previous pin: v0.10.0 sha256:54dda9db84d14d60b81900e4a61af06fac31f47460ae80ccd2c7af0d7aad5b29
    digest: <ARBITER_DIGEST>
```

and the `verifier:` block (lines 117-120) gains:

```yaml
  # Persistent state dir (registration.json, the verifier's last
  # registration_seq), turned on with arbiter <ARBITER_TAG>; the StatefulSet is
  # deleted with --cascade=orphan right before the sync that adds the volume
  # (docs/storage-integrity-devnet2-multi-source.md §3.1).
  state:
    enabled: true
```

In `charts/storage-integrity/tests/test_render.py`: `DEVNET2_ARBITER_DIGEST = "<ARBITER_DIGEST>"` (line 29) and `DEVNET2_VERIFIER_STATE_ENABLED = True`. Then:

```bash
cd "$P" && python3 -B charts/storage-integrity/tests/test_render.py 2>&1 | tail -1
git -C "$P" add -A && git -C "$P" commit -m "chore(devnet2): arbiter $ARBITER_TAG and persistent verifier state for multi-source stage 1

Verifiers move first, each with a state volume for registration.json;
voters are OnDelete and keep v0.10.0 until replaced one at a time
(docs/storage-integrity-devnet2-multi-source.md §3). The voters' logs
replay identically under v0.10.0 and $ARBITER_TAG.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

Expected: `OK`. Gate: "push the verifier commit to production main, delete the verifier StatefulSet with --cascade=orphan (pods keep running), sync release storage-integrity-si-v2: the verifiers roll one at a time onto <ARBITER_TAG>, each with a new 1Gi local-path claim; the voters do not change". After a yes, run runbook §3.1's block. Expected: §3.1's (three `state-…` claims, three `registration.json` files, three active verifiers, safe following sealed).

- [ ] **Step 5: Replace the voters one at a time (requires explicit user confirmation for each deletion)**

```bash
si_leader | tee "$si_backup/leader-before-replacement"
```

For each ordinal in order — the two followers, then the recorded leader — ask "delete voter storage-integrity-si-v2-arbiter-<i> (OnDelete) to move it to <ARBITER_TAG>", and after a yes run `si_replace_voter <i>` (it waits up to 15 minutes; run it in the background and read its output). Expected after each: its capability line with `features` `["client_lanes_v1","signed_claims_v1"]` and three equal `arbiter_sealed_block_seq` lines; a failure prints `sealed tips still differ after 5 minutes; stop` and the procedure stops. After the third: `si_images` shows all six arbiter pods on `<ARBITER_DIGEST>`. Voter rollouts are frozen from here until Step 11.

- [ ] **Step 6: indexer-a to `SN_COMMIT` (requires explicit user confirmation)**

In `$P`, one commit: `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml` becomes

```yaml
# Merged LAST into indexer-a by storage-integrity-si-v2.helmfile.yaml: the
# embedded source SNode must run arbiter-core v0.8.0 or later; this pin runs
# sentio-node <SN_COMMIT>: housegate <HG_TAG> (SOURCE_UNAVAILABLE as a
# retryable, session-preserving refusal, on top of v0.17.2's client_seq lanes,
# contract writer authorization, SI ingress denylist and sipeerguard),
# rewriter-proto/rewriter-go as resolved by that build, arbiter-core <CORE_TAG>
# (SNode messages signed with indexer-a's key, owner-scoped data plane;
# advertises signed_claims_v1, inert until the signed-claims activation). It
# checks the overlay's genesisSnapshotID against indexer-a's genesis table at
# startup. indexer-b runs the same image. Its startup probe needs rewriter-grpc v0.17.0
# or later, so it moves together with the in-pod rewriter pinned in
# sentio-node-indexer-a-storage-integrity-si-v2.yaml.
# Stage-2 rollback target of docs/storage-integrity-devnet2-multi-source.md
# (only before the signed-claims activation, §10.1):
# sha-ff336f146993ebe76a39184bc0f820fa5de036d6@sha256:29ca9da403559f68fba64900b7739ecf92f2818b55e38b5e130a3e07a25673a8.
# A copy of the verified input in docs/examples/storage-integrity-fresh-source-image.yaml
# (sentio-node <SN_COMMIT>, official main CI). Keep the two identical; the
# render tests check it.
node:
  image:
    repository: ghcr.io/sentioxyz/sentio-node
    tag: "sha-<SN_COMMIT>@<SN_DIGEST>"
    pullPolicy: IfNotPresent
```

`docs/examples/storage-integrity-fresh-source-image.yaml` gets the same `node` block, its header's `sentio-node ff336f1: housegate v0.17.2, …, arbiter-core v0.12.1 (sentio-node PRs 192, 193, 194)` line rewritten to `sentio-node <SN_COMMIT>: housegate <HG_TAG>, rewriter-proto/rewriter-go as resolved by that build, arbiter-core <CORE_TAG> (multi-source stage 1)`, and `FRESH_SOURCE_IMAGE` in `test_render.py` (38-41) becomes `"ghcr.io/sentioxyz/sentio-node:sha-<SN_COMMIT>" "@<SN_DIGEST>"` (already in `WRITER_AUTHORIZATION_IMAGES`). Fill the placeholders, run the tests (`OK`), commit `chore(devnet2): indexer-a sentio-node <SN_COMMIT> for multi-source stage 1` (body: "Embeds HouseGate <HG_TAG>, checks the overlay's genesis snapshot id against the genesis table at startup and advertises signed_claims_v1, inert until the activation; rollback target ff336f1 until then." and the Co-Authored-By line).

Gate: "push the indexer-a image commit and sync release sentio-node-devnet2-indexer-a (pod restarts onto the stage-1 image, which embeds HouseGate <HG_TAG> and checks the overlay's genesis snapshot id; one acceptance INSERT value 801 into devnet101.swap_new2)". After a yes, run runbook §4's block. Expected: §4's — `indexer-a runs the stage-1 image`, the pod Ready (so the configured genesis snapshot id matched the one derived from its genesis table; a startup refusal naming `storage_integrity.snode.genesis_snapshot_id` stops the rollout, and the way back is §10.1, asked first), `enrolled: true`, `si_indexer_id 0`, a 13-digit `registration_seq`, safe read `1`, and in `si_features` the entry `snode-1` with `claims: true`. That entry is the CONTRACT §3b.7 check: the SNode advertises `signed_claims_v1` only when sentio-node gave it a claim signer, so `claims: false` or a missing `snode-1` stops the rollout here.

- [ ] **Step 7: Re-register the data plane and drain (requires explicit user confirmation for the restart)**

Gate: "rollout restart the three verifiers so they re-register with the current leader". After a yes, run runbook §5.1's block. Expected: §5.1's — nodes exactly `snode-1`, `verifier-1..3`, all `claims: true` (this is the pre-activation feature-book check of `snode-1`'s claim signer); drained metrics. If `snode-1` is missing (a new term since Step 6 cleared the book), also restart indexer-a (separate yes) and repeat the block. Any further node would be evicted by the activation (CONTRACT §3c): stop and report it; this rollout never passes `--evict-unnamed`.

- [ ] **Step 8: indexer-a's enrolment statement (requires explicit user confirmation: signs with indexer-a's key)**

Gate: "run the enrolment-statement subcommand inside sentio-node-devnet2-indexer-a-0; it signs {devnet2, genesis 0x63700cf1… (the overlay's genesisSnapshotID), indexer 0, snode-1} with indexer-a's key, reads the chain and Redis, never contacts the arbiter and changes nothing". After a yes, run the first two commands of runbook §5.2. Expected: header `ES256K`, `purpose` `arbiter-snode-enrollment-v1`.

- [ ] **Step 9: Verifier keys (user, fish)**

Ask the user to run, in their own fish terminal (OpenSSL 3 from Homebrew first in `PATH`; the first command must print `d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a`):

```fish
printf '302e020100300506032b657004220420%s' 9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60 | xxd -r -p | openssl pkey -inform DER -pubout -outform DER | tail -c 32 | xxd -p -c 64
for n in 1 2 3
    set -l seed (kubectl --context sentio-sea -n sentio-network-devnet2 get secret storage-integrity-devnet2-si-v2-verifier-seeds -o "jsonpath={.data.verifier-$n}" | base64 -d | string trim)
    printf 'verifier-%s %s\n' $n (printf '302e020100300506032b657004220420%s' $seed | xxd -r -p | openssl pkey -inform DER -pubout -outform DER | tail -c 32 | xxd -p -c 64)
end > ~/si-devnet2-multisource/verifier-pubkeys.txt
cat ~/si-devnet2-multisource/verifier-pubkeys.txt
```

Then the agent checks the file: `awk 'NF != 2 || $2 !~ /^[0-9a-f]{64}$/ {bad=1} END {exit bad}' "$si_backup/verifier-pubkeys.txt" && cut -d' ' -f1 "$si_backup/verifier-pubkeys.txt" | tr '\n' ' '` → `verifier-1 verifier-2 verifier-3`, three distinct keys. Only public keys are in the file.

- [ ] **Step 10: Signed-claims activation (requires explicit user confirmation; IRREVERSIBLE POINT 1)**

Re-run `si_features` and the drain metrics of §5.1 immediately before asking. Gate, stated in these words: "commit the signed-claims activation (epoch 2 → 3): si_indexers = [{0, 5508931, 0x22a67f498e669cbde18a0eecfff74a72c48331f4, snode-1, <the Step 8 JWS>}], verifiers = verifier-1..3 with the Step 9 keys, max_writers 1; these lists name every registered node, so nothing is evicted. This is irreversible: v19 snapshots, signed messages only, no image downgrade ever again." After a yes, run runbook §5.3's block; its first line refuses unless indexer-a runs the stage-1 image. Expected: `activate.out` holds the committed answer. On a gate refusal or `InvalidArgument`: nothing changed; report, fix, ask again. A `would evict … --evict-unnamed` refusal names an unexpected registration: report it and stop. On "commit outcome may be unknown": `si_show` first.

- [ ] **Step 11: Verify the activation and the re-registration canary (requires explicit user confirmation for the restarts and acceptance INSERTs)**

Run the first block of runbook §5.4 (one acceptance INSERT, value 802; ask first). Expected: epoch `"3"` (record `ACTIVATION_EPOCH=3`), the single `si_indexers` entry, `verifiers` `verifier-1..3`, `verifier keys committed as derived`, `registry_version` + 1, safe read `1`. Then gate "rollout restart the verifiers and indexer-a to prove signed re-registration; indexer-a's tables answer SOURCE_UNAVAILABLE (retryable) for about a minute; one acceptance INSERT value 803", and run the second block. Expected: four nodes `claims: true`, higher `registration_seq` values in the verifiers' `registration.json`, safe read `1`. The v19 snapshot check of §5.4 runs later, whenever a snapshot exists (read-only). The voter-rollout freeze ends here.

- [ ] **Step 12: indexer-b read-only checks** — runbook §6.1.

Expected: §6.1's. Stop (and ask the user) on a non-empty `HGET`, a running driver task or any `parts=` above 0.

- [ ] **Step 13: indexer-b's source ClickHouse (requires explicit user confirmation)**

Run the existence check and the server dry run of §6.2 first (read-only). Gate: "kubectl apply the ClickHouseInstallation storage-integrity-si-v2-indexer-b (new pod storage-integrity-si-v2-indexer-b-source-0-0-0 with 10Gi + 1Gi local-path volumes; joins Keeper root /storage-integrity/devnet2/devnet2-si-v2; clickhouse-devnet2 is untouched)". After a yes, run the rest of §6.2. Expected: §6.2's.

- [ ] **Step 14: indexer-b's ordinary objects (requires explicit user confirmation)**

Generate `create.sql` with §6.3's converter and show it in full to the user. Gate: "run these statements on indexer-b's new source ClickHouse". After a yes, run the apply and the listing. Expected: §6.3's.

- [ ] **Step 15: Sync indexer-b (requires explicit user confirmation)**

Gate: "sync release sentio-node-devnet2-indexer-b from storage-integrity-si-v2.helmfile.yaml: pod restarts onto sentio-node <SN_COMMIT>, rewriter 0.17.0, sidecar v0.15.0, ClickHouse storage-integrity-si-v2-indexer-b-source; its SNode (no genesis table, empty-set schema root, genesis snapshot id 0x63700cf1…) defers its registration until the enrolment". After a yes, run runbook §6.4's block. Expected: §6.4's (`indexer-b runs the stage-1 image`, the pod Ready with no startup refusal about the schema declarer's network id or the genesis snapshot id, `si_indexer_id null`, `self_indexer_id 1`, `enrolled false`, the `storage-integrity SNode registration deferred: …` warning, `ordinary` answers, `0` rows). Until Step 17 the way back is runbook §10.3 (ask first).

- [ ] **Step 16: indexer-b's enrolment statement (requires explicit user confirmation: signs with indexer-b's key)**

Gate: "run the enrolment-statement subcommand inside sentio-node-devnet2-indexer-b-0; it signs {devnet2, genesis 0x63700cf1… (the overlay's genesisSnapshotID), indexer 1, snode-2} with indexer-b's key, reads the chain and Redis, never contacts the arbiter and changes nothing". After a yes, run runbook §6.5's block. Expected: `ES256K`, `arbiter-snode-enrollment-v1`.

- [ ] **Step 17: Enrolment of indexer-b (requires explicit user confirmation; IRREVERSIBLE POINT 2)**

Compute and show the activation block first (read-only): `echo "safe head $(si_safe_head); activation $(( $(si_safe_head) + 1800 ))"`. Gate, stated in these words: "commit the enrolment (epoch 3 → 4): append {1, <activation>, 0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3, snode-2, <the Step 16 JWS>} with entry 0 restated byte-identically from consensus show, max_writers 2. Irreversible: the entry can never be removed, and from block <activation> on a stuck indexer-b stalls every indexer's safe progress." After a yes, run runbook §7's first block (its first two lines refuse unless both SI indexers run the stage-1 image and indexer-b runs its SI overlay, not yet enrolled: the overlay must be live before the activation block this update picks), then its verify block (it waits up to 10 minutes for `snode-2`). Expected: §7's, including `storage-integrity SNode registered after its enrolment committed` in indexer-b's log, `"si_indexer_id":1,"enrolled":true`, and `ordinary` from indexer-a for `devuser1.t` and `devuser3b`, and from indexer-b for `devnet101.swap_new2` and `devuser1.t`; record `ENROLMENT_EPOCH` and `IB_ACTIVATION_BLOCK` (`$si_backup/indexer-b-activation-block`).

- [ ] **Step 18: Seed at indexer-b's activation block (read-only; about one hour)**

Run runbook §8's block with `run_in_background: true` and wait for it. Expected: §8's (`seed` applied on the leader, no halts or mismatches, `devuser1.t` `ordinary`).

- [ ] **Step 19: End to end (user writes in fish; agent checks; requires explicit user confirmation for the acceptance INSERT)**

The user runs, in their own fish terminal, with the key that owns `devuser1` (never pasted into chat; housegate `HG_TAG` or any release with the 2026-10-09 agent):

```fish
set -l key_file /path/to/devuser1-owner.key
begin
    set -lx HOUSEGATE_AGENT_KEY (cat $key_file)
    housegate > ~/si-devnet2-multisource/e2e-agent.log 2>&1 &
end
sleep 5; tail -n 20 ~/si-devnet2-multisource/e2e-agent.log
clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'CREATE TABLE s1_e2e (id UInt64, v String) ENGINE = MergeTree ORDER BY id'
```

The agent polls `si_status b devuser1 s1_e2e` until `active` (`pending` first, while indexer-b's SNode readies the table), checks `si_status a devuser1 s1_e2e` and `si_status b devnet101 swap_new2` = `ordinary`, and records `base="$(si_safe)"` in `$si_backup/e2e-base`. Then the user runs:

```fish
printf '1,a\n2,b\n' | clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'INSERT INTO s1_e2e FORMAT CSV'
printf '3,c\n' | clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --query 'INSERT INTO devuser1.s1_e2e FORMAT CSV'
string match -ri 'switch|upstream|indexer' < ~/si-devnet2-multisource/e2e-agent.log | tail -n 10
```

The agent runs the read-only checks of runbook §9 (safe watermark past the base, `hg_safe` 3 rows on indexer-b's source, 3 rows in every verifier's `hg_unsafe`, the two `system.tables` inventories), then asks before `si_accept_a 901`. The user reads back and drops:

```fish
clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query "SELECT count() FROM s1_e2e SETTINGS SQL_x_read_mode='safe'"
clickhouse-client --host 127.0.0.1 --port 9000 --compression 0 --database devuser1 --query 'DROP TABLE s1_e2e'
```

The agent polls `si_status b devuser1 s1_e2e` until `pending` and runs the remaining checks of §9; finally the user stops the agent (`jobs`, then `kill (jobs -p)`). Expected: every bullet of runbook §9's expected list, in particular `3` from the safe read, indexer-b's source holding only `devuser1__s1_e2e` `hg_*` tables and indexer-a's none, and no `ADMISSION_CODE_SOURCE_UNAVAILABLE` beyond Step 11's window.

- [ ] **Step 20: Record the outcome (requires explicit user confirmation for the push and the PR)**

In `$P` (after `merge --ff-only origin/main`), one docs commit on a new branch: in the runbook, a short paragraph at the end of §7 recording the dates, `ACTIVATION_EPOCH` 3, `ENROLMENT_EPOCH` 4, `IB_ACTIVATION_BLOCK`, the seed confirmation and the E2E result; and the `Since` column of §1.2's ledger for `snode-2`. Open it as a small PR (`docs(storage-integrity): record the devnet2 multi-source stage 1 rollout`), body ending with `🤖 Generated with [Claude Code](https://claude.com/claude-code)`. Keep `$HOME/si-devnet2-multisource` (backups, JWS files, digests) until stage 2 starts; `wt remove` the live worktree only when the user asks.

**What rollback means here.** Before Step 10: runbook §10.1 (revert Step 6, revert Step 4 with the verifier StatefulSet orphan-deleted again, replace the voters back one at a time; the voters wrote only v18 and no new field). From Step 10 on: no rollback of any voter, verifier or SNode image (§10.2); the activation cannot be undone. Between Steps 13 and 17: indexer-b alone can go back to `clickhouse-devnet2-node-b` with its previous images (§10.3); its new installation may stay. From Step 17 on: none (§10.4); the remedy for a stuck indexer-b is bringing its SNode back on its PVC.

---

## Appendix B: drafting decisions and cross-task facts

Each task group was drafted against the binding contract (Plan S1-A Appendix A). This appendix keeps, per group, how each contract question was resolved and the facts later tasks rely on. Where a note here disagrees with the Global Constraints or with Plan S1-A Appendix A, those win.

### Tasks 13–15: contract conflicts (resolved)

Resolved by CONTRACT §3b and applied above: the SNode's genesis snapshot id (§3b.3, configured as `storage_integrity.snode.genesis_snapshot_id`), the empty genesis set (§3b.8) and the reconciler owner (`tableset.Config.Owner`, §3b.1; sentio-node does not construct the reconciler).

1. **CLI flag.** The first draft of the host contract wrote `--config <path>`; sentio-node's root command owns the persistent flag `--config-path` (`main.go:42`) that every subcommand inherits. Task 14 follows the repository: `sentio-node --config-path <path> storage-integrity enrollment-statement` (`/sentio-node` in the image, `Dockerfile` `ENTRYPOINT`).
2. **Where the CLI gets the genesis snapshot id.** The first draft of the host contract described a `schema_snapshot_id`-derived id; with §3b.3 and "the CLI must not need the arbiter", deriving it would mean loading the genesis schemas inside the CLI (ClickHouse, or the Databases contract through the configured schema source), and only the founding indexer could. Proposal, as written in Task 14: the CLI requires the configured `storage_integrity.snode.genesis_snapshot_id` on every indexer, including the founding one; Task 16 sets it on indexer-a's overlay too (startup checks it against the derivation from indexer-a's genesis tables, so a wrong value cannot run), and the arbiter checks the signed statement against its own genesis snapshot id when it applies the update (CONTRACT §3 rule 4).
3. **Pin triple for arbiter-proto.** Neither housegate's nor sentio-node's `MODULE.bazel` has a `bazel_dep`/`git_override` for arbiter-proto (both resolve it through `go_deps.from_file`); `go.mod` is its only pin. housegate and arbiter-core keep all three pins in sentio-node.
4. **"Exactly how `LANE_BUDGET_EXCEEDED` is surfaced"** cannot hold for a session-preserving refusal: the lane and gap budget refusals are classified as coded terminal rejects (which Task 13 copies) but surfaced as a plain unspent-marked error with the generic 403, which closes the session (`storage_integrity_ingress.go:841-846`). The `ClientError` + `KeepSession` + `SeqUnspent` construction comes from the `SCHEMA_NOT_ALLOWED` (`:825-836`) and merge-latch 733 (`:669-672`) refusals, and the deferred lane additionally needs `isSessionPreservingIngressException` to recognise the exact message (`pkg/proxy/relay.go:2309-2325`); Task 13 does both.

### Tasks 13–15: facts for other tasks

- **Ordering.** Every SI host image must carry `HG_TAG` before the activation update commits: an older HouseGate maps code 10 to `OutcomeUnknown` (status-probe convergence, no prompt cleanup). indexer-a's `SN_DIGEST` image embeds `HG_TAG` and `CORE_TAG`. The driver sidecar keeps its session on a `SOURCE_UNAVAILABLE` refusal only from `HG_TAG` on (older sidecars close the connection; the refusal stays retryable); indexer-b's sidecar runs v0.12.0 today and should move with indexer-a's pin (spec §11, v0.15.0 or later) when Task 16 aligns images.
- **SNode `indexer_id` needs no chart value** (Task 16): standalone resolves it on-chain (`env.EnsureRegistered`, `standalone.go:94-98`) and passes `env.IndexerRuntime.ResolvedIndexerID()`, the value `sentio_getStorageIntegrityInfo` reports as `self_indexer_id`. An SI-enabled observer node now refuses startup.
- **New config key `storage_integrity.snode.genesis_snapshot_id`** (Task 16): the chart needs a value rendering it, quoted like `schema_root`. Validation: a `0x`-prefixed 64-digit lowercase hex digest; required when `table_ids` is empty (`storage_integrity.snode.genesis_snapshot_id is required when storage_integrity.snode.table_ids is empty: …`). devnet2-si-v2: `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06`; `arbiter-admin consensus show --address <leader>` prints it as `genesis_snapshot_id`. Set it on both overlays: indexer-b needs it to start, indexer-a needs it for the enrolment CLI (Task 17 activation statement). On indexer-a startup checks it against the derivation from its genesis tables and refuses a mismatch with `storage_integrity.snode.genesis_snapshot_id <configured> is not the network's genesis snapshot id <derived> derived from this node's genesis tables`.
- **indexer-b overlay values** (Task 16): `storageIntegrity.snode.tableIDs: []` (or omitted; the loader accepts nil); `genesisSnapshotID` as above; `schemaRoot: "0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59"` (pinned by `TestEmptyGenesisSchemaRoot`; config validation refuses any other root with an empty `table_ids`); `networkID: devnet2`, `schemaSnapshotID: devnet2-si-v2`, `executorProfileID`, `authorityAddresses` and `arbiterPeers` identical to indexer-a's; `nodeID` = the runbook-fixed id = `housegate.storageIntegrity.runtime.expectedSource`; its own state dirs. `table_ids` must list only genesis tables indexer-b owns: the genesis plan does not check incarnation owners, so listing indexer-a's `devnet101.swap_new2` would make indexer-b's preflight create its `hg_*` tables. The registry must be enabled (it is on devnet2).
- **indexer-b can be deployed with its SI overlay before its enrolment** (Task 17 step 5): it starts, its tables answer `ordinary`, the log shows `storage-integrity SNode registration deferred: this indexer is not enrolled in the arbiter's si_indexers yet; its tables answer ordinary until the enrolment commits`, `sentio_getStorageIntegrityInfo` answers `"enrolled": false, "si_indexer_id": null`. After the enrolment update commits the follower wakes the task, the SNode registers and marks itself Active, and the log shows `storage-integrity SNode registered after its enrolment committed`; then `"enrolled": true, "si_indexer_id": 1`. Its SNode advertises `signed_claims_v1` only from that registration on, so it is absent from the leader's feature book at activation time (the activation gate covers registered nodes only). An enrolment naming another `snode_node_id` or signer fails the runtime with `indexer 1 is enrolled with snode_node_id … / signer …`, and the restarted pod refuses startup with the same message (bootstrap step `check-enrollment`).
- **The arbiter's refusal of an unenrolled SNODE registration** is `codes.InvalidArgument` (FSM `Rejected`, `arbiter:server/server.go:256-260`), which `WithLeaderRetry` returns at once; sentio-node therefore decides from the registry view, and the S1 arbiter must bump the registry version on every `si_indexers` change (CONTRACT §3a) or a deferred registration waits for its 30 s poll instead of the wake-up.
- **Enrolment statement CLI** (Task 17 activation and step 5): run in the indexer pod, `kubectl exec <pod> -c <sentio-node container> -- /sentio-node --config-path <rendered config path> storage-integrity enrollment-statement > indexer-b-enrollment.jws` (fish: `> indexer-b-enrollment.jws` works unchanged). It needs `PRIVATE_KEY` (the container's env), the Ethereum RPC, Redis (`common.NewNodeEnv` pings it) and a configured `storage_integrity.snode.genesis_snapshot_id`; it never contacts the arbiter. stdout carries only the JWS line, logs go to stderr. The statement binds `snode.network_id`, `snode.node_id`, the IndexerRegistry id of the key's signer (read-only lookup, no auto-registration) and the configured genesis snapshot id; a wrong value is refused by the arbiter when the update is applied, not by the CLI. Refusals before any read: no indexer node, SI disabled, no genesis snapshot id, no `PRIVATE_KEY`.
- **Schema network id.** On an SI node the declarer always hashes under `snode.network_id` (`standalone/schema_registry.go:58-61`); before its SI overlay indexer-b declares under `physical_database` (`devnet2` on devnet2, `k8s-sea/sentio-network-devnet2/common/sentio-node-indexer.yaml:1`), so the two agree there. The runbook must still apply indexer-b's SI overlay before its activation block, so every table created at or after it is declared under the arbiter network id. The new startup refusal reads `storage integrity: the schema declarer hashes table schemas under network id "<declarer>", but the arbiter network id (storage_integrity.snode.network_id) is "<snode>"; …`.
- **`sentio_getStorageIntegrityInfo`** gains `enrolled`; `si_indexer_id` is the host's own id while enrolled (on indexer-a: 0 before and after activation; on indexer-b: null until enrolment, then 1). housegate's `registry.StorageIntegrityInfo` has no `enrolled` field and `json.Unmarshal` ignores it; the agent needs no change (spec §11).
- **Owner-aware answers to expect on devnet2 after enrolment:** indexer-a answers `ordinary` for every `devuser1` / `devuser3b` table (owner 1), indexer-b answers `ordinary` for `devnet101.swap_new2` and every other owner-0 table; a table created in `devuser1` after indexer-b's activation block is `pending` on indexer-b until registered/ready, then `active`; `devuser1.t` (created before the block) stays `ordinary` until indexer-b's seed commits, then becomes Legacy (`ordinary`).
- **Compensation** on indexer-b runs only while it is enrolled and from its own activation block (no declaration transactions before then).
- **indexer-a after the `SN_DIGEST` upgrade** (Task 17 step 3): the founding indexer counts as enrolled before `si_indexers` exists, so its SNode registers synchronously at startup exactly as today; sentio-node always sets `ClaimSigner` (with `IndexerID`), so the registration advertises `signed_claims_v1` (CONTRACT §3b.7) and the leader's feature book (`arbiter-admin` / `ConsensusAdmin.GetNodeFeatures`) lists it for `snode-1` once the pod has restarted against the current leader. The SNode's signatures before activation are dropped by the arbiter (CONTRACT §0). sentio-node leaves `snode.Deps.Now` nil (`time.Now`), so the first registration after the upgrade jumps `registration_seq` to the current Unix milliseconds (CONTRACT §3a); the FSM only requires a higher sequence.
- **For Task 4 (optional hardening):** arbiter-core takes a configured `snode.Config.GenesisSnapshotID` as given even when it holds genesis tables to derive it from (`claimGenesisSnapshotID`); sentio-node performs that check itself (`checkStorageIntegrityGenesisSnapshotID`), so no change is required.
- **SNode state** (Task 16): the persisted `registration_seq` lives in the SNode's `storage_integrity.snode.state_dir` (arbiter-core's `state.json`); sentio-node passes nothing for it. Each indexer's overlay needs its own persistent state dir; a lost dir only moves the sequence to `now()` in milliseconds (CONTRACT §2).

### Tasks 16–17: contract conflicts (resolved)

- **C1. "Allow `maxWriters` > 1" in `charts/storage-integrity` and the render test "`maxWriters` 2 accepted" are not implemented.** `genesis.maxWriters` renders into the arbiter's `genesis.max_writers`, the immutable bootstrap value (arbiter `README.md:56-75`: "Changing YAML cannot resize an existing writer pool and fails the bootstrap trust check"; `fsm/consensus_history.go:26-28` refuses a restore whose snapshot bootstrap params differ from the configured genesis params; a replay from genesis with another cap would apply the first consensus update over a different `PreviousParamsDigest`). Raising it on devnet2 would stop every voter; on a fresh network it only enables a multi-writer pool with hash source selection before `si_indexers` exists, the failure mode of spec §2 (a fresh network cannot set `si_indexers` at genesis). The runtime limit is raised by the enrolment update (`--max-writers 2`, Task 17 Step 17). Proposal (implemented in Task 16): keep `enum: [1]`, document the bootstrap semantics in `values.yaml` and the schema description, keep `test_only_single_writer_is_supported_by_pilot`, and add `test_devnet2_genesis_writer_cap_stays_one`.
- **C2. No read path exposes the registered verifiers' ed25519 keys before the activation**, which must list each verifier with its registered key (CONTRACT §3 rule 6; under §3c a listed verifier registered under another key refuses the update): `GetNodeFeatures`' `NodeFeatureEntry` carries `node_id`, `features`, `registered_unix` only (arbiter-proto `consensus.proto:114-118`), `consensus show` prints `verifiers` only once set, and `arbiter-verifier` has no flag that prints its key. Fallback used here: the operator derives each key from `storage-integrity-devnet2-si-v2-verifier-seeds` (Task 17 Step 9, fish, with an RFC 8032 self-test); a wrong key is refused atomically. Proposal for the next arbiter-proto/arbiter release: `NodeFeatureEntry.ed25519_pubkey = 4` (and `registration_seq = 5`, which also closes S1-A C9), filled by `GetNodeFeatures`, printed by `arbiter-admin consensus features`.
- **C3. "macros `replica` = the SNode node id" is not needed.** arbiter-core writes the replica name literally into the ReplicatedMergeTree DDL (`dataplane/ddl/build.go:47-48`, `ReplicaName` = `Pinned.NodeID`) and drops stale replicas by that literal (`table.go:110-178`); the operator's `{replica}` macro (`storage-integrity-si-v2-source-replica-0` live) is never used by SI DDL. The new installation keeps the operator default, like the network installation.
- **C4. Option (a) of the contract cannot work without a CHI-wide user change.** The SNode's ClickHouse client has no credentials (sentio-node `standalone/standalone.go:211-213`), `clickhouse-devnet2`'s `default` user has a password, and CHI users are not per cluster. Decided: option (b), D16-1.
- **C5. Enrolment subcommand spelling.** The contract writes `sentio-node storage-integrity enrollment-statement --config <path>`; sentio-node's cobra root owns only the persistent `--config-path` (`main.go:42`), and the Task 14 draft and the sentio-node drafter's binding facts follow it: `kubectl exec <pod> -c sentio-node -- /sentio-node --config-path <rendered config path> storage-integrity enrollment-statement`. The runbook and Task 17 run exactly that through `si_indexer` / `si_indexer_b` (`kubectl exec … -c sentio-node --`) with the pod's rendered config `/config/standalone.yaml` (the container's own `--config-path`, `templates/node.yaml:643-650`) and `PRIVATE_KEY` from the container env.
- **C6. "Same pins as indexer-a's stage-1 image" cannot be an equality test in the PR.** At PR time indexer-a still pins `ff336f1` (its move is the live single-service commit of Task 17 Step 6). The PR pins indexer-b to the stage-1 image, mirrors it as `INDEXER_B_SOURCE_IMAGE`, lists it in `WRITER_AUTHORIZATION_IMAGES`, and the runbook moves indexer-a to the same image; after Step 6 `FRESH_SOURCE_IMAGE == INDEXER_B_SOURCE_IMAGE`.
- **C7. The chart's proxy for "registry enabled".** The chart cannot know whether the arbiter's registry is enabled; it accepts an empty `tableIDs` only in the registry-driven shape (`housegate.storageIntegrity.enabled` with no `tables`), with the empty-set schema root and a genesis snapshot id, and it checks only the spelling of a genesis snapshot id (it cannot derive the network's). sentio-node's own validation (Task 14: the empty-set root, the required id, and on an SNode with genesis tables the comparison with the derived id) remains the authority at startup; `test_render.py` pins the devnet2 value for both overlays.
- **C8. The verifier's state-dir input (CONTRACT §3b.9 says "flag/config"); resolved by the S1-A Task 8 draft.** The chart renders it as the YAML key `state_dir` in `verifier.yaml`, next to `promote_database`, and the Task 8 draft adds exactly that key to `cmd/arbiter-verifier` (`Config.StateDir`, passed through `toRoleConfig`, with a startup warning when it is empty). The current `cmd/arbiter-verifier/config.go` decodes with plain `yaml.Unmarshal` (`:77`), so a binary that predates the key ignores it. Task 8 also adds an optional verifier `genesis_snapshot_id`; the chart leaves it unset because every verifier lists the complete genesis set and derives it. Should the key change before release, Task 16 changes the one ConfigMap line and `VerifierStateTest`.
- **C9. Verifier state needs a StatefulSet recreation, not an in-place change.** Kubernetes refuses changes to a StatefulSet's `volumeClaimTemplates`, so devnet2 cannot get the state volume through a plain sync; runbook §3.1 deletes the verifier StatefulSet with `--cascade=orphan` immediately before the sync that also moves the verifiers to `ARBITER_TAG` (one roll in total), and §10.1 does the same before a revert. No other volume can host the file: the verifier pod mounts only an emptyDir and the seeds Secret.

### Tasks 16–17: facts for other tasks

- **Live devnet2 facts (read-only, 2026-10-10):** voters and verifiers all run arbiter `v0.10.0` (`ghcr.io/sentioxyz/arbiter@sha256:54dda9db84d14d60b81900e4a61af06fac31f47460ae80ccd2c7af0d7aad5b29`, imageIDs of all six pods); leader `storage-integrity-si-v2-arbiter-1` (election won 2026-10-09 09:24 UTC, term 8); consensus epoch 2 with client lanes 256 and genesis snapshot id `0x63700cf134ce037961d9acf4ab5ff120dde6c6b1d2ba258fa321d581139d1c06` (the stage-5 `show-after.json`); verifier node ids `verifier-1..3` (live StatefulSet init script); voter node ids = pod hostnames; the verifier StatefulSet has no volume claim template (only the `config` emptyDir and the seeds Secret); indexer-a runs sentio-node `ff336f1` / rewriter 0.17.0 / sidecar v0.15.0; indexer-b runs sentio-node `devnet-36b5c0a…@sha256:56cf2d16…` (= `validate_source_binding.LEGACY_SOURCE_DIGEST`, so the validator rejects it as a source image) / rewriter 0.14.0 / sidecar v0.12.0 on `clickhouse-devnet2-node-b`; operator `devnet2-operator-altinity-clickhouse-operator` 0.27.2 in the namespace; no NetworkPolicy or CiliumNetworkPolicy in `sentio-network-devnet2`; CHI pods have containers `clickhouse` and `clickhouse-log`; the redis pod container is `redis`.
- **Empty-set schema root:** `payloadexec.SchemaRoot(net, nil)` = `DigestString("schema-root\x00")` = `0x3aaa143018bc0bfd8f029e9f6ebf84bd059742ebc3750297350a5c38426b7e59` for every network (`pkg/replay/payloadexec/executor.go:713-747`; verified with `sha256`). It is `validate_source_binding.EMPTY_SCHEMA_ROOT` and hard-coded in the sentio-node chart guard.
- **S1-A Task 4 / 11 / 12:** Task 17 relies on `--voter` being accepted with the first `--si-indexer` (Task 11 does), `pubkey=` accepting 64 hex, `enrollment=` being read as a file path inside the container where `arbiter-admin` runs (the runbook copies each JWS to `/tmp/enroll-indexer-<n>.jws` in all three voters), the activation preflight refusing with `would evict [...]` and naming `--evict-unnamed` when registered nodes are unnamed (CONTRACT §3c; the runbook never passes the flag and proves in §5.1 that the list is empty), `consensus show` (protojson, `EmitUnpopulated`, proto names) printing the top-level `genesis_snapshot_id` and `current.si_indexers[].{indexer_id,activation_block,signer,snode_node_id,enrollment_jws}` with uint64 as strings and `current.verifiers[].ed25519_pubkey` as base64, Task 12 Step 7's `$HOME/si-devnet2-multisource-rehearsal/bin/{old,new}/{statedigest,v19check}` (old from `v0.10.0`; statedigest takes no new option and the runbook compares exactly the lanes runbook's §7.2 line set; `v19check CONTAINER` prints `restore_refused version 19: …` / `restore_ok version 19`, which the runbook also runs on a live voter's newest `state.bin`), and `cmd/arbiter-verifier` reading `state_dir` (C8) into `verifier.Config.StateDir`, which writes `<state_dir>/registration.json` as `{"registration_seq":N}`.
- **Task 14:** the runbook expects `kubectl exec <pod> -c sentio-node -- /sentio-node --config-path /config/standalone.yaml storage-integrity enrollment-statement` to print only the compact JWS on stdout (ES256K, payload `purpose` `arbiter-snode-enrollment-v1`), to need `PRIVATE_KEY`, the Ethereum RPC, Redis and the configured `storage_integrity.snode.genesis_snapshot_id` (chart value `storageIntegrity.snode.genesisSnapshotID`, set on both devnet2 SI overlays by the Task 16 PR), and never to contact the arbiter; the warning `storage-integrity SNode registration deferred: this indexer is not enrolled in the arbiter's si_indexers yet; its tables answer ordinary until the enrolment commits` before enrolment and `storage-integrity SNode registered after its enrolment committed` after it; on indexer-a the startup refusal `storage_integrity.snode.genesis_snapshot_id <configured> is not the network's genesis snapshot id <derived> derived from this node's genesis tables` for a wrong value, and on indexer-b `indexer 1 is enrolled with snode_node_id … / signer …` for a mismatched entry. indexer-a's live image ff336f1 decodes its config with yaml.v3 `yaml.Unmarshal` (`config/config.go:359`), so the new key is ignored until Task 17 Step 6. The SNode's `state.json` (in `snode.stateDir`) exposes `registration_seq`.
- **Task 15:** runbook expectations — indexer-a on the stage-1 image before the activation: `"si_indexer_id":0,"self_indexer_id":0,"enrolled":true`; indexer-b before its enrolment: `"si_indexer_id":null,"self_indexer_id":1,"enrolled":false`, every table `ordinary`; after: `"si_indexer_id":1,"enrolled":true`; indexer-a answers `ordinary` for every `devuser1` / `devuser3b` name, existing or not, and indexer-b for `devnet101.swap_new2`; a `devuser1` table created after indexer-b's activation block is `pending` on indexer-b, then `active`; `devuser1.t` stays `ordinary` (Legacy after the seed); a purged name on indexer-b answers `pending`. `SN_COMMIT` / `SN_DIGEST` must exist before Task 16 (indexer-b's pin and `WRITER_AUTHORIZATION_IMAGES`). The sentio-node image must keep `wget` (the runbook's RPC helpers).
- **Ordering (sentio-node drafter, binding):** every SI host image carries `HG_TAG` (via `SN_DIGEST`) before the activation commits (runbook §5.3 checks indexer-a's image; indexer-b becomes an SI host only in §6.4, on the same image); indexer-b's sidecar moves from v0.12.0 to indexer-a's v0.15.0 pin with its SI overlay (both sidecars close the session on a `SOURCE_UNAVAILABLE` refusal, which stays retryable); indexer-b's SI overlay is live before its activation block (§7 refuses otherwise).
- **Task 16 measured results** (on a copy of `origin/main` with stand-in pins): baseline `Ran 65 tests OK`; after Step 3 `Ran 72 tests OK`; after Step 5 `Ran 81 tests OK`; the CI `helm lint --strict` runs and `--set verifier.state.enabled=true` pass; `storage-integrity-si-v2`, the legacy `storage-integrity` release and every sentio-node release of `helmfile.yaml` (indexer-a, indexer-b, observer) and `storage-integrity.helmfile.yaml` (indexer-a) render byte-identically to `origin/main`, and indexer-a's SI release differs only by its `genesis_snapshot_id` line and `checksum/config`; the indexer-b release renders `upstream` = `clickhouse_dsn` = `storage-integrity-si-v2-indexer-b-source:9000`, `snode-2`, `table_ids: []`, the empty-set root, the devnet2 genesis snapshot id, storage RPC 33003, nodeSelector `sea-g-4`; flipping devnet2's `verifier.state.enabled` (Task 17 Step 4) changes the network release only by `state_dir: /state`, the `/state` mount and the `state` volume claim template, and the suite still passes with `DEVNET2_VERIFIER_STATE_ENABLED = True`; the §6.3 converter's output for a MergeTree table plus a view runs in clickhouse-local 26.8.
- **CI:** `.github/workflows/storage-integrity-chart.yml` did not watch `sentio-node-indexer-b*.yaml`; Task 16 adds it to both path lists.
- **Arbiter README (S1-A Task 12)** links `docs/storage-integrity-devnet2-multi-source.md`; the 1800-block preflight is the manual `si_safe_head` check of runbook §7 (no `cast` needed: the safe head is read through voter 0 from `sentio-node-op-reth-0`).
- **Stage 2 later:** the runbook's §11 rules (no downgrade, `snode-3` next, byte-identical restated entries, verifier state) carry over; the snapshot-query lane must stay off while two indexers are enrolled.
