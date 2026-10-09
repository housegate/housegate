# User-run agents writing SI tables — Plan B: client_seq lanes (Phase B) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let several agents share one signing key by giving each agent instance its own `client_seq` lane — a fourth segment of the flat statement id that the arbiter accumulator keys on — activated network-wide by an authority-signed, raise-only consensus parameter that every voter, verifier and SNode must understand first.

**Architecture:** arbiter-proto gains additive fields (lane, lane params, features, lane-budget code, two read RPCs). arbiter-core renders and converts laned ids and carries `ClientLanes` through the canonical consensus update and the table-registry snapshot, while keeping node features out of every Raft command. arbiter keys the accumulator on `account` or `account:lane`, counts lanes per account, refuses lanes until `Params.ClientLanes` is committed, bumps the registry version when it is set or raised so data-plane nodes learn it through the existing follower, writes snapshot v18 only after activation, and gates activation operationally (voter probe + leader feature book) in `arbiter-admin`. housegate parses and forwards laned ids, gates them on the host's `client_lanes_enabled`, and gives the agent a flock-protected lane pool with rotation on budget refusals. sentio-node feeds `client_lanes_enabled` from the registry follower. production bumps images and adds the activation stage to the devnet2 runbook.

**Tech Stack:** Go 1.26, protobuf/buf (arbiter-proto), Bazel 9.1.0 + Bzlmod + gazelle (arbiter-core, arbiter, housegate, sentio-node), hashicorp/raft + raft-boltdb, Prometheus client, Helm + `test_render.py` (production), `kubectl` / `helmfile` (live steps).

**Spec:** `/Users/uranuswch/src/remotesrc/claude/housegate/spec-user-agent-si-writes/docs/superpowers/specs/2026-10-09-user-agent-si-writes-design.md` — binding. This plan implements §8.1 steps B1–B6 (Phase B) and the Phase-B parts of §5, §6.1, §6.5, §6.6, §6.7, §6.8, §6.10, §7, §8.2 step 2–4, §8.3, §9.3, §9.4 steps 5, 6, 10. Phase A (spec steps A1–A3: Plan A1 for housegate, Plan A2 for sentio-node, the production chart and C1) is a hard prerequisite of Tasks 13–20 (see "Plan A contract" below); Tasks 1–12 do not depend on it.

## Global Constraints

- **Statement id forms (D9):** legacy `<account>:<seq>:<nonce>`; laned `<account>:<lane>:<seq>:<nonce>`; `lane` matches `^[0-9a-f]{16}$` exactly (8 random bytes); parsing is by segment count; account lowercase `0x` + hex (any length, as today); seq canonical non-zero decimal; nonce non-empty, no `:`, no surrounding whitespace. Uniqueness key `(account, lane, seq)`.
- **Accumulator subject (D12):** `account` for legacy, `account + ":" + lane` for laned; profile `sentio-spent-ids-v1`, leaf preimage, key hash, node hash, `K = MaxOpenGapRanges = 64` and proof format are unchanged.
- **arbiter-proto field numbers (§5.2, verbatim):** `StatementID.client_lane = 4`; `AdmissionCode.ADMISSION_CODE_LANE_BUDGET_EXCEEDED = 9`; `message ClientLaneParams { uint32 max_lanes_per_account = 1; }`; `ConsensusParamsUpdate.client_lanes = 10`; `ConsensusMutableParams.client_lanes = 5`; `TableRegistrySnapshot.client_lanes = 6`; `ProtocolInfo.features = 4` (repeated string); `NodeRegistration.features = 5` (repeated string, request-only); `ConsensusAdmin.GetNodeFeatures(Empty) → NodeFeatures { repeated NodeFeatureEntry entries = 1; repeated string raft_voter_ids = 2; }`; `NodeFeatureEntry { string node_id = 1; repeated string features = 2; int64 registered_unix = 3; }`.
- **Feature string:** `"client_lanes_v1"`.
- **Go JSON tags (canonical forms, all `omitempty`):** `StatementID.ClientLane` → `client_lane`; `StatementCoord.Lane` → `lane`; `ConsensusParamsUpdate.ClientLanes` / `fsm.Params.ClientLanes` → `client_lanes`; `ClientLaneParams.MaxLanesPerAccount` → `max_lanes_per_account`; FSM `LaneCounts` → `lane_counts`. A nil/empty value must leave every existing digest and snapshot byte-identical.
- **Consensus rule for `ClientLanes` (D13):** absent until an authority-signed `UpdateConsensusParams` sets it; setting requires `TableRegistry != nil` (previously or in the same update) and `max_lanes_per_account ≥ 1`; afterwards every update must carry it; lowering and removal are refused; raising is accepted; every set or raise calls `registryChangedLocked` exactly once. Initial value **256**.
- **Admission (§5.5):** laned id with `Params.ClientLanes == nil` → `MALFORMED` `"client lanes are not enabled"`; lane not 16 lowercase hex → `MALFORMED`; a new laned subject with `LaneCounts[account] >= max_lanes_per_account` → `LANE_BUDGET_EXCEEDED` with state unchanged, checked immediately before `SpentIDs.Insert`; the legacy lane never counts.
- **Snapshot:** container `v18`, written only once `Params.ClientLanes != nil`; until then the writer keeps v14–v17 exactly as today.
- **Strict decoder (§3.2, U3):** nothing lane-related enters a Raft command before activation; `NodeRegistration.features` is never copied into a `RaftCommand` or replicated state; node features live only in a leader-local, non-replicated feature book.
- **Envelope / row id (D10, D11):** `auth.StatementPurposeV2`, `JWSStatementPayloadV2`, `housegate-row-id-v1`, `pkg/auth/testdata/statement_jws_v2.json` and `auth.SharedStatementVectorsSHA256` do not change. New vectors go only into `pkg/auth/testdata/statement_jws_v2_lanes.json` with `auth.SharedStatementLaneVectorsSHA256`.
- **Agent lane store (D15, §6.5):** `<si_dir>/lanes/<lane>.json` = `{"version":1,"lane":"<16 hex>","next":N,"free":[…],"abandoned":false}`, written tmp + fsync + rename + dir fsync; `<si_dir>/lanes/<lane>.lock` held with `flock(LOCK_EX|LOCK_NB)` for the process lifetime; free list capped at 64 (overflow drops the largest); `storage_integrity.agent.max_inflight_per_lane` default 16.
- **Agent messages (verbatim):** `"SDK statement ids must use lane <lane>"` (R9); `"retry: the agent moved to a new client_seq lane"` (GAP_BUDGET rotation). Ingress: `"storage_integrity: client lanes are not enabled on this network"`.
- **Driver sidecar (R8):** stays on legacy ids: the chart renders env `HOUSEGATE_SI_LANES=off` on the sidecar (Plan A2 Task 7; the sidecar image stays housegate v0.15.0, which ignores the env, and an image at `<A1_TAG>` or later honours it — A1 Task 16 maps it onto `storage_integrity.agent.lanes`). No task in Plans A1, A2 or B moves the sidecar image; that is a separate single-service values change (spec §4.5).
- **Metrics (verbatim names):** arbiter `arbiter_admission_rejects_total{code}` (gateway labels also `fence`, `freshness`), `arbiter_client_lanes_total`, `arbiter_accounts_at_lane_cap`, `arbiter_subject_open_gap_ranges`; agent `clickhouse_proxy_agent_si_lane_rotations_total{reason="gap_budget|lost_state|new_process"}`, `clickhouse_proxy_agent_si_inflight`.
- **Never weaken existing determinism/golden tests:** do not edit `arbiter:accumulator/testdata/spent_ids_vectors.json`, `arbiter:fsm/testdata/l3_commitment_golden.json`, `arbiter:fsm/testdata/l3_transition_header_golden.json`, `arbiter:fsm/testdata/statement_jws_v2.json`, `housegate:pkg/auth/testdata/statement_jws_v2.json`, `arbiter-core:conformance/statement_envelope_golden_test.go` expectations, or any existing determinism test's assertions. New goldens are added beside them.
- **Repo commands (ground truth):** arbiter-proto `make tools && make proto && make lint && make breaking && make test`; arbiter-core and arbiter `bazel build //... && bazel test //...` (arbiter-core CI additionally runs `bazel test --build_tests_only --@rules_go//go/config:race //...`), after dependency changes `bazel mod tidy && bazel run //:gazelle`; housegate `bazel test //...` and the dependency recipe `.claude/skills/upgrade-dependency/SKILL.md`; sentio-node `bazel build //... && bazel test //...` with `bazel run @rules_go//go -- mod tidy`; production `python3 -B charts/<chart>/tests/test_render.py`.
- **Pins:** arbiter pins arbiter-core with `bash scripts/update-arbiter-core.sh <tag-or-sha>` and housegate with `bash scripts/update-housegate.sh <tag-or-sha>`; sentio-node pins housegate and arbiter-core three times each (`go.mod` require, `MODULE.bazel` `bazel_dep` version, `git_override` commit — verify the commit with `git -C <dep> rev-parse <tag>^{commit}`). Never predict a tag: record it in the release ledger below when it is cut.
- **Worktrees:** one URWT/Worktrunk worktree per repository at `~/src/remotesrc/claude/<repo>/client-seq-lanes`, branch `urwt/claude/client-seq-lanes`, created with the `bash -c` command from the URWT skill (never `git worktree add`, never `--yes`/`--clobber`). In agent-run commands `$W` is that worktree's absolute path and `$PROTO_TAG`, `$CORE_TAG`, `$ARBITER_TAG`, `$ARBITER_DIGEST`, `$HG_TAG`, `$SN_COMMIT`, `$SN_DIGEST` are the release-ledger values; the agent's Bash tool does not keep shell variables between calls, so either prefix each command with the assignments (`W=/abs/path; …`) or substitute the literal values. In the user-run fish steps of Task 20 they are fish variables set once with `set -g NAME value` from the ledger.
- **Commits:** every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; sentio-node uses conventional commits with a scope; Markdown is never hard-wrapped; code comments are English.
- **Remote and live actions:** creating PRs, merging, cutting releases, pushing to production `main`, `helmfile sync`, deleting pods and every `arbiter-admin consensus update` require explicit user confirmation in chat; commands handed to the user are fish. Steps after the activation step are **irreversible**.
- **production repository rule:** chart and runbook changes (`charts/`, `docs/`) go through a PR; single-service values changes (image digests, one overlay value) go directly to `main` after the PR merges.

### Release ledger (fill in at cut time; later tasks read these names)

| Name | Meaning | Resolved by |
|---|---|---|
| `PROTO_TAG` | arbiter-proto release containing Task 1 | Task 1 Step 9 |
| `CORE_TAG` | arbiter-core release containing Tasks 2–4 | Task 4 Step 6 |
| `ARBITER_TAG` / `ARBITER_DIGEST` | arbiter release containing Tasks 5–12 and its multi-arch image index digest | Task 12 Step 6 |
| `HG_TAG` | housegate release containing Tasks 13–16 | Task 16 Step 12 |
| `SN_COMMIT` / `SN_DIGEST` | sentio-node main commit containing Task 17 and its image index digest | Task 17 Step 6 |

### Plan A contract (prerequisite for Tasks 13–20)

Phase A (Plan A1 housegate, Plan A2 sentio-node + production) must be merged, released and rolled out first: A1's release is `<A1_TAG>` (Plan A1 Task 19 / release step), A2's sentio-node image is `<A2_COMMIT>`/`<A2_DIGEST>` (Plan A2 Task 6), and the devnet2 indexer-a overlay is on stage 4b (Plan A2 Task 12). This plan relies on these deliverables, named exactly as Plans A1 and A2 produce them (`docs/superpowers/plans/2026-10-09-user-agent-si-writes-a1-housegate.md`, `…-a2-sentio-node-production.md`). Each dependent task starts with a check; if a symbol is missing or named differently, **stop** and reconcile against the Plan A document instead of improvising a second implementation.

| ID | Repo | Producer | Deliverable relied on |
|---|---|---|---|
| PA1 | housegate | A1 Task 1 | `pkg/storageintegrity/statementid.go`: `type StatementID struct{ Account, Lane string; Seq uint64; Nonce string }`; `func ParseStatementID(flat string) (StatementID, error)` accepting **both** forms; `func ParseLegacyStatementID(flat string) (StatementID, error)` refusing a laned id with `ErrClientLanesNotEnabled`; `func (StatementID) Flat() string`, `Subject() string`, `IsLaned() bool`; `var ErrClientLanesNotEnabled` (text `storage_integrity: client lanes are not enabled on this network`); `ParseFlatStatementID` kept as a legacy-only wrapper; both duplicate `parseFlatStatementID` copies removed; `ParseLegacyStatementID` is what `EnvelopeFromAdmission` (`intake.go`), `ArbiterStatementEnvelopeToProto` (`arbiter_proto.go`) and the ingress helper `parseIngressStatementID` (`pkg/plugins/storageintegrity/plugin.go`) call |
| PA2 | housegate | A1 Tasks 6, 7 | `chproto.ClientError.SeqUnspent`, `chproto.MarkSeqUnspent(err error) error`, `chproto.IsSeqUnspent`, `chproto.SeqUnspentSuffix` (` [client_seq unspent]`), `chproto.HasSeqUnspentSuffix` / `TrimSeqUnspentSuffix`; the suffix rendered once in `exceptionForPluginError`; a coded terminal reject rendered as `storage_integrity: statement <id> rejected by the arbiter: <ADMISSION_CODE_…>` and marked unspent for every code except `sicore.AdmissionCodeDuplicateClientSeq` |
| PA3 | housegate | A1 Tasks 11, 12, 14; housegate#225 (pre-send release, `QueryContext.UpstreamQueryUnsent`) | `pkg/plugins/sistatement/seq.go` `SeqCounter` (`OpenSeqCounter`, `Reserve() (uint64, error)`, `Release(seq) (overflow bool, err error)`, `ReserveSupplied(seq) error`, `Close()`, `MaxFreeSeqs = 64`); `seq_lock.go` / `seq_lock_unix.go` (`//go:build linux \|\| darwin`) / `seq_lock_other.go` with `lockFile(path) (func() error, error)`, `ErrSeqLocked`, `ErrSeqLockUnsupported`; `seq_dir.go` with `mkdirAllDurable`, `syncDir`, `writeDurable`, `openSeqDir`; reservation in `OnQueryInputCompleteStrict` (`reservedSeq` per session, `releaseSeq`); `(*Plugin).OnException` releasing on the suffix; `SeqObserver{SeqRecycled(); SeqBurned(reason)}`; `Options.OpenSeq func(networkID string) (*SeqCounter, error)` |
| PA4 | housegate | A1 Tasks 13, 14, 16 | `registry.StorageIntegrityInfo` (field `ClientLanesEnabled bool`, JSON `client_lanes_enabled`) and `registry.StorageIntegrityDiscovery` implemented by `*network.RpcNetworkState`; the A1 agent caches the info per target database for the process lifetime and Task 16 adds a 60 s expiry of successful entries (spec §6.4; `sistatement` `discovery.go`: `(*Plugin).siInfo`, map `p.infos` keyed by database, guarded by `p.mu`); `sistatement.Options.Discovery` (nil for YAML / host-injected status sources and the integration fixtures); `StorageIntegrityAgentConfig.Lanes` (`lanes`: `""`/`auto`/`off`, both non-`off` values meaning the legacy lane in A1) with flag `-si-lanes` and env `HOUSEGATE_SI_LANES`; A1's agent uses A2's `sentio_isDatabaseWriter` for its writer pre-check |
| PA5 | sentio-node | A2 Tasks 1, 3, 4 | `sentio_getStorageIntegrityInfo` served from `rpc/storage_integrity_info.go` (`rpc.StorageIntegrityInfo`, `rpc.StorageIntegrityInfoSource`, `rpc.WithStorageIntegrityInfo`) with the runtime source `(*storageIntegrityTableStateRuntime).StorageIntegrityInfo()` in `standalone/storage_integrity_info.go`, fields `enabled`, `network_id`, `keeper_shard_id` (0), `si_indexer_id` (nullable), `self_indexer_id` (nullable), `client_lanes_enabled` (constant `false` in Phase A), `ingress_max_payload_bytes`, `max_token_age_seconds`, `default_read_mode`, `registry_version`, `server_unix_time`; the embedded HouseGate's registry is `RedisNetworkState` (`standalone/standalone.go:159`), which with `FromStatecore` implements `registry.WriterAccess`; sentio-node pins housegate `<A1_TAG>` |
| PA6 | production | A2 Task 7 | `charts/storage-integrity/tests/test_render.py` (which also tests the sentio-node chart; there is no `charts/sentio-node/tests/`) holds `WRITER_AUTHORIZATION_IMAGES` and `WriterAuthorizationTest.test_sidecar_stays_on_legacy_lanes`, which asserts the chart renders env `HOUSEGATE_SI_LANES=off` on the `housegate-sidecar` container from `housegateSidecar.storageIntegrity.lanes: "off"`; `ActualFreshNetworkTest.test_writer_authorization_needs_a_reviewed_image` requires the indexer-a image (`FRESH_SOURCE_IMAGE`) to be listed in `WRITER_AUTHORIZATION_IMAGES` while `writerAuthorization: true`; the runbook has "§6 Stage 4: open storage-integrity writes" and "§7 Operating rules" (A2 Task 8); the sidecar stays housegate v0.15.0 through Phase A |

## Review Focus

1. **An unrelated consensus update after activation** (an authority rotation by an operator, run with `arbiter-admin` without `--client-lanes-max-per-account`) must carry `client_lanes` forward unchanged; the FSM must refuse an update that omits it. Tests: Task 10 Step 1 (`TestUpdateCarriesClientLanesForward`), Task 6 Step 1 (`TestClientLanesMustBeCarried`).
2. **A laned statement reaching a lane-aware leader before activation** (a misconfigured agent, an SDK) must be answered `MALFORMED` at the gateway without proposing a Raft entry, so an old follower never decodes field 4. Test: Task 7 Step 1 (`TestSubmitStatementRefusesLanedIDBeforeActivationWithoutProposing`).
3. **A leader change between the gate's probes and the update, or a node that registered with a previous leader:** `GetNodeFeatures` must report no features for that node and the gate must refuse with its id. Tests: Task 9 Step 1 (`TestFeatureBookClearedOnLeadershipLoss`), Task 10 Step 1 (`TestGateRefusesNodeMissingFromFeatureBook`).
4. **A crash-truncated or corrupt lane file** (empty, invalid JSON, `lane` field naming another lane): the pool must never reuse it, never delete it, and mint a fresh lane. Test: Task 15 Step 1 (`TestAcquireSkipsCorruptLaneFiles`).
5. **SDK-supplied query ids while lanes are on:** a legacy-form own id, an id carrying another lane, an uppercase lane, and a laned id while lanes are off must each be refused with the exact R9 message, never silently re-minted. Test: Task 16 Step 1 (`TestOwnSuppliedStatementIDLaneRule`).

## Task order and dependencies

| # | Repo | Task | Spec step | Needs |
|---|---|---|---|---|
| 1 | arbiter-proto | Additive lane/feature fields, read RPCs, conformance | B1 | — |
| 2 | arbiter-core | `StatementID.ClientLane`, `Flat`, `Coord`, `Subject`, wire conversion, frozen legacy command bytes | B2 | 1 |
| 3 | arbiter-core | `ClientLaneParams`, `ConsensusParamsUpdate.ClientLanes`, normalisation/digest, registry snapshot field | B2 | 2 |
| 4 | arbiter-core | Request-only features, SNode/verifier registration, RC lane linkage; release `CORE_TAG` | B2 | 3 |
| 5 | arbiter | Pin `CORE_TAG`, legacy-history golden, accumulator keyed by subject, lane vectors | B3 | 4 |
| 6 | arbiter | `Params.ClientLanes`, transition rule, registry version bump, views | B3 | 5 |
| 7 | arbiter | Admission: lane shape, `LaneCounts`, `LANE_BUDGET_EXCEEDED`, gateway pre-activation refusal | B3 | 6 |
| 8 | arbiter | Snapshot v18 and `LaneCounts` restore validation | B3 | 7 |
| 9 | arbiter | Feature book, `GetProtocolInfo.features`, `GetNodeFeatures`, post-activation registration refusal | B3 | 8 |
| 10 | arbiter | `arbiter-admin --client-lanes-max-per-account` with the §5.6 gate | B3 | 9 |
| 11 | arbiter | Metrics and `GetClientSeqState` | B3 | 10 |
| 12 | arbiter | README activation section, full verification, release `ARBITER_TAG`, offline replay-check binaries | B3 | 11 |
| 13 | housegate | Laned parser, proto conversion, `LANE_BUDGET_EXCEEDED` outcome, ingress lanes gate | B4 | 1; A1 merged and released (Plan A1 Task 19 Steps 7–8, `<A1_TAG>`) |
| 14 | housegate | Lane JWS vectors + `SharedStatementLaneVectorsSHA256` | B4 | 13 |
| 15 | housegate | Agent lane pool with flock | B4 | 13 |
| 16 | housegate | Agent lane selection, rotation, R9, in-flight cap, metrics; release `HG_TAG` | B4 | 14, 15 |
| 17 | sentio-node | Adapter, `client_lanes_enabled` from the follower, pins | B5 | 4, 16; A2 merged (Plan A2 Task 6) |
| 18 | arbiter | Adopt the housegate lane vectors verbatim | B3 follow-up | 12, 16 |
| 19 | production | Sidecar `HOUSEGATE_SI_LANES=off` render check + activation runbook stage (PR) | B6 | 12, 17; A2 chart/runbook PR merged (Plan A2 Tasks 7–8) |
| 20 | production / devnet2 | Image rollout, offline replay check, voter replacement, activation, E2E | B6, §8.2–§8.3, §9.4 | 19; devnet2 Phase A done (Plan A2 Task 12, stage 4b) |

Tasks 13–16 depend only on Task 1 and Plan A1 and may run in parallel with Tasks 2–12; Task 18 is test-only and does not gate the B6 images. There is no dependency cycle: housegate depends on arbiter-proto only (Task 13 pins `PROTO_TAG`), while arbiter-core and arbiter import housegate's `pkg/replay`, `pkg/lthash` and `pkg/auth` (never `pkg/storageintegrity`), so arbiter-core (Tasks 2–4) and arbiter (Tasks 5–12) build against their existing housegate pins, sentio-node (Task 17) is the first module to combine `CORE_TAG` with `HG_TAG`, and arbiter moves to `HG_TAG` only in the test-only Task 18.

## File Structure

**arbiter-proto** (`/Users/uranuswch/Dev/sentio_xyz/arbiter-proto`)
- Modify `proto/arbiter.proto` — `StatementID.client_lane`, `ADMISSION_CODE_LANE_BUDGET_EXCEEDED`, `NodeRegistration.features`, `SafeState.GetClientSeqState` and its messages.
- Modify `proto/table_registry.proto` — `ClientLaneParams`, `TableRegistrySnapshot.client_lanes` (defined here because `consensus.proto` imports this file, not the reverse).
- Modify `proto/consensus.proto` — `ConsensusMutableParams.client_lanes`, `ConsensusParamsUpdate.client_lanes`, `ProtocolInfo.features`, `NodeFeatures`, `NodeFeatureEntry`, `ConsensusAdmin.GetNodeFeatures`.
- Regenerate `gen/pb/*.pb.go`.
- Create `conformance/client_lanes_test.go`; modify `conformance/consensus_test.go`, `conformance/table_registry_read_test.go`.

**arbiter-core** (`/Users/uranuswch/Dev/sentio_xyz/arbiter-core`)
- Modify `types.go` (+`types_test.go`) — lane on `StatementID`/`StatementCoord`, `Subject`, `ValidClientLane`, `StatementIDStringWithLane`, `AdmissionCodeLaneBudgetExceeded`.
- Modify `consensus.go` (+`consensus_test.go`) — `ClientLaneParams`, `ConsensusParamsUpdate.ClientLanes`, `ClientLanesFeature`, `LocalNodeFeatures`.
- Modify `wire/convert.go`, `wire/consensus.go`, `wire/table_registry_snapshot.go`, `wire/command.go`; create `wire/legacy_command_golden_test.go`, `wire/client_lanes_test.go`, `wire/registration_request_only_test.go`.
- Modify `authority/consensus.go`; create `authority/consensus_golden_test.go`.
- Modify `snode/snode.go`, `verifier/verifier.go`; create `snode/registration_test.go`, `verifier/registration_test.go`.
- Modify `conformance/arbiter_wire_test.go`.
- Modify `go.mod`, `go.sum` (arbiter-proto `PROTO_TAG`), BUILD files via gazelle.

**arbiter** (`/Users/uranuswch/Dev/sentio_xyz/arbiter`)
- Create `fsm/legacy_lane_golden_test.go`, `fsm/testdata/legacy_lane_golden.json` (Task 5, captured before any change).
- Modify `accumulator/spentids.go`, `accumulator/proof.go`; create `accumulator/subjects_test.go`, `accumulator/lane_vectors_test.go` (laned key/leaf/root vectors as frozen literals); modify `accumulator/fuzz_test.go` (new fuzz target only).
- Modify `fsm/state.go`, `fsm/watch.go`, `fsm/params_identity.go`, `fsm/consensus_reads.go`, `fsm/consensus_history.go`, `fsm/consensus_updates.go`, `fsm/table_registry.go`, `fsm/admission.go`, `fsm/snapshot.go`, `fsm/reads_metrics.go`; create `fsm/client_lanes.go`, `fsm/client_lanes_params_test.go`, `fsm/admission_lanes_test.go`, `fsm/snapshot_lanes_test.go`; modify `fsm/admission_test.go` (`signStatementV2` renders `Flat()`).
- Modify `server/server.go`, `server/membership.go`, `server/consensus_admin.go`, `server/ingress.go`, `server/table_registry.go`, `server/safestate.go`; create `server/features.go`, `server/features_test.go`, `server/client_lanes_test.go`, `server/admission_metrics.go`.
- Modify `raftnode/node.go` (`VoterIDs`), `metrics/metrics.go`, `cmd/arbiter/services.go`, `cmd/arbiter-admin/main.go`; create `cmd/arbiter-admin/client_lanes_test.go`, `metrics/client_lanes_test.go`.
- Modify `README.md`.
- Task 18: create `fsm/testdata/statement_jws_v2_lanes.json`, `fsm/shared_lane_vectors_test.go`.

**housegate** (worktree of `/Users/uranuswch/Dev/housegate/housegate`)
- Modify `go.mod`, `go.sum` (arbiter-proto `PROTO_TAG`).
- Modify `pkg/storageintegrity/statementid.go` (+test), `pkg/storageintegrity/arbiter_proto.go` (+`admission_code_test.go`).
- Modify `pkg/plugins/storageintegrity/plugin.go` (+`plugin_test.go`), `proxy.go`, `build.go`.
- Modify `pkg/auth/shared_vectors.go`; create `pkg/auth/statement_v2_lane_vectors_test.go`, `pkg/auth/testdata/statement_jws_v2_lanes.json`, `pkg/storageintegrity/statementid_lane_vectors_test.go`.
- Create `pkg/plugins/sistatement/lanepool.go`, `lanepool_lock_unix.go`, `lanepool_other.go`, `lanepool_test.go`, `lanepool_helper_test.go`, `lanepool_other_test.go`, `lanes.go`, `lanes_test.go`; modify `pkg/plugins/sistatement/plugin.go`, `observer.go`.
- Modify `pkg/config/storage_integrity_config.go` (+test: `MaxInflightPerLane` only; A1 Task 16 ships `Lanes`, `-si-lanes` and `HOUSEGATE_SI_LANES`), `pkg/proxy/observer.go`.
- Modify `pkg/integration/storage_integrity_agent_test.go` (lane integration tests).
- Modify `CLAUDE.md` (statement-id grammar, lanes) and `docs/agent-inline-values.md` only if it documents statement ids.

**sentio-node** (`/Users/uranuswch/Dev/sentio_xyz/sentio-node`)
- Modify `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock`.
- Modify `storageintegrityadapter/adapter.go` (+`adapter_test.go`).
- Modify `storageintegrityadapter/tablestate/model.go`, `arbitercore.go`, `state.go` (+tests).
- Modify `standalone/standalone.go`, `standalone/storage_integrity_table_state.go`, and the PA5 info handler.

**production** (`/Users/uranuswch/Dev/sentio_xyz/production`)
- Modify `docs/storage-integrity-devnet2-dynamic-table-set.md` (new "§7 Stage 5: client_seq lanes"; A2's "§7 Operating rules" becomes §8). The sidecar `HOUSEGATE_SI_LANES=off` render assertion already exists (PA6); Task 19 only verifies it.
- Task 20 (direct to `main`): `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` + `charts/storage-integrity/tests/test_render.py` (`DEVNET2_ARBITER_DIGEST`); `k8s-sea/sentio-network-devnet2/sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, its mirror `docs/examples/storage-integrity-fresh-source-image.yaml`, and `FRESH_SOURCE_IMAGE` plus a new `WRITER_AUTHORIZATION_IMAGES` entry in `charts/storage-integrity/tests/test_render.py`.

---

## Task 1: arbiter-proto — additive lane, feature and read-RPC fields (B1)

**Files:**
- Modify: `proto/arbiter.proto` (`StatementID` at lines 33-38, `AdmissionCode` at 86-106, `NodeRegistration` at 406-415, `service SafeState`)
- Modify: `proto/table_registry.proto` (after `TableRegistryParams`; `TableRegistrySnapshot` at 145-151)
- Modify: `proto/consensus.proto` (`ConsensusMutableParams` 17-26, `ConsensusParamsUpdate` 31-43, `ProtocolInfo` 83-87, `service ConsensusAdmin`)
- Regenerate: `gen/pb/arbiter.pb.go`, `gen/pb/arbiter_grpc.pb.go`, `gen/pb/consensus.pb.go`, `gen/pb/consensus_grpc.pb.go`, `gen/pb/table_registry.pb.go`
- Create: `conformance/client_lanes_test.go`
- Modify: `conformance/consensus_test.go`, `conformance/table_registry_read_test.go`

**Interfaces:**
- Consumes: nothing.
- Produces (Go, package `pb`): `StatementID.ClientLane string`; `AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED`; `ClientLaneParams{MaxLanesPerAccount uint32}`; `ConsensusParamsUpdate.ClientLanes *ClientLaneParams`; `ConsensusMutableParams.ClientLanes *ClientLaneParams`; `TableRegistrySnapshot.ClientLanes *ClientLaneParams`; `ProtocolInfo.Features []string`; `NodeRegistration.Features []string`; `NodeFeatures{Entries []*NodeFeatureEntry; RaftVoterIds []string}`; `NodeFeatureEntry{NodeId string; Features []string; RegisteredUnix int64}`; `ConsensusAdminClient.GetNodeFeatures(ctx, *emptypb.Empty) (*NodeFeatures, error)`; `GetClientSeqStateRequest{ClientAccount, ClientLane string}`; `ClientSeqRange{Start, End uint64}`; `ClientSeqState{Found bool; Subject string; Hi uint64; Ranges []*ClientSeqRange}`; `SafeStateClient.GetClientSeqState`.

- [ ] **Step 1: Create the worktree**

```bash
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
' urwt /Users/uranuswch/Dev/sentio_xyz/arbiter-proto claude client-seq-lanes
```

Expected: one JSON object whose `path` is `~/src/remotesrc/claude/arbiter-proto/client-seq-lanes`. Every later step of this task runs in that path (`W` below). The same command, with only the repository path changed, creates the worktree for every other repository in this plan.

- [ ] **Step 2: Write the failing conformance test**

Create `conformance/client_lanes_test.go`:

```go
package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestClientLanesFieldNumbers pins the client_seq lane additions (housegate
// spec 2026-10-09 §5.2). Each number is a consensus constant: arbiter-core's
// strict Raft command decoder refuses any field it does not know, so a voter
// that disagrees on one of these numbers forks on the first command that
// carries it.
func TestClientLanesFieldNumbers(t *testing.T) {
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		msgType  protoreflect.FullName
	}{
		{&pb.StatementID{}, "client_lane", 4, protoreflect.StringKind, false, ""},
		{&pb.ClientLaneParams{}, "max_lanes_per_account", 1, protoreflect.Uint32Kind, false, ""},
		{&pb.ConsensusParamsUpdate{}, "client_lanes", 10, protoreflect.MessageKind, false, "arbiter.ClientLaneParams"},
		{&pb.ConsensusMutableParams{}, "client_lanes", 5, protoreflect.MessageKind, false, "arbiter.ClientLaneParams"},
		{&pb.TableRegistrySnapshot{}, "client_lanes", 6, protoreflect.MessageKind, false, "arbiter.ClientLaneParams"},
		{&pb.ProtocolInfo{}, "features", 4, protoreflect.StringKind, true, ""},
		{&pb.NodeRegistration{}, "features", 5, protoreflect.StringKind, true, ""},
		{&pb.NodeFeatures{}, "entries", 1, protoreflect.MessageKind, true, "arbiter.NodeFeatureEntry"},
		{&pb.NodeFeatures{}, "raft_voter_ids", 2, protoreflect.StringKind, true, ""},
		{&pb.NodeFeatureEntry{}, "node_id", 1, protoreflect.StringKind, false, ""},
		{&pb.NodeFeatureEntry{}, "features", 2, protoreflect.StringKind, true, ""},
		{&pb.NodeFeatureEntry{}, "registered_unix", 3, protoreflect.Int64Kind, false, ""},
		{&pb.GetClientSeqStateRequest{}, "client_account", 1, protoreflect.StringKind, false, ""},
		{&pb.GetClientSeqStateRequest{}, "client_lane", 2, protoreflect.StringKind, false, ""},
		{&pb.ClientSeqRange{}, "start", 1, protoreflect.Uint64Kind, false, ""},
		{&pb.ClientSeqRange{}, "end", 2, protoreflect.Uint64Kind, false, ""},
		{&pb.ClientSeqState{}, "found", 1, protoreflect.BoolKind, false, ""},
		{&pb.ClientSeqState{}, "subject", 2, protoreflect.StringKind, false, ""},
		{&pb.ClientSeqState{}, "hi", 3, protoreflect.Uint64Kind, false, ""},
		{&pb.ClientSeqState{}, "ranges", 4, protoreflect.MessageKind, true, "arbiter.ClientSeqRange"},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated {
				t.Fatalf("field = %v, want number %d kind %s repeated %v", f, tc.number, tc.kind, tc.repeated)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	for msg, want := range map[proto.Message]int{
		&pb.StatementID{}: 4, &pb.NodeRegistration{}: 5, &pb.ClientLaneParams{}: 1,
		&pb.NodeFeatures{}: 2, &pb.NodeFeatureEntry{}: 3, &pb.ProtocolInfo{}: 4,
		&pb.GetClientSeqStateRequest{}: 2, &pb.ClientSeqRange{}: 2, &pb.ClientSeqState{}: 4,
	} {
		if got := msg.ProtoReflect().Descriptor().Fields().Len(); got != want {
			t.Fatalf("%s field count = %d, want %d", msg.ProtoReflect().Descriptor().Name(), got, want)
		}
	}
}

func TestLaneBudgetAdmissionCodeIsNine(t *testing.T) {
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED); got != 9 {
		t.Fatalf("ADMISSION_CODE_LANE_BUDGET_EXCEEDED = %d, want 9", got)
	}
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_GAP_BUDGET_EXCEEDED); got != 8 {
		t.Fatalf("ADMISSION_CODE_GAP_BUDGET_EXCEEDED moved to %d", got)
	}
	if n := pb.AdmissionCode(0).Descriptor().Values().Len(); n != 10 {
		t.Fatalf("AdmissionCode has %d values, want 10", n)
	}
}

func TestClientLaneRPCSignatures(t *testing.T) {
	for _, tc := range []struct {
		file          protoreflect.FileDescriptor
		service       protoreflect.Name
		method        protoreflect.Name
		input, output protoreflect.FullName
	}{
		{pb.File_consensus_proto, "ConsensusAdmin", "GetNodeFeatures", "google.protobuf.Empty", "arbiter.NodeFeatures"},
		{pb.File_arbiter_proto, "SafeState", "GetClientSeqState", "arbiter.GetClientSeqStateRequest", "arbiter.ClientSeqState"},
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
}
```

- [ ] **Step 3: Run it and watch it fail**

Run: `cd "$W" && go test ./conformance/ -run 'TestClientLanes|TestLaneBudget|TestClientLaneRPC' -v`
Expected: build failure, `undefined: pb.ClientLaneParams` (and the other new types).

- [ ] **Step 4: Edit `proto/arbiter.proto`**

Replace the `StatementID` message with:

```proto
// StatementID is the structured client-assigned statement identity.
// Uniqueness key is (client_account, client_lane, client_seq) — client_nonce
// contributes entropy to _hg_row_id but is NOT part of the uniqueness key.
// Flat string form (replay projection, RCRecord linkage, row-id derivation):
// "<lowercase client_account>:<decimal client_seq>:<client_nonce>" for the
// legacy default lane, "<account>:<client_lane>:<seq>:<nonce>" for a lane.
message StatementID {
  // Lowercase 0x-prefixed Ethereum address of the signing account.
  string client_account = 1;
  uint64 client_seq = 2;
  string client_nonce = 3;
  // Client lane (housegate spec 2026-10-09 D9): exactly 16 lowercase hex
  // characters, or empty for the legacy default lane. Admitted only once the
  // consensus parameter client_lanes is set; before that a laned statement is
  // ADMISSION_CODE_MALFORMED.
  string client_lane = 4;
}
```

Append to `enum AdmissionCode`, after `ADMISSION_CODE_GAP_BUDGET_EXCEEDED = 8;`:

```proto
  // A statement would open a new client lane for an account that already
  // holds client_lanes.max_lanes_per_account lanes. The coordinate is unspent;
  // the client continues on a lane it already holds or on the legacy lane.
  ADMISSION_CODE_LANE_BUDGET_EXCEEDED = 9;
```

Append to `message NodeRegistration`, after `dial_addr = 4`:

```proto
  // Capability strings of the registering binary ("client_lanes_v1").
  // Request-only: the leader records them in a non-replicated feature book and
  // never copies them into a RaftCommand or replicated state (housegate spec
  // 2026-10-09 §5.6), because a voter that does not know this field refuses
  // every command that carries it.
  repeated string features = 5;
```

Add before `service SafeState`:

```proto
// GetClientSeqStateRequest names one accumulator subject: the account alone
// for the legacy lane, account and lane for a client lane.
message GetClientSeqStateRequest {
  string client_account = 1;
  string client_lane = 2;
}

// ClientSeqRange is a closed range [start, end] of unspent client_seq values.
message ClientSeqRange {
  uint64 start = 1;
  uint64 end = 2;
}

// ClientSeqState is the serving voter's applied spent-id state of one subject:
// spent = [1, hi] minus ranges. A local read, not a consensus read.
message ClientSeqState {
  bool found = 1;
  string subject = 2;
  uint64 hi = 3;
  repeated ClientSeqRange ranges = 4;
}
```

and add to `service SafeState`, after `GetQueryPolicy`:

```proto
  // Local read of the serving voter's applied accumulator state for one
  // subject (operators and recovery tooling); no leader barrier.
  rpc GetClientSeqState (GetClientSeqStateRequest) returns (ClientSeqState) {}
```

- [ ] **Step 5: Edit `proto/table_registry.proto` and `proto/consensus.proto`**

In `table_registry.proto`, after `message TableRegistryParams { … }`:

```proto
// ClientLaneParams enables client_seq lanes (housegate spec 2026-10-09 D13).
// Absent until an authority-signed consensus update sets it; afterwards every
// update carries it, max_lanes_per_account only rises, and it is never removed.
// Defined here because consensus.proto imports this file.
message ClientLaneParams {
  uint32 max_lanes_per_account = 1;
}
```

and append to `message TableRegistrySnapshot`, after `incarnations = 5;`:

```proto
  // The committed client-lane parameter, so data-plane nodes learn activation
  // through the registry follower. Setting or raising it bumps version.
  ClientLaneParams client_lanes = 6;
```

In `consensus.proto` append `ClientLaneParams client_lanes = 5;` to `ConsensusMutableParams` (comment: `// Set once by a signed update, then carried by every update; raise-only.`) and `ClientLaneParams client_lanes = 10;` to `ConsensusParamsUpdate` (comment: `// Absent keeps client lanes disabled. Once set it must be resent; lowering or removal is refused.`). Append to `ProtocolInfo`:

```proto
  // Capability strings of this binary ("client_lanes_v1"); a static local
  // property, read by arbiter-admin's activation gate on every voter.
  repeated string features = 4;
```

Add after `ConsensusParamsState`:

```proto
// NodeFeatureEntry joins one non-evicted verifier or SNode registration with
// the features its latest RegisterNode on the current leader advertised.
// features is empty when this leader has not seen it register.
message NodeFeatureEntry {
  string node_id = 1;
  repeated string features = 2;
  int64 registered_unix = 3;
}

// NodeFeatures is the leader's non-replicated feature book. raft_voter_ids is
// the current Raft configuration's voter ids, so the activation gate can prove
// it probed every voter.
message NodeFeatures {
  repeated NodeFeatureEntry entries = 1;
  repeated string raft_voter_ids = 2;
}
```

and add to `service ConsensusAdmin`:

```proto
  // Leader-only read with a barrier: one entry per non-evicted verifier and
  // SNode registration joined with the leader's non-replicated feature book.
  rpc GetNodeFeatures (google.protobuf.Empty) returns (NodeFeatures) {}
```

- [ ] **Step 6: Regenerate and update the existing conformance pins**

Run: `cd "$W" && make tools && make proto`

In `conformance/consensus_test.go`:
- `ConsensusParamsUpdate` names/kinds: append `"client_lanes"` / `protoreflect.MessageKind`.
- `ConsensusMutableParams`: append `"client_lanes"` / `protoreflect.MessageKind`.
- `ProtocolInfo`: append `"features"` / `protoreflect.StringKind`.
- Replace the repeated check `if f.IsList() != (name == "authority_addresses")` with `if f.IsList() != (name == "authority_addresses" || name == "features")`.
- `TestConsensusAdminRPCSignatures`: `service.Methods().Len() != 7` and add `{"GetNodeFeatures", "google.protobuf.Empty", "arbiter.NodeFeatures"}`.
- In the loop that pins `table_registry`'s message type, add the same assertion for `client_lanes` → `arbiter.ClientLaneParams`.

In `conformance/table_registry_read_test.go` append `{"client_lanes", 6, protoreflect.MessageKind, false}` to the `TableRegistrySnapshot` row.

- [ ] **Step 7: Run every gate**

Run: `cd "$W" && make lint && make breaking && make test && git diff --exit-code -- gen/ || true`
Expected: lint and breaking pass (all additions); `make test` PASS including the three new tests and the edited ones. (`git diff -- gen/` shows the regenerated code, which is committed.)

- [ ] **Step 8: Commit**

```bash
cd "$W" && git add proto gen conformance && git commit -m "feat(proto): client_seq lanes, node features and lane-budget admission code

StatementID.client_lane (4), ADMISSION_CODE_LANE_BUDGET_EXCEEDED (9),
ClientLaneParams on ConsensusParamsUpdate (10), ConsensusMutableParams (5)
and TableRegistrySnapshot (6), request-only NodeRegistration.features (5),
ProtocolInfo.features (4), ConsensusAdmin.GetNodeFeatures and
SafeState.GetClientSeqState (housegate spec 2026-10-09 §5.2).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 9: PR, merge and release (requires explicit user confirmation)**

Ask the user before each remote action. After approval: `git push -u origin urwt/claude/client-seq-lanes`, `gh pr create --repo sentioxyz/arbiter-proto --fill`, wait for CI and review, merge, then run the cut-release workflow: `gh workflow run cut-release.yml --repo sentioxyz/arbiter-proto --ref main`. Record the produced tag as `PROTO_TAG` in the release ledger (verify by content: `git show "$PROTO_TAG":proto/arbiter.proto | grep -c 'client_lane = 4'` prints `1`).

---

## Task 2: arbiter-core — laned `StatementID`, `Coord`, `Subject`, wire conversion (B2)

**Files:**
- Create: `wire/legacy_command_golden_test.go` (captured on unchanged code)
- Modify: `go.mod`, `go.sum` (arbiter-proto `PROTO_TAG`), `MODULE.bazel.lock` if `bazel mod tidy` changes it
- Modify: `types.go:14-35` (`StatementCoord`, `StatementIDString`), `types.go:85-124` (`AdmissionCode`, `StatementID`, `Flat`, `Coord`)
- Modify: `types_test.go`
- Modify: `wire/convert.go:24-30` (`statementIDFromPB`, `statementIDToPB`)
- Create: `wire/client_lanes_test.go`
- Modify: `conformance/arbiter_wire_test.go`

**Interfaces:**
- Consumes: Task 1 `pb.StatementID.ClientLane`, `pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED`.
- Produces (package `arbiter`):
  - `const ClientLaneHexLen = 16`
  - `func ValidClientLane(lane string) bool`
  - `func StatementIDStringWithLane(clientAccount, clientLane string, clientSeq uint64, clientNonce string) string`
  - `StatementID.ClientLane string` (`json:"client_lane,omitempty"`); `(StatementID).Flat()` renders four segments when the lane is set
  - `StatementCoord.Lane string` (`json:"lane,omitempty"`); `(StatementID).Coord()` carries the lane; `func (c StatementCoord) Subject() string`
  - `const AdmissionCodeLaneBudgetExceeded AdmissionCode = 9`

- [ ] **Step 1: Create the worktree** (URWT command from Task 1 Step 1 with `/Users/uranuswch/Dev/sentio_xyz/arbiter-core`; `W` is its `path`).

- [ ] **Step 2: Freeze the legacy command bytes on unchanged code**

Create `wire/legacy_command_golden_test.go`:

```go
package wire

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func legacyGoldenEnvelope() arbiter.StatementEnvelope {
	return arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: "0x00000000000000000000000000000000000000a1", ClientSeq: 42, ClientNonce: "9f1c"},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             "INSERT INTO db.t FORMAT Native",
		SQLHash:         "0x" + string(bytes.Repeat([]byte("11"), 32)),
		SettingsHash:    "0x" + string(bytes.Repeat([]byte("22"), 32)),
		PayloadRef:      "ref-1",
		PayloadHash:     "0x" + string(bytes.Repeat([]byte("44"), 32)),
		PayloadLength:   3,
		TargetTableID:   "db.t",
		UserJWS:         "h.p.s",
		EnvelopeVersion: 2,
		NetworkID:       "net",
		PayloadFormat:   "clickhouse-native-data-v1",
		ClientRevision:  54460,
		SchemaHash:      "0x" + string(bytes.Repeat([]byte("33"), 32)),
		RowIDProfileID:  "housegate-row-id-v1",
	}
}

func legacyGoldenUpdate() arbiter.ConsensusParamsUpdate {
	return arbiter.ConsensusParamsUpdate{
		NetworkID: "net", GenesisSnapshotID: "0xgenesis", ExpectedEpoch: 1, PreviousParamsDigest: "0xdigest",
		AuthorityAddresses: []string{"0x00000000000000000000000000000000000000b2"}, MaxWriters: 1, ExpectedPromotionSeq: 7,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1",
			SIIndexerID: 0, ActivationBlock: 100, Confirmation: arbiter.TableRegistryConfirmationSafe},
	}
}

// TestLegacyCommandBytesAreFrozen pins the exact RaftCommand bytes this module
// encodes for the command kinds that exist before client lanes are activated
// (housegate spec 2026-10-09 §9.3 "strict decoder"). Every hex value was
// captured from the release before client lanes. The previous release's
// wire.Decode accepts exactly these bytes; an encoder that emitted even an
// empty new field would make every not-yet-upgraded voter reject the entry.
// Never regenerate these values: a diff here is a consensus break.
func TestLegacyCommandBytesAreFrozen(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  Command
		want string
	}{
		{"submit_statement_legacy_id", Command{SubmitStatement: &SubmitStatement{Envelope: legacyGoldenEnvelope()}}, ""},
		{"register_rc_legacy_id", Command{RegisterRC: &RegisterRC{RC: arbiter.RCRecord{StatementID: legacyGoldenEnvelope().StatementID, SourceNode: "s1", SourceClaimRoot: "0xroot"}}}, ""},
		{"register_verifier", Command{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: bytes.Repeat([]byte{7}, 32), DialAddr: "v1:7080"}}}, ""},
		{"register_snode", Command{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}}, ""},
		{"update_consensus_params_with_registry", Command{UpdateConsensusParams: &UpdateConsensusParams{Update: legacyGoldenUpdate(), AuthorityJWS: "h.p.s"}}, ""},
	} {
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

Run: `cd "$W" && go test ./wire/ -run TestLegacyCommandBytesAreFrozen -v`
Expected: five FAILs, each printing `got <hex>`. Paste each printed hex into its `want` field (this run is on unchanged `origin/main` code, which is what makes the value "the previous release's bytes"), rerun, expect PASS, then commit:

```bash
cd "$W" && git add wire/legacy_command_golden_test.go && git commit -m "test(wire): freeze pre-lane RaftCommand bytes

Captured on the release before client_seq lanes so any later field that
leaks into a pre-activation command fails here instead of forking an
un-upgraded voter (housegate spec 2026-10-09 §9.3).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 3: Pin arbiter-proto `PROTO_TAG`**

Run:
```bash
cd "$W" && go get "github.com/sentioxyz/arbiter-proto@$PROTO_TAG" && go mod tidy && bazel mod tidy && bazel run //:gazelle && bazel build //... && go test ./wire/ -run TestLegacyCommandBytesAreFrozen
```
Expected: build succeeds; the golden still PASSES (new optional fields are absent from legacy commands). Commit `chore(deps): pin arbiter-proto $PROTO_TAG` (with the trailer).

- [ ] **Step 4: Write the failing lane tests**

Append to `types_test.go`:

```go
func TestStatementIDLaneRendering(t *testing.T) {
	legacy := StatementID{ClientAccount: "0xAbC", ClientSeq: 42, ClientNonce: "9f1c"}
	if got := legacy.Flat(); got != "0xabc:42:9f1c" {
		t.Fatalf("legacy Flat = %q", got)
	}
	laned := legacy
	laned.ClientLane = "5e1f0a2b7c9d3e4f"
	if got := laned.Flat(); got != "0xabc:5e1f0a2b7c9d3e4f:42:9f1c" {
		t.Fatalf("laned Flat = %q", got)
	}
	if c := legacy.Coord(); c != (StatementCoord{Account: "0xabc", ClientSeq: 42}) || c.Subject() != "0xabc" {
		t.Fatalf("legacy coord %+v subject %q", c, c.Subject())
	}
	if c := laned.Coord(); c != (StatementCoord{Account: "0xabc", Lane: "5e1f0a2b7c9d3e4f", ClientSeq: 42}) || c.Subject() != "0xabc:5e1f0a2b7c9d3e4f" {
		t.Fatalf("laned coord %+v subject %q", c, c.Subject())
	}
	if got := StatementIDStringWithLane("0xABC", "", 1, "n"); got != StatementIDString("0xABC", 1, "n") {
		t.Fatalf("empty lane must render the legacy form, got %q", got)
	}
}

func TestStatementIDJSONOmitsEmptyLane(t *testing.T) {
	b, err := json.Marshal(StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"})
	if err != nil || string(b) != `{"client_account":"0xabc","client_seq":1,"client_nonce":"n"}` {
		t.Fatalf("legacy JSON = %s (%v): an empty lane must not change any canonical preimage", b, err)
	}
	b, _ = json.Marshal(StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n", ClientLane: "00000000000000ff"})
	if string(b) != `{"client_account":"0xabc","client_seq":1,"client_nonce":"n","client_lane":"00000000000000ff"}` {
		t.Fatalf("laned JSON = %s", b)
	}
	b, _ = json.Marshal(StatementCoord{Account: "0xabc", ClientSeq: 1})
	if string(b) != `{"account":"0xabc","client_seq":1}` {
		t.Fatalf("legacy coord JSON = %s", b)
	}
}

func TestValidClientLane(t *testing.T) {
	for lane, want := range map[string]bool{
		"5e1f0a2b7c9d3e4f": true, "0000000000000000": true, "ffffffffffffffff": true,
		"": false, "5e1f0a2b7c9d3e4": false, "5e1f0a2b7c9d3e4f0": false,
		"5E1F0A2B7C9D3E4F": false, "5e1f0a2b7c9d3e4g": false, "5e1f0a2b:c9d3e4f": false, " 5e1f0a2b7c9d3e4": false,
	} {
		if got := ValidClientLane(lane); got != want {
			t.Errorf("ValidClientLane(%q) = %v, want %v", lane, got, want)
		}
	}
}
```

(add `"encoding/json"` to the imports of `types_test.go`).

Create `wire/client_lanes_test.go`:

```go
package wire

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func TestStatementIDLaneSurvivesEveryConverter(t *testing.T) {
	env := legacyGoldenEnvelope()
	env.StatementID.ClientLane = "5e1f0a2b7c9d3e4f"
	if got := EnvelopeFromPB(EnvelopeToPB(env)).StatementID; got != env.StatementID {
		t.Fatalf("envelope round trip = %+v", got)
	}
	rc := arbiter.RCRecord{StatementID: env.StatementID, SourceNode: "s1"}
	if got := RCFromPB(RCToPB(rc)).StatementID; got != env.StatementID || got.Flat() != "0x00000000000000000000000000000000000000a1:5e1f0a2b7c9d3e4f:42:9f1c" {
		t.Fatalf("RC round trip = %+v (%s): the SNode's claim must name the same laned flat id the FSM indexed", got, got.Flat())
	}
	b, err := Encode(Command{SubmitStatement: &SubmitStatement{Envelope: env}})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := Decode(b)
	if err != nil || cmd.SubmitStatement.Envelope.StatementID != env.StatementID {
		t.Fatalf("command round trip = %+v, %v", cmd.SubmitStatement, err)
	}
}

// TestDispatchedStatementIDIsOpaque pins the fact behind housegate spec
// 2026-10-09 §7 (activation gate rationale): a verifier receives each
// statement's flat id as an opaque string that the leader rendered with
// StatementID.Flat() (arbiter fsm/reads_dispatch.go), and RowID hashes those
// bytes. A verifier's row ids therefore follow the leader's rendering, not the
// verifier's own StatementID type; the lane-stripped rendering a lane-unaware
// leader would produce yields different row ids, which gate (1) of §5.6
// (every voter lane-aware) prevents.
func TestDispatchedStatementIDIsOpaque(t *testing.T) {
	laned := arbiter.StatementID{ClientAccount: "0x00000000000000000000000000000000000000a1", ClientLane: "5e1f0a2b7c9d3e4f", ClientSeq: 42, ClientNonce: "9f1c"}
	job := replay.ReplayJob{BlockSeq: 7, Statements: []replay.Statement{{StatementID: laned.Flat(), StatementSeq: 1}}}
	got := ReplayJobFromPB(ReplayJobToPB(job))
	if len(got.Statements) != 1 || got.Statements[0].StatementID != "0x00000000000000000000000000000000000000a1:5e1f0a2b7c9d3e4f:42:9f1c" {
		t.Fatalf("dispatched statements = %+v: the flat id must cross the wire byte for byte", got.Statements)
	}
	stripped := laned
	stripped.ClientLane = ""
	if bytes.Equal(payloadexec.RowID("net", "db.t", got.Statements[0].StatementID, 0), payloadexec.RowID("net", "db.t", stripped.Flat(), 0)) {
		t.Fatal("a lane-stripped flat id must derive different row ids")
	}
}
```

Add `"bytes"`, `"github.com/housegate/housegate/pkg/replay"` and `"github.com/housegate/housegate/pkg/replay/payloadexec"` to the imports of `wire/client_lanes_test.go` (both housegate packages are already arbiter-core dependencies: `wire/dispatch.go`, `snode/converge.go`). `TestDispatchedStatementIDIsOpaque` documents the dispatch contract rather than driving code: once `ClientLane` compiles it passes without further change.

Run: `cd "$W" && go test . ./wire/ -run 'Lane|ValidClientLane|TestDispatchedStatementIDIsOpaque' -v`
Expected: build failure (`unknown field ClientLane`, `undefined: ValidClientLane`).

- [ ] **Step 5: Implement in `types.go`**

Replace `StatementCoord` and add the helpers next to `StatementIDString`:

```go
// StatementCoord is the statement_id uniqueness coordinate: one statement per
// (account, lane, client_seq); client_nonce is NOT part of the key (§6.1).
// Lane is empty for the legacy default lane.
type StatementCoord struct {
	Account   string `json:"account"`
	Lane      string `json:"lane,omitempty"`
	ClientSeq uint64 `json:"client_seq"`
}

// Subject is the accumulator key of the coordinate (housegate spec 2026-10-09
// D12): the account for the legacy lane, "<account>:<lane>" for a client lane.
// A legacy account is 0x + hex and never contains ':', so the two forms never
// collide.
func (c StatementCoord) Subject() string {
	if c.Lane == "" {
		return c.Account
	}
	return c.Account + ":" + c.Lane
}

// ClientLaneHexLen is the length of a client lane id: 8 random bytes in hex.
const ClientLaneHexLen = 16

// ValidClientLane reports whether lane is exactly 16 lowercase hex characters.
func ValidClientLane(lane string) bool {
	if len(lane) != ClientLaneHexLen {
		return false
	}
	for i := 0; i < len(lane); i++ {
		c := lane[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// StatementIDStringWithLane renders the canonical flat statement_id. An empty
// lane renders the legacy three-segment form byte-identically, so every
// historical _hg_row_id, partition root and state root is unchanged.
func StatementIDStringWithLane(clientAccount, clientLane string, clientSeq uint64, clientNonce string) string {
	if clientLane == "" {
		return StatementIDString(clientAccount, clientSeq, clientNonce)
	}
	return strings.ToLower(clientAccount) + ":" + clientLane + ":" + strconv.FormatUint(clientSeq, 10) + ":" + clientNonce
}
```

Add `AdmissionCodeLaneBudgetExceeded AdmissionCode = 9` after `AdmissionCodeGapBudgetExceeded` with the comment `// AdmissionCodeLaneBudgetExceeded: a new client lane above client_lanes.max_lanes_per_account (arbiter-proto client-lane append).`. Replace `StatementID`, `Flat` and `Coord`:

```go
// StatementID is the structured client-assigned statement identity
// (uniqueness key = (client_account, client_lane, client_seq); nonce is
// entropy, §6.1). ClientLane is empty for the legacy default lane.
type StatementID struct {
	ClientAccount string `json:"client_account"`
	ClientSeq     uint64 `json:"client_seq"`
	ClientNonce   string `json:"client_nonce"`
	ClientLane    string `json:"client_lane,omitempty"`
}

// Flat renders the canonical flat statement_id string form.
func (id StatementID) Flat() string {
	return StatementIDStringWithLane(id.ClientAccount, id.ClientLane, id.ClientSeq, id.ClientNonce)
}

// Coord is the accumulator uniqueness coordinate (account normalized).
func (id StatementID) Coord() StatementCoord {
	return StatementCoord{Account: strings.ToLower(id.ClientAccount), Lane: id.ClientLane, ClientSeq: id.ClientSeq}
}
```

In `wire/convert.go`:

```go
func statementIDFromPB(m *pb.StatementID) arbiter.StatementID {
	return arbiter.StatementID{ClientAccount: m.GetClientAccount(), ClientSeq: m.GetClientSeq(), ClientNonce: m.GetClientNonce(), ClientLane: m.GetClientLane()}
}

func statementIDToPB(v arbiter.StatementID) *pb.StatementID {
	return &pb.StatementID{ClientAccount: v.ClientAccount, ClientSeq: v.ClientSeq, ClientNonce: v.ClientNonce, ClientLane: v.ClientLane}
}
```

In `conformance/arbiter_wire_test.go` add `int32(arbiter.AdmissionCodeLaneBudgetExceeded) != int32(pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED) ||` to the `AdmissionCode` chain. Leave `assertMirror(t, arbiter.NodeRegistration{}, …)` for Task 4.

- [ ] **Step 6: Run the package tests**

Run: `cd "$W" && go test . ./wire/ ./conformance/ ./snode/ ./verifier/ 2>&1 | tail -20`
Expected: the new tests PASS; `TestLegacyCommandBytesAreFrozen` PASSES; `conformance` FAILS only in `TestArbiterMirrorsMatchProto` on `NodeRegistration: proto field "features" has no Go mirror json tag` (fixed in Task 4) — every other test PASSES. If `conformance/statement_envelope_golden_test.go` fails, stop: a legacy digest moved.

- [ ] **Step 7: Commit**

```bash
cd "$W" && git add types.go types_test.go wire conformance && git commit -m "feat(types): client lane on StatementID, coordinate subject

Flat renders <account>:<lane>:<seq>:<nonce> when a lane is set and the
legacy form byte-identically otherwise; Coord carries the lane and
Subject keys the accumulator (housegate spec 2026-10-09 D9, D12).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 3: arbiter-core — `ClientLaneParams` in the consensus update and the registry snapshot (B2)

**Files:**
- Create: `authority/consensus_golden_test.go` (captured before the change)
- Modify: `consensus.go` (`ConsensusParamsUpdate`, lines 15-37)
- Modify: `wire/consensus.go:12-60`
- Modify: `authority/consensus.go:53-59` (normalisation)
- Modify: `wire/table_registry_snapshot.go:106-112`, `TableRegistrySnapshotFromPB` (~148), `TableRegistrySnapshotToPB`
- Modify: `conformance/arbiter_wire_test.go`
- Test: `consensus_test.go` (create if absent), `wire/client_lanes_test.go`, `authority/consensus_golden_test.go`

**Interfaces:**
- Consumes: Task 1 `pb.ClientLaneParams`, `pb.ConsensusParamsUpdate.ClientLanes`, `pb.TableRegistrySnapshot.ClientLanes`.
- Produces: `type ClientLaneParams struct{ MaxLanesPerAccount uint32 \`json:"max_lanes_per_account"\` }`; `func (ClientLaneParams) Validate() error`; `ConsensusParamsUpdate.ClientLanes *ClientLaneParams \`json:"client_lanes,omitempty"\``; `wire.ClientLaneParamsFromPB(*pb.ClientLaneParams) *arbiter.ClientLaneParams`; `wire.ClientLaneParamsToPB(*arbiter.ClientLaneParams) *pb.ClientLaneParams`; `wire.TableRegistrySnapshot.ClientLanes *arbiter.ClientLaneParams`.

- [ ] **Step 1: Freeze a signed update's hash on unchanged code**

Create `authority/consensus_golden_test.go`:

```go
package authority

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestConsensusParamsUpdateHashIsFrozen pins the canonical hash of an update
// signed before client lanes existed. A previously signed update must keep its
// digest forever: the FSM re-verifies every historical transition on restore.
// Never regenerate this value.
func TestConsensusParamsUpdateHashIsFrozen(t *testing.T) {
	update := arbiter.ConsensusParamsUpdate{
		NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", ExpectedEpoch: 1, PreviousParamsDigest: "0xdigest",
		AuthorityAddresses: []string{"0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705"}, MaxWriters: 1, ExpectedPromotionSeq: 3,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000D1",
			SIIndexerID: 0, ActivationBlock: 5508931, Confirmation: arbiter.TableRegistryConfirmationSafe},
	}
	got, err := ConsensusParamsUpdateHash(update)
	if err != nil {
		t.Fatal(err)
	}
	const want = ""
	if got != want {
		t.Fatalf("ConsensusParamsUpdateHash = %s, want %s", got, want)
	}
}
```

Run `cd "$W" && go test ./authority/ -run TestConsensusParamsUpdateHashIsFrozen -v` on the code as Task 2 left it (no `ClientLanes` field yet); it FAILS printing the hash; paste it into `want`, rerun (PASS), commit `test(authority): freeze a pre-lane consensus update hash` with the trailer.

- [ ] **Step 2: Write the failing tests**

Create `consensus_test.go` (package `arbiter`) — or append if it exists:

```go
package arbiter

import (
	"encoding/json"
	"testing"
)

func TestClientLaneParamsValidate(t *testing.T) {
	if err := (ClientLaneParams{}).Validate(); err == nil {
		t.Fatal("max_lanes_per_account 0 must be refused: it would admit no lane")
	}
	if err := (ClientLaneParams{MaxLanesPerAccount: 256}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusParamsUpdateOmitsAbsentClientLanes(t *testing.T) {
	b, _ := json.Marshal(ConsensusParamsUpdate{NetworkID: "n"})
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if _, ok := m["client_lanes"]; ok {
		t.Fatalf("absent client_lanes must be omitted from the canonical form: %s", b)
	}
	b, _ = json.Marshal(ConsensusParamsUpdate{NetworkID: "n", ClientLanes: &ClientLaneParams{MaxLanesPerAccount: 256}})
	_ = json.Unmarshal(b, &m)
	if got, ok := m["client_lanes"].(map[string]any); !ok || got["max_lanes_per_account"] != float64(256) {
		t.Fatalf("client_lanes JSON = %s", b)
	}
}

func TestLocalNodeFeatures(t *testing.T) {
	got := LocalNodeFeatures()
	if len(got) != 1 || got[0] != "client_lanes_v1" || ClientLanesFeature != "client_lanes_v1" {
		t.Fatalf("LocalNodeFeatures = %v", got)
	}
	got[0] = "mutated"
	if LocalNodeFeatures()[0] != ClientLanesFeature {
		t.Fatal("LocalNodeFeatures must return a fresh slice")
	}
}
```

Append to `wire/client_lanes_test.go`:

```go
func TestClientLaneParamsWire(t *testing.T) {
	if ClientLaneParamsFromPB(nil) != nil || ClientLaneParamsToPB(nil) != nil {
		t.Fatal("absent params must stay absent")
	}
	u := legacyGoldenUpdate()
	u.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	got := ConsensusParamsUpdateFromPB(ConsensusParamsUpdateToPB(u))
	if got.ClientLanes == nil || *got.ClientLanes != *u.ClientLanes || got.ClientLanes == u.ClientLanes {
		t.Fatalf("update round trip = %+v", got.ClientLanes)
	}
}

func TestTableRegistrySnapshotCarriesClientLanes(t *testing.T) {
	s := TableRegistrySnapshot{Params: arbiter.TableRegistryParams{ChainID: 1, DatabasesContract: "0x00000000000000000000000000000000000000d1", ActivationBlock: 1, Confirmation: arbiter.TableRegistryConfirmationSafe}, Version: 3}
	got, err := TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || got.ClientLanes != nil {
		t.Fatalf("disabled lanes: %+v, %v", got.ClientLanes, err)
	}
	s.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	got, err = TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || got.ClientLanes == nil || got.ClientLanes.MaxLanesPerAccount != 256 {
		t.Fatalf("enabled lanes: %+v, %v", got.ClientLanes, err)
	}
	m := TableRegistrySnapshotToPB(s)
	m.ClientLanes.MaxLanesPerAccount = 0
	if _, err := TableRegistrySnapshotFromPB(m); err == nil {
		t.Fatal("a snapshot carrying client_lanes with max_lanes_per_account 0 must be refused")
	}
}
```

Append to `authority/consensus_golden_test.go`:

```go
func TestNormalizeConsensusParamsUpdateClientLanes(t *testing.T) {
	base := arbiter.ConsensusParamsUpdate{NetworkID: "n", GenesisSnapshotID: "g", PreviousParamsDigest: "d",
		AuthorityAddresses: []string{"0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705"}, MaxWriters: 1}
	withLanes := base
	withLanes.ClientLanes = &arbiter.ClientLaneParams{}
	if _, err := NormalizeConsensusParamsUpdate(withLanes); err == nil {
		t.Fatal("max_lanes_per_account 0 must be refused")
	}
	params := &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	withLanes.ClientLanes = params
	got, err := NormalizeConsensusParamsUpdate(withLanes)
	if err != nil || got.ClientLanes == params || *got.ClientLanes != *params {
		t.Fatalf("normalised lanes %+v (%v): want an equal, independent copy", got.ClientLanes, err)
	}
	a, _ := ConsensusParamsUpdateHash(base)
	b, _ := ConsensusParamsUpdateHash(withLanes)
	if a == b {
		t.Fatal("client_lanes must be bound by the update hash")
	}
}
```

Run: `cd "$W" && go test . ./wire/ ./authority/ -run 'ClientLane|LocalNodeFeatures|OmitsAbsent|NormalizeConsensusParamsUpdateClientLanes' -v`
Expected: build failure (`undefined: ClientLaneParams`).

- [ ] **Step 3: Implement**

In `consensus.go` add (with `import "fmt"`):

```go
// ClientLanesFeature is the capability string a lane-aware binary advertises
// in ProtocolInfo.features and NodeRegistration.features (housegate spec
// 2026-10-09 §5.6).
const ClientLanesFeature = "client_lanes_v1"

// LocalNodeFeatures returns this binary's capability strings in a fresh slice.
func LocalNodeFeatures() []string { return []string{ClientLanesFeature} }

// ClientLaneParams enables client_seq lanes (housegate spec 2026-10-09 D13).
// Absent from every update until an authority sets it; afterwards every
// update carries it, MaxLanesPerAccount only rises, and it is never removed.
type ClientLaneParams struct {
	MaxLanesPerAccount uint32 `json:"max_lanes_per_account"`
}

// Validate refuses a budget that would admit no client lane at all.
func (p ClientLaneParams) Validate() error {
	if p.MaxLanesPerAccount == 0 {
		return fmt.Errorf("client lanes: max_lanes_per_account must be at least 1")
	}
	return nil
}
```

and append to `ConsensusParamsUpdate`:

```go
	// ClientLanes enables client_seq lanes. Absent (nil) keeps them disabled
	// and is omitted from the canonical form, so every previously signed
	// update keeps its digest. Once committed it must be resent by every later
	// update; the FSM refuses removal and lowering and requires the table
	// registry to be enabled first.
	ClientLanes *ClientLaneParams `json:"client_lanes,omitempty"`
```

In `wire/consensus.go`, add `ClientLanes: ClientLaneParamsFromPB(m.GetClientLanes()),` to `ConsensusParamsUpdateFromPB`, `ClientLanes: ClientLaneParamsToPB(v.ClientLanes),` to `ConsensusParamsUpdateToPB`, and:

```go
// ClientLaneParamsFromPB copies the transport fields; validation belongs to
// authority normalisation. A missing message decodes to nil.
func ClientLaneParamsFromPB(m *pb.ClientLaneParams) *arbiter.ClientLaneParams {
	if m == nil {
		return nil
	}
	return &arbiter.ClientLaneParams{MaxLanesPerAccount: m.GetMaxLanesPerAccount()}
}

// ClientLaneParamsToPB returns an independent transport message.
func ClientLaneParamsToPB(v *arbiter.ClientLaneParams) *pb.ClientLaneParams {
	if v == nil {
		return nil
	}
	return &pb.ClientLaneParams{MaxLanesPerAccount: v.MaxLanesPerAccount}
}
```

In `authority/consensus.go`, inside `NormalizeConsensusParamsUpdate`, directly after the `if cmd.TableRegistry != nil { … }` block:

```go
	if cmd.ClientLanes != nil {
		lanes := *cmd.ClientLanes
		if err := lanes.Validate(); err != nil {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: %w", err)
		}
		cmd.ClientLanes = &lanes
	}
```

In `wire/table_registry_snapshot.go` add `ClientLanes *arbiter.ClientLaneParams` to `TableRegistrySnapshot` (comment: `// ClientLanes is the committed client-lane parameter; nil until activation.`). In `TableRegistrySnapshotFromPB`, after the cursor block:

```go
	if lanes := ClientLaneParamsFromPB(m.GetClientLanes()); lanes != nil {
		if err := lanes.Validate(); err != nil {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: %w", err)
		}
		out.ClientLanes = lanes
	}
```

and in `TableRegistrySnapshotToPB` set `ClientLanes: ClientLaneParamsToPB(s.ClientLanes),` in the composite literal.

In `conformance/arbiter_wire_test.go` add `assertMirror(t, arbiter.ClientLaneParams{}, &pb.ClientLaneParams{})`.

- [ ] **Step 4: Run the tests**

Run: `cd "$W" && go test ./... 2>&1 | tail -30`
Expected: every new test PASSES; `TestConsensusParamsUpdateHashIsFrozen` and `TestLegacyCommandBytesAreFrozen` PASS unchanged; the only remaining failure is the `NodeRegistration` mirror (Task 4).

- [ ] **Step 5: Commit**

```bash
cd "$W" && git add consensus.go consensus_test.go wire authority conformance && git commit -m "feat(consensus): ClientLaneParams on the update and the registry snapshot

Absent until set, omitted from the canonical form so every previously
signed update keeps its digest; normalisation refuses a zero budget; the
table-registry snapshot carries it so the follower delivers activation
(housegate spec 2026-10-09 D13, §5.3).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 4: arbiter-core — request-only features, SNode/verifier registration, release (B2)

**Files:**
- Modify: `wire/command.go` (`Decode`, case `*pb.RaftCommand_RegisterNode`, ~line 388)
- Create: `wire/registration_request_only_test.go`
- Modify: `snode/snode.go:139-160` (`Register`), `verifier/verifier.go:113-135` (`Register`)
- Create: `snode/registration_test.go`, `verifier/registration_test.go`
- Modify: `conformance/arbiter_wire_test.go` (`assertMirror` → request-only exception)
- Modify: `README.md` (Compatibility section)

**Interfaces:**
- Consumes: Task 3 `arbiter.LocalNodeFeatures()`, Task 1 `pb.NodeRegistration.Features`.
- Produces: `snode.registrationRequest(nodeID string) *pb.NodeRegistration`; `verifier.registrationRequest(replicaID string, pub ed25519.PublicKey) *pb.NodeRegistration`; `wire.Decode` refuses a `RegisterNode` command whose registration carries `features`; release `CORE_TAG`.

- [ ] **Step 1: Write the failing tests**

Create `wire/registration_request_only_test.go`:

```go
package wire

import (
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// TestRegistrationFeaturesNeverEnterARaftCommand pins housegate spec
// 2026-10-09 §5.6: NodeRegistration.features is request-only. A leader that
// encoded it into a RegisterNode command would make every voter that does not
// know field 5 reject the entry while upgraded voters apply it, forking
// membership.
func TestRegistrationFeaturesNeverEnterARaftCommand(t *testing.T) {
	req := &pb.NodeRegistration{NodeId: "v1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER},
		Ed25519Pubkey: make([]byte, 32), Features: []string{arbiter.ClientLanesFeature}}
	reg := RegistrationFromPB(req)
	if out := RegistrationToPB(reg); len(out.GetFeatures()) != 0 {
		t.Fatalf("RegistrationToPB set features %v", out.GetFeatures())
	}
	b, err := Encode(Command{RegisterNode: &RegisterNode{Registration: reg}})
	if err != nil {
		t.Fatal(err)
	}
	var cmd pb.RaftCommand
	if err := proto.Unmarshal(b, &cmd); err != nil {
		t.Fatal(err)
	}
	if got := cmd.GetRegisterNode().GetRegistration(); len(got.GetFeatures()) != 0 || len(got.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("encoded registration carries features or unknown fields: %v", got)
	}
}

// TestDecodeRefusesARegistrationCommandCarryingFeatures makes a buggy encoder
// fail identically on every voter: an old voter refuses field 5 as unknown, a
// lane-aware voter refuses it here, so no voter applies it.
func TestDecodeRefusesARegistrationCommandCarryingFeatures(t *testing.T) {
	b, err := proto.Marshal(&pb.RaftCommand{Cmd: &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{
		Registration: &pb.NodeRegistration{NodeId: "s1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}, Features: []string{"client_lanes_v1"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(b); err == nil || !strings.Contains(err.Error(), "request-only") {
		t.Fatalf("Decode = %v, want a request-only refusal", err)
	}
}
```

Create `snode/registration_test.go`:

```go
package snode

import (
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func TestSNodeRegistrationAdvertisesClientLanes(t *testing.T) {
	req := registrationRequest("s1")
	if req.GetNodeId() != "s1" || !slices.Equal(req.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}) {
		t.Fatalf("registration = %v", req)
	}
	if !slices.Contains(req.GetFeatures(), arbiter.ClientLanesFeature) {
		t.Fatalf("features = %v, want %s", req.GetFeatures(), arbiter.ClientLanesFeature)
	}
}
```

Create `verifier/registration_test.go`:

```go
package verifier

import (
	"crypto/ed25519"
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func TestVerifierRegistrationAdvertisesClientLanes(t *testing.T) {
	pub := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	req := registrationRequest("v1", pub)
	if req.GetNodeId() != "v1" || !slices.Equal(req.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}) || !slices.Equal(req.GetEd25519Pubkey(), []byte(pub)) {
		t.Fatalf("registration = %v", req)
	}
	if !slices.Contains(req.GetFeatures(), arbiter.ClientLanesFeature) {
		t.Fatalf("features = %v, want %s", req.GetFeatures(), arbiter.ClientLanesFeature)
	}
}
```

Run: `cd "$W" && go test ./wire/ ./snode/ ./verifier/ -run 'Registration|RequestOnly|Features' -v`
Expected: build failure (`undefined: registrationRequest`) and the Decode test FAILS.

- [ ] **Step 2: Implement**

In `wire/command.go`, replace the `RegisterNode` case of `Decode`:

```go
	case *pb.RaftCommand_RegisterNode:
		// NodeRegistration.features is request-only (housegate spec 2026-10-09
		// §5.6): refuse it here so a buggy encoder fails on every voter alike.
		if len(cmd.RegisterNode.GetRegistration().GetFeatures()) != 0 {
			return Command{}, fmt.Errorf("wire: NodeRegistration.features is request-only and never part of a RaftCommand")
		}
		return Command{RegisterNode: &RegisterNode{Registration: RegistrationFromPB(cmd.RegisterNode.GetRegistration())}}, nil
```

In `snode/snode.go`, add and use in `Register`:

```go
// registrationRequest is the RegisterNode request this SNode sends. Features
// are request-only: the leader records them outside replicated state.
func registrationRequest(nodeID string) *pb.NodeRegistration {
	return &pb.NodeRegistration{
		NodeId:   nodeID,
		Roles:    []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE},
		Features: arbiter.LocalNodeFeatures(),
	}
}
```

replacing the inline literal with `pb.NewMembershipClient(conn).RegisterNode(ctx, registrationRequest(r.cfg.NodeID))`. In `verifier/verifier.go`:

```go
// registrationRequest is the RegisterNode request this verifier sends.
func registrationRequest(replicaID string, pub ed25519.PublicKey) *pb.NodeRegistration {
	return &pb.NodeRegistration{
		NodeId:        replicaID,
		Roles:         []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER},
		Ed25519Pubkey: pub,
		Features:      arbiter.LocalNodeFeatures(),
	}
}
```

replacing the inline literal with `registrationRequest(r.cfg.ReplicaID, pub)`. Add the `arbiter` import (`github.com/sentioxyz/arbiter-core`) where missing.

In `conformance/arbiter_wire_test.go` add:

```go
// assertMirrorExcept is assertMirror for a pb message with request-only
// fields that deliberately have no canonical Go mirror.
func assertMirrorExcept(t *testing.T, goValue any, msg proto.Message, requestOnly ...string) {
	t.Helper()
	goTags := map[string]bool{}
	rt := reflect.TypeOf(goValue)
	for i := 0; i < rt.NumField(); i++ {
		goTags[strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if slices.Contains(requestOnly, name) {
			if goTags[name] {
				t.Errorf("%s: request-only field %q must not have a Go mirror (it would enter replicated state)", rt.Name(), name)
			}
			continue
		}
		if !goTags[name] {
			t.Errorf("%s: proto field %q has no Go mirror json tag", rt.Name(), name)
		}
		delete(goTags, name)
	}
	for tag := range goTags {
		t.Errorf("%s: Go json tag %q has no proto field", rt.Name(), tag)
	}
}
```

Replace `assertMirror(t, arbiter.NodeRegistration{}, &pb.NodeRegistration{})` with `assertMirrorExcept(t, arbiter.NodeRegistration{}, &pb.NodeRegistration{}, "features")`. Add `"slices"` to the imports.

In `README.md` § Compatibility add one paragraph (no hard wraps): client lanes (`StatementID.ClientLane`, `ConsensusParamsUpdate.ClientLanes`, request-only `NodeRegistration.features`), that legacy ids, digests and command bytes are unchanged (pinned by `wire/legacy_command_golden_test.go` and `authority/consensus_golden_test.go`), and that SNode and verifier registrations advertise `client_lanes_v1`.

- [ ] **Step 3: Run the full suite**

Run: `cd "$W" && bazel run //:gazelle && bazel build //... && bazel test //... && bazel test --build_tests_only --@rules_go//go/config:race //...`
Expected: all PASS (ClickHouse-gated SNode/verifier tests skip without `ARBITER_CH_INTEGRATION`).

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add -A && git commit -m "feat(membership): request-only client_lanes_v1 feature on SNode and verifier registration

Data-plane registrations advertise client_lanes_v1; the converter never
copies features into a RaftCommand and Decode refuses a command that
carries them, so a buggy encoder cannot fork membership (housegate spec
2026-10-09 §5.6).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 5: PR, review, merge (requires explicit user confirmation)**

Push the branch, `gh pr create --repo sentioxyz/arbiter-core --fill`, wait for both CI jobs (unit + `integration-clickhouse`), merge after review.

- [ ] **Step 6: Release `CORE_TAG` (requires explicit user confirmation)**

Run the Cut Release workflow (`gh workflow run cut-release.yml --repo sentioxyz/arbiter-core --ref main`); record the tag as `CORE_TAG`; verify by content: `git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core fetch -q --tags && git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core show "$CORE_TAG":types.go | grep -c 'func (c StatementCoord) Subject'` prints `1`.

---

## Task 5: arbiter — pin `CORE_TAG`, freeze legacy history, key the accumulator by subject (B3)

**Files:**
- Create: `fsm/legacy_lane_golden_test.go`, `fsm/testdata/legacy_lane_golden.json` (captured on unchanged `origin/main`)
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (via `scripts/update-arbiter-core.sh`)
- Modify: `accumulator/spentids.go` (`Insert`, `Status`; new `Has`, `Subjects`), `accumulator/proof.go:119-195` (`Verify`, `ProveNonMembership`)
- Create: `accumulator/subjects_test.go`, `accumulator/lane_vectors_test.go`
- Modify: `accumulator/fuzz_test.go` (append `FuzzLanedSubjects` only)

**Interfaces:**
- Consumes: Task 2 `arbiter.StatementCoord.Lane`, `(StatementCoord).Subject()`.
- Produces: accumulator keyed by `c.Subject()` everywhere; `func (a *SpentIDs) Has(subject string) bool`; `func (a *SpentIDs) Subjects() []string` (sorted); `AccountState(subject string)` unchanged in signature; the committed golden `fsm/testdata/legacy_lane_golden.json` that every later arbiter task must keep green.

- [ ] **Step 1: Create the worktree** (URWT command from Task 1 Step 1 with `/Users/uranuswch/Dev/sentio_xyz/arbiter`; `W` is its `path`). Do not touch `go.mod` yet.

- [ ] **Step 2: Freeze a legacy history on unchanged code**

Create `fsm/legacy_lane_golden_test.go`:

```go
package fsm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	legacyGoldenPath         = "testdata/legacy_lane_golden.json"
	legacyGoldenUserKeyHex   = "8f2a55949038a9610f50fb23b5883af3b4ecb3c3bb792cbcefbd1542c692be63"
	legacyGoldenAuthorityHex = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
)

// legacyGolden is the observable result of one fixed, lane-free history:
// every command's FSM answer, every sealed header's chain hash, the
// spent_ids_root and the exact snapshot container. It was captured from the
// release before client_seq lanes (housegate spec 2026-10-09 §7, §9.3:
// "legacy ids stay valid forever"). Never recapture it: a difference here
// means the lane release replays existing history differently.
type legacyGolden struct {
	Results         []string `json:"results"`
	ChainHashes     []string `json:"chain_hashes"`
	SpentIDsRoot    string   `json:"spent_ids_root"`
	SnapshotVersion byte     `json:"snapshot_version"`
	SnapshotSHA256  string   `json:"snapshot_sha256"`
}

func describeApply(res any) string {
	switch r := res.(type) {
	case SubmitResult:
		return fmt.Sprintf("submit:%d:%d:%s", r.Code, r.StatementSeq, r.Message)
	case Rejected:
		return "rejected:" + r.Reason
	default:
		return fmt.Sprintf("%T", res)
	}
}

func runLegacyGoldenHistory(t *testing.T) legacyGolden {
	t.Helper()
	signer, err := authority.NewSignerFromHex(legacyGoldenAuthorityHex)
	if err != nil {
		t.Fatal(err)
	}
	params := testParams()
	params.AuthorityAddresses = []string{signer.Address()}
	f := mustNewFSM(t, params)
	key, err := crypto.HexToECDSA(legacyGoldenUserKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	account := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	var out legacyGolden
	apply := func(c wire.Command) { out.Results = append(out.Results, describeApply(f.Apply(mkLog(t, c)))) }
	apply(wire.Command{RegisterNode: &wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}})
	apply(wire.Command{MarkActive: &wire.MarkActive{NodeID: "s1"}})
	// 1, a jump to 3 (one gap range), its edge fill 2, a duplicate 2, a jump
	// to 5 and the fill 4: every accumulator transition a legacy client makes.
	for _, seq := range []uint64{1, 3, 2, 2, 5, 4} {
		apply(wire.Command{SubmitStatement: &wire.SubmitStatement{Envelope: validEnvelope(t, key, account, seq)}})
	}
	apply(wire.Command{SealL3Block: &wire.SealL3Block{}})
	update := consensusUpdate(t, f, params.AuthorityAddresses, 1)
	update.TableRegistry = testRegistryParams()
	apply(signedConsensusUpdate(t, signer, update))
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

func TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease(t *testing.T) {
	got := runLegacyGoldenHistory(t)
	if os.Getenv("ARBITER_CAPTURE_LEGACY_LANE_GOLDEN") == "1" {
		if _, err := os.Stat(legacyGoldenPath); err == nil {
			t.Fatalf("%s exists; it is captured once from the pre-lane release and never rewritten", legacyGoldenPath)
		}
		b, _ := json.MarshalIndent(got, "", "  ")
		if err := os.WriteFile(legacyGoldenPath, append(b, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Skip("captured; commit the file")
	}
	raw, err := os.ReadFile(legacyGoldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var want legacyGolden
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		gb, _ := json.MarshalIndent(got, "", "  ")
		t.Fatalf("legacy history replays differently from the pre-lane release:\n%s", gb)
	}
	if len(got.ChainHashes) == 0 {
		t.Fatal("the history sealed no block; the golden would not cover chain hashes")
	}
}
```

Capture it with Go directly on the unmodified tree (Bazel's sandbox cannot write into the source tree):

Run: `cd "$W" && ARBITER_CAPTURE_LEGACY_LANE_GOLDEN=1 go test ./fsm/ -run TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease -v && go test ./fsm/ -run TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease -v && cat fsm/testdata/legacy_lane_golden.json`
Expected: first run SKIP ("captured"), second run PASS; the file lists seven `submit:` results with codes 1 (accepted) and 2 (duplicate), a non-empty `chain_hashes`, and `snapshot_version` 15. If `chain_hashes` is empty the test fails — stop and investigate the seal before continuing. Then add the testdata file to the `fsm` test target's `data` (`bazel run //:gazelle` does not do this; add `"testdata/legacy_lane_golden.json"` to the existing `data = glob(["testdata/**"])` or equivalent list in `fsm/BUILD.bazel` if it is not already covered) and run `bazel test //fsm:fsm_test --test_filter=TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`.

```bash
cd "$W" && git add fsm/legacy_lane_golden_test.go fsm/testdata/legacy_lane_golden.json fsm/BUILD.bazel && git commit -m "test(fsm): freeze a lane-free history from the pre-lane release

Command answers, chain hashes, spent_ids_root and the exact snapshot
container of a fixed legacy history; the client_seq lane release must
replay it byte for byte (housegate spec 2026-10-09 §7, §9.3).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 3: Pin arbiter-core `CORE_TAG`**

Run: `cd "$W" && bash scripts/update-arbiter-core.sh "$CORE_TAG" && bazel build //... && bazel test //...`
Expected: build succeeds (the new pb RPCs are covered by the embedded `Unimplemented*Server` types); `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, `TestDeterminism_*`, the L3 commitment goldens, `accumulator` vector tests and `fsm/shared_vectors_test.go` all PASS. Commit `chore(deps): pin arbiter-core $CORE_TAG` with the trailer.

- [ ] **Step 4: Write the failing accumulator tests**

Create `accumulator/subjects_test.go`:

```go
package accumulator

import (
	"errors"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

const (
	testLaneA = "000000000000000a"
	testLaneB = "000000000000000b"
)

func coord(account, lane string, seq uint64) arbiter.StatementCoord {
	return arbiter.StatementCoord{Account: account, Lane: lane, ClientSeq: seq}
}

func TestSubjectsAreIndependent(t *testing.T) {
	a := NewSpentIDs()
	for _, c := range []arbiter.StatementCoord{coord("0xaa", "", 1), coord("0xaa", testLaneA, 1), coord("0xaa", testLaneB, 1)} {
		if err := a.Insert(c); err != nil {
			t.Fatalf("Insert(%+v): %v", c, err)
		}
	}
	if a.Status(coord("0xaa", testLaneA, 1)) != StatusSpentDuplicate || a.Status(coord("0xaa", testLaneA, 2)) != StatusFresh {
		t.Fatal("lane A state is wrong")
	}
	if err := a.Insert(coord("0xaa", testLaneA, 1)); !errors.Is(err, ErrSpentDuplicate) {
		t.Fatalf("duplicate on lane A = %v", err)
	}
	if hi, _, ok := a.AccountState("0xaa"); !ok || hi != 1 {
		t.Fatalf("legacy subject hi=%d ok=%v", hi, ok)
	}
	if !a.Has("0xaa:" + testLaneA) || a.Has("0xaa:0000000000000000") || !a.Has("0xaa") {
		t.Fatal("Has does not answer per subject")
	}
	if got, want := a.Subjects(), []string{"0xaa", "0xaa:" + testLaneA, "0xaa:" + testLaneB}; !slices.Equal(got, want) {
		t.Fatalf("Subjects = %v, want %v", got, want)
	}
}

func TestGapBudgetIsPerSubject(t *testing.T) {
	a := NewSpentIDs()
	// Inserting 2, 4, …, 128 on lane A opens 64 single-seq gaps: the budget.
	for seq := uint64(2); seq <= 2*MaxOpenGapRanges; seq += 2 {
		if err := a.Insert(coord("0xaa", testLaneA, seq)); err != nil {
			t.Fatalf("seq %d: %v", seq, err)
		}
	}
	if err := a.Insert(coord("0xaa", testLaneA, 2*MaxOpenGapRanges+2)); !errors.Is(err, ErrGapBudgetExceeded) {
		t.Fatalf("65th gap on lane A = %v, want ErrGapBudgetExceeded", err)
	}
	for _, c := range []arbiter.StatementCoord{coord("0xaa", testLaneB, 5), coord("0xaa", "", 5)} {
		if err := a.Insert(c); err != nil {
			t.Fatalf("lane A's exhausted budget leaked into %+v: %v", c, err)
		}
	}
}

func TestAscendingRecycleNeverAddsARange(t *testing.T) {
	a := NewSpentIDs()
	// The agent reserved 1..5; 2 and 3 were refused unspent and recycled.
	for _, seq := range []uint64{1, 4, 5} {
		if err := a.Insert(coord("0xaa", testLaneA, seq)); err != nil {
			t.Fatal(err)
		}
	}
	_, ranges, _ := a.AccountState("0xaa:" + testLaneA)
	open := len(ranges)
	for _, seq := range []uint64{2, 3, 6} {
		if err := a.Insert(coord("0xaa", testLaneA, seq)); err != nil {
			t.Fatal(err)
		}
		_, ranges, _ = a.AccountState("0xaa:" + testLaneA)
		if len(ranges) > open {
			t.Fatalf("recycling seq %d opened a gap range: %v", seq, ranges)
		}
		open = len(ranges)
	}
	if open != 0 {
		t.Fatalf("ranges after the recycle = %d, want 0", open)
	}
}

func TestNonMembershipProofIsBoundToTheSubject(t *testing.T) {
	a := NewSpentIDs()
	for _, c := range []arbiter.StatementCoord{coord("0xaa", "", 1), coord("0xaa", testLaneA, 1), coord("0xaa", testLaneA, 10)} {
		if err := a.Insert(c); err != nil {
			t.Fatal(err)
		}
	}
	laned := coord("0xaa", testLaneA, 5)
	p, err := a.ProveNonMembership(laned)
	if err != nil || !Verify(a.Root(), laned, p) {
		t.Fatalf("laned proof: %v", err)
	}
	if Verify(a.Root(), coord("0xaa", "", 5), p) || Verify(a.Root(), coord("0xaa", testLaneB, 5), p) {
		t.Fatal("a proof for lane A must not verify for another subject")
	}
}
```

Create `accumulator/lane_vectors_test.go`:

```go
package accumulator

import (
	"encoding/hex"
	"testing"
)

// TestLanedSubjectVectors freezes the sentio-spent-ids-v1 key and leaf hashes
// of laned subjects (housegate spec 2026-10-09 D12): the profile is unchanged,
// a laned subject is simply the opaque string "<account>:<lane>". Values were
// computed once with this profile and are pasted, never regenerated, under the
// same rule as testdata/spent_ids_vectors.json.
func TestLanedSubjectVectors(t *testing.T) {
	for _, tc := range []struct {
		subject       string
		hi            uint64
		ranges        []SeqRange
		wantKey, want string
	}{
		{"0x00000000000000000000000000000000000000a1:5e1f0a2b7c9d3e4f", 1, nil, "", ""},
		{"0x00000000000000000000000000000000000000a1:5e1f0a2b7c9d3e4f", 9, []SeqRange{{Start: 3, End: 4}}, "", ""},
		{"0x00000000000000000000000000000000000000a1:ffffffffffffffff", 2, nil, "", ""},
	} {
		key := hashKey(tc.subject)
		leaf := hashLeafVal(tc.subject, tc.hi, tc.ranges)
		if hex.EncodeToString(key[:]) != tc.wantKey || hex.EncodeToString(leaf[:]) != tc.want {
			t.Errorf("%s hi=%d ranges=%v: key %x leaf %x", tc.subject, tc.hi, tc.ranges, key, leaf)
		}
	}
	a := NewSpentIDs()
	for _, c := range []struct {
		lane string
		seq  uint64
	}{{"", 1}, {"5e1f0a2b7c9d3e4f", 1}, {"5e1f0a2b7c9d3e4f", 3}, {"ffffffffffffffff", 1}} {
		if err := a.Insert(coord("0x00000000000000000000000000000000000000a1", c.lane, c.seq)); err != nil {
			t.Fatal(err)
		}
	}
	const wantRoot = ""
	if got := hex.EncodeToString(a.Root()); got != wantRoot {
		t.Errorf("root = %s", got)
	}
}
```

Append to `accumulator/fuzz_test.go`:

```go
// FuzzLanedSubjects feeds subjects containing ':' (housegate spec 2026-10-09
// §9.3): two accumulators fed the same operations agree, a laned insert never
// touches the account's legacy subject, and a laned non-membership proof does
// not verify for the legacy coordinate.
func FuzzLanedSubjects(f *testing.F) {
	f.Add("0xaa", "000000000000000a", uint64(1), uint64(3))
	f.Add("0x01", "ffffffffffffffff", uint64(7), uint64(7))
	f.Fuzz(func(t *testing.T, account, lane string, s1, s2 uint64) {
		if account == "" || len(account) > 64 || strings.Contains(account, ":") || lane == "" || len(lane) > 32 {
			t.Skip()
		}
		a, b := NewSpentIDs(), NewSpentIDs()
		for _, s := range []uint64{s1%1000 + 1, s2%1000 + 1} {
			c := arbiter.StatementCoord{Account: account, Lane: lane, ClientSeq: s}
			if (a.Insert(c) == nil) != (b.Insert(c) == nil) {
				t.Fatal("identical operations diverged")
			}
		}
		if !bytes.Equal(a.Root(), b.Root()) {
			t.Fatal("roots diverged")
		}
		if a.Has(account) || a.Status(arbiter.StatementCoord{Account: account, ClientSeq: s1%1000 + 1}) != StatusFresh {
			t.Fatal("a laned insert touched the legacy subject")
		}
		c := arbiter.StatementCoord{Account: account, Lane: lane, ClientSeq: 5000}
		p, err := a.ProveNonMembership(c)
		if err != nil {
			t.Fatal(err)
		}
		if !Verify(a.Root(), c, p) || Verify(a.Root(), arbiter.StatementCoord{Account: account, ClientSeq: 5000}, p) {
			t.Fatal("non-membership proof is not bound to its subject")
		}
	})
}
```

(add `"strings"` to the `fuzz_test.go` imports.)

Run: `cd "$W" && go test ./accumulator/ -run 'Subjects|GapBudgetIsPerSubject|AscendingRecycle|NonMembershipProofIsBound|LanedSubjectVectors' -v`
Expected: build failure (`a.Has undefined`, `a.Subjects undefined`); after adding just those two methods, `TestSubjectsAreIndependent` FAILS because the accumulator still keys on `c.Account` (lane A seq 1 collides with legacy seq 1 → `ErrSpentDuplicate`).

- [ ] **Step 5: Key the accumulator by subject**

In `accumulator/spentids.go`, inside `Insert` and `Status` replace every `c.Account` with a local `subject := c.Subject()` (validation, map lookup, map store, `hashKey`, `hashLeafVal`). In `accumulator/proof.go`, `Verify` uses `validateAccount(c.Subject())`, `d.account != c.Subject()` and `hashKey(c.Subject())`; `ProveNonMembership` uses `c.Subject()` for validation, lookup, `pathSiblings(hashKey(...))`, the error message and `encodeProof`. Update the doc comments: "account" → "subject (the account for the legacy lane, `<account>:<lane>` for a client lane)". Add to `spentids.go`:

```go
// Has reports whether subject holds accumulator state.
func (a *SpentIDs) Has(subject string) bool {
	_, ok := a.accounts[subject]
	return ok
}

// Subjects returns every subject holding state, sorted. Snapshot restore uses
// it to re-derive per-account lane counts.
func (a *SpentIDs) Subjects() []string {
	out := make([]string, 0, len(a.accounts))
	for subject := range a.accounts {
		out = append(out, subject)
	}
	slices.Sort(out)
	return out
}
```

(add `"slices"` to the imports).

- [ ] **Step 6: Capture the laned vectors and run everything**

Run: `cd "$W" && go test ./accumulator/ -run TestLanedSubjectVectors -v` — it fails printing each `key`/`leaf` and the `root`; paste them into `wantKey`, `want` and `wantRoot` (the profile is unchanged, so these are pure functions of the frozen hashing rules), then:

Run: `cd "$W" && go test ./accumulator/ -fuzz FuzzLanedSubjects -fuzztime 60s && bazel test //accumulator:all //fsm:all`
Expected: the fuzzer finds nothing; all PASS, including `TestVectors` over the untouched `spent_ids_vectors.json` and `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` (legacy subjects equal the account, so nothing moved).

- [ ] **Step 7: Commit**

```bash
cd "$W" && git add accumulator && git commit -m "feat(accumulator): key spent ids by subject (account or account:lane)

The sentio-spent-ids-v1 profile is unchanged; a laned coordinate is the
opaque subject <account>:<lane>, so legacy-only history keeps its
spent_ids_root. Adds Has/Subjects, laned key/leaf/root vectors and a fuzz
target over subjects containing ':' (housegate spec 2026-10-09 D12).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 6: arbiter — `Params.ClientLanes`, its transition rule and the registry version bump (B3)

**Files:**
- Modify: `fsm/state.go:85-114` (`Params`), `fsm/watch.go:80-104` (`NewWithNotify`), `fsm/params_identity.go` (`normalizeConsensusParams`), `fsm/consensus_reads.go` (`cloneParams`), `fsm/consensus_history.go:10-25`, `fsm/consensus_updates.go` (`applyUpdateConsensusParams`, `verifyConsensusTransition`), `fsm/table_registry.go:259-305` (`TableRegistryView`)
- Create: `fsm/client_lanes.go`, `fsm/client_lanes_params_test.go`
- Modify: `server/consensus_admin.go:68-80` (`consensusMutableParams`), `server/table_registry.go:99` (`tableRegistryToPB`)
- Create: `server/client_lanes_test.go`

**Interfaces:**
- Consumes: Task 3 `arbiter.ClientLaneParams`, `ConsensusParamsUpdate.ClientLanes`, `wire.ClientLaneParamsToPB`; Task 5 golden.
- Produces: `fsm.Params.ClientLanes *arbiter.ClientLaneParams`; `func (f *FSM) ClientLanes() (arbiter.ClientLaneParams, bool)`; `func (f *FSM) ClientLanesEnabled() bool`; `fsm.TableRegistryView.ClientLanes *arbiter.ClientLaneParams`; test helper `activateLanes(t, f, signer, params, max)` in `fsm/client_lanes_params_test.go`.

- [ ] **Step 1: Write the failing tests**

Create `fsm/client_lanes_params_test.go`:

```go
package fsm

import (
	"errors"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
)

// activateLanes enables the table registry (when it is not yet enabled) and
// then client lanes with budget max, each through a signed update.
func activateLanes(t *testing.T, f *FSM, a *authority.Signer, params Params, max uint32) {
	t.Helper()
	if f.st.Params.TableRegistry == nil {
		mustApply(t, f, signedConsensusUpdate(t, a, registryUpdate(t, f, params, testRegistryParams())))
	}
	u := registryUpdate(t, f, params, testRegistryParams())
	u.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: max}
	mustApply(t, f, signedConsensusUpdate(t, a, u))
}

func lanesUpdate(t *testing.T, f *FSM, params Params, max uint32) arbiter.ConsensusParamsUpdate {
	t.Helper()
	u := registryUpdate(t, f, params, testRegistryParams())
	u.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: max}
	return u
}

func TestClientLanesRequireTheTableRegistry(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	u := consensusUpdate(t, f, params.AuthorityAddresses, 1)
	u.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, u), "before the table registry")
}

func TestClientLanesMayBeEnabledWithTheRegistry(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	mustApply(t, f, signedConsensusUpdate(t, a, lanesUpdate(t, f, params, 256)))
	if got, ok := f.ClientLanes(); !ok || got.MaxLanesPerAccount != 256 {
		t.Fatalf("ClientLanes = %+v, %v", got, ok)
	}
	if v := f.st.TableRegistry.Version; v != 2 {
		t.Fatalf("registry version = %d, want 2 (created at 1, bumped once for lanes)", v)
	}
}

func TestClientLanesActivationBumpsTheRegistryVersionOnce(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	mustApply(t, f, signedConsensusUpdate(t, a, registryUpdate(t, f, params, testRegistryParams())))
	before := f.st.TableRegistry.Version
	cursor, incarnations := f.st.TableRegistry.Cursor, len(f.st.TableRegistry.Incarnations)
	changed := f.RegistryChanged()
	mustApply(t, f, signedConsensusUpdate(t, a, lanesUpdate(t, f, params, 256)))
	if got := f.st.TableRegistry.Version; got != before+1 {
		t.Fatalf("registry version %d -> %d, want exactly +1", before, got)
	}
	select {
	case <-changed:
	default:
		t.Fatal("registry watchers were not woken")
	}
	if f.st.TableRegistry.Cursor != cursor || len(f.st.TableRegistry.Incarnations) != incarnations {
		t.Fatal("a ClientLanes-only update changed what the registry auditor reads")
	}
	view, ok := f.TableRegistryView()
	if !ok || view.ClientLanes == nil || view.ClientLanes.MaxLanesPerAccount != 256 {
		t.Fatalf("TableRegistryView.ClientLanes = %+v", view.ClientLanes)
	}
	view.ClientLanes.MaxLanesPerAccount = 1
	if p, _ := f.ClientLanes(); p.MaxLanesPerAccount != 256 {
		t.Fatal("TableRegistryView leaked the ClientLanes pointer")
	}
}

func TestClientLanesMustBeCarried(t *testing.T) {
	a, params := authorityFixture(t)
	b, _ := authorityFixture(t)
	f := mustNewFSM(t, params)
	activateLanes(t, f, a, params, 256)
	omitted := registryUpdate(t, f, params, testRegistryParams())
	omitted.AuthorityAddresses = []string{b.Address()}
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, omitted), "must carry the client lanes")
	version := f.st.TableRegistry.Version
	carried := lanesUpdate(t, f, params, 256)
	carried.AuthorityAddresses = []string{b.Address()}
	mustApply(t, f, signedConsensusUpdate(t, a, carried))
	if !f.AuthorityAllowed(b.Address()) || f.st.TableRegistry.Version != version {
		t.Fatal("an update carrying unchanged lanes must rotate authorities without bumping the registry")
	}
}

func TestClientLanesAreRaiseOnly(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	activateLanes(t, f, a, params, 256)
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, lanesUpdate(t, f, params, 255)), "cannot lower")
	version := f.st.TableRegistry.Version
	mustApply(t, f, signedConsensusUpdate(t, a, lanesUpdate(t, f, params, 300)))
	if p, _ := f.ClientLanes(); p.MaxLanesPerAccount != 300 || f.st.TableRegistry.Version != version+1 {
		t.Fatalf("raise: lanes %+v version %d", p, f.st.TableRegistry.Version)
	}
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, lanesUpdate(t, f, params, 300)), "does not change")
}

func TestClientLanesZeroBudgetIsRefused(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	rejectConsensusUnchanged(t, f, signedConsensusUpdate(t, a, lanesUpdate(t, f, params, 0)), "max_lanes_per_account")
}

func TestClientLanesAreNeverAGenesisParameter(t *testing.T) {
	params := testParams()
	params.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	if _, err := New(params); !errors.Is(err, ErrGenesisParams) || !strings.Contains(err.Error(), "client_lanes") {
		t.Fatalf("New = %v, want ErrGenesisParams naming client_lanes", err)
	}
}

func TestParamsDigestUnchangedWithoutClientLanes(t *testing.T) {
	params := testParams()
	before, err := params.ConsensusDigest()
	if err != nil {
		t.Fatal(err)
	}
	params.ClientLanes = nil
	if after, _ := params.ConsensusDigest(); after != before {
		t.Fatal("nil client lanes changed the params digest")
	}
	params.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	if with, _ := params.ConsensusDigest(); with == before {
		t.Fatal("client lanes must be bound by the params digest")
	}
}
```

Create `server/client_lanes_test.go`:

```go
package server

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"

	"github.com/sentioxyz/arbiter/fsm"
)

func TestConsensusMutableParamsCarryClientLanes(t *testing.T) {
	if got := consensusMutableParams(fsm.Params{}); got.GetClientLanes() != nil {
		t.Fatalf("disabled lanes rendered %v", got.GetClientLanes())
	}
	got := consensusMutableParams(fsm.Params{ClientLanes: &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}})
	if got.GetClientLanes().GetMaxLanesPerAccount() != 256 {
		t.Fatalf("client_lanes = %v", got.GetClientLanes())
	}
}

func TestTableRegistryToPBCarriesClientLanes(t *testing.T) {
	view := fsm.TableRegistryView{Version: 4, ClientLanes: &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}}
	if got := tableRegistryToPB(view); got.GetClientLanes().GetMaxLanesPerAccount() != 256 || got.GetVersion() != 4 {
		t.Fatalf("snapshot = %v", got)
	}
	if got := tableRegistryToPB(fsm.TableRegistryView{}); got.GetClientLanes() != nil {
		t.Fatal("disabled lanes must be absent from the snapshot")
	}
}
```

Run: `cd "$W" && go test ./fsm/ ./server/ -run 'ClientLanes|ParamsDigestUnchangedWithoutClientLanes|CarryClientLanes|CarriesClientLanes' -v`
Expected: build failure (`unknown field ClientLanes in struct literal of type fsm.Params`).

- [ ] **Step 2: Implement the parameter**

`fsm/state.go`, append to `Params` after `TableRegistry`:

```go
	// ClientLanes enables client_seq lanes (housegate spec 2026-10-09 D13). It
	// is set by an authority-signed update once the table registry exists,
	// carried by every later update, raised only, and never removed; nil keeps
	// every laned statement MALFORMED.
	ClientLanes *arbiter.ClientLaneParams `json:"client_lanes,omitempty"`
```

`fsm/params_identity.go`, in `normalizeConsensusParams` after the `TableRegistry` copy, and `fsm/consensus_reads.go`, in `cloneParams` after the `TableRegistry` copy:

```go
	if p.ClientLanes != nil {
		lanes := *p.ClientLanes
		p.ClientLanes = &lanes
	}
```

(`params` instead of `p` in `cloneParams`.)

`fsm/watch.go`, in `NewWithNotify` after the `TableRegistry` refusal:

```go
	// client_lanes is installed only by a signed update after the table
	// registry; a genesis value would bypass that ordering.
	if params.ClientLanes != nil {
		return nil, fmt.Errorf("%w: client_lanes is set only by a consensus update, not at genesis", ErrGenesisParams)
	}
```

`fsm/consensus_history.go`, after the bootstrap `TableRegistry` refusal:

```go
	if st.BootstrapParams.ClientLanes != nil {
		return fmt.Errorf("snapshot bootstrap consensus params: client_lanes is set only by a consensus update, not at genesis")
	}
```

`fsm/consensus_updates.go`, in `verifyConsensusTransition` directly after the `TableRegistry` switch:

```go
	// ClientLanes has its own rule: the registry rule above refuses every
	// change, while this parameter must be raisable (spec 2026-10-09 §5.4).
	switch {
	case previous.ClientLanes == nil:
		if normalized.ClientLanes != nil {
			if next.TableRegistry == nil {
				return Params{}, fmt.Errorf("consensus update cannot enable client lanes before the table registry")
			}
			lanes := *normalized.ClientLanes
			next.ClientLanes = &lanes
		}
	case normalized.ClientLanes == nil:
		return Params{}, fmt.Errorf("consensus update must carry the client lanes parameters")
	case normalized.ClientLanes.MaxLanesPerAccount < previous.ClientLanes.MaxLanesPerAccount:
		return Params{}, fmt.Errorf("consensus update cannot lower client lanes max_lanes_per_account (%d -> %d)", previous.ClientLanes.MaxLanesPerAccount, normalized.ClientLanes.MaxLanesPerAccount)
	default:
		lanes := *normalized.ClientLanes
		next.ClientLanes = &lanes
	}
```

and in `applyUpdateConsensusParams`, replace the tail from `wasEnabled := …` with:

```go
	wasEnabled := f.st.Params.TableRegistry != nil
	previousLanes := f.st.Params.ClientLanes
	f.st.Params = next
	f.mirrorArtifactDispositionCapabilityLocked()
	if !wasEnabled && f.st.Params.TableRegistry != nil {
		f.enableTableRegistryLocked(genesisID)
	}
	// Setting or raising client lanes is delivered to data-plane nodes through
	// the registry follower, which only accepts a strictly newer version. The
	// bump is a deterministic function of this applied update.
	if lanes := f.st.Params.ClientLanes; lanes != nil && (previousLanes == nil || lanes.MaxLanesPerAccount != previousLanes.MaxLanesPerAccount) {
		f.registryChangedLocked(f.st.TableRegistry)
	}
	f.st.ConsensusEpoch++
	f.st.ConsensusUpdates = append(f.st.ConsensusUpdates, ConsensusParamsChange{Update: update, AuthorityJWS: c.AuthorityJWS})
	f.emit(Event{Kind: EventConsensusParamsUpdated})
	return Applied{}
```

Create `fsm/client_lanes.go`:

```go
package fsm

import "github.com/sentioxyz/arbiter-core"

// ClientLanes returns the committed client-lane parameter; ok is false while
// lanes are disabled. A read for gateways outside Apply.
func (f *FSM) ClientLanes() (arbiter.ClientLaneParams, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.st.Params.ClientLanes == nil {
		return arbiter.ClientLaneParams{}, false
	}
	return *f.st.Params.ClientLanes, true
}

// ClientLanesEnabled reports whether client lanes are committed.
func (f *FSM) ClientLanesEnabled() bool {
	_, ok := f.ClientLanes()
	return ok
}
```

`fsm/table_registry.go`: add `ClientLanes *arbiter.ClientLaneParams` to `TableRegistryView` and, in `TableRegistryView()`, after building `out`:

```go
	if lanes := f.st.Params.ClientLanes; lanes != nil {
		c := *lanes
		out.ClientLanes = &c
	}
```

`server/consensus_admin.go` `consensusMutableParams`: add `ClientLanes: wire.ClientLaneParamsToPB(p.ClientLanes),`. `server/table_registry.go` `tableRegistryToPB`: add `ClientLanes: wire.ClientLaneParamsToPB(v.ClientLanes),` to the composite literal.

- [ ] **Step 3: Run the tests**

Run: `cd "$W" && bazel test //fsm:all //server:all`
Expected: all PASS, including `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, every `TestConsensusUpdate*` and every determinism test.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add fsm server && git commit -m "feat(fsm): ClientLanes consensus parameter, raise-only, delivered through the registry

Set only after the table registry, carried by every later update, never
lowered or removed; each set or raise bumps the registry version once so
sentio-node learns activation through the existing follower (housegate
spec 2026-10-09 D13, §5.4).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 7: arbiter — lane admission, `LaneCounts`, `LANE_BUDGET_EXCEEDED`, gateway refusal (B3)

**Files:**
- Modify: `fsm/state.go` (`State`: `LaneCounts`; `newState` initialisation)
- Modify: `fsm/admission.go:20-135`
- Modify: `fsm/admission_test.go` (`signStatementV2`: render `env.StatementID.Flat()`)
- Create: `fsm/admission_lanes_test.go`
- Modify: `server/ingress.go` (`SubmitStatement`)
- Modify: `server/client_lanes_test.go`

**Interfaces:**
- Consumes: Task 6 `activateLanes`, `FSM.ClientLanesEnabled`; Task 5 `SpentIDs.Has`; Task 2 `arbiter.ValidClientLane`, `AdmissionCodeLaneBudgetExceeded`.
- Produces: `State.LaneCounts map[string]uint32` (`json:"lane_counts,omitempty"` on the snapshot document in Task 8); admission behaviour of §5.5; gateway pre-activation refusal message `"client lanes are not enabled"`; test helper `laneEnvelope(t, f, key, account, lane, seq)`.

- [ ] **Step 1: Write the failing tests**

In `fsm/admission_test.go`, change `signStatementV2`'s `StatementID:` line to `StatementID:    env.StatementID.Flat(),` (byte-identical for legacy ids; required so laned test envelopes are signed over the laned flat id). Run `go test ./fsm/ -run TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` — it must still PASS.

Create `fsm/admission_lanes_test.go`:

```go
package fsm

import (
	"bytes"
	"crypto/ecdsa"
	"fmt"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

const (
	testClientLaneA = "5e1f0a2b7c9d3e4f"
	testClientLaneB = "00000000000000b0"
)

// laneEnvelope is validEnvelope on lane (empty = legacy), re-signed over the
// laned flat id and, once the registry is enabled, bound to db.t's registered
// schema hash so admission reaches the accumulator.
func laneEnvelope(t *testing.T, f *FSM, key *ecdsa.PrivateKey, account, lane string, seq uint64) arbiter.StatementEnvelope {
	t.Helper()
	env := validEnvelope(t, key, account, seq)
	env.StatementID.ClientLane = lane
	if reg := f.st.TableRegistry; reg != nil {
		if inc := reg.Live(arbiter.TableKey("db", "t")); inc != nil {
			env.SchemaHash = inc.SchemaHash
		}
	}
	env.UserJWS = signStatementV2(t, key, env)
	return env
}

func lanesFSM(t *testing.T, max uint32) (*FSM, *ecdsa.PrivateKey, string) {
	t.Helper()
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	activateLanes(t, f, a, params, max)
	key, account := testAccount(t)
	return f, key, account
}

func TestLanedStatementBeforeActivationIsMalformedAndChangesNothing(t *testing.T) {
	f := newTestFSM(t)
	registerActive(t, f, "s1", arbiter.NodeRoleSNode)
	key, account := testAccount(t)
	before := snapshotBytes(t, f)
	r := submit(t, f, laneEnvelope(t, f, key, account, testClientLaneA, 1))
	if r.Code != arbiter.AdmissionCodeMalformed || r.Message != "client lanes are not enabled" {
		t.Fatalf("pre-activation laned submit = %+v", r)
	}
	if !bytes.Equal(before, snapshotBytes(t, f)) || len(f.st.LaneCounts) != 0 {
		t.Fatal("a pre-activation laned statement changed replicated state")
	}
}

func TestLanedStatementsAfterActivation(t *testing.T) {
	f, key, account := lanesFSM(t, 256)
	for _, tc := range []struct {
		lane string
		seq  uint64
		want arbiter.AdmissionCode
	}{
		{"", 1, arbiter.AdmissionCodeAccepted},
		{testClientLaneA, 1, arbiter.AdmissionCodeAccepted},
		{testClientLaneB, 1, arbiter.AdmissionCodeAccepted},
		{testClientLaneA, 1, arbiter.AdmissionCodeDuplicateClientSeq},
		{testClientLaneA, 2, arbiter.AdmissionCodeAccepted},
		{"", 1, arbiter.AdmissionCodeDuplicateClientSeq},
	} {
		if r := submit(t, f, laneEnvelope(t, f, key, account, tc.lane, tc.seq)); r.Code != tc.want {
			t.Fatalf("lane %q seq %d = %+v, want code %d", tc.lane, tc.seq, r, tc.want)
		}
	}
	if _, ok := f.st.ByStatementID[account+":"+testClientLaneA+":2:n"]; !ok {
		t.Fatal("laned statement is not indexed by its four-segment flat id")
	}
	if got := f.st.LaneCounts[account]; got != 2 {
		t.Fatalf("LaneCounts = %d, want 2 (the legacy lane never counts)", got)
	}
	if hi, _, ok := f.st.SpentIDs.AccountState(account + ":" + testClientLaneA); !ok || hi != 2 {
		t.Fatalf("lane A subject hi=%d ok=%v", hi, ok)
	}
}

func TestMalformedLaneAfterActivation(t *testing.T) {
	f, key, account := lanesFSM(t, 256)
	before := snapshotBytes(t, f)
	for _, lane := range []string{"5E1F0A2B7C9D3E4F", "5e1f0a2b7c9d3e4", "5e1f0a2b7c9d3e4f0"} {
		if r := submit(t, f, laneEnvelope(t, f, key, account, lane, 1)); r.Code != arbiter.AdmissionCodeMalformed {
			t.Fatalf("lane %q = %+v, want MALFORMED", lane, r)
		}
	}
	if !bytes.Equal(before, snapshotBytes(t, f)) {
		t.Fatal("a malformed lane changed state")
	}
}

func TestLaneBudget(t *testing.T) {
	f, key, account := lanesFSM(t, 256)
	for i := 0; i < 256; i++ {
		if r := submit(t, f, laneEnvelope(t, f, key, account, fmt.Sprintf("%016x", i), 1)); r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("lane %d = %+v", i, r)
		}
	}
	before := snapshotBytes(t, f)
	r := submit(t, f, laneEnvelope(t, f, key, account, fmt.Sprintf("%016x", 256), 1))
	if r.Code != arbiter.AdmissionCodeLaneBudgetExceeded {
		t.Fatalf("257th lane = %+v, want LANE_BUDGET_EXCEEDED", r)
	}
	if !bytes.Equal(before, snapshotBytes(t, f)) {
		t.Fatal("a refused lane changed state")
	}
	for _, env := range []arbiter.StatementEnvelope{
		laneEnvelope(t, f, key, account, fmt.Sprintf("%016x", 7), 2),
		laneEnvelope(t, f, key, account, "", 1),
	} {
		if r := submit(t, f, env); r.Code != arbiter.AdmissionCodeAccepted {
			t.Fatalf("existing lane or legacy lane at the cap = %+v", r)
		}
	}
	otherKey, other := testAccount(t)
	if r := submit(t, f, laneEnvelope(t, f, otherKey, other, testClientLaneA, 1)); r.Code != arbiter.AdmissionCodeAccepted {
		t.Fatalf("another account's first lane = %+v", r)
	}
}

func TestLaneIsBoundByTheSignature(t *testing.T) {
	f, key, account := lanesFSM(t, 256)
	env := laneEnvelope(t, f, key, account, testClientLaneA, 1)
	env.StatementID.ClientLane = testClientLaneB
	if r := submit(t, f, env); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("lane swapped after signing = %+v, want INVALID_SIGNATURE", r)
	}
	legacy := laneEnvelope(t, f, key, account, "", 1)
	legacy.StatementID.ClientLane = testClientLaneA
	if r := submit(t, f, legacy); r.Code != arbiter.AdmissionCodeInvalidSignature {
		t.Fatalf("lane added to a legacy-signed statement = %+v, want INVALID_SIGNATURE", r)
	}
}
```

Append to `server/client_lanes_test.go` (add imports `context`, `time`, `pb`):

```go
func TestSubmitStatementRefusesLanedIDBeforeActivationWithoutProposing(t *testing.T) {
	node := &fakeNode{f: newServerTestFSM(t), leader: true}
	publishServerGenesis(t, node)
	s := New(Deps{Node: node, FSM: node.f, Cfg: Config{NodeID: "arb-1", ApplyTimeout: time.Second, MaxStatementAge: time.Hour}})
	env := testServerStatementEnvelope(t)
	env.StatementId.ClientLane = "5e1f0a2b7c9d3e4f"
	before := node.appliedCount()
	ack, err := (&ingressService{s: s}).SubmitStatement(context.Background(), env)
	if err != nil || ack.GetCode() != pb.AdmissionCode_ADMISSION_CODE_MALFORMED || ack.GetMessage() != "client lanes are not enabled" {
		t.Fatalf("ack = %v, err = %v", ack, err)
	}
	if node.appliedCount() != before {
		t.Fatal("a pre-activation laned statement reached the Raft log; an old follower would fail to decode it")
	}
}
```

Run: `cd "$W" && go test ./fsm/ ./server/ -run 'Lane|Laned' -v`
Expected: build failure (`f.st.LaneCounts undefined`); after adding the field, the admission tests FAIL (laned ids collide with legacy subjects; no MALFORMED before activation) and the gateway test FAILS (`appliedCount` increases).

- [ ] **Step 2: Implement admission**

`fsm/state.go`: add to `State` (next to `SpentIDs`):

```go
	// LaneCounts is the number of distinct client lanes each account holds
	// (spec 2026-10-09 §5.4). Derived from the accumulator's laned subjects
	// and checked against them on restore; the legacy lane never counts.
	LaneCounts map[string]uint32
```

and initialise `LaneCounts: map[string]uint32{}` in `newState`.

`fsm/admission.go`, after the `id.ClientSeq == 0` check in step 1:

```go
	// 1a. client lane shape (spec 2026-10-09 §5.5 step 1). Before activation a
	// laned id is malformed on every voter; a lane-aware gateway refuses it
	// earlier so the field never reaches a voter that cannot decode it.
	if id.ClientLane != "" {
		if f.st.Params.ClientLanes == nil {
			return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "client lanes are not enabled"}
		}
		if !arbiter.ValidClientLane(id.ClientLane) {
			return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: "client_lane must be 16 lowercase hex characters"}
		}
	}
```

Replace step 5 (`coord := arbiter.StatementCoord{…}` through the `SpentIDs.Insert` error handling) with:

```go
	// 5. dedup admission over the P0b primitives, keyed by the subject
	// (account for the legacy lane, account:lane for a client lane).
	coord := id.Coord()
	if st := f.st.SpentIDs.Status(coord); st == accumulator.StatusSpentDuplicate {
		return SubmitResult{Code: arbiter.AdmissionCodeDuplicateClientSeq, Message: "client_seq already spent (client bug: do not retry with the same seq)"}
	}
	if f.snapshotQueryBarrierFencesAdmission() {
		return Rejected{Reason: snapshotQueryAdmissionFencedReason}
	}
	if f.tableSetTransitionBlockedLocked() == "" {
		return Rejected{Reason: tableSetTransitionAdmissionFencedReason}
	}
	// Lane budget, immediately before the only state mutation (spec §5.5
	// step 4): a new laned subject above the cap is refused with state
	// unchanged; an existing lane and the legacy lane are never refused here.
	newLane := coord.Lane != "" && !f.st.SpentIDs.Has(coord.Subject())
	if newLane {
		if held := f.st.LaneCounts[acct]; held >= f.st.Params.ClientLanes.MaxLanesPerAccount {
			return SubmitResult{Code: arbiter.AdmissionCodeLaneBudgetExceeded,
				Message: fmt.Sprintf("account %s already holds %d client lanes (max_lanes_per_account %d)", acct, held, f.st.Params.ClientLanes.MaxLanesPerAccount)}
		}
	}
	if err := f.st.SpentIDs.Insert(coord); err != nil {
		if errors.Is(err, accumulator.ErrGapBudgetExceeded) {
			return SubmitResult{Code: arbiter.AdmissionCodeGapBudgetExceeded, Message: err.Error()}
		}
		return SubmitResult{Code: arbiter.AdmissionCodeMalformed, Message: err.Error()}
	}
	if newLane {
		f.st.LaneCounts[acct]++
	}
```

Keep the existing comments of the fence lines when editing. `acct` is already `strings.ToLower(id.ClientAccount)` and equals `coord.Account`. Step 6 (`flat := norm.StatementID.Flat()`) already includes the lane.

`server/ingress.go`, in `SubmitStatement` directly after `env := normalizeStatementEnvelope(...)`:

```go
	// A lane-aware leader refuses a laned id before activation without
	// proposing it (spec 2026-10-09 §5.6): the command would carry
	// StatementID.client_lane, which a voter that predates lanes fails to
	// decode. The FSM refuses the same statement deterministically as well.
	if env.StatementID.ClientLane != "" && !svc.s.d.FSM.ClientLanesEnabled() {
		return &pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_MALFORMED, Message: "client lanes are not enabled"}, nil
	}
```

- [ ] **Step 3: Run the tests**

Run: `cd "$W" && bazel test //fsm:all //server:all //accumulator:all`
Expected: all PASS, including the golden and `TestAdmission_*`.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add fsm server && git commit -m "feat(admission): client lanes, per-account lane budget, LANE_BUDGET_EXCEEDED

Laned ids are MALFORMED until ClientLanes is committed (and refused by a
lane-aware gateway without a Raft entry); dedup and gap budget key on the
subject; a new lane above max_lanes_per_account is refused with state
unchanged (housegate spec 2026-10-09 §5.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 8: arbiter — snapshot v18 and `LaneCounts` restore validation (B3)

**Files:**
- Modify: `fsm/snapshot.go` (version constants 32-46, `snapshotDoc` 60-84, `snapshotWriteVersion` 171-184, `snapshotDocument`, `readSnapshot` 222-420)
- Modify: `fsm/client_lanes.go` (`validateLaneCounts`)
- Create: `fsm/snapshot_lanes_test.go`

**Interfaces:**
- Consumes: Task 7 `State.LaneCounts`; Task 5 `SpentIDs.Subjects`.
- Produces: `snapshotVersionV18 = 18` (`snapshotVersion = 18`); snapshot JSON key `lane_counts`; restore refuses lane state in a pre-v18 container and any `lane_counts` that disagrees with the accumulator.

- [ ] **Step 1: Write the failing tests**

Create `fsm/snapshot_lanes_test.go`:

```go
package fsm

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"strings"
	"testing"
)

// rewriteSnapshotDoc edits the JSON document of a snapshot container and
// re-frames it with the given version byte.
func rewriteSnapshotDoc(t *testing.T, b []byte, version byte, edit func(map[string]any)) []byte {
	t.Helper()
	n := binary.BigEndian.Uint64(b[5:13])
	var doc map[string]any
	if err := json.Unmarshal(b[13:13+n], &doc); err != nil {
		t.Fatal(err)
	}
	if edit != nil {
		edit(doc)
	}
	jb, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	out.Write(b[:4])
	out.WriteByte(version)
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(jb)))
	out.Write(lenBuf[:])
	out.Write(jb)
	out.Write(b[13+n:])
	return out.Bytes()
}

func TestSnapshotStaysPreV18UntilActivation(t *testing.T) {
	a, params := authorityFixture(t)
	f := mustNewFSM(t, params)
	mustApply(t, f, signedConsensusUpdate(t, a, registryUpdate(t, f, params, testRegistryParams())))
	if v := snapshotBytes(t, f)[4]; v == snapshotVersionV18 {
		t.Fatal("v18 written before ClientLanes was committed; an old follower could no longer install snapshots")
	}
}

func TestSnapshotV18AfterActivationRoundTrips(t *testing.T) {
	f, key, account := lanesFSM(t, 256)
	submit(t, f, laneEnvelope(t, f, key, account, testClientLaneA, 1))
	submit(t, f, laneEnvelope(t, f, key, account, testClientLaneB, 1))
	b := snapshotBytes(t, f)
	if b[4] != snapshotVersionV18 {
		t.Fatalf("version byte = %d, want 18", b[4])
	}
	restored := restoreInto(t, b)
	if lanes, ok := restored.ClientLanes(); !ok || lanes.MaxLanesPerAccount != 256 || restored.st.LaneCounts[account] != 2 {
		t.Fatalf("restored lanes %+v counts %v", lanes, restored.st.LaneCounts)
	}
	if !bytes.Equal(snapshotBytes(t, restored), b) {
		t.Fatal("restore ∘ snapshot is not the identity")
	}
}

func TestPreV18ContainerCarryingLanesIsRefused(t *testing.T) {
	f, key, account := lanesFSM(t, 256)
	submit(t, f, laneEnvelope(t, f, key, account, testClientLaneA, 1))
	forged := rewriteSnapshotDoc(t, snapshotBytes(t, f), snapshotVersionV17, nil)
	if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), "pre-v18") {
		t.Fatalf("readSnapshot = %v, want a pre-v18 refusal", err)
	}
}

func TestLaneCountsMustMatchTheAccumulator(t *testing.T) {
	f, key, account := lanesFSM(t, 256)
	submit(t, f, laneEnvelope(t, f, key, account, testClientLaneA, 1))
	for name, edit := range map[string]func(map[string]any){
		"inflated": func(doc map[string]any) { doc["lane_counts"] = map[string]any{account: 2} },
		"missing":  func(doc map[string]any) { delete(doc, "lane_counts") },
		"stranger": func(doc map[string]any) { doc["lane_counts"] = map[string]any{account: 1, "0x01": 1} },
	} {
		forged := rewriteSnapshotDoc(t, snapshotBytes(t, f), snapshotVersionV18, edit)
		if _, err := readSnapshot(bytes.NewReader(forged)); err == nil || !strings.Contains(err.Error(), "lane_counts") {
			t.Fatalf("%s: readSnapshot = %v, want a lane_counts refusal", name, err)
		}
	}
}
```

Run: `cd "$W" && go test ./fsm/ -run 'SnapshotStaysPreV18|SnapshotV18|PreV18|LaneCountsMustMatch' -v`
Expected: build failure (`undefined: snapshotVersionV18`).

- [ ] **Step 2: Implement v18**

In `fsm/snapshot.go`:
- Constants: `snapshotVersion = 18`, add `snapshotVersionV18 = 18`, keep `snapshotVersionV17 = 17`. Extend the `snapshotVersion` doc comment: "v18 once `Params.ClientLanes` is committed (client_seq lanes, spec 2026-10-09 §5.4); a pre-lane binary must refuse such a container because it would drop `client_lanes` and `lane_counts` on its next snapshot."
- `snapshotDoc`: add `LaneCounts map[string]uint32 \`json:"lane_counts,omitempty"\``; `snapshotDocument`: `LaneCounts: st.LaneCounts,`.
- `snapshotWriteVersion`: first line `if st.Params.ClientLanes != nil { return snapshotVersionV18 }`, and update its comment.
- `readSnapshot`: find every version chain with `git grep -n 'ver\[0\] == snapshotVersion ||' fsm/snapshot.go` (four sites: the acceptance check, two document-shape checks, and the artifact-disposition restore) and add `ver[0] == snapshotVersionV17 ||` immediately after `ver[0] == snapshotVersion ||` in each, and the matching `%d` + `snapshotVersionV17` in the acceptance error message. A v17 container must keep restoring exactly as before.
- After the v17 refusal block, add:

```go
	// Client lanes were added in v18. An older container cannot have
	// committed the parameter, a lane count or a laned subject, and an older
	// binary would drop them on its next snapshot.
	if ver[0] < snapshotVersionV18 {
		if doc.Params.ClientLanes != nil || len(doc.LaneCounts) != 0 || laneSubjectsPresent(acc) {
			return nil, fmt.Errorf("client lanes are present in a pre-v18 container")
		}
	}
	if doc.LaneCounts != nil {
		st.LaneCounts = doc.LaneCounts
	}
	if err := validateLaneCounts(st); err != nil {
		return nil, fmt.Errorf("snapshot: %w", err)
	}
```

In `fsm/client_lanes.go` add (imports `fmt`, `maps`, `strings`, `accumulator`):

```go
// laneSubjectsPresent reports whether the accumulator holds any laned subject.
func laneSubjectsPresent(acc *accumulator.SpentIDs) bool {
	for _, subject := range acc.Subjects() {
		if strings.Contains(subject, ":") {
			return true
		}
	}
	return false
}

// validateLaneCounts re-derives the per-account lane count from the
// accumulator's laned subjects and requires lane_counts to equal it exactly:
// LaneCounts is derived state, and a forged count would grant or deny an
// account lanes on one replica only.
func validateLaneCounts(st *State) error {
	derived := map[string]uint32{}
	for _, subject := range st.SpentIDs.Subjects() {
		account, lane, laned := strings.Cut(subject, ":")
		if !laned {
			continue
		}
		if !arbiter.ValidClientLane(lane) {
			return fmt.Errorf("spent-ids subject %q carries an invalid client lane", subject)
		}
		derived[account]++
	}
	if len(derived) != 0 && st.Params.ClientLanes == nil {
		return fmt.Errorf("spent ids hold client lanes but client_lanes is not committed")
	}
	if !maps.Equal(derived, st.LaneCounts) {
		return fmt.Errorf("lane_counts do not match the accumulator's client lanes")
	}
	for account, held := range derived {
		if held > st.Params.ClientLanes.MaxLanesPerAccount {
			return fmt.Errorf("account %s holds %d client lanes, above max_lanes_per_account %d", account, held, st.Params.ClientLanes.MaxLanesPerAccount)
		}
	}
	return nil
}
```

- [ ] **Step 3: Run the suite**

Run: `cd "$W" && bazel test //fsm:all //server:all //cmd/...`
Expected: all PASS; `TestDeterminism_SameLogSameState` still asserts `snapshotVersionV17` for its lane-free script and PASSES; `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` PASSES with version byte 15.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add fsm && git commit -m "feat(snapshot): v18 container only after client lanes are committed

Carries lane_counts and Params.client_lanes; restore refuses lane state
under an older version byte and any lane_counts the accumulator does not
reproduce, so an un-upgraded follower keeps installing snapshots until
activation and refuses them afterwards (housegate spec 2026-10-09 §5.4).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 9: arbiter — feature book, `GetProtocolInfo.features`, `GetNodeFeatures`, post-activation registration refusal (B3)

**Files:**
- Create: `server/features.go`, `server/features_test.go`
- Modify: `server/server.go` (`Server` gets `features *featureBook`; `New`; `OnLeadershipLost`)
- Modify: `server/membership.go` (`RegisterNode`)
- Modify: `server/consensus_admin.go` (`GetProtocolInfo`; new `GetNodeFeatures`)
- Modify: `raftnode/node.go` (`VoterIDs`), `raftnode/node_test.go`

**Interfaces:**
- Consumes: Task 4 `arbiter.ClientLanesFeature`, `arbiter.LocalNodeFeatures()`; Task 6 `FSM.ClientLanesEnabled()`; existing `FSM.PurgeNodeSet()` (every registered, non-evicted SNode and verifier, sorted) and `Server.consensusReadBarrier(ctx)`.
- Produces: `type featureBook` with `record(nodeID string, features []string, at time.Time)`, `get(nodeID string) (featureEntry, bool)`, `clear()`; `func (n *raftnode.Node) VoterIDs() ([]string, error)`; optional interface `voterLister{ VoterIDs() ([]string, error) }` checked on `Deps.Node`; RPC `ConsensusAdmin.GetNodeFeatures`; `RegisterNode` refusal `FailedPrecondition "client lanes are active; this binary does not support them"`.

- [ ] **Step 1: Write the failing tests**

Create `server/features_test.go`:

```go
package server

import (
	"context"
	"slices"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

type voterNode struct {
	*fakeNode
	voters []string
}

func (n voterNode) VoterIDs() ([]string, error) { return slices.Clone(n.voters), nil }

func featureServer(t *testing.T) (*Server, *fakeNode) {
	t.Helper()
	node := &fakeNode{f: newServerTestFSM(t), leader: true}
	publishServerGenesis(t, node)
	s := New(Deps{Node: voterNode{fakeNode: node, voters: []string{"arb-0", "arb-1", "arb-2"}}, FSM: node.f,
		Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second, MaxStatementAge: time.Hour}})
	return s, node
}

func verifierRequest(id string, seed byte, features ...string) *pb.NodeRegistration {
	pub := make([]byte, 32)
	pub[0] = seed
	return &pb.NodeRegistration{NodeId: id, Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}, Ed25519Pubkey: pub, Features: features}
}

func TestGetProtocolInfoReportsFeatures(t *testing.T) {
	s, _ := featureServer(t)
	info, err := (&consensusAdminService{s: s}).GetProtocolInfo(context.Background(), &emptypb.Empty{})
	if err != nil || !slices.Contains(info.GetFeatures(), arbiter.ClientLanesFeature) {
		t.Fatalf("GetProtocolInfo = %v, %v", info, err)
	}
}

func TestRegisterNodeRecordsFeaturesOutsideReplicatedState(t *testing.T) {
	s, _ := featureServer(t)
	m := &membershipService{s: s}
	if _, err := m.RegisterNode(context.Background(), verifierRequest("v1", 1, arbiter.ClientLanesFeature)); err != nil {
		t.Fatal(err)
	}
	if _, err := m.RegisterNode(context.Background(), &pb.NodeRegistration{NodeId: "s1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}}); err != nil {
		t.Fatal(err)
	}
	got, err := (&consensusAdminService{s: s}).GetNodeFeatures(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.GetRaftVoterIds(), []string{"arb-0", "arb-1", "arb-2"}) || len(got.GetEntries()) != 2 {
		t.Fatalf("GetNodeFeatures = %v", got)
	}
	for _, e := range got.GetEntries() {
		want := e.GetNodeId() == "v1"
		if slices.Contains(e.GetFeatures(), arbiter.ClientLanesFeature) != want || (want && e.GetRegisteredUnix() == 0) {
			t.Fatalf("entry %v", e)
		}
	}
	// Replicated state stores the four registration fields only.
	if reg := wire.RegistrationToPB(wire.RegistrationFromPB(verifierRequest("v1", 1, arbiter.ClientLanesFeature))); len(reg.GetFeatures()) != 0 {
		t.Fatal("features leaked into the replicated registration")
	}
}

func TestFeatureBookIsNotRecordedOnAFailedProposal(t *testing.T) {
	s, node := featureServer(t)
	node.leader = false
	if _, err := (&membershipService{s: s}).RegisterNode(context.Background(), verifierRequest("v1", 1, arbiter.ClientLanesFeature)); err == nil {
		t.Fatal("a follower accepted a proposal")
	}
	if _, ok := s.features.get("v1"); ok {
		t.Fatal("a registration that was never applied entered the feature book")
	}
}

func TestFeatureBookClearedOnLeadershipLoss(t *testing.T) {
	s, _ := featureServer(t)
	if _, err := (&membershipService{s: s}).RegisterNode(context.Background(), verifierRequest("v1", 1, arbiter.ClientLanesFeature)); err != nil {
		t.Fatal(err)
	}
	s.OnLeadershipLost()
	got, err := (&consensusAdminService{s: s}).GetNodeFeatures(context.Background(), &emptypb.Empty{})
	if err != nil || len(got.GetEntries()) != 1 || len(got.GetEntries()[0].GetFeatures()) != 0 {
		t.Fatalf("after leadership loss = %v, %v; the gate must see an empty book until v1 re-registers", got, err)
	}
}

func TestGetNodeFeaturesRequiresTheVoterConfiguration(t *testing.T) {
	node := &fakeNode{f: newServerTestFSM(t), leader: true}
	publishServerGenesis(t, node)
	s := New(Deps{Node: node, FSM: node.f, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second}})
	if _, err := (&consensusAdminService{s: s}).GetNodeFeatures(context.Background(), &emptypb.Empty{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("err = %v, want FailedPrecondition", err)
	}
}

func TestRegistrationWithoutLaneSupportIsRefusedAfterActivation(t *testing.T) {
	s, node := featureServer(t)
	activateServerClientLanes(t, node, 256)
	before := node.appliedCount()
	_, err := (&membershipService{s: s}).RegisterNode(context.Background(), verifierRequest("v9", 9))
	if status.Code(err) != codes.FailedPrecondition || status.Convert(err).Message() != "client lanes are active; this binary does not support them" {
		t.Fatalf("err = %v", err)
	}
	if node.appliedCount() != before {
		t.Fatal("the refused registration was proposed")
	}
	if _, err := (&membershipService{s: s}).RegisterNode(context.Background(), verifierRequest("v9", 9, arbiter.ClientLanesFeature)); err != nil {
		t.Fatalf("a lane-aware registration after activation = %v", err)
	}
}
```

Add the shared helper to `server/client_lanes_test.go`:

```go
// activateServerClientLanes commits the table registry and then client lanes
// with budget max, both signed by serverTestAuthority.
func activateServerClientLanes(t *testing.T, node *fakeNode, max uint32) {
	t.Helper()
	update := func(lanes *arbiter.ClientLaneParams) {
		view, err := node.f.ConsensusParamsView()
		if err != nil {
			t.Fatal(err)
		}
		u := arbiter.ConsensusParamsUpdate{
			NetworkID: view.Current.NetworkID, GenesisSnapshotID: view.GenesisSnapshotID, ExpectedEpoch: view.Epoch,
			PreviousParamsDigest: view.ParamsDigest, AuthorityAddresses: view.Current.AuthorityAddresses, MaxWriters: 1,
			ExpectedPromotionSeq: view.PromotionSeq,
			TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1",
				SIIndexerID: 1, ActivationBlock: 100, Confirmation: arbiter.TableRegistryConfirmationSafe},
			ClientLanes: lanes,
		}
		token, err := serverTestAuthority.SignConsensusParamsUpdateAt(u, 1)
		if err != nil {
			t.Fatal(err)
		}
		mustDirectApply(t, node, wire.Command{UpdateConsensusParams: &wire.UpdateConsensusParams{Update: u, AuthorityJWS: token}})
	}
	update(nil)
	update(&arbiter.ClientLaneParams{MaxLanesPerAccount: max})
	if !node.f.ClientLanesEnabled() {
		t.Fatal("client lanes not committed")
	}
}
```

Append to `raftnode/node_test.go` (reuse the file's existing single-node construction if it has one; otherwise this inline one):

```go
func TestVoterIDsListsTheConfiguration(t *testing.T) {
	store := raft.NewInmemStore()
	addr, transport := raft.NewInmemTransport("")
	cfg := raft.DefaultConfig()
	cfg.LocalID = "n1"
	n, err := New(Options{NodeID: "n1", FSM: &raft.MockFSM{}, LogStore: store, StableStore: store,
		SnapshotStore: raft.NewInmemSnapshotStore(), Transport: transport, RaftConfig: cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Shutdown() })
	if err := n.Bootstrap([]raft.Server{{ID: "n1", Address: addr}}); err != nil {
		t.Fatal(err)
	}
	ids, err := n.VoterIDs()
	if err != nil || len(ids) != 1 || ids[0] != "n1" {
		t.Fatalf("VoterIDs = %v, %v", ids, err)
	}
}
```

(`raftnode.Options` field names are those `cmd/arbiter/raft.go` uses; if `New` requires a non-nil `RaftConfig` with timeouts, copy `raft.DefaultConfig()` as above.)

Run: `cd "$W" && go test ./server/ ./raftnode/ -run 'Feature|ProtocolInfoReportsFeatures|RegistrationWithoutLaneSupport|VoterIDs' -v`
Expected: build failure (`s.features undefined`, `GetNodeFeatures undefined`, `VoterIDs undefined`).

- [ ] **Step 2: Implement**

Create `server/features.go`:

```go
package server

import (
	"context"
	"slices"
	"sync"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core"
)

// featureBook records the capability strings each data-plane node advertised
// in its latest RegisterNode applied through this voter (housegate spec
// 2026-10-09 §5.6). It is process-local and never replicated: a feature in a
// Raft command would be refused by every voter that predates the field, while
// voters that know it would apply the command. Only the leader accepts
// proposals, and the book is cleared when leadership is lost, so every entry
// is that node's latest registration during the current term.
type featureBook struct {
	mu      sync.Mutex
	entries map[string]featureEntry
}

type featureEntry struct {
	features   []string
	registered time.Time
}

func newFeatureBook() *featureBook { return &featureBook{entries: map[string]featureEntry{}} }

func (b *featureBook) record(nodeID string, features []string, at time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries[nodeID] = featureEntry{features: slices.Clone(features), registered: at}
}

func (b *featureBook) get(nodeID string) (featureEntry, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	e, ok := b.entries[nodeID]
	e.features = slices.Clone(e.features)
	return e, ok
}

func (b *featureBook) clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entries = map[string]featureEntry{}
}

// voterLister is implemented by raftnode.Node; fakes may omit it.
type voterLister interface {
	VoterIDs() ([]string, error)
}

// GetNodeFeatures answers under the leader barrier GetConsensusParams uses:
// one entry per non-evicted verifier and SNode registration in replicated
// state, joined with this leader's feature book, plus the Raft voter ids so
// arbiter-admin can prove it probed every voter.
func (svc *consensusAdminService) GetNodeFeatures(ctx context.Context, _ *emptypb.Empty) (*pb.NodeFeatures, error) {
	if err := svc.s.consensusReadBarrier(ctx); err != nil {
		return nil, err
	}
	lister, ok := svc.s.d.Node.(voterLister)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, "raft voter configuration is unavailable on this node")
	}
	voters, err := lister.VoterIDs()
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "read raft voter configuration: %v", err)
	}
	out := &pb.NodeFeatures{RaftVoterIds: voters}
	for _, id := range svc.s.d.FSM.PurgeNodeSet() {
		entry := &pb.NodeFeatureEntry{NodeId: id}
		if e, ok := svc.s.features.get(id); ok {
			entry.Features = e.features
			entry.RegisteredUnix = e.registered.Unix()
		}
		out.Entries = append(out.Entries, entry)
	}
	return out, nil
}

// isDataPlaneRegistration reports a verifier or SNode role.
func isDataPlaneRegistration(req *pb.NodeRegistration) bool {
	for _, role := range req.GetRoles() {
		if role == pb.NodeRole_NODE_ROLE_VERIFIER || role == pb.NodeRole_NODE_ROLE_SNODE {
			return true
		}
	}
	return false
}

func supportsClientLanes(features []string) bool {
	return slices.Contains(features, arbiter.ClientLanesFeature)
}
```

`server/server.go`: add `features *featureBook` to `Server`, `features: newFeatureBook(),` in `New`, and in `OnLeadershipLost` call `s.features.clear()` before `s.CloseStreams()`.

`server/membership.go`:

```go
func (svc *membershipService) RegisterNode(ctx context.Context, req *pb.NodeRegistration) (*pb.Ack, error) {
	// After activation the data plane stays on the lane-aware release line
	// (housegate spec 2026-10-09 §7): a lane-unaware SNode refuses laned ids
	// (a write outage on that node), and a verifier is kept on the tested
	// release even though it takes flat ids opaquely from the leader. This is
	// a gateway refusal read from applied state, not a replicated rule, so it
	// cannot diverge.
	if isDataPlaneRegistration(req) && svc.s.d.FSM.ClientLanesEnabled() && !supportsClientLanes(req.GetFeatures()) {
		return nil, status.Error(codes.FailedPrecondition, "client lanes are active; this binary does not support them")
	}
	_, err := svc.s.propose(ctx, wire.Command{
		RegisterNode: &wire.RegisterNode{Registration: wire.RegistrationFromPB(req)},
	})
	if err != nil {
		return nil, err
	}
	// Recorded only after the registration applied, outside replicated state.
	svc.s.features.record(req.GetNodeId(), req.GetFeatures(), time.Now())
	return &pb.Ack{}, nil
}
```

(imports `time`, `codes`, `status`).

`server/consensus_admin.go` `GetProtocolInfo`: add `Features: arbiter.LocalNodeFeatures(),`.

`raftnode/node.go`:

```go
// VoterIDs returns the server ids of the current Raft configuration's voters,
// sorted. arbiter-admin's client-lane activation gate compares it with the
// voters it probed.
func (n *Node) VoterIDs() ([]string, error) {
	future := n.Raw().GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}
	var ids []string
	for _, srv := range future.Configuration().Servers {
		if srv.Suffrage == raft.Voter {
			ids = append(ids, string(srv.ID))
		}
	}
	slices.Sort(ids)
	return ids, nil
}
```

- [ ] **Step 3: Run the tests**

Run: `cd "$W" && bazel test //server:all //raftnode:all //cmd/arbiter:all`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add server raftnode && git commit -m "feat(server): non-replicated feature book, GetNodeFeatures, post-activation registration refusal

Voters report client_lanes_v1 in GetProtocolInfo; the leader records each
data-plane node's advertised features after its registration applies and
clears them on leadership loss; GetNodeFeatures joins them with the
registered nodes and the Raft voter ids; after activation a lane-unaware
verifier or SNode registration is refused before proposing (housegate
spec 2026-10-09 §5.6).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 10: arbiter — `arbiter-admin consensus update --client-lanes-max-per-account` and the activation gate (B3)

**Files:**
- Modify: `cmd/arbiter-admin/main.go` (usage, flags, `update` path; new `features` operation)
- Create: `cmd/arbiter-admin/client_lanes_test.go`

**Interfaces:**
- Consumes: Task 1 `pb.ConsensusAdminClient.GetNodeFeatures`, `pb.ProtocolInfo.Features`, `pb.ConsensusMutableParams.ClientLanes`; Task 3 `arbiter.ClientLaneParams`, `wire.ClientLaneParamsFromPB`; Task 4 `arbiter.ClientLanesFeature`.
- Produces: CLI `consensus update … [--client-lanes-max-per-account N --voter HOST:PORT …]`, `consensus features --address LEADER`; exported-for-test `func clientLanesGate(ctx context.Context, leader pb.ConsensusAdminClient, voters []string, dial func(string) (pb.ConsensusAdminClient, func(), error)) error`.

Rules: omitted flag → carry `state.current.client_lanes` unchanged (including absent). Given flag → `N ≥ 1`; `N` below the current value is refused locally; `N` equal to the current value is a carry (no gate); a set or raise requires `--voter` for every voter, the table registry enabled, and the gate. Drain preconditions are not visible over gRPC; the FSM refuses an undrained update atomically and the CLI reports that refusal (`InvalidArgument`).

- [ ] **Step 1: Write the failing tests**

Create `cmd/arbiter-admin/client_lanes_test.go`:

```go
package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core"
)

const gateTestKey = "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"

type fakeAdmin struct {
	pb.UnimplementedConsensusAdminServer
	nodeID   string
	features []string
	state    *pb.ConsensusParamsState
	book     *pb.NodeFeatures
	mu       sync.Mutex
	updates  []*pb.UpdateConsensusParamsCmd
}

func (f *fakeAdmin) GetProtocolInfo(context.Context, *emptypb.Empty) (*pb.ProtocolInfo, error) {
	return &pb.ProtocolInfo{NodeId: f.nodeID, ProtocolVersion: arbiter.ConsensusAdminProtocolVersion, UpdatesEnabled: true, Features: f.features}, nil
}
func (f *fakeAdmin) GetConsensusParams(context.Context, *emptypb.Empty) (*pb.ConsensusParamsState, error) {
	return f.state, nil
}
func (f *fakeAdmin) GetNodeFeatures(context.Context, *emptypb.Empty) (*pb.NodeFeatures, error) {
	return f.book, nil
}
func (f *fakeAdmin) UpdateConsensusParams(_ context.Context, req *pb.UpdateConsensusParamsCmd) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updates = append(f.updates, req)
	return &pb.Ack{}, nil
}

func serveAdmin(t *testing.T, f *fakeAdmin) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	pb.RegisterConsensusAdminServer(g, f)
	go func() { _ = g.Serve(ln) }()
	t.Cleanup(g.Stop)
	return ln.Addr().String()
}

func gateFixture(t *testing.T, current *pb.ClientLaneParams) (leader *fakeAdmin, leaderAddr string, voterAddrs []string) {
	t.Helper()
	t.Setenv(authorityPrivateKeyEnv, gateTestKey)
	state := &pb.ConsensusParamsState{
		ProtocolVersion: arbiter.ConsensusAdminProtocolVersion, NetworkId: "devnet2", GenesisSnapshotId: "0xgenesis",
		Epoch: 1, ParamsDigest: "0xdigest", PromotionSeq: 3,
		Current: &pb.ConsensusMutableParams{AuthorityAddresses: []string{"0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705"}, MaxWriters: 1,
			TableRegistry: &pb.TableRegistryParams{ChainId: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1", ActivationBlock: 100, Confirmation: "safe"},
			ClientLanes:   current},
	}
	book := &pb.NodeFeatures{RaftVoterIds: []string{"arb-0", "arb-1", "arb-2"}, Entries: []*pb.NodeFeatureEntry{
		{NodeId: "s1", Features: []string{arbiter.ClientLanesFeature}, RegisteredUnix: 1},
		{NodeId: "v1", Features: []string{arbiter.ClientLanesFeature}, RegisteredUnix: 1},
	}}
	for i, id := range []string{"arb-0", "arb-1", "arb-2"} {
		f := &fakeAdmin{nodeID: id, features: []string{arbiter.ClientLanesFeature}, state: state, book: book}
		addr := serveAdmin(t, f)
		voterAddrs = append(voterAddrs, addr)
		if i == 0 {
			leader, leaderAddr = f, addr
		}
	}
	return leader, leaderAddr, voterAddrs
}

func updateArgs(leader string, extra ...string) []string {
	return append([]string{"consensus", "update", "--address", leader, "--expected-epoch", "1",
		"--authority", "0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705", "--max-writers", "1"}, extra...)
}

func voterArgs(addrs []string) []string {
	var out []string
	for _, a := range addrs {
		out = append(out, "--voter", a)
	}
	return out
}

func TestUpdateCarriesClientLanesForward(t *testing.T) {
	leader, addr, _ := gateFixture(t, &pb.ClientLaneParams{MaxLanesPerAccount: 256})
	if err := run(context.Background(), updateArgs(addr), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if got := leader.updates[0].GetUpdate().GetClientLanes(); got.GetMaxLanesPerAccount() != 256 {
		t.Fatalf("an update without --client-lanes-max-per-account dropped client_lanes: %v", got)
	}
}

func TestActivationPassesTheGate(t *testing.T) {
	leader, addr, voters := gateFixture(t, nil)
	args := updateArgs(addr, append([]string{"--client-lanes-max-per-account", "256"}, voterArgs(voters)...)...)
	if err := run(context.Background(), args, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if got := leader.updates[0].GetUpdate().GetClientLanes().GetMaxLanesPerAccount(); got != 256 {
		t.Fatalf("submitted client_lanes = %d", got)
	}
}

func TestGateRefusesAVoterWithoutTheFeature(t *testing.T) {
	leader, addr, voters := gateFixture(t, nil)
	old := &fakeAdmin{nodeID: "arb-2", state: leader.state, book: leader.book}
	voters[2] = serveAdmin(t, old)
	err := run(context.Background(), updateArgs(addr, append([]string{"--client-lanes-max-per-account", "256"}, voterArgs(voters)...)...), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "arb-2") || len(leader.updates) != 0 {
		t.Fatalf("err = %v, updates = %d", err, len(leader.updates))
	}
}

func TestGateRefusesAMissingVoter(t *testing.T) {
	leader, addr, voters := gateFixture(t, nil)
	err := run(context.Background(), updateArgs(addr, append([]string{"--client-lanes-max-per-account", "256"}, voterArgs(voters[:2])...)...), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "arb-2") || len(leader.updates) != 0 {
		t.Fatalf("err = %v", err)
	}
}

func TestGateRefusesNodeMissingFromFeatureBook(t *testing.T) {
	leader, addr, voters := gateFixture(t, nil)
	leader.book.Entries[1].Features = nil // v1 registered with a previous leader
	err := run(context.Background(), updateArgs(addr, append([]string{"--client-lanes-max-per-account", "256"}, voterArgs(voters)...)...), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "v1") || len(leader.updates) != 0 {
		t.Fatalf("err = %v", err)
	}
}

func TestGateRequiresTheTableRegistry(t *testing.T) {
	leader, addr, voters := gateFixture(t, nil)
	leader.state.Current.TableRegistry = nil
	err := run(context.Background(), updateArgs(addr, append([]string{"--client-lanes-max-per-account", "256"}, voterArgs(voters)...)...), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "table registry") {
		t.Fatalf("err = %v", err)
	}
}

func TestClientLanesCannotBeLoweredLocally(t *testing.T) {
	_, addr, voters := gateFixture(t, &pb.ClientLaneParams{MaxLanesPerAccount: 256})
	err := run(context.Background(), updateArgs(addr, append([]string{"--client-lanes-max-per-account", "255"}, voterArgs(voters)...)...), &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "cannot lower") {
		t.Fatalf("err = %v", err)
	}
	if err := run(context.Background(), updateArgs(addr, "--client-lanes-max-per-account", "0"), &bytes.Buffer{}); err == nil {
		t.Fatal("0 must be refused")
	}
}

func TestFeaturesOperationPrintsTheBook(t *testing.T) {
	_, addr, _ := gateFixture(t, nil)
	var out bytes.Buffer
	if err := run(context.Background(), []string{"consensus", "features", "--address", addr}, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"raft_voter_ids"`) || !strings.Contains(out.String(), arbiter.ClientLanesFeature) {
		t.Fatalf("output = %s", out.String())
	}
}
```

Run: `cd "$W" && go test ./cmd/arbiter-admin/ -run 'ClientLanes|Gate|Activation|FeaturesOperation' -v`
Expected: FAIL (`flag provided but not defined: -client-lanes-max-per-account`, `unknown operation features`).

- [ ] **Step 2: Implement**

In `cmd/arbiter-admin/main.go`:
- Usage: add `[--client-lanes-max-per-account N --voter VOTER_GRPC …]` to the `update` line, a `arbiter-admin consensus features --address LEADER_GRPC [--timeout 10s]` line, and a paragraph: "--client-lanes-max-per-account sets or raises the client_seq lane budget; omitted, the current value is carried. Setting or raising it requires --voter for every Raft voter (direct addresses): every voter must report client_lanes_v1, the leader's GetNodeFeatures must list client_lanes_v1 for every registered verifier and SNode, and the table registry must be enabled. Lanes cannot be lowered or removed; voter, verifier and SNode images must never be downgraded afterwards."
- Accept `features` in the operation switch; for it, after dialing, `nf, err := client.GetNodeFeatures(ctx, &emptypb.Empty{})` → `writeProtoJSON(out, nf)`.
- In the `update` flag block: `fs.Uint64Var(&clientLanesMax, "client-lanes-max-per-account", 0, "...")` and `fs.Var(&voters, "voter", "direct gRPC address of a Raft voter; repeat for every voter (required to set or raise client lanes)")` (reuse the `authorities` string-slice type for `voters`); record `clientLanesSet := present["client-lanes-max-per-account"]`; refuse `clientLanesSet && (clientLanesMax == 0 || clientLanesMax > math.MaxUint32)` with `--client-lanes-max-per-account must be between 1 and 4294967295`.
- After `tableRegistryTarget` is computed:

```go
	// Absent, carry the committed client lanes unchanged: the FSM refuses an
	// update that drops them once set.
	clientLanesTarget := wire.ClientLaneParamsFromPB(state.GetCurrent().GetClientLanes())
	if clientLanesSet {
		requested := &arbiter.ClientLaneParams{MaxLanesPerAccount: uint32(clientLanesMax)}
		current := clientLanesTarget
		if current != nil && requested.MaxLanesPerAccount < current.MaxLanesPerAccount {
			return fmt.Errorf("client lanes cannot lower max_lanes_per_account (%d -> %d)", current.MaxLanesPerAccount, requested.MaxLanesPerAccount)
		}
		if current == nil || requested.MaxLanesPerAccount > current.MaxLanesPerAccount {
			if tableRegistryTarget == nil {
				return errors.New("client lanes require the table registry; enable it first (or in the same update)")
			}
			if err := clientLanesGate(ctx, client, voters, dialAdmin); err != nil {
				return err
			}
		}
		clientLanesTarget = requested
	}
```

and pass `ClientLanes: clientLanesTarget,` to `authority.NormalizeConsensusParamsUpdate`.
- Add:

```go
func dialAdmin(address string) (pb.ConsensusAdminClient, func(), error) {
	conn, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return pb.NewConsensusAdminClient(conn), func() { _ = conn.Close() }, nil
}

// clientLanesGate is the operational activation gate of housegate spec
// 2026-10-09 §5.6. Node features never enter replicated state, so the FSM
// cannot check them; this gate refuses unless (1) every Raft voter, probed
// directly, reports client_lanes_v1 and the probed set equals the leader's
// voter configuration, and (2) every registered verifier and SNode advertised
// client_lanes_v1 to the current leader. It names every node to restart.
func clientLanesGate(ctx context.Context, leader pb.ConsensusAdminClient, voters []string,
	dial func(string) (pb.ConsensusAdminClient, func(), error)) error {
	if len(voters) == 0 {
		return errors.New("client lanes gate: pass --voter for every Raft voter")
	}
	book, err := leader.GetNodeFeatures(ctx, &emptypb.Empty{})
	if err != nil {
		return fmt.Errorf("client lanes gate: read the leader's feature book: %w", err)
	}
	probed := map[string]bool{}
	var problems []string
	for _, address := range voters {
		client, closeFn, err := dial(address)
		if err != nil {
			return fmt.Errorf("client lanes gate: connect to voter %s: %w", address, err)
		}
		info, err := client.GetProtocolInfo(ctx, &emptypb.Empty{})
		closeFn()
		if err != nil {
			return fmt.Errorf("client lanes gate: probe voter %s: %w", address, err)
		}
		probed[info.GetNodeId()] = true
		if !slices.Contains(info.GetFeatures(), arbiter.ClientLanesFeature) {
			problems = append(problems, "voter "+info.GetNodeId()+" ("+address+") lacks "+arbiter.ClientLanesFeature)
		}
	}
	for _, id := range book.GetRaftVoterIds() {
		if !probed[id] {
			problems = append(problems, "voter "+id+" was not probed (add its --voter address)")
		}
	}
	for _, e := range book.GetEntries() {
		if !slices.Contains(e.GetFeatures(), arbiter.ClientLanesFeature) {
			problems = append(problems, "data-plane node "+e.GetNodeId()+" has not registered with the current leader as lane-aware; restart it on a lane-aware release")
		}
	}
	if len(problems) > 0 {
		slices.Sort(problems)
		return fmt.Errorf("client lanes gate refused:\n  %s", strings.Join(problems, "\n  "))
	}
	return nil
}
```

(add `slices` to the imports; `voters` is an `authorities`-typed slice, convert with `[]string(voters)`.)

- [ ] **Step 3: Run the tests**

Run: `cd "$W" && bazel test //cmd/arbiter-admin:all`
Expected: all PASS, including the existing `main_test.go` and `consensus_integration_test.go`.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add cmd/arbiter-admin && git commit -m "feat(arbiter-admin): client lanes flag, carry-forward and activation gate

consensus update carries the committed client_lanes when the flag is
omitted, refuses lowering locally, and before setting or raising them
probes every voter for client_lanes_v1, matches the probed set to the
leader's voter configuration and requires every registered verifier and
SNode in the leader's feature book; consensus features prints the book
(housegate spec 2026-10-09 §5.6).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 11: arbiter — admission/lane metrics and `GetClientSeqState` (B3)

**Files:**
- Create: `server/admission_metrics.go`
- Modify: `server/server.go` (`Deps.AdmissionMetrics`), `server/ingress.go` (`SubmitStatement`), `server/safestate.go` (`GetClientSeqState`)
- Modify: `fsm/reads_metrics.go` (`MetricsSnapshot` lane fields), `fsm/client_lanes.go` (`ClientSeqState`)
- Modify: `metrics/metrics.go`; create `metrics/client_lanes_test.go`
- Modify: `cmd/arbiter/services.go` (wire `AdmissionMetrics`)
- Test: `server/client_lanes_test.go`, `fsm/admission_lanes_test.go`

**Interfaces:**
- Consumes: Task 7 `LaneCounts`; Task 5 `SpentIDs.Subjects`, `AccountState`.
- Produces: `server.AdmissionMetrics interface{ AdmissionRejected(code string) }`; `(*metrics.Metrics).AdmissionRejected(code string)`; `fsm.MetricsSnapshot.{ClientLanes uint64; AccountsAtLaneCap uint64; SubjectOpenGapRanges map[int]uint64}`; `func (f *FSM) ClientSeqState(account, lane string) (subject string, hi uint64, ranges []accumulator.SeqRange, found bool)`; RPC `SafeState.GetClientSeqState`.

- [ ] **Step 1: Write the failing tests**

Append to `server/client_lanes_test.go`:

```go
type recordingAdmissionMetrics struct{ codes []string }

func (r *recordingAdmissionMetrics) AdmissionRejected(code string) { r.codes = append(r.codes, code) }

func TestAdmissionRejectsAreCountedByCode(t *testing.T) {
	node := &fakeNode{f: newServerTestFSM(t), leader: true}
	publishServerGenesis(t, node)
	rec := &recordingAdmissionMetrics{}
	s := New(Deps{Node: node, FSM: node.f, AdmissionMetrics: rec, Cfg: Config{NodeID: "arb-0", ApplyTimeout: time.Second, MaxStatementAge: time.Hour}})
	env := testServerStatementEnvelope(t)
	env.StatementId.ClientLane = "5e1f0a2b7c9d3e4f"
	_, _ = (&ingressService{s: s}).SubmitStatement(context.Background(), env)
	if len(rec.codes) != 1 || rec.codes[0] != "ADMISSION_CODE_MALFORMED" {
		t.Fatalf("recorded %v", rec.codes)
	}
}

func TestGetClientSeqStateIsALocalRead(t *testing.T) {
	node := &fakeNode{f: newServerTestFSM(t), leader: false}
	s := New(Deps{Node: node, FSM: node.f, Cfg: Config{NodeID: "arb-1"}})
	got, err := (&safeStateService{s: s}).GetClientSeqState(context.Background(), &pb.GetClientSeqStateRequest{ClientAccount: "0xAA"})
	if err != nil || got.GetFound() || got.GetSubject() != "0xaa" {
		t.Fatalf("unknown subject on a follower = %v, %v", got, err)
	}
	if _, err := (&safeStateService{s: s}).GetClientSeqState(context.Background(), &pb.GetClientSeqStateRequest{ClientAccount: "0xaa", ClientLane: "XYZ"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid lane err = %v", err)
	}
}
```

Append to `fsm/admission_lanes_test.go`:

```go
func TestMetricsSnapshotCountsLanes(t *testing.T) {
	f, key, account := lanesFSM(t, 2)
	submit(t, f, laneEnvelope(t, f, key, account, testClientLaneA, 1))
	submit(t, f, laneEnvelope(t, f, key, account, testClientLaneB, 3))
	m := f.MetricsSnapshot()
	if m.ClientLanes != 2 || m.AccountsAtLaneCap != 1 || m.SubjectOpenGapRanges[1] != 1 || m.SubjectOpenGapRanges[0] != 1 {
		t.Fatalf("lanes %d at cap %d ranges %v", m.ClientLanes, m.AccountsAtLaneCap, m.SubjectOpenGapRanges)
	}
	subject, hi, ranges, found := f.ClientSeqState(account, testClientLaneB)
	if !found || subject != account+":"+testClientLaneB || hi != 3 || len(ranges) != 1 {
		t.Fatalf("ClientSeqState = %s %d %v %v", subject, hi, ranges, found)
	}
}
```

Create `metrics/client_lanes_test.go` asserting, through `testutil.ToFloat64` on the registry (`github.com/prometheus/client_golang/prometheus/testutil`), that `AdmissionRejected("ADMISSION_CODE_LANE_BUDGET_EXCEEDED")` increments `arbiter_admission_rejects_total{code="ADMISSION_CODE_LANE_BUDGET_EXCEEDED"}`, and that a `snapshotReader` fake returning `fsm.MetricsSnapshot{ClientLanes: 5, AccountsAtLaneCap: 1, SubjectOpenGapRanges: map[int]uint64{0: 3, 2: 1}}` yields `arbiter_client_lanes_total 5`, `arbiter_accounts_at_lane_cap 1` and an `arbiter_subject_open_gap_ranges` histogram with `_count 4` and `_sum 2`:

```go
package metrics

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/sentioxyz/arbiter/fsm"
)

type laneSnapshot struct{ m fsm.MetricsSnapshot }

func (s laneSnapshot) MetricsSnapshot() fsm.MetricsSnapshot { return s.m }

func TestClientLaneMetrics(t *testing.T) {
	m := New(laneSnapshot{fsm.MetricsSnapshot{ClientLanes: 5, AccountsAtLaneCap: 1, SubjectOpenGapRanges: map[int]uint64{0: 3, 2: 1}}})
	m.AdmissionRejected("ADMISSION_CODE_LANE_BUDGET_EXCEEDED")
	if got := testutil.ToFloat64(m.admissionRejects.WithLabelValues("ADMISSION_CODE_LANE_BUDGET_EXCEEDED")); got != 1 {
		t.Fatalf("admission rejects = %v", got)
	}
	want := `
# HELP arbiter_accounts_at_lane_cap Accounts holding max_lanes_per_account client lanes.
# TYPE arbiter_accounts_at_lane_cap gauge
arbiter_accounts_at_lane_cap 1
# HELP arbiter_client_lanes_total Distinct client lanes held across all accounts (sum of lane counts).
# TYPE arbiter_client_lanes_total gauge
arbiter_client_lanes_total 5
`
	if err := testutil.GatherAndCompare(m.registry, strings.NewReader(want), "arbiter_client_lanes_total", "arbiter_accounts_at_lane_cap"); err != nil {
		t.Fatal(err)
	}
	n, err := testutil.GatherAndCount(m.registry, "arbiter_subject_open_gap_ranges")
	if err != nil || n != 1 {
		t.Fatalf("histogram series = %d, %v", n, err)
	}
}
```

Run: `cd "$W" && go test ./server/ ./fsm/ ./metrics/ -run 'AdmissionRejects|ClientSeqState|MetricsSnapshotCountsLanes|ClientLaneMetrics' -v`
Expected: build failures for the new names.

- [ ] **Step 2: Implement**

`server/admission_metrics.go`:

```go
package server

// AdmissionMetrics counts SubmitStatement answers other than ACCEPTED by
// admission code name (ADMISSION_CODE_*), plus the gateway labels "fence"
// (a snapshot-query or table-set fence mapped to Unavailable) and
// "freshness" (a user_jws iat outside the window). Process-local, never
// replicated; nil disables it.
type AdmissionMetrics interface {
	AdmissionRejected(code string)
}

func (s *Server) reportAdmission(code string) {
	if s.d.AdmissionMetrics != nil {
		s.d.AdmissionMetrics.AdmissionRejected(code)
	}
}
```

`server/server.go` `Deps`: `AdmissionMetrics AdmissionMetrics` with a comment. In `server/ingress.go` `SubmitStatement`: in the pre-activation branch call `svc.s.reportAdmission(pb.AdmissionCode_ADMISSION_CODE_MALFORMED.String())`; when `validateStatementFreshness` fails call `svc.s.reportAdmission("freshness")`; inside each fence mapping call `svc.s.reportAdmission("fence")`; right after `submit, ok := res.(fsm.SubmitResult)` succeeds:

```go
	if submit.Code != arbiter.AdmissionCodeAccepted {
		svc.s.reportAdmission(pb.AdmissionCode(submit.Code).String())
	}
```

Also log duplicate and gap/lane-budget refusals with account, lane and seq at warn: `svc.s.d.Logger.Warn("statement admission refused", "code", pb.AdmissionCode(submit.Code).String(), "account", env.StatementID.ClientAccount, "lane", env.StatementID.ClientLane, "seq", env.StatementID.ClientSeq)` for codes `DUPLICATE_CLIENT_SEQ`, `GAP_BUDGET_EXCEEDED`, `LANE_BUDGET_EXCEEDED`.

`fsm/client_lanes.go`:

```go
// ClientSeqState is a local read of one subject's applied spent-id state.
func (f *FSM) ClientSeqState(account, lane string) (subject string, hi uint64, ranges []accumulator.SeqRange, found bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	subject = arbiter.StatementCoord{Account: strings.ToLower(account), Lane: lane}.Subject()
	hi, ranges, found = f.st.SpentIDs.AccountState(subject)
	return subject, hi, ranges, found
}
```

`fsm/reads_metrics.go`: add to `MetricsSnapshot`:

```go
	// ClientLanes is the sum of LaneCounts; AccountsAtLaneCap counts accounts
	// holding max_lanes_per_account lanes; SubjectOpenGapRanges maps an open
	// gap-range count to the number of subjects holding it.
	ClientLanes          uint64
	AccountsAtLaneCap    uint64
	SubjectOpenGapRanges map[int]uint64
```

and populate them in `MetricsSnapshot()` under the existing lock:

```go
	m.SubjectOpenGapRanges = map[int]uint64{}
	for _, held := range f.st.LaneCounts {
		m.ClientLanes += uint64(held)
		if lanes := f.st.Params.ClientLanes; lanes != nil && held >= lanes.MaxLanesPerAccount {
			m.AccountsAtLaneCap++
		}
	}
	for _, subject := range f.st.SpentIDs.Subjects() {
		_, ranges, _ := f.st.SpentIDs.AccountState(subject)
		m.SubjectOpenGapRanges[len(ranges)]++
	}
```

`server/safestate.go`:

```go
// GetClientSeqState is a local read of this voter's applied accumulator; it
// takes no leader barrier and may lag the leader.
func (svc *safeStateService) GetClientSeqState(_ context.Context, req *pb.GetClientSeqStateRequest) (*pb.ClientSeqState, error) {
	account := strings.ToLower(req.GetClientAccount())
	if !strings.HasPrefix(account, "0x") || len(account) < 3 {
		return nil, status.Error(codes.InvalidArgument, "client_account must be a 0x address")
	}
	if lane := req.GetClientLane(); lane != "" && !arbiter.ValidClientLane(lane) {
		return nil, status.Error(codes.InvalidArgument, "client_lane must be 16 lowercase hex characters")
	}
	subject, hi, ranges, found := svc.s.d.FSM.ClientSeqState(account, req.GetClientLane())
	out := &pb.ClientSeqState{Found: found, Subject: subject, Hi: hi}
	for _, r := range ranges {
		out.Ranges = append(out.Ranges, &pb.ClientSeqRange{Start: r.Start, End: r.End})
	}
	return out, nil
}
```

`metrics/metrics.go`: add `admissionRejects *prometheus.CounterVec` (`arbiter_admission_rejects_total`, help "SubmitStatement answers other than ACCEPTED on this node's gateway, by admission code name or by the gateway label fence or freshness.", label `code`), register it, and `func (m *Metrics) AdmissionRejected(code string) { m.admissionRejects.WithLabelValues(code).Inc() }`. In `collector` add descriptors `clientLanes` (`arbiter_client_lanes_total`, "Distinct client lanes held across all accounts (sum of lane counts).") and `atLaneCap` (`arbiter_accounts_at_lane_cap`, "Accounts holding max_lanes_per_account client lanes.") and `gapRanges` (`arbiter_subject_open_gap_ranges`, "Open gap ranges per accumulator subject, sampled at scrape time."); describe them; in `Collect`:

```go
	emit(c.clientLanes, m.ClientLanes)
	emit(c.atLaneCap, m.AccountsAtLaneCap)
	buckets := map[float64]uint64{}
	var count uint64
	var sum float64
	bounds := []float64{0, 1, 2, 4, 8, 16, 32, 64}
	for ranges, subjects := range m.SubjectOpenGapRanges {
		count += subjects
		sum += float64(ranges) * float64(subjects)
		for _, b := range bounds {
			if float64(ranges) <= b {
				buckets[b] += subjects
			}
		}
	}
	ch <- prometheus.MustNewConstHistogram(c.gapRanges, count, sum, buckets)
```

`cmd/arbiter/services.go`: next to `EvidenceMetrics: rt.evidenceMetrics,` add `AdmissionMetrics: rt.admissionMetrics,`, where `rt.admissionMetrics` is set from the same `*metrics.Metrics` that already backs `evidenceMetrics` (follow how `evidenceMetrics` is assigned in `cmd/arbiter/metrics.go`/`app.go`, and leave it nil when metrics are disabled).

- [ ] **Step 3: Run the tests**

Run: `cd "$W" && bazel test //server:all //fsm:all //metrics:all //cmd/arbiter:all`
Expected: all PASS.

- [ ] **Step 4: Commit**

```bash
cd "$W" && git add server fsm metrics cmd/arbiter && git commit -m "feat(metrics): admission rejects by code, client lane gauges, GetClientSeqState

arbiter_admission_rejects_total{code} at the gateway (fence and freshness
labelled separately), arbiter_client_lanes_total,
arbiter_accounts_at_lane_cap, arbiter_subject_open_gap_ranges, and a local
SafeState read of one subject's spent-id state (housegate spec 2026-10-09
§6.8, §5.2).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 12: arbiter — documentation, full verification, release, offline replay-check binaries (B3)

**Files:**
- Modify: `README.md` (new "Client lanes" section after the consensus administration section)
- Local only, never committed: `$R/arbiter-old`, `$R/arbiter-new` worktrees, `$R/statedigest/main.go`, `$R/bin/{old,new}/statedigest` with `R="$HOME/si-devnet2-lanes-rehearsal"`

**Interfaces:**
- Consumes: Tasks 5–11.
- Produces: `ARBITER_TAG`, `ARBITER_DIGEST`; `$R/bin/old/statedigest` (arbiter `v0.9.0`, the release devnet2 voters run) and `$R/bin/new/statedigest` (`ARBITER_TAG`) for Task 20.

- [ ] **Step 1: README**

Add a "Client lanes" section (one paragraph per line, no hard wraps) covering: what a lane is (fourth segment of the flat id, accumulator subject `account:lane`, legacy ids unchanged forever); the parameter (`client_lanes.max_lanes_per_account`, set after the table registry, raise-only, never removed, initial value 256 on devnet2); `LANE_BUDGET_EXCEEDED` (9); snapshot v18 written only after activation; the activation order of housegate spec 2026-10-09 §8.3 (all voters lane-aware → all verifiers and SNodes re-registered with the current leader → drain → `consensus update --client-lanes-max-per-account N --voter …` → after activation the leader refuses lane-unaware data-plane registrations); the `consensus features` read; the rule that no voter, verifier or SNode image may be downgraded below the lane-aware release after activation, and that agents fall back with `-si-lanes off`; the new metrics and `GetClientSeqState`. Extend the "Snapshot versions" list with "**v18** once `client_lanes` is committed".

- [ ] **Step 2: Full verification**

Run: `cd "$W" && bazel build //... && bazel test //... && go test ./accumulator/ -fuzz FuzzLanedSubjects -fuzztime 120s`
Expected: all PASS; in particular `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease`, `TestDeterminism_*`, the L3 goldens, `TestVectors`, `TestSharedStatementVectorsAreByteIdenticalToHousegate`, `TestVerifyUserJWSV2_SharedVectors`. Also run the integration package if docker is available: `bazel test //integration:all --test_output=errors`.

- [ ] **Step 3: Commit**

```bash
cd "$W" && git add README.md && git commit -m "docs(readme): client_seq lanes activation, gate and no-downgrade rule

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: PR and merge (requires explicit user confirmation)**

Push, `gh pr create --repo sentioxyz/arbiter --fill`; the PR description lists the consensus ordering of spec §8.3 and the strict-decoder evidence (`wire/legacy_command_golden_test.go` in arbiter-core, `fsm/legacy_lane_golden_test.go` here). Merge after CI and review.

- [ ] **Step 5: Build the offline replay-check binaries (local only)**

```bash
R="$HOME/si-devnet2-lanes-rehearsal"; mkdir -p "$R/bin/old" "$R/bin/new" "$R/statedigest"
git -C /Users/uranuswch/Dev/sentio_xyz/arbiter fetch -q --tags origin
git -C /Users/uranuswch/Dev/sentio_xyz/arbiter worktree list >/dev/null  # read-only sanity check
```

Create the two source trees with Worktrunk (never `git worktree add`): run the URWT command of Task 1 Step 1 for `/Users/uranuswch/Dev/sentio_xyz/arbiter` twice, with names `lanes-replay-old` (branch `urwt/claude/lanes-replay-old`, base `v0.9.0`) and `lanes-replay-new` (branch `urwt/claude/lanes-replay-new`, base `origin/main` after the merge). Create `$R/statedigest/main.go` with this content and copy it to `cmd/statedigest/main.go` in both worktrees (never committed; delete both branches with `wt remove` only when the user asks after Task 20):

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

Build: `(cd <old worktree path> && go build -o "$R/bin/old/" ./cmd/statedigest) && (cd <new worktree path> && go build -o "$R/bin/new/" ./cmd/statedigest)` (paths from the URWT JSON). If `config.Config` in `v0.9.0` names a field differently (`TableSchemas()`, `Genesis.*`), fix the throwaway copy in that worktree only, mirroring `cmd/arbiter/genesis.go` and `cmd/arbiter/services.go:configuredFSMParams` of that tag. Expected: both binaries build. Rehearse on a local copy: run both against `$R/sample/raft` produced by the arbiter integration suite's data dir (or any local single-node run of `bazel run //cmd/arbiter -- -config configs/local.yaml` stopped after a few statements) and confirm both print identical `key`, `spent_ids` and `snapshot_sha256` lines and the same `written_version`.

- [ ] **Step 6: Release `ARBITER_TAG` (requires explicit user confirmation)**

Run the Cut Release workflow (`gh workflow run cut-release.yml --repo sentioxyz/arbiter --ref main`) and wait for `docker-push`; record `ARBITER_TAG` and the image index digest `ARBITER_DIGEST` (`docker buildx imagetools inspect ghcr.io/sentioxyz/arbiter:$ARBITER_TAG`, which must report OCI revision = the tag's commit).

---

## Task 13: housegate — laned statement-id grammar, proto conversion, `LANE_BUDGET_EXCEEDED`, ingress lanes gate (B4)

**Files:**
- Modify: `go.mod`, `go.sum` (arbiter-proto `PROTO_TAG`)
- Modify: `pkg/storageintegrity/statementid.go` (PA1), `pkg/storageintegrity/statementid_test.go`
- Modify: `pkg/storageintegrity/arbiter_proto.go` (`ArbiterStatementEnvelopeToProto`, `SubmitOutcomeFromSequencedAck`), `pkg/storageintegrity/intake.go` (`EnvelopeFromAdmission`), `pkg/storageintegrity/arbiter_proto_test.go`, `pkg/storageintegrity/admission_code_test.go`
- Modify: `pkg/plugins/storageintegrity/plugin.go` (`Config.ClientLanesEnabled`; the OnQuery statement-id check), `pkg/plugins/storageintegrity/plugin_test.go`
- Modify: `proxy.go` (`Options.StorageIntegrityClientLanes`), `build.go` (ingress construction, ~line 903)

**Interfaces:**
- Consumes: Task 1 `pb.StatementID.ClientLane`, `pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED`; PA1 `StatementID`, `ParseStatementID` (already both grammars), `ParseLegacyStatementID`, `IsLaned`, `ErrClientLanesNotEnabled`, the ingress helper `parseIngressStatementID`; PA2 marker (applied by Relay to every OnQuery refusal of a token-bearing query).
- Produces: laned ids accepted end to end (ingress, intake, arbiter conversion) once the host reports lanes enabled; `sicore.ValidClientLane(lane string) bool`; `sicore.ClientLaneHexLen = 16`; `sicore.AdmissionCodeLaneBudgetExceeded` (string); `storageintegrity.Config.ClientLanesEnabled func() bool`; `housegate.Options.StorageIntegrityClientLanes func() bool`; ingress refusal text `storage_integrity: client lanes are not enabled on this network`.

- [ ] **Step 1: Worktree and Plan A contract check**

Create the worktree (URWT command of Task 1 Step 1 with `/Users/uranuswch/Dev/housegate/housegate`). Then:

Run: `cd "$W" && git grep -n "func ParseStatementID\|func ParseLegacyStatementID\|func (id StatementID) Flat\|func (id StatementID) Subject\|func (id StatementID) IsLaned\|ErrClientLanesNotEnabled =\|func parseIngressStatementID\|func MarkSeqUnspent\|SeqUnspent" -- pkg/ | head -30`
Expected: PA1 and PA2 symbols present, and `ParseLegacyStatementID` called from `intake.go` (`EnvelopeFromAdmission`), `arbiter_proto.go` (`ArbiterStatementEnvelopeToProto`) and `parseIngressStatementID` — the three sites this task switches to `ParseStatementID`. If not, stop and reconcile with Plan A1 Task 1.

- [ ] **Step 2: Pin arbiter-proto `PROTO_TAG`** (recipe `.claude/skills/upgrade-dependency/SKILL.md`)

Run: `cd "$W" && go get "github.com/sentioxyz/arbiter-proto@$PROTO_TAG" && go mod tidy && bazel mod tidy && bazel run //:gazelle && go build ./... && bazel test //pkg/storageintegrity:all //pkg/plugins/storageintegrity:all`
Expected: PASS (additive proto). Commit `chore(deps): pin arbiter-proto $PROTO_TAG` with the trailer.

- [ ] **Step 3: Write the failing tests**

Append to `pkg/storageintegrity/statementid_test.go`:

```go
func TestParseStatementIDAcceptsTheLanedForm(t *testing.T) {
	id, err := ParseStatementID("0xabc:5e1f0a2b7c9d3e4f:42:9f1c")
	if err != nil {
		t.Fatal(err)
	}
	want := StatementID{Account: "0xabc", Lane: "5e1f0a2b7c9d3e4f", Seq: 42, Nonce: "9f1c"}
	if id != want || !id.IsLaned() || id.Flat() != "0xabc:5e1f0a2b7c9d3e4f:42:9f1c" || id.Subject() != "0xabc:5e1f0a2b7c9d3e4f" {
		t.Fatalf("parsed %+v flat %q subject %q", id, id.Flat(), id.Subject())
	}
	legacy, err := ParseStatementID("0xabc:42:9f1c")
	if err != nil || legacy.IsLaned() || legacy.Flat() != "0xabc:42:9f1c" || legacy.Subject() != "0xabc" {
		t.Fatalf("legacy %+v, %v", legacy, err)
	}
}

func TestParseStatementIDRefusesMalformedLanes(t *testing.T) {
	for _, flat := range []string{
		"0xabc:5e1f0a2b7c9d3e4:42:n",     // lane 15
		"0xabc:5e1f0a2b7c9d3e4f0:42:n",   // lane 17
		"0xabc:5E1F0A2B7C9D3E4F:42:n",    // uppercase
		"0xabc::42:n",                    // empty lane
		"0xabc:5e1f0a2b7c9d3e4g:42:n",    // not hex
		"0xabc:5e1f0a2b7c9d3e4f:042:n",   // leading-zero seq
		"0xabc:5e1f0a2b7c9d3e4f:0:n",     // seq 0
		"0xabc:5e1f0a2b7c9d3e4f:42: n",   // nonce with surrounding whitespace
		"0xABC:5e1f0a2b7c9d3e4f:42:n",    // uppercase account
		"0xabc:5e1f0a2b7c9d3e4f:42:n:x",  // five segments
	} {
		if _, err := ParseStatementID(flat); err == nil {
			t.Errorf("ParseStatementID(%q) accepted a malformed id", flat)
		}
	}
}

func TestParseFlatStatementIDStaysLegacyOnly(t *testing.T) {
	if _, _, _, err := ParseFlatStatementID("0xabc:5e1f0a2b7c9d3e4f:42:9f1c"); err == nil {
		t.Fatal("the legacy SDK wrapper must refuse a laned id")
	}
}
```

Append to `pkg/storageintegrity/arbiter_proto_test.go`:

```go
func TestArbiterStatementEnvelopeToProtoCarriesTheLane(t *testing.T) {
	env := arbiterProtoEnvelopeFixture()
	got, err := ArbiterStatementEnvelopeToProto(env)
	if err != nil || got.GetStatementId().GetClientLane() != "" {
		t.Fatalf("legacy: %v, %v", got.GetStatementId(), err)
	}
	env.StatementID = "0xabc:5e1f0a2b7c9d3e4f:7:nonce-7"
	got, err = ArbiterStatementEnvelopeToProto(env)
	if err != nil {
		t.Fatal(err)
	}
	id := got.GetStatementId()
	if id.GetClientAccount() != "0xabc" || id.GetClientLane() != "5e1f0a2b7c9d3e4f" || id.GetClientSeq() != 7 || id.GetClientNonce() != "nonce-7" {
		t.Fatalf("laned: %v", id)
	}
}
```

Append to `pkg/storageintegrity/admission_code_test.go`:

```go
func TestLaneBudgetIsATerminalRejectWithItsCode(t *testing.T) {
	got := SubmitOutcomeFromSequencedAck(&pb.SequencedAck{Code: pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED, Message: "account holds 256 client lanes"})
	if got.Category != OutcomeTerminalReject || got.AdmissionCode != AdmissionCodeLaneBudgetExceeded || got.AdmissionCode != "ADMISSION_CODE_LANE_BUDGET_EXCEEDED" {
		t.Fatalf("outcome = %+v", got)
	}
}
```

Append to `pkg/plugins/storageintegrity/plugin_test.go`:

```go
func TestIngressRefusesLanedIDsUntilClientLanesAreEnabled(t *testing.T) {
	const lanedID = "5e1f0a2b7c9d3e4f"
	for _, enabled := range []bool{false, true} {
		p, signer := newSignedIngressWithConfig(t, Config{ClientLanesEnabled: func() bool { return enabled }})
		sql := "INSERT INTO tenant.events FORMAT Native"
		qctx := signedQueryContext(t, 1, signer, sql, sql, sqlmeta.StatementTypeInsert)
		qctx.Query.ID = strings.ToLower(signer.Address()) + ":" + lanedID + ":1:n1" // the grammar requires a lowercase account
		defaultPayload := []byte{byte(chproto.ClientDataCode), 0, 0xab, 0xcd}
		withStatementToken(t, qctx, signer, v2Statement(signer, qctx.Query.ID, sql, payloadexec.TableSchemaHash("testnet-v2", ingressSchema()), defaultPayload, uint32(qctx.Session.State().ClientRevision)))
		err := p.OnQuery(context.Background(), qctx)
		lanesRefusal := err != nil && err.Error() == "storage_integrity: client lanes are not enabled on this network"
		if enabled == lanesRefusal {
			t.Fatalf("enabled=%v: OnQuery = %v", enabled, err)
		}
	}
}
```

(The SQL and table are those of the existing `TestIngressAcceptsSignedMaterializedInsert`; the lanes gate runs right after the statement-id check, before table resolution, so `enabled=true` must merely not produce the lanes refusal — any later refusal of this minimal fixture is acceptable for this test.)

Run: `cd "$W" && go test ./pkg/storageintegrity/ ./pkg/plugins/storageintegrity/ -run 'Laned|LanedForm|MalformedLanes|LegacyOnly|CarriesTheLane|LaneBudget|ClientLanesAreEnabled' -v`
Expected: FAIL — `ValidClientLane`, `AdmissionCodeLaneBudgetExceeded` and `Config.ClientLanesEnabled` are undefined, and `ArbiterStatementEnvelopeToProto` refuses the laned id (A1 parses it with `ParseLegacyStatementID`). The two parser tests pass as soon as the package compiles: A1's `ParseStatementID` already accepts both forms, and they pin that.

- [ ] **Step 4: Implement the grammar**

In `pkg/storageintegrity/statementid.go` add and use:

```go
// ClientLaneHexLen is the length of a client lane: 8 random bytes in hex
// (spec 2026-10-09 D9).
const ClientLaneHexLen = 16

// ValidClientLane reports whether lane is exactly 16 lowercase hex characters.
func ValidClientLane(lane string) bool {
	if len(lane) != ClientLaneHexLen {
		return false
	}
	for i := 0; i < len(lane); i++ {
		if c := lane[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
```

A1's `ParseStatementID` already dispatches by segment count, renders `Flat()` and `Subject()` for both forms and has `IsLaned()`; do not re-implement them. Make A1's private `isLane` return `ValidClientLane(s)`, so the grammar has one lane rule. `ParseFlatStatementID` and `ParseLegacyStatementID` stay legacy-only exactly as A1 ships them (a laned id fails with `ErrClientLanesNotEnabled`). Switch the three A1 call sites from `ParseLegacyStatementID` to `ParseStatementID`: `EnvelopeFromAdmission` in `intake.go` (otherwise every laned admission fails in the intake after the ingress admitted it), `ArbiterStatementEnvelopeToProto` in `arbiter_proto.go`, and `parseIngressStatementID` in `pkg/plugins/storageintegrity/plugin.go` (which then only reports grammar errors; the lane decision moves to the gate in Step 5). A1's existing statement-id tests must pass unchanged.

In `pkg/storageintegrity/arbiter_proto.go`: set `ClientLane: id.Lane,` in the `pb.StatementID` literal of `ArbiterStatementEnvelopeToProto` (the id comes from `ParseStatementID` after A1), add

```go
// AdmissionCodeLaneBudgetExceeded is the arbiter's refusal of a statement that
// would open one client lane too many for its account (spec 2026-10-09 D13).
// The coordinate is unspent; the agent switches to its legacy lane.
var AdmissionCodeLaneBudgetExceeded = pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED.String()
```

and add `pb.AdmissionCode_ADMISSION_CODE_LANE_BUDGET_EXCEEDED` to the terminal-reject case list of `SubmitOutcomeFromSequencedAck`. Because the code is non-empty and not `DUPLICATE_CLIENT_SEQ`, PA2 marks the refusal ` [client_seq unspent]` with no further change; confirm by grepping the PA2 condition (`git grep -n 'DUPLICATE_CLIENT_SEQ' storage_integrity_ingress.go`).

- [ ] **Step 5: Implement the ingress gate**

In `pkg/plugins/storageintegrity/plugin.go` add to `Config`:

```go
	// ClientLanesEnabled reports whether the network committed client lanes
	// (the arbiter's client_lanes parameter, delivered by the host's registry
	// follower). Nil means disabled. Before activation a laned statement id is
	// refused here instead of reaching the arbiter as MALFORMED.
	ClientLanesEnabled func() bool
```

store it on the plugin as `clientLanesEnabled`, and in `OnQuery` immediately after A1's `if err := requireStatementIDSigner(stmtID, signer); err != nil { return err }` (A1's `TestIngressRefusesLanedStatementIDBeforeActivation` keeps passing: its laned id names the signer, so it now reaches this gate and still fails with `ErrClientLanesNotEnabled`):

```go
	if parsed, err := sicore.ParseStatementID(stmtID); err == nil && parsed.IsLaned() && (p.clientLanesEnabled == nil || !p.clientLanesEnabled()) {
		return sicore.ErrClientLanesNotEnabled
	}
```

In `proxy.go` add to `Options` (next to `StorageIntegrityTableState`):

```go
	// StorageIntegrityClientLanes reports whether the arbiter committed client
	// lanes (spec 2026-10-09 D13); sentio-node reads it from its table-registry
	// follower. Nil means disabled: the SI ingress refuses laned statement ids.
	StorageIntegrityClientLanes func() bool
```

and in `build.go` pass `ClientLanesEnabled: opts.StorageIntegrityClientLanes,` in the `storageintegrity.Config` literal, adding `"client_lanes_port", opts.StorageIntegrityClientLanes != nil` to the "storage_integrity ingress enabled" log line.

- [ ] **Step 6: Run and commit**

Run: `cd "$W" && bazel run //:gazelle && bazel test //...`
Expected: all PASS (A1's legacy tests untouched).

```bash
cd "$W" && git add -A && git commit -m "feat(storageintegrity): laned statement ids, LANE_BUDGET_EXCEEDED, ingress lanes gate

ParseStatementID accepts <account>:<lane>:<seq>:<nonce>; the arbiter
envelope carries client_lane; LANE_BUDGET_EXCEEDED is a coded terminal
reject; the ingress refuses laned ids until the host reports client lanes
enabled (housegate spec 2026-10-09 §6.1).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 14: housegate — lane JWS vectors and `SharedStatementLaneVectorsSHA256` (B4)

**Files:**
- Create: `pkg/auth/statement_v2_lane_vectors_test.go`, `pkg/auth/testdata/statement_jws_v2_lanes.json`
- Modify: `pkg/auth/shared_vectors.go`, `pkg/auth/BUILD.bazel` (test `data`)
- Create: `pkg/storageintegrity/statementid_lane_vectors_test.go`, add the testdata to `pkg/storageintegrity/BUILD.bazel` test `data` as `//pkg/auth:testdata/statement_jws_v2_lanes.json` (export it with `exports_files` in `pkg/auth/BUILD.bazel`)

**Interfaces:**
- Consumes: existing `statementV2TestKey`, `statementV2Fixture`, `(*RelaySigner).SignStatementV2`, `(*EthValidator).ValidateStatementV2`; Task 13 `ParseStatementID`.
- Produces: `pkg/auth/testdata/statement_jws_v2_lanes.json` (6 vectors) and `const auth.SharedStatementLaneVectorsSHA256`; the vector JSON schema below, consumed verbatim by arbiter Task 18.

- [ ] **Step 1: Write the generator and checks**

Create `pkg/auth/statement_v2_lane_vectors_test.go`:

```go
package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"strconv"
	"testing"
	"time"
)

// statementV2LaneVector is one client-lane statement-JWS conformance vector
// (housegate spec 2026-10-09 D10, §7). client_* is the structured statement
// id a verifier rebuilds; payload is what the token signs. A verifier must
// accept exactly when the flat id rendered from client_* equals the signed
// payload.statement_id and the signature recovers signer_address.
type statementV2LaneVector struct {
	Name          string                `json:"name"`
	Expect        string                `json:"expect"`                  // "accept" | "reject"
	RejectReason  string                `json:"reject_reason,omitempty"` // "binding"
	RejectField   string                `json:"reject_field,omitempty"`  // "statement_id"
	ClientAccount string                `json:"client_account"`
	ClientLane    string                `json:"client_lane"`
	ClientSeq     uint64                `json:"client_seq"`
	ClientNonce   string                `json:"client_nonce"`
	Payload       JWSStatementPayloadV2 `json:"payload"`
	Token         string                `json:"token"`
}

type statementV2LaneVectorFile struct {
	SignerPrivateKeyHex string                  `json:"signer_private_key_hex"`
	SignerAddress       string                  `json:"signer_address"`
	Vectors             []statementV2LaneVector `json:"vectors"`
}

const statementV2LaneVectorPath = "testdata/statement_jws_v2_lanes.json"

// laneVectorFlat renders the flat statement id from structured fields; it is
// the rule every verifier must apply (legacy form when the lane is empty).
func laneVectorFlat(account, lane string, seq uint64, nonce string) string {
	if lane == "" {
		return account + ":" + strconv.FormatUint(seq, 10) + ":" + nonce
	}
	return account + ":" + lane + ":" + strconv.FormatUint(seq, 10) + ":" + nonce
}

// TestGenerateStatementV2LaneVectors rewrites the shared lane vector file when
// HOUSEGATE_WRITE_LANE_VECTORS=1. Regenerating it is a coordinated wire change:
// update SharedStatementLaneVectorsSHA256 and the arbiter's verbatim copy.
func TestGenerateStatementV2LaneVectors(t *testing.T) {
	if os.Getenv("HOUSEGATE_WRITE_LANE_VECTORS") != "1" {
		t.Skip("set HOUSEGATE_WRITE_LANE_VECTORS=1 to regenerate testdata/statement_jws_v2_lanes.json")
	}
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	account := signer.Address()
	sign := func(flat string) (JWSStatementPayloadV2, string) {
		p := statementV2Fixture(account)
		p.Purpose = StatementPurposeV2
		p.StatementID = flat
		token, err := signer.SignStatementV2(p)
		if err != nil {
			t.Fatal(err)
		}
		return p, token
	}
	const laneA, laneB, nonce = "5e1f0a2b7c9d3e4f", "ffffffffffffffff", "9f1c0000000000000000000000000001"
	lanedPayload, lanedToken := sign(laneVectorFlat(account, laneA, 42, nonce))
	otherPayload, otherToken := sign(laneVectorFlat(account, laneB, 1, nonce))
	legacyPayload, legacyToken := sign(laneVectorFlat(account, "", 42, nonce))
	vec := func(name, expect, lane string, seq uint64, p JWSStatementPayloadV2, token string) statementV2LaneVector {
		v := statementV2LaneVector{Name: name, Expect: expect, ClientAccount: account, ClientLane: lane, ClientSeq: seq, ClientNonce: nonce, Payload: p, Token: token}
		if expect == "reject" {
			v.RejectReason, v.RejectField = "binding", "statement_id"
		}
		return v
	}
	file := statementV2LaneVectorFile{SignerPrivateKeyHex: statementV2TestKey, SignerAddress: account, Vectors: []statementV2LaneVector{
		vec("laned_valid", "accept", laneA, 42, lanedPayload, lanedToken),
		vec("laned_valid_other_lane", "accept", laneB, 1, otherPayload, otherToken),
		vec("laned_lane_swapped", "reject", laneB, 42, lanedPayload, lanedToken),
		vec("laned_lane_stripped", "reject", "", 42, lanedPayload, lanedToken),
		vec("legacy_signed_presented_laned", "reject", laneA, 42, legacyPayload, legacyToken),
		vec("laned_seq_changed", "reject", laneA, 43, lanedPayload, lanedToken),
	}}
	b, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statementV2LaneVectorPath, append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestStatementV2LaneVectors proves every vector against this package's
// validator with the expectation rebuilt from the structured fields.
func TestStatementV2LaneVectors(t *testing.T) {
	raw, err := os.ReadFile(statementV2LaneVectorPath)
	if err != nil {
		t.Fatalf("read lane vectors: %v", err)
	}
	var file statementV2LaneVectorFile
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) != 6 {
		t.Fatalf("expected exactly 6 lane vectors, got %d", len(file.Vectors))
	}
	validator := NewEthValidator([]string{file.SignerAddress}, 100*365*24*time.Hour, true, false, "", nil)
	for _, vec := range file.Vectors {
		t.Run(vec.Name, func(t *testing.T) {
			want := vec.Payload
			want.StatementID = laneVectorFlat(vec.ClientAccount, vec.ClientLane, vec.ClientSeq, vec.ClientNonce)
			_, err := validator.ValidateStatementV2(vec.Token, want)
			switch vec.Expect {
			case "accept":
				if err != nil || want.StatementID != vec.Payload.StatementID {
					t.Fatalf("expected accept: %v", err)
				}
			case "reject":
				if err == nil || err.Error() != "statement token binding mismatch on "+vec.RejectField {
					t.Fatalf("expected a %s binding reject, got %v", vec.RejectField, err)
				}
			default:
				t.Fatalf("unknown expect %q", vec.Expect)
			}
		})
	}
}

// TestSharedStatementLaneVectorsSHA256 is the cross-repo link for the lane
// vectors: the arbiter asserts its verbatim copy hashes to the same constant.
func TestSharedStatementLaneVectorsSHA256(t *testing.T) {
	raw, err := os.ReadFile(statementV2LaneVectorPath)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != SharedStatementLaneVectorsSHA256 {
		t.Fatalf("statement_jws_v2_lanes.json sha256 = %s, SharedStatementLaneVectorsSHA256 = %s\n"+
			"regenerating the lane vectors is a coordinated wire change: update the constant, copy the file into arbiter fsm/testdata, and cut both releases together", got, SharedStatementLaneVectorsSHA256)
	}
}
```

Create `pkg/storageintegrity/statementid_lane_vectors_test.go`:

```go
package storageintegrity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// TestStatementIDGrammarMatchesTheLaneVectors checks the one statement-id
// grammar against the shared lane vectors: every accepted vector's signed flat
// id parses to its structured fields and renders back byte-identically.
func TestStatementIDGrammarMatchesTheLaneVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "auth", "testdata", "statement_jws_v2_lanes.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Vectors []struct {
			Name          string `json:"name"`
			Expect        string `json:"expect"`
			ClientAccount string `json:"client_account"`
			ClientLane    string `json:"client_lane"`
			ClientSeq     uint64 `json:"client_seq"`
			ClientNonce   string `json:"client_nonce"`
			Payload       struct {
				StatementID string `json:"statement_id"`
			} `json:"payload"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	for _, v := range file.Vectors {
		structured := StatementID{Account: v.ClientAccount, Lane: v.ClientLane, Seq: v.ClientSeq, Nonce: v.ClientNonce}
		parsed, err := ParseStatementID(v.Payload.StatementID)
		if err != nil {
			t.Fatalf("%s: %v", v.Name, err)
		}
		if parsed.Flat() != v.Payload.StatementID {
			t.Fatalf("%s: Flat(Parse(x)) != x", v.Name)
		}
		if (v.Expect == "accept") != (parsed == structured) {
			t.Fatalf("%s: parsed %+v vs structured %+v disagrees with expect=%s", v.Name, parsed, structured, v.Expect)
		}
	}
}
```

(Under Bazel the relative path resolves through the `data` dependency added in this task's file list; if the runfiles path differs, read it with `runfiles.Rlocation("_main/pkg/auth/testdata/statement_jws_v2_lanes.json")` as other housegate tests that read another package's testdata do — `git grep -n Rlocation pkg/ | head -3`.)

Add to `pkg/auth/shared_vectors.go`:

```go
// SharedStatementLaneVectorsSHA256 is the SHA-256 of
// pkg/auth/testdata/statement_jws_v2_lanes.json — the client-lane statement
// JWS vectors (spec 2026-10-09 D10) the Arbiter FSM consumes verbatim. A
// separate file and constant keep SharedStatementVectorsSHA256 and the
// arbiter's existing copy unchanged.
const SharedStatementLaneVectorsSHA256 = ""
```

- [ ] **Step 2: Generate, pin, run**

Run (Go, not Bazel, because the generator writes into the source tree):
```bash
cd "$W/pkg/auth" && HOUSEGATE_WRITE_LANE_VECTORS=1 go test -run TestGenerateStatementV2LaneVectors . && shasum -a 256 testdata/statement_jws_v2_lanes.json
```
Paste the printed digest into `SharedStatementLaneVectorsSHA256`, then:

Run: `cd "$W" && go test ./pkg/auth/ ./pkg/storageintegrity/ -run 'LaneVectors|SharedStatement' -v && bazel test //pkg/auth:all //pkg/storageintegrity:all`
Expected: PASS; `TestSharedStatementVectorsSHA256` (the old file) unchanged and PASS.

- [ ] **Step 3: Commit**

```bash
cd "$W" && git add pkg/auth pkg/storageintegrity && git commit -m "test(auth): client-lane statement JWS vectors with their own shared digest

Six vectors bind the lane through statement_id under the unchanged
StatementPurposeV2 and payload; a separate file and
SharedStatementLaneVectorsSHA256 leave the existing vectors and the
arbiter's verbatim copy untouched (housegate spec 2026-10-09 D10, §7).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 15: housegate — agent lane pool with `flock` (B4)

**Files:**
- Create: `pkg/plugins/sistatement/lanepool.go` (platform-independent logic)
- Create: `pkg/plugins/sistatement/lanepool_lock_unix.go` (`//go:build linux || darwin`)
- Create: `pkg/plugins/sistatement/lanepool_other.go` (`//go:build !(linux || darwin)`)
- Create: `pkg/plugins/sistatement/lanepool_test.go`, `lanepool_helper_test.go` (both `//go:build linux || darwin`), `lanepool_other_test.go` (`//go:build !(linux || darwin)`)

**Interfaces:**
- Consumes: Task 13 `sicore.ValidClientLane`; PA3 (Plan A1 Task 11) `lockFile`, `ErrSeqLocked`, `ErrSeqLockUnsupported`, `mkdirAllDurable`, `writeDurable`, `openSeqDir`, `MaxFreeSeqs` (see Step 1).
- Produces:
  - `type LanePoolOptions struct{ Rand io.Reader; OnCorrupt func(lane string, err error); OnBurn func(reason string) }`
  - `func OpenLanePool(siDir string, opts LanePoolOptions) (*LanePool, error)`; `var ErrLanesUnsupported error`
  - `type AcquireReason string` with `AcquireReused`, `AcquireNewProcess` (`"new_process"`), `AcquireLostState` (`"lost_state"`)
  - `func (p *LanePool) Acquire() (*LanedStore, AcquireReason, error)`
  - `*LanedStore` methods `Lane() string`, `Reserve() (uint64, error)`, `ReserveSupplied(seq uint64) error`, `Release(seq uint64) error`, `Abandon() error`, `Close() error`

The spec says `//go:build unix`; this plan uses `linux || darwin` (the only release targets, U9) because `syscall.Flock` is not available on every `unix` GOOS (aix, solaris), which would break `GOOS=… go vet`. Every other platform gets the refusing stub.

- [ ] **Step 1: Confirm PA3's lock and durable-write helpers**

Plan A1 Task 11 ships, in package `sistatement`: `lockFile(path string) (func() error, error)` (`flock(LOCK_EX|LOCK_NB)`, `ErrSeqLocked` on `EWOULDBLOCK`, `ErrSeqLockUnsupported` on the stub) in `seq_lock_unix.go` / `seq_lock_other.go` / `seq_lock.go`; and `mkdirAllDurable(dir, perm, openDir)`, `syncDir(openDir, path)`, `writeDurable(openDir, path, data)` with the default `openSeqDir` in `seq_dir.go`; plus `MaxFreeSeqs = 64` in `seq.go`. This task reuses all of them and never re-declares a syscall or a durable-write helper (a second `syncDir` in the package would not compile).

Run: `cd "$W" && grep -n 'func lockFile\|func mkdirAllDurable\|func syncDir\|func writeDurable\|func openSeqDir\|MaxFreeSeqs =\|go:build' pkg/plugins/sistatement/seq*.go`
Expected: each of the seven names above, with build tags `linux || darwin` / `!(linux || darwin)` on the two lock files. If any is missing or differently named, stop and reconcile with Plan A1 Task 11.

- [ ] **Step 2: Write the failing tests**

Create `pkg/plugins/sistatement/lanepool_test.go`:

```go
//go:build linux || darwin

package sistatement

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func openPool(t *testing.T, dir string, opts LanePoolOptions) *LanePool {
	t.Helper()
	p, err := OpenLanePool(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func acquire(t *testing.T, p *LanePool) (*LanedStore, AcquireReason) {
	t.Helper()
	s, reason, err := p.Acquire()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, reason
}

func reserve(t *testing.T, s *LanedStore) uint64 {
	t.Helper()
	seq, err := s.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	return seq
}

func TestAcquireMintsThenReusesAfterRestart(t *testing.T) {
	dir := t.TempDir()
	s, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if reason != AcquireLostState || len(s.Lane()) != 16 {
		t.Fatalf("first acquire: lane %q reason %s", s.Lane(), reason)
	}
	if reserve(t, s) != 1 || reserve(t, s) != 2 {
		t.Fatal("a new lane starts at seq 1")
	}
	lane := s.Lane()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	again, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if again.Lane() != lane || reason != AcquireReused || reserve(t, again) != 3 {
		t.Fatalf("restart: lane %q reason %s", again.Lane(), reason)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "lanes", lane+".json"))
	var f map[string]any
	if err := json.Unmarshal(raw, &f); err != nil || f["version"] != float64(1) || f["lane"] != lane || f["next"] != float64(4) || f["abandoned"] != false {
		t.Fatalf("lane file %s (%v)", raw, err)
	}
	if _, ok := f["free"].([]any); !ok {
		t.Fatalf("free must be a JSON array, got %s", raw)
	}
}

func TestTwoHoldersGetDistinctLanes(t *testing.T) {
	dir := t.TempDir()
	a, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	b, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if a.Lane() == b.Lane() || reason != AcquireNewProcess {
		t.Fatalf("lanes %s / %s reason %s: a held lane must never be shared", a.Lane(), b.Lane(), reason)
	}
}

func TestAbandonedLanesAreNeverReused(t *testing.T) {
	dir := t.TempDir()
	p := openPool(t, dir, LanePoolOptions{})
	s, _ := acquire(t, p)
	old := s.Lane()
	if err := s.Abandon(); err != nil {
		t.Fatal(err)
	}
	next, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if next.Lane() == old {
		t.Fatal("an abandoned lane was reused")
	}
	if _, err := s.Reserve(); err == nil {
		t.Fatal("an abandoned store still reserves")
	}
}

func TestAcquireSkipsCorruptLaneFiles(t *testing.T) {
	dir := t.TempDir()
	lanes := filepath.Join(dir, "lanes")
	if err := os.MkdirAll(lanes, 0o700); err != nil {
		t.Fatal(err)
	}
	corrupt := map[string][]byte{
		"000000000000000a": nil,
		"000000000000000b": []byte("{"),
		"000000000000000c": []byte(`{"version":1,"lane":"000000000000000d","next":5,"free":[],"abandoned":false}`),
		"000000000000000e": []byte(`{"version":1,"lane":"000000000000000e","next":3,"free":[3],"abandoned":false}`),
	}
	for lane, body := range corrupt {
		if err := os.WriteFile(filepath.Join(lanes, lane+".json"), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var reported []string
	s, _ := acquire(t, openPool(t, dir, LanePoolOptions{OnCorrupt: func(lane string, _ error) { reported = append(reported, lane) }}))
	if _, bad := corrupt[s.Lane()]; bad {
		t.Fatalf("reused corrupt lane %s", s.Lane())
	}
	if len(reported) != len(corrupt) {
		t.Fatalf("reported %v, want every corrupt lane", reported)
	}
	for lane, body := range corrupt {
		got, err := os.ReadFile(filepath.Join(lanes, lane+".json"))
		if err != nil || !bytes.Equal(got, body) {
			t.Fatalf("corrupt lane %s was modified or deleted", lane)
		}
	}
}

func TestFreeListOrderingAndCap(t *testing.T) {
	var burned int
	s, _ := acquire(t, openPool(t, t.TempDir(), LanePoolOptions{OnBurn: func(reason string) {
		if reason == "free_list_overflow" {
			burned++
		}
	}}))
	for i := 0; i < 70; i++ {
		reserve(t, s)
	}
	for seq := uint64(70); seq >= 1; seq-- {
		if err := s.Release(seq); err != nil {
			t.Fatal(err)
		}
	}
	if burned != 6 {
		t.Fatalf("burned %d, want 6 (65..70 dropped, the largest first)", burned)
	}
	for want := uint64(1); want <= 64; want++ {
		if got := reserve(t, s); got != want {
			t.Fatalf("reserve = %d, want %d (smallest free first)", got, want)
		}
	}
	if got := reserve(t, s); got != 71 {
		t.Fatalf("after the free list, reserve = %d, want 71", got)
	}
}

func TestReleaseRules(t *testing.T) {
	dir := t.TempDir()
	s, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	reserve(t, s)
	reserve(t, s)
	for _, seq := range []uint64{0, 3, 99} {
		if err := s.Release(seq); err == nil {
			t.Fatalf("Release(%d) of a never-reserved seq accepted", seq)
		}
	}
	if err := s.Release(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(1); err != nil {
		t.Fatalf("a repeated release must be a no-op: %v", err)
	}
	lane := s.Lane()
	_ = s.Close()
	again, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if again.Lane() != lane || reserve(t, again) != 1 || reserve(t, again) != 3 {
		t.Fatal("a released seq must survive a restart and be reused first")
	}
}

func TestReserveSupplied(t *testing.T) {
	s, _ := acquire(t, openPool(t, t.TempDir(), LanePoolOptions{}))
	reserve(t, s)
	if err := s.ReserveSupplied(1); err == nil {
		t.Fatal("a supplied seq below next must be refused")
	}
	if err := s.ReserveSupplied(10); err != nil {
		t.Fatal(err)
	}
	if got := reserve(t, s); got != 11 {
		t.Fatalf("next after supplied 10 = %d", got)
	}
}

func TestOpenLanePoolRefusesAFileAsDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "lanes"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenLanePool(dir, LanePoolOptions{}); err == nil || errors.Is(err, ErrLanesUnsupported) {
		t.Fatalf("err = %v", err)
	}
}
```

Create `pkg/plugins/sistatement/lanepool_helper_test.go`:

```go
//go:build linux || darwin

package sistatement

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestLanePoolHelperProcess is re-executed by TestAcquireFromTwoProcesses; it
// holds one lane until its stdin closes.
func TestLanePoolHelperProcess(t *testing.T) {
	dir := os.Getenv("SISTATEMENT_LANE_HELPER_DIR")
	if dir == "" {
		t.Skip("helper process only")
	}
	p, err := OpenLanePool(dir, LanePoolOptions{})
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(1)
	}
	s, _, err := p.Acquire()
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(1)
	}
	fmt.Println("LANE", s.Lane())
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

func TestAcquireFromTwoProcesses(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestLanePoolHelperProcess$")
	cmd.Env = append(os.Environ(), "SISTATEMENT_LANE_HELPER_DIR="+dir)
	stdin, _ := cmd.StdinPipe()
	stdout, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "LANE ") {
		t.Fatalf("helper: %q %v", line, err)
	}
	other := strings.TrimSpace(strings.TrimPrefix(line, "LANE "))
	mine, _ := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if mine.Lane() == other {
		t.Fatal("two processes share a lane")
	}
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	_ = mine.Close()
	reused, reason := acquire(t, openPool(t, dir, LanePoolOptions{}))
	if reason != AcquireReused {
		t.Fatalf("after both exited a third lane was minted (%s)", reused.Lane())
	}
}
```

Create `pkg/plugins/sistatement/lanepool_other_test.go`:

```go
//go:build !(linux || darwin)

package sistatement

import (
	"errors"
	"testing"
)

func TestLanePoolIsRefusedOnThisPlatform(t *testing.T) {
	if _, err := OpenLanePool(t.TempDir(), LanePoolOptions{}); !errors.Is(err, ErrLanesUnsupported) {
		t.Fatalf("err = %v, want ErrLanesUnsupported", err)
	}
}
```

Run: `cd "$W" && go test ./pkg/plugins/sistatement/ -run 'Acquire|Abandoned|FreeList|ReleaseRules|ReserveSupplied|OpenLanePool|TwoHolders|TwoProcesses' -v`
Expected: build failure (`undefined: OpenLanePool`).

- [ ] **Step 3: Implement**

Create `pkg/plugins/sistatement/lanepool.go`:

```go
package sistatement

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// ErrLanesUnsupported means this platform has no exclusive file lock, so a
// lane could be shared by two processes; lanes are refused there (spec
// 2026-10-09 D15).
var ErrLanesUnsupported = errors.New("sistatement: client lanes need flock, which this platform does not provide")

var errLaneLocked = errors.New("sistatement: lane is held by another process")

const (
	laneFileVersion = 1
	maxFreeSeqs     = 64
	lanesDirName    = "lanes"
)

// AcquireReason says why Acquire returned its lane (metrics label).
type AcquireReason string

const (
	AcquireReused     AcquireReason = "reused"
	AcquireNewProcess AcquireReason = "new_process"
	AcquireLostState  AcquireReason = "lost_state"
)

// laneFile is <si_dir>/lanes/<lane>.json.
type laneFile struct {
	Version   int      `json:"version"`
	Lane      string   `json:"lane"`
	Next      uint64   `json:"next"`
	Free      []uint64 `json:"free"`
	Abandoned bool     `json:"abandoned"`
}

// LanePoolOptions configures a pool; every field is optional.
type LanePoolOptions struct {
	Rand      io.Reader                    // nil = crypto/rand
	OnCorrupt func(lane string, err error) // a lane file that cannot be trusted
	OnBurn    func(reason string)          // a provably unspent seq dropped: "free_list_overflow"
}

// LanePool manages this agent's client_seq lanes under <si_dir>/lanes (spec
// 2026-10-09 D15). Each lane is a JSON file plus an exclusive flock on
// <lane>.lock held for the process lifetime, so two processes never share a
// lane and a restarted agent reuses its lane.
type LanePool struct {
	dir  string
	opts LanePoolOptions
}

// OpenLanePool creates <siDir>/lanes (mode 0700, durably) when missing.
func OpenLanePool(siDir string, opts LanePoolOptions) (*LanePool, error) {
	if !lanesSupported {
		return nil, ErrLanesUnsupported
	}
	if opts.Rand == nil {
		opts.Rand = rand.Reader
	}
	dir := filepath.Join(siDir, lanesDirName)
	// A1's mkdirAllDurable creates <siDir> too when the whole state directory
	// was lost (spec §9.2 "lost state dir"), fsyncing every new entry.
	if err := mkdirAllDurable(dir, 0o700, openSeqDir); err != nil {
		return nil, err
	}
	return &LanePool{dir: dir, opts: opts}, nil
}

// Acquire returns the first unlocked, intact, not-abandoned lane in sorted
// order, or mints a new one. A file that cannot be trusted is reported and
// skipped, never reused and never deleted.
func (p *LanePool) Acquire() (*LanedStore, AcquireReason, error) {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return nil, "", fmt.Errorf("sistatement: list %s: %w", p.dir, err)
	}
	var lanes []string
	for _, e := range entries {
		if lane, ok := strings.CutSuffix(e.Name(), ".json"); ok && sicore.ValidClientLane(lane) && !e.IsDir() {
			lanes = append(lanes, lane)
		}
	}
	slices.Sort(lanes)
	for _, lane := range lanes {
		lock, err := tryLockLaneFile(filepath.Join(p.dir, lane+".lock"))
		if errors.Is(err, errLaneLocked) {
			continue
		}
		if err != nil {
			return nil, "", err
		}
		st, err := readLaneFile(filepath.Join(p.dir, lane+".json"), lane)
		if err != nil {
			_ = lock.Unlock()
			if p.opts.OnCorrupt != nil {
				p.opts.OnCorrupt(lane, err)
			}
			continue
		}
		if st.Abandoned {
			_ = lock.Unlock()
			continue
		}
		return &LanedStore{pool: p, lane: lane, lock: lock, state: st}, AcquireReused, nil
	}
	reason := AcquireNewProcess
	if len(lanes) == 0 {
		reason = AcquireLostState
	}
	s, err := p.mint()
	return s, reason, err
}

func (p *LanePool) mint() (*LanedStore, error) {
	for attempt := 0; attempt < 8; attempt++ {
		var b [8]byte
		if _, err := io.ReadFull(p.opts.Rand, b[:]); err != nil {
			return nil, fmt.Errorf("sistatement: lane id: %w", err)
		}
		lane := hex.EncodeToString(b[:])
		if _, err := os.Stat(filepath.Join(p.dir, lane+".json")); err == nil {
			continue
		}
		lock, err := tryLockLaneFile(filepath.Join(p.dir, lane+".lock"))
		if errors.Is(err, errLaneLocked) {
			continue
		}
		if err != nil {
			return nil, err
		}
		s := &LanedStore{pool: p, lane: lane, lock: lock}
		if err := s.persist(laneFile{Version: laneFileVersion, Lane: lane, Next: 1, Free: []uint64{}}); err != nil {
			_ = lock.Unlock()
			return nil, err
		}
		return s, nil
	}
	return nil, errors.New("sistatement: could not mint an unused client lane")
}

func readLaneFile(path, lane string) (laneFile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return laneFile{}, err
	}
	var st laneFile
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&st); err != nil {
		return laneFile{}, fmt.Errorf("decode: %w", err)
	}
	switch {
	case st.Version != laneFileVersion:
		return laneFile{}, fmt.Errorf("version %d", st.Version)
	case st.Lane != lane:
		return laneFile{}, fmt.Errorf("file names lane %q", st.Lane)
	case st.Next == 0:
		return laneFile{}, errors.New("next is 0")
	case len(st.Free) > maxFreeSeqs:
		return laneFile{}, fmt.Errorf("%d free seqs", len(st.Free))
	}
	for i, seq := range st.Free {
		if seq == 0 || seq >= st.Next || (i > 0 && seq <= st.Free[i-1]) {
			return laneFile{}, fmt.Errorf("free list %v is not ascending, unique and below next %d", st.Free, st.Next)
		}
	}
	if st.Free == nil {
		st.Free = []uint64{}
	}
	return st, nil
}

// LanedStore is one acquired lane. Every mutation is durable before it
// returns (temp file, fsync, rename, directory fsync).
type LanedStore struct {
	pool   *LanePool
	lane   string
	lock   laneLock
	mu     sync.Mutex
	state  laneFile
	closed bool
}

func (s *LanedStore) Lane() string { return s.lane }

// Reserve returns the smallest free seq, else next (then next+1 is
// persisted). Ascending reuse is a sequential append or a start-edge fill,
// which never adds a gap range at the arbiter.
func (s *LanedStore) Reserve() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, fmt.Errorf("sistatement: lane %s is closed", s.lane)
	}
	next := s.state
	next.Free = slices.Clone(s.state.Free)
	var seq uint64
	if len(next.Free) > 0 {
		seq, next.Free = next.Free[0], next.Free[1:]
	} else {
		if next.Next == math.MaxUint64 {
			return 0, fmt.Errorf("sistatement: lane %s client_seq exhausted", s.lane)
		}
		seq = next.Next
		next.Next++
	}
	if err := s.persist(next); err != nil {
		return 0, err
	}
	s.state = next
	return seq, nil
}

// ReserveSupplied accepts an SDK-supplied seq at or above next.
func (s *LanedStore) ReserveSupplied(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return fmt.Errorf("sistatement: lane %s is closed", s.lane)
	}
	if seq < s.state.Next || seq == math.MaxUint64 {
		return fmt.Errorf("sistatement: supplied client_seq %d is below lane %s next %d", seq, s.lane, s.state.Next)
	}
	next := s.state
	next.Next = seq + 1
	if err := s.persist(next); err != nil {
		return err
	}
	s.state = next
	return nil
}

// Release returns a provably unspent seq to the free list. A full list
// drops its largest entry, which only burns that seq.
func (s *LanedStore) Release(seq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq == 0 || seq >= s.state.Next {
		return fmt.Errorf("sistatement: client_seq %d was never reserved on lane %s", seq, s.lane)
	}
	if _, found := slices.BinarySearch(s.state.Free, seq); found {
		return nil
	}
	next := s.state
	next.Free = slices.Clone(s.state.Free)
	i, _ := slices.BinarySearch(next.Free, seq)
	next.Free = slices.Insert(next.Free, i, seq)
	burned := false
	if len(next.Free) > maxFreeSeqs {
		next.Free = next.Free[:maxFreeSeqs]
		burned = true
	}
	if err := s.persist(next); err != nil {
		return err
	}
	s.state = next
	if burned && s.pool.opts.OnBurn != nil {
		s.pool.opts.OnBurn("free_list_overflow")
	}
	return nil
}

// Abandon marks the lane durably abandoned (its gap budget is exhausted) and
// releases its lock; the caller acquires a new lane for later statements.
func (s *LanedStore) Abandon() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.state
	next.Abandoned = true
	if err := s.persist(next); err != nil {
		return err
	}
	s.state = next
	s.closed = true
	return s.lock.Unlock()
}

// Close releases the lane's lock; the lane stays reusable.
func (s *LanedStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.lock.Unlock()
}

func (s *LanedStore) persist(st laneFile) error {
	b, err := json.Marshal(st)
	if err != nil {
		return err
	}
	// Plan A1 Task 11's writeDurable: temp file, fsync, rename, directory fsync.
	return writeDurable(openSeqDir, filepath.Join(s.pool.dir, s.lane+".json"), append(b, '\n'))
}

// laneLock is an exclusive lock held for the process lifetime.
type laneLock interface{ Unlock() error }

type unlockFunc func() error

func (f unlockFunc) Unlock() error { return f() }

// tryLockLaneFile takes Plan A1's lockFile (flock LOCK_EX|LOCK_NB on path,
// created when missing) and maps "held by another process" to errLaneLocked.
func tryLockLaneFile(path string) (laneLock, error) {
	unlock, err := lockFile(path)
	switch {
	case errors.Is(err, ErrSeqLocked):
		return nil, errLaneLocked
	case errors.Is(err, ErrSeqLockUnsupported):
		return nil, ErrLanesUnsupported
	case err != nil:
		return nil, err
	}
	return unlockFunc(unlock), nil
}
```

(`readLaneFile` refuses unknown fields so a file from a future layout is never silently rewritten; the `.tmp` suffix is not `.json`, so a crash between write and rename leaves nothing `Acquire` lists. Drop the now-unused `io/fs` import from `lanepool.go` if gazelle/vet reports it, and replace the local `maxFreeSeqs` constant with A1's exported `MaxFreeSeqs` so the two stores cannot drift.)

Create `pkg/plugins/sistatement/lanepool_lock_unix.go`:

```go
//go:build linux || darwin

package sistatement

// lanesSupported reports whether A1's lockFile has a real flock here; the
// build tags match A1's seq_lock_unix.go / seq_lock_other.go exactly.
const lanesSupported = true
```

Create `pkg/plugins/sistatement/lanepool_other.go`:

```go
//go:build !(linux || darwin)

package sistatement

const lanesSupported = false
```

- [ ] **Step 4: Run, including the stub build**

Run: `cd "$W" && bazel run //:gazelle && go test ./pkg/plugins/sistatement/ -run 'Acquire|Abandoned|FreeList|ReleaseRules|ReserveSupplied|OpenLanePool|TwoHolders|TwoProcesses' -count=3 -v && GOOS=freebsd GOARCH=amd64 go vet ./pkg/plugins/sistatement/ && bazel test //pkg/plugins/sistatement:all`
Expected: PASS; the `freebsd` vet compiles the stub path (proves the non-Linux/Darwin build refuses lanes rather than failing to build).

- [ ] **Step 5: Commit**

```bash
cd "$W" && git add pkg/plugins/sistatement && git commit -m "feat(sistatement): flock-protected client_seq lane pool

<si_dir>/lanes/<lane>.json with a lifetime flock on <lane>.lock: acquire
reuses the first free intact lane or mints one, corrupt files are
reported and never reused or deleted, reservations take the smallest free
seq first, the free list is capped at 64, abandoned lanes are never
reused; platforms without flock refuse lanes (housegate spec 2026-10-09
D15, §6.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Task 16: housegate — agent lane selection, rotation, SDK rule R9, in-flight cap, metrics, release (B4)

**Files:**
- Create: `pkg/plugins/sistatement/lanes.go`, `pkg/plugins/sistatement/lanes_test.go`
- Modify: `pkg/plugins/sistatement/plugin.go` (Options, reservation in `OnQueryInputCompleteStrict`, pending cleanup, `OnException`), `pkg/plugins/sistatement/observer.go`
- Modify: `pkg/config/storage_integrity_config.go` (+test): `Lanes`, `MaxInflightPerLane`
- Modify: `build.go` (`buildAgent`: pass lane options and the observer)
- Modify: `pkg/proxy/observer.go` (agent lane metrics)
- Modify: `pkg/integration/storage_integrity_agent_test.go`
- Modify: `CLAUDE.md` (sistatement bullet: lanes; statement-id grammar)

**Interfaces:**
- Consumes: Task 15 `LanePool`, `LanedStore`, `AcquireReason`; Task 13 `sicore.ParseStatementID`, `StatementID`, `sicore.AdmissionCodeLaneBudgetExceeded`; PA3 legacy store and reservation site; PA4 discovery `ClientLanesEnabled`.
- Produces:
  - `type LaneMode string` (`LaneModeAuto = "auto"`, `LaneModeOff = "off"`), `func ParseLaneMode(string) (LaneMode, error)`
  - `type seqLane interface{ Lane() string; Reserve() (uint64, error); ReserveSupplied(uint64) error; Release(uint64) error }`
  - `type laneSelector` with `pick(ctx context.Context, lanesEnabled bool, legacy seqLane) (seqLane, func(), error)`, `rotate(lane string) error`, `pinLegacy(reason string)`
  - `func ownSuppliedStatementID(queryID, ownAccount, lane string) (sicore.StatementID, bool, error)`
  - `func laneRotationFor(message string) laneRotation` (`rotationNone`, `rotationGapBudget`, `rotationLaneBudget`, `rotationLanesDisabled`)
  - config `storage_integrity.agent.lanes` (`auto` default, `off`; A1 Task 16 already ships the field, `-si-lanes` and `HOUSEGATE_SI_LANES`), `storage_integrity.agent.max_inflight_per_lane` (default 16); `sistatement.Options.ClientLanesEnabled func() bool` (fed from `housegate.Options.StorageIntegrityClientLanes` when the agent has no RPC discovery)
  - optional observer `LaneObserver{ LaneRotated(reason string); SIInflight(delta int) }`; metrics `clickhouse_proxy_agent_si_lane_rotations_total{reason}`, `clickhouse_proxy_agent_si_inflight`
  - release `HG_TAG`

- [ ] **Step 1: Plan A contract check**

Run: `cd "$W" && git grep -n "OnQueryInputCompleteStrict\|Reserve()\|ReserveSupplied\|client_seq unspent\|ClientLanesEnabled\|si-lanes\|HOUSEGATE_SI_LANES\|MaxInflightPerLane" -- pkg/plugins/sistatement cmd pkg/config pkg/network | head -40`
Expected: PA3 reservation in `OnQueryInputCompleteStrict`, PA3 `OnException` release, PA4 `ClientLanesEnabled` on `registry.StorageIntegrityInfo`, `StorageIntegrityAgentConfig.Lanes` with the `-si-lanes` flag and `HOUSEGATE_SI_LANES` env (A1 Task 16; the A2 chart already renders `HOUSEGATE_SI_LANES=off` for the driver sidecar), and no `MaxInflightPerLane` (this task adds it). If any PA3/PA4 item is missing, stop and reconcile with Plan A1.

- [ ] **Step 2: Write the failing unit tests**

Create `pkg/plugins/sistatement/lanes_test.go`:

```go
//go:build linux || darwin

package sistatement

import (
	"context"
	"strings"
	"testing"
	"time"
)

type fakeLegacyLane struct{ next uint64 }

func (f *fakeLegacyLane) Lane() string                 { return "" }
func (f *fakeLegacyLane) Reserve() (uint64, error)     { f.next++; return f.next, nil }
func (f *fakeLegacyLane) ReserveSupplied(uint64) error { return nil }
func (f *fakeLegacyLane) Release(uint64) error         { return nil }

func TestOwnSuppliedStatementIDLaneRule(t *testing.T) {
	const own = "0x00000000000000000000000000000000000000aa"
	const lane = "5e1f0a2b7c9d3e4f"
	for _, tc := range []struct {
		name, queryID, lane string
		wantOK               bool
		wantErr              string
	}{
		{"foreign id is minted over", "0xbb:1:n", lane, false, ""},
		{"non-SI query id", "my-query", lane, false, ""},
		{"own id on the current lane", own + ":" + lane + ":7:n", lane, true, ""},
		{"own id, account case-folded", strings.ToUpper(own[:4]) + own[4:] + ":" + lane + ":7:n", lane, true, ""},
		{"legacy own id while lanes are on", own + ":7:n", lane, false, "SDK statement ids must use lane " + lane},
		{"own id on another lane", own + ":ffffffffffffffff:7:n", lane, false, "SDK statement ids must use lane " + lane},
		{"own id with an uppercase lane", own + ":5E1F0A2B7C9D3E4F:7:n", lane, false, "SDK statement ids must use lane " + lane},
		{"laned own id while lanes are off", own + ":" + lane + ":7:n", "", false, "SDK statement ids must use the legacy form while client lanes are off"},
		{"legacy own id while lanes are off", own + ":7:n", "", true, ""},
		{"malformed own id is minted over (unchanged legacy behaviour)", own + ":07:n", "", false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, ok, err := ownSuppliedStatementID(tc.queryID, own, tc.lane)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil || ok != tc.wantOK {
				t.Fatalf("ok=%v err=%v", ok, err)
			}
			if ok && (id.Account != own || id.Lane != tc.lane || id.Seq != 7) {
				t.Fatalf("id = %+v", id)
			}
		})
	}
}

func TestLaneRotationIsReadFromTheCodedRejection(t *testing.T) {
	for msg, want := range map[string]laneRotation{
		"storage_integrity: statement 0xaa:5e1f0a2b7c9d3e4f:9:n rejected by the arbiter: ADMISSION_CODE_GAP_BUDGET_EXCEEDED [client_seq unspent]":  rotationGapBudget,
		"storage_integrity: statement 0xaa:5e1f0a2b7c9d3e4f:1:n rejected by the arbiter: ADMISSION_CODE_LANE_BUDGET_EXCEEDED [client_seq unspent]": rotationLaneBudget,
		"storage_integrity: client lanes are not enabled on this network [client_seq unspent]":                                                  rotationLanesDisabled,
		"storage_integrity: statement 0xaa:1:n rejected by the arbiter: ADMISSION_CODE_DUPLICATE_CLIENT_SEQ":                                    rotationNone,
		"some other error": rotationNone,
	} {
		if got := laneRotationFor(msg); got != want {
			t.Errorf("laneRotationFor(%q) = %v, want %v", msg, got, want)
		}
	}
}

func newSelector(t *testing.T, mode LaneMode, maxInflight int) (*laneSelector, *fakeLegacyLane) {
	t.Helper()
	pool := openPool(t, t.TempDir(), LanePoolOptions{})
	return newLaneSelector(mode, func() (*LanePool, error) { return pool, nil }, maxInflight, nil), &fakeLegacyLane{}
}

func TestSelectorUsesLanesOnlyWhenEnabledAndAuto(t *testing.T) {
	sel, legacy := newSelector(t, LaneModeAuto, 16)
	lane, done, err := sel.pick(context.Background(), false, legacy)
	if err != nil || lane.Lane() != "" {
		t.Fatalf("lanes disabled on the network: %v %v", lane, err)
	}
	done()
	lane, done, err = sel.pick(context.Background(), true, legacy)
	if err != nil || len(lane.Lane()) != 16 {
		t.Fatalf("lanes enabled: %v %v", lane, err)
	}
	done()
	off, legacy2 := newSelector(t, LaneModeOff, 16)
	lane, done, err = off.pick(context.Background(), true, legacy2)
	if err != nil || lane.Lane() != "" {
		t.Fatalf("lanes off (driver sidecar): %v %v", lane, err)
	}
	done()
}

func TestSelectorRotatesOnGapBudgetAndPinsLegacyOnLaneBudget(t *testing.T) {
	sel, legacy := newSelector(t, LaneModeAuto, 16)
	first, done, _ := sel.pick(context.Background(), true, legacy)
	done()
	if err := sel.rotate(first.Lane()); err != nil {
		t.Fatal(err)
	}
	second, done, _ := sel.pick(context.Background(), true, legacy)
	done()
	if second.Lane() == first.Lane() || second.Lane() == "" {
		t.Fatalf("rotation kept lane %q", second.Lane())
	}
	sel.pinLegacy("lane budget exceeded")
	third, done, _ := sel.pick(context.Background(), true, legacy)
	done()
	if third.Lane() != "" {
		t.Fatal("after LANE_BUDGET_EXCEEDED the process must stay on the legacy lane")
	}
}

func TestSelectorCapsInflightPerLane(t *testing.T) {
	sel, legacy := newSelector(t, LaneModeAuto, 2)
	_, d1, _ := sel.pick(context.Background(), true, legacy)
	_, d2, _ := sel.pick(context.Background(), true, legacy)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, _, err := sel.pick(ctx, true, legacy); err == nil || !strings.Contains(err.Error(), "in flight") {
		t.Fatalf("third pick = %v, want an in-flight refusal", err)
	}
	d1()
	if _, d3, err := sel.pick(context.Background(), true, legacy); err != nil {
		t.Fatal(err)
	} else {
		d3()
	}
	d2()
}
```

A1 Task 16 already validates `storage_integrity.agent.lanes` (`""`/`auto`/`off`) in `validateAgent` and tests it in `pkg/config/storage_integrity_agent_config_test.go`. Add to that file's table (the `func(c *Config)` mutator / expected-substring rows A1 extended):

```go
		{"negative max_inflight_per_lane", func(c *Config) { c.StorageIntegrity.Agent.MaxInflightPerLane = -1 }, "storage_integrity.agent.max_inflight_per_lane"},
```

and append to the same file:

```go
func TestAgentMaxInflightPerLaneDefault(t *testing.T) {
	if got := (StorageIntegrityAgentConfig{}).EffectiveMaxInflightPerLane(); got != 16 {
		t.Fatalf("unset max_inflight_per_lane = %d, want 16", got)
	}
	if got := (StorageIntegrityAgentConfig{MaxInflightPerLane: 4}).EffectiveMaxInflightPerLane(); got != 4 {
		t.Fatalf("explicit max_inflight_per_lane = %d, want 4", got)
	}
}
```

Append to `lanes_test.go`:

```go
func TestParseLaneMode(t *testing.T) {
	for in, want := range map[string]LaneMode{"": LaneModeAuto, "auto": LaneModeAuto, "off": LaneModeOff} {
		if got, err := ParseLaneMode(in); err != nil || got != want {
			t.Errorf("ParseLaneMode(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"on", "OFF", "yes"} {
		if _, err := ParseLaneMode(bad); err == nil {
			t.Errorf("ParseLaneMode(%q) accepted", bad)
		}
	}
}

// The legacy counter must not be opened (and its lock not taken) by a process
// that uses a client lane: a second agent sharing the state directory would
// otherwise fail on ErrSeqLocked before acquiring its own lane.
func TestLegacyLaneOpensTheCounterOnlyWhenUsed(t *testing.T) {
	opened := 0
	dir := t.TempDir()
	legacy := legacyLane{open: func() (*SeqCounter, error) { opened++; return OpenSeqCounter(dir, "0x00000000000000000000000000000000000000aa") }}
	sel, _ := newSelector(t, LaneModeAuto, 16)
	lane, done, err := sel.pick(context.Background(), true, legacy)
	if err != nil || lane.Lane() == "" {
		t.Fatalf("lanes enabled: %v %v", lane, err)
	}
	if _, err := lane.Reserve(); err != nil {
		t.Fatal(err)
	}
	done()
	if opened != 0 {
		t.Fatalf("a laned statement opened the legacy counter %d times", opened)
	}
	lane, done, err = sel.pick(context.Background(), false, legacy)
	if err != nil || lane.Lane() != "" {
		t.Fatalf("lanes disabled: %v %v", lane, err)
	}
	if seq, err := lane.Reserve(); err != nil || seq != 1 || opened != 1 {
		t.Fatalf("legacy Reserve = %d, %v (opened %d)", seq, err, opened)
	}
	done()
}
```

(In this test `open` opens a fresh counter on every call, so the test reserves on the legacy lane exactly once; in the plugin `open` is PA3's `p.seqFor`, which caches one counter per network.)

Run: `cd "$W" && go test ./pkg/plugins/sistatement/ ./pkg/config/ -run 'OwnSupplied|LaneRotation|Selector|ParseLaneMode|LegacyLaneOpens|AgentMaxInflightPerLane|StorageIntegrityAgent' -v`
Expected: build failure for the new names.

- [ ] **Step 3: Implement `lanes.go`**

Create `pkg/plugins/sistatement/lanes.go`:

```go
package sistatement

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// LaneMode is storage_integrity.agent.lanes / -si-lanes / HOUSEGATE_SI_LANES.
type LaneMode string

const (
	// LaneModeAuto uses a client lane when the hosting indexer reports
	// client_lanes_enabled (spec 2026-10-09 §6.4).
	LaneModeAuto LaneMode = "auto"
	// LaneModeOff keeps legacy ids (the driver sidecar, R8).
	LaneModeOff LaneMode = "off"
)

func ParseLaneMode(s string) (LaneMode, error) {
	switch LaneMode(s) {
	case "", LaneModeAuto:
		return LaneModeAuto, nil
	case LaneModeOff:
		return LaneModeOff, nil
	}
	return "", fmt.Errorf("storage_integrity.agent.lanes must be %q or %q, got %q", LaneModeAuto, LaneModeOff, s)
}

// seqLane is the reservation surface shared by the legacy lane (Plan A) and a
// client lane (*LanedStore).
type seqLane interface {
	Lane() string
	Reserve() (uint64, error)
	ReserveSupplied(seq uint64) error
	Release(seq uint64) error
}

// LaneObserver is the optional metrics surface of lane selection.
type LaneObserver interface {
	LaneRotated(reason string)
	SIInflight(delta int)
}

// laneSelector picks the lane for each SI statement and bounds the number of
// statements in flight per lane, which bounds the gap ranges out-of-order
// arrival opens transiently at the arbiter (spec §6.5).
type laneSelector struct {
	mode        LaneMode
	openPool    func() (*LanePool, error)
	maxInflight int
	observer    LaneObserver

	mu        sync.Mutex
	cond      *sync.Cond
	pool      *LanePool
	current   *LanedStore
	legacyPin string // non-empty: this process stays on the legacy lane
	inflight  map[string]int
}

func newLaneSelector(mode LaneMode, openPool func() (*LanePool, error), maxInflight int, observer LaneObserver) *laneSelector {
	s := &laneSelector{mode: mode, openPool: openPool, maxInflight: maxInflight, observer: observer, inflight: map[string]int{}}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// pick returns the lane for one statement and a done func that must be called
// exactly once when the statement's outcome is known (success, Exception,
// abort or session close). It blocks while the lane is at its in-flight cap,
// until ctx ends; no seq is reserved by pick itself.
func (s *laneSelector) pick(ctx context.Context, lanesEnabled bool, legacy seqLane) (seqLane, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var lane seqLane = legacy
	if s.mode == LaneModeAuto && lanesEnabled && s.legacyPin == "" {
		if s.current == nil {
			if err := s.acquireLocked(); err != nil {
				return nil, nil, err
			}
		}
		lane = s.current
	}
	key := lane.Lane()
	stop := context.AfterFunc(ctx, func() { s.mu.Lock(); s.cond.Broadcast(); s.mu.Unlock() })
	defer stop()
	for s.maxInflight > 0 && s.inflight[key] >= s.maxInflight {
		if ctx.Err() != nil {
			return nil, nil, fmt.Errorf("storage_integrity agent: lane %q has %d statements in flight; retry", key, s.inflight[key])
		}
		s.cond.Wait()
	}
	s.inflight[key]++
	if s.observer != nil {
		s.observer.SIInflight(1)
	}
	var once sync.Once
	done := func() {
		once.Do(func() {
			s.mu.Lock()
			s.inflight[key]--
			s.cond.Broadcast()
			s.mu.Unlock()
			if s.observer != nil {
				s.observer.SIInflight(-1)
			}
		})
	}
	return lane, done, nil
}

func (s *laneSelector) acquireLocked() error {
	if s.pool == nil {
		pool, err := s.openPool()
		if err != nil {
			return fmt.Errorf("storage_integrity agent: open client lanes: %w", err)
		}
		s.pool = pool
	}
	store, reason, err := s.pool.Acquire()
	if err != nil {
		return fmt.Errorf("storage_integrity agent: acquire a client lane: %w", err)
	}
	s.current = store
	if reason != AcquireReused && s.observer != nil {
		s.observer.LaneRotated(string(reason))
	}
	return nil
}

// rotate abandons lane after GAP_BUDGET_EXCEEDED; the next pick acquires a
// new lane. A rotation for a lane that is no longer current is a no-op.
func (s *laneSelector) rotate(lane string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == nil || s.current.Lane() != lane {
		return nil
	}
	err := s.current.Abandon()
	s.current = nil
	if s.observer != nil {
		s.observer.LaneRotated("gap_budget")
	}
	return err
}

// pinLegacy keeps this process on the legacy lane after LANE_BUDGET_EXCEEDED.
func (s *laneSelector) pinLegacy(reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.legacyPin = reason
}

// laneRotation is what an upstream refusal asks the agent to do.
type laneRotation int

const (
	rotationNone laneRotation = iota
	rotationGapBudget
	rotationLaneBudget
	rotationLanesDisabled
)

// laneRotationFor reads the coded terminal rejection the server renders
// ("storage_integrity: statement <id> rejected by the arbiter: <CODE>") and
// the ingress pre-activation refusal.
func laneRotationFor(message string) laneRotation {
	switch {
	case strings.Contains(message, "rejected by the arbiter: ADMISSION_CODE_GAP_BUDGET_EXCEEDED"):
		return rotationGapBudget
	case strings.Contains(message, "rejected by the arbiter: "+sicore.AdmissionCodeLaneBudgetExceeded):
		return rotationLaneBudget
	case strings.Contains(message, "storage_integrity: client lanes are not enabled on this network"):
		return rotationLanesDisabled
	}
	return rotationNone
}

var errSDKLegacyWhileLanesOff = errors.New("SDK statement ids must use the legacy form while client lanes are off")

// ownSuppliedStatementID applies spec R9 to a client-supplied query id. ok is
// false (and err nil) when the id is not this account's statement id, in which
// case the agent mints one; err is non-nil when it is this account's id in the
// wrong lane form. A malformed own id keeps the legacy behaviour (minted over).
func ownSuppliedStatementID(queryID, ownAccount, lane string) (sicore.StatementID, bool, error) {
	account, rest, found := strings.Cut(strings.TrimSpace(queryID), ":")
	if !found || !strings.EqualFold(account, ownAccount) {
		return sicore.StatementID{}, false, nil
	}
	segments := strings.Split(rest, ":")
	switch {
	case lane != "" && (len(segments) != 3 || segments[0] != lane):
		return sicore.StatementID{}, false, fmt.Errorf("SDK statement ids must use lane %s", lane)
	case lane == "" && len(segments) == 3:
		return sicore.StatementID{}, false, errSDKLegacyWhileLanesOff
	}
	id, err := sicore.ParseStatementID(strings.ToLower(account) + ":" + rest)
	if err != nil || id.Account != strings.ToLower(ownAccount) {
		return sicore.StatementID{}, false, nil
	}
	return id, true, nil
}
```

(`segments` counts the parts after the account: two for a legacy id, three for a laned id. With lanes off only a laned own id is refused; a legacy own id falls through to the legacy parse.)

- [ ] **Step 4: Integrate into the plugin**

In `pkg/plugins/sistatement/plugin.go` (after Plan A):
1. `Options`: add `Lanes LaneMode`, `MaxInflightPerLane int`, `OpenLanePool func(siDir string) (*LanePool, error)` (nil = `OpenLanePool(siDir, LanePoolOptions{OnCorrupt: func(lane string, err error) { log.Warnw("sistatement: ignoring an untrusted client lane file", "lane", lane, "err", err) }, OnBurn: func(reason string) { p.observeSeq(func(o SeqObserver) { o.SeqBurned(reason) }) }})`, using PA3's `observeSeq` / `SeqObserver`).
2. Where PA3 lazily opens the legacy counter (`Options.OpenSeq(networkID)` / `p.seqFor`, A1 Task 14), create (once per network) `newLaneSelector(opts.Lanes, func() (*LanePool, error) { return open(siDir) }, opts.MaxInflightPerLane, laneObserver)` and keep it next to the counter; `laneObserver` is `opts.Observer` type-asserted to `LaneObserver`. `siDir` is `<base>/si/<network_id>/<signer>` when the agent uses the default state directory (A1 plan decision P4), and `<state_dir>/<signer>` when `storage_integrity.agent.state_dir` is explicit (A1 keeps the legacy `<state_dir>/<signer>.seq` there, and an explicit directory may be shared by several keys, whose lanes must never be shared: a lane file's `next` is per subject). Adapt A1's `*SeqCounter` to `seqLane` instead of changing its methods (its `Release` reports free-list overflow, which the adapter turns into the existing burn metric). The adapter opens the legacy counter lazily, on its first reservation: a process that uses a client lane must never take the legacy counter's lock, or a second agent sharing the state directory (spec §9.2) would fail on `ErrSeqLocked` before it could acquire its own lane:

```go
// legacyLane is the legacy (lane-less) client_seq counter of Plan A1 as a
// seqLane. open is PA3's p.seqFor bound to the statement's network; it is
// called only when the legacy lane is actually used.
type legacyLane struct {
	open   func() (*SeqCounter, error)
	onBurn func(reason string) // A1's SeqObserver.SeqBurned; nil = no metric
}

func (l legacyLane) Lane() string { return "" }

func (l legacyLane) Reserve() (uint64, error) {
	c, err := l.open()
	if err != nil {
		return 0, err
	}
	return c.Reserve()
}

func (l legacyLane) ReserveSupplied(seq uint64) error {
	c, err := l.open()
	if err != nil {
		return err
	}
	return c.ReserveSupplied(seq)
}

func (l legacyLane) Release(seq uint64) error {
	c, err := l.open()
	if err != nil {
		return err
	}
	overflow, err := c.Release(seq)
	if overflow && l.onBurn != nil {
		l.onBurn("free_list_overflow")
	}
	return err
}
```

Add a unit test `TestLegacyLaneOpensTheCounterOnlyWhenUsed` in `lanes_test.go`: a `legacyLane` whose `open` counts calls is passed to `pick` with lanes enabled (a temp-dir pool) and `open` is never called; with lanes disabled, `Reserve` calls it once.

Change A1's `reservedSeq.counter *SeqCounter` (A1 Task 14) to `lane seqLane`, so `OnException`'s release-on-marker and every pre-send release go to the lane that reserved the seq. The pre-send release is the one housegate#225 shipped after A1 (in the housegate release that follows v0.17.0): Relay sets `QueryContext.UpstreamQueryUnsent` through `abortUnsentQuery` only before `WriteQuery` in `forwardSignedInsert` (later strict-hook refusal, nil upstream, lost `beginActiveQuery` race), refuses it once the write began, and `sistatement.OnQueryAbort` releases only when that flag is set and the reservation matches the statement id; this task routes that existing release to `lane` and adds, in `pkg/plugins/sistatement` and `pkg/proxy`, laned variants of #225's tests (each pre-send site releases to the reserving client lane; a failed `WriteQuery` stays burned on a client lane); A1's `releaseSeq(seq)` (A1 Task 12; A1 Task 14 routes it to the statement's counter) becomes `releaseSeq(lane seqLane, seq uint64)`: an error counts `SeqBurned("unknown_outcome")`, success counts `SeqRecycled()`; a free-list overflow is counted by the store itself (the adapter above, or `LanePoolOptions.OnBurn` for a client lane), because the entry an overflow drops is the largest free seq, not necessarily the one being released.
3. At PA3's reservation point in `OnQueryInputCompleteStrict`, before reserving: `lane, done, err := sel.pick(ctx, lanesEnabled, legacyLane{open: func() (*SeqCounter, error) { return p.seqFor(st.networkID) }, onBurn: burn})` (`burn` the same `SeqBurned` callback as above; A1 Task 14 must no longer open the counter before this point) where `lanesEnabled` is `info.ClientLanesEnabled` from PA4's cached `registry.StorageIntegrityInfo` of the hosting indexer when `Options.Discovery` is set, and otherwise `opts.ClientLanesEnabled != nil && opts.ClientLanesEnabled()` (new `sistatement.Options.ClientLanesEnabled func() bool`; A1's agent without discovery — a YAML or host-injected status source, and every integration fixture — has no SI info to read); on error refuse locally (no seq reserved). Then apply `ownSuppliedStatementID(clientQueryID, p.account, lane.Lane())` (replacing PA3's legacy-only SDK check): `err` → `done()` and refuse with that message; `ok` → `lane.ReserveSupplied(id.Seq)` and keep the supplied id; otherwise `seq, err := lane.Reserve()` and mint `sicore.StatementID{Account: p.account, Lane: lane.Lane(), Seq: seq, Nonce: hex(16 random bytes)}.Flat()`. Store `lane` and `done` on the pending statement; every PA3 path that releases a reserved seq before the Query is written (signing or local write failure) calls `lane.Release(seq)`; log the lane at info with the statement id.
4. In the single function where PA3 removes `p.pending[sessID]` (success, exception, abort, close), call `st.done()` if set.
5. In PA3's `OnException`, after PA3's release-on-marker handling for the active statement: `switch laneRotationFor(exc.Message)`: `rotationGapBudget` with a laned `st` → `sel.rotate(st.lane.Lane())` (log warn with old lane) and `exc.Message += "; retry: the agent moved to a new client_seq lane"`; `rotationLaneBudget` → `sel.pinLegacy(...)`, log error `"client lane budget exhausted for <account>; this process stays on the legacy client_seq lane"`, and append `"; retry: the agent switched to its legacy client_seq lane"`; `rotationLanesDisabled` → delete the statement's database from PA4's `p.infos` (under `p.mu`) so the next statement re-reads `client_lanes_enabled` from the hosting indexer. Independently of refusals, timestamp every successful `p.infos` entry and treat one older than 60 s as absent, so an agent that cached `client_lanes_enabled: false` before activation picks it up without a restart (spec §6.4); add a unit test with a fake clock and a `Discovery` that answers false then true, asserting the second statement after the expiry is laned. A legacy-lane GAP_BUDGET keeps PA3's behaviour.
6. When `sel.pinLegacy` is active and the legacy store cannot be opened because another process holds its lock (PA3 returns its lock error), refuse SI writes with `"storage_integrity agent: client lane budget exhausted and the legacy client_seq lane is held by another process"`.

`pkg/plugins/sistatement/observer.go`: document `LaneObserver` there (move the interface from `lanes.go` if preferred). `pkg/proxy/observer.go`: add

```go
	agentSILaneRotationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "clickhouse_proxy_agent_si_lane_rotations_total",
		Help: "Agent client_seq lane changes by reason: gap_budget (abandoned after GAP_BUDGET_EXCEEDED), lost_state (no lane files existed), new_process (every existing lane was held or abandoned)",
	}, []string{"reason"})
	agentSIInflight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "clickhouse_proxy_agent_si_inflight",
		Help: "Agent SI statements between client_seq reservation and outcome",
	})
```

registered with the others, and `func (m *MetricsObserver) LaneRotated(reason string) { agentSILaneRotationsTotal.WithLabelValues(reason).Inc() }`, `func (m *MetricsObserver) SIInflight(delta int) { agentSIInflight.Add(float64(delta)) }`.

- [ ] **Step 5: Config, flag, env, wiring**

`StorageIntegrityAgentConfig.Lanes` (`lanes`, `""`/`auto`/`off`), its validation, the `-si-lanes` flag and the `HOUSEGATE_SI_LANES` env already exist from A1 Task 16; in this task `""` and `auto` start meaning "use a lane when lanes are enabled" (A1 treated both as the legacy lane), and only the in-flight cap is new. In `StorageIntegrityAgentConfig` add:

```go
	// MaxInflightPerLane bounds SI statements between reservation and
	// outcome on one lane (default 16).
	MaxInflightPerLane int `json:"max_inflight_per_lane" yaml:"max_inflight_per_lane"`
```

plus `func (a StorageIntegrityAgentConfig) EffectiveMaxInflightPerLane() int` (0 → 16) and, in A1's `validateAgent`, `if a.MaxInflightPerLane < 0 { errs = append(errs, fmt.Errorf("storage_integrity.agent.max_inflight_per_lane %d must not be negative", a.MaxInflightPerLane)) }`; `buildAgent` passes `EffectiveMaxInflightPerLane()` (a plugin built directly with `sistatement.Options.MaxInflightPerLane == 0` stays unbounded, which only unit tests do); map `Lanes` onto `sistatement.LaneMode` with `ParseLaneMode` (`""` → `auto`). In `build.go` `buildAgent`, pass `Lanes`, `MaxInflightPerLane`, the observer and `ClientLanesEnabled: opts.StorageIntegrityClientLanes` (Task 13's `housegate.Options` field, reused in agent mode: "client lanes are active on this host's network"; consulted only when the agent has no RPC discovery) into `sistatement.Options`.

- [ ] **Step 6: Run the unit suite**

Run: `cd "$W" && bazel run //:gazelle && bazel test //pkg/plugins/sistatement:all //pkg/config:all //:housegate_test`
Expected: PASS, including every Plan A sistatement test (legacy behaviour unchanged when lanes are disabled or `off`).

- [ ] **Step 7: Integration tests (docker-bound)**

In `pkg/integration/storage_integrity_agent_test.go`, turn the existing `startSIAgentPair(t, networkID) (*testenv.TestProxy, *capturingConsumer)` fixture (on `main` since envelope v2, as amended by Plan A1) into a wrapper over a new `startSIAgentPairWith(t, networkID string, o siAgentPairOptions) (agent, server *testenv.TestProxy, consumer *capturingConsumer)` with `type siAgentPairOptions struct{ ClientLanes bool; AgentStateDir string; Lanes string }`: `ClientLanes` appends `func(_ *config.Config, opts *housegate.Options) { opts.StorageIntegrityClientLanes = func() bool { return true } }` to both the server's and the agent's proxy options (the agent fixture uses a declared YAML schema and has no RPC discovery, so Step 4 item 3's `ClientLanesEnabled` fallback is what it reads); `AgentStateDir` replaces `t.TempDir()` for `cfg.StorageIntegrity.Agent.StateDir` when non-empty; `Lanes` sets `cfg.StorageIntegrity.Agent.Lanes`. `startSIAgentPair` keeps its signature and calls `startSIAgentPairWith(t, networkID, siAgentPairOptions{})`. Add a helper `startSecondAgent(t *testing.T, serverAddr, networkID, stateDir, lanes string) *testenv.TestProxy` that starts another agent with the same `authTestKey1` against the same server with the identical agent options. Then add:

```go
func insertOneRow(t *testing.T, addr string, id uint64) {
	t.Helper()
	conn := openConnNoCompression(t, addr)
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+siTenantDB+".si_events")
	if err != nil {
		t.Fatal(err)
	}
	if err := batch.Append(id, "eu"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Send(); err != nil {
		t.Fatal(err)
	}
}

func admittedIDs(t *testing.T, c *capturingConsumer, want int) []sicore.StatementID {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		c.mu.Lock()
		n := len(c.seen)
		c.mu.Unlock()
		if n >= want || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.seen) != want {
		t.Fatalf("admissions = %d, want %d", len(c.seen), want)
	}
	var out []sicore.StatementID
	for _, adm := range c.seen {
		id, err := sicore.ParseStatementID(adm.StatementID)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, id)
	}
	return out
}

func TestStorageIntegrity_TwoAgentsOneKeySeparateStateDirsUseDistinctLanes(t *testing.T) {
	first, server, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{ClientLanes: true})
	second := startSecondAgent(t, server.Addr, "itest-net", t.TempDir(), "auto")
	insertOneRow(t, first.Addr, 1)
	insertOneRow(t, second.Addr, 2)
	ids := admittedIDs(t, consumer, 2)
	if !ids[0].IsLaned() || !ids[1].IsLaned() || ids[0].Lane == ids[1].Lane || ids[0].Seq != 1 || ids[1].Seq != 1 {
		t.Fatalf("ids = %+v: one key, two agents must use two lanes, each starting at seq 1", ids)
	}
}

func TestStorageIntegrity_TwoAgentsSharingAStateDirUseDistinctLanes(t *testing.T) {
	dir := t.TempDir()
	first, server, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{ClientLanes: true, AgentStateDir: dir})
	second := startSecondAgent(t, server.Addr, "itest-net", dir, "auto")
	insertOneRow(t, first.Addr, 1)
	insertOneRow(t, second.Addr, 2)
	ids := admittedIDs(t, consumer, 2)
	if ids[0].Lane == ids[1].Lane {
		t.Fatalf("two processes on one state dir shared lane %s", ids[0].Lane)
	}
}

func TestStorageIntegrity_LostStateDirStartsANewLane(t *testing.T) {
	dir := t.TempDir()
	first, server, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{ClientLanes: true, AgentStateDir: dir})
	insertOneRow(t, first.Addr, 1)
	first.Close()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	second := startSecondAgent(t, server.Addr, "itest-net", dir, "auto")
	insertOneRow(t, second.Addr, 2)
	ids := admittedIDs(t, consumer, 2)
	if ids[0].Lane == ids[1].Lane || ids[1].Seq != 1 {
		t.Fatalf("ids = %+v: a lost state dir must mint a new lane rather than collide", ids)
	}
}

func TestStorageIntegrity_LanesOffKeepsLegacyIDs(t *testing.T) {
	agent, _, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{ClientLanes: true, Lanes: "off"})
	insertOneRow(t, agent.Addr, 1)
	if ids := admittedIDs(t, consumer, 1); ids[0].IsLaned() {
		t.Fatalf("lanes off produced %+v", ids[0])
	}
}

func TestStorageIntegrity_LanedIDRefusedBeforeActivation(t *testing.T) {
	agent, _, consumer := startSIAgentPairWith(t, "itest-net", siAgentPairOptions{})
	insertOneRow(t, agent.Addr, 1)
	if ids := admittedIDs(t, consumer, 1); ids[0].IsLaned() {
		t.Fatal("an agent must not emit a laned id while the indexer reports client lanes disabled")
	}
}
```

(`startSIAgentPairWith` is the options-taking form of `startSIAgentPair`, returning the agent proxy, the server proxy and the consumer; keep `startSIAgentPair` as a wrapper so Plan A's tests are untouched. Use the proxy's existing close/stop method in place of `first.Close()` — `git grep -n 'func (p \*TestProxy)' pkg/integration/testenv/` names it.)

Run: `cd "$W" && docker context ls && bazel test //pkg/integration:integration_test --test_filter='TwoAgents|LostStateDir|LanesOff|LanedIDRefused' --test_output=errors --test_env=DOCKER_HOST=<socket from docker context ls> --test_env=HOME`
Expected: PASS.

- [ ] **Step 8: Docs**

Update `CLAUDE.md`'s `sistatement` bullet (one paragraph per line): statement id `<account>:<seq>:<nonce>` or, with lanes, `<account>:<lane>:<seq>:<nonce>`; `storage_integrity.agent.lanes` (`auto`/`off`, env `HOUSEGATE_SI_LANES`), the lane pool under `<si_dir>/lanes` with a lifetime `flock` (Linux/macOS only), smallest-free-seq reuse, rotation on `GAP_BUDGET_EXCEEDED`, legacy pin on `LANE_BUDGET_EXCEEDED`, R9 SDK rule, `max_inflight_per_lane`; and in the `storageintegrity` ingress bullet the `Options.StorageIntegrityClientLanes` gate. Mention `pkg/auth/testdata/statement_jws_v2_lanes.json` / `SharedStatementLaneVectorsSHA256` next to the existing vector sentence.

- [ ] **Step 9: Full verification**

Run: `cd "$W" && bazel build //... && bazel test //...` and the full integration list from CI: `bazel test //pkg/integration:integration_test //pkg/integration/testenv:testenv_test --test_output=errors --test_env=DOCKER_HOST=<socket> --test_env=HOME`
Expected: PASS; compare any integration failure against a clean `main` run (main-baseline rule) before calling it a regression.

- [ ] **Step 10: Commit**

```bash
cd "$W" && git add -A && git commit -m "feat(sistatement): client_seq lanes in the agent

Lanes are used when the hosting indexer reports client_lanes_enabled and
storage_integrity.agent.lanes is auto; GAP_BUDGET_EXCEEDED abandons and
rotates the lane, LANE_BUDGET_EXCEEDED pins the legacy lane, SDK ids must
carry the current lane (R9), and at most max_inflight_per_lane statements
are in flight per lane (housegate spec 2026-10-09 D7, D15, D16, §6.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 11: PR and merge (requires explicit user confirmation)**

Push, `gh pr create --repo housegate/housegate --fill`, wait for the `Build` and `Integration (ClickHouse)` jobs, merge after review.

- [ ] **Step 12: Release `HG_TAG` (requires explicit user confirmation)**

Run the release workflow (`.github/workflows/release.yml`, `gh workflow run release.yml --repo housegate/housegate --ref main`); record `HG_TAG`; verify by content: `git -C /Users/uranuswch/Dev/housegate/housegate fetch -q --tags && git -C /Users/uranuswch/Dev/housegate/housegate show "$HG_TAG":pkg/auth/shared_vectors.go | grep -c SharedStatementLaneVectorsSHA256` prints `1`. If only the Homebrew job fails, re-run that job alone.

---

## Task 17: sentio-node — laned adapter, `client_lanes_enabled` from the registry follower, pins (B5)

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel` (`bazel_dep` + `git_override` for `housegate` and `arbiter_core`), `MODULE.bazel.lock`
- Modify: `storageintegrityadapter/adapter.go` (delete `parseStatementID`; `toArbiterEnvelope`), `storageintegrityadapter/adapter_test.go`
- Modify: `storageintegrityadapter/tablestate/model.go` (`Registry.ClientLanes`), `arbitercore.go` (`followerSource.View`), `state.go` (`ClientLanesEnabled`), `arbitercore_test.go`, `state_test.go`
- Modify: `standalone/storage_integrity_table_state.go`, `standalone/standalone.go:405-420` (`housegate.Options`), the PA5 info source `standalone/storage_integrity_info.go` (`(*storageIntegrityTableStateRuntime).StorageIntegrityInfo`) and its test in `standalone/storage_integrity_table_state_test.go`

**Interfaces:**
- Consumes: Task 3 `wire.TableRegistrySnapshot.ClientLanes`; Task 13 `sicore.ParseStatementID`, `housegate.Options.StorageIntegrityClientLanes`; PA5 `sentio_getStorageIntegrityInfo` (fields `enabled`, `network_id`, `keeper_shard_id`, `si_indexer_id`, `self_indexer_id`, `client_lanes_enabled`, `ingress_max_payload_bytes`, `max_token_age_seconds`, `default_read_mode`, `registry_version`, `server_unix_time`).
- Produces: `tablestate.ClientLanes{MaxLanesPerAccount uint32}`; `tablestate.Registry.ClientLanes *ClientLanes`; `func (s *State) ClientLanesEnabled() bool`; `client_lanes_enabled` answered from the follower snapshot; housegate ingress gate fed; `SN_COMMIT`, `SN_DIGEST`.

Scope notes (from Plan A2): the registry injected into the embedded HouseGate is `RedisNetworkState` (`standalone/standalone.go:159`), not `FromStatecore`; this task adds nothing to that registry — the lane flag comes from the storage-integrity table-state runtime, which exists only on the SI node. arbiter-core's SNode promotes per (table, partition) without a per-statement callback, so nothing here emits `OnStatementSafe`.

- [ ] **Step 1: Worktree and contract check**

Create the worktree (URWT command of Task 1 Step 1 with `/Users/uranuswch/Dev/sentio_xyz/sentio-node`). Run `cd "$W" && git grep -n "client_lanes_enabled\|ClientLanesEnabled" -- rpc standalone` — expect `rpc/storage_integrity_info.go`'s JSON tag and `standalone/storage_integrity_info.go` setting `ClientLanesEnabled: false` (A2 Task 3), plus A2's `require.False(t, got.ClientLanesEnabled, "Phase A never advertises lanes")`; stop otherwise.

- [ ] **Step 2: Pins**

```bash
cd "$W"
HG_SHA="$(git -C /Users/uranuswch/Dev/housegate/housegate rev-parse "$HG_TAG^{commit}")"
CORE_SHA="$(git -C /Users/uranuswch/Dev/sentio_xyz/arbiter-core rev-parse "$CORE_TAG^{commit}")"
go get "github.com/housegate/housegate@$HG_TAG" "github.com/sentioxyz/arbiter-core@$CORE_TAG" && go mod tidy
```

Edit `MODULE.bazel`: `bazel_dep(name = "housegate", version = "<HG_TAG without v>")`, `bazel_dep(name = "arbiter_core", version = "<CORE_TAG without v>")`, the two `git_override` `commit` values to `$HG_SHA` / `$CORE_SHA`, and their `# Resolved …` comments. Then `bazel run @rules_go//go -- mod tidy && bazel mod tidy && bazel build //...`. Confirm `grep -n "$HG_SHA\|$CORE_SHA" MODULE.bazel` prints both lines (the silent-failure check of the upgrade-dependency skill). Commit `chore(deps): pin housegate $HG_TAG and arbiter-core $CORE_TAG` with the trailer.

- [ ] **Step 3: Write the failing tests**

Append to `storageintegrityadapter/adapter_test.go`:

```go
func TestToArbiterEnvelopeCarriesTheClientLane(t *testing.T) {
	got, err := toArbiterEnvelope(validEnvelope("0xabc:5e1f0a2b7c9d3e4f:7:n"))
	require.NoError(t, err)
	require.Equal(t, arbiter.StatementID{ClientAccount: "0xabc", ClientLane: "5e1f0a2b7c9d3e4f", ClientSeq: 7, ClientNonce: "n"}, got.StatementID)
	require.Equal(t, "0xabc:5e1f0a2b7c9d3e4f:7:n", got.StatementID.Flat(), "the SNode's row ids derive from this flat id")
	legacy, err := toArbiterEnvelope(validEnvelope("0xabc:7:n"))
	require.NoError(t, err)
	require.Empty(t, legacy.StatementID.ClientLane)
	for _, bad := range []string{"0xabc:5E1F0A2B7C9D3E4F:7:n", "0xabc:5e1f0a2b7c9d3e4:7:n", "0xabc:x:y:7:n"} {
		_, err := toArbiterEnvelope(validEnvelope(bad))
		require.Error(t, err, bad)
	}
}
```

Append to `storageintegrityadapter/tablestate/arbitercore_test.go`:

```go
type laneFollower struct{ snap wire.TableRegistrySnapshot }

func (f laneFollower) View() (wire.TableRegistrySnapshot, bool) { return f.snap, true }
func (f laneFollower) Changed() <-chan struct{}                 { return nil }
func (f laneFollower) Connected() bool                          { return true }

func TestFollowerSourceMapsClientLanes(t *testing.T) {
	view, ok := FromFollower(laneFollower{wire.TableRegistrySnapshot{Version: 3}}).View()
	if !ok || view.ClientLanes != nil {
		t.Fatalf("disabled lanes: %+v", view.ClientLanes)
	}
	view, _ = FromFollower(laneFollower{wire.TableRegistrySnapshot{Version: 4, ClientLanes: &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}}}).View()
	if view.ClientLanes == nil || view.ClientLanes.MaxLanesPerAccount != 256 {
		t.Fatalf("enabled lanes: %+v", view.ClientLanes)
	}
}
```

(imports `github.com/sentioxyz/arbiter-core` as `arbiter` and `github.com/sentioxyz/arbiter-core/wire`.)

Append to `storageintegrityadapter/tablestate/state_test.go` (construct the fakes exactly as the file's existing `New(Config{…})` tests do):

```go
func TestClientLanesEnabledFollowsTheRegistry(t *testing.T) {
	reg := &fakeRegistry{}
	s, err := New(Config{Registry: reg, Readiness: &fakeReadiness{}, Chain: &fakeChain{}, NetworkID: "devnet2"})
	if err != nil {
		t.Fatal(err)
	}
	if s.ClientLanesEnabled() {
		t.Fatal("lanes enabled before the first refresh")
	}
	reg.set(true, Registry{Version: 3})
	if err := s.Refresh(context.Background()); err != nil || s.ClientLanesEnabled() {
		t.Fatalf("registry without client_lanes: enabled=%v err=%v", s.ClientLanesEnabled(), err)
	}
	reg.set(true, Registry{Version: 4, ClientLanes: &ClientLanes{MaxLanesPerAccount: 256}})
	if err := s.Refresh(context.Background()); err != nil || !s.ClientLanesEnabled() {
		t.Fatalf("registry with client_lanes: enabled=%v err=%v", s.ClientLanesEnabled(), err)
	}
	reg.set(false, Registry{})
	if err := s.Refresh(context.Background()); err != nil || s.ClientLanesEnabled() {
		t.Fatal("a disabled registry must report lanes disabled")
	}
}
```

Extend PA5's info handler test with a case where the injected source reports lanes enabled and the RPC answers `client_lanes_enabled: true` (same table-test shape PA5 used for `false`).

Run: `cd "$W" && go test ./storageintegrityadapter/... ./rpc/... -run 'ClientLane|ClientLanes' -v`
Expected: build failures (`unknown field ClientLanes`, `s.ClientLanesEnabled undefined`) and the adapter test FAILS (the local parser refuses four segments).

- [ ] **Step 4: Implement**

`adapter.go`: delete `parseStatementID` (and its now-unused `isLowerHex`/`isDecimal` helpers if nothing else uses them); in `toArbiterEnvelope`:

```go
	parsed, err := sicore.ParseStatementID(env.StatementID)
	if err != nil {
		return arbiter.StatementEnvelope{}, fmt.Errorf("statement id %q: %w", env.StatementID, err)
	}
	id := arbiter.StatementID{ClientAccount: parsed.Account, ClientLane: parsed.Lane, ClientSeq: parsed.Seq, ClientNonce: parsed.Nonce}
```

`tablestate/model.go`:

```go
// ClientLanes mirrors the arbiter's committed client-lane parameter
// (housegate spec 2026-10-09 D13); nil until activation.
type ClientLanes struct{ MaxLanesPerAccount uint32 }
```

and `ClientLanes *ClientLanes` on `Registry`. `tablestate/arbitercore.go` `View()`: after building `out`, `if l := snapshot.ClientLanes; l != nil { out.ClientLanes = &ClientLanes{MaxLanesPerAccount: l.MaxLanesPerAccount} }`. `tablestate/state.go`: add `clientLanes atomic.Bool` to `State`, in `Refresh` right after `view, enabled := s.cfg.Registry.View()` store `s.clientLanes.Store(enabled && view.ClientLanes != nil)` (before the chain read, so a failing chain read does not hide activation), and:

```go
// ClientLanesEnabled reports whether the latest registry view the state read
// carries the committed client_lanes parameter. Lanes are never disabled
// once committed; before the first refresh this is false.
func (s *State) ClientLanesEnabled() bool { return s.clientLanes.Load() }
```

`standalone/storage_integrity_table_state.go`:

```go
// clientLanesEnabled is housegate.Options.StorageIntegrityClientLanes and the
// source of sentio_getStorageIntegrityInfo.client_lanes_enabled; a nil runtime
// (no storage integrity on this node) yields nil, which housegate reads as
// disabled.
func (r *storageIntegrityTableStateRuntime) clientLanesEnabled() func() bool {
	if r == nil {
		return nil
	}
	return r.state.ClientLanesEnabled
}
```

`standalone/standalone.go`: add `StorageIntegrityClientLanes: siTableState.clientLanesEnabled(),` to the `housegate.Options` literal. In `standalone/storage_integrity_info.go`, `(*storageIntegrityTableStateRuntime).StorageIntegrityInfo()` sets `ClientLanesEnabled: r.state.ClientLanesEnabled()` instead of the constant `false` (a node without the SI runtime has no runtime and keeps answering `enabled: false`, `client_lanes_enabled: false` from `rpc.GetStorageIntegrityInfo`'s nil-source path). In `standalone/storage_integrity_table_state_test.go` replace A2's `require.False(t, got.ClientLanesEnabled, "Phase A never advertises lanes")` with a case per registry view: without `client_lanes` → false, with `client_lanes` → true.

- [ ] **Step 5: Run and commit**

Run: `cd "$W" && bazel run //:gazelle && bazel build //... && bazel test //...`
Expected: PASS.

```bash
cd "$W" && git add -A && git commit -m "feat(storageintegrity): client lanes from the arbiter registry follower

The adapter parses laned statement ids with housegate's shared grammar and
forwards client_lane to the SNode; client_lanes_enabled in
sentio_getStorageIntegrityInfo and the embedded HouseGate's ingress gate
follow the registry snapshot's client_lanes (housegate spec 2026-10-09
§6.7).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 6: PR, merge, image (requires explicit user confirmation)**

sentio-node always goes through a PR: push, `gh pr create --repo sentioxyz/sentio-node --fill`, wait for CI, merge. Record the merge commit as `SN_COMMIT` and the main CI `docker-push` image `ghcr.io/sentioxyz/sentio-node:sha-$SN_COMMIT` index digest as `SN_DIGEST` (`docker buildx imagetools inspect`, OCI revision must equal `SN_COMMIT`).

---

## Task 18: arbiter — adopt the housegate lane vectors verbatim (B3 follow-up, test-only)

**Files:**
- Modify: `go.mod`, `go.sum`, `MODULE.bazel`, `MODULE.bazel.lock` (`scripts/update-housegate.sh "$HG_TAG"`)
- Create: `fsm/testdata/statement_jws_v2_lanes.json` (verbatim copy), `fsm/shared_lane_vectors_test.go`

**Interfaces:**
- Consumes: Task 14 `pkg/auth/testdata/statement_jws_v2_lanes.json`, `auth.SharedStatementLaneVectorsSHA256`; Task 2 `arbiter.StatementID.Flat`; Task 7 `verifyUserJWSV2`.
- Produces: the cross-repo check that housegate's signer, arbiter-core's renderer and the FSM's verifier agree on laned ids. It ships in the next arbiter release; it does not gate the Task 20 images (the housegate bump changes no FSM code path, and the Task 5 golden proves it).

- [ ] **Step 1: Bump and copy**

```bash
cd "$W" && git switch -c urwt/claude/client-seq-lane-vectors origin/main
bash scripts/update-housegate.sh "$HG_TAG"
git -C /Users/uranuswch/Dev/housegate/housegate show "$HG_TAG":pkg/auth/testdata/statement_jws_v2_lanes.json > fsm/testdata/statement_jws_v2_lanes.json
```

(Use a fresh URWT worktree for this branch if the Task 5–12 worktree was removed.)

- [ ] **Step 2: Write the test**

Create `fsm/shared_lane_vectors_test.go`:

```go
package fsm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/housegate/housegate/pkg/auth"

	"github.com/sentioxyz/arbiter-core"
)

type laneVector struct {
	Name          string                     `json:"name"`
	Expect        string                     `json:"expect"`
	RejectField   string                     `json:"reject_field"`
	ClientAccount string                     `json:"client_account"`
	ClientLane    string                     `json:"client_lane"`
	ClientSeq     uint64                     `json:"client_seq"`
	ClientNonce   string                     `json:"client_nonce"`
	Payload       auth.JWSStatementPayloadV2 `json:"payload"`
	Token         string                     `json:"token"`
}

// TestSharedLaneVectorsAreByteIdenticalToHousegate is the cross-repo link for
// testdata/statement_jws_v2_lanes.json. Re-copy the file from the pinned
// housegate module instead of editing the constant.
func TestSharedLaneVectorsAreByteIdenticalToHousegate(t *testing.T) {
	raw, err := os.ReadFile("testdata/statement_jws_v2_lanes.json")
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if got := hex.EncodeToString(sum[:]); got != auth.SharedStatementLaneVectorsSHA256 {
		t.Fatalf("fsm/testdata/statement_jws_v2_lanes.json sha256 = %s, housegate publishes %s", got, auth.SharedStatementLaneVectorsSHA256)
	}
}

// TestLaneVectorsAgainstRendererAndVerifier rebuilds each envelope from the
// structured fields with arbiter-core's StatementID and runs the FSM's
// deterministic verifier: housegate's signer, arbiter-core's Flat and
// verifyUserJWSV2 must agree on every vector.
func TestLaneVectorsAgainstRendererAndVerifier(t *testing.T) {
	raw, err := os.ReadFile("testdata/statement_jws_v2_lanes.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Vectors []laneVector `json:"vectors"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Vectors) != 6 {
		t.Fatalf("lane vectors = %d, want 6", len(file.Vectors))
	}
	for _, v := range file.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			id := arbiter.StatementID{ClientAccount: v.ClientAccount, ClientLane: v.ClientLane, ClientSeq: v.ClientSeq, ClientNonce: v.ClientNonce}
			if (id.Flat() == v.Payload.StatementID) != (v.Expect == "accept") {
				t.Fatalf("Flat %q vs signed %q disagrees with expect=%s", id.Flat(), v.Payload.StatementID, v.Expect)
			}
			env := arbiter.StatementEnvelope{
				StatementID: id, StatementKind: arbiter.StatementKind(v.Payload.StatementKind), SQL: "irrelevant-for-jws-vector",
				SQLHash: v.Payload.SQLHash, SettingsHash: v.Payload.SettingsHash, PayloadHash: v.Payload.PayloadHash,
				PayloadLength: v.Payload.PayloadLength, TargetTableID: v.Payload.TargetTableID, UserJWS: v.Token,
				EnvelopeVersion: 2, NetworkID: v.Payload.NetworkID, KeeperShardID: v.Payload.KeeperShardID,
				PayloadFormat: v.Payload.PayloadFormat, ClientRevision: v.Payload.ClientRevision,
				SchemaHash: v.Payload.SchemaHash, RowIDProfileID: v.Payload.RowIDProfileID,
			}
			err := verifyUserJWSV2(env)
			switch v.Expect {
			case "accept":
				if err != nil {
					t.Fatalf("expected accept: %v", err)
				}
			case "reject":
				if err == nil || err.Error() != "user_jws: "+v.RejectField+" does not bind the envelope" {
					t.Fatalf("expected a %s binding reject, got %v", v.RejectField, err)
				}
			}
		})
	}
}
```

- [ ] **Step 3: Run the determinism ladder and commit**

Run: `cd "$W" && bazel test //...`
Expected: PASS, in particular `TestLegacyHistoryIsByteIdenticalAcrossTheLaneRelease` (the housegate bump must not move any replayed byte), the existing shared-vector tests and the two new ones. If the golden fails, stop: the housegate bump changed a replay path and needs its own investigation.

```bash
cd "$W" && git add -A && git commit -m "test(fsm): adopt housegate's client-lane statement JWS vectors verbatim

Pins housegate $HG_TAG; the copy must hash to
auth.SharedStatementLaneVectorsSHA256 and every vector must agree across
housegate's signer, arbiter-core's Flat and verifyUserJWSV2 (housegate spec
2026-10-09 §7).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

PR and merge require explicit user confirmation.

---

## Task 19: production — sidecar lane setting check and the lane-activation runbook stage (B6, PR)

**Files:**
- Modify: `docs/storage-integrity-devnet2-dynamic-table-set.md` (new section "7. Stage 5: client_seq lanes" inserted before A2's "7. Operating rules", which becomes "8. Operating rules"; table row in the stage table at the top)

**Interfaces:**
- Consumes: PA6 (A2 Task 7's chart renders `HOUSEGATE_SI_LANES=off` for the driver sidecar, which housegate v0.15.0 ignores and a lane-aware image honours, and `WriterAuthorizationTest.test_sidecar_stays_on_legacy_lanes` asserts it; A2 Task 8's runbook §6 Stage 4 and §7 Operating rules); `ARBITER_TAG`/`ARBITER_DIGEST`, `SN_COMMIT`/`SN_DIGEST` for the runbook's pin table.
- Produces: the runbook stage Task 20 follows.

- [ ] **Step 1: Worktree and the sidecar check**

Create the production worktree (URWT command of Task 1 Step 1 with `/Users/uranuswch/Dev/sentio_xyz/production`). Run:

```bash
cd "$W" && git grep -n "HOUSEGATE_SI_LANES" -- charts/sentio-node && git grep -n "def test_sidecar_stays_on_legacy_lanes" -- charts/storage-integrity/tests/test_render.py && python3 -B charts/storage-integrity/tests/test_render.py WriterAuthorizationTest
```

Expected: the template renders `HOUSEGATE_SI_LANES` from `housegateSidecar.storageIntegrity.lanes` (default `"off"`) on the `housegate-sidecar` container, `test_sidecar_stays_on_legacy_lanes` exists, and `WriterAuthorizationTest` passes. This guards R8: the sidecar must stay on legacy ids after activation. If the test is missing, Phase A is incomplete: stop and finish Plan A2 Task 7 instead of adding a second copy here.

- [ ] **Step 2: Write the runbook stage**

Append a row to the stage table (after A2's stage 4 row): `| 5 | client_seq lanes: arbiter $ARBITER_TAG (verifiers, then voters one at a time), indexer-a sentio-node $SN_COMMIT, then the activation update | Before activation: revert images in reverse order. After activation: none (no voter, verifier or SNode downgrade; agents fall back with -si-lanes off) |`.

Insert section "## 7. Stage 5: client_seq lanes" before A2's "## 7. Operating rules" and renumber that heading to "## 8. Operating rules" (first run `grep -n '§7\b\|§7\.' docs/storage-integrity-devnet2-dynamic-table-set.md` and update every existing reference to the old §7 to §8). One paragraph per line; command blocks in bash like the rest of the file, using its §0 helpers; these subsections, each carrying the commands of the matching Task 20 step translated to bash:
- 7.1 Preconditions: Phase A done (§6 stage 4b: indexer-a on the A2 image with `writerAuthorization: true`); table registry enabled (`si_show | jq .current.table_registry` non-null); pins table (`ARBITER_DIGEST`, `SN_DIGEST`, each with OCI revision evidence).
- 7.2 Offline replay check: `statedigest` old (`v0.9.0`) vs new (`ARBITER_TAG`) on a tar copy of every voter's `/data/raft` and `/config/arbiter.yaml`; identical `key`, `spent_ids`, `snapshot_sha256`, `written_version`, `live_check`, `apply_*` lines; a raft.db that fails to open is a torn copy and is re-copied.
- 7.3 Verifiers: values commit (`arbiter.image.digest` + `DEVNET2_ARBITER_DIGEST`), sync, rollout status, safe watermark keeps advancing.
- 7.4 indexer-a: values commit (sentio-node pin in `sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, its mirrors `docs/examples/storage-integrity-fresh-source-image.yaml` and `FRESH_SOURCE_IMAGE`, and the new image appended to `WRITER_AUTHORIZATION_IMAGES`, because indexer-a keeps `writerAuthorization: true`), sync, `sentio_getStorageIntegrityInfo` answers `client_lanes_enabled: false`.
- 7.5 Voters: §4's one-at-a-time replacement (followers first, leader last, sealed tip equal on all three before the next deletion), then every voter's capability lists `client_lanes_v1`.
- 7.6 Gate and activation: `consensus features`, restart any listed data-plane node, drain check, `consensus update … --client-lanes-max-per-account 256 --voter <arb-0> --voter <arb-1> --voter <arb-2>`; **irreversible**.
- 7.7 Verify: epoch +1 with `client_lanes.max_lanes_per_account` 256, registry version +1, `client_lanes_enabled: true`, snapshot v18 after the next snapshot, `arbiter_admission_rejects_total` baseline.
- 7.8 Rollback: before 7.6 only — revert the 7.4 commit and sync, revert the 7.3 commit and sync, replace voters back one at a time (they still write v17 and no command carries a new field); after 7.6 none.
- 7.9 Operating rules (appended to §8 Operating rules): never downgrade a voter, verifier or SNode below `ARBITER_TAG` / `SN_COMMIT` after activation; a new data-plane node must run a lane-aware image (the leader refuses its registration otherwise); the driver sidecar keeps `HOUSEGATE_SI_LANES=off`.

- [ ] **Step 3: Render tests and commit**

Run: `cd "$W" && python3 -B charts/storage-integrity/tests/test_render.py`
Expected: PASS.

```bash
cd "$W" && git add docs/storage-integrity-devnet2-dynamic-table-set.md && git commit -m "docs(storage-integrity): devnet2 client_seq lane activation stage

Offline replay check, verifier and indexer-a rollout, one-at-a-time voter
replacement, the arbiter-admin activation gate and the no-downgrade rule
(housegate spec 2026-10-09 §8.2-§8.3).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

- [ ] **Step 4: PR (requires explicit user confirmation)** — `charts/` and `docs/` changes go through a PR on `sentioxyz/production`; merge after review.

---

## Task 20: devnet2 — image rollout, offline replay check, voter replacement, activation, E2E (B6, live)

Every step is run by the user in their own fish shell. Steps 3–10 touch devnet2 (Step 3 copies voter data; Steps 4–9 change it; Step 10 writes user data through the signed lane) and each **requires explicit user confirmation**; Step 8 is **irreversible after activation**. Stop on any unexpected output; before Step 8 the way back is §7.8 of the runbook.

**Interfaces:**
- Consumes: Task 12 `$R/bin/{old,new}/statedigest`, `ARBITER_DIGEST`; Task 17 `SN_COMMIT`, `SN_DIGEST`; Task 19 runbook stage.
- Produces: client lanes active on devnet2 with `max_lanes_per_account` 256.

- [ ] **Step 1: Session helpers (fish)**

```fish
function si_kubectl; kubectl --context sentio-sea -n sentio-network-devnet2 $argv; end
function si_arbiter; si_kubectl exec storage-integrity-si-v2-arbiter-$argv[1] -c arbiter -- $argv[2..-1]; end
function si_voter; printf 'storage-integrity-si-v2-arbiter-%s.storage-integrity-si-v2-arbiter-raft.sentio-network-devnet2.svc.cluster.local:7080' $argv[1]; end
function si_leader
    for i in 0 1 2
        if si_arbiter $i /usr/local/bin/arbiter-admin consensus show --address (si_voter $i) >/dev/null 2>&1
            echo $i
            return 0
        end
    end
    return 1
end
function si_show; set -l l (si_leader); si_arbiter $l /usr/local/bin/arbiter-admin consensus show --address (si_voter $l); end
function si_capability; si_arbiter $argv[1] /usr/local/bin/arbiter-admin consensus capability --address (si_voter $argv[1]) | jq -c .; end
function si_sealed; si_arbiter $argv[1] wget -qO- http://127.0.0.1:9090/metrics | string match -r '^arbiter_sealed_block_seq .*'; end
function si_rpc; si_kubectl exec sentio-node-devnet2-indexer-a-0 -c sentio-node -- wget -qO- --header 'Content-Type: application/json' --post-data $argv[1] http://localhost:32003; end
set -g si_backup $HOME/si-devnet2-lanes; mkdir -p $si_backup
set -g R $HOME/si-devnet2-lanes-rehearsal
```

- [ ] **Step 2: Baseline (read-only)**

```fish
si_show | tee $si_backup/show-before.json | jq -c '{epoch, params_digest, table_registry: .current.table_registry, client_lanes: .current.client_lanes}'
si_rpc '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityInfo","params":[]}' | tee $si_backup/info-before.json | jq -c '.result | {client_lanes_enabled, registry_version}'
for i in 0 1 2; si_sealed $i; si_capability $i; end
```

Expected: `table_registry` non-null, `client_lanes` null; `client_lanes_enabled: false`; record `registry_version`.

- [ ] **Step 3: Offline replay check (read-only on the cluster; local CPU only)**

```fish
for i in 0 1 2
    si_kubectl exec storage-integrity-si-v2-arbiter-$i -c arbiter -- tar -C /data -czf - raft > $si_backup/arbiter-$i-raft.tgz
    si_kubectl exec storage-integrity-si-v2-arbiter-$i -c arbiter -- cat /config/arbiter.yaml > $si_backup/arbiter-$i.yaml
    set work (mktemp -d)
    tar -C $work -xzf $si_backup/arbiter-$i-raft.tgz
    for v in old new
        $R/bin/$v/statedigest -data-dir $work/raft -config $si_backup/arbiter-$i.yaml > $si_backup/digest-$i-$v.txt 2> $si_backup/digest-$i-$v.err
        or begin; echo "statedigest $v failed for arbiter-$i"; cat $si_backup/digest-$i-$v.err; end
    end
    for v in old new
        grep -E '^(snapshot_index|last_index|applied_commands|rejected_commands|written_version|genesis_configured|live_check|apply_error|apply_rejected|key|spent_ids|snapshot_sha256)' $si_backup/digest-$i-$v.txt > $si_backup/digest-$i-$v.cmp
    end
    if cmp -s $si_backup/digest-$i-old.cmp $si_backup/digest-$i-new.cmp
        echo "arbiter-$i: old and new replay identically"
    else
        echo "arbiter-$i: DIFFERENT — stop"; diff $si_backup/digest-$i-old.cmp $si_backup/digest-$i-new.cmp
    end
    rm -rf $work
end
```

Expected: three "replay identically" lines; `applied_commands` above zero; `live_check` lines equal to Step 2's `show-before.json` epoch/params_digest/promotion_seq and the leader's sealed/safe metrics (taken a moment earlier, so the sealed tip may be equal or one below); no `apply_error`. A "raft.db … re-copy" error means the copy was taken mid-write: rerun that voter's iteration. Any difference: stop, do not continue to Step 4.

- [ ] **Step 4: Verifiers to `ARBITER_TAG` (requires explicit user confirmation)**

In the production checkout, one commit setting `arbiter.image.digest` in `k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.yaml` to `$ARBITER_DIGEST` (with the version comment naming `$ARBITER_TAG` and the previous pin) and `DEVNET2_ARBITER_DIGEST` in `charts/storage-integrity/tests/test_render.py`; `python3 -B charts/storage-integrity/tests/test_render.py` passes; then (direct to `main`, single-service values):

```fish
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=storage-integrity-si-v2
si_kubectl rollout status statefulset/storage-integrity-si-v2-verifier --timeout=15m
for i in 0 1 2; si_sealed $i; end
si_kubectl logs storage-integrity-si-v2-verifier-0 --since=10m | string match -r 'register|registered' | head
```

Expected: verifiers roll (RollingUpdate), each re-registers (its log shows registration), the safe watermark keeps advancing over the next blocks. Voters are `OnDelete` and keep `v0.9.0` — do not delete them in this step.

- [ ] **Step 5: indexer-a to `SN_COMMIT` (requires explicit user confirmation)**

One commit: sentio-node `sha-$SN_COMMIT@$SN_DIGEST` in `sentio-node-indexer-a-storage-integrity-si-v2-source-image.yaml`, its mirrors `docs/examples/storage-integrity-fresh-source-image.yaml` and `FRESH_SOURCE_IMAGE` in `charts/storage-integrity/tests/test_render.py`, and the same image string appended to `WRITER_AUTHORIZATION_IMAGES` in that file (PA6: indexer-a keeps `writerAuthorization: true`, so `test_writer_authorization_needs_a_reviewed_image` fails until the B5 image is listed; it carries D17 because it is built on housegate `$HG_TAG`, which contains A1); `python3 -B charts/storage-integrity/tests/test_render.py` passing; then:

```fish
git push origin HEAD:main
helmfile -f k8s-sea/sentio-network-devnet2/storage-integrity-si-v2.helmfile.yaml sync --selector name=sentio-node-devnet2-indexer-a
si_kubectl rollout status statefulset/sentio-node-devnet2-indexer-a --timeout=15m
si_rpc '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityInfo","params":[]}' | jq -c '.result | {client_lanes_enabled, registry_version}'
si_kubectl get pod sentio-node-devnet2-indexer-a-0 -o json | jq -c '.spec.containers[] | select(.name=="housegate-sidecar") | .env[] | select(.name=="HOUSEGATE_SI_LANES")'
```

Expected: `client_lanes_enabled: false`, `registry_version` unchanged; sidecar env `HOUSEGATE_SI_LANES=off`; an ordinary signed INSERT through the sidecar still lands (runbook §3.4 checks).

- [ ] **Step 6: Replace voters one at a time (requires explicit user confirmation for each deletion)**

```fish
set leader (si_leader)
for i in (printf '0\n1\n2\n' | string match -v $leader) $leader
    si_kubectl delete pod storage-integrity-si-v2-arbiter-$i --wait
    while not si_kubectl get pod storage-integrity-si-v2-arbiter-$i >/dev/null 2>&1; sleep 2; end
    si_kubectl wait --for=condition=Ready pod/storage-integrity-si-v2-arbiter-$i --timeout=15m
    si_capability $i
    for t in (seq 60)
        set tips (for j in 0 1 2; si_sealed $j; end | sort -u)
        test (count $tips) -eq 1; and break
        sleep 5
    end
    for j in 0 1 2; si_sealed $j; end
    read -P "arbiter-$i replaced; sealed tips equal on all three? continue with the next voter? [y/N] " answer
    test "$answer" = y; or break
end
```

Expected: each capability line names its own `node_id`, `protocol_version` 1 and `features` `["client_lanes_v1"]`; the sealed tip matches on all three before the next deletion. Until Step 8 the way back is replacing voters with the previous image the same way (they still write v17 snapshots).

- [ ] **Step 7: Gate dry run (read-only)**

```fish
set leader (si_leader)
si_arbiter $leader /usr/local/bin/arbiter-admin consensus features --address (si_voter $leader) | jq -c '{voters: .raft_voter_ids, nodes: [.entries[] | {id: .node_id, lanes: ((.features // []) | index("client_lanes_v1") != null)}]}'
si_show | jq -c '{epoch, promotion_seq}'
```

Expected: three voter ids; every entry `lanes: true`. An entry with `lanes: false` registered with a previous leader or runs an old image: restart that node (`si_kubectl delete pod <its pod>` — requires confirmation) and re-run this step. Check drain: no pending promotions or cleanups (the safe watermark equals the sealed tip on the leader's metrics, and the last promotion's cleanup is acknowledged in the leader log); if unsure, proceed — the FSM refuses an undrained update atomically with `InvalidArgument` and nothing changes.

- [ ] **Step 8: Activate client lanes (requires explicit user confirmation; IRREVERSIBLE)**

```fish
set leader (si_leader)
set epoch (si_show | jq -r .epoch)
si_arbiter $leader /usr/local/bin/arbiter-admin consensus update --address (si_voter $leader) \
    --expected-epoch $epoch --authority 0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705 --max-writers 1 \
    --client-lanes-max-per-account 256 \
    --voter (si_voter 0) --voter (si_voter 1) --voter (si_voter 2) --timeout 20s
```

The table registry parameters are carried from the leader automatically. A gate refusal lists the nodes to fix and submits nothing; an `InvalidArgument` means the FSM refused (undrained work or a raced epoch): run `si_show` and retry. On "commit outcome may be unknown", run `si_show` before anything else.

- [ ] **Step 9: Verify activation**

```fish
si_show | jq -c '{epoch, client_lanes: .current.client_lanes}'
si_rpc '{"jsonrpc":"2.0","id":1,"method":"sentio_getStorageIntegrityInfo","params":[]}' | jq -c '.result | {client_lanes_enabled, registry_version}'
for i in 0 1 2; si_arbiter $i wget -qO- http://127.0.0.1:9090/metrics | string match -r '^arbiter_(client_lanes_total|accounts_at_lane_cap|admission_rejects_total).*'; end
```

Expected: epoch = Step 2's epoch + 1 and `client_lanes.max_lanes_per_account` 256; `client_lanes_enabled: true` and `registry_version` = Step 2's + 1; `arbiter_client_lanes_total 0`. After the next Raft snapshot, a voter's `/data/raft/snapshots/*/state.bin` version byte is 18 (`si_arbiter 0 sh -c 'for d in /data/raft/snapshots/*/; do head -c 5 "$d/state.bin" | od -An -tu1; done'`).

- [ ] **Step 10: E2E after activation (spec §9.4 steps 2, 5, 6, 10; requires explicit user confirmation: signed INSERTs on devnet2)**

From a user machine with the lane-aware agent (`housegate` at `HG_TAG`), using only `housegate -agent-key $KEY` and `clickhouse-client --host 127.0.0.1 --port 9000`:
1. Insert into the user's Active SI table (`INSERT INTO devuser1.t FORMAT CSV` with stdin) → accepted; the agent log names its lane at info; on every voter `arbiter_client_lanes_total` becomes 1. (`SafeState.GetClientSeqState` is a gRPC read; use it only if `grpcurl` and the arbiter proto descriptors are available on the operator machine, through `si_kubectl port-forward pod/storage-integrity-si-v2-arbiter-0 17080:7080`.)
2. Start a second agent with the same key on another machine (or another state dir) and insert → a distinct lane; `arbiter_client_lanes_total` 2; no `ADMISSION_CODE_DUPLICATE_CLIENT_SEQ` in `arbiter_admission_rejects_total`.
3. Delete the first agent's state dir, restart, insert → a new lane, accepted (`arbiter_client_lanes_total` 3).
4. Record `arbiter_admission_rejects_total` by code and the driver sidecar's ids still legacy (`si_kubectl logs sentio-node-devnet2-indexer-a-0 -c housegate-sidecar --since=10m | string match -r 'statement_id=[^ ]+' | head`: three segments).

---

## Spec coverage map

| Spec item | Task(s) |
|---|---|
| §5.1 statement id forms, canonical rules | 2, 13, 14, 17 |
| §5.2 proto fields, `GetNodeFeatures`, `GetClientSeqState` | 1 |
| §5.3 arbiter-core canonical types, mirrors, wire, registry snapshot | 2, 3, 4 |
| §5.4 `Params.ClientLanes`, transition rule, registry bump, view, `LaneCounts`, v18 | 6, 7, 8 |
| §5.5 admission order and `LANE_BUDGET_EXCEEDED` | 7 |
| §5.6 features request-only, feature book, voter probe, gate, post-activation refusal | 4, 9, 10 |
| §6.1 shared parser, proto conversion, `LANE_BUDGET_EXCEEDED` outcome, `lanesEnabled` | 13 |
| §6.5 lane pool, flock, reserve/release/abandon, rotation, in-flight cap, R9, sidecar off | 15, 16, 19 |
| §6.6 marker for `LANE_BUDGET_EXCEEDED` | 13 (via PA2) |
| §6.7 adapter, `client_lanes_enabled`, `lanesEnabled`, pins | 17 |
| §6.8 arbiter metrics, `GetProtocolInfo.features`, CLI | 9, 10, 11 |
| §6.10 image bumps, runbook stage | 19, 20 |
| §7 compatibility, no rollback past activation, vectors file | 5, 8, 14, 18, 19, 20 |
| §8.2 step 2–4, §8.3 ordering | 20 |
| §9.3 determinism, strict decoder, pre-activation, activation transition, gate, accumulator, lane cap, replay, metrics, fuzz | 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 20 |
| §9.4 steps 5, 6, 10 | 20 |
