# Signed INSERT ... SELECT Native Integration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Connect native clients to the verified snapshot-query lifecycle and produce complete disposable-network activation evidence.

**Architecture:** A new agent path obtains/finalizes/signs a reserved input; an independent executing-host plugin validates it before ordinary rewrite/forwarding to ClickHouse. Relay drives an asynchronous local query completion with one framed client reader. Capability-gated adapters connect the shared runtime to sentio-node and the Arbiter, and a multi-process test harness verifies the full protocol.

**Tech Stack:** Go native TCP codecs, ClickHouse CLI/Go drivers, protobuf/gRPC, Bazel, Docker, immutable release manifests.

**Spec:** [Signed INSERT ... SELECT design](../specs/2026-09-16-signed-insert-select-design.md), D3, D8–D10 and A1–A14; [index](2026-09-16-signed-insert-select.md), [contracts](2026-09-16-signed-insert-select-contracts.md), [replay](2026-09-16-signed-insert-select-replay.md), [coordination](2026-09-16-signed-insert-select-coordination.md).

## Global Constraints

- All [index constraints](2026-09-16-signed-insert-select.md#global-constraints) apply. This plan produces code and acceptance evidence; production activation is outside its authorization boundary.
- Query-only input creates no `DeferredInsertPlan`, sends no sample block, waits for no row terminator and forwards no SQL/Data to ordinary ClickHouse execution.
- Exactly one query is in flight and one codec reads each connection. Keep framed-upstream `OnQuerySuccess` semantics; a local ACK2 is not an upstream success or safe publication.
- Run final analysis/materialization under the reserved profile, sign the final logical SQL, and independently validate it at the executing host even on trusted peer sessions.
- Admit `SQL_x_read_mode` only when absent or `safe`; refuse user settings/session dependencies/parameters and all client row payloads.
- Keep all capabilities disabled by default. Mixed versions refuse; rollback retains accepted v3 history, executable profiles and snapshot artifacts.

---

## File map

| Owner | Create | Modify |
|---|---|---|
| HG agent | `pkg/plugins/sisnapshotquery/plugin.go`, `plugin_test.go`, `journal.go`, `journal_test.go`, `BUILD.bazel` | `pkg/plugin/context.go`, `pkg/plugins/materialize/materialize.go`, `pkg/plugins/sistatement/plugin.go`, their tests/BUILD files, `pkg/storageintegrity/settings.go`, `settings_test.go` |
| HG ingress | `pkg/plugins/snapshotquery/plugin.go`, `plugin_test.go`, `BUILD.bazel` | `pkg/plugins/rewrite/rewriter.go`, `pkg/plugins/storageintegrity/plugin.go`, their tests/BUILD files; skip only after validated query ownership |
| HG relay | `pkg/proxy/relay_query_only.go`, `relay_query_only_test.go` | `pkg/plugin/context.go`, `hooks.go`, `pkg/proxy/relay.go`, `pkg/proxy/BUILD.bazel`, plugin-chain tests |
| HG runtime | `storage_integrity_query_runtime.go`, `storage_integrity_query_runtime_test.go`, `pkg/config/storage_integrity_query_config_test.go` | `proxy.go`, `build.go`, `storage_integrity_runtime.go`, `pkg/config/storage_integrity_config.go`, `pkg/config/config.go`, affected BUILD files |
| SN adapter | `storageintegrityadapter/snapshot_query.go`, `snapshot_query_test.go` | `storageintegrityadapter/adapter.go`, `ingress_client.go`, `BUILD.bazel`, `standalone/standalone.go`, `standalone/standalone_test.go`, dependency pins |
| HG acceptance | `pkg/integration/snapshotquery/cluster_test.go`, `protocol_test.go`, `recovery_test.go`, `limits_test.go`, `BUILD.bazel`, `testdata/cases.json`; `docs/superpowers/validation/snapshot-query-release.md` | `.github/workflows/ci.yml`, `README.md`, `CLAUDE.md`, relevant SI configuration documentation |
| AC verifier fixture | `integration/snapshotquery/verifier/main.go`, `BUILD.bazel` | AC CI for building the test fixture image, replay/source integration tests and dependency pins |
| AR acceptance | `fsm/snapshot_query_acceptance_test.go`, `server/snapshot_query_acceptance_test.go` | AR CI and BUILD files |

### Task D1: Obtain, finalize and sign the agent's query-only input

**Files:** HG agent row. Use A5's analyzer, A2's signer, C2's reservation API and the existing durable statement sequence counter.

**Interfaces:** `sisnapshotquery.Options` contains `Signer auth.StatementSignerV3`, `ControlSigner auth.SnapshotQueryControlSigner`, `Analyzer rewriter.SnapshotQueryAnalyzer`, `Reservations SnapshotReservationClient`, `Catalog SnapshotCatalogSource`, `Seq *sistatement.SeqCounter`, `Journal AgentQueryJournal`, `NetworkID string`, `KeeperShardID uint32`. `auth.StatementSignerV3` exposes `Address() string` and `SignStatementV3(auth.JWSStatementPayloadV3) (string, error)`; the control signer exposes `SignSnapshotQueryControl(binding auth.SnapshotQueryControlBinding) (string, error)` and is implemented by the existing relay signer using C2's purpose. Define these small interfaces in `pkg/auth/snapshot_query_control.go`/`statement_v3.go` when their producer tasks land.

`SnapshotReservationClient` mirrors C2 acquire/lookup/release; `SnapshotCatalogSource.Load(ctx context.Context, pin replay.SnapshotPin) (replay.SafeSnapshotManifest, []payloadexec.TableSchema, error)` verifies the published pin and complete schema map. `AgentQueryJournal` persists statement ID, original/final SQL, request ID, grant, full signed envelope, send/status ambiguity and terminal outcome with `Load`, `List`, `Save` methods over a `Record` with those fields. No journal entry may silently replace a signed operation with a fresh pin.

Transport the canonical input as raw-base64url JSON in one new explicitly owned custom setting `SQL_x_snapshot_query_input`; use the existing `SQL_x_statement_token` for the v3 JWS and `SQL_x_auth_token` for final SQL query authentication. Add the new exact setting key to the enumerable owned-setting list; no prefix-wide exemption. Enforce the reserved profile's SQL/descriptor limits before encoding and bound encoded bytes before decoding. Duplicate settings or a v3 token/input mismatch reject. The input root is recomputed from the decoded input; the descriptor is actually transported, not replaced by an inaccessible root.

- [ ] **Step 1: Add agent tests for both WITH placements, durable identity and profile timing.** Fake the reservation server to return Q2 after preliminary analysis under Q1; require final materialization/analysis under Q2 and require the signed fields to contain Q2. The agent must not hand these queries to the payload signer.

```text
receive original SQL -> preliminary AST classification and authorization context
persist statement/request ID -> acquire -> load authenticated S/catalog
final materialize+analyze using reserved query profile -> complete read descriptor
canonical input/root -> sign v3 -> persist original signed envelope -> set final Query body/settings
query-auth signer signs final Query body -> ordinary agent forwarding
assert DeferredInsert=nil, no sample, no fabricated Data, one statement ID
```

Test pending/failed USE, logical database changes, constant SELECT, duplicate/full target lists, read-mode override, session/query settings and parameters. On materialization/pool/profile failure, no signed operation is sent and the reservation is released only after reconciliation proves it unconsumed.

- [ ] **Step 2: Run `bazel test //pkg/plugins/sisnapshotquery:sisnapshotquery_test`.** Add the new target in this task. The initial tests fail because the query signer/reservation flow is absent.

- [ ] **Step 3: Implement a separate agent plugin before ordinary materialization.** Preserve original client SQL before the ordinary fail-open materializer can alter it. The new plugin uses the rewriter's AST classification to claim the snapshot-query lane; ordinary materialize/sistatement plugins skip only that explicit claimed context. Define `plugin.SnapshotQueryAgentKey = "snapshot_query_agent_owned"` and set `qctx.Values[plugin.SnapshotQueryAgentKey] = true` only in this agent plugin after successful classification; the key is process-local and never populated from client settings. Final analysis/materialization runs from the preserved logical program under the granted profile, builds the exact read descriptor from S and signs once. General query auth still signs the resulting final SQL. Existing v2 payload input ordering is unchanged.

Use existing successful-USE tracking for logical database resolution, committing a candidate only after the existing framed upstream success; never infer current DB from an unacknowledged USE. Respect on-behalf-of authorization at ingress: statement identity/signature belongs to the signer, while effective payer/owner permissions use the existing validated delegation relationship. Do not turn an unvalidated `SQL_x_payer` into an alternate signed account.

```go
input, err := replay.CanonicalSnapshotQueryInput(input)
if err != nil { return err }
root, err := replay.SnapshotQueryInputRoot(input)
if err != nil { return err }
token, err := signer.SignStatementV3(auth.JWSStatementPayloadV3{
    Purpose: "housegate-statement-v3", Binding: input.Binding, InputRoot: root,
})
if err != nil { return err }
envelope := replay.SnapshotQueryEnvelope{Input: input, InputRoot: root, UserJWS: token}
// Save envelope durably before setting query fields or forwarding it.
```

Persist a retry correlation from `(signer, network, nonempty native Query.ID)` to the journaled statement ID before acquisition; bind it to the original SQL and logical database too. The same query ID with different input refuses. A reconnect/retry with that same ID looks up the saved envelope/status and never signs again; add CLI `--query_id` and Go driver query-ID tests. An empty Query.ID may start a new operation, but cannot request an inferred retry based only on identical SQL. Recovery of the already persisted agent record still uses its durable statement ID. A caller that needs retry correlation must keep its nonempty query ID; a deliberate new operation, including after abort, uses a new one. This distinction prevents both double writes and accidental deduplication of two intentional identical INSERTs.

- [ ] **Step 4: Run agent, materialization, signing and settings regressions.** `bazel test //pkg/plugins/sisnapshotquery:sisnapshotquery_test //pkg/plugins/sistatement:sistatement_test //pkg/plugins/materialize:materialize_test //pkg/auth:auth_test //pkg/storageintegrity:storageintegrity_test`. Test restart after grant, signing and send with no duplicate reservation or write.

- [ ] **Step 5: Commit `feat(agent): sign reserved snapshot query inputs`.** Keep the feature off until D3 wiring and D4 acceptance pass.

### Task D2: Independently admit at the executing host and drive local native completion

**Files:** HG ingress and relay rows. Depend on C4 intake, A5 analyzer and A2 verification. Current `SuppressUpstreamExecution` still forwards Query to obtain a payload sample; current `AbortWithSuccess` invokes abort/complete hooks. Neither is the new query-only execution primitive.

**Interfaces:** Add `plugin.QueryContext.QueryOnly *QueryOnlyPlan` with `QueryOnlyPlan{Run func(context.Context) error, CancelClient func(), MaxControlBytes uint64}`. `Run` returns after the configured successful ACK boundary or an error; accepted operation ownership remains in the durable intake. `CancelClient` stops delivery and invokes pre-/post-submit cancellation rules, never unconditionally rolls back source work. `QueryOnlyPlan` is mutually exclusive with `DeferredInsert`, `SuppressUpstreamExecution` and `AbortWithSuccess`. Add `Relay.runQueryOnly` in the new file, keeping packet reading in the existing client loop.

The new ingress plugin consumes the decoded signed input, performs normal query authentication/authorization plus independent reserved-profile AST/descriptor verification, and installs `QueryOnlyPlan.Run` over `SnapshotQueryIntake.Submit`. Define a distinct process-local `plugin.SnapshotQueryHostKey = "snapshot_query_host_validated"`; only this successful host validator may set it, and ordinary rewrite/payload plugins check this key before skipping. An agent-owned marker or a client setting cannot set host ownership. The plugin must execute at the owning host on ordinary, trusted-peer and privileged sessions. A routing-only hop may transport opaque input but cannot claim execution; the receiving host validates regardless of trust flags. Do not widen route/peer bypasses.

- [ ] **Step 1: Measure real client packet traces and encode regression cases.** Using the pinned official ClickHouse CLI and Go driver, record Query, optional empty external-table marker and terminal packets for INSERT SELECT with both WITH positions. Turn observed traces into fixture bytes with fragmented/coalesced delivery, plus adversarial missing marker, named/nonempty Data, duplicate marker, Cancel/EOF and next Query while active. A fixture-only synthetic codec test is necessary but does not replace the measured clients.

```text
Query only -> begin work immediately -> success EOS; no sample and no Data wait
Query + permitted empty marker in same write -> drain locally; never forward marker
fragmented Query/marker -> same lifecycle and one terminal response
named empty block or nonempty Data -> Exception; no row payload enters the query input
next Query before terminal exposure -> reject; no concurrent query ownership
EOF after Submit -> no second execution; durable reconciliation continues
ACK2 completion -> client EOS but no assertion of safe visibility
safe ACK completion -> EOS only after applied safe publication
```

- [ ] **Step 2: Run `bazel test //pkg/proxy:proxy_test --test_filter=TestRelayQueryOnly` and `bazel test //pkg/plugins/snapshotquery:snapshotquery_test`.** Initially expect Query forwarded upstream or a payload/sample wait; the new tests must detect both.

- [ ] **Step 3: Implement ingress ownership and the relay state machine.** Decode exactly one bounded input setting and statement token; reject unknown/present-empty payload fields, query body/hash mismatch, unauthorized read/write databases, stale reservation, non-safe read mode, settings and parameters. Reanalyze signed logical SQL against the authenticated pinned schema/profile and compare the exact full descriptor. An AST-classified snapshot query without a valid v3 input is refused, including leading WITH; it cannot slip into ordinary rewrite. General SI rewrite/payload intake skip only after the new plugin has validated and claimed execution, avoiding their current INSERT SELECT rejection/physical rewrite.

```text
on QueryOnlyPlan: atomically mark active local-query ownership before starting Run worker
client loop remains the sole client Codec reader; worker never reads/peeks the socket
drain at most the measured empty external-table marker under the active query generation
reject named/nonempty or extra Data; never forward client Data to ordinary upstream
serialize client writes; terminal worker claims the same active generation exactly once
run local completion callback and OnQueryComplete before EOS/Exception exposure
clear active ownership only at the supported terminal boundary; preserve one query in flight
```

Keep `OnQuerySuccess` restricted to genuine framed upstream success. A locally generated query-only EOS uses the explicit local completion path; it cannot commit a pending USE or simulate framed upstream success. Do not reuse raw unsupported-result fallback. A permitted late empty marker may be drained for the just-completed query only until the next Query packet; clear that allowance at the next query and reject ambiguous or excess data. Never block completion waiting for an optional marker.

Malformed Data observed before Submit cancels/rejects before sequencing. If it arrives after accepted Submit, fail the client protocol and reconcile the accepted operation under C4/C3; do not claim that late transport input can undo a sequenced write. The payload is never executed. Cancellation and terminal races must select one client outcome while retaining correct durable ownership.

- [ ] **Step 4: Run all relay/plugin lifecycle regressions and real-client cases.** `bazel test //pkg/proxy:proxy_test //pkg/plugin:plugin_test //pkg/plugins/snapshotquery:snapshotquery_test`. Include routed/trusted/forward-pivot sessions, compression/chunk negotiation and revision `54470`; assert one reader, no ordinary upstream Query/Data, no double completion and correct next-query behavior after supported terminal boundaries. D4 repeats this through real source/verifier/Arbiter processes.

- [ ] **Step 5: Commit `feat(proxy): serve query-only snapshot insert lifecycles`.** Update hook/context documentation in the same commit so local completion and framed upstream success remain distinguishable.

### Task D3: Wire capabilities, durable ports and embedded adapters with defaults off

**Files:** HG runtime and SN adapter rows; dependency pins in HG, AC, AR and SN using each repository's actual upgrade workflow.

**Interfaces:** Add proposed `storage_integrity.snapshot_query.enabled` defaulting to `false`, `profile_manifest_path`, durable `state_dir`, and host-side `artifact_dir` configuration. The first artifact profile requires the source/verifier's shared durable mounted namespace from B1; validate it and keep disposable caches separate. The profile manifest enumerates exact installed executor/query pairs and build/artifact digests; active admission policy still comes from the Arbiter. Add injected HG runtime options for A5 analyzer, C2 reservation client, D1 published catalog, C4 intake, B1 snapshot/retention and C5 source ports. Constructors fail when enabled and any required capability/durable port is absent. No implicit fallback to v2 or ordinary SQL is permitted.

SN `storageintegrityadapter/snapshot_query.go` implements the HG query source/sequencer/status interfaces using AC snode methods and `WithLeaderRetry`. `standalone/standalone.go` wires them beside existing `NewSourcePreparer`, `NewLeaderIngressClient`, `NewStatusQuerier` and `NewMergeConn`. SN does not currently construct AC's verifier; keep verifier construction in AC's role/runtime and test it separately.

- [ ] **Step 1: Add configuration and field-carry-through tests.** Test defaults off, enabled with one missing port at a time, wrong profile digest, wrong native/gRPC acknowledgement, unsupported Arbiter, unavailable artifact retention, missing state directory and old embedded Housegate. For adapter tests, fill every A1/C3/C5 field with a distinct nonzero fixture value and round-trip it; verify arrays, original JWS, profile pair, generation, output and exact candidate commitments, not just statement ID.

```text
enabled=false + old dependencies -> existing v2 behavior unchanged
enabled=true + any missing required role capability -> startup/admission refusal naming it
installed Q1 and Q2 + active Q2 -> probe Q2 and grant only Q2
old agent on upgraded network -> v2 works; no implicit v3 capability
new agent reaching old host/Arbiter -> explicit refusal; no ordinary SQL fallback
new source/verifier restarting with Q1 historical history -> retain and replay Q1
```

- [ ] **Step 2: Run HG config/runtime tests and SN `bazel test //storageintegrityadapter:storageintegrityadapter_test //standalone:standalone_test`.** Expect missing configuration/adapter methods or a dropped binding before implementation.

- [ ] **Step 3: Implement wiring and capability verification.** At enablement, probe exact positive/negative analysis behavior, committed active-pair policy, source query preparation/status, verifier dispatch, authenticated snapshot restore and durable retention support. Validate the profile's binary/engine/tzdata/settings/limits IDs against actual deployed artifacts, not a manually typed label. Bootstrap the recovery journals before listening for new work. Active-policy changes are C1 control transitions, not live config reloads that reinterpret active reservations.

Use separate query adapter methods; do not loosen `toArbiterEnvelope`'s INSERT-only v2 behavior or introduce an HG→AC dependency. Preserve physical candidate hashes on the new path where the byte-side claim requires them; the old adapter's intentional projection does not justify dropping new commitment fields.

```text
HG ports/contracts release -> AC implementation and verifier release -> AR control-plane release
RP contract -> RG and RC matched engine releases -> HG engine dependency/FFI pins
SN pins compatible HG + AC + AP -> standalone constructs all query ports disabled by default
release manifest records every actual commit, module pin, image digest and profile digest
```

Regenerate `go.sum`/Bazel dependencies and any `MODULE.bazel` git overrides together with module pins. Account explicitly for the out-of-band polyglot FFI library and RC's RP submodule; a `go.mod` bump alone is incomplete. Never edit an installed historical profile artifact in place.

- [ ] **Step 4: Run owning-repository required checks and version matrix.** HG `bazel test //...` and build; AC/AR Bazel suites plus wire conformance; SN adapter/standalone tests plus repository-required checks. In test startup, restore a nonterminal query journal before accepting a new write and verify wrong-profile startup/admission failures are visible. A source/verifier process merely starting is not end-to-end acceptance.

- [ ] **Step 5: Commit `feat(runtime): wire gated snapshot query capabilities` and `feat(node): adapt signed snapshot query intake`.** Create normal ready PRs in their owners after checks; keep all deployment configuration disabled.

### Task D4: Prove the full acceptance matrix and write the activation runbook

**Files:** HG acceptance, AC verifier fixture, AR acceptance rows; source/integration test extensions from B/C. No production manifest is changed in this task.

**Interfaces:** The new manual Bazel target is `//pkg/integration/snapshotquery:snapshotquery_test`, backed by a disposable process/container cluster. Its release input is `SI_QUERY_RELEASE_MANIFEST_JSON`, validated as JSON with exact repository commits, module pins, source/node/agent/Arbiter/verifier/rewriter/ClickHouse image digests, architecture, tzdata digest, executor/query profile IDs, schema digest and limits. Refuse mutable image tags or absent required digests. Store the validated manifest and all observed test evidence in `TEST_UNDECLARED_OUTPUTS_DIR` for CI upload.

AC `integration/snapshotquery/verifier/main.go` is a test-only role host that constructs the real B5 verifier backend with real snapshot/artifact ports and runs the existing verifier subscription/byte scanner against the configured Arbiter. Register its explicit Bazel binary and build a digest-pinned fixture image in AC CI. Use the real SN standalone source/embedded Housegate and a separate HG agent process; do not replace the source/verifier/Arbiter with in-memory fakes in the end-to-end gate. HG imports no AC/AR code: it controls these external test processes through native/gRPC interfaces and declared image inputs.

- [ ] **Step 1: Implement the disposable cluster and literal case manifest.** `testdata/cases.json` enumerates every spec A1–A14 ID, SQL/setup, fault point, expected native outcome and expected state/receipt/candidate invariants. `cluster_test.go` creates unique network/schema/volumes, two independent ClickHouse instances for source/verifier, artifact storage, Arbiter, both rewriter variants, agent and SN source. It validates capabilities, derives genesis and performs the profile transition before requesting a reservation.

```json
{
  "id": "A4-self-insert",
  "setup_rows": [1, 2],
  "sql": "INSERT INTO tenant.events SELECT * FROM tenant.events",
  "local_unsafe_extra_rows": [99],
  "first_safe_row_count": 4,
  "second_safe_row_count": 8,
  "assertions": ["same_signed_pin", "distinct_new_row_ids", "no_unsafe_reads", "whole_ledger_preserved"]
}
```

Add zero-result, full R/W/U ledger, constant query, `65536` duplicate outputs across partitions, forgery of every signed field, hidden dependencies, missing/corrupt artifacts, old-profile downgrade, source row/candidate substitution, delayed part visibility and missing/nonempty native marker cases. Use `protocol_test.go` for measured CLI/Go traces, `recovery_test.go` for fault injection and `limits_test.go` for limit/benchmark evidence.

- [ ] **Step 2: Run the gate with an intentionally incompatible role, then a compatible candidate.** The first run must fail on missing capability before any unsafe write; the compatible run must execute all named cases without skips. HG command:

```bash
bazel test //pkg/integration/snapshotquery:snapshotquery_test --test_env=SI_QUERY_RELEASE_MANIFEST_JSON --test_output=errors --test_timeout=1800
```

The shell environment contains the validated JSON prepared by CI; it contains no credentials. Native/gRPC runs use the same case manifest. Existing manual targets `//pkg/integration:integration_test`, `//pkg/integration/testenv:testenv_test`, and `//pkg/integration/sipressurescale:sipressurescale_test` remain explicitly listed and continue passing.

- [ ] **Step 3: Complete the fault, fraud, migration and limit matrix.** Inject crash/lost response at reservation, Submit, restore, sort, capacity acquisition, unsafe prepare, claim, publication and cleanup. Restart the actual owning process with preserved volume state and retry the same operation. Require one consumed identity, the same pin/output, no extra row IDs and fenced late work. Resolve unavailability/deterministic execution failure through retry or committed abort and prove the next block progresses.

```text
For each A1-A14 case and rewriter engine:
  record immutable build/profile manifest and exact input/root/pin/reservation/generation
  execute through CLI and Go native clients where applicable
  inspect sequencer status, source journal, verifier receipt, candidate scan and safe manifest
  compare expected rows/IDs/full roots and forbidden side effects
  save structured outcome, logs and packet/fault trace with secrets removed
  fail the gate for a missing/skipped case or mismatched artifact/profile
```

For A13 test old/new agents, embedded servers, rewriters, source/verifier and Arbiter, v2 historical vectors, explicit genesis, executor transition, old query profile installed but refused for new work, new snapshot migration/restart, and rollback with accepted v3 history. Disable new reservations first, resolve accepted blocks and replay them with retained historical artifacts/executors; test that an old binary refuses incompatible history. Do not simulate rollback by erasing journals or sequence state.

For A14 measure acquisition wait, restore bytes/time, scan/evaluation time, output rows/bytes, peak sort memory/spill, unsafe/ACK2/safe latency and barrier occupancy at the A4 limits and one unit above each bound. Require atomic refusals, finite memory/disk use and correct cleanup/retention. Record measurements as release evidence; do not invent production latency thresholds from payload INSERT benchmarks. A proposed profile whose limits cannot be enforced or whose measured canary cost is unacceptable cannot be activated unchanged.

- [ ] **Step 4: Register CI and write the reviewable activation/rollback runbook.** Add the manual query target explicitly to `.github/workflows/ci.yml` with compatible digest-pinned fixture inputs and required artifact uploads. Existing `bazel test //...` does not select manual targets. Update README/CLAUDE only after runtime implementation passes, stating exactly the supported profile and query-only semantics; preserve unsupported inline VALUES guidance. `docs/superpowers/validation/snapshot-query-release.md` records actual release hashes, a row for every A1–A14 result, measured cost, known limitations and the following operator sequence:

```text
install compatible readers/executors and durable artifact retention with acquisition disabled
verify every role's exact capability/profile and recovery readiness
run complete disposable-network gate and bounded opt-in canary
drain network -> publish approved executor transition if needed -> commit exact active pair
enable new reservations only after authenticated activation publication
observe correctness, resource limits and barrier occupancy against recorded canary evidence

rollback: disable new reservations -> resolve accepted work -> retain v3 readers/profiles/artifacts
verify historical replay and cleanup debt; never reset identities or discard active pins
```

This task writes the runbook and performs disposable tests. A real deployment/activation follows the separate operational authorization and target-network procedure. Keep secrets out of fixture manifests, logs, command arguments and PR evidence.

- [ ] **Step 5: Commit `test(si): gate signed snapshot query activation`.** Run all required repository/CI checks on the final pinned commits. The implementation report lists passing A1–A14 evidence and explicitly distinguishes disposable acceptance from any later production activation; merging the implementation alone does not satisfy the latter.
