# Commitment Durability and Admission Hardening

**Date:** 2026-08-19 **Status:** Proposed **Roadmap:** [remediation roadmap](2026-08-19-storage-integrity-remediation-roadmap.md) Spec K. **Remediates:** [Spec A signed envelope v2](2026-08-18-storage-integrity-signed-envelope-v2-design.md) (Implemented) — findings from the 2026-08-19 review. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §7, §9; [2026-06-30 Arbiter design](2026-06-30-sentio-arbiter-design.md) §4.3, §5.2. **Code base:** arbiter `71657a8` (v0.2.1), arbiter-core `b669ccd` (v0.3.1), housegate `621eaab` (v0.9.3). **Source of truth:** English version.

## 1. Problem

Spec A's two core fixes are correct and adversarially tested: `verifyUserJWSV2` compares all thirteen bound fields against expectations the verifier derives from its own state, and `statements_root` sits inside `ChainHash()` so the anchored hash pins what was sequenced. The remaining defects are around the edges of those two properties.

**1a — the `statements_root` preimage is not frozen.** `replay.CanonicalDigest` is `SHA256("housegate-replay-mvp-v0:" ‖ domain ‖ 0x00 ‖ json.Marshal(v))` (`housegate/pkg/replay/hash.go:36-47`). `json.Marshal` emits struct fields in **declaration order**, so `statements_root` — and through it `ChainHash`, and through that every value anchored on L2 — is a function of the field order of `arbiter.StatementEnvelope` (`arbiter-core/types.go:128-146`). The v2 fields were appended, which was right, but nothing prevents the next refactor from reordering them. `fsm/seal_test.go:190-206` pins a golden for the *header*, in which `StatementsRoot` appears only as an opaque string; `TestSealL3Block_StatementsRootCommitsEnvelopes` recomputes with the same code, so it cannot detect a change; arbiter-core's conformance gate compares json-tag *names* to proto field names, not order or types.

Failure: someone alphabetizes or regroups `StatementEnvelope` in arbiter-core v0.4.0. Every historical `statements_root`, hence every historical `ChainHash`, hence every anchored value, becomes unrecomputable — and CI is green in all repos. This defeats the non-repudiation Spec A exists to add.

**1b — `Params.NetworkID` is validated only at config load.** `fsm.New(Params{})` accepts an empty `NetworkID`; admission then evaluates `env.NetworkID != f.st.Params.NetworkID`, so with both empty the cross-network binding is a no-op. The only guard is `config.Load` → `Validate`; `run(ctx, cfg, logger)` never re-validates and `cmd/arbiter/main_test.go` constructs a `config.Config` literal and calls `run` directly, proving the bypass exists for embedded and library hosts.

**1c — `L3BlockView` can return fewer envelopes than `StatementCount`.** `fsm/reads.go:126-131` appends only non-nil statement records with no length assertion, and `GetL3Block` forwards whatever it gets. This is the auditor's recompute surface: a short list yields a different `statements_root` and reads as fraud with no diagnostic. Latent today (nothing deletes from `f.st.Statements`) and untested.

**1d — the SNode journal-replay path skips `schema_hash` re-verification.** `arbiter-core/snode/staged.go:87-113` returns the cached result after only a `reflect.DeepEqual` on the envelope; the schema-hash check sits below that branch. If the role's table set changed between the original prepare and a replay, the cached result is served under the new schema. Spec A §8 says the check happens before decoding — true on the fresh path, skipped on replay.

**1e — the deferred lane accepts named client Data blocks.** `chproto.ClientDataPacketIsEmpty` ignores the block name and `runDeferredInsert` classifies purely on empty/non-empty. A client that sends an external temporary table before the INSERT payload gets those bytes folded into `payload_hash`, and the following end-of-external-tables empty block is consumed as the terminator, so the real payload is never read. The ingress's Native decode would almost certainly reject on column mismatch, so today's net effect is a confusing failure rather than corruption — but the signed lane should refuse a non-empty *named* block explicitly.

**1f — `settings_hash` is enforced against a prefix, not a key set.** `pkg/storageintegrity/settings.go:23-25` treats any `SQL_x_*` / `SQL_sentio_*` key as HouseGate-owned and excludes it from the hash, so a client can attach arbitrary unsigned `SQL_x_whatever` settings that reach ClickHouse. Spec A D4's stated invariant is "the SI lane requires no remaining client settings"; the owned set is actually enumerable (`AuthTokenSettingKey`, `StatementTokenSettingKey`, `PayerSettingKey`, `DriverSettingKey`, `MaintenanceSettingKey`, `PlatformOperatorSettingKey`, `SQL_x_read_mode`).

**1g — `statement_kind` is not in the signed payload.** Spec A §2 goal 1 says the signature binds every field that affects what is executed; §4.2's payload list omits `statement_kind`. Today it is pinned by the INSERT-only admission freeze, so this is latent — and it becomes attacker-malleable the day the mutation lane ships. The implementation followed §4.2 faithfully; the *spec* is internally inconsistent.

Two smaller items, folded in because they touch the same files: the arbiter classifies a non-canonical protected header as `INVALID_SIGNATURE` rather than `MALFORMED` (PR #17's taxonomy work missed this one case, and its test asserts only `err != nil`); and `snode.ErrPayloadMismatch` from `validatePrepareBindings` — a binding violation as unretriable as `ErrSchemaHashMismatch` — is not mapped to `ErrPrepareTerminalReject` by sentio-node's adapter, so it holds the source frontier behind a lookup for a write that provably cannot exist.

## 2. Goals / non-goals

Goals: (1) the anchored commitment's preimage is pinned by a test that fails on any encoding change; (2) network binding cannot degrade to a no-op; (3) the audit read surface is complete or errors; (4) replay and deferred-input paths are as strict as the fresh paths; (5) `settings_hash` is enforced against the actual owned key set; (6) `statement_kind` is bound before the mutation lane exists.

Non-goals: replacing `CanonicalDigest` with RFC-8785 canonical JSON (recorded as debt in D1); changing the row-id or row-element profile; the mutation lane itself; freshness semantics inside `Apply` (it stays wall-clock-free).

## 3. Decisions

**D1 — Freeze the preimage with golden vectors, and record the canonicalization debt.** Add `CanonicalDigest(DomainL3Statements, [fixed envelope list])` and `CanonicalDigest(DomainL3Header, [fixed header])` golden vectors as committed JSON test data in arbiter, with the exact expected digests, plus the marshalled bytes so a diff shows *what* changed rather than only that a hash moved. Mirror the envelope vector in arbiter-core so a field reorder there fails that repo's own tests first. Record in the spec and in Spec B's edit list that structural canonicalization (field-order-independent encoding) is the correct long-term fix and is deliberately deferred. Rejected for this spec: switching to RFC-8785 — it changes every historical root and is a protocol migration, not a hardening fix.

**D2 — Validate genesis identity at the FSM boundary.** `fsm.New` and `Restore` reject an empty `NetworkID` (and an empty `SchemaSnapshotID` / `ExecutorProfileID`, which have the same shape of risk). The config-level validation stays.

**D3 — `L3BlockView` errors on an incomplete block.** Assert `len(envs) == header.StatementCount` and return a distinguishable error; `GetL3Block` maps it to a gRPC error naming the block seq. An auditor must never silently receive a short list.

**D4 — Replay and fresh prepare share one validation function.** Hoist the binding checks — `schema_hash` against the role's current schema source, payload format, revision — above the cached-result branch in `snode/staged.go`, so a cached result is only returned after the same checks the fresh path runs.

**D5 — The deferred lane rejects non-empty named Data blocks.** Extend the chproto helper to expose the block name, and have `runDeferredInsert` refuse a non-empty block whose name is not empty, with a message naming external tables as unsupported in the signed lane. An empty named block (the external-tables terminator) is likewise refused rather than being mistaken for the payload terminator.

**D6 — `settings_hash` is enforced against the enumerated owned key set.** Replace the prefix test with a set membership test over the exported setting-key constants. Any other setting, prefixed or not, is a rejection naming the key (the existing message quality is good and should be kept).

**D7 — Bind `statement_kind`.** Add it to `JWSStatementPayloadV2` and to `StatementPayloadV2Mismatch`, and to the arbiter and ingress `want` construction. This is a signing-payload change, so it is a wire break: bump arbiter-proto's minor, regenerate the shared JWS vectors, and cut coordinated releases — the same hard-cutover discipline Spec A used, and cheap now because nothing is deployed. Also correct Spec A §4.2's field list (Spec B's edit list) so spec and code agree.

**D8 — Two small corrections.** Wrap the non-canonical-header error in `errMalformedUserJWS` so it classifies as `MALFORMED`, and strengthen its test to assert the code rather than `err != nil`. Map `snode.ErrPayloadMismatch` to `ErrPrepareTerminalReject` in sentio-node's adapter.

## 4. Testing / acceptance

- The golden vectors fail when a field is reordered in `StatementEnvelope` or `L3BlockHeader` — prove it by reordering locally, observing red, and reverting (the test's whole purpose is that it cannot be satisfied by recomputation).
- `fsm.New`/`Restore` with an empty `NetworkID` returns an error; the existing `run()`-with-literal-config test is updated to a valid config and a new test asserts the rejection.
- `GetL3Block` on a synthetically incomplete block errors and names the seq.
- A cached prepare whose role schema changed since the original returns the terminal schema-hash rejection rather than the cached result.
- A deferred INSERT preceded by an external-table Data block is rejected before any signing, with the payload buffer released.
- An unsigned `SQL_x_anything` on the SI lane is rejected naming the key; the six owned keys are accepted.
- Shared JWS vectors regenerated with `statement_kind`; both housegate and arbiter consume them; the reject vectors assert the failing field name rather than only `err != nil` (the current weakness on both sides).
- The arbiter fraud test's `strings.Contains(msg, "payload_hash")` coupling to housegate's switch ordering is replaced by an assertion on a structured field name, so reordering `StatementPayloadV2Mismatch` cannot break an arbiter test silently.

## 5. Delivery

1. arbiter + arbiter-core: golden vectors (D1), FSM identity validation (D2), `L3BlockView` completeness (D3), header-taxonomy fix (D8a).
2. arbiter-core: shared prepare validation (D4), and the `ErrPayloadMismatch` classification consumed by sentio-node (D8b).
3. housegate: deferred named-block rejection (D5), owned-key-set `settings_hash` (D6).
4. arbiter-proto + housegate + arbiter + arbiter-core + sentio-node: `statement_kind` binding (D7) as a coordinated minor bump with regenerated vectors.
5. Spec B edit list: §4.2 field list correction and the canonicalization-debt note.
