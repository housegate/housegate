# Signed Statement Envelope v2 and Agent-Side Payload Commitment

**Date:** 2026-08-18 **Status:** Proposed **Roadmap:** [v1 closure roadmap](2026-08-18-storage-integrity-v1-closure-roadmap.md) Spec A. **Base:** [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §4, §7, §9; [2026-06-30 Arbiter design](2026-06-30-sentio-arbiter-design.md) §4–§6; [2026-07-16 signed ingress](housegate-storage-integrity-insert/2026-07-16-housegate-storage-integrity-signed-ingress-design.md); [2026-07-20 P1e runtime](housegate-storage-integrity-insert/2026-07-20-housegate-storage-integrity-p1e-runtime-e2e-design.md); [2026-07-01 agent materialize](2026-07-01-agent-materialize-nondeterminism-design.md). **Code base:** arbiter-proto `v0.4.0`, arbiter `edd23c3`, arbiter-core `829c44f`, housegate `c6f7a6d`, sentio-node `9f12620`. **Source of truth:** English version.

## 1. Problem

The base design's trust argument (§4, §9) is: the user signs `(sql_hash, payload_hash, statement_id, target table, …)` before submission; the operator-side HouseGate is untrusted once it merely forwards signed input; therefore a HouseGate that swaps the payload or re-associates the SQL with a different statement is caught, because verifiers replay what was signed and the byte-side checks bind the promoted bytes to that replay. Two implementation shortcuts break the first link of that chain:

1. **`user_jws` binds only the SQL text.** The token the agent attaches (`SQL_x_auth_token`) is the ordinary housegate query JWS — payload `{iat, qhash = Keccak256(sql)}` — and Arbiter admission re-verifies exactly that (`arbiter/fsm/userjws.go`, `applySubmitStatement` step 3). `statement_id`, `payload_hash`, `payload_length`, `target_table_id`, `settings_hash`, purpose: none are signed. The ingress checks that the JWS signer equals `statement_id.client_account`, but a captured token can be resubmitted with a fresh `client_seq`, a different payload, and a different target table, and admission accepts it (the accumulator dedups by `(account, client_seq)`, which is unsigned; the only bound is the `iat` freshness window at ingress). The `StatementEnvelopeV2` proto also lacks `envelope_version`, `network_id`, `keeper_shard_id`, `payload_format`, `row_id_profile_id`, and always sends `settings_hash = ""`.
2. **`L3BlockHeader.ChainHash` commits no statement content** (`arbiter/fsm/state.go`, `L3BlockHeader`): only the seq range, `spent_ids_root_after`, and the prev/schema/profile identities. The anchored `l3BlockHash` therefore does not pin which SQL/payload was sequenced under a given `(account, client_seq)`.

A third, related gap explains *why* (1) happened and constrains the fix: the ClickHouse native protocol sends the Query packet (which carries the settings, hence the token) **before** the INSERT payload, and a client will not send its Data blocks until the server has answered the Query with the sample block. The agent therefore signed the only thing it had in hand at Query time — the SQL. Signing `payload_hash` requires the agent to answer the sample block itself, buffer the payload, hash it, and only then forward the Query. That in turn requires the agent to know the table's column structure without asking upstream, which the schema registry (Phase B, network-state `TableSchemas`) now provides.

Nothing is deployed, so this is a hard cutover: envelope v2 replaces v1, arbiter-proto gets a minor bump, the FSM snapshot version bumps, and no dual-read is built.

## 2. Goals / non-goals

Goals:

1. The user signature binds every field of the envelope that affects what gets executed and where it is attributed: `statement_id`, `sql_hash`, `settings_hash`, `schema_hash`, `payload_hash`, `payload_length`, `payload_format`, `client_revision`, `target_table_id`, `network_id`, `keeper_shard_id`, `row_id_profile_id`, with a dedicated `purpose`.
2. The bytes stored in the payload store and replayed by verifiers are byte-identical to the bytes the user signed. HouseGate performs no transformation of the signed payload (base design §5.1).
3. The L3 chain hash commits to the sequenced envelopes.
4. Admission verifies all bindings deterministically inside the FSM (no wall clock; the `iat` window stays at ingress).
5. The agent-mode HouseGate can produce v2 envelopes for `clickhouse-client` / clickhouse-go writers without SDK changes.

Non-goals: per-statement `schema_snapshot_id` / DDL lane; signing anything sequencer-assigned; SDK-native envelope construction (compatible, not built); changing `_hg_row_id` derivation or its SNode-side injection; multi-agent-per-account `client_seq` coordination.

## 3. Decisions

**D1 — Canonical signed payload is the Native wire encoding.** The SI lane's `payload_format` is `clickhouse-native-data-v1`: the concatenation, in arrival order, of the de-chunked `Packet.Raw` bytes of every non-empty client `Data` packet of the INSERT (exactly the byte definition today's `CapturedPayload` in `pkg/plugins/storageintegrity` uses; the terminal empty block is excluded; compressed blocks stay rejected). Both the agent and the ingress see these bytes identically because `Packet.Raw` is transport-framing-independent (chunked/non-chunked is per leg). The ingress-side `NativeCSVPayloadMaterializer` bridge and `StorageIntegrityPayloadMaterializer` option are **removed** from the SI path; `csv-with-names-v1` remains only as a `payloadexec` test/legacy encoding. Rejected alternative: keep CSV canonical and have the agent run the same Native→CSV conversion — it needs the same schema at the agent anyway, keeps a schema-dependent transformation on the untrusted side (only "checked" by re-derivation), and turns every housegate version skew into a fail-closed mismatch; byte identity has neither problem and deletes code.

**D2 — The agent answers the INSERT sample block locally** from the network-state table schema and defers the upstream Query until the payload is complete. This is the only way to sign `payload_hash` inside the native protocol without an SDK. Rejected: a second "commit" token after the data (no protocol slot; breaks one-query-in-flight), or trusting the ingress for the payload hash (the attack we are closing).

**D3 — Two tokens per SI INSERT.** `SQL_x_auth_token` keeps its legacy query-JWS payload for the ordinary auth plugin; a new setting `SQL_x_statement_token` carries the v2 statement JWS. Domain separation stays intact and the auth plugin is untouched. Rejected: one token with both `qhash` and the v2 fields — it would make the auth plugin's purpose-optional legacy path accept a statement token and vice versa.

**D4 — v1 `settings_hash` commits to an empty user-settings set.** After stripping housegate-owned keys (`SQL_x_*`, `SQL_sentio_*` — the auth token, payer, driver flag, statement token), the SI lane requires no remaining client settings (a writer that needs, e.g., `async_insert` or `input_format_*` cannot use the SI lane in v1 — surface this as a clear rejection message naming the setting); `settings_hash = replay.DigestJSON("housegate-settings-v1", [])` (a constant). Replay applies no settings today, so admitting any would silently break determinism. Lifting this is a P2+ per-setting review.

**D5 — `schema_hash`, not `schema_snapshot_id`, in the envelope.** The agent signs the Phase-B `schema_hash` of the table version it built the sample block from; SNode and verifier verify it against their own schema source before decoding. `schema_snapshot_id` remains the block-level genesis parameter (base design §7 "v1: block-level").

**D6 — Agent-generated `statement_id` when the client did not supply one.** `client_account` = agent signer address; `client_seq` from a durable per-agent counter; `client_nonce` = 16 random bytes hex. A client that already set the ClickHouse query id in the flat SI form with a matching account keeps it (SDK path), canonicalized to the lowercase-account flat form; before signing, the agent durably advances its counter to at least that supplied `client_seq`, so the next generated id cannot reuse the SDK-reserved sequence.

## 4. Envelope v2

### 4.1 Wire (arbiter-proto `proto/arbiter.proto`, minor bump → v0.5.0)

Append to `StatementEnvelopeV2` (compatible field appends; the message name keeps "V2" — the version is now explicit in the payload):

```proto
message StatementEnvelopeV2 {
  StatementID statement_id = 1;
  StatementKind statement_kind = 2;
  string sql = 3;
  string sql_hash = 4;
  string settings_hash = 5;
  string payload_ref = 6;
  string payload_hash = 7;
  uint64 payload_length = 8;
  string target_table_id = 9;
  string user_jws = 10;
  // ---- v2 additions (all signed by user_jws) ----
  uint32 envelope_version = 11;   // must be 2
  string network_id = 12;         // must equal the Arbiter genesis network id
  uint32 keeper_shard_id = 13;    // must be 0 in v1 (Sharder returns group 0)
  string payload_format = 14;     // "clickhouse-native-data-v1"
  uint32 client_revision = 15;    // ClickHouse client protocol revision the Native blocks were encoded for
  string schema_hash = 16;        // Phase-B TableSchemaHash the payload was encoded against
  string row_id_profile_id = 17;  // "housegate-row-id-v1"
}
```

`arbiter-core` root `StatementEnvelope` gets the same fields with JSON tags equal to the proto names (the `conformance/arbiter_wire_test.go` field-set gate must be extended). `wire/convert.go` maps them.

### 4.2 `user_jws_v2` signing payload

Compact JWS, header `{"alg":"ES256K","typ":"JWT"}` (same secp256k1 recovery scheme as `pkg/auth.RelaySigner` / `EthValidator`; the recovered address must equal `statement_id.client_account`). Payload:

```json
{
  "purpose": "housegate-statement-v2",
  "iat": 1755000000,
  "network_id": "…",
  "keeper_shard_id": 0,
  "statement_id": "<account>:<seq>:<nonce>",
  "sql_hash": "0x…",
  "settings_hash": "0x…",
  "schema_hash": "0x…",
  "payload_hash": "0x…",
  "payload_length": 12345,
  "payload_format": "clickhouse-native-data-v1",
  "client_revision": 54460,
  "target_table_id": "db.table",
  "row_id_profile_id": "housegate-row-id-v1"
}
```

Field-for-field this is base design §7's `user_jws_v2` with `schema_hash` in place of `schema_snapshot_id` (D5) and `client_revision` added (needed to interpret the Native bytes). Add `auth.StatementPurposeV2 = "housegate-statement-v2"`, `auth.StatementTokenSettingKey = "SQL_x_statement_token"`, a `JWSStatementPayloadV2` struct, `RelaySigner.SignStatementV2(payload)`, and `EthValidator.ValidateStatementV2(token, want JWSStatementPayloadV2) (signer, error)` that requires purpose equality and **exact equality of every field** against the envelope-derived expectation (not just `qhash`). Keep the legacy `SignToken`/`ValidateQuery` untouched.

### 4.3 Hash profiles

- `sql_hash = replay.DigestString(sql)` (unchanged).
- `payload_hash = replay.DigestBytes(payload)` over the D1 bytes (unchanged digest, changed bytes: no CSV conversion).
- `settings_hash = replay.DigestJSON("housegate-settings-v1", []string{})` — the constant empty-set digest; export it as `storageintegrity.EmptySettingsHash`.
- `schema_hash = payloadexec.TableSchemaHash(networkID, schema)` (Phase B, unchanged).

### 4.4 L3 header

`arbiter/fsm/state.go` `L3BlockHeader` gains `StatementsRoot string \`json:"statements_root"\`` = `replay.CanonicalDigest("arbiter-l3-statements-v1", []StatementEnvelope)` over the sealed block's envelopes in `statement_seq` order (canonical JSON of the arbiter-core type). `applySealL3Block` computes it; `ChainHash()` includes it (still excluding the back-filled anchor). Add the domain constant to `arbiter-core/domains.go`. Also expose it in `SafeState.GetManifestByBlock` / a new `SafeState.GetL3Block(seq)` response so auditors can fetch header + envelopes and recompute (D of the roadmap consumes the same RPC).

## 5. Agent side (housegate agent mode)

### 5.1 New plugin `pkg/plugins/sistatement` (agent-mode-only, default off, config `storage_integrity.agent.*`)

Runs **after** `materialize` and **before** `agent.Signer` in `buildAgent`. Hook set: `QueryPlugin` (`OnQuery`), `StrictDataPlugin` (`OnClientDataStrict`), `QueryInputCompletePlugin`-strict (`OnQueryInputCompleteStrict`), `QueryAbortPlugin`, `ClosePlugin`. Behaviour per INSERT that classifies as payload-local Native INSERT (reuse `sicore.InsertPayloadEncoding` from `pkg/storageintegrity/sql.go`; anything else — `VALUES`, `SELECT`, non-INSERT — is left to the ordinary path):

1. **OnQuery.** Resolve `target_table_id` from the SQL (`db.table`, `USE` mirrored from `SessionState`). Resolve the table schema from `NetworkState` (`registry.TableSchemas.LatestTableSchema`); missing schema → reject the query with an Exception (fail closed; a table not declared on chain cannot enter the SI lane). Compute `schema_hash`. Strip nothing yet; verify the client sent no non-`SQL_x_*` settings (D4) else reject. Determine/generate `statement_id` (D6) and set `Query.ID` to its flat form. Set `qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: schema.Columns (minus `_hg_row_id`), MaxPayloadBytes}` — this tells Relay to run the deferred-INSERT protocol in §5.2.
2. **OnClientDataStrict.** Append `Packet.Raw` to the payload buffer under the byte budget (reuse the `ClientDataReadLimit` pattern from the ingress plugin so an oversized packet is rejected before allocation). Reject compressed blocks.
3. **OnQueryInputCompleteStrict.** Compute `payload_hash`/`payload_length`, build `JWSStatementPayloadV2`, sign with the agent key, append `SQL_x_statement_token` (Custom, quoted like the auth token) to `qctx.Query.Settings`. Return; Relay now forwards.
4. **OnQueryAbort / OnClose.** Drop the buffer.

`client_seq` durability: `storage_integrity.agent.state_dir/<account>.seq` holds the last issued seq; write-and-fsync **before** signing; on restart continue from `last+1`. A crash between fsync and submission wastes one seq (a gap the accumulator's K=64 budget absorbs); reuse is impossible by construction. Multiple agents sharing one key are out of scope (documented).

Config: `storage_integrity.agent.{enabled, network_id, keeper_shard_id(=0), state_dir, max_payload_bytes(64MiB), require_network_state(true)}`; `Validate()` runs agent-mode-only; requires `agent.private_key_hex` and `network_state.source`.

### 5.2 Relay deferred-INSERT mode (`pkg/proxy/relay.go`)

When a QueryPlugin sets `qctx.DeferredInsert`, Relay:

1. does **not** forward the Query; instead writes to the client a `Data` packet holding an empty block with the plan's columns (new `Codec.WriteSampleBlock(cols []proto.ColInput)` — encode a 0-row `proto.Block` for the client's negotiated revision through the client codec's writer; `WriteEmptyDataBlock` is the column-less variant and stays for the terminator);
2. reads client packets: `Data` → fire `OnClientDataStrict` and buffer `Packet.Raw` (Relay owns the buffer bounded by `MaxPayloadBytes`; exceeding it → Exception + abort); `Cancel` → abort; the terminal empty block → fire `OnQueryInputCompleteStrict`;
3. on success writes the (now token-carrying) Query upstream, reads exactly one upstream response packet: `Data` (the upstream sample block — discard) or `Exception` (forward to client, abort); then writes every buffered raw Data packet via `WriteRawPacket` and the terminator, and resumes the ordinary upstream→client loop for the terminal `EndOfStream`/`Exception`;
4. keeps the existing invariants: one query in flight; the client codec's `proto.Reader` is never replaced; forwarding uses `WriteRawPacket`, never `Splice`; the deferred buffer is released on every exit path.

`DeferredInsert` and `SuppressUpstreamExecution` are mutually exclusive on one query (agent vs server roles); Relay rejects a plan that sets both. Add fragmentation/coalescing tests in the style of the existing relay tests (Query+first Data in one segment; terminator split; upstream Exception at the sample-block step; oversized payload; cancel mid-payload).

## 6. Ingress side (housegate server mode, `pkg/plugins/storageintegrity` + `pkg/storageintegrity`)

- Read `SQL_x_statement_token`; validate with `ValidateStatementV2` against the expectation the ingress computes itself from what it captured: `sql_hash` of the signed SQL, `payload_hash/length` of its own captured bytes (D1 — no conversion), `settings_hash == EmptySettingsHash`, `payload_format == clickhouse-native-data-v1`, `client_revision == SessionState.ClientRevision`, `target_table_id`, `network_id` from config, `keeper_shard_id == 0`, `row_id_profile_id == payloadexec.RowIDProfileID`, `schema_hash` **equal to the ingress's own resolution of the same table's latest network-state schema** (fail closed if the ingress cannot resolve it). Reject on any mismatch before payload upload. Keep the existing `SQL_x_auth_token` validation (the auth plugin) as is.
- Delete the CSV bridge: `StorageIntegrityPayloadMaterializer` option, `NativeCSVPayloadMaterializer`, `csv_payload.go`, the "runtime pins MaterializerCSV" rule, and the `Revision == 0` CSV check. `SelectMaterializerKind` stays (Native path).
- `AdmissionRecord` / `StatementEnvelope` (`intake.go`) gain the v2 fields; `EnvelopeFromAdmission` re-verifies them; `arbiter_proto.go` fills every proto field (no more `SettingsHash: ""`).
- `sql.go`'s `requirePayloadLocalInsert` unchanged; `FORMAT CSVWithNames` in the SQL text is still accepted because the wire is Native regardless — the *stored* encoding is now always native.

## 7. Arbiter side

`fsm/admission.go` `applySubmitStatement` gains, between steps 1 and 3: `envelope_version == 2`; `network_id == f.st.Params.NetworkID` (add `NetworkID` to genesis `Params`/config); `keeper_shard_id == 0`; `payload_format ∈ {clickhouse-native-data-v1}`; `row_id_profile_id == "housegate-row-id-v1"`; `settings_hash == EmptySettingsHash`; `schema_hash != ""`; `client_revision != 0`. Step 3 becomes `verifyUserJWSV2(env)`: parse the compact JWS, require `purpose == housegate-statement-v2`, and require every payload field to equal the envelope-derived value (`statement_id` flat form with lowercased account), then recover and compare the signer. Malformed → `MALFORMED`; binding/signature failure → `INVALID_SIGNATURE`. All wall-clock-free.

`fsm/state.go`: `L3BlockHeader.StatementsRoot` (§4.4); `snapshotVersion = 2` (hard cutover: a v1 snapshot is refused with a clear error; devnet2 has none). `fsm/reads_dispatch.go` / `orchestrator/dispatch.go`: `ReplayJob` statements carry `payload_format` + `client_revision` (extend `replay.proto` `Statement` and housegate `replay.Statement`/`PreparedStatement` accordingly — additive, JSON-tag frozen, conformance test extended).

## 8. Data plane (arbiter-core)

- `snode/staged.go`: accept `payload_format == clickhouse-native-data-v1`; decode via housegate's Native decoder; keep the payload-before-write gate (`payload_hash`/`payload_length` re-verified against the fetched bytes). Verify `schema_hash` against the SNode's schema source for `target_table_id` **before** decoding; mismatch → terminal reject (new `PrepareLocalStatement` error class the housegate orchestrator maps to `TerminalReject`). Drop the `stagedCSVEncoding` constant.
- `verifier/backends.go` / housegate `pkg/replay/chexec`: `chexec.Materializer.Materialize` branches on `st.PayloadFormat`: native → `DecodeNativePayload(schema, revision, payload)`, csv → `DecodeCSV` (kept for tests). Move `DecodeNativePayload` + `NativeMaterializer` out of `pkg/storageintegrity` into `pkg/replay/nativepayload` so `chexec` does not import the ingress package (pure move, same behaviour, `_hg_row_id`-in-payload rejection kept).
- `verifier` also verifies `schema_hash` against its schema source before replay; mismatch → the receipt is still signed with the mismatch (challenge evidence, base design C.4), not a local error.
- sentio-node `storageintegrityadapter`: carry the new fields through `toHousegatePrepared`; drop the `NewSchemaResolver` wiring for the CSV bridge; nothing else.

## 9. Testing / acceptance

- housegate: `pkg/auth` v2 sign/verify vectors (purpose mismatch, each field mismatch → reject; recovered address ≠ account → reject); relay deferred-INSERT fragmentation matrix (§5.2); `sistatement` plugin unit tests (schema missing → reject; non-`SQL_x_*` setting → reject; seq durability across restart; oversized payload); ingress v2 validation matrix; `pkg/replay/nativepayload` move keeps `native_payload_test.go` green; `pkg/integration` adds an agent-mode SI INSERT through a real ClickHouse (agent → server housegate → CH) asserting the stored bytes equal the client's Data packets.
- arbiter: admission table tests for every new reject; `verifyUserJWSV2` vectors shared with housegate (`testdata/statement_jws_v2.json`, produced once, consumed by both repos); `L3BlockHeader.ChainHash` golden vector updated with `statements_root`; snapshot v2 round-trip; `integration/chpipeline` end-to-end re-run with native payloads plus a **fourth fraud class**: a modified ingress that swaps the payload after signing → `INVALID_SIGNATURE` at admission (this is the property the spec exists for).
- arbiter-core: conformance field-set gates updated; `snode` native prepare + `schema_hash` mismatch reject; `verifier` native replay equivalence with the in-process executor.
- Docs: base design §7 signing payload table updated by Spec B, referencing this spec.

## 10. Delivery (PR-sized, in order)

1. arbiter-proto: envelope fields + `replay.proto` `Statement.payload_format/client_revision` → tag v0.5.0.
2. housegate: `pkg/auth` v2 payload/signer/validator + `pkg/replay/nativepayload` move + `replay.Statement` fields (no behaviour change yet).
3. housegate: Relay deferred-INSERT mode + `Codec.WriteSampleBlock` + tests.
4. housegate: `sistatement` agent plugin + config + `buildAgent` wiring.
5. housegate: ingress v2 validation, CSV bridge removal, `arbiter_proto.go` fill, orchestrator field carry.
6. arbiter-core: types/wire/conformance, snode native + `schema_hash`, verifier native branch → tag.
7. arbiter: admission v2, `statements_root`, snapshot v2, `GetL3Block`, chpipeline fourth fraud class.
8. sentio-node: adapter carry-through, config, smoke update.
9. Spec B doc updates.
