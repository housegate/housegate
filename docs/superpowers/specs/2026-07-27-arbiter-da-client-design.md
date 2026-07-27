# Sentio Arbiter — DA Payload-Store Client Integration Design

**Phase:** DA caller-side integration (the P1d §10 recorded follow-up; precedes and unblocks P1e's put leg). **Base:** arbiter main `3ab733c` (P1d complete: EVM anchor merged, CI on self-hosted), arbiter-proto `v0.3.0` (da.proto tagged and consumable), da-store server `sentioxyz/network-da` main `8b67059` (v1 service implemented: GCS/fs/mem backends, dev mode, k8s `da-test` deployment), housegate main `bc85c46`. **Parent designs:** [2026-06-30 Sentio Arbiter design](2026-06-30-sentio-arbiter-design.md) §3.5 steps 1–3, [P1d design](2026-07-15-arbiter-p1d-evm-anchor-design.md) §9/§10 (da.proto context + deferral record), arbiter-proto `docs/superpowers/specs/2026-07-14-da-payload-store-api-design.md` (the frozen contract), [2026-07-01 incremental design](2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md) §3.2 (payload-before-write, P1e dependency).

This phase implements the three client legs of the frozen `da.proto` contract inside the arbiter repo: the Verifier fetch path (`PayloadStore.FetchPayloads` behind housegate's `replay.PayloadStore` interface), the SNode put-spool (`PutPayloadInline`/`PutPayload` behind the intake's payload-before-write gate), and the Arbiter custody chain (`PayloadLifecycle.PinPayloads`/`ReleasePins` driven from the ingress ack barrier and the leader orchestrator). After this phase the data plane no longer assumes a shared filesystem between SNode and Verifiers, and every sequenced payload is covered by a durable pin from its `SubmitStatement` ack onward.

## 1. Decisions (frozen for this phase)

| # | Decision | Rationale |
|---|---|---|
| 1 | **One shared client package `dataplane/dastore`** — the repo's only da.proto pb ⇄ Go boundary (mirroring `wire/` for raftlog.proto). Verifier, SNode, and both arbiter-node injection points consume the same `Client`. | Chunked-put framing, fetch reassembly, limits caching, and batch splitting are protocol logic that must not be written three times. A separate package (not `dataplane/`) because the store is a passive server — the existing `dataplane.Client`'s dial-the-Arbiter/NotLeader re-homing machinery explicitly does not apply (da.proto scope note). |
| 2 | **Custody progression is pure idempotent rescan — no new Raft command, no FSM Apply/snapshot change.** The orchestrator derives "which pins must exist" from a new read-facade view and re-asserts them every rescan, deduplicated by leader-term memory; a failed-over leader re-asserts from scratch. | da.proto's own failover model ("the new leader idempotently re-asserts the pins the chain says must exist") plus the store's idempotency guarantees (re-pin is a no-op, releasing an unknown key returns OK) make persistence unnecessary. Extending the command alphabet for an external side effect would touch the P1a frozen surface for no correctness gain. |
| 3 | **Ingress ack barrier:** `SubmitStatement` pins SEQUENCED (scope = decimal `statement_seq`) after a successful `ACCEPTED` apply and **before** returning the ack; pin failure returns gRPC `Unavailable` with no ack. The duplicate re-ack path re-asserts the pin before re-emitting the stored ack. Non-`ACCEPTED` outcomes never pin. | Verbatim the da.proto custody-chain rule: the ack is the custody barrier that ends the writer's lease duty, and no ack path — first send or idempotent replay — may outrun the pin. The client-retry → duplicate-path → re-pin → re-ack loop is the contract's own recovery design. |
| 4 | **REPLAY→AUDIT transition = `Safe ∧ EvidenceComplete`; AUDIT pins are not released in v1.** Per sealed block: (a) pin REPLAY (scope = decimal `block_seq`) then release the covered SEQUENCED pins; (b) once the safe watermark covers the block AND all three dispatched verifiers have both attestation and scan recorded, pin AUDIT then release REPLAY. The audit window (master design Open Question 6) is P3's to define; until then AUDIT pins accumulate. | v1 has no verifier-timeout policy (P1b: "timeouts do not abandon work"), so da.proto's REPLAY hold condition ("every dispatched verifier has reported or been timed out per stated policy") is only satisfiable by full evidence. Holding AUDIT forever errs in the conservative direction — the store may never collect, which is a conforming outcome. User-approved on 2026-07-27. |
| 5 | **`ReleasePins.authority_jws` stays empty (channel-trust).** | da-store v1 is channel-trust-only: `--enforcement=on` refuses to start and the JWS value is only audit-logged. The signed variant (purpose `arbiter-payload-release`, domain `arbiter-payload-release-v1`, monotonic `release_seq`) is a recorded follow-up gated on the store enabling enforcement. |
| 6 | **`fspayload` stays as the `fs` backend.** SNode's dependency narrows from `*fspayload.Store` to a `PayloadSpool` interface; the verifier binary already takes the `replay.PayloadStore` interface. Both binaries grow a `payload_store` config block with `backend: fs | grpc`, defaulting to `fs` with the existing `payload_dir` semantics. | Local dev, unit tests, and single-machine deployments keep working unchanged; the grpc backend is opt-in per deployment. The arbiter node's custody wiring is likewise optional — no `payload_store` block means no pins, preserving current cluster tests and deployments byte-for-byte. |
| 7 | **Put asserts ref identity.** SNode's put sends the payload with the envelope's declared `(payload_hash, payload_length)` and fails closed if the store's minted `payload_ref` differs from `envelope.payload_ref`. Dedupe hits are success (and are the lease-refresh primitive). | The envelope's ref must be the store's ref or replay-time fetches resolve the wrong identity; divergence means the submitter and the store disagree on ref minting and must be surfaced, not papered over. |
| 8 | **The client is hash-silent end to end.** `GetPayload` returns raw bytes; content verification against the sequenced envelope's `payload_hash`/`payload_length` stays exclusively in `replay.Verifier` where it already lives. | The contract's structural trust rule: no read response asserts integrity, and layering the check twice would blur which layer is load-bearing. |
| 9 | **v1 simplifications:** whole-payload fetch (no range resume — a broken stream is re-fetched from zero), no `StatPayloads` prefetch gating, no batch fetch fan-in (the `replay.PayloadStore` interface is per-ref), `GetStoreLimits` fetched lazily once per process and cached, plaintext gRPC (P1b/P1c decision carries over). | YAGNI with recorded roll-forwards (§10). Payloads in the v1 envelope are single-statement CSV bodies; resume and prefetch optimize a cost we have not observed. |

## 2. Non-goals (explicitly out)

The payload-store **service** (implemented and owned in `sentioxyz/network-da`); HouseGate ingress client and its put leg (P1e — it will reuse `dastore.Client`); AUDIT-pin release and the audit window (P3 / master design Open Question 6); signed `ReleasePins` with monotonic `release_seq` (follow-up gated on store enforcement); `AnchorRef.DARef` block-level DA references (P5+); Celestia/EigenDA/4844 backends (P5+, store-side config swap by contract design); fetch range resume, batch prefetch, `StatPayloads`-based dispatch gating (recorded roll-forwards); TLS/mTLS (P3 hardening, repo-wide); metrics (repo-wide metrics follow-up — structured logs only, matching P1d).

## 3. `dataplane/dastore` package

```go
type Config struct {
    DataAddr    string        // PayloadStore endpoint (prod :9001; dev mode shares one addr)
    ControlAddr string        // PayloadLifecycle endpoint (prod :9002); empty ⇒ lifecycle calls error
    DialTimeout time.Duration // default 5s
    CallTimeout time.Duration // per unary RPC, default 10s; streams use ctx only
}

func New(cfg Config) (*Client, error)                                  // lazy dial, no I/O
func (c *Client) Close() error
func (c *Client) Put(ctx context.Context, expectedRef string, payload []byte) error
func (c *Client) GetPayload(ctx context.Context, payloadRef string) ([]byte, error) // satisfies replay.PayloadStore
func (c *Client) Pin(ctx context.Context, purpose pb.PinPurpose, scopeKey string, refs []string) error
func (c *Client) Release(ctx context.Context, purpose pb.PinPurpose, scopeKey string) error
```

Semantics, all frozen by the contract:

- **Limits.** First call that needs them fetches `GetStoreLimits` once and caches under a `sync.Once`-style guard (retry on failure — a failed fetch does not poison the cache). All framing decisions (`max_inline_bytes`, `max_chunk_bytes`, `max_batch_refs`) come from the cached limits; no magic numbers.
- **Put.** `len(payload) ≤ max_inline_bytes` → `PutPayloadInline`; larger → `PutPayload` client stream: one header frame `(DigestBytes(payload), len)` then sequential chunks of at most `max_chunk_bytes`. `code != OK` maps to a terminal error carrying the code and message; `PUT_CODE_COMMITMENT_MISMATCH` is a caller spool bug and is never auto-retried with the same bytes. On `OK`, `result.payload_ref != expectedRef` fails closed (decision 7). `deduplicated` is logged, not surfaced.
- **GetPayload.** One `FetchPayloads` call with a single whole-payload spec. Reassembly enforces the frame grammar: `Begin(spec 0)` → contiguous `Data` (offset must equal bytes received so far) → exactly one `End`. `code == OK` returns the buffer; `NOT_FOUND`/`RELEASED` return an error tagged as an availability incident (the store broke a hold it should have honored — escalation semantics per the contract); `PENDING` retries in-call with short backoff (3 attempts, 200ms doubling) then errors; `OFFSET_OUT_OF_RANGE`/`INTERNAL` error with the message. A truncated stream (missing `End`) or grammar violation is a transport-class error; the caller's next attempt re-fetches from zero (decision 9).
- **Pin.** Splits `refs` into `max_batch_refs` batches; each batch is one `PinPayloads` with `PinKey{holder_id: "arbiter", purpose, scope_key}`. Any per-ref `NOT_FOUND`/`RELEASED` fails the call with a custody-chain-broken error (escalation, not retry-and-hope — though the orchestrator's warn-loop will naturally retry next pass). Empty `refs` is a no-op returning nil.
- **Release.** One `ReleasePins` with the same key shape and an empty `authority_jws` (decision 5). `UNAUTHORIZED` errors; `OK` (including release-of-unknown) succeeds.
- **holder_id** is the constant `"arbiter"` — cluster-stable per the contract, so a failed-over leader re-asserts its predecessor's pins instead of orphaning them.

## 4. SNode seam

`snode.Deps.Payloads` changes from `*fspayload.Store` to:

```go
type PayloadSpool interface {
    Put(ctx context.Context, ref string, payload []byte) error
}
```

`fspayload.Store.Put` gains the `ctx` parameter (ignored) to satisfy it; `dastore.Client.Put` satisfies it directly with `ref` as `expectedRef`. The intake sequence is unchanged — `validatePayloadBinding` (envelope hash/length check) → `Payloads.Put` → decode → unsafe write — so payload-before-write holds: a put failure produces no `hg_unsafe` part, and the local hash check runs before any bytes leave the process, meaning a `COMMITMENT_MISMATCH` from the store indicates transport corruption or a store fault, not a silent bad payload.

## 5. Verifier seam

Zero changes to the `verifier` package. `verifier.NewReplayCore`'s fourth parameter is already `replay.PayloadStore`; `cmd/arbiter-verifier` constructs either `fspayload.New(dir)` or `dastore.New(cfg)` from config and passes it through. Fetch errors surface through the existing `replay.Verifier` pre-receipt refusal path: the verifier declines to attest, the orchestrator sees incomplete evidence and re-dispatches, and a persistent availability incident becomes a visible warn-loop (same operational class as an on-chain anchor conflict).

## 6. Arbiter custody wiring

### 6.1 Ingress (SEQUENCED)

`server.Deps` gains an optional `Custody` (narrow interface over `Pin`; nil ⇒ feature off). In `SubmitStatement`: after a successful propose with `submit.Code == ACCEPTED` and `env.PayloadRef != ""`, call `Pin(SEQUENCED, decimal(submit.StatementSeq), [env.PayloadRef])` with the request context; on error, log and return gRPC `Unavailable` — the statement stays sequenced in the log, the client retries, and the retry lands in the duplicate path. `reackDuplicate` performs the same pin (using the stored seq) before returning the stored ack, with the same failure behavior. Both paths therefore satisfy the contract's "no ack outruns the pin" rule.

### 6.2 Orchestrator (REPLAY / AUDIT)

The FSM read facade (P1b's detached-view layer — no Apply, snapshot, or wire change) gains:

```go
type CustodyWork struct {
    BlockSeq         uint64
    Refs             []string // deduplicated payload refs of the block's statements; may be empty
    StatementSeqs    []uint64
    EvidenceComplete bool     // all VerifierSet members have both attestation and scan
    Safe             bool     // safe watermark ≥ this block
    Rejected         bool     // challenge resolved Rejected — block will never become Safe
}
func (f *FSM) CustodyWorkSet() []CustodyWork // ascending BlockSeq, sealed blocks only
```

derived entirely from existing replicated state (sealed block headers + statements' `PayloadRef`/`StatementSeq`, the evidence maps behind `BlockDispatchInfo`, the safe watermark, and challenge state). `orchestrator.Deps` gains the same optional `Custody` interface (adding `Release`); when nil the custody step is skipped. `rescan` runs `advanceCustody` after `handleWorkSet`, keeping leader-term memory `custodyStage map[uint64]stage` (`stageNone → stageReplayPinned → stageAuditDone`):

1. Stage < replay-pinned: `Pin(REPLAY, block_seq, refs)`, then `Release(SEQUENCED, seq)` for each statement seq; `stageReplayPinned` is marked only when the pin **and every release** succeeded, so any failure re-runs the whole step next pass (the re-pin is a no-op) instead of stranding a hold. Pin-before-release ordering within the step preserves successor-before-predecessor.
2. Stage < audit-done and `Safe && EvidenceComplete`: `Pin(AUDIT, block_seq, refs)`, then `Release(REPLAY, block_seq)`; mark `stageAuditDone`.
3. Blocks with empty `Refs` skip RPCs and advance stages directly; `Rejected` blocks stop at `stageReplayPinned` by design — their payloads stay REPLAY-pinned as challenge evidence until P3 defines disposal.

Ordering inside one pass runs step 1 before step 2 for the same block, so a freshly elected leader that finds an already-Safe block still asserts REPLAY before AUDIT — successor-before-predecessor holds pass-locally, and cross-failover the store's idempotency absorbs duplicate asserts. Errors go through the existing `handleLoopError` warn-and-continue path.

### 6.3 Recorded v1 edges (conservative direction only)

(a) If seal + REPLAY-pin + SEQUENCED-release win the race against the ingress pin (apply → seal can be faster than the handler's pin call), the late ingress pin creates a dangling SEQUENCED hold that nothing releases — bytes are over-retained, never under-retained. (b) A duplicate re-ack arriving after the block is already audit-done re-asserts a SEQUENCED pin nothing will release — same direction. (c) `custodyStage` is leader-term memory, so each new leader replays idempotent asserts across all sealed history; bounded in v1 deployments, and P3's audit frontier will bound it structurally. All three are recorded for P3 cleanup alongside the audit window.

## 7. Config surface

```yaml
# cmd/arbiter — optional; absent ⇒ Custody nil ⇒ no pins (today's behavior)
payload_store:
  data_addr: "127.0.0.1:9001"      # PayloadStore — needed for GetStoreLimits (pin batch sizing)
  control_addr: "127.0.0.1:9002"   # PayloadLifecycle — pins/releases

# cmd/arbiter-snode / cmd/arbiter-verifier — backend selection, default fs
payload_store:
  backend: grpc            # fs (default) | grpc
  data_addr: "127.0.0.1:9001"
```

Validation: `backend` allowlisted to `[fs grpc]`; `grpc` requires `data_addr`; `fs` keeps requiring `payload_dir` (existing rule, now scoped to the fs backend); `cmd/arbiter` requires both `data_addr` and `control_addr` when the block is present — `GetStoreLimits` lives on the PayloadStore service (data plane), so pin batch sizing needs the data endpoint even though pins themselves go to the control endpoint (dev mode serves both on one address). Sample configs gain commented-out grpc examples; the k8s smoke-test forward (`kubectl --context sentio-sea -n da-test port-forward svc/da-store 9001:9001 9002:9002`) is documented in the README.

## 8. Error-handling summary

| Site | Failure | Behavior |
|---|---|---|
| SNode put | terminal store code / ref mismatch | `SubmitLocalStatement` errors before any unsafe write (payload-before-write) |
| SNode put | transport error | error to caller; caller retries (puts are content-idempotent) |
| Verifier fetch | `NOT_FOUND`/`RELEASED` on a pinned block | error → verifier refuses to attest → orchestrator re-dispatch warn-loop (availability incident) |
| Verifier fetch | `PENDING` | in-call bounded backoff, then error (retry next dispatch) |
| Ingress pin | any error | `Unavailable`, no ack; client retry → duplicate re-pin → re-ack |
| Orchestrator pin/release | any error | warn via `handleLoopError`, stage not advanced, retried next rescan |
| Limits fetch | error | that call errors; next call re-fetches (no poisoned cache) |

## 9. Testing

- **Unit (bufconn fake store).** A test-only in-process gRPC store implementing both services with injectable per-RPC codes and a call recorder. Covers: inline/chunked selection at the exact `max_inline_bytes` boundary, chunk slicing at `max_chunk_bytes`, ref-mismatch fail-closed, dedupe-hit success, fetch frame reassembly (grammar violations, contiguity check, each FetchCode, PENDING backoff), pin batch splitting at `max_batch_refs`, empty-refs no-op, release idempotency; ingress ordering (pin-before-ack via recorder, pin-failure ⇒ `Unavailable` and no ack, duplicate re-pin-before-re-ack, non-ACCEPTED never pins, nil Custody bypass); orchestrator custody progression (REPLAY-then-SEQUENCED-release order, AUDIT gated on `Safe ∧ EvidenceComplete`, AUDIT-then-REPLAY-release order, Rejected blocks stop at REPLAY, second rescan is RPC-silent via stage memory, a fresh Loop re-asserts idempotently, pin failure leaves stage unadvanced).
- **Integration (chpipeline).** A real `da-store --dev` instance (memory backends, one shared listener) gated like ClickHouse: `ARBITER_DA_INTEGRATION=1` plus `DA_STORE_BIN` (spawn) or `DA_STORE_ADDR` (attach). The harness swaps the shared `fspayload` instance for per-role `dastore` clients — retiring the shared-filesystem fiction — and the existing pipeline + fraud tests run unchanged on top; one added assertion path checks pins advance to AUDIT after promotion/manifest publication. The fs-backend path keeps its current coverage. CI gains a da-store container/binary step in the integration job (self-hosted runner already runs docker).
- **Conformance.** A test asserting the client never reads any hash/length assertion from store responses (hash-silence is structural: the generated types carry none — the test pins that assumption against future proto drift by checking the response descriptors).
- **Manual.** README smoke-test recipe against the k8s `da-test` deployment via the port-forward above.

## 10. Recorded follow-ups (not this phase)

P1e HouseGate ingress client reusing `dastore.Client` for its put leg (unchanged from P1c/P1d records; unblocked by this phase). AUDIT-pin release once P3 defines the audit window, plus disposal policy for Rejected blocks' REPLAY pins and the §6.3 dangling-SEQUENCED cleanups. Signed `ReleasePins` (`release_seq` sourcing, per-authority watermark) when da-store enables `--enforcement=on`. Fetch range resume and batch prefetch if payload sizes or block widths ever make whole-payload re-fetch or per-ref fan-out measurable. Custody stage persistence/bounding via the P3 audit frontier. Repo-wide metrics (pin/fetch counters ride that follow-up).
