# HouseGate Storage Integrity: PR19 — SafeAudit Serving Evidence

Date: 2026-07-21
Branch: `feat/housegate-sentio-pr19-25-safeaudit-quarantine`
Design: `docs/superpowers/specs/2026-07-01-sentio-sequencer-storage-integrity-insert-bounded-update-delete-design.zh-CN.md` §5.1

## 1. Scope

SafeAudit is the post-publication serving audit (design §5.1): after promotion/manifest publication (ACK3), an auditor validates each worker's local `hg_safe` active set against the frozen manifest scope, then recomputes the same-semantic audit hash over the manifest-covered parts and votes. It is **not** the pre-promotion byte-side check and **not** an ACK3 precondition. This PR adds the pure auditor logic (task, local-evidence verification, vote signing/verification, deterministic decision derivation) plus the gated worker that would drive it end to end.

## 2. Blocked-skeleton status

The companion **C3** SafeAudit seam (`SubmitAuditVote` / `AuditVote` / `ServingEvidence`) is **absent** from the Sentio arbiter (03aa035) and arbiter-proto (2fa9263) — verified by grep, no such RPC/message exists. So the end-to-end path (real post-ACK3 readback against `hg_safe`, real vote submission to the Arbiter FSM) cannot run. This PR ships:

- **Pure, green-today logic** — `AuditTask`, `ComputeAuditHash`, `VerifyLocalActiveSet`, `SignAuditVote`, `VerifyAuditVoteSignature`, `DeriveAuditDecision`. These are exercised by real unit tests.
- **A gated worker** — `SafeAuditWorker` with `SafeAuditReadbackPort` / `SafeAuditVoteSubmitter` ports whose only real implementation needs C3. `RunAudit` fails closed while `CompanionMutationConsensusAvailable == false`. The end-to-end tests `t.Skip` via `requireCompanionMutationConsensus`.

No Arbiter/SNode/SafeAudit protocol is fabricated.

## 3. Auditor logic (`safe_auditor.go`)

1. **`AuditTask`** fixes the frozen snapshot id, the expected active parts (`[]replay.PartManifestEntry`), and the participant set (design §5.1 step 1). `Valid()` fails closed on a blank snapshot, empty expected set, or fewer participants than the serving floor (a task that could never reach a majority is not auditable).
2. **`VerifyLocalActiveSet`** is the fail-closed local check a worker runs before it may vote Pass (design §5.1 step 4): the local readback must exactly equal the expected active set (part names + count), every part's metadata (row count / bytes) must match, every part's checksum (`PartRowLtHash`) must match, and the worker's recomputed row-hash must equal the expected part's row-lthash. Any mismatch returns `Fail` with a typed reason (`AuditActiveSetMismatch` / `AuditPartMetadataMismatch` / `AuditChecksumMismatch` / `AuditRowHashMismatch` / `AuditReadbackMissing`). A `Fail` can never be coerced to `Pass`.
3. **`ComputeAuditHash`** folds the manifest-covered parts into an order-insensitive canonical digest (`safe-audit-hash-v1`). This is the *same semantic* hash a vote is defined on. The `PartLtHashCache` (PR23) may pre-check but must not change this hash (design §5.1 step 5) — the function is cache-independent.
4. **`SignAuditVote`** runs `VerifyLocalActiveSet` first and refuses to sign a Pass on any mismatch; on a mismatch it emits a signed `Fail` (a real, non-repudiable disagreement), never a forged Pass. Votes are signed via `Ed25519ClaimSigner` over `safe-audit-vote-v1`, exactly like mutation claims. `VerifyAuditVoteSignature` is the recompute+verify mirror.
5. **`DeriveAuditDecision`** is the deterministic derivation the Arbiter FSM applies over signed votes alone (design §5.1 step 3 + the FSM note: it records signed votes and derives decision/quarantine, reading no row data). Counting Pass votes that agree on the *same* audit hash: all participants agreeing ⇒ `Pass`; a majority (≥ floor) agreeing ⇒ `PassWithQuarantine`, quarantining every non-agreeing participant (Fail, wrong hash, or timeout); otherwise ⇒ `Failed`. The result is independent of vote order and of duplicate per-worker votes.

## 4. Gated worker (`safe_audit_worker.go`)

`SafeAuditWorker` holds the two gated ports and the worker signer. `RunAudit` reads the local active set, builds a fail-closed signed vote, and submits it — but fails closed while C3 is absent. It holds no Arbiter FSM/decision state; the FSM derives the decision from the signed votes.

## 5. Reused types (never redefined)

`replay.PartManifestEntry`, `replay.CanonicalDigest`, `CandidatePart` (the CH-driver-free local readback shape), `Ed25519ClaimSigner`, `MutationServingAvailabilityFloor`, `CompanionMutationConsensusAvailable`, `requireCompanionMutationConsensus`.

## 6. Tests

- **Pure (green today):** `AuditTask.Valid` matrix; `ComputeAuditHash` determinism / order-insensitivity / content-sensitivity / cross-worker recompute; `VerifyLocalActiveSet` pass + every typed fail; `SignAuditVote` refuses a Pass on mismatch; `VerifyAuditVoteSignature` rejects tamper/wrong-key; `DeriveAuditDecision` 3/3-pass, 2/3-quarantine-minority, timeout, no-majority, disjoint-hash, order-independence, non-participant-ignored.
- **Gated (skip-closed):** real post-ACK3 readback, FSM vote submission, minority-quarantine-into-read-set (ACK3 not retroactively revoked) — all `t.Skip` then `t.Fatal("unreachable ...")`.
