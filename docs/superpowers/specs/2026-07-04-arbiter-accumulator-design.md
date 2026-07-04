# Sentio Arbiter — statement_id Uniqueness Accumulator (P0b construction freeze)

**Date:** 2026-07-04 **Status:** Proposed (v1) **Base:** [2026-06-30 Sentio Arbiter design](2026-06-30-sentio-arbiter-design.md) §6 + [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) §7/§14 + [2026-06-10 multi-replica trust design](2026-06-10-multi-replica-trust-design.md) Appendix B.2. **Source of truth:** English version.

This document freezes the byte-exact construction of the `spent_ids_root` accumulator — the base-spec §14 P0 deliverable ("accumulator construction + test vectors") and arbiter-design Open Question 1. It lands as the `accumulator` package in `github.com/sentioxyz/arbiter`, implementing (an amended version of) the §3.4 seam frozen in P0a.

## 1. Scope and requirements recap

The accumulator commits the set of spent `(client_account, client_seq)` coordinates (the uniqueness key of §6.1; `client_nonce` is entropy, not key material). Frozen requirements from the base specs: per-account-global scope, permanent retention (a coordinate once committed is never removed), no trusted setup, deterministically replayable from the L3 stream, `spent_ids_root_after` committed per L3 block, efficient non-membership proofs, and an O(1)-amortized proof-free fast path for well-behaved (strictly increasing `client_seq`) traffic via per-account high-water marks.

Inputs fixed by brainstorm: v1 scale envelope is **few accounts (≤ tens of thousands), many statements (up to 10⁸-level cumulative)**; near-term there is only the Go implementation (vectors guard drift and keep the door open, they do not serve a live second implementation).

## 2. Construction decision: account-granularity authenticated dictionary over an SMT

**Key insight.** Because the uniqueness key is `(account, seq)` with `seq` a per-account counter, each account's spent set is exactly and compactly described by `(hi_seq, open gap ranges)` — a bounded description independent of statement count. Both base specs already require the FSM to hold `HiSeq[account]` + a gap set for admission; the accumulator therefore does not need to commit statement-granularity leaves at all — it authenticates precisely the per-account state the FSM must hold anyway.

**Construction.** `spent_ids_root` is the root of a **hash-keyed sparse Merkle tree (SMT)** with one leaf per account that has ever spent a coordinate. The leaf value commits the account's full spent-state `(hi_seq, gap ranges)`. Account absence (nothing ever spent) is the SMT default-empty path — non-membership for unseen accounts is the standard SMT exclusion proof, with no sorted-neighbor/predecessor machinery.

**Why this deviates from the arbiter-design §6.2 recommendation, and why that is sound.** §6.2 recommends a statement-granularity "mountain-range / sorted-indexed-Merkle" accumulator; base-spec B.2 explicitly leaves the exact construction open as a P0 decision and lists sparse Merkle as acceptable. At the v1 scale envelope, statement-granularity trees fail both ways: an FSM-held tree is O(statements) (tens of GB at 10⁸ leaves — kills FSM snapshots), and a stateless FSM requires a ~1KB insertion witness carried in **every** `SubmitStatement` command (inflating the Raft log by ~100GB at 10⁸ statements, serializing witness generation, and breaking the §6.3 "fast path needs no proof" promise). The account-granularity SMT dissolves the tension between arbiter-design §6.4 (compact FSM authenticator) and §6.3 (proof-free `Insert` inside `Apply`): the FSM holds the whole tree at O(accounts) (a few MB at 10⁴ accounts, ≤ a few hundred MB even at a 10⁶-account stress case), `Insert` is witness-free, commands carry nothing, and external proofs stay ~0.5KB. The "larger constants" objection to sparse Merkle applies to statement-granularity 256-bit paths in proof-heavy settings; at account granularity with FSM-held state, proofs are rare (audit/decentralized-verifier path only) and the same order of size as a sorted-tree proof.

**Properties.** The root is a pure function of the spent **set** (insertion-order independent — strictly stronger than the required "replayable from L3 order"). Set-append-only holds semantically: `hi` only advances, ranges only shrink or split toward smaller total gap mass; a spent coordinate can never leave the set. No trusted setup. Proof size ≈ 32B bitmap + ~log₂(accounts) siblings + (for present accounts) the ≤1KB leaf preimage.

## 3. Byte-encoding freeze (profile `sentio-spent-ids-v1`)

All hashing is **BLAKE3-256** over `domain-ASCII ‖ 0x00 ‖ payload` (NUL-separated domain framing, mirroring the `canonicalDigest` framing style; plain BLAKE3, no keyed/derive-key modes, so any implementation with a standard BLAKE3 library can reproduce it). All integers are **big-endian**. The profile constants live in one Go constant block; see §8 for governance.

```text
domainKey  = "sentio-spent-ids-v1:key"
domainLeaf = "sentio-spent-ids-v1:leaf"
domainNode = "sentio-spent-ids-v1:node"

key(account)   = BLAKE3-256(domainKey  ‖ 0x00 ‖ account_bytes)
leafval(state) = BLAKE3-256(domainLeaf ‖ 0x00 ‖ len(account) u16 ‖ account_bytes
                            ‖ hi_seq u64 ‖ n_ranges u32 ‖ ranges…)
                 where each range = start u64 ‖ end u64
node(l, r)     = BLAKE3-256(domainNode ‖ 0x00 ‖ l(32B) ‖ r(32B))

empty leaf     = 32 × 0x00
D[0] = empty leaf;  D[k+1] = node(D[k], D[k]);  EmptyRoot = D[256]
```

`account_bytes` is the account string in its canonical lowercase form — byte-identical to the account component of `StatementIDString` (P0a): for Ethereum accounts, lowercase 0x-prefixed hex. Admission normalizes before the accumulator sees it; the accumulator treats the string as opaque bytes.

**Tree shape.** Depth-256 binary SMT addressed by `key(account)` bits MSB-first: the root branches on bit 0 (the most significant bit of `key[0]`, 0 = left), each level consumes the next bit, the leaf sits at level 0 (leaf) / 256 (root) counting leaf-up. Equivalently, when folding bottom-up, the position bit at level ℓ is key bit `255 − ℓ` (bit i = bit `7 − (i mod 8)` of byte `i / 8`). Setting a leaf replaces the empty leaf at the key's position with `leafval`; leaves are never deleted (spent state never empties once `hi ≥ 1`). The in-memory representation is free (the reference implementation uses a compressed binary trie with the `D[k]` table for empty subtrees); only the root derivation above is normative.

**Canonical state form (enforced before hashing; non-canonical encodings are invalid).** Gap ranges: sorted ascending by `start`; pairwise disjoint; **non-adjacent** (`end_i + 1 < start_{i+1}` — adjacent ranges must be merged); each within `[1, hi_seq − 1]`; `start ≤ end`. `hi_seq ≥ 1` for every existing leaf; the state `hi = 0 ∧ no ranges` is by definition the absent leaf, so the canonical form is unique. **`client_seq = 0` is invalid** and rejected at admission (a strictly-increasing counter starts at 1); this is what makes `hi = 0` unambiguous.

**Spent-set semantics.** `spent(account) = [1, hi_seq] \ ⋃ ranges`. `hi_seq` itself is always spent. A coordinate `(account, seq)` is unspent iff the account leaf is absent, or `seq > hi_seq`, or `seq` lies inside some gap range.

## 4. State-transition rules (the admission-facing mutation semantics)

`Insert(account, seq)` (deterministic, runs inside `Apply` with no witness):

```text
seq == 0                    → reject SeqZero
leaf absent (hi = 0):
  seq == 1                  → create leaf, hi = 1, no ranges
  seq >  1                  → create leaf, hi = seq, ranges = {[1, seq−1]}   (count check)
leaf present:
  seq >  hi:
    seq == hi + 1           → hi = seq
    seq >  hi + 1           → ranges += [hi+1, seq−1]; hi = seq              (count check)
  seq <= hi:
    seq ∈ some range [a,b]:
      a == b                → remove range (count −1)
      seq == a              → range becomes [a+1, b]
      seq == b              → range becomes [a, b−1]
      a < seq < b           → split into [a, seq−1], [seq+1, b] (count +1)   (count check)
    else                    → reject SpentDuplicate
(count check): if the resulting open-range count would exceed K → reject GapBudgetExceeded,
               leaving state unchanged.
```

**Gap budget K = 64 (frozen in the profile).** One uniform rule — any operation whose result would hold more than K open ranges for the account is rejected — covers both fast-path jumps (each adds one range) and mid-range gap-fills (each split adds one). Without the fill-side check, an attacker could jump once then fill every other seq, inflating one account to O(span) ranges; K bounds both FSM state and proof size (leaf preimage ≤ 64 × 16B = 1KB). A rejected client always has a remedy: filling from a range's **edges** never increases the count. Forward jumps are otherwise unbounded (a jump of any span costs one range; burning seq space is the client's own choice). The rejection is a distinct admission outcome — arbiter-proto gains `ADMISSION_CODE_GAP_BUDGET_EXCEEDED` (compatible enum append, ships with v0.2.0 in P1a; recorded here, not built in P0b).

## 5. Non-membership proof (external verifiers / challenge audit; the FSM does not consume proofs)

Because the FSM holds the full dictionary, `Apply` decides admission from its own committed state — the P0a `SubmitStatementCmd.non_membership_proof` field stays reserved and **always empty in v1**; it exists for the decentralized phase where an external party proves non-membership against a published root. Proofs are produced by `ProveNonMembership` (any node holding the dictionary) and verified by the package-level pure function `Verify(root, coord, proof) bool`.

Deterministic hand-rolled binary encoding (no proto dependency in the crypto layer):

```text
proof := ver(u8 = 1) ‖ kind(u8: 1 = account absent, 2 = account present)
       ‖ pathBitmap(32B)                       # bit ℓ set ⇒ a sibling is supplied for level ℓ; else sibling = D[ℓ]
       ‖ nSiblings(u16) ‖ siblings(32B each)   # bottom-up order (level 0 … 255), only for set bits
       ‖ [kind = 2: len(account) u16 ‖ account_bytes ‖ hi u64 ‖ n_ranges u32 ‖ ranges…]
```

Bitmap bit ℓ (level ℓ, leaf-up) is bit `7 − (ℓ mod 8)` of byte `ℓ / 8` (MSB-first). Verification: `kind = 1` folds from the empty leaf; `kind = 2` recomputes `leafval` from the carried preimage (rejecting non-canonical range lists), folds bottom-up choosing left/right by the key bit at each level, compares the computed root, then checks the coordinate is unspent under §3's semantics (`kind = 1`: always unspent; `kind = 2`: `seq > hi` or `seq` inside a carried range). Strict decoding: `nSiblings` must equal the bitmap's popcount, no trailing bytes, version must be 1; `kind = 2` requires the carried `account_bytes` to byte-equal the proof target's canonical account (the preimage IS the target's leaf, and the fold runs at `key(account_bytes)`'s position). Malformed or non-canonical proofs verify false.

## 6. Package API (amends the P0a §3.4 seam — documented breakage)

The P0a-frozen `Accumulator` interface predates this construction; two amendments, with rationale recorded here: `Insert` gains an `error` return (this construction rejects `SeqZero` / `SpentDuplicate` / `GapBudgetExceeded` at the accumulator layer), and admission/snapshot primitives are added for P1a.

```go
type SeqRange struct{ Start, End uint64 }

type Status uint8 // Fresh | SpentDuplicate | GapFillable | SeqZero

type Accumulator interface {
	Root() []byte                                      // 32-byte spent_ids_root (EmptyRoot when empty)
	Insert(c arbiter.StatementCoord) error             // §4 rules; state unchanged on error
	Status(c arbiter.StatementCoord) Status            // read-only admission query (P1a's §6.3 primitive)
	AccountState(account string) (hi uint64, ranges []SeqRange, ok bool)
	ProveNonMembership(c arbiter.StatementCoord) (Proof, error)
	VerifyNonMembership(c arbiter.StatementCoord, p Proof) bool // = Verify(Root(), c, p)
	Snapshot(w io.Writer) error                        // canonical dictionary dump (FSM snapshot integration)
	Restore(r io.Reader) error
}

func Verify(root []byte, c arbiter.StatementCoord, p Proof) bool // package-level pure verifier
```

**Snapshot format** (canonical, tree rebuilt on restore — the tree is derived state): `magic "SIDS" ‖ ver u8=1 ‖ count u64 ‖ entries…`, entries sorted ascending by `account_bytes`, each `len(account) u16 ‖ account_bytes ‖ hi u64 ‖ n_ranges u32 ‖ ranges…`. All integers big-endian. `Restore` rejects unsorted, duplicate-account, or non-canonical entries.

## 7. Single-hash-profile tripwire reconciliation (§13 of the arbiter design)

The accumulator is an **algorithmic authenticator** in the same class as `pkg/lthash` — it carries its own byte-precise profile, exactly as lthash carries `housegate-lthash-v1`. The §13 "one canonicalization profile" tripwire governs **evidence-comparison and manifest roots**, which must go through `replay.CanonicalDigest`; it does not (and cannot — JSON canonicalization of a Merkle tree's internal nodes is meaningless) govern authenticator internals. The boundary: `spent_ids_root` is produced by this profile as an opaque 32-byte scalar; the L3 block header that carries it (`SpentIDsRootAfter`) is hashed through `CanonicalDigest` when P1 seals headers. Tripwire restated precisely: evidence/manifest roots must not bypass `CanonicalDigest`; algorithmic authenticators (lthash, spent-ids) must each have exactly one frozen profile with published test vectors — a second profile for the same authenticator is the regression.

## 8. Profile governance (no runtime configurability)

The domain prefixes are consensus parameters, not configuration: any two participants disagreeing on the prefix bytes derive different roots from the same L3 stream — a silent fork. The profile (`sentio-spent-ids-v1`, the three domains, K = 64, and every encoding rule in §3–§6) is therefore **frozen at compile time** in a single constant block; there is deliberately no constructor parameter or config field for it. The prefix deliberately avoids the `housegate` keyword (product-neutral naming); the already-shipped `housegate-*` profiles (lthash, row-id, replay-mvp) are outside this document's scope — their bytes are user-invisible and stay as shipped, or migrate via a coordinated v2 in their own repos. Any future change to this profile (rebrand, multi-network deployment, algorithm upgrade) is a **new versioned profile** (`sentio-spent-ids-v2`) with new vectors and an explicit migration, mirroring the `ExecutorProfileID` versioning philosophy — never a knob.

## 9. Test vectors and testing

**JSON vectors** at `accumulator/testdata/spent_ids_vectors.json` (language-neutral; the byte spec's normative examples): (1) primitive vectors — `account → key` samples, leaf-preimage → `leafval` samples, `EmptyRoot`, a sampling of `D[k]`; (2) operation-sequence vectors — insert sequences with the expected root after each step, covering: new account with seq 1, fast path increments, a large jump (range creation), edge fills (both ends), a mid-range split, range-elimination fill, second account (two-leaf tree), inserts in permuted order reaching the same set (same final root); (3) proof vectors — hex-encoded proofs with `valid: true/false` for: absent account, present account with `seq > hi`, in-gap coordinate, spent coordinate (must fail), tampered sibling (must fail), non-canonical range list (must fail); (4) rejection vectors — op sequences ending in `SeqZero` / `SpentDuplicate` / `GapBudgetExceeded` with the expected unchanged root.

**Property tests** against a naive `map[string]map[uint64]bool` reference: arbitrary op sequences agree with the reference on accept/reject and membership; set-equal insertion orders produce equal roots; every generated proof verifies; every proof against a mutated root or coordinate fails; `Snapshot`/`Restore` round-trips the root; K-budget boundary behavior (63→64 accepted, 64→65 rejected, edge-fill at budget accepted).

**Fuzz**: Go native fuzzing over `Verify` inputs (arbitrary proof bytes must never panic and never verify true against a fixed honest root except for the honestly generated proofs).

## 10. Deliverable boundary

**P0b (this design + its plan):** the `accumulator` package (SMT, state rules, proofs, snapshot), the frozen profile constants, the JSON vectors + property/fuzz tests, and the P0a seam amendment. **Not P0b:** FSM wiring and the §6.3 admission composition (P1a — the `Status` primitive is ready for it), the `ADMISSION_CODE_GAP_BUDGET_EXCEEDED` enum append (arbiter-proto v0.2.0, P1a), leader-side proof serving RPC (decentralized phase), and any change to the shipped `housegate-*` profiles.

## 11. Resolved open items

- Arbiter-design Open Question 1 (byte encoding + vectors): resolved by §3, §5, §9.
- Base-spec B.2 P0 items: construction = account-granularity SMT (§2); `client_seq` width = **u64** (confirmed; already frozen in P0a proto/Go); gap-tolerance policy = K = 64 uniform open-range budget (§4).
- Arbiter-design §6.3/§6.4 tension (compact authenticator vs proof-free Insert): dissolved by construction (§2).

## 12. References

- [2026-06-30 Sentio Arbiter design](2026-06-30-sentio-arbiter-design.md) — §3.4 seam (amended by §6), §4.3 red lines, §6, §13
- [2026-06-22 storage integrity design](2026-06-22-storage-integrity-design.md) — §7, §14 P0 freeze deliverable
- [2026-06-10 multi-replica trust design](2026-06-10-multi-replica-trust-design.md) — Appendix B.2
- `github.com/sentioxyz/arbiter` — landing repo (`accumulator/` package, P0a seam at `accumulator/accumulator.go`)
- housegate `pkg/lthash` — the sibling algorithmic-authenticator profile precedent
