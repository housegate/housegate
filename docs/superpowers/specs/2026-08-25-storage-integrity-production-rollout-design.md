# Storage Integrity: Production Rollout of Specs I–L

**Date:** 2026-08-25 **Status:** Proposed **Roadmap:** [closure roadmap](2026-08-25-storage-integrity-closure-roadmap.md) Spec O. **Consumes:** [Spec N lexical/namespace closure](2026-08-25-storage-integrity-lexical-namespace-closure-design.md), and the landed [I](2026-08-19-storage-integrity-surface-failclosed-design.md) / [J](2026-08-19-storage-integrity-verification-restoration-design.md) / [K](2026-08-19-storage-integrity-commitment-durability-design.md) / [L](2026-08-19-storage-integrity-table-backpressure-hardening-design.md) work. **Code base:** housegate `6fd56b8` (v0.11.0) plus open PR #141, arbiter `c1d32f6` (v0.3.0), arbiter-core `32b59a8` (v0.5.1), arbiter-proto `19d90fc` (v0.6.0), sentio-node `58f5e5f`, rewriter-go `23687cc` (v0.9.0). **Source of truth:** English version.

## 1. Problem

Specs I–L are largely implemented and, where the 2026-08-25 review could test them empirically, they hold up. None of that work is reachable from production.

**1a — HouseGate `main` compiles and tests against the engine that still has the holes.** `go.mod:108` pins `github.com/housegate/rewriter-go v0.7.1` and `.github/workflows/ci.yml:111` fetches the FFI library with `--tag v0.7.1`. Spec I's engine half is in v0.8.0/v0.9.0. Every HouseGate unit and integration test that exercises the native engine therefore exercises the pre-Spec-I behaviour, and CI is green on it.

**1b — Spec I's HouseGate half is unmerged.** PR #141 (`feature/si-surface-failclosed-housegate`) carries D3, D5, D6 and D7e — the unconditional fail-closed on any non-`Success`, the startup engine probe, the recorded peer-trust exemption, and `pkg/plugins/sireserved`. It is `MERGEABLE`, all three required checks are `SUCCESS`, and it has been open since the remediation round. Until it merges, `pkg/rewriter/sentio.go:349` still returns `RewriteResult{SQL: sql}, nil` for `UnsupportedStatement` and `pkg/plugins/rewrite/rewriter.go:164` only rejects on a non-nil error, so an unmodelled statement is forwarded verbatim.

Together 1a and 1b mean **all three of Spec I's defence layers are absent from `main`**, and the two Critical statements Spec I exists to close are end-to-end live there. The engine's D2 deliberately leaves `original_accessed_tables` empty on these rejections (verified: every such case in the 210-case corpus has `want_accessed: null`), so `sentio.go:327`'s SI-flag-keyed gate is not a fallback — it cannot fire.

**1c — sentio-node, the production host, consumes none of it.** Its pins are `housegate v0.9.2` (ten commits and two minor versions behind, older than Spec J itself), `arbiter-core v0.4.0`, `arbiter-proto v0.5.0` (the only repo in the set not on v0.6.0). Consequences, each verified against the pinned versions rather than inferred:

- **Spec L D1 is dark.** `payloadexec.ValidateTableSchemaColumns` does not exist in v0.9.2, so §1a's permanent brick — a malformed column type creates extra columns, the schema drifts, there is no auto-`ALTER`, and the node cannot start — is still reachable in production.
- **Spec L D3 and D6 are dark.** The embedded proxy still runs the unbounded `system.parts` scan on every admission behind a hard-coded 2s timeout, and a code-252 refusal still tears down the client connection.
- **Spec K is entirely unconsumed**, including D4.
- **D2 compiles today only because `arbiter-core` v0.4.0 still exports the `ProtocolTables` field.** Moving to v0.5.x breaks the build. That is the good failure mode — loud, not silent — but it is unhandled work sitting in front of the bump.

**1d — two disagreeing mode derivations, and a test that pins the wrong one.** `standalone/standalone.go:810-819` errors on an unknown schema source; `storageintegrityadapter/adapter.go:39-44` silently returns `ddl.ModeVerifyOnly`. PR #179 switched production to the strict one but left the fail-open function in place, and `storage_integrity_bootstrap_test.go:241` still computes its expected value from the fail-open function — so the guard no longer checks the production predicate. It is a tautology that will stay green through a regression.

**1e — Spec K D8b was never implemented, and the prescription it was given is wrong.** `adapter.go:73` classifies only `ErrSchemaHashMismatch` and `ErrEncodingNotSupported` as terminal. `adapter_test.go:149` asserts `{snode.ErrPayloadMismatch, false}` — the test actively pins the defect, so CI is green *because* the behaviour is wrong. But the remediation plan's instruction to map `ErrPayloadMismatch` wholesale to `sicore.ErrPrepareTerminalReject` is unsound: in arbiter-core the sentinel spans two semantic classes. `snode/staged.go:122,130` raise it **before** `journal.save`, where no unsafe write can exist; `staged.go:242-251` raise it from `validateRecordedBindings`, which is only reached when a journal record already exists and a write may have happened. HouseGate treats terminal-reject as *provably no write* — `abortTerminalPrepareReject` hard-errors when `len(parts) != 0` — so a blanket mapping converts a convergent path into a permanently wedged retry loop. **"Not retryable" is not the same as "did not write."** The two classes are not separable by raise site, either — see D1.

**1f — Spec L D7's "explicit disable" cannot be expressed.** arbiter-core has no disable sentinel in v0.4.0 or v0.5.1: `snode/config.go:108-111` rewrites `HardPartsPerPartition == 0` to `DefaultHardPartsPerPartition` (2950) and rejects negatives. So `backpressure: {enabled: false, hard_parts_per_partition: 0}` passes both validators while the source still refuses at 2950. Spec L §1g survives verbatim.

**1g — three regressions that compile clean and then refuse to start.** Enumerated empirically while planning this spec (a scratch-copy `go get` + `go build` + `go vet`, not inference), and none of them predicted by the drafting of §1c. The two compile breaks are the easy half — `standalone/standalone.go:302`'s `snode.Config.ProtocolTables`, which became `SchemaSource ddl.SchemaSource` with the mode derived by `ddl.ModeFromSchemaSource`, and `standalone/storage_integrity_smoke_test.go:218`'s three-variable assignment against a `ddl.Intents` that now returns four (the `hg_promote` intent, Spec L D5). The silent half matters more:

- **The SI column-type whitelist is now enforced at `snode.New`.** `arbiter-core/snode/config.go:75` and `verifier/config.go:71` call `payloadexec.ValidateTableSchemaColumns` during config validation, and the whitelist (`pkg/replay/payloadexec/column_types.go:56-78`) is `String`, `FixedString(N)`, `Bool`, `Float32/64`, `UInt8…64`, `Int8…64` — **no `DateTime`, `Date`, `Decimal`, `UUID`, `Nullable(…)`, `Array(…)`, `LowCardinality(…)` or `Enum`.** The executor never supported those types, so this is a change of *timing*, not of capability: what used to fail at replay now fails at startup, which is the correct direction and exactly what Spec L D1 intended. But it means any deployment whose SI tables declare a `DateTime` — which is most real indexer tables — stops starting the moment it takes this bump. This is a product constraint on SI v1, not just a rollout step.
- **`hg_promote` is no longer auto-created** and is verified in both DDL modes.
- **`schema_source: ""` is rejected** by `ddl.ModeFromSchemaSource`, and sentio-node's config explicitly admits the empty value with a test pinning it.

## 2. Goals / non-goals

**Goals.** Get Specs I–N onto the production host, in an order where each step's failure is loud and local. Fix the two things that block the bump (the arbiter-core sentinel split, the disable sentinel), remove the fail-open duplicate, and re-point the test that pins the defect. Leave sentio-node with its first release tag so its commits are addressable.

**Non-goals.** The devnet2 pilot (Spec M) — it consumes the tags this spec produces. Any behaviour change in HouseGate, arbiter or arbiter-core beyond what is needed to make the bump correct; residual gaps go to Spec P.

## 3. Decisions

### D1 — arbiter-core splits `ErrPayloadMismatch` into two sentinels that both still match the old one

```go
// snode/staged.go
var ErrPayloadMismatch = errors.New("snode: payload does not match envelope")

// Raised only where no unsafe write can exist yet — before journal.save.
var ErrPayloadMismatchPreWrite = fmt.Errorf("%w (proved before any unsafe write)", ErrPayloadMismatch)

// Raised where a durable record already exists, so a write may have happened.
var ErrPayloadMismatchPostRecord = fmt.Errorf("%w (a durable record already exists)", ErrPayloadMismatch)
```

Because both wrap the original value, `errors.Is(err, ErrPayloadMismatch)` stays true for every existing caller — the change is additive and cannot silently break a consumer.

**The class is a property of the caller, not of the raise site.** An earlier draft of this decision keyed the split on line numbers (`staged.go:122,130,194,198,202` → pre-write; `:242,245,248,251` → post-record). That is unsound, and the plan for this spec caught it. `staged.go:189-202`'s `validatePrepareBindings` has two callers in different classes: `PrepareLocalStatement` at `:79`, which is genuinely pre-write, and `validateRecordedBindings` at `:212`, reached from `RegisterPreparedClaim` (`:342`), `:402`, and `converge.go:29` and `:239` — all of which run against a loaded journal record whose lifecycle may already be `UnsafeWritten` or `RCBound`. `snode/recorded_bindings_test.go` drives exactly those lines post-record. A line-keyed split would relabel definitely-written statements as pre-write and wedge them on HouseGate's `abortTerminalPrepareReject` hard error — the same failure mode this decision exists to avoid, arrived at from the opposite direction.

So `validatePrepareBindings` takes the class as a parameter (or returns an unwrapped classification its callers wrap), and each call site names its own class. `staged.go:122,130` stay pre-write; `:242,245,248,251` stay post-record; `:194,198,202` inherit whichever caller invoked them.

The classification rule is *"is this reachable after `journal.save` has succeeded for this statement id?"*, answered per call path. arbiter-core adds a test that walks each **call path** — not each raise line — and asserts its class, so a future caller cannot inherit the wrong one by copy-paste.

### D2 — arbiter-core gains an explicit hard-parts disable

`snode.Config` gains `DisableHardParts bool`. When true, `staged.go:150`'s check is skipped and a non-zero `HardPartsPerPartition` is a **validation error**, so a half-configured disable is impossible. `HardPartsPerPartition == 0` keeps defaulting to 2950 — the existing behaviour and the safe one for anyone who does not opt out. sentio-node maps HouseGate's `storage_integrity.backpressure.enabled: false` onto it, and refuses to start if the two disagree.

This is a deliberate footgun made explicit rather than removed: with it set, inserts fail at ClickHouse's own `parts_to_throw_insert` instead of at the source, which is the operator's call to make and now requires them to say so twice.

### D3 — sentio-node deletes the fail-open mode derivation and re-points its test

`storageintegrityadapter.ProtocolTablesMode` is deleted. The single remaining derivation is `standalone.go`'s, which errors on an unknown source; if the adapter needs it, it imports it rather than re-deriving. `storage_integrity_bootstrap_test.go:241` computes its expected value from the production function, so the assertion checks the production predicate. If deleting the export breaks an external consumer, the fix is to export the strict one under the same name — never to keep both.

### D4 — sentio-node implements K D8b against the split sentinel

`adapter.go:73` maps `ErrSchemaHashMismatch`, `ErrEncodingNotSupported` and `ErrPayloadMismatchPreWrite` to `sicore.ErrPrepareTerminalReject`. `ErrPayloadMismatchPostRecord` stays non-terminal and goes through the ordinary source-lookup path, because a write may exist and HouseGate's terminal-reject contract would hard-error on the recovered parts. `adapter_test.go`'s table gains a row per sentinel with the class stated, and the `{ErrPayloadMismatch, false}` row is **replaced**, not extended — the bare sentinel is no longer raised by any arbiter-core path once D1 lands, and a test asserting its classification would be asserting dead behaviour.

### D5 — the bump order is engine → HouseGate → host, one loud step at a time

1. rewriter-go **v0.9.1** (Spec N).
2. HouseGate: the v0.9.1 pin lands **on PR #141's branch**, not on `main` after the merge. #141 already carries the v0.7.1 → v0.9.0 bump (and polyglot v0.8.1 → v0.9.2), so a separate "bump after merge" step would conflict with itself. `go.mod` and `.github/workflows/ci.yml`'s FFI `--tag` move in the same commit — they are one pin expressed twice, and CLAUDE.md's dependency-upgrade recipe treats a split as the classic mistake. Then merge #141 and cut **v0.12.0**.
3. arbiter-core: D1 + D2, then cut a tag. **Do not name the version here.** arbiter-core's `next-version.sh` derives it from the UTC day with no bump override, and v0.5.1 was cut on 2026-08-25, so a same-day cut yields v0.5.2 regardless of what this spec says. The plan carries a `<arbiter-core-tag>` placeholder resolved at cut time.
4. arbiter: bump the housegate pin to v0.12.0.
5. sentio-node: bump `housegate` → v0.12.0, `arbiter-core` → the tag from step 3, `arbiter-proto` → v0.6.0 in one commit (the arbiter-proto step is a no-op at the code level — zero exported identifiers added or removed — but keeps the set consistent); fix the two §1g compile breaks and the three silent startup regressions; D3; D4. Cut its first tag.
6. **sentio-node has no release-cut workflow at all**, so "cut a tag" there is a manual `git tag` unless one is added. The plan states which, rather than assuming the mechanism exists.

Each step's verification command is named in the plan. A step that goes red stops the chain; nothing is bundled to "save a round trip".

### D6 — sentio-node gains the two acceptance tests Spec L §4 asked for and never got

- **Type rejection creates no table.** A malformed column type is refused by `payloadexec.ValidateTableSchemaColumns` *before* any DDL executes, and the target table does not exist afterwards. This is the direct test for §1a's permanent brick; it must fail against the v0.9.2 pin.
- **A 252 refusal leaves the connection usable.** After a back-pressure refusal, the same connection answers a subsequent `SELECT 42`. HouseGate has this as a docker e2e; sentio-node needs it against the embedded proxy, because the embedded path is what production runs.

Both are docker-bound and follow **sentio-node's existing CI pattern** — the env gate plus Spec J's anti-skip guards. An earlier draft said "explicit-target convention rather than env self-skip (see Spec P D6)", which contradicts Spec P D6: that decision keeps sentio-node's env gate and applies the explicit-target convention to arbiter-core and arbiter, which have no anti-skip guards at all. Spec P owns that change; this spec follows it.

## 4. Testing / acceptance

1. arbiter-core: the raise-site classification test; a config test proving `DisableHardParts` with a non-zero limit is rejected; the full suite green.
2. sentio-node: builds against the new pins; `ProtocolTablesMode` gone with no dangling references; the bootstrap test's expectation sourced from `standalone.go`; the D4 sentinel table; both D6 acceptance tests green under docker and red against the old pins.
3. HouseGate: `bazel test //...` green with the v0.9.1 FFI library actually fetched — the plan asserts the resolved library path, since a stale cache hit would silently keep the old engine.
4. End-to-end on the sentio-node stack: `SYSTEM START MERGES hg_unsafe.<t>` and `TRUNCATE DATABASE hg_safe` both refused; the Spec N heredoc statements refused; a normal SI INSERT and `unsafe_latest` read still work. **The acceptance must name its rewriter engine.** sentio-node contains no FFI references, so it runs the default `engine: grpc` and this acceptance actually proves a **rewriter-grpc** build — which no plan in this set pins to a version. Either pin the rewriter-grpc image explicitly, or configure `engine: native` with the v0.9.1 FFI library so the acceptance exercises the engine the rest of the chain just bumped. Running it against an unpinned service proves nothing about the tags this spec produces.
5. Tags exist and are fetchable: rewriter-go v0.9.1, housegate v0.12.0, arbiter-core v0.6.0, sentio-node v0.1.0.

## 5. Out of scope / recorded debt

- Spec M devnet2 readiness, which starts once these tags exist.
- The pre-existing committed plaintext ClickHouse password, Harbor robot password and sidecar private key in `production` — noted in the previous roadmap, still outside storage integrity's blast radius but inside Spec M's.
- Whether arbiter-core should reject `DisableHardParts` outright in a network that has an on-chain safety argument for the limit. Recorded, not decided.
