# PLUGINS GUIDE

## OVERVIEW
Each subdirectory is one plugin package plus its config and tests. `build.go` owns production ordering; plugins should stay leaf-level and avoid importing `pkg/proxy`.

## STRUCTURE
```
pkg/plugins/
|-- agent/          # agent-mode query signing and driver/payer settings
|-- auth/           # SQL-bound JWS auth
|-- commitgate/     # DDL/DCL observer gate
|-- concurrency/    # per-query/session quota acquisition and release
|-- credential/     # upstream credential replacement and __peer__ validation
|-- forward/        # session-level peer rebind on hello / USE
|-- indexing_usage/ # INSERT usage reporting
|-- lthash/         # raw ClientData LtHash MVP observer
|-- materialize/    # agent-side nondeterministic SQL materialization
|-- metrics/        # plugin-chain metrics adapter
|-- rewrite/        # per-session SQL rewriter plugin
|-- route/          # __route__ stripper and signer
|-- sessionstate/   # OnHello capture of the logical ClientHello database
|-- sireserved/     # privileged SI namespace/placeholder/object-carrier guard
|-- sistatement/    # agent-side signed SI INSERT lane and successful-USE tracking
|-- storageintegrity/ # server-side signed SI ingress and admission
`-- usage/          # query billing usage reporting
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Hook interfaces and filters | `../plugin/` | `RouteAware`, `PeerTrustAware`, `ForwardAware`, hook dispatch. |
| Production ordering | `../../build.go` | Add plugin wiring here after package-local code is ready. |
| New plugin template | `../../.claude/skills/add-housegate-plugin/SKILL.md` | Skim before adding to the chain. |
| SQL metadata source | `rewrite/`, `../../pkg/rewriter/` | Ordinary policy consumes `QueryContext`; the narrow text-inspection exceptions are listed below. |
| SI INSERT identity | `sistatement/`, `storageintegrity/`, `../storageintegrity/` | Agent and ingress share INSERT-target helpers; the agent also parses column lists, and only payload-local Native INSERTs are statement-signed. |
| Agent USE state | `sistatement/` | Strictly parses standalone USE candidates and commits the database only after upstream success; USE is not statement-signed and server ingress does not use this parser. |
| Other USE routing/state | `forward/`, `rewrite/`, `../../pkg/rewriter/` | `forward.matchUse` delegates to shared fail-open `ParseUseDatabase`; `sentioRewriter` uses backend classification plus a known-physical fallback to mirror state. |
| SI metadata cross-check | `storageintegrity/plugin.go` | Enabled ingress independently classifies INSERT/UPDATE/DELETE/ALTER/read-like text shapes and rejects backend metadata mismatches. |
| SI operator-bypass guard | `sireserved/` | Parser-free, fail-closed scan for reserved tokens, Identifier placeholders, and local-catalog/foreign-connector object carriers on maintenance/platform-operator sessions. Its lexical model covers every span a name can hide in — `'…'`, `` `…` ``, `"…"`, `--`, `#`, `#!`, `//`, nested `/* */` and ClickHouse heredocs (`$$…$$` / `$tag$…$tag$`) — and refuses a `$` that opens no well-formed heredoc. |
| DDL/DCL gates | `commitgate/` | Observers may abort with synthetic success. |
| Raw INSERT body observation | `lthash/` | Only registered `DataPlugin`; compressed blocks are out of MVP scope. |

## CONVENTIONS
- Default route behavior is skip; implement `RouteAware.RunOnRouted() == true` only for plugins that must run on routed proxy-to-proxy sessions.
- Default peer-trust behavior is run; auth, forward, rewrite, indexing usage, commitgate, and storage-integrity ingress opt out where applying them again would violate the ownership boundary. `IsForwardedFromPeer` explicitly overrides this filter because the receiving host then owns the original client SQL.
- Forwarded sessions keep origin-side plugins such as auth, usage, concurrency, sessionstate, credential, and metrics; rewrite, indexing usage, commitgate, and storage-integrity ingress opt out so the receiving host owns that work on its own session.
- In agent mode, enabled materialization runs before `sistatement`, which runs before the agent signer, so both statement and query tokens bind the same final SQL. Materializer construction is startup fail-fast, while individual plugin calls remain fail-open.
- For ordinary non-bypass queries, any configured SI table surface makes rewrite fail closed on unavailable classification or contract mismatch. Maintenance/platform sessions bypass rewrite; routed sessions, `remote()`-style peer-trusted sessions, and origin-side forwarding pivots filter it out. `IsForwardedFromPeer` is the explicit host-ownership override that keeps rewrite eligible on the receiving proxy. Only when `storage_integrity.ingress.enabled` is true is signed ingress wired after rewrite to validate and capture exact INSERT payloads; it follows the same routing/peer/forward ownership filters and no-ops unless rewrite metadata marks an SI table. With ingress disabled, ordinary SI INSERT is rejected during rewrite and the SI surface remains read-only.
- `sireserved` is the deliberate parser-free exception for maintenance/platform-operator sessions that bypass rewrite. It ignores comments but treats literals as identifier-bearing surfaces, rejects all backslash-bearing literals/quoted identifiers and `{name:Identifier}` placeholders independent of parameter transport, and rejects the known local-catalog and foreign-connector carrier callable family independent of arguments. Its span model includes ClickHouse heredocs: a heredoc body is blanked from the executable surface and written verbatim to the literal surface (blanking it from both would turn `merge($$hg_safe$$, …)`, refused today, into a bypass), heredoc bodies are exempt from the backslash refusal because ClickHouse applies no escape processing inside them, the tag charset is a deliberate strict subset of the grammar's, and a `$` opening no well-formed heredoc is refused rather than copied through (Spec N D1). It opts into forward/peer filters but returns immediately for ordinary sessions, including ordinary peers; preserve that D6 boundary.
- Plugin packages should expose narrow constructors and package-local config structs.
- Hook implementations should degrade consistently with the existing fail-open/fail-closed boundary for that plugin.

## ANTI-PATTERNS
- Do not import `pkg/proxy` from a plugin; use interfaces to avoid cycles.
- Do not re-parse general SQL in downstream plugins when rewrite metadata exists. Deliberate narrow correctness boundaries are the shared agent/ingress INSERT identity parser, agent `ParseUseDatabaseStrict`, forward's shared fail-open `ParseUseDatabase` wrapper, `sentioRewriter`'s backend-classified known-physical USE fallback, enabled ingress's INSERT/UPDATE/DELETE/ALTER/read-like metadata cross-check, and the conservative `sireserved` operator-bypass scanner. The optional lthash MVP separately uses a fail-open INSERT-target regex only to arm its observer. None rewrites SQL locally; do not turn `sireserved` into a partial ClickHouse parser or inspect carrier arguments—the full callable-name refusal exists because constant folding defeats token-level target proofs.
- Do not double-fire commitgate on routed, ordinary `remote()` peer-trusted, or origin-side forwarding traffic. `IsForwardedFromPeer` is the receiving-host override where commitgate must run once against the original client SQL.
- Do not add a plugin to `build.go` without tests proving marker-interface behavior if it participates in route, peer-trust, or forward paths.
- Do not hide new Redis requirements outside config validation.

## COMMANDS
```bash
bazel test //pkg/plugins/...:all
bazel test //pkg/proxy:proxy_test --test_filter='Test.*Plugin|Test.*Forward|Test.*Peer|Test.*Route'
bazel run //:gazelle
```

## NOTES
- `sessionstate` is OnHello-only today; relay does not call it on every query.
- `route` and `peer` envelopes share the `|` delimiter convention.
- `metrics` runs last for semantic forwarded-query counting; relay wire counters are separate.
