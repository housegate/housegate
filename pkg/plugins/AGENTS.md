# PLUGINS GUIDE

## OVERVIEW
Each subdirectory is one plugin package plus its config and tests. `build.go` owns production ordering; plugins should stay leaf-level and avoid importing `pkg/proxy`.

## STRUCTURE
```
pkg/plugins/
|-- agent/          # agent-mode query signing and driver/payer settings
|-- auth/           # SQL-bound JWS auth
|-- credential/     # upstream credential replacement and __peer__ validation
|-- forward/        # session-level peer rebind on hello / USE
|-- rewrite/        # per-session SQL rewriter plugin
|-- route/          # __route__ stripper and signer
|-- commitgate/     # DDL/DCL observer gate
|-- lthash/         # raw ClientData LtHash MVP observer
`-- metrics/        # plugin-chain metrics adapter
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Hook interfaces and filters | `../plugin/` | `RouteAware`, `PeerTrustAware`, `ForwardAware`, hook dispatch. |
| Production ordering | `../../build.go` | Add plugin wiring here after package-local code is ready. |
| New plugin template | `../../.claude/skills/add-housegate-plugin/SKILL.md` | Skim before adding to the chain. |
| SQL metadata source | `rewrite/`, `../../pkg/rewriter/` | Downstream plugins consume `QueryContext`, not SQL text. |
| DDL/DCL gates | `commitgate/` | Observers may abort with synthetic success. |
| Raw INSERT body observation | `lthash/` | Only registered `DataPlugin`; compressed blocks are out of MVP scope. |

## CONVENTIONS
- Default route behavior is skip; implement `RouteAware.RunOnRouted() == true` only for plugins that must run on routed proxy-to-proxy sessions.
- Default peer-trust behavior is run; implement `PeerTrustAware.RunOnPeerTrust() == false` for auth/rewrite/commitgate-style plugins that must not double-apply.
- Forwarded sessions keep origin-side plugins such as auth, usage, concurrency, sessionstate, credential, and metrics; host-side rewrite/commitgate opt out.
- Plugin packages should expose narrow constructors and package-local config structs.
- Hook implementations should degrade consistently with the existing fail-open/fail-closed boundary for that plugin.

## ANTI-PATTERNS
- Do not import `pkg/proxy` from a plugin; use interfaces to avoid cycles.
- Do not re-parse SQL in downstream plugins when rewrite metadata exists.
- Do not double-fire commitgate on routed or peer-trusted traffic.
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
