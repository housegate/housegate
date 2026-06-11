# rewriter-go Integration: Selectable In-Process SQL Rewriter Engine

**Date:** 2026-06-11
**Status:** Approved
**Repos touched:** [housegate/rewriter-go](https://github.com/housegate/rewriter-go) (prerequisite API addition), housegate/housegate (integration)

## 1. Goal

housegate's SQL rewriting currently requires an external gRPC `sql-rewriter` service
(the C++ `rewriter-grpc`). [rewriter-go](https://github.com/housegate/rewriter-go) is a
native Go implementation of the same contract, built on the polyglot Rust SQL engine
loaded via purego FFI (`CGO_ENABLED=0`). This design makes the rewriter engine
**selectable per deployment** via config:

```yaml
rewriter:
  engine: grpc            # grpc | native; default grpc (existing behavior unchanged)
  service_addr: ...        # required only when engine=grpc (unchanged)
  native_library_path: ""  # engine=native only; "" → POLYGLOT_SQL_FFI_PATH / system default
```

Everything downstream of the rewrite call — fail-open posture, `RewriteResult`
consumption by commitgate/observers, USE mirroring, remote() emission — behaves
identically under both engines because both speak the exact same protobuf contract.

## 2. Verified Facts (exploration findings this design rests on)

1. **The interfaces are already aligned.** rewriter-go's public `Rewriter` interface
   (`Rewrite(ctx, sql, effectiveAccount)` / `RewriteErrorMessage(ctx, message)` /
   `Close()`) matches `pkg/rewriter.Rewriter` signature-for-signature; the proto
   contracts are byte-identical except for the `go_package` option.
2. **Dual proto registration panics.** Both generated packages
   (`housegate/housegate/protos` and `github.com/housegate/rewriter-go/gen/pb`)
   declare proto `package rewriter;`. Linking both into one binary registers the same
   fully-qualified message names (`rewriter.RewriteSQLRequest`, …) twice in the
   protobuf global registry → init panic ("conflicting names"). Must be resolved, not
   worked around. Empirical confirmation is implementation step 0 (a sandbox
   restriction prevented running the probe during design; the conclusion follows from
   protobuf-go's documented registry semantics and the byte-identical proto packages).
3. **housegate's pb import surface is 5 files**: `pkg/rewriter/args.go`,
   `pkg/rewriter/sentio.go`, `pkg/integration/commitgate_test.go`,
   `pkg/integration/permission_isolation_test.go`,
   `pkg/integration/testenv/rewriter_mock.go`.
4. **rewriter-go cannot currently be constructed from outside its module.**
   `rewriter.New(e engine.Engine, …)` takes an `internal/engine` type; no exported
   function returns one. (`cmd/rewrite` compiles only because it lives in-module.)
   A public constructor in rewriter-go is therefore mandatory, not optional.
5. **`NativeRewriter.Close()` closes the engine and `RewriteErrorMessage` relies on a
   per-connection stash** — whereas housegate's `sentioRewriter` already calls the
   gRPC `RewriteErrorMessage` statelessly (sql + options travel in the request). The
   stateless request/response shape is the contract housegate consumes today.
6. **polyglot's Go bindings resolve cleanly for consumers.** Tag
   `packages/go/v0.5.1` exists upstream and points at commit `e3a8913a…` — the exact
   commit rewriter-go pins as its submodule. rewriter-go's `replace` directive (not
   inherited by consumers) is development-only; housegate resolves the identical code
   from the module proxy.
7. **The polyglot `Client` is safe for concurrent use.** Operations take
   `mu.RLock()` (client.go:455); `Close` takes the write lock. One shared engine per
   process is safe; no pool or serialization needed.
8. **Local checkout** of rewriter-go lives at `~/Dev/housegate/rewriter-go` (sibling
   of this repo); rewriter-go changes are made there and pushed before housegate pins
   the new commit.

## 3. Decisions

| # | Decision | Choice | Alternatives rejected |
|---|---|---|---|
| D1 | Integration architecture | **Stateless Service API in rewriter-go + a 2-method `backend` seam inside `SentioNetworkFactory`** | (a) per-connection `NativeRewriter` + second housegate Factory — duplicates response-trichotomy/USE-mirroring logic, two competing `RewriteErrorMessage` semantics, engine Close-ownership footgun; (b) in-process gRPC over bufconn — needs the same stateless service anyway, plus serialization and server ceremony for nothing |
| D2 | Proto dual-registration | **housegate adopts `github.com/housegate/rewriter-go/gen/pb` as its single pb package; `protos/` is deleted** | (a) rename rewriter-go's proto package — permanent contract fork, breaks its oracle harness (C++ service FQN) unless a second pb is kept; (b) `GOLANG_PROTOBUF_REGISTRATION_CONFLICT=warn` — global hack, undefined reflection-by-name behavior |
| D3 | Default engine | **`grpc`** — existing deployments unchanged; native is explicit opt-in | default `native` puts parity risk on the default path and degrades silently when the FFI lib is missing |
| D4 | Engine granularity | Process-global (one engine per proxy instance, chosen at startup) | per-session switching — YAGNI |
| D5 | FFI library distribution | Operator-provided at runtime (`native_library_path` config or `POLYGLOT_SQL_FFI_PATH` env) | building the Rust lib inside housegate's Bazel/CI/image — follow-up, out of scope |

## 4. Architecture

```
sentioRewriter.Rewrite / RewriteErrorMessage            ← unchanged
  → buildDatabaseMap / buildRemoteUpstreams / buildDynamicArgs   ← unchanged
  → factory.backend.Rewrite(ctx, *pb.RewriteSQLRequest)          ← the only seam that changes
       ├─ grpcBackend   : existing pb.RewriterServiceClient (dial logic moves, not rewritten)
       └─ nativeBackend : rewritergo.Service — in-process, no RPC
  → Success / UnsupportedStatement / reject trichotomy           ← unchanged
```

All session-aware logic — dynamic-args construction, auth-filtered database maps,
remote() upstream routing, USE mirroring into the session, proto→sqlmeta conversion,
fail-open error contract — stays single-sourced in `sentioRewriter` /
`SentioNetworkFactory` and is shared verbatim by both engines.

## 5. rewriter-go Changes (prerequisite, lands first)

New root-package file `service.go`:

```go
// Service is the stateless, process-shared entry point mirroring the
// rewriter-grpc service contract. Safe for concurrent use.
type Service struct { /* engine engine.Engine */ }

func NewService(libPath string) (*Service, error)
// libPath == "" → engine.NewPolyglot("") → polyglot.OpenDefault()
// (honors POLYGLOT_SQL_FFI_PATH and standard install dirs).

func (s *Service) Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error)
func (s *Service) RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error)
func (s *Service) Close() error   // Service owns the engine; Close releases it.
```

Internal refactor: extract the handler pipeline body of `NativeRewriter.Rewrite`
(parse → classify → existence-clause → writes → db-level → exists/show-create →
grant → select → pass-through, plus `finalize`) into an unexported
`doRewrite(e engine.Engine, sql string, opts []*pb.RewriteOption) (*pb.RewriteSQLResponse, error)`.

- `NativeRewriter` keeps its existing public behavior (options via the `WithOptions`
  callback, per-connection stash, `Close` closes the engine) — its tests and the
  differential harness are untouched.
- `Service.Rewrite` calls `doRewrite` with `req.GetOptions()` directly. No stash.
- `Service.RewriteErrorMessage` re-derives the forward maps by running `doRewrite`
  on `req.Sql` + `req.Options`, then applies `reverse.Invert`. When the re-run is
  non-Success, the message passes through unchanged (mirroring the existing
  non-Success passthrough and the C++ `doRewriteErrorMessage` semantics). Error
  paths are rare; the extra parse per exception is acceptable.
- Concurrency: engine ops are RLock-guarded; `Service` holds no mutable state.

Validation: a `service_test.go` that runs the existing golden corpus through both
`Service` and `NativeRewriter` and asserts field-identical responses (FFI-gated by
`POLYGLOT_SQL_FFI_PATH`, same skip pattern as the rest of the repo).

## 6. housegate Changes

### 6.1 pb unification (kills the dual-registration conflict)

- `go.mod`: add `github.com/housegate/rewriter-go` pinned to the commit containing
  the Service API (pseudo-version; tag later if desired). Transitively pulls
  `github.com/tobilg/polyglot/packages/go v0.5.1` + `github.com/ebitengine/purego`.
- Swap the import `pb "housegate/housegate/protos"` →
  `pb "github.com/housegate/rewriter-go/gen/pb"` in the 5 files listed in §2.3.
- **Delete `protos/`** (the `.proto`, the checked-in `rewriter.pb.go`, and the
  `proto_library` / `go_proto_library` Bazel targets). CLAUDE.md gains a pointer:
  the contract's source of truth is `rewriter-go/proto/rewriter.proto` (itself
  vendored from `rewriter-grpc`).
- `bazel mod tidy && bazel run //:gazelle`.
- Expected side effect: the "`go test ./...` panics in `protos/rewriter.pb.go` init"
  rough edge in CLAUDE.md disappears (verify, then update CLAUDE.md).

### 6.2 backend seam (`pkg/rewriter/sentio.go`)

```go
// backend abstracts the rewrite transport: remote gRPC service or
// in-process rewriter-go. Both implementations speak the same proto
// contract; sentioRewriter cannot tell them apart.
type backend interface {
    Rewrite(ctx context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error)
    RewriteErrorMessage(ctx context.Context, req *pb.RewriteErrorMessageRequest) (*pb.RewriteErrorMessageResponse, error)
    Close() error
}
```

- `grpcBackend`: today's dial + keepalive + `WithBlock` logic moves into
  `newGRPCBackend(opts)`; methods delegate to `pb.RewriterServiceClient`.
- native: `*rewritergo.Service` satisfies `backend` directly (signatures match by
  construction) — no adapter layer.
- `SentioNetworkFactory`: `grpcConn`/`grpcClient` fields replaced by one `backend`;
  `NewSentioNetworkFactory` branches on `Options.Engine` to construct it;
  `Factory.Close` → `backend.Close()`.
- `sentioRewriter`: exactly two call sites change
  (`r.factory.grpcClient.X` → `r.factory.backend.X`). Everything else untouched.
- Timeout: the existing per-call `context.WithTimeout` wraps both engines. Under
  native the FFI call cannot be interrupted mid-flight, but calls are local and
  fast; a comment documents this.

### 6.3 Config & wiring

- `pkg/plugins/rewrite.Config` gains `Engine string` (`grpc` | `native` | "" ≡ grpc;
  env `HOUSEGATE_REWRITER_ENGINE`) and `NativeLibraryPath string`.
- `rewriter.Options` gains the same two fields; `buildRewriterFactory` forwards them.
- `Config.Validate`: reject unknown `engine` values; `service_addr` is required only
  for the grpc engine (the existing `NewSentioNetworkFactory` check moves behind the
  branch).
- Failure posture is symmetric with today: native FFI-lib load failure at startup →
  warn + nil factory → rewriting disabled (fail-open), exactly like a failed gRPC
  dial. Startup log gains an `engine` field.
- Observability: the existing `Rewritten(duration)` histogram covers both engines
  unchanged; engine identity is visible in the startup log line.

## 7. Testing

| Layer | Coverage |
|---|---|
| rewriter-go unit | `service_test.go`: Service ≡ NativeRewriter on the golden corpus (FFI-gated) |
| housegate unit | config validation + engine selection (pure Go, no FFI). Existing `sentio_test.go` covers only pure helpers (`buildDatabaseMap`) and is untouched; the new `backend` interface additionally makes `sentioRewriter`'s request/response handling unit-testable with a fake backend for the first time (no gRPC server needed) — add such a test for the Success/Unsupported/reject trichotomy |
| housegate native smoke | a small test driving `NewSentioNetworkFactory(engine=native)` end-to-end through `Rewrite`; skips when `POLYGLOT_SQL_FFI_PATH` is unset → runs locally, auto-skips in CI |
| housegate integration | the existing docker-bound suite + `testenv/rewriter_mock.go` keeps covering the grpc engine; a native-engine integration target is a follow-up (blocked on FFI-lib distribution in CI) |
| regression baseline | `bazel test //...` diffed against a clean `main` run per CLAUDE.md convention |

## 8. Deployment Notes

- `engine: native` requires `libpolyglot_sql_ffi.{so,dylib}` present on the host /
  image, located via `native_library_path` or `POLYGLOT_SQL_FFI_PATH`. Build it once
  from the polyglot submodule (`make ffi` in rewriter-go) or distribute prebuilt.
- Rollout: default stays grpc → flip one deployment to native and observe → widen.
- The known rewriter-go limitation (GLOBAL JOIN with a `remote()` left operand
  cannot be synthesized through polyglot's generator) carries over to the native
  engine; deployments relying on that pattern should stay on grpc until it's fixed.

## 9. Out of Scope

- Shadow/dual-run comparison mode (parity is owned by rewriter-go's differential
  oracle harness).
- Per-session or per-query engine switching.
- Building the Rust FFI library inside housegate's Bazel build, CI, or production
  image (operator-provided artifact for now; image packaging is a follow-up).
- Any change to the rewrite plugin, commitgate, or other RewriteResult consumers —
  the contract above the Factory is untouched.

## 10. Implementation Order

1. **Step 0 (probe):** empirically confirm the dual-registration panic with a
   2-import scratch program (expected: panic; if it somehow doesn't, D2 is still
   right — one contract source beats two).
2. **rewriter-go:** `doRewrite` extraction + `Service` + `service_test.go`; push.
3. **housegate:** pb swap + `protos/` deletion + `bazel mod tidy` + gazelle; build green.
4. **housegate:** backend seam + config + `buildRewriterFactory` dispatch + tests.
5. **Verify:** `bazel test //...` vs `main` baseline; local native smoke with a
   locally built FFI lib; CLAUDE.md updates (rough-edge removal, module map, config docs).
