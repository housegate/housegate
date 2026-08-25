# REWRITER GUIDE

## OVERVIEW
`pkg/rewriter` is the shared SQL rewrite orchestration layer above two backend transports: external gRPC service and in-process `rewriter-go` native engine.

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Backend seam | `backend.go` | `grpc` default, `native` FFI-backed service, shared proto contract. |
| Per-session rewrite logic | `sentio.go` | Dynamic args, table/database maps, remote upstream credentials. |
| Test backend | `backend_test.go` | Unit tests should use `fakeBackend`, not a live service. |
| Native smoke | `native_smoke_test.go` | Skips unless `POLYGLOT_SQL_FFI_PATH` is set. |
| Plugin entry | `../plugins/rewrite/` | Query hook owns one `Rewriter` per connection and applies ordinary versus SI failure policy. |
| SI contract adapter | `storage_integrity.go`, `sentio.go` | Builds contract-v1 args, validates acknowledgements, and turns SI uncertainty into `RejectedError`. |
| SI startup conformance | `probe.go` | Bounded DESCRIBE/catch-all/protected-target suite; every concrete or injected SI factory must implement and pass it. |
| Agent materializer | `materialize.go`, `../plugins/materialize/` | Separate SQL materialization seam; startup and per-call failure policies differ. |
| Exception reverse mapping | `backend.go`, `sentio.go`, `../plugins/rewrite/` | With an active rewrite, `OnException` delegates through the per-connection rewriter to the selected backend's `RewriteErrorMessage`. |

## CONVENTIONS
- Do not add general SQL parsing here. The backend supplies `StatementType`, accessed tables, table/database rewrites, and privilege deltas. Shared strict agent/ingress identity parsing is limited to INSERT targets in `pkg/storageintegrity`; the agent additionally parses INSERT columns and calls `ParseUseDatabaseStrict` for post-success state, while `forward.matchUse` delegates to the shared fail-open `ParseUseDatabase` wrapper for session rebind. `sentioRewriter` retains a backend-classified known-physical USE fallback only to mirror state. Enabled ingress separately text-classifies INSERT/UPDATE/DELETE/ALTER/read-like shapes to reject metadata mismatches. None of these paths rewrites SQL locally.
- `Options.Engine == ""` means gRPC. Valid engines are `grpc` and `native`; config validation is mode-independent.
- Native library resolution order is explicit path, `POLYGLOT_SQL_FFI_PATH`, then default search locations.
- Native ctx deadlines are advisory because an in-flight FFI call cannot be interrupted.
- Without configured SI tables, backend startup failure preserves the warn-and-disable fail-open posture. With `storage_integrity.tables`, server construction instead requires an available factory that reports exact contract-v1 capability, implements `StorageIntegrityProbeFactory`, and passes the bounded Spec I behavioral suite. This requirement applies to caller-injected factories too; capability without a prober is an unverified startup and fails closed.
- For non-bypass queries that reach rewrite, ordinary call failures fail open only on the empty-SI surface. `RejectedError` always fails closed; with SI configured, transport/classification failures, missing or wrong contract acknowledgements, and SI-classified non-success responses propagate as QueryPlugin errors and client-visible ClickHouse Exceptions.
- The enabled agent materializer is built separately and is startup fail-fast, including native-library resolution. After successful startup, individual materialization call errors and non-success outcomes remain fail-open and leave the SQL unchanged.
- Cross-indexer `remote()` credentials go through `resolveRemoteCredentials`.

## ANTI-PATTERNS
- Do not revive in-repo generated proto packages. Import `github.com/housegate/rewriter-proto/gen/pb`.
- Do not make ordinary empty-SI rewrite backend unavailability crash startup, and do not relax the existing SI contract-v1, SI behavioral-probe, or enabled-materializer startup gates. The DESCRIBE fingerprint alone is insufficient because older gRPC builds emit it byte-identically; keep the generic and protected-target probes too.
- Do not duplicate the shared SI INSERT-target parser; agent and ingress must bind the same structured INSERT identity and reject ambiguity consistently. Keep agent `ParseUseDatabaseStrict`, forward's shared fail-open `ParseUseDatabase` wrapper, the `sentioRewriter` known-physical USE fallback, and the ingress text-shape cross-check in their existing narrow scopes.
- Use the operator key `rewriter.physical_database`. Dynamic args carry its value through per-logical `database_map` entries and may carry the session's current physical context through optional `upstream_physical_database_in_context`. Both engines build the table identifier as logical plus delimiter-joined extras (when present), then a literal `.` and the original table.
- Do not reverse-map exception text with local string replacement. When `SessionState.HasActiveRewrite` is set, `rewrite.Plugin.OnException` calls the per-connection `Rewriter.RewriteErrorMessage`, which rebuilds dynamic args from the most recent SQL and effective account before delegating to the selected backend; RPC errors and non-success responses preserve the original message.

## COMMANDS
```bash
bazel test //pkg/rewriter:rewriter_test
bazel test //pkg/rewriter:rewriter_test --test_filter=TestNativeEngineSmoke --test_env=POLYGLOT_SQL_FFI_PATH=/path/to/libpolyglot_sql_ffi.dylib
bazel test //pkg/plugins/rewrite:rewrite_test
```

## NOTES
- `grpc.DialContext` + `WithBlock()` is a known deprecated API in `backend.go`; modernize separately.
- Calls racing factory `Close` can surface as syntax-style rejections. Only the empty-SI path absorbs them through ordinary fail-open behavior; SI-configured paths reject them.
