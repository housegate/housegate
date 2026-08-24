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
| Plugin entry | `../plugins/rewrite/` | Query hook owns one `Rewriter` per connection. |

## CONVENTIONS
- The proxy never parses SQL. The rewriter backend supplies `StatementType`, accessed tables, table/database rewrites, and privilege deltas.
- `Options.Engine == ""` means gRPC. Valid engines are `grpc` and `native`; config validation is mode-independent.
- Native library resolution order is explicit path, `POLYGLOT_SQL_FFI_PATH`, then default search locations.
- Native ctx deadlines are advisory because an in-flight FFI call cannot be interrupted.
- Backend startup failure preserves the warn-and-disable fail-open posture.
- Cross-indexer `remote()` credentials go through `resolveRemoteCredentials`.

## ANTI-PATTERNS
- Do not revive in-repo generated proto packages. Import `github.com/housegate/rewriter-proto/gen/pb`.
- Do not make rewrite backend unavailability crash startup without a deliberate fail-closed design change.
- Do not assert exact physical table spelling across gRPC and native engines when `state.physical_database` is active; assert the physical DB move.
- Do not wire new SQL features around `ErrorReverseMap`; it remains a no-op until the backend RPC is implemented.

## COMMANDS
```bash
bazel test //pkg/rewriter:rewriter_test
bazel test //pkg/rewriter:rewriter_test --test_filter=TestNativeEngineSmoke --test_env=POLYGLOT_SQL_FFI_PATH=/path/to/libpolyglot_sql_ffi.dylib
bazel test //pkg/plugins/rewrite:rewrite_test
```

## NOTES
- `grpc.DialContext` + `WithBlock()` is a known deprecated API in `backend.go`; modernize separately.
- Calls racing factory `Close` can surface as syntax-style rejections and are absorbed by fail-open plugin behavior.
