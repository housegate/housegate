# INTEGRATION GUIDE

## OVERVIEW
`pkg/integration` is the docker/testcontainers ClickHouse suite. It proves whole-proxy behavior and ClickHouse-specific replay/storage behavior that unit tests cannot cover.

## STRUCTURE
```
pkg/integration/
|-- *_test.go        # end-to-end proxy, routing, metrics, replay, storage probes
`-- testenv/         # shared ClickHouse/proxy/client fixtures
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Shared ClickHouse startup | `smoke_test.go`, `testenv/clickhouse.go` | `TestMain` starts one shared container. |
| Proxy fixture | `testenv/proxy.go` | Builds configs, injects network state, runs `housegate.New`. |
| Rewriter mock | `testenv/rewriter_mock.go` | Mock service and fail-open rewrite scenarios. |
| ClickHouse CLI gating | `testenv/cli.go` | Skips when `tests/bin/clickhouse` is absent. |
| Storage promotion probe | `storage_promotion_mvp_test.go` | Opt-in with `HOUSEGATE_RUN_STORAGE_PROMOTION_MVP=1`. |
| ClickHouse replay materializer | `chreplay_test.go` | Docker-bound replay equivalence/fraud tests. |

## CONVENTIONS
- Integration targets are Bazel `manual` and `requires-docker`; `bazel test //...` does not run all of them by default.
- The ClickHouse image is pinned to `clickhouse/clickhouse-server:25.8` in testenv/CI.
- CLI-dependent cases skip if `tests/bin/clickhouse` is missing.
- When diagnosing failures, compare against a clean `main` run before calling it a regression.
- Keep exact ClickHouse outcome evidence in fragile storage tests; log attach/replace errors and row counts.

## ANTI-PATTERNS
- Do not require docker-bound tests to pass in docker-less local verification unless the task specifically targets integration behavior.
- Do not make opt-in probes part of the normal integration suite accidentally.
- Do not assert more than the probe is designed to learn when ClickHouse behavior itself is under observation.
- Do not add a new docker-bound Bazel target without adding it to `.github/workflows/ci.yml`.

## COMMANDS
```bash
bazel test //pkg/integration:integration_test --test_output=errors
bazel test //pkg/integration:integration_test --test_filter='TestAuth' --test_output=errors
bazel test //pkg/integration/testenv:testenv_test --test_output=errors
HOUSEGATE_RUN_STORAGE_PROMOTION_MVP=1 bazel test //pkg/integration:integration_test --test_filter=TestStoragePromotionMVP_hardlinkUnsafePartIntoPromoteTable --test_env=HOUSEGATE_RUN_STORAGE_PROMOTION_MVP=1 --test_output=errors
```

## NOTES
- `HOME` must be writable for the testcontainers reaper.
- The harness waits for HTTP 8123 and also polls native TCP 9000 before tests proceed.
