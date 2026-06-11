# FFI Library Auto-Fetch by Release Tag

**Date:** 2026-06-11
**Status:** Approved
**Builds on:** [2026-06-11-rewriter-go-integration-design.md](2026-06-11-rewriter-go-integration-design.md) (the `engine: native` path this feeds)
**Branch:** stacked on `feat/native-rewriter-engine` (PR #47)

## 1. Goal

The native rewriter engine needs `libpolyglot_sql_ffi.{so,dylib}` on disk. rewriter-go's
release workflow already attaches per-platform builds to every GitHub release
(`libpolyglot_sql_ffi-linux-x86_64.so`, `libpolyglot_sql_ffi-macos-arm64.dylib`). This
feature lets an operator name a release tag and have the library fetched automatically —
two entry points over one shared downloader:

1. **CLI:** `housegate fetch-rewriter-lib --tag v0.2.0 [--dest dir] [--sha256 hex] [--base-url url]`
   → downloads, verifies, prints the final path (stdout is machine-consumable; logs go to
   stderr). For Dockerfiles, init containers, local dev.
2. **Runtime:** `rewriter.native_library_release: v0.2.0` → at startup, when the engine is
   `native` and no explicit `native_library_path` is set, resolve the cache and download on
   miss. Download failure keeps the existing warn-and-disable fail-open posture.

## 2. Shared downloader — `pkg/ffifetch`

```go
type Options struct {
    Tag     string        // required release tag, e.g. "v0.2.0"
    BaseURL string        // default "https://github.com/housegate/rewriter-go/releases/download"
    SHA256  string        // optional hex pin; when set it MUST match
    CacheDir string       // default os.UserCacheDir()/housegate/rewriter-ffi (layout: <tag>/libpolyglot_sql_ffi.<ext>)
    DestDir string        // when set, bypass the cache layout: place the file at DestDir/libpolyglot_sql_ffi.<ext>
    Client  *http.Client  // default: timeout 5m, follows redirects (GitHub → objects.githubusercontent.com)
}
func Fetch(ctx context.Context, opts Options) (path string, err error)
```

Behavior:

- **Platform mapping** via a pure `assetNameFor(goos, goarch)` (table-testable):
  `linux/amd64 → libpolyglot_sql_ffi-linux-x86_64.so`, `darwin/arm64 →
  libpolyglot_sql_ffi-macos-arm64.dylib`; anything else errors with "no prebuilt FFI
  library for <goos>/<goarch>; build from source (`make ffi` in rewriter-go) and set
  native_library_path". The downloaded asset is renamed to the canonical
  `libpolyglot_sql_ffi.<ext>` so polyglot's `OpenDefault` conventions hold.
- **Cache hit** (target exists): with a SHA256 pin, verify the cached file; mismatch →
  delete and re-download once (self-heal), persistent mismatch → error. Without a pin,
  return immediately — zero network. Offline deployments warm the cache once (or use the
  CLI + `--dest`).
- **Download**: stream to a temp file in the target directory while hashing, then
  `os.Rename` (atomic; concurrent fetchers race benignly — identical content, last rename
  wins). File mode 0755.
- **Integrity**, in order:
  1. Explicit `SHA256` pin set → must match, else error.
  2. Otherwise fetch `<BaseURL>/<tag>/SHA256SUMS` (standard `sha256sum` format): present →
     the asset's line must match, mismatch → error; HTTP 404 → log a warning ("no
     SHA256SUMS asset on this release; integrity rests on TLS alone") and proceed. This
     keeps v0.2.0 (which predates the SHA256SUMS asset) usable.
- `BaseURL` override exists for mirrors / internal artifact servers (the URL shape
  `<base>/<tag>/<asset>` is the only contract).

## 3. Config surface (additive)

```yaml
rewriter:
  engine: native
  native_library_release: v0.2.0       # fetch from rewriter-go GitHub releases on miss
  # native_library_path: /path/...     # explicit file ALWAYS wins; no network when set
  # native_library_sha256: <64-hex>    # optional hard pin (verified on cache hits too)
  # native_library_release_base_url: https://mirror.example.com/rewriter-go  # mirror hook
```

Resolution precedence: `native_library_path` → `native_library_release` (cache, then
download) → `POLYGLOT_SQL_FFI_PATH` / system paths (existing behavior, unchanged).
`Config.Validate` checks `native_library_sha256` is 64 hex chars when set; a `release`
set while `engine: grpc` is silently ignored (same convention as `service_addr` under
native). Wiring lives in `buildRewriterFactory`: resolve release → path before
constructing the factory; on fetch error, warn + nil factory (rewriting disabled), same
as every other backend-unavailable case. `rewriter.Options` mirrors the three fields.

## 4. rewriter-go side (small, separate PR)

`release.yml` additionally generates and uploads a `SHA256SUMS` asset
(`sha256sum libpolyglot_sql_ffi-* > SHA256SUMS`) next to the libraries. Existing
v0.2.0 can be back-filled manually with `gh release upload` (operator's call — the
404-fallback covers it either way).

## 5. Testing

`pkg/ffifetch` tests run against `httptest` (no real network, no FFI): happy path +
cache-hit-with-server-stopped (proves zero network), SHA256SUMS match/mismatch/absent,
pin match/mismatch, cache self-heal on pin mismatch, asset 404 error text, `assetNameFor`
table. CLI gets a smoke (flag parsing + dest layout). Config validation cases for the
sha256 format. Real-network verification (subcommand against live v0.2.0 + pointing the
native smoke test at the produced path) is a manual step at the end, not a CI test.

## 6. Out of scope

Background auto-upgrade / "latest" resolution (tag is explicit), Bazel `http_file`
integration (not selected), agent mode (no rewriter), GitHub API auth for private
mirrors (BaseURL swap is the extension point).
