# FFI Library Auto-Fetch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `housegate fetch-rewriter-lib --tag vX.Y.Z` and `rewriter.native_library_release: vX.Y.Z` both resolve the polyglot FFI library from rewriter-go's GitHub release assets via one shared downloader.

**Architecture:** New leaf package `pkg/ffifetch` (pure net/http downloader: platform→asset mapping, tag-keyed cache, atomic rename, SHA256SUMS/pin verification). Entry point 1 = `cmd/fetch.go` subcommand mirroring `secretSubcommand`'s `(handled, exit)` pattern; entry point 2 = `buildRewriterFactory` resolving release→path before constructing the factory (fail-open warn-and-disable on fetch error). rewriter-go's release workflow gains a SHA256SUMS asset; absent SHA256SUMS (v0.2.0) degrades to a warning.

**Tech Stack:** Go stdlib only (net/http, crypto/sha256), httptest for tests. No new module deps.

**Spec:** [docs/superpowers/specs/2026-06-11-ffi-release-fetch-design.md](../specs/2026-06-11-ffi-release-fetch-design.md)

**Branches:** housegate → continue on `feat/native-rewriter-engine`. rewriter-go → new `feat/release-sha256sums`.

**Spec amendment carried by this plan:** §3's "rewriter.Options mirrors the three fields" is WRONG and superseded — release resolution happens entirely in `buildRewriterFactory` before `rewriter.Options` is constructed, so Options stays untouched (Task F3 fixes the spec line).

---

### Task F1: rewriter-go — SHA256SUMS release asset

**Files (in /Users/uranuswch/Dev/housegate/rewriter-go):**
- Modify: `.github/workflows/release.yml`

- [ ] **Step F1.1: Branch + read the packaging step**

```bash
cd /Users/uranuswch/Dev/housegate/rewriter-go
git checkout main && git pull && git checkout -b feat/release-sha256sums
grep -n "upload\|files:\|gh release\|softprops\|artifact" .github/workflows/release.yml
```

Read the job that collects the two `libpolyglot_sql_ffi-*` artifacts and creates the GitHub Release (whatever its mechanism: `gh release create`, `softprops/action-gh-release`, etc.).

- [ ] **Step F1.2: Generate SHA256SUMS next to the libs**

In the release-creation job, AFTER both platform artifacts are downloaded/collected into one directory and BEFORE the release-upload step, add a step (adapt the working-directory to the actual layout):

```yaml
      - name: Generate SHA256SUMS
        run: |
          cd <dir-with-the-two-libs>
          sha256sum libpolyglot_sql_ffi-* > SHA256SUMS
          cat SHA256SUMS
```

and add `SHA256SUMS` to the release's file list (same syntax the workflow already uses for the two libs — e.g. another line under `files:` or another argument to `gh release upload`).

- [ ] **Step F1.3: Validate + commit + push + PR**

```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/release.yml')); print('YAML OK')"
git add .github/workflows/release.yml
git commit -m "ci(release): publish SHA256SUMS next to the FFI library assets

Lets consumers (housegate's ffifetch) verify downloads; releases
without the asset (v0.2.0 and earlier) degrade to a TLS-only warning
on the consumer side."
git push -u origin feat/release-sha256sums
gh pr create --title "ci(release): publish SHA256SUMS asset" --body "Adds a sha256sum manifest next to the FFI libraries so downstream auto-fetch (housegate fetch-rewriter-lib / rewriter.native_library_release) can verify integrity. Pre-existing releases degrade to a TLS-only warning.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

(The workflow can't be exercised without cutting a release — YAML-parse check + review is the gate. Note in the PR that the next routine release validates it.)

### Task F2: housegate — `pkg/ffifetch` (TDD)

**Files:**
- Create: `pkg/ffifetch/ffifetch.go`
- Create: `pkg/ffifetch/ffifetch_test.go`

- [ ] **Step F2.1: Write the failing tests** — create `pkg/ffifetch/ffifetch_test.go`:

```go
package ffifetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// testAsset resolves the asset/ext for the host platform, skipping on
// platforms with no prebuilt (the pure mapping itself is table-tested below).
func testAsset(t *testing.T) (asset, ext string) {
	t.Helper()
	asset, ext, err := assetNameFor(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		t.Skipf("no prebuilt asset for this platform: %v", err)
	}
	return asset, ext
}

// fakeRelease serves <prefix>/<tag>/<asset> and optionally SHA256SUMS.
func fakeRelease(t *testing.T, tag, asset string, lib []byte, sums string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/"+tag+"/"+asset, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(lib)
	})
	if sums != "" {
		mux.HandleFunc("/"+tag+"/SHA256SUMS", func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(sums))
		})
	}
	return httptest.NewServer(mux)
}

func sumOf(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestAssetNameFor(t *testing.T) {
	cases := []struct {
		goos, goarch, asset, ext string
		wantErr                  bool
	}{
		{"linux", "amd64", "libpolyglot_sql_ffi-linux-x86_64.so", ".so", false},
		{"darwin", "arm64", "libpolyglot_sql_ffi-macos-arm64.dylib", ".dylib", false},
		{"linux", "arm64", "", "", true},
		{"windows", "amd64", "", "", true},
	}
	for _, c := range cases {
		asset, ext, err := assetNameFor(c.goos, c.goarch)
		if (err != nil) != c.wantErr || asset != c.asset || ext != c.ext {
			t.Errorf("assetNameFor(%s,%s) = (%q,%q,%v), want (%q,%q,err=%v)",
				c.goos, c.goarch, asset, ext, err, c.asset, c.ext, c.wantErr)
		}
	}
}

func TestFetch_HappyPath_ThenCacheHitOffline(t *testing.T) {
	asset, ext := testAsset(t)
	lib := []byte("fake ffi lib bytes")
	sums := fmt.Sprintf("%s  %s\n", sumOf(lib), asset)
	srv := fakeRelease(t, "v9.9.9", asset, lib, sums)
	cache := t.TempDir()

	path, err := Fetch(context.Background(), Options{Tag: "v9.9.9", BaseURL: srv.URL, CacheDir: cache})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	want := filepath.Join(cache, "v9.9.9", "libpolyglot_sql_ffi"+ext)
	if path != want {
		t.Errorf("path = %q, want %q", path, want)
	}
	got, err := os.ReadFile(path)
	if err != nil || string(got) != string(lib) {
		t.Fatalf("content mismatch: %q err=%v", got, err)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o755 {
		t.Errorf("mode = %v, want 0755", fi.Mode().Perm())
	}

	srv.Close() // cache hit must need NO network
	path2, err := Fetch(context.Background(), Options{Tag: "v9.9.9", BaseURL: srv.URL, CacheDir: cache})
	if err != nil || path2 != path {
		t.Fatalf("cache hit: path=%q err=%v", path2, err)
	}
}

func TestFetch_SumsMismatchFails(t *testing.T) {
	asset, _ := testAsset(t)
	lib := []byte("payload")
	sums := fmt.Sprintf("%064d  %s\n", 0, asset) // wrong hash
	srv := fakeRelease(t, "v1.0.0", asset, lib, sums)
	defer srv.Close()
	cache := t.TempDir()

	_, err := Fetch(context.Background(), Options{Tag: "v1.0.0", BaseURL: srv.URL, CacheDir: cache})
	if err == nil || !strings.Contains(err.Error(), "SHA256SUMS") {
		t.Fatalf("err = %v, want SHA256SUMS mismatch", err)
	}
	entries, _ := os.ReadDir(filepath.Join(cache, "v1.0.0"))
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".ffifetch-") { // tmp may linger only if remove failed
			t.Errorf("unexpected cached file after failed verify: %s", e.Name())
		}
	}
}

func TestFetch_NoSumsAssetSucceedsWithWarning(t *testing.T) {
	asset, _ := testAsset(t)
	lib := []byte("payload")
	srv := fakeRelease(t, "v0.2.0", asset, lib, "") // no SHA256SUMS route → 404
	defer srv.Close()

	path, err := Fetch(context.Background(), Options{Tag: "v0.2.0", BaseURL: srv.URL, CacheDir: t.TempDir()})
	if err != nil {
		t.Fatalf("Fetch without SHA256SUMS must succeed (TLS-only warning): %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != "payload" {
		t.Errorf("content = %q", got)
	}
}

func TestFetch_PinMismatchFails_PinMatchSucceeds(t *testing.T) {
	asset, _ := testAsset(t)
	lib := []byte("pinned payload")
	srv := fakeRelease(t, "v1.1.0", asset, lib, "")
	defer srv.Close()

	_, err := Fetch(context.Background(), Options{Tag: "v1.1.0", BaseURL: srv.URL, CacheDir: t.TempDir(),
		SHA256: strings.Repeat("ab", 32)})
	if err == nil || !strings.Contains(err.Error(), "pinned") {
		t.Fatalf("err = %v, want pin mismatch", err)
	}

	path, err := Fetch(context.Background(), Options{Tag: "v1.1.0", BaseURL: srv.URL, CacheDir: t.TempDir(),
		SHA256: sumOf(lib)})
	if err != nil {
		t.Fatalf("pin match: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != string(lib) {
		t.Errorf("content = %q", got)
	}
}

func TestFetch_CacheSelfHealsOnPinMismatch(t *testing.T) {
	asset, ext := testAsset(t)
	lib := []byte("good bytes")
	srv := fakeRelease(t, "v1.2.0", asset, lib, "")
	defer srv.Close()
	cache := t.TempDir()

	// Pre-corrupt the cache slot.
	dir := filepath.Join(cache, "v1.2.0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "libpolyglot_sql_ffi"+ext), []byte("corrupt"), 0o755); err != nil {
		t.Fatal(err)
	}

	path, err := Fetch(context.Background(), Options{Tag: "v1.2.0", BaseURL: srv.URL, CacheDir: cache, SHA256: sumOf(lib)})
	if err != nil {
		t.Fatalf("self-heal: %v", err)
	}
	if got, _ := os.ReadFile(path); string(got) != string(lib) {
		t.Errorf("content after self-heal = %q", got)
	}
}

func TestFetch_Asset404IsClearError(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	_, err := Fetch(context.Background(), Options{Tag: "v404.0.0", BaseURL: srv.URL, CacheDir: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "404") || !strings.Contains(err.Error(), "v404.0.0") {
		t.Fatalf("err = %v, want HTTP 404 mentioning the tag", err)
	}
}

func TestFetch_DestDirBypassesCacheLayout(t *testing.T) {
	asset, ext := testAsset(t)
	lib := []byte("dest payload")
	srv := fakeRelease(t, "v1.3.0", asset, lib, "")
	defer srv.Close()
	dest := t.TempDir()

	path, err := Fetch(context.Background(), Options{Tag: "v1.3.0", BaseURL: srv.URL, DestDir: dest})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if want := filepath.Join(dest, "libpolyglot_sql_ffi"+ext); path != want {
		t.Errorf("path = %q, want %q (no <tag>/ sublayout)", path, want)
	}
}

func TestFetch_InvalidInputs(t *testing.T) {
	if _, err := Fetch(context.Background(), Options{}); err == nil {
		t.Error("empty tag must error")
	}
	if _, err := Fetch(context.Background(), Options{Tag: "v1", SHA256: "zz"}); err == nil {
		t.Error("malformed sha256 pin must error")
	}
}
```

- [ ] **Step F2.2: Run to verify failure**

```bash
bazel run //:gazelle -- pkg/ffifetch
bazel test //pkg/ffifetch:ffifetch_test --test_output=errors
```

Expected: compile FAILURE (`undefined: assetNameFor`, `undefined: Fetch`, `undefined: Options`).

- [ ] **Step F2.3: Implement** — create `pkg/ffifetch/ffifetch.go`:

```go
// Package ffifetch downloads the polyglot FFI library that the native
// rewriter engine dlopens, from a rewriter-go GitHub release (or any
// mirror exposing the same <base>/<tag>/<asset> URL shape). One shared
// implementation behind both the `housegate fetch-rewriter-lib`
// subcommand and the rewriter.native_library_release startup path.
package ffifetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"housegate/housegate/pkg/log"
)

// DefaultBaseURL is the rewriter-go GitHub release download root. The only
// URL contract is <base>/<tag>/<asset>, so mirrors swap cleanly.
const DefaultBaseURL = "https://github.com/housegate/rewriter-go/releases/download"

// Options configure Fetch. Tag is required; everything else defaults.
type Options struct {
	// Tag is the rewriter-go release tag, e.g. "v0.2.0".
	Tag string
	// BaseURL overrides DefaultBaseURL (mirrors / internal artifact servers).
	BaseURL string
	// SHA256 is an optional hex pin; when set the download (and any cached
	// copy) must match it.
	SHA256 string
	// CacheDir overrides the cache root (default
	// os.UserCacheDir()/housegate/rewriter-ffi); layout is
	// <CacheDir>/<tag>/libpolyglot_sql_ffi.<ext>.
	CacheDir string
	// DestDir, when set, bypasses the cache layout entirely: the file lands
	// at DestDir/libpolyglot_sql_ffi.<ext>.
	DestDir string
	// Client overrides the default HTTP client (5-minute total timeout;
	// redirects followed — GitHub bounces to objects.githubusercontent.com).
	Client *http.Client
}

// errNoSums distinguishes "release publishes no SHA256SUMS asset" (warn and
// proceed) from transport failures (abort).
var errNoSums = errors.New("no SHA256SUMS asset")

// assetNameFor maps a platform to the release asset name and the canonical
// library extension. Pure so the whole table is testable on any host.
func assetNameFor(goos, goarch string) (asset, ext string, err error) {
	switch goos + "/" + goarch {
	case "linux/amd64":
		return "libpolyglot_sql_ffi-linux-x86_64.so", ".so", nil
	case "darwin/arm64":
		return "libpolyglot_sql_ffi-macos-arm64.dylib", ".dylib", nil
	default:
		return "", "", fmt.Errorf("no prebuilt FFI library for %s/%s; build from source (`make ffi` in rewriter-go) and set native_library_path", goos, goarch)
	}
}

// Fetch resolves the FFI library for opts.Tag, downloading on cache miss,
// and returns the absolute path of the canonical library file.
func Fetch(ctx context.Context, opts Options) (string, error) {
	if opts.Tag == "" {
		return "", fmt.Errorf("ffifetch: release tag is required")
	}
	if opts.SHA256 != "" {
		if raw, err := hex.DecodeString(opts.SHA256); err != nil || len(raw) != sha256.Size {
			return "", fmt.Errorf("ffifetch: sha256 pin must be 64 hex chars")
		}
	}
	asset, ext, err := assetNameFor(runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return "", err
	}

	dir := opts.DestDir
	if dir == "" {
		cache := opts.CacheDir
		if cache == "" {
			base, err := os.UserCacheDir()
			if err != nil {
				return "", fmt.Errorf("ffifetch: resolve user cache dir: %w", err)
			}
			cache = filepath.Join(base, "housegate", "rewriter-ffi")
		}
		dir = filepath.Join(cache, opts.Tag)
	}
	target := filepath.Join(dir, "libpolyglot_sql_ffi"+ext)

	if _, statErr := os.Stat(target); statErr == nil {
		if opts.SHA256 == "" {
			return target, nil // cache hit, zero network
		}
		if sum, herr := fileSHA256(target); herr == nil && strings.EqualFold(sum, opts.SHA256) {
			return target, nil
		}
		// Self-heal: cached copy fails the pin → drop it and re-download.
		log.Warnw("cached FFI library fails sha256 pin; re-downloading", "path", target)
		_ = os.Remove(target)
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("ffifetch: %w", err)
	}
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Minute}
	}
	base := strings.TrimSuffix(opts.BaseURL, "/")
	if base == "" {
		base = DefaultBaseURL
	}

	sum, tmp, err := downloadTemp(ctx, client, base+"/"+opts.Tag+"/"+asset, dir)
	if err != nil {
		return "", err
	}
	defer func() {
		if tmp != "" {
			_ = os.Remove(tmp)
		}
	}()

	if opts.SHA256 != "" {
		if !strings.EqualFold(sum, opts.SHA256) {
			return "", fmt.Errorf("ffifetch: %s sha256 = %s, want pinned %s", asset, sum, strings.ToLower(opts.SHA256))
		}
	} else {
		want, sumsErr := releaseSum(ctx, client, base+"/"+opts.Tag+"/SHA256SUMS", asset)
		switch {
		case errors.Is(sumsErr, errNoSums):
			log.Warnw("release publishes no SHA256SUMS asset; integrity rests on TLS alone", "tag", opts.Tag)
		case sumsErr != nil:
			return "", sumsErr
		case want == "":
			log.Warnw("SHA256SUMS has no entry for asset; integrity rests on TLS alone", "tag", opts.Tag, "asset", asset)
		case !strings.EqualFold(sum, want):
			return "", fmt.Errorf("ffifetch: %s sha256 = %s, SHA256SUMS says %s", asset, sum, want)
		}
	}

	if err := os.Chmod(tmp, 0o755); err != nil {
		return "", fmt.Errorf("ffifetch: %w", err)
	}
	if err := os.Rename(tmp, target); err != nil {
		return "", fmt.Errorf("ffifetch: %w", err)
	}
	tmp = "" // consumed — disarm the deferred remove
	return target, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// downloadTemp streams url into a fresh temp file inside dir (same volume as
// the final target so the eventual rename is atomic), hashing as it copies.
func downloadTemp(ctx context.Context, client *http.Client, url, dir string) (sum, tmpPath string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", "", fmt.Errorf("ffifetch: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("ffifetch: download %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("ffifetch: download %s: HTTP %s", url, resp.Status)
	}
	tmp, err := os.CreateTemp(dir, ".ffifetch-*")
	if err != nil {
		return "", "", fmt.Errorf("ffifetch: %w", err)
	}
	h := sha256.New()
	_, cpErr := io.Copy(io.MultiWriter(tmp, h), resp.Body)
	clErr := tmp.Close()
	if cpErr != nil || clErr != nil {
		_ = os.Remove(tmp.Name())
		return "", "", fmt.Errorf("ffifetch: download %s: %w", url, errors.Join(cpErr, clErr))
	}
	return hex.EncodeToString(h.Sum(nil)), tmp.Name(), nil
}

// releaseSum fetches and parses the release's SHA256SUMS (sha256sum format:
// "<hex>  <name>" per line, optional leading '*' on the name for binary
// mode). Returns "" when the manifest exists but lacks the asset's line, and
// errNoSums when the release has no SHA256SUMS asset at all (HTTP 404).
func releaseSum(ctx context.Context, client *http.Client, url, asset string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("ffifetch: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("ffifetch: fetch SHA256SUMS: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return "", errNoSums
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("ffifetch: fetch SHA256SUMS: HTTP %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("ffifetch: read SHA256SUMS: %w", err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 2 && strings.TrimPrefix(fields[1], "*") == asset {
			return fields[0], nil
		}
	}
	return "", nil
}
```

- [ ] **Step F2.4: Run to verify pass**

```bash
bazel run //:gazelle -- pkg/ffifetch
bazel test //pkg/ffifetch:ffifetch_test --test_output=errors
bazel build //...
```

Expected: all 9 tests PASS; build green.

- [ ] **Step F2.5: Commit**

```bash
git add pkg/ffifetch/
git commit -m "feat(ffifetch): download the polyglot FFI library from release assets

Tag-keyed cache under os.UserCacheDir, atomic rename, platform→asset
mapping, integrity via SHA256SUMS asset or an explicit pin (with
cache self-heal); releases without SHA256SUMS degrade to a TLS-only
warning. Shared by the CLI subcommand and the startup resolver."
```

### Task F3: housegate — subcommand + config + wiring + docs

**Files:**
- Create: `cmd/fetch.go`
- Modify: `cmd/main.go` (one dispatch call next to `secretSubcommand()`)
- Modify: `pkg/plugins/rewrite/config.go` (+3 fields)
- Modify: `pkg/config/config.go` (Validate: sha256 format check)
- Modify: `pkg/config/config_test.go` (validate cases)
- Modify: `build.go` (`buildRewriterFactory` resolves release → path)
- Modify: `configs/local.server.yaml`, `configs/local.server-mock-remote.yaml`, `configs/local.server.json`, `README.md` (new keys), re-encrypt `configs/local.server.yaml.age`
- Modify: `docs/superpowers/specs/2026-06-11-ffi-release-fetch-design.md` (spec amendment, see plan header)

- [ ] **Step F3.1: Failing validate test** — in `pkg/config/config_test.go`, next to the `rewriter_engine_*` cases, same scaffold:

```go
	t.Run("rewriter_native_sha256_invalid", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.Rewriter.NativeLibrarySHA256 = "not-hex"
		err := cfg.Validate()
		if err == nil || !strings.Contains(err.Error(), "native_library_sha256") {
			t.Fatalf("err = %v, want native_library_sha256 rejection", err)
		}
	})

	t.Run("rewriter_native_sha256_ok", func(t *testing.T) {
		cfg := minimalServerConfig(t)
		cfg.Rewriter.NativeLibrarySHA256 = strings.Repeat("ab", 32)
		if err := cfg.Validate(); err != nil && strings.Contains(err.Error(), "native_library_sha256") {
			t.Fatalf("valid sha256 rejected: %v", err)
		}
	})
```

(Adapt the scaffold helper name to what `TestConfigValidate` actually uses — the engine cases added earlier are the reference.) Run `bazel test //pkg/config:config_test --test_output=errors` → expect compile FAIL (field undefined).

- [ ] **Step F3.2: Config fields** — `pkg/plugins/rewrite/config.go`, after `NativeLibraryPath`:

```go
	// NativeLibraryRelease names a rewriter-go release tag (e.g. "v0.2.0")
	// to fetch the FFI library from when the native engine is selected and
	// NativeLibraryPath is empty. The library is cached under the user
	// cache dir and downloaded only on miss; fetch failure disables
	// rewriting (fail-open), like any other backend-unavailable case.
	NativeLibraryRelease string `json:"native_library_release" yaml:"native_library_release"`

	// NativeLibrarySHA256 optionally pins the library's sha256 (64 hex
	// chars). Verified against cached copies too; mismatch re-downloads
	// once, then fails. Without a pin, the release's SHA256SUMS asset is
	// used when present (TLS-only warning otherwise).
	NativeLibrarySHA256 string `json:"native_library_sha256" yaml:"native_library_sha256"`

	// NativeLibraryReleaseBaseURL overrides the download root (default:
	// rewriter-go's GitHub releases) for mirrors / internal artifact
	// servers. URL shape: <base>/<tag>/<asset>.
	NativeLibraryReleaseBaseURL string `json:"native_library_release_base_url" yaml:"native_library_release_base_url"`
```

- [ ] **Step F3.3: Validate** — `pkg/config/config.go`, right after the engine switch:

```go
	if s := c.Rewriter.NativeLibrarySHA256; s != "" {
		if raw, err := hex.DecodeString(s); err != nil || len(raw) != 32 {
			errs = append(errs, fmt.Errorf("rewriter.native_library_sha256 must be 64 hex chars, got %q", s))
		}
	}
```

(import `encoding/hex`.) Run the F3.1 cases → PASS.

- [ ] **Step F3.4: Subcommand** — create `cmd/fetch.go`:

```go
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"housegate/housegate/pkg/ffifetch"
)

// fetchSubcommand dispatches "fetch-rewriter-lib". Mirrors
// secretSubcommand's (handled, exit) contract so main can run it before
// flag.Parse without touching proxy flags.
func fetchSubcommand() (handled bool, exit int) {
	if len(os.Args) < 2 || os.Args[1] != "fetch-rewriter-lib" {
		return false, 0
	}
	return true, runFetchRewriterLib(os.Args[2:])
}

// runFetchRewriterLib downloads the FFI library for a release tag and
// prints the resulting path on stdout (logs go to stderr), so shells can
// do: POLYGLOT_SQL_FFI_PATH=$(housegate fetch-rewriter-lib --tag v0.2.0).
func runFetchRewriterLib(args []string) int {
	fs := flag.NewFlagSet("fetch-rewriter-lib", flag.ContinueOnError)
	tag := fs.String("tag", "", "rewriter-go release tag, e.g. v0.2.0 (required)")
	dest := fs.String("dest", "", "directory for libpolyglot_sql_ffi.<ext> (default: user cache, keyed by tag)")
	sha := fs.String("sha256", "", "optional sha256 pin (64 hex chars)")
	baseURL := fs.String("base-url", "", "release download root (default: rewriter-go GitHub releases)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *tag == "" {
		fmt.Fprintln(os.Stderr, "usage: housegate fetch-rewriter-lib --tag vX.Y.Z [--dest dir] [--sha256 hex] [--base-url url]")
		return 2
	}
	path, err := ffifetch.Fetch(context.Background(), ffifetch.Options{
		Tag:     *tag,
		DestDir: *dest,
		SHA256:  *sha,
		BaseURL: *baseURL,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch-rewriter-lib:", err)
		return 1
	}
	fmt.Println(path)
	return 0
}
```

In `cmd/main.go`, find the `secretSubcommand()` call and add the same pattern next to it:

```go
	if handled, exit := fetchSubcommand(); handled {
		os.Exit(exit)
	}
```

(Read the surrounding code first and mirror its exact shape — if secretSubcommand's result is handled differently, match that.)

- [ ] **Step F3.5: Startup wiring** — `build.go`, inside `buildRewriterFactory`, after the router-only early return and BEFORE the `rwConfig := rewriter.Options{...}` literal:

```go
	// native_library_release: resolve the FFI library from a rewriter-go
	// release before constructing the factory. Explicit NativeLibraryPath
	// wins; fetch failure keeps the warn-and-disable fail-open posture.
	nativeLibPath := cfg.Rewriter.NativeLibraryPath
	if cfg.Rewriter.Engine == rewriter.EngineNative && nativeLibPath == "" && cfg.Rewriter.NativeLibraryRelease != "" {
		p, err := ffifetch.Fetch(context.Background(), ffifetch.Options{
			Tag:     cfg.Rewriter.NativeLibraryRelease,
			SHA256:  cfg.Rewriter.NativeLibrarySHA256,
			BaseURL: cfg.Rewriter.NativeLibraryReleaseBaseURL,
		})
		if err != nil {
			log.Warne(err, "failed to fetch native rewriter library, rewriting disabled")
			return nil
		}
		log.Infow("native rewriter library resolved from release",
			"tag", cfg.Rewriter.NativeLibraryRelease, "path", p)
		nativeLibPath = p
	}
```

and change the literal's line to `NativeLibraryPath: nativeLibPath,`. Add imports (`context`, `housegate/housegate/pkg/ffifetch`). `rewriter.Options` is NOT extended (see plan header spec amendment).

- [ ] **Step F3.6: Spec amendment** — in `docs/superpowers/specs/2026-06-11-ffi-release-fetch-design.md` §3, replace "`rewriter.Options` mirrors the three fields." with "`rewriter.Options` is untouched — resolution completes in `buildRewriterFactory` before Options is constructed; library embedders building their own `rewriter.Options` call `ffifetch.Fetch` themselves if they want release resolution."

- [ ] **Step F3.7: Examples + README**

YAML examples (both `local.server.yaml` and `local.server-mock-remote.yaml`), extend the rewriter block's commented hints right under the existing `# native_library_path:` line:

```yaml
  # native_library_release: v0.2.0   # native only: auto-fetch the FFI lib from rewriter-go releases (cached; native_library_path wins)
  # native_library_sha256: ""        # optional hard pin for the fetched lib
```

JSON example (`local.server.json` rewriter object): add `"native_library_release": "",` after `"native_library_path": ""`. README rewriter table: two rows after `native_library_path` (`engine`-table style established earlier):

```
| `rewriter.native_library_release` | string | No | `` | rewriter-go release tag to auto-fetch the FFI lib from (native only; cached under the user cache dir; `native_library_path` wins; also overridable via `rewriter.native_library_release_base_url` for mirrors) |
| `rewriter.native_library_sha256` | string | No | `` | Optional sha256 pin for the fetched library (64 hex); without it the release's `SHA256SUMS` asset is checked when present |
```

Re-encrypt the age example (binary already built earlier; rebuild if needed):

```bash
bazel build //cmd:housegate
rm configs/local.server.yaml.age
./bazel-bin/cmd/housegate_/housegate secret-encrypt -r age1smeshc4q2u54yaqg5lavsexp2pk6pydr00rfuj5radxj6kas7uzszucx2k configs/local.server.yaml configs/local.server.yaml.age
HOUSEGATE_AGE_IDENTITY_FILE=configs/local.age-key ./bazel-bin/cmd/housegate_/housegate secret-decrypt configs/local.server.yaml.age | diff configs/local.server.yaml - && echo ROUNDTRIP OK
```

- [ ] **Step F3.8: Verify + commit**

```bash
bazel run //:gazelle
bazel test //pkg/config:config_test //pkg/ffifetch:ffifetch_test --test_output=errors
bazel build //...
# subcommand smoke against the REAL v0.2.0 release (network):
./bazel-bin/cmd/housegate_/housegate fetch-rewriter-lib --tag v0.2.0 --dest /tmp/ffi-fetch-smoke
ls -l /tmp/ffi-fetch-smoke/
git add cmd/ pkg/plugins/rewrite/config.go pkg/config/ build.go configs/ README.md docs/superpowers/specs/2026-06-11-ffi-release-fetch-design.md
git commit -m "feat(ffifetch): fetch-rewriter-lib subcommand + native_library_release config

housegate fetch-rewriter-lib --tag vX.Y.Z downloads/caches/verifies and
prints the path; rewriter.native_library_release does the same at
startup when the native engine has no explicit library path (fetch
failure = warn-and-disable). sha256 pin validated in Config.Validate."
```

(Real-network smoke expected: prints `/tmp/ffi-fetch-smoke/libpolyglot_sql_ffi.dylib` with a warn about missing SHA256SUMS on v0.2.0. If the sandbox blocks the network call, note it and defer to Task F4's manual verify.)

### Task F4: Final verification (controller)

- [ ] `bazel test //... --test_output=errors` — full sweep green.
- [ ] End-to-end native-over-fetched-lib check: run the native smoke test pointing `POLYGLOT_SQL_FFI_PATH` at the file produced by the F3.8 subcommand smoke (proves the downloaded artifact actually dlopens):
  `bazel test //pkg/rewriter:rewriter_test --test_filter=TestNativeEngineSmoke --test_output=all --cache_test_results=no --test_env=POLYGLOT_SQL_FFI_PATH=/tmp/ffi-fetch-smoke/libpolyglot_sql_ffi.dylib`
- [ ] Push; comment on PR #47 summarizing the addition; surface the optional v0.2.0 SHA256SUMS backfill command to the user.
