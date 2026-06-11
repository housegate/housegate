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
	for _, bad := range []string{"../escape", "a/b", "a\\b", ".."} {
		_, err := Fetch(context.Background(), Options{Tag: bad})
		if err == nil || !strings.Contains(err.Error(), "release tag") {
			t.Errorf("tag %q: err = %v, want tag-shape rejection", bad, err)
		}
	}
}
