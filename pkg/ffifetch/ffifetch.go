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

	"github.com/housegate/housegate/pkg/log"
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
	if strings.ContainsAny(opts.Tag, `/\`) || opts.Tag == ".." || strings.Contains(opts.Tag, "..") {
		return "", fmt.Errorf("ffifetch: release tag %q must be a bare tag name (no path separators)", opts.Tag)
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
