package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/housegate/housegate/pkg/ffifetch"
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
// do: POLYGLOT_SQL_FFI_PATH=$(housegate fetch-rewriter-lib). Without --tag
// it fetches ffifetch.DefaultRelease, the release this binary is built for.
func runFetchRewriterLib(args []string) int {
	fs := flag.NewFlagSet("fetch-rewriter-lib", flag.ContinueOnError)
	tag := fs.String("tag", ffifetch.DefaultRelease, "rewriter-go release tag")
	dest := fs.String("dest", "", "directory for libpolyglot_sql_ffi.<ext> (default: user cache, keyed by tag)")
	sha := fs.String("sha256", "", "optional sha256 pin (64 hex chars)")
	baseURL := fs.String("base-url", "", "release download root (default: rewriter-go GitHub releases)")
	requireChecksum := fs.Bool("require-checksum", false, "without --sha256, fail unless the release's SHA256SUMS lists the library and it matches (also checks a cached copy)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *tag == "" {
		fmt.Fprintln(os.Stderr, "usage: housegate fetch-rewriter-lib [--tag vX.Y.Z] [--dest dir] [--sha256 hex | --require-checksum] [--base-url url]")
		return 2
	}
	path, err := ffifetch.Fetch(context.Background(), ffifetch.Options{
		Tag:             *tag,
		DestDir:         *dest,
		SHA256:          *sha,
		BaseURL:         *baseURL,
		RequireChecksum: *requireChecksum,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "fetch-rewriter-lib:", err)
		return 1
	}
	fmt.Println(path)
	return 0
}
