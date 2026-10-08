package housegate

import (
	"os"
	"regexp"
	"testing"

	"github.com/housegate/housegate/pkg/ffifetch"
)

// The implicit native materializer fetches ffifetch.DefaultRelease; it must
// be the rewriter-go version the binary's Go binding was built against.
func TestFFIDefaultReleaseMatchesGoMod(t *testing.T) {
	raw, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^\s*github\.com/housegate/rewriter-go (v\S+)`).FindSubmatch(raw)
	if m == nil {
		t.Fatal("go.mod has no rewriter-go require")
	}
	if string(m[1]) != ffifetch.DefaultRelease {
		t.Fatalf("ffifetch.DefaultRelease = %s, go.mod requires rewriter-go %s; bump them together", ffifetch.DefaultRelease, m[1])
	}
}
