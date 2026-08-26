package rewriter

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/sqlmeta"
)

// TestNativeEngineSmoke drives the real in-process rewriter-go engine
// through the full factory path: dynamic args from network state →
// native backend → RewriteResult. Skips when the polyglot FFI library is
// not available (CI); run locally with:
//
//	bazel test //pkg/rewriter:rewriter_test \
//	  --test_filter=TestNativeEngineSmoke \
//	  --test_env=POLYGLOT_SQL_FFI_PATH=$HOME/Dev/housegate/rewriter-go/third_party/lib/libpolyglot_sql_ffi.dylib
func TestNativeEngineSmoke(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; native engine FFI lib unavailable")
	}
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1"}

	f, err := NewSentioNetworkFactory(Options{
		Engine:           EngineNative,
		PhysicalDatabase: "phys",
		Listen:           ":9000",
	}, st)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(native): %v", err)
	}
	defer f.Close()

	rw := f.NewRewriter(&fakeSession{})
	res, err := rw.Rewrite(context.Background(), "SELECT a FROM db1.t", "")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}

	// Log the actual output — invaluable when the smoke test trips later.
	t.Logf("rewritten: %q tableRewrites=%v", res.SQL, res.TableRewrites)

	if res.StatementType != sqlmeta.StatementTypeSelect {
		t.Errorf("StatementType = %v, want SELECT (%v)", res.StatementType, sqlmeta.StatementTypeSelect)
	}
	// The native engine resolves db1.t into the physical database; the
	// physical table half is the dotted, double-quoted logical name
	// (phys."db1.t"), NOT an underscore join.
	if !strings.Contains(res.SQL, "phys.") {
		t.Errorf("SQL = %q, want it moved into the physical database", res.SQL)
	}
	if res.SQL == "SELECT a FROM db1.t" {
		t.Errorf("SQL unchanged — rewrite did not fire")
	}
	if len(res.TableRewrites) == 0 {
		t.Errorf("TableRewrites empty, want the db1.t mapping")
	}
}

// TestNativeEngineProbeSmoke runs the real startup build probe against the
// pinned native engine. The scripted probe tests prove the probe's logic; this
// proves the engine the pin actually resolves to answers every case, including
// the Spec N tagged-heredoc discriminator. Without it the floor in go.mod and
// ci.yml would be asserted only against a fake.
func TestNativeEngineProbeSmoke(t *testing.T) {
	if os.Getenv("POLYGLOT_SQL_FFI_PATH") == "" {
		t.Skip("POLYGLOT_SQL_FFI_PATH not set; native engine FFI lib unavailable")
	}
	st := network.NewInMemoryNetworkState()
	st.DatabaseInfos["db1"] = network.DatabaseInfo{DatabaseId: "db1"}

	f, err := NewSentioNetworkFactory(Options{
		Engine:           EngineNative,
		PhysicalDatabase: "phys",
		Listen:           ":9000",
	}, st)
	if err != nil {
		t.Fatalf("NewSentioNetworkFactory(native): %v", err)
	}
	defer f.Close()

	if err := f.ProbeStorageIntegrityBuild(context.Background()); err != nil {
		t.Fatalf("the pinned native engine failed its own startup build probe: %v", err)
	}
}
