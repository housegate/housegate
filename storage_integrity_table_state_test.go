package housegate

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
	"github.com/housegate/housegate/pkg/rewriter"
	"github.com/housegate/housegate/pkg/sitable"
)

func boolPtr(v bool) *bool { return &v }

func TestResolveStorageIntegrityTableState(t *testing.T) {
	fake := sitable.NewFake(sitable.Pending)
	for _, tc := range []struct {
		name     string
		enabled  *bool
		tables   []string
		injected sitable.TableState
		wantErr  string
		static   bool
		dynamic  bool
	}{
		{name: "disabled"},
		{name: "disabled with an injected state", injected: fake, wantErr: "requires storage_integrity.enabled: true"},
		{name: "tables default to enabled and static", tables: []string{"db1.t"}, static: true},
		{name: "explicit switch with an injected state", enabled: boolPtr(true), injected: fake, dynamic: true},
		{name: "both sources", enabled: boolPtr(true), tables: []string{"db1.t"}, injected: fake, wantErr: "not both"},
		{name: "neither source", enabled: boolPtr(true), wantErr: "requires a table-set source"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := minimalServerCfg(t)
			cfg.StorageIntegrity.Enabled = tc.enabled
			cfg.StorageIntegrity.Tables = tc.tables
			state, static, err := resolveStorageIntegrityTableState(Options{Config: cfg, StorageIntegrityTableState: tc.injected}, network.NewInMemoryNetworkState())
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.static:
				if static == nil || state != sitable.TableState(static) || state.Current().Lookup("db1", "t").Status != sitable.Active {
					t.Fatalf("state = %v static = %v, want Static with db1.t Active", state, static)
				}
			case tc.dynamic:
				if static != nil || state != sitable.TableState(fake) {
					t.Fatalf("state = %v static = %v, want the injected state", state, static)
				}
			default:
				if state != nil || static != nil {
					t.Fatalf("disabled must return no state, got %v %v", state, static)
				}
			}
		})
	}
}

// TestStaticSchemasPreferTheRuntimeSet pins that sitable.Static serves the
// runtime's authoritative startup schemas when the host supplies them.
func TestStaticSchemasPreferTheRuntimeSet(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"net1.events"}
	opts := Options{Config: cfg, StorageIntegrityRuntime: StorageIntegrityRuntimeOptions{TableSchemas: bpSchemas()}}
	_, static, err := resolveStorageIntegrityTableState(opts, network.NewInMemoryNetworkState())
	if err != nil {
		t.Fatal(err)
	}
	table, ok := static.Current().Schema("net1.events")
	if !ok || table.Schema.PartitionBy != "region" || table.SchemaHash == "" {
		t.Fatalf("static schema = %+v ok=%v, want the runtime set's schema", table, ok)
	}
}

// TestBuildServer_InjectedTableStateEnablesTheSurface pins spec 2026-09-24
// §6.2: an explicit switch with an injected state and no tables wires the
// fail-closed rewrite plugin over that state and the reserved-name guard.
func TestBuildServer_InjectedTableStateEnablesTheSurface(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Enabled = boolPtr(true)
	fake := sitable.NewFake(sitable.Pending)
	bs, err := buildServer(Options{
		Config:                     cfg,
		NetworkState:               network.NewInMemoryNetworkState(),
		Rewriter:                   siProbeStubRewriterFactory{},
		StorageIntegrityTableState: fake,
	}, nil)
	if err != nil {
		t.Fatalf("build with an injected table state: %v", err)
	}
	defer bs.teardown()
	var rw *rewrite.Plugin
	guarded := false
	for _, candidate := range requireExternalChain(t, bs).QueryPlugins {
		switch typed := candidate.(type) {
		case *rewrite.Plugin:
			rw = typed
		case *sireserved.Plugin:
			guarded = true
		}
	}
	if rw == nil || rw.TableState != sitable.TableState(fake) || !rw.FailClosedOnError || rw.RequiredStorageIntegrityContractVersion != rewriter.StorageIntegrityContractV2 {
		t.Fatalf("rewrite plugin = %+v, want fail-closed V2 over the injected state", rw)
	}
	if !guarded {
		t.Fatal("an enabled deployment must wire the reserved-name guard")
	}
}
