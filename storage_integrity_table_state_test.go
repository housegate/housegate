package housegate

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/network"
	"github.com/housegate/housegate/pkg/plugins/commitgate"
	"github.com/housegate/housegate/pkg/plugins/rewrite"
	"github.com/housegate/housegate/pkg/plugins/sireserved"
	"github.com/housegate/housegate/pkg/plugins/sitablestate"
	"github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/registry"
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

// TestBuildServer_TableStateGateWiring is spec 2026-09-24 §7.1: sitablestate
// runs after rewrite and before the SI ingress and commitgate.
func TestBuildServer_TableStateGateWiring(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Enabled = boolPtr(true)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	fake := sitable.NewFake(sitable.Pending)
	bs, err := buildServer(Options{
		Config:                            cfg,
		NetworkState:                      network.NewInMemoryNetworkState(),
		Rewriter:                          siProbeStubRewriterFactory{},
		StorageIntegrityTableState:        fake,
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err != nil {
		t.Fatalf("build with an injected table state: %v", err)
	}
	defer bs.teardown()
	rewriteAt, gateAt, ingressAt, commitAt := -1, -1, -1, -1
	for i, candidate := range requireExternalChain(t, bs).QueryPlugins {
		switch candidate.(type) {
		case *rewrite.Plugin:
			rewriteAt = i
		case *sitablestate.Plugin:
			gateAt = i
		case *storageintegrity.Plugin:
			ingressAt = i
		case *commitgate.Plugin:
			commitAt = i
		}
	}
	if !(rewriteAt >= 0 && rewriteAt < gateAt && gateAt < ingressAt && ingressAt < commitAt) {
		t.Fatalf("plugin order rewrite=%d sitablestate=%d ingress=%d commitgate=%d, want strictly increasing", rewriteAt, gateAt, ingressAt, commitAt)
	}
}

func TestBuildServer_DisabledWiresNoTableStateGate(t *testing.T) {
	bs, err := buildServer(Options{
		Config:       minimalServerCfg(t),
		NetworkState: network.NewInMemoryNetworkState(),
		Rewriter:     stubRewriterFactory{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.teardown()
	for _, candidate := range requireExternalChain(t, bs).QueryPlugins {
		if _, ok := candidate.(*sitablestate.Plugin); ok {
			t.Fatal("a disabled deployment must not wire sitablestate")
		}
		if p, ok := candidate.(*rewrite.Plugin); ok && p.TableState != nil {
			t.Fatal("a disabled deployment must not give the rewrite plugin a table state")
		}
	}
}

// registryOnly exposes only registry.Registry, hiding the declared-schema
// view the in-memory state also implements.
type registryOnly struct{ registry.Registry }

// TestBuildServer_EnabledIngressNeedsNoDeclaredSchemaSource pins spec
// 2026-09-24 §9.1: with storage integrity enabled the ingress binds the
// query snapshot's schema, so it needs no registry.TableSchemas source; the
// disabled ingress still does.
func TestBuildServer_EnabledIngressNeedsNoDeclaredSchemaSource(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Enabled = boolPtr(true)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	bs, err := buildServer(Options{
		Config:                            cfg,
		NetworkState:                      registryOnly{network.NewInMemoryNetworkState()},
		Rewriter:                          siProbeStubRewriterFactory{},
		StorageIntegrityTableState:        sitable.NewFake(sitable.Pending),
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err != nil {
		t.Fatalf("an enabled ingress must build without a declared-schema source: %v", err)
	}
	bs.teardown()

	cfg.StorageIntegrity.Enabled = nil
	_, err = buildServer(Options{
		Config:                            cfg,
		NetworkState:                      registryOnly{network.NewInMemoryNetworkState()},
		Rewriter:                          stubRewriterFactory{},
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err == nil || !strings.Contains(err.Error(), "implements registry.TableSchemas") {
		t.Fatalf("err = %v, want the disabled ingress to require a declared-schema source", err)
	}
}

// TestBuildServer_EnabledIngressRequiresSnapshotAndIgnoresDeclaredSchemas pins
// that an enabled build hands the ingress no declared-schema source even when
// the NetworkState offers one, and requires every query's snapshot.
func TestBuildServer_EnabledIngressRequiresSnapshotAndIgnoresDeclaredSchemas(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Enabled = boolPtr(true)
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	ns := network.NewInMemoryNetworkState()
	var _ registry.TableSchemas = ns // the declared source is available
	bs, err := buildServer(Options{
		Config:                            cfg,
		NetworkState:                      ns,
		Rewriter:                          siProbeStubRewriterFactory{},
		StorageIntegrityTableState:        sitable.NewFake(sitable.Pending),
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.teardown()
	var ingress *storageintegrity.Plugin
	for _, candidate := range requireExternalChain(t, bs).QueryPlugins {
		if p, ok := candidate.(*storageintegrity.Plugin); ok {
			ingress = p
		}
	}
	if ingress == nil {
		t.Fatal("the enabled build did not wire the ingress")
	}
	if !ingress.RequiresTableSnapshot() {
		t.Fatal("an enabled build must require the table-state snapshot")
	}
	if ingress.ResolvesDeclaredSchemas() {
		t.Fatal("an enabled build must not give the ingress a declared-schema source")
	}
}

// TestStaticIngressRequiresADeclaredSchemaSource pins final ruling I3: static
// tables with the ingress enabled and no runtime schema set refuse startup
// when no declared-schema source can be resolved, as before the dynamic table
// set; a single table missing its declaration only warns.
func TestStaticIngressRequiresADeclaredSchemaSource(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"db1.t"}
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"

	_, _, err := resolveStorageIntegrityTableState(Options{Config: cfg}, registryOnly{network.NewInMemoryNetworkState()})
	if err == nil || !strings.Contains(err.Error(), "implements registry.TableSchemas") {
		t.Fatalf("err = %v, want startup refused without a declared-schema source", err)
	}

	_, static, err := resolveStorageIntegrityTableState(Options{Config: cfg}, network.NewInMemoryNetworkState())
	if err != nil {
		t.Fatalf("a resolvable source with an undeclared table must only warn: %v", err)
	}
	if table, _ := static.Current().Schema("db1.t"); table.SchemaHash != "" {
		t.Fatalf("an undeclared table must stay schema-less, got %+v", table)
	}

	cfg.StorageIntegrity.Ingress.Enabled = false
	if _, _, err := resolveStorageIntegrityTableState(Options{Config: cfg}, registryOnly{network.NewInMemoryNetworkState()}); err != nil {
		t.Fatalf("without the ingress no declared-schema source is needed: %v", err)
	}
}

// TestBuildServer_StaticIngressWithoutDeclaredSchemasFailsFast is the same
// rule through buildServer: startup fails instead of refusing every signed
// INSERT later.
func TestBuildServer_StaticIngressWithoutDeclaredSchemasFailsFast(t *testing.T) {
	cfg := minimalServerCfg(t)
	cfg.StorageIntegrity.Tables = []string{"db1.t"}
	cfg.StorageIntegrity.Ingress.Enabled = true
	cfg.StorageIntegrity.Ingress.NetworkID = "testnet-v2"
	cfg.StorageIntegrity.Ingress.AllowedAddresses = []string{"0x1111111111111111111111111111111111111111"}
	bs, err := buildServer(Options{
		Config:                            cfg,
		NetworkState:                      registryOnly{network.NewInMemoryNetworkState()},
		Rewriter:                          siProbeStubRewriterFactory{},
		StorageIntegrityAdmissionConsumer: &recordingAdmissionConsumer{},
	}, nil)
	if err == nil {
		bs.teardown()
		t.Fatal("buildServer succeeded, want startup refused without a declared-schema source")
	}
	if !strings.Contains(err.Error(), "implements registry.TableSchemas") {
		t.Fatalf("err = %v, want the declared-schema source error", err)
	}
}
