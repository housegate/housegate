package housegate

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/interserver"
	"housegate/housegate/pkg/keeper"
	"housegate/housegate/pkg/network"
)

// These cover the config→build.go→listener seam for the keeper_proxy and
// interserver_mesh blocks — what the kpx/imesh container tests bypass by
// constructing the proxy ServerConfig directly. They assert buildServer
// appends the right listeners (addr + concrete type) when the block is
// configured, and that the keeper-pool membership func is wired off the
// optional registry.KeeperPool capability.

func findListener(t *testing.T, bs *builtServer, label string) serverListener {
	t.Helper()
	for _, l := range bs.listeners {
		if l.Label == label {
			return l
		}
	}
	t.Fatalf("no %q listener among %d listeners", label, len(bs.listeners))
	return serverListener{}
}

func TestBuildServer_KeeperProxyListeners(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	cfg.KeeperProxy = config.KeeperProxyConfig{
		Shards: []config.KeeperShardConfig{
			{Name: "default", Listen: "127.0.0.1:0", Members: []string{"127.0.0.1:9181", "127.0.0.1:9182"}, Strategy: "leader_pref"},
			{Name: "shard_2", Listen: "127.0.0.1:0", Members: []string{"127.0.0.1:9183"}},
		},
	}
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	for _, sh := range cfg.KeeperProxy.Shards {
		l := findListener(t, bs, "keeper:"+sh.Name)
		if l.ListenAddr != sh.Listen {
			t.Errorf("shard %q listener addr = %q, want %q", sh.Name, l.ListenAddr, sh.Listen)
		}
		if _, ok := l.Runner.(*keeper.Server); !ok {
			t.Errorf("shard %q listener Runner is %T, want *keeper.Server", sh.Name, l.Runner)
		}
	}
}

func TestBuildServer_InterserverMeshListeners(t *testing.T) {
	cert, key, ca := writeMeshCerts(t)
	cfg := minimalRouterOnlyCfg(t)
	cfg.InterserverMesh = config.InterserverMeshConfig{
		EgressListen:       "127.0.0.1:0",
		IngressListen:      "127.0.0.1:0",
		LocalCHInterserver: "127.0.0.2:9010",
		TLS:                config.InterserverMeshTLS{CertFile: cert, KeyFile: key, CAFile: ca},
	}
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()

	eg := findListener(t, bs, "interserver_mesh_egress")
	if _, ok := eg.Runner.(*interserver.Egress); !ok {
		t.Errorf("egress listener Runner is %T, want *interserver.Egress", eg.Runner)
	}
	in := findListener(t, bs, "interserver_mesh_ingress")
	if _, ok := in.Runner.(*interserver.Ingress); !ok {
		t.Errorf("ingress listener Runner is %T, want *interserver.Ingress", in.Runner)
	}
}

func TestBuildServer_NoProxyListenersWhenDisabled(t *testing.T) {
	cfg := minimalRouterOnlyCfg(t)
	bs, err := buildServer(Options{Config: cfg, NetworkState: network.NewInMemoryNetworkState()}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	defer bs.teardown()
	for _, l := range bs.listeners {
		if strings.HasPrefix(l.Label, "keeper:") {
			t.Errorf("unexpected keeper shard listener %q when proxy is disabled", l.Label)
		}
		switch l.Label {
		case "interserver_mesh_egress", "interserver_mesh_ingress":
			t.Errorf("unexpected %q listener when mesh is disabled", l.Label)
		}
	}
}

func TestKeeperShardMembersFunc_FromNetworkState(t *testing.T) {
	ns := network.NewInMemoryNetworkState()
	ns.SetKeeperPool("default", "k1:9181", "k2:9181")
	ns.SetKeeperPool("shard_2", "k4:9181")

	f := keeperShardMembersFunc(ns, "default")
	if f == nil {
		t.Fatal("expected non-nil MembersFunc when NetworkState implements registry.KeeperPool")
	}
	got := f()
	if len(got) != 2 || got[0] != "k1:9181" || got[1] != "k2:9181" {
		t.Fatalf("default shard members = %v, want [k1:9181 k2:9181]", got)
	}

	// The func reflects live updates so a reconfig is picked up.
	ns.SetKeeperPool("default", "k1:9181", "k3:9181")
	got = f()
	if len(got) != 2 || got[1] != "k3:9181" {
		t.Fatalf("default shard members did not reflect SetKeeperPool update: %v", got)
	}

	// Shards are isolated: the second shard's members don't leak into the
	// first shard's lookup.
	g := keeperShardMembersFunc(ns, "shard_2")
	if got := g(); len(got) != 1 || got[0] != "k4:9181" {
		t.Fatalf("shard_2 members = %v, want [k4:9181]", got)
	}
}

// writeMeshCerts emits a self-signed CA + a leaf cert into t.TempDir() and
// returns their PEM paths. Used by TestBuildServer_InterserverMeshListeners
// so the buildServer path actually loads real keypair files; the cert is
// never used for an actual TLS handshake in these tests.
func writeMeshCerts(t *testing.T) (cert, key, ca string) {
	t.Helper()
	dir := t.TempDir()

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519 ca: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "housegate-build-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caPub, caPriv)
	if err != nil {
		t.Fatalf("create ca: %v", err)
	}
	caCert, _ := x509.ParseCertificate(caDER)

	leafPub, leafPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519 leaf: %v", err)
	}
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "housegate-build-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, leafPub, caPriv)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}

	keyDER, err := x509.MarshalPKCS8PrivateKey(leafPriv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	files := map[string][]byte{
		filepath.Join(dir, "ca.crt"):   pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		filepath.Join(dir, "mesh.crt"): pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}),
		filepath.Join(dir, "mesh.key"): pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}
	for path, data := range files {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return filepath.Join(dir, "mesh.crt"), filepath.Join(dir, "mesh.key"), filepath.Join(dir, "ca.crt")
}
