package testenv

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// MeshCA holds a self-signed CA + a single leaf cert shared by every
// housegate in the test mesh. PEM-encoded paths are written to a temp dir
// the test mounts into each imesh sidecar container.
type MeshCA struct {
	CAPath   string
	CertPath string
	KeyPath  string
}

// NewMeshCA generates a self-signed CA + one leaf cert covering every name
// peers might dial each other by (peer aliases, localhost, 127.0.0.1). The
// same leaf cert is used as both the mTLS server cert (Ingress) and client
// cert (Egress) — fine for the test, where every "housegate" presents the
// same identity; in production each housegate would have its own cert
// signed by the shared CA.
func NewMeshCA(t *testing.T, peerAliases ...string) *MeshCA {
	t.Helper()
	dir := t.TempDir()

	caPub, caPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ca keygen: %v", err)
	}
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "housegate-test-mesh-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(2 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, caPub, caPriv)
	if err != nil {
		t.Fatalf("create ca: %v", err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	caCert, _ := x509.ParseCertificate(caDER)

	leafPub, leafPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("leaf keygen: %v", err)
	}
	dns := append([]string{"localhost"}, peerAliases...)
	leafTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "housegate-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(2 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("127.0.0.2"), net.ParseIP("::1")},
		DNSNames:     dns,
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTmpl, caCert, leafPub, caPriv)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})

	keyDER, err := x509.MarshalPKCS8PrivateKey(leafPriv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	ca := &MeshCA{
		CAPath:   filepath.Join(dir, "ca.crt"),
		CertPath: filepath.Join(dir, "mesh.crt"),
		KeyPath:  filepath.Join(dir, "mesh.key"),
	}
	for path, data := range map[string][]byte{
		ca.CAPath:   caPEM,
		ca.CertPath: leafPEM,
		ca.KeyPath:  keyPEM,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	return ca
}

// StartInterserverMeshSidecar runs the imesh binary in a container that
// SHARES the target ClickHouse container's network namespace
// (NetworkMode container:<chContainerName>) — so CH's outbound dial of
// localhost:9010 lands on this sidecar's egress, and peers reaching this
// CH on the network alias hit the sidecar's ingress.
//
// peerRoutes maps each peer replica name (the key in CH's
// /replicas/<replica>/ keeper path) to the host:port of that peer's
// Ingress (e.g. "ch-2:19009").
func StartInterserverMeshSidecar(t *testing.T, chContainerName string, ca *MeshCA, peerRoutes map[string]string) {
	t.Helper()
	ctx := context.Background()
	bin := runfileBinary(t, "pkg/integration/testenv/cmd/imesh/imesh_/imesh")

	peerArgs := make([]string, 0, len(peerRoutes))
	for k, v := range peerRoutes {
		peerArgs = append(peerArgs, fmt.Sprintf("%s=%s", k, v))
	}

	const sidecarCertDir = "/etc/imesh"
	req := testcontainers.ContainerRequest{
		Image: "alpine:3.20",
		Files: []testcontainers.ContainerFile{
			{HostFilePath: bin, ContainerFilePath: "/imesh", FileMode: 0o755},
			{HostFilePath: ca.CertPath, ContainerFilePath: sidecarCertDir + "/mesh.crt", FileMode: 0o644},
			{HostFilePath: ca.KeyPath, ContainerFilePath: sidecarCertDir + "/mesh.key", FileMode: 0o600},
			{HostFilePath: ca.CAPath, ContainerFilePath: sidecarCertDir + "/ca.crt", FileMode: 0o644},
		},
		Cmd: []string{"/imesh",
			"-egress-listen", "127.0.0.1:9010",
			"-ingress-listen", "0.0.0.0:19009",
			"-local-ch", "127.0.0.2:9010",
			"-cert", sidecarCertDir + "/mesh.crt",
			"-key", sidecarCertDir + "/mesh.key",
			"-ca", sidecarCertDir + "/ca.crt",
			"-peers", strings.Join(peerArgs, ","),
		},
		HostConfigModifier: func(hc *container.HostConfig) {
			// Share the CH container's network namespace — the whole
			// point of the sidecar: same lo, same external IP/aliases.
			hc.NetworkMode = container.NetworkMode("container:" + chContainerName)
		},
		WaitingFor: wait.ForLog("imesh: egress").WithStartupTimeout(30 * time.Second),
	}
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start imesh sidecar for %s: %v", chContainerName, err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })
}
