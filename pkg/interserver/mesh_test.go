package interserver

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestExtractReplica(t *testing.T) {
	cases := map[string]string{
		"/?endpoint=DataPartsExchange:/clickhouse/tables/01/db/tbl/replicas/ch-1/&part=p":   "ch-1",
		"/?endpoint=DataPartsExchange:/clickhouse/tables/01/db/tbl/replicas/replica_alpha/": "replica_alpha",
		"/?endpoint=DataPartsExchange:/replicas/x/&part=p&compress=true":                    "x",
		"/?endpoint=": "",
		"/":           "",
		"/?endpoint=DataPartsExchange:/clickhouse/no-marker": "",
	}
	for raw, want := range cases {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse %q: %v", raw, err)
		}
		if got := extractReplica(u); got != want {
			t.Errorf("extractReplica(%q) = %q, want %q", raw, got, want)
		}
	}
}

// --- self-signed CA + leaf-cert helpers ---------------------------------

func newTestCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519 ca: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "housegate-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}
	ca, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse ca: %v", err)
	}
	return ca, priv
}

func newTestLeaf(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, cn string) tls.Certificate {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("ed25519 leaf: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, pub, caKey)
	if err != nil {
		t.Fatalf("create leaf: %v", err)
	}
	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  priv,
	}
}

func caPool(ca *x509.Certificate) *x509.CertPool {
	p := x509.NewCertPool()
	p.AddCert(ca)
	return p
}

// --- end-to-end mesh round-trip ----------------------------------------

func TestMeshEgressIngressRoundTrip(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fake "local CH" HTTP backend the ingress forwards to.
	const partBody = "PART-DATA-XYZ-1234567890"
	gotEndpoint := ""
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEndpoint = r.URL.Query().Get("endpoint")
		fmt.Fprint(w, partBody)
	}))
	defer backend.Close()
	backendAddr := backend.Listener.Addr().String()

	// One CA shared by both legs; one leaf cert acts as this housegate's
	// identity (used as ingress server cert AND egress client cert).
	ca, caKey := newTestCA(t)
	cert := newTestLeaf(t, ca, caKey, "housegate-test")
	pool := caPool(ca)

	ingress, err := NewIngress(IngressConfig{
		LocalCH: backendAddr,
		TLSServer: &tls.Config{
			Certificates: []tls.Certificate{cert},
			ClientCAs:    pool,
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		},
	})
	if err != nil {
		t.Fatalf("NewIngress: %v", err)
	}
	ingLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("ingress listen: %v", err)
	}
	go func() { _ = ingress.Serve(ctx, ingLn) }()
	ingAddr := ingLn.Addr().String()

	// Egress: its PeerLookup maps "ch-1" → the ingress address.
	egress, err := NewEgress(EgressConfig{
		PeerLookup: func(replica string) (string, bool) {
			if replica == "ch-1" {
				return ingAddr, true
			}
			return "", false
		},
		TLSClient: &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      pool,
			MinVersion:   tls.VersionTLS12,
		},
	})
	if err != nil {
		t.Fatalf("NewEgress: %v", err)
	}
	egLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("egress listen: %v", err)
	}
	go func() { _ = egress.Serve(ctx, egLn) }()
	egAddr := egLn.Addr().String()

	// Simulate CH outbound: HTTP GET on the egress with an interserver
	// endpoint URL whose source replica is ch-1.
	const endpoint = "DataPartsExchange:/clickhouse/tables/01/db/tbl/replicas/ch-1/"
	reqURL := "http://" + egAddr + "/?endpoint=" + url.QueryEscape(endpoint) + "&part=p1"
	resp, err := http.Get(reqURL)
	if err != nil {
		t.Fatalf("GET via egress: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != partBody {
		t.Fatalf("relayed body = %q, want %q", body, partBody)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	// Sanity: the original endpoint query made it through both hops to the
	// backend (proves both ReverseProxies preserved request data).
	if gotEndpoint != endpoint {
		t.Fatalf("backend saw endpoint=%q, want %q", gotEndpoint, endpoint)
	}

	if egress.Served() < 1 {
		t.Errorf("egress.Served() = %d, want >= 1", egress.Served())
	}
	if ingress.Served() < 1 {
		t.Errorf("ingress.Served() = %d, want >= 1", ingress.Served())
	}
}

func TestMeshIngressRejectsNoClientCert(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("backend must NOT receive a request from an unauthenticated client")
	}))
	defer backend.Close()

	ca, caKey := newTestCA(t)
	serverCert := newTestLeaf(t, ca, caKey, "housegate-test")

	ingress, err := NewIngress(IngressConfig{
		LocalCH: backend.Listener.Addr().String(),
		TLSServer: &tls.Config{
			Certificates: []tls.Certificate{serverCert},
			ClientCAs:    caPool(ca),
			ClientAuth:   tls.RequireAndVerifyClientCert,
			MinVersion:   tls.VersionTLS12,
		},
	})
	if err != nil {
		t.Fatalf("NewIngress: %v", err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = ingress.Serve(ctx, ln) }()

	// Client trusts the CA but presents NO cert — the ingress must reject
	// at the TLS handshake before any request body is exchanged.
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs:    caPool(ca),
		MinVersion: tls.VersionTLS12,
	}}}
	resp, err := client.Get("https://" + ln.Addr().String() + "/")
	if err == nil {
		resp.Body.Close()
		t.Fatalf("expected mTLS to reject client without cert, got status %d", resp.StatusCode)
	}
	if !strings.Contains(err.Error(), "certificate") && !strings.Contains(err.Error(), "tls") {
		t.Fatalf("expected TLS/cert error, got %v", err)
	}
}

func TestMeshEgressUnknownPeerReturns502(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ca, caKey := newTestCA(t)
	cert := newTestLeaf(t, ca, caKey, "housegate-test")

	egress, err := NewEgress(EgressConfig{
		PeerLookup: func(string) (string, bool) { return "", false },
		TLSClient: &tls.Config{
			Certificates: []tls.Certificate{cert},
			RootCAs:      caPool(ca),
			MinVersion:   tls.VersionTLS12,
		},
	})
	if err != nil {
		t.Fatalf("NewEgress: %v", err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	go func() { _ = egress.Serve(ctx, ln) }()

	reqURL := "http://" + ln.Addr().String() +
		"/?endpoint=" + url.QueryEscape("DataPartsExchange:/clickhouse/tables/01/db/tbl/replicas/missing/")
	resp, err := http.Get(reqURL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if egress.Rejected() < 1 {
		t.Errorf("egress.Rejected() = %d, want >= 1", egress.Rejected())
	}
}
