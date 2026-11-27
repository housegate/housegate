package endpointtls_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/housegate/housegate/pkg/endpointtls"
)

const chainID = 7892301

var bigOne = big.NewInt(1)

func signerKey(t *testing.T) (*ecdsa.PrivateKey, string) {
	t.Helper()
	k, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	return k, strings.ToLower(crypto.PubkeyToAddress(k.PublicKey).Hex())
}

func signerCert(t *testing.T, k *ecdsa.PrivateKey, chain uint64) tls.Certificate {
	t.Helper()
	c, err := endpointtls.NewSignerCertificate(k, chain, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSignerCertificateRoundTrip(t *testing.T) {
	k, addr := signerKey(t)
	c := signerCert(t, k, chainID)
	if c.Leaf.PublicKeyAlgorithm != x509.ECDSA {
		t.Fatalf("key algorithm %v", c.Leaf.PublicKeyAlgorithm)
	}
	got, err := endpointtls.CertificateSigner(c.Leaf, chainID, time.Now())
	if err != nil || got != addr {
		t.Fatalf("signer %q, %v; want %q", got, err, addr)
	}
	if _, err := endpointtls.CertificateSigner(c.Leaf, chainID+1, time.Now()); err == nil || !strings.Contains(err.Error(), "for chain") {
		t.Fatalf("another chain: %v", err)
	}
	if _, err := endpointtls.CertificateSigner(c.Leaf, chainID, time.Now().Add(2*time.Hour)); err == nil || !strings.Contains(err.Error(), "valid from") {
		t.Fatalf("expired: %v", err)
	}
}

// selfSigned returns a P-256 certificate carrying uris, signed by its own key.
func selfSigned(t *testing.T, uris ...*url.URL) *x509.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: bigOne, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), URIs: uris}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return leaf
}

func TestCertificateSignerRefusals(t *testing.T) {
	k, addr := signerKey(t)
	good := signerCert(t, k, chainID).Leaf
	uri := good.URIs[0]

	// The signer URI of one certificate on another key recovers some other
	// address: the signature binds the public key.
	if got, err := endpointtls.CertificateSigner(selfSigned(t, uri), chainID, time.Now()); err == nil && got == addr {
		t.Fatal("a signer URI moved onto another key still recovers the signer")
	}
	if _, err := endpointtls.CertificateSigner(selfSigned(t), chainID, time.Now()); err == nil || !strings.Contains(err.Error(), "no signer URI") {
		t.Fatalf("no URI: %v", err)
	}
	if _, err := endpointtls.CertificateSigner(selfSigned(t, uri, uri), chainID, time.Now()); err == nil || !strings.Contains(err.Error(), "more than one") {
		t.Fatalf("two URIs: %v", err)
	}
	for _, bad := range []string{
		"urn:housegate:endpoint-signer:v1:7892301",
		"urn:housegate:endpoint-signer:v1:x:0x00",
		"urn:housegate:endpoint-signer:v1:7892301:00",
		"urn:housegate:endpoint-signer:v1:7892301:0x1234",
	} {
		u, err := url.Parse(bad)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := endpointtls.CertificateSigner(selfSigned(t, u), chainID, time.Now()); err == nil || !strings.Contains(err.Error(), "malformed") {
			t.Errorf("%s: %v", bad, err)
		}
	}
	if _, err := endpointtls.CertificateSigner(nil, chainID, time.Now()); err == nil {
		t.Fatal("nil certificate accepted")
	}
}

func TestNewSignerRequiresChainAndLookup(t *testing.T) {
	lookup := func(string) (string, error) { return "", nil }
	if _, err := endpointtls.NewSigner(0, lookup); err == nil {
		t.Fatal("chain id 0 accepted")
	}
	if _, err := endpointtls.NewSigner(chainID, nil); err == nil {
		t.Fatal("nil lookup accepted")
	}
}

func serveSignerTLS(t *testing.T, cert tls.Certificate) string {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	serveEcho(t, ln)
	return ln.Addr().String()
}

func TestSignerModeDial(t *testing.T) {
	k, addr := signerKey(t)
	_, other := signerKey(t)
	target := serveSignerTLS(t, signerCert(t, k, chainID))

	for _, tc := range []struct {
		name   string
		lookup endpointtls.SignerLookup
		want   string
	}{
		{"registered signer", func(a string) (string, error) {
			if a != target {
				return "", errors.New("asked for " + a)
			}
			return strings.ToUpper(addr[:2]) + addr[2:], nil
		}, ""},
		{"another signer", func(string) (string, error) { return other, nil }, "the network state registers " + other},
		{"no signer registered", func(string) (string, error) { return "", nil }, "registers no signer"},
		{"lookup fails", func(string) (string, error) { return "", errors.New("state down") }, "state down"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := endpointtls.NewSigner(chainID, tc.lookup)
			if err != nil {
				t.Fatal(err)
			}
			if !cfg.Enabled() || !cfg.SignerMode() {
				t.Fatal("signer config not enabled")
			}
			c, err := cfg.DialConn(context.Background(), target, 2*time.Second)
			if tc.want == "" {
				if err != nil {
					t.Fatalf("dial: %v", err)
				}
				defer c.Close()
				roundTrip(t, c)
				return
			}
			if err == nil {
				_ = c.Close()
				t.Fatal("dial succeeded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err %v, want %q", err, tc.want)
			}
		})
	}

	// A certificate for another chain is refused even for the right signer.
	wrongChain := serveSignerTLS(t, signerCert(t, k, chainID+1))
	cfg, _ := endpointtls.NewSigner(chainID, func(string) (string, error) { return addr, nil })
	if _, err := cfg.Dial(context.Background(), wrongChain, 2*time.Second); err == nil || !strings.Contains(err.Error(), "for chain") {
		t.Fatalf("wrong chain: %v", err)
	}

	// The hostname mode refuses the self-signed certificate outright.
	host, err := endpointtls.New("")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := host.Dial(context.Background(), target, 2*time.Second); err == nil {
		t.Fatal("hostname mode accepted a signer certificate")
	}
}

func TestSignerModeHTTPClient(t *testing.T) {
	k, addr := signerKey(t)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{signerCert(t, k, chainID)}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	target := strings.TrimPrefix(srv.URL, "https://")

	var asked []string
	cfg, err := endpointtls.NewSigner(chainID, func(a string) (string, error) { asked = append(asked, a); return addr, nil })
	if err != nil {
		t.Fatal(err)
	}
	resp, err := cfg.HTTPClient(2 * time.Second).Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	_ = resp.Body.Close()
	if len(asked) == 0 || asked[0] != target {
		t.Fatalf("lookup asked %v, want %s", asked, target)
	}

	if tr, ok := cfg.HTTPClient(time.Second).Transport.(*http.Transport); !ok || tr.Proxy != nil || tr.DialTLSContext == nil {
		t.Fatal("the signer mode HTTP client must not use a proxy and must dial TLS itself")
	}

	_, other := signerKey(t)
	refuse, _ := endpointtls.NewSigner(chainID, func(string) (string, error) { return other, nil })
	if _, err := refuse.HTTPClient(2 * time.Second).Get(srv.URL); err == nil || !strings.Contains(err.Error(), "presented a certificate of signer") {
		t.Fatalf("another signer: %v", err)
	}
}

// TestSignerModeHTTPClientBoundsAStalledHandshake: a server that accepts and
// never answers the ClientHello has its connection closed once the client's
// timeout expires, not left open after the request returns.
func TestSignerModeHTTPClientBoundsAStalledHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	closed := make(chan struct{})
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = io.Copy(io.Discard, c) // returns when the client closes
		close(closed)
	}()
	cfg, _ := endpointtls.NewSigner(chainID, func(string) (string, error) { return "0x01", nil })
	if _, err := cfg.HTTPClient(100 * time.Millisecond).Get("https://" + ln.Addr().String()); err == nil {
		t.Fatal("request to a stalled server succeeded")
	}
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("the stalled TLS dial kept its connection open")
	}
}

func TestSignerModeRedialKeepsTheConfiguredName(t *testing.T) {
	k, addr := signerKey(t)
	target := serveSignerTLS(t, signerCert(t, k, chainID))
	var asked []string
	cfg, _ := endpointtls.NewSigner(chainID, func(a string) (string, error) { asked = append(asked, a); return addr, nil })
	c, err := cfg.DialConn(context.Background(), target, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	r, err := c.RedialExact(context.Background(), 2*time.Second)
	if err != nil {
		t.Fatalf("redial: %v", err)
	}
	defer r.Close()
	if len(asked) != 2 || asked[1] != target {
		t.Fatalf("lookups %v", asked)
	}
	_ = net.Conn(r)
}
