package replicationproxy

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newInterserverTestServer(t *testing.T, peerAuth *InterserverPeerAuth, options InterserverOptions) *InterserverServer {
	t.Helper()
	options.PeerAuth = peerAuth
	server, err := NewInterserverServer(options)
	if err != nil {
		t.Fatalf("NewInterserverServer: %v", err)
	}
	return server
}

func newInterserverRequest(t *testing.T, method string, url string, body string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	return req
}

func doInterserverRequest(t *testing.T, client *http.Client, req *http.Request) (*http.Response, string) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll response: %v", err)
	}
	return resp, string(body)
}

func hostPort(t *testing.T, rawURL string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, rawURL, nil)
	host := req.URL.Host
	if _, _, err := net.SplitHostPort(host); err != nil {
		t.Fatalf("test server URL %q host %q is not host:port: %v", rawURL, host, err)
	}
	return host
}

func requireHTTPHit(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for HTTP upstream request")
	}
}

func requireNoHTTPHit(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("unexpected HTTP upstream request")
	default:
	}
}
