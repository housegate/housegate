package replicationproxy

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInterserver_LocalEgressRoutesToPeerAndAttachesAuth_whenPeerRouteConfigured(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	peerSeen := make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method=%s, want POST", r.Method)
		}
		if r.URL.Path != "/replicas/path" || r.URL.RawQuery != "part=1&checksum=abc" {
			t.Errorf("url=%s, want /replicas/path?part=1&checksum=abc", r.URL.String())
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if string(body) != "part-body" {
			t.Errorf("body=%q, want part-body", body)
		}
		if r.Header.Get(DefaultInterserverPeerUserHeader) == "" {
			t.Error("peer user header is empty")
		}
		if r.Header.Get(DefaultInterserverPeerTokenHeader) == "" {
			t.Error("peer token header is empty")
		}
		if _, err := peerAuth.Validate(r, 42); err != nil {
			t.Errorf("validate peer auth at peer: %v", err)
		}
		peerSeen <- struct{}{}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("peer-ok"))
	}))
	defer peer.Close()
	local := httptest.NewServer(http.NotFoundHandler())
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes: []InterserverRoute{
			{Peer: "peer-one", TargetIndexerID: 41, Upstream: reserveClosedTCPAddr(t)},
			{Peer: "peer-two", TargetIndexerID: 42, Upstream: hostPort(t, peer.URL)},
		},
	})
	proxy := httptest.NewServer(server.Handler())
	defer proxy.Close()
	req := newInterserverRequest(t, http.MethodPost, proxy.URL+"/replicas/path?part=1&checksum=abc", "part-body")
	req.Header.Set(DefaultInterserverRouteHeader, "peer-two")

	// When
	resp, body := doInterserverRequest(t, proxy.Client(), req)

	// Then
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d body=%q, want 202", resp.StatusCode, body)
	}
	if body != "peer-ok" {
		t.Fatalf("body=%q, want peer-ok", body)
	}
	requireHTTPHit(t, peerSeen)
}

func TestInterserver_LocalEgressBypassesEnvironmentProxy_whenPeerAuthAttached(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	proxySeen := make(chan struct{}, 1)
	envProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxySeen <- struct{}{}
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer envProxy.Close()
	t.Setenv("HTTP_PROXY", envProxy.URL)
	t.Setenv("HTTPS_PROXY", envProxy.URL)
	t.Setenv("http_proxy", envProxy.URL)
	t.Setenv("https_proxy", envProxy.URL)
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")

	peerSeen := make(chan struct{}, 1)
	peer, peerUpstream := newRoutableInterserverPeer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := peerAuth.Validate(r, 42); err != nil {
			t.Errorf("validate peer auth at peer: %v", err)
		}
		peerSeen <- struct{}{}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("peer-ok"))
	}))
	defer peer.Close()
	local := httptest.NewServer(http.NotFoundHandler())
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes:        []InterserverRoute{{Peer: "peer-two", TargetIndexerID: 42, Upstream: peerUpstream}},
	})
	housegate := httptest.NewServer(server.Handler())
	defer housegate.Close()
	client := &http.Client{Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	req := newInterserverRequest(t, http.MethodGet, housegate.URL+"/replicas/path", "")

	// When
	resp, body := doInterserverRequest(t, client, req)

	// Then
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status=%d body=%q, want 202", resp.StatusCode, body)
	}
	if body != "peer-ok" {
		t.Fatalf("body=%q, want peer-ok", body)
	}
	requireHTTPHit(t, peerSeen)
	requireNoHTTPHit(t, proxySeen)
}

func newRoutableInterserverPeer(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	for _, ip := range nonLoopbackIPv4Addrs(t) {
		ln, err := net.Listen("tcp", net.JoinHostPort(ip.String(), "0"))
		if err != nil {
			continue
		}
		server := httptest.NewUnstartedServer(handler)
		server.Listener = ln
		server.Start()
		return server, ln.Addr().String()
	}
	t.Fatal("no routable local IPv4 address available for environment proxy regression")
	return nil, ""
}

func nonLoopbackIPv4Addrs(t *testing.T) []net.IP {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("Interfaces: %v", err)
	}
	var ips []net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			t.Fatalf("Interface %s Addrs: %v", iface.Name, err)
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipNet.IP.To4()
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ips = append(ips, ip)
		}
	}
	return ips
}

func TestInterserver_LocalEgressRejectsInvalidRoute_whenPeerUnknown(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	peerSeen := make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerSeen <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer peer.Close()
	local := httptest.NewServer(http.NotFoundHandler())
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes:        []InterserverRoute{{Peer: "peer-two", TargetIndexerID: 42, Upstream: hostPort(t, peer.URL)}},
	})
	proxy := httptest.NewServer(server.Handler())
	defer proxy.Close()
	req := newInterserverRequest(t, http.MethodGet, proxy.URL+"/replicas/path", "")
	req.Header.Set(DefaultInterserverRouteHeader, "missing-peer")

	// When
	resp, body := doInterserverRequest(t, proxy.Client(), req)

	// Then
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q, want 400", resp.StatusCode, body)
	}
	requireNoHTTPHit(t, peerSeen)
}

func TestInterserver_LocalEgressRejectsRemoteSource_whenRouteHeaderConfigured(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	peerSeen := make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		peerSeen <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer peer.Close()
	local := httptest.NewServer(http.NotFoundHandler())
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes:        []InterserverRoute{{Peer: "peer-two", TargetIndexerID: 42, Upstream: hostPort(t, peer.URL)}},
	})
	req := newInterserverRequest(t, http.MethodGet, "http://housegate.test/replicas/path", "")
	req.RemoteAddr = "198.51.100.10:43125"
	req.Header.Set(DefaultInterserverRouteHeader, "peer-two")
	rec := httptest.NewRecorder()

	// When
	server.Handler().ServeHTTP(rec, req)

	// Then
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%q, want 403", rec.Code, rec.Body.String())
	}
	requireNoHTTPHit(t, peerSeen)
}
