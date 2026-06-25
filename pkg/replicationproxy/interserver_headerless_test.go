package replicationproxy

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInterserver_LocalEgressRoutesToOnlyPeer_whenNoRouteHeaderConfigured(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	peerSeen := make(chan struct{}, 1)
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.String() != "/replicas/path?part=abc" {
			t.Errorf("url=%s, want /replicas/path?part=abc", r.URL.String())
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
		Routes:        []InterserverRoute{{Peer: "42", TargetIndexerID: 42, Upstream: hostPort(t, peer.URL)}},
	})
	proxy := httptest.NewServer(server.Handler())
	defer proxy.Close()
	req := newInterserverRequest(t, http.MethodGet, proxy.URL+"/replicas/path?part=abc", "")

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

func TestInterserver_LocalEgressRejectsRemoteSource_whenNoRouteHeaderAndSingleRoute(t *testing.T) {
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
		Routes:        []InterserverRoute{{Peer: "42", TargetIndexerID: 42, Upstream: hostPort(t, peer.URL)}},
	})
	req := newInterserverRequest(t, http.MethodGet, "http://housegate.test/replicas/path?part=abc", "")
	req.RemoteAddr = "198.51.100.10:43125"
	rec := httptest.NewRecorder()

	// When
	server.Handler().ServeHTTP(rec, req)

	// Then
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%q, want 403", rec.Code, rec.Body.String())
	}
	requireNoHTTPHit(t, peerSeen)
}

func TestInterserver_LocalEgressRejectsAmbiguousRoute_whenNoRouteHeaderAndMultipleRoutes(t *testing.T) {
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
		Routes: []InterserverRoute{
			{Peer: "41", TargetIndexerID: 41, Upstream: reserveClosedTCPAddr(t)},
			{Peer: "42", TargetIndexerID: 42, Upstream: hostPort(t, peer.URL)},
		},
	})
	proxy := httptest.NewServer(server.Handler())
	defer proxy.Close()
	req := newInterserverRequest(t, http.MethodGet, proxy.URL+"/replicas/path", "")

	// When
	resp, body := doInterserverRequest(t, proxy.Client(), req)

	// Then
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d body=%q, want 400", resp.StatusCode, body)
	}
	if body != "replicationproxy: ambiguous interserver route\n" {
		t.Fatalf("body=%q, want ambiguous route error", body)
	}
	requireNoHTTPHit(t, peerSeen)
}
