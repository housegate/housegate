package replicationproxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInterserver_InboundRejectsInvalidPeerAuth_whenPeerAuthHeadersPresent(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	localSeen := make(chan struct{}, 1)
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		localSeen <- struct{}{}
		w.WriteHeader(http.StatusOK)
	}))
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes:        []InterserverRoute{{Peer: "peer-two", TargetIndexerID: 42, Upstream: reserveClosedTCPAddr(t)}},
	})
	proxy := httptest.NewServer(server.Handler())
	defer proxy.Close()
	req := newInterserverRequest(t, http.MethodGet, proxy.URL+"/replicas/path", "")
	req.Header.Set(DefaultInterserverPeerUserHeader, "0x0000000000000000000000000000000000000000")
	req.Header.Set(DefaultInterserverPeerTokenHeader, "invalid-token")

	// When
	resp, body := doInterserverRequest(t, proxy.Client(), req)

	// Then
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status=%d body=%q, want 401", resp.StatusCode, body)
	}
	requireNoHTTPHit(t, localSeen)
}

func TestInterserver_InboundForwardsToLocalUpstream_whenPeerAuthValid(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	localSeen := make(chan struct{}, 1)
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Errorf("method=%s, want PUT", r.Method)
		}
		if r.URL.Path != "/parts/fetch" || r.URL.RawQuery != "replica=a" {
			t.Errorf("url=%s, want /parts/fetch?replica=a", r.URL.String())
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		if string(body) != "fetch-body" {
			t.Errorf("body=%q, want fetch-body", body)
		}
		localSeen <- struct{}{}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("local-ok"))
	}))
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes:        []InterserverRoute{{Peer: "peer-two", TargetIndexerID: 42, Upstream: reserveClosedTCPAddr(t)}},
	})
	proxy := httptest.NewServer(server.Handler())
	defer proxy.Close()
	req := newInterserverRequest(t, http.MethodPut, proxy.URL+"/parts/fetch?replica=a", "fetch-body")
	if err := peerAuth.Attach(req, 7); err != nil {
		t.Fatalf("Attach inbound auth: %v", err)
	}

	// When
	resp, body := doInterserverRequest(t, proxy.Client(), req)

	// Then
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status=%d body=%q, want 201", resp.StatusCode, body)
	}
	if body != "local-ok" {
		t.Fatalf("body=%q, want local-ok", body)
	}
	requireHTTPHit(t, localSeen)
}

func TestInterserver_InboundForwardsToLocalUpstream_whenPeerAuthValidFromRemoteSource(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	localSeen := make(chan struct{}, 1)
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/parts/remote" {
			t.Errorf("url=%s, want /parts/remote", r.URL.String())
		}
		localSeen <- struct{}{}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("local-remote-ok"))
	}))
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes:        []InterserverRoute{{Peer: "peer-two", TargetIndexerID: 42, Upstream: reserveClosedTCPAddr(t)}},
	})
	req := newInterserverRequest(t, http.MethodGet, "http://housegate.test/parts/remote", "")
	req.RemoteAddr = "198.51.100.10:43125"
	if err := peerAuth.Attach(req, 7); err != nil {
		t.Fatalf("Attach inbound auth: %v", err)
	}
	rec := httptest.NewRecorder()

	// When
	server.Handler().ServeHTTP(rec, req)

	// Then
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%q, want 200", rec.Code, rec.Body.String())
	}
	if rec.Body.String() != "local-remote-ok" {
		t.Fatalf("body=%q, want local-remote-ok", rec.Body.String())
	}
	requireHTTPHit(t, localSeen)
}

func TestInterserver_PreservesUpstreamStatusAndBody_whenLocalUpstreamReturns500(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("clickhouse failed"))
	}))
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes:        []InterserverRoute{{Peer: "peer-two", TargetIndexerID: 42, Upstream: reserveClosedTCPAddr(t)}},
	})
	proxy := httptest.NewServer(server.Handler())
	defer proxy.Close()
	req := newInterserverRequest(t, http.MethodGet, proxy.URL+"/parts/failing", "")
	if err := peerAuth.Attach(req, 7); err != nil {
		t.Fatalf("Attach inbound auth: %v", err)
	}

	// When
	resp, body := doInterserverRequest(t, proxy.Client(), req)

	// Then
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status=%d body=%q, want 500", resp.StatusCode, body)
	}
	if body != "clickhouse failed" {
		t.Fatalf("body=%q, want clickhouse failed", body)
	}
}
