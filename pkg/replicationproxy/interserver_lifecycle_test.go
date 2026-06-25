package replicationproxy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestInterserver_ServeWaitsForActiveRequestDrain_whenContextCancelled(t *testing.T) {
	// Given
	peerAuth, _ := newInterserverPeerAuthCarrier(t, time.Minute)
	upstreamStarted := make(chan struct{}, 1)
	releaseUpstream := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamStarted <- struct{}{}
		<-releaseUpstream
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("drained"))
	}))
	defer peer.Close()
	local := httptest.NewServer(http.NotFoundHandler())
	defer local.Close()
	server := newInterserverTestServer(t, peerAuth, InterserverOptions{
		SelfIndexerID: 7,
		LocalUpstream: hostPort(t, local.URL),
		Routes:        []InterserverRoute{{Peer: "42", TargetIndexerID: 42, Upstream: hostPort(t, peer.URL)}},
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(ctx, ln)
	}()
	client := &http.Client{Timeout: 3 * time.Second}
	responseDone := make(chan string, 1)
	go func() {
		req := newInterserverRequest(t, http.MethodGet, "http://"+ln.Addr().String()+"/parts/drain", "")
		resp, body := doInterserverRequest(t, client, req)
		if resp.StatusCode != http.StatusOK {
			t.Errorf("status=%d body=%q, want 200", resp.StatusCode, body)
		}
		responseDone <- body
	}()
	requireHTTPHit(t, upstreamStarted)

	// When
	cancel()

	// Then
	var earlyServeErr error
	select {
	case err := <-serveDone:
		earlyServeErr = err
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseUpstream)
	if body := <-responseDone; body != "drained" {
		t.Fatalf("body=%q, want drained", body)
	}
	if earlyServeErr != nil {
		t.Fatalf("Serve returned before active request drained: %v", earlyServeErr)
	}
	if err := <-serveDone; err != context.Canceled {
		t.Fatalf("Serve error=%v, want context.Canceled", err)
	}
}
