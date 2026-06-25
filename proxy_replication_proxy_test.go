package housegate

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/config"
	"housegate/housegate/pkg/network"
	"housegate/housegate/pkg/replicationproxy"
)

func TestProxy_Run_ReplicationProxyForwardsLocalFakeUpstreams(t *testing.T) {
	keeperUpstreamAddr, keeperObserved := startFakeKeeperUpstream(t)
	peerSigner, err := auth.NewRelaySigner("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	if err != nil {
		t.Fatalf("NewRelaySigner: %v", err)
	}
	peerValidator := auth.NewEthValidator([]string{peerSigner.Address()}, time.Minute, true, false, "", nil)
	localInterserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("local method=%s, want GET", r.Method)
		}
		if r.URL.String() != "/replicas/inbound?part=local" {
			t.Errorf("local url=%s, want /replicas/inbound?part=local", r.URL.String())
		}
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("local-interserver-ok"))
	}))
	defer localInterserver.Close()
	peerSeen := make(chan struct{}, 1)
	peerInterserver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("peer method=%s, want GET", r.Method)
		}
		if r.URL.String() != "/replicas/path?part=abc" {
			t.Errorf("peer url=%s, want /replicas/path?part=abc", r.URL.String())
		}
		if _, err := peerValidator.ValidatePeerLogin(r.Header.Get(replicationproxy.DefaultInterserverPeerTokenHeader), "1001"); err != nil {
			t.Errorf("peer token validation: %v", err)
		}
		peerSeen <- struct{}{}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("peer-interserver-ok"))
	}))
	defer peerInterserver.Close()

	cfg := minimalRouterOnlyConfig(t)
	cfg.Listen = reserveClosedTCPAddr(t)
	cfg.IndexerID = 1000
	cfg.RelayPrivateKeyHex = "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	cfg.PeerTokenTTL = config.Duration{Duration: time.Minute}
	cfg.Auth.Enabled = true
	cfg.Auth.AllowedAddresses = []string{peerSigner.Address()}
	cfg.ReplicationProxy.Keeper.Enabled = true
	cfg.ReplicationProxy.Keeper.Listen = reserveClosedTCPAddr(t)
	cfg.ReplicationProxy.Keeper.Upstreams = []string{keeperUpstreamAddr}
	cfg.ReplicationProxy.Interserver.Enabled = true
	cfg.ReplicationProxy.Interserver.Listen = reserveClosedTCPAddr(t)
	cfg.ReplicationProxy.Interserver.LocalUpstream = hostPortFromURL(t, localInterserver.URL)
	cfg.ReplicationProxy.Interserver.Routes = []config.ReplicationProxyInterserverRoute{
		{Peer: "1001", Upstream: hostPortFromURL(t, peerInterserver.URL)},
	}

	p, err := New(Options{
		Config:       cfg,
		NetworkState: network.NewInMemoryNetworkState(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("Run returned unexpected error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run did not return after ctx cancel")
		}
	}()
	waitForProxyAddr(t, p)
	waitForTCPPort(t, cfg.ReplicationProxy.Interserver.Listen)

	keeperBody := keeperRoundTrip(t, cfg.ReplicationProxy.Keeper.Listen, "keeper-ping")
	if keeperBody != "keeper-pong" {
		t.Fatalf("keeper response=%q, want keeper-pong", keeperBody)
	}
	select {
	case got := <-keeperObserved:
		if got != "keeper-ping" {
			t.Fatalf("keeper upstream saw %q, want keeper-ping", got)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for keeper upstream observation")
	}

	httpClient := &http.Client{Timeout: 2 * time.Second}
	egressReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cfg.ReplicationProxy.Interserver.Listen+"/replicas/path?part=abc", nil)
	if err != nil {
		t.Fatalf("NewRequest egress: %v", err)
	}
	egressResp, egressBody := doHTTPRoundTrip(t, httpClient, egressReq)
	if egressResp.StatusCode != http.StatusAccepted {
		t.Fatalf("egress status=%d body=%q, want 202", egressResp.StatusCode, egressBody)
	}
	if egressBody != "peer-interserver-ok" {
		t.Fatalf("egress body=%q, want peer-interserver-ok", egressBody)
	}
	select {
	case <-peerSeen:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for peer interserver upstream")
	}

	inboundReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+cfg.ReplicationProxy.Interserver.Listen+"/replicas/inbound?part=local", nil)
	if err != nil {
		t.Fatalf("NewRequest inbound: %v", err)
	}
	token, err := peerSigner.SignPeerLogin("1000", time.Minute)
	if err != nil {
		t.Fatalf("SignPeerLogin: %v", err)
	}
	inboundReq.Header.Set(replicationproxy.DefaultInterserverPeerUserHeader, peerSigner.Address())
	inboundReq.Header.Set(replicationproxy.DefaultInterserverPeerTokenHeader, token)
	inboundResp, inboundBody := doHTTPRoundTrip(t, httpClient, inboundReq)
	if inboundResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("inbound status=%d body=%q, want 206", inboundResp.StatusCode, inboundBody)
	}
	if inboundBody != "local-interserver-ok" {
		t.Fatalf("inbound body=%q, want local-interserver-ok", inboundBody)
	}
}

func startFakeKeeperUpstream(t *testing.T) (string, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen fake keeper: %v", err)
	}
	observed := make(chan string, 1)
	go func() {
		defer ln.Close()
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		buf := make([]byte, len("keeper-ping"))
		if _, err := io.ReadFull(conn, buf); err != nil {
			return
		}
		observed <- string(buf)
		_, _ = conn.Write([]byte("keeper-pong"))
	}()
	return ln.Addr().String(), observed
}

func reserveClosedTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve tcp addr: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close reserved listener: %v", err)
	}
	return addr
}

func hostPortFromURL(t *testing.T, rawURL string) string {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse URL %q: %v", rawURL, err)
	}
	if _, _, err := net.SplitHostPort(parsed.Host); err != nil {
		t.Fatalf("URL %q host %q is not host:port: %v", rawURL, parsed.Host, err)
	}
	return parsed.Host
}

func waitForProxyAddr(t *testing.T, p Proxy) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Addr() != nil {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Addr() == nil 2s after Run started")
}

func waitForTCPPort(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		lastErr = err
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("tcp listener %s did not accept within 2s: %v", addr, lastErr)
}

func keeperRoundTrip(t *testing.T, addr string, payload string) string {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial keeper proxy: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Write([]byte(payload)); err != nil {
		t.Fatalf("write keeper payload: %v", err)
	}
	buf := make([]byte, len("keeper-pong"))
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read keeper response: %v", err)
	}
	return string(buf)
}

func doHTTPRoundTrip(t *testing.T, client *http.Client, req *http.Request) (*http.Response, string) {
	t.Helper()
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do %s: %v", req.URL.String(), err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll response: %v", err)
	}
	return resp, string(body)
}
