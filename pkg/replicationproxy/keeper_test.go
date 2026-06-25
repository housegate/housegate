package replicationproxy

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestKeeper_ForwardsBytesByteForByte_whenUpstreamAvailable(t *testing.T) {
	// Given
	upstream := startKeeperEchoUpstream(t)
	proxy := startKeeperProxy(t, KeeperOptions{
		Upstreams:   []string{upstream.addr},
		DialTimeout: 200 * time.Millisecond,
	})
	conn := dialKeeperProxy(t, proxy.addr)
	defer conn.Close()

	// When
	payload := []byte("keeper-ping-123")
	writeAll(t, conn, payload)
	got := readExactly(t, conn, len(payload))

	// Then
	if string(got) != string(payload) {
		t.Fatalf("echoed payload=%q, want %q", got, payload)
	}
	if string(receiveBytes(t, upstream.observed)) != string(payload) {
		t.Fatalf("upstream did not observe the exact client payload")
	}
	requireAccepted(t, upstream.accepted)
}

func TestKeeper_FallsBackToSecondHealthyUpstream_whenFirstUnavailableForNewConnection(t *testing.T) {
	// Given
	unavailableAddr := reserveClosedTCPAddr(t)
	healthy := startKeeperEchoUpstream(t)
	proxy := startKeeperProxy(t, KeeperOptions{
		Upstreams:   []string{unavailableAddr, healthy.addr},
		DialTimeout: 50 * time.Millisecond,
	})
	conn := dialKeeperProxy(t, proxy.addr)
	defer conn.Close()

	// When
	payload := []byte("keeper-failover-456")
	writeAll(t, conn, payload)
	got := readExactly(t, conn, len(payload))

	// Then
	if string(got) != string(payload) {
		t.Fatalf("echoed payload=%q, want %q", got, payload)
	}
	requireAccepted(t, healthy.accepted)
	if string(receiveBytes(t, healthy.observed)) != string(payload) {
		t.Fatalf("fallback upstream did not observe the exact client payload")
	}
}

func TestKeeper_DoesNotReconnectMidSession_whenUpstreamCloses(t *testing.T) {
	// Given
	first := startKeeperEchoOnceUpstream(t)
	second := startKeeperEchoUpstream(t)
	proxy := startKeeperProxy(t, KeeperOptions{
		Upstreams:   []string{first.addr, second.addr},
		DialTimeout: 200 * time.Millisecond,
	})
	conn := dialKeeperProxy(t, proxy.addr)
	defer conn.Close()

	// When
	payload := []byte("keeper-one-session-789")
	writeAll(t, conn, payload)
	got := readExactly(t, conn, len(payload))
	err := readEOF(t, conn)

	// Then
	if string(got) != string(payload) {
		t.Fatalf("first upstream echo=%q, want %q", got, payload)
	}
	if err != nil {
		t.Fatalf("read EOF after upstream close: %v", err)
	}
	requireNoAcceptedConnection(t, second.accepted)
}

func TestKeeper_FailsDeterministically_whenAllUpstreamsUnavailable(t *testing.T) {
	// Given
	unavailableAddr := reserveClosedTCPAddr(t)
	proxy := startKeeperProxy(t, KeeperOptions{
		Upstreams:   []string{unavailableAddr},
		DialTimeout: 50 * time.Millisecond,
	})
	conn := dialKeeperProxy(t, proxy.addr)
	defer conn.Close()

	// When
	writeAll(t, conn, []byte("keeper-no-upstream"))
	err := readEOF(t, conn)

	// Then
	if err != nil {
		t.Fatalf("read EOF after all upstream dials failed: %v", err)
	}
}

func TestKeeper_RejectsEmptyUpstreamList_whenConstructed(t *testing.T) {
	// Given
	options := KeeperOptions{DialTimeout: 50 * time.Millisecond}

	// When
	_, err := NewKeeperServer(options)

	// Then
	if !errors.Is(err, ErrNoKeeperUpstreams) {
		t.Fatalf("NewKeeperServer error=%v, want ErrNoKeeperUpstreams", err)
	}
}

func TestKeeper_ClosesOpenSession_whenContextCanceled(t *testing.T) {
	// Given
	upstream := startKeeperHoldingUpstream(t)
	ctx, cancel := context.WithCancel(context.Background())
	proxy := startKeeperProxyWithContext(t, ctx, KeeperOptions{
		Upstreams:   []string{upstream.addr},
		DialTimeout: 200 * time.Millisecond,
	})
	conn := dialKeeperProxy(t, proxy.addr)
	defer conn.Close()
	writeAll(t, conn, []byte("keeper-cancel-session"))
	requireAccepted(t, upstream.accepted)

	// When
	cancel()
	err := readEOF(t, conn)

	// Then
	if err != nil {
		t.Fatalf("read EOF after context cancellation: %v", err)
	}
	select {
	case serveErr := <-proxy.done:
		if !errors.Is(serveErr, context.Canceled) {
			t.Fatalf("Serve error=%v, want context.Canceled", serveErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not exit after context cancellation")
	}
}
