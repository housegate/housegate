package housegate

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chsession"
)

// TestUpstreamDialerRefusesUnadvertisedRouteTarget pins the dialer's own
// check on route targets: whatever set SessionState.RouteTarget, the
// dialer only connects to a housegate the network state advertises, so a
// client can never turn the proxy into a relay to an arbitrary host.
func TestUpstreamDialerRefusesUnadvertisedRouteTarget(t *testing.T) {
	// A listener that must never see a connection: it stands in for an
	// in-cluster ClickHouse or any other host behind the proxy.
	forbidden, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = forbidden.Close() })
	accepted := make(chan struct{}, 1)
	go func() {
		if c, err := forbidden.Accept(); err == nil {
			_ = c.Close()
			accepted <- struct{}{}
		}
	}()

	cfg := minimalRouterOnlyCfg(t)
	cfg.NetworkState.Source = networkStateYAML(t, 1) // advertises only localhost:1
	bs, err := buildServer(Options{Config: cfg}, nil)
	if err != nil {
		t.Fatalf("buildServer: %v", err)
	}
	t.Cleanup(bs.teardown)
	srv := requireProxyServer(t, bs.listeners[0])

	client, other := net.Pipe()
	t.Cleanup(func() { _ = client.Close(); _ = other.Close() })

	for _, target := range []string{
		forbidden.Addr().String(),
		"localhost:" + strings.TrimPrefix(forbidden.Addr().String(), "127.0.0.1:"),
	} {
		sess := chsession.New(1, client)
		sess.State().SetRouteTarget(target)
		codec, err := srv.UpstreamDialer(context.Background(), sess)
		if err == nil {
			if c, ok := codec.Conn().(net.Conn); ok {
				_ = c.Close()
			}
			t.Fatalf("dialer connected to unadvertised route target %s", target)
		}
		if !strings.Contains(err.Error(), "refusing to dial route target") {
			t.Fatalf("dialer error for %s = %v, want a route-target refusal", target, err)
		}
	}
	select {
	case <-accepted:
		t.Fatal("the unadvertised host received a connection")
	default:
	}
}
