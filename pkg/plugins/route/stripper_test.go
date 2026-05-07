package routeplugin

import (
	"context"
	"net"
	"strings"
	"testing"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/peer"
	"housegate/housegate/pkg/route"
)

// newTestSession creates a minimal Session backed by a throwaway net.Pipe
// connection. The caller is responsible for closing if cleanup matters.
func newTestSession(t *testing.T) chsession.Session {
	t.Helper()
	clientConn, _ := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	return chsession.New(0, clientConn)
}

func TestStripper_OnHello_RejectsRouteOnInternalPort(t *testing.T) {
	s := &Stripper{}
	sess := newTestSession(t)
	sess.State().IsInternalPort = true

	hello := &chproto.ClientHello{User: route.FormatRouteUser("peer:9001", "alice")}
	err := s.OnHello(context.Background(), sess, hello)
	if err == nil {
		t.Fatalf("expected error on __route__ over internal-port, got nil")
	}
	if !strings.Contains(err.Error(), "route envelope on internal-port") {
		t.Fatalf("error message should explain the rejection: %v", err)
	}
}

func TestStripper_OnHello_AllowsNonRouteOnInternalPort(t *testing.T) {
	s := &Stripper{}
	sess := newTestSession(t)
	sess.State().IsInternalPort = true

	hello := &chproto.ClientHello{User: peer.FormatUser("peer:9001")}
	if err := s.OnHello(context.Background(), sess, hello); err != nil {
		t.Fatalf("__peer__ on internal-port must pass through: %v", err)
	}
}
