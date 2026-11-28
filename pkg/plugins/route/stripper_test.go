package routeplugin

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/credentials"
	"github.com/housegate/housegate/pkg/peer"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/route"
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

type fakeTopology map[uint64]registry.ProxyAddress

func (f fakeTopology) ProxyByIndexerId(id uint64) (registry.ProxyAddress, bool) {
	a, ok := f[id]
	return a, ok
}

func (f fakeTopology) AllIndexers() map[uint64]registry.ProxyAddress {
	out := make(map[uint64]registry.ProxyAddress, len(f))
	for k, v := range f {
		out[k] = v
	}
	return out
}

type fakeCreds struct{ user, password string }

func (c fakeCreds) GetDefaultCredential() (string, string, error) { return c.user, c.password, nil }
func (c fakeCreds) GetCredentialForIndexer(uint64) (string, string, error) {
	return c.user, c.password, nil
}

var _ credentials.CredentialProvider = fakeCreds{}

func testStripper() Stripper {
	return Stripper{
		Topology: fakeTopology{
			7: {Url: "peer-b.example", HousegatePort: 9001, StorageRPCPort: 8545, ComputeRPCPort: 8546},
			8: {Url: "https://peer-c.example", HousegatePort: 9002},
		},
		Credentials: fakeCreds{user: "si_gateway", password: "gw-secret"},
	}
}

func TestStripper_OnHello_RouteAdmission(t *testing.T) {
	peerUser := peer.FormatUser("0x00000000000000000000000000000000000000aa")
	cases := []struct {
		name     string
		s        Stripper
		target   string
		user     string
		password string
		admit    bool
	}{
		{"advertised peer with peer envelope", testStripper(), "peer-b.example:9001", peerUser, "jws", true},
		{"advertised peer, url with scheme, raw Addr form", testStripper(), "https://peer-c.example:9002", peerUser, "jws", true},
		{"advertised peer, url with scheme, host form", testStripper(), "peer-c.example:9002", peerUser, "jws", true},
		{"advertised peer with static gateway credential", testStripper(), "peer-b.example:9001", "si_gateway", "gw-secret", true},

		{"in-cluster clickhouse ip", testStripper(), "10.0.0.5:9000", peerUser, "jws", false},
		{"in-cluster clickhouse service", testStripper(), "clickhouse-0.clickhouse:9000", peerUser, "jws", false},
		{"in-cluster postgres", testStripper(), "postgres.db:5432", peerUser, "jws", false},
		{"advertised host on its storage rpc port", testStripper(), "peer-b.example:8545", peerUser, "jws", false},
		{"advertised host on a foreign port", testStripper(), "peer-b.example:9000", peerUser, "jws", false},
		{"empty-host target", testStripper(), ":9001", peerUser, "jws", false},
		{"client-supplied admin credential", testStripper(), "peer-b.example:9001", "admin", "admin-password", false},
		{"gateway user with wrong password", testStripper(), "peer-b.example:9001", "si_gateway", "guess", false},
		{"no credential provider: static user refused", Stripper{Topology: testStripper().Topology}, "peer-b.example:9001", "si_gateway", "gw-secret", false},
		{"zero stripper refuses every route", Stripper{}, "peer-b.example:9001", peerUser, "jws", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sess := newTestSession(t)
			hello := &chproto.ClientHello{User: route.FormatRouteUser(tc.target, tc.user), Password: tc.password}
			err := tc.s.OnHello(context.Background(), sess, hello)
			if tc.admit {
				if err != nil {
					t.Fatalf("route must be admitted: %v", err)
				}
				if got := sess.State().GetRouteTarget(); got != tc.target {
					t.Fatalf("RouteTarget = %q, want %q", got, tc.target)
				}
				if hello.User != tc.user {
					t.Fatalf("hello.User = %q, want stripped %q", hello.User, tc.user)
				}
				return
			}
			if !errors.Is(err, ErrRouteRefused) {
				t.Fatalf("route must be refused with ErrRouteRefused, got %v", err)
			}
			if got := sess.State().GetRouteTarget(); got != "" {
				t.Fatalf("refused route must not set RouteTarget, got %q", got)
			}
		})
	}
}

func TestStripper_OnHello_NonRouteUntouched(t *testing.T) {
	sess := newTestSession(t)
	hello := &chproto.ClientHello{User: "admin", Password: "x"}
	if err := (Stripper{}).OnHello(context.Background(), sess, hello); err != nil {
		t.Fatalf("plain user must pass Stripper (credential plugin replaces it): %v", err)
	}
	if sess.State().GetRouteTarget() != "" || hello.User != "admin" {
		t.Fatalf("plain hello must be left as is")
	}
}
