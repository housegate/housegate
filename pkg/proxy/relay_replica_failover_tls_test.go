package proxy

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	"github.com/housegate/housegate/pkg/endpointtls"
	"github.com/housegate/housegate/pkg/endpointtls/endpointtlstest"
)

// TestDialFailoverCandidateTLS pins that a replica failover dials its
// candidate with the relay's failover TLS posture and keeps the dialed
// address for the SI lane's upstream switch.
func TestDialFailoverCandidateTLS(t *testing.T) {
	ca := endpointtlstest.NewCA(t)
	ln := ca.Listen(t, "localhost")
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.(*tls.Conn).Handshake()
		}
	}()
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	addr := net.JoinHostPort("localhost", port)

	cfg, err := endpointtls.New(ca.File)
	if err != nil {
		t.Fatal(err)
	}
	up, err := dialFailoverCandidate(context.Background(), addr, cfg)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer closeCodec(up)
	named, ok := up.Conn().(interface{ UpstreamAddress() string })
	if !ok || named.UpstreamAddress() != addr {
		t.Fatalf("conn %T does not report %q", up.Conn(), addr)
	}
	if _, ok := up.Conn().(exactRedialer); !ok {
		t.Fatal("failover conn cannot be redialed exactly")
	}
	if _, ok := up.Conn().(*endpointtls.Conn).Conn.(*tls.Conn); !ok {
		t.Fatal("failover candidate was not dialed over TLS")
	}
}
