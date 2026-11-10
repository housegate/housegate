package forward

import (
	"context"
	"crypto/tls"
	"net"
	"testing"

	"github.com/housegate/housegate/pkg/endpointtls"
	"github.com/housegate/housegate/pkg/endpointtls/endpointtlstest"
)

// TestDefaultDialUsesEndpointTLS pins that a pivot dials the advertised peer
// over TLS when EndpointTLS is set, and plaintext when it is not.
func TestDefaultDialUsesEndpointTLS(t *testing.T) {
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
	p := &Plugin{EndpointTLS: cfg}
	codec, err := p.defaultDial(context.Background(), addr)
	if err != nil {
		t.Fatalf("TLS pivot dial: %v", err)
	}
	c := codec.Conn().(*endpointtls.Conn)
	defer c.Close()
	if _, ok := c.Conn.(*tls.Conn); !ok || c.UpstreamAddress() != addr {
		t.Fatalf("conn = %T %q", c.Conn, c.UpstreamAddress())
	}

	plain := &Plugin{}
	codec, err = plain.defaultDial(context.Background(), addr)
	if err != nil {
		t.Fatalf("plaintext pivot dial: %v", err)
	}
	pc := codec.Conn().(*endpointtls.Conn)
	defer pc.Close()
	if _, ok := pc.Conn.(*tls.Conn); ok {
		t.Fatal("a plugin without EndpointTLS dialed TLS")
	}
}
