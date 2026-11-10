// Package endpointtls dials the endpoints a network registry advertises
// (another indexer's housegate, a pinned agent upstream) with or without TLS.
//
// A nil *Config means plaintext, which keeps every caller's default exactly
// as before TLS existed. A non-nil Config always verifies the server
// certificate against the system roots, or against a CA bundle when one is
// configured, and sends the dialed host as SNI. There is deliberately no way
// to skip verification: the point is that an agent cannot be pointed at an
// impostor endpoint.
package endpointtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"
)

// Config is the client TLS posture for one class of endpoints. The zero
// value is not useful; build one with New. A nil *Config dials plaintext.
type Config struct {
	base *tls.Config
}

// New returns a TLS client configuration that verifies servers against the
// system roots, or only against the PEM bundle at caFile when it is set.
func New(caFile string) (*Config, error) {
	base := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("endpoint tls: read CA file %s: %w", caFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("endpoint tls: CA file %s holds no PEM certificate", caFile)
		}
		base.RootCAs = pool
	}
	return &Config{base: base}, nil
}

// Enabled reports whether dials through c use TLS.
func (c *Config) Enabled() bool { return c != nil }

// ClientConfig returns the tls.Config for a dial to addr ("host:port"): a
// copy of the base configuration with ServerName set to the host. It returns
// nil for a nil Config.
func (c *Config) ClientConfig(addr string) *tls.Config {
	if c == nil {
		return nil
	}
	cfg := c.base.Clone()
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	cfg.ServerName = host
	return cfg
}

// HTTPClient returns an http.Client with the given timeout whose https
// requests verify servers the same way dials through c do. A nil Config, or
// one without a CA bundle, returns a client using the system roots.
func (c *Config) HTTPClient(timeout time.Duration) *http.Client {
	client := &http.Client{Timeout: timeout}
	if c != nil && c.base.RootCAs != nil {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.TLSClientConfig = c.base.Clone()
		client.Transport = tr
	}
	return client
}

// Dial connects to addr over TCP and, when c is non-nil, completes a TLS
// handshake with the host of addr as the server name before returning. timeout
// bounds the TCP connect and the handshake together; zero means no bound
// beyond ctx.
func (c *Config) Dial(ctx context.Context, addr string, timeout time.Duration) (net.Conn, error) {
	return c.dial(ctx, "tcp", addr, addr, timeout)
}

// DialExact connects to the already-resolved remote address of an existing
// connection, so a redial reaches the same server, and verifies it under the
// name the original connection was dialed with (configured "host:port").
func (c *Config) DialExact(ctx context.Context, remote net.Addr, configured string, timeout time.Duration) (net.Conn, error) {
	network := remote.Network()
	if network == "" {
		network = "tcp"
	}
	return c.dial(ctx, network, remote.String(), configured, timeout)
}

func (c *Config) dial(ctx context.Context, network, target, name string, timeout time.Duration) (net.Conn, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, target)
	if err != nil {
		return nil, err
	}
	if c == nil {
		return conn, nil
	}
	tc := tls.Client(conn, c.ClientConfig(name))
	if err := tc.HandshakeContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("tls handshake with %s: %w", name, err)
	}
	return tc, nil
}

// Conn is a dialed endpoint connection that remembers the address it was
// dialed with (UpstreamAddress, stable across DNS resolution) and how, so it
// can be redialed to the same server.
type Conn struct {
	net.Conn
	// Address is the configured "host:port" the connection was dialed with.
	Address string
	// TLS is the configuration the connection was dialed with; nil for
	// plaintext.
	TLS *Config
}

// UpstreamAddress returns the configured address the connection was dialed
// with.
func (c *Conn) UpstreamAddress() string { return c.Address }

// RedialExact opens a new connection to the same resolved server, under the
// same TLS posture and server name.
func (c *Conn) RedialExact(ctx context.Context, timeout time.Duration) (net.Conn, error) {
	conn, err := c.TLS.DialExact(ctx, c.Conn.RemoteAddr(), c.Address, timeout)
	if err != nil {
		return nil, err
	}
	return &Conn{Conn: conn, Address: c.Address, TLS: c.TLS}, nil
}

// DialConn is Dial returning a *Conn that records addr and c.
func (c *Config) DialConn(ctx context.Context, addr string, timeout time.Duration) (*Conn, error) {
	conn, err := c.Dial(ctx, addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Conn{Conn: conn, Address: addr, TLS: c}, nil
}
