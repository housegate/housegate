package interserver

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"housegate/housegate/pkg/log"
)

// The mesh sidecar topology (link B, two-hop):
//
//   ch-2 (advertises localhost:9010 in keeper)
//     ↓  CH outbound fetch: dial localhost:9010 (its OWN netns → its sidecar Egress)
//   Egress (ch-2 sidecar)
//     - reads HTTP endpoint query param, extracts source replica name (e.g. "ch-1")
//     - PeerLookup("ch-1") → ch-1's Ingress address (e.g. "ch-1:19009")
//     - mTLS dials there, presenting its housegate client cert
//     ↓
//   Ingress (ch-1 sidecar, listens 0.0.0.0:19009 with ClientAuth=Required)
//     - validates the peer's client cert against the shared CA
//     - forwards plain HTTP to local CH (127.0.0.1:9009)
//     ↓
//   ch-1 (real interserver, loopback only)
//
// Every CH advertises the SAME self-relative host "localhost:9010", so each
// peer naturally dials its OWN sidecar (the same trick that makes a
// co-located housegate work for 9000 — see CLAUDE.md's "CH only opens TCP
// to co-located housegate" invariant). The replica name in the HTTP URL is
// the only routing key the egress needs.

// PeerLookup resolves a source replica name (as it appears under
// /clickhouse/tables/.../replicas/<replica>/ in ClickHouse's interserver
// endpoint URL) to that replica's Ingress address (host:port). The (zero,
// false) return signals "unknown peer" and the egress fails the fetch with
// a 502.
type PeerLookup func(replica string) (addr string, ok bool)

// EgressConfig configures the local-CH-facing sidecar leg.
type EgressConfig struct {
	// PeerLookup is required: it provides the routing table from source
	// replica name to peer Ingress address.
	PeerLookup PeerLookup
	// TLSClient is the mTLS client config used to dial peer Ingresses
	// (must include this housegate's own cert + the shared CA as RootCAs).
	TLSClient *tls.Config
	// DialTimeout bounds the TLS dial to a peer Ingress. Default 10s.
	DialTimeout time.Duration
}

// Egress is the sidecar leg local CH dials on its own netns to fetch parts.
type Egress struct {
	cfg EgressConfig

	served    atomic.Int64 // requests forwarded
	rejected  atomic.Int64 // requests refused before any peer dial (bad/missing replica, unknown peer)
	roundTrip atomic.Int64 // requests where the peer round-trip completed (any status)
}

// NewEgress builds an Egress. PeerLookup and TLSClient are required.
func NewEgress(cfg EgressConfig) (*Egress, error) {
	if cfg.PeerLookup == nil {
		return nil, errors.New("interserver.NewEgress: PeerLookup is required")
	}
	if cfg.TLSClient == nil {
		return nil, errors.New("interserver.NewEgress: TLSClient is required (mTLS client cert + CA)")
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	return &Egress{cfg: cfg}, nil
}

// Serve runs the egress until ctx is cancelled. Matches the housegate
// listenerServer contract.
func (e *Egress) Serve(ctx context.Context, ln net.Listener) error {
	srv := &http.Server{
		Handler:           http.HandlerFunc(e.handle),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return serveHTTP(ctx, srv, ln)
}

// Served / Rejected / RoundTrip are observability accessors.
func (e *Egress) Served() int64    { return e.served.Load() }
func (e *Egress) Rejected() int64  { return e.rejected.Load() }
func (e *Egress) RoundTrip() int64 { return e.roundTrip.Load() }

func (e *Egress) handle(w http.ResponseWriter, r *http.Request) {
	replica := extractReplica(r.URL)
	if replica == "" {
		e.rejected.Add(1)
		log.Warnw("interserver egress: no replica in endpoint", "url", r.URL.String())
		http.Error(w, "no replica in endpoint query", http.StatusBadRequest)
		return
	}
	target, ok := e.cfg.PeerLookup(replica)
	if !ok || target == "" {
		e.rejected.Add(1)
		log.Warnw("interserver egress: unknown peer", "replica", replica)
		http.Error(w, fmt.Sprintf("unknown peer %q", replica), http.StatusBadGateway)
		return
	}

	e.served.Add(1)
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "https"
			req.URL.Host = target
			// Host header is informational once the URL is rewritten; keep
			// it as the original so any logging downstream stays meaningful.
		},
		Transport: &http.Transport{
			TLSClientConfig:   e.cfg.TLSClient.Clone(),
			DialContext:       (&net.Dialer{Timeout: e.cfg.DialTimeout}).DialContext,
			ForceAttemptHTTP2: false,
		},
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			log.Warnw("interserver egress: peer round-trip failed", "replica", replica, "target", target, "err", err)
			http.Error(w, fmt.Sprintf("peer round-trip failed: %v", err), http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r)
	e.roundTrip.Add(1)
}

// IngressConfig configures the peer-mTLS-facing sidecar leg.
type IngressConfig struct {
	// LocalCH is the co-located ClickHouse's real interserver address (the
	// loopback-only listener the gateway forwards to).
	LocalCH string
	// TLSServer is the mTLS server config (own cert + ClientCAs +
	// ClientAuth=RequireAndVerifyClientCert).
	TLSServer *tls.Config
}

// Ingress is the sidecar leg peer Egresses dial over mTLS.
type Ingress struct {
	cfg IngressConfig

	served   atomic.Int64
	rejected atomic.Int64
}

// NewIngress builds an Ingress. LocalCH and TLSServer are required, and
// TLSServer must enforce client cert verification.
func NewIngress(cfg IngressConfig) (*Ingress, error) {
	if cfg.LocalCH == "" {
		return nil, errors.New("interserver.NewIngress: LocalCH is required")
	}
	if cfg.TLSServer == nil {
		return nil, errors.New("interserver.NewIngress: TLSServer is required")
	}
	if cfg.TLSServer.ClientAuth < tls.RequireAndVerifyClientCert {
		return nil, errors.New("interserver.NewIngress: TLSServer.ClientAuth must be RequireAndVerifyClientCert (mTLS)")
	}
	return &Ingress{cfg: cfg}, nil
}

// Serve runs the ingress until ctx is cancelled.
func (i *Ingress) Serve(ctx context.Context, ln net.Listener) error {
	tlsLn := tls.NewListener(ln, i.cfg.TLSServer)
	srv := &http.Server{
		Handler:           http.HandlerFunc(i.handle),
		ReadHeaderTimeout: 10 * time.Second,
	}
	return serveHTTP(ctx, srv, tlsLn)
}

func (i *Ingress) Served() int64   { return i.served.Load() }
func (i *Ingress) Rejected() int64 { return i.rejected.Load() }

func (i *Ingress) handle(w http.ResponseWriter, r *http.Request) {
	// mTLS handshake already validated the client cert at this point; any
	// request that reaches here is from an authenticated peer Egress.
	i.served.Add(1)
	proxy := &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = "http"
			req.URL.Host = i.cfg.LocalCH
		},
		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			i.rejected.Add(1)
			log.Warnw("interserver ingress: local CH round-trip failed", "target", i.cfg.LocalCH, "err", err)
			http.Error(w, fmt.Sprintf("local CH failed: %v", err), http.StatusBadGateway)
		},
	}
	proxy.ServeHTTP(w, r)
}

// extractReplica parses the ClickHouse interserver "endpoint" query
// parameter and returns the source replica name. The endpoint takes the
// form "DataPartsExchange:/clickhouse/tables/<shard>/<db>/<tbl>/replicas/<replica>/"
// and the replica name is the path segment after the final "/replicas/".
func extractReplica(u *url.URL) string {
	ep := u.Query().Get("endpoint")
	if ep == "" {
		return ""
	}
	const marker = "/replicas/"
	idx := strings.LastIndex(ep, marker)
	if idx < 0 {
		return ""
	}
	rest := ep[idx+len(marker):]
	if end := strings.IndexAny(rest, "/?&"); end >= 0 {
		rest = rest[:end]
	}
	return rest
}

// serveHTTP runs srv until ctx is cancelled, then gracefully shuts down.
func serveHTTP(ctx context.Context, srv *http.Server, ln net.Listener) error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ln) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
		<-errCh
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}
