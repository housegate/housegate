package replicationproxy

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

const DefaultInterserverRouteHeader = "X-Housegate-Interserver-Peer"

var (
	ErrInvalidInterserverOptions = errors.New("replicationproxy: invalid interserver options")
	ErrUnknownInterserverRoute   = errors.New("replicationproxy: unknown interserver route")
	ErrAmbiguousInterserverRoute = errors.New("replicationproxy: ambiguous interserver route")
)

// InterserverRoute maps one configured peer selector to that peer's HouseGate
// interserver HTTP listener and peer-login JWS audience.
type InterserverRoute struct {
	Peer            string
	TargetIndexerID uint64
	Upstream        string
}

// InterserverOptions configures the ClickHouse interserver HTTP reverse proxy.
type InterserverOptions struct {
	SelfIndexerID uint64
	LocalUpstream string
	Routes        []InterserverRoute
	PeerAuth      *InterserverPeerAuth
	DialTimeout   time.Duration
	ReadTimeout   time.Duration
	WriteTimeout  time.Duration
	RouteHeader   string

	localSourceAllowed func(remoteAddr string) bool
}

// InterserverServer proxies local ClickHouse interserver egress to peer
// HouseGate endpoints, and peer-authenticated inbound requests to local
// ClickHouse interserver HTTP.
type InterserverServer struct {
	selfIndexerID uint64
	peerAuth      *InterserverPeerAuth
	routeHeader   string
	localAllowed  func(remoteAddr string) bool
	localProxy    *httputil.ReverseProxy
	routeHandlers map[string]http.Handler
	soleRoute     http.Handler
	readTimeout   time.Duration
	writeTimeout  time.Duration
}

func NewInterserverServer(options InterserverOptions) (*InterserverServer, error) {
	if options.PeerAuth == nil {
		return nil, fmt.Errorf("peer auth: %w", ErrInvalidInterserverOptions)
	}
	localTarget, err := parseHTTPUpstream(options.LocalUpstream)
	if err != nil {
		return nil, fmt.Errorf("local upstream: %w", err)
	}
	if len(options.Routes) == 0 {
		return nil, fmt.Errorf("routes: %w", ErrInvalidInterserverOptions)
	}
	routeHeader := strings.TrimSpace(options.RouteHeader)
	if routeHeader == "" {
		routeHeader = DefaultInterserverRouteHeader
	}
	localAllowed := options.localSourceAllowed
	if localAllowed == nil {
		localAllowed = isLoopbackRemoteAddr
	}
	transport := interserverTransport(options.DialTimeout)
	routeHandlers := make(map[string]http.Handler, len(options.Routes))
	var soleRoute http.Handler
	for _, route := range options.Routes {
		peer := strings.TrimSpace(route.Peer)
		if peer == "" {
			return nil, fmt.Errorf("route peer: %w", ErrInvalidInterserverOptions)
		}
		if _, exists := routeHandlers[peer]; exists {
			return nil, fmt.Errorf("route peer %q: %w", peer, ErrInvalidInterserverOptions)
		}
		target, err := parseHTTPUpstream(route.Upstream)
		if err != nil {
			return nil, fmt.Errorf("route %q upstream: %w", peer, err)
		}
		handler := newInterserverRouteHandler(interserverRouteProxyOptions{
			target:          target,
			targetIndexerID: route.TargetIndexerID,
			peerAuth:        options.PeerAuth,
			routeHeader:     routeHeader,
			transport:       transport,
		})
		routeHandlers[peer] = handler
		if len(options.Routes) == 1 {
			soleRoute = handler
		}
	}
	return &InterserverServer{
		selfIndexerID: options.SelfIndexerID,
		peerAuth:      options.PeerAuth,
		routeHeader:   routeHeader,
		localAllowed:  localAllowed,
		localProxy: newInterserverLocalProxy(interserverLocalProxyOptions{
			target:      localTarget,
			peerAuth:    options.PeerAuth,
			routeHeader: routeHeader,
			transport:   transport,
		}),
		routeHandlers: routeHandlers,
		soleRoute:     soleRoute,
		readTimeout:   options.ReadTimeout,
		writeTimeout:  options.WriteTimeout,
	}, nil
}

func (s *InterserverServer) Handler() http.Handler {
	return http.HandlerFunc(s.ServeHTTP)
}

func (s *InterserverServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if _, err := s.peerAuth.Validate(r, s.selfIndexerID); err == nil {
		s.localProxy.ServeHTTP(w, r)
		return
	}
	if s.hasPeerAuthHeader(r) {
		http.Error(w, http.StatusText(http.StatusUnauthorized), http.StatusUnauthorized)
		return
	}
	if !s.localAllowed(r.RemoteAddr) {
		http.Error(w, http.StatusText(http.StatusForbidden), http.StatusForbidden)
		return
	}

	if peer := strings.TrimSpace(r.Header.Get(s.routeHeader)); peer != "" {
		handler, ok := s.routeHandlers[peer]
		if !ok {
			http.Error(w, ErrUnknownInterserverRoute.Error(), http.StatusBadRequest)
			return
		}
		handler.ServeHTTP(w, r)
		return
	}
	if s.soleRoute != nil {
		s.soleRoute.ServeHTTP(w, r)
		return
	}
	http.Error(w, ErrAmbiguousInterserverRoute.Error(), http.StatusBadRequest)
}

func (s *InterserverServer) hasPeerAuthHeader(r *http.Request) bool {
	return strings.TrimSpace(r.Header.Get(s.peerAuth.userHeader)) != "" ||
		strings.TrimSpace(r.Header.Get(s.peerAuth.tokenHeader)) != ""
}

type interserverRouteProxyOptions struct {
	target          *url.URL
	targetIndexerID uint64
	peerAuth        *InterserverPeerAuth
	routeHeader     string
	transport       http.RoundTripper
}

type interserverRouteHandler struct {
	targetIndexerID uint64
	peerAuth        *InterserverPeerAuth
	routeHeader     string
	proxy           *httputil.ReverseProxy
}

type interserverLocalProxyOptions struct {
	target      *url.URL
	peerAuth    *InterserverPeerAuth
	routeHeader string
	transport   http.RoundTripper
}

func newInterserverRouteHandler(options interserverRouteProxyOptions) http.Handler {
	return &interserverRouteHandler{
		targetIndexerID: options.targetIndexerID,
		peerAuth:        options.peerAuth,
		routeHeader:     options.routeHeader,
		proxy: &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				rewriteInterserverTarget(req, options.target)
			},
			Transport:    options.transport,
			ErrorHandler: interserverProxyError,
		},
	}
}

func (h *interserverRouteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	r.Header.Del(h.routeHeader)
	r.Header.Del(h.peerAuth.userHeader)
	r.Header.Del(h.peerAuth.tokenHeader)
	if err := h.peerAuth.Attach(r, h.targetIndexerID); err != nil {
		http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
		return
	}
	h.proxy.ServeHTTP(w, r)
}

func newInterserverLocalProxy(options interserverLocalProxyOptions) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			rewriteInterserverTarget(req, options.target)
			req.Header.Del(options.routeHeader)
			req.Header.Del(options.peerAuth.userHeader)
			req.Header.Del(options.peerAuth.tokenHeader)
		},
		Transport:    options.transport,
		ErrorHandler: interserverProxyError,
	}
}

func rewriteInterserverTarget(req *http.Request, target *url.URL) {
	req.URL.Scheme = target.Scheme
	req.URL.Host = target.Host
	req.Host = target.Host
}

func parseHTTPUpstream(addr string) (*url.URL, error) {
	addr = strings.TrimSpace(addr)
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return nil, fmt.Errorf("upstream %q: %w", addr, ErrInvalidInterserverOptions)
	}
	return &url.URL{Scheme: "http", Host: addr}, nil
}

func interserverTransport(dialTimeout time.Duration) http.RoundTripper {
	if dialTimeout == 0 {
		dialTimeout = 5 * time.Second
	}
	return &http.Transport{
		// Peer-auth headers are attached before this transport sends; route only
		// to the configured HouseGate peer target, never an ambient env proxy.
		DialContext: (&net.Dialer{
			Timeout: dialTimeout,
		}).DialContext,
	}
}

func interserverProxyError(w http.ResponseWriter, _ *http.Request, _ error) {
	http.Error(w, http.StatusText(http.StatusBadGateway), http.StatusBadGateway)
}
