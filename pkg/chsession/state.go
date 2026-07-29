package chsession

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
)

// VersionTriple holds client-advertised version numbers.
type VersionTriple struct{ Major, Minor, Patch int }

// SessionState is the single source of truth for per-connection state.
//
// Fields are grouped by their rebind semantics:
//
//	Stable after handshake (not replayed on rebind — new upstream
//	renegotiates):
//	  ClientHostname, ClientVersion, ClientRevision, ServerDisplayName,
//	  Timezone, ChunkedRecv, ChunkedSend.
//
//	Identity & credentials (mirrored to every rebound upstream via its own
//	handshake using MappedUser / MappedPassword):
//	  AuthenticatedUser, MappedUser, MappedPassword, Identity.
//
//	Replayable on rebind (the reason SessionState exists — a new upstream
//	has a fresh default database / no applied SETTINGS; Replay() replays
//	USE and SET statements to bring it in sync):
//	  Database, Settings.
//
//	Logical / physical mapping (when a static PhysicalDatabase is
//	configured so that multiple user-facing "logical" databases are
//	multiplexed over a single real database via table-name prefixes):
//	  LogicalDatabase holds the name the user uttered (e.g. "database1"),
//	  while Database holds the name upstream actually sees (e.g. "testnet").
//	  When no mapping is configured the two fields stay in sync.
//
//	Routing (set by routeplugin.Stripper when ClientHello.User carries a
//	__route__|<target>|<user> prefix; consumed by the upstream dialer to
//	connect directly to the peer proxy and by PluginChain to bypass
//	non-RouteAware plugins for the rest of the connection):
//	  RouteTarget.
//
//	Maintenance bypass (set once by host auth validator when
//	SQL_sentio_maintenance=1 is observed; consumed by rewrite / usage /
//	commitgate plugins to short-circuit and forward verbatim):
//	  maintenance.
//
//	Runtime tracking (not replayed; lives for the lifetime of a single
//	query):
//	  LastQueryID, LastClientInfo, LastRewriteArgs, HasActiveRewrite.
type SessionState struct {
	ClientHostname    string
	ClientVersion     VersionTriple
	ClientRevision    int
	ServerDisplayName string
	Timezone          string
	ChunkedRecv       string
	ChunkedSend       string

	AuthenticatedUser string
	MappedUser        string
	MappedPassword    string
	Identity          IdentityClaims

	Database        string
	LogicalDatabase string
	Settings        map[string]chproto.Setting

	RouteTarget string

	// IsPeerTrusted is set by the credential plugin during OnHello when
	// the inbound ClientHello.User carries the peer.Prefix marker and
	// the peer-relay JWS in the password validates against the
	// configured PeerValidator. Once set, downstream OnQuery plugins —
	// auth (JWS qhash), rewrite (don't re-rewrite already-rewritten
	// inner SQL emitted by an upstream housegate's rewriter) — bypass
	// themselves for the rest of the connection. PeerAddress carries
	// the recovered Ethereum address of the signing peer for logging.
	IsPeerTrusted bool
	PeerAddress   string

	// IsForwardedFromPeer is true when the peer envelope carried the
	// peer.ForwardedMarker, signalling a forward-decision pivot rather
	// than a rewriter-emitted remote() loopback. The chain's peer-trust
	// filter ignores the auth/rewrite opt-out when this flag is set —
	// a forward-pivoted session arrives with the client's raw SQL, so
	// rewrite must run locally on this proxy and the original agent
	// JWS still validates (qhash matches the unrewritten body).
	//
	// Always implies IsPeerTrusted; the credential plugin sets both
	// in the same OnHello call.
	IsForwardedFromPeer bool

	// IsInternalPort is true when the connection arrived on the server's
	// internal-only listener. Used by Stripper to hard-reject __route__
	// envelopes that have no business arriving from a peer.
	IsInternalPort bool

	// IsForwarding is set by the forward plugin when the entire session is
	// being transparently forwarded to a peer's internal-port. ForwardAware
	// plugins use this to skip themselves (rewrite/commitgate
	// opt out by default); plugins without the marker keep running on the
	// entry proxy by default.
	//
	// Concurrency invariant: written once during OnHello on the relay's
	// client-to-upstream goroutine, then read by subsequent query-stage
	// hooks on the same goroutine. Lockless reads are safe under this
	// invariant; do not write IsForwarding from any other goroutine.
	IsForwarding bool

	LastQueryID    string
	LastClientInfo *chproto.ClientInfo
	// LastRewriteArgs is the rewriter.RewriteArgs interface from pkg/proxy,
	// kept as `any` to avoid a chsession → rewriter dependency. Relay casts
	// on read when needed (Task 13+).
	LastRewriteArgs  any
	HasActiveRewrite bool

	// CommitGateEvent is the *commitgate.Event that the commitgate
	// plugin stashed during OnQuery. Read by commitgate.Plugin.OnException
	// to dispatch OnStatementException to subscribed observers; cleared
	// by commitgate.Plugin.OnQueryComplete. Kept as `any` to avoid a
	// chsession → commitgate import dependency. Plugins that need to read
	// it cast to *commitgate.Event.
	CommitGateEvent any

	// PeerServerHelloRaw holds the raw on-wire bytes of the ServerHello
	// received from the peer during RebindToPeer. Written once by
	// RebindToPeer after a successful pivot; read by relay.handshake to
	// echo the peer's ServerHello to the original client without
	// repeating the upstream hello exchange. Nil when IsForwarding is
	// false.
	PeerServerHelloRaw []byte
	// PeerRevision is the negotiated protocol revision established during
	// RebindToPeer. relay.handshake uses this to set codec revisions
	// without re-reading a second ServerHello.
	PeerRevision int

	// maintenance is set by the host (sentio-node) auth validator when
	// it recognizes SQL_sentio_maintenance=1 on an indexer-signed query.
	// When true, the rewrite, usage, and commitgate plugins short-circuit
	// and forward verbatim. Guarded by mu; read via Snapshot().Maintenance,
	// written via SetMaintenance.
	maintenance bool

	// platformOperator is set by the auth validator when it recognizes
	// SQL_sentio_platform_operator on a query whose recovered signer
	// address is in the configured platform-operator allowlist. When
	// true, the rewrite, usage, and commitgate plugins short-circuit
	// and forward verbatim — the same bypass shape as maintenance, but
	// gated on operator identity rather than indexer identity. Guarded
	// by mu; read via Snapshot().PlatformOperator, written via
	// SetPlatformOperator.
	platformOperator bool

	// isDriver is set by the auth validator when it recognizes
	// SQL_sentio_driver on an indexer-signed query. Narrower than
	// maintenance: only the usage and commitgate plugins short-circuit;
	// rewrite continues to run so driver-written logical names get
	// translated to physical names. Guarded by mu; read via
	// Snapshot().IsDriver, written via SetIsDriver.
	isDriver bool

	mu sync.RWMutex
}

// NewSessionState returns an empty SessionState with a non-nil Settings map.
func NewSessionState() *SessionState {
	return &SessionState{Settings: make(map[string]chproto.Setting)}
}

// SessionStateSnapshot is an immutable view for plugins and observers.
// Callers MUST treat it as read-only.
type SessionStateSnapshot struct {
	ClientHostname    string
	ClientVersion     VersionTriple
	ClientRevision    int
	ServerDisplayName string
	Timezone          string
	ChunkedRecv       string
	ChunkedSend       string

	AuthenticatedUser string
	MappedUser        string
	MappedPassword    string
	Identity          IdentityClaims

	Database        string
	LogicalDatabase string
	Settings        map[string]chproto.Setting

	RouteTarget string

	IsPeerTrusted       bool
	PeerAddress         string
	IsForwardedFromPeer bool
	IsInternalPort      bool
	IsForwarding        bool

	LastQueryID      string
	LastClientInfo   *chproto.ClientInfo
	LastRewriteArgs  any
	HasActiveRewrite bool
	CommitGateEvent  any
	Maintenance      bool
	PlatformOperator bool
	IsDriver         bool
}

// Snapshot returns a copy of the state under a read lock. Settings map is
// shallow-copied so the caller cannot mutate the original.
func (s *SessionState) Snapshot() SessionStateSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snap := SessionStateSnapshot{
		ClientHostname:      s.ClientHostname,
		ClientVersion:       s.ClientVersion,
		ClientRevision:      s.ClientRevision,
		ServerDisplayName:   s.ServerDisplayName,
		Timezone:            s.Timezone,
		ChunkedRecv:         s.ChunkedRecv,
		ChunkedSend:         s.ChunkedSend,
		AuthenticatedUser:   s.AuthenticatedUser,
		MappedUser:          s.MappedUser,
		MappedPassword:      s.MappedPassword,
		Identity:            s.Identity,
		Database:            s.Database,
		LogicalDatabase:     s.LogicalDatabase,
		RouteTarget:         s.RouteTarget,
		IsPeerTrusted:       s.IsPeerTrusted,
		PeerAddress:         s.PeerAddress,
		IsForwardedFromPeer: s.IsForwardedFromPeer,
		IsInternalPort:      s.IsInternalPort,
		IsForwarding:        s.IsForwarding,
		LastQueryID:         s.LastQueryID,
		LastClientInfo:      s.LastClientInfo,
		LastRewriteArgs:     s.LastRewriteArgs,
		HasActiveRewrite:    s.HasActiveRewrite,
		CommitGateEvent:     s.CommitGateEvent,
		Maintenance:         s.maintenance,
		PlatformOperator:    s.platformOperator,
		IsDriver:            s.isDriver,
	}
	if len(s.Settings) > 0 {
		snap.Settings = make(map[string]chproto.Setting, len(s.Settings))
		for k, v := range s.Settings {
			snap.Settings[k] = v
		}
	}
	return snap
}

// Account returns the authenticated account address (Identity.UserID).
// Empty when no auth has run. Implements pkg/rewriter.Session.
func (s *SessionState) Account() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Identity.UserID
}

// LogicalDatabaseName returns the user-facing database name.
// Implements pkg/rewriter.Session. Named with the "Name" suffix to
// avoid collision with the LogicalDatabase struct field.
func (s *SessionState) LogicalDatabaseName() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.LogicalDatabase
}

// PhysicalDatabaseName returns the upstream database name (the
// Database field). Implements pkg/rewriter.Session.
func (s *SessionState) PhysicalDatabaseName() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Database
}

// SetRouteTarget records the target proxy address for a routed session
// (a connection whose ClientHello.User carried a __route__|<target>|<user>
// prefix). Set once by routeplugin.Stripper during OnHello; the upstream
// dialer reads it to skip cluster routing, and PluginChain reads it to
// bypass non-RouteAware plugins for the rest of the connection's
// lifetime. Empty string means "not a routed session".
func (s *SessionState) SetRouteTarget(addr string) {
	s.mu.Lock()
	s.RouteTarget = addr
	s.mu.Unlock()
}

// GetRouteTarget returns the routed-session target address, or empty
// string for a regular session. Named with the "Get" prefix to avoid
// collision with the RouteTarget struct field.
func (s *SessionState) GetRouteTarget() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.RouteTarget
}

// IsRouted reports whether the session is a routed (proxy-to-proxy)
// connection — i.e. one whose ClientHello.User carried a __route__
// envelope and was unwrapped by routeplugin.Stripper.
//
// Forward-pivoted sessions (forward.Plugin.pivotToPeer) ALSO populate
// RouteTarget, but only as an internal memo for the forward plugin's
// USE-comparison logic. They are conceptually "forwarding", not
// "routed", and IsRouted() must return false for them so:
//
//   - routeplugin.Signer (RouteAware-opt-in, internally guarded by
//     IsRouted()) does NOT inject the relay JWS — the receiving peer
//     already validates the original agent's per-query JWS, and
//     duplicating SQL_x_auth_token would let the relay's token
//     overwrite the agent's in the receiver's settings map.
//   - non-RouteAware plugins (auth, usage, concurrency, forward
//     itself for subsequent USE detection) keep firing on the
//     forwarded leg of the session.
//
// IsForwarding takes precedence: a session that is both forward-
// pivoted and route-marked is treated as forwarding.
func (s *SessionState) IsRouted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.RouteTarget != "" && !s.IsForwarding
}

// SetForwarding marks the session as transparently forwarded to a peer
// proxy's internal-port. Set once by the forward plugin during OnHello;
// ForwardAware plugins read IsForwarding to decide whether to skip
// themselves for the rest of the connection.
func (s *SessionState) SetForwarding(v bool) {
	s.mu.Lock()
	s.IsForwarding = v
	s.mu.Unlock()
}

// SetPeerTrust marks the session as authenticated by an upstream
// housegate peer (validated peer-relay JWS observed at OnHello time)
// and records the recovered signer address. Once set, OnQuery-stage
// plugins that would otherwise re-validate or re-rewrite the inner
// SQL emitted by the upstream's rewriter bypass themselves. Idempotent
// per connection — set once during OnHello, never cleared.
//
// Equivalent to SetPeerTrustForwarded(peerAddr, false) — used for the
// rewriter's remote() loopback path where the inner SQL is already
// rewritten and must NOT be rewritten again on this proxy.
func (s *SessionState) SetPeerTrust(peerAddr string) {
	s.SetPeerTrustForwarded(peerAddr, false)
}

// SetPeerTrustForwarded is the forward-pivot variant of SetPeerTrust:
// it records the same peer-trust state but additionally flags the
// session as forwarded-from-peer so the chain's peer-trust filter
// keeps running rewrite/auth on the unrewritten client SQL. Used by
// the credential plugin when the inbound peer envelope carries
// peer.ForwardedMarker.
func (s *SessionState) SetPeerTrustForwarded(peerAddr string, forwarded bool) {
	s.mu.Lock()
	s.IsPeerTrusted = true
	s.PeerAddress = peerAddr
	s.IsForwardedFromPeer = forwarded
	s.mu.Unlock()
}

// PeerTrusted reports whether the session arrived as a peer-trusted
// connection (validated by a peer-relay JWS in the ClientHello
// password). Reads through the lock to stay consistent with concurrent
// state-tracker mutations on the same session.
func (s *SessionState) PeerTrusted() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.IsPeerTrusted
}

// GetPeerAddress returns the recovered signer address of a peer-trusted
// session, or empty string when the session is not peer-trusted.
func (s *SessionState) GetPeerAddress() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.PeerAddress
}

// SetDatabase updates the current database. Called by StateTrackerPlugin
// when it observes a USE statement.
func (s *SessionState) SetDatabase(db string) {
	s.mu.Lock()
	s.Database = db
	s.mu.Unlock()
}

// SetLogicalDatabase records the user-facing ("logical") database name,
// which may differ from Database when a static PhysicalDatabase is mapped
// onto multiple logical databases via table-name prefixes. Callers that
// surface database context to plugins (e.g. query rewriters that need to
// filter by the user's logical database) should read this field rather
// than Database.
func (s *SessionState) SetLogicalDatabase(db string) {
	s.mu.Lock()
	s.LogicalDatabase = db
	s.mu.Unlock()
}

// AddSetting records a session-level SETTING. Called by StateTrackerPlugin
// when it observes a SET statement. Per spec D7, Query.Settings (per-query
// payload) must NOT flow through here.
func (s *SessionState) AddSetting(k string, v chproto.Setting) {
	s.mu.Lock()
	if s.Settings == nil {
		s.Settings = make(map[string]chproto.Setting)
	}
	s.Settings[k] = v
	s.mu.Unlock()
}

// MarkActiveRewrite flags the current in-flight query as having undergone
// rewriting. Relay uses the flag to decide whether to request Exception
// decoding on the upstream-to-client path (so reverse-mapping can run).
func (s *SessionState) MarkActiveRewrite(args any) {
	s.mu.Lock()
	s.HasActiveRewrite = true
	s.LastRewriteArgs = args
	s.mu.Unlock()
}

// ClearActiveRewrite is called when the query completes (EndOfStream or
// Exception).
func (s *SessionState) ClearActiveRewrite() {
	s.mu.Lock()
	s.HasActiveRewrite = false
	s.mu.Unlock()
}

// SetCommitGateEvent stashes the commitgate Event for the in-flight
// query so the commitgate plugin's OnException hook can recover it
// when ClickHouse rejects the statement. Stored as `any` to avoid a
// chsession → commitgate import cycle.
func (s *SessionState) SetCommitGateEvent(ev any) {
	s.mu.Lock()
	s.CommitGateEvent = ev
	s.mu.Unlock()
}

// TakeCommitGateEvent atomically removes and returns the in-flight commitgate
// Event. The success path uses this before exposing EndOfStream to the client,
// preventing the next query from replacing the event between observation and
// asynchronous dispatch.
func (s *SessionState) TakeCommitGateEvent() any {
	s.mu.Lock()
	ev := s.CommitGateEvent
	s.CommitGateEvent = nil
	s.mu.Unlock()
	return ev
}

// ClearCommitGateEvent drops the stashed commitgate Event. Called by
// commitgate.Plugin.OnQueryComplete on both success and failure paths
// so the next query starts clean.
func (s *SessionState) ClearCommitGateEvent() {
	s.mu.Lock()
	s.CommitGateEvent = nil
	s.mu.Unlock()
}

// SetMaintenance flags this session as an indexer-signed maintenance
// session. When true, the rewrite, usage, and commitgate plugins
// short-circuit and forward verbatim. Hosts (sentio-node) set this via
// the auth validator after recognizing SQL_sentio_maintenance=1.
func (s *SessionState) SetMaintenance(v bool) {
	s.mu.Lock()
	s.maintenance = v
	s.mu.Unlock()
}

// SetPlatformOperator flags this session as a platform-operator session.
// When true, the rewrite, usage, and commitgate plugins short-circuit
// and forward verbatim — same bypass shape as maintenance, but gated on
// the recovered Ethereum signer being in the configured platform-operator
// allowlist (set by the auth validator after recognizing
// SQL_sentio_platform_operator).
func (s *SessionState) SetPlatformOperator(v bool) {
	s.mu.Lock()
	s.platformOperator = v
	s.mu.Unlock()
}

// SetIsDriver flags this session as indexer-driver traffic. When true,
// the usage and commitgate plugins short-circuit; rewrite still runs so
// driver-written logical names get translated to physical. Set by the
// auth validator after recognizing SQL_sentio_driver on an indexer-signed
// query.
func (s *SessionState) SetIsDriver(v bool) {
	s.mu.Lock()
	s.isDriver = v
	s.mu.Unlock()
}

// Replay re-applies the replayable subset (Database, Settings) to a newly
// bound upstream by sending USE and SET queries through it.
//
// The queries use the upstream codec's current revision; the caller
// (Session.RebindUpstream) is responsible for completing the handshake before
// calling Replay.
//
// Settings are iterated in sorted key order so replay is deterministic and
// testable.
func (s *SessionState) Replay(ctx context.Context, up *chproto.Codec) error {
	return s.replay(ctx, up, true)
}

// ReplaySettings re-applies only session-level settings to a newly bound
// upstream. It intentionally skips Database replay for rebind paths whose
// ClientHello already selected the desired upstream database and whose
// triggering client USE query will still flow through the normal query path.
func (s *SessionState) ReplaySettings(ctx context.Context, up *chproto.Codec) error {
	return s.replay(ctx, up, false)
}

func (s *SessionState) replay(ctx context.Context, up *chproto.Codec, includeDatabase bool) error {
	s.mu.RLock()
	db := s.Database
	settings := make(map[string]chproto.Setting, len(s.Settings))
	for k, v := range s.Settings {
		settings[k] = v
	}
	s.mu.RUnlock()

	if includeDatabase && db != "" {
		q := &chproto.Query{
			Body: fmt.Sprintf("USE %s", db),
			Info: chproto.ClientInfo{
				ProtocolVersion: up.Revision(),
				Interface:       proto.InterfaceTCP,
				Query:           proto.ClientQueryInitial,
			},
		}
		if err := up.WriteQuery(q); err != nil {
			return fmt.Errorf("replay USE: %w", err)
		}
	}

	keys := make([]string, 0, len(settings))
	for k := range settings {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := settings[k]
		q := &chproto.Query{
			Body: fmt.Sprintf("SET %s=%v", k, v.Value),
			Info: chproto.ClientInfo{
				ProtocolVersion: up.Revision(),
				Interface:       proto.InterfaceTCP,
				Query:           proto.ClientQueryInitial,
			},
		}
		if err := up.WriteQuery(q); err != nil {
			return fmt.Errorf("replay SET %s: %w", k, err)
		}
	}
	return nil
}
