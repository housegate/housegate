// Package chsession holds per-connection ClickHouse proxy state and the
// Session interface that carries it alongside client and upstream codecs.
//
// Design: docs/superpowers/specs/2026-04-21-clickhouse-tcp-conn-interface-design.md
package chsession

// IdentityClaims is the subset of JWS/JWT claims the rest of the proxy
// cares about. It is populated by the JWSValidator plugin after successful
// verification of the client's signing token.
type IdentityClaims struct {
	TenantID string
	UserID   string
	Role     string
	// additional fields may be added as plugins demand
}

// IsAnonymous reports whether no identity was established.
func (c IdentityClaims) IsAnonymous() bool {
	return c.TenantID == "" && c.UserID == ""
}
