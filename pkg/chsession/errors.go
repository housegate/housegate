package chsession

import "errors"

var (
	ErrNoUpstream   = errors.New("chsession: no upstream bound")
	ErrRebindDenied = errors.New("chsession: rebind denied")

	// ErrUpstreamRevisionTooLow refuses an upstream switch whose negotiated
	// revision differs from the client leg's: Relay forwards upstream packets
	// encoded at the upstream revision to a client parsing at its own, so the
	// two legs must run at exactly the same revision. SwitchUpstream clamps
	// the replayed hello to SessionState.ClientRevision, so a differing
	// revision can only be a lower one.
	ErrUpstreamRevisionTooLow = errors.New("chsession: upstream revision below the client revision")

	// ErrUpstreamTimezoneMismatch refuses an upstream switch to a server whose
	// ServerHello timezone differs from the one the client already received
	// at its handshake: the client renders DateTime values with that timezone.
	ErrUpstreamTimezoneMismatch = errors.New("chsession: upstream timezone differs from the client's server timezone")
)
