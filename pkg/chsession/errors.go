package chsession

import "errors"

var (
	ErrNoUpstream   = errors.New("chsession: no upstream bound")
	ErrRebindDenied = errors.New("chsession: rebind denied")

	// ErrUpstreamRevisionTooLow refuses an upstream switch whose negotiated
	// revision is below the client leg's: Relay re-frames packets between the
	// legs and cannot down-convert them.
	ErrUpstreamRevisionTooLow = errors.New("chsession: upstream revision below the client revision")
)
