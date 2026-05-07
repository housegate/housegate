package chsession

import "errors"

var (
	ErrNoUpstream   = errors.New("chsession: no upstream bound")
	ErrRebindDenied = errors.New("chsession: rebind denied")
)
