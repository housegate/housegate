// Package peer owns the __peer__|<address> encoding used by housegate
// to authenticate one proxy to another at the ClickHouse native
// protocol handshake.
//
// When a server-mode proxy's rewriter emits a remote(callbackAddr, ...)
// clause that should land at another housegate proxy, it puts a
// peer-relay envelope into the user / password fields:
//
//	user     = "__peer__|<lowercased-eth-address>"
//	password = <peer-relay JWS produced by auth.PeerSigner>
//
// The receiving proxy's credential plugin recognises the peer.Prefix
// in OnHello, validates the JWS in the password against its
// PeerValidator, and marks the session SessionState.IsPeerTrusted on
// success.
//
// Shared with pkg/route via the same "|" delimiter — the two
// envelopes are designed to nest: an upstream rewriter wraps a peer
// envelope inside a route envelope so a single ClickHouse remote()
// call can both target the right peer and authenticate as a peer:
//
//	"__route__|10.0.0.8:9001|__peer__|0xabcdef..."
//	            └─ target ─┘ └────── realUser ──────┘
//
// The route Stripper on the local proxy peels the outer route layer;
// the peer credential plugin on the remote proxy peels the inner peer
// layer.
//
// Nothing in pkg/peer imports pkg/proxy or plugin packages; the
// dependency flows the other way.
package peer

import (
	"strings"

	"github.com/housegate/housegate/pkg/route"
)

// Prefix is the marker the peer encoding puts in front of the signer
// address inside the Hello user field. Exposed for callers that need
// to construct or recognise peer-encoded user strings.
const Prefix = "__peer__"

// Delim is the field separator. Re-exported from pkg/route so
// callers don't have to depend on both packages just to assemble or
// disassemble a peer envelope.
const Delim = route.Delim

// ForwardedMarker is the optional third token that distinguishes a
// forward-pivot peer envelope ("__peer__|<addr>|forwarded") from the
// legacy two-token form ("__peer__|<addr>", emitted by the rewriter
// for cross-shard remote() calls). The receiving credential plugin
// uses this distinction to decide whether the inbound SQL has already
// been rewritten by the entry proxy (legacy: yes; forwarded: no).
const ForwardedMarker = "forwarded"

// FormatUser builds the peer-encoded user string for the given signer
// address. The address is expected lowercased and 0x-prefixed; this
// function does not normalize.
//
// Use FormatUserForwarded when the connection is a forward-decision
// pivot rather than a rewriter-emitted remote() loopback — see
// ForwardedMarker.
func FormatUser(address string) string {
	return Prefix + Delim + address
}

// FormatUserForwarded builds the peer-encoded user string with the
// ForwardedMarker appended. The receiving proxy uses this signal to
// run rewrite/auth on the session because the SQL is the client's
// raw form, not the rewriter's already-prefixed output.
func FormatUserForwarded(address string) string {
	return Prefix + Delim + address + Delim + ForwardedMarker
}

// ParseUser reads a peer-encoded user string and returns
// (address, forwarded, true) when it recognises the encoding. Returns
// ("", false, false) for plain user strings.
//
// Recognised forms:
//
//	__peer__|<address>             → (address, false, true)   (legacy / remote())
//	__peer__|<address>|forwarded   → (address, true,  true)   (forward pivot)
//
// Anything else (empty address, unknown trailing tokens, wrong
// prefix) returns isPeer=false.
func ParseUser(user string) (address string, forwarded bool, isPeer bool) {
	if !strings.HasPrefix(user, Prefix+Delim) {
		return "", false, false
	}
	rest := user[len(Prefix)+len(Delim):]
	if rest == "" {
		return "", false, false
	}
	// At most one optional trailer is allowed: the ForwardedMarker.
	// Splitting on Delim with a cap of 2 keeps any future field
	// additions out of the legacy parse path until they're explicitly
	// handled here.
	parts := strings.SplitN(rest, Delim, 2)
	address = parts[0]
	if address == "" {
		return "", false, false
	}
	if len(parts) == 1 {
		return address, false, true
	}
	if parts[1] == ForwardedMarker {
		return address, true, true
	}
	// Unrecognised trailer — refuse to parse so unknown extensions
	// can't silently downgrade to the legacy form.
	return "", false, false
}
