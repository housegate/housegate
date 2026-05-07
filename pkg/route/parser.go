// Package route owns the __route__ encoding used by housegate for
// proxy-to-proxy routing through the ClickHouse native protocol.
//
// ClickHouse remote() calls accept only a single user field; housegate
// smuggles a target-proxy address through it by prefixing with
// "__route__|<target>|<realUser>". Both the proxy performing the route
// setup and the proxy receiving the redirected connection agree on this
// convention here.
//
// The pipe ("|") delimiter is shared with the __peer__ login encoding
// so the two routing/auth envelopes have a uniform shape.
//
// Nothing in pkg/route imports pkg/proxy or plugin packages; the
// dependency flows the other way.
package route

import "strings"

// Prefix is the marker the route encoding puts in front of the target
// address inside the Hello user field. Exposed for callers that need
// to construct or recognise route-encoded user strings.
const Prefix = "__route__"

// Delim is the field separator used inside route- and peer-encoded
// user strings. Kept as a package constant so other encodings that
// share the same envelope shape can refer to it.
const Delim = "|"

// FormatRouteUser builds the route-encoded user string for the given
// target proxy address and the underlying real ClickHouse user.
//
// The resulting string is suitable for use as the user argument of a
// ClickHouse remote() call where the target is another housegate
// proxy: the receiving Stripper will recognise the prefix, set
// SessionState.RouteTarget = targetAddr, and restore hello.User to
// realUser before downstream plugins run.
func FormatRouteUser(targetAddr, realUser string) string {
	return Prefix + Delim + targetAddr + Delim + realUser
}

// ParseRouteFromUser reads a route-encoded user string and returns
// (targetAddr, realUser, true) when it recognises the encoding. It
// returns ("", "", false) for plain user strings.
//
// Format: __route__|<target>|<realUser>
// Example: "__route__|10.0.0.8:9001|default" → ("10.0.0.8:9001", "default", true)
//
// The split is greedy on the realUser side: a realUser that itself
// contains "|" is preserved verbatim (mirroring how ClickHouse user
// names are otherwise opaque to this layer).
func ParseRouteFromUser(user string) (targetAddr, realUser string, isRoute bool) {
	if !strings.HasPrefix(user, Prefix+Delim) {
		return "", "", false
	}
	rest := user[len(Prefix)+len(Delim):] // e.g. "10.0.0.8:9001|default"
	idx := strings.Index(rest, Delim)
	if idx < 0 {
		return "", "", false
	}
	targetAddr = rest[:idx]
	realUser = rest[idx+len(Delim):]
	if targetAddr == "" || realUser == "" {
		return "", "", false
	}
	return targetAddr, realUser, true
}
