// Package route implements the proxy-to-proxy routing plugins.
//
// Two plugins live here because they share the same routing convention:
//
//   - Stripper (HelloPlugin) detects a "__route__|<target>|<realUser>"
//     prefix in ClientHello.User. When found, it strips the prefix in
//     place and stashes the target proxy address into SessionState so the
//     dialer can connect to the right peer instead of going through the
//     local cluster manager.
//
//   - Signer (QueryPlugin) signs every query in a routed session with the
//     shared relay private key, injecting the JWS as the standard auth
//     token setting. The receiving proxy validates the relay token through
//     its normal JWS validation flow.
//
// The two communicate via SessionState.RouteTarget, which Stripper sets
// during OnHello. RouteTarget exposes the same value to callers outside
// this package (notably the upstream dialer in cmd).
//
// Once Stripper marks a session as routed, PluginChain bypasses every
// non-RouteAware plugin in the query stages — auth, rewrite, state,
// usage, concurrency, etc. all stop firing for the rest of the
// connection's lifetime. Signer (which IS the routed flow) and the
// metrics plugin (operators want metrics on routed traffic) opt back
// in via the plugin.RouteAware interface.
package routeplugin

import (
	"housegate/housegate/pkg/chsession"
)

// RouteTarget returns the target proxy address stashed by Stripper, or
// empty string if the session is not a routed one.
func RouteTarget(s *chsession.SessionState) string {
	return s.GetRouteTarget()
}
