package routeplugin

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/log"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/route"
)

// Stripper detects the __route__|<target>|<realUser> prefix in
// ClientHello.User, strips it in place, and stashes the target proxy
// address in SessionState.RouteTarget so the upstream dialer and the
// PluginChain can both observe the routed status.
//
// Stripper itself is RouteAware — it must run during OnHello on routed
// sessions because it is the plugin that sets the routed flag in the
// first place. Plugins ordered after Stripper that aren't RouteAware
// are skipped automatically by PluginChain.
//
// Security note: Stripper does not enforce that the originating
// connection is local. The caller (typically cmd) is responsible
// for rejecting non-local clients before this plugin runs, since
// allowing any client to set a route target would be an SSRF vector.
type Stripper struct{}

func (Stripper) OnHello(ctx context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
	targetAddr, realUser, isRoute := route.ParseRouteFromUser(hello.User)
	if isRoute && sess.State().IsInternalPort {
		return fmt.Errorf("route envelope on internal-port (target=%q user=%q): "+
			"internal-port must never receive __route__; check peer config or firewall",
			targetAddr, realUser)
	}
	if !isRoute {
		return nil
	}
	_, logger := log.FromContext(ctx)
	logger.Debugw("route stripper: extracted route target",
		"target_addr", targetAddr,
		"real_user", realUser,
	)
	hello.User = realUser
	sess.State().SetRouteTarget(targetAddr)
	return nil
}

// RunOnRouted reports that Stripper stays in the OnHello chain on routed
// sessions — it has to, because it's the plugin that marks them routed.
// Idempotent: a connection that arrives already-marked just doesn't
// match the prefix and falls through to a no-op.
func (Stripper) RunOnRouted() bool { return true }

var (
	_ plugin.HelloPlugin = Stripper{}
	_ plugin.RouteAware  = Stripper{}
)
