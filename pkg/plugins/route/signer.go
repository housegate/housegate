package routeplugin

import (
	"context"
	"fmt"

	"housegate/housegate/pkg/log"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugin"
)

// Observer is the narrow metrics surface this plugin depends on. Left
// as an interface so the plugin has no hard dependency on pkg/proxy
// (which would cycle via pkg/plugin). *proxy.MetricsObserver satisfies
// it. The proxy-to-proxy ("relay") signer reuses the same agent
// token counters because both emit identical JWS tokens — operators
// care about total JWS volume, not which signer path produced it.
type Observer interface {
	AgentTokenInjected()
	AgentTokenError()
}

// Signer signs every query in a routed session with the shared relay
// private key, injecting the JWS as the standard auth token setting so
// the receiving proxy can validate it via its normal JWS validation flow.
//
// Signer only signs sessions that Stripper marked with a route target; for
// regular sessions it is a no-op. A nil Signer field also makes the plugin
// a no-op (useful when relay signing is disabled by config).
//
// Observer is optional — when nil, token metrics are not emitted.
type Signer struct {
	Signer   auth.Signer
	Observer Observer
}

func (p *Signer) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.Signer == nil {
		return nil
	}
	// PluginChain has already filtered non-RouteAware plugins out on
	// routed sessions, so by the time we get here on a routed session
	// the work is unconditional. The explicit check is kept as a
	// belt-and-suspenders guard against being wired into a non-routing
	// chain by mistake — a stray Signer in a regular chain should still
	// be a no-op rather than signing every single query.
	if !qctx.Session.State().IsRouted() {
		return nil
	}

	token, err := p.Signer.SignToken(qctx.OriginalSQL)
	if err != nil {
		if p.Observer != nil {
			p.Observer.AgentTokenError()
		}
		return fmt.Errorf("relay signer: sign token: %w", err)
	}

	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{
		Key: auth.AuthTokenSettingKey,
		// Custom=true makes CH parse the value via Field::restoreFromDump
		// instead of looking the name up in its built-in-settings table.
		// restoreFromDump's String form is `'…'` (single-quoted, with `\\`
		// and `\'` escaped); the JWS we mint is base64url + `.` + `-` so
		// it never contains those, and a bare wrap is sufficient.
		// EthValidator.ValidateQuery already strings.Trims `"` and `'` off
		// the token so non-CH consumers see the raw JWS unchanged.
		Value:  "'" + token + "'",
		Custom: true,
	})
	if p.Observer != nil {
		p.Observer.AgentTokenInjected()
	}
	_, logger := log.FromContext(ctx)
	logger.Debugw("relay signer: injected relay token", auth.AuthTokenSettingKey, token)
	return nil
}

// RunOnRouted reports that Signer is the whole point of routed
// sessions and must keep firing once the chain has gated everything
// else off.
func (*Signer) RunOnRouted() bool { return true }

var (
	_ plugin.QueryPlugin = (*Signer)(nil)
	_ plugin.RouteAware  = (*Signer)(nil)
)
