// Package agent implements the agent-mode signer plugin, used in
// agent-mode proxies to authenticate every outgoing query against an
// upstream server-mode proxy.
//
// Unlike the route signer (which only signs queries belonging to a routed
// session) this plugin signs unconditionally, because every query an
// agent emits must be authenticated before it reaches the server-mode
// proxy.
package agent

import (
	"context"
	"fmt"

	"github.com/housegate/housegate/pkg/log"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
)

// Observer is the narrow metrics surface this plugin depends on. Left
// as an interface so the plugin has no hard dependency on pkg/proxy
// (which would cycle via pkg/plugin). *proxy.MetricsObserver satisfies
// it.
type Observer interface {
	AgentTokenInjected()
	AgentTokenError()
}

// Plugin signs each outgoing Query.Body and injects the JWS as the
// standard auth token setting. A nil Signer makes the plugin a no-op.
//
// Observer is optional — when nil, token metrics are not emitted.
//
// Owner is optional. When non-empty the plugin also injects an
// SQL_x_payer setting carrying the billed account, signalling to the
// upstream that the signer is an operator acting on behalf of Owner.
// Empty Owner leaves the upstream's default behaviour (signer pays
// for itself) intact. Validation lives in Config.Validate.
//
// IsDriver is optional. When true the plugin also injects an
// SQL_sentio_driver=1 setting, telling the upstream server-mode
// housegate that this connection carries indexer-driver traffic. The
// upstream's EthValidator additionally gates this setting on signer ==
// IndexerAddress, so the sidecar's PrivateKeyHex must be the indexer's
// own key for the bypass to take effect. Deployment-time flag — flips
// for sidecars co-located with an indexer driver, false elsewhere.
//
// Naming note: the wire-level setting is SQL_x_payer because the
// upstream usage plugin already consumes that key; the deployment-
// config layer uses "owner" to match the on-chain
// IsOperator(owner, operator) authorization model.
type Plugin struct {
	Signer   auth.Signer
	Observer Observer
	Owner    string
	IsDriver bool
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.Signer == nil || qctx.Query == nil {
		return nil
	}
	if err := p.refreshAuthToken(ctx, qctx); err != nil {
		return err
	}
	if p.Owner != "" {
		// Same Custom-string wrapping as the auth token: the upstream
		// usage plugin trims `'…'` before HexToAddress (see
		// pkg/plugins/usage/usage.go). The signer then becomes the
		// operator for this Owner, gated by IsOperator() on-chain.
		qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{
			Key:    auth.PayerSettingKey,
			Value:  "'" + p.Owner + "'",
			Custom: true,
		})
	}
	if p.IsDriver {
		// Mark this connection as indexer-driver traffic. EthValidator
		// on the upstream gates the bypass on signer == IndexerAddress
		// independently of this flag — so this is a "request the
		// bypass" signal, not a "claim the bypass" override. Same
		// Custom-string quoting as the auth token / payer above:
		// upstream's eth_validator.isDriver trims `'…'` before
		// truthy-matching.
		qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{
			Key:    auth.DriverSettingKey,
			Value:  "'1'",
			Custom: true,
		})
	}
	return nil
}

// OnQueryInputCompleteStrict refreshes the legacy query JWS after a deferred
// payload has been collected. Collection can outlive MaxTokenAge; replacing the
// setting here keeps the forwarded Query fresh while binding the same final SQL
// that OnQuery signed. Non-deferred queries keep the ordinary single-sign path.
func (p *Plugin) OnQueryInputCompleteStrict(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.DeferredInsert == nil || qctx.Query == nil || p.Signer == nil {
		return nil
	}
	return p.refreshAuthToken(ctx, qctx)
}

func (p *Plugin) refreshAuthToken(ctx context.Context, qctx *plugin.QueryContext) error {
	token, err := p.Signer.SignToken(qctx.Query.Body)
	if err != nil {
		if p.Observer != nil {
			p.Observer.AgentTokenError()
		}
		return fmt.Errorf("agent-sign: %w", err)
	}
	setting := chproto.Setting{
		Key: auth.AuthTokenSettingKey,
		// See routeplugin.Signer for the wrapping-in-quotes rationale:
		// Custom=true triggers Field::restoreFromDump which expects
		// `'…'` for a String. EthValidator strips the quotes back off.
		Value:  "'" + token + "'",
		Custom: true,
	}
	qctx.Query.Settings = replaceUniqueSetting(qctx.Query.Settings, setting)
	if p.Observer != nil {
		p.Observer.AgentTokenInjected()
	}
	_, logger := log.FromContext(ctx)
	logger.Debugw("agent signer: injected auth token", "query_id", qctx.Query.ID)
	return nil
}

func replaceUniqueSetting(settings []chproto.Setting, replacement chproto.Setting) []chproto.Setting {
	out := settings[:0]
	replaced := false
	for _, setting := range settings {
		if setting.Key != replacement.Key {
			out = append(out, setting)
			continue
		}
		if !replaced {
			out = append(out, replacement)
			replaced = true
		}
	}
	if !replaced {
		out = append(out, replacement)
	}
	return out
}

var (
	_ plugin.QueryPlugin                    = (*Plugin)(nil)
	_ plugin.QueryInputCompleteStrictPlugin = (*Plugin)(nil)
)
