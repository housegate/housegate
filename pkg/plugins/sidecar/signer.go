// Package sidecar implements SidecarSignerPlugin, used in sidecar-mode
// proxies to authenticate every outgoing query against an upstream
// server-mode proxy.
//
// Unlike the route signer (which only signs queries belonging to a routed
// session) this plugin signs unconditionally, because every query a
// sidecar emits must be authenticated before it reaches the server-mode
// proxy.
package sidecar

import (
	"context"
	"fmt"

	"sentioxyz/sentio-core/common/log"

	"housegate/housegate/pkg/auth"
	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugin"
)

// Observer is the narrow metrics surface this plugin depends on. Left
// as an interface so the plugin has no hard dependency on pkg/proxy
// (which would cycle via pkg/plugin). *proxy.MetricsObserver satisfies
// it.
type Observer interface {
	SidecarTokenInjected()
	SidecarTokenError()
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
// Naming note: the wire-level setting is SQL_x_payer because the
// upstream usage plugin already consumes that key; the deployment-
// config layer uses "owner" to match the on-chain
// IsOperator(owner, operator) authorization model.
type Plugin struct {
	Signer   auth.Signer
	Observer Observer
	Owner    string
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.Signer == nil || qctx.Query == nil {
		return nil
	}
	token, err := p.Signer.SignToken(qctx.Query.Body)
	if err != nil {
		if p.Observer != nil {
			p.Observer.SidecarTokenError()
		}
		return fmt.Errorf("sidecar-sign: %w", err)
	}
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{
		Key: auth.AuthTokenSettingKey,
		// See routeplugin.Signer for the wrapping-in-quotes rationale:
		// Custom=true triggers Field::restoreFromDump which expects
		// `'…'` for a String. EthValidator strips the quotes back off.
		Value:  "'" + token + "'",
		Custom: true,
	})
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
	if p.Observer != nil {
		p.Observer.SidecarTokenInjected()
	}
	_, logger := log.FromContext(ctx)
	logger.Debugw("sidecar signer: injected auth token", auth.AuthTokenSettingKey, token)
	return nil
}

var _ plugin.QueryPlugin = (*Plugin)(nil)
