// Package sessionstate implements the state-tracker plugin. After the
// rewriter migration its single job is OnHello: capture the database
// the user advertised in ClientHello and mirror it into
// SessionState.LogicalDatabase so the rewriter (which runs later, in
// OnQuery) sees the correct logical-database context.
//
// Mid-session `USE <name>` updates are NOT observed by this plugin —
// the rewriter itself reads its own response's `database_rewrites`
// and `statement_type` and calls back into SessionState when it
// classifies the SQL as STATEMENT_TYPE_USE. That keeps state-tracking
// in one place (no second regex on the hot path) and inherits the
// rewriter's parser for edge cases.
package sessionstate

import (
	"context"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
)

// Plugin records ClientHello.Database into SessionState.LogicalDatabase.
// Zero value is valid; there is currently no operator-tunable surface.
type Plugin struct {
	Config Config
}

// OnHello captures hello.Database into SessionState.LogicalDatabase.
// hello.Database itself is left untouched here — the rewrite plugin
// owns the wire-level rewrite to the physical name.
func (p *Plugin) OnHello(_ context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
	if hello.Database == "" {
		return nil
	}
	sess.State().SetLogicalDatabase(hello.Database)
	return nil
}

var _ plugin.HelloPlugin = (*Plugin)(nil)
