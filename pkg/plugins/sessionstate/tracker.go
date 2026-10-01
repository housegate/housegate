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

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/plugin"
)

// Plugin records ClientHello.Database into SessionState.LogicalDatabase.
// Zero value is valid.
type Plugin struct {
	Config Config

	// PhysicalDatabase mirrors rewriter.physical_database. A ClientHello
	// database equal to it is not a logical database: internal services
	// such as the Sentio indexer driver connect with the physical name and
	// address every table by a qualified logical name, so the session gets
	// no logical context (forward.Plugin short-circuits the same name).
	// Recording it would make every rewriter request carry a protected
	// database as its logical context, which the engines refuse for every
	// statement (spec 2026-09-26 T3). Empty disables the special case.
	PhysicalDatabase string
}

// OnHello captures hello.Database into SessionState.LogicalDatabase,
// except the configured physical database, which leaves the session
// without a logical context. hello.Database itself is left untouched
// here — the rewrite plugin owns the wire-level rewrite to the physical
// name.
func (p *Plugin) OnHello(_ context.Context, sess chsession.Session, hello *chproto.ClientHello) error {
	if hello.Database == "" {
		return nil
	}
	if p.PhysicalDatabase != "" && hello.Database == p.PhysicalDatabase {
		return nil
	}
	sess.State().SetLogicalDatabase(hello.Database)
	return nil
}

var _ plugin.HelloPlugin = (*Plugin)(nil)
