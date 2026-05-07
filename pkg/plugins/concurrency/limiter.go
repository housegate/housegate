// Package concurrency implements a multi-dimension concurrency-limit
// Plugin backed by a Redis + Lua limiter (see redislimiter.go).
//
// Each authenticated Query invokes every configured DimensionResolver to
// build a list of (dimension, value, quota) tuples; the limiter then
// takes a permit across every non-empty dimension atomically. The permit
// is released when Relay observes the query's lifecycle ending — through
// OnQueryComplete in the normal path or OnClose for client disconnects
// mid-stream.
//
// Dimensions are open-ended: adding a new one (e.g. stake-token level)
// is a single resolver-fn change. Dimensions whose resolver returns an
// empty Value are skipped, which lets resolvers be installed before
// their data source is ready (see resolvers.go for the placeholder
// NoneStakeLevelResolver).
package concurrency

import (
	"context"
	"errors"
	"fmt"
	"sync"

	log "sentioxyz/sentio-core/common/log"

	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/plugin"
)

// DimensionResolver computes one Dimension from a query in progress.
// Returning a Dimension with an empty Value tells the Plugin to skip
// this dimension for this query, so resolvers can be wired in before
// their underlying data source exists.
type DimensionResolver func(sess chsession.Session, qctx *plugin.QueryContext) Dimension

// Plugin holds permits in-process keyed by session ID. The TCP protocol
// guarantees at most one in-flight Query per connection at a time, so a
// single permit per session is sufficient.
type Plugin struct {
	// Limiter performs the Redis-backed permit accounting. Required when
	// the plugin is registered; nil makes OnQuery a no-op so the chain
	// can be assembled before construction is finalised.
	Limiter Limiter

	// Resolvers contribute one Dimension each per query. An empty slice
	// makes the plugin a no-op (every query passes without limiting).
	Resolvers []DimensionResolver

	// FailOpen controls behaviour when the limiter returns
	// ErrAcquireFailed (typically a Redis outage). True allows the query
	// through with a warn log; false rejects it.
	FailOpen bool

	permits sync.Map // sess.ID() (int64) → Permit
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.Limiter == nil || len(p.Resolvers) == 0 {
		return nil
	}

	dims := make([]Dimension, 0, len(p.Resolvers))
	for _, resolve := range p.Resolvers {
		d := resolve(qctx.Session, qctx)
		if d.Value == "" {
			continue
		}
		dims = append(dims, d)
	}
	if len(dims) == 0 {
		return nil
	}

	permit, err := p.Limiter.Acquire(ctx, dims)
	_, logger := log.FromContext(ctx)
	if errors.Is(err, ErrAcquireFailed) {
		logger.Warnfe(err, "concurrency limiter: infrastructure error fail_open=%v", p.FailOpen)
		if p.FailOpen {
			return nil
		}
		return fmt.Errorf("concurrency limiter unavailable: %w", err)
	}
	if err != nil {
		// ErrQuotaExceeded — already names the offending dimension.
		return fmt.Errorf("concurrency quota exceeded: %w", err)
	}
	if permit.IsZero() {
		// All dimensions were skipped server-side (shouldn't happen given
		// our pre-filter, but defensive).
		return nil
	}
	p.permits.Store(qctx.Session.ID(), permit)
	logger.Debugw("concurrency limiter: acquired", "limiter_id", permit.ID)
	return nil
}

// OnQueryComplete releases the permit acquired in OnQuery, if any.
// Calling it more than once for the same session is safe — only the
// first call actually releases.
func (p *Plugin) OnQueryComplete(ctx context.Context, sess chsession.Session) {
	v, ok := p.permits.LoadAndDelete(sess.ID())
	if !ok {
		return
	}
	pm := v.(Permit)
	p.Limiter.Release(ctx, pm)
	_, logger := log.FromContext(ctx)
	logger.Debugw("concurrency limiter: released", "limiter_id", pm.ID)
}

// OnClose releases any permit still held when the session tears down.
// Normal completion releases via OnQueryComplete; this hook is the
// safety net for client disconnects mid-query and for relay-level errors
// that bypass the OnQueryComplete fire points.
//
// Uses context.Background because the original request context is
// typically already cancelled by the time OnClose runs.
func (p *Plugin) OnClose(sess chsession.Session) {
	v, ok := p.permits.LoadAndDelete(sess.ID())
	if !ok {
		return
	}
	pm := v.(Permit)
	p.Limiter.Release(context.Background(), pm)
	log.Debugw("concurrency limiter: released via on-close safety net",
		"conn", sess.ID(),
		"limiter_id", pm.ID,
	)
}

var (
	_ plugin.QueryPlugin         = (*Plugin)(nil)
	_ plugin.QueryCompletePlugin = (*Plugin)(nil)
	_ plugin.ClosePlugin         = (*Plugin)(nil)
)
