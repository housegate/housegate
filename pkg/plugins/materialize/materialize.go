// Package materialize implements the agent-mode Phase-1 plugin that
// rewrites non-deterministic SQL functions (now()/rand()/generateUUIDv4()/…)
// to constants before the agent signer runs, so the signed and forwarded
// SQL are identical and deterministic. Fail-open: any materialization
// failure leaves the query body unchanged.
package materialize

import (
	"context"

	pb "github.com/housegate/rewriter-go/gen/pb"

	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/rewriter"
)

// Materializer is the SQL→SQL seam this plugin depends on. *rewriter's
// Materializer implementation satisfies it; tests inject a fake.
type Materializer interface {
	Materialize(ctx context.Context, sql string) (rewriter.MaterializeOutcome, error)
}

// Observer is the narrow metrics surface. *proxy.MetricsObserver satisfies
// it. Optional — a nil Observer disables metrics.
type Observer interface {
	MaterializeApplied()
	MaterializeNoop()
	MaterializeNonSuccess(code string)
	MaterializeCallError()
}

// Plugin rewrites qctx.Query.Body in place before the agent signer runs.
// A nil Materializer makes it a no-op.
type Plugin struct {
	Materializer Materializer
	Observer     Observer
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p.Materializer == nil || qctx.Query == nil || qctx.Query.Body == "" {
		return nil
	}
	_, logger := log.FromContext(ctx)
	out, err := p.Materializer.Materialize(ctx, qctx.Query.Body)
	if err != nil {
		if p.Observer != nil {
			p.Observer.MaterializeCallError()
		}
		logger.Warnw("materialize: call failed, forwarding original SQL", "err", err)
		return nil // fail-open
	}
	if out.Code != pb.MaterializeCode_MaterializeSuccess {
		if p.Observer != nil {
			p.Observer.MaterializeNonSuccess(out.Code.String())
		}
		logger.Warnw("materialize: engine non-success, forwarding original SQL",
			"code", out.Code.String(), "message", out.Message)
		return nil // fail-open
	}
	if out.Changed {
		qctx.Query.Body = out.SQL
		if p.Observer != nil {
			p.Observer.MaterializeApplied()
		}
		logger.Debugw("materialize: applied", "sql", out.SQL)
		return nil
	}
	if p.Observer != nil {
		p.Observer.MaterializeNoop()
	}
	return nil
}

var _ plugin.QueryPlugin = (*Plugin)(nil)
