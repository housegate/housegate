package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
)

// agentPrepareResult is owned by clientToUpstream.  The worker only delivers
// PreparedAgentQuery on its channel; it never sees a socket or live qctx.
type agentPrepareResult struct {
	prepared   plugin.PreparedAgentQuery
	plan       *plugin.AgentPreparePlan
	queryID    string
	generation uint64
}

// nextGeneration is intentionally separate from activeQueryID.  Query IDs are
// client controlled and can repeat; the monotonically increasing generation
// makes a late worker result incapable of attaching to a later query.
func (r *Relay) nextAgentPrepareGeneration() uint64 {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	r.agentGeneration++
	return r.agentGeneration
}

func (r *Relay) agentPrepareLive(queryID string, generation uint64) bool {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	return r.activeQuery && !r.queryCanceled && r.activeQueryID == queryID && r.agentGeneration == generation
}

func (r *Relay) winAgentForwardGate(queryID string, generation uint64) bool {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	if !r.activeQuery || r.queryCanceled || r.activeQueryID != queryID || r.agentGeneration != generation {
		return false
	}
	r.agentForwardGeneration = generation
	return true
}

// waitAgentPrepare keeps the existing clientToUpstream goroutine as the only
// framed reader while the worker is blocked in acquire/finalize.  Packet-start
// polling never consumes a partial packet, so a worker result can be applied
// without a second codec reader.  A cancellation/EOF wins before any result is
// applied or any upstream Query bytes are written.
func (r *Relay) waitAgentPrepare(ctx context.Context, qctx *plugin.QueryContext, generation uint64) (*agentPrepareResult, error) {
	plan := qctx.AgentPrepare
	if plan == nil || plan.Prepare == nil {
		return nil, errors.New("missing agent prepare plan")
	}
	workerCtx, stop := context.WithCancel(ctx)
	defer stop()
	type outcome struct {
		prepared plugin.PreparedAgentQuery
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		prepared, err := plan.Prepare(workerCtx)
		done <- outcome{prepared: prepared, err: err}
	}()

	client := r.sess.Client()
	for {
		select {
		case out := <-done:
			if !r.agentPrepareLive(qctx.Query.ID, generation) {
				r.reconcileAgentPrepareCancel(plan)
				return nil, errAgentPrepareCanceled
			}
			if out.err != nil {
				return nil, fmt.Errorf("agent prepare: %w", out.err)
			}
			if out.prepared.Query == nil {
				return nil, errors.New("agent prepare returned nil query")
			}
			return &agentPrepareResult{prepared: out.prepared, plan: plan, queryID: qctx.Query.ID, generation: generation}, nil
		default:
		}

		ready, err := client.WaitForPacketStart(10 * time.Millisecond)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				r.cancelAgentPrepare(qctx.Query.ID, generation, plan)
				return nil, io.EOF
			}
			return nil, fmt.Errorf("wait for preparation control packet: %w", err)
		}
		if !ready {
			continue
		}
		pkt, err := client.ReadPacketWithLimit(plan.MaxControlBytes, uint64(chproto.ClientQueryCode))
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				r.cancelAgentPrepare(qctx.Query.ID, generation, plan)
			}
			return nil, err
		}
		if pkt.Type == uint64(chproto.ClientCancelCode) {
			r.cancelAgentPrepare(qctx.Query.ID, generation, plan)
			return nil, errAgentPrepareCanceled
		}
		// Query-only input has no payload lane.  Do not splice an early Data
		// packet while the signed identity is still being prepared.
		return nil, fmt.Errorf("client packet %s during agent preparation", clientPacketName(pkt.Type))
	}
}

var errAgentPrepareCanceled = errors.New("agent preparation canceled")
var errAgentPrepareForwardUnknown = errors.New("agent forwarding requires reconciliation")

func (r *Relay) cancelAgentPrepare(queryID string, generation uint64, plan *plugin.AgentPreparePlan) {
	r.queryMu.Lock()
	forwardWon := r.agentForwardGeneration == generation
	if r.activeQuery && r.activeQueryID == queryID && r.agentGeneration == generation && !forwardWon {
		r.queryCanceled = true
	}
	r.queryMu.Unlock()
	if !forwardWon {
		r.reconcileAgentPrepareCancel(plan)
	}
	// A stage callback can be non-cooperative.  It may not share the session
	// with another query after cancellation, so quarantine the connection until
	// the stage join below has completed (or forever, if it never returns).
	_ = r.sess.Close()
}

func (r *Relay) reconcileAgentPrepareCancel(plan *plugin.AgentPreparePlan) {
	if plan == nil || plan.ReconcileCancel == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = plan.ReconcileCancel(ctx)
	}()
}

// runAgentPrepareStage runs a potentially blocking coordinator stage while a
// single, temporary control-reader remains responsible for the client codec.
// It is used for continuation and durable journal callbacks as well as the
// detached Prepare worker.  The stage receives only a context; it must not
// access the codec.  At most one invocation of this helper is live from
// clientToUpstream, so there is never a second framed reader.
func (r *Relay) runAgentPrepareStage(ctx context.Context, result *agentPrepareResult, action func(context.Context) error) error {
	if result == nil || !r.agentPrepareLive(result.queryID, result.generation) {
		return errAgentPrepareCanceled
	}
	stageCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- action(stageCtx) }()
	client := r.sess.Client()
	canceled := false
	forwardWon := r.agentForwardGeneration == result.generation
	for {
		select {
		case err := <-done:
			if canceled {
				if forwardWon {
					return errAgentPrepareForwardUnknown
				}
				return errAgentPrepareCanceled
			}
			if !r.agentPrepareLive(result.queryID, result.generation) {
				r.reconcileAgentPrepareCancel(result.plan)
				return errAgentPrepareCanceled
			}
			return err
		default:
		}
		ready, err := client.WaitForPacketStart(10 * time.Millisecond)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				r.cancelAgentPrepare(result.queryID, result.generation, result.plan)
				canceled = true
				stop()
				// Do not let a callback outlive Relay cleanup.  The closed session
				// is the fence for a non-cooperative callback.
				<-done
				if forwardWon {
					return errAgentPrepareForwardUnknown
				}
				return io.EOF
			}
			return fmt.Errorf("wait for agent preparation stage control packet: %w", err)
		}
		if !ready {
			continue
		}
		pkt, err := client.ReadPacketWithLimit(result.plan.MaxControlBytes, uint64(chproto.ClientQueryCode))
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				r.cancelAgentPrepare(result.queryID, result.generation, result.plan)
			}
			return err
		}
		if pkt.Type == uint64(chproto.ClientCancelCode) {
			r.cancelAgentPrepare(result.queryID, result.generation, result.plan)
			canceled = true
			stop()
			<-done
			if forwardWon {
				return errAgentPrepareForwardUnknown
			}
			return errAgentPrepareCanceled
		}
		return fmt.Errorf("client packet %s during agent preparation stage", clientPacketName(pkt.Type))
	}
}

// applyAgentPrepare only runs in Relay after the generation gate remains live.
// It applies detached data, resumes the remaining hooks once, and records the
// non-authorizing ForwardIntent.  The caller must recheck before the final
// authorization/write transition.
func (r *Relay) applyAgentPrepare(ctx context.Context, qctx *plugin.QueryContext, result *agentPrepareResult) error {
	if result == nil || !r.agentPrepareLive(result.queryID, result.generation) {
		return errAgentPrepareCanceled
	}
	qctx.Query = result.prepared.Query
	if qctx.Values == nil {
		qctx.Values = make(map[string]any)
	}
	if result.prepared.Claimed {
		qctx.Values[plugin.SnapshotQueryAgentKey] = true
	}
	if err := r.runAgentPrepareStage(ctx, result, func(stageCtx context.Context) error {
		return r.hooks.ResumeQuery(stageCtx, qctx)
	}); err != nil {
		return fmt.Errorf("resume query hooks: %w", err)
	}
	if !r.agentPrepareLive(result.queryID, result.generation) {
		return errAgentPrepareCanceled
	}
	if err := r.runAgentPrepareStage(ctx, result, func(stageCtx context.Context) error {
		return result.plan.PersistForwardIntent(stageCtx, result.prepared)
	}); err != nil {
		return fmt.Errorf("persist forward intent: %w", err)
	}
	return nil
}

// authorizeAgentForward is the final serialized launch gate.  Authorization
// persistence happens after the winner is chosen and outside the gate; a
// failed or unknown persistence result forbids writes.  A later cancel does
// not reverse a winner, but it still prevents client delivery via the ordinary
// active-query cancellation path.
func (r *Relay) authorizeAgentForward(ctx context.Context, result *agentPrepareResult) error {
	if result == nil || !r.winAgentForwardGate(result.queryID, result.generation) {
		return errAgentPrepareCanceled
	}
	if err := r.runAgentPrepareStage(ctx, result, func(stageCtx context.Context) error {
		return result.plan.AuthorizeForward(stageCtx, result.prepared)
	}); err != nil {
		if errors.Is(err, errAgentPrepareForwardUnknown) {
			if reconcileErr := result.plan.PersistForwardUnknown(context.Background(), result.prepared); reconcileErr != nil {
				return fmt.Errorf("persist forward unknown after authorized gate: %w", reconcileErr)
			}
			return errAgentPrepareForwardUnknown
		}
		return fmt.Errorf("persist forward authorization: %w", err)
	}
	return nil
}
