package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
)

var (
	errQueryOnlyCanceled           = errors.New("query-only execution canceled")
	errQueryOnlyLifecycleFinalized = errors.New("query-only lifecycle already finalized")
)

// This is only the reader's liveness poll while Run is outstanding. It is
// never used to decide whether local success may be exposed.
const queryOnlyControlPoll = 10 * time.Millisecond

// consumeQueryOnlyMarker accepts only the native empty, unnamed ClientData
// terminator. It is deliberately stricter than ClientDataPacketIsEmpty: a
// named empty block is an external temporary table and must fail closed.
func consumeQueryOnlyMarker(pkt *chproto.Packet, compression proto.Compression, seen *bool) (bool, error) {
	if pkt.Type != uint64(chproto.ClientDataCode) {
		return false, nil
	}
	info, err := chproto.InspectClientDataPacket(pkt.Raw, compression)
	if err != nil {
		return false, fmt.Errorf("classify query-only client Data: %w", err)
	}
	if info.BlockName != "" || !info.Empty {
		return false, errors.New("query-only execution received nonempty or external-table client Data")
	}
	if *seen {
		return false, errors.New("query-only execution received more than one empty client Data marker")
	}
	*seen = true
	return true, nil
}

// runQueryOnly owns one locally completed query. The client loop remains the
// only codec reader while Run may block on durable host work. No upstream codec
// is consulted or written by this path.
func (r *Relay) runQueryOnly(ctx context.Context, qctx *plugin.QueryContext) error {
	if qctx == nil || qctx.Query == nil || qctx.QueryOnly == nil || !qctx.QueryOnly.ValidHostPlan() {
		return errors.New("invalid query-only host plan")
	}
	if !r.beginActiveQuery(qctx.Query.ID) {
		return fmt.Errorf("query %q raced with another active query", qctx.Query.ID)
	}

	plan := qctx.QueryOnly
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- plan.Run(runCtx) }()

	cancelled := false
	cancelClient := func() {
		if cancelled {
			return
		}
		cancelled = true
		r.cancelActiveQuery()
		stop()
		plan.CancelClient()
	}
	finishCanceled := func(runDone bool) error {
		if !runDone {
			<-done
		}
		r.takeActiveQuery()
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		// Cancellation ends at the same supported terminal boundary as local
		// success, so it carries plan D2's one late-marker allowance too.
		r.markQueryOnlyLateMarkerAllowed()
		if err := r.sess.Client().WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			return fmt.Errorf("%w: write query-only cancellation end-of-stream: %w", errQueryOnlyLifecycleFinalized, err)
		}
		return nil
	}
	markerSeen := false
	readControl := func() (*chproto.Packet, error) {
		pkt, err := r.sess.Client().ReadPacketWithLimit(plan.MaxControlBytes(), uint64(chproto.ClientQueryCode))
		if err != nil {
			cancelClient()
			r.takeActiveQuery()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return nil, io.EOF
			}
			return nil, fmt.Errorf("read query-only control packet: %w", err)
		}
		return pkt, nil
	}
	finishSuccess := func() error {
		_, wasCanceled, active, _ := r.takeActiveQueryState()
		if !active || wasCanceled {
			return errQueryOnlyCanceled
		}
		// Local completion is not a framed upstream success: do not invoke
		// OnQuerySuccess, which may commit unrelated session state.
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.markQueryOnlyLateMarkerAllowed()
		if err := r.sess.Client().WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			return fmt.Errorf("%w: write query-only end-of-stream: %w", errQueryOnlyLifecycleFinalized, err)
		}
		// INSERT ... SELECT has no mandatory ClientData terminator, but a client
		// may still deliver this operation's empty external-table marker after
		// the local terminal. Plan D2 allows the caller to drain exactly one
		// such marker before the next Query packet; the caller retains the sole
		// reader throughout.
		return nil
	}
	fail := func(err error) error {
		// Once the host execution lane has claimed this query, a local failure
		// reaches the same wire boundary as local success: the client may have
		// pipelined this operation's empty marker. Plan D2's one-marker
		// allowance therefore also applies after the caller's Exception.
		r.takeActiveQuery()
		r.markQueryOnlyLateMarkerAllowed()
		return err
	}
	for {
		if cancelled {
			return finishCanceled(false)
		}
		select {
		case err := <-done:
			if err != nil {
				// The caller owns the single client Exception and the matching
				// abort/complete hooks for a failed local execution.
				return fail(fmt.Errorf("query-only execution: %w", err))
			}
			return finishSuccess()
		default:
		}

		ready, err := r.sess.Client().WaitForPacketStart(queryOnlyControlPoll)
		if err != nil {
			cancelClient()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return fail(io.EOF)
			}
			return fail(fmt.Errorf("wait for query-only control packet: %w", err))
		}
		if !ready {
			continue
		}
		pkt, err := readControl()
		if err != nil {
			return fail(err)
		}
		if pkt.Type == uint64(chproto.ClientCancelCode) {
			cancelClient()
			continue
		}
		if consumed, err := consumeQueryOnlyMarker(pkt, r.sess.Client().Compression(), &markerSeen); err != nil {
			cancelClient()
			return fail(err)
		} else if consumed {
			continue
		}
		cancelClient()
		return fail(fmt.Errorf("client packet %s during query-only execution", clientPacketName(pkt.Type)))
	}
}

func (r *Relay) markQueryOnlyLateMarkerAllowed() {
	r.queryMu.Lock()
	r.queryOnlyLateMarkerAllowed = true
	r.queryMu.Unlock()
}

// consumeQueryOnlyLateMarker applies the plan-D2 allowance. It returns
// drained=true for the one permitted late empty marker. A Query packet clears
// the allowance and is handled by the caller. Anything else while the
// allowance is set is a protocol violation and closes the connection.
func (r *Relay) consumeQueryOnlyLateMarker(pkt *chproto.Packet) (bool, error) {
	r.queryMu.Lock()
	allowed := r.queryOnlyLateMarkerAllowed
	if allowed {
		r.queryOnlyLateMarkerAllowed = false
	}
	r.queryMu.Unlock()
	if !allowed {
		return false, nil
	}
	switch pkt.Type {
	case uint64(chproto.ClientQueryCode):
		return false, nil
	case uint64(chproto.ClientDataCode):
		info, err := chproto.InspectClientDataPacket(pkt.Raw, r.sess.Client().Compression())
		if err != nil {
			return false, fmt.Errorf("classify late query-only marker: %w", err)
		}
		if info.BlockName != "" || !info.Empty {
			return false, errors.New("nonempty or external-table client Data after query-only local completion")
		}
		return true, nil
	default:
		return false, fmt.Errorf("client packet %s after query-only local completion; connection is not reusable", clientPacketName(pkt.Type))
	}
}
