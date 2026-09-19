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

var errQueryOnlyCanceled = errors.New("query-only execution canceled")

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
		r.markQueryOnlySessionTerminal()
		if err := r.sess.Client().WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			return fmt.Errorf("write query-only cancellation end-of-stream: %w", err)
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
		r.markQueryOnlySessionTerminal()
		if err := r.sess.Client().WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
			return fmt.Errorf("write query-only end-of-stream: %w", err)
		}
		// INSERT ... SELECT has no ClientData terminator. There is therefore no
		// wire boundary that can distinguish a delayed control packet for this
		// local operation from the next query. The caller must retain the sole
		// reader but never reuse this connection after this local success.
		return nil
	}
	for {
		if cancelled {
			return finishCanceled(false)
		}
		select {
		case err := <-done:
			if err != nil {
				r.takeActiveQuery()
				// The caller owns the single client Exception and the matching
				// abort/complete hooks for a failed local execution.
				return fmt.Errorf("query-only execution: %w", err)
			}
			return finishSuccess()
		default:
		}

		ready, err := r.sess.Client().WaitForPacketStart(queryOnlyControlPoll)
		if err != nil {
			cancelClient()
			r.takeActiveQuery()
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) {
				return io.EOF
			}
			return fmt.Errorf("wait for query-only control packet: %w", err)
		}
		if !ready {
			continue
		}
		pkt, err := readControl()
		if err != nil {
			return err
		}
		if pkt.Type == uint64(chproto.ClientCancelCode) {
			cancelClient()
			continue
		}
		if consumed, err := consumeQueryOnlyMarker(pkt, r.sess.Client().Compression(), &markerSeen); err != nil {
			cancelClient()
			r.takeActiveQuery()
			return err
		} else if consumed {
			continue
		}
		cancelClient()
		r.takeActiveQuery()
		return fmt.Errorf("client packet %s during query-only execution", clientPacketName(pkt.Type))
	}
}

func (r *Relay) markQueryOnlySessionTerminal() {
	r.queryMu.Lock()
	r.queryOnlySessionTerminal = true
	r.queryMu.Unlock()
}

func (r *Relay) queryOnlySessionIsTerminal() bool {
	r.queryMu.Lock()
	defer r.queryMu.Unlock()
	return r.queryOnlySessionTerminal
}
