package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
)

// signedInsertForward owns the post-input forwarding of a signed INSERT.
type signedInsertForward struct {
	lane          string
	forwardCancel bool
	sampleColumns []chproto.SampleColumn

	compression               proto.Compression
	markerRaw, terminatorRaw  []byte
	payload                   [][]byte
	payloadBytes              uint64
	rejectClose, rejectResume func(error) error
}

func (r *Relay) forwardSignedInsert(ctx context.Context, qctx *plugin.QueryContext, fw signedInsertForward) error {
	_, logger := log.FromContext(ctx)
	client := r.sess.Client()
	q := qctx.Query
	if err := r.hooks.OnQueryInputCompleteStrict(ctx, qctx); err != nil {
		if chproto.KeepsSession(err) {
			return fw.rejectResume(fmt.Errorf("query input complete strict hook: %w", err))
		}
		return fw.rejectClose(fmt.Errorf("query input complete strict hook: %w", err))
	}

	up := r.sess.Upstream()
	if up == nil {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return chsession.ErrNoUpstream
	}
	up.SetCompression(fw.compression)
	if !r.beginActiveQuery(q.ID) {
		return fw.rejectClose(fmt.Errorf("query %q raced with another active upstream query", q.ID))
	}
	inputGate := r.armDeferredInput(qctx)
	defer r.finishDeferredInput(inputGate)
	gate := r.armDeferredSample(q.ID)
	abortFromWriter := func(stage string, cause error) error {
		r.disarmDeferredSample()
		if !inputGate.claimWriterLifecycle() {
			// The upstream reader won the single lifecycle owner CAS. Close the
			// downstream so a pending terminal write cannot block, release writer
			// ownership, and wait for that owner to settle exactly once.
			closeCodec(client)
			r.finishDeferredInput(inputGate)
			select {
			case <-inputGate.delivered:
				return fw.errf("%q %s after upstream lifecycle ownership: %w", q.ID, stage, cause)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		inputGate.stopWriter()
		closeCodec(up)
		inputGate.abort(ctx, r.hooks)
		r.finishDeferredInput(inputGate)
		r.sess.State().ClearActiveRewrite()
		r.takeActiveQuery()
		inputGate.complete(ctx, r.hooks, r.sess)
		// Retain the writer-owned tombstone until the upstream reader exits so
		// an already-decoded packet cannot be mistaken for a fresh lifecycle.
		inputGate.markDelivered()
		return fw.errf("%q %s: %w", q.ID, stage, cause)
	}
	forwardFail := func(stage string, err error) error {
		return abortFromWriter(fmt.Sprintf("%s to %s", stage, upstreamAddr(up)), err)
	}
	abortAfterTerminal := func(stage string) error {
		r.finishDeferredInput(inputGate)
		select {
		case <-inputGate.delivered:
			return fw.errf("%q terminated while %s", q.ID, stage)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	abortFromClient := func(cause error) error {
		return abortFromWriter("client terminated while awaiting upstream", cause)
	}
	// Cancellation and terminal decode arbitrate before either changes lifecycle
	// semantics. A terminal racing the write waits for its actual result: observing
	// Cancel alone must never make a premature EOS reusable.
	forwardCancel := func(raw []byte, stage string) error {
		if !fw.forwardCancel {
			return abortFromClient(fmt.Errorf("client cancelled %s%s", fw.lane, stage))
		}
		if inputGate.beginCancel(r.cancelActiveQuery) {
			err := up.WriteRawPacket(raw)
			inputGate.finishCancel(err)
			r.finishDeferredInput(inputGate)
			if err != nil {
				return forwardFail("forward cancel", err)
			}
		} else {
			r.finishDeferredInput(inputGate)
		}
		return nil
	}
	// Raw buffered Data uses the client's original framing regardless of any
	// QueryPlugin mutation, exactly like the ordinary relay path.
	q.Compression = fw.compression
	if err := up.WriteQuery(q); err != nil {
		return forwardFail("forward query", err)
	}
	markerRaw := fw.markerRaw
	if err := up.WriteRawPacket(markerRaw); err != nil {
		if inputGate.terminalObserved() {
			return abortAfterTerminal("writing external-tables marker")
		}
		return forwardFail("forward external-tables marker", err)
	}
	inputGate.markMarkerWritten()
	var sampleResult deferredSampleResult
	for {
		select {
		case sampleResult = <-gate:
			goto sampleSettled
		case <-ctx.Done():
			return abortFromClient(ctx.Err())
		default:
		}

		// The same sole client reader that collected the body continues to
		// arbitrate liveness while ClickHouse prepares its sample. Otherwise a
		// client EOF/Cancel plus an upstream that never answers the sample leaks
		// both relay loops for the server-lifetime context.
		available, err := client.WaitForPacketStart(25 * time.Millisecond)
		if err != nil {
			return abortFromClient(err)
		}
		if !available {
			continue
		}
		pkt, decErr := client.ReadPacket(uint64(chproto.ClientQueryCode))
		switch {
		case decErr != nil && pkt == nil:
			return abortFromClient(decErr)
		case pkt == nil:
			return abortFromClient(io.EOF)
		case pkt.Type == uint64(chproto.ClientCancelCode):
			if err := forwardCancel(pkt.Raw, " while awaiting upstream sample"); err != nil {
				return err
			}
			goto awaitTerminal
		case decErr != nil:
			return abortFromClient(decErr)
		default:
			return abortFromClient(fmt.Errorf("client sent packet type %d (%s) before %s sample", pkt.Type, clientPacketName(pkt.Type), fw.lane))
		}
	}

sampleSettled:
	if sampleResult.err != nil {
		r.finishDeferredInput(inputGate)
		select {
		case <-inputGate.delivered:
			return fw.errf("%q sample negotiation: %w", q.ID, sampleResult.err)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if sampleResult.exception != nil {
		// upstreamToClient already forwarded the Exception and ran the
		// terminal hooks (takeActiveQuery + OnQueryComplete); only the
		// plugin-side buffer state is left to drop.
		logger.Debugw(fw.lane+" rejected by upstream at sample step", "query_id", q.ID, "code", sampleResult.exception.Code, "message", sampleResult.exception.Message)
		r.finishDeferredInput(inputGate)
		select {
		case <-inputGate.delivered:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if fw.sampleColumns != nil {
		got, err := parseSampleColumns(sampleResult.sampleRaw, up.Revision())
		if err != nil {
			return forwardFail("decode upstream sample block", err)
		}
		if err := matchSampleColumns(fw.sampleColumns, got); err != nil {
			r.writeExceptionToClient(ctx, fw.errf("%q upstream sample mismatch: %w", q.ID, err))
			return abortFromWriter("upstream sample mismatch", err)
		}
	}
	{
		for _, raw := range fw.payload {
			if inputGate.terminalObserved() {
				return abortAfterTerminal("forwarding payload")
			}
			if err := up.WriteRawPacket(raw); err != nil {
				if inputGate.terminalObserved() {
					return abortAfterTerminal("forwarding payload")
				}
				return forwardFail("forward payload", err)
			}
			if inputGate.terminalObserved() {
				return abortAfterTerminal("forwarding payload")
			}
		}
		var terminatorErr error
		if fw.terminatorRaw != nil {
			terminatorErr = up.WriteRawPacket(fw.terminatorRaw)
		} else {
			terminatorErr = up.WriteEmptyDataBlock()
		}
		if err := terminatorErr; err != nil {
			if inputGate.terminalObserved() {
				return abortAfterTerminal("writing terminator")
			}
			return forwardFail("forward terminator", err)
		}
		inputGate.markTerminatorWritten()
		if inputGate.terminalObserved() {
			return abortAfterTerminal("completing input")
		}
		r.hooks.OnQueryInputComplete(ctx, qctx)
		r.finishDeferredInput(inputGate)
		logger.Debugw(fw.lane+" forwarded", "query_id", q.ID, "packets", len(fw.payload), "payload_bytes", fw.payloadBytes)
	}
awaitTerminal:
	awaitTerminalExposure := func(prefetched *clientPacketResult) (bool, error) {
		if !inputGate.terminalExposed() && !inputGate.terminalClaimed() {
			return false, nil
		}
		if !inputGate.terminalExposed() {
			select {
			case <-inputGate.exposureDone:
			case <-inputGate.terminal:
				return true, abortAfterTerminal("awaiting terminal exposure")
			case <-ctx.Done():
				return true, ctx.Err()
			}
		}
		if inputGate.terminalObserved() {
			return true, abortAfterTerminal("awaiting terminal exposure")
		}
		if !inputGate.terminalExposed() {
			// The client-facing terminal write failed after it began. The
			// upstream loop owns the already-decided query lifecycle, but this
			// side must still stop instead of reading another packet forever.
			select {
			case <-inputGate.delivered:
				return true, fw.errf("terminal could not be exposed to client")
			case <-ctx.Done():
				return true, ctx.Err()
			}
		}
		if prefetched != nil {
			r.prefetchedClient = prefetched
		}
		select {
		case <-inputGate.terminal:
			return true, abortAfterTerminal("awaiting upstream terminal")
		case <-inputGate.delivered:
			if inputGate.terminalObserved() {
				return true, abortAfterTerminal("awaiting upstream terminal")
			}
			return true, nil
		case <-ctx.Done():
			return true, ctx.Err()
		}
	}
	for {
		if inputGate.terminalObserved() {
			return abortAfterTerminal("awaiting upstream terminal")
		}
		select {
		case <-inputGate.terminal:
			return abortAfterTerminal("awaiting upstream terminal")
		case <-inputGate.delivered:
			if inputGate.terminalObserved() {
				return abortAfterTerminal("awaiting upstream terminal")
			}
			return nil
		case <-ctx.Done():
			return abortFromClient(ctx.Err())
		default:
		}

		// Do not leave the client side parked once its payload terminator has
		// been written. A pending INSERT may otherwise outlive a disconnected
		// client forever. The bounded wait is only a cancellation polling
		// mechanism; unlike payload classification, it never changes protocol
		// meaning.
		available, err := client.WaitForPacketStart(25 * time.Millisecond)
		if err != nil {
			if handled, exposureErr := awaitTerminalExposure(nil); handled {
				return exposureErr
			}
			select {
			case <-inputGate.delivered:
				if inputGate.terminalObserved() {
					return abortAfterTerminal("awaiting upstream terminal")
				}
				return nil
			default:
				return abortFromClient(err)
			}
		}
		if !available {
			continue
		}
		pkt, decErr := client.ReadPacket(uint64(chproto.ClientQueryCode))
		if handled, exposureErr := awaitTerminalExposure(&clientPacketResult{pkt: pkt, err: decErr}); handled {
			return exposureErr
		}
		select {
		case <-inputGate.delivered:
			if inputGate.terminalObserved() {
				return abortAfterTerminal("awaiting upstream terminal")
			}
			// The upstream terminal became visible while this packet was being
			// framed. Preserve it for the ordinary client loop; it belongs to
			// the now-reusable session rather than the completed INSERT.
			r.prefetchedClient = &clientPacketResult{pkt: pkt, err: decErr}
			return nil
		default:
		}
		if decErr != nil && pkt == nil {
			return abortFromClient(decErr)
		}
		if pkt != nil && pkt.Type == uint64(chproto.ClientCancelCode) {
			if err := forwardCancel(pkt.Raw, ""); err != nil {
				return err
			}
			continue
		}
		if decErr != nil {
			return abortFromClient(decErr)
		}
		if pkt == nil {
			return abortFromClient(io.EOF)
		}
		return abortFromClient(fmt.Errorf("client sent packet type %d (%s) before %s terminal", pkt.Type, clientPacketName(pkt.Type), fw.lane))
	}
}

func (fw signedInsertForward) errf(format string, args ...any) error {
	return fmt.Errorf("%s "+format, append([]any{fw.lane}, args...)...)
}

// parseSampleColumns validates a decoding copy; captured sample bytes stay intact.
func parseSampleColumns(raw []byte, revision int) ([]chproto.SampleColumn, error) {
	if len(raw) == 0 {
		return nil, errors.New("upstream sample block was not captured")
	}
	normalized, err := chproto.NormalizeServerDataBlockInfo(raw, revision)
	if err != nil {
		return nil, err
	}
	r := proto.NewReader(bytes.NewReader(normalized))
	code, err := r.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("sample packet code: %w", err)
	}
	if code != uint64(proto.ServerCodeData) {
		return nil, fmt.Errorf("sample packet code %d is not ServerData", code)
	}
	if _, err := r.Str(); err != nil {
		return nil, fmt.Errorf("sample block name: %w", err)
	}
	if proto.FeatureBlockInfo.In(revision) {
		var info proto.BlockInfo
		if err := info.Decode(r); err != nil {
			return nil, err
		}
	}
	columns, err := r.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("sample columns: %w", err)
	}
	rows, err := r.UVarInt()
	if err != nil || rows != 0 {
		return nil, fmt.Errorf("sample block carries %d rows (err %v), want 0", rows, err)
	}
	// A valid pair needs at least two string length bytes. Bound allocation by
	// the already framed packet rather than trusting its declared column count.
	if columns > uint64(len(normalized))/2 {
		return nil, errors.New("sample column count exceeds packet size")
	}
	out := make([]chproto.SampleColumn, 0, columns)
	for i := uint64(0); i < columns; i++ {
		name, nameErr := r.Str()
		typ, typeErr := r.Str()
		if nameErr != nil || typeErr != nil {
			return nil, fmt.Errorf("sample column %d: %w", i, errors.Join(nameErr, typeErr))
		}
		if proto.FeatureCustomSerialization.In(revision) {
			if _, err := r.Bool(); err != nil {
				return nil, fmt.Errorf("sample column %d custom serialization flag: %w", i, err)
			}
		}
		out = append(out, chproto.SampleColumn{Name: name, Type: typ})
	}
	return out, nil
}

func matchSampleColumns(want, got []chproto.SampleColumn) error {
	for i := range want {
		if i >= len(got) {
			return fmt.Errorf("column %d %q is missing upstream (upstream has %d columns, plan has %d)",
				i, want[i].Name, len(got), len(want))
		}
		if got[i].Name != want[i].Name || got[i].Type != want[i].Type {
			return fmt.Errorf("column %d: upstream has %q %q, plan expects %q %q",
				i, got[i].Name, got[i].Type, want[i].Name, want[i].Type)
		}
	}
	if len(got) != len(want) {
		return fmt.Errorf("upstream has %d columns, plan has %d (first extra is %q)",
			len(got), len(want), got[len(want)].Name)
	}
	return nil
}

// synthesizedMarkerAllowance bounds the one client packet this lane reads: a
// single empty external-tables marker. Anything larger is an external table or
// a payload block, both of which the lane refuses.
const synthesizedMarkerAllowance uint64 = 4 << 10

// runSynthesizedInsert is runDeferredInsert in the reverse direction (spec
// 2026-09-23 D8): the rows come from an evaluated plan instead of the client.
// The client sent a complete inline INSERT ... VALUES statement plus exactly
// one empty Data marker, so the lane reads that marker, encodes the plan's
// blocks into client Data packets at the upstream codec's negotiated revision,
// and hands the shared forwarder a payload the strict hook signs before the
// Query reaches upstream.
func (r *Relay) runSynthesizedInsert(ctx context.Context, qctx *plugin.QueryContext, compression proto.Compression) error {
	_, logger := log.FromContext(ctx)
	client := r.sess.Client()
	plan := qctx.SynthesizedInsert
	q := qctx.Query
	rejectClose := func(err error) error {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.writeExceptionToClient(ctx, err)
		return err
	}
	// Nothing has been written upstream and the marker was consumed whole, so a
	// retryable rejection ends only this query and leaves both packet streams on
	// a clean boundary.
	rejectResume := func(err error) error {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		r.writeExceptionToClient(ctx, err)
		return fmt.Errorf("%w: %w", errQueryRejectedResume, err)
	}
	if len(plan.Blocks) == 0 || len(plan.SampleColumns) == 0 {
		return rejectClose(fmt.Errorf("query %q: synthesized INSERT plan carries %d blocks and %d sample columns, want both non-empty",
			q.ID, len(plan.Blocks), len(plan.SampleColumns)))
	}
	if compression == proto.CompressionEnabled {
		return rejectClose(fmt.Errorf("query %q: synthesized INSERT requires an uncompressed session", q.ID))
	}

	// 1. The client's single empty external-tables marker.
	var markerRaw []byte
	// Keep cancellation responsive even inside a fragmented marker or a
	// backpressured upstream write, without introducing another codec reader.
	cancelClosed := make(chan struct{})
	stopOnCancel := context.AfterFunc(ctx, func() {
		defer close(cancelClosed)
		closeCodec(client)
		closeCodec(r.sess.Upstream())
	})
	defer func() {
		// Stop a dormant callback, or join one already closing the codecs.
		// No old lane callback may survive handoff to the next query.
		if !stopOnCancel() {
			<-cancelClosed
		}
	}()
	for markerRaw == nil {
		pkt, decErr := client.ReadPacketWithDataLimit(synthesizedMarkerAllowance, uint64(chproto.ClientQueryCode))
		if errors.Is(decErr, chproto.ErrPacketTooLarge) {
			return rejectClose(fmt.Errorf("synthesized INSERT %q received an oversized client packet: %w", q.ID, decErr))
		}
		if pkt == nil || decErr != nil {
			r.hooks.OnQueryAbort(ctx, qctx)
			r.hooks.OnQueryComplete(ctx, r.sess)
			if pkt == nil && decErr == nil {
				return io.EOF
			}
			return fmt.Errorf("read synthesized INSERT marker: %w", decErr)
		}
		if r.obs != nil {
			r.obs.ClientPacket(clientPacketName(pkt.Type))
			r.obs.BytesTransferred("client_to_upstream", float64(pkt.RawLen))
		}
		switch pkt.Type {
		case uint64(chproto.ClientDataCode):
			info, err := chproto.InspectClientDataPacket(pkt.Raw, compression)
			if err != nil {
				r.hooks.OnQueryAbort(ctx, qctx)
				r.hooks.OnQueryComplete(ctx, r.sess)
				return fmt.Errorf("classify synthesized client data packet: %w", err)
			}
			if info.BlockName != "" {
				return rejectClose(fmt.Errorf(
					"synthesized INSERT %q received external table block %q; external tables are not supported on the storage-integrity signed lane",
					q.ID, info.BlockName))
			}
			if !info.Empty {
				// The rows are already inside the signed statement; a payload
				// block would be unsigned bytes riding the same INSERT.
				return rejectClose(fmt.Errorf(
					"synthesized INSERT %q received a client payload block; an inline VALUES statement streams no data", q.ID))
			}
			markerRaw = append([]byte(nil), pkt.Raw...)
		case uint64(chproto.ClientCancelCode):
			// Nothing reached upstream. ClickHouse answers a cancelled query with
			// EndOfStream; do the same locally and drop the plan.
			r.hooks.OnQueryAbort(ctx, qctx)
			r.hooks.OnQueryComplete(ctx, r.sess)
			logger.Debugw("synthesized INSERT cancelled by client before forwarding", "query_id", q.ID)
			if err := client.WriteRawPacket([]byte{byte(chproto.ServerEndOfStreamCode)}); err != nil {
				return fmt.Errorf("write end-of-stream after synthesized cancel: %w", err)
			}
			return nil
		default:
			return rejectClose(fmt.Errorf(
				"client sent packet type %d (%s) while synthesized INSERT %q was awaiting its external-tables marker",
				pkt.Type, clientPacketName(pkt.Type), q.ID))
		}
	}

	// 2. Encode the payload at the upstream codec's negotiated revision.
	up := r.sess.Upstream()
	if up == nil {
		r.hooks.OnQueryAbort(ctx, qctx)
		r.hooks.OnQueryComplete(ctx, r.sess)
		return chsession.ErrNoUpstream
	}
	up.SetCompression(compression)
	packets := make([][]byte, 0, len(plan.Blocks))
	var payloadBytes uint64
	for i, cols := range plan.Blocks {
		raw, err := up.EncodeClientDataPacket(cols)
		if err != nil {
			return rejectClose(fmt.Errorf("synthesized INSERT %q: encode block %d: %w", q.ID, i, err))
		}
		payloadBytes += uint64(len(raw))
		packets = append(packets, raw)
	}
	// The chain's strict-data budget is this statement's max_payload_bytes.
	if limit, enforce := r.hooks.ClientDataReadLimit(qctx); enforce && payloadBytes > limit {
		return rejectResume(fmt.Errorf("synthesized INSERT %q payload of %d bytes exceeds limit of %d bytes",
			q.ID, payloadBytes, limit))
	}
	plan.Packets = packets
	plan.PayloadBytes = payloadBytes
	logger.Debugw("synthesized INSERT payload encoded",
		"query_id", q.ID, "packets", len(packets), "payload_bytes", payloadBytes, "revision", up.Revision())

	// 3-7. Strict input completion, Query, marker, sample gate, payload, exactly
	// one terminator, OnQueryInputComplete and terminal arbitration.
	return r.forwardSignedInsert(ctx, qctx, signedInsertForward{
		lane:          "synthesized INSERT",
		forwardCancel: true,
		compression:   compression,
		markerRaw:     markerRaw,
		terminatorRaw: nil,
		payload:       packets,
		payloadBytes:  payloadBytes,
		sampleColumns: plan.SampleColumns,
		rejectClose:   rejectClose,
		rejectResume:  rejectResume,
	})
}

// beginCancel is serialized with terminal decoding, never with network I/O.
// It returns false when a decoded terminal already owns the outcome, or when
// the client has already forwarded a Cancel for this query.
func (g *deferredInputGate) beginCancel(cancel func() (string, bool)) bool {
	g.cancelMu.Lock()
	defer g.cancelMu.Unlock()
	if g.terminalDecoded || g.cancelDone != nil {
		return false
	}
	cancel()
	g.cancelDone = make(chan struct{})
	return true
}

func (g *deferredInputGate) finishCancel(err error) {
	g.cancelMu.Lock()
	defer g.cancelMu.Unlock()
	g.cancelErr = err
	close(g.cancelDone)
}

// terminalCancel waits only for an already-started Cancel write. The upstream
// reader has consumed the terminal bytes, so a peer writing that terminal can
// now read Cancel and release the writer. A failed write remains fail-closed.
func (g *deferredInputGate) terminalCancel(ctx context.Context) (bool, error) {
	g.cancelMu.Lock()
	g.terminalDecoded = true
	done := g.cancelDone
	g.cancelMu.Unlock()
	if done == nil {
		return false, nil
	}
	select {
	case <-done:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	g.cancelMu.Lock()
	defer g.cancelMu.Unlock()
	return g.cancelErr == nil, g.cancelErr
}
