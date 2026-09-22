package sistatement

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sqlident"
)

// Bound retention across all tenants as well as within one session identity.
const maxIdleEvaluatorConns = 4
const maxTotalIdleEvaluatorConns = 16
const evaluatorControlLimit = 64 << 10

type evaluatorPoolKey struct {
	address, account, user, database string
	password                         [32]byte
	revision                         uint64
}

// UpstreamValuesEvaluator opens a dedicated connection to the current session's
// endpoint. Dial must honor ctx and dial address exactly, without selecting a
// different peer. A failed attempt is closed, never retried or returned to idle.
type UpstreamValuesEvaluator struct {
	dial      func(context.Context, string) (net.Conn, error)
	signer    auth.Signer
	mu        sync.Mutex
	idle      map[evaluatorPoolKey][]*evaluatorConn
	active    map[*evaluatorConn]struct{}
	idleCount int
	closed    bool
}

type evaluatorConn struct {
	conn  net.Conn
	codec *chproto.Codec
}

func NewUpstreamValuesEvaluator(dial func(context.Context, string) (net.Conn, error), signer auth.Signer) *UpstreamValuesEvaluator {
	return &UpstreamValuesEvaluator{dial: dial, signer: signer, idle: make(map[evaluatorPoolKey][]*evaluatorConn), active: make(map[*evaluatorConn]struct{})}
}

func (e *UpstreamValuesEvaluator) Evaluate(ctx context.Context, req ValuesEvaluation) ([][]proto.InputColumn, error) {
	if e == nil || e.dial == nil || e.signer == nil {
		return nil, fmt.Errorf("values evaluator is not configured")
	}
	if req.Hello == nil || req.UpstreamAddress == "" {
		return nil, fmt.Errorf("evaluation requires the session upstream address and hello")
	}
	if req.Timeout <= 0 || req.MaxRows == 0 || req.MaxBytes == 0 {
		return nil, fmt.Errorf("evaluation requires positive timeout, row and byte limits")
	}
	ctx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sql, err := buildValuesQuery(req)
	if err != nil {
		return nil, err
	}
	token, err := e.signer.SignToken(sql)
	if err != nil {
		return nil, fmt.Errorf("sign evaluation query: %w", err)
	}
	ec, fresh, err := e.acquire(ctx, req)
	if err != nil {
		return nil, err
	}
	reusable := false
	defer func() { e.release(poolKey(req), ec, reusable) }()
	deadline, _ := ctx.Deadline()
	if err := ec.conn.SetDeadline(deadline); err != nil {
		return nil, fmt.Errorf("set evaluation deadline: %w", err)
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { ec.close(); close(done) })
	var stopOnce sync.Once
	stopCancellation := func() {
		stopOnce.Do(func() {
			if !stop() {
				<-done
			}
		})
	}
	defer stopCancellation()
	if fresh {
		if err := ec.handshake(req.Hello); err != nil {
			return nil, err
		}
	}
	blocks, err := runEvaluation(ec, req, sql, token)
	// Fence the cancellation callback before clearing deadlines or returning to
	// the pool, otherwise cancellation can close the next borrower's connection.
	stopCancellation()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ec.conn.SetDeadline(time.Time{}); err != nil {
		return nil, err
	}
	reusable = true
	return blocks, nil
}

// Close fences new acquisitions and late releases, including in-flight dials.
func (e *UpstreamValuesEvaluator) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = true
	for key, pool := range e.idle {
		for _, ec := range pool {
			ec.close()
		}
		delete(e.idle, key)
	}
	e.idleCount = 0
	for ec := range e.active {
		ec.close()
	}
	return nil
}

// buildValuesQuery renders the helper SELECT (spec D4). The structure string
// is one single-quoted literal, so every single quote a declared type carries -- DateTime('UTC') -- is doubled inside it.
func buildValuesQuery(req ValuesEvaluation) (string, error) {
	if len(req.Columns) == 0 {
		return "", fmt.Errorf("evaluation requires at least one column")
	}
	declared := declaredTypes(req)
	selects := make([]string, len(req.Columns))
	structure := make([]string, len(req.Columns))
	for i, name := range req.Columns {
		typ, ok := declared[name]
		if !ok {
			return "", fmt.Errorf("column %q is not declared in the schema for %s", name, req.Schema.TableID)
		}
		if _, err := payloadexec.ResolveColumnProfile(typ); err != nil {
			return "", fmt.Errorf("column %q: %w", name, err)
		}
		quoted := quoteValuesColumn(name)
		selects[i] = quoted
		structure[i] = quoted + " " + typ
	}
	return "SELECT " + strings.Join(selects, ", ") +
		" FROM VALUES(" + singleQuoted(strings.Join(structure, ", ")) + ", " + req.Rows + ")", nil
}

func singleQuoted(v string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "''").Replace(v) + "'"
}

// A column is one identifier even when its name contains a dot. Escape the
// identifier layer before escaping the enclosing structure string layer.
func quoteValuesColumn(name string) string {
	return sqlident.Quote(strings.ReplaceAll(name, "\\", "\\\\"))
}

func poolKey(req ValuesEvaluation) evaluatorPoolKey {
	return evaluatorPoolKey{address: req.UpstreamAddress, account: req.Account, user: req.Hello.User, database: req.Hello.Database, password: sha256.Sum256([]byte(req.Hello.Password)), revision: uint64(req.Hello.ProtocolVersion)}
}

func declaredTypes(req ValuesEvaluation) map[string]string {
	out := make(map[string]string, len(req.Schema.Columns))
	for _, c := range req.Schema.Columns {
		out[c.Name] = c.Type
	}
	return out
}

func runEvaluation(ec *evaluatorConn, req ValuesEvaluation, sql, token string) ([][]proto.InputColumn, error) {
	up := ec.codec
	q := &chproto.Query{
		Info: chproto.ClientInfo{
			Query: proto.ClientQueryInitial, InitialUser: req.Hello.User, InitialAddress: "127.0.0.1:0",
			Interface: proto.InterfaceTCP, OSUser: "housegate", ClientHostname: "housegate",
			ClientName: "housegate-inline-values", Major: 1, Minor: 0, ProtocolVersion: up.Revision(),
		},
		Stage: proto.StageComplete, Compression: proto.CompressionDisabled, Body: sql,
		Settings: []chproto.Setting{
			{Key: "max_execution_time", Value: strconv.FormatFloat(req.Timeout.Seconds(), 'f', -1, 64)},
			{Key: "max_result_rows", Value: strconv.FormatUint(req.MaxRows, 10)},
			{Key: "max_result_bytes", Value: strconv.FormatUint(req.MaxBytes, 10)},
			{Key: "max_block_size", Value: strconv.FormatUint(req.MaxRows, 10)},
			{Key: auth.AuthTokenSettingKey, Value: "'" + token + "'", Custom: true},
		},
	}
	if err := up.WriteQuery(q); err != nil {
		return nil, fmt.Errorf("write evaluation query: %w", err)
	}
	if err := up.WriteEmptyDataBlock(); err != nil {
		return nil, fmt.Errorf("write evaluation terminator: %w", err)
	}
	var blocks [][]proto.InputColumn
	var totalRows, totalBytes uint64
	for {
		// Keep packet framing bounded before decoding. Header-only and control
		// packets have a fixed independent ceiling; row packets are checked below
		// against the remaining aggregate payload budget.
		limit := req.MaxBytes - totalBytes
		if limit < evaluatorControlLimit {
			limit = evaluatorControlLimit
		}
		pkt, err := up.ReadPacketWithLimit(limit, uint64(chproto.ServerExceptionCode))
		if err != nil {
			if errors.Is(err, chproto.ErrPacketTooLarge) {
				return nil, fmt.Errorf("evaluation exceeds max_payload_bytes (%d) or control packet limit: %w", req.MaxBytes, err)
			}
			return nil, fmt.Errorf("read evaluation result: %w", err)
		}
		switch pkt.Type {
		case uint64(chproto.ServerEndOfStreamCode):
			if totalRows == 0 {
				return nil, fmt.Errorf("evaluation produced no rows")
			}
			return blocks, nil
		case uint64(chproto.ServerExceptionCode):
			exc, _ := pkt.Decoded.(*chproto.Exception)
			if exc == nil {
				return nil, fmt.Errorf("evaluation returned a malformed exception")
			}
			return nil, fmt.Errorf("ClickHouse refused the rows: code=%d %s: %s", exc.Code, exc.Name, exc.Message)
		case uint64(chproto.ServerDataCode):
			remaining := req
			remaining.MaxRows -= totalRows
			cols, derr := decodeServerDataColumns(pkt.Raw, up.Revision(), remaining)
			if derr != nil {
				return nil, derr
			}
			if len(cols) != 0 && cols[0].Data.Rows() > 0 {
				rows := uint64(cols[0].Data.Rows())
				if rows > req.MaxRows-totalRows {
					return nil, fmt.Errorf("evaluation exceeds max_rows (%d)", req.MaxRows)
				}
				packet, err := nativepayload.EncodeClientDataPacket(up.Revision(), cols)
				if err != nil {
					return nil, err
				}
				if uint64(len(packet)) > req.MaxBytes-totalBytes {
					return nil, fmt.Errorf("evaluation exceeds max_payload_bytes (%d)", req.MaxBytes)
				}
				totalRows += rows
				totalBytes += uint64(len(packet))
				blocks = append(blocks, cols)
			}
		case uint64(proto.ServerCodeProgress), uint64(proto.ServerCodeProfile), uint64(proto.ServerCodeLog), uint64(proto.ServerProfileEvents):
			// Ordinary query progress does not contribute row payload.
		default:
			return nil, fmt.Errorf("unexpected evaluation packet type %d", pkt.Type)
		}
	}
}

// decodeServerDataColumns decodes one server Data packet the way
// nativepayload decodes a client one: proto.Results.Auto() infers each column
// from its wire type, checked against the sole profile authority. Header-only
// blocks are validated before the caller drops them.
func decodeServerDataColumns(raw []byte, revision int, req ValuesEvaluation) ([]proto.InputColumn, error) {
	normalized, err := chproto.NormalizeServerDataBlockInfo(raw, revision)
	if err != nil {
		return nil, err
	}
	pr := proto.NewReader(bytes.NewReader(normalized))
	code, err := pr.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("evaluation block code: %w", err)
	}
	if code != uint64(proto.ServerCodeData) {
		return nil, fmt.Errorf("evaluation packet type %d is not ServerData", code)
	}
	if _, err := pr.Str(); err != nil {
		return nil, fmt.Errorf("evaluation block name: %w", err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(pr, revision, boundedEvaluationResult{target: results.Auto(), columns: len(req.Columns), maxRows: req.MaxRows}); err != nil {
		return nil, fmt.Errorf("decode evaluation block: %w", err)
	}
	if len(results) != len(req.Columns) {
		return nil, fmt.Errorf("evaluation block has %d columns, the INSERT lists %d", len(results), len(req.Columns))
	}
	declared := declaredTypes(req)
	out := make([]proto.InputColumn, len(results))
	for i, rc := range results {
		want := req.Columns[i]
		if rc.Name != want {
			return nil, fmt.Errorf("evaluated column %d is %q, expected %q", i, rc.Name, want)
		}
		profile, perr := payloadexec.ResolveColumnProfile(declared[want])
		if perr != nil {
			return nil, fmt.Errorf("column %q: %w", want, perr)
		}
		if got := string(rc.Data.Type()); got != profile.NativeWireType {
			return nil, fmt.Errorf("evaluated column %q has wire type %q, the declared type %q requires %q",
				want, got, declared[want], profile.NativeWireType)
		}
		input, ok := rc.Data.(proto.ColInput)
		if !ok {
			return nil, fmt.Errorf("evaluated column %q (%T) cannot be re-encoded", want, rc.Data)
		}
		out[i] = proto.InputColumn{Name: want, Data: input}
	}
	cols := make([]chproto.SampleColumn, len(req.Columns))
	for i, name := range req.Columns {
		cols[i] = chproto.SampleColumn{Name: name, Type: declared[name]}
	}
	if _, err := validateEvaluatedBlock(out, cols); err != nil {
		return nil, err
	}
	return out, nil
}

func (e *UpstreamValuesEvaluator) acquire(ctx context.Context, req ValuesEvaluation) (*evaluatorConn, bool, error) {
	key := poolKey(req)
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil, false, errors.New("values evaluator is closed")
	}
	if pool := e.idle[key]; len(pool) > 0 {
		ec := pool[len(pool)-1]
		if len(pool) == 1 {
			delete(e.idle, key)
		} else {
			e.idle[key] = pool[:len(pool)-1]
		}
		e.idleCount--
		e.active[ec] = struct{}{}
		e.mu.Unlock()
		return ec, false, nil
	}
	e.mu.Unlock()
	conn, err := e.dial(ctx, req.UpstreamAddress)
	if err != nil {
		return nil, false, fmt.Errorf("dial evaluation upstream: %w", err)
	}
	if conn == nil {
		return nil, false, errors.New("evaluation dialer returned a nil connection")
	}
	ec := &evaluatorConn{conn: conn, codec: chproto.NewCodec(conn, chproto.DirToUpstream)}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed || ctx.Err() != nil {
		ec.close()
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		return nil, false, errors.New("values evaluator is closed")
	}
	e.active[ec] = struct{}{}
	return ec, true, nil
}

func (e *UpstreamValuesEvaluator) release(key evaluatorPoolKey, ec *evaluatorConn, reusable bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.active, ec)
	if !reusable || e.closed || len(e.idle[key]) >= maxIdleEvaluatorConns || e.idleCount >= maxTotalIdleEvaluatorConns {
		ec.close()
		return
	}
	e.idle[key] = append(e.idle[key], ec)
	e.idleCount++
}

func (ec *evaluatorConn) close() { _ = ec.conn.Close() }

// handshake mirrors Relay.handshakeFreshUpstream: the stored hello capped by
// ClientHelloForUpstream, the ServerHello revision floor, compression pinned off, and the one-way client addendum.
func (ec *evaluatorConn) handshake(stored *chproto.ClientHello) error {
	hello := chproto.ClientHelloForUpstream(stored)
	if err := ec.codec.WriteClientHello(hello); err != nil {
		return fmt.Errorf("write evaluation hello: %w", err)
	}
	ec.codec.SetServerHelloRevisionHint(int(hello.ProtocolVersion))
	pkt, err := ec.codec.ReadPacketWithLimit(evaluatorControlLimit, uint64(chproto.ServerHelloCode), uint64(chproto.ServerExceptionCode))
	if err != nil {
		return fmt.Errorf("read evaluation server hello: %w", err)
	}
	if exc, ok := pkt.Decoded.(*chproto.Exception); ok {
		return fmt.Errorf("evaluation upstream rejected hello: code=%d %s: %s", exc.Code, exc.Name, exc.Message)
	}
	srv, ok := pkt.Decoded.(*chproto.ServerHello)
	if !ok {
		return fmt.Errorf("unexpected evaluation hello packet type=%d", pkt.Type)
	}
	rev := int(hello.ProtocolVersion)
	if srv.Revision < rev {
		rev = srv.Revision
	}
	ec.codec.SetRevision(rev)
	ec.codec.SetCompression(proto.CompressionDisabled)
	if chproto.SupportsAddendum(rev) {
		res := ec.codec.ResolveUpstreamAddendum(chproto.AddendumResult{},
			chproto.AddendumOpts{ProposedRecv: "notchunked", ProposedSend: "notchunked"})
		if res.NegotiatedRecv == "chunked" || res.NegotiatedSend == "chunked" {
			return fmt.Errorf("evaluation requires bounded non-chunked transport")
		}
		if err := ec.codec.SendAddendum(res); err != nil {
			return fmt.Errorf("send evaluation addendum: %w", err)
		}
	}
	return nil
}

var _ ValuesEvaluator = (*UpstreamValuesEvaluator)(nil)

// Enforce request-specific shape bounds before a second column allocation.
// Stream framing still belongs to Codec.ReadPacketWithLimit.
type boundedEvaluationResult struct {
	target  proto.Result
	columns int
	maxRows uint64
}

func (r boundedEvaluationResult) DecodeResult(pr *proto.Reader, revision int, block proto.Block) error {
	if block.Columns != r.columns {
		return fmt.Errorf("evaluation block has %d columns, the INSERT lists %d", block.Columns, r.columns)
	}
	if block.Rows < 0 || uint64(block.Rows) > r.maxRows {
		return fmt.Errorf("evaluation exceeds remaining max_rows (%d)", r.maxRows)
	}
	return r.target.DecodeResult(pr, revision, block)
}
