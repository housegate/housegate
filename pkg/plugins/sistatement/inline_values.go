package sistatement

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// ValuesEvaluation is one request to turn an inline VALUES row list into
// typed Native blocks (spec D4). Hello carries the current session identity and signing database. Account
// isolates helper pools; UpstreamAddress identifies the already selected peer.
type ValuesEvaluation struct {
	UpstreamAddress string
	Hello           *chproto.ClientHello
	Account         string
	Owner           string
	IsDriver        bool
	Schema          payloadexec.TableSchema
	Columns         []string
	Rows            string
	Timeout         time.Duration
	MaxRows         uint64
	MaxBytes        uint64
}

// ValuesEvaluator evaluates inline VALUES rows through the tenant's own
// ClickHouse. Every failure is terminal; the lane never retries.
type ValuesEvaluator interface {
	Evaluate(ctx context.Context, req ValuesEvaluation) ([][]proto.InputColumn, error)
}

// InlineValuesOptions is the plugin-side view of storage_integrity.agent.inline_values.
type InlineValuesOptions struct {
	Enabled           bool
	EvaluationTimeout time.Duration
	MaxRows           uint64
}

func inlineErrorf(format string, args ...any) error {
	return fmt.Errorf(sicore.InlineValuesErrorPrefix+format, args...)
}

func inlineWrap(err error) error {
	if err == nil || strings.HasPrefix(err.Error(), sicore.InlineValuesErrorPrefix) {
		return err
	}
	return fmt.Errorf("%s%w", sicore.InlineValuesErrorPrefix, err)
}

// inlineValuesCandidate reports whether sql is an inline VALUES INSERT this
// lane claims. ok=false keeps today's fallthrough exactly (feature off, the 25.x truncated shape, FORMAT, SELECT, WITH, non-INSERT); an error is a prefixed refusal of an INSERT ... VALUES the lane will not sign.
func (p *Plugin) inlineValuesCandidate(sql string) (sicore.InlineValuesInsert, bool, error) {
	if !p.inline.Enabled {
		return sicore.InlineValuesInsert{}, false, nil
	}
	parsed, err := sicore.ParseInlineValuesInsert(sql)
	if err != nil {
		if errors.Is(err, sicore.ErrNotInlineValues) {
			return sicore.InlineValuesInsert{}, false, nil
		}
		return sicore.InlineValuesInsert{}, false, inlineWrap(err)
	}
	return parsed, true, nil
}

// requireMaterialized enforces spec D2: fail-closed on the materialize
// plugin's outcome, because the ingress would reject any residual volatile function anyway and an agent-side error is the clearer one.
func requireMaterialized(qctx *plugin.QueryContext) error {
	outcome, ok := qctx.Values[plugin.ValuesKeyMaterialized].(string)
	if !ok {
		return inlineErrorf("materialization did not run")
	}
	if reason, isErr := strings.CutPrefix(outcome, plugin.MaterializeOutcomeErrorPrefix); isErr {
		return inlineErrorf("materialization failed: %s", reason)
	}
	if outcome != plugin.MaterializeOutcomeApplied && outcome != plugin.MaterializeOutcomeNoop {
		return inlineErrorf("unknown materialization outcome %q", outcome)
	}
	return nil
}

// evaluateInlineValues runs the closure gate and the single evaluation, then
// validates the blocks against the INSERT column order and the declared wire types. It runs before statementIDFor, so nothing it refuses consumes a client_seq (spec D9).
func (p *Plugin) evaluateInlineValues(ctx context.Context, qctx *plugin.QueryContext, parsed sicore.InlineValuesInsert,
	schema payloadexec.TableSchema, cols []chproto.SampleColumn) (planOut *plugin.SynthesizedInsertPlan, resultErr error) {
	if err := requireMaterialized(qctx); err != nil {
		return nil, err
	}
	if err := sicore.ValuesClosure(parsed.Rows); err != nil {
		p.observeInline(func(o Observer) { o.InlineValuesClosureRefused() })
		return nil, inlineWrap(err)
	}
	up := qctx.Session.Upstream()
	if up == nil || up.Conn() == nil {
		return nil, inlineErrorf("session has no current upstream")
	}
	address := ""
	if named, ok := up.Conn().(interface{ UpstreamAddress() string }); ok {
		address = named.UpstreamAddress()
	}
	if conn, ok := up.Conn().(net.Conn); address == "" && ok && conn.RemoteAddr() != nil {
		address = conn.RemoteAddr().String()
	}
	if address == "" {
		return nil, inlineErrorf("session upstream address is unavailable")
	}
	hello := qctx.Session.State().UpstreamHello()
	if hello == nil {
		return nil, inlineErrorf("session has no stored upstream hello; cannot open an evaluation connection")
	}
	if db := p.sessionDatabase(qctx.Session); db != "" {
		hello.Database = db
	}
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = c.Name
	}
	evalCtx, cancel := context.WithTimeout(ctx, p.inline.EvaluationTimeout)
	defer cancel()
	defer func() {
		if resultErr != nil {
			p.observeInline(func(o Observer) { o.InlineValuesEvaluationFailed() })
		}
	}()
	blocks, err := p.evaluator.Evaluate(evalCtx, ValuesEvaluation{
		UpstreamAddress: address, Hello: hello, Account: p.account, Schema: schema, Columns: names, Rows: parsed.Rows,
		Owner: p.owner, IsDriver: p.isDriver,
		Timeout: p.inline.EvaluationTimeout, MaxRows: p.inline.MaxRows, MaxBytes: p.maxPayload,
	})
	if err != nil {
		return nil, inlineWrap(err)
	}
	plan := &plugin.SynthesizedInsertPlan{SampleColumns: cols}
	var payloadBytes uint64
	for _, block := range blocks {
		rows, err := validateEvaluatedBlock(block, cols)
		if err != nil {
			return nil, inlineWrap(err)
		}
		if rows == 0 {
			continue
		}
		if uint64(rows) > p.inline.MaxRows-plan.Rows {
			return nil, inlineErrorf("evaluation exceeds max_rows (%d)", p.inline.MaxRows)
		}
		packet, err := nativepayload.EncodeClientDataPacket(qctx.Session.Upstream().Revision(), block)
		if err != nil {
			return nil, inlineWrap(err)
		}
		if uint64(len(packet)) > p.maxPayload-payloadBytes {
			return nil, inlineErrorf("evaluation exceeds max_payload_bytes (%d)", p.maxPayload)
		}
		payloadBytes += uint64(len(packet))
		plan.Blocks = append(plan.Blocks, block)
		plan.Rows += uint64(rows)
	}
	if plan.Rows == 0 {
		return nil, inlineErrorf("evaluation produced no rows")
	}
	return plan, nil
}

// inlineInsertBody renders the signed statement text (spec D6): the resolved
// target and INSERT column order, each column quoted as one identifier
// (including dots and reserved words), followed by FORMAT Native.
func inlineInsertBody(target sicore.InsertTarget, cols []chproto.SampleColumn) string {
	names := make([]string, len(cols))
	for i, c := range cols {
		names[i] = quoteValuesColumn(c.Name)
	}
	return "INSERT INTO " + target.CanonicalID() + " (" + strings.Join(names, ", ") + ") FORMAT Native"
}

func (p *Plugin) observeInline(fn func(Observer)) {
	if p.observer != nil {
		fn(p.observer)
	}
}

// Validate even header-only blocks before discarding them. Injected evaluators
// are subject to the same schema contract as the production decoder.
func validateEvaluatedBlock(block []proto.InputColumn, cols []chproto.SampleColumn) (int, error) {
	if len(cols) == 0 || len(block) != len(cols) {
		return 0, fmt.Errorf("evaluated block has %d columns, the INSERT lists %d", len(block), len(cols))
	}
	rows := -1
	for i, col := range block {
		if col.Name != cols[i].Name {
			return 0, fmt.Errorf("evaluated column %d is %q, expected %q", i, col.Name, cols[i].Name)
		}
		if col.Data == nil || (reflect.ValueOf(col.Data).Kind() == reflect.Pointer && reflect.ValueOf(col.Data).IsNil()) {
			return 0, fmt.Errorf("evaluated column %q has nil data", col.Name)
		}
		profile, err := payloadexec.ResolveColumnProfile(cols[i].Type)
		if err != nil {
			return 0, fmt.Errorf("column %q: %w", col.Name, err)
		}
		if got := string(col.Data.Type()); got != profile.NativeWireType {
			return 0, fmt.Errorf("evaluated column %q has wire type %q, expected %q", col.Name, got, profile.NativeWireType)
		}
		n := col.Data.Rows()
		if n < 0 || (rows >= 0 && n != rows) {
			return 0, fmt.Errorf("evaluated column %q has inconsistent rows", col.Name)
		}
		rows = n
	}
	return rows, nil
}
