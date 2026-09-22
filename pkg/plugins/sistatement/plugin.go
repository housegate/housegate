package sistatement

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/plugin"
	"github.com/housegate/housegate/pkg/registry"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/schemaregistry"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// Options wires the plugin. KeeperShardID must be zero in v1; InlineValues
// defaults off and Observer is optional.
type Options struct {
	Signer          auth.StatementSignerV2
	Schemas         registry.TableSchemas
	NetworkID       string
	KeeperShardID   uint32
	Seq             *SeqCounter
	MaxPayloadBytes uint64
	// InlineValues configures the signed inline VALUES lane (spec D10).
	InlineValues InlineValuesOptions
	// Evaluator is required when InlineValues.Enabled.
	Evaluator ValuesEvaluator
	// Observer is the narrow metrics surface; nil disables it.
	Observer Observer
}

// Plugin is the agent-mode storage-integrity statement plugin. See doc.go.
type Plugin struct {
	signer        auth.StatementSignerV2
	account       string // lowercase 0x
	loader        *schemaregistry.NetworkStateLoader
	networkID     string
	keeperShardID uint32
	seq           *SeqCounter
	maxPayload    uint64
	inline        InlineValuesOptions
	evaluator     ValuesEvaluator
	observer      Observer

	mu      sync.Mutex
	pending map[int64]*pendingStatement // by session id; at most one per session
	useDB   map[int64]string            // last successful standalone USE per session
	useNext map[int64]pendingUse        // candidate USE awaiting upstream success
}

type pendingStatement struct {
	statementID    string
	tableID        string
	schemaHash     string
	clientRevision uint32
	payload        bytes.Buffer
}

type pendingUse struct {
	queryID string
	db      string
}

// New validates opts and returns the plugin.
func New(opts Options) (*Plugin, error) {
	var errs []error
	if opts.Signer == nil {
		errs = append(errs, errors.New("signer is required"))
	}
	if opts.Schemas == nil {
		errs = append(errs, errors.New("network-state TableSchemas source is required"))
	}
	if strings.TrimSpace(opts.NetworkID) == "" {
		errs = append(errs, errors.New("network id is required"))
	}
	if opts.KeeperShardID != 0 {
		errs = append(errs, fmt.Errorf("keeper_shard_id must be 0 in v1, got %d", opts.KeeperShardID))
	}
	if opts.Seq == nil {
		errs = append(errs, errors.New("seq counter is required"))
	}
	if opts.MaxPayloadBytes == 0 {
		errs = append(errs, errors.New("max payload bytes must be > 0"))
	}
	if opts.InlineValues.Enabled {
		if opts.Evaluator == nil {
			errs = append(errs, errors.New("values evaluator is required when inline_values is enabled"))
		}
		if opts.InlineValues.EvaluationTimeout < time.Second {
			errs = append(errs, fmt.Errorf("inline values evaluation timeout must be >= 1s, got %s", opts.InlineValues.EvaluationTimeout))
		}
		if opts.InlineValues.MaxRows == 0 {
			errs = append(errs, errors.New("inline values max rows must be > 0"))
		}
	}
	if joined := errors.Join(errs...); joined != nil {
		return nil, fmt.Errorf("sistatement: %w", joined)
	}
	return &Plugin{
		signer:        opts.Signer,
		account:       strings.ToLower(opts.Signer.Address()),
		loader:        schemaregistry.NewNetworkStateLoader(opts.Schemas, opts.NetworkID),
		networkID:     opts.NetworkID,
		keeperShardID: opts.KeeperShardID,
		seq:           opts.Seq,
		maxPayload:    opts.MaxPayloadBytes,
		inline:        opts.InlineValues,
		evaluator:     opts.Evaluator,
		observer:      opts.Observer,
		pending:       map[int64]*pendingStatement{},
		useDB:         map[int64]string{},
		useNext:       map[int64]pendingUse{},
	}, nil
}

// OnQuery classifies the statement; payload-local Native INSERTs enter the SI
// lane (deferred plan + statement id), everything else passes through.
func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) (resultErr error) {
	if p == nil || qctx == nil || qctx.Query == nil || qctx.Session == nil {
		return nil
	}
	sql := qctx.Query.Body
	sessID := qctx.Session.ID()
	db, isUse, err := matchUse(sql)
	if err != nil {
		return fmt.Errorf("storage_integrity agent: inspect USE: %w", err)
	}
	if isUse {
		p.mu.Lock()
		p.useNext[sessID] = pendingUse{queryID: qctx.Query.ID, db: db}
		p.mu.Unlock()
		return nil
	}
	// Spec D1/D6: InsertPayloadEncoding refuses the 26.x inline VALUES shape
	// because no payload arrives on the wire. With the lane enabled the statement is claimed here and its rows are evaluated below; every other shape keeps falling through exactly as before.
	var inline *sicore.InlineValuesInsert
	if _, err := sicore.InsertPayloadEncoding(sql); err != nil {
		parsed, claimed, perr := p.inlineValuesCandidate(sql)
		if perr != nil {
			return perr
		}
		if !claimed {
			if errors.Is(err, sicore.ErrInsertIntoFunction) || errors.Is(err, sicore.ErrBackslashEscapedIdentifier) {
				return fmt.Errorf("storage_integrity agent: %w", err)
			}
			// VALUES / SELECT / non-INSERT: ordinary path.
			return nil
		}
		inline = &parsed
	}
	if inline != nil {
		defer func() { resultErr = inlineWrap(resultErr) }()
	}
	if qctx.Query.Compression == proto.CompressionEnabled {
		return errors.New("storage_integrity agent rejects compressed INSERT payloads; retry with ClickHouse query compression disabled")
	}
	keys := make([]string, 0, len(qctx.Query.Settings))
	for _, s := range qctx.Query.Settings {
		keys = append(keys, s.Key)
	}
	inlineKeys, err := sicore.InlineInsertSettingKeys(sql)
	if err != nil {
		return fmt.Errorf("storage_integrity agent: inspect inline SETTINGS: %w", err)
	}
	keys = append(keys, inlineKeys...)
	if err := sicore.RejectUserSettings(keys); err != nil {
		return err
	}
	target, err := sicore.ResolveInsertTarget(sql, p.sessionDatabase(qctx.Session))
	if err != nil {
		return fmt.Errorf("storage_integrity agent: %w", err)
	}
	tableID := target.CanonicalID()
	schema, schemaHash, err := p.loadSchema(ctx, target)
	if err != nil {
		return err
	}
	listed, _, err := insertColumnList(sql)
	if err != nil {
		return fmt.Errorf("storage_integrity agent: %w", err)
	}
	cols, err := sampleColumnsFor(schema, listed)
	if err != nil {
		return fmt.Errorf("storage_integrity agent: %w", err)
	}
	revision := qctx.Session.State().ClientRevision
	if revision <= 0 {
		return errors.New("storage_integrity agent: client protocol revision is unknown; cannot sign client_revision")
	}
	if !proto.FeatureSettingsSerializedAsStrings.In(revision) {
		return fmt.Errorf("storage_integrity agent: client protocol revision %d cannot carry required string settings; revision >= %d is required", revision, proto.FeatureSettingsSerializedAsStrings.Version())
	}
	if inline != nil {
		if len(qctx.Query.Parameters) != 0 {
			return inlineErrorf("query parameters are unsupported")
		}
		if qctx.Session.Upstream() == nil {
			return inlineErrorf("session has no current upstream")
		}
		revision = qctx.Session.Upstream().Revision()
		if !proto.FeatureSettingsSerializedAsStrings.In(revision) {
			return inlineErrorf("upstream revision %d cannot carry required string settings", revision)
		}
	}
	var synthesized *plugin.SynthesizedInsertPlan
	if inline != nil {
		synthesized, err = p.evaluateInlineValues(ctx, qctx, *inline, schema, cols)
		if err != nil {
			return err
		}
	}
	// Preserve the existing deferred-lane sequence timing. The inline lane
	// must reject an overlapping pending statement before reserving its ID.
	var statementID string
	if inline == nil {
		statementID, err = p.statementIDFor(qctx.Query.ID)
		if err != nil {
			return err
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.pending[sessID]; existing != nil {
		return fmt.Errorf("storage_integrity agent: previous SI INSERT %s on this session has not completed", existing.statementID)
	}
	if inline != nil {
		statementID, err = p.statementIDFor(qctx.Query.ID)
		if err != nil {
			return err
		}
	}
	p.pending[sessID] = &pendingStatement{statementID: statementID, tableID: tableID, schemaHash: schemaHash, clientRevision: uint32(revision)}
	qctx.Query.ID = statementID
	_, logger := log.FromContext(ctx)
	if synthesized != nil {
		qctx.Query.Body = inlineInsertBody(target, cols)
		qctx.SynthesizedInsert = synthesized
		// D11: the original statement text is debug-only, never info or above.
		logger.Debugw("sistatement: inline VALUES synthesized", "statement_id", statementID, "original_sql", sql)
	} else {
		qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: cols, MaxPayloadBytes: p.maxPayload}
	}
	if synthesized != nil {
		p.observeInline(func(o Observer) { o.InlineValuesSynthesized() })
	}
	logger.Debugw("sistatement: SI INSERT admitted for signing", "statement_id", statementID, "table_id", tableID, "columns", len(cols))
	return nil
}

func (p *Plugin) sessionDatabase(sess chsession.Session) string {
	p.mu.Lock()
	db := p.useDB[sess.ID()]
	p.mu.Unlock()
	if db != "" {
		return db
	}
	if state := sess.State(); state != nil {
		if logical := state.LogicalDatabaseName(); logical != "" {
			return logical
		}
		return state.PhysicalDatabaseName()
	}
	return ""
}

func (p *Plugin) loadSchema(ctx context.Context, target sicore.InsertTarget) (payloadexec.TableSchema, string, error) {
	tableID := target.CanonicalID()
	if tableID == "" || target.Database == "" || target.Table == "" {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: invalid structured table target %#v", target)
	}
	schemas, err := p.loader.Load(ctx, []schemaregistry.TableRef{
		{
			TableID:         tableID,
			Database:        target.Database,
			Table:           target.Table,
			LogicalDatabase: target.Database,
			LogicalTable:    target.Table,
		},
	})
	if err != nil {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: table %s is not declared in network state (SI INSERT requires a declared, hash-verified schema): %w", tableID, err)
	}
	schema := schemas[0]
	return schema, payloadexec.TableSchemaHash(p.networkID, schema), nil
}

// statementIDFor keeps a client-supplied flat id for this agent's own
// account (SDK path, D6), after durably reserving its seq and canonicalizing
// the account; otherwise it mints <account>:<seq>:<nonce>.
func (p *Plugin) statementIDFor(queryID string) (string, error) {
	if canonical, seq, ok := ownSuppliedStatementID(queryID, p.account); ok {
		if err := p.seq.ReserveSupplied(seq); err != nil {
			return "", fmt.Errorf("storage_integrity agent: reserve supplied client_seq: %w", err)
		}
		return canonical, nil
	}
	seq, err := p.seq.Next()
	if err != nil {
		return "", fmt.Errorf("storage_integrity agent: issue client_seq: %w", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", fmt.Errorf("storage_integrity agent: nonce: %w", err)
	}
	return p.account + ":" + strconv.FormatUint(seq, 10) + ":" + hex.EncodeToString(nonce[:]), nil
}

// ownSuppliedStatementID accepts account casing from SDK query ids, but keeps
// the shared parser strict for the sequence and nonce. Only the account segment
// is normalized; the client nonce is preserved byte-for-byte.
func ownSuppliedStatementID(queryID, ownAccount string) (canonical string, seq uint64, ok bool) {
	account, tail, found := strings.Cut(strings.TrimSpace(queryID), ":")
	if !found || !strings.EqualFold(account, ownAccount) {
		return "", 0, false
	}
	account = strings.ToLower(account)
	parsedAccount, parsedSeq, nonce, err := sicore.ParseFlatStatementID(account + ":" + tail)
	if err != nil || parsedAccount != strings.ToLower(ownAccount) {
		return "", 0, false
	}
	return parsedAccount + ":" + strconv.FormatUint(parsedSeq, 10) + ":" + nonce, parsedSeq, true
}

// OnClientDataStrict buffers one raw non-empty Data packet under the budget.
func (p *Plugin) OnClientDataStrict(_ context.Context, qctx *plugin.QueryContext, raw []byte) error {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil || len(raw) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.pending[qctx.Session.ID()]
	if st == nil || st.statementID != qctx.Query.ID {
		return nil
	}
	if next := uint64(st.payload.Len()) + uint64(len(raw)); next > p.maxPayload {
		delete(p.pending, qctx.Session.ID())
		return fmt.Errorf("storage_integrity agent: payload for %s exceeds max_payload_bytes (%d > %d)", st.statementID, next, p.maxPayload)
	}
	_, _ = st.payload.Write(raw)
	return nil
}

// ClientDataReadLimit exposes the remaining budget so Relay rejects an
// oversized packet while reading it.
func (p *Plugin) ClientDataReadLimit(qctx *plugin.QueryContext) (uint64, bool) {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return 0, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	st := p.pending[qctx.Session.ID()]
	if st == nil || st.statementID != qctx.Query.ID {
		return 0, false
	}
	used := uint64(st.payload.Len())
	if used >= p.maxPayload {
		return 0, true
	}
	return p.maxPayload - used, true
}

// OnQueryInputCompleteStrict signs the v2 statement token over the buffered
// payload and appends SQL_x_statement_token; the pending state is released.
func (p *Plugin) OnQueryInputCompleteStrict(ctx context.Context, qctx *plugin.QueryContext) (resultErr error) {
	if qctx != nil && qctx.SynthesizedInsert != nil {
		defer func() { resultErr = inlineWrap(resultErr) }()
	}
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return nil
	}
	p.mu.Lock()
	st := p.pending[qctx.Session.ID()]
	if st == nil || st.statementID != qctx.Query.ID {
		p.mu.Unlock()
		return nil
	}
	delete(p.pending, qctx.Session.ID())
	p.mu.Unlock()
	payload := st.payload.Bytes()
	if plan := qctx.SynthesizedInsert; plan != nil {
		up := qctx.Session.Upstream()
		if up == nil || uint32(up.Revision()) != st.clientRevision {
			return inlineErrorf("upstream revision changed before signing")
		}
		var size uint64
		for _, packet := range plan.Packets {
			if uint64(len(packet)) > p.maxPayload-size {
				return inlineErrorf("encoded payload exceeds max_payload_bytes (%d)", p.maxPayload)
			}
			size += uint64(len(packet))
		}
		if size != plan.PayloadBytes {
			return inlineErrorf("encoded payload byte count is inconsistent")
		}
		payload = plan.Payload()
	}
	if len(payload) == 0 {
		return fmt.Errorf("storage_integrity agent: SI INSERT %s carried no payload", st.statementID)
	}
	token, err := p.signer.SignStatementV2(auth.JWSStatementPayloadV2{
		NetworkID:      p.networkID,
		KeeperShardID:  p.keeperShardID,
		StatementID:    st.statementID,
		SQLHash:        replay.DigestString(qctx.Query.Body),
		SettingsHash:   sicore.EmptySettingsHash,
		SchemaHash:     st.schemaHash,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		PayloadFormat:  sicore.PayloadEncodingClickHouseNativeData,
		ClientRevision: st.clientRevision,
		TargetTableID:  st.tableID,
		RowIDProfileID: payloadexec.RowIDProfileID,
		StatementKind:  sicore.StatementKindCodeInsert,
	})
	if err != nil {
		return fmt.Errorf("storage_integrity agent: sign statement %s: %w", st.statementID, err)
	}
	// Same Custom + single-quote wrapping as the auth token (see agent.Plugin).
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + token + "'", Custom: true})
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: statement token signed", "statement_id", st.statementID, "table_id", st.tableID, "payload_bytes", len(payload))
	return nil
}

// OnQueryAbort drops the buffer for the exact query.
func (p *Plugin) OnQueryAbort(_ context.Context, qctx *plugin.QueryContext) {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return
	}
	p.mu.Lock()
	if st := p.pending[qctx.Session.ID()]; st != nil && st.statementID == qctx.Query.ID {
		delete(p.pending, qctx.Session.ID())
	}
	if use, ok := p.useNext[qctx.Session.ID()]; ok && use.queryID == qctx.Query.ID {
		delete(p.useNext, qctx.Session.ID())
	}
	p.mu.Unlock()
}

// OnQuerySuccess commits a standalone USE only after Relay observes the
// upstream EndOfStream for that exact query. An Exception or local rejection
// reaches completion without this hook and therefore cannot change the signing
// database for later unqualified INSERTs.
func (p *Plugin) OnQuerySuccess(_ context.Context, sess chsession.Session, queryID string) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	use, ok := p.useNext[sess.ID()]
	if !ok || use.queryID != queryID {
		return
	}
	p.useDB[sess.ID()] = use.db
	delete(p.useNext, sess.ID())
}

// OnQueryComplete drops any candidate USE that did not reach the success
// boundary. Relay permits only one query in flight per session, so the session
// id is sufficient at this terminal hook.
func (p *Plugin) OnQueryComplete(_ context.Context, sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	delete(p.useNext, sess.ID())
	p.mu.Unlock()
}

// OnClose drops all per-session state.
func (p *Plugin) OnClose(sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	delete(p.pending, sess.ID())
	delete(p.useDB, sess.ID())
	delete(p.useNext, sess.ID())
	p.mu.Unlock()
}

var (
	_ plugin.QueryPlugin                    = (*Plugin)(nil)
	_ plugin.StrictDataPlugin               = (*Plugin)(nil)
	_ plugin.StrictDataLimitPlugin          = (*Plugin)(nil)
	_ plugin.QueryInputCompleteStrictPlugin = (*Plugin)(nil)
	_ plugin.QueryAbortPlugin               = (*Plugin)(nil)
	_ plugin.QuerySuccessPlugin             = (*Plugin)(nil)
	_ plugin.QueryCompletePlugin            = (*Plugin)(nil)
	_ plugin.ClosePlugin                    = (*Plugin)(nil)
)
