package sistatement

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// Options wires the plugin. KeeperShardID must be zero in v1; InlineValues
// defaults off and Observer is optional.
type Options struct {
	Signer auth.StatementSignerV2
	// Statuses answers each INSERT target's storage-integrity status (spec
	// 2026-09-24 §10.2): RpcNetworkState in production. When nil, Schemas is
	// adapted: a declared table is Active, every other table Ordinary.
	Statuses registry.TableStatuses
	Schemas  registry.TableSchemas
	// Discovery asks the indexer hosting each INSERT's database for its
	// network id and runs the writer pre-check (spec 2026-10-09 D18). Nil
	// keeps the static behaviour: NetworkID is required and nothing is
	// pre-checked.
	Discovery registry.StorageIntegrityDiscovery
	// NetworkID is the configured network. With Discovery it is optional: a
	// discovered id that differs refuses, and it is the fallback when the
	// discovery fails (plan decision P7).
	NetworkID     string
	KeeperShardID uint32
	// Seq, when set, serves every network (tests, an explicit state dir).
	// Otherwise OpenSeq opens the counter for a network at its first SI write
	// (plan decision P4). The plugin owns either: Close releases it.
	Seq     *SeqCounter
	OpenSeq func(networkID string) (*SeqCounter, error)
	// WriterPrecheck enables the advisory sentio_isDatabaseWriter pre-check
	// (spec 2026-10-09 §6.4); build leaves it off for drivers.
	WriterPrecheck bool
	// Now is the clock for the clock-skew warning; nil means time.Now.
	Now             func() time.Time
	MaxPayloadBytes uint64
	// Owner and IsDriver preserve the configured agent's ordinary helper-query
	// authorization and billing context; they do not change statement identity.
	Owner    string
	IsDriver bool
	// InlineValues configures the signed inline VALUES lane (spec D10).
	InlineValues InlineValuesOptions
	// Evaluator is required when InlineValues.Enabled.
	Evaluator ValuesEvaluator
	// Observer is the narrow metrics surface; nil disables it.
	Observer Observer
	// Hosting and Dial enable the upstream switch to the indexer hosting an SI
	// INSERT's database (spec 2026-10-09 §6.4, D19). Dial must return a codec
	// whose conn reports the dialed address (build's dialRaw wrapper). A nil
	// Hosting or Dial, or PinnedUpstream (agent.upstream is configured),
	// disables the switch.
	Hosting        registry.DatabaseHosting
	Dial           func(ctx context.Context, address string) (*chproto.Codec, error)
	PinnedUpstream bool
	// SwitchTimeout bounds the hosting lookups, the dial and the replayed
	// handshake of one switch; zero means 10s.
	SwitchTimeout time.Duration
}

// Plugin is the agent-mode storage-integrity statement plugin. See doc.go.
type Plugin struct {
	signer         auth.StatementSignerV2
	account        string // lowercase 0x
	owner          string
	isDriver       bool
	statuses       registry.TableStatuses
	discovery      registry.StorageIntegrityDiscovery
	networkID      string // configured; empty when only discovery supplies it
	keeperShardID  uint32
	seq            *SeqCounter
	openSeq        func(networkID string) (*SeqCounter, error)
	writerPrecheck bool
	now            func() time.Time
	maxPayload     uint64
	inline         InlineValuesOptions
	evaluator      ValuesEvaluator
	observer       Observer
	hosting        registry.DatabaseHosting
	dial           func(ctx context.Context, address string) (*chproto.Codec, error)
	pinnedUpstream bool
	switchTimeout  time.Duration

	// seqMu guards the lazily opened counters apart from mu, so a first open
	// does not stall other sessions' hooks.
	seqMu     sync.Mutex
	seqs      map[string]*SeqCounter // by network id
	seqClosed bool

	mu    sync.Mutex
	infos map[string]registry.StorageIntegrityInfo // by database
	// infoFailures remembers a failed info lookup per database for
	// infoFailureTTL; discoveryWarnEvery throttles the P7 fallback warning.
	infoFailures       map[string]infoFailure
	discoveryWarnEvery time.Duration
	statusWarnEvery    time.Duration               // throttles the status-lookup failure warning per table
	skewWarned         map[string]bool             // by database
	pending            map[int64]*pendingStatement // by session id; at most one per session
	reserved           map[int64]*reservedSeq      // by session id; at most one per session
	useDB              map[int64]string            // last successful standalone USE per session
	useNext            map[int64]pendingUse        // candidate USE awaiting upstream success
	// nonSwitchable marks sessions holding server-side state the agent cannot
	// replay (a successful SET, temporary table or transaction); statefulNext
	// is the query id of such a statement awaiting upstream success.
	nonSwitchable map[int64]bool
	statefulNext  map[int64]string
	hostingCache  map[string]hostingEntry // by database; successes only
}

// pendingStatement is a claimed SI INSERT whose input is still arriving. It
// holds no client_seq: the seq is reserved at the strict input boundary
// (spec 2026-10-09 D16 (a)).
type pendingStatement struct {
	queryID        string // the client's query id until the strict hook replaces it
	networkID      string // resolved in OnQuery; selects the token field and the counter
	tableID        string
	schemaHash     string
	clientRevision uint32
	payload        bytes.Buffer
}

// reservedSeq is the statement whose client_seq was reserved and whose
// outcome is not known yet; at most one per session. OnException attributes an
// upstream Exception to this reservation by session alone: the ExceptionPlugin
// hook carries no query id, and Relay allows one query in flight per
// connection, so the only Exception that can arrive while it is outstanding is
// the reserved statement's own. Plan B lanes (several in-flight statements per
// lane) must revisit this attribution.
type reservedSeq struct {
	statementID string
	counter     *SeqCounter // the network's counter that issued seq
	seq         uint64
	resolved    bool // released as unspent, or sequenced (success)
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
	statuses := opts.Statuses
	if statuses == nil && opts.Schemas != nil {
		statuses = registry.TableStatusesFromSchemas(opts.Schemas)
	}
	if statuses == nil {
		errs = append(errs, errors.New("a table status source (Statuses or Schemas) is required"))
	}
	if opts.Discovery == nil && strings.TrimSpace(opts.NetworkID) == "" {
		errs = append(errs, errors.New("network id is required"))
	}
	if opts.KeeperShardID != 0 {
		errs = append(errs, fmt.Errorf("keeper_shard_id must be 0 in v1, got %d", opts.KeeperShardID))
	}
	if opts.Seq == nil && opts.OpenSeq == nil {
		errs = append(errs, errors.New("seq counter or opener is required"))
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
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	networkID := opts.NetworkID
	if strings.TrimSpace(networkID) == "" {
		networkID = "" // only discovery supplies it
	}
	return &Plugin{
		signer:             opts.Signer,
		account:            strings.ToLower(opts.Signer.Address()),
		owner:              opts.Owner,
		isDriver:           opts.IsDriver,
		statuses:           statuses,
		discovery:          opts.Discovery,
		networkID:          networkID,
		keeperShardID:      opts.KeeperShardID,
		seq:                opts.Seq,
		openSeq:            opts.OpenSeq,
		writerPrecheck:     opts.WriterPrecheck,
		now:                now,
		maxPayload:         opts.MaxPayloadBytes,
		inline:             opts.InlineValues,
		evaluator:          opts.Evaluator,
		observer:           opts.Observer,
		hosting:            opts.Hosting,
		dial:               opts.Dial,
		pinnedUpstream:     opts.PinnedUpstream,
		switchTimeout:      opts.SwitchTimeout,
		seqs:               map[string]*SeqCounter{},
		infos:              map[string]registry.StorageIntegrityInfo{},
		infoFailures:       map[string]infoFailure{},
		discoveryWarnEvery: discoveryWarnInterval,
		statusWarnEvery:    statusWarnInterval,
		skewWarned:         map[string]bool{},
		pending:            map[int64]*pendingStatement{},
		reserved:           map[int64]*reservedSeq{},
		useDB:              map[int64]string{},
		useNext:            map[int64]pendingUse{},
		nonSwitchable:      map[int64]bool{},
		statefulNext:       map[int64]string{},
		hostingCache:       map[string]hostingEntry{},
	}, nil
}

// OnQuery classifies the statement; payload-local Native INSERTs enter the SI
// lane (deferred or synthesized plan), everything else passes through. It
// leaves the client's query id in place and reserves no client_seq: nothing
// reaches the server before OnQueryInputCompleteStrict, which reserves the seq
// and writes the final statement id (spec 2026-10-09 §6.5).
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
	// Spec 2026-10-09 §6.4 step 1: state the agent cannot replay on another
	// server pins the session once the statement succeeds.
	if holdsServerState(sql) {
		p.mu.Lock()
		p.statefulNext[sessID] = qctx.Query.ID
		p.mu.Unlock()
	}
	// Spec 2026-09-24 §10.1: the target's status comes first. Only an Active
	// table is signed; every other INSERT passes through unchanged and the
	// server decides. A target this parser cannot resolve keeps today's
	// classification below.
	target, targetErr := sicore.ResolveInsertTarget(sql, p.sessionDatabase(qctx.Session))
	var (
		schema     payloadexec.TableSchema
		schemaHash string
		networkID  string
	)
	if targetErr == nil {
		status, active := p.activeStatus(ctx, target)
		if !active {
			return nil
		}
		// Spec 2026-10-09 §6.4: the schema hash binds the network the
		// hosting indexer reports, so it is resolved before the hash check.
		if networkID, err = p.resolveNetworkID(ctx, target.Database); err != nil {
			return err
		}
		if schema, schemaHash, err = p.verifySchema(target, status, networkID); err != nil {
			return err
		}
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
		return errors.New("storage_integrity agent rejects compressed INSERT payloads; retry with ClickHouse query compression disabled; clickhouse-client compresses by default only for non-local hosts — connect to 127.0.0.1 or pass --compression 0")
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
		return fmt.Errorf("%w (clickhouse-client also sends settings from ~/.clickhouse-client/config.xml)", err)
	}
	if targetErr != nil {
		return fmt.Errorf("storage_integrity agent: %w", targetErr)
	}
	// Advisory, before the statement is claimed (spec 2026-10-09 §6.4).
	if err := p.precheckWriter(ctx, target.Database); err != nil {
		return err
	}
	tableID := target.CanonicalID()
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
	// Spec 2026-10-09 §6.4 (D19): after every local check that needs no
	// upstream and before the statement is claimed, move the session to the
	// indexer hosting the target. The inline checks below read the (possibly
	// new) upstream.
	if err := p.maybeSwitch(ctx, qctx.Session, target.Database); err != nil {
		return err
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
	p.mu.Lock()
	defer p.mu.Unlock()
	if existing := p.pending[sessID]; existing != nil {
		return fmt.Errorf("storage_integrity agent: previous SI INSERT %s on this session has not completed", existing.queryID)
	}
	p.pending[sessID] = &pendingStatement{queryID: qctx.Query.ID, networkID: networkID, tableID: tableID, schemaHash: schemaHash, clientRevision: uint32(revision)}
	_, logger := log.FromContext(ctx)
	if synthesized != nil {
		qctx.Query.Body = inlineInsertBody(target, cols)
		qctx.SynthesizedInsert = synthesized
		// D11: the original statement text is debug-only, never info or above.
		logger.Debugw("sistatement: inline VALUES synthesized", "query_id", qctx.Query.ID, "original_sql", sql)
	} else {
		qctx.DeferredInsert = &plugin.DeferredInsertPlan{SampleColumns: cols, MaxPayloadBytes: p.maxPayload}
	}
	if synthesized != nil {
		p.observeInline(func(o Observer) { o.InlineValuesSynthesized() })
	}
	logger.Debugw("sistatement: SI INSERT admitted for signing", "query_id", qctx.Query.ID, "table_id", tableID, "columns", len(cols))
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

// activeStatus asks the status source about the INSERT target. It reports
// active=false for every status but Active and when the status lookup itself
// fails: the INSERT then passes through unsigned, which is safe because the
// server rejects an unsigned INSERT into an Active table (spec 2026-09-24 H7).
func (p *Plugin) activeStatus(ctx context.Context, target sicore.InsertTarget) (registry.TableStatus, bool) {
	tableID := target.CanonicalID()
	if tableID == "" || target.Database == "" || target.Table == "" {
		return registry.TableStatus{}, false
	}
	_, logger := log.FromContext(ctx)
	status, err := p.statuses.StorageIntegrityTableStatus(ctx, target.Database, target.Table)
	if err != nil {
		p.observeStatus(func(o StatusObserver) { o.TableStatusLookupFailed() })
		logger.WarnEvery(fmt.Sprintf("sistatement-status-%p-%s", p, tableID), p.statusWarnEvery,
			"sistatement: table status unavailable; passing the INSERT through unsigned", "table_id", tableID, "error", err)
		return registry.TableStatus{}, false
	}
	if status.Status != registry.TableStatusActive {
		logger.Debugw("sistatement: target is not active; passing the INSERT through unsigned", "table_id", tableID, "status", status.Status)
		return registry.TableStatus{}, false
	}
	return status, true
}

// verifySchema decodes an Active table's registry schema and refuses the
// INSERT when the hash recomputed for networkID differs from the declared one.
func (p *Plugin) verifySchema(target sicore.InsertTarget, status registry.TableStatus, networkID string) (payloadexec.TableSchema, string, error) {
	tableID := target.CanonicalID()
	var schema payloadexec.TableSchema
	if err := json.Unmarshal([]byte(status.SchemaJSON), &schema); err != nil {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: active table %s has an undecodable schema_json: %w", tableID, err)
	}
	if schema.TableID != tableID {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: active table %s carries a schema for %q", tableID, schema.TableID)
	}
	hash := payloadexec.TableSchemaHash(networkID, schema)
	if hash != status.SchemaHash {
		return payloadexec.TableSchema{}, "", fmt.Errorf("storage_integrity agent: active table %s schema_hash %s does not match the recomputed %s for network %s; refusing to sign", tableID, status.SchemaHash, hash, networkID)
	}
	return schema, hash, nil
}

func (p *Plugin) observeStatus(fn func(StatusObserver)) {
	if o, ok := p.observer.(StatusObserver); ok && o != nil {
		fn(o)
	}
}

// reserveStatementID durably reserves a client_seq at the strict input
// boundary (spec 2026-10-09 D16 (a)): an SDK-supplied flat id for this
// agent's own account keeps its seq; otherwise the smallest free seq or the
// next one is used with a fresh nonce. A recycled seq therefore only ever
// appears under a NEW statement id; the agent never re-presents an id it sent.
func (p *Plugin) reserveStatementID(counter *SeqCounter, queryID string) (string, uint64, error) {
	if canonical, seq, ok := ownSuppliedStatementID(queryID, p.account); ok {
		if err := counter.ReserveSupplied(seq); err != nil {
			return "", 0, fmt.Errorf("storage_integrity agent: reserve supplied client_seq: %w", err)
		}
		return canonical, seq, nil
	}
	seq, err := counter.Reserve()
	if err != nil {
		return "", 0, fmt.Errorf("storage_integrity agent: issue client_seq: %w", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		// No statement id exists yet, so nothing can have left the agent.
		p.releaseSeq(counter, seq)
		return "", 0, fmt.Errorf("storage_integrity agent: nonce: %w", err)
	}
	return p.account + ":" + strconv.FormatUint(seq, 10) + ":" + hex.EncodeToString(nonce[:]), seq, nil
}

// releaseSeq returns a provably unspent seq to the free list of the counter
// that issued it.
func (p *Plugin) releaseSeq(counter *SeqCounter, seq uint64) {
	overflow, err := counter.Release(seq)
	if err != nil {
		log.Warnw("sistatement: could not release client_seq; it stays burned", "client_seq", seq, "err", err)
		p.observeSeq(func(o SeqObserver) { o.SeqBurned("unknown_outcome") })
		return
	}
	if overflow {
		p.observeSeq(func(o SeqObserver) { o.SeqBurned("free_list_overflow") })
		return
	}
	p.observeSeq(func(o SeqObserver) { o.SeqRecycled() })
}

func (p *Plugin) observeSeq(fn func(SeqObserver)) {
	if o, ok := p.observer.(SeqObserver); ok && o != nil {
		fn(o)
	}
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
	if st == nil || st.queryID != qctx.Query.ID {
		return nil
	}
	if next := uint64(st.payload.Len()) + uint64(len(raw)); next > p.maxPayload {
		delete(p.pending, qctx.Session.ID())
		return fmt.Errorf("storage_integrity agent: payload for %s exceeds max_payload_bytes (%d > %d)", st.queryID, next, p.maxPayload)
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
	if st == nil || st.queryID != qctx.Query.ID {
		return 0, false
	}
	used := uint64(st.payload.Len())
	if used >= p.maxPayload {
		return 0, true
	}
	return p.maxPayload - used, true
}

// OnQueryInputCompleteStrict reserves the client_seq, writes the final
// statement id into qctx.Query.ID (which Relay records as the active query and
// forwards, spec 2026-10-09 §3.1), signs the v2 statement token over the
// buffered payload and appends SQL_x_statement_token; the pending state is
// released. Every payload check runs before the reservation, so a refusal
// there consumes no seq.
func (p *Plugin) OnQueryInputCompleteStrict(ctx context.Context, qctx *plugin.QueryContext) (resultErr error) {
	if qctx != nil && qctx.SynthesizedInsert != nil {
		defer func() { resultErr = inlineWrap(resultErr) }()
	}
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return nil
	}
	p.mu.Lock()
	st := p.pending[qctx.Session.ID()]
	if st == nil || st.queryID != qctx.Query.ID {
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
		return fmt.Errorf("storage_integrity agent: SI INSERT %s carried no payload", st.queryID)
	}
	counter, err := p.seqFor(st.networkID)
	if err != nil {
		return err
	}
	statementID, seq, err := p.reserveStatementID(counter, st.queryID)
	if err != nil {
		return err
	}
	token, err := p.signer.SignStatementV2(auth.JWSStatementPayloadV2{
		NetworkID:      st.networkID,
		KeeperShardID:  p.keeperShardID,
		StatementID:    statementID,
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
		// No token exists, so nothing has left the agent: the seq is provably
		// unspent. This is the only failure after a statement id exists that
		// releases locally (the nonce failure in reserveStatementID releases
		// before any id exists); any later local failure (another strict hook,
		// the upstream write) burns the seq at OnQueryComplete, because a
		// partial write cannot be proven unspent.
		p.releaseSeq(counter, seq)
		return fmt.Errorf("storage_integrity agent: sign statement %s: %w", statementID, err)
	}
	qctx.Query.ID = statementID
	// Same Custom + single-quote wrapping as the auth token (see agent.Plugin).
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + token + "'", Custom: true})
	p.mu.Lock()
	burned := p.trackReservedLocked(qctx.Session.ID(), &reservedSeq{statementID: statementID, counter: counter, seq: seq})
	p.mu.Unlock()
	if burned {
		p.observeSeq(func(o SeqObserver) { o.SeqBurned("unknown_outcome") })
	}
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: statement token signed", "statement_id", statementID, "query_id", st.queryID, "table_id", st.tableID, "payload_bytes", len(payload))
	return nil
}

// trackReservedLocked records r as the session's outstanding seq. Relay
// completes every query before the next one starts, so a predecessor should
// already be gone; one that is still unresolved is reported as burned.
func (p *Plugin) trackReservedLocked(sessID int64, r *reservedSeq) (burnedPrevious bool) {
	if prev := p.reserved[sessID]; prev != nil && !prev.resolved {
		burnedPrevious = true
	}
	p.reserved[sessID] = r
	return burnedPrevious
}

// dropReservedLocked forgets the session's outstanding seq and reports whether
// it was still unresolved, i.e. burned with an unknown outcome.
func (p *Plugin) dropReservedLocked(sessID int64) (burned bool) {
	r := p.reserved[sessID]
	if r == nil {
		return false
	}
	delete(p.reserved, sessID)
	return !r.resolved
}

// OnException recycles the outstanding seq when the server proved it unspent
// (spec 2026-10-09 D16 (b)): the marker is matched as a suffix of the trimmed
// message, never by prefix or equality, because the server composes it after
// arbitrary refusal text. Any other Exception leaves the seq to
// OnQueryComplete, which counts it burned. The Exception is attributed to the
// session's outstanding reservation without a query id (see reservedSeq):
// sound only while Relay keeps one query in flight per connection.
func (p *Plugin) OnException(_ context.Context, sess chsession.Session, exc *chproto.Exception) error {
	if p == nil || sess == nil || exc == nil || !chproto.HasSeqUnspentSuffix(exc.Message) {
		return nil
	}
	p.mu.Lock()
	r := p.reserved[sess.ID()]
	if r == nil || r.resolved {
		p.mu.Unlock()
		return nil
	}
	r.resolved = true
	counter, seq := r.counter, r.seq
	p.mu.Unlock()
	p.releaseSeq(counter, seq)
	return nil
}

// OnQueryAbort drops the buffer for the exact query.
func (p *Plugin) OnQueryAbort(_ context.Context, qctx *plugin.QueryContext) {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return
	}
	p.mu.Lock()
	if st := p.pending[qctx.Session.ID()]; st != nil && st.queryID == qctx.Query.ID {
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
// database for later unqualified INSERTs. A succeeded statement that holds
// server-side state likewise marks the session non-switchable only here.
func (p *Plugin) OnQuerySuccess(_ context.Context, sess chsession.Session, queryID string) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if id, ok := p.statefulNext[sess.ID()]; ok && id == queryID {
		p.nonSwitchable[sess.ID()] = true
		delete(p.statefulNext, sess.ID())
	}
	if r := p.reserved[sess.ID()]; r != nil && r.statementID == queryID {
		// Sequenced: the seq is spent, which is neither recycled nor burned.
		r.resolved = true
	}
	use, ok := p.useNext[sess.ID()]
	if !ok || use.queryID != queryID {
		return
	}
	p.useDB[sess.ID()] = use.db
	delete(p.useNext, sess.ID())
}

// OnQueryComplete drops any candidate USE or stateful statement that did not
// reach the success boundary, and the outstanding seq: one that was neither
// sequenced nor proven unspent is counted burned (unknown outcome, spec
// 2026-10-09 D16 (c)). Relay permits only one query in flight per session, so
// the session id is sufficient at this terminal hook.
func (p *Plugin) OnQueryComplete(_ context.Context, sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	delete(p.useNext, sess.ID())
	delete(p.statefulNext, sess.ID())
	burned := p.dropReservedLocked(sess.ID())
	p.mu.Unlock()
	if burned {
		p.observeSeq(func(o SeqObserver) { o.SeqBurned("unknown_outcome") })
	}
}

// OnClose drops all per-session state; an outstanding unresolved seq is
// counted burned.
func (p *Plugin) OnClose(sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	delete(p.pending, sess.ID())
	delete(p.useDB, sess.ID())
	delete(p.useNext, sess.ID())
	delete(p.nonSwitchable, sess.ID())
	delete(p.statefulNext, sess.ID())
	burned := p.dropReservedLocked(sess.ID())
	p.mu.Unlock()
	if burned {
		p.observeSeq(func(o SeqObserver) { o.SeqBurned("unknown_outcome") })
	}
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
	_ plugin.ExceptionPlugin                = (*Plugin)(nil)
)
