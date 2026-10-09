package sistatement

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	// Lanes selects client_seq lanes (spec 2026-10-09 D15): LaneModeAuto, and
	// the zero value, use a client lane whenever the network reports client
	// lanes enabled; LaneModeOff keeps legacy ids (the driver sidecar, R8).
	Lanes LaneMode
	// MaxInflightPerLane bounds the SI statements between client_seq
	// reservation and outcome on one client lane (spec §6.5); the legacy lane
	// is never bounded. Zero is unbounded,
	// which only unit tests use (build passes the configured default 16).
	MaxInflightPerLane int
	// LaneDir returns the si directory of a network; its client lanes live in
	// <dir>/lanes. Nil leaves client lanes unusable: a statement that would
	// use one is refused.
	LaneDir func(networkID string) (string, error)
	// OpenLanePool opens the lane pool in an si directory; nil means
	// OpenLanePool with a warning for an untrusted lane file and the
	// free_list_overflow burn metric.
	OpenLanePool func(siDir string) (*LanePool, error)
	// ClientLanesEnabled reports whether client lanes are active on the
	// network when Discovery is nil (a YAML or host-injected status source);
	// with Discovery the hosting indexer's client_lanes_enabled decides. Nil
	// means disabled.
	ClientLanesEnabled func() bool
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
	signer        auth.StatementSignerV2
	account       string // lowercase 0x
	owner         string
	isDriver      bool
	statuses      registry.TableStatuses
	discovery     registry.StorageIntegrityDiscovery
	networkID     string // configured; empty when only discovery supplies it
	keeperShardID uint32
	seq           *SeqCounter
	openSeq       func(networkID string) (*SeqCounter, error)
	lanes         LaneMode
	maxInflight   int
	laneDir       func(networkID string) (string, error)
	openLanePool  func(siDir string) (*LanePool, error)
	// clientLanesEnabled is Options.ClientLanesEnabled (no discovery).
	clientLanesEnabled func() bool
	// inflightWait bounds how long a statement waits for an in-flight slot on
	// its lane before it is refused.
	inflightWait   time.Duration
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

	// seqMu guards the lazily opened counters and the lane selectors apart
	// from mu, so a first open does not stall other sessions' hooks.
	seqMu     sync.Mutex
	seqs      map[string]*SeqCounter   // by network id
	selectors map[string]*laneSelector // by network id
	seqClosed bool

	mu    sync.Mutex
	infos map[string]infoEntry // by database; successes only, for infoSuccessTTL
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
	database       string // the target's database: the key of its cached SI info
	lanesEnabled   bool   // the network reported client lanes enabled (resolveNetworkID)
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
// the reserved statement's own. Client lanes keep that attribution sound:
// several statements may be in flight on one lane, but each is on its own
// session, and the reservation is still found by session.
//
// lane and done live here, never on the pending statement, which is gone by
// the time the seq is reserved. done frees the lane's in-flight slot; finish
// calls it once the outcome is known (success, marked Exception, pre-send
// abort) and again, as a no-op, when the reservation is dropped (ambiguous
// completion, close).
type reservedSeq struct {
	statementID string
	lane        seqLane // the lane that issued seq: every release goes there
	seq         uint64
	networkID   string
	database    string
	done        func() // idempotent; nil only in tests that build one by hand
	resolved    bool   // released as unspent, or sequenced (success)
}

// finish frees the reservation's in-flight slot; it is idempotent.
func (r *reservedSeq) finish() {
	if r != nil && r.done != nil {
		r.done()
	}
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
	lanes, err := ParseLaneMode(string(opts.Lanes))
	if err != nil {
		errs = append(errs, err)
	}
	if opts.MaxInflightPerLane < 0 {
		errs = append(errs, fmt.Errorf("max in-flight statements per lane must not be negative, got %d", opts.MaxInflightPerLane))
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
	p := &Plugin{
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
		lanes:              lanes,
		maxInflight:        opts.MaxInflightPerLane,
		laneDir:            opts.LaneDir,
		openLanePool:       opts.OpenLanePool,
		clientLanesEnabled: opts.ClientLanesEnabled,
		inflightWait:       laneInflightWait,
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
		selectors:          map[string]*laneSelector{},
		infos:              map[string]infoEntry{},
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
	}
	if p.openLanePool == nil {
		p.openLanePool = func(siDir string) (*LanePool, error) {
			return OpenLanePool(siDir, LanePoolOptions{
				OnCorrupt: func(lane string, err error) {
					log.Warnw("sistatement: ignoring an untrusted client lane file", "lane", lane, "err", err)
				},
				OnBurn: p.burnSeq,
			})
		}
	}
	return p, nil
}

// laneInflightWait bounds how long a statement waits for an in-flight slot on
// its lane (max_inflight_per_lane) before it is refused with a retry message.
// Slots free as soon as an outcome is known, so the wait is short unless the
// lane's statements are stuck upstream.
const laneInflightWait = 30 * time.Second

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
		schema       payloadexec.TableSchema
		schemaHash   string
		networkID    string
		lanesEnabled bool
	)
	if targetErr == nil {
		status, active := p.activeStatus(ctx, target)
		if !active {
			return nil
		}
		// Spec 2026-10-09 §6.4: the schema hash binds the network the
		// hosting indexer reports, so it is resolved before the hash check.
		if networkID, lanesEnabled, err = p.resolveNetworkID(ctx, target.Database); err != nil {
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
	p.pending[sessID] = &pendingStatement{queryID: qctx.Query.ID, networkID: networkID, database: target.Database, lanesEnabled: lanesEnabled, tableID: tableID, schemaHash: schemaHash, clientRevision: uint32(revision)}
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

// reserveStatementID durably reserves a client_seq on lane at the strict
// input boundary (spec 2026-10-09 D16 (a)): an SDK-supplied id for this
// agent's own account keeps its seq when it is in the lane's form (R9, refused
// otherwise); a foreign or malformed id is minted over with the smallest free
// seq or the next one and a fresh nonce. A recycled seq therefore only ever
// appears under a NEW statement id; the agent never re-presents an id it sent.
func (p *Plugin) reserveStatementID(lane seqLane, queryID string) (string, uint64, error) {
	id, ok, err := ownLanedStatementID(queryID, p.account, lane.Lane())
	if err != nil {
		return "", 0, fmt.Errorf("storage_integrity agent: %w", err)
	}
	if ok {
		if err := lane.ReserveSupplied(id.Seq); err != nil {
			return "", 0, fmt.Errorf("storage_integrity agent: reserve supplied client_seq: %w", err)
		}
		return id.Flat(), id.Seq, nil
	}
	seq, err := lane.Reserve()
	if err != nil {
		return "", 0, fmt.Errorf("storage_integrity agent: issue client_seq: %w", err)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		// No statement id exists yet, so nothing can have left the agent.
		p.releaseSeq(lane, seq)
		return "", 0, fmt.Errorf("storage_integrity agent: nonce: %w", err)
	}
	return sicore.StatementID{Account: p.account, Lane: lane.Lane(), Seq: seq, Nonce: hex.EncodeToString(nonce[:])}.Flat(), seq, nil
}

// releaseSeq returns a provably unspent seq to the lane that issued it. A
// failure counts the seq burned, except on a client lane that was abandoned
// after GAP_BUDGET_EXCEEDED: no later statement can use that lane, so the
// release is dropped. A free-list overflow is counted by the lane itself
// (legacyLane, LanePoolOptions.OnBurn): the entry it drops is the largest free
// seq, not necessarily this one.
func (p *Plugin) releaseSeq(lane seqLane, seq uint64) {
	if err := lane.Release(seq); err != nil {
		if store, ok := lane.(*LanedStore); ok && store.Retired() && errors.Is(err, ErrSeqClosed) {
			log.Infow("sistatement: client lane was abandoned; dropping the release of its unspent client_seq", "lane", store.Lane(), "client_seq", seq)
			return
		}
		log.Warnw("sistatement: could not release client_seq; it stays burned", "lane", lane.Lane(), "client_seq", seq, "err", err)
		p.burnSeq("unknown_outcome")
		return
	}
	p.observeSeq(func(o SeqObserver) { o.SeqRecycled() })
}

func (p *Plugin) burnSeq(reason string) {
	p.observeSeq(func(o SeqObserver) { o.SeqBurned(reason) })
}

func (p *Plugin) observeSeq(fn func(SeqObserver)) {
	if o, ok := p.observer.(SeqObserver); ok && o != nil {
		fn(o)
	}
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
	lane, done, statementID, seq, err := p.reserve(ctx, st)
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
		// unspent and is released here (the nonce failure in
		// reserveStatementID releases before any id exists). A later local
		// failure before the Query is written (another strict hook, a missing
		// upstream, a lost active-query race) is released by OnQueryAbort on
		// Relay's UpstreamQueryUnsent proof; a failure once the upstream write
		// began burns the seq at OnQueryComplete, because a partial write
		// cannot be proven unspent. The reservation is not tracked yet, so the
		// lane's in-flight slot is freed here too.
		p.releaseSeq(lane, seq)
		done()
		return fmt.Errorf("storage_integrity agent: sign statement %s: %w", statementID, err)
	}
	qctx.Query.ID = statementID
	// Same Custom + single-quote wrapping as the auth token (see agent.Plugin).
	qctx.Query.Settings = append(qctx.Query.Settings, chproto.Setting{Key: auth.StatementTokenSettingKey, Value: "'" + token + "'", Custom: true})
	p.mu.Lock()
	prev, burned := p.trackReservedLocked(qctx.Session.ID(), &reservedSeq{statementID: statementID, lane: lane, seq: seq, networkID: st.networkID, database: st.database, done: done})
	p.mu.Unlock()
	prev.finish()
	if burned {
		p.burnSeq("unknown_outcome")
	}
	_, logger := log.FromContext(ctx)
	logger.Infow("sistatement: statement token signed", "statement_id", statementID, "lane", lane.Lane(), "query_id", st.queryID, "table_id", st.tableID, "payload_bytes", len(payload))
	return nil
}

// reserve picks the statement's lane (waiting at most inflightWait for an
// in-flight slot) and reserves its client_seq there. On success the caller
// owns done; on every error the slot is already freed and no seq is held. A
// laned pick that loses a race with a concurrent GAP_BUDGET rotation (its
// lane was abandoned between pick and Reserve) is retried once on the new
// lane.
func (p *Plugin) reserve(ctx context.Context, st *pendingStatement) (seqLane, func(), string, uint64, error) {
	sel, err := p.selectorFor(st.networkID)
	if err != nil {
		return nil, nil, "", 0, err
	}
	legacy := legacyLane{open: func() (*SeqCounter, error) { return p.seqFor(st.networkID) }, onBurn: p.burnSeq}
	for attempt := 0; ; attempt++ {
		waitCtx, cancel := context.WithTimeout(ctx, p.inflightWait)
		lane, done, err := sel.pick(waitCtx, st.lanesEnabled, legacy)
		cancel()
		if err != nil {
			return nil, nil, "", 0, err
		}
		statementID, seq, err := p.reserveStatementID(lane, st.queryID)
		if err == nil {
			return lane, done, statementID, seq, nil
		}
		done()
		if store, ok := lane.(*LanedStore); ok && attempt == 0 && store.Retired() && errors.Is(err, ErrSeqClosed) {
			continue
		}
		if lane.Lane() == "" && errors.Is(err, errSDKLegacyWhileLanesOff) && sel.legacyPinned() {
			return nil, nil, "", 0, fmt.Errorf("storage_integrity agent: %w", errSDKLegacyWhilePinned)
		}
		if lane.Lane() == "" && sel.legacyPinned() && errors.Is(err, ErrSeqLocked) {
			_, logger := log.FromContext(ctx)
			logger.Warnw("sistatement: the legacy client_seq lane is held by another process", "err", err)
			return nil, nil, "", 0, errLegacyPinnedButHeld
		}
		return nil, nil, "", 0, err
	}
}

var errLegacyPinnedButHeld = errors.New("storage_integrity agent: client lane budget exhausted and the legacy client_seq lane is held by another process")

// trackReservedLocked records r as the session's outstanding seq. Relay
// completes every query before the next one starts, so a predecessor should
// already be gone; one that is still unresolved is reported as burned. The
// caller finishes the returned predecessor after unlocking.
func (p *Plugin) trackReservedLocked(sessID int64, r *reservedSeq) (prev *reservedSeq, burnedPrevious bool) {
	prev = p.reserved[sessID]
	if prev != nil && !prev.resolved {
		burnedPrevious = true
	}
	p.reserved[sessID] = r
	return prev, burnedPrevious
}

// dropReservedLocked forgets the session's outstanding seq and reports whether
// it was still unresolved, i.e. burned with an unknown outcome. The caller
// finishes the returned reservation after unlocking: this is the one place
// every reservation ends, so its in-flight slot is always freed here at the
// latest.
func (p *Plugin) dropReservedLocked(sessID int64) (r *reservedSeq, burned bool) {
	r = p.reserved[sessID]
	if r == nil {
		return nil, false
	}
	delete(p.reserved, sessID)
	return r, !r.resolved
}

// OnException recycles the outstanding seq when the server proved it unspent
// (spec 2026-10-09 D16 (b)): the marker is matched as a suffix of the trimmed
// message, never by prefix or equality, because the server composes it after
// arbitrary refusal text. Any other Exception leaves the seq to
// OnQueryComplete, which counts it burned. The Exception is attributed to the
// session's outstanding reservation without a query id (see reservedSeq):
// sound only while Relay keeps one query in flight per connection.
//
// After the release, a refusal that concerns the reservation's client lane is
// acted on (spec §6.5): GAP_BUDGET_EXCEEDED abandons the lane, which the next
// statement replaces; LANE_BUDGET_EXCEEDED pins this process to the legacy
// lane; and the ingress's pre-activation refusal expires the cached SI info
// (with client_lanes_enabled cleared) so the next statement re-reads it. The first two tell the
// client to retry. A legacy-lane GAP_BUDGET_EXCEEDED changes nothing.
func (p *Plugin) OnException(ctx context.Context, sess chsession.Session, exc *chproto.Exception) error {
	if p == nil || sess == nil || exc == nil {
		return nil
	}
	marked := chproto.HasSeqUnspentSuffix(exc.Message)
	p.mu.Lock()
	r := p.reserved[sess.ID()]
	if r == nil {
		p.mu.Unlock()
		return nil
	}
	rotation := laneRotationFor(exc.Message, r.statementID)
	if !marked && rotation == rotationNone {
		p.mu.Unlock()
		return nil
	}
	release := marked && !r.resolved
	if release {
		r.resolved = true
	}
	p.mu.Unlock()
	if release {
		p.releaseSeq(r.lane, r.seq)
		r.finish()
	}
	p.applyLaneRotation(ctx, r, rotation, exc)
	return nil
}

func (p *Plugin) applyLaneRotation(ctx context.Context, r *reservedSeq, rotation laneRotation, exc *chproto.Exception) {
	lane := r.lane.Lane()
	_, logger := log.FromContext(ctx)
	switch rotation {
	case rotationGapBudget:
		sel := p.existingSelector(r.networkID)
		if lane == "" || sel == nil {
			return
		}
		logger.Warnw("sistatement: GAP_BUDGET_EXCEEDED on a client lane; abandoning it for a new one", "lane", lane, "statement_id", r.statementID)
		if err := sel.rotate(lane); err != nil {
			logger.Warnw("sistatement: could not persist the abandoned client lane; it is closed and not reused by this process", "lane", lane, "err", err)
		}
		exc.Message = withRetryHint(exc.Message, "retry: the agent moved to a new client_seq lane")
	case rotationLaneBudget:
		sel := p.existingSelector(r.networkID)
		if lane == "" || sel == nil {
			return
		}
		sel.pinLegacy(sicore.AdmissionCodeLaneBudgetExceeded)
		logger.Errorw("sistatement: client lane budget exhausted for "+p.account+"; this process stays on the legacy client_seq lane", "lane", lane, "statement_id", r.statementID, "network_id", r.networkID)
		exc.Message = withRetryHint(exc.Message, "retry: the agent switched to its legacy client_seq lane")
	case rotationLanesDisabled:
		p.expireLanesInfo(r.database)
	}
}

// OnQueryAbort drops the buffer for the exact query. When Relay proves the
// statement's Query never reached the upstream writer (UpstreamQueryUnsent:
// a later strict hook refused, the upstream was gone, the active-query race
// was lost), the seq reserved for that statement is provably unspent and is
// released (spec 2026-10-09 §6.5). Without that proof an abort leaves the
// reservation to OnQueryComplete, which burns it: Relay also aborts after the
// Query was sent, for example at the sample step, where only a marked
// Exception may still recycle the seq.
func (p *Plugin) OnQueryAbort(ctx context.Context, qctx *plugin.QueryContext) {
	if p == nil || qctx == nil || qctx.Session == nil || qctx.Query == nil {
		return
	}
	sessID := qctx.Session.ID()
	var release *reservedSeq
	p.mu.Lock()
	if st := p.pending[sessID]; st != nil && st.queryID == qctx.Query.ID {
		delete(p.pending, sessID)
	}
	if use, ok := p.useNext[sessID]; ok && use.queryID == qctx.Query.ID {
		delete(p.useNext, sessID)
	}
	if r := p.reserved[sessID]; qctx.UpstreamQueryUnsent && r != nil && !r.resolved && r.statementID == qctx.Query.ID {
		r.resolved = true
		release = r
	}
	p.mu.Unlock()
	if release != nil {
		_, logger := log.FromContext(ctx)
		logger.Infow("sistatement: statement never reached upstream; releasing client_seq", "statement_id", release.statementID, "lane", release.lane.Lane(), "client_seq", release.seq)
		p.releaseSeq(release.lane, release.seq)
		release.finish()
	}
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
	var sequenced *reservedSeq
	p.mu.Lock()
	defer func() { p.mu.Unlock(); sequenced.finish() }()
	if id, ok := p.statefulNext[sess.ID()]; ok && id == queryID {
		p.nonSwitchable[sess.ID()] = true
		delete(p.statefulNext, sess.ID())
	}
	if r := p.reserved[sess.ID()]; r != nil && r.statementID == queryID && !r.resolved {
		// Sequenced: the seq is spent, which is neither recycled nor burned.
		r.resolved = true
		sequenced = r
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
	r, burned := p.dropReservedLocked(sess.ID())
	p.mu.Unlock()
	r.finish()
	if burned {
		p.burnSeq("unknown_outcome")
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
	r, burned := p.dropReservedLocked(sess.ID())
	p.mu.Unlock()
	r.finish()
	if burned {
		p.burnSeq("unknown_outcome")
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
