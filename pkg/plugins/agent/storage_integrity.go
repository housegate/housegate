package agent

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"

	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/sqlident"
	core "housegate/housegate/pkg/storageintegrity"
)

type StorageIntegrityConfig struct {
	Enabled   bool   `json:"enabled"    yaml:"enabled"`
	NetworkID string `json:"network_id" yaml:"network_id"`
}

// StorageIntegrityPlugin is the client-side/agent half of storage integrity.
// It keeps ordinary ClickHouse clients unaware of `_hg_row_id`: server sample
// blocks are stripped on the way down, and Native Data blocks are augmented on
// the way up before the server-side HouseGate sees them.
type StorageIntegrityPlugin struct {
	NetworkID string

	mu     sync.Mutex
	active map[int64]*storageIntegrityState
}

type storageIntegrityState struct {
	tableID      string
	statementID  string
	revision     int
	nextOrdinal  uint64
	sawDataRows  bool
	sampleHidden bool
}

func NewStorageIntegrityPlugin(cfg StorageIntegrityConfig) *StorageIntegrityPlugin {
	networkID := cfg.NetworkID
	if networkID == "" {
		networkID = "sentio"
	}
	return &StorageIntegrityPlugin{
		NetworkID: networkID,
		active:    map[int64]*storageIntegrityState{},
	}
}

func (p *StorageIntegrityPlugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if p == nil || qctx == nil || qctx.Query == nil || qctx.Session == nil {
		return nil
	}
	tableID, ok := agentInsertTableID(qctx)
	if !ok {
		return nil
	}
	if qctx.Query.Compression != proto.CompressionDisabled {
		return fmt.Errorf("agent storage_integrity supports only uncompressed Native INSERT data")
	}
	if qctx.Query.ID == "" {
		qctx.Query.ID = uuid.NewString()
	}
	state := &storageIntegrityState{
		tableID:     tableID,
		statementID: qctx.Query.ID,
		revision:    qctx.Session.State().ClientRevision,
	}
	p.mu.Lock()
	p.active[qctx.Session.ID()] = state
	p.mu.Unlock()

	_, logger := log.FromContext(ctx)
	logger.Debugw("agent storage_integrity: tracking insert",
		"table_id", tableID,
		"statement_id", state.statementID,
	)
	return nil
}

func (p *StorageIntegrityPlugin) OnClientData(context.Context, *plugin.QueryContext, []byte) error {
	return nil
}

func (p *StorageIntegrityPlugin) RewriteClientData(ctx context.Context, qctx *plugin.QueryContext, raw []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil || qctx == nil || qctx.Session == nil || len(raw) == 0 {
		return raw, nil
	}
	p.mu.Lock()
	state := p.active[qctx.Session.ID()]
	if state == nil {
		p.mu.Unlock()
		return raw, nil
	}
	rewritten, rows, err := core.InjectNativeRowIDs(p.NetworkID, state.tableID, state.statementID, state.revision, raw, state.nextOrdinal)
	if err != nil {
		p.mu.Unlock()
		return nil, err
	}
	state.nextOrdinal += rows
	if rows > 0 {
		state.sawDataRows = true
	}
	p.mu.Unlock()
	return rewritten, nil
}

func (p *StorageIntegrityPlugin) RewriteServerData(ctx context.Context, sess chsession.Session, raw []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p == nil || sess == nil || len(raw) == 0 {
		return raw, nil
	}
	p.mu.Lock()
	state := p.active[sess.ID()]
	p.mu.Unlock()
	if state == nil {
		return raw, nil
	}
	rewritten, stripped, err := core.StripNativeRowIDFromServerData(raw, state.revision)
	if err != nil {
		return nil, err
	}
	if stripped {
		p.mu.Lock()
		if current := p.active[sess.ID()]; current != nil {
			current.sampleHidden = true
		}
		p.mu.Unlock()
	}
	return rewritten, nil
}

func (p *StorageIntegrityPlugin) OnQueryComplete(_ context.Context, sess chsession.Session) {
	p.drop(sess)
}

func (p *StorageIntegrityPlugin) OnClose(sess chsession.Session) {
	p.drop(sess)
}

func (p *StorageIntegrityPlugin) drop(sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	delete(p.active, sess.ID())
	p.mu.Unlock()
}

func (p *StorageIntegrityPlugin) RunOnRouted() bool { return true }

var (
	_ plugin.QueryPlugin             = (*StorageIntegrityPlugin)(nil)
	_ plugin.DataPlugin              = (*StorageIntegrityPlugin)(nil)
	_ plugin.DataRewritePlugin       = (*StorageIntegrityPlugin)(nil)
	_ plugin.ServerDataRewritePlugin = (*StorageIntegrityPlugin)(nil)
	_ plugin.QueryCompletePlugin     = (*StorageIntegrityPlugin)(nil)
	_ plugin.ClosePlugin             = (*StorageIntegrityPlugin)(nil)
	_ plugin.RouteAware              = (*StorageIntegrityPlugin)(nil)
)

const agentIdentifierPathPattern = `(?:` + "`[^`]+`" + `|[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:` + "`[^`]+`" + `|[A-Za-z_][A-Za-z0-9_]*))?`

var agentInsertIntoPattern = regexp.MustCompile(`(?is)^\s*INSERT\s+INTO\s+(` + agentIdentifierPathPattern + `)\b`)

func agentInsertTableID(qctx *plugin.QueryContext) (string, bool) {
	sql := ""
	if qctx.Query != nil {
		sql = qctx.Query.Body
	}
	match := agentInsertIntoPattern.FindStringSubmatch(sql)
	if len(match) != 2 {
		return "", false
	}
	target := normalizeAgentIdentifierPath(match[1])
	if target == "" {
		return "", false
	}
	db, table := splitAgentIdentifierPath(target)
	if db == "" && qctx.Session != nil && qctx.Session.State() != nil {
		db = normalizeAgentIdentifierPath(qctx.Session.State().Snapshot().Database)
	}
	if db == "" {
		return table, true
	}
	return db + "." + table, true
}

func splitAgentIdentifierPath(path string) (db, table string) {
	return sqlident.SplitLastPath(path)
}

func normalizeAgentIdentifierPath(path string) string {
	return sqlident.NormalizePath(path)
}
