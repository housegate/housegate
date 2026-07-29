// Package lthashplugin is the MVP wire-side LtHash commitment pipeline
// from the storage-network data-integrity design
// (docs/superpowers/specs/2026-06-10-multi-replica-trust-design.md).
//
// Scope: for every INSERT flowing through the proxy it decodes the client
// Data blocks, hashes each row into an LtHash accumulator (raw row values
// — the MVP profile, no _hg_row_id), and on query completion folds the
// per-statement accumulator into a process-global per-table Registry.
// Verification (comparing against a re-scan of the stored rows) lives in
// the integration test; the production design moves it to the Keeper
// validation front and replicas.
package lthashplugin

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chsession"
	"github.com/housegate/housegate/pkg/log"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/plugin"
)

// Config enables the MVP commitment pipeline.
type Config struct {
	Enabled bool `json:"enabled" yaml:"enabled"`
}

// Plugin implements QueryPlugin + DataPlugin + QueryCompletePlugin +
// ClosePlugin. One instance serves all sessions; per-session insert state
// lives in states keyed by session ID.
type Plugin struct {
	registry *Registry
	states   sync.Map // int64 session ID -> *insertState
}

// New returns a Plugin folding results into reg (DefaultRegistry if nil).
func New(reg *Registry) *Plugin {
	if reg == nil {
		reg = DefaultRegistry
	}
	return &Plugin{registry: reg}
}

type insertState struct {
	mu       sync.Mutex
	table    string
	revision int
	skip     bool
	skipWhy  string
	acc      *lthash.Hash
	rows     uint64
}

// insertRe matches `INSERT INTO <table>` with optional backtick quoting
// and db qualification. Table functions (INSERT INTO FUNCTION ...) and
// other exotic forms deliberately do not match.
var insertRe = regexp.MustCompile("(?is)^\\s*insert\\s+into\\s+(`[^`]+`(?:\\.`[^`]+`)?|[A-Za-z_][\\w$]*(?:\\.[A-Za-z_][\\w$]*)?)")

func parseInsertTable(sql string) string {
	m := insertRe.FindStringSubmatch(sql)
	if m == nil {
		return ""
	}
	name := strings.ReplaceAll(m[1], "`", "")
	if strings.EqualFold(name, "function") || strings.EqualFold(name, "table") {
		return ""
	}
	return name
}

// OnQuery arms the per-session insert state when the statement is an
// INSERT; any other statement disarms it.
func (p *Plugin) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	sid := qctx.Session.ID()
	p.states.Delete(sid)

	table := parseInsertTable(qctx.OriginalSQL)
	if table == "" {
		return nil
	}
	st := &insertState{
		table:    table,
		revision: qctx.Session.State().ClientRevision,
		acc:      lthash.New(),
	}
	if qctx.Query != nil && qctx.Query.Compression != proto.CompressionDisabled {
		st.skip = true
		st.skipWhy = "compressed data blocks are out of MVP scope"
	}
	p.states.Store(sid, st)
	return nil
}

// OnClientData decodes one Data packet of the armed INSERT and folds its
// rows into the statement accumulator. Errors poison the statement (skip)
// so one log line is emitted at completion instead of one per packet.
func (p *Plugin) OnClientData(_ context.Context, qctx *plugin.QueryContext, raw []byte) error {
	if qctx == nil || qctx.Session == nil {
		return nil
	}
	v, ok := p.states.Load(qctx.Session.ID())
	if !ok {
		return nil
	}
	st := v.(*insertState)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.skip {
		return nil
	}

	blk, err := DecodeClientData(raw, st.revision)
	if err != nil {
		st.skip, st.skipWhy = true, err.Error()
		return fmt.Errorf("lthash decode for table %q: %w", st.table, err)
	}
	for i := 0; i < blk.Rows; i++ {
		values, err := blk.Row(i)
		if err != nil {
			st.skip, st.skipWhy = true, err.Error()
			return fmt.Errorf("lthash row extract for table %q: %w", st.table, err)
		}
		h, err := lthash.RowHash(st.table, blk.Columns, values)
		if err != nil {
			st.skip, st.skipWhy = true, err.Error()
			return fmt.Errorf("lthash row hash for table %q: %w", st.table, err)
		}
		st.acc.AddHash(h)
	}
	st.rows += uint64(blk.Rows)
	return nil
}

// OnQueryComplete finalizes the statement: fold the accumulator into the
// registry and emit the per-statement digest.
func (p *Plugin) OnQueryComplete(_ context.Context, sess chsession.Session) {
	v, ok := p.states.LoadAndDelete(sess.ID())
	if !ok {
		return
	}
	st := v.(*insertState)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.skip {
		log.Warnw("lthash: insert not committed", "table", st.table, "reason", st.skipWhy)
		return
	}
	if st.rows == 0 {
		return
	}
	p.registry.add(st.table, st.acc, st.rows)
	log.Infow("lthash: insert committed",
		"table", st.table,
		"rows", st.rows,
		"statement_digest", st.acc.Hex(),
	)
}

// OnClose drops any dangling per-session state.
func (p *Plugin) OnClose(sess chsession.Session) {
	p.states.Delete(sess.ID())
}
