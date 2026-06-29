package storageintegrity

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/chsession"
	"housegate/housegate/pkg/log"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/replay"
	"housegate/housegate/pkg/sqlmeta"
	core "housegate/housegate/pkg/storageintegrity"
)

type Config struct {
	UnsafeDatabase    string
	SafeDatabase      string
	UnsafeTableSuffix string
}

type Plugin struct {
	layout   core.TableLayout
	payloads *core.MockPayloadStore
	sink     core.IngressSink

	mu      sync.Mutex
	active  map[int64]*insertCapture
	now     func() time.Time
	newStmt func(*plugin.QueryContext) string
}

type insertCapture struct {
	tableID     string
	statementID string
	originalSQL string
	unsafeSQL   string
	unsafeTable string
	safeTable   string
	dataPackets [][]byte
	startedAt   time.Time
}

func New(cfg Config, payloads *core.MockPayloadStore, sink core.IngressSink) *Plugin {
	return &Plugin{
		layout: core.NewTableLayout(core.TableLayoutConfig{
			UnsafeDatabase:    cfg.UnsafeDatabase,
			SafeDatabase:      cfg.SafeDatabase,
			UnsafeTableSuffix: cfg.UnsafeTableSuffix,
		}),
		payloads: payloads,
		sink:     sink,
		active:   map[int64]*insertCapture{},
		now:      time.Now,
		newStmt:  defaultStatementID,
	}
}

func (p *Plugin) OnQuery(ctx context.Context, qctx *plugin.QueryContext) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || qctx == nil || qctx.Query == nil {
		return nil
	}
	switch qctx.StatementType {
	case sqlmeta.StatementTypeInsert:
		return p.onInsert(qctx)
	case sqlmeta.StatementTypeSelect:
		return p.onSelect(qctx)
	default:
		return nil
	}
}

func (p *Plugin) OnClientData(ctx context.Context, qctx *plugin.QueryContext, raw []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || qctx == nil || qctx.Session == nil || len(raw) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	cap := p.active[qctx.Session.ID()]
	if cap == nil {
		return nil
	}
	cp := append([]byte(nil), raw...)
	cap.dataPackets = append(cap.dataPackets, cp)
	return nil
}

func (p *Plugin) OnQueryComplete(ctx context.Context, sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	cap := p.active[sess.ID()]
	delete(p.active, sess.ID())
	p.mu.Unlock()
	if cap == nil || p.payloads == nil || p.sink == nil {
		return
	}
	payload := cap.payloadBytes()
	commit, err := p.payloads.PutPayload(ctx, core.PutPayloadRequest{
		TableID:     cap.tableID,
		StatementID: cap.statementID,
		Payload:     payload,
	})
	if err != nil {
		log.Warnw("storage_integrity: put payload failed",
			"statement_id", cap.statementID,
			"table_id", cap.tableID,
			"err", err,
		)
		return
	}
	if err := p.sink.SubmitInsert(ctx, core.InsertRecord{
		TableID:     cap.tableID,
		StatementID: cap.statementID,
		OriginalSQL: cap.originalSQL,
		UnsafeSQL:   cap.unsafeSQL,
		UnsafeTable: cap.unsafeTable,
		SafeTable:   cap.safeTable,
		Payload:     commit,
		ReceivedAt:  p.now().UTC(),
	}); err != nil {
		log.Warnw("storage_integrity: submit insert failed",
			"statement_id", cap.statementID,
			"table_id", cap.tableID,
			"err", err,
		)
	}
}

func (p *Plugin) OnException(ctx context.Context, sess chsession.Session, _ *chproto.Exception) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || sess == nil {
		return nil
	}
	p.mu.Lock()
	delete(p.active, sess.ID())
	p.mu.Unlock()
	return nil
}

func (p *Plugin) OnClose(sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.mu.Lock()
	delete(p.active, sess.ID())
	p.mu.Unlock()
}

func (p *Plugin) onInsert(qctx *plugin.QueryContext) error {
	target, ok := targetFromContext(qctx)
	if !ok {
		return fmt.Errorf("storage_integrity: INSERT target is required")
	}
	unsafeTable := p.layout.UnsafeTable(target.tableID)
	safeTable := p.layout.SafeTable(target.tableID)
	unsafeSQL, err := replaceTargetAfterKeyword(qctx.Query.Body, "insert into", unsafeTable)
	if err != nil {
		return fmt.Errorf("storage_integrity rewrite INSERT to unsafe: %w", err)
	}
	qctx.Query.Body = unsafeSQL
	qctx.RewrittenSQL = unsafeSQL
	if qctx.Session == nil {
		return nil
	}
	statementID := qctx.Query.ID
	if statementID == "" {
		statementID = p.newStmt(qctx)
	}
	p.mu.Lock()
	p.active[qctx.Session.ID()] = &insertCapture{
		tableID:     target.tableID,
		statementID: statementID,
		originalSQL: qctx.OriginalSQL,
		unsafeSQL:   unsafeSQL,
		unsafeTable: unsafeTable,
		safeTable:   safeTable,
		startedAt:   p.now().UTC(),
	}
	p.mu.Unlock()
	return nil
}

func (p *Plugin) onSelect(qctx *plugin.QueryContext) error {
	target, ok := targetFromContext(qctx)
	if !ok {
		return nil
	}
	safeSQL, err := replaceTargetAfterKeyword(qctx.Query.Body, "from", p.layout.SafeTable(target.tableID))
	if err != nil {
		return nil
	}
	qctx.Query.Body = safeSQL
	qctx.RewrittenSQL = safeSQL
	return nil
}

type sqlTarget struct {
	tableID string
}

func targetFromContext(qctx *plugin.QueryContext) (sqlTarget, bool) {
	if qctx == nil {
		return sqlTarget{}, false
	}
	for _, t := range qctx.AccessedTables {
		if strings.TrimSpace(t.OriginalTable) == "" {
			continue
		}
		db := firstNonEmpty(t.LogicalDatabase, t.OriginalDatabase)
		if db == "" && qctx.Session != nil {
			st := qctx.Session.State()
			db = firstNonEmpty(st.LogicalDatabase, st.Database)
		}
		tableID := t.OriginalTable
		if db != "" {
			tableID = db + "." + t.OriginalTable
		}
		return sqlTarget{tableID: tableID}, true
	}
	return sqlTarget{}, false
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func replaceTargetAfterKeyword(sql, keyword, target string) (string, error) {
	start, ok := findKeywordEnd(sql, keyword)
	if !ok {
		return "", fmt.Errorf("keyword %q not found", keyword)
	}
	targetStart := skipSpaces(sql, start)
	targetEnd, ok := scanQualifiedIdentifier(sql, targetStart)
	if !ok {
		return "", fmt.Errorf("target identifier not found after %q", keyword)
	}
	return sql[:targetStart] + target + sql[targetEnd:], nil
}

func findKeywordEnd(sql, keyword string) (int, bool) {
	lowerSQL := strings.ToLower(sql)
	lowerKeyword := strings.ToLower(keyword)
	searchFrom := 0
	for {
		idx := strings.Index(lowerSQL[searchFrom:], lowerKeyword)
		if idx < 0 {
			return 0, false
		}
		start := searchFrom + idx
		end := start + len(lowerKeyword)
		if isBoundary(sql, start-1) && isBoundary(sql, end) {
			return end, true
		}
		searchFrom = end
	}
}

func isBoundary(sql string, idx int) bool {
	if idx < 0 || idx >= len(sql) {
		return true
	}
	r := rune(sql[idx])
	return !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_')
}

func skipSpaces(sql string, i int) int {
	for i < len(sql) && unicode.IsSpace(rune(sql[i])) {
		i++
	}
	return i
}

func scanQualifiedIdentifier(sql string, i int) (int, bool) {
	end, ok := scanIdentifierToken(sql, i)
	if !ok {
		return 0, false
	}
	for {
		j := skipSpaces(sql, end)
		if j >= len(sql) || sql[j] != '.' {
			return end, true
		}
		j = skipSpaces(sql, j+1)
		next, ok := scanIdentifierToken(sql, j)
		if !ok {
			return end, true
		}
		end = next
	}
}

func scanIdentifierToken(sql string, i int) (int, bool) {
	if i >= len(sql) {
		return 0, false
	}
	switch sql[i] {
	case '`':
		return scanQuoted(sql, i, '`')
	case '"':
		return scanQuoted(sql, i, '"')
	default:
		start := i
		for i < len(sql) {
			r := rune(sql[i])
			if unicode.IsSpace(r) || r == '(' || r == ')' || r == ',' || r == ';' || r == '.' {
				break
			}
			i++
		}
		return i, i > start
	}
}

func scanQuoted(sql string, i int, quote byte) (int, bool) {
	i++
	for i < len(sql) {
		if sql[i] != quote {
			i++
			continue
		}
		if i+1 < len(sql) && sql[i+1] == quote {
			i += 2
			continue
		}
		return i + 1, true
	}
	return 0, false
}

func (c *insertCapture) payloadBytes() []byte {
	var buf bytes.Buffer
	buf.WriteString("# housegate.storage_integrity.insert.v1\n")
	buf.WriteString("statement_id: ")
	buf.WriteString(c.statementID)
	buf.WriteByte('\n')
	buf.WriteString("table_id: ")
	buf.WriteString(c.tableID)
	buf.WriteByte('\n')
	buf.WriteString("original_sql_length: ")
	buf.WriteString(strconv.Itoa(len(c.originalSQL)))
	buf.WriteByte('\n')
	buf.WriteString(c.originalSQL)
	buf.WriteByte('\n')
	buf.WriteString("unsafe_sql_length: ")
	buf.WriteString(strconv.Itoa(len(c.unsafeSQL)))
	buf.WriteByte('\n')
	buf.WriteString(c.unsafeSQL)
	buf.WriteByte('\n')
	buf.WriteString("data_packet_count: ")
	buf.WriteString(strconv.Itoa(len(c.dataPackets)))
	buf.WriteByte('\n')
	for i, pkt := range c.dataPackets {
		buf.WriteString("data_packet_")
		buf.WriteString(strconv.Itoa(i))
		buf.WriteString("_length: ")
		buf.WriteString(strconv.Itoa(len(pkt)))
		buf.WriteByte('\n')
		buf.Write(pkt)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func defaultStatementID(qctx *plugin.QueryContext) string {
	var seed string
	if qctx != nil {
		seed = qctx.OriginalSQL
		if qctx.Session != nil {
			seed = fmt.Sprintf("%d:%s", qctx.Session.ID(), seed)
		}
	}
	return replay.DigestBytes([]byte(fmt.Sprintf("%s:%d", seed, time.Now().UnixNano())))
}

var _ plugin.QueryPlugin = (*Plugin)(nil)
var _ plugin.DataPlugin = (*Plugin)(nil)
var _ plugin.QueryCompletePlugin = (*Plugin)(nil)
var _ plugin.ExceptionPlugin = (*Plugin)(nil)
var _ plugin.ClosePlugin = (*Plugin)(nil)
