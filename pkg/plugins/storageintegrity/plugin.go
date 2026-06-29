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

	"github.com/ClickHouse/ch-go/proto"

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
	case sqlmeta.StatementTypeUnspecified:
		switch inferStatementTypeFromSQL(firstNonEmpty(qctx.OriginalSQL, qctx.Query.Body)) {
		case sqlmeta.StatementTypeInsert:
			return p.onInsert(qctx)
		case sqlmeta.StatementTypeSelect:
			return p.onSelect(qctx)
		}
		return nil
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
	cap := p.active[qctx.Session.ID()]
	if cap == nil {
		p.mu.Unlock()
		return nil
	}
	cp := append([]byte(nil), raw...)
	cap.dataPackets = append(cap.dataPackets, cp)
	revision := qctx.Session.State().ClientRevision
	complete := isClientDataTerminator(raw, revision)
	log.Debugw("storage_integrity: client data captured",
		"session", qctx.Session.ID(),
		"statement_id", cap.statementID,
		"table_id", cap.tableID,
		"raw_len", len(raw),
		"revision", revision,
		"terminator", complete,
	)
	if complete {
		delete(p.active, qctx.Session.ID())
	}
	p.mu.Unlock()
	if complete {
		p.finalizeCapture(ctx, cap)
	}
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
	p.finalizeCapture(ctx, cap)
}

func (p *Plugin) finalizeCapture(ctx context.Context, cap *insertCapture) {
	if cap == nil || p.payloads == nil || p.sink == nil {
		return
	}
	log.Debugw("storage_integrity: finalizing insert",
		"statement_id", cap.statementID,
		"table_id", cap.tableID,
		"data_packets", len(cap.dataPackets),
	)
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
	log.Infow("storage_integrity: insert submitted",
		"statement_id", cap.statementID,
		"table_id", cap.tableID,
		"unsafe_table", cap.unsafeTable,
		"safe_table", cap.safeTable,
		"payload_ref", commit.Ref,
		"payload_hash", commit.Hash,
	)
}

func isClientDataTerminator(raw []byte, revision int) bool {
	r := proto.NewReader(bytes.NewReader(raw))
	code, err := r.UVarInt()
	if err != nil || code != uint64(chproto.ClientDataCode) {
		return false
	}
	if _, err := r.Str(); err != nil {
		return false
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, revision, results.Auto()); err != nil {
		return false
	}
	return block.Columns == 0 && block.Rows == 0
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
	log.Debugw("storage_integrity: insert capture armed",
		"session", qctx.Session.ID(),
		"statement_id", statementID,
		"table_id", target.tableID,
		"unsafe_table", unsafeTable,
		"safe_table", safeTable,
	)
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
	defaultDB := ""
	if qctx.Session != nil {
		st := qctx.Session.State()
		defaultDB = firstNonEmpty(st.LogicalDatabase, st.Database)
	}
	if tableID, ok := insertTargetFromSQL(firstNonEmpty(qctx.OriginalSQL, qctx.Query.Body), defaultDB); ok {
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

func inferStatementTypeFromSQL(sql string) sqlmeta.StatementType {
	sql = strings.TrimLeftFunc(sql, unicode.IsSpace)
	if keywordPrefix(sql, "insert") {
		return sqlmeta.StatementTypeInsert
	}
	if keywordPrefix(sql, "select") {
		return sqlmeta.StatementTypeSelect
	}
	return sqlmeta.StatementTypeUnspecified
}

func keywordPrefix(sql, keyword string) bool {
	if len(sql) < len(keyword) || !strings.EqualFold(sql[:len(keyword)], keyword) {
		return false
	}
	return isBoundary(sql, len(keyword))
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

func insertTargetFromSQL(sql, defaultDB string) (string, bool) {
	start, ok := findKeywordEnd(sql, "insert into")
	if !ok {
		return "", false
	}
	i := skipSpaces(sql, start)
	if end, ok := matchKeyword(sql, i, "table"); ok {
		i = skipSpaces(sql, end)
	}
	parts := make([]string, 0, 2)
	for {
		token, end, ok := readIdentifierToken(sql, i)
		if !ok {
			return "", false
		}
		parts = append(parts, token)
		i = skipSpaces(sql, end)
		if i >= len(sql) || sql[i] != '.' {
			break
		}
		i = skipSpaces(sql, i+1)
	}
	if len(parts) == 0 {
		return "", false
	}
	if len(parts) == 1 && strings.TrimSpace(defaultDB) != "" {
		return strings.TrimSpace(defaultDB) + "." + parts[0], true
	}
	return strings.Join(parts, "."), true
}

func matchKeyword(sql string, i int, keyword string) (int, bool) {
	end := i + len(keyword)
	if i < 0 || end > len(sql) || !strings.EqualFold(sql[i:end], keyword) {
		return 0, false
	}
	if !isBoundary(sql, i-1) || !isBoundary(sql, end) {
		return 0, false
	}
	return end, true
}

func readIdentifierToken(sql string, i int) (string, int, bool) {
	if i >= len(sql) {
		return "", 0, false
	}
	switch sql[i] {
	case '`':
		return readQuotedIdentifier(sql, i, '`')
	case '"':
		return readQuotedIdentifier(sql, i, '"')
	default:
		start := i
		for i < len(sql) {
			r := rune(sql[i])
			if unicode.IsSpace(r) || r == '(' || r == ')' || r == ',' || r == ';' || r == '.' {
				break
			}
			i++
		}
		if i == start {
			return "", 0, false
		}
		return sql[start:i], i, true
	}
}

func readQuotedIdentifier(sql string, i int, quote byte) (string, int, bool) {
	var b strings.Builder
	i++
	for i < len(sql) {
		if sql[i] != quote {
			b.WriteByte(sql[i])
			i++
			continue
		}
		if i+1 < len(sql) && sql[i+1] == quote {
			b.WriteByte(quote)
			i += 2
			continue
		}
		return b.String(), i + 1, true
	}
	return "", 0, false
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
