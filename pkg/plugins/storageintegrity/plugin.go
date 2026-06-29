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
	TableRewriter     TableRewriter
	PartitionIDs      []string
}

const defaultTerminatorDelay = time.Second

type TableRewriter interface {
	RewriteTables(ctx context.Context, sql string, tableMap map[string]string) (string, error)
}

type Plugin struct {
	layout       core.TableLayout
	payloads     *core.MockPayloadStore
	sink         core.IngressSink
	rewriter     TableRewriter
	partitionIDs []string

	mu              sync.Mutex
	active          map[int64]*insertCapture
	now             func() time.Time
	newStmt         func(*plugin.QueryContext) string
	terminatorDelay time.Duration
}

type insertCapture struct {
	tableID        string
	statementID    string
	originalSQL    string
	unsafeSQL      string
	unsafeTable    string
	safeTable      string
	partitionIDs   []string
	dataPackets    [][]byte
	startedAt      time.Time
	timer          *time.Timer
	terminatorSeen bool
}

func New(cfg Config, payloads *core.MockPayloadStore, sink core.IngressSink) *Plugin {
	return &Plugin{
		layout: core.NewTableLayout(core.TableLayoutConfig{
			UnsafeDatabase:    cfg.UnsafeDatabase,
			SafeDatabase:      cfg.SafeDatabase,
			UnsafeTableSuffix: cfg.UnsafeTableSuffix,
		}),
		payloads:        payloads,
		sink:            sink,
		rewriter:        cfg.TableRewriter,
		partitionIDs:    normalizePartitionIDs(cfg.PartitionIDs),
		active:          map[int64]*insertCapture{},
		now:             time.Now,
		newStmt:         defaultStatementID,
		terminatorDelay: defaultTerminatorDelay,
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
		return p.onInsert(ctx, qctx)
	case sqlmeta.StatementTypeSelect:
		return p.onSelect(ctx, qctx)
	case sqlmeta.StatementTypeUnspecified:
		inferred := inferStatementTypeFromSQL(firstNonEmpty(qctx.OriginalSQL, qctx.Query.Body))
		switch inferred {
		case sqlmeta.StatementTypeInsert:
			qctx.StatementType = inferred
			return p.onInsert(ctx, qctx)
		case sqlmeta.StatementTypeSelect:
			qctx.StatementType = inferred
			return p.onSelect(ctx, qctx)
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
	if complete && cap.timer == nil {
		cap.terminatorSeen = true
		sessionID := qctx.Session.ID()
		delay := p.terminatorDelay
		if delay <= 0 {
			delay = defaultTerminatorDelay
		}
		cap.timer = time.AfterFunc(delay, func() {
			p.finalizeSession(context.Background(), sessionID, cap)
		})
	}
	p.mu.Unlock()
	return nil
}

func (p *Plugin) OnQueryComplete(ctx context.Context, sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.finalizeSession(ctx, sess.ID(), nil)
}

func (p *Plugin) finalizeSession(ctx context.Context, sessionID int64, expected *insertCapture) {
	p.mu.Lock()
	cap := p.active[sessionID]
	if expected != nil && cap != expected {
		p.mu.Unlock()
		return
	}
	delete(p.active, sessionID)
	if cap != nil && cap.timer != nil {
		cap.timer.Stop()
		cap.timer = nil
	}
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
		TableID:      cap.tableID,
		StatementID:  cap.statementID,
		OriginalSQL:  cap.originalSQL,
		UnsafeSQL:    cap.unsafeSQL,
		UnsafeTable:  cap.unsafeTable,
		SafeTable:    cap.safeTable,
		PartitionIDs: append([]string(nil), cap.partitionIDs...),
		Payload:      commit,
		ReceivedAt:   p.now().UTC(),
	}); err != nil {
		log.Warnw("storage_integrity: submit insert failed",
			"statement_id", cap.statementID,
			"table_id", cap.tableID,
			"err", err,
		)
		return
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
	empty, err := chproto.IsEmptyClientDataBlock(raw, revision)
	return err == nil && empty
}

func normalizePartitionIDs(ids []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func (p *Plugin) OnException(ctx context.Context, sess chsession.Session, _ *chproto.Exception) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p == nil || sess == nil {
		return nil
	}
	p.dropCapture(sess.ID())
	return nil
}

func (p *Plugin) OnClose(sess chsession.Session) {
	if p == nil || sess == nil {
		return
	}
	p.finalizeTerminatedOrDrop(sess.ID())
}

func (p *Plugin) dropCapture(sessionID int64) {
	p.mu.Lock()
	cap := p.active[sessionID]
	delete(p.active, sessionID)
	if cap != nil && cap.timer != nil {
		cap.timer.Stop()
		cap.timer = nil
	}
	p.mu.Unlock()
}

func (p *Plugin) finalizeTerminatedOrDrop(sessionID int64) {
	p.mu.Lock()
	cap := p.active[sessionID]
	delete(p.active, sessionID)
	if cap != nil && cap.timer != nil {
		cap.timer.Stop()
		cap.timer = nil
	}
	if cap == nil || !cap.terminatorSeen {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()
	p.finalizeCapture(context.Background(), cap)
}

func (p *Plugin) onInsert(ctx context.Context, qctx *plugin.QueryContext) error {
	if containsUnmaterializedNondeterminism(qctx.Query.Body) || containsUnmaterializedNondeterminism(qctx.OriginalSQL) {
		return fmt.Errorf("storage_integrity: non-deterministic INSERT must be materialized before signing")
	}
	target, ok := targetFromContext(qctx)
	if !ok {
		return fmt.Errorf("storage_integrity: INSERT target is required")
	}
	ensureAccessedTable(qctx, target.tableID)
	unsafeTable := p.layout.UnsafeTable(target.tableID)
	safeTable := p.layout.SafeTable(target.tableID)
	unsafeSQL, err := p.rewriteInsertTable(ctx, qctx, target, p.unsafeRewriteTarget(target.tableID), unsafeTable)
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
		tableID:      target.tableID,
		statementID:  statementID,
		originalSQL:  qctx.OriginalSQL,
		unsafeSQL:    unsafeSQL,
		unsafeTable:  unsafeTable,
		safeTable:    safeTable,
		partitionIDs: append([]string(nil), p.partitionIDs...),
		startedAt:    p.now().UTC(),
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

func (p *Plugin) onSelect(ctx context.Context, qctx *plugin.QueryContext) error {
	target, ok := targetFromContext(qctx)
	if !ok {
		return nil
	}
	safeSQL, err := p.rewriteTable(ctx, qctx, target, p.safeRewriteTarget(target.tableID), p.layout.SafeTable(target.tableID))
	if err != nil {
		return fmt.Errorf("storage_integrity rewrite SELECT to safe: %w", err)
	}
	qctx.Query.Body = safeSQL
	qctx.RewrittenSQL = safeSQL
	return nil
}

type sqlTarget struct {
	tableID       string
	rewriteTarget string
}

func (p *Plugin) rewriteTable(ctx context.Context, qctx *plugin.QueryContext, target sqlTarget, rewriteTo, fallbackTo string) (string, error) {
	from := target.rewriteTarget
	if strings.TrimSpace(from) == "" {
		from = target.tableID
	}
	if p.rewriter != nil {
		return p.rewriter.RewriteTables(ctx, qctx.Query.Body, map[string]string{from: rewriteTo})
	}
	return "", fmt.Errorf("sql rewriter is required for storage-integrity table routing")
}

func (p *Plugin) rewriteInsertTable(ctx context.Context, qctx *plugin.QueryContext, target sqlTarget, rewriteTo, fallbackTo string) (string, error) {
	sql, err := p.rewriteTable(ctx, qctx, target, rewriteTo, fallbackTo)
	if err != nil {
		return "", err
	}
	if targetID, ok := insertTargetFromSQL(sql, ""); ok && strings.EqualFold(targetID, rewriteTo) && !insertRewriteLostInlineValues(qctx, sql) {
		return sql, nil
	}
	source := sql
	if insertRewriteLostInlineValues(qctx, sql) {
		source = originalInsertSQL(qctx)
	}
	rewritten, err := replaceInsertTarget(source, fallbackTo)
	if err != nil {
		return "", err
	}
	return rewritten, nil
}

func insertRewriteLostInlineValues(qctx *plugin.QueryContext, rewritten string) bool {
	return hasInlineValuesClause(originalInsertSQL(qctx)) && hasFormatValuesClause(rewritten)
}

func originalInsertSQL(qctx *plugin.QueryContext) string {
	if qctx == nil {
		return ""
	}
	body := ""
	if qctx.Query != nil {
		body = qctx.Query.Body
	}
	return firstNonEmpty(qctx.OriginalSQL, body)
}

func hasInlineValuesClause(sql string) bool {
	valuesEnd, ok := findKeywordEnd(sql, "values")
	if !ok {
		return false
	}
	formatEnd, ok := findKeywordEnd(sql, "format")
	return !ok || formatEnd > valuesEnd
}

func hasFormatValuesClause(sql string) bool {
	formatEnd, ok := findKeywordEnd(sql, "format")
	if !ok {
		return false
	}
	_, ok = matchKeyword(sql, skipSpaces(sql, formatEnd), "values")
	return ok
}

func (p *Plugin) unsafeRewriteTarget(tableID string) string {
	return p.layout.UnsafeDatabase + "." + tableID + p.layout.UnsafeTableSuffix
}

func (p *Plugin) safeRewriteTarget(tableID string) string {
	return p.layout.SafeDatabase + "." + tableID
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
		return sqlTarget{tableID: tableID, rewriteTarget: rewrittenTargetFor(qctx, tableID, t)}, true
	}
	defaultDB := ""
	if qctx.Session != nil {
		st := qctx.Session.State()
		defaultDB = firstNonEmpty(st.LogicalDatabase, st.Database)
	}
	sql := firstNonEmpty(qctx.OriginalSQL, qctx.Query.Body)
	switch qctx.StatementType {
	case sqlmeta.StatementTypeInsert:
		if tableID, ok := insertTargetFromSQL(sql, defaultDB); ok {
			return sqlTarget{tableID: tableID, rewriteTarget: rewrittenTargetFor(qctx, tableID, sqlmeta.AccessedTable{})}, true
		}
	case sqlmeta.StatementTypeSelect:
		if tableID, ok := selectTargetFromSQL(sql, defaultDB); ok {
			return sqlTarget{tableID: tableID, rewriteTarget: rewrittenTargetFor(qctx, tableID, sqlmeta.AccessedTable{})}, true
		}
	default:
		if tableID, ok := insertTargetFromSQL(sql, defaultDB); ok {
			return sqlTarget{tableID: tableID, rewriteTarget: rewrittenTargetFor(qctx, tableID, sqlmeta.AccessedTable{})}, true
		}
		if tableID, ok := selectTargetFromSQL(sql, defaultDB); ok {
			return sqlTarget{tableID: tableID, rewriteTarget: rewrittenTargetFor(qctx, tableID, sqlmeta.AccessedTable{})}, true
		}
	}
	return sqlTarget{}, false
}

func targetFromSQLAfterKeyword(sql, keyword, defaultDB string) (string, bool) {
	start, ok := findKeywordEnd(sql, keyword)
	if !ok {
		return "", false
	}
	i := skipSpaces(sql, start)
	if strings.EqualFold(keyword, "insert into") {
		if end, ok := matchKeyword(sql, i, "table"); ok {
			i = skipSpaces(sql, end)
		}
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

func selectTargetFromSQL(sql, defaultDB string) (string, bool) {
	return targetFromSQLAfterKeyword(sql, "from", defaultDB)
}

func insertTargetFromSQL(sql, defaultDB string) (string, bool) {
	return targetFromSQLAfterKeyword(sql, "insert into", defaultDB)
}

func ensureAccessedTable(qctx *plugin.QueryContext, tableID string) {
	if qctx == nil || len(qctx.AccessedTables) != 0 {
		return
	}
	db, table := splitTableID(tableID)
	if strings.TrimSpace(table) == "" {
		return
	}
	qctx.AccessedTables = []sqlmeta.AccessedTable{{
		OriginalDatabase: db,
		OriginalTable:    table,
		LogicalDatabase:  db,
	}}
}

func splitTableID(tableID string) (string, string) {
	parts := strings.Split(tableID, ".")
	if len(parts) < 2 {
		return "", tableID
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

func rewrittenTargetFor(qctx *plugin.QueryContext, tableID string, accessed sqlmeta.AccessedTable) string {
	if qctx == nil {
		return tableID
	}
	if accessed.PhysicalDatabase != "" && accessed.OriginalTable != "" {
		table := accessed.OriginalTable
		if logical := firstNonEmpty(accessed.LogicalDatabase, accessed.OriginalDatabase); logical != "" {
			table = logical + "." + accessed.OriginalTable
		}
		return accessed.PhysicalDatabase + "." + table
	}
	if qctx.TableRewrites != nil {
		if rewritten := qctx.TableRewrites[tableID]; strings.TrimSpace(rewritten) != "" {
			return rewritten
		}
		if accessed.OriginalTable != "" {
			if rewritten := qctx.TableRewrites[accessed.OriginalTable]; strings.TrimSpace(rewritten) != "" {
				return rewritten
			}
		}
	}
	return tableID
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

func replaceInsertTarget(sql, target string) (string, error) {
	start, ok := findKeywordEnd(sql, "insert into")
	if !ok {
		return "", fmt.Errorf("keyword %q not found", "insert into")
	}
	i := skipSpaces(sql, start)
	if end, ok := matchKeyword(sql, i, "table"); ok {
		i = skipSpaces(sql, end)
	}
	targetStart := i
	targetEnd, ok := scanQualifiedIdentifier(sql, targetStart)
	if !ok {
		return "", fmt.Errorf("target identifier not found after %q", "insert into")
	}
	return sql[:targetStart] + target + sql[targetEnd:], nil
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

func containsUnmaterializedNondeterminism(sql string) bool {
	for i := 0; i < len(sql); {
		switch sql[i] {
		case '\'':
			i = skipQuotedSQL(sql, i, '\'')
			continue
		case '`':
			i = skipQuotedSQL(sql, i, '`')
			continue
		case '"':
			i = skipQuotedSQL(sql, i, '"')
			continue
		case '-':
			if i+1 < len(sql) && sql[i+1] == '-' {
				i = skipLineComment(sql, i)
				continue
			}
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				i = skipBlockComment(sql, i)
				continue
			}
		}
		if isSQLIdentStart(rune(sql[i])) {
			name, end := readSQLIdent(sql, i)
			if isNondeterministicZeroArgFunction(name) {
				if _, ok := zeroArgFunctionCallEnd(sql, end); ok {
					return true
				}
			}
			i = end
			continue
		}
		i++
	}
	return false
}

func skipQuotedSQL(sql string, i int, quote byte) int {
	i++
	for i < len(sql) {
		if sql[i] == '\\' && quote == '\'' && i+1 < len(sql) {
			i += 2
			continue
		}
		if sql[i] == quote {
			if i+1 < len(sql) && sql[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		}
		i++
	}
	return i
}

func skipLineComment(sql string, i int) int {
	for i < len(sql) {
		i++
		if sql[i-1] == '\n' {
			return i
		}
	}
	return i
}

func skipBlockComment(sql string, i int) int {
	i += 2
	for i < len(sql) {
		if i+1 < len(sql) && sql[i] == '*' && sql[i+1] == '/' {
			return i + 2
		}
		i++
	}
	return i
}

func readSQLIdent(sql string, i int) (string, int) {
	start := i
	for i < len(sql) && isSQLIdentPart(rune(sql[i])) {
		i++
	}
	return sql[start:i], i
}

func zeroArgFunctionCallEnd(sql string, i int) (int, bool) {
	i = skipSpaces(sql, i)
	if i >= len(sql) || sql[i] != '(' {
		return 0, false
	}
	i = skipSpaces(sql, i+1)
	if i >= len(sql) || sql[i] != ')' {
		return 0, false
	}
	return i + 1, true
}

func isNondeterministicZeroArgFunction(name string) bool {
	switch strings.ToLower(name) {
	case "now", "rand", "rand64", "random", "generateuuidv4":
		return true
	default:
		return false
	}
}

func isSQLIdentStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isSQLIdentPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
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
