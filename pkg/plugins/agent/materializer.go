package agent

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/ClickHouse/ch-go/proto"

	"housegate/housegate/pkg/chproto"
	"housegate/housegate/pkg/plugin"
	"housegate/housegate/pkg/replay/payloadexec"
)

type MaterializerOptions struct {
	Now   func() time.Time
	Rand  func() uint64
	UUID  func() string
	RowID RowIDOptions
}

type RowIDOptions struct {
	Enabled   bool
	NetworkID string
}

type Materializer struct {
	now  func() time.Time
	rand func() uint64
	uuid func() string

	rowID RowIDOptions
	mu    sync.Mutex
	rows  map[int64]*rowIDState
}

type rowIDState struct {
	tableID     string
	statementID string
	nextOrdinal uint64
}

func NewMaterializer(opts MaterializerOptions) *Materializer {
	m := &Materializer{
		now:  opts.Now,
		rand: opts.Rand,
		uuid: opts.UUID,
		rowID: RowIDOptions{
			Enabled:   opts.RowID.Enabled,
			NetworkID: strings.TrimSpace(opts.RowID.NetworkID),
		},
		rows: map[int64]*rowIDState{},
	}
	if m.now == nil {
		m.now = time.Now
	}
	if m.rand == nil {
		m.rand = randomUint64
	}
	if m.uuid == nil {
		m.uuid = randomUUIDv4
	}
	return m
}

func (m *Materializer) OnQuery(_ context.Context, qctx *plugin.QueryContext) error {
	if m == nil || qctx == nil || qctx.Query == nil {
		return nil
	}
	rewritten := materializeNondeterministicSQL(qctx.Query.Body, m.literalFor)
	if rewritten != qctx.Query.Body {
		qctx.Query.Body = rewritten
		qctx.RewrittenSQL = rewritten
	}
	if err := m.materializeRowIDs(qctx); err != nil {
		return err
	}
	return nil
}

func (m *Materializer) materializeRowIDs(qctx *plugin.QueryContext) error {
	if !m.rowID.Enabled {
		return nil
	}
	if m.rowID.NetworkID == "" {
		return fmt.Errorf("agent row-id materializer: network_id is required")
	}
	if qctx.Session != nil {
		m.disarmRowID(qctx.Session.ID())
	}
	tableID, ok := parseInsertTableID(qctx.Query.Body)
	if !ok {
		return nil
	}
	if containsInsertSelect(qctx.Query.Body) {
		return fmt.Errorf("agent row-id materializer: INSERT ... SELECT is unsupported")
	}
	if qctx.Query.ID == "" {
		qctx.Query.ID = randomUUIDv4()
	}
	state := &rowIDState{tableID: tableID, statementID: qctx.Query.ID}
	if rewritten, rows, ok, err := injectRowIDsIntoValuesSQL(qctx.Query.Body, m.rowID.NetworkID, state); err != nil {
		return err
	} else if ok {
		state.nextOrdinal += uint64(rows)
		qctx.Query.Body = rewritten
		qctx.RewrittenSQL = rewritten
		return nil
	}
	rewritten, err := ensureRowIDColumnInInsertList(qctx.Query.Body)
	if err != nil {
		return err
	}
	if rewritten != qctx.Query.Body {
		qctx.Query.Body = rewritten
		qctx.RewrittenSQL = rewritten
	}
	if qctx.Session != nil && containsFormatNative(qctx.Query.Body) {
		if qctx.Query.Compression != proto.CompressionDisabled {
			return fmt.Errorf("agent row-id materializer: compressed Native Data blocks are unsupported")
		}
		m.mu.Lock()
		m.rows[qctx.Session.ID()] = state
		m.mu.Unlock()
		return nil
	}
	if !containsFormatNative(qctx.Query.Body) {
		return fmt.Errorf("agent row-id materializer: only INSERT ... VALUES and FORMAT Native are supported")
	}
	return nil
}

func (m *Materializer) RewriteClientData(_ context.Context, qctx *plugin.QueryContext, raw []byte) ([]byte, error) {
	if m == nil || !m.rowID.Enabled || qctx == nil || qctx.Session == nil || len(raw) == 0 {
		return raw, nil
	}
	m.mu.Lock()
	st := m.rows[qctx.Session.ID()]
	m.mu.Unlock()
	if st == nil {
		return raw, nil
	}
	revision := qctx.Session.State().ClientRevision
	if empty, err := chproto.IsEmptyClientDataBlock(raw, revision); err == nil && empty {
		m.disarmRowID(qctx.Session.ID())
		return raw, nil
	}
	rewritten, rows, err := injectRowIDIntoNativeData(raw, revision, m.rowID.NetworkID, st)
	if err != nil {
		return raw, err
	}
	if rows > 0 {
		m.mu.Lock()
		if current := m.rows[qctx.Session.ID()]; current == st {
			st.nextOrdinal += uint64(rows)
		}
		m.mu.Unlock()
	}
	return rewritten, nil
}

func (m *Materializer) disarmRowID(sessionID int64) {
	m.mu.Lock()
	delete(m.rows, sessionID)
	m.mu.Unlock()
}

func (m *Materializer) literalFor(name string) string {
	switch strings.ToLower(name) {
	case "now":
		return "'" + m.now().UTC().Format("2006-01-02 15:04:05") + "'"
	case "rand", "rand64", "random":
		return fmt.Sprintf("%d", m.rand())
	case "generateuuidv4":
		return "'" + m.uuid() + "'"
	default:
		return name
	}
}

func materializeNondeterministicSQL(sql string, literal func(string) string) string {
	var b strings.Builder
	b.Grow(len(sql))
	for i := 0; i < len(sql); {
		switch sql[i] {
		case '\'':
			next := copyQuotedSQL(&b, sql, i, '\'')
			i = next
			continue
		case '`':
			next := copyQuotedSQL(&b, sql, i, '`')
			i = next
			continue
		case '"':
			next := copyQuotedSQL(&b, sql, i, '"')
			i = next
			continue
		case '-':
			if i+1 < len(sql) && sql[i+1] == '-' {
				next := copyLineComment(&b, sql, i)
				i = next
				continue
			}
		case '/':
			if i+1 < len(sql) && sql[i+1] == '*' {
				next := copyBlockComment(&b, sql, i)
				i = next
				continue
			}
		}
		if isIdentStart(rune(sql[i])) {
			name, end := readIdent(sql, i)
			if isNondeterministicFunction(name) {
				if close, ok := zeroArgCallEnd(sql, end); ok {
					b.WriteString(literal(name))
					i = close
					continue
				}
			}
		}
		b.WriteByte(sql[i])
		i++
	}
	return b.String()
}

func parseInsertTableID(sql string) (string, bool) {
	i := skipSQLSpaces(sql, 0)
	if !consumeKeyword(sql, &i, "insert") {
		return "", false
	}
	if !consumeKeyword(sql, &i, "into") {
		return "", false
	}
	i = skipSQLSpaces(sql, i)
	table, end, ok := readTableName(sql, i)
	if !ok {
		return "", false
	}
	_ = end
	return table, true
}

func readTableName(sql string, start int) (string, int, bool) {
	var parts []string
	i := start
	for {
		i = skipSQLSpaces(sql, i)
		var part string
		if i < len(sql) && sql[i] == '`' {
			end, unquoted, ok := readBacktickIdent(sql, i)
			if !ok {
				return "", start, false
			}
			part = unquoted
			i = end
		} else if i < len(sql) && isIdentStart(rune(sql[i])) {
			part, i = readIdent(sql, i)
		} else {
			break
		}
		parts = append(parts, part)
		i = skipSQLSpaces(sql, i)
		if i >= len(sql) || sql[i] != '.' {
			break
		}
		i++
	}
	if len(parts) == 0 {
		return "", start, false
	}
	return strings.Join(parts, "."), i, true
}

func readBacktickIdent(sql string, start int) (int, string, bool) {
	var b strings.Builder
	i := start + 1
	for i < len(sql) {
		if sql[i] == '`' {
			if i+1 < len(sql) && sql[i+1] == '`' {
				b.WriteByte('`')
				i += 2
				continue
			}
			return i + 1, b.String(), true
		}
		b.WriteByte(sql[i])
		i++
	}
	return start, "", false
}

func containsInsertSelect(sql string) bool {
	i := 0
	for i < len(sql) {
		next, ok := skipSQLLiteralOrComment(sql, i)
		if ok {
			i = next
			continue
		}
		if isIdentStart(rune(sql[i])) {
			name, end := readIdent(sql, i)
			if strings.EqualFold(name, "select") {
				return true
			}
			i = end
			continue
		}
		i++
	}
	return false
}

func containsFormatNative(sql string) bool {
	i := 0
	for i < len(sql) {
		next, ok := skipSQLLiteralOrComment(sql, i)
		if ok {
			i = next
			continue
		}
		if isIdentStart(rune(sql[i])) {
			name, end := readIdent(sql, i)
			if strings.EqualFold(name, "format") {
				j := skipSQLSpaces(sql, end)
				if j < len(sql) && isIdentStart(rune(sql[j])) {
					format, _ := readIdent(sql, j)
					return strings.EqualFold(format, "native")
				}
			}
			i = end
			continue
		}
		i++
	}
	return false
}

func ensureRowIDColumnInInsertList(sql string) (string, error) {
	_, targetEnd, ok := parseInsertTargetEnd(sql)
	if !ok {
		return sql, nil
	}
	i := skipSQLSpaces(sql, targetEnd)
	if i >= len(sql) || sql[i] != '(' {
		return sql, nil
	}
	close, ok := matchingParen(sql, i)
	if !ok {
		return "", fmt.Errorf("agent row-id materializer: malformed INSERT column list")
	}
	cols := sql[i+1 : close]
	if containsColumnName(cols, "_hg_row_id") {
		return "", fmt.Errorf("agent row-id materializer: user INSERT must not include _hg_row_id")
	}
	return sql[:i+1] + "_hg_row_id, " + sql[i+1:], nil
}

func injectRowIDsIntoValuesSQL(sql, networkID string, st *rowIDState) (string, int, bool, error) {
	valuesStart, ok := findKeywordOutsideSQL(sql, "values")
	if !ok {
		return sql, 0, false, nil
	}
	withCols, err := ensureRowIDColumnInInsertList(sql[:valuesStart])
	if err != nil {
		return "", 0, false, err
	}
	var b strings.Builder
	b.Grow(len(sql) + 96)
	b.WriteString(withCols)
	i := valuesStart
	b.WriteString(sql[i : i+lenKeywordAt(sql, i)])
	i += lenKeywordAt(sql, i)
	rows := 0
	for i < len(sql) {
		next, skipped := skipSQLLiteralOrComment(sql, i)
		if skipped {
			b.WriteString(sql[i:next])
			i = next
			continue
		}
		if sql[i] != '(' {
			b.WriteByte(sql[i])
			i++
			continue
		}
		rowID := payloadexec.RowID(networkID, st.tableID, st.statementID, st.nextOrdinal+uint64(rows))
		b.WriteByte('(')
		b.WriteString("unhex('")
		b.WriteString(hex.EncodeToString(rowID))
		b.WriteString("')")
		close, ok := matchingParen(sql, i)
		if !ok {
			return "", 0, true, fmt.Errorf("agent row-id materializer: malformed VALUES tuple")
		}
		if strings.TrimSpace(sql[i+1:close]) != "" {
			b.WriteString(", ")
			b.WriteString(sql[i+1 : close])
		}
		b.WriteByte(')')
		rows++
		i = close + 1
	}
	return b.String(), rows, true, nil
}

func injectRowIDIntoNativeData(raw []byte, revision int, networkID string, st *rowIDState) ([]byte, int, error) {
	r := proto.NewReader(bytes.NewReader(raw))
	code, err := r.UVarInt()
	if err != nil {
		return raw, 0, fmt.Errorf("agent row-id materializer: data packet code: %w", err)
	}
	if code != uint64(chproto.ClientDataCode) {
		return raw, 0, fmt.Errorf("agent row-id materializer: packet type %d is not ClientData", code)
	}
	blockName, err := r.Str()
	if err != nil {
		return raw, 0, fmt.Errorf("agent row-id materializer: block name: %w", err)
	}
	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, revision, results.Auto()); err != nil {
		return raw, 0, fmt.Errorf("agent row-id materializer: decode Native block: %w", err)
	}
	if block.Rows == 0 {
		return raw, 0, nil
	}
	input := make(proto.Input, 0, len(results)+1)
	rowIDs := &proto.ColFixedStr{Size: 32}
	for row := 0; row < block.Rows; row++ {
		rowIDs.Append(payloadexec.RowID(networkID, st.tableID, st.statementID, st.nextOrdinal+uint64(row)))
	}
	input = append(input, proto.InputColumn{Name: "_hg_row_id", Data: rowIDs})
	for _, result := range results {
		if strings.EqualFold(result.Name, "_hg_row_id") {
			return raw, 0, fmt.Errorf("agent row-id materializer: user Data block must not include _hg_row_id")
		}
		col := result.Data
		if auto, ok := col.(*proto.ColAuto); ok {
			col = auto.Data
		}
		in, ok := col.(proto.ColInput)
		if !ok {
			return raw, 0, fmt.Errorf("agent row-id materializer: column %q (%T) is not encodable", result.Name, col)
		}
		input = append(input, proto.InputColumn{Name: result.Name, Data: in})
	}
	var out proto.Buffer
	out.PutUVarInt(uint64(proto.ClientCodeData))
	out.PutString(blockName)
	encoded := proto.Block{Rows: block.Rows, Columns: len(input)}
	if err := encoded.EncodeBlock(&out, revision, input); err != nil {
		return raw, 0, fmt.Errorf("agent row-id materializer: encode Native block: %w", err)
	}
	return out.Buf, block.Rows, nil
}

func parseInsertTargetEnd(sql string) (string, int, bool) {
	i := skipSQLSpaces(sql, 0)
	if !consumeKeyword(sql, &i, "insert") {
		return "", 0, false
	}
	if !consumeKeyword(sql, &i, "into") {
		return "", 0, false
	}
	return readTableName(sql, i)
}

func consumeKeyword(sql string, i *int, keyword string) bool {
	j := skipSQLSpaces(sql, *i)
	if j+len(keyword) > len(sql) || !strings.EqualFold(sql[j:j+len(keyword)], keyword) {
		return false
	}
	beforeOK := j == 0 || !isIdentPart(rune(sql[j-1]))
	after := j + len(keyword)
	afterOK := after >= len(sql) || !isIdentPart(rune(sql[after]))
	if !beforeOK || !afterOK {
		return false
	}
	*i = after
	return true
}

func findKeywordOutsideSQL(sql, keyword string) (int, bool) {
	i := 0
	for i < len(sql) {
		next, ok := skipSQLLiteralOrComment(sql, i)
		if ok {
			i = next
			continue
		}
		if isIdentStart(rune(sql[i])) {
			name, end := readIdent(sql, i)
			if strings.EqualFold(name, keyword) {
				return i, true
			}
			i = end
			continue
		}
		i++
	}
	return 0, false
}

func lenKeywordAt(sql string, i int) int {
	if i >= len(sql) || !isIdentStart(rune(sql[i])) {
		return 0
	}
	_, end := readIdent(sql, i)
	return end - i
}

func matchingParen(sql string, open int) (int, bool) {
	depth := 0
	for i := open; i < len(sql); {
		next, ok := skipSQLLiteralOrComment(sql, i)
		if ok {
			i = next
			continue
		}
		switch sql[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i, true
			}
		}
		i++
	}
	return 0, false
}

func containsColumnName(cols, name string) bool {
	i := 0
	for i < len(cols) {
		next, ok := skipSQLLiteralOrComment(cols, i)
		if ok {
			i = next
			continue
		}
		if isIdentStart(rune(cols[i])) {
			got, end := readIdent(cols, i)
			if strings.EqualFold(got, name) {
				return true
			}
			i = end
			continue
		}
		i++
	}
	return false
}

func skipSQLLiteralOrComment(sql string, i int) (int, bool) {
	if i >= len(sql) {
		return i, false
	}
	switch sql[i] {
	case '\'':
		return scanQuotedSQL(sql, i, '\''), true
	case '`':
		return scanQuotedSQL(sql, i, '`'), true
	case '"':
		return scanQuotedSQL(sql, i, '"'), true
	case '-':
		if i+1 < len(sql) && sql[i+1] == '-' {
			return scanLineComment(sql, i), true
		}
	case '/':
		if i+1 < len(sql) && sql[i+1] == '*' {
			return scanBlockComment(sql, i), true
		}
	}
	return i, false
}

func scanQuotedSQL(sql string, start int, quote byte) int {
	i := start + 1
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

func scanLineComment(sql string, start int) int {
	i := start
	for i < len(sql) {
		i++
		if sql[i-1] == '\n' {
			return i
		}
	}
	return i
}

func scanBlockComment(sql string, start int) int {
	i := start
	for i < len(sql) {
		if i > start && sql[i-1] == '*' && sql[i] == '/' {
			return i + 1
		}
		i++
	}
	return i
}

func copyQuotedSQL(b *strings.Builder, sql string, start int, quote byte) int {
	i := start
	b.WriteByte(sql[i])
	i++
	for i < len(sql) {
		b.WriteByte(sql[i])
		if sql[i] == '\\' && quote == '\'' && i+1 < len(sql) {
			i++
			b.WriteByte(sql[i])
			i++
			continue
		}
		if sql[i] == quote {
			if i+1 < len(sql) && sql[i+1] == quote {
				i++
				b.WriteByte(sql[i])
				i++
				continue
			}
			i++
			return i
		}
		i++
	}
	return i
}

func copyLineComment(b *strings.Builder, sql string, start int) int {
	i := start
	for i < len(sql) {
		b.WriteByte(sql[i])
		i++
		if sql[i-1] == '\n' {
			return i
		}
	}
	return i
}

func copyBlockComment(b *strings.Builder, sql string, start int) int {
	i := start
	for i < len(sql) {
		b.WriteByte(sql[i])
		if i > start && sql[i-1] == '*' && sql[i] == '/' {
			i++
			return i
		}
		i++
	}
	return i
}

func readIdent(sql string, start int) (string, int) {
	i := start
	for i < len(sql) && isIdentPart(rune(sql[i])) {
		i++
	}
	return sql[start:i], i
}

func zeroArgCallEnd(sql string, start int) (int, bool) {
	i := skipSQLSpaces(sql, start)
	if i >= len(sql) || sql[i] != '(' {
		return 0, false
	}
	i = skipSQLSpaces(sql, i+1)
	if i >= len(sql) || sql[i] != ')' {
		return 0, false
	}
	return i + 1, true
}

func skipSQLSpaces(sql string, i int) int {
	for i < len(sql) && unicode.IsSpace(rune(sql[i])) {
		i++
	}
	return i
}

func isNondeterministicFunction(name string) bool {
	switch strings.ToLower(name) {
	case "now", "rand", "rand64", "random", "generateuuidv4":
		return true
	default:
		return false
	}
}

func isIdentStart(r rune) bool {
	return unicode.IsLetter(r) || r == '_'
}

func isIdentPart(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

func randomUint64() uint64 {
	var buf [8]byte
	if _, err := crand.Read(buf[:]); err != nil {
		return uint64(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint64(buf[:])
}

func randomUUIDv4() string {
	var b [16]byte
	if _, err := crand.Read(b[:]); err != nil {
		n := uint64(time.Now().UnixNano())
		binary.BigEndian.PutUint64(b[:8], n)
		binary.BigEndian.PutUint64(b[8:], n^0xa5a5a5a5a5a5a5a5)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		binary.BigEndian.Uint32(b[0:4]),
		binary.BigEndian.Uint16(b[4:6]),
		binary.BigEndian.Uint16(b[6:8]),
		binary.BigEndian.Uint16(b[8:10]),
		b[10:16],
	)
}

var _ plugin.QueryPlugin = (*Materializer)(nil)
