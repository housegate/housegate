package storageintegrity

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"housegate/housegate/pkg/replay"
)

type ClickHouseInsertReplayVerifier struct {
	conn     clickhouseReplayConn
	Signer   replay.Signer
	Payloads replay.PayloadStore
	Timeout  time.Duration
	counter  atomic.Uint64
}

type clickhouseReplayConn interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	PrepareBatch(ctx context.Context, query string, opts ...driver.PrepareBatchOption) (driver.Batch, error)
}

func NewClickHouseInsertReplayVerifier(addr string, signer replay.Signer) (*ClickHouseInsertReplayVerifier, error) {
	if strings.TrimSpace(addr) == "" {
		return nil, fmt.Errorf("clickhouse upstream address is required")
	}
	if signer == nil {
		return nil, fmt.Errorf("replay signer is required")
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse replay connection: %w", err)
	}
	return &ClickHouseInsertReplayVerifier{conn: conn, Signer: signer}, nil
}

func (v *ClickHouseInsertReplayVerifier) Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) {
	if v == nil || v.conn == nil {
		return replay.ReplayAttestation{}, fmt.Errorf("clickhouse insert replay verifier is nil")
	}
	if v.Signer == nil {
		return replay.ReplayAttestation{}, fmt.Errorf("replay signer is required")
	}
	if len(job.Statements) != 1 {
		return replay.ReplayAttestation{}, fmt.Errorf("insert replay supports exactly one statement, got %d", len(job.Statements))
	}
	st := job.Statements[0]
	payload, err := v.loadStatementPayload(ctx, 0, st)
	if err != nil {
		return replay.ReplayAttestation{}, err
	}
	result, err := v.ComputeInsertReplay(ctx, InsertReplayRequest{
		TableID:     st.TargetTableID,
		StatementID: st.StatementID,
		SQL:         st.SQL,
		Payload:     payload,
	})
	if err != nil {
		return replay.ReplayAttestation{}, err
	}
	statementRoot, err := hashDomain("housegate-insert-replay-statement-root-v1", job.Statements)
	if err != nil {
		return replay.ReplayAttestation{}, err
	}
	payloadRoot, err := hashDomain("housegate-insert-replay-payload-root-v1", []struct {
		StatementID   string `json:"statement_id"`
		PayloadRef    string `json:"payload_ref"`
		PayloadHash   string `json:"payload_hash"`
		PayloadLength uint64 `json:"payload_length"`
	}{{
		StatementID:   st.StatementID,
		PayloadRef:    st.PayloadRef,
		PayloadHash:   st.PayloadHash,
		PayloadLength: st.PayloadLength,
	}})
	if err != nil {
		return replay.ReplayAttestation{}, err
	}
	receipt := replay.ExecutionReceipt{
		BlockSeq:           job.BlockSeq,
		PrevSafeSnapshotID: job.PrevSafeSnapshotID,
		PrevStateRoot:      job.PrevStateRoot,
		SchemaSnapshotID:   job.SchemaSnapshotID,
		ExecutorProfileID:  job.ExecutorProfileID,
		StatementRoot:      statementRoot,
		PayloadRoot:        payloadRoot,
		SourceClaimRoot:    job.SourceClaimRoot,
		ComputedStateRoot:  result.StateRoot,
		MatchSourceRoot:    result.StateRoot == job.SourceClaimRoot,
		ReplayLogHash:      result.RowsHash,
	}
	receiptHash, err := receipt.Hash()
	if err != nil {
		return replay.ReplayAttestation{}, err
	}
	workerID, sig, err := v.Signer.SignReplayReceipt(ctx, receiptHash)
	if err != nil {
		return replay.ReplayAttestation{}, fmt.Errorf("sign replay receipt: %w", err)
	}
	if workerID == "" || sig == "" {
		return replay.ReplayAttestation{}, fmt.Errorf("replay signer returned incomplete attestation")
	}
	return replay.ReplayAttestation{
		ReplicaID:       workerID,
		Receipt:         receipt,
		ReceiptHash:     receiptHash,
		Signature:       sig,
		MatchSourceRoot: receipt.MatchSourceRoot,
	}, nil
}

func (v *ClickHouseInsertReplayVerifier) ComputeInsertReplay(ctx context.Context, req InsertReplayRequest) (InsertReplayResult, error) {
	if v == nil || v.conn == nil {
		return InsertReplayResult{}, fmt.Errorf("clickhouse insert replay verifier is nil")
	}
	if req.TableID == "" {
		return InsertReplayResult{}, fmt.Errorf("table_id is required")
	}
	if req.StatementID == "" {
		return InsertReplayResult{}, fmt.Errorf("statement_id is required")
	}
	if strings.TrimSpace(req.SQL) == "" {
		return InsertReplayResult{}, fmt.Errorf("insert replay sql is required")
	}
	target, start, end, err := insertTargetSpan(req.SQL)
	if err != nil {
		return InsertReplayResult{}, err
	}
	timeout := durationOrDefault(v.Timeout, 30*time.Second)
	replayCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	scratch := fmt.Sprintf("_hg_replay_insert_%d_%d", time.Now().UnixNano(), v.counter.Add(1))
	scratchTable := QuoteTable("default", scratch)
	defer func() { _ = v.conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+scratchTable) }()
	if err := v.conn.Exec(replayCtx, "CREATE TABLE "+scratchTable+" AS "+target+" ENGINE = MergeTree ORDER BY tuple()"); err != nil {
		return InsertReplayResult{}, fmt.Errorf("create replay scratch table: %w", err)
	}
	if containsFormatNative(req.SQL) {
		if err := v.replayNativePayload(replayCtx, req, scratchTable); err != nil {
			return InsertReplayResult{}, err
		}
	} else {
		replaySQL := req.SQL[:start] + scratchTable + req.SQL[end:]
		if err := v.conn.Exec(replayCtx, replaySQL); err != nil {
			return InsertReplayResult{}, fmt.Errorf("execute replay insert: %w", err)
		}
	}
	rows, err := readTableRowsForHash(replayCtx, v.conn, "default", scratch)
	if err != nil {
		return InsertReplayResult{}, err
	}
	return hashReplayRows(req.TableID, req.StatementID, req.SQL, rows)
}

func (v *ClickHouseInsertReplayVerifier) loadStatementPayload(ctx context.Context, idx int, st replay.Statement) ([]byte, error) {
	if st.PayloadRef == "" {
		if st.PayloadHash != "" || st.PayloadLength != 0 {
			return nil, fmt.Errorf("statement %d: payload hash/length set without payload_ref", idx)
		}
		return nil, nil
	}
	if v.Payloads == nil {
		return nil, fmt.Errorf("statement %d: payload store is required", idx)
	}
	payload, err := v.Payloads.GetPayload(ctx, st.PayloadRef)
	if err != nil {
		return nil, fmt.Errorf("statement %d: load payload %q: %w", idx, st.PayloadRef, err)
	}
	if uint64(len(payload)) != st.PayloadLength {
		return nil, fmt.Errorf("statement %d: payload length mismatch: got %d want %d", idx, len(payload), st.PayloadLength)
	}
	if got := replay.DigestBytes(payload); got != st.PayloadHash {
		return nil, fmt.Errorf("statement %d: payload digest mismatch: got %s want %s", idx, got, st.PayloadHash)
	}
	return payload, nil
}

const defaultNativeReplayRevision = 54453

type insertPayloadEnvelope struct {
	StatementID    string
	TableID        string
	OriginalSQL    string
	UnsafeSQL      string
	ClientRevision int
	DataPackets    [][]byte
}

func (v *ClickHouseInsertReplayVerifier) replayNativePayload(ctx context.Context, req InsertReplayRequest, scratchTable string) error {
	if len(req.Payload) == 0 {
		return fmt.Errorf("native replay payload is required")
	}
	env, err := parseInsertPayloadEnvelope(req.Payload)
	if err != nil {
		return fmt.Errorf("parse native replay payload: %w", err)
	}
	if env.StatementID != req.StatementID {
		return fmt.Errorf("native replay payload statement_id mismatch: got %q want %q", env.StatementID, req.StatementID)
	}
	if env.TableID != req.TableID {
		return fmt.Errorf("native replay payload table_id mismatch: got %q want %q", env.TableID, req.TableID)
	}
	if strings.TrimSpace(env.UnsafeSQL) != strings.TrimSpace(req.SQL) {
		return fmt.Errorf("native replay payload unsafe_sql mismatch")
	}
	revision := env.ClientRevision
	if revision == 0 {
		revision = defaultNativeReplayRevision
	}
	inserted := 0
	for i, raw := range env.DataPackets {
		block, err := decodeNativeReplayBlock(raw, revision)
		if err != nil {
			return fmt.Errorf("decode native data packet %d: %w", i, err)
		}
		if block.Rows == 0 {
			continue
		}
		if err := ensureNativeReplayRowIDColumn(block.Columns); err != nil {
			return fmt.Errorf("native data packet %d: %w", i, err)
		}
		if err := v.insertNativeReplayBlock(ctx, scratchTable, block); err != nil {
			return fmt.Errorf("insert native data packet %d into replay scratch: %w", i, err)
		}
		inserted += block.Rows
	}
	if inserted == 0 {
		return fmt.Errorf("native replay payload contained no rows")
	}
	return nil
}

type nativeReplayBlock struct {
	Columns []string
	Rows    int
	Values  [][]any
}

func (v *ClickHouseInsertReplayVerifier) insertNativeReplayBlock(ctx context.Context, scratchTable string, block nativeReplayBlock) error {
	query := "INSERT INTO " + scratchTable + " (" + quoteIdentifierList(block.Columns) + ")"
	batch, err := v.conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, row := range block.Values {
		if err := batch.Append(row...); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		_ = batch.Abort()
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

func decodeNativeReplayBlock(raw []byte, revision int) (nativeReplayBlock, error) {
	r := proto.NewReader(bytes.NewReader(raw))
	code, err := r.UVarInt()
	if err != nil {
		return nativeReplayBlock{}, fmt.Errorf("packet code: %w", err)
	}
	if code != uint64(proto.ClientCodeData) {
		return nativeReplayBlock{}, fmt.Errorf("packet type %d is not ClientData", code)
	}
	if _, err := r.Str(); err != nil {
		return nativeReplayBlock{}, fmt.Errorf("block name: %w", err)
	}

	var (
		results proto.Results
		block   proto.Block
	)
	if err := block.DecodeBlock(r, revision, results.Auto()); err != nil {
		return nativeReplayBlock{}, fmt.Errorf("decode block: %w", err)
	}
	out := nativeReplayBlock{
		Rows:    block.Rows,
		Columns: make([]string, 0, len(results)),
		Values:  make([][]any, 0, block.Rows),
	}
	cols := make([]proto.ColResult, 0, len(results))
	for _, rc := range results {
		out.Columns = append(out.Columns, rc.Name)
		col := rc.Data
		if auto, ok := col.(*proto.ColAuto); ok {
			col = auto.Data
		}
		cols = append(cols, col)
	}
	for row := 0; row < block.Rows; row++ {
		values := make([]any, len(cols))
		for colIdx, col := range cols {
			value, err := nativeReplayColumnValue(col, row)
			if err != nil {
				return nativeReplayBlock{}, fmt.Errorf("column %q: %w", out.Columns[colIdx], err)
			}
			values[colIdx] = value
		}
		out.Values = append(out.Values, values)
	}
	return out, nil
}

func nativeReplayColumnValue(col proto.ColResult, row int) (any, error) {
	switch c := col.(type) {
	case *proto.ColFixedStr:
		return append([]byte(nil), c.Row(row)...), nil
	case *proto.ColFixedStr32:
		v := c.Row(row)
		return append([]byte(nil), v[:]...), nil
	case *proto.ColUInt8:
		return (*c)[row], nil
	case *proto.ColUInt16:
		return (*c)[row], nil
	case *proto.ColUInt32:
		return (*c)[row], nil
	case *proto.ColUInt64:
		return (*c)[row], nil
	case *proto.ColInt8:
		return (*c)[row], nil
	case *proto.ColInt16:
		return (*c)[row], nil
	case *proto.ColInt32:
		return (*c)[row], nil
	case *proto.ColInt64:
		return (*c)[row], nil
	case *proto.ColFloat32:
		return (*c)[row], nil
	case *proto.ColFloat64:
		return (*c)[row], nil
	case *proto.ColStr:
		return c.Row(row), nil
	case *proto.ColBool:
		return (*c)[row], nil
	case *proto.ColDate:
		return c.Row(row), nil
	case *proto.ColDateTime:
		return c.Row(row), nil
	case *proto.ColDateTime64:
		return c.Row(row), nil
	default:
		return nil, fmt.Errorf("unsupported Native replay column type %T", col)
	}
}

func ensureNativeReplayRowIDColumn(columns []string) error {
	if len(columns) == 0 {
		return fmt.Errorf("block has no columns")
	}
	for _, col := range columns {
		if strings.EqualFold(col, "_hg_row_id") {
			return nil
		}
	}
	return fmt.Errorf("block is missing _hg_row_id")
}

func quoteIdentifierList(columns []string) string {
	var b strings.Builder
	for i, col := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(QuoteIdentifier(col))
	}
	return b.String()
}

func parseInsertPayloadEnvelope(payload []byte) (insertPayloadEnvelope, error) {
	p := payloadEnvelopeParser{rest: payload}
	line, err := p.readLine()
	if err != nil {
		return insertPayloadEnvelope{}, err
	}
	if line != "# housegate.storage_integrity.insert.v1" {
		return insertPayloadEnvelope{}, fmt.Errorf("unsupported payload envelope %q", line)
	}
	var out insertPayloadEnvelope
	if out.StatementID, err = p.readHeader("statement_id"); err != nil {
		return insertPayloadEnvelope{}, err
	}
	if out.TableID, err = p.readHeader("table_id"); err != nil {
		return insertPayloadEnvelope{}, err
	}
	next, err := p.peekHeaderName()
	if err != nil {
		return insertPayloadEnvelope{}, err
	}
	if next == "client_revision" {
		value, err := p.readHeader("client_revision")
		if err != nil {
			return insertPayloadEnvelope{}, err
		}
		rev, err := strconv.Atoi(value)
		if err != nil || rev < 0 {
			return insertPayloadEnvelope{}, fmt.Errorf("invalid client_revision %q", value)
		}
		out.ClientRevision = rev
	}
	out.OriginalSQL, err = p.readSizedText("original_sql_length")
	if err != nil {
		return insertPayloadEnvelope{}, err
	}
	out.UnsafeSQL, err = p.readSizedText("unsafe_sql_length")
	if err != nil {
		return insertPayloadEnvelope{}, err
	}
	countText, err := p.readHeader("data_packet_count")
	if err != nil {
		return insertPayloadEnvelope{}, err
	}
	count, err := strconv.Atoi(countText)
	if err != nil || count < 0 {
		return insertPayloadEnvelope{}, fmt.Errorf("invalid data_packet_count %q", countText)
	}
	out.DataPackets = make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		packet, err := p.readSizedBytes(fmt.Sprintf("data_packet_%d_length", i))
		if err != nil {
			return insertPayloadEnvelope{}, err
		}
		out.DataPackets = append(out.DataPackets, packet)
	}
	if len(bytes.TrimSpace(p.rest)) != 0 {
		return insertPayloadEnvelope{}, fmt.Errorf("unexpected trailing payload bytes")
	}
	return out, nil
}

type payloadEnvelopeParser struct {
	rest []byte
}

func (p *payloadEnvelopeParser) readLine() (string, error) {
	idx := bytes.IndexByte(p.rest, '\n')
	if idx < 0 {
		return "", fmt.Errorf("missing newline")
	}
	line := string(bytes.TrimSuffix(p.rest[:idx], []byte{'\r'}))
	p.rest = p.rest[idx+1:]
	return line, nil
}

func (p *payloadEnvelopeParser) peekHeaderName() (string, error) {
	idx := bytes.IndexByte(p.rest, '\n')
	if idx < 0 {
		return "", fmt.Errorf("missing header line")
	}
	line := string(p.rest[:idx])
	name, _, ok := strings.Cut(line, ":")
	if !ok {
		return "", fmt.Errorf("malformed header line %q", line)
	}
	return strings.TrimSpace(name), nil
}

func (p *payloadEnvelopeParser) readHeader(name string) (string, error) {
	line, err := p.readLine()
	if err != nil {
		return "", fmt.Errorf("%s: %w", name, err)
	}
	got, value, ok := strings.Cut(line, ":")
	if !ok {
		return "", fmt.Errorf("malformed header line %q", line)
	}
	if strings.TrimSpace(got) != name {
		return "", fmt.Errorf("unexpected header %q, want %q", strings.TrimSpace(got), name)
	}
	return strings.TrimSpace(value), nil
}

func (p *payloadEnvelopeParser) readSizedText(header string) (string, error) {
	b, err := p.readSizedBytes(header)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *payloadEnvelopeParser) readSizedBytes(header string) ([]byte, error) {
	lengthText, err := p.readHeader(header)
	if err != nil {
		return nil, err
	}
	length, err := strconv.Atoi(lengthText)
	if err != nil || length < 0 {
		return nil, fmt.Errorf("invalid %s %q", header, lengthText)
	}
	if len(p.rest) < length {
		return nil, fmt.Errorf("%s wants %d bytes, only %d remain", header, length, len(p.rest))
	}
	out := append([]byte(nil), p.rest[:length]...)
	p.rest = p.rest[length:]
	if len(p.rest) == 0 || p.rest[0] != '\n' {
		return nil, fmt.Errorf("%s body missing trailing newline", header)
	}
	p.rest = p.rest[1:]
	return out, nil
}

type replayHashRow struct {
	RowID  string
	Values []string
}

func readTableRowsForHash(ctx context.Context, conn clickhouseReplayConn, databaseName, tableName string) ([]replayHashRow, error) {
	columns, err := readHashColumns(ctx, conn, databaseName, tableName)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s.%s has no user columns", databaseName, tableName)
	}
	var b strings.Builder
	b.WriteString("SELECT lower(hex(_hg_row_id))")
	for _, col := range columns {
		b.WriteString(", toString(")
		b.WriteString(QuoteIdentifier(col))
		b.WriteByte(')')
	}
	b.WriteString(" FROM ")
	b.WriteString(QuoteTable(databaseName, tableName))
	b.WriteString(" ORDER BY _hg_row_id")

	rows, err := conn.Query(ctx, b.String())
	if err != nil {
		return nil, fmt.Errorf("read replay rows: %w", err)
	}
	defer rows.Close()

	out := make([]replayHashRow, 0)
	for rows.Next() {
		var rowID string
		values := make([]string, len(columns))
		dests := make([]any, len(columns)+1)
		dests[0] = &rowID
		for i := range values {
			dests[i+1] = &values[i]
		}
		if err := rows.Scan(dests...); err != nil {
			return nil, fmt.Errorf("scan replay row: %w", err)
		}
		if rowID == "" {
			return nil, fmt.Errorf("replay row has empty _hg_row_id")
		}
		out = append(out, replayHashRow{RowID: rowID, Values: values})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read replay rows: %w", err)
	}
	return out, nil
}

func readHashColumns(ctx context.Context, conn clickhouseReplayConn, databaseName, tableName string) ([]string, error) {
	rows, err := conn.Query(ctx, "SELECT name FROM system.columns WHERE database = ? AND table = ? ORDER BY position", databaseName, tableName)
	if err != nil {
		return nil, fmt.Errorf("read table columns: %w", err)
	}
	defer rows.Close()

	var out []string
	hasRowID := false
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan table column: %w", err)
		}
		if strings.EqualFold(name, "_hg_row_id") {
			hasRowID = true
			continue
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read table columns: %w", err)
	}
	if !hasRowID {
		return nil, fmt.Errorf("table %s.%s is missing _hg_row_id", databaseName, tableName)
	}
	return out, nil
}

func hashReplayRows(tableID, statementID, sql string, rows []replayHashRow) (InsertReplayResult, error) {
	rowHashes := make([]string, 0, len(rows))
	for _, row := range rows {
		h, err := hashDomain("housegate-insert-replay-row-v1", row)
		if err != nil {
			return InsertReplayResult{}, err
		}
		rowHashes = append(rowHashes, h)
	}
	rowsHash, err := hashDomain("housegate-insert-replay-rows-v1", rowHashes)
	if err != nil {
		return InsertReplayResult{}, err
	}
	stateRoot, err := hashDomain("housegate-insert-replay-state-v1", struct {
		TableID     string   `json:"table_id"`
		StatementID string   `json:"statement_id"`
		SQLHash     string   `json:"sql_hash"`
		Rows        []string `json:"rows"`
	}{
		TableID:     tableID,
		StatementID: statementID,
		SQLHash:     replay.DigestString(sql),
		Rows:        rowHashes,
	})
	if err != nil {
		return InsertReplayResult{}, err
	}
	return InsertReplayResult{StateRoot: stateRoot, RowsHash: rowsHash, RowCount: uint64(len(rows))}, nil
}

func insertTargetSpan(sql string) (target string, start int, end int, err error) {
	i := skipSQLSpaces(sql, 0)
	if !consumeSQLKeyword(sql, &i, "insert") || !consumeSQLKeyword(sql, &i, "into") {
		return "", 0, 0, fmt.Errorf("insert replay only supports INSERT INTO statements")
	}
	if consumeSQLKeyword(sql, &i, "table") {
		i = skipSQLSpaces(sql, i)
	}
	start = skipSQLSpaces(sql, i)
	end, ok := scanQualifiedIdentifierForReplay(sql, start)
	if !ok {
		return "", 0, 0, fmt.Errorf("insert replay target table not found")
	}
	return strings.TrimSpace(sql[start:end]), start, end, nil
}

func containsFormatNative(sql string) bool {
	lower := strings.ToLower(sql)
	return strings.Contains(lower, " format native")
}

func consumeSQLKeyword(sql string, i *int, keyword string) bool {
	j := skipSQLSpaces(sql, *i)
	end := j + len(keyword)
	if end > len(sql) || !strings.EqualFold(sql[j:end], keyword) {
		return false
	}
	if !isSQLBoundary(sql, j-1) || !isSQLBoundary(sql, end) {
		return false
	}
	*i = end
	return true
}

func skipSQLSpaces(sql string, i int) int {
	for i < len(sql) && unicode.IsSpace(rune(sql[i])) {
		i++
	}
	return i
}

func scanQualifiedIdentifierForReplay(sql string, i int) (int, bool) {
	end, ok := scanIdentifierForReplay(sql, i)
	if !ok {
		return 0, false
	}
	for {
		j := skipSQLSpaces(sql, end)
		if j >= len(sql) || sql[j] != '.' {
			return end, true
		}
		j = skipSQLSpaces(sql, j+1)
		next, ok := scanIdentifierForReplay(sql, j)
		if !ok {
			return end, true
		}
		end = next
	}
}

func scanIdentifierForReplay(sql string, i int) (int, bool) {
	i = skipSQLSpaces(sql, i)
	if i >= len(sql) {
		return 0, false
	}
	if sql[i] == '`' || sql[i] == '"' {
		quote := sql[i]
		i++
		for i < len(sql) {
			if sql[i] == quote {
				if i+1 < len(sql) && sql[i+1] == quote {
					i += 2
					continue
				}
				return i + 1, true
			}
			i++
		}
		return 0, false
	}
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

func isSQLBoundary(sql string, idx int) bool {
	if idx < 0 || idx >= len(sql) {
		return true
	}
	r := rune(sql[idx])
	return !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_')
}
