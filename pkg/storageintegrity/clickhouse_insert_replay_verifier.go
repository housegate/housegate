package storageintegrity

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"unicode"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"housegate/housegate/pkg/replay"
)

type ClickHouseInsertReplayVerifier struct {
	conn    clickhouseReplayConn
	Signer  replay.Signer
	Timeout time.Duration
	counter atomic.Uint64
}

type clickhouseReplayConn interface {
	Exec(ctx context.Context, query string, args ...any) error
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
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
	result, err := v.ComputeInsertReplay(ctx, InsertReplayRequest{
		TableID:     st.TargetTableID,
		StatementID: st.StatementID,
		SQL:         st.SQL,
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
	if containsFormatNative(req.SQL) {
		return InsertReplayResult{}, fmt.Errorf("insert replay for FORMAT Native payloads is not implemented")
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
	replaySQL := req.SQL[:start] + scratchTable + req.SQL[end:]
	if err := v.conn.Exec(replayCtx, replaySQL); err != nil {
		return InsertReplayResult{}, fmt.Errorf("execute replay insert: %w", err)
	}
	rows, err := readTableRowsForHash(replayCtx, v.conn, "default", scratch)
	if err != nil {
		return InsertReplayResult{}, err
	}
	return hashReplayRows(req.TableID, req.StatementID, req.SQL, rows)
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
