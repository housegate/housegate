package storageintegrity

import (
	"context"
	"fmt"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"housegate/housegate/pkg/replay"
)

type UnsafeReplicaDigestReader interface {
	ReadUnsafeDigest(ctx context.Context, replica UnsafeReplica, unsafeTable string) (UnsafeReplicaDigest, error)
}

type UnsafeReplicaHashVerifier struct {
	Reader         UnsafeReplicaDigestReader
	ReplicaTimeout time.Duration
}

func (v UnsafeReplicaHashVerifier) VerifyUnsafe(ctx context.Context, task UnsafeValidationTask) (UnsafeValidationResult, error) {
	if v.Reader == nil {
		return UnsafeValidationResult{}, fmt.Errorf("unsafe replica digest reader is required")
	}
	if task.ValidationID == "" {
		return UnsafeValidationResult{}, fmt.Errorf("validation_id is required")
	}
	if task.StatementID == "" {
		return UnsafeValidationResult{}, fmt.Errorf("statement_id is required")
	}
	if task.TableID == "" {
		return UnsafeValidationResult{}, fmt.Errorf("table_id is required")
	}
	if task.UnsafeTable == "" {
		return UnsafeValidationResult{}, fmt.Errorf("unsafe_table is required")
	}
	if len(task.Replicas) < 2 {
		return UnsafeValidationResult{}, fmt.Errorf("at least two unsafe replicas are required")
	}

	var expected UnsafeReplicaDigest
	results := make([]UnsafeReplicaDigest, 0, len(task.Replicas))
	replicaTimeout := v.ReplicaTimeout
	if replicaTimeout <= 0 {
		replicaTimeout = defaultUnsafeReplicaTimeout
	}
	for i, replica := range task.Replicas {
		if replica.ReplicaID == "" {
			return UnsafeValidationResult{}, fmt.Errorf("replica %d: replica_id is required", i)
		}
		readCtx, cancel := context.WithTimeout(ctx, replicaTimeout)
		digest, err := v.Reader.ReadUnsafeDigest(readCtx, replica, task.UnsafeTable)
		cancel()
		if err != nil {
			return UnsafeValidationResult{}, fmt.Errorf("read unsafe digest from replica %q: %w", replica.ReplicaID, err)
		}
		if digest.ReplicaID == "" {
			digest.ReplicaID = replica.ReplicaID
		}
		if digest.RowsHash == "" {
			return UnsafeValidationResult{}, fmt.Errorf("replica %q returned empty rows hash", digest.ReplicaID)
		}
		if i == 0 {
			expected = digest
		} else if digest.RowCount != expected.RowCount || digest.RowsHash != expected.RowsHash {
			return UnsafeValidationResult{}, fmt.Errorf(
				"unsafe replica digest mismatch: replica %q count/hash=%d/%s replica %q count/hash=%d/%s",
				expected.ReplicaID, expected.RowCount, expected.RowsHash,
				digest.ReplicaID, digest.RowCount, digest.RowsHash,
			)
		}
		results = append(results, digest)
	}
	return UnsafeValidationResult{
		ValidationID: task.ValidationID,
		StatementID:  task.StatementID,
		TableID:      task.TableID,
		UnsafeTable:  task.UnsafeTable,
		RowCount:     expected.RowCount,
		RowsHash:     expected.RowsHash,
		Replicas:     results,
	}, nil
}

const defaultUnsafeReplicaTimeout = 30 * time.Second

type ClickHouseUnsafeDigestReader struct {
	Username string
	Password string
	Database string
}

func (r ClickHouseUnsafeDigestReader) ReadUnsafeDigest(ctx context.Context, replica UnsafeReplica, unsafeTable string) (UnsafeReplicaDigest, error) {
	if strings.TrimSpace(replica.Addr) == "" {
		return UnsafeReplicaDigest{}, fmt.Errorf("replica addr is required")
	}
	databaseName, tableName, err := splitClickHouseQualifiedTableName(unsafeTable)
	if err != nil {
		return UnsafeReplicaDigest{}, err
	}
	username := r.Username
	if username == "" {
		username = "default"
	}
	database := r.Database
	if database == "" {
		database = "default"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{replica.Addr},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: r.Password,
		},
		Protocol: clickhouse.Native,
	})
	if err != nil {
		return UnsafeReplicaDigest{}, fmt.Errorf("open clickhouse connection: %w", err)
	}
	defer conn.Close()

	var rowCount uint64
	var rowsHash string
	if err := queryUnsafePartsDigest(ctx, conn, databaseName, tableName, &rowCount, &rowsHash); err != nil {
		return UnsafeReplicaDigest{}, err
	}
	return UnsafeReplicaDigest{
		ReplicaID: replica.ReplicaID,
		RowCount:  rowCount,
		RowsHash:  "0x" + strings.TrimPrefix(rowsHash, "0x"),
	}, nil
}

type unsafeDigestConn interface {
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}

func queryUnsafePartsDigest(ctx context.Context, conn unsafeDigestConn, databaseName, tableName string, rowCount *uint64, rowsHash *string) error {
	queries := []string{
		"SELECT ifNull(sum(rows), 0), lower(hex(sipHash128(groupArray(toString(tuple(name, rows, hash_of_all_files, hash_of_uncompressed_files)))))) FROM (SELECT name, rows, hash_of_all_files, hash_of_uncompressed_files FROM system.parts WHERE active AND database = ? AND table = ? ORDER BY name)",
		"SELECT ifNull(sum(rows), 0), lower(hex(sipHash128(groupArray(toString(tuple(name, rows, bytes_on_disk)))))) FROM (SELECT name, rows, bytes_on_disk FROM system.parts WHERE active AND database = ? AND table = ? ORDER BY name)",
	}
	var lastErr error
	for _, query := range queries {
		rows, err := conn.Query(ctx, query, databaseName, tableName)
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				lastErr = err
			} else {
				lastErr = fmt.Errorf("query returned no rows")
			}
			if ctx.Err() != nil {
				break
			}
			continue
		}
		if err := rows.Scan(rowCount, rowsHash); err != nil {
			lastErr = err
			if ctx.Err() != nil {
				break
			}
			continue
		}
		return nil
	}
	return fmt.Errorf("query unsafe parts digest: %w", lastErr)
}

func splitClickHouseQualifiedTableName(name string) (string, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", "", fmt.Errorf("unsafe_table is required")
	}
	parts := make([]string, 0, 2)
	var current strings.Builder
	inBacktick := false
	for _, r := range name {
		switch r {
		case '`':
			inBacktick = !inBacktick
			current.WriteRune(r)
		case '.':
			if inBacktick {
				current.WriteRune(r)
				continue
			}
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if inBacktick {
		return "", "", fmt.Errorf("unsafe_table %q has unterminated quoted identifier", name)
	}
	parts = append(parts, current.String())
	if len(parts) != 2 {
		return "", "", fmt.Errorf("unsafe_table %q must be qualified as database.table", name)
	}
	databaseName := cleanClickHouseIdentifier(parts[0])
	tableName := cleanClickHouseIdentifier(parts[1])
	if databaseName == "" || tableName == "" {
		return "", "", fmt.Errorf("unsafe_table %q must include non-empty database and table", name)
	}
	return databaseName, tableName, nil
}

func cleanClickHouseIdentifier(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "`") && strings.HasSuffix(s, "`") && len(s) >= 2 {
		s = strings.TrimPrefix(strings.TrimSuffix(s, "`"), "`")
		s = strings.ReplaceAll(s, "``", "`")
	}
	return s
}

type MockUnsafeValidationVerifier struct {
	ReplicaID string
}

func (v MockUnsafeValidationVerifier) VerifyUnsafe(ctx context.Context, task UnsafeValidationTask) (UnsafeValidationResult, error) {
	if err := ctx.Err(); err != nil {
		return UnsafeValidationResult{}, err
	}
	replicaID := v.ReplicaID
	if replicaID == "" {
		replicaID = "mock-unsafe"
	}
	rowsHash := replayDigestUnsafe(task)
	return UnsafeValidationResult{
		ValidationID: task.ValidationID,
		StatementID:  task.StatementID,
		TableID:      task.TableID,
		UnsafeTable:  task.UnsafeTable,
		RowsHash:     rowsHash,
		Replicas: []UnsafeReplicaDigest{{
			ReplicaID: replicaID,
			RowsHash:  rowsHash,
		}},
	}, nil
}

func replayDigestUnsafe(task UnsafeValidationTask) string {
	return replay.DigestString("unsafe-validation\x00" + task.StatementID + "\x00" + task.UnsafeTable)
}
