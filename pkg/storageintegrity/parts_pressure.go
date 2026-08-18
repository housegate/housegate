package storageintegrity

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	DefaultSoftPartsPerPartition = 2400
	DefaultHardPartsPerPartition = 2950
)

// ErrBackpressure is the sentinel every part-pressure refusal unwraps to.
var ErrBackpressure = errors.New("storage_integrity: back-pressure")

// BackpressureError describes the refused physical table and partition.
type BackpressureError struct {
	Database  string
	Table     string
	Partition string
	Parts     int
	Limit     int
	Kind      string
}

func (e *BackpressureError) Error() string {
	if e.Kind == "unavailable" {
		return fmt.Sprintf("storage_integrity: back-pressure: part inventory unavailable for %s.%s; retry later", e.Database, e.Table)
	}
	return fmt.Sprintf("storage_integrity: back-pressure: %s.%s partition %s has %d active parts (%s limit %d); retry later",
		e.Database, e.Table, e.Partition, e.Parts, e.Kind, e.Limit)
}

func (e *BackpressureError) Unwrap() error { return ErrBackpressure }

// LogicalPartitionID maps system.parts.partition text to the row-side
// p_<value> / all convention. partition_id is deliberately not used.
func LogicalPartitionID(partitionText string) string {
	if partitionText == "tuple()" {
		return "all"
	}
	return "p_" + partitionText
}

type PartsKey struct {
	Database  string
	Table     string
	Partition string
}

type PartsSnapshot map[PartsKey]int

type PartsPressureConfig struct {
	UnsafeDatabase        string
	SafeDatabase          string
	SoftPartsPerPartition int
	HardPartsPerPartition int
}

// PartsPressureGuard caches active-part counts and answers ingress admission
// from the last successful snapshot.
type PartsPressureGuard struct {
	conn MergeConn
	cfg  PartsPressureConfig

	mu       sync.RWMutex
	snapshot PartsSnapshot
	takenAt  time.Time
	haveSnap bool

	invalidated chan struct{}
}

func NewPartsPressureGuard(conn MergeConn, cfg PartsPressureConfig) *PartsPressureGuard {
	if cfg.SoftPartsPerPartition <= 0 {
		cfg.SoftPartsPerPartition = DefaultSoftPartsPerPartition
	}
	if cfg.HardPartsPerPartition <= 0 {
		cfg.HardPartsPerPartition = DefaultHardPartsPerPartition
	}
	return &PartsPressureGuard{conn: conn, cfg: cfg, invalidated: make(chan struct{}, 1)}
}

// BuildSnapshotQuery groups active parts by partition text, not partition_id.
func (g *PartsPressureGuard) BuildSnapshotQuery() string {
	databases := []string{quoteMergeString(g.cfg.UnsafeDatabase)}
	if g.cfg.SafeDatabase != "" {
		databases = append(databases, quoteMergeString(g.cfg.SafeDatabase))
	}
	return "SELECT database, table, partition, count() FROM system.parts WHERE database IN (" +
		strings.Join(databases, ", ") + ") AND active GROUP BY database, table, partition"
}

// Refresh replaces the cached snapshot only after a complete successful read.
func (g *PartsPressureGuard) Refresh(ctx context.Context) (PartsSnapshot, error) {
	rows, err := g.conn.Query(ctx, g.BuildSnapshotQuery())
	if err != nil {
		return nil, fmt.Errorf("storage_integrity: parts snapshot query failed: %w", err)
	}
	defer rows.Close()
	snapshot := PartsSnapshot{}
	for rows.Next() {
		var database, table, partition string
		var number uint64
		if err := rows.Scan(&database, &table, &partition, &number); err != nil {
			return nil, fmt.Errorf("storage_integrity: scan parts snapshot: %w", err)
		}
		snapshot[PartsKey{Database: database, Table: table, Partition: LogicalPartitionID(partition)}] = int(number)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("storage_integrity: read parts snapshot: %w", err)
	}
	g.mu.Lock()
	g.snapshot = snapshot
	g.takenAt = time.Now()
	g.haveSnap = true
	g.mu.Unlock()
	return g.copySnapshot(), nil
}

func (g *PartsPressureGuard) Snapshot() (PartsSnapshot, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.haveSnap {
		return nil, false
	}
	out := make(PartsSnapshot, len(g.snapshot))
	for key, number := range g.snapshot {
		out[key] = number
	}
	return out, true
}

func (g *PartsPressureGuard) copySnapshot() PartsSnapshot {
	snapshot, _ := g.Snapshot()
	return snapshot
}

// Allow admits below the soft limit and refuses at soft/hard or without a
// snapshot. An unseen partition has zero active parts and is admitted.
func (g *PartsPressureGuard) Allow(table, partitionID string) error {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if !g.haveSnap {
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Kind: "unavailable"}
	}
	number := g.snapshot[PartsKey{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID}]
	switch {
	case number >= g.cfg.HardPartsPerPartition:
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Parts: number, Limit: g.cfg.HardPartsPerPartition, Kind: "hard"}
	case number >= g.cfg.SoftPartsPerPartition:
		return &BackpressureError{Database: g.cfg.UnsafeDatabase, Table: table, Partition: partitionID, Parts: number, Limit: g.cfg.SoftPartsPerPartition, Kind: "soft"}
	default:
		return nil
	}
}

func (g *PartsPressureGuard) Invalidate() {
	select {
	case g.invalidated <- struct{}{}:
	default:
	}
}

func (g *PartsPressureGuard) Invalidated() <-chan struct{} { return g.invalidated }
