// Package sitable is the storage-integrity table-state port (spec
// 2026-09-24 §5). A host hands HouseGate versioned, immutable snapshots of
// every table's status; HouseGate takes exactly one snapshot per query and
// every stage of that query reads it. The package holds only types and the
// static implementation and imports no proxy package.
package sitable

import (
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Status is a table's storage-integrity status as the host judges it.
type Status uint8

const (
	// Ordinary is not governed: another indexer, Legacy, or registry not enabled.
	Ordinary Status = iota
	// Pending includes default deny: on the SI indexer and not recorded.
	Pending
	// Refused carries the arbiter's refused_code and refused_reason.
	Refused
	// Active is served from hg_safe / hg_unsafe and written through the signed lane.
	Active
	// Gone means the newest incarnation is Retiring or Purging.
	Gone
)

// String returns the lowercase wire name used by the JSON-RPC contract.
func (s Status) String() string {
	switch s {
	case Ordinary:
		return "ordinary"
	case Pending:
		return "pending"
	case Refused:
		return "refused"
	case Active:
		return "active"
	case Gone:
		return "gone"
	default:
		return "unknown"
	}
}

// ParseStatus maps a JSON-RPC status name back to a Status.
func ParseStatus(name string) (Status, bool) {
	switch name {
	case "ordinary":
		return Ordinary, true
	case "pending":
		return Pending, true
	case "refused":
		return Refused, true
	case "active":
		return Active, true
	case "gone":
		return Gone, true
	default:
		return Ordinary, false
	}
}

// Table is one table's status in a snapshot.
type Table struct {
	ID            string // "<database>.<table>", logical
	Status        Status
	RefusedCode   string // Refused only; the arbiter's refused_code vocabulary
	RefusedReason string
	Schema        payloadexec.TableSchema // set for Active and Gone
	SchemaHash    string
}

// Snapshot is immutable; every method is in-memory and performs no I/O.
type Snapshot interface {
	Version() uint64
	Lookup(database, table string) Table // answers for any table
	Active() []Table                     // sorted by ID
	Schema(id string) (Table, bool)      // Active or Gone
}

// TableState hands out snapshots. Changed returns a channel that is closed at
// the next version change after the call; callers call Changed again after
// every wake.
type TableState interface {
	Current() Snapshot
	Changed() <-chan struct{}
}

// Protocol-owned storage-integrity databases (Spec C D2 naming freeze). They
// equal config.StorageIntegritySafeDatabase / UnsafeDatabase /
// PromoteDatabase; this leaf package cannot import pkg/config (which imports
// pkg/rewriter), and a pkg/config test pins the equality.
const (
	SafeDatabase    = "hg_safe"
	UnsafeDatabase  = "hg_unsafe"
	PromoteDatabase = "hg_promote"
)

// ReservedDatabases returns the protocol-owned databases HouseGate sends as
// StorageIntegrityArgs.reserved_databases on every enabled request, so the
// engines protect them even while no table is Active.
func ReservedDatabases() []string {
	return []string{SafeDatabase, UnsafeDatabase, PromoteDatabase}
}

// PhysicalTable maps a logical table id to its hg_safe / hg_unsafe table name
// (the same rule as storageintegrity.PhysicalTableName).
func PhysicalTable(id string) string {
	return strings.ReplaceAll(id, ".", "__")
}

// TableID joins a logical database and table into the snapshot id.
func TableID(database, table string) string {
	return database + "." + table
}

type tableKey struct {
	database string
	table    string
}

type snapshot struct {
	version  uint64
	fallback Status
	byKey    map[tableKey]Table
	byID     map[string]Table
	active   []Table
}

// NewSnapshot builds an immutable snapshot. Every table not listed answers
// fallback. The database/table split of each entry is taken from its ID at the
// first dot, so IDs must be "<database>.<table>" with a dot-free database.
func NewSnapshot(version uint64, fallback Status, tables []Table) Snapshot {
	s := &snapshot{
		version:  version,
		fallback: fallback,
		byKey:    make(map[tableKey]Table, len(tables)),
		byID:     make(map[string]Table, len(tables)),
	}
	for _, t := range tables {
		database, table, _ := strings.Cut(t.ID, ".")
		s.byKey[tableKey{database: database, table: table}] = t
		s.byID[t.ID] = t
		if t.Status == Active {
			s.active = append(s.active, t)
		}
	}
	sort.Slice(s.active, func(i, j int) bool { return s.active[i].ID < s.active[j].ID })
	return s
}

func (s *snapshot) Version() uint64 { return s.version }

func (s *snapshot) Lookup(database, table string) Table {
	if t, ok := s.byKey[tableKey{database: database, table: table}]; ok {
		return t
	}
	return Table{ID: TableID(database, table), Status: s.fallback}
}

func (s *snapshot) Active() []Table {
	return append([]Table(nil), s.active...)
}

func (s *snapshot) Schema(id string) (Table, bool) {
	t, ok := s.byID[id]
	if !ok || (t.Status != Active && t.Status != Gone) {
		return Table{}, false
	}
	return t, true
}

// neverChanged is shared by every state whose version never moves.
var neverChanged = make(chan struct{})
