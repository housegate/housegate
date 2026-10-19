// Package sitable is the storage-integrity table-state port (spec
// 2026-09-24 §5). A host hands HouseGate versioned, immutable snapshots of
// every table's status; HouseGate takes exactly one snapshot per query and
// every stage of that query reads it. The package holds only types and the
// static implementation and imports no proxy package.
package sitable

import (
	"sort"
	"strconv"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Status is a table's storage-integrity status as the host judges it.
type Status uint8

const (
	// Ordinary is not governed on this host: a table another indexer hosts —
	// including one another SI indexer of the network owns (spec 2026-10-10
	// §9, §10) — a Legacy table, or any table while the registry is not
	// enabled. Its reads and writes are plain ClickHouse here; the owning
	// host, if any, governs it.
	Ordinary Status = iota
	// Pending includes default deny: a table of a database this host's
	// enrolled SI indexer hosts that the registry has not recorded. A host
	// also reports Pending for an Active incarnation it holds back: not yet
	// ready on its SNode, or, in the owner audit's repair mode, while the
	// owner's hg_safe audit is unclean (spec 2026-10-14 §14). Such a held
	// entry may keep the table's Schema and SchemaHash (Schema.TableID set),
	// so journal recovery and cleanup still resolve it; reads and writes are
	// still refused retryably, and the signed lane still admits only Active
	// tables.
	Pending
	// Refused carries the arbiter's refused_code and refused_reason, or the
	// host-synthesized code "indexer_quarantined" for each table the host would
	// otherwise report Active while its SI indexer is quarantined, and after a
	// release until its rejected parts are cleaned (spec 2026-10-10 §7.4, §7.5,
	// §10). A Refused table is refused for reads as well as writes. Such a
	// quarantined entry keeps the table's Schema and SchemaHash, so journal
	// recovery and cleanup still resolve it; an admission-refused table carries
	// none.
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
	RefusedCode   string // Refused only; the arbiter's refused_code vocabulary or the host's "indexer_quarantined"
	RefusedReason string
	Schema        payloadexec.TableSchema // set for Active and Gone, for a Refused table whose indexer is quarantined, and for a Pending table its host holds back
	SchemaHash    string
}

// cloneSchema returns a TableSchema whose Columns slice is a fresh copy, so
// the result shares no backing array with the argument. TableSchema currently
// has no other slice or map field.
func cloneSchema(schema payloadexec.TableSchema) payloadexec.TableSchema {
	if schema.Columns != nil {
		schema.Columns = append([]lthash.Column(nil), schema.Columns...)
	}
	return schema
}

// cloneTable returns a Table whose Schema owns its own Columns backing
// array, independent of both the argument and any other clone taken from the
// same source. A snapshot must not reach data it does not own (in), and a
// caller must not be able to reach into a snapshot's internals through a
// value the snapshot handed back (out).
func cloneTable(t Table) Table {
	t.Schema = cloneSchema(t.Schema)
	return t
}

// Snapshot is immutable; every method is in-memory and performs no I/O.
//
// Schema is how journal recovery and cleanup resolve a table's schema. It
// answers an Active or Gone table, a Refused table that carries a schema (a
// quarantined indexer's, code "indexer_quarantined") and a Pending table that
// carries one (an Active incarnation its host holds back, spec 2026-10-14
// §14); NewSnapshot, Static and Fake do. It misses for every other table,
// including a Refused or Pending one without a schema (an admission refusal,
// default deny) and an Ordinary one.
type Snapshot interface {
	Version() uint64
	Lookup(database, table string) Table // answers for any table
	Active() []Table                     // sorted by ID
	Schema(id string) (Table, bool)      // Active or Gone, or Refused or Pending with a schema
}

// TableState hands out snapshots. Changed returns a channel that is closed at
// the next version change after the call; callers call Changed again after
// every wake.
type TableState interface {
	Current() Snapshot
	Changed() <-chan struct{}
}

// Protocol-owned storage-integrity databases (Spec C D2 naming freeze). The
// first three are the SNode's and equal config.StorageIntegritySafeDatabase /
// UnsafeDatabase / PromoteDatabase; this leaf package cannot import
// pkg/config (which imports pkg/rewriter), and a pkg/config test pins the
// equality. The last four are the stage-3 member databases (spec 2026-10-14
// §8.1, §14), owned by the si-member process rather than the SNode: a
// member's ReplicatedMergeTree replicas of an owner's unsafe tables, its
// verified copies, its install and replay scratch tables, and a name reserved
// for a later transfer phase. HouseGate never creates any of them; it only
// keeps every session from addressing them.
const (
	SafeDatabase         = "hg_safe"
	UnsafeDatabase       = "hg_unsafe"
	PromoteDatabase      = "hg_promote"
	MemberUnsafeDatabase = "hg_member_unsafe"
	ReplicaDatabase      = "hg_replica"
	ReplicaStageDatabase = "hg_rstage"
	TransferDatabase     = "hg_xfer"
)

// ReservedDatabases returns the protocol-owned databases in this fixed order,
// the SNode's three first: HouseGate sends them as
// StorageIntegrityArgs.reserved_databases on every enabled request (so the
// engines protect them even while no table is Active) and inside
// protected_databases on every request, and tablerefguard, sireserved, the
// exception scrubber and the ingress DROP TABLE check refuse or redact them.
// Every call returns a fresh slice.
func ReservedDatabases() []string {
	return []string{
		SafeDatabase, UnsafeDatabase, PromoteDatabase,
		MemberUnsafeDatabase, ReplicaDatabase, ReplicaStageDatabase, TransferDatabase,
	}
}

// PhysicalTable maps a logical table id to its hg_safe / hg_unsafe table name
// (the same rule as storageintegrity.PhysicalTableName).
func PhysicalTable(id string) string {
	return strings.ReplaceAll(id, ".", "__")
}

// CopyTable is the name, in ReplicaDatabase (hg_replica), of a stage-3
// member's verified copy of incarnation incarnationSeq of table id (spec
// 2026-10-14 §8.3, §20): PhysicalTable(id) + "__i" + the decimal seq. Copies
// are incarnation-scoped, so a dropped and recreated name never adopts an old
// copy, and the digits after the last "__i" give the seq back. HouseGate
// creates no copy; it only names it.
func CopyTable(id string, incarnationSeq uint64) string {
	return PhysicalTable(id) + "__i" + strconv.FormatUint(incarnationSeq, 10)
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
		// Clone on the way in: the snapshot must not keep aliasing the
		// caller's Columns backing array after this constructor returns.
		ct := cloneTable(t)
		database, table, _ := strings.Cut(ct.ID, ".")
		s.byKey[tableKey{database: database, table: table}] = ct
		s.byID[ct.ID] = ct
		if ct.Status == Active {
			s.active = append(s.active, ct)
		}
	}
	sort.Slice(s.active, func(i, j int) bool { return s.active[i].ID < s.active[j].ID })
	return s
}

func (s *snapshot) Version() uint64 { return s.version }

func (s *snapshot) Lookup(database, table string) Table {
	if t, ok := s.byKey[tableKey{database: database, table: table}]; ok {
		// Clone on the way out: the caller must not be able to reach the
		// snapshot's stored Columns array through the returned value.
		return cloneTable(t)
	}
	return Table{ID: TableID(database, table), Status: s.fallback}
}

func (s *snapshot) Active() []Table {
	out := make([]Table, len(s.active))
	for i, t := range s.active {
		out[i] = cloneTable(t)
	}
	return out
}

// Schema answers an Active or Gone table, and a Refused or Pending table that
// carries a schema: a host keeps the schema of each table it reports Refused
// because its SI indexer is quarantined (code "indexer_quarantined", spec
// 2026-10-10 §10) and of each Active incarnation it reports Pending while it
// holds it back (not ready on its SNode, or an unclean owner hg_safe audit in
// repair mode, spec 2026-10-14 §14), so journal recovery and cleanup still
// resolve it. A Refused table without a schema (the arbiter's admission
// refusal), a Pending one without a schema (default deny), an Ordinary one and
// an unlisted one all miss.
func (s *snapshot) Schema(id string) (Table, bool) {
	t, ok := s.byID[id]
	if !ok {
		return Table{}, false
	}
	switch t.Status {
	case Active, Gone:
	case Refused, Pending:
		if t.Schema.TableID == "" {
			return Table{}, false
		}
	default:
		return Table{}, false
	}
	return cloneTable(t), true
}

// neverChanged is shared by every state whose version never moves.
var neverChanged = make(chan struct{})
