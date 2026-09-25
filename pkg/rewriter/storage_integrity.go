package rewriter

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	pb "github.com/housegate/rewriter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/sitable"
)

// ReadModeSettingKey is the per-query ClickHouse custom setting that selects
// the storage-integrity read mode (Spec G D1). Like the other SQL_x_* keys
// it is read by housegate and forwarded unchanged (ClickHouse accepts it
// under custom_settings_prefixes = SQL_).
const ReadModeSettingKey = "SQL_x_read_mode"

// ReadMode selects which physical surface an SI table read resolves to.
type ReadMode string

const (
	ReadModeSafe         ReadMode = "safe"          // hg_safe only
	ReadModeUnsafeLatest ReadMode = "unsafe_latest" // hg_safe ∪ hg_unsafe minus promoted-not-yet-cleaned parts
)

// DefaultReservedRowIDColumn is the protocol row-identity column hidden
// from the logical surface (Spec G D3).
const DefaultReservedRowIDColumn = "_hg_row_id"

// ParseReadMode parses a setting/config value. Surrounding whitespace and
// the '…' / "…" quoting clickhouse-go's CustomSetting adds are stripped.
// Empty is an error -- callers decide their own default.
func ParseReadMode(raw string) (ReadMode, error) {
	v := strings.Trim(strings.TrimSpace(raw), "\"'")
	v = strings.TrimSpace(v)
	switch ReadMode(v) {
	case ReadModeSafe, ReadModeUnsafeLatest:
		return ReadMode(v), nil
	}
	return "", fmt.Errorf("%s: invalid value %q (want 'safe' or 'unsafe_latest')", ReadModeSettingKey, raw)
}

// StorageIntegrityReadState is the host port supplying the unsafe parts a
// promotion already copied into hg_safe but whose cleanup is not yet
// acknowledged (Spec G D2). sentio-node satisfies it with *snode.Role. Nil
// means "no co-located SNode": unsafe_latest is refused, never degraded.
type StorageIntegrityReadState interface {
	PromotedUnsafeParts(tableID string) ([]string, error)
}

// StorageIntegrityOptions is the read-surface slice of rewriter.Options.
type StorageIntegrityOptions struct {
	// Enabled is storage_integrity.enabled. It activates the V2 contract on
	// every request, fail-closed handling and the acknowledgement gate,
	// independent of how many tables are Active (spec 2026-09-24 H6).
	Enabled bool
	// TableState supplies the per-query snapshot when the caller did not
	// attach one to the context (WithTableSnapshot). Required when Enabled.
	TableState      sitable.TableState
	DefaultReadMode ReadMode                  // "" → safe
	ReadState       StorageIntegrityReadState // nil → unsafe_latest refused
	// InsertLaneEnabled is true when the SI ingress plugin is wired (it then
	// owns INSERT admission). When false, an INSERT whose accessed tables
	// include an SI table is rejected here (plan deviation D-1).
	InsertLaneEnabled bool
}

type tableSnapshotCtxKey struct{}

// WithTableSnapshot attaches the query's table-state snapshot. The rewrite
// plugin takes exactly one snapshot per query and every stage reads it.
func WithTableSnapshot(ctx context.Context, snap sitable.Snapshot) context.Context {
	return context.WithValue(ctx, tableSnapshotCtxKey{}, snap)
}

// TableSnapshotFromContext returns the snapshot attached by WithTableSnapshot.
func TableSnapshotFromContext(ctx context.Context) (sitable.Snapshot, bool) {
	snap, ok := ctx.Value(tableSnapshotCtxKey{}).(sitable.Snapshot)
	return snap, ok && snap != nil
}

// snapshotFor returns the query's snapshot, falling back to the current one.
func (o StorageIntegrityOptions) snapshotFor(ctx context.Context) sitable.Snapshot {
	if snap, ok := TableSnapshotFromContext(ctx); ok {
		return snap
	}
	if o.TableState != nil {
		return o.TableState.Current()
	}
	return sitable.NewSnapshot(0, sitable.Ordinary, nil)
}

type readModeCtxKey struct{}

// WithReadMode attaches the per-query read mode (from SQL_x_read_mode).
func WithReadMode(ctx context.Context, m ReadMode) context.Context {
	return context.WithValue(ctx, readModeCtxKey{}, m)
}

// ReadModeFromContext returns the per-query read mode, if any.
func ReadModeFromContext(ctx context.Context) (ReadMode, bool) {
	m, ok := ctx.Value(readModeCtxKey{}).(ReadMode)
	return m, ok && m != ""
}

// RejectedError is a rewrite outcome that MUST reach the client as an
// Exception (the plugin fails closed on it) instead of falling open to
// the original SQL. Used for every storage-integrity rejection (reserved
// column, non-lane write, unavailable read mode) and for any failure before
// a trustworthy classification when SI membership is configured.
type RejectedError struct {
	Code    pb.RewriteCode
	Message string
	Cause   error
}

func (e *RejectedError) Error() string {
	return "rewriter rejected SQL (code=" + e.Code.String() + "): " + e.Message
}

func (e *RejectedError) Unwrap() error {
	return e.Cause
}

// buildStorageIntegrityArgs renders the proto block for one call. It returns
// nil only when storage integrity is disabled; when enabled the block is sent
// even with an empty table map, because the V2 contract is activated by
// version (spec 2026-09-24 H6), and it always names the reserved databases. Every Active table of the snapshot is listed;
// parts carries the promoted-but-not-yet-cleaned unsafe parts of the Active
// tables the query accesses, and is consulted only in unsafe_latest mode.
func buildStorageIntegrityArgs(opts StorageIntegrityOptions, snap sitable.Snapshot, mode ReadMode, parts map[string][]string) (*pb.StorageIntegrityArgs, error) {
	if !opts.Enabled {
		return nil, nil
	}
	if mode == "" {
		mode = ReadModeSafe
	}
	active := snap.Active()
	out := &pb.StorageIntegrityArgs{
		Tables:              make(map[string]*pb.StorageIntegrityArgs_Table, len(active)),
		ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
		ReservedRowIdColumn: DefaultReservedRowIDColumn,
		ContractVersion:     StorageIntegrityContractV2,
		// Sent on every enabled request: under V2 the engines protect these
		// databases even when no table is Active (plan A handoff).
		ReservedDatabases: sitable.ReservedDatabases(),
	}
	if mode == ReadModeUnsafeLatest {
		if opts.ReadState == nil {
			return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
				Message: "storage_integrity read mode unsafe_latest is unavailable on this housegate: no promotion-state port (co-located SNode) is wired"}
		}
		out.ReadMode = pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST
	}
	for _, t := range active {
		phys := sitable.PhysicalTable(t.ID)
		entry := &pb.StorageIntegrityArgs_Table{
			SafeTable:   sitable.SafeDatabase + "." + phys,
			UnsafeTable: sitable.UnsafeDatabase + "." + phys,
		}
		if mode == ReadModeUnsafeLatest {
			entry.ExcludedUnsafeParts = parts[t.ID]
		}
		out.Tables[t.ID] = entry
	}
	return out, nil
}

// promotedPartsFor fetches the promoted-but-not-yet-cleaned unsafe parts of
// exactly the given Active tables (spec 2026-09-24 §6.2). Every port error is a
// RejectedError: unsafe_latest fails closed, never degrades (Spec G D2).
func promotedPartsFor(rs StorageIntegrityReadState, tableIDs []string) (map[string][]string, error) {
	out := make(map[string][]string, len(tableIDs))
	for _, id := range tableIDs {
		parts, err := rs.PromotedUnsafeParts(id)
		if err != nil {
			return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
				Message: fmt.Sprintf("storage_integrity read mode unsafe_latest: cannot resolve promoted unsafe parts for %s: %v", id, err),
				Cause:   err}
		}
		if len(parts) > 0 {
			out[id] = parts
		}
	}
	return out, nil
}

// requireActiveAccessedIDs refuses an SI-flagged accessed id that is not an
// Active table of the query's snapshot. unsafe_latest keys its exclusions by
// Active id, so such an id would lose its exclusions without a trace
// (final ruling I2): an engine reporting ids differently fails loudly instead.
func requireActiveAccessedIDs(snap sitable.Snapshot, ids []string) error {
	active := map[string]bool{}
	for _, t := range snap.Active() {
		active[t.ID] = true
	}
	for _, id := range ids {
		if !active[id] {
			return &RejectedError{Code: pb.RewriteCode_RewriteError,
				Message: fmt.Sprintf("storage-integrity unsafe_latest rewrite accessed %q, which is not an Active storage-integrity table of this query's snapshot", id)}
		}
	}
	return nil
}

// storageIntegrityAccessedIDs returns the sorted, de-duplicated logical ids of
// the SI-flagged accessed tables.
func storageIntegrityAccessedIDs(tables []*pb.AccessedTable) []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range tables {
		if !t.GetIsStorageIntegrity() {
			continue
		}
		db := t.GetLogicalDatabase()
		if db == "" {
			db = t.GetOriginalDatabase()
		}
		id := t.GetOriginalTable()
		if db != "" {
			id = db + "." + id
		}
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	sort.Strings(out)
	return out
}

// storageIntegrityAccess returns the first SI-flagged accessed table's
// logical "db.table" key and whether one exists.
func storageIntegrityAccess(tables []*pb.AccessedTable) (string, bool) {
	for _, t := range tables {
		if t.GetIsStorageIntegrity() {
			db := t.GetLogicalDatabase()
			if db == "" {
				db = t.GetOriginalDatabase()
			}
			if db == "" {
				return t.GetOriginalTable(), true
			}
			return db + "." + t.GetOriginalTable(), true
		}
	}
	return "", false
}

// storageIntegrityRedaction replaces a protocol-owned name that has no logical
// equivalent, such as a reserved physical database or the row-id column.
const storageIntegrityRedaction = "<storage-integrity>"

// StorageIntegrityScrubber removes protocol-owned SI names from text that is
// about to reach a client. Qualified physical names map back to their logical
// table id; reserved databases and the row-id column are redacted.
//
// This is deliberately a narrow SI mapping, not a general exception reverse
// mapper. The selected rewriter backend still owns general reverse mapping.
type StorageIntegrityScrubber struct {
	replacer *strings.Replacer
}

// NewStorageIntegrityScrubber builds the scrubber for one snapshot: every
// Active table's qualified physical names map back to its logical id, and the
// two reserved databases and the row-id column are always redacted, even when
// no table is Active.
func NewStorageIntegrityScrubber(snap sitable.Snapshot) *StorageIntegrityScrubber {
	active := snap.Active()
	// Qualified names must precede their bare database prefixes.
	pairs := make([]string, 0, len(active)*4+6)
	for _, table := range active {
		phys := sitable.PhysicalTable(table.ID)
		pairs = append(pairs,
			sitable.SafeDatabase+"."+phys, table.ID,
			sitable.UnsafeDatabase+"."+phys, table.ID,
		)
	}
	pairs = append(pairs,
		sitable.SafeDatabase, storageIntegrityRedaction,
		sitable.UnsafeDatabase, storageIntegrityRedaction,
		DefaultReservedRowIDColumn, storageIntegrityRedaction,
	)
	return &StorageIntegrityScrubber{replacer: strings.NewReplacer(pairs...)}
}

// StorageIntegrityScrubberCache keeps the scrubber of the newest snapshot
// version it was asked for, so a scrubber is built once per version rather
// than once per exception.
type StorageIntegrityScrubberCache struct {
	mu      sync.Mutex
	version uint64
	built   bool
	current *StorageIntegrityScrubber
	builds  int
}

// For returns the scrubber for snap's version, building it on a version change.
func (c *StorageIntegrityScrubberCache) For(snap sitable.Snapshot) *StorageIntegrityScrubber {
	if c == nil || snap == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.built && c.version == snap.Version() {
		return c.current
	}
	c.current = NewStorageIntegrityScrubber(snap)
	c.version = snap.Version()
	c.built = true
	c.builds++
	return c.current
}

// Builds reports how many scrubbers the cache has built (test observability).
func (c *StorageIntegrityScrubberCache) Builds() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.builds
}

// Scrub is safe on a nil receiver and an empty message.
func (s *StorageIntegrityScrubber) Scrub(message string) string {
	if s == nil || s.replacer == nil || message == "" {
		return message
	}
	return s.replacer.Replace(message)
}
