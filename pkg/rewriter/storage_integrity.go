package rewriter

import (
	"context"
	"fmt"
	"sort"
	"strings"

	pb "github.com/housegate/rewriter-proto/gen/pb"
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

// StorageIntegrityTable is one SI table as housegate knows it: the logical
// id (== the rewriter's lookup key) and its two physical homes.
type StorageIntegrityTable struct {
	TableID     string // logical "<db>.<table>"
	SafeTable   string // "hg_safe.<phys>"
	UnsafeTable string // "hg_unsafe.<phys>"
}

// StorageIntegrityOptions is the read-surface slice of rewriter.Options.
type StorageIntegrityOptions struct {
	Tables          []StorageIntegrityTable
	DefaultReadMode ReadMode                  // "" → safe
	ReadState       StorageIntegrityReadState // nil → unsafe_latest refused
	// InsertLaneEnabled is true when the SI ingress plugin is wired (it then
	// owns INSERT admission). When false, an INSERT whose accessed tables
	// include an SI table is rejected here (plan deviation D-1).
	InsertLaneEnabled bool
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

// buildStorageIntegrityArgs renders the proto block for one call. Safe mode
// never consults the port; unsafe_latest requires it and surfaces every
// port error as a RejectedError (fail closed, D2).
func buildStorageIntegrityArgs(opts StorageIntegrityOptions, mode ReadMode) (*pb.StorageIntegrityArgs, error) {
	if len(opts.Tables) == 0 {
		return nil, nil
	}
	if mode == "" {
		mode = ReadModeSafe
	}
	out := &pb.StorageIntegrityArgs{
		Tables:              make(map[string]*pb.StorageIntegrityArgs_Table, len(opts.Tables)),
		ReadMode:            pb.StorageIntegrityArgs_READ_MODE_SAFE,
		ReservedRowIdColumn: DefaultReservedRowIDColumn,
		ContractVersion:     StorageIntegrityContractV1,
	}
	if mode == ReadModeUnsafeLatest {
		if opts.ReadState == nil {
			return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
				Message: "storage_integrity read mode unsafe_latest is unavailable on this housegate: no promotion-state port (co-located SNode) is wired"}
		}
		out.ReadMode = pb.StorageIntegrityArgs_READ_MODE_UNSAFE_LATEST
	}
	for _, t := range opts.Tables {
		entry := &pb.StorageIntegrityArgs_Table{SafeTable: t.SafeTable, UnsafeTable: t.UnsafeTable}
		if mode == ReadModeUnsafeLatest {
			parts, err := opts.ReadState.PromotedUnsafeParts(t.TableID)
			if err != nil {
				return nil, &RejectedError{Code: pb.RewriteCode_RewriteError,
					Message: fmt.Sprintf("storage_integrity read mode unsafe_latest: cannot resolve promoted unsafe parts for %s: %v", t.TableID, err),
					Cause:   err}
			}
			entry.ExcludedUnsafeParts = parts
		}
		out.Tables[t.TableID] = entry
	}
	return out, nil
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

// NewStorageIntegrityScrubber returns nil when no SI table is configured, so
// non-SI deployments pay no scrubbing cost.
func NewStorageIntegrityScrubber(opts StorageIntegrityOptions) *StorageIntegrityScrubber {
	if len(opts.Tables) == 0 {
		return nil
	}
	// Qualified names must precede their bare database prefixes.
	pairs := make([]string, 0, len(opts.Tables)*4+6)
	databases := make(map[string]bool)
	for _, table := range opts.Tables {
		pairs = append(pairs,
			table.SafeTable, table.TableID,
			table.UnsafeTable, table.TableID,
		)
		if db, _, ok := strings.Cut(table.SafeTable, "."); ok {
			databases[db] = true
		}
		if db, _, ok := strings.Cut(table.UnsafeTable, "."); ok {
			databases[db] = true
		}
	}
	databaseNames := make([]string, 0, len(databases))
	for db := range databases {
		databaseNames = append(databaseNames, db)
	}
	sort.Strings(databaseNames)
	for _, db := range databaseNames {
		pairs = append(pairs, db, storageIntegrityRedaction)
	}
	pairs = append(pairs, DefaultReservedRowIDColumn, storageIntegrityRedaction)
	return &StorageIntegrityScrubber{replacer: strings.NewReplacer(pairs...)}
}

// Scrub is safe on a nil receiver and an empty message.
func (s *StorageIntegrityScrubber) Scrub(message string) string {
	if s == nil || s.replacer == nil || message == "" {
		return message
	}
	return s.replacer.Replace(message)
}
