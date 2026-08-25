package storageintegrity

import (
	"fmt"
	"strings"
)

// PartsScope names the keys one inventory read is authoritative for. A read
// proves both presence and absence inside its scope and says nothing outside
// it, which is what lets the admission path read a few partitions instead of
// every active part in the deployment.
type PartsScope struct {
	Database            string
	IncludeSafeDatabase bool
	SafeDatabase        string
	Table               string
	Partitions          []string
}

// Covers reports whether this read is authoritative for key.
func (s PartsScope) Covers(key PartsKey) bool {
	switch key.Database {
	case s.Database:
	case s.SafeDatabase:
		if !s.IncludeSafeDatabase || s.SafeDatabase == "" {
			return false
		}
		return s.Table == ""
	default:
		return false
	}
	if s.Table != "" && key.Table != s.Table {
		return false
	}
	if s.Table == "" || len(s.Partitions) == 0 {
		return true
	}
	for _, partition := range s.Partitions {
		if partition == key.Partition {
			return true
		}
	}
	return false
}

// RequestedKeys returns the keys explicitly enumerated by this scope.
func (s PartsScope) RequestedKeys() []PartsKey {
	if s.Table == "" || len(s.Partitions) == 0 {
		return nil
	}
	keys := make([]PartsKey, 0, len(s.Partitions))
	for _, partition := range s.Partitions {
		keys = append(keys, PartsKey{Database: s.Database, Table: s.Table, Partition: partition})
	}
	return keys
}

// IsFull reports whether the scope covers every key in both configured
// databases.
func (s PartsScope) IsFull(cfg PartsPressureConfig) bool {
	return s.Table == "" && s.Database == cfg.UnsafeDatabase &&
		(cfg.SafeDatabase == "" || (s.IncludeSafeDatabase && s.SafeDatabase == cfg.SafeDatabase))
}

// partitionTexts maps logical partition IDs back to system.parts.partition
// text. "all" denotes an unpartitioned table and cannot be mixed with p_-IDs.
func partitionTexts(ids []string) ([]string, bool, error) {
	if len(ids) == 0 {
		return nil, true, nil
	}
	sawAll := false
	texts := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "all" {
			sawAll = true
			continue
		}
		if !strings.HasPrefix(id, "p_") {
			return nil, false, fmt.Errorf("storage_integrity: partition id %q is neither \"all\" nor p_-prefixed", id)
		}
		texts = append(texts, strings.TrimPrefix(id, "p_"))
	}
	if sawAll && len(texts) > 0 {
		return nil, false, fmt.Errorf("storage_integrity: partition id set mixes \"all\" with partitioned ids: %v", ids)
	}
	if sawAll {
		return nil, true, nil
	}
	return texts, false, nil
}
