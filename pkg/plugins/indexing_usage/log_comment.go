package indexingusage

import (
	"encoding/json"
	"strings"
)

// LogCommentSettingKey is the ClickHouse session setting the driver
// uses to ship per-commit context. See sentioxyz/sentio PR #18293
// (driver/controller/startup/startup.go::buildCommitCtx).
const LogCommentSettingKey = "log_comment"

// driverLogComment is the producer schema written by
// driver/controller/startup/startup.go::buildCommitCtx. We only consume
// processor_id (for cross-checking against the destination DB's
// ProcessorId) and watching (the only signal that distinguishes
// backfill from live processing). Extra fields are tolerated and
// ignored to keep us forward-compatible with future driver additions.
//
// Watching is a pointer so an absent "watching" key (nil) is
// distinguishable from an explicit "watching":false. Callers treat nil
// as watching=true (NOT backfill) — matching the missing-setting default
// — so a well-formed log_comment that simply omits the key does not flip
// live writes to the backfill SKU; only an explicit false marks backfill.
type driverLogComment struct {
	ProcessorID string `json:"processor_id"`
	Watching    *bool  `json:"watching"`
}

// ParseLogComment decodes a log_comment setting value into the fields
// indexing_usage cares about. Returns the parsed view + ok=true on a
// well-formed JSON object; ok=false when the value is empty, not JSON,
// or shaped unexpectedly.
//
// The driver writes a bare JSON object string. clickhouse-go's
// CustomSetting path may wrap the value in single or double quotes
// during Field::restoreFromDump (the same wrap that bit auth's payer
// setting before TrimQuotes was added) — strip a single pair of
// surrounding quotes before parsing so we don't reject otherwise-valid
// payloads on a transport detail.
func ParseLogComment(raw string) (driverLogComment, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) >= 2 {
		first, last := raw[0], raw[len(raw)-1]
		if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
			raw = raw[1 : len(raw)-1]
		}
	}
	if raw == "" {
		return driverLogComment{}, false
	}
	var lc driverLogComment
	if err := json.Unmarshal([]byte(raw), &lc); err != nil {
		return driverLogComment{}, false
	}
	return lc, true
}
