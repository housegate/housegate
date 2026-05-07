package forward

import (
	"regexp"
)

// useRegex matches a standalone USE statement of the form "USE <name>" with
// optional surrounding whitespace, optional backtick or double-quote delimiters
// around the database name, and an optional trailing semicolon.
//
// Intentionally narrow: "USE x SETTINGS y=1", multi-statement inputs, and
// comment-only lines all return ("", false) so the rewriter's
// maybeUpdateLogicalDatabase callback handles the more complex forms.
//
// Go's regexp (RE2) does not support backreferences, so the opening and closing
// quote characters are matched in three separate alternative branches below,
// each capturing the database name in group 1.
var useRegex = regexp.MustCompile(
	"(?i)^\\s*USE\\s+(?:" +
		"`([A-Za-z0-9_-]+)`" +
		`|"([A-Za-z0-9_-]+)"` +
		`|([A-Za-z0-9_-]+)` +
		")\\s*;?\\s*$",
)

// matchUse returns (databaseName, true) when sql is a standalone USE statement.
// Returns ("", false) for anything more elaborate (USE x SETTINGS y=1,
// multi-statement, comments, etc.).
func matchUse(sql string) (string, bool) {
	m := useRegex.FindStringSubmatch(sql)
	if m == nil {
		return "", false
	}
	// m[1] = backtick match, m[2] = double-quote match, m[3] = unquoted match;
	// exactly one will be non-empty.
	for _, g := range m[1:] {
		if g != "" {
			return g, true
		}
	}
	return "", false
}
