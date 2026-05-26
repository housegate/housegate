// Package schema canonicalizes a SHOW CREATE TABLE result and hashes
// the column-definition portion. Engine-level settings (storage_policy,
// ORDER BY, SETTINGS, etc.) are intentionally excluded — they do not
// affect column layout and would cause false-positive schema-drift
// errors when operators tune them.
package schema

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/zeebo/blake3"
)

var (
	lineCommentRE = regexp.MustCompile(`--[^\n]*`)
	wsRunRE       = regexp.MustCompile(`\s+`)
	commaSpaceRE  = regexp.MustCompile(`\s*,\s*`)
	parenOpenRE   = regexp.MustCompile(`\s*\(\s*`)
	parenCloseRE  = regexp.MustCompile(`\s*\)\s*`)
)

// Canonicalize extracts the column-definition body from a SHOW CREATE
// TABLE result and returns it with comments stripped and whitespace
// normalized.
func Canonicalize(showCreate string) (string, error) {
	open := strings.Index(showCreate, "(")
	if open < 0 {
		return "", fmt.Errorf("schema: no opening paren in %q", showCreate)
	}
	depth, end := 0, -1
	for i := open; i < len(showCreate); i++ {
		switch showCreate[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				end = i
			}
		}
		if end >= 0 {
			break
		}
	}
	if end < 0 {
		return "", fmt.Errorf("schema: no matching close paren")
	}
	body := showCreate[open+1 : end]
	body = lineCommentRE.ReplaceAllString(body, "")
	body = strings.TrimSpace(body)
	body = wsRunRE.ReplaceAllString(body, " ")
	body = commaSpaceRE.ReplaceAllString(body, ",")
	body = parenOpenRE.ReplaceAllString(body, "(")
	body = parenCloseRE.ReplaceAllString(body, ")")
	return body, nil
}

// Hash returns blake3-256 of the canonical form.
func Hash(showCreate string) ([32]byte, error) {
	canon, err := Canonicalize(showCreate)
	if err != nil {
		return [32]byte{}, err
	}
	return blake3.Sum256([]byte(canon)), nil
}
