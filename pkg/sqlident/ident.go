// Package sqlident contains small ClickHouse identifier helpers used by
// security-sensitive plugins when they need a stable database/table path.
package sqlident

import "strings"

// NormalizePath trims a dot-separated ClickHouse identifier path and returns a
// canonical path that preserves identifier segment boundaries. Plain identifier
// segments stay unquoted; segments that need escaping are emitted as canonical
// ClickHouse backtick identifiers. It returns an empty string when malformed.
func NormalizePath(value string) string {
	segments, ok := parsePath(value)
	if !ok || len(segments) == 0 {
		return ""
	}
	parts := make([]string, len(segments))
	for i, segment := range segments {
		if segment == "" {
			return ""
		}
		parts[i] = canonicalSegment(segment)
	}
	return strings.Join(parts, ".")
}

// SplitLastPath returns the final canonical identifier segment and the optional
// canonical database path before it. Malformed paths return two empty strings.
func SplitLastPath(value string) (database, table string) {
	segments, ok := parsePath(value)
	if !ok || len(segments) == 0 {
		return "", ""
	}
	parts := make([]string, len(segments))
	for i, segment := range segments {
		if segment == "" {
			return "", ""
		}
		parts[i] = canonicalSegment(segment)
	}
	if len(parts) == 1 {
		return "", parts[0]
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

// Quote returns one ClickHouse backtick-quoted identifier segment.
func Quote(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

// QuotePath normalizes and quotes every path segment. Malformed paths return
// an empty string so callers can fail closed.
func QuotePath(value string) string {
	segments, ok := parsePath(value)
	if !ok || len(segments) == 0 {
		return ""
	}
	parts := make([]string, len(segments))
	for i, segment := range segments {
		if segment == "" {
			return ""
		}
		parts[i] = Quote(segment)
	}
	return strings.Join(parts, ".")
}

func parsePath(value string) ([]string, bool) {
	parts := splitPath(value)
	if len(parts) == 0 {
		return nil, false
	}
	segments := make([]string, len(parts))
	for i, part := range parts {
		segment, ok := parseSegment(part)
		if !ok || segment == "" {
			return nil, false
		}
		segments[i] = segment
	}
	return segments, true
}

func splitPath(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	var (
		parts    []string
		start    int
		inQuoted bool
	)
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '`':
			if inQuoted && i+1 < len(value) && value[i+1] == '`' {
				i++
				continue
			}
			inQuoted = !inQuoted
		case '.':
			if !inQuoted {
				parts = append(parts, value[start:i])
				start = i + 1
			}
		}
	}
	if inQuoted {
		return nil
	}
	parts = append(parts, value[start:])
	return parts
}

func parseSegment(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}
	if strings.HasPrefix(value, "`") || strings.HasSuffix(value, "`") {
		return unquote(value)
	}
	if strings.Contains(value, "`") {
		return "", false
	}
	return value, true
}

func canonicalSegment(value string) string {
	if isBareIdentifier(value) {
		return value
	}
	return Quote(value)
}

func isBareIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if i == 0 {
			if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_' {
				continue
			}
			return false
		}
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' {
			continue
		}
		return false
	}
	return true
}

func unquote(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) < 2 || value[0] != '`' || value[len(value)-1] != '`' {
		return "", false
	}
	body := value[1 : len(value)-1]
	var b strings.Builder
	for i := 0; i < len(body); i++ {
		if body[i] == '`' {
			if i+1 >= len(body) || body[i+1] != '`' {
				return "", false
			}
			b.WriteByte('`')
			i++
			continue
		}
		b.WriteByte(body[i])
	}
	return b.String(), true
}
