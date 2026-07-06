package sqlident

import "strings"

// NormalizePath trims ClickHouse identifier path segments and removes
// identifier quoting. Empty segments make the whole path invalid.
func NormalizePath(value string) string {
	parts := strings.Split(strings.TrimSpace(value), ".")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, "`")
		if part == "" {
			return ""
		}
		parts[i] = part
	}
	return strings.Join(parts, ".")
}

func SplitLastPath(value string) (database, table string) {
	path := NormalizePath(value)
	if path == "" {
		return "", ""
	}
	parts := strings.Split(path, ".")
	if len(parts) == 1 {
		return "", parts[0]
	}
	return strings.Join(parts[:len(parts)-1], "."), parts[len(parts)-1]
}

func Quote(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func QuotePath(value string) string {
	path := NormalizePath(value)
	if path == "" {
		return ""
	}
	parts := strings.Split(path, ".")
	for i, part := range parts {
		parts[i] = Quote(part)
	}
	return strings.Join(parts, ".")
}
