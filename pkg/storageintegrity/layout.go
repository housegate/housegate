package storageintegrity

import "strings"

const (
	DefaultUnsafeDatabase    = "hg_unsafe"
	DefaultSafeDatabase      = "hg_safe"
	DefaultUnsafeTableSuffix = ""
)

type TableLayoutConfig struct {
	UnsafeDatabase    string
	SafeDatabase      string
	UnsafeTableSuffix string
}

type TableLayout struct {
	UnsafeDatabase    string
	SafeDatabase      string
	UnsafeTableSuffix string
}

func NewTableLayout(cfg TableLayoutConfig) TableLayout {
	layout := TableLayout{
		UnsafeDatabase:    strings.TrimSpace(cfg.UnsafeDatabase),
		SafeDatabase:      strings.TrimSpace(cfg.SafeDatabase),
		UnsafeTableSuffix: cfg.UnsafeTableSuffix,
	}
	if layout.UnsafeDatabase == "" {
		layout.UnsafeDatabase = DefaultUnsafeDatabase
	}
	if layout.SafeDatabase == "" {
		layout.SafeDatabase = DefaultSafeDatabase
	}
	if layout.UnsafeTableSuffix == "" {
		layout.UnsafeTableSuffix = DefaultUnsafeTableSuffix
	}
	return layout
}

func (l TableLayout) UnsafeTable(tableID string) string {
	return QuoteTable(l.UnsafeDatabase, tableID+l.UnsafeTableSuffix)
}

func (l TableLayout) SafeTable(tableID string) string {
	return QuoteTable(l.SafeDatabase, tableID)
}

func QuoteTable(database, table string) string {
	return QuoteIdentifier(database) + "." + QuoteIdentifier(table)
}

func QuoteIdentifier(s string) string {
	return "`" + strings.ReplaceAll(s, "`", "``") + "`"
}
