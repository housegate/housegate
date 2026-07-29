package lthashplugin

import (
	"sync"

	"github.com/housegate/housegate/pkg/lthash"
)

// TableDigest is the registry's per-table view: the running LtHash
// accumulator over every committed INSERT plus row/statement counters.
type TableDigest struct {
	Acc        *lthash.Hash
	Rows       uint64
	Statements uint64
}

// Registry accumulates per-table commitments process-wide. It is the MVP
// stand-in for the partition ledger the design places at the Keeper
// validation front.
type Registry struct {
	mu     sync.Mutex
	tables map[string]*TableDigest
}

// DefaultRegistry is the registry build.go wires when the plugin is
// enabled via config; tests read it back for verification.
var DefaultRegistry = NewRegistry()

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{tables: make(map[string]*TableDigest)}
}

func (r *Registry) add(table string, acc *lthash.Hash, rows uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	td := r.tables[table]
	if td == nil {
		td = &TableDigest{Acc: lthash.New()}
		r.tables[table] = td
	}
	td.Acc.AddHash(acc)
	td.Rows += rows
	td.Statements++
}

// Table returns a copy of the digest state for table, if any.
func (r *Registry) Table(table string) (TableDigest, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	td, ok := r.tables[table]
	if !ok {
		return TableDigest{}, false
	}
	cp, _ := lthash.FromBytes(td.Acc.Bytes())
	return TableDigest{Acc: cp, Rows: td.Rows, Statements: td.Statements}, true
}

// Reset clears all state (test hygiene).
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tables = make(map[string]*TableDigest)
}
