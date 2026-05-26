// Package ids derives stable byte-string identifiers from human-readable
// (database, table) names. The same derivation runs in publisher and
// rebuilder so both sides agree on the contract's bytes32 indexing keys
// and the Celestia namespace.
package ids

import "github.com/zeebo/blake3"

const namespacePrefix = "hgmv" // 4 bytes; housegate-mvp marker

// DBID returns blake3("hgmv-db|" + name).
func DBID(name string) [32]byte {
	return blake3.Sum256([]byte("hgmv-db|" + name))
}

// TableID returns blake3("hgmv-tbl|" + name) — distinct domain from DBID.
func TableID(name string) [32]byte {
	return blake3.Sum256([]byte("hgmv-tbl|" + name))
}

// Namespace returns the 10-byte Celestia namespace for (db, table):
// "hgmv" + 6 bytes from blake3(db||"|"||table).
func Namespace(db, table string) []byte {
	h := blake3.Sum256([]byte(db + "|" + table))
	out := make([]byte, 10)
	copy(out[:4], []byte(namespacePrefix))
	copy(out[4:], h[:6])
	return out
}
