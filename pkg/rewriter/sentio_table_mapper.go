package rewriter

// SentioNetworkTableMapper maps Sentio-Network virtual table names
// to physical names for one processor. Implementations are typically
// backed by sentio-core's TableMapper.
type SentioNetworkTableMapper interface {
	Database() string
	RawTable(table string) (string, bool, error)
	RawTables(tables ...string) (map[string]string, error)
	All() map[string]string
	Reverse(rawTable string) (string, bool, error)
}
