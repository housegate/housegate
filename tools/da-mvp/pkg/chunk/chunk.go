// Package chunk splits a byte slice into pieces no larger than
// MaxChunkSize and reassembles them. Used by da-publisher to fit
// Parquet payloads under the Celestia single-blob limit.
package chunk

// MaxChunkSize is 1.5 MiB — well under Celestia's ~1.97 MiB PFB limit.
const MaxChunkSize = 1500 * 1024

// Split partitions data into chunks of at most MaxChunkSize bytes.
// Always returns at least one chunk; an empty input yields [[]byte{}].
func Split(data []byte) [][]byte {
	if len(data) == 0 {
		return [][]byte{{}}
	}
	out := make([][]byte, 0, (len(data)+MaxChunkSize-1)/MaxChunkSize)
	for i := 0; i < len(data); i += MaxChunkSize {
		end := i + MaxChunkSize
		if end > len(data) {
			end = len(data)
		}
		out = append(out, data[i:end])
	}
	return out
}

// Assemble concatenates chunks back to the original.
func Assemble(chunks [][]byte) []byte {
	total := 0
	for _, c := range chunks {
		total += len(c)
	}
	out := make([]byte, 0, total)
	for _, c := range chunks {
		out = append(out, c...)
	}
	return out
}
