package chproto

// Protocol revision constants used to gate addendum and chunked-packet features.
//
// These values come from ClickHouse server source (src/Core/ProtocolDefines.h)
// and match the constants in github.com/ClickHouse/ch-go/proto (feature.go).
// We re-export them here so callers outside pkg/chproto do not need to import
// ch-go directly for revision checks.
//
// Cross-reference with legacy proxy.go:
//   - fallbackRevision = 54423 — revision used when Hello parsing fails
//   - FeatureAddendum = FeatureQuotaKey = 54458 — addendum block present in handshake
//   - FeatureChunkedPackets = 54470 — chunked transport negotiated in addendum
//   - FeatureVersionedParallelReplicas = 54471 — parallel_replicas_version field in addendum
const (
	// RevisionMinAddendum is the minimum protocol revision at which the addendum
	// block (quota_key + chunked-mode fields) appears after ServerHello.
	// Matches ch-go's FeatureAddendum = FeatureQuotaKey = 54458.
	RevisionMinAddendum = 54458

	// RevisionMinChunkedPackets is the minimum protocol revision at which the
	// proto_send_chunked / proto_recv_chunked fields appear in the addendum.
	// Matches ch-go's FeatureChunkedPackets = 54470.
	RevisionMinChunkedPackets = 54470

	// RevisionMinVersionedParallelReplicas is the minimum protocol revision at which the
	// parallel_replicas_version UVarInt field appears in the addendum.
	// Matches ch-go's FeatureVersionedParallelReplicas = 54471.
	RevisionMinVersionedParallelReplicas = 54471

	// FallbackRevision is the protocol revision used when handshake decoding
	// fails and we need a safe default. Matches legacy proxy.go's fallbackRevision.
	FallbackRevision = 54423
)

// SupportsAddendum reports whether the given negotiated revision carries an
// addendum block after ServerHello.
func SupportsAddendum(rev int) bool { return rev >= RevisionMinAddendum }

// SupportsChunkedPackets reports whether the given negotiated revision supports
// the chunked-packet transport negotiation fields in the addendum.
func SupportsChunkedPackets(rev int) bool { return rev >= RevisionMinChunkedPackets }

// SupportsVersionedParallelReplicas reports whether the given negotiated revision
// includes the parallel_replicas_version UVarInt field in the addendum.
func SupportsVersionedParallelReplicas(rev int) bool {
	return rev >= RevisionMinVersionedParallelReplicas
}
