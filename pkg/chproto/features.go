package chproto

// Protocol revision constants used to gate addendum and chunked-packet features.
//
// These values come from ClickHouse server source (src/Core/ProtocolDefines.h)
// and are defined here so revision checks do not depend on which ClickHouse Go
// SDK version is linked.
//
// Cross-reference with legacy proxy.go:
//   - fallbackRevision = 54423 — revision used when Hello parsing fails
//   - FeatureAddendum = FeatureQuotaKey = 54458 — addendum block present in handshake
//   - RevisionMinChunkedPackets = 54470 — chunked transport negotiated in addendum
//   - RevisionMinVersionedParallelReplicas = 54471 — parallel_replicas_version field in addendum
const (
	// RevisionMinAddendum is the minimum protocol revision at which the addendum
	// block (quota_key + chunked-mode fields) appears after ServerHello.
	RevisionMinAddendum = 54458

	// RevisionMinChunkedPackets is the minimum protocol revision at which the
	// proto_send_chunked / proto_recv_chunked fields appear in the addendum.
	RevisionMinChunkedPackets = 54470

	// RevisionMinVersionedParallelReplicas is the minimum protocol revision at which the
	// parallel_replicas_version UVarInt field appears in the addendum.
	RevisionMinVersionedParallelReplicas = 54471

	// RevisionMinInterserverExternallyGrantedRoles is the minimum protocol
	// revision at which the externally granted roles field is carried in the
	// interserver section of a Query packet.
	RevisionMinInterserverExternallyGrantedRoles = 54472

	// RevisionMinQueryAndLineNumbers is the minimum protocol revision at which
	// the script query number and line number fields appear in the client info.
	RevisionMinQueryAndLineNumbers = 54475

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

// SupportsInterserverExternallyGrantedRoles reports whether the given negotiated
// revision carries the externally granted roles field in a Query packet.
func SupportsInterserverExternallyGrantedRoles(rev int) bool {
	return rev >= RevisionMinInterserverExternallyGrantedRoles
}

// SupportsQueryAndLineNumbers reports whether the given negotiated revision
// carries the script query number and line number fields in the client info.
func SupportsQueryAndLineNumbers(rev int) bool { return rev >= RevisionMinQueryAndLineNumbers }
