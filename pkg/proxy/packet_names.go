package proxy

import (
	"encoding/binary"
	"fmt"
)

// packetNames maps client → server packet type IDs to the labels used
// by the wire-level packets_total{type=...} counter.
var packetNames = map[uint64]string{
	0:  "Hello",
	1:  "Query",
	2:  "Data",
	3:  "Cancel",
	4:  "Ping",
	5:  "TablesStatusRequest",
	6:  "KeepAlive",
	7:  "Scalar",
	8:  "IgnoredPartUUIDs",
	9:  "ReadTaskResponse",
	10: "MergeTreeReadTaskResponse",
	11: "QueryPlan",
	// type 12 is reserved by ClickHouse TCPHandler.
	13: "ClusterFunctionReadTaskResponse",
}

// serverPacketNames maps server → client packet type IDs to the labels
// used by server_packets_total{type=...}. Used by the upstream-to-client
// path's first-byte heuristic in Relay.upstreamToClient.
var serverPacketNames = map[uint64]string{
	0:  "Hello",
	1:  "Data",
	2:  "Exception",
	3:  "Progress",
	4:  "Pong",
	5:  "EndOfStream",
	6:  "ProfileInfo",
	7:  "Totals",
	8:  "Extremes",
	9:  "TablesStatusResponse",
	10: "Log",
	11: "TableColumns",
	12: "PartUUIDs",
	13: "ReadTaskRequest",
	14: "ProfileEvents",
	15: "MergeTreeReadTaskRequest",
	16: "MergeTreeAllRangesAnnouncement",
	17: "TimezoneUpdate",
}

// detectServerPacketType peeks at the leading VarUInt of a buffer to
// classify a server-side packet for metrics. Returns "unknown" when the
// buffer doesn't start at a packet boundary (mid-payload reads from
// upstreamToClient's chunked io.Copy) so the metrics emitter can skip
// the increment cleanly.
func detectServerPacketType(chunk []byte) string {
	if len(chunk) == 0 {
		return "unknown"
	}
	if chunk[0]&0x80 != 0 {
		return "unknown"
	}
	typ, n := binary.Uvarint(chunk)
	if n <= 0 {
		return "unknown"
	}
	if name, ok := serverPacketNames[typ]; ok {
		return name
	}
	if typ < 32 {
		return fmt.Sprintf("type_%d", typ)
	}
	return "unknown"
}
