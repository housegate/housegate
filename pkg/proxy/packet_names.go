package proxy

import (
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

// serverPacketNames maps server → client packet type IDs to the labels used
// by server_packets_total{type=...}. Relay classifies already-framed packets
// by their decoded type, never by looking at arbitrary TCP chunks.
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
	15: "MergeTreeAllRangesAnnouncement",
	16: "MergeTreeReadTaskRequest",
	17: "TimezoneUpdate",
	18: "SSHChallenge",
}

func serverPacketName(typ uint64) string {
	if name, ok := serverPacketNames[typ]; ok {
		return name
	}
	if typ < 32 {
		return fmt.Sprintf("type_%d", typ)
	}
	return "unknown"
}
