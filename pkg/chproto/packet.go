// Package chproto is a thin proxy-oriented wrapper around
// github.com/ClickHouse/ch-go/proto. It adds a streaming tagged-union
// ReadPacket, zero-copy Splice for Data/Progress/etc., addendum (chunked)
// negotiation, and a few ClientCode constants ch-go does not expose.
//
// Design: docs/superpowers/specs/2026-04-21-clickhouse-tcp-conn-interface-design.md
package chproto

import (
	"errors"

	"github.com/ClickHouse/ch-go/proto"
)

// Re-exports: downstream packages import chproto only.
//
// Query is a NATIVE struct (not an alias to proto.Query) because we use a
// custom codec that captures fields ch-go does not expose. See query.go.
type (
	ClientCode  = proto.ClientCode
	ServerCode  = proto.ServerCode
	ClientHello = proto.ClientHello
	ServerHello = proto.ServerHello
	Exception   = proto.Exception
	ClientInfo  = proto.ClientInfo
	Setting     = proto.Setting
)

// Known ClientCode values from ch-go (aliased for locality).
const (
	ClientHelloCode  = proto.ClientCodeHello
	ClientQueryCode  = proto.ClientCodeQuery
	ClientDataCode   = proto.ClientCodeData
	ClientCancelCode = proto.ClientCodeCancel
	ClientPingCode   = proto.ClientCodePing
)

// Proxy-only ClientCode constants ch-go v0.71 does not expose.
// Source: ClickHouse TCPHandler enum order, verified against server source.
const (
	ClientKeepAlive                   ClientCode = 6
	ClientScalar                      ClientCode = 7
	ClientIgnoredPartUUIDs            ClientCode = 8
	ClientReadTaskResponse            ClientCode = 9
	ClientMergeTreeReadTaskResponse   ClientCode = 10
	ClientQueryPlan                   ClientCode = 11
	ClientClusterFunctionReadTaskResp ClientCode = 13
)

// Known ServerCode re-exports.
const (
	ServerHelloCode                               = proto.ServerCodeHello
	ServerDataCode                                = proto.ServerCodeData
	ServerExceptionCode                           = proto.ServerCodeException
	ServerProgressCode                            = proto.ServerCodeProgress
	ServerPongCode                                = proto.ServerCodePong
	ServerEndOfStreamCode                         = proto.ServerCodeEndOfStream
	ServerProfileCode                             = proto.ServerCodeProfile
	ServerTotalsCode                              = proto.ServerCodeTotals
	ServerExtremesCode                            = proto.ServerCodeExtremes
	ServerTablesStatusCode                        = proto.ServerCodeTablesStatus
	ServerLogCode                                 = proto.ServerCodeLog
	ServerTableColumnsCode                        = proto.ServerCodeTableColumns
	ServerPartUUIDsCode                           = proto.ServerPartUUIDs
	ServerReadTaskRequestCode                     = proto.ServerReadTaskRequest
	ServerProfileEventsCode                       = proto.ServerProfileEvents
	ServerAllRangesAnnouncementCode    ServerCode = 15
	ServerMergeTreeReadTaskRequestCode ServerCode = 16
	ServerTimezoneUpdateCode           ServerCode = 17
	ServerSSHChallengeCode                        = proto.ServerCodeSSHChallenge
)

// Direction tells the Codec which code-set to expect on reads.
type Direction int

const (
	// DirFromClient: reader expects ClientCode packets (client→proxy side).
	DirFromClient Direction = iota
	// DirToUpstream: reader expects ServerCode packets (upstream→proxy side).
	DirToUpstream
)

// Packet is the tagged union produced by Codec.ReadPacket.
//
// Normal cases populate exactly one of (Decoded, Raw):
//   - Decoded != nil when Type was in the decodeTypes list passed to ReadPacket
//   - Raw   != nil when Type was NOT requested (caller hands it to Splice)
//
// Exception — ServerHello: when an upstream-direction codec successfully
// decodes a ServerHello, Raw is also populated with the full on-wire bytes
// so the caller can forward the raw ServerHello to the client without
// losing trailing fields that ch-go's ServerHello struct does not model.
// See Codec.ReadPacket for details.
type Packet struct {
	// Type is a ClientCode or ServerCode value cast to uint64 (ch-go uses byte
	// for these enums; uint64 keeps the API forward-compatible if CH ever
	// extends past 255).
	Type uint64
	// RawLen is the total on-wire length of the packet including its leading
	// VarUInt type byte.
	RawLen int
	// Decoded holds a typed struct pointer: *ClientHello, *ServerHello,
	// *Query, *Exception. nil for packets not in the decodeTypes set.
	Decoded any
	// Raw holds the accumulated on-wire bytes of the packet (including the
	// leading type VarUInt) when Decoded is nil. Intended to be handed to
	// Codec.Splice.
	Raw []byte
}

// Sentinel errors.
var (
	ErrMalformed      = errors.New("chproto: malformed packet")
	ErrUnknownPacket  = errors.New("chproto: unknown packet type")
	ErrDecode         = errors.New("chproto: decode failed")
	ErrPacketTooLarge = errors.New("chproto: packet exceeds configured byte limit")
	// ErrUnsupportedResultType means a valid server Data packet named a
	// Native column type that Housegate's boundary decoder cannot consume.
	// Relay may preserve transparency by forwarding the already-captured
	// compressed bytes and temporarily switching to opaque streaming.
	ErrUnsupportedResultType = errors.New("chproto: unsupported server result type")
)
