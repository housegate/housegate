package chproto

// query.go — Query packet types and revision-aware codec.
//
// We do NOT use ch-go's proto.Query.DecodeAware / EncodeAware. Those have
// several documented bugs that bite the proxy in production:
//
//  1. DecodeAware hard-errors on revision < FeatureSettingsSerializedAsStrings (54429)
//     with "unsupported version" — older clients are silently dropped.
//  2. EncodeAware hardcodes Stage to StageComplete, ignoring the client's
//     actual stage value (StageFetchColumns / StageWithMergeableState etc.).
//     This silently changes query execution semantics.
//  3. EncodeAware misses FeatureInterserverExternallyGrantedRoles (>= 54472).
//  4. ClientInfo.EncodeAware misses FeatureQueryAndLineNumbers (>= 54475)
//     and FeatureInterserverJWT (>= 54476) fields.
//
// The custom codec below is a strict, field-by-field decode + encode aligned
// with ClickHouse server's TCPHandler::receiveQuery and ClientInfo::write.
// Decode and encode are mirrors: anything conditionally read is conditionally
// written back, in the same order. Round-trip is lossless.
//
// Adapted from the legacy pkg/proxy/query_codec.go (which is being removed in
// favour of this canonical version).

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/segmentio/asm/bswap"
	"go.opentelemetry.io/otel/trace"
)

// Query represents a ClickHouse Query packet.
//
// Fields are duplicated from proto.Query (rather than embedding it) so the
// natural literal syntax `&chproto.Query{Body: "..."}` keeps working — Go
// embedding does not promote field initialisers in struct literals.
//
// Round-trip integrity: every field present on the wire across supported
// revisions has a slot here, so a decode→encode pair never loses data.
type Query struct {
	// --- Core fields (mirror proto.Query) ---

	ID          string
	Info        ClientInfo
	Settings    []Setting
	Secret      string
	Stage       proto.Stage
	Compression proto.Compression
	Body        string
	Parameters  []proto.Parameter

	// --- Revision-gated fields ch-go does not expose ---

	// ExternalRoles is the FeatureInterserverExternallyGrantedRoles
	// (>= 54472) field — interserver call records the externally granted
	// roles here.
	ExternalRoles string

	// OldSettings carries the pre-FeatureSettingsSerializedAsStrings
	// (< 54429) settings format. Mutually exclusive with Settings: a
	// given decode populates one or the other based on revision.
	OldSettings []OldSetting

	// ScriptQueryNumber / ScriptLineNumber: FeatureQueryAndLineNumbers
	// (>= 54475). clickhouse-client uses these for line-number reporting.
	ScriptQueryNumber uint64
	ScriptLineNumber  uint64

	// HasJWT / JWT: FeatureInterserverJWT (>= 54476). Carries an
	// interserver JWT credential.
	HasJWT bool
	JWT    string

	// HasTrace preserves the raw hasTrace flag from the wire so encoding
	// uses it verbatim instead of consulting Span.IsValid() — that
	// distinction matters when trace data round-trips through us with
	// fields that satisfy the wire format but not OpenTelemetry's
	// stricter validity rules.
	HasTrace bool
}

// OldSetting represents a single setting in the pre-54429 wire format:
// [Key: String][Value: UInt64]. The list is terminated by an empty Key.
type OldSetting struct {
	Key   string
	Value uint64
}

// DecodeQuery reads a Query body at the given negotiated revision. The
// caller must have already consumed the leading type VarUInt.
func DecodeQuery(r *proto.Reader, revision int) (*Query, error) {
	q := &Query{}

	// 1. QueryID
	qid, err := r.Str()
	if err != nil {
		return nil, decodeFieldErr("QueryID", err)
	}
	q.ID = qid

	// 2. ClientInfo
	if proto.FeatureClientWriteInfo.In(revision) {
		if err := decodeClientInfo(r, &q.Info, revision, q); err != nil {
			return nil, decodeFieldErr("ClientInfo", err)
		}
	}

	// 3. Settings — wire format depends on revision
	if proto.FeatureSettingsSerializedAsStrings.In(revision) {
		// New format: [Key: String][Flags: UVarInt][Value: String], terminated by empty Key.
		for {
			var s proto.Setting
			if err := s.Decode(r); err != nil {
				return nil, decodeFieldErr("Setting", err)
			}
			if s.Key == "" {
				break
			}
			q.Settings = append(q.Settings, s)
		}
	} else {
		// Old format: [Key: String][Value: UInt64], terminated by empty Key.
		// Server side reads UInt64 (8 bytes LE unsigned), not Int64.
		for {
			key, err := r.Str()
			if err != nil {
				return nil, decodeFieldErr("OldSetting.Key", err)
			}
			if key == "" {
				break
			}
			val, err := r.UInt64()
			if err != nil {
				return nil, decodeFieldErr("OldSetting.Value", err)
			}
			q.OldSettings = append(q.OldSettings, OldSetting{Key: key, Value: val})
		}
	}

	// 4. ExternalRoles
	if proto.FeatureInterserverExternallyGrantedRoles.In(revision) {
		roles, err := r.Str()
		if err != nil {
			return nil, decodeFieldErr("ExternalRoles", err)
		}
		q.ExternalRoles = roles
	}

	// 5. InterServerSecret
	if proto.FeatureInterServerSecret.In(revision) {
		secret, err := r.Str()
		if err != nil {
			return nil, decodeFieldErr("InterServerSecret", err)
		}
		q.Secret = secret
	}

	// 6. Stage
	stageVal, err := r.UVarInt()
	if err != nil {
		return nil, decodeFieldErr("Stage", err)
	}
	q.Stage = proto.Stage(stageVal)

	// 7. Compression
	compVal, err := r.UVarInt()
	if err != nil {
		return nil, decodeFieldErr("Compression", err)
	}
	q.Compression = proto.Compression(compVal)

	// 8. Body (SQL)
	body, err := r.Str()
	if err != nil {
		return nil, decodeFieldErr("Body", err)
	}
	q.Body = body

	// 9. Parameters
	if proto.FeatureParameters.In(revision) {
		for {
			var p proto.Parameter
			if err := p.Decode(r); err != nil {
				return nil, decodeFieldErr("Parameter", err)
			}
			if p.Key == "" {
				break
			}
			q.Parameters = append(q.Parameters, p)
		}
	}

	return q, nil
}

// EncodeQuery writes the Query packet (including its leading type byte)
// to b at the given negotiated revision. The encoding is the strict
// mirror of DecodeQuery.
func EncodeQuery(b *proto.Buffer, q *Query, revision int) {
	// Packet type
	proto.ClientCodeQuery.Encode(b)

	// 1. QueryID
	b.PutString(q.ID)

	// 2. ClientInfo
	if proto.FeatureClientWriteInfo.In(revision) {
		encodeClientInfo(b, &q.Info, revision, q)
	}

	// 3. Settings — match decode's revision branch
	if proto.FeatureSettingsSerializedAsStrings.In(revision) {
		for _, s := range q.Settings {
			s.Encode(b)
		}
		b.PutString("")
	} else {
		for _, s := range q.OldSettings {
			b.PutString(s.Key)
			b.PutUInt64(s.Value)
		}
		b.PutString("")
	}

	// 4. ExternalRoles
	if proto.FeatureInterserverExternallyGrantedRoles.In(revision) {
		b.PutString(q.ExternalRoles)
	}

	// 5. InterServerSecret
	if proto.FeatureInterServerSecret.In(revision) {
		b.PutString(q.Secret)
	}

	// 6. Stage — write the client's actual value, not a hardcoded constant.
	q.Stage.Encode(b)

	// 7. Compression
	q.Compression.Encode(b)

	// 8. Body
	b.PutString(q.Body)

	// 9. Parameters
	if proto.FeatureParameters.In(revision) {
		for _, p := range q.Parameters {
			p.Encode(b)
		}
		b.PutString("")
	}
}

// decodeClientInfo is the strict field-by-field ClientInfo decoder.
// Fields not present on the proto.ClientInfo struct (script line/query
// numbers, JWT, raw hasTrace flag) are stored on the surrounding Query.
func decodeClientInfo(r *proto.Reader, info *ClientInfo, revision int, q *Query) error {
	v, err := r.UInt8()
	if err != nil {
		return decodeFieldErr("query kind", err)
	}
	info.Query = proto.ClientQueryKind(v)

	if info.InitialUser, err = r.Str(); err != nil {
		return decodeFieldErr("initial user", err)
	}
	if info.InitialQueryID, err = r.Str(); err != nil {
		return decodeFieldErr("initial query id", err)
	}
	if info.InitialAddress, err = r.Str(); err != nil {
		return decodeFieldErr("initial address", err)
	}

	if proto.FeatureQueryStartTime.In(revision) {
		if info.InitialTime, err = r.Int64(); err != nil {
			return decodeFieldErr("query start time", err)
		}
	}

	ifByte, err := r.UInt8()
	if err != nil {
		return decodeFieldErr("interface", err)
	}
	info.Interface = proto.Interface(ifByte)

	if info.OSUser, err = r.Str(); err != nil {
		return decodeFieldErr("os user", err)
	}
	if info.ClientHostname, err = r.Str(); err != nil {
		return decodeFieldErr("client hostname", err)
	}
	if info.ClientName, err = r.Str(); err != nil {
		return decodeFieldErr("client name", err)
	}

	if info.Major, err = r.Int(); err != nil {
		return decodeFieldErr("major version", err)
	}
	if info.Minor, err = r.Int(); err != nil {
		return decodeFieldErr("minor version", err)
	}
	if info.ProtocolVersion, err = r.Int(); err != nil {
		return decodeFieldErr("protocol version", err)
	}

	if proto.FeatureQuotaKeyInClientInfo.In(revision) {
		if info.QuotaKey, err = r.Str(); err != nil {
			return decodeFieldErr("quota key", err)
		}
	}

	if proto.FeatureDistributedDepth.In(revision) {
		if info.DistributedDepth, err = r.Int(); err != nil {
			return decodeFieldErr("distributed depth", err)
		}
	}

	if proto.FeatureVersionPatch.In(revision) && info.Interface == proto.InterfaceTCP {
		if info.Patch, err = r.Int(); err != nil {
			return decodeFieldErr("patch version", err)
		}
	}

	if proto.FeatureOpenTelemetry.In(revision) {
		hasTrace, err := r.Bool()
		if err != nil {
			return decodeFieldErr("open telemetry flag", err)
		}
		q.HasTrace = hasTrace
		if hasTrace {
			var cfg trace.SpanContextConfig
			traceIDBytes, err := r.ReadRaw(16)
			if err != nil {
				return decodeFieldErr("trace id", err)
			}
			bswap.Swap64(traceIDBytes)
			copy(cfg.TraceID[:], traceIDBytes)

			spanIDBytes, err := r.ReadRaw(8)
			if err != nil {
				return decodeFieldErr("span id", err)
			}
			bswap.Swap64(spanIDBytes)
			copy(cfg.SpanID[:], spanIDBytes)

			tsStr, err := r.Str()
			if err != nil {
				return decodeFieldErr("trace state", err)
			}
			if tsStr != "" {
				ts, err := trace.ParseTraceState(tsStr)
				if err != nil {
					return decodeFieldErr("parse trace state", err)
				}
				cfg.TraceState = ts
			}

			tf, err := r.Byte()
			if err != nil {
				return decodeFieldErr("trace flags", err)
			}
			cfg.TraceFlags = trace.TraceFlags(tf)

			info.Span = trace.NewSpanContext(cfg)
		}
	}

	if proto.FeatureParallelReplicas.In(revision) {
		collaborateVal, err := r.Int()
		if err != nil {
			return decodeFieldErr("parallel replicas", err)
		}
		info.CollaborateWithInitiator = collaborateVal == 1

		if info.CountParticipatingReplicas, err = r.Int(); err != nil {
			return decodeFieldErr("count participating replicas", err)
		}
		if info.NumberOfCurrentReplica, err = r.Int(); err != nil {
			return decodeFieldErr("number of current replica", err)
		}
	}

	if proto.FeatureQueryAndLineNumbers.In(revision) {
		if q.ScriptQueryNumber, err = r.UVarInt(); err != nil {
			return decodeFieldErr("script query number", err)
		}
		if q.ScriptLineNumber, err = r.UVarInt(); err != nil {
			return decodeFieldErr("script line number", err)
		}
	}

	if proto.Feature(54476).In(revision) {
		if q.HasJWT, err = r.Bool(); err != nil {
			return decodeFieldErr("jwt flag", err)
		}
		if q.HasJWT {
			if q.JWT, err = r.Str(); err != nil {
				return decodeFieldErr("jwt", err)
			}
		}
	}

	return nil
}

// encodeClientInfo is the strict mirror of decodeClientInfo. It emits the
// FeatureQueryAndLineNumbers and FeatureInterserverJWT fields that
// ch-go's ClientInfo.EncodeAware leaves out.
func encodeClientInfo(b *proto.Buffer, info *ClientInfo, revision int, q *Query) {
	b.PutByte(byte(info.Query))

	b.PutString(info.InitialUser)
	b.PutString(info.InitialQueryID)
	b.PutString(info.InitialAddress)

	if proto.FeatureQueryStartTime.In(revision) {
		b.PutInt64(info.InitialTime)
	}

	b.PutByte(byte(info.Interface))

	b.PutString(info.OSUser)
	b.PutString(info.ClientHostname)
	b.PutString(info.ClientName)

	b.PutInt(info.Major)
	b.PutInt(info.Minor)
	b.PutInt(info.ProtocolVersion)

	if proto.FeatureQuotaKeyInClientInfo.In(revision) {
		b.PutString(info.QuotaKey)
	}

	if proto.FeatureDistributedDepth.In(revision) {
		b.PutInt(info.DistributedDepth)
	}

	if proto.FeatureVersionPatch.In(revision) && info.Interface == proto.InterfaceTCP {
		b.PutInt(info.Patch)
	}

	if proto.FeatureOpenTelemetry.In(revision) {
		// Use the raw HasTrace bit captured at decode rather than
		// Span.IsValid() — see the type-level comment for the
		// distinction.
		if q.HasTrace {
			b.PutByte(1)
			{
				v := info.Span.TraceID()
				start := len(b.Buf)
				b.Buf = append(b.Buf, v[:]...)
				bswap.Swap64(b.Buf[start:])
			}
			{
				v := info.Span.SpanID()
				start := len(b.Buf)
				b.Buf = append(b.Buf, v[:]...)
				bswap.Swap64(b.Buf[start:])
			}
			b.PutString(info.Span.TraceState().String())
			b.PutByte(byte(info.Span.TraceFlags()))
		} else {
			b.PutByte(0)
		}
	}

	if proto.FeatureParallelReplicas.In(revision) {
		if info.CollaborateWithInitiator {
			b.PutInt(1)
		} else {
			b.PutInt(0)
		}
		b.PutInt(info.CountParticipatingReplicas)
		b.PutInt(info.NumberOfCurrentReplica)
	}

	if proto.FeatureQueryAndLineNumbers.In(revision) {
		b.PutUVarInt(q.ScriptQueryNumber)
		b.PutUVarInt(q.ScriptLineNumber)
	}

	if proto.Feature(54476).In(revision) {
		if q.HasJWT {
			b.PutByte(1)
			b.PutString(q.JWT)
		} else {
			b.PutByte(0)
		}
	}
}

func decodeFieldErr(field string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("decode %s: %w", field, err)
}

// decodeQuery is the codec method invoked by ReadPacket. Requires
// SetRevision because Query's wire layout is revision-sensitive.
func (c *Codec) decodeQuery() (*Query, error) {
	rev := c.Revision()
	if rev == 0 {
		return nil, fmt.Errorf("%w: Query decode requires SetRevision first", ErrMalformed)
	}
	q, err := DecodeQuery(c.r, rev)
	if err != nil {
		return nil, fmt.Errorf("%w: query: %v", ErrDecode, err)
	}
	return q, nil
}

// WriteQuery encodes a Query (with the leading type byte) and writes it
// to the wire at the codec's current negotiated revision.
func (c *Codec) WriteQuery(q *Query) error {
	rev := c.Revision()
	if rev == 0 {
		return fmt.Errorf("%w: WriteQuery requires SetRevision first", ErrMalformed)
	}
	var buf proto.Buffer
	EncodeQuery(&buf, q, rev)
	_, err := c.w.Write(buf.Buf)
	return err
}
