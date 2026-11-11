package storageintegrity

import (
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// PayloadEncodingClickHouseNativeData is the SI lane's canonical payload
// format; the decoder itself lives in pkg/replay/nativepayload.
const PayloadEncodingClickHouseNativeData = nativepayload.PayloadFormat

// ErrNativePayloadUnsupported aliases nativepayload.ErrUnsupported.
var ErrNativePayloadUnsupported = nativepayload.ErrUnsupported

// ErrDateTime64OutOfRange aliases nativepayload.ErrDateTime64OutOfRange: intake
// refuses a DateTime64 value outside the int64-nanosecond range (#343).
var ErrDateTime64OutOfRange = nativepayload.ErrDateTime64OutOfRange

// NativeMaterializer aliases nativepayload.Materializer.
type NativeMaterializer = nativepayload.Materializer

// DecodeNativePayload aliases nativepayload.Decode.
func DecodeNativePayload(schema payloadexec.TableSchema, revision int, payload []byte) ([]payloadexec.Row, error) {
	return nativepayload.Decode(schema, revision, payload)
}

// ValidateNativePayloadDecodable aliases nativepayload.ValidateDecodable.
func ValidateNativePayloadDecodable(schema payloadexec.TableSchema, revision int, payload []byte) error {
	return nativepayload.ValidateDecodable(schema, revision, payload)
}
