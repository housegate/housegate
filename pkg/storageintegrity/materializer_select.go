package storageintegrity

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// EncodingCSVWithNames names the CSVWithNames wire payload encoding. It sits
// beside PayloadEncodingClickHouseNativeData (native_payload.go) so the runtime
// selects a materializer by an explicit encoding string rather than defaulting
// silently.
const EncodingCSVWithNames = payloadexec.PayloadFormatCSVWithNames

// MaterializerKind is the replay materializer family used to re-produce rows
// from a captured payload. The built-in runtime pins MaterializerNative;
// MaterializerCSV remains available for payloadexec test/legacy payloads.
type MaterializerKind int

const (
	// MaterializerUnspecified is the zero value; never a valid selection.
	MaterializerUnspecified MaterializerKind = iota
	// MaterializerNative decodes an uncompressed ClickHouse Native block
	// (payloadexec/chexec Native path).
	MaterializerNative
	// MaterializerCSV decodes a CSVWithNames payload (payloadexec csv path).
	MaterializerCSV
)

// SelectMaterializerKind maps a pinned payload encoding to the materializer that
// must decode it. It is a pure, deterministic selection and fails closed on an
// unrecognized encoding: a Native payload must never be mis-decoded as CSV, so
// an unknown non-empty encoding is an error rather than a silent CSV default.
// An empty encoding maps to CSV — the legacy default wire shape — deliberately.
func SelectMaterializerKind(encoding string) (MaterializerKind, error) {
	switch encoding {
	case PayloadEncodingClickHouseNativeData:
		return MaterializerNative, nil
	case "", EncodingCSVWithNames:
		return MaterializerCSV, nil
	default:
		return MaterializerUnspecified, fmt.Errorf("storageintegrity: unrecognized payload encoding %q; refusing to guess a materializer", encoding)
	}
}
