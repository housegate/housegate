package proxy

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
)

func TestClientPacketName_KnownCodes(t *testing.T) {
	cases := []struct {
		code uint64
		want string
	}{
		{uint64(chproto.ClientHelloCode), "Hello"},
		{uint64(chproto.ClientQueryCode), "Query"},
		{uint64(chproto.ClientDataCode), "Data"},
		{uint64(chproto.ClientCancelCode), "Cancel"},
		{uint64(chproto.ClientPingCode), "Ping"},
		{uint64(chproto.ClientScalar), "Scalar"},
		{uint64(chproto.ClientKeepAlive), "KeepAlive"},
		{uint64(chproto.ClientIgnoredPartUUIDs), "IgnoredPartUUIDs"},
		{uint64(chproto.ClientReadTaskResponse), "ReadTaskResponse"},
		{uint64(chproto.ClientClusterFunctionReadTaskResp), "ClusterFunctionReadTaskResponse"},
	}
	for _, c := range cases {
		if got := clientPacketName(c.code); got != c.want {
			t.Errorf("clientPacketName(%d)=%q, want %q", c.code, got, c.want)
		}
	}
}

func TestClientPacketName_UnknownCodeWithinLowRange_FormatsType(t *testing.T) {
	got := clientPacketName(12)
	if got != "type_12" {
		t.Errorf("clientPacketName(12)=%q, want type_12", got)
	}
}

func TestClientPacketName_OutOfRange_ReturnsUnknown(t *testing.T) {
	got := clientPacketName(200)
	if got != "unknown" {
		t.Errorf("clientPacketName(200)=%q, want unknown", got)
	}
}

func TestCompressionMode(t *testing.T) {
	if got := compressionMode(proto.CompressionEnabled); got != "compressed" {
		t.Errorf("compressionMode(Enabled)=%q, want compressed", got)
	}
	if got := compressionMode(proto.CompressionDisabled); got != "uncompressed" {
		t.Errorf("compressionMode(Disabled)=%q, want uncompressed", got)
	}
}
