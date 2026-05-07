package chproto

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// TestQueryCodecRoundTrip_LowRevision 测试低版本客户端(54412)的 Query 编解码往返。
// revision=54412 低于 FeatureSettingsSerializedAsStrings(54429)，使用旧格式 Settings。
func TestQueryCodecRoundTrip_LowRevision(t *testing.T) {
	revision := 54412

	original := &Query{}
	original.ID = "test-query-id-low"
	// revision < 54429：使用旧格式 OldSettings [Key: String][Value: UInt64]
	original.OldSettings = []OldSetting{
		{Key: "max_threads", Value: 4},
		{Key: "max_memory_usage", Value: 1000000},
	}
	original.Stage = proto.StageComplete
	original.Compression = proto.CompressionDisabled
	original.Body = "SELECT * FROM sentio_remote_orders.orders"

	// 编码
	buf := &proto.Buffer{}
	EncodeQuery(buf, original, revision)

	// 解码（跳过 ClientCodeQuery 类型字节）
	data := buf.Buf
	if len(data) == 0 {
		t.Fatal("encoded buffer is empty")
	}
	if data[0] != byte(proto.ClientCodeQuery) {
		t.Fatalf("expected ClientCodeQuery byte %d, got %d", proto.ClientCodeQuery, data[0])
	}
	data = data[1:]

	r := proto.NewReader(bytes.NewReader(data))
	decoded, err := DecodeQuery(r, revision)
	if err != nil {
		t.Fatalf("DecodeQuery error: %v", err)
	}

	// 验证字段
	if decoded.ID != original.ID {
		t.Errorf("QueryID: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Body != original.Body {
		t.Errorf("Body: got %q, want %q", decoded.Body, original.Body)
	}
	if decoded.Stage != original.Stage {
		t.Errorf("Stage: got %d, want %d", decoded.Stage, original.Stage)
	}
	if decoded.Compression != original.Compression {
		t.Errorf("Compression: got %d, want %d", decoded.Compression, original.Compression)
	}
	// 旧格式 Settings 验证
	if len(decoded.OldSettings) != len(original.OldSettings) {
		t.Errorf("OldSettings count: got %d, want %d", len(decoded.OldSettings), len(original.OldSettings))
	}
	for i, s := range decoded.OldSettings {
		if s.Key != original.OldSettings[i].Key || s.Value != original.OldSettings[i].Value {
			t.Errorf("OldSetting[%d]: got {%q, %d}, want {%q, %d}", i, s.Key, s.Value, original.OldSettings[i].Key, original.OldSettings[i].Value)
		}
	}
	// 新格式 Settings 应为空
	if len(decoded.Settings) != 0 {
		t.Errorf("Settings should be empty for low revision, got %d", len(decoded.Settings))
	}
	// 验证低版本不包含的字段应为零值
	if decoded.Secret != "" {
		t.Errorf("Secret should be empty for low revision, got %q", decoded.Secret)
	}
	if decoded.ExternalRoles != "" {
		t.Errorf("ExternalRoles should be empty for low revision, got %q", decoded.ExternalRoles)
	}
	if len(decoded.Parameters) != 0 {
		t.Errorf("Parameters should be empty for low revision, got %d", len(decoded.Parameters))
	}
}

// TestQueryCodecRoundTrip_HighRevision 测试高版本客户端(54476)的 Query 编解码往返。
// revision=54476 满足所有 Feature 门槛（除了 FeatureChunkedPackets 54470 和 FeatureVersionedParallelReplicas 54471
// 以及 FeatureJSONStrings 54475），应包含所有条件字段。
func TestQueryCodecRoundTrip_HighRevision(t *testing.T) {
	revision := 54476

	original := &Query{}
	original.ID = "test-query-id-high"
	original.Info = proto.ClientInfo{
		Query:            proto.ClientQueryInitial,
		InitialUser:      "default",
		InitialQueryID:   "init-qid",
		InitialAddress:   "127.0.0.1:12345",
		InitialTime:      1234567890,
		Interface:        proto.InterfaceTCP,
		OSUser:           "testuser",
		ClientHostname:   "testhost",
		ClientName:       "test-client",
		Major:            24,
		Minor:            1,
		ProtocolVersion:  54476,
		QuotaKey:         "test-quota",
		DistributedDepth: 0,
		Patch:            100,
	}
	original.Settings = []proto.Setting{
		{Key: "max_threads", Value: "8"},
	}
	original.ExternalRoles = "test-roles"
	original.Secret = "test-secret"
	original.Stage = proto.StageComplete
	original.Compression = proto.CompressionEnabled
	original.Body = "SELECT 1"
	original.Parameters = []proto.Parameter{
		{Key: "param1", Value: "value1"},
	}
	original.ScriptQueryNumber = 1
	original.ScriptLineNumber = 42
	original.HasJWT = false

	// 编码
	buf := &proto.Buffer{}
	EncodeQuery(buf, original, revision)

	// 解码（跳过 ClientCodeQuery 字节）
	data := buf.Buf[1:]
	r := proto.NewReader(bytes.NewReader(data))
	decoded, err := DecodeQuery(r, revision)
	if err != nil {
		t.Fatalf("DecodeQuery error: %v", err)
	}

	// 验证所有字段
	if decoded.ID != original.ID {
		t.Errorf("QueryID: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Info.InitialUser != original.Info.InitialUser {
		t.Errorf("InitialUser: got %q, want %q", decoded.Info.InitialUser, original.Info.InitialUser)
	}
	if decoded.Info.OSUser != original.Info.OSUser {
		t.Errorf("OSUser: got %q, want %q", decoded.Info.OSUser, original.Info.OSUser)
	}
	if decoded.Info.ClientName != original.Info.ClientName {
		t.Errorf("ClientName: got %q, want %q", decoded.Info.ClientName, original.Info.ClientName)
	}
	if decoded.Info.Patch != original.Info.Patch {
		t.Errorf("Patch: got %d, want %d", decoded.Info.Patch, original.Info.Patch)
	}
	if decoded.ExternalRoles != original.ExternalRoles {
		t.Errorf("ExternalRoles: got %q, want %q", decoded.ExternalRoles, original.ExternalRoles)
	}
	if decoded.Secret != original.Secret {
		t.Errorf("Secret: got %q, want %q", decoded.Secret, original.Secret)
	}
	if decoded.Stage != original.Stage {
		t.Errorf("Stage: got %d, want %d", decoded.Stage, original.Stage)
	}
	if decoded.Body != original.Body {
		t.Errorf("Body: got %q, want %q", decoded.Body, original.Body)
	}
	if len(decoded.Parameters) != len(original.Parameters) {
		t.Fatalf("Parameters count: got %d, want %d", len(decoded.Parameters), len(original.Parameters))
	}
	if decoded.Parameters[0].Key != "param1" || decoded.Parameters[0].Value != "value1" {
		t.Errorf("Parameter: got {%q, %q}, want {%q, %q}", decoded.Parameters[0].Key, decoded.Parameters[0].Value, "param1", "value1")
	}
	if decoded.ScriptQueryNumber != original.ScriptQueryNumber {
		t.Errorf("ScriptQueryNumber: got %d, want %d", decoded.ScriptQueryNumber, original.ScriptQueryNumber)
	}
	if decoded.ScriptLineNumber != original.ScriptLineNumber {
		t.Errorf("ScriptLineNumber: got %d, want %d", decoded.ScriptLineNumber, original.ScriptLineNumber)
	}
}

// TestQueryCodecRoundTrip_ByteLevel 验证编码-解码往返后二进制内容完全一致。
func TestQueryCodecRoundTrip_ByteLevel(t *testing.T) {
	revisions := []int{54412, 54420, 54429, 54441, 54459, 54472, 54476}

	for _, rev := range revisions {
		t.Run("rev_"+strconv.Itoa(rev), func(t *testing.T) {
			original := &Query{}
			original.ID = "byte-test"
			original.Stage = proto.StageComplete
			original.Compression = proto.CompressionDisabled
			original.Body = "SELECT 42"

			if proto.FeatureClientWriteInfo.In(rev) {
				original.Info = proto.ClientInfo{
					Query:           proto.ClientQueryInitial,
					InitialUser:     "user1",
					InitialQueryID:  "qid1",
					InitialAddress:  "127.0.0.1:9000",
					Interface:       proto.InterfaceTCP,
					OSUser:          "os1",
					ClientHostname:  "host1",
					ClientName:      "client1",
					Major:           1,
					Minor:           2,
					ProtocolVersion: rev,
				}
			}
			if proto.FeatureInterServerSecret.In(rev) {
				original.Secret = "s3cret"
			}
			if proto.FeatureInterserverExternallyGrantedRoles.In(rev) {
				original.ExternalRoles = "ext-roles"
			}
			if proto.FeatureParameters.In(rev) {
				original.Parameters = []proto.Parameter{
					{Key: "p1", Value: "v1"},
				}
			}

			// 第一次编码
			buf1 := &proto.Buffer{}
			EncodeQuery(buf1, original, rev)

			// 解码
			data := buf1.Buf[1:] // 跳过 ClientCodeQuery
			r := proto.NewReader(bytes.NewReader(data))
			decoded, err := DecodeQuery(r, rev)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}

			// 第二次编码
			buf2 := &proto.Buffer{}
			EncodeQuery(buf2, decoded, rev)

			// 二进制比较
			if !bytes.Equal(buf1.Buf, buf2.Buf) {
				t.Errorf("byte-level mismatch at revision %d\n  first:  %x\n  second: %x", rev, buf1.Buf, buf2.Buf)
			}
		})
	}
}

// TestQueryCodecRoundTrip_StageNotHardcoded 验证 Stage 不会被硬编码为 StageComplete。
func TestQueryCodecRoundTrip_StageNotHardcoded(t *testing.T) {
	stages := []proto.Stage{
		proto.StageFetchColumns,
		proto.StageWithMergeableState,
		proto.StageComplete,
	}

	for _, stage := range stages {
		t.Run("stage_"+strconv.Itoa(int(stage)), func(t *testing.T) {
			original := &Query{}
			original.ID = "stage-test"
			original.Stage = stage
			original.Compression = proto.CompressionDisabled
			original.Body = "SELECT 1"

			buf := &proto.Buffer{}
			EncodeQuery(buf, original, 54412)

			data := buf.Buf[1:]
			r := proto.NewReader(bytes.NewReader(data))
			decoded, err := DecodeQuery(r, 54412)
			if err != nil {
				t.Fatalf("decode error: %v", err)
			}
			if decoded.Stage != stage {
				t.Errorf("Stage: got %d, want %d", decoded.Stage, stage)
			}
		})
	}
}

// TestQueryCodecRoundTrip_EmptyQuery 验证空 SQL 的处理。
func TestQueryCodecRoundTrip_EmptyQuery(t *testing.T) {
	original := &Query{}
	original.ID = ""
	original.Stage = proto.StageComplete
	original.Compression = proto.CompressionDisabled
	original.Body = ""

	buf := &proto.Buffer{}
	EncodeQuery(buf, original, 54412)

	data := buf.Buf[1:]
	r := proto.NewReader(bytes.NewReader(data))
	decoded, err := DecodeQuery(r, 54412)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.Body != "" {
		t.Errorf("Body: got %q, want empty", decoded.Body)
	}
}
