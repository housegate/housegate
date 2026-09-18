package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
)

type buildIdentityFixture struct {
	NativeArtifactSetsJSON     []string `json:"native_artifact_sets_json"`
	NativeAnalyzerBuildDigests []string `json:"native_analyzer_build_digests"`
	QueryProfileRecordsJSON    []string `json:"query_profile_records_json"`
	QueryProfileIDs            []string `json:"query_profile_ids"`
	ProfileFileJSON            string   `json:"profile_file_json"`
	ProfileFileSHA256          string   `json:"profile_file_sha256"`
}

func buildIdentityVector(t *testing.T) buildIdentityFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/snapshot_query_build_identity_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture buildIdentityFixture
	if err := json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func independentBuildIdentityDigest(domain string, canonical []byte) string {
	sum := sha256.Sum256(append([]byte("housegate-replay-mvp-v0:"+domain+"\x00"), canonical...))
	return "0x" + hex.EncodeToString(sum[:])
}

func cloneProfileFile(t *testing.T, in QueryProfileFileV1) QueryProfileFileV1 {
	t.Helper()
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out QueryProfileFileV1
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func validProfileFile(t *testing.T) (buildIdentityFixture, QueryProfileFileV1) {
	t.Helper()
	fixture := buildIdentityVector(t)
	file, err := DecodeCanonicalQueryProfileFileV1([]byte(fixture.ProfileFileJSON))
	if err != nil {
		t.Fatal(err)
	}
	return fixture, file
}

func TestSnapshotQueryBuildIdentityFrozenVectors(t *testing.T) {
	// These are synthetic cross-implementation contract vectors. They do not
	// identify a measured, installed, qualified, or production analyzer build.
	fixture, file := validProfileFile(t)
	if len(file.Profiles) != 2 {
		t.Fatalf("profiles=%d want=2", len(file.Profiles))
	}
	fileSum := sha256.Sum256([]byte(fixture.ProfileFileJSON))
	if got := hex.EncodeToString(fileSum[:]); got != fixture.ProfileFileSHA256 {
		t.Fatalf("profile file SHA-256=%s want=%s", got, fixture.ProfileFileSHA256)
	}
	encoded, err := EncodeCanonicalQueryProfileFileV1(file)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, []byte(fixture.ProfileFileJSON)) {
		t.Fatal("profile file canonical bytes changed")
	}
	for i, entry := range file.Profiles {
		setBytes, err := EncodeCanonicalNativeArtifactSetV1(entry.NativeArtifactSet)
		if err != nil {
			t.Fatal(err)
		}
		if string(setBytes) != fixture.NativeArtifactSetsJSON[i] {
			t.Fatalf("artifact set %d canonical bytes changed", i)
		}
		n, err := NativeArtifactSetV1Digest(entry.NativeArtifactSet)
		if err != nil {
			t.Fatal(err)
		}
		if n != fixture.NativeAnalyzerBuildDigests[i] || n != independentBuildIdentityDigest("snapshot-native-analyzer-artifact-set-v1", setBytes) {
			t.Fatalf("artifact set %d digest=%s", i, n)
		}
		recordBytes, err := json.Marshal(entry.Record)
		if err != nil {
			t.Fatal(err)
		}
		if string(recordBytes) != fixture.QueryProfileRecordsJSON[i] {
			t.Fatalf("record %d canonical bytes changed", i)
		}
		q, err := entry.Record.Hash()
		if err != nil {
			t.Fatal(err)
		}
		if q != fixture.QueryProfileIDs[i] || q != independentBuildIdentityDigest("snapshot-query-profile-v1", recordBytes) {
			t.Fatalf("profile %d digest=%s", i, q)
		}
	}
	if file.Profiles[0].Record.Platform == file.Profiles[0].NativeArtifactSet.Artifacts[0].Platform {
		t.Fatal("fixture must prove analyzer-host platform is independent of SQL platform")
	}
}

func TestSnapshotQueryBuildIdentityValueValidation(t *testing.T) {
	_, file := validProfileFile(t)
	baseSet := file.Profiles[0].NativeArtifactSet
	for _, test := range []struct {
		name   string
		mutate func(*NativeArtifactSetV1)
	}{
		{"version_zero", func(s *NativeArtifactSetV1) { s.Version = 0 }},
		{"version_future", func(s *NativeArtifactSetV1) { s.Version = 2 }},
		{"nil_artifacts", func(s *NativeArtifactSetV1) { s.Artifacts = nil }},
		{"empty_artifacts", func(s *NativeArtifactSetV1) { s.Artifacts = []NativeArtifactV1{} }},
		{"blank_platform", func(s *NativeArtifactSetV1) { s.Artifacts[0].Platform = " " }},
		{"short_executable_digest", func(s *NativeArtifactSetV1) { s.Artifacts[0].ExecutableDigest = "0x12" }},
		{"uppercase_executable_digest", func(s *NativeArtifactSetV1) { s.Artifacts[0].ExecutableDigest = "0x" + strings.Repeat("A", 64) }},
		{"bare_ffi_digest", func(s *NativeArtifactSetV1) { s.Artifacts[0].FFIDigest = strings.Repeat("2", 64) }},
		{"duplicate", func(s *NativeArtifactSetV1) { s.Artifacts[1] = s.Artifacts[0] }},
		{"out_of_order", func(s *NativeArtifactSetV1) { s.Artifacts[0], s.Artifacts[1] = s.Artifacts[1], s.Artifacts[0] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			set := baseSet
			set.Artifacts = append([]NativeArtifactV1(nil), set.Artifacts...)
			test.mutate(&set)
			if err := ValidateNativeArtifactSetV1(set); err == nil {
				t.Fatal("malformed artifact set accepted")
			}
			if _, err := EncodeCanonicalNativeArtifactSetV1(set); err == nil {
				t.Fatal("malformed artifact set encoded")
			}
			if _, err := NativeArtifactSetV1Digest(set); err == nil {
				t.Fatal("malformed artifact set hashed")
			}
		})
	}

	for _, test := range []struct {
		name   string
		mutate func(*QueryProfileFileV1)
	}{
		{"version_zero", func(f *QueryProfileFileV1) { f.Version = 0 }},
		{"version_future", func(f *QueryProfileFileV1) { f.Version = 2 }},
		{"nil_profiles", func(f *QueryProfileFileV1) { f.Profiles = nil }},
		{"empty_profiles", func(f *QueryProfileFileV1) { f.Profiles = []QueryProfileEntryV1{} }},
		{"duplicate_profile", func(f *QueryProfileFileV1) { f.Profiles[1] = f.Profiles[0] }},
		{"out_of_order", func(f *QueryProfileFileV1) { f.Profiles[0], f.Profiles[1] = f.Profiles[1], f.Profiles[0] }},
		{"invalid_query_profile_id", func(f *QueryProfileFileV1) { f.Profiles[0].QueryProfileID = "0x12" }},
		{"mismatched_query_profile_id", func(f *QueryProfileFileV1) {
			f.Profiles[0].QueryProfileID = "0x0643484a1c64d5325096e8bb0295f8931b4294c0d2c05dc8b644afb0bc48d11d"
		}},
		{"mismatched_native_digest", func(f *QueryProfileFileV1) {
			f.Profiles[0].Record.NativeAnalyzerBuildDigest = "0x" + strings.Repeat("9", 64)
		}},
	} {
		t.Run("file/"+test.name, func(t *testing.T) {
			bad := cloneProfileFile(t, file)
			test.mutate(&bad)
			if err := ValidateQueryProfileFileV1(bad); err == nil {
				t.Fatal("malformed profile file accepted")
			}
			if _, err := EncodeCanonicalQueryProfileFileV1(bad); err == nil {
				t.Fatal("malformed profile file encoded")
			}
		})
	}
}

func TestSnapshotQueryBuildIdentityArtifactMutationsBreakCommitment(t *testing.T) {
	_, file := validProfileFile(t)
	for _, test := range []struct {
		name   string
		mutate func(*NativeArtifactV1)
	}{
		{"platform", func(a *NativeArtifactV1) { a.Platform = "darwin/arm65" }},
		{"executable", func(a *NativeArtifactV1) { a.ExecutableDigest = "0x" + strings.Repeat("1", 63) + "2" }},
		{"ffi", func(a *NativeArtifactV1) { a.FFIDigest = "0x" + strings.Repeat("2", 63) + "3" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := cloneProfileFile(t, file)
			test.mutate(&bad.Profiles[0].NativeArtifactSet.Artifacts[0])
			if err := ValidateQueryProfileFileV1(bad); err == nil {
				t.Fatal("artifact mutation retained native commitment")
			}
		})
	}
	bad := cloneProfileFile(t, file)
	bad.Profiles[0].NativeArtifactSet.Artifacts[0].ExecutableDigest = "0x" + strings.Repeat("1", 63) + "2"
	n, err := NativeArtifactSetV1Digest(bad.Profiles[0].NativeArtifactSet)
	if err != nil {
		t.Fatal(err)
	}
	bad.Profiles[0].Record.NativeAnalyzerBuildDigest = n
	if err := ValidateQueryProfileFileV1(bad); err == nil {
		t.Fatal("updated N without updated Q accepted")
	}
}

func TestSnapshotQueryBuildIdentityRejectsMalformedRecord(t *testing.T) {
	_, file := validProfileFile(t)
	for _, test := range []struct {
		name   string
		mutate func(*QueryProfileRecord)
	}{
		{"version", func(r *QueryProfileRecord) { r.Version = 2 }},
		{"clickhouse_digest", func(r *QueryProfileRecord) { r.ClickHouseBuildDigest = "0x12" }},
		{"grpc_digest", func(r *QueryProfileRecord) { r.GRPCAnalyzerBuildDigest = "0x" + strings.Repeat("A", 64) }},
		{"tzdata_digest", func(r *QueryProfileRecord) { r.TZDataDigest = "" }},
		{"platform", func(r *QueryProfileRecord) { r.Platform = " " }},
		{"column_profile", func(r *QueryProfileRecord) { r.ColumnProfileID = "" }},
		{"output_order", func(r *QueryProfileRecord) { r.OutputOrderID = "" }},
		{"setting_name", func(r *QueryProfileRecord) { r.Settings[0].Name = "" }},
		{"duplicate_setting", func(r *QueryProfileRecord) { r.Settings[1] = r.Settings[0] }},
		{"out_of_order_settings", func(r *QueryProfileRecord) { r.Settings[0], r.Settings[1] = r.Settings[1], r.Settings[0] }},
		{"duplicate_operator", func(r *QueryProfileRecord) { r.ScalarOperators[1] = r.ScalarOperators[0] }},
		{"out_of_order_operators", func(r *QueryProfileRecord) {
			r.ScalarOperators[0], r.ScalarOperators[1] = r.ScalarOperators[1], r.ScalarOperators[0]
		}},
		{"zero_limit", func(r *QueryProfileRecord) { r.Limits.MaxOutputRows = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := cloneProfileFile(t, file)
			test.mutate(&bad.Profiles[0].Record)
			if err := ValidateQueryProfileFileV1(bad); err == nil {
				t.Fatal("malformed embedded QueryProfileRecord accepted")
			}
		})
	}
}

func TestSnapshotQueryBuildIdentityStrictJSONShape(t *testing.T) {
	fixture := buildIdentityVector(t)
	raw := []byte(fixture.ProfileFileJSON)
	var root any
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatal(err)
	}
	var walk func(any, string)
	walk = func(value any, path string) {
		switch node := value.(type) {
		case map[string]any:
			keys := make([]string, 0, len(node))
			for key := range node {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for _, key := range keys {
				old := node[key]
				delete(node, key)
				encoded, _ := json.Marshal(root)
				if _, err := DecodeCanonicalQueryProfileFileV1(encoded); err == nil {
					t.Fatalf("%s.%s omission accepted", path, key)
				}
				node[key] = nil
				encoded, _ = json.Marshal(root)
				if _, err := DecodeCanonicalQueryProfileFileV1(encoded); err == nil {
					t.Fatalf("%s.%s null accepted", path, key)
				}
				node[key] = old
				walk(old, path+"."+key)
			}
			node["unexpected"] = 1
			encoded, _ := json.Marshal(root)
			if _, err := DecodeCanonicalQueryProfileFileV1(encoded); err == nil {
				t.Fatalf("%s unknown field accepted", path)
			}
			delete(node, "unexpected")
		case []any:
			for i, item := range node {
				walk(item, fmt.Sprintf("%s[%d]", path, i))
			}
		}
	}
	walk(root, "file")

	duplicates := []struct {
		name string
		old  string
		new  string
	}{
		{"file", `{"version":1,"profiles":`, `{"version":1,"version":1,"profiles":`},
		{"entry", `{"query_profile_id":"0x0543484a1c64d5325096e8bb0295f8931b4294c0d2c05dc8b644afb0bc48d11d","record":`, `{"query_profile_id":"0x0543484a1c64d5325096e8bb0295f8931b4294c0d2c05dc8b644afb0bc48d11d","query_profile_id":"0x0543484a1c64d5325096e8bb0295f8931b4294c0d2c05dc8b644afb0bc48d11d","record":`},
		{"record", `"record":{"version":1,"clickhouse_build_digest":`, `"record":{"version":1,"version":1,"clickhouse_build_digest":`},
		{"setting", `{"name":"max_threads","value":"1"}`, `{"name":"max_threads","name":"max_threads","value":"1"}`},
		{"limits", `"limits":{"max_sql_bytes":65537,`, `"limits":{"max_sql_bytes":65537,"max_sql_bytes":65537,`},
		{"set", `"native_artifact_set":{"version":1,"artifacts":`, `"native_artifact_set":{"version":1,"version":1,"artifacts":`},
		{"artifact", `{"platform":"darwin/arm64","executable_digest":`, `{"platform":"darwin/arm64","platform":"darwin/arm64","executable_digest":`},
	}
	for _, test := range duplicates {
		t.Run("duplicate/"+test.name, func(t *testing.T) {
			mutated := bytes.Replace(raw, []byte(test.old), []byte(test.new), 1)
			if bytes.Equal(mutated, raw) {
				t.Fatal("duplicate mutation did not match")
			}
			if _, err := DecodeCanonicalQueryProfileFileV1(mutated); err == nil {
				t.Fatal("duplicate field accepted")
			}
		})
	}
}

func TestSnapshotQueryBuildIdentityRejectsNoncanonicalBytes(t *testing.T) {
	fixture := buildIdentityVector(t)
	raw := []byte(fixture.ProfileFileJSON)
	var generic any
	if err := json.Unmarshal(raw, &generic); err != nil {
		t.Fatal(err)
	}
	pretty, err := json.MarshalIndent(generic, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	reordered := bytes.Replace(raw, []byte(`{"version":1,"profiles":`), []byte(`{"profiles":`), 1)
	reordered = append(reordered[:len(reordered)-1], []byte(`,"version":1}`)...)
	for _, mutated := range [][]byte{
		append([]byte(" "), raw...),
		append(append([]byte(nil), raw...), '\n'),
		pretty,
		reordered,
		bytes.Replace(raw, []byte("linux/amd64"), []byte(`\u006cinux/amd64`), 1),
	} {
		if _, err := DecodeCanonicalQueryProfileFileV1(mutated); err == nil {
			t.Fatal("noncanonical profile bytes accepted")
		}
	}
}

func TestSnapshotQueryBuildIdentityDoesNotMutateCallers(t *testing.T) {
	_, file := validProfileFile(t)
	before, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateQueryProfileFileV1(file); err != nil {
		t.Fatal(err)
	}
	if _, err := EncodeCanonicalQueryProfileFileV1(file); err != nil {
		t.Fatal(err)
	}
	for _, entry := range file.Profiles {
		if err := ValidateNativeArtifactSetV1(entry.NativeArtifactSet); err != nil {
			t.Fatal(err)
		}
		if _, err := EncodeCanonicalNativeArtifactSetV1(entry.NativeArtifactSet); err != nil {
			t.Fatal(err)
		}
		if _, err := NativeArtifactSetV1Digest(entry.NativeArtifactSet); err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Record.Hash(); err != nil {
			t.Fatal(err)
		}
	}
	after, err := json.Marshal(file)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("validation or hashing mutated caller-owned values")
	}
}
