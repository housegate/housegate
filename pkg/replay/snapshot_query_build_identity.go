package replay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
)

const (
	// NativeArtifactSetV1Version is the only canonical native artifact-set version.
	NativeArtifactSetV1Version uint32 = 1
	// QueryProfileFileV1Version is the only canonical external profile-file version.
	QueryProfileFileV1Version uint32 = 1
)

const (
	nativeArtifactSetV1Domain   = "snapshot-native-analyzer-artifact-set-v1"
	queryProfileRecordV1Version = 1
)

// NativeArtifactV1 identifies the final executable and FFI bytes for one
// analyzer host platform. It contains measured values; it performs no host
// measurement or local capability check itself.
type NativeArtifactV1 struct {
	Platform         string `json:"platform"`
	ExecutableDigest string `json:"executable_digest"`
	FFIDigest        string `json:"ffi_digest"`
}

// NativeArtifactSetV1 is the ordered witness committed by
// QueryProfileRecord.NativeAnalyzerBuildDigest.
type NativeArtifactSetV1 struct {
	Version   uint32             `json:"version"`
	Artifacts []NativeArtifactV1 `json:"artifacts"`
}

// QueryProfileEntryV1 binds an existing query-profile record to the native
// artifacts whose canonical commitment appears in that record.
type QueryProfileEntryV1 struct {
	QueryProfileID    string              `json:"query_profile_id"`
	Record            QueryProfileRecord  `json:"record"`
	NativeArtifactSet NativeArtifactSetV1 `json:"native_artifact_set"`
}

// QueryProfileFileV1 is the canonical external query-profile witness.
type QueryProfileFileV1 struct {
	Version  uint32                `json:"version"`
	Profiles []QueryProfileEntryV1 `json:"profiles"`
}

func validSHA256Digest(digest string) bool {
	if len(digest) != 66 || digest[0] != '0' || digest[1] != 'x' {
		return false
	}
	for _, c := range digest[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func compareNativeArtifact(a, b NativeArtifactV1) int {
	if a.Platform != b.Platform {
		return strings.Compare(a.Platform, b.Platform)
	}
	if a.ExecutableDigest != b.ExecutableDigest {
		return strings.Compare(a.ExecutableDigest, b.ExecutableDigest)
	}
	return strings.Compare(a.FFIDigest, b.FFIDigest)
}

// ValidateNativeArtifactSetV1 validates the exact ordered value contract. It
// rejects values that would require sorting or normalization and never mutates
// the caller's slice.
func ValidateNativeArtifactSetV1(set NativeArtifactSetV1) error {
	if set.Version != NativeArtifactSetV1Version {
		return fmt.Errorf("native artifact set version must be %d", NativeArtifactSetV1Version)
	}
	if len(set.Artifacts) == 0 {
		return fmt.Errorf("native artifact set artifacts must be nonempty")
	}
	for i, artifact := range set.Artifacts {
		if strings.TrimSpace(artifact.Platform) == "" || strings.TrimSpace(artifact.Platform) != artifact.Platform {
			return fmt.Errorf("native artifact %d platform is required and must be canonical", i)
		}
		if !validSHA256Digest(artifact.ExecutableDigest) {
			return fmt.Errorf("native artifact %d executable_digest must be lowercase 0x SHA-256", i)
		}
		if !validSHA256Digest(artifact.FFIDigest) {
			return fmt.Errorf("native artifact %d ffi_digest must be lowercase 0x SHA-256", i)
		}
		if i > 0 {
			comparison := compareNativeArtifact(set.Artifacts[i-1], artifact)
			if comparison == 0 {
				return fmt.Errorf("duplicate native artifact at index %d", i)
			}
			if comparison > 0 {
				return fmt.Errorf("native artifacts are not sorted at index %d", i)
			}
		}
	}
	return nil
}

// EncodeCanonicalNativeArtifactSetV1 returns the exact ordered JSON bytes used
// by NativeArtifactSetV1Digest.
func EncodeCanonicalNativeArtifactSetV1(set NativeArtifactSetV1) ([]byte, error) {
	if err := ValidateNativeArtifactSetV1(set); err != nil {
		return nil, err
	}
	return json.Marshal(set)
}

// NativeArtifactSetV1Digest returns N, the native analyzer build commitment.
func NativeArtifactSetV1Digest(set NativeArtifactSetV1) (string, error) {
	if err := ValidateNativeArtifactSetV1(set); err != nil {
		return "", err
	}
	return CanonicalDigest(nativeArtifactSetV1Domain, set)
}

func validateCanonicalQueryProfileRecord(record QueryProfileRecord) error {
	if record.Version != queryProfileRecordV1Version {
		return fmt.Errorf("query profile record version must be %d", queryProfileRecordV1Version)
	}
	for _, field := range []struct {
		name   string
		digest string
	}{
		{"clickhouse_build_digest", record.ClickHouseBuildDigest},
		{"native_analyzer_build_digest", record.NativeAnalyzerBuildDigest},
		{"grpc_analyzer_build_digest", record.GRPCAnalyzerBuildDigest},
		{"tzdata_digest", record.TZDataDigest},
	} {
		if !validSHA256Digest(field.digest) {
			return fmt.Errorf("query profile %s must be lowercase 0x SHA-256", field.name)
		}
	}
	if strings.TrimSpace(record.Platform) == "" || strings.TrimSpace(record.Platform) != record.Platform {
		return fmt.Errorf("query profile platform is required and must be canonical")
	}
	if strings.TrimSpace(record.ColumnProfileID) == "" || strings.TrimSpace(record.ColumnProfileID) != record.ColumnProfileID {
		return fmt.Errorf("query profile column_profile_id is required and must be canonical")
	}
	if strings.TrimSpace(record.OutputOrderID) == "" || strings.TrimSpace(record.OutputOrderID) != record.OutputOrderID {
		return fmt.Errorf("query profile output_order_id is required and must be canonical")
	}
	for i, setting := range record.Settings {
		if strings.TrimSpace(setting.Name) == "" || strings.TrimSpace(setting.Name) != setting.Name {
			return fmt.Errorf("query profile setting %d name is required and must be canonical", i)
		}
		if i > 0 && record.Settings[i-1].Name >= setting.Name {
			return fmt.Errorf("query profile settings must be unique and sorted")
		}
	}
	for i, operator := range record.ScalarOperators {
		if strings.TrimSpace(operator) == "" || strings.TrimSpace(operator) != operator {
			return fmt.Errorf("query profile scalar operator %d is required and must be canonical", i)
		}
		if i > 0 && record.ScalarOperators[i-1] >= operator {
			return fmt.Errorf("query profile scalar operators must be unique and sorted")
		}
	}
	limits := record.Limits
	if limits.MaxSQLBytes == 0 || limits.MaxDescriptorBytes == 0 || limits.MaxOutputRows == 0 || limits.MaxOutputBytes == 0 || limits.MaxRestoreBytes == 0 || limits.MaxSortMemoryBytes == 0 || limits.MaxSpillBytes == 0 || limits.MaxExecutionMS == 0 {
		return fmt.Errorf("query profile limits must be nonzero")
	}
	return nil
}

// ValidateQueryProfileFileV1 validates ordering, uniqueness, every embedded
// value, and both the native artifact (N) and query profile (Q) commitments.
// It does not measure local binaries or decide whether a host supports a profile.
func ValidateQueryProfileFileV1(file QueryProfileFileV1) error {
	if file.Version != QueryProfileFileV1Version {
		return fmt.Errorf("query profile file version must be %d", QueryProfileFileV1Version)
	}
	if len(file.Profiles) == 0 {
		return fmt.Errorf("query profile file profiles must be nonempty")
	}
	for i, entry := range file.Profiles {
		if !validSHA256Digest(entry.QueryProfileID) {
			return fmt.Errorf("profile %d query_profile_id must be lowercase 0x SHA-256", i)
		}
		if i > 0 {
			if file.Profiles[i-1].QueryProfileID == entry.QueryProfileID {
				return fmt.Errorf("duplicate query_profile_id at index %d", i)
			}
			if file.Profiles[i-1].QueryProfileID > entry.QueryProfileID {
				return fmt.Errorf("profiles are not sorted at index %d", i)
			}
		}
		if err := validateCanonicalQueryProfileRecord(entry.Record); err != nil {
			return fmt.Errorf("profile %d record: %w", i, err)
		}
		nativeDigest, err := NativeArtifactSetV1Digest(entry.NativeArtifactSet)
		if err != nil {
			return fmt.Errorf("profile %d native_artifact_set: %w", i, err)
		}
		if entry.Record.NativeAnalyzerBuildDigest != nativeDigest {
			return fmt.Errorf("profile %d native_analyzer_build_digest mismatch", i)
		}
		queryProfileID, err := entry.Record.Hash()
		if err != nil {
			return fmt.Errorf("profile %d record: %w", i, err)
		}
		if entry.QueryProfileID != queryProfileID {
			return fmt.Errorf("profile %d query_profile_id mismatch", i)
		}
	}
	return nil
}

// EncodeCanonicalQueryProfileFileV1 validates and encodes the exact profile
// file bytes. It rejects values that would require member reordering.
func EncodeCanonicalQueryProfileFileV1(file QueryProfileFileV1) ([]byte, error) {
	if err := ValidateQueryProfileFileV1(file); err != nil {
		return nil, err
	}
	return json.Marshal(file)
}

// DecodeCanonicalQueryProfileFileV1 accepts only the exact canonical profile
// bytes, with complete recursive field presence and no unknown, duplicate or
// null fields.
func DecodeCanonicalQueryProfileFileV1(raw []byte) (QueryProfileFileV1, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := queryJSONShape(decoder, reflect.TypeOf(QueryProfileFileV1{}), "query_profile_file"); err != nil {
		return QueryProfileFileV1{}, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return QueryProfileFileV1{}, fmt.Errorf("query_profile_file: trailing JSON")
	}
	var file QueryProfileFileV1
	if err := json.Unmarshal(raw, &file); err != nil {
		return QueryProfileFileV1{}, err
	}
	canonical, err := EncodeCanonicalQueryProfileFileV1(file)
	if err != nil {
		return QueryProfileFileV1{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return QueryProfileFileV1{}, fmt.Errorf("noncanonical query profile file bytes")
	}
	return file, nil
}
