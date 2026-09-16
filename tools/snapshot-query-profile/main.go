// snapshot-query-profile measures final local artifacts and emits one canonical
// snapshot-query profile plus a separate, unsigned provenance manifest.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/replay"
)

const (
	// RecipeV1Version is the only accepted local recipe format.
	RecipeV1Version uint32 = 1
	// ProvenanceManifestV1Version is the emitted unsigned provenance format.
	ProvenanceManifestV1Version uint32 = 1
)

// NativeMemberRecipe names one native analyzer member's final executable and
// FFI artifacts. Platform is explicit and is never inferred from either path.
type NativeMemberRecipe struct {
	Platform       string `json:"platform"`
	ExecutablePath string `json:"executable_path"`
	FFIPath        string `json:"ffi_path"`
}

// RecipeV1 is a local tool input. Record's four measured digest fields must be
// present but empty; GenerateProfile always fills them from artifact bytes.
type RecipeV1 struct {
	Version                  uint32                    `json:"version"`
	Record                   replay.QueryProfileRecord `json:"record"`
	NativeMembers            []NativeMemberRecipe      `json:"native_members"`
	GRPCExecutablePath       string                    `json:"grpc_executable_path"`
	ClickHouseExecutablePath string                    `json:"clickhouse_executable_path"`
	TZDataArtifactPath       string                    `json:"tzdata_artifact_path"`
}

// ArtifactMeasurement records the exact local path supplied to the tool and
// the SHA-256 and length obtained from one opened regular file.
type ArtifactMeasurement struct {
	Path   string `json:"path"`
	Bytes  uint64 `json:"bytes"`
	Digest string `json:"digest"`
}

// NativeMemberProvenance keeps the platform separate from both measured files.
type NativeMemberProvenance struct {
	Platform   string              `json:"platform"`
	Executable ArtifactMeasurement `json:"executable"`
	FFI        ArtifactMeasurement `json:"ffi"`
}

// ProvenanceManifestV1 is deliberately outside Q. It records local paths and
// measurements so later qualification gates can retain their evidence.
type ProvenanceManifestV1 struct {
	Version                   uint32                   `json:"version"`
	QueryProfileID            string                   `json:"query_profile_id"`
	NativeAnalyzerBuildDigest string                   `json:"native_analyzer_build_digest"`
	ProfileOutput             ArtifactMeasurement      `json:"profile_output"`
	ClickHouse                ArtifactMeasurement      `json:"clickhouse"`
	GRPCAnalyzer              ArtifactMeasurement      `json:"grpc_analyzer"`
	TZData                    ArtifactMeasurement      `json:"tzdata"`
	NativeMembers             []NativeMemberProvenance `json:"native_members"`
}

// Generation contains deterministic output bytes and their primary IDs.
type Generation struct {
	ProfileBytes              []byte
	ProvenanceBytes           []byte
	QueryProfileID            string
	NativeAnalyzerBuildDigest string
	Provenance                ProvenanceManifestV1
}

// DecodeRecipe accepts exactly one complete JSON value with every declared
// field present, no nulls, and no unknown or duplicate fields.
func DecodeRecipe(raw []byte) (RecipeV1, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := validateJSONShape(decoder, reflect.TypeOf(RecipeV1{}), "recipe"); err != nil {
		return RecipeV1{}, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err == nil {
			return RecipeV1{}, fmt.Errorf("recipe: trailing JSON")
		}
		return RecipeV1{}, fmt.Errorf("recipe: trailing JSON: %w", err)
	}
	var recipe RecipeV1
	if err := json.Unmarshal(raw, &recipe); err != nil {
		return RecipeV1{}, fmt.Errorf("decode recipe: %w", err)
	}
	if recipe.Version != RecipeV1Version {
		return RecipeV1{}, fmt.Errorf("recipe version must be %d", RecipeV1Version)
	}
	return recipe, nil
}

func validateJSONShape(decoder *json.Decoder, typ reflect.Type, path string) error {
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	if token == nil {
		return fmt.Errorf("%s: null is forbidden", path)
	}
	switch typ.Kind() {
	case reflect.Struct:
		if token != json.Delim('{') {
			return fmt.Errorf("%s: expected object", path)
		}
		fields := make(map[string]reflect.Type, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			fields[field.Tag.Get("json")] = field.Type
		}
		seen := make(map[string]bool, len(fields))
		for decoder.More() {
			keyToken, keyErr := decoder.Token()
			if keyErr != nil {
				return fmt.Errorf("%s: %w", path, keyErr)
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("%s: invalid key", path)
			}
			fieldType, ok := fields[key]
			if !ok {
				return fmt.Errorf("%s: unknown field %q", path, key)
			}
			if seen[key] {
				return fmt.Errorf("%s: duplicate field %q", path, key)
			}
			seen[key] = true
			if err := validateJSONShape(decoder, fieldType, path+"."+key); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		for i := 0; i < typ.NumField(); i++ {
			key := typ.Field(i).Tag.Get("json")
			if !seen[key] {
				return fmt.Errorf("%s: missing field %q", path, key)
			}
		}
	case reflect.Slice:
		if token != json.Delim('[') {
			return fmt.Errorf("%s: expected array", path)
		}
		for index := 0; decoder.More(); index++ {
			if err := validateJSONShape(decoder, typ.Elem(), fmt.Sprintf("%s[%d]", path, index)); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	default:
		if _, ok := token.(json.Delim); ok {
			return fmt.Errorf("%s: expected scalar", path)
		}
	}
	return nil
}

// GenerateProfile measures all recipe artifacts and constructs canonical
// profile and provenance bytes. It performs no writes and does not mutate the
// caller-owned recipe or its slices.
func GenerateProfile(recipe RecipeV1, profileOutputPath string) (Generation, error) {
	if recipe.Version != RecipeV1Version {
		return Generation{}, fmt.Errorf("recipe version must be %d", RecipeV1Version)
	}
	if strings.TrimSpace(profileOutputPath) == "" {
		return Generation{}, fmt.Errorf("profile output path is required")
	}
	if recipe.Record.ClickHouseBuildDigest != "" || recipe.Record.NativeAnalyzerBuildDigest != "" || recipe.Record.GRPCAnalyzerBuildDigest != "" || recipe.Record.TZDataDigest != "" {
		return Generation{}, fmt.Errorf("record measured digest fields must be empty; the generator measures and replaces them")
	}
	if len(recipe.NativeMembers) == 0 {
		return Generation{}, fmt.Errorf("native_members must be nonempty")
	}
	for _, input := range []struct {
		name string
		path string
	}{
		{"clickhouse_executable_path", recipe.ClickHouseExecutablePath},
		{"grpc_executable_path", recipe.GRPCExecutablePath},
		{"tzdata_artifact_path", recipe.TZDataArtifactPath},
	} {
		if strings.TrimSpace(input.path) == "" {
			return Generation{}, fmt.Errorf("%s is required", input.name)
		}
	}

	clickhouse, err := measureArtifact(recipe.ClickHouseExecutablePath)
	if err != nil {
		return Generation{}, fmt.Errorf("measure clickhouse executable: %w", err)
	}
	grpc, err := measureArtifact(recipe.GRPCExecutablePath)
	if err != nil {
		return Generation{}, fmt.Errorf("measure gRPC analyzer executable: %w", err)
	}
	tzdata, err := measureArtifact(recipe.TZDataArtifactPath)
	if err != nil {
		return Generation{}, fmt.Errorf("measure tzdata artifact: %w", err)
	}

	type measuredNative struct {
		artifact   replay.NativeArtifactV1
		provenance NativeMemberProvenance
	}
	measured := make([]measuredNative, 0, len(recipe.NativeMembers))
	for index, member := range recipe.NativeMembers {
		if strings.TrimSpace(member.Platform) == "" || strings.TrimSpace(member.Platform) != member.Platform {
			return Generation{}, fmt.Errorf("native member %d platform is required and must be canonical", index)
		}
		if strings.TrimSpace(member.ExecutablePath) == "" || strings.TrimSpace(member.FFIPath) == "" {
			return Generation{}, fmt.Errorf("native member %d executable_path and ffi_path are required", index)
		}
		executable, measureErr := measureArtifact(member.ExecutablePath)
		if measureErr != nil {
			return Generation{}, fmt.Errorf("measure native member %d executable: %w", index, measureErr)
		}
		ffi, measureErr := measureArtifact(member.FFIPath)
		if measureErr != nil {
			return Generation{}, fmt.Errorf("measure native member %d FFI: %w", index, measureErr)
		}
		measured = append(measured, measuredNative{
			artifact:   replay.NativeArtifactV1{Platform: member.Platform, ExecutableDigest: executable.Digest, FFIDigest: ffi.Digest},
			provenance: NativeMemberProvenance{Platform: member.Platform, Executable: executable, FFI: ffi},
		})
	}
	sort.Slice(measured, func(i, j int) bool {
		a, b := measured[i].artifact, measured[j].artifact
		if a.Platform != b.Platform {
			return a.Platform < b.Platform
		}
		if a.ExecutableDigest != b.ExecutableDigest {
			return a.ExecutableDigest < b.ExecutableDigest
		}
		return a.FFIDigest < b.FFIDigest
	})
	artifacts := make([]replay.NativeArtifactV1, 0, len(measured))
	nativeProvenance := make([]NativeMemberProvenance, 0, len(measured))
	for _, member := range measured {
		artifacts = append(artifacts, member.artifact)
		nativeProvenance = append(nativeProvenance, member.provenance)
	}
	artifactSet := replay.NativeArtifactSetV1{Version: replay.NativeArtifactSetV1Version, Artifacts: artifacts}
	nativeDigest, err := replay.NativeArtifactSetV1Digest(artifactSet)
	if err != nil {
		return Generation{}, fmt.Errorf("validate native artifact set: %w", err)
	}

	record := recipe.Record
	record.Settings = append([]replay.ProfileSetting{}, recipe.Record.Settings...)
	sort.Slice(record.Settings, func(i, j int) bool { return record.Settings[i].Name < record.Settings[j].Name })
	record.ScalarOperators = append([]string{}, recipe.Record.ScalarOperators...)
	sort.Strings(record.ScalarOperators)
	record.ClickHouseBuildDigest = clickhouse.Digest
	record.NativeAnalyzerBuildDigest = nativeDigest
	record.GRPCAnalyzerBuildDigest = grpc.Digest
	record.TZDataDigest = tzdata.Digest
	queryProfileID, err := record.Hash()
	if err != nil {
		return Generation{}, fmt.Errorf("hash query profile record: %w", err)
	}
	profile := replay.QueryProfileFileV1{Version: replay.QueryProfileFileV1Version, Profiles: []replay.QueryProfileEntryV1{{
		QueryProfileID: queryProfileID, Record: record, NativeArtifactSet: artifactSet,
	}}}
	profileBytes, err := replay.EncodeCanonicalQueryProfileFileV1(profile)
	if err != nil {
		return Generation{}, fmt.Errorf("validate query profile: %w", err)
	}
	profileMeasurement := ArtifactMeasurement{Path: profileOutputPath, Bytes: uint64(len(profileBytes)), Digest: digestBytes(profileBytes)}
	provenance := ProvenanceManifestV1{
		Version:                   ProvenanceManifestV1Version,
		QueryProfileID:            queryProfileID,
		NativeAnalyzerBuildDigest: nativeDigest,
		ProfileOutput:             profileMeasurement,
		ClickHouse:                clickhouse,
		GRPCAnalyzer:              grpc,
		TZData:                    tzdata,
		NativeMembers:             nativeProvenance,
	}
	provenanceBytes, err := json.Marshal(provenance)
	if err != nil {
		return Generation{}, fmt.Errorf("encode provenance: %w", err)
	}
	return Generation{
		ProfileBytes: profileBytes, ProvenanceBytes: provenanceBytes, QueryProfileID: queryProfileID,
		NativeAnalyzerBuildDigest: nativeDigest, Provenance: provenance,
	}, nil
}

func measureArtifact(path string) (ArtifactMeasurement, error) {
	file, err := os.Open(path)
	if err != nil {
		return ArtifactMeasurement{}, err
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return ArtifactMeasurement{}, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return ArtifactMeasurement{}, fmt.Errorf("%q is not a regular file", path)
	}
	hash := sha256.New()
	length, err := io.Copy(hash, file)
	if err != nil {
		_ = file.Close()
		return ArtifactMeasurement{}, err
	}
	if closeErr := file.Close(); closeErr != nil {
		return ArtifactMeasurement{}, closeErr
	}
	return ArtifactMeasurement{Path: path, Bytes: uint64(length), Digest: "0x" + hex.EncodeToString(hash.Sum(nil))}, nil
}

func digestBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "0x" + hex.EncodeToString(sum[:])
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("snapshot-query-profile", flag.ContinueOnError)
	flags.SetOutput(stderr)
	recipePath := flags.String("recipe", "", "strict JSON recipe path")
	profilePath := flags.String("profile-out", "", "canonical profile output path")
	provenancePath := flags.String("provenance-out", "", "unsigned provenance output path")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if *recipePath == "" || *profilePath == "" || *provenancePath == "" {
		return fmt.Errorf("-recipe, -profile-out and -provenance-out are required")
	}
	if *profilePath == *provenancePath {
		return fmt.Errorf("profile and provenance output paths must differ")
	}
	raw, err := os.ReadFile(*recipePath)
	if err != nil {
		return fmt.Errorf("read recipe: %w", err)
	}
	recipe, err := DecodeRecipe(raw)
	if err != nil {
		return err
	}
	result, err := GenerateProfile(recipe, *profilePath)
	if err != nil {
		return err
	}
	if err := writeOutputPair(*profilePath, result.ProfileBytes, *provenancePath, result.ProvenanceBytes); err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "query_profile_id=%s\nnative_analyzer_build_digest=%s\n", result.QueryProfileID, result.NativeAnalyzerBuildDigest)
	return err
}

type stagedFile struct {
	path     string
	tempPath string
}

func stageFile(path string, data []byte) (stagedFile, error) {
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return stagedFile{}, err
	}
	tempPath := file.Name()
	cleanup := func(stageErr error) (stagedFile, error) {
		_ = file.Close()
		_ = os.Remove(tempPath)
		return stagedFile{}, stageErr
	}
	if err := file.Chmod(0o644); err != nil {
		return cleanup(err)
	}
	if _, err := file.Write(data); err != nil {
		return cleanup(err)
	}
	if err := file.Sync(); err != nil {
		return cleanup(err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return stagedFile{}, err
	}
	return stagedFile{path: path, tempPath: tempPath}, nil
}

func writeOutputPair(profilePath string, profile []byte, provenancePath string, provenance []byte) error {
	profileStage, err := stageFile(profilePath, profile)
	if err != nil {
		return fmt.Errorf("stage profile output: %w", err)
	}
	defer os.Remove(profileStage.tempPath)
	provenanceStage, err := stageFile(provenancePath, provenance)
	if err != nil {
		return fmt.Errorf("stage provenance output: %w", err)
	}
	defer os.Remove(provenanceStage.tempPath)
	if err := os.Rename(profileStage.tempPath, profileStage.path); err != nil {
		return fmt.Errorf("commit profile output: %w", err)
	}
	profileStage.tempPath = ""
	if err := os.Rename(provenanceStage.tempPath, provenanceStage.path); err != nil {
		return fmt.Errorf("commit provenance output: %w", err)
	}
	provenanceStage.tempPath = ""
	return nil
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			fmt.Fprintf(os.Stderr, "snapshot-query-profile: %v\n", err)
		}
		os.Exit(2)
	}
}
