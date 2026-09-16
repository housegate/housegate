package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

func rawDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return "0x" + hex.EncodeToString(sum[:])
}

func domainDigest(domain string, canonical []byte) string {
	sum := sha256.Sum256(append([]byte("housegate-replay-mvp-v0:"+domain+"\x00"), canonical...))
	return "0x" + hex.EncodeToString(sum[:])
}

func writeArtifact(t *testing.T, dir, name, contents string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func validRecipe(t *testing.T, dir string) RecipeV1 {
	t.Helper()
	return RecipeV1{
		Version: RecipeV1Version,
		Record: replay.QueryProfileRecord{
			Version:         1,
			Platform:        "linux/amd64",
			Settings:        []replay.ProfileSetting{{Name: "z_setting", Value: "2"}, {Name: "a_setting", Value: "1"}},
			ScalarOperators: []string{"literal", "column"},
			ColumnProfileID: "column-profile-test",
			OutputOrderID:   "output-order-test",
			Limits: replay.QueryLimits{
				MaxSQLBytes:        101,
				MaxDescriptorBytes: 103,
				MaxOutputRows:      107,
				MaxOutputBytes:     109,
				MaxRestoreBytes:    113,
				MaxSortMemoryBytes: 127,
				MaxSpillBytes:      131,
				MaxExecutionMS:     137,
			},
		},
		NativeMembers: []NativeMemberRecipe{
			{Platform: "linux/amd64", ExecutablePath: writeArtifact(t, dir, "native-z", "native-z-bytes"), FFIPath: writeArtifact(t, dir, "ffi-z", "ffi-z-bytes")},
			{Platform: "darwin/arm64", ExecutablePath: writeArtifact(t, dir, "native-a", "native-a-bytes"), FFIPath: writeArtifact(t, dir, "ffi-a", "ffi-a-bytes")},
		},
		GRPCExecutablePath:       writeArtifact(t, dir, "grpc", "grpc-bytes"),
		ClickHouseExecutablePath: writeArtifact(t, dir, "clickhouse", "clickhouse-bytes"),
		TZDataArtifactPath:       writeArtifact(t, dir, "tzdata", "tzdata-bytes"),
	}
}

func marshalRecipe(t *testing.T, recipe RecipeV1) []byte {
	t.Helper()
	raw, err := json.Marshal(recipe)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func cloneRecipe(t *testing.T, recipe RecipeV1) RecipeV1 {
	t.Helper()
	raw := marshalRecipe(t, recipe)
	var out RecipeV1
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestGenerateProfileMeasuresCanonicalizesAndPreservesInput(t *testing.T) {
	dir := t.TempDir()
	recipe := validRecipe(t, dir)
	before := marshalRecipe(t, recipe)
	profilePath := filepath.Join(dir, "profile.json")

	result, err := GenerateProfile(recipe, profilePath)
	if err != nil {
		t.Fatal(err)
	}
	if after := marshalRecipe(t, recipe); !bytes.Equal(before, after) {
		t.Fatal("GenerateProfile mutated the caller's recipe")
	}

	wantArtifacts := []replay.NativeArtifactV1{
		{Platform: "darwin/arm64", ExecutableDigest: rawDigest([]byte("native-a-bytes")), FFIDigest: rawDigest([]byte("ffi-a-bytes"))},
		{Platform: "linux/amd64", ExecutableDigest: rawDigest([]byte("native-z-bytes")), FFIDigest: rawDigest([]byte("ffi-z-bytes"))},
	}
	setJSON, err := json.Marshal(replay.NativeArtifactSetV1{Version: 1, Artifacts: wantArtifacts})
	if err != nil {
		t.Fatal(err)
	}
	wantN := domainDigest("snapshot-native-analyzer-artifact-set-v1", setJSON)
	wantRecord := recipe.Record
	wantRecord.ClickHouseBuildDigest = rawDigest([]byte("clickhouse-bytes"))
	wantRecord.NativeAnalyzerBuildDigest = wantN
	wantRecord.GRPCAnalyzerBuildDigest = rawDigest([]byte("grpc-bytes"))
	wantRecord.TZDataDigest = rawDigest([]byte("tzdata-bytes"))
	wantRecord.Settings = []replay.ProfileSetting{{Name: "a_setting", Value: "1"}, {Name: "z_setting", Value: "2"}}
	wantRecord.ScalarOperators = []string{"column", "literal"}
	recordJSON, err := json.Marshal(wantRecord)
	if err != nil {
		t.Fatal(err)
	}
	wantQ := domainDigest("snapshot-query-profile-v1", recordJSON)
	wantProfile, err := json.Marshal(replay.QueryProfileFileV1{Version: 1, Profiles: []replay.QueryProfileEntryV1{{
		QueryProfileID: wantQ, Record: wantRecord, NativeArtifactSet: replay.NativeArtifactSetV1{Version: 1, Artifacts: wantArtifacts},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.ProfileBytes, wantProfile) {
		t.Fatalf("profile bytes mismatch\n got: %s\nwant: %s", result.ProfileBytes, wantProfile)
	}
	if result.QueryProfileID != wantQ || result.NativeAnalyzerBuildDigest != wantN {
		t.Fatalf("got Q=%s N=%s want Q=%s N=%s", result.QueryProfileID, result.NativeAnalyzerBuildDigest, wantQ, wantN)
	}
	if got := result.Provenance.ProfileOutput.Digest; got != rawDigest(wantProfile) {
		t.Fatalf("output digest=%s", got)
	}
	if got := result.Provenance.ProfileOutput.Bytes; got != uint64(len(wantProfile)) {
		t.Fatalf("output bytes=%d", got)
	}
	if result.Provenance.ProfileOutput.Path != profilePath {
		t.Fatalf("output path=%q", result.Provenance.ProfileOutput.Path)
	}
	if len(result.Provenance.NativeMembers) != 2 || result.Provenance.NativeMembers[0].Platform != "darwin/arm64" {
		t.Fatalf("native provenance not sorted: %+v", result.Provenance.NativeMembers)
	}
	if result.Provenance.NativeMembers[0].Executable.Path != recipe.NativeMembers[1].ExecutablePath || result.Provenance.NativeMembers[0].Executable.Bytes != uint64(len("native-a-bytes")) {
		t.Fatalf("native provenance lost source evidence: %+v", result.Provenance.NativeMembers[0])
	}
	if result.Provenance.ClickHouse.Path != recipe.ClickHouseExecutablePath || result.Provenance.ClickHouse.Digest != wantRecord.ClickHouseBuildDigest {
		t.Fatalf("clickhouse provenance mismatch: %+v", result.Provenance.ClickHouse)
	}
	if result.Provenance.GRPCAnalyzer.Path != recipe.GRPCExecutablePath || result.Provenance.TZData.Path != recipe.TZDataArtifactPath {
		t.Fatal("provenance paths do not match recipe paths")
	}
	var decoded any
	if err := json.Unmarshal(result.ProvenanceBytes, &decoded); err != nil {
		t.Fatalf("invalid provenance JSON: %v", err)
	}
}

func TestGenerateProfilePathRelocationDoesNotChangeQ(t *testing.T) {
	dir := t.TempDir()
	recipe := validRecipe(t, dir)
	first, err := GenerateProfile(recipe, "profile.json")
	if err != nil {
		t.Fatal(err)
	}
	relocated := cloneRecipe(t, recipe)
	relocatedDir := t.TempDir()
	for _, pair := range [][2]*string{
		{&recipe.NativeMembers[0].ExecutablePath, &relocated.NativeMembers[0].ExecutablePath},
		{&recipe.NativeMembers[0].FFIPath, &relocated.NativeMembers[0].FFIPath},
		{&recipe.NativeMembers[1].ExecutablePath, &relocated.NativeMembers[1].ExecutablePath},
		{&recipe.NativeMembers[1].FFIPath, &relocated.NativeMembers[1].FFIPath},
		{&recipe.GRPCExecutablePath, &relocated.GRPCExecutablePath},
		{&recipe.ClickHouseExecutablePath, &relocated.ClickHouseExecutablePath},
		{&recipe.TZDataArtifactPath, &relocated.TZDataArtifactPath},
	} {
		data, readErr := os.ReadFile(*pair[0])
		if readErr != nil {
			t.Fatal(readErr)
		}
		newPath := filepath.Join(relocatedDir, filepath.Base(*pair[0]))
		if writeErr := os.WriteFile(newPath, data, 0o600); writeErr != nil {
			t.Fatal(writeErr)
		}
		*pair[1] = newPath
	}
	second, err := GenerateProfile(relocated, "profile.json")
	if err != nil {
		t.Fatal(err)
	}
	if first.QueryProfileID != second.QueryProfileID || !bytes.Equal(first.ProfileBytes, second.ProfileBytes) {
		t.Fatal("relocating identical artifacts changed the signed profile")
	}
	if bytes.Equal(first.ProvenanceBytes, second.ProvenanceBytes) {
		t.Fatal("relocating artifacts did not change provenance")
	}
}

func TestGenerateProfileArtifactMutationChangesCommitments(t *testing.T) {
	dir := t.TempDir()
	recipe := validRecipe(t, dir)
	base, err := GenerateProfile(recipe, filepath.Join(dir, "profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(recipe.NativeMembers[0].FFIPath, []byte("ffi-z-bytes-changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	changed, err := GenerateProfile(recipe, filepath.Join(dir, "profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	if base.NativeAnalyzerBuildDigest == changed.NativeAnalyzerBuildDigest || base.QueryProfileID == changed.QueryProfileID {
		t.Fatal("native artifact mutation did not change N and Q")
	}
	if base.Provenance.ClickHouse.Digest != changed.Provenance.ClickHouse.Digest || base.Provenance.GRPCAnalyzer.Digest != changed.Provenance.GRPCAnalyzer.Digest {
		t.Fatal("unrelated artifact digest changed")
	}
}

func TestGenerateProfileRejectsMeasuredFieldsAndInvalidValues(t *testing.T) {
	base := validRecipe(t, t.TempDir())
	tests := []struct {
		name   string
		mutate func(*RecipeV1)
	}{
		{"clickhouse_prepopulated", func(r *RecipeV1) { r.Record.ClickHouseBuildDigest = "0x" + strings.Repeat("1", 64) }},
		{"native_prepopulated", func(r *RecipeV1) { r.Record.NativeAnalyzerBuildDigest = "x" }},
		{"grpc_prepopulated", func(r *RecipeV1) { r.Record.GRPCAnalyzerBuildDigest = "x" }},
		{"tzdata_prepopulated", func(r *RecipeV1) { r.Record.TZDataDigest = "x" }},
		{"empty_native_set", func(r *RecipeV1) { r.NativeMembers = []NativeMemberRecipe{} }},
		{"zero_limit", func(r *RecipeV1) { r.Record.Limits.MaxSQLBytes = 0 }},
		{"duplicate_setting", func(r *RecipeV1) { r.Record.Settings = append(r.Record.Settings, r.Record.Settings[0]) }},
		{"duplicate_operator", func(r *RecipeV1) {
			r.Record.ScalarOperators = append(r.Record.ScalarOperators, r.Record.ScalarOperators[0])
		}},
		{"blank_native_platform", func(r *RecipeV1) { r.NativeMembers[0].Platform = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recipe := cloneRecipe(t, base)
			test.mutate(&recipe)
			if _, err := GenerateProfile(recipe, "profile.json"); err == nil {
				t.Fatal("invalid recipe accepted")
			}
		})
	}

	duplicate := cloneRecipe(t, base)
	duplicate.NativeMembers = append(duplicate.NativeMembers, NativeMemberRecipe{
		Platform:       duplicate.NativeMembers[0].Platform,
		ExecutablePath: duplicate.NativeMembers[0].ExecutablePath,
		FFIPath:        duplicate.NativeMembers[0].FFIPath,
	})
	if _, err := GenerateProfile(duplicate, "profile.json"); err == nil {
		t.Fatal("duplicate measured tuple accepted")
	}
}

func TestDecodeRecipeRejectsMalformedInput(t *testing.T) {
	base := validRecipe(t, t.TempDir())
	raw := marshalRecipe(t, base)
	tests := map[string][]byte{
		"unknown":          bytes.Replace(raw, []byte(`{"version":1,`), []byte(`{"version":1,"unknown":true,`), 1),
		"duplicate":        bytes.Replace(raw, []byte(`{"version":1,`), []byte(`{"version":1,"version":1,`), 1),
		"missing":          bytes.Replace(raw, []byte(`,"tzdata_artifact_path":"`+base.TZDataArtifactPath+`"`), nil, 1),
		"nested_unknown":   bytes.Replace(raw, []byte(`"platform":"linux/amd64",`), []byte(`"platform":"linux/amd64","unknown":true,`), 1),
		"nested_duplicate": bytes.Replace(raw, []byte(`"platform":"linux/amd64",`), []byte(`"platform":"linux/amd64","platform":"linux/amd64",`), 1),
		"trailing":         append(append([]byte(nil), raw...), []byte(" trailing")...),
		"malformed":        []byte(`{"version":`),
	}
	badVersion := append([]byte(nil), raw...)
	badVersion = bytes.Replace(badVersion, []byte(`{"version":1,`), []byte(`{"version":2,`), 1)
	tests["version"] = badVersion
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeRecipe(input); err == nil {
				t.Fatal("malformed recipe accepted")
			}
		})
	}
}

func TestDecodeRecipeRejectsNullIndependently(t *testing.T) {
	base := validRecipe(t, t.TempDir())
	raw := marshalRecipe(t, base)
	members, err := json.Marshal(base.NativeMembers)
	if err != nil {
		t.Fatal(err)
	}
	mutated := bytes.Replace(raw, append([]byte(`"native_members":`), members...), []byte(`"native_members":null`), 1)
	if bytes.Equal(mutated, raw) {
		t.Fatal("null mutation did not match")
	}
	_, err = DecodeRecipe(mutated)
	if err == nil || !strings.Contains(err.Error(), "recipe.native_members: null is forbidden") {
		t.Fatalf("null rejection=%v", err)
	}
}

func TestGenerateProfileRejectsArtifactFailures(t *testing.T) {
	base := validRecipe(t, t.TempDir())
	tests := []struct {
		name   string
		mutate func(*testing.T, *RecipeV1)
	}{
		{"missing", func(t *testing.T, r *RecipeV1) { r.GRPCExecutablePath = filepath.Join(t.TempDir(), "missing") }},
		{"nonregular", func(t *testing.T, r *RecipeV1) { r.TZDataArtifactPath = t.TempDir() }},
		{"unreadable", func(t *testing.T, r *RecipeV1) {
			path := writeArtifact(t, t.TempDir(), "unreadable", "secret")
			if err := os.Chmod(path, 0); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(path, 0o600) })
			r.ClickHouseExecutablePath = path
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recipe := cloneRecipe(t, base)
			test.mutate(t, &recipe)
			if _, err := GenerateProfile(recipe, "profile.json"); err == nil {
				t.Fatal("artifact failure accepted")
			}
		})
	}
}

func TestRunWritesValidatedOutputsAndReportsWriteErrors(t *testing.T) {
	dir := t.TempDir()
	recipe := validRecipe(t, dir)
	recipePath := filepath.Join(dir, "recipe.json")
	if err := os.WriteFile(recipePath, marshalRecipe(t, recipe), 0o600); err != nil {
		t.Fatal(err)
	}
	profilePath := filepath.Join(dir, "profile.json")
	provenancePath := filepath.Join(dir, "provenance.json")
	var stdout, stderr bytes.Buffer
	if err := run([]string{"-recipe", recipePath, "-profile-out", profilePath, "-provenance-out", provenancePath}, &stdout, &stderr); err != nil {
		t.Fatalf("run: %v stderr=%s", err, stderr.String())
	}
	profileRaw, err := os.ReadFile(profilePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replay.DecodeCanonicalQueryProfileFileV1(profileRaw); err != nil {
		t.Fatalf("written profile is invalid: %v", err)
	}
	provenanceRaw, err := os.ReadFile(provenancePath)
	if err != nil {
		t.Fatal(err)
	}
	var provenance ProvenanceManifestV1
	if err := json.Unmarshal(provenanceRaw, &provenance); err != nil {
		t.Fatal(err)
	}
	if provenance.ProfileOutput.Path != profilePath {
		t.Fatalf("profile output path=%q", provenance.ProfileOutput.Path)
	}
	if !strings.Contains(stdout.String(), provenance.QueryProfileID) || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}

	badProfilePath := filepath.Join(dir, "missing", "profile.json")
	if err := run([]string{"-recipe", recipePath, "-profile-out", badProfilePath, "-provenance-out", provenancePath}, &stdout, &stderr); err == nil {
		t.Fatal("profile output write error not reported")
	}
	if _, err := os.Stat(badProfilePath); !os.IsNotExist(err) {
		t.Fatalf("partial profile left behind: %v", err)
	}
	deferredProfilePath := filepath.Join(dir, "deferred-profile.json")
	if err := run([]string{"-recipe", recipePath, "-profile-out", deferredProfilePath, "-provenance-out", filepath.Join(dir, "missing", "provenance.json")}, &stdout, &stderr); err == nil {
		t.Fatal("provenance output write error not reported")
	}
	if _, err := os.Stat(deferredProfilePath); !os.IsNotExist(err) {
		t.Fatalf("profile was committed before provenance staging completed: %v", err)
	}
}

func TestRunRejectsOutputAliasesWithoutChangingFiles(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*testing.T, string, RecipeV1, string) (string, string, map[string][]byte)
	}{
		{
			name: "cleaned_output_alias",
			setup: func(t *testing.T, dir string, _ RecipeV1, _ string) (string, string, map[string][]byte) {
				profile := writeArtifact(t, dir, "profile.json", "existing-profile")
				return profile, dir + string(os.PathSeparator) + "." + string(os.PathSeparator) + "profile.json", map[string][]byte{profile: []byte("existing-profile")}
			},
		},
		{
			name: "relative_absolute_output_alias",
			setup: func(t *testing.T, dir string, _ RecipeV1, _ string) (string, string, map[string][]byte) {
				profile := writeArtifact(t, dir, "profile.json", "existing-profile")
				workingDirectory, err := os.Getwd()
				if err != nil {
					t.Fatal(err)
				}
				relative, err := filepath.Rel(workingDirectory, profile)
				if err != nil {
					t.Fatal(err)
				}
				return profile, relative, map[string][]byte{profile: []byte("existing-profile")}
			},
		},
		{
			name: "symlink_output_alias",
			setup: func(t *testing.T, dir string, _ RecipeV1, _ string) (string, string, map[string][]byte) {
				profile := writeArtifact(t, dir, "profile.json", "existing-profile")
				provenance := filepath.Join(dir, "provenance.json")
				if err := os.Symlink(profile, provenance); err != nil {
					t.Fatal(err)
				}
				return profile, provenance, map[string][]byte{profile: []byte("existing-profile"), provenance: []byte("existing-profile")}
			},
		},
		{
			name: "hardlink_output_alias",
			setup: func(t *testing.T, dir string, _ RecipeV1, _ string) (string, string, map[string][]byte) {
				profile := writeArtifact(t, dir, "profile.json", "existing-profile")
				provenance := filepath.Join(dir, "provenance.json")
				if err := os.Link(profile, provenance); err != nil {
					t.Fatal(err)
				}
				return profile, provenance, map[string][]byte{profile: []byte("existing-profile"), provenance: []byte("existing-profile")}
			},
		},
		{
			name: "profile_aliases_recipe",
			setup: func(t *testing.T, dir string, _ RecipeV1, recipePath string) (string, string, map[string][]byte) {
				provenance := writeArtifact(t, dir, "provenance.json", "existing-provenance")
				recipeBytes, err := os.ReadFile(recipePath)
				if err != nil {
					t.Fatal(err)
				}
				return filepath.Join(dir, ".", "recipe.json"), provenance, map[string][]byte{recipePath: recipeBytes, provenance: []byte("existing-provenance")}
			},
		},
		{
			name: "provenance_hardlink_aliases_artifact",
			setup: func(t *testing.T, dir string, recipe RecipeV1, _ string) (string, string, map[string][]byte) {
				profile := writeArtifact(t, dir, "profile.json", "existing-profile")
				provenance := filepath.Join(dir, "provenance.json")
				if err := os.Link(recipe.GRPCExecutablePath, provenance); err != nil {
					t.Fatal(err)
				}
				artifactBytes, err := os.ReadFile(recipe.GRPCExecutablePath)
				if err != nil {
					t.Fatal(err)
				}
				return profile, provenance, map[string][]byte{profile: []byte("existing-profile"), recipe.GRPCExecutablePath: artifactBytes, provenance: artifactBytes}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			recipe := validRecipe(t, dir)
			recipePath := filepath.Join(dir, "recipe.json")
			if err := os.WriteFile(recipePath, marshalRecipe(t, recipe), 0o600); err != nil {
				t.Fatal(err)
			}
			profile, provenance, protected := test.setup(t, dir, recipe, recipePath)
			if err := run([]string{"-recipe", recipePath, "-profile-out", profile, "-provenance-out", provenance}, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
				t.Fatal("aliased output paths accepted")
			}
			for path, want := range protected {
				got, err := os.ReadFile(path)
				if err != nil {
					t.Fatalf("protected path %q: %v", path, err)
				}
				if !bytes.Equal(got, want) {
					t.Fatalf("protected path %q changed: got %q want %q", path, got, want)
				}
			}
		})
	}
}

func TestFIFOInputsAreRejectedWithoutBlocking(t *testing.T) {
	assertBoundedError := func(t *testing.T, call func() error) {
		t.Helper()
		result := make(chan error, 1)
		go func() { result <- call() }()
		select {
		case err := <-result:
			if err == nil || !strings.Contains(err.Error(), "not a regular file") {
				t.Fatalf("FIFO rejection=%v", err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("FIFO input blocked instead of being rejected")
		}
	}
	t.Run("recipe", func(t *testing.T) {
		dir := t.TempDir()
		fifo := filepath.Join(dir, "recipe.fifo")
		if err := syscall.Mkfifo(fifo, 0o600); err != nil {
			t.Fatal(err)
		}
		assertBoundedError(t, func() error {
			return run([]string{"-recipe", fifo, "-profile-out", filepath.Join(dir, "profile.json"), "-provenance-out", filepath.Join(dir, "provenance.json")}, &bytes.Buffer{}, &bytes.Buffer{})
		})
	})
	t.Run("artifact", func(t *testing.T) {
		dir := t.TempDir()
		recipe := validRecipe(t, dir)
		fifo := filepath.Join(dir, "artifact.fifo")
		if err := syscall.Mkfifo(fifo, 0o600); err != nil {
			t.Fatal(err)
		}
		recipe.ClickHouseExecutablePath = fifo
		assertBoundedError(t, func() error {
			_, err := GenerateProfile(recipe, filepath.Join(dir, "profile.json"))
			return err
		})
	})
}

func TestCanonicalizationIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	recipe := validRecipe(t, dir)
	a, err := GenerateProfile(recipe, filepath.Join(dir, "profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	reversed := cloneRecipe(t, recipe)
	reversed.SettingsSwapForTest()
	reversed.NativeMembers[0], reversed.NativeMembers[1] = reversed.NativeMembers[1], reversed.NativeMembers[0]
	reversed.Record.ScalarOperators[0], reversed.Record.ScalarOperators[1] = reversed.Record.ScalarOperators[1], reversed.Record.ScalarOperators[0]
	b, err := GenerateProfile(reversed, filepath.Join(dir, "profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("input ordering changed output\nfirst=%+v\nsecond=%+v", a, b)
	}
}

func (r *RecipeV1) SettingsSwapForTest() {
	r.Record.Settings[0], r.Record.Settings[1] = r.Record.Settings[1], r.Record.Settings[0]
}

func TestRunRejectsIncompleteFlags(t *testing.T) {
	for _, args := range [][]string{nil, {"-recipe", "x"}, {"extra"}} {
		t.Run(fmt.Sprint(args), func(t *testing.T) {
			if err := run(args, &bytes.Buffer{}, &bytes.Buffer{}); err == nil {
				t.Fatal("incomplete CLI accepted")
			}
		})
	}
}
