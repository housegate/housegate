package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

type snapshotQuerySchemaVector struct {
	ArtifactJSON            string   `json:"artifact_json"`
	ArtifactDigest          string   `json:"artifact_digest"`
	AuthorityJWSSegments    []string `json:"authority_jws_segments"`
	AuthorityAddress        string   `json:"authority_address"`
	AuthenticatedJSONPrefix string   `json:"authenticated_json_prefix"`
	AuthenticatedJSONInfix  string   `json:"authenticated_json_infix"`
	AuthenticatedJSONSuffix string   `json:"authenticated_json_suffix"`
	AuthenticatedDigest     string   `json:"authenticated_digest"`
}

func (v snapshotQuerySchemaVector) authorityJWS() string {
	return strings.Join(v.AuthorityJWSSegments, ".")
}

func (v snapshotQuerySchemaVector) authenticatedJSON() string {
	return v.AuthenticatedJSONPrefix + v.ArtifactJSON + v.AuthenticatedJSONInfix + v.authorityJWS() + v.AuthenticatedJSONSuffix
}

func schemaVector(t *testing.T) snapshotQuerySchemaVector {
	t.Helper()
	raw, err := os.ReadFile("testdata/snapshot_query_schema_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var vector snapshotQuerySchemaVector
	if err := json.Unmarshal(raw, &vector); err != nil {
		t.Fatal(err)
	}
	return vector
}

func validSchemaArtifact(t *testing.T) SnapshotQuerySchemaArtifactV1 {
	t.Helper()
	artifact, err := DecodeCanonicalSnapshotQuerySchemaArtifactV1([]byte(schemaVector(t).ArtifactJSON))
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func TestSnapshotQuerySchemaArtifactFrozenVector(t *testing.T) {
	vector := schemaVector(t)
	artifact, err := DecodeCanonicalSnapshotQuerySchemaArtifactV1([]byte(vector.ArtifactJSON))
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := EncodeCanonicalSnapshotQuerySchemaArtifactV1(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != vector.ArtifactJSON {
		t.Fatalf("artifact bytes changed: %s", encoded)
	}
	digest, err := SnapshotQuerySchemaArtifactDigestV1(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if digest != vector.ArtifactDigest || digest != DigestBytes([]byte(vector.ArtifactJSON)) {
		t.Fatalf("artifact digest=%s want=%s", digest, vector.ArtifactDigest)
	}
	if independent := fmt.Sprintf("0x%x", sha256.Sum256([]byte(vector.ArtifactJSON))); digest != independent {
		t.Fatalf("artifact digest differs from independent SHA-256: %s", independent)
	}
	if len(artifact.Tables) != 2 || artifact.Tables[1].Columns[1].Generation != SnapshotQueryColumnGenerationDefault || artifact.Tables[1].Columns[1].DefaultExpression != "now()" {
		t.Fatal("generation metadata was not preserved")
	}
}

func TestSnapshotQuerySchemaArtifactStrictCanonicalBytes(t *testing.T) {
	canonical := schemaVector(t).ArtifactJSON
	variants := map[string]string{
		"leading_whitespace": " " + canonical,
		"trailing":           canonical + "{}",
		"escaped_kind":       strings.Replace(canonical, "snapshot-query", `snapshot\u002dquery`, 1),
		"reordered":          strings.Replace(canonical, `{"kind":"snapshot-query-schema-artifact-v1","version":1`, `{"version":1,"kind":"snapshot-query-schema-artifact-v1"`, 1),
		"unknown":            strings.Replace(canonical, `{"kind":`, `{"foreign":0,"kind":`, 1),
		"duplicate":          strings.Replace(canonical, `"version":1`, `"version":1,"version":1`, 1),
		"missing":            strings.Replace(canonical, `"kind":"snapshot-query-schema-artifact-v1",`, "", 1),
		"null":               strings.Replace(canonical, `"tables":[`, `"tables":null,"ignored":[`, 1),
		"nested_unknown":     strings.Replace(canonical, `"table_id":"orders"`, `"unknown":0,"table_id":"orders"`, 1),
		"nested_duplicate":   strings.Replace(canonical, `"generation":1`, `"generation":1,"generation":1`, 1),
		"nested_missing":     strings.Replace(canonical, `,"default_expression":""`, "", 1),
		"nested_null":        strings.Replace(canonical, `"columns":[`, `"columns":null,"ignored":[`, 1),
	}
	for name, raw := range variants {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeCanonicalSnapshotQuerySchemaArtifactV1([]byte(raw)); err == nil {
				t.Fatal("noncanonical schema artifact accepted")
			}
		})
	}
}

func TestSnapshotQuerySchemaArtifactValueValidation(t *testing.T) {
	base := validSchemaArtifact(t)
	mutations := map[string]func(*SnapshotQuerySchemaArtifactV1){
		"kind":             func(a *SnapshotQuerySchemaArtifactV1) { a.Kind = "other" },
		"version":          func(a *SnapshotQuerySchemaArtifactV1) { a.Version = 2 },
		"network":          func(a *SnapshotQuerySchemaArtifactV1) { a.NetworkID = "" },
		"snapshot_digest":  func(a *SnapshotQuerySchemaArtifactV1) { a.SnapshotID = "0x12" },
		"manifest_digest":  func(a *SnapshotQuerySchemaArtifactV1) { a.ManifestRoot = strings.ToUpper(a.ManifestRoot) },
		"schema_digest":    func(a *SnapshotQuerySchemaArtifactV1) { a.SchemaRoot = a.SchemaRoot[2:] },
		"table_digest":     func(a *SnapshotQuerySchemaArtifactV1) { a.Tables[0].SchemaHash = "0x12" },
		"duplicate_table":  func(a *SnapshotQuerySchemaArtifactV1) { a.Tables[1] = a.Tables[0] },
		"unsorted_table":   func(a *SnapshotQuerySchemaArtifactV1) { a.Tables[0], a.Tables[1] = a.Tables[1], a.Tables[0] },
		"empty_columns":    func(a *SnapshotQuerySchemaArtifactV1) { a.Tables[0].Columns = []SnapshotQuerySchemaColumnV1{} },
		"duplicate_column": func(a *SnapshotQuerySchemaArtifactV1) { a.Tables[0].Columns[1].Name = a.Tables[0].Columns[0].Name },
		"empty_column_name": func(a *SnapshotQuerySchemaArtifactV1) {
			a.Tables[0].Columns[0].Name = ""
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			artifact := base.Clone()
			mutate(&artifact)
			if _, err := EncodeCanonicalSnapshotQuerySchemaArtifactV1(artifact); err == nil {
				t.Fatal("malformed schema artifact encoded")
			}
		})
	}
}

func TestSnapshotQuerySchemaArtifactBounds(t *testing.T) {
	base := validSchemaArtifact(t)
	tooManyTables := base.Clone()
	tooManyTables.Tables = make([]SnapshotQueryTableSchemaV1, SnapshotQuerySchemaMaxTables+1)
	for i := range tooManyTables.Tables {
		tooManyTables.Tables[i] = base.Tables[0]
		tooManyTables.Tables[i].TableID = fmt.Sprintf("table-%04d", i)
	}
	if _, err := EncodeCanonicalSnapshotQuerySchemaArtifactV1(tooManyTables); err == nil {
		t.Fatal("table bound not enforced")
	}
	tooManyColumns := base.Clone()
	tooManyColumns.Tables[0].Columns = make([]SnapshotQuerySchemaColumnV1, SnapshotQuerySchemaMaxColumnsPerTable+1)
	for i := range tooManyColumns.Tables[0].Columns {
		tooManyColumns.Tables[0].Columns[i] = SnapshotQuerySchemaColumnV1{Name: string(rune(i + 1)), Type: "UInt8", Generation: SnapshotQueryColumnGenerationOrdinary}
	}
	if _, err := EncodeCanonicalSnapshotQuerySchemaArtifactV1(tooManyColumns); err == nil {
		t.Fatal("per-table column bound not enforced")
	}
	tooManyTotal := base.Clone()
	tooManyTotal.Tables = make([]SnapshotQueryTableSchemaV1, 17)
	for i := range tooManyTotal.Tables {
		tooManyTotal.Tables[i] = base.Tables[0]
		tooManyTotal.Tables[i].TableID = fmt.Sprintf("table-%02d", i)
		tooManyTotal.Tables[i].Columns = make([]SnapshotQuerySchemaColumnV1, SnapshotQuerySchemaMaxColumnsPerTable)
		for j := range tooManyTotal.Tables[i].Columns {
			tooManyTotal.Tables[i].Columns[j] = SnapshotQuerySchemaColumnV1{Name: fmt.Sprintf("column-%04d", j), Type: "UInt8", Generation: SnapshotQueryColumnGenerationOrdinary}
		}
	}
	if _, err := EncodeCanonicalSnapshotQuerySchemaArtifactV1(tooManyTotal); err == nil {
		t.Fatal("total column bound not enforced")
	}
	oversized := bytes.Repeat([]byte{' '}, SnapshotQuerySchemaMaxCanonicalBytes+1)
	if _, err := DecodeCanonicalSnapshotQuerySchemaArtifactV1(oversized); err == nil {
		t.Fatal("byte bound not enforced before decoding")
	}
}

func TestAuthenticatedSnapshotQuerySchemaStrictOuterBytes(t *testing.T) {
	vector := schemaVector(t)
	authenticatedJSON := vector.authenticatedJSON()
	authorityJWS := vector.authorityJWS()
	authenticated, err := DecodeCanonicalAuthenticatedSnapshotQuerySchemaV1([]byte(authenticatedJSON))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := EncodeCanonicalAuthenticatedSnapshotQuerySchemaV1(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != authenticatedJSON || authenticated.AuthorityJWS != authorityJWS || !bytes.Equal(raw, mustSchemaJSON(t, authenticated)) {
		t.Fatal("outer schema artifact bytes changed")
	}
	digest, err := AuthenticatedSnapshotQuerySchemaDigestV1(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	if independent := fmt.Sprintf("0x%x", sha256.Sum256(raw)); digest != vector.AuthenticatedDigest || digest != independent {
		t.Fatalf("authenticated digest=%s want=%s independent=%s", digest, vector.AuthenticatedDigest, independent)
	}
	for _, bad := range [][]byte{
		append([]byte(" "), raw...),
		bytes.Replace(raw, []byte(`{"artifact":`), []byte(`{"unknown":0,"artifact":`), 1),
		bytes.Replace(raw, []byte(`{"artifact":`), []byte(`{"artifact":null,"artifact":`), 1),
		bytes.Replace(raw, []byte(`,"authority_jws":"`+authorityJWS+`"`), nil, 1),
		bytes.Replace(raw, []byte(`"authority_jws":"`+authorityJWS+`"`), []byte(`"authority_jws":"`+authorityJWS+`","authority_jws":"`+authorityJWS+`"`), 1),
		bytes.Replace(raw, []byte(`"authority_jws":"`+authorityJWS+`"`), []byte(`"authority_jws":null`), 1),
		bytes.Repeat([]byte{' '}, SnapshotQuerySchemaMaxCanonicalBytes+1),
	} {
		if _, err := DecodeCanonicalAuthenticatedSnapshotQuerySchemaV1(bad); err == nil {
			t.Fatal("noncanonical outer schema artifact accepted")
		}
	}
}

func TestSnapshotQuerySchemaGenerationMetadataIsSeparatelyCommitted(t *testing.T) {
	artifact := validSchemaArtifact(t)
	originalDigest, err := SnapshotQuerySchemaArtifactDigestV1(artifact)
	if err != nil {
		t.Fatal(err)
	}
	originalLegacyHash := artifact.Tables[0].SchemaHash
	artifact.Tables[0].Columns[0].Generation = SnapshotQueryColumnGenerationDefault
	artifact.Tables[0].Columns[0].DefaultExpression = "0"
	changedDigest, err := SnapshotQuerySchemaArtifactDigestV1(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if changedDigest == originalDigest || artifact.Tables[0].SchemaHash != originalLegacyHash {
		t.Fatal("generation metadata must change A without redefining the legacy schema hash")
	}
}

func mustSchemaJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
