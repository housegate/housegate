package snapshotquery

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

const schemaProfileTestKey = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func profileFixture(t *testing.T) (replay.SafeSnapshotManifest, replay.SnapshotPin, replay.AuthenticatedSnapshotQuerySchemaV1, string) {
	t.Helper()
	schemas := []payloadexec.TableSchema{
		{TableID: "orders", PartitionBy: "day", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "day", Type: "Date"}, {Name: "amount", Type: "Int64"}}},
		{TableID: "untouched", Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "created_at", Type: "DateTime"}}},
	}
	manifest, err := payloadexec.New("network-fixture-1", schemas...).GenesisSnapshot(12, "schema-fixture-1", "executor-fixture-1")
	if err != nil {
		t.Fatal(err)
	}
	pin := replay.SnapshotPin{
		NetworkID:        "network-fixture-1",
		KeeperShardID:    7,
		SnapshotID:       manifest.SnapshotID,
		SafeBlockSeq:     manifest.SafeBlockSeq,
		ManifestRoot:     manifest.ManifestRoot,
		StateRoot:        manifest.StateRoot,
		SchemaSnapshotID: manifest.SchemaSnapshotID,
		SchemaRoot:       manifest.SchemaRoot,
	}
	artifact := replay.SnapshotQuerySchemaArtifactV1{
		Kind:             replay.SnapshotQuerySchemaArtifactKindV1,
		Version:          replay.SnapshotQuerySchemaArtifactVersionV1,
		NetworkID:        pin.NetworkID,
		KeeperShardID:    pin.KeeperShardID,
		SnapshotID:       pin.SnapshotID,
		ManifestRoot:     pin.ManifestRoot,
		SchemaSnapshotID: pin.SchemaSnapshotID,
		SchemaRoot:       pin.SchemaRoot,
		Tables: []replay.SnapshotQueryTableSchemaV1{
			{TableID: "orders", SchemaHash: payloadexec.TableSchemaHash(pin.NetworkID, schemas[0]), PartitionBy: "day", Columns: []replay.SnapshotQuerySchemaColumnV1{{Name: "id", Type: "UInt64", Generation: replay.SnapshotQueryColumnGenerationOrdinary}, {Name: "day", Type: "Date", Generation: replay.SnapshotQueryColumnGenerationOrdinary}, {Name: "amount", Type: "Int64", Generation: replay.SnapshotQueryColumnGenerationOrdinary}}},
			{TableID: "untouched", SchemaHash: payloadexec.TableSchemaHash(pin.NetworkID, schemas[1]), Columns: []replay.SnapshotQuerySchemaColumnV1{{Name: "id", Type: "UInt64", Generation: replay.SnapshotQueryColumnGenerationOrdinary}, {Name: "created_at", Type: "DateTime", Generation: replay.SnapshotQueryColumnGenerationDefault, DefaultExpression: "now()"}}},
		},
	}
	return signProfile(t, manifest, pin, artifact)
}

func signProfile(t *testing.T, manifest replay.SafeSnapshotManifest, pin replay.SnapshotPin, artifact replay.SnapshotQuerySchemaArtifactV1) (replay.SafeSnapshotManifest, replay.SnapshotPin, replay.AuthenticatedSnapshotQuerySchemaV1, string) {
	t.Helper()
	digest, err := replay.SnapshotQuerySchemaArtifactDigestV1(artifact)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := auth.NewSnapshotSchemaCertificateSigner(schemaProfileTestKey)
	if err != nil {
		t.Fatal(err)
	}
	token, err := signer.SignSnapshotSchemaCertificateV1(digest)
	if err != nil {
		t.Fatal(err)
	}
	authenticated := replay.AuthenticatedSnapshotQuerySchemaV1{Artifact: artifact, AuthorityJWS: token}
	outerDigest, err := replay.AuthenticatedSnapshotQuerySchemaDigestV1(authenticated)
	if err != nil {
		t.Fatal(err)
	}
	return manifest, pin, authenticated, outerDigest
}

func TestValidateAuthenticatedSchemaProfileCompleteProjection(t *testing.T) {
	manifest, pin, schema, outerDigest := profileFixture(t)
	profile, err := ValidateAuthenticatedSchemaProfile(manifest, pin, schema, outerDigest)
	if err != nil {
		t.Fatal(err)
	}
	if profile.Authority() != "0x8fd379246834eac74b8419ffda202cf8051f7a03" {
		t.Fatalf("authority=%s", profile.Authority())
	}
	projected := profile.Schemas()
	if len(projected) != 2 || projected[0].TableID != "orders" || projected[1].TableID != "untouched" {
		t.Fatalf("incomplete projection: %+v", projected)
	}
	if payloadexec.SchemaRoot(pin.NetworkID, projected) != pin.SchemaRoot {
		t.Fatal("projection does not reproduce schema_root")
	}
	if err := ValidateInitialTableColumnProfile(schema.Artifact.Tables[0]); err != nil {
		t.Fatalf("eligible target rejected: %v", err)
	}
	if err := ValidateInitialTableColumnProfile(schema.Artifact.Tables[1]); err == nil {
		t.Fatal("generated column in explicitly selected table accepted")
	}
	// The complete authenticated ledger retains unrelated ineligible U without
	// letting its eligibility decide an orders-only query.
	if len(profile.SchemaArtifact().Artifact.Tables) != 2 {
		t.Fatal("unrelated untouched table U was omitted")
	}

	// Returned values never alias the caller or another getter result.
	schema.Artifact.Tables[0].Columns[0].Name = "mutated-caller"
	projected[0].Columns[0].Name = "mutated-result"
	if got := profile.Schemas()[0].Columns[0].Name; got != "id" {
		t.Fatalf("mutable schema alias escaped: %s", got)
	}
	copyArtifact := profile.SchemaArtifact()
	copyArtifact.Artifact.Tables[0].Columns[0].Name = "mutated-artifact"
	if got := profile.SchemaArtifact().Artifact.Tables[0].Columns[0].Name; got != "id" {
		t.Fatalf("mutable artifact alias escaped: %s", got)
	}
}

func TestValidateAuthenticatedSchemaProfileBindsEveryLayer(t *testing.T) {
	manifest, pin, schema, outerDigest := profileFixture(t)
	for name, mutate := range map[string]func(*replay.SafeSnapshotManifest, *replay.SnapshotPin, *replay.AuthenticatedSnapshotQuerySchemaV1, *string){
		"outer_digest": func(_ *replay.SafeSnapshotManifest, _ *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, d *string) {
			*d = "0x" + strings.Repeat("f", 64)
		},
		"network": func(_ *replay.SafeSnapshotManifest, p *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			p.NetworkID = "other"
		},
		"shard": func(_ *replay.SafeSnapshotManifest, p *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			p.KeeperShardID++
		},
		"snapshot": func(_ *replay.SafeSnapshotManifest, p *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			p.SnapshotID = "0x" + strings.Repeat("f", 64)
		},
		"safe_seq": func(_ *replay.SafeSnapshotManifest, p *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			p.SafeBlockSeq++
		},
		"state_root": func(_ *replay.SafeSnapshotManifest, p *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			p.StateRoot = "0x" + strings.Repeat("f", 64)
		},
		"manifest_root": func(_ *replay.SafeSnapshotManifest, p *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			p.ManifestRoot = "0x" + strings.Repeat("f", 64)
		},
		"schema_snapshot": func(_ *replay.SafeSnapshotManifest, p *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			p.SchemaSnapshotID = "other"
		},
		"schema_root": func(_ *replay.SafeSnapshotManifest, p *replay.SnapshotPin, _ *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			p.SchemaRoot = "0x" + strings.Repeat("f", 64)
		},
		"certificate": func(_ *replay.SafeSnapshotManifest, _ *replay.SnapshotPin, s *replay.AuthenticatedSnapshotQuerySchemaV1, _ *string) {
			s.AuthorityJWS += "x"
		},
	} {
		t.Run(name, func(t *testing.T) {
			m, p, s, d := manifest, pin, schema, outerDigest
			mutate(&m, &p, &s, &d)
			if _, err := ValidateAuthenticatedSchemaProfile(m, p, s, d); err == nil {
				t.Fatal("layer mismatch accepted")
			}
		})
	}
}

func TestValidateAuthenticatedSchemaProfileRejectsIncompleteOrChangedProjection(t *testing.T) {
	manifest, pin, schema, _ := profileFixture(t)
	for name, mutate := range map[string]func(*replay.SnapshotQuerySchemaArtifactV1){
		"omit_U":       func(a *replay.SnapshotQuerySchemaArtifactV1) { a.Tables = a.Tables[:1] },
		"change_name":  func(a *replay.SnapshotQuerySchemaArtifactV1) { a.Tables[0].Columns[0].Name = "other" },
		"change_type":  func(a *replay.SnapshotQuerySchemaArtifactV1) { a.Tables[0].Columns[0].Type = "UInt32" },
		"partition_by": func(a *replay.SnapshotQuerySchemaArtifactV1) { a.Tables[0].PartitionBy = "id" },
		"schema_hash":  func(a *replay.SnapshotQuerySchemaArtifactV1) { a.Tables[0].SchemaHash = "0x" + strings.Repeat("f", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			changed := schema.Artifact.Clone()
			mutate(&changed)
			_, _, authenticated, outerDigest := signProfile(t, manifest, pin, changed)
			if _, err := ValidateAuthenticatedSchemaProfile(manifest, pin, authenticated, outerDigest); err == nil {
				t.Fatal("incomplete or changed legacy projection accepted")
			}
		})
	}
}

func TestValidateAuthenticatedSchemaProfileGenerationIsAuthenticatedButEligibilityIsSeparate(t *testing.T) {
	manifest, pin, schema, _ := profileFixture(t)
	changed := schema.Artifact.Clone()
	changed.Tables[0].Columns[0].Generation = replay.SnapshotQueryColumnGenerationDefault
	changed.Tables[0].Columns[0].DefaultExpression = "0"
	_, _, authenticated, outerDigest := signProfile(t, manifest, pin, changed)
	if _, err := ValidateAuthenticatedSchemaProfile(manifest, pin, authenticated, outerDigest); err != nil {
		t.Fatalf("authenticated generation metadata changed legacy projection: %v", err)
	}
	if err := ValidateInitialTableColumnProfile(changed.Tables[0]); err == nil {
		t.Fatal("DEFAULT column admitted by initial profile")
	}
	for _, generation := range []uint32{replay.SnapshotQueryColumnGenerationUnspecified, replay.SnapshotQueryColumnGenerationMaterialized, replay.SnapshotQueryColumnGenerationAlias, replay.SnapshotQueryColumnGenerationOther, 99} {
		table := schema.Artifact.Tables[0]
		table.Columns[0].Generation = generation
		if err := ValidateInitialTableColumnProfile(table); err == nil {
			t.Fatalf("generation %d admitted", generation)
		}
	}
	table := schema.Artifact.Tables[0]
	table.Columns[0].DefaultExpression = "0"
	if err := ValidateInitialTableColumnProfile(table); err == nil {
		t.Fatal("ordinary column with default expression admitted")
	}
}

func TestValidateAuthenticatedSchemaProfileEmptyManifestSemantics(t *testing.T) {
	manifest, err := payloadexec.New("network-fixture-1").GenesisSnapshot(0, "schema-fixture-1", "executor-fixture-1")
	if err != nil {
		t.Fatal(err)
	}
	pin := replay.SnapshotPin{NetworkID: "network-fixture-1", KeeperShardID: 7, SnapshotID: manifest.SnapshotID, SafeBlockSeq: manifest.SafeBlockSeq, ManifestRoot: manifest.ManifestRoot, StateRoot: manifest.StateRoot, SchemaSnapshotID: manifest.SchemaSnapshotID, SchemaRoot: manifest.SchemaRoot}
	artifact := replay.SnapshotQuerySchemaArtifactV1{Kind: replay.SnapshotQuerySchemaArtifactKindV1, Version: 1, NetworkID: pin.NetworkID, KeeperShardID: pin.KeeperShardID, SnapshotID: pin.SnapshotID, ManifestRoot: pin.ManifestRoot, SchemaSnapshotID: pin.SchemaSnapshotID, SchemaRoot: pin.SchemaRoot, Tables: []replay.SnapshotQueryTableSchemaV1{}}
	_, _, schema, outerDigest := signProfile(t, manifest, pin, artifact)
	profile, err := ValidateAuthenticatedSchemaProfile(manifest, pin, schema, outerDigest)
	if err != nil || len(profile.Schemas()) != 0 {
		t.Fatalf("empty manifest/schema pair rejected: %+v %v", profile, err)
	}

	_, nonemptyPin, nonempty, _ := profileFixture(t)
	_, _, wrong, wrongDigest := signProfile(t, manifest, pin, nonempty.Artifact)
	if _, err := ValidateAuthenticatedSchemaProfile(manifest, pin, wrong, wrongDigest); err == nil {
		t.Fatal("declared tables accepted for empty manifest")
	}
	_ = nonemptyPin
}
