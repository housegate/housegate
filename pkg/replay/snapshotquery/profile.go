package snapshotquery

import (
	"fmt"

	"github.com/housegate/housegate/pkg/auth"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Limits is the exact authenticated query-profile resource contract.
type Limits = replay.QueryLimits

// ValidatedSchemaProfile is an immutable-by-copy result of complete schema,
// certificate, manifest and legacy-projection validation. It is not proof of
// current authority, publication, freshness, retention, or query eligibility.
type ValidatedSchemaProfile struct {
	authenticated replay.AuthenticatedSnapshotQuerySchemaV1
	authority     string
	schemas       []payloadexec.TableSchema
}

func (p ValidatedSchemaProfile) Authority() string { return p.authority }

func (p ValidatedSchemaProfile) SchemaArtifact() replay.AuthenticatedSnapshotQuerySchemaV1 {
	out := p.authenticated
	out.Artifact = p.authenticated.Artifact.Clone()
	return out
}

func (p ValidatedSchemaProfile) Schemas() []payloadexec.TableSchema {
	return cloneSchemas(p.schemas)
}

// ValidateAuthenticatedSchemaProfile composes the pure B1.1 validation path:
// exact S/pin binding, inner A digest and certificate, outer O digest, complete
// manifest ledger, and unchanged payloadexec table/schema-root projections.
func ValidateAuthenticatedSchemaProfile(manifest replay.SafeSnapshotManifest, pin replay.SnapshotPin, authenticated replay.AuthenticatedSnapshotQuerySchemaV1, schemaArtifactDigest string) (ValidatedSchemaProfile, error) {
	if err := manifest.Validate(); err != nil {
		return ValidatedSchemaProfile{}, fmt.Errorf("manifest: %w", err)
	}
	if err := validateManifestPin(manifest, pin); err != nil {
		return ValidatedSchemaProfile{}, err
	}
	artifact := authenticated.Artifact
	if artifact.NetworkID != pin.NetworkID || artifact.KeeperShardID != pin.KeeperShardID {
		return ValidatedSchemaProfile{}, fmt.Errorf("schema artifact network or keeper shard mismatch")
	}
	if artifact.SnapshotID != pin.SnapshotID || artifact.ManifestRoot != pin.ManifestRoot || artifact.SchemaSnapshotID != pin.SchemaSnapshotID || artifact.SchemaRoot != pin.SchemaRoot {
		return ValidatedSchemaProfile{}, fmt.Errorf("schema artifact snapshot pin mismatch")
	}
	innerDigest, err := replay.SnapshotQuerySchemaArtifactDigestV1(artifact)
	if err != nil {
		return ValidatedSchemaProfile{}, fmt.Errorf("schema artifact: %w", err)
	}
	authority, err := auth.VerifySnapshotSchemaCertificateV1(authenticated.AuthorityJWS, innerDigest)
	if err != nil {
		return ValidatedSchemaProfile{}, fmt.Errorf("schema certificate: %w", err)
	}
	outerDigest, err := replay.AuthenticatedSnapshotQuerySchemaDigestV1(authenticated)
	if err != nil {
		return ValidatedSchemaProfile{}, fmt.Errorf("authenticated schema artifact: %w", err)
	}
	if schemaArtifactDigest == "" || outerDigest != schemaArtifactDigest {
		return ValidatedSchemaProfile{}, fmt.Errorf("schema_artifact_digest mismatch")
	}

	manifestTables := make(map[string]replay.TableManifest, len(manifest.Tables))
	for _, table := range manifest.Tables {
		if _, exists := manifestTables[table.TableID]; exists {
			return ValidatedSchemaProfile{}, fmt.Errorf("duplicate manifest table %q", table.TableID)
		}
		manifestTables[table.TableID] = table
	}
	if len(artifact.Tables) != len(manifestTables) {
		return ValidatedSchemaProfile{}, fmt.Errorf("schema artifact does not contain the complete manifest table ledger")
	}
	schemas := make([]payloadexec.TableSchema, 0, len(artifact.Tables))
	for _, table := range artifact.Tables {
		manifestTable, ok := manifestTables[table.TableID]
		if !ok {
			return ValidatedSchemaProfile{}, fmt.Errorf("schema table %q is absent from manifest", table.TableID)
		}
		columns := make([]lthash.Column, len(table.Columns))
		for i, column := range table.Columns {
			columns[i] = lthash.Column{Name: column.Name, Type: column.Type}
		}
		schema := payloadexec.TableSchema{TableID: table.TableID, PartitionBy: table.PartitionBy, Columns: columns}
		computed := payloadexec.TableSchemaHash(artifact.NetworkID, schema)
		if computed != table.SchemaHash {
			return ValidatedSchemaProfile{}, fmt.Errorf("schema table %q legacy schema_hash mismatch", table.TableID)
		}
		if manifestTable.SchemaHash != table.SchemaHash {
			return ValidatedSchemaProfile{}, fmt.Errorf("schema table %q manifest schema_hash mismatch", table.TableID)
		}
		schemas = append(schemas, schema)
		delete(manifestTables, table.TableID)
	}
	if len(manifestTables) != 0 {
		return ValidatedSchemaProfile{}, fmt.Errorf("schema artifact omitted manifest tables")
	}
	if root := payloadexec.SchemaRoot(artifact.NetworkID, schemas); root != artifact.SchemaRoot {
		return ValidatedSchemaProfile{}, fmt.Errorf("complete legacy schema_root mismatch")
	}
	return ValidatedSchemaProfile{
		authenticated: replay.AuthenticatedSnapshotQuerySchemaV1{Artifact: artifact.Clone(), AuthorityJWS: authenticated.AuthorityJWS},
		authority:     authority,
		schemas:       cloneSchemas(schemas),
	}, nil
}

func validateManifestPin(manifest replay.SafeSnapshotManifest, pin replay.SnapshotPin) error {
	if pin.NetworkID == "" {
		return fmt.Errorf("snapshot pin network_id is required")
	}
	if pin.SnapshotID != manifest.SnapshotID || pin.SafeBlockSeq != manifest.SafeBlockSeq || pin.ManifestRoot != manifest.ManifestRoot || pin.StateRoot != manifest.StateRoot || pin.SchemaSnapshotID != manifest.SchemaSnapshotID || pin.SchemaRoot != manifest.SchemaRoot {
		return fmt.Errorf("selected manifest pin mismatch")
	}
	return nil
}

// ValidateInitialTableColumnProfile applies only the initial query eligibility
// profile to one explicitly selected target/read table. Canonical decoding and
// complete-ledger validation intentionally do not call it, so unrelated U is
// preserved and cannot decide another query's eligibility.
func ValidateInitialTableColumnProfile(table replay.SnapshotQueryTableSchemaV1) error {
	if len(table.Columns) == 0 {
		return fmt.Errorf("table %q has no declared user columns", table.TableID)
	}
	for _, column := range table.Columns {
		if column.Generation != replay.SnapshotQueryColumnGenerationOrdinary || column.DefaultExpression != "" {
			return fmt.Errorf("table %q column %q is not explicit ORDINARY with an empty default expression", table.TableID, column.Name)
		}
	}
	return nil
}

func cloneSchemas(in []payloadexec.TableSchema) []payloadexec.TableSchema {
	out := append([]payloadexec.TableSchema{}, in...)
	for i := range out {
		out[i].Columns = append([]lthash.Column{}, in[i].Columns...)
	}
	return out
}
