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
	SnapshotQuerySchemaArtifactKindV1    = "snapshot-query-schema-artifact-v1"
	SnapshotQuerySchemaArtifactVersionV1 = uint32(1)

	SnapshotQuerySchemaMaxCanonicalBytes  = 8 << 20
	SnapshotQuerySchemaMaxTables          = 4096
	SnapshotQuerySchemaMaxColumnsPerTable = 4096
	SnapshotQuerySchemaMaxTotalColumns    = 65536

	SnapshotQueryColumnGenerationUnspecified  = uint32(0)
	SnapshotQueryColumnGenerationOrdinary     = uint32(1)
	SnapshotQueryColumnGenerationDefault      = uint32(2)
	SnapshotQueryColumnGenerationMaterialized = uint32(3)
	SnapshotQueryColumnGenerationAlias        = uint32(4)
	SnapshotQueryColumnGenerationOther        = uint32(5)
)

// SnapshotQuerySchemaArtifactV1 is the complete declaration-order schema
// ledger authenticated for one exact snapshot. Generation metadata is outside
// the legacy name/type schema hashes but remains inside this artifact's digest.
type SnapshotQuerySchemaArtifactV1 struct {
	Kind             string                       `json:"kind"`
	Version          uint32                       `json:"version"`
	NetworkID        string                       `json:"network_id"`
	KeeperShardID    uint32                       `json:"keeper_shard_id"`
	SnapshotID       string                       `json:"snapshot_id"`
	ManifestRoot     string                       `json:"manifest_root"`
	SchemaSnapshotID string                       `json:"schema_snapshot_id"`
	SchemaRoot       string                       `json:"schema_root"`
	Tables           []SnapshotQueryTableSchemaV1 `json:"tables"`
}

type SnapshotQueryTableSchemaV1 struct {
	TableID     string                        `json:"table_id"`
	SchemaHash  string                        `json:"schema_hash"`
	PartitionBy string                        `json:"partition_by"`
	Columns     []SnapshotQuerySchemaColumnV1 `json:"columns"`
}

type SnapshotQuerySchemaColumnV1 struct {
	Name              string `json:"name"`
	Type              string `json:"type"`
	Generation        uint32 `json:"generation"`
	DefaultExpression string `json:"default_expression"`
}

// AuthenticatedSnapshotQuerySchemaV1 carries the exact schema artifact and
// its compact schema-authority certificate. Signature and content validation
// compose in the snapshotquery leaf to avoid a replay/auth package cycle.
type AuthenticatedSnapshotQuerySchemaV1 struct {
	Artifact     SnapshotQuerySchemaArtifactV1 `json:"artifact"`
	AuthorityJWS string                        `json:"authority_jws"`
}

func (a SnapshotQuerySchemaArtifactV1) MarshalJSON() ([]byte, error) {
	type plain SnapshotQuerySchemaArtifactV1
	if a.Tables == nil {
		a.Tables = []SnapshotQueryTableSchemaV1{}
	}
	return json.Marshal(plain(a))
}

func (t SnapshotQueryTableSchemaV1) MarshalJSON() ([]byte, error) {
	type plain SnapshotQueryTableSchemaV1
	if t.Columns == nil {
		t.Columns = []SnapshotQuerySchemaColumnV1{}
	}
	return json.Marshal(plain(t))
}

// Clone returns a complete independently owned copy.
func (a SnapshotQuerySchemaArtifactV1) Clone() SnapshotQuerySchemaArtifactV1 {
	out := a
	out.Tables = append([]SnapshotQueryTableSchemaV1{}, a.Tables...)
	for i := range out.Tables {
		out.Tables[i].Columns = append([]SnapshotQuerySchemaColumnV1{}, a.Tables[i].Columns...)
	}
	return out
}

// ValidateSnapshotQuerySchemaArtifactV1 checks the canonical record itself.
// It deliberately does not grant column-profile eligibility or authenticate a
// signer, manifest publication, freshness, retention, or current authority.
func ValidateSnapshotQuerySchemaArtifactV1(a SnapshotQuerySchemaArtifactV1) error {
	if a.Kind != SnapshotQuerySchemaArtifactKindV1 || a.Version != SnapshotQuerySchemaArtifactVersionV1 {
		return fmt.Errorf("unsupported snapshot query schema kind or version")
	}
	for name, value := range map[string]string{
		"network_id":         a.NetworkID,
		"snapshot_id":        a.SnapshotID,
		"manifest_root":      a.ManifestRoot,
		"schema_snapshot_id": a.SchemaSnapshotID,
		"schema_root":        a.SchemaRoot,
	} {
		if err := validateSchemaIdentity(name, value); err != nil {
			return err
		}
	}
	for name, digest := range map[string]string{
		"snapshot_id":   a.SnapshotID,
		"manifest_root": a.ManifestRoot,
		"schema_root":   a.SchemaRoot,
	} {
		if !isCanonicalReplayDigest(digest) {
			return fmt.Errorf("%s must be a lowercase 0x-prefixed SHA-256 digest", name)
		}
	}
	if len(a.Tables) > SnapshotQuerySchemaMaxTables {
		return fmt.Errorf("schema table count %d exceeds %d", len(a.Tables), SnapshotQuerySchemaMaxTables)
	}
	totalColumns := 0
	for i, table := range a.Tables {
		if err := validateSchemaIdentity(fmt.Sprintf("tables[%d].table_id", i), table.TableID); err != nil {
			return err
		}
		if !isCanonicalReplayDigest(table.SchemaHash) {
			return fmt.Errorf("tables[%d].schema_hash must be a lowercase 0x-prefixed SHA-256 digest", i)
		}
		if strings.ContainsRune(table.PartitionBy, '\x00') {
			return fmt.Errorf("tables[%d].partition_by contains NUL", i)
		}
		if i > 0 && a.Tables[i-1].TableID >= table.TableID {
			return fmt.Errorf("schema tables must be unique and sorted by table_id")
		}
		if len(table.Columns) == 0 {
			return fmt.Errorf("tables[%d].columns must contain the complete declared schema", i)
		}
		if len(table.Columns) > SnapshotQuerySchemaMaxColumnsPerTable {
			return fmt.Errorf("tables[%d] column count %d exceeds %d", i, len(table.Columns), SnapshotQuerySchemaMaxColumnsPerTable)
		}
		totalColumns += len(table.Columns)
		if totalColumns > SnapshotQuerySchemaMaxTotalColumns {
			return fmt.Errorf("total schema column count exceeds %d", SnapshotQuerySchemaMaxTotalColumns)
		}
		seen := make(map[string]struct{}, len(table.Columns))
		for j, column := range table.Columns {
			if err := validateSchemaIdentity(fmt.Sprintf("tables[%d].columns[%d].name", i, j), column.Name); err != nil {
				return err
			}
			if err := validateSchemaIdentity(fmt.Sprintf("tables[%d].columns[%d].type", i, j), column.Type); err != nil {
				return err
			}
			if _, ok := seen[column.Name]; ok {
				return fmt.Errorf("tables[%d] has duplicate column name %q", i, column.Name)
			}
			seen[column.Name] = struct{}{}
			if strings.ContainsRune(column.DefaultExpression, '\x00') {
				return fmt.Errorf("tables[%d].columns[%d].default_expression contains NUL", i, j)
			}
		}
	}
	return nil
}

func validateSchemaIdentity(name, value string) error {
	if strings.TrimSpace(value) == "" || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s is malformed", name)
	}
	return nil
}

func isCanonicalReplayDigest(value string) bool {
	if len(value) != 66 || value[:2] != "0x" {
		return false
	}
	for _, c := range value[2:] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

func EncodeCanonicalSnapshotQuerySchemaArtifactV1(a SnapshotQuerySchemaArtifactV1) ([]byte, error) {
	if err := ValidateSnapshotQuerySchemaArtifactV1(a); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return nil, fmt.Errorf("encode snapshot query schema artifact: %w", err)
	}
	if len(raw) > SnapshotQuerySchemaMaxCanonicalBytes {
		return nil, fmt.Errorf("schema artifact is %d bytes; maximum is %d", len(raw), SnapshotQuerySchemaMaxCanonicalBytes)
	}
	return raw, nil
}

func DecodeCanonicalSnapshotQuerySchemaArtifactV1(raw []byte) (SnapshotQuerySchemaArtifactV1, error) {
	if err := checkSchemaCanonicalShape(raw, reflect.TypeOf(SnapshotQuerySchemaArtifactV1{}), "schema_artifact"); err != nil {
		return SnapshotQuerySchemaArtifactV1{}, err
	}
	var artifact SnapshotQuerySchemaArtifactV1
	if err := json.Unmarshal(raw, &artifact); err != nil {
		return SnapshotQuerySchemaArtifactV1{}, fmt.Errorf("decode snapshot query schema artifact: %w", err)
	}
	canonical, err := EncodeCanonicalSnapshotQuerySchemaArtifactV1(artifact)
	if err != nil {
		return SnapshotQuerySchemaArtifactV1{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return SnapshotQuerySchemaArtifactV1{}, fmt.Errorf("noncanonical snapshot query schema artifact bytes")
	}
	return artifact.Clone(), nil
}

func SnapshotQuerySchemaArtifactDigestV1(a SnapshotQuerySchemaArtifactV1) (string, error) {
	raw, err := EncodeCanonicalSnapshotQuerySchemaArtifactV1(a)
	if err != nil {
		return "", err
	}
	return DigestBytes(raw), nil
}

func EncodeCanonicalAuthenticatedSnapshotQuerySchemaV1(a AuthenticatedSnapshotQuerySchemaV1) ([]byte, error) {
	if a.AuthorityJWS == "" {
		return nil, fmt.Errorf("authority_jws is required")
	}
	if err := ValidateSnapshotQuerySchemaArtifactV1(a.Artifact); err != nil {
		return nil, fmt.Errorf("artifact: %w", err)
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return nil, fmt.Errorf("encode authenticated snapshot query schema: %w", err)
	}
	if len(raw) > SnapshotQuerySchemaMaxCanonicalBytes {
		return nil, fmt.Errorf("authenticated schema artifact is %d bytes; maximum is %d", len(raw), SnapshotQuerySchemaMaxCanonicalBytes)
	}
	return raw, nil
}

func DecodeCanonicalAuthenticatedSnapshotQuerySchemaV1(raw []byte) (AuthenticatedSnapshotQuerySchemaV1, error) {
	if err := checkSchemaCanonicalShape(raw, reflect.TypeOf(AuthenticatedSnapshotQuerySchemaV1{}), "authenticated_schema"); err != nil {
		return AuthenticatedSnapshotQuerySchemaV1{}, err
	}
	var authenticated AuthenticatedSnapshotQuerySchemaV1
	if err := json.Unmarshal(raw, &authenticated); err != nil {
		return AuthenticatedSnapshotQuerySchemaV1{}, fmt.Errorf("decode authenticated snapshot query schema: %w", err)
	}
	canonical, err := EncodeCanonicalAuthenticatedSnapshotQuerySchemaV1(authenticated)
	if err != nil {
		return AuthenticatedSnapshotQuerySchemaV1{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return AuthenticatedSnapshotQuerySchemaV1{}, fmt.Errorf("noncanonical authenticated snapshot query schema bytes")
	}
	authenticated.Artifact = authenticated.Artifact.Clone()
	return authenticated, nil
}

func AuthenticatedSnapshotQuerySchemaDigestV1(a AuthenticatedSnapshotQuerySchemaV1) (string, error) {
	raw, err := EncodeCanonicalAuthenticatedSnapshotQuerySchemaV1(a)
	if err != nil {
		return "", err
	}
	return DigestBytes(raw), nil
}

type schemaShapeCount struct {
	totalColumns int
}

func checkSchemaCanonicalShape(raw []byte, typ reflect.Type, path string) error {
	if len(raw) > SnapshotQuerySchemaMaxCanonicalBytes {
		return fmt.Errorf("%s is %d bytes; maximum is %d", path, len(raw), SnapshotQuerySchemaMaxCanonicalBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := boundedSchemaJSONShape(decoder, typ, path, &schemaShapeCount{}); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return fmt.Errorf("%s: trailing JSON", path)
	}
	return nil
}

func boundedSchemaJSONShape(decoder *json.Decoder, typ reflect.Type, path string, count *schemaShapeCount) error {
	token, err := decoder.Token()
	if err != nil {
		return err
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
			fields[typ.Field(i).Tag.Get("json")] = typ.Field(i).Type
		}
		seen := make(map[string]struct{}, typ.NumField())
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("%s: invalid key", path)
			}
			fieldType, ok := fields[key]
			if !ok {
				return fmt.Errorf("%s: unknown field %q", path, key)
			}
			if _, ok := seen[key]; ok {
				return fmt.Errorf("%s: duplicate field %q", path, key)
			}
			seen[key] = struct{}{}
			if err := boundedSchemaJSONShape(decoder, fieldType, path+"."+key, count); err != nil {
				return err
			}
		}
		if _, err := decoder.Token(); err != nil {
			return err
		}
		for key := range fields {
			if _, ok := seen[key]; !ok {
				return fmt.Errorf("%s: missing field %q", path, key)
			}
		}
	case reflect.Slice:
		if token != json.Delim('[') {
			return fmt.Errorf("%s: expected array", path)
		}
		items := 0
		for decoder.More() {
			items++
			switch typ.Elem() {
			case reflect.TypeOf(SnapshotQueryTableSchemaV1{}):
				if items > SnapshotQuerySchemaMaxTables {
					return fmt.Errorf("%s exceeds %d tables", path, SnapshotQuerySchemaMaxTables)
				}
			case reflect.TypeOf(SnapshotQuerySchemaColumnV1{}):
				if items > SnapshotQuerySchemaMaxColumnsPerTable {
					return fmt.Errorf("%s exceeds %d columns", path, SnapshotQuerySchemaMaxColumnsPerTable)
				}
				count.totalColumns++
				if count.totalColumns > SnapshotQuerySchemaMaxTotalColumns {
					return fmt.Errorf("schema exceeds %d total columns", SnapshotQuerySchemaMaxTotalColumns)
				}
			}
			if err := boundedSchemaJSONShape(decoder, typ.Elem(), fmt.Sprintf("%s[%d]", path, items-1), count); err != nil {
				return err
			}
		}
		_, err = decoder.Token()
		return err
	default:
		if _, ok := token.(json.Delim); ok {
			return fmt.Errorf("%s: expected scalar", path)
		}
	}
	return nil
}
