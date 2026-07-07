package replay

// ComputeDataRoot returns the commitment over active table data. It is
// content-addressed: per table only {TableID, PartitionRoots} enter the digest.
// Partition roots are LtHash sums of row content, so the data root is
// independent of part names, physical hashes, sizes, and storage refs.
// ActiveParts stay covered by ComputeManifestRoot. The domain is versioned so
// pre-P1c roots can never be silently compared with P1c roots.
func (m SafeSnapshotManifest) ComputeDataRoot() (string, error) {
	type tableData struct {
		TableID        string                `json:"table_id"`
		PartitionRoots []PartitionCommitment `json:"partition_roots"`
	}

	var tables []tableData
	for _, t := range m.normalized().Tables {
		tables = append(tables, tableData{
			TableID:        t.TableID,
			PartitionRoots: t.PartitionRoots,
		})
	}
	return canonicalDigest("safe-snapshot-data-v2", tables)
}

// AssembleStateRoot derives DataRoot and StateRoot for an arbitrary table-set
// view without sealing a manifest. This is the shared state-root assembly used
// by replay executors and source-side data-plane roles.
func AssembleStateRoot(schemaSnapshotID, schemaRoot, executorProfileID string, tables []TableManifest) (dataRoot, stateRoot string, err error) {
	m := SafeSnapshotManifest{
		SchemaSnapshotID:  schemaSnapshotID,
		SchemaRoot:        schemaRoot,
		ExecutorProfileID: executorProfileID,
		Tables:            tables,
	}
	dataRoot, err = m.ComputeDataRoot()
	if err != nil {
		return "", "", err
	}
	m.DataRoot = dataRoot
	stateRoot, err = m.ComputeStateRoot()
	if err != nil {
		return "", "", err
	}
	return dataRoot, stateRoot, nil
}

// ComputeStateRoot returns the commitment over schema + data for this snapshot.
func (m SafeSnapshotManifest) ComputeStateRoot() (string, error) {
	v := struct {
		SchemaSnapshotID  string `json:"schema_snapshot_id"`
		SchemaRoot        string `json:"schema_root"`
		ExecutorProfileID string `json:"executor_profile_id"`
		DataRoot          string `json:"data_root"`
	}{
		SchemaSnapshotID:  m.SchemaSnapshotID,
		SchemaRoot:        m.SchemaRoot,
		ExecutorProfileID: m.ExecutorProfileID,
		DataRoot:          m.DataRoot,
	}
	return canonicalDigest("safe-snapshot-state", v)
}

// ComputeManifestRoot returns the commitment over snapshot metadata and data
// manifest, excluding ManifestRoot and SnapshotID to avoid self-reference.
func (m SafeSnapshotManifest) ComputeManifestRoot() (string, error) {
	n := m.normalized()
	v := struct {
		ParentSnapshotID  string          `json:"parent_snapshot_id,omitempty"`
		SafeBlockSeq      uint64          `json:"safe_block_seq"`
		StateRoot         string          `json:"state_root"`
		SchemaSnapshotID  string          `json:"schema_snapshot_id"`
		SchemaRoot        string          `json:"schema_root"`
		ExecutorProfileID string          `json:"executor_profile_id"`
		DataRoot          string          `json:"data_root"`
		Tables            []TableManifest `json:"tables"`
	}{
		ParentSnapshotID:  n.ParentSnapshotID,
		SafeBlockSeq:      n.SafeBlockSeq,
		StateRoot:         n.StateRoot,
		SchemaSnapshotID:  n.SchemaSnapshotID,
		SchemaRoot:        n.SchemaRoot,
		ExecutorProfileID: n.ExecutorProfileID,
		DataRoot:          n.DataRoot,
		Tables:            n.Tables,
	}
	return canonicalDigest("safe-snapshot-manifest", v)
}
