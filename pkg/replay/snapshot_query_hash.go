package replay

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

func queryRequired(v any) error {
	r := reflect.ValueOf(v)
	t := r.Type()
	for i := 0; i < r.NumField(); i++ {
		f := r.Field(i)
		if f.Kind() == reflect.String && strings.TrimSpace(f.String()) == "" {
			return fmt.Errorf("%s is required", t.Field(i).Tag.Get("json"))
		}
	}
	return nil
}

func validateQueryIdentity(account, id string) error {
	parts := strings.Split(id, ":")
	if len(parts) != 3 || parts[0] != account || !strings.HasPrefix(account, "0x") || len(account) <= 2 {
		return fmt.Errorf("statement_id requires matching canonical client_account:seq:nonce")
	}
	for _, c := range account[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return fmt.Errorf("statement_id requires lowercase 0x client_account")
		}
	}
	seq, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil || seq == 0 || strconv.FormatUint(seq, 10) != parts[1] {
		return fmt.Errorf("statement_id requires canonical nonzero decimal sequence")
	}
	if parts[2] == "" || strings.TrimSpace(parts[2]) != parts[2] {
		return fmt.Errorf("statement_id requires nonempty canonical nonce")
	}
	return nil
}

func validateQueryPin(pin SnapshotPin) error { return queryRequired(pin) }

type queryPartitionKey struct{ table, partition string }
type queryPartKey struct{ table, partition, part string }

func partKey(p SnapshotReadPart) queryPartKey {
	return queryPartKey{p.TableID, p.PartitionID, p.PartName}
}
func lessPart(a, b queryPartKey) bool {
	if a.table != b.table {
		return a.table < b.table
	}
	if a.partition != b.partition {
		return a.partition < b.partition
	}
	return a.part < b.part
}

func canonicalQueryPartitions(parts []PartitionCommitment) ([]PartitionCommitment, error) {
	seen := map[queryPartitionKey]bool{}
	for _, p := range parts {
		if err := queryRequired(p); err != nil {
			return nil, err
		}
		k := queryPartitionKey{p.TableID, p.PartitionID}
		if seen[k] {
			return nil, fmt.Errorf("duplicate partition identity: %v", k)
		}
		seen[k] = true
	}
	n := append([]PartitionCommitment{}, parts...)
	sort.Slice(n, func(i, j int) bool {
		if n[i].TableID != n[j].TableID {
			return n[i].TableID < n[j].TableID
		}
		return n[i].PartitionID < n[j].PartitionID
	})
	return n, nil
}
func canonicalQueryParts(parts []SnapshotReadPart) ([]SnapshotReadPart, error) {
	seen := map[queryPartKey]bool{}
	for _, p := range parts {
		if err := queryRequired(p); err != nil {
			return nil, err
		}
		k := partKey(p)
		if seen[k] {
			return nil, fmt.Errorf("duplicate part identity: %v", k)
		}
		seen[k] = true
	}
	n := append([]SnapshotReadPart{}, parts...)
	sort.Slice(n, func(i, j int) bool { return lessPart(partKey(n[i]), partKey(n[j])) })
	return n, nil
}
func queryPartProjection(p PartManifestEntry) SnapshotReadPart {
	return SnapshotReadPart{p.TableID, p.PartitionID, p.PartName, p.PartPhysHash, p.PartRowLtHash, p.RowCount, p.Bytes}
}

func canonicalQueryTable(t SnapshotReadTable) (SnapshotReadTable, error) {
	if err := queryRequired(t); err != nil {
		return t, err
	}
	var err error
	t.PartitionRoots, err = canonicalQueryPartitions(t.PartitionRoots)
	if err != nil {
		return t, err
	}
	t.ActiveParts, err = canonicalQueryParts(t.ActiveParts)
	if err != nil {
		return t, err
	}
	partitions := map[string]bool{}
	for _, p := range t.PartitionRoots {
		if p.TableID != t.TableID {
			return t, fmt.Errorf("partition table_id mismatch")
		}
		partitions[p.PartitionID] = true
	}
	for _, p := range t.ActiveParts {
		if p.TableID != t.TableID {
			return t, fmt.Errorf("part table_id mismatch")
		}
		if !partitions[p.PartitionID] {
			return t, fmt.Errorf("part partition_id has no partition commitment")
		}
	}
	return t, nil
}
func canonicalQueryReadSet(in SnapshotReadSet) (SnapshotReadSet, error) {
	if err := validateQueryPin(in.ReadSnapshot); err != nil {
		return in, err
	}
	out := in
	out.Tables = make([]SnapshotReadTable, 0, len(in.Tables))
	ids := map[string]bool{}
	names := map[[2]string]bool{}
	for _, t := range in.Tables {
		if ids[t.TableID] || names[[2]string{t.Database, t.Table}] {
			return out, fmt.Errorf("duplicate or conflicting table identity")
		}
		ids[t.TableID] = true
		names[[2]string{t.Database, t.Table}] = true
		n, err := canonicalQueryTable(t)
		if err != nil {
			return out, err
		}
		out.Tables = append(out.Tables, n)
	}
	sort.Slice(out.Tables, func(i, j int) bool { return out.Tables[i].TableID < out.Tables[j].TableID })
	return out, nil
}

// ValidateSnapshotQueryInput checks structural bindings before sorting. It does
// not authenticate the signer, the published snapshot, or SQL analysis.
func ValidateSnapshotQueryInput(in SnapshotQueryInput) error {
	b := in.Binding
	if b.EnvelopeVersion != SnapshotQueryEnvelopeVersion || b.InputKind != SnapshotQueryInputKind || b.StatementKind != SnapshotQueryStatementKind {
		return fmt.Errorf("unsupported snapshot query envelope version, input kind or statement kind")
	}
	if err := queryRequired(b); err != nil {
		return err
	}
	if err := validateQueryIdentity(b.ClientAccount, b.StatementID); err != nil {
		return err
	}
	if b.FencingGeneration == 0 {
		return fmt.Errorf("fencing_generation is required")
	}
	if b.ClientRevision == 0 {
		return fmt.Errorf("client_revision is required")
	}
	if strings.TrimSpace(in.SQL) == "" || DigestString(in.SQL) != b.SQLHash {
		return fmt.Errorf("sql_hash mismatch")
	}
	if b.ReadSnapshot != in.ReadSet.ReadSnapshot {
		return fmt.Errorf("read_snapshot mismatch")
	}
	p := b.ReadSnapshot
	if b.NetworkID != p.NetworkID || b.KeeperShardID != p.KeeperShardID {
		return fmt.Errorf("network or keeper shard mismatch")
	}
	if b.SchemaSnapshotID != p.SchemaSnapshotID || b.SchemaRoot != p.SchemaRoot {
		return fmt.Errorf("schema snapshot or root mismatch")
	}
	root, err := SnapshotQueryReadSetRoot(in.ReadSet)
	if err != nil {
		return err
	}
	if b.ReadSetRoot != root {
		return fmt.Errorf("read_set_root mismatch")
	}
	for _, t := range in.ReadSet.Tables {
		if t.TableID == b.TargetTableID && t.SchemaHash != b.SchemaHash {
			return fmt.Errorf("target schema_hash mismatch")
		}
	}
	return nil
}

// CanonicalSnapshotQueryInput copies and sorts a well-formed unsigned input.
// Empty new-lane collections are always represented by non-nil empty slices.
func CanonicalSnapshotQueryInput(in SnapshotQueryInput) (SnapshotQueryInput, error) {
	if err := ValidateSnapshotQueryInput(in); err != nil {
		return SnapshotQueryInput{}, err
	}
	read, err := canonicalQueryReadSet(in.ReadSet)
	if err != nil {
		return SnapshotQueryInput{}, err
	}
	in.ReadSet = read
	return in, nil
}

// SnapshotQueryInputRoot binds the canonical input under the new input domain.
func SnapshotQueryInputRoot(in SnapshotQueryInput) (string, error) {
	n, err := CanonicalSnapshotQueryInput(in)
	if err != nil {
		return "", err
	}
	return CanonicalDigest("snapshot-query-input-v1", n)
}

// SnapshotQueryReadSetRoot binds the pin and complete hint-free read descriptors.
func SnapshotQueryReadSetRoot(in SnapshotReadSet) (string, error) {
	n, err := canonicalQueryReadSet(in)
	if err != nil {
		return "", err
	}
	return CanonicalDigest("snapshot-query-read-set-v1", n)
}

// SnapshotQueryStatementRoot additionally binds sequencing and original JWS bytes.
func SnapshotQueryStatementRoot(st SnapshotQueryStatement) (string, error) {
	root, err := SnapshotQueryInputRoot(st.Envelope.Input)
	if err != nil {
		return "", err
	}
	if root != st.Envelope.InputRoot {
		return "", fmt.Errorf("input_root mismatch")
	}
	if st.StatementSeq == 0 || st.Envelope.UserJWS == "" {
		return "", fmt.Errorf("statement_seq and original user_jws are required")
	}
	return CanonicalDigest("snapshot-query-statement-root-v1", struct {
		StatementSeq uint64 `json:"statement_seq"`
		InputRoot    string `json:"input_root"`
		UserJWS      string `json:"user_jws"`
	}{st.StatementSeq, root, st.Envelope.UserJWS})
}

// Hash excludes fetch hints through an explicit new-domain part projection.
func (r SnapshotQueryReceipt) Hash() (string, error) {
	n, err := canonicalSnapshotQueryReceipt(r)
	if err != nil {
		return "", err
	}
	return CanonicalDigest("snapshot-query-receipt-v1", n)
}
func canonicalSnapshotQueryReceipt(r SnapshotQueryReceipt) (SnapshotQueryReceipt, error) {
	if err := validateQueryPin(r.ReadSnapshot); err != nil {
		return r, err
	}
	if r.SchemaSnapshotID != r.ReadSnapshot.SchemaSnapshotID {
		return r, fmt.Errorf("receipt schema_snapshot_id mismatch")
	}
	var err error
	r.PartitionCommitmentsAfter, err = canonicalQueryPartitions(r.PartitionCommitmentsAfter)
	if err != nil {
		return r, err
	}
	parts := make([]SnapshotReadPart, 0, len(r.AffectedParts))
	for _, p := range r.AffectedParts {
		parts = append(parts, queryPartProjection(p))
	}
	parts, err = canonicalQueryParts(parts)
	if err != nil {
		return r, err
	}
	r.AffectedParts = make([]PartManifestEntry, 0, len(parts))
	for _, p := range parts {
		r.AffectedParts = append(r.AffectedParts, PartManifestEntry{TableID: p.TableID, PartitionID: p.PartitionID, PartName: p.PartName, PartPhysHash: p.PartPhysHash, PartRowLtHash: p.PartRowLtHash, RowCount: p.RowCount, Bytes: p.Bytes})
	}
	return r, nil
}
func (c SnapshotQueryClaim) Hash() (string, error) {
	n, err := canonicalSnapshotQueryClaim(c)
	if err != nil {
		return "", err
	}
	return CanonicalDigest("snapshot-query-claim-v1", n)
}
func canonicalSnapshotQueryClaim(c SnapshotQueryClaim) (SnapshotQueryClaim, error) {
	var err error
	c.PartitionDeltas, err = canonicalQueryPartitions(c.PartitionDeltas)
	if err != nil {
		return c, err
	}
	c.PartitionCommitmentsAfter, err = canonicalQueryPartitions(c.PartitionCommitmentsAfter)
	if err != nil {
		return c, err
	}
	c.CandidateParts, err = canonicalQueryParts(c.CandidateParts)
	return c, err
}
func (p QueryProfileRecord) Hash() (string, error) {
	n, err := canonicalQueryProfile(p)
	if err != nil {
		return "", err
	}
	return CanonicalDigest("snapshot-query-profile-v1", n)
}
func canonicalQueryProfile(p QueryProfileRecord) (QueryProfileRecord, error) {
	p.Settings = append([]ProfileSetting{}, p.Settings...)
	p.ScalarOperators = append([]string{}, p.ScalarOperators...)
	seen := map[string]bool{}
	for _, s := range p.Settings {
		if s.Name == "" || seen[s.Name] {
			return p, fmt.Errorf("empty or duplicate profile setting")
		}
		seen[s.Name] = true
	}
	seen = map[string]bool{}
	for _, s := range p.ScalarOperators {
		if s == "" || seen[s] {
			return p, fmt.Errorf("empty or duplicate scalar operator")
		}
		seen[s] = true
	}
	sort.Slice(p.Settings, func(i, j int) bool { return p.Settings[i].Name < p.Settings[j].Name })
	sort.Strings(p.ScalarOperators)
	return p, nil
}
func (a SnapshotArtifactSet) Hash() (string, error) {
	n, err := canonicalSnapshotArtifactSet(a)
	if err != nil {
		return "", err
	}
	return CanonicalDigest("snapshot-query-artifact-set-v1", n)
}
func canonicalSnapshotArtifactSet(a SnapshotArtifactSet) (SnapshotArtifactSet, error) {
	if a.SchemaArtifactDigest == "" {
		return a, fmt.Errorf("schema_artifact_digest is required")
	}
	a.Parts = append([]SnapshotArtifactEntry{}, a.Parts...)
	seen := map[queryPartKey]bool{}
	for _, p := range a.Parts {
		if err := queryRequired(p); err != nil {
			return a, err
		}
		k := queryPartKey{p.TableID, p.PartitionID, p.PartName}
		if seen[k] {
			return a, fmt.Errorf("duplicate artifact identity")
		}
		seen[k] = true
	}
	sort.Slice(a.Parts, func(i, j int) bool {
		p, q := a.Parts[i], a.Parts[j]
		return lessPart(queryPartKey{p.TableID, p.PartitionID, p.PartName}, queryPartKey{q.TableID, q.PartitionID, q.PartName})
	})
	return a, nil
}
func (r SnapshotArtifactReady) Hash() (string, error) {
	return CanonicalDigest("snapshot-query-artifact-ready-v1", r)
}
func (r SnapshotQueryAbortRecord) Hash() (string, error) {
	return CanonicalDigest("snapshot-query-abort-v1", r)
}

// Hash commits the transition only; C1 owns transition authorization/validation.
func (r ExecutorProfileTransition) Hash() (string, error) {
	return CanonicalDigest("executor-profile-transition-v1", r)
}

// New-lane JSON retains explicit empty collections even before signing. These
// methods do not sort or hide malformed/duplicate input. Legacy types are untouched.
func (v SnapshotReadTable) MarshalJSON() ([]byte, error) {
	type plain SnapshotReadTable
	if v.PartitionRoots == nil {
		v.PartitionRoots = []PartitionCommitment{}
	}
	if v.ActiveParts == nil {
		v.ActiveParts = []SnapshotReadPart{}
	}
	return json.Marshal(plain(v))
}
func (v SnapshotReadSet) MarshalJSON() ([]byte, error) {
	type plain SnapshotReadSet
	if v.Tables == nil {
		v.Tables = []SnapshotReadTable{}
	}
	return json.Marshal(plain(v))
}
func (v SnapshotQueryReceipt) MarshalJSON() ([]byte, error) {
	type plain SnapshotQueryReceipt
	if v.PartitionCommitmentsAfter == nil {
		v.PartitionCommitmentsAfter = []PartitionCommitment{}
	}
	if v.AffectedParts == nil {
		v.AffectedParts = []PartManifestEntry{}
	}
	return json.Marshal(plain(v))
}
func (v SnapshotQueryClaim) MarshalJSON() ([]byte, error) {
	type plain SnapshotQueryClaim
	if v.PartitionDeltas == nil {
		v.PartitionDeltas = []PartitionCommitment{}
	}
	if v.PartitionCommitmentsAfter == nil {
		v.PartitionCommitmentsAfter = []PartitionCommitment{}
	}
	if v.CandidateParts == nil {
		v.CandidateParts = []SnapshotReadPart{}
	}
	return json.Marshal(plain(v))
}
func (v QueryProfileRecord) MarshalJSON() ([]byte, error) {
	type plain QueryProfileRecord
	if v.Settings == nil {
		v.Settings = []ProfileSetting{}
	}
	if v.ScalarOperators == nil {
		v.ScalarOperators = []string{}
	}
	return json.Marshal(plain(v))
}
func (v SnapshotArtifactSet) MarshalJSON() ([]byte, error) {
	type plain SnapshotArtifactSet
	if v.Parts == nil {
		v.Parts = []SnapshotArtifactEntry{}
	}
	return json.Marshal(plain(v))
}

// Hash commits the supplied row order, preserving duplicates. The producer owns
// canonical row encoding and sorting by full row bytes, never by their digests.
func (o SnapshotQueryOutputCommitment) Hash() (string, error) {
	if err := queryRequired(o); err != nil {
		return "", err
	}
	if o.RowCount != uint64(len(o.RowHashes)) {
		return "", fmt.Errorf("row_count does not match row_hashes length")
	}
	for _, h := range o.RowHashes {
		if h == "" {
			return "", fmt.Errorf("row hash is required")
		}
	}
	return CanonicalDigest("snapshot-query-output-v1", o)
}
func (o SnapshotQueryOutputCommitment) MarshalJSON() ([]byte, error) {
	type plain SnapshotQueryOutputCommitment
	if o.RowHashes == nil {
		o.RowHashes = []string{}
	}
	return json.Marshal(plain(o))
}
