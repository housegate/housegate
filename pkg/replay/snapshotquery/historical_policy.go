package snapshotquery

import (
	"context"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/replay"
)

// HistoricalPolicy authenticates the original accepted assignment, activation,
// and exact committed published-ready association. It does not admit current
// artifact use. Production wiring must fix the trusted C1 authority and scope.
type HistoricalPolicy interface {
	VerifyReservation(context.Context, replay.SnapshotQueryJob) (HistoricalDecision, error)
}

// HistoricalDecision is immutable by value. Zero is invalid; it has no wire
// representation, bypass-token semantics, or current-use authority.
type HistoricalDecision struct {
	reservation          replay.SnapshotQueryReservation
	activation           replay.ActiveQueryPolicy
	blockSeq             uint64
	statementRoot        string
	ready                replay.SnapshotArtifactReady
	schemaArtifactDigest string
}

// HistoricalDecisionRecords is structural constructor input AFTER trusted C1
// authentication. Neither this type nor the constructor authenticates records.
type HistoricalDecisionRecords struct {
	Reservation   replay.SnapshotQueryReservation
	Activation    replay.ActiveQueryPolicy
	BlockSeq      uint64
	StatementRoot string
	Ready         replay.SnapshotArtifactReady
	Artifacts     replay.SnapshotArtifactSet
}

func (d HistoricalDecision) Reservation() replay.SnapshotQueryReservation { return d.reservation }
func (d HistoricalDecision) Activation() replay.ActiveQueryPolicy         { return d.activation }
func (d HistoricalDecision) Ready() replay.SnapshotArtifactReady          { return d.ready }
func (d HistoricalDecision) SchemaArtifactDigest() string                 { return d.schemaArtifactDigest }

// NewHistoricalDecisionFromVerifiedRecords only binds already authenticated
// records. Production callers must independently authenticate original history,
// assignment, complete artifact-set/manifest correspondence and publication.
// Fabricated consistent records are NOT proof. No caller slices are retained.
func NewHistoricalDecisionFromVerifiedRecords(job replay.SnapshotQueryJob, records HistoricalDecisionRecords) (HistoricalDecision, error) {
	d := HistoricalDecision{reservation: records.Reservation, activation: records.Activation, blockSeq: records.BlockSeq, statementRoot: records.StatementRoot, ready: records.Ready, schemaArtifactDigest: records.Artifacts.SchemaArtifactDigest}
	if err := d.CheckJob(job); err != nil {
		return HistoricalDecision{}, err
	}
	for _, p := range records.Artifacts.Parts {
		if !canonicalDigest(p.PartPhysHash) || !canonicalDigest(p.ObjectDigest) {
			return HistoricalDecision{}, fmt.Errorf("invalid artifact part digest")
		}
	}
	root, err := records.Artifacts.Hash()
	if err != nil {
		return HistoricalDecision{}, fmt.Errorf("artifact set: %w", err)
	}
	if root != records.Ready.ArtifactSetRoot {
		return HistoricalDecision{}, fmt.Errorf("committed artifact set root mismatch")
	}
	return d, nil
}

func (d HistoricalDecision) CheckJob(job replay.SnapshotQueryJob) error {
	if err := validateJob(job); err != nil {
		return err
	}
	r, a, p := d.reservation, d.activation, d.reservation.ReadSnapshot
	if r != job.Reservation || d.blockSeq != job.BlockSeq {
		return fmt.Errorf("historical reservation or assigned block mismatch")
	}
	if !a.Enabled || a.ActivationID == "" || a.ActivationID != r.ActivationID || a.NetworkID != p.NetworkID || a.KeeperShardID != p.KeeperShardID || a.ExecutorProfileID != r.ExecutorProfileID || a.QueryProfileID != r.QueryProfileID {
		return fmt.Errorf("historical activation mismatch")
	}
	// An activation's applicability is authenticated by C1, never inferred from
	// an activation-block inequality or substituted from today's active policy.
	root, err := replay.SnapshotQueryStatementRoot(job.Statement)
	if err != nil {
		return err
	}
	if !canonicalDigest(d.statementRoot) || root != d.statementRoot {
		return fmt.Errorf("committed statement root mismatch")
	}
	ready := d.ready
	if ready.SnapshotID != p.SnapshotID || ready.ManifestRoot != p.ManifestRoot || ready.SchemaRoot != p.SchemaRoot || strings.TrimSpace(ready.PublisherID) == "" || strings.TrimSpace(ready.RetentionPolicyID) == "" || !canonicalDigest(ready.ArtifactSetRoot) || !canonicalDigest(d.schemaArtifactDigest) {
		return fmt.Errorf("historical published readiness mismatch")
	}
	return nil
}

func canonicalDigest(s string) bool {
	if len(s) != 66 || !strings.HasPrefix(s, "0x") {
		return false
	}
	for _, c := range s[2:] {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validatePin(p replay.SnapshotPin) error {
	if strings.TrimSpace(p.NetworkID) == "" || strings.TrimSpace(p.SchemaSnapshotID) == "" || !canonicalDigest(p.SnapshotID) || !canonicalDigest(p.ManifestRoot) || !canonicalDigest(p.StateRoot) || !canonicalDigest(p.SchemaRoot) {
		return fmt.Errorf("incomplete or invalid snapshot pin")
	}
	return nil
}

func validateJob(j replay.SnapshotQueryJob) error {
	in := j.Statement.Envelope.Input
	if err := replay.ValidateSnapshotQueryInput(in); err != nil {
		return err
	}
	b, r, p := in.Binding, j.Reservation, in.Binding.ReadSnapshot
	if err := validatePin(p); err != nil {
		return err
	}
	if j.BlockSeq == 0 || j.BlockSeq <= p.SafeBlockSeq || j.PrevSafeSnapshotID != p.SnapshotID || j.PrevStateRoot != p.StateRoot || j.SchemaSnapshotID != p.SchemaSnapshotID || j.ExecutorProfileID != b.ExecutorProfileID || j.QueryProfileID != b.QueryProfileID {
		return fmt.Errorf("job predecessor, assignment or profile mismatch")
	}
	if r.ReadSnapshot != p || r.ClientAccount != b.ClientAccount || r.StatementID != b.StatementID || r.ReservationID != b.ReservationID || r.FencingGeneration != b.FencingGeneration || r.ExecutorProfileID != b.ExecutorProfileID || r.QueryProfileID != b.QueryProfileID || strings.TrimSpace(r.ActivationID) == "" {
		return fmt.Errorf("job reservation binding mismatch")
	}
	for _, s := range []string{b.SQLHash, b.SettingsHash, b.SchemaHash, b.ReadSetRoot, b.SchemaRoot, b.QueryProfileID, j.Statement.Envelope.InputRoot} {
		if !canonicalDigest(s) {
			return fmt.Errorf("invalid query digest encoding")
		}
	}
	for _, t := range in.ReadSet.Tables {
		if !canonicalDigest(t.SchemaHash) {
			return fmt.Errorf("invalid read schema digest")
		}
		for _, part := range t.ActiveParts {
			if !canonicalDigest(part.PartPhysHash) {
				return fmt.Errorf("invalid read part physical digest")
			}
		}
	}
	_, err := replay.SnapshotQueryStatementRoot(j.Statement)
	return err
}
