package snapshotquery

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

// These structurally consistent records are NON-AUTHORITATIVE scaffolding.
// They prove no committed assignment, publication, retention, or C1 authority.
func historicalFixture(t *testing.T) (replay.SnapshotQueryJob, HistoricalDecisionRecords) {
	t.Helper()
	m, p, _, digest := profileFixture(t)
	reads := replay.SnapshotReadSet{ReadSnapshot: p}
	readRoot, err := replay.SnapshotQueryReadSetRoot(reads)
	if err != nil {
		t.Fatal(err)
	}
	sql := "INSERT INTO tenant.orders SELECT 1"
	b := replay.SnapshotQueryBinding{EnvelopeVersion: 3, InputKind: replay.SnapshotQueryInputKind, ClientAccount: "0xabc", StatementID: "0xabc:1:nonce", StatementKind: 2, NetworkID: p.NetworkID, KeeperShardID: p.KeeperShardID, SQLHash: replay.DigestString(sql), SettingsHash: replay.DigestString("settings"), TargetTableID: "orders", SchemaHash: m.Tables[0].SchemaHash, RowIDProfileID: "row-profile", ClientRevision: 54470, ReadSnapshot: p, ReadSetRoot: readRoot, SchemaSnapshotID: p.SchemaSnapshotID, SchemaRoot: p.SchemaRoot, LogicalDatabase: "tenant", QueryProfileID: replay.DigestString("query-profile"), ExecutorProfileID: m.ExecutorProfileID, ReservationID: "reservation-1", FencingGeneration: 1}
	in := replay.SnapshotQueryInput{Binding: b, SQL: sql, ReadSet: reads}
	root, err := replay.SnapshotQueryInputRoot(in)
	if err != nil {
		t.Fatal(err)
	}
	r := replay.SnapshotQueryReservation{ReservationID: b.ReservationID, FencingGeneration: b.FencingGeneration, ClientAccount: b.ClientAccount, StatementID: b.StatementID, ReadSnapshot: p, ExecutorProfileID: b.ExecutorProfileID, QueryProfileID: b.QueryProfileID, ActivationID: "activation-1"}
	j := replay.SnapshotQueryJob{BlockSeq: 13, PrevSafeSnapshotID: p.SnapshotID, PrevStateRoot: p.StateRoot, SchemaSnapshotID: p.SchemaSnapshotID, ExecutorProfileID: b.ExecutorProfileID, QueryProfileID: b.QueryProfileID, Reservation: r, Statement: replay.SnapshotQueryStatement{StatementSeq: 42, Envelope: replay.SnapshotQueryEnvelope{Input: in, InputRoot: root, UserJWS: "original-jws-fixture"}}}
	stRoot, err := replay.SnapshotQueryStatementRoot(j.Statement)
	if err != nil {
		t.Fatal(err)
	}
	set := replay.SnapshotArtifactSet{SchemaArtifactDigest: digest}
	setRoot, err := set.Hash()
	if err != nil {
		t.Fatal(err)
	}
	return j, HistoricalDecisionRecords{Reservation: r, Activation: replay.ActiveQueryPolicy{ActivationID: r.ActivationID, NetworkID: p.NetworkID, KeeperShardID: p.KeeperShardID, ActivationBlockSeq: 1, ExecutorProfileID: r.ExecutorProfileID, QueryProfileID: r.QueryProfileID, Enabled: true}, BlockSeq: j.BlockSeq, StatementRoot: stRoot, Ready: replay.SnapshotArtifactReady{SnapshotID: p.SnapshotID, ManifestRoot: p.ManifestRoot, SchemaRoot: p.SchemaRoot, ArtifactSetRoot: setRoot, PublisherID: "publisher-fixture", RetentionPolicyID: "retention-fixture"}, Artifacts: set}
}

func TestHistoricalDecisionExactJob(t *testing.T) {
	j, r := historicalFixture(t)
	d, err := NewHistoricalDecisionFromVerifiedRecords(j, r)
	if err != nil {
		t.Fatal(err)
	}
	if err = d.CheckJob(j); err != nil {
		t.Fatal(err)
	}
	if d.Reservation() != r.Reservation || d.Activation() != r.Activation || d.Ready() != r.Ready || d.SchemaArtifactDigest() != r.Artifacts.SchemaArtifactDigest {
		t.Fatal("records not retained")
	}
	r.Reservation.ActivationID = "mutated"
	r.Ready.PublisherID = "mutated"
	r.Artifacts.SchemaArtifactDigest = "mutated"
	if err = d.CheckJob(j); err != nil {
		t.Fatal("decision aliased records", err)
	}
	// Source claims are deliberately not historical authority inputs.
	j.SourceClaimRoot = "unverified"
	j.SourceClaim = &replay.SnapshotQueryClaim{SourceNode: "unverified"}
	if err = d.CheckJob(j); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*replay.SnapshotQueryJob){
		"block":       func(j *replay.SnapshotQueryJob) { j.BlockSeq++ },
		"sequence":    func(j *replay.SnapshotQueryJob) { j.Statement.StatementSeq++ },
		"jws":         func(j *replay.SnapshotQueryJob) { j.Statement.Envelope.UserJWS += "x" },
		"input":       func(j *replay.SnapshotQueryJob) { j.Statement.Envelope.Input.Binding.LogicalDatabase = "other" },
		"input_root":  func(j *replay.SnapshotQueryJob) { j.Statement.Envelope.InputRoot = replay.DigestString("wrong") },
		"activation":  func(j *replay.SnapshotQueryJob) { j.Reservation.ActivationID = "other" },
		"fence":       func(j *replay.SnapshotQueryJob) { j.Reservation.FencingGeneration++ },
		"reservation": func(j *replay.SnapshotQueryJob) { j.Reservation.ReservationID = "other" },
		"client":      func(j *replay.SnapshotQueryJob) { j.Reservation.ClientAccount = "0xdef" },
		"statement":   func(j *replay.SnapshotQueryJob) { j.Reservation.StatementID = "0xabc:2:other" },
		"pin":         func(j *replay.SnapshotQueryJob) { j.Reservation.ReadSnapshot.SafeBlockSeq++ },
		"previous":    func(j *replay.SnapshotQueryJob) { j.PrevSafeSnapshotID = "other" },
		"state":       func(j *replay.SnapshotQueryJob) { j.PrevStateRoot = replay.DigestString("other") },
		"schema":      func(j *replay.SnapshotQueryJob) { j.SchemaSnapshotID = "other" },
		"executor":    func(j *replay.SnapshotQueryJob) { j.ExecutorProfileID = "other" },
		"query":       func(j *replay.SnapshotQueryJob) { j.QueryProfileID = replay.DigestString("other") },
	} {
		t.Run(name, func(t *testing.T) {
			c := j
			mutate(&c)
			if d.CheckJob(c) == nil {
				t.Fatal("swapped job accepted")
			}
		})
	}
	if (HistoricalDecision{}).CheckJob(j) == nil {
		t.Fatal("zero decision accepted")
	}
}

func TestHistoricalDecisionRejectsInvalidRecords(t *testing.T) {
	j, r := historicalFixture(t)
	for name, mutate := range map[string]func(*HistoricalDecisionRecords){
		"zero":           func(r *HistoricalDecisionRecords) { *r = HistoricalDecisionRecords{} },
		"grant":          func(r *HistoricalDecisionRecords) { r.Reservation.FencingGeneration++ },
		"activation_id":  func(r *HistoricalDecisionRecords) { r.Activation.ActivationID = "wrong" },
		"disabled":       func(r *HistoricalDecisionRecords) { r.Activation.Enabled = false },
		"network":        func(r *HistoricalDecisionRecords) { r.Activation.NetworkID = "wrong" },
		"shard":          func(r *HistoricalDecisionRecords) { r.Activation.KeeperShardID++ },
		"pair":           func(r *HistoricalDecisionRecords) { r.Activation.QueryProfileID = replay.DigestString("wrong") },
		"assignment":     func(r *HistoricalDecisionRecords) { r.BlockSeq++ },
		"statement_root": func(r *HistoricalDecisionRecords) { r.StatementRoot = replay.DigestString("wrong") },
		"ready_pin":      func(r *HistoricalDecisionRecords) { r.Ready.ManifestRoot = replay.DigestString("wrong") },
		"publisher":      func(r *HistoricalDecisionRecords) { r.Ready.PublisherID = "" },
		"retention":      func(r *HistoricalDecisionRecords) { r.Ready.RetentionPolicyID = "" },
		"set_root":       func(r *HistoricalDecisionRecords) { r.Ready.ArtifactSetRoot = replay.DigestString("wrong") },
		"outer":          func(r *HistoricalDecisionRecords) { r.Artifacts.SchemaArtifactDigest = replay.DigestString("wrong") },
		"digest_encoding": func(r *HistoricalDecisionRecords) {
			r.Artifacts.SchemaArtifactDigest = "0x" + strings.Repeat("G", 64)
			r.Ready.ArtifactSetRoot, _ = r.Artifacts.Hash()
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := r
			mutate(&c)
			d, err := NewHistoricalDecisionFromVerifiedRecords(j, c)
			if err == nil || d != (HistoricalDecision{}) {
				t.Fatalf("invalid records accepted: %+v %v", d, err)
			}
		})
	}
}

func TestHistoricalDecisionRejectsCoherentRebinding(t *testing.T) {
	j, records := historicalFixture(t)
	d, err := NewHistoricalDecisionFromVerifiedRecords(j, records)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"reservation", "fence", "statement", "query", "sql", "settings", "read-root"} {
		t.Run(field, func(t *testing.T) {
			c := cloneJob(j)
			b := &c.Statement.Envelope.Input.Binding
			switch field {
			case "reservation":
				b.ReservationID = "other"
				c.Reservation.ReservationID = b.ReservationID
			case "fence":
				b.FencingGeneration++
				c.Reservation.FencingGeneration = b.FencingGeneration
			case "statement":
				b.StatementID = "0xabc:2:new"
				c.Reservation.StatementID = b.StatementID
			case "query":
				b.QueryProfileID = replay.DigestString("other-profile")
				c.QueryProfileID = b.QueryProfileID
				c.Reservation.QueryProfileID = b.QueryProfileID
			case "sql":
				c.Statement.Envelope.Input.SQL += " "
				b.SQLHash = replay.DigestString(c.Statement.Envelope.Input.SQL)
			case "settings":
				b.SettingsHash = replay.DigestString("other-settings")
			case "read-root":
				c.Statement.Envelope.Input.ReadSet.Tables = []replay.SnapshotReadTable{{Database: "tenant", Table: "orders", TableID: "orders", SchemaHash: b.SchemaHash}}
				b.ReadSetRoot, err = replay.SnapshotQueryReadSetRoot(c.Statement.Envelope.Input.ReadSet)
				if err != nil {
					t.Fatal(err)
				}
			}
			c.Statement.Envelope.InputRoot, err = replay.SnapshotQueryInputRoot(c.Statement.Envelope.Input)
			if err != nil {
				t.Fatal(err)
			}
			if err = validateJob(c); err != nil {
				t.Fatal("fixture is not internally coherent", err)
			}
			if err = d.CheckJob(c); err == nil {
				t.Fatal("independently committed historical binding was bypassed")
			}
		})
	}
}
