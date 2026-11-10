package housegate

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// histogramSamples is the sample count of the series of the histogram
// family name whose label values, in label-name order, are labels.
func histogramSamples(t *testing.T, name string, labels ...string) uint64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
	metrics:
		for _, metric := range family.GetMetric() {
			pairs := metric.GetLabel()
			if len(pairs) != len(labels) {
				continue
			}
			for i, pair := range pairs {
				if pair.GetValue() != labels[i] {
					continue metrics
				}
			}
			return metric.GetHistogram().GetSampleCount()
		}
	}
	t.Fatalf("no %s series %v", name, labels)
	return 0
}

// TestStorageIntegrityIngress_TimesTheIntake: an ACK2 admission is timed end
// to end as ack2, through the payload put, the orchestrator call and the
// orchestrator's own stages; a refusal before the put is timed as error only.
func TestStorageIntegrityIngress_TimesTheIntake(t *testing.T) {
	adm := siplugin.Admission{
		StatementID:     "0xabc:1:n1",
		Kind:            siplugin.KindInsert,
		TableID:         "net1.events",
		SQL:             "INSERT INTO events FORMAT Native",
		SQLHash:         replay.DigestString("INSERT INTO events FORMAT Native"),
		Signer:          "0xabc",
		UserJWS:         "jws",
		EnvelopeVersion: sicore.EnvelopeVersionV2,
		NetworkID:       "testnet-v2",
		SettingsHash:    sicore.EmptySettingsHash,
		SchemaHash:      "0x11",
		RowIDProfileID:  payloadexec.RowIDProfileID,
		Payload: siplugin.CapturedPayload{
			Bytes:    []byte("native-block-bytes"),
			Length:   uint64(len("native-block-bytes")),
			Encoding: sicore.PayloadEncodingClickHouseNativeData,
			Revision: 54465,
			Complete: true,
		},
	}
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	preparer := &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A", StageObserver: intakeStageMetrics{}})
	ing, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerNative, writer)
	if err != nil {
		t.Fatal(err)
	}

	type series struct {
		family string
		labels []string // in label-name order: outcome, stage
	}
	const intake, stages = "storage_integrity_intake_seconds", "storage_integrity_intake_stage_seconds"
	watched := map[string]series{
		"ack2":           {intake, []string{"ack2"}},
		"error":          {intake, []string{"error"}},
		"payload_put":    {stages, []string{"ok", intakeStagePayloadPut}},
		"orchestrate":    {stages, []string{"ok", intakeStageOrchestrate}},
		"prepare":        {stages, []string{"ok", sicore.IntakeStagePrepare}},
		"submit":         {stages, []string{"ok", sicore.IntakeStageSubmit}},
		"register_claim": {stages, []string{"ok", sicore.IntakeStageRegisterClaim}},
	}
	snapshot := func() map[string]uint64 {
		out := map[string]uint64{}
		for name, s := range watched {
			out[name] = histogramSamples(t, s.family, s.labels...)
		}
		return out
	}
	delta := func(before map[string]uint64) map[string]uint64 {
		after := snapshot()
		out := map[string]uint64{}
		for name := range watched {
			out[name] = after[name] - before[name]
		}
		return out
	}

	before := snapshot()
	if err := ing.ConsumeStorageIntegrityAdmission(context.Background(), adm); err != nil {
		t.Fatalf("ConsumeStorageIntegrityAdmission: %v", err)
	}
	got := delta(before)
	for _, name := range []string{"ack2", "payload_put", "orchestrate", "prepare", "submit", "register_claim"} {
		if got[name] != 1 {
			t.Fatalf("%s samples = %d, want 1 (all: %v)", name, got[name], got)
		}
	}
	if got["error"] != 0 {
		t.Fatalf("error samples = %d, want 0", got["error"])
	}

	before = snapshot()
	if err := ing.ConsumeStorageIntegrityAdmission(context.Background(), storageIntegrityAdmissionForEncoding(sicore.EncodingCSVWithNames, 54465)); err == nil {
		t.Fatal("a CSV admission on a Native runtime was accepted")
	}
	got = delta(before)
	if got["error"] != 1 || got["ack2"] != 0 || got["payload_put"] != 0 || got["orchestrate"] != 0 {
		t.Fatalf("refusal before the put: %v", got)
	}
}
