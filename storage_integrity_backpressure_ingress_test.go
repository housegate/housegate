package housegate

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/housegate/housegate/pkg/chproto"
	"github.com/housegate/housegate/pkg/lthash"
	siplugin "github.com/housegate/housegate/pkg/plugins/storageintegrity"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

type fakePartsPressure struct {
	refuse      map[string]error
	allowCalls  []string
	invalidated int
}

func (f *fakePartsPressure) Allow(table, partitionID string) error {
	f.allowCalls = append(f.allowCalls, table+"/"+partitionID)
	return f.refuse[table+"/"+partitionID]
}

func (f *fakePartsPressure) Invalidate() { f.invalidated++ }

func bpSchemaResolver() sicore.TableSchemaResolver {
	return sicore.TableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		if tableID != "net1.events" {
			return payloadexec.TableSchema{}, false
		}
		return payloadexec.TableSchema{
			TableID: "net1.events", PartitionBy: "region",
			Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: "region", Type: "String"}},
		}, true
	})
}

func bpAdmission() siplugin.Admission {
	sql := "INSERT INTO events FORMAT CSVWithNames"
	payload := []byte("id,region\n1,eu\n2,us\n3,eu\n")
	return siplugin.Admission{
		StatementID: "0xabc:1:n1", Kind: siplugin.KindInsert, TableID: "net1.events",
		SQL: sql, SQLHash: replay.DigestString(sql), Signer: "0xabc", UserJWS: "jws",
		Payload: siplugin.CapturedPayload{Bytes: payload, Length: uint64(len(payload)), Encoding: sicore.EncodingCSVWithNames, Revision: 54465, Complete: true},
	}
}

func newBackpressureIngress(t *testing.T, pressure *fakePartsPressure) (*StorageIntegrityIngress, *rootRecordingPayloadWriter, *rootRecordingSubmitter, *rootRecordingPreparer) {
	t.Helper()
	writer := &rootRecordingPayloadWriter{result: sicore.PayloadPutResult{PayloadRef: "payload://store/ref-1", State: sicore.PayloadStateAvailable}}
	submitter := &rootRecordingSubmitter{outcome: sicore.SubmitOutcome{Category: sicore.OutcomeAccepted}}
	preparer := &rootRecordingPreparer{source: "snode-A", claim: sicore.ClaimOutcome{Category: sicore.OutcomeAccepted, BoundSource: "snode-A"}}
	orch := sicore.NewOrchestrator(submitter, preparer, sicore.OrchestratorConfig{ExpectedSource: "snode-A"})
	ingress, err := NewStorageIntegrityIngressWithPayloadWriter(orch, nil, sicore.MaterializerCSV, writer)
	if err != nil {
		t.Fatalf("NewStorageIntegrityIngressWithPayloadWriter: %v", err)
	}
	ingress.WithPartsPressure(pressure, bpSchemaResolver())
	return ingress, writer, submitter, preparer
}

func TestIngress_BackpressureRefusesWithException252BeforePayloadPut(t *testing.T) {
	pressure := &fakePartsPressure{refuse: map[string]error{
		"net1__events/p_us": &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_us", Parts: 2400, Limit: 2400, Kind: "soft"},
	}}
	ingress, writer, submitter, preparer := newBackpressureIngress(t, pressure)

	before := testutil.ToFloat64(storageIntegrityBackpressureTotal.WithLabelValues("net1__events"))
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeTooManyParts {
		t.Fatalf("err = %v, want ClientError 252", err)
	}
	if !strings.HasPrefix(clientErr.Message, "storage_integrity: back-pressure") || !strings.Contains(clientErr.Message, "p_us") {
		t.Fatalf("client message = %q", clientErr.Message)
	}
	if !errors.Is(err, sicore.ErrBackpressure) {
		t.Fatal("must unwrap to ErrBackpressure")
	}
	if writer.calls != 0 || submitter.calls != 0 || preparer.prepareCalls != 0 {
		t.Fatalf("writer/submit/prepare calls = %d/%d/%d, want 0/0/0", writer.calls, submitter.calls, preparer.prepareCalls)
	}
	if got := strings.Join(pressure.allowCalls, ","); got != "net1__events/p_eu,net1__events/p_us" {
		t.Fatalf("allow calls = %q", got)
	}
	if got := testutil.ToFloat64(storageIntegrityBackpressureTotal.WithLabelValues("net1__events")); got != before+1 {
		t.Fatalf("backpressure counter = %v, want %v", got, before+1)
	}
}

func TestIngress_BackpressureAllowsAndInvalidatesAfterAck2(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, writer, _, _ := newBackpressureIngress(t, pressure)
	if err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission()); err != nil {
		t.Fatalf("ConsumeStorageIntegrityAdmission: %v", err)
	}
	if writer.calls != 1 {
		t.Fatalf("payload writer calls = %d want 1", writer.calls)
	}
	if got := strings.Join(pressure.allowCalls, ","); got != "net1__events/p_eu,net1__events/p_us" {
		t.Fatalf("allow calls = %q", got)
	}
	if pressure.invalidated != 1 {
		t.Fatalf("invalidate after ACK2 = %d want 1", pressure.invalidated)
	}
}

func TestIngress_MapsSNodeMirrorBackpressureToException252(t *testing.T) {
	pressure := &fakePartsPressure{}
	ingress, _, _, preparer := newBackpressureIngress(t, pressure)
	preparer.err = &sicore.BackpressureError{Database: "hg_unsafe", Table: "net1__events", Partition: "p_eu", Parts: 2950, Limit: 2950, Kind: "hard"}
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), bpAdmission())
	var clientErr *chproto.ClientError
	if !errors.As(err, &clientErr) || clientErr.Code != chproto.CodeTooManyParts || !strings.Contains(clientErr.Message, "hard limit 2950") {
		t.Fatalf("err = %v, want ClientError 252 from the SNode mirror", err)
	}
	if pressure.invalidated != 0 {
		t.Fatal("a refused admission must not invalidate")
	}
}

func TestIngress_UnknownSchemaOrFreezeViolationRefusedWithoutPut(t *testing.T) {
	ingress, writer, _, _ := newBackpressureIngress(t, &fakePartsPressure{})
	adm := bpAdmission()
	adm.TableID = "net1.unknown"
	err := ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	if err == nil || !strings.Contains(err.Error(), "no pinned schema") || writer.calls != 0 {
		t.Fatalf("err = %v writer calls = %d", err, writer.calls)
	}

	ingress.schemas = sicore.TableSchemaResolverFunc(func(tableID string) (payloadexec.TableSchema, bool) {
		return payloadexec.TableSchema{
			TableID: tableID, PartitionBy: "region",
			Columns: []lthash.Column{{Name: "region", Type: "UInt64"}},
		}, true
	})
	adm = bpAdmission()
	err = ingress.ConsumeStorageIntegrityAdmission(context.Background(), adm)
	if !errors.Is(err, sicore.ErrPartitionFreeze) || writer.calls != 0 {
		t.Fatalf("freeze err = %v writer calls = %d", err, writer.calls)
	}
}
