package plugin

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/housegate/housegate/pkg/chproto"
)

func TestSynthesizedInsertPlan(t *testing.T) {
	plan := &SynthesizedInsertPlan{
		Blocks:        [][]proto.InputColumn{{{Name: "a", Data: &proto.ColInt64{1, 2}}}},
		SampleColumns: []chproto.SampleColumn{{Name: "a", Type: "Int64"}},
		Rows:          2,
		PayloadBytes:  4,
		Packets:       [][]byte{{0x02, 0x00}, {0x02, 0x01}},
	}
	if got, want := string(plan.Payload()), "\x02\x00\x02\x01"; got != want {
		t.Fatalf("Payload() = %q, want %q", got, want)
	}
	if got := uint64(len(plan.Payload())); got != plan.PayloadBytes {
		t.Fatalf("len(Payload()) = %d, want PayloadBytes %d", got, plan.PayloadBytes)
	}
	if got := (&SynthesizedInsertPlan{}).Payload(); len(got) != 0 {
		t.Fatalf("Payload() of an unencoded plan = %q, want empty", got)
	}
	var nilPlan *SynthesizedInsertPlan
	if got := nilPlan.Payload(); got != nil {
		t.Fatalf("nil plan Payload() = %q, want nil", got)
	}
	var qctx QueryContext
	if qctx.SynthesizedInsert != nil {
		t.Fatal("SynthesizedInsert must default nil so the ordinary path is unchanged")
	}
	if ValuesKeyMaterialized != "materialize.outcome" {
		t.Fatalf("ValuesKeyMaterialized = %q, want \"materialize.outcome\"", ValuesKeyMaterialized)
	}
}
