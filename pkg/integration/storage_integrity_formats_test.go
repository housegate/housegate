package integration

import (
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/config"
	"github.com/housegate/housegate/pkg/integration/testenv"
	sicore "github.com/housegate/housegate/pkg/storageintegrity"
)

// signableInsertFormats spells ONE logical row -- id=1, region='eu' -- in every
// input format the signed lane admits. The column-less spellings rely on the
// order the sample block declares (id, region), which is the same thing a real
// client relies on.
//
// Native is absent by construction: its input is the binary encoding this test
// is measuring, so writing it here would assert the expectation against itself.
// What pins the family to Native is the Encoding assertion below, which the
// ingress records from the captured wire bytes.
var signableInsertFormats = []struct {
	format string
	stdin  string
}{
	{"CSVWithNames", "id,region\n1,eu\n"},
	{"CSV", "1,eu\n"},
	{"Values", "(1,'eu')\n"},
	{"JSONEachRow", "{\"id\":1,\"region\":\"eu\"}\n"},
	{"TSVWithNames", "id\tregion\n1\teu\n"},
	{"TabSeparatedWithNames", "id\tregion\n1\teu\n"},
	{"TSV", "1\teu\n"},
	{"TabSeparated", "1\teu\n"},
}

// TestCLI_SignableInsertFormatsShareOneWirePayload measures the claim
// clientParsedInsertFormats rests on, using the official client rather than a
// Go driver: the ClickHouse native protocol carries INSERT rows only as Native
// ClientData, and FORMAT selects client-side PARSING, so the same logical rows
// reach the wire as byte-identical blocks no matter how they were written.
//
// That byte identity is the whole licence for admitting these formats into a
// signed lane. payload_hash commits to these bytes, replay decodes them through
// the one Native decoder, and sql_hash separately covers the differing SQL
// text. A format whose client did NOT convert would put foreign bytes under a
// signature that claims Native -- so this test is what must be extended, with a
// real measurement, before any entry is added to that allowlist.
func TestCLI_SignableInsertFormatsShareOneWirePayload(t *testing.T) {
	bin := testenv.ClickHouseCLI(t)
	const networkID = "itest-net"
	// The SQL is fully qualified and ClientHello.Database stays empty, because
	// the 25.8 client copies --database into Query settings and that unsigned
	// setting is correctly refused. One physical context keeps the rewriter mock
	// classifying those fully-qualified INSERTs.
	agentProxy, consumer := startSIAgentPair(t, networkID,
		testenv.WithConfigMutator(func(cfg *config.Config) {
			cfg.Rewriter.PhysicalDatabase = chEnv.Database
		}),
	)
	insert := "INSERT INTO " + chEnv.Database + ".si_events FORMAT "

	for _, tc := range signableInsertFormats {
		out, err := testenv.RunCLIStdin(t, bin, agentProxy.Addr, "", insert+tc.format, tc.stdin)
		if err != nil {
			t.Fatalf("FORMAT %s was refused by the signed lane: %v\nout: %s", tc.format, err, out)
		}
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		consumer.mu.Lock()
		n := len(consumer.seen)
		consumer.mu.Unlock()
		if n >= len(signableInsertFormats) || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	if len(consumer.seen) != len(signableInsertFormats) {
		t.Fatalf("ingress admitted %d statements, want %d (one per format)", len(consumer.seen), len(signableInsertFormats))
	}

	base := consumer.seen[0]
	for i, adm := range consumer.seen {
		format := signableInsertFormats[i].format
		if adm.Payload.Encoding != sicore.PayloadEncodingClickHouseNativeData {
			t.Errorf("FORMAT %s stored encoding %q, want %q", format, adm.Payload.Encoding, sicore.PayloadEncodingClickHouseNativeData)
		}
		if adm.Payload.SHA256 != base.Payload.SHA256 || adm.Payload.Length != base.Payload.Length {
			t.Errorf("FORMAT %s put different bytes on the wire than FORMAT %s: %s (%d bytes) vs %s (%d bytes)",
				format, signableInsertFormats[0].format,
				adm.Payload.SHA256, adm.Payload.Length, base.Payload.SHA256, base.Payload.Length)
		}
		// The SQL differs by construction, and is what sql_hash covers.
		if adm.SQL == base.SQL && i != 0 {
			t.Errorf("FORMAT %s signed the same SQL as the baseline; the fixture is not exercising distinct spellings", format)
		}
	}
}
