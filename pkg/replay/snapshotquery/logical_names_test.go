package snapshotquery

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
)

func TestLogicalNameValidation(t *testing.T) {
	for _, name := range []string{"pin", "digest", "empty", "duplicate-id", "duplicate-pair", "empty-id", "nul", "utf8", "blank"} {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			n := &f.opts.LogicalNames
			switch name {
			case "pin":
				n.Pin.SafeBlockSeq++
				n.Pin.StateRoot = ""
			case "digest":
				n.SchemaArtifactDigest = "invalid"
			case "empty":
				n.Tables = nil
			case "duplicate-id":
				n.Tables[1].TableID = n.Tables[0].TableID
			case "duplicate-pair":
				n.Tables[1].Database = n.Tables[0].Database
				n.Tables[1].Table = n.Tables[0].Table
			case "empty-id":
				n.Tables[0].TableID = ""
			case "nul":
				n.Tables[0].Table = "a\x00b"
			case "utf8":
				n.Tables[0].Database = string([]byte{0xff})
			case "blank":
				n.Tables[0].Table = "  "
			}
			if e, err := newExecutor(context.Background(), f.opts, fixtureProbe); e != nil || err == nil {
				t.Fatal("bad names accepted")
			}
		})
	}
	// Quoted identifier values remain exact: no trimming, splitting or case folding.
	f := newExecutionFixture(t)
	f.opts.LogicalNames.Tables[2].Database = " odd.db "
	f.opts.LogicalNames.Tables[2].Table = "a`b.C"
	e := f.executor(t)
	n := e.names["untouched-U"]
	if n.Database != " odd.db " || n.Table != "a`b.C" {
		t.Fatal(n)
	}
}

func TestPrepareRequiresCompleteScopedNames(t *testing.T) {
	for _, name := range []string{"missing-U", "missing-W", "extra", "pin", "outer", "read-name"} {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			switch name {
			case "missing-U":
				f.opts.LogicalNames.Tables = f.opts.LogicalNames.Tables[:2]
			case "missing-W":
				f.opts.LogicalNames.Tables = append(f.opts.LogicalNames.Tables[:1], f.opts.LogicalNames.Tables[2])
			case "extra":
				f.opts.LogicalNames.Tables = append(f.opts.LogicalNames.Tables, LogicalTableName{TableID: "extra", Database: "x", Table: "y"})
			case "pin":
				f.opts.LogicalNames.Pin.SafeBlockSeq++
			case "outer":
				f.opts.LogicalNames.SchemaArtifactDigest = replay.DigestString("other")
			case "read-name":
				f.opts.LogicalNames.Tables[0].Table = "wrong"
			}
			e := f.executor(t)
			p, err := e.Prepare(context.Background(), f.request())
			if p != nil || err == nil || f.analyzer.analyses != 0 {
				t.Fatal("wrong/missing name scope accepted", err)
			}
			if (name == "pin" || name == "outer") && f.store.calls != 0 {
				t.Fatal("scope mismatch opened snapshot")
			}
		})
	}
}
