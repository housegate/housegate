package snapshotquery

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/housegate/housegate/pkg/rewriter"
	pb "github.com/housegate/rewriter-proto/gen/pb"
)

func TestConstructorFreezesExactProfileAndNames(t *testing.T) {
	f := newExecutionFixture(t)
	calls := 0
	e, err := newExecutor(context.Background(), f.opts, func(_ context.Context, a rewriter.SnapshotQueryAnalyzer, q string, c []*pb.SnapshotQueryCatalogTable) error {
		calls++
		if a != f.analyzer || q != f.opts.QueryProfileID || len(c) != 2 || c[0].Database != "tenant" || c[0].Table != "copy" || c[1].Table != "events" {
			t.Fatal("probe did not receive exact pair/analyzer and private catalog")
		}
		c[0].Columns[0].Name = "mutated"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	f.registry.profile.Record.Settings[0].Value = "99"
	f.registry.profile.Record.ScalarOperators[0] = "mutated"
	f.registry.profile.Record.Limits.MaxOutputRows = 1
	f.registry.profile.QueryProfileID = "mutated"
	f.opts.LogicalNames.Tables[0].Table = "mutated"
	if e.profile.Record.Settings[0].Value != "1" || e.profile.Record.ScalarOperators[0] != "column" || e.profile.Record.Limits.MaxOutputRows != 100 || snapshotQueryProbeCatalog()[0].Columns[0].Name != "value" {
		t.Fatal("constructor retains mutable config")
	}
	p, err := e.Prepare(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	p.Close()
	if calls != 1 || f.registry.calls != 1 {
		t.Fatalf("configuration reselected: probe=%d lookup=%d", calls, f.registry.calls)
	}
}

func TestConstructorRejectsInvalidDependenciesAndProfiles(t *testing.T) {
	for _, name := range []string{"store", "analyzer", "policy", "uses", "registry", "appender", "network", "pair", "record", "limits", "version", "digest", "operators", "settings", "probe", "cancel"} {
		t.Run(name, func(t *testing.T) {
			f := newExecutionFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			probe := snapshotQueryProbe(fixtureProbe)
			switch name {
			case "store":
				f.opts.Snapshots = nil
			case "analyzer":
				var a *fixtureAnalyzer
				f.opts.Analyzer = a
			case "policy":
				f.opts.HistoricalPolicy = nil
			case "uses":
				f.opts.Uses = nil
			case "registry":
				f.opts.Profiles = nil
			case "appender":
				f.opts.Appender = nil
			case "network":
				f.opts.NetworkID = "wrong"
			case "pair":
				f.opts.QueryProfileID = "wrong"
			case "record":
				f.registry.profile.Record.Settings[0].Value = "changed"
			case "limits":
				f.registry.profile.Record.Limits.MaxSQLBytes = 0
			case "version":
				f.registry.profile.Record.Version = 0
			case "digest":
				f.registry.profile.Record.ClickHouseBuildDigest = "invalid"
			case "operators":
				f.registry.profile.Record.ScalarOperators = []string{"literal", "column"}
			case "settings":
				f.registry.profile.Record.Settings[0].Name = " "
			case "probe":
				probe = nil
			case "cancel":
				cancel()
			}
			e, err := newExecutor(ctx, f.opts, probe)
			if e != nil || err == nil || f.store.calls != 0 || f.uses.calls != 0 {
				t.Fatalf("accepted invalid config: %v %v", e, err)
			}
		})
	}
}

type unacknowledgedAnalyzer struct {
	fixtureAnalyzer
	positiveErr error
}

func (a *unacknowledgedAnalyzer) AnalyzeSnapshotQuery(_ context.Context, r *pb.AnalyzeSnapshotQueryRequest) (*pb.AnalyzeSnapshotQueryResponse, error) {
	if r.Sql == "INSERT INTO tenant.copy SELECT 7" && a.positiveErr == nil {
		return &pb.AnalyzeSnapshotQueryResponse{ContractVersion: 1, QueryProfileId: r.QueryProfileId, Code: pb.SnapshotQueryCode_SUCCESS, SqlAfterMaterialization: r.Sql, TargetTableId: r.Catalog[0].TableId, TargetColumns: []string{"value"}}, nil
	}
	if a.positiveErr != nil {
		return nil, a.positiveErr
	}
	return nil, &rewriter.SnapshotQueryError{Code: pb.SnapshotQueryCode_UNSUPPORTED, Message: "non-authoritative code-only fixture"}
}
func TestPublicConstructorDoesNotBypassStrictProbe(t *testing.T) {
	for _, cause := range []error{nil, errors.New("probe transport failure"), context.Canceled} {
		f := newExecutionFixture(t)
		f.opts.Analyzer = &unacknowledgedAnalyzer{positiveErr: cause}
		e, err := NewExecutor(context.Background(), f.opts)
		if e != nil || err == nil {
			t.Fatal("public constructor accepted unacknowledged fixture")
		}
		if cause != nil && !errors.Is(err, cause) {
			t.Fatal("lost probe cause", err)
		}
		if f.store.calls != 0 || f.uses.calls != 0 {
			t.Fatal("constructor performed artifact work")
		}
	}
}

func TestPrivateProbeFailureCannotReturnExecutor(t *testing.T) {
	f := newExecutionFixture(t)
	cause := errors.New("private probe fixture failure")
	e, err := newExecutor(context.Background(), f.opts, func(context.Context, rewriter.SnapshotQueryAnalyzer, string, []*pb.SnapshotQueryCatalogTable) error {
		return cause
	})
	if e != nil || !errors.Is(err, cause) || !reflect.DeepEqual(f.events, []string(nil)) {
		t.Fatal(e, err, f.events)
	}
}
