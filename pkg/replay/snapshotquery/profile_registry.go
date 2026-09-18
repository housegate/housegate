package snapshotquery

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

// ProfileRegistry is trusted configuration of supported exact execution pairs.
// A record commits engine, settings, type/order and operator semantics; presence
// is not measurement, historical activation, or admission. The creator must wire
// matching measured analyzer/read-store engines; constructors freeze one record.
type ProfileRegistry interface {
	Lookup(executorID, queryID string) (Profile, bool)
}
type Profile struct {
	ExecutorProfileID string
	QueryProfileID    string
	Record            replay.QueryProfileRecord
}

func freezeProfile(registry ProfileRegistry, executorID, queryID string) (Profile, error) {
	p, ok := registry.Lookup(executorID, queryID)
	if !ok || executorID == "" || p.ExecutorProfileID != executorID || p.QueryProfileID != queryID || !canonicalDigest(queryID) {
		return Profile{}, fmt.Errorf("unsupported exact executor/query profile pair")
	}
	p.Record.Settings = append([]replay.ProfileSetting{}, p.Record.Settings...)
	p.Record.ScalarOperators = append([]string{}, p.Record.ScalarOperators...)
	r := p.Record
	if r.Version != 1 || strings.TrimSpace(r.Platform) == "" || r.Platform != strings.TrimSpace(r.Platform) || strings.TrimSpace(r.ColumnProfileID) == "" || r.ColumnProfileID != strings.TrimSpace(r.ColumnProfileID) || strings.TrimSpace(r.OutputOrderID) == "" || r.OutputOrderID != strings.TrimSpace(r.OutputOrderID) {
		return Profile{}, fmt.Errorf("incomplete query profile record")
	}
	for _, d := range []string{r.ClickHouseBuildDigest, r.NativeAnalyzerBuildDigest, r.GRPCAnalyzerBuildDigest, r.TZDataDigest} {
		if !canonicalDigest(d) {
			return Profile{}, fmt.Errorf("invalid profile build digest")
		}
	}
	for i, s := range r.Settings {
		if strings.TrimSpace(s.Name) == "" || s.Name != strings.TrimSpace(s.Name) || (i > 0 && r.Settings[i-1].Name >= s.Name) {
			return Profile{}, fmt.Errorf("profile settings must be canonical, sorted and unique")
		}
	}
	for i, s := range r.ScalarOperators {
		if strings.TrimSpace(s) == "" || s != strings.TrimSpace(s) || (i > 0 && r.ScalarOperators[i-1] >= s) {
			return Profile{}, fmt.Errorf("profile operators must be canonical, sorted and unique")
		}
	}
	if err := replay.ValidateQueryLimits(r.Limits); err != nil {
		return Profile{}, err
	}
	if r.Limits.MaxExecutionMS > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return Profile{}, fmt.Errorf("profile execution deadline overflow")
	}
	hash, err := r.Hash()
	if err != nil {
		return Profile{}, err
	}
	if hash != queryID {
		return Profile{}, fmt.Errorf("query profile record commitment mismatch")
	}
	return p, nil
}
