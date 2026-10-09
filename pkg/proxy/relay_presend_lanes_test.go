//go:build linux || darwin

package proxy

import (
	"slices"
	"testing"
)

// housegate#225 on a client lane (spec 2026-10-09 §6.5): a seq the real
// sistatement plugin reserved on a client lane is released to that lane when
// Relay terminates the signed INSERT before WriteQuery, and stays burned on
// that lane once the write began. The legacy counter is never touched.

func TestRelay_LanedSignedInsertPreSendTerminationReleasesToTheLane(t *testing.T) {
	sites := map[string]presendSite{
		"later strict hook refuses (close)": siteStrictHookClose,
		"later strict hook refuses (keep)":  siteStrictHookKeep,
		"no upstream after strict hook":     siteNoUpstream,
		"active query race":                 siteActiveQueryRace,
	}
	for _, lane := range []struct {
		name   string
		inline bool
	}{{"deferred", false}, {"synthesized", true}} {
		for name, site := range sites {
			t.Run(lane.name+"/"+name, func(t *testing.T) {
				f := newPresendFixtureWith(t, site, lane.inline, true)
				f.run(t, lane.inline)
				recycled, burned := f.metrics.snapshot()
				if recycled != 1 || len(burned) != 0 {
					t.Fatalf("recycled=%d burned=%v; want the pre-send seq released", recycled, burned)
				}
				if got := f.onlyLane(t); !slices.Equal(got.Free, []uint64{1}) || got.Next != 2 || got.Abandoned {
					t.Fatalf("lane %s = %+v; want seq 1 back on its free list", got.Lane, got)
				}
			})
		}
	}
}

func TestRelay_LanedSignedInsertFailedQueryWriteBurnsOnTheLane(t *testing.T) {
	for _, lane := range []struct {
		name   string
		inline bool
	}{{"deferred", false}, {"synthesized", true}} {
		t.Run(lane.name, func(t *testing.T) {
			f := newPresendFixtureWith(t, siteWriteQueryFailed, lane.inline, true)
			f.run(t, lane.inline)
			recycled, burned := f.metrics.snapshot()
			if recycled != 0 || burned["unknown_outcome"] != 1 {
				t.Fatalf("recycled=%d burned=%v; want the seq burned once WriteQuery began", recycled, burned)
			}
			if got := f.onlyLane(t); len(got.Free) != 0 || got.Next != 2 {
				t.Fatalf("lane %s = %+v; want seq 1 burned (not on the free list)", got.Lane, got)
			}
		})
	}
}
