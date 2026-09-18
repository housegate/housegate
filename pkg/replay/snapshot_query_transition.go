package replay

import (
	"fmt"
	"reflect"
)

// ValidateExecutorProfileTransition checks a control-plane executor change.
// It copies the predecessor ledger, requires a sealed child whose only
// execution identity change is the executor profile, and binds the transition
// record to that pair. Authorization, Raft publication and current-use
// admission remain C1/D3; this function does not enable the query lane.
func ValidateExecutorProfileTransition(prev, next SafeSnapshotManifest, transition ExecutorProfileTransition) error {
	if err := prev.Validate(); err != nil {
		return fmt.Errorf("previous snapshot: %w", err)
	}
	if err := next.Validate(); err != nil {
		return fmt.Errorf("next snapshot: %w", err)
	}
	if prev.ExecutorProfileID == "" || next.ExecutorProfileID == "" {
		return fmt.Errorf("executor profile identities are required")
	}
	if prev.ExecutorProfileID == next.ExecutorProfileID {
		return fmt.Errorf("executor profile identity did not change")
	}
	if next.ParentSnapshotID != prev.SnapshotID {
		return fmt.Errorf("next snapshot parent is not the previous snapshot")
	}
	if next.SafeBlockSeq != prev.SafeBlockSeq+1 {
		return fmt.Errorf("next snapshot did not advance at the control-plane boundary")
	}
	if next.SchemaSnapshotID != prev.SchemaSnapshotID || next.SchemaRoot != prev.SchemaRoot {
		return fmt.Errorf("schema identity changed across the executor transition")
	}
	if next.DataRoot != prev.DataRoot {
		return fmt.Errorf("data root changed across the executor transition")
	}
	if err := sameTableLedger(prev.Tables, next.Tables); err != nil {
		return err
	}
	dataRoot, stateRoot, err := AssembleStateRoot(next.SchemaSnapshotID, next.SchemaRoot, next.ExecutorProfileID, next.Tables)
	if err != nil {
		return err
	}
	if next.DataRoot != dataRoot || next.StateRoot != stateRoot {
		return fmt.Errorf("next state root does not match the existing safe-snapshot-state formula")
	}
	if transition.NetworkID == "" {
		return fmt.Errorf("transition network_id is required")
	}
	if transition.OldExecutorProfileID != prev.ExecutorProfileID {
		return fmt.Errorf("transition old executor profile mismatch")
	}
	if transition.NewExecutorProfileID != next.ExecutorProfileID {
		return fmt.Errorf("transition new executor profile mismatch")
	}
	if transition.PrevSnapshotID != prev.SnapshotID || transition.PrevStateRoot != prev.StateRoot {
		return fmt.Errorf("transition predecessor identity mismatch")
	}
	if transition.SchemaSnapshotID != next.SchemaSnapshotID || transition.SchemaRoot != next.SchemaRoot || transition.DataRoot != next.DataRoot {
		return fmt.Errorf("transition schema or data identity mismatch")
	}
	if transition.NextSnapshotID != next.SnapshotID || transition.NextStateRoot != next.StateRoot || transition.NextManifestRoot != next.ManifestRoot {
		return fmt.Errorf("transition next manifest identity mismatch")
	}
	act := transition.Activation
	if !act.Enabled {
		return fmt.Errorf("transition activation is not enabled")
	}
	if act.NetworkID != transition.NetworkID || act.KeeperShardID != transition.KeeperShardID {
		return fmt.Errorf("activation network or shard mismatch")
	}
	if act.ActivationID == "" || act.QueryProfileID == "" {
		return fmt.Errorf("activation identity is incomplete")
	}
	if act.ExecutorProfileID != next.ExecutorProfileID {
		return fmt.Errorf("activation executor profile is not the new pair")
	}
	if act.ActivationBlockSeq != next.SafeBlockSeq {
		return fmt.Errorf("activation block is not the transition child")
	}
	return nil
}

func sameTableLedger(prev, next []TableManifest) error {
	if len(prev) != len(next) {
		return fmt.Errorf("table ledger membership changed across the executor transition")
	}
	for i := range prev {
		if prev[i].TableID != next[i].TableID || prev[i].SchemaHash != next[i].SchemaHash {
			return fmt.Errorf("table %q identity changed across the executor transition", prev[i].TableID)
		}
		if !reflect.DeepEqual(prev[i].PartitionRoots, next[i].PartitionRoots) {
			return fmt.Errorf("table %q partition roots changed across the executor transition", prev[i].TableID)
		}
		if len(prev[i].ActiveParts) != len(next[i].ActiveParts) {
			return fmt.Errorf("table %q active parts changed across the executor transition", prev[i].TableID)
		}
		for j := range prev[i].ActiveParts {
			if !samePartLedger(prev[i].ActiveParts[j], next[i].ActiveParts[j]) {
				return fmt.Errorf("table %q active parts changed across the executor transition", prev[i].TableID)
			}
		}
	}
	return nil
}

func samePartLedger(a, b PartManifestEntry) bool {
	return a.TableID == b.TableID && a.PartitionID == b.PartitionID && a.PartName == b.PartName &&
		a.PartPhysHash == b.PartPhysHash && a.PartRowLtHash == b.PartRowLtHash &&
		a.RowCount == b.RowCount && a.Bytes == b.Bytes
}
