package registry

import (
	"encoding/json"
	"testing"
)

func TestStorageIntegrityInfoDecodesEnrolled(t *testing.T) {
	const base = `"enabled":true,"network_id":"devnet2","keeper_shard_id":1,"self_indexer_id":7,` +
		`"client_lanes_enabled":true,"ingress_max_payload_bytes":1024,"max_token_age_seconds":60,` +
		`"default_read_mode":"","registry_version":3,"server_unix_time":9`
	tests := []struct {
		name         string
		body         string
		wantEnrolled bool
		wantSIID     *uint64
	}{
		{"enrolled", `{` + base + `,"si_indexer_id":7,"enrolled":true}`, true, ptr(7)},
		{"not enrolled", `{` + base + `,"si_indexer_id":null,"enrolled":false}`, false, nil},
		{"older server omits enrolled", `{` + base + `,"si_indexer_id":7}`, false, ptr(7)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var info StorageIntegrityInfo
			if err := json.Unmarshal([]byte(tc.body), &info); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if info.Enrolled != tc.wantEnrolled {
				t.Errorf("Enrolled = %v, want %v", info.Enrolled, tc.wantEnrolled)
			}
			switch {
			case tc.wantSIID == nil && info.SIIndexerID != nil:
				t.Errorf("SIIndexerID = %d, want nil", *info.SIIndexerID)
			case tc.wantSIID != nil && (info.SIIndexerID == nil || *info.SIIndexerID != *tc.wantSIID):
				t.Errorf("SIIndexerID = %v, want %d", info.SIIndexerID, *tc.wantSIID)
			}
			if info.NetworkID != "devnet2" || info.RegistryVersion != 3 {
				t.Errorf("other fields not decoded: %+v", info)
			}
		})
	}
}

func ptr(v uint64) *uint64 { return &v }

// TestStorageIntegrityInfoDecodesReplicaHostsAndDatabases: the P3 fields of
// sentio_getStorageIntegrityInfo (spec 2026-10-14 §11, Plan S3-C4 K50)
// decode, and an older server's answer, which has neither, decodes them as
// empty.
func TestStorageIntegrityInfoDecodesReplicaHostsAndDatabases(t *testing.T) {
	var info StorageIntegrityInfo
	body := `{"enabled":true,"network_id":"devnet2","replica_databases":["db2"],"replica_hosts":{"db2":[0,7],"db3":[]}}`
	if err := json.Unmarshal([]byte(body), &info); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(info.ReplicaDatabases) != 1 || info.ReplicaDatabases[0] != "db2" {
		t.Fatalf("ReplicaDatabases = %v, want [db2]", info.ReplicaDatabases)
	}
	if got := info.ReplicaHosts["db2"]; len(got) != 2 || got[0] != 0 || got[1] != 7 {
		t.Fatalf("ReplicaHosts[db2] = %v, want [0 7]", got)
	}
	if got, ok := info.ReplicaHosts["db3"]; !ok || len(got) != 0 {
		t.Fatalf("ReplicaHosts[db3] = %v (present %v), want present and empty", got, ok)
	}
	var older StorageIntegrityInfo
	if err := json.Unmarshal([]byte(`{"enabled":true,"network_id":"devnet2"}`), &older); err != nil {
		t.Fatalf("decode older answer: %v", err)
	}
	if len(older.ReplicaDatabases) != 0 || len(older.ReplicaHosts) != 0 || older.ReplicaHosts["db2"] != nil {
		t.Fatalf("an older server's answer decoded replica fields %v %v", older.ReplicaDatabases, older.ReplicaHosts)
	}
}
