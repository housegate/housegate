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
