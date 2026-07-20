package storageintegrity

import "testing"

// TestSelectMaterializerKind pins that the replay materializer family is chosen
// by the pinned payload encoding — explicitly, not defaulted silently — and
// fails closed on an unrecognized encoding so a Native payload can never be
// mis-materialized as CSV (design section 3, "Native vs CSV must be explicit").
func TestSelectMaterializerKind(t *testing.T) {
	cases := []struct {
		name    string
		enc     string
		want    MaterializerKind
		wantErr bool
	}{
		{"native", PayloadEncodingClickHouseNativeData, MaterializerNative, false},
		{"csv explicit", EncodingCSVWithNames, MaterializerCSV, false},
		{"empty defaults csv", "", MaterializerCSV, false},
		{"unknown fails closed", "bogus-encoding", MaterializerUnspecified, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SelectMaterializerKind(tc.enc)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("encoding %q must fail closed, got kind %v", tc.enc, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("encoding %q: unexpected error %v", tc.enc, err)
			}
			if got != tc.want {
				t.Fatalf("encoding %q: got %v want %v", tc.enc, got, tc.want)
			}
		})
	}
}
