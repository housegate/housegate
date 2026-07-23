package storageintegrity

import "testing"

func TestInsertPayloadEncodingAcceptsNativeOnly(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "implicit native client data",
			sql:  "INSERT INTO events",
			want: PayloadEncodingClickHouseNativeData,
		},
		{
			name: "explicit native",
			sql:  "INSERT INTO events FORMAT Native",
			want: PayloadEncodingClickHouseNativeData,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := InsertPayloadEncoding(tc.sql)
			if err != nil {
				t.Fatalf("InsertPayloadEncoding: %v", err)
			}
			if got != tc.want {
				t.Fatalf("encoding = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestInsertPayloadEncodingRejectsInlineAndUnsupportedFormats(t *testing.T) {
	for _, sql := range []string{
		"INSERT INTO events VALUES (1)",
		"INSERT INTO events SELECT * FROM source",
		"INSERT INTO events WITH 1 AS id SELECT id",
		"INSERT INTO events FORMAT CSV",
		"INSERT INTO events FORMAT CSVWithNames",
		"INSERT INTO events FORMAT JSONEachRow",
	} {
		t.Run(sql, func(t *testing.T) {
			if _, err := InsertPayloadEncoding(sql); err == nil {
				t.Fatal("InsertPayloadEncoding accepted unsupported INSERT payload form")
			}
		})
	}
}
