package sqlmeta

import "testing"

func TestStatementTypeDescribeString(t *testing.T) {
	if got := StatementTypeDescribe.String(); got != "DESCRIBE" {
		t.Fatalf("StatementTypeDescribe.String() = %q, want DESCRIBE", got)
	}
}
