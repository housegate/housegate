// pkg/network/inmemory_writer_test.go
package network

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/registry"
)

const (
	writerIndexerSigner = "0x00000000000000000000000000000000000000aa"
	writerOwner         = "0x00000000000000000000000000000000000000b1"
	writerWrite         = "0x00000000000000000000000000000000000000b2"
	writerAdminOnly     = "0x00000000000000000000000000000000000000b3"
	writerReadOnly      = "0x00000000000000000000000000000000000000b4"
	writerNobody        = "0x00000000000000000000000000000000000000b5"
)

func writerFixture() *InMemoryNetworkState {
	s := NewInMemoryNetworkState()
	s.IndexerInfos[7] = IndexerInfo{IndexerId: 7, Signer: strings.ToUpper(writerIndexerSigner[:2]) + writerIndexerSigner[2:]}
	s.DatabaseInfos["db1"] = DatabaseInfo{DatabaseId: "db1", IndexerId: 7}
	s.DatabaseInfos["gone"] = DatabaseInfo{DatabaseId: "gone", IndexerId: 7, PendingDelete: true}
	s.DatabasePermissions[writerOwner] = DatabasePermissions{"db1": registry.DbAuthOwner}
	s.DatabasePermissions[writerWrite] = DatabasePermissions{"db1": registry.DbAuthWrite}
	s.DatabasePermissions[writerAdminOnly] = DatabasePermissions{"db1": registry.DbAuthAdmin | registry.DbAuthRead}
	s.DatabasePermissions[writerReadOnly] = DatabasePermissions{"db1": registry.DbAuthRead}
	// A wildcard Write grant: the contract does not union address(0).
	s.DatabasePermissions[WildcardAddress] = DatabasePermissions{"db1": registry.DbAuthWrite | registry.DbAuthOwner}
	return s
}

func TestInMemoryIsDatabaseWriter_ContractSemantics(t *testing.T) {
	s := writerFixture()
	for _, tc := range []struct {
		account string
		want    bool
	}{
		{writerIndexerSigner, true}, // indexer signer of the hosting indexer (stored mixed-case)
		{writerOwner, true},         // Owner bit
		{writerWrite, true},         // Write bit
		{writerAdminOnly, false},    // Admin alone is not a writer (Databases.sol code, not NatSpec)
		{writerReadOnly, false},     // Read only
		{writerNobody, false},       // only the wildcard grant applies to them
		{string(WildcardAddress), false},
		{"", false},
	} {
		got, err := s.IsDatabaseWriter("db1", tc.account)
		if err != nil || got != tc.want {
			t.Errorf("IsDatabaseWriter(db1, %q) = %v, %v; want %v, nil", tc.account, got, err, tc.want)
		}
	}
}

func TestInMemoryIsDatabaseWriter_UnknownAndPendingDeleteDatabases(t *testing.T) {
	s := writerFixture()
	for _, db := range []string{"nope", "gone"} {
		if ok, err := s.IsDatabaseWriter(db, writerOwner); err == nil || ok {
			t.Errorf("IsDatabaseWriter(%s) = %v, %v; want false and an error", db, ok, err)
		}
	}
}

func TestInMemoryIsDatabaseWriter_AccountCaseInsensitive(t *testing.T) {
	s := writerFixture()
	mixed := "0x" + strings.ToUpper(writerWrite[2:])
	if ok, err := s.IsDatabaseWriter("db1", mixed); err != nil || !ok {
		t.Fatalf("mixed-case writer = %v, %v; want true", ok, err)
	}
}

func TestInMemoryIsDatabaseWriter_IndexerWithoutSignerIsNotAWriter(t *testing.T) {
	s := NewInMemoryNetworkState()
	s.IndexerInfos[1] = IndexerInfo{IndexerId: 1}
	s.DatabaseInfos["db1"] = DatabaseInfo{DatabaseId: "db1", IndexerId: 1}
	if ok, err := s.IsDatabaseWriter("db1", ""); err != nil || ok {
		t.Fatalf("empty signer matched the empty account: %v, %v", ok, err)
	}
}
