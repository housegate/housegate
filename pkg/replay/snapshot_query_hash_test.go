package replay

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type snapshotQueryVector struct {
	Name          string          `json:"name"`
	Domain        string          `json:"domain"`
	Value         json.RawMessage `json:"value"`
	CanonicalJSON string          `json:"canonical_json"`
	Hash          string          `json:"hash"`
}
type snapshotQueryFixture struct {
	Name    string `json:"name"`
	Schemas []struct {
		TableID     string `json:"table_id"`
		PartitionBy string `json:"partition_by"`
		Columns     []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"columns"`
	} `json:"schemas"`
	Genesis             SafeSnapshotManifest             `json:"genesis"`
	Manifest            SafeSnapshotManifest             `json:"manifest"`
	Input               SnapshotQueryInput               `json:"input"`
	CanonicalJSON       string                           `json:"canonical_input_json"`
	InputRoot           string                           `json:"input_root"`
	ReadSetRoot         string                           `json:"read_set_root"`
	ReadSetJSON         string                           `json:"canonical_read_set_json"`
	Statement           SnapshotQueryStatement           `json:"statement"`
	StatementRoot       string                           `json:"statement_root"`
	StatementJSON       string                           `json:"canonical_statement_root_json"`
	Receipt             SnapshotQueryReceipt             `json:"receipt"`
	ReceiptHash         string                           `json:"receipt_hash"`
	ReceiptJSON         string                           `json:"canonical_receipt_json"`
	ErrorContains       string                           `json:"error_contains"`
	Contracts           []snapshotQueryVector            `json:"contracts"`
	ReservationStatuses []SnapshotQueryReservationStatus `json:"reservation_statuses"`
	Statuses            []SnapshotQueryStatus            `json:"statuses"`
}

func queryFixtures(t *testing.T) []snapshotQueryFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/snapshot_query_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var f []snapshotQueryFixture
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f
}
func queryJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func copyQueryInput(t *testing.T, in SnapshotQueryInput) SnapshotQueryInput {
	t.Helper()
	var n SnapshotQueryInput
	if err := json.Unmarshal(queryJSON(t, in), &n); err != nil {
		t.Fatal(err)
	}
	return n
}
func refreshReadRoot(t *testing.T, in *SnapshotQueryInput) {
	t.Helper()
	root, err := SnapshotQueryReadSetRoot(in.ReadSet)
	if err != nil {
		t.Fatal(err)
	}
	in.Binding.ReadSetRoot = root
}
func checkRoot(t *testing.T, want, got string, err error) {
	t.Helper()
	if err != nil || got != want {
		t.Fatalf("root=%s want=%s err=%v", got, want, err)
	}
}
func independentQueryDigest(domain, literal string) string {
	sum := sha256.Sum256([]byte("housegate-replay-mvp-v0:" + domain + "\x00" + literal))
	return "0x" + hex.EncodeToString(sum[:])
}

func TestSnapshotQueryRejectEmpty(t *testing.T) {
	if _, err := CanonicalSnapshotQueryInput(SnapshotQueryInput{}); err == nil {
		t.Fatal("empty input accepted")
	}
}
func TestSnapshotQueryGolden(t *testing.T) {
	for _, f := range queryFixtures(t) {
		t.Run(f.Name, func(t *testing.T) {
			before := queryJSON(t, f.Input)
			n, err := CanonicalSnapshotQueryInput(f.Input)
			if f.ErrorContains != "" {
				if err == nil || !strings.Contains(err.Error(), f.ErrorContains) {
					t.Fatalf("got %v want %s", err, f.ErrorContains)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := string(queryJSON(t, n)); got != f.CanonicalJSON {
				t.Fatalf("canonical input differs: %s", got)
			}
			if !bytes.Equal(before, queryJSON(t, f.Input)) {
				t.Fatal("canonicalizer mutated its caller")
			}
			got, err := SnapshotQueryInputRoot(f.Input)
			checkRoot(t, f.InputRoot, got, err)
			got, err = SnapshotQueryReadSetRoot(f.Input.ReadSet)
			checkRoot(t, f.ReadSetRoot, got, err)
			got, err = SnapshotQueryStatementRoot(f.Statement)
			checkRoot(t, f.StatementRoot, got, err)
			got, err = f.Receipt.Hash()
			checkRoot(t, f.ReceiptHash, got, err)
			if string(queryJSON(t, n.ReadSet)) != f.ReadSetJSON {
				t.Fatal("read-set bytes differ")
			}
			pr, err := canonicalSnapshotQueryReceipt(f.Receipt)
			if err != nil {
				t.Fatal(err)
			}
			if string(queryJSON(t, pr)) != f.ReceiptJSON {
				t.Fatal("receipt projection bytes differ")
			}
			stProjection := struct {
				StatementSeq uint64 `json:"statement_seq"`
				InputRoot    string `json:"input_root"`
				UserJWS      string `json:"user_jws"`
			}{f.Statement.StatementSeq, f.InputRoot, f.Statement.Envelope.UserJWS}
			if string(queryJSON(t, stProjection)) != f.StatementJSON {
				t.Fatal("statement projection bytes differ")
			}
			for _, v := range []struct{ domain, literal, root string }{{"snapshot-query-input-v1", f.CanonicalJSON, f.InputRoot}, {"snapshot-query-read-set-v1", f.ReadSetJSON, f.ReadSetRoot}, {"snapshot-query-statement-root-v1", f.StatementJSON, f.StatementRoot}, {"snapshot-query-receipt-v1", f.ReceiptJSON, f.ReceiptHash}} {
				if independentQueryDigest(v.domain, v.literal) != v.root {
					t.Fatalf("independent literal digest differs for %s", v.domain)
				}
			}
			if _, err := DecodeCanonicalSnapshotQueryInput([]byte(f.CanonicalJSON)); err != nil {
				t.Fatal(err)
			}
			if err := ValidateSnapshotQueryManifest(n, f.Manifest); err != nil {
				t.Fatal(err)
			}
			for _, m := range []SafeSnapshotManifest{f.Genesis, f.Manifest} {
				if err := m.Validate(); err != nil {
					t.Fatal(err)
				}
			}
			// Re-derive schema commitments independently from the full literal schema.
			schemaHashes := map[string]string{}
			for _, s := range f.Schemas {
				buf := "table-schema\x00" + n.Binding.NetworkID + "\x00" + s.TableID + "\x00" + s.PartitionBy + "\x00"
				for _, c := range s.Columns {
					buf += c.Name + "\x00" + c.Type + "\x00"
				}
				sum := sha256.Sum256([]byte(buf))
				schemaHashes[s.TableID] = "0x" + hex.EncodeToString(sum[:])
			}
			ids := make([]string, 0, len(schemaHashes))
			for id := range schemaHashes {
				ids = append(ids, id)
			}
			sort.Strings(ids)
			schemaBytes := "schema-root\x00"
			for _, id := range ids {
				schemaBytes += schemaHashes[id] + "\x00"
			}
			sum := sha256.Sum256([]byte(schemaBytes))
			if "0x"+hex.EncodeToString(sum[:]) != f.Manifest.SchemaRoot {
				t.Fatal("full schema root differs")
			}
			for _, table := range f.Manifest.Tables {
				if table.SchemaHash != schemaHashes[table.TableID] {
					t.Fatal("full table schema differs")
				}
			}
		})
	}
}

func TestSnapshotQueryEveryBindingField(t *testing.T) {
	f := queryFixtures(t)[3]
	for i := 0; i < reflect.TypeOf(f.Input.Binding).NumField(); i++ {
		field := reflect.TypeOf(f.Input.Binding).Field(i)
		t.Run(field.Name, func(t *testing.T) {
			n := copyQueryInput(t, f.Input)
			v := reflect.ValueOf(&n.Binding).Elem().Field(i)
			switch v.Kind() {
			case reflect.String:
				v.SetString(v.String() + "-changed")
			case reflect.Uint32, reflect.Uint64:
				v.SetUint(v.Uint() + 1)
			case reflect.Struct:
				v.FieldByName("SnapshotID").SetString("changed")
			}
			got, err := SnapshotQueryInputRoot(n)
			if err == nil && got == f.InputRoot {
				t.Fatal("binding field did not affect root")
			}
		})
	}
	for i := 0; i < reflect.TypeOf(f.Input.Binding.ReadSnapshot).NumField(); i++ {
		field := reflect.TypeOf(f.Input.Binding.ReadSnapshot).Field(i)
		t.Run("pin/"+field.Name, func(t *testing.T) {
			n := copyQueryInput(t, f.Input)
			v := reflect.ValueOf(&n.Binding.ReadSnapshot).Elem().Field(i)
			if v.Kind() == reflect.String {
				v.SetString(v.String() + "x")
			} else {
				v.SetUint(v.Uint() + 1)
			}
			if _, err := SnapshotQueryInputRoot(n); err == nil {
				t.Fatal("conflicting pin accepted")
			}
		})
	}
}

func TestSnapshotQueryStrictTransport(t *testing.T) {
	f := queryFixtures(t)[3]
	var obj map[string]any
	if err := json.Unmarshal([]byte(f.CanonicalJSON), &obj); err != nil {
		t.Fatal(err)
	}
	// Walk every nested object/array: omission, foreign fields and null arrays
	// must fail ordinary Unmarshal as well, before any validator loses presence.
	var walk func(any, string)
	walk = func(v any, path string) {
		switch n := v.(type) {
		case map[string]any:
			keys := make([]string, 0, len(n))
			for k := range n {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				old := n[k]
				delete(n, k)
				raw := queryJSON(t, obj)
				var in SnapshotQueryInput
				err := json.Unmarshal(raw, &in)
				if err == nil || !strings.Contains(err.Error(), "missing field") {
					t.Fatalf("%s.%s omission: %v", path, k, err)
				}
				n[k] = old
				walk(old, path+"."+k)
			}
			for _, foreign := range []string{"payload_ref", "payload_hash", "payload_length", "payload_format", "user_jws", "storage_refs"} {
				for _, zero := range []any{"", 0, nil, []any{}} {
					n[foreign] = zero
					var in SnapshotQueryInput
					err := json.Unmarshal(queryJSON(t, obj), &in)
					delete(n, foreign)
					if err == nil || !strings.Contains(err.Error(), "unknown field") {
						t.Fatalf("%s foreign %s=%v: %v", path, foreign, zero, err)
					}
				}
			}
		case []any:
			for i, x := range n {
				walk(x, fmt.Sprintf("%s[%d]", path, i))
			}
		}
	}
	walk(obj, "input")
	raw := []byte(f.CanonicalJSON)
	for _, test := range []struct {
		name string
		raw  []byte
	}{{"duplicate_top", bytes.Replace(raw, []byte(`{"binding":`), []byte(`{"sql":"","binding":`), 1)}, {"duplicate_nested", bytes.Replace(raw, []byte(`"network_id":"fixture-network"`), []byte(`"network_id":"fixture-network","network_id":"fixture-network"`), 1)}, {"null_reads", bytes.Replace(raw, []byte(`"tables":[`), []byte(`"tables":null,"extra":[`), 1)}, {"null_scalar", bytes.Replace(raw, []byte(`"safe_block_seq":12`), []byte(`"safe_block_seq":null`), 1)}} {
		t.Run(test.name, func(t *testing.T) {
			var in SnapshotQueryInput
			if err := json.Unmarshal(test.raw, &in); err == nil {
				t.Fatal("ambiguous transport accepted")
			}
		})
	}
	for _, raw := range [][]byte{append([]byte(" "), []byte(f.CanonicalJSON)...), queryJSON(t, obj), queryJSON(t, f.Input), bytes.Replace([]byte(f.CanonicalJSON), []byte(`"app"`), []byte(`"\u0061pp"`), 1)} {
		if _, err := DecodeCanonicalSnapshotQueryInput(raw); err == nil {
			t.Fatal("noncanonical signed transport accepted")
		}
	}
}

func TestSnapshotQueryPermutationsAndDuplicates(t *testing.T) {
	f := queryFixtures(t)[4]
	n := copyQueryInput(t, f.Input)
	for i, j := 0, len(n.ReadSet.Tables)-1; i < j; i, j = i+1, j-1 {
		n.ReadSet.Tables[i], n.ReadSet.Tables[j] = n.ReadSet.Tables[j], n.ReadSet.Tables[i]
	}
	for i := range n.ReadSet.Tables {
		table := &n.ReadSet.Tables[i]
		for a, b := 0, len(table.ActiveParts)-1; a < b; a, b = a+1, b-1 {
			table.ActiveParts[a], table.ActiveParts[b] = table.ActiveParts[b], table.ActiveParts[a]
		}
		for a, b := 0, len(table.PartitionRoots)-1; a < b; a, b = a+1, b-1 {
			table.PartitionRoots[a], table.PartitionRoots[b] = table.PartitionRoots[b], table.PartitionRoots[a]
		}
	}
	root, err := SnapshotQueryInputRoot(n)
	checkRoot(t, f.InputRoot, root, err)
	for _, mutate := range []func(*SnapshotReadTable){func(t *SnapshotReadTable) { t.PartitionRoots = append(t.PartitionRoots, t.PartitionRoots[0]) }, func(t *SnapshotReadTable) { t.PartitionRoots[0].TableID = "other" }, func(t *SnapshotReadTable) { t.ActiveParts[0].TableID = "other" }, func(t *SnapshotReadTable) { t.ActiveParts[0].PartitionID = "missing" }} {
		n := copyQueryInput(t, queryFixtures(t)[3].Input)
		mutate(&n.ReadSet.Tables[0])
		if _, err := SnapshotQueryReadSetRoot(n.ReadSet); err == nil {
			t.Fatal("conflicting structured identity accepted")
		}
	}
}

func TestSnapshotQueryManifestCompleteness(t *testing.T) {
	f := queryFixtures(t)[3]
	for _, mutate := range []func(*SnapshotReadTable){func(t *SnapshotReadTable) { t.ActiveParts = t.ActiveParts[1:] }, func(t *SnapshotReadTable) { t.ActiveParts = nil; t.PartitionRoots = nil }, func(t *SnapshotReadTable) { t.ActiveParts[0].Bytes++ }, func(t *SnapshotReadTable) { t.PartitionRoots[0].Root = "wrong" }, func(t *SnapshotReadTable) { t.SchemaHash = "wrong" }} {
		n := copyQueryInput(t, f.Input)
		mutate(&n.ReadSet.Tables[0])
		refreshReadRoot(t, &n)
		if err := ValidateSnapshotQueryInput(n); err != nil {
			t.Fatal(err)
		}
		if err := ValidateSnapshotQueryManifest(n, f.Manifest); err == nil {
			t.Fatal("incomplete descriptor accepted")
		}
	}
	for _, field := range []string{"SnapshotID", "SafeBlockSeq", "ManifestRoot", "StateRoot", "SchemaSnapshotID", "SchemaRoot"} {
		n := copyQueryInput(t, f.Input)
		v := reflect.ValueOf(&n.ReadSet.ReadSnapshot).Elem().FieldByName(field)
		if v.Kind() == reflect.String {
			v.SetString(v.String() + "x")
		} else {
			v.SetUint(v.Uint() + 1)
		}
		n.Binding.ReadSnapshot = n.ReadSet.ReadSnapshot
		n.Binding.SchemaSnapshotID = n.Binding.ReadSnapshot.SchemaSnapshotID
		n.Binding.SchemaRoot = n.Binding.ReadSnapshot.SchemaRoot
		refreshReadRoot(t, &n)
		if err := ValidateSnapshotQueryManifest(n, f.Manifest); err == nil {
			t.Fatalf("changed pin %s accepted", field)
		}
	}
	// Self-consistent malformed manifests still must not treat absence as genesis.
	for _, kind := range []string{"missing_target", "duplicate_table", "duplicate_part", "duplicate_partition"} {
		n := copyQueryInput(t, f.Input)
		m := f.Manifest
		m.Tables = append([]TableManifest{}, m.Tables...)
		switch kind {
		case "missing_target":
			m.Tables = m.Tables[1:]
		case "duplicate_table":
			m.Tables = append(m.Tables, m.Tables[0])
		case "duplicate_part":
			m.Tables[1].ActiveParts = append(append([]PartManifestEntry{}, m.Tables[1].ActiveParts...), m.Tables[1].ActiveParts[0])
		case "duplicate_partition":
			m.Tables[1].PartitionRoots = append(append([]PartitionCommitment{}, m.Tables[1].PartitionRoots...), m.Tables[1].PartitionRoots[0])
		}
		m.SnapshotID = ""
		var err error
		m, err = m.Seal()
		if err != nil {
			t.Fatal(err)
		}
		pin := n.Binding.ReadSnapshot
		pin.SnapshotID = m.SnapshotID
		pin.ManifestRoot = m.ManifestRoot
		pin.StateRoot = m.StateRoot
		n.Binding.ReadSnapshot = pin
		n.ReadSet.ReadSnapshot = pin
		refreshReadRoot(t, &n)
		if err := ValidateSnapshotQueryManifest(n, m); err == nil {
			t.Fatalf("%s manifest accepted", kind)
		}
	}
	// Hint changes are never copied into read descriptors. The selected legacy
	// manifest root still commits its own old representation, intentionally.
	hinted := f.Manifest
	hinted.Tables = append([]TableManifest{}, hinted.Tables...)
	hinted.Tables[1].ActiveParts = append([]PartManifestEntry{}, hinted.Tables[1].ActiveParts...)
	hinted.Tables[1].ActiveParts[0].StorageRefs = []string{"different"}
	if err := ValidateSnapshotQueryManifest(f.Input, hinted); err == nil {
		t.Fatal("unsealed changed legacy manifest accepted")
	}
}

func TestSnapshotQueryStatementAndReceiptBinding(t *testing.T) {
	f := queryFixtures(t)[3]
	st := f.Statement
	st.StatementSeq++
	got, err := SnapshotQueryStatementRoot(st)
	if err != nil || got == f.StatementRoot {
		t.Fatalf("sequence not bound: %v", err)
	}
	st = f.Statement
	st.Envelope.UserJWS += ".original-byte-change"
	got, err = SnapshotQueryStatementRoot(st)
	if err != nil || got == f.StatementRoot {
		t.Fatalf("original JWS not bound: %v", err)
	}
	st = f.Statement
	st.Envelope.InputRoot = "wrong"
	if _, err := SnapshotQueryStatementRoot(st); err == nil {
		t.Fatal("wrong input_root accepted")
	}
	receipt := f.Receipt
	receipt.AffectedParts = append([]PartManifestEntry{}, receipt.AffectedParts...)
	receipt.AffectedParts[0].StorageRefs = []string{"a", "b"}
	got, err = receipt.Hash()
	checkRoot(t, f.ReceiptHash, got, err)
	for i := 0; i < reflect.TypeOf(receipt).NumField(); i++ {
		n := f.Receipt
		n.PartitionCommitmentsAfter = append([]PartitionCommitment{}, n.PartitionCommitmentsAfter...)
		n.AffectedParts = append([]PartManifestEntry{}, n.AffectedParts...)
		field := reflect.TypeOf(n).Field(i)
		t.Run(field.Name, func(t *testing.T) {
			v := reflect.ValueOf(&n).Elem().Field(i)
			switch v.Kind() {
			case reflect.String:
				v.SetString(v.String() + "x")
			case reflect.Uint64:
				v.SetUint(v.Uint() + 1)
			case reflect.Bool:
				v.SetBool(!v.Bool())
			case reflect.Struct:
				v.FieldByName("SnapshotID").SetString("changed")
			case reflect.Slice:
				if field.Name == "AffectedParts" {
					n.AffectedParts[0].Bytes++
				} else {
					n.PartitionCommitmentsAfter[0].Root += "x"
				}
			}
			got, err := n.Hash()
			if err == nil && got == f.ReceiptHash {
				t.Fatal("receipt field not bound")
			}
		})
	}
	n := f.Receipt
	n.AffectedParts = append(append([]PartManifestEntry{}, n.AffectedParts...), n.AffectedParts[0])
	if _, err := n.Hash(); err == nil {
		t.Fatal("duplicate receipt part accepted")
	}
}

type queryHasher interface{ Hash() (string, error) }

func vectorValue(v snapshotQueryVector) queryHasher {
	switch v.Domain {
	case "snapshot-query-output-v1":
		return &SnapshotQueryOutputCommitment{}
	case "snapshot-query-profile-v1":
		return &QueryProfileRecord{}
	case "snapshot-query-artifact-set-v1":
		return &SnapshotArtifactSet{}
	case "snapshot-query-artifact-ready-v1":
		return &SnapshotArtifactReady{}
	case "snapshot-query-abort-v1":
		return &SnapshotQueryAbortRecord{}
	case "snapshot-query-claim-v1":
		return &SnapshotQueryClaim{}
	case "executor-profile-transition-v1":
		return &ExecutorProfileTransition{}
	case "snapshot-query-receipt-v1":
		return &SnapshotQueryReceipt{}
	}
	panic(v.Domain)
}
func TestSnapshotQueryContractVectors(t *testing.T) {
	for _, v := range queryFixtures(t)[0].Contracts {
		t.Run(v.Name, func(t *testing.T) {
			record := vectorValue(v)
			if err := json.Unmarshal(v.Value, record); err != nil {
				t.Fatal(err)
			}
			got, err := record.Hash()
			checkRoot(t, v.Hash, got, err)
			if string(queryJSON(t, record)) != v.CanonicalJSON {
				t.Fatal("contract bytes differ")
			}
			if independentQueryDigest(v.Domain, v.CanonicalJSON) != v.Hash {
				t.Fatal("independent contract digest differs")
			}
			// Every scalar/nested/collection field contributes, even zero values.
			for i := 0; i < reflect.ValueOf(record).Elem().NumField(); i++ {
				n := vectorValue(v)
				if err := json.Unmarshal(v.Value, n); err != nil {
					t.Fatal(err)
				}
				rv := reflect.ValueOf(n).Elem()
				field := rv.Field(i)
				name := rv.Type().Field(i).Name
				switch field.Kind() {
				case reflect.String:
					field.SetString(field.String() + "x")
				case reflect.Uint32, reflect.Uint64:
					field.SetUint(field.Uint() + 1)
				case reflect.Bool:
					field.SetBool(!field.Bool())
				case reflect.Struct:
					sf := field.Field(0)
					if sf.Kind() == reflect.String {
						sf.SetString(sf.String() + "x")
					} else {
						sf.SetUint(sf.Uint() + 1)
					}
				case reflect.Slice:
					if field.Len() == 0 {
						continue
					}
					if field.Index(0).Kind() == reflect.String {
						field.Index(0).SetString(field.Index(0).String() + "x")
					} else {
						sf := field.Index(0).Field(0)
						sf.SetString(sf.String() + "x")
					}
				}
				hash, err := n.Hash()
				if err == nil && hash == v.Hash {
					t.Fatalf("unbound field %s", name)
				}
			}
		})
	}
}
func TestSnapshotQueryContractNormalization(t *testing.T) {
	for _, v := range queryFixtures(t)[0].Contracts {
		if v.Domain == "snapshot-query-output-v1" {
			continue
		}
		record := vectorValue(v)
		if err := json.Unmarshal(v.Value, record); err != nil {
			t.Fatal(err)
		}
		rv := reflect.ValueOf(record).Elem()
		for i := 0; i < rv.NumField(); i++ {
			field := rv.Field(i)
			if field.Kind() != reflect.Slice || field.Type().Elem().Kind() == reflect.Uint8 {
				continue
			}
			for a, b := 0, field.Len()-1; a < b; a, b = a+1, b-1 {
				reflect.Swapper(field.Interface())(a, b)
			}
		}
		got, err := record.Hash()
		checkRoot(t, v.Hash, got, err)
		for i := 0; i < rv.NumField(); i++ {
			field := rv.Field(i)
			if field.Kind() != reflect.Slice || field.Len() == 0 || field.Type().Elem().Kind() == reflect.Uint8 {
				continue
			}
			old := reflect.MakeSlice(field.Type(), field.Len(), field.Len())
			reflect.Copy(old, field)
			field.Set(reflect.Append(field, field.Index(0)))
			if _, err := record.Hash(); err == nil {
				t.Fatalf("%s duplicate %s accepted", v.Name, rv.Type().Field(i).Name)
			}
			field.Set(old)
		}
	}
}
func TestSnapshotQueryEmptyArraysAndIdentity(t *testing.T) {
	f := queryFixtures(t)[0]
	in := f.Input
	in.ReadSet.Tables = nil
	got, err := SnapshotQueryInputRoot(in)
	checkRoot(t, f.InputRoot, got, err)
	if !bytes.Contains(queryJSON(t, in), []byte(`"tables":[]`)) {
		t.Fatal("nil reads encoded as null")
	}
	for _, raw := range [][]byte{bytes.Replace([]byte(f.CanonicalJSON), []byte(`"tables":[]`), []byte(`"tables":null`), 1), bytes.Replace([]byte(queryFixtures(t)[1].CanonicalJSON), []byte(`"active_parts":[]`), []byte(`"active_parts":null`), 1), bytes.Replace([]byte(queryFixtures(t)[1].CanonicalJSON), []byte(`"partition_roots":[]`), []byte(`"partition_roots":null`), 1)} {
		var n SnapshotQueryInput
		if err := json.Unmarshal(raw, &n); err == nil {
			t.Fatal("null collection accepted")
		}
	}
	for _, id := range []string{"0x1234:0:n", "0x1234:01:n", "0x1234:+1:n", "0x1234:18446744073709551616:n", "0x1234:1: n", "0x1234:1:", "0X1234:1:n", "0xabCD:1:n", "other:1:n", "0x1234:1:n:extra"} {
		n := f.Input
		n.Binding.StatementID = id
		if _, err := SnapshotQueryInputRoot(n); err == nil {
			t.Fatalf("bad identity accepted: %s", id)
		}
	}
}
func TestSnapshotQueryStatusRoundTrips(t *testing.T) {
	f := queryFixtures(t)[0]
	if len(f.ReservationStatuses) != 6 || len(f.Statuses) != 2 {
		t.Fatal("status fixtures incomplete")
	}
	for i, status := range f.ReservationStatuses {
		raw := queryJSON(t, status)
		var got SnapshotQueryReservationStatus
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(status, got) {
			t.Fatal("reservation status changed")
		}
		if i == 0 {
			if got.Version != 1 || got.Found || got.Reservation != nil || got.FencingGeneration != 0 {
				t.Fatal("negative lookup changed")
			}
		}
		if got.State == "draining" && got.Reservation != nil {
			t.Fatal("draining grant invented")
		}
		if got.State == "released" && len(got.TerminalProof) == 0 {
			t.Fatal("tombstone proof lost")
		}
	}
	// Canonical transport has a real presence distinction for an optional grant.
	absent := f.ReservationStatuses[0]
	present := absent
	present.Reservation = &SnapshotQueryReservation{}
	if bytes.Equal(queryJSON(t, absent), queryJSON(t, present)) {
		t.Fatal("absent and empty grant collapsed")
	}
	for _, status := range f.Statuses {
		var got SnapshotQueryStatus
		if err := json.Unmarshal(queryJSON(t, status), &got); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(status, got) {
			t.Fatal("status roundtrip differs")
		}
	}
}

func TestSnapshotQueryManifestExecutorProfile(t *testing.T) {
	f := queryFixtures(t)[3]
	in := copyQueryInput(t, f.Input)
	in.Binding.ExecutorProfileID = "different-executor-profile"
	if err := ValidateSnapshotQueryManifest(in, f.Manifest); err == nil {
		t.Fatal("executor profile differs from selected manifest")
	}
}

func TestSnapshotQueryOutputOrderAndCount(t *testing.T) {
	output := SnapshotQueryOutputCommitment{TargetTableID: "events", SchemaHash: "schema", RowCount: 3, RowHashes: []string{DigestBytes([]byte{0}), DigestBytes([]byte{1}), DigestBytes([]byte{1})}}
	root, err := output.Hash()
	if err != nil {
		t.Fatal(err)
	}
	reversed := output
	reversed.RowHashes = []string{output.RowHashes[1], output.RowHashes[0], output.RowHashes[2]}
	other, err := reversed.Hash()
	if err != nil || other == root {
		t.Fatalf("ordered output hashes were sorted: %v", err)
	}
	output.RowCount--
	if _, err := output.Hash(); err == nil {
		t.Fatal("row_count/row_hashes mismatch accepted")
	}
}

func TestSnapshotQueryOutputLiteralRows(t *testing.T) {
	// These are commitment-only synthetic byte strings, not SQL row execution.
	rows := map[string][][]byte{
		"empty_output":     {},
		"multiple_output":  {{0}, {1}},
		"reordered_output": {{1}, {0}},
		"duplicate_output": {{0}, {1}, {1}},
	}
	for _, v := range queryFixtures(t)[0].Contracts {
		rawRows, ok := rows[v.Name]
		if !ok {
			continue
		}
		var out SnapshotQueryOutputCommitment
		if err := json.Unmarshal(v.Value, &out); err != nil {
			t.Fatal(err)
		}
		if len(out.RowHashes) != len(rawRows) {
			t.Fatal("missing literal row")
		}
		for i, row := range rawRows {
			sum := sha256.Sum256(row)
			if "0x"+hex.EncodeToString(sum[:]) != out.RowHashes[i] {
				t.Fatal("literal output row digest differs")
			}
		}
	}
	// Empty outputs bind target and schema, not a shared opaque empty marker.
	for _, f := range queryFixtures(t) {
		if f.ErrorContains != "" {
			continue
		}
		empty := SnapshotQueryOutputCommitment{TargetTableID: f.Input.Binding.TargetTableID, SchemaHash: f.Input.Binding.SchemaHash}
		root, err := empty.Hash()
		checkRoot(t, f.Receipt.OutputRowsRoot, root, err)
	}
}
