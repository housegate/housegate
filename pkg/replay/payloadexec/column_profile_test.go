package payloadexec

import (
	"errors"
	"strings"
	"testing"
)

// TestAdmittedColumnTypeVectorsAreWellFormed is the authority's own contract
// test. Every vector must resolve, must already be in canonical spelling, and
// must carry the three claims every consumer reads: the Go value type the
// decoders produce, the lthash kind tag its values encode under, and the type
// string a decoded ch-go column reports. A vector missing any of them would let
// a consumer derive nothing while still appearing admitted.
func TestAdmittedColumnTypeVectorsAreWellFormed(t *testing.T) {
	vectors := AdmittedColumnTypeVectors()
	if len(vectors) == 0 {
		t.Fatal("AdmittedColumnTypeVectors() is empty")
	}
	seen := map[string]struct{}{}
	for _, vector := range vectors {
		t.Run(vector, func(t *testing.T) {
			if _, dup := seen[vector]; dup {
				t.Fatalf("duplicate admitted vector %q", vector)
			}
			seen[vector] = struct{}{}

			profile, err := ResolveColumnProfile(vector)
			if err != nil {
				t.Fatalf("ResolveColumnProfile(%q) = %v, want nil", vector, err)
			}
			if profile.Canonical != vector {
				t.Errorf("profile.Canonical = %q, want the vector itself", profile.Canonical)
			}
			canonical, err := CanonicalColumnType(vector)
			if err != nil {
				t.Fatalf("CanonicalColumnType(%q) = %v, want nil", vector, err)
			}
			if canonical != vector {
				t.Errorf("CanonicalColumnType(%q) = %q, want the vector itself", vector, canonical)
			}
			if profile.GoType == nil {
				t.Error("profile.GoType is nil: no consumer can derive a value type")
			}
			if profile.KindTag == 0 {
				t.Error("profile.KindTag is 0: no consumer can derive an lthash kind")
			}
			if profile.NativeWireType == "" {
				t.Error("profile.NativeWireType is empty: the Native lane cannot compare block types")
			}
			if profile.Family == FamilyFixedString && profile.FixedStringWidth <= 0 {
				t.Errorf("FixedString vector has width %d, want a positive width", profile.FixedStringWidth)
			}
			if profile.Family != FamilyFixedString && profile.FixedStringWidth != 0 {
				t.Errorf("non-FixedString vector carries width %d, want 0", profile.FixedStringWidth)
			}
		})
	}
}

// TestEveryColumnFamilyHasAnAdmittedVector closes the loop the other way: a
// family declared but never reachable through a vector is a capability claim
// nothing tests, and the cross-component authority test would never see it.
func TestEveryColumnFamilyHasAnAdmittedVector(t *testing.T) {
	covered := map[ColumnFamily]string{}
	for _, vector := range AdmittedColumnTypeVectors() {
		profile, err := ResolveColumnProfile(vector)
		if err != nil {
			t.Fatalf("ResolveColumnProfile(%q) = %v", vector, err)
		}
		covered[profile.Family] = vector
	}
	declared := map[ColumnFamily]struct{}{}
	for _, family := range allColumnFamilies {
		declared[family] = struct{}{}
		if _, ok := covered[family]; !ok {
			t.Errorf("family %q has no admitted vector: add one, or remove the family", family)
		}
	}
	for family, vector := range covered {
		if _, ok := declared[family]; !ok {
			t.Errorf("vector %q resolves to family %q, which is not in allColumnFamilies", vector, family)
		}
	}
}

// TestResolveColumnProfileRejectionsUnwrapAndNameTheType keeps the rejection
// contract every caller depends on: one sentinel, and an operator message that
// names both the offending declaration and the admitted profile.
func TestResolveColumnProfileRejectionsUnwrapAndNameTheType(t *testing.T) {
	for _, typeName := range []string{"Nullable(String)", "IPv4", "FixedString(x)", ""} {
		_, err := ResolveColumnProfile(typeName)
		if !errors.Is(err, ErrUnsupportedColumnType) {
			t.Errorf("ResolveColumnProfile(%q) = %v, want ErrUnsupportedColumnType", typeName, err)
			continue
		}
		if typeName != "" && !strings.Contains(err.Error(), typeName) {
			t.Errorf("ResolveColumnProfile(%q) error %q does not name the offending type", typeName, err)
		}
		for _, vector := range AdmittedColumnTypeVectors() {
			if !strings.Contains(err.Error(), vector) {
				t.Errorf("ResolveColumnProfile(%q) error %q omits admitted vector %q", typeName, err, vector)
			}
		}
	}
}
