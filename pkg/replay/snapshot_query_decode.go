package replay

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

// UnmarshalJSON preserves signed-input field presence instead of silently
// discarding foreign fields. Ordering is checked by the signed-byte decoder.
func (in *SnapshotQueryInput) UnmarshalJSON(raw []byte) error {
	type plain SnapshotQueryInput
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := queryJSONShape(d, reflect.TypeOf(plain{}), "input"); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("input: trailing JSON")
	}
	var decoded plain
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*in = SnapshotQueryInput(decoded)
	return nil
}

// queryJSONShape checks every field before ordinary decoding can lose duplicate
// keys, unknown fields, explicit nulls, or omitted zero-valued fields.
func queryJSONShape(d *json.Decoder, typ reflect.Type, path string) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token == nil {
		return fmt.Errorf("%s: null is forbidden", path)
	}
	switch typ.Kind() {
	case reflect.Struct:
		if token != json.Delim('{') {
			return fmt.Errorf("%s: expected object", path)
		}
		fields := make(map[string]reflect.Type, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			fields[f.Tag.Get("json")] = f.Type
		}
		seen := map[string]bool{}
		for d.More() {
			keyToken, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok {
				return fmt.Errorf("%s: invalid key", path)
			}
			ft, ok := fields[key]
			if !ok {
				return fmt.Errorf("%s: unknown field %q", path, key)
			}
			if seen[key] {
				return fmt.Errorf("%s: duplicate field %q", path, key)
			}
			seen[key] = true
			if err := queryJSONShape(d, ft, path+"."+key); err != nil {
				return err
			}
		}
		if _, err := d.Token(); err != nil {
			return err
		}
		for i := 0; i < typ.NumField(); i++ {
			key := typ.Field(i).Tag.Get("json")
			if !seen[key] {
				return fmt.Errorf("%s: missing field %q", path, key)
			}
		}
	case reflect.Slice:
		if token != json.Delim('[') {
			return fmt.Errorf("%s: expected array", path)
		}
		for i := 0; d.More(); i++ {
			if err := queryJSONShape(d, typ.Elem(), fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
		_, err = d.Token()
		return err
	default:
		if _, ok := token.(json.Delim); ok {
			return fmt.Errorf("%s: expected scalar", path)
		}
	}
	return nil
}

// DecodeCanonicalSnapshotQueryInput accepts only exact signed canonical bytes.
// Authentication of the JWS and published snapshot is a separate caller duty.
func DecodeCanonicalSnapshotQueryInput(raw []byte) (SnapshotQueryInput, error) {
	var in SnapshotQueryInput
	if err := json.Unmarshal(raw, &in); err != nil {
		return SnapshotQueryInput{}, err
	}
	n, err := CanonicalSnapshotQueryInput(in)
	if err != nil {
		return SnapshotQueryInput{}, err
	}
	canonical, err := json.Marshal(n)
	if err != nil {
		return SnapshotQueryInput{}, err
	}
	if !bytes.Equal(raw, canonical) {
		return SnapshotQueryInput{}, fmt.Errorf("noncanonical snapshot query input bytes")
	}
	return n, nil
}
