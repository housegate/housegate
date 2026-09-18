package auth

import (
	"encoding/base64"
	"encoding/json"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

func TestStatementV3RoundTrip(t *testing.T) {
	signer, err := NewRelaySigner(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	want := statementV3Payload(t)
	token, err := signer.SignStatementV3(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeStatementV3Payload(token)
	if err != nil || got != want {
		t.Fatalf("round trip = %+v, %v", got, err)
	}
	account, err := VerifyStatementV3Signature(token, want)
	if err != nil || account != signer.Address() {
		t.Fatalf("verify = %s, %v", account, err)
	}
}

// Walk the complete frozen struct so a newly added field cannot silently lack
// tamper coverage. Nested pin fields get their own named refusal.
func TestStatementV3EveryBoundField(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	want := statementV3Payload(t)
	token, err := signer.SignStatementV3(want)
	if err != nil {
		t.Fatal(err)
	}
	var walk func(reflect.Type, []int, string)
	walk = func(typ reflect.Type, indices []int, prefix string) {
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			path := append(append([]int(nil), indices...), i)
			name := prefix + field.Tag.Get("json")
			if name == "iat" {
				continue
			}
			if field.Type.Kind() == reflect.Struct {
				if name == "binding" {
					walk(field.Type, path, "")
				} else {
					walk(field.Type, path, name+".")
				}
				continue
			}
			t.Run(name, func(t *testing.T) {
				changed := want
				value := reflect.ValueOf(&changed).Elem().FieldByIndex(path)
				switch value.Kind() {
				case reflect.String:
					value.SetString(value.String() + "x")
				case reflect.Uint32, reflect.Uint64:
					value.SetUint(value.Uint() + 1)
				default:
					t.Fatalf("add tamper for %s", field.Type)
				}
				if got := StatementPayloadV3Mismatch(changed, want); got != name {
					t.Fatalf("mismatch = %q, want %q", got, name)
				}
				if _, err := VerifyStatementV3Signature(token, changed); err == nil || !strings.Contains(err.Error(), name) {
					t.Fatalf("expected %s refusal, got %v", name, err)
				}
				// A valid signature over a different binding must also fail.
				raw, err := json.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
				other := statementV3SignRaw(t, canonicalJWSProtectedHeader, string(raw))
				if _, err := VerifyStatementV3Signature(other, want); err == nil || !strings.Contains(err.Error(), name) {
					t.Fatalf("signed tamper must name %s: %v", name, err)
				}
			})
		}
	}
	walk(reflect.TypeOf(want), nil, "")
}

func TestStatementPayloadV3MismatchBindsQueryProfile(t *testing.T) {
	want := statementV3Payload(t)
	want.Binding.QueryProfileID = "active"
	got := want
	got.Binding.QueryProfileID = "historical"
	if field := StatementPayloadV3Mismatch(got, want); field != "query_profile_id" {
		t.Fatalf("mismatch field=%q", field)
	}
	got = want
	got.Iat++
	if field := StatementPayloadV3Mismatch(got, want); field != "" {
		t.Fatalf("iat must be separate from input identity: %s", field)
	}
}

func statementV3SignRaw(t *testing.T, header, payload string) string {
	t.Helper()
	input := base64.RawURLEncoding.EncodeToString([]byte(header)) + "." + base64.RawURLEncoding.EncodeToString([]byte(payload))
	key, err := crypto.HexToECDSA(statementV2TestKey)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := crypto.Sign(keccak256([]byte(input)), key)
	if err != nil {
		t.Fatal(err)
	}
	sig[64] += 27
	return input + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func TestStatementV3CanonicalPayload(t *testing.T) {
	want := statementV3Payload(t)
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	canonical := string(raw)
	variants := map[string]string{
		"whitespace":    " " + canonical,
		"trailing":      canonical + "{}",
		"null":          "null",
		"unknown":       strings.Replace(canonical, `"iat":`, `"extra":1,"iat":`, 1),
		"mixed_v2":      strings.Replace(canonical, `"iat":`, `"payload_hash":"0x01","iat":`, 1),
		"mixed_case":    strings.Replace(canonical, `"purpose":`, `"Purpose":`, 1),
		"escaped_value": strings.Replace(canonical, "housegate-statement", `housegate\u002dstatement`, 1),
		"escaped_key":   strings.Replace(canonical, `"purpose"`, `"purpos\u0065"`, 1),
		"iat_exponent":  strings.Replace(canonical, `"iat":1789550000`, `"iat":1789550000e0`, 1),
		"iat_overflow":  strings.Replace(canonical, `"iat":1789550000`, `"iat":9223372036854775808`, 1),
		"reordered":     strings.Replace(canonical, `"purpose":"housegate-statement-v3","iat":1789550000`, `"iat":1789550000,"purpose":"housegate-statement-v3"`, 1),
	}
	// Refuse missing, duplicate, unknown and null fields at every object depth,
	// including fields whose zero value is otherwise meaningful (genesis/shard).
	for _, key := range []string{"purpose", "iat", "binding", "input_root", "envelope_version", "keeper_shard_id", "read_snapshot", "safe_block_seq", "state_root"} {
		needle := `"` + key + `":`
		start := strings.Index(canonical, needle)
		if start < 0 {
			t.Fatalf("missing fixture key %s", key)
		}
		var value json.RawMessage
		d := json.NewDecoder(strings.NewReader(canonical[start+len(needle):]))
		if err := d.Decode(&value); err != nil {
			t.Fatal(err)
		}
		entry := needle + string(value)
		variants["duplicate_"+key] = strings.Replace(canonical, entry, entry+","+entry, 1)
		variants["null_"+key] = strings.Replace(canonical, entry, needle+"null", 1)
		if strings.Contains(canonical, entry+",") {
			variants["missing_"+key] = strings.Replace(canonical, entry+",", "", 1)
		} else {
			variants["missing_"+key] = strings.Replace(canonical, ","+entry, "", 1)
		}
		variants["unknown_near_"+key] = strings.Replace(canonical, entry, `"foreign_field":0,`+entry, 1)
	}
	for name, payload := range variants {
		t.Run(name, func(t *testing.T) {
			if payload == canonical {
				t.Fatal("mutation did not change payload")
			}
			token := statementV3SignRaw(t, canonicalJWSProtectedHeader, payload)
			if _, err := DecodeStatementV3Payload(token); err == nil || !strings.Contains(err.Error(), "payload") {
				t.Fatalf("decode must refuse payload: %v", err)
			}
			if _, err := VerifyStatementV3Signature(token, want); err == nil || !strings.Contains(err.Error(), "payload") {
				t.Fatalf("verify must refuse payload: %v", err)
			}
		})
	}
}

func TestStatementV3CanonicalHeaderAndBase64(t *testing.T) {
	want := statementV3Payload(t)
	raw, _ := json.Marshal(want)
	for name, header := range map[string]string{
		"whitespace": `{ "alg":"ES256K","typ":"JWT"}`,
		"order":      `{"typ":"JWT","alg":"ES256K"}`,
		"alias":      `{"alg":"secp256k1","typ":"JWT"}`,
		"unknown":    `{"alg":"ES256K","typ":"JWT","kid":"a"}`,
		"duplicate":  `{"alg":"ES256K","alg":"ES256K","typ":"JWT"}`,
		"missing":    `{"alg":"ES256K"}`,
		"none":       `{"alg":"none","typ":"JWT"}`,
	} {
		t.Run(name, func(t *testing.T) {
			token := statementV3SignRaw(t, header, string(raw))
			if _, err := VerifyStatementV3Signature(token, want); err == nil || !strings.Contains(err.Error(), "header") {
				t.Fatalf("expected protected header refusal: %v", err)
			}
		})
	}
	token := statementV3SignRaw(t, canonicalJWSProtectedHeader, string(raw))
	parts := strings.Split(token, ".")
	for index, name := range []string{"header", "payload", "signature"} {
		variants := []string{parts[index] + "=", parts[index] + "\n"}
		alias, ok := nonCanonicalPadBitAlias(parts[index])
		if !ok {
			t.Fatalf("fixture segment %s must have pad bits", name)
		}
		variants = append(variants, alias)
		for _, variant := range variants {
			changed := append([]string(nil), parts...)
			changed[index] = variant
			if _, err := VerifyStatementV3Signature(strings.Join(changed, "."), want); err == nil || !strings.Contains(err.Error(), "canonical base64url") {
				t.Fatalf("%s: expected encoding refusal, got %v", name, err)
			}
		}
	}
	for _, wrapped := range []string{" " + token, token + " ", `"` + token + `"`, "'" + token + "'", token + ".", "not.a.jws", ""} {
		if _, err := VerifyStatementV3Signature(wrapped, want); err == nil {
			t.Fatal("accepted noncanonical token wrapper")
		}
	}
}

func TestStatementV3PurposeSeparationAndRecoveredAccount(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	want := statementV3Payload(t)
	v2, err := signer.SignStatementV2(statementV2Fixture(signer.Address()))
	if err != nil {
		t.Fatal(err)
	}
	query, err := signer.signCompactJWS(JWSPayload{Iat: want.Iat, Purpose: QueryPurpose, QueryHash: want.Binding.SQLHash})
	if err != nil {
		t.Fatal(err)
	}
	peer, err := signer.signCompactJWS(JWSPeerPayload{Iat: want.Iat, Exp: want.Iat + 60, Aud: "peer", Purpose: PeerLoginPurpose})
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{"v2": v2, "query": query, "peer": peer} {
		t.Run(name, func(t *testing.T) {
			if _, err := VerifyStatementV3Signature(token, want); err == nil {
				t.Fatal("accepted different token lane")
			}
		})
	}
	other, _ := NewRelaySigner("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	token, err := other.SignStatementV3(want)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyStatementV3Signature(token, want); err == nil || !strings.Contains(err.Error(), "client_account") {
		t.Fatalf("expected signer/account mismatch: %v", err)
	}
	// Changing only iat preserves bound input identity but still invalidates the
	// signature. Pure replay ignores freshness, never cryptographic integrity.
	token, _ = signer.SignStatementV3(want)
	parts := strings.Split(token, ".")
	want.Iat++
	raw, _ := json.Marshal(want)
	parts[1] = base64.RawURLEncoding.EncodeToString(raw)
	if _, err := VerifyStatementV3Signature(strings.Join(parts, "."), want); err == nil {
		t.Fatal("accepted unsigned iat change")
	}
}

func TestStatementV3FreshnessAndHistoricalVerification(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	want := statementV3Payload(t)
	validator := NewEthValidator([]string{signer.Address()}, time.Minute, true, false, "", nil)
	now := time.Now().Unix()
	for _, tc := range []struct {
		name    string
		iat     int64
		refusal string
	}{
		{"current", now, ""}, {"future_skew", now + 4, ""}, {"future", now + 60, "future"},
		{"old", now - 120, "expired"}, {"min_int", math.MinInt64, "expired"}, {"max_int", math.MaxInt64, "future"},
		{"duration_overflow", now - (1 << 40), "expired"}, {"zero", 0, "expired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := want
			payload.Iat = tc.iat
			// Raw canonical signing permits the zero historical timestamp; the
			// public signer supplies the current time when iat is omitted.
			token, err := signer.signCompactJWS(payload)
			if err != nil {
				t.Fatal(err)
			}
			if account, err := VerifyStatementV3Signature(token, want); err != nil || account != signer.Address() {
				t.Fatalf("pure verifier depends on time: %s, %v", account, err)
			}
			_, err = validator.ValidateStatementV3(token, want)
			if tc.refusal == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("expected %s: %v", tc.refusal, err)
			}
		})
	}
}

func TestStatementV3AdmissionAllowlistAndSignerDefaults(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	want := statementV3Payload(t)
	want.Iat = 0
	want.Purpose = "caller-supplied-purpose"
	before := time.Now().Unix()
	token, err := signer.SignStatementV3(want)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := DecodeStatementV3Payload(token)
	if err != nil {
		t.Fatal(err)
	}
	if payload.Iat < before || payload.Iat > time.Now().Unix() || payload.Purpose != StatementPurposeV3 {
		t.Fatal("signer did not supply purpose/iat")
	}
	want.Purpose = StatementPurposeV3
	for _, tc := range []struct {
		name      string
		addresses []string
		denied    bool
	}{
		{"allowed", []string{strings.ToUpper(signer.Address())}, false},
		{"open", nil, false}, {"denied", []string{"0x0000000000000000000000000000000000000001"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validator := NewEthValidator(tc.addresses, time.Minute, false, true, "", nil)
			_, err := validator.ValidateStatementV3(token, want)
			if tc.denied {
				if err == nil || !strings.Contains(err.Error(), "allowlist") {
					t.Fatalf("expected allowlist refusal: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if _, err := validator.ValidateStatementV3("", want); err == nil {
				t.Fatal("Enabled/AllowNoAuth must not bypass signed statements")
			}
		})
	}
}

func TestStatementV3CanonicalSignatureIdentity(t *testing.T) {
	want := statementV3Payload(t)
	token := statementV3Fixture(t).Identities[0].UserJWS
	parts := strings.Split(token, ".")
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		t.Fatal(err)
	}
	lowV := append([]byte(nil), sig...)
	lowV[64] -= 27
	highS := append([]byte(nil), sig...)
	s := new(big.Int).SetBytes(sig[32:64])
	new(big.Int).Sub(crypto.S256().Params().N, s).FillBytes(highS[32:64])
	highS[64] = 27 + ((sig[64] - 27) ^ 1)
	// Prove these are alternate encodings of the SAME recovered identity.
	// The v3 refusal is intentional canonical policy, not invalid crypto.
	for name, alternate := range map[string][]byte{"V_0_1": lowV, "high_S": highS} {
		t.Run(name, func(t *testing.T) {
			address, err := recoverAddress(keccak256([]byte(parts[0]+"."+parts[1])), alternate)
			if err != nil || address != want.Binding.ClientAccount {
				t.Fatalf("alternate is not equivalent: %s, %v", address, err)
			}
			mutated := parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(alternate)
			if _, err := VerifyStatementV3Signature(mutated, want); err == nil || !strings.Contains(err.Error(), "signature") {
				t.Fatalf("expected canonical signature refusal: %v", err)
			}
		})
	}
	for name, alternate := range map[string][]byte{
		"empty": nil, "64_bytes": sig[:64], "66_bytes": append(append([]byte(nil), sig...), 0),
		"zero_scalar": make([]byte, 65),
	} {
		t.Run(name, func(t *testing.T) {
			if name == "zero_scalar" {
				alternate[64] = 27
			}
			mutated := parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(alternate)
			if _, err := VerifyStatementV3Signature(mutated, want); err == nil || !strings.Contains(err.Error(), "signature") {
				t.Fatalf("expected signature refusal: %v", err)
			}
		})
	}
	for _, v := range []byte{0, 1, 2, 26, 29, 255} {
		alternate := append([]byte(nil), sig...)
		alternate[64] = v
		mutated := parts[0] + "." + parts[1] + "." + base64.RawURLEncoding.EncodeToString(alternate)
		if _, err := VerifyStatementV3Signature(mutated, want); err == nil || !strings.Contains(err.Error(), "V=27/28") {
			t.Fatalf("accepted recovery byte %d: %v", v, err)
		}
	}
}

func TestStatementV3DomainCannotBeOverriddenByWant(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	for name, mutate := range map[string]func(*JWSStatementPayloadV3){
		"purpose":          func(p *JWSStatementPayloadV3) { p.Purpose = StatementPurposeV2 },
		"envelope_version": func(p *JWSStatementPayloadV3) { p.Binding.EnvelopeVersion = 2 },
		"input_kind":       func(p *JWSStatementPayloadV3) { p.Binding.InputKind = "payload" },
		"statement_kind":   func(p *JWSStatementPayloadV3) { p.Binding.StatementKind = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			want := statementV3Payload(t)
			mutate(&want)
			token, err := signer.signCompactJWS(want)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyStatementV3Signature(token, want); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("expected %s domain refusal: %v", name, err)
			}
		})
	}
}

func TestStatementV3HistoricalSignatureDoesNotAuthorizeCurrentProfile(t *testing.T) {
	signer, _ := NewRelaySigner(statementV2TestKey)
	historical := statementV3Payload(t)
	historical.Binding.QueryProfileID = "historical"
	token, err := signer.SignStatementV3(historical)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyStatementV3Signature(token, historical); err != nil {
		t.Fatal(err)
	}
	active := historical
	active.Binding.QueryProfileID = "active"
	if _, err := VerifyStatementV3Signature(token, active); err == nil || !strings.Contains(err.Error(), "query_profile_id") {
		t.Fatalf("historical signature cannot satisfy a different current binding: %v", err)
	}
}

func TestStatementV3GenesisZeroFieldsMustBePresent(t *testing.T) {
	want := statementV3Payload(t)
	want.Binding.KeeperShardID = 0
	want.Binding.ReadSnapshot.KeeperShardID = 0
	want.Binding.ReadSnapshot.SafeBlockSeq = 0
	raw, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	token := statementV3SignRaw(t, canonicalJWSProtectedHeader, string(raw))
	if _, err := VerifyStatementV3Signature(token, want); err != nil {
		t.Fatalf("explicit zeros must survive: %v", err)
	}
	for _, field := range []string{`"keeper_shard_id":0,`, `"safe_block_seq":0,`} {
		mutated := strings.ReplaceAll(string(raw), field, "")
		token := statementV3SignRaw(t, canonicalJWSProtectedHeader, mutated)
		if _, err := VerifyStatementV3Signature(token, want); err == nil || !strings.Contains(err.Error(), "payload") {
			t.Fatalf("missing zero field %s: %v", field, err)
		}
	}
}

func TestStatementV3FreshnessBoundaries(t *testing.T) {
	// Deterministic boundaries at different local clocks, including both int64
	// extremes. Real admission integration is covered above with time.Now().
	for _, now := range []int64{-123456789, 0, 1789550000} {
		for _, tc := range []struct {
			iat     int64
			maxAge  time.Duration
			refusal string
		}{
			{now - 60, time.Minute, ""}, {now - 61, time.Minute, "expired"},
			{now + 5, time.Minute, ""}, {now + 6, time.Minute, "future"},
			{now, time.Duration(0), ""}, {now - 1, 0, "expired"},
			{now, -1, "expired"}, {now + 5, -1, "expired"},
			{now - 1, 1500 * time.Millisecond, ""}, {now - 2, 1500 * time.Millisecond, "expired"},
			{math.MinInt64, time.Duration(math.MaxInt64), "expired"},
			{math.MaxInt64, time.Duration(math.MaxInt64), "future"},
		} {
			err := statementV3Freshness(tc.iat, now, tc.maxAge)
			if tc.refusal == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.refusal) {
				t.Fatalf("iat=%d now=%d max=%s: want %s, got %v", tc.iat, now, tc.maxAge, tc.refusal, err)
			}
		}
	}
	for _, tc := range []struct {
		iat, now int64
		refusal  string
	}{
		{math.MaxInt64, math.MinInt64, "future"},
		{math.MinInt64, math.MaxInt64, "expired"},
		{math.MinInt64, math.MinInt64, ""},
		{math.MaxInt64, math.MaxInt64, ""},
	} {
		err := statementV3Freshness(tc.iat, tc.now, time.Minute)
		if tc.refusal == "" {
			if err != nil {
				t.Fatal(err)
			}
		} else if err == nil || !strings.Contains(err.Error(), tc.refusal) {
			t.Fatalf("extreme timestamp refusal = %v, want %s", err, tc.refusal)
		}
	}
}
