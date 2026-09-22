package storageintegrity

import (
	"strings"
	"testing"
)

var pinnedNondeterministicNames = []string{
	"any", "anylast", "blocknumber", "blocksize", "curdate", "current_date", "current_timestamp",
	"datetimetouuidv7", "fuzzbits", "fuzzquery", "generaterandomstructure", "generateserialid",
	"generatesnowflakeid", "generateuuidv4", "generateuuidv7", "localtime", "localtimestamp",
	"now", "now64", "nowinblock", "nowinblock64", "obfuscatequery", "quantile", "quantiles",
	"rand", "rand32", "rand64", "randbernoulli", "randbinomial", "randcanonical", "randchisquared",
	"randconstant", "randexponential", "randfisherf", "randlognormal", "randnegativebinomial",
	"randnormal", "randpoisson", "randstudentt", "randuniform", "random", "randomfixedstring",
	"randomprintableascii", "randomstring", "randomstringutf8", "rownumberinallblocks",
	"rownumberinblock", "runningaccumulate", "runningconcurrency", "runningdifference",
	"runningdifferencestartingwithfirstvalue", "today", "utc_timestamp", "utctimestamp",
	"uuidv4", "yesterday",
}

// TestContainsUnmaterializedNondeterminism_PinnedAcrossMove freezes the
// ingress guard's complete name policy and its literal/comment blanking while
// the list moves into the shared core package.
func TestContainsUnmaterializedNondeterminism_PinnedAcrossMove(t *testing.T) {
	if got := len(pinnedNondeterministicNames); got != 56 {
		t.Fatalf("pin has %d names, want 56", got)
	}
	for _, lower := range pinnedNondeterministicNames {
		for _, name := range []string{lower, strings.ToUpper(lower)} {
			t.Run("call/"+name, func(t *testing.T) {
				got, ok := containsUnmaterializedNondeterminism("INSERT INTO db.t (a) VALUES (" + name + "())")
				if !ok || got != name {
					t.Fatalf("got (%q, %v), want (%q, true)", got, ok, name)
				}
			})
		}
	}

	cases := []struct{ sql, want string }{
		{"INSERT INTO db.t (a) SELECT today", "today"},
		{"INSERT INTO db.t (a) VALUES (current_timestamp)", "current_timestamp"},
		{"INSERT INTO db.t (a) VALUES ('now()')", ""},
		{"INSERT INTO db.t (a) VALUES (1) -- now()", ""},
		{"INSERT INTO db.t (a) /* rand() */ VALUES (1)", ""},
		{"INSERT INTO db.t (a) VALUES (unhex('4142'))", ""},
		{"INSERT INTO db.t (a) VALUES (hostName())", ""},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			got, ok := containsUnmaterializedNondeterminism(tc.sql)
			if tc.want == "" && ok {
				t.Fatalf("got %q, want no match", got)
			}
			if tc.want != "" && (!ok || got != tc.want) {
				t.Fatalf("got (%q, %v), want (%q, true)", got, ok, tc.want)
			}
		})
	}
}
