package storageintegrity

import "testing"

// movedNondeterministicNames is the name set moved out of
// pkg/plugins/storageintegrity/plugin.go, byte for byte. Adding or removing an
// entry is a policy change that must also change the ingress pin test there.
var movedNondeterministicNames = []string{
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

// serverStateNames is the spec D3 deny-list: names whose value is known only
// to the server that executes the statement.
var serverStateNames = []string{
	"dictget", "dictgetstring", "dictgetuint64ordefault", "dictgethierarchy", "joinget",
	"joingetornull", "currentuser", "currentdatabase", "currentschemas", "hostname", "fqdn",
	"version", "uptime", "serveruuid", "getsetting", "getmacro", "tcpport", "hascolumnintable",
	"sleep", "sleepeachrow", "throwif",
}

func TestSharedFunctionNameLists(t *testing.T) {
	if got := len(movedNondeterministicNames); got != 56 {
		t.Fatalf("moved list has %d names, want the 56 of the ingress guard", got)
	}
	// Both functions take an already-lowercased name. Case-insensitivity belongs
	// to the two gates, which are pinned separately.
	groups := []struct {
		label string
		fn    func(string) bool
		yes   []string
		no    []string
	}{
		{"nondeterministic", IsKnownNondeterministicName, movedNondeterministicNames,
			[]string{"", "tostring", "unhex", "randomize", "NOW", "Rand", "dictget"}},
		{"server state", IsServerStateFunctionName, serverStateNames,
			[]string{"", "dict", "dictionaryhelper", "tostring", "DictGet", "Sleep", "now"}},
	}
	for _, g := range groups {
		for _, name := range g.yes {
			if !g.fn(name) {
				t.Errorf("%s(%q) = false, want true", g.label, name)
			}
		}
		for _, name := range g.no {
			if g.fn(name) {
				t.Errorf("%s(%q) = true, want false", g.label, name)
			}
		}
	}
}
