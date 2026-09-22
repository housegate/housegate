package storageintegrity

import "strings"

// IsKnownNondeterministicName reports whether an already-lowercased ClickHouse
// name produces a value depending on the clock, on randomness, or on block/row
// position. Moved verbatim from the server-side ingress guard so that guard and
// the agent-mode inline VALUES closure gate share one list.
func IsKnownNondeterministicName(name string) bool {
	switch name {
	case "any", "anylast", "blocknumber", "blocksize", "curdate", "current_date",
		"current_timestamp", "datetimetouuidv7", "fuzzbits", "fuzzquery",
		"generaterandomstructure", "generateserialid", "generatesnowflakeid",
		"generateuuidv4", "generateuuidv7", "localtime", "localtimestamp", "now", "now64",
		"nowinblock", "nowinblock64", "obfuscatequery", "quantile", "quantiles", "rand",
		"rand32", "rand64", "randbernoulli", "randbinomial", "randcanonical",
		"randchisquared", "randconstant", "randexponential", "randfisherf", "randlognormal",
		"randnegativebinomial", "randnormal", "randpoisson", "randstudentt", "randuniform",
		"random", "randomfixedstring", "randomprintableascii", "randomstring",
		"randomstringutf8", "rownumberinallblocks", "rownumberinblock", "runningaccumulate",
		"runningconcurrency", "runningdifference",
		"runningdifferencestartingwithfirstvalue", "today", "utc_timestamp", "utctimestamp",
		"uuidv4", "yesterday":
		return true
	default:
		return false
	}
}

// IsServerStateFunctionName reports whether an already-lowercased name reads
// server, catalog or dictionary state (spec 2026-09-23 D3): a signed row must
// not embed a value only the executing server knows. Every dictGet* variant
// matches by prefix; the list is expected to grow and each addition is a policy
// change recorded in D3.
func IsServerStateFunctionName(name string) bool {
	if strings.HasPrefix(name, "dictget") {
		return true
	}
	switch name {
	case "joinget", "joingetornull", "currentuser", "currentdatabase", "currentschemas",
		"hostname", "fqdn", "version", "uptime", "serveruuid", "getsetting", "getmacro",
		"tcpport", "hascolumnintable", "sleep", "sleepeachrow", "throwif":
		return true
	default:
		return false
	}
}
