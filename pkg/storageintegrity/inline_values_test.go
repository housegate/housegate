package storageintegrity

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestParseInlineValuesInsert(t *testing.T) {
	cases := []struct {
		name, sql, wantDB, wantRows, wantErr string // wantErr "" = accept, "!" = ErrNotInlineValues
		wantCols                             []string
	}{
		{name: "no column list", sql: "INSERT INTO db.t VALUES (1), (2)", wantDB: "db", wantRows: "(1), (2)"},
		{name: "column list", sql: "INSERT INTO db.t (a, b) VALUES (1, 'x')", wantDB: "db", wantRows: "(1, 'x')", wantCols: []string{"a", "b"}},
		{name: "lowercase keywords", sql: "insert into db.t values (1)", wantDB: "db", wantRows: "(1)"},
		{name: "session database", sql: "INSERT INTO t (a) VALUES (1)", wantRows: "(1)", wantCols: []string{"a"}},
		{name: "quoted identifiers", sql: "INSERT INTO `db`.`t` (`a`) VALUES (1)", wantDB: "db", wantRows: "(1)", wantCols: []string{"a"}},
		{name: "quoted identifiers and comments", sql: "/*lead*/ INSERT /*a*/ INTO /*b*/ `db` /*c*/ . /*d*/ `t` /*e*/ (/*f*/ `a`, /*g*/ \"b\") /*h*/ VALUES (1, 'x')", wantDB: "db", wantRows: "(1, 'x')", wantCols: []string{"a", "b"}},
		{name: "comments without column list", sql: "INSERT INTO db.t /*before values*/ VALUES /*before rows*/ (1)", wantDB: "db", wantRows: "/*before rows*/ (1)"},
		{name: "expression rows", sql: "INSERT INTO db.t VALUES (1 + 2, unhex('4142'))", wantDB: "db", wantRows: "(1 + 2, unhex('4142'))"},
		{name: "trailing semicolon", sql: "INSERT INTO db.t VALUES (1);", wantDB: "db", wantRows: "(1)"},
		{name: "semicolon inside literal", sql: "INSERT INTO db.t VALUES ('a;b')", wantDB: "db", wantRows: "('a;b')"},
		{name: "25.x truncated shape", sql: "INSERT INTO db.t VALUES ", wantErr: "!"},
		{name: "25.x truncated no space", sql: "INSERT INTO db.t VALUES", wantErr: "!"},
		{name: "format native", sql: "INSERT INTO db.t FORMAT Native", wantErr: "!"},
		{name: "format values", sql: "INSERT INTO db.t FORMAT Values", wantErr: "!"},
		{name: "format with trailing settings", sql: "INSERT INTO db.t FORMAT Native SETTINGS async_insert = 1", wantErr: "!"},
		{name: "format with leading settings", sql: "INSERT INTO db.t SETTINGS async_insert = 1 FORMAT Native", wantErr: "!"},
		{name: "insert select", sql: "INSERT INTO db.t SELECT * FROM s", wantErr: "!"},
		{name: "insert select with trailing settings", sql: "INSERT INTO db.t SELECT 1 SETTINGS max_threads = 1", wantErr: "!"},
		{name: "insert select with leading settings", sql: "INSERT INTO db.t SETTINGS max_threads = 1 SELECT 1", wantErr: "!"},
		{name: "insert with", sql: "INSERT INTO db.t WITH x AS (SELECT 1) SELECT * FROM x", wantErr: "!"},
		{name: "insert with trailing settings", sql: "INSERT INTO db.t WITH 1 AS x SELECT x SETTINGS max_threads = 1", wantErr: "!"},
		{name: "not an insert", sql: "SELECT 1", wantErr: "!"},
		{name: "empty text", sql: "", wantErr: "!"},
		{name: "trailing settings", sql: "INSERT INTO db.t VALUES (1) SETTINGS async_insert = 1", wantErr: "async_insert"},
		{name: "leading settings", sql: "INSERT INTO db.t SETTINGS async_insert = 1 VALUES (1)", wantErr: "async_insert"},
		{name: "second statement", sql: "INSERT INTO db.t VALUES (1); INSERT INTO db.t VALUES (2)", wantErr: "multi-statement"},
		{name: "second statement supplies values", sql: "INSERT INTO db.t; INSERT INTO db.u VALUES (1)", wantErr: "multi-statement"},
		{name: "second statement after column list supplies values", sql: "INSERT INTO db.t (a) ; INSERT INTO db.u VALUES (1)", wantErr: "multi-statement"},
		{name: "garbage before values", sql: "INSERT INTO db.t nonsense VALUES (1)", wantErr: "nonsense"},
		{name: "garbage after column list before values", sql: "INSERT INTO db.t (a) nonsense VALUES (1)", wantErr: "nonsense"},
		{name: "insert into function", sql: "INSERT INTO FUNCTION remote('h', db.t) VALUES (1)", wantErr: "INSERT INTO FUNCTION"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseInlineValuesInsert(tc.sql)
			if tc.wantErr == "!" {
				if !errors.Is(err, ErrNotInlineValues) {
					t.Fatalf("err = %v, want ErrNotInlineValues", err)
				}
				return
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) ||
					!strings.HasPrefix(err.Error(), InlineValuesErrorPrefix) || errors.Is(err, ErrNotInlineValues) {
					t.Fatalf("err = %v, want a named %q-prefixed refusal containing %q", err, InlineValuesErrorPrefix, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Target.Database != tc.wantDB || got.Target.Table != "t" || got.Rows != tc.wantRows ||
				strings.Join(got.Columns, ",") != strings.Join(tc.wantCols, ",") {
				t.Fatalf("got %+v, want database %q table \"t\" rows %q columns %v", got, tc.wantDB, tc.wantRows, tc.wantCols)
			}
		})
	}
}

func TestValuesClosure(t *testing.T) {
	accept := []string{
		"(1), (2)",
		"(-1, +2)",
		"(0, 1., 1.5, 2.5e-3, 4E7)",
		"(0xFF, 0X00)",
		"('it''s')",
		`('\'', '\\', '\n', '\t', '\x41')`,
		"(true, FALSE, True)",
		"(1 + 2 - 3 * 4 / 5 % 6)",
		"(1 = 1, 1 != 2, 1 <> 2, 1 < 2, 1 <= 2, 2 >= 1)",
		"((1 + 2) * 3)",
		"(unhex('4142'), toInt64(1))",
		"(toDateTime(fromUnixTimestamp64Milli(1758000)))",
		"(1),\n\t(2)",
		"(dictionaryHelper(1))",
	}
	for _, rows := range accept {
		t.Run("accept "+rows, func(t *testing.T) {
			if err := ValuesClosure(rows); err != nil {
				t.Fatalf("ValuesClosure(%q) = %v, want nil", rows, err)
			}
		})
	}

	refuse := []struct{ rows, want string }{
		{"1", "parenthesized row tuple"},
		{"()", "empty row tuple"},
		{"(1)(2)", "expected ',' between row tuples"},
		{"(1),", "row tuple after ','"},
		{"(0x)", "hexadecimal literal requires at least one digit"},
		{"(0xG)", "hexadecimal literal requires at least one digit"},
		{"(1e)", "exponent requires at least one digit"},
		{"(1e+)", "exponent requires at least one digit"},
		{"(1abc)", "numeric literal is not delimited"},
		{"(0xFFoops)", "numeric literal is not delimited"},
		{"((SELECT count() FROM system.tables))", `keyword "SELECT"`},
		{"(1 FROM t)", `keyword "FROM"`},
		{"(WITH x AS 1)", `keyword "WITH"`},
		{"(1 IN (1, 2))", `keyword "IN"`},
		{"(NULL)", `keyword "NULL"`},
		{"(DEFAULT)", `keyword "DEFAULT"`},
		{"(CAST(1 AS Int64))", `keyword "CAST"`},
		{"(1 AS x)", `keyword "AS"`},
		{"(INTERVAL 1 DAY)", `keyword "INTERVAL"`},
		{"(1 AND 1)", `keyword "AND"`},
		{"(1 OR 1)", `keyword "OR"`},
		{"(NOT 1)", `keyword "NOT"`},
		{"(1 and 1)", `keyword "and"`},
		{"(x)", `bare identifier "x"`},
		{"(a + 1)", `bare identifier "a"`},
		{"(now ())", `bare identifier "now"`},
		{"(now())", `nondeterministic function "now"`},
		{"(GenerateUUIDv4())", `nondeterministic function "GenerateUUIDv4"`},
		{"(hostName())", `server-state function "hostName"`},
		{"(SLEEP(1))", `server-state function "SLEEP"`},
		{"(dictGetString('d', 'k', 1))", `server-state function "dictGetString"`},
		{"(1) -- rest", "SQL comments are not accepted"},
		{"(1) # rest", "SQL comments are not accepted"},
		{"(1 /* x */)", "SQL comments are not accepted"},
		{"(1) // rest", "SQL comments are not accepted"},
		{"(`a`)", "quoted identifiers are not accepted"},
		{`("a")`, "quoted identifiers are not accepted"},
		{"($$abc$$)", "heredoc string literals are not accepted"},
		{"($tag$abc$tag$)", "heredoc string literals are not accepted"},
		{"({p:Identifier})", "query parameters are not accepted"},
		{"(?)", `token "?"`},
		{"(@x)", `token "@"`},
		{"(1::Int64)", `token ":"`},
		{"([1, 2])", `token "["`},
		{"(1); (2)", `token ";"`},
		{"('abc)", "unterminated string literal"},
		{`('\q')`, `string escape "\\q" is not accepted`},
		{`('\xZZ')`, `escape \x requires two hexadecimal digits`},
		{"(1", "unbalanced '('"},
		{"(1))", "unbalanced ')'"},
		{"", "the VALUES row list is empty"},
		{"   ", "the VALUES row list is empty"},
	}
	for _, tc := range refuse {
		t.Run("refuse "+tc.rows, func(t *testing.T) {
			err := ValuesClosure(tc.rows)
			if err == nil {
				t.Fatalf("ValuesClosure(%q) = nil, want a refusal", tc.rows)
			}
			if !strings.HasPrefix(err.Error(), InlineValuesErrorPrefix) || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %q, want %q-prefixed and containing %q", err.Error(), InlineValuesErrorPrefix, tc.want)
			}
		})
	}
}

func TestValuesClosurePinsFunctionNamePoliciesCaseInsensitively(t *testing.T) {
	groups := []struct {
		label string
		names []string
	}{
		{"nondeterministic function", movedNondeterministicNames},
		{"server-state function", append(append([]string(nil), serverStateNames...), "dictgetcustom")},
	}
	for _, group := range groups {
		for _, lower := range group.names {
			for _, name := range []string{lower, strings.ToUpper(lower)} {
				t.Run(group.label+"/"+name, func(t *testing.T) {
					err := ValuesClosure(fmt.Sprintf("(%s())", name))
					if err == nil || !strings.Contains(err.Error(), group.label+" "+fmt.Sprintf("%q", name)) {
						t.Fatalf("ValuesClosure(%q) = %v, want %s refusal", name, err, group.label)
					}
				})
			}
		}
	}
}
