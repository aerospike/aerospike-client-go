//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package opdifferences reports how the server actually evaluates Aerospike
// Expression Language, where that differs from what a client-side parser does.
//
// Port of the Java SDK's OperationDifferences, by way of the Rust SDK's
// `operation_differences`. Each check runs one AEL filter against one seeded
// record and compares the outcome against what the specification describes, so
// the output is a report rather than a set of assertions: the point is to show
// where the two disagree, not to fail when they do.
package opdifferences

import (
	"errors"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

const separator = "========================================================================"

// outcome is what running one filter against one record did.
type outcome int

const (
	// matched means the record passed the filter.
	matched outcome = iota
	// filteredOut means the filter evaluated to false for this record.
	filteredOut
	// failed means the server refused the expression.
	failed
)

func (o outcome) String() string {
	switch o {
	case matched:
		return "matched"
	case filteredOut:
		return "filtered out"
	default:
		return "failed"
	}
}

// report tallies how many checks behaved as the specification describes.
type report struct {
	env     *exrun.Env
	total   int
	matched int
}

// check records one comparison, printing the expectation and what happened.
func (r *report) check(what, expected string, got, want outcome) {
	r.total++
	verdict := "DIFFERS"
	if got == want {
		r.matched++
		verdict = "as described"
	}
	r.env.Printf("  %-58s %-14s [%s]", what, got, verdict)
	if got != want {
		r.env.Printf("      expected: %s", expected)
	}
}

// Run executes the example.
func Run(env *exrun.Env) error {
	set, err := env.DataSet("op_differences")
	if err != nil {
		return err
	}

	// The whole example is about server-compiled AEL, so without it there is
	// nothing to report on.
	if !env.Cluster.SupportsServerCompiledAEL() {
		env.Printf("skipping: this cluster cannot compile AEL text (needs server 8.1.3+), " +
			"so there is no server-side AEL behavior to report on.")
		return nil
	}

	if err := seed(env, set); err != nil {
		return err
	}

	r := &report{env: env}
	if err := mapKeyIdentifiers(env, set, r); err != nil {
		return err
	}
	if err := rightShift(env, set, r); err != nil {
		return err
	}
	if err := existsFunction(env, set, r); err != nil {
		return err
	}
	if err := numericCasts(env, set, r); err != nil {
		return err
	}

	env.Printf("%s", separator)
	env.Printf("SUMMARY: %d/%d checks behaved as the spec describes; %d differ",
		r.matched, r.total, r.total-r.matched)
	env.Printf("%s", separator)
	return nil
}

// heading introduces a group of checks.
func heading(env *exrun.Env, title, detail string) {
	env.Printf("%s", separator)
	env.Printf("%s", title)
	env.Printf("  %s", detail)
	env.Printf("%s", separator)
}

// mapKeyIdentifiers asks what a bare digit means as a path step: the integer
// key 1, or the string key "1"?
func mapKeyIdentifiers(env *exrun.Env, set *sdk.DataSet, r *report) error {
	heading(env, "Digit path steps and map keys",
		"the spec's identifiers start with a letter, so $.m.1 addresses the integer key 1. "+
			"Java's client-side parser accepts a digit-leading identifier and looks up the "+
			"string key \"1\" instead; here the server does the parsing.")

	intKeys := set.Key(2)
	got, err := probe(env, intKeys, "$.m.1 == 'val_from_int_key_1'")
	if err != nil {
		return err
	}
	r.check("map with integer keys only: $.m.1 == 'val_from_int_key_1'",
		"matched (integer key 1 resolved)", got, matched)

	// A map holding both keys tells which one the server picked.
	bothKeys := set.Key(3)
	if got, err = probe(env, bothKeys, "$.m.1 == 'INTEGER_KEY_1'"); err != nil {
		return err
	}
	r.check("map with both keys: $.m.1 == 'INTEGER_KEY_1'",
		"matched (integer key wins)", got, matched)

	if got, err = probe(env, bothKeys, "$.m.1 == 'STRING_KEY_1'"); err != nil {
		return err
	}
	r.check("map with both keys: $.m.1 == 'STRING_KEY_1'",
		"filtered out (the string key is a different entry)", got, filteredOut)

	// A quoted step is unambiguous, whichever way the bare form resolves.
	if got, err = probe(env, bothKeys, "$.m.'1' == 'STRING_KEY_1'"); err != nil {
		return err
	}
	r.check("map with both keys: $.m.'1' == 'STRING_KEY_1'",
		"matched (a quoted step is a string key)", got, matched)
	return nil
}

// rightShift asks whether >> is arithmetic or logical, and whether >>> exists.
func rightShift(env *exrun.Env, set *sdk.DataSet, r *report) error {
	heading(env, "Right shift",
		"spec and Java convention: >> is arithmetic (sign-preserving) and >>> is logical "+
			"(zero-fill). Java's client-side parser wires >> to the logical shift and has no "+
			">>> at all; here the server compiles both.")

	key := set.Key(1)
	// -8 >> 1 is -4 arithmetically, 9223372036854775804 logically.
	got, err := probe(env, key, "$.intBin >> 1 == -4")
	if err != nil {
		return err
	}
	r.check("negative value: $.intBin >> 1 == -4",
		"matched (an arithmetic shift keeps the sign)", got, matched)

	if got, err = probe(env, key, "$.intBin >>> 1 == 9223372036854775804"); err != nil {
		return err
	}
	r.check("negative value: $.intBin >>> 1 == 9223372036854775804",
		"matched (a logical shift zero-fills)", got, matched)
	return nil
}

// existsFunction checks exists() on a present bin, a missing bin, and negated.
func existsFunction(env *exrun.Env, set *sdk.DataSet, r *report) error {
	heading(env, "exists()",
		"record 4 has binA=42 and flag=true, and no binB at all. Java's client-side visitor "+
			"drops exists() from the expression it builds; the server implements it. Note the "+
			"server's grammar wants the negation parenthesized: `not (...)`.")

	key := set.Key(4)
	got, err := probe(env, key, "$.binA.exists() and $.flag")
	if err != nil {
		return err
	}
	r.check("present bin: $.binA.exists() and $.flag",
		"matched (binA exists, flag is true)", got, matched)

	if got, err = probe(env, key, "$.binB.exists()"); err != nil {
		return err
	}
	r.check("missing bin: $.binB.exists()",
		"filtered out (exists() is false, not an error)", got, filteredOut)

	if got, err = probe(env, key, "not ($.binB.exists())"); err != nil {
		return err
	}
	r.check("missing bin, negated: not ($.binB.exists())",
		"matched (the negation of a false existence check)", got, matched)

	if got, err = probe(env, key, "$.binB.exists() == false"); err != nil {
		return err
	}
	r.check("missing bin, compared: $.binB.exists() == false",
		"matched (exists() yields a boolean, comparable as one)", got, matched)
	return nil
}

// numericCasts exercises mixed-type arithmetic and the explicit casts the
// specification provides for it.
func numericCasts(env *exrun.Env, set *sdk.DataSet, r *report) error {
	heading(env, "asInt() / asFloat()",
		"record 7 has intBin=10 (INT) and floatBin=3.5 (FLOAT). Arithmetic wants both "+
			"operands in the same type, and the spec supplies these casts to get there.")

	key := set.Key(7)

	// Baselines, so the cast results below mean something.
	for _, c := range []struct {
		filter, what, expected string
		want                   outcome
	}{
		{"$.intBin == 10", "baseline: $.intBin == 10", "matched (the bin really holds 10)", matched},
		{"$.floatBin == 3.5", "baseline: $.floatBin == 3.5", "matched (the bin really holds 3.5)", matched},

		// Mixed types without a cast: neither sum matches, and the server does
		// not report the type mismatch either.
		{"$.intBin + $.floatBin == 13", "no cast: $.intBin + $.floatBin == 13",
			"failed (the spec wants mixed-type arithmetic rejected)", failed},
		{"$.intBin + $.floatBin == 13.5", "no cast: $.intBin + $.floatBin == 13.5",
			"filtered out (no silent promotion to float)", filteredOut},

		// Cross-type casts work as the specification describes.
		{"$.intBin + $.floatBin.asInt() == 13", "float to int: $.intBin + $.floatBin.asInt() == 13",
			"matched (3.5 truncates to 3)", matched},
		{"$.intBin.asFloat() + $.floatBin == 13.5", "int to float: $.intBin.asFloat() + $.floatBin == 13.5",
			"matched (10 widens to 10.0)", matched},
		{"$.floatBin.asInt().asFloat() == 3.0", "round trip: $.floatBin.asInt().asFloat() == 3.0",
			"matched (the fractional part is lost)", matched},

		// Same-type casts, which ought to be no-ops.
		{"$.intBin.asInt() == 10", "no-op cast: $.intBin.asInt() == 10",
			"matched (casting an int to int changes nothing)", matched},
		{"$.floatBin.asFloat() == 3.5", "no-op cast: $.floatBin.asFloat() == 3.5",
			"matched (casting a float to float changes nothing)", matched},
	} {
		got, err := probe(env, key, c.filter)
		if err != nil {
			return err
		}
		r.check(c.what, c.expected, got, c.want)
	}
	return nil
}

// probe runs one filter as a server-compiled AEL filter on one record.
//
// A filtered-out record and a refused expression are both outcomes here, not
// errors: reporting the difference is the whole point.
func probe(env *exrun.Env, key *as.Key, filter string) (outcome, error) {
	stream, err := env.Session.Query(key).Where(filter).FailOnFilteredOut().Execute()
	if err != nil {
		return classify(err), nil
	}
	defer stream.Close()

	row, err := stream.Next()
	if err != nil {
		return classify(err), nil
	}
	if row == nil {
		return filteredOut, nil
	}
	if !row.IsOK() {
		if row.ResultCode == types.FILTERED_OUT {
			return filteredOut, nil
		}
		return failed, nil
	}
	return matched, nil
}

// classify sorts an error into a filtered-out record or a refused expression.
func classify(err error) outcome {
	var e *sdk.Error
	if errors.As(err, &e) {
		if e.Matches(types.FILTERED_OUT) {
			return filteredOut
		}
		if e.Kind() == sdk.KindFilteredOut {
			return filteredOut
		}
	}
	if strings.Contains(strings.ToLower(err.Error()), "filtered") {
		return filteredOut
	}
	return failed
}

// seed writes the records the checks read.
func seed(env *exrun.Env, set *sdk.DataSet) error {
	// Record 1: integers for the shift checks.
	k1 := set.Key(1)
	if _, err := env.Session.Upsert(k1).
		SetTo("intBin", -8).SetTo("posInt", 16).Execute(); err != nil {
		return err
	}

	// Record 2: a map whose keys are integers only.
	k2 := set.Key(2)
	if err := env.Session.Put(k2, as.BinMap{"m": map[any]any{
		1: "val_from_int_key_1",
		2: "val_from_int_key_2",
		3: "val_from_int_key_3",
	}}); err != nil {
		return err
	}

	// Record 3: a map holding integer key 1 and string key "1" at once, which
	// is what makes the ambiguity observable.
	k3 := set.Key(3)
	if err := env.Session.Put(k3, as.BinMap{"m": map[any]any{
		1:      "INTEGER_KEY_1",
		"1":    "STRING_KEY_1",
		"name": "hello",
	}}); err != nil {
		return err
	}

	// Record 4: has binA and flag, and deliberately no binB.
	k4 := set.Key(4)
	if _, err := env.Session.Upsert(k4).
		SetTo("binA", 42).SetTo("flag", true).Execute(); err != nil {
		return err
	}

	// Record 7, keeping the Java numbering: one int bin and one float bin.
	k7 := set.Key(7)
	if _, err := env.Session.Upsert(k7).
		SetTo("intBin", 10).SetTo("floatBin", 3.5).Execute(); err != nil {
		return err
	}
	return nil
}
