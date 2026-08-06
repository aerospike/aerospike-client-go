/*
 * Copyright 2026 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

// Ported from the Java client's examples/OperateString.java. Exercises every
// StrXxxOp helper grouped as read-only ops, in-place modify ops, and toString.
// Requires server version 8.1.3 or later.

package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
	shared "github.com/aerospike/aerospike-client-go/v8/examples/shared"
)

const bin = "text"

func main() {
	runReadOps(shared.Client)
	runModifyOps(shared.Client)
	runToString(shared.Client)

	log.Println("Example finished successfully.")
}

// Read-only string operations: return information about the bin without
// mutating it.
func runReadOps(client *as.Client) {
	key, err := as.NewKey(*shared.Namespace, *shared.Set, "opstr_read")
	shared.PanicOnError(err)

	// strlen — codepoint count
	put(client, key, "hello world")
	r := operate(client, key, as.StrLenOp(bin))
	log.Printf(`strlen("hello world") = %v`, r.Bins[bin])

	// substr(start) — codepoint slice to end of string
	r = operate(client, key, as.StrSubstrFromOp(bin, 6))
	log.Printf(`substr(6) = %q`, r.Bins[bin])

	// substr(start, length) — Go keeps the length param name; the server
	// has always interpreted this as a half-open codepoint range.
	r = operate(client, key, as.StrSubstrOp(bin, 0, 5))
	log.Printf(`substr(0, 5) = %q`, r.Bins[bin])

	// charAt — single-codepoint slice
	r = operate(client, key, as.StrCharAtOp(bin, 6))
	log.Printf(`charAt(6) = %q`, r.Bins[bin])

	// find(needle) — index of first match, -1 if absent
	r = operate(client, key, as.StrFindOp(bin, "world"))
	log.Printf(`find("world") = %v`, r.Bins[bin])

	// find(needle, occurrence) — index of nth match
	put(client, key, "ababab")
	r = operate(client, key, as.StrFindNthOp(bin, "ab", 2))
	log.Printf(`find("ab", occurrence=2) on "ababab" = %v`, r.Bins[bin])

	// contains
	put(client, key, "hello world")
	r = operate(client, key, as.StrContainsOp(bin, "hello"))
	log.Printf(`contains("hello") = %v`, r.Bins[bin])

	// startsWith
	r = operate(client, key, as.StrStartsWithOp(bin, "hello"))
	log.Printf(`startsWith("hello") = %v`, r.Bins[bin])

	// endsWith
	r = operate(client, key, as.StrEndsWithOp(bin, "world"))
	log.Printf(`endsWith("world") = %v`, r.Bins[bin])

	// toInteger — parse string as int64
	put(client, key, "12345")
	r = operate(client, key, as.StrToIntegerOp(bin))
	log.Printf(`toInteger("12345") = %v`, r.Bins[bin])

	// toDouble — parse string as float64
	put(client, key, "3.14")
	r = operate(client, key, as.StrToDoubleOp(bin))
	log.Printf(`toDouble("3.14") = %v`, r.Bins[bin])

	// byteLength — UTF-8 byte count (differs from strlen for non-ASCII)
	put(client, key, "héllo")
	r = operate(client, key, as.StrByteLengthOp(bin))
	log.Printf(`byteLength("héllo") = %v (5 codepoints, 6 UTF-8 bytes)`, r.Bins[bin])

	// isNumeric — accepts integer or float
	put(client, key, "12345")
	r = operate(client, key, as.StrIsNumericOp(bin))
	log.Printf(`isNumeric("12345") = %v`, r.Bins[bin])

	// isNumeric(numericType) — restrict by StringNumericType
	put(client, key, "3.14")
	r = operate(client, key, as.StrIsNumericTypedOp(bin, as.StringNumericInt))
	log.Printf(`isNumeric("3.14", INT) = %v`, r.Bins[bin])

	// isUpper
	put(client, key, "HELLO")
	r = operate(client, key, as.StrIsUpperOp(bin))
	log.Printf(`isUpper("HELLO") = %v`, r.Bins[bin])

	// isLower
	put(client, key, "hello")
	r = operate(client, key, as.StrIsLowerOp(bin))
	log.Printf(`isLower("hello") = %v`, r.Bins[bin])

	// toBlob — UTF-8 bytes
	r = operate(client, key, as.StrToBlobOp(bin))
	log.Printf(`toBlob("hello") = %v`, r.Bins[bin])

	// split — one element per codepoint
	put(client, key, "abc")
	r = operate(client, key, as.StrSplitOp(bin))
	log.Printf(`split() = %v`, r.Bins[bin])

	// split(separator)
	put(client, key, "one,two,three")
	r = operate(client, key, as.StrSplitBySeparatorOp(bin, ","))
	log.Printf(`split(",") = %v`, r.Bins[bin])

	// b64Decode — decode base64 text to bytes
	put(client, key, "aGVsbG8=")
	r = operate(client, key, as.StrB64DecodeOp(bin))
	log.Printf(`b64Decode("aGVsbG8=") = %q`, r.Bins[bin])

	// regexCompare — ICU regex pattern match
	put(client, key, "Hello123World")
	r = operate(client, key, as.StrRegexCompareOp(bin, "[0-9]+"))
	log.Printf(`regexCompare("[0-9]+") = %v`, r.Bins[bin])

	// regexCompare(flags) — case-insensitive match
	put(client, key, "HELLO")
	r = operate(client, key, as.StrRegexCompareWithFlagsOp(bin, "hello", as.StringRegexCaseInsensitive))
	log.Printf(`regexCompare("hello", CASE_INSENSITIVE) = %v`, r.Bins[bin])
}

// Modify operations: mutate the bin in place. Each call below performs the
// modify op then re-reads the bin to display the new value.
func runModifyOps(client *as.Client) {
	key, err := as.NewKey(*shared.Namespace, *shared.Set, "opstr_modify")
	shared.PanicOnError(err)

	policy := as.DefaultStringPolicy

	// insert — splice value at codepoint index
	put(client, key, "hello world")
	modifyAndShow(client, key, `insert(5, " beautiful")`,
		as.StrInsertOp(policy, bin, 5, " beautiful"))

	// overwrite — replace codepoints starting at index
	put(client, key, "hello world")
	modifyAndShow(client, key, `overwrite(6, "earth")`,
		as.StrOverwriteOp(policy, bin, 6, "earth"))

	// concat(value) — append a single string
	put(client, key, "hello")
	modifyAndShow(client, key, `concat("!")`,
		as.StrConcatOp(policy, bin, "!"))

	// concat(values) — append each list element in order
	put(client, key, "hello")
	modifyAndShow(client, key, `concat([" ", "big", " world"])`,
		as.StrConcatListOp(policy, bin, []string{" ", "big", " world"}))

	// append — Unicode-aware end-append (alongside the legacy AppendOp)
	put(client, key, "hello")
	modifyAndShow(client, key, `append("!")`,
		as.StrAppendOp(policy, bin, "!"))

	// prepend — Unicode-aware front-insert (alongside the legacy PrependOp)
	put(client, key, "world")
	modifyAndShow(client, key, `prepend("hello ")`,
		as.StrPrependOp(policy, bin, "hello "))

	// snip — remove half-open codepoint range
	put(client, key, "hello beautiful world")
	modifyAndShow(client, key, "snip(5, 15)",
		as.StrSnipOp(policy, bin, 5, 15))

	// replace — first occurrence only
	put(client, key, "hello world world")
	modifyAndShow(client, key, `replace("world", "earth")`,
		as.StrReplaceOp(policy, bin, "world", "earth"))

	// replaceAll — every occurrence
	put(client, key, "aabaa")
	modifyAndShow(client, key, `replaceAll("a", "x")`,
		as.StrReplaceAllOp(policy, bin, "a", "x"))

	// upper
	put(client, key, "hello world")
	modifyAndShow(client, key, "upper()",
		as.StrUpperOp(policy, bin))

	// lower
	put(client, key, "HELLO WORLD")
	modifyAndShow(client, key, "lower()",
		as.StrLowerOp(policy, bin))

	// caseFold — locale-independent fold for comparison keys
	put(client, key, "HELLO World")
	modifyAndShow(client, key, "caseFold()",
		as.StrCaseFoldOp(policy, bin))

	// normalizeNFC — Unicode NFC normalization
	put(client, key, "café")
	modifyAndShow(client, key, "normalizeNFC()",
		as.StrNormalizeNFCOp(policy, bin))

	// trimStart — drop leading whitespace
	put(client, key, "  hello  ")
	modifyAndShow(client, key, "trimStart()",
		as.StrTrimStartOp(policy, bin))

	// trimEnd — drop trailing whitespace
	put(client, key, "  hello  ")
	modifyAndShow(client, key, "trimEnd()",
		as.StrTrimEndOp(policy, bin))

	// trim — drop both ends
	put(client, key, "  hello world  ")
	modifyAndShow(client, key, "trim()",
		as.StrTrimOp(policy, bin))

	// padStart — left-pad up to target codepoint length
	put(client, key, "hello")
	modifyAndShow(client, key, `padStart(10, "*")`,
		as.StrPadStartOp(policy, bin, 10, "*"))

	// padEnd — right-pad up to target codepoint length
	put(client, key, "hello")
	modifyAndShow(client, key, `padEnd(10, ".")`,
		as.StrPadEndOp(policy, bin, 10, "."))

	// repeat — repeat string n times
	put(client, key, "ab")
	modifyAndShow(client, key, "repeat(3)",
		as.StrRepeatOp(policy, bin, 3))

	// regexReplace — pass GLOBAL to replace every match (default replaces first only)
	put(client, key, "abc123def456")
	modifyAndShow(client, key, `regexReplace("[0-9]+", "NUM", GLOBAL)`,
		as.StrRegexReplaceOp(policy, bin, "[0-9]+", "NUM", as.StringRegexGlobal))
}

// toString — convert any int / float / string / blob bin to its string
// representation. Unlike the other ops, this does not accept a CTX.
func runToString(client *as.Client) {
	key, err := as.NewKey(*shared.Namespace, *shared.Set, "opstr_tostring")
	shared.PanicOnError(err)

	const numBin = "n"

	if _, err := client.Delete(shared.WritePolicy, key); err != nil {
		shared.PanicOnError(err)
	}
	if err := client.PutBins(shared.WritePolicy, key, as.NewBin(numBin, 42)); err != nil {
		shared.PanicOnError(err)
	}

	r, err := client.Operate(shared.WritePolicy, key, as.StrToStringOp(numBin))
	shared.PanicOnError(err)
	log.Printf(`toString(int 42) = %q`, r.Bins[numBin])
}

func put(client *as.Client, key *as.Key, value string) {
	if _, err := client.Delete(shared.WritePolicy, key); err != nil {
		shared.PanicOnError(err)
	}
	if err := client.PutBins(shared.WritePolicy, key, as.NewBin(bin, value)); err != nil {
		shared.PanicOnError(err)
	}
}

func operate(client *as.Client, key *as.Key, op *as.Operation) *as.Record {
	r, err := client.Operate(shared.WritePolicy, key, op)
	shared.PanicOnError(err)
	return r
}

func modifyAndShow(client *as.Client, key *as.Key, label string, modifyOp *as.Operation) {
	if _, err := client.Operate(shared.WritePolicy, key, modifyOp); err != nil {
		shared.PanicOnError(err)
	}
	r, err := client.Get(shared.Policy, key)
	shared.PanicOnError(err)
	log.Printf("%s -> %q", label, r.Bins[bin])
}
