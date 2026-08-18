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
)

const strBin = "text"

// runOperateString exercises every server-side string operation, grouped as
// read-only ops, in-place modify ops, and toString.
func runOperateString() error {
	if err := runStringReadOps(); err != nil {
		return err
	}
	if err := runStringModifyOps(); err != nil {
		return err
	}
	return runStringToString()
}

// Read-only string operations: return information about the bin without
// mutating it.
func runStringReadOps() error {
	key, err := as.NewKey(ns, set, "opstr_read")
	if err != nil {
		return err
	}

	// strlen — codepoint count
	if err := strPut(key, "hello world"); err != nil {
		return err
	}
	r, err := strOperate(key, as.StrLenOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`strlen("hello world") = %v`, r.Bins[strBin])

	// substr(start) — codepoint slice to end of string
	r, err = strOperate(key, as.StrSubstrFromOp(strBin, 6))
	if err != nil {
		return err
	}
	log.Printf(`substr(6) = %q`, r.Bins[strBin])

	// substr(start, length) — Go keeps the length param name; the server
	// has always interpreted this as a half-open codepoint range.
	r, err = strOperate(key, as.StrSubstrOp(strBin, 0, 5))
	if err != nil {
		return err
	}
	log.Printf(`substr(0, 5) = %q`, r.Bins[strBin])

	// charAt — single-codepoint slice
	r, err = strOperate(key, as.StrCharAtOp(strBin, 6))
	if err != nil {
		return err
	}
	log.Printf(`charAt(6) = %q`, r.Bins[strBin])

	// find(needle) — index of first match, -1 if absent
	r, err = strOperate(key, as.StrFindOp(strBin, "world"))
	if err != nil {
		return err
	}
	log.Printf(`find("world") = %v`, r.Bins[strBin])

	// find(needle, occurrence) — index of nth match
	if err := strPut(key, "ababab"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrFindNthOp(strBin, "ab", 2))
	if err != nil {
		return err
	}
	log.Printf(`find("ab", occurrence=2) on "ababab" = %v`, r.Bins[strBin])

	// contains
	if err := strPut(key, "hello world"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrContainsOp(strBin, "hello"))
	if err != nil {
		return err
	}
	log.Printf(`contains("hello") = %v`, r.Bins[strBin])

	// startsWith
	r, err = strOperate(key, as.StrStartsWithOp(strBin, "hello"))
	if err != nil {
		return err
	}
	log.Printf(`startsWith("hello") = %v`, r.Bins[strBin])

	// endsWith
	r, err = strOperate(key, as.StrEndsWithOp(strBin, "world"))
	if err != nil {
		return err
	}
	log.Printf(`endsWith("world") = %v`, r.Bins[strBin])

	// toInteger — parse string as int64
	if err := strPut(key, "12345"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrToIntegerOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`toInteger("12345") = %v`, r.Bins[strBin])

	// toDouble — parse string as float64
	if err := strPut(key, "3.14"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrToDoubleOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`toDouble("3.14") = %v`, r.Bins[strBin])

	// byteLength — UTF-8 byte count (differs from strlen for non-ASCII)
	if err := strPut(key, "héllo"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrByteLengthOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`byteLength("héllo") = %v (5 codepoints, 6 UTF-8 bytes)`, r.Bins[strBin])

	// isNumeric — accepts integer or float
	if err := strPut(key, "12345"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrIsNumericOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`isNumeric("12345") = %v`, r.Bins[strBin])

	// isNumeric(numericType) — restrict by StringNumericType
	if err := strPut(key, "3.14"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrIsNumericTypedOp(strBin, as.StringNumericInt))
	if err != nil {
		return err
	}
	log.Printf(`isNumeric("3.14", INT) = %v`, r.Bins[strBin])

	// isUpper
	if err := strPut(key, "HELLO"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrIsUpperOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`isUpper("HELLO") = %v`, r.Bins[strBin])

	// isLower
	if err := strPut(key, "hello"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrIsLowerOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`isLower("hello") = %v`, r.Bins[strBin])

	// toBlob — UTF-8 bytes
	r, err = strOperate(key, as.StrToBlobOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`toBlob("hello") = %v`, r.Bins[strBin])

	// split — one element per codepoint
	if err := strPut(key, "abc"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrSplitOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`split() = %v`, r.Bins[strBin])

	// split(separator)
	if err := strPut(key, "one,two,three"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrSplitBySeparatorOp(strBin, ","))
	if err != nil {
		return err
	}
	log.Printf(`split(",") = %v`, r.Bins[strBin])

	// b64Decode — decode base64 text to bytes
	if err := strPut(key, "aGVsbG8="); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrB64DecodeOp(strBin))
	if err != nil {
		return err
	}
	log.Printf(`b64Decode("aGVsbG8=") = %q`, r.Bins[strBin])

	// regexCompare — ICU regex pattern match
	if err := strPut(key, "Hello123World"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrRegexCompareOp(strBin, "[0-9]+"))
	if err != nil {
		return err
	}
	log.Printf(`regexCompare("[0-9]+") = %v`, r.Bins[strBin])

	// regexCompare(flags) — case-insensitive match
	if err := strPut(key, "HELLO"); err != nil {
		return err
	}
	r, err = strOperate(key, as.StrRegexCompareWithFlagsOp(strBin, "hello", as.StringRegexCaseInsensitive))
	if err != nil {
		return err
	}
	log.Printf(`regexCompare("hello", CASE_INSENSITIVE) = %v`, r.Bins[strBin])
	return nil
}

// Modify operations: mutate the bin in place. Each call below performs the
// modify op then re-reads the bin to display the new value.
func runStringModifyOps() error {
	key, err := as.NewKey(ns, set, "opstr_modify")
	if err != nil {
		return err
	}

	policy := as.DefaultStringPolicy

	// insert — splice value at codepoint index
	if err := strPut(key, "hello world"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `insert(5, " beautiful")`,
		as.StrInsertOp(policy, strBin, 5, " beautiful")); err != nil {
		return err
	}

	// overwrite — replace codepoints starting at index
	if err := strPut(key, "hello world"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `overwrite(6, "earth")`,
		as.StrOverwriteOp(policy, strBin, 6, "earth")); err != nil {
		return err
	}

	// concat(value) — append a single string
	if err := strPut(key, "hello"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `concat("!")`,
		as.StrConcatOp(policy, strBin, "!")); err != nil {
		return err
	}

	// concat(values) — append each list element in order
	if err := strPut(key, "hello"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `concat([" ", "big", " world"])`,
		as.StrConcatListOp(policy, strBin, []string{" ", "big", " world"})); err != nil {
		return err
	}

	// append — Unicode-aware end-append (alongside the legacy AppendOp)
	if err := strPut(key, "hello"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `append("!")`,
		as.StrAppendOp(policy, strBin, "!")); err != nil {
		return err
	}

	// prepend — Unicode-aware front-insert (alongside the legacy PrependOp)
	if err := strPut(key, "world"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `prepend("hello ")`,
		as.StrPrependOp(policy, strBin, "hello ")); err != nil {
		return err
	}

	// snip — remove half-open codepoint range
	if err := strPut(key, "hello beautiful world"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "snip(5, 15)",
		as.StrSnipOp(policy, strBin, 5, 15)); err != nil {
		return err
	}

	// replace — first occurrence only
	if err := strPut(key, "hello world world"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `replace("world", "earth")`,
		as.StrReplaceOp(policy, strBin, "world", "earth")); err != nil {
		return err
	}

	// replaceAll — every occurrence
	if err := strPut(key, "aabaa"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `replaceAll("a", "x")`,
		as.StrReplaceAllOp(policy, strBin, "a", "x")); err != nil {
		return err
	}

	// upper
	if err := strPut(key, "hello world"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "upper()",
		as.StrUpperOp(policy, strBin)); err != nil {
		return err
	}

	// lower
	if err := strPut(key, "HELLO WORLD"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "lower()",
		as.StrLowerOp(policy, strBin)); err != nil {
		return err
	}

	// caseFold — locale-independent fold for comparison keys
	if err := strPut(key, "HELLO World"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "caseFold()",
		as.StrCaseFoldOp(policy, strBin)); err != nil {
		return err
	}

	// normalizeNFC — Unicode NFC normalization
	if err := strPut(key, "café"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "normalizeNFC()",
		as.StrNormalizeNFCOp(policy, strBin)); err != nil {
		return err
	}

	// trimStart — drop leading whitespace
	if err := strPut(key, "  hello  "); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "trimStart()",
		as.StrTrimStartOp(policy, strBin)); err != nil {
		return err
	}

	// trimEnd — drop trailing whitespace
	if err := strPut(key, "  hello  "); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "trimEnd()",
		as.StrTrimEndOp(policy, strBin)); err != nil {
		return err
	}

	// trim — drop both ends
	if err := strPut(key, "  hello world  "); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "trim()",
		as.StrTrimOp(policy, strBin)); err != nil {
		return err
	}

	// padStart — left-pad up to target codepoint length
	if err := strPut(key, "hello"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `padStart(10, "*")`,
		as.StrPadStartOp(policy, strBin, 10, "*")); err != nil {
		return err
	}

	// padEnd — right-pad up to target codepoint length
	if err := strPut(key, "hello"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, `padEnd(10, ".")`,
		as.StrPadEndOp(policy, strBin, 10, ".")); err != nil {
		return err
	}

	// repeat — repeat string n times
	if err := strPut(key, "ab"); err != nil {
		return err
	}
	if err := strModifyAndShow(key, "repeat(3)",
		as.StrRepeatOp(policy, strBin, 3)); err != nil {
		return err
	}

	// regexReplace — pass GLOBAL to replace every match (default replaces first only)
	if err := strPut(key, "abc123def456"); err != nil {
		return err
	}
	return strModifyAndShow(key, `regexReplace("[0-9]+", "NUM", GLOBAL)`,
		as.StrRegexReplaceOp(policy, strBin, "[0-9]+", "NUM", as.StringRegexGlobal))
}

// toString — convert any int / float / string / blob bin to its string
// representation. Unlike the other ops, this does not accept a CTX.
func runStringToString() error {
	key, err := as.NewKey(ns, set, "opstr_tostring")
	if err != nil {
		return err
	}

	const numBin = "n"

	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin(numBin, 42)); err != nil {
		return err
	}

	r, err := client.Operate(nil, key, as.StrToStringOp(numBin))
	if err != nil {
		return err
	}
	log.Printf(`toString(int 42) = %q`, r.Bins[numBin])
	return nil
}

func strPut(key *as.Key, value string) as.Error {
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	return client.PutBins(nil, key, as.NewBin(strBin, value))
}

func strOperate(key *as.Key, op *as.Operation) (*as.Record, as.Error) {
	return client.Operate(nil, key, op)
}

func strModifyAndShow(key *as.Key, label string, modifyOp *as.Operation) as.Error {
	if _, err := client.Operate(nil, key, modifyOp); err != nil {
		return err
	}
	r, err := client.Get(nil, key)
	if err != nil {
		return err
	}
	log.Printf("%s -> %q", label, r.Bins[strBin])
	return nil
}
