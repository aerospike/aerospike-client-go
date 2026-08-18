/*
 * Copyright 2026 Aerospike, Inc.
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

// Read-only string operations: return information about the bin without
// mutating it. Requires server version 8.1.3 or later.

package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

func runOperateStringRead() error {
	key, err := as.NewKey(ns, set, "opstr_read")
	if err != nil {
		return err
	}

	// strlen — codepoint count
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello world")); err != nil {
		return err
	}
	r, err := client.Operate(nil, key, as.StrLenOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`strlen("hello world") = %v`, r.Bins["text"])

	// substr(start) — codepoint slice to end of string
	r, err = client.Operate(nil, key, as.StrSubstrFromOp("text", 6))
	if err != nil {
		return err
	}
	log.Printf(`substr(6) = %q`, r.Bins["text"])

	// substr(start, length) — Go keeps the length param name; the server
	// has always interpreted this as a half-open codepoint range.
	r, err = client.Operate(nil, key, as.StrSubstrOp("text", 0, 5))
	if err != nil {
		return err
	}
	log.Printf(`substr(0, 5) = %q`, r.Bins["text"])

	// charAt — single-codepoint slice
	r, err = client.Operate(nil, key, as.StrCharAtOp("text", 6))
	if err != nil {
		return err
	}
	log.Printf(`charAt(6) = %q`, r.Bins["text"])

	// find(needle) — index of first match, -1 if absent
	r, err = client.Operate(nil, key, as.StrFindOp("text", "world"))
	if err != nil {
		return err
	}
	log.Printf(`find("world") = %v`, r.Bins["text"])

	// find(needle, occurrence) — index of nth match
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "ababab")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrFindNthOp("text", "ab", 2))
	if err != nil {
		return err
	}
	log.Printf(`find("ab", occurrence=2) on "ababab" = %v`, r.Bins["text"])

	// contains
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello world")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrContainsOp("text", "hello"))
	if err != nil {
		return err
	}
	log.Printf(`contains("hello") = %v`, r.Bins["text"])

	// startsWith
	r, err = client.Operate(nil, key, as.StrStartsWithOp("text", "hello"))
	if err != nil {
		return err
	}
	log.Printf(`startsWith("hello") = %v`, r.Bins["text"])

	// endsWith
	r, err = client.Operate(nil, key, as.StrEndsWithOp("text", "world"))
	if err != nil {
		return err
	}
	log.Printf(`endsWith("world") = %v`, r.Bins["text"])

	// toInteger — parse string as int64
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "12345")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrToIntegerOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`toInteger("12345") = %v`, r.Bins["text"])

	// toDouble — parse string as float64
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "3.14")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrToDoubleOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`toDouble("3.14") = %v`, r.Bins["text"])

	// byteLength — UTF-8 byte count (differs from strlen for non-ASCII)
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "héllo")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrByteLengthOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`byteLength("héllo") = %v (5 codepoints, 6 UTF-8 bytes)`, r.Bins["text"])

	// isNumeric — accepts integer or float
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "12345")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrIsNumericOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`isNumeric("12345") = %v`, r.Bins["text"])

	// isNumeric(numericType) — restrict by StringNumericType
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "3.14")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrIsNumericTypedOp("text", as.StringNumericInt))
	if err != nil {
		return err
	}
	log.Printf(`isNumeric("3.14", INT) = %v`, r.Bins["text"])

	// isUpper
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "HELLO")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrIsUpperOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`isUpper("HELLO") = %v`, r.Bins["text"])

	// isLower
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "hello")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrIsLowerOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`isLower("hello") = %v`, r.Bins["text"])

	// toBlob — UTF-8 bytes
	r, err = client.Operate(nil, key, as.StrToBlobOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`toBlob("hello") = %v`, r.Bins["text"])

	// split — one element per codepoint
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "abc")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrSplitOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`split() = %v`, r.Bins["text"])

	// split(separator)
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "one,two,three")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrSplitBySeparatorOp("text", ","))
	if err != nil {
		return err
	}
	log.Printf(`split(",") = %v`, r.Bins["text"])

	// b64Decode — decode base64 text to bytes
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "aGVsbG8=")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrB64DecodeOp("text"))
	if err != nil {
		return err
	}
	log.Printf(`b64Decode("aGVsbG8=") = %q`, r.Bins["text"])

	// regexCompare — ICU regex pattern match
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "Hello123World")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrRegexCompareOp("text", "[0-9]+"))
	if err != nil {
		return err
	}
	log.Printf(`regexCompare("[0-9]+") = %v`, r.Bins["text"])

	// regexCompare(flags) — case-insensitive match
	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin("text", "HELLO")); err != nil {
		return err
	}
	r, err = client.Operate(nil, key, as.StrRegexCompareWithFlagsOp("text", "hello", as.StringRegexCaseInsensitive))
	if err != nil {
		return err
	}
	log.Printf(`regexCompare("hello", CASE_INSENSITIVE) = %v`, r.Bins["text"])

	return nil
}
