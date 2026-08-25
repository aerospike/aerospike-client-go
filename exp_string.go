// Copyright 2014-2026 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package aerospike

// String expression generator. Produces [Expression] nodes that read or transform
// string values inside an Aerospike expression. Mirrors the operations exposed by
// the StringOperation helpers (see cdt_string.go), but composes inside expressions
// instead of being sent as standalone operate ops.
//
// Each builder takes a `src` expression that produces the string to operate on.
// Common sources:
//   - [ExpStringBin] — read a string bin
//   - [ExpStringVal] — a string literal
//   - Another StringExp expression — chains read/transform ops.
//
// Modify-style expressions (e.g. [ExpStringUpper], [ExpStringReplace]) return the
// modified string value; they do not mutate the underlying bin. To persist a
// change, write the returned value back via [ExpWriteOp] or use the
// StringOperation helpers for direct ops.
//
// Index orientation is left-to-right with codepoint addressing. Negative indexes
// count from the end of the string. Out-of-bounds indexes are clamped to the
// valid range; no error is returned.
//
// Unlike the StringOperation helpers, these builders do NOT accept a
// [CDTContext]. To apply a string expression to a value nested inside a list or
// map, compose with the List/Map expression getters (which do take CTX) to
// extract the leaf, then pass the resulting expression as `src`.
//
// String expressions require server version 8.1.3 or later.

const _stringExpMODULE = 3

//-----------------------------------------------------------------
// Read expressions
//-----------------------------------------------------------------

// ExpStringLen creates an expression that returns the number of Unicode codepoints
// in `src` as an int64. The returned value is the codepoint count, not the count
// of user-perceived characters (grapheme clusters). For UTF-8 byte length, use
// [ExpStringByteLength].
func ExpStringLen(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeINT, IntegerValue(_STR_OP_STRLEN))
}

// ExpStringSubstrFrom creates an expression that returns the substring of `src`
// from codepoint `start` to the end. Negative `start` counts from the end of
// the string.
func ExpStringSubstrFrom(start *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeSTRING, IntegerValue(_STR_OP_SUBSTR), start)
}

// ExpStringSubstr creates an expression that returns the substring of `src` in
// the half-open codepoint range `[start, end)`. Negative indexes count from
// the end.
func ExpStringSubstr(start *Expression, end *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeSTRING, IntegerValue(_STR_OP_SUBSTR), start, end)
}

// ExpStringCharAt creates an expression that returns the codepoint at `index`
// of `src` as a one-codepoint string. Negative indexes count from the end.
func ExpStringCharAt(index *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeSTRING, IntegerValue(_STR_OP_CHAR_AT), index)
}

// ExpStringFind creates an expression that returns the codepoint index of the
// first occurrence of `needle` in `src`, or -1 if not found.
func ExpStringFind(needle *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeINT, IntegerValue(_STR_OP_FIND), needle)
}

// ExpStringFindNth creates an expression that returns the codepoint index of
// the `occurrence`-th match of `needle` (1 = first, -1 = last), or -1 if not
// found.
func ExpStringFindNth(needle *Expression, occurrence *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeINT, IntegerValue(_STR_OP_FIND), needle, occurrence)
}

// ExpStringContains creates an expression that tests whether `src` contains
// `needle` as a substring. Returns a boolean.
func ExpStringContains(needle *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_CONTAINS), needle)
}

// ExpStringStartsWith creates an expression that tests whether `src` begins
// with `prefix`. Returns a boolean.
func ExpStringStartsWith(prefix *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_STARTS_WITH), prefix)
}

// ExpStringEndsWith creates an expression that tests whether `src` ends with
// `suffix`. Returns a boolean.
func ExpStringEndsWith(suffix *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_ENDS_WITH), suffix)
}

// ExpStringToInteger creates an expression that parses `src` as an int64. The
// expression returns an error if the source cannot be parsed as an integer.
func ExpStringToInteger(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeINT, IntegerValue(_STR_OP_TO_INTEGER))
}

// ExpStringToDouble creates an expression that parses `src` as a 64-bit float.
// The expression returns an error if the source cannot be parsed as a double.
func ExpStringToDouble(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeFLOAT, IntegerValue(_STR_OP_TO_DOUBLE))
}

// ExpStringByteLength creates an expression that returns the UTF-8 byte length
// of `src` as an int64. Differs from [ExpStringLen] for non-ASCII content where
// one codepoint can encode to multiple bytes.
func ExpStringByteLength(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeINT, IntegerValue(_STR_OP_BYTE_LENGTH))
}

// ExpStringIsNumeric creates an expression that tests whether `src` contains a
// valid integer or float literal. Returns a boolean.
func ExpStringIsNumeric(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_IS_NUMERIC))
}

// ExpStringIsNumericTyped creates an expression that tests whether `src` parses
// as a number of the requested [StringNumericType]. Returns a boolean.
func ExpStringIsNumericTyped(numericType StringNumericType, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_IS_NUMERIC), IntegerValue(int(numericType)))
}

// ExpStringIsUpper creates an expression that tests whether every cased
// codepoint in `src` is uppercase. Returns a boolean.
func ExpStringIsUpper(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_IS_UPPER))
}

// ExpStringIsLower creates an expression that tests whether every cased
// codepoint in `src` is lowercase. Returns a boolean.
func ExpStringIsLower(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_IS_LOWER))
}

// ExpStringToBlob creates an expression that returns the UTF-8 bytes of `src`
// as a blob.
func ExpStringToBlob(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBLOB, IntegerValue(_STR_OP_TO_BLOB))
}

// ExpStringSplit creates an expression that splits `src` by Unicode codepoint
// — each codepoint becomes its own list element.
func ExpStringSplit(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeLIST, IntegerValue(_STR_OP_SPLIT))
}

// ExpStringSplitBySeparator creates an expression that splits `src` by the
// `separator` substring. If the separator is absent, the result is a singleton
// list containing the whole source.
func ExpStringSplitBySeparator(separator *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeLIST, IntegerValue(_STR_OP_SPLIT), separator)
}

// ExpStringB64Decode creates an expression that base64-decodes `src` and
// returns the decoded bytes as a blob.
func ExpStringB64Decode(src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBLOB, IntegerValue(_STR_OP_B64_DECODE))
}

// ExpStringRegexCompare creates an expression that tests whether `pattern`
// (ICU regex syntax) matches `src`. Returns a boolean.
func ExpStringRegexCompare(pattern *Expression, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_REGEX_COMPARE), pattern)
}

// ExpStringRegexCompareWithFlags creates an expression that tests whether
// `pattern` matches `src` under the supplied [StringRegexFlags]. Flags can be
// combined with bitwise OR. Returns a boolean.
func ExpStringRegexCompareWithFlags(pattern *Expression, regexFlags StringRegexFlags, src *Expression) *Expression {
	return addStringReadExp(src, ExpTypeBOOL, IntegerValue(_STR_OP_REGEX_COMPARE), pattern, IntegerValue(int(regexFlags)))
}

//-----------------------------------------------------------------
// Modify expressions (return the modified string; do not persist)
//-----------------------------------------------------------------

// ExpStringInsert creates an expression that splices `value` into `src` at
// codepoint `index` and returns the resulting string. Negative indexes count
// from the end. Does not modify the underlying bin.
func ExpStringInsert(policy *StringPolicy, index *Expression, value *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_INSERT), index, value, IntegerValue(policy.flags))
}

// ExpStringOverwrite creates an expression that overwrites codepoints in `src`
// starting at codepoint `index` with `value`, returning the resulting string.
// Does not modify the underlying bin.
func ExpStringOverwrite(policy *StringPolicy, index *Expression, value *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_OVERWRITE), index, value, IntegerValue(policy.flags))
}

// ExpStringConcat creates an expression that concatenates `values` (a list of
// strings) onto `src` in order, returning the resulting string. Does not modify
// the underlying bin.
func ExpStringConcat(policy *StringPolicy, values *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_CONCAT), values, IntegerValue(policy.flags))
}

// ExpStringAppend creates an expression that appends `value` to the end of
// `src` and returns the resulting string. Unicode/DBCS-aware counterpart to
// the legacy byte-level append; does not modify the underlying bin.
func ExpStringAppend(policy *StringPolicy, value *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_APPEND), value, IntegerValue(policy.flags))
}

// ExpStringPrepend creates an expression that prepends `value` to the start of
// `src` and returns the resulting string. Unicode/DBCS-aware counterpart to
// the legacy byte-level prepend; does not modify the underlying bin.
func ExpStringPrepend(policy *StringPolicy, value *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_PREPEND), value, IntegerValue(policy.flags))
}

// ExpStringSnip creates an expression that removes the half-open codepoint
// range [start, end) from `src` and returns the resulting string. Does not
// modify the underlying bin.
func ExpStringSnip(policy *StringPolicy, start *Expression, end *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_SNIP), start, end, IntegerValue(policy.flags))
}

// ExpStringReplace creates an expression that replaces the first occurrence of
// `needle` in `src` with `replacement` and returns the resulting string. Does
// not modify the underlying bin.
func ExpStringReplace(policy *StringPolicy, needle *Expression, replacement *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_REPLACE),
		stringExpQuotedPair(needle, replacement), IntegerValue(policy.flags))
}

// stringExpQuotedPair builds the nested [first, second] argument that the
// replace-family string expressions require. The pair must be QUOTE-wrapped
// (opcode 126) so the server treats it as a literal list rather than a nested
// call — the same wrapping ExpListValueVal applies for concat. Emitting the
// pair without the quote is what the server rejected with PARAMETER.
func stringExpQuotedPair(first *Expression, second *Expression) *Expression {
	return newFilterExpression(
		&expOpQUOTED, ValueArray{first.val, second.val}, nil, nil, nil, nil)
}

// ExpStringReplaceAll creates an expression that replaces every occurrence of
// `needle` in `src` with `replacement` and returns the resulting string. Does
// not modify the underlying bin.
func ExpStringReplaceAll(policy *StringPolicy, needle *Expression, replacement *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_REPLACE_ALL),
		stringExpQuotedPair(needle, replacement), IntegerValue(policy.flags))
}

// ExpStringUpper creates an expression that returns `src` uppercased. Does not
// modify the underlying bin.
func ExpStringUpper(policy *StringPolicy, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_UPPER), IntegerValue(policy.flags))
}

// ExpStringLower creates an expression that returns `src` lowercased. Does not
// modify the underlying bin.
func ExpStringLower(policy *StringPolicy, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_LOWER), IntegerValue(policy.flags))
}

// ExpStringCaseFold creates an expression that returns `src` case-folded
// (locale-independent lowercase). Useful for normalized comparison keys. Does
// not modify the underlying bin.
func ExpStringCaseFold(policy *StringPolicy, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_CASE_FOLD), IntegerValue(policy.flags))
}

// ExpStringNormalizeNFC creates an expression that returns `src` normalized to
// Unicode NFC form. Already-normalized strings are unchanged. Does not modify
// the underlying bin.
func ExpStringNormalizeNFC(policy *StringPolicy, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_NORMALIZE_NFC), IntegerValue(policy.flags))
}

// ExpStringTrimStart creates an expression that returns `src` with whitespace
// removed from the start. Does not modify the underlying bin.
func ExpStringTrimStart(policy *StringPolicy, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_TRIM_START), IntegerValue(policy.flags))
}

// ExpStringTrimEnd creates an expression that returns `src` with whitespace
// removed from the end. Does not modify the underlying bin.
func ExpStringTrimEnd(policy *StringPolicy, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_TRIM_END), IntegerValue(policy.flags))
}

// ExpStringTrim creates an expression that returns `src` with whitespace
// removed from both ends. Does not modify the underlying bin.
func ExpStringTrim(policy *StringPolicy, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_TRIM), IntegerValue(policy.flags))
}

// ExpStringPadStart creates an expression that prepends `padString` to `src`
// repeatedly until the result reaches `targetLength` codepoints. No-op when
// the source is already at or above the target length. Does not modify the
// underlying bin.
func ExpStringPadStart(policy *StringPolicy, targetLength *Expression, padString *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_PAD_START), targetLength, padString, IntegerValue(policy.flags))
}

// ExpStringPadEnd creates an expression that appends `padString` to `src`
// repeatedly until the result reaches `targetLength` codepoints. No-op when
// the source is already at or above the target length. Does not modify the
// underlying bin.
func ExpStringPadEnd(policy *StringPolicy, targetLength *Expression, padString *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_PAD_END), targetLength, padString, IntegerValue(policy.flags))
}

// ExpStringRepeat creates an expression that returns `src` repeated `count`
// times. Does not modify the underlying bin.
func ExpStringRepeat(policy *StringPolicy, count *Expression, src *Expression) *Expression {
	policy = stringPolicyOrDefault(policy)
	return addStringModifyExp(src, IntegerValue(_STR_OP_REPEAT), count, IntegerValue(policy.flags))
}

// ExpStringRegexReplace creates an expression that replaces matches of `pattern`
// (ICU regex syntax) in `src` with `replacement` and returns the resulting
// string. Pass [StringRegexGlobal] to replace every match. Flag values may be
// combined with bitwise OR. Does not modify the underlying bin.
//
// The server's regex_replace op table does not accept policy write flags, so
// `policy` is kept for API symmetry with the other modify expressions and is
// ignored.
func ExpStringRegexReplace(policy *StringPolicy, pattern *Expression, replacement *Expression, regexFlags StringRegexFlags, src *Expression) *Expression {
	_ = stringPolicyOrDefault(policy)
	// The server's regex_replace op table takes [list, regexFlags] (no slot for
	// policy flags), so pass the quoted [pattern, replacement] pair + regexFlags.
	return addStringModifyExp(src, IntegerValue(_STR_OP_REGEX_REPLACE),
		stringExpQuotedPair(pattern, replacement), IntegerValue(int(regexFlags)))
}

//-----------------------------------------------------------------
// Type conversion expression
//-----------------------------------------------------------------

// ExpStringToString creates an expression that returns the string representation
// of `src`, where `src` may be any expression yielding an integer, float, string,
// or blob value. Returns an error for any other source type.
func ExpStringToString(src *Expression) *Expression {
	// Dedicated TO_STRING opcode (99), encoded as [99, bin]. The prior
	// CALL_REPR (module 4) shape was rejected by the server with PARAMETER.
	// Mirrors aerospike-client-c CLIENT-5164 (PR #228).
	return &Expression{
		cmd: &expOpTO_STRING,
		bin: src,
	}
}

//-----------------------------------------------------------------
// Internals
//-----------------------------------------------------------------

func addStringReadExp(src *Expression, retType ExpType, args ...ExpressionArgument) *Expression {
	flags := int64(_stringExpMODULE)
	return &Expression{
		cmd:       &expOpCALL,
		val:       nil,
		bin:       src,
		flags:     &flags,
		module:    &retType,
		exps:      nil,
		arguments: args,
	}
}

func addStringModifyExp(src *Expression, args ...ExpressionArgument) *Expression {
	flags := int64(_stringExpMODULE | _MODIFY)
	return &Expression{
		cmd:       &expOpCALL,
		val:       nil,
		bin:       src,
		flags:     &flags,
		module:    &ExpTypeSTRING,
		exps:      nil,
		arguments: args,
	}
}
