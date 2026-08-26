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

import (
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

// String operations. Create operations to be passed to the client Operate command
// for inspecting and modifying string bins.
//
// Index orientation is left-to-right with codepoint addressing. Negative indexes
// count from the end of the string (-1 = last codepoint). Out-of-bounds indexes
// are clamped to the valid range; no error is returned.
//
// String operations require server version 8.1.3 or later. A non-empty CDTContext
// argument navigates into a string nested inside a list or map bin; with no context
// the operation targets the bin itself. The context-navigated leaf must already
// be an Aerospike string — operations on non-string leaves return
// AEROSPIKE_ERR_INCOMPATIBLE_TYPE.

// Read op codes
const (
	_STR_OP_STRLEN        = 0
	_STR_OP_SUBSTR        = 1
	_STR_OP_CHAR_AT       = 2
	_STR_OP_FIND          = 3
	_STR_OP_CONTAINS      = 4
	_STR_OP_STARTS_WITH   = 5
	_STR_OP_ENDS_WITH     = 6
	_STR_OP_TO_INTEGER    = 7
	_STR_OP_TO_DOUBLE     = 8
	_STR_OP_BYTE_LENGTH   = 9
	_STR_OP_IS_NUMERIC    = 10
	_STR_OP_IS_UPPER      = 11
	_STR_OP_IS_LOWER      = 12
	_STR_OP_TO_BLOB       = 13
	_STR_OP_SPLIT         = 14
	_STR_OP_B64_DECODE    = 15
	_STR_OP_REGEX_COMPARE = 16
)

// Modify op codes
const (
	_STR_OP_INSERT        = 50
	_STR_OP_OVERWRITE     = 51
	_STR_OP_CONCAT        = 52
	_STR_OP_SNIP          = 53
	_STR_OP_REPLACE       = 54
	_STR_OP_REPLACE_ALL   = 55
	_STR_OP_UPPER         = 56
	_STR_OP_LOWER         = 57
	_STR_OP_CASE_FOLD     = 58
	_STR_OP_NORMALIZE_NFC = 59
	_STR_OP_TRIM_START    = 60
	_STR_OP_TRIM_END      = 61
	_STR_OP_TRIM          = 62
	_STR_OP_PAD_START     = 63
	_STR_OP_PAD_END       = 64
	_STR_OP_REPEAT        = 65
	_STR_OP_REGEX_REPLACE = 66
	_STR_OP_APPEND        = 67
	_STR_OP_PREPEND       = 68
)

// StringNumericType is the numeric type filter used by [StrIsNumericTypedOp]
// and [ExpStringIsNumericTyped]. Combine with the IS_NUMERIC sub-op to restrict
// validation to integers or floats.
type StringNumericType int

const (
	// StringNumericAny matches either an integer or a floating-point number.
	StringNumericAny StringNumericType = 0

	// StringNumericInt matches only integers.
	StringNumericInt StringNumericType = 1

	// StringNumericFloat matches only floating-point numbers.
	StringNumericFloat StringNumericType = 2
)

// StringRegexFlags configure the regex behavior of [StrRegexCompareWithFlagsOp],
// [StrRegexReplaceOp], [ExpStringRegexCompareWithFlags] and [ExpStringRegexReplace].
// Combine values with bitwise OR.
type StringRegexFlags int

const (
	// StringRegexDefault selects the default ICU regex flags (no options).
	StringRegexDefault StringRegexFlags = 0

	// StringRegexCaseInsensitive performs case insensitive matching.
	StringRegexCaseInsensitive StringRegexFlags = 1 << 0

	// StringRegexMultiline treats the input as a multi-line string. `^` and `$`
	// match the start and end of any line, not just the start and end of the input.
	StringRegexMultiline StringRegexFlags = 1 << 1

	// StringRegexDotAll makes the `.` metacharacter match any character including
	// line terminators.
	StringRegexDotAll StringRegexFlags = 1 << 2

	// StringRegexUnixLines treats only `\n` as a line terminator (Unix-style line endings).
	StringRegexUnixLines StringRegexFlags = 1 << 3

	// StringRegexGlobal replaces all matches in the input. Only applicable to
	// regex replace operations.
	StringRegexGlobal StringRegexFlags = 1 << 4
)

// StringWriteFlags configures the write semantics of string modify operations.
// Combine values with bitwise OR.
type StringWriteFlags int

const (
	// StringWriteDefault allows create or update.
	StringWriteDefault StringWriteFlags = 0

	// StringWriteNoFail suppresses the error if the operation cannot be applied
	// to the bin (e.g. the bin is missing). The bin is left unchanged and a nil
	// result is returned for that operation.
	StringWriteNoFail StringWriteFlags = 4
)

// StringPolicy is a per-operation policy carrying [StringWriteFlags]. It is
// passed inline to each modify [StringOperation] and is not part of the
// client's dynamic configuration. Mirrors how BitPolicy and HLLPolicy are scoped.
type StringPolicy struct {
	flags int
}

// DefaultStringPolicy is the default per-operation policy. It uses
// [StringWriteDefault] flags.
var DefaultStringPolicy = &StringPolicy{flags: int(StringWriteDefault)}

// NewStringPolicy returns a new [StringPolicy] with the supplied flags.
// Use bitwise OR of [StringWriteFlags] constants to combine flags.
func NewStringPolicy(flags StringWriteFlags) *StringPolicy {
	return &StringPolicy{flags: int(flags)}
}

//-----------------------------------------------------------------
// Read operations
//-----------------------------------------------------------------

// StrLenOp creates a string `strlen` operation. The server returns the number
// of Unicode codepoints in the string bin as an int64. This is the codepoint
// count — not the grapheme cluster count and not the UTF-8 byte length.
// Use [StrByteLengthOp] for the byte length.
func StrLenOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_STRLEN, binName, ctx)
}

// StrSubstrFromOp creates a string `substr` operation that reads from `start`
// to the end of the string. Negative `start` counts from the end.
func StrSubstrFromOp(binName string, start int, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_SUBSTR, binName, ctx, IntegerValue(start))
}

// StrSubstrOp creates a string `substr` operation that reads codepoints in the
// half-open range `[start, end)`. Negative indexes count from the end. `end`
// is clamped to the string length.
func StrSubstrOp(binName string, start int, end int, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_SUBSTR, binName, ctx, IntegerValue(start), IntegerValue(end))
}

// StrCharAtOp creates a string `charAt` operation. The server returns the
// codepoint at `index` as a one-codepoint string. Negative indexes count from
// the end.
func StrCharAtOp(binName string, index int, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_CHAR_AT, binName, ctx, IntegerValue(index))
}

// StrFindOp creates a string `find` operation. The server returns the codepoint
// index of the first occurrence of `needle`, or -1 if not found.
func StrFindOp(binName string, needle string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_FIND, binName, ctx, StringValue(needle))
}

// StrFindNthOp creates a string `find` operation that locates a specific
// `occurrence` of `needle` (1 = first match, -1 = last match). The server
// returns the codepoint index of that match, or -1 if not found.
func StrFindNthOp(binName string, needle string, occurrence int, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_FIND, binName, ctx, StringValue(needle), IntegerValue(occurrence))
}

// StrContainsOp creates a string `contains` operation. The server returns true
// if the bin contains `needle` as a substring, false otherwise.
func StrContainsOp(binName string, needle string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_CONTAINS, binName, ctx, StringValue(needle))
}

// StrStartsWithOp creates a string `startsWith` operation. The server returns
// true if the bin begins with `prefix`, false otherwise.
func StrStartsWithOp(binName string, prefix string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_STARTS_WITH, binName, ctx, StringValue(prefix))
}

// StrEndsWithOp creates a string `endsWith` operation. The server returns true
// if the bin ends with `suffix`, false otherwise.
func StrEndsWithOp(binName string, suffix string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_ENDS_WITH, binName, ctx, StringValue(suffix))
}

// StrToIntegerOp creates a string `toInteger` operation. The server parses the
// string as an int64. Returns AEROSPIKE_ERR_PARAMETER if the bin cannot be
// parsed as an integer.
func StrToIntegerOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_TO_INTEGER, binName, ctx)
}

// StrToDoubleOp creates a string `toDouble` operation. The server parses the
// string as a 64-bit float. Returns AEROSPIKE_ERR_PARAMETER if the bin cannot
// be parsed as a double.
func StrToDoubleOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_TO_DOUBLE, binName, ctx)
}

// StrByteLengthOp creates a string `byteLength` operation. The server returns
// the number of UTF-8 bytes in the string (int64). Differs from [StrLenOp] for
// non-ASCII content where one codepoint can encode to multiple bytes.
func StrByteLengthOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_BYTE_LENGTH, binName, ctx)
}

// StrIsNumericOp creates a string `isNumeric` operation. The server returns
// true if the bin contains a valid integer or float, false otherwise.
func StrIsNumericOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_IS_NUMERIC, binName, ctx)
}

// StrIsNumericTypedOp creates a string `isNumeric` operation that filters by
// `numericType` (see [StringNumericType]). For example, restrict to integer-only
// or float-only validation.
func StrIsNumericTypedOp(binName string, numericType StringNumericType, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_IS_NUMERIC, binName, ctx, IntegerValue(int(numericType)))
}

// StrIsUpperOp creates a string `isUpper` operation. The server returns true
// if every cased codepoint in the bin is uppercase, false otherwise.
func StrIsUpperOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_IS_UPPER, binName, ctx)
}

// StrIsLowerOp creates a string `isLower` operation. The server returns true
// if every cased codepoint in the bin is lowercase, false otherwise.
func StrIsLowerOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_IS_LOWER, binName, ctx)
}

// StrToBlobOp creates a string `toBlob` operation. The server returns the
// UTF-8 bytes of the string as a blob ([]byte).
func StrToBlobOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_TO_BLOB, binName, ctx)
}

// StrSplitOp creates a string `split` operation that splits by Unicode codepoint
// — each codepoint becomes its own element of the returned list.
func StrSplitOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_SPLIT, binName, ctx)
}

// StrSplitBySeparatorOp creates a string `split` operation that splits the bin
// by the `separator` substring. If the separator is absent the result is a
// singleton list containing the whole string.
func StrSplitBySeparatorOp(binName string, separator string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_SPLIT, binName, ctx, StringValue(separator))
}

// StrB64DecodeOp creates a string `b64Decode` operation. The server treats the
// bin as base64-encoded text and returns the decoded bytes as a blob.
func StrB64DecodeOp(binName string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_B64_DECODE, binName, ctx)
}

// StrRegexCompareOp creates a string `regexCompare` operation. The server
// matches `pattern` (ICU regex syntax) against the bin and returns true on
// match, false otherwise.
func StrRegexCompareOp(binName string, pattern string, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_REGEX_COMPARE, binName, ctx, StringValue(pattern))
}

// StrRegexCompareWithFlagsOp creates a string `regexCompare` operation that
// honors [StringRegexFlags] (e.g. [StringRegexCaseInsensitive]). Flag values
// may be combined with bitwise OR.
func StrRegexCompareWithFlagsOp(binName string, pattern string, regexFlags StringRegexFlags, ctx ...*CDTContext) *Operation {
	return newStringReadOp(_STR_OP_REGEX_COMPARE, binName, ctx, StringValue(pattern), IntegerValue(int(regexFlags)))
}

//-----------------------------------------------------------------
// Modify operations
//-----------------------------------------------------------------

// StrInsertOp creates a string `insert` operation that splices `value` into the
// bin at codepoint `index`. Negative indexes count from the end of the string.
func StrInsertOp(policy *StringPolicy, binName string, index int, value string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_INSERT, binName, ctx, IntegerValue(index), StringValue(value), IntegerValue(policy.flags))
}

// StrOverwriteOp creates a string `overwrite` operation that overwrites
// codepoints starting at codepoint `index` with `value`. The result may grow
// beyond the original length when `value` extends past the end.
func StrOverwriteOp(policy *StringPolicy, binName string, index int, value string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_OVERWRITE, binName, ctx, IntegerValue(index), StringValue(value), IntegerValue(policy.flags))
}

// StrConcatOp creates a string `concat` operation that appends `value` to the bin.
func StrConcatOp(policy *StringPolicy, binName string, value string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_CONCAT, binName, ctx, ListValue{StringValue(value)}, IntegerValue(policy.flags))
}

// StrConcatListOp creates a string `concat` operation that appends each element
// of `values` to the bin in order.
func StrConcatListOp(policy *StringPolicy, binName string, values []string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	list := make(ListValue, len(values))
	for i, s := range values {
		list[i] = StringValue(s)
	}
	return newStringModifyOp(_STR_OP_CONCAT, binName, ctx, list, IntegerValue(policy.flags))
}

// StrAppendOp creates a string `append` operation that appends `value` to the
// end of the bin. Unlike the legacy byte-level [AppendOp], this operation is
// Unicode/DBCS-aware and shares the consistent [StringPolicy] / CTX interface
// of the rest of the string package.
func StrAppendOp(policy *StringPolicy, binName string, value string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_APPEND, binName, ctx, StringValue(value), IntegerValue(policy.flags))
}

// StrPrependOp creates a string `prepend` operation that prepends `value` to
// the start of the bin. Unlike the legacy byte-level [PrependOp], this
// operation is Unicode/DBCS-aware and shares the consistent [StringPolicy] /
// CTX interface of the rest of the string package.
func StrPrependOp(policy *StringPolicy, binName string, value string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_PREPEND, binName, ctx, StringValue(value), IntegerValue(policy.flags))
}

// StrSnipOp creates a string `snip` operation that removes the half-open
// codepoint range [start, end) from the bin.
func StrSnipOp(policy *StringPolicy, binName string, start int, end int, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_SNIP, binName, ctx, IntegerValue(start), IntegerValue(end), IntegerValue(policy.flags))
}

// StrReplaceOp creates a string `replace` operation that replaces the first
// occurrence of `needle` with `replacement`.
func StrReplaceOp(policy *StringPolicy, binName string, needle string, replacement string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_REPLACE, binName, ctx, ListValue{StringValue(needle), StringValue(replacement)}, IntegerValue(policy.flags))
}

// StrReplaceAllOp creates a string `replaceAll` operation that replaces every
// occurrence of `needle` with `replacement`.
func StrReplaceAllOp(policy *StringPolicy, binName string, needle string, replacement string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_REPLACE_ALL, binName, ctx, ListValue{StringValue(needle), StringValue(replacement)}, IntegerValue(policy.flags))
}

// StrUpperOp creates a string `upper` operation that uppercases the bin in place.
func StrUpperOp(policy *StringPolicy, binName string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_UPPER, binName, ctx, IntegerValue(policy.flags))
}

// StrLowerOp creates a string `lower` operation that lowercases the bin in place.
func StrLowerOp(policy *StringPolicy, binName string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_LOWER, binName, ctx, IntegerValue(policy.flags))
}

// StrCaseFoldOp creates a string `caseFold` operation that applies a
// locale-independent case fold (lowercase) to the bin. Useful for normalized
// comparison keys.
func StrCaseFoldOp(policy *StringPolicy, binName string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_CASE_FOLD, binName, ctx, IntegerValue(policy.flags))
}

// StrNormalizeNFCOp creates a string `normalizeNFC` operation that normalizes
// the bin to Unicode NFC form. Already-normalized strings are unchanged.
func StrNormalizeNFCOp(policy *StringPolicy, binName string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_NORMALIZE_NFC, binName, ctx, IntegerValue(policy.flags))
}

// StrTrimStartOp creates a string `trimStart` operation that removes whitespace
// from the start of the bin.
func StrTrimStartOp(policy *StringPolicy, binName string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_TRIM_START, binName, ctx, IntegerValue(policy.flags))
}

// StrTrimEndOp creates a string `trimEnd` operation that removes whitespace
// from the end of the bin.
func StrTrimEndOp(policy *StringPolicy, binName string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_TRIM_END, binName, ctx, IntegerValue(policy.flags))
}

// StrTrimOp creates a string `trim` operation that removes whitespace from
// both ends of the bin.
func StrTrimOp(policy *StringPolicy, binName string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_TRIM, binName, ctx, IntegerValue(policy.flags))
}

// StrPadStartOp creates a string `padStart` operation that prepends `padString`
// repeatedly until the bin reaches `targetLength` codepoints. No-op when the
// bin is already at or above the target length.
func StrPadStartOp(policy *StringPolicy, binName string, targetLength int, padString string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_PAD_START, binName, ctx, IntegerValue(targetLength), StringValue(padString), IntegerValue(policy.flags))
}

// StrPadEndOp creates a string `padEnd` operation that appends `padString`
// repeatedly until the bin reaches `targetLength` codepoints. No-op when the
// bin is already at or above the target length.
func StrPadEndOp(policy *StringPolicy, binName string, targetLength int, padString string, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_PAD_END, binName, ctx, IntegerValue(targetLength), StringValue(padString), IntegerValue(policy.flags))
}

// StrRepeatOp creates a string `repeat` operation that repeats the bin contents
// `count` times. `count` must be non-negative.
func StrRepeatOp(policy *StringPolicy, binName string, count int, ctx ...*CDTContext) *Operation {
	policy = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_REPEAT, binName, ctx, IntegerValue(count), IntegerValue(policy.flags))
}

// StrRegexReplaceOp creates a string `regexReplace` operation that replaces the
// first match of `pattern` with `replacement`. Pass [StringRegexGlobal] to replace
// every match. Flag values from [StringRegexFlags] may be combined with bitwise OR.
//
// The server's regex_replace op table does not accept policy write flags, so
// `policy` is kept for API symmetry with the other modify ops and is ignored.
func StrRegexReplaceOp(policy *StringPolicy, binName string, pattern string, replacement string, regexFlags StringRegexFlags, ctx ...*CDTContext) *Operation {
	_ = stringPolicyOrDefault(policy)
	return newStringModifyOp(_STR_OP_REGEX_REPLACE, binName, ctx, ListValue{StringValue(pattern), StringValue(replacement)}, IntegerValue(int(regexFlags)))
}

//-----------------------------------------------------------------
// Type conversion
//-----------------------------------------------------------------

// StrToStringOp creates a `toString` operation that converts an integer, float,
// string, or blob bin to its string representation. Returns
// AEROSPIKE_ERR_INCOMPATIBLE_TYPE for any other bin type.
//
// Unlike the other builders in this file, StrToStringOp does not accept a
// [CDTContext]. The other string operations are sent as STRING_READ /
// STRING_MODIFY wire ops carrying a msgpack payload (with the optional CTX
// envelope). StrToStringOp is a separate top-level wire op (TO_STRING, code 19)
// that carries no payload at all — the bin is referenced solely by the operation
// header — and the server-side handler for it never inspects an op payload.
//
// To convert a value nested inside a list or map, extract the leaf with
// [ListGetByIndexOp] or [MapGetByKeyOp] (using the appropriate [CDTContext])
// and convert it client-side.
func StrToStringOp(binName string) *Operation {
	return &Operation{
		opType:   _TO_STRING,
		binName:  binName,
		binValue: NewNullValue(),
	}
}

//-----------------------------------------------------------------
// Internals
//-----------------------------------------------------------------

func stringPolicyOrDefault(p *StringPolicy) *StringPolicy {
	if p == nil {
		return DefaultStringPolicy
	}
	return p
}

func newStringReadOp(subop int, binName string, ctx []*CDTContext, args ...any) *Operation {
	return newStringOp(_STRING_READ, subop, binName, ctx, args)
}

func newStringModifyOp(subop int, binName string, ctx []*CDTContext, args ...any) *Operation {
	return newStringOp(_STRING_MODIFY, subop, binName, ctx, args)
}

// newStringOp pre-encodes the msgpack payload for a string operation and wraps
// it in a RawBlobValue tagged with ParticleType.STRING. The wire layout is
// `[SUBOP, args...]` when CTX is empty, and
// `[0xFF, [ctx_id, ctx_value, ...], [SUBOP, args...]]` when CTX is present —
// the same CONTEXT_EVAL envelope the CDT list/map/bitwise ops use, with a
// fixed outer element count of 3. Nesting the inner op makes its arity
// self-describing, so a trailing element an older server does not understand
// is rejected instead of being consumed as the op's optional policy flags.
func newStringOp(opType OperationType, subop int, binName string, ctx []*CDTContext, args []any) *Operation {
	sz, err := packStringOpBytes(nil, subop, ctx, args)
	if err != nil {
		panic(err)
	}

	buf := newBuffer(sz)
	_, err = packStringOpBytes(buf, subop, ctx, args)
	if err != nil {
		panic(err)
	}

	return &Operation{
		opType:   opType,
		binName:  binName,
		binValue: &RawBlobValue{ParticleType: ParticleType.STRING, Data: buf.Bytes()},
	}
}

func packStringOpBytes(buf BufferEx, subop int, ctx []*CDTContext, args []any) (int, Error) {
	size := 0
	n := 0
	var err Error

	if len(ctx) > 0 {
		if n, err = packArrayBegin(buf, 3); err != nil {
			return size + n, err
		}
		size += n

		if n, err = packAInt64(buf, 0xff); err != nil {
			return size + n, err
		}
		size += n

		if n, err = packArrayBegin(buf, len(ctx)*2); err != nil {
			return size + n, err
		}
		size += n

		// CDTContext.pack writes the id and the value as two separate msgpack
		// elements, which is why the list count above is len(ctx)*2.
		for _, c := range ctx {
			if n, err = c.pack(buf); err != nil {
				return size + n, err
			}
			size += n
		}
	}

	if n, err = packArrayBegin(buf, 1+len(args)); err != nil {
		return size + n, err
	}
	size += n

	if n, err = packAInt(buf, subop); err != nil {
		return size + n, err
	}
	size += n

	for _, a := range args {
		switch v := a.(type) {
		case Value:
			n, err = v.pack(buf)
		default:
			n, err = packObject(buf, a, false)
		}
		if err != nil {
			return size + n, err
		}
		size += n
	}

	return size, nil
}
