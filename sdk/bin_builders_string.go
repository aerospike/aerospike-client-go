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

package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// The server-side string operations require server 8.1.3 or newer. Gate on
// [Cluster.SupportsStringOperations] when you need to branch rather than let
// the server reject the command.

// --- String reads, on the write-side bin builder ---

// StrLen reports the string's length in characters.
func (b *WriteBinBuilder) StrLen() *WriteSegmentBuilder { return b.push(as.StrLenOp(b.bin)) }

// StrSubstrFrom reports the substring from an index to the end.
func (b *WriteBinBuilder) StrSubstrFrom(start int) *WriteSegmentBuilder {
	return b.push(as.StrSubstrFromOp(b.bin, start))
}

// StrSubstr reports the substring between two indexes.
func (b *WriteBinBuilder) StrSubstr(start, end int) *WriteSegmentBuilder {
	return b.push(as.StrSubstrOp(b.bin, start, end))
}

// StrCharAt reports the character at an index.
func (b *WriteBinBuilder) StrCharAt(index int) *WriteSegmentBuilder {
	return b.push(as.StrCharAtOp(b.bin, index))
}

// StrFind reports the index of the first occurrence of a substring.
func (b *WriteBinBuilder) StrFind(needle string) *WriteSegmentBuilder {
	return b.push(as.StrFindOp(b.bin, needle))
}

// StrFindNth reports the index of the n-th occurrence of a substring.
func (b *WriteBinBuilder) StrFindNth(needle string, occurrence int) *WriteSegmentBuilder {
	return b.push(as.StrFindNthOp(b.bin, needle, occurrence))
}

// StrContains reports whether the string holds a substring.
func (b *WriteBinBuilder) StrContains(needle string) *WriteSegmentBuilder {
	return b.push(as.StrContainsOp(b.bin, needle))
}

// StrStartsWith reports whether the string starts with a prefix.
func (b *WriteBinBuilder) StrStartsWith(prefix string) *WriteSegmentBuilder {
	return b.push(as.StrStartsWithOp(b.bin, prefix))
}

// StrEndsWith reports whether the string ends with a suffix.
func (b *WriteBinBuilder) StrEndsWith(suffix string) *WriteSegmentBuilder {
	return b.push(as.StrEndsWithOp(b.bin, suffix))
}

// StrToInteger parses the string as an integer.
func (b *WriteBinBuilder) StrToInteger() *WriteSegmentBuilder {
	return b.push(as.StrToIntegerOp(b.bin))
}

// StrToDouble parses the string as a floating-point number.
func (b *WriteBinBuilder) StrToDouble() *WriteSegmentBuilder {
	return b.push(as.StrToDoubleOp(b.bin))
}

// StrByteLength reports the string's length in bytes.
func (b *WriteBinBuilder) StrByteLength() *WriteSegmentBuilder {
	return b.push(as.StrByteLengthOp(b.bin))
}

// StrIsNumeric reports whether the string is numeric.
func (b *WriteBinBuilder) StrIsNumeric() *WriteSegmentBuilder {
	return b.push(as.StrIsNumericOp(b.bin))
}

// StrIsUpper reports whether the string is upper case.
func (b *WriteBinBuilder) StrIsUpper() *WriteSegmentBuilder {
	return b.push(as.StrIsUpperOp(b.bin))
}

// StrIsLower reports whether the string is lower case.
func (b *WriteBinBuilder) StrIsLower() *WriteSegmentBuilder {
	return b.push(as.StrIsLowerOp(b.bin))
}

// StrToBlob reinterprets the string as a byte slice.
func (b *WriteBinBuilder) StrToBlob() *WriteSegmentBuilder {
	return b.push(as.StrToBlobOp(b.bin))
}

// StrSplit splits the string on whitespace.
func (b *WriteBinBuilder) StrSplit() *WriteSegmentBuilder { return b.push(as.StrSplitOp(b.bin)) }

// StrSplitBySeparator splits the string on a separator.
func (b *WriteBinBuilder) StrSplitBySeparator(sep string) *WriteSegmentBuilder {
	return b.push(as.StrSplitBySeparatorOp(b.bin, sep))
}

// StrB64Decode decodes the string as base64.
func (b *WriteBinBuilder) StrB64Decode() *WriteSegmentBuilder {
	return b.push(as.StrB64DecodeOp(b.bin))
}

// StrRegexCompare matches the string against a pattern.
func (b *WriteBinBuilder) StrRegexCompare(pattern string) *WriteSegmentBuilder {
	return b.push(as.StrRegexCompareOp(b.bin, pattern))
}

// StrRegexCompareWithFlags matches the string against a pattern under flags.
func (b *WriteBinBuilder) StrRegexCompareWithFlags(pattern string, flags as.StringRegexFlags) *WriteSegmentBuilder {
	return b.push(as.StrRegexCompareWithFlagsOp(b.bin, pattern, flags))
}

// --- String modifications ---

// StrInsert inserts a substring at an index.
func (b *WriteBinBuilder) StrInsert(p *as.StringPolicy, index int, value string) *WriteSegmentBuilder {
	return b.push(as.StrInsertOp(p, b.bin, index, value))
}

// StrOverwrite overwrites at an index.
func (b *WriteBinBuilder) StrOverwrite(p *as.StringPolicy, index int, value string) *WriteSegmentBuilder {
	return b.push(as.StrOverwriteOp(p, b.bin, index, value))
}

// StrConcat appends a string.
func (b *WriteBinBuilder) StrConcat(p *as.StringPolicy, value string) *WriteSegmentBuilder {
	return b.push(as.StrConcatOp(p, b.bin, value))
}

// --- String reads, on the read-side bin builder ---

// StrLen reports the string's length in characters.
func (b *QueryBinBuilder) StrLen() *QueryBuilder { return b.push(as.StrLenOp(b.bin)) }

// StrSubstr reports the substring between two indexes.
func (b *QueryBinBuilder) StrSubstr(start, end int) *QueryBuilder {
	return b.push(as.StrSubstrOp(b.bin, start, end))
}

// StrSubstrFrom reports the substring from an index to the end.
func (b *QueryBinBuilder) StrSubstrFrom(start int) *QueryBuilder {
	return b.push(as.StrSubstrFromOp(b.bin, start))
}

// StrCharAt reports the character at an index.
func (b *QueryBinBuilder) StrCharAt(index int) *QueryBuilder {
	return b.push(as.StrCharAtOp(b.bin, index))
}

// StrFind reports the index of the first occurrence of a substring.
func (b *QueryBinBuilder) StrFind(needle string) *QueryBuilder {
	return b.push(as.StrFindOp(b.bin, needle))
}

// StrContains reports whether the string holds a substring.
func (b *QueryBinBuilder) StrContains(needle string) *QueryBuilder {
	return b.push(as.StrContainsOp(b.bin, needle))
}

// StrStartsWith reports whether the string starts with a prefix.
func (b *QueryBinBuilder) StrStartsWith(prefix string) *QueryBuilder {
	return b.push(as.StrStartsWithOp(b.bin, prefix))
}

// StrEndsWith reports whether the string ends with a suffix.
func (b *QueryBinBuilder) StrEndsWith(suffix string) *QueryBuilder {
	return b.push(as.StrEndsWithOp(b.bin, suffix))
}

// StrToInteger parses the string as an integer.
func (b *QueryBinBuilder) StrToInteger() *QueryBuilder { return b.push(as.StrToIntegerOp(b.bin)) }

// StrToDouble parses the string as a floating-point number.
func (b *QueryBinBuilder) StrToDouble() *QueryBuilder { return b.push(as.StrToDoubleOp(b.bin)) }

// StrByteLength reports the string's length in bytes.
func (b *QueryBinBuilder) StrByteLength() *QueryBuilder { return b.push(as.StrByteLengthOp(b.bin)) }

// StrIsNumeric reports whether the string is numeric.
func (b *QueryBinBuilder) StrIsNumeric() *QueryBuilder { return b.push(as.StrIsNumericOp(b.bin)) }

// StrIsUpper reports whether the string is upper case.
func (b *QueryBinBuilder) StrIsUpper() *QueryBuilder { return b.push(as.StrIsUpperOp(b.bin)) }

// StrIsLower reports whether the string is lower case.
func (b *QueryBinBuilder) StrIsLower() *QueryBuilder { return b.push(as.StrIsLowerOp(b.bin)) }

// StrSplit splits the string on whitespace.
func (b *QueryBinBuilder) StrSplit() *QueryBuilder { return b.push(as.StrSplitOp(b.bin)) }

// StrSplitBySeparator splits the string on a separator.
func (b *QueryBinBuilder) StrSplitBySeparator(sep string) *QueryBuilder {
	return b.push(as.StrSplitBySeparatorOp(b.bin, sep))
}

// StrB64Decode decodes the string as base64.
func (b *QueryBinBuilder) StrB64Decode() *QueryBuilder { return b.push(as.StrB64DecodeOp(b.bin)) }

// StrRegexCompare matches the string against a pattern.
func (b *QueryBinBuilder) StrRegexCompare(pattern string) *QueryBuilder {
	return b.push(as.StrRegexCompareOp(b.bin, pattern))
}

// StrToBlob reinterprets the string as a byte slice.
func (b *QueryBinBuilder) StrToBlob() *QueryBuilder { return b.push(as.StrToBlobOp(b.bin)) }

// --- The remaining string modifications ---

// StrAppend appends to the string.
func (b *WriteBinBuilder) StrAppend(p *as.StringPolicy, value string) *WriteSegmentBuilder {
	return b.push(as.StrAppendOp(p, b.bin, value))
}

// StrPrepend prepends to the string.
func (b *WriteBinBuilder) StrPrepend(p *as.StringPolicy, value string) *WriteSegmentBuilder {
	return b.push(as.StrPrependOp(p, b.bin, value))
}

// StrUpper upper-cases the string.
func (b *WriteBinBuilder) StrUpper(p *as.StringPolicy) *WriteSegmentBuilder {
	return b.push(as.StrUpperOp(p, b.bin))
}

// StrLower lower-cases the string.
func (b *WriteBinBuilder) StrLower(p *as.StringPolicy) *WriteSegmentBuilder {
	return b.push(as.StrLowerOp(p, b.bin))
}

// StrCaseFold case-folds the string for caseless comparison.
func (b *WriteBinBuilder) StrCaseFold(p *as.StringPolicy) *WriteSegmentBuilder {
	return b.push(as.StrCaseFoldOp(p, b.bin))
}

// StrNormalizeNFC normalizes the string to Unicode NFC.
func (b *WriteBinBuilder) StrNormalizeNFC(p *as.StringPolicy) *WriteSegmentBuilder {
	return b.push(as.StrNormalizeNFCOp(p, b.bin))
}

// StrTrim removes leading and trailing whitespace.
func (b *WriteBinBuilder) StrTrim(p *as.StringPolicy) *WriteSegmentBuilder {
	return b.push(as.StrTrimOp(p, b.bin))
}

// StrTrimStart removes leading whitespace.
func (b *WriteBinBuilder) StrTrimStart(p *as.StringPolicy) *WriteSegmentBuilder {
	return b.push(as.StrTrimStartOp(p, b.bin))
}

// StrTrimEnd removes trailing whitespace.
func (b *WriteBinBuilder) StrTrimEnd(p *as.StringPolicy) *WriteSegmentBuilder {
	return b.push(as.StrTrimEndOp(p, b.bin))
}

// StrPadStart pads the front of the string to a target length.
func (b *WriteBinBuilder) StrPadStart(p *as.StringPolicy, targetLength int, pad string) *WriteSegmentBuilder {
	return b.push(as.StrPadStartOp(p, b.bin, targetLength, pad))
}

// StrPadEnd pads the end of the string to a target length.
func (b *WriteBinBuilder) StrPadEnd(p *as.StringPolicy, targetLength int, pad string) *WriteSegmentBuilder {
	return b.push(as.StrPadEndOp(p, b.bin, targetLength, pad))
}

// StrRepeat repeats the string count times.
func (b *WriteBinBuilder) StrRepeat(p *as.StringPolicy, count int) *WriteSegmentBuilder {
	return b.push(as.StrRepeatOp(p, b.bin, count))
}

// StrSnip removes the characters between two indexes.
func (b *WriteBinBuilder) StrSnip(p *as.StringPolicy, start, end int) *WriteSegmentBuilder {
	return b.push(as.StrSnipOp(p, b.bin, start, end))
}

// StrReplace replaces the first occurrence of a substring.
func (b *WriteBinBuilder) StrReplace(p *as.StringPolicy, needle, replacement string) *WriteSegmentBuilder {
	return b.push(as.StrReplaceOp(p, b.bin, needle, replacement))
}

// StrReplaceAll replaces every occurrence of a substring.
func (b *WriteBinBuilder) StrReplaceAll(p *as.StringPolicy, needle, replacement string) *WriteSegmentBuilder {
	return b.push(as.StrReplaceAllOp(p, b.bin, needle, replacement))
}

// StrRegexReplace replaces the matches of a pattern.
func (b *WriteBinBuilder) StrRegexReplace(p *as.StringPolicy, pattern, replacement string, flags as.StringRegexFlags) *WriteSegmentBuilder {
	return b.push(as.StrRegexReplaceOp(p, b.bin, pattern, replacement, flags))
}

// StrConcatList appends several strings in order.
func (b *WriteBinBuilder) StrConcatList(p *as.StringPolicy, values []string) *WriteSegmentBuilder {
	return b.push(as.StrConcatListOp(p, b.bin, values))
}

// StrToString reports the string as a string, the identity read the server
// provides for type normalization.
func (b *WriteBinBuilder) StrToString() *WriteSegmentBuilder {
	return b.push(as.StrToStringOp(b.bin))
}

// StrIsNumericTyped reports whether the string parses as a specific numeric
// type.
func (b *WriteBinBuilder) StrIsNumericTyped(t as.StringNumericType) *WriteSegmentBuilder {
	return b.push(as.StrIsNumericTypedOp(b.bin, t))
}

// --- The reads the query-side builder was missing ---

// StrFindNth reports the index of the n-th occurrence of a substring.
func (b *QueryBinBuilder) StrFindNth(needle string, occurrence int) *QueryBuilder {
	return b.push(as.StrFindNthOp(b.bin, needle, occurrence))
}

// StrIsNumericTyped reports whether the string parses as a specific numeric
// type.
func (b *QueryBinBuilder) StrIsNumericTyped(t as.StringNumericType) *QueryBuilder {
	return b.push(as.StrIsNumericTypedOp(b.bin, t))
}

// StrRegexCompareWithFlags matches the string against a pattern under flags.
func (b *QueryBinBuilder) StrRegexCompareWithFlags(pattern string, flags as.StringRegexFlags) *QueryBuilder {
	return b.push(as.StrRegexCompareWithFlagsOp(b.bin, pattern, flags))
}

// StrToString reports the string as a string.
func (b *QueryBinBuilder) StrToString() *QueryBuilder {
	return b.push(as.StrToStringOp(b.bin))
}
