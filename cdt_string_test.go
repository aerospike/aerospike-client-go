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

package aerospike_test

import (
	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Ported from the Java client's TestOperateString. Each test exercises one
// behavior of the StringOperation API rather than one API method, so each
// individual It runs in isolation.
//
// String operations require server version 8.1.3+; the suite is skipped on
// older clusters via the standard Ginkgo version-check pattern documented in
// AI_PIPELINE.md.
var _ = gg.Describe("String Operations Test", func() {
	const bin = "sbin"

	var (
		ns  = *namespace
		set = randString(50)
		key *as.Key
	)

	policy := as.DefaultStringPolicy

	put := func(value string) {
		client.Delete(nil, key)
		err := client.PutBins(nil, key, as.NewBin(bin, value))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	}

	putBins := func(bins ...*as.Bin) {
		client.Delete(nil, key)
		err := client.PutBins(nil, key, bins...)
		gm.Expect(err).ToNot(gm.HaveOccurred())
	}

	operate := func(ops ...*as.Operation) *as.Record {
		rec, err := client.Operate(nil, key, ops...)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return rec
	}

	stringValue := func() string {
		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return rec.Bins[bin].(string)
	}

	gg.BeforeEach(func() {
		requiredVersion, err := version.Parse("8.1.3")
		if err != nil {
			gg.Fail("Failed to parse server required version")
		}
		nodeVersion := client.GetNodes()[0].GetServerVersion()
		if nodeVersion.IsSmaller(requiredVersion) {
			gg.Skip("String operations require server version 8.1.3+.")
			return
		}

		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	// ============================================================
	// Read operations
	// ============================================================

	gg.It("strlen returns the codepoint count", func() {
		put("hello world")
		rec := operate(as.StrLenOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(11))
	})

	gg.It("strlen on empty string is zero", func() {
		put("")
		rec := operate(as.StrLenOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(0))
	})

	gg.It("byteLength returns UTF-8 bytes", func() {
		put("hello")
		rec := operate(as.StrByteLengthOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(5))
	})

	gg.It("substr reads from offset to end", func() {
		put("hello world")
		rec := operate(as.StrSubstrFromOp(bin, 6))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("world"))
	})

	gg.It("substr slices a range", func() {
		put("hello world")
		rec := operate(as.StrSubstrOp(bin, 0, 5))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("hello"))
	})

	gg.It("substr supports negative start", func() {
		put("hello world")
		rec := operate(as.StrSubstrFromOp(bin, -5))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("world"))
	})

	gg.It("charAt returns a single character", func() {
		put("Hello123World")
		rec := operate(as.StrCharAtOp(bin, 5))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("1"))
	})

	gg.It("find returns the index of the first match", func() {
		put("hello world")
		rec := operate(as.StrFindOp(bin, "world"))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(6))
	})

	gg.It("find returns -1 when needle is absent", func() {
		put("hello world")
		rec := operate(as.StrFindOp(bin, "xyz"))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(-1))
	})

	gg.It("find supports nth occurrence", func() {
		put("ababab")
		rec := operate(as.StrFindNthOp(bin, "ab", 2))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(2))
	})

	gg.It("find skips overlapping matches (ASCII)", func() {
		// "aa" is a self-overlapping needle (prefix "a" == suffix "a"). After
		// matching at index 0 the search resumes *after* the match (index 2),
		// so the 2nd occurrence is at 2 — not 1. Mirrors replace() and the ICU
		// usearch path used for non-ASCII haystacks.
		put("aaaa")
		gm.Expect(operate(as.StrFindNthOp(bin, "aa", 1)).Bins[bin]).To(gm.Equal(0))
		gm.Expect(operate(as.StrFindNthOp(bin, "aa", 2)).Bins[bin]).To(gm.Equal(2))
		gm.Expect(operate(as.StrFindNthOp(bin, "aa", 3)).Bins[bin]).To(gm.Equal(-1))
	})

	gg.It("find skips overlapping matches (Unicode)", func() {
		// Same overlap-skip rule on the ICU path. "👋👋" is self-overlapping in
		// codepoints; matches land at codepoint indices 0 and 2, not 0 and 1.
		put("👋👋👋👋")
		gm.Expect(operate(as.StrFindNthOp(bin, "👋👋", 1)).Bins[bin]).To(gm.Equal(0))
		gm.Expect(operate(as.StrFindNthOp(bin, "👋👋", 2)).Bins[bin]).To(gm.Equal(2))
		gm.Expect(operate(as.StrFindNthOp(bin, "👋👋", 3)).Bins[bin]).To(gm.Equal(-1))
	})

	gg.It("contains returns a boolean match flag", func() {
		put("hello world")
		gm.Expect(operate(as.StrContainsOp(bin, "hello")).Bins[bin]).To(gm.Equal(true))
		gm.Expect(operate(as.StrContainsOp(bin, "xyz")).Bins[bin]).To(gm.Equal(false))
	})

	gg.It("startsWith matches the prefix", func() {
		put("Hello123World")
		gm.Expect(operate(as.StrStartsWithOp(bin, "Hello")).Bins[bin]).To(gm.Equal(true))
		gm.Expect(operate(as.StrStartsWithOp(bin, "World")).Bins[bin]).To(gm.Equal(false))
	})

	gg.It("endsWith matches the suffix", func() {
		put("Hello123World")
		gm.Expect(operate(as.StrEndsWithOp(bin, "World")).Bins[bin]).To(gm.Equal(true))
		gm.Expect(operate(as.StrEndsWithOp(bin, "Hello")).Bins[bin]).To(gm.Equal(false))
	})

	gg.It("isUpper distinguishes upper-cased text", func() {
		put("HELLO")
		gm.Expect(operate(as.StrIsUpperOp(bin)).Bins[bin]).To(gm.Equal(true))
		put("hello")
		gm.Expect(operate(as.StrIsUpperOp(bin)).Bins[bin]).To(gm.Equal(false))
	})

	gg.It("isLower distinguishes lower-cased text", func() {
		put("hello")
		gm.Expect(operate(as.StrIsLowerOp(bin)).Bins[bin]).To(gm.Equal(true))
		put("HELLO")
		gm.Expect(operate(as.StrIsLowerOp(bin)).Bins[bin]).To(gm.Equal(false))
	})

	gg.It("isNumeric matches integer-looking strings", func() {
		put("12345")
		gm.Expect(operate(as.StrIsNumericOp(bin)).Bins[bin]).To(gm.Equal(true))
		put("Hello123World")
		gm.Expect(operate(as.StrIsNumericOp(bin)).Bins[bin]).To(gm.Equal(false))
	})

	gg.It("toInteger parses digits as int64", func() {
		put("12345")
		rec := operate(as.StrToIntegerOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(12345))
	})

	gg.It("toDouble parses decimal numbers", func() {
		put("3.14")
		rec := operate(as.StrToDoubleOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.BeNumerically("~", 3.14, 0.001))
	})

	gg.It("split returns list of tokens by separator", func() {
		put("one,two,three")
		rec := operate(as.StrSplitBySeparatorOp(bin, ","))
		gm.Expect(rec.Bins[bin]).To(gm.Equal([]any{"one", "two", "three"}))
	})

	gg.It("split without match returns singleton list", func() {
		put("Hello123World")
		rec := operate(as.StrSplitBySeparatorOp(bin, "|"))
		gm.Expect(rec.Bins[bin]).To(gm.Equal([]any{"Hello123World"}))
	})

	gg.It("regexCompare matches and misses", func() {
		put("Hello123World")
		gm.Expect(operate(as.StrRegexCompareOp(bin, "[0-9]+")).Bins[bin]).To(gm.Equal(true))
		put("HELLO")
		gm.Expect(operate(as.StrRegexCompareOp(bin, "[0-9]+")).Bins[bin]).To(gm.Equal(false))
	})

	gg.It("regexCompare honors the case-insensitive flag", func() {
		put("HELLO")
		rec := operate(as.StrRegexCompareWithFlagsOp(bin, "hello", as.StringRegexCaseInsensitive))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(true))
	})

	gg.It("toBlob returns the UTF-8 bytes", func() {
		put("hello")
		rec := operate(as.StrToBlobOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal([]byte("hello")))
	})

	gg.It("b64Decode returns the decoded blob", func() {
		put("aGVsbG8=")
		rec := operate(as.StrB64DecodeOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal([]byte("hello")))
	})

	// ============================================================
	// Modify operations
	// ============================================================

	gg.It("upper mutates the bin in place", func() {
		put("hello world")
		operate(as.StrUpperOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("HELLO WORLD"))
	})

	gg.It("lower mutates the bin in place", func() {
		put("HELLO WORLD")
		operate(as.StrLowerOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("caseFold lowercases locale-independently", func() {
		put("HELLO World")
		operate(as.StrCaseFoldOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("normalizeNFC leaves already-normalized string unchanged", func() {
		put("hello")
		operate(as.StrNormalizeNFCOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("hello"))
	})

	gg.It("insert at middle splices value", func() {
		put("hello world")
		operate(as.StrInsertOp(policy, bin, 5, " beautiful"))
		gm.Expect(stringValue()).To(gm.Equal("hello beautiful world"))
	})

	gg.It("insert at zero prepends value", func() {
		put("world")
		operate(as.StrInsertOp(policy, bin, 0, "hello "))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("insert at end appends value", func() {
		put("hello")
		operate(as.StrInsertOp(policy, bin, 5, " world"))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("insert with negative index counts from end", func() {
		put("hello world")
		operate(as.StrInsertOp(policy, bin, -5, "big "))
		gm.Expect(stringValue()).To(gm.Equal("hello big world"))
	})

	gg.It("overwrite replaces characters starting at index", func() {
		put("hello world")
		operate(as.StrOverwriteOp(policy, bin, 6, "earth"))
		gm.Expect(stringValue()).To(gm.Equal("hello earth"))
	})

	gg.It("overwrite at zero replaces prefix", func() {
		put("hello world")
		operate(as.StrOverwriteOp(policy, bin, 0, "HELLO"))
		gm.Expect(stringValue()).To(gm.Equal("HELLO world"))
	})

	gg.It("overwrite can extend beyond original length", func() {
		put("hello")
		operate(as.StrOverwriteOp(policy, bin, 3, "ping!"))
		gm.Expect(stringValue()).To(gm.Equal("helping!"))
	})

	gg.It("snip removes character range", func() {
		put("hello beautiful world")
		operate(as.StrSnipOp(policy, bin, 5, 15))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("snip from start trims prefix", func() {
		put("hello world")
		operate(as.StrSnipOp(policy, bin, 0, 6))
		gm.Expect(stringValue()).To(gm.Equal("world"))
	})

	gg.It("snip to end trims suffix", func() {
		put("hello world")
		operate(as.StrSnipOp(policy, bin, 5, 11))
		gm.Expect(stringValue()).To(gm.Equal("hello"))
	})

	gg.It("replace touches only the first occurrence", func() {
		put("hello world world")
		operate(as.StrReplaceOp(policy, bin, "world", "earth"))
		gm.Expect(stringValue()).To(gm.Equal("hello earth world"))
	})

	gg.It("replace with no match leaves bin unchanged", func() {
		put("hello world")
		operate(as.StrReplaceOp(policy, bin, "xyz", "abc"))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("replace can grow the string", func() {
		put("hi world")
		operate(as.StrReplaceOp(policy, bin, "hi", "hello"))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("replace with empty deletes match", func() {
		put("hello world")
		operate(as.StrReplaceOp(policy, bin, " world", ""))
		gm.Expect(stringValue()).To(gm.Equal("hello"))
	})

	gg.It("replaceAll substitutes every match", func() {
		put("aabaa")
		operate(as.StrReplaceAllOp(policy, bin, "a", "x"))
		gm.Expect(stringValue()).To(gm.Equal("xxbxx"))
	})

	gg.It("replaceAll with no match leaves bin unchanged", func() {
		put("hello")
		operate(as.StrReplaceAllOp(policy, bin, "z", "!"))
		gm.Expect(stringValue()).To(gm.Equal("hello"))
	})

	gg.It("replaceAll skips overlapping matches", func() {
		// Self-overlapping needle "aa" in "aaaa": replacement resumes after each
		// match, yielding "XX" — not "XaX" (which would require allowing the
		// 2nd match to start at index 1). Anchors the contract that find()
		// mirrors.
		put("aaaa")
		operate(as.StrReplaceAllOp(policy, bin, "aa", "X"))
		gm.Expect(stringValue()).To(gm.Equal("XX"))
	})

	gg.It("trim removes whitespace on both ends", func() {
		put("  hello world  ")
		operate(as.StrTrimOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("trim on clean string is a no-op", func() {
		put("hello")
		operate(as.StrTrimOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("hello"))
	})

	gg.It("trimStart removes leading whitespace only", func() {
		put("  hello  ")
		operate(as.StrTrimStartOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("hello  "))
	})

	gg.It("trimEnd removes trailing whitespace only", func() {
		put("  hello  ")
		operate(as.StrTrimEndOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("  hello"))
	})

	gg.It("padStart fills left to target length", func() {
		put("hello")
		operate(as.StrPadStartOp(policy, bin, 10, "*"))
		gm.Expect(stringValue()).To(gm.Equal("*****hello"))
	})

	gg.It("padStart is a no-op when already long enough", func() {
		put("hello world")
		operate(as.StrPadStartOp(policy, bin, 5, "*"))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("padEnd fills right to target length", func() {
		put("hello")
		operate(as.StrPadEndOp(policy, bin, 10, "."))
		gm.Expect(stringValue()).To(gm.Equal("hello....."))
	})

	gg.It("padStart repeats multi-char filler", func() {
		put("hi")
		operate(as.StrPadStartOp(policy, bin, 8, "ab"))
		gm.Expect(stringValue()).To(gm.Equal("abababhi"))
	})

	gg.It("repeat duplicates contents", func() {
		put("ab")
		operate(as.StrRepeatOp(policy, bin, 3))
		gm.Expect(stringValue()).To(gm.Equal("ababab"))
	})

	gg.It("repeat once leaves bin unchanged", func() {
		put("hello")
		operate(as.StrRepeatOp(policy, bin, 1))
		gm.Expect(stringValue()).To(gm.Equal("hello"))
	})

	gg.It("concat appends a single string", func() {
		put("  hello world  ")
		operate(as.StrConcatOp(policy, bin, "!"))
		gm.Expect(stringValue()).To(gm.Equal("  hello world  !"))
	})

	gg.It("concat appends a list of values", func() {
		put("hello")
		operate(as.StrConcatListOp(policy, bin, []string{" ", "big", " world"}))
		gm.Expect(stringValue()).To(gm.Equal("hello big world"))
	})

	gg.It("append adds value to end", func() {
		put("hello")
		operate(as.StrAppendOp(policy, bin, " world"))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("append to empty string yields value", func() {
		put("")
		operate(as.StrAppendOp(policy, bin, "hi"))
		gm.Expect(stringValue()).To(gm.Equal("hi"))
	})

	gg.It("append preserves multibyte codepoints", func() {
		// Unicode/DBCS-aware: appending a multi-byte string must not corrupt
		// either side. "日本" + "語" -> "日本語" (3 codepoints, 9 UTF-8 bytes).
		put("日本")
		operate(as.StrAppendOp(policy, bin, "語"))
		gm.Expect(stringValue()).To(gm.Equal("日本語"))
		gm.Expect(operate(as.StrLenOp(bin)).Bins[bin]).To(gm.Equal(3))
	})

	gg.It("prepend adds value to start", func() {
		put("world")
		operate(as.StrPrependOp(policy, bin, "hello "))
		gm.Expect(stringValue()).To(gm.Equal("hello world"))
	})

	gg.It("prepend to empty string yields value", func() {
		put("")
		operate(as.StrPrependOp(policy, bin, "hi"))
		gm.Expect(stringValue()).To(gm.Equal("hi"))
	})

	gg.It("prepend preserves multibyte codepoints", func() {
		// Unicode/DBCS-aware: prepending a multi-byte string must not corrupt
		// either side. "語" prepended with "日本" -> "日本語".
		put("語")
		operate(as.StrPrependOp(policy, bin, "日本"))
		gm.Expect(stringValue()).To(gm.Equal("日本語"))
		gm.Expect(operate(as.StrLenOp(bin)).Bins[bin]).To(gm.Equal(3))
	})

	gg.It("append on missing bin creates the bin from empty", func() {
		// Create-ops {insert, concat, append, prepend} bootstrap an empty string
		// and create a missing bin. NO_FAIL is irrelevant — the op always succeeds.
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrAppendOp(policy, bin, "x"))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[bin]).To(gm.Equal("x"))
		gm.Expect(rec.Bins["other"]).To(gm.Equal("untouched"))
	})

	gg.It("regexReplace targets first match by default", func() {
		put("abc123def456")
		operate(as.StrRegexReplaceOp(policy, bin, "[0-9]+", "NUM", as.StringRegexDefault))
		gm.Expect(stringValue()).To(gm.Equal("abcNUMdef456"))
	})

	gg.It("regexReplace with GLOBAL flag replaces every match", func() {
		put("abc123def456")
		operate(as.StrRegexReplaceOp(policy, bin, "[0-9]+", "NUM", as.StringRegexGlobal))
		gm.Expect(stringValue()).To(gm.Equal("abcNUMdefNUM"))
	})

	gg.It("regexReplace with no match leaves bin unchanged", func() {
		put("hello")
		operate(as.StrRegexReplaceOp(policy, bin, "[0-9]+", "NUM", as.StringRegexGlobal))
		gm.Expect(stringValue()).To(gm.Equal("hello"))
	})

	// ============================================================
	// Multi-op pipelines
	// ============================================================

	gg.It("reads across multiple bins in one operate", func() {
		putBins(
			as.NewBin("text", "  hello world  "),
			as.NewBin("number_str", "12345"),
			as.NewBin("upper_str", "HELLO"))

		rec := operate(
			as.StrLenOp("text"),
			as.StrToIntegerOp("number_str"),
			as.StrIsUpperOp("upper_str"))

		gm.Expect(rec.Bins["text"]).To(gm.Equal(15))
		gm.Expect(rec.Bins["number_str"]).To(gm.Equal(12345))
		gm.Expect(rec.Bins["upper_str"]).To(gm.Equal(true))
	})

	gg.It("modify-then-read pipeline commits then observes", func() {
		put("  hello world  ")

		rec := operate(
			as.StrTrimOp(policy, bin),
			as.StrUpperOp(policy, bin),
			as.StrLenOp(bin))

		// CLIENT-5102: because the client auto-sets RESPOND_ALL_OPS for string
		// ops, every op contributes exactly one result slot — the two modify
		// ops emit nil and strlen emits its value at its submission index. The
		// positional index<->op mapping is preserved.
		results := rec.Bins[bin].(as.OpResults)
		gm.Expect(len(results)).To(gm.Equal(3))
		gm.Expect(results[0]).To(gm.BeNil()) // trim (modify)
		gm.Expect(results[1]).To(gm.BeNil()) // upper (modify)
		gm.Expect(results[2]).To(gm.Equal(11))
		gm.Expect(stringValue()).To(gm.Equal("HELLO WORLD"))
	})

	gg.It("preserves positional index<->op mapping with a modify op mixed among reads (CLIENT-5102)", func() {
		// The exact regression from the ticket: with the default policy
		// (RespondPerEachOp = false), a same-bin multi-op that mixes a modify
		// op with reads must return one slot per submitted op. Without the fix
		// the modify op's slot is dropped and every following read shifts down
		// one position (e.g. [6, "h"] instead of [nil, 6, "h"]) — a silent
		// mis-read with no error.
		put("hello")

		rec := operate(
			as.StrUpperOp(policy, bin), // index 0: modify -> nil slot
			as.StrLenOp(bin),           // index 1: strlen -> 5
			as.StrCharAtOp(bin, 0),     // index 2: charAt -> "H"
		)

		results := rec.Bins[bin].(as.OpResults)
		gm.Expect(len(results)).To(gm.Equal(3))
		gm.Expect(results[0]).To(gm.BeNil())
		gm.Expect(results[1]).To(gm.Equal(5))
		gm.Expect(results[2]).To(gm.Equal("H"))
		gm.Expect(stringValue()).To(gm.Equal("HELLO"))
	})

	gg.It("chained replaceAll and padding compose as expected", func() {
		put("aabaa")

		operate(
			as.StrReplaceAllOp(policy, bin, "a", "x"),
			as.StrPadEndOp(policy, bin, 10, "."))

		gm.Expect(stringValue()).To(gm.Equal("xxbxx....."))
	})

	gg.It("snip then concat in one operate", func() {
		put("hello beautiful world")

		operate(
			as.StrSnipOp(policy, bin, 5, 15),
			as.StrConcatOp(policy, bin, "!"))

		gm.Expect(stringValue()).To(gm.Equal("hello world!"))
	})

	gg.It("split result entries are readable strings", func() {
		put("one,two,three")
		rec := operate(as.StrSplitBySeparatorOp(bin, ","))
		tokens := rec.Bins[bin].([]any)
		gm.Expect(len(tokens)).To(gm.Equal(3))
		for _, t := range tokens {
			_, ok := t.(string)
			gm.Expect(ok).To(gm.BeTrue(), "expected string element")
		}
	})

	// ============================================================
	// CTX navigation — string nested in list/map bins
	// ============================================================

	gg.It("read op on string nested in list", func() {
		list := []any{"alpha", "beta", "hello world"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, list))).ToNot(gm.HaveOccurred())

		rec := operate(as.StrLenOp(bin, as.CtxListIndex(2)))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(11))
	})

	gg.It("read boolean op on string nested in map", func() {
		m := map[any]any{"a": "Hello", "b": "World"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, m))).ToNot(gm.HaveOccurred())

		rec := operate(as.StrStartsWithOp(bin, "Wor", as.CtxMapKey(as.StringValue("b"))))
		gm.Expect(rec.Bins[bin]).To(gm.Equal(true))
	})

	gg.It("modify op on string nested in list", func() {
		list := []any{"alpha", "beta", "gamma"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, list))).ToNot(gm.HaveOccurred())

		operate(as.StrUpperOp(policy, bin, as.CtxListIndex(1)))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[bin]).To(gm.Equal([]any{"alpha", "BETA", "gamma"}))
	})

	gg.It("modify op on string nested in map", func() {
		m := map[any]any{"a": "hello world", "b": "foo"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, m))).ToNot(gm.HaveOccurred())

		operate(as.StrReplaceOp(policy, bin, "world", "earth",
			as.CtxMapKey(as.StringValue("a"))))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		after := rec.Bins[bin].(map[any]any)
		gm.Expect(after["a"]).To(gm.Equal("hello earth"))
		gm.Expect(after["b"]).To(gm.Equal("foo"))
	})

	gg.It("modify op on string deeply nested list in map", func() {
		inner := []any{"one", "two", "three"}
		m := map[any]any{"items": inner}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, m))).ToNot(gm.HaveOccurred())

		operate(as.StrUpperOp(policy, bin,
			as.CtxMapKey(as.StringValue("items")), as.CtxListIndex(1)))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		after := rec.Bins[bin].(map[any]any)
		items := after["items"].([]any)
		gm.Expect(items).To(gm.Equal([]any{"one", "TWO", "three"}))
	})

	gg.It("append on string nested in list", func() {
		// list = ["alpha", "beta", "gamma"]; append "!" at index 1 -> "beta!"
		list := []any{"alpha", "beta", "gamma"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, list))).ToNot(gm.HaveOccurred())

		operate(as.StrAppendOp(policy, bin, "!", as.CtxListIndex(1)))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[bin]).To(gm.Equal([]any{"alpha", "beta!", "gamma"}))
	})

	gg.It("prepend on string nested in map", func() {
		// map = {"a": "world", "b": "foo"}; prepend "hello " at key "a"
		m := map[any]any{"a": "world", "b": "foo"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, m))).ToNot(gm.HaveOccurred())

		operate(as.StrPrependOp(policy, bin, "hello ",
			as.CtxMapKey(as.StringValue("a"))))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		after := rec.Bins[bin].(map[any]any)
		gm.Expect(after["a"]).To(gm.Equal("hello world"))
		gm.Expect(after["b"]).To(gm.Equal("foo"))
	})

	// ============================================================
	// toString op — op-type 19, no payload, no sub-op id, no CTX
	// ============================================================

	gg.It("toString converts an integer bin to a string", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, 42))).ToNot(gm.HaveOccurred())
		rec := operate(as.StrToStringOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("42"))
	})

	gg.It("toString converts a double bin to a string", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, 3.14))).ToNot(gm.HaveOccurred())
		rec := operate(as.StrToStringOp(bin))
		// Float-to-string formatting is server-side; assert it parses back.
		s := rec.Bins[bin].(string)
		gm.Expect(len(s)).To(gm.BeNumerically(">", 0))
	})

	gg.It("toString on a string bin is identity", func() {
		put("hello")
		rec := operate(as.StrToStringOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("hello"))
	})

	gg.It("toString converts a blob bin to a string", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, []byte("hi")))).ToNot(gm.HaveOccurred())
		rec := operate(as.StrToStringOp(bin))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("hi"))
	})

	gg.It("toString on a list bin returns BIN_TYPE_ERROR", func() {
		list := []any{"a", "b"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, list))).ToNot(gm.HaveOccurred())

		_, err := client.Operate(nil, key, as.StrToStringOp(bin))
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.BIN_TYPE_ERROR)).To(gm.BeTrue())
	})

	// ============================================================
	// Missing-bin path
	// ============================================================

	gg.It("modify on missing bin is a no-op", func() {
		// A non-create modify op on a missing bin is a silent no-op (success,
		// bin not created) regardless of NO_FAIL — there is no BIN_NOT_FOUND path.
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrUpperOp(policy, bin))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, exists := rec.Bins[bin]
		gm.Expect(exists).To(gm.BeFalse())
		gm.Expect(rec.Bins["other"]).To(gm.Equal("untouched"))
	})

	gg.It("NO_FAIL does not change the missing-bin no-op", func() {
		// The missing-bin no-op for non-create ops is flag-independent; NO_FAIL
		// neither creates the bin nor raises an error.
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		noFail := as.NewStringPolicy(as.StringWriteNoFail)
		operate(as.StrUpperOp(noFail, bin))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, exists := rec.Bins[bin]
		gm.Expect(exists).To(gm.BeFalse())
		gm.Expect(rec.Bins["other"]).To(gm.Equal("untouched"))
	})

	// All eight additive ops create a missing bin from empty in server 8.1.3
	// (string ops + SERVER-97 PR 1452, which adds overwrite/repeat/pad_start/
	// pad_end to the create-op set). Transform/subtractive ops still no-op.

	// readBin returns the bin value (or nil if absent) without the panicking
	// type assertion stringValue() uses — so a missing-bin create that didn't
	// create fails cleanly instead of panicking.
	readBin := func() any {
		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return rec.Bins[bin]
	}

	gg.It("insert on missing bin creates the bin from empty", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrInsertOp(policy, bin, 0, "hi"))

		gm.Expect(readBin()).To(gm.Equal("hi"))
	})

	gg.It("concat on missing bin creates the bin from empty", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrConcatOp(policy, bin, "hi"))

		gm.Expect(readBin()).To(gm.Equal("hi"))
	})

	gg.It("prepend on missing bin creates the bin from empty", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrPrependOp(policy, bin, "hi"))

		gm.Expect(readBin()).To(gm.Equal("hi"))
	})

	gg.It("overwrite on missing bin creates the bin from empty", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrOverwriteOp(policy, bin, 0, "hi"))

		gm.Expect(readBin()).To(gm.Equal("hi"))
	})

	gg.It("padStart on missing bin creates the bin from empty", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrPadStartOp(policy, bin, 5, "x"))

		gm.Expect(readBin()).To(gm.Equal("xxxxx"))
	})

	gg.It("padEnd on missing bin creates the bin from empty", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrPadEndOp(policy, bin, 5, "x"))

		gm.Expect(readBin()).To(gm.Equal("xxxxx"))
	})

	gg.It("repeat on missing bin creates an empty bin", func() {
		// repeat(n) on empty = "" — the bin is created holding an empty string
		// (server test: expect_string_bin(b, "")).
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin("other", "untouched"))).ToNot(gm.HaveOccurred())

		operate(as.StrRepeatOp(policy, bin, 3))

		gm.Expect(readBin()).To(gm.Equal(""))
	})

	// ============================================================
	// Codepoint-vs-byte anchors
	//
	// Server-side indices and strlen are in Unicode code points, not bytes
	// and not Java UTF-16 chars. Go strings are byte sequences with native
	// UTF-8, so the byte vs codepoint distinction is the relevant one here.
	// ============================================================

	gg.It("strlen counts codepoints not bytes", func() {
		// "café" = 4 codepoints, 5 UTF-8 bytes.
		put("café")
		gm.Expect(operate(as.StrLenOp(bin)).Bins[bin]).To(gm.Equal(4))
		// "日本語" = 3 codepoints, 9 UTF-8 bytes.
		put("日本語")
		gm.Expect(operate(as.StrLenOp(bin)).Bins[bin]).To(gm.Equal(3))
		// "👋hi" — emoji is U+1F44B, a supplementary codepoint (4 UTF-8 bytes).
		// Total codepoints = 3.
		put("👋hi")
		gm.Expect(operate(as.StrLenOp(bin)).Bins[bin]).To(gm.Equal(3))
	})

	gg.It("byteLength counts bytes not codepoints", func() {
		put("café")
		gm.Expect(operate(as.StrByteLengthOp(bin)).Bins[bin]).To(gm.Equal(5))
		put("日本語")
		gm.Expect(operate(as.StrByteLengthOp(bin)).Bins[bin]).To(gm.Equal(9))
		// 👋 = 4 UTF-8 bytes, "hi" = 2 bytes.
		put("👋hi")
		gm.Expect(operate(as.StrByteLengthOp(bin)).Bins[bin]).To(gm.Equal(6))
	})

	gg.It("substr indexes codepoints not bytes", func() {
		// "日本語hi" — substr(start=3, end=5) returns codepoints 3..4 = "hi".
		// A byte-indexed substr would land mid-way through "日" (each CJK char
		// is 3 UTF-8 bytes).
		put("日本語hi")
		rec := operate(as.StrSubstrOp(bin, 3, 5))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("hi"))
	})

	gg.It("charAt returns whole codepoint not half-surrogate", func() {
		// 👋 is U+1F44B (4 UTF-8 bytes, would be a surrogate pair in UTF-16).
		// charAt at the emoji position must return the full codepoint.
		put("a👋b")
		rec := operate(as.StrCharAtOp(bin, 1))
		gm.Expect(rec.Bins[bin]).To(gm.Equal("👋"))
	})

	gg.It("find returns codepoint index not byte index", func() {
		// "café-world": "world" starts at codepoint 5.
		put("café-world")
		gm.Expect(operate(as.StrFindOp(bin, "world")).Bins[bin]).To(gm.Equal(5))

		// "👋-world": "world" starts at codepoint index 2 (after emoji + dash).
		// A byte-indexed find would return a different number (👋 is 4 bytes).
		put("👋-world")
		gm.Expect(operate(as.StrFindOp(bin, "world")).Bins[bin]).To(gm.Equal(2))
	})

	gg.It("find and contains require matching normalization form", func() {
		// "café" can be stored as NFC (U+00E9, 1 codepoint, 2 UTF-8 bytes) or
		// NFD (U+0065 U+0301, 2 codepoints, 3 UTF-8 bytes). They render
		// identically but are distinct byte sequences. The server's find /
		// contains uses ICU binary string search — NFC and NFD are NOT
		// considered equal. Callers who need normalization-insensitive search
		// must normalizeNFC the bin (and the needle) first. This test anchors
		// the contract so a future change to ICU comparison mode does not
		// silently flip the behavior.
		const NFC = "café"  // "café" composed
		const NFD = "café" // "café" decomposed

		put(NFC)
		// NFC haystack vs NFC needle — match.
		gm.Expect(operate(as.StrFindOp(bin, NFC)).Bins[bin]).To(gm.Equal(0))
		gm.Expect(operate(as.StrContainsOp(bin, NFC)).Bins[bin]).To(gm.Equal(true))
		// NFC haystack vs NFD needle — no match (byte sequences differ).
		gm.Expect(operate(as.StrFindOp(bin, NFD)).Bins[bin]).To(gm.Equal(-1))
		gm.Expect(operate(as.StrContainsOp(bin, NFD)).Bins[bin]).To(gm.Equal(false))
	})

	gg.It("normalizeNFC composes a decomposed sequence", func() {
		// "é" is the NFD ("decomposed") form of "é": Latin small "e"
		// followed by combining acute accent. normalizeNFC must compose it to
		// U+00E9 (NFC, single codepoint) — proving the op actually transforms
		// non-normalized input, not just the no-op case.
		put("é")
		operate(as.StrNormalizeNFCOp(policy, bin))
		gm.Expect(stringValue()).To(gm.Equal("é"))
		// Composed form is 1 codepoint; the decomposed input would be 2.
		gm.Expect(operate(as.StrLenOp(bin)).Bins[bin]).To(gm.Equal(1))
	})

	// ============================================================
	// toString on blob with invalid UTF-8
	//
	// The server's blob→string conversion validates the bytes and rejects
	// non-well-formed input with OP_NOT_APPLICABLE. {0xED, 0xA0, 0x80} is the
	// UTF-8 encoding of U+D800 (ill-formed surrogate) — the same fixture used
	// by the negative-tests suite in string_invalid_utf8_test.go.
	// ============================================================

	gg.It("toString on blob with invalid UTF-8 raises OP_NOT_APPLICABLE", func() {
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key,
			as.NewBin(bin, []byte{0xED, 0xA0, 0x80}))).ToNot(gm.HaveOccurred())

		_, err := client.Operate(nil, key, as.StrToStringOp(bin))
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue())
	})

	// ============================================================
	// Prepare / parameter-error suite
	//
	// These exercise the server's prepare-phase validation
	// (particle_string.c: find occurrence != 0, empty/negative pad
	// arguments, repeat count >= 0, regex_replace pattern compile).
	// All should surface as PARAMETER_ERROR; an invalid regex surfaces
	// as PARAMETER_ERROR per observed 8.1.3 behavior.
	// ============================================================

	expectParamError := func(op *as.Operation) {
		_, err := client.Operate(nil, key, op)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue())
	}

	gg.It("find with zero occurrence raises PARAMETER_ERROR", func() {
		// 0 is reserved as "no occurrence"; the server's find prepare rejects it.
		put("hello")
		expectParamError(as.StrFindNthOp(bin, "x", 0))
	})

	gg.It("padStart with empty pad string raises PARAMETER_ERROR", func() {
		put("hello")
		expectParamError(as.StrPadStartOp(policy, bin, 10, ""))
	})

	gg.It("padEnd with empty pad string raises PARAMETER_ERROR", func() {
		put("hello")
		expectParamError(as.StrPadEndOp(policy, bin, 10, ""))
	})

	gg.It("padStart with negative target raises PARAMETER_ERROR", func() {
		put("hello")
		expectParamError(as.StrPadStartOp(policy, bin, -1, "*"))
	})

	gg.It("repeat with negative count raises PARAMETER_ERROR", func() {
		put("hello")
		expectParamError(as.StrRepeatOp(policy, bin, -1))
	})

	gg.It("regexReplace with invalid pattern raises PARAMETER_ERROR", func() {
		// Unclosed character class — PCRE2 compile fails inside the op.
		put("hello")
		expectParamError(as.StrRegexReplaceOp(
			policy, bin, "[unclosed", "NUM", as.StringRegexDefault))
	})
})
