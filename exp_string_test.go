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

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Ported from the Java client's TestStringExp. Each test builds an Expression
// that wraps a StringExp.* call, evaluates it via an ExpReadOp into a virtual
// bin, and asserts the result.
//
// String expressions require server version 8.1.3+; the suite is skipped on
// older clusters via the standard Ginkgo version-check pattern documented in
// AI_PIPELINE.md.
//
// Several modify-expression tests are @Ignore'd in the Java suite because an
// older server SIGSEGV'd in particle_string.c on the expression-modify path.
// That bug is fixed on current server builds, so the Go suite runs them.
var _ = gg.Describe("String Expressions Test", func() {
	const bin = "sbin"
	const variable = "v"

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

	putRaw := func(b *as.Bin) {
		client.Delete(nil, key)
		err := client.PutBins(nil, key, b)
		gm.Expect(err).ToNot(gm.HaveOccurred())
	}

	eval := func(e *as.Expression) *as.Record {
		rec, err := client.Operate(nil, key,
			as.ExpReadOp(variable, e, as.ExpReadFlagDefault))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return rec
	}

	gg.BeforeEach(func() {
		requiredVersion, err := version.Parse("8.1.3")
		if err != nil {
			gg.Fail("Failed to parse server required version")
		}
		nodeVersion := client.GetNodes()[0].GetServerVersion()
		if nodeVersion.IsSmaller(requiredVersion) {
			gg.Skip("String expressions require server version 8.1.3+.")
			return
		}

		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	// ============================================================
	// Read expressions
	// ============================================================

	gg.It("strlen returns the codepoint count", func() {
		put("hello world")
		rec := eval(as.ExpStringLen(as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal(11))
	})

	gg.It("substr from offset and range", func() {
		put("hello world")
		r1 := eval(as.ExpStringSubstrFrom(as.ExpStringBin(bin), as.ExpIntVal(6)))
		gm.Expect(r1.Bins[variable]).To(gm.Equal("world"))
		r2 := eval(as.ExpStringSubstr(as.ExpStringBin(bin), as.ExpIntVal(0), as.ExpIntVal(5)))
		gm.Expect(r2.Bins[variable]).To(gm.Equal("hello"))
	})

	gg.It("charAt returns a single character", func() {
		put("Hello123World")
		rec := eval(as.ExpStringCharAt(as.ExpStringBin(bin), as.ExpIntVal(5)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("1"))
	})

	gg.It("find returns the index of the first and nth match", func() {
		put("ababab")
		r1 := eval(as.ExpStringFind(as.ExpStringBin(bin), as.ExpStringVal("ab")))
		gm.Expect(r1.Bins[variable]).To(gm.Equal(0))
		r2 := eval(as.ExpStringFindNth(as.ExpStringBin(bin), as.ExpStringVal("ab"), as.ExpIntVal(2)))
		gm.Expect(r2.Bins[variable]).To(gm.Equal(2))
	})

	gg.It("find skips overlapping matches", func() {
		// Self-overlapping needle "aa" in "aaaa": after match at 0, search
		// resumes at 2 — so the 2nd occurrence is at 2, not 1. Mirrors the
		// StrFindNthOp contract and ICU usearch behavior.
		put("aaaa")
		gm.Expect(eval(as.ExpStringFindNth(as.ExpStringBin(bin), as.ExpStringVal("aa"), as.ExpIntVal(1))).Bins[variable]).To(gm.Equal(0))
		gm.Expect(eval(as.ExpStringFindNth(as.ExpStringBin(bin), as.ExpStringVal("aa"), as.ExpIntVal(2))).Bins[variable]).To(gm.Equal(2))
		gm.Expect(eval(as.ExpStringFindNth(as.ExpStringBin(bin), as.ExpStringVal("aa"), as.ExpIntVal(3))).Bins[variable]).To(gm.Equal(-1))
	})

	gg.It("contains returns a boolean", func() {
		put("hello world")
		present := eval(as.ExpStringContains(as.ExpStringBin(bin), as.ExpStringVal("hello")))
		absent := eval(as.ExpStringContains(as.ExpStringBin(bin), as.ExpStringVal("xyz")))
		gm.Expect(present.Bins[variable]).To(gm.Equal(true))
		gm.Expect(absent.Bins[variable]).To(gm.Equal(false))
	})

	gg.It("startsWith matches the prefix", func() {
		put("Hello123World")
		gm.Expect(eval(as.ExpStringStartsWith(as.ExpStringBin(bin), as.ExpStringVal("Hello"))).Bins[variable]).To(gm.Equal(true))
		gm.Expect(eval(as.ExpStringStartsWith(as.ExpStringBin(bin), as.ExpStringVal("World"))).Bins[variable]).To(gm.Equal(false))
	})

	gg.It("endsWith matches the suffix", func() {
		put("Hello123World")
		gm.Expect(eval(as.ExpStringEndsWith(as.ExpStringBin(bin), as.ExpStringVal("World"))).Bins[variable]).To(gm.Equal(true))
		gm.Expect(eval(as.ExpStringEndsWith(as.ExpStringBin(bin), as.ExpStringVal("Hello"))).Bins[variable]).To(gm.Equal(false))
	})

	gg.It("toInteger parses digits as int64", func() {
		put("12345")
		rec := eval(as.ExpStringToInteger(as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal(12345))
	})

	gg.It("toDouble parses decimal numbers", func() {
		put("3.14")
		rec := eval(as.ExpStringToDouble(as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.BeNumerically("~", 3.14, 0.001))
	})

	gg.It("byteLength returns the UTF-8 byte count", func() {
		put("hello")
		rec := eval(as.ExpStringByteLength(as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal(5))
	})

	gg.It("isNumeric matches by default and by numeric type", func() {
		put("12345")
		gm.Expect(eval(as.ExpStringIsNumeric(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(true))
		gm.Expect(eval(as.ExpStringIsNumericTyped(as.ExpStringBin(bin), as.StringNumericInt)).Bins[variable]).To(gm.Equal(true))
		put("3.14")
		gm.Expect(eval(as.ExpStringIsNumericTyped(as.ExpStringBin(bin), as.StringNumericInt)).Bins[variable]).To(gm.Equal(false))
		put("hello")
		gm.Expect(eval(as.ExpStringIsNumeric(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(false))
	})

	gg.It("isNumeric FLOAT filter requires a fractional digit", func() {
		put("3.14")
		gm.Expect(eval(as.ExpStringIsNumericTyped(as.ExpStringBin(bin), as.StringNumericFloat)).Bins[variable]).To(gm.Equal(true))
		put("5")
		gm.Expect(eval(as.ExpStringIsNumericTyped(as.ExpStringBin(bin), as.StringNumericFloat)).Bins[variable]).To(gm.Equal(false))
		gm.Expect(eval(as.ExpStringIsNumericTyped(as.ExpStringBin(bin),as.StringNumericAny)).Bins[variable]).To(gm.Equal(true))
		put("5.")
		gm.Expect(eval(as.ExpStringIsNumericTyped(as.ExpStringBin(bin), as.StringNumericFloat)).Bins[variable]).To(gm.Equal(false))
		put("1e5")
		gm.Expect(eval(as.ExpStringIsNumericTyped(as.ExpStringBin(bin), as.StringNumericFloat, )).Bins[variable]).To(gm.Equal(false))
		gm.Expect(eval(as.ExpStringIsNumericTyped(as.ExpStringBin(bin), as.StringNumericAny)).Bins[variable]).To(gm.Equal(false))
	})

	gg.It("isUpper and isLower distinguish case", func() {
		put("HELLO")
		gm.Expect(eval(as.ExpStringIsUpper(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(true))
		gm.Expect(eval(as.ExpStringIsLower(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(false))

		put("hello")
		gm.Expect(eval(as.ExpStringIsUpper(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(false))
		gm.Expect(eval(as.ExpStringIsLower(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(true))
	})

	gg.It("toBlob returns the UTF-8 bytes", func() {
		put("hello")
		rec := eval(as.ExpStringToBlob(as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal([]byte("hello")))
	})

	gg.It("split with and without separator", func() {
		put("one,two,three")
		r1 := eval(as.ExpStringSplitBySeparator(as.ExpStringBin(bin), as.ExpStringVal(",")))
		gm.Expect(r1.Bins[variable]).To(gm.Equal([]any{"one", "two", "three"}))

		put("abc")
		r2 := eval(as.ExpStringSplit(as.ExpStringBin(bin)))
		gm.Expect(r2.Bins[variable]).To(gm.Equal([]any{"a", "b", "c"}))
	})

	gg.It("b64Decode returns the decoded blob", func() {
		put("aGVsbG8=")
		rec := eval(as.ExpStringB64Decode(as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal([]byte("hello")))
	})

	gg.It("regexCompare with and without case-insensitive flag", func() {
		put("Hello123World")
		gm.Expect(eval(as.ExpStringRegexCompare(as.ExpStringBin(bin), as.ExpStringVal("[0-9]+"))).Bins[variable]).To(gm.Equal(true))

		put("HELLO")
		gm.Expect(eval(as.ExpStringRegexCompare(as.ExpStringBin(bin), as.ExpStringVal("hello"))).Bins[variable]).To(gm.Equal(false))
		gm.Expect(eval(as.ExpStringRegexCompareWithFlags(as.ExpStringBin(bin), as.ExpStringVal("hello"), as.StringRegexCaseInsensitive)).Bins[variable]).To(gm.Equal(true))
	})

	// ============================================================
	// Modify expressions (return the modified string; do not persist)
	// ============================================================
	//
	// Most of these mirror tests that are @Ignore'd in the Java suite because
	// an older server SIGSEGV'd in particle_string.c on the expression-modify
	// path. That bug is fixed on current server builds, so we run them.

	gg.It("insert splices into source", func() {
		put("hello world")
		rec := eval(as.ExpStringInsert(policy, as.ExpStringBin(bin), as.ExpIntVal(5), as.ExpStringVal(" beautiful")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello beautiful world"))
	})

	gg.It("overwrite replaces a range", func() {
		put("hello world")
		rec := eval(as.ExpStringOverwrite(policy, as.ExpStringBin(bin), as.ExpIntVal(6), as.ExpStringVal("earth")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello earth"))
	})

	gg.It("concat appends a list of values", func() {
		put("hello")
		values := as.ExpListValueVal(" ", "big", " world")
		rec := eval(as.ExpStringConcat(policy, as.ExpStringBin(bin), values))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello big world"))
	})

	gg.It("append adds value to end", func() {
		put("hello")
		rec := eval(as.ExpStringAppend(policy, as.ExpStringBin(bin), as.ExpStringVal(" world")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello world"))
	})

	gg.It("append preserves multibyte codepoints", func() {
		// Unicode/DBCS-aware: "日本" + "語" -> "日本語".
		put("日本")
		rec := eval(as.ExpStringAppend(policy, as.ExpStringBin(bin), as.ExpStringVal("語")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("日本語"))
	})

	gg.It("prepend adds value to start", func() {
		put("world")
		rec := eval(as.ExpStringPrepend(policy, as.ExpStringBin(bin), as.ExpStringVal("hello ")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello world"))
	})

	gg.It("prepend preserves multibyte codepoints", func() {
		// Unicode/DBCS-aware: "語" prepended with "日本" -> "日本語".
		put("語")
		rec := eval(as.ExpStringPrepend(policy, as.ExpStringBin(bin), as.ExpStringVal("日本")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("日本語"))
	})

	gg.It("snip removes range", func() {

		put("hello beautiful world")
		r := eval(as.ExpStringSnip(policy, as.ExpStringBin(bin), as.ExpIntVal(5), as.ExpIntVal(15)))
		gm.Expect(r.Bins[variable]).To(gm.Equal("hello world"))
	})

	gg.It("snip from truncates to the end of the string", func() {
		put("hello world")
		r := eval(as.ExpStringSnipFrom(policy, as.ExpStringBin(bin), as.ExpIntVal(5)))
		gm.Expect(r.Bins[variable]).To(gm.Equal("hello"))
	})

	gg.It("snip from leaves the underlying bin untouched", func() {
		put("hello world")
		r := eval(as.ExpStringSnipFrom(policy, as.ExpStringBin(bin), as.ExpIntVal(5)))
		gm.Expect(r.Bins[variable]).To(gm.Equal("hello"))

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[bin]).To(gm.Equal("hello world"))
	})

	gg.It("snip from negative index counts from the end", func() {
		put("hello world")
		r := eval(as.ExpStringSnipFrom(policy, as.ExpStringBin(bin), as.ExpIntVal(-5)))
		gm.Expect(r.Bins[variable]).To(gm.Equal("hello "))
	})

	gg.It("replace touches only the first match", func() {
		put("hello world world")
		rec := eval(as.ExpStringReplace(policy, as.ExpStringBin(bin), as.ExpStringVal("world"), as.ExpStringVal("earth")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello earth world"))
	})

	gg.It("replaceAll substitutes every match", func() {
		put("aabaa")
		rec := eval(as.ExpStringReplaceAll(policy, as.ExpStringBin(bin), as.ExpStringVal("a"), as.ExpStringVal("x")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("xxbxx"))
	})

	gg.It("upper and lower produce correct case", func() {
		put("hello World")
		gm.Expect(eval(as.ExpStringUpper(policy, as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal("HELLO WORLD"))
		gm.Expect(eval(as.ExpStringLower(policy, as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal("hello world"))
	})

	gg.It("caseFold lowercases independently of locale", func() {
		put("HELLO World")
		rec := eval(as.ExpStringCaseFold(policy, as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello world"))
	})

	gg.It("normalizeNFC leaves already-normalized string unchanged", func() {
		put("hello")
		rec := eval(as.ExpStringNormalizeNFC(policy, as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello"))
	})

	gg.It("trim variants strip appropriate edges", func() {
		put("  hello world  ")
		gm.Expect(eval(as.ExpStringTrim(policy, as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal("hello world"))
		gm.Expect(eval(as.ExpStringTrimStart(policy, as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal("hello world  "))
		gm.Expect(eval(as.ExpStringTrimEnd(policy, as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal("  hello world"))
	})

	gg.It("padStart fills left to target length", func() {
		put("hello")
		rec := eval(as.ExpStringPadStart(policy, as.ExpStringBin(bin), as.ExpIntVal(10), as.ExpStringVal("*")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("*****hello"))
	})

	gg.It("padEnd fills right to target length", func() {
		put("hello")
		rec := eval(as.ExpStringPadEnd(policy, as.ExpStringBin(bin), as.ExpIntVal(10), as.ExpStringVal(".")))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello....."))
	})

	gg.It("repeat duplicates contents", func() {
		put("ab")
		rec := eval(as.ExpStringRepeat(policy, as.ExpStringBin(bin), as.ExpIntVal(3)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("ababab"))
	})

	gg.It("regexReplace first and global", func() {
		put("abc123def456")
		r1 := eval(as.ExpStringRegexReplace(policy, as.ExpStringBin(bin), as.ExpStringVal("[0-9]+"), as.ExpStringVal("NUM"), as.StringRegexDefault))
		gm.Expect(r1.Bins[variable]).To(gm.Equal("abcNUMdef456"))

		r2 := eval(as.ExpStringRegexReplace(policy, as.ExpStringBin(bin), as.ExpStringVal("[0-9]+"), as.ExpStringVal("NUM"), as.StringRegexGlobal))
		gm.Expect(r2.Bins[variable]).To(gm.Equal("abcNUMdefNUM"))
	})

	// ============================================================
	// Type conversion expression
	// ============================================================

	gg.It("toString converts an integer bin", func() {
		putRaw(as.NewBin(bin, 42))
		rec := eval(as.ExpStringToString(as.ExpIntBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("42"))
	})

	// ============================================================
	// Chained expressions — modify result feeds another StringExp
	// ============================================================

	gg.It("chained trim then upper composes", func() {
		put("  hello world  ")
		chain := as.ExpStringUpper(policy,
			as.ExpStringTrim(policy, as.ExpStringBin(bin)))
		rec := eval(chain)
		gm.Expect(rec.Bins[variable]).To(gm.Equal("HELLO WORLD"))
	})

	// ============================================================
	// Filter-expression usage — predicate gates record retrieval
	// ============================================================

	gg.It("startsWith filter gates Get", func() {
		put("hello world")
		p := as.NewPolicy()

		// In the Go client, *Expression values can be passed directly as a
		// filter — there is no separate Build step.
		p.FilterExpression = as.ExpStringStartsWith(as.ExpStringBin(bin), as.ExpStringVal("hello"))
		rec, err := client.Get(p, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[bin]).To(gm.Equal("hello world"))

		p.FilterExpression = as.ExpStringStartsWith(as.ExpStringBin(bin), as.ExpStringVal("world"))
		_, err = client.Get(p, key)
		// Server returns FILTERED_OUT; the client surfaces it as an error.
		gm.Expect(err).To(gm.HaveOccurred())
	})

	// ============================================================
	// Nested-source — string inside a list projected via List getter
	// ============================================================

	gg.It("strlen on string nested in list projected via ExpListGetByIndex", func() {
		list := []any{"alpha", "beta", "hello world"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, list))).ToNot(gm.HaveOccurred())

		nested := as.ExpListGetByIndex(
			as.ListReturnTypeValue, as.ExpTypeSTRING, as.ExpIntVal(2), as.ExpListBin(bin))
		rec := eval(as.ExpStringLen(nested))
		gm.Expect(rec.Bins[variable]).To(gm.Equal(11))
	})

	gg.It("upper on string nested in map projected via ExpMapGetByKey", func() {
		m := map[any]any{"a": "hello", "b": "world"}
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, m))).ToNot(gm.HaveOccurred())

		nested := as.ExpMapGetByKey(
			as.MapReturnType.VALUE, as.ExpTypeSTRING, as.ExpStringVal("a"), as.ExpMapBin(bin))
		rec := eval(as.ExpStringUpper(policy, nested))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("HELLO"))
	})

	// ============================================================
	// Write flags (CLIENT-5361)
	// ============================================================

	gg.It("CREATE_ONLY with NO_FAIL evaluates to the unmodified source string", func() {
		// A suppressed modify restores the original particle, so the expression
		// evaluates to the source string — not nil.
		put("hello")

		coNoFail := as.NewStringPolicy(as.StringWriteCreateOnly | as.StringWriteNoFail)
		rec := eval(as.ExpStringAppend(coNoFail, as.ExpStringVal(" there"), as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello"))
	})

	gg.It("NO_FAIL suppressing a prepare failure evaluates to the source string", func() {
		put("hello")

		noFail := as.NewStringPolicy(as.StringWriteNoFail)
		rec := eval(as.ExpStringPadStart(noFail, as.ExpIntVal(10), as.ExpStringVal(""), as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello"))
	})

	gg.It("UPDATE_ONLY applies to an existing source", func() {
		put("hello")

		updateOnly := as.NewStringPolicy(as.StringWriteUpdateOnly)
		rec := eval(as.ExpStringAppend(updateOnly, as.ExpStringVal(" there"), as.ExpStringBin(bin)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("hello there"))
	})

	gg.It("CREATE_ONLY on an existing source fails", func() {
		put("hello")

		createOnly := as.NewStringPolicy(as.StringWriteCreateOnly)
		_, err := client.Operate(nil, key, as.ExpReadOp(variable,
			as.ExpStringAppend(createOnly, as.ExpStringVal(" there"), as.ExpStringBin(bin)),
			as.ExpReadFlagDefault))
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("CREATE_ONLY combined with UPDATE_ONLY fails", func() {
		put("hello")

		both := as.NewStringPolicy(as.StringWriteCreateOnly | as.StringWriteUpdateOnly)
		_, err := client.Operate(nil, key, as.ExpReadOp(variable,
			as.ExpStringAppend(both, as.ExpStringVal(" there"), as.ExpStringBin(bin)),
			as.ExpReadFlagDefault))
		gm.Expect(err).To(gm.HaveOccurred())
	})

	// ============================================================
	// Codepoint-vs-byte anchors (mirror of cdt_string_test.go)
	// ============================================================

	gg.It("strlen counts codepoints and byteLength counts bytes", func() {
		// "café" = 4 codepoints, 5 UTF-8 bytes.
		put("café")
		gm.Expect(eval(as.ExpStringLen(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(4))
		gm.Expect(eval(as.ExpStringByteLength(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(5))

		// "日本語" = 3 codepoints, 9 UTF-8 bytes.
		put("日本語")
		gm.Expect(eval(as.ExpStringLen(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(3))
		gm.Expect(eval(as.ExpStringByteLength(as.ExpStringBin(bin))).Bins[variable]).To(gm.Equal(9))
	})

	gg.It("charAt returns whole supplementary codepoint", func() {
		// 👋 is U+1F44B (4 UTF-8 bytes). charAt must return the whole
		// codepoint, not a half-surrogate or a byte.
		put("a👋b")
		rec := eval(as.ExpStringCharAt(as.ExpStringBin(bin), as.ExpIntVal(1)))
		gm.Expect(rec.Bins[variable]).To(gm.Equal("👋"))
	})
})
