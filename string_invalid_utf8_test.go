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
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Ported from the Java client's TestStringInvalidUtf8.
//
// Every read and modify op in cdt_string.go must reject a string bin whose
// stored bytes are not well-formed UTF-8. The server's as_bin_string_read /
// as_bin_string_modify entry helpers run utf8_string_length on the bin before
// dispatching to the op-specific code, returning AS_ERR_INVALID_ENCODING
// (ast.INVALID_ENCODING).
//
// To plant raw invalid bytes in a string-typed bin we wrap the bytes in a
// RawBlobValue tagged with ParticleType.STRING — this writes the bytes
// verbatim under the STRING particle type, bypassing any client-side UTF-8
// sanitization.
//
// The fixture BAD = {0xED, 0xA0, 0x80} is the UTF-8 encoding of U+D800
// (ill-formed surrogate), the same fixture used by the server's EntryParityUtf8
// unit tests and the Java client's TestStringInvalidUtf8 suite.
var _ = gg.Describe("String Invalid UTF-8 Tests", func() {
	const bin = "sbin"

	var (
		ns  = *namespace
		set = randString(50)
		key *as.Key
	)

	policy := as.DefaultStringPolicy

	// Ill-formed UTF-8: 3-byte encoding of U+D800 (surrogate).
	badBytes := []byte{0xED, 0xA0, 0x80}

	plantInvalidBin := func() {
		client.Delete(nil, key)
		err := client.PutBins(nil, key,
			as.NewBin(bin, as.NewRawBlobValue(ParticleType.STRING, badBytes)))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	}

	assertInvalidEncoding := func(op *as.Operation) {
		_, err := client.Operate(nil, key, op)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.INVALID_ENCODING)).To(gm.BeTrue(),
			"expected INVALID_ENCODING, got: %v", err)
	}

	gg.BeforeEach(func() {
		requiredVersion, err := version.Parse("8.1.2")
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
		plantInvalidBin()
	})

	// ============================================================
	// Read ops — bin gate fires before op-specific logic.
	// ============================================================

	gg.It("strlen rejects invalid bin", func() {
		assertInvalidEncoding(as.StrLenOp(bin))
	})

	gg.It("substr rejects invalid bin", func() {
		assertInvalidEncoding(as.StrSubstrFromOp(bin, 0))
	})

	gg.It("charAt rejects invalid bin", func() {
		assertInvalidEncoding(as.StrCharAtOp(bin, 0))
	})

	gg.It("find rejects invalid bin", func() {
		assertInvalidEncoding(as.StrFindOp(bin, "x"))
	})

	gg.It("contains rejects invalid bin", func() {
		assertInvalidEncoding(as.StrContainsOp(bin, "x"))
	})

	gg.It("startsWith rejects invalid bin", func() {
		assertInvalidEncoding(as.StrStartsWithOp(bin, "x"))
	})

	gg.It("endsWith rejects invalid bin", func() {
		assertInvalidEncoding(as.StrEndsWithOp(bin, "x"))
	})

	gg.It("toInteger rejects invalid bin", func() {
		assertInvalidEncoding(as.StrToIntegerOp(bin))
	})

	gg.It("toDouble rejects invalid bin", func() {
		assertInvalidEncoding(as.StrToDoubleOp(bin))
	})

	// byte_length, to_blob, b64_decode, trim*, repeat, concat are listed in
	// the 8.1.3 client report as "unaffected" by UTF-8, but per the doc's §3
	// and §11 they hit the same bin gate as strlen and must also reject.
	gg.It("byteLength rejects invalid bin", func() {
		assertInvalidEncoding(as.StrByteLengthOp(bin))
	})

	gg.It("isNumeric rejects invalid bin", func() {
		assertInvalidEncoding(as.StrIsNumericOp(bin))
	})

	gg.It("isUpper rejects invalid bin", func() {
		assertInvalidEncoding(as.StrIsUpperOp(bin))
	})

	gg.It("isLower rejects invalid bin", func() {
		assertInvalidEncoding(as.StrIsLowerOp(bin))
	})

	gg.It("toBlob rejects invalid bin", func() {
		assertInvalidEncoding(as.StrToBlobOp(bin))
	})

	gg.It("split rejects invalid bin", func() {
		assertInvalidEncoding(as.StrSplitBySeparatorOp(bin, ","))
	})

	gg.It("b64Decode rejects invalid bin", func() {
		assertInvalidEncoding(as.StrB64DecodeOp(bin))
	})

	gg.It("regexCompare rejects invalid bin", func() {
		assertInvalidEncoding(as.StrRegexCompareOp(bin, "x"))
	})

	// ============================================================
	// Modify ops — bin gate also fires here; bin must remain unchanged.
	//
	// We can't easily verify "bin bytes unchanged" via client.Get because the
	// client decodes STRING particles through Go's native UTF-8 handling;
	// the raw bytes are not recoverable through the public client surface.
	// The fact that a subsequent strlen on the same bin still hits
	// INVALID_ENCODING (see failedModifyDoesNotOverwriteBin below) proves
	// the failed modify did not replace the bin with a well-formed value.
	// ============================================================

	gg.It("insert rejects invalid bin", func() {
		assertInvalidEncoding(as.StrInsertOp(policy, bin, 0, "x"))
	})

	gg.It("overwrite rejects invalid bin", func() {
		assertInvalidEncoding(as.StrOverwriteOp(policy, bin, 0, "x"))
	})

	gg.It("concat rejects invalid bin", func() {
		assertInvalidEncoding(as.StrConcatOp(policy, bin, "x"))
	})

	gg.It("snip rejects invalid bin", func() {
		assertInvalidEncoding(as.StrSnipOp(policy, bin, 0, 1))
	})

	gg.It("replace rejects invalid bin", func() {
		assertInvalidEncoding(as.StrReplaceOp(policy, bin, "x", "y"))
	})

	gg.It("replaceAll rejects invalid bin", func() {
		assertInvalidEncoding(as.StrReplaceAllOp(policy, bin, "x", "y"))
	})

	gg.It("upper rejects invalid bin", func() {
		assertInvalidEncoding(as.StrUpperOp(policy, bin))
	})

	gg.It("lower rejects invalid bin", func() {
		assertInvalidEncoding(as.StrLowerOp(policy, bin))
	})

	gg.It("caseFold rejects invalid bin", func() {
		assertInvalidEncoding(as.StrCaseFoldOp(policy, bin))
	})

	gg.It("normalizeNFC rejects invalid bin", func() {
		assertInvalidEncoding(as.StrNormalizeNFCOp(policy, bin))
	})

	gg.It("trimStart rejects invalid bin", func() {
		assertInvalidEncoding(as.StrTrimStartOp(policy, bin))
	})

	gg.It("trimEnd rejects invalid bin", func() {
		assertInvalidEncoding(as.StrTrimEndOp(policy, bin))
	})

	gg.It("trim rejects invalid bin", func() {
		assertInvalidEncoding(as.StrTrimOp(policy, bin))
	})

	gg.It("padStart rejects invalid bin", func() {
		assertInvalidEncoding(as.StrPadStartOp(policy, bin, 10, "*"))
	})

	gg.It("padEnd rejects invalid bin", func() {
		assertInvalidEncoding(as.StrPadEndOp(policy, bin, 10, "*"))
	})

	gg.It("repeat rejects invalid bin", func() {
		assertInvalidEncoding(as.StrRepeatOp(policy, bin, 2))
	})

	gg.It("regexReplace rejects invalid bin", func() {
		assertInvalidEncoding(as.StrRegexReplaceOp(
			policy, bin, "x", "y", as.StringRegexDefault))
	})

	// ============================================================
	// Post-failure invariant
	// ============================================================

	gg.It("failed modify does not overwrite bin", func() {
		// First modify attempt must fail with INVALID_ENCODING.
		assertInvalidEncoding(as.StrUpperOp(policy, bin))
		// A subsequent read on the same bin must also fail at the gate,
		// proving the bin still holds the original invalid bytes (the failed
		// modify did not replace it with a well-formed value).
		assertInvalidEncoding(as.StrLenOp(bin))
	})

	// ============================================================
	// Server-side string-arg gate
	//
	// The Java client throws client-side on a "\uD800" needle via the
	// unconditional Utf8.encodedLength gate. The Go client only enforces
	// arg UTF-8 when ClientPolicy.ValidateUTF8 is set — and even then the
	// validator does not recurse into RawBlobValue (the pre-packed payload
	// the StrXxxOp builders produce). So Go relies on the server's "invalid
	// arg" gate.
	//
	// Note: the server distinguishes bin-side invalid UTF-8 (INVALID_ENCODING)
	// from arg-side invalid UTF-8 (PARAMETER_ERROR). Both reject the operation
	// before any wire data leaves the gate; only the surfaced result code
	// differs.
	// ============================================================

	gg.It("invalid UTF-8 needle is rejected by server", func() {
		// Replace the planted invalid bin with a well-formed string so the
		// bin gate doesn't short-circuit; the rejection must come from the
		// arg gate.
		client.Delete(nil, key)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, "hello"))).
			ToNot(gm.HaveOccurred())

		// "\xED\xA0\x80" is the UTF-8 encoding of U+D800 (unpaired surrogate),
		// well-formed at the Go-string level (any byte sequence is a valid
		// Go string) but ill-formed as UTF-8. The server's arg-gate surfaces
		// PARAMETER_ERROR (not INVALID_ENCODING — that code is reserved for
		// bin-side failures).
		_, err := client.Operate(nil, key, as.StrFindOp(bin, "\xED\xA0\x80"))
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue(),
			"expected PARAMETER_ERROR for invalid-UTF-8 arg, got: %v", err)
	})
})
