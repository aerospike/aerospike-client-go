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
	"errors"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Three byte sequences that are not valid UTF-8:
//   - lone 0xFF — never legal anywhere in UTF-8
//   - unpaired UTF-16 surrogate U+D800 encoded as 0xED 0xA0 0x80
//   - truncated multi-byte start (0xC2 expects a continuation byte)
var (
	badRawBytes  = []byte{0xff, 0xfe, 0xfd}
	badSurrogate = []byte{0xed, 0xa0, 0x80}
	badTruncated = []byte{0xc2}
)

var _ = gg.Describe("UTF-8 Write Validation", gg.Ordered, func() {
	const bin = "vbin"

	var (
		ns           = *namespace
		set          = randString(50)
		key          *as.Key
		strictClient *as.Client // ValidateUTF8 = true
	)

	gg.BeforeAll(func() {
		// Build a second client that opts in to UTF-8 validation. The
		// suite-wide `client` keeps the legacy default (off) so existing
		// tests are unaffected.
		p := *clientPolicy
		p.ValidateUTF8 = true
		c, err := as.NewClientWithPolicy(&p, *host, *port)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		strictClient = c
	})

	gg.AfterAll(func() {
		if strictClient != nil {
			strictClient.Close()
		}
	})

	gg.BeforeEach(func() {
		var err as.Error
		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		client.Delete(nil, key)
	})

	expectParameterError := func(err as.Error) {
		gm.Expect(err).To(gm.HaveOccurred())
		ae := &as.AerospikeError{}
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	}

	// ============================================================
	// Default policy (ValidateUTF8 off) is unchanged: non-UTF-8
	// strings round-trip without error. This guards backward
	// compatibility — applications relying on the existing behavior
	// must keep working.
	// ============================================================

	gg.It("default policy round-trips non-UTF-8 bytes (backward compatibility)", func() {
		nonUTF8 := string(badRawBytes)
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, nonUTF8))).ToNot(gm.HaveOccurred())
		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins[bin]).To(gm.Equal(nonUTF8))
	})

	// ============================================================
	// Opt-in policy rejects invalid UTF-8 in every Value shape we
	// care about. Each test confirms the error is PARAMETER_ERROR
	// and is raised before the wire send (no record is created).
	// ============================================================

	gg.It("opt-in rejects invalid UTF-8 in a plain string bin (raw bytes)", func() {
		err := strictClient.PutBins(nil, key, as.NewBin(bin, string(badRawBytes)))
		expectParameterError(err)
		// Failure occurred pre-send: no record exists.
		_, gerr := client.Get(nil, key)
		gm.Expect(gerr).To(gm.HaveOccurred())
	})

	gg.It("opt-in rejects invalid UTF-8 in a plain string bin (surrogate)", func() {
		err := strictClient.PutBins(nil, key, as.NewBin(bin, string(badSurrogate)))
		expectParameterError(err)
	})

	gg.It("opt-in rejects invalid UTF-8 in a plain string bin (truncated)", func() {
		err := strictClient.PutBins(nil, key, as.NewBin(bin, string(badTruncated)))
		expectParameterError(err)
	})

	gg.It("opt-in rejects invalid UTF-8 nested in a list value", func() {
		list := []any{"ok", "fine", string(badRawBytes), "also fine"}
		err := strictClient.PutBins(nil, key, as.NewBin(bin, list))
		expectParameterError(err)
	})

	gg.It("opt-in rejects invalid UTF-8 nested in a map value", func() {
		m := map[any]any{"good": "value", "bad": string(badRawBytes)}
		err := strictClient.PutBins(nil, key, as.NewBin(bin, m))
		expectParameterError(err)
	})

	gg.It("opt-in rejects invalid UTF-8 in a map key", func() {
		m := map[any]any{string(badRawBytes): "value", "good": "value"}
		err := strictClient.PutBins(nil, key, as.NewBin(bin, m))
		expectParameterError(err)
	})

	gg.It("opt-in rejects invalid UTF-8 inside a deeply nested list-in-map", func() {
		inner := []any{"ok", string(badRawBytes)}
		m := map[any]any{"k": inner}
		err := strictClient.PutBins(nil, key, as.NewBin(bin, m))
		expectParameterError(err)
	})

	gg.It("opt-in rejects invalid UTF-8 in a BinMap value (Put)", func() {
		err := strictClient.Put(nil, key, as.BinMap{bin: string(badRawBytes)})
		expectParameterError(err)
	})

	gg.It("opt-in rejects invalid UTF-8 in Append/Prepend", func() {
		gm.Expect(client.PutBins(nil, key, as.NewBin(bin, "seed"))).ToNot(gm.HaveOccurred())
		err := strictClient.AppendBins(nil, key, as.NewBin(bin, string(badRawBytes)))
		expectParameterError(err)
		err = strictClient.PrependBins(nil, key, as.NewBin(bin, string(badRawBytes)))
		expectParameterError(err)
	})

	gg.It("opt-in rejects invalid UTF-8 in an Operate PutOp", func() {
		_, err := strictClient.Operate(nil, key, as.PutOp(as.NewBin(bin, string(badRawBytes))))
		expectParameterError(err)
	})

	// ============================================================
	// Opt-in policy must still pass valid UTF-8 — including emoji
	// and other multi-byte sequences.
	// ============================================================

	gg.It("opt-in accepts pure ASCII", func() {
		gm.Expect(strictClient.PutBins(nil, key, as.NewBin(bin, "hello world"))).ToNot(gm.HaveOccurred())
	})

	gg.It("opt-in accepts emoji and non-ASCII UTF-8", func() {
		err := strictClient.PutBins(nil, key,
			as.NewBin("a", "café"),
			as.NewBin("b", "日本語"),
			as.NewBin("c", "🎉🚀"),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["a"]).To(gm.Equal("café"))
		gm.Expect(rec.Bins["b"]).To(gm.Equal("日本語"))
		gm.Expect(rec.Bins["c"]).To(gm.Equal("🎉🚀"))
	})

	gg.It("opt-in accepts mixed UTF-8 inside lists and maps", func() {
		err := strictClient.PutBins(nil, key,
			as.NewBin("list", []any{"a", "café", "日本語", "🎉"}),
			as.NewBin("map", map[any]any{"k1": "v1", "café": "日本語"}),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})
})
