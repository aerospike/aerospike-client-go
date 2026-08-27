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
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Live-server verification of the (ResultCode, SubCode) pairs the client
// publishes as the SubCode* constants in the types package. That catalogue is a
// hand-maintained mirror of the server's per-status subcode enums
// (as/include/base/proto.h); these tests pin
// the pairs that are reachable from a plain Go client (CDT / HLL / bitwise ops)
// so the mirror can't drift silently.
//
// The verbosity_test / exp_error_detail suites already cover a first slice of
// subcodes (CDT index/rank, HLL fold-index-too-large, bit offset/size, HLL
// cannot-create). This suite extends that to the remaining reachable subcodes.
//
// Subcodes that need cluster state (PARTITION_UNAVAILABLE), config
// (FAIL_FORBIDDEN stop-writes / durability), concurrency (MRT_BLOCKED) or ACL are
// out of reach here and intentionally not covered.
//
// Trigger recipes are grounded in the server emit sites: particle_blob.c:1605
// (bits resize), particle_list.c:3068 (bounded insert), particle_hll.c:1145-1373
// (HLL prepare paths), particle_string.c (b64 decode). Requires an 8.1.3+ server.
var _ = gg.Describe("ErrorDetail subcode catalogue (integration)", func() {
	const edsBin = "eds-bin"

	var set string

	verbosityWP := func() *as.WritePolicy {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2
		return wp
	}

	// makeSketch builds a standalone HLL sketch with the given parameters on a
	// throwaway key and returns its serialized value, for use as a union /
	// intersect input. minHashBits = -1 yields a plain (minhash=0) sketch.
	makeSketch := func(indexBits, minHashBits int) as.HLLValue {
		k, _ := as.NewKey(*namespace, set, "eds-sketch-"+randString(12))
		_, err := client.Operate(as.NewWritePolicy(0, 0), k,
			as.HLLAddOp(as.DefaultHLLPolicy(), edsBin,
				[]as.Value{as.NewValue("a"), as.NewValue("b")}, indexBits, minHashBits))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		rec, err := client.Operate(as.NewWritePolicy(0, 0), k,
			as.GetBinOp(edsBin), as.HLLGetCountOp(edsBin))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		results := rec.Bins[edsBin].(as.OpResults)
		return results[0].(as.HLLValue)
	}

	gg.BeforeEach(func() {
		nodes := client.GetNodes()
		if len(nodes) == 0 {
			gg.Skip("no nodes available")
		}
		serverVersion := nodes[0].GetServerVersion()
		if serverVersion.IsSmaller(version.ServerVersion_8_1_3) {
			gg.Skip("Extended error-detail requires server version 8.1.3 or later; got " + serverVersion.String())
		}
		set = randString(20)
	})

	// -----------------------------------------------------------
	// High-confidence single-bin triggers.
	// -----------------------------------------------------------

	gg.It("bit resize beyond max blob size: PARAM_BITS_RESIZE_EXCEEDED subcode", func() {
		key, _ := as.NewKey(*namespace, set, "eds-bits-resize-key")
		err := client.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin(edsBin, []byte{0x01, 0x02}))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// PROTO_SIZE_MAX = 128 MiB; the check is on the resulting size (>=).
		_, err = client.Operate(verbosityWP(), key,
			as.BitResizeOp(as.DefaultBitPolicy(), edsBin, 128*1024*1024, as.BitResizeFlagsDefault))
		assertSubcode(err, types.PARAMETER_ERROR, types.SubCodeParamBitsResizeExceeded)
	})

	gg.It("bounded list insert past end: CDT_BOUNDED_LIST_OVERFLOW subcode", func() {
		key, _ := as.NewKey(*namespace, set, "eds-bounded-list-key")
		err := client.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin(edsBin, []interface{}{10, 20, 30}))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// ele_count = 3, valid insert indices 0..3; index 5 overflows a bounded list.
		boundedPolicy := as.NewListPolicy(as.ListOrderUnordered, as.ListWriteFlagsInsertBounded)
		_, err = client.Operate(verbosityWP(), key,
			as.ListInsertWithPolicyOp(boundedPolicy, edsBin, 5, 99))
		assertSubcode(err, types.OP_NOT_APPLICABLE, types.SubCodeOpNotCDTBoundedListOverflow)
	})

	gg.It("b64Decode on a non-base64 string: STRING_B64_INVALID subcode", func() {
		key, _ := as.NewKey(*namespace, set, "eds-str-b64-key")
		err := client.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin(edsBin, "not!valid!base64!"))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// Bad alphabet and a length that is not a multiple of 4.
		_, err = client.Operate(verbosityWP(), key, as.StrB64DecodeOp(edsBin))
		assertSubcode(err, types.OP_NOT_APPLICABLE, types.SubCodeOpNotStringB64Invalid)
	})

	gg.It("HLL add without index_bits on a new bin: HLL_INDEX_BITS_UNSET subcode", func() {
		key, _ := as.NewKey(*namespace, set, "eds-hll-unset-key")
		_, _ = client.Delete(as.NewWritePolicy(0, 0), key)

		// No existing sketch to inherit index_bits from, and index_bits left unset (-1).
		_, err := client.Operate(verbosityWP(), key,
			as.HLLAddOp(as.DefaultHLLPolicy(), edsBin, []as.Value{as.NewValue("x")}, -1, -1))
		assertSubcode(err, types.OP_NOT_APPLICABLE, types.SubCodeOpNotHLLIndexBitsUnset)
	})

	gg.It("HLL fold on a minhash sketch: HLL_CANNOT_FOLD_MINHASH subcode", func() {
		key, _ := as.NewKey(*namespace, set, "eds-hll-foldmh-key")
		_, _ = client.Delete(as.NewWritePolicy(0, 0), key)

		_, err := client.Operate(as.NewWritePolicy(0, 0), key,
			as.HLLInitOp(as.DefaultHLLPolicy(), edsBin, 12, 4)) // minhash_bits = 4 > 0
		gm.Expect(err).NotTo(gm.HaveOccurred())

		_, err = client.Operate(verbosityWP(), key, as.HLLFoldOp(edsBin, 8))
		assertSubcode(err, types.OP_NOT_APPLICABLE, types.SubCodeOpNotHLLCannotFoldMinhash)
	})

	// -----------------------------------------------------------
	// HLL union / intersect triggers (crafted input sketches).
	//
	// These follow the server emit-site recipes but exercise multi-sketch state;
	// worth a live-server smoke check to confirm they hit the intended subcode
	// rather than a parse-time PARAMETER error.
	// -----------------------------------------------------------

	gg.It("HLL union reducing index_bits without fold: HLL_CANNOT_REDUCE_INDEX_BITS subcode", func() {
		input := makeSketch(6, -1) // index_bits = 6

		key, _ := as.NewKey(*namespace, set, "eds-hll-reduceidx-key")
		_, _ = client.Delete(as.NewWritePolicy(0, 0), key)
		_, err := client.Operate(as.NewWritePolicy(0, 0), key,
			as.HLLInitOp(as.DefaultHLLPolicy(), edsBin, 12, -1)) // bin index_bits = 12
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// 12 > 6, default policy has no ALLOW_FOLD -> cannot reduce.
		_, err = client.Operate(verbosityWP(), key,
			as.HLLSetUnionOp(as.DefaultHLLPolicy(), edsBin, []as.HLLValue{input}))
		assertSubcode(err, types.OP_NOT_APPLICABLE, types.SubCodeOpNotHLLCannotReduceIndexBits)
	})

	gg.It("HLL union reducing minhash_bits without fold: HLL_CANNOT_REDUCE_MINHASH_BITS subcode", func() {
		input := makeSketch(10, 6) // same index_bits, minhash = 6

		key, _ := as.NewKey(*namespace, set, "eds-hll-reducemh-key")
		_, _ = client.Delete(as.NewWritePolicy(0, 0), key)
		_, err := client.Operate(as.NewWritePolicy(0, 0), key,
			as.HLLInitOp(as.DefaultHLLPolicy(), edsBin, 10, 4)) // bin minhash = 4
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// index_bits equal (10), minhash 4 != 6, no ALLOW_FOLD -> cannot reduce minhash.
		_, err = client.Operate(verbosityWP(), key,
			as.HLLSetUnionOp(as.DefaultHLLPolicy(), edsBin, []as.HLLValue{input}))
		assertSubcode(err, types.OP_NOT_APPLICABLE, types.SubCodeOpNotHLLCannotReduceMinhashBits)
	})

	gg.It("HLL intersect of 3 inputs with mismatched minhash: HLL_INTERSECT_MINHASH_MISMATCH subcode", func() {
		// 3+ inputs, internally consistent (same params) with non-zero minhash.
		s1 := makeSketch(10, 4)
		s2 := makeSketch(10, 4)
		s3 := makeSketch(10, 4)

		key, _ := as.NewKey(*namespace, set, "eds-hll-intersect-key")
		_, _ = client.Delete(as.NewWritePolicy(0, 0), key)
		_, err := client.Operate(as.NewWritePolicy(0, 0), key,
			as.HLLAddOp(as.DefaultHLLPolicy(), edsBin,
				[]as.Value{as.NewValue("z")}, 10, -1)) // bin minhash = 0
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// n_elements = 3 > 2; bin minhash (0) mismatches inputs' minhash (4).
		_, err = client.Operate(verbosityWP(), key,
			as.HLLGetIntersectCountOp(edsBin, []as.HLLValue{s1, s2, s3}))
		assertSubcode(err, types.OP_NOT_APPLICABLE, types.SubCodeOpNotHLLIntersectMinhashMismatch)
	})
})
