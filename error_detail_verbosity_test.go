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
	"fmt"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Validates the extended error-detail feature (CLIENT-4221) against an
// 8.1.3+ Aerospike server. Skips when running against an older server.
var _ = gg.Describe("ErrorDetailVerbosity (integration)", func() {
	const edvBinName = "edv-bin"

	var (
		intKey    *as.Key
		strKey    *as.Key
		listKey   *as.Key
		set       string
		supported bool
	)

	gg.BeforeEach(func() {
		nodes := client.GetNodes()
		if len(nodes) == 0 {
			gg.Skip("no nodes available")
		}
		serverVersion := nodes[0].GetServerVersion()
		if serverVersion.IsSmaller(version.ServerVersion_8_1_1) {
			fmt.Println("skipping tests")
			gg.Skip("Extended error-detail requires server version 8.1.3 or later; got " + serverVersion.String())
		}
		supported = true

		set = randString(20)
		intKey, _ = as.NewKey(*namespace, set, "edv-int-key")
		strKey, _ = as.NewKey(*namespace, set, "edv-str-key")
		listKey, _ = as.NewKey(*namespace, set, "edv-list-key")

		wp := as.NewWritePolicy(0, 0)
		err := client.PutBins(wp, intKey, as.NewBin(edvBinName, 1))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		err = client.PutBins(wp, strKey, as.NewBin(edvBinName, "hello"))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		err = client.PutBins(wp, listKey, as.NewBin(edvBinName, []interface{}{10, 20, 30}))
		gm.Expect(err).NotTo(gm.HaveOccurred())
	})

	// -----------------------------------------------------------
	// Verbosity level semantics.
	// -----------------------------------------------------------

	gg.It("defaults verbosity to zero", func() {
		if !supported {
			return
		}
		p := as.NewPolicy()
		gm.Expect(p.ErrorDetailVerbosity).To(gm.Equal(0))

		wp := as.NewWritePolicy(0, 0)
		gm.Expect(wp.ErrorDetailVerbosity).To(gm.Equal(0))
	})

	gg.It("verbosity disabled: no server message", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 0

		_, err := client.Operate(wp, intKey, as.AppendOp(as.NewBin(edvBinName, "bad")))
		gm.Expect(err).To(gm.HaveOccurred())
		ae := &as.AerospikeError{}
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(types.BIN_TYPE_ERROR))
		gm.Expect(ae.SubCode).To(gm.Equal(as.SubCodeNone))
		gm.Expect(ae.ServerMessage).To(gm.Equal(""))
	})

	gg.It("verbosity subcode-only surfaces subcode without message text", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 1

		key, _ := as.NewKey(*namespace, set, "edv-subonly-key")
		err := client.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin("other-bin", 1))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		_, err = client.Operate(wp, key, as.HLLRefreshCountOp("no-hll-bin"))
		gm.Expect(err).To(gm.HaveOccurred())
		ae := &as.AerospikeError{}
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(types.BIN_NOT_FOUND))
		gm.Expect(ae.SubCode).To(gm.Equal(as.SubCodeBinNotFoundHLLCannotCreateWithOp))
		gm.Expect(ae.ServerMessage).To(gm.ContainSubstring("subcode=1"))
	})

	gg.It("verbosity subcode+message surfaces both", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		key, _ := as.NewKey(*namespace, set, "edv-submsg-key")
		err := client.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin("other-bin", 1))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		_, err = client.Operate(wp, key, as.HLLRefreshCountOp("no-hll-bin"))
		gm.Expect(err).To(gm.HaveOccurred())
		ae := &as.AerospikeError{}
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(types.BIN_NOT_FOUND))
		gm.Expect(ae.SubCode).To(gm.Equal(as.SubCodeBinNotFoundHLLCannotCreateWithOp))
		gm.Expect(ae.ServerMessage).To(gm.ContainSubstring("(subcode=1)"))
		gm.Expect(strings.ToLower(ae.ServerMessage)).To(gm.ContainSubstring("count op"))
	})

	// -----------------------------------------------------------
	// Subcode-absent cases (status already maximally specific).
	// -----------------------------------------------------------

	gg.It("append to integer bin: BIN_TYPE_ERROR with subcode absent", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		_, err := client.Operate(wp, intKey, as.AppendOp(as.NewBin(edvBinName, "bad-append")))
		assertSubcodeAbsent(err, types.BIN_TYPE_ERROR, "cannot append")
	})

	gg.It("increment string bin: BIN_TYPE_ERROR with subcode absent", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		_, err := client.Operate(wp, strKey, as.AddOp(as.NewBin(edvBinName, 1)))
		assertSubcodeAbsent(err, types.BIN_TYPE_ERROR, "cannot increment")
	})

	gg.It("HLL add on integer bin: BIN_TYPE_ERROR with subcode absent", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		_, err := client.Operate(wp, intKey,
			as.HLLAddOp(as.DefaultHLLPolicy(), edvBinName, []as.Value{as.NewValue("element1")}, 8, 0))
		assertSubcodeAbsent(err, types.BIN_TYPE_ERROR, "bin is not hll type")
	})

	gg.It("delete generation mismatch: GENERATION_ERROR with subcode absent", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2
		wp.GenerationPolicy = as.EXPECT_GEN_EQUAL
		wp.Generation = 777

		_, err := client.Delete(wp, intKey)
		assertSubcodeAbsent(err, types.GENERATION_ERROR, "generation")
	})

	// -----------------------------------------------------------
	// Subcode-present cases.
	// -----------------------------------------------------------

	gg.It("HLL refresh count on missing bin: BIN_NOT_FOUND with HLL subcode", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		key, _ := as.NewKey(*namespace, set, "edv-no-hll-key")
		err := client.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin("other-bin", 1))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		_, err = client.Operate(wp, key, as.HLLRefreshCountOp("no-hll-bin"))
		assertSubcode(err, types.BIN_NOT_FOUND, as.SubCodeBinNotFoundHLLCannotCreateWithOp)
	})

	gg.It("list get index out of bounds: OP_NOT_APPLICABLE with CDT-index subcode", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		_, err := client.Operate(wp, listKey, as.ListGetOp(edvBinName, 99))
		assertSubcode(err, types.OP_NOT_APPLICABLE, as.SubCodeOpNotCDTIndexOutOfBounds)
	})

	gg.It("list get by rank out of bounds: CDT rank subcode", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		_, err := client.Operate(wp, listKey, as.ListGetByRankOp(edvBinName, 99, as.ListReturnTypeValue))
		assertSubcode(err, types.OP_NOT_APPLICABLE, as.SubCodeOpNotCDTRankOutOfBounds)
	})

	gg.It("HLL fold target too large: OPNOT_HLL_FOLD_INDEX_BITS_TOO_LARGE subcode", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		key, _ := as.NewKey(*namespace, set, "edv-hll-fold-key")
		_, _ = client.Delete(as.NewWritePolicy(0, 0), key)
		_, err := client.Operate(as.NewWritePolicy(0, 0), key, as.HLLInitOp(as.DefaultHLLPolicy(), edvBinName, 8, 0))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		_, err = client.Operate(wp, key, as.HLLFoldOp(edvBinName, 14))
		assertSubcode(err, types.OP_NOT_APPLICABLE, as.SubCodeOpNotHLLFoldIndexBitsTooLarge)
	})

	gg.It("bit get offset out of range: PARAM_BITS_OFFSET subcode", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		key, _ := as.NewKey(*namespace, set, "edv-bits-key")
		err := client.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin(edvBinName, []byte{0xAA, 0xBB, 0xCC, 0xDD}))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		_, err = client.Operate(wp, key, as.BitGetOp(edvBinName, 2000000000, 8))
		assertSubcode(err, types.PARAMETER_ERROR, as.SubCodeParamBitsOffsetOutOfRange)
	})

	gg.It("bit get size zero: PARAM_BITS_SIZE subcode", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		key, _ := as.NewKey(*namespace, set, "edv-bits-key2")
		err := client.PutBins(as.NewWritePolicy(0, 0), key, as.NewBin(edvBinName, []byte{0xAA, 0xBB, 0xCC, 0xDD}))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		_, err = client.Operate(wp, key, as.BitGetOp(edvBinName, 0, 0))
		assertSubcode(err, types.PARAMETER_ERROR, as.SubCodeParamBitsSizeOutOfRange)
	})

	gg.It("read filtered out: FILTERED_OUT with FILTERED_BINS subcode", func() {
		p := as.NewPolicy()
		p.ErrorDetailVerbosity = 2
		p.FilterExpression = as.ExpEq(as.ExpIntBin(edvBinName), as.ExpIntVal(99))
		p.SendKey = false

		_, err := client.Get(p, intKey)
		assertSubcode(err, types.FILTERED_OUT, as.SubCodeFilteredBins)
	})

	// -----------------------------------------------------------
	// Write / delete / read policy (subcode absent unless noted).
	// -----------------------------------------------------------

	gg.It("CREATE_ONLY existing record: KEY_EXISTS_ERROR with subcode absent", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2
		wp.RecordExistsAction = as.CREATE_ONLY

		err := client.PutBins(wp, intKey, as.NewBin(edvBinName, 2))
		assertSubcodeAbsent(err, types.KEY_EXISTS_ERROR)
	})

	gg.It("REPLACE_ONLY missing record: KEY_NOT_FOUND_ERROR with subcode absent", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2
		wp.RecordExistsAction = as.REPLACE_ONLY

		key, _ := as.NewKey(*namespace, set, "edv-replace-missing-key")
		_, _ = client.Delete(as.NewWritePolicy(0, 0), key)

		err := client.PutBins(wp, key, as.NewBin(edvBinName, 1))
		assertSubcodeAbsent(err, types.KEY_NOT_FOUND_ERROR)
	})

	gg.It("write generation mismatch: GENERATION_ERROR with subcode absent", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2
		wp.GenerationPolicy = as.EXPECT_GEN_EQUAL
		wp.Generation = 999

		err := client.PutBins(wp, intKey, as.NewBin(edvBinName, 2))
		assertSubcodeAbsent(err, types.GENERATION_ERROR, "generation")
	})

	gg.It("operate filtered out: FILTERED_OUT with FILTERED_BINS subcode", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2
		wp.FilterExpression = as.ExpEq(as.ExpIntBin(edvBinName), as.ExpIntVal(99))

		_, err := client.Operate(wp, intKey, as.GetOp())
		assertSubcode(err, types.FILTERED_OUT, as.SubCodeFilteredBins)
	})

	// -----------------------------------------------------------
	// Happy path: verbosity must not break successful commands.
	// -----------------------------------------------------------

	gg.It("verbosity set on a successful command does not break it", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 2

		key, _ := as.NewKey(*namespace, set, "edv-success-key")
		err := client.PutBins(wp, key, as.NewBin(edvBinName, 42))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		rp := as.NewPolicy()
		rp.ErrorDetailVerbosity = 2
		rec, err := client.Get(rp, key)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rec).NotTo(gm.BeNil())
		gm.Expect(rec.Bins[edvBinName]).To(gm.Equal(42))
	})
})

func assertSubcode(err error, expectedResultCode types.ResultCode, expectedSubcode int) {
	gm.Expect(err).To(gm.HaveOccurred())
	ae := &as.AerospikeError{}
	gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
	gm.Expect(ae.ResultCode).To(gm.Equal(expectedResultCode))
	gm.Expect(ae.SubCode).To(gm.Equal(expectedSubcode))
	gm.Expect(ae.ServerMessage).NotTo(gm.BeEmpty())
	gm.Expect(ae.ServerMessage).To(gm.ContainSubstring("subcode=" + intToString(expectedSubcode)))
}

func assertSubcodeAbsent(err error, expectedResultCode types.ResultCode, expectedSubstrings ...string) {
	gm.Expect(err).To(gm.HaveOccurred())
	ae := &as.AerospikeError{}
	gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
	gm.Expect(ae.ResultCode).To(gm.Equal(expectedResultCode))
	gm.Expect(ae.SubCode).To(gm.Equal(as.SubCodeNone))
	gm.Expect(ae.ServerMessage).NotTo(gm.BeEmpty())
	for _, expected := range expectedSubstrings {
		gm.Expect(strings.ToLower(ae.ServerMessage)).To(gm.ContainSubstring(strings.ToLower(expected)))
	}
	gm.Expect(ae.ServerMessage).NotTo(gm.ContainSubstring("subcode="))
}

func intToString(v int) string {
	// Tiny helper to avoid importing strconv just for one call.
	return strings.TrimSpace(fmtInt(v))
}

func fmtInt(v int) string {
	if v == 0 {
		return "0"
	}
	neg := false
	if v < 0 {
		neg = true
		v = -v
	}
	var digits [20]byte
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		digits[i] = '-'
	}
	return string(digits[i:])
}
