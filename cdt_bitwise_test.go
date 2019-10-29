// Copyright 2013-2019 Aerospike, Inc.
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
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	as "github.com/aerospike/aerospike-client-go"
	ast "github.com/aerospike/aerospike-client-go/types"
)

var _ = Describe("CDT Bitwise Test", func() {
	initTestVars()

	if !featureEnabled("blob-bits") {
		By("CDT Bitwise Tests will not run since feature is not supported by the server.")
		return
	}

	// connection data
	var ns = *namespace
	var set = randString(50)
	var key *as.Key
	var wpolicy = as.NewWritePolicy(0, 0)
	var cdtBinName string

	var assertEquals = func(e string, v1, v2 interface{}) {
		Expect(v1).To(Equal(v2), e)
	}

	var assertBitModifyRegion = func(bin_sz, offset, set_sz int, expected []byte, isInsert bool, ops ...*as.Operation) {
		client.Delete(nil, key)

		initial := make([]byte, bin_sz)

		for i := 0; i < bin_sz; i++ {
			initial[i] = 0xFF
		}

		err := client.PutBins(nil, key, as.NewBin(cdtBinName, initial))
		Expect(err).ToNot(HaveOccurred())

		int_sz := 64

		if set_sz < int_sz {
			int_sz = set_sz
		}

		bin_bit_sz := bin_sz * 8

		if isInsert {
			bin_bit_sz += set_sz
		}

		full_ops := make([]*as.Operation, len(ops)+7)
		copy(full_ops, ops)
		full_ops[len(full_ops)-7] = as.BitLScanOp(cdtBinName, offset, set_sz, true)
		full_ops[len(full_ops)-6] = as.BitRScanOp(cdtBinName, offset, set_sz, true)
		full_ops[len(full_ops)-5] = as.BitGetIntOp(cdtBinName, offset, int_sz, false)
		full_ops[len(full_ops)-4] = as.BitCountOp(cdtBinName, offset, set_sz)
		full_ops[len(full_ops)-3] = as.BitLScanOp(cdtBinName, 0, bin_bit_sz, false)
		full_ops[len(full_ops)-2] = as.BitRScanOp(cdtBinName, 0, bin_bit_sz, false)
		full_ops[len(full_ops)-1] = as.BitGetOp(cdtBinName, offset, set_sz)

		record, err := client.Operate(nil, key, full_ops...)
		Expect(err).ToNot(HaveOccurred())

		result_list := record.Bins[cdtBinName].([]interface{})
		lscan1_result := result_list[len(result_list)-7].(int)
		rscan1_result := result_list[len(result_list)-6].(int)
		getint_result := result_list[len(result_list)-5].(int)
		count_result := result_list[len(result_list)-4].(int)
		lscan_result := result_list[len(result_list)-3].(int)
		rscan_result := result_list[len(result_list)-2].(int)
		actual := (result_list[len(result_list)-1]).([]byte)
		err_output := fmt.Sprintf("bin_sz %d offset %d set_sz %d", bin_sz, offset, set_sz)

		assertEquals("lscan1 - "+err_output, -1, lscan1_result)
		assertEquals("rscan1 - "+err_output, -1, rscan1_result)
		assertEquals("getint - "+err_output, 0, getint_result)
		assertEquals("count - "+err_output, 0, count_result)
		assertEquals("lscan - "+err_output, offset, lscan_result)
		assertEquals("rscan - "+err_output, offset+set_sz-1, rscan_result)
		assertEquals("op - "+err_output, expected, actual)
	}

	var assertBitModifyRegionNotInsert = func(bin_sz, offset, set_sz int, expected []byte, ops ...*as.Operation) {
		assertBitModifyRegion(bin_sz, offset, set_sz, expected, false, ops...)
	}

	var assertBitModifyInsert = func(bin_sz, offset, set_sz int, expected []byte, ops ...*as.Operation) {
		assertBitModifyRegion(bin_sz, offset, set_sz, expected, true, ops...)
	}

	var assertBitReadOperation = func(initial []byte, expected []int64, ops ...*as.Operation) {
		client.Delete(nil, key)
		err := client.PutBins(nil, key, as.NewBin(cdtBinName, initial))
		Expect(err).ToNot(HaveOccurred())

		rec, err := client.Operate(wpolicy, key, ops...)
		Expect(err).ToNot(HaveOccurred())

		Expect(rec.Bins[cdtBinName]).To(BeAssignableToTypeOf([]interface{}{}))
		binResults := rec.Bins[cdtBinName].([]interface{})
		results := make([]int64, len(binResults))
		for i := range binResults {
			results[i] = int64(binResults[i].(int))
		}

		Expect(results).To(Equal(expected))
	}

	var assertBitModifyOperations = func(initial, expected []byte, ops ...*as.Operation) {
		client.Delete(nil, key)

		if initial != nil {
			err := client.PutBins(wpolicy, key, as.NewBin(cdtBinName, initial))
			Expect(err).ToNot(HaveOccurred())
		}

		_, err := client.Operate(nil, key, ops...)
		Expect(err).ToNot(HaveOccurred())

		rec, err := client.Get(nil, key)
		Expect(err).ToNot(HaveOccurred())

		Expect(rec.Bins[cdtBinName]).To(Equal(expected))
	}

	var assertThrows = func(code ast.ResultCode, ops ...*as.Operation) {
		_, err := client.Operate(nil, key, ops...)
		Expect(err).To(HaveOccurred())
		Expect(err).To(BeAssignableToTypeOf(ast.AerospikeError{}))
		Expect(err.(ast.AerospikeError).ResultCode()).To(Equal(code))
	}

	BeforeEach(func() {
		key, err = as.NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())

		cdtBinName = randString(10)
	})

	Describe("CDT BitWise Operations", func() {

		// const listSize = 10

		// // make a fresh list before each operation
		// BeforeEach(func() {
		// 	list = []interface{}{}

		// 	for i := 1; i <= listSize; i++ {
		// 		list = append(list, i)

		// 		sz, err := client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, i))
		// 		Expect(err).ToNot(HaveOccurred())
		// 		Expect(sz.Bins[cdtBinName]).To(Equal(i))
		// 	}
		// })

		It("should Set a Bin", func() {

			bit0 := []byte{0x80}
			putMode := as.DefaultBitPolicy()
			updateMode := as.NewBitPolicy(as.BitWriteFlagsUpdateOnly)

			assertBitModifyOperations(
				[]byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08},
				[]byte{0x51, 0x02, 0x03, 0x04, 0x05, 0x06},
				as.BitSetOp(putMode, cdtBinName, 1, 1, bit0),
				as.BitSetOp(updateMode, cdtBinName, 3, 1, bit0),
				as.BitRemoveOp(updateMode, cdtBinName, 6, 2),
			)

			addMode := as.NewBitPolicy(as.BitWriteFlagsCreateOnly)
			bytes1 := []byte{0x0A}

			assertBitModifyOperations(
				nil, []byte{0x00, 0x0A},
				as.BitInsertOp(addMode, cdtBinName, 1, bytes1),
			)

			assertThrows(17,
				as.BitSetOp(putMode, "b", 1, 1, bit0))

			assertThrows(4,
				as.BitSetOp(addMode, cdtBinName, 1, 1, bit0))
		})

		It("should Set a Bin's bits", func() {

			putMode := as.DefaultBitPolicy()
			bit0 := []byte{0x80}
			bits1 := []byte{0x11, 0x22, 0x33}

			assertBitModifyOperations(
				[]byte{0x01, 0x12, 0x02, 0x03, 0x04, 0x05, 0x06,
					0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D,
					0x0E, 0x0F, 0x10, 0x11, 0x41},
				[]byte{0x41,
					0x13,
					0x11, 0x22, 0x33,
					0x11, 0x22, 0x33,
					0x08,
					0x08, 0x91, 0x1B,
					0X01, 0x12, 0x23,
					0x11, 0x22, 0x11,
					0xc1},
				as.BitSetOp(putMode, cdtBinName, 1, 1, bit0),
				as.BitSetOp(putMode, cdtBinName, 15, 1, bit0),
				// SUM Offest Size
				as.BitSetOp(putMode, cdtBinName, 16, 24, bits1),  //  Y    Y      Y
				as.BitSetOp(putMode, cdtBinName, 40, 22, bits1),  //  N    Y      N
				as.BitSetOp(putMode, cdtBinName, 73, 21, bits1),  //  N    N      N
				as.BitSetOp(putMode, cdtBinName, 100, 20, bits1), //  Y    N      N
				as.BitSetOp(putMode, cdtBinName, 120, 17, bits1), //  N    Y      N

				as.BitSetOp(putMode, cdtBinName, 144, 1, bit0),
			)
		})

		It("should LSHIFT bits", func() {

			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x01, 0x01, 0x00, 0x80,
					0xFF, 0x01, 0x01,
					0x18, 0x01},
				[]byte{0x02, 0x40, 0x01, 0x00,
					0xF8, 0x08, 0x01,
					0x28, 0x01},
				as.BitLShiftOp(putMode, cdtBinName, 0, 8, 1),
				as.BitLShiftOp(putMode, cdtBinName, 9, 7, 6),
				as.BitLShiftOp(putMode, cdtBinName, 23, 2, 1),

				as.BitLShiftOp(putMode, cdtBinName, 37, 18, 3),

				as.BitLShiftOp(putMode, cdtBinName, 58, 2, 1),
				as.BitLShiftOp(putMode, cdtBinName, 64, 4, 7),
			)

			assertBitModifyOperations(
				[]byte{0xFF, 0xFF, 0xFF},
				[]byte{0xF8, 0x00, 0x0F},
				as.BitLShiftOp(putMode, cdtBinName, 0, 20, 15),
			)
		})

		It("should RSHIFT bits", func() {

			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x80, 0x40, 0x01, 0x00,
					0xFF, 0x01, 0x01,
					0x18, 0x80},
				[]byte{0x40, 0x01, 0x00, 0x80,
					0xF8, 0xE0, 0x21,
					0x14, 0x80},
				as.BitRShiftOp(putMode, cdtBinName, 0, 8, 1),
				as.BitRShiftOp(putMode, cdtBinName, 9, 7, 6),
				as.BitRShiftOp(putMode, cdtBinName, 23, 2, 1),

				as.BitRShiftOp(putMode, cdtBinName, 37, 18, 3),

				as.BitRShiftOp(putMode, cdtBinName, 60, 2, 1),
				as.BitRShiftOp(putMode, cdtBinName, 68, 4, 7),
			)
		})

		It("should OR bits", func() {

			bits1 := []byte{0x11, 0x22, 0x33}
			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x80, 0x40, 0x01, 0x00, 0x00,
					0x01, 0x02, 0x03},
				[]byte{0x90, 0x48, 0x01, 0x20, 0x11,
					0x11, 0x22, 0x33},
				as.BitOrOp(putMode, cdtBinName, 0, 5, bits1),
				as.BitOrOp(putMode, cdtBinName, 9, 7, bits1),
				as.BitOrOp(putMode, cdtBinName, 23, 6, bits1),
				as.BitOrOp(putMode, cdtBinName, 32, 8, bits1),

				as.BitOrOp(putMode, cdtBinName, 40, 24, bits1),
			)
		})

		It("should XOR bits", func() {

			bits1 := []byte{0x11, 0x22, 0x33}
			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x80, 0x40, 0x01, 0x00, 0x00,
					0x01, 0x02, 0x03},
				[]byte{0x90, 0x48, 0x01, 0x20, 0x11, 0x10, 0x20,
					0x30},
				as.BitXorOp(putMode, cdtBinName, 0, 5, bits1),
				as.BitXorOp(putMode, cdtBinName, 9, 7, bits1),
				as.BitXorOp(putMode, cdtBinName, 23, 6, bits1),
				as.BitXorOp(putMode, cdtBinName, 32, 8, bits1),

				as.BitXorOp(putMode, cdtBinName, 40, 24, bits1),
			)
		})

		It("should AND bits", func() {

			bits1 := []byte{0x11, 0x22, 0x33}
			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x80, 0x40, 0x01, 0x00, 0x00,
					0x01, 0x02, 0x03},
				[]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03},
				as.BitAndOp(putMode, cdtBinName, 0, 5, bits1),
				as.BitAndOp(putMode, cdtBinName, 9, 7, bits1),
				as.BitAndOp(putMode, cdtBinName, 23, 6, bits1),
				as.BitAndOp(putMode, cdtBinName, 32, 8, bits1),

				as.BitAndOp(putMode, cdtBinName, 40, 24, bits1),
			)
		})

		It("should NOT bits", func() {

			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x80, 0x40, 0x01, 0x00, 0x00, 0x01, 0x02, 0x03},
				[]byte{0x78, 0x3F, 0x00, 0xF8, 0xFF, 0xFE, 0xFD, 0xFC},
				as.BitNotOp(putMode, cdtBinName, 0, 5),
				as.BitNotOp(putMode, cdtBinName, 9, 7),
				as.BitNotOp(putMode, cdtBinName, 23, 6),
				as.BitNotOp(putMode, cdtBinName, 32, 8),

				as.BitNotOp(putMode, cdtBinName, 40, 24),
			)
		})

		It("should ADD bits", func() {

			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x38, 0x1F, 0x00, 0xE8, 0x7F,
					0x00, 0x00, 0x00,
					0x01, 0x01, 0x01,
					0x01, 0x01, 0x01,
					0x02, 0x02, 0x02,
					0x03, 0x03, 0x03},
				[]byte{0x40, 0x20, 0x01, 0xF0, 0x80,
					0x7F, 0x7F, 0x7F,
					0x02, 0x02, 0x01,
					0x02, 0x02, 0x02,
					0x03, 0x03, 0x06,
					0x07, 0x07, 0x07},
				as.BitAddOp(putMode, cdtBinName, 0, 5, 1, false, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 9, 7, 1, false, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 23, 6, 0x21, false, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 32, 8, 1, false, as.BitOverflowActionFail),

				as.BitAddOp(putMode, cdtBinName, 40, 24, 0x7F7F7F, false, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 64, 20, 0x01010, false, as.BitOverflowActionFail),

				as.BitAddOp(putMode, cdtBinName, 92, 20, 0x10101, false, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 113, 22, 0x8082, false, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 136, 23, 0x20202, false, as.BitOverflowActionFail),
			)

			initial := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
			i := 0

			assertBitModifyOperations(
				initial,
				[]byte{0xFE, 0xFE, 0x7F, 0xFF, 0x7F, 0x80},
				as.BitAddOp(putMode, cdtBinName, 8*i, 8, 0xFF, false, as.BitOverflowActionWrap),
				as.BitAddOp(putMode, cdtBinName, 8*i, 8, 0xFF, false, as.BitOverflowActionWrap),

				as.BitAddOp(putMode, cdtBinName, 8*(i+1), 8, 0x7F, true, as.BitOverflowActionWrap),
				as.BitAddOp(putMode, cdtBinName, 8*(i+1), 8, 0x7F, true, as.BitOverflowActionWrap),

				as.BitAddOp(putMode, cdtBinName, 8*(i+2), 8, 0x80, true, as.BitOverflowActionWrap),
				as.BitAddOp(putMode, cdtBinName, 8*(i+2), 8, 0xFF, true, as.BitOverflowActionWrap),

				as.BitAddOp(putMode, cdtBinName, 8*(i+3), 8, 0x80, false, as.BitOverflowActionSaturate),
				as.BitAddOp(putMode, cdtBinName, 8*(i+3), 8, 0x80, false, as.BitOverflowActionSaturate),

				as.BitAddOp(putMode, cdtBinName, 8*(i+4), 8, 0x77, true, as.BitOverflowActionSaturate),
				as.BitAddOp(putMode, cdtBinName, 8*(i+4), 8, 0x77, true, as.BitOverflowActionSaturate),

				as.BitAddOp(putMode, cdtBinName, 8*(i+5), 8, 0x8F, true, as.BitOverflowActionSaturate),
				as.BitAddOp(putMode, cdtBinName, 8*(i+5), 8, 0x8F, true, as.BitOverflowActionSaturate),
			)

			err := client.PutBins(nil, key, as.NewBin(cdtBinName, initial))
			Expect(err).ToNot(HaveOccurred())

			assertThrows(26,
				as.BitAddOp(putMode, cdtBinName, 0, 8, 0xFF, false, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 0, 8, 0xFF, false, as.BitOverflowActionFail),
			)

			assertThrows(26,
				as.BitAddOp(putMode, cdtBinName, 0, 8, 0x7F, true, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 0, 8, 0x02, true, as.BitOverflowActionFail),
			)

			assertThrows(26,
				as.BitAddOp(putMode, cdtBinName, 0, 8, 0x81, true, as.BitOverflowActionFail),
				as.BitAddOp(putMode, cdtBinName, 0, 8, 0xFE, true, as.BitOverflowActionFail),
			)
		})

		It("should SUB bits", func() {

			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x38, 0x1F, 0x00, 0xE8, 0x7F,

					0x80, 0x80, 0x80,
					0x01, 0x01, 0x01,

					0x01, 0x01, 0x01,
					0x02, 0x02, 0x02,
					0x03, 0x03, 0x03},
				[]byte{0x30, 0x1E, 0x00, 0xD0, 0x7E,

					0x7F, 0x7F, 0x7F,
					0x00, 0xF0, 0xF1,

					0x00, 0x00, 0x00,
					0x01, 0xFD, 0xFE,
					0x00, 0xE0, 0xE1},
				as.BitSubtractOp(putMode, cdtBinName, 0, 5, 0x01, false, as.BitOverflowActionFail),
				as.BitSubtractOp(putMode, cdtBinName, 9, 7, 0x01, false, as.BitOverflowActionFail),
				as.BitSubtractOp(putMode, cdtBinName, 23, 6, 0x03, false, as.BitOverflowActionFail),
				as.BitSubtractOp(putMode, cdtBinName, 32, 8, 0x01, false, as.BitOverflowActionFail),

				as.BitSubtractOp(putMode, cdtBinName, 40, 24, 0x10101, false, as.BitOverflowActionFail),
				as.BitSubtractOp(putMode, cdtBinName, 64, 20, 0x101, false, as.BitOverflowActionFail),

				as.BitSubtractOp(putMode, cdtBinName, 92, 20, 0x10101, false, as.BitOverflowActionFail),
				as.BitSubtractOp(putMode, cdtBinName, 113, 21, 0x101, false, as.BitOverflowActionFail),
				as.BitSubtractOp(putMode, cdtBinName, 136, 23, 0x11111, false, as.BitOverflowActionFail),
			)

			initial := []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
			i := 0

			assertBitModifyOperations(
				initial,
				[]byte{0xFF, 0xF6, 0x7F, 0x00, 0x80, 0x7F},
				as.BitSubtractOp(putMode, cdtBinName, 8*i, 8, 0x01, false, as.BitOverflowActionWrap),

				as.BitSubtractOp(putMode, cdtBinName, 8*(i+1), 8, 0x80, true, as.BitOverflowActionWrap),
				as.BitSubtractOp(putMode, cdtBinName, 8*(i+1), 8, 0x8A, true, as.BitOverflowActionWrap),

				as.BitSubtractOp(putMode, cdtBinName, 8*(i+2), 8, 0x7F, true, as.BitOverflowActionWrap),
				as.BitSubtractOp(putMode, cdtBinName, 8*(i+2), 8, 0x02, true, as.BitOverflowActionWrap),

				as.BitSubtractOp(putMode, cdtBinName, 8*(i+3), 8, 0xAA, false, as.BitOverflowActionSaturate),

				as.BitSubtractOp(putMode, cdtBinName, 8*(i+4), 8, 0x77, true, as.BitOverflowActionSaturate),
				as.BitSubtractOp(putMode, cdtBinName, 8*(i+4), 8, 0x77, true, as.BitOverflowActionSaturate),

				as.BitSubtractOp(putMode, cdtBinName, 8*(i+5), 8, 0x81, true, as.BitOverflowActionSaturate),
				as.BitSubtractOp(putMode, cdtBinName, 8*(i+5), 8, 0x8F, true, as.BitOverflowActionSaturate),
			)

			err := client.PutBins(nil, key, as.NewBin(cdtBinName, initial))
			Expect(err).ToNot(HaveOccurred())

			assertThrows(26,
				as.BitSubtractOp(putMode, cdtBinName, 0, 8, 1, false, as.BitOverflowActionFail),
			)

			assertThrows(26,
				as.BitSubtractOp(putMode, cdtBinName, 0, 8, 0x7F, true, as.BitOverflowActionFail),
				as.BitSubtractOp(putMode, cdtBinName, 0, 8, 0x02, true, as.BitOverflowActionFail),
			)

			assertThrows(26,
				as.BitSubtractOp(putMode, cdtBinName, 0, 8, 0x81, true, as.BitOverflowActionFail),
				as.BitSubtractOp(putMode, cdtBinName, 0, 8, 0xFE, true, as.BitOverflowActionFail),
			)
		})

		It("should SetInt bits", func() {

			putMode := as.DefaultBitPolicy()

			assertBitModifyOperations(
				[]byte{0x38, 0x1F, 0x00, 0xE8, 0x7F,

					0x80, 0x80, 0x80,
					0x01, 0x01, 0x01,

					0x01, 0x01, 0x01,
					0x02, 0x02, 0x02,
					0x03, 0x03, 0x03},
				[]byte{0x08, 0x01, 0x00, 0x18, 0x01,

					0x01, 0x01, 0x01,
					0x00, 0x10, 0x11,

					0x01, 0x01, 0x01,
					0x00, 0x04, 0x06,
					0x02, 0x22, 0x23},
				as.BitSetIntOp(putMode, cdtBinName, 0, 5, 0x01),
				as.BitSetIntOp(putMode, cdtBinName, 9, 7, 0x01),
				as.BitSetIntOp(putMode, cdtBinName, 23, 6, 0x03),
				as.BitSetIntOp(putMode, cdtBinName, 32, 8, 0x01),

				as.BitSetIntOp(putMode, cdtBinName, 40, 24, 0x10101),
				as.BitSetIntOp(putMode, cdtBinName, 64, 20, 0x101),

				as.BitSetIntOp(putMode, cdtBinName, 92, 20, 0x10101),
				as.BitSetIntOp(putMode, cdtBinName, 113, 21, 0x101),
				as.BitSetIntOp(putMode, cdtBinName, 136, 23, 0x11111),
			)
		})

		It("should Get bits", func() {

			client.Delete(nil, key)

			bytes := []byte{0xC1, 0xAA, 0xAA}
			err := client.PutBins(nil, key, as.NewBin(cdtBinName, bytes))
			Expect(err).ToNot(HaveOccurred())

			record, err := client.Operate(nil, key,
				as.BitGetOp(cdtBinName, 0, 1),
				as.BitGetOp(cdtBinName, 1, 1),
				as.BitGetOp(cdtBinName, 7, 1),
				as.BitGetOp(cdtBinName, 0, 8),

				as.BitGetOp(cdtBinName, 8, 16),
				as.BitGetOp(cdtBinName, 9, 15),
				as.BitGetOp(cdtBinName, 9, 14),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(record).NotTo(BeNil())

			expected := [][]byte{
				[]byte{0x80},
				[]byte{0x80},
				[]byte{0x80},
				[]byte{0xC1},

				[]byte{0xAA, 0xAA},
				[]byte{0x55, 0x54},
				[]byte{0x55, 0x54},
			}

			// assertRecordFound(key, record)
			//System.out.println("Record: " + record);

			result_list := record.Bins[cdtBinName].([]interface{})
			results := make([][]byte, len(expected))

			for i := 0; i < len(expected); i++ {
				results[i] = result_list[i].([]byte)
			}

			Expect(expected).To(Equal(results))
		})

		It("should Count bits", func() {

			assertBitReadOperation(
				[]byte{0xC1, 0xAA, 0xAB},
				[]int64{1, 1, 1, 3, 9, 8, 7},
				as.BitCountOp(cdtBinName, 0, 1),
				as.BitCountOp(cdtBinName, 1, 1),
				as.BitCountOp(cdtBinName, 7, 1),
				as.BitCountOp(cdtBinName, 0, 8),

				as.BitCountOp(cdtBinName, 8, 16),
				as.BitCountOp(cdtBinName, 9, 15),
				as.BitCountOp(cdtBinName, 9, 14),
			)
		})

		It("should LSCAN bits", func() {

			assertBitReadOperation(
				[]byte{0xFF, 0xFF, 0xFF,
					0xFF, 0x00, 0x00, 0x00, 0x00, 0x01},
				[]int64{0, 0, 0,
					0, -1, -1,
					39, -1, 0, 0,
					0, 32,
					0, -1},
				as.BitLScanOp(cdtBinName, 0, 1, true),
				as.BitLScanOp(cdtBinName, 0, 8, true),
				as.BitLScanOp(cdtBinName, 0, 9, true),

				as.BitLScanOp(cdtBinName, 0, 32, true),
				as.BitLScanOp(cdtBinName, 0, 32, false),
				as.BitLScanOp(cdtBinName, 1, 30, false),

				as.BitLScanOp(cdtBinName, 32, 40, true),
				as.BitLScanOp(cdtBinName, 33, 38, true),
				as.BitLScanOp(cdtBinName, 32, 40, false),
				as.BitLScanOp(cdtBinName, 33, 38, false),

				as.BitLScanOp(cdtBinName, 0, 72, true),
				as.BitLScanOp(cdtBinName, 0, 72, false),

				as.BitLScanOp(cdtBinName, -1, 1, true),
				as.BitLScanOp(cdtBinName, -1, 1, false),
			)
		})

		It("should RSCAN bits", func() {

			assertBitReadOperation(
				[]byte{0xFF, 0xFF, 0xFF, 0xFF,
					0x00, 0x00, 0x00, 0x00, 0x01},
				[]int64{0, 7, 8,
					31, -1, -1,
					39, -1, 38, 37,
					71, 70,
					0, -1},
				as.BitRScanOp(cdtBinName, 0, 1, true),
				as.BitRScanOp(cdtBinName, 0, 8, true),
				as.BitRScanOp(cdtBinName, 0, 9, true),

				as.BitRScanOp(cdtBinName, 0, 32, true),
				as.BitRScanOp(cdtBinName, 0, 32, false),
				as.BitRScanOp(cdtBinName, 1, 30, false),

				as.BitRScanOp(cdtBinName, 32, 40, true),
				as.BitRScanOp(cdtBinName, 33, 38, true),
				as.BitRScanOp(cdtBinName, 32, 40, false),
				as.BitRScanOp(cdtBinName, 33, 38, false),

				as.BitRScanOp(cdtBinName, 0, 72, true),
				as.BitRScanOp(cdtBinName, 0, 72, false),

				as.BitRScanOp(cdtBinName, -1, 1, true),
				as.BitRScanOp(cdtBinName, -1, 1, false),
			)
		})

		It("should GetInt bits", func() {

			assertBitReadOperation(
				[]byte{0x0F, 0x0F, 0x00},
				[]int64{15, -1,
					15, 15,
					8, -8,
					3840, 3840,
					3840, 3840,
					1920, 1920,
					115648, -15424,
					15, -1},
				as.BitGetIntOp(cdtBinName, 4, 4, false),
				as.BitGetIntOp(cdtBinName, 4, 4, true),

				as.BitGetIntOp(cdtBinName, 0, 8, false),
				as.BitGetIntOp(cdtBinName, 0, 8, true),

				as.BitGetIntOp(cdtBinName, 7, 4, false),
				as.BitGetIntOp(cdtBinName, 7, 4, true),

				as.BitGetIntOp(cdtBinName, 8, 16, false),
				as.BitGetIntOp(cdtBinName, 8, 16, true),

				as.BitGetIntOp(cdtBinName, 9, 15, false),
				as.BitGetIntOp(cdtBinName, 9, 15, true),

				as.BitGetIntOp(cdtBinName, 9, 14, false),
				as.BitGetIntOp(cdtBinName, 9, 14, true),

				as.BitGetIntOp(cdtBinName, 5, 17, false),
				as.BitGetIntOp(cdtBinName, 5, 17, true),

				as.BitGetIntOp(cdtBinName, -12, 4, false),
				as.BitGetIntOp(cdtBinName, -12, 4, true),
			)
		})

		It("should BitSetEx bits", func() {

			policy := as.DefaultBitPolicy()
			bin_sz := 15
			bin_bit_sz := bin_sz * 8

			for set_sz := 1; set_sz <= 80; set_sz++ {
				set_data := make([]byte, (set_sz+7)/8)

				for offset := 0; offset <= (bin_bit_sz - set_sz); offset++ {
					assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, set_data, as.BitSetOp(policy, cdtBinName, offset, set_sz, set_data))
				}
			}
		})

		It("should LSHIFTEX bits", func() {

			policy := as.DefaultBitPolicy()
			bin_sz := 15
			bin_bit_sz := bin_sz * 8

			for set_sz := 1; set_sz <= 80; set_sz++ {
				set_data := make([]byte, (set_sz+7)/8)

				for offset := 0; offset <= (bin_bit_sz - set_sz); offset++ {
					limit := 16
					if set_sz < 16 {
						limit = set_sz + 1
					}

					for n_bits := 0; n_bits <= limit; n_bits++ {
						assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, set_data,
							as.BitSetOp(policy, cdtBinName, offset, set_sz,
								set_data),
							as.BitLShiftOp(policy, cdtBinName, offset, set_sz,
								n_bits))
					}

					for n_bits := 63; n_bits <= set_sz; n_bits++ {
						assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, set_data,
							as.BitSetOp(policy, cdtBinName, offset, set_sz,
								set_data),
							as.BitLShiftOp(policy, cdtBinName, offset, set_sz,
								n_bits))
					}
				}
			}
		})

		It("should RSHIFT Ex bits", func() {

			policy := as.DefaultBitPolicy()
			partial_policy := as.NewBitPolicy(as.BitWriteFlagsPartial)
			bin_sz := 15
			bin_bit_sz := bin_sz * 8

			for set_sz := 1; set_sz <= 80; set_sz++ {
				set_data := make([]byte, (set_sz+7)/8)

				for offset := 0; offset <= (bin_bit_sz - set_sz); offset++ {
					limit := 16
					if set_sz < 16 {
						limit = set_sz + 1
					}

					for n_bits := 0; n_bits <= limit; n_bits++ {
						assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, set_data,
							as.BitSetOp(policy, cdtBinName, offset, set_sz,
								set_data),
							as.BitRShiftOp(policy, cdtBinName, offset, set_sz,
								n_bits))
					}

					for n_bits := 63; n_bits <= set_sz; n_bits++ {
						assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, set_data,
							as.BitSetOp(policy, cdtBinName, offset, set_sz,
								set_data),
							as.BitRShiftOp(policy, cdtBinName, offset, set_sz,
								n_bits))
					}
				}

				// Test Partial
				n_bits := 1

				for offset := bin_bit_sz - set_sz + 1; offset < bin_bit_sz; offset++ {
					actual_set_sz := bin_bit_sz - offset
					actual_set_data := make([]byte, (actual_set_sz+7)/8)

					assertBitModifyRegionNotInsert(bin_sz, offset, actual_set_sz,
						actual_set_data,
						as.BitSetOp(partial_policy, cdtBinName, offset, set_sz,
							set_data),
						as.BitRShiftOp(partial_policy, cdtBinName, offset, set_sz,
							n_bits))
				}
			}
		})

		It("should AND Ex bits", func() {

			policy := as.DefaultBitPolicy()
			bin_sz := 15
			bin_bit_sz := bin_sz * 8

			for set_sz := 1; set_sz <= 80; set_sz++ {
				set_data := make([]byte, (set_sz+7)/8)

				for offset := 0; offset <= (bin_bit_sz - set_sz); offset++ {
					assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, set_data,
						as.BitAndOp(policy, cdtBinName, offset, set_sz,
							set_data))
				}
			}
		})

		It("should NOT Ex bits", func() {

			policy := as.DefaultBitPolicy()
			bin_sz := 15
			bin_bit_sz := bin_sz * 8

			for set_sz := 1; set_sz <= 80; set_sz++ {
				set_data := make([]byte, (set_sz+7)/8)

				for offset := 0; offset <= (bin_bit_sz - set_sz); offset++ {
					assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, set_data,
						as.BitNotOp(policy, cdtBinName, offset, set_sz))
				}
			}
		})

		It("should INSERT Ex bits", func() {

			policy := as.DefaultBitPolicy()
			bin_sz := 15

			for set_sz := 1; set_sz <= 10; set_sz++ {
				set_data := make([]byte, set_sz)

				for offset := 0; offset <= bin_sz; offset++ {
					assertBitModifyInsert(bin_sz, offset*8, set_sz*8, set_data,
						as.BitInsertOp(policy, cdtBinName, offset, set_data))
				}
			}
		})

		It("should ADD Ex bits", func() {

			policy := as.DefaultBitPolicy()
			bin_sz := 15
			bin_bit_sz := bin_sz * 8

			for set_sz := 1; set_sz <= 64; set_sz++ {
				set_data := make([]byte, (set_sz+7)/8)

				for offset := 0; offset <= (bin_bit_sz - set_sz); offset++ {
					assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, set_data,
						as.BitAddOp(policy, cdtBinName, offset, set_sz, 1,
							false, as.BitOverflowActionWrap))
				}
			}
		})

		It("should SUB Ex bits", func() {

			policy := as.DefaultBitPolicy()
			bin_sz := 15
			bin_bit_sz := bin_sz * 8

			for set_sz := 1; set_sz <= 64; set_sz++ {
				expected := make([]byte, (set_sz+7)/8)
				value := int64(uint64(0xFFFFffffFFFFffff >> uint(64-set_sz)))

				for offset := 0; offset <= (bin_bit_sz - set_sz); offset++ {
					assertBitModifyRegionNotInsert(bin_sz, offset, set_sz, expected,
						as.BitSubtractOp(policy, cdtBinName, offset, set_sz,
							value, false, as.BitOverflowActionWrap))
				}
			}
		})

		It("should LSHIFT bits", func() {

			policy := as.DefaultBitPolicy()
			initial := []byte{}
			buf := []byte{0x80}

			client.Delete(nil, key)
			err := client.PutBins(nil, key, as.NewBin(cdtBinName, initial))
			Expect(err).ToNot(HaveOccurred())

			assertThrows(26,
				as.BitSetOp(policy, cdtBinName, 0, 1, buf))
			assertThrows(26,
				as.BitOrOp(policy, cdtBinName, 0, 1, buf))
			assertThrows(26,
				as.BitXorOp(policy, cdtBinName, 0, 1, buf))
			assertThrows(26,
				as.BitAndOp(policy, cdtBinName, 0, 1, buf))
			assertThrows(26,
				as.BitNotOp(policy, cdtBinName, 0, 1))
			assertThrows(26,
				as.BitLShiftOp(policy, cdtBinName, 0, 1, 1))
			assertThrows(26,
				as.BitRShiftOp(policy, cdtBinName, 0, 1, 1))
			// OK for insert.
			assertThrows(4,
				as.BitRemoveOp(policy, cdtBinName, 0, 1))
			assertThrows(26,
				as.BitAddOp(policy, cdtBinName, 0, 1, 1, false, as.BitOverflowActionFail))
			assertThrows(26,
				as.BitSubtractOp(policy, cdtBinName, 0, 1, 1, false, as.BitOverflowActionFail))
			assertThrows(26,
				as.BitSetIntOp(policy, cdtBinName, 0, 1, 1))

			assertThrows(26,
				as.BitGetOp(cdtBinName, 0, 1))
			assertThrows(26,
				as.BitCountOp(cdtBinName, 0, 1))
			assertThrows(26,
				as.BitLScanOp(cdtBinName, 0, 1, true))
			assertThrows(26,
				as.BitRScanOp(cdtBinName, 0, 1, true))
			assertThrows(26,
				as.BitGetIntOp(cdtBinName, 0, 1, false))
		})

		It("should Resize bits", func() {

			client.Delete(nil, key)

			policy := as.DefaultBitPolicy()
			noFail := as.NewBitPolicy(as.BitWriteFlagsNoFail)
			record, err := client.Operate(nil, key,
				as.BitResizeOp(policy, cdtBinName, 20, as.BitResizeFlagsDefault),
				as.BitGetOp(cdtBinName, 19*8, 8),
				as.BitResizeOp(noFail, cdtBinName, 10, as.BitResizeFlagsGrowOnly),
				as.BitGetOp(cdtBinName, 19*8, 8),
				as.BitResizeOp(policy, cdtBinName, 10, as.BitResizeFlagsShrinkOnly),
				as.BitGetOp(cdtBinName, 9*8, 8),
				as.BitResizeOp(noFail, cdtBinName, 30, as.BitResizeFlagsShrinkOnly),
				as.BitGetOp(cdtBinName, 9*8, 8),
				as.BitResizeOp(policy, cdtBinName, 19, as.BitResizeFlagsGrowOnly),
				as.BitGetOp(cdtBinName, 18*8, 8),
				as.BitResizeOp(noFail, cdtBinName, 0, as.BitResizeFlagsGrowOnly),
				as.BitResizeOp(policy, cdtBinName, 0, as.BitResizeFlagsShrinkOnly),
			)
			Expect(err).ToNot(HaveOccurred())

			//System.out.println("Record: " + record);

			result_list := record.Bins[cdtBinName].([]interface{})
			get0 := result_list[1].([]byte)
			get1 := result_list[3].([]byte)
			get2 := result_list[5].([]byte)
			get3 := result_list[7].([]byte)
			get4 := result_list[9].([]byte)

			Expect([]byte{0x00}).To(Equal(get0))
			Expect([]byte{0x00}).To(Equal(get1))
			Expect([]byte{0x00}).To(Equal(get2))
			Expect([]byte{0x00}).To(Equal(get3))
			Expect([]byte{0x00}).To(Equal(get4))
		})
	})

}) // describe
