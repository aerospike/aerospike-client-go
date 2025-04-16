// Copyright 2014-2019 Aerospike, Inc.
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
	"math"
	"strconv"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/types"
)

var _ = gg.Describe("HyperLogLog Test", func() {

	// connection data
	var ns = *namespace
	var set = randString(50)
	var binName string = "ophbin"
	var nEntries int = 1 << 18

	var minIndexBits int = 4
	var maxIndexBits int = 16
	var minMinhashBits int = 4
	var maxMinhashBits int = 51

	key, _ := as.NewKey(ns, set, "ophkey")
	key0, _ := as.NewKey(ns, set, "ophkey0")
	key1, _ := as.NewKey(ns, set, "ophkey1")
	key2, _ := as.NewKey(ns, set, "ophkey2")
	keys := []*as.Key{key0, key1, key2}

	var entries []as.Value
	var legalIndexBits []int
	var legalDescriptions [][]int
	var illegalDescriptions [][]int

	gg.BeforeEach(func() {

		for i := 0; i < nEntries; i++ {
			entries = append(entries, as.StringValue("key "+strconv.Itoa(i)))
		}

		for index_bits := minIndexBits; index_bits <= maxIndexBits; index_bits += 4 {
			combined_bits := maxMinhashBits + index_bits
			max_allowed_minhash_bits := maxMinhashBits

			if combined_bits > 64 {
				max_allowed_minhash_bits -= combined_bits - 64
			}

			mid_minhash_bits := (max_allowed_minhash_bits + index_bits) / 2
			var legal_zero []int
			var legal_min []int
			var legal_mid []int
			var legal_max []int

			legalIndexBits = append(legalIndexBits, index_bits)
			legal_zero = append(legal_zero, index_bits)
			legal_min = append(legal_min, index_bits)
			legal_mid = append(legal_mid, index_bits)
			legal_max = append(legal_max, index_bits)

			legal_zero = append(legal_zero, 0)
			legal_min = append(legal_min, minMinhashBits)
			legal_mid = append(legal_mid, mid_minhash_bits)
			legal_max = append(legal_max, max_allowed_minhash_bits)

			legalDescriptions = append(legalDescriptions, legal_zero)
			legalDescriptions = append(legalDescriptions, legal_min)
			legalDescriptions = append(legalDescriptions, legal_mid)
			legalDescriptions = append(legalDescriptions, legal_max)
		}

		for index_bits := minIndexBits - 1; index_bits <= maxIndexBits+5; index_bits += 4 {
			if index_bits < minIndexBits || index_bits > maxIndexBits {
				var illegal_zero []int
				var illegal_min []int
				var illegal_max []int

				illegal_zero = append(illegal_zero, index_bits)
				illegal_min = append(illegal_min, index_bits)
				illegal_max = append(illegal_max, index_bits)

				illegal_zero = append(illegal_zero, 0)
				illegal_min = append(illegal_min, minMinhashBits-1)
				illegal_max = append(illegal_max, maxMinhashBits)

				illegalDescriptions = append(illegalDescriptions, illegal_zero)
				illegalDescriptions = append(illegalDescriptions, illegal_min)
				illegalDescriptions = append(illegalDescriptions, illegal_max)
			} else {
				var illegal_min []int
				var illegal_max []int
				var illegal_max1 []int

				illegal_min = append(illegal_min, index_bits)
				illegal_max = append(illegal_max, index_bits)

				illegal_min = append(illegal_min, minMinhashBits-1)
				illegal_max = append(illegal_max, maxMinhashBits+1)

				illegalDescriptions = append(illegalDescriptions, illegal_min)
				illegalDescriptions = append(illegalDescriptions, illegal_max)

				if index_bits+maxMinhashBits > 64 {
					illegal_max1 = append(illegal_max1, index_bits)
					illegal_max1 = append(illegal_max1, 1+maxMinhashBits-(64-(index_bits+maxMinhashBits)))
					illegalDescriptions = append(illegalDescriptions, illegal_max1)
				}
			}
		}
	})

	expectErrors := func(key *as.Key, eresult types.ResultCode, ops ...*as.Operation) {
		_, err := client.Operate(nil, key, ops...)
		gm.Expect(err).To(gm.HaveOccurred())

		aerr, ok := err.(types.AerospikeError)
		gm.Expect(ok).To(gm.BeTrue())
		gm.Expect(aerr.ResultCode()).To(gm.Equal(eresult))
	}

	expectSuccess := func(key *as.Key, ops ...*as.Operation) *as.Record {
		record, err := client.Operate(nil, key, ops...)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(record).NotTo(gm.BeNil())
		return record
	}

	checkBits := func(index_bits, minhash_bits int) bool {
		return !(index_bits < minIndexBits || index_bits > maxIndexBits ||
			(minhash_bits != 0 && minhash_bits < minMinhashBits) ||
			minhash_bits > maxMinhashBits || index_bits+minhash_bits > 64)
	}

	relativeCountError := func(n_index_bits int) float64 {
		return 1.04 / math.Sqrt(math.Pow(2, float64(n_index_bits)))
	}

	expectDescription := func(description []interface{}, index_bits, minhash_bits int) {
		gm.Expect(index_bits).To(gm.Equal(description[0]))
		gm.Expect(minhash_bits).To(gm.Equal(description[1]))
	}

	expectInit := func(index_bits, minhash_bits int, should_pass bool) {
		p := as.DefaultHLLPolicy()
		ops := []*as.Operation{
			as.HLLInitOp(p, binName, index_bits, minhash_bits),
			as.HLLGetCountOp(binName),
			as.HLLRefreshCountOp(binName),
			as.HLLDescribeOp(binName),
		}

		if !should_pass {
			expectErrors(key, types.PARAMETER_ERROR, ops...)
			return
		}

		record := expectSuccess(key, ops...)
		result_list := record.Bins[binName].([]interface{})
		count := result_list[1]
		count1 := result_list[2]
		description := result_list[3].([]interface{})

		expectDescription(description, index_bits, minhash_bits)
		gm.Expect(0).To(gm.Equal(count))
		gm.Expect(0).To(gm.Equal(count1))
	}

	gg.It("Init should work", func() {
		_, err := client.Delete(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for _, desc := range legalDescriptions {
			expectInit(desc[0], desc[1], true)
		}

		for _, desc := range illegalDescriptions {
			expectInit(desc[0], desc[1], false)
		}
	})

	gg.It("HLL Flags should work", func() {
		index_bits := 4

		// Keep record around win binName is removed.
		expectSuccess(key,
			as.DeleteOp(),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName+"other", index_bits, -1))

		// create_only
		c := as.NewHLLPolicy(as.HLLWriteFlagsCreateOnly)

		expectSuccess(key, as.HLLInitOp(c, binName, index_bits, -1))
		expectErrors(key, types.BIN_EXISTS_ERROR,
			as.HLLInitOp(c, binName, index_bits, -1))

		// update_only
		u := as.NewHLLPolicy(as.HLLWriteFlagsUpdateOnly)

		expectSuccess(key, as.HLLInitOp(u, binName, index_bits, -1))
		expectSuccess(key, as.PutOp(as.NewBin(binName, nil)))
		expectErrors(key, types.BIN_NOT_FOUND,
			as.HLLInitOp(u, binName, index_bits, -1))

		// create_only no_fail
		cn := as.NewHLLPolicy(as.HLLWriteFlagsCreateOnly | as.HLLWriteFlagsNoFail)

		expectSuccess(key, as.HLLInitOp(cn, binName, index_bits, -1))
		expectSuccess(key, as.HLLInitOp(cn, binName, index_bits, -1))

		// update_only no_fail
		un := as.NewHLLPolicy(as.HLLWriteFlagsUpdateOnly | as.HLLWriteFlagsNoFail)

		expectSuccess(key, as.HLLInitOp(un, binName, index_bits, -1))
		expectSuccess(key, as.PutOp(as.NewBin(binName, nil)))
		expectSuccess(key, as.HLLInitOp(un, binName, index_bits, -1))

		// fold
		expectSuccess(key, as.HLLInitOp(c, binName, index_bits, -1))

		f := as.NewHLLPolicy(as.HLLWriteFlagsAllowFold)

		expectErrors(key, types.PARAMETER_ERROR,
			as.HLLInitOp(f, binName, index_bits, -1))
	})

	gg.It("Bad Init should NOT work", func() {
		p := as.DefaultHLLPolicy()

		expectSuccess(key, as.DeleteOp(), as.HLLInitOp(p, binName, maxIndexBits, 0))
		expectErrors(key, types.OP_NOT_APPLICABLE,
			as.HLLInitOp(p, binName, -1, maxMinhashBits))
	})

	isWithinRelativeError := func(expected, estimate int, relative_error float64) bool {
		return float64(expected)*(1-relative_error) <= float64(estimate) || float64(estimate) <= float64(expected)*(1+relative_error)
	}

	expectHLLCount := func(index_bits int, hll_count, expected int) {
		count_err_6sigma := relativeCountError(index_bits) * 6

		gm.Expect(isWithinRelativeError(expected, hll_count, count_err_6sigma)).To(gm.BeTrue())
	}

	expectAddInit := func(index_bits, minhash_bits int) {
		client.Delete(nil, key)

		p := as.DefaultHLLPolicy()
		ops := []*as.Operation{
			as.HLLAddOp(p, binName, entries, index_bits, minhash_bits),
			as.HLLGetCountOp(binName),
			as.HLLRefreshCountOp(binName),
			as.HLLDescribeOp(binName),
			as.HLLAddOp(p, binName, entries, -1, -1),
		}

		if !checkBits(index_bits, minhash_bits) {
			expectErrors(key, types.PARAMETER_ERROR, ops...)
			return
		}

		record := expectSuccess(key, ops...)
		result_list := record.Bins[binName].([]interface{})
		count := result_list[1].(int)
		count1 := result_list[2].(int)
		description := result_list[3].([]interface{})
		n_added := result_list[4]

		expectDescription(description, index_bits, minhash_bits)
		expectHLLCount(index_bits, count, len(entries))
		gm.Expect(count).To(gm.Equal(count1))
		gm.Expect(n_added).To(gm.Equal(0))
	}

	gg.It("Add Init should work", func() {
		for _, desc := range legalDescriptions {
			expectAddInit(desc[0], desc[1])
		}
	})

	gg.It("Add Flags should work", func() {
		index_bits := 4

		// Keep record around win binName is removed.
		expectSuccess(key,
			as.DeleteOp(),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName+"other", index_bits, -1))

		// create_only
		c := as.NewHLLPolicy(as.HLLWriteFlagsCreateOnly)

		expectSuccess(key, as.HLLAddOp(c, binName, entries, index_bits, -1))
		expectErrors(key, types.BIN_EXISTS_ERROR,
			as.HLLAddOp(c, binName, entries, index_bits, -1))

		// update_only
		u := as.NewHLLPolicy(as.HLLWriteFlagsUpdateOnly)

		expectErrors(key, types.PARAMETER_ERROR,
			as.HLLAddOp(u, binName, entries, index_bits, -1))

		// create_only no_fail
		cn := as.NewHLLPolicy(as.HLLWriteFlagsCreateOnly | as.HLLWriteFlagsNoFail)

		expectSuccess(key, as.HLLAddOp(cn, binName, entries, index_bits, -1))
		expectSuccess(key, as.HLLAddOp(cn, binName, entries, index_bits, -1))

		// fold
		expectSuccess(key, as.HLLInitOp(as.DefaultHLLPolicy(), binName, index_bits, -1))

		f := as.NewHLLPolicy(as.HLLWriteFlagsAllowFold)

		expectErrors(key, types.PARAMETER_ERROR,
			as.HLLAddOp(f, binName, entries, index_bits, -1))
	})

	expectFold := func(vals0, vals1 []as.Value, index_bits int) {
		p := as.DefaultHLLPolicy()

		for ix := minIndexBits; ix <= index_bits; ix++ {
			if !checkBits(index_bits, 0) || !checkBits(ix, 0) {
				// gm.Expected valid inputs
				gm.Expect(true).To(gm.BeFalse())
			}

			recorda := expectSuccess(key,
				as.DeleteOp(),
				as.HLLAddOp(p, binName, vals0, index_bits, -1),
				as.HLLGetCountOp(binName),
				as.HLLRefreshCountOp(binName),
				as.HLLDescribeOp(binName))

			resulta_list := recorda.Bins[binName].([]interface{})
			counta := resulta_list[1].(int)
			counta1 := resulta_list[2].(int)
			descriptiona := resulta_list[3].([]interface{})

			expectDescription(descriptiona, index_bits, 0)
			expectHLLCount(index_bits, counta, len(vals0))
			gm.Expect(counta).To(gm.Equal(counta1))

			recordb := expectSuccess(key,
				as.HLLFoldOp(binName, ix),
				as.HLLGetCountOp(binName),
				as.HLLAddOp(p, binName, vals0, -1, -1),
				as.HLLAddOp(p, binName, vals1, -1, -1),
				as.HLLGetCountOp(binName),
				as.HLLDescribeOp(binName))

			resultb_list := recordb.Bins[binName].([]interface{})
			countb := resultb_list[1].(int)
			n_added0 := resultb_list[2].(int)
			countb1 := resultb_list[4].(int)
			descriptionb := resultb_list[5].([]interface{})

			gm.Expect(0).To(gm.Equal(n_added0))
			expectDescription(descriptionb, ix, 0)
			expectHLLCount(ix, countb, len(vals0))
			expectHLLCount(ix, countb1, len(vals0)+len(vals1))
		}
	}

	gg.It("Fold should work", func() {
		var vals0 []as.Value
		var vals1 []as.Value

		for i := 0; i < nEntries/2; i++ {
			vals0 = append(vals0, as.StringValue("key "+strconv.Itoa(i)))
		}

		for i := nEntries / 2; i < nEntries; i++ {
			vals1 = append(vals1, as.StringValue("key "+strconv.Itoa(i)))
		}

		for index_bits := 4; index_bits < maxIndexBits; index_bits++ {
			expectFold(vals0, vals1, index_bits)
		}
	})

	gg.It("Fold Exists should work", func() {
		index_bits := 10
		fold_down := 4
		fold_up := 16

		// Keep record around win binName is removed.
		expectSuccess(key,
			as.DeleteOp(),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName+"other", index_bits, -1),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName, index_bits, -1))

		// Exists.
		expectSuccess(key, as.HLLFoldOp(binName, fold_down))
		expectErrors(key, types.OP_NOT_APPLICABLE,
			as.HLLFoldOp(binName, fold_up))

		// Does not exist.
		expectSuccess(key, as.PutOp(as.NewBin(binName, nil)))

		expectErrors(key, types.BIN_NOT_FOUND,
			as.HLLFoldOp(binName, fold_down))
	})

	expectSetUnion := func(vals [][]as.Value, index_bits int, folding, allow_folding bool) {
		p := as.DefaultHLLPolicy()
		u := as.DefaultHLLPolicy()

		if allow_folding {
			u = as.NewHLLPolicy(as.HLLWriteFlagsAllowFold)
		}

		union_expected := 0
		folded := false

		for i := 0; i < len(keys); i++ {
			ix := index_bits

			if folding {
				ix -= i

				if ix < minIndexBits {
					ix = minIndexBits
				}

				if ix < index_bits {
					folded = true
				}
			}

			sub_vals := vals[i]

			union_expected += len(sub_vals)

			record := expectSuccess(keys[i],
				as.DeleteOp(),
				as.HLLAddOp(p, binName, sub_vals, ix, -1),
				as.HLLGetCountOp(binName))
			result_list := record.Bins[binName].([]interface{})
			count := result_list[1].(int)

			expectHLLCount(ix, count, len(sub_vals))
		}

		var hlls []as.HLLValue

		for i := 0; i < len(keys); i++ {
			record := expectSuccess(keys[i], as.GetOpForBin(binName), as.HLLGetCountOp(binName))
			result_list := record.Bins[binName].([]interface{})
			hll := as.HLLValue(result_list[0].([]byte))

			gm.Expect(hll).NotTo(gm.BeNil())
			hlls = append(hlls, hll)
		}

		ops := []*as.Operation{
			as.DeleteOp(),
			as.HLLInitOp(p, binName, index_bits, -1),
			as.HLLSetUnionOp(u, binName, hlls),
			as.HLLGetCountOp(binName),
			as.DeleteOp(), // And recreate it to test creating empty.
			as.HLLSetUnionOp(p, binName, hlls),
			as.HLLGetCountOp(binName),
		}

		if folded && !allow_folding {
			expectErrors(key, types.OP_NOT_APPLICABLE, ops...)
			return
		}

		record_union := expectSuccess(key, ops...)
		union_result_list := record_union.Bins[binName].([]interface{})
		union_count := union_result_list[2].(int)
		union_count2 := union_result_list[4].(int)

		expectHLLCount(index_bits, union_count, union_expected)
		gm.Expect(union_count).To(gm.Equal(union_count2))

		for i := 0; i < len(keys); i++ {
			sub_vals := vals[i]
			record := expectSuccess(key,
				as.HLLAddOp(p, binName, sub_vals, index_bits, -1),
				as.HLLGetCountOp(binName))
			result_list := record.Bins[binName].([]interface{})
			n_added := result_list[0].(int)
			count := result_list[1].(int)

			gm.Expect(0).To(gm.Equal(n_added))
			gm.Expect(union_count).To(gm.Equal(count))
			expectHLLCount(index_bits, count, union_expected)
		}
	}

	gg.It("Set Union should work", func() {
		var vals [][]as.Value

		for i := 0; i < len(keys); i++ {
			var sub_vals []as.Value

			for j := 0; j < nEntries/3; j++ {
				sub_vals = append(sub_vals, as.StringValue("key"+strconv.Itoa(i)+" "+strconv.Itoa(j)))
			}

			vals = append(vals, sub_vals)
		}

		for _, index_bits := range legalIndexBits {
			expectSetUnion(vals, index_bits, false, false)
			expectSetUnion(vals, index_bits, false, true)
			expectSetUnion(vals, index_bits, true, false)
			expectSetUnion(vals, index_bits, true, true)
		}
	})

	gg.It("Set Union Flags should work", func() {
		index_bits := 6
		low_n_bits := 4
		high_n_bits := 8
		otherName := binName + "o"

		// Keep record around win binName is removed.
		var hlls []as.HLLValue
		record := expectSuccess(key,
			as.DeleteOp(),
			as.HLLAddOp(as.DefaultHLLPolicy(), otherName, entries, index_bits, -1),
			as.GetOpForBin(otherName))
		result_list := record.Bins[otherName].([]interface{})
		hll := as.HLLValue(result_list[1].([]byte))

		hlls = append(hlls, hll)

		// create_only
		c := as.NewHLLPolicy(as.HLLWriteFlagsCreateOnly)

		expectSuccess(key, as.HLLSetUnionOp(c, binName, hlls))
		expectErrors(key, types.BIN_EXISTS_ERROR,
			as.HLLSetUnionOp(c, binName, hlls))

		// update_only
		u := as.NewHLLPolicy(as.HLLWriteFlagsUpdateOnly)

		expectSuccess(key, as.HLLSetUnionOp(u, binName, hlls))
		expectSuccess(key, as.PutOp(as.NewBin(binName, nil)))
		expectErrors(key, types.BIN_NOT_FOUND,
			as.HLLSetUnionOp(u, binName, hlls))

		// create_only no_fail
		cn := as.NewHLLPolicy(as.HLLWriteFlagsCreateOnly | as.HLLWriteFlagsNoFail)

		expectSuccess(key, as.HLLSetUnionOp(cn, binName, hlls))
		expectSuccess(key, as.HLLSetUnionOp(cn, binName, hlls))

		// update_only no_fail
		un := as.NewHLLPolicy(as.HLLWriteFlagsUpdateOnly | as.HLLWriteFlagsNoFail)

		expectSuccess(key, as.HLLSetUnionOp(un, binName, hlls))
		expectSuccess(key, as.PutOp(as.NewBin(binName, nil)))
		expectSuccess(key, as.HLLSetUnionOp(un, binName, hlls))

		// fold
		f := as.NewHLLPolicy(as.HLLWriteFlagsAllowFold)

		// fold down
		expectSuccess(key, as.HLLInitOp(as.DefaultHLLPolicy(), binName, high_n_bits, -1))
		expectSuccess(key, as.HLLSetUnionOp(f, binName, hlls))

		// fold up
		expectSuccess(key, as.HLLInitOp(as.DefaultHLLPolicy(), binName, low_n_bits, -1))
		expectSuccess(key, as.HLLSetUnionOp(f, binName, hlls))
	})

	gg.It("Refresh Count should work", func() {
		index_bits := 6

		// Keep record around win binName is removed.
		expectSuccess(key,
			as.DeleteOp(),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName+"other", index_bits, -1),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName, index_bits, -1))

		// Exists.
		expectSuccess(key, as.HLLRefreshCountOp(binName),
			as.HLLRefreshCountOp(binName))
		expectSuccess(key, as.HLLAddOp(as.DefaultHLLPolicy(), binName, entries, -1, -1))
		expectSuccess(key, as.HLLRefreshCountOp(binName),
			as.HLLRefreshCountOp(binName))

		// Does not exist.
		expectSuccess(key, as.PutOp(as.NewBin(binName, nil)))
		expectErrors(key, types.BIN_NOT_FOUND,
			as.HLLRefreshCountOp(binName))
	})

	gg.It("Get Count should work", func() {
		index_bits := 6

		// Keep record around win binName is removed.
		expectSuccess(key,
			as.DeleteOp(),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName+"other", index_bits, -1),
			as.HLLAddOp(as.DefaultHLLPolicy(), binName, entries, index_bits, -1))

		// Exists.
		record := expectSuccess(key, as.HLLGetCountOp(binName))
		count := record.Bins[binName].(int)
		expectHLLCount(index_bits, count, len(entries))

		// Does not exist.
		expectSuccess(key, as.PutOp(as.NewBin(binName, nil)))
		record = expectSuccess(key, as.HLLGetCountOp(binName))
		gm.Expect(record.Bins[binName]).To(gm.BeNil())
	})

	gg.It("Get Union should work", func() {
		index_bits := 14
		expected_union_count := 0
		var vals [][]as.Value
		var hlls []as.HLLValue

		for i := 0; i < len(keys); i++ {
			var sub_vals []as.Value

			for j := 0; j < nEntries/3; j++ {
				sub_vals = append(sub_vals, as.StringValue("key"+strconv.Itoa(i)+" "+strconv.Itoa(j)))
			}

			record := expectSuccess(keys[i],
				as.DeleteOp(),
				as.HLLAddOp(as.DefaultHLLPolicy(), binName, sub_vals, index_bits, -1),
				as.GetOpForBin(binName))

			result_list := record.Bins[binName].([]interface{})
			hlls = append(hlls, as.HLLValue(result_list[1].([]byte)))
			expected_union_count += len(sub_vals)
			vals = append(vals, sub_vals)
		}

		// Keep record around win binName is removed.
		expectSuccess(key,
			as.DeleteOp(),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName+"other", index_bits, -1),
			as.HLLAddOp(as.DefaultHLLPolicy(), binName, vals[0], index_bits, -1))

		record := expectSuccess(key,
			as.HLLGetUnionOp(binName, hlls),
			as.HLLGetUnionCountOp(binName, hlls))
		result_list := record.Bins[binName].([]interface{})
		union_count := result_list[1].(int)

		expectHLLCount(index_bits, union_count, expected_union_count)

		union_hll := as.HLLValue(result_list[0].([]byte))

		record = expectSuccess(key,
			as.PutOp(as.NewBin(binName, union_hll)),
			as.HLLGetCountOp(binName))
		result_list = record.Bins[binName].([]interface{})
		union_count_2 := result_list[1].(int)

		gm.Expect(union_count).To(gm.Equal(union_count_2))
	})

	gg.It("Put should work", func() {
		for _, desc := range legalDescriptions {
			index_bits := desc[0]
			minhash_bits := desc[1]

			expectSuccess(key,
				as.DeleteOp(), as.HLLInitOp(as.DefaultHLLPolicy(), binName, index_bits, minhash_bits))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			hll := as.HLLValue(record.Bins[binName].([]byte))

			client.Delete(nil, key)
			client.PutBins(nil, key, as.NewBin(binName, hll))

			record = expectSuccess(key,
				as.HLLGetCountOp(binName),
				as.HLLDescribeOp(binName))

			result_list := record.Bins[binName].([]interface{})
			count := result_list[0].(int)
			description := result_list[1].([]interface{})

			gm.Expect(count).To(gm.Equal(0))
			expectDescription(description, index_bits, minhash_bits)
		}
	})

	absoluteSimilarityError := func(index_bits, minhash_bits int, expected_similarity float64) float64 {
		min_err_index := 1 / math.Sqrt(float64(int64(1<<uint(index_bits))))
		min_err_minhash := 6 * math.Pow(math.E, float64(minhash_bits*-1)) / expected_similarity

		return math.Max(min_err_index, min_err_minhash)
	}

	expectHMHSimilarity := func(index_bits, minhash_bits int, similarity,
		expected_similarity float64, intersect_count, expected_intersect_count int) {
		sim_err_6sigma := float64(0)

		if minhash_bits != 0 {
			sim_err_6sigma = 6 * absoluteSimilarityError(index_bits, minhash_bits, float64(expected_similarity))
		}

		if minhash_bits == 0 {
			return
		}

		gm.Expect(sim_err_6sigma > math.Abs(expected_similarity-similarity)).To(gm.BeTrue())
		gm.Expect(isWithinRelativeError(expected_intersect_count, intersect_count, sim_err_6sigma)).To(gm.BeTrue())
	}

	expectSimilarityOp := func(overlap float64, common []as.Value, vals [][]as.Value, index_bits,
		minhash_bits int) {
		var hlls []as.HLLValue

		for i := 0; i < len(keys); i++ {
			record := expectSuccess(keys[i],
				as.DeleteOp(),
				as.HLLAddOp(as.DefaultHLLPolicy(), binName, vals[i], index_bits, minhash_bits),
				as.HLLAddOp(as.DefaultHLLPolicy(), binName, common, index_bits, minhash_bits),
				as.GetOpForBin(binName))

			result_list := record.Bins[binName].([]interface{})
			hlls = append(hlls, as.HLLValue(result_list[2].([]byte)))
		}

		// Keep record around win binName is removed.
		record := expectSuccess(key,
			as.DeleteOp(),
			as.HLLInitOp(as.DefaultHLLPolicy(), binName+"other", index_bits, minhash_bits),
			as.HLLSetUnionOp(as.DefaultHLLPolicy(), binName, hlls),
			as.HLLDescribeOp(binName))
		result_list := record.Bins[binName].([]interface{})
		description := result_list[1].([]interface{})

		expectDescription(description, index_bits, minhash_bits)

		record = expectSuccess(key,
			as.HLLGetSimilarityOp(binName, hlls),
			as.HLLGetIntersectCountOp(binName, hlls))
		result_list = record.Bins[binName].([]interface{})
		sim := result_list[0].(float64)
		intersect_count := result_list[1].(int)
		expected_similarity := overlap
		expected_intersect_count := len(common)

		expectHMHSimilarity(index_bits, minhash_bits, sim, expected_similarity, intersect_count,
			expected_intersect_count)
	}

	gg.It("Similarity should work", func() {
		overlaps := []float64{0.0001, 0.001, 0.01, 0.1, 0.5}

		for _, overlap := range overlaps {
			expected_intersect_count := int(math.Floor(float64(nEntries) * overlap))
			var common []as.Value

			for i := 0; i < expected_intersect_count; i++ {
				common = append(common, as.StringValue("common"+strconv.Itoa(i)))
			}

			var vals [][]as.Value
			unique_entries_per_node := (nEntries - expected_intersect_count) / 3

			for i := 0; i < len(keys); i++ {
				var sub_vals []as.Value

				for j := 0; j < unique_entries_per_node; j++ {
					sub_vals = append(sub_vals, as.StringValue("key"+strconv.Itoa(i)+" "+strconv.Itoa(j)))
				}

				vals = append(vals, sub_vals)
			}

			for _, desc := range legalDescriptions {
				index_bits := desc[0]
				minhash_bits := desc[1]

				if minhash_bits == 0 {
					continue
				}

				expectSimilarityOp(overlap, common, vals, index_bits, minhash_bits)
			}
		}
	})

	gg.It("Empty Similarity should work", func() {
		for _, desc := range legalDescriptions {
			nIndexBits := desc[0]
			nMinhashBits := desc[1]

			record := expectSuccess(key,
				as.DeleteOp(),
				as.HLLInitOp(as.DefaultHLLPolicy(), binName, nIndexBits, nMinhashBits),
				as.GetOpForBin(binName))

			resultList := record.Bins[binName].([]interface{})
			var hlls []as.HLLValue

			hlls = append(hlls, as.HLLValue(resultList[1].([]byte)))

			record = expectSuccess(key,
				as.HLLGetSimilarityOp(binName, hlls),
				as.HLLGetIntersectCountOp(binName, hlls))

			resultList = record.Bins[binName].([]interface{})

			sim := resultList[0].(float64)
			intersectCount := resultList[1].(int)

			gm.Expect(0).To(gm.Equal(intersectCount))
			gm.Expect(math.IsNaN(sim)).To(gm.BeTrue())
		}
	})

	gg.It("Intersect should work", func() {
		otherBinName := binName + "other"

		for _, desc := range legalDescriptions {
			indexBits := desc[0]
			minhashBits := desc[1]

			if minhashBits != 0 {
				break
			}

			record := expectSuccess(key,
				as.DeleteOp(),
				as.HLLAddOp(as.DefaultHLLPolicy(), binName, entries, indexBits, minhashBits),
				as.GetOpForBin(binName),
				as.HLLAddOp(as.DefaultHLLPolicy(), otherBinName, entries, indexBits, 4),
				as.GetOpForBin(otherBinName))

			var hlls []as.HLLValue
			var hmhs []as.HLLValue
			resultList := record.Bins[binName].([]interface{})

			hlls = append(hlls, as.HLLValue(resultList[1].([]byte)))
			hlls = append(hlls, hlls[0])

			resultList = record.Bins[otherBinName].([]interface{})
			hmhs = append(hmhs, as.HLLValue(resultList[1].([]byte)))
			hmhs = append(hmhs, hmhs[0])

			record = expectSuccess(key,
				as.HLLGetIntersectCountOp(binName, hlls),
				as.HLLGetSimilarityOp(binName, hlls))
			resultList = record.Bins[binName].([]interface{})

			intersectCount := resultList[0].(int)

			gm.Expect(float64(intersectCount) < 1.8*float64(len(entries))).To(gm.BeTrue())

			hlls = append(hlls, hlls[0])

			expectErrors(key, types.PARAMETER_ERROR,
				as.HLLGetIntersectCountOp(binName, hlls))
			expectErrors(key, types.PARAMETER_ERROR,
				as.HLLGetSimilarityOp(binName, hlls))

			record = expectSuccess(key,
				as.HLLGetIntersectCountOp(binName, hmhs),
				as.HLLGetSimilarityOp(binName, hmhs))
			resultList = record.Bins[binName].([]interface{})
			intersectCount = resultList[0].(int)

			gm.Expect(float64(intersectCount) < 1.8*float64(len(entries))).To(gm.BeTrue())

			hmhs = append(hmhs, hmhs[0])

			expectErrors(key, types.OP_NOT_APPLICABLE,
				as.HLLGetIntersectCountOp(binName, hmhs))
			expectErrors(key, types.OP_NOT_APPLICABLE,
				as.HLLGetSimilarityOp(binName, hmhs))
		}
	})

}) // describe
