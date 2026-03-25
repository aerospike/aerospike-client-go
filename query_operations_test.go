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

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Query operations with ops projection", func() {
	var ns = *namespace
	var set string
	var wpolicy = as.NewWritePolicy(0, 0)

	const size = 20
	const binName1 = "tqobin1"
	const binName2 = "tqobin2"
	const binName3 = "tqobin3"
	const mapBin = "tqomapbin"

	var indexName string

	gg.BeforeEach(func() {
		if serverIsOlderThan("8.1.2") {
			gg.Skip("Bin projection tests require server version 8.1.2 or later")
		}

		set = randString(50)
		indexName = set + binName1

		createIndex(wpolicy, ns, set, indexName, binName1, as.NUMERIC)

		for i := 1; i <= size; i++ {
			key, err := as.NewKey(ns, set, fmt.Sprintf("tqokey%d", i))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			mapVal := map[interface{}]interface{}{
				"a": i,
				"b": i * 10,
			}
			err = client.PutBins(wpolicy, key,
				as.NewBin(binName1, i),
				as.NewBin(binName2, i*10),
				as.NewBin(binName3, i*100),
				as.NewBin(mapBin, mapVal),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
	})

	gg.AfterEach(func() {
		if indexName != "" {
			gm.Expect(client.DropIndex(nil, ns, set, indexName)).ToNot(gm.HaveOccurred())
		}
	})

	var queryPolicy = as.NewQueryPolicy()

	gg.It("must project multiple bins via get operations", func() {
		stm := as.NewStatement(ns, set)
		stm.Operations = []*as.Operation{
			as.GetBinOp(binName1),
			as.GetBinOp(binName2),
			as.MapGetByKeyOp(mapBin, "a", as.MapReturnType.VALUE),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			gm.Expect(rec.Bins[binName1]).ToNot(gm.BeNil())
			gm.Expect(rec.Bins[binName2]).ToNot(gm.BeNil())
			gm.Expect(rec.Bins[mapBin]).ToNot(gm.BeNil())

			val1 := rec.Bins[binName1].(int)
			val2 := rec.Bins[binName2].(int)
			mapVal := rec.Bins[mapBin].(int)
			gm.Expect(val1 * 10).To(gm.Equal(val2))
			gm.Expect(val1).To(gm.Equal(mapVal))

			_, hasBin3 := rec.Bins[binName3]
			gm.Expect(hasBin3).To(gm.BeFalse())
			count++
		}
		gm.Expect(count).To(gm.BeNumerically(">=", size))
	})

	gg.It("must project a subset of bins with a range filter", func() {
		begin := 1
		end := 10

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))
		stm.Operations = []*as.Operation{
			as.GetBinOp(binName1),
			as.GetBinOp(binName3),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			val1 := rec.Bins[binName1].(int)
			val3 := rec.Bins[binName3].(int)
			gm.Expect(val1).To(gm.BeNumerically(">=", begin))
			gm.Expect(val1).To(gm.BeNumerically("<=", end))
			gm.Expect(val1 * 100).To(gm.Equal(val3))

			_, hasBin2 := rec.Bins[binName2]
			gm.Expect(hasBin2).To(gm.BeFalse())
			count++
		}
		gm.Expect(count).To(gm.Equal(end - begin + 1))
	})

	gg.It("must project bins via expression read operations", func() {
		stm := as.NewStatement(ns, set)

		exp1 := as.ExpIntBin(binName1)
		exp2 := as.ExpIntBin(binName2)
		exp3 := as.ExpIntBin(binName3)

		stm.Operations = []*as.Operation{
			as.ExpReadOp("result1", exp1, as.ExpReadFlagDefault),
			as.ExpReadOp("result2", exp2, as.ExpReadFlagDefault),
			as.ExpReadOp("result3", exp3, as.ExpReadFlagDefault),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			r1 := rec.Bins["result1"].(int)
			r2 := rec.Bins["result2"].(int)
			r3 := rec.Bins["result3"].(int)
			gm.Expect(r1 * 10).To(gm.Equal(r2))
			gm.Expect(r1 * 100).To(gm.Equal(r3))
			count++
		}
		gm.Expect(count).To(gm.BeNumerically(">=", size))
	})

	gg.It("must project bins via expression read with range filter", func() {
		begin := 1
		end := 10

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))

		exp1 := as.ExpIntBin(binName1)
		exp2 := as.ExpIntBin(binName2)
		exp3 := as.ExpIntBin(binName3)

		stm.Operations = []*as.Operation{
			as.ExpReadOp("result1", exp1, as.ExpReadFlagDefault),
			as.ExpReadOp("result2", exp2, as.ExpReadFlagDefault),
			as.ExpReadOp("result3", exp3, as.ExpReadFlagDefault),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			r1 := rec.Bins["result1"].(int)
			r2 := rec.Bins["result2"].(int)
			r3 := rec.Bins["result3"].(int)
			gm.Expect(r1).To(gm.BeNumerically(">=", begin))
			gm.Expect(r1).To(gm.BeNumerically("<=", end))
			gm.Expect(r1 * 10).To(gm.Equal(r2))
			gm.Expect(r1 * 100).To(gm.Equal(r3))
			count++
		}
		gm.Expect(count).To(gm.Equal(end - begin + 1))
	})

	gg.It("must project mixed get and expression read operations", func() {
		begin := 1
		end := 10

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))

		computedExp := as.ExpNumAdd(as.ExpIntBin(binName1), as.ExpIntBin(binName2))

		stm.Operations = []*as.Operation{
			as.GetBinOp(binName1),
			as.ExpReadOp("sum", computedExp, as.ExpReadFlagDefault),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			val1 := rec.Bins[binName1].(int)
			sum := rec.Bins["sum"].(int)
			gm.Expect(val1).To(gm.BeNumerically(">=", begin))
			gm.Expect(val1).To(gm.BeNumerically("<=", end))
			gm.Expect(val1 + val1*10).To(gm.Equal(sum))

			_, hasBin2 := rec.Bins[binName2]
			gm.Expect(hasBin2).To(gm.BeFalse())
			_, hasBin3 := rec.Bins[binName3]
			gm.Expect(hasBin3).To(gm.BeFalse())
			count++
		}
		gm.Expect(count).To(gm.Equal(end - begin + 1))
	})

	gg.It("must query with expression read multiply operation", func() {
		begin := 1
		end := 10

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))

		exp := as.ExpNumMul(as.ExpIntBin(binName1), as.ExpIntVal(100))

		stm.Operations = []*as.Operation{
			as.GetBinOp(binName1),
			as.ExpReadOp("computed", exp, as.ExpReadFlagDefault),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			original := rec.Bins[binName1].(int)
			computed := rec.Bins["computed"].(int)
			gm.Expect(original * 100).To(gm.Equal(computed))
			count++
		}
		gm.Expect(count).To(gm.Equal(end - begin + 1))
	})

	gg.It("must query with multiple expression read operations", func() {
		begin := 5
		end := 15

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))

		sumExp := as.ExpNumAdd(as.ExpIntBin(binName1), as.ExpIntBin(binName2))
		diffExp := as.ExpNumSub(as.ExpIntBin(binName2), as.ExpIntBin(binName1))

		stm.Operations = []*as.Operation{
			as.GetBinOp(binName1),
			as.GetBinOp(binName2),
			as.ExpReadOp("sum", sumExp, as.ExpReadFlagDefault),
			as.ExpReadOp("diff", diffExp, as.ExpReadFlagDefault),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			val1 := rec.Bins[binName1].(int)
			val2 := rec.Bins[binName2].(int)
			sum := rec.Bins["sum"].(int)
			diff := rec.Bins["diff"].(int)
			gm.Expect(val1 + val2).To(gm.Equal(sum))
			gm.Expect(val2 - val1).To(gm.Equal(diff))
			count++
		}
		gm.Expect(count).To(gm.Equal(end - begin + 1))
	})

	gg.It("must query with expression read and filter expression", func() {
		begin := 1
		end := 20

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))

		computedExp := as.ExpNumMul(as.ExpIntBin(binName1), as.ExpIntVal(2))

		stm.Operations = []*as.Operation{
			as.GetBinOp(binName1),
			as.ExpReadOp("doubled", computedExp, as.ExpReadFlagDefault),
		}

		qp := as.NewQueryPolicy()
		qp.FilterExpression = as.ExpLess(as.ExpIntBin(binName1), as.ExpIntVal(6))

		recordset, err := client.Query(qp, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			original := rec.Bins[binName1].(int)
			doubled := rec.Bins["doubled"].(int)
			gm.Expect(original * 2).To(gm.Equal(doubled))
			gm.Expect(original).To(gm.BeNumerically("<", 6))
			count++
		}
		gm.Expect(count).To(gm.Equal(5))
	})

	gg.It("must query with a single get operation", func() {
		begin := 1
		end := 5

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))
		stm.Operations = []*as.Operation{as.GetBinOp(binName1)}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			val1 := rec.Bins[binName1].(int)
			gm.Expect(val1).To(gm.BeNumerically(">=", begin))
			gm.Expect(val1).To(gm.BeNumerically("<=", end))

			_, hasBin2 := rec.Bins[binName2]
			gm.Expect(hasBin2).To(gm.BeFalse())
			count++
		}
		gm.Expect(count).To(gm.Equal(end - begin + 1))
	})

	gg.It("must reject write operation in foreground query", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, 1, 5))
		stm.Operations = []*as.Operation{as.PutOp(as.NewBin("foo", "bar"))}

		recordset, err := client.Query(queryPolicy, stm)
		if err == nil {
			for res := range recordset.Results() {
				if res.Err != nil {
					err = res.Err
					break
				}
			}
		}

		gm.Expect(err).To(gm.HaveOccurred())
		var ae *as.AerospikeError
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

	gg.It("must reject exp write operation in foreground query", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, 1, 5))

		exp := as.ExpStringVal("bar")
		stm.Operations = []*as.Operation{as.ExpWriteOp("foo", exp, as.ExpWriteFlagDefault)}

		recordset, err := client.Query(queryPolicy, stm)
		if err == nil {
			for res := range recordset.Results() {
				if res.Err != nil {
					err = res.Err
					break
				}
			}
		}

		gm.Expect(err).To(gm.HaveOccurred())
		var ae *as.AerospikeError
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

	gg.It("must reject mixed read and write operations in foreground query", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, 1, 5))

		readExp := as.ExpIntBin(binName1)
		writeExp := as.ExpStringVal("updated")

		stm.Operations = []*as.Operation{
			as.ExpReadOp("computed", readExp, as.ExpReadFlagDefault),
			as.ExpWriteOp("foo", writeExp, as.ExpWriteFlagDefault),
		}

		recordset, err := client.Query(queryPolicy, stm)
		if err == nil {
			for res := range recordset.Results() {
				if res.Err != nil {
					err = res.Err
					break
				}
			}
		}

		gm.Expect(err).To(gm.HaveOccurred())
		var ae *as.AerospikeError
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

	gg.It("must reject read-only operations in background execute", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, 1, 5))

		exp := as.ExpIntBin(binName1)

		_, err := client.QueryExecute(queryPolicy, nil, stm, as.ExpReadOp("computed", exp, as.ExpReadFlagDefault))
		gm.Expect(err).To(gm.HaveOccurred())

		var ae *as.AerospikeError
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

	gg.It("must succeed with write operation in background execute", func() {
		begin := 1
		end := 3

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))

		exp := as.ExpStringVal("executed")
		tsk, err := client.QueryExecute(queryPolicy, nil, stm, as.ExpWriteOp("marker", exp, as.ExpWriteFlagDefault))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-tsk.OnComplete()).To(gm.BeNil())

		for i := begin; i <= end; i++ {
			key, err := as.NewKey(ns, set, fmt.Sprintf("tqokey%d", i))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			rec, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(rec).ToNot(gm.BeNil())
			gm.Expect(rec.Bins["marker"]).To(gm.Equal("executed"))
		}
	})

	gg.It("must reject mixed read and write operations in background execute", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, 1, 5))

		readExp := as.ExpIntBin(binName1)
		writeExp := as.ExpStringVal("mixed")

		_, err := client.QueryExecute(queryPolicy, nil, stm,
			as.ExpReadOp("computed", readExp, as.ExpReadFlagDefault),
			as.ExpWriteOp("tag", writeExp, as.ExpWriteFlagDefault),
		)
		gm.Expect(err).To(gm.HaveOccurred())

		var ae *as.AerospikeError
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

	gg.It("must query with expression read and no filter", func() {
		stm := as.NewStatement(ns, set)

		exp := as.ExpNumAdd(as.ExpIntBin(binName1), as.ExpIntVal(1000))
		stm.Operations = []*as.Operation{as.ExpReadOp("offset", exp, as.ExpReadFlagDefault)}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			gm.Expect(rec.Bins["offset"]).ToNot(gm.BeNil())
			count++
		}
		gm.Expect(count).To(gm.BeNumerically(">=", size))
	})

	gg.It("must query with conditional expression read", func() {
		begin := 1
		end := 20

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))

		exp := as.ExpCond(
			as.ExpGreater(as.ExpIntBin(binName1), as.ExpIntVal(10)), as.ExpStringVal("high"),
			as.ExpStringVal("low"),
		)

		stm.Operations = []*as.Operation{
			as.GetBinOp(binName1),
			as.ExpReadOp("category", exp, as.ExpReadFlagDefault),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		highCount := 0
		lowCount := 0

		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record

			val := rec.Bins[binName1].(int)
			category := rec.Bins["category"].(string)
			gm.Expect(category).ToNot(gm.BeEmpty())

			if val > 10 {
				gm.Expect(category).To(gm.Equal("high"))
				highCount++
			} else {
				gm.Expect(category).To(gm.Equal("low"))
				lowCount++
			}
		}
		gm.Expect(highCount).To(gm.Equal(10))
		gm.Expect(lowCount).To(gm.Equal(10))
	})

	gg.It("must reject touch operation in foreground query", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, 1, 5))
		stm.Operations = []*as.Operation{as.TouchOp()}

		recordset, err := client.Query(queryPolicy, stm)
		if err == nil {
			for res := range recordset.Results() {
				if res.Err != nil {
					err = res.Err
					break
				}
			}
		}

		gm.Expect(err).To(gm.HaveOccurred())
		var ae *as.AerospikeError
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

	gg.It("must reject delete operation in foreground query", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, 1, 5))
		stm.Operations = []*as.Operation{as.DeleteOp()}

		recordset, err := client.Query(queryPolicy, stm)
		if err == nil {
			for res := range recordset.Results() {
				if res.Err != nil {
					err = res.Err
					break
				}
			}
		}

		gm.Expect(err).To(gm.HaveOccurred())
		var ae *as.AerospikeError
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

	gg.It("must query with expression read eval no fail on nonexistent bin", func() {
		begin := 1
		end := 5

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, int64(begin), int64(end)))

		exp := as.ExpIntBin("nonexistent")
		stm.Operations = []*as.Operation{
			as.GetBinOp(binName1),
			as.ExpReadOp("result", exp, as.ExpReadFlagEvalNoFail),
		}

		recordset, err := client.Query(queryPolicy, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			gm.Expect(rec.Bins[binName1]).ToNot(gm.BeNil())
			count++
		}
		gm.Expect(count).To(gm.Equal(end - begin + 1))
	})

	gg.It("must reject get operation in background execute", func() {
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter(binName1, 1, 5))

		_, err := client.QueryExecute(queryPolicy, nil, stm, as.GetBinOp(binName1))
		gm.Expect(err).To(gm.HaveOccurred())

		var ae *as.AerospikeError
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
	})

})
