// Copyright 2014-2022 Aerospike, Inc.
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
	"math/rand"
	"time"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v6"
	"github.com/aerospike/aerospike-client-go/v6/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike", func() {

	gg.Describe("Batch Write operations", func() {
		var ns = *namespace
		var set = randString(50)
		var wpolicy = as.NewWritePolicy(0, 0)
		var rpolicy = as.NewPolicy()
		var bpolicy = as.NewBatchPolicy()
		var bdpolicy = as.NewBatchDeletePolicy()
		// bpolicy.AllowInline = true

		if *useReplicas {
			rpolicy.ReplicaPolicy = as.MASTER_PROLES
		}

		gg.BeforeEach(func() {
			bpolicy.FilterExpression = nil
		})

		gg.Context("Batch Delete operations", func() {
			const keyCount = 1000
			var exists []bool
			var ekeys []*as.Key
			var dkeys []*as.Key

			gg.BeforeEach(func() {
				bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))
				ekeys = []*as.Key{}
				dkeys = []*as.Key{}
				for i := 0; i < keyCount; i++ {
					key, err := as.NewKey(ns, set, randString(50))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					// make sure they exists in the DB
					exists, err := client.Exists(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(exists).To(gm.Equal(true))

					// if key shouldExist == true, put it in the DB
					if i%2 == 0 {
						ekeys = append(ekeys, key)
					} else {
						dkeys = append(dkeys, key)
					}
				}
			})

			gg.It("must return the result with same ordering", func() {
				res, err := client.BatchDelete(bpolicy, bdpolicy, dkeys)

				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(res).NotTo(gm.BeNil())
				gm.Expect(len(res)).To(gm.Equal(len(dkeys)))
				for _, br := range res {
					gm.Expect(br.ResultCode).To(gm.Equal(types.OK))
				}

				// true case
				exists, err = client.BatchExists(bpolicy, ekeys)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(len(exists)).To(gm.Equal(len(ekeys)))
				for _, keyExists := range exists {
					gm.Expect(keyExists).To(gm.BeTrue())
				}

				// false case
				exists, err = client.BatchExists(bpolicy, dkeys)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(len(exists)).To(gm.Equal(len(dkeys)))
				for _, keyExists := range exists {
					gm.Expect(keyExists).To(gm.BeFalse())
				}
			})
		})

		gg.Context("BatchOperate operations", func() {
			gg.It("must return the result with same ordering", func() {
				key1, _ := as.NewKey(ns, set, 1)
				op1 := as.NewBatchWrite(nil, key1, as.PutOp(as.NewBin("bin1", "a")), as.PutOp(as.NewBin("bin2", "b")))
				op3 := as.NewBatchRead(key1, []string{"bin2"})

				key2, _ := as.NewKey(ns, set, 2)
				op5 := as.NewBatchWrite(nil, key2, as.PutOp(as.NewBin("bin1", "a")))

				brecs := []as.BatchRecordIfc{op1, op3, op5}
				err := client.BatchOperate(bpolicy, brecs)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(op1.BatchRec().Err).ToNot(gm.HaveOccurred())
				gm.Expect(op1.BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(op1.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1": nil, "bin2": nil}))
				gm.Expect(op1.BatchRec().InDoubt).To(gm.BeFalse())

				// gm.Expect(op2.BatchRec().Err).ToNot(gm.HaveOccurred())
				// gm.Expect(op2.BatchRec().ResultCode).To(gm.Equal(types.OK))
				// gm.Expect(op2.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin2": nil}))
				// gm.Expect(op2.BatchRec().InDoubt).To(gm.BeFalse())

				gm.Expect(op3.BatchRec().Err).ToNot(gm.HaveOccurred())
				gm.Expect(op3.BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(op3.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin2": "b"}))
				gm.Expect(op3.BatchRec().InDoubt).To(gm.BeFalse())

				// gm.Expect(op4.BatchRec().Err).ToNot(gm.HaveOccurred())
				// gm.Expect(op4.BatchRec().ResultCode).To(gm.Equal(types.OK))
				// gm.Expect(op4.BatchRec().InDoubt).To(gm.BeFalse())

				// make sure the delete case actually ran
				// exists, err := client.Exists(nil, key1)
				// gm.Expect(exists).To(gm.BeFalse())

				// make sure the delete case actually ran
				exists, err := client.Exists(nil, key2)
				gm.Expect(exists).To(gm.BeTrue())
			})

			gg.It("must successfully execute a delete op", func() {
				bwPolicy := as.NewBatchWritePolicy()
				bdPolicy := as.NewBatchDeletePolicy()

				for _, sendKey := range []bool{true, false}	{
					bwPolicy.SendKey = sendKey
					bdPolicy.SendKey = sendKey
					bpolicy.SendKey = !sendKey

					key1, _ := as.NewKey(ns, set, 1)
					op1 := as.NewBatchWrite(bwPolicy, key1, as.PutOp(as.NewBin("bin1", "a")), as.PutOp(as.NewBin("bin2", "b")))
					op2 := as.NewBatchDelete(bdPolicy, key1)
					op3 := as.NewBatchRead(key1, []string{"bin2"})

					brecs := []as.BatchRecordIfc{op1, op2, op3}
					err := client.BatchOperate(bpolicy, brecs)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					gm.Expect(op1.BatchRec().Err).ToNot(gm.HaveOccurred())
					gm.Expect(op1.BatchRec().ResultCode).To(gm.Equal(types.OK))
					gm.Expect(op1.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1": nil, "bin2": nil}))
					gm.Expect(op1.BatchRec().InDoubt).To(gm.BeFalse())

					gm.Expect(op2.BatchRec().Err).ToNot(gm.HaveOccurred())
					gm.Expect(op2.BatchRec().ResultCode).To(gm.Equal(types.OK))
					gm.Expect(op2.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{}))
					gm.Expect(op2.BatchRec().InDoubt).To(gm.BeFalse())

					gm.Expect(op3.BatchRec().Err).To(gm.HaveOccurred())
					gm.Expect(op3.BatchRec().ResultCode).To(gm.Equal(types.KEY_NOT_FOUND_ERROR))
					gm.Expect(op3.BatchRec().Record).To(gm.BeNil())
					gm.Expect(op3.BatchRec().InDoubt).To(gm.BeFalse())

					exists, err := client.Exists(nil, key1)
					gm.Expect(exists).To(gm.BeFalse())
				}
			})

			gg.It("must successfully execute ops with policies", func() {
				key1, _ := as.NewKey(ns, set, randString(50))
				err := client.Put(nil, key1, as.BinMap{"bin1": 1, "bin2": 2})
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// Create the policy
				writePolicy := as.NewBatchWritePolicy()
				writePolicy.FilterExpression = as.ExpLess(as.ExpIntBin("bin1"), as.ExpIntVal(1))

				// Create write operation
				record := as.NewBatchWrite(writePolicy, key1,
					as.PutOp(as.NewBin("bin3", 3)),
					as.PutOp(as.NewBin("bin4", 4)),
				)

				records := []as.BatchRecordIfc{record}

				err = client.BatchOperate(nil, records)
				gm.Expect(err).ToNot(gm.HaveOccurred())		
				gm.Expect(record.ResultCode).To(gm.Equal(types.FILTERED_OUT))

				rec, err := client.Get(nil, key1)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(len(rec.Bins)).To(gm.Equal(2))

				// remove the filter

				writePolicy.FilterExpression = nil
				err = client.BatchOperate(nil, records)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(record.ResultCode).To(gm.Equal(types.OK))

				rec, err = client.Get(nil, key1)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(len(rec.Bins)).To(gm.Equal(4))
			})
		})

		gg.Context("BatchOperate operations", func() {

			gg.It("Overall command error should be reflected in API call error and not BatchRecord error", func() {
				var batchRecords []as.BatchRecordIfc
				for i := 0; i < len(client.Cluster().GetNodes()) * 5500; i++ {
					key, _ := as.NewKey(*namespace, set, i)
					batchRecords = append(batchRecords, as.NewBatchReadHeader(key))
				}

				err := client.BatchOperate(nil, batchRecords)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(types.BATCH_MAX_REQUESTS_EXCEEDED)).To(gm.BeTrue())

				for _, bri := range batchRecords {
					gm.Expect(bri.BatchRec().ResultCode).To(gm.Equal(types.NO_RESPONSE))
				}
			})

			gg.It("ListGetByValueRangeOp and ListRemoveByValueRangeOp with nil arguments correctly", func() {
				const binName = "int_bin"

				key, err := as.NewKey(ns, set, "list_key1")
				gm.Expect(err).ToNot(gm.HaveOccurred())

				l := []int{7, 6, 5, 8, 9, 10}
				err = client.PutBins(wpolicy, key, as.NewBin(binName, l))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// Get
				op1 := as.ListGetByValueRangeOp(binName, as.NewValue(7), as.NewValue(9), as.ListReturnTypeValue)
				op2 := as.ListGetByValueRangeOp(binName, as.NewValue(7), nil, as.ListReturnTypeIndex)
				op3 := as.ListGetByValueRangeOp(binName, as.NewValue(7), nil, as.ListReturnTypeValue)
				op4 := as.ListGetByValueRangeOp(binName, as.NewValue(7), nil, as.ListReturnTypeRank)
				op5 := as.ListGetByValueRangeOp(binName, nil, as.NewValue(9), as.ListReturnTypeValue)
				r, err := client.Operate(wpolicy, key, op1, op2, op3, op4, op5)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(r.Bins[binName]).To(gm.Equal([]interface{}{[]interface{}{7, 8}, []interface{}{0, 3, 4, 5}, []interface{}{7, 8, 9, 10}, []interface{}{2, 3, 4, 5}, []interface{}{7, 6, 5, 8}}))

				// Remove
				op6 := as.ListRemoveByValueRangeOp(binName, as.ListReturnTypeIndex, as.NewValue(7), nil)
				r2, err2 := client.Operate(wpolicy, key, op6)
				gm.Expect(err2).ToNot(gm.HaveOccurred())
				gm.Expect(r2.Bins[binName]).To(gm.Equal([]interface{}{0, 3, 4, 5}))

				r3, err3 := client.Get(nil, key)
				gm.Expect(err3).ToNot(gm.HaveOccurred())
				gm.Expect(r3.Bins[binName]).To(gm.Equal([]interface{}{6, 5}))
			})

			gg.It("must return the result with same ordering", func() {
				const keyCount = 50
				keys := []*as.Key{}

				for i := 0; i < keyCount; i++ {
					bin := as.NewBin("i", i)

					key, err := as.NewKey(ns, set, randString(50))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					// make sure they exists in the DB
					exists, err := client.Exists(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(exists).To(gm.Equal(true))

					keys = append(keys, key)
				}

				for i, key := range keys {
					op1 := as.NewBatchWrite(nil, key, as.PutOp(as.NewBin("bin1", "a")))
					op2 := as.NewBatchWrite(nil, key, as.PutOp(as.NewBin("bin2", "b")))
					op3 := as.NewBatchRead(key, []string{"bin2"})

					bpolicy.FilterExpression = as.ExpLess(
						as.ExpIntBin("i"),
						as.ExpIntVal(3),
					)

					brecs := []as.BatchRecordIfc{op1, op2, op3}
					err := client.BatchOperate(bpolicy, brecs)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					for _, rec := range brecs {
						if i < 3 {
							gm.Expect(rec.BatchRec().ResultCode).To(gm.Equal(types.OK))
						} else {
							gm.Expect(rec.BatchRec().ResultCode).To(gm.Equal(types.FILTERED_OUT))
						}
					}
				}
			})
		})

		gg.Context("BatchUDF operations", func() {
			gg.It("must return the results when one operation is against an invalid namespace", func() {
				luaCode := []byte(`-- Create a record
				function rec_create(rec, bins)
				    if bins ~= nil then
				        for b, bv in map.pairs(bins) do
				            rec[b] = bv
				        end
				    end
				    status = aerospike:create(rec)
				    return status
				end`)

				client.RemoveUDF(nil, "test_ops.lua")
				client.RegisterUDF(nil, luaCode, "test_ops.lua", as.LUA)

				batchRecords := []as.BatchRecordIfc{}

				key1, _ := as.NewKey(randString(10), set, 1)
				args := make(map[interface{}]interface{})
				args["bin1_str"] = "a"
				batchRecords = append(batchRecords, as.NewBatchUDF(
					nil,
					key1,
					"test_ops",
					"rec_create",
					as.NewMapValue(args),
				))

				key2, _ := as.NewKey(ns, set, 2)
				batchRecords = append(batchRecords, as.NewBatchWrite(
					nil,
					key2,
					as.PutOp(as.NewBin("bin1_str", "aa")),
				))

				key3, _ := as.NewKey(ns, set, 3)
				batchRecords = append(batchRecords, as.NewBatchWrite(
					nil,
					key3,
					as.PutOp(as.NewBin("bin1_str", "aaa")),
				))

				batchRecords = append(batchRecords, as.NewBatchRead(
					key1,
					[]string{"bin1_str"},
				))

				batchRecords = append(batchRecords, as.NewBatchRead(
					key2,
					[]string{"bin1_str"},
				))

				batchRecords = append(batchRecords, as.NewBatchRead(
					key3,
					[]string{"bin1_str"},
				))

				bp := as.NewBatchPolicy()
				bp.RespondAllKeys = false
				err := client.BatchOperate(bp, batchRecords)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(batchRecords[0].BatchRec().Err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
				gm.Expect(batchRecords[0].BatchRec().ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))

				gm.Expect(batchRecords[1].BatchRec().Err).To(gm.BeNil())
				gm.Expect(batchRecords[1].BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(batchRecords[1].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": nil}))

				gm.Expect(batchRecords[2].BatchRec().Err).To(gm.BeNil())
				gm.Expect(batchRecords[2].BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(batchRecords[2].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": nil}))

				gm.Expect(batchRecords[3].BatchRec().Err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
				gm.Expect(batchRecords[3].BatchRec().ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))

				gm.Expect(batchRecords[4].BatchRec().Err).To(gm.BeNil())
				gm.Expect(batchRecords[4].BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(batchRecords[4].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": "aa"}))

				gm.Expect(batchRecords[5].BatchRec().Err).To(gm.BeNil())
				gm.Expect(batchRecords[5].BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(batchRecords[5].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": "aaa"}))

				bp.RespondAllKeys = true
				err = client.BatchOperate(bp, batchRecords)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(batchRecords[0].BatchRec().Err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
				gm.Expect(batchRecords[0].BatchRec().ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))

				gm.Expect(batchRecords[1].BatchRec().Err).To(gm.BeNil())
				gm.Expect(batchRecords[1].BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(batchRecords[1].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": nil}))

				gm.Expect(batchRecords[2].BatchRec().Err).To(gm.BeNil())
				gm.Expect(batchRecords[2].BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(batchRecords[2].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": nil}))

				gm.Expect(batchRecords[3].BatchRec().Err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
				gm.Expect(batchRecords[3].BatchRec().ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))

				gm.Expect(batchRecords[4].BatchRec().Err).To(gm.BeNil())
				gm.Expect(batchRecords[4].BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(batchRecords[4].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": "aa"}))

				gm.Expect(batchRecords[5].BatchRec().Err).To(gm.BeNil())
				gm.Expect(batchRecords[5].BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(batchRecords[5].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": "aaa"}))
			})

			gg.It("must return correct errors", func() {

				client.Truncate(nil, ns, set, nil)

				udf := `function wait_and_update(rec, bins, n)
						    info("WAIT_AND_WRITE BEGIN")
						    sleep(n)
						    info("WAIT FINISHED")
						    if bins ~= nil then
						        for b, bv in map.pairs(bins) do
						            rec[b] = bv
						        end
						    end
						    status = aerospike:update(rec)
						    return status
						end

						function rec_create(rec, bins)
						    if bins ~= nil then
						        for b, bv in map.pairs(bins) do
						            rec[b] = bv
						        end
						    end
						    status = aerospike:create(rec)
						    return status
						end`

				task, err := client.RegisterUDF(nil, []byte(udf), "test_ops.lua", as.LUA)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				err = <-task.OnComplete()
				gm.Expect(err).ToNot(gm.HaveOccurred())

				var batchRecords []as.BatchRecordIfc
				for i := 0; i < 100; i++ {
					key, _ := as.NewKey(ns, set+"1", i)
					client.PutBins(nil, key, as.NewBin("i", 1))

					bin := make(map[string]int, 0)
					bin["bin"] = i
					batchRecords = append(batchRecords,
						as.NewBatchUDF(nil, key, "test_ops", "wait_and_update", as.NewValue(bin), as.NewValue(2)),
					)
				}

				bp := as.NewBatchPolicy()
				bp.TotalTimeout = 10000 * time.Millisecond
				bp.SocketTimeout = 1000 * time.Millisecond
				bp.MaxRetries = 5
				err = client.BatchOperate(bp, batchRecords)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				for _, bri := range batchRecords {
					br := bri.BatchRec()
					gm.Expect(br.InDoubt).To(gm.BeTrue())
					gm.Expect(br.ResultCode).To(gm.Equal(types.UDF_BAD_RESPONSE))
					gm.Expect(br.Err.Matches(types.UDF_BAD_RESPONSE)).To(gm.Equal(true))
					gm.Expect(br.Err.IsInDoubt()).To(gm.Equal(true))
				}

				if nsInfo(ns, "storage-engine") == "device" {
					writeBlockSize := 1048576
					bigBin := make(map[string]string, 0)
					bigBin["big_bin"] = strings.Repeat("a", writeBlockSize)
					smallBin := make(map[string]string, 0)
					smallBin["small_bin"] = strings.Repeat("a", 1000)
					key1, _ := as.NewKey(ns, set, 0)
					key2, _ := as.NewKey(ns, set, 1)
					key3, _ := as.NewKey(ns+"1", set, 2)
					batchRecords = []as.BatchRecordIfc{
						as.NewBatchUDF(nil, key1, "test_ops", "rec_create", as.NewValue(bigBin)),
						as.NewBatchUDF(nil, key2, "test_ops", "rec_create", as.NewValue(bigBin)),
						as.NewBatchUDF(nil, key3, "test_ops", "rec_create", as.NewValue(smallBin)),
					}

					err = client.BatchOperate(nil, batchRecords)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					br := batchRecords[0].BatchRec()
					gm.Expect(br.Err.IsInDoubt()).To(gm.BeTrue())
					gm.Expect(br.ResultCode).To(gm.Equal(types.RECORD_TOO_BIG))
					gm.Expect(br.Err.Matches(types.RECORD_TOO_BIG)).To(gm.Equal(true))
					gm.Expect(br.Err.IsInDoubt()).To(gm.Equal(true))

					br = batchRecords[1].BatchRec()
					gm.Expect(br.Err.IsInDoubt()).To(gm.BeTrue())
					gm.Expect(br.ResultCode).To(gm.Equal(types.RECORD_TOO_BIG))
					gm.Expect(br.Err.Matches(types.RECORD_TOO_BIG)).To(gm.Equal(true))
					gm.Expect(br.Err.IsInDoubt()).To(gm.Equal(true))

					br = batchRecords[2].BatchRec()
					gm.Expect(br.Err.IsInDoubt()).To(gm.BeFalse())
					gm.Expect(br.ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))
					gm.Expect(br.Err.Matches(types.INVALID_NAMESPACE)).To(gm.Equal(true))
					gm.Expect(br.Err.IsInDoubt()).To(gm.Equal(false))
				}
			})

			gg.It("must return the result with same ordering", func() {
				const keyCount = 50
				keys := []*as.Key{}

				regTask, err := client.RegisterUDF(wpolicy, []byte(udfBody), "udf1.lua", as.LUA)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// wait until UDF is created
				gm.Expect(<-regTask.OnComplete()).NotTo(gm.HaveOccurred())

				for i := 0; i < keyCount; i++ {
					bin := as.NewBin("bin1", i*6)

					key, err := as.NewKey(ns, set, randString(50))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					err = client.PutBins(wpolicy, key, bin)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					// make sure they exists in the DB
					exists, err := client.Exists(rpolicy, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(exists).To(gm.Equal(true))

					keys = append(keys, key)
				}

				brecs, err := client.BatchExecute(bpolicy, nil, keys, "udf1", "testFunc1", as.NewValue(2))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				for _, rec := range brecs {
					gm.Expect(rec.Err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.ResultCode).To(gm.Equal(types.OK))
					gm.Expect(rec.InDoubt).To(gm.BeFalse())
					gm.Expect(rec.Record.Bins["SUCCESS"]).To(gm.Equal(map[interface{}]interface{}{"status": "OK"}))
				}

				recs, err := client.BatchGet(nil, keys)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(len(recs)).To(gm.Equal(len(keys)))
				for i, rec := range recs {
					gm.Expect(rec.Bins["bin2"].(int)).To(gm.Equal(i * 3))
				}
			})
		})
	})
})
