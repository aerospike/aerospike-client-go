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

// import (
// 	"bytes"
// 	"errors"
// 	"fmt"
// 	"math"
// 	"math/rand"
// 	// "strconv"
// 	"strings"
// 	// "time"
// 	// "log"

// 	as "github.com/aerospike/aerospike-client-go/v6"
// 	"github.com/aerospike/aerospike-client-go/v6/types"
// 	asub "github.com/aerospike/aerospike-client-go/v6/utils/buffer"

// 	gg "github.com/onsi/ginkgo/v2"
// 	gm "github.com/onsi/gomega"
// )

// // ALL tests are isolated by SetName and Key, which are 50 random characters
// var _ = gg.Describe("Aerospike GRPC", func() {

// 	gg.Describe("Batch Write operations", func() {
// 		var ns = *namespace
// 		var set = "testset" //randString(50)
// 		var wpolicy = as.NewWritePolicy(0, 0)
// 		var rpolicy = as.NewPolicy()
// 		var bpolicy = as.NewBatchPolicy()
// 		var bdpolicy = as.NewBatchDeletePolicy()
// 		// bpolicy.AllowInline = true

// 		if *useReplicas {
// 			rpolicy.ReplicaPolicy = as.MASTER_PROLES
// 		}

// 		gg.BeforeEach(func() {
// 			bpolicy.FilterExpression = nil
// 		})

// 		gg.Context("Batch Delete operations", func() {
// 			const keyCount = 1000
// 			var exists []bool
// 			var ekeys []*as.Key
// 			var dkeys []*as.Key

// 			gg.BeforeEach(func() {
// 				bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))
// 				ekeys = []*as.Key{}
// 				dkeys = []*as.Key{}
// 				for i := 0; i < keyCount; i++ {
// 					key, err := as.NewKey(ns, set, randString(50))
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					// make sure they exists in the DB
// 					exists, err := client.Exists(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(exists).To(gm.Equal(true))

// 					// if key shouldExist == true, put it in the DB
// 					if i%2 == 0 {
// 						ekeys = append(ekeys, key)
// 					} else {
// 						dkeys = append(dkeys, key)
// 					}
// 				}
// 			})

// 			gg.It("ZZZXXXmust return the result with same ordering", func() {
// 				res, err := client.BatchDelete(bpolicy, bdpolicy, dkeys)

// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(res).NotTo(gm.BeNil())
// 				gm.Expect(len(res)).To(gm.Equal(len(dkeys)))
// 				for _, br := range res {
// 					gm.Expect(br.ResultCode).To(gm.Equal(types.OK))
// 				}

// 				// true case
// 				exists, err = client.BatchExists(bpolicy, ekeys)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(len(exists)).To(gm.Equal(len(ekeys)))
// 				for _, keyExists := range exists {
// 					gm.Expect(keyExists).To(gm.BeTrue())
// 				}

// 				// false case
// 				exists, err = client.BatchExists(bpolicy, dkeys)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(len(exists)).To(gm.Equal(len(dkeys)))
// 				for _, keyExists := range exists {
// 					gm.Expect(keyExists).To(gm.BeFalse())
// 				}
// 			})
// 		})

// 		gg.Context("BatchOperate operations", func() {
// 			gg.It("must return the result with same ordering", func() {
// 				key1, _ := as.NewKey(ns, set, 1)
// 				op1 := as.NewBatchWrite(nil, key1, as.PutOp(as.NewBin("bin1", "a")), as.PutOp(as.NewBin("bin2", "b")))
// 				op3 := as.NewBatchRead(key1, []string{"bin2"})

// 				key2, _ := as.NewKey(ns, set, 2)
// 				op5 := as.NewBatchWrite(nil, key2, as.PutOp(as.NewBin("bin1", "a")))

// 				brecs := []as.BatchRecordIfc{op1, op3, op5}
// 				err := client.BatchOperate(bpolicy, brecs)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(op1.BatchRec().Err).ToNot(gm.HaveOccurred())
// 				gm.Expect(op1.BatchRec().ResultCode).To(gm.Equal(types.OK))
// 				gm.Expect(op1.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1": nil, "bin2": nil}))
// 				gm.Expect(op1.BatchRec().InDoubt).To(gm.BeFalse())

// 				// gm.Expect(op2.BatchRec().Err).ToNot(gm.HaveOccurred())
// 				// gm.Expect(op2.BatchRec().ResultCode).To(gm.Equal(types.OK))
// 				// gm.Expect(op2.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin2": nil}))
// 				// gm.Expect(op2.BatchRec().InDoubt).To(gm.BeFalse())

// 				gm.Expect(op3.BatchRec().Err).ToNot(gm.HaveOccurred())
// 				gm.Expect(op3.BatchRec().ResultCode).To(gm.Equal(types.OK))
// 				gm.Expect(op3.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin2": "b"}))
// 				gm.Expect(op3.BatchRec().InDoubt).To(gm.BeFalse())

// 				// gm.Expect(op4.BatchRec().Err).ToNot(gm.HaveOccurred())
// 				// gm.Expect(op4.BatchRec().ResultCode).To(gm.Equal(types.OK))
// 				// gm.Expect(op4.BatchRec().InDoubt).To(gm.BeFalse())

// 				// make sure the delete case actually ran
// 				// exists, err := client.Exists(nil, key1)
// 				// gm.Expect(exists).To(gm.BeFalse())

// 				// make sure the delete case actually ran
// 				exists, err := client.Exists(nil, key2)
// 				gm.Expect(exists).To(gm.BeTrue())
// 			})

// 			gg.It("must successfully execute a delete op", func() {
// 				bwPolicy := as.NewBatchWritePolicy()
// 				bdPolicy := as.NewBatchDeletePolicy()

// 				for _, sendKey := range []bool{true, false}	{
// 					bwPolicy.SendKey = sendKey
// 					bdPolicy.SendKey = sendKey
// 					bpolicy.SendKey = !sendKey

// 					key1, _ := as.NewKey(ns, set, 1)
// 					op1 := as.NewBatchWrite(bwPolicy, key1, as.PutOp(as.NewBin("bin1", "a")), as.PutOp(as.NewBin("bin2", "b")))
// 					op2 := as.NewBatchDelete(bdPolicy, key1)
// 					op3 := as.NewBatchRead(key1, []string{"bin2"})

// 					brecs := []as.BatchRecordIfc{op1, op2, op3}
// 					err := client.BatchOperate(bpolicy, brecs)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					gm.Expect(op1.BatchRec().Err).ToNot(gm.HaveOccurred())
// 					gm.Expect(op1.BatchRec().ResultCode).To(gm.Equal(types.OK))
// 					gm.Expect(op1.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1": nil, "bin2": nil}))
// 					gm.Expect(op1.BatchRec().InDoubt).To(gm.BeFalse())

// 					gm.Expect(op2.BatchRec().Err).ToNot(gm.HaveOccurred())
// 					gm.Expect(op2.BatchRec().ResultCode).To(gm.Equal(types.OK))
// 					gm.Expect(op2.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{}))
// 					gm.Expect(op2.BatchRec().InDoubt).To(gm.BeFalse())

// 					gm.Expect(op3.BatchRec().Err).ToNot(gm.HaveOccurred())
// 					gm.Expect(op3.BatchRec().ResultCode).To(gm.Equal(types.KEY_NOT_FOUND_ERROR))
// 					gm.Expect(op3.BatchRec().Record).To(gm.BeNil())
// 					gm.Expect(op3.BatchRec().InDoubt).To(gm.BeFalse())

// 					exists, err := client.Exists(nil, key1)
// 					gm.Expect(exists).To(gm.BeFalse())
// 				}
// 			})

// 			gg.It("must successfully execute ops with policies", func() {
// 				key1, _ := as.NewKey(ns, set, randString(50))
// 				err := client.Put(nil, key1, as.BinMap{"bin1": 1, "bin2": 2})
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// Create the policy
// 				writePolicy := as.NewBatchWritePolicy()
// 				writePolicy.FilterExpression = as.ExpLess(as.ExpIntBin("bin1"), as.ExpIntVal(1))

// 				// Create write operation
// 				record := as.NewBatchWrite(writePolicy, key1,
// 					as.PutOp(as.NewBin("bin3", 3)),
// 					as.PutOp(as.NewBin("bin4", 4)),
// 				)

// 				records := []as.BatchRecordIfc{record}

// 				err = client.BatchOperate(nil, records)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(record.ResultCode).To(gm.Equal(types.FILTERED_OUT))

// 				rec, err := client.Get(nil, key1)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(len(rec.Bins)).To(gm.Equal(2))

// 				// remove the filter

// 				writePolicy.FilterExpression = nil
// 				err = client.BatchOperate(nil, records)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(record.ResultCode).To(gm.Equal(types.OK))

// 				rec, err = client.Get(nil, key1)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(len(rec.Bins)).To(gm.Equal(4))
// 			})
// 		})

// 		gg.Context("BatchOperate operations", func() {

// 			gg.It("Overall command error should be reflected in API call error and not BatchRecord error", func() {
// 				if *grpc {
// 					gg.Skip("Not possible to test for the GRPC CLient")
// 				}

// 				var batchRecords []as.BatchRecordIfc
// 				for i := 0; i < len(client.Cluster().GetNodes()) * 5500; i++ {
// 					key, _ := as.NewKey(*namespace, set, i)
// 					batchRecords = append(batchRecords, as.NewBatchReadHeader(key))
// 				}

// 				err := client.BatchOperate(nil, batchRecords)
// 				gm.Expect(err).To(gm.HaveOccurred())
// 				gm.Expect(err.Matches(types.BATCH_MAX_REQUESTS_EXCEEDED)).To(gm.BeTrue())

// 				for _, bri := range batchRecords {
// 					gm.Expect(bri.BatchRec().ResultCode).To(gm.Equal(types.NO_RESPONSE))
// 				}
// 			})

// 			gg.It("ListGetByValueRangeOp and ListRemoveByValueRangeOp with nil arguments correctly", func() {
// 				const binName = "int_bin"

// 				key, err := as.NewKey(ns, set, "list_key1")
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				l := []int{7, 6, 5, 8, 9, 10}
// 				err = client.PutBins(wpolicy, key, as.NewBin(binName, l))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// Get
// 				op1 := as.ListGetByValueRangeOp(binName, as.NewValue(7), as.NewValue(9), as.ListReturnTypeValue)
// 				op2 := as.ListGetByValueRangeOp(binName, as.NewValue(7), nil, as.ListReturnTypeIndex)
// 				op3 := as.ListGetByValueRangeOp(binName, as.NewValue(7), nil, as.ListReturnTypeValue)
// 				op4 := as.ListGetByValueRangeOp(binName, as.NewValue(7), nil, as.ListReturnTypeRank)
// 				op5 := as.ListGetByValueRangeOp(binName, nil, as.NewValue(9), as.ListReturnTypeValue)
// 				r, err := client.Operate(wpolicy, key, op1, op2, op3, op4, op5)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(r.Bins[binName]).To(gm.Equal([]interface{}{[]interface{}{7, 8}, []interface{}{0, 3, 4, 5}, []interface{}{7, 8, 9, 10}, []interface{}{2, 3, 4, 5}, []interface{}{7, 6, 5, 8}}))

// 				// Remove
// 				op6 := as.ListRemoveByValueRangeOp(binName, as.ListReturnTypeIndex, as.NewValue(7), nil)
// 				r2, err2 := client.Operate(wpolicy, key, op6)
// 				gm.Expect(err2).ToNot(gm.HaveOccurred())
// 				gm.Expect(r2.Bins[binName]).To(gm.Equal([]interface{}{0, 3, 4, 5}))

// 				r3, err3 := client.Get(nil, key)
// 				gm.Expect(err3).ToNot(gm.HaveOccurred())
// 				gm.Expect(r3.Bins[binName]).To(gm.Equal([]interface{}{6, 5}))
// 			})

// 			gg.It("must return the result with same ordering", func() {
// 				const keyCount = 50
// 				keys := []*as.Key{}

// 				for i := 0; i < keyCount; i++ {
// 					bin := as.NewBin("i", i)

// 					key, err := as.NewKey(ns, set, randString(50))
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					// make sure they exists in the DB
// 					exists, err := client.Exists(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(exists).To(gm.Equal(true))

// 					keys = append(keys, key)
// 				}

// 				for i, key := range keys {
// 					op1 := as.NewBatchWrite(nil, key, as.PutOp(as.NewBin("bin1", "a")))
// 					op2 := as.NewBatchWrite(nil, key, as.PutOp(as.NewBin("bin2", "b")))
// 					op3 := as.NewBatchRead(key, []string{"bin2"})

// 					bpolicy.FilterExpression = as.ExpLess(
// 						as.ExpIntBin("i"),
// 						as.ExpIntVal(3),
// 					)

// 					brecs := []as.BatchRecordIfc{op1, op2, op3}
// 					err := client.BatchOperate(bpolicy, brecs)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					for _, rec := range brecs {
// 						if i < 3 {
// 							gm.Expect(rec.BatchRec().ResultCode).To(gm.Equal(types.OK))
// 						} else {
// 							gm.Expect(rec.BatchRec().ResultCode).To(gm.Equal(types.FILTERED_OUT))
// 						}
// 					}
// 				}
// 			})
// 		})

// 		gg.Context("BatchUDF operations", func() {
// 			gg.It("must return the results when one operation is against an invalid namespace", func() {
// 				luaCode := []byte(`-- Create a record
// 				function rec_create(rec, bins)
// 				    if bins ~= nil then
// 				        for b, bv in map.pairs(bins) do
// 				            rec[b] = bv
// 				        end
// 				    end
// 				    status = aerospike:create(rec)
// 				    return status
// 				end`)

// 				client.RemoveUDF(nil, "test_ops.lua")
// 				client.RegisterUDF(nil, luaCode, "test_ops.lua", as.LUA)

// 				batchRecords := []as.BatchRecordIfc{}

// 				key1, _ := as.NewKey(randString(10), set, 1)
// 				args := make(map[interface{}]interface{})
// 				args["bin1_str"] = "a"
// 				batchRecords = append(batchRecords, as.NewBatchUDF(
// 					nil,
// 					key1,
// 					"test_ops",
// 					"rec_create",
// 					as.NewMapValue(args),
// 				))

// 				key2, _ := as.NewKey(ns, set, 2)
// 				batchRecords = append(batchRecords, as.NewBatchWrite(
// 					nil,
// 					key2,
// 					as.PutOp(as.NewBin("bin1_str", "aa")),
// 				))

// 				key3, _ := as.NewKey(ns, set, 3)
// 				batchRecords = append(batchRecords, as.NewBatchWrite(
// 					nil,
// 					key3,
// 					as.PutOp(as.NewBin("bin1_str", "aaa")),
// 				))

// 				batchRecords = append(batchRecords, as.NewBatchRead(
// 					key1,
// 					[]string{"bin1_str"},
// 				))

// 				batchRecords = append(batchRecords, as.NewBatchRead(
// 					key2,
// 					[]string{"bin1_str"},
// 				))

// 				batchRecords = append(batchRecords, as.NewBatchRead(
// 					key3,
// 					[]string{"bin1_str"},
// 				))

// 				bp := as.NewBatchPolicy()
// 				bp.RespondAllKeys = false
// 				err := client.BatchOperate(bp, batchRecords)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(batchRecords[0].BatchRec().Err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
// 				gm.Expect(batchRecords[0].BatchRec().ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))

// 				gm.Expect(batchRecords[1].BatchRec().Err).To(gm.BeNil())
// 				gm.Expect(batchRecords[1].BatchRec().ResultCode).To(gm.Equal(types.OK))
// 				gm.Expect(batchRecords[1].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": nil}))

// 				gm.Expect(batchRecords[2].BatchRec().Err).To(gm.BeNil())
// 				gm.Expect(batchRecords[2].BatchRec().ResultCode).To(gm.Equal(types.OK))
// 				gm.Expect(batchRecords[2].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": nil}))

// 				gm.Expect(batchRecords[3].BatchRec().Err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
// 				gm.Expect(batchRecords[3].BatchRec().ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))

// 				gm.Expect(batchRecords[4].BatchRec().Err).To(gm.BeNil())
// 				gm.Expect(batchRecords[4].BatchRec().ResultCode).To(gm.Equal(types.OK))
// 				gm.Expect(batchRecords[4].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": "aa"}))

// 				gm.Expect(batchRecords[5].BatchRec().Err).To(gm.BeNil())
// 				gm.Expect(batchRecords[5].BatchRec().ResultCode).To(gm.Equal(types.OK))
// 				gm.Expect(batchRecords[5].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1_str": "aaa"}))

// 				bp.RespondAllKeys = true
// 				err = client.BatchOperate(bp, batchRecords)
// 				gm.Expect(err).To(gm.HaveOccurred())
// 				gm.Expect(err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
// 			})

// 			gg.It("must return the result with same ordering", func() {
// 				const keyCount = 50
// 				keys := []*as.Key{}

// 				regTask, err := client.RegisterUDF(wpolicy, []byte(udfBody), "udf1.lua", as.LUA)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// wait until UDF is created
// 				gm.Expect(<-regTask.OnComplete()).NotTo(gm.HaveOccurred())

// 				for i := 0; i < keyCount; i++ {
// 					bin := as.NewBin("bin1", i*6)

// 					key, err := as.NewKey(ns, set, randString(50))
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					// make sure they exists in the DB
// 					exists, err := client.Exists(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(exists).To(gm.Equal(true))

// 					keys = append(keys, key)
// 				}

// 				brecs, err := client.BatchExecute(bpolicy, nil, keys, "udf1", "testFunc1", as.NewValue(2))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				for _, rec := range brecs {
// 					gm.Expect(rec.Err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.ResultCode).To(gm.Equal(types.OK))
// 					gm.Expect(rec.InDoubt).To(gm.BeFalse())
// 					gm.Expect(rec.Record.Bins["SUCCESS"]).To(gm.Equal(map[interface{}]interface{}{"status": "OK"}))
// 				}

// 				recs, err := client.BatchGet(nil, keys)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(len(recs)).To(gm.Equal(len(keys)))
// 				for i, rec := range recs {
// 					gm.Expect(rec.Bins["bin2"].(int)).To(gm.Equal(i * 3))
// 				}
// 			})
// 		})
// 	})

// 	gg.Describe("Data operations on native types", func() {
// 		// connection data
// 		var err error
// 		var ns = *namespace
// 		var set = randString(50)
// 		var key *as.Key
// 		var wpolicy = as.NewWritePolicy(0, 0)
// 		var rpolicy = as.NewPolicy()
// 		var bpolicy = as.NewBatchPolicy()
// 		var rec *as.Record

// 		if *useReplicas {
// 			rpolicy.ReplicaPolicy = as.MASTER_PROLES
// 		}

// 		gg.BeforeEach(func() {
// 			key, err = as.NewKey(ns, set, randString(50))
// 			gm.Expect(err).ToNot(gm.HaveOccurred())
// 		})

// 		gg.Context("Put operations", func() {

// 			gg.Context("Expiration values", func() {

// 				// TODO: SKIP
// 				// gg.It("must return 30d if set to TTLServerDefault", func() {
// 				// 	wpolicy := as.NewWritePolicy(0, as.TTLServerDefault)
// 				// 	bin := as.NewBin("Aerospike", "value")
// 				// 	rec, err = client.Operate(wpolicy, key, as.PutOp(bin), as.GetOp())
// 				// 	gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// 	defaultTTL, err := strconv.Atoi(nsInfo(ns, "default-ttl"))
// 				// 	gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// 	switch defaultTTL {
// 				// 	case 0:
// 				// 		gm.Expect(rec.Expiration).To(gm.Equal(uint32(math.MaxUint32)))
// 				// 	default:
// 				// 		gm.Expect(rec.Expiration).To(gm.Equal(uint32(defaultTTL)))
// 				// 	}

// 				// })

// 				gg.It("must return TTLDontExpire if set to TTLDontExpire", func() {
// 					wpolicy := as.NewWritePolicy(0, as.TTLDontExpire)
// 					bin := as.NewBin("Aerospike", "value")
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Expiration).To(gm.Equal(uint32(as.TTLDontExpire)))
// 				})

// 				// TODO: Implement
// 				// gg.It("must not change the TTL if set to TTLDontUpdate", func() {
// 				// 	wpolicy := as.NewWritePolicy(0, as.TTLServerDefault)
// 				// 	bin := as.NewBin("Aerospike", "value")
// 				// 	err = client.PutBins(wpolicy, key, bin)
// 				// 	gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// 	time.Sleep(3 * time.Second)

// 				// 	wpolicy = as.NewWritePolicy(0, as.TTLDontUpdate)
// 				// 	bin = as.NewBin("Aerospike", "value")
// 				// 	err = client.PutBins(wpolicy, key, bin)
// 				// 	gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// 	defaultTTL, err := strconv.Atoi(nsInfo(ns, "default-ttl"))
// 				// 	gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// 	rec, err = client.Get(rpolicy, key)
// 				// 	gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// 	switch defaultTTL {
// 				// 	case 0:
// 				// 		gm.Expect(rec.Expiration).To(gm.Equal(uint32(math.MaxUint32)))
// 				// 	default:
// 				// 		gm.Expect(rec.Expiration).To(gm.BeNumerically("<=", uint32(defaultTTL-3))) // default expiration on server is set to 30d
// 				// 	}
// 				// })
// 			})

// 			gg.Context("Bins with `nil` values should be deleted", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", "value")
// 					bin1 := as.NewBin("Aerospike1", "value2") // to avoid deletion of key
// 					err = client.PutBins(wpolicy, key, bin, bin1)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))

// 					bin2 := as.NewBin("Aerospike", nil)
// 					err = client.PutBins(wpolicy, key, bin2)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					// Key should not exist
// 					_, exists := rec.Bins[bin.Name]
// 					gm.Expect(exists).To(gm.Equal(false))
// 				})

// 				gg.It("must save a key with MULTIPLE bins", func() {
// 					bin1 := as.NewBin("Aerospike1", "nil")
// 					bin2 := as.NewBin("Aerospike2", "value")
// 					bin3 := as.NewBin("Aerospike3", "value")
// 					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					bin2nil := as.NewBin("Aerospike2", nil)
// 					bin3nil := as.NewBin("Aerospike3", nil)
// 					err = client.PutBins(wpolicy, key, bin2nil, bin3nil)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					// Key should not exist
// 					_, exists := rec.Bins[bin2.Name]
// 					gm.Expect(exists).To(gm.Equal(false))
// 					_, exists = rec.Bins[bin3.Name]
// 					gm.Expect(exists).To(gm.Equal(false))
// 				})

// 				gg.It("must save a key with MULTIPLE bins using a BinMap", func() {
// 					bin1 := as.NewBin("Aerospike1", "nil")
// 					bin2 := as.NewBin("Aerospike2", "value")
// 					bin3 := as.NewBin("Aerospike3", "value")
// 					err = client.Put(wpolicy, key, as.BinMap{bin1.Name: bin1.Value, bin2.Name: bin2.Value, bin3.Name: bin3.Value})
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					bin2nil := as.NewBin("Aerospike2", nil)
// 					bin3nil := as.NewBin("Aerospike3", nil)
// 					err = client.Put(wpolicy, key, as.BinMap{bin2nil.Name: bin2nil.Value, bin3nil.Name: bin3nil.Value})
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					// Key should not exist
// 					_, exists := rec.Bins[bin2.Name]
// 					gm.Expect(exists).To(gm.Equal(false))
// 					_, exists = rec.Bins[bin3.Name]
// 					gm.Expect(exists).To(gm.Equal(false))
// 				})
// 			})

// 			gg.Context("Bins with `string` values", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", "Awesome")
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 				})

// 				gg.It("must save a key with MULTIPLE bins", func() {
// 					bin1 := as.NewBin("Aerospike1", "Awesome1")
// 					bin2 := as.NewBin("Aerospike2", "")
// 					err = client.PutBins(wpolicy, key, bin1, bin2)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
// 				})
// 			})

// 			gg.Context("Bins with `int8` and `uint8` values", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", int8(rand.Intn(math.MaxInt8)))
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 				})

// 				gg.It("must save a key with MULTIPLE bins", func() {
// 					bin1 := as.NewBin("Aerospike1", int8(math.MaxInt8))
// 					bin2 := as.NewBin("Aerospike2", int8(math.MinInt8))
// 					bin3 := as.NewBin("Aerospike3", uint8(math.MaxUint8))
// 					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin3.Name]).To(gm.Equal(bin3.Value.GetObject()))
// 				})
// 			})

// 			gg.Context("Bins with `int16` and `uint16` values", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", int16(rand.Intn(math.MaxInt16)))
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 				})

// 				gg.It("must save a key with MULTIPLE bins", func() {
// 					bin1 := as.NewBin("Aerospike1", int16(math.MaxInt16))
// 					bin2 := as.NewBin("Aerospike2", int16(math.MinInt16))
// 					bin3 := as.NewBin("Aerospike3", uint16(math.MaxUint16))
// 					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin3.Name]).To(gm.Equal(bin3.Value.GetObject()))
// 				})
// 			})

// 			gg.Context("Bins with `int` and `uint` values", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", rand.Int())
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 				})

// 				gg.It("must save a key with MULTIPLE bins; uint of > MaxInt32 will always result in LongValue", func() {
// 					bin1 := as.NewBin("Aerospike1", math.MaxInt32)
// 					bin2, bin3 := func() (*as.Bin, *as.Bin) {
// 						if asub.Arch32Bits {
// 							return as.NewBin("Aerospike2", int(math.MinInt32)),
// 								as.NewBin("Aerospike3", uint(math.MaxInt32))
// 						}
// 						return as.NewBin("Aerospike2", int(math.MinInt64)),
// 							as.NewBin("Aerospike3", uint(math.MaxInt64))

// 					}()

// 					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
// 					if asub.Arch64Bits {
// 						gm.Expect(rec.Bins[bin2.Name].(int)).To(gm.Equal(bin2.Value.GetObject()))
// 						gm.Expect(int64(rec.Bins[bin3.Name].(int))).To(gm.Equal(bin3.Value.GetObject()))
// 					} else {
// 						gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
// 						gm.Expect(rec.Bins[bin3.Name]).To(gm.Equal(bin3.Value.GetObject()))
// 					}
// 				})
// 			})

// 			gg.Context("Bins with `int64` only values (uint64 is supported via type cast to int64) ", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", rand.Int63())
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					if asub.Arch64Bits {
// 						gm.Expect(int64(rec.Bins[bin.Name].(int))).To(gm.Equal(bin.Value.GetObject()))
// 					} else {
// 						gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 					}
// 				})

// 				gg.It("must save a key with MULTIPLE bins", func() {
// 					bin1 := as.NewBin("Aerospike1", math.MaxInt64)
// 					bin2 := as.NewBin("Aerospike2", math.MinInt64)
// 					err = client.PutBins(wpolicy, key, bin1, bin2)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
// 				})
// 			})

// 			gg.Context("Bins with `float32` only values", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", rand.Float32())
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(float64(rec.Bins[bin.Name].(float64))).To(gm.Equal(bin.Value.GetObject()))
// 				})

// 				gg.It("must save a key with MULTIPLE bins", func() {
// 					bin1 := as.NewBin("Aerospike1", math.MaxFloat32)
// 					bin2 := as.NewBin("Aerospike2", -math.MaxFloat32)
// 					err = client.PutBins(wpolicy, key, bin1, bin2)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
// 				})
// 			})

// 			gg.Context("Bins with `float64` only values", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", rand.Float64())
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(float64(rec.Bins[bin.Name].(float64))).To(gm.Equal(bin.Value.GetObject()))
// 				})

// 				gg.It("must save a key with MULTIPLE bins", func() {
// 					bin1 := as.NewBin("Aerospike1", math.MaxFloat64)
// 					bin2 := as.NewBin("Aerospike2", -math.MaxFloat64)
// 					err = client.PutBins(wpolicy, key, bin1, bin2)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
// 				})
// 			})

// 			gg.Context("Bins with `bool` only values", func() {
// 				gg.It("must save a key with SINGLE bin", func() {
// 					bin := as.NewBin("Aerospike", as.BoolValue(true))
// 					err = client.PutBins(wpolicy, key, bin)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(bool(rec.Bins[bin.Name].(bool))).To(gm.Equal(bin.Value.GetObject()))
// 				})

// 				gg.It("must save a key with MULTIPLE bins", func() {
// 					bin1 := as.NewBin("Aerospike1", as.BoolValue(true))
// 					bin2 := as.NewBin("Aerospike2", as.BoolValue(false))
// 					err = client.PutBins(wpolicy, key, bin1, bin2)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					rec, err = client.Get(rpolicy, key)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())

// 					gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
// 					gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
// 				})
// 			})

// 			gg.Context("Bins with complex types", func() {

// 				gg.Context("Bins with BLOB type", func() {
// 					gg.It("must save and retrieve Bins with AerospikeBlobs type", func() {
// 						person := &testBLOB{name: "SomeDude"}
// 						bin := as.NewBin("Aerospike1", person)
// 						err = client.PutBins(wpolicy, key, bin)
// 						gm.Expect(err).ToNot(gm.HaveOccurred())

// 						rec, err = client.Get(rpolicy, key)
// 						gm.Expect(err).ToNot(gm.HaveOccurred())
// 					})
// 				})

// 				gg.Context("Bins with LIST type", func() {

// 					gg.It("must save a key with Array Types", func() {
// 						// All int types and sizes should be encoded into an int64,
// 						// unless if they are of type uint64, which always encodes to uint64
// 						// regardless of the values inside
// 						intList := []interface{}{math.MinInt64, math.MinInt64 + 1}
// 						for i := uint(0); i < 64; i++ {
// 							intList = append(intList, -(1 << i))
// 							intList = append(intList, -(1<<i)-1)
// 							intList = append(intList, -(1<<i)+1)
// 							intList = append(intList, 1<<i)
// 							intList = append(intList, (1<<i)-1)
// 							intList = append(intList, (1<<i)+1)
// 						}
// 						intList = append(intList, -1)
// 						intList = append(intList, 0)
// 						intList = append(intList, uint64(1))
// 						intList = append(intList, math.MaxInt64-1)
// 						intList = append(intList, math.MaxInt64)
// 						intList = append(intList, uint64(math.MaxInt64+1))
// 						intList = append(intList, uint64(math.MaxUint64-1))
// 						intList = append(intList, uint64(math.MaxUint64))
// 						bin0 := as.NewBin("Aerospike0", intList)

// 						bin1 := as.NewBin("Aerospike1", []interface{}{math.MinInt8, 0, 1, 2, 3, math.MaxInt8})
// 						bin2 := as.NewBin("Aerospike2", []interface{}{math.MinInt16, 0, 1, 2, 3, math.MaxInt16})
// 						bin3 := as.NewBin("Aerospike3", []interface{}{math.MinInt32, 0, 1, 2, 3, math.MaxInt32})
// 						bin4 := as.NewBin("Aerospike4", []interface{}{math.MinInt64, 0, 1, 2, 3, math.MaxInt64})
// 						bin5 := as.NewBin("Aerospike5", []interface{}{0, 1, 2, 3, math.MaxUint8})
// 						bin6 := as.NewBin("Aerospike6", []interface{}{0, 1, 2, 3, math.MaxUint16})
// 						bin7 := as.NewBin("Aerospike7", []interface{}{0, 1, 2, 3, math.MaxUint32})
// 						bin8 := as.NewBin("Aerospike8", []interface{}{"", "\n", "string"})
// 						bin9 := as.NewBin("Aerospike9", []interface{}{"", 1, nil, true, false, uint64(math.MaxUint64), math.MaxFloat32, math.MaxFloat64, as.NewGeoJSONValue(`{ "type": "Point", "coordinates": [0.00, 0.00] }"`), []interface{}{1, 2, 3}})

// 						// complex type, consisting different arrays
// 						bin10 := as.NewBin("Aerospike10", []interface{}{
// 							nil,
// 							bin0.Value.GetObject(),
// 							bin1.Value.GetObject(),
// 							bin2.Value.GetObject(),
// 							bin3.Value.GetObject(),
// 							bin4.Value.GetObject(),
// 							bin5.Value.GetObject(),
// 							bin6.Value.GetObject(),
// 							bin7.Value.GetObject(),
// 							bin8.Value.GetObject(),
// 							bin9.Value.GetObject(),
// 							map[interface{}]interface{}{
// 								1: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15},
// 								// [3]int{0, 1, 2}:          []interface{}{"string", 12, nil},
// 								// [3]string{"0", "1", "2"}: []interface{}{"string", 12, nil},
// 								15:                        nil,
// 								int8(math.MaxInt8):        int8(math.MaxInt8),
// 								int64(math.MinInt64):      int64(math.MinInt64),
// 								int64(math.MaxInt64):      int64(math.MaxInt64),
// 								uint64(math.MaxUint64):    uint64(math.MaxUint64),
// 								float32(-math.MaxFloat32): float32(-math.MaxFloat32),
// 								float64(-math.MaxFloat64): float64(-math.MaxFloat64),
// 								float32(math.MaxFloat32):  float32(math.MaxFloat32),
// 								float64(math.MaxFloat64):  float64(math.MaxFloat64),
// 								"true":                    true,
// 								"false":                   false,
// 								"string":                  map[interface{}]interface{}{nil: "string", "string": 19},                // map to complex array
// 								nil:                       []interface{}{18, 41},                                                   // array to complex map
// 								"GeoJSON":                 as.NewGeoJSONValue(`{ "type": "Point", "coordinates": [0.00, 0.00] }"`), // bit-sign test
// 								"intList":                 intList,
// 							},
// 						})

// 						err = client.PutBins(wpolicy, key, bin0, bin1, bin2, bin3, bin4, bin5, bin6, bin7, bin8, bin9, bin10)
// 						gm.Expect(err).ToNot(gm.HaveOccurred())

// 						rec, err = client.Get(rpolicy, key)
// 						gm.Expect(err).ToNot(gm.HaveOccurred())

// 						arraysEqual(rec.Bins[bin0.Name], bin0.Value.GetObject())
// 						arraysEqual(rec.Bins[bin1.Name], bin1.Value.GetObject())
// 						arraysEqual(rec.Bins[bin2.Name], bin2.Value.GetObject())
// 						arraysEqual(rec.Bins[bin3.Name], bin3.Value.GetObject())
// 						arraysEqual(rec.Bins[bin4.Name], bin4.Value.GetObject())
// 						arraysEqual(rec.Bins[bin5.Name], bin5.Value.GetObject())
// 						arraysEqual(rec.Bins[bin6.Name], bin6.Value.GetObject())
// 						arraysEqual(rec.Bins[bin7.Name], bin7.Value.GetObject())
// 						arraysEqual(rec.Bins[bin8.Name], bin8.Value.GetObject())
// 						arraysEqual(rec.Bins[bin9.Name], bin9.Value.GetObject())
// 						arraysEqual(rec.Bins[bin10.Name], bin10.Value.GetObject())
// 					})

// 				}) // context list

// 				gg.Context("Bins with MAP type", func() {

// 					gg.It("must save a key with Array Types", func() {
// 						// complex type, consisting different maps
// 						bin1 := as.NewBin("Aerospike1", map[interface{}]interface{}{
// 							0:                    "",
// 							int32(math.MaxInt32): randString(100),
// 							int32(math.MinInt32): randString(100),
// 						})

// 						bin2 := as.NewBin("Aerospike2", map[interface{}]interface{}{
// 							15:                        nil,
// 							"true":                    true,
// 							"false":                   false,
// 							int8(math.MaxInt8):        int8(math.MaxInt8),
// 							int64(math.MinInt64):      int64(math.MinInt64),
// 							int64(math.MaxInt64):      int64(math.MaxInt64),
// 							uint64(math.MaxUint64):    uint64(math.MaxUint64),
// 							float32(-math.MaxFloat32): float32(-math.MaxFloat32),
// 							float64(-math.MaxFloat64): float64(-math.MaxFloat64),
// 							float32(math.MaxFloat32):  float32(math.MaxFloat32),
// 							float64(math.MaxFloat64):  float64(math.MaxFloat64),
// 							"string":                  map[interface{}]interface{}{nil: "string", "string": 19},                // map to complex array
// 							nil:                       []interface{}{18, 41},                                                   // array to complex map
// 							"longString":              strings.Repeat("s", 32911),                                              // bit-sign test
// 							"GeoJSON":                 as.NewGeoJSONValue(`{ "type": "Point", "coordinates": [0.00, 0.00] }"`), // bit-sign test
// 						})

// 						err = client.PutBins(wpolicy, key, bin1, bin2)
// 						gm.Expect(err).ToNot(gm.HaveOccurred())

// 						rec, err = client.Get(rpolicy, key)
// 						gm.Expect(err).ToNot(gm.HaveOccurred())

// 						mapsEqual(rec.Bins[bin1.Name], bin1.Value.GetObject())
// 						mapsEqual(rec.Bins[bin2.Name], bin2.Value.GetObject())
// 					})

// 				}) // context map

// 			}) // context complex types

// 		}) // put context

// 		gg.Context("Get operations", func() {
// 			gg.It("must get only requested bins", func() {
// 				bins := as.BinMap{
// 					"bin1": 1,
// 					"bin2": 2,
// 					"bin3": 3,
// 					"bin4": 4,
// 				}

// 				err := client.Put(wpolicy, key, bins)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err := client.Get(rpolicy, key, "bin1", "bin2")
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(len(rec.Bins)).To(gm.Equal(2))
// 				gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"bin1": 1, "bin2": 2}))
// 			})
// 		})

// 		gg.Context("Append operations", func() {
// 			bin := as.NewBin("Aerospike", randString(rand.Intn(100)))

// 			gg.BeforeEach(func() {
// 				err = client.PutBins(wpolicy, key, bin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 			})

// 			gg.It("must append to a SINGLE bin", func() {
// 				appbin := as.NewBin(bin.Name, randString(rand.Intn(100)))
// 				err = client.AppendBins(wpolicy, key, appbin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject().(string) + appbin.Value.GetObject().(string)))
// 			})

// 			gg.It("must append to a SINGLE bin using a BinMap", func() {
// 				appbin := as.NewBin(bin.Name, randString(rand.Intn(100)))
// 				err = client.Append(wpolicy, key, as.BinMap{bin.Name: appbin.Value})
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject().(string) + appbin.Value.GetObject().(string)))
// 			})

// 		}) // append context

// 		gg.Context("Prepend operations", func() {
// 			bin := as.NewBin("Aerospike", randString(rand.Intn(100)))

// 			gg.BeforeEach(func() {
// 				err = client.PutBins(wpolicy, key, bin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 			})

// 			gg.It("must Prepend to a SINGLE bin", func() {
// 				appbin := as.NewBin(bin.Name, randString(rand.Intn(100)))
// 				err = client.PrependBins(wpolicy, key, appbin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(appbin.Value.GetObject().(string) + bin.Value.GetObject().(string)))
// 			})

// 			gg.It("must Prepend to a SINGLE bin using a BinMap", func() {
// 				appbin := as.NewBin(bin.Name, randString(rand.Intn(100)))
// 				err = client.Prepend(wpolicy, key, as.BinMap{bin.Name: appbin.Value})
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(appbin.Value.GetObject().(string) + bin.Value.GetObject().(string)))
// 			})

// 		}) // prepend context

// 		gg.Context("Add operations", func() {
// 			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

// 			gg.BeforeEach(func() {
// 				err = client.PutBins(wpolicy, key, bin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 			})

// 			gg.It("must Add to a SINGLE bin", func() {
// 				addBin := as.NewBin(bin.Name, rand.Intn(math.MaxInt16))
// 				err = client.AddBins(wpolicy, key, addBin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(addBin.Value.GetObject().(int) + bin.Value.GetObject().(int)))
// 			})

// 			gg.It("must Add to a SINGLE bin using a BinMap", func() {
// 				addBin := as.NewBin(bin.Name, rand.Intn(math.MaxInt16))
// 				err = client.Add(wpolicy, key, as.BinMap{addBin.Name: addBin.Value})
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(addBin.Value.GetObject().(int) + bin.Value.GetObject().(int)))
// 			})

// 		}) // add context

// 		gg.Context("Delete operations", func() {
// 			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

// 			gg.BeforeEach(func() {
// 				err = client.PutBins(wpolicy, key, bin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 			})

// 			gg.It("must Delete a non-existing key", func() {
// 				var nxkey *as.Key
// 				nxkey, err = as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				var existed bool
// 				existed, err = client.Delete(wpolicy, nxkey)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(existed).To(gm.Equal(false))
// 			})

// 			gg.It("must Delete an existing key", func() {
// 				var existed bool
// 				existed, err = client.Delete(wpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(existed).To(gm.Equal(true))

// 				existed, err = client.Exists(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(existed).To(gm.Equal(false))
// 			})

// 		}) // Delete context

// 		gg.Context("Touch operations", func() {
// 			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

// 			gg.BeforeEach(func() {
// 				err = client.PutBins(wpolicy, key, bin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 			})

// 			gg.It("must Touch a non-existing key", func() {
// 				var nxkey *as.Key
// 				nxkey, err = as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				err = client.Touch(wpolicy, nxkey)
// 				gm.Expect(errors.Is(err, as.ErrKeyNotFound)).To(gm.BeTrue())
// 			})

// 			gg.It("must Touch an existing key", func() {
// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				generation := rec.Generation

// 				wpolicy := as.NewWritePolicy(0, 0)
// 				wpolicy.SendKey = true
// 				err = client.Touch(wpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(rec.Generation).To(gm.BeNumerically(">", generation))

// 				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// make sure the
// 				for r := range recordset.Results() {
// 					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
// 					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
// 						gm.Expect(r.Record.Key.Value()).To(gm.Equal(key.Value()))
// 						gm.Expect(r.Record.Bins).To(gm.Equal(rec.Bins))
// 					}
// 				}
// 			})

// 		}) // Touch context

// 		gg.Context("Exists operations", func() {
// 			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

// 			gg.BeforeEach(func() {
// 				err = client.PutBins(wpolicy, key, bin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 			})

// 			gg.It("must check Existence of a non-existing key", func() {
// 				var nxkey *as.Key
// 				nxkey, err = as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				var exists bool
// 				exists, err = client.Exists(rpolicy, nxkey)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(exists).To(gm.Equal(false))
// 			})

// 			gg.It("must checks Existence of an existing key", func() {
// 				var exists bool
// 				exists, err = client.Exists(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(exists).To(gm.Equal(true))
// 			})

// 		}) // Exists context

// 		gg.Context("Batch Exists operations", func() {
// 			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))
// 			const keyCount = 2048

// 			gg.BeforeEach(func() {
// 			})

// 			for _, useInline := range []bool{true, false} {
// 				gg.It(fmt.Sprintf("must return the result with same ordering. AllowInline: %v", useInline), func() {
// 					var exists []bool
// 					keys := []*as.Key{}

// 					for i := 0; i < keyCount; i++ {
// 						key, err := as.NewKey(ns, set, randString(50))
// 						gm.Expect(err).ToNot(gm.HaveOccurred())
// 						keys = append(keys, key)

// 						// if key shouldExist == true, put it in the DB
// 						if i%2 == 0 {
// 							err = client.PutBins(wpolicy, key, bin)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())

// 							// make sure they exists in the DB
// 							exists, err := client.Exists(rpolicy, key)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())
// 							gm.Expect(exists).To(gm.Equal(true))
// 						}
// 					}

// 					bpolicy.AllowInline = useInline
// 					exists, err = client.BatchExists(bpolicy, keys)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(len(exists)).To(gm.Equal(len(keys)))
// 					for idx, keyExists := range exists {
// 						gm.Expect(keyExists).To(gm.Equal(idx%2 == 0))
// 					}
// 				})
// 			}

// 		}) // Batch Exists context

// 		gg.Context("Batch Get operations", func() {
// 			bin := as.NewBin("Aerospike", rand.Int())
// 			const keyCount = 2048

// 			gg.BeforeEach(func() {
// 			})

// 			for _, useInline := range []bool{true, false} {
// 				gg.It(fmt.Sprintf("must return the records with same ordering as keys. AllowInline: %v", useInline), func() {
// 					binRedundant := as.NewBin("Redundant", "Redundant")

// 					var records []*as.Record
// 					type existence struct {
// 						key         *as.Key
// 						shouldExist bool // set randomly and checked against later
// 					}

// 					exList := make([]existence, 0, keyCount)
// 					keys := make([]*as.Key, 0, keyCount)

// 					for i := 0; i < keyCount; i++ {
// 						key, err := as.NewKey(ns, set, randString(50))
// 						gm.Expect(err).ToNot(gm.HaveOccurred())
// 						e := existence{key: key, shouldExist: rand.Intn(100) > 50}
// 						exList = append(exList, e)
// 						keys = append(keys, key)

// 						// if key shouldExist == true, put it in the DB
// 						if e.shouldExist {
// 							err = client.PutBins(wpolicy, key, bin, binRedundant)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())

// 							// make sure they exists in the DB
// 							rec, err := client.Get(rpolicy, key)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())
// 							gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 							gm.Expect(rec.Bins[binRedundant.Name]).To(gm.Equal(binRedundant.Value.GetObject()))
// 						} else {
// 							// make sure they exists in the DB
// 							exists, err := client.Exists(rpolicy, key)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())
// 							gm.Expect(exists).To(gm.Equal(false))
// 						}
// 					}

// 					brecords := make([]*as.BatchRead, len(keys))
// 					for i := range keys {
// 						brecords[i] = &as.BatchRead{
// 							BatchRecord: as.BatchRecord{
// 								Key: keys[i],
// 							},
// 							ReadAllBins: true,
// 						}
// 					}
// 					bpolicy.AllowInline = useInline
// 					err = client.BatchGetComplex(bpolicy, brecords)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					for idx, rec := range brecords {
// 						if exList[idx].shouldExist {
// 							gm.Expect(rec).NotTo(gm.BeNil())
// 							gm.Expect(rec.Record).NotTo(gm.BeNil())
// 							gm.Expect(len(rec.Record.Bins)).To(gm.Equal(2))
// 							gm.Expect(rec.Record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
// 						} else {
// 							gm.Expect(rec.Record).To(gm.BeNil())
// 						}
// 					}

// 					brecords = make([]*as.BatchRead, len(keys))
// 					for i := range keys {
// 						brecords[i] = &as.BatchRead{
// 							BatchRecord: as.BatchRecord{
// 								Key: keys[i],
// 							},
// 							ReadAllBins: false,
// 							BinNames:    []string{bin.Name},
// 						}
// 					}
// 					err = client.BatchGetComplex(bpolicy, brecords)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					for idx, rec := range brecords {
// 						if exList[idx].shouldExist {
// 							gm.Expect(len(rec.Record.Bins)).To(gm.Equal(1))
// 							gm.Expect(rec.Record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
// 						} else {
// 							gm.Expect(rec.Record).To(gm.BeNil())
// 						}
// 					}

// 					records, err = client.BatchGet(bpolicy, keys)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(len(records)).To(gm.Equal(len(keys)))
// 					for idx, rec := range records {
// 						if exList[idx].shouldExist {
// 							gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 							gm.Expect(rec.Key).To(gm.Equal(keys[idx]))
// 						} else {
// 							gm.Expect(rec).To(gm.BeNil())
// 						}
// 					}

// 					records, err = client.BatchGet(bpolicy, keys, bin.Name)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(len(records)).To(gm.Equal(len(keys)))
// 					for idx, rec := range records {
// 						if exList[idx].shouldExist {
// 							// only bin1 has been requested
// 							gm.Expect(rec.Bins[binRedundant.Name]).To(gm.BeNil())
// 							gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 							gm.Expect(rec.Key).To(gm.Equal(keys[idx]))
// 						} else {
// 							gm.Expect(rec).To(gm.BeNil())
// 						}
// 					}
// 				})

// 				gg.It(fmt.Sprintf("must return the records with same ordering as keys via Batch Complex Protocol. AllowInline: %v", useInline), func() {
// 					binRedundant := as.NewBin("Redundant", "Redundant")

// 					type existence struct {
// 						key         *as.Key
// 						shouldExist bool // set randomly and checked against later
// 					}

// 					exList := make([]existence, 0, keyCount)
// 					keys := make([]*as.Key, 0, keyCount)

// 					for i := 0; i < keyCount; i++ {
// 						key, err := as.NewKey(ns, set, randString(50))
// 						gm.Expect(err).ToNot(gm.HaveOccurred())
// 						e := existence{key: key, shouldExist: rand.Intn(100) > 50}
// 						exList = append(exList, e)
// 						keys = append(keys, key)

// 						// if key shouldExist == true, put it in the DB
// 						if e.shouldExist {
// 							err = client.PutBins(wpolicy, key, bin, binRedundant)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())

// 							// make sure they exists in the DB
// 							rec, err := client.Get(rpolicy, key)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())
// 							gm.Expect(rec.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 							gm.Expect(rec.Bins[binRedundant.Name]).To(gm.Equal(binRedundant.Value.GetObject()))
// 						} else {
// 							// make sure they exists in the DB
// 							exists, err := client.Exists(rpolicy, key)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())
// 							gm.Expect(exists).To(gm.Equal(false))
// 						}
// 					}

// 					brecords := make([]*as.BatchRead, len(keys))
// 					for i := range keys {
// 						brecords[i] = &as.BatchRead{
// 							BatchRecord: as.BatchRecord{
// 								Key: keys[i],
// 							},
// 							ReadAllBins: true,
// 						}
// 					}
// 					bpolicy.AllowInline = useInline
// 					err = client.BatchGetComplex(bpolicy, brecords)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					for idx, rec := range brecords {
// 						if exList[idx].shouldExist {
// 							gm.Expect(rec).NotTo(gm.BeNil())
// 							gm.Expect(rec.Record).NotTo(gm.BeNil())
// 							gm.Expect(rec.Record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
// 						} else {
// 							gm.Expect(rec.Record).To(gm.BeNil())
// 						}
// 					}

// 					brecords = make([]*as.BatchRead, len(keys))
// 					for i := range keys {
// 						brecords[i] = &as.BatchRead{
// 							BatchRecord: as.BatchRecord{
// 								Key: keys[i],
// 							},
// 							ReadAllBins: false,
// 							BinNames:    []string{"Aerospike", "Redundant"},
// 						}
// 					}
// 					bpolicy.AllowInline = useInline
// 					err = client.BatchGetComplex(bpolicy, brecords)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					for idx, rec := range brecords {
// 						if exList[idx].shouldExist {
// 							gm.Expect(rec.Record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))
// 							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
// 						} else {
// 							gm.Expect(rec.Record).To(gm.BeNil())
// 						}
// 					}

// 					brecords = make([]*as.BatchRead, len(keys))
// 					for i := range keys {
// 						brecords[i] = &as.BatchRead{
// 							BatchRecord: as.BatchRecord{
// 								Key: keys[i],
// 							},
// 							ReadAllBins: false,
// 							BinNames:    nil,
// 						}
// 					}
// 					bpolicy.AllowInline = useInline
// 					err = client.BatchGetComplex(bpolicy, brecords)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					for idx, rec := range brecords {
// 						if exList[idx].shouldExist {
// 							gm.Expect(len(rec.Record.Bins)).To(gm.Equal(0))
// 							gm.Expect(rec.Record.Key).To(gm.Equal(keys[idx]))
// 						} else {
// 							gm.Expect(rec.Record).To(gm.BeNil())
// 						}
// 					}

// 				})
// 			}
// 		}) // Batch Get context

// 		gg.Context("GetHeader operations", func() {
// 			bin := as.NewBin("Aerospike", rand.Intn(math.MaxInt16))

// 			gg.BeforeEach(func() {
// 				err = client.PutBins(wpolicy, key, bin)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 			})

// 			gg.It("must Get the Header of an existing key after touch", func() {
// 				rec, err = client.Get(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				generation := rec.Generation

// 				err = client.Touch(wpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				rec, err = client.GetHeader(rpolicy, key)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(rec.Generation).To(gm.BeNumerically(">", generation))
// 				gm.Expect(rec.Bins[bin.Name]).To(gm.BeNil())
// 			})

// 		}) // GetHeader context

// 		gg.Context("BatchOperate", func() {

// 			gg.It("must execute BatchGetOperate with Operations", func() {
// 				const keyCount = 10
// 				const listSize = 10
// 				const cdtBinName = "cdtBin"

// 				keys := make([]*as.Key, 10)
// 				for i := 0; i < keyCount; i++ {
// 					keys[i], err = as.NewKey(ns, set, randString(10))
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 				}

// 				// First Part: For CDTs
// 				list := []interface{}{}
// 				for j, key := range keys {
// 					for i := 1; i <= listSize; i++ {
// 						list = append(list, i*100)

// 						sz, err := client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, j+i*100))
// 						gm.Expect(err).ToNot(gm.HaveOccurred())
// 						gm.Expect(sz.Bins[cdtBinName]).To(gm.Equal(i))
// 					}
// 				}

// 				records, err := client.BatchGetOperate(bpolicy, keys,
// 					as.ListSizeOp(cdtBinName),
// 					as.ListGetByIndexOp(cdtBinName, -1, as.ListReturnTypeValue),
// 					as.ListGetByIndexOp(cdtBinName, 0, as.ListReturnTypeValue),
// 					as.ListGetByIndexOp(cdtBinName, 2, as.ListReturnTypeValue),
// 				)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				for i, key := range keys {
// 					rec := records[i]
// 					gm.Expect(rec).NotTo(gm.BeNil())
// 					gm.Expect(rec.Key).NotTo(gm.BeNil())
// 					gm.Expect(rec.Key.Digest()).To(gm.Equal(key.Digest()))
// 					gm.Expect(rec.Bins[cdtBinName]).To(gm.Equal(as.OpResults{listSize, i + listSize*100, i + 100, i + 300}))
// 				}

// 			})

// 			gg.It("must execute BatchGetComplex with Operations", func() {
// 				const keyCount = 10
// 				const listSize = 10
// 				const cdtBinName = "cdtBin"

// 				keys := make([]*as.Key, 10)
// 				for i := 0; i < keyCount; i++ {
// 					keys[i], err = as.NewKey(ns, set, randString(10))
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 				}

// 				brecords := make([]*as.BatchRead, len(keys))
// 				for i := range keys {
// 					brecords[i] = &as.BatchRead{
// 						BatchRecord: as.BatchRecord{
// 							Key: keys[i],
// 						},
// 						ReadAllBins: false, // just to check if the internal flags are set correctly regardless
// 						Ops: []*as.Operation{
// 							as.ListSizeOp(cdtBinName),
// 							as.ListGetByIndexOp(cdtBinName, -1, as.ListReturnTypeValue),
// 							as.ListGetByIndexOp(cdtBinName, 0, as.ListReturnTypeValue),
// 							as.ListGetByIndexOp(cdtBinName, 2, as.ListReturnTypeValue),
// 						},
// 					}
// 				}

// 				// First Part: For CDTs
// 				list := []interface{}{}
// 				for j, key := range keys {
// 					for i := 1; i <= listSize; i++ {
// 						list = append(list, i*100)

// 						sz, err := client.Operate(wpolicy, key, as.ListAppendOp(cdtBinName, j+i*100))
// 						gm.Expect(err).ToNot(gm.HaveOccurred())
// 						gm.Expect(sz.Bins[cdtBinName]).To(gm.Equal(i))
// 					}
// 				}

// 				err = client.BatchGetComplex(bpolicy, brecords)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				for i, key := range keys {
// 					rec := brecords[i].Record
// 					gm.Expect(rec).NotTo(gm.BeNil())
// 					gm.Expect(rec.Key).NotTo(gm.BeNil())
// 					gm.Expect(rec.Key.Digest()).To(gm.Equal(key.Digest()))
// 					gm.Expect(rec.Bins[cdtBinName]).To(gm.Equal(as.OpResults{listSize, i + listSize*100, i + 100, i + 300}))
// 				}

// 			})
// 		})

// 		gg.Context("Batch Get Header operations", func() {
// 			bin := as.NewBin("Aerospike", rand.Int())
// 			const keyCount = 1024

// 			gg.BeforeEach(func() {
// 			})

// 			for _, useInline := range []bool{true, false} {
// 				gg.It(fmt.Sprintf("must return the record headers with same ordering as keys. AllowInline: %v", useInline), func() {
// 					var records []*as.Record
// 					type existence struct {
// 						key         *as.Key
// 						shouldExist bool // set randomly and checked against later
// 					}

// 					exList := []existence{}
// 					keys := []*as.Key{}

// 					for i := 0; i < keyCount; i++ {
// 						key, err := as.NewKey(ns, set, randString(50))
// 						gm.Expect(err).ToNot(gm.HaveOccurred())
// 						e := existence{key: key, shouldExist: rand.Intn(100) > 50}
// 						exList = append(exList, e)
// 						keys = append(keys, key)

// 						// if key shouldExist == true, put it in the DB
// 						if e.shouldExist {
// 							err = client.PutBins(wpolicy, key, bin)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())

// 							// update generation
// 							err = client.Touch(wpolicy, key)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())

// 							// make sure they exists in the DB
// 							exists, err := client.Exists(rpolicy, key)
// 							gm.Expect(err).ToNot(gm.HaveOccurred())
// 							gm.Expect(exists).To(gm.Equal(true))
// 						}
// 					}

// 					bpolicy.AllowInline = useInline
// 					records, err = client.BatchGetHeader(bpolicy, keys)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(len(records)).To(gm.Equal(len(keys)))
// 					for idx, rec := range records {
// 						if exList[idx].shouldExist {
// 							gm.Expect(rec.Bins[bin.Name]).To(gm.BeNil())
// 							gm.Expect(rec.Generation).To(gm.Equal(uint32(2)))
// 						} else {
// 							gm.Expect(rec).To(gm.BeNil())
// 						}
// 					}
// 				})
// 			}
// 		}) // Batch Get Header context

// 		gg.Context("Operate operations", func() {
// 			bin1 := as.NewBin("Aerospike1", rand.Intn(math.MaxInt16))
// 			bin2 := as.NewBin("Aerospike2", randString(100))

// 			gg.BeforeEach(func() {
// 				// err = client.PutBins(wpolicy, key, bin)
// 				// gm.Expect(err).ToNot(gm.HaveOccurred())
// 			})

// 			gg.It("must return proper error on write operations, but not reads", func() {
// 				key, err := as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				wpolicy := as.NewWritePolicy(0, 0)
// 				rec, err = client.Operate(wpolicy, key, as.GetOp())
// 				gm.Expect(errors.Is(err, as.ErrKeyNotFound)).To(gm.BeTrue())

// 				rec, err = client.Operate(wpolicy, key, as.TouchOp())
// 				gm.Expect(err).To(gm.HaveOccurred())
// 			})

// 			gg.It("must work correctly when no BinOps are passed as argument", func() {
// 				key, err := as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				ops1 := []*as.Operation{}

// 				wpolicy := as.NewWritePolicy(0, 0)
// 				rec, err = client.Operate(wpolicy, key, ops1...)
// 				gm.Expect(err).To(gm.HaveOccurred())
// 				gm.Expect(err.Error()).To(gm.ContainSubstring("No operations were passed."))
// 			})

// 			gg.It("must send key on Put operations", func() {
// 				key, err := as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				ops1 := []*as.Operation{
// 					as.PutOp(bin1),
// 					as.PutOp(bin2),
// 					as.GetOp(),
// 				}

// 				wpolicy := as.NewWritePolicy(0, 0)
// 				wpolicy.SendKey = true
// 				rec, err = client.Operate(wpolicy, key, ops1...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// make sure the result is what we put in
// 				for r := range recordset.Results() {
// 					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
// 					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
// 						gm.Expect(r.Record.Key.Value()).To(gm.Equal(key.Value()))
// 						gm.Expect(r.Record.Bins).To(gm.Equal(rec.Bins))
// 					}
// 				}
// 			})

// 			gg.It("must send key on Touch operations", func() {
// 				key, err := as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				ops1 := []*as.Operation{
// 					as.GetOp(),
// 					as.PutOp(bin2),
// 				}

// 				wpolicy := as.NewWritePolicy(0, 0)
// 				wpolicy.SendKey = false
// 				rec, err = client.Operate(wpolicy, key, ops1...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// make sure the key is not saved
// 				for r := range recordset.Results() {
// 					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
// 					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
// 						gm.Expect(r.Record.Key.Value()).To(gm.BeNil())
// 					}
// 				}

// 				ops2 := []*as.Operation{
// 					as.GetOp(),
// 					as.TouchOp(),
// 				}
// 				wpolicy.SendKey = true
// 				rec, err = client.Operate(wpolicy, key, ops2...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				recordset, err = client.ScanAll(nil, key.Namespace(), key.SetName())
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				// make sure the
// 				for r := range recordset.Results() {
// 					gm.Expect(r.Err).ToNot(gm.HaveOccurred())
// 					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
// 						gm.Expect(r.Record.Key.Value()).To(gm.Equal(key.Value()))
// 						gm.Expect(r.Record.Bins).To(gm.Equal(rec.Bins))
// 					}
// 				}
// 			})

// 			gg.It("must apply all operations, and result should match expectation", func() {
// 				key, err := as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				ops1 := []*as.Operation{
// 					as.PutOp(bin1),
// 					as.PutOp(bin2),
// 					as.GetOp(),
// 				}

// 				rec, err = client.Operate(nil, key, ops1...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int)))
// 				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject().(string)))
// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(1)))

// 				ops2 := []*as.Operation{
// 					as.AddOp(bin1),    // double the value of the bin
// 					as.AppendOp(bin2), // with itself
// 					as.GetOp(),
// 				}

// 				rec, err = client.Operate(nil, key, ops2...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int) * 2))
// 				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(strings.Repeat(bin2.Value.GetObject().(string), 2)))
// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(2)))

// 				ops3 := []*as.Operation{
// 					as.AddOp(bin1),
// 					as.PrependOp(bin2),
// 					as.TouchOp(),
// 					as.GetOp(),
// 				}

// 				rec, err = client.Operate(nil, key, ops3...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int) * 3))
// 				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(strings.Repeat(bin2.Value.GetObject().(string), 3)))
// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(3)))

// 				ops4 := []*as.Operation{
// 					as.TouchOp(),
// 					as.GetHeaderOp(),
// 				}

// 				rec, err = client.Operate(nil, key, ops4...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(4)))
// 				gm.Expect(len(rec.Bins)).To(gm.Equal(0))

// 				// GetOp should override GetHEaderOp
// 				ops5 := []*as.Operation{
// 					as.GetOp(),
// 					as.GetHeaderOp(),
// 				}

// 				rec, err = client.Operate(nil, key, ops5...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(4)))
// 				gm.Expect(len(rec.Bins)).To(gm.Equal(2))

// 				// GetOp should override GetHeaderOp
// 				ops6 := []*as.Operation{
// 					as.GetHeaderOp(),
// 					as.DeleteOp(),
// 					as.PutOp(bin1),
// 				}

// 				rec, err = client.Operate(nil, key, ops6...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(5)))
// 				gm.Expect(len(rec.Bins)).To(gm.Equal(0))

// 				// GetOp should override GetHeaderOp
// 				ops7 := []*as.Operation{
// 					as.GetOp(),
// 					as.TouchOp(),
// 				}

// 				rec, err = client.Operate(nil, key, ops7...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(6)))
// 				gm.Expect(len(rec.Bins)).To(gm.Equal(1))
// 			})

// 			gg.It("must re-apply the same operations, and result should match expectation", func() {
// 				const listSize = 10
// 				const cdtBinName = "cdtBin"

// 				// First Part: For CDTs
// 				list := []interface{}{}
// 				opAppend := as.ListAppendOp(cdtBinName, 1)
// 				for i := 1; i <= listSize; i++ {
// 					list = append(list, i)

// 					sz, err := client.Operate(wpolicy, key, opAppend)
// 					gm.Expect(err).ToNot(gm.HaveOccurred())
// 					gm.Expect(sz.Bins[cdtBinName]).To(gm.Equal(i))
// 				}

// 				op := as.ListGetOp(cdtBinName, -1)
// 				cdtListRes, err := client.Operate(wpolicy, key, op)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(cdtListRes.Bins[cdtBinName]).To(gm.Equal(1))

// 				cdtListRes, err = client.Operate(wpolicy, key, op)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())
// 				gm.Expect(cdtListRes.Bins[cdtBinName]).To(gm.Equal(1))

// 				// Second Part: For other normal Ops
// 				bin1 := as.NewBin("Aerospike1", 1)
// 				bin2 := as.NewBin("Aerospike2", "a")

// 				key, err := as.NewKey(ns, set, randString(50))
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				ops1 := []*as.Operation{
// 					as.PutOp(bin1),
// 					as.PutOp(bin2),
// 					as.GetOp(),
// 				}

// 				rec, err = client.Operate(nil, key, ops1...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int)))
// 				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject().(string)))
// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(1)))

// 				ops2 := []*as.Operation{
// 					as.AddOp(bin1),
// 					as.AppendOp(bin2),
// 					as.GetOp(),
// 				}

// 				rec, err = client.Operate(nil, key, ops2...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int) + 1))
// 				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject().(string) + "a"))
// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(2)))

// 				rec, err = client.Operate(nil, key, ops2...)
// 				gm.Expect(err).ToNot(gm.HaveOccurred())

// 				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject().(int) + 2))
// 				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject().(string) + "aa"))
// 				gm.Expect(rec.Generation).To(gm.Equal(uint32(3)))

// 			})

// 		}) // GetHeader context

// 	})

// })
