//go:build !app_engine

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
	"strconv"
	"strings"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike", func() {

	gg.Describe("BatchGetOperate operations", func() {
		var ns = *namespace
		var set = randString(50)

		gg.It("must return the result with same ordering", func() {
			for _, keyCount := range []int{256, 5, 4, 3, 2, 1} {
				var keys []*as.Key
				for i := 0; i < keyCount; i++ {
					key, _ := as.NewKey(ns, set, i)
					client.PutBins(nil, key, as.NewBin("i", i), as.NewBin("j", i))

					keys = append(keys, key)
				}

				ops := []*as.Operation{as.GetBinOp("i"), as.PutOp(as.NewBin("h", 1))}
				_, err := client.BatchGetOperate(nil, keys, ops...)
				gm.Expect(err).To(gm.HaveOccurred())

				ops = []*as.Operation{as.GetBinOp("i")}
				recs, err := client.BatchGetOperate(nil, keys, ops...)

				gm.Expect(err).ToNot(gm.HaveOccurred())
				for i, rec := range recs {
					gm.Expect(len(rec.Bins)).To(gm.Equal(1))
					gm.Expect(rec.Bins["i"]).To(gm.Equal(i))
				}

			}
		}) // it

	}) // describe

	gg.Describe("Batch Write operations", func() {
		var ns = *namespace
		var set = randString(50)
		var wpolicy = as.NewWritePolicy(0, 0)
		var rpolicy = as.NewPolicy()
		var bpolicy = as.NewBatchPolicy()
		var bdpolicy = as.NewBatchDeletePolicy()
		// bpolicy.AllowInline = true

		wpolicy.TotalTimeout = 45 * time.Second
		wpolicy.SocketTimeout = 15 * time.Second
		rpolicy.TotalTimeout = 45 * time.Second
		rpolicy.SocketTimeout = 15 * time.Second
		bpolicy.TotalTimeout = 45 * time.Second
		bpolicy.SocketTimeout = 15 * time.Second

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

			gg.It("must return the result with same ordering for s single key", func() {
				keys := []*as.Key{ekeys[0]}
				res, err := client.BatchDelete(bpolicy, bdpolicy, keys)

				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(res).NotTo(gm.BeNil())
				gm.Expect(len(res)).To(gm.Equal(len(keys)))
				for _, br := range res {
					gm.Expect(br.ResultCode).To(gm.Equal(types.OK))
					gm.Expect(br.Record.Bins).To(gm.Equal(as.BinMap{}))
				}

				// true case
				exists, err = client.BatchExists(bpolicy, keys)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(len(exists)).To(gm.Equal(len(keys)))
				for _, keyExists := range exists {
					gm.Expect(keyExists).To(gm.BeFalse())
				}
			})

			gg.It("must return prioritize BatchDeletePolicy over BatchPolicy", func() {
				set := randString(10)

				var keys []*as.Key
				for i := 0; i < 5; i++ {
					key, _ := as.NewKey(ns, set, i)
					if i == 0 {
						keys = append(keys, key)
					}
					bin0 := as.NewBin("count", i)
					err := client.PutBins(nil, key, bin0)
					gm.Expect(err).ToNot(gm.HaveOccurred())
				}

				bdp := as.NewBatchDeletePolicy()
				bdp.FilterExpression = as.ExpEq(
					as.ExpIntBin("count"),
					as.ExpIntVal(0))

				bp := as.NewBatchPolicy()
				bp.FilterExpression = as.ExpEq(
					as.ExpIntBin("count"),
					as.ExpIntVal(999))
				records, err := client.BatchDelete(bp, bdp, keys)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(len(records)).To(gm.BeNumerically(">", 0))

				for _, br := range records {
					bri := br.BatchRec()
					gm.Expect(bri.ResultCode).To(gm.Equal(types.ResultCode(0)))
					gm.Expect(bri.Record).NotTo(gm.BeNil())
				}

				// scanning
				rs, err := client.ScanAll(nil, ns, set)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				cnt := 0
				for res := range rs.Results() {
					gm.Expect(res.Err).ToNot(gm.HaveOccurred())
					gm.Expect(res.Record.Bins["count"]).ToNot(gm.Equal(0))
					cnt++
				}
				gm.Expect(cnt).To(gm.Equal(4))
			})
		})

		gg.Context("BatchOperate operations", func() {
			gg.It("must return the result with same ordering", func() {
				key1, _ := as.NewKey(ns, set, 1)
				op1 := as.NewBatchWrite(nil, key1, as.PutOp(as.NewBin("bin1", "a")), as.PutOp(as.NewBin("bin2", "b")))
				op3 := as.NewBatchRead(nil, key1, []string{"bin2"})

				key2, _ := as.NewKey(ns, set, 2)
				op5 := as.NewBatchWrite(nil, key2, as.PutOp(as.NewBin("bin1", "a")))

				brecs := []as.BatchRecordIfc{op1, op3, op5}
				err := client.BatchOperate(bpolicy, brecs)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// Since the ops will run out of order, there is always a chance that
				// the read operation will run first and return a KEY_NOT_FOUND error.
				// As a result we run the operate command twice.
				err = client.BatchOperate(bpolicy, brecs)
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
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(exists).To(gm.BeTrue())
			})

			gg.It("must successfully execute a BatchRead with empty ops", func() {
				var batchRecords []as.BatchRecordIfc
				for i := 0; i < 5; i++ {
					key, _ := as.NewKey(ns, set, i)
					client.PutBins(nil, key, as.NewBin("i", i), as.NewBin("j", 5-i))

					if i == 0 {
						batchRead := as.NewBatchRead(nil, key, nil)
						batchRead.ReadAllBins = true
						batchRecords = append(batchRecords,
							batchRead,
						)
					}
				}

				err := client.BatchOperate(nil, batchRecords)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				op1 := batchRecords[0].BatchRec()
				gm.Expect(op1.BatchRec().Err).ToNot(gm.HaveOccurred())
				gm.Expect(op1.BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(op1.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"i": 0, "j": 5}))
				gm.Expect(op1.BatchRec().InDoubt).To(gm.BeFalse())

			})

			gg.It("must successfully execute a BatchOperate for many keys", func() {
				gm.Expect(err).ToNot(gm.HaveOccurred())
				bwPolicy := as.NewBatchWritePolicy()
				bdPolicy := as.NewBatchDeletePolicy()

				var keys []*as.Key
				for i := 0; i < 64; i++ {
					key, _ := as.NewKey(ns, set, i)
					if i == 0 {
						keys = append(keys, key)
					}
					bin0 := as.NewBin("count", i)
					err := client.PutBins(nil, key, bin0)
					gm.Expect(err).ToNot(gm.HaveOccurred())
				}

				for _, sendKey := range []bool{true, false} {
					bwPolicy.SendKey = sendKey
					bdPolicy.SendKey = sendKey
					bpolicy.SendKey = !sendKey

					var brecs []as.BatchRecordIfc
					for _, key := range keys {
						brecs = append(brecs, as.NewBatchWrite(bwPolicy, key, as.PutOp(as.NewBin("bin1", "a")), as.PutOp(as.NewBin("bin2", "b"))))
						brecs = append(brecs, as.NewBatchDelete(bdPolicy, key))
						brecs = append(brecs, as.NewBatchRead(nil, key, []string{"bin2"}))
					}

					err := client.BatchOperate(bpolicy, brecs)
					gm.Expect(err).ToNot(gm.HaveOccurred())
				}
			})

			gg.It("must successfully execute a delete op", func() {
				gm.Expect(err).ToNot(gm.HaveOccurred())
				bwPolicy := as.NewBatchWritePolicy()
				bdPolicy := as.NewBatchDeletePolicy()

				for _, sendKey := range []bool{true, false} {
					bwPolicy.SendKey = sendKey
					bdPolicy.SendKey = sendKey
					bpolicy.SendKey = !sendKey

					key1, _ := as.NewKey(ns, set, 1)
					op1 := as.NewBatchWrite(bwPolicy, key1, as.PutOp(as.NewBin("bin1", "a")), as.PutOp(as.NewBin("bin2", "b")))
					op2 := as.NewBatchDelete(bdPolicy, key1)
					op3 := as.NewBatchRead(nil, key1, []string{"bin2"})

					brecs := []as.BatchRecordIfc{op1, op3}
					err := client.BatchOperate(bpolicy, brecs)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					gm.Expect(op1.BatchRec().Err).ToNot(gm.HaveOccurred())
					gm.Expect(op1.BatchRec().ResultCode).To(gm.Equal(types.OK))
					gm.Expect(op1.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1": nil, "bin2": nil}))
					gm.Expect(op1.BatchRec().InDoubt).To(gm.BeFalse())

					brecs = []as.BatchRecordIfc{op1, op3}
					err = client.BatchOperate(bpolicy, brecs)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					// There is no guarantee for the order of execution for different commands
					gm.Expect(op3.BatchRec().Err).ToNot(gm.HaveOccurred())
					gm.Expect(op3.BatchRec().Record).ToNot(gm.BeNil())
					gm.Expect(op3.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin2": "b"}))

					exists, err := client.Exists(nil, key1)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(exists).To(gm.BeTrue())

					brecs = []as.BatchRecordIfc{op2}
					err = client.BatchOperate(bpolicy, brecs)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					gm.Expect(op2.BatchRec().Err).ToNot(gm.HaveOccurred())
					gm.Expect(op2.BatchRec().ResultCode).To(gm.Equal(types.OK))
					gm.Expect(op2.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{}))
					gm.Expect(op2.BatchRec().InDoubt).To(gm.BeFalse())

					exists, err = client.Exists(nil, key1)
					gm.Expect(err).ToNot(gm.HaveOccurred())
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

			gg.It("Should return the error for entire operation", func() {
				key, _ := as.NewKey(*namespace, set, 0)
				var batchRecords []as.BatchRecordIfc
				for i := 0; i < 2000000; i++ {
					batchRecords = append(batchRecords, as.NewBatchReadHeader(nil, key))
				}
				bp := as.NewBatchPolicy()
				bp.RespondAllKeys = true
				bp.TotalTimeout = 10 * time.Second
				bp.SocketTimeout = 10 * time.Second
				err := client.BatchOperate(bp, batchRecords)
				gm.Expect(err).To(gm.HaveOccurred())
				// gm.Expect(err.Matches(types.BATCH_MAX_REQUESTS_EXCEEDED)).To(gm.BeTrue())
			})

			gg.It("Should return the error for invalid namespace", func() {
				var brs []as.BatchRecordIfc

				for i := 0; i < 1; i++ {
					key, _ := as.NewKey("non_exist", "non_exist", i)
					brr := as.NewBatchReadOps(nil, key, []*as.Operation{as.GetBinOp("i_bin")}...)
					brs = append(brs, brr)
				}

				bp := as.NewBatchPolicy()
				err := client.BatchOperate(bp, brs)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
			})

			gg.It("Should not panic on BatchDelete with non-existent namespace", func() {
				// This test validates the fix for nil cmd.node panic in retry logic
				var keys []*as.Key
				for i := 0; i < 10; i++ {
					key, _ := as.NewKey("non_existent_namespace", "non_existent_set", i)
					keys = append(keys, key)
				}

				bp := as.NewBatchPolicy()
				bp.MaxRetries = 2 // Ensure retry logic is exercised
				bdp := as.NewBatchDeletePolicy()

				// This should return an error but not panic
				records, err := client.BatchDelete(bp, bdp, keys)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())

				// Also check individual record errors
				gm.Expect(records).NotTo(gm.BeNil())
				gm.Expect(len(records)).To(gm.Equal(len(keys)))
				for _, record := range records {
					gm.Expect(record.Err).To(gm.HaveOccurred())
					gm.Expect(record.Err.Matches(types.INVALID_NAMESPACE)).To(gm.BeTrue())
					gm.Expect(record.ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))
				}
			})

			gg.It("Overall command error should be reflected in API call error and not BatchRecord error", func() {
				var batchRecords []as.BatchRecordIfc
				key, _ := as.NewKey(*namespace, set, 0)
				for i := 0; i < len(client.Cluster().GetNodes())*2000000; i++ {
					batchRecords = append(batchRecords, as.NewBatchReadHeader(nil, key))
				}

				err := client.BatchOperate(nil, batchRecords)
				gm.Expect(err).To(gm.HaveOccurred())
				// gm.Expect(err.Matches(types.BATCH_MAX_REQUESTS_EXCEEDED)).To(gm.BeTrue())

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
				gm.Expect(r.Bins[binName]).To(gm.Equal(as.OpResults{[]any{7, 8}, []any{0, 3, 4, 5}, []any{7, 8, 9, 10}, []any{2, 3, 4, 5}, []any{7, 6, 5, 8}}))

				// Remove
				op6 := as.ListRemoveByValueRangeOp(binName, as.ListReturnTypeIndex, as.NewValue(7), nil)
				r2, err2 := client.Operate(wpolicy, key, op6)
				gm.Expect(err2).ToNot(gm.HaveOccurred())
				gm.Expect(r2.Bins[binName]).To(gm.Equal([]any{0, 3, 4, 5}))

				r3, err3 := client.Get(nil, key)
				gm.Expect(err3).ToNot(gm.HaveOccurred())
				gm.Expect(r3.Bins[binName]).To(gm.Equal([]any{6, 5}))
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
					op3 := as.NewBatchRead(nil, key, []string{"bin2"})

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

		gg.Context("BatchRead operations with TTL", func() {
			gg.BeforeEach(func() {
				if serverIsOlderThan("7") {
					gg.Skip("Not supported in server before v7.1")
				}
			})

			gg.It("Reset Read TTL", func() {
				if nsupPeriod(ns) == 0 {
					gg.Skip("Not supported with nsup-period == 0")
				}

				key, _ := as.NewKey(ns, set, "expirekey3")
				bin := as.NewBin("expireBinName", "expirevalue")

				// Specify that record expires 2 seconds after it's written.
				writePolicy := as.NewWritePolicy(0, 2)
				err := client.PutBins(writePolicy, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// Read the record before it expires and reset read ttl.
				time.Sleep(1 * time.Second)
				readPolicy := as.NewPolicy()
				readPolicy.ReadTouchTTLPercent = 80
				record, err := client.Get(readPolicy, key, bin.Name)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))

				// Read the record again, but don't reset read ttl.
				time.Sleep(1 * time.Second)
				readPolicy.ReadTouchTTLPercent = -1
				record, err = client.Get(readPolicy, key, bin.Name)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(record.Bins[bin.Name]).To(gm.Equal(bin.Value.GetObject()))

				// Read the record after it expires, showing it's gone.
				time.Sleep(2 * time.Second)
				_, err = client.Get(nil, key, bin.Name)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(types.KEY_NOT_FOUND_ERROR)).To(gm.BeTrue())
			})

			gg.It("BatchRead TTL", func() {
				// WARNING: This test takes a long time to run due to sleeps.
				// Define keys
				key1, _ := as.NewKey(ns, set, 88888)
				key2, _ := as.NewKey(ns, set, 88889)

				// Write keys with ttl.
				bwp := as.NewBatchWritePolicy()
				bwp.Expiration = 10
				bw1 := as.NewBatchWrite(bwp, key1, as.PutOp(as.NewBin("a", 1)))
				bw2 := as.NewBatchWrite(bwp, key2, as.PutOp(as.NewBin("a", 1)))

				list := []as.BatchRecordIfc{bw1, bw2}
				err := client.BatchOperate(nil, list)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// Read records before they expire and reset read ttl on one record.
				time.Sleep(8 * time.Second)
				brp1 := as.NewBatchReadPolicy()
				brp1.ReadTouchTTLPercent = 80

				brp2 := as.NewBatchReadPolicy()
				brp2.ReadTouchTTLPercent = -1

				br1 := as.NewBatchRead(brp1, key1, []string{"a"})
				br2 := as.NewBatchRead(brp2, key2, []string{"a"})

				list = []as.BatchRecordIfc{br1, br2}

				err = client.BatchOperate(nil, list)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(types.OK, br1.ResultCode)
				gm.Expect(types.OK, br2.ResultCode)

				// Read records again, but don't reset read ttl.
				time.Sleep(3 * time.Second)
				brp1.ReadTouchTTLPercent = -1
				brp2.ReadTouchTTLPercent = -1

				br1 = as.NewBatchRead(brp1, key1, []string{"a"})
				br2 = as.NewBatchRead(brp2, key2, []string{"a"})

				list = []as.BatchRecordIfc{br1, br2}

				err = client.BatchOperate(nil, list)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// Key 2 should have expired.
				gm.Expect(types.OK, br1.ResultCode)
				gm.Expect(types.KEY_NOT_FOUND_ERROR, br2.ResultCode)

				// Read  record after it expires, showing it's gone.
				time.Sleep(8 * time.Second)
				err = client.BatchOperate(nil, list)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(br1.ResultCode).To(gm.Equal(types.KEY_NOT_FOUND_ERROR))
				gm.Expect(br2.ResultCode).To(gm.Equal(types.KEY_NOT_FOUND_ERROR))
			})
		})

		gg.Context("BatchUDF operations", func() {
			gg.It("must return the results for single BatchUDF vs multiple", func() {
				luaCode := `-- Create a record
				function rec_create(rec, bins)
				    return bins
				end`

				removeUDF("test_ops.lua")
				registerUDF(luaCode, "test_ops.lua")

				for _, keyCount := range []int{10, 1} {
					client.Truncate(nil, ns, set, nil)
					batchRecords := []as.BatchRecordIfc{}

					for k := 0; k < keyCount; k++ {
						key, _ := as.NewKey(ns, set, k)
						args := make(map[any]any)
						args["bin1_str"] = "a"
						batchRecords = append(batchRecords, as.NewBatchUDF(
							nil,
							key,
							"test_ops",
							"rec_create",
							as.NewMapValue(args),
						))
					}
					bp := as.NewBatchPolicy()
					err := client.BatchOperate(bp, batchRecords)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					for i := 0; i < keyCount; i++ {
						gm.Expect(batchRecords[i].BatchRec().Err).To(gm.BeNil())
						gm.Expect(batchRecords[i].BatchRec().ResultCode).To(gm.Equal(types.OK))
						gm.Expect(batchRecords[i].BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"SUCCESS": map[any]any{"bin1_str": "a"}}))
					}
				}
			})

			gg.It("must return the results when one operation is against an invalid namespace", func() {
				luaCode := `-- Create a record
				function rec_create(rec, bins)
				    if bins ~= nil then
				        for b, bv in map.pairs(bins) do
				            rec[b] = bv
				        end
				    end
				    status = aerospike:create(rec)
				    return status
				end`

				removeUDF("test_ops.lua")
				registerUDF(luaCode, "test_ops.lua")

				batchRecords := []as.BatchRecordIfc{}

				key1, _ := as.NewKey(randString(10), set, 1)
				args := make(map[any]any)
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
					nil,
					key1,
					[]string{"bin1_str"},
				))

				batchRecords = append(batchRecords, as.NewBatchRead(
					nil,
					key2,
					[]string{"bin1_str"},
				))

				batchRecords = append(batchRecords, as.NewBatchRead(
					nil,
					key3,
					[]string{"bin1_str"},
				))

				bp := as.NewBatchPolicy()
				bp.RespondAllKeys = false
				// The overall error is intentionally not asserted: the
				// per-record outcomes below are the contract under test.
				_ = client.BatchOperate(bp, batchRecords)

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

				registerUDF(udf, "test_ops.lua")

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
					gm.Expect(br.InDoubt).To(gm.BeFalse())
					gm.Expect(br.ResultCode).To(gm.Equal(types.UDF_BAD_RESPONSE))
					gm.Expect(br.Err.Matches(types.UDF_BAD_RESPONSE)).To(gm.Equal(true))
					gm.Expect(br.Err.IsInDoubt()).To(gm.BeFalse())
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
					gm.Expect(br.Err.IsInDoubt()).To(gm.BeFalse())
					gm.Expect(br.ResultCode).To(gm.Equal(types.RECORD_TOO_BIG))
					gm.Expect(br.Err.Matches(types.RECORD_TOO_BIG)).To(gm.Equal(true))
					gm.Expect(br.Err.IsInDoubt()).To(gm.Equal(false))

					br = batchRecords[1].BatchRec()
					gm.Expect(br.Err.IsInDoubt()).To(gm.BeFalse())
					gm.Expect(br.ResultCode).To(gm.Equal(types.RECORD_TOO_BIG))
					gm.Expect(br.Err.Matches(types.RECORD_TOO_BIG)).To(gm.Equal(true))
					gm.Expect(br.Err.IsInDoubt()).To(gm.Equal(false))

					br = batchRecords[2].BatchRec()
					gm.Expect(br.Err.IsInDoubt()).To(gm.BeFalse())
					gm.Expect(br.ResultCode).To(gm.Equal(types.INVALID_NAMESPACE))
					gm.Expect(br.Err.Matches(types.INVALID_NAMESPACE)).To(gm.Equal(true))
					gm.Expect(br.Err.IsInDoubt()).To(gm.Equal(false))
				}
			})

			gg.It("must return the result with same ordering", func() {
				registerUDF(udfBody, "udf1.lua")
				for _, keyCount := range []int{50, 1} {
					keys := []*as.Key{}
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
						gm.Expect(rec.Record.Bins["SUCCESS"]).To(gm.Equal(map[any]any{"status": "OK"}))
					}

					recs, err := client.BatchGet(nil, keys)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(len(recs)).To(gm.Equal(len(keys)))
					for i, rec := range recs {
						gm.Expect(rec.Bins["bin2"].(int)).To(gm.Equal(i * 3))
					}
				}
			})
		})
	})

	gg.Describe("AllowPartialResults should not suppress timeout errors on single-key batches", func() {
		var ns = *namespace
		var set = randString(50)

		// Write a record that we can read back in batch calls.
		gg.BeforeEach(func() {
			key, _ := as.NewKey(ns, set, 0)
			err := client.PutBins(nil, key, as.NewBin("bin1", 1))
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		// Helper: a batch policy with AllowPartialResults=true and a timeout
		// short enough to expire before the server can respond.
		newTimedOutBatchPolicy := func() *as.BatchPolicy {
			bp := as.NewBatchPolicy()
			bp.AllowPartialResults = true
			bp.TotalTimeout = 1 * time.Nanosecond
			bp.SocketTimeout = 1 * time.Nanosecond
			bp.MaxRetries = 0
			return bp
		}

		gg.It("BatchGet must surface timeout error even with AllowPartialResults", func() {
			key, _ := as.NewKey(ns, set, 0)
			keys := []*as.Key{key}
			bp := newTimedOutBatchPolicy()

			_, err := client.BatchGet(bp, keys)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.TIMEOUT)).To(gm.BeTrue())
		})

		gg.It("BatchGetHeader must surface timeout error even with AllowPartialResults", func() {
			key, _ := as.NewKey(ns, set, 0)
			keys := []*as.Key{key}
			bp := newTimedOutBatchPolicy()

			_, err := client.BatchGetHeader(bp, keys)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.TIMEOUT)).To(gm.BeTrue())
		})

		gg.It("BatchDelete must surface timeout error even with AllowPartialResults", func() {
			key, _ := as.NewKey(ns, set, 0)
			keys := []*as.Key{key}
			bp := newTimedOutBatchPolicy()

			_, err := client.BatchDelete(bp, nil, keys)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.TIMEOUT)).To(gm.BeTrue())
		})

		gg.It("BatchOperate must surface timeout error even with AllowPartialResults", func() {
			key, _ := as.NewKey(ns, set, 0)
			bp := newTimedOutBatchPolicy()

			var records []as.BatchRecordIfc
			records = append(records, as.NewBatchReadOps(nil, key, as.GetBinOp("bin1")))

			err := client.BatchOperate(bp, records)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.TIMEOUT)).To(gm.BeTrue())
		})

		gg.It("BatchGetComplex must surface timeout error even with AllowPartialResults", func() {
			key, _ := as.NewKey(ns, set, 0)
			bp := newTimedOutBatchPolicy()

			records := []*as.BatchRead{
				as.NewBatchRead(nil, key, []string{"bin1"}),
			}

			err := client.BatchGetComplex(bp, records)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.TIMEOUT)).To(gm.BeTrue())
		})
	})
})

// ── BatchWrite ───────────────────────────────────────────────────────────────

var _ = gg.Describe("CLIENT-4898 BatchWrite sendKey", func() {
	ns := *namespace
	set := "ck4898_write"

	gg.It("resolves sendKey as the union of parent and per-record policy (single & multi record)", func() {
		for _, sz := range sendKeyBatchSizes {
			// parent BatchPolicy.SendKey=true, per-record nil → key stored
			bp := as.NewBatchPolicy()
			bp.SendKey = true
			keys := sendKeyKeys(ns, set, "w-parent-"+sz.name, sz.n)
			runSendKeyBatchWrites(client, keys, bp, nil)
			expectSendKeyStored(ns, set, keys, true)

			// per-record BatchWritePolicy.SendKey=true, parent false → key stored
			bp = as.NewBatchPolicy()
			bp.SendKey = false
			wp := as.NewBatchWritePolicy()
			wp.SendKey = true
			keys = sendKeyKeys(ns, set, "w-perrec-"+sz.name, sz.n)
			runSendKeyBatchWrites(client, keys, bp, wp)
			expectSendKeyStored(ns, set, keys, true)

			// parent SendKey=true overrides per-record SendKey=false (union) → key stored
			bp = as.NewBatchPolicy()
			bp.SendKey = true
			wp = as.NewBatchWritePolicy()
			wp.SendKey = false
			keys = sendKeyKeys(ns, set, "w-union-"+sz.name, sz.n)
			runSendKeyBatchWrites(client, keys, bp, wp)
			expectSendKeyStored(ns, set, keys, true)

			// neither parent nor per-record set → key NOT stored
			bp = as.NewBatchPolicy()
			bp.SendKey = false
			wp = as.NewBatchWritePolicy()
			wp.SendKey = false
			keys = sendKeyKeys(ns, set, "w-none-"+sz.name, sz.n)
			runSendKeyBatchWrites(client, keys, bp, wp)
			expectSendKeyStored(ns, set, keys, false)
		}
	})

	// Cluster default must be honored even when a per-record policy with SendKey=false is present.
	gg.It("cluster DefaultBatchWritePolicy.SendKey=true is honored despite a per-record SendKey=false", func() {
		orig := client.DefaultBatchWritePolicy
		defer func() { client.DefaultBatchWritePolicy = orig }()
		def := as.NewBatchWritePolicy()
		def.SendKey = true // cluster default enables sendKey
		client.DefaultBatchWritePolicy = def

		for _, sz := range sendKeyBatchSizes {
			bp := as.NewBatchPolicy()
			bp.SendKey = false // parent does NOT request the key
			wp := as.NewBatchWritePolicy()
			wp.SendKey = false // per-record policy present but does NOT request the key
			keys := sendKeyKeys(ns, set, "w-clusterdef-"+sz.name, sz.n)
			runSendKeyBatchWrites(client, keys, bp, wp)
			expectSendKeyStored(ns, set, keys, true)
		}
	})
})

// ── BatchUDF (dedicated client.BatchExecute path) ────────────────────────────

var _ = gg.Describe("CLIENT-4898 BatchUDF sendKey", func() {
	ns := *namespace
	set := "ck4898_udf"

	gg.It("resolves sendKey from parent or per-record policy (single & multi record)", func() {
		for _, sz := range sendKeyBatchSizes {
			// parent BatchPolicy.SendKey=true, per-record nil → key stored
			bp := as.NewBatchPolicy()
			bp.SendKey = true
			keys := sendKeyKeys(ns, set, "u-parent-"+sz.name, sz.n)
			runSendKeyBatchUDF(client, keys, bp, nil)
			expectSendKeyStored(ns, set, keys, true)

			// per-record BatchUDFPolicy.SendKey=true, parent false → key stored
			bp = as.NewBatchPolicy()
			bp.SendKey = false
			up := as.NewBatchUDFPolicy()
			up.SendKey = true
			keys = sendKeyKeys(ns, set, "u-perrec-"+sz.name, sz.n)
			runSendKeyBatchUDF(client, keys, bp, up)
			expectSendKeyStored(ns, set, keys, true)
		}
	})
})

// ── BatchDelete (dedicated client.BatchDelete path) ──────────────────────────
//
// sendKey on a delete stores the key on the (durable) tombstone, which is not observable via a
// normal scan. This is a functional/smoke test: the parent-union code path must encode and run
// cleanly for both single- and multi-record deletes.

var _ = gg.Describe("CLIENT-4898 BatchDelete sendKey [smoke — tombstone key not scannable]", func() {
	ns := *namespace
	set := "ck4898_delete"

	gg.It("parent BatchPolicy.SendKey=true deletes cleanly (single & multi record)", func() {
		for _, sz := range sendKeyBatchSizes {
			bp := as.NewBatchPolicy()
			bp.SendKey = true
			keys := sendKeyKeys(ns, set, "d-parent-"+sz.name, sz.n)
			for i, k := range keys {
				gm.Expect(client.PutBins(nil, k, as.NewBin("v", i))).ToNot(gm.HaveOccurred())
			}

			recs, err := client.BatchDelete(bp, nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for _, r := range recs {
				gm.Expect(r.ResultCode).To(gm.Equal(types.OK))
			}
			for _, k := range keys {
				ex, err := client.Exists(nil, k)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(ex).To(gm.BeFalse(), "record should be deleted")
			}
		}
	})
})

// ── Mixed batch (heterogeneous BatchOperate) ─────────────────────────────────

var _ = gg.Describe("CLIENT-4898 mixed batch sendKey", func() {
	ns := *namespace
	set := "ck4898_mixed"

	gg.It("parent SendKey=true: write/UDF records store the key, read records do not", func() {
		ensureSendKeyUDF()

		bp := as.NewBatchPolicy()
		bp.SendKey = true

		wKey := sendKeyKeys(ns, set, "mx-write", 1)[0]
		uKey := sendKeyKeys(ns, set, "mx-udf", 1)[0]
		rKey := sendKeyKeys(ns, set, "mx-read", 1)[0]
		// Seed the read target (with a nil policy, so its own key is NOT stored).
		gm.Expect(client.PutBins(nil, rKey, as.NewBin("v", 1))).ToNot(gm.HaveOccurred())

		recs := []as.BatchRecordIfc{
			as.NewBatchWrite(nil, wKey, as.PutOp(as.NewBin("v", 1))),
			as.NewBatchUDF(nil, uKey, "client4898udf", "writeBin", as.NewValue("v"), as.NewValue(1)),
			as.NewBatchRead(nil, rKey, []string{"v"}),
		}
		gm.Expect(client.BatchOperate(bp, recs)).ToNot(gm.HaveOccurred())

		// Writes honor parent sendKey; the read never stores a key.
		gm.Expect(sendKeyStored(ns, set, wKey)).To(gm.BeTrue(), "write record must store key under parent SendKey=true")
		gm.Expect(sendKeyStored(ns, set, uKey)).To(gm.BeTrue(), "UDF record must store key under parent SendKey=true")
		gm.Expect(sendKeyStored(ns, set, rKey)).To(gm.BeFalse(), "read record must never store a key")
	})
})

// Shared helpers for the batch sendKey tests (batch_test.go, dynconfig_serialze_test.go).
var sendKeyBatchSizes = []struct {
	name string
	n    int
}{
	{"single-record", 1},
	{"multi-record", 3},
}

// sendKeyStored reports whether the SERVER stored the user key for this digest. A scan is the
// only correct probe: the client does not supply the key, so Record.Key.Value() is non-nil ONLY
// if the server stored and echoed it at write time. (client.Get can't tell — it already holds
// the key it queried with.)
func sendKeyStored(ns, set string, key *as.Key) bool {
	rs, err := client.ScanAll(as.NewScanPolicy(), ns, set)
	if err != nil {
		return false
	}
	defer rs.Close()

	want := string(key.Digest())
	for res := range rs.Results() {
		if res.Err != nil || res.Record == nil || res.Record.Key == nil {
			continue
		}
		if string(res.Record.Key.Digest()) == want {
			return res.Record.Key.Value() != nil
		}
	}
	return false
}

func expectSendKeyStored(ns, set string, keys []*as.Key, want bool) {
	for _, k := range keys {
		gm.ExpectWithOffset(1, sendKeyStored(ns, set, k)).To(gm.Equal(want),
			"key %v: server-stored mismatch (want stored=%v)", k.Value(), want)
	}
}

// sendKeyKeys builds n distinct keys under a unique prefix (prefixes keep scenarios from
// colliding in the shared set's scan).
func sendKeyKeys(ns, set, prefix string, n int) []*as.Key {
	keys := make([]*as.Key, n)
	for i := 0; i < n; i++ {
		k, err := as.NewKey(ns, set, prefix+"-"+strconv.Itoa(i))
		gm.ExpectWithOffset(1, err).ToNot(gm.HaveOccurred())
		keys[i] = k
	}
	return keys
}

// runWrites issues a BatchWrite per key and returns the keys.
func runSendKeyBatchWrites(c *as.Client, keys []*as.Key, bp *as.BatchPolicy, wp *as.BatchWritePolicy) {
	recs := make([]as.BatchRecordIfc, len(keys))
	for i, k := range keys {
		recs[i] = as.NewBatchWrite(wp, k, as.PutOp(as.NewBin("v", i)))
	}
	gm.ExpectWithOffset(1, c.BatchOperate(bp, recs)).ToNot(gm.HaveOccurred())
}

// The UDF writes a bin, so the record is created and its key becomes observable via scan.
const sendKeyUDFBody = `
function writeBin(rec, name, val)
    rec[name] = val
    if aerospike:exists(rec) then
        aerospike:update(rec)
    else
        aerospike:create(rec)
    end
end
`

var sendKeyUDFOnce sync.Once

func ensureSendKeyUDF() {
	sendKeyUDFOnce.Do(func() {
		t, err := client.RegisterUDF(as.NewWritePolicy(0, 0), []byte(sendKeyUDFBody), "client4898udf.lua", as.LUA)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-t.OnComplete()).ToNot(gm.HaveOccurred())
	})
}

func runSendKeyBatchUDF(c *as.Client, keys []*as.Key, bp *as.BatchPolicy, up *as.BatchUDFPolicy) {
	ensureSendKeyUDF()
	recs, err := c.BatchExecute(bp, up, keys, "client4898udf", "writeBin", as.NewValue("v"), as.NewValue(1))
	gm.ExpectWithOffset(1, err).ToNot(gm.HaveOccurred())
	for _, r := range recs {
		gm.ExpectWithOffset(1, r.ResultCode).To(gm.Equal(types.OK))
	}
}
