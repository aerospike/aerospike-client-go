// Copyright 2014-2021 Aerospike, Inc.
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

	as "github.com/aerospike/aerospike-client-go/v5"
	"github.com/aerospike/aerospike-client-go/v5/types"

	gg "github.com/onsi/ginkgo"
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
		bpolicy.AllowInline = true

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
				op1 := as.NewBatchWrite(nil, key1, as.PutOp(as.NewBin("bin1", "a")))
				op2 := as.NewBatchWrite(nil, key1, as.PutOp(as.NewBin("bin2", "b")))
				op3 := as.NewBatchRead(key1, []string{"bin2"})
				op4 := as.NewBatchWrite(nil, key1, as.DeleteOp())

				key2, _ := as.NewKey(ns, set, 2)
				op5 := as.NewBatchWrite(nil, key2, as.PutOp(as.NewBin("bin1", "a")))

				brecs := []as.BatchRecordIfc{op1, op2, op3, op4, op5}
				err := client.BatchOperate(bpolicy, brecs)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(op1.BatchRec().Err).ToNot(gm.HaveOccurred())
				gm.Expect(op1.BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(op1.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin1": nil}))
				gm.Expect(op1.BatchRec().InDoubt).To(gm.BeFalse())

				gm.Expect(op2.BatchRec().Err).ToNot(gm.HaveOccurred())
				gm.Expect(op2.BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(op2.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin2": nil}))
				gm.Expect(op2.BatchRec().InDoubt).To(gm.BeFalse())

				gm.Expect(op3.BatchRec().Err).ToNot(gm.HaveOccurred())
				gm.Expect(op3.BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(op3.BatchRec().Record.Bins).To(gm.Equal(as.BinMap{"bin2": "b"}))
				gm.Expect(op3.BatchRec().InDoubt).To(gm.BeFalse())

				gm.Expect(op4.BatchRec().Err).ToNot(gm.HaveOccurred())
				gm.Expect(op4.BatchRec().ResultCode).To(gm.Equal(types.OK))
				gm.Expect(op4.BatchRec().InDoubt).To(gm.BeFalse())

				// make sure the delete case actually ran
				exists, err := client.Exists(nil, key1)
				gm.Expect(exists).To(gm.BeFalse())

				// make sure the delete case actually ran
				exists, err = client.Exists(nil, key2)
				gm.Expect(exists).To(gm.BeTrue())
			})
		})

		gg.Context("BatchOperate operations", func() {
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
