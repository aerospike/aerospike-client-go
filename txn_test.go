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
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike", func() {

	gg.Describe("Multi Record Transaction operations", gg.Ordered, func() {
		var ns = *namespace
		var set = randString(50)
		const binName = "bin"

		gg.BeforeAll(func() {
			// skip the tests if the cluster is not in SC mode or the server is older than v8
			if serverIsOlderThan("8") {
				gg.Skip("Not supported in server before v8")
			}

			if !as.ConfiguredAsStrongConsistency(client, ns) {
				gg.Skip("Not supported in namespaces without Strong Consistency support")
			}

			const luaFunc = `
				local function putBin(r,name,value)
					if not aerospike:exists(r) then aerospike:create(r) end
					r[name] = value
					aerospike:update(r)
				end

				-- Set a particular bin
				function writeBin(r,name,value)
					putBin(r,name,value)
				end

				function get_gen(rec)
					return record.gen(rec)
				end
				
				function rec_read(rec)
					local m = map()
					names = record.bin_names(rec)
					for i, bn in ipairs(names) do
						m[bn] = rec[bn]
					end
					return m
				end
				`

			regTask, err := client.RegisterUDF(nil, []byte(luaFunc), "record_example.lua", as.LUA)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(<-regTask.OnComplete()).ToNot(gm.HaveOccurred())
		})

		gg.Context("must support empty transactions", func() {

			gg.It("Committing should not panic", func() {
				txn := as.NewTxn()

				status, err := client.Commit(txn)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(status).To(gm.Equal(as.CommitStatusOK))
			}) // it

			gg.It("Canceling should not panic", func() {
				txn := as.NewTxn()

				status, err := client.Abort(txn)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(status).To(gm.Equal(as.AbortStatusOK))
			}) // it

		}) // Context

		gg.It("must write and commit", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must write twice", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must write correctly during conflict", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			txn1 := as.NewTxn()
			wp1 := as.NewWritePolicy(0, 0)
			wp1.Txn = txn1

			txn2 := as.NewTxn()
			wp2 := as.NewWritePolicy(0, 0)
			wp2.Txn = txn2

			err = client.PutBins(wp1, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(wp2, key, as.NewBin(binName, "val2"))
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.(*as.AerospikeError).ResultCode).To(gm.Equal(types.MRT_BLOCKED))

			status, err := client.Commit(txn1)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			status, err = client.Commit(txn2)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must be blocked before other transaction is committed", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			defer client.Commit(txn)

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(nil, key, as.NewBin(binName, "val3"))
			gm.Expect(err.(*as.AerospikeError).ResultCode).To(gm.Equal(types.MRT_BLOCKED))
		}) // it

		gg.It("must support write and read", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must support write and abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rp := as.NewPolicy()
			rp.Txn = txn

			record, err := client.Get(rp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must support delete", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.DurableDelete = true
			wp.Txn = txn

			existed, err := client.Delete(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeTrue())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.KEY_NOT_FOUND_ERROR)).To(gm.BeTrue())
			gm.Expect(record).To(gm.BeNil())
		}) // it

		gg.It("must support delete and abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.DurableDelete = true
			wp.Txn = txn

			existed, err := client.Delete(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeTrue())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must support delete twice", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.DurableDelete = true
			wp.Txn = txn

			existed, err := client.Delete(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeTrue())

			existed, err = client.Delete(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(existed).To(gm.BeFalse())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.KEY_NOT_FOUND_ERROR)).To(gm.BeTrue())
			gm.Expect(record).To(gm.BeNil())
		}) // it

		gg.It("must support touch", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err := client.Touch(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
			gm.Expect(record.Generation).To(gm.BeNumerically(">", 1))
		}) // it

		gg.It("must support touch and abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err = client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err := client.Touch(wp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
			gm.Expect(record.Generation).To(gm.Equal(uint32(3)))
		}) // it

		gg.It("must support operate write", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"), as.NewBin("bin2", "bal1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			record, err := client.Operate(wp, key,
				as.PutOp(as.NewBin(binName, "val2")),
				as.GetBinOp("bin2"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["bin2"]).To(gm.Equal("bal1"))

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must support operate write abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"), as.NewBin("bin2", "bal1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			record, err := client.Operate(wp, key,
				as.PutOp(as.NewBin(binName, "val2")),
				as.GetBinOp("bin2"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["bin2"]).To(gm.Equal("bal1"))

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must support UDF", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"), as.NewBin("bin2", "bal1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			_, err = client.Execute(
				wp,
				key,
				"record_example",
				"writeBin",
				as.NewValue(binName),
				as.NewValue("val2"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val2"))
		}) // it

		gg.It("must support UDF and abort", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"), as.NewBin("bin2", "bal1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()

			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			_, err = client.Execute(
				wp,
				key,
				"record_example",
				"writeBin",
				as.NewValue(binName),
				as.NewValue("val2"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("val1"))
		}) // it

		gg.It("must support batchDelete", func() {
			bin := as.NewBin(binName, 1)
			keys := make([]*as.Key, 10)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key

				err := client.PutBins(nil, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			records, err := client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}

			txn := as.NewTxn()

			bp := as.NewBatchPolicy()
			bp.Txn = txn

			dp := as.NewBatchDeletePolicy()
			dp.DurableDelete = true

			_, err = client.BatchDelete(bp, dp, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			records, err = client.BatchGet(bp, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).To(gm.BeNil())
			}

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			records, err = client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).To(gm.BeNil())
			}
		}) // it

		gg.It("must support batchDelete and abort", func() {
			bin := as.NewBin(binName, 1)
			keys := make([]*as.Key, 10)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key

				err := client.PutBins(nil, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			records, err := client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}

			txn := as.NewTxn()

			bp := as.NewBatchPolicy()
			bp.Txn = txn

			dp := as.NewBatchDeletePolicy()
			dp.DurableDelete = true

			_, err = client.BatchDelete(bp, dp, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			records, err = client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}
		}) // it

		gg.It("must support batch", func() {
			bin := as.NewBin(binName, 1)
			keys := make([]*as.Key, 10)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key

				err := client.PutBins(nil, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			records, err := client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}

			txn := as.NewTxn()

			bin = as.NewBin(binName, 2)

			bp := as.NewBatchPolicy()
			bp.Txn = txn

			brecs := make([]as.BatchRecordIfc, len(keys))
			for i := range brecs {
				brecs[i] = as.NewBatchWrite(nil, keys[i], as.PutOp(bin))
			}

			err = client.BatchOperate(bp, brecs)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			records, err = client.BatchGet(bp, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(2))
			}

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			records, err = client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(2))
			}
		}) // it

		gg.It("must support batch and abort", func() {
			bin := as.NewBin(binName, 1)
			keys := make([]*as.Key, 10)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key
				err := client.PutBins(nil, key, bin)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			records, err := client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}

			txn := as.NewTxn()

			bin = as.NewBin(binName, 2)

			pp := as.NewBatchPolicy()
			pp.Txn = txn

			brecs := make([]as.BatchRecordIfc, len(keys))
			for i := range brecs {
				brecs[i] = as.NewBatchWrite(nil, keys[i], as.PutOp(bin))
			}

			err = client.BatchOperate(pp, brecs)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))

			records, err = client.BatchGet(nil, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for i := range records {
				gm.Expect(records[i]).ToNot(gm.BeNil())
				gm.Expect(records[i].Bins[binName]).To(gm.Equal(1))
			}
		}) // it

		gg.It("UDF should not read expired record", func() {
			k, _ := as.NewKey("test", "demo", 0)
			client.PutBins(nil, k, as.NewBin("bin", 10))

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			client.PutBins(wp, k, as.NewBin("bin", 20))

			time.Sleep(40 * time.Second)

			p := as.NewPolicy()
			p.Txn = txn

			r, err := client.Get(p, k)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(r.Bins["bin"]).To(gm.Equal(10))

			r, err = client.Get(nil, k)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(r.Bins["bin"]).To(gm.Equal(10))

			_, err = client.Commit(txn)
			gm.Expect(err).To(gm.HaveOccurred())

			p = as.NewPolicy()
			p.Txn = txn
			_, err = client.Get(p, k)
			gm.Expect(err).To(gm.HaveOccurred())

			wp = as.NewWritePolicy(0, 0)
			wp.Txn = txn
			_, err = client.Execute(wp, k, "mrt_ops", "rec_read")
			gm.Expect(err).To(gm.HaveOccurred())
		})

		gg.It("must handle different key pointers", func() {

			getGen := func(cli *as.Client, key *as.Key, txn *as.Txn) int {
				var wp *as.WritePolicy
				if txn != nil {
					wp = as.NewWritePolicy(0, 0)
					wp.Txn = txn
				}
				if val, err := cli.Execute(wp, key, "record_example", "get_gen"); err == nil {
					return val.(int)
				} else {
					panic(err)
				}
			}

			key, _ := as.NewKey(ns, set, []byte("0"))
			err = client.PutBins(nil, key, as.NewBin("bin", 1))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(getGen(client, key, nil)).To(gm.Equal(1))

			txn := as.NewTxn()

			wp1 := as.NewWritePolicy(0, 0)
			wp1.Txn = txn
			key, _ = as.NewKey(ns, set, []byte("0"))
			err = client.PutBins(wp1, key, as.NewBin("bin", 2))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(getGen(client, key, txn)).To(gm.Equal(2))

			key, _ = as.NewKey(ns, set, []byte("0"))
			err = client.PutBins(wp1, key, as.NewBin("bin", 2))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(getGen(client, key, txn)).To(gm.Equal(3))

			key, _ = as.NewKey(ns, set, []byte("0"))
			err = client.PutBins(wp1, key, as.NewBin("bin", 2))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(getGen(client, key, txn)).To(gm.Equal(4))

			key, _ = as.NewKey(ns, set, []byte("0"))
			err = client.PutBins(wp1, key, as.NewBin("bin", 2))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(getGen(client, key, txn)).To(gm.Equal(5))

			key, _ = as.NewKey(ns, set, []byte("0"))
			if err := client.PutBins(wp1, key, as.NewBin("bin", 2)); err != nil {
				fmt.Println(err.Error())
			}
			gm.Expect(getGen(client, key, txn)).To(gm.Equal(6))

			cs, err := client.Commit(txn)
			gm.Expect(err).NotTo(gm.HaveOccurred())
			gm.Expect(cs).To(gm.Equal(as.CommitStatusOK))

			gm.Expect(getGen(client, key, nil)).To(gm.Equal(7))
		}) // it

		gg.It("must ensure MRT_VERSION_MISMATCH is always returned regardless of the number of records", func() {
			for _, count := range []int{1, 10, 100, 1000} {
				var keys []*as.Key
				for i := 0; i < count; i++ {
					key, _ := as.NewKey(ns, set, i)
					err := client.PutBins(nil, key, as.NewBin("bin", 1000))
					gm.Expect(err).NotTo(gm.HaveOccurred())
					keys = append(keys, key)
				}

				txn := as.NewTxn()

				for _, key := range keys {
					p := as.NewPolicy()
					p.Txn = txn
					rec, err := client.Get(p, key)
					gm.Expect(err).NotTo(gm.HaveOccurred())
					gm.Expect(rec).NotTo(gm.BeNil())
				}

				key0, _ := as.NewKey(ns, set, 0)
				err := client.PutBins(nil, key0, as.NewBin("bin", 999))
				gm.Expect(err).NotTo(gm.HaveOccurred())

				_, err = client.Commit(txn)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(types.MRT_VERSION_MISMATCH)).To(gm.BeTrue())
			}
		})

		gg.It("must succeed BatchGet with expressions and transaction", func() {
			gg.Skip("Skipped due to issue tracked in CLIENT-4009")

			client.Truncate(nil, "test", "", nil)

			startKey := 0
			endKey := 999
			num := 300 // example value, change to test others like 0, 600, 1010
			// Generate keys
			keys := make([]*as.Key, 0, endKey-startKey+1)
			for i := startKey; i <= endKey; i++ {
				key, err := as.NewKey(ns, set, i)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				keys = append(keys, key)

			}

			// Create the expression: bin_i > num
			expr := as.ExpGreater(
				as.ExpIntBin("bin_i"),
				as.ExpIntVal(int64(num)),
			)

			// Create MRT transaction
			txn := as.NewTxn()

			// Create policy with expression and txn
			policy := as.NewBatchPolicy()
			policy.BasePolicy = *as.NewPolicy()
			policy.FilterExpression = expr
			policy.Txn = txn

			// Perform BatchGet
			records, err := client.BatchGet(policy, keys)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Check filtered-in records
			for _, rec := range records {
				if rec != nil {
					val := rec.Bins["bin_i"]
					gm.Expect(val).NotTo(gm.BeNil())
					gm.Expect(val.(int)).To(gm.BeNumerically(">", num))
				}
			}

			_, err = client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.It("must support BatchGetComplex operation with transaction", func() {
			// Test that indexRecords path correctly parses fields
			keys := make([]*as.Key, 5)
			brecs := make([]*as.BatchRead, 5)

			for i := range keys {
				key, _ := as.NewKey(ns, set, i)
				keys[i] = key
				err := client.PutBins(nil, key, as.NewBin(binName, i))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				brecs[i] = as.NewBatchRead(nil, key, []string{binName})
			}

			txn := as.NewTxn()
			bp := as.NewBatchPolicy()
			bp.Txn = txn

			err := client.BatchGetComplex(bp, brecs)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Verify records were populated
			for i := range brecs {
				gm.Expect(brecs[i].Record).ToNot(gm.BeNil())
				gm.Expect(brecs[i].Record.Bins[binName]).To(gm.Equal(i))
			}

			// Modify a record outside transaction
			err = client.PutBins(nil, keys[0], as.NewBin(binName, 999))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Commit should detect version mismatch
			_, err = client.Commit(txn)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.MRT_VERSION_MISMATCH)).To(gm.BeTrue())
		})

		gg.It("must allow abort after clean mark-roll-forward failure", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.Txn = txn

			err = client.PutBins(wp, key, as.NewBin(binName, "val2"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			mkey, _ := as.NewKey(ns, "<ERO~MRT", txn.Id())
			dwp := as.NewWritePolicy(0, 0)
			dwp.DurableDelete = true
			_, err = client.Delete(dwp, mkey)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			commitStatus, err := client.Commit(txn)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.MRT_EXPIRED)).To(gm.BeTrue())
			gm.Expect(err.IsInDoubt()).To(gm.BeFalse())
			gm.Expect(commitStatus).To(gm.Equal(as.CommitStatusMarkRollForwardAbandoned))
			gm.Expect(txn.GetInDoubt()).To(gm.BeFalse())
			gm.Expect(txn.State()).To(gm.Equal(as.TxnStateVerified))

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))
			gm.Expect(txn.State()).To(gm.Equal(as.TxnStateAborted))
		})

		gg.It("must allow abort after verify failure because transaction was rolled back", func() {
			key, _ := as.NewKey(ns, set, randString(50))

			err := client.PutBins(nil, key, as.NewBin(binName, "val1"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			txn := as.NewTxn()
			rp := as.NewPolicy()
			rp.Txn = txn

			_, err = client.Get(rp, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.PutBins(nil, key, as.NewBin(binName, "val3"))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			_, err = client.Commit(txn)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(txn.State()).To(gm.Equal(as.TxnStateAborted))

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusAlreadyAborted))
		})

	}) // describe
})
