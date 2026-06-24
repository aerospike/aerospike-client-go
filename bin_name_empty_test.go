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

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Regression coverage. The Aerospike server accepts empty bin
// names (notably for single-bin namespaces); the v8.6.0 client introduced a
// `binName != ""` clause in writeAndValidateBinName that erroneously rejected
// them locally before the request reached the wire. The Java client performs
// no such client-side check. These tests pin the corrected behavior so the
// regression cannot reappear.
var _ = gg.Describe("Empty bin names — regression coverage", func() {

	var ns = *namespace
	var set = randString(50)
	var key *as.Key

	gg.BeforeEach(func() {
		var err as.Error
		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		_, _ = client.Delete(nil, key)
	})

	gg.It("PutBins accepts an empty bin name", func() {
		err := client.PutBins(nil, key, as.NewBin("", "single-bin-value"))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", "single-bin-value"))
	})

	gg.It("Put with BinMap{\"\": v} accepts an empty bin name", func() {
		err := client.Put(nil, key, as.BinMap{"": 42})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", 42))
	})

	gg.It("AddBins accepts an empty bin name", func() {
		err := client.PutBins(nil, key, as.NewBin("", 10))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		err = client.AddBins(nil, key, as.NewBin("", 5))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", 15))
	})

	gg.It("AppendBins accepts an empty bin name (and creates the bin if absent)", func() {
		// Mirrors the Python regression test: append to a non-existent
		// empty-named bin must create the bin with the supplied value.
		err := client.AppendBins(nil, key, as.NewBin("", "pune"))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", "pune"))
	})

	gg.It("PrependBins accepts an empty bin name", func() {
		err := client.PutBins(nil, key, as.NewBin("", "world"))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		err = client.PrependBins(nil, key, as.NewBin("", "hello "))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", "hello world"))
	})

	gg.It("Operate(PutOp) accepts an empty bin name", func() {
		_, err := client.Operate(nil, key, as.PutOp(as.NewBin("", "via-operate")))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", "via-operate"))
	})

	gg.It("Operate(AppendOp) accepts an empty bin name on a non-existent bin", func() {
		// Direct port of the Python test_pos_operate_with_nonexistent_bin
		// scenario: append to a non-existent bin must create it. Reading
		// back via Get because Operate's per-op GetBinOp("") is the
		// "read all bins" sentinel and won't isolate the empty-named bin.
		_, err := client.Operate(nil, key, as.AppendOp(as.NewBin("", "pune")))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", "pune"))
	})

	gg.It("Operate(PrependOp) accepts an empty bin name", func() {
		err := client.PutBins(nil, key, as.NewBin("", "world"))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		_, err2 := client.Operate(nil, key, as.PrependOp(as.NewBin("", "hello ")))
		gm.Expect(err2).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", "hello world"))
	})

	gg.It("BatchOperate(BatchWrite + PutOp) accepts an empty bin name", func() {
		bw := as.NewBatchWrite(nil, key, as.PutOp(as.NewBin("", "via-batch")))
		err := client.BatchOperate(nil, []as.BatchRecordIfc{bw})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(bw.BatchRec().Err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.HaveKeyWithValue("", "via-batch"))
	})
})
