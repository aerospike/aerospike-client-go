//go:build !app_engine

// Copyright 2014-2024 Aerospike, Inc.
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
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Tests for https://github.com/aerospike/aerospike-client-go/pull/655
//
// When a batch node has exactly 1 key assigned to it, several batch command
// types fall back to executeSingle which incorrectly iterates ALL records/keys
// in the entire batch instead of only the record(s) assigned to the current
// node via cmd.batch.offsets.
//
// These tests call executeSingle directly (via helpers in helper_test.go) with
// controlled offsets. After the call, only records at the specified offsets
// should be populated — all others must remain untouched.
// If executeSingle ignores offsets and iterates all records, non-offset records
// will also be populated and the test will fail.
var _ = gg.Describe("Batch executeSingle offset correctness", func() {
	var ns = *namespace
	var set = randString(50)
	var binName = "val"
	var keyCount = 5

	// offsets simulates a node that was assigned only keys at indices 1 and 3
	var offsets = []int{1, 3}

	var keys []*as.Key
	var bpolicy *as.BatchPolicy
	var wpolicy *as.WritePolicy

	gg.BeforeEach(func() {
		bpolicy = as.NewBatchPolicy()
		bpolicy.TotalTimeout = 15 * time.Second
		bpolicy.SocketTimeout = 5 * time.Second

		wpolicy = as.NewWritePolicy(0, 0)

		// Seed records with distinct values: key i -> bin "val" = i
		keys = make([]*as.Key, keyCount)
		for i := 0; i < keyCount; i++ {
			var err error
			keys[i], err = as.NewKey(ns, set, i)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			err = client.PutBins(wpolicy, keys[i], as.NewBin(binName, i))
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
	})

	gg.It("executeSingle must only process offset records (batchIndexCommandGet)", func() {
		reads := make([]*as.BatchRead, keyCount)
		for j := 0; j < keyCount; j++ {
			reads[j] = as.NewBatchRead(nil, keys[j], []string{binName})
		}

		err := as.ExecuteSingleBatchIndexGet(client, bpolicy, reads, offsets)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for j, br := range reads {
			if j == 1 || j == 3 {
				gm.Expect(br.Record).ToNot(gm.BeNil(), "record at offset index %d should be populated", j)
				gm.Expect(br.Record.Bins[binName]).To(gm.Equal(j),
					"record at offset index %d has wrong value: expected %d, got %v", j, j, br.Record.Bins[binName])
			} else {
				gm.Expect(br.Record).To(gm.BeNil(),
					"record at non-offset index %d should be nil but was populated", j)
			}
		}
	})

	gg.It("executeSingle must only process offset records (batchCommandOperate)", func() {
		records := make([]as.BatchRecordIfc, keyCount)
		for j := 0; j < keyCount; j++ {
			records[j] = as.NewBatchRead(as.NewBatchReadPolicy(), keys[j], []string{binName})
		}

		err := as.ExecuteSingleBatchOperate(client, bpolicy, records, offsets)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for j, rec := range records {
			br := rec.BatchRec()
			if j == 1 || j == 3 {
				gm.Expect(br.Record).ToNot(gm.BeNil(), "record at offset index %d should be populated", j)
				gm.Expect(br.Record.Bins[binName]).To(gm.Equal(j),
					"record at offset index %d has wrong value: expected %d, got %v", j, j, br.Record.Bins[binName])
			} else {
				gm.Expect(br.Record).To(gm.BeNil(),
					"record at non-offset index %d should be nil but was populated", j)
			}
		}
	})

	gg.It("executeSingle must only process offset records (batchCommandDelete)", func() {
		records := make([]*as.BatchRecord, keyCount)
		for j := 0; j < keyCount; j++ {
			records[j] = &as.BatchRecord{
				Key: keys[j],
			}
		}

		err := as.ExecuteSingleBatchDelete(client, bpolicy, as.NewBatchDeletePolicy(), keys, records, offsets)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for j, br := range records {
			if j == 1 || j == 3 {
				gm.Expect(br.ResultCode).To(gm.Equal(types.OK),
					"record at offset index %d has result code %s, expected OK", j, br.ResultCode)
			} else {
				gm.Expect(br.Record).To(gm.BeNil(),
					"record at non-offset index %d should be nil but was populated", j)
			}
		}

		// Verify only the offset keys were deleted
		exists, err := client.BatchExists(bpolicy, keys)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		for j, e := range exists {
			if j == 1 || j == 3 {
				gm.Expect(e).To(gm.BeFalse(),
					"key at offset index %d should have been deleted but still exists", j)
			} else {
				gm.Expect(e).To(gm.BeTrue(),
					"key at non-offset index %d should still exist but was deleted", j)
			}
		}
	})
})
