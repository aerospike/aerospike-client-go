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

package aerospike

import (
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// A read record must stay eligible for the batch repeat flag regardless of the batch policy(parent policy) sendKey
// (reads never send the key). We encode and compare sizes: a repeated read costs ~1 byte vs a full
// namespace/set header, so the repeated-read batch must be smaller; if equal, the repeat was lost.

var _ = gg.Describe("Batch read repeat optimization is preserved regardless of parent BatchPolicy.SendKey", func() {

	// Minimal client whose nil-policy lookups resolve to the built-in defaults (no server).
	newEncodeClient := func() *Client {
		return &Client{
			DefaultBatchPolicy:       NewBatchPolicy(),
			DefaultBatchReadPolicy:   NewBatchReadPolicy(),
			DefaultBatchWritePolicy:  NewBatchWritePolicy(),
			DefaultBatchDeletePolicy: NewBatchDeletePolicy(),
			DefaultBatchUDFPolicy:    NewBatchUDFPolicy(),
		}
	}

	offsets := func(n int) BatchOffsets {
		o := make([]int, n)
		for i := range o {
			o[i] = i
		}
		return &batchOffsetsNative{offsets: o}
	}

	rKey, _ := NewKey("test", "ck4898_repeat", "r")
	wKey, _ := NewKey("test", "ck4898_repeat", "w")

	// Encodes a 3-record mixed batch [write, read, third] and returns the wire size. `third` is
	// produced from the shared read so the caller can pass back the SAME pointer (repeat-eligible)
	// or a distinct one (full header).
	encodeSize := func(parentSendKey bool, third func(shared BatchRecordIfc) BatchRecordIfc) int {
		client := newEncodeClient()
		policy := NewBatchPolicy()
		policy.SendKey = parentSendKey

		bw := NewBatchWrite(nil, wKey, PutOp(NewBin("b", 1)))
		shared := NewBatchRead(nil, rKey, []string{"b"})

		records := []BatchRecordIfc{bw, shared, third(shared)}
		cmd := &baseCommand{}
		_, err := cmd.setBatchOperateIfcOffsets(client, policy, records, offsets(3))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return cmd.dataOffset
	}

	repeated := func(shared BatchRecordIfc) BatchRecordIfc { return shared } // same pointer → repeat
	distinct := func(BatchRecordIfc) BatchRecordIfc {                        // new pointer → full header
		return NewBatchRead(nil, rKey, []string{"b"})
	}

	// Read repeat must apply regardless of the parent's sendKey — reads never send the key, so a
	// repeated read encodes ~1 byte while a distinct read needs a full namespace/set header. This
	// must hold for parent SendKey both true (the bug: parent must not block read repeat) and false.
	gg.It("read records keep the repeat flag independent of parent sendKey (true & false)", func() {
		for _, parentSendKey := range []bool{true, false} {
			rep := encodeSize(parentSendKey, repeated)
			dist := encodeSize(parentSendKey, distinct)
			gm.Expect(rep).To(gm.BeNumerically("<", dist),
				"read repeat must apply with parent BatchPolicy.SendKey=%v", parentSendKey)
		}
	})
})
