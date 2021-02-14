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

package aerospike

import (
	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Connection Heap tests", func() {

	conn := new(Connection)

	gg.Context("singleConnectionHeap", func() {

		gg.It("Must add until full", func() {
			h := newSingleConnectionHeap(10)
			for i := 0; i < 10; i++ {
				gm.Expect(h.Len()).To(gm.Equal(i))
				gm.Expect(h.head).To(gm.Equal(uint32(i)))
				gm.Expect(h.tail).To(gm.Equal(uint32(0)))
				gm.Expect(h.Offer(conn)).To(gm.BeTrue())
				gm.Expect(h.Len()).To(gm.Equal(i + 1))
				gm.Expect(h.head).To(gm.Equal(uint32(i+1) % 10))
				gm.Expect(h.tail).To(gm.Equal(uint32(0)))
			}

			gm.Expect(h.Offer(conn)).To(gm.BeFalse())
			gm.Expect(h.Offer(conn)).To(gm.BeFalse())
			gm.Expect(h.Len()).To(gm.Equal(10))
			gm.Expect(h.full).To(gm.BeTrue())
			gm.Expect(h.head).To(gm.Equal(h.tail))
		})

		gg.It("Must add until full, then Poll successfully", func() {
			h := newSingleConnectionHeap(10)
			for i := 0; i < 10; i++ {
				gm.Expect(h.Offer(conn)).To(gm.BeTrue())
			}

			for i := 0; i < 10; i++ {
				gm.Expect(h.Len()).To(gm.Equal(10 - i))
				gm.Expect(h.head).To(gm.Equal(uint32(10-i) % 10))
				gm.Expect(h.tail).To(gm.Equal(uint32(0)))
				gm.Expect(h.Poll()).NotTo(gm.BeNil())
				gm.Expect(h.full).To(gm.BeFalse())
				gm.Expect(h.Len()).To(gm.Equal(10 - i - 1))
				gm.Expect(h.head).To(gm.Equal(uint32(10 - i - 1)))
				gm.Expect(h.tail).To(gm.Equal(uint32(0)))
			}

			gm.Expect(h.Poll()).To(gm.BeNil())
			gm.Expect(h.Poll()).To(gm.BeNil())
			gm.Expect(h.Len()).To(gm.Equal(0))
			gm.Expect(h.full).To(gm.BeFalse())
			gm.Expect(h.head).To(gm.Equal(h.tail))
		})

		gg.It("Must add then Poll successfully", func() {
			h := newSingleConnectionHeap(10)
			gm.Expect(h.Offer(conn)).To(gm.BeTrue())
			gm.Expect(h.Len()).To(gm.Equal(1))
			gm.Expect(h.head).To(gm.Equal(uint32(1)))
			gm.Expect(h.full).To(gm.BeFalse())

			gm.Expect(h.Poll()).NotTo(gm.BeNil())
			gm.Expect(h.Poll()).To(gm.BeNil())
			gm.Expect(h.Len()).To(gm.Equal(0))
			gm.Expect(h.full).To(gm.BeFalse())
			gm.Expect(h.head).To(gm.Equal(uint32(0)))
			gm.Expect(h.head).To(gm.Equal(h.tail))

		})

	})
})
