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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = Describe("Connection Heap tests", func() {

	conn := new(Connection)

	Context("singleConnectionHeap", func() {

		It("Must add until full", func() {
			h := newSingleConnectionHeap(10)
			for i := 0; i < 10; i++ {
				Expect(h.Len()).To(Equal(i))
				Expect(h.head).To(Equal(uint32(i)))
				Expect(h.tail).To(Equal(uint32(0)))
				Expect(h.Offer(conn)).To(BeTrue())
				Expect(h.Len()).To(Equal(i + 1))
				Expect(h.head).To(Equal(uint32(i+1) % 10))
				Expect(h.tail).To(Equal(uint32(0)))
			}

			Expect(h.Offer(conn)).To(BeFalse())
			Expect(h.Offer(conn)).To(BeFalse())
			Expect(h.Len()).To(Equal(10))
			Expect(h.full).To(BeTrue())
			Expect(h.head).To(Equal(h.tail))
		})

		It("Must add until full, then Poll successfully", func() {
			h := newSingleConnectionHeap(10)
			for i := 0; i < 10; i++ {
				Expect(h.Offer(conn)).To(BeTrue())
			}

			for i := 0; i < 10; i++ {
				Expect(h.Len()).To(Equal(10 - i))
				Expect(h.head).To(Equal(uint32(10-i) % 10))
				Expect(h.tail).To(Equal(uint32(0)))
				Expect(h.Poll()).NotTo(BeNil())
				Expect(h.full).To(BeFalse())
				Expect(h.Len()).To(Equal(10 - i - 1))
				Expect(h.head).To(Equal(uint32(10 - i - 1)))
				Expect(h.tail).To(Equal(uint32(0)))
			}

			Expect(h.Poll()).To(BeNil())
			Expect(h.Poll()).To(BeNil())
			Expect(h.Len()).To(Equal(0))
			Expect(h.full).To(BeFalse())
			Expect(h.head).To(Equal(h.tail))
		})

		It("Must add then Poll successfully", func() {
			h := newSingleConnectionHeap(10)
			Expect(h.Offer(conn)).To(BeTrue())
			Expect(h.Len()).To(Equal(1))
			Expect(h.head).To(Equal(uint32(1)))
			Expect(h.full).To(BeFalse())

			Expect(h.Poll()).NotTo(BeNil())
			Expect(h.Poll()).To(BeNil())
			Expect(h.Len()).To(Equal(0))
			Expect(h.full).To(BeFalse())
			Expect(h.head).To(Equal(uint32(0)))
			Expect(h.head).To(Equal(h.tail))

		})

	})
})
