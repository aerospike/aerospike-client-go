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

package types

import (
	"math/rand"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("BufferPool Test", func() {

	gg.Context("Any size Buffer Pool", func() {
		var bp *BufferPool
		check := func(sz int) {
			buf := bp.Get(sz)

			gm.Expect(len(buf)).To(gm.BeNumerically(">=", sz))
			if sz <= maxBufSize {
				if powerOf2(sz) {
					gm.Expect(len(buf)).To(gm.BeNumerically("==", 1<<(fastlog2(uint64(sz)))))
					gm.Expect(cap(buf)).To(gm.BeNumerically("==", 1<<(fastlog2(uint64(sz)))))
				} else {
					gm.Expect(len(buf)).To(gm.BeNumerically("==", 1<<(fastlog2(uint64(sz))+1)))
					gm.Expect(cap(buf)).To(gm.BeNumerically("==", 1<<(fastlog2(uint64(sz))+1)))
				}
			} else {
				gm.Expect(len(buf)).To(gm.BeNumerically("==", sz))
			}
			bp.Put(buf)
		}

		gg.It("should return a buffer with correct size", func() {
			bp = NewBufferPool()

			for i := 1; i < 24; i++ {
				check(1<<i - 1)
				check(1 << i)
				check(1<<i + 1)
			}
		})

		gg.It("should return a buffer with correct size of random value", func() {
			bp = NewBufferPool()

			for i := 1; i < 1e5; i++ {
				check(rand.Intn(maxBufSize))
			}
		})

	})
})
