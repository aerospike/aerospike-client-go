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

package histogram_test

import (
	"testing"

	"github.com/aerospike/aerospike-client-go/v8/types/histogram"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

func TestHistogram(t *testing.T) {
	gm.RegisterFailHandler(gg.Fail)
	gg.RunSpecs(t, "Histogram Suite")
}

var _ = gg.Describe("Histogram", func() {

	gg.Context("Integer Values", func() {

		gg.Context("Linear", func() {

			gg.It("must make the correct histogram", func() {
				l := []int{1, 1, 3, 4, 5, 5, 9, 11, 11, 11, 16, 16, 21}
				h := histogram.New[int](histogram.Linear, 5, 5)

				sum := 0
				for _, v := range l {
					sum += v
					h.Add(v)
				}

				gm.Expect(h.Min).To(gm.Equal(1))
				gm.Expect(h.Max).To(gm.Equal(21))
				gm.Expect(uint64(h.Count)).To(gm.Equal(uint64(len(l))))
				gm.Expect(h.Sum).To(gm.Equal(float64(sum)))
				gm.Expect(h.Buckets).To(gm.Equal([]uint64{4, 3, 3, 2, 1}))
			})

			gg.It("must find the correct median", func() {
				l := []int{1e3, 2e3, 3e3, 4e3, 5e3, 6e3, 7e3, 8e3, 9e3, 10e3, 11e3, 12e3, 13e3}
				h := histogram.New[int](histogram.Linear, 1000, 10)

				sum := 0
				for _, v := range l {
					sum += v
					h.Add(v)
				}

				gm.Expect(h.Min).To(gm.Equal(1000))
				gm.Expect(h.Max).To(gm.Equal(13000))
				gm.Expect(uint64(h.Count)).To(gm.Equal(uint64(len(l))))
				gm.Expect(h.Sum).To(gm.Equal(float64(sum)))
				gm.Expect(h.Buckets).To(gm.Equal([]uint64{0, 1, 1, 1, 1, 1, 1, 1, 1, 5}))
				gm.Expect(h.Median()).To(gm.Equal(7000))
			})

		})

		gg.Context("Exponential", func() {

			gg.It("must make the correct histogram", func() {
				l := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
				h := histogram.New[int](histogram.Logarithmic, 2, 5)

				sum := 0
				for _, v := range l {
					sum += v
					h.Add(v)
				}

				gm.Expect(h.Min).To(gm.Equal(0))
				gm.Expect(h.Max).To(gm.Equal(20))
				gm.Expect(uint64(h.Count)).To(gm.Equal(uint64(len(l))))
				gm.Expect(h.Sum).To(gm.Equal(float64(sum)))
				gm.Expect(h.Buckets).To(gm.Equal([]uint64{2, 2, 4, 8, 5}))
			})

			gg.It("must make the correct histogram on barriers", func() {
				l := []int{0, 1, 2, 3, 4, 5, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 255, 256, 257, 511, 512, 513, 1023, 1024, 1025}
				h := histogram.New[int](histogram.Logarithmic, 4, 8)

				sum := 0
				for _, v := range l {
					sum += v
					h.Add(v)
				}

				gm.Expect(h.Min).To(gm.Equal(0))
				gm.Expect(h.Max).To(gm.Equal(1025))
				gm.Expect(uint64(h.Count)).To(gm.Equal(uint64(len(l))))
				gm.Expect(h.Sum).To(gm.Equal(float64(sum)))
				gm.Expect(h.Buckets).To(gm.Equal([]uint64{4, 6, 6, 6, 6, 2, 0, 0}))
			})

			gg.It("must find the correct median", func() {
				l := []int{10e3, 12e3, 3e3, 4e3, 50e3, 6e5, 75e3, 7e3, 21e3, 11e3, 113e3, 29e3, 189e3}
				h := histogram.New[int](histogram.Logarithmic, 2, 18)

				sum := 0
				for _, v := range l {
					sum += v
					h.Add(v)
				}

				gm.Expect(h.Min).To(gm.Equal(int(3e3)))
				gm.Expect(h.Max).To(gm.Equal(int(600e3)))
				gm.Expect(uint64(h.Count)).To(gm.Equal(uint64(len(l))))
				gm.Expect(h.Sum).To(gm.Equal(float64(sum)))
				gm.Expect(h.Buckets).To(gm.Equal([]uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 3, 2, 1, 2, 2}))
				gm.Expect(h.Median()).To(gm.Equal(1 << 14))
			})
		})

		gg.Context("Log2Histogram", func() {

			gg.It("must make the correct histogram", func() {
				l := []uint64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
				h := histogram.NewLog2(5)

				var sum uint64
				for _, v := range l {
					sum += v
					h.Add(v)
				}

				gm.Expect(h.Min).To(gm.Equal(uint64(0)))
				gm.Expect(h.Max).To(gm.Equal(uint64(20)))
				gm.Expect(uint64(h.Count)).To(gm.Equal(uint64(len(l))))
				gm.Expect(h.Sum).To(gm.Equal(sum))
				gm.Expect(h.Buckets).To(gm.Equal([]uint64{2, 2, 4, 8, 5}))
			})

			gg.It("must find the correct median", func() {
				l := []uint64{10e3, 12e3, 3e3, 4e3, 50e3, 6e5, 75e3, 7e3, 21e3, 11e3, 113e3, 29e3, 189e3}
				h := histogram.NewLog2(18)

				var sum uint64
				for _, v := range l {
					sum += v
					h.Add(v)
				}

				gm.Expect(h.Min).To(gm.Equal(uint64(3000)))
				gm.Expect(h.Max).To(gm.Equal(uint64(600000)))
				gm.Expect(uint64(h.Count)).To(gm.Equal(uint64(len(l))))
				gm.Expect(h.Sum).To(gm.Equal(sum))
				gm.Expect(h.Buckets).To(gm.Equal([]uint64{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 1, 3, 2, 1, 2, 2}))
				gm.Expect(h.Median()).To(gm.Equal(uint64(1 << 14)))
			})
		})
		gg.Context("LatencyBuckets", func() {
			gg.It("uses shift buckets matching learn-metrics defaults", func() {
				h := histogram.NewLatencyBuckets(7, 1)
				h.Add(0)
				h.Add(1)
				h.Add(2)
				h.Add(3)
				h.Add(5)
				h.Add(33)
				gm.Expect(h.Buckets).To(gm.Equal([]uint64{2, 1, 1, 1, 0, 0, 1}))
				gm.Expect(histogram.LatencyBucketIndex(1, 7, 1)).To(gm.Equal(0))
				gm.Expect(histogram.LatencyBucketIndex(2, 7, 1)).To(gm.Equal(1))
				gm.Expect(histogram.LatencyBucketIndex(4, 7, 1)).To(gm.Equal(2))
				gm.Expect(histogram.LatencyBucketIndex(8, 7, 1)).To(gm.Equal(3))
			})

			gg.It("preserves Min of 0 across Merge", func() {
				a := histogram.NewLatencyBuckets(7, 1)
				a.Add(0)
				a.Add(0)
				b := histogram.NewLatencyBuckets(7, 1)
				b.Add(3)
				a.Merge(b)
				gm.Expect(a.Min).To(gm.Equal(uint64(0)))
				gm.Expect(a.Max).To(gm.Equal(uint64(3)))
				gm.Expect(a.Count).To(gm.Equal(uint64(3)))
			})

			gg.It("Add on zero-value buckets is a no-op", func() {
				var h histogram.LatencyBuckets
				gm.Expect(func() { h.Add(5) }).NotTo(gm.Panic())
				gm.Expect(h.Count).To(gm.Equal(uint64(0)))
			})

			gg.It("LatencyBucketIndex covers boundaries", func() {
				gm.Expect(histogram.LatencyBucketIndex(0, 7, 1)).To(gm.Equal(0))
				gm.Expect(histogram.LatencyBucketIndex(1, 7, 1)).To(gm.Equal(0))
				gm.Expect(histogram.LatencyBucketIndex(^uint64(0), 7, 1)).To(gm.Equal(6))
				// shift=2: <=1, >1(≤4), >4(≤16), >16(≤64), ...
				gm.Expect(histogram.LatencyBucketIndex(16, 7, 2)).To(gm.Equal(2))
				gm.Expect(histogram.LatencyBucketIndex(17, 7, 2)).To(gm.Equal(3))
			})

			gg.It("Merge ignores shape mismatch", func() {
				a := histogram.NewLatencyBuckets(7, 1)
				a.Add(1)
				b := histogram.NewLatencyBuckets(5, 1)
				b.Add(1)
				a.Merge(b)
				gm.Expect(a.Count).To(gm.Equal(uint64(1)))
			})
		})
	})
})
