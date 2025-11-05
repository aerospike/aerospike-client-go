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

package atomic_test

import (
	"runtime"

	"github.com/aerospike/aerospike-client-go/v8/internal/atomic"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Atomic Guard", func() {
	// atomic tests require actual parallelism
	runtime.GOMAXPROCS(runtime.NumCPU())

	type S struct {
		a int
		b bool
	}

	var grd *atomic.Guard[S]

	gg.BeforeEach(func() {
		grd = atomic.NewGuard[S](&S{a: 1, b: true})
	})

	gg.It("must pass internal value correctly", func() {
		grd.Do(func(s *S) {
			gm.Expect(*s).To(gm.Equal(S{a: 1, b: true}))
		})

	})

	gg.It("must assign/copy internal value correctly", func() {
		local := S{a: 99, b: false}
		grd.Do(func(s *S) {
			*s = local
		})

		grd.Do(func(s *S) {
			gm.Expect(*s).To(gm.Equal(S{a: 99, b: false}))
		})

	})

	gg.It("must initialize and assign internal value correctly", func() {
		flocal := func() *S { return &S{a: 99, b: false} }

		var grd atomic.Guard[S]
		grd.Do(func(s *S) {
			gm.Expect(s).To(gm.BeNil())
		})

		grd.InitDo(flocal, func(s *S) {
			gm.Expect(*s).To(gm.Equal(S{a: 99, b: false}))
			s.a++
			s.b = true
		})

		grd.InitDo(flocal, func(s *S) {
			gm.Expect(*s).To(gm.Equal(S{a: 100, b: true}))
		})

		grd.Do(func(s *S) {
			gm.Expect(*s).To(gm.Equal(S{a: 100, b: true}))
		})
	})

	gg.It("must initialize and assign internal value correctly", func() {
		flocal := func() map[int]int { return map[int]int{1: 1, 2: 2, 3: 3} }

		var grd atomic.Guard[map[int]int]
		grd.Do(func(s *map[int]int) {
			gm.Expect(s).To(gm.BeNil())
		})

		grd.InitDoVal(flocal, func(s map[int]int) {
			gm.Expect(s).To(gm.Equal(map[int]int{1: 1, 2: 2, 3: 3}))
		})

		grd.InitDoVal(flocal, func(s map[int]int) {
			gm.Expect(s).To(gm.Equal(map[int]int{1: 1, 2: 2, 3: 3}))
			for i := 4; i < 100; i++ {
				s[i] = i
			}
		})

		grd.DoVal(func(s map[int]int) {
			gm.Expect(len(s)).To(gm.Equal(99))
		})
	})

	gg.It("must replace internal value's reference correctly", func() {
		local := S{a: 99, b: false}
		grd.Update(func(s **S) error {
			*s = &local

			return nil
		})

		grd.Do(func(s *S) {
			gm.Expect(s == &local).To(gm.BeTrue())
		})

	})
})
