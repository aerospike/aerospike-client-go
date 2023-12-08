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
	"sync"

	"github.com/aerospike/aerospike-client-go/v7/internal/atomic"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Atomic Bool", func() {
	// atomic tests require actual parallelism
	runtime.GOMAXPROCS(runtime.NumCPU())

	var ab *atomic.Bool

	gg.BeforeEach(func() {
		ab = atomic.NewBool(true)
	})

	gg.It("must CompareAndToggle correctly", func() {
		gm.Expect(ab.CompareAndToggle(true)).To(gm.BeTrue())
		gm.Expect(ab.CompareAndToggle(true)).To(gm.BeFalse())
	})

	gg.It("must CompareAndToggle correctly", func() {
		var count int = 1e5
		wg := new(sync.WaitGroup)
		wg.Add(count * 4)
		for i := 0; i < count; i++ {
			go func() {
				defer wg.Done()
				ab.Set(true)
			}()
		}

		for i := 0; i < count; i++ {
			go func() {
				defer wg.Done()
				ab.Set(false)
			}()
		}

		for i := 0; i < count; i++ {
			go func() {
				defer wg.Done()
				ab.Get()
			}()
		}

		for i := 0; i < count; i++ {
			go func(i int) {
				defer wg.Done()
				ab.CompareAndToggle(i%2 == 0)
			}(i)
		}

		wg.Wait()
	})

})
