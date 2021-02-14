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

package atomic_test

import (
	"runtime"

	"github.com/aerospike/aerospike-client-go/internal/atomic"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

type testStruct struct{ i int }

var _ = gg.Describe("Atomic Queue", func() {
	// atomic tests require actual parallelism
	runtime.GOMAXPROCS(runtime.NumCPU())

	var qcap int
	var q *atomic.Queue
	var elem interface{}

	gg.BeforeEach(func() {
		qcap = 10
		q = atomic.NewQueue(qcap)
	})

	gg.It("must Offer() more elements than queue's capacity, and still not block", func() {
		for i := 0; i < 2*qcap; i++ {
			q.Offer(&testStruct{})
		}
	})

	gg.It("must Poll() more elements than queue's capacity, and still not block", func() {
		for i := 0; i < 2*qcap; i++ {
			elem = q.Poll()
		}
		gm.Expect(elem).To(gm.BeNil())
	})

	gg.It("must Offer() more elements than queue's capacity, and Poll() as many as capacity", func() {
		// test for many iterations
		for j := 0; j < 10; j++ {
			for i := 0; i < 2*qcap; i++ {
				q.Offer(&testStruct{i})
			}

			for i := 0; i < 2*qcap; i++ {
				obj := q.Poll()
				if i < qcap {
					gm.Expect(obj.(*testStruct).i).To(gm.Equal(i))
				} else {
					gm.Expect(obj).To(gm.BeNil())
				}
			}
		}
	})

})
