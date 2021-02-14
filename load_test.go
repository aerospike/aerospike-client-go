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

package aerospike_test

import (
	"math/rand"
	"runtime"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

func init() {
	// load test require actual parallelism
	runtime.GOMAXPROCS(runtime.NumCPU())
}

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike load tests", func() {
	gg.Describe("Single long random string test", func() {
		var ns = *namespace
		var set = "load"
		var wpolicy = as.NewWritePolicy(0, 0)
		var rpolicy = as.NewPolicy()
		rpolicy.TotalTimeout = 200 * time.Millisecond
		if *useReplicas {
			rpolicy.ReplicaPolicy = as.MASTER_PROLES
		}

		bname1 := randString(14)
		bname2 := randString(14)

		gg.Context("Concurrent Load", func() {

			gg.It("must save and then retrieve an INT and STRING bin with random key", func() {
				const Concurrency = 10
				const IterationPerWorker = 10

				var wg sync.WaitGroup
				wg.Add(Concurrency)

				for j := 0; j < Concurrency; j++ {
					go func() {
						defer gg.GinkgoRecover()
						defer wg.Done()
						for i := 0; i < IterationPerWorker; i++ {
							key, err := as.NewKey(ns, set, randString(50))
							gm.Expect(err).ToNot(gm.HaveOccurred())

							bin1 := as.NewBin(bname1, randString(10))
							bin2 := as.NewBin(bname2, rand.Int())
							err = client.PutBins(wpolicy, key, bin1, bin2)
							gm.Expect(err).ToNot(gm.HaveOccurred())

							rec, err := client.Get(rpolicy, key)
							gm.Expect(err).ToNot(gm.HaveOccurred())
							gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
							gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))
						}
					}()
				}

				// wait until everything is written
				wg.Wait()
			}) // it

		})
	})
})
