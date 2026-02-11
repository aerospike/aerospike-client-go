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

package aerospike_test

import (
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Complex Index operations test", func() {

	gg.Describe("Complex Index Creation", func() {
		// connection data
		var err error
		var ns = *namespace
		var set = randString(40)
		var key *as.Key
		var wpolicy = as.NewWritePolicy(0, 0)

		const keyCount = 1000

		valueList := []any{1, 2, 3, "a", "ab", "abc"}
		valueMap := map[any]any{"a": "b", 0: 1, 1: "a", "b": 2}

		bin1 := as.NewBin("Aerospike1", valueList)
		bin2 := as.NewBin("Aerospike2", valueMap)

		gg.BeforeEach(func() {
			for i := 0; i < keyCount; i++ {
				key, err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				err = client.PutBins(wpolicy, key, bin1, bin2)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
		})

		gg.Context("Create non-existing complex index", func() {

			gg.It("must create a complex Index for Lists", func() {
				idxTask, err := client.CreateComplexIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, as.STRING, as.ICT_LIST)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer client.DropIndex(wpolicy, ns, set, set+bin1.Name)

				// wait until index is created
				<-idxTask.OnComplete()

				// no duplicate index is allowed
				_, err = client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, as.STRING)
				gm.Expect(err).To(gm.HaveOccurred())
			})

			gg.It("must create a complex Index for Map Keys", func() {
				idxTask, err := client.CreateComplexIndex(wpolicy, ns, set, set+bin2.Name+"keys", bin2.Name, as.STRING, as.ICT_MAPKEYS)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer client.DropIndex(wpolicy, ns, set, set+bin2.Name+"keys")

				// wait until index is created
				<-idxTask.OnComplete()

				// no duplicate index is allowed
				_, err = client.CreateIndex(wpolicy, ns, set, set+bin2.Name+"keys", bin1.Name, as.STRING)
				gm.Expect(err).To(gm.HaveOccurred())
			})

			gg.It("must create a complex Index for Map Values", func() {
				idxTask, err := client.CreateComplexIndex(wpolicy, ns, set, set+bin2.Name+"values", bin2.Name, as.STRING, as.ICT_MAPVALUES)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer client.DropIndex(wpolicy, ns, set, set+bin2.Name+"values")

				// wait until index is created
				<-idxTask.OnComplete()

				// no duplicate index is allowed
				_, err = client.CreateIndex(wpolicy, ns, set, set+bin2.Name+"values", bin1.Name, as.STRING)
				gm.Expect(err).To(gm.HaveOccurred())
			})

		})

		gg.Context("Concurrent index drop and create operations", func() {
			indexName := "concurrent_test_index"
			binName := "test_bin"

			gg.It("must handle concurrent drop/create without hanging", func() {
				// Use a short timeout policy (5 seconds) to keep test suite fast
				shortTimeoutPolicy := as.NewWritePolicy(0, 0)
				shortTimeoutPolicy.SocketTimeout = 5 * time.Second

				// Clean up any existing index first
				_ = client.DropIndex(shortTimeoutPolicy, ns, set, indexName)

				var wg sync.WaitGroup
				errors := make(chan error, 20)

				for i := 0; i < 10; i++ {
					wg.Add(1)
					go func(iteration int) {
						defer wg.Done()

						err := client.DropIndex(shortTimeoutPolicy, ns, set, indexName)
						if err != nil {
							if !err.Matches(types.INDEX_NOTFOUND) && !err.Matches(types.TIMEOUT) {
								errors <- err
							}
						}

						// Create index
						idxTask, err := client.CreateIndex(shortTimeoutPolicy, ns, set, indexName, binName, as.STRING)
						if err != nil {
							if !err.Matches(types.INDEX_FOUND) && !err.Matches(types.TIMEOUT) {
								errors <- err
							}
						} else {
							err = <-idxTask.OnComplete()
							if err != nil {
								if !err.Matches(types.TIMEOUT) {
									errors <- err
								}
							}
						}
					}(i)
				}

				wg.Wait()
				close(errors)

				var errs []error
				for err := range errors {
					errs = append(errs, err)
				}

				_ = client.DropIndex(shortTimeoutPolicy, ns, set, indexName)

				gm.Expect(errs).To(gm.BeEmpty(), "Unexpected errors during concurrent index operations")
			})
		})

	})
})
