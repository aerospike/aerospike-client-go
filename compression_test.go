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

package aerospike_test

import (
	"math/rand"
	"strings"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v7"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Compression", func() {

	var setName string

	gg.BeforeEach(func() {
		setName = randString(20)
	})

	// Compressed and plain commands share the client in one spec, so a connection buffer
	// leaked into the shared pool is picked up regardless of spec order.
	//
	// The traffic mix follows the tiered pool's routing: a donated connection
	// buffer is DefaultBufferSize and lands in the tier consumed only by new
	// connections, while oversized requests draw from the next tier up. The
	// churn clients create connections during the storm and the large
	// operations borrow from the pool.
	//
	// This is an integration smoke test, not a deterministic guard: sync.Pool
	// synchronizes Put and Get, so a shared buffer is invisible to the race
	// detector and only shows up as corruption when two accesses collide. The
	// deterministic guards are in command_buffer_release_test.go.
	gg.It("does not share a connection buffer between concurrent commands after compressed commands", func() {
		if !isEnterpriseEdition() {
			gg.Skip("requires Enterprise Edition compression support")
		}

		const (
			concurrency = 16
			iterations  = 50
		)

		cwp := as.NewWritePolicy(0, 0)
		cwp.UseCompression = true
		crp := as.NewPolicy()
		crp.UseCompression = true

		var wg sync.WaitGroup

		// Connection churn: each fresh client draws DefaultBufferSize buffers
		// from the shared pool for its new connections, exactly where a
		// donated live connection buffer would be waiting.
		const churnClients = 8
		wg.Add(churnClients)
		for j := 0; j < churnClients; j++ {
			go func() {
				defer gg.GinkgoRecover()
				defer wg.Done()
				for i := 0; i < 5; i++ {
					churn, err := as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					key, err := as.NewKey(*namespace, setName, randString(20))
					gm.Expect(err).ToNot(gm.HaveOccurred())
					payload := randString(100)
					gm.Expect(churn.PutBins(nil, key, as.NewBin("p", payload))).ToNot(gm.HaveOccurred())
					rec, err := churn.Get(nil, key, "p")
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins["p"]).To(gm.Equal(payload))
					churn.Close()
				}
			}()
		}

		wg.Add(concurrency)
		for j := 0; j < concurrency; j++ {
			go func() {
				defer gg.GinkgoRecover()
				defer wg.Done()
				for i := 0; i < iterations; i++ {
					// below the compression threshold, so the request is sent uncompressed
					ckey, err := as.NewKey(*namespace, setName, randString(10))
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(client.Put(cwp, ckey, as.BinMap{"bin": "x"})).ToNot(gm.HaveOccurred())
					_, err = client.Get(crp, ckey, "bin")
					gm.Expect(err).ToNot(gm.HaveOccurred())

					key, err := as.NewKey(*namespace, setName, randString(50))
					gm.Expect(err).ToNot(gm.HaveOccurred())
					s, n := randString(10), rand.Int()
					gm.Expect(client.PutBins(nil, key, as.NewBin("s", s), as.NewBin("n", n))).ToNot(gm.HaveOccurred())
					rec, err := client.Get(nil, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(rec.Bins["s"]).To(gm.Equal(s))
					gm.Expect(rec.Bins["n"]).To(gm.Equal(n))

					// Over DefaultBufferSize: this request borrows its buffer
					// from the shared pool, where it would collide with any
					// donated live connection buffer.
					bigKey, err := as.NewKey(*namespace, setName, randString(20))
					gm.Expect(err).ToNot(gm.HaveOccurred())
					big := strings.Repeat(randString(1), as.DefaultBufferSize+1024)
					gm.Expect(client.PutBins(nil, bigKey, as.NewBin("big", big))).ToNot(gm.HaveOccurred())
					bigRec, err := client.Get(nil, bigKey, "big")
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(bigRec.Bins["big"]).To(gm.Equal(big))
				}
			}()
		}
		wg.Wait()
	})

})
