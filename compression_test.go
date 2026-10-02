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

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Failed to parse large compressed read responses. Small records still passed and masked the regression.
var _ = gg.Describe("Compression", func() {
	const (
		smallPayload = 64
		largePayload = 64 * 1024 // well above 128-byte client/server threshold
	)

	var (
		setName string
	)

	gg.BeforeEach(func() {
		setName = randString(20)
	})

	gg.It("writes and reads with UseCompression (small record)", func() {
		if !isEnterpriseEdition() {
			gg.Skip("requires Enterprise Edition compression support")
		}

		key, err := as.NewKey(*namespace, setName, randString(10))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		wp := as.NewWritePolicy(0, 0)
		wp.UseCompression = true
		gm.Expect(client.Put(wp, key, as.BinMap{"bin": strings.Repeat("x", smallPayload)})).ToNot(gm.HaveOccurred())

		rp := as.NewPolicy()
		rp.UseCompression = true
		rec, err := client.Get(rp, key, "bin")
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["bin"]).To(gm.Equal(strings.Repeat("x", smallPayload)))
	})

	gg.It("writes and reads with UseCompression (large record)", func() {
		if !isEnterpriseEdition() {
			gg.Skip("requires Enterprise Edition compression support")
		}

		key, err := as.NewKey(*namespace, setName, randString(10))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		payload := strings.Repeat("y", largePayload)

		wp := as.NewWritePolicy(0, 0)
		wp.UseCompression = true
		gm.Expect(client.Put(wp, key, as.BinMap{"bin": payload})).ToNot(gm.HaveOccurred())

		rp := as.NewPolicy()
		rp.UseCompression = true
		rec, err := client.Get(rp, key, "bin")
		gm.Expect(err).ToNot(gm.HaveOccurred(), "large compressed read should not PARSE_ERROR")
		gm.Expect(rec.Bins["bin"]).To(gm.Equal(payload))
	})

	// Compressed and plain commands share the client in one spec, so a connection buffer
	// leaked into the shared pool is picked up regardless of spec order.
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
				}
			}()
		}
		wg.Wait()
	})

})
