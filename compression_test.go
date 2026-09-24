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
	"strings"

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

})
