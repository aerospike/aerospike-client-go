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
	"math"
	"math/rand"
	"time"

	as "github.com/aerospike/aerospike-client-go"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Truncate operations test", func() {

	gg.Context("Truncate", func() {
		var err error
		var ns = *namespace
		var set = randString(50)
		var key *as.Key
		var wpolicy = as.NewWritePolicy(0, 0)
		wpolicy.SendKey = true

		const keyCount = 1000
		bin1 := as.NewBin("Aerospike1", rand.Intn(math.MaxInt16))
		bin2 := as.NewBin("Aerospike2", randString(100))

		gg.BeforeEach(func() {
			err := client.Truncate(nil, ns, set, nil)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)
			for i := 0; i < keyCount; i++ {
				key, err = as.NewKey(ns, set, i)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				_, err = client.Operate(wpolicy, key, as.PutOp(bin1), as.PutOp(bin2), as.GetOp())
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
		})

		var countRecords = func(namespace, setName string) int {
			stmt := as.NewStatement(namespace, setName)
			res, err := client.Query(nil, stmt)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			cnt := 0
			for rec := range res.Results() {
				gm.Expect(rec.Err).ToNot(gm.HaveOccurred())
				cnt++
			}

			return cnt
		}

		gg.It("must truncate only the current set", func() {
			gm.Expect(countRecords(ns, set)).To(gm.Equal(keyCount))

			err := client.Truncate(nil, ns, set, nil)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)
			gm.Expect(countRecords(ns, set)).To(gm.Equal(0))
		})

		gg.It("must truncate the whole namespace", func() {
			gm.Expect(countRecords(ns, "")).ToNot(gm.Equal(0))

			err := client.Truncate(nil, ns, "", nil)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)
			gm.Expect(countRecords(ns, "")).To(gm.Equal(0))
		})

		gg.It("must truncate only older records", func() {
			time.Sleep(3 * time.Second)
			t := time.Now()

			gm.Expect(countRecords(ns, set)).To(gm.Equal(keyCount))

			for i := keyCount; i < 2*keyCount; i++ {
				key, err = as.NewKey(ns, set, i)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				_, err = client.Operate(wpolicy, key, as.PutOp(bin1), as.PutOp(bin2), as.GetOp())
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
			gm.Expect(countRecords(ns, set)).To(gm.Equal(2 * keyCount))

			err := client.Truncate(nil, ns, set, &t)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(3 * time.Second)
			gm.Expect(countRecords(ns, set)).To(gm.Equal(keyCount))
		})

	})
})
