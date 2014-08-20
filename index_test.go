// Copyright 2013-2014 Aerospike, Inc.
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
	// "fmt"
	"math"
	"math/rand"
	// "strings"
	"time"

	. "github.com/aerospike/aerospike-client-go"
	// . "github.com/aerospike/aerospike-client-go/logger"
	// . "github.com/aerospike/aerospike-client-go/types"

	// . "github.com/aerospike/aerospike-client-go/utils/buffer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Index operations test", func() {
	rand.Seed(time.Now().UnixNano())

	Describe("Index creation", func() {
		// connection data
		var client *Client
		var err error
		var ns = "test"
		var set = randString(50)
		var key *Key
		var wpolicy = NewWritePolicy(0, 0)

		const keyCount = 10000
		bin1 := NewBin("Aerospike1", rand.Intn(math.MaxInt16))
		bin2 := NewBin("Aerospike2", randString(100))

		BeforeEach(func() {
			client, err = NewClient("127.0.0.1", 3000)
			Expect(err).ToNot(HaveOccurred())
			key, err = NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())

			for i := 0; i < keyCount; i++ {
				key, err := NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				err = client.PutBins(wpolicy, key, bin1, bin2)
				Expect(err).ToNot(HaveOccurred())
			}
		})

		Context("Create non-existing index", func() {

			It("must create an Index", func() {
				idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, STRING)
				Expect(err).ToNot(HaveOccurred())
				defer client.DropIndex(wpolicy, ns, set, set+bin1.Name)

				// wait until index is created
				<-idxTask.OnComplete()

				// no duplicate index is allowed
				_, err = client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, STRING)
				Expect(err).To(HaveOccurred())
			})

			It("must drop an Index", func() {
				err := client.DropIndex(wpolicy, ns, set, set+bin1.Name)
				Expect(err).ToNot(HaveOccurred())

				err = client.DropIndex(wpolicy, ns, set, set+bin1.Name)
				Expect(err).ToNot(HaveOccurred())
			})

		})

	})
})
