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
	"math"
	"math/rand"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Index operations test", func() {

	gg.Describe("Index creation", func() {

		var err error
		var ns = *namespace
		var set = randString(50)
		var key *as.Key
		var wpolicy = as.NewWritePolicy(0, 0)

		const keyCount = 1000
		bin1 := as.NewBin("Aerospike1", rand.Intn(math.MaxInt16))
		bin2 := as.NewBin("Aerospike2", randString(100))

		gg.BeforeEach(func() {
			for i := 0; i < keyCount; i++ {
				key, err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				err = client.PutBins(wpolicy, key, bin1, bin2)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
		})

		gg.Context("Create non-existing index", func() {

			gg.It("must create an Index", func() {
				idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, as.STRING)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer client.DropIndex(wpolicy, ns, set, set+bin1.Name)

				// wait until index is created
				<-idxTask.OnComplete()

				// no duplicate index is allowed
				// _, err = client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, as.STRING)
				// gm.Expect(err).To(gm.HaveOccurred())
				// gm.Expect(err.Matches(ast.INDEX_FOUND)).To(gm.BeTrue())
			})

			gg.It("must drop an Index", func() {
				idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, as.STRING)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// wait until index is created
				<-idxTask.OnComplete()

				err = client.DropIndex(wpolicy, ns, set, set+bin1.Name)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				err = client.DropIndex(wpolicy, ns, set, set+bin1.Name)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

			gg.It("must drop an Index, and recreate it again to verify", func() {
				idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, as.STRING)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// wait until index is created
				gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

				// dropping second time is not expected to raise any errors
				err = client.DropIndex(wpolicy, ns, set, set+bin1.Name)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// create the index again; should not encounter any errors
				idxTask, err = client.CreateIndex(wpolicy, ns, set, set+bin1.Name, bin1.Name, as.STRING)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// wait until index is created
				gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

				err = client.DropIndex(wpolicy, ns, set, set+bin1.Name)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			})

		})

	})

	gg.Describe("INTEGER Index creation", func() {

		var ns = *namespace
		var set = randString(50)
		var wpolicy = as.NewWritePolicy(0, 0)
		var integerIndexName = set + "intbin"

		const keyCount = 100

		gg.BeforeEach(func() {
			if serverIsOlderThan("8.1.3") {
				gg.Skip("INTEGER index type requires server version 8.1.3+")
			}

			for i := 0; i < keyCount; i++ {
				key, err := as.NewKey(ns, set, i)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				err = client.PutBins(wpolicy, key, as.NewBin("intbin", i))
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
		})

		gg.It("must create an INTEGER index and query it", func() {
			idxTask, err := client.CreateIndex(wpolicy, ns, set, integerIndexName, "intbin", as.INTEGER)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer client.DropIndex(wpolicy, ns, set, integerIndexName)

			// wait until index is created
			gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

			// range query on the INTEGER index
			stmt := as.NewStatement(ns, set)
			gm.Expect(stmt.SetFilter(as.NewRangeFilter("intbin", 10, 19))).ToNot(gm.HaveOccurred())

			rs, err := client.Query(nil, stmt)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			count := 0
			for res := range rs.Results() {
				gm.Expect(res.Err).ToNot(gm.HaveOccurred())
				gm.Expect(res.Record.Bins["intbin"]).To(gm.BeNumerically(">=", 10))
				gm.Expect(res.Record.Bins["intbin"]).To(gm.BeNumerically("<=", 19))
				count++
			}
			gm.Expect(count).To(gm.Equal(10))

			// equality query on the INTEGER index
			stmt = as.NewStatement(ns, set)
			gm.Expect(stmt.SetFilter(as.NewEqualFilter("intbin", 42))).ToNot(gm.HaveOccurred())

			rs, err = client.Query(nil, stmt)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			count = 0
			for res := range rs.Results() {
				gm.Expect(res.Err).ToNot(gm.HaveOccurred())
				gm.Expect(res.Record.Bins["intbin"]).To(gm.Equal(42))
				count++
			}
			gm.Expect(count).To(gm.Equal(1))
		})

	})

	gg.Describe("Set Index creation", func() {

		var ns = *namespace
		var set = randString(50)
		var wpolicy = as.NewWritePolicy(0, 0)
		var setIndexName = set + "setindex"

		gg.BeforeEach(func() {
			if serverIsOlderThan("8.1.2") {
				gg.Skip("Set index requires server version 8.1.2+")
			}
		})

		gg.It("must create and drop a Set Index", func() {
			// Drop set index if it already exists
			client.DropIndex(wpolicy, ns, set, setIndexName)

			idxTask, err := client.CreateSetIndex(wpolicy, ns, set, setIndexName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// wait until index is created
			gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

			err = client.DropIndex(wpolicy, ns, set, setIndexName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.It("must drop a Set Index, and recreate it again to verify", func() {
			// Drop set index if it already exists
			client.DropIndex(wpolicy, ns, set, setIndexName)

			idxTask, err := client.CreateSetIndex(wpolicy, ns, set, setIndexName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// wait until index is created
			gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

			// drop the index
			err = client.DropIndex(wpolicy, ns, set, setIndexName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// create the index again; should not encounter any errors
			idxTask, err = client.CreateSetIndex(wpolicy, ns, set, setIndexName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// wait until index is created
			gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

			err = client.DropIndex(wpolicy, ns, set, setIndexName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

	})
})
