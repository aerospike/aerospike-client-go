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
	"math"
	"math/rand"

	. "github.com/aerospike/aerospike-client-go"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Scan operations", func() {
	initTestVars()

	// connection data
	var ns = "test"
	var set = randString(50)
	var wpolicy = NewWritePolicy(0, 0)
	wpolicy.SendKey = true

	const keyCount = 100
	bin1 := NewBin("Aerospike1", rand.Intn(math.MaxInt16))
	bin2 := NewBin("Aerospike2", randString(100))
	var keys map[string]*Key

	// use the same client for all
	client, err := NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		panic(err)
	}

	// read all records from the channel and make sure all of them are returned
	// if cancelCnt is set, it will cancel the scan after specified record count
	var checkResults = func(recordset *Recordset, cancelCnt int) {
		counter := 0
	L:
		for {
			select {
			case rec := <-recordset.Records:
				if rec == nil {
					break L
				}
				key, exists := keys[string(rec.Key.Digest())]

				Expect(exists).To(Equal(true))
				Expect(key.Value().GetObject()).To(Equal(rec.Key.Value().GetObject()))
				Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
				Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))

				delete(keys, string(rec.Key.Digest()))

				counter++
				// cancel scan abruptly
				if cancelCnt != 0 && counter == cancelCnt {
					recordset.Close()
				}

			case err := <-recordset.Errors:
				// Expect(error) returns humongous error message
				panic(err)
			}
		}

		Expect(counter).To(BeNumerically(">", 0))
	}

	BeforeEach(func() {
		keys = make(map[string]*Key, keyCount)
		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())

			keys[string(key.Digest())] = key
			err = client.PutBins(wpolicy, key, bin1, bin2)
			Expect(err).ToNot(HaveOccurred())
		}
	})

	It("must Scan and get all records back for a specified node", func() {
		for _, node := range client.GetNodes() {
			recordset, err := client.ScanNode(nil, node, ns, set)
			Expect(err).ToNot(HaveOccurred())

			checkResults(recordset, 0)
		}

		Expect(len(keys)).To(Equal(0))
	})

	It("must Scan and get all records back from all nodes concurrently", func() {
		recordset, err := client.ScanAll(nil, ns, set)
		Expect(err).ToNot(HaveOccurred())

		checkResults(recordset, 0)

		Expect(len(keys)).To(Equal(0))
	})

	It("must Scan and get all records back from all nodes sequnetially", func() {
		scanPolicy := NewScanPolicy()
		scanPolicy.ConcurrentNodes = false

		recordset, err := client.ScanAll(scanPolicy, ns, set)
		Expect(err).ToNot(HaveOccurred())

		checkResults(recordset, 0)

		Expect(len(keys)).To(Equal(0))
	})

	It("must Cancel Scan", func() {
		recordset, err := client.ScanAll(nil, ns, set)
		Expect(err).ToNot(HaveOccurred())

		checkResults(recordset, keyCount/2)

		Expect(len(keys)).To(BeNumerically("<=", keyCount/2))
	})

})
