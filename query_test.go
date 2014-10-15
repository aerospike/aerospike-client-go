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
	"flag"
	"math"
	"math/rand"
	"time"

	. "github.com/aerospike/aerospike-client-go"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Query operations", func() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	// connection data
	var ns = "test"
	var set = randString(50)
	var wpolicy = NewWritePolicy(0, 0)
	wpolicy.SendKey = true

	const keyCount = 100
	bin1 := NewBin("Aerospike1", rand.Intn(math.MaxInt16))
	bin2 := NewBin("Aerospike2", randString(100))
	bin3 := NewBin("Aerospike3", rand.Intn(math.MaxInt16))
	var keys map[string]*Key

	// use the same client for all
	client, _ := NewClient(*host, *port)

	// read all records from the channel and make sure all of them are returned
	var checkResults = func(recordset *Recordset, cancelCnt int) {
		counter := 0
	L:
		for {
			select {
			case rec, chanOpen := <-recordset.Records:
				if rec == nil && !chanOpen {
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
				panic(err)
			}
		}

		Expect(counter).To(BeNumerically(">", 0))
	}

	BeforeEach(func() {
		keys = make(map[string]*Key)
		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())

			keys[string(key.Digest())] = key
			bin3 = NewBin("Aerospike3", rand.Intn(math.MaxInt16))
			err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
			Expect(err).ToNot(HaveOccurred())
		}

		// queries only work on indices
		idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin3.Name, bin3.Name, NUMERIC)
		if err == nil {
			// wait until index is created
			for err := range idxTask.OnComplete() {
				if err != nil {
					panic(err)
				}
			}
		}
	})

	It("must Query a range and get all records back", func() {
		stm := NewStatement(ns, set)
		recordset, err := client.Query(nil, stm)
		Expect(err).ToNot(HaveOccurred())

		checkResults(recordset, 0)

		Expect(len(keys)).To(Equal(0))
	})

	It("must Cancel Query abruptly", func() {
		stm := NewStatement(ns, set)
		recordset, err := client.Query(nil, stm)
		Expect(err).ToNot(HaveOccurred())

		checkResults(recordset, keyCount/2)

		Expect(len(keys)).To(BeNumerically("<=", keyCount/2))
	})

	It("must Query a specific range and get only relevant records back", func() {
		stm := NewStatement(ns, set)
		stm.Addfilter(NewRangeFilter(bin3.Name, 0, math.MaxInt16/2))
		recordset, err := client.Query(nil, stm)
		Expect(err).ToNot(HaveOccurred())

		cnt := 0
	L:
		for {
			select {
			case rec, chanOpen := <-recordset.Records:
				if !chanOpen {
					break L
				}
				cnt++
				_, exists := keys[string(rec.Key.Digest())]
				Expect(exists).To(Equal(true))
				Expect(rec.Bins[bin3.Name]).To(BeNumerically("<=", math.MaxInt16/2))
			case err := <-recordset.Errors:
				panic(err)
			}
		}

		Expect(cnt).To(BeNumerically(">", 0))
	})

	It("must Query specific equality filters and get only relevant records back", func() {
		// save a record with requested value
		key, err := NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())

		bin3 := NewBin("Aerospike3", rand.Intn(math.MaxInt16))
		err = client.PutBins(wpolicy, key, bin3)
		Expect(err).ToNot(HaveOccurred())

		stm := NewStatement(ns, set, bin3.Name)
		stm.Addfilter(NewEqualFilter(bin3.Name, bin3.Value))

		recordset, err := client.Query(nil, stm)
		Expect(err).ToNot(HaveOccurred())

		recs := []interface{}{}
		// consume recordset and check errors
	L:
		for {
			select {
			case rec, chanOpen := <-recordset.Records:
				if !chanOpen {
					break L
				}
				Expect(rec).ToNot(BeNil())
				recs = append(recs, rec.Bins[bin3.Name])
			case err := <-recordset.Errors:
				panic(err)
			}
		}

		// there should be at least one result
		Expect(len(recs)).To(BeNumerically(">", 0))
		Expect(recs).To(ContainElement(bin3.Value.GetObject()))
	})

})
