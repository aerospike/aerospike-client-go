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
	"bytes"
	"math"
	"math/rand"

	as "github.com/aerospike/aerospike-client-go/v6"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Scan operations", func() {

	// connection data
	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)
	wpolicy.SendKey = true

	const keyCount = 1000
	const ldtElemCount = 10
	bin1 := as.NewBin("Aerospike1", rand.Intn(math.MaxInt16))
	bin2 := as.NewBin("Aerospike2", randString(100))
	var keys map[string]*as.Key

	// read all records from the channel and make sure all of them are returned
	// if cancelCnt is set, it will cancel the scan after specified record count
	var checkResults = func(recordset *as.Recordset, cancelCnt int, checkLDT bool) int {
		counter := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			key, exists := keys[string(rec.Key.Digest())]

			gm.Expect(exists).To(gm.Equal(true))
			gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))
			gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
			gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

			ldt := res.Record.Bins["LDT"]
			if checkLDT {
				gm.Expect(ldt).NotTo(gm.BeNil())
				gm.Expect(len(ldt.([]interface{}))).To(gm.Equal(ldtElemCount))
			} else {
				gm.Expect(ldt).To(gm.BeNil())
			}

			delete(keys, string(rec.Key.Digest()))

			counter++
			// cancel scan abruptly
			if cancelCnt != 0 && counter == cancelCnt {
				recordset.Close()
			}
		}

		gm.Expect(counter).To(gm.BeNumerically(">", 0))
		return counter
	}

	gg.BeforeEach(func() {
		keys = make(map[string]*as.Key, keyCount)
		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			keys[string(key.Digest())] = key
			err = client.PutBins(wpolicy, key, bin1, bin2)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
	})

	var scanPolicy = as.NewScanPolicy()

	gg.It("must Scan and paginate to get all records back from all partitions concurrently", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		pf := as.NewPartitionFilterAll()
		spolicy := as.NewScanPolicy()
		spolicy.MaxRecords = 30

		times := 0
		received := 0
		for received < keyCount {
			times++
			recordset, err := client.ScanPartitions(spolicy, pf, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			recs := checkResults(recordset, 0, false)
			gm.Expect(recs).To(gm.BeNumerically("<=", int(spolicy.MaxRecords)))
			received += recs
		}

		gm.Expect(times).To(gm.BeNumerically(">=", keyCount/spolicy.MaxRecords))
		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and paginate using a persisted cursor to get all records back from all partitions concurrently", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		spolicy := as.NewScanPolicy()
		spolicy.MaxRecords = 30

		received := 0
		var buf []byte
		times := 0
		for received < keyCount {
			times++
			pf := as.NewPartitionFilterAll()

			if len(buf) > 0 {
				err = pf.DecodeCursor(buf)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			recordset, err := client.ScanPartitions(spolicy, pf, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			recs := checkResults(recordset, 0, false)
			gm.Expect(recs).To(gm.BeNumerically("<=", int(spolicy.MaxRecords)))
			received += recs

			buf, err = pf.EncodeCursor()
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		gm.Expect(times).To(gm.BeNumerically(">=", keyCount/spolicy.MaxRecords))
		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and get all records back from all partitions concurrently", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		pf := as.NewPartitionFilterByRange(0, 4096)
		recordset, err := client.ScanPartitions(nil, pf, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0, false)

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and get all partition records back for a specified key", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		counter := 0

		var rkey *as.Key
		for _, k := range keys {
			rkey = k

			pf := as.NewPartitionFilterByKey(rkey)
			recordset, err := client.ScanPartitions(scanPolicy, pf, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).NotTo(gm.HaveOccurred())
				gm.Expect(res.Record.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(res.Record.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				// the key itself should not be returned
				gm.Expect(bytes.Equal(rkey.Digest(), res.Record.Key.Digest())).To(gm.BeFalse())

				delete(keys, string(res.Record.Key.Digest()))

				counter++
			}

		}
		gm.Expect(len(keys)).To(gm.BeNumerically(">", 0))
		gm.Expect(counter).To(gm.BeNumerically(">", 0))
		gm.Expect(counter).To(gm.BeNumerically("<", keyCount))
	})

	gg.It("must Scan and get all partition records back for a specified partition range", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		pbegin := 1000
		for i := 1; i < 10; i++ {
			counter := 0

			pf := as.NewPartitionFilterByRange(pbegin, rand.Intn(i*191)+1)
			recordset, err := client.ScanPartitions(scanPolicy, pf, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).NotTo(gm.HaveOccurred())
				gm.Expect(res.Record.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(res.Record.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				delete(keys, string(res.Record.Key.Digest()))

				counter++

				gm.Expect(counter).To(gm.BeNumerically(">", 0))
				gm.Expect(counter).To(gm.BeNumerically("<", keyCount))
			}
		}
		gm.Expect(len(keys)).To(gm.BeNumerically(">", 0))
	})

	gg.It("must Scan and get all records back for a specified node using Results() channel", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		counter := 0
		for _, node := range client.GetNodes() {
			recordset, err := client.ScanNode(scanPolicy, node, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).NotTo(gm.HaveOccurred())
				key, exists := keys[string(res.Record.Key.Digest())]

				gm.Expect(exists).To(gm.Equal(true))
				gm.Expect(key.Value().GetObject()).To(gm.Equal(res.Record.Key.Value().GetObject()))
				gm.Expect(res.Record.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(res.Record.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				delete(keys, string(res.Record.Key.Digest()))

				counter++
			}
		}

		gm.Expect(len(keys)).To(gm.Equal(0))
		gm.Expect(counter).To(gm.Equal(keyCount))
	})

	gg.It("must Scan and get all records back for a specified node", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		for _, node := range client.GetNodes() {
			recordset, err := client.ScanNode(scanPolicy, node, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			checkResults(recordset, 0, false)
		}

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and get all records back from all nodes concurrently", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		recordset, err := client.ScanAll(scanPolicy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0, false)

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and get all records back from all nodes concurrently with policy.RecordsPerSecond set", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		policy := as.NewScanPolicy()
		policy.RecordsPerSecond = keyCount - 100
		recordset, err := client.ScanAll(policy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0, false)

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and get all records back from all nodes concurrently without the Bin Data", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		sp := as.NewScanPolicy()
		sp.IncludeBinData = false
		recordset, err := client.ScanAll(sp, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			rec := res.Record
			key, exists := keys[string(rec.Key.Digest())]

			gm.Expect(exists).To(gm.Equal(true))
			gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))
			gm.Expect(len(rec.Bins)).To(gm.Equal(0))

			delete(keys, string(res.Record.Key.Digest()))
		}

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and get all records back from all nodes sequnetially", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		scanPolicy := as.NewScanPolicy()
		scanPolicy.MaxConcurrentNodes = 1

		recordset, err := client.ScanAll(scanPolicy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0, false)

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Cancel Scan", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		recordset, err := client.ScanAll(scanPolicy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, keyCount/2, false)

		gm.Expect(len(keys)).To(gm.BeNumerically("<=", keyCount/2))
	})
})
