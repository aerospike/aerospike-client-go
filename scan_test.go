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
	"errors"
	"math"
	"math/rand"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"
	particleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

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
	var scanPolicy = as.NewScanPolicy()

	const keyCount = 1000
	bin1 := as.NewBin("Aerospike1", rand.Intn(math.MaxInt16))
	bin2 := as.NewBin("Aerospike2", randString(100))
	bin3 := as.NewBin("map", map[string]int{"1": 1, "2": 2})
	bin4 := as.NewBin("list", []int{1, 2, 3})
	var keys map[string]*as.Key

	// read all records from the channel and make sure all of them are returned
	// if cancelCnt is set, it will cancel the scan after specified record count
	var checkResults = func(recordset *as.Recordset, cancelCnt int, rawCDT bool) int {
		counter := 0
		for rec, err := range recordset.Records() {
			gm.Expect(err).ToNot(gm.HaveOccurred())
			key, exists := keys[string(rec.Key.Digest())]

			gm.Expect(exists).To(gm.Equal(true))
			gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))

			gm.Expect(rec.Bins[bin3.Name]).NotTo(gm.BeNil())
			gm.Expect(rec.Bins[bin4.Name]).NotTo(gm.BeNil())
			if rawCDT {
				gm.Expect(rec.Bins[bin3.Name].(*as.RawBlobValue).ParticleType).To(gm.Equal(particleType.MAP))
				gm.Expect(rec.Bins[bin4.Name].(*as.RawBlobValue).ParticleType).To(gm.Equal(particleType.LIST))

				// rewrite the record to the database to see if the values are correctly written
				err := client.Put(nil, rec.Key, rec.Bins)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			} else {
				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				gm.Expect(rec.Bins[bin3.Name]).To(gm.Equal(map[any]any{"1": 1, "2": 2}))
				gm.Expect(rec.Bins[bin4.Name]).To(gm.Equal([]any{1, 2, 3}))

				delete(keys, string(rec.Key.Digest()))
			}

			counter++
			// cancel scan abruptly
			if cancelCnt != 0 && counter == cancelCnt {
				recordset.Close()
			}
		}

		gm.Expect(counter).To(gm.BeNumerically(">", 0))
		return counter
	}

	// read all records from the channel and make sure all of them are returned
	// if cancelCnt is set, it will cancel the scan after specified record count
	var checkResultsIter = func(recordset *as.Recordset, cancelCnt int, rawCDT bool) int {
		counter := 0
		for rec, err := range recordset.Records() {
			gm.Expect(err).ToNot(gm.HaveOccurred())
			key, exists := keys[string(rec.Key.Digest())]

			gm.Expect(exists).To(gm.Equal(true))
			gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))

			gm.Expect(rec.Bins[bin3.Name]).NotTo(gm.BeNil())
			gm.Expect(rec.Bins[bin4.Name]).NotTo(gm.BeNil())
			if rawCDT {
				gm.Expect(rec.Bins[bin3.Name].(*as.RawBlobValue).ParticleType).To(gm.Equal(particleType.MAP))
				gm.Expect(rec.Bins[bin4.Name].(*as.RawBlobValue).ParticleType).To(gm.Equal(particleType.LIST))

				// rewrite the record to the database to see if the values are correctly written
				err := client.Put(nil, rec.Key, rec.Bins)
				gm.Expect(err).ToNot(gm.HaveOccurred())
			} else {
				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				gm.Expect(rec.Bins[bin3.Name]).To(gm.Equal(map[any]any{"1": 1, "2": 2}))
				gm.Expect(rec.Bins[bin4.Name]).To(gm.Equal([]any{1, 2, 3}))

				delete(keys, string(rec.Key.Digest()))
			}

			counter++
			// cancel scan abruptly
			if cancelCnt != 0 && counter == cancelCnt {
				recordset.Close()
			}
		}

		gm.Expect(counter).To(gm.BeNumerically(">", 0))
		return counter
	}

	var getMasterPartitionIds = func(length int) []int {
		partitionIds := make([]int, length)
		for i := range partitionIds {
			partitionIds[i] = rand.Intn(4096)
		}
		return partitionIds
	}

	gg.BeforeEach(func() {
		keys = make(map[string]*as.Key, keyCount)
		set = randString(50)
		for i := 0; i < keyCount; i++ {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			keys[string(key.Digest())] = key
			err = client.PutBins(wpolicy, key, bin1, bin2, bin3, bin4)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
	})

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

	gg.It("must Scan and paginate to get all records back from all partitions concurrently using Records() iterator", func() {
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

			recs := checkResultsIter(recordset, 0, false)
			gm.Expect(recs).To(gm.BeNumerically("<=", int(spolicy.MaxRecords)))
			received += recs
		}

		gm.Expect(times).To(gm.BeNumerically(">=", keyCount/spolicy.MaxRecords))
		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and paginate to get all records back from all partitions concurrently, ONE BY ONE", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		pf := as.NewPartitionFilterAll()
		spolicy := as.NewScanPolicy()
		spolicy.MaxRecords = 1

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

			for rec, err := range recordset.Records() {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				// the key itself should not be returned
				gm.Expect(bytes.Equal(rkey.Digest(), rec.Key.Digest())).To(gm.BeFalse())

				delete(keys, string(rec.Key.Digest()))

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

			for rec, err := range recordset.Records() {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				delete(keys, string(rec.Key.Digest()))

				counter++

				gm.Expect(counter).To(gm.BeNumerically(">", 0))
				gm.Expect(counter).To(gm.BeNumerically("<", keyCount))
			}
		}
		gm.Expect(len(keys)).To(gm.BeNumerically(">", 0))
	})

	gg.It("must Scan and get all partition records back for a specified partitions", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))
		previousReplicaValue := scanPolicy.ReplicaPolicy
		previousMaxRecordsValue := scanPolicy.MaxRecords

		// Making sure that the replica policy is set back to original value
		defer func() {
			scanPolicy.ReplicaPolicy = previousReplicaValue
			scanPolicy.MaxRecords = previousMaxRecordsValue
		}()

		for i := 5; i < 1000; i++ {
			scanPolicy.ReplicaPolicy = as.MASTER
			partitions := getMasterPartitionIds(i)
			scanPolicy.MaxRecords = 1
			counter := 0

			pf, _ := as.NewPartitionFilterSelectPartitions(partitions)
			recordset, err := client.ScanPartitions(scanPolicy, pf, ns, set)

			gm.Expect(err).ToNot(gm.HaveOccurred())

			for rec, err := range recordset.Records() {
				key, exists := keys[string(rec.Key.Digest())]

				gm.Expect(exists).To(gm.Equal(true))
				gm.Expect(err).NotTo(gm.HaveOccurred())

				gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))
				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				counter++

				gm.Expect(counter).To(gm.BeNumerically(">", 0))
				gm.Expect(counter).To(gm.BeNumerically("<", keyCount))
			}
		}
	})

	gg.It("must Scan and get all records back for a specified node using Results() channel", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		counter := 0
		for _, node := range client.GetNodes() {
			recordset, err := client.ScanNode(scanPolicy, node, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for rec, err := range recordset.Records() {
				gm.Expect(err).NotTo(gm.HaveOccurred())
				key, exists := keys[string(rec.Key.Digest())]

				gm.Expect(exists).To(gm.Equal(true))
				gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))
				gm.Expect(rec.Bins[bin1.Name]).To(gm.Equal(bin1.Value.GetObject()))
				gm.Expect(rec.Bins[bin2.Name]).To(gm.Equal(bin2.Value.GetObject()))

				delete(keys, string(rec.Key.Digest()))

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

		for rec, err := range recordset.Records() {
			gm.Expect(err).ToNot(gm.HaveOccurred())
			key, exists := keys[string(rec.Key.Digest())]

			gm.Expect(exists).To(gm.Equal(true))
			gm.Expect(key.Value().GetObject()).To(gm.Equal(rec.Key.Value().GetObject()))
			gm.Expect(len(rec.Bins)).To(gm.Equal(0))

			delete(keys, string(rec.Key.Digest()))
		}

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and get all records back from all nodes sequentially", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		scanPolicy := as.NewScanPolicy()
		scanPolicy.MaxConcurrentNodes = 1

		recordset, err := client.ScanAll(scanPolicy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0, false)

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Scan and get all records back in RawBlobValue when policy.RawCDT is set to true", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		scanPolicy := as.NewScanPolicy()
		scanPolicy.MaxConcurrentNodes = 1
		scanPolicy.RawCDT = true

		recordset, err := client.ScanAll(scanPolicy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0, scanPolicy.RawCDT)

		scanPolicy.RawCDT = false

		recordset, err = client.ScanAll(scanPolicy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, 0, scanPolicy.RawCDT)

		gm.Expect(len(keys)).To(gm.Equal(0))
	})

	gg.It("must Cancel Scan", func() {
		gm.Expect(len(keys)).To(gm.Equal(keyCount))

		recordset, err := client.ScanAll(scanPolicy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		checkResults(recordset, keyCount/2, false)

		gm.Expect(len(keys)).To(gm.BeNumerically("<=", keyCount/2))
	})

	gg.It("must return the error for invalid expression", func() {
		scanPolicy := as.NewScanPolicy()
		// the right expression has to be as.ExpListVal(as.NewIntegerValue(11)) for valid expression
		scanPolicy.FilterExpression = as.ExpEq(as.ExpListGetByValueRange(as.ListReturnTypeValue, as.ExpIntVal(10), as.ExpIntVal(13), as.ExpListBin(bin1.Name)), as.ExpIntVal(11))
		recordset, err := client.ScanAll(scanPolicy, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		counter := 0
		for _, err := range recordset.Records() {
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue())
		}

		gm.Expect(counter).To(gm.Equal(0))
	})

	gg.It("must return error for out of range partition IDs", func() {
		// Test negative partition ID
		_, err := as.NewPartitionFilterSelectPartitions([]int{-1})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, &as.AerospikeError{ResultCode: ast.PARAMETER_ERROR})).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Partition id out of range"))

		// Test partition ID >= 4096
		_, err = as.NewPartitionFilterSelectPartitions([]int{4096})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, &as.AerospikeError{ResultCode: ast.PARAMETER_ERROR})).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Partition id out of range"))

		// Test partition ID way above range
		_, err = as.NewPartitionFilterSelectPartitions([]int{10000})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, &as.AerospikeError{ResultCode: ast.PARAMETER_ERROR})).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Partition id out of range"))

		// Test multiple partition IDs with one out of range
		_, err = as.NewPartitionFilterSelectPartitions([]int{100, 200, -5})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, &as.AerospikeError{ResultCode: ast.PARAMETER_ERROR})).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Partition id out of range"))

		// Test multiple partition IDs with one at upper boundary
		_, err = as.NewPartitionFilterSelectPartitions([]int{100, 200, 4095, 4096})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, &as.AerospikeError{ResultCode: ast.PARAMETER_ERROR})).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Partition id out of range"))
	})

	gg.It("must handle single partition ID correctly", func() {
		// Test with partition ID 1 (should succeed)
		pf, err := as.NewPartitionFilterSelectPartitions([]int{1})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(pf).ToNot(gm.BeNil())

		// Test with partition ID 0 (should succeed)
		pf, err = as.NewPartitionFilterSelectPartitions([]int{0})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(pf).ToNot(gm.BeNil())

		// Test with partition ID 4095 (maximum valid, should succeed)
		pf, err = as.NewPartitionFilterSelectPartitions([]int{4095})
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(pf).ToNot(gm.BeNil())

		// Test scanning with single partition ID
		_, err = as.NewPartitionFilterSelectPartitions([]int{1})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Test scanning with single partition ID
		_, err = as.NewPartitionFilterSelectPartitions([]int{5})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		recordset, err := client.ScanPartitions(scanPolicy, pf, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(recordset).ToNot(gm.BeNil())

		// Just ensure we can iterate (may be empty depending on data distribution)
		for rec, err := range recordset.Records() {
			if err != nil {
				// If there's an error, it shouldn't be a parameter error
				gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeFalse())
				break
			}
			gm.Expect(rec).ToNot(gm.BeNil())
		}
	})

	gg.It("must return error for empty partition ID list", func() {
		_, err := as.NewPartitionFilterSelectPartitions([]int{})
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, &as.AerospikeError{ResultCode: ast.PARAMETER_ERROR})).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Partition ids is empty"))

		// Also test with nil slice
		_, err = as.NewPartitionFilterSelectPartitions(nil)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(errors.Is(err, &as.AerospikeError{ResultCode: ast.PARAMETER_ERROR})).To(gm.BeTrue())
		gm.Expect(err.Error()).To(gm.ContainSubstring("Partition ids is empty"))
	})
})
