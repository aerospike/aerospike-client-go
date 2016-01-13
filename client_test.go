// Copyright 2013-2016 Aerospike, Inc.
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
	"strings"

	. "github.com/aerospike/aerospike-client-go"
	. "github.com/aerospike/aerospike-client-go/utils/buffer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Aerospike", func() {
	initTestVars()

	Describe("Client Management", func() {
		It("must open and close the client without a problem", func() {
			// use the same client for all
			client, err := NewClientWithPolicy(clientPolicy, *host, *port)
			Expect(err).ToNot(HaveOccurred())
			Expect(client.IsConnected()).To(BeTrue())

			client.Close()
			Expect(client.IsConnected()).To(BeFalse())
		})
	})

	Describe("Data operations on native types", func() {
		// connection data
		var err error
		var ns = "test"
		var set = randString(50)
		var key *Key
		var wpolicy = NewWritePolicy(0, 0)
		var rpolicy = NewPolicy()
		var rec *Record
		var client *Client

		BeforeEach(func() {
			// use the same client for all
			client, err = NewClientWithPolicy(clientPolicy, *host, *port)
			Expect(err).ToNot(HaveOccurred())

			key, err = NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("Put operations", func() {

			Context("Bins with `nil` values should be deleted", func() {
				It("must save a key with SINGLE bin", func() {
					bin := NewBin("Aerospike", "value")
					bin1 := NewBin("Aerospike1", "value2") // to avoid deletion of key
					err = client.PutBins(wpolicy, key, bin, bin1)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))

					bin2 := NewBin("Aerospike", nil)
					err = client.PutBins(wpolicy, key, bin2)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					// Key should not exist
					_, exists := rec.Bins[bin.Name]
					Expect(exists).To(Equal(false))
				})

				It("must save a key with MULTIPLE bins", func() {
					bin1 := NewBin("Aerospike1", "nil")
					bin2 := NewBin("Aerospike2", "value")
					bin3 := NewBin("Aerospike3", "value")
					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					bin2nil := NewBin("Aerospike2", nil)
					bin3nil := NewBin("Aerospike3", nil)
					err = client.PutBins(wpolicy, key, bin2nil, bin3nil)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					// Key should not exist
					_, exists := rec.Bins[bin2.Name]
					Expect(exists).To(Equal(false))
					_, exists = rec.Bins[bin3.Name]
					Expect(exists).To(Equal(false))
				})

				It("must save a key with MULTIPLE bins using a BinMap", func() {
					bin1 := NewBin("Aerospike1", "nil")
					bin2 := NewBin("Aerospike2", "value")
					bin3 := NewBin("Aerospike3", "value")
					err = client.Put(wpolicy, key, BinMap{bin1.Name: bin1.Value, bin2.Name: bin2.Value, bin3.Name: bin3.Value})
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					bin2nil := NewBin("Aerospike2", nil)
					bin3nil := NewBin("Aerospike3", nil)
					err = client.Put(wpolicy, key, BinMap{bin2nil.Name: bin2nil.Value, bin3nil.Name: bin3nil.Value})
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					// Key should not exist
					_, exists := rec.Bins[bin2.Name]
					Expect(exists).To(Equal(false))
					_, exists = rec.Bins[bin3.Name]
					Expect(exists).To(Equal(false))
				})
			})

			Context("Bins with `string` values", func() {
				It("must save a key with SINGLE bin", func() {
					bin := NewBin("Aerospike", "Awesome")
					err = client.PutBins(wpolicy, key, bin)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))
				})

				It("must save a key with MULTIPLE bins", func() {
					bin1 := NewBin("Aerospike1", "Awesome1")
					bin2 := NewBin("Aerospike2", "")
					err = client.PutBins(wpolicy, key, bin1, bin2)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
					Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))
				})
			})

			Context("Bins with `int8` and `uint8` values", func() {
				It("must save a key with SINGLE bin", func() {
					bin := NewBin("Aerospike", int8(rand.Intn(math.MaxInt8)))
					err = client.PutBins(wpolicy, key, bin)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))
				})

				It("must save a key with MULTIPLE bins", func() {
					bin1 := NewBin("Aerospike1", int8(math.MaxInt8))
					bin2 := NewBin("Aerospike2", int8(math.MinInt8))
					bin3 := NewBin("Aerospike3", uint8(math.MaxUint8))
					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
					Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))
					Expect(rec.Bins[bin3.Name]).To(Equal(bin3.Value.GetObject()))
				})
			})

			Context("Bins with `int16` and `uint16` values", func() {
				It("must save a key with SINGLE bin", func() {
					bin := NewBin("Aerospike", int16(rand.Intn(math.MaxInt16)))
					err = client.PutBins(wpolicy, key, bin)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))
				})

				It("must save a key with MULTIPLE bins", func() {
					bin1 := NewBin("Aerospike1", int16(math.MaxInt16))
					bin2 := NewBin("Aerospike2", int16(math.MinInt16))
					bin3 := NewBin("Aerospike3", uint16(math.MaxUint16))
					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
					Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))
					Expect(rec.Bins[bin3.Name]).To(Equal(bin3.Value.GetObject()))
				})
			})

			Context("Bins with `int` and `uint` values", func() {
				It("must save a key with SINGLE bin", func() {
					bin := NewBin("Aerospike", rand.Int())
					err = client.PutBins(wpolicy, key, bin)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))
				})

				It("must save a key with MULTIPLE bins; uint of > MaxInt32 will always result in LongValue", func() {
					bin1 := NewBin("Aerospike1", math.MaxInt32)
					bin2, bin3 := func() (*Bin, *Bin) {
						if Arch32Bits {
							return NewBin("Aerospike2", int(math.MinInt32)),
								NewBin("Aerospike3", uint(math.MaxInt32))
						} else {
							return NewBin("Aerospike2", int(math.MinInt64)),
								NewBin("Aerospike3", uint(math.MaxInt64))
						}
					}()

					err = client.PutBins(wpolicy, key, bin1, bin2, bin3)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
					if Arch64Bits {
						Expect(rec.Bins[bin2.Name].(int)).To(Equal(bin2.Value.GetObject()))
						Expect(int64(rec.Bins[bin3.Name].(int))).To(Equal(bin3.Value.GetObject()))
					} else {
						Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))
						Expect(rec.Bins[bin3.Name]).To(Equal(bin3.Value.GetObject()))
					}
				})
			})

			Context("Bins with `int64` only values (uint64 is supported via type cast to int64) ", func() {
				It("must save a key with SINGLE bin", func() {
					bin := NewBin("Aerospike", rand.Int63())
					err = client.PutBins(wpolicy, key, bin)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					if Arch64Bits {
						Expect(int64(rec.Bins[bin.Name].(int))).To(Equal(bin.Value.GetObject()))
					} else {
						Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))
					}
				})

				It("must save a key with MULTIPLE bins", func() {
					bin1 := NewBin("Aerospike1", math.MaxInt64)
					bin2 := NewBin("Aerospike2", math.MinInt64)
					err = client.PutBins(wpolicy, key, bin1, bin2)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
					Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))
				})
			})

			Context("Bins with `float32` only values", func() {
				It("must save a key with SINGLE bin", func() {
					bin := NewBin("Aerospike", rand.Float32())
					err = client.PutBins(wpolicy, key, bin)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(float64(rec.Bins[bin.Name].(float64))).To(Equal(bin.Value.GetObject()))
				})

				It("must save a key with MULTIPLE bins", func() {
					bin1 := NewBin("Aerospike1", math.MaxFloat32)
					bin2 := NewBin("Aerospike2", -math.MaxFloat32)
					err = client.PutBins(wpolicy, key, bin1, bin2)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
					Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))
				})
			})

			Context("Bins with `float64` only values", func() {
				It("must save a key with SINGLE bin", func() {
					bin := NewBin("Aerospike", rand.Float64())
					err = client.PutBins(wpolicy, key, bin)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())
					Expect(float64(rec.Bins[bin.Name].(float64))).To(Equal(bin.Value.GetObject()))
				})

				It("must save a key with MULTIPLE bins", func() {
					bin1 := NewBin("Aerospike1", math.MaxFloat64)
					bin2 := NewBin("Aerospike2", -math.MaxFloat64)
					err = client.PutBins(wpolicy, key, bin1, bin2)
					Expect(err).ToNot(HaveOccurred())

					rec, err = client.Get(rpolicy, key)
					Expect(err).ToNot(HaveOccurred())

					Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject()))
					Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject()))
				})
			})

			Context("Bins with complex types", func() {

				Context("Bins with BLOB type", func() {
					It("must save and retrieve Bins with AerospikeBlobs type", func() {
						person := &testBLOB{name: "SomeDude"}
						bin := NewBin("Aerospike1", person)
						err = client.PutBins(wpolicy, key, bin)
						Expect(err).ToNot(HaveOccurred())

						rec, err = client.Get(rpolicy, key)
						Expect(err).ToNot(HaveOccurred())
					})
				})

				Context("Bins with LIST type", func() {

					It("must save a key with Array Types", func() {
						bin1 := NewBin("Aerospike1", []int8{math.MinInt8, 0, 1, 2, 3, math.MaxInt8})
						bin2 := NewBin("Aerospike2", []int16{math.MinInt16, 0, 1, 2, 3, math.MaxInt16})
						bin3 := NewBin("Aerospike3", []int32{math.MinInt32, 0, 1, 2, 3, math.MaxInt32})
						bin4 := NewBin("Aerospike4", []int64{math.MinInt64, 0, 1, 2, 3, math.MaxInt64})
						bin5 := NewBin("Aerospike5", []uint8{0, 1, 2, 3, math.MaxUint8})
						bin6 := NewBin("Aerospike6", []uint16{0, 1, 2, 3, math.MaxUint16})
						bin7 := NewBin("Aerospike7", []uint32{0, 1, 2, 3, math.MaxUint32})
						bin8 := NewBin("Aerospike8", []string{"", "\n", "string"})
						bin9 := NewBin("Aerospike9", []interface{}{"", 1, nil, true, false, uint64(math.MaxUint64), math.MaxFloat32, math.MaxFloat64})

						// complex type, consisting different arrays
						bin10 := NewBin("Aerospike10", []interface{}{
							nil,
							bin1.Value.GetObject(),
							bin2.Value.GetObject(),
							bin3.Value.GetObject(),
							bin4.Value.GetObject(),
							bin5.Value.GetObject(),
							bin6.Value.GetObject(),
							bin7.Value.GetObject(),
							bin8.Value.GetObject(),
							bin9.Value.GetObject(),
							map[interface{}]interface{}{
								15:                        nil,
								int8(math.MaxInt8):        int8(math.MaxInt8),
								int64(math.MinInt64):      int64(math.MinInt64),
								int64(math.MaxInt64):      int64(math.MaxInt64),
								uint64(math.MaxUint64):    uint64(math.MaxUint64),
								float32(-math.MaxFloat32): float32(-math.MaxFloat32),
								float64(-math.MaxFloat64): float64(-math.MaxFloat64),
								float32(math.MaxFloat32):  float32(math.MaxFloat32),
								float64(math.MaxFloat64):  float64(math.MaxFloat64),
								"true":   true,
								"false":  false,
								"string": map[interface{}]interface{}{nil: "string", "string": 19}, // map to complex array
								nil:      []int{18, 41},                                            // array to complex map
							},
						})

						err = client.PutBins(wpolicy, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7, bin8, bin9, bin10)
						Expect(err).ToNot(HaveOccurred())

						rec, err = client.Get(rpolicy, key)
						Expect(err).ToNot(HaveOccurred())

						arraysEqual(rec.Bins[bin1.Name], bin1.Value.GetObject())
						arraysEqual(rec.Bins[bin2.Name], bin2.Value.GetObject())
						arraysEqual(rec.Bins[bin3.Name], bin3.Value.GetObject())
						arraysEqual(rec.Bins[bin4.Name], bin4.Value.GetObject())
						arraysEqual(rec.Bins[bin5.Name], bin5.Value.GetObject())
						arraysEqual(rec.Bins[bin6.Name], bin6.Value.GetObject())
						arraysEqual(rec.Bins[bin7.Name], bin7.Value.GetObject())
						arraysEqual(rec.Bins[bin8.Name], bin8.Value.GetObject())
						arraysEqual(rec.Bins[bin9.Name], bin9.Value.GetObject())
						arraysEqual(rec.Bins[bin10.Name], bin10.Value.GetObject())
					})

				}) // context list

				Context("Bins with MAP type", func() {

					It("must save a key with Array Types", func() {
						// complex type, consisting different maps
						bin1 := NewBin("Aerospike1", map[int32]string{
							0:                    "",
							int32(math.MaxInt32): randString(100),
							int32(math.MinInt32): randString(100),
						})

						bin2 := NewBin("Aerospike2", map[interface{}]interface{}{
							15:                        nil,
							"true":                    true,
							"false":                   false,
							int8(math.MaxInt8):        int8(math.MaxInt8),
							int64(math.MinInt64):      int64(math.MinInt64),
							int64(math.MaxInt64):      int64(math.MaxInt64),
							uint64(math.MaxUint64):    uint64(math.MaxUint64),
							float32(-math.MaxFloat32): float32(-math.MaxFloat32),
							float64(-math.MaxFloat64): float64(-math.MaxFloat64),
							float32(math.MaxFloat32):  float32(math.MaxFloat32),
							float64(math.MaxFloat64):  float64(math.MaxFloat64),
							"string":                  map[interface{}]interface{}{nil: "string", "string": 19}, // map to complex array
							nil:                       []int{18, 41},                                            // array to complex map
							"longString":              strings.Repeat("s", 32911),                               // bit-sign test
						})

						err = client.PutBins(wpolicy, key, bin1, bin2)
						Expect(err).ToNot(HaveOccurred())

						rec, err = client.Get(rpolicy, key)
						Expect(err).ToNot(HaveOccurred())

						mapsEqual(rec.Bins[bin1.Name], bin1.Value.GetObject())
						mapsEqual(rec.Bins[bin2.Name], bin2.Value.GetObject())
					})

				}) // context map

			}) // context complex types

		}) // put context

		Context("Append operations", func() {
			bin := NewBin("Aerospike", randString(rand.Intn(100)))

			BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				Expect(err).ToNot(HaveOccurred())
			})

			It("must append to a SINGLE bin", func() {
				appbin := NewBin(bin.Name, randString(rand.Intn(100)))
				err = client.AppendBins(wpolicy, key, appbin)
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject().(string) + appbin.Value.GetObject().(string)))
			})

			It("must append to a SINGLE bin using a BinMap", func() {
				appbin := NewBin(bin.Name, randString(rand.Intn(100)))
				err = client.Append(wpolicy, key, BinMap{bin.Name: appbin.Value})
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject().(string) + appbin.Value.GetObject().(string)))
			})

		}) // append context

		Context("Prepend operations", func() {
			bin := NewBin("Aerospike", randString(rand.Intn(100)))

			BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				Expect(err).ToNot(HaveOccurred())
			})

			It("must Prepend to a SINGLE bin", func() {
				appbin := NewBin(bin.Name, randString(rand.Intn(100)))
				err = client.PrependBins(wpolicy, key, appbin)
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Bins[bin.Name]).To(Equal(appbin.Value.GetObject().(string) + bin.Value.GetObject().(string)))
			})

			It("must Prepend to a SINGLE bin using a BinMap", func() {
				appbin := NewBin(bin.Name, randString(rand.Intn(100)))
				err = client.Prepend(wpolicy, key, BinMap{bin.Name: appbin.Value})
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Bins[bin.Name]).To(Equal(appbin.Value.GetObject().(string) + bin.Value.GetObject().(string)))
			})

		}) // prepend context

		Context("Add operations", func() {
			bin := NewBin("Aerospike", rand.Intn(math.MaxInt16))

			BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				Expect(err).ToNot(HaveOccurred())
			})

			It("must Add to a SINGLE bin", func() {
				addBin := NewBin(bin.Name, rand.Intn(math.MaxInt16))
				err = client.AddBins(wpolicy, key, addBin)
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Bins[bin.Name]).To(Equal(addBin.Value.GetObject().(int) + bin.Value.GetObject().(int)))
			})

			It("must Add to a SINGLE bin using a BinMap", func() {
				addBin := NewBin(bin.Name, rand.Intn(math.MaxInt16))
				err = client.Add(wpolicy, key, BinMap{addBin.Name: addBin.Value})
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Bins[bin.Name]).To(Equal(addBin.Value.GetObject().(int) + bin.Value.GetObject().(int)))
			})

		}) // add context

		Context("Delete operations", func() {
			bin := NewBin("Aerospike", rand.Intn(math.MaxInt16))

			BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				Expect(err).ToNot(HaveOccurred())
			})

			It("must Delete to a non-existing key", func() {
				var nxkey *Key
				nxkey, err = NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				var existed bool
				existed, err = client.Delete(wpolicy, nxkey)
				Expect(err).ToNot(HaveOccurred())
				Expect(existed).To(Equal(false))
			})

			It("must Delete to an existing key", func() {
				var existed bool
				existed, err = client.Delete(wpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(existed).To(Equal(true))

				existed, err = client.Exists(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(existed).To(Equal(false))
			})

		}) // Delete context

		Context("Touch operations", func() {
			bin := NewBin("Aerospike", rand.Intn(math.MaxInt16))

			BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				Expect(err).ToNot(HaveOccurred())
			})

			It("must Touch to a non-existing key", func() {
				var nxkey *Key
				nxkey, err = NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				err = client.Touch(wpolicy, nxkey)
				Expect(err).To(HaveOccurred())
			})

			It("must Touch to an existing key", func() {
				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				generation := rec.Generation

				wpolicy := NewWritePolicy(0, 0)
				wpolicy.SendKey = true
				err = client.Touch(wpolicy, key)
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Generation).To(Equal(generation + 1))

				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
				Expect(err).ToNot(HaveOccurred())

				// make sure the
				for r := range recordset.Results() {
					Expect(r.Err).ToNot(HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						Expect(r.Record.Key.Value()).To(Equal(key.Value()))
						Expect(r.Record.Bins).To(Equal(rec.Bins))
					}
				}
			})

		}) // Touch context

		Context("Exists operations", func() {
			bin := NewBin("Aerospike", rand.Intn(math.MaxInt16))

			BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				Expect(err).ToNot(HaveOccurred())
			})

			It("must check Existance of a non-existing key", func() {
				var nxkey *Key
				nxkey, err = NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				var exists bool
				exists, err = client.Exists(rpolicy, nxkey)
				Expect(err).ToNot(HaveOccurred())
				Expect(exists).To(Equal(false))
			})

			It("must checks Existance of an existing key", func() {
				var exists bool
				exists, err = client.Exists(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(exists).To(Equal(true))
			})

		}) // Exists context

		Context("Batch Exists operations", func() {
			bin := NewBin("Aerospike", rand.Intn(math.MaxInt16))
			const keyCount = 2048

			BeforeEach(func() {
			})

			It("must return the result with same ordering", func() {
				var exists []bool
				keys := []*Key{}

				for i := 0; i < keyCount; i++ {
					key, err := NewKey(ns, set, randString(50))
					Expect(err).ToNot(HaveOccurred())
					keys = append(keys, key)

					// if key shouldExist == true, put it in the DB
					if i%2 == 0 {
						err = client.PutBins(wpolicy, key, bin)
						Expect(err).ToNot(HaveOccurred())

						// make sure they exists in the DB
						exists, err := client.Exists(rpolicy, key)
						Expect(err).ToNot(HaveOccurred())
						Expect(exists).To(Equal(true))
					}
				}

				exists, err = client.BatchExists(rpolicy, keys)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(exists)).To(Equal(len(keys)))
				for idx, keyExists := range exists {
					Expect(keyExists).To(Equal(idx%2 == 0))
				}
			})

		}) // Batch Exists context

		Context("Batch Get operations", func() {
			bin := NewBin("Aerospike", rand.Int())
			const keyCount = 2048

			BeforeEach(func() {
			})

			It("must return the records with same ordering as keys", func() {
				binRedundant := NewBin("Redundant", "Redundant")

				var records []*Record
				type existance struct {
					key         *Key
					shouldExist bool // set randomly and checked against later
				}

				exList := make([]existance, 0, keyCount)
				keys := make([]*Key, 0, keyCount)

				for i := 0; i < keyCount; i++ {
					key, err := NewKey(ns, set, randString(50))
					Expect(err).ToNot(HaveOccurred())
					e := existance{key: key, shouldExist: rand.Intn(100) > 50}
					exList = append(exList, e)
					keys = append(keys, key)

					// if key shouldExist == true, put it in the DB
					if e.shouldExist {
						err = client.PutBins(wpolicy, key, bin, binRedundant)
						Expect(err).ToNot(HaveOccurred())

						// make sure they exists in the DB
						rec, err := client.Get(rpolicy, key)
						Expect(err).ToNot(HaveOccurred())
						Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))
						Expect(rec.Bins[binRedundant.Name]).To(Equal(binRedundant.Value.GetObject()))
					} else {
						// make sure they exists in the DB
						exists, err := client.Exists(rpolicy, key)
						Expect(err).ToNot(HaveOccurred())
						Expect(exists).To(Equal(false))
					}
				}

				records, err = client.BatchGet(rpolicy, keys)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(records)).To(Equal(len(keys)))
				for idx, rec := range records {
					if exList[idx].shouldExist {
						Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))
					} else {
						Expect(rec).To(BeNil())
					}
				}

				records, err = client.BatchGet(rpolicy, keys, bin.Name)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(records)).To(Equal(len(keys)))
				for idx, rec := range records {
					if exList[idx].shouldExist {
						// only bin1 has been requested
						Expect(rec.Bins[binRedundant.Name]).To(BeNil())
						Expect(rec.Bins[bin.Name]).To(Equal(bin.Value.GetObject()))
					} else {
						Expect(rec).To(BeNil())
					}
				}
			})

		}) // Batch Get context

		Context("GetHeader operations", func() {
			bin := NewBin("Aerospike", rand.Intn(math.MaxInt16))

			BeforeEach(func() {
				err = client.PutBins(wpolicy, key, bin)
				Expect(err).ToNot(HaveOccurred())
			})

			It("must Get the Header of an existing key after touch", func() {
				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				generation := rec.Generation

				err = client.Touch(wpolicy, key)
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.GetHeader(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Generation).To(Equal(generation + 1))
				Expect(rec.Bins[bin.Name]).To(BeNil())
			})

		}) // GetHeader context

		Context("Batch Get Header operations", func() {
			bin := NewBin("Aerospike", rand.Int())
			const keyCount = 1024

			BeforeEach(func() {
			})

			It("must return the records with same ordering as keys", func() {
				var records []*Record
				type existance struct {
					key         *Key
					shouldExist bool // set randomly and checked against later
				}

				exList := []existance{}
				keys := []*Key{}

				for i := 0; i < keyCount; i++ {
					key, err := NewKey(ns, set, randString(50))
					Expect(err).ToNot(HaveOccurred())
					e := existance{key: key, shouldExist: rand.Intn(100) > 50}
					exList = append(exList, e)
					keys = append(keys, key)

					// if key shouldExist == true, put it in the DB
					if e.shouldExist {
						err = client.PutBins(wpolicy, key, bin)
						Expect(err).ToNot(HaveOccurred())

						// update generation
						err = client.Touch(wpolicy, key)
						Expect(err).ToNot(HaveOccurred())

						// make sure they exists in the DB
						exists, err := client.Exists(rpolicy, key)
						Expect(err).ToNot(HaveOccurred())
						Expect(exists).To(Equal(true))
					}
				}

				records, err = client.BatchGetHeader(rpolicy, keys)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(records)).To(Equal(len(keys)))
				for idx, rec := range records {
					if exList[idx].shouldExist {
						Expect(rec.Bins[bin.Name]).To(BeNil())
						Expect(rec.Generation).To(Equal(uint32(2)))
					} else {
						Expect(rec).To(BeNil())
					}
				}
			})

		}) // Batch Get Header context

		Context("Operate operations", func() {
			bin1 := NewBin("Aerospike1", rand.Intn(math.MaxInt16))
			bin2 := NewBin("Aerospike2", randString(100))

			BeforeEach(func() {
				// err = client.PutBins(wpolicy, key, bin)
				// Expect(err).ToNot(HaveOccurred())
			})

			It("must send key on Put operations", func() {
				key, err := NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				ops1 := []*Operation{
					PutOp(bin1),
					PutOp(bin2),
					GetOp(),
				}

				wpolicy := NewWritePolicy(0, 0)
				wpolicy.SendKey = true
				rec, err = client.Operate(wpolicy, key, ops1...)
				Expect(err).ToNot(HaveOccurred())

				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
				Expect(err).ToNot(HaveOccurred())

				// make sure the result is what we put in
				for r := range recordset.Results() {
					Expect(r.Err).ToNot(HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						Expect(r.Record.Key.Value()).To(Equal(key.Value()))
						Expect(r.Record.Bins).To(Equal(rec.Bins))
					}
				}
			})

			It("must send key on Touch operations", func() {
				key, err := NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				ops1 := []*Operation{
					GetOp(),
					PutOp(bin2),
				}

				wpolicy := NewWritePolicy(0, 0)
				wpolicy.SendKey = false
				rec, err = client.Operate(wpolicy, key, ops1...)
				Expect(err).ToNot(HaveOccurred())

				recordset, err := client.ScanAll(nil, key.Namespace(), key.SetName())
				Expect(err).ToNot(HaveOccurred())

				// make sure the key is not saved
				for r := range recordset.Results() {
					Expect(r.Err).ToNot(HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						Expect(r.Record.Key.Value()).To(BeNil())
					}
				}

				ops2 := []*Operation{
					GetOp(),
					TouchOp(),
				}
				wpolicy.SendKey = true
				rec, err = client.Operate(wpolicy, key, ops2...)
				Expect(err).ToNot(HaveOccurred())

				recordset, err = client.ScanAll(nil, key.Namespace(), key.SetName())
				Expect(err).ToNot(HaveOccurred())

				// make sure the
				for r := range recordset.Results() {
					Expect(r.Err).ToNot(HaveOccurred())
					if bytes.Equal(key.Digest(), r.Record.Key.Digest()) {
						Expect(r.Record.Key.Value()).To(Equal(key.Value()))
						Expect(r.Record.Bins).To(Equal(rec.Bins))
					}
				}
			})

			It("must apply all operations, and result should match expectation", func() {
				key, err := NewKey(ns, set, randString(50))
				Expect(err).ToNot(HaveOccurred())

				ops1 := []*Operation{
					PutOp(bin1),
					PutOp(bin2),
					GetOp(),
				}

				rec, err = client.Operate(nil, key, ops1...)
				Expect(err).ToNot(HaveOccurred())

				Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject().(int)))
				Expect(rec.Bins[bin2.Name]).To(Equal(bin2.Value.GetObject().(string)))
				Expect(rec.Generation).To(Equal(uint32(1)))

				ops2 := []*Operation{
					AddOp(bin1),    // double the value of the bin
					AppendOp(bin2), // with itself
					GetOp(),
				}

				rec, err = client.Operate(nil, key, ops2...)
				Expect(err).ToNot(HaveOccurred())

				Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject().(int) * 2))
				Expect(rec.Bins[bin2.Name]).To(Equal(strings.Repeat(bin2.Value.GetObject().(string), 2)))
				Expect(rec.Generation).To(Equal(uint32(2)))

				ops3 := []*Operation{
					AddOp(bin1),
					PrependOp(bin2),
					TouchOp(),
					GetOp(),
				}

				rec, err = client.Operate(nil, key, ops3...)
				Expect(err).ToNot(HaveOccurred())

				Expect(rec.Bins[bin1.Name]).To(Equal(bin1.Value.GetObject().(int) * 3))
				Expect(rec.Bins[bin2.Name]).To(Equal(strings.Repeat(bin2.Value.GetObject().(string), 3)))
				Expect(rec.Generation).To(Equal(uint32(3)))

				ops4 := []*Operation{
					TouchOp(),
					GetHeaderOp(),
				}

				rec, err = client.Operate(nil, key, ops4...)
				Expect(err).ToNot(HaveOccurred())

				Expect(rec.Generation).To(Equal(uint32(4)))
				Expect(len(rec.Bins)).To(Equal(0))

				// GetOp should override GetHEaderOp
				ops5 := []*Operation{
					GetOp(),
					GetHeaderOp(),
				}

				rec, err = client.Operate(nil, key, ops5...)
				Expect(err).ToNot(HaveOccurred())

				Expect(rec.Generation).To(Equal(uint32(4)))
				Expect(len(rec.Bins)).To(Equal(2))
			})

		}) // GetHeader context

	})
})
