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
	"fmt"
	"math"
	"math/rand"
	"time"

	. "github.com/aerospike/aerospike-client-go"
	. "github.com/aerospike/aerospike-client-go/logger"

	. "github.com/aerospike/aerospike-client-go/utils/buffer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func init() {
	fmt.Println("Testing")
	rand.Seed(time.Now().UnixNano())
	Logger.SetLevel(ERR)
}

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Aerospike", func() {

	Describe("Data operations on native types", func() {
		// connection data
		var client *Client
		var err error
		var ns = "test"
		var set = randString(50)
		var key *Key
		var wpolicy = NewWritePolicy(0, 0)
		var rpolicy = NewPolicy()
		var rec *Record

		BeforeEach(func() {
			client, err = NewClient("127.0.0.1", 3000)
			Expect(err).ToNot(HaveOccurred())
			key, err = NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())
		})

		Context("Put operations", func() {
			// TODO: Resolve this
			// It("must save a key without bins?!", func() {
			// 	// bin := NewBin("dbname", "Aerospike")
			// 	err = client.PutBins(wpolicy, key)
			// 	Expect(err).ToNot(HaveOccurred())

			// 	var exists bool
			// 	exists, err := client.Exists(rpolicy, key)
			// 	rec, _ := client.Get(rpolicy, key)
			// 	fmt.Printf("%#v: %#v", key, rec)
			// 	Expect(err).ToNot(HaveOccurred())
			// 	Expect(exists).To(Equal(true))
			// })

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

			Context("Bins with `int64` only values (uint64 will be supported through big.Int) ", func() {
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
						bin9 := NewBin("Aerospike9", []interface{}{"", 1, nil})

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
								15:                   nil,
								int8(math.MaxInt8):   int8(math.MaxInt8),
								int64(math.MinInt64): int64(math.MinInt64),
								int64(math.MaxInt64): int64(math.MaxInt64),
								"string":             map[interface{}]interface{}{nil: "string", "string": 19}, // map to complex array
								nil:                  []int{18, 41},                                            // array to complex map
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
							15:                   nil,
							int8(math.MaxInt8):   int8(math.MaxInt8),
							int64(math.MinInt64): int64(math.MinInt64),
							int64(math.MaxInt64): int64(math.MaxInt64),
							"string":             map[interface{}]interface{}{nil: "string", "string": 19}, // map to complex array
							nil:                  []int{18, 41},                                            // array to complex map
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

				err = client.Touch(wpolicy, key)
				Expect(err).ToNot(HaveOccurred())

				rec, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())
				Expect(rec.Generation).To(Equal(generation + 1))
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

	})
})
