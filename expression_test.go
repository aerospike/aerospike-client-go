// Copyright 2017-2019 Aerospike, Inc.
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

	as "github.com/aerospike/aerospike-client-go"
	atomic "github.com/aerospike/aerospike-client-go/internal/atomic"
	ParticleType "github.com/aerospike/aerospike-client-go/internal/particle_type"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = Describe("Expression Operations", func() {

	var ns = *namespace
	var set = randString(50)
	// var rpolicy = as.NewPolicy()
	var wpolicy = as.NewWritePolicy(0, 0)
	var qpolicy = as.NewQueryPolicy()

	var _ = Context("Generic", func() {

		var set = "expression_tests" // The name of the set should be consistent because of predexp_modulo tests, since set name is a part of the digest

		const keyCount = 1000

		insertRecs := atomic.NewAtomicBool(true)

		BeforeEach(func() {
			if !insertRecs.Get() {
				return
			}

			client.DropIndex(nil, ns, set, "intval")
			client.DropIndex(nil, ns, set, "strval")

			wpolicy = as.NewWritePolicy(0, 24*60*60)

			starbucks := [][2]float64{
				{-122.1708441, 37.4241193},
				{-122.1492040, 37.4273569},
				{-122.1441078, 37.4268202},
				{-122.1251714, 37.4130590},
				{-122.0964289, 37.4218102},
				{-122.0776641, 37.4158199},
				{-122.0943475, 37.4114654},
				{-122.1122861, 37.4028493},
				{-122.0947230, 37.3909250},
				{-122.0831037, 37.3876090},
				{-122.0707119, 37.3787855},
				{-122.0303178, 37.3882739},
				{-122.0464861, 37.3786236},
				{-122.0582128, 37.3726980},
				{-122.0365083, 37.3676930},
			}

			for ii := 0; ii < keyCount; ii++ {

				// On iteration 333 we pause for a few mSec and note the
				// time.  Later we can check last_update time for either
				// side of this gap ...
				//
				// Also, we update the WritePolicy to never expire so
				// records w/ 0 TTL can be counted later.
				//

				key, err := as.NewKey(ns, set, ii)
				Expect(err).ToNot(HaveOccurred())

				lng := -122.0 + (0.01 * float64(ii))
				lat := 37.5 + (0.01 * float64(ii))
				pointstr := fmt.Sprintf(
					"{ \"type\": \"Point\", \"coordinates\": [%f, %f] }",
					lng, lat)

				var regionstr string
				if ii < len(starbucks) {
					regionstr = fmt.Sprintf(
						"{ \"type\": \"AeroCircle\", "+
							"  \"coordinates\": [[%f, %f], 3000.0 ] }",
						starbucks[ii][0], starbucks[ii][1])
				} else {
					// Somewhere off Africa ...
					regionstr =
						"{ \"type\": \"AeroCircle\", " +
							"  \"coordinates\": [[0.0, 0.0], 3000.0 ] }"
				}

				// Accumulate prime factors of the index into a list and map.
				listval := []int{}
				mapval := map[int]string{}
				for _, ff := range []int{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31} {
					if ii >= ff && ii%ff == 0 {
						listval = append(listval, ff)
						mapval[ff] = fmt.Sprintf("0x%04x", ff)
					}
				}

				ballast := make([]byte, ii*16)

				bins := as.BinMap{
					"intval":  ii,
					"strval":  fmt.Sprintf("0x%04x", ii),
					"modval":  ii % 10,
					"locval":  as.NewGeoJSONValue(pointstr),
					"rgnval":  as.NewGeoJSONValue(regionstr),
					"lstval":  listval,
					"mapval":  mapval,
					"ballast": ballast,
				}
				err = client.Put(wpolicy, key, bins)
				Expect(err).ToNot(HaveOccurred())
			}

			idxTask, err := client.CreateIndex(wpolicy, ns, set, "intval", "intval", as.NUMERIC)
			Expect(err).ToNot(HaveOccurred())
			Expect(<-idxTask.OnComplete()).ToNot(HaveOccurred())

			idxTask, err = client.CreateIndex(wpolicy, ns, set, "strval", "strval", as.STRING)
			Expect(err).ToNot(HaveOccurred())
			Expect(<-idxTask.OnComplete()).ToNot(HaveOccurred())

			insertRecs.Set(false)
		})

		// AfterEach(func() {
		// 	Expect(client.DropIndex(nil, ns, set, "intval")).ToNot(HaveOccurred())
		// 	Expect(client.DropIndex(nil, ns, set, "strval")).ToNot(HaveOccurred())
		// })

		It("server error with top level expression value node", func() {
			// This statement doesn't form a predicate expression.
			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter("intval", 0, 400))

			qpolicy.FilterExpression = as.ExpIntVal(8)
			recordset, err := client.Query(qpolicy, stm)
			Expect(err).ToNot(HaveOccurred())
			for res := range recordset.Results() {
				Expect(res.Err).To(HaveOccurred())
			}
		})

		It("expression filters should be prioritized over predexp", func() {
			// This statement doesn't form a predicate expression.
			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter("intval", 0, 400))

			stm.SetPredExp(as.NewPredExpIntegerValue(8))
			qpolicy.FilterExpression = as.ExpGreaterEq(as.ExpIntBin("modval"), as.ExpIntVal(8))

			recordset, err := client.Query(qpolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			// The query clause selects [0, 1, ... 400, 401] The predexp
			// only takes mod 8 and 9, should be 2 pre decade or 80 total.

			cnt := 0
			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				cnt++
			}

			Expect(cnt).To(BeNumerically("==", 80))
		})

		It("expression must additionally filter indexed query results", func() {

			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewRangeFilter("intval", 0, 400))
			qpolicy.FilterExpression = as.ExpGreaterEq(as.ExpIntBin("modval"), as.ExpIntVal(8))
			recordset, err := client.Query(qpolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			// The query clause selects [0, 1, ... 400, 401] The predexp
			// only takes mod 8 and 9, should be 2 pre decade or 80 total.

			cnt := 0
			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				cnt++
			}

			Expect(cnt).To(BeNumerically("==", 80))
		})

		It("expression must work with implied scan", func() {

			stm := as.NewStatement(ns, set)
			qpolicy.FilterExpression = as.ExpEq(as.ExpStringBin("strval"), as.ExpStringVal("0x0001"))
			recordset, err := client.Query(qpolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			cnt := 0
			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				cnt++
			}

			Expect(cnt).To(BeNumerically("==", 1))
		})

		It("expression and or and not must all work", func() {

			stm := as.NewStatement(ns, set)
			qpolicy.FilterExpression = as.ExpOr(
				as.ExpAnd(
					as.ExpNot(as.ExpEq(as.ExpStringBin("strval"), as.ExpStringVal("0x0001"))),
					as.ExpGreaterEq(as.ExpIntBin("modval"), as.ExpIntVal(8)),
				),
				as.ExpEq(as.ExpStringBin("strval"), as.ExpStringVal("0x0104")),
				as.ExpEq(as.ExpStringBin("strval"), as.ExpStringVal("0x0105")),
				as.ExpEq(as.ExpStringBin("strval"), as.ExpStringVal("0x0106")),
			)

			recordset, err := client.Query(qpolicy, stm)
			Expect(err).ToNot(HaveOccurred())

			cnt := 0
			for res := range recordset.Results() {
				Expect(res.Err).ToNot(HaveOccurred())
				cnt++
			}

			Expect(cnt).To(BeNumerically("==", 203))
		})
	})

	runQuery := func(filter *as.FilterExpression, set_name string) *as.Recordset {
		qpolicy.FilterExpression = filter
		stmt := as.NewStatement(ns, set_name)
		rs, err := client.Query(qpolicy, stmt)
		Expect(err).NotTo(HaveOccurred())

		return rs
	}

	countResults := func(rs *as.Recordset) int {
		count := 0

		for res := range rs.Results() {
			Expect(res.Err).ToNot(HaveOccurred())
			count += 1
		}

		return count
	}

	var _ = Describe("Expressions", func() {
		const keyCount = 100
		set = randString(50)

		insertRecs := atomic.NewAtomicBool(true)

		// wpolicy.Expiration = as.TTLDontExpire

		BeforeEach(func() {
			if !insertRecs.Get() {
				return
			}

			for ii := 0; ii < keyCount; ii++ {
				key, _ := as.NewKey(ns, set, ii)
				ibin := as.BinMap{
					"bin":  ii,
					"bin2": fmt.Sprintf("%d", ii),
					"bin3": float64(ii) / 3,
					"bin4": []byte(fmt.Sprintf("blob%d", ii)),
					"bin5": []interface{}{"a", "b", ii},
					"bin6": map[string]interface{}{"a": "test", "b": ii},
				}
				client.Delete(wpolicy, key)
				client.Put(wpolicy, key, ibin)

			}

			insertRecs.Set(false)
		})

		var _ = Context("Data Types", func() {

			It("ExpIntBin must work", func() {
				// INT
				rs := runQuery(
					as.ExpEq(
						as.ExpIntBin("bin"),
						as.ExpIntVal(1),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(1))

			})

			It("ExpStringBin must work", func() {
				// STRING
				rs := runQuery(
					as.ExpEq(
						as.ExpStringBin("bin2"),
						as.ExpStringVal("1"),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(1))

			})

			It("ExpFloatBin must work", func() {
				rs := runQuery(
					as.ExpEq(
						as.ExpFloatBin("bin3"),
						as.ExpFloatVal(2),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(1))

			})

			It("ExpBlobBin must work", func() {
				rs := runQuery(
					as.ExpEq(
						as.ExpBlobBin("bin4"),
						as.ExpBlobVal([]byte("blob5")),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(1))

			})

			It("ExpBinType must work", func() {
				rs := runQuery(
					as.ExpNotEq(
						as.ExpBinType("bin"),
						as.ExpIntVal(ParticleType.NULL),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})
		})

		var _ = Context("Logical Ops", func() {
			// AND
			It("ExpAnd must work", func() {
				rs := runQuery(
					as.ExpAnd(
						as.ExpEq(
							as.ExpIntBin("bin"),
							as.ExpIntVal(1),
						),
						as.ExpEq(
							as.ExpStringBin("bin2"),
							as.ExpStringVal("1"),
						),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(1))
			})
			// OR
			It("ExpOr must work", func() {
				rs := runQuery(
					as.ExpOr(
						as.ExpEq(
							as.ExpIntBin("bin"),
							as.ExpIntVal(1),
						),
						as.ExpEq(
							as.ExpIntBin("bin"),
							as.ExpIntVal(3),
						),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(2))
			})
			// NOT
			It("ExpNot must work", func() {
				rs := runQuery(
					as.ExpNot(as.ExpEq(
						as.ExpIntBin("bin"),
						as.ExpIntVal(1),
					)),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(99))
			})

		})

		var _ = Context("Comparisons", func() {

			It("ExpEq must work", func() {
				// EQ
				rs := runQuery(
					as.ExpEq(
						as.ExpIntBin("bin"),
						as.ExpIntVal(1),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(1))
			})

			It("ExpNotEq must work", func() {
				// NE
				rs := runQuery(
					as.ExpNotEq(
						as.ExpIntBin("bin"),
						as.ExpIntVal(1),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(99))
			})

			It("ExpLess must work", func() {
				// LT
				rs := runQuery(
					as.ExpLess(
						as.ExpIntBin("bin"),
						as.ExpIntVal(100),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			It("ExpLessEq must work", func() {
				// LE
				rs := runQuery(
					as.ExpLessEq(
						as.ExpIntBin("bin"),
						as.ExpIntVal(100),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			It("ExpGreater must work", func() {
				// GT
				rs := runQuery(
					as.ExpGreater(
						as.ExpIntBin("bin"),
						as.ExpIntVal(1),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(98))
			})

			It("ExpGreaterEq must work", func() {
				// GE
				rs := runQuery(
					as.ExpGreaterEq(
						as.ExpIntBin("bin"),
						as.ExpIntVal(1),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(99))
			})

		}) // Context

		var _ = Context("Record Ops", func() {

			It("ExpDeviceSize must work", func() {
				// dev size 0 because in-memory
				rs := runQuery(
					as.ExpLessEq(as.ExpDeviceSize(), as.ExpIntVal(0)),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			It("ExpLastUpdate must work", func() {
				rs := runQuery(
					as.ExpGreater(as.ExpLastUpdate(), as.ExpIntVal(15000)),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			It("ExpSinceUpdate must work", func() {
				rs := runQuery(
					as.ExpGreater(as.ExpSinceUpdate(), as.ExpIntVal(150)),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			// It("ExpVoidTime must work", func() {
			// 	// Records dont expire
			// 	rs := runQuery(
			// 		as.ExpLessEq(as.ExpVoidTime(), as.ExpIntVal(0)),
			// 		set,
			// 	)
			// 	count := countResults(rs)
			// 	Expect(count).To(Equal(100))
			// })

			// It("ExpTTL must work", func() {
			// 	rs := runQuery(
			// 		as.ExpLessEq(as.ExpTTL(), as.ExpIntVal(0)),
			// 		set,
			// 	)
			// 	count := countResults(rs)
			// 	Expect(count).To(Equal(100))
			// })

			It("ExpIsTombstone must work", func() {
				rs := runQuery(
					as.ExpNot(as.ExpIsTombstone()),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			It("ExpSetName must work", func() {
				rs := runQuery(
					as.ExpEq(
						as.ExpSetName(),
						as.ExpStringVal(set),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			It("ExpBinExists must work", func() {
				rs := runQuery(as.ExpBinExists("bin4"), set)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			It("ExpDigestModulo must work", func() {
				rs := runQuery(
					as.ExpEq(as.ExpDigestModulo(3), as.ExpIntVal(1)),
					set,
				)
				count := countResults(rs)
				Expect(count > 0 && count < 100).To(BeTrue())
			})

			It("ExpKey must work", func() {
				rs := runQuery(
					as.ExpEq(as.ExpKey(as.ExpTypeINT), as.ExpIntVal(50)),
					set,
				)
				count := countResults(rs)
				// 0 because key is not saved
				Expect(count).To(Equal(0))
			})

			It("ExpKeyExists must work", func() {
				rs := runQuery(as.ExpKeyExists(), set)
				count := countResults(rs)
				// 0 because key is not saved
				Expect(count).To(Equal(0))
			})

			It("ExpEq Nil test must work", func() {
				rs := runQuery(
					as.ExpEq(as.ExpNilValue(), as.ExpNilValue()),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(100))
			})

			It("ExpRegexCompare must work", func() {
				rs := runQuery(
					as.ExpRegexCompare(
						"[1-5]",
						as.ExpRegexFlagICASE,
						as.ExpStringBin("bin2"),
					),
					set,
				)
				count := countResults(rs)
				Expect(count).To(Equal(75))
			})
		})

		var _ = Context("Commands", func() {

			rpolicy := as.NewPolicy()
			wpolicy := as.NewWritePolicy(0, 0)
			spolicy := as.NewScanPolicy()
			bpolicy := as.NewBatchPolicy()

			BeforeEach(func() {
				for i := 0; i < keyCount; i++ {
					key, _ := as.NewKey(ns, set, i)
					ibin := as.BinMap{"bin": i}

					client.Delete(wpolicy, key)
					client.Put(nil, key, ibin)
				}
			})

			It("Delete must work", func() {
				// DELETE
				key, _ := as.NewKey(ns, set, 15)
				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(16),
				)
				_, err := client.Delete(wpolicy, key)
				Expect(err).To(HaveOccurred())

				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(15),
				)
				_, err = client.Delete(wpolicy, key)
				Expect(err).ToNot(HaveOccurred())

			})

			It("Put must work", func() {
				// PUT
				key, _ := as.NewKey(ns, set, 25)
				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(15),
				)
				err := client.PutBins(wpolicy, key, as.NewBin("bin", 26))
				Expect(err).To(HaveOccurred())

				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(25),
				)
				err = client.PutBins(wpolicy, key, as.NewBin("bin", 26))
				Expect(err).ToNot(HaveOccurred())

			})

			It("Get must work", func() {
				// GET
				key, _ := as.NewKey(ns, set, 35)
				rpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(15),
				)
				_, err := client.Get(rpolicy, key)
				Expect(err).To(HaveOccurred())

				rpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(35),
				)
				_, err = client.Get(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())

			})

			It("Exists must work", func() {
				// EXISTS
				key, _ := as.NewKey(ns, set, 45)
				rpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(15),
				)
				_, err := client.Exists(rpolicy, key)
				Expect(err).To(HaveOccurred())

				rpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(45),
				)
				_, err = client.Exists(rpolicy, key)
				Expect(err).ToNot(HaveOccurred())

			})

			It("Add must work", func() {
				// APPEND
				key, _ := as.NewKey(ns, set, 55)
				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(15),
				)
				err := client.AddBins(wpolicy, key, as.NewBin("test55", "test"))
				Expect(err).To(HaveOccurred())

				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(55),
				)
				err = client.AddBins(wpolicy, key, as.NewBin("test55", "test"))
				Expect(err).ToNot(HaveOccurred())

			})

			It("Prepend must work", func() {
				// PREPEND
				key, _ := as.NewKey(ns, set, 55)
				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(15),
				)
				err := client.PrependBins(wpolicy, key, as.NewBin("test55", "test"))
				Expect(err).To(HaveOccurred())

				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(55),
				)
				err = client.PrependBins(wpolicy, key, as.NewBin("test55", "test"))
				Expect(err).ToNot(HaveOccurred())

			})

			It("Touch must work", func() {
				// TOUCH
				key, _ := as.NewKey(ns, set, 65)
				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(15),
				)
				err := client.Touch(wpolicy, key)
				Expect(err).To(HaveOccurred())

				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(65),
				)
				err = client.Touch(wpolicy, key)
				Expect(err).ToNot(HaveOccurred())
			})

			It("Scan must work", func() {
				// SCAN
				spolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(75),
				)

				rs, err := client.ScanAll(spolicy, ns, set)
				Expect(err).ToNot(HaveOccurred())

				count := 0
				for res := range rs.Results() {
					Expect(res.Err).ToNot(HaveOccurred())
					count += 1
				}
				Expect(count).To(Equal(1))
			})

			It("Operate must work", func() {
				// OPERATE
				bin := as.NewBin("test85", 85)

				key, _ := as.NewKey(ns, set, 85)
				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(15),
				)
				_, err := client.Operate(wpolicy, key, as.AddOp(bin))
				Expect(err).To(HaveOccurred())

				key, _ = as.NewKey(ns, set, 85)
				wpolicy.FilterExpression = as.ExpEq(
					as.ExpIntBin("bin"),
					as.ExpIntVal(85),
				)
				_, err = client.Operate(wpolicy, key, as.AddOp(bin))
				Expect(err).ToNot(HaveOccurred())
			})

			It("Batch must work", func() {
				// BATCH GET
				keys := []*as.Key{}
				for i := 85; i < 90; i++ {
					key, _ := as.NewKey(ns, set, i)
					keys = append(keys, key)
				}
				bpolicy.FilterExpression = as.ExpGreater(
					as.ExpIntBin("bin"),
					as.ExpIntVal(88),
				)
				results, err := client.BatchGet(bpolicy, keys)
				// all keys other than one are filtered out, so error is returned
				Expect(err).To(HaveOccurred())

				count := 0
				for _, result := range results {
					if result != nil {
						count++
					}
				}
				Expect(count).To(Equal(1))
			})
		})

	}) // Describe

}) // Describe
