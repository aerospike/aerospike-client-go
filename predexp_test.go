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
	"time"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/internal/atomic"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("predexp operations", func() {

	const keyCount = 1000

	var ns = *namespace
	var set = "predexp_tests" // The name of the set should be consistent because of predexp_modulo tests, since set name is a part of the digest
	var wpolicy = as.NewWritePolicy(0, 0)

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

	var gaptime int64

	insertRecs := atomic.NewBool(true)

	gg.BeforeEach(func() {
		if !insertRecs.Get() {
			return
		}

		client.DropIndex(nil, ns, set, "intval")
		client.DropIndex(nil, ns, set, "strval")

		wpolicy = as.NewWritePolicy(0, 24*60*60)

		for ii := 0; ii < keyCount; ii++ {

			// On iteration 333 we pause for a few mSec and note the
			// time.  Later we can check last_update time for either
			// side of this gap ...
			//
			// Also, we update the WritePolicy to never expire so
			// records w/ 0 TTL can be counted later.
			//
			if ii == 333 {
				time.Sleep(500 * time.Millisecond)
				gaptime = time.Now().UnixNano()
				time.Sleep(500 * time.Millisecond)

				wpolicy = as.NewWritePolicy(0, as.TTLDontExpire)
			}

			key, err := as.NewKey(ns, set, ii)
			gm.Expect(err).ToNot(gm.HaveOccurred())

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
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		idxTask, err := client.CreateIndex(wpolicy, ns, set, "intval", "intval", as.NUMERIC)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

		idxTask, err = client.CreateIndex(wpolicy, ns, set, "strval", "strval", as.STRING)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

		insertRecs.Set(false)
	})

	// gg.AfterEach(func() {
	// 	gm.Expect(client.DropIndex(nil, ns, set, "intval")).ToNot(gm.HaveOccurred())
	// 	gm.Expect(client.DropIndex(nil, ns, set, "strval")).ToNot(gm.HaveOccurred())
	// })

	gg.It("server error with top level predexp value node", func() {

		// This statement doesn't form a predicate expression.
		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("intval", 0, 400))
		stm.SetPredExp(as.NewPredExpIntegerValue(8))
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		for res := range recordset.Results() {
			gm.Expect(res.Err).To(gm.HaveOccurred())
		}
	})

	gg.It("server error with multiple top-level predexp", func() {

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("intval", 0, 400))
		stm.SetPredExp(
			as.NewPredExpIntegerBin("modval"),
			as.NewPredExpIntegerValue(8),
			as.NewPredExpIntegerGreaterEq(),
			as.NewPredExpIntegerBin("modval"),
			as.NewPredExpIntegerValue(8),
			as.NewPredExpIntegerGreaterEq(),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		for res := range recordset.Results() {
			gm.Expect(res.Err).To(gm.HaveOccurred())
		}
	})

	gg.It("server error with missing child predexp", func() {

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("intval", 0, 400))
		stm.SetPredExp(
			as.NewPredExpIntegerValue(8),
			as.NewPredExpIntegerGreaterEq(),
		) // needs two children!
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		for res := range recordset.Results() {
			gm.Expect(res.Err).To(gm.HaveOccurred())
		}
	})

	gg.It("predexp must additionally filter indexed query results", func() {

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewRangeFilter("intval", 0, 400))
		stm.SetPredExp(
			as.NewPredExpIntegerBin("modval"),
			as.NewPredExpIntegerValue(8),
			as.NewPredExpIntegerGreaterEq(),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// The query clause selects [0, 1, ... 400, 401] The predexp
		// only takes mod 8 and 9, should be 2 pre decade or 80 total.

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		gm.Expect(cnt).To(gm.BeNumerically("==", 80))
	})

	gg.It("predexp must work with implied scan", func() {

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpStringValue("0x0001"),
			as.NewPredExpStringBin("strval"),
			as.NewPredExpStringEqual(),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		gm.Expect(cnt).To(gm.BeNumerically("==", 1))
	})

	gg.It("predexp and or and not must all work", func() {

		stm := as.NewStatement(ns, set)

		// This returns 999
		stm.SetPredExp(
			as.NewPredExpStringValue("0x0001"),
			as.NewPredExpStringBin("strval"),
			as.NewPredExpStringEqual(),
			as.NewPredExpNot(),

			// This is two per decade
			as.NewPredExpIntegerBin("modval"),
			as.NewPredExpIntegerValue(8),
			as.NewPredExpIntegerGreaterEq(),

			// Should be 200
			as.NewPredExpAnd(2),

			// Should exactly match 3 values not in prior set
			as.NewPredExpStringValue("0x0104"),
			as.NewPredExpStringBin("strval"),
			as.NewPredExpStringEqual(),
			as.NewPredExpStringValue("0x0105"),
			as.NewPredExpStringBin("strval"),
			as.NewPredExpStringEqual(),
			as.NewPredExpStringValue("0x0106"),
			as.NewPredExpStringBin("strval"),
			as.NewPredExpStringEqual(),

			// 200 + 3
			as.NewPredExpOr(4),
		)

		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		gm.Expect(cnt).To(gm.BeNumerically("==", 203))
	})

	gg.It("predexp regex match must work", func() {

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpStringBin("strval"),
			as.NewPredExpStringValue("0x00.[12]"),
			as.NewPredExpStringRegex(0),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Should be 32 results:
		// 0x0001, 0x0002,
		// 0x0011, 0x0012,
		// ...
		// 0x00f1, 0x00f2,

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		gm.Expect(cnt).To(gm.BeNumerically("==", 32))
	})

	gg.It("predexp geo PIR query must work", func() {

		region :=
			"{ " +
				"    \"type\": \"Polygon\", " +
				"    \"coordinates\": [ " +
				"        [[-122.500000, 37.000000],[-121.000000, 37.000000], " +
				"         [-121.000000, 38.080000],[-122.500000, 38.080000], " +
				"         [-122.500000, 37.000000]] " +
				"    ] " +
				"}"

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpGeoJSONBin("locval"),
			as.NewPredExpGeoJSONValue(region),
			as.NewPredExpGeoJSONWithin(),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Correct answer is 59.
		gm.Expect(cnt).To(gm.BeNumerically("==", 59))
	})

	gg.It("predexp geo RCP query must work", func() {

		point :=
			"{ " +
				"    \"type\": \"Point\", " +
				"    \"coordinates\": [ -122.0986857, 37.4214209 ] " +
				"}"

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpGeoJSONBin("rgnval"),
			as.NewPredExpGeoJSONValue(point),
			as.NewPredExpGeoJSONContains(),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Correct answer is 6.  See:
		// aerospike-client-c/src/test/aerospike_geo/query_geospatial.c:
		// predexp_points_within_region

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Correct answer is 5.  See:
		// aerospike-client-c/src/test/aerospike_geo/query_geospatial.c:
		// predexp_regions_containing_point

		gm.Expect(cnt).To(gm.BeNumerically("==", 5))
	})

	gg.It("predexp last_update must work", func() {

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(as.NewPredExpRecLastUpdate(),
			as.NewPredExpIntegerValue(gaptime),
			as.NewPredExpIntegerGreater(),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// The answer should be 1000 - 333 = 667

		gm.Expect(cnt).To(gm.BeNumerically("==", 667))
	})

	gg.It("predexp void_time must work", func() {

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpIntegerValue(0),
			as.NewPredExpRecVoidTime(),
			as.NewPredExpIntegerEqual(),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// The answer should be 1000 - 333 = 667

		gm.Expect(cnt).To(gm.BeNumerically("==", 667))
	})

	gg.It("predexp rec_size work", func() {

		if len(nsInfo(ns, "device_total_bytes")) == 0 {
			gg.Skip("gg.Skipping Predexp rec_size test since the namespace is not persisted.")
		}

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpRecDeviceSize(),
			as.NewPredExpIntegerValue(12*1024),
			as.NewPredExpIntegerGreaterEq(),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Answer should roughly be 1000 - (12/16 * 1000) ~= 250 + ovhd

		gm.Expect(cnt).To(gm.BeNumerically(">", 250))
		gm.Expect(cnt).To(gm.BeNumerically("<", 300))
	})

	gg.It("predexp digest_modulo must work", func() {

		cnt := []int{0, 0, 0}
		for _, ndx := range []int64{0, 1, 2} {
			stm := as.NewStatement(ns, set)
			stm.SetPredExp(
				as.NewPredExpRecDigestModulo(3),
				as.NewPredExpIntegerValue(ndx),
				as.NewPredExpIntegerEqual(),
			)
			recordset, err := client.Query(nil, stm)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			for res := range recordset.Results() {
				gm.Expect(res.Err).ToNot(gm.HaveOccurred())
				cnt[ndx]++
			}
		}

		// The count should be split 3 ways, roughly equally.
		sum := 0
		gm.Expect(cnt).To(gm.Equal([]int{308, 374, 318}))
		for _, cc := range cnt {
			sum += cc
		}
		gm.Expect(sum).To(gm.BeNumerically("==", 1000))
	})

	gg.It("predexp list_iter_or work", func() {

		// Select all records w/ list contains a 17.

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpIntegerValue(17),
			as.NewPredExpIntegerVar("ff"),
			as.NewPredExpIntegerEqual(),
			as.NewPredExpListBin("lstval"),
			as.NewPredExpListIterateOr("ff"),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Answer should be floor(1000 / 17) = 58

		gm.Expect(cnt).To(gm.BeNumerically("==", 58))
	})

	gg.It("predexp list_iter_and work", func() {

		// Select all records w/ list doesn't have a 3.

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpIntegerValue(3),
			as.NewPredExpIntegerVar("ff"),
			as.NewPredExpIntegerEqual(),
			as.NewPredExpNot(),
			as.NewPredExpListBin("lstval"),
			as.NewPredExpListIterateAnd("ff"),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Answer should be 1000 - (ceil(1000 / 3) - 1) = 667

		gm.Expect(cnt).To(gm.BeNumerically("==", 667))
	})

	gg.It("predexp mapkey_iter_or work", func() {

		// Select all records w/ mapkey containing 19.

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpIntegerValue(19),
			as.NewPredExpIntegerVar("kk"),
			as.NewPredExpIntegerEqual(),
			as.NewPredExpMapBin("mapval"),
			as.NewPredExpMapKeyIterateOr("kk"),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Answer should be floor(1000 / 19) = 52

		gm.Expect(cnt).To(gm.BeNumerically("==", 52))
	})

	gg.It("predexp mapkey_iter_and work", func() {

		// Select all records w/ no mapkey containing 5.

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpIntegerValue(5),
			as.NewPredExpIntegerVar("kk"),
			as.NewPredExpIntegerEqual(),
			as.NewPredExpNot(),
			as.NewPredExpMapBin("mapval"),
			as.NewPredExpMapKeyIterateAnd("kk"),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Answer should be 1000 - (ceil(1000 / 5) - 1) = 801

		gm.Expect(cnt).To(gm.BeNumerically("==", 801))
	})

	gg.It("predexp mapval_iter_or work", func() {

		// Select all records w/ mapval of 19 ("0x0013")

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpStringValue("0x0013"),
			as.NewPredExpStringVar("vv"),
			as.NewPredExpStringEqual(),
			as.NewPredExpMapBin("mapval"),
			as.NewPredExpMapValIterateOr("vv"),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Answer should be floor(1000 / 19) = 52

		gm.Expect(cnt).To(gm.BeNumerically("==", 52))
	})

	gg.It("predexp mapval_iter_and work", func() {

		// Select all records w/ no mapval of 5 ("0x0005").

		stm := as.NewStatement(ns, set)
		stm.SetPredExp(
			as.NewPredExpStringValue("0x0005"),
			as.NewPredExpStringVar("vv"),
			as.NewPredExpStringEqual(),
			as.NewPredExpNot(),
			as.NewPredExpMapBin("mapval"),
			as.NewPredExpMapValIterateAnd("vv"),
		)
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cnt := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			cnt++
		}

		// Answer should be 1000 - (ceil(1000 / 5) - 1) = 801

		gm.Expect(cnt).To(gm.BeNumerically("==", 801))
	})

})
