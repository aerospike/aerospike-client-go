// Copyright 2014-2021 Aerospike, Inc.
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

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Geo Spacial Tests", func() {

	gg.BeforeEach(func() {
		if !featureEnabled("geo") {
			gg.Skip("Geo Tests will not run since feature is not supported by the server.")
			return
		}
	})

	// connection data
	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)
	wpolicy.SendKey = true
	var size = 20
	const keyCount = 1000

	var binName = "GeoBin"

	gg.It("must Query a specific Region Containing a Point and get only relevant records back", func() {

		regions := []string{
			`{
		    "type": "Polygon",
		    "coordinates": [
		        [[-122.500000, 37.000000],[-121.000000, 37.000000],
		         [-121.000000, 38.080000],[-122.500000, 38.080000],
		         [-122.500000, 37.000000]]
		    ]
		}`,
			// 	`{
			//     "type": "Polygon",
			//     "coordinates": [
			//         [[-125.500000, 33.000000],[-124.000000, 31.000000],
			//          [-123.000000, 32.080000],[-123.500000, 32.080000],
			//          [-126.500000, 34.000000]]
			//     ]
			// }`,
		}

		for i, ptsb := range regions {
			key, _ := as.NewKey(ns, set, i)
			bin := as.NewBin(binName, as.NewGeoJSONValue(ptsb))
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		// queries only work on indices
		client.DropIndex(wpolicy, ns, set, set+binName)
		time.Sleep(time.Second)

		idxTask, err := client.CreateIndex(wpolicy, ns, set, set+binName, binName, as.GEO2DSPHERE)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until index is created
		gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

		defer client.DropIndex(wpolicy, ns, set, set+binName)

		points := []string{
			`{ "type": "Point", "coordinates": [-122.000000, 37.500000] }`,
			`{ "type": "Point", "coordinates": [-121.700000, 37.800000] }`,
			`{ "type": "Point", "coordinates": [-121.900000, 37.600000] }`,
			`{ "type": "Point", "coordinates": [-121.800000, 37.700000] }`,
			`{ "type": "Point", "coordinates": [-121.600000, 37.900000] }`,
			`{ "type": "Point", "coordinates": [-121.500000, 38.000000] }`,
		}

		for _, rgnsb := range points {
			stm := as.NewStatement(ns, set)
			stm.SetFilter(as.NewGeoWithinRegionFilter(binName, rgnsb))
			recordset, err := client.Query(nil, stm)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			count := 0
			for res := range recordset.Results() {
				gm.Expect(res.Err).ToNot(gm.HaveOccurred())
				gm.Expect(regions).To(gm.ContainElement(res.Record.Bins[binName].(string)))
				count++
			}

			// 1 region should be found
			gm.Expect(count).To(gm.Equal(1))
		}
	})

	gg.It("must Query a specific Point in Region and get only relevant records back", func() {
		points := []string{}
		for i := 0; i < size; i++ {
			lng := -122.0 + (0.1 * float64(i))
			lat := 37.5 + (0.1 * float64(i))
			ptsb := "{ \"type\": \"Point\", \"coordinates\": ["
			ptsb += fmt.Sprintf("%f", lng)
			ptsb += ", "
			ptsb += fmt.Sprintf("%f", lat)
			ptsb += "] }"

			points = append(points, ptsb)

			key, _ := as.NewKey(ns, set, i)
			bin := as.NewBin(binName, as.NewGeoJSONValue(ptsb))
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		// queries only work on indices
		client.DropIndex(wpolicy, ns, set, set+binName)
		time.Sleep(time.Second)

		idxTask, err := client.CreateIndex(wpolicy, ns, set, set+binName, binName, as.GEO2DSPHERE)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until index is created
		gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

		defer client.DropIndex(wpolicy, ns, set, set+binName)

		rgnsb := `{
		    "type": "Polygon",
		    "coordinates": [
		        [[-122.500000, 37.000000],[-121.000000, 37.000000],
		         [-121.000000, 38.080000],[-122.500000, 38.080000],
		         [-122.500000, 37.000000]]
		    ]
		}`

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewGeoRegionsContainingPointFilter(binName, rgnsb))
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			gm.Expect(points).To(gm.ContainElement(res.Record.Bins[binName].(string)))
			count++
		}

		// 6 points should be found
		gm.Expect(count).To(gm.Equal(6))
	})

	gg.It("must Query specific Points in Region denoted by a point and radius and get only relevant records back", func() {
		points := []string{}
		for i := 0; i < size; i++ {
			lng := -122.0 + (0.1 * float64(i))
			lat := 37.5 + (0.1 * float64(i))
			ptsb := "{ \"type\": \"Point\", \"coordinates\": ["
			ptsb += fmt.Sprintf("%f", lng)
			ptsb += ", "
			ptsb += fmt.Sprintf("%f", lat)
			ptsb += "] }"

			points = append(points, ptsb)

			key, _ := as.NewKey(ns, set, i)
			bin := as.NewBin(binName, as.NewGeoJSONValue(ptsb))
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		// queries only work on indices
		client.DropIndex(wpolicy, ns, set, set+binName)
		time.Sleep(time.Second)

		idxTask, err := client.CreateIndex(wpolicy, ns, set, set+binName, binName, as.GEO2DSPHERE)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until index is created
		gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())

		defer client.DropIndex(wpolicy, ns, set, set+binName)

		lon := float64(-122.0)
		lat := float64(37.5)
		radius := float64(50000.0)

		stm := as.NewStatement(ns, set)
		stm.SetFilter(as.NewGeoWithinRadiusFilter(binName, lon, lat, radius))
		recordset, err := client.Query(nil, stm)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		count := 0
		for res := range recordset.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			gm.Expect(points).To(gm.ContainElement(res.Record.Bins[binName].(string)))
			count++
		}

		// 6 points should be found
		gm.Expect(count).To(gm.Equal(4))
	})

})
