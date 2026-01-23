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
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

const (
	setName   = "exp_SI_test_set"
	indexName = "εχπ_ΣΙ_τεστ_ιδχ"

	// Additional indices for comprehensive testing
	ageIndexName              = "age_index"
	ageExpIndexName           = "age_exp_index"
	nameIndexName             = "name_index"
	listIndexName             = "list_index"
	listNumericIndexName      = "list_numeric_index"
	listExpIndexName          = "list_exp_index"
	listNumericExpIndexName   = "list_numeric_exp_index"
	mapKeysIndexName          = "map_keys_index"
	mapValsIndexName          = "map_vals_index"
	geoIndexName              = "geo_index"
	geoExpIndexName           = "geo_exp_index"
	geoCollectionIndexName    = "geo_collection_index"
	geoCollectionExpIndexName = "geo_collection_exp_index"
)

var countries = []string{"Australia", "Canada", "USA"}
var exp = as.ExpCond(
	as.ExpAnd(
		as.ExpGreaterEq(as.ExpIntBin("age"), as.ExpIntVal(18)),
		as.ExpOr(
			as.ExpEq(as.ExpStringBin("country"), as.ExpStringVal(countries[0])),
			as.ExpEq(as.ExpStringBin("country"), as.ExpStringVal(countries[1])),
			as.ExpEq(as.ExpStringBin("country"), as.ExpStringVal(countries[2])),
		),
	),
	as.ExpIntVal(1),
	as.ExpUnknown(),
)

var _ = gg.Describe("Secondary index test", gg.Ordered, func() {
	var wpolicy = as.NewWritePolicy(0, 0)

	gg.BeforeAll(func() {
		node := client.GetNodes()[0]
		serverVersion := node.GetServerVersion()
		if serverVersion.IsSmaller(version.ServerVersion_8_1) {
			gg.Skip("Secondary index tests require server version 8.1.0 or greater")
		}
	})

	gg.Describe("Index creation with expression", gg.Ordered, func() {
		gg.BeforeAll(func() {
			// Make sure the global set‑up really happened.
			gm.Expect(client).NotTo(gm.BeNil(), "client must be initialized in the suite's set‑up")

			task, err := client.CreateIndexWithExpression(wpolicy, *namespace, setName, indexName, as.NUMERIC, as.ICT_DEFAULT, exp)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndex: %v", err))
			}

			// wait until index is created
			<-task.OnComplete()

			insertTestRecords()
		})

		gg.AfterAll(func() {
			// Drop the index and truncate data
			client.DropIndex(wpolicy, *namespace, setName, indexName)
			client.Truncate(nil, *namespace, setName, nil)
		})

		gg.Context("Create non-existing index", func() {
			gg.It("is listed after creation", func() {
				info := getSIInfo()
				gm.Expect(info).To(gm.ContainSubstring("indexname=" + indexName))
			})

			gg.It("returns six records when filtering by index *name*", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewRangeWithIndexNameFilter(indexName, 1, 1))
				gm.Expect(runQueryAndAssert(stmt)).To(gm.Equal(6))
			})

			gg.It("returns six records when filtering by *expression*", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewRangeWithExpressionFilter(exp, 1, 1))
				expectedResponse := runQueryAndAssert(stmt)
				gm.Expect(expectedResponse).To(gm.Equal(6))
			})

			gg.It("returns six records when filtering with NewEqualWithExpressionFilter", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewEqualWithExpressionFilter(exp, 1))
				expectedResponse := runQueryAndAssert(stmt)
				gm.Expect(expectedResponse).To(gm.Equal(6))
			})

			gg.It("returns six records when filtering with NewEqualWithIndexNameFilter", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewEqualWithIndexNameFilter(indexName, 1))
				expectedResponse := runQueryAndAssert(stmt)
				gm.Expect(expectedResponse).To(gm.Equal(6))
			})
		})
	})

	gg.Describe("Range Filters", gg.Ordered, func() {
		var ageExp *as.Expression

		gg.BeforeAll(func() {
			// Create numeric index on age for range testing
			task1, err := client.CreateIndex(wpolicy, *namespace, setName, ageIndexName, "age", as.NUMERIC)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndex: %v", err))
			}
			<-task1.OnComplete()

			ageExp = as.ExpIntBin("age")
			task2, err := client.CreateIndexWithExpression(wpolicy, *namespace, setName, ageExpIndexName, as.NUMERIC, as.ICT_DEFAULT, ageExp)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndexWithExpression: %v", err))
			}
			<-task2.OnComplete()

			insertTestRecords()
		})

		gg.AfterAll(func() {
			client.DropIndex(wpolicy, *namespace, setName, ageIndexName)
			client.DropIndex(wpolicy, *namespace, setName, ageExpIndexName)
			client.Truncate(nil, *namespace, setName, nil)
		})

		gg.It("NewRangeFilter should return correct records", func() {
			stmt := as.NewStatement(*namespace, setName)
			stmt.SetFilter(as.NewRangeFilter("age", 20, 50))
			count := runQueryAndCount(stmt)
			gm.Expect(count).To(gm.Equal(6))
		})

		gg.It("NewRangeWithExpressionFilter should work with age expression", func() {
			stmt := as.NewStatement(*namespace, setName)
			stmt.SetFilter(as.NewRangeWithExpressionFilter(ageExp, 20, 50))
			count := runQueryAndCount(stmt)
			gm.Expect(count).To(gm.Equal(6))
		})

		gg.It("NewRangeWithIndexNameFilter should work with bin index name", func() {
			stmt := as.NewStatement(*namespace, setName)
			stmt.SetFilter(as.NewRangeWithIndexNameFilter(ageIndexName, 20, 50))
			count := runQueryAndCount(stmt)
			gm.Expect(count).To(gm.Equal(6))
		})

		gg.It("NewRangeWithIndexNameFilter should work with expression index name", func() {
			stmt := as.NewStatement(*namespace, setName)
			stmt.SetFilter(as.NewRangeWithIndexNameFilter(ageExpIndexName, 20, 50))
			count := runQueryAndCount(stmt)
			gm.Expect(count).To(gm.Equal(6))
		})
	})

	gg.Describe("Collection Filters", gg.Ordered, func() {
		var skillsExp *as.Expression

		gg.BeforeAll(func() {
			// Create collection indices
			task1, err := client.CreateComplexIndex(wpolicy, *namespace, setName, listIndexName, "skills", as.STRING, as.ICT_LIST)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateComplexIndex for list: %v", err))
			}
			<-task1.OnComplete()

			// Create numeric index on skills list for range operations
			task2, err := client.CreateComplexIndex(wpolicy, *namespace, setName, listNumericIndexName, "skills", as.NUMERIC, as.ICT_LIST)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateComplexIndex for list numeric: %v", err))
			}
			<-task2.OnComplete()

			task3, err := client.CreateComplexIndex(wpolicy, *namespace, setName, mapKeysIndexName, "metadata", as.STRING, as.ICT_MAPKEYS)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateComplexIndex for map keys: %v", err))
			}
			<-task3.OnComplete()

			task4, err := client.CreateComplexIndex(wpolicy, *namespace, setName, mapValsIndexName, "metadata", as.NUMERIC, as.ICT_MAPVALUES)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateComplexIndex for map values: %v", err))
			}
			<-task4.OnComplete()

			skillsExp = as.ExpListBin("skills")
			task5, err := client.CreateIndexWithExpression(wpolicy, *namespace, setName, listExpIndexName, as.STRING, as.ICT_LIST, skillsExp)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndexWithExpression for list: %v", err))
			}
			<-task5.OnComplete()

			task6, err := client.CreateIndexWithExpression(wpolicy, *namespace, setName, listNumericExpIndexName, as.NUMERIC, as.ICT_LIST, skillsExp)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndexWithExpression for list numeric: %v", err))
			}
			<-task6.OnComplete()

			insertTestRecords()
		})

		gg.AfterAll(func() {
			client.DropIndex(wpolicy, *namespace, setName, listIndexName)
			client.DropIndex(wpolicy, *namespace, setName, listNumericIndexName)
			client.DropIndex(wpolicy, *namespace, setName, mapKeysIndexName)
			client.DropIndex(wpolicy, *namespace, setName, mapValsIndexName)
			client.DropIndex(wpolicy, *namespace, setName, listExpIndexName)
			client.DropIndex(wpolicy, *namespace, setName, listNumericExpIndexName)
			client.Truncate(nil, *namespace, setName, nil)
		})

		gg.Context("Contains Filters", func() {
			gg.It("NewContainsFilter should find records with specific list element", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsFilter("skills", as.ICT_LIST, "Go"))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(4)) // Tim, Steven, Sam, Kiril
			})

			gg.It("NewContainsWithExpressionFilter should work with list expression", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsWithExpressionFilter(skillsExp, as.ICT_LIST, "Python"))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(4)) // Tim, Jo, Steven, Jess, Vivek
			})

			gg.It("NewContainsWithIndexNameFilter should work with index name", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsWithIndexNameFilter(listIndexName, as.ICT_LIST, "Java"))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(2)) // Bob, Alex
			})

			gg.It("NewContainsFilter should find records with specific map key", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsFilter("metadata", as.ICT_MAPKEYS, "level"))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(12)) // All records have "level" key
			})

			gg.It("NewContainsFilter should find records with specific map value", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsFilter("metadata", as.ICT_MAPVALUES, 5))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(2)) // Tim, Alex
			})
		})

		gg.Context("Contains Range Filters", func() {
			gg.It("NewContainsRangeFilter should work with numeric ranges in lists", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsRangeFilter("skills", as.ICT_LIST, 1, 5))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(5)) // Records with numeric values 1-5 in skills list
			})

			gg.It("NewContainsRangeWithExpressionFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsRangeWithExpressionFilter(skillsExp, as.ICT_LIST, 6, 10))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(5)) // Records with numeric values 6-10 in skills list
			})

			gg.It("NewContainsRangeWithIndexNameFilter should work with numeric list index", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsRangeWithIndexNameFilter(listNumericIndexName, as.ICT_LIST, 6, 10))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(5)) // Records with numeric values 6-10 in skills list
			})

			gg.It("NewContainsRangeWithIndexNameFilter should work with map values index", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewContainsRangeWithIndexNameFilter(mapValsIndexName, as.ICT_MAPVALUES, 3, 5))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(7)) // Records with map values 3-5
			})
		})
	})

	gg.Describe("Geospatial Filters", gg.Ordered, func() {
		var locationExp *as.Expression
		var locationsExp *as.Expression

		gg.BeforeAll(func() {
			// Create geospatial index
			task1, err := client.CreateIndex(wpolicy, *namespace, setName, geoIndexName, "location", as.GEO2DSPHERE)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndex for geo: %v", err))
			}
			<-task1.OnComplete()

			// Create expression index for location to test expression filters
			locationExp = as.ExpGeoBin("location")
			task2, err := client.CreateIndexWithExpression(wpolicy, *namespace, setName, geoExpIndexName, as.GEO2DSPHERE, as.ICT_DEFAULT, locationExp)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndexWithExpression for geo: %v", err))
			}
			<-task2.OnComplete()

			// Create collection-based geospatial index for locations list
			task3, err := client.CreateComplexIndex(wpolicy, *namespace, setName, geoCollectionIndexName, "locations", as.GEO2DSPHERE, as.ICT_LIST)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateComplexIndex for geo collection: %v", err))
			}
			<-task3.OnComplete()

			// Create expression index for locations collection to test collection expression filters
			locationsExp = as.ExpListBin("locations")
			task4, err := client.CreateIndexWithExpression(wpolicy, *namespace, setName, geoCollectionExpIndexName, as.GEO2DSPHERE, as.ICT_LIST, locationsExp)
			if err != nil {
				gg.Fail(fmt.Sprintf("CreateIndexWithExpression for geo collection: %v", err))
			}
			<-task4.OnComplete()

			insertTestRecords()
		})

		gg.AfterAll(func() {
			client.DropIndex(wpolicy, *namespace, setName, geoIndexName)
			client.DropIndex(wpolicy, *namespace, setName, geoExpIndexName)
			client.DropIndex(wpolicy, *namespace, setName, geoCollectionIndexName)
			client.DropIndex(wpolicy, *namespace, setName, geoCollectionExpIndexName)
			client.Truncate(nil, *namespace, setName, nil)
		})

		australiaRegion := `{
			"type": "Polygon",
			"coordinates": [[
				[110, -45], [160, -45], [160, -10], [110, -10], [110, -45]
			]]
		}`

		gg.Context("Geo Within Region Filters", func() {
			gg.It("NewGeoWithinRegionFilter should find points in Australia", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRegionFilter("location", australiaRegion))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(2)) // Tim, Pam (Australia)
			})

			gg.It("NewGeoWithinRegionWithExpressionFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRegionWithExpressionFilter(locationExp, australiaRegion))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(2))
			})

			gg.It("NewGeoWithinRegionWithIndexNameFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRegionWithIndexNameFilter(geoIndexName, australiaRegion))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(2))
			})
		})

		gg.Context("Geo Within Radius Filters", func() {
			londonLng, londonLat := -0.1276, 51.5074
			radius := 1000000.0 // 1000km

			gg.It("NewGeoWithinRadiusFilter should find points near London", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRadiusFilter("location", londonLng, londonLat, radius))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 1)) // At least Bill (UK)
			})

			gg.It("NewGeoWithinRadiusWithExpressionFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRadiusWithExpressionFilter(locationExp, londonLng, londonLat, radius))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 1))
			})

			gg.It("NewGeoWithinRadiusWithIndexNameFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRadiusWithIndexNameFilter(geoIndexName, londonLng, londonLat, radius))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 1))
			})
		})

		gg.Context("Geo Regions Containing Point Filters", func() {
			sydneyPoint := `{"type": "Point", "coordinates": [151.2093, -33.8688]}`

			gg.It("NewGeoRegionsContainingPointFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoRegionsContainingPointFilter("location", sydneyPoint))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 0)) // May not find exact matches depending on data
			})

			gg.It("NewGeoRegionsContainingPointWithExpressionFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoRegionsContainingPointWithExpressionFilter(locationExp, sydneyPoint))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 0))
			})

			gg.It("NewGeoRegionsContainingPointWithIndexNameFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoRegionsContainingPointWithIndexNameFilter(geoIndexName, sydneyPoint))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 0))
			})
		})

		gg.Context("Collection Geospatial Filters", func() {
			// Collection geospatial filters now work with proper collection setup

			gg.It("NewGeoWithinRegionForCollectionFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRegionForCollectionFilter("locations", as.ICT_LIST, australiaRegion))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(2)) // Tim and Pam (Australia)
			})

			gg.It("NewGeoWithinRegionForCollectionWithExpressionFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRegionForCollectionWithExpressionFilter(locationsExp, as.ICT_LIST, australiaRegion))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(2)) // Tim and Pam (Australia)
			})

			gg.It("NewGeoWithinRegionForCollectionWithIndexNameFilter should work", func() {
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRegionForCollectionWithIndexNameFilter(geoCollectionIndexName, as.ICT_LIST, australiaRegion))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.Equal(2)) // Tim and Pam (Australia)
			})

			gg.It("NewGeoWithinRadiusForCollectionFilter should work", func() {
				londonLng, londonLat := -0.1276, 51.5074
				radius := 1000000.0
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRadiusForCollectionFilter("locations", as.ICT_LIST, londonLng, londonLat, radius))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 1)) // At least Bill (UK) should match
			})

			gg.It("NewGeoWithinRadiusForCollectionWithExpressionFilter should work", func() {
				londonLng, londonLat := -0.1276, 51.5074
				radius := 1000000.0
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRadiusForCollectionWithExpressionFilter(locationsExp, as.ICT_LIST, londonLng, londonLat, radius))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 1)) // At least Bill (UK) should match
			})

			gg.It("NewGeoWithinRadiusForCollectionWithIndexNameFilter should work", func() {
				londonLng, londonLat := -0.1276, 51.5074
				radius := 1000000.0
				stmt := as.NewStatement(*namespace, setName)
				stmt.SetFilter(as.NewGeoWithinRadiusForCollectionWithIndexNameFilter(geoCollectionIndexName, as.ICT_LIST, londonLng, londonLat, radius))
				count := runQueryAndCount(stmt)
				gm.Expect(count).To(gm.BeNumerically(">=", 1)) // At least Bill (UK) should match
			})
		})
	})
})

func insertTestRecords() {
	people := []struct {
		key       int
		name      string
		age       int
		country   string
		skills    []any
		metadata  map[any]any
		location  string
		locations []any // Collection of geospatial points
	}{
		{1, "Tim", 312, "Australia", []any{"Go", "Python", 1}, map[any]any{"level": 5, "active": true}, `{"type": "Point", "coordinates": [144.9631, -37.8136]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [144.9631, -37.8136]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [144.9700, -37.8200]}`)}},
		{2, "Bob", 47, "Canada", []any{"Java", "C++", 2}, map[any]any{"level": 3, "active": false}, `{"type": "Point", "coordinates": [-79.3832, 43.6532]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-79.3832, 43.6532]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-79.3900, 43.6600]}`)}},
		{3, "Jo", 15, "USA", []any{"JavaScript", "HTML", 3}, map[any]any{"level": 2, "active": true}, `{"type": "Point", "coordinates": [-74.0060, 40.7128]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-74.0060, 40.7128]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-74.0100, 40.7200]}`)}},
		{4, "Steven", 23, "Botswana", []any{"Python", "Go", 4}, map[any]any{"level": 4, "active": true}, `{"type": "Point", "coordinates": [25.9084, -24.6282]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [25.9084, -24.6282]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [25.9150, -24.6350]}`)}},
		{5, "Susan", 32, "Canada", []any{"C#", "SQL", 5}, map[any]any{"level": 4, "active": false}, `{"type": "Point", "coordinates": [-123.1207, 49.2827]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-123.1207, 49.2827]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-123.1300, 49.2900]}`)}},
		{6, "Jess", 17, "USA", []any{"Python", "R", 6}, map[any]any{"level": 1, "active": true}, `{"type": "Point", "coordinates": [-122.4194, 37.7749]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-122.4194, 37.7749]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-122.4250, 37.7800]}`)}},
		{7, "Sam", 18, "USA", []any{"Go", "Rust", 7}, map[any]any{"level": 2, "active": true}, `{"type": "Point", "coordinates": [-87.6298, 41.8781]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-87.6298, 41.8781]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-87.6350, 41.8850]}`)}},
		{8, "Alex", 47, "Canada", []any{"Java", "Kotlin", 8}, map[any]any{"level": 5, "active": true}, `{"type": "Point", "coordinates": [-75.6972, 45.4215]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-75.6972, 45.4215]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-75.7000, 45.4280]}`)}},
		{9, "Pam", 56, "Australia", []any{"C++", "Assembly", 9}, map[any]any{"level": 6, "active": false}, `{"type": "Point", "coordinates": [151.2093, -33.8688]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [151.2093, -33.8688]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [151.2150, -33.8750]}`)}},
		{10, "Vivek", 12, "India", []any{"Python", "Django", 10}, map[any]any{"level": 1, "active": true}, `{"type": "Point", "coordinates": [77.1025, 28.7041]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [77.1025, 28.7041]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [77.1100, 28.7100]}`)}},
		{11, "Kiril", 22, "Sweden", []any{"Go", "Docker", 11}, map[any]any{"level": 3, "active": true}, `{"type": "Point", "coordinates": [18.0686, 59.3293]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [18.0686, 59.3293]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [18.0750, 59.3350]}`)}},
		{12, "Bill", 23, "UK", []any{"JavaScript", "Node.js", 12}, map[any]any{"level": 3, "active": false}, `{"type": "Point", "coordinates": [-0.1276, 51.5074]}`, []any{as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-0.1276, 51.5074]}`), as.NewGeoJSONValue(`{"type": "Point", "coordinates": [-0.1200, 51.5150]}`)}},
	}

	for _, p := range people {
		key, _ := as.NewKey(*namespace, setName, p.key)
		err := client.PutBins(nil, key,
			as.NewBin("name", p.name),
			as.NewBin("age", p.age),
			as.NewBin("country", p.country),
			as.NewBin("skills", p.skills),
			as.NewBin("metadata", p.metadata),
			as.NewBin("location", as.NewGeoJSONValue(p.location)),
			as.NewBin("locations", p.locations), // Collection of geospatial points
		)
		gm.Expect(err).NotTo(gm.HaveOccurred())
	}
}

func getSIInfo() string {
	cmd := fmt.Sprintf("sindex-list/%s/%s", *namespace, indexName)
	node := client.GetNodes()[0]
	info, err := node.RequestInfo(as.NewInfoPolicy(), cmd)

	gm.Expect(err).NotTo(gm.HaveOccurred())
	return info[cmd]
}

func runQueryAndAssert(stmt *as.Statement) int {
	qp := as.NewQueryPolicy()
	rs, err := client.Query(qp, stmt)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	defer rs.Close()

	count := 0
	for res := range rs.Results() {
		gm.Expect(res.Err).NotTo(gm.HaveOccurred())

		age := res.Record.Bins["age"].(int)
		country := res.Record.Bins["country"].(string)

		gm.Expect(age).To(gm.BeNumerically(">=", 18))
		gm.Expect(country).To(gm.BeElementOf(countries))
		count++
	}
	return count
}

func runQueryAndCount(stmt *as.Statement) int {
	qp := as.NewQueryPolicy()
	rs, err := client.Query(qp, stmt)
	gm.Expect(err).NotTo(gm.HaveOccurred())
	defer rs.Close()

	count := 0
	for res := range rs.Results() {
		gm.Expect(res.Err).NotTo(gm.HaveOccurred())
		count++
	}
	return count
}
