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
	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"

	as "github.com/aerospike/aerospike-client-go"
)

const udfCDTTests = `
function add(rec, bin, key, value)
	if not aerospike:exists(rec) then
		--create record
		local m1 = map()
		local m2 = map()
		m2[value] = key

		m1[key] = m2			-- set map to map
		rec[bin] = m1		-- set map to record
		--create record
		return aerospike:create(rec)
	end

	--record existed, let's see if key exists
	local m1 = rec[bin]		--get map
	local m2 = m1[key]		--get second map

	--did list already exist?
	if m2 == nil then
		m2 = map() 		--map didn't exist yet, let's create it
	end
	local doesExist = m2[value]
	if doesExist ~= nil then
		return 0 --value already existed, no need to update record
	end

	m2[value] 	= key
	--done setting values, let's store information back to set
	m1[key] 		= m2		--map back to map
	rec[bin] 	= m1	--map back to record

	return aerospike:update(rec) --..and update aerospike :)
end

--remove from map[]map[]
function remove(rec, bin, key, value)
	if not aerospike:exists(rec) then
		return 0 --record does not exist, cannot remove so no error
	end

	local m1 = rec[bin]
	local m2 = m1[key]
	if m2 == nil then
		return 0 --key does not exist, cannot remove
	end

	--remove key from map
	local doesExist = m2[value]
	if doesExist == nil then
		return 0 --value does not exist
	end

	map.remove(m2, value) --remove value from map
	--done, let's update record with modified maps
	m1[key] 		= m2 --back to map
	rec[bin]		= m1 --back to record

	return aerospike:update(rec) --and update!
end
`

var _ = gg.Describe("CDT Map Test", func() {

	// connection data
	var err error
	var ns = *namespace
	var set = randString(50)
	var key *as.Key
	var wpolicy = as.NewWritePolicy(0, 0)
	var cdtBinName string
	// var list []interface{}

	putMode := as.DefaultMapPolicy()
	addMode := as.NewMapPolicy(as.MapOrder.UNORDERED, as.MapWriteMode.CREATE_ONLY)

	gg.BeforeEach(func() {

		if !featureEnabled("cdt-map") {
			gg.Skip("CDT Map Tests will not run since feature is not supported by the server.")
			return
		}

		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtBinName = randString(10)
	})

	gg.Describe("Simple Usecases", func() {

		gg.It("should create a valid CDT Map using MapPutOp", func() {
			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutOp(putMode, cdtBinName, 1, 1),
				as.MapPutOp(putMode, cdtBinName, 2, 2),
				as.MapPutOp(addMode, cdtBinName, 3, 3),
				as.MapPutOp(addMode, cdtBinName, 4, 4),
				as.MapPutOp(addMode, cdtBinName, 6, 6),
				as.MapPutOp(addMode, cdtBinName, 7, 7),
				as.MapPutOp(addMode, cdtBinName, 8, 8),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			cdtMap, err = client.Get(nil, key, cdtBinName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal(map[interface{}]interface{}{1: 1, 2: 2, 3: 3, 4: 4, 6: 6, 7: 7, 8: 8}))
		})

		gg.It("should unpack an empty Non-Ordered CDT map correctly", func() {
			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutOp(putMode, cdtBinName, 1, 1),
				as.MapRemoveByKeyOp(cdtBinName, 1, as.MapReturnType.NONE),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			cdtMap, err = client.Get(nil, key, cdtBinName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal(map[interface{}]interface{}{}))
		})

		gg.It("should unpack an empty Ordered CDT map correctly", func() {
			_, err := client.Operate(wpolicy, key,
				as.MapPutOp(as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE), cdtBinName, 1, 1),
				// MapRemoveByKeyOp(cdtBinName, 1, as.MapReturnType.NONE),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rs, err := client.ScanAll(nil, ns, set)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for rs := range rs.Results() {
				gm.Expect(rs.Err).ToNot(gm.HaveOccurred())

			}

		})

		gg.It("should return the content of an Ordered CDT map correctly", func() {
			items := map[interface{}]interface{}{
				"mk1": []interface{}{"v1.0", "v1.1"},
				"mk2": []interface{}{"v2.0", "v2.1"},
			}

			rec, err := client.Operate(nil, key,
				as.MapPutItemsOp(as.NewMapPolicy(as.MapOrder.KEY_VALUE_ORDERED, as.MapWriteMode.UPDATE), "bin", items),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rec, err = client.Operate(nil, key,
				as.MapGetByKeyOp("bin", "mk1", as.MapReturnType.VALUE),
				as.MapGetByKeyOp("bin", "mk2", as.MapReturnType.VALUE),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"bin": []interface{}{[]interface{}{"v1.0", "v1.1"}, []interface{}{"v2.0", "v2.1"}}}))

			rec, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"bin": []as.MapPair{as.MapPair{Key: "mk1", Value: []interface{}{"v1.0", "v1.1"}}, as.MapPair{Key: "mk2", Value: []interface{}{"v2.0", "v2.1"}}}}))
		})

		gg.It("should create a valid CDT Map using MapPutOp", func() {
			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutOp(putMode, cdtBinName, 1, 1),
				as.MapPutOp(putMode, cdtBinName, 2, 2),
				as.MapPutOp(addMode, cdtBinName, 3, 3),
				as.MapPutOp(addMode, cdtBinName, 4, 4),

				as.GetOpForBin(cdtBinName),
			)
			// gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap).NotTo(gm.Equal([]interface{}{1, 2, 3, 4, 4, 4, map[interface{}]interface{}{1: 1, 2: 2, 3: 3, 4: 4}}))

			cdtMap, err = client.Get(nil, key, cdtBinName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal(map[interface{}]interface{}{1: 1, 2: 2, 3: 3, 4: 4}))
		})

		gg.It("should create a valid CDT Map using MapPutItemsOp", func() {
			addMap := map[interface{}]interface{}{
				12:    "myValue",
				-8734: "str2",
				1:     "my default",
			}

			putMap := map[interface{}]interface{}{
				12: "myval12222",
				13: "str13",
			}

			updateMap := map[interface{}]interface{}{
				13: "myval2",
			}

			replaceMap := map[interface{}]interface{}{
				12:    23,
				-8734: "changed",
			}

			putMode := as.DefaultMapPolicy()
			addMode := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.CREATE_ONLY)
			updateMode := as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE_ONLY)

			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutItemsOp(addMode, cdtBinName, addMap),
				as.MapPutItemsOp(putMode, cdtBinName, putMap),
				as.MapPutItemsOp(updateMode, cdtBinName, updateMap),
				as.MapPutItemsOp(updateMode, cdtBinName, replaceMap),
				as.MapGetByKeyOp(cdtBinName, 1, as.MapReturnType.VALUE),
				as.MapGetByKeyOp(cdtBinName, -8734, as.MapReturnType.VALUE),
				as.MapGetByKeyRangeOp(cdtBinName, 12, 15, as.MapReturnType.KEY_VALUE),
				// as.GetOpForBin(cdtBinName),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal([]interface{}{3, 4, 4, 4, "my default", "changed", []as.MapPair{{Key: 12, Value: 23}, {Key: 13, Value: "myval2"}}}))

			cdtMap, err = client.Get(nil, key, cdtBinName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal([]as.MapPair{{Key: -8734, Value: "changed"}, {Key: 1, Value: "my default"}, {Key: 12, Value: 23}, {Key: 13, Value: "myval2"}}))
		})

		gg.It("should create a valid CDT Map using mixed MapPutOp and MapPutItemsOp", func() {

			items := map[interface{}]interface{}{
				12:    "myval",
				-8734: "str2",
				1:     "my default",
				7:     1,
			}

			otherBinName := "other_bin"

			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutItemsOp(as.NewMapPolicy(as.MapOrder.KEY_VALUE_ORDERED, as.MapWriteMode.UPDATE), cdtBinName, items),
				as.PutOp(as.NewBin(otherBinName, "head")),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal(4))
			gm.Expect(cdtMap.Bins).To(gm.HaveKey(otherBinName))

			cdtMap, err = client.Operate(wpolicy, key,
				as.MapGetByKeyOp(cdtBinName, 12, as.MapReturnType.INDEX),
				as.AppendOp(as.NewBin(otherBinName, "...tail")),
				as.GetOpForBin(otherBinName),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: 3, "other_bin": []interface{}{nil, "head...tail"}})) // there were two operations for bin `other_bin`, so the results come back in an array

			// Should set SendKey == true for a solely read operation without getting PARAMETER_ERROR from the server
			wpolicy2 := *wpolicy
			wpolicy2.SendKey = true
			cdtMap, err = client.Operate(&wpolicy2, key,
				as.MapGetByKeyOp(cdtBinName, 12, as.MapReturnType.VALUE),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: "myval"}))
		})

		gg.It("should create a valid CDT Map and then Switch Policy For Order", func() {

			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutOp(as.DefaultMapPolicy(), cdtBinName, 4, 1),
				as.MapPutOp(as.DefaultMapPolicy(), cdtBinName, 3, 2),
				as.MapPutOp(as.DefaultMapPolicy(), cdtBinName, 2, 3),
				as.MapPutOp(as.DefaultMapPolicy(), cdtBinName, 1, 4),

				as.MapGetByIndexOp(cdtBinName, 2, as.MapReturnType.KEY_VALUE),
				as.MapGetByIndexRangeCountOp(cdtBinName, 0, 10, as.MapReturnType.KEY_VALUE),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[0]).To(gm.Equal(1))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[1]).To(gm.Equal(2))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[2]).To(gm.Equal(3))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[3]).To(gm.Equal(4))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[4]).To(gm.Equal([]as.MapPair{{Key: 3, Value: 2}}))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[5]).To(gm.ConsistOf([]as.MapPair{{Key: 1, Value: 4}, {Key: 2, Value: 3}, {Key: 3, Value: 2}, {Key: 4, Value: 1}}))

			cdtMap, err = client.Operate(wpolicy, key,
				as.MapSetPolicyOp(as.NewMapPolicy(as.MapOrder.KEY_ORDERED, as.MapWriteMode.UPDATE), cdtBinName),

				as.MapGetByKeyRangeOp(cdtBinName, 3, 5, as.MapReturnType.COUNT),
				as.MapGetByKeyRangeOp(cdtBinName, -5, 2, as.MapReturnType.KEY_VALUE),
				as.MapGetByIndexRangeCountOp(cdtBinName, 0, 10, as.MapReturnType.KEY_VALUE),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: []interface{}{interface{}(nil), 2, []as.MapPair{{Key: 1, Value: 4}}, []as.MapPair{{Key: 1, Value: 4}, {Key: 2, Value: 3}, {Key: 3, Value: 2}, {Key: 4, Value: 1}}}}))

		})

		gg.It("should create a valid CDT Map and then apply Inc/Dec operations and Get Correct Values", func() {

			items := map[interface{}]interface{}{
				"Charlie": 55,
				"Jim":     98,
				"John":    76,
				"Harry":   82,
			}

			// Write values to empty map.
			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutItemsOp(as.DefaultMapPolicy(), cdtBinName, items),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())

			cdtMap, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

			cdtMap, err = client.Operate(wpolicy, key,
				as.MapIncrementOp(as.DefaultMapPolicy(), cdtBinName, "John", 5),
				as.MapDecrementOp(as.DefaultMapPolicy(), cdtBinName, "Jim", 4),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			cdtMap, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

			cdtMap, err = client.Operate(nil, key,
				as.MapGetByRankRangeCountOp(cdtBinName, -2, 2, as.MapReturnType.KEY),
				as.MapGetByRankRangeCountOp(cdtBinName, 0, 2, as.MapReturnType.KEY_VALUE),
				as.MapGetByRankOp(cdtBinName, 0, as.MapReturnType.VALUE),
				as.MapGetByRankOp(cdtBinName, 2, as.MapReturnType.KEY),
				as.MapGetByValueRangeOp(cdtBinName, 90, 95, as.MapReturnType.RANK),
				as.MapGetByValueRangeOp(cdtBinName, 90, 95, as.MapReturnType.COUNT),
				as.MapGetByValueRangeOp(cdtBinName, 90, 95, as.MapReturnType.KEY_VALUE),
				as.MapGetByValueRangeOp(cdtBinName, 81, 82, as.MapReturnType.KEY),
				as.MapGetByValueOp(cdtBinName, 77, as.MapReturnType.KEY),
				as.MapGetByValueOp(cdtBinName, 81, as.MapReturnType.RANK),
				as.MapGetByKeyOp(cdtBinName, "Charlie", as.MapReturnType.RANK),
				as.MapGetByKeyOp(cdtBinName, "Charlie", as.MapReturnType.REVERSE_RANK),
				as.MapGetByKeyListOp(cdtBinName, []interface{}{"Charlie", "Jim"}, as.MapReturnType.KEY),
				as.MapGetByValueListOp(cdtBinName, []interface{}{55, 94}, as.MapReturnType.KEY),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: []interface{}{
				[]interface{}{"Harry", "Jim"},
				[]as.MapPair{{Key: "Charlie", Value: 55}, {Key: "John", Value: 81}},
				55,
				"Harry",
				[]interface{}{3}, 1, []as.MapPair{{Key: "Jim", Value: 94}},
				[]interface{}{"John"},
				[]interface{}{},
				[]interface{}{1},
				0,
				3,
				[]interface{}{"Charlie", "Jim"},
				[]interface{}{"Charlie", "Jim"},
			}}))
		})

		gg.It("should create a valid CDT Map and then Get via MapReturnType.INVERTED", func() {

			items := map[interface{}]interface{}{
				"Charlie": 55,
				"Jim":     98,
				"John":    76,
				"Harry":   82,
			}

			// Write values to empty map.
			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutItemsOp(as.DefaultMapPolicy(), cdtBinName, items),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())

			cdtMap, err = client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

			cdtMap, err = client.Operate(nil, key,
				as.MapGetByRankRangeCountOp(cdtBinName, -2, 2, as.MapReturnType.KEY|as.MapReturnType.INVERTED),
				as.MapGetByRankRangeCountOp(cdtBinName, 0, 2, as.MapReturnType.KEY_VALUE|as.MapReturnType.INVERTED),
				as.MapGetByValueRangeOp(cdtBinName, 90, 95, as.MapReturnType.RANK|as.MapReturnType.INVERTED),
				as.MapGetByValueRangeOp(cdtBinName, 90, 95, as.MapReturnType.COUNT|as.MapReturnType.INVERTED),
				as.MapGetByValueRangeOp(cdtBinName, 90, 95, as.MapReturnType.KEY_VALUE|as.MapReturnType.INVERTED),
				as.MapGetByValueRangeOp(cdtBinName, 81, 82, as.MapReturnType.KEY|as.MapReturnType.INVERTED),
				as.MapGetByValueOp(cdtBinName, 77, as.MapReturnType.KEY|as.MapReturnType.INVERTED),
				as.MapGetByValueOp(cdtBinName, 81, as.MapReturnType.RANK|as.MapReturnType.INVERTED),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: []interface{}{
				[]interface{}{"Charlie", "John"},
				[]as.MapPair{{Key: "Harry", Value: 82}, {Key: "Jim", Value: 98}},
				[]interface{}{0, 1, 2, 3},
				4,
				[]as.MapPair{{Key: "Charlie", Value: 55}, {Key: "Harry", Value: 82}, {Key: "Jim", Value: 98}, {Key: "John", Value: 76}},
				[]interface{}{"Charlie", "Harry", "Jim", "John"},
				[]interface{}{"Charlie", "Harry", "Jim", "John"},
				[]interface{}{0, 1, 2, 3},
			}}))
		})

		gg.It("should create a valid CDT Map and then execute Remove operations", func() {

			items := map[interface{}]interface{}{
				"Charlie": 55,
				"Jim":     98,
				"John":    76,
				"Harry":   82,
				"Sally":   79,
				"Lenny":   84,
				"Abe":     88,
			}

			itemsToRemove := []interface{}{
				"Sally",
				"UNKNOWN",
				"Lenny",
			}

			// Write values to empty map.
			cdtMap, err := client.Operate(wpolicy, key,
				as.MapPutItemsOp(as.DefaultMapPolicy(), cdtBinName, items),
				as.MapRemoveByKeyOp(cdtBinName, "NOTFOUND", as.MapReturnType.VALUE),
				as.MapRemoveByKeyOp(cdtBinName, "Jim", as.MapReturnType.VALUE),
				as.MapRemoveByKeyListOp(cdtBinName, itemsToRemove, as.MapReturnType.VALUE),
				as.MapRemoveByValueOp(cdtBinName, 55, as.MapReturnType.KEY),
			)

			gm.Expect(err).ToNot(gm.HaveOccurred())

			// gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: []interface{}{7, nil, 98, []interface{}{79, 84}, []interface{}{"Charlie"}}}))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[0]).To(gm.Equal(7))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[1]).To(gm.BeNil())
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[2]).To(gm.Equal(98))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[3]).To(gm.ConsistOf([]interface{}{79, 84}))
			gm.Expect(cdtMap.Bins[cdtBinName].([]interface{})[4]).To(gm.Equal([]interface{}{"Charlie"}))
		})

	})

	gg.It("should create a valid CDT Map and then execute RemoveRange operations", func() {

		items := map[interface{}]interface{}{
			"Charlie": 55,
			"Jim":     98,
			"John":    76,
			"Harry":   82,
			"Sally":   79,
			"Lenny":   84,
			"Abe":     88,
		}

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			as.MapPutItemsOp(as.DefaultMapPolicy(), cdtBinName, items),
			as.MapRemoveByKeyRangeOp(cdtBinName, "J", "K", as.MapReturnType.COUNT),
			as.MapRemoveByValueRangeOp(cdtBinName, 80, 85, as.MapReturnType.COUNT),
			as.MapRemoveByIndexRangeCountOp(cdtBinName, 0, 2, as.MapReturnType.COUNT),
			as.MapRemoveByRankRangeCountOp(cdtBinName, 0, 2, as.MapReturnType.COUNT),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: []interface{}{7, 2, 2, 2, 1}}))
	})

	gg.It("should create a valid CDT Map and then execute Clear operations", func() {

		items := map[interface{}]interface{}{
			"Charlie": 55,
			"Jim":     98,
			"John":    76,
			"Harry":   82,
			"Sally":   79,
			"Lenny":   84,
			"Abe":     88,
		}

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			as.MapPutItemsOp(as.DefaultMapPolicy(), cdtBinName, items),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapClearOp(cdtBinName),
			as.MapSizeOp(cdtBinName),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: []interface{}{nil, 0}}))
	})

	gg.It("should create a valid CDT Map and then execute RANK operations", func() {

		items := map[interface{}]interface{}{
			"p1": 0,
			"p2": 0,
			"p3": 0,
			"p4": 0,
		}

		mapPolicy := as.NewMapPolicy(as.MapOrder.KEY_VALUE_ORDERED, as.MapWriteMode.UPDATE)

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			as.MapPutItemsOp(mapPolicy, cdtBinName, items),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

		_, err = client.Operate(wpolicy, key,
			as.MapIncrementOp(mapPolicy, cdtBinName, "p1", 10),
			as.MapIncrementOp(mapPolicy, cdtBinName, "p2", 20),
			as.MapIncrementOp(mapPolicy, cdtBinName, "p3", 1),
			as.MapIncrementOp(mapPolicy, cdtBinName, "p4", 20),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapGetByRankRangeCountOp(cdtBinName, -3, 3, as.MapReturnType.KEY),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: []interface{}{"p1", "p2", "p4"}}))

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapRemoveByValueOp(cdtBinName, 10, as.MapReturnType.KEY),
			as.MapGetByRankRangeCountOp(cdtBinName, -3, 3, as.MapReturnType.KEY),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cdtMap.Bins).To(gm.Equal(as.BinMap{cdtBinName: []interface{}{[]interface{}{"p1"}, []interface{}{"p3", "p2", "p4"}}}))
	})

	gg.It("should support MapWriteFlagsPartial & MapWriteFlagsNoFail", func() {
		client.Delete(nil, key)

		cdtBinName2 := cdtBinName + "2"
		items := map[interface{}]interface{}{
			0: 17,
			4: 2,
			5: 15,
			9: 10,
		}

		mapPolicy := as.DefaultMapPolicy()

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			as.MapPutItemsOp(mapPolicy, cdtBinName, items),
			as.MapPutItemsOp(mapPolicy, cdtBinName2, items),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(2))

		cdtMapPolicy1 := as.NewMapPolicyWithFlags(as.MapOrder.UNORDERED, as.MapWriteFlagsCreateOnly|as.MapWriteFlagsPartial|as.MapWriteFlagsNoFail)
		cdtMapPolicy2 := as.NewMapPolicyWithFlags(as.MapOrder.UNORDERED, as.MapWriteFlagsCreateOnly|as.MapWriteFlagsNoFail)

		items = map[interface{}]interface{}{
			3: 3,
			5: 15,
		}

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapPutItemsOp(cdtMapPolicy1, cdtBinName, items),
			as.MapPutItemsOp(cdtMapPolicy2, cdtBinName2, items),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal(5))
		gm.Expect(cdtMap.Bins[cdtBinName2]).To(gm.Equal(4))

	})

	gg.It("should support Map Infinity ops", func() {
		client.Delete(nil, key)

		items := map[interface{}]interface{}{
			0: 17,
			4: 2,
			5: 15,
			9: 10,
		}

		mapPolicy := as.DefaultMapPolicy()

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			as.MapPutItemsOp(mapPolicy, cdtBinName, items),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapGetByKeyRangeOp(cdtBinName, 5, as.NewInfinityValue(), as.MapReturnType.KEY),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal([]interface{}{5, 9}))
	})

	gg.It("should support Map WildCard ops", func() {
		client.Delete(nil, key)

		items := map[interface{}]interface{}{
			4: []interface{}{"John", 55},
			5: []interface{}{"Jim", 95},
			9: []interface{}{"Joe", 80},
		}

		mapPolicy := as.DefaultMapPolicy()

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			as.MapPutItemsOp(mapPolicy, cdtBinName, items),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapGetByValueOp(cdtBinName, []interface{}{"Joe", as.NewWildCardValue()}, as.MapReturnType.KEY),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal([]interface{}{9}))
	})

	gg.It("should support Relative MapGet ops", func() {
		client.Delete(nil, key)

		items := map[interface{}]interface{}{
			0: 17,
			4: 2,
			5: 15,
			9: 10,
		}

		mapPolicy := as.DefaultMapPolicy()

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			as.MapPutItemsOp(mapPolicy, cdtBinName, items),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapGetByKeyRelativeIndexRangeOp(cdtBinName, 5, 0, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeOp(cdtBinName, 5, 1, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeOp(cdtBinName, 5, -1, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeOp(cdtBinName, 3, 2, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeOp(cdtBinName, 3, -2, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeCountOp(cdtBinName, 5, 0, 1, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeCountOp(cdtBinName, 5, 1, 2, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeCountOp(cdtBinName, 5, -1, 1, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeCountOp(cdtBinName, 3, 2, 1, as.MapReturnType.KEY),
			as.MapGetByKeyRelativeIndexRangeCountOp(cdtBinName, 3, -2, 2, as.MapReturnType.KEY),
			as.MapGetByValueRelativeRankRangeOp(cdtBinName, 11, 1, as.MapReturnType.VALUE),
			as.MapGetByValueRelativeRankRangeOp(cdtBinName, 11, -1, as.MapReturnType.VALUE),
			as.MapGetByValueRelativeRankRangeCountOp(cdtBinName, 11, 1, 1, as.MapReturnType.VALUE),
			as.MapGetByValueRelativeRankRangeCountOp(cdtBinName, 11, -1, 1, as.MapReturnType.VALUE),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal([]interface{}{[]interface{}{5, 9}, []interface{}{9}, []interface{}{4, 5, 9}, []interface{}{9}, []interface{}{0, 4, 5, 9}, []interface{}{5}, []interface{}{9}, []interface{}{4}, []interface{}{9}, []interface{}{0}, []interface{}{17}, []interface{}{10, 15, 17}, []interface{}{17}, []interface{}{10}}))
	})

	gg.It("should support Relative MapRemove ops", func() {
		client.Delete(nil, key)

		items := map[interface{}]interface{}{
			0: 17,
			4: 2,
			5: 15,
			9: 10,
		}

		mapPolicy := as.DefaultMapPolicy()

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			as.MapPutItemsOp(mapPolicy, cdtBinName, items),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapRemoveByKeyRelativeIndexRangeOp(cdtBinName, 5, 0, as.MapReturnType.VALUE),
			as.MapRemoveByKeyRelativeIndexRangeOp(cdtBinName, 5, 1, as.MapReturnType.VALUE),
			as.MapRemoveByKeyRelativeIndexRangeCountOp(cdtBinName, 5, -1, 1, as.MapReturnType.VALUE),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal([]interface{}{[]interface{}{15, 10}, []interface{}{}, []interface{}{2}}))

		client.Delete(nil, key)
		cdtMap, err = client.Operate(wpolicy, key,
			as.MapPutItemsOp(mapPolicy, cdtBinName, items),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(len(cdtMap.Bins)).To(gm.Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			as.MapRemoveByValueRelativeRankRangeOp(cdtBinName, 11, 1, as.MapReturnType.VALUE),
			as.MapRemoveByValueRelativeRankRangeCountOp(cdtBinName, 11, -1, 1, as.MapReturnType.VALUE),
		)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(cdtMap.Bins[cdtBinName]).To(gm.Equal([]interface{}{[]interface{}{17}, []interface{}{10}}))
	})

	gg.It("should support Nested Map ops", func() {
		client.Delete(nil, key)

		m := map[interface{}]interface{}{
			"key1": map[interface{}]interface{}{
				"key11": 9, "key12": 4,
			},
			"key2": map[interface{}]interface{}{
				"key21": 3, "key22": 5,
			},
		}

		err := client.Put(wpolicy, key, as.BinMap{cdtBinName: m})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		record, err := client.Operate(wpolicy, key, as.GetOpForBin(cdtBinName))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(record.Bins[cdtBinName]).To(gm.Equal(m))

		record, err = client.Operate(wpolicy, key, as.MapPutOp(as.DefaultMapPolicy(), cdtBinName, as.StringValue("key21"), as.IntegerValue(11), as.CtxMapKey(as.StringValue("key2"))), as.GetOpForBin(cdtBinName))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		gm.Expect(record.Bins[cdtBinName]).To(gm.Equal([]interface{}{
			2,
			map[interface{}]interface{}{
				"key1": map[interface{}]interface{}{
					"key11": 9, "key12": 4,
				},
				"key2": map[interface{}]interface{}{
					"key21": 11, "key22": 5,
				},
			},
		}))
	})

	gg.It("should support Double Nested Map ops", func() {
		client.Delete(nil, key)

		m := map[interface{}]interface{}{
			"key1": map[interface{}]interface{}{
				"key11": map[interface{}]interface{}{"key111": 1}, "key12": map[interface{}]interface{}{"key121": 5},
			},
			"key2": map[interface{}]interface{}{
				"key21": map[interface{}]interface{}{"key211": 7},
			},
		}

		err := client.Put(wpolicy, key, as.BinMap{cdtBinName: m})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		record, err := client.Operate(wpolicy, key, as.GetOpForBin(cdtBinName))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(record.Bins[cdtBinName]).To(gm.Equal(m))

		record, err = client.Operate(wpolicy, key, as.MapPutOp(as.DefaultMapPolicy(), cdtBinName, as.StringValue("key121"), as.IntegerValue(11), as.CtxMapKey(as.StringValue("key1")), as.CtxMapRank(-1)), as.GetOpForBin(cdtBinName))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		gm.Expect(record.Bins[cdtBinName]).To(gm.Equal([]interface{}{
			1,
			map[interface{}]interface{}{
				"key1": map[interface{}]interface{}{
					"key11": map[interface{}]interface{}{"key111": 1}, "key12": map[interface{}]interface{}{"key121": 11},
				},
				"key2": map[interface{}]interface{}{
					"key21": map[interface{}]interface{}{"key211": 7},
				},
			},
		}))
	})

	gg.It("should handle CDTs in UDFs", func() {

		regTsk, err := client.RegisterUDF(nil, []byte(udfCDTTests), "cdt_tests.lua", as.LUA)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		gm.Expect(<-regTsk.OnComplete()).ToNot(gm.HaveOccurred())

		_, err = client.Execute(nil, key, "cdt_tests", "add", as.NewValue("b"), as.NewValue("k"), as.NewValue(1))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		sets, err := client.ScanAll(nil, ns, "skill")
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"b": map[interface{}]interface{}{"k": map[interface{}]interface{}{1: "k"}}}))

		for res := range sets.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			_, err = client.Delete(nil, res.Record.Key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}

		_, err = client.Execute(nil, key, "cdt_tests", "add", as.NewValue("b"), as.NewValue("k"), as.NewValue(1))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		_, err = client.Execute(nil, key, "cdt_tests", "remove", as.NewValue("b"), as.NewValue("k"), as.NewValue(1))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		rec, err = client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"b": map[interface{}]interface{}{"k": map[interface{}]interface{}{}}}))

		sets, err = client.ScanAll(nil, ns, set)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		for res := range sets.Results() {
			gm.Expect(res.Err).ToNot(gm.HaveOccurred())
			_, err = client.Delete(nil, res.Record.Key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		}
	})

	gg.It("should handle Map Create gg.Context", func() {
		// Key key = new Key(args.namespace, args.set, "opmkey22");
		client.Delete(nil, key)

		m1 := map[string]int{"key11": 9, "key12": 4}
		m2 := map[string]int{"key21": 3, "key22": 5}
		inputMap := map[string]interface{}{"key1": m1, "key2": m2}

		// Create maps.
		err := client.Put(nil, key, as.BinMap{cdtBinName: inputMap})
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// Set map value to 11 for map key "key21" inside of map key "key2"
		// and retrieve all maps.
		record, err := client.Operate(nil, key,
			as.MapCreateOp(cdtBinName, as.MapOrder.KEY_ORDERED, []*as.CDTContext{as.CtxMapKey(as.StringValue("key3"))}),
			as.MapPutOp(as.DefaultMapPolicy(), cdtBinName, "key31", 99, as.CtxMapKey(as.StringValue("key3"))),
			as.GetOpForBin(cdtBinName),
		)

		gm.Expect(err).ToNot(gm.HaveOccurred())

		results := record.Bins[cdtBinName].([]interface{})

		count := results[1]
		gm.Expect(count).To(gm.Equal(1))

		m := results[2].(map[interface{}]interface{})
		gm.Expect(len(m)).To(gm.Equal(3))

		mp := m["key3"].([]as.MapPair)

		gm.Expect(len(mp)).To(gm.Equal(1))
		gm.Expect(mp[0].Key).To(gm.Equal("key31"))
		gm.Expect(mp[0].Value).To(gm.Equal(99))
	})

}) // describe
