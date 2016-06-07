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
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/aerospike/aerospike-client-go"
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

var _ = Describe("CDT Map Test", func() {
	initTestVars()

	// connection data
	var client *Client
	var err error
	var ns = "test"
	var set = randString(50)
	var key *Key
	var wpolicy = NewWritePolicy(0, 0)
	var cdtBinName string
	// var list []interface{}

	putMode := DefaultMapPolicy()
	addMode := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.CREATE_ONLY)
	updateMode := NewMapPolicy(MapOrder.UNORDERED, MapWriteMode.UPDATE_ONLY)
	orderedUpdateMode := NewMapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE_ONLY)

	BeforeEach(func() {
		client, err = NewClientWithPolicy(clientPolicy, *host, *port)
		Expect(err).ToNot(HaveOccurred())
		key, err = NewKey(ns, set, randString(50))
		Expect(err).ToNot(HaveOccurred())

		cdtBinName = randString(10)
	})

	Describe("Simple Usecases", func() {

		It("should create a valid CDT Map using MapPutOp", func() {
			cdtMap, err := client.Operate(wpolicy, key,
				MapPutOp(putMode, cdtBinName, 1, 1),
				MapPutOp(putMode, cdtBinName, 2, 2),
				MapPutOp(addMode, cdtBinName, 3, 3),
				MapPutOp(addMode, cdtBinName, 4, 4),
				MapPutOp(addMode, cdtBinName, 6, 6),
				MapPutOp(addMode, cdtBinName, 7, 7),
				MapPutOp(addMode, cdtBinName, 8, 8),
			)
			Expect(err).ToNot(HaveOccurred())

			cdtMap, err = client.Get(nil, key, cdtBinName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins[cdtBinName]).To(Equal(map[interface{}]interface{}{1: 1, 2: 2, 3: 3, 4: 4, 6: 6, 7: 7, 8: 8}))
		})

		It("should unpack an empty Non-Ordered CDT map correctly", func() {
			cdtMap, err := client.Operate(wpolicy, key,
				MapPutOp(putMode, cdtBinName, 1, 1),
				MapRemoveByKeyOp(cdtBinName, 1, MapReturnType.NONE),
			)
			Expect(err).ToNot(HaveOccurred())

			cdtMap, err = client.Get(nil, key, cdtBinName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins[cdtBinName]).To(Equal(map[interface{}]interface{}{}))
		})

		It("should unpack an empty Ordered CDT map correctly", func() {
			_, err := client.Operate(wpolicy, key,
				MapPutOp(NewMapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), cdtBinName, 1, 1),
				// MapRemoveByKeyOp(cdtBinName, 1, MapReturnType.NONE),
			)
			Expect(err).ToNot(HaveOccurred())

			// cdtMap, err = client.Get(nil, key, cdtBinName)
			// Expect(err).ToNot(HaveOccurred())
			// Expect(cdtMap.Bins[cdtBinName]).To(Equal([]MapPair{}))

			rs, err := client.ScanAll(nil, ns, set)
			Expect(err).ToNot(HaveOccurred())
			for rs := range rs.Results() {
				Expect(rs.Err).ToNot(HaveOccurred())

			}

		})

		It("should create a valid CDT Map using MapPutOp", func() {
			cdtMap, err := client.Operate(wpolicy, key,
				MapPutOp(putMode, cdtBinName, 1, 1),
				MapPutOp(putMode, cdtBinName, 2, 2),
				MapPutOp(addMode, cdtBinName, 3, 3),
				MapPutOp(addMode, cdtBinName, 4, 4),

				// OrderedUpdate should be ignored since the map has been created already
				MapPutOp(orderedUpdateMode, cdtBinName, 5, 5),

				MapPutOp(updateMode, cdtBinName, 6, 6),
				GetOpForBin(cdtBinName),
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap).NotTo(Equal([]interface{}{1, 2, 3, 4, 4, 4, map[interface{}]interface{}{1: 1, 2: 2, 3: 3, 4: 4}}))

			cdtMap, err = client.Get(nil, key, cdtBinName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins[cdtBinName]).To(Equal(map[interface{}]interface{}{1: 1, 2: 2, 3: 3, 4: 4}))
		})

		It("should create a valid CDT Map using MapPutItemsOp", func() {
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
				14: "str14",
			}

			replaceMap := map[interface{}]interface{}{
				12:    23,
				-8734: "changed",
			}

			putMode := DefaultMapPolicy()
			addMode := NewMapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.CREATE_ONLY)
			updateMode := NewMapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE_ONLY)

			cdtMap, err := client.Operate(wpolicy, key,
				MapPutItemsOp(addMode, cdtBinName, addMap),
				MapPutItemsOp(putMode, cdtBinName, putMap),
				MapPutItemsOp(updateMode, cdtBinName, updateMap),
				MapPutItemsOp(updateMode, cdtBinName, replaceMap),
				MapGetByKeyOp(cdtBinName, 1, MapReturnType.VALUE),
				MapGetByKeyOp(cdtBinName, -8734, MapReturnType.VALUE),
				MapGetByKeyRangeOp(cdtBinName, 12, 15, MapReturnType.KEY_VALUE),
				// GetOpForBin(cdtBinName),
			)

			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins[cdtBinName]).To(Equal([]interface{}{3, 4, 4, 4, "my default", "changed", []MapPair{MapPair{Key: 12, Value: 23}, MapPair{Key: 13, Value: "myval2"}}}))

			cdtMap, err = client.Get(nil, key, cdtBinName)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins[cdtBinName]).To(Equal([]MapPair{MapPair{Key: -8734, Value: "changed"}, MapPair{Key: 1, Value: "my default"}, MapPair{Key: 12, Value: 23}, MapPair{Key: 13, Value: "myval2"}}))
		})

		It("should create a valid CDT Map using mixed MapPutOp and MapPutItemsOp", func() {

			items := map[interface{}]interface{}{
				12:    "myval",
				-8734: "str2",
				1:     "my default",
				7:     1,
			}

			otherBinName := "other_bin"

			cdtMap, err := client.Operate(wpolicy, key,
				MapPutItemsOp(NewMapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE), cdtBinName, items),
				PutOp(NewBin(otherBinName, "head")),
			)

			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins[cdtBinName]).To(Equal(4))
			Expect(cdtMap.Bins).To(HaveKey(otherBinName))

			cdtMap, err = client.Operate(wpolicy, key,
				MapGetByKeyOp(cdtBinName, 12, MapReturnType.INDEX),
				AppendOp(NewBin(otherBinName, "...tail")),
				GetOpForBin(otherBinName),
			)

			cdtMap, err = client.Get(nil, key)
			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []MapPair{MapPair{Key: -8734, Value: "str2"}, MapPair{Key: 1, Value: "my default"}, MapPair{Key: 7, Value: 1}, MapPair{Key: 12, Value: "myval"}}, "other_bin": "head...tail"}))
		})

		It("should create a valid CDT Map and then Switch Policy For Order", func() {

			cdtMap, err := client.Operate(wpolicy, key,
				MapPutOp(DefaultMapPolicy(), cdtBinName, 4, 1),
				MapPutOp(DefaultMapPolicy(), cdtBinName, 3, 2),
				MapPutOp(DefaultMapPolicy(), cdtBinName, 2, 3),
				MapPutOp(DefaultMapPolicy(), cdtBinName, 1, 4),

				MapGetByIndexOp(cdtBinName, 2, MapReturnType.KEY_VALUE),
				MapGetByIndexRangeCountOp(cdtBinName, 0, 10, MapReturnType.KEY_VALUE),
			)

			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []interface{}{1, 2, 3, 4, []MapPair{MapPair{Key: 3, Value: 2}}, []MapPair{MapPair{Key: 1, Value: 4}, MapPair{Key: 2, Value: 3}, MapPair{Key: 3, Value: 2}, MapPair{Key: 4, Value: 1}}}}))

			cdtMap, err = client.Operate(wpolicy, key,
				MapSetPolicyOp(NewMapPolicy(MapOrder.KEY_ORDERED, MapWriteMode.UPDATE), cdtBinName),

				MapGetByKeyRangeOp(cdtBinName, 3, 5, MapReturnType.COUNT),
				MapGetByKeyRangeOp(cdtBinName, -5, 2, MapReturnType.KEY_VALUE),
				MapGetByIndexRangeCountOp(cdtBinName, 0, 10, MapReturnType.KEY_VALUE),
			)

			Expect(err).ToNot(HaveOccurred())
			Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []interface{}{interface{}(nil), 2, []MapPair{MapPair{Key: 1, Value: 4}}, []MapPair{MapPair{Key: 1, Value: 4}, MapPair{Key: 2, Value: 3}, MapPair{Key: 3, Value: 2}, MapPair{Key: 4, Value: 1}}}}))

		})

		It("should create a valid CDT Map and then apply Inc/Dec operations and Get Correct Values", func() {

			items := map[interface{}]interface{}{
				"Charlie": 55,
				"Jim":     98,
				"John":    76,
				"Harry":   82,
			}

			// Write values to empty map.
			cdtMap, err := client.Operate(wpolicy, key,
				MapPutItemsOp(DefaultMapPolicy(), cdtBinName, items),
			)

			Expect(err).ToNot(HaveOccurred())

			cdtMap, err = client.Get(nil, key)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(cdtMap.Bins)).To(Equal(1))

			cdtMap, err = client.Operate(wpolicy, key,
				MapIncrementOp(DefaultMapPolicy(), cdtBinName, "John", 5),
				MapDecrementOp(DefaultMapPolicy(), cdtBinName, "Jim", 4),
			)
			Expect(err).ToNot(HaveOccurred())

			cdtMap, err = client.Get(nil, key)
			Expect(err).ToNot(HaveOccurred())
			Expect(len(cdtMap.Bins)).To(Equal(1))

			cdtMap, err = client.Operate(nil, key,
				MapGetByRankRangeCountOp(cdtBinName, -2, 2, MapReturnType.KEY),
				MapGetByRankRangeCountOp(cdtBinName, 0, 2, MapReturnType.KEY_VALUE),
				MapGetByRankOp(cdtBinName, 0, MapReturnType.VALUE),
				MapGetByRankOp(cdtBinName, 2, MapReturnType.KEY),
				MapGetByValueRangeOp(cdtBinName, 90, 95, MapReturnType.RANK),
				MapGetByValueRangeOp(cdtBinName, 90, 95, MapReturnType.COUNT),
				MapGetByValueRangeOp(cdtBinName, 90, 95, MapReturnType.KEY_VALUE),
				MapGetByValueRangeOp(cdtBinName, 81, 82, MapReturnType.KEY),
				MapGetByValueOp(cdtBinName, 77, MapReturnType.KEY),
				MapGetByValueOp(cdtBinName, 81, MapReturnType.RANK),
				MapGetByKeyOp(cdtBinName, "Charlie", MapReturnType.RANK),
				MapGetByKeyOp(cdtBinName, "Charlie", MapReturnType.REVERSE_RANK),
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []interface{}{"Harry", "Jim", []MapPair{MapPair{Key: "Charlie", Value: 55}, MapPair{Key: "John", Value: 81}}, 55, "Harry", []interface{}{3}, 1, []MapPair{MapPair{Key: "Jim", Value: 94}}, []interface{}{"John"}, []interface{}{}, []interface{}{1}, 0, 3}}))
		})

		It("should create a valid CDT Map and then execute Remove operations", func() {

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
				MapPutItemsOp(DefaultMapPolicy(), cdtBinName, items),
				MapRemoveByKeyOp(cdtBinName, "NOTFOUND", MapReturnType.VALUE),
				MapRemoveByKeyOp(cdtBinName, "Jim", MapReturnType.VALUE),
				MapRemoveByKeyListOp(cdtBinName, itemsToRemove, MapReturnType.VALUE),
				MapRemoveByValueOp(cdtBinName, 55, MapReturnType.KEY),
			)

			Expect(err).ToNot(HaveOccurred())

			Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []interface{}{7, nil, 98, []interface{}{79, 84}, []interface{}{"Charlie"}}}))
		})

	})

	It("should create a valid CDT Map and then execute RemoveRange operations", func() {

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
			MapPutItemsOp(DefaultMapPolicy(), cdtBinName, items),
			MapRemoveByKeyRangeOp(cdtBinName, "J", "K", MapReturnType.COUNT),
			MapRemoveByValueRangeOp(cdtBinName, 80, 85, MapReturnType.COUNT),
			MapRemoveByIndexRangeCountOp(cdtBinName, 0, 2, MapReturnType.COUNT),
			MapRemoveByRankRangeCountOp(cdtBinName, 0, 2, MapReturnType.COUNT),
		)

		Expect(err).ToNot(HaveOccurred())

		Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []interface{}{7, 2, 2, 2, 1}}))
	})

	It("should create a valid CDT Map and then execute Clear operations", func() {

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
			MapPutItemsOp(DefaultMapPolicy(), cdtBinName, items),
		)

		Expect(err).ToNot(HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		Expect(err).ToNot(HaveOccurred())
		Expect(len(cdtMap.Bins)).To(Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			MapClearOp(cdtBinName),
			MapSizeOp(cdtBinName),
		)

		Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []interface{}{nil, 0}}))
	})

	It("should create a valid CDT Map and then execute RANK operations", func() {

		items := map[interface{}]interface{}{
			"p1": 0,
			"p2": 0,
			"p3": 0,
			"p4": 0,
		}

		mapPolicy := NewMapPolicy(MapOrder.KEY_VALUE_ORDERED, MapWriteMode.UPDATE)

		// Write values to empty map.
		cdtMap, err := client.Operate(wpolicy, key,
			MapPutItemsOp(mapPolicy, cdtBinName, items),
		)

		Expect(err).ToNot(HaveOccurred())

		cdtMap, err = client.Get(nil, key)
		Expect(err).ToNot(HaveOccurred())
		Expect(len(cdtMap.Bins)).To(Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			MapIncrementOp(mapPolicy, cdtBinName, "p1", 10),
			MapIncrementOp(mapPolicy, cdtBinName, "p2", 20),
			MapIncrementOp(mapPolicy, cdtBinName, "p3", 1),
			MapIncrementOp(mapPolicy, cdtBinName, "p4", 20),
		)

		cdtMap, err = client.Get(nil, key)
		Expect(err).ToNot(HaveOccurred())
		Expect(len(cdtMap.Bins)).To(Equal(1))

		cdtMap, err = client.Operate(wpolicy, key,
			MapGetByRankRangeCountOp(cdtBinName, -3, 3, MapReturnType.KEY),
		)

		Expect(err).ToNot(HaveOccurred())
		Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []interface{}{"p1", "p2", "p4"}}))

		cdtMap, err = client.Operate(wpolicy, key,
			MapRemoveByValueOp(cdtBinName, 10, MapReturnType.KEY),
			MapGetByRankRangeCountOp(cdtBinName, -3, 3, MapReturnType.KEY),
		)

		Expect(err).ToNot(HaveOccurred())
		Expect(cdtMap.Bins).To(Equal(BinMap{cdtBinName: []interface{}{"p1", []interface{}{"p3", "p2", "p4"}}}))
	})

	It("should handle CDTs in UDFs", func() {

		regTsk, err := client.RegisterUDF(nil, []byte(udfCDTTests), "cdt_tests.lua", LUA)
		Expect(err).ToNot(HaveOccurred())

		Expect(<-regTsk.OnComplete()).ToNot(HaveOccurred())

		_, err = client.Execute(nil, key, "cdt_tests", "add", NewValue("b"), NewValue("k"), NewValue(1))
		Expect(err).ToNot(HaveOccurred())

		sets, err := client.ScanAll(nil, "test", "skill")
		Expect(err).ToNot(HaveOccurred())

		rec, err := client.Get(nil, key)
		Expect(err).ToNot(HaveOccurred())
		Expect(rec.Bins).To(Equal(BinMap{"b": map[interface{}]interface{}{"k": map[interface{}]interface{}{1: "k"}}}))

		for res := range sets.Results() {
			Expect(res.Err).ToNot(HaveOccurred())
			_, err = client.Delete(nil, res.Record.Key)
			Expect(err).ToNot(HaveOccurred())
		}

		_, err = client.Execute(nil, key, "cdt_tests", "add", NewValue("b"), NewValue("k"), NewValue(1))
		Expect(err).ToNot(HaveOccurred())

		_, err = client.Execute(nil, key, "cdt_tests", "remove", NewValue("b"), NewValue("k"), NewValue(1))
		Expect(err).ToNot(HaveOccurred())

		rec, err = client.Get(nil, key)
		Expect(err).ToNot(HaveOccurred())
		Expect(rec.Bins).To(Equal(BinMap{"b": map[interface{}]interface{}{"k": map[interface{}]interface{}{}}}))

		sets, err = client.ScanAll(nil, ns, set)
		Expect(err).ToNot(HaveOccurred())
		for res := range sets.Results() {
			Expect(res.Err).ToNot(HaveOccurred())
			_, err = client.Delete(nil, res.Record.Key)
			Expect(err).ToNot(HaveOccurred())
		}
	})

}) // describe
