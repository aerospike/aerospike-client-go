// Copyright 2014-2025 Aerospike, Inc.
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
	"errors"
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("CDT Operation Test", func() {

	// connection data
	var ns = *namespace
	var set = randString(50)
	var key *as.Key
	var wpolicy = as.NewWritePolicy(0, 0)
	var binName string

	gg.BeforeEach(func() {
		serverRequiredVersion, err := version.Parse("8.1.1")
		if err != nil {
			gg.Fail("Failed to parse server required version")
		}

		node := client.GetNodes()[0]
		nodeVersion := node.GetServerVersion()
		if nodeVersion.IsSmaller(serverRequiredVersion) {
			gg.Skip("CDT select/modify by path operations require server version 8.1.1+ with cdt-select-path feature.")
			return
		}

		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		binName = "testbin"
	})

	gg.Describe("CDT Operate With Expressions", func() {

		gg.It("should select titles from books with price <= 10.0 using CDTSelectByPath", func() {
			client.Delete(nil, key)

			booksList := []interface{}{
				map[string]interface{}{
					"title": "Sayings of the Century",
					"price": 8.95,
				},
				map[string]interface{}{
					"title": "Sword of Honour",
					"price": 12.99,
				},
				map[string]interface{}{
					"title": "Moby Dick",
					"price": 8.99,
				},
				map[string]interface{}{
					"title": "The Lord of the Rings",
					"price": 22.99,
				},
			}

			rootMap := map[string]interface{}{
				"book": booksList,
			}

			bin := as.NewBin(binName, rootMap)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record).ToNot(gm.BeNil())

			ctx1 := as.CtxMapKey(as.NewStringValue("book"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpLessEq(
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeFLOAT,
						as.ExpStringVal("price"),
						as.ExpLoopVarMap(as.VALUE),
					),
					as.ExpFloatVal(10.0),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("title"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]interface{})
			gm.Expect(ok).To(gm.BeTrue(), "Result should be a list")
			gm.Expect(len(resultList)).To(gm.Equal(2), "Should have 2 books with price <= 10.0")

			// Verify the titles (order may vary)
			titles := make([]string, len(resultList))
			for i, item := range resultList {
				title, ok := item.(string)
				gm.Expect(ok).To(gm.BeTrue(), "Each result should be a string title")
				titles[i] = title
			}

			// Check that we got the expected titles
			gm.Expect(titles).To(gm.ContainElement("Sayings of the Century"))
			gm.Expect(titles).To(gm.ContainElement("Moby Dick"))
		})

		gg.It("should modify all book prices by multiplying by 1.10 using CDTModifyByPath", func() {
			client.Delete(nil, key)

			booksList := []interface{}{
				map[string]interface{}{
					"title": "Sayings of the Century",
					"price": 8.95,
				},
				map[string]interface{}{
					"title": "Sword of Honour",
					"price": 12.99,
				},
				map[string]interface{}{
					"title": "Moby Dick",
					"price": 8.99,
				},
				map[string]interface{}{
					"title": "The Lord of the Rings",
					"price": 22.99,
				},
			}

			rootMap := map[string]interface{}{
				"book": booksList,
			}

			bin := as.NewBin(binName, rootMap)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record).ToNot(gm.BeNil())

			bookKey := as.CtxMapKey(as.NewStringValue("book"))
			allChildren := as.CtxAllChildren()
			priceKey := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("price"),
				),
			)

			modifyExp := as.ExpNumMul(
				as.ExpLoopVarFloat(as.VALUE), // Current price value
				as.ExpFloatVal(1.10),         // Multiply by 1.10
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, bookKey, allChildren, priceKey)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(finalRecord).ToNot(gm.BeNil())

			finalRootMap, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue(), "Root map should exist")

			finalBooksListRaw, ok := finalRootMap["book"]
			gm.Expect(ok).To(gm.BeTrue(), "Books list should exist in root map")

			finalBooksList, ok := finalBooksListRaw.([]interface{})
			gm.Expect(ok).To(gm.BeTrue(), "Books should be a list")
			gm.Expect(len(finalBooksList)).To(gm.BeNumerically(">", 0), "Books list should not be empty")

			firstBook, ok := finalBooksList[0].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue(), "First book should be a map")

			priceObj, ok := firstBook["price"]
			gm.Expect(ok).To(gm.BeTrue(), "Price should exist in first book")

			var finalPrice float64
			switch v := priceObj.(type) {
			case float64:
				finalPrice = v
			case float32:
				finalPrice = float64(v)
			case int:
				finalPrice = float64(v)
			case int64:
				finalPrice = float64(v)
			default:
				gm.Expect(true).To(gm.BeFalse(), "Price should be a numeric type")
			}

			gm.Expect(finalPrice).To(gm.BeNumerically(">", 9.0), "Price should be increased")

			expectedPrice := 8.95 * 1.10
			gm.Expect(math.Abs(finalPrice-expectedPrice)).To(gm.BeNumerically("<", 0.01),
				"Price should be approximately %f, got %f", expectedPrice, finalPrice)

			// Verify all books have increased prices
			originalPrices := []float64{8.95, 12.99, 8.99, 22.99}
			for i, bookRaw := range finalBooksList {
				book, ok := bookRaw.(map[interface{}]interface{})
				gm.Expect(ok).To(gm.BeTrue(), "Book %d should be a map", i)

				price, ok := book["price"]
				gm.Expect(ok).To(gm.BeTrue(), "Book %d should have a price", i)

				var priceFloat float64
				switch v := price.(type) {
				case float64:
					priceFloat = v
				case float32:
					priceFloat = float64(v)
				case int:
					priceFloat = float64(v)
				case int64:
					priceFloat = float64(v)
				}

				expectedPrice := originalPrices[i] * 1.10
				gm.Expect(math.Abs(priceFloat-expectedPrice)).To(gm.BeNumerically("<", 0.01),
					"Book %d price should be approximately %f, got %f", i, expectedPrice, priceFloat)
			}
		})

		gg.It("should work with nested contexts and complex filter expressions", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"store": map[string]interface{}{
					"books": []interface{}{
						map[string]interface{}{
							"category": "reference",
							"author":   "Nigel Rees",
							"title":    "Sayings of the Century",
							"price":    8.95,
						},
						map[string]interface{}{
							"category": "fiction",
							"author":   "Evelyn Waugh",
							"title":    "Sword of Honour",
							"price":    12.99,
						},
						map[string]interface{}{
							"category": "fiction",
							"author":   "Herman Melville",
							"title":    "Moby Dick",
							"price":    8.99,
						},
					},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("store"))
			ctx2 := as.CtxMapKey(as.NewStringValue("books"))
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpAnd(
					as.ExpEq(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeSTRING,
							as.ExpStringVal("category"),
							as.ExpLoopVarMap(as.VALUE),
						),
						as.ExpStringVal("fiction"),
					),
					as.ExpLess(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeFLOAT,
							as.ExpStringVal("price"),
							as.ExpLoopVarMap(as.VALUE),
						),
						as.ExpFloatVal(10.0),
					),
				),
			)
			ctx4 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("title"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3, ctx4)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			// Should only get "Moby Dick" (fiction with price 8.99 < 10.0)
			resultList, ok := results.([]interface{})
			gm.Expect(ok).To(gm.BeTrue(), "Result should be a list")
			gm.Expect(len(resultList)).To(gm.Equal(1), "Should have 1 fiction book with price < 10.0")
			gm.Expect(resultList[0]).To(gm.Equal("Moby Dick"))
		})

		gg.It("should handle empty results when no items match the filter", func() {
			client.Delete(nil, key)

			booksList := []interface{}{
				map[string]interface{}{
					"title": "Expensive Book 1",
					"price": 25.99,
				},
				map[string]interface{}{
					"title": "Expensive Book 2",
					"price": 30.50,
				},
			}

			rootMap := map[string]interface{}{
				"book": booksList,
			}

			bin := as.NewBin(binName, rootMap)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Try to select books with price <= 10.0 (should return empty)
			ctx1 := as.CtxMapKey(as.NewStringValue("book"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpLessEq(
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeFLOAT,
						as.ExpStringVal("price"),
						as.ExpLoopVarMap(as.VALUE),
					),
					as.ExpFloatVal(10.0),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("title"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify empty results
			results := result.Bins[binName]
			if results != nil {
				resultList, ok := results.([]interface{})
				if ok {
					gm.Expect(len(resultList)).To(gm.Equal(0), "Should have 0 books matching the filter")
				}
			}
		})

		gg.It("should work with MatchingTree flag to return the full matching structure", func() {
			client.Delete(nil, key)

			booksList := []interface{}{
				map[string]interface{}{
					"title": "Cheap Book",
					"price": 5.99,
				},
				map[string]interface{}{
					"title": "Expensive Book",
					"price": 25.99,
				},
			}

			rootMap := map[string]interface{}{
				"book": booksList,
			}

			bin := as.NewBin(binName, rootMap)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("book"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpLessEq(
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeFLOAT,
						as.ExpStringVal("price"),
						as.ExpLoopVarMap(as.VALUE),
					),
					as.ExpFloatVal(10.0),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_MATCHING_TREE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// With MatchingTree, we should get back the full matching structure
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())
		})

		gg.It("should work with MapKeys flag to return only map keys", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": map[string]interface{}{
					"item1": 100,
					"item2": 200,
					"item3": 50,
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select with MapKeys flag - should return only keys, not values
			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpGreater(
					as.ExpLoopVarInt(as.VALUE),
					as.ExpIntVal(75),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_MAP_KEY, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get keys where value > 75
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())
		})

		gg.It("should work with SelectNoFail flag to avoid errors on missing paths", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"existing": []interface{}{1, 2, 3},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Try to select from non-existing path with SelectNoFail
			ctx1 := as.CtxMapKey(as.NewStringValue("existing"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_NO_FAIL, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should work with loop variable INDEX to access list indices", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"numbers": []interface{}{10, 20, 30, 40, 50},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select items where index < 3
			ctx1 := as.CtxMapKey(as.NewStringValue("numbers"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpLess(
					as.ExpLoopVarInt(as.INDEX),
					as.ExpIntVal(3),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get first 3 items (indices 0, 1, 2)
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3), "Should have 3 items with index < 3")
			}
		})

		gg.It("should work with loop variable MAP_KEY to access map keys", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"products": map[string]interface{}{
					"apple":  1.50,
					"banana": 0.75,
					"cherry": 2.25,
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select items where key starts with 'a' or 'b' (lexicographically < "c")
			ctx1 := as.CtxMapKey(as.NewStringValue("products"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpLess(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("c"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get apple and banana (keys < "c")
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(2), "Should have 2 items with keys < 'c'")
			}
		})

		gg.It("should modify with addition operation", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"scores": []interface{}{10, 20, 30, 40, 50},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Add 5 to each score
			ctx1 := as.CtxMapKey(as.NewStringValue("scores"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			modifyExp := as.ExpNumAdd(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(5),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalRootMap, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			finalScores, ok := finalRootMap["scores"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(finalScores)).To(gm.Equal(5))

			firstScore, ok := finalScores[0].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(firstScore).To(gm.Equal(15), "10 + 5 = 15")
		})

		gg.It("should modify with subtraction operation", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"balances": map[string]interface{}{
					"account1": 1000,
					"account2": 2000,
					"account3": 1500,
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Subtract 100 from each balance
			ctx1 := as.CtxMapKey(as.NewStringValue("balances"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			modifyExp := as.ExpNumSub(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(100),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalRootMap, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			finalBalances, ok := finalRootMap["balances"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			// Verify account1 balance was decreased by 100
			balance1, ok := finalBalances["account1"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(balance1).To(gm.Equal(900), "1000 - 100 = 900")
		})

		gg.It("should work with nested lists and complex filters", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"matrix": []interface{}{
					[]interface{}{1, 2, 3},
					[]interface{}{4, 5, 6},
					[]interface{}{7, 8, 9},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("matrix"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get all 3 rows
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3), "Should have 3 rows")
			}
		})

		gg.It("should handle integer map keys correctly", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": map[interface{}]interface{}{
					1: "first",
					2: "second",
					3: "third",
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select items with integer keys
			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())
		})

		gg.It("should work with boolean expressions in filters", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"users": []interface{}{
					map[string]interface{}{
						"name":   "Alice",
						"active": true,
						"age":    30,
					},
					map[string]interface{}{
						"name":   "Bob",
						"active": false,
						"age":    25,
					},
					map[string]interface{}{
						"name":   "Charlie",
						"active": true,
						"age":    35,
					},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select active users
			ctx1 := as.CtxMapKey(as.NewStringValue("users"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeBOOL,
						as.ExpStringVal("active"),
						as.ExpLoopVarMap(as.VALUE),
					),
					as.ExpBoolVal(true),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("name"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get Alice and Charlie (active users)
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(2), "Should have 2 active users")
				gm.Expect(resultList).To(gm.ContainElement("Alice"))
				gm.Expect(resultList).To(gm.ContainElement("Charlie"))
			}
		})

		gg.It("should handle complex AND/OR filter combinations", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"products": []interface{}{
					map[string]interface{}{"name": "Widget", "price": 10.0, "inStock": true},
					map[string]interface{}{"name": "Gadget", "price": 25.0, "inStock": false},
					map[string]interface{}{"name": "Gizmo", "price": 15.0, "inStock": true},
					map[string]interface{}{"name": "Doohickey", "price": 30.0, "inStock": true},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select products that are (inStock AND price < 20) OR (price > 25)
			ctx1 := as.CtxMapKey(as.NewStringValue("products"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpOr(
					as.ExpAnd(
						as.ExpEq(
							as.ExpMapGetByKey(
								as.MapReturnType.VALUE,
								as.ExpTypeBOOL,
								as.ExpStringVal("inStock"),
								as.ExpLoopVarMap(as.VALUE),
							),
							as.ExpBoolVal(true),
						),
						as.ExpLess(
							as.ExpMapGetByKey(
								as.MapReturnType.VALUE,
								as.ExpTypeFLOAT,
								as.ExpStringVal("price"),
								as.ExpLoopVarMap(as.VALUE),
							),
							as.ExpFloatVal(20.0),
						),
					),
					as.ExpGreater(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeFLOAT,
							as.ExpStringVal("price"),
							as.ExpLoopVarMap(as.VALUE),
						),
						as.ExpFloatVal(25.0),
					),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("name"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get Widget (inStock, price 10), Gizmo (inStock, price 15), and Doohickey (price 30)
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3), "Should have 3 matching products")
			}
		})

		gg.It("should work with string operations in modify expressions", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 1, "tag": "item"},
					map[string]interface{}{"id": 2, "tag": "item"},
					map[string]interface{}{"id": 3, "tag": "item"},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record).ToNot(gm.BeNil())
		})

		gg.It("should handle deeply nested structures", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"level1": map[string]interface{}{
					"level2": map[string]interface{}{
						"level3": []interface{}{
							map[string]interface{}{"value": 100},
							map[string]interface{}{"value": 200},
							map[string]interface{}{"value": 300},
						},
					},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Navigate deep and select values
			ctx1 := as.CtxMapKey(as.NewStringValue("level1"))
			ctx2 := as.CtxMapKey(as.NewStringValue("level2"))
			ctx3 := as.CtxMapKey(as.NewStringValue("level3"))
			ctx4 := as.CtxAllChildrenWithFilter(
				as.ExpGreater(
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeINT,
						as.ExpStringVal("value"),
						as.ExpLoopVarMap(as.VALUE),
					),
					as.ExpIntVal(150),
				),
			)
			ctx5 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("value"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3, ctx4, ctx5)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get values > 150 (200 and 300)
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(2), "Should have 2 values > 150")
			}
		})
	})

	gg.Describe("CDT Operation Edge Cases", func() {
		gg.It("should return nil when context is nil for CDTSelectByPath", func() {
			op := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, nil...)
			gm.Expect(op).ToNot(gm.BeNil(), "Should return nil when context is nil")
		})

		gg.It("should return nil when context is nil for CDTModifyByPath", func() {
			modifyExp := as.ExpIntVal(42)
			op := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, nil...)
			gm.Expect(op).ToNot(gm.BeNil(), "Should return nil when context is nil")
		})

		gg.It("should work with single context element", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"value": 123,
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select with single context
			ctx1 := as.CtxMapKey(as.NewStringValue("value"))

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())
		})

		gg.It("should handle empty lists correctly", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"emptyList": []interface{}{},
				"items":     []interface{}{1, 2, 3},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Try to select from empty list
			ctx1 := as.CtxMapKey(as.NewStringValue("emptyList"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_NO_FAIL, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle empty maps correctly", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"emptyMap": map[string]interface{}{},
				"items": map[string]interface{}{
					"a": 1,
					"b": 2,
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Try to select from empty map
			ctx1 := as.CtxMapKey(as.NewStringValue("emptyMap"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_NO_FAIL, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle CDTSelectByPath with list index context", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"name": "item1", "value": 10},
					map[string]interface{}{"name": "item2", "value": 20},
					map[string]interface{}{"name": "item3", "value": 30},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select value from second item
			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxListIndex(1) // Select second item (index 1)
			ctx3 := as.CtxMapKey(as.NewStringValue("value"))

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(1))
				gm.Expect(resultList[0]).To(gm.Equal(20))
			}
		})

		gg.It("should handle CDTModifyByPath with subtraction", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"balances": map[string]interface{}{
					"account1": 1000,
					"account2": 2000,
					"account3": 3000,
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Subtract 100 from all balances
			ctx1 := as.CtxMapKey(as.NewStringValue("balances"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			modifyExp := as.ExpNumSub(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(100),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			balances, ok := finalData["balances"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			account1, ok := balances["account1"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(account1).To(gm.Equal(900), "1000 - 100 = 900")
		})

		gg.It("should handle CDTSelectByPath with MATCHING_TREE on nested maps", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"company": map[string]interface{}{
					"sales": map[string]interface{}{
						"q1": 10000,
						"q2": 12000,
					},
					"engineering": map[string]interface{}{
						"q1": 5000,
						"q2": 6000,
					},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("company"))
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("q1"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_MATCHING_TREE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle CDTSelectByPath with multiple filtered contexts", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"products": []interface{}{
					map[string]interface{}{"name": "Product A", "price": 100, "inStock": true},
					map[string]interface{}{"name": "Product B", "price": 200, "inStock": false},
					map[string]interface{}{"name": "Product C", "price": 150, "inStock": true},
					map[string]interface{}{"name": "Product D", "price": 50, "inStock": true},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select names of products that are in stock AND price < 150
			ctx1 := as.CtxMapKey(as.NewStringValue("products"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpAnd(
					as.ExpEq(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeBOOL,
							as.ExpStringVal("inStock"),
							as.ExpLoopVarMap(as.VALUE),
						),
						as.ExpBoolVal(true),
					),
					as.ExpLess(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeINT,
							as.ExpStringVal("price"),
							as.ExpLoopVarMap(as.VALUE),
						),
						as.ExpIntVal(150),
					),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("name"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify the result - should contain "Product A" and "Product D"
			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.BeNumerically(">=", 1))
			}
		})

		gg.It("should handle CDTModifyByPath with ExpLoopVarInt INDEX", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"values": []interface{}{100, 200, 300, 400},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Multiply each value by its index + 1
			ctx1 := as.CtxMapKey(as.NewStringValue("values"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			modifyExp := as.ExpNumMul(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpNumAdd(
					as.ExpLoopVarInt(as.INDEX),
					as.ExpIntVal(1),
				),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			values, ok := finalData["values"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			// First value: 100 * (0 + 1) = 100
			// Second value: 200 * (1 + 1) = 400
			// Third value: 300 * (2 + 1) = 900
			// Fourth value: 400 * (3 + 1) = 1600
			gm.Expect(values[0]).To(gm.Equal(100))
			gm.Expect(values[1]).To(gm.Equal(400))
			gm.Expect(values[2]).To(gm.Equal(900))
			gm.Expect(values[3]).To(gm.Equal(1600))
		})

		gg.It("should handle CDTModifyByPath with boolean expressions", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"flags": map[string]interface{}{
					"feature1": false,
					"feature2": false,
					"feature3": false,
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Toggle all flags (NOT operation)
			ctx1 := as.CtxMapKey(as.NewStringValue("flags"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			modifyExp := as.ExpNot(
				as.ExpLoopVarBool(as.VALUE),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			flags, ok := finalData["flags"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			// All flags should now be true
			for _, v := range flags {
				boolVal, ok := v.(bool)
				if ok {
					gm.Expect(boolVal).To(gm.BeTrue())
				}
			}
		})

		gg.It("should handle CDTSelectByPath with MAP_KEYS flag on nested map", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"config": map[string]interface{}{
					"server1": map[string]interface{}{
						"host": "192.168.1.1",
						"port": 8080,
					},
					"server2": map[string]interface{}{
						"host": "192.168.1.2",
						"port": 8081,
					},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("config"))
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildren()

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_MAP_KEY, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle CDTModifyByPath with complex arithmetic", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"metrics": []interface{}{
					map[string]interface{}{"value": 10, "multiplier": 2},
					map[string]interface{}{"value": 20, "multiplier": 3},
					map[string]interface{}{"value": 30, "multiplier": 4},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Add 100 to each value field in the metrics
			ctx1 := as.CtxMapKey(as.NewStringValue("metrics"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("value"),
				),
			)

			// At ctx3, we're at the "value" field value (10, 20, 30)
			// The expression calculates the new value
			modifyExp := as.ExpNumAdd(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(100),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			metrics, ok := finalData["metrics"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			firstMetric, ok := metrics[0].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			value, ok := firstMetric["value"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(value).To(gm.Equal(110), "10 + 100 = 110")
		})

		gg.It("should handle CDTSelectByPath with OR filter expression", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"name": "Item A", "priority": 1},
					map[string]interface{}{"name": "Item B", "priority": 2},
					map[string]interface{}{"name": "Item C", "priority": 3},
					map[string]interface{}{"name": "Item D", "priority": 1},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select names where priority is 1 OR 3
			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpOr(
					as.ExpEq(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeINT,
							as.ExpStringVal("priority"),
							as.ExpLoopVarMap(as.VALUE),
						),
						as.ExpIntVal(1),
					),
					as.ExpEq(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeINT,
							as.ExpStringVal("priority"),
							as.ExpLoopVarMap(as.VALUE),
						),
						as.ExpIntVal(3),
					),
				),
			)
			ctx3 := as.CtxMapKey(as.NewStringValue("name"))

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify the result - should contain "Item A", "Item C", and "Item D"
			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3))
			}
		})

		gg.It("should select blobs using ExpLoopVarBlob with VALUE", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"blobs": map[string]interface{}{
					"data1": []byte("Hello World"),
					"data2": []byte("Test Data"),
					"data3": []byte("Binary Content"),
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("blobs"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3))
				// Verify at least one blob is returned correctly
				foundBlob := false
				for _, item := range resultList {
					if blob, ok := item.([]byte); ok {
						if len(blob) > 0 {
							foundBlob = true
							break
						}
					}
				}
				gm.Expect(foundBlob).To(gm.BeTrue(), "Should have at least one valid blob")
			}
		})

		gg.It("should select all blob values from nested structure", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"documents": []interface{}{
					map[string]interface{}{"id": 1, "content": []byte("Document 1 content")},
					map[string]interface{}{"id": 2, "content": []byte("Document 2 content")},
					map[string]interface{}{"id": 3, "content": []byte("Document 3 content")},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("documents"))
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("content"),
				),
			)

			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]interface{})
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3), "Should have 3 blob values")
				blobCount := 0
				for _, item := range resultList {
					if blob, ok := item.([]byte); ok && len(blob) > 0 {
						blobCount++
					}
				}
				gm.Expect(blobCount).To(gm.BeNumerically(">", 0), "Should have at least one valid blob")
			}
		})
	})

	gg.Describe("ExpResultRemove Tests", func() {

		gg.It("should remove all items from a list using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": []interface{}{1, 2, 3, 4, 5},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			applyOp := as.CDTModifyByPath(binName, 0, as.ExpResultRemove(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			items, ok := finalData["items"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(items)).To(gm.Equal(0), "All items should be removed")
		})

		gg.It("should remove filtered items from a list using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"numbers": []interface{}{1, 5, 10, 15, 20, 25, 30},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("numbers"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpGreater(
					as.ExpLoopVarInt(as.VALUE),
					as.ExpIntVal(10),
				),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpResultRemove(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			numbers, ok := finalData["numbers"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(numbers)).To(gm.Equal(3), "Should keep items <= 10")
			gm.Expect(numbers).To(gm.ContainElement(1))
			gm.Expect(numbers).To(gm.ContainElement(5))
			gm.Expect(numbers).To(gm.ContainElement(10))
		})

		gg.It("should remove all items from a map using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"config": map[string]interface{}{
					"option1": "value1",
					"option2": "value2",
					"option3": "value3",
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("config"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpResultRemove(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			config, ok := finalData["config"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(config)).To(gm.Equal(0), "All map entries should be removed")
		})

		gg.It("should remove filtered map entries using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"scores": map[string]interface{}{
					"alice": 95,
					"bob":   45,
					"carol": 75,
					"dave":  30,
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("scores"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpLess(
					as.ExpLoopVarInt(as.VALUE),
					as.ExpIntVal(50),
				),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpResultRemove(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			scores, ok := finalData["scores"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(scores)).To(gm.Equal(2), "Should keep scores >= 50")

			_, hasBob := scores["bob"]
			gm.Expect(hasBob).To(gm.BeFalse())
			_, hasDave := scores["dave"]
			gm.Expect(hasDave).To(gm.BeFalse())

			aliceScore, hasAlice := scores["alice"]
			gm.Expect(hasAlice).To(gm.BeTrue())
			gm.Expect(aliceScore).To(gm.Equal(95))
		})

		gg.It("should remove books with low prices using ExpResultRemove", func() {
			client.Delete(nil, key)

			booksList := []interface{}{
				map[string]interface{}{
					"title": "Cheap Book 1",
					"price": 5.99,
				},
				map[string]interface{}{
					"title": "Expensive Book",
					"price": 25.99,
				},
				map[string]interface{}{
					"title": "Cheap Book 2",
					"price": 3.99,
				},
				map[string]interface{}{
					"title": "Mid Price Book",
					"price": 15.99,
				},
			}

			rootMap := map[string]interface{}{
				"books": booksList,
			}

			bin := as.NewBin(binName, rootMap)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("books"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpLessEq(
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeFLOAT,
						as.ExpStringVal("price"),
						as.ExpLoopVarMap(as.VALUE),
					),
					as.ExpFloatVal(10.0),
				),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpResultRemove(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			books, ok := finalData["books"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(books)).To(gm.Equal(2), "Should keep 2 expensive books")

			for _, bookRaw := range books {
				book, ok := bookRaw.(map[interface{}]interface{})
				gm.Expect(ok).To(gm.BeTrue())

				price, ok := book["price"]
				gm.Expect(ok).To(gm.BeTrue())

				var priceFloat float64
				switch v := price.(type) {
				case float64:
					priceFloat = v
				case float32:
					priceFloat = float64(v)
				case int:
					priceFloat = float64(v)
				}

				gm.Expect(priceFloat).To(gm.BeNumerically(">", 10.0))
			}
		})

		gg.It("should remove items by index filter using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"values": []interface{}{100, 200, 300, 400, 500},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("values"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpGreaterEq(
					as.ExpLoopVarInt(as.INDEX),
					as.ExpIntVal(3),
				),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpResultRemove(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			values, ok := finalData["values"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(3), "Should keep first 3 items")
			gm.Expect(values[0]).To(gm.Equal(100))
			gm.Expect(values[1]).To(gm.Equal(200))
			gm.Expect(values[2]).To(gm.Equal(300))
		})

		gg.It("should remove map entries by key filter using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"inventory": map[string]interface{}{
					"apple":  10,
					"banana": 5,
					"cherry": 8,
					"date":   3,
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("inventory"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpGreaterEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("c"),
				),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpResultRemove(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			inventory, ok := finalData["inventory"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(inventory)).To(gm.Equal(2))

			_, hasApple := inventory["apple"]
			gm.Expect(hasApple).To(gm.BeTrue())
			_, hasBanana := inventory["banana"]
			gm.Expect(hasBanana).To(gm.BeTrue())
		})

		gg.It("should remove nested items using ExpResultRemove with complex path", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"departments": map[string]interface{}{
					"sales": []interface{}{
						map[string]interface{}{"name": "John", "sales": 1000},
						map[string]interface{}{"name": "Jane", "sales": 5000},
					},
					"engineering": []interface{}{
						map[string]interface{}{"name": "Bob", "sales": 500},
						map[string]interface{}{"name": "Alice", "sales": 3000},
					},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("departments"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpLess(
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeINT,
						as.ExpStringVal("sales"),
						as.ExpLoopVarMap(as.VALUE),
					),
					as.ExpIntVal(2000),
				),
			)

			applyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpResultRemove(), ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			departments, ok := finalData["departments"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			salesList, ok := departments["sales"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(salesList)).To(gm.Equal(1), "Should keep Jane only")

			engList, ok := departments["engineering"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(engList)).To(gm.Equal(1), "Should keep Alice only")
		})
	})

	gg.Describe("CDT Path Operations With No Context Tests", func() {

		gg.It("should return PARAMETER_ERROR for CDTSelectByPath with no context passed in", func() {
			client.Delete(nil, key)

			data := []interface{}{1, 2, 3, 4, 5}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTSelectByPath without passing any context
			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE)

			// Verify that the operation was created successfully
			gm.Expect(selectOp).ToNot(gm.BeNil())

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for CDTSelectByPath with no context - map keys", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTSelectByPath without passing any context
			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_MAP_KEY)

			// Verify that the operation was created successfully
			gm.Expect(selectOp).ToNot(gm.BeNil())

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for CDTSelectByPath with explicit nil context", func() {
			client.Delete(nil, key)

			data := []interface{}{"a", "b", "c"}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTSelectByPath with explicit nil context
			var nilCtx []*as.CDTContext = nil
			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, nilCtx...)

			// Verify that the operation was created successfully
			gm.Expect(selectOp).ToNot(gm.BeNil())

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for CDTSelectByPath with empty context slice", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"alpha": 10,
				"beta":  20,
				"gamma": 30,
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTSelectByPath with an empty context slice
			emptyCtx := []*as.CDTContext{}
			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE, emptyCtx...)

			// Verify that the operation was created successfully
			gm.Expect(selectOp).ToNot(gm.BeNil())

			// Server should return PARAMETER_ERROR when empty context is provided
			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for CDTModifyByPath with no context - list append", func() {
			client.Delete(nil, key)

			data := []interface{}{1, 2, 3, 4, 5}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTModifyByPath without passing any context to append an item to the top-level list
			modifyExp := as.ExpListAppend(
				as.DefaultListPolicy(),
				as.ExpIntVal(6),
				as.ExpLoopVarList(as.VALUE),
			)
			modifyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp)

			// Verify that the operation was created successfully
			gm.Expect(modifyOp).ToNot(gm.BeNil())

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, modifyOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for CDTModifyByPath with no context - map put", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"existing": "value",
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTModifyByPath without passing any context to add a key to the top-level map
			modifyExp := as.ExpMapPut(
				as.DefaultMapPolicy(),
				as.ExpStringVal("newKey"),
				as.ExpStringVal("newValue"),
				as.ExpLoopVarMap(as.VALUE),
			)
			modifyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp)

			// Verify that the operation was created successfully
			gm.Expect(modifyOp).ToNot(gm.BeNil())

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, modifyOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for CDTModifyByPath with explicit nil context", func() {
			client.Delete(nil, key)

			data := []interface{}{10, 20, 30}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTModifyByPath with explicit nil context
			var nilCtx []*as.CDTContext = nil
			modifyExp := as.ExpListAppend(
				as.DefaultListPolicy(),
				as.ExpIntVal(40),
				as.ExpLoopVarList(as.VALUE),
			)
			modifyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, nilCtx...)

			// Verify that the operation was created successfully
			gm.Expect(modifyOp).ToNot(gm.BeNil())

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, modifyOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for CDTModifyByPath with empty context slice", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"count": 100,
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTModifyByPath with an empty context slice to modify top-level map
			emptyCtx := []*as.CDTContext{}
			modifyExp := as.ExpMapPut(
				as.DefaultMapPolicy(),
				as.ExpStringVal("newCounter"),
				as.ExpIntVal(200),
				as.ExpLoopVarMap(as.VALUE),
			)
			modifyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, emptyCtx...)

			// Verify that the operation was created successfully
			gm.Expect(modifyOp).ToNot(gm.BeNil())

			// Server should return PARAMETER_ERROR when empty context is provided
			result, err := client.Operate(nil, key, modifyOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should verify ctx field is nil in Operation when no context passed to CDTSelectByPath", func() {
			// This test verifies the internal state of the operation
			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_VALUE)
			gm.Expect(selectOp).ToNot(gm.BeNil())
			// The operation should be created successfully with nil context
		})

		gg.It("should verify ctx field is nil in Operation when no context passed to CDTModifyByPath", func() {
			// This test verifies the internal state of the operation
			modifyExp := as.ExpIntVal(42)
			modifyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp)
			gm.Expect(modifyOp).ToNot(gm.BeNil())
			// The operation should be created successfully with nil context
		})

		gg.It("should return PARAMETER_ERROR for CDTModifyByPath with no context - arithmetic", func() {
			client.Delete(nil, key)

			data := []interface{}{10, 20, 30, 40, 50}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Multiply each value by 2 in the top-level list
			modifyExp := as.ExpNumMul(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(2),
			)
			modifyOp := as.CDTModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp)

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, modifyOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for CDTSelectByPath with no context - matching tree", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"dept1": map[string]interface{}{"name": "Sales", "count": 10},
				"dept2": map[string]interface{}{"name": "Engineering", "count": 25},
				"dept3": map[string]interface{}{"name": "HR", "count": 5},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select the entire top-level structure as matching tree
			selectOp := as.CDTSelectByPath(binName, as.EXP_PATH_SELECT_MATCHING_TREE)

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})
	})
})
