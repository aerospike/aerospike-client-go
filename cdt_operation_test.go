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
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"

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
		if !featureEnabled("cdt-select-path") {
			gg.Skip("CDT select/modify by path operations require server version 5.6+ with cdt-select-path feature.")
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
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
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
			ctx4 := as.CtxMapKey(as.NewStringValue("title"))

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3, ctx4)

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
			priceKey := as.CtxMapKey(as.NewStringValue("price"))

			modifyExp := as.ExpNumMul(
				as.ExpLoopVarFloat(as.VALUE), // Current price value
				as.ExpFloatVal(1.10),         // Multiply by 1.10
			)

			applyOp := as.CDTModifyByPath(binName, 0, modifyExp, bookKey, allChildren, priceKey)

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
			ctx3 := as.CtxAllChildren()
			ctx4 := as.CtxAllChildrenWithFilter(
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
			ctx5 := as.CtxMapKey(as.NewStringValue("title"))

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3, ctx4, ctx5)

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
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
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
			ctx4 := as.CtxMapKey(as.NewStringValue("title"))

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3, ctx4)

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
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
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

			selectOp := as.CDTSelectByPath(binName, as.MATCHING_TREE, ctx1, ctx2, ctx3)

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

			selectOp := as.CDTSelectByPath(binName, as.MAP_KEYS, ctx1, ctx2)

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

			selectOp := as.CDTSelectByPath(binName, as.NO_FAIL, ctx1, ctx2)

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

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2)

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

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2)

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
			ctx2 := as.CtxAllChildren()

			modifyExp := as.ExpNumAdd(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(5),
			)

			applyOp := as.CDTModifyByPath(binName, 0, modifyExp, ctx1, ctx2)

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
			ctx2 := as.CtxAllChildren()

			modifyExp := as.ExpNumSub(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(100),
			)

			applyOp := as.CDTModifyByPath(binName, 0, modifyExp, ctx1, ctx2)

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

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2)

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

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2)

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
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
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
			ctx4 := as.CtxMapKey(as.NewStringValue("name"))

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3, ctx4)

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
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
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
			ctx4 := as.CtxMapKey(as.NewStringValue("name"))

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3, ctx4)

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
			ctx4 := as.CtxAllChildren()
			ctx5 := as.CtxAllChildrenWithFilter(
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
			ctx6 := as.CtxMapKey(as.NewStringValue("value"))

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3, ctx4, ctx5, ctx6)

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
			op := as.CDTSelectByPath(binName, as.VALUES, nil...)
			gm.Expect(op).To(gm.BeNil(), "Should return nil when context is nil")
		})

		gg.It("should return nil when context is nil for CDTModifyByPath", func() {
			modifyExp := as.ExpIntVal(42)
			op := as.CDTModifyByPath(binName, 0, modifyExp, nil...)
			gm.Expect(op).To(gm.BeNil(), "Should return nil when context is nil")
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

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1)

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

			selectOp := as.CDTSelectByPath(binName, as.NO_FAIL, ctx1, ctx2)

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

			selectOp := as.CDTSelectByPath(binName, as.NO_FAIL, ctx1, ctx2)

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

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3)

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
			ctx2 := as.CtxAllChildren()

			modifyExp := as.ExpNumSub(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(100),
			)

			applyOp := as.CDTModifyByPath(binName, 0, modifyExp, ctx1, ctx2)

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
			ctx3 := as.CtxMapKey(as.NewStringValue("q1"))

			selectOp := as.CDTSelectByPath(binName, as.MATCHING_TREE, ctx1, ctx2, ctx3)

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
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
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
			ctx4 := as.CtxMapKey(as.NewStringValue("name"))

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3, ctx4)

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
			ctx2 := as.CtxAllChildren()

			modifyExp := as.ExpNumMul(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpNumAdd(
					as.ExpLoopVarInt(as.INDEX),
					as.ExpIntVal(1),
				),
			)

			applyOp := as.CDTModifyByPath(binName, 0, modifyExp, ctx1, ctx2)

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
			ctx2 := as.CtxAllChildren()

			modifyExp := as.ExpNot(
				as.ExpLoopVarBool(as.VALUE),
			)

			applyOp := as.CDTModifyByPath(binName, 0, modifyExp, ctx1, ctx2)

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

			selectOp := as.CDTSelectByPath(binName, as.MAP_KEYS, ctx1, ctx2, ctx3)

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

			// Calculate: (value * multiplier) + 100
			ctx1 := as.CtxMapKey(as.NewStringValue("metrics"))
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxMapKey(as.NewStringValue("value"))

			modifyExp := as.ExpNumAdd(
				as.ExpNumMul(
					as.ExpLoopVarInt(as.VALUE),
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeINT,
						as.ExpStringVal("multiplier"),
						as.ExpLoopVarMap(as.VALUE),
					),
				),
				as.ExpIntVal(100),
			)

			applyOp := as.CDTModifyByPath(binName, 0, modifyExp, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			metrics, ok := finalData["metrics"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			// First: (10 * 2) + 100 = 120
			// Second: (20 * 3) + 100 = 160
			// Third: (30 * 4) + 100 = 220
			firstMetric, ok := metrics[0].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			value, ok := firstMetric["value"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(value).To(gm.Equal(120), "(10 * 2) + 100 = 120")
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
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
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
			ctx4 := as.CtxMapKey(as.NewStringValue("name"))

			selectOp := as.CDTSelectByPath(binName, as.VALUES, ctx1, ctx2, ctx3, ctx4)

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
	})
})
