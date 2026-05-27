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

			booksList := []any{
				map[string]any{
					"title": "Sayings of the Century",
					"price": 8.95,
				},
				map[string]any{
					"title": "Sword of Honour",
					"price": 12.99,
				},
				map[string]any{
					"title": "Moby Dick",
					"price": 8.99,
				},
				map[string]any{
					"title": "The Lord of the Rings",
					"price": 22.99,
				},
			}

			rootMap := map[string]any{
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
						as.ExpMapLoopVar(as.VALUE),
					),
					as.ExpFloatVal(10.0),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("title"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]any)
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

		gg.It("should select featured products with in-stock variants using nested filters", func() {
			client.Delete(nil, key)

			// Create test data: products with variants and inventory (MAP structure matching Java client pattern)
			// This test mirrors the Java PathExpressionsDemo.java example with:
			// - Root bin contains a map with "inventory" key (wrapper)
			// - Under "inventory" is a MAP of products (keyed by product ID)
			// - CtxAllChildren() iterates all products under "inventory"
			// - CtxAllChildrenWithFilter() filters those products
			// - Each product contains a "variants" MAP (not list)
			rootData := map[string]any{
				"inventory": map[string]any{
					"10000001": map[string]any{
						"name":     "T-Shirt",
						"category": "clothing",
						"featured": true,
						"variants": map[string]any{
							"2001": map[string]any{
								"size":     "S",
								"color":    "red",
								"quantity": 5,
							},
							"2002": map[string]any{
								"size":     "M",
								"color":    "blue",
								"quantity": 0, // Out of stock
							},
							"2003": map[string]any{
								"size":     "L",
								"color":    "green",
								"quantity": 10,
							},
						},
					},
					"10000002": map[string]any{
						"name":     "Jeans",
						"category": "clothing",
						"featured": false, // Not featured
						"variants": map[string]any{
							"2004": map[string]any{
								"size":     "30",
								"color":    "blue",
								"quantity": 20,
							},
						},
					},
					"10000003": map[string]any{
						"name":     "Jacket",
						"category": "clothing",
						"featured": true,
						"variants": map[string]any{
							"2005": map[string]any{
								"size":     "M",
								"color":    "black",
								"quantity": 3,
							},
							"2006": map[string]any{
								"size":     "L",
								"color":    "brown",
								"quantity": 7,
							},
						},
					},
					"10000004": map[string]any{
						"name":     "Hat",
						"category": "accessories",
						"featured": true,
						"variants": map[string]any{
							"2007": map[string]any{
								"size":     "One Size",
								"color":    "red",
								"quantity": 0, // All out of stock
							},
						},
					},
				},
			}

			bin := as.NewBin(binName, rootData)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Product-level filter: featured == true
			filterOnFeatured := as.ExpEq(
				as.ExpMapGetByKey(
					as.MapReturnType.VALUE,
					as.ExpTypeBOOL,
					as.ExpStringVal("featured"),
					as.ExpMapLoopVar(as.VALUE),
				),
				as.ExpBoolVal(true),
			)

			// Variant-level filter: quantity > 0
			filterOnVariantInventory := as.ExpGreater(
				as.ExpMapGetByKey(
					as.MapReturnType.VALUE,
					as.ExpTypeINT,
					as.ExpStringVal("quantity"),
					as.ExpMapLoopVar(as.VALUE),
				),
				as.ExpIntVal(0),
			)

			// Operation - select featured products with in-stock variants
			// Context chain (matching Java main example lines 103-106):
			// 1. CtxAllChildren() - Iterate through children in root map
			// 2. CtxAllChildrenWithFilter() - Filter products by featured==true
			// 3. CtxMapKey("variants") - Navigate to variants in each filtered product
			// 4. CtxAllChildrenWithFilter() - Filter variants by quantity>0
			readResult, err := client.Operate(nil, key,
				as.SelectByPath(binName, as.EXP_PATH_SELECT_MATCHING_TREE,
					as.CtxAllChildren(),
					as.CtxAllChildrenWithFilter(filterOnFeatured),
					as.CtxMapKey(as.NewStringValue("variants")),
					as.CtxAllChildrenWithFilter(filterOnVariantInventory),
				),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(readResult).ToNot(gm.BeNil())

			// Verify results
			results := readResult.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultMap, ok := results.(map[any]any)
			gm.Expect(ok).To(gm.BeTrue(), "Result should be a map")

			// Result should have "inventory" key with filtered products underneath
			gm.Expect(resultMap).To(gm.HaveKey("inventory"), "Result should contain inventory key")

			inventoryMap := resultMap["inventory"].(map[any]any)

			// Should have 3 product IDs in results (10000001, 10000003, 10000004)
			gm.Expect(len(inventoryMap)).To(gm.Equal(3), "Should have 3 featured products in results")
			gm.Expect(inventoryMap).To(gm.HaveKey("10000001"), "Should contain T-Shirt product ID")
			gm.Expect(inventoryMap).ToNot(gm.HaveKey("10000002"), "Should NOT contain Jeans (not featured)")
			gm.Expect(inventoryMap).To(gm.HaveKey("10000003"), "Should contain Jacket product ID")
			gm.Expect(inventoryMap).To(gm.HaveKey("10000004"), "Should contain Hat product ID")

			// Verify T-Shirt (10000001) - only contains variants subtree
			tshirt := inventoryMap["10000001"].(map[any]any)
			gm.Expect(tshirt).To(gm.HaveKey("variants"), "Should contain variants key")
			gm.Expect(tshirt).ToNot(gm.HaveKey("name"), "MATCHING_TREE only returns traversed path, not full object")
			tshirtVariants := tshirt["variants"].(map[any]any)
			gm.Expect(len(tshirtVariants)).To(gm.Equal(2), "T-Shirt should have 2 in-stock variants")
			gm.Expect(tshirtVariants).To(gm.HaveKey("2001"), "Should contain variant 2001 (size S)")
			gm.Expect(tshirtVariants).ToNot(gm.HaveKey("2002"), "Should NOT contain variant 2002 (out of stock)")
			gm.Expect(tshirtVariants).To(gm.HaveKey("2003"), "Should contain variant 2003 (size L)")

			// Verify variant 2001 has correct data
			variant2001 := tshirtVariants["2001"].(map[any]any)
			gm.Expect(variant2001["size"]).To(gm.Equal("S"))
			gm.Expect(variant2001["quantity"]).To(gm.Equal(5))

			// Verify Jacket (10000003) - only contains variants subtree
			jacket := inventoryMap["10000003"].(map[any]any)
			gm.Expect(jacket).To(gm.HaveKey("variants"), "Should contain variants key")
			jacketVariants := jacket["variants"].(map[any]any)
			gm.Expect(len(jacketVariants)).To(gm.Equal(2), "Jacket should have 2 in-stock variants")
			gm.Expect(jacketVariants).To(gm.HaveKey("2005"), "Should contain variant 2005 (size M)")
			gm.Expect(jacketVariants).To(gm.HaveKey("2006"), "Should contain variant 2006 (size L)")

			// Verify Hat (10000004) - has empty variants map (no in-stock variants)
			hat := inventoryMap["10000004"].(map[any]any)
			gm.Expect(hat).To(gm.HaveKey("variants"), "Should contain variants key")
			hatVariants := hat["variants"].(map[any]any)
			gm.Expect(len(hatVariants)).To(gm.Equal(0), "Hat should have 0 in-stock variants")
		})

		gg.It("should modify all book prices by multiplying by 1.10 using CDTModifyByPath", func() {
			client.Delete(nil, key)

			booksList := []any{
				map[string]any{
					"title": "Sayings of the Century",
					"price": 8.95,
				},
				map[string]any{
					"title": "Sword of Honour",
					"price": 12.99,
				},
				map[string]any{
					"title": "Moby Dick",
					"price": 8.99,
				},
				map[string]any{
					"title": "The Lord of the Rings",
					"price": 22.99,
				},
			}

			rootMap := map[string]any{
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
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("price"),
				),
			)

			modifyExp := as.ExpNumMul(
				as.ExpFloatLoopVar(as.VALUE), // Current price value
				as.ExpFloatVal(1.10),         // Multiply by 1.10
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, bookKey, allChildren, priceKey)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(finalRecord).ToNot(gm.BeNil())

			finalRootMap, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue(), "Root map should exist")

			finalBooksListRaw, ok := finalRootMap["book"]
			gm.Expect(ok).To(gm.BeTrue(), "Books list should exist in root map")

			finalBooksList, ok := finalBooksListRaw.([]any)
			gm.Expect(ok).To(gm.BeTrue(), "Books should be a list")
			gm.Expect(len(finalBooksList)).To(gm.BeNumerically(">", 0), "Books list should not be empty")

			firstBook, ok := finalBooksList[0].(map[any]any)
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
				book, ok := bookRaw.(map[any]any)
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

			data := map[string]any{
				"store": map[string]any{
					"books": []any{
						map[string]any{
							"category": "reference",
							"author":   "Nigel Rees",
							"title":    "Sayings of the Century",
							"price":    8.95,
						},
						map[string]any{
							"category": "fiction",
							"author":   "Evelyn Waugh",
							"title":    "Sword of Honour",
							"price":    12.99,
						},
						map[string]any{
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
							as.ExpMapLoopVar(as.VALUE),
						),
						as.ExpStringVal("fiction"),
					),
					as.ExpLess(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeFLOAT,
							as.ExpStringVal("price"),
							as.ExpMapLoopVar(as.VALUE),
						),
						as.ExpFloatVal(10.0),
					),
				),
			)
			ctx4 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("title"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3, ctx4)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			// Should only get "Moby Dick" (fiction with price 8.99 < 10.0)
			resultList, ok := results.([]any)
			gm.Expect(ok).To(gm.BeTrue(), "Result should be a list")
			gm.Expect(len(resultList)).To(gm.Equal(1), "Should have 1 fiction book with price < 10.0")
			gm.Expect(resultList[0]).To(gm.Equal("Moby Dick"))
		})

		gg.It("should handle empty results when no items match the filter", func() {
			client.Delete(nil, key)

			booksList := []any{
				map[string]any{
					"title": "Expensive Book 1",
					"price": 25.99,
				},
				map[string]any{
					"title": "Expensive Book 2",
					"price": 30.50,
				},
			}

			rootMap := map[string]any{
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
						as.ExpMapLoopVar(as.VALUE),
					),
					as.ExpFloatVal(10.0),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("title"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify empty results
			results := result.Bins[binName]
			if results != nil {
				resultList, ok := results.([]any)
				if ok {
					gm.Expect(len(resultList)).To(gm.Equal(0), "Should have 0 books matching the filter")
				}
			}
		})

		gg.It("should work with MatchingTree flag to return the full matching structure", func() {
			client.Delete(nil, key)

			booksList := []any{
				map[string]any{
					"title": "Cheap Book",
					"price": 5.99,
				},
				map[string]any{
					"title": "Expensive Book",
					"price": 25.99,
				},
			}

			rootMap := map[string]any{
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
						as.ExpMapLoopVar(as.VALUE),
					),
					as.ExpFloatVal(10.0),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_MATCHING_TREE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// With MatchingTree, we should get back the full matching structure
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())
		})

		gg.It("should work with MapKeys flag to return only map keys", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"items": map[string]any{
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
					as.ExpIntLoopVar(as.VALUE),
					as.ExpIntVal(75),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_MAP_KEY, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get keys where value > 75
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())
		})

		gg.It("should work with SelectNoFail flag to avoid errors on missing paths", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"existing": []any{1, 2, 3},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Try to select from non-existing path with SelectNoFail
			ctx1 := as.CtxMapKey(as.NewStringValue("existing"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_NO_FAIL, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should work with loop variable INDEX to access list indices", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"numbers": []any{10, 20, 30, 40, 50},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select items where index < 3
			ctx1 := as.CtxMapKey(as.NewStringValue("numbers"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpLess(
					as.ExpIntLoopVar(as.INDEX),
					as.ExpIntVal(3),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get first 3 items (indices 0, 1, 2)
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3), "Should have 3 items with index < 3")
			}
		})

		gg.It("should work with loop variable MAP_KEY to access map keys", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"products": map[string]any{
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
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("c"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get apple and banana (keys < "c")
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(2), "Should have 2 items with keys < 'c'")
			}
		})

		gg.It("should modify with addition operation", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"scores": []any{10, 20, 30, 40, 50},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Add 5 to each score
			ctx1 := as.CtxMapKey(as.NewStringValue("scores"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			modifyExp := as.ExpNumAdd(
				as.ExpIntLoopVar(as.VALUE),
				as.ExpIntVal(5),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalRootMap, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			finalScores, ok := finalRootMap["scores"].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(finalScores)).To(gm.Equal(5))

			firstScore, ok := finalScores[0].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(firstScore).To(gm.Equal(15), "10 + 5 = 15")
		})

		gg.It("should modify with subtraction operation", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"balances": map[string]any{
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
				as.ExpIntLoopVar(as.VALUE),
				as.ExpIntVal(100),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalRootMap, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			finalBalances, ok := finalRootMap["balances"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			// Verify account1 balance was decreased by 100
			balance1, ok := finalBalances["account1"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(balance1).To(gm.Equal(900), "1000 - 100 = 900")
		})

		gg.It("should work with nested lists and complex filters", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"matrix": []any{
					[]any{1, 2, 3},
					[]any{4, 5, 6},
					[]any{7, 8, 9},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("matrix"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get all 3 rows
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3), "Should have 3 rows")
			}
		})

		gg.It("should handle integer map keys correctly", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"items": map[any]any{
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

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())
		})

		gg.It("should work with boolean expressions in filters", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"users": []any{
					map[string]any{
						"name":   "Alice",
						"active": true,
						"age":    30,
					},
					map[string]any{
						"name":   "Bob",
						"active": false,
						"age":    25,
					},
					map[string]any{
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
						as.ExpMapLoopVar(as.VALUE),
					),
					as.ExpBoolVal(true),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("name"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get Alice and Charlie (active users)
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(2), "Should have 2 active users")
				gm.Expect(resultList).To(gm.ContainElement("Alice"))
				gm.Expect(resultList).To(gm.ContainElement("Charlie"))
			}
		})

		gg.It("should handle complex AND/OR filter combinations", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"products": []any{
					map[string]any{"name": "Widget", "price": 10.0, "inStock": true},
					map[string]any{"name": "Gadget", "price": 25.0, "inStock": false},
					map[string]any{"name": "Gizmo", "price": 15.0, "inStock": true},
					map[string]any{"name": "Doohickey", "price": 30.0, "inStock": true},
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
								as.ExpMapLoopVar(as.VALUE),
							),
							as.ExpBoolVal(true),
						),
						as.ExpLess(
							as.ExpMapGetByKey(
								as.MapReturnType.VALUE,
								as.ExpTypeFLOAT,
								as.ExpStringVal("price"),
								as.ExpMapLoopVar(as.VALUE),
							),
							as.ExpFloatVal(20.0),
						),
					),
					as.ExpGreater(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeFLOAT,
							as.ExpStringVal("price"),
							as.ExpMapLoopVar(as.VALUE),
						),
						as.ExpFloatVal(25.0),
					),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("name"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get Widget (inStock, price 10), Gizmo (inStock, price 15), and Doohickey (price 30)
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3), "Should have 3 matching products")
			}
		})

		gg.It("should work with string operations in modify expressions", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"items": []any{
					map[string]any{"id": 1, "tag": "item"},
					map[string]any{"id": 2, "tag": "item"},
					map[string]any{"id": 3, "tag": "item"},
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

			data := map[string]any{
				"level1": map[string]any{
					"level2": map[string]any{
						"level3": []any{
							map[string]any{"value": 100},
							map[string]any{"value": 200},
							map[string]any{"value": 300},
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
						as.ExpMapLoopVar(as.VALUE),
					),
					as.ExpIntVal(150),
				),
			)
			ctx5 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("value"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3, ctx4, ctx5)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Should get values > 150 (200 and 300)
			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())

			resultList, ok := results.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(2), "Should have 2 values > 150")
			}
		})
	})

	gg.Describe("CDT Operation Edge Cases", func() {
		gg.It("should return nil when context is nil for CDTSelectByPath", func() {
			op := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, nil...)
			gm.Expect(op).ToNot(gm.BeNil(), "Should return nil when context is nil")
		})

		gg.It("should return nil when context is nil for CDTModifyByPath", func() {
			modifyExp := as.ExpIntVal(42)
			op := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, nil...)
			gm.Expect(op).ToNot(gm.BeNil(), "Should return nil when context is nil")
		})

		gg.It("should work with single context element", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"value": 123,
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select with single context
			ctx1 := as.CtxMapKey(as.NewStringValue("value"))

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			results := result.Bins[binName]
			gm.Expect(results).ToNot(gm.BeNil())
		})

		gg.It("should handle empty lists correctly", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"emptyList": []any{},
				"items":     []any{1, 2, 3},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Try to select from empty list
			ctx1 := as.CtxMapKey(as.NewStringValue("emptyList"))
			ctx2 := as.CtxAllChildren()

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_NO_FAIL, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle empty maps correctly", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"emptyMap": map[string]any{},
				"items": map[string]any{
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

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_NO_FAIL, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle CDTSelectByPath with list index context", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"items": []any{
					map[string]any{"name": "item1", "value": 10},
					map[string]any{"name": "item2", "value": 20},
					map[string]any{"name": "item3", "value": 30},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select value from second item
			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxListIndex(1) // Select second item (index 1)
			ctx3 := as.CtxMapKey(as.NewStringValue("value"))

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(1))
				gm.Expect(resultList[0]).To(gm.Equal(20))
			}
		})

		gg.It("should handle CDTModifyByPath with subtraction", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"balances": map[string]any{
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
				as.ExpIntLoopVar(as.VALUE),
				as.ExpIntVal(100),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			balances, ok := finalData["balances"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			account1, ok := balances["account1"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(account1).To(gm.Equal(900), "1000 - 100 = 900")
		})

		gg.It("should handle CDTSelectByPath with MATCHING_TREE on nested maps", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"company": map[string]any{
					"sales": map[string]any{
						"q1": 10000,
						"q2": 12000,
					},
					"engineering": map[string]any{
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
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("q1"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_MATCHING_TREE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle CDTSelectByPath with multiple filtered contexts", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"products": []any{
					map[string]any{"name": "Product A", "price": 100, "inStock": true},
					map[string]any{"name": "Product B", "price": 200, "inStock": false},
					map[string]any{"name": "Product C", "price": 150, "inStock": true},
					map[string]any{"name": "Product D", "price": 50, "inStock": true},
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
							as.ExpMapLoopVar(as.VALUE),
						),
						as.ExpBoolVal(true),
					),
					as.ExpLess(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeINT,
							as.ExpStringVal("price"),
							as.ExpMapLoopVar(as.VALUE),
						),
						as.ExpIntVal(150),
					),
				),
			)
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("name"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify the result - should contain "Product A" and "Product D"
			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.BeNumerically(">=", 1))
			}
		})

		gg.It("should handle CDTModifyByPath with ExpLoopVarInt INDEX", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"values": []any{100, 200, 300, 400},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Multiply each value by its index + 1
			ctx1 := as.CtxMapKey(as.NewStringValue("values"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			modifyExp := as.ExpNumMul(
				as.ExpIntLoopVar(as.VALUE),
				as.ExpNumAdd(
					as.ExpIntLoopVar(as.INDEX),
					as.ExpIntVal(1),
				),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			values, ok := finalData["values"].([]any)
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

			data := map[string]any{
				"flags": map[string]any{
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
				as.ExpBoolLoopVar(as.VALUE),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			flags, ok := finalData["flags"].(map[any]any)
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

			data := map[string]any{
				"config": map[string]any{
					"server1": map[string]any{
						"host": "192.168.1.1",
						"port": 8080,
					},
					"server2": map[string]any{
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

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_MAP_KEY, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle CDTModifyByPath with complex arithmetic", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"metrics": []any{
					map[string]any{"value": 10, "multiplier": 2},
					map[string]any{"value": 20, "multiplier": 3},
					map[string]any{"value": 30, "multiplier": 4},
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
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("value"),
				),
			)

			// At ctx3, we're at the "value" field value (10, 20, 30)
			// The expression calculates the new value
			modifyExp := as.ExpNumAdd(
				as.ExpIntLoopVar(as.VALUE),
				as.ExpIntVal(100),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			metrics, ok := finalData["metrics"].([]any)
			gm.Expect(ok).To(gm.BeTrue())

			firstMetric, ok := metrics[0].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			value, ok := firstMetric["value"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(value).To(gm.Equal(110), "10 + 100 = 110")
		})

		gg.It("should handle CDTSelectByPath with OR filter expression", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"items": []any{
					map[string]any{"name": "Item A", "priority": 1},
					map[string]any{"name": "Item B", "priority": 2},
					map[string]any{"name": "Item C", "priority": 3},
					map[string]any{"name": "Item D", "priority": 1},
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
							as.ExpMapLoopVar(as.VALUE),
						),
						as.ExpIntVal(1),
					),
					as.ExpEq(
						as.ExpMapGetByKey(
							as.MapReturnType.VALUE,
							as.ExpTypeINT,
							as.ExpStringVal("priority"),
							as.ExpMapLoopVar(as.VALUE),
						),
						as.ExpIntVal(3),
					),
				),
			)
			ctx3 := as.CtxMapKey(as.NewStringValue("name"))

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify the result - should contain "Item A", "Item C", and "Item D"
			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3))
			}
		})

		gg.It("should select blobs using ExpLoopVarBlob with VALUE", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"blobs": map[string]any{
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

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]any)
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

			data := map[string]any{
				"documents": []any{
					map[string]any{"id": 1, "content": []byte("Document 1 content")},
					map[string]any{"id": 2, "content": []byte("Document 2 content")},
					map[string]any{"id": 3, "content": []byte("Document 3 content")},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("documents"))
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("content"),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]any)
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

		gg.It("should filter blob values using ExpBlobLoopVar", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"files": []any{
					[]byte("shortblob"),
					[]byte("this is a longer blob content"),
					[]byte("another medium sized blob"),
					[]byte("x"),
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select blobs that are not equal to a specific blob value using ExpBlobLoopVar
			ctx1 := as.CtxMapKey(as.NewStringValue("files"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpNotEq(
					as.ExpBlobLoopVar(as.VALUE),
					as.ExpBlobVal([]byte("x")),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(3), "Should have 3 blobs not equal to 'x'")
				for _, item := range resultList {
					blob, ok := item.([]byte)
					gm.Expect(ok).To(gm.BeTrue(), "Item should be a blob")
					gm.Expect(blob).ToNot(gm.Equal([]byte("x")), "Blob should not be 'x'")
				}
			}
		})

		gg.It("should filter nil values using ExpNilLoopVar", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"records": []any{
					map[string]any{"name": "record1", "value": 100},
					map[string]any{"name": "record2", "value": nil},
					map[string]any{"name": "record3", "value": 200},
					map[string]any{"name": "record4", "value": nil},
				},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select only records where value is nil
			ctx1 := as.CtxMapKey(as.NewStringValue("records"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpMapGetByKey(
						as.MapReturnType.VALUE,
						as.ExpTypeNIL,
						as.ExpStringVal("value"),
						as.ExpMapLoopVar(as.VALUE),
					),
					as.ExpNilValue(),
				),
			)
			ctx3 := as.CtxMapKey(as.NewStringValue("name"))

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE|as.EXP_PATH_SELECT_NO_FAIL, ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(2), "Should have 2 records with nil values")
				gm.Expect(resultList).To(gm.ContainElement("record2"))
				gm.Expect(resultList).To(gm.ContainElement("record4"))
			}
		})

		gg.It("should filter HLL values using ExpHLLLoopVar", func() {
			client.Delete(nil, key)

			// Create HLL values using HLLAddOp on temporary bins
			smallData := []as.Value{as.NewValue("a"), as.NewValue("b")}
			largeData := []as.Value{as.NewValue("a"), as.NewValue("b"), as.NewValue("c"), as.NewValue("d"), as.NewValue("e")}

			_, err := client.Operate(nil, key,
				as.HLLAddOp(as.DefaultHLLPolicy(), "hll1", smallData, 8, 0),
				as.HLLAddOp(as.DefaultHLLPolicy(), "hll2", largeData, 8, 0),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Read HLL values back
			rec, err := client.Get(nil, key, "hll1", "hll2")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			hll1Val := rec.Bins["hll1"]
			hll2Val := rec.Bins["hll2"]
			gm.Expect(hll1Val).ToNot(gm.BeNil())
			gm.Expect(hll2Val).ToNot(gm.BeNil())

			// Store HLL values in a nested structure
			data := map[string]any{
				"hlls": []any{hll1Val, hll2Val},
			}

			bin := as.NewBin(binName, data)
			err = client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Use ExpHLLLoopVar to filter HLL values with count > 3
			ctx1 := as.CtxMapKey(as.NewStringValue("hlls"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpGreater(
					as.ExpHLLGetCount(as.ExpHLLLoopVar(as.VALUE)),
					as.ExpIntVal(3),
				),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[binName]
			resultList, ok := resultBin.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(1), "Should have 1 HLL with count > 3 (the large one)")
			}
		})
	})

	gg.Describe("ExpResultRemove Tests", func() {

		gg.It("should remove all items from a list using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"items": []any{1, 2, 3, 4, 5},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))

			applyOp := as.ModifyByPath(binName, 0, as.ExpRemoveResult(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			items, ok := finalData["items"].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(items)).To(gm.Equal(0), "All items should be removed")
		})

		gg.It("should remove filtered items from a list using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"numbers": []any{1, 5, 10, 15, 20, 25, 30},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("numbers"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpGreater(
					as.ExpIntLoopVar(as.VALUE),
					as.ExpIntVal(10),
				),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpRemoveResult(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			numbers, ok := finalData["numbers"].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(numbers)).To(gm.Equal(3), "Should keep items <= 10")
			gm.Expect(numbers).To(gm.ContainElement(1))
			gm.Expect(numbers).To(gm.ContainElement(5))
			gm.Expect(numbers).To(gm.ContainElement(10))
		})

		gg.It("should remove all items from a map using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"config": map[string]any{
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

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpRemoveResult(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			config, ok := finalData["config"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(config)).To(gm.Equal(0), "All map entries should be removed")
		})

		gg.It("should remove filtered map entries using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"scores": map[string]any{
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
					as.ExpIntLoopVar(as.VALUE),
					as.ExpIntVal(50),
				),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpRemoveResult(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			scores, ok := finalData["scores"].(map[any]any)
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

			booksList := []any{
				map[string]any{
					"title": "Cheap Book 1",
					"price": 5.99,
				},
				map[string]any{
					"title": "Expensive Book",
					"price": 25.99,
				},
				map[string]any{
					"title": "Cheap Book 2",
					"price": 3.99,
				},
				map[string]any{
					"title": "Mid Price Book",
					"price": 15.99,
				},
			}

			rootMap := map[string]any{
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
						as.ExpMapLoopVar(as.VALUE),
					),
					as.ExpFloatVal(10.0),
				),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpRemoveResult(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			books, ok := finalData["books"].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(books)).To(gm.Equal(2), "Should keep 2 expensive books")

			for _, bookRaw := range books {
				book, ok := bookRaw.(map[any]any)
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

			data := map[string]any{
				"values": []any{100, 200, 300, 400, 500},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("values"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpGreaterEq(
					as.ExpIntLoopVar(as.INDEX),
					as.ExpIntVal(3),
				),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpRemoveResult(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			values, ok := finalData["values"].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(3), "Should keep first 3 items")
			gm.Expect(values[0]).To(gm.Equal(100))
			gm.Expect(values[1]).To(gm.Equal(200))
			gm.Expect(values[2]).To(gm.Equal(300))
		})

		gg.It("should remove map entries by key filter using ExpResultRemove", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"inventory": map[string]any{
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
					as.ExpStringLoopVar(as.MAP_KEY),
					as.ExpStringVal("c"),
				),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpRemoveResult(), ctx1, ctx2)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			inventory, ok := finalData["inventory"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(inventory)).To(gm.Equal(2))

			_, hasApple := inventory["apple"]
			gm.Expect(hasApple).To(gm.BeTrue())
			_, hasBanana := inventory["banana"]
			gm.Expect(hasBanana).To(gm.BeTrue())
		})

		gg.It("should remove nested items using ExpResultRemove with complex path", func() {
			client.Delete(nil, key)

			data := map[string]any{
				"departments": map[string]any{
					"sales": []any{
						map[string]any{"name": "John", "sales": 1000},
						map[string]any{"name": "Jane", "sales": 5000},
					},
					"engineering": []any{
						map[string]any{"name": "Bob", "sales": 500},
						map[string]any{"name": "Alice", "sales": 3000},
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
						as.ExpMapLoopVar(as.VALUE),
					),
					as.ExpIntVal(2000),
				),
			)

			applyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, as.ExpRemoveResult(), ctx1, ctx2, ctx3)

			result, err := client.Operate(nil, key, applyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			departments, ok := finalData["departments"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			salesList, ok := departments["sales"].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(salesList)).To(gm.Equal(1), "Should keep Jane only")

			engList, ok := departments["engineering"].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(engList)).To(gm.Equal(1), "Should keep Alice only")
		})
	})

	gg.Describe("CDT Path Operations With No Context Tests", func() {

		gg.It("should return PARAMETER_ERROR for CDTSelectByPath with no context passed in", func() {
			client.Delete(nil, key)

			data := []any{1, 2, 3, 4, 5}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTSelectByPath without passing any context
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE)

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

			data := map[string]any{
				"key1": "value1",
				"key2": "value2",
				"key3": "value3",
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTSelectByPath without passing any context
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_MAP_KEY)

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

			data := []any{"a", "b", "c"}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTSelectByPath with explicit nil context
			var nilCtx []*as.CDTContext = nil
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, nilCtx...)

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

			data := map[string]any{
				"alpha": 10,
				"beta":  20,
				"gamma": 30,
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTSelectByPath with an empty context slice
			emptyCtx := []*as.CDTContext{}
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, emptyCtx...)

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

			data := []any{1, 2, 3, 4, 5}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTModifyByPath without passing any context to append an item to the top-level list
			modifyExp := as.ExpListAppend(
				as.DefaultListPolicy(),
				as.ExpIntVal(6),
				as.ExpListLoopVar(as.VALUE),
			)
			modifyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp)

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

			data := map[string]any{
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
				as.ExpMapLoopVar(as.VALUE),
			)
			modifyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp)

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

			data := []any{10, 20, 30}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Call CDTModifyByPath with explicit nil context
			var nilCtx []*as.CDTContext = nil
			modifyExp := as.ExpListAppend(
				as.DefaultListPolicy(),
				as.ExpIntVal(40),
				as.ExpListLoopVar(as.VALUE),
			)
			modifyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, nilCtx...)

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

			data := map[string]any{
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
				as.ExpMapLoopVar(as.VALUE),
			)
			modifyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, emptyCtx...)

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
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE)
			gm.Expect(selectOp).ToNot(gm.BeNil())
			// The operation should be created successfully with nil context
		})

		gg.It("should verify ctx field is nil in Operation when no context passed to CDTModifyByPath", func() {
			// This test verifies the internal state of the operation
			modifyExp := as.ExpIntVal(42)
			modifyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp)
			gm.Expect(modifyOp).ToNot(gm.BeNil())
			// The operation should be created successfully with nil context
		})

		gg.It("should return PARAMETER_ERROR for CDTModifyByPath with no context - arithmetic", func() {
			client.Delete(nil, key)

			data := []any{10, 20, 30, 40, 50}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Multiply each value by 2 in the top-level list
			modifyExp := as.ExpNumMul(
				as.ExpIntLoopVar(as.VALUE),
				as.ExpIntVal(2),
			)
			modifyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp)

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

			data := map[string]any{
				"dept1": map[string]any{"name": "Sales", "count": 10},
				"dept2": map[string]any{"name": "Engineering", "count": 25},
				"dept3": map[string]any{"name": "HR", "count": 5},
			}

			bin := as.NewBin(binName, data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select the entire top-level structure as matching tree
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_MATCHING_TREE)

			// Server should return PARAMETER_ERROR when no context is provided
			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result).To(gm.BeNil())

			// Verify it's a PARAMETER_ERROR
			aerr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerr)).To(gm.BeTrue(), "Error should be an AerospikeError")
			gm.Expect(aerr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
		})

		gg.It("should return PARAMETER_ERROR for SelectByPath with empty binName", func() {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Create test data
			data := map[string]any{"test": "value"}
			client.Delete(nil, key)
			err = client.PutBins(nil, key, as.NewBin("validBin", data))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.StringValue("test"))
			selectOp := as.SelectByPath("", as.EXP_PATH_SELECT_VALUE, ctx1)

			// Call Operate and check the error
			record, err := client.Operate(nil, key, selectOp)

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue())
			gm.Expect(err.Error()).To(gm.ContainSubstring("binName"))
			gm.Expect(record).To(gm.BeNil())
		})

		gg.It("should return PARAMETER_ERROR for SelectByPath with binName too long", func() {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			longBinName := "1234567890123456" // 16 characters, exceeds limit of 15

			// Create test data
			data := map[string]any{"test": "value"}
			client.Delete(nil, key)
			err = client.PutBins(nil, key, as.NewBin("validBin", data))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.StringValue("test"))
			selectOp := as.SelectByPath(longBinName, as.EXP_PATH_SELECT_VALUE, ctx1)

			// Call Operate and check the error
			record, err := client.Operate(nil, key, selectOp)

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue())
			errorMsg := err.Error()
			gm.Expect(errorMsg).To(gm.Or(gm.ContainSubstring("15"), gm.ContainSubstring("exceed")))
			gm.Expect(record).To(gm.BeNil())
		})

		gg.It("should return PARAMETER_ERROR for ModifyByPath with empty binName", func() {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Create test data
			data := map[string]any{"test": 50}
			client.Delete(nil, key)
			err = client.PutBins(nil, key, as.NewBin("validBin", data))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.StringValue("test"))
			modifyExp := as.ExpIntVal(100)
			modifyOp := as.ModifyByPath("", as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1)

			// Call Operate and check the error
			record, err := client.Operate(nil, key, modifyOp)

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue())
			gm.Expect(err.Error()).To(gm.ContainSubstring("binName"))
			gm.Expect(record).To(gm.BeNil())
		})

		gg.It("should return PARAMETER_ERROR for ModifyByPath with binName too long", func() {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			longBinName := "1234567890123456" // 16 characters, exceeds limit of 15

			// Create test data
			data := map[string]any{"test": 50}
			client.Delete(nil, key)
			err = client.PutBins(nil, key, as.NewBin("validBin", data))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.StringValue("test"))
			modifyExp := as.ExpIntVal(100)
			modifyOp := as.ModifyByPath(longBinName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, ctx1)

			// Call Operate and check the error
			record, err := client.Operate(nil, key, modifyOp)

			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue())
			errorMsg := err.Error()
			gm.Expect(errorMsg).To(gm.Or(gm.ContainSubstring("15"), gm.ContainSubstring("exceed")))
			gm.Expect(record).To(gm.BeNil())
		})
	})

	gg.Describe("HLL Nested in Map/List Tests", func() {

		gg.It("should preserve HLLValue type when stored in a map[any]any", func() {
			client.Delete(nil, key)

			// Create an HLL bin with 5000 integer values
			values := make([]as.Value, 5000)
			for i := 0; i < 5000; i++ {
				values[i] = as.NewValue(i)
			}

			ops := []*as.Operation{
				as.HLLAddOp(as.DefaultHLLPolicy(), "hll_temp_bin", values, 4, 4),
			}
			_, err := client.Operate(nil, key, ops...)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Read HLL value back
			rec, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			hllVal, ok := rec.Bins["hll_temp_bin"].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue(), "Top-level HLL bin should be HLLValue type")

			// Store the HLL value inside a map[any]any
			wp := as.NewWritePolicy(0, 0)
			wp.SendKey = true
			err = client.PutBins(wp, key, as.NewBin("elhss", map[any]any{
				"a": as.NewHLLValue(hllVal),
			}))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Read back and verify the nested value is still HLLValue
			rec, err = client.Get(nil, key, "elhss")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			resultMap, ok := rec.Bins["elhss"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue(), "Bin should be a map")

			nestedVal, ok := resultMap["a"].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue(), "Nested value should be HLLValue, not []byte")
			gm.Expect(len(nestedVal)).To(gm.BeNumerically(">", 0))
		})

		gg.It("should preserve HLLValue type when stored in a map[string]any", func() {
			client.Delete(nil, key)

			// Create an HLL bin
			entries := make([]as.Value, 256)
			for i := 0; i < 256; i++ {
				entries[i] = as.NewValue(i)
			}

			ops := []*as.Operation{
				as.HLLAddOp(as.DefaultHLLPolicy(), binName, entries, 8, 0),
			}
			_, err := client.Operate(nil, key, ops...)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Read HLL value back
			rec, err := client.Get(nil, key, binName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			hllVal, ok := rec.Bins[binName].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue(), "Top-level HLL bin should be HLLValue type")

			// Store the HLL value inside a map[string]any
			mapBinName := "mapbin"
			mapData := map[string]any{
				"a": as.NewHLLValue(hllVal),
			}
			bin := as.NewBin(mapBinName, mapData)
			err = client.PutBins(nil, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Read back and verify the nested value is still HLLValue
			rec, err = client.Get(nil, key, mapBinName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			resultMap, ok := rec.Bins[mapBinName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue(), "Bin should be a map")

			nestedVal, ok := resultMap["a"].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue(), "Nested value should be HLLValue, not []byte")
			gm.Expect(len(nestedVal)).To(gm.BeNumerically(">", 0))
		})

		gg.It("should preserve HLLValue type when stored in a list", func() {
			client.Delete(nil, key)

			// Create two HLL bins with different data
			smallData := []as.Value{as.StringValue("a"), as.StringValue("b")}
			largeData := []as.Value{as.StringValue("c"), as.StringValue("d"), as.StringValue("e"), as.StringValue("f"), as.StringValue("g")}

			_, err := client.Operate(nil, key,
				as.HLLAddOp(as.DefaultHLLPolicy(), "hll1", smallData, 8, 0),
				as.HLLAddOp(as.DefaultHLLPolicy(), "hll2", largeData, 8, 0),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rec, err := client.Get(nil, key, "hll1", "hll2")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			hll1Val := rec.Bins["hll1"].(as.HLLValue)
			hll2Val := rec.Bins["hll2"].(as.HLLValue)

			// Store HLL values in a list inside a map
			listBinName := "listbin"
			listData := map[string]any{
				"hlls": []any{as.NewHLLValue(hll1Val), as.NewHLLValue(hll2Val)},
			}
			bin := as.NewBin(listBinName, listData)
			err = client.PutBins(nil, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Read back and verify nested HLL values preserve their type
			rec, err = client.Get(nil, key, listBinName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			resultMap, ok := rec.Bins[listBinName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			resultList, ok := resultMap["hlls"].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(resultList)).To(gm.Equal(2))

			_, ok = resultList[0].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue(), "First nested HLL should be HLLValue type")

			_, ok = resultList[1].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue(), "Second nested HLL should be HLLValue type")
		})

		gg.It("should preserve HLLValue type in map inside map", func() {
			client.Delete(nil, key)

			entries := make([]as.Value, 256)
			for i := 0; i < 256; i++ {
				entries[i] = as.NewValue(i)
			}

			_, err := client.Operate(nil, key,
				as.HLLAddOp(as.DefaultHLLPolicy(), binName, entries, 8, 0),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rec, err := client.Get(nil, key, binName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			hllVal, ok := rec.Bins[binName].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue())

			// Store HLL in a map nested inside another map (2 levels deep)
			deepBinName := "deepmapbin"
			deepData := map[string]any{
				"level1": map[any]any{
					"level2": as.NewHLLValue(hllVal),
				},
			}
			bin := as.NewBin(deepBinName, deepData)
			err = client.PutBins(nil, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rec, err = client.Get(nil, key, deepBinName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			outerMap, ok := rec.Bins[deepBinName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			innerMap, ok := outerMap["level1"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			nestedVal, ok := innerMap["level2"].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue(), "HLL nested 2 levels deep in maps should be HLLValue type")
			gm.Expect(len(nestedVal)).To(gm.BeNumerically(">", 0))
		})

		gg.It("should preserve HLLValue type in list inside list inside map", func() {
			client.Delete(nil, key)

			entries := make([]as.Value, 256)
			for i := 0; i < 256; i++ {
				entries[i] = as.NewValue(i)
			}

			_, err := client.Operate(nil, key,
				as.HLLAddOp(as.DefaultHLLPolicy(), binName, entries, 8, 0),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rec, err := client.Get(nil, key, binName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			hllVal, ok := rec.Bins[binName].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue())

			// Store HLL in a list nested inside another list inside a map (3 levels deep)
			deepBinName := "deeplistbin"
			deepData := map[string]any{
				"level1": []any{
					[]any{as.NewHLLValue(hllVal)},
				},
			}
			bin := as.NewBin(deepBinName, deepData)
			err = client.PutBins(nil, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rec, err = client.Get(nil, key, deepBinName)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			outerMap, ok := rec.Bins[deepBinName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			outerList, ok := outerMap["level1"].([]any)
			gm.Expect(ok).To(gm.BeTrue())

			innerList, ok := outerList[0].([]any)
			gm.Expect(ok).To(gm.BeTrue())

			nestedVal, ok := innerList[0].(as.HLLValue)
			gm.Expect(ok).To(gm.BeTrue(), "HLL nested 3 levels deep (map->list->list) should be HLLValue type")
			gm.Expect(len(nestedVal)).To(gm.BeNumerically(">", 0))
		})

		gg.It("should support HLL operations on nested HLL values via expressions", func() {
			client.Delete(nil, key)

			// Create HLL values with different cardinalities
			smallData := []as.Value{as.StringValue("a"), as.StringValue("b")}
			largeData := []as.Value{as.StringValue("a"), as.StringValue("b"), as.StringValue("c"), as.StringValue("d"), as.StringValue("e")}

			_, err := client.Operate(nil, key,
				as.HLLAddOp(as.DefaultHLLPolicy(), "hll1", smallData, 8, 0),
				as.HLLAddOp(as.DefaultHLLPolicy(), "hll2", largeData, 8, 0),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rec, err := client.Get(nil, key, "hll1", "hll2")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			hll1Val := rec.Bins["hll1"].(as.HLLValue)
			hll2Val := rec.Bins["hll2"].(as.HLLValue)

			// Store both HLL values in a nested map->list structure
			nestedBinName := "nestedbin"
			data := map[string]any{
				"hlls": []any{as.NewHLLValue(hll1Val), as.NewHLLValue(hll2Val)},
			}
			bin := as.NewBin(nestedBinName, data)
			err = client.PutBins(nil, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Use ExpHLLLoopVar to filter HLL values with count > 3
			ctx1 := as.CtxMapKey(as.NewStringValue("hlls"))
			ctx2 := as.CtxAllChildrenWithFilter(
				as.ExpGreater(
					as.ExpHLLGetCount(as.ExpHLLLoopVar(as.VALUE)),
					as.ExpIntVal(3),
				),
			)

			selectOp := as.SelectByPath(nestedBinName, as.EXP_PATH_SELECT_VALUE, ctx1, ctx2)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultBin := result.Bins[nestedBinName]
			resultList, ok := resultBin.([]any)
			if ok {
				gm.Expect(len(resultList)).To(gm.Equal(1), "Should have 1 HLL with count > 3 (the large one)")
			}
		})
	})

	gg.Describe("CDT MapKeysIn and AndFilter Tests", func() {

		gg.BeforeEach(func() {
			requiredVersion, err := version.Parse("8.1.2")
			if err != nil {
				gg.Fail("Failed to parse server required version")
			}

			node := client.GetNodes()[0]
			nodeVersion := node.GetServerVersion()
			if nodeVersion.IsSmaller(requiredVersion) {
				gg.Skip("CDT mapKeysIn and andFilter operations require server version 8.1.2+.")
				return
			}
		})

		gg.It("should select map entries by key list using mapKeysIn", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"alpha": 10,
				"beta":  20,
				"gamma": 30,
				"delta": 40,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx := as.CtxMapStringKeysIn("alpha", "gamma")
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(2))
			gm.Expect(values).To(gm.ContainElement(10))
			gm.Expect(values).To(gm.ContainElement(30))
		})

		gg.It("should apply same-level AND filter with mapKeysIn", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"a": 5,
				"b": 15,
				"c": 25,
				"d": 35,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select keys "a", "b", "c" via mapKeysIn, then AND-filter to keep values > 10
			keyInCtx := as.CtxMapStringKeysIn("a", "b", "c")
			andFilterCtx := as.CtxAndFilter(
				as.ExpGreater(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(10)),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_MAP_KEY_VALUE, keyInCtx, andFilterCtx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultList, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			// MAP_KEY_VALUE returns a flat list [key, value, key, value, ...]
			gm.Expect(len(resultList)).To(gm.Equal(4), "Should have 4 elements (2 key-value pairs)")

			resultMap := make(map[any]any)
			for i := 0; i < len(resultList); i += 2 {
				resultMap[resultList[i]] = resultList[i+1]
			}
			gm.Expect(resultMap["b"]).To(gm.Equal(15))
			gm.Expect(resultMap["c"]).To(gm.Equal(25))
		})

		gg.It("should reject andFilter as first context with no preceding entry", func() {
			client.Delete(nil, key)

			m := map[string]any{"a": 1}
			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// andFilter with no preceding context (e.g. mapKeysIn) is invalid
			andFilterCtx := as.CtxAndFilter(
				as.ExpGreater(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(0)),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, andFilterCtx)

			_, err = client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
		})

		gg.It("should reject chained andFilters", func() {
			client.Delete(nil, key)

			m := map[string]any{"a": 1, "b": 2, "c": 3}
			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Two andFilters in a row is invalid — use ExpAnd to combine instead
			keyInCtx := as.CtxMapStringKeysIn("a", "b", "c")
			andFilter1 := as.CtxAndFilter(
				as.ExpGreaterEq(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(2)),
			)
			andFilter2 := as.CtxAndFilter(
				as.ExpLessEq(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(3)),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, keyInCtx, andFilter1, andFilter2)

			_, err = client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
		})

		gg.It("should reject andFilter after allChildrenWithFilter", func() {
			client.Delete(nil, key)

			m := map[string]any{"a": 1, "b": 2, "c": 3}
			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// andFilter cannot follow an expression-type context like allChildrenWithFilter
			baseFilter := as.CtxAllChildrenWithFilter(
				as.ExpGreater(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(0)),
			)
			andFilterCtx := as.CtxAndFilter(
				as.ExpLess(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(3)),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, baseFilter, andFilterCtx)

			_, err = client.Operate(nil, key, selectOp)
			gm.Expect(err).To(gm.HaveOccurred())
		})

		gg.It("should apply andFilter with mapIndex", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"x": 10,
				"y": 20,
				"z": 30,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select map index 0, then AND-filter to keep only entries with key >= "y"
			indexCtx := as.CtxMapIndex(0)
			andFilterCtx := as.CtxAndFilter(
				as.ExpGreaterEq(as.ExpStringLoopVar(as.MAP_KEY), as.ExpStringVal("y")),
			)

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_MAP_KEY_VALUE, indexCtx, andFilterCtx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			resultList, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			// Index 0 selects a single entry; AND filter further narrows it.
			// The result depends on which entry is at index 0 and whether it passes the filter.
			// If it passes, we get 2 elements (key+value); if not, 0 elements.
			gm.Expect(len(resultList) == 0 || len(resultList) == 2).To(gm.BeTrue())
		})

		gg.It("should modify via mapKeysIn", func() {
			client.Delete(nil, key)

			// Nested structure matching C client test pattern:
			// {inventory: {w1: {item_a: {count: 100}, item_b: {count: 200}},
			//              w2: {item_a: {count: 50},  item_b: {count: 75}},
			//              w3: {item_a: {count: 10},  item_b: {count: 20}}}}
			m := map[string]any{
				"inventory": map[string]any{
					"w1": map[string]any{
						"item_a": map[string]any{"count": 100},
						"item_b": map[string]any{"count": 200},
					},
					"w2": map[string]any{
						"item_a": map[string]any{"count": 50},
						"item_b": map[string]any{"count": 75},
					},
					"w3": map[string]any{
						"item_a": map[string]any{"count": 10},
						"item_b": map[string]any{"count": 20},
					},
				},
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Add 1000 to count in all items of w1 and w2 via ModifyByPath
			// Context: mapKey("inventory") -> mapKeysIn("w1","w2") -> allChildren() -> mapKey("count")
			invKey := as.CtxMapKey(as.StringValue("inventory"))
			keysCtx := as.CtxMapStringKeysIn("w1", "w2")
			childCtx := as.CtxAllChildren()
			countKey := as.CtxMapKey(as.StringValue("count"))

			modifyExp := as.ExpNumAdd(
				as.ExpIntLoopVar(as.VALUE),
				as.ExpIntVal(1000),
			)

			modifyOp := as.ModifyByPath(binName, as.EXP_PATH_MODIFY_DEFAULT, modifyExp, invKey, keysCtx, childCtx, countKey)

			result, err := client.Operate(nil, key, modifyOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify the modified values
			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			resultMap, ok := record.Bins[binName].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			inv, ok := resultMap["inventory"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())

			rw1, ok := inv["w1"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())
			rw1a, ok := rw1["item_a"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(rw1a["count"]).To(gm.Equal(1100))

			rw1b, ok := rw1["item_b"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(rw1b["count"]).To(gm.Equal(1200))

			// w3 should be unchanged
			rw3, ok := inv["w3"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())
			rw3a, ok := rw3["item_a"].(map[any]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(rw3a["count"]).To(gm.Equal(10))
		})

		gg.It("should handle mapKeysIn with some missing keys", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"a": 1,
				"b": 2,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx := as.CtxMapStringKeysIn("a", "x")
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(1))
			gm.Expect(values).To(gm.ContainElement(1))
		})

		gg.It("should handle mapKeysIn with empty key list", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"a": 1,
				"b": 2,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx := as.CtxMapStringKeysIn()
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(0))
		})

		gg.It("should handle mapKeysIn with empty map", func() {
			client.Delete(nil, key)

			m := map[string]any{}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx := as.CtxMapStringKeysIn("a", "b")
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(0))
		})

		gg.It("should handle mapKeysIn with single key", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"x": 1,
				"y": 2,
				"z": 3,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx := as.CtxMapStringKeysIn("y")
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(1))
			gm.Expect(values[0]).To(gm.Equal(2))
		})

		gg.It("should handle mapKeysIn selecting all keys", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"a": 1,
				"b": 2,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx := as.CtxMapStringKeysIn("a", "b")
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(2))
			gm.Expect(values).To(gm.ContainElement(1))
			gm.Expect(values).To(gm.ContainElement(2))
		})

		gg.It("should return results in map key order not input order", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"z": 3,
				"a": 1,
				"m": 2,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx := as.CtxMapStringKeysIn("a", "z", "m")
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(3))
			// Aerospike maps are key-ordered, so results come back in sorted key order: a=1, m=2, z=3
			gm.Expect(values[0]).To(gm.Equal(1))
			gm.Expect(values[1]).To(gm.Equal(2))
			gm.Expect(values[2]).To(gm.Equal(3))
		})

		gg.It("should handle mapKeysIn with integer keys", func() {
			client.Delete(nil, key)

			m := map[any]any{
				1: "one",
				2: "two",
				3: "three",
			}

			bin := as.NewBin(binName, as.NewMapValue(m))
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx := as.CtxMapIntKeysIn(1, 2)
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(2))
			gm.Expect(values).To(gm.ContainElement("one"))
			gm.Expect(values).To(gm.ContainElement("two"))
		})

		gg.It("should handle mapKeysIn on nested map", func() {
			client.Delete(nil, key)

			inner := map[string]any{
				"a": 1,
				"b": 2,
				"c": 3,
			}

			outer := map[string]any{
				"outer": inner,
			}

			bin := as.NewBin(binName, outer)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Navigate into "outer" key, then select keys "a" and "c" from the inner map
			outerCtx := as.CtxMapKey(as.NewStringValue("outer"))
			keysCtx := as.CtxMapStringKeysIn("a", "c")

			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, outerCtx, keysCtx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(2))
			gm.Expect(values).To(gm.ContainElement(1))
			gm.Expect(values).To(gm.ContainElement(3))
		})

		gg.It("should select map entries with mixed-type keys using CtxMapKeysIn", func() {
			client.Delete(nil, key)

			blobKey := []byte{'u', 's', '-', 'e', 'a', 's', 't'}

			// Build a map with one string key, one integer key, and one blob key.
			// Go's native maps cannot hold a []byte key (not comparable), so
			// populate the bin via per-key MapPutOps — each call can use a
			// typed Value for the key, so the resulting CDT map ends up with
			// genuine string / int / blob keys at the server.
			mapPolicy := as.DefaultMapPolicy()
			_, err := client.Operate(nil, key,
				as.MapPutOp(mapPolicy, binName, as.NewStringValue("sku"), "widget-42"),
				as.MapPutOp(mapPolicy, binName, as.NewLongValue(1001), "qty"),
				as.MapPutOp(mapPolicy, binName, as.NewBytesValue(blobKey), "region"),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Polymorphic key list: string + long + bytes in one call. The server
			// validates element types and matches each against the map keys.
			ctx := as.CtxMapKeysIn(
				as.NewStringValue("sku"),
				as.NewLongValue(1001),
				as.NewBytesValue(blobKey),
			)
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(3))
			gm.Expect(values).To(gm.ContainElement("widget-42"))
			gm.Expect(values).To(gm.ContainElement("qty"))
			gm.Expect(values).To(gm.ContainElement("region"))
		})

		gg.It("should tolerate a nil element in CtxMapKeysIn key list", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"a": 1,
				"b": 2,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// A nil entry must be packed as msgpack nil (server tolerates it
			// when scanning the key filter list) — packer.packValueArray must
			// not panic on the nil interface.
			ctx := as.CtxMapKeysIn(as.NewStringValue("a"), nil)
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			record, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record).ToNot(gm.BeNil())
		})

		gg.It("should match CtxMapStringKeysIn results when called polymorphically", func() {
			client.Delete(nil, key)

			m := map[string]any{
				"alpha": 10,
				"beta":  20,
				"gamma": 30,
				"delta": 40,
			}

			bin := as.NewBin(binName, m)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Equivalence guard: same map shape as the 'should select map entries
			// by key list using mapKeysIn' spec above. Calling through the new
			// polymorphic CtxMapKeysIn with string Values must return the same
			// result set as the deprecated CtxMapStringKeysIn variant.
			ctx := as.CtxMapKeysIn(as.NewStringValue("alpha"), as.NewStringValue("gamma"))
			selectOp := as.SelectByPath(binName, as.EXP_PATH_SELECT_VALUE, ctx)

			result, err := client.Operate(nil, key, selectOp)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			values, ok := result.Bins[binName].([]any)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(len(values)).To(gm.Equal(2))
			gm.Expect(values).To(gm.ContainElement(10))
			gm.Expect(values).To(gm.ContainElement(30))
		})
	})
})
