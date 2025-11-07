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
	"github.com/aerospike/aerospike-client-go/v8/internal/version"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Expression CDT Operations Test", func() {

	// connection data
	var ns = *namespace
	var set = randString(50)
	var key *as.Key
	var wpolicy = as.NewWritePolicy(0, 0)

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
	})

	gg.Describe("ExpSelectByPath Tests", func() {

		gg.It("should select prices from books using ExpSelectByPath", func() {
			client.Delete(nil, key)

			booksList := []interface{}{
				map[string]interface{}{
					"title": "Sayings of the Century",
					"price": 10.45,
				},
				map[string]interface{}{
					"title": "Sword of Honour",
					"price": 20.99,
				},
				map[string]interface{}{
					"title": "Moby Dick",
					"price": 5.01,
				},
				map[string]interface{}{
					"title": "The Lord of the Rings",
					"price": 30.98,
				},
			}

			rootMap := map[string]interface{}{
				"book": booksList,
			}

			bin := as.NewBin("res1", rootMap)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			bookKey := as.CtxMapKey(as.NewStringValue("book"))
			allChildren := as.CtxAllChildren()
			priceKey := as.CtxMapKey(as.NewStringValue("price"))

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("res1"),
				bookKey, allChildren, priceKey,
			)

			// Execute the operation using ExpWriteOp
			result, err := client.Operate(nil, key,
				as.ExpWriteOp("A", selectExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			priceList := finalRecord.Bins["A"]
			gm.Expect(priceList).ToNot(gm.BeNil(), "Price list should exist")

			priceListSlice, ok := priceList.([]interface{})
			gm.Expect(ok).To(gm.BeTrue(), "Should be a list")
			gm.Expect(len(priceListSlice)).To(gm.Equal(4), "Should have 4 prices")

			// Verify first price
			firstPrice, ok := priceListSlice[0].(float64)
			if !ok {
				// Try int conversion
				firstPriceInt, ok := priceListSlice[0].(int)
				gm.Expect(ok).To(gm.BeTrue(), "First price should be numeric")
				firstPrice = float64(firstPriceInt)
			}
			gm.Expect(firstPrice).To(gm.BeNumerically("<", 11), "First price should be < 11")
		})

		gg.It("should select titles from books with price filter", func() {
			client.Delete(nil, key)

			booksList := []interface{}{
				map[string]interface{}{
					"title": "Cheap Book",
					"price": 5.99,
				},
				map[string]interface{}{
					"title": "Medium Book",
					"price": 15.50,
				},
				map[string]interface{}{
					"title": "Expensive Book",
					"price": 25.99,
				},
			}

			rootMap := map[string]interface{}{
				"book": booksList,
			}

			bin := as.NewBin("res1", rootMap)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select titles where price <= 10
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

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("res1"),
				ctx1, ctx2, ctx3,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("titles", selectExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify result
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			titles := finalRecord.Bins["titles"]
			gm.Expect(titles).ToNot(gm.BeNil())

			titlesList, ok := titles.([]interface{})
			if ok {
				gm.Expect(len(titlesList)).To(gm.Equal(1), "Should have 1 book with price <= 10")
				gm.Expect(titlesList[0]).To(gm.Equal("Cheap Book"))
			}
		})

		gg.It("should use ExpReadOp with ExpSelectByPath", func() {
			client.Delete(nil, key)

			// Create simple data
			data := map[string]interface{}{
				"items": []interface{}{10, 20, 30},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select all items
			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildren()

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			// Use ExpReadOp to read without modifying
			result, err := client.Operate(nil, key,
				as.ExpReadOp("result", selectExp, as.ExpReadFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify result
			items := result.Bins["result"]
			gm.Expect(items).ToNot(gm.BeNil())

			itemsList, ok := items.([]interface{})
			if ok {
				gm.Expect(len(itemsList)).To(gm.Equal(3))
			}
		})
	})

	gg.Describe("ExpModifyByPath Tests", func() {

		gg.It("should modify all book prices by multiplying by 1.50 using ExpModifyByPath", func() {
			client.Delete(nil, key)

			booksList := []interface{}{
				map[string]interface{}{
					"title": "Sayings of the Century",
					"price": 10.45,
				},
				map[string]interface{}{
					"title": "Sword of Honour",
					"price": 20.99,
				},
				map[string]interface{}{
					"title": "Moby Dick",
					"price": 5.01,
				},
				map[string]interface{}{
					"title": "The Lord of the Rings",
					"price": 30.98,
				},
			}

			rootMap := map[string]interface{}{
				"book": booksList,
			}

			bin := as.NewBin("res1", rootMap)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Build the context chain to modify prices
			bookKey := as.CtxMapKey(as.NewStringValue("book"))
			allChildren := as.CtxAllChildren()
			priceKey := as.CtxMapKey(as.NewStringValue("price"))

			// Create modify expression that multiplies by 1.50
			modifyExp := as.ExpNumMul(
				as.ExpLoopVarFloat(as.VALUE), // Current price value
				as.ExpFloatVal(1.50),         // Multiply by 1.50
			)

			// Create apply expression
			applyExp := as.ExpModifyByPath(
				as.ExpTypeMAP,                  // Return type: map
				0,                              // Flags
				as.ExpMapBin("res1"),           // Source bin
				modifyExp,                      // Modify expression
				bookKey, allChildren, priceKey, // CTX path
			)

			// Execute the operation using ExpWriteOp with UPDATE_ONLY
			result, err := client.Operate(nil, key,
				as.ExpWriteOp("res1", applyExp, as.ExpWriteFlagUpdateOnly),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(finalRecord).ToNot(gm.BeNil())

			finalRootMap, ok := finalRecord.Bins["res1"].(map[interface{}]interface{})
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

			// Convert price to float64
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

			// Verify the price was increased (original was 10.45)
			gm.Expect(finalPrice).To(gm.BeNumerically(">", 11.0), "Price should be increased")

			// Verify it's approximately 10.45 * 1.50
			expectedPrice := 10.45 * 1.50
			gm.Expect(math.Abs(finalPrice-expectedPrice)).To(gm.BeNumerically("<", 0.01),
				"Price should be approximately %f, got %f", expectedPrice, finalPrice)
		})

		gg.It("should modify prices with addition operation", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"products": []interface{}{
					map[string]interface{}{"name": "A", "price": 10.0},
					map[string]interface{}{"name": "B", "price": 20.0},
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Add 5 to each price
			ctx1 := as.CtxMapKey(as.NewStringValue("products"))
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxMapKey(as.NewStringValue("price"))

			modifyExp := as.ExpNumAdd(
				as.ExpLoopVarFloat(as.VALUE),
				as.ExpFloatVal(5.0),
			)

			applyExp := as.ExpModifyByPath(
				as.ExpTypeMAP,
				0,
				as.ExpMapBin("data"),
				modifyExp,
				ctx1, ctx2, ctx3,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("data", applyExp, as.ExpWriteFlagUpdateOnly),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify modification
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins["data"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			products, ok := finalData["products"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			firstProduct, ok := products[0].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			price := firstProduct["price"]
			var priceFloat float64
			switch v := price.(type) {
			case float64:
				priceFloat = v
			case int:
				priceFloat = float64(v)
			}

			// Verify price is 15.0 (10.0 + 5.0)
			gm.Expect(math.Abs(priceFloat - 15.0)).To(gm.BeNumerically("<", 0.01))
		})

		gg.It("should modify with subtraction operation", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"accounts": map[string]interface{}{
					"acc1": 1000,
					"acc2": 2000,
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Subtract 100 from each account
			ctx1 := as.CtxMapKey(as.NewStringValue("accounts"))
			ctx2 := as.CtxAllChildren()

			modifyExp := as.ExpNumSub(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(100),
			)

			applyExp := as.ExpModifyByPath(
				as.ExpTypeMAP,
				0,
				as.ExpMapBin("data"),
				modifyExp,
				ctx1, ctx2,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("data", applyExp, as.ExpWriteFlagUpdateOnly),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify modification
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins["data"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			accounts, ok := finalData["accounts"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			acc1, ok := accounts["acc1"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(acc1).To(gm.Equal(900), "Account1 should be 900 (1000 - 100)")
		})

		gg.It("should work with ExpWriteFlagCreateOnly", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"values": []interface{}{1, 2, 3},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("values"))
			ctx2 := as.CtxAllChildren()

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			// This should succeed (new bin)
			result, err := client.Operate(nil, key,
				as.ExpWriteOp("newbin", selectExp, as.ExpWriteFlagCreateOnly),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// This should fail (bin already exists)
			_, err = client.Operate(nil, key,
				as.ExpWriteOp("newbin", selectExp, as.ExpWriteFlagCreateOnly),
			)
			gm.Expect(err).To(gm.HaveOccurred())
		})

		gg.It("should combine ExpSelectByPath and ExpModifyByPath", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 1, "value": 10},
					map[string]interface{}{"id": 2, "value": 20},
					map[string]interface{}{"id": 3, "value": 30},
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// First, select all values
			selectCtx1 := as.CtxMapKey(as.NewStringValue("items"))
			selectCtx2 := as.CtxAllChildren()
			selectCtx3 := as.CtxMapKey(as.NewStringValue("value"))

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				selectCtx1, selectCtx2, selectCtx3,
			)

			// Write selected values to a new bin
			_, err = client.Operate(nil, key,
				as.ExpWriteOp("values", selectExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Then, modify all values by doubling them
			modifyCtx1 := as.CtxMapKey(as.NewStringValue("items"))
			modifyCtx2 := as.CtxAllChildren()
			modifyCtx3 := as.CtxMapKey(as.NewStringValue("value"))

			modifyExp := as.ExpNumMul(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(2),
			)

			applyExp := as.ExpModifyByPath(
				as.ExpTypeMAP,
				0,
				as.ExpMapBin("data"),
				modifyExp,
				modifyCtx1, modifyCtx2, modifyCtx3,
			)

			_, err = client.Operate(nil, key,
				as.ExpWriteOp("data", applyExp, as.ExpWriteFlagUpdateOnly),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Verify both bins
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Check original values (should be [10, 20, 30])
			values := finalRecord.Bins["values"]
			valuesList, ok := values.([]interface{})
			if ok {
				gm.Expect(len(valuesList)).To(gm.Equal(3))
			}

			// Check modified data (values should be doubled)
			finalData, ok := finalRecord.Bins["data"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			items, ok := finalData["items"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			firstItem, ok := items[0].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			value, ok := firstItem["value"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(value).To(gm.Equal(20), "Value should be doubled (10 * 2 = 20)")
		})

		gg.It("should NOT work with different flag combinations", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": []interface{}{5, 10, 15},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select with different flags
			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildren()

			// Test with flag value 1 (Entries)
			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("result1", selectExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Test with MATCHING_TREE flag
			selectExp2 := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_MATCHING_TREE,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			result2, err := client.Operate(nil, key,
				as.ExpWriteOp("result2", selectExp2, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(result2).To(gm.BeNil())
		})
	})

	gg.Describe("Advanced Expression CDT Tests", func() {

		gg.It("should handle list of lists with ExpSelectByPath", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"matrix": []interface{}{
					[]interface{}{1, 2, 3},
					[]interface{}{4, 5, 6},
					[]interface{}{7, 8, 9},
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select all rows
			ctx1 := as.CtxMapKey(as.NewStringValue("matrix"))
			ctx2 := as.CtxAllChildren()

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("rows", selectExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify result
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			rows := finalRecord.Bins["rows"]
			rowsList, ok := rows.([]interface{})
			if ok {
				gm.Expect(len(rowsList)).To(gm.Equal(3), "Should have 3 rows")
			}
		})

		gg.It("should modify nested map values with ExpModifyByPath", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"departments": map[string]interface{}{
					"sales": map[string]interface{}{
						"revenue": 100000,
						"target":  120000,
					},
					"engineering": map[string]interface{}{
						"revenue": 50000,
						"target":  60000,
					},
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Increase all revenue by 10%
			ctx1 := as.CtxMapKey(as.NewStringValue("departments"))
			ctx2 := as.CtxAllChildrenWithFilter(as.ExpBoolVal(true))
			ctx3 := as.CtxAllChildrenWithFilter(
				as.ExpEq(
					as.ExpLoopVarString(as.MAP_KEY),
					as.ExpStringVal("revenue"),
				),
			)

			modifyExp := as.ExpNumMul(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(2),
			)

			applyExp := as.ExpModifyByPath(
				as.ExpTypeMAP,
				0,
				as.ExpMapBin("data"),
				modifyExp,
				ctx1, ctx2, ctx3,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("data", applyExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify modification
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins["data"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			depts, ok := finalData["departments"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			sales, ok := depts["sales"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			revenue := sales["revenue"]
			gm.Expect(revenue).To(gm.Equal(200000), "Revenue should be 200000 (100000 * 2)")
		})

		gg.It("should use ExpSelectByPath with integer values", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"scores": map[string]interface{}{
					"player1": 100,
					"player2": 200,
					"player3": 150,
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select all scores
			ctx1 := as.CtxMapKey(as.NewStringValue("scores"))
			ctx2 := as.CtxAllChildren()

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			result, err := client.Operate(nil, key,
				as.ExpReadOp("allScores", selectExp, as.ExpReadFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify result
			scores := result.Bins["allScores"]
			gm.Expect(scores).ToNot(gm.BeNil())

			scoresList, ok := scores.([]interface{})
			if ok {
				gm.Expect(len(scoresList)).To(gm.Equal(3), "Should have 3 scores")
			}
		})

		gg.It("should handle ExpModifyByPath with division", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"values": []interface{}{100, 200, 300},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Divide all values by 10
			ctx1 := as.CtxMapKey(as.NewStringValue("values"))
			ctx2 := as.CtxAllChildren()

			modifyExp := as.ExpNumDiv(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(10),
			)

			applyExp := as.ExpModifyByPath(
				as.ExpTypeMAP,
				0,
				as.ExpMapBin("data"),
				modifyExp,
				ctx1, ctx2,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("data", applyExp, as.ExpWriteFlagUpdateOnly),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify modification
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins["data"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			values, ok := finalData["values"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			firstValue, ok := values[0].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(firstValue).To(gm.Equal(10), "100 / 10 = 10")
		})

		gg.It("should work with MAP_KEYS flag in ExpSelectByPath", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"products": map[string]interface{}{
					"apple":  1.50,
					"banana": 0.75,
					"cherry": 2.25,
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select all keys
			ctx1 := as.CtxMapKey(as.NewStringValue("products"))
			ctx2 := as.CtxAllChildren()

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_MAP_KEY,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("keys", selectExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify result - should get keys
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			keys := finalRecord.Bins["keys"]
			gm.Expect(keys).ToNot(gm.BeNil())
		})

		gg.It("should handle ExpSelectByPath with filtered results", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"employees": []interface{}{
					map[string]interface{}{"name": "Alice", "salary": 50000, "active": true},
					map[string]interface{}{"name": "Bob", "salary": 60000, "active": false},
					map[string]interface{}{"name": "Charlie", "salary": 55000, "active": true},
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select names of active employees
			ctx1 := as.CtxMapKey(as.NewStringValue("employees"))
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

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2, ctx3,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("activeEmployees", selectExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify result
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			names := finalRecord.Bins["activeEmployees"]
			namesList, ok := names.([]interface{})
			if ok {
				gm.Expect(len(namesList)).To(gm.Equal(2), "Should have 2 active employees")
				gm.Expect(namesList).To(gm.ContainElement("Alice"))
				gm.Expect(namesList).To(gm.ContainElement("Charlie"))
			}
		})

		gg.It("should handle ExpModifyByPath with conditional expressions", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"items": []interface{}{
					map[string]interface{}{"id": 1, "count": 5},
					map[string]interface{}{"id": 2, "count": 10},
					map[string]interface{}{"id": 3, "count": 15},
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxMapKey(as.NewStringValue("count"))

			modifyExp := as.ExpNumMul(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(2),
			)

			applyExp := as.ExpModifyByPath(
				as.ExpTypeMAP,
				0,
				as.ExpMapBin("data"),
				modifyExp,
				ctx1, ctx2, ctx3,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("data", applyExp, as.ExpWriteFlagUpdateOnly),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			finalData, ok := finalRecord.Bins["data"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			items, ok := finalData["items"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			// Check first item
			firstItem, ok := items[0].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			count, ok := firstItem["count"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(count).To(gm.Equal(10), "5 * 2 = 10")

			// Check second item
			secondItem, ok := items[1].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			count2, ok := secondItem["count"].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(count2).To(gm.Equal(20), "10 * 2 = 20")
		})

		gg.It("should work with ExpWriteFlagEvalNoFail on missing bins", func() {
			client.Delete(nil, key)

			// Don't create the bin
			bin := as.NewBin("otherbin", "test")
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Try to select from non-existent bin with EvalNoFail
			ctx1 := as.CtxMapKey(as.NewStringValue("items"))
			ctx2 := as.CtxAllChildren()

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("nonexistent"),
				ctx1, ctx2,
			)

			// Should not fail with EvalNoFail flag
			result, err := client.Operate(nil, key,
				as.ExpWriteOp("result", selectExp, as.ExpWriteFlagEvalNoFail),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())
		})

		gg.It("should handle multiple ExpWriteOp operations in sequence", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"values": []interface{}{1, 2, 3},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Select values
			ctx1 := as.CtxMapKey(as.NewStringValue("values"))
			ctx2 := as.CtxAllChildren()

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			// Modify values (double them)
			modifyExp := as.ExpNumMul(
				as.ExpLoopVarInt(as.VALUE),
				as.ExpIntVal(2),
			)

			applyExp := as.ExpModifyByPath(
				as.ExpTypeMAP,
				0,
				as.ExpMapBin("data"),
				modifyExp,
				ctx1, ctx2,
			)

			// Execute both operations in one call
			result, err := client.Operate(nil, key,
				as.ExpWriteOp("original", selectExp, as.ExpWriteFlagDefault),
				as.ExpWriteOp("data", applyExp, as.ExpWriteFlagUpdateOnly),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			// Verify both results
			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Original values should be [1, 2, 3]
			original := finalRecord.Bins["original"]
			originalList, ok := original.([]interface{})
			if ok {
				gm.Expect(len(originalList)).To(gm.Equal(3))
			}

			// Modified values should be doubled
			finalData, ok := finalRecord.Bins["data"].(map[interface{}]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			values, ok := finalData["values"].([]interface{})
			gm.Expect(ok).To(gm.BeTrue())

			firstValue, ok := values[0].(int)
			gm.Expect(ok).To(gm.BeTrue())
			gm.Expect(firstValue).To(gm.Equal(2), "1 * 2 = 2")
		})

		gg.It("should select blobs using ExpSelectByPath with ExpLoopVarBlob", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"files": []interface{}{
					map[string]interface{}{"name": "file1.bin", "data": []byte("Binary data 1")},
					map[string]interface{}{"name": "file2.bin", "data": []byte("Binary data 2")},
					map[string]interface{}{"name": "file3.bin", "data": []byte("Binary data 3")},
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("files"))
			ctx2 := as.CtxAllChildren()
			ctx3 := as.CtxMapKey(as.NewStringValue("data"))

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2, ctx3,
			)

			result, err := client.Operate(nil, key,
				as.ExpReadOp("blobResults", selectExp, as.ExpReadFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			blobs := result.Bins["blobResults"]
			gm.Expect(blobs).ToNot(gm.BeNil())

			blobList, ok := blobs.([]interface{})
			if ok {
				gm.Expect(len(blobList)).To(gm.Equal(3), "Should have 3 blobs")
				// Verify at least one blob has content
				foundValidBlob := false
				for _, item := range blobList {
					if blob, ok := item.([]byte); ok && len(blob) > 0 {
						foundValidBlob = true
						break
					}
				}
				gm.Expect(foundValidBlob).To(gm.BeTrue(), "Should have at least one valid blob")
			}
		})

		gg.It("should work with blob list operations using ExpSelectByPath", func() {
			client.Delete(nil, key)

			data := map[string]interface{}{
				"blobList": []interface{}{
					[]byte("First blob content"),
					[]byte("Second blob content"),
					[]byte("Third blob content"),
					[]byte("Fourth blob content"),
				},
			}

			bin := as.NewBin("data", data)
			err := client.PutBins(wpolicy, key, bin)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			ctx1 := as.CtxMapKey(as.NewStringValue("blobList"))
			ctx2 := as.CtxAllChildren()

			selectExp := as.ExpSelectByPath(
				as.ExpTypeLIST,
				as.EXP_PATH_SELECT_VALUE,
				as.ExpMapBin("data"),
				ctx1, ctx2,
			)

			result, err := client.Operate(nil, key,
				as.ExpWriteOp("allBlobs", selectExp, as.ExpWriteFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(result).ToNot(gm.BeNil())

			finalRecord, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			allBlobs := finalRecord.Bins["allBlobs"]
			gm.Expect(allBlobs).ToNot(gm.BeNil())

			blobList, ok := allBlobs.([]interface{})
			if ok {
				gm.Expect(len(blobList)).To(gm.Equal(4), "Should have 4 blobs")
				// Verify all items are byte arrays
				for i, item := range blobList {
					blob, ok := item.([]byte)
					gm.Expect(ok).To(gm.BeTrue(), "Item %d should be a byte array", i)
					gm.Expect(len(blob)).To(gm.BeNumerically(">", 0), "Blob %d should not be empty", i)
				}
			}
		})
	})
})
