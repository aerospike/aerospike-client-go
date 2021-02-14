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

package aerospike

// import (
// 	"math"
// 	"strings"

// 	gg "github.com/onsi/ginkgo"
// 	gm "github.com/onsi/gomega"
// )

// func testPackingFor(v interface{}) interface{} {
// 	packer := newPacker()

// 	err := packer.PackObject(v)
// 	gm.Expect(err).ToNot(gm.HaveOccurred())

// 	unpacker := newUnpacker(packer.buffer.Bytes(), 0, len(packer.buffer.Bytes()))
// 	unpackedValue, err := unpacker.unpackObject(false)
// 	gm.Expect(err).ToNot(gm.HaveOccurred())

// 	return unpackedValue
// }

// var _ = gg.Describe("Packing Test", func() {

// 	gg.Context("Simple Value Types", func() {

// 		gg.It("should pack and unpack nil values", func() {
// 			gm.Expect(testPackingFor(nil)).To(gm.BeNil())
// 		})

// 		gg.It("should pack and unpack -32 < int8 < 32 values", func() {
// 			v := int8(31)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))

// 			v = int8(-32)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))
// 		})

// 		gg.It("should pack and unpack int8 values", func() {
// 			v := int8(math.MaxInt8)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))

// 			v = int8(math.MinInt8)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))
// 		})

// 		gg.It("should pack and unpack uint8 values", func() {
// 			v := uint8(math.MaxUint8)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))
// 		})

// 		gg.It("should pack and unpack int16 values", func() {
// 			v := int16(math.MaxInt16)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))

// 			v = int16(math.MinInt16)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))
// 		})

// 		gg.It("should pack and unpack uint16 values", func() {
// 			v := uint16(math.MaxUint16)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))
// 		})

// 		gg.It("should pack and unpack int32 values", func() {
// 			v := int32(math.MaxInt32)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))

// 			v = int32(math.MinInt32)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))
// 		})

// 		gg.It("should pack and unpack uint32 values", func() {
// 			v := uint32(math.MaxUint32)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))
// 		})

// 		gg.It("should pack and unpack int64 values", func() {
// 			v := int64(math.MaxInt64)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))

// 			v = int64(math.MinInt64)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(int(v)))
// 		})

// 		gg.It("should pack and unpack uint64 values", func() {
// 			v := uint64(math.MaxUint64)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))
// 		})

// 		gg.It("should pack and unpack string values", func() {
// 			v := "string123456789\n"
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))
// 		})

// 		gg.It("should pack and unpack string values of size 32911 for sign bit check", func() {
// 			v := strings.Repeat("s", 32911)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))
// 		})

// 		gg.It("should pack and unpack boolean: true values", func() {
// 			v := true
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))
// 		})

// 		gg.It("should pack and unpack boolean: false values", func() {
// 			v := false
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))
// 		})

// 		gg.It("should pack and unpack float32 values", func() {
// 			v := float32(math.MaxFloat32)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))

// 			v = float32(-math.MaxFloat32)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))
// 		})

// 		gg.It("should pack and unpack float64 values", func() {
// 			v := float64(math.MaxFloat64)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))

// 			v = float64(-math.MaxFloat64)
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(v))
// 		})
// 	})

// 	gg.Context("Array Value Types", func() {

// 		gg.It("should pack and unpack empty array of int8", func() {
// 			v := []int8{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{}))
// 		})

// 		gg.It("should pack and unpack an array of int8", func() {
// 			v := []int8{1, 2, 3}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{1, 2, 3}))
// 		})

// 		gg.It("should pack and unpack empty array of uint8", func() {
// 			// Note: An array of uint8 ends up as being a ByteArrayValue
// 			v := []uint8{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]byte{}))
// 		})

// 		gg.It("should pack and unpack an array of uint8", func() {
// 			// Note: An array of uint8 ends up as being a ByteArrayValue
// 			v := []uint8{1, 2, 3}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]byte{1, 2, 3}))
// 		})

// 		gg.It("should pack and unpack empty array of int16", func() {
// 			v := []int16{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{}))
// 		})

// 		gg.It("should pack and unpack an array of int16", func() {
// 			v := []int16{1, 2, 3}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{1, 2, 3}))
// 		})

// 		gg.It("should pack and unpack empty array of uint16", func() {
// 			v := []uint16{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{}))
// 		})

// 		gg.It("should pack and unpack an array of uint16", func() {
// 			v := []uint16{1, 2, 3}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{1, 2, 3}))
// 		})

// 		gg.It("should pack and unpack empty array of int32", func() {
// 			v := []int32{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{}))
// 		})

// 		gg.It("should pack and unpack an array of int32", func() {
// 			v := []int32{1, 2, 3}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{1, 2, 3}))
// 		})

// 		gg.It("should pack and unpack empty array of uint32", func() {
// 			v := []uint32{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{}))
// 		})

// 		gg.It("should pack and unpack an array of uint32", func() {
// 			v := []uint32{1, 2, 3}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{1, 2, 3}))
// 		})

// 		gg.It("should pack and unpack empty array of int64", func() {
// 			v := []int64{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{}))
// 		})

// 		gg.It("should pack and unpack an array of int64", func() {
// 			v := []int64{1, 2, 3}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{1, 2, 3}))
// 		})

// 		gg.It("should pack and unpack empty array of uint64", func() {
// 			v := []uint64{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{}))
// 		})

// 		gg.It("should pack and unpack an array of uint64", func() {
// 			v := []uint64{1, 2, 3}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{uint64(1), uint64(2), uint64(3)}))
// 		})

// 		gg.It("should pack and unpack empty array of string", func() {
// 			v := []string{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{}))
// 		})

// 		gg.It("should pack and unpack an array of string", func() {
// 			v := []string{"this", "is", "an", "array", "of", "strings", strings.Repeat("s", 32911)}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal([]interface{}{"this", "is", "an", "array", "of", "strings", strings.Repeat("s", 32911)}))
// 		})

// 	})

// 	gg.Context("Map Value Types", func() {

// 		gg.It("should pack and unpack empty map", func() {
// 			v := map[interface{}]interface{}{}
// 			gm.Expect(testPackingFor(v)).To(gm.Equal(map[interface{}]interface{}{}))
// 		})

// 		gg.It("should pack and unpack a complex map", func() {
// 			v := map[interface{}]interface{}{
// 				"uint8":      uint8(math.MaxUint8),
// 				"int8":       int8(math.MaxInt8),
// 				"mint8":      int8(math.MinInt8),
// 				"uint16":     uint16(math.MaxUint16),
// 				"int16":      int16(math.MaxInt16),
// 				"mint16":     int16(math.MinInt16),
// 				"uint32":     uint32(math.MaxUint32),
// 				"int32":      int32(math.MaxInt32),
// 				"mint32":     int32(math.MinInt32),
// 				"uint":       uint64(math.MaxUint64),
// 				"int":        int64(math.MaxInt64),
// 				"mint":       int64(math.MinInt64),
// 				"uint64":     uint64(math.MaxUint64),
// 				"int64":      int64(math.MaxInt64),
// 				"mint64":     int64(math.MinInt64),
// 				"maxFloat32": float32(math.MaxFloat32),
// 				"minFloat32": float32(-math.MaxFloat32),
// 				"maxFloat64": float64(math.MaxFloat64),
// 				"minFloat64": float64(-math.MaxFloat64),
// 				"str":        "this is a string",
// 				"strbitsign": strings.Repeat("s", 32911),
// 				"nil":        nil,
// 				"true":       true,
// 				"false":      false,
// 			}

// 			vRes := map[interface{}]interface{}{
// 				"uint8":      int(math.MaxUint8),
// 				"int8":       int(math.MaxInt8),
// 				"mint8":      int(math.MinInt8),
// 				"uint16":     int(math.MaxUint16),
// 				"int16":      int(math.MaxInt16),
// 				"mint16":     int(math.MinInt16),
// 				"uint32":     int(math.MaxUint32),
// 				"int32":      int(math.MaxInt32),
// 				"mint32":     int(math.MinInt32),
// 				"uint":       uint64(math.MaxUint64),
// 				"int":        int(math.MaxInt64),
// 				"mint":       int(math.MinInt64),
// 				"uint64":     uint64(math.MaxUint64),
// 				"int64":      int(math.MaxInt64),
// 				"mint64":     int(math.MinInt64),
// 				"maxFloat32": float32(math.MaxFloat32),
// 				"minFloat32": float32(-math.MaxFloat32),
// 				"maxFloat64": float64(math.MaxFloat64),
// 				"minFloat64": float64(-math.MaxFloat64),
// 				"str":        "this is a string",
// 				"strbitsign": strings.Repeat("s", 32911),
// 				"nil":        nil,
// 				"true":       true,
// 				"false":      false,
// 			}

// 			gm.Expect(testPackingFor(v)).To(gm.Equal(vRes))
// 		})

// 		gg.It("should pack and unpack map with varying key types", func() {
// 			// Test Values
// 			vUint8 := map[uint8]interface{}{
// 				uint8(math.MaxUint8): "v",
// 			}

// 			vInt8 := map[int8]interface{}{
// 				int8(math.MaxInt8): "v",
// 			}

// 			vUint16 := map[uint16]interface{}{
// 				uint16(math.MaxUint16): "v",
// 			}

// 			vInt16 := map[int16]interface{}{
// 				int16(math.MaxInt16): "v",
// 			}

// 			vUint32 := map[uint32]interface{}{
// 				uint32(math.MaxUint32): "v",
// 			}

// 			vInt32 := map[int32]interface{}{
// 				int32(math.MaxInt32): "v",
// 			}

// 			vUint64 := map[uint64]interface{}{
// 				uint64(math.MaxUint64): "v",
// 			}

// 			vInt64 := map[int64]interface{}{
// 				int64(math.MaxInt64): "v",
// 			}

// 			vFloat32 := map[float32]interface{}{
// 				float32(math.MaxFloat32): "v",
// 			}

// 			vFloat64 := map[float64]interface{}{
// 				float64(math.MaxFloat64): "v",
// 			}

// 			vStr := map[string]interface{}{
// 				"string key": "v",
// 			}

// 			// gm.Expected Values
// 			retUint8 := map[interface{}]interface{}{
// 				int(math.MaxUint8): "v",
// 			}

// 			retInt8 := map[interface{}]interface{}{
// 				int(math.MaxInt8): "v",
// 			}

// 			retUint16 := map[interface{}]interface{}{
// 				int(math.MaxUint16): "v",
// 			}

// 			retInt16 := map[interface{}]interface{}{
// 				int(math.MaxInt16): "v",
// 			}

// 			retUint32 := map[interface{}]interface{}{
// 				int(math.MaxUint32): "v",
// 			}

// 			retInt32 := map[interface{}]interface{}{
// 				int(math.MaxInt32): "v",
// 			}

// 			retUint64 := map[interface{}]interface{}{
// 				uint64(math.MaxUint64): "v",
// 			}

// 			retInt64 := map[interface{}]interface{}{
// 				int(math.MaxInt64): "v",
// 			}

// 			retFloat32 := map[interface{}]interface{}{
// 				float32(math.MaxFloat32): "v",
// 			}

// 			retFloat64 := map[interface{}]interface{}{
// 				float64(math.MaxFloat64): "v",
// 			}

// 			retStr := map[interface{}]interface{}{
// 				"string key": "v",
// 			}

// 			gm.Expect(testPackingFor(vUint8)).To(gm.Equal(retUint8))
// 			gm.Expect(testPackingFor(vInt8)).To(gm.Equal(retInt8))
// 			gm.Expect(testPackingFor(vUint16)).To(gm.Equal(retUint16))
// 			gm.Expect(testPackingFor(vInt16)).To(gm.Equal(retInt16))
// 			gm.Expect(testPackingFor(vUint32)).To(gm.Equal(retUint32))
// 			gm.Expect(testPackingFor(vInt32)).To(gm.Equal(retInt32))
// 			gm.Expect(testPackingFor(vUint64)).To(gm.Equal(retUint64))
// 			gm.Expect(testPackingFor(vInt64)).To(gm.Equal(retInt64))
// 			gm.Expect(testPackingFor(vFloat32)).To(gm.Equal(retFloat32))
// 			gm.Expect(testPackingFor(vFloat64)).To(gm.Equal(retFloat64))
// 			gm.Expect(testPackingFor(vStr)).To(gm.Equal(retStr))
// 		})
// 	})
// })
