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

package aerospike

import (
	"testing"

	"math"
	"reflect"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	"iter"
	"maps"
	"slices"

	"github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	"github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

type testBLOB struct {
	name string
}

func (b *testBLOB) EncodeBlob() ([]byte, error) {
	return []byte(b.name), nil
}

func isValidIntegerValue(i int, v Value) bool {
	gm.Expect(reflect.TypeOf(v)).To(gm.Equal(reflect.TypeOf(NewIntegerValue(0))))
	gm.Expect(v.GetObject()).To(gm.Equal(i))
	gm.Expect(v.EstimateSize()).To(gm.Equal(int(buffer.SizeOfInt)))
	gm.Expect(v.GetType()).To(gm.Equal(ParticleType.INTEGER))

	return true
}

func isValidLongValue(i int64, v Value) bool {
	gm.Expect(reflect.TypeOf(v)).To(gm.Equal(reflect.TypeOf(NewLongValue(0))))
	gm.Expect(v.GetObject().(int64)).To(gm.Equal(i))
	gm.Expect(v.EstimateSize()).To(gm.Equal(int(buffer.SizeOfInt64)))
	gm.Expect(v.GetType()).To(gm.Equal(ParticleType.INTEGER))

	return true
}

func isValidFloatValue(i float64, v Value) bool {
	gm.Expect(reflect.TypeOf(v)).To(gm.Equal(reflect.TypeOf(NewFloatValue(0))))
	gm.Expect(v.GetObject().(float64)).To(gm.Equal(i))
	gm.Expect(v.EstimateSize()).To(gm.Equal(8))
	gm.Expect(v.GetType()).To(gm.Equal(ParticleType.FLOAT))

	return true
}

var _ = gg.Describe("Value Test", func() {

	gg.Context("NullValue", func() {
		gg.It("should create a valid NullValue", func() {
			v := NewValue(nil)

			gm.Expect(v.GetObject()).To(gm.BeNil())
			gm.Expect(v.EstimateSize()).To(gm.Equal(0))
			gm.Expect(v.GetType()).To(gm.Equal(ParticleType.NULL))
		})
	})

	gg.Context("StringValues", func() {
		gg.It("should create a valid string value", func() {
			str := "string value"
			v := NewValue(str)

			gm.Expect(v.GetObject()).To(gm.Equal(str))
			gm.Expect(v.EstimateSize()).To(gm.Equal(len(str)))
			gm.Expect(v.GetType()).To(gm.Equal(ParticleType.STRING))
		})

		gg.It("should create a valid empty string value", func() {
			str := ""
			v := NewValue(str)

			gm.Expect(v.GetObject()).To(gm.Equal(str))
			gm.Expect(v.EstimateSize()).To(gm.Equal(len(str)))
			gm.Expect(v.GetType()).To(gm.Equal(ParticleType.STRING))
		})
	})

	gg.Context("Blob Values", func() {

		gg.It("should create a BytesValue on valid types, and encode", func() {
			person := &testBLOB{name: "SomeDude"}

			bval := NewValue(person)
			gm.Expect(bval.GetType()).To(gm.Equal(ParticleType.BLOB))
			gm.Expect(bval).To(gm.BeAssignableToTypeOf(BytesValue{}))
			gm.Expect(bval.GetObject()).To(gm.Equal([]byte(person.name)))
		})
	})

	gg.Context("Numeric Values", func() {

		gg.It("should create a valid IntegerValue on boundries of int8", func() {
			i := int8(math.MinInt8)
			v := NewValue(i)
			isValidIntegerValue(int(i), v)

			i = int8(math.MaxInt8)
			v = NewValue(i)
			isValidIntegerValue(int(i), v)
		})

		gg.It("should create a valid IntegerValue on boundries of uint8", func() {
			i := uint8(0)
			v := NewValue(i)
			isValidIntegerValue(int(i), v)

			i = uint8(math.MaxUint8)
			v = NewValue(i)
			isValidIntegerValue(int(i), v)
		})

		gg.It("should create a valid IntegerValue on boundries of int16", func() {
			i := int16(math.MinInt16)
			v := NewValue(i)
			isValidIntegerValue(int(i), v)

			i = int16(math.MaxInt16)
			v = NewValue(i)
			isValidIntegerValue(int(i), v)
		})

		gg.It("should create a valid IntegerValue on boundries of uint16", func() {
			i := uint16(0)
			v := NewValue(i)
			isValidIntegerValue(int(i), v)

			i = uint16(math.MaxUint16)
			v = NewValue(i)
			isValidIntegerValue(int(i), v)
		})

		gg.It("should create a valid IntegerValue on boundries of int32", func() {
			i := int32(math.MinInt32)
			v := NewValue(i)
			isValidIntegerValue(int(i), v)

			i = int32(math.MaxInt32)
			v = NewValue(i)
			isValidIntegerValue(int(i), v)
		})

		gg.It("should create a valid IntegerValue on boundries of native int on 32 bit machines", func() {
			if buffer.Arch32Bits {
				i := math.MinInt32
				v := NewValue(i)
				isValidIntegerValue(i, v)

				i = math.MaxInt32
				v = NewValue(i)
				isValidIntegerValue(i, v)
			}
		})

		gg.It("should create a valid LongValue after boundries of int32 is passed on 32 bit machines", func() {
			if buffer.Arch32Bits {
				i := math.MinInt32 - 1
				v := NewValue(i)
				isValidLongValue(int64(i), v)

				i = math.MaxInt32 + 1
				v = NewValue(i)
				isValidLongValue(int64(i), v)
			}
		})

		gg.It("should create a valid IntegerValue on boundries of native int on 64 bit machines", func() {
			if buffer.Arch64Bits {
				i := math.MinInt64
				v := NewValue(i)
				isValidIntegerValue(i, v)

				i = math.MaxInt64
				v = NewValue(i)
				isValidIntegerValue(i, v)
			}
		})

		gg.It("should create a valid LongValue on boundries of int64", func() {
			i := int64(math.MinInt64)
			v := NewValue(i)
			isValidLongValue(i, v)

			i = int64(math.MaxInt64)
			v = NewValue(i)
			isValidLongValue(i, v)
		})

		gg.It("should create a valid FloatValue on boundries of float64", func() {
			i := float64(-math.MaxFloat64)
			v := NewValue(i)
			isValidFloatValue(i, v)

			i = float64(math.MaxFloat64)
			v = NewValue(i)
			isValidFloatValue(i, v)
		})

	}) // numeric values context
})

// TypedMapValue packs through packTypedObject, a per-entry fast path that avoids
// boxing typed keys and values into interfaces. These specs pin the two
// properties that make the fast path safe: it produces byte-identical wire
// output to the untyped MapValue for the same data, and it does not allocate.
var _ = gg.Describe("TypedMapValue packing", func() {

	packValue := func(v Value) []byte {
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}
		n, err := v.pack(buf)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return buf.dataBuffer[:n]
	}

	// Single-entry maps make the msgpack output deterministic without
	// canonical ordering, so typed and untyped bytes can be compared directly.
	gg.It("must pack byte-identically to MapValue across the concrete fast-path types", func() {
		gm.Expect(packValue(NewTypedMapValue(map[string]int{"k": 42}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{"k": 42}))))
		gm.Expect(packValue(NewTypedMapValue(map[int]string{7: "v"}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{7: "v"}))))
		gm.Expect(packValue(NewTypedMapValue(map[int64]float64{-9: 2.5}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{int64(-9): 2.5}))))
		gm.Expect(packValue(NewTypedMapValue(map[uint32]bool{3: true}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{uint32(3): true}))))
		gm.Expect(packValue(NewTypedMapValue(map[string][]byte{"b": {1, 2, 3}}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{"b": []byte{1, 2, 3}}))))
	})

	// The whole-map dispatch routes the popular shapes to the generics.go
	// MapIter wrappers; each covered combination must stay byte-identical to
	// the untyped path.
	gg.It("must pack byte-identically through the whole-map dispatch", func() {
		gm.Expect(packValue(NewTypedMapValue(map[string]string{"k": "v"}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{"k": "v"}))))
		gm.Expect(packValue(NewTypedMapValue(map[string]float64{"f": 1.25}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{"f": 1.25}))))
		gm.Expect(packValue(NewTypedMapValue(map[int]string{3: "v"}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{3: "v"}))))
		gm.Expect(packValue(NewTypedMapValue(map[int]int{1: 2}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{1: 2}))))
		gm.Expect(packValue(NewTypedMapValue(map[int64]int64{-5: 9}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{int64(-5): int64(9)}))))
		gm.Expect(packValue(NewTypedMapValue(map[int64]float64{7: 0.5}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{int64(7): 0.5}))))
	})

	// NewValue routes typed maps and slices to the generic value types
	// (generics.go's per-shape wrapper types are retired). GetObject must
	// return the plain container, so a caller's type assertion on the
	// original map/slice type succeeds.
	gg.It("must be what NewValue returns for typed maps and slices", func() {
		mv := NewValue(map[string]int{"a": 1})
		gm.Expect(mv).To(gm.BeAssignableToTypeOf(TypedMapValue[string, int]{}))
		gm.Expect(mv.GetObject()).To(gm.Equal(map[string]int{"a": 1}))

		lv := NewValue([]string{"a", "b"})
		gm.Expect(lv).To(gm.BeAssignableToTypeOf(TypedListValue[string]{}))
		gm.Expect(lv.GetObject()).To(gm.Equal([]string{"a", "b"}))

		// Key types newly admitted to ValidMapKey (previously covered only
		// by the retired wrappers): uint64, float32, float64.
		gm.Expect(NewValue(map[uint64]float32{7: 0.5})).
			To(gm.BeAssignableToTypeOf(TypedMapValue[uint64, float32]{}))
		gm.Expect(NewValue(map[float64]any{0.5: "x"})).
			To(gm.BeAssignableToTypeOf(TypedMapValue[float64, any]{}))

		// And they pack byte-identically to the untyped path.
		gm.Expect(packValue(NewValue(map[uint64]float32{7: 0.5}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{uint64(7): float32(0.5)}))))
		gm.Expect(packValue(NewValue(map[float64]int{0.5: 3}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{0.5: 3}))))
	})

	// Named key/value types miss the whole-map dispatch by design and take
	// the per-entry typed path; the bytes must not differ.
	gg.It("must pack named-type instantiations identically to unnamed ones", func() {
		type myKey string
		type myVal int
		gm.Expect(packValue(NewTypedMapValue(map[myKey]myVal{"k": 42}))).
			To(gm.Equal(packValue(NewTypedMapValue(map[string]int{"k": 42}))))
	})

	gg.It("must pack byte-identically through the default (boxing) branch", func() {
		// A nested untyped map exercises packTypedObject's fallback to
		// packObject; a Value-typed value exercises the Value dispatch.
		nested := map[any]any{"inner": 1}
		gm.Expect(packValue(NewTypedMapValue(map[string]map[any]any{"m": nested}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{"m": nested}))))
		gm.Expect(packValue(NewTypedMapValue(map[string]Value{"v": NewValue("s")}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{"v": NewValue("s")}))))
	})

	gg.It("must pack multi-entry maps byte-identically under canonical ordering", func() {
		// Enough keys that a random map iteration order is effectively
		// never sorted by luck (12! orderings) -- this spec must also catch
		// a whole-map dispatch that ignores the canonical flag and packs in
		// map order.
		typed := map[string]int{}
		untyped := map[any]any{}
		for i := 0; i < 12; i++ {
			k := string(rune('a' + i))
			typed[k] = i
			untyped[k] = i
		}

		packCanonical := func(v Value) []byte {
			buf := &bufferEx{dataBuffer: make([]byte, 8192)}
			buf.setCanonicalKeys(true)
			n, err := v.pack(buf)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			return buf.dataBuffer[:n]
		}
		gm.Expect(packCanonical(NewTypedMapValue(typed))).
			To(gm.Equal(packCanonical(NewMapValue(untyped))))
	})

	// The optimization's reason to exist: packing a typed map must not box
	// its entries. AllocsPerRun keeps the escape-analysis property honest --
	// in particular the default branch must not force the switch temporary
	// onto the heap for the concrete cases.
	gg.It("must pack without allocating for fast-path key and value types", func() {
		typed := NewTypedMapValue(map[string]int64{"alpha": 1, "beta": 2, "gamma": 3})
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}

		allocs := testing.AllocsPerRun(100, func() {
			buf.dataOffset = 0
			_, err := typed.pack(buf)
			if err != nil {
				panic(err)
			}
		})
		gm.Expect(allocs).To(gm.BeZero(),
			"typed map packing must not box its entries")
	})
})

// TypedListValue packs typed slices without boxing: popular element types route
// once per pack to the generics.go ListIter wrappers, the rest through the
// per-entry typed path. Slices iterate in order, so multi-element outputs
// compare byte-for-byte directly.
var _ = gg.Describe("TypedListValue packing", func() {

	packValue := func(v Value) []byte {
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}
		n, err := v.pack(buf)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return buf.dataBuffer[:n]
	}

	gg.It("must pack byte-identically to ListValue through the whole-slice dispatch", func() {
		gm.Expect(packValue(NewTypedListValue([]string{"a", "b", "c"}))).
			To(gm.Equal(packValue(NewListValue([]any{"a", "b", "c"}))))
		gm.Expect(packValue(NewTypedListValue([]int{1, -2, 300}))).
			To(gm.Equal(packValue(NewListValue([]any{1, -2, 300}))))
		gm.Expect(packValue(NewTypedListValue([]int64{9, -9}))).
			To(gm.Equal(packValue(NewListValue([]any{int64(9), int64(-9)}))))
		gm.Expect(packValue(NewTypedListValue([]float64{0.5, -1.75}))).
			To(gm.Equal(packValue(NewListValue([]any{0.5, -1.75}))))
	})

	gg.It("must pack byte-identically through the per-entry typed path", func() {
		// Element types without a whole-slice wrapper: bool, []byte, and a
		// named type that misses the dispatch.
		gm.Expect(packValue(NewTypedListValue([]bool{true, false}))).
			To(gm.Equal(packValue(NewListValue([]any{true, false}))))
		gm.Expect(packValue(NewTypedListValue([][]byte{{1}, {2, 3}}))).
			To(gm.Equal(packValue(NewListValue([]any{[]byte{1}, []byte{2, 3}}))))
		type myInt int
		gm.Expect(packValue(NewTypedListValue([]myInt{4, 5}))).
			To(gm.Equal(packValue(NewTypedListValue([]int{4, 5}))))
	})

	gg.It("must pack byte-identically through the default (boxing) branch", func() {
		nested := map[any]any{"inner": 1}
		gm.Expect(packValue(NewTypedListValue([]map[any]any{nested}))).
			To(gm.Equal(packValue(NewListValue([]any{nested}))))
	})

	gg.It("must pack without allocating for fast-path and typed-path elements", func() {
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}
		for _, v := range []Value{
			NewTypedListValue([]string{"alpha", "beta", "gamma"}), // whole-slice dispatch
			NewTypedListValue([]bool{true, false, true}),          // per-entry typed path
		} {
			allocs := testing.AllocsPerRun(100, func() {
				buf.dataOffset = 0
				if _, err := v.pack(buf); err != nil {
					panic(err)
				}
			})
			gm.Expect(allocs).To(gm.BeZero(), "typed list packing must not box its elements")
		}
	})
})

// SeqMapValue and SeqListValue pack typed sequences without materializing
// them. The wire protocol needs the count before the entries, so the
// declared count is validated against what the sequence actually yields.
var _ = gg.Describe("SeqMapValue and SeqListValue packing", func() {

	packValue := func(v Value) []byte {
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}
		n, err := v.pack(buf)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return buf.dataBuffer[:n]
	}

	gg.It("must pack a map sequence byte-identically to TypedMapValue", func() {
		m := map[string]int{"k": 42}
		gm.Expect(packValue(NewSeqMapValue(maps.All(m), len(m)))).
			To(gm.Equal(packValue(NewTypedMapValue(m))))

		// A hand-rolled sequence, not derived from a map.
		pairs := func(yield func(int64, float64) bool) {
			yield(7, 0.5)
		}
		gm.Expect(packValue(NewSeqMapValue(iter.Seq2[int64, float64](pairs), 1))).
			To(gm.Equal(packValue(NewTypedMapValue(map[int64]float64{7: 0.5}))))
	})

	gg.It("must pack a list sequence byte-identically to TypedListValue", func() {
		l := []string{"a", "b", "c"}
		gm.Expect(packValue(NewSeqListValue(slices.Values(l), len(l)))).
			To(gm.Equal(packValue(NewTypedListValue(l))))
		bl := []bool{true, false}
		gm.Expect(packValue(NewSeqListValue(slices.Values(bl), len(bl)))).
			To(gm.Equal(packValue(NewTypedListValue(bl))))
	})

	gg.It("must pack multi-entry map sequences byte-identically under canonical ordering", func() {
		typed := map[string]int{}
		untyped := map[any]any{}
		for i := 0; i < 12; i++ {
			k := string(rune('a' + i))
			typed[k] = i
			untyped[k] = i
		}
		packCanonical := func(v Value) []byte {
			buf := &bufferEx{dataBuffer: make([]byte, 8192)}
			buf.setCanonicalKeys(true)
			n, err := v.pack(buf)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			return buf.dataBuffer[:n]
		}
		gm.Expect(packCanonical(NewSeqMapValue(maps.All(typed), len(typed)))).
			To(gm.Equal(packCanonical(NewMapValue(untyped))))
	})

	gg.It("must reject sequences that yield a different count than declared", func() {
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}
		m := map[string]int{"a": 1, "b": 2}

		_, err := NewSeqMapValue(maps.All(m), 3).pack(buf)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(types.PARAMETER_ERROR)).To(gm.BeTrue())

		_, err = NewSeqMapValue(maps.All(m), 1).pack(buf)
		gm.Expect(err).To(gm.HaveOccurred())

		// Canonical path validates too.
		buf.setCanonicalKeys(true)
		_, err = NewSeqMapValue(maps.All(m), 3).pack(buf)
		gm.Expect(err).To(gm.HaveOccurred())
		buf.setCanonicalKeys(false)

		_, err = NewSeqListValue(slices.Values([]int{1, 2, 3}), 2).pack(buf)
		gm.Expect(err).To(gm.HaveOccurred())
		_, err = NewSeqListValue(slices.Values([]int{1}), 2).pack(buf)
		gm.Expect(err).To(gm.HaveOccurred())
	})

	gg.It("must pack sequences without allocating", func() {
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}
		m := map[string]int64{"alpha": 1, "beta": 2, "gamma": 3}
		l := []string{"alpha", "beta", "gamma"}
		for _, v := range []Value{
			NewSeqMapValue(maps.All(m), len(m)),
			NewSeqListValue(slices.Values(l), len(l)),
		} {
			allocs := testing.AllocsPerRun(100, func() {
				buf.dataOffset = 0
				if _, err := v.pack(buf); err != nil {
					panic(err)
				}
			})
			gm.Expect(allocs).To(gm.BeZero(), "sequence packing must not allocate")
		}
	})
})

// NewValue must accept any map or slice without per-entry allocation:
// unnamed primitive shapes hit the generated type-switch (TypedMapValue/
// TypedListValue), named outer types are shed via an O(1) reflect.Convert back
// into that switch, and everything else packs through the reflective
// values, which read entries in place instead of boxing them.
var _ = gg.Describe("NewValue reflection fallback packing", func() {

	type myMap map[string]int
	type mySlice []int64
	type myKey string
	type myVal int

	packValue := func(v Value) []byte {
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}
		n, err := v.pack(buf)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return buf.dataBuffer[:n]
	}

	gg.It("must route previously uncovered primitive shapes to the typed values", func() {
		gm.Expect(NewValue(map[string]bool{"a": true})).
			To(gm.BeAssignableToTypeOf(TypedMapValue[string, bool]{}))
		gm.Expect(NewValue(map[uint8]string{1: "x"})).
			To(gm.BeAssignableToTypeOf(TypedMapValue[uint8, string]{}))
		gm.Expect(NewValue([]bool{true})).
			To(gm.BeAssignableToTypeOf(TypedListValue[bool]{}))
		gm.Expect(packValue(NewValue(map[string]bool{"a": true}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{"a": true}))))
	})

	gg.It("must shed named outer container types onto the typed fast path", func() {
		gm.Expect(NewValue(myMap{"a": 1})).
			To(gm.BeAssignableToTypeOf(TypedMapValue[string, int]{}))
		gm.Expect(NewValue(mySlice{5})).
			To(gm.BeAssignableToTypeOf(TypedListValue[int64]{}))
		gm.Expect(packValue(NewValue(myMap{"a": 1}))).
			To(gm.Equal(packValue(NewValue(map[string]int{"a": 1}))))
		gm.Expect(packValue(NewValue(mySlice{5, -9}))).
			To(gm.Equal(packValue(NewValue([]int64{5, -9}))))
	})

	gg.It("must pack named key/element types byte-identically via the reflective values", func() {
		gm.Expect(packValue(NewValue(map[myKey]myVal{"k": 42}))).
			To(gm.Equal(packValue(NewValue(map[string]int{"k": 42}))))
		gm.Expect(packValue(NewValue([]myVal{4, 5}))).
			To(gm.Equal(packValue(NewValue([]int{4, 5}))))
		// Narrow integer kinds take a distinct packReflect branch.
		type myI16 int16
		type myU32 uint32
		gm.Expect(packValue(NewValue([]myI16{-3, 7}))).
			To(gm.Equal(packValue(NewValue([]int16{-3, 7}))))
		gm.Expect(packValue(NewValue(map[myU32]myI16{9: -2}))).
			To(gm.Equal(packValue(NewValue(map[uint32]int16{9: -2}))))
		gm.Expect(packValue(NewValue([3]int{7, 8, 9}))).
			To(gm.Equal(packValue(NewValue([]int{7, 8, 9}))))
		// Nested containers and interface elements recurse reflectively.
		gm.Expect(packValue(NewValue(map[myKey]any{"k": []any{1, "s"}}))).
			To(gm.Equal(packValue(NewMapValue(map[any]any{"k": []any{1, "s"}}))))
	})

	gg.It("must pack named scalar types instead of panicking", func() {
		type myFloat float64
		type myBool bool
		gm.Expect(packValue(NewValue(myFloat(1.5)))).
			To(gm.Equal(packValue(NewValue(1.5))))
		gm.Expect(packValue(NewValue(myBool(true)))).
			To(gm.Equal(packValue(NewValue(true))))
	})

	gg.It("must pack multi-entry reflective maps byte-identically under canonical ordering", func() {
		typed := map[myKey]myVal{}
		untyped := map[any]any{}
		for i := 0; i < 12; i++ {
			k := string(rune('a' + i))
			typed[myKey(k)] = myVal(i)
			untyped[k] = i
		}
		packCanonical := func(v Value) []byte {
			buf := &bufferEx{dataBuffer: make([]byte, 8192)}
			buf.setCanonicalKeys(true)
			n, err := v.pack(buf)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			return buf.dataBuffer[:n]
		}
		gm.Expect(packCanonical(NewValue(typed))).
			To(gm.Equal(packCanonical(NewMapValue(untyped))))
	})

	gg.It("must return the original typed container from GetObject", func() {
		m := map[myKey]myVal{"k": 1}
		gm.Expect(NewValue(m).GetObject()).To(gm.Equal(m))
		l := []myVal{1, 2}
		gm.Expect(NewValue(l).GetObject()).To(gm.Equal(l))
	})

	gg.It("must pack reflective maps and lists without allocating", func() {
		buf := &bufferEx{dataBuffer: make([]byte, 8192)}
		for _, v := range []Value{
			NewValue(map[myKey]myVal{"alpha": 1, "beta": 2, "gamma": 3}),
			NewValue([]myVal{1, 2, 3}),
		} {
			allocs := testing.AllocsPerRun(100, func() {
				buf.dataOffset = 0
				if _, err := v.pack(buf); err != nil {
					panic(err)
				}
			})
			gm.Expect(allocs).To(gm.BeZero(), "reflective packing must not box entries")
		}
	})
})
