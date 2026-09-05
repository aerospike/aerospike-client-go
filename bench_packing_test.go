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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"strings"
	"testing"

	// "time"

	"maps"
	_ "net/http/pprof"
	"reflect"
	"slices"
)

var buf *benchBuffer

func init() {
	buf = &benchBuffer{dataBuffer: make([]byte, 1024*1024), dataOffset: 0}
}

func Benchmark_Pack_binary_Write(b *testing.B) {
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		buf.Reset()
		binary.Write(buf, binary.BigEndian, int64(0))
	}
}

func Benchmark_Pack_binary_PutUint64(b *testing.B) {
	buf := make([]byte, 8)
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(buf, 0)
	}
}

func doPack(val any, b *testing.B) {
	var err error
	v := NewValue(val)
	runtime.GC()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.dataOffset = 0
		_, err = v.pack(buf)
		if err != nil {
			panic(err)
		}
	}
}

func Benchmark_Pack_________Int64(b *testing.B) {
	val := rand.Int63()
	doPack(val, b)
}

func Benchmark_Pack_________Int32(b *testing.B) {
	val := rand.Int31()
	doPack(val, b)
}

func Benchmark_Pack_String______1(b *testing.B) {
	val := strings.Repeat("s", 1)
	doPack(val, b)
}

func Benchmark_Pack_String_____10(b *testing.B) {
	val := strings.Repeat("s", 10)
	doPack(val, b)
}

func Benchmark_Pack_String____100(b *testing.B) {
	val := strings.Repeat("s", 100)
	doPack(val, b)
}

func Benchmark_Pack_String___1000(b *testing.B) {
	val := strings.Repeat("s", 1000)
	doPack(val, b)
}

func Benchmark_Pack_String__10000(b *testing.B) {
	val := strings.Repeat("s", 10000)
	doPack(val, b)
}

func Benchmark_Pack_String_100000(b *testing.B) {
	val := strings.Repeat("s", 100000)
	doPack(val, b)
}

func Benchmark_Pack_Complex_IfcArray_Direct(b *testing.B) {
	val := []any{1, 1, 1, "a simple string", nil, rand.Int63(), []byte{12, 198, 211}}
	doPack(val, b)
}

var _ ListIter = myList([]string{})

// supports old generic slices
type myList []string

func (m myList) PackList(buf BufferEx) (int, error) {
	size := 0
	for _, elem := range m {
		n, err := packString(buf, elem)
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func (m myList) Len() int {
	return len(m)
}

func Benchmark_Pack_Complex_Array_ListIter(b *testing.B) {
	val := myList([]string{strings.Repeat("s", 1), strings.Repeat("s", 2), strings.Repeat("s", 3), strings.Repeat("s", 4), strings.Repeat("s", 5), strings.Repeat("s", 6), strings.Repeat("s", 7), strings.Repeat("s", 8), strings.Repeat("s", 9), strings.Repeat("s", 10)})
	doPack(val, b)
}

func Benchmark_Pack_Complex_ValueArray(b *testing.B) {
	val := []Value{NewValue(1), NewValue(strings.Repeat("s", 100000)), NewValue(1.75), NewValue(nil)}
	doPack(val, b)
}

func Benchmark_Pack_Complex_Map(b *testing.B) {
	val := map[any]any{
		rand.Int63(): rand.Int63(),
		nil:          1,
		"s":          491871,
		15892987:     strings.Repeat("s", 100),
		"s2":         []any{"a simple string", nil, rand.Int63(), []byte{12, 198, 211}},
	}
	doPack(val, b)
}

func Benchmark_Pack_Complex_JsonMap(b *testing.B) {
	val := map[string]any{
		"rand.Int63()": rand.Int63(),
		"nil":          1,
		"s":            491871,
		"15892987":     strings.Repeat("s", 100),
		"s2":           []any{"a simple string", nil, rand.Int63(), []byte{12, 198, 211}},
	}
	doPack(val, b)
}

// //////////////////////////////////////////////////////////////////////////////////////
type benchBuffer struct {
	dataBuffer []byte
	dataOffset int
}

// Int64ToBytes converts an int64 into slice of Bytes.
func (bb *benchBuffer) WriteInt64(num int64) int {
	return bb.WriteUint64(uint64(num))
}

// Uint64ToBytes converts an uint64 into slice of Bytes.
func (bb *benchBuffer) WriteUint64(num uint64) int {
	binary.BigEndian.PutUint64(bb.dataBuffer[bb.dataOffset:bb.dataOffset+8], num)
	bb.dataOffset += 8
	return 8
}

// Int32ToBytes converts an int32 to a byte slice of size 4
func (bb *benchBuffer) WriteInt32(num int32) int {
	return bb.WriteUint32(uint32(num))
}

// Uint32ToBytes converts an uint32 to a byte slice of size 4
func (bb *benchBuffer) WriteUint32(num uint32) int {
	binary.BigEndian.PutUint32(bb.dataBuffer[bb.dataOffset:bb.dataOffset+4], num)
	bb.dataOffset += 4
	return 4
}

// Int16ToBytes converts an int16 to slice of bytes
func (bb *benchBuffer) WriteInt16(num int16) int {
	return bb.WriteUint16(uint16(num))
}

// Int16ToBytes converts an int16 to slice of bytes
func (bb *benchBuffer) WriteUint16(num uint16) int {
	binary.BigEndian.PutUint16(bb.dataBuffer[bb.dataOffset:bb.dataOffset+2], num)
	bb.dataOffset += 2
	return 2
}

func (bb *benchBuffer) WriteFloat32(float float32) int {
	bits := math.Float32bits(float)
	binary.BigEndian.PutUint32(bb.dataBuffer[bb.dataOffset:bb.dataOffset+4], bits)
	bb.dataOffset += 4
	return 4
}

func (bb *benchBuffer) WriteFloat64(float float64) int {
	bits := math.Float64bits(float)
	binary.BigEndian.PutUint64(bb.dataBuffer[bb.dataOffset:bb.dataOffset+8], bits)
	bb.dataOffset += 8
	return 8
}

func (bb *benchBuffer) WriteBool(b bool) int {
	if b {
		bb.WriteByte(1)
	} else {
		bb.WriteByte(0)
	}
	return 1
}

//nolint:govet,stdmethods // deliberate bare signature, see BufferEx.
func (bb *benchBuffer) WriteByte(b byte) {
	bb.dataBuffer[bb.dataOffset] = b
	bb.dataOffset++
}

func (bb *benchBuffer) WriteString(s string) (int, Error) {
	copy(bb.dataBuffer[bb.dataOffset:bb.dataOffset+len(s)], s)
	bb.dataOffset += len(s)
	return len(s), nil
}

func (bb *benchBuffer) Write(b []byte) (int, Error) {
	copy(bb.dataBuffer[bb.dataOffset:bb.dataOffset+len(b)], b)
	bb.dataOffset += len(b)
	return len(b), nil
}

// --- MapValue (map[any]any) vs TypedMapValue (typed map) packing ---
//
// Both pack to the identical wire bytes; the difference under measurement is
// the per-entry interface boxing and dynamic dispatch the untyped map pays,
// versus the generic path. Each pair packs the same logical map.

func doPackValue(v Value, b *testing.B) {
	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.dataOffset = 0
		if _, err := v.pack(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func stringIntMaps(n int) (map[any]any, map[string]int) {
	untyped := make(map[any]any, n)
	typed := make(map[string]int, n)
	for i := 0; i < n; i++ {
		k := "key-" + strings.Repeat("x", 8) + string(rune('a'+i%26))
		untyped[k+string(rune('0'+i/26))] = i
		typed[k+string(rune('0'+i/26))] = i
	}
	return untyped, typed
}

func intInt64Maps(n int) (map[any]any, map[int]int64) {
	untyped := make(map[any]any, n)
	typed := make(map[int]int64, n)
	for i := 0; i < n; i++ {
		untyped[i] = int64(i * 1000)
		typed[i] = int64(i * 1000)
	}
	return untyped, typed
}

func Benchmark_Pack_MapValue_StringInt___10(b *testing.B) {
	m, _ := stringIntMaps(10)
	doPackValue(NewMapValue(m), b)
}

func Benchmark_Pack_TypedMapValue_StringInt___10(b *testing.B) {
	_, m := stringIntMaps(10)
	doPackValue(NewTypedMapValue(m), b)
}

func Benchmark_Pack_MapValue_StringInt__100(b *testing.B) {
	m, _ := stringIntMaps(100)
	doPackValue(NewMapValue(m), b)
}

func Benchmark_Pack_TypedMapValue_StringInt__100(b *testing.B) {
	_, m := stringIntMaps(100)
	doPackValue(NewTypedMapValue(m), b)
}

func Benchmark_Pack_MapValue_IntInt64____10(b *testing.B) {
	m, _ := intInt64Maps(10)
	doPackValue(NewMapValue(m), b)
}

func Benchmark_Pack_TypedMapValue_IntInt64____10(b *testing.B) {
	_, m := intInt64Maps(10)
	doPackValue(NewTypedMapValue(m), b)
}

func Benchmark_Pack_MapValue_IntInt64___100(b *testing.B) {
	m, _ := intInt64Maps(100)
	doPackValue(NewMapValue(m), b)
}

func Benchmark_Pack_TypedMapValue_IntInt64___100(b *testing.B) {
	_, m := intInt64Maps(100)
	doPackValue(NewTypedMapValue(m), b)
}

// --- MapIter: the third way to pack a map ---
//
// MapIter/ListIter are the documented reflection-free serialization hooks
// (value_helpers.go): the user's PackMap/PackList drives the exported Pack*
// helpers per entry, with no interface boxing anywhere. All Iter rows use
// hand-written implementations following the value_helpers.go template,
// measuring what a user-supplied iterator costs next to the typed values.

type benchStringIntIter map[string]int

func (m benchStringIntIter) PackMap(buf BufferEx) (int, error) {
	size := 0
	for k, v := range m {
		n, err := PackString(buf, k)
		size += n
		if err != nil {
			return size, err
		}
		n, err = PackInt64(buf, int64(v))
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func (m benchStringIntIter) Len() int { return len(m) }

type benchStringSliceIter []string

func (l benchStringSliceIter) PackList(buf BufferEx) (int, error) {
	size := 0
	for i := range l {
		n, err := PackString(buf, l[i])
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func (l benchStringSliceIter) Len() int { return len(l) }

type benchInt64SliceIter []int64

func (l benchInt64SliceIter) PackList(buf BufferEx) (int, error) {
	size := 0
	for i := range l {
		n, err := PackInt64(buf, l[i])
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func (l benchInt64SliceIter) Len() int { return len(l) }

type benchIntInt64Iter map[int]int64

func (m benchIntInt64Iter) PackMap(buf BufferEx) (int, error) {
	size := 0
	for k, v := range m {
		n, err := PackInt64(buf, int64(k))
		size += n
		if err != nil {
			return size, err
		}
		n, err = PackInt64(buf, v)
		size += n
		if err != nil {
			return size, err
		}
	}
	return size, nil
}

func (m benchIntInt64Iter) Len() int { return len(m) }

func Benchmark_Pack_MapIter_StringInt___10(b *testing.B) {
	_, m := stringIntMaps(10)
	doPackValue(NewMapperValue(benchStringIntIter(m)), b)
}

func Benchmark_Pack_MapIter_StringInt__100(b *testing.B) {
	_, m := stringIntMaps(100)
	doPackValue(NewMapperValue(benchStringIntIter(m)), b)
}

func Benchmark_Pack_MapIter_IntInt64____10(b *testing.B) {
	_, m := intInt64Maps(10)
	doPackValue(NewMapperValue(benchIntInt64Iter(m)), b)
}

func Benchmark_Pack_MapIter_IntInt64___100(b *testing.B) {
	_, m := intInt64Maps(100)
	doPackValue(NewMapperValue(benchIntInt64Iter(m)), b)
}

// The list trio mirrors the map trio: untyped ListValue (boxes per element),
// typed TypedListValue (whole-slice dispatch to monomorphic loops), and a
// hand-written user ListIter implementation.

func benchStrings(n int) ([]any, []string) {
	untyped := make([]any, n)
	typed := make([]string, n)
	for i := 0; i < n; i++ {
		s := fmt.Sprintf("key_string_%05d", i)
		untyped[i] = s
		typed[i] = s
	}
	return untyped, typed
}

func benchInt64s(n int) ([]any, []int64) {
	untyped := make([]any, n)
	typed := make([]int64, n)
	for i := 0; i < n; i++ {
		v := int64(i)*7919 - 1000
		untyped[i] = v
		typed[i] = v
	}
	return untyped, typed
}

func Benchmark_Pack_ListValue_String____100(b *testing.B) {
	l, _ := benchStrings(100)
	doPackValue(NewListValue(l), b)
}

func Benchmark_Pack_TypedListValue_String___100(b *testing.B) {
	_, l := benchStrings(100)
	doPackValue(NewTypedListValue(l), b)
}

func Benchmark_Pack_ListIter_String_____100(b *testing.B) {
	_, l := benchStrings(100)
	doPackValue(NewListerValue(benchStringSliceIter(l)), b)
}

func Benchmark_Pack_ListValue_Int64_____100(b *testing.B) {
	l, _ := benchInt64s(100)
	doPackValue(NewListValue(l), b)
}

func Benchmark_Pack_TypedListValue_Int64____100(b *testing.B) {
	_, l := benchInt64s(100)
	doPackValue(NewTypedListValue(l), b)
}

func Benchmark_Pack_ListIter_Int64______100(b *testing.B) {
	_, l := benchInt64s(100)
	doPackValue(NewListerValue(benchInt64SliceIter(l)), b)
}

// Sequence-backed values: same data as the map/list trios, packed straight
// from iter.Seq2/iter.Seq without materializing.

func Benchmark_Pack_MapSeq_StringInt__100(b *testing.B) {
	_, m := stringIntMaps(100)
	doPackValue(NewSeqMapValue(maps.All(m), len(m)), b)
}

func Benchmark_Pack_MapSeq_IntInt64___100(b *testing.B) {
	_, m := intInt64Maps(100)
	doPackValue(NewSeqMapValue(maps.All(m), len(m)), b)
}

func Benchmark_Pack_ListSeq_String_____100(b *testing.B) {
	_, l := benchStrings(100)
	doPackValue(NewSeqListValue(slices.Values(l), len(l)), b)
}

func Benchmark_Pack_ListSeq_Int64______100(b *testing.B) {
	_, l := benchInt64s(100)
	doPackValue(NewSeqListValue(slices.Values(l), len(l)), b)
}

// An exotic shape that used to have a dedicated generics.go wrapper and now
// runs TypedMapValue's per-entry typed loop -- the documented cost of retiring
// the 142 stamped wrapper types for cold shapes.
func Benchmark_Pack_TypedMapValue_ExoticShape100(b *testing.B) {
	m := make(map[int16]uint32, 100)
	for i := 0; i < 100; i++ {
		m[int16(i)] = uint32(i * 7)
	}
	doPackValue(NewValue(m), b)
}

// The reflective values pack maps and slices whose types miss the NewValue
// fast path (named key/element types). Zero per-entry allocations; the
// per-entry cost is reflect's iterator, scratch copies, and Kind dispatch.

type benchNamedKey string

func Benchmark_Pack_ReflectMap_100(b *testing.B) {
	m := make(map[benchNamedKey]int64, 100)
	for i := 0; i < 100; i++ {
		m[benchNamedKey(fmt.Sprintf("key_string_%05d", i))] = int64(i)
	}
	doPackValue(NewValue(m), b)
}

type benchNamedElem int64

func Benchmark_Pack_ReflectList_100(b *testing.B) {
	l := make([]benchNamedElem, 100)
	for i := range l {
		l[i] = benchNamedElem(i * 7919)
	}
	doPackValue(NewValue(l), b)
}

// End-to-end NewValue + pack, with construction inside the timed loop.
// oldReflectFallbackValue replicates verbatim what value_reflect.go did for
// maps and slices missing the fast path before reflectMapValue/
// reflectListValue: materialize into map[any]any / []any, boxing every
// entry via reflection. The new system's fallback allocates only a small
// constant at construction (value struct, two scratch cells, map iterator)
// and nothing per entry, while typed-switch shapes allocate nothing at all.
func oldReflectFallbackValue(v any) Value {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Array, reflect.Slice:
		l := rv.Len()
		arr := make([]any, l)
		for i := 0; i < l; i++ {
			arr[i] = rv.Index(i).Interface()
		}
		return NewListValue(arr)
	case reflect.Map:
		l := rv.Len()
		amap := make(map[any]any, l)
		for _, i := range rv.MapKeys() {
			amap[i.Interface()] = rv.MapIndex(i).Interface()
		}
		return NewMapValue(amap)
	}
	return nil
}

func doNewValuePack(b *testing.B, mk func() Value) {
	runtime.GC()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.dataOffset = 0
		if _, err := mk().pack(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func namedMaps(n int) map[benchNamedKey]int64 {
	m := make(map[benchNamedKey]int64, n)
	for i := 0; i < n; i++ {
		m[benchNamedKey(fmt.Sprintf("key_string_%05d", i))] = int64(i)
	}
	return m
}

func namedSlices(n int) []benchNamedElem {
	l := make([]benchNamedElem, n)
	for i := range l {
		l[i] = benchNamedElem(i * 7919)
	}
	return l
}

var benchSizes = []int{1, 10, 100, 1000}

func Benchmark_NewValuePack_Map_Named_Old(b *testing.B) {
	for _, n := range benchSizes {
		m := namedMaps(n)
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			doNewValuePack(b, func() Value { return oldReflectFallbackValue(m) })
		})
	}
}

func Benchmark_NewValuePack_Map_Named_New(b *testing.B) {
	for _, n := range benchSizes {
		m := namedMaps(n)
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			doNewValuePack(b, func() Value { return NewValue(m) })
		})
	}
}

func Benchmark_NewValuePack_List_Named_Old(b *testing.B) {
	for _, n := range benchSizes {
		l := namedSlices(n)
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			doNewValuePack(b, func() Value { return oldReflectFallbackValue(l) })
		})
	}
}

func Benchmark_NewValuePack_List_Named_New(b *testing.B) {
	for _, n := range benchSizes {
		l := namedSlices(n)
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			doNewValuePack(b, func() Value { return NewValue(l) })
		})
	}
}

// Unnamed primitive shapes hit the generated type switch: NewValue is a
// zero-alloc conversion even with construction in the loop.
func Benchmark_NewValuePack_Map_Typed_New(b *testing.B) {
	for _, n := range benchSizes {
		_, m := stringIntMaps(n)
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			doNewValuePack(b, func() Value { return NewValue(m) })
		})
	}
}

func Benchmark_NewValuePack_List_Typed_New(b *testing.B) {
	for _, n := range benchSizes {
		_, l := benchInt64s(n)
		b.Run(fmt.Sprintf("%d", n), func(b *testing.B) {
			doNewValuePack(b, func() Value { return NewValue(l) })
		})
	}
}
