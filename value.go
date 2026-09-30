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
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strconv"

	"github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

// this function will be set in value_slow file if included
var newValueReflect func(any) Value

// MapPair is used when the client returns sorted maps from the server
// Since the default map in Go is a hash map, we will use a slice
// to return the results in server order
type MapPair struct{ Key, Value any }

// Value interface is used to efficiently serialize objects into the wire protocol.
type Value interface {

	// Calculate number of vl.bytes necessary to serialize the value in the wire protocol.
	EstimateSize() (int, Error)

	// Serialize the value in the wire protocol.
	write(cmd BufferEx) (int, Error)

	// Serialize the value using MessagePack.
	pack(cmd BufferEx) (int, Error)

	// GetType returns wire protocol value type.
	GetType() int

	// GetObject returns original value as an any.
	GetObject() any

	// String implements Stringer interface.
	String() string
}

//revive:disable

// AerospikeBlob interface allows the user to write a conversion function from their value to []bytes.
type AerospikeBlob interface {
	// EncodeBlob returns a byte slice representing the encoding of the
	// receiver for transmission to a Decoder, usually of the same
	// concrete type.
	EncodeBlob() ([]byte, error)
}

//revive:enable

// tryConcreteValue will return an aerospike value.
// If the encoder does not exist, it will not try to use reflection.
func tryConcreteValue(v any) Value {
	switch val := v.(type) {
	case Value:
		return val
	case int:
		return IntegerValue(val)
	case int64:
		return LongValue(val)
	case string:
		return StringValue(val)
	case []any:
		return ListValue(val)
	case map[string]any:
		return JsonValue(val)
	case map[any]any:
		return NewMapValue(val)
	case nil:
		return nullValue
	case []Value:
		return NewValueArray(val)
	case []byte:
		return BytesValue(val)
	case int8:
		return IntegerValue(int(val))
	case int16:
		return IntegerValue(int(val))
	case int32:
		return IntegerValue(int(val))
	case uint8: // byte supported here
		return IntegerValue(int(val))
	case uint16:
		return IntegerValue(int(val))
	case uint32:
		return IntegerValue(int(val))
	case float32:
		return FloatValue(float64(val))
	case float64:
		return FloatValue(val)
	case uint:
		// if it doesn't overflow int64, it is OK
		if int64(val) >= 0 {
			return LongValue(int64(val))
		}
	case bool:
		return BoolValue(val)
	case MapIter:
		return NewMapperValue(val)
	case ListIter:
		return NewListerValue(val)
	case AerospikeBlob:
		return NewBlobValue(val)

	/*
		The following cases avoid reflection by routing common typed maps and
		slices to the generic TypedMapValue/TypedListValue values.
		If you have custom type aliases in your code, cast them to the plain
		map/slice type (or use TypedMapValue/TypedListValue directly) to avoid
		hitting the reflection.
	*/
	case []string:
		return TypedListValue[string](val)
	case []int:
		return TypedListValue[int](val)
	case []int8:
		return TypedListValue[int8](val)
	case []int16:
		return TypedListValue[int16](val)
	case []int32:
		return TypedListValue[int32](val)
	case []int64:
		return TypedListValue[int64](val)
	case []uint:
		return TypedListValue[uint](val)
	case []uint16:
		return TypedListValue[uint16](val)
	case []uint32:
		return TypedListValue[uint32](val)
	case []uint64:
		return TypedListValue[uint64](val)
	case []float32:
		return TypedListValue[float32](val)
	case []float64:
		return TypedListValue[float64](val)
	case []bool:
		return TypedListValue[bool](val)
	case [][]byte:
		return TypedListValue[[]byte](val)
	case map[string]string:
		return TypedMapValue[string, string](val)
	case map[string]int:
		return TypedMapValue[string, int](val)
	case map[string]int8:
		return TypedMapValue[string, int8](val)
	case map[string]int16:
		return TypedMapValue[string, int16](val)
	case map[string]int32:
		return TypedMapValue[string, int32](val)
	case map[string]int64:
		return TypedMapValue[string, int64](val)
	case map[string]uint:
		return TypedMapValue[string, uint](val)
	case map[string]uint8:
		return TypedMapValue[string, uint8](val)
	case map[string]uint16:
		return TypedMapValue[string, uint16](val)
	case map[string]uint32:
		return TypedMapValue[string, uint32](val)
	case map[string]uint64:
		return TypedMapValue[string, uint64](val)
	case map[string]float32:
		return TypedMapValue[string, float32](val)
	case map[string]float64:
		return TypedMapValue[string, float64](val)
	case map[string]bool:
		return TypedMapValue[string, bool](val)
	case map[string][]byte:
		return TypedMapValue[string, []byte](val)
	case map[int]string:
		return TypedMapValue[int, string](val)
	case map[int]int:
		return TypedMapValue[int, int](val)
	case map[int]int8:
		return TypedMapValue[int, int8](val)
	case map[int]int16:
		return TypedMapValue[int, int16](val)
	case map[int]int32:
		return TypedMapValue[int, int32](val)
	case map[int]int64:
		return TypedMapValue[int, int64](val)
	case map[int]uint:
		return TypedMapValue[int, uint](val)
	case map[int]uint8:
		return TypedMapValue[int, uint8](val)
	case map[int]uint16:
		return TypedMapValue[int, uint16](val)
	case map[int]uint32:
		return TypedMapValue[int, uint32](val)
	case map[int]uint64:
		return TypedMapValue[int, uint64](val)
	case map[int]float32:
		return TypedMapValue[int, float32](val)
	case map[int]float64:
		return TypedMapValue[int, float64](val)
	case map[int]bool:
		return TypedMapValue[int, bool](val)
	case map[int][]byte:
		return TypedMapValue[int, []byte](val)
	case map[int]any:
		return TypedMapValue[int, any](val)
	case map[int8]string:
		return TypedMapValue[int8, string](val)
	case map[int8]int:
		return TypedMapValue[int8, int](val)
	case map[int8]int8:
		return TypedMapValue[int8, int8](val)
	case map[int8]int16:
		return TypedMapValue[int8, int16](val)
	case map[int8]int32:
		return TypedMapValue[int8, int32](val)
	case map[int8]int64:
		return TypedMapValue[int8, int64](val)
	case map[int8]uint:
		return TypedMapValue[int8, uint](val)
	case map[int8]uint8:
		return TypedMapValue[int8, uint8](val)
	case map[int8]uint16:
		return TypedMapValue[int8, uint16](val)
	case map[int8]uint32:
		return TypedMapValue[int8, uint32](val)
	case map[int8]uint64:
		return TypedMapValue[int8, uint64](val)
	case map[int8]float32:
		return TypedMapValue[int8, float32](val)
	case map[int8]float64:
		return TypedMapValue[int8, float64](val)
	case map[int8]bool:
		return TypedMapValue[int8, bool](val)
	case map[int8][]byte:
		return TypedMapValue[int8, []byte](val)
	case map[int8]any:
		return TypedMapValue[int8, any](val)
	case map[int16]string:
		return TypedMapValue[int16, string](val)
	case map[int16]int:
		return TypedMapValue[int16, int](val)
	case map[int16]int8:
		return TypedMapValue[int16, int8](val)
	case map[int16]int16:
		return TypedMapValue[int16, int16](val)
	case map[int16]int32:
		return TypedMapValue[int16, int32](val)
	case map[int16]int64:
		return TypedMapValue[int16, int64](val)
	case map[int16]uint:
		return TypedMapValue[int16, uint](val)
	case map[int16]uint8:
		return TypedMapValue[int16, uint8](val)
	case map[int16]uint16:
		return TypedMapValue[int16, uint16](val)
	case map[int16]uint32:
		return TypedMapValue[int16, uint32](val)
	case map[int16]uint64:
		return TypedMapValue[int16, uint64](val)
	case map[int16]float32:
		return TypedMapValue[int16, float32](val)
	case map[int16]float64:
		return TypedMapValue[int16, float64](val)
	case map[int16]bool:
		return TypedMapValue[int16, bool](val)
	case map[int16][]byte:
		return TypedMapValue[int16, []byte](val)
	case map[int16]any:
		return TypedMapValue[int16, any](val)
	case map[int32]string:
		return TypedMapValue[int32, string](val)
	case map[int32]int:
		return TypedMapValue[int32, int](val)
	case map[int32]int8:
		return TypedMapValue[int32, int8](val)
	case map[int32]int16:
		return TypedMapValue[int32, int16](val)
	case map[int32]int32:
		return TypedMapValue[int32, int32](val)
	case map[int32]int64:
		return TypedMapValue[int32, int64](val)
	case map[int32]uint:
		return TypedMapValue[int32, uint](val)
	case map[int32]uint8:
		return TypedMapValue[int32, uint8](val)
	case map[int32]uint16:
		return TypedMapValue[int32, uint16](val)
	case map[int32]uint32:
		return TypedMapValue[int32, uint32](val)
	case map[int32]uint64:
		return TypedMapValue[int32, uint64](val)
	case map[int32]float32:
		return TypedMapValue[int32, float32](val)
	case map[int32]float64:
		return TypedMapValue[int32, float64](val)
	case map[int32]bool:
		return TypedMapValue[int32, bool](val)
	case map[int32][]byte:
		return TypedMapValue[int32, []byte](val)
	case map[int32]any:
		return TypedMapValue[int32, any](val)
	case map[int64]string:
		return TypedMapValue[int64, string](val)
	case map[int64]int:
		return TypedMapValue[int64, int](val)
	case map[int64]int8:
		return TypedMapValue[int64, int8](val)
	case map[int64]int16:
		return TypedMapValue[int64, int16](val)
	case map[int64]int32:
		return TypedMapValue[int64, int32](val)
	case map[int64]int64:
		return TypedMapValue[int64, int64](val)
	case map[int64]uint:
		return TypedMapValue[int64, uint](val)
	case map[int64]uint8:
		return TypedMapValue[int64, uint8](val)
	case map[int64]uint16:
		return TypedMapValue[int64, uint16](val)
	case map[int64]uint32:
		return TypedMapValue[int64, uint32](val)
	case map[int64]uint64:
		return TypedMapValue[int64, uint64](val)
	case map[int64]float32:
		return TypedMapValue[int64, float32](val)
	case map[int64]float64:
		return TypedMapValue[int64, float64](val)
	case map[int64]bool:
		return TypedMapValue[int64, bool](val)
	case map[int64][]byte:
		return TypedMapValue[int64, []byte](val)
	case map[int64]any:
		return TypedMapValue[int64, any](val)
	case map[uint]string:
		return TypedMapValue[uint, string](val)
	case map[uint]int:
		return TypedMapValue[uint, int](val)
	case map[uint]int8:
		return TypedMapValue[uint, int8](val)
	case map[uint]int16:
		return TypedMapValue[uint, int16](val)
	case map[uint]int32:
		return TypedMapValue[uint, int32](val)
	case map[uint]int64:
		return TypedMapValue[uint, int64](val)
	case map[uint]uint:
		return TypedMapValue[uint, uint](val)
	case map[uint]uint8:
		return TypedMapValue[uint, uint8](val)
	case map[uint]uint16:
		return TypedMapValue[uint, uint16](val)
	case map[uint]uint32:
		return TypedMapValue[uint, uint32](val)
	case map[uint]uint64:
		return TypedMapValue[uint, uint64](val)
	case map[uint]float32:
		return TypedMapValue[uint, float32](val)
	case map[uint]float64:
		return TypedMapValue[uint, float64](val)
	case map[uint]bool:
		return TypedMapValue[uint, bool](val)
	case map[uint][]byte:
		return TypedMapValue[uint, []byte](val)
	case map[uint]any:
		return TypedMapValue[uint, any](val)
	case map[uint8]string:
		return TypedMapValue[uint8, string](val)
	case map[uint8]int:
		return TypedMapValue[uint8, int](val)
	case map[uint8]int8:
		return TypedMapValue[uint8, int8](val)
	case map[uint8]int16:
		return TypedMapValue[uint8, int16](val)
	case map[uint8]int32:
		return TypedMapValue[uint8, int32](val)
	case map[uint8]int64:
		return TypedMapValue[uint8, int64](val)
	case map[uint8]uint:
		return TypedMapValue[uint8, uint](val)
	case map[uint8]uint8:
		return TypedMapValue[uint8, uint8](val)
	case map[uint8]uint16:
		return TypedMapValue[uint8, uint16](val)
	case map[uint8]uint32:
		return TypedMapValue[uint8, uint32](val)
	case map[uint8]uint64:
		return TypedMapValue[uint8, uint64](val)
	case map[uint8]float32:
		return TypedMapValue[uint8, float32](val)
	case map[uint8]float64:
		return TypedMapValue[uint8, float64](val)
	case map[uint8]bool:
		return TypedMapValue[uint8, bool](val)
	case map[uint8][]byte:
		return TypedMapValue[uint8, []byte](val)
	case map[uint8]any:
		return TypedMapValue[uint8, any](val)
	case map[uint16]string:
		return TypedMapValue[uint16, string](val)
	case map[uint16]int:
		return TypedMapValue[uint16, int](val)
	case map[uint16]int8:
		return TypedMapValue[uint16, int8](val)
	case map[uint16]int16:
		return TypedMapValue[uint16, int16](val)
	case map[uint16]int32:
		return TypedMapValue[uint16, int32](val)
	case map[uint16]int64:
		return TypedMapValue[uint16, int64](val)
	case map[uint16]uint:
		return TypedMapValue[uint16, uint](val)
	case map[uint16]uint8:
		return TypedMapValue[uint16, uint8](val)
	case map[uint16]uint16:
		return TypedMapValue[uint16, uint16](val)
	case map[uint16]uint32:
		return TypedMapValue[uint16, uint32](val)
	case map[uint16]uint64:
		return TypedMapValue[uint16, uint64](val)
	case map[uint16]float32:
		return TypedMapValue[uint16, float32](val)
	case map[uint16]float64:
		return TypedMapValue[uint16, float64](val)
	case map[uint16]bool:
		return TypedMapValue[uint16, bool](val)
	case map[uint16][]byte:
		return TypedMapValue[uint16, []byte](val)
	case map[uint16]any:
		return TypedMapValue[uint16, any](val)
	case map[uint32]string:
		return TypedMapValue[uint32, string](val)
	case map[uint32]int:
		return TypedMapValue[uint32, int](val)
	case map[uint32]int8:
		return TypedMapValue[uint32, int8](val)
	case map[uint32]int16:
		return TypedMapValue[uint32, int16](val)
	case map[uint32]int32:
		return TypedMapValue[uint32, int32](val)
	case map[uint32]int64:
		return TypedMapValue[uint32, int64](val)
	case map[uint32]uint:
		return TypedMapValue[uint32, uint](val)
	case map[uint32]uint8:
		return TypedMapValue[uint32, uint8](val)
	case map[uint32]uint16:
		return TypedMapValue[uint32, uint16](val)
	case map[uint32]uint32:
		return TypedMapValue[uint32, uint32](val)
	case map[uint32]uint64:
		return TypedMapValue[uint32, uint64](val)
	case map[uint32]float32:
		return TypedMapValue[uint32, float32](val)
	case map[uint32]float64:
		return TypedMapValue[uint32, float64](val)
	case map[uint32]bool:
		return TypedMapValue[uint32, bool](val)
	case map[uint32][]byte:
		return TypedMapValue[uint32, []byte](val)
	case map[uint32]any:
		return TypedMapValue[uint32, any](val)
	case map[uint64]string:
		return TypedMapValue[uint64, string](val)
	case map[uint64]int:
		return TypedMapValue[uint64, int](val)
	case map[uint64]int8:
		return TypedMapValue[uint64, int8](val)
	case map[uint64]int16:
		return TypedMapValue[uint64, int16](val)
	case map[uint64]int32:
		return TypedMapValue[uint64, int32](val)
	case map[uint64]int64:
		return TypedMapValue[uint64, int64](val)
	case map[uint64]uint:
		return TypedMapValue[uint64, uint](val)
	case map[uint64]uint8:
		return TypedMapValue[uint64, uint8](val)
	case map[uint64]uint16:
		return TypedMapValue[uint64, uint16](val)
	case map[uint64]uint32:
		return TypedMapValue[uint64, uint32](val)
	case map[uint64]uint64:
		return TypedMapValue[uint64, uint64](val)
	case map[uint64]float32:
		return TypedMapValue[uint64, float32](val)
	case map[uint64]float64:
		return TypedMapValue[uint64, float64](val)
	case map[uint64]bool:
		return TypedMapValue[uint64, bool](val)
	case map[uint64][]byte:
		return TypedMapValue[uint64, []byte](val)
	case map[uint64]any:
		return TypedMapValue[uint64, any](val)
	case map[float32]string:
		return TypedMapValue[float32, string](val)
	case map[float32]int:
		return TypedMapValue[float32, int](val)
	case map[float32]int8:
		return TypedMapValue[float32, int8](val)
	case map[float32]int16:
		return TypedMapValue[float32, int16](val)
	case map[float32]int32:
		return TypedMapValue[float32, int32](val)
	case map[float32]int64:
		return TypedMapValue[float32, int64](val)
	case map[float32]uint:
		return TypedMapValue[float32, uint](val)
	case map[float32]uint8:
		return TypedMapValue[float32, uint8](val)
	case map[float32]uint16:
		return TypedMapValue[float32, uint16](val)
	case map[float32]uint32:
		return TypedMapValue[float32, uint32](val)
	case map[float32]uint64:
		return TypedMapValue[float32, uint64](val)
	case map[float32]float32:
		return TypedMapValue[float32, float32](val)
	case map[float32]float64:
		return TypedMapValue[float32, float64](val)
	case map[float32]bool:
		return TypedMapValue[float32, bool](val)
	case map[float32][]byte:
		return TypedMapValue[float32, []byte](val)
	case map[float32]any:
		return TypedMapValue[float32, any](val)
	case map[float64]string:
		return TypedMapValue[float64, string](val)
	case map[float64]int:
		return TypedMapValue[float64, int](val)
	case map[float64]int8:
		return TypedMapValue[float64, int8](val)
	case map[float64]int16:
		return TypedMapValue[float64, int16](val)
	case map[float64]int32:
		return TypedMapValue[float64, int32](val)
	case map[float64]int64:
		return TypedMapValue[float64, int64](val)
	case map[float64]uint:
		return TypedMapValue[float64, uint](val)
	case map[float64]uint8:
		return TypedMapValue[float64, uint8](val)
	case map[float64]uint16:
		return TypedMapValue[float64, uint16](val)
	case map[float64]uint32:
		return TypedMapValue[float64, uint32](val)
	case map[float64]uint64:
		return TypedMapValue[float64, uint64](val)
	case map[float64]float32:
		return TypedMapValue[float64, float32](val)
	case map[float64]float64:
		return TypedMapValue[float64, float64](val)
	case map[float64]bool:
		return TypedMapValue[float64, bool](val)
	case map[float64][]byte:
		return TypedMapValue[float64, []byte](val)
	case map[float64]any:
		return TypedMapValue[float64, any](val)
	}

	return nil
}

// OpResults encapsulates the results of batch read operations
type OpResults []any

// NewValue generates a new Value object based on the type.
// If the type is not supported, NewValue will panic.
// This method is a convenience method, and should not be used
// when absolute performance is required unless for the reason mentioned below.
//
// If you have custom maps or slices like:
//
//	type MyMap map[primitive1]primitive2, eg: map[int]string
//
// or
//
//	type MySlice []primitive, eg: []float64
//
// cast them to their primitive type when passing them to this method:
//
//	v := NewValue(map[int]string(myVar))
//	v := NewValue([]float64(myVar))
//
// This way you will avoid hitting reflection.
// To completely avoid reflection in the library,
// use the build tag: as_performance while building your program.
func NewValue(v any) Value {
	if value := tryConcreteValue(v); value != nil {
		return value
	}

	if newValueReflect != nil {
		if res := newValueReflect(v); res != nil {
			return res
		}
	}

	// panic for anything that is not supported.
	panic(newError(types.TYPE_NOT_SUPPORTED, fmt.Sprintf("Value type '%v' (%s) not supported (if you are compiling via 'as_performance' tag, use cast either to primitives, or use ListIter or MapIter interfaces.)", v, reflect.TypeOf(v).String())))
}

// NullValue is an empty value.
type NullValue struct{}

var nullValue NullValue

// NewNullValue generates a NullValue instance.
func NewNullValue() NullValue {
	return nullValue
}

// EstimateSize returns the size of the NullValue in wire protocol.
func (vl NullValue) EstimateSize() (int, Error) {
	return 0, nil
}

func (vl NullValue) write(cmd BufferEx) (int, Error) {
	return 0, nil
}

func (vl NullValue) pack(cmd BufferEx) (int, Error) {
	return packNil(cmd)
}

// GetType returns wire protocol value type.
func (vl NullValue) GetType() int {
	return ParticleType.NULL
}

// GetObject returns original value as an any.
func (vl NullValue) GetObject() any {
	return nil
}

func (vl NullValue) String() string {
	return ""
}

///////////////////////////////////////////////////////////////////////////////

// InfinityValue is an empty value.
type InfinityValue struct{}

var infinityValue InfinityValue

// NewInfinityValue generates a InfinityValue instance.
func NewInfinityValue() InfinityValue {
	return infinityValue
}

// EstimateSize returns the size of the InfinityValue in wire protocol.
func (vl InfinityValue) EstimateSize() (int, Error) {
	return 0, nil
}

func (vl InfinityValue) write(cmd BufferEx) (int, Error) {
	return 0, nil
}

func (vl InfinityValue) pack(cmd BufferEx) (int, Error) {
	return packInfinity(cmd)
}

// GetType returns wire protocol value type.
func (vl InfinityValue) GetType() int {
	panic("Invalid particle type: INF")
}

// GetObject returns original value as an any.
func (vl InfinityValue) GetObject() any {
	return nil
}

func (vl InfinityValue) String() string {
	return "INF"
}

///////////////////////////////////////////////////////////////////////////////

// WildCardValue is an empty value.
type WildCardValue struct{}

var wildCardValue WildCardValue

// NewWildCardValue generates a WildCardValue instance.
func NewWildCardValue() WildCardValue {
	return wildCardValue
}

// EstimateSize returns the size of the WildCardValue in wire protocol.
func (vl WildCardValue) EstimateSize() (int, Error) {
	return 0, nil
}

func (vl WildCardValue) write(cmd BufferEx) (int, Error) {
	return 0, nil
}

func (vl WildCardValue) pack(cmd BufferEx) (int, Error) {
	return packWildCard(cmd)
}

// GetType returns wire protocol value type.
func (vl WildCardValue) GetType() int {
	panic("Invalid particle type: WildCard")
}

// GetObject returns original value as an any.
func (vl WildCardValue) GetObject() any {
	return nil
}

func (vl WildCardValue) String() string {
	return "*"
}

///////////////////////////////////////////////////////////////////////////////

// BytesValue encapsulates an array of bytes.
type BytesValue []byte

// NewBytesValue generates a ByteValue instance.
func NewBytesValue(bytes []byte) BytesValue {
	return BytesValue(bytes)
}

// NewBlobValue accepts an AerospikeBlob interface, and automatically
// converts it to a BytesValue.
// If Encode returns an err, it will panic.
func NewBlobValue(object AerospikeBlob) BytesValue {
	buf, err := object.EncodeBlob()
	if err != nil {
		panic(err)
	}

	return NewBytesValue(buf)
}

// EstimateSize returns the size of the BytesValue in wire protocol.
func (vl BytesValue) EstimateSize() (int, Error) {
	return len(vl), nil
}

func (vl BytesValue) write(cmd BufferEx) (int, Error) {
	return cmd.Write(vl)
}

func (vl BytesValue) pack(cmd BufferEx) (int, Error) {
	return packBytes(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl BytesValue) GetType() int {
	return ParticleType.BLOB
}

// GetObject returns original value as an any.
func (vl BytesValue) GetObject() any {
	return []byte(vl)
}

// String implements Stringer interface.
func (vl BytesValue) String() string {
	return fmt.Sprintf("% 02x", []byte(vl))
}

///////////////////////////////////////////////////////////////////////////////

// StringValue encapsulates a string value.
type StringValue string

// NewStringValue generates a StringValue instance.
func NewStringValue(value string) StringValue {
	return StringValue(value)
}

// EstimateSize returns the size of the StringValue in wire protocol.
func (vl StringValue) EstimateSize() (int, Error) {
	return len(vl), nil
}

func (vl StringValue) write(cmd BufferEx) (int, Error) {
	return cmd.WriteString(string(vl))
}

func (vl StringValue) pack(cmd BufferEx) (int, Error) {
	return packString(cmd, string(vl))
}

// GetType returns wire protocol value type.
func (vl StringValue) GetType() int {
	return ParticleType.STRING
}

// GetObject returns original value as an any.
func (vl StringValue) GetObject() any {
	return string(vl)
}

// String implements Stringer interface.
func (vl StringValue) String() string {
	return string(vl)
}

///////////////////////////////////////////////////////////////////////////////

// IntegerValue encapsulates an integer value.
type IntegerValue int

// NewIntegerValue generates an IntegerValue instance.
func NewIntegerValue(value int) IntegerValue {
	return IntegerValue(value)
}

// EstimateSize returns the size of the IntegerValue in wire protocol.
func (vl IntegerValue) EstimateSize() (int, Error) {
	return 8, nil
}

func (vl IntegerValue) write(cmd BufferEx) (int, Error) {
	n := cmd.WriteInt64(int64(vl))
	return n, nil
}

func (vl IntegerValue) pack(cmd BufferEx) (int, Error) {
	return packAInt64(cmd, int64(vl))
}

// GetType returns wire protocol value type.
func (vl IntegerValue) GetType() int {
	return ParticleType.INTEGER
}

// GetObject returns original value as an any.
func (vl IntegerValue) GetObject() any {
	return int(vl)
}

// String implements Stringer interface.
func (vl IntegerValue) String() string {
	return strconv.Itoa(int(vl))
}

///////////////////////////////////////////////////////////////////////////////

// LongValue encapsulates an int64 value.
type LongValue int64

// NewLongValue generates a LongValue instance.
func NewLongValue(value int64) LongValue {
	return LongValue(value)
}

// EstimateSize returns the size of the LongValue in wire protocol.
func (vl LongValue) EstimateSize() (int, Error) {
	return 8, nil
}

func (vl LongValue) write(cmd BufferEx) (int, Error) {
	n := cmd.WriteInt64(int64(vl))
	return n, nil
}

func (vl LongValue) pack(cmd BufferEx) (int, Error) {
	return packAInt64(cmd, int64(vl))
}

// GetType returns wire protocol value type.
func (vl LongValue) GetType() int {
	return ParticleType.INTEGER
}

// GetObject returns original value as an any.
func (vl LongValue) GetObject() any {
	return int64(vl)
}

// String implements Stringer interface.
func (vl LongValue) String() string {
	return strconv.Itoa(int(vl))
}

///////////////////////////////////////////////////////////////////////////////

// FloatValue encapsulates an float64 value.
type FloatValue float64

// NewFloatValue generates a FloatValue instance.
func NewFloatValue(value float64) FloatValue {
	return FloatValue(value)
}

// EstimateSize returns the size of the FloatValue in wire protocol.
func (vl FloatValue) EstimateSize() (int, Error) {
	return 8, nil
}

func (vl FloatValue) write(cmd BufferEx) (int, Error) {
	n := cmd.WriteFloat64(float64(vl))
	return n, nil
}

func (vl FloatValue) pack(cmd BufferEx) (int, Error) {
	return packFloat64(cmd, float64(vl))
}

// GetType returns wire protocol value type.
func (vl FloatValue) GetType() int {
	return ParticleType.FLOAT
}

// GetObject returns original value as an any.
func (vl FloatValue) GetObject() any {
	return float64(vl)
}

// String implements Stringer interface.
func (vl FloatValue) String() string {
	return (fmt.Sprintf("%f", vl))
}

///////////////////////////////////////////////////////////////////////////////

// BoolValue encapsulates a boolean value.
// Supported by Aerospike server v5.6+ only.
type BoolValue bool

// NewBoolValue generates a BoolValue instance.
func NewBoolValue(b bool) BoolValue {
	return BoolValue(b)
}

// EstimateSize returns the size of the BoolValue in wire protocol.
func (vb BoolValue) EstimateSize() (int, Error) {
	return PackBool(nil, bool(vb))
}

func (vb BoolValue) write(cmd BufferEx) (int, Error) {
	n := cmd.WriteBool(bool(vb))
	return n, nil
}

func (vb BoolValue) pack(cmd BufferEx) (int, Error) {
	return PackBool(cmd, bool(vb))
}

// GetType returns wire protocol value type.
func (vb BoolValue) GetType() int {
	return ParticleType.BOOL
}

// GetObject returns original value as an any.
func (vb BoolValue) GetObject() any {
	return bool(vb)
}

// String implements Stringer interface.
func (vb BoolValue) String() string {
	return (fmt.Sprintf("%v", bool(vb)))
}

///////////////////////////////////////////////////////////////////////////////

// ValueArray encapsulates an array of Value.
// Supported by Aerospike 3+ servers only.
type ValueArray []Value

// NewValueArray generates a ValueArray instance.
func NewValueArray(array []Value) *ValueArray {
	// return &ValueArray{*NewListerValue(valueList(array))}
	res := ValueArray(array)
	return &res
}

// EstimateSize returns the size of the ValueArray in wire protocol.
func (va ValueArray) EstimateSize() (int, Error) {
	return packValueArray(nil, va)
}

func (va ValueArray) write(cmd BufferEx) (int, Error) {
	return packValueArray(cmd, va)
}

func (va ValueArray) pack(cmd BufferEx) (int, Error) {
	return packValueArray(cmd, []Value(va))
}

// GetType returns wire protocol value type.
func (va ValueArray) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an any.
func (va ValueArray) GetObject() any {
	return []Value(va)
}

// String implements Stringer interface.
func (va ValueArray) String() string {
	return fmt.Sprintf("%v", []Value(va))
}

///////////////////////////////////////////////////////////////////////////////

// ListValue encapsulates any arbitrary array.
// Supported by Aerospike 3+ servers only.
type ListValue []any

// NewListValue generates a ListValue instance.
func NewListValue(list []any) ListValue {
	return ListValue(list)
}

// EstimateSize returns the size of the ListValue in wire protocol.
func (vl ListValue) EstimateSize() (int, Error) {
	return packIfcList(nil, vl)
}

func (vl ListValue) write(cmd BufferEx) (int, Error) {
	return packIfcList(cmd, vl)
}

func (vl ListValue) pack(cmd BufferEx) (int, Error) {
	return packIfcList(cmd, []any(vl))
}

// GetType returns wire protocol value type.
func (vl ListValue) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an any.
func (vl ListValue) GetObject() any {
	return []any(vl)
}

// String implements Stringer interface.
func (vl ListValue) String() string {
	return fmt.Sprintf("%v", []any(vl))
}

///////////////////////////////////////////////////////////////////////////////

// ListerValue encapsulates any arbitrary array.
// Supported by Aerospike 3+ servers only.
type ListerValue struct {
	list ListIter
}

// NewListerValue generates a NewListerValue instance.
func NewListerValue(list ListIter) *ListerValue {
	res := &ListerValue{
		list: list,
	}

	return res
}

// EstimateSize returns the size of the ListerValue in wire protocol.
func (vl *ListerValue) EstimateSize() (int, Error) {
	return packList(nil, vl.list)
}

func (vl *ListerValue) write(cmd BufferEx) (int, Error) {
	return packList(cmd, vl.list)
}

func (vl *ListerValue) pack(cmd BufferEx) (int, Error) {
	return packList(cmd, vl.list)
}

// GetType returns wire protocol value type.
func (vl *ListerValue) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an any.
func (vl *ListerValue) GetObject() any {
	return vl.list
}

// String implements Stringer interface.
func (vl *ListerValue) String() string {
	return fmt.Sprintf("%v", vl.list)
}

///////////////////////////////////////////////////////////////////////////////

// MapValue encapsulates an arbitrary map.
// Supported by Aerospike 3+ servers only.
type MapValue map[any]any

// NewMapValue generates a MapValue instance.
func NewMapValue(vmap map[any]any) MapValue {
	return MapValue(vmap)
}

// EstimateSize returns the size of the MapValue in wire protocol.
func (vl MapValue) EstimateSize() (int, Error) {
	return packIfcMap(nil, vl)
}

func (vl MapValue) write(cmd BufferEx) (int, Error) {
	return packIfcMap(cmd, vl)
}

func (vl MapValue) pack(cmd BufferEx) (int, Error) {
	return packIfcMap(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl MapValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an any.
func (vl MapValue) GetObject() any {
	return map[any]any(vl)
}

func (vl MapValue) String() string {
	return fmt.Sprintf("%v", map[any]any(vl))
}

///////////////////////////////////////////////////////////////////////////////

type ValidMapKey interface {
	comparable
	~int | ~uint |
		~int64 | ~int32 | ~int16 | ~int8 |
		~uint64 | ~uint32 | ~uint16 | ~uint8 |
		~float64 | ~float32 |
		~string
}

// TypedMapValue encapsulates an arbitrary map.
// Supported by Aerospike 3+ servers only.
type TypedMapValue[K ValidMapKey, V any] map[K]V

// NewMapValue generates a TypedMapValue instance.
func NewTypedMapValue[K ValidMapKey, V any](vmap map[K]V) TypedMapValue[K, V] {
	return TypedMapValue[K, V](vmap)
}

// EstimateSize returns the size of the TypedMapValue in wire protocol.
func (vl TypedMapValue[K, V]) EstimateSize() (int, Error) {
	return packIfcMapT(nil, vl)
}

func (vl TypedMapValue[K, V]) write(cmd BufferEx) (int, Error) {
	return packIfcMapT(cmd, vl)
}

func (vl TypedMapValue[K, V]) pack(cmd BufferEx) (int, Error) {
	return packIfcMapT(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl TypedMapValue[K, V]) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an any.
func (vl TypedMapValue[K, V]) GetObject() any {
	return map[K]V(vl)
}

func (vl TypedMapValue[K, V]) String() string {
	return fmt.Sprintf("%v", map[K]V(vl))
}

///////////////////////////////////////////////////////////////////////////////

// TypedListValue is a typed list. Unlike [ListValue], the element type is known
// at compile time, so packing avoids boxing the elements into interfaces:
// the popular element types route to the same monomorphic packers the
// [ListIter] implementations use, and the rest go through a per-entry typed
// path that still does not allocate.
// Supported by Aerospike 3+ servers only.
type TypedListValue[T any] []T

// NewTypedListValue generates a TypedListValue instance.
func NewTypedListValue[T any](list []T) TypedListValue[T] {
	return TypedListValue[T](list)
}

// EstimateSize returns the size of the TypedListValue in wire protocol.
func (vl TypedListValue[T]) EstimateSize() (int, Error) {
	return packIfcListT(nil, vl)
}

func (vl TypedListValue[T]) write(cmd BufferEx) (int, Error) {
	return packIfcListT(cmd, vl)
}

func (vl TypedListValue[T]) pack(cmd BufferEx) (int, Error) {
	return packIfcListT(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl TypedListValue[T]) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an any.
func (vl TypedListValue[T]) GetObject() any {
	return []T(vl)
}

// String implements Stringer interface.
func (vl TypedListValue[T]) String() string {
	return fmt.Sprintf("%v", []T(vl))
}

///////////////////////////////////////////////////////////////////////////////

// SeqMapValue packs a typed key/value sequence (iter.Seq2) as a map without
// materializing it. Because the wire protocol needs the entry count before
// the entries, the constructor takes it explicitly; packing fails with a
// PARAMETER_ERROR if the sequence yields a different number of pairs.
//
// The sequence is iterated once per serialization pass (size estimation and
// the write itself), so it must be re-iterable and yield the same entries
// each time. A SeqMapValue keeps iteration scratch state on itself so that
// packing does not allocate; as a consequence a single instance must not be
// used by concurrent commands.
// Supported by Aerospike 3+ servers only.
type SeqMapValue[K ValidMapKey, V any] struct {
	seq     iter.Seq2[K, V]
	entries int

	// Scratch for the prebuilt yield closure. Building the closure once at
	// construction is what keeps pack allocation-free: a closure built
	// inside pack would escape into the opaque seq call on every pack.
	cmd   BufferEx
	size  int
	count int
	err   Error
	yield func(K, V) bool
}

// NewSeqMapValue generates a SeqMapValue instance from a sequence that
// yields exactly entries pairs.
func NewSeqMapValue[K ValidMapKey, V any](seq iter.Seq2[K, V], entries int) *SeqMapValue[K, V] {
	v := &SeqMapValue[K, V]{seq: seq, entries: entries}
	v.yield = func(k K, val V) bool {
		if v.count >= v.entries {
			v.count++
			return false
		}
		v.count++
		n, err := packTypedObject(v.cmd, k, true)
		v.size += n
		if err != nil {
			v.err = err
			return false
		}
		n, err = packTypedObject(v.cmd, val, false)
		v.size += n
		if err != nil {
			v.err = err
			return false
		}
		return true
	}
	return v
}

func (v *SeqMapValue[K, V]) countError() Error {
	return newError(types.PARAMETER_ERROR, fmt.Sprintf(
		"SeqMapValue declared %d entries but the sequence yielded %s",
		v.entries, yieldedCount(v.count, v.entries)))
}

// EstimateSize returns the size of the SeqMapValue in wire protocol.
func (v *SeqMapValue[K, V]) EstimateSize() (int, Error) {
	return v.pack(nil)
}

func (v *SeqMapValue[K, V]) write(cmd BufferEx) (int, Error) {
	return v.pack(cmd)
}

func (v *SeqMapValue[K, V]) pack(cmd BufferEx) (int, Error) {
	if v.entries < 0 {
		return 0, newError(types.PARAMETER_ERROR, "SeqMapValue entry count must not be negative")
	}
	if isCanonicalPack(cmd) && v.entries > 1 {
		return v.packCanonical(cmd)
	}

	size, err := packMapBegin(cmd, v.entries)
	if err != nil {
		return size, err
	}

	v.cmd, v.size, v.count, v.err = cmd, 0, 0, nil
	v.seq(v.yield)
	size += v.size
	v.cmd = nil
	if v.err != nil {
		return 0, v.err
	}
	if v.count != v.entries {
		return 0, v.countError()
	}
	return size, nil
}

// packCanonical materializes and sorts the entries in the server's canonical
// msgpack key order (see packIfcMap). Filter-expression literals are built
// once per expression, so the allocations here are off the hot path.
func (v *SeqMapValue[K, V]) packCanonical(cmd BufferEx) (int, Error) {
	keys := make([]K, 0, v.entries)
	vals := make([]V, 0, v.entries)
	for k, val := range v.seq {
		if len(keys) > v.entries {
			break
		}
		keys = append(keys, k)
		vals = append(vals, val)
	}
	if len(keys) != v.entries {
		v.count = len(keys)
		return 0, v.countError()
	}

	idx := make([]int, len(keys))
	for i := range idx {
		idx[i] = i
	}
	slices.SortFunc(idx, func(a, b int) int { return compareCanonicalKeys(keys[a], keys[b]) })

	size, err := packMapBegin(cmd, v.entries)
	if err != nil {
		return size, err
	}
	for _, i := range idx {
		n, err := packTypedObject(cmd, keys[i], true)
		size += n
		if err != nil {
			return 0, err
		}
		n, err = packTypedObject(cmd, vals[i], false)
		size += n
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

// GetType returns wire protocol value type.
func (v *SeqMapValue[K, V]) GetType() int {
	return ParticleType.MAP
}

// GetObject returns the original sequence as an any.
func (v *SeqMapValue[K, V]) GetObject() any {
	return v.seq
}

// String implements Stringer interface.
func (v *SeqMapValue[K, V]) String() string {
	m := make(map[K]V, v.entries)
	for k, val := range v.seq {
		m[k] = val
	}
	return fmt.Sprintf("%v", m)
}

///////////////////////////////////////////////////////////////////////////////

// SeqListValue packs a typed element sequence (iter.Seq) as a list without
// materializing it. Because the wire protocol needs the element count before
// the elements, the constructor takes it explicitly; packing fails with a
// PARAMETER_ERROR if the sequence yields a different number of elements.
//
// The sequence is iterated once per serialization pass (size estimation and
// the write itself), so it must be re-iterable and yield the same elements
// each time. A SeqListValue keeps iteration scratch state on itself so that
// packing does not allocate; as a consequence a single instance must not be
// used by concurrent commands.
// Supported by Aerospike 3+ servers only.
type SeqListValue[T any] struct {
	seq      iter.Seq[T]
	elements int

	// Scratch for the prebuilt yield closure; see SeqMapValue.
	cmd   BufferEx
	size  int
	count int
	err   Error
	yield func(T) bool
}

// NewSeqListValue generates a SeqListValue instance from a sequence that
// yields exactly elements values.
func NewSeqListValue[T any](seq iter.Seq[T], elements int) *SeqListValue[T] {
	v := &SeqListValue[T]{seq: seq, elements: elements}
	v.yield = func(elem T) bool {
		if v.count >= v.elements {
			v.count++
			return false
		}
		v.count++
		n, err := packTypedObject(v.cmd, elem, false)
		v.size += n
		if err != nil {
			v.err = err
			return false
		}
		return true
	}
	return v
}

// EstimateSize returns the size of the SeqListValue in wire protocol.
func (v *SeqListValue[T]) EstimateSize() (int, Error) {
	return v.pack(nil)
}

func (v *SeqListValue[T]) write(cmd BufferEx) (int, Error) {
	return v.pack(cmd)
}

func (v *SeqListValue[T]) pack(cmd BufferEx) (int, Error) {
	if v.elements < 0 {
		return 0, newError(types.PARAMETER_ERROR, "SeqListValue element count must not be negative")
	}

	size, err := packArrayBegin(cmd, v.elements)
	if err != nil {
		return size, err
	}

	v.cmd, v.size, v.count, v.err = cmd, 0, 0, nil
	v.seq(v.yield)
	size += v.size
	v.cmd = nil
	if v.err != nil {
		return 0, v.err
	}
	if v.count != v.elements {
		return 0, newError(types.PARAMETER_ERROR, fmt.Sprintf(
			"SeqListValue declared %d elements but the sequence yielded %s",
			v.elements, yieldedCount(v.count, v.elements)))
	}
	return size, nil
}

// GetType returns wire protocol value type.
func (v *SeqListValue[T]) GetType() int {
	return ParticleType.LIST
}

// GetObject returns the original sequence as an any.
func (v *SeqListValue[T]) GetObject() any {
	return v.seq
}

// String implements Stringer interface.
func (v *SeqListValue[T]) String() string {
	l := make([]T, 0, v.elements)
	for elem := range v.seq {
		l = append(l, elem)
	}
	return fmt.Sprintf("%v", l)
}

// yieldedCount phrases a sequence-length mismatch: iteration stops at the
// first excess element, so past the declared count only "more" is known.
func yieldedCount(count, declared int) string {
	if count > declared {
		return "more"
	}
	return strconv.Itoa(count)
}

///////////////////////////////////////////////////////////////////////////////

// JsonValue encapsulates a Json map.
// Supported by Aerospike 3+ servers only.
type JsonValue map[string]any

// NewJsonValue generates a JsonValue instance.
func NewJsonValue(vmap map[string]any) JsonValue {
	return JsonValue(vmap)
}

// EstimateSize returns the size of the JsonValue in wire protocol.
func (vl JsonValue) EstimateSize() (int, Error) {
	return packJsonMap(nil, vl)
}

func (vl JsonValue) write(cmd BufferEx) (int, Error) {
	return packJsonMap(cmd, vl)
}

func (vl JsonValue) pack(cmd BufferEx) (int, Error) {
	return packJsonMap(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl JsonValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an any.
func (vl JsonValue) GetObject() any {
	return map[string]any(vl)
}

func (vl JsonValue) String() string {
	return fmt.Sprintf("%v", map[string]any(vl))
}

///////////////////////////////////////////////////////////////////////////////

// MapperValue encapsulates an arbitrary map which implements a MapIter interface.
// Supported by Aerospike 3+ servers only.
type MapperValue struct {
	vmap MapIter
}

// NewMapperValue generates a MapperValue instance.
func NewMapperValue(vmap MapIter) *MapperValue {
	res := &MapperValue{
		vmap: vmap,
	}

	return res
}

// EstimateSize returns the size of the MapperValue in wire protocol.
func (vl *MapperValue) EstimateSize() (int, Error) {
	return packMap(nil, vl.vmap)
}

func (vl *MapperValue) write(cmd BufferEx) (int, Error) {
	return packMap(cmd, vl.vmap)
}

func (vl *MapperValue) pack(cmd BufferEx) (int, Error) {
	return packMap(cmd, vl.vmap)
}

// GetType returns wire protocol value type.
func (vl *MapperValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an any.
func (vl *MapperValue) GetObject() any {
	return vl.vmap
}

func (vl *MapperValue) String() string {
	return fmt.Sprintf("%v", vl.vmap)
}

///////////////////////////////////////////////////////////////////////////////

// GeoJSONValue encapsulates a 2D Geo point.
// Supported by Aerospike 3.6.1 servers and later only.
type GeoJSONValue string

// NewGeoJSONValue generates a GeoJSONValue instance.
func NewGeoJSONValue(value string) GeoJSONValue {
	res := GeoJSONValue(value)
	return res
}

// EstimateSize returns the size of the GeoJSONValue in wire protocol.
func (vl GeoJSONValue) EstimateSize() (int, Error) {
	// flags + ncells + jsonstr
	return 1 + 2 + len(string(vl)), nil
}

func (vl GeoJSONValue) write(cmd BufferEx) (int, Error) {
	cmd.WriteByte(0) // flags
	cmd.WriteByte(0) // flags
	cmd.WriteByte(0) // flags

	return cmd.WriteString(string(vl))
}

func (vl GeoJSONValue) pack(cmd BufferEx) (int, Error) {
	return packGeoJson(cmd, string(vl))
}

// GetType returns wire protocol value type.
func (vl GeoJSONValue) GetType() int {
	return ParticleType.GEOJSON
}

// GetObject returns original value as an any.
func (vl GeoJSONValue) GetObject() any {
	return string(vl)
}

// String implements Stringer interface.
func (vl GeoJSONValue) String() string {
	return string(vl)
}

///////////////////////////////////////////////////////////////////////////////

// HLLValue encapsulates a HyperLogLog value.
type HLLValue []byte

// NewHLLValue generates a ByteValue instance.
func NewHLLValue(bytes []byte) HLLValue {
	return HLLValue(bytes)
}

// EstimateSize returns the size of the HLLValue in wire protocol.
func (vl HLLValue) EstimateSize() (int, Error) {
	return len(vl), nil
}

func (vl HLLValue) write(cmd BufferEx) (int, Error) {
	return cmd.Write(vl)
}

func (vl HLLValue) pack(cmd BufferEx) (int, Error) {
	return packHLL(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl HLLValue) GetType() int {
	return ParticleType.HLL
}

// GetObject returns original value as an any.
func (vl HLLValue) GetObject() any {
	return []byte(vl)
}

// String implements Stringer interface.
func (vl HLLValue) String() string {
	return fmt.Sprintf("% 02x", []byte(vl))
}

///////////////////////////////////////////////////////////////////////////////

// RawBlobValue encapsulates a CDT BLOB value.
// Notice: Do not use this value, it is for internal aerospike use only.
type RawBlobValue struct {
	// ParticleType signifies the data
	ParticleType int
	// Data carries the data
	Data []byte
}

// NewRawBlobValue generates a RawBlobValue instance for a CDT List or map using a particle type.
func NewRawBlobValue(pt int, b []byte) *RawBlobValue {
	data := make([]byte, len(b))
	copy(data, b)
	return &RawBlobValue{ParticleType: pt, Data: data}
}

// EstimateSize returns the size of the RawBlobValue in wire protocol.
func (vl *RawBlobValue) EstimateSize() (int, Error) {
	return len(vl.Data), nil
}

func (vl *RawBlobValue) write(cmd BufferEx) (int, Error) {
	return cmd.Write(vl.Data)
}

func (vl *RawBlobValue) pack(cmd BufferEx) (int, Error) {
	panic(unreachable)
}

// GetType returns wire protocol value type.
func (vl *RawBlobValue) GetType() int {
	return vl.ParticleType
}

// GetObject returns original value as an any.
func (vl *RawBlobValue) GetObject() any {
	return []byte(vl.Data)
}

// String implements Stringer interface.
func (vl *RawBlobValue) String() string {
	return fmt.Sprintf("% 02x", vl.Data)
}

//////////////////////////////////////////////////////////////////////////////

func bytesToParticleRaw(ptype int, buf []byte, offset int, length int, raw bool) (any, Error) {
	switch ptype {
	case ParticleType.MAP:
		if raw {
			return NewRawBlobValue(ptype, buf[offset:offset+length]), nil
		}
		return newUnpacker(buf, offset, length).UnpackMap()

	case ParticleType.LIST:
		if raw {
			return NewRawBlobValue(ptype, buf[offset:offset+length]), nil
		}
		return newUnpacker(buf, offset, length).UnpackList()
	}
	return bytesToParticle(ptype, buf, offset, length)
}

func bytesToParticle(ptype int, buf []byte, offset int, length int) (any, Error) {

	switch ptype {
	case ParticleType.INTEGER:
		// return `int` for 64bit platforms for compatibility reasons
		if Buffer.Arch64Bits {
			return int(Buffer.VarBytesToInt64(buf, offset, length)), nil
		}
		return Buffer.VarBytesToInt64(buf, offset, length), nil

	case ParticleType.STRING:
		return string(buf[offset : offset+length]), nil

	case ParticleType.FLOAT:
		return Buffer.BytesToFloat64(buf, offset), nil

	case ParticleType.BOOL:
		return Buffer.BytesToBool(buf, offset, length), nil

	case ParticleType.MAP:
		return newUnpacker(buf, offset, length).UnpackMap()

	case ParticleType.LIST:
		return newUnpacker(buf, offset, length).UnpackList()

	case ParticleType.GEOJSON:
		ncells := int(Buffer.BytesToInt16(buf, offset+1))
		headerSize := 1 + 2 + (ncells * 8)
		return GeoJSONValue(string(buf[offset+headerSize : offset+length])), nil

	case ParticleType.HLL:
		newObj := make([]byte, length)
		copy(newObj, buf[offset:offset+length])
		return HLLValue(newObj), nil

	case ParticleType.BLOB:
		newObj := make([]byte, length)
		copy(newObj, buf[offset:offset+length])
		return newObj, nil

	case ParticleType.LDT:
		return newUnpacker(buf, offset, length).unpackObjects()

	case ParticleType.PHP_BLOB:
		if length == 4 {
			if bytes.Equal(buf[offset:offset+length], []byte{0x62, 0x3A, 0x31, 0x3B}) {
				return true, nil
			} else if bytes.Equal(buf[offset:offset+length], []byte{0x62, 0x3A, 0x30, 0x3B}) {
				return false, nil
			}
		} else if length == 2 {
			if bytes.Equal(buf[offset:offset+length], []byte{0x4E, 0x3B}) {
				return nil, nil
			}
		}
		// generic PHP_BLOB
		newObj := make([]byte, length)
		copy(newObj, buf[offset:offset+length])
		return newObj, nil
	}
	return nil, nil
}

func bytesToKeyValue(pType int, buf []byte, offset int, length int) (Value, Error) {

	switch pType {
	case ParticleType.STRING:
		return NewStringValue(string(buf[offset : offset+length])), nil

	case ParticleType.INTEGER:
		return NewLongValue(Buffer.VarBytesToInt64(buf, offset, length)), nil

	case ParticleType.FLOAT:
		return NewFloatValue(Buffer.BytesToFloat64(buf, offset)), nil

	case ParticleType.BLOB:
		bytes := make([]byte, length)
		copy(bytes, buf[offset:offset+length])
		return NewBytesValue(bytes), nil

	case ParticleType.LIST:
		v, err := newUnpacker(buf, offset, length).UnpackList()
		if err != nil {
			return nil, err
		}
		return ListValue(v), nil

	case ParticleType.NULL:
		return NewNullValue(), nil

	default:
		return nil, newError(types.PARSE_ERROR, fmt.Sprintf("ParticleType %d not recognized. Please file a github issue.", pType))
	}
}

func unwrapValue(v any) any {
	if v == nil {
		return nil
	}

	if uv, ok := v.(Value); ok {
		return unwrapValue(uv.GetObject())
	} else if uv, ok := v.([]Value); ok {
		a := make([]any, len(uv))
		for i := range uv {
			a[i] = unwrapValue(uv[i].GetObject())
		}
		return a
	}

	return v
}
