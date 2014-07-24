// Copyright 2013-2014 Aerospike, Inc.
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
	"fmt"
	"math"
	"reflect"
	"strconv"
	"unsafe"

	// . "github.com/citrusleaf/aerospike-client-go/logger"
	. "github.com/citrusleaf/aerospike-client-go/types"
	ParticleType "github.com/citrusleaf/aerospike-client-go/types/particle_type"
	Buffer "github.com/citrusleaf/aerospike-client-go/utils/buffer"
)

// Polymorphic value classes used to efficiently serialize objects into the wire protocol.
type Value interface {

	// Calculate number of this.bytes necessary to serialize the value in the wire protocol.
	EstimateSize() int

	// Serialize the value in the wire protocol.
	Write(buffer []byte, offset int) (int, error)

	// Serialize the value using MessagePack.
	Pack(packer *Packer) error

	// Get wire protocol value type.
	GetType() int

	// Return original value as an Object.
	GetObject() interface{}

	// Implement Stringer interface
	String() string

	// Return value as an Object.
	// GetLuaValue() LuaValue
}

type AerospikeBlob interface {
	// Encode returns a byte slice representing the encoding of the
	// receiver for transmission to a Decoder, usually of the same
	// concrete type.
	EncodeBlob() ([]byte, error)
}

var sizeOfInt = unsafe.Sizeof(int(0))
var sizeOfInt32 = unsafe.Sizeof(int32(0))

func NewValue(v interface{}) Value {
	switch v.(type) {
	case nil:
		return &NullValue{}
	case int:
		return NewIntegerValue(int(v.(int)))
	case string:
		return NewStringValue(v.(string))
	case []Value:
		return NewValueArray(v.([]Value))
	case []byte:
		return NewBytesValue(v.([]byte))
	case int8:
		return NewIntegerValue(int(v.(int8)))
	case int16:
		return NewIntegerValue(int(v.(int16)))
	case int32:
		return NewIntegerValue(int(v.(int32)))
	case uint8: // byte supported here
		return NewIntegerValue(int(v.(uint8)))
	case uint16:
		return NewIntegerValue(int(v.(uint16)))
	case uint32:
		return NewLongValue(int64(v.(uint32)))
	case uint:
		t := v.(uint)
		if !Buffer.Arch64Bits || (t <= math.MaxInt64) {
			return NewLongValue(int64(t))
		}
	case int64:
		return NewLongValue(int64(v.(int64)))
	case AerospikeBlob:
		return NewBlobValue(v.(AerospikeBlob))
	}

	// check for array and map
	switch reflect.TypeOf(v).Kind() {
	case reflect.Array, reflect.Slice:
		s := reflect.ValueOf(v)
		l := s.Len()
		arr := make([]interface{}, l)
		for i := 0; i < l; i++ {
			arr[i] = s.Index(i).Interface()
		}

		return NewListValue(arr)
	case reflect.Map:
		s := reflect.ValueOf(v)
		l := s.Len()
		amap := make(map[interface{}]interface{}, l)
		for _, i := range s.MapKeys() {
			amap[i.Interface()] = s.MapIndex(i).Interface()
		}

		return NewMapValue(amap)
	}

	// panic for anything that is not supported.
	panic(TypeNotSupportedErr())

}

// Empty value.

type NullValue struct{}

func (this *NullValue) EstimateSize() int {
	return 0
}

func (this *NullValue) Write(buffer []byte, offset int) (int, error) {
	return 0, nil
}

func (this *NullValue) Pack(packer *Packer) error {
	packer.PackNil()
	return nil
}

func (this *NullValue) GetType() int {
	return ParticleType.NULL
}

func (this *NullValue) GetObject() interface{} {
	return nil
}

// func (this *NullValue) GetLuaValue() LuaValue {
// 	return LuaNil.NIL
// }

func (this *NullValue) String() string {
	return ""
}

///////////////////////////////////////////////////////////////////////////////

// Byte array value.
type BytesValue struct {
	bytes []byte
}

func NewBytesValue(bytes []byte) *BytesValue {
	return &BytesValue{bytes: bytes}
}

// NewBlobValue accepts an AerospikeBlob interface, and automatically
// converts it to a BytesValue. If Encode returns an err, it will panic.
func NewBlobValue(object AerospikeBlob) *BytesValue {
	buf, err := object.EncodeBlob()
	if err != nil {
		panic(err)
	}

	return NewBytesValue(buf)
}

func (this *BytesValue) EstimateSize() int {
	return len(this.bytes)
}

func (this *BytesValue) Write(buffer []byte, offset int) (int, error) {
	len := copy(buffer[offset:], this.bytes)
	return len, nil
}

func (this *BytesValue) Pack(packer *Packer) error {
	packer.PackBytes(this.bytes)
	return nil
}

func (this *BytesValue) GetType() int {
	return ParticleType.BLOB
}

func (this *BytesValue) GetObject() interface{} {
	return this.bytes
}

// func (this *BytesValue) GetLuaValue() LuaValue {
// 	return LuaString.valueOf(this.bytes)
// }

func (this *BytesValue) String() string {
	return Buffer.BytesToHexString(this.bytes)
}

///////////////////////////////////////////////////////////////////////////////

// value string.
type StringValue struct {
	value string
}

func NewStringValue(value string) *StringValue {
	return &StringValue{value: value}
}

func (this *StringValue) EstimateSize() int {
	return len(this.value)
}

func (this *StringValue) Write(buffer []byte, offset int) (int, error) {
	return copy(buffer[offset:], []byte(this.value)), nil
}

func (this *StringValue) Pack(packer *Packer) error {
	packer.PackString(this.value)
	return nil
}

func (this *StringValue) GetType() int {
	return ParticleType.STRING
}

func (this *StringValue) GetObject() interface{} {
	return this.value
}

// func (this *StringValue) GetLuaValue() LuaValue {
// 	return LuaString.valueOf(this.value)
// }

func (this *StringValue) String() string {
	return this.value
}

///////////////////////////////////////////////////////////////////////////////

// Integer value.
type IntegerValue struct {
	value int
}

func NewIntegerValue(value int) *IntegerValue {
	return &IntegerValue{value: value}
}

func (this *IntegerValue) EstimateSize() int {
	return int(sizeOfInt)
}

func (this *IntegerValue) Write(buffer []byte, offset int) (int, error) {
	if sizeOfInt == sizeOfInt32 {
		Buffer.Int32ToBytes(int32(this.value), buffer, offset)
	} else {
		Buffer.Int64ToBytes(int64(this.value), buffer, offset)
	}
	return int(sizeOfInt), nil
}

func (this *IntegerValue) Pack(packer *Packer) error {
	packer.PackAInt(this.value)
	return nil
}

func (this *IntegerValue) GetType() int {
	return ParticleType.INTEGER
}

func (this *IntegerValue) GetObject() interface{} {
	return int(this.value)
}

// func (this *IntegerValue) GetLuaValue() LuaValue {
// 	return LuaInteger.valueOf(this.value)
// }

func (this *IntegerValue) String() string {
	return strconv.Itoa(int(this.value))
}

///////////////////////////////////////////////////////////////////////////////

// Long value.
type LongValue struct {
	value int64
}

func NewLongValue(value int64) *LongValue {
	return &LongValue{value: value}
}

func (this *LongValue) EstimateSize() int {
	return 8
}

func (this *LongValue) Write(buffer []byte, offset int) (int, error) {
	Buffer.Int64ToBytes(this.value, buffer, offset)
	return 8, nil
}

func (this *LongValue) Pack(packer *Packer) error {
	packer.PackALong(this.value)
	return nil
}

func (this *LongValue) GetType() int {
	return ParticleType.INTEGER
}

func (this *LongValue) GetObject() interface{} {
	return this.value
}

// func (this *LongValue) GetLuaValue() LuaValue {
// 	return LuaInteger.valueOf(this.value)
// }

func (this *LongValue) String() string {
	return strconv.Itoa(int(this.value))
}

///////////////////////////////////////////////////////////////////////////////

// Value array.
// Supported by Aerospike 3 servers only.
type ValueArray struct {
	array []Value
	bytes []byte
}

func NewValueArray(array []Value) *ValueArray {
	return &ValueArray{
		array: array,
	}
}

func (this *ValueArray) EstimateSize() int {
	this.bytes, _ = PackValueArray(this.array)
	return len(this.bytes)
}

func (this *ValueArray) Write(buffer []byte, offset int) (int, error) {
	copy(buffer[offset:], this.bytes)
	return len(this.bytes), nil
}

func (this *ValueArray) Pack(packer *Packer) error {
	packer.PackValueArray(this.array)
	return nil
}

func (this *ValueArray) GetType() int {
	return ParticleType.LIST
}

func (this *ValueArray) GetObject() interface{} {
	return this.array
}

// func (this *ValueArray) GetLuaValue() LuaValue {
// 	return nil
// }

func (this *ValueArray) String() string {
	return "" //return Arrays.toString(this.array)
}

///////////////////////////////////////////////////////////////////////////////

// List value.
// Supported by Aerospike 3 servers only.
type ListValue struct {
	list  []interface{}
	bytes []byte
}

func NewListValue(list []interface{}) *ListValue {
	return &ListValue{
		list: list,
	}
}

func (this *ListValue) EstimateSize() int {
	// var err error
	this.bytes, _ = PackAnyArray(this.list)
	return len(this.bytes)
}

func (this *ListValue) Write(buffer []byte, offset int) (int, error) {
	l := copy(buffer[offset:], this.bytes)
	return l, nil
}

func (this *ListValue) Pack(packer *Packer) error {
	return packer.PackList(this.list)
}

func (this *ListValue) GetType() int {
	return ParticleType.LIST
}

func (this *ListValue) GetObject() interface{} {
	return this.list
}

// func (this *ListValue) GetLuaValue() LuaValue {
// 	return nil
// }

func (this *ListValue) String() string {
	return fmt.Sprintf("%v", this.list)
}

///////////////////////////////////////////////////////////////////////////////

// Map value.
// Supported by Aerospike 3 servers only.
type MapValue struct {
	vmap  map[interface{}]interface{}
	bytes []byte
}

func NewMapValue(vmap map[interface{}]interface{}) *MapValue {
	return &MapValue{
		vmap: vmap,
	}
}

func (this *MapValue) EstimateSize() int {
	this.bytes, _ = PackAnyMap(this.vmap)
	return len(this.bytes)
}

func (this *MapValue) Write(buffer []byte, offset int) (int, error) {
	return copy(buffer[offset:], this.bytes), nil
}

func (this *MapValue) Pack(packer *Packer) error {
	return packer.PackMap(this.vmap)
}

func (this *MapValue) GetType() int {
	return ParticleType.MAP
}

func (this *MapValue) GetObject() interface{} {
	return this.vmap
}

// func (this *MapValue) GetLuaValue() LuaValue {
// 	return nil
// }

func (this *MapValue) String() string {
	return fmt.Sprintf("%v", this.vmap)
}

//////////////////////////////////////////////////////////////////////////////

func BytesToParticle(ptype int, buf []byte, offset int, length int) (interface{}, error) {

	switch ptype {
	case ParticleType.STRING:
		return string(buf[offset : offset+length]), nil

	case ParticleType.INTEGER:
		return Buffer.BytesToNumber(buf, offset, length), nil

	case ParticleType.BLOB:
		newObj := make([]byte, length)
		copy(newObj, buf[offset:offset+length])
		return newObj, nil

	case ParticleType.LIST:
		return NewUnpacker(buf, offset, length).UnpackList()

	case ParticleType.MAP:
		return NewUnpacker(buf, offset, length).UnpackMap()

	}
	return nil, nil
}
