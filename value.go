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

	// . "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

// Polymorphic value classes used to efficiently serialize objects into the wire protocol.
type Value interface {

	// Calculate number of vl.bytes necessary to serialize the value in the wire protocol.
	estimateSize() int

	// Serialize the value in the wire protocol.
	write(buffer []byte, offset int) (int, error)

	// Serialize the value using MessagePack.
	pack(packer *packer) error

	// Get wire protocol value type.
	GetType() int

	// Return original value as an Object.
	GetObject() interface{}

	// Implement Stringer interface
	String() string

	// Return value as an Object.
	// GetLuaValue() LuaValue

	// Returns Bytes
	getBytes() []byte
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
	case Value:
		return v.(Value)
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
	panic(NewAerospikeError(TYPE_NOT_SUPPORTED, "Value type '"+reflect.TypeOf(v).Name()+"' not supported"))
}

// Empty value.

type NullValue struct{}

func NewNullValue() *NullValue {
	return &NullValue{}
}

func (vl *NullValue) estimateSize() int {
	return 0
}

func (vl *NullValue) write(buffer []byte, offset int) (int, error) {
	return 0, nil
}

func (vl *NullValue) pack(packer *packer) error {
	packer.PackNil()
	return nil
}

func (vl *NullValue) GetType() int {
	return ParticleType.NULL
}

func (vl *NullValue) GetObject() interface{} {
	return nil
}

// func (vl *NullValue) GetLuaValue() LuaValue {
// 	return LuaNil.NIL
// }

func (vl *NullValue) getBytes() []byte {
	return nil
}

func (vl *NullValue) String() string {
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

func (vl *BytesValue) estimateSize() int {
	return len(vl.bytes)
}

func (vl *BytesValue) write(buffer []byte, offset int) (int, error) {
	len := copy(buffer[offset:], vl.bytes)
	return len, nil
}

func (vl *BytesValue) pack(packer *packer) error {
	packer.PackBytes(vl.bytes)
	return nil
}

func (vl *BytesValue) GetType() int {
	return ParticleType.BLOB
}

func (vl *BytesValue) GetObject() interface{} {
	return vl.bytes
}

// func (vl *BytesValue) GetLuaValue() LuaValue {
// 	return LuaString.valueOf(vl.bytes)
// }

func (vl *BytesValue) getBytes() []byte {
	return vl.bytes
}

func (vl *BytesValue) String() string {
	return Buffer.BytesToHexString(vl.bytes)
}

///////////////////////////////////////////////////////////////////////////////

// value string.
type StringValue struct {
	value string
}

func NewStringValue(value string) *StringValue {
	return &StringValue{value: value}
}

func (vl *StringValue) estimateSize() int {
	return len(vl.value)
}

func (vl *StringValue) write(buffer []byte, offset int) (int, error) {
	return copy(buffer[offset:], []byte(vl.value)), nil
}

func (vl *StringValue) pack(packer *packer) error {
	packer.PackString(vl.value)
	return nil
}

func (vl *StringValue) GetType() int {
	return ParticleType.STRING
}

func (vl *StringValue) GetObject() interface{} {
	return vl.value
}

// func (vl *StringValue) GetLuaValue() LuaValue {
// 	return LuaString.valueOf(vl.value)
// }

func (vl *StringValue) getBytes() []byte {
	return []byte(vl.value)
}

func (vl *StringValue) String() string {
	return vl.value
}

///////////////////////////////////////////////////////////////////////////////

// Integer value.
type IntegerValue struct {
	value int
}

func NewIntegerValue(value int) *IntegerValue {
	return &IntegerValue{value: value}
}

func (vl *IntegerValue) estimateSize() int {
	return 8
}

func (vl *IntegerValue) write(buffer []byte, offset int) (int, error) {
	Buffer.Int64ToBytes(int64(vl.value), buffer, offset)
	return 8, nil
}

func (vl *IntegerValue) pack(packer *packer) error {
	if sizeOfInt == sizeOfInt32 {
		packer.PackAInt(vl.value)
	} else {
		packer.PackALong(int64(vl.value))
	}
	return nil
}

func (vl *IntegerValue) GetType() int {
	return ParticleType.INTEGER
}

func (vl *IntegerValue) GetObject() interface{} {
	return int(vl.value)
}

// func (vl *IntegerValue) GetLuaValue() LuaValue {
// 	return LuaInteger.valueOf(vl.value)
// }

func (vl *IntegerValue) getBytes() []byte {
	return Buffer.Int64ToBytes(int64(vl.value), nil, 0)
}

func (vl *IntegerValue) String() string {
	return strconv.Itoa(int(vl.value))
}

///////////////////////////////////////////////////////////////////////////////

// Long value.
type LongValue struct {
	value int64
}

func NewLongValue(value int64) *LongValue {
	return &LongValue{value: value}
}

func (vl *LongValue) estimateSize() int {
	return 8
}

func (vl *LongValue) write(buffer []byte, offset int) (int, error) {
	Buffer.Int64ToBytes(vl.value, buffer, offset)
	return 8, nil
}

func (vl *LongValue) pack(packer *packer) error {
	packer.PackALong(vl.value)
	return nil
}

func (vl *LongValue) GetType() int {
	return ParticleType.INTEGER
}

func (vl *LongValue) GetObject() interface{} {
	return vl.value
}

// func (vl *LongValue) GetLuaValue() LuaValue {
// 	return LuaInteger.valueOf(vl.value)
// }

func (vl *LongValue) getBytes() []byte {
	return Buffer.Int64ToBytes(int64(vl.value), nil, 0)
}

func (vl *LongValue) String() string {
	return strconv.Itoa(int(vl.value))
}

///////////////////////////////////////////////////////////////////////////////

// Value array.
// Supported by Aerospike 3 servers only.
type ValueArray struct {
	array []Value
	bytes []byte
}

func ToValueArray(array []interface{}) *ValueArray {
	res := make([]Value, 0, len(array))
	for i := range array {
		res = append(res, NewValue(array[i]))
	}
	return NewValueArray(res)
}

func NewValueArray(array []Value) *ValueArray {
	res := &ValueArray{
		array: array,
	}

	res.bytes, _ = packValueArray(array)

	return res
}

func (vl *ValueArray) estimateSize() int {
	return len(vl.bytes)
}

func (vl *ValueArray) write(buffer []byte, offset int) (int, error) {
	res := copy(buffer[offset:], vl.bytes)
	return res, nil
}

func (vl *ValueArray) pack(packer *packer) error {
	packer.packValueArray(vl.array)
	return nil
}

func (vl *ValueArray) GetType() int {
	return ParticleType.LIST
}

func (vl *ValueArray) GetObject() interface{} {
	return vl.array
}

// func (vl *ValueArray) GetLuaValue() LuaValue {
// 	return nil
// }

func (vl *ValueArray) getBytes() []byte {
	return vl.bytes
}

func (vl *ValueArray) String() string {
	return fmt.Sprintf("%v", vl.array)
}

///////////////////////////////////////////////////////////////////////////////

// List value.
// Supported by Aerospike 3 servers only.
type ListValue struct {
	list  []interface{}
	bytes []byte
}

func NewListValue(list []interface{}) *ListValue {
	res := &ListValue{
		list: list,
	}

	res.bytes, _ = packAnyArray(list)

	return res
}

func (vl *ListValue) estimateSize() int {
	// var err error
	vl.bytes, _ = packAnyArray(vl.list)
	return len(vl.bytes)
}

func (vl *ListValue) write(buffer []byte, offset int) (int, error) {
	l := copy(buffer[offset:], vl.bytes)
	return l, nil
}

func (vl *ListValue) pack(packer *packer) error {
	return packer.PackList(vl.list)
}

func (vl *ListValue) GetType() int {
	return ParticleType.LIST
}

func (vl *ListValue) GetObject() interface{} {
	return vl.list
}

// func (vl *ListValue) GetLuaValue() LuaValue {
// 	return nil
// }

func (vl *ListValue) getBytes() []byte {
	return vl.bytes
}

func (vl *ListValue) String() string {
	return fmt.Sprintf("%v", vl.list)
}

///////////////////////////////////////////////////////////////////////////////

// Map value.
// Supported by Aerospike 3 servers only.
type MapValue struct {
	vmap  map[interface{}]interface{}
	bytes []byte
}

func NewMapValue(vmap map[interface{}]interface{}) *MapValue {
	res := &MapValue{
		vmap: vmap,
	}

	res.bytes, _ = packAnyMap(vmap)

	return res
}

func (vl *MapValue) estimateSize() int {
	return len(vl.bytes)
}

func (vl *MapValue) write(buffer []byte, offset int) (int, error) {
	return copy(buffer[offset:], vl.bytes), nil
}

func (vl *MapValue) pack(packer *packer) error {
	return packer.PackMap(vl.vmap)
}

func (vl *MapValue) GetType() int {
	return ParticleType.MAP
}

func (vl *MapValue) GetObject() interface{} {
	return vl.vmap
}

// func (vl *MapValue) GetLuaValue() LuaValue {
// 	return nil
// }

func (vl *MapValue) getBytes() []byte {
	return vl.bytes
}

func (vl *MapValue) String() string {
	return fmt.Sprintf("%v", vl.vmap)
}

//////////////////////////////////////////////////////////////////////////////

func bytesToParticle(ptype int, buf []byte, offset int, length int) (interface{}, error) {

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
		return newUnpacker(buf, offset, length).UnpackList()

	case ParticleType.MAP:
		return newUnpacker(buf, offset, length).UnpackMap()

	}
	return nil, nil
}
