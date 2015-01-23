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
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	// . "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

// Value interface is used to efficiently serialize objects into the wire protocol.
type Value interface {

	// Calculate number of vl.bytes necessary to serialize the value in the wire protocol.
	estimateSize() int

	// Serialize the value in the wire protocol.
	write(buffer []byte, offset int) (int, error)

	// Serialize the value using MessagePack.
	pack(packer *packer) error

	// GetType returns wire protocol value type.
	GetType() int

	// GetObject returns original value as an interface{}.
	GetObject() interface{}

	// String implements Stringer interface.
	String() string

	// Return value as an Object.
	// GetLuaValue() LuaValue

	reader() io.Reader
}

// AerospikeBlob defines an interface to automatically
// encode an object to a []byte.
type AerospikeBlob interface {
	// EncodeBlob returns a byte slice representing the encoding of the
	// receiver for transmission to a Decoder, usually of the same
	// concrete type.
	EncodeBlob() ([]byte, error)
}

var sizeOfInt uintptr
var sizeOfInt32 = uintptr(4)
var sizeOfInt64 = uintptr(8)

func init() {
	var j, i int = 0, ^0

	for i != 0 {
		j++
		i >>= 1
	}

	sizeOfInt = uintptr(j)
}

// NewValue generates a new Value object based on the type.
// If the type is not supported, NewValue will panic.
func NewValue(v interface{}) Value {
	switch val := v.(type) {
	case nil:
		return &NullValue{}
	case int:
		return NewIntegerValue(int(val))
	case string:
		return NewStringValue(val)
	case []Value:
		return NewValueArray(val)
	case []byte:
		return NewBytesValue(val)
	case int8:
		return NewIntegerValue(int(val))
	case int16:
		return NewIntegerValue(int(val))
	case int32:
		return NewIntegerValue(int(val))
	case uint8: // byte supported here
		return NewIntegerValue(int(val))
	case uint16:
		return NewIntegerValue(int(val))
	case uint32:
		return NewLongValue(int64(val))
	case uint:
		if !Buffer.Arch64Bits || (val <= math.MaxInt64) {
			return NewLongValue(int64(val))
		}
	case int64:
		return NewLongValue(int64(val))
	case []interface{}:
		return NewListValue(val)
	case map[interface{}]interface{}:
		return NewMapValue(val)
	case Value:
		return val
	case AerospikeBlob:
		return NewBlobValue(val)
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

// NullValue is an empty value.
type NullValue struct{}

// NewNullValue generates a NullValue instance.
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

// GetType returns wire protocol value type.
func (vl *NullValue) GetType() int {
	return ParticleType.NULL
}

// GetObject returns original value as an interface{}.
func (vl *NullValue) GetObject() interface{} {
	return nil
}

// func (vl *NullValue) GetLuaValue() LuaValue {
// 	return LuaNil.NIL
// }

func (vl *NullValue) reader() io.Reader {
	return nil
}

// String implements Stringer interface.
func (vl *NullValue) String() string {
	return ""
}

///////////////////////////////////////////////////////////////////////////////

// BytesValue encapsulates an array of bytes.
type BytesValue struct {
	bytes []byte
}

// NewBytesValue generates a ByteValue instance.
func NewBytesValue(bytes []byte) *BytesValue {
	return &BytesValue{bytes: bytes}
}

// NewBlobValue accepts an AerospikeBlob interface, and automatically
// converts it to a BytesValue.
// If Encode returns an err, it will panic.
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

// GetType returns wire protocol value type.
func (vl *BytesValue) GetType() int {
	return ParticleType.BLOB
}

// GetObject returns original value as an interface{}.
func (vl *BytesValue) GetObject() interface{} {
	return vl.bytes
}

// func (vl *BytesValue) GetLuaValue() LuaValue {
// 	return LuaString.valueOf(vl.bytes)
// }

func (vl *BytesValue) reader() io.Reader {
	return bytes.NewReader(vl.bytes)
}

// String implements Stringer interface.
func (vl *BytesValue) String() string {
	return Buffer.BytesToHexString(vl.bytes)
}

///////////////////////////////////////////////////////////////////////////////

// StringValue encapsulates a string value.
type StringValue struct {
	value string
}

// NewStringValue generates a StringValue instance.
func NewStringValue(value string) *StringValue {
	return &StringValue{value: value}
}

func (vl *StringValue) estimateSize() int {
	return len(vl.value)
}

func (vl *StringValue) write(buffer []byte, offset int) (int, error) {
	return copy(buffer[offset:], vl.value), nil
}

func (vl *StringValue) pack(packer *packer) error {
	packer.PackString(vl.value)
	return nil
}

// GetType returns wire protocol value type.
func (vl *StringValue) GetType() int {
	return ParticleType.STRING
}

// GetObject returns original value as an interface{}.
func (vl *StringValue) GetObject() interface{} {
	return vl.value
}

// func (vl *StringValue) GetLuaValue() LuaValue {
// 	return LuaString.valueOf(vl.value)
// }

func (vl *StringValue) reader() io.Reader {
	return strings.NewReader(vl.value)
}

// String implements Stringer interface.
func (vl *StringValue) String() string {
	return vl.value
}

///////////////////////////////////////////////////////////////////////////////

// IntegerValue encapsulates an integer value.
type IntegerValue struct {
	value int
}

// NewIntegerValue generates an IntegerValue instance.
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

// GetType returns wire protocol value type.
func (vl *IntegerValue) GetType() int {
	return ParticleType.INTEGER
}

// GetObject returns original value as an interface{}.
func (vl *IntegerValue) GetObject() interface{} {
	return int(vl.value)
}

// func (vl *IntegerValue) GetLuaValue() LuaValue {
// 	return LuaInteger.valueOf(vl.value)
// }

func (vl *IntegerValue) reader() io.Reader {
	return bytes.NewReader(Buffer.Int64ToBytes(int64(vl.value), nil, 0))
}

// String implements Stringer interface.
func (vl *IntegerValue) String() string {
	return strconv.Itoa(int(vl.value))
}

///////////////////////////////////////////////////////////////////////////////

// LongValue encapsulates an int64 value.
type LongValue struct {
	value int64
}

// NewLongValue generates a LongValue instance.
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

// GetType returns wire protocol value type.
func (vl *LongValue) GetType() int {
	return ParticleType.INTEGER
}

// GetObject returns original value as an interface{}.
func (vl *LongValue) GetObject() interface{} {
	return vl.value
}

// func (vl *LongValue) GetLuaValue() LuaValue {
// 	return LuaInteger.valueOf(vl.value)
// }

func (vl *LongValue) reader() io.Reader {
	return bytes.NewReader(Buffer.Int64ToBytes(int64(vl.value), nil, 0))
}

// String implements Stringer interface.
func (vl *LongValue) String() string {
	return strconv.Itoa(int(vl.value))
}

///////////////////////////////////////////////////////////////////////////////

// ValueArray encapsulates an array of Value.
// Supported by Aerospike 3 servers only.
type ValueArray struct {
	array []Value
	bytes []byte
}

// ToValueArray converts a []interface{} to []Value.
// It will panic if any of array element types are not supported.
func ToValueArray(array []interface{}) *ValueArray {
	res := make([]Value, 0, len(array))
	for i := range array {
		res = append(res, NewValue(array[i]))
	}
	return NewValueArray(res)
}

// NewValueArray generates a ValueArray instance.
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
	return packer.packValueArray(vl.array)
}

// GetType returns wire protocol value type.
func (vl *ValueArray) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an interface{}.
func (vl *ValueArray) GetObject() interface{} {
	return vl.array
}

// func (vl *ValueArray) GetLuaValue() LuaValue {
// 	return nil
// }

func (vl *ValueArray) reader() io.Reader {
	return bytes.NewReader(vl.bytes)
}

// String implements Stringer interface.
func (vl *ValueArray) String() string {
	return fmt.Sprintf("%v", vl.array)
}

///////////////////////////////////////////////////////////////////////////////

// ListValue encapsulates any arbitray array.
// Supported by Aerospike 3 servers only.
type ListValue struct {
	list  []interface{}
	bytes []byte
}

// NewListValue generates a ListValue instance.
func NewListValue(list []interface{}) *ListValue {
	res := &ListValue{
		list: list,
	}

	res.bytes, _ = packAnyArray(list)

	return res
}

func (vl *ListValue) estimateSize() int {
	return len(vl.bytes)
}

func (vl *ListValue) write(buffer []byte, offset int) (int, error) {
	l := copy(buffer[offset:], vl.bytes)
	return l, nil
}

func (vl *ListValue) pack(packer *packer) error {
	// return packer.PackList(vl.list)
	_, err := packer.buffer.Write(vl.bytes)
	return err
}

// GetType returns wire protocol value type.
func (vl *ListValue) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an interface{}.
func (vl *ListValue) GetObject() interface{} {
	return vl.list
}

// func (vl *ListValue) GetLuaValue() LuaValue {
// 	return nil
// }

func (vl *ListValue) reader() io.Reader {
	return bytes.NewReader(vl.bytes)
}

// String implements Stringer interface.
func (vl *ListValue) String() string {
	return fmt.Sprintf("%v", vl.list)
}

///////////////////////////////////////////////////////////////////////////////

// MapValue encapsulates an arbitray map.
// Supported by Aerospike 3 servers only.
type MapValue struct {
	vmap  map[interface{}]interface{}
	bytes []byte
}

// NewMapValue generates a MapValue instance.
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
	// return packer.PackMap(vl.vmap)
	_, err := packer.buffer.Write(vl.bytes)
	return err
}

// GetType returns wire protocol value type.
func (vl *MapValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an interface{}.
func (vl *MapValue) GetObject() interface{} {
	return vl.vmap
}

// func (vl *MapValue) GetLuaValue() LuaValue {
// 	return nil
// }

func (vl *MapValue) reader() io.Reader {
	return bytes.NewReader(vl.bytes)
}

// String implements Stringer interface.
func (vl *MapValue) String() string {
	return fmt.Sprintf("%v", vl.vmap)
}

//////////////////////////////////////////////////////////////////////////////

func bytesToParticle(ptype int, buf []byte, offset int, length int) (interface{}, error) {

	switch ptype {
	case ParticleType.INTEGER:
		return Buffer.BytesToNumber(buf, offset, length), nil

	case ParticleType.STRING:
		return string(buf[offset : offset+length]), nil

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

func bytesToKeyValue(pType int, buf []byte, offset int, len int) (Value, error) {

	switch pType {
	case ParticleType.STRING:
		return NewStringValue(string(buf[offset : offset+len])), nil

	case ParticleType.INTEGER:
		return NewLongValue(Buffer.VarBytesToInt64(buf, offset, len)), nil

	case ParticleType.BLOB:
		bytes := make([]byte, len, len)
		copy(bytes, buf[offset:offset+len])
		return NewBytesValue(bytes), nil

	default:
		return nil, nil
	}
}
