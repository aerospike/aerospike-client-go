// Copyright 2013-2016 Aerospike, Inc.
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

	. "github.com/aerospike/aerospike-client-go/types"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

// Map pair is used when the client returns sorted maps from the server
// Since the default map in Go is a hash map, we will use a slice
// to return the results in server order
type MapPair struct{ Key, Value interface{} }

// Value interface is used to efficiently serialize objects into the wire protocol.
type Value interface {

	// Calculate number of vl.bytes necessary to serialize the value in the wire protocol.
	estimateSize() (int, error)

	// Serialize the value in the wire protocol.
	write(cmd aerospikeBuffer) (int, error)

	// Serialize the value using MessagePack.
	pack(cmd aerospikeBuffer) (int, error)

	// GetType returns wire protocol value type.
	GetType() int

	// GetObject returns original value as an interface{}.
	GetObject() interface{}

	// String implements Stringer interface.
	String() string
}

type AerospikeBlob interface {
	// EncodeBlob returns a byte slice representing the encoding of the
	// receiver for transmission to a Decoder, usually of the same
	// concrete type.
	EncodeBlob() ([]byte, error)
}

// NewValue generates a new Value object based on the type.
// If the type is not supported, NewValue will panic.
func NewValue(v interface{}) Value {
	switch val := v.(type) {
	case Value:
		return val
	case int:
		return IntegerValue(val)
	case int64:
		return LongValue(val)
	case string:
		return StringValue(val)
	case []interface{}:
		return ListValue(val)
	case map[string]interface{}:
		return JsonValue(val)
	case map[interface{}]interface{}:
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
		if val <= math.MaxInt64 {
			return LongValue(int64(val))
		}
	case MapIter:
		return NewMapperValue(val)
	case ListIter:
		return NewListerValue(val)
	case AerospikeBlob:
		return NewBlobValue(val)
	}

	// check for array and map
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Array, reflect.Slice:
		l := rv.Len()
		arr := make([]interface{}, l)
		for i := 0; i < l; i++ {
			arr[i] = rv.Index(i).Interface()
		}

		return NewListValue(arr)
	case reflect.Map:
		l := rv.Len()
		amap := make(map[interface{}]interface{}, l)
		for _, i := range rv.MapKeys() {
			amap[i.Interface()] = rv.MapIndex(i).Interface()
		}

		return NewMapValue(amap)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return NewLongValue(reflect.ValueOf(v).Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return NewLongValue(int64(reflect.ValueOf(v).Uint()))
	case reflect.String:
		return NewStringValue(rv.String())
	}

	// panic for anything that is not supported.
	panic(NewAerospikeError(TYPE_NOT_SUPPORTED, "Value type '"+reflect.TypeOf(v).Name()+"' not supported"))
}

// NullValue is an empty value.
type NullValue struct{}

var nullValue NullValue

// NewNullValue generates a NullValue instance.
func NewNullValue() NullValue {
	return nullValue
}

func (vl NullValue) estimateSize() (int, error) {
	return 0, nil
}

func (vl NullValue) write(cmd aerospikeBuffer) (int, error) {
	return 0, nil
}

func (vl NullValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackNil(cmd)
}

// GetType returns wire protocol value type.
func (vl NullValue) GetType() int {
	return ParticleType.NULL
}

// GetObject returns original value as an interface{}.
func (vl NullValue) GetObject() interface{} {
	return nil
}

func (vl NullValue) String() string {
	return ""
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

func (vl BytesValue) estimateSize() (int, error) {
	return len(vl), nil
}

func (vl BytesValue) write(cmd aerospikeBuffer) (int, error) {
	return cmd.Write(vl)
}

func (vl BytesValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackBytes(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl BytesValue) GetType() int {
	return ParticleType.BLOB
}

// GetObject returns original value as an interface{}.
func (vl BytesValue) GetObject() interface{} {
	return []byte(vl)
}

// String implements Stringer interface.
func (vl BytesValue) String() string {
	return Buffer.BytesToHexString(vl)
}

///////////////////////////////////////////////////////////////////////////////

// StringValue encapsulates a string value.
type StringValue string

// NewStringValue generates a StringValue instance.
func NewStringValue(value string) StringValue {
	return StringValue(value)
}

func (vl StringValue) estimateSize() (int, error) {
	return len(vl), nil
}

func (vl StringValue) write(cmd aerospikeBuffer) (int, error) {
	return cmd.WriteString(string(vl))
}

func (vl StringValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackString(cmd, string(vl))
}

// GetType returns wire protocol value type.
func (vl StringValue) GetType() int {
	return ParticleType.STRING
}

// GetObject returns original value as an interface{}.
func (vl StringValue) GetObject() interface{} {
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

func (vl IntegerValue) estimateSize() (int, error) {
	return 8, nil
}

func (vl IntegerValue) write(cmd aerospikeBuffer) (int, error) {
	return cmd.WriteInt64(int64(vl))
}

func (vl IntegerValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackAInt64(cmd, int64(vl))
}

// GetType returns wire protocol value type.
func (vl IntegerValue) GetType() int {
	return ParticleType.INTEGER
}

// GetObject returns original value as an interface{}.
func (vl IntegerValue) GetObject() interface{} {
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

func (vl LongValue) estimateSize() (int, error) {
	return 8, nil
}

func (vl LongValue) write(cmd aerospikeBuffer) (int, error) {
	return cmd.WriteInt64(int64(vl))
}

func (vl LongValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackAInt64(cmd, int64(vl))
}

// GetType returns wire protocol value type.
func (vl LongValue) GetType() int {
	return ParticleType.INTEGER
}

// GetObject returns original value as an interface{}.
func (vl LongValue) GetObject() interface{} {
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

func (vl FloatValue) estimateSize() (int, error) {
	return 8, nil
}

func (vl FloatValue) write(cmd aerospikeBuffer) (int, error) {
	return cmd.WriteFloat64(float64(vl))
}

func (vl FloatValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackFloat64(cmd, float64(vl))
}

// GetType returns wire protocol value type.
func (vl FloatValue) GetType() int {
	return ParticleType.FLOAT
}

// GetObject returns original value as an interface{}.
func (vl FloatValue) GetObject() interface{} {
	return float64(vl)
}

// String implements Stringer interface.
func (vl FloatValue) String() string {
	return (fmt.Sprintf("%f", vl))
}

///////////////////////////////////////////////////////////////////////////////

// ValueArray encapsulates an array of Value.
// Supported by Aerospike 3 servers only.
type ValueArray struct {
	*ListerValue
}

// ToValueSlice converts a []interface{} to []Value.
// It will panic if any of array element types are not supported.
// TODO: Do something about this
func ToValueSlice(array []interface{}) []Value {
	res := make([]Value, 0, len(array))
	for i := range array {
		res = append(res, NewValue(array[i]))
	}
	return res
}

// ToValueArray converts a []interface{} to a ValueArray type.
// It will panic if any of array element types are not supported.
func ToValueArray(array []interface{}) *ValueArray {
	// return NewValueArray(ToValueSlice(array))
	res := ValueArray{NewListerValue(ifcValueList(array))}
	return &res

}

// NewValueArray generates a ValueArray instance.
func NewValueArray(array []Value) *ValueArray {
	res := ValueArray{NewListerValue(valueList(array))}
	return &res
}

///////////////////////////////////////////////////////////////////////////////

// ListValue encapsulates any arbitrary array.
// Supported by Aerospike 3 servers only.
type ListValue []interface{}

// NewListValue generates a ListValue instance.
func NewListValue(list []interface{}) ListValue {
	return ListValue(list)
}

func (vl ListValue) estimateSize() (int, error) {
	return __PackIfcList(nil, vl)
}

func (vl ListValue) write(cmd aerospikeBuffer) (int, error) {
	return __PackIfcList(cmd, vl)
}

func (vl ListValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackIfcList(cmd, []interface{}(vl))
}

// GetType returns wire protocol value type.
func (vl ListValue) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an interface{}.
func (vl ListValue) GetObject() interface{} {
	return vl
}

// String implements Stringer interface.
func (vl ListValue) String() string {
	return fmt.Sprintf("%v", []interface{}(vl))
}

///////////////////////////////////////////////////////////////////////////////

// ListValue encapsulates any arbitrary array.
// Supported by Aerospike 3 servers only.
type ListerValue struct {
	list ListIter
}

// NewListValue generates a ListValue instance.
func NewListerValue(list ListIter) *ListerValue {
	res := &ListerValue{
		list: list,
	}

	return res
}

func (vl *ListerValue) estimateSize() (int, error) {
	return __PackList(nil, vl.list)
}

func (vl *ListerValue) write(cmd aerospikeBuffer) (int, error) {
	return __PackList(cmd, vl.list)
}

func (vl *ListerValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackList(cmd, vl.list)
}

// GetType returns wire protocol value type.
func (vl *ListerValue) GetType() int {
	return ParticleType.LIST
}

// GetObject returns original value as an interface{}.
func (vl *ListerValue) GetObject() interface{} {
	return vl.list
}

// String implements Stringer interface.
func (vl *ListerValue) String() string {
	return fmt.Sprintf("%v", vl.list)
}

///////////////////////////////////////////////////////////////////////////////

// MapValue encapsulates an arbitrary map.
// Supported by Aerospike 3 servers only.
type MapValue map[interface{}]interface{}

// NewMapValue generates a MapValue instance.
func NewMapValue(vmap map[interface{}]interface{}) MapValue {
	return MapValue(vmap)
}

func (vl MapValue) estimateSize() (int, error) {
	return __PackIfcMap(nil, vl)
}

func (vl MapValue) write(cmd aerospikeBuffer) (int, error) {
	return __PackIfcMap(cmd, vl)
}

func (vl MapValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackIfcMap(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl MapValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an interface{}.
func (vl MapValue) GetObject() interface{} {
	return vl
}

func (vl MapValue) String() string {
	return fmt.Sprintf("%v", map[interface{}]interface{}(vl))
}

///////////////////////////////////////////////////////////////////////////////

// JsonValue encapsulates a Json map.
// Supported by Aerospike 3 servers only.
type JsonValue map[string]interface{}

// NewMapValue generates a JsonValue instance.
func NewJsonValue(vmap map[string]interface{}) JsonValue {
	return JsonValue(vmap)
}

func (vl JsonValue) estimateSize() (int, error) {
	return __PackJsonMap(nil, vl)
}

func (vl JsonValue) write(cmd aerospikeBuffer) (int, error) {
	return __PackJsonMap(cmd, vl)
}

func (vl JsonValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackJsonMap(cmd, vl)
}

// GetType returns wire protocol value type.
func (vl JsonValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an interface{}.
func (vl JsonValue) GetObject() interface{} {
	return vl
}

func (vl JsonValue) String() string {
	return fmt.Sprintf("%v", map[string]interface{}(vl))
}

///////////////////////////////////////////////////////////////////////////////

// MapperValue encapsulates an arbitrary map which implements a MapIter interface.
// Supported by Aerospike 3 servers only.
type MapperValue struct {
	vmap MapIter
}

// NewMapValue generates a MapperValue instance.
func NewMapperValue(vmap MapIter) *MapperValue {
	res := &MapperValue{
		vmap: vmap,
	}

	return res
}

func (vl *MapperValue) estimateSize() (int, error) {
	return __PackMap(nil, vl.vmap)
}

func (vl *MapperValue) write(cmd aerospikeBuffer) (int, error) {
	return __PackMap(cmd, vl.vmap)
}

func (vl *MapperValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackMap(cmd, vl.vmap)
}

// GetType returns wire protocol value type.
func (vl *MapperValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns original value as an interface{}.
func (vl *MapperValue) GetObject() interface{} {
	return vl.vmap
}

func (vl *MapperValue) String() string {
	return fmt.Sprintf("%v", vl.vmap)
}

///////////////////////////////////////////////////////////////////////////////

// GeoJSONValue encapsulates a 2D Geo point.
// Supported by Aerospike 3.6.1 servers only.
type GeoJSONValue string

// NewMapValue generates a GeoJSONValue instance.
func NewGeoJSONValue(value string) GeoJSONValue {
	res := GeoJSONValue(value)
	return res
}

func (vl GeoJSONValue) estimateSize() (int, error) {
	// flags + ncells + jsonstr
	return 1 + 2 + len(string(vl)), nil
}

func (vl GeoJSONValue) write(cmd aerospikeBuffer) (int, error) {
	cmd.WriteByte(0) // flags
	cmd.WriteByte(0) // flags
	cmd.WriteByte(0) // flags

	return cmd.WriteString(string(vl))
}

func (vl GeoJSONValue) pack(cmd aerospikeBuffer) (int, error) {
	return __PackGeoJson(cmd, string(vl))
}

// GetType returns wire protocol value type.
func (vl GeoJSONValue) GetType() int {
	return ParticleType.GEOJSON
}

// GetObject returns original value as an interface{}.
func (vl GeoJSONValue) GetObject() interface{} {
	return string(vl)
}

// String implements Stringer interface.
func (vl GeoJSONValue) String() string {
	return string(vl)
}

//////////////////////////////////////////////////////////////////////////////

func bytesToParticle(ptype int, buf []byte, offset int, length int) (interface{}, error) {

	switch ptype {
	case ParticleType.INTEGER:
		return int(Buffer.VarBytesToInt64(buf, offset, length)), nil

	case ParticleType.STRING:
		return string(buf[offset : offset+length]), nil

	case ParticleType.FLOAT:
		return Buffer.BytesToFloat64(buf, offset), nil

	case ParticleType.MAP:
		return newUnpacker(buf, offset, length).UnpackMap()

	case ParticleType.LIST:
		return newUnpacker(buf, offset, length).UnpackList()

	case ParticleType.GEOJSON:
		ncells := int(Buffer.BytesToInt16(buf, offset+1))
		headerSize := 1 + 2 + (ncells * 8)
		return string(buf[offset+headerSize : offset+length]), nil

	case ParticleType.BLOB:
		newObj := make([]byte, length)
		copy(newObj, buf[offset:offset+length])
		return newObj, nil

	case ParticleType.LDT:
		return newUnpacker(buf, offset, length).unpackObjects()

	}
	return nil, nil
}

func bytesToKeyValue(pType int, buf []byte, offset int, len int) (Value, error) {

	switch pType {
	case ParticleType.STRING:
		return NewStringValue(string(buf[offset : offset+len])), nil

	case ParticleType.INTEGER:
		return NewLongValue(Buffer.VarBytesToInt64(buf, offset, len)), nil

	case ParticleType.FLOAT:
		return NewFloatValue(Buffer.BytesToFloat64(buf, offset)), nil

	case ParticleType.BLOB:
		bytes := make([]byte, len, len)
		copy(bytes, buf[offset:offset+len])
		return NewBytesValue(bytes), nil

	default:
		return nil, NewAerospikeError(PARSE_ERROR, fmt.Sprintf("ParticleType %d not recognized. Please file a github issue.", pType))
	}
}

func unwrapValue(v interface{}) interface{} {
	if v == nil {
		return nil
	}

	if uv, ok := v.(Value); ok {
		return unwrapValue(uv.GetObject())
	} else if uv, ok := v.([]Value); ok {
		a := make([]interface{}, len(uv))
		for i := range uv {
			a[i] = unwrapValue(uv[i].GetObject())
		}
		return a
	}

	return v
}
