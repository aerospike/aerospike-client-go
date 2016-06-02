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
	"bytes"
	"fmt"
	"math"
	"reflect"
	"time"

	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type packer struct {
	buffer *bytes.Buffer
	offset int
}

func packValueArray(val []Value) ([]byte, error) {
	packer := newPacker()
	if err := packer.packValueArray(val); err != nil {
		return nil, err
	}
	return packer.buffer.Bytes(), nil
}

func packAnyArray(val []interface{}) ([]byte, error) {
	packer := newPacker()
	if err := packer.PackList(val); err != nil {
		return nil, err
	}
	return packer.buffer.Bytes(), nil
}

func packAnyMap(val map[interface{}]interface{}) ([]byte, error) {
	packer := newPacker()
	if err := packer.PackMap(val); err != nil {
		return nil, err
	}
	return packer.buffer.Bytes(), nil
}

///////////////////////////////////////////////////////////////////////////////

func newPacker() *packer {
	p := &packer{
		buffer: bytes.NewBuffer(make([]byte, 0, 256)),
	}

	return p
}

func (pckr *packer) packValueArray(values []Value) error {
	if err := pckr.PackArrayBegin(len(values)); err != nil {
		return err
	}
	for i := range values {
		if err := values[i].pack(pckr); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackList(list []interface{}) error {
	if err := pckr.PackArrayBegin(len(list)); err != nil {
		return err
	}
	for i := range list {
		if err := pckr.PackObject(list[i]); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackArrayBegin(size int) error {
	if size < 16 {
		return pckr.PackAByte(0x90 | byte(size))
	} else if size <= math.MaxUint16 {
		return pckr.PackShort(0xdc, int16(size))
	} else {
		return pckr.PackInt(0xdd, int32(size))
	}
}

func (pckr *packer) PackMap(theMap map[interface{}]interface{}) error {
	if err := pckr.PackMapBegin(len(theMap)); err != nil {
		return err
	}
	for k, v := range theMap {
		if k != nil {
			t := reflect.TypeOf(k)
			if t.Kind() == reflect.Map || t.Kind() == reflect.Slice ||
				(t.Kind() == reflect.Array && t.Elem().Kind() != reflect.Uint8) {
				panic("Maps, Slices, and bounded arrays other than Bounded Byte Arrays are not supported as Map keys.")
			}
		}

		if err := pckr.PackObject(k); err != nil {
			return err
		}
		if err := pckr.PackObject(v); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackMapBegin(size int) error {
	if size < 16 {
		return pckr.PackAByte(0x80 | byte(size))
	} else if size <= math.MaxUint16 {
		return pckr.PackShort(0xde, int16(size))
	} else {
		return pckr.PackInt(0xdf, int32(size))
	}
}

func (pckr *packer) PackBytes(b []byte) error {
	if err := pckr.PackByteArrayBegin(len(b) + 1); err != nil {
		return err
	}
	if err := pckr.PackAByte(ParticleType.BLOB); err != nil {
		return err
	}
	if err := pckr.PackByteArray(b, 0, len(b)); err != nil {
		return err
	}
	return nil
}

func (pckr *packer) PackByteArrayBegin(length int) error {
	if length < 32 {
		return pckr.PackAByte(0xa0 | byte(length))
	} else if length < 65536 {
		return pckr.PackShort(0xda, int16(length))
	} else {
		return pckr.PackInt(0xdb, int32(length))
	}
}

func (pckr *packer) PackObject(obj interface{}) error {
	switch v := obj.(type) {
	case Value:
		return v.pack(pckr)
	case string:
		return pckr.PackString(v)
	case []byte:
		return pckr.PackBytes(obj.([]byte))
	case int8:
		return pckr.PackAInt(int(v))
	case uint8:
		return pckr.PackAInt(int(v))
	case int16:
		return pckr.PackAInt(int(v))
	case uint16:
		return pckr.PackAInt(int(v))
	case int32:
		return pckr.PackAInt(int(v))
	case uint32:
		return pckr.PackAInt(int(v))
	case int:
		if Buffer.Arch32Bits {
			return pckr.PackAInt(v)
		}
		return pckr.PackALong(int64(v))
	case uint:
		if Buffer.Arch32Bits {
			return pckr.PackAInt(int(v))
		}
		return pckr.PackAULong(uint64(v))
	case int64:
		return pckr.PackALong(v)
	case uint64:
		return pckr.PackAULong(v)
	case time.Time:
		return pckr.PackALong(v.UnixNano())
	case nil:
		return pckr.PackNil()
	case bool:
		return pckr.PackBool(v)
	case float32:
		return pckr.PackFloat32(v)
	case float64:
		return pckr.PackFloat64(v)
	case struct{}:
		return pckr.PackMap(map[interface{}]interface{}{})
	case []interface{}:
		return pckr.PackList(obj.([]interface{}))
	case map[interface{}]interface{}:
		return pckr.PackMap(obj.(map[interface{}]interface{}))
	}

	// check for array and map
	rv := reflect.ValueOf(obj)
	switch reflect.TypeOf(obj).Kind() {
	case reflect.Array, reflect.Slice:
		// pack bounded array of bytes differently
		if reflect.TypeOf(obj).Kind() == reflect.Array && reflect.TypeOf(obj).Elem().Kind() == reflect.Uint8 {
			l := rv.Len()
			arr := make([]byte, l)
			for i := 0; i < l; i++ {
				arr[i] = rv.Index(i).Interface().(uint8)
			}
			return pckr.PackBytes(arr)
		}

		l := rv.Len()
		arr := make([]interface{}, l)
		for i := 0; i < l; i++ {
			arr[i] = rv.Index(i).Interface()
		}
		return pckr.PackList(arr)
	case reflect.Map:
		l := rv.Len()
		amap := make(map[interface{}]interface{}, l)
		for _, i := range rv.MapKeys() {
			amap[i.Interface()] = rv.MapIndex(i).Interface()
		}
		return pckr.PackMap(amap)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return pckr.PackObject(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return pckr.PackObject(rv.Uint())
	case reflect.Bool:
		return pckr.PackObject(rv.Bool())
	case reflect.String:
		return pckr.PackObject(rv.String())
	case reflect.Float32, reflect.Float64:
		return pckr.PackObject(rv.Float())
	}

	panic(fmt.Sprintf("Type `%v` not supported to pack.", reflect.TypeOf(obj)))
}

func (pckr *packer) PackAULong(val uint64) error {
	return pckr.PackULong(val)
}

func (pckr *packer) PackALong(val int64) error {
	if val >= 0 {
		if val < 128 {
			return pckr.PackAByte(byte(val))
		}

		if val <= math.MaxUint8 {
			return pckr.PackByte(0xcc, byte(val))
		}

		if val <= math.MaxUint16 {
			return pckr.PackShort(0xcd, int16(val))
		}

		if val <= math.MaxUint32 {
			return pckr.PackInt(0xce, int32(val))
		}
		return pckr.PackLong(0xd3, val)
	} else {
		if val >= -32 {
			return pckr.PackAByte(0xe0 | (byte(val) + 32))
		}

		if val >= math.MinInt8 {
			return pckr.PackByte(0xd0, byte(val))
		}

		if val >= math.MinInt16 {
			return pckr.PackShort(0xd1, int16(val))
		}

		if val >= math.MinInt32 {
			return pckr.PackInt(0xd2, int32(val))
		}
		return pckr.PackLong(0xd3, val)
	}
}

func (pckr *packer) PackAInt(val int) error {
	if val >= 0 {
		if val < 128 {
			return pckr.PackAByte(byte(val))
		}

		if val < 256 {
			return pckr.PackByte(0xcc, byte(val))
		}

		if val < 65536 {
			return pckr.PackShort(0xcd, int16(val))
		}
		return pckr.PackInt(0xce, int32(val))
	} else {
		if val >= -32 {
			return pckr.PackAByte(0xe0 | (byte(val) + 32))
		}

		if val >= math.MinInt8 {
			return pckr.PackByte(0xd0, byte(val))
		}

		if val >= math.MinInt16 {
			return pckr.PackShort(0xd1, int16(val))
		}
		return pckr.PackInt(0xd2, int32(val))
	}
}

var _b8 = []byte{0, 0, 0, 0, 0, 0, 0, 0}
var _b4 = []byte{0, 0, 0, 0}
var _b2 = []byte{0, 0}

func (pckr *packer) grow(b []byte) (int, error) {
	pos := pckr.buffer.Len()
	_, err := pckr.buffer.Write(b)
	return pos, err
}

func (pckr *packer) PackString(val string) error {
	size := len(val) + 1
	if err := pckr.PackByteArrayBegin(size); err != nil {
		return err
	}
	if err := pckr.buffer.WriteByte(byte(ParticleType.STRING)); err != nil {
		return err
	}
	if _, err := pckr.buffer.WriteString(val); err != nil {
		return err
	}
	return nil
}

func (pckr *packer) PackGeoJson(val string) error {
	size := len(val) + 1
	if err := pckr.PackByteArrayBegin(size); err != nil {
		return err
	}
	if err := pckr.buffer.WriteByte(byte(ParticleType.GEOJSON)); err != nil {
		return err
	}
	if _, err := pckr.buffer.WriteString(val); err != nil {
		return err
	}
	return nil
}

func (pckr *packer) PackByteArray(src []byte, srcOffset int, srcLength int) error {
	_, err := pckr.buffer.Write(src[srcOffset : srcOffset+srcLength])
	return err
}

func (pckr *packer) PackLong(valType int, val int64) error {
	if err := pckr.buffer.WriteByte(byte(valType)); err != nil {
		return err
	}
	pos, err := pckr.grow(_b8)
	if err != nil {
		return err
	}

	Buffer.Int64ToBytes(val, pckr.buffer.Bytes(), pos)
	return nil
}

func (pckr *packer) PackULong(val uint64) error {
	if err := pckr.buffer.WriteByte(byte(0xcf)); err != nil {
		return err
	}

	pos, err := pckr.grow(_b8)
	if err != nil {
		return err
	}
	Buffer.Int64ToBytes(int64(val), pckr.buffer.Bytes(), pos)
	return nil
}

func (pckr *packer) PackInt(valType int, val int32) error {
	if err := pckr.buffer.WriteByte(byte(valType)); err != nil {
		return err
	}
	pos, err := pckr.grow(_b4)
	if err != nil {
		return err
	}
	Buffer.Int32ToBytes(val, pckr.buffer.Bytes(), pos)
	return nil
}

func (pckr *packer) PackShort(valType int, val int16) error {
	if err := pckr.buffer.WriteByte(byte(valType)); err != nil {
		return err
	}
	pos, err := pckr.grow(_b2)
	if err != nil {
		return err
	}
	Buffer.Int16ToBytes(val, pckr.buffer.Bytes(), pos)
	return nil
}

// This method is not compatible with MsgPack specs and is only used by aerospike client<->server
// for wire transfer only
func (pckr *packer) PackShortRaw(val int16) error {
	pos, err := pckr.grow(_b2)
	if err != nil {
		return err
	}
	Buffer.Int16ToBytes(val, pckr.buffer.Bytes(), pos)
	return nil
}

func (pckr *packer) PackByte(valType int, val byte) error {
	if err := pckr.buffer.WriteByte(byte(valType)); err != nil {
		return err
	}
	if err := pckr.buffer.WriteByte(val); err != nil {
		return err
	}
	return nil
}

func (pckr *packer) PackNil() error {
	return pckr.buffer.WriteByte(0xc0)
}

func (pckr *packer) PackBool(val bool) error {
	if val {
		return pckr.buffer.WriteByte(0xc3)
	} else {
		return pckr.buffer.WriteByte(0xc2)
	}
}

func (pckr *packer) PackFloat32(val float32) error {
	if err := pckr.buffer.WriteByte(0xca); err != nil {
		return err
	}
	pos, err := pckr.grow(_b4)
	if err != nil {
		return err
	}
	Buffer.Float32ToBytes(val, pckr.buffer.Bytes(), pos)
	return nil
}

func (pckr *packer) PackFloat64(val float64) error {
	if err := pckr.buffer.WriteByte(0xcb); err != nil {
		return err
	}
	pos, err := pckr.grow(_b8)
	if err != nil {
		return err
	}
	Buffer.Float64ToBytes(val, pckr.buffer.Bytes(), pos)
	return nil
}

func (pckr *packer) PackAByte(val byte) error {
	return pckr.buffer.WriteByte(val)
}
