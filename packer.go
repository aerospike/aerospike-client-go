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
	"math"
	"reflect"

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
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

func packAnyArray(val []interface{}) ([]byte, error) {
	packer := newPacker()
	if err := packer.PackList(val); err != nil {
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

func packAnyMap(val map[interface{}]interface{}) ([]byte, error) {
	packer := newPacker()
	if err := packer.PackMap(val); err != nil {
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

///////////////////////////////////////////////////////////////////////////////

func newPacker() *packer {
	p := &packer{
		buffer: bytes.NewBuffer(nil),
	}

	return p
}

func (pckr *packer) packValueArray(values []Value) error {
	pckr.PackArrayBegin(len(values))
	for _, value := range values {
		if err := value.pack(pckr); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackList(list []interface{}) error {
	pckr.PackArrayBegin(len(list))
	for _, obj := range list {
		if err := pckr.PackObject(obj); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackArrayBegin(size int) {
	if size < 16 {
		pckr.PackAByte(0x90 | byte(size))
	} else if size <= math.MaxUint16 {
		pckr.PackShort(0xdc, int16(size))
	} else {
		pckr.PackInt(0xdd, int32(size))
	}
}

func (pckr *packer) PackMap(theMap map[interface{}]interface{}) error {
	pckr.PackMapBegin(len(theMap))
	for k, v := range theMap {
		if err := pckr.PackObject(k); err != nil {
			return err
		}
		if err := pckr.PackObject(v); err != nil {
			return err
		}
	}
	return nil
}

func (pckr *packer) PackMapBegin(size int) {
	if size < 16 {
		pckr.PackAByte(0x80 | byte(size))
	} else if size <= math.MaxUint16 {
		pckr.PackShort(0xde, int16(size))
	} else {
		pckr.PackInt(0xdf, int32(size))
	}
}

func (pckr *packer) PackBytes(b []byte) {
	pckr.PackByteArrayBegin(len(b) + 1)
	pckr.PackAByte(ParticleType.BLOB)
	pckr.PackByteArray(b, 0, len(b))
}

func (pckr *packer) PackByteArrayBegin(length int) {
	if length < 32 {
		pckr.PackAByte(0xa0 | byte(length))
	} else if length < 65536 {
		pckr.PackShort(0xda, int16(length))
	} else {
		pckr.PackInt(0xdb, int32(length))
	}
}

func (pckr *packer) PackObject(obj interface{}) error {
	switch obj.(type) {
	case Value:
		return obj.(Value).pack(pckr)
	case string:
		pckr.PackString(obj.(string))
		return nil
	case []byte:
		pckr.PackBytes(obj.([]byte))
		return nil
	case int8:
		pckr.PackAInt(int(obj.(int8)))
		return nil
	case uint8:
		pckr.PackAInt(int(obj.(uint8)))
		return nil
	case int16:
		pckr.PackAInt(int(obj.(int16)))
		return nil
	case uint16:
		pckr.PackAInt(int(obj.(uint16)))
		return nil
	case int32:
		pckr.PackAInt(int(obj.(int32)))
		return nil
	case uint32:
		pckr.PackAInt(int(obj.(uint32)))
		return nil
	case int:
		if Buffer.Arch32Bits {
			pckr.PackAInt(obj.(int))
			return nil
		}
		pckr.PackALong(int64(obj.(int)))
		return nil
	case uint:
		if Buffer.Arch32Bits {
			pckr.PackAInt(int(obj.(uint)))
			return nil
		}
		pckr.PackAULong(obj.(uint64))
	case int64:
		pckr.PackALong(obj.(int64))
		return nil
	case uint64:
		pckr.PackAULong(obj.(uint64))
		return nil
	case nil:
		pckr.PackNil()
		return nil
	}

	// check for array and map
	switch reflect.TypeOf(obj).Kind() {
	case reflect.Array, reflect.Slice:
		s := reflect.ValueOf(obj)
		l := s.Len()
		arr := make([]interface{}, l)
		for i := 0; i < l; i++ {
			arr[i] = s.Index(i).Interface()
		}
		return pckr.PackList(arr)
	case reflect.Map:
		return pckr.PackMap(obj.(map[interface{}]interface{}))
	}

	panic(fmt.Sprintf("Type `%v` not supported to pack.", reflect.TypeOf(obj)))
}

func (pckr *packer) PackAULong(val uint64) {
	pckr.PackULong(val)
}

func (pckr *packer) PackALong(val int64) {
	if val >= 0 {
		if val < 128 {
			pckr.PackAByte(byte(val))
			return
		}

		if val <= math.MaxUint8 {
			pckr.PackByte(0xcc, byte(val))
			return
		}

		if val <= math.MaxUint16 {
			pckr.PackShort(0xcd, int16(val))
			return
		}

		if val <= math.MaxUint32 {
			pckr.PackInt(0xce, int32(val))
			return
		}
		pckr.PackLong(0xd3, val)
	} else {
		if val >= -32 {
			pckr.PackAByte(0xe0 | byte(val) + 32)
			return
		}

		if val >= math.MinInt8 {
			pckr.PackByte(0xd0, byte(val))
			return
		}

		if val >= math.MinInt16 {
			pckr.PackShort(0xd1, int16(val))
			return
		}

		if val >= math.MinInt32 {
			pckr.PackInt(0xd2, int32(val))
			return
		}
		pckr.PackLong(0xd3, int64(val))
	}
}

func (pckr *packer) PackAInt(val int) {
	if val >= 0 {
		if val < 128 {
			pckr.PackAByte(byte(val))
			return
		}

		if val < 256 {
			pckr.PackByte(0xcc, byte(val))
			return
		}

		if val < 65536 {
			pckr.PackShort(0xcd, int16(val))
			return
		}
		pckr.PackInt(0xce, int32(val))
	} else {
		if val >= -32 {
			pckr.PackAByte(0xe0 | byte(val+32))
			return
		}

		if val >= math.MinInt8 {
			pckr.PackByte(0xd0, byte(val))
			return
		}

		if val >= math.MinInt16 {
			pckr.PackShort(0xd1, int16(val))
			return
		}
		pckr.PackInt(0xd2, int32(val))
	}
}

func (pckr *packer) PackString(val string) {
	size := len(val) + 1
	pckr.PackByteArrayBegin(size)
	pckr.buffer.WriteByte(byte(ParticleType.STRING))
	pckr.buffer.Write([]byte(val))
}

func (pckr *packer) PackByteArray(src []byte, srcOffset int, srcLength int) {
	pckr.buffer.Write(src[srcOffset : srcOffset+srcLength])
}

func (pckr *packer) PackLong(valType int, val int64) {
	pckr.buffer.WriteByte(byte(valType))
	pckr.buffer.Write(Buffer.Int64ToBytes(val, nil, pckr.offset))
}

func (pckr *packer) PackULong(val uint64) {
	pckr.buffer.WriteByte(byte(0xcf))
	pckr.buffer.Write(Buffer.Int64ToBytes(int64(val), nil, pckr.offset))
}

func (pckr *packer) PackInt(valType int, val int32) {
	pckr.buffer.WriteByte(byte(valType))
	pckr.buffer.Write(Buffer.Int32ToBytes(val, nil, pckr.offset))
}

func (pckr *packer) PackShort(valType int, val int16) {
	pckr.buffer.WriteByte(byte(valType))
	pckr.buffer.Write(Buffer.Int16ToBytes(val, nil, pckr.offset))
}

func (pckr *packer) PackByte(valType int, val byte) {
	pckr.buffer.WriteByte(byte(valType))
	pckr.buffer.WriteByte(val)
}

func (pckr *packer) PackNil() {
	pckr.buffer.WriteByte(0xc0)
}

func (pckr *packer) PackAByte(val byte) {
	pckr.buffer.WriteByte(val)
}
