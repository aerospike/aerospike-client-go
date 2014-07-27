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
	"math"
	"reflect"

	// . "github.com/aerospike/aerospike-client-go/logger"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type Packer struct {
	buffer *bytes.Buffer
	offset int
}

func PackValueArray(val []Value) ([]byte, error) {
	packer := NewPacker()
	if err := packer.PackValueArray(val); err != nil {
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

func PackAnyArray(val []interface{}) ([]byte, error) {
	packer := NewPacker()
	if err := packer.PackList(val); err != nil {
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

func PackAnyMap(val map[interface{}]interface{}) ([]byte, error) {
	packer := NewPacker()
	if err := packer.PackMap(val); err != nil {
		return nil, nil
	}
	return packer.buffer.Bytes(), nil
}

///////////////////////////////////////////////////////////////////////////////

func NewPacker() *Packer {
	p := &Packer{
		buffer: bytes.NewBuffer(nil),
	}

	return p
}

func (this *Packer) PackValueArray(values []Value) error {
	this.PackArrayBegin(len(values))
	for _, value := range values {
		if err := value.Pack(this); err != nil {
			return err
		}
	}
	return nil
}

func (this *Packer) PackList(list []interface{}) error {
	this.PackArrayBegin(len(list))
	for _, obj := range list {
		if err := this.PackObject(obj); err != nil {
			return err
		}
	}
	// Logger.Error("Packing result: %s", Buffer.BytesToHexString(this.buffer.Bytes()))
	return nil
}

func (this *Packer) PackArrayBegin(size int) {
	if size < 16 {
		this.PackAByte(0x90 | byte(size))
	} else if size <= math.MaxUint16 {
		this.PackShort(0xdc, int16(size))
	} else {
		this.PackInt(0xdd, int32(size))
	}
}

func (this *Packer) PackMap(theMap map[interface{}]interface{}) error {
	this.PackMapBegin(len(theMap))
	for k, v := range theMap {
		if err := this.PackObject(k); err != nil {
			return err
		}
		if err := this.PackObject(v); err != nil {
			return err
		}
	}
	// Logger.Error("Packing result: %s", Buffer.BytesToHexString(this.buffer.Bytes()))

	return nil
}

func (this *Packer) PackMapBegin(size int) {
	if size < 16 {
		this.PackAByte(0x80 | byte(size))
	} else if size <= math.MaxUint16 {
		this.PackShort(0xde, int16(size))
	} else {
		this.PackInt(0xdf, int32(size))
	}
}

func (this *Packer) PackBytes(b []byte) {
	this.PackByteArrayBegin(len(b) + 1)
	this.PackAByte(ParticleType.BLOB)
	this.PackByteArray(b, 0, len(b))
}

func (this *Packer) PackByteArrayBegin(length int) {
	if length < 32 {
		this.PackAByte(0xa0 | byte(length))
	} else if length < 65536 {
		this.PackShort(0xda, int16(length))
	} else {
		this.PackInt(0xdb, int32(length))
	}
}

func (this *Packer) PackObject(obj interface{}) error {
	switch obj.(type) {
	case Value:
		return obj.(Value).Pack(this)
	case string:
		this.PackString(obj.(string))
		return nil
	case []byte:
		this.PackBytes(obj.([]byte))
		return nil
	case int8:
		this.PackAInt(int(obj.(int8)))
		return nil
	case uint8:
		this.PackAInt(int(obj.(uint8)))
		return nil
	case int16:
		this.PackAInt(int(obj.(int16)))
		return nil
	case uint16:
		this.PackAInt(int(obj.(uint16)))
		return nil
	case int32:
		this.PackAInt(int(obj.(int32)))
		return nil
	case uint32:
		this.PackAInt(int(obj.(uint32)))
		return nil
	case int:
		if Buffer.Arch32Bits {
			this.PackAInt(obj.(int))
			return nil
		} else {
			this.PackALong(int64(obj.(int)))
			return nil
		}
	case uint:
		if Buffer.Arch32Bits {
			this.PackAInt(int(obj.(uint)))
			return nil
		} // uint64 not supported
	case int64:
		this.PackALong(obj.(int64))
		return nil
	case nil:
		this.PackNil()
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

		return this.PackList(arr)
	case reflect.Map:
		return this.PackMap(obj.(map[interface{}]interface{}))
	}

	// TODO: Must complete for all types
	return nil
}

func (this *Packer) PackALong(val int64) {
	if val >= 0 {
		if val < 128 {
			this.PackAByte(byte(val))
			return
		}

		if val <= math.MaxUint8 {
			this.PackByte(0xcc, byte(val))
			return
		}

		if val <= math.MaxUint16 {
			this.PackShort(0xcd, int16(val))
			return
		}

		if val <= math.MaxUint32 {
			this.PackInt(0xce, int32(val))
			return
		}
		this.PackLong(0xcf, val)
	} else {
		if val >= -32 {
			this.PackAByte(0xe0 | byte(val) + 32)
			return
		}

		if val >= math.MinInt8 {
			this.PackByte(0xd0, byte(val))
			return
		}

		if val >= math.MinInt16 {
			this.PackShort(0xd1, int16(val))
			return
		}

		if val >= math.MinInt32 {
			this.PackInt(0xd2, int32(val))
			return
		}
		this.PackLong(0xd3, val)
	}
}

func (this *Packer) PackAInt(val int) {
	if val >= 0 {
		if val < 128 {
			this.PackAByte(byte(val))
			return
		}

		if val < 256 {
			this.PackByte(0xcc, byte(val))
			return
		}

		if val < 65536 {
			this.PackShort(0xcd, int16(val))
			return
		}
		this.PackInt(0xce, int32(val))
	} else {
		if val >= -32 {
			this.PackAByte(0xe0 | byte(val+32))
			return
		}

		if val >= math.MinInt8 {
			this.PackByte(0xd0, byte(val))
			return
		}

		if val >= math.MinInt16 {
			this.PackShort(0xd1, int16(val))
			return
		}
		this.PackInt(0xd2, int32(val))
	}
}

func (this *Packer) PackString(val string) {
	size := len(val) + 1
	this.PackByteArrayBegin(size)
	this.buffer.WriteByte(byte(ParticleType.STRING))
	this.buffer.Write([]byte(val))
}

func (this *Packer) PackByteArray(src []byte, srcOffset int, srcLength int) {
	this.buffer.Write(src[srcOffset : srcOffset+srcLength])
}

func (this *Packer) PackLong(valType int, val int64) {
	this.buffer.WriteByte(byte(valType))
	this.buffer.Write(Buffer.Int64ToBytes(val, nil, this.offset))
}

func (this *Packer) PackInt(valType int, val int32) {
	this.buffer.WriteByte(byte(valType))
	this.buffer.Write(Buffer.Int32ToBytes(val, nil, this.offset))
}

func (this *Packer) PackShort(valType int, val int16) {
	this.buffer.WriteByte(byte(valType))
	this.buffer.Write(Buffer.Int16ToBytes(val, nil, this.offset))
}

func (this *Packer) PackByte(valType int, val byte) {
	this.buffer.WriteByte(byte(valType))
	this.buffer.WriteByte(val)
}

func (this *Packer) PackNil() {
	this.buffer.WriteByte(0xc0)
}

func (this *Packer) PackAByte(val byte) {
	this.buffer.WriteByte(val)
}
