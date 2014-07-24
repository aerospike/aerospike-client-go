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
	"errors"
	"fmt"

	// . "github.com/citrusleaf/aerospike-client-go/logger"
	. "github.com/citrusleaf/aerospike-client-go/types"
	ParticleType "github.com/citrusleaf/aerospike-client-go/types/particle_type"
	Buffer "github.com/citrusleaf/aerospike-client-go/utils/buffer"
)

type Unpacker struct {
	buffer []byte
	offset int
	length int
}

func NewUnpacker(buffer []byte, offset int, length int) *Unpacker {
	return &Unpacker{
		buffer: buffer,
		offset: offset,
		length: length,
	}
}

func (this *Unpacker) UnpackList() ([]interface{}, error) {
	if this.length <= 0 {
		return nil, nil
	}

	theType := this.buffer[this.offset] & 0xff
	this.offset++
	var count int

	if (theType & 0xf0) == 0x90 {
		count = int(theType & 0x0f)
	} else if theType == 0xdc {
		count = int(Buffer.BytesToInt16(this.buffer, this.offset))
		this.offset += 2
	} else if theType == 0xdd {
		count = int(Buffer.BytesToInt32(this.buffer, this.offset))
		this.offset += 4
	} else {
		return nil, nil
	}

	// Logger.Error("count %d, type: %x, %s", count, theType&0xf0, Buffer.BytesToHexString(this.buffer))

	return this.unpackList(count)
}

func (this *Unpacker) unpackList(count int) ([]interface{}, error) {
	// Logger.Error(">>>>>>> List size: %d", count)
	out := make([]interface{}, 0, count)

	for i := 0; i < count; i++ {
		if obj, err := this.unpackObject(); err != nil {
			return nil, err
		} else {
			// Logger.Error("adding element %#v to array index: %d", obj, i)
			out = append(out, obj)
		}
	}
	return out, nil
}

func (this *Unpacker) UnpackMap() (map[interface{}]interface{}, error) {
	if this.length <= 0 {
		return nil, nil
	}

	theType := this.buffer[this.offset] & 0xff
	this.offset++
	var count int

	if (theType & 0xf0) == 0x80 {
		count = int(theType & 0x0f)
	} else if theType == 0xde {
		count = int(Buffer.BytesToInt16(this.buffer, this.offset))
		this.offset += 2
	} else if theType == 0xdf {
		count = int(Buffer.BytesToInt32(this.buffer, this.offset))
		this.offset += 4
	} else {
		return make(map[interface{}]interface{}), nil
	}
	return this.unpackMap(count)
}

func (this *Unpacker) unpackMap(count int) (map[interface{}]interface{}, error) {
	out := make(map[interface{}]interface{}, count)

	for i := 0; i < count; i++ {
		key, err := this.unpackObject()
		if err != nil {
			return nil, err
		}
		val, err := this.unpackObject()
		if err != nil {
			return nil, err
		}
		out[key] = val
	}
	return out, nil
}

func (this *Unpacker) unpackBlob(count int) (interface{}, error) {
	theType := this.buffer[this.offset] & 0xff
	this.offset++
	count--
	var val interface{}

	switch theType {
	case ParticleType.STRING:
		val = string(this.buffer[this.offset : this.offset+count])
		break

	default:
		// Logger.Error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		val = this.buffer[this.offset : this.offset+count]
		break
	}
	this.offset += count

	return val, nil
}

func (this *Unpacker) unpackObject() (interface{}, error) {
	theType := this.buffer[this.offset] & 0xff
	this.offset++
	// Logger.Error("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@, type: %x", theType)

	switch theType {
	case 0xc0:
		return nil, nil

	case 0xc3:
		return true, nil

	case 0xc2:
		return false, nil

		// TODO: Float support for Value?
	// case 0xca: {
	//   val = Float.intBitsToFloat(Buffer.bytesToInt(this.buffer, this.offset));
	//   this.offset += 4;
	//   return this.GetDouble(val);
	// }

	// case 0xcb: {
	//   double val = Double.longBitsToDouble(Buffer.bytesToLong(this.buffer, this.offset));
	//   this.offset += 8;
	//   return this.GetDouble(val);
	// }

	case 0xcc:
		r := this.buffer[this.offset] & 0xff
		this.offset++

		return int(r), nil

	case 0xcd:
		val := uint16(Buffer.BytesToInt16(this.buffer, this.offset))
		this.offset += 2
		return int(val), nil

	case 0xce:
		val := uint32(Buffer.BytesToInt32(this.buffer, this.offset))
		this.offset += 4

		if Buffer.Arch64Bits {
			return int(val), nil
		} else {
			return int64(val), nil
		}

	// TODO: Fix this
	case 0xcf:
		val := Buffer.BytesToInt64(this.buffer, this.offset)
		this.offset += 8
		return val, nil

	case 0xd0:
		r := int8(this.buffer[this.offset])
		this.offset++
		return int(r), nil

	case 0xd1:
		val := Buffer.BytesToInt16(this.buffer, this.offset)
		this.offset += 2
		return int(val), nil

	case 0xd2:
		val := Buffer.BytesToInt32(this.buffer, this.offset)
		this.offset += 4
		return int(val), nil

	case 0xd3:
		val := Buffer.BytesToInt64(this.buffer, this.offset)
		this.offset += 8
		return int64(val), nil

	case 0xda:
		count := int(Buffer.BytesToInt16(this.buffer, this.offset))
		this.offset += 2
		return this.unpackBlob(count)

	case 0xdb:
		count := int(Buffer.BytesToInt32(this.buffer, this.offset))
		this.offset += 4
		return this.unpackBlob(count)

	case 0xdc:
		count := int(Buffer.BytesToInt16(this.buffer, this.offset))
		this.offset += 2
		return this.unpackList(count)

	case 0xdd:
		count := int(Buffer.BytesToInt32(this.buffer, this.offset))
		this.offset += 4
		return this.unpackList(count)

	case 0xde:
		count := int(Buffer.BytesToInt16(this.buffer, this.offset))
		this.offset += 2
		return this.unpackMap(count)

	case 0xdf:
		count := int(Buffer.BytesToInt32(this.buffer, this.offset))
		this.offset += 4
		return this.unpackMap(count)

	default:
		if (theType & 0xe0) == 0xa0 {
			return this.unpackBlob(int(theType & 0x1f))
		}

		if (theType & 0xf0) == 0x80 {
			return this.unpackMap(int(theType & 0x0f))
		}

		if (theType & 0xf0) == 0x90 {
			return this.unpackList(int(theType & 0x0f))
		}

		if theType < 0x80 {
			return int(theType), nil
		}

		if theType >= 0xe0 {
			return int(theType - 0xe0 - 32), nil
		}
		// TODO: add the error or panic
		panic(errors.New(fmt.Sprintf("Unknown this.unpack theType: %x", theType)))
	}

	return nil, SerializationErr()
}
