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

package buffer

import (
	"encoding/binary"
	// "errors"
	"fmt"
	"math"
	"unsafe"

	// . "github.com/aerospike/aerospike-client-go/logger"
)

var sizeOfInt = unsafe.Sizeof(int(0))
var sizeOfInt32 = unsafe.Sizeof(int32(0))
var sizeOfInt64 = unsafe.Sizeof(int64(0))

var uint64sz int = int(unsafe.Sizeof(uint64(0)))
var uint32sz int = int(unsafe.Sizeof(uint32(0)))
var uint16sz int = int(unsafe.Sizeof(uint16(0)))

var Arch64Bits = (sizeOfInt == sizeOfInt64)
var Arch32Bits = (sizeOfInt == sizeOfInt32)

// Coverts a byte slice into a hex string
func BytesToHexString(buf []byte) string {
	hlist := make([]byte, 3*len(buf))

	for i := range buf {
		hex := fmt.Sprintf("%02x ", buf[i])
		idx := i * 3
		copy(hlist[idx:], []byte(hex))
	}
	return string(hlist)
}

// converts a slice of bytes into an interger with appropriate type
func BytesToNumber(buf []byte, offset, length int) interface{} {
	if len(buf) == 0 {
		return int(0)
	}

	// This will work for negative integers too which
	// will be represented in two's compliment representation.
	val := int64(0)

	for i := 0; i < length; i++ {
		val <<= 8
		val = val | int64((buf[offset+i] & 0xFF))
	}

	if (sizeOfInt == sizeOfInt64) || (val <= math.MaxInt32 && val >= math.MinInt32) {
		return int(val)
	}

	return int64(val)
}

func BytesToIntIntel(buf []byte, offset int) int {
	return int(((buf[offset+3] & 0xFF) << 24) |
		((buf[offset+2] & 0xFF) << 16) |
		((buf[offset+1] & 0xFF) << 8) |
		(buf[offset] & 0xFF))
}

// Covertes a slice into int64; only maximum of 8 bytes will be used
func BytesToInt64(buf []byte, offset int) int64 {
	l := len(buf[offset:])
	if l > uint64sz {
		l = uint64sz
	}
	r := int64(binary.BigEndian.Uint64(buf[offset : offset+l]))
	return r
}

// Converts an int64 into slice of Bytes.
func Int64ToBytes(num int64, buffer []byte, offset int) []byte {
	if buffer != nil {
		binary.BigEndian.PutUint64(buffer[offset:], uint64(num))
		return nil
	} else {
		b := make([]byte, uint64sz)
		binary.BigEndian.PutUint64(b, uint64(num))
		return b
	}
}

// Covertes a slice into int32; only maximum of 4 bytes will be used
func BytesToInt32(buf []byte, offset int) int32 {
	return int32(binary.BigEndian.Uint32(buf[offset : offset+uint32sz]))
}

// Converts an int32 to a byte slice of size 4
func Int32ToBytes(num int32, buffer []byte, offset int) []byte {
	if buffer != nil {
		binary.BigEndian.PutUint32(buffer[offset:], uint32(num))
		return nil
	} else {
		b := make([]byte, uint32sz)
		binary.BigEndian.PutUint32(b, uint32(num))
		return b
	}
}

// Converts
func BytesToInt16(buf []byte, offset int) int16 {
	return int16(binary.BigEndian.Uint16(buf[offset : offset+uint16sz]))
}

// converts a
func Int16ToBytes(num int16, buffer []byte, offset int) []byte {
	if buffer != nil {
		binary.BigEndian.PutUint16(buffer[offset:], uint16(num))
		return nil
	} else {
		b := make([]byte, uint16sz)
		binary.BigEndian.PutUint16(b, uint16(num))
		return b
	}
}

func GetUnsigned(b byte) int {
	r := b

	if r < 0 {
		r = r & 0x7f
		r = r | 0x80
	}
	return int(r)
}
