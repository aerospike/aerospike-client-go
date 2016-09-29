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
	"time"

	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

func __PackIfcList(cmd AerospikeBuffer, list []interface{}) (int, error) {
	size := 0
	n, err := __PackArrayBegin(cmd, len(list))
	if err != nil {
		return n, err
	}
	size += n

	for i := range list {
		n, err := __PackObject(cmd, list[i])
		if err != nil {
			return 0, err
		}
		size += n
	}

	return size, err
}

func __PackList(cmd AerospikeBuffer, list ListIter) (int, error) {
	size := 0
	n, err := __PackArrayBegin(cmd, list.Len())
	if err != nil {
		return n, err
	}
	size += n

	f := func(v interface{}) error {
		n, err := __PackObject(cmd, v)
		if err != nil {
			return err
		}
		size += n

		return nil
	}

	err = list.Range(f)
	return size, err
}

func __PackArrayBegin(cmd AerospikeBuffer, size int) (int, error) {
	if size < 16 {
		return __PackAByte(cmd, 0x90|byte(size))
	} else if size <= math.MaxUint16 {
		return __PackShort(cmd, 0xdc, int16(size))
	} else {
		return __PackInt(cmd, 0xdd, int32(size))
	}
}

func __PackIfcMap(cmd AerospikeBuffer, theMap map[interface{}]interface{}) (int, error) {
	size := 0
	n, err := __PackMapBegin(cmd, len(theMap))
	if err != nil {
		return n, err
	}
	size += n

	for k, v := range theMap {
		// TODO: This needs to go, possibly by the use of Value in Iter
		// if k != nil {
		// 	t := reflect.TypeOf(k)
		// 	if t.Kind() == reflect.Map || t.Kind() == reflect.Slice ||
		// 		(t.Kind() == reflect.Array && t.Elem().Kind() != reflect.Uint8) {
		// 		panic("Maps, Slices, and bounded arrays other than Bounded Byte Arrays are not supported as Map keys.")
		// 	}
		// }

		n, err := __PackObject(cmd, k)
		if err != nil {
			return 0, err
		}
		size += n
		n, err = __PackObject(cmd, v)
		if err != nil {
			return 0, err
		}
		size += n
	}

	return size, err
}

func __PackJsonMap(cmd AerospikeBuffer, theMap map[string]interface{}) (int, error) {
	size := 0
	n, err := __PackMapBegin(cmd, len(theMap))
	if err != nil {
		return n, err
	}
	size += n

	for k, v := range theMap {
		n, err := __PackString(cmd, k)
		if err != nil {
			return 0, err
		}
		size += n
		n, err = __PackObject(cmd, v)
		if err != nil {
			return 0, err
		}
		size += n
	}

	return size, err
}

func __PackMap(cmd AerospikeBuffer, theMap MapIter) (int, error) {
	size := 0
	n, err := __PackMapBegin(cmd, theMap.Len())
	if err != nil {
		return n, err
	}
	size += n

	f := func(k, v interface{}) error {
		// TODO: This needs to go, possibly by the use of Value in Iter
		if k != nil {
			t := reflect.TypeOf(k)
			if t.Kind() == reflect.Map || t.Kind() == reflect.Slice ||
				(t.Kind() == reflect.Array && t.Elem().Kind() != reflect.Uint8) {
				panic("Maps, Slices, and bounded arrays other than Bounded Byte Arrays are not supported as Map keys.")
			}
		}

		n, err := __PackObject(cmd, k)
		if err != nil {
			return err
		}
		size += n
		n, err = __PackObject(cmd, v)
		if err != nil {
			return err
		}
		size += n

		return nil
	}

	err = theMap.Range(f)
	return size, err
}

func __PackMapBegin(cmd AerospikeBuffer, size int) (int, error) {
	if size < 16 {
		return __PackAByte(cmd, 0x80|byte(size))
	} else if size <= math.MaxUint16 {
		return __PackShort(cmd, 0xde, int16(size))
	} else {
		return __PackInt(cmd, 0xdf, int32(size))
	}
}

func __PackBytes(cmd AerospikeBuffer, b []byte) (int, error) {
	size := 0
	n, err := __PackByteArrayBegin(cmd, len(b)+1)
	if err != nil {
		return n, err
	}
	size += n

	n, err = __PackAByte(cmd, ParticleType.BLOB)
	if err != nil {
		return size + n, err
	}
	size += n

	n, err = __PackByteArray(cmd, b)
	if err != nil {
		return size + n, err
	}
	size += n

	return size, nil
}

func __PackByteArrayBegin(cmd AerospikeBuffer, length int) (int, error) {
	if length < 32 {
		return __PackAByte(cmd, 0xa0|byte(length))
	} else if length < 65536 {
		return __PackShort(cmd, 0xda, int16(length))
	} else {
		return __PackInt(cmd, 0xdb, int32(length))
	}
}

func __PackObject(cmd AerospikeBuffer, obj interface{}) (int, error) {
	switch v := obj.(type) {
	case Value:
		return v.pack(cmd)
	case string:
		return __PackString(cmd, v)
	case []byte:
		return __PackBytes(cmd, obj.([]byte))
	case int8:
		return __PackAInt(cmd, int(v))
	case uint8:
		return __PackAInt(cmd, int(v))
	case int16:
		return __PackAInt(cmd, int(v))
	case uint16:
		return __PackAInt(cmd, int(v))
	case int32:
		return __PackAInt(cmd, int(v))
	case uint32:
		return __PackAInt(cmd, int(v))
	case int:
		if Buffer.Arch32Bits {
			return __PackAInt(cmd, v)
		}
		return __PackAInt64(cmd, int64(v))
	case uint:
		if Buffer.Arch32Bits {
			return __PackAInt(cmd, int(v))
		}
		return __PackAUInt64(cmd, uint64(v))
	case int64:
		return __PackAInt64(cmd, v)
	case uint64:
		return __PackAUInt64(cmd, v)
	case time.Time:
		return __PackAInt64(cmd, v.UnixNano())
	case nil:
		return __PackNil(cmd)
	case bool:
		return __PackBool(cmd, v)
	case float32:
		return __PackFloat32(cmd, v)
	case float64:
		return __PackFloat64(cmd, v)
	case struct{}:
		return __PackIfcMap(cmd, map[interface{}]interface{}{})
	case []interface{}:
		return __PackIfcList(cmd, v)
	case map[interface{}]interface{}:
		return __PackIfcMap(cmd, v)
	case ListIter:
		return __PackList(cmd, obj.(ListIter))
	case MapIter:
		return __PackMap(cmd, obj.(MapIter))
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
			return __PackBytes(cmd, arr)
		}

		l := rv.Len()
		arr := make([]interface{}, l)
		for i := 0; i < l; i++ {
			arr[i] = rv.Index(i).Interface()
		}
		return __PackIfcList(cmd, arr)
	case reflect.Map:
		l := rv.Len()
		amap := make(map[interface{}]interface{}, l)
		for _, i := range rv.MapKeys() {
			amap[i.Interface()] = rv.MapIndex(i).Interface()
		}
		return __PackIfcMap(cmd, amap)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return __PackObject(cmd, rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return __PackObject(cmd, rv.Uint())
	case reflect.Bool:
		return __PackObject(cmd, rv.Bool())
	case reflect.String:
		return __PackObject(cmd, rv.String())
	case reflect.Float32, reflect.Float64:
		return __PackObject(cmd, rv.Float())
	}

	panic(fmt.Sprintf("Type `%v` not supported to pack.", reflect.TypeOf(obj)))
}

func __PackAUInt64(cmd AerospikeBuffer, val uint64) (int, error) {
	return __PackUInt64(cmd, val)
}

func __PackAInt64(cmd AerospikeBuffer, val int64) (int, error) {
	if val >= 0 {
		if val < 128 {
			return __PackAByte(cmd, byte(val))
		}

		if val <= math.MaxUint8 {
			return __PackByte(cmd, 0xcc, byte(val))
		}

		if val <= math.MaxUint16 {
			return __PackShort(cmd, 0xcd, int16(val))
		}

		if val <= math.MaxUint32 {
			return __PackInt(cmd, 0xce, int32(val))
		}
		return __PackInt64(cmd, 0xd3, val)
	} else {
		if val >= -32 {
			return __PackAByte(cmd, 0xe0|(byte(val)+32))
		}

		if val >= math.MinInt8 {
			return __PackByte(cmd, 0xd0, byte(val))
		}

		if val >= math.MinInt16 {
			return __PackShort(cmd, 0xd1, int16(val))
		}

		if val >= math.MinInt32 {
			return __PackInt(cmd, 0xd2, int32(val))
		}
		return __PackInt64(cmd, 0xd3, val)
	}
}

func __PackAInt(cmd AerospikeBuffer, val int) (int, error) {
	if val >= 0 {
		if val < 128 {
			return __PackAByte(cmd, byte(val))
		}

		if val < 256 {
			return __PackByte(cmd, 0xcc, byte(val))
		}

		if val < 65536 {
			return __PackShort(cmd, 0xcd, int16(val))
		}
		return __PackInt(cmd, 0xce, int32(val))
	} else {
		if val >= -32 {
			return __PackAByte(cmd, 0xe0|(byte(val)+32))
		}

		if val >= math.MinInt8 {
			return __PackByte(cmd, 0xd0, byte(val))
		}

		if val >= math.MinInt16 {
			return __PackShort(cmd, 0xd1, int16(val))
		}
		return __PackInt(cmd, 0xd2, int32(val))
	}
}

func __PackString(cmd AerospikeBuffer, val string) (int, error) {
	size := 0
	slen := len(val) + 1
	n, err := __PackByteArrayBegin(cmd, slen)
	if err != nil {
		return n, err
	}
	size += n

	if cmd != nil {
		n, err = cmd.WriteByte(byte(ParticleType.STRING))
		if err != nil {
			return size + n, err
		}
		size += n

		n, err = cmd.WriteString(val)
		if err != nil {
			return size + n, err
		}
		size += n
	} else {
		size += 1 + len(val)
	}

	return size, nil
}

func __PackGeoJson(cmd AerospikeBuffer, val string) (int, error) {
	size := 0
	slen := len(val) + 1
	n, err := __PackByteArrayBegin(cmd, slen)
	if err != nil {
		return n, err
	}
	size += n

	if cmd != nil {
		n, err = cmd.WriteByte(byte(ParticleType.GEOJSON))
		if err != nil {
			return size + n, err
		}
		size += n

		n, err = cmd.WriteString(val)
		if err != nil {
			return size + n, err
		}
		size += n
	} else {
		size += 1 + len(val)
	}

	return size, nil
}

func __PackByteArray(cmd AerospikeBuffer, src []byte) (int, error) {
	if cmd != nil {
		return cmd.Write(src)
	}
	return len(src), nil
}

func __PackInt64(cmd AerospikeBuffer, valType int, val int64) (int, error) {
	if cmd != nil {
		size, err := cmd.WriteByte(byte(valType))
		if err != nil {
			return size, err
		}

		n, err := cmd.WriteInt64(val)
		return size + n, err
	}
	return 1 + 8, nil
}

func __PackUInt64(cmd AerospikeBuffer, val uint64) (int, error) {
	if cmd != nil {
		size, err := cmd.WriteByte(byte(0xcf))
		if err != nil {
			return size, err
		}

		n, err := cmd.WriteInt64(int64(val))
		return size + n, err
	}
	return 1 + 8, nil
}

func __PackInt(cmd AerospikeBuffer, valType int, val int32) (int, error) {
	if cmd != nil {
		size, err := cmd.WriteByte(byte(valType))
		if err != nil {
			return size, err
		}
		n, err := cmd.WriteInt32(val)
		return size + n, err
	}
	return 1 + 4, nil
}

func __PackShort(cmd AerospikeBuffer, valType int, val int16) (int, error) {
	if cmd != nil {
		size, err := cmd.WriteByte(byte(valType))
		if err != nil {
			return size, err
		}

		n, err := cmd.WriteInt16(val)
		return size + n, err
	}
	return 1 + 2, nil
}

// This method is not compatible with MsgPack specs and is only used by aerospike client<->server
// for wire transfer only
func __PackShortRaw(cmd AerospikeBuffer, val int16) (int, error) {
	if cmd != nil {
		return cmd.WriteInt16(val)
	}
	return 2, nil
}

func __PackByte(cmd AerospikeBuffer, valType int, val byte) (int, error) {
	if cmd != nil {
		size := 0
		n, err := cmd.WriteByte(byte(valType))
		if err != nil {
			return n, err
		}
		size += n

		n, err = cmd.WriteByte(val)
		if err != nil {
			return size + n, err
		}
		size += n

		return size, nil
	}
	return 1 + 1, nil
}

func __PackNil(cmd AerospikeBuffer) (int, error) {
	if cmd != nil {
		return cmd.WriteByte(0xc0)
	}
	return 1, nil
}

func __PackBool(cmd AerospikeBuffer, val bool) (int, error) {
	if cmd != nil {
		if val {
			return cmd.WriteByte(0xc3)
		}
		return cmd.WriteByte(0xc2)
	}
	return 1, nil
}

func __PackFloat32(cmd AerospikeBuffer, val float32) (int, error) {
	if cmd != nil {
		size := 0
		n, err := cmd.WriteByte(0xca)
		if err != nil {
			return n, err
		}
		size += n
		n, err = cmd.WriteFloat32(val)
		return size + n, err
	}
	return 1 + 4, nil
}

func __PackFloat64(cmd AerospikeBuffer, val float64) (int, error) {
	if cmd != nil {
		size := 0
		n, err := cmd.WriteByte(0xcb)
		if err != nil {
			return n, err
		}
		size += n
		n, err = cmd.WriteFloat64(val)
		return size + n, err
	}
	return 1 + 8, nil
}

func __PackAByte(cmd AerospikeBuffer, val byte) (int, error) {
	if cmd != nil {
		return cmd.WriteByte(val)
	}
	return 1, nil
}
