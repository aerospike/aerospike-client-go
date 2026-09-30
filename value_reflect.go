//go:build !as_performance

// Copyright 2014-2022 Aerospike, Inc.
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
	"reflect"

	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

func init() {
	newValueReflect = concreteNewValueReflect
}

// if the returned value is nil, the caller will panic
func concreteNewValueReflect(v any) Value {
	// check for array and map
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Slice:
		// A named slice type differs from its unnamed shape only by the
		// outer name; Convert retypes the same slice header in O(1) and the
		// result re-enters the typed fast path.
		rt := rv.Type()
		if u := reflect.SliceOf(rt.Elem()); u != rt {
			if value := tryConcreteValue(rv.Convert(u).Interface()); value != nil {
				return value
			}
		}
		return newReflectListValue(rv)
	case reflect.Array:
		return newReflectListValue(rv)
	case reflect.Map:
		// Same O(1) name-shedding for maps. Only the outer name can be
		// shed: Go's conversion rules require identical key/elem types.
		rt := rv.Type()
		if u := reflect.MapOf(rt.Key(), rt.Elem()); u != rt {
			if value := tryConcreteValue(rv.Convert(u).Interface()); value != nil {
				return value
			}
		}
		return newReflectMapValue(rv)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return NewLongValue(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return NewLongValue(int64(rv.Uint()))
	case reflect.Bool:
		return NewBoolValue(rv.Bool())
	case reflect.Float32, reflect.Float64:
		return NewFloatValue(rv.Float())
	case reflect.String:
		return NewStringValue(rv.String())
	}

	return nil
}

///////////////////////////////////////////////////////////////////////////////

// reflectMapValue packs a map whose type misses the NewValue fast path
// (named key or value types, exotic shapes) without boxing its entries:
// each entry is copied into two scratch cells allocated once at
// construction, and scalars are read off them kind-by-kind. Packing itself
// therefore does not allocate. The scratch cells and the stored iterator
// make packing stateful, so a single instance must not be used by
// concurrent commands.
type reflectMapValue struct {
	rv   reflect.Value
	iter *reflect.MapIter
	k, v reflect.Value
}

func newReflectMapValue(rv reflect.Value) *reflectMapValue {
	rt := rv.Type()
	return &reflectMapValue{
		rv:   rv,
		iter: rv.MapRange(),
		k:    reflect.New(rt.Key()).Elem(),
		v:    reflect.New(rt.Elem()).Elem(),
	}
}

// EstimateSize returns the size of the value in wire protocol.
func (m *reflectMapValue) EstimateSize() (int, Error) {
	return m.pack(nil)
}

func (m *reflectMapValue) write(cmd BufferEx) (int, Error) {
	return m.pack(cmd)
}

func (m *reflectMapValue) pack(cmd BufferEx) (int, Error) {
	// Canonical (filter-expression) packs need key-sorted output; that path
	// materializes and boxes, but it only runs while building an expression.
	if isCanonicalPack(cmd) && m.rv.Len() > 1 {
		amap := make(map[any]any, m.rv.Len())
		m.iter.Reset(m.rv)
		for m.iter.Next() {
			// Unwrap named types to their base kinds: canonical key ranking
			// (canonicalKeyRank) matches concrete primitives, so a named
			// string key boxed as-is would rank as "other" and sort wrong.
			amap[reflectBaseIfc(m.iter.Key())] = reflectBaseIfc(m.iter.Value())
		}
		return packIfcMap(cmd, amap)
	}

	size, err := packMapBegin(cmd, m.rv.Len())
	if err != nil {
		return size, err
	}
	m.iter.Reset(m.rv)
	for m.iter.Next() {
		m.k.SetIterKey(m.iter)
		m.v.SetIterValue(m.iter)
		n, err := packReflect(cmd, m.k, true)
		size += n
		if err != nil {
			return 0, err
		}
		n, err = packReflect(cmd, m.v, false)
		size += n
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

// GetType returns wire protocol value type.
func (m *reflectMapValue) GetType() int {
	return ParticleType.MAP
}

// GetObject returns the original map as an any.
func (m *reflectMapValue) GetObject() any {
	return m.rv.Interface()
}

// String implements Stringer interface.
func (m *reflectMapValue) String() string {
	return fmt.Sprintf("%v", m.rv.Interface())
}

///////////////////////////////////////////////////////////////////////////////

// reflectListValue packs a slice or array whose type misses the NewValue
// fast path, reading elements in place via Index -- no per-entry boxing, no
// scratch state, so unlike reflectMapValue it is safe for concurrent packs.
type reflectListValue struct {
	rv reflect.Value
}

func newReflectListValue(rv reflect.Value) reflectListValue {
	return reflectListValue{rv: rv}
}

// EstimateSize returns the size of the value in wire protocol.
func (l reflectListValue) EstimateSize() (int, Error) {
	return l.pack(nil)
}

func (l reflectListValue) write(cmd BufferEx) (int, Error) {
	return l.pack(cmd)
}

func (l reflectListValue) pack(cmd BufferEx) (int, Error) {
	size, err := packArrayBegin(cmd, l.rv.Len())
	if err != nil {
		return size, err
	}
	for i := 0; i < l.rv.Len(); i++ {
		n, err := packReflect(cmd, l.rv.Index(i), false)
		size += n
		if err != nil {
			return 0, err
		}
	}
	return size, nil
}

// GetType returns wire protocol value type.
func (l reflectListValue) GetType() int {
	return ParticleType.LIST
}

// GetObject returns the original slice or array as an any.
func (l reflectListValue) GetObject() any {
	return l.rv.Interface()
}

// String implements Stringer interface.
func (l reflectListValue) String() string {
	return fmt.Sprintf("%v", l.rv.Interface())
}

///////////////////////////////////////////////////////////////////////////////

// packReflect packs a reflected value without boxing it where the kind
// allows: scalar kinds are read via the non-allocating accessors (Int,
// Uint, Float, String, Bool, Bytes), containers recurse reflectively, and
// only kinds with no reflective packing (structs other than through the
// interface path, etc.) fall back to boxing via packObject. The output is
// byte-identical to packTypedObject/packObject for the same data.
func packReflect(cmd BufferEx, v reflect.Value, mapKey bool) (int, Error) {
	switch v.Kind() {
	case reflect.String:
		return packString(cmd, v.String())
	case reflect.Int8, reflect.Int16, reflect.Int32:
		return packAInt(cmd, int(v.Int()))
	case reflect.Int, reflect.Int64:
		if Buffer.Arch32Bits && v.Kind() == reflect.Int {
			return packAInt(cmd, int(v.Int()))
		}
		return packAInt64(cmd, v.Int())
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return packAInt(cmd, int(v.Uint()))
	case reflect.Uint, reflect.Uint64:
		if Buffer.Arch32Bits && v.Kind() == reflect.Uint {
			return packAInt(cmd, int(v.Uint()))
		}
		return packAUInt64(cmd, v.Uint())
	case reflect.Bool:
		return packBool(cmd, v.Bool())
	case reflect.Float32:
		return packFloat32(cmd, float32(v.Float()))
	case reflect.Float64:
		return packFloat64(cmd, v.Float())
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return packBytes(cmd, v.Bytes())
		}
		return reflectListValue{rv: v}.pack(cmd)
	case reflect.Array:
		return reflectListValue{rv: v}.pack(cmd)
	case reflect.Map:
		// Nested exotic maps need their own scratch cells; nesting is the
		// rare case inside an already-rare path.
		return newReflectMapValue(v).pack(cmd)
	case reflect.Interface:
		if v.IsNil() {
			return packNil(cmd)
		}
		return packObject(cmd, v.Interface(), mapKey)
	default:
		return packObject(cmd, v.Interface(), mapKey)
	}
}

// reflectBaseIfc boxes a reflected value as its base kind (a named string
// becomes string, a named int becomes int64, ...), preserving float width
// since float32 and float64 pack differently. Containers and everything
// else box as-is.
func reflectBaseIfc(v reflect.Value) any {
	switch v.Kind() {
	case reflect.String:
		return v.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return v.Uint()
	case reflect.Float32:
		return float32(v.Float())
	case reflect.Float64:
		return v.Float()
	case reflect.Bool:
		return v.Bool()
	default:
		return v.Interface()
	}
}
