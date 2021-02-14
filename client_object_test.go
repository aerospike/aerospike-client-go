// +build !as_performance

// Copyright 2014-2021 Aerospike, Inc.
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

package aerospike_test

import (
	"fmt"
	"math"
	"strconv"
	"time"

	as "github.com/aerospike/aerospike-client-go"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike", func() {

	gg.Describe("Data operations on objects", func() {
		// connection data
		var err error
		var ns = *namespace
		var set = randString(50)
		var key *as.Key

		gg.BeforeEach(func() {

			key, err = as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		type SomeBool bool
		type SomeByte byte
		type SomeInt int
		type SomeUint uint
		type SomeInt8 int8
		type SomeUint8 uint8
		type SomeInt16 int16
		type SomeUint16 uint16
		type SomeInt32 int32
		type SomeUint32 uint32
		type SomeInt64 int64
		type SomeUint64 uint64
		type SomeFloat32 float32
		type SomeFloat64 float64
		type SomeString string

		type SomeStruct struct {
			A    int
			Self *SomeStruct
		}

		type testObject struct {
			TTL uint32 `asm:"ttl"`
			Gen uint32 `asm:"gen"`

			Nil  interface{}
			NilP *int

			Bool  bool
			BoolP *bool

			Byte  byte
			ByteP *byte

			Int  int
			Intp *int

			Int8   int8
			Int8P  *int8
			UInt8  uint8
			UInt8P *uint8

			Int16   int16
			Int16P  *int16
			UInt16  uint16
			UInt16P *uint16

			Int32   int32
			Int32P  *int32
			UInt32  uint32
			UInt32P *uint32

			Int64   int64
			Int64P  *int64
			UInt64  uint64
			UInt64P *uint64

			F32  float32
			F32P *float32

			F64  float64
			F64P *float64

			String  string
			StringP *string

			Interface interface{}
			// InterfaceP interface{}
			InterfacePP *interface{}

			ByteArray     []byte
			Array         [3]interface{}
			SliceString   []string
			SliceFloat64  []float64
			SliceInt      []interface{}
			Slice         []interface{}
			ArrayOfMaps   [1]map[int]string
			SliceOfMaps   []map[int]string
			ArrayOfSlices [1][]interface{}
			SliceOfSlices [][]interface{}
			ArrayOfArrays [1][1]interface{}
			SliceOfArrays [][1]interface{}

			ArrayOfStructs [1]SomeStruct
			SliceOfStructs []SomeStruct

			Map           map[interface{}]interface{}
			MapOfMaps     map[string]map[int64]byte
			MapOfSlices   map[string][]byte
			MapOfArrays   map[string][3]byte
			MapOfStructs  map[string]SomeStruct
			MapOfPStructs map[string]*SomeStruct

			CustomBool    SomeBool
			CustomBoolP   *SomeBool
			CustomByte    SomeByte
			CustomByteP   *SomeByte
			CustomInt     SomeInt
			CustomIntP    *SomeInt
			CustomUint    SomeUint
			CustomUintP   *SomeUint
			CustomInt8    SomeInt8
			CustomInt8P   *SomeInt8
			CustomUint8   SomeUint8
			CustomUint8P  *SomeUint8
			CustomInt16   SomeInt16
			CustomInt16P  *SomeInt16
			CustomUint16  SomeUint16
			CustomUint16P *SomeUint16
			CustomInt32   SomeInt32
			CustomInt32P  *SomeInt32
			CustomUint32  SomeUint32
			CustomUint32P *SomeUint32
			CustomInt64   SomeInt64
			CustomInt64P  *SomeInt64
			CustomUint64  SomeUint64
			CustomUint64P *SomeUint64

			CustomFloat32  SomeFloat32
			CustomFloat32P *SomeFloat32
			CustomFloat64  SomeFloat64
			CustomFloat64P *SomeFloat64

			CustomString  SomeString
			CustomStringP *SomeString

			NestedObj     SomeStruct
			NestedObjP    *testObject
			EmpNestedObjP *testObject

			// Important: Used in ODMs
			NestedObjSlice []SomeStruct
			EmpNstdObjSlic []SomeStruct
			NstdObjPSlice  []*testObject
			EmpNstdObjPSlc []*testObject

			// std lib type
			Tm  time.Time
			TmP *time.Time

			Anonym struct {
				SomeStruct
			}

			AnonymP *struct {
				SomeStruct
			}
		}

		type testObjectTagged struct {
			TTL uint32 `asm:"ttl"`
			Gen uint32 `asm:"gen"`

			Nil  interface{} `as:"nil"`
			NilP *int        `as:"nilp"`

			Bool  bool  `as:"bool"`
			BoolP *bool `as:"boolp"`

			Byte  byte  `as:"byte"`
			ByteP *byte `as:"bytep"`

			Int  int  `as:"int"`
			Intp *int `as:"intp"`

			Int8   int8   `as:"int8"`
			Int8P  *int8  `as:"int8p"`
			UInt8  uint8  `as:"uint8"`
			UInt8P *uint8 `as:"uint8p"`

			Int16   int16   `as:"int16"`
			Int16P  *int16  `as:"int16p"`
			UInt16  uint16  `as:"uint16"`
			UInt16P *uint16 `as:"uint16p"`

			Int32   int32   `as:"int32"`
			Int32P  *int32  `as:"int32p"`
			UInt32  uint32  `as:"uint32"`
			UInt32P *uint32 `as:"uint32p"`

			Int64   int64   `as:"int64"`
			Int64P  *int64  `as:"int64p"`
			UInt64  uint64  `as:"uint64"`
			UInt64P *uint64 `as:"uint64p"`

			F32  float32  `as:"f32"`
			F32P *float32 `as:"f32p"`

			F64  float64  `as:"f64"`
			F64P *float64 `as:"f64p"`

			String  string  `as:"string"`
			StringP *string `as:"stringp"`

			Interface interface{} `as:"interface"`
			// InterfaceP interface{}  `as:"// interface"`
			InterfacePP *interface{} `as:"interfacepp"`

			ByteArray    []byte         `as:"bytearray"`
			Array        [3]interface{} `as:"array"`
			SliceString  []string       `as:"slicestring"`
			SliceFloat64 []float64      `as:"slicefloat64"`
			SliceInt     []interface{}  `as:"sliceint"`
			Slice        []interface{}  `as:"slice"`

			ArrayOfMaps    [1]map[int]string `as:"arrayOfMaps"`
			SliceOfMaps    []map[int]string  `as:"sliceOfMaps"`
			ArrayOfSlices  [1][]interface{}  `as:"arrayOfSlices"`
			SliceOfSlices  [][]interface{}   `as:"sliceOfSlices"`
			ArrayOfArrays  [1][1]interface{} `as:"arrayOfArrays"`
			SliceOfArrays  [][1]interface{}  `as:"sliceOfArrays"`
			ArrayOfStructs [1]SomeStruct     `as:"ArrayOfStructs"`
			SliceOfStructs []SomeStruct      `as:"SliceOfStructs"`

			Map           map[interface{}]interface{} `as:"map"`
			MapOfMaps     map[string]map[int64]byte   `as:"mapOfMaps"`
			MapOfSlices   map[string][]byte           `as:"mapOfSlices"`
			MapOfArrays   map[string][3]byte          `as:"MapOfArrays"`
			MapOfStructs  map[string]SomeStruct       `as:"mapOfStructs"`
			MapOfPStructs map[string]*SomeStruct      `as:"mapOfPStructs"`

			CustomBool    SomeBool    `as:"custombool"`
			CustomBoolP   *SomeBool   `as:"customboolp"`
			CustomByte    SomeByte    `as:"custombyte"`
			CustomByteP   *SomeByte   `as:"custombytep"`
			CustomInt     SomeInt     `as:"customint"`
			CustomIntP    *SomeInt    `as:"customintp"`
			CustomUint    SomeUint    `as:"customuint"`
			CustomUintP   *SomeUint   `as:"customuintp"`
			CustomInt8    SomeInt8    `as:"customint8"`
			CustomInt8P   *SomeInt8   `as:"customint8p"`
			CustomUint8   SomeUint8   `as:"customuint8"`
			CustomUint8P  *SomeUint8  `as:"customuint8p"`
			CustomInt16   SomeInt16   `as:"customint16"`
			CustomInt16P  *SomeInt16  `as:"customint16p"`
			CustomUint16  SomeUint16  `as:"customuint16"`
			CustomUint16P *SomeUint16 `as:"customuint16p"`
			CustomInt32   SomeInt32   `as:"customint32"`
			CustomInt32P  *SomeInt32  `as:"customint32p"`
			CustomUint32  SomeUint32  `as:"customuint32"`
			CustomUint32P *SomeUint32 `as:"customuint32p"`
			CustomInt64   SomeInt64   `as:"customint64"`
			CustomInt64P  *SomeInt64  `as:"customint64p"`
			CustomUint64  SomeUint64  `as:"customuint64"`
			CustomUint64P *SomeUint64 `as:"customuint64p"`

			CustomFloat32  SomeFloat32  `as:"customfloat32"`
			CustomFloat32P *SomeFloat32 `as:"customfloat32p"`
			CustomFloat64  SomeFloat64  `as:"customfloat64"`
			CustomFloat64P *SomeFloat64 `as:"customfloat64p"`

			CustomString  SomeString  `as:"customstring"`
			CustomStringP *SomeString `as:"customstringp"`

			NestedObj     SomeStruct  `as:"nestedobj"`
			NestedObjP    *testObject `as:"nestedobjp"`
			EmpNestedObjP *testObject `as:"empnestedobj"`

			// Important: Used in ODMs  `as:"// important"`
			NestedObjSlice []SomeStruct  `as:"nestedobjslice"`
			EmpNstdObjSlic []SomeStruct  `as:"empnstdobj"`
			NstdObjPSlice  []*testObject `as:"nstdobjpslice"`
			EmpNstdObjPSlc []*testObject `as:"empnstdobjp"`

			// std lib type  `as:"// std lib"`
			Tm  time.Time  `as:"tm  time."`
			TmP *time.Time `as:"tmp"`

			Anonym struct {
				SomeStruct
			} `as:"anonym"`

			AnonymP *struct {
				SomeStruct
			} `as:"anonymp"`
		}

		makeTestObject := func() *testObject {
			bl := true
			b := byte(0)
			ip := 11
			p8 := int8(4)
			up8 := uint8(6)
			p16 := int16(8)
			up16 := uint16(10)
			p32 := int32(12)
			up32 := uint32(14)
			p64 := int64(16)
			up64 := uint64(math.MaxUint64)
			f32p := float32(math.MaxFloat32)
			f64p := math.MaxFloat64
			str := "pointer to a string"
			iface := interface{}("a string")

			ctbl := SomeBool(true)
			ctb := SomeByte(100)
			cti := SomeInt(math.MinInt64)
			ctui := SomeUint(math.MaxInt64)
			cti8 := SomeInt8(103)
			ctui8 := SomeUint8(math.MaxUint8)
			cti16 := SomeInt16(math.MinInt16)
			ctui16 := SomeUint16(math.MaxUint16)
			cti32 := SomeInt32(math.MinInt32)
			ctui32 := SomeUint32(math.MaxUint32)
			cti64 := SomeInt64(math.MinInt64)
			ctui64 := SomeUint64(math.MaxUint64)
			cf32 := SomeFloat32(math.SmallestNonzeroFloat32)
			cf64 := SomeFloat64(math.SmallestNonzeroFloat64)
			ctstr := SomeString("Some string")

			now := time.Now().Round(time.Nanosecond)

			return &testObject{
				Bool:  true,
				BoolP: &bl,

				Nil:  nil,
				NilP: nil,

				Byte:  byte(0),
				ByteP: &b,

				Int:  1,
				Intp: &ip,

				Int8:   3,
				Int8P:  &p8,
				UInt8:  5,
				UInt8P: &up8,

				Int16:   7,
				Int16P:  &p16,
				UInt16:  9,
				UInt16P: &up16,

				Int32:   11,
				Int32P:  &p32,
				UInt32:  13,
				UInt32P: &up32,

				Int64:   math.MaxInt64,
				Int64P:  &p64,
				UInt64:  math.MaxUint64,
				UInt64P: &up64,

				F32:  1.87132794,
				F32P: &f32p,
				F64:  59285092891.502818573,
				F64P: &f64p,

				String:  "string",
				StringP: &str,

				Interface: iface,
				// InterfaceP:  ifaceP, // NOTICE: NOT SUPPORTED
				InterfacePP: &iface,

				ByteArray:      []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
				Array:          [3]interface{}{1, "string", nil},
				SliceString:    []string{"string1", "string2", "string3"},
				SliceFloat64:   []float64{1.1, 2.2, 3.3, 4.4},
				SliceInt:       []interface{}{1, 2, 3},
				Slice:          []interface{}{1, "string", []byte{1, 11, 111}, nil, true},
				ArrayOfMaps:    [1]map[int]string{{1: "str"}},
				SliceOfMaps:    []map[int]string{{1: "str"}},
				ArrayOfSlices:  [1][]interface{}{{1, 2, 3}},
				SliceOfSlices:  [][]interface{}{{1, 2, 3}, {4, 5, 6}},
				ArrayOfArrays:  [1][1]interface{}{{1}},
				SliceOfArrays:  [][1]interface{}{{1}, {2}, {3}},
				ArrayOfStructs: [1]SomeStruct{{A: 1, Self: &SomeStruct{A: 1}}},
				SliceOfStructs: []SomeStruct{{A: 1, Self: &SomeStruct{A: 1}}},

				Map:           map[interface{}]interface{}{1: "string", "string": nil, nil: map[interface{}]interface{}{"1": ip}, true: false},
				MapOfMaps:     map[string]map[int64]byte{"1": {1: 1, 2: 2}},
				MapOfSlices:   map[string][]byte{"1": {1, 2}, "2": {3, 4}},
				MapOfArrays:   map[string][3]byte{"1": {1, 2, 3}, "2": {3, 4, 5}},
				MapOfStructs:  map[string]SomeStruct{"1": {A: 10, Self: &SomeStruct{A: 10}}},
				MapOfPStructs: map[string]*SomeStruct{"1": {A: 10, Self: &SomeStruct{A: 10}}},

				CustomBool:    true,
				CustomBoolP:   &ctbl,
				CustomByte:    100,
				CustomByteP:   &ctb,
				CustomInt:     100,
				CustomIntP:    &cti,
				CustomUint:    100,
				CustomUintP:   &ctui,
				CustomInt8:    100,
				CustomInt8P:   &cti8,
				CustomUint8:   100,
				CustomUint8P:  &ctui8,
				CustomInt16:   100,
				CustomInt16P:  &cti16,
				CustomUint16:  100,
				CustomUint16P: &ctui16,
				CustomInt32:   100,
				CustomInt32P:  &cti32,
				CustomUint32:  100,
				CustomUint32P: &ctui32,
				CustomInt64:   100,
				CustomInt64P:  &cti64,
				CustomUint64:  100,
				CustomUint64P: &ctui64,

				CustomFloat32:  cf32,
				CustomFloat32P: &cf32,
				CustomFloat64:  cf64,
				CustomFloat64P: &cf64,

				CustomString:  ctstr,
				CustomStringP: &ctstr,

				NestedObj:  SomeStruct{A: 1, Self: &SomeStruct{A: 999}},
				NestedObjP: &testObject{Int: 1, Intp: &ip, Tm: now},

				NestedObjSlice: []SomeStruct{{A: 1, Self: &SomeStruct{A: 999}}, {A: 2, Self: &SomeStruct{A: 998}}},
				NstdObjPSlice:  []*testObject{{Int: 1, Intp: &ip, Tm: now}, {Int: 2, Intp: &ip, Tm: now}},

				Tm:  now,
				TmP: &now,

				Anonym:  struct{ SomeStruct }{SomeStruct{A: 1, Self: &SomeStruct{A: 999}}},
				AnonymP: &(struct{ SomeStruct }{SomeStruct{A: 1, Self: &SomeStruct{A: 999}}}),
			}
		}

		makeTestObjectTagged := func() *testObjectTagged {
			bl := true
			b := byte(0)
			ip := 11
			p8 := int8(4)
			up8 := uint8(6)
			p16 := int16(8)
			up16 := uint16(10)
			p32 := int32(12)
			up32 := uint32(14)
			p64 := int64(16)
			up64 := uint64(math.MaxUint64)
			f32p := float32(math.MaxFloat32)
			f64p := math.MaxFloat64
			str := "pointer to a string"
			iface := interface{}("a string")

			ctbl := SomeBool(true)
			ctb := SomeByte(100)
			cti := SomeInt(math.MinInt64)
			ctui := SomeUint(math.MaxInt64)
			cti8 := SomeInt8(103)
			ctui8 := SomeUint8(math.MaxUint8)
			cti16 := SomeInt16(math.MinInt16)
			ctui16 := SomeUint16(math.MaxUint16)
			cti32 := SomeInt32(math.MinInt32)
			ctui32 := SomeUint32(math.MaxUint32)
			cti64 := SomeInt64(math.MinInt64)
			ctui64 := SomeUint64(math.MaxUint64)
			cf32 := SomeFloat32(math.SmallestNonzeroFloat32)
			cf64 := SomeFloat64(math.SmallestNonzeroFloat64)
			ctstr := SomeString("Some string")

			now := time.Now().Round(time.Nanosecond)

			return &testObjectTagged{
				Bool:  true,
				BoolP: &bl,

				Nil:  nil,
				NilP: nil,

				Byte:  byte(0),
				ByteP: &b,

				Int:  1,
				Intp: &ip,

				Int8:   3,
				Int8P:  &p8,
				UInt8:  5,
				UInt8P: &up8,

				Int16:   7,
				Int16P:  &p16,
				UInt16:  9,
				UInt16P: &up16,

				Int32:   11,
				Int32P:  &p32,
				UInt32:  13,
				UInt32P: &up32,

				Int64:   math.MaxInt64,
				Int64P:  &p64,
				UInt64:  math.MaxUint64,
				UInt64P: &up64,

				F32:  1.87132794,
				F32P: &f32p,
				F64:  59285092891.502818573,
				F64P: &f64p,

				String:  "string",
				StringP: &str,

				Interface: iface,
				// InterfaceP:  ifaceP, // NOTICE: NOT SUPPORTED
				InterfacePP: &iface,

				ByteArray:      []byte{1, 2, 3, 4, 5, 6, 7, 8, 9},
				Array:          [3]interface{}{1, "string", nil},
				SliceString:    []string{"string1", "string2", "string3"},
				SliceFloat64:   []float64{1.1, 2.2, 3.3, 4.4},
				SliceInt:       []interface{}{1, 2, 3},
				Slice:          []interface{}{1, "string", []byte{1, 11, 111}, nil, true},
				ArrayOfMaps:    [1]map[int]string{{1: "str"}},
				SliceOfMaps:    []map[int]string{{1: "str"}},
				ArrayOfSlices:  [1][]interface{}{{1, 2, 3}},
				SliceOfSlices:  [][]interface{}{{1, 2, 3}, {4, 5, 6}},
				ArrayOfArrays:  [1][1]interface{}{{1}},
				SliceOfArrays:  [][1]interface{}{{1}, {2}, {3}},
				ArrayOfStructs: [1]SomeStruct{{A: 1, Self: &SomeStruct{A: 1}}},
				SliceOfStructs: []SomeStruct{{A: 1, Self: &SomeStruct{A: 1}}},

				Map:           map[interface{}]interface{}{1: "string", "string": nil, nil: map[interface{}]interface{}{"1": ip}, true: false},
				MapOfMaps:     map[string]map[int64]byte{"1": {1: 1, 2: 2}},
				MapOfSlices:   map[string][]byte{"1": {1, 2}, "2": {3, 4}},
				MapOfArrays:   map[string][3]byte{"1": {1, 2, 3}, "2": {3, 4, 5}},
				MapOfStructs:  map[string]SomeStruct{"1": {A: 10, Self: &SomeStruct{A: 10}}},
				MapOfPStructs: map[string]*SomeStruct{"1": {A: 10, Self: &SomeStruct{A: 10}}},

				CustomBool:    true,
				CustomBoolP:   &ctbl,
				CustomByte:    100,
				CustomByteP:   &ctb,
				CustomInt:     100,
				CustomIntP:    &cti,
				CustomUint:    100,
				CustomUintP:   &ctui,
				CustomInt8:    100,
				CustomInt8P:   &cti8,
				CustomUint8:   100,
				CustomUint8P:  &ctui8,
				CustomInt16:   100,
				CustomInt16P:  &cti16,
				CustomUint16:  100,
				CustomUint16P: &ctui16,
				CustomInt32:   100,
				CustomInt32P:  &cti32,
				CustomUint32:  100,
				CustomUint32P: &ctui32,
				CustomInt64:   100,
				CustomInt64P:  &cti64,
				CustomUint64:  100,
				CustomUint64P: &ctui64,

				CustomFloat32:  cf32,
				CustomFloat32P: &cf32,
				CustomFloat64:  cf64,
				CustomFloat64P: &cf64,

				CustomString:  ctstr,
				CustomStringP: &ctstr,

				NestedObj:  SomeStruct{A: 1, Self: &SomeStruct{A: 999}},
				NestedObjP: &testObject{Int: 1, Intp: &ip, Tm: now},

				NestedObjSlice: []SomeStruct{{A: 1, Self: &SomeStruct{A: 999}}, {A: 2, Self: &SomeStruct{A: 998}}},
				NstdObjPSlice:  []*testObject{{Int: 1, Intp: &ip, Tm: now}, {Int: 2, Intp: &ip, Tm: now}},

				Tm:  now,
				TmP: &now,

				Anonym:  struct{ SomeStruct }{SomeStruct{A: 1, Self: &SomeStruct{A: 999}}},
				AnonymP: &(struct{ SomeStruct }{SomeStruct{A: 1, Self: &SomeStruct{A: 999}}}),
			}
		}

		gg.Context("PutObject operations", func() {

			gg.It("must respect `,omitempty` option", func() {
				type Inner struct {
					V1 int    `as:"   v1,ommitempty "`
					V2 string `as:"   v2,ommitempty    "`
				}

				type T struct {
					Name        string `as:"   name "`
					Description string `as:" desc    ,omitempty"`
					Age         int    `as:"     ,omitempty"`

					InnerVal Inner `as:"inner,omitempty"`
				}

				t := &T{
					Name:        "Ada Lovelace",
					Description: "Was doing it before it was cool",
					Age:         31,
				}

				key, _ := as.NewKey(ns, set, randString(50))
				err = client.PutObject(nil, key, t)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"name": t.Name, "desc": t.Description, "Age": 31, "inner": map[interface{}]interface{}{"v1": 0, "v2": ""}}))

				key, _ = as.NewKey(ns, set, randString(50))

				t = &T{
					Name:        "",
					Description: "",
					Age:         0,
				}

				err = client.PutObject(nil, key, t)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err = client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"name": t.Name, "inner": map[interface{}]interface{}{"v1": 0, "v2": ""}}))
			})

			gg.It("must save an object with the most complex structure possible", func() {

				testObj := makeTestObject()
				err := client.PutObject(nil, key, &testObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				resObj := &testObject{}
				err = client.GetObject(nil, key, resObj)

				// set the Gen and TTL
				testObj.TTL = resObj.TTL
				testObj.Gen = resObj.Gen

				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(resObj).To(gm.Equal(testObj))
				gm.Expect(resObj.AnonymP).NotTo(gm.BeNil())

				// should not panic if read back to an object with none of the bins
				T := struct {
					NonExisting int `as:"nonexisting"`
				}{-1}
				err = client.GetObject(nil, key, &T)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(T.NonExisting).To(gm.Equal(-1))

				// get the same object via BatchGetObject
				resObj = &testObject{}
				found, err := client.BatchGetObjects(nil, []*as.Key{key}, []interface{}{resObj})
				gm.Expect(len(found)).To(gm.Equal(1))
				gm.Expect(found[0]).To(gm.BeTrue())
				gm.Expect(err).ToNot(gm.HaveOccurred())

				// set the Gen and TTL
				testObj.TTL = resObj.TTL
				testObj.Gen = resObj.Gen

				gm.Expect(resObj).To(gm.Equal(testObj))
				gm.Expect(resObj.AnonymP).NotTo(gm.BeNil())
			})

			gg.It("must save a tagged object with the most complex structure possible", func() {

				testObj := makeTestObjectTagged()
				err := client.PutObject(nil, key, &testObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				resObj := &testObjectTagged{}
				err = client.GetObject(nil, key, resObj)

				// set the Gen and TTL
				testObj.TTL = resObj.TTL
				testObj.Gen = resObj.Gen

				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(resObj).To(gm.Equal(testObj))
				gm.Expect(resObj.AnonymP).NotTo(gm.BeNil())
			})

			gg.It("must save an object and read it back respecting the tags", func() {

				type InnerStruct struct {
					Strings         []string `as:"b"`
					PersistNot      int      `as:"-"`
					PersistAsInner1 int      `as:"inner1"`
				}

				type TaggedStruct struct {
					Strings       []string `as:"b"`
					DontPersist   int      `as:"-"`
					PersistAsFld1 int      `as:"fld1"`
					Bytes         []byte   `as:"fldbytes"`

					IStruct InnerStruct `as:"istruct"`
				}

				testObj := TaggedStruct{Strings: []string{"a", "b", "c"}, DontPersist: 1, PersistAsFld1: 2, Bytes: []byte{1, 2, 3, 4}, IStruct: InnerStruct{Strings: []string{"d", "e", "f", "g"}, PersistNot: 10, PersistAsInner1: 11}}
				err := client.PutObject(nil, key, &testObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				resObj := &TaggedStruct{}
				err = client.GetObject(nil, key, resObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(resObj.DontPersist).To(gm.Equal(0))
				gm.Expect(resObj.PersistAsFld1).To(gm.Equal(2))
				gm.Expect(resObj.IStruct.PersistNot).To(gm.Equal(0))
				gm.Expect(resObj.IStruct.PersistAsInner1).To(gm.Equal(11))

				// get the bins and check for bin names
				rec, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(rec.Bins).To(gm.Equal(
					as.BinMap{
						"b":        []interface{}{"a", "b", "c"},
						"fld1":     2,
						"fldbytes": []byte{1, 2, 3, 4},
						"istruct": map[interface{}]interface{}{
							"b":      []interface{}{"d", "e", "f", "g"},
							"inner1": 11,
						},
					}))

				gm.Expect(len(rec.Bins)).To(gm.Equal(4))
				gm.Expect(rec.Bins["DontPersist"]).To(gm.BeNil())
				gm.Expect(rec.Bins["fld1"]).To(gm.Equal(2))
				innerStruct := rec.Bins["istruct"].(map[interface{}]interface{})
				gm.Expect(len(innerStruct)).To(gm.Equal(2))
				gm.Expect(innerStruct["PersistNot"]).To(gm.BeNil())
				gm.Expect(innerStruct["inner1"]).To(gm.Equal(11))

			})

			gg.It("must save an object *pointer* and read it back respecting the tags", func() {

				type InnerStruct struct {
					PersistNot      int `as:"-"`
					PersistAsInner1 int `as:"inner1"`
				}

				type TaggedStruct struct {
					DontPersist   int `as:"-"`
					PersistAsFld1 int `as:"fld1"`

					IStruct *InnerStruct
				}

				testObj := &TaggedStruct{DontPersist: 1, PersistAsFld1: 2, IStruct: &InnerStruct{PersistNot: 10, PersistAsInner1: 11}}
				err := client.PutObject(nil, key, &testObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				resObj := &TaggedStruct{IStruct: &InnerStruct{}}
				err = client.GetObject(nil, key, resObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(resObj.DontPersist).To(gm.Equal(0))
				gm.Expect(resObj.PersistAsFld1).To(gm.Equal(2))
				gm.Expect(resObj.IStruct.PersistNot).To(gm.Equal(0))
				gm.Expect(resObj.IStruct.PersistAsInner1).To(gm.Equal(11))

				// get the bins and check for bin names
				rec, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(len(rec.Bins)).To(gm.Equal(2))
				gm.Expect(rec.Bins["DontPersist"]).To(gm.BeNil())
				gm.Expect(rec.Bins["fld1"]).To(gm.Equal(2))
				innerStruct := rec.Bins["IStruct"].(map[interface{}]interface{})
				gm.Expect(len(innerStruct)).To(gm.Equal(1))
				gm.Expect(innerStruct["PersistNot"]).To(gm.BeNil())
				gm.Expect(innerStruct["inner1"]).To(gm.Equal(11))

			})

			gg.It("must put and get pre-assigned lists and maps", func() {

				type MyObject struct {
					List []string
					Map  map[int]string
				}

				var t, o1, o2 MyObject
				o1.List = nil // the default is nil anyways
				o1.Map = nil  // the default is nil anyways

				o2.List = []string{"Should be overwritten"}
				o2.Map = map[int]string{666: "Nope"}

				t = MyObject{
					List: []string{"Apple", "Orange"},
					Map:  map[int]string{1: "Apple", 2: "Orange"},
				}

				err := client.PutObject(nil, key, &t)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{
					"Map":  map[interface{}]interface{}{1: "Apple", 2: "Orange"},
					"List": []interface{}{"Apple", "Orange"},
				}))

				err = client.GetObject(nil, key, &o1)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				err = client.GetObject(nil, key, &o2)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(o1).To(gm.Equal(o2))
				gm.Expect(t).To(gm.Equal(o1))
				gm.Expect(t).To(gm.Equal(o2))
			})

		}) // PutObject context

		gg.Context("Metadata operations", func() {

			gg.It("must save an object and read its metadata back", func() {

				type objMeta struct {
					TTL1, TTL2 uint32 `asm:"ttl"`
					GEN1, GEN2 uint32 `asm:"gen"`
					Val        int    `as:"val"`
				}

				testObj := objMeta{Val: 1}
				err := client.PutObject(nil, key, &testObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				rec, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(rec.Bins).To(gm.Equal(as.BinMap{"val": 1}))

				resObj := &objMeta{}
				err = client.GetObject(nil, key, resObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(resObj.TTL1).NotTo(gm.Equal(uint32(0)))
				gm.Expect(resObj.TTL1).To(gm.Equal(resObj.TTL2))

				gm.Expect(resObj.GEN1).To(gm.Equal(uint32(1)))
				gm.Expect(resObj.GEN2).To(gm.Equal(uint32(1)))

				// put it again to check the generation
				err = client.PutObject(nil, key, &testObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				err = client.GetObject(nil, key, resObj)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(resObj.TTL1).NotTo(gm.Equal(uint32(0)))
				gm.Expect(resObj.TTL1).To(gm.Equal(resObj.TTL2))

				defaultTTL, err := strconv.Atoi(nsInfo(ns, "default-ttl"))
				gm.Expect(err).ToNot(gm.HaveOccurred())

				switch defaultTTL {
				case 0:
					gm.Expect(resObj.TTL1).To(gm.Equal(uint32(math.MaxUint32)))
				default:
					gm.Expect(resObj.TTL1).To(gm.Equal(uint32(defaultTTL)))
				}

				gm.Expect(resObj.GEN1).To(gm.Equal(uint32(2)))
				gm.Expect(resObj.GEN2).To(gm.Equal(uint32(2)))
			})

		}) // PutObject context

		gg.Context("BatchGetObjects operations", func() {

			var keys []*as.Key
			var resObjects []interface{}
			var objects []*testObjectTagged

			gg.BeforeEach(func() {
				set = randString(50)

				keys = nil
				resObjects = nil
				objects = nil

				wpolicy := as.NewWritePolicy(0, 500)
				for i := 0; i < 100; i++ {
					key, err = as.NewKey(ns, set, randString(50))
					gm.Expect(err).ToNot(gm.HaveOccurred())
					keys = append(keys, key)
					resObjects = append(resObjects, new(testObjectTagged))

					// only put odd objects in the db
					if i%2 == 0 {
						objects = append(objects, new(testObjectTagged))
						continue
					}

					testObj := makeTestObjectTagged()
					objects = append(objects, testObj)
					err := client.PutObject(wpolicy, key, testObj)
					gm.Expect(err).ToNot(gm.HaveOccurred())
				}

			})

			gg.It("must return error on invalid input", func() {
				_, err := client.BatchGetObjects(nil, nil, resObjects)
				gm.Expect(err).To(gm.HaveOccurred())

				_, err = client.BatchGetObjects(nil, nil, nil)
				gm.Expect(err).To(gm.HaveOccurred())

				_, err = client.BatchGetObjects(nil, []*as.Key{}, []interface{}{})
				gm.Expect(err).To(gm.HaveOccurred())
			})

			gg.It("must get all objects with the most complex structure possible", func() {
				found, err := client.BatchGetObjects(nil, keys, resObjects)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				for i := range resObjects {
					if i%2 == 0 {
						gm.Expect(found[i]).To(gm.BeFalse())
						resObj := resObjects[i].(*testObjectTagged)
						gm.Expect(*resObj).To(gm.BeZero())
					} else {
						gm.Expect(found[i]).To(gm.BeTrue())
						resObj := resObjects[i].(*testObjectTagged)

						gm.Expect(resObj.TTL).To(gm.BeNumerically("<=", 500))
						gm.Expect(resObj.Gen).To(gm.BeNumerically(">", 0))

						objects[i].TTL = resObj.TTL
						objects[i].Gen = resObj.Gen

						gm.Expect(resObj).To(gm.Equal(objects[i]))
					}
				}
			})

		}) // ScanObjects context

		gg.Context("UDF Objects operations", func() {
			gg.It("must store and get values of types which implement Value interface using udf", func() {
				udfFunc := []byte(`function setValue(rec, val)
					rec['value'] = val
					aerospike:update(rec)
				
					return rec
				end`)

				regTask, err := client.RegisterUDF(nil, udfFunc, "test_set.lua", as.LUA)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(<-regTask.OnComplete()).ToNot(gm.HaveOccurred())

				var (
					bytes    = []byte("bytes")
					str      = "string"
					i        = 10
					f        = 3.14
					valArray = []as.Value{as.NewValue(i), as.NewValue(str)}
					list     = []interface{}{i, str}
					m        = map[interface{}]interface{}{"int": i, "string": str}
					json     = map[string]interface{}{"int": i, "string": str}
				)

				cases := []struct {
					in  as.Value
					out interface{}
				}{
					{in: as.NewNullValue(), out: nil},
					{in: as.NewInfinityValue(), out: nil},
					{in: as.NewWildCardValue(), out: nil},
					{in: as.NewBytesValue(bytes), out: bytes},
					{in: as.NewStringValue(str), out: str},
					{in: as.NewIntegerValue(i), out: i},
					{in: as.NewFloatValue(f), out: f},
					{in: as.NewValueArray(valArray), out: list},
					{in: as.NewListValue(list), out: list},
					{in: as.NewMapValue(m), out: m},
					{in: as.NewJsonValue(json), out: m},
				}

				for i, data := range cases {
					err = client.PutBins(nil, key, as.NewBin("test", i))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					_, err = client.Execute(nil, key, "test_set", "setValue", data.in)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					rec, err := client.Get(nil, key, "value")
					gm.Expect(err).ToNot(gm.HaveOccurred())

					if data.out == nil {
						gm.Expect(rec.Bins["value"]).To(gm.BeNil())
					} else {
						gm.Expect(rec.Bins["value"]).To(gm.Equal(data.out))
					}

				}
			}) // #272 issue

			gg.It("must serialize values to the lua function and deserialize into object, even if it is int", func() {

				type Test struct {
					Test    float64 `as:"test"`
					TestLua float64 `as:"testLua"`
				}

				udfFunc := []byte(`function addValue(rec, val)
			    local ret = map()
			    if not aerospike:exists(rec) then
			        ret['status'] = false
			        ret['result'] = 'Record does not exist'
			    else
			        rec['testLua'] = (rec['testLua'] or 0.0) + val
			        aerospike:update(rec)
			        ret['status'] = true
			    end
			    return ret
			end`)

				regTask, err := client.RegisterUDF(nil, udfFunc, "test.lua", as.LUA)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				gm.Expect(<-regTask.OnComplete()).ToNot(gm.HaveOccurred())

				adOp := as.AddOp(as.NewBin("test", float64(1)))
				testLua := float64(0)
				for i := 1; i <= 100; i++ {
					_, err := client.Operate(nil, key, adOp)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					testLua += float64(i) * float64(0.1)
					_, err = client.Execute(nil, key, "test", "addValue", as.NewValue(float64(i)*float64(0.1)))
					gm.Expect(err).ToNot(gm.HaveOccurred())

					t := &Test{}
					err = client.GetObject(nil, key, t)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					gm.Expect(t.Test).To(gm.Equal(float64(i)))
					gm.Expect(t.TestLua).To(gm.Equal(testLua))
				}

			}) // it
		})

		for _, failOnClusterChange := range []bool{false, true} {
			var scanPolicy = as.NewScanPolicy()
			scanPolicy.FailOnClusterChange = failOnClusterChange

			var queryPolicy = as.NewQueryPolicy()
			queryPolicy.FailOnClusterChange = failOnClusterChange

			gg.Context("ScanObjects operations", func() {

				type InnerStruct struct {
					PersistNot      int    `as:"-"`
					PersistAsInner1 int    `as:"inner1"`
					Gen             uint32 `asm:"gen"`
					TTL             uint32 `asm:"ttl"`
				}

				gg.BeforeEach(func() {
					set = randString(50)

					wp := as.NewWritePolicy(0, 500)
					for i := 1; i < 100; i++ {
						key, err = as.NewKey(ns, set, randString(50))
						gm.Expect(err).ToNot(gm.HaveOccurred())

						testObj := InnerStruct{PersistAsInner1: i}
						err := client.PutObject(wp, key, &testObj)
						gm.Expect(err).ToNot(gm.HaveOccurred())
					}

				})

				gg.It(fmt.Sprintf("must scan all objects with the most complex structure possible. FailOnClusterChange: %v", failOnClusterChange), func() {

					testObj := &InnerStruct{}

					retChan := make(chan *InnerStruct, 10)

					rs, err := client.ScanAllObjects(scanPolicy, retChan, ns, set)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					cnt := 0
					for resObj := range retChan {
						gm.Expect(resObj.PersistAsInner1).To(gm.BeNumerically(">", 0))
						gm.Expect(resObj.PersistNot).To(gm.Equal(0))
						gm.Expect(resObj.Gen).To(gm.BeNumerically(">", 0))
						gm.Expect(resObj.TTL).To(gm.BeNumerically("<=", 500))

						testObj.PersistAsInner1 = resObj.PersistAsInner1
						testObj.Gen = resObj.Gen
						testObj.TTL = resObj.TTL
						gm.Expect(resObj).To(gm.Equal(testObj))
						cnt++
					}

					for e := range rs.Errors {
						gm.Expect(e).ToNot(gm.HaveOccurred())
					}

					gm.Expect(cnt).To(gm.Equal(99))
				})

			}) // ScanObjects context

			gg.Context("QueryObjects operations", func() {

				type InnerStruct struct {
					PersistNot      int    `as:"-"`
					PersistAsInner1 int    `as:"inner1"`
					Gen             uint32 `asm:"gen"`
					TTL             uint32 `asm:"ttl"`
				}

				gg.BeforeEach(func() {
					set = randString(50)

					wp := as.NewWritePolicy(5, 500)
					for i := 1; i < 100; i++ {
						key, err = as.NewKey(ns, set, randString(50))
						gm.Expect(err).ToNot(gm.HaveOccurred())

						testObj := InnerStruct{PersistAsInner1: i}
						err := client.PutObject(wp, key, &testObj)
						gm.Expect(err).ToNot(gm.HaveOccurred())
					}

				})

				gg.It(fmt.Sprintf("must scan all objects with the most complex structure possible. FailOnClusterChange: %v", failOnClusterChange), func() {

					testObj := &InnerStruct{}

					retChan := make(chan *InnerStruct, 10)
					stmt := as.NewStatement(ns, set)

					rs, err := client.QueryObjects(queryPolicy, stmt, retChan)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					cnt := 0
					for resObj := range retChan {
						gm.Expect(resObj.PersistAsInner1).To(gm.BeNumerically(">", 0))
						gm.Expect(resObj.PersistNot).To(gm.Equal(0))
						gm.Expect(resObj.Gen).To(gm.BeNumerically(">", 0))
						gm.Expect(resObj.TTL).To(gm.BeNumerically("<=", 500))

						testObj.PersistAsInner1 = resObj.PersistAsInner1
						testObj.Gen = resObj.Gen
						testObj.TTL = resObj.TTL
						gm.Expect(resObj).To(gm.Equal(testObj))
						cnt++
					}

					for e := range rs.Errors {
						gm.Expect(e).ToNot(gm.HaveOccurred())
					}

					gm.Expect(cnt).To(gm.Equal(99))
				})

				gg.It(fmt.Sprintf("must query only relevant objects with the most complex structure possible. FailOnClusterChange: %v", failOnClusterChange), func() {

					// first create an index
					idxTask, err := client.CreateIndex(nil, ns, set, set+"inner1", "inner1", as.NUMERIC)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					defer client.DropIndex(nil, ns, set, set+"inner1")

					// wait until index is created
					<-idxTask.OnComplete()

					testObj := &InnerStruct{}

					retChan := make(chan *InnerStruct, 10)
					stmt := as.NewStatement(ns, set)
					stmt.SetFilter(as.NewRangeFilter("inner1", 21, 70))

					rs, err := client.QueryObjects(queryPolicy, stmt, retChan)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					cnt := 0
					for resObj := range retChan {
						gm.Expect(resObj.PersistAsInner1).To(gm.BeNumerically(">=", 21))
						gm.Expect(resObj.PersistAsInner1).To(gm.BeNumerically("<=", 70))
						gm.Expect(resObj.PersistNot).To(gm.Equal(0))
						gm.Expect(resObj.Gen).To(gm.BeNumerically(">", 0))
						gm.Expect(resObj.TTL).To(gm.BeNumerically("<=", 500))

						testObj.PersistAsInner1 = resObj.PersistAsInner1
						testObj.Gen = resObj.Gen
						testObj.TTL = resObj.TTL
						gm.Expect(resObj).To(gm.Equal(testObj))
						cnt++
					}

					for e := range rs.Errors {
						gm.Expect(e).ToNot(gm.HaveOccurred())
					}

					gm.Expect(cnt).To(gm.Equal(50))
				})

				gg.It(fmt.Sprintf("must query only relevant objects, and close and return. FailOnClusterChange: %v", failOnClusterChange), func() {

					// first create an index
					idxTask, err := client.CreateIndex(nil, ns, set, set+"inner1", "inner1", as.NUMERIC)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					defer client.DropIndex(nil, ns, set, set+"inner1")

					// wait until index is created
					<-idxTask.OnComplete()

					testObj := &InnerStruct{}

					retChan := make(chan *InnerStruct, 1)
					stmt := as.NewStatement(ns, set)
					stmt.SetFilter(as.NewRangeFilter("inner1", 21, 70))

					qpolicy := as.NewQueryPolicy()
					qpolicy.RecordQueueSize = 1

					rs, err := client.QueryObjects(queryPolicy, stmt, retChan)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					cnt := 0
					for resObj := range retChan {
						gm.Expect(resObj.PersistAsInner1).To(gm.BeNumerically(">=", 21))
						gm.Expect(resObj.PersistAsInner1).To(gm.BeNumerically("<=", 70))
						gm.Expect(resObj.PersistNot).To(gm.Equal(0))
						gm.Expect(resObj.Gen).To(gm.BeNumerically(">", 0))
						gm.Expect(resObj.TTL).To(gm.BeNumerically("<=", 500))

						testObj.PersistAsInner1 = resObj.PersistAsInner1
						testObj.Gen = resObj.Gen
						testObj.TTL = resObj.TTL
						gm.Expect(resObj).To(gm.Equal(testObj))
						cnt++

						if cnt >= 10 {
							rs.Close()
							gm.Eventually(rs.Errors).Should(gm.BeClosed())
						}
					}

					for e := range rs.Errors {
						gm.Expect(e).ToNot(gm.HaveOccurred())
					}

					gm.Expect(cnt).To(gm.BeNumerically("<=", 11))
				})

			}) // QueryObject context
		}
	})
})
