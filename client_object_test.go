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

package aerospike_test

import (
	"math"
	"time"

	. "github.com/aerospike/aerospike-client-go"
	// . "github.com/aerospike/aerospike-client-go/utils/buffer"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Aerospike", func() {
	initTestVars()

	Describe("Data operations on objects", func() {
		// connection data
		var err error
		var ns = "test"
		var set = randString(50)
		var key *Key
		var client *Client

		BeforeEach(func() {
			// use the same client for all
			client, err = NewClientWithPolicy(clientPolicy, *host, *port)
			Expect(err).ToNot(HaveOccurred())

			key, err = NewKey(ns, set, randString(50))
			Expect(err).ToNot(HaveOccurred())
		})

		// type SomeBool bool TODO: FIXIT
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

			Array [3]interface{}
			Slice []interface{}
			Map   map[interface{}]interface{}

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

			now := time.Now()

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

				Array: [3]interface{}{1, "string", nil},
				Slice: []interface{}{1, "string", []byte{1, 11, 111}, nil, true},
				Map:   map[interface{}]interface{}{1: "string", "string": nil, nil: map[interface{}]interface{}{"1": ip}, true: false},

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

				NestedObjSlice: []SomeStruct{SomeStruct{A: 1, Self: &SomeStruct{A: 999}}, SomeStruct{A: 2, Self: &SomeStruct{A: 998}}},
				NstdObjPSlice:  []*testObject{&testObject{Int: 1, Intp: &ip, Tm: now}, &testObject{Int: 2, Intp: &ip, Tm: now}},

				Tm:  now,
				TmP: &now,
			}
		}

		Context("PutObject operations", func() {

			It("must save an object with the most complex structure possible", func() {

				testObj := makeTestObject()
				err := client.PutObject(nil, key, &testObj)
				Expect(err).ToNot(HaveOccurred())

				resObj := &testObject{}
				err = client.GetObject(nil, key, resObj)
				Expect(err).ToNot(HaveOccurred())
				Expect(resObj).To(Equal(testObj))

			})

			It("must save an object and read it back respecting the tags", func() {

				type InnerStruct struct {
					PersistNot      int `as:"-"`
					PersistAsInner1 int `as:"inner1"`
				}

				type TaggedStruct struct {
					DontPersist   int `as:"-"`
					PersistAsFld1 int `as:"fld1"`

					IStruct InnerStruct
				}

				testObj := TaggedStruct{DontPersist: 1, PersistAsFld1: 2, IStruct: InnerStruct{PersistNot: 10, PersistAsInner1: 11}}
				err := client.PutObject(nil, key, &testObj)
				Expect(err).ToNot(HaveOccurred())

				resObj := &TaggedStruct{}
				err = client.GetObject(nil, key, resObj)
				Expect(err).ToNot(HaveOccurred())

				Expect(resObj.DontPersist).To(Equal(0))
				Expect(resObj.PersistAsFld1).To(Equal(2))
				Expect(resObj.IStruct.PersistNot).To(Equal(0))
				Expect(resObj.IStruct.PersistAsInner1).To(Equal(11))

				// get the bins and check for bin names
				rec, err := client.Get(nil, key)
				Expect(err).ToNot(HaveOccurred())

				Expect(len(rec.Bins)).To(Equal(2))
				Expect(rec.Bins["DontPersist"]).To(BeNil())
				Expect(rec.Bins["fld1"]).To(Equal(2))
				innerStruct := rec.Bins["IStruct"].(map[interface{}]interface{})
				Expect(len(innerStruct)).To(Equal(1))
				Expect(innerStruct["PersistNot"]).To(BeNil())
				Expect(innerStruct["inner1"]).To(Equal(11))

			})

		}) // PutObject context

		Context("ScanObjects operations", func() {

			type InnerStruct struct {
				PersistNot      int `as:"-"`
				PersistAsInner1 int `as:"inner1"`
			}

			BeforeEach(func() {
				// use the same client for all
				client, err = NewClientWithPolicy(clientPolicy, *host, *port)
				Expect(err).ToNot(HaveOccurred())

				set = randString(50)

				for i := 1; i < 100; i++ {
					key, err = NewKey(ns, set, randString(50))
					Expect(err).ToNot(HaveOccurred())

					testObj := InnerStruct{PersistAsInner1: i}
					err := client.PutObject(nil, key, &testObj)
					Expect(err).ToNot(HaveOccurred())
				}

			})

			It("must scan all objects with the most complex structure possible", func() {

				testObj := &InnerStruct{}

				retChan := make(chan *InnerStruct, 10)

				_, err := client.ScanAllObjects(nil, retChan, ns, set)
				Expect(err).ToNot(HaveOccurred())

				cnt := 0
				for resObj := range retChan {
					Expect(resObj.PersistAsInner1).To(BeNumerically(">", 0))
					Expect(resObj.PersistNot).To(Equal(0))

					testObj.PersistAsInner1 = resObj.PersistAsInner1
					Expect(resObj).To(Equal(testObj))
					cnt++
				}

				Expect(cnt).To(Equal(99))
			})

		}) // ScanObjects context

		Context("QueryObjects operations", func() {

			type InnerStruct struct {
				PersistNot      int `as:"-"`
				PersistAsInner1 int `as:"inner1"`
			}

			BeforeEach(func() {
				// use the same client for all
				client, err = NewClientWithPolicy(clientPolicy, *host, *port)
				Expect(err).ToNot(HaveOccurred())

				set = randString(50)

				for i := 1; i < 100; i++ {
					key, err = NewKey(ns, set, randString(50))
					Expect(err).ToNot(HaveOccurred())

					testObj := InnerStruct{PersistAsInner1: i}
					err := client.PutObject(nil, key, &testObj)
					Expect(err).ToNot(HaveOccurred())
				}

			})

			It("must scan all objects with the most complex structure possible", func() {

				testObj := &InnerStruct{}

				retChan := make(chan *InnerStruct, 10)
				stmt := NewStatement(ns, set)

				_, err := client.QueryObjects(nil, stmt, retChan)
				Expect(err).ToNot(HaveOccurred())

				cnt := 0
				for resObj := range retChan {
					Expect(resObj.PersistAsInner1).To(BeNumerically(">", 0))
					Expect(resObj.PersistNot).To(Equal(0))

					testObj.PersistAsInner1 = resObj.PersistAsInner1
					Expect(resObj).To(Equal(testObj))
					cnt++
				}

				Expect(cnt).To(Equal(99))
			})

			It("must query only relevent objects with the most complex structure possible", func() {

				// first create an index
				idxTask, err := client.CreateIndex(nil, ns, set, set+"inner1", "inner1", NUMERIC)
				Expect(err).ToNot(HaveOccurred())
				defer client.DropIndex(nil, ns, set, set+"inner1")

				// wait until index is created
				<-idxTask.OnComplete()

				testObj := &InnerStruct{}

				retChan := make(chan *InnerStruct, 10)
				stmt := NewStatement(ns, set)
				stmt.Addfilter(NewRangeFilter("inner1", 21, 70))

				rs, err := client.QueryObjects(nil, stmt, retChan)
				Expect(err).ToNot(HaveOccurred())

				cnt := 0
				for resObj := range retChan {
					Expect(resObj.PersistAsInner1).To(BeNumerically(">=", 21))
					Expect(resObj.PersistAsInner1).To(BeNumerically("<=", 70))
					Expect(resObj.PersistNot).To(Equal(0))

					testObj.PersistAsInner1 = resObj.PersistAsInner1
					Expect(resObj).To(Equal(testObj))
					cnt++
				}

				for e := range rs.Errors {
					panic(e.Error())
					Expect(e).ToNot(HaveOccurred())
				}

				Expect(cnt).To(Equal(50))
			})

			It("must query only relevent objects, and close and return", func() {

				// first create an index
				idxTask, err := client.CreateIndex(nil, ns, set, set+"inner1", "inner1", NUMERIC)
				Expect(err).ToNot(HaveOccurred())
				defer client.DropIndex(nil, ns, set, set+"inner1")

				// wait until index is created
				<-idxTask.OnComplete()

				testObj := &InnerStruct{}

				retChan := make(chan *InnerStruct, 10)
				stmt := NewStatement(ns, set)
				stmt.Addfilter(NewRangeFilter("inner1", 21, 70))

				rs, err := client.QueryObjects(nil, stmt, retChan)
				Expect(err).ToNot(HaveOccurred())

				cnt := 0
				for resObj := range retChan {
					Expect(resObj.PersistAsInner1).To(BeNumerically(">=", 21))
					Expect(resObj.PersistAsInner1).To(BeNumerically("<=", 70))
					Expect(resObj.PersistNot).To(Equal(0))

					testObj.PersistAsInner1 = resObj.PersistAsInner1
					Expect(resObj).To(Equal(testObj))
					cnt++

					if cnt >= 10 {
						rs.Close()
					}
				}

				for e := range rs.Errors {
					panic(e.Error())
					Expect(e).ToNot(HaveOccurred())
				}

				Expect(cnt).To(BeNumerically("<", 20))
			})

		}) // QueryObject context

	})
})
