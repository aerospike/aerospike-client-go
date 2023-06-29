//go:build !as_performance
// +build !as_performance

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
	"reflect"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Read Command Reflect setValue", func() {
	type testStruct struct {
		Int                   int
		Bool                  bool
		Int64                 int64
		String                string
		Float64               float64
		SliceString           []string
		SliceInt              []int
		SliceFloat64          []float64
		MapStringFloat64      map[string]float64
		MapStringString       map[string]string
		MapInterfaceInterface map[interface{}]interface{}
	}

	ts := &testStruct{}
	reflectField := func(name string) reflect.Value {
		return reflect.Indirect(reflect.ValueOf(ts)).FieldByName(name)
	}

	tests := []struct {
		name  string
		field reflect.Value
		obj   interface{}
		error bool
	}{
		{name: "int->int", field: reflectField("Int"), obj: 5},
		{name: "int->int64", field: reflectField("Int64"), obj: int64(5)},
		{name: "int->bool", field: reflectField("Bool"), obj: true},
		{name: "int->float64", field: reflectField("Float64"), obj: 5},
		{name: "[]string->[]string", field: reflectField("SliceString"), obj: []string{"1", "2"}},
		{name: "[]int->[]int", field: reflectField("SliceInt"), obj: []int{1, 2}},
		{name: "map[string]string->map[string]string", field: reflectField("MapStringString"), obj: map[interface{}]interface{}{"1": "2"}},
		{name: "map[string]float64->map[string]float64", field: reflectField("MapStringFloat64"), obj: map[interface{}]interface{}{"1": 2}},
		{name: "map[interface{}]interface{}->map[interface{}]interface{}", field: reflectField("MapInterfaceInterface"), obj: map[interface{}]interface{}{"1": 2}},

		{name: "string->int", field: reflectField("Int"), obj: "5", error: true},
		{name: "string->bool", field: reflectField("Bool"), obj: "true", error: true},
		{name: "int->string", field: reflectField("String"), obj: 5, error: true},
		{name: "bool->int", field: reflectField("Int"), obj: true, error: true},
		{name: "bool->string", field: reflectField("String"), obj: true, error: true},
		{name: "int->[]string", field: reflectField("SliceString"), obj: 5, error: true},
		{name: "int->[]int", field: reflectField("SliceInt"), obj: 5, error: true},
		{name: "int->[]float64", field: reflectField("SliceFloat64"), obj: 5, error: true},
		{name: "[]string->int", field: reflectField("Int"), obj: []string{"1", "2"}, error: true},
		{name: "[]string->int64", field: reflectField("Int64"), obj: []string{"1", "2"}, error: true},
		{name: "[]string->float64", field: reflectField("Float64"), obj: []string{"1", "2"}, error: true},
		{name: "[]int->int", field: reflectField("Int"), obj: []int{1, 2}, error: true},
		{name: "[]int->int64", field: reflectField("Int64"), obj: []int{1, 2}, error: true},
		{name: "[]string->[]int", field: reflectField("SliceInt"), obj: []string{"1", "2"}, error: true},
		{name: "map[string]string->[]int", field: reflectField("SliceInt"), obj: map[interface{}]interface{}{"1": "2"}, error: true},
		{name: "[]int->map[string]string", field: reflectField("MapStringString"), obj: []int{1, 2}, error: true},
		{name: "map[string]string->map[string]float64", field: reflectField("MapStringFloat64"), obj: map[interface{}]interface{}{"1": "2"}, error: true},
	}

	for _, tt := range tests {
		tc := tt
		gg.Context(tc.name, func() {
			gg.It("Should return correct error", func() {
				gm.Expect(func() {
					err := setValue(tc.field, tc.obj)
					if tc.error {
						gm.Expect(err).To(gm.HaveOccurred())
						return
					}
					gm.Expect(err).ToNot(gm.HaveOccurred())
				}).To(gm.Not(gm.Panic()))
			})
		})
	}
})
