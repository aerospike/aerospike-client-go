// Copyright 2014-2026 Aerospike, Inc.
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

// The field-index paths stored in the object-mapping cache must each own their
// backing array. fillMapping used to append onto the shared parent index
// slice, so once the slice carried spare capacity -- which append's capacity
// rounding produces at embedding depth >= 3 -- sibling fields overwrote each
// other's stored paths: both bin names ended up mapped to the same field, and
// PutObject/GetObject silently used the wrong field.
var _ = gg.Describe("object mapping field-index paths", func() {

	gg.It("must store distinct paths for sibling fields under deep embedding", func() {
		type L3 struct {
			F0 int
			F1 int
			F2 int
		}
		type L2 struct{ L3 }
		type L1 struct{ L2 }
		type L0 struct{ L1 }

		typ := reflect.TypeOf(L0{})
		mapping := objectMappings.getMapping(typ)

		gm.Expect(mapping["F0"]).To(gm.Equal([]int{0, 0, 0, 0}))
		gm.Expect(mapping["F1"]).To(gm.Equal([]int{0, 0, 0, 1}))
		gm.Expect(mapping["F2"]).To(gm.Equal([]int{0, 0, 0, 2}))
	})

	gg.It("must resolve every mapped path to the declared field", func() {
		type Inner struct {
			A string
			B string
		}
		type Mid struct{ Inner }
		type Outer struct {
			Mid
			C string
		}

		typ := reflect.TypeOf(Outer{})
		mapping := objectMappings.getMapping(typ)

		v := reflect.ValueOf(Outer{Mid: Mid{Inner: Inner{A: "a", B: "b"}}, C: "c"})
		for name, idx := range mapping {
			got := v.FieldByIndex(idx).Interface()
			gm.Expect(got).To(gm.BeEquivalentTo(map[string]string{
				"A": "a", "B": "b", "C": "c",
			}[name]), "field %q resolved through its cached path", name)
		}
	})
})
