//go:build !as_performance && !app_engine

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

package aerospike_test

import (
	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Vector has only unexported fields, so without special handling the object
// mapper would store it as an empty map. These specs pin the native round trip.
var _ = gg.Describe("Vector Object API Test", func() {
	type embedding struct {
		Ptr   *as.Vector `as:"ptr"`
		Value as.Vector  `as:"val"`
		Name  string     `as:"name"`
	}

	var key *as.Key

	gg.BeforeEach(func() {
		if !serverSupportsVectors() {
			gg.Skip("native vector bins require a server with vector support")
		}

		var err as.Error
		key, err = as.NewKey(*namespace, randString(50), randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.AfterEach(func() {
		client.Delete(nil, key)
	})

	gg.It("stores Vector and *Vector struct fields as native vector bins", func() {
		stored := embedding{
			Ptr:   mustVector(as.NewVectorFloat32([]float32{1, 2})),
			Value: *mustVector(as.NewVectorInt32([]int32{7})),
			Name:  "doc",
		}
		gm.Expect(client.PutObject(nil, key, &stored)).To(gm.Succeed())

		// Read back through the plain API: both are real vector particles.
		record, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(expectVector(record.Bins["ptr"]).Equal(stored.Ptr)).To(gm.BeTrue())
		gm.Expect(expectVector(record.Bins["val"]).Equal(&stored.Value)).To(gm.BeTrue())

		var loaded embedding
		gm.Expect(client.GetObject(nil, key, &loaded)).To(gm.Succeed())
		gm.Expect(loaded.Ptr.Equal(stored.Ptr)).To(gm.BeTrue())
		gm.Expect(loaded.Value.Equal(&stored.Value)).To(gm.BeTrue())
		gm.Expect(loaded.Name).To(gm.Equal("doc"))
	})

	gg.It("stores vectors inside slices, maps and nested structs", func() {
		type chunk struct {
			Text      string     `as:"text"`
			Embedding *as.Vector `as:"emb"`
		}
		type document struct {
			Chunks []chunk               `as:"chunks"`
			ByName map[string]*as.Vector `as:"by_name"`
		}
		first := mustVector(as.NewVectorFloat32([]float32{1, 2}))
		second := mustVector(as.NewVectorInt32([]int32{3}))
		stored := document{
			Chunks: []chunk{{Text: "intro", Embedding: first}, {Text: "body", Embedding: second}},
			ByName: map[string]*as.Vector{"title": first},
		}
		gm.Expect(client.PutObject(nil, key, &stored)).To(gm.Succeed())

		var loaded document
		gm.Expect(client.GetObject(nil, key, &loaded)).To(gm.Succeed())
		gm.Expect(loaded.Chunks).To(gm.HaveLen(2))
		gm.Expect(loaded.Chunks[0].Text).To(gm.Equal("intro"))
		gm.Expect(loaded.Chunks[0].Embedding.Equal(first)).To(gm.BeTrue())
		gm.Expect(loaded.Chunks[1].Embedding.Equal(second)).To(gm.BeTrue())
		gm.Expect(loaded.ByName["title"].Equal(first)).To(gm.BeTrue())
	})

	gg.It("rejects stored type-16 bytes that are not a vector for a Vector field", func() {
		// The server does not validate vectors nested in a map, so invalid
		// type-16 bytes can reach a Vector field of a nested struct.
		type inner struct {
			Value as.Vector `as:"val"`
		}
		type outer struct {
			Inner inner `as:"inner"`
		}
		invalid := as.NewRawBlobValue(ParticleType.VECTOR, []byte{0xde, 0xad, 0xbe})
		gm.Expect(client.PutBins(nil, key, as.NewBin("inner", map[any]any{"val": invalid}))).To(gm.Succeed())

		var loaded outer
		err := client.GetObject(nil, key, &loaded)
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(ast.PARSE_ERROR)).To(gm.BeTrue(), err.Error())
		gm.Expect(err.Error()).To(gm.ContainSubstring("for Vector field"))
	})

	gg.It("omits unset Vector and *Vector fields tagged omitempty", func() {
		// A zero Vector has no serialized bytes; sending it would make the
		// server reject the whole record.
		type draft struct {
			Ptr   *as.Vector `as:"ptr,omitempty"`
			Value as.Vector  `as:"val,omitempty"`
			Name  string     `as:"name"`
		}
		gm.Expect(client.PutObject(nil, key, &draft{Name: "doc"})).To(gm.Succeed())

		record, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(record.Bins).To(gm.Equal(as.BinMap{"name": "doc"}))
	})
})
