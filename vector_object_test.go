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

	// A vector can't be an element of a list or a map, so the server rejects
	// a struct that puts one in a slice, a map or a nested struct.
	gg.It("rejects vectors inside slices, maps and nested structs", func() {
		type chunk struct {
			Text      string     `as:"text"`
			Embedding *as.Vector `as:"emb"`
		}
		first := mustVector(as.NewVectorFloat32([]float32{1, 2}))

		policy := as.NewWritePolicy(0, 0)
		policy.ErrorDetailVerbosity = 2
		for _, stored := range []any{
			&struct {
				Chunks []chunk `as:"chunks"`
			}{Chunks: []chunk{{Text: "intro", Embedding: first}}},
			&struct {
				ByName map[string]*as.Vector `as:"by_name"`
			}{ByName: map[string]*as.Vector{"title": first}},
			&struct {
				Inner chunk `as:"inner"`
			}{Inner: chunk{Text: "intro", Embedding: first}},
		} {
			err := client.PutObject(policy, key, stored)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue(), err.Error())
			gm.Expect(err.Error()).To(gm.ContainSubstring("list/map cannot hold a vector"))
		}

		exists, err := client.Exists(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(exists).To(gm.BeFalse())
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
