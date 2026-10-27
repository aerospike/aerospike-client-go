//go:build !app_engine

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

package aerospike_test

import (
	"os"
	"sync"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Query Aggregate operations", func() {

	var sumAll = func(upTo int) float64 {
		return float64((1 + upTo) * upTo / 2.0)
	}

	// connection data
	var ns = *namespace
	var set = randString(50)
	var wpolicy = as.NewWritePolicy(0, 0)
	wpolicy.SendKey = true

	// Set LuaPath
	luaPath, _ := os.Getwd()
	luaPath += "/test/resources/"
	as.SetLuaPath(luaPath)

	const keyCount = 1000

	createUDFs := new(sync.Once)

	gg.BeforeEach(func() {
		createUDFs.Do(func() {
			registerUDFFromFile(luaPath, "sum_single_bin")
			registerUDFFromFile(luaPath, "average")
		})

		set = randString(50)
		for i := 1; i <= keyCount; i++ {
			key, err := as.NewKey(ns, set, randString(50))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			bin1 := as.NewBin("bin1", i)
			client.PutBins(nil, key, bin1)
		}

		// // queries only work on indices
		// idxTask, err := client.CreateIndex(wpolicy, ns, set, set+bin3.Name, bin3.Name, NUMERIC)
		// gm.Expect(err).ToNot(gm.HaveOccurred())

		// wait until index is created
		// gm.Expect(<-idxTask.OnComplete()).ToNot(gm.HaveOccurred())
	})

	gg.It("must return the sum of specified bin to the client", func() {
		stm := as.NewStatement(ns, set)
		res, err := client.QueryAggregate(nil, stm, "sum_single_bin", "sum_single_bin", as.StringValue("bin1"))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// gm.Expect(res.TaskId()).To(gm.Equal(stm.TaskId))
		gm.Expect(res.TaskId()).To(gm.BeNumerically(">", 0))

		for rec := range res.Results() {
			gm.Expect(rec.Err).ToNot(gm.HaveOccurred())
			gm.Expect(rec.Record.Bins["SUCCESS"]).To(gm.Equal(sumAll(keyCount)))
		}
	})

	gg.It("must handle running an aggregate query on a set that does not exist without timing out", func() {
		stm := as.NewStatement(ns, set+"_NOT_EXISTS")
		res, err := client.QueryAggregate(nil, stm, "sum_single_bin", "sum_single_bin", as.StringValue("bin1"))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// gm.Expect(res.TaskId()).To(gm.Equal(stm.TaskId))
		gm.Expect(res.TaskId()).To(gm.BeNumerically(">", 0))

		cnt := 0
		for rec := range res.Results() {
			gm.Expect(rec.Err).ToNot(gm.HaveOccurred())
			gm.Expect(rec.Record.Bins["SUCCESS"]).To(gm.Equal(sumAll(keyCount)))
			cnt++
		}
		gm.Expect(cnt).To(gm.Equal(0))
	})

	gg.It("must return Sum and Count to the client", func() {
		stm := as.NewStatement(ns, set)
		res, err := client.QueryAggregate(nil, stm, "average", "average", as.StringValue("bin1"))
		gm.Expect(err).ToNot(gm.HaveOccurred())

		for rec := range res.Results() {
			gm.Expect(rec.Err).ToNot(gm.HaveOccurred())
			gm.Expect(rec.Record.Bins["SUCCESS"]).To(gm.Equal(map[any]any{"sum": sumAll(keyCount), "count": float64(keyCount)}))
		}
	})
})

// Go's client-side Lua has no bytes type, so in a stream aggregation a vector
// reaches it as a list of its serialized bytes.
var _ = gg.Describe("Query Aggregate with vectors", func() {
	const module = "vector_aggregate"
	vector := mustVector(as.NewVectorFloat32([]float32{1, 2}))

	var (
		set string
		key *as.Key
	)

	gg.BeforeEach(func() {
		if !serverSupportsVectors() {
			gg.Skip("native vector bins require a server with vector support")
		}

		luaPath, _ := os.Getwd()
		luaPath += "/test/resources/"
		as.SetLuaPath(luaPath)
		registerUDFFromFile(luaPath, module)

		set = randString(50)
		var err as.Error
		key, err = as.NewKey(*namespace, set, 1)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(client.PutBins(nil, key, as.NewBin("id", 1), as.NewBin("v", vector))).To(gm.Succeed())
	})

	gg.AfterEach(func() {
		client.Delete(nil, key)
		removeUDF(module + ".lua")
	})

	aggregate := func(function string, args ...as.Value) any {
		recordset, err := client.QueryAggregate(nil, as.NewStatement(*namespace, set), module, function, args...)
		gm.ExpectWithOffset(1, err).ToNot(gm.HaveOccurred())

		var results []any
		for result := range recordset.Results() {
			gm.ExpectWithOffset(1, result.Err).ToNot(gm.HaveOccurred())
			results = append(results, result.Record.Bins["SUCCESS"])
		}
		gm.ExpectWithOffset(1, results).To(gm.HaveLen(1))
		return results[0]
	}

	// asLuaList is how client-side Lua returns a byte value: a list of its bytes.
	asLuaList := func(bytes []byte) []any {
		list := make([]any, len(bytes))
		for index, element := range bytes {
			list[index] = element
		}
		return list
	}

	// The vector's serialized bytes: version 1, float32, 2 dimensions, then 1.0 and 2.0.
	wire := []byte{1, 3, 2, 0, 0, 0, 0, 0, 0x80, 0x3f, 0, 0, 0, 0x40}

	gg.It("passes a stored vector to client-side Lua as a list of its bytes", func() {
		gm.Expect(aggregate("stored", as.NewValue("v"))).To(gm.Equal(asLuaList(wire)))
	})

	gg.It("passes a vector argument to client-side Lua as a list of its bytes", func() {
		gm.Expect(aggregate("argument", as.NewVectorValue(vector))).To(gm.Equal(asLuaList(wire)))
	})
})
