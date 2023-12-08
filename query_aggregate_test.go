//go:build !app_engine
// +build !app_engine

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

	as "github.com/aerospike/aerospike-client-go/v7"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

func registerUDFFromFile(path, filename string) {
	regTask, err := nativeClient.RegisterUDFFromFile(nil, path+filename+".lua", filename+".lua", as.LUA)
	gm.Expect(err).ToNot(gm.HaveOccurred())

	// wait until UDF is created
	gm.Expect(<-regTask.OnComplete()).ToNot(gm.HaveOccurred())
}

func registerUDF(udf, moduleName string) {
	regTask, err := nativeClient.RegisterUDF(nil, []byte(udf), moduleName, as.LUA)
	gm.Expect(err).ToNot(gm.HaveOccurred())

	// wait until UDF is created
	gm.Expect(<-regTask.OnComplete()).ToNot(gm.HaveOccurred())
}

func removeUDF(moduleName string) {
	remTask, err := nativeClient.RemoveUDF(nil, moduleName)
	gm.Expect(err).ToNot(gm.HaveOccurred())

	// wait until UDF is created
	gm.Expect(<-remTask.OnComplete()).ToNot(gm.HaveOccurred())
}

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
		if *dbaas {
			gg.Skip("Not supported in DBAAS environment")
		}

		if *proxy {
			gg.Skip("Not supported in Proxy Client")
		}

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
			gm.Expect(rec.Record.Bins["SUCCESS"]).To(gm.Equal(map[interface{}]interface{}{"sum": sumAll(keyCount), "count": float64(keyCount)}))
		}
	})
})
