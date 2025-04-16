// +build !app_engine

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
	"os"

	as "github.com/aerospike/aerospike-client-go"
	"github.com/aerospike/aerospike-client-go/internal/atomic"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

func registerUDF(client *as.Client, path, filename string) error {
	regTask, err := client.RegisterUDFFromFile(nil, path+filename+".lua", filename+".lua", as.LUA)
	if err != nil {
		return err
	}

	// wait until UDF is created
	return <-regTask.OnComplete()
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

	createUDFs := atomic.NewBool(true)

	gg.BeforeEach(func() {
		if createUDFs.Get() {
			err := registerUDF(client, luaPath, "sum_single_bin")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = registerUDF(client, luaPath, "average")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			createUDFs.Set(false)
		}

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
