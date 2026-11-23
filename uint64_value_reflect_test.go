//go:build !as_performance

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
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"
	ast "github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

type uint64Hash uint64

// Named uint64 types go through reflection, which as_performance turns off.
var _ = gg.Describe("Named uint64 values", func() {

	var ns = *namespace
	var set = randString(50)
	var key *as.Key

	gg.BeforeEach(func() {
		var err as.Error
		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.It("are stored like int64 when they fit in int64", func() {
		for _, v := range []uint64{0, 5, math.MaxInt64} {
			gm.Expect(client.Put(nil, key, as.BinMap{"h": uint64Hash(v)})).ToNot(gm.HaveOccurred())
			rec, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(rec.Bins["h"]).To(gm.Equal(int(v)))
		}
	})

	gg.It("return a parameter error, not a panic, when they overflow int64", func() {
		for _, v := range []uint64{1 << 63, math.MaxUint64} {
			var err as.Error
			gm.Expect(func() { err = client.Put(nil, key, as.BinMap{"h": uint64Hash(v)}) }).ToNot(gm.Panic())
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue())
		}
	})
})
