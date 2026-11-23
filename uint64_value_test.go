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

var _ = gg.Describe("uint64 values", gg.Ordered, func() {

	var ns = *namespace
	var set = randString(50)
	var key *as.Key

	seed := func() {
		gm.Expect(client.Put(nil, key, as.BinMap{
			"l": []int{1, 2, 3},
			"m": map[any]any{1: 1, 2: 2},
			"n": 1,
		})).ToNot(gm.HaveOccurred())
	}

	operate := func(ops ...*as.Operation) as.Error {
		_, err := client.Operate(nil, key, ops...)
		return err
	}

	query := func(filter *as.Filter) as.Error {
		stm := as.NewStatement(ns, set)
		gm.Expect(stm.SetFilter(filter)).ToNot(gm.HaveOccurred())
		rs, err := client.Query(nil, stm)
		if err != nil {
			return err
		}
		for res := range rs.Results() {
			if res.Err != nil {
				return res.Err
			}
		}
		return nil
	}

	// Every entry point that converts its argument with NewValue.
	entryPoints := map[string]func(v uint64) as.Error{
		"Put":     func(v uint64) as.Error { return client.Put(nil, key, as.BinMap{"v": v}) },
		"PutBins": func(v uint64) as.Error { return client.PutBins(nil, key, as.NewBin("v", v)) },
		"NewKey": func(v uint64) as.Error {
			_, err := as.NewKey(ns, set, v)
			return err
		},
		"NewKeyWithDigest": func(v uint64) as.Error {
			_, err := as.NewKeyWithDigest(ns, set, v, key.Digest())
			return err
		},
		"AddOp":           func(v uint64) as.Error { return operate(as.AddOp(as.NewBin("n", v))) },
		"Equal filter":    func(v uint64) as.Error { return query(as.NewEqualFilter("n", v)) },
		"ListAppendOp":    func(v uint64) as.Error { return operate(as.ListAppendOp("l", v)) },
		"ListInsertOp":    func(v uint64) as.Error { return operate(as.ListInsertOp("l", 0, v)) },
		"ListSetOp":       func(v uint64) as.Error { return operate(as.ListSetOp("l", 0, v)) },
		"ListIncrementOp": func(v uint64) as.Error { return operate(as.ListIncrementOp("l", 0, v)) },
		"ListIncrementWithPolicyOp": func(v uint64) as.Error {
			return operate(as.ListIncrementWithPolicyOp(as.DefaultListPolicy(), "l", 0, v))
		},
		"ListRemoveByValueOp": func(v uint64) as.Error { return operate(as.ListRemoveByValueOp("l", v, as.ListReturnTypeNone)) },
		"ListRemoveByValueRangeOp": func(v uint64) as.Error {
			return operate(as.ListRemoveByValueRangeOp("l", as.ListReturnTypeNone, v, nil))
		},
		"ListGetByValueOp":      func(v uint64) as.Error { return operate(as.ListGetByValueOp("l", v, as.ListReturnTypeCount)) },
		"ListGetByValueRangeOp": func(v uint64) as.Error { return operate(as.ListGetByValueRangeOp("l", 0, v, as.ListReturnTypeCount)) },
		"ListGetByValueRelativeRankRangeOp": func(v uint64) as.Error {
			return operate(as.ListGetByValueRelativeRankRangeOp("l", v, 0, as.ListReturnTypeCount))
		},
		"MapRemoveByKeyRelativeIndexRangeOp": func(v uint64) as.Error {
			return operate(as.MapRemoveByKeyRelativeIndexRangeOp("m", v, 0, as.MapReturnType.NONE))
		},
		"MapGetByValueRelativeRankRangeOp": func(v uint64) as.Error {
			return operate(as.MapGetByValueRelativeRankRangeOp("m", v, 0, as.MapReturnType.COUNT))
		},
		"CtxMapKey": func(v uint64) as.Error {
			return operate(as.MapSizeOp("m", as.CtxMapKey(as.NewValue(v))))
		},
		"BatchOperate": func(v uint64) as.Error {
			return client.BatchOperate(nil, []as.BatchRecordIfc{as.NewBatchWrite(nil, key, as.PutOp(as.NewBin("v", v)))})
		},
		"Execute args": func(v uint64) as.Error {
			_, err := client.Execute(nil, key, "no_such_module", "fn", as.NewValue(v))
			return err
		},
		"Expression value": func(v uint64) as.Error {
			policy := as.NewPolicy()
			policy.FilterExpression = as.ExpEq(as.ExpListBin("l"), as.ExpListVal(as.NewValue(v)))
			_, err := client.Get(policy, key)
			if err != nil && err.Matches(ast.FILTERED_OUT) {
				return nil
			}
			return err
		},
	}

	gg.BeforeEach(func() {
		var err as.Error
		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		seed()
	})

	gg.BeforeAll(func() {
		createIndex(nil, ns, set, set+"_n", "n", as.NUMERIC)
	})

	gg.AfterAll(func() {
		dropIndex(nil, ns, set, set+"_n")
	})

	gg.It("are stored like int64 when they fit in int64", func() {
		for _, v := range []uint64{0, 5, math.MaxInt64} {
			gm.Expect(client.Put(nil, key, as.BinMap{"v": v})).ToNot(gm.HaveOccurred())
			rec, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(rec.Bins["v"]).To(gm.Equal(int(v)))

			uk, err := as.NewKey(ns, set, v)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			ik, err := as.NewKey(ns, set, int64(v))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(uk.Digest()).To(gm.Equal(ik.Digest()))
		}
	})

	gg.It("are accepted by every entry point when they fit in int64", func() {
		for name, fn := range entryPoints {
			seed()
			err := fn(5)
			if err != nil && (err.Matches(ast.UDF_BAD_RESPONSE) || err.Matches(ast.OP_NOT_APPLICABLE)) {
				err = nil // the value was encoded and reached the server
			}
			gm.Expect(err).ToNot(gm.HaveOccurred(), name)
		}
	})

	gg.It("return a parameter error, not a panic, from every entry point when they overflow int64", func() {
		for _, v := range []uint64{1 << 63, math.MaxUint64} {
			for name, fn := range entryPoints {
				seed()
				var err as.Error
				gm.Expect(func() { err = fn(v) }).ToNot(gm.Panic(), name)
				gm.Expect(err).To(gm.HaveOccurred(), name)
				gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue(), name)
			}
		}
	})

	gg.It("leave the record untouched when they overflow int64", func() {
		err := operate(as.PutOp(as.NewBin("x", 1)), as.ListRemoveByValueRangeOp("l", as.ListReturnTypeNone, uint64(1<<63), nil))
		gm.Expect(err).To(gm.HaveOccurred())

		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins).ToNot(gm.HaveKey("x"))
		gm.Expect(rec.Bins["l"]).To(gm.Equal([]any{1, 2, 3}))
	})

	gg.It("keep the existing encoding inside slices", func() {
		gm.Expect(client.Put(nil, key, as.BinMap{"l": []uint64{1, 1 << 63}})).ToNot(gm.HaveOccurred())
		rec, err := client.Get(nil, key)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(rec.Bins["l"]).To(gm.Equal([]any{1, math.MinInt64}))
	})
})
