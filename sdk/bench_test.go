//go:build go1.27

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

// Benchmarks comparing the core client's API with this SDK's, each as a pair of
// sub-benchmarks named `core` and `sdk` so they can be read side by side:
//
//	go1.27rc2 test ./sdk/ -run '^$' -bench . -benchmem \
//	    -args -h 127.0.0.1 -p 3000 -n test -use-services-alternate
//
// Two groups, because they answer different questions.
//
// `Build*` benchmarks construct commands without sending them. No network is
// involved, so they isolate what the SDK's abstraction actually costs per
// operation: the fluent chain, policy resolution, the target unions.
//
// Every other benchmark issues a real command. Those are dominated by the
// round trip -- a local server still costs tens of microseconds against
// hundreds of nanoseconds of client work -- so read their ns/op as "the SDK
// does not add a round trip" and read the allocation columns for the real
// comparison. Drawing conclusions about CPU from the end-to-end numbers is a
// mistake; that is what the Build* group is for.
//
// Both sides share one connection pool, obtained through
// Client.UnderlyingClient, so pool warmup and cluster tend cannot skew the
// comparison. Core policies are set to match what the SDK's DEFAULT behavior
// resolves to -- notably SendKey, which DEFAULT enables -- so the two sides put
// the same bytes on the wire.
package sdk_test

import (
	"fmt"
	"sync"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

var (
	benchOnce    sync.Once
	benchCluster *sdk.Cluster
	benchSession *sdk.Session
	benchCore    *as.Client
	benchDS      *sdk.DataSet
	benchErr     error

	// Core policies matched to what the SDK's DEFAULT behavior resolves to.
	benchWritePolicy *as.WritePolicy
	benchReadPolicy  *as.BasePolicy
	benchBatchPolicy *as.BatchPolicy
	benchQueryPolicy *as.QueryPolicy

	// Seeded keys, shared by the read-side benchmarks.
	benchKeys []*as.Key
)

const benchSeeded = 100

const benchUDFModule = "sdk_bench_mod"

const benchUDFBody = `
function echo(rec, value)
    return value
end
`

// benchSetup connects once for the whole benchmark binary. The ginkgo suite's
// BeforeSuite does not run under -bench, so this cannot reuse testCluster.
func benchSetup(b *testing.B) {
	b.Helper()
	benchOnce.Do(func() {
		def := sdk.NewClusterDefinition(*host, *port)
		if *servicesAlternate {
			def = def.UsingServicesAlternate()
		}
		benchCluster, benchErr = def.Connect()
		if benchErr != nil {
			return
		}
		benchSession, benchErr = benchCluster.CreateSession(nil)
		if benchErr != nil {
			return
		}
		benchCore, benchErr = benchSession.Client().UnderlyingClient()
		if benchErr != nil {
			return
		}
		benchDS, benchErr = sdk.DataSetOf(*namespace, "sdk_bench")
		if benchErr != nil {
			return
		}
		if benchErr = benchSession.Truncate(benchDS, 0); benchErr != nil {
			return
		}

		// SendKey mirrors the SDK's DEFAULT behavior, so both sides send the
		// same record.
		benchWritePolicy = as.NewWritePolicy(0, 0)
		benchWritePolicy.SendKey = true
		benchReadPolicy = as.NewPolicy()
		benchReadPolicy.SendKey = true
		benchBatchPolicy = as.NewBatchPolicy()
		benchBatchPolicy.SendKey = true
		benchQueryPolicy = as.NewQueryPolicy()

		// A module for the UDF benchmark.
		task, rerr := benchSession.RegisterUDF([]byte(benchUDFBody), benchUDFModule+".lua", as.LUA)
		if rerr != nil {
			benchErr = rerr
			return
		}
		if cerr := <-task.OnComplete(); cerr != nil {
			benchErr = cerr
			return
		}

		// Seed the records the read-side benchmarks seek.
		benchKeys = benchDS.Keys(intRange(0, benchSeeded))
		for i, key := range benchKeys {
			bins := as.BinMap{
				"name":  fmt.Sprintf("user-%d", i),
				"age":   i,
				"score": i * 3,
				"tags":  []any{"a", "b", "c"},
				"prefs": map[any]any{"theme": "dark", "lang": "en"},
				"doc":   map[any]any{"mid": map[any]any{"leaf": i}},
			}
			if aerr := benchCore.Put(benchWritePolicy, key, bins); aerr != nil {
				benchErr = aerr
				return
			}
		}
	})
	if benchErr != nil {
		b.Fatalf("benchmark setup failed (check -h, -p, -use-services-alternate): %v", benchErr)
	}
}

func intRange(from, to int) []int {
	out := make([]int, 0, to-from)
	for i := from; i < to; i++ {
		out = append(out, i)
	}
	return out
}

// benchPair runs two implementations of the same operation under one benchmark.
func benchPair(b *testing.B, core, sdkFn func(i int) error) {
	b.Helper()
	b.Run("core", func(b *testing.B) { benchRun(b, core) })
	b.Run("sdk", func(b *testing.B) { benchRun(b, sdkFn) })
}

func benchRun(b *testing.B, fn func(i int) error) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if err := fn(i); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------------------------------------------------------------------------
// Group 1: construction only. No I/O, so this is the abstraction's real cost.
//
// Two cautions apply to everything in this group.
//
// Results are assigned to `sink`, because a constructed value that is never
// used can be deleted outright by the compiler -- which times an empty loop and
// reports a flattering few nanoseconds at zero allocations.
//
// These benchmarks measure the cost of *expressing* a command, not of preparing
// the same amount of work. Where one side defers materialization to Execute, it
// looks cheaper here and pays the difference in the end-to-end pair; the SDK's
// batch chain does exactly that. Read a Build* result next to its end-to-end
// counterpart, never on its own.
// ---------------------------------------------------------------------------

// sink defeats dead-code elimination.
var sink any

// BenchmarkBuildKey compares minting a key. The SDK's generic method normalizes
// through a type switch and cannot fail; the core takes `any` and returns an
// error.
func BenchmarkBuildKey(b *testing.B) {
	benchSetup(b)
	ns, set := benchDS.Namespace(), benchDS.SetName()
	benchPair(b,
		func(i int) error {
			k, err := as.NewKey(ns, set, i)
			if err != nil {
				return err
			}
			sink = k
			return nil
		},
		func(i int) error {
			sink = benchDS.Key(i)
			return nil
		},
	)
}

// BenchmarkBuildWrite compares assembling a three-bin write: a core operation
// slice against the SDK's fluent chain, stopping short of Execute.
func BenchmarkBuildWrite(b *testing.B) {
	benchSetup(b)
	key := benchDS.Key("build")
	benchPair(b,
		func(i int) error {
			ops := []*as.Operation{
				as.PutOp(as.NewBin("name", "Ada")),
				as.PutOp(as.NewBin("age", i)),
				as.AddOp(as.NewBin("score", 1)),
			}
			sink = ops
			return nil
		},
		func(i int) error {
			sink = benchSession.Upsert(key).
				SetTo("name", "Ada").
				SetTo("age", i).
				Add("score", 1)
			return nil
		},
	)
}

// BenchmarkBuildCDTWithContext compares a nested CDT write: the core threads a
// CDTContext slice through the operation constructor, the SDK expresses the same
// path as chained navigation steps.
func BenchmarkBuildCDTWithContext(b *testing.B) {
	benchSetup(b)
	key := benchDS.Key("build_cdt")
	keyOrdered := as.MapOrder.KEY_ORDERED
	mapPolicy := as.DefaultMapPolicy()
	benchPair(b,
		func(i int) error {
			op := as.MapPutOp(mapPolicy, "doc", "leaf", i,
				as.CtxMapKey(as.NewValue("mid")))
			sink = op
			return nil
		},
		func(i int) error {
			sink = benchSession.Upsert(key).
				Bin("doc").OnMapKey("mid", &keyOrdered).OnMapKey("leaf", &keyOrdered).SetTo(i)
			return nil
		},
	)
}

// BenchmarkBuildExpressionFilter compares attaching a filter expression. Both
// sides build the same *as.Expression, so this measures the cost of getting it
// onto the command -- a policy copy for the core, a builder field for the SDK.
func BenchmarkBuildExpressionFilter(b *testing.B) {
	benchSetup(b)
	expr := as.ExpGreater(as.ExpIntBin("age"), as.ExpIntVal(50))
	benchPair(b,
		func(i int) error {
			p := *benchQueryPolicy
			p.FilterExpression = expr
			stmt := as.NewStatement(benchDS.Namespace(), benchDS.SetName())
			sink = [2]any{&p, stmt}
			return nil
		},
		func(i int) error {
			sink = benchSession.Query(benchDS).Where(expr)
			return nil
		},
	)
}

// BenchmarkBuildBatchWrite compares assembling a 10-key batch write.
func BenchmarkBuildBatchWrite(b *testing.B) {
	benchSetup(b)
	keys := benchKeys[:10]
	benchPair(b,
		func(i int) error {
			recs := make([]as.BatchRecordIfc, 0, len(keys))
			for _, k := range keys {
				recs = append(recs, as.NewBatchWrite(nil, k,
					as.PutOp(as.NewBin("v", i))))
			}
			sink = recs
			return nil
		},
		func(i int) error {
			sink = benchSession.Upsert(keys).SetTo("v", i)
			return nil
		},
	)
}

// ---------------------------------------------------------------------------
// Group 2: end-to-end. Round-trip bound; read the allocation columns.
// ---------------------------------------------------------------------------

// BenchmarkPut compares a single-record write three ways: the core API, the
// SDK's fast path, and the SDK's builder chain.
func BenchmarkPut(b *testing.B) {
	benchSetup(b)
	key := benchDS.Key("put")
	bins := as.BinMap{"name": "Ada", "age": 36}

	b.Run("core", func(b *testing.B) {
		benchRun(b, func(i int) error {
			return errOf(benchCore.Put(benchWritePolicy, key, bins))
		})
	})
	b.Run("sdk_fastpath", func(b *testing.B) {
		benchRun(b, func(i int) error { return benchSession.Put(key, bins) })
	})
	b.Run("sdk_builder", func(b *testing.B) {
		benchRun(b, func(i int) error {
			stream, err := benchSession.Upsert(key).
				SetTo("name", "Ada").SetTo("age", 36).Execute()
			if err != nil {
				return err
			}
			stream.Close()
			return nil
		})
	})
}

// BenchmarkGet compares a single-record read, again across the fast path and
// the builder.
func BenchmarkGet(b *testing.B) {
	benchSetup(b)
	key := benchKeys[7]

	b.Run("core", func(b *testing.B) {
		benchRun(b, func(i int) error {
			_, aerr := benchCore.Get(benchReadPolicy, key)
			return errOf(aerr)
		})
	})
	b.Run("sdk_fastpath", func(b *testing.B) {
		benchRun(b, func(i int) error {
			_, err := benchSession.Get(key, sdk.AllBins)
			return err
		})
	})
	b.Run("sdk_builder", func(b *testing.B) {
		benchRun(b, func(i int) error {
			stream, err := benchSession.Query(key).Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.FirstOrRaise()
			return err
		})
	})
}

// BenchmarkGetProjection compares reading two named bins.
func BenchmarkGetProjection(b *testing.B) {
	benchSetup(b)
	key := benchKeys[7]
	benchPair(b,
		func(i int) error {
			_, aerr := benchCore.Get(benchReadPolicy, key, "name", "age")
			return errOf(aerr)
		},
		func(i int) error {
			_, err := benchSession.Get(key, []string{"name", "age"})
			return err
		},
	)
}

// BenchmarkGetHeader compares a metadata-only read.
func BenchmarkGetHeader(b *testing.B) {
	benchSetup(b)
	key := benchKeys[7]
	benchPair(b,
		func(i int) error {
			_, aerr := benchCore.GetHeader(benchReadPolicy, key)
			return errOf(aerr)
		},
		func(i int) error {
			_, err := benchSession.Get(key, sdk.NoBins)
			return err
		},
	)
}

// BenchmarkExists compares an existence check.
func BenchmarkExists(b *testing.B) {
	benchSetup(b)
	key := benchKeys[7]
	benchPair(b,
		func(i int) error {
			_, aerr := benchCore.Exists(benchReadPolicy, key)
			return errOf(aerr)
		},
		func(i int) error {
			stream, err := benchSession.Exists(key).Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.FirstOrRaise()
			return err
		},
	)
}

// BenchmarkOperate compares a multi-operation single-record command: two writes
// and a read in one round trip.
func BenchmarkOperate(b *testing.B) {
	benchSetup(b)
	key := benchDS.Key("operate")
	benchPair(b,
		func(i int) error {
			_, aerr := benchCore.Operate(benchWritePolicy, key,
				as.PutOp(as.NewBin("name", "Ada")),
				as.AddOp(as.NewBin("counter", 1)),
				as.GetBinOp("counter"),
			)
			return errOf(aerr)
		},
		func(i int) error {
			stream, err := benchSession.Upsert(key).
				SetTo("name", "Ada").
				Add("counter", 1).
				Bin("counter").Get().
				Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.FirstOrRaise()
			return err
		},
	)
}

// BenchmarkCDTMapOperate compares a nested map write plus read-back through a
// context, executed end to end.
func BenchmarkCDTMapOperate(b *testing.B) {
	benchSetup(b)
	key := benchDS.Key("cdt_operate")
	keyOrdered := as.MapOrder.KEY_ORDERED
	mapPolicy := as.DefaultMapPolicy()
	// A *create* context, matching what the SDK's OnMapKey(k, &order) emits.
	// Plain CtxMapKey fails with OP_NOT_APPLICABLE when `doc` has no `mid` yet,
	// and would also put different bytes on the wire than the SDK side.
	ctx := as.CtxMapKeyCreate(as.NewValue("mid"), keyOrdered)

	benchPair(b,
		func(i int) error {
			_, aerr := benchCore.Operate(benchWritePolicy, key,
				as.MapPutOp(mapPolicy, "doc", "leaf", i, ctx),
				as.MapGetByKeyOp("doc", "leaf", as.MapReturnType.VALUE, ctx),
			)
			return errOf(aerr)
		},
		func(i int) error {
			stream, err := benchSession.Upsert(key).
				Bin("doc").OnMapKey("mid", &keyOrdered).OnMapKey("leaf", &keyOrdered).SetTo(i).
				Bin("doc").OnMapKey("mid", &keyOrdered).OnMapKey("leaf", &keyOrdered).GetValues().
				Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.FirstOrRaise()
			return err
		},
	)
}

// BenchmarkCDTListOperate compares writing one list element and reading a range
// back.
//
// This sets an existing index rather than appending: an append would grow the
// record without bound across millions of iterations, so the later iterations
// would be measuring a different, larger record than the earlier ones.
func BenchmarkCDTListOperate(b *testing.B) {
	benchSetup(b)
	key := benchDS.Key("cdt_list")
	if err := benchSession.Put(key, as.BinMap{"tags": []any{"a", "b", "c"}}); err != nil {
		b.Fatal(err)
	}
	benchPair(b,
		func(i int) error {
			_, aerr := benchCore.Operate(benchWritePolicy, key,
				as.ListSetOp("tags", 0, i),
				as.ListGetRangeOp("tags", 0, 3),
			)
			return errOf(aerr)
		},
		func(i int) error {
			stream, err := benchSession.Upsert(key).
				Bin("tags").ListSet(0, i).
				Bin("tags").OnListIndexRange(0, 3).GetValues().
				Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.FirstOrRaise()
			return err
		},
	)
}

// BenchmarkBatchRead compares reading 10 records in one batch.
func BenchmarkBatchRead(b *testing.B) {
	benchSetup(b)
	keys := benchKeys[:10]
	benchPair(b,
		func(i int) error {
			_, aerr := benchCore.BatchGet(benchBatchPolicy, keys)
			return errOf(aerr)
		},
		func(i int) error {
			stream, err := benchSession.Query(keys).Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.Collect()
			return err
		},
	)
}

// BenchmarkBatchWrite compares writing 10 records in one batch.
func BenchmarkBatchWrite(b *testing.B) {
	benchSetup(b)
	keys := benchKeys[:10]
	benchPair(b,
		func(i int) error {
			recs := make([]as.BatchRecordIfc, 0, len(keys))
			for _, k := range keys {
				recs = append(recs, as.NewBatchWrite(nil, k, as.PutOp(as.NewBin("v", i))))
			}
			return errOf(benchCore.BatchOperate(benchBatchPolicy, recs))
		},
		func(i int) error {
			stream, err := benchSession.Upsert(keys).SetTo("v", i).Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.Collect()
			return err
		},
	)
}

// BenchmarkQueryScan compares a full set scan, draining every record.
func BenchmarkQueryScan(b *testing.B) {
	benchSetup(b)
	benchPair(b,
		func(i int) error {
			stmt := as.NewStatement(benchDS.Namespace(), benchDS.SetName())
			rs, aerr := benchCore.Query(benchQueryPolicy, stmt)
			if aerr != nil {
				return errOf(aerr)
			}
			defer rs.Close()
			for res := range rs.Results() {
				if res.Err != nil {
					return res.Err
				}
			}
			return nil
		},
		func(i int) error {
			stream, err := benchSession.Query(benchDS).Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.Collect()
			return err
		},
	)
}

// BenchmarkQueryWithExpression compares a scan filtered by an expression, which
// is where the SDK's Predicate union earns its keep: the same method takes a
// typed expression or AEL source text.
func BenchmarkQueryWithExpression(b *testing.B) {
	benchSetup(b)
	expr := as.ExpGreater(as.ExpIntBin("age"), as.ExpIntVal(50))
	benchPair(b,
		func(i int) error {
			policy := *benchQueryPolicy
			policy.FilterExpression = expr
			stmt := as.NewStatement(benchDS.Namespace(), benchDS.SetName())
			rs, aerr := benchCore.Query(&policy, stmt)
			if aerr != nil {
				return errOf(aerr)
			}
			defer rs.Close()
			for res := range rs.Results() {
				if res.Err != nil {
					return res.Err
				}
			}
			return nil
		},
		func(i int) error {
			stream, err := benchSession.Query(benchDS).Where(expr).Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.Collect()
			return err
		},
	)
}

// BenchmarkWriteWithExpressionGuard compares a conditional write: the core sets
// FilterExpression on a policy copy, the SDK takes the predicate on the chain.
func BenchmarkWriteWithExpressionGuard(b *testing.B) {
	benchSetup(b)
	key := benchKeys[3]
	expr := as.ExpGreaterEq(as.ExpIntBin("age"), as.ExpIntVal(0))
	benchPair(b,
		func(i int) error {
			policy := *benchWritePolicy
			policy.FilterExpression = expr
			_, aerr := benchCore.Operate(&policy, key, as.PutOp(as.NewBin("seen", i)))
			return errOf(aerr)
		},
		func(i int) error {
			stream, err := benchSession.Upsert(key).Where(expr).SetTo("seen", i).Execute()
			if err != nil {
				return err
			}
			stream.Close()
			return nil
		},
	)
}

// BenchmarkExecuteUDF compares a single-key user-defined function call. The SDK
// routes a one-key UDF to the core's point Execute rather than issuing a
// one-row batch UDF, so this pair should sit within noise of each other.
func BenchmarkExecuteUDF(b *testing.B) {
	benchSetup(b)
	key := benchDS.Key("udf_bench")
	arg := as.NewValue(42)
	benchPair(b,
		func(i int) error {
			_, aerr := benchCore.Execute(benchWritePolicy, key, benchUDFModule, "echo", arg)
			return errOf(aerr)
		},
		func(i int) error {
			stream, err := benchSession.ExecuteUDF(key).
				Function(benchUDFModule, "echo").Passing(arg).Execute()
			if err != nil {
				return err
			}
			defer stream.Close()
			_, err = stream.FirstUDFResult()
			return err
		},
	)
}

// errOf adapts the core client's error type, whose nil-ness has to be checked
// on the concrete type rather than through the error interface.
func errOf(aerr as.Error) error {
	if aerr == nil {
		return nil
	}
	return aerr
}
