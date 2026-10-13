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
	"errors"
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	ast "github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// serverSupportsVectors reports whether the server is new enough for native
// vector bins.
func serverSupportsVectors() bool {
	nodes := client.GetNodes()
	if len(nodes) == 0 {
		return false
	}
	serverVersion := nodes[0].GetServerVersion()
	return !serverVersion.IsSmaller(version.ServerVersionVector)
}

// expectVector fails the spec with the actual value when value is not a
// *as.Vector, instead of panicking on a type assertion.
func expectVector(value any) *as.Vector {
	vector, ok := value.(*as.Vector)
	gm.ExpectWithOffset(1, ok).To(gm.BeTrue(), "expected a *as.Vector, got %#v", value)
	return vector
}

var _ = gg.Describe("Vector Test", func() {
	const binName = "vec"

	var (
		ns  = *namespace
		set string
		key *as.Key
	)

	put := func(bins ...*as.Bin) as.Error {
		return client.PutBins(nil, key, bins...)
	}

	getVector := func(name string) *as.Vector {
		record, err := client.Get(nil, key, name)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return expectVector(record.Bins[name])
	}

	gg.BeforeEach(func() {
		if !serverSupportsVectors() {
			gg.Skip("native vector bins require a server with vector support")
		}

		set = randString(50)
		var err as.Error
		key, err = as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.AfterEach(func() {
		client.Delete(nil, key)
	})

	gg.Context("round trip", func() {
		testCases := []struct {
			name   string
			vector *as.Vector
		}{
			{"float16", as.NewVectorFloat16Bits([]uint16{0x3c00, 0x8000, 0x0001, 0x7bff})},
			{"int32", as.NewVectorInt32([]int32{math.MinInt32, 0, math.MaxInt32})},
			{"float32", as.NewVectorFloat32([]float32{1.5, float32(math.Copysign(0, -1)), math.MaxFloat32})},
			{"float64", as.NewVectorFloat64([]float64{-2.25, math.SmallestNonzeroFloat64, math.MaxFloat64})},
			{"single dimension", as.NewVectorFloat32([]float32{42})},
		}

		for _, testCase := range testCases {
			gg.It("stores and returns a "+testCase.name+" vector unchanged", func() {
				gm.Expect(put(as.NewBin(binName, testCase.vector))).To(gm.Succeed())
				gm.Expect(getVector(binName).Equal(testCase.vector)).To(gm.BeTrue())
			})
		}

		gg.It("accepts the largest vector of each element type", func() {
			// The server caps the element array at 2^18 bytes.
			largestVectors := map[string]*as.Vector{
				"float16": as.NewVectorFloat16Bits(make([]uint16, 131072)),
				"int32":   as.NewVectorInt32(make([]int32, 65536)),
				"float32": as.NewVectorFloat32(make([]float32, 65536)),
				"float64": as.NewVectorFloat64(make([]float64, 32768)),
			}
			for elementType, vector := range largestVectors {
				gm.Expect(put(as.NewBin(binName, vector))).To(gm.Succeed(), elementType)
				gm.Expect(getVector(binName).Dimensions()).To(gm.Equal(vector.Dimensions()), elementType)
			}
		})

		gg.It("replaces a vector with a different vector or a plain value", func() {
			gm.Expect(put(as.NewBin(binName, as.NewVectorFloat32([]float32{1, 2, 3})))).To(gm.Succeed())

			replacement := as.NewVectorFloat64([]float64{9})
			gm.Expect(put(as.NewBin(binName, replacement))).To(gm.Succeed())
			gm.Expect(getVector(binName).Equal(replacement)).To(gm.BeTrue())

			gm.Expect(put(as.NewBin(binName, "text"))).To(gm.Succeed())
			record, err := client.Get(nil, key, binName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("text"))
		})

		gg.It("deletes a vector bin and keeps the other bins", func() {
			gm.Expect(put(as.NewBin(binName, as.NewVectorInt32([]int32{1})), as.NewBin("other", 1))).To(gm.Succeed())
			gm.Expect(put(as.NewBin(binName, nil))).To(gm.Succeed())

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins).To(gm.Equal(as.BinMap{"other": 1}))
		})
	})

	gg.Context("lists and maps", func() {
		embedding := as.NewVectorFloat32([]float32{0.5, -0.5})

		gg.It("keeps vectors nested in list and map bins", func() {
			gm.Expect(put(
				as.NewBin("list", []any{embedding, 1}),
				as.NewBin("map", map[any]any{"emb": embedding}),
			)).To(gm.Succeed())

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(expectVector(record.Bins["list"].([]any)[0]).Equal(embedding)).To(gm.BeTrue())
			gm.Expect(expectVector(record.Bins["map"].(map[any]any)["emb"]).Equal(embedding)).To(gm.BeTrue())
		})

		gg.It("appends a vector to a list and reads it back by index", func() {
			gm.Expect(put(as.NewBin("list", []any{1}))).To(gm.Succeed())

			record, err := client.Operate(nil, key,
				as.ListAppendOp("list", embedding),
				as.ListGetByIndexOp("list", 1, as.ListReturnTypeValue),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			results := record.Bins["list"].(as.OpResults)
			gm.Expect(expectVector(results[1]).Equal(embedding)).To(gm.BeTrue())
		})

		gg.It("puts a vector under a map key and reads it back by key", func() {
			record, err := client.Operate(nil, key,
				as.MapPutOp(as.DefaultMapPolicy(), "map", "emb", embedding),
				as.MapGetByKeyOp("map", "emb", as.MapReturnType.VALUE),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			results := record.Bins["map"].(as.OpResults)
			gm.Expect(expectVector(results[1]).Equal(embedding)).To(gm.BeTrue())
		})

		gg.It("writes a vector into a list nested inside a map", func() {
			gm.Expect(put(as.NewBin("doc", map[any]any{"chunks": []any{}}))).To(gm.Succeed())

			chunks := []*as.CDTContext{as.CtxMapKey(as.NewValue("chunks"))}
			_, err := client.Operate(nil, key, as.ListAppendWithPolicyContextOp(as.DefaultListPolicy(), "doc", chunks, embedding))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			record, err := client.Get(nil, key, "doc")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			stored := record.Bins["doc"].(map[any]any)["chunks"].([]any)[0]
			gm.Expect(expectVector(stored).Equal(embedding)).To(gm.BeTrue())
		})

		gg.It("keeps an empty vector nested in a list or map", func() {
			// Empty vectors are only rejected as a whole bin; nested ones are stored.
			empty := as.NewVectorFloat32(nil)
			gm.Expect(put(as.NewBin("list", []any{empty}), as.NewBin("map", map[any]any{"emb": empty}))).To(gm.Succeed())

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(expectVector(record.Bins["list"].([]any)[0]).Equal(empty)).To(gm.BeTrue())
			gm.Expect(expectVector(record.Bins["map"].(map[any]any)["emb"]).Equal(empty)).To(gm.BeTrue())
		})

		gg.It("returns an invalid nested vector as raw bytes without failing the read", func() {
			// The server does not validate vectors inside a list or map.
			invalid := []byte{0xde, 0xad, 0xbe}
			gm.Expect(put(as.NewBin("list", []any{as.NewRawBlobValue(ParticleType.VECTOR, invalid), "next"}))).To(gm.Succeed())

			record, err := client.Get(nil, key, "list")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			listBin := record.Bins["list"].([]any)
			gm.Expect(listBin[0]).To(gm.Equal(&as.RawBlobValue{ParticleType: ParticleType.VECTOR, Data: invalid}))
			gm.Expect(listBin[1]).To(gm.Equal("next"))
		})
	})

	gg.Context("expressions", func() {
		gg.It("reports particle type 16 through ExpBinType", func() {
			gm.Expect(put(as.NewBin(binName, as.NewVectorFloat32([]float32{1})), as.NewBin("blob", []byte{1}))).To(gm.Succeed())

			isVectorBin := func(name string) bool {
				policy := as.NewPolicy()
				policy.FilterExpression = as.ExpEq(as.ExpBinType(name), as.ExpIntVal(ParticleType.VECTOR))
				_, err := client.Get(policy, key)
				if err != nil {
					gm.Expect(err.Matches(ast.FILTERED_OUT)).To(gm.BeTrue(), err.Error())
					return false
				}
				return true
			}
			gm.Expect(isVectorBin(binName)).To(gm.BeTrue())
			gm.Expect(isVectorBin("blob")).To(gm.BeFalse())
		})

		gg.It("filters a record that holds a vector bin", func() {
			stored := as.NewVectorFloat32([]float32{1, 2})
			gm.Expect(put(as.NewBin(binName, stored), as.NewBin("score", 7))).To(gm.Succeed())

			getWhereScoreIs := func(score int64) as.Error {
				policy := as.NewPolicy()
				policy.FilterExpression = as.ExpAnd(
					as.ExpBinExists(binName),
					as.ExpEq(as.ExpIntBin("score"), as.ExpIntVal(score)),
				)
				record, err := client.Get(policy, key)
				if err == nil {
					gm.Expect(expectVector(record.Bins[binName]).Equal(stored)).To(gm.BeTrue())
				}
				return err
			}
			gm.Expect(getWhereScoreIs(7)).To(gm.Succeed())
			gm.Expect(getWhereScoreIs(8).Matches(ast.FILTERED_OUT)).To(gm.BeTrue())
		})
	})

	// Values from the server's own vector expression tests: a = {1,2,3},
	// b = {4,6,8}.
	gg.Context("distance expressions", func() {
		stored := as.NewVectorFloat32([]float32{1, 2, 3})
		query := as.NewVectorFloat32([]float32{4, 6, 8})

		distanceTo := func(metric as.VectorDistanceMetric, bin string) *as.Expression {
			return as.ExpVectorDistance(metric, query, as.ExpVectorBin(bin))
		}

		// An application that already holds candidate keys ranks them by reading
		// each key's distance in one batch.
		gg.It("returns the distance of each key read in a batch, and none for a key it cannot compare", func() {
			candidates := []struct {
				name       string
				vector     *as.Vector
				distance   float64
				comparable bool
			}{
				{"far", stored, 50, true},
				{"same", query, 0, true},
				{"near", as.NewVectorFloat32([]float32{5, 6, 8}), 1, true},
				{"fewer dims", as.NewVectorFloat32([]float32{1, 2}), 0, false},
			}
			batchKeys := make([]*as.Key, len(candidates))
			for index, candidate := range candidates {
				var err as.Error
				batchKeys[index], err = as.NewKey(ns, set, candidate.name)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(client.PutBins(nil, batchKeys[index], as.NewBin(binName, candidate.vector))).To(gm.Succeed())
			}
			defer func() {
				for _, batchKey := range batchKeys {
					client.Delete(nil, batchKey)
				}
			}()
			distance := func(flags as.ExpReadFlags) *as.Operation {
				return as.ExpReadOp("dist", distanceTo(as.VectorEuclidean, binName), flags)
			}

			// With EvalNoFail a key that cannot be compared just has no distance.
			records, err := client.BatchGetOperate(nil, batchKeys, distance(as.ExpReadFlagEvalNoFail))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for index, candidate := range candidates {
				gm.Expect(records[index]).ToNot(gm.BeNil(), candidate.name)
				if candidate.comparable {
					gm.Expect(records[index].Bins["dist"]).To(gm.Equal(candidate.distance), candidate.name)
				} else {
					gm.Expect(records[index].Bins["dist"]).To(gm.BeNil(), candidate.name)
				}
			}

			// Without EvalNoFail, that one key fails the whole BatchGetOperate.
			_, err = client.BatchGetOperate(nil, batchKeys, distance(as.ExpReadFlagDefault))
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue(), err.Error())

			// Read per record, the key that cannot be compared fails on its own.
			reads := make([]as.BatchRecordIfc, len(batchKeys))
			for index, batchKey := range batchKeys {
				reads[index] = as.NewBatchReadOps(nil, batchKey, distance(as.ExpReadFlagDefault))
			}
			gm.Expect(client.BatchOperate(nil, reads)).To(gm.Succeed())
			for index, candidate := range candidates {
				result := reads[index].BatchRec()
				if candidate.comparable {
					gm.Expect(result.ResultCode).To(gm.Equal(ast.OK), candidate.name)
					gm.Expect(result.Record.Bins["dist"]).To(gm.Equal(candidate.distance), candidate.name)
				} else {
					gm.Expect(result.ResultCode).To(gm.Equal(ast.OP_NOT_APPLICABLE), candidate.name)
				}
			}
		})

		gg.It("returns the squared Euclidean distance, dot product and cosine similarity", func() {
			gm.Expect(put(as.NewBin(binName, stored))).To(gm.Succeed())

			record, err := client.Operate(nil, key,
				as.ExpReadOp("euclidean", distanceTo(as.VectorEuclidean, binName), as.ExpReadFlagDefault),
				as.ExpReadOp("dot", distanceTo(as.VectorDotProduct, binName), as.ExpReadFlagDefault),
				as.ExpReadOp("cosine", distanceTo(as.VectorCosine, binName), as.ExpReadFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["euclidean"]).To(gm.Equal(50.0))
			gm.Expect(record.Bins["dot"]).To(gm.Equal(40.0))
			// The server computes in float32.
			gm.Expect(record.Bins["cosine"]).To(gm.BeNumerically("~", 40/math.Sqrt(1624), 1e-6))
		})

		gg.It("filters records by distance", func() {
			gm.Expect(put(as.NewBin(binName, stored))).To(gm.Succeed())

			getWithinDistance := func(limit float64) as.Error {
				policy := as.NewPolicy()
				policy.FilterExpression = as.ExpLess(distanceTo(as.VectorEuclidean, binName), as.ExpFloatVal(limit))
				_, err := client.Get(policy, key)
				return err
			}
			gm.Expect(getWithinDistance(51)).To(gm.Succeed())
			gm.Expect(getWithinDistance(50).Matches(ast.FILTERED_OUT)).To(gm.BeTrue())
		})

		gg.It("returns nil with EvalNoFail and fails by default when the bin cannot be compared", func() {
			gm.Expect(put(
				as.NewBin("fewer_dims", as.NewVectorFloat32([]float32{1, 2})),
				as.NewBin("float64", as.NewVectorFloat64([]float64{1, 2, 3})),
				as.NewBin("text", "not a vector"),
			)).To(gm.Succeed())

			for _, bin := range []string{"fewer_dims", "float64", "text", "missing"} {
				record, err := client.Operate(nil, key, as.ExpReadOp("d", distanceTo(as.VectorEuclidean, bin), as.ExpReadFlagEvalNoFail))
				gm.Expect(err).ToNot(gm.HaveOccurred(), bin)
				gm.Expect(record.Bins["d"]).To(gm.BeNil(), bin)

				_, err = client.Operate(nil, key, as.ExpReadOp("d", distanceTo(as.VectorEuclidean, bin), as.ExpReadFlagDefault))
				gm.Expect(err).To(gm.HaveOccurred(), bin)
				gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue(), err.Error())
			}
		})

		gg.It("reads and copies a whole vector bin through expressions", func() {
			gm.Expect(put(as.NewBin(binName, stored))).To(gm.Succeed())

			record, err := client.Operate(nil, key,
				as.ExpWriteOp("copy", as.ExpVectorBin(binName), as.ExpWriteFlagDefault),
				as.ExpReadOp("read", as.ExpVectorBin(binName), as.ExpReadFlagDefault),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(expectVector(record.Bins["read"]).Equal(stored)).To(gm.BeTrue())
			gm.Expect(getVector("copy").Equal(stored)).To(gm.BeTrue())
		})
	})

	gg.Context("vectors inside expressions, contexts and UDF arguments", func() {
		embedding := as.NewVectorFloat32([]float32{0.5, -0.5})
		other := as.NewVectorFloat32([]float32{1, 1})

		// holdsEmbedding is true for a map whose values include embedding.
		holdsEmbedding := as.ExpGreater(
			as.ExpMapGetByValueList(as.MapReturnType.COUNT, as.ExpListVal(embedding), as.ExpMapLoopVar(as.VALUE)),
			as.ExpIntVal(0),
		)

		putChunks := func() {
			gm.Expect(put(as.NewBin("chunks", []any{
				map[any]any{"text": "first", "emb": embedding, "hits": 0},
				map[any]any{"text": "second chunk", "emb": other, "hits": 0},
			}))).To(gm.Succeed())
		}

		gg.It("finds a record whose list holds a given vector with a filter expression", func() {
			gm.Expect(put(as.NewBin("embs", []any{embedding, other}))).To(gm.Succeed())

			getWhereListHolds := func(vector *as.Vector) as.Error {
				policy := as.NewPolicy()
				policy.FilterExpression = as.ExpGreater(
					as.ExpListGetByValueList(as.ListReturnTypeCount, as.ExpListVal(vector), as.ExpListBin("embs")),
					as.ExpIntVal(0),
				)
				_, err := client.Get(policy, key)
				return err
			}
			gm.Expect(getWhereListHolds(embedding)).To(gm.Succeed())
			gm.Expect(getWhereListHolds(as.NewVectorFloat32([]float32{9, 9})).Matches(ast.FILTERED_OUT)).To(gm.BeTrue())
		})

		gg.It("counts a vector in a list through an expression read operation", func() {
			gm.Expect(put(as.NewBin("embs", []any{embedding, other, embedding}))).To(gm.Succeed())

			count := as.ExpListGetByValueList(as.ListReturnTypeCount, as.ExpListVal(embedding), as.ExpListBin("embs"))
			record, err := client.Operate(nil, key, as.ExpReadOp("hits", count, as.ExpReadFlagDefault))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["hits"]).To(gm.Equal(2))
		})

		gg.It("selects the chunks that hold a vector through a context filter", func() {
			putChunks()

			record, err := client.Operate(nil, key, as.SelectByPath("chunks", as.EXP_PATH_SELECT_VALUE,
				as.CtxAllChildrenWithFilter(holdsEmbedding), as.CtxMapKey(as.NewValue("text"))))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["chunks"]).To(gm.Equal([]any{"first"}))
		})

		gg.It("updates the chunks that hold a vector through modify by path", func() {
			putChunks()

			increment := as.ExpNumAdd(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(1))
			_, err := client.Operate(nil, key, as.ModifyByPath("chunks", as.EXP_PATH_MODIFY_DEFAULT, increment,
				as.CtxAllChildrenWithFilter(holdsEmbedding), as.CtxMapKey(as.NewValue("hits"))))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			record, err := client.Get(nil, key, "chunks")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			chunks := record.Bins["chunks"].([]any)
			gm.Expect(chunks[0].(map[any]any)["hits"]).To(gm.Equal(1))
			gm.Expect(chunks[1].(map[any]any)["hits"]).To(gm.Equal(0))
		})

		gg.Context("in UDFs", func() {
			const moduleName = "vector_udf"
			const module = `
function store(rec, value)
  rec['stored'] = value
  if aerospike:exists(rec) then aerospike:update(rec) else aerospike:create(rec) end
  return 0
end

function copy(rec)
  rec['copied'] = rec['vec']
  aerospike:update(rec)
  return 0
end

function echo(rec, value)
  return value
end

function read_vec(rec)
  return rec['vec']
end
`

			gg.BeforeEach(func() {
				registerUDF(module, moduleName+".lua")
			})

			gg.AfterEach(func() {
				removeUDF(moduleName + ".lua")
			})

			gg.It("stores a vector a UDF writes as a vector", func() {
				_, err := client.Execute(nil, key, moduleName, "store", embedding)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(getVector("stored").Equal(embedding)).To(gm.BeTrue())
			})

			gg.It("keeps a vector a vector when a UDF copies it to another bin", func() {
				gm.Expect(put(as.NewBin("vec", embedding))).To(gm.Succeed())

				_, err := client.Execute(nil, key, moduleName, "copy")
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(getVector("copied").Equal(embedding)).To(gm.BeTrue())
			})

			gg.It("rejects invalid vector bytes a UDF writes", func() {
				invalid := as.NewRawBlobValue(ParticleType.VECTOR, []byte{0xde, 0xad, 0xbe})
				_, err := client.Execute(nil, key, moduleName, "store", invalid)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue(), err.Error())
			})

			// The server labels every Lua bytes return value BLOB: the bytes come
			// back unchanged, the vector type does not (NOTES Q5).
			gg.It("returns a vector from a UDF as plain bytes", func() {
				gg.Skip("Server feedback needed: should a UDF return a vector as a vector or as a blob? (NOTES Q5)")

				gm.Expect(put(as.NewBin("vec", embedding))).To(gm.Succeed())

				fromArgument, err := client.Execute(nil, key, moduleName, "echo", embedding)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(fromArgument).To(gm.Equal(embedding.Bytes()))

				fromBin, err := client.Execute(nil, key, moduleName, "read_vec")
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(fromBin).To(gm.Equal(embedding.Bytes()))
			})
		})
	})

	gg.Context("other commands", func() {
		gg.It("writes and reads a vector in one operate command", func() {
			vector := as.NewVectorInt32([]int32{3, 1, 4})
			record, err := client.Operate(nil, key, as.PutOp(as.NewBin(binName, vector)), as.GetBinOp(binName))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(expectVector(record.Bins[binName]).Equal(vector)).To(gm.BeTrue())
		})

		gg.It("writes and reads vectors through batch commands", func() {
			batchKeys := make([]*as.Key, 3)
			batchWrites := make([]as.BatchRecordIfc, len(batchKeys))
			for index := range batchKeys {
				var err as.Error
				batchKeys[index], err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				vector := as.NewVectorFloat32([]float32{float32(index)})
				batchWrites[index] = as.NewBatchWrite(nil, batchKeys[index], as.PutOp(as.NewBin(binName, vector)))
			}
			defer func() {
				for _, batchKey := range batchKeys {
					client.Delete(nil, batchKey)
				}
			}()

			gm.Expect(client.BatchOperate(nil, batchWrites)).To(gm.Succeed())
			for _, batchWrite := range batchWrites {
				gm.Expect(batchWrite.BatchRec().ResultCode).To(gm.Equal(ast.OK))
			}

			records, err := client.BatchGet(nil, batchKeys, binName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			for index, record := range records {
				want := as.NewVectorFloat32([]float32{float32(index)})
				gm.Expect(expectVector(record.Bins[binName]).Equal(want)).To(gm.BeTrue())
			}
		})

		gg.It("returns every vector from a query over the set", func() {
			const recordCount = 5
			queryKeys := make([]*as.Key, recordCount)
			for id := range queryKeys {
				var err as.Error
				queryKeys[id], err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				vector := as.NewVectorFloat32([]float32{float32(id), float32(id)})
				gm.Expect(client.PutBins(nil, queryKeys[id], as.NewBin("id", id), as.NewBin(binName, vector))).To(gm.Succeed())
			}
			defer func() {
				for _, queryKey := range queryKeys {
					client.Delete(nil, queryKey)
				}
			}()

			recordset, err := client.Query(nil, as.NewStatement(ns, set))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			seen := map[int]bool{}
			for result := range recordset.Results() {
				gm.Expect(result.Err).ToNot(gm.HaveOccurred())
				id := result.Record.Bins["id"].(int)
				want := as.NewVectorFloat32([]float32{float32(id), float32(id)})
				gm.Expect(expectVector(result.Record.Bins[binName]).Equal(want)).To(gm.BeTrue(), "record %d", id)
				seen[id] = true
			}
			gm.Expect(seen).To(gm.HaveLen(recordCount))
		})
	})

	gg.Context("in multi-record transactions", func() {
		first := as.NewVectorFloat32([]float32{1, 2, 3})
		second := as.NewVectorFloat32([]float32{4, 5, 6})

		gg.BeforeEach(func() {
			if serverIsOlderThan("8") {
				gg.Skip("transactions require server 8.0 or later")
			}
			if !as.ConfiguredAsStrongConsistency(client, ns) {
				gg.Skip("transactions require a strong consistency namespace")
			}
		})

		inTxn := func(txn *as.Txn) *as.WritePolicy {
			policy := as.NewWritePolicy(0, 0)
			policy.Txn = txn
			return policy
		}

		gg.It("commits a vector written in a transaction and reads it back as a vector", func() {
			txn := as.NewTxn()
			gm.Expect(client.PutBins(inTxn(txn), key, as.NewBin(binName, first))).To(gm.Succeed())

			readInTxn := as.NewPolicy()
			readInTxn.Txn = txn
			record, err := client.Get(readInTxn, key, binName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(expectVector(record.Bins[binName]).Equal(first)).To(gm.BeTrue())

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))
			gm.Expect(getVector(binName).Equal(first)).To(gm.BeTrue())
		})

		gg.It("keeps the earlier vector when a transaction that replaced it is aborted", func() {
			gm.Expect(put(as.NewBin(binName, first))).To(gm.Succeed())

			txn := as.NewTxn()
			gm.Expect(client.PutBins(inTxn(txn), key, as.NewBin(binName, second))).To(gm.Succeed())

			status, err := client.Abort(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.AbortStatusOK))
			gm.Expect(getVector(binName).Equal(first)).To(gm.BeTrue())
		})

		gg.It("commits vectors written by a batch in a transaction", func() {
			batchKeys := make([]*as.Key, 2)
			for index := range batchKeys {
				var err as.Error
				batchKeys[index], err = as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
			defer func() {
				for _, batchKey := range batchKeys {
					client.Delete(nil, batchKey)
				}
			}()

			txn := as.NewTxn()
			batchPolicy := as.NewBatchPolicy()
			batchPolicy.Txn = txn
			writes := []as.BatchRecordIfc{
				as.NewBatchWrite(nil, batchKeys[0], as.PutOp(as.NewBin(binName, first))),
				as.NewBatchWrite(nil, batchKeys[1], as.PutOp(as.NewBin(binName, second))),
			}
			gm.Expect(client.BatchOperate(batchPolicy, writes)).To(gm.Succeed())
			for _, write := range writes {
				gm.Expect(write.BatchRec().ResultCode).To(gm.Equal(ast.OK))
			}

			status, err := client.Commit(txn)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(status).To(gm.Equal(as.CommitStatusOK))

			records, err := client.BatchGet(nil, batchKeys, binName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(expectVector(records[0].Bins[binName]).Equal(first)).To(gm.BeTrue())
			gm.Expect(expectVector(records[1].Bins[binName]).Equal(second)).To(gm.BeTrue())
		})
	})

	gg.Context("on a cluster without vector support", func() {
		vector := as.NewVectorFloat32([]float32{1, 2})
		containsVector := as.ExpEq(as.ExpListVal(vector), as.ExpListVal(vector))

		expectRefused := func(err as.Error) {
			gm.ExpectWithOffset(1, err).To(gm.HaveOccurred())
			gm.ExpectWithOffset(1, err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue(), err.Error())
			gm.ExpectWithOffset(1, err.Error()).To(gm.ContainSubstring("Vector is not supported by all nodes in the cluster"))
			gm.ExpectWithOffset(1, err.IsInDoubt()).To(gm.BeFalse())
		}

		// A client of its own, so the shared client's view of the cluster is
		// never changed.
		var clientWithoutVectorSupport *as.Client

		gg.BeforeEach(func() {
			var err as.Error
			clientWithoutVectorSupport, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			clientWithoutVectorSupport.Cluster().SetVectorSupportForTest(false)
		})

		gg.AfterEach(func() {
			clientWithoutVectorSupport.Close()
		})

		gg.It("refuses a vector bin before anything is sent", func() {
			expectRefused(clientWithoutVectorSupport.PutBins(nil, key, as.NewBin(binName, vector)))

			exists, err := client.Exists(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(exists).To(gm.BeFalse())
		})

		gg.It("refuses a batch write that carries a vector", func() {
			write := as.NewBatchWrite(nil, key, as.PutOp(as.NewBin(binName, vector)))
			expectRefused(clientWithoutVectorSupport.BatchOperate(nil, []as.BatchRecordIfc{write}))
		})

		// twoKeysOnOneNode returns two keys with the same write master. One record
		// per node is sent as a single command; two make a real batch command.
		twoKeysOnOneNode := func() []*as.Key {
			cluster := clientWithoutVectorSupport.Cluster()
			policy := as.NewPolicy()
			policy.ReplicaPolicy = as.MASTER

			var sameNodeKeys []*as.Key
			var masterName string
			for attempt := 0; attempt < 10000 && len(sameNodeKeys) < 2; attempt++ {
				candidate, err := as.NewKey(ns, set, randString(50))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				partition, err := as.PartitionForWrite(cluster, policy, candidate)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				master, err := partition.GetNodeWrite(cluster)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				if masterName == "" {
					masterName = master.GetName()
				}
				if master.GetName() == masterName {
					sameNodeKeys = append(sameNodeKeys, candidate)
				}
			}
			gm.Expect(sameNodeKeys).To(gm.HaveLen(2))
			return sameNodeKeys
		}

		// expectBatchRefused does not check in-doubt: a multi-record batch marks
		// any client-side failure in doubt, whatever the error (see NOTES).
		expectBatchRefused := func(err as.Error, keys []*as.Key) {
			gm.ExpectWithOffset(1, err).To(gm.HaveOccurred())
			gm.ExpectWithOffset(1, err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue(), err.Error())
			gm.ExpectWithOffset(1, err.Error()).To(gm.ContainSubstring("Vector is not supported by all nodes in the cluster"))
			for _, batchKey := range keys {
				exists, err := client.Exists(nil, batchKey)
				gm.ExpectWithOffset(1, err).ToNot(gm.HaveOccurred())
				gm.ExpectWithOffset(1, exists).To(gm.BeFalse())
			}
		}

		gg.It("refuses a batch write of several records on one node that carries a vector", func() {
			keys := twoKeysOnOneNode()
			writes := make([]as.BatchRecordIfc, len(keys))
			for index, batchKey := range keys {
				writes[index] = as.NewBatchWrite(nil, batchKey, as.PutOp(as.NewBin(binName, vector)))
			}
			expectBatchRefused(clientWithoutVectorSupport.BatchOperate(nil, writes), keys)
		})

		gg.It("refuses a batch UDF of several records on one node whose arguments carry a vector", func() {
			// Refused before sending, so the UDF module does not need to exist.
			keys := twoKeysOnOneNode()
			calls := make([]as.BatchRecordIfc, len(keys))
			for index, batchKey := range keys {
				calls[index] = as.NewBatchUDF(nil, batchKey, "vector_module", "store", vector)
			}
			expectBatchRefused(clientWithoutVectorSupport.BatchOperate(nil, calls), keys)
		})

		gg.It("refuses a UDF call whose arguments carry a vector", func() {
			// Refused before sending, so the UDF module does not need to exist.
			_, err := clientWithoutVectorSupport.Execute(nil, key, "vector_module", "store", vector)
			expectRefused(err)

			exists, err := client.Exists(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(exists).To(gm.BeFalse())
		})

		gg.It("refuses a query whose filter carries a vector", func() {
			policy := as.NewQueryPolicy()
			policy.FilterExpression = containsVector
			recordset, err := clientWithoutVectorSupport.Query(policy, as.NewStatement(ns, set))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			var queryErr as.Error
			for result := range recordset.Results() {
				if result.Err != nil {
					queryErr = result.Err
				}
			}
			expectRefused(queryErr)
		})

		gg.It("still writes bins without vectors", func() {
			gm.Expect(clientWithoutVectorSupport.PutBins(nil, key, as.NewBin("score", 7))).To(gm.Succeed())
		})
	})

	gg.Context("rejected writes", func() {
		// Detail text is returned only at error-detail verbosity 2.
		expectWriteRejected := func(value as.Value, serverDetail string) {
			policy := as.NewWritePolicy(0, 0)
			policy.ErrorDetailVerbosity = 2
			err := client.PutBins(policy, key, as.NewBin(binName, value))
			gm.Expect(err).To(gm.HaveOccurred())

			aerospikeErr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerospikeErr)).To(gm.BeTrue())
			gm.Expect(aerospikeErr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR))
			gm.Expect(aerospikeErr.ServerMessage).To(gm.ContainSubstring(serverDetail))

			// The rejected write stores nothing.
			exists, err := client.Exists(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(exists).To(gm.BeFalse())
		}

		gg.It("rejects a Vector that was never built", func() {
			expectWriteRejected(&as.Vector{}, "vector: value size 0 smaller than 6-byte header")
		})

		gg.It("rejects a vector with no dimensions", func() {
			expectWriteRejected(as.NewVectorFloat32(nil), "vector: must have at least 1 dimension")
		})

		gg.It("rejects a vector one dimension over the limit", func() {
			expectWriteRejected(as.NewVectorFloat64(make([]float64, 32769)), "vector: 32769 dimensions exceed max 32768")
		})

		gg.It("rejects NaN and infinite elements", func() {
			expectWriteRejected(as.NewVectorFloat32([]float32{1, float32(math.NaN())}), "vector: elements must be finite")
			expectWriteRejected(as.NewVectorFloat64([]float64{math.Inf(1)}), "vector: elements must be finite")
		})

		gg.It("rejects append, prepend and increment and leaves the whole record unchanged", func() {
			stored := as.NewVectorFloat32([]float32{1})
			gm.Expect(put(as.NewBin(binName, stored), as.NewBin("other", 1))).To(gm.Succeed())
			before, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Each mutation shares its command with a write to another bin, which
			// must not be applied when the command fails.
			for _, mutation := range []*as.Operation{
				as.AppendOp(as.NewBin(binName, "x")),
				as.PrependOp(as.NewBin(binName, "x")),
				as.AddOp(as.NewBin(binName, 1)),
			} {
				_, err := client.Operate(nil, key,
					as.PutOp(as.NewBin("other", 2)),
					mutation,
				)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(ast.BIN_TYPE_ERROR)).To(gm.BeTrue(), err.Error())

				after, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(after.Generation).To(gm.Equal(before.Generation))
				gm.Expect(after.Bins["other"]).To(gm.Equal(1))
				gm.Expect(expectVector(after.Bins[binName]).Equal(stored)).To(gm.BeTrue())
			}
		})
	})
})
