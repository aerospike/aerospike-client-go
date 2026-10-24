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

// mustVector returns vector. It panics if the constructor returned an error.
func mustVector(vector *as.Vector, err as.Error) *as.Vector {
	if err != nil {
		panic(err)
	}
	return vector
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

	// expectNoDimensionsRefused asserts the client refused a vector with no
	// dimensions itself: a parameter error with no message from the server.
	expectNoDimensionsRefused := func(err as.Error) {
		gm.ExpectWithOffset(1, err).To(gm.HaveOccurred())
		aerospikeErr := &as.AerospikeError{}
		gm.ExpectWithOffset(1, errors.As(err, &aerospikeErr)).To(gm.BeTrue())
		gm.ExpectWithOffset(1, aerospikeErr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR), err.Error())
		gm.ExpectWithOffset(1, err.Error()).To(gm.ContainSubstring("vector: must have at least 1 dimension"))
		gm.ExpectWithOffset(1, aerospikeErr.ServerMessage).To(gm.BeEmpty())
	}

	// expectVectorMapKeyRefused asserts the client refused a vector map key
	// itself: a parameter error with no message from the server.
	expectVectorMapKeyRefused := func(err as.Error) {
		gm.ExpectWithOffset(1, err).To(gm.HaveOccurred())
		aerospikeErr := &as.AerospikeError{}
		gm.ExpectWithOffset(1, errors.As(err, &aerospikeErr)).To(gm.BeTrue())
		gm.ExpectWithOffset(1, aerospikeErr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR), err.Error())
		gm.ExpectWithOffset(1, err.Error()).To(gm.ContainSubstring("list/map cannot hold a vector"))
		gm.ExpectWithOffset(1, aerospikeErr.ServerMessage).To(gm.BeEmpty())
	}

	// withErrorDetails returns a write policy that asks for subcodes and messages.
	withErrorDetails := func() *as.WritePolicy {
		policy := as.NewWritePolicy(0, 0)
		policy.ErrorDetailVerbosity = 2
		return policy
	}

	// expectServerError asserts the server rejected the command with
	// resultCode and a message containing serverMessage. Vector errors carry
	// no subcode.
	expectServerError := func(err as.Error, resultCode ast.ResultCode, serverMessage string) {
		gm.ExpectWithOffset(1, err).To(gm.HaveOccurred())

		aerospikeErr := &as.AerospikeError{}
		gm.ExpectWithOffset(1, errors.As(err, &aerospikeErr)).To(gm.BeTrue())
		gm.ExpectWithOffset(1, aerospikeErr.ResultCode).To(gm.Equal(resultCode), err.Error())
		gm.ExpectWithOffset(1, aerospikeErr.SubCode).To(gm.Equal(ast.SubCodeNone))
		gm.ExpectWithOffset(1, aerospikeErr.ServerMessage).To(gm.ContainSubstring(serverMessage))
	}

	// operation returns a function that runs op on key, asking for error details.
	operation := func(op *as.Operation) func() as.Error {
		return func() as.Error {
			_, err := client.Operate(withErrorDetails(), key, op)
			return err
		}
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
			{"float16", mustVector(as.NewVectorFloat16Bits([]uint16{0x3c00, 0x8000, 0x0001, 0x7bff}))},
			{"int32", mustVector(as.NewVectorInt32([]int32{math.MinInt32, 0, math.MaxInt32}))},
			{"float32", mustVector(as.NewVectorFloat32([]float32{1.5, float32(math.Copysign(0, -1)), math.MaxFloat32}))},
			{"float64", mustVector(as.NewVectorFloat64([]float64{-2.25, math.SmallestNonzeroFloat64, math.MaxFloat64}))},
			{"single dimension", mustVector(as.NewVectorFloat32([]float32{42}))},
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
				"float16": mustVector(as.NewVectorFloat16Bits(make([]uint16, 131072))),
				"int32":   mustVector(as.NewVectorInt32(make([]int32, 65536))),
				"float32": mustVector(as.NewVectorFloat32(make([]float32, 65536))),
				"float64": mustVector(as.NewVectorFloat64(make([]float64, 32768))),
			}
			for elementType, vector := range largestVectors {
				gm.Expect(put(as.NewBin(binName, vector))).To(gm.Succeed(), elementType)
				gm.Expect(getVector(binName).Dimensions()).To(gm.Equal(vector.Dimensions()), elementType)
			}
		})

		gg.It("replaces a vector with a different vector or a plain value", func() {
			gm.Expect(put(as.NewBin(binName, mustVector(as.NewVectorFloat32([]float32{1, 2, 3}))))).To(gm.Succeed())

			replacement := mustVector(as.NewVectorFloat64([]float64{9}))
			gm.Expect(put(as.NewBin(binName, replacement))).To(gm.Succeed())
			gm.Expect(getVector(binName).Equal(replacement)).To(gm.BeTrue())

			gm.Expect(put(as.NewBin(binName, "text"))).To(gm.Succeed())
			record, err := client.Get(nil, key, binName)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName]).To(gm.Equal("text"))
		})

		gg.It("deletes a vector bin and keeps the other bins", func() {
			gm.Expect(put(as.NewBin(binName, mustVector(as.NewVectorInt32([]int32{1}))), as.NewBin("other", 1))).To(gm.Succeed())
			gm.Expect(put(as.NewBin(binName, nil))).To(gm.Succeed())

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins).To(gm.Equal(as.BinMap{"other": 1}))
		})
	})

	// A vector can't be an element of a list or a map, at any depth, as a value
	// or as a key: the server rejects the write and the record is unchanged.
	gg.Context("lists and maps", func() {
		embedding := mustVector(as.NewVectorFloat32([]float32{0.5, -0.5}))
		chunks := []*as.CDTContext{as.CtxMapKey(as.NewValue("chunks"))}

		putBin := func(bin *as.Bin) func() as.Error {
			return func() as.Error { return client.PutBins(withErrorDetails(), key, bin) }
		}

		writes := []struct {
			name          string
			write         func() as.Error
			serverMessage string
		}{
			{"in a list bin", putBin(as.NewBin("list", []any{embedding, 1})), "list/map cannot hold a vector"},
			{"as a map bin value", putBin(as.NewBin("map", map[any]any{"emb": embedding})), "list/map cannot hold a vector"},
			{"appended to a list", operation(as.ListAppendOp("list", embedding)), "list_append: list/map cannot hold a vector"},
			{"inserted into a list", operation(as.ListInsertOp("list", 0, embedding)), "list_insert: list/map cannot hold a vector"},
			{"set in a list", operation(as.ListSetOp("list", 0, embedding)), "list_set: list/map cannot hold a vector"},
			{"put as a map value", operation(as.MapPutOp(as.DefaultMapPolicy(), "map", "emb", embedding)), "map_add: invalid value - list/map cannot hold a vector"},
			{"put as a map key", operation(as.MapPutOp(as.DefaultMapPolicy(), "map", embedding, 1)), "map_add: invalid key - list/map cannot hold a vector"},
			{"appended to a list inside a map", operation(as.ListAppendWithPolicyContextOp(as.DefaultListPolicy(), "doc", chunks, embedding)), "list_append: list/map cannot hold a vector"},
		}

		for _, write := range writes {
			gg.It("rejects a vector "+write.name, func() {
				gm.Expect(put(as.NewBin("list", []any{1}), as.NewBin("doc", map[any]any{"chunks": []any{}}))).To(gm.Succeed())
				before, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())

				expectServerError(write.write(), ast.PARAMETER_ERROR, write.serverMessage)

				after, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(after.Generation).To(gm.Equal(before.Generation))
				gm.Expect(after.Bins).To(gm.Equal(before.Bins))
			})
		}

		gg.It("refuses a vector map key in a map bin before sending it", func() {
			for _, mapKey := range []any{embedding, as.NewVectorValue(embedding)} {
				expectVectorMapKeyRefused(put(as.NewBin("map", map[any]any{mapKey: 1})))
			}

			exists, err := client.Exists(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(exists).To(gm.BeFalse())
		})

		gg.It("refuses a vector with no dimensions in a list or map before sending it", func() {
			empty := &as.Vector{}
			for _, bin := range []*as.Bin{as.NewBin("list", []any{empty}), as.NewBin("map", map[any]any{"emb": empty})} {
				expectNoDimensionsRefused(put(bin))
			}

			exists, err := client.Exists(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(exists).To(gm.BeFalse())
		})

		gg.It("refuses raw type-16 bytes in a list before sending them", func() {
			err := put(as.NewBin("list", []any{as.NewRawBlobValue(ParticleType.VECTOR, []byte{0xde, 0xad, 0xbe}), "next"}))
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.PARAMETER_ERROR)).To(gm.BeTrue(), err.Error())
			gm.Expect(err.Error()).To(gm.ContainSubstring("a raw value of particle type 16 can only be written as a whole bin"))

			exists, err := client.Exists(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(exists).To(gm.BeFalse())
		})
	})

	gg.Context("expressions", func() {
		gg.It("reports particle type 16 through ExpBinType", func() {
			gm.Expect(put(as.NewBin(binName, mustVector(as.NewVectorFloat32([]float32{1}))), as.NewBin("blob", []byte{1}))).To(gm.Succeed())

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
			stored := mustVector(as.NewVectorFloat32([]float32{1, 2}))
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
		stored := mustVector(as.NewVectorFloat32([]float32{1, 2, 3}))
		query := mustVector(as.NewVectorFloat32([]float32{4, 6, 8}))

		allMetrics := []as.VectorDistanceMetric{as.VectorEuclideanSquared, as.VectorDotProduct, as.VectorCosineSimilarity}

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
				{"near", mustVector(as.NewVectorFloat32([]float32{5, 6, 8})), 1, true},
				{"fewer dims", mustVector(as.NewVectorFloat32([]float32{1, 2})), 0, false},
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
				return as.ExpReadOp("dist", distanceTo(as.VectorEuclideanSquared, binName), flags)
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
				as.ExpReadOp("euclidean", distanceTo(as.VectorEuclideanSquared, binName), as.ExpReadFlagDefault),
				as.ExpReadOp("dot", distanceTo(as.VectorDotProduct, binName), as.ExpReadFlagDefault),
				as.ExpReadOp("cosine", distanceTo(as.VectorCosineSimilarity, binName), as.ExpReadFlagDefault),
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
				policy.FilterExpression = as.ExpLess(distanceTo(as.VectorEuclideanSquared, binName), as.ExpFloatVal(limit))
				_, err := client.Get(policy, key)
				return err
			}
			gm.Expect(getWithinDistance(51)).To(gm.Succeed())
			gm.Expect(getWithinDistance(50).Matches(ast.FILTERED_OUT)).To(gm.BeTrue())
		})

		gg.It("returns nil with EvalNoFail and fails by default when the bin cannot be compared", func() {
			gm.Expect(put(
				as.NewBin("fewer_dims", mustVector(as.NewVectorFloat32([]float32{1, 2}))),
				as.NewBin("float64", mustVector(as.NewVectorFloat64([]float64{1, 2, 3}))),
				as.NewBin("int32", mustVector(as.NewVectorInt32([]int32{1, 2, 3}))),
				as.NewBin("text", "not a vector"),
			)).To(gm.Succeed())

			for _, metric := range allMetrics {
				for _, bin := range []string{"fewer_dims", "float64", "int32", "text", "missing"} {
					record, err := client.Operate(nil, key, as.ExpReadOp("d", distanceTo(metric, bin), as.ExpReadFlagEvalNoFail))
					gm.Expect(err).ToNot(gm.HaveOccurred(), bin)
					gm.Expect(record.Bins["d"]).To(gm.BeNil(), bin)

					_, err = client.Operate(nil, key, as.ExpReadOp("d", distanceTo(metric, bin), as.ExpReadFlagDefault))
					gm.Expect(err).To(gm.HaveOccurred(), bin)
					gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue(), err.Error())
				}
			}
		})

		gg.It("filters out a record whose distance cannot be computed", func() {
			gm.Expect(put(as.NewBin(binName, mustVector(as.NewVectorFloat32([]float32{1, 2}))))).To(gm.Succeed())

			policy := as.NewPolicy()
			policy.FilterExpression = as.ExpLess(distanceTo(as.VectorEuclideanSquared, binName), as.ExpFloatVal(math.MaxFloat64))
			_, err := client.Get(policy, key)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.FILTERED_OUT)).To(gm.BeTrue(), err.Error())
		})

		gg.It("treats a distance that is not finite as one it cannot compute", func() {
			huge := mustVector(as.NewVectorFloat64([]float64{1e300, -1e300, 1e300}))
			opposite := mustVector(as.NewVectorFloat64([]float64{-1e300, 1e300, -1e300}))
			gm.Expect(put(as.NewBin(binName, huge))).To(gm.Succeed())

			// The vectors can be compared: their cosine similarity is finite.
			record, err := client.Operate(nil, key, as.ExpReadOp("cosine",
				as.ExpVectorDistance(as.VectorCosineSimilarity, opposite, as.ExpVectorBin(binName)), as.ExpReadFlagDefault))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["cosine"]).To(gm.BeNumerically("~", -1.0, 1e-6))

			for _, metric := range []as.VectorDistanceMetric{as.VectorEuclideanSquared, as.VectorDotProduct} {
				distance := as.ExpVectorDistance(metric, opposite, as.ExpVectorBin(binName))
				record, err := client.Operate(nil, key, as.ExpReadOp("d", distance, as.ExpReadFlagEvalNoFail))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(record.Bins["d"]).To(gm.BeNil())

				_, err = client.Operate(nil, key, as.ExpReadOp("d", distance, as.ExpReadFlagDefault))
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue(), err.Error())
			}
		})

		gg.It("returns a finite cosine similarity for very large values", func() {
			large := mustVector(as.NewVectorFloat64([]float64{1e200, 1e200}))
			gm.Expect(put(as.NewBin(binName, large))).To(gm.Succeed())

			record, err := client.Operate(nil, key, as.ExpReadOp("cosine",
				as.ExpVectorDistance(as.VectorCosineSimilarity, large, as.ExpVectorBin(binName)), as.ExpReadFlagDefault))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["cosine"]).To(gm.BeNumerically("~", 1.0, 1e-6))
		})

		gg.It("rejects a distance whose operand is not a vector when the expression is built", func() {
			gm.Expect(put(as.NewBin("count", 1))).To(gm.Succeed())

			policy := as.NewWritePolicy(0, 0)
			policy.ErrorDetailVerbosity = 3
			_, err := client.Operate(policy, key, as.ExpReadOp("d",
				as.ExpVectorDistance(as.VectorEuclideanSquared, query, as.ExpIntBin("count")), as.ExpReadFlagDefault))
			gm.Expect(err).To(gm.HaveOccurred())

			aerospikeErr := &as.AerospikeError{}
			gm.Expect(errors.As(err, &aerospikeErr)).To(gm.BeTrue())
			gm.Expect(aerospikeErr.ResultCode).To(gm.Equal(ast.PARAMETER_ERROR), err.Error())
			gm.Expect(aerospikeErr.SubCode).To(gm.Equal(ast.SubCodeNone))
			gm.Expect(aerospikeErr.ServerMessage).To(gm.ContainSubstring("invalid expression in operation request"))
			gm.Expect(aerospikeErr.ExpTrace).ToNot(gm.BeNil())
		})

		gg.It("refuses a distance to a query vector with no dimensions before sending it", func() {
			gm.Expect(put(as.NewBin(binName, stored))).To(gm.Succeed())
			distance := as.ExpVectorDistance(as.VectorEuclideanSquared, &as.Vector{}, as.ExpVectorBin(binName))

			// A filter expression is encoded with the command, which returns the error.
			policy := as.NewPolicy()
			policy.FilterExpression = as.ExpLess(distance, as.ExpFloatVal(1))
			_, err := client.Get(policy, key)
			expectNoDimensionsRefused(err)

			// ExpReadOp encodes its expression when it is built and panics on an
			// expression it cannot encode.
			gm.Expect(func() { as.ExpReadOp("d", distance, as.ExpReadFlagDefault) }).
				To(gm.PanicWith(gm.MatchError(gm.ContainSubstring("vector: must have at least 1 dimension"))))
		})

		gg.It("fails a blob read of a vector bin and keeps serving the record", func() {
			gm.Expect(put(as.NewBin(binName, stored))).To(gm.Succeed())

			_, err := client.Operate(nil, key, as.ExpReadOp("b", as.ExpBlobBin(binName), as.ExpReadFlagDefault))
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(ast.OP_NOT_APPLICABLE)).To(gm.BeTrue(), err.Error())

			gm.Expect(getVector(binName).Equal(stored)).To(gm.BeTrue())
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
		embedding := mustVector(as.NewVectorFloat32([]float32{0.5, -0.5}))

		// An expression can't hold a vector inside a list literal: the server
		// rejects the expression when it builds it.
		gg.Context("in a list literal", func() {
			holdsEmbedding := as.ExpGreater(
				as.ExpListGetByValueList(as.ListReturnTypeCount, as.ExpListVal(as.NewVectorValue(embedding)), as.ExpListBin("embs")),
				as.ExpIntVal(0),
			)
			increment := as.ExpNumAdd(as.ExpIntLoopVar(as.VALUE), as.ExpIntVal(1))

			filter := func() as.Error {
				policy := as.NewPolicy()
				policy.ErrorDetailVerbosity = 2
				policy.FilterExpression = holdsEmbedding
				_, err := client.Get(policy, key)
				return err
			}

			uses := []struct {
				name string
				run  func() as.Error
			}{
				{"a filter expression", filter},
				{"an expression read operation", operation(as.ExpReadOp("hits", holdsEmbedding, as.ExpReadFlagDefault))},
				{"an expression write operation", operation(as.ExpWriteOp("copy", as.ExpListVal(as.NewVectorValue(embedding)), as.ExpWriteFlagDefault))},
				{"a context filter", operation(as.SelectByPath("chunks", as.EXP_PATH_SELECT_VALUE, as.CtxAllChildrenWithFilter(holdsEmbedding)))},
				{"modify by path", operation(as.ModifyByPath("chunks", as.EXP_PATH_MODIFY_DEFAULT, increment,
					as.CtxAllChildrenWithFilter(holdsEmbedding), as.CtxMapKey(as.NewValue("hits"))))},
			}

			for _, use := range uses {
				gg.It("rejects "+use.name, func() {
					gm.Expect(put(as.NewBin("embs", []any{1, 2}), as.NewBin("chunks", []any{map[any]any{"hits": 0}}))).To(gm.Succeed())
					before, err := client.Get(nil, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())

					expectServerError(use.run(), ast.PARAMETER_ERROR, "expression literal: list/map cannot hold a vector")

					after, err := client.Get(nil, key)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(after.Generation).To(gm.Equal(before.Generation))
				})
			}
		})

		gg.It("refuses a map literal with a vector key among other keys before sending it", func() {
			gm.Expect(put(as.NewBin("id", 1))).To(gm.Succeed())

			policy := as.NewPolicy()
			policy.ErrorDetailVerbosity = 2
			policy.FilterExpression = as.ExpEq(
				as.ExpMapVal(as.NewMapValue(map[any]any{embedding: 1, "other": 2})),
				as.ExpMapBin("m"),
			)
			_, err := client.Get(policy, key)
			expectVectorMapKeyRefused(err)
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

function store_invalid_vector(rec)
  local invalid = bytes(0)
  bytes.set_type(invalid, 16)
  rec['stored'] = invalid
  if aerospike:exists(rec) then aerospike:update(rec) else aerospike:create(rec) end
  return 0
end

function store_in_list(rec, value)
  rec['stored'] = list{value}
  if aerospike:exists(rec) then aerospike:update(rec) else aerospike:create(rec) end
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

			gg.It("stores a vector argument a UDF writes as a vector bin", func() {
				_, err := client.Execute(nil, key, moduleName, "store", as.NewVectorValue(embedding))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(getVector("stored").Equal(embedding)).To(gm.BeTrue())
			})

			gg.It("keeps the vector type when a UDF copies a vector bin to another bin", func() {
				gm.Expect(put(as.NewBin("vec", embedding))).To(gm.Succeed())

				_, err := client.Execute(nil, key, moduleName, "copy")
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(getVector("copied").Equal(embedding)).To(gm.BeTrue())
			})

			gg.It("rejects invalid vector bytes a UDF writes", func() {
				_, err := client.Execute(withErrorDetails(), key, moduleName, "store_invalid_vector")
				expectServerError(err, ast.PARAMETER_ERROR, "vector: value size 0 smaller than 6-byte header")

				exists, err := client.Exists(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(exists).To(gm.BeFalse())
			})

			gg.It("rejects a list holding a vector that a UDF writes", func() {
				_, err := client.Execute(withErrorDetails(), key, moduleName, "store_in_list", as.NewVectorValue(embedding))
				expectServerError(err, ast.PARAMETER_ERROR, "list/map cannot hold a vector")

				exists, err := client.Exists(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(exists).To(gm.BeFalse())
			})

			// The server labels every Lua bytes return value BLOB: the bytes come
			// back unchanged, the vector type does not (NOTES Q5).
			gg.It("returns a vector from a UDF as plain bytes", func() {
				gg.Skip("Server feedback needed: should a UDF return a vector as a vector or as a blob? (NOTES Q5)")

				gm.Expect(put(as.NewBin("vec", embedding))).To(gm.Succeed())

				fromArgument, err := client.Execute(nil, key, moduleName, "echo", as.NewVectorValue(embedding))
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
			vector := mustVector(as.NewVectorInt32([]int32{3, 1, 4}))
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
				vector := mustVector(as.NewVectorFloat32([]float32{float32(index)}))
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
				want := mustVector(as.NewVectorFloat32([]float32{float32(index)}))
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
				vector := mustVector(as.NewVectorFloat32([]float32{float32(id), float32(id)}))
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
				want := mustVector(as.NewVectorFloat32([]float32{float32(id), float32(id)}))
				gm.Expect(expectVector(result.Record.Bins[binName]).Equal(want)).To(gm.BeTrue(), "record %d", id)
				seen[id] = true
			}
			gm.Expect(seen).To(gm.HaveLen(recordCount))
		})
	})

	gg.Context("in multi-record transactions", func() {
		first := mustVector(as.NewVectorFloat32([]float32{1, 2, 3}))
		second := mustVector(as.NewVectorFloat32([]float32{4, 5, 6}))

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
		vector := mustVector(as.NewVectorFloat32([]float32{1, 2}))
		containsVector := as.ExpEq(as.ExpListVal(as.NewVectorValue(vector)), as.ExpListVal(as.NewVectorValue(vector)))

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
				calls[index] = as.NewBatchUDF(nil, batchKey, "vector_module", "store", as.NewVectorValue(vector))
			}
			expectBatchRefused(clientWithoutVectorSupport.BatchOperate(nil, calls), keys)
		})

		gg.It("refuses a UDF call whose arguments carry a vector", func() {
			// Refused before sending, so the UDF module does not need to exist.
			_, err := clientWithoutVectorSupport.Execute(nil, key, "vector_module", "store", as.NewVectorValue(vector))
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

	// The server rejects an invalid vector with a result code and a message;
	// vector errors carry no subcode.
	gg.Context("rejected writes", func() {
		expectWriteRejected := func(value as.Value, serverMessage string) {
			err := client.PutBins(withErrorDetails(), key, as.NewBin(binName, value))
			expectServerError(err, ast.PARAMETER_ERROR, serverMessage)

			// The rejected write stores nothing.
			exists, err := client.Exists(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(exists).To(gm.BeFalse())
		}

		gg.It("refuses a Vector that was never built before sending it", func() {
			expectNoDimensionsRefused(client.PutBins(withErrorDetails(), key, as.NewBin(binName, &as.Vector{})))

			exists, err := client.Exists(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(exists).To(gm.BeFalse())
		})

		// The constructors refuse the vectors below, so these specs write the
		// bytes as they are to reach the server's own checks.
		rawVector := func(wire []byte) *as.RawBlobValue {
			return as.NewRawBlobValue(ParticleType.VECTOR, wire)
		}

		gg.It("rejects vector bytes shorter than the header", func() {
			expectWriteRejected(rawVector([]byte{}), "vector: value size 0 smaller than 6-byte header")
		})

		gg.It("rejects a vector with no dimensions", func() {
			expectWriteRejected(rawVector([]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x00}), "vector: must have at least 1 dimension")
		})

		gg.It("rejects a vector one dimension over the limit", func() {
			// float64, 32,769 dimensions
			overLimit := append([]byte{0x01, 0x04, 0x01, 0x80, 0x00, 0x00}, make([]byte, 32769*8)...)
			expectWriteRejected(rawVector(overLimit), "vector: 32769 dimensions exceed max 32768")
		})

		gg.It("rejects NaN and infinite elements", func() {
			expectWriteRejected(as.NewVectorValue(mustVector(as.NewVectorFloat32([]float32{1, float32(math.NaN())}))), "vector: elements must be finite")
			expectWriteRejected(as.NewVectorValue(mustVector(as.NewVectorFloat64([]float64{math.Inf(1)}))), "vector: elements must be finite")
		})

		gg.It("rejects append, prepend and increment and leaves the whole record unchanged", func() {
			stored := mustVector(as.NewVectorFloat32([]float32{1}))
			gm.Expect(put(as.NewBin(binName, stored), as.NewBin("other", 1))).To(gm.Succeed())
			before, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Each mutation shares its command with a write to another bin, which
			// must not be applied when the command fails.
			for _, mutation := range []struct {
				operation     *as.Operation
				serverMessage string
			}{
				{as.AppendOp(as.NewBin(binName, "x")), "cannot append/prepend to a vector bin"},
				{as.PrependOp(as.NewBin(binName, "x")), "cannot append/prepend to a vector bin"},
				{as.AddOp(as.NewBin(binName, 1)), "cannot increment a vector bin"},
			} {
				_, err := client.Operate(withErrorDetails(), key,
					as.PutOp(as.NewBin("other", 2)),
					mutation.operation,
				)
				expectServerError(err, ast.BIN_TYPE_ERROR, mutation.serverMessage)

				after, err := client.Get(nil, key)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(after.Generation).To(gm.Equal(before.Generation))
				gm.Expect(after.Bins["other"]).To(gm.Equal(1))
				gm.Expect(expectVector(after.Bins[binName]).Equal(stored)).To(gm.BeTrue())
			}
		})
	})
})
