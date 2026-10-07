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
		vector, ok := record.Bins[name].(*as.Vector)
		gm.Expect(ok).To(gm.BeTrue(), "bin %q holds %#v", name, record.Bins[name])
		return vector
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
			gm.Expect(record.Bins["list"].([]any)[0].(*as.Vector).Equal(embedding)).To(gm.BeTrue())
			gm.Expect(record.Bins["map"].(map[any]any)["emb"].(*as.Vector).Equal(embedding)).To(gm.BeTrue())
		})

		gg.It("appends a vector to a list and reads it back by index", func() {
			gm.Expect(put(as.NewBin("list", []any{1}))).To(gm.Succeed())

			record, err := client.Operate(nil, key,
				as.ListAppendOp("list", embedding),
				as.ListGetByIndexOp("list", 1, as.ListReturnTypeValue),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			results := record.Bins["list"].(as.OpResults)
			gm.Expect(results[1].(*as.Vector).Equal(embedding)).To(gm.BeTrue())
		})

		gg.It("puts a vector under a map key and reads it back by key", func() {
			record, err := client.Operate(nil, key,
				as.MapPutOp(as.DefaultMapPolicy(), "map", "emb", embedding),
				as.MapGetByKeyOp("map", "emb", as.MapReturnType.VALUE),
			)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			results := record.Bins["map"].(as.OpResults)
			gm.Expect(results[1].(*as.Vector).Equal(embedding)).To(gm.BeTrue())
		})

		gg.It("writes a vector into a list nested inside a map", func() {
			gm.Expect(put(as.NewBin("doc", map[any]any{"chunks": []any{}}))).To(gm.Succeed())

			chunks := []*as.CDTContext{as.CtxMapKey(as.NewValue("chunks"))}
			_, err := client.Operate(nil, key, as.ListAppendWithPolicyContextOp(as.DefaultListPolicy(), "doc", chunks, embedding))
			gm.Expect(err).ToNot(gm.HaveOccurred())

			record, err := client.Get(nil, key, "doc")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			stored := record.Bins["doc"].(map[any]any)["chunks"].([]any)[0]
			gm.Expect(stored.(*as.Vector).Equal(embedding)).To(gm.BeTrue())
		})

		gg.It("keeps an empty vector nested in a list or map", func() {
			// Empty vectors are only rejected as a whole bin; nested ones are stored.
			empty := as.NewVectorFloat32(nil)
			gm.Expect(put(as.NewBin("list", []any{empty}), as.NewBin("map", map[any]any{"emb": empty}))).To(gm.Succeed())

			record, err := client.Get(nil, key)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins["list"].([]any)[0].(*as.Vector).Equal(empty)).To(gm.BeTrue())
			gm.Expect(record.Bins["map"].(map[any]any)["emb"].(*as.Vector).Equal(empty)).To(gm.BeTrue())
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
					gm.Expect(record.Bins[binName].(*as.Vector).Equal(stored)).To(gm.BeTrue())
				}
				return err
			}
			gm.Expect(getWhereScoreIs(7)).To(gm.Succeed())
			gm.Expect(getWhereScoreIs(8).Matches(ast.FILTERED_OUT)).To(gm.BeTrue())
		})
	})

	gg.Context("other commands", func() {
		gg.It("writes and reads a vector in one operate command", func() {
			vector := as.NewVectorInt32([]int32{3, 1, 4})
			record, err := client.Operate(nil, key, as.PutOp(as.NewBin(binName, vector)), as.GetBinOp(binName))
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins[binName].(*as.Vector).Equal(vector)).To(gm.BeTrue())
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
				gm.Expect(record.Bins[binName].(*as.Vector).Equal(want)).To(gm.BeTrue())
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
				gm.Expect(result.Record.Bins[binName].(*as.Vector).Equal(want)).To(gm.BeTrue(), "record %d", id)
				seen[id] = true
			}
			gm.Expect(seen).To(gm.HaveLen(recordCount))
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

		gg.It("rejects append, prepend and increment and leaves the vector unchanged", func() {
			stored := as.NewVectorFloat32([]float32{1})
			gm.Expect(put(as.NewBin(binName, stored))).To(gm.Succeed())

			for _, mutation := range []*as.Operation{
				as.AppendOp(as.NewBin(binName, "x")),
				as.PrependOp(as.NewBin(binName, "x")),
				as.AddOp(as.NewBin(binName, 1)),
			} {
				_, err := client.Operate(nil, key, mutation)
				gm.Expect(err).To(gm.HaveOccurred())
				gm.Expect(err.Matches(ast.BIN_TYPE_ERROR)).To(gm.BeTrue(), err.Error())
			}
			gm.Expect(getVector(binName).Equal(stored)).To(gm.BeTrue())
		})
	})
})
