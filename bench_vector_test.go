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

package aerospike

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"

	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

// benchmarkDimensions is a common embedding size.
const benchmarkDimensions = 768

func benchmarkFloat32Elements() []float32 {
	elements := make([]float32, benchmarkDimensions)
	for index := range elements {
		elements[index] = rand.Float32()
	}
	return elements
}

func benchmarkVector(b *testing.B) *Vector {
	vector, err := NewVectorFloat32(benchmarkFloat32Elements())
	if err != nil {
		b.Fatal(err)
	}
	return vector
}

func Benchmark_Vector_NewFloat32(b *testing.B) {
	elements := benchmarkFloat32Elements()
	b.ReportAllocs()
	b.SetBytes(int64(len(elements) * 4))
	for range b.N {
		if _, err := NewVectorFloat32(elements); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Vector_Float32Getter(b *testing.B) {
	vector := benchmarkVector(b)
	b.ReportAllocs()
	for range b.N {
		if _, err := vector.Float32(); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Vector_Decode(b *testing.B) {
	serialized := benchmarkVector(b).Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(serialized)))
	for range b.N {
		if _, err := bytesToParticle(ParticleType.VECTOR, serialized, 0, len(serialized)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Vector_DecodeBlobBaseline(b *testing.B) {
	serialized := benchmarkVector(b).Bytes()
	b.ReportAllocs()
	b.SetBytes(int64(len(serialized)))
	for range b.N {
		if _, err := bytesToParticle(ParticleType.BLOB, serialized, 0, len(serialized)); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Vector_PackInList(b *testing.B) {
	value := NewVectorValue(benchmarkVector(b))
	packBuffer := &benchBuffer{dataBuffer: make([]byte, 1024*1024)}
	b.ReportAllocs()
	for range b.N {
		packBuffer.dataOffset = 0
		if _, err := value.pack(packBuffer); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkWriteCommand(b *testing.B, value any) {
	policy := NewWritePolicy(0, 0)
	dataBuffer := make([]byte, 1024*1024)
	bins := []*Bin{NewBin("embedding", value)}
	key, _ := NewKey("test", "bench_vector", 1)
	b.ReportAllocs()
	for range b.N {
		command, err := newWriteCommand(nil, policy, key, bins, nil, _WRITE)
		if err != nil {
			b.Fatal(err)
		}
		command.baseCommand.dataBuffer = dataBuffer
		if err := command.writeBuffer(&command); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Vector_WriteCommand(b *testing.B) {
	benchmarkWriteCommand(b, benchmarkVector(b))
}

func Benchmark_Vector_WriteCommandBlobBaseline(b *testing.B) {
	benchmarkWriteCommand(b, benchmarkVector(b).Bytes())
}

func Benchmark_Vector_DistanceExpressionPack(b *testing.B) {
	query := benchmarkVector(b)
	packBuffer := newBuffer(8 * 1024)
	b.ReportAllocs()
	for range b.N {
		packBuffer.dataOffset = 0
		expression := ExpVectorDistance(VectorCosineSimilarity, query, ExpVectorBin("embedding"))
		if _, err := expression.pack(packBuffer); err != nil {
			b.Fatal(err)
		}
	}
}

// benchmarkDimensionSweep covers a small, a common and a large embedding size.
var benchmarkDimensionSweep = []int{128, 768, 1536}

func benchmarkVectorOfDimensions(b *testing.B, dimensions int) *Vector {
	elements := make([]float32, dimensions)
	for index := range elements {
		elements[index] = rand.Float32()
	}
	vector, err := NewVectorFloat32(elements)
	if err != nil {
		b.Fatal(err)
	}
	return vector
}

func Benchmark_Vector_NewFloat32_Dims(b *testing.B) {
	for _, dimensions := range benchmarkDimensionSweep {
		b.Run(fmt.Sprintf("dims=%d", dimensions), func(b *testing.B) {
			elements, err := benchmarkVectorOfDimensions(b, dimensions).Float32()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(elements) * 4))
			for range b.N {
				if _, err := NewVectorFloat32(elements); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Benchmark_Vector_Pack_Dims(b *testing.B) {
	packBuffer := &benchBuffer{dataBuffer: make([]byte, 1024*1024)}
	for _, dimensions := range benchmarkDimensionSweep {
		b.Run(fmt.Sprintf("dims=%d", dimensions), func(b *testing.B) {
			value := NewVectorValue(benchmarkVectorOfDimensions(b, dimensions))
			b.ReportAllocs()
			for range b.N {
				packBuffer.dataOffset = 0
				if _, err := value.pack(packBuffer); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Benchmark_Vector_Decode_Dims(b *testing.B) {
	for _, dimensions := range benchmarkDimensionSweep {
		b.Run(fmt.Sprintf("dims=%d", dimensions), func(b *testing.B) {
			serialized := benchmarkVectorOfDimensions(b, dimensions).Bytes()
			b.ReportAllocs()
			b.SetBytes(int64(len(serialized)))
			for range b.N {
				if _, err := bytesToParticle(ParticleType.VECTOR, serialized, 0, len(serialized)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Benchmark_Vector_DecodeInList reads a vector back through the unpacker, as
// a vector inside a list or map bin arrives.
func Benchmark_Vector_DecodeInList(b *testing.B) {
	for _, dimensions := range benchmarkDimensionSweep {
		b.Run(fmt.Sprintf("dims=%d", dimensions), func(b *testing.B) {
			packer := newPacker()
			if _, err := packIfcList(packer, []any{benchmarkVectorOfDimensions(b, dimensions)}); err != nil {
				b.Fatal(err)
			}
			packed := packer.Bytes()
			b.ReportAllocs()
			b.SetBytes(int64(len(packed)))
			for range b.N {
				if _, err := newUnpacker(packed, 0, len(packed)).UnpackList(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Benchmark_Vector_Float64Getter(b *testing.B) {
	elements := make([]float64, benchmarkDimensions)
	for index := range elements {
		elements[index] = rand.Float64()
	}
	vector, err := NewVectorFloat64(elements)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		if _, err := vector.Float64(); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Vector_Int32Getter(b *testing.B) {
	elements := make([]int32, benchmarkDimensions)
	for index := range elements {
		elements[index] = rand.Int31()
	}
	vector, err := NewVectorInt32(elements)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for range b.N {
		if _, err := vector.Int32(); err != nil {
			b.Fatal(err)
		}
	}
}

// benchmarkTopKResults builds records whose integer "score" bin and digest
// are random, as a Top-K query would receive them.
func benchmarkTopKResults(b *testing.B, count int) []*Result {
	results := make([]*Result, count)
	for index := range results {
		key, err := NewKey("test", "bench_topk", index)
		if err != nil {
			b.Fatal(err)
		}
		bins := BinMap{"score": rand.Intn(1_000_000)}
		results[index] = &Result{Record: newRecord(nil, key, bins, 1, 0)}
	}
	return results
}

func benchmarkTopKStatement(k int) *Statement {
	statement := NewStatement("test", "bench_topk", "score")
	statement.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
	statement.SetTopK(k)
	return statement
}

func Benchmark_TopK_MergerAdd(b *testing.B) {
	results := benchmarkTopKResults(b, 10_000)
	for _, limit := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("k=%d", limit), func(b *testing.B) {
			statement := benchmarkTopKStatement(limit)
			b.ReportAllocs()
			for range b.N {
				merger := newTopKMerger(statement)
				for _, result := range results {
					merger.add(result, 0)
				}
			}
		})
	}
}

// Benchmark_TopK_MergerAddConcurrent offers records from one goroutine per
// node, as a query on a three-node cluster does.
func Benchmark_TopK_MergerAddConcurrent(b *testing.B) {
	const nodeCount = 3
	results := benchmarkTopKResults(b, 3_000)
	for _, limit := range []int{10, 1000} {
		b.Run(fmt.Sprintf("k=%d", limit), func(b *testing.B) {
			statement := benchmarkTopKStatement(limit)
			b.ReportAllocs()
			for range b.N {
				merger := newTopKMerger(statement)
				var nodes sync.WaitGroup
				nodes.Add(nodeCount)
				for node := range nodeCount {
					go func() {
						defer nodes.Done()
						for index := node; index < len(results); index += nodeCount {
							merger.add(results[index], 0)
						}
					}()
				}
				nodes.Wait()
			}
		})
	}
}

func Benchmark_TopK_MergerResults(b *testing.B) {
	results := benchmarkTopKResults(b, 1000)
	merger := newTopKMerger(benchmarkTopKStatement(1000))
	for _, result := range results {
		merger.add(result, 0)
	}
	b.ReportAllocs()
	for range b.N {
		_ = merger.results()
	}
}

// Benchmarks for values dispatched through packObject's type switch and
// through tryConcreteValue behind NewValue.

func Benchmark_Pack_AnyScalarList(b *testing.B) {
	value := NewValue([]any{1, 2, 3, 4, 5, 6, 7, 8, "a", "b", "c", "d", 1.5, 2.5, true, false})
	packBuffer := newBuffer(4096)
	b.ReportAllocs()
	for range b.N {
		packBuffer.dataOffset = 0
		if _, err := value.pack(packBuffer); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Pack_AnyMixedList(b *testing.B) {
	value := NewValue([]any{1, "name", []byte{1, 2, 3}, 3.5, map[any]any{"k": 1}, []any{1, 2}})
	packBuffer := newBuffer(4096)
	b.ReportAllocs()
	for range b.N {
		packBuffer.dataOffset = 0
		if _, err := value.pack(packBuffer); err != nil {
			b.Fatal(err)
		}
	}
}

func Benchmark_Pack_AnyStringMap(b *testing.B) {
	value := NewValue(map[any]any{"a": 1, "b": 2, "c": 3, "d": 4, "e": 5, "f": 6, "g": 7, "h": 8})
	packBuffer := newBuffer(4096)
	b.ReportAllocs()
	for range b.N {
		packBuffer.dataOffset = 0
		if _, err := value.pack(packBuffer); err != nil {
			b.Fatal(err)
		}
	}
}

var benchmarkScalarValueSink Value

func Benchmark_NewValue_Scalars(b *testing.B) {
	inputs := []any{1, int64(2), "name", 1.5, true, []byte{1}, uint16(3), int32(4), float32(2.5)}
	b.ReportAllocs()
	for range b.N {
		for _, input := range inputs {
			benchmarkScalarValueSink = NewValue(input)
		}
	}
}

// Benchmark_SetQuery writes a query command buffer, with and without Top-K
// order-by fields, mirroring how queryFields builds one.
func Benchmark_SetQuery(b *testing.B) {
	plain := NewStatement("test", "bench_query")
	plain.BinNames = []string{"score", "id"}

	topK := NewStatement("test", "bench_query")
	topK.BinNames = []string{"score", "id"}
	topK.SetOrderBy("score", BinDataTypeInteger, OrderDesc)
	topK.ThenOrderBy("id", BinDataTypeInteger, OrderAsc)
	topK.SetTopK(1000)

	policy := NewQueryPolicy()
	dataBuffer := make([]byte, 8*1024)
	scenarios := []struct {
		name      string
		statement *Statement
		sendTopK  bool
	}{
		{"plain", plain, false},
		{"two_order_by_keys_and_k", topK, true},
	}
	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				cmd := baseCommand{bufferEx: bufferEx{dataBuffer: dataBuffer}}
				if err := cmd.setQuery(policy, nil, scenario.statement, 1, nil, false, nil, scenario.sendTopK); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
