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
	"testing"

	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

// benchRecord is the user-facing struct that both the sink path and the
// BinMap+extract path populate. Identical to what codegen would emit for
// this shape, so benching the hand-written receiver is equivalent to
// benching codegen output.
type benchRecord struct {
	I1   int    `as:"i1"`
	I2   int    `as:"i2"`
	I3   int    `as:"i3"`
	I4   int    `as:"i4"`
	S1   string `as:"s1"`
	S2   string `as:"s2"`
	S3   string `as:"s3"`
	S4   string `as:"s4"`
	B1   []byte `as:"b1"`
	B2   []byte `as:"b2"`
	B3   []byte `as:"b3"`
	B4   []byte `as:"b4"`
}

func (r *benchRecord) AerospikeBin(name string, value any) Error {
	switch name {
	case "i1":
		r.I1 = value.(int)
	case "i2":
		r.I2 = value.(int)
	case "i3":
		r.I3 = value.(int)
	case "i4":
		r.I4 = value.(int)
	case "s1":
		r.S1 = value.(string)
	case "s2":
		r.S2 = value.(string)
	case "s3":
		r.S3 = value.(string)
	case "s4":
		r.S4 = value.(string)
	case "b1":
		r.B1 = value.([]byte)
	case "b2":
		r.B2 = value.([]byte)
	case "b3":
		r.B3 = value.([]byte)
	case "b4":
		r.B4 = value.([]byte)
	}
	return nil
}

func (r *benchRecord) AerospikeBinNames() []string {
	return []string{"i1", "i2", "i3", "i4", "s1", "s2", "s3", "s4", "b1", "b2", "b3", "b4"}
}

// extractFromBinMap fills a benchRecord from a BinMap the same way a real
// user of Client.Get would after their parseRecord call. We measure this
// as part of the BinMap comparison because the BinMap-only benchmark
// understates the true cost of the Get API: the user still has to extract
// and type-assert every value before they can use it.
func extractFromBinMap(bins BinMap, r *benchRecord) {
	if v, ok := bins["i1"].(int); ok {
		r.I1 = v
	}
	if v, ok := bins["i2"].(int); ok {
		r.I2 = v
	}
	if v, ok := bins["i3"].(int); ok {
		r.I3 = v
	}
	if v, ok := bins["i4"].(int); ok {
		r.I4 = v
	}
	if v, ok := bins["s1"].(string); ok {
		r.S1 = v
	}
	if v, ok := bins["s2"].(string); ok {
		r.S2 = v
	}
	if v, ok := bins["s3"].(string); ok {
		r.S3 = v
	}
	if v, ok := bins["s4"].(string); ok {
		r.S4 = v
	}
	if v, ok := bins["b1"].([]byte); ok {
		r.B1 = v
	}
	if v, ok := bins["b2"].([]byte); ok {
		r.B2 = v
	}
	if v, ok := bins["b3"].([]byte); ok {
		r.B3 = v
	}
	if v, ok := bins["b4"].([]byte); ok {
		r.B4 = v
	}
}

func buildBenchOps() []wireOp {
	return []wireOp{
		{name: "i1", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(1)},
		{name: "i2", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(2)},
		{name: "i3", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(3)},
		{name: "i4", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(4)},
		{name: "s1", particleType: ParticleType.STRING, particle: []byte("alpha")},
		{name: "s2", particleType: ParticleType.STRING, particle: []byte("beta")},
		{name: "s3", particleType: ParticleType.STRING, particle: []byte("gamma")},
		{name: "s4", particleType: ParticleType.STRING, particle: []byte("delta")},
		{name: "b1", particleType: ParticleType.BLOB, particle: []byte{1, 2, 3, 4}},
		{name: "b2", particleType: ParticleType.BLOB, particle: []byte{5, 6, 7, 8}},
		{name: "b3", particleType: ParticleType.BLOB, particle: []byte{9, 10, 11, 12}},
		{name: "b4", particleType: ParticleType.BLOB, particle: []byte{13, 14, 15, 16}},
	}
}

// BenchmarkRead_BinMap measures the parser-only cost of producing a
// BinMap. It is NOT a fair comparison to the sink path on its own — see
// BenchmarkRead_BinMapToTyped for the user-equivalent comparison.
func BenchmarkRead_BinMap(b *testing.B) {
	ops := buildBenchOps()
	wire := encodeOps(ops)

	cmd := &baseCommand{}
	cmd.dataBuffer = wire
	rp := &recordParser{cmd: cmd, opCount: len(ops)}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd.dataOffset = 0
		rp.cmd.dataOffset = 0
		_, _ = rp.parseRecord(nil, false)
	}
}

// BenchmarkRead_BinMapToTyped measures what a real Client.Get user pays
// end-to-end on the parsing side: parser builds BinMap, then user code
// pulls each value out and type-asserts into a struct. This is the
// apples-to-apples comparison for the sink path.
func BenchmarkRead_BinMapToTyped(b *testing.B) {
	ops := buildBenchOps()
	wire := encodeOps(ops)

	cmd := &baseCommand{}
	cmd.dataBuffer = wire
	rp := &recordParser{cmd: cmd, opCount: len(ops)}

	dst := &benchRecord{}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd.dataOffset = 0
		rp.cmd.dataOffset = 0
		rec, _ := rp.parseRecord(nil, false)
		extractFromBinMap(rec.Bins, dst)
	}
}

// BenchmarkRead_Sink measures the sink path: wire → typed struct in one
// pass, no intermediate map, no reflection.
func BenchmarkRead_Sink(b *testing.B) {
	ops := buildBenchOps()
	wire := encodeOps(ops)

	dst := &benchRecord{}
	brc := &baseReadCommand{}
	brc.dataBuffer = wire
	brc.sink = dst

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		brc.dataOffset = 0
		_ = parseSink(brc, len(ops), 0, 0, 0)
	}
}
