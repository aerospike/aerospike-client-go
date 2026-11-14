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
	"encoding/binary"
	"fmt"
	"math"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// mustVector returns vector. It panics if the constructor returned an error.
func mustVector(vector *Vector, err Error) *Vector {
	if err != nil {
		panic(err)
	}
	return vector
}

// packed returns the msgpack encoding of value.
func packed(value Value) []byte {
	size, err := value.pack(nil)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	buffer := newBuffer(size)
	_, err = value.pack(buffer)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return buffer.Bytes()
}

// binOp is one bin of a record as the server returns it.
type binOp struct {
	name         string
	particleType int
	particle     []byte
}

// recordOps lays out bins as the server sends them in a record: a 4-byte
// size, the operation, particle type, version and name length bytes, the
// name, then the particle.
func recordOps(bins ...binOp) []byte {
	var wire []byte
	for _, bin := range bins {
		wire = binary.BigEndian.AppendUint32(wire, uint32(4+len(bin.name)+len(bin.particle)))
		wire = append(wire, _READ.op, byte(bin.particleType), 0, byte(len(bin.name)))
		wire = append(wire, bin.name...)
		wire = append(wire, bin.particle...)
	}
	return wire
}

// expectParseError asserts err is a PARSE_ERROR mentioning messagePart.
func expectParseError(err Error, messagePart string) {
	gm.ExpectWithOffset(1, err).To(gm.HaveOccurred())
	gm.ExpectWithOffset(1, err.Matches(types.PARSE_ERROR)).To(gm.BeTrue(), err.Error())
	gm.ExpectWithOffset(1, err.Error()).To(gm.ContainSubstring(messagePart))
}

// expectParameterError asserts err is a PARAMETER_ERROR mentioning messagePart.
func expectParameterError(err Error, messagePart string) {
	gm.ExpectWithOffset(1, err).To(gm.HaveOccurred())
	gm.ExpectWithOffset(1, err.Matches(types.PARAMETER_ERROR)).To(gm.BeTrue(), err.Error())
	gm.ExpectWithOffset(1, err.Error()).To(gm.ContainSubstring(messagePart))
}

// Expected bytes follow the server's vector layout: version(1) | element
// type(1) | dimensions(u32 LE) | elements(LE).
var _ = gg.Describe("Vector encoding", func() {

	gg.Context("wire format", func() {
		testCases := []struct {
			name   string
			vector *Vector
			want   []byte
		}{
			// Byte-for-byte the server's GoldenSixByteHeaderImage.
			{"float32", mustVector(NewVectorFloat32([]float32{1, -2})),
				[]byte{0x01, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0xc0}},
			{"float64", mustVector(NewVectorFloat64([]float64{1.5})),
				[]byte{0x01, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f}},
			{"int32 extremes", mustVector(NewVectorInt32([]int32{math.MinInt32, math.MaxInt32, -1})),
				[]byte{0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff}},
			{"float16 bit patterns", mustVector(NewVectorFloat16Bits([]uint16{0x3c00, 0x8000, 0x7bff})),
				[]byte{0x01, 0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x80, 0xff, 0x7b}},
		}

		for _, testCase := range testCases {
			gg.It("encodes a "+testCase.name+" vector exactly as the server stores it", func() {
				gm.Expect(testCase.vector.Bytes()).To(gm.Equal(testCase.want))
				gm.Expect(NewVectorValue(testCase.vector).EstimateSize()).To(gm.Equal(len(testCase.want)))
				gm.Expect(NewVectorValue(testCase.vector).GetType()).To(gm.Equal(ParticleType.VECTOR))
			})
		}
	})

	// The server refuses a NaN or an infinity, so the constructors refuse it
	// first and name the element. Decoding stored bytes doesn't check.
	gg.Context("non-finite elements", func() {
		nonFinite := []struct {
			name  string
			build func() (*Vector, Error)
			value string
		}{
			{"a float32 NaN", func() (*Vector, Error) { return NewVectorFloat32([]float32{1, 2, float32(math.NaN()), 4}) }, "NaN"},
			{"a float32 +Inf", func() (*Vector, Error) { return NewVectorFloat32([]float32{1, 2, float32(math.Inf(1)), 4}) }, "+Inf"},
			{"a float32 -Inf", func() (*Vector, Error) { return NewVectorFloat32([]float32{1, 2, float32(math.Inf(-1)), 4}) }, "-Inf"},
			{"a float64 NaN", func() (*Vector, Error) { return NewVectorFloat64([]float64{1, 2, math.NaN(), 4}) }, "NaN"},
			{"a float64 +Inf", func() (*Vector, Error) { return NewVectorFloat64([]float64{1, 2, math.Inf(1), 4}) }, "+Inf"},
			{"a float64 -Inf", func() (*Vector, Error) { return NewVectorFloat64([]float64{1, 2, math.Inf(-1), 4}) }, "-Inf"},
			{"a float16 +Inf", func() (*Vector, Error) { return NewVectorFloat16Bits([]uint16{0x3c00, 0x4000, 0x7c00, 0x4400}) }, "0x7c00"},
			{"a float16 -Inf", func() (*Vector, Error) { return NewVectorFloat16Bits([]uint16{0x3c00, 0x4000, 0xfc00, 0x4400}) }, "0xfc00"},
			{"a float16 NaN", func() (*Vector, Error) { return NewVectorFloat16Bits([]uint16{0x3c00, 0x4000, 0x7e00, 0x4400}) }, "0x7e00"},
		}

		for _, element := range nonFinite {
			gg.It("refuses "+element.name+" and names the element", func() {
				vector, err := element.build()
				gm.Expect(vector).To(gm.BeNil())
				expectParameterError(err, "vector element 2 is not finite: "+element.value)
			})
		}

		gg.It("accepts the largest, smallest and subnormal finite values", func() {
			float32s := []float32{math.MaxFloat32, -math.MaxFloat32, math.SmallestNonzeroFloat32, 0}
			stored, err := mustVector(NewVectorFloat32(float32s)).Float32()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(stored).To(gm.Equal(float32s))

			float64s := []float64{math.MaxFloat64, -math.MaxFloat64, math.SmallestNonzeroFloat64, 0}
			storedFloat64s, err := mustVector(NewVectorFloat64(float64s)).Float64()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(storedFloat64s).To(gm.Equal(float64s))

			// 0x7bff and 0xfbff are the largest finite float16 values; 0x0001 is the smallest subnormal.
			float16s := []uint16{0x7bff, 0xfbff, 0x0001, 0x8000}
			storedBits, err := mustVector(NewVectorFloat16Bits(float16s)).Float16Bits()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(storedBits).To(gm.Equal(float16s))
		})

		gg.It("keeps a built vector unchanged when the caller later writes a NaN into its slice", func() {
			float32s := []float32{1, 2, 3}
			float32Vector := mustVector(NewVectorFloat32(float32s))
			float32s[1] = float32(math.NaN())

			float64s := []float64{1, 2, 3}
			float64Vector := mustVector(NewVectorFloat64(float64s))
			float64s[1] = math.Inf(1)

			float16s := []uint16{0x3c00, 0x4000, 0x4200}
			float16Vector := mustVector(NewVectorFloat16Bits(float16s))
			float16s[1] = 0x7e00

			gm.Expect(float32Vector.Float32()).To(gm.Equal([]float32{1, 2, 3}))
			gm.Expect(float64Vector.Float64()).To(gm.Equal([]float64{1, 2, 3}))
			gm.Expect(float16Vector.Float16Bits()).To(gm.Equal([]uint16{0x3c00, 0x4000, 0x4200}))

			// The vector still packs as built, so nothing the caller changed reaches the server.
			for _, vector := range []*Vector{float32Vector, float64Vector, float16Vector} {
				_, err := NewVectorValue(vector).pack(newBuffer(64))
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}
		})

		gg.It("leaves the caller's slice untouched when it refuses an element", func() {
			float32s := []float32{1, float32(math.Inf(1)), 3}
			vector, err := NewVectorFloat32(float32s)
			gm.Expect(vector).To(gm.BeNil())
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(float32s[0]).To(gm.Equal(float32(1)))
			gm.Expect(math.IsInf(float64(float32s[1]), 1)).To(gm.BeTrue())
			gm.Expect(float32s[2]).To(gm.Equal(float32(3)))
		})

		gg.It("still decodes stored bytes that hold a NaN", func() {
			wire := []byte{0x01, 0x03, 0x01, 0x00, 0x00, 0x00}
			wire = binary.LittleEndian.AppendUint32(wire, math.Float32bits(float32(math.NaN())))

			vector, err := NewVectorFromBytes(wire)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			elements, err := vector.Float32()
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(math.IsNaN(float64(elements[0]))).To(gm.BeTrue())
		})
	})

	// Each element type holds 1 to as many elements as fit in 256 KiB.
	gg.Context("construction", func() {
		constructors := []struct {
			name          string
			build         func(elements int) (*Vector, Error)
			maxDimensions int
		}{
			{"float16", func(elements int) (*Vector, Error) { return NewVectorFloat16Bits(make([]uint16, elements)) }, 131072},
			{"int32", func(elements int) (*Vector, Error) { return NewVectorInt32(make([]int32, elements)) }, 65536},
			{"float32", func(elements int) (*Vector, Error) { return NewVectorFloat32(make([]float32, elements)) }, 65536},
			{"float64", func(elements int) (*Vector, Error) { return NewVectorFloat64(make([]float64, elements)) }, 32768},
		}

		for _, constructor := range constructors {
			gg.It(fmt.Sprintf("builds a %s vector of 1 to %d elements and refuses none or more", constructor.name, constructor.maxDimensions), func() {
				for _, elements := range []int{1, constructor.maxDimensions} {
					vector, err := constructor.build(elements)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					gm.Expect(vector.Dimensions()).To(gm.Equal(elements))
				}

				vector, err := constructor.build(0)
				gm.Expect(vector).To(gm.BeNil())
				expectParameterError(err, "vector: must have at least 1 dimension")

				vector, err = constructor.build(constructor.maxDimensions + 1)
				gm.Expect(vector).To(gm.BeNil())
				expectParameterError(err, fmt.Sprintf("vector: %d dimensions exceed max %d", constructor.maxDimensions+1, constructor.maxDimensions))
			})
		}
	})

	gg.Context("decoding", func() {
		valid := mustVector(NewVectorFloat32([]float32{1, 2})).Bytes()
		malformedInputs := []struct {
			name        string
			input       []byte
			messagePart string
		}{
			{"shorter than the header", valid[:5], "vector: value size 5 smaller than 6-byte header"},
			{"of an unknown version", append([]byte{0x02}, valid[1:]...), "vector: unsupported format version 2 (expected 1)"},
			{"of an unknown element type", append([]byte{0x01, 0x09}, valid[2:]...), "vector: invalid element type 9"},
			{"with no dimensions", []byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x00}, "vector: must have at least 1 dimension"},
			{"with more dimensions than the element type allows", []byte{0x01, 0x04, 0x01, 0x80, 0x00, 0x00}, "vector: 32769 dimensions exceed max 32768"},
			{"with truncated elements", valid[:len(valid)-1], "vector: value size 13 does not match 14 (2 dimensions of 4 bytes, little-endian)"},
			{"with trailing bytes", append(append([]byte{}, valid...), 0x00), "vector: value size 15 does not match 14 (2 dimensions of 4 bytes, little-endian)"},
		}

		for _, malformed := range malformedInputs {
			gg.It("rejects bytes "+malformed.name, func() {
				vector, err := NewVectorFromBytes(malformed.input)
				gm.Expect(vector).To(gm.BeNil())
				expectParseError(err, malformed.messagePart)
			})
		}
	})

	gg.It("refuses to write or pack a Vector that was never built", func() {
		_, err := NewVectorValue(&Vector{}).write(newBuffer(16))
		expectParameterError(err, "vector: must have at least 1 dimension")

		_, err = NewVectorValue(&Vector{}).pack(newBuffer(16))
		expectParameterError(err, "vector: must have at least 1 dimension")
	})

	gg.It("wraps a vector in a Value without allocating", func() {
		vector := mustVector(NewVectorFloat32([]float32{1, 2}))

		var value Value
		allocations := testing.AllocsPerRun(100, func() { value = NewValue(vector) })

		gm.Expect(allocations).To(gm.BeZero())
		gm.Expect(value).To(gm.Equal(VectorValue{vector: vector}))
	})

	gg.It("appends its serialized bytes to a caller's buffer without allocating", func() {
		vector := mustVector(NewVectorFloat32(make([]float32, 768)))
		serialized := vector.Bytes()

		prefix := []byte{0xde, 0xad}
		appended := vector.AppendBytes(append([]byte{}, prefix...))
		gm.Expect(appended).To(gm.Equal(append(append([]byte{}, prefix...), serialized...)))

		buffer := make([]byte, 0, len(serialized))
		allocations := testing.AllocsPerRun(100, func() { buffer = vector.AppendBytes(buffer[:0]) })
		gm.Expect(allocations).To(gm.BeZero())

		buffer[len(buffer)-1] ^= 0xff
		gm.Expect(vector.Bytes()).To(gm.Equal(serialized))
	})

	gg.It("packs a list element as msgpack str tagged with the vector particle type", func() {
		vector := mustVector(NewVectorFloat32([]float32{1, -2}))
		wire := vector.Bytes()
		// fixarray(1), fixstr(len(wire)+1), particle type, wire.
		want := append([]byte{0x91, 0xa0 | byte(len(wire)+1), ParticleType.VECTOR}, wire...)
		gm.Expect(packed(NewValue([]any{vector}))).To(gm.Equal(want))
	})

	// Only data stored before servers validated vectors can hold such bytes.
	gg.It("refuses stored type-16 bytes that are not a vector, as a bin and inside a list", func() {
		invalid := []byte{0xde, 0xad, 0xbe}
		decoded, err := bytesToParticle(ParticleType.VECTOR, invalid, 0, len(invalid))
		gm.Expect(decoded).To(gm.BeNil())
		expectParseError(err, "vector: value size 3 smaller than 6-byte header")

		packedList := append([]byte{0x91, 0xa0 | byte(len(invalid)+1), ParticleType.VECTOR}, invalid...)
		decoded, err = newUnpacker(packedList, 0, len(packedList)).unpackObjects()
		gm.Expect(decoded).To(gm.BeNil())
		expectParseError(err, "vector: value size 3 smaller than 6-byte header")
	})

	// Only data stored before servers validated vectors can hold such bytes.
	// They are read the way the client reads any value it can't decode.
	gg.Context("stored type-16 bytes that are not a vector", func() {
		invalid := []byte{0xde, 0xad, 0xbe}
		const message = "vector: value size 3 smaller than 6-byte header"

		gg.It("fail a query, scan or batch read with a parse error, as a bin and inside a list", func() {
			decoded, err := bytesToParticle(ParticleType.VECTOR, invalid, 0, len(invalid))
			gm.Expect(decoded).To(gm.BeNil())
			expectParseError(err, message)

			decoded, err = bytesToParticleRaw(ParticleType.VECTOR, invalid, 0, len(invalid), true)
			gm.Expect(decoded).To(gm.BeNil())
			expectParseError(err, message)

			packedList := append([]byte{0x91, 0xa0 | byte(len(invalid)+1), ParticleType.VECTOR}, invalid...)
			decoded, err = newUnpacker(packedList, 0, len(packedList)).unpackObjects()
			gm.Expect(decoded).To(gm.BeNil())
			expectParseError(err, message)
		})

		gg.It("read as no bin in a single-record read, as a type the client doesn't know does", func() {
			wire := recordOps(
				binOp{"vec", ParticleType.VECTOR, invalid},
				binOp{"unknown", 99, []byte{1, 2, 3}},
				binOp{"n", ParticleType.INTEGER, binary.BigEndian.AppendUint64(nil, 7)},
			)
			parser := &recordParser{opCount: 3, cmd: &baseCommand{bufferEx: bufferEx{dataBuffer: wire}}}

			record, err := parser.parseRecord(nil, false)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(record.Bins).To(gm.HaveKeyWithValue("vec", gm.BeNil()))
			gm.Expect(record.Bins).To(gm.HaveKeyWithValue("unknown", gm.BeNil()))
			gm.Expect(record.Bins).To(gm.HaveKeyWithValue("n", 7))
		})
	})

	gg.It("refuses to pack a raw value inside a list", func() {
		_, err := NewValue([]any{NewRawBlobValue(ParticleType.VECTOR, []byte{0xde, 0xad})}).pack(nil)
		expectParameterError(err, "a raw value of particle type 16 can only be written as a whole bin")
	})

	gg.Context("distance expressions", func() {
		query := mustVector(NewVectorFloat32([]float32{4, 6, 8}))

		packedExpression := func(exp *Expression) ([]byte, Error) {
			size, err := exp.size()
			if err != nil {
				return nil, err
			}
			buffer := newBuffer(size)
			if _, err := exp.pack(buffer); err != nil {
				return nil, err
			}
			return buffer.Bytes(), nil
		}

		metrics := []struct {
			name   string
			metric VectorDistanceMetric
			opCode byte
		}{
			{"squared Euclidean", VectorEuclideanSquared, 52},
			{"dot product", VectorDotProduct, 53},
			{"cosine", VectorCosineSimilarity, 54},
		}

		for _, testCase := range metrics {
			gg.It("packs "+testCase.name+" as [op, vector bin, query as blob]", func() {
				wire := query.Bytes()
				// fixarray(3), op, [BIN(81), VECTOR(10), "v"], fixstr(len+1), BLOB, wire.
				want := append([]byte{0x93, testCase.opCode, 0x93, 0x51, 0x0a, 0xa1, 'v',
					0xa0 | byte(len(wire)+1), ParticleType.BLOB}, wire...)

				got, err := packedExpression(ExpVectorDistance(testCase.metric, query, ExpVectorBin("v")))
				gm.Expect(err).ToNot(gm.HaveOccurred())
				gm.Expect(got).To(gm.Equal(want))
			})
		}

		gg.It("panics on an unknown metric", func() {
			gm.Expect(func() { ExpVectorDistance(VectorDistanceMetric(3), query, ExpVectorBin("v")) }).
				To(gm.PanicWith(gm.MatchError(gm.ContainSubstring("Invalid VectorDistanceMetric: 3"))))
		})

		gg.It("names each metric", func() {
			for metric, name := range map[VectorDistanceMetric]string{
				VectorEuclideanSquared:  "EUCLIDEAN_SQUARED",
				VectorDotProduct:        "DOT_PRODUCT",
				VectorCosineSimilarity:  "COSINE_SIMILARITY",
				VectorDistanceMetric(7): "VectorDistanceMetric(7)",
			} {
				gm.Expect(metric.String()).To(gm.Equal(name))
			}
		})

		gg.It("rejects a nil query when packed", func() {
			_, err := packedExpression(ExpVectorDistance(VectorCosineSimilarity, nil, ExpVectorBin("v")))
			expectParameterError(err, "distance query must not be nil")
		})
	})
})
