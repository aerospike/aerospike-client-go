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
	"math"

	"github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// packed returns the msgpack encoding of value.
func packed(value Value) []byte {
	size, err := value.pack(nil)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	buffer := newBuffer(size)
	_, err = value.pack(buffer)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return buffer.Bytes()
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
			{"float32", NewVectorFloat32([]float32{1, -2}),
				[]byte{0x01, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0xc0}},
			{"float64", NewVectorFloat64([]float64{1.5}),
				[]byte{0x01, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f}},
			{"int32 extremes", NewVectorInt32([]int32{math.MinInt32, math.MaxInt32, -1}),
				[]byte{0x01, 0x02, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff}},
			{"float16 bit patterns", NewVectorFloat16Bits([]uint16{0x3c00, 0x8000, 0x7c00}),
				[]byte{0x01, 0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x80, 0x00, 0x7c}},
			// Construction stays permissive; the server rejects zero dimensions.
			{"empty vector", NewVectorFloat32(nil),
				[]byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x00}},
		}

		for _, testCase := range testCases {
			gg.It("encodes a "+testCase.name+" vector exactly as the server stores it", func() {
				gm.Expect(testCase.vector.Bytes()).To(gm.Equal(testCase.want))
				gm.Expect(testCase.vector.EstimateSize()).To(gm.Equal(len(testCase.want)))
				gm.Expect(testCase.vector.GetType()).To(gm.Equal(ParticleType.VECTOR))
			})
		}
	})

	gg.Context("decoding", func() {
		valid := NewVectorFloat32([]float32{1, 2}).Bytes()
		malformedInputs := []struct {
			name        string
			input       []byte
			messagePart string
		}{
			{"shorter than the header", valid[:5], "shorter than the 6 byte header"},
			{"of an unknown version", append([]byte{0x02}, valid[1:]...), "unsupported format version 2"},
			{"of an unknown element type", append([]byte{0x01, 0x09}, valid[2:]...), "unknown element type 9"},
			{"with truncated elements", valid[:len(valid)-1], "does not match 2 float32 elements"},
			{"with trailing bytes", append(append([]byte{}, valid...), 0x00), "does not match 2 float32 elements"},
		}

		for _, malformed := range malformedInputs {
			gg.It("rejects bytes "+malformed.name, func() {
				vector, err := NewVectorFromBytes(malformed.input)
				gm.Expect(vector).To(gm.BeNil())
				expectParameterError(err, malformed.messagePart)
			})
		}
	})

	gg.It("packs a list element as msgpack str tagged with the vector particle type", func() {
		vector := NewVectorFloat32([]float32{1, -2})
		wire := vector.Bytes()
		// fixarray(1), fixstr(len(wire)+1), particle type, wire.
		want := append([]byte{0x91, 0xa0 | byte(len(wire)+1), ParticleType.VECTOR}, wire...)
		gm.Expect(packed(NewValue([]any{vector}))).To(gm.Equal(want))
	})

	// Lists and maps are stored without checking their vectors, so a Vector{}
	// nested in a list comes back as a type-16 value with no bytes. It is
	// returned raw instead of failing the stream.
	gg.It("returns invalid stored bytes raw and writes them back unchanged", func() {
		invalid := []byte{}
		decoded, err := bytesToParticle(ParticleType.VECTOR, invalid, 0, len(invalid))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(decoded).To(gm.Equal(&RawBlobValue{ParticleType: ParticleType.VECTOR, Data: invalid}))

		packedList := append([]byte{0x91, 0xa0 | byte(len(invalid)+1), ParticleType.VECTOR}, invalid...)
		decoded, err = newUnpacker(packedList, 0, len(packedList)).unpackObjects()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(packed(NewValue(decoded))).To(gm.Equal(packedList))
	})

	gg.Context("distance expressions", func() {
		query := NewVectorFloat32([]float32{4, 6, 8})

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

		gg.It("rejects a nil query when packed", func() {
			_, err := packedExpression(ExpVectorDistance(VectorCosineSimilarity, nil, ExpVectorBin("v")))
			expectParameterError(err, "distance query must not be nil")
		})
	})
})
