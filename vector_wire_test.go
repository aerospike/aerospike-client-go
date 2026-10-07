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
	"bytes"
	"math"
	"strings"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

// Expected bytes follow the server's vector layout: version(1) | element
// type(1) | dimensions(u32 LE) | elements(LE).

// retiredVectorLayout is a vector in the pre-release 8-byte header layout
// (2 reserved bytes after the dimensions). Neither the server nor the client
// accepts it as a vector.
var retiredVectorLayout = []byte{0x01, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3f}

// packed returns the msgpack encoding of value.
func packed(t *testing.T, value Value) []byte {
	t.Helper()

	size, err := value.pack(nil)
	if err != nil {
		t.Fatalf("sizing pack: %v", err)
	}
	buffer := newBuffer(size)
	if _, err := value.pack(buffer); err != nil {
		t.Fatalf("pack: %v", err)
	}
	return buffer.Bytes()
}

func expectParameterError(t *testing.T, err Error, messagePart string) {
	t.Helper()

	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	if !err.Matches(types.PARAMETER_ERROR) {
		t.Fatalf("expected PARAMETER_ERROR, got %v", err)
	}
	if !strings.Contains(err.Error(), messagePart) {
		t.Fatalf("error %q does not mention %q", err.Error(), messagePart)
	}
}

func TestVectorEncodesServerWireFormat(t *testing.T) {
	testCases := []struct {
		name   string
		vector *Vector
		want   []byte
	}{
		{
			// Byte-for-byte the server's GoldenSixByteHeaderImage.
			name:   "float32 golden",
			vector: NewVectorFloat32([]float32{1, -2}),
			want:   []byte{0x01, 0x03, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80, 0x3f, 0x00, 0x00, 0x00, 0xc0},
		},
		{
			name:   "float64",
			vector: NewVectorFloat64([]float64{1.5}),
			want:   []byte{0x01, 0x04, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf8, 0x3f},
		},
		{
			name:   "int32 extremes",
			vector: NewVectorInt32([]int32{math.MinInt32, math.MaxInt32, -1}),
			want: []byte{0x01, 0x02, 0x03, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x80, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff, 0xff},
		},
		{
			name:   "float16 bit patterns are stored verbatim",
			vector: NewVectorFloat16Bits([]uint16{0x3c00, 0x8000, 0x7c00}),
			want:   []byte{0x01, 0x01, 0x03, 0x00, 0x00, 0x00, 0x00, 0x3c, 0x00, 0x80, 0x00, 0x7c},
		},
		{
			// Construction stays permissive; the server rejects zero dimensions.
			name:   "empty vector",
			vector: NewVectorFloat32(nil),
			want:   []byte{0x01, 0x03, 0x00, 0x00, 0x00, 0x00},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := testCase.vector.Bytes(); !bytes.Equal(got, testCase.want) {
				t.Fatalf("wire mismatch\n got: % x\nwant: % x", got, testCase.want)
			}
			if size, _ := testCase.vector.EstimateSize(); size != len(testCase.want) {
				t.Fatalf("EstimateSize %d, want %d", size, len(testCase.want))
			}
			if particleType := testCase.vector.GetType(); particleType != ParticleType.VECTOR {
				t.Fatalf("GetType %d, want %d", particleType, ParticleType.VECTOR)
			}
		})
	}
}

func TestVectorDecodeRoundTripsSpecialValues(t *testing.T) {
	float32Elements := []float32{float32(math.NaN()), float32(math.Inf(1)), float32(math.Inf(-1)), float32(math.Copysign(0, -1)), math.SmallestNonzeroFloat32}
	float64Elements := []float64{math.NaN(), math.Inf(-1), math.Copysign(0, -1), math.MaxFloat64}
	int32Elements := []int32{math.MinInt32, 0, math.MaxInt32}
	float16Bits := []uint16{0x0001, 0x7c00, 0xfc00, 0x7e00, 0x8000} // subnormal, +Inf, -Inf, NaN, -0

	decode := func(t *testing.T, vector *Vector) *Vector {
		t.Helper()
		decoded, err := NewVectorFromBytes(vector.Bytes())
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !decoded.Equal(vector) {
			t.Fatalf("decoded vector differs: %v vs %v", decoded, vector)
		}
		return decoded
	}

	t.Run("float32", func(t *testing.T) {
		got, err := decode(t, NewVectorFloat32(float32Elements)).Float32()
		if err != nil {
			t.Fatal(err)
		}
		for index := range float32Elements {
			if math.Float32bits(got[index]) != math.Float32bits(float32Elements[index]) {
				t.Fatalf("element %d: bits %#x, want %#x", index, math.Float32bits(got[index]), math.Float32bits(float32Elements[index]))
			}
		}
	})

	t.Run("float64", func(t *testing.T) {
		got, err := decode(t, NewVectorFloat64(float64Elements)).Float64()
		if err != nil {
			t.Fatal(err)
		}
		for index := range float64Elements {
			if math.Float64bits(got[index]) != math.Float64bits(float64Elements[index]) {
				t.Fatalf("element %d: bits %#x, want %#x", index, math.Float64bits(got[index]), math.Float64bits(float64Elements[index]))
			}
		}
	})

	t.Run("int32", func(t *testing.T) {
		got, err := decode(t, NewVectorInt32(int32Elements)).Int32()
		if err != nil {
			t.Fatal(err)
		}
		for index := range int32Elements {
			if got[index] != int32Elements[index] {
				t.Fatalf("element %d: %d, want %d", index, got[index], int32Elements[index])
			}
		}
	})

	t.Run("float16", func(t *testing.T) {
		vector := decode(t, NewVectorFloat16Bits(float16Bits))
		got, err := vector.Float16Bits()
		if err != nil {
			t.Fatal(err)
		}
		for index := range float16Bits {
			if got[index] != float16Bits[index] {
				t.Fatalf("element %d: %#04x, want %#04x", index, got[index], float16Bits[index])
			}
		}
		if vector.ElementType() != VectorElementFloat16 || vector.Dimensions() != len(float16Bits) {
			t.Fatalf("header: %s/%d", vector.ElementType(), vector.Dimensions())
		}
	})
}

func TestNewVectorFromBytesRejectsMalformedInput(t *testing.T) {
	valid := NewVectorFloat32([]float32{1, 2}).Bytes()

	testCases := []struct {
		name        string
		input       []byte
		messagePart string
	}{
		{"shorter than header", valid[:5], "shorter than the 6 byte header"},
		{"unknown version", append([]byte{0x02}, valid[1:]...), "unsupported format version 2"},
		{"unknown element type", append([]byte{0x01, 0x09}, valid[2:]...), "unknown element type 9"},
		{"truncated elements", valid[:len(valid)-1], "does not match 2 float32 elements"},
		{"trailing bytes", append(append([]byte{}, valid...), 0x00), "does not match 2 float32 elements"},
		{"retired 8-byte header", retiredVectorLayout, "does not match 1 float32 elements"},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			vector, err := NewVectorFromBytes(testCase.input)
			if vector != nil {
				t.Fatalf("expected nil vector, got %v", vector)
			}
			expectParameterError(t, err, testCase.messagePart)
		})
	}
}

func TestVectorTypedGetterRejectsOtherElementType(t *testing.T) {
	vector := NewVectorInt32([]int32{1})

	_, err := vector.Float32()
	expectParameterError(t, err, "element type is int32, not float32")

	// A zero Vector (not built by a constructor) must not panic.
	var zeroVector Vector
	if zeroVector.Dimensions() != 0 || zeroVector.ElementType() != 0 {
		t.Fatalf("zero vector header: %s/%d", zeroVector.ElementType(), zeroVector.Dimensions())
	}
	_, err = zeroVector.Float64()
	expectParameterError(t, err, "not float64")
}

func TestVectorIsImmutable(t *testing.T) {
	input := []float32{1, 2, 3}
	vector := NewVectorFloat32(input)
	input[0] = 99

	elements, _ := vector.Float32()
	if elements[0] != 1 {
		t.Fatalf("vector aliased its constructor input: %v", elements)
	}

	elements[1] = 99
	if again, _ := vector.Float32(); again[1] != 2 {
		t.Fatalf("vector aliased a getter result: %v", again)
	}

	serialized := vector.Bytes()
	serialized[6] = 0xff
	if again, _ := vector.Float32(); again[0] != 1 {
		t.Fatalf("vector aliased Bytes(): %v", again)
	}
}

func TestVectorEqualComparesBitPatterns(t *testing.T) {
	nan := float32(math.NaN())
	if !NewVectorFloat32([]float32{nan}).Equal(NewVectorFloat32([]float32{nan})) {
		t.Fatal("NaN vectors with identical bits must be equal")
	}
	if NewVectorFloat32([]float32{0}).Equal(NewVectorFloat32([]float32{float32(math.Copysign(0, -1))})) {
		t.Fatal("+0 and -0 differ in bits and must not be equal")
	}
	// Same element bytes, different element type.
	if NewVectorInt32([]int32{0x3f800000}).Equal(NewVectorFloat32([]float32{1})) {
		t.Fatal("vectors of different element types must not be equal")
	}
}

func TestVectorInsideCollectionsUsesVectorParticleTag(t *testing.T) {
	vector := NewVectorFloat32([]float32{1, -2})
	wire := vector.Bytes()

	t.Run("list element is a msgpack str tagged 0x10", func(t *testing.T) {
		got := packed(t, NewValue([]any{vector}))
		// fixarray(1), fixstr(len(wire)+1), particle type, wire.
		want := append([]byte{0x91, 0xa0 | byte(len(wire)+1), ParticleType.VECTOR}, wire...)
		if !bytes.Equal(got, want) {
			t.Fatalf("packed list\n got: % x\nwant: % x", got, want)
		}
	})

	t.Run("nested list and map round-trip to *Vector", func(t *testing.T) {
		packedList := packed(t, NewValue([]any{map[any]any{"emb": vector}, vector}))
		decoded, err := newUnpacker(packedList, 0, len(packedList)).unpackObjects()
		if err != nil {
			t.Fatalf("unpack: %v", err)
		}
		list := decoded.([]any)
		if got, ok := list[1].(*Vector); !ok || !got.Equal(vector) {
			t.Fatalf("list element: %#v", list[1])
		}
		if got, ok := list[0].(map[any]any)["emb"].(*Vector); !ok || !got.Equal(vector) {
			t.Fatalf("map value: %#v", list[0])
		}
	})
}

func TestStoredVectorParticleDecoding(t *testing.T) {
	vector := NewVectorFloat64([]float64{0.5, 0.25})

	t.Run("valid bin value decodes to *Vector", func(t *testing.T) {
		wire := vector.Bytes()
		decoded, err := bytesToParticle(ParticleType.VECTOR, wire, 0, len(wire))
		if err != nil {
			t.Fatal(err)
		}
		if got, ok := decoded.(*Vector); !ok || !got.Equal(vector) {
			t.Fatalf("got %#v", decoded)
		}
	})

	// Older servers, and lists or maps on current ones, store type-16 bytes
	// without checking them. They come back raw instead of failing the stream.
	t.Run("malformed bin value is surfaced raw", func(t *testing.T) {
		decoded, err := bytesToParticle(ParticleType.VECTOR, retiredVectorLayout, 0, len(retiredVectorLayout))
		if err != nil {
			t.Fatal(err)
		}
		rawValue, ok := decoded.(*RawBlobValue)
		if !ok || rawValue.ParticleType != ParticleType.VECTOR || !bytes.Equal(rawValue.Data, retiredVectorLayout) {
			t.Fatalf("got %#v", decoded)
		}

		// Nested in a list it re-packs to exactly what was read.
		packedList := append([]byte{0x91, 0xa0 | byte(len(retiredVectorLayout)+1), ParticleType.VECTOR}, retiredVectorLayout...)
		decoded, err = newUnpacker(packedList, 0, len(packedList)).unpackObjects()
		if err != nil {
			t.Fatal(err)
		}
		if got := packed(t, NewValue(decoded)); !bytes.Equal(got, packedList) {
			t.Fatalf("re-packed\n got: % x\nwant: % x", got, packedList)
		}
	})
}

func TestNewValueMapsVectors(t *testing.T) {
	vector := NewVectorInt32([]int32{7})

	if got, ok := NewValue(vector).(*Vector); !ok || got != vector {
		t.Fatalf("NewValue(*Vector) = %#v", NewValue(vector))
	}
	if got, ok := NewValue(*vector).(*Vector); !ok || !got.Equal(vector) {
		t.Fatalf("NewValue(Vector) = %#v", NewValue(*vector))
	}
	// Plain numeric slices must keep packing as lists, not vectors.
	if particleType := NewValue([]float32{1}).GetType(); particleType != ParticleType.LIST {
		t.Fatalf("NewValue([]float32) type %d, want LIST", particleType)
	}
}

func TestVectorIsNotAValidRecordKey(t *testing.T) {
	vector := NewVectorFloat32([]float32{1})

	if _, err := NewKey("test", "s", vector); err == nil || !err.Matches(types.PARAMETER_ERROR) {
		t.Fatalf("NewKey: %v", err)
	}

	_, err := NewKeyWithDigest("test", "s", vector, make([]byte, 20))
	expectParameterError(t, err, "Invalid key type: Vector")
}
