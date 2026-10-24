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
	"encoding/binary"
	"fmt"
	"math"
	"strings"

	"github.com/aerospike/aerospike-client-go/v8/types"
	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

// VectorElementType identifies the numeric type of every element of a Vector.
type VectorElementType byte

const (
	// VectorElementFloat16 is an IEEE 754 half precision element (2 bytes).
	VectorElementFloat16 VectorElementType = 0x01
	// VectorElementInt32 is a signed 32 bit integer element (4 bytes).
	VectorElementInt32 VectorElementType = 0x02
	// VectorElementFloat32 is an IEEE 754 single precision element (4 bytes).
	VectorElementFloat32 VectorElementType = 0x03
	// VectorElementFloat64 is an IEEE 754 double precision element (8 bytes).
	VectorElementFloat64 VectorElementType = 0x04
)

// size returns the encoded size of one element, or 0 for an unknown type.
func (elementType VectorElementType) size() int {
	switch elementType {
	case VectorElementFloat16:
		return 2
	case VectorElementInt32, VectorElementFloat32:
		return 4
	case VectorElementFloat64:
		return 8
	}
	return 0
}

// String implements the Stringer interface.
func (elementType VectorElementType) String() string {
	switch elementType {
	case VectorElementFloat16:
		return "float16"
	case VectorElementInt32:
		return "int32"
	case VectorElementFloat32:
		return "float32"
	case VectorElementFloat64:
		return "float64"
	}
	return fmt.Sprintf("VectorElementType(%d)", byte(elementType))
}

// maxDimensions returns the most elements a vector of this type can hold,
// or 0 for an unknown type.
func (elementType VectorElementType) maxDimensions() int {
	if size := elementType.size(); size > 0 {
		return vectorMaxElementBytes / size
	}
	return 0
}

const (
	// vectorFormatVersion is the only vector layout the server accepts.
	vectorFormatVersion = 1
	// vectorHeaderSize is version(1) + element type(1) + dimensions(4, little-endian).
	vectorHeaderSize = 6
	// vectorMaxElementBytes is the server cap on a vector's element payload (256 KiB).
	vectorMaxElementBytes = 256 * 1024
)

// Vector is a dense, fixed-dimension numeric vector stored in a native
// vector bin (server particle type 16).
//
// A Vector is immutable after construction and safe for concurrent use.
// It is stored on the wire as:
//
//	| version (1) | element type (1) | dimensions (4, little-endian) | elements (little-endian) |
//
// Vector values require a server that supports native vectors, see
// ExpVectorDistance for similarity scoring. NewValue and NewBin wrap a
// *Vector in a VectorValue to send it.
type Vector struct {
	// wire holds the complete serialized vector, header included.
	wire []byte
}

func newVector(elementType VectorElementType, dimensions int) (*Vector, Error) {
	if err := checkVectorDimensions(elementType, dimensions); err != nil {
		return nil, err
	}
	wire := make([]byte, vectorHeaderSize+(dimensions*elementType.size()))
	wire[0] = vectorFormatVersion
	wire[1] = byte(elementType)
	binary.LittleEndian.PutUint32(wire[2:], uint32(dimensions))
	return &Vector{wire: wire}, nil
}

// checkVectorDimensions refuses a dimension count the server would reject:
// none, or more than the element type allows.
func checkVectorDimensions(elementType VectorElementType, dimensions int) Error {
	if dimensions < 1 {
		return newError(types.PARAMETER_ERROR, "vector: must have at least 1 dimension")
	}
	if maxDimensions := elementType.maxDimensions(); dimensions > maxDimensions {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("vector: %d dimensions exceed max %d for element type %d", dimensions, maxDimensions, byte(elementType)))
	}
	return nil
}

// NewVectorFloat32 creates a vector of 32 bit float elements.
// The input slice is copied.
//
// It returns a PARAMETER_ERROR if elements is empty or holds more than 65,536 elements.
func NewVectorFloat32(elements []float32) (*Vector, Error) {
	vector, err := newVector(VectorElementFloat32, len(elements))
	if err != nil {
		return nil, err
	}
	payload := vector.wire[vectorHeaderSize:]
	for index, element := range elements {
		binary.LittleEndian.PutUint32(payload[index*4:], math.Float32bits(element))
	}
	return vector, nil
}

// NewVectorFloat64 creates a vector of 64 bit float elements.
// The input slice is copied.
//
// It returns a PARAMETER_ERROR if elements is empty or holds more than 32,768 elements.
func NewVectorFloat64(elements []float64) (*Vector, Error) {
	vector, err := newVector(VectorElementFloat64, len(elements))
	if err != nil {
		return nil, err
	}
	payload := vector.wire[vectorHeaderSize:]
	for index, element := range elements {
		binary.LittleEndian.PutUint64(payload[index*8:], math.Float64bits(element))
	}
	return vector, nil
}

// NewVectorInt32 creates a vector of signed 32 bit integer elements.
// The input slice is copied.
//
// It returns a PARAMETER_ERROR if elements is empty or holds more than 65,536 elements.
func NewVectorInt32(elements []int32) (*Vector, Error) {
	vector, err := newVector(VectorElementInt32, len(elements))
	if err != nil {
		return nil, err
	}
	payload := vector.wire[vectorHeaderSize:]
	for index, element := range elements {
		binary.LittleEndian.PutUint32(payload[index*4:], uint32(element))
	}
	return vector, nil
}

// NewVectorFloat16Bits creates a vector of IEEE 754 half precision elements
// from their raw 16 bit patterns. No conversion is performed.
// The input slice is copied.
//
// It returns a PARAMETER_ERROR if bits is empty or holds more than 131,072 elements.
func NewVectorFloat16Bits(bits []uint16) (*Vector, Error) {
	vector, err := newVector(VectorElementFloat16, len(bits))
	if err != nil {
		return nil, err
	}
	payload := vector.wire[vectorHeaderSize:]
	for index, element := range bits {
		binary.LittleEndian.PutUint16(payload[index*2:], element)
	}
	return vector, nil
}

// NewVectorFromBytes decodes a complete serialized vector (header included),
// as returned by Vector.Bytes. The input is copied.
//
// It returns a PARAMETER_ERROR if the bytes are not a complete version-1 vector
// of a known type and allowed size.
func NewVectorFromBytes(serialized []byte) (*Vector, Error) {
	if err := validateVectorWire(serialized); err != nil {
		return nil, err
	}
	wire := make([]byte, len(serialized))
	copy(wire, serialized)
	return &Vector{wire: wire}, nil
}

func validateVectorWire(serialized []byte) Error {
	if len(serialized) < vectorHeaderSize {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("vector: %d bytes is shorter than the %d byte header", len(serialized), vectorHeaderSize))
	}
	if serialized[0] != vectorFormatVersion {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("vector: unsupported format version %d, expected %d", serialized[0], vectorFormatVersion))
	}
	elementType := VectorElementType(serialized[1])
	elementSize := elementType.size()
	if elementSize == 0 {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("vector: unknown element type %d", serialized[1]))
	}
	dimensions := int64(binary.LittleEndian.Uint32(serialized[2:]))
	if err := checkVectorDimensions(elementType, int(dimensions)); err != nil {
		return err
	}
	if expectedSize := int64(vectorHeaderSize) + dimensions*int64(elementSize); int64(len(serialized)) != expectedSize {
		return newError(types.PARAMETER_ERROR, fmt.Sprintf("vector: %d bytes does not match %d %s elements (expected %d bytes)", len(serialized), dimensions, elementType, expectedSize))
	}
	return nil
}

// ElementType returns the element type, or 0 for a zero Vector that was not
// built with a constructor.
func (vector *Vector) ElementType() VectorElementType {
	if len(vector.wire) < vectorHeaderSize {
		return 0
	}
	return VectorElementType(vector.wire[1])
}

// Dimensions returns the number of elements.
func (vector *Vector) Dimensions() int {
	if len(vector.wire) < vectorHeaderSize {
		return 0
	}
	return int(binary.LittleEndian.Uint32(vector.wire[2:]))
}

// Bytes returns a copy of the complete serialized vector, header included.
func (vector *Vector) Bytes() []byte {
	serialized := make([]byte, len(vector.wire))
	copy(serialized, vector.wire)
	return serialized
}

// Equal reports whether both vectors have identical serialized bytes: the
// same element type, dimensions and element bit patterns. Floats compare by
// bits, so -0.0 does not equal 0.0 and NaNs are equal only when their bit
// patterns are.
func (vector *Vector) Equal(other *Vector) bool {
	if vector == nil || other == nil {
		return vector == other
	}
	return bytes.Equal(vector.wire, other.wire)
}

func (vector *Vector) elementBytes(elementType VectorElementType) ([]byte, Error) {
	if vector.ElementType() != elementType {
		return nil, newError(types.PARAMETER_ERROR, fmt.Sprintf("vector: element type is %s, not %s", vector.ElementType(), elementType))
	}
	return vector.wire[vectorHeaderSize:], nil
}

// Float32 returns a copy of the elements of a float32 vector.
func (vector *Vector) Float32() ([]float32, Error) {
	payload, err := vector.elementBytes(VectorElementFloat32)
	if err != nil {
		return nil, err
	}
	elements := make([]float32, vector.Dimensions())
	for index := range elements {
		elements[index] = math.Float32frombits(binary.LittleEndian.Uint32(payload[index*4:]))
	}
	return elements, nil
}

// Float64 returns a copy of the elements of a float64 vector.
func (vector *Vector) Float64() ([]float64, Error) {
	payload, err := vector.elementBytes(VectorElementFloat64)
	if err != nil {
		return nil, err
	}
	elements := make([]float64, vector.Dimensions())
	for index := range elements {
		elements[index] = math.Float64frombits(binary.LittleEndian.Uint64(payload[index*8:]))
	}
	return elements, nil
}

// Int32 returns a copy of the elements of an int32 vector.
func (vector *Vector) Int32() ([]int32, Error) {
	payload, err := vector.elementBytes(VectorElementInt32)
	if err != nil {
		return nil, err
	}
	elements := make([]int32, vector.Dimensions())
	for index := range elements {
		elements[index] = int32(binary.LittleEndian.Uint32(payload[index*4:]))
	}
	return elements, nil
}

// Float16Bits returns a copy of the raw IEEE 754 half precision bit patterns
// of a float16 vector.
func (vector *Vector) Float16Bits() ([]uint16, Error) {
	payload, err := vector.elementBytes(VectorElementFloat16)
	if err != nil {
		return nil, err
	}
	elements := make([]uint16, vector.Dimensions())
	for index := range elements {
		elements[index] = binary.LittleEndian.Uint16(payload[index*2:])
	}
	return elements, nil
}

// checkDimensions refuses to send a vector the constructors would not build,
// such as a zero Vector{}.
func (vector *Vector) checkDimensions() Error {
	return checkVectorDimensions(vector.ElementType(), vector.Dimensions())
}

// luaVectorValue is the value client-side Lua sees for a vector in a stream
// aggregation. Go's client-side Lua has no bytes type, so a vector reaches it
// as a list of its serialized bytes.
func luaVectorValue(vector *Vector) []byte {
	return vector.Bytes()
}

// String implements the Stringer interface, e.g. "float32[1 2 3]".
func (vector *Vector) String() string {
	var builder strings.Builder
	builder.WriteString(vector.ElementType().String())
	switch vector.ElementType() {
	case VectorElementFloat32:
		elements, _ := vector.Float32()
		fmt.Fprint(&builder, elements)
	case VectorElementFloat64:
		elements, _ := vector.Float64()
		fmt.Fprint(&builder, elements)
	case VectorElementInt32:
		elements, _ := vector.Int32()
		fmt.Fprint(&builder, elements)
	case VectorElementFloat16:
		elements, _ := vector.Float16Bits()
		fmt.Fprintf(&builder, "%#04x", elements)
	}
	return builder.String()
}

// VectorValue is the Value that sends a vector: a bin value, a UDF argument or
// an operation value. Reads return the *Vector itself.
type VectorValue struct {
	vector *Vector
}

var _ Value = VectorValue{}

// NewVectorValue wraps a vector to send it. NewValue does the same for a
// *Vector.
func NewVectorValue(vector *Vector) VectorValue {
	return VectorValue{vector: vector}
}

// EstimateSize returns the size of the vector in wire protocol.
func (vectorValue VectorValue) EstimateSize() (int, Error) {
	if vectorValue.vector == nil {
		return 0, nil
	}
	return len(vectorValue.vector.wire), nil
}

func (vectorValue VectorValue) write(cmd BufferEx) (int, Error) {
	if err := vectorValue.checkVector(); err != nil {
		return 0, err
	}
	if err := markVector(cmd); err != nil {
		return 0, err
	}
	return cmd.Write(vectorValue.vector.wire)
}

func (vectorValue VectorValue) pack(cmd BufferEx) (int, Error) {
	if err := vectorValue.checkVector(); err != nil {
		return 0, err
	}
	if err := markVector(cmd); err != nil {
		return 0, err
	}
	return packVector(cmd, vectorValue.vector.wire)
}

// checkVector refuses to send a nil vector, or one the constructors would not
// build, such as a zero Vector{}.
func (vectorValue VectorValue) checkVector() Error {
	if vectorValue.vector == nil {
		return newError(types.PARAMETER_ERROR, "vector: value must not be nil")
	}
	return vectorValue.vector.checkDimensions()
}

// GetType returns wire protocol value type.
func (vectorValue VectorValue) GetType() int {
	return ParticleType.VECTOR
}

// GetObject returns the *Vector.
func (vectorValue VectorValue) GetObject() any {
	return vectorValue.vector
}

// String implements the Stringer interface, e.g. "float32[1 2 3]".
func (vectorValue VectorValue) String() string {
	if vectorValue.vector == nil {
		return "<nil>"
	}
	return vectorValue.vector.String()
}
