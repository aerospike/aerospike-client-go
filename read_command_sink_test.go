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
	"reflect"
	"testing"

	ParticleType "github.com/aerospike/aerospike-client-go/v8/types/particle_type"
)

// op wire layout per record_parser.go:170-201 and read_command_reflect.go:79-93:
//
//	[opSize uint32 BE] [byte] [byte] [particleType byte] [byte] [byte] [byte] [nameSize byte]
//	[name nameSize bytes] [particle particleBytesSize bytes]
//
// opSize == 4 + nameSize + particleBytesSize.
type wireOp struct {
	name         string
	particleType int
	particle     []byte
}

func encodeOps(ops []wireOp) []byte {
	var out []byte
	for _, op := range ops {
		nameSize := len(op.name)
		particleSize := len(op.particle)
		opSize := uint32(4 + nameSize + particleSize)
		header := make([]byte, 8)
		binary.BigEndian.PutUint32(header[0:4], opSize)
		header[5] = byte(op.particleType)
		header[7] = byte(nameSize)
		out = append(out, header...)
		out = append(out, []byte(op.name)...)
		out = append(out, op.particle...)
	}
	return out
}

// encodeInt8Particle encodes a small signed int as a single byte; the
// VarBytesToInt64 path supports 1, 2, 4 and 8 byte widths.
func encodeInt8Particle(v int64) []byte {
	return []byte{byte(v)}
}

// recordedBin captures a single AerospikeBin callback for assertions.
type recordedBin struct {
	name  string
	value any
}

type recordingSink struct {
	bins    []recordedBin
	gen     uint32
	exp     uint32
	hadMeta bool
}

func (r *recordingSink) AerospikeBin(name string, value any) Error {
	r.bins = append(r.bins, recordedBin{name, value})
	return nil
}

func (r *recordingSink) AerospikeMetadata(generation, expiration uint32) Error {
	r.hadMeta = true
	r.gen = generation
	r.exp = expiration
	return nil
}

func newSinkTestCommand(buf []byte) *baseReadCommand {
	brc := &baseReadCommand{}
	brc.dataBuffer = buf
	brc.dataOffset = 0
	return brc
}

func TestParseSink_BasicTypes(t *testing.T) {
	ops := []wireOp{
		{name: "i", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(42)},
		{name: "s", particleType: ParticleType.STRING, particle: []byte("hello")},
		{name: "blob", particleType: ParticleType.BLOB, particle: []byte{0xde, 0xad, 0xbe, 0xef}},
	}
	buf := encodeOps(ops)

	sink := &recordingSink{}
	brc := newSinkTestCommand(buf)
	brc.sink = sink

	if err := parseSink(brc, len(ops), 0, 7, 99); err != nil {
		t.Fatalf("parseSink returned error: %v", err)
	}

	if !sink.hadMeta {
		t.Fatalf("expected metadata callback")
	}
	if sink.gen != 7 || sink.exp != 99 {
		t.Fatalf("metadata mismatch: gen=%d exp=%d", sink.gen, sink.exp)
	}

	if len(sink.bins) != 3 {
		t.Fatalf("expected 3 bins, got %d", len(sink.bins))
	}

	expectInt(t, sink.bins[0], "i", 42)
	expectStr(t, sink.bins[1], "s", "hello")
	expectBlob(t, sink.bins[2], "blob", []byte{0xde, 0xad, 0xbe, 0xef})
}

// receiverWithoutMetadata only implements BinReceiver — no metadata
// capability — so the parser must not invoke a metadata callback.
type receiverWithoutMetadata struct {
	bins []recordedBin
}

func (r *receiverWithoutMetadata) AerospikeBin(name string, value any) Error {
	r.bins = append(r.bins, recordedBin{name, value})
	return nil
}

func TestParseSink_NoMetadataCapability(t *testing.T) {
	ops := []wireOp{
		{name: "x", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(1)},
	}
	buf := encodeOps(ops)
	sink := &receiverWithoutMetadata{}
	brc := newSinkTestCommand(buf)
	brc.sink = sink

	if err := parseSink(brc, len(ops), 0, 5, 5); err != nil {
		t.Fatal(err)
	}
	if len(sink.bins) != 1 {
		t.Fatalf("expected 1 bin, got %d", len(sink.bins))
	}
}

// errorSink returns an error on the second bin to verify error propagation.
type errorSink struct {
	count int
	limit int
}

func (e *errorSink) AerospikeBin(name string, value any) Error {
	e.count++
	if e.count >= e.limit {
		return ErrInvalidParam.err()
	}
	return nil
}

func TestParseSink_PropagatesError(t *testing.T) {
	ops := []wireOp{
		{name: "a", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(1)},
		{name: "b", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(2)},
		{name: "c", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(3)},
	}
	buf := encodeOps(ops)
	sink := &errorSink{limit: 2}
	brc := newSinkTestCommand(buf)
	brc.sink = sink

	if err := parseSink(brc, len(ops), 0, 0, 0); err == nil {
		t.Fatalf("expected error from sink, got nil")
	}
	if sink.count != 2 {
		t.Fatalf("expected parser to stop after 2 callbacks, got %d", sink.count)
	}
}

func TestParseSink_SkipsFields(t *testing.T) {
	// Build a fake field section: two fields, each prefixed with a 4-byte
	// length, followed by one INTEGER bin.
	field1 := append(uint32BE(3), []byte{0x01, 0x02, 0x03}...)
	field2 := append(uint32BE(5), []byte{0x04, 0x05, 0x06, 0x07, 0x08}...)
	bins := encodeOps([]wireOp{{name: "z", particleType: ParticleType.INTEGER, particle: encodeInt8Particle(99)}})

	buf := append(append(field1, field2...), bins...)
	sink := &recordingSink{}
	brc := newSinkTestCommand(buf)
	brc.sink = sink

	if err := parseSink(brc, 1, 2, 1, 1); err != nil {
		t.Fatal(err)
	}
	if len(sink.bins) != 1 {
		t.Fatalf("expected 1 bin after skipping fields, got %d", len(sink.bins))
	}
	expectInt(t, sink.bins[0], "z", 99)
}

func uint32BE(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func expectInt(t *testing.T, got recordedBin, name string, want int64) {
	t.Helper()
	if got.name != name {
		t.Fatalf("bin name: want %q, got %q", name, got.name)
	}
	// bytesToParticle returns `int` on 64-bit platforms, `int64` otherwise.
	switch v := got.value.(type) {
	case int:
		if int64(v) != want {
			t.Fatalf("bin %q: want %d, got %d", name, want, v)
		}
	case int64:
		if v != want {
			t.Fatalf("bin %q: want %d, got %d", name, want, v)
		}
	default:
		t.Fatalf("bin %q: unexpected type %T", name, got.value)
	}
}

func expectStr(t *testing.T, got recordedBin, name, want string) {
	t.Helper()
	if got.name != name {
		t.Fatalf("bin name: want %q, got %q", name, got.name)
	}
	if got.value != want {
		t.Fatalf("bin %q: want %q, got %v", name, want, got.value)
	}
}

func expectBlob(t *testing.T, got recordedBin, name string, want []byte) {
	t.Helper()
	if got.name != name {
		t.Fatalf("bin name: want %q, got %q", name, got.name)
	}
	if !reflect.DeepEqual(got.value, want) {
		t.Fatalf("bin %q: want %v, got %v", name, want, got.value)
	}
}
