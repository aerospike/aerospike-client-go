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
	"errors"

	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Unit-style tests (no server connection) filling gaps around the extended-error
// feature that the byte-level ErrorDetailParser suite and the live-server
// integration suites don't reach:
//
//   - ExpressionTrace.String() rendering (all optional-field branches).
//   - recordParser.parseFields with a non-nil Txn - the field-walk that both
//     tracks the MRT and captures field 45 (a different loop from parseFieldsError).
//   - newServerError carrying the parsed *ExpressionTrace onto AerospikeError.
//   - parseExpTrace over msgpack encodings the trace suite doesn't exercise
//     (map16 header, str8 op, array16 path, a non-string path element, uint16 offset).
//
// Reuses the msgpack builders and the parserForDetail / parserWithFields helpers
// from error_detail_parser_test.go (same package).

var _ = gg.Describe("ErrorDetail wired-path gaps (unit)", func() {

	// ------------------------------------------------------------
	// ExpressionTrace.String() - every optional-field branch.
	// ------------------------------------------------------------

	gg.Context("ExpressionTrace.String", func() {
		gg.It("renders <nil> for a nil trace", func() {
			var t *ExpressionTrace
			gm.Expect(t.String()).To(gm.Equal("<nil>"))
		})

		gg.It("renders every field of a full msgpack build trace", func() {
			t := &ExpressionTrace{
				Phase:      ExpTracePhaseBuild,
				ByteOffset: 7,
				Op:         "cmp_eq",
				Depth:      3,
				Path:       []string{"and", "eq"},
				Snippet:    "eq(int,float)",
				Lang:       ExpTraceLangMsgpack,
				AelOffset:  -1,
				AelSpan:    -1,
			}
			gm.Expect(t.String()).To(gm.Equal(
				"ExpressionTrace[phase=1, byteOffset=7, op=cmp_eq, depth=3, path=[and eq], snippet=eq(int,float), lang=1]"))
		})

		gg.It("omits absent optionals (op, path, snippet, ael offsets)", func() {
			t := &ExpressionTrace{
				Phase:      ExpTracePhaseBuild,
				ByteOffset: 7,
				Depth:      3,
				Lang:       ExpTraceLangMsgpack,
				AelOffset:  -1,
				AelSpan:    -1,
			}
			gm.Expect(t.String()).To(gm.Equal(
				"ExpressionTrace[phase=1, byteOffset=7, depth=3, lang=1]"))
		})

		gg.It("includes ael offsets when present", func() {
			t := &ExpressionTrace{
				Phase:      ExpTracePhaseBuild,
				ByteOffset: -1,
				Depth:      -1,
				Lang:       ExpTraceLangAel,
				AelOffset:  42,
				AelSpan:    6,
			}
			gm.Expect(t.String()).To(gm.Equal(
				"ExpressionTrace[phase=1, byteOffset=-1, depth=-1, lang=2, aelOffset=42, aelSpan=6]"))
		})
	})

	// ------------------------------------------------------------
	// parseFields with a non-nil Txn: a distinct field-walk from
	// parseFieldsError that must ALSO capture the field-45 error detail
	// while tracking the record version for the MRT.
	// ------------------------------------------------------------

	gg.Context("parseFields (transaction path)", func() {
		key, keyErr := NewKey("ns", "set", "edp-txn-key")

		gg.It("captures the field-45 detail alongside the record version (read)", func() {
			gm.Expect(keyErr).To(gm.BeNil())

			detail := fixmap2(
				pair(intKey(1), fixint(9)),
				pair(intKey(2), fixstr("cannot append")),
			)
			// A valid 7-byte RECORD_VERSION field followed by the ERROR_MESSAGE field.
			rp := parserWithFields(
				[]FieldType{RECORD_VERSION, ERROR_MESSAGE},
				[][]byte{{1, 2, 3, 4, 5, 6, 7}, detail},
			)

			txn := NewTxn()
			err := rp.parseFields(txn, key, false /* hasWrite */)
			gm.Expect(err).To(gm.BeNil())

			// Field 45 was decoded even though a Txn is tracking the response.
			gm.Expect(rp.serverMessage).To(gm.Equal("cannot append (subcode=9)"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(types.SubCode(9)))
			// The 7-byte version was parsed and recorded on the read set.
			gm.Expect(txn.ReadExistsForKey(key)).To(gm.BeTrue())
		})

		gg.It("captures the field-45 detail on the write path too", func() {
			gm.Expect(keyErr).To(gm.BeNil())

			detail := fixmap1(pair(intKey(1), fixint(3)))
			rp := parserWithFields(
				[]FieldType{RECORD_VERSION, ERROR_MESSAGE},
				[][]byte{{9, 8, 7, 6, 5, 4, 3}, detail},
			)

			txn := NewTxn()
			err := rp.parseFields(txn, key, true /* hasWrite */)
			gm.Expect(err).To(gm.BeNil())

			gm.Expect(rp.serverMessage).To(gm.Equal("error subcode=3"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(types.SubCode(3)))
			gm.Expect(txn.ReadExistsForKey(key)).To(gm.BeTrue())
		})

		gg.It("rejects a record-version field of the wrong size", func() {
			gm.Expect(keyErr).To(gm.BeNil())

			// RECORD_VERSION must be 7 bytes; 5 is a parse error.
			rp := parserWithFields(
				[]FieldType{RECORD_VERSION},
				[][]byte{{1, 2, 3, 4, 5}},
			)

			err := rp.parseFields(NewTxn(), key, false)
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(err.Matches(types.PARSE_ERROR)).To(gm.BeTrue())
		})

		gg.It("with a nil Txn still captures the detail (delegates to parseFieldsError)", func() {
			detail := fixmap2(
				pair(intKey(1), fixint(1)),
				pair(intKey(2), fixstr("nil txn")),
			)
			rp := parserForDetail(detail)

			err := rp.parseFields(nil, nil, false)
			gm.Expect(err).To(gm.BeNil())
			gm.Expect(rp.serverMessage).To(gm.Equal("nil txn (subcode=1)"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(types.SubCode(1)))
		})
	})

	// ------------------------------------------------------------
	// newServerError carries the parsed ExpressionTrace onto the error.
	// ------------------------------------------------------------

	gg.Context("newServerError expression-trace carry-through", func() {
		gg.It("attaches the ExpTrace (build failure: PARAMETER_ERROR + types.SubCodeNone)", func() {
			trace := &ExpressionTrace{
				Phase:      ExpTracePhaseBuild,
				ByteOffset: 7,
				Op:         "cmp_eq",
				Depth:      3,
				Lang:       ExpTraceLangMsgpack,
				AelOffset:  -1,
				AelSpan:    -1,
			}
			err := newServerError(types.PARAMETER_ERROR, "failed to build expression", types.SubCodeNone, trace)

			ae := &AerospikeError{}
			gm.Expect(err.Matches(types.PARAMETER_ERROR)).To(gm.BeTrue())
			gm.Expect(err.Error()).To(gm.ContainSubstring("failed to build expression"))

			gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
			gm.Expect(ae.ResultCode).To(gm.Equal(types.PARAMETER_ERROR))
			gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
			gm.Expect(ae.ServerMessage).To(gm.Equal("failed to build expression"))
			gm.Expect(ae.ExpTrace).To(gm.BeIdenticalTo(trace))
		})

		gg.It("leaves ExpTrace nil when the server sent no trace", func() {
			err := newServerError(types.OP_NOT_APPLICABLE, "index out of bounds (subcode=1)", 1, nil)

			ae := &AerospikeError{}
			gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
			gm.Expect(ae.ExpTrace).To(gm.BeNil())
			gm.Expect(ae.SubCode).To(gm.Equal(types.SubCode(1)))
			gm.Expect(ae.ServerMessage).To(gm.Equal("index out of bounds (subcode=1)"))
		})
	})

	// ------------------------------------------------------------
	// parseExpTrace over msgpack encodings the trace suite doesn't reach.
	// ------------------------------------------------------------

	gg.Context("parseExpTrace msgpack encoding variants", func() {
		gg.It("parses a map16 nested-trace header with a str8 op", func() {
			longOp := "a_very_long_operation_name_exceeding_fixstr" // > 31 bytes => str8
			trace := map16(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyOp), str8(longOp)),
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			gm.Expect(rp.expTrace).NotTo(gm.BeNil())
			gm.Expect(rp.expTrace.Phase).To(gm.Equal(ExpTracePhaseBuild))
			gm.Expect(rp.expTrace.Op).To(gm.Equal(longOp))
		})

		gg.It("parses an array16 path with more than fixarray-many elements", func() {
			elems := make([][]byte, 0, 20)
			expect := make([]string, 0, 20)
			for i := 0; i < 20; i++ {
				s := "op" + string(rune('a'+i))
				elems = append(elems, fixstr(s))
				expect = append(expect, s)
			}
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyDepth), fixint(20)),
				pair(intKey(expTraceKeyPath), array16(elems...)),
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			gm.Expect(rp.expTrace).NotTo(gm.BeNil())
			gm.Expect(rp.expTrace.Path).To(gm.Equal(expect))
		})

		gg.It("leaves an empty slot for a non-string path element", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				// middle element is an int, not a string.
				pair(intKey(expTraceKeyPath), fixarray(fixstr("and"), fixint(5), fixstr("eq"))),
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			gm.Expect(rp.expTrace).NotTo(gm.BeNil())
			gm.Expect(rp.expTrace.Path).To(gm.Equal([]string{"and", "", "eq"}))
		})

		gg.It("parses a uint16-encoded byte offset", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyByteOffset), uint16Val(1100)),
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			gm.Expect(rp.expTrace).NotTo(gm.BeNil())
			gm.Expect(rp.expTrace.ByteOffset).To(gm.Equal(1100))
		})
	})
})

// ---------- local msgpack builders (names distinct from the parser suite) ----------

// map16 builds a map16 (0xDE) with a uint16 count.
func map16(kvs ...[]byte) []byte {
	var out bytes.Buffer
	out.WriteByte(0xDE)
	writeUint16(&out, uint16(len(kvs)))
	for _, kv := range kvs {
		out.Write(kv)
	}
	return out.Bytes()
}

// array16 builds an array16 (0xDC) with a uint16 length.
func array16(elems ...[]byte) []byte {
	var out bytes.Buffer
	out.WriteByte(0xDC)
	writeUint16(&out, uint16(len(elems)))
	for _, e := range elems {
		out.Write(e)
	}
	return out.Bytes()
}

// str8 builds a str8 (0xD9) msgpack string (length 0-255).
func str8(s string) []byte {
	data := []byte(s)
	var out bytes.Buffer
	out.WriteByte(0xD9)
	out.WriteByte(byte(len(data)))
	out.Write(data)
	return out.Bytes()
}

// uint16Val builds a uint16 (0xCD) msgpack integer.
func uint16Val(v uint16) []byte {
	var out bytes.Buffer
	out.WriteByte(0xCD)
	writeUint16(&out, v)
	return out.Bytes()
}
