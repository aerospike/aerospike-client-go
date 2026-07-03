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
	"strings"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Unit-style tests that exercise recordParser's msgpack error-detail decoding
// by feeding it synthetic buffers. No server connection required.
//
// Also verifies the info4 verbosity bit math
// (_INFO4_ERROR_VERBOSITY_SHIFT / _INFO4_ERROR_VERBOSITY_MASK).

var _ = gg.Describe("ErrorDetailParser (unit)", func() {

	// ------------------------------------------------------------
	// Verbosity bit math.
	// ------------------------------------------------------------

	gg.Context("verbosity bit math", func() {
		gg.It("has consistent shift and mask constants", func() {
			gm.Expect(_INFO4_ERROR_VERBOSITY_SHIFT).To(gm.Equal(5))
			gm.Expect(_INFO4_ERROR_VERBOSITY_MASK).To(gm.Equal(0x60))
			// Mask must cover exactly two bits at the shift position.
			gm.Expect(0x03 << _INFO4_ERROR_VERBOSITY_SHIFT).To(gm.Equal(_INFO4_ERROR_VERBOSITY_MASK))
		})

		gg.It("preserves in-range verbosity values after masking", func() {
			for v := 0; v <= 3; v++ {
				actual := (v << _INFO4_ERROR_VERBOSITY_SHIFT) & _INFO4_ERROR_VERBOSITY_MASK
				gm.Expect(actual).To(gm.Equal(v << _INFO4_ERROR_VERBOSITY_SHIFT))
			}
		})

		gg.It("out-of-range verbosity cannot corrupt other info4 bits", func() {
			otherBits := (^_INFO4_ERROR_VERBOSITY_MASK) & 0xFF
			for _, v := range []int{0, 1, 2, 3, 4, 8, 16, 255} {
				written := (v << _INFO4_ERROR_VERBOSITY_SHIFT) & _INFO4_ERROR_VERBOSITY_MASK
				gm.Expect(written & otherBits).To(gm.Equal(0))
				gm.Expect(written).To(gm.Equal(written & _INFO4_ERROR_VERBOSITY_MASK))
			}
			// Pre-mask these set bits OUTSIDE 5-6; result is 0.
			gm.Expect((4 << _INFO4_ERROR_VERBOSITY_SHIFT) & _INFO4_ERROR_VERBOSITY_MASK).To(gm.Equal(0))
			gm.Expect((8 << _INFO4_ERROR_VERBOSITY_SHIFT) & _INFO4_ERROR_VERBOSITY_MASK).To(gm.Equal(0))
			gm.Expect((16 << _INFO4_ERROR_VERBOSITY_SHIFT) & _INFO4_ERROR_VERBOSITY_MASK).To(gm.Equal(0))
		})
	})

	// ------------------------------------------------------------
	// Parser: fixmap (baseline).
	// ------------------------------------------------------------

	gg.Context("fixmap baseline", func() {
		gg.It("parses fixmap with subcode and message", func() {
			detail := fixmap2(
				pair(intKey(1), fixint(99)),
				pair(intKey(2), fixstr("cannot append")),
			)
			rp := parserForDetail(detail)
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("cannot append (subcode=99)"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(99))
		})

		gg.It("parses fixmap with subcode only", func() {
			detail := fixmap1(pair(intKey(1), fixint(42)))
			rp := parserForDetail(detail)
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("error subcode=42"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(42))
		})

		gg.It("parses fixmap with message only", func() {
			detail := fixmap1(pair(intKey(2), fixstr("oops")))
			rp := parserForDetail(detail)
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("oops"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(SubCodeNone))
		})

		gg.It("parses keys in reverse order", func() {
			detail := fixmap2(
				pair(intKey(2), fixstr("swap")),
				pair(intKey(1), fixint(7)),
			)
			rp := parserForDetail(detail)
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("swap (subcode=7)"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(7))
		})

		gg.It("parses multi-byte UTF-8 message", func() {
			multibyte := "αβγ · 测试 · 🚀"
			detail := fixmap2(
				pair(intKey(1), fixint(1)),
				pair(intKey(2), fixstr(multibyte)),
			)
			rp := parserForDetail(detail)
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal(multibyte + " (subcode=1)"))
		})
	})

	// ------------------------------------------------------------
	// Parser: msgpack types that the original hand-rolled decoder didn't cover.
	// ------------------------------------------------------------

	gg.Context("msgpack header / value variants", func() {
		gg.It("parses map16 header", func() {
			var payload bytes.Buffer
			payload.WriteByte(0xDE)
			payload.WriteByte(0x00)
			payload.WriteByte(16)
			payload.Write(pair(intKey(1), fixint(7)))
			payload.Write(pair(intKey(2), fixstr("boom")))
			for i := 0; i < 14; i++ {
				// unknown key, uint8, nil value
				payload.WriteByte(0xCC)
				payload.WriteByte(byte(100 + i))
				payload.WriteByte(0xC0)
			}
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("boom (subcode=7)"))
		})

		gg.It("parses map32 header", func() {
			var payload bytes.Buffer
			payload.WriteByte(0xDF)
			writeInt32(&payload, 2)
			payload.Write(pair(intKey(1), fixint(9)))
			payload.Write(pair(intKey(2), fixstr("m32")))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("m32 (subcode=9)"))
		})

		gg.It("parses str32 message", func() {
			big := strings.Repeat("x", 100)
			var payload bytes.Buffer
			payload.WriteByte(0x82) // fixmap, 2 entries
			payload.Write(pair(intKey(1), fixint(5)))
			payload.Write(intKey(2))
			payload.WriteByte(0xDB)
			payload.WriteByte(0x00)
			payload.WriteByte(0x00)
			payload.WriteByte(0x00)
			payload.WriteByte(byte(len(big)))
			payload.Write([]byte(big))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal(big + " (subcode=5)"))
		})

		gg.It("parses subcode as fixint", func() {
			detail := fixmap2(
				pair(intKey(1), fixint(127)),
				pair(intKey(2), fixstr("fx")),
			)
			rp := parserForDetail(detail)
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("fx (subcode=127)"))
		})

		gg.It("parses subcode as uint8", func() {
			var payload bytes.Buffer
			payload.WriteByte(0x82)
			payload.Write(intKey(1))
			payload.WriteByte(0xCC)
			payload.WriteByte(200)
			payload.Write(pair(intKey(2), fixstr("u8")))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("u8 (subcode=200)"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(200))
		})

		gg.It("parses subcode as uint16", func() {
			var payload bytes.Buffer
			payload.WriteByte(0x82)
			payload.Write(intKey(1))
			payload.WriteByte(0xCD)
			payload.WriteByte(0x04)
			payload.WriteByte(0x4C)
			payload.Write(pair(intKey(2), fixstr("hi")))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("hi (subcode=1100)"))
		})

		gg.It("parses subcode as uint32", func() {
			var payload bytes.Buffer
			payload.WriteByte(0x82)
			payload.Write(intKey(1))
			payload.WriteByte(0xCE)
			payload.WriteByte(0x00)
			payload.WriteByte(0x01)
			payload.WriteByte(0x11)
			payload.WriteByte(0x70)
			payload.Write(pair(intKey(2), fixstr("x")))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("x (subcode=70000)"))
		})

		gg.It("parses subcode as uint64", func() {
			value := int64(5_000_000_000)
			var payload bytes.Buffer
			payload.WriteByte(0x82)
			payload.Write(intKey(1))
			payload.WriteByte(0xCF)
			writeInt64(&payload, value)
			payload.Write(pair(intKey(2), fixstr("u64")))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.HavePrefix("u64 (subcode="))
			gm.Expect(rp.serverSubcode).To(gm.Equal(int(value)))
		})

		gg.It("parses message as str8", func() {
			msg := "string8"
			var payload bytes.Buffer
			payload.WriteByte(0x82)
			payload.Write(pair(intKey(1), fixint(3)))
			payload.Write(intKey(2))
			payload.WriteByte(0xD9)
			payload.WriteByte(byte(len(msg)))
			payload.Write([]byte(msg))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal(msg + " (subcode=3)"))
		})

		gg.It("parses message as str16", func() {
			msg := "string16"
			var payload bytes.Buffer
			payload.WriteByte(0x82)
			payload.Write(pair(intKey(1), fixint(4)))
			payload.Write(intKey(2))
			payload.WriteByte(0xDA)
			writeUint16(&payload, uint16(len(msg)))
			payload.Write([]byte(msg))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal(msg + " (subcode=4)"))
		})
	})

	// ------------------------------------------------------------
	// Defensive / edge cases.
	// ------------------------------------------------------------

	gg.Context("defensive edge cases", func() {
		gg.It("empty map produces no message", func() {
			rp := parserForDetail([]byte{0x80})
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal(""))
			gm.Expect(rp.serverSubcode).To(gm.Equal(SubCodeNone))
		})

		gg.It("truncated value does not panic", func() {
			rp := parserForDetail([]byte{0x81, 0x01, 0xCD})
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal(""))
		})

		gg.It("truncated map header returns no message", func() {
			rp := parserForDetail([]byte{0xDE})
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal(""))
		})

		gg.It("unknown keys are skipped, not fatal", func() {
			var payload bytes.Buffer
			payload.WriteByte(0x84)                    // fixmap, 4 entries
			payload.Write(pair(intKey(50), fixint(0))) // unknown
			payload.Write(pair(intKey(1), fixint(7)))
			payload.Write(intKey(51))
			payload.WriteByte(0xC0) // nil value
			payload.Write(pair(intKey(2), fixstr("z")))
			rp := parserForDetail(payload.Bytes())
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("z (subcode=7)"))
		})

		gg.It("non-error-message field types are skipped", func() {
			// fieldCount = 2 with an unknown field type followed by ERROR_MESSAGE.
			detail := fixmap2(
				pair(intKey(1), fixint(1)),
				pair(intKey(2), fixstr("ok")),
			)
			rp := parserWithFields(
				[]FieldType{FieldType(99), ERROR_MESSAGE},
				[][]byte{{0x01, 0x02, 0x03}, detail},
			)
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal("ok (subcode=1)"))
		})

		gg.It("returns no message when ERROR_MESSAGE field is absent", func() {
			rp := parserWithFields(nil, nil)
			rp.parseFieldsError()
			gm.Expect(rp.serverMessage).To(gm.Equal(""))
		})
	})

	// ------------------------------------------------------------
	// Parser: verbosity-3 expression trace (nested key-3 map).
	// ------------------------------------------------------------

	gg.Context("verbosity-3 expression trace", func() {
		gg.It("parses a full expression trace alongside the message", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyByteOffset), fixint(7)),
				pair(intKey(expTraceKeyOp), fixstr("cmp_eq")),
				pair(intKey(expTraceKeyDepth), fixint(3)),
				pair(intKey(expTraceKeyPath), fixarray(fixstr("and"), fixstr("eq"), fixstr("cmp_eq"))),
				pair(intKey(expTraceKeySnippet), fixstr("eq(int,float)")),
			)
			detail := fixmapN(
				pair(intKey(2), fixstr("bad exp")),
				pair(intKey(asErrorDetailKeyExpTrace), trace),
			)
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			// Message still surfaces unchanged; subcode absent (no key 1).
			gm.Expect(rp.serverMessage).To(gm.Equal("bad exp"))
			gm.Expect(rp.serverSubcode).To(gm.Equal(SubCodeNone))

			t := rp.expTrace
			gm.Expect(t).NotTo(gm.BeNil())
			gm.Expect(t.Phase).To(gm.Equal(ExpTracePhaseBuild))
			gm.Expect(t.ByteOffset).To(gm.Equal(7))
			gm.Expect(t.Op).To(gm.Equal("cmp_eq"))
			gm.Expect(t.Depth).To(gm.Equal(3))
			gm.Expect(t.Snippet).To(gm.Equal("eq(int,float)"))
			// path order preserved root -> fault.
			gm.Expect(t.Path).To(gm.Equal([]string{"and", "eq", "cmp_eq"}))
			// lang absent => msgpack default.
			gm.Expect(t.Lang).To(gm.Equal(ExpTraceLangMsgpack))
		})

		gg.It("keeps the '...' path-truncation sentinel and reports the true depth", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyDepth), fixint(20)),
				pair(intKey(expTraceKeyPath),
					fixarray(fixstr("and"), fixstr("or"), fixstr("..."), fixstr("cmp_eq"))),
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			t := rp.expTrace
			gm.Expect(t).NotTo(gm.BeNil())
			// depth reports the TRUE count, not the truncated path length.
			gm.Expect(t.Depth).To(gm.Equal(20))
			gm.Expect(t.Path).To(gm.HaveLen(4))
			gm.Expect(t.Path[2]).To(gm.Equal(ExpTracePathTruncationSentinel))
			gm.Expect(t.Path[2]).To(gm.Equal("..."))
			// order preserved around the sentinel.
			gm.Expect(t.Path[0]).To(gm.Equal("and"))
			gm.Expect(t.Path[3]).To(gm.Equal("cmp_eq"))
		})

		gg.It("tolerates snippet and path absent within a present trace", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyByteOffset), fixint(12)),
				pair(intKey(expTraceKeyOp), fixstr("add")),
				pair(intKey(expTraceKeyDepth), fixint(2)),
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			t := rp.expTrace
			gm.Expect(t).NotTo(gm.BeNil())
			gm.Expect(t.Phase).To(gm.Equal(ExpTracePhaseBuild))
			gm.Expect(t.ByteOffset).To(gm.Equal(12))
			gm.Expect(t.Op).To(gm.Equal("add"))
			gm.Expect(t.Depth).To(gm.Equal(2))
			gm.Expect(t.Snippet).To(gm.Equal("")) // snippet absent
			gm.Expect(t.Path).To(gm.BeNil())      // path absent
		})

		gg.It("skips unknown / reserved trace keys without corrupting known fields", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyOutcome), fixint(5)), // 7 reserved
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyAelLine), fixint(9)), // 11 reserved
				pair(intKey(expTraceKeyByteOffset), fixint(4)),
				pair(intKey(expTraceKeyAelCol), fixint(2)), // 12 reserved
				pair(intKey(99), fixstr("ignored")),        // wholly unknown
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			t := rp.expTrace
			gm.Expect(t).NotTo(gm.BeNil())
			gm.Expect(t.Phase).To(gm.Equal(ExpTracePhaseBuild))
			gm.Expect(t.ByteOffset).To(gm.Equal(4))
			// unknown keys did not corrupt absent fields.
			gm.Expect(t.Op).To(gm.Equal(""))
			gm.Expect(t.Depth).To(gm.Equal(-1))
		})

		gg.It("treats an absent lang as msgpack", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyByteOffset), fixint(1)),
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			t := rp.expTrace
			gm.Expect(t).NotTo(gm.BeNil())
			gm.Expect(t.Lang).To(gm.Equal(ExpTraceLangMsgpack))
			gm.Expect(t.AelOffset).To(gm.Equal(-1))
			gm.Expect(t.AelSpan).To(gm.Equal(-1))
		})

		gg.It("exposes lang=AEL with its offsets when present", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyLang), fixint(ExpTraceLangAel)),
				pair(intKey(expTraceKeyAelOffset), fixint(42)),
				pair(intKey(expTraceKeyAelSpan), fixint(6)),
			)
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), trace))
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			t := rp.expTrace
			gm.Expect(t).NotTo(gm.BeNil())
			gm.Expect(t.Lang).To(gm.Equal(ExpTraceLangAel))
			gm.Expect(t.AelOffset).To(gm.Equal(42))
			gm.Expect(t.AelSpan).To(gm.Equal(6))
		})

		gg.It("leaves expTrace nil for a plain subcode+message response", func() {
			detail := fixmap2(
				pair(intKey(1), fixint(4)),
				pair(intKey(2), fixstr("plain")),
			)
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			gm.Expect(rp.serverMessage).To(gm.Equal("plain (subcode=4)"))
			gm.Expect(rp.expTrace).To(gm.BeNil()) // no key 3 => no expression trace
		})

		gg.It("surfaces the message even when key 3 precedes key 2", func() {
			trace := fixmapN(
				pair(intKey(expTraceKeyPhase), fixint(ExpTracePhaseBuild)),
				pair(intKey(expTraceKeyOp), fixstr("eq")),
			)
			detail := fixmapN(
				pair(intKey(asErrorDetailKeyExpTrace), trace),
				pair(intKey(2), fixstr("bad exp")),
			)
			rp := parserForDetail(detail)
			rp.parseFieldsError()

			gm.Expect(rp.serverMessage).To(gm.Equal("bad exp"))
			gm.Expect(rp.expTrace).NotTo(gm.BeNil())
			gm.Expect(rp.expTrace.Op).To(gm.Equal("eq"))
		})

		gg.It("treats a present-but-empty nested map as no trace", func() {
			detail := fixmapN(pair(intKey(asErrorDetailKeyExpTrace), fixmapN()))
			rp := parserForDetail(detail)
			rp.parseFieldsError()
			gm.Expect(rp.expTrace).To(gm.BeNil())
		})
	})
})

// ---------- helpers ----------

// parserForDetail builds a recordParser sitting at offset 0 of a buffer that
// contains one ERROR_MESSAGE field with the given msgpack payload.
func parserForDetail(msgpackDetail []byte) *recordParser {
	return parserWithFields(
		[]FieldType{ERROR_MESSAGE},
		[][]byte{msgpackDetail},
	)
}

// parserWithFields builds a recordParser whose dataBuffer contains the given
// list of fields ([fieldlen:int32][type:byte][data...]) starting at offset 0.
// fieldCount is set explicitly; the proto / message header is not laid out
// since parseFieldsError reads only the fields themselves.
func parserWithFields(types []FieldType, data [][]byte) *recordParser {
	gm.Expect(len(types)).To(gm.Equal(len(data)))

	var buf bytes.Buffer
	for i := range types {
		size := uint32(len(data[i]) + 1)
		writeUint32(&buf, size)
		buf.WriteByte(byte(types[i]))
		buf.Write(data[i])
	}

	cmd := &baseCommand{}
	cmd.dataBuffer = buf.Bytes()
	cmd.dataOffset = 0

	rp := &recordParser{
		cmd:           cmd,
		serverSubcode: SubCodeNone,
		fieldCount:    len(types),
	}
	return rp
}

// fixmapN builds a fixmap (0-15 key/value pairs).
func fixmapN(kvs ...[]byte) []byte {
	gm.Expect(len(kvs) <= 15).To(gm.BeTrue())
	var out bytes.Buffer
	out.WriteByte(byte(0x80 | len(kvs)))
	for _, kv := range kvs {
		out.Write(kv)
	}
	return out.Bytes()
}

// fixarray builds a fixarray (0-15 elements).
func fixarray(elems ...[]byte) []byte {
	gm.Expect(len(elems) <= 15).To(gm.BeTrue())
	var out bytes.Buffer
	out.WriteByte(byte(0x90 | len(elems)))
	for _, e := range elems {
		out.Write(e)
	}
	return out.Bytes()
}

func fixmap1(kv []byte) []byte {
	var out bytes.Buffer
	out.WriteByte(0x81)
	out.Write(kv)
	return out.Bytes()
}

func fixmap2(kv1, kv2 []byte) []byte {
	var out bytes.Buffer
	out.WriteByte(0x82)
	out.Write(kv1)
	out.Write(kv2)
	return out.Bytes()
}

func pair(k, v []byte) []byte {
	var out bytes.Buffer
	out.Write(k)
	out.Write(v)
	return out.Bytes()
}

func intKey(v int) []byte {
	gm.Expect(v >= 0 && v <= 0x7F).To(gm.BeTrue())
	return []byte{byte(v)}
}

func fixint(v int) []byte {
	gm.Expect(v >= 0 && v <= 0x7F).To(gm.BeTrue())
	return []byte{byte(v)}
}

func fixstr(s string) []byte {
	data := []byte(s)
	gm.Expect(len(data) <= 31).To(gm.BeTrue())
	var out bytes.Buffer
	out.WriteByte(byte(0xA0 | len(data)))
	out.Write(data)
	return out.Bytes()
}

func writeUint16(buf *bytes.Buffer, v uint16) {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], v)
	buf.Write(b[:])
}

func writeInt32(buf *bytes.Buffer, v int32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(v))
	buf.Write(b[:])
}

func writeUint32(buf *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	buf.Write(b[:])
}

func writeInt64(buf *bytes.Buffer, v int64) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	buf.Write(b[:])
}
