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

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// CLIENT-5045: canonical msgpack packs non-negative integers in unsigned
// form. Positive int64 values > math.MaxUint32 must be packed as 0xcf
// (uint64), not 0xd3 (signed int64); servers 8.1.2+ (AER-6930) reject the
// signed form inside filter-expression list/map literals. These tests pin
// the exact wire bytes for every encoding boundary of packAInt64.
var _ = gg.Describe("Packer int64 canonical form (CLIENT-5045)", func() {

	packInt64Bytes := func(val int64) []byte {
		pckr := newPacker()
		n, err := packAInt64(pckr, val)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		buf := pckr.Bytes()
		// the size returned with a nil buffer drives EstimateSize; it must
		// agree with the bytes actually written
		gm.Expect(n).To(gm.Equal(len(buf)))
		sz, err := packAInt64(nil, val)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(sz).To(gm.Equal(len(buf)))
		return buf
	}

	unpackInt64 := func(buf []byte) int64 {
		upckr := newUnpacker(buf, 0, len(buf))
		obj, err := upckr.unpackObject(false)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		switch v := obj.(type) {
		case int:
			return int64(v)
		case int64:
			return v
		}
		gg.Fail("unpacked to unexpected type")
		return 0
	}

	gg.DescribeTable("packs each boundary value in canonical msgpack form",
		func(val int64, expected []byte) {
			buf := packInt64Bytes(val)
			gm.Expect(buf).To(gm.Equal(expected))
			gm.Expect(unpackInt64(buf)).To(gm.Equal(val))
		},

		// non-negative values: canonical form is always unsigned
		gg.Entry("0 → fixint", int64(0), []byte{0x00}),
		gg.Entry("127 → fixint max", int64(127), []byte{0x7f}),
		gg.Entry("128 → uint8", int64(128), []byte{0xcc, 0x80}),
		gg.Entry("255 → uint8 max", int64(math.MaxUint8), []byte{0xcc, 0xff}),
		gg.Entry("256 → uint16", int64(256), []byte{0xcd, 0x01, 0x00}),
		gg.Entry("65535 → uint16 max", int64(math.MaxUint16), []byte{0xcd, 0xff, 0xff}),
		gg.Entry("65536 → uint32", int64(65536), []byte{0xce, 0x00, 0x01, 0x00, 0x00}),
		gg.Entry("2^32-1 → uint32 max", int64(math.MaxUint32),
			[]byte{0xce, 0xff, 0xff, 0xff, 0xff}),
		// CLIENT-5045: the smallest previously-rejected value; 0xcf, not 0xd3
		gg.Entry("2^32 → uint64 (the CLIENT-5045 boundary)", int64(math.MaxUint32+1),
			[]byte{0xcf, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00}),
		gg.Entry("MaxInt64 → uint64", int64(math.MaxInt64),
			[]byte{0xcf, 0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}),

		// negative values: canonical form is signed and must stay unchanged
		gg.Entry("-1 → negative fixint", int64(-1), []byte{0xff}),
		gg.Entry("-32 → negative fixint min", int64(-32), []byte{0xe0}),
		gg.Entry("-33 → int8", int64(-33), []byte{0xd0, 0xdf}),
		gg.Entry("-128 → int8 min", int64(math.MinInt8), []byte{0xd0, 0x80}),
		gg.Entry("-129 → int16", int64(-129), []byte{0xd1, 0xff, 0x7f}),
		gg.Entry("-32768 → int16 min", int64(math.MinInt16), []byte{0xd1, 0x80, 0x00}),
		gg.Entry("-32769 → int32", int64(-32769), []byte{0xd2, 0xff, 0xff, 0x7f, 0xff}),
		gg.Entry("-2^31 → int32 min", int64(math.MinInt32), []byte{0xd2, 0x80, 0x00, 0x00, 0x00}),
		gg.Entry("-2^31-1 → int64", int64(math.MinInt32-1),
			[]byte{0xd3, 0xff, 0xff, 0xff, 0xff, 0x7f, 0xff, 0xff, 0xff}),
		gg.Entry("MinInt64 → int64 min", int64(math.MinInt64),
			[]byte{0xd3, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}),
	)

	gg.It("never emits the signed 0xd3 marker for a non-negative int64", func() {
		vals := []int64{
			0, 1, 127, 128, 255, 256, 65535, 65536,
			math.MaxUint32 - 1, math.MaxUint32, math.MaxUint32 + 1,
			math.MaxUint32 + 2, 1 << 40, 1 << 50, 1 << 62,
			math.MaxInt64 - 1, math.MaxInt64,
		}
		for _, v := range vals {
			buf := packInt64Bytes(v)
			gm.Expect(buf[0]).ToNot(gm.Equal(byte(0xd3)),
				"value %d must not use the signed int64 form", v)
			gm.Expect(unpackInt64(buf)).To(gm.Equal(v))
		}
	})

	gg.It("packs uint64 values unchanged as 0xcf", func() {
		pckr := newPacker()
		n, err := packAUInt64(pckr, math.MaxUint64)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(n).To(gm.Equal(9))
		gm.Expect(pckr.Bytes()).To(gm.Equal(
			[]byte{0xcf, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}))
	})

	gg.It("packs a large positive int64 identically to the same-valued uint64", func() {
		const v = uint64(math.MaxUint32 + 12345)
		signed := newPacker()
		_, err := packAInt64(signed, int64(v))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		unsigned := newPacker()
		_, err = packAUInt64(unsigned, v)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(signed.Bytes()).To(gm.Equal(unsigned.Bytes()))
	})
})
