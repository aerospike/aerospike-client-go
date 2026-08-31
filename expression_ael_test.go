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
	"strings"

	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// CLIENT-5116: ExpFromAEL frames AEL source text as the server's
// EXP_AEL_COMPILE payload. These in-package tests pin the exact bytes that
// reach the wire, because the framing is a protocol contract with the server
// (`build_internal` on the server's dsl branch) rather than an internal detail.
var _ = gg.Describe("ExpFromAEL wire framing (CLIENT-5116)", func() {

	// packBytes returns exactly what the expression writes on the wire.
	packBytes := func(exp *Expression) []byte {
		sz, err := exp.size()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		buf := newBuffer(sz)
		n, err := exp.pack(buf)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(n).To(gm.Equal(sz))

		return buf.Bytes()
	}

	gg.It("frames a short expression with a bin8 header", func() {
		exp, err := ExpFromAEL("true")
		gm.Expect(err).ToNot(gm.HaveOccurred())

		// fixarray(2), uint8(128), bin8(4), "true"
		gm.Expect(packBytes(exp)).To(gm.Equal([]byte{
			0x92, 0xcc, 0x80, 0xc4, 0x04, 't', 'r', 'u', 'e',
		}))
	})

	gg.It("frames the largest bin8 payload with a bin8 header", func() {
		text := strings.Repeat("a", 255)
		exp, err := ExpFromAEL(text)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		b := packBytes(exp)
		gm.Expect(b[:5]).To(gm.Equal([]byte{0x92, 0xcc, 0x80, 0xc4, 0xff}))
		gm.Expect(b[5:]).To(gm.Equal([]byte(text)))
	})

	gg.It("frames a 256 byte expression with a bin16 header", func() {
		text := strings.Repeat("a", 256)
		exp, err := ExpFromAEL(text)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		b := packBytes(exp)
		gm.Expect(b[:6]).To(gm.Equal([]byte{0x92, 0xcc, 0x80, 0xc5, 0x01, 0x00}))
		gm.Expect(b[6:]).To(gm.Equal([]byte(text)))
	})

	gg.It("frames a 64KiB expression with a bin32 header", func() {
		text := strings.Repeat("a", 1<<16)
		exp, err := ExpFromAEL(text)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		b := packBytes(exp)
		gm.Expect(b[:8]).To(gm.Equal([]byte{0x92, 0xcc, 0x80, 0xc6, 0x00, 0x01, 0x00, 0x00}))
		gm.Expect(b[8:]).To(gm.Equal([]byte(text)))
	})

	gg.It("rejects empty source text, which the server requires to be non-empty", func() {
		exp, err := ExpFromAEL("")
		gm.Expect(exp).To(gm.BeNil())
		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(types.PARAMETER_ERROR)).To(gm.BeTrue())
	})

	gg.It("encodes to base64 for transport through ExpFromBase64", func() {
		exp, err := ExpFromAEL("$.n + 1 == 2")
		gm.Expect(err).ToNot(gm.HaveOccurred())

		b64, err := exp.Base64()
		gm.Expect(err).ToNot(gm.HaveOccurred())

		roundTripped, err := ExpFromBase64(b64)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(packBytes(roundTripped)).To(gm.Equal(packBytes(exp)))
	})
})
