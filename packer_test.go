// Copyright 2014-2025 Aerospike, Inc.
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

package aerospike_test

import (
	"encoding/base64"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	as "github.com/aerospike/aerospike-client-go/v8"
)

var _ = gg.Describe("Packer ValueArray nil-element handling", func() {

	// Exercises packer.packValueArray via the public CDTContext → base64
	// round-trip path. CtxMapKeysIn wraps its arguments in a ValueArray;
	// CDTContextToBase64 packs the context, which packs that ValueArray,
	// which dispatches to packValueArray. A nil entry in the ValueArray
	// must be packed as msgpack nil (0xc0) rather than panicking on a nil
	// interface dereference.
	gg.It("packs a nil Value element as msgpack nil (0xc0)", func() {
		ctxl := []*as.CDTContext{
			as.CtxMapKeysIn(as.NewStringValue("x"), nil),
		}

		// Must not panic; must produce valid base64.
		b64, err := as.CDTContextToBase64(ctxl)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(b64).ToNot(gm.BeEmpty())

		raw, derr := base64.StdEncoding.DecodeString(b64)
		gm.Expect(derr).ToNot(gm.HaveOccurred())

		// The wire bytes must contain msgpack nil (0xc0) — emitted by the
		// nil branch of packValueArray for the second list element.
		gm.Expect(raw).To(gm.ContainElement(byte(0xc0)))
	})

	gg.It("round-trips a CDTContext built from CtxMapKeysIn with a nil element", func() {
		ctxl := []*as.CDTContext{
			as.CtxMapKeysIn(as.NewStringValue("a"), nil),
		}

		b64, err := as.CDTContextToBase64(ctxl)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		decoded, err := as.Base64ToCDTContext(b64)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(decoded).To(gm.HaveLen(1))
	})
})
