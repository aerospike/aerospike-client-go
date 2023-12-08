// Copyright 2014-2022 Aerospike, Inc.
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
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"

	as "github.com/aerospike/aerospike-client-go/v7"
)

var _ = gg.Describe("CDTContext Test", func() {

	gg.It("should convert to/from base64", func() {
		ctxl := []*as.CDTContext{
			as.CtxMapKey(as.StringValue("key2")),
			as.CtxListRank(0),
		}

		b, err := as.CDTContextToBase64(ctxl)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(b).ToNot(gm.BeNil())

		ctxl2, err := as.Base64ToCDTContext(b)
		gm.Expect(err).ToNot(gm.HaveOccurred())

		arraysEqual(ctxl2, ctxl)
	})

}) // describe
