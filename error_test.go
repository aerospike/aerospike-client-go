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

package aerospike

import (
	"errors"

	ast "github.com/aerospike/aerospike-client-go/v7/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Aerospike Error Tests", func() {

	gg.Context("Matches()", func() {

		gg.It("should handle simple case", func() {
			err := newError(ast.UDF_BAD_RESPONSE)

			res := err.Matches(ast.UDF_BAD_RESPONSE)
			gm.Expect(res).To(gm.BeTrue())
		})

		gg.It("should handle simple case", func() {
			inner := newError(ast.UDF_BAD_RESPONSE)
			err := newError(ast.TIMEOUT).wrap(inner)

			res := err.Matches(ast.UDF_BAD_RESPONSE)
			gm.Expect(res).To(gm.BeTrue())

			res = err.Matches(ast.TIMEOUT)
			gm.Expect(res).To(gm.BeTrue())

			res = err.Matches(ast.UDF_BAD_RESPONSE, ast.TIMEOUT)
			gm.Expect(res).To(gm.BeTrue())
		})

	})

	gg.Context("chainErrors()", func() {

		gg.It("should handle nil for inner error", func() {
			outer := newError(ast.UDF_BAD_RESPONSE)
			err := chainErrors(outer, nil)

			res := err.Matches(ast.UDF_BAD_RESPONSE)
			gm.Expect(res).To(gm.BeTrue())
		})

		gg.It("should handle nil for inner error", func() {
			inner := newError(ast.UDF_BAD_RESPONSE)
			err := chainErrors(nil, inner)

			res := err.Matches(ast.UDF_BAD_RESPONSE)
			gm.Expect(res).To(gm.BeTrue())
		})

	})

	gg.Context("errors.Is", func() {

		gg.It("should handle simple case", func() {
			err := newError(ast.UDF_BAD_RESPONSE)

			res := errors.Is(err, ErrUDFBadResponse)
			gm.Expect(res).To(gm.BeTrue())
		})

		gg.It("should handle complex case", func() {
			err := newError(ast.UDF_BAD_RESPONSE)

			res := errors.Is(err, &AerospikeError{ResultCode: ast.UDF_BAD_RESPONSE})
			gm.Expect(res).To(gm.BeTrue())

		})

		gg.It("should handle complex case with inDoubt", func() {
			err := newError(ast.UDF_BAD_RESPONSE)

			res := errors.Is(err, &AerospikeError{ResultCode: ast.UDF_BAD_RESPONSE, InDoubt: true})
			gm.Expect(res).To(gm.BeFalse())

		})

		gg.It("should handle wrapped cases", func() {
			inner := newError(ast.UDF_BAD_RESPONSE)
			err := newError(ast.TIMEOUT).wrap(inner)

			res := errors.Is(err, &AerospikeError{ResultCode: ast.UDF_BAD_RESPONSE, InDoubt: true})
			gm.Expect(res).To(gm.BeFalse())
		})

		gg.It("should handle chained cases", func() {
			inner1 := newError(ast.UDF_BAD_RESPONSE)
			inner2 := newError(ast.BATCH_DISABLED)
			inner := chainErrors(inner2, inner1)
			outer := newError(ast.TIMEOUT)
			err := chainErrors(outer, inner)

			res := errors.Is(err, &AerospikeError{ResultCode: ast.UDF_BAD_RESPONSE, InDoubt: true})
			gm.Expect(res).To(gm.BeFalse())
		})

	}) // Context

	gg.Context("errors.As", func() {

		gg.It("should handle simple case", func() {
			err := newError(ast.UDF_BAD_RESPONSE)

			ae := new(AerospikeError)
			res := errors.As(err, &ae)
			gm.Expect(res).To(gm.BeTrue())
			gm.Expect(errors.Is(err, &AerospikeError{ResultCode: ast.UDF_BAD_RESPONSE})).To(gm.BeTrue())
		})

		gg.It("should handle chained case", func() {
			inner := newError(ast.UDF_BAD_RESPONSE).setInDoubt(false, true)
			outer := newError(ast.TIMEOUT)
			err := chainErrors(outer, inner)

			ae := new(AerospikeError)
			res := errors.As(err, &ae)
			gm.Expect(res).To(gm.BeTrue())
			gm.Expect(errors.Is(err, &AerospikeError{ResultCode: ast.UDF_BAD_RESPONSE})).To(gm.BeTrue())
			gm.Expect(errors.Is(err, &AerospikeError{ResultCode: ast.UDF_BAD_RESPONSE, InDoubt: true})).To(gm.BeTrue())
			gm.Expect(errors.Is(err, &AerospikeError{ResultCode: ast.UDF_BAD_RESPONSE, InDoubt: false})).To(gm.BeTrue())
		})

	})

}) // Describe
