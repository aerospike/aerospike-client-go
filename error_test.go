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
	"net"
	"os"

	ast "github.com/aerospike/aerospike-client-go/v8/types"

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
			inner := newError(ast.UDF_BAD_RESPONSE).setInDoubt(false, 2)
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

// prepareRetryTimeout decides replica sequencing on retry: when it reports
// true the failure is treated as a timeout, so writes retry on the same node
// and linearized reads keep their replica sequence. Only an explicit
// SERVER_NOT_AVAILABLE response (a node disowning the partition) justifies
// advancing a write to the next replica, matching the Java client. The
// polarity was previously inverted: writes moved replicas on connection
// errors and stayed put on SERVER_NOT_AVAILABLE.
var _ = gg.Describe("prepareRetryTimeout replica-sequencing polarity", func() {

	gg.It("must treat a client timeout as a timeout regardless of the error", func() {
		gm.Expect(prepareRetryTimeout(true, nil)).To(gm.BeTrue())
		gm.Expect(prepareRetryTimeout(true, newError(ast.SERVER_NOT_AVAILABLE))).To(gm.BeTrue())
	})

	gg.It("must not treat SERVER_NOT_AVAILABLE as a timeout, so writes advance the replica", func() {
		gm.Expect(prepareRetryTimeout(false, newError(ast.SERVER_NOT_AVAILABLE))).To(gm.BeFalse())
	})

	gg.It("must treat every other failure as a timeout, so writes retry in place", func() {
		for _, rc := range []ast.ResultCode{
			ast.NETWORK_ERROR,
			ast.TIMEOUT,
			ast.DEVICE_OVERLOAD,
			ast.KEY_BUSY,
			ast.PARSE_ERROR,
		} {
			gm.Expect(prepareRetryTimeout(false, newError(rc))).To(gm.BeTrue(),
				"result code %v must be treated as a timeout", rc)
		}
	})

	gg.It("must not treat an error-less failed pass (an inactive node) as a timeout", func() {
		gm.Expect(prepareRetryTimeout(false, nil)).To(gm.BeFalse())
	})
})

// overloadedServerError gates the retry of transient server responses:
// DEVICE_OVERLOAD (node momentarily overloaded) and KEY_BUSY (record locked by
// another transaction). Both previously fell through to the fatal return and
// failed the command on the first response; they now retry and feed the
// circuit breaker, matching the Java client.
var _ = gg.Describe("overloadedServerError retry classification", func() {

	gg.It("must classify DEVICE_OVERLOAD and KEY_BUSY as retryable", func() {
		gm.Expect(overloadedServerError(newError(ast.DEVICE_OVERLOAD))).To(gm.BeTrue())
		gm.Expect(overloadedServerError(newError(ast.KEY_BUSY))).To(gm.BeTrue())
	})

	gg.It("must not classify other failures as overloaded", func() {
		for _, rc := range []ast.ResultCode{
			ast.TIMEOUT,
			ast.NETWORK_ERROR,
			ast.PARAMETER_ERROR,
			ast.KEY_NOT_FOUND_ERROR,
			ast.SERVER_NOT_AVAILABLE,
		} {
			gm.Expect(overloadedServerError(newError(rc))).To(gm.BeFalse(),
				"result code %v must not be classified as overloaded", rc)
		}
	})

	// The response arrives fully parsed on a healthy connection, so the retry
	// path pools it rather than closing it; KeepConnection must agree.
	gg.It("must keep the connection for both, so retries reuse the pool", func() {
		gm.Expect(KeepConnection(newError(ast.DEVICE_OVERLOAD))).To(gm.BeTrue())
		gm.Expect(KeepConnection(newError(ast.KEY_BUSY))).To(gm.BeTrue())
	})
})

// parsedServerTimeout separates the two errors that share result code TIMEOUT:
// a timeout the server returned as a parsed response (connection healthy,
// counts toward the circuit breaker, retried with the connection pooled) and a
// client-side I/O deadline (connection possibly mid-message, must not be
// pooled). The tell is the wrapped net.Error that only the client-side path
// attaches.
var _ = gg.Describe("parsedServerTimeout classification", func() {

	gg.It("must classify a bare TIMEOUT as a server response", func() {
		gm.Expect(parsedServerTimeout(newError(ast.TIMEOUT))).To(gm.BeTrue())
	})

	gg.It("must not classify a client-side I/O deadline, which wraps a net.Error", func() {
		ioTimeout := &net.OpError{Op: "read", Err: os.ErrDeadlineExceeded}
		gm.Expect(parsedServerTimeout(newErrorAndWrap(ioTimeout, ast.TIMEOUT))).To(gm.BeFalse())
	})

	gg.It("must not classify other result codes", func() {
		for _, rc := range []ast.ResultCode{
			ast.NETWORK_ERROR,
			ast.DEVICE_OVERLOAD,
			ast.SERVER_NOT_AVAILABLE,
			ast.PARAMETER_ERROR,
		} {
			gm.Expect(parsedServerTimeout(newError(rc))).To(gm.BeFalse(),
				"result code %v must not classify as a server timeout", rc)
		}
	})

	// The retry path pools the connection for a parsed server timeout; the
	// fatal path must still refuse to pool any TIMEOUT, because there it
	// cannot distinguish a drained response from an abandoned stream.
	gg.It("must keep KeepConnection conservative about TIMEOUT", func() {
		gm.Expect(KeepConnection(newError(ast.TIMEOUT))).To(gm.BeFalse())
	})
})
