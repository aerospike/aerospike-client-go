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

package aerospike_test

import (
	"errors"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Extended-error coverage for command paths that are wired to surface field 45
// but were otherwise exercised only through Operate/Get/Put/Delete: the
// header-read path (Exists / GetHeader), the UDF apply path (Execute), and the
// single-record-in-transaction path (parseFields with a non-nil Txn).
//
// The trigger op is incidental here; the assertion is purely the extended-error
// surface (SubCode / ServerMessage / ExpTrace) flowing through each path.
// Requires an 8.1.3+ server; the transaction cases additionally require a
// Strong-Consistency namespace.
// edpUDFBody is a trivial module for the Execute path; the filter is evaluated
// before the body runs, so the body itself is never exercised. Kept local to
// avoid the shared udfBody/registerUDF helpers, which live behind !app_engine.
const edpUDFBody = `function testFunc1(rec, div)
   return 1
end`

var _ = gg.Describe("ErrorDetail wired-path coverage (integration)", func() {
	const edpBinName = "edp-bin"

	var (
		intKey  *as.Key
		listKey *as.Key
		set     string
	)

	// buildErrorExp is a type-mismatched comparison (int vs float) that fails to
	// *build* server-side: PARAMETER_ERROR + SubCodeNone + a build-phase trace.
	buildErrorExp := func() *as.Expression {
		return as.ExpEq(as.ExpIntVal(5), as.ExpFloatVal(6.0))
	}

	// filteredOutExp is a well-formed record filter that evaluates false against
	// the seeded record: FILTERED_OUT + contextual message + no subcode.
	filteredOutExp := func() *as.Expression {
		return as.ExpEq(as.ExpIntBin(edpBinName), as.ExpIntVal(99))
	}

	// assertBuildTrace pins the extended-error surface of a build failure.
	assertBuildTrace := func(err error) {
		gm.Expect(err).To(gm.HaveOccurred())
		ae := &as.AerospikeError{}
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(types.PARAMETER_ERROR))
		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		gm.Expect(ae.ExpTrace).NotTo(gm.BeNil(), "Expected a build trace at verbosity 3")
		gm.Expect(ae.ExpTrace.Phase).To(gm.Equal(as.ExpTracePhaseBuild))
		gm.Expect(ae.ExpTrace.ByteOffset).To(gm.BeNumerically(">=", 0), "Msgpack build traces carry byte_offset")
	}

	gg.BeforeEach(func() {
		nodes := client.GetNodes()
		if len(nodes) == 0 {
			gg.Skip("no nodes available")
		}
		serverVersion := nodes[0].GetServerVersion()
		if serverVersion.IsSmaller(version.ServerVersion_8_1_3) {
			gg.Skip("Extended error-detail requires server version 8.1.3 or later; got " + serverVersion.String())
		}

		set = randString(20)
		intKey, _ = as.NewKey(*namespace, set, "edp-int-key")
		listKey, _ = as.NewKey(*namespace, set, "edp-list-key")

		wp := as.NewWritePolicy(0, 0)
		err := client.PutBins(wp, intKey, as.NewBin(edpBinName, 1))
		gm.Expect(err).NotTo(gm.HaveOccurred())
		err = client.PutBins(wp, listKey, as.NewBin(edpBinName, []interface{}{10, 20, 30}))
		gm.Expect(err).NotTo(gm.HaveOccurred())
	})

	// -----------------------------------------------------------
	// Header-read path: Exists (readHeaderCommand / setExists).
	// -----------------------------------------------------------

	gg.It("Exists surfaces a FILTERED_OUT message", func() {
		p := as.NewPolicy()
		p.ErrorDetailVerbosity = 2
		p.FilterExpression = filteredOutExp()

		_, err := client.Exists(p, intKey)
		assertSubcodeAbsent(err, types.FILTERED_OUT, "filtered out")
	})

	gg.It("Exists surfaces a build trace at verbosity 3", func() {
		p := as.NewPolicy()
		p.ErrorDetailVerbosity = 3
		p.FilterExpression = buildErrorExp()

		_, err := client.Exists(p, intKey)
		assertBuildTrace(err)
	})

	// -----------------------------------------------------------
	// Header-read path: GetHeader (readHeaderCommand / setReadHeader).
	// -----------------------------------------------------------

	gg.It("GetHeader surfaces a FILTERED_OUT message", func() {
		p := as.NewPolicy()
		p.ErrorDetailVerbosity = 2
		p.FilterExpression = filteredOutExp()

		_, err := client.GetHeader(p, intKey)
		assertSubcodeAbsent(err, types.FILTERED_OUT, "filtered out")
	})

	gg.It("GetHeader surfaces a build trace at verbosity 3", func() {
		p := as.NewPolicy()
		p.ErrorDetailVerbosity = 3
		p.FilterExpression = buildErrorExp()

		_, err := client.GetHeader(p, intKey)
		assertBuildTrace(err)
	})

	// -----------------------------------------------------------
	// UDF apply path: Execute (executeCommand). The filter is evaluated before
	// the UDF body runs, so a trivial registered module suffices; the failure is
	// the filter, surfaced through the execute path's newServerError.
	// -----------------------------------------------------------

	gg.Context("Execute (UDF apply)", func() {
		gg.BeforeEach(func() {
			// Registered inline (rather than via the shared registerUDF/udfBody
			// helpers) so this file stays free of the !app_engine build gate those
			// helpers live behind and still compiles under -tags=app_engine.
			regTask, err := client.RegisterUDF(nil, []byte(edpUDFBody), "udf1.lua", as.LUA)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(<-regTask.OnComplete()).ToNot(gm.HaveOccurred())
		})

		gg.It("returns bare FILTERED_OUT with no staged detail", func() {
			// Unlike the single-record read/write/delete verbs, the UDF-apply
			// path (server udf.c) does not stage a "filtered out" detail on a
			// record-filter FILTERED_OUT - it returns the bare status. Pin that
			// so a future server change that starts staging it is flagged for
			// review. (The build-trace case below still proves field 45 flows
			// through the execute path.)
			wp := as.NewWritePolicy(0, 0)
			wp.ErrorDetailVerbosity = 2
			wp.FilterExpression = filteredOutExp()

			_, err := client.Execute(wp, intKey, "udf1", "testFunc1", as.NewValue(2))
			gm.Expect(err).To(gm.HaveOccurred())
			ae := &as.AerospikeError{}
			gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
			gm.Expect(ae.ResultCode).To(gm.Equal(types.FILTERED_OUT))
			gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
			gm.Expect(ae.ServerMessage).To(gm.BeEmpty())
			gm.Expect(ae.ExpTrace).To(gm.BeNil())
		})

		gg.It("surfaces a build trace at verbosity 3", func() {
			wp := as.NewWritePolicy(0, 0)
			wp.ErrorDetailVerbosity = 3
			wp.FilterExpression = buildErrorExp()

			_, err := client.Execute(wp, intKey, "udf1", "testFunc1", as.NewValue(2))
			assertBuildTrace(err)
		})
	})

	// -----------------------------------------------------------
	// Single-record-in-transaction path: parseFields with a non-nil Txn (as
	// opposed to parseFieldsError). MRT requires a Strong-Consistency namespace.
	// -----------------------------------------------------------

	gg.Context("within a transaction", func() {
		gg.BeforeEach(func() {
			if !as.ConfiguredAsStrongConsistency(client, *namespace) {
				gg.Skip("Transactions require a Strong-Consistency namespace")
			}
		})

		gg.It("a subcode-bearing op still surfaces its subcode", func() {
			txn := as.NewTxn()
			wp := as.NewWritePolicy(0, 0)
			wp.ErrorDetailVerbosity = 2
			wp.Txn = txn

			_, err := client.Operate(wp, listKey, as.ListGetOp(edpBinName, 99))
			assertSubcode(err, types.OP_NOT_APPLICABLE, types.SubCodeOpNotCDTIndexOutOfBounds)

			_, _ = client.Abort(txn)
		})

		gg.It("a filtered read still surfaces its message", func() {
			txn := as.NewTxn()
			rp := as.NewPolicy()
			rp.ErrorDetailVerbosity = 2
			rp.Txn = txn
			rp.FilterExpression = filteredOutExp()

			_, err := client.Get(rp, intKey)
			assertSubcodeAbsent(err, types.FILTERED_OUT, "filtered out")

			_, _ = client.Abort(txn)
		})
	})
})
