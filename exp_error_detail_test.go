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
	"math"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// Expression error-detail coverage (CLIENT-4221) ported from the Java client's
// TestExpErrorDetail, itself ported from the Python QE suites on
// aerospike-tests-python branch dylan/ael-error-details
// (test_msgpack_exp_error_details.py and the language-agnostic rows of
// test_ael_error_details.py / test_ael_expop_error_details.py).
//
// Four areas:
//
//  1. Eval-phase (PHASE_EVAL) expression traces for filter runtime faults:
//     div/mod by zero, INT64_MIN overflow, CDT out-of-bounds (with subcode),
//     unordered-map compare.
//  2. Verbosity tier-1 suppression semantics: an AS_SUB_NONE error at
//     verbosity 1 stages no error details at all, while a real-subcode CDT
//     fault ships subcode-only (no message, no trace).
//  3. Verb parity: build / fault / FALSE / absent / metadata-FALSE / tier-2 /
//     clean-pass filter scenarios across put, delete and operate, including
//     the verb-specific metadata-filter messages.
//  4. Exp-op context breadth: exp-read build/fault/absent details,
//     EVAL_NO_FAIL swallowing, legal non-boolean value reads, invalid read
//     flags, and write-policy flag outcomes that stage no details.
//
// The Go client sends classic msgpack expressions (no AEL source), so traces
// follow the msgpack contract: build traces always carry byte_offset, eval
// traces never do, and lang / ael_offset / ael_span are never present.
// Eval-trace keys outcome (7) and operands (13) are decoded and asserted in
// the explainer specs at the end of this file.
var _ = gg.Describe("ExpErrorDetail (integration)", func() {
	const (
		binInt     = "x"    // 10
		binFloat   = "y"    // 2.5
		binStr     = "name" // "ael"
		binList    = "xs"   // [1, 2, 3]
		binMap1    = "um1"  // unordered map
		binMap2    = "um2"  // unordered map
		binMissing = "missing"
	)

	var (
		stdKey     *as.Key
		scratchKey *as.Key
	)

	reseedScratch := func() {
		err := client.PutBins(as.NewWritePolicy(0, 0), scratchKey,
			as.NewBin(binInt, 10), as.NewBin("keep", 1))
		gm.Expect(err).NotTo(gm.HaveOccurred())
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

		set := randString(20)
		stdKey, _ = as.NewKey(*namespace, set, "eed-std-key")
		scratchKey, _ = as.NewKey(*namespace, set, "eed-scratch-key")

		err := client.PutBins(as.NewWritePolicy(0, 0), stdKey,
			as.NewBin(binInt, 10),
			as.NewBin(binFloat, 2.5),
			as.NewBin(binStr, "ael"),
			as.NewBin(binList, []interface{}{1, 2, 3}),
			as.NewBin(binMap1, map[string]int{"a": 1}),
			as.NewBin(binMap2, map[string]int{"b": 2}))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		reseedScratch()
	})

	// ---------------------------------------------------------------------
	// Shared expression inducers.
	// ---------------------------------------------------------------------

	// Build failure: type-mismatched comparison (int vs float).
	buildErrorExp := func() *as.Expression {
		return as.ExpEq(as.ExpIntVal(5), as.ExpFloatVal(6.0))
	}

	// Eval fault: integer division by zero (gt(div(5, 0), 1)).
	divZeroFilterExp := func() *as.Expression {
		return as.ExpGreater(as.ExpNumDiv(as.ExpIntVal(5), as.ExpIntVal(0)), as.ExpIntVal(1))
	}

	// Eval fault: CDT list index 9 over [1,2,3] (carries a real subcode).
	cdtOobExp := func() *as.Expression {
		return as.ExpListGetByIndex(as.ListReturnTypeValue, as.ExpTypeINT, as.ExpIntVal(9), as.ExpListBin(binList))
	}

	// ---------------------------------------------------------------------
	// Runners.
	// ---------------------------------------------------------------------

	toAerospikeError := func(err error, expectedRc types.ResultCode) *as.AerospikeError {
		gm.Expect(err).To(gm.HaveOccurred())
		ae := &as.AerospikeError{}
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue())
		gm.Expect(ae.ResultCode).To(gm.Equal(expectedRc), "Unexpected result code")
		return ae
	}

	expectFilteredGet := func(verbosity int, filter *as.Expression, expectedRc types.ResultCode) *as.AerospikeError {
		p := as.NewPolicy()
		p.ErrorDetailVerbosity = verbosity
		p.FilterExpression = filter

		_, err := client.Get(p, stdKey)
		return toAerospikeError(err, expectedRc)
	}

	expectOperateError := func(key *as.Key, verbosity int, expectedRc types.ResultCode, op *as.Operation) *as.AerospikeError {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = verbosity

		_, err := client.Operate(wp, key, op)
		return toAerospikeError(err, expectedRc)
	}

	// ---------------------------------------------------------------------
	// Assertion helpers.
	// ---------------------------------------------------------------------

	assertMessageContains := func(ae *as.AerospikeError, expected string) {
		gm.Expect(ae.ServerMessage).NotTo(gm.BeEmpty(), "Expected server error message")
		gm.Expect(ae.ServerMessage).To(gm.ContainSubstring(expected))
	}

	// Assert an eval-phase (runtime) trace. Per the msgpack contract, runtime
	// traces never carry byte_offset (ByteOffset == -1).
	assertEvalTrace := func(ae *as.AerospikeError, op string, depth int, path []string) *as.ExpressionTrace {
		t := ae.ExpTrace
		gm.Expect(t).NotTo(gm.BeNil(), "Expected a non-nil expression trace at verbosity 3")
		gm.Expect(t.Phase).To(gm.Equal(as.ExpTracePhaseEval), "Expected an eval-phase trace")
		gm.Expect(t.Op).To(gm.Equal(op), "Unexpected trace op")
		gm.Expect(t.Depth).To(gm.Equal(depth), "Unexpected trace depth")
		gm.Expect(t.Path).To(gm.Equal(path), "Unexpected trace path")
		gm.Expect(t.ByteOffset).To(gm.Equal(-1), "Runtime traces must not carry byte_offset")
		gm.Expect(t.Snippet).NotTo(gm.BeEmpty(), "Expected an op-stream snippet")
		return t
	}

	// Assert a build-phase trace. Per the msgpack contract, build traces
	// always carry byte_offset.
	assertBuildTrace := func(ae *as.AerospikeError) *as.ExpressionTrace {
		t := ae.ExpTrace
		gm.Expect(t).NotTo(gm.BeNil(), "Expected a non-nil expression trace at verbosity 3")
		gm.Expect(t.Phase).To(gm.Equal(as.ExpTracePhaseBuild), "Expected a build-phase trace")
		gm.Expect(t.ByteOffset).To(gm.BeNumerically(">=", 0), "Msgpack build traces must carry byte_offset")
		return t
	}

	// Assert the server staged NO error details (no field 45): there is no
	// server message, no subcode and no trace.
	assertNoDetails := func(ae *as.AerospikeError, expectedRc types.ResultCode) {
		gm.Expect(ae.ResultCode).To(gm.Equal(expectedRc), "Unexpected result code")
		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone), "Expected no subcode")
		gm.Expect(ae.ServerMessage).To(gm.BeEmpty(), "Expected no server-supplied error detail")
		gm.Expect(ae.ExpTrace).To(gm.BeNil(), "Expected no expression trace")
	}

	// ---------------------------------------------------------------------
	// 1. Eval-phase (runtime) filter traces at verbosity 3.
	//
	// Python: test_msgpack_exp_error_details.py CASES_MP_FAULT and the
	// language-agnostic rows of test_ael_error_details.py CASES_FILTER_FAULT.
	// ---------------------------------------------------------------------

	gg.It("filter fault div by zero surfaces an eval trace", func() {
		ae := expectFilteredGet(3, divZeroFilterExp(), types.FILTERED_OUT)
		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "integer division by zero")

		t := assertEvalTrace(ae, "div", 2, []string{"gt", "div"})
		gm.Expect(t.Snippet).To(gm.ContainSubstring("div("), "Expected div op in snippet")
	})

	gg.It("filter fault mod by zero surfaces an eval trace", func() {
		exp := as.ExpEq(as.ExpNumMod(as.ExpIntBin(binInt), as.ExpIntVal(0)), as.ExpIntVal(1))

		ae := expectFilteredGet(3, exp, types.FILTERED_OUT)
		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "integer modulo by zero")

		t := assertEvalTrace(ae, "mod", 2, []string{"eq", "mod"})
		gm.Expect(t.Snippet).To(gm.ContainSubstring("mod("), "Expected mod op in snippet")
	})

	gg.It("filter fault div overflow surfaces an eval trace", func() {
		// INT64_MIN / -1 overflows 64-bit signed division.
		exp := as.ExpGreater(
			as.ExpNumDiv(as.ExpIntVal(math.MinInt64), as.ExpIntVal(-1)), as.ExpIntVal(1))

		ae := expectFilteredGet(3, exp, types.FILTERED_OUT)
		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "integer division overflow")

		assertEvalTrace(ae, "div", 2, []string{"gt", "div"})
	})

	gg.It("filter fault unordered map compare surfaces an eval trace", func() {
		// Both bins are unordered maps; an ordered equality compare faults.
		exp := as.ExpEq(as.ExpMapBin(binMap1), as.ExpMapBin(binMap2))

		ae := expectFilteredGet(3, exp, types.FILTERED_OUT)
		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "cannot compare an unordered map")

		assertEvalTrace(ae, "eq", 1, []string{"eq"})
	})

	gg.It("filter fault CDT out of bounds carries subcode and eval trace", func() {
		// A CDT sub-op fault carries a real subcode through the FILTERED_OUT
		// status (the CDT layer's out-of-bounds subcode = 1).
		exp := as.ExpEq(cdtOobExp(), as.ExpIntVal(1))

		ae := expectFilteredGet(3, exp, types.FILTERED_OUT)
		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeOpNotCDTIndexOutOfBounds))
		assertMessageContains(ae, "out of bounds")

		assertEvalTrace(ae, "call", 2, []string{"eq", "call"})
	})

	// ---------------------------------------------------------------------
	// 2. Verbosity tier-1 suppression semantics.
	//
	// Python: CASES_FILTER_VERBOSITY / CASES_MP_VERBOSITY. Build failures and
	// div-by-zero faults carry AS_SUB_NONE, so at tier 1 (subcode-only) they
	// have nothing to send and field 45 is suppressed entirely; a real-subcode
	// CDT fault ships subcode only (no message, no trace).
	// ---------------------------------------------------------------------

	gg.It("verbosity 1 suppresses build-error details entirely", func() {
		ae := expectFilteredGet(1, buildErrorExp(), types.PARAMETER_ERROR)
		assertNoDetails(ae, types.PARAMETER_ERROR)
	})

	gg.It("verbosity 1 suppresses eval-fault details entirely", func() {
		ae := expectFilteredGet(1, divZeroFilterExp(), types.FILTERED_OUT)
		assertNoDetails(ae, types.FILTERED_OUT)
	})

	gg.It("verbosity 1 CDT fault ships subcode only", func() {
		exp := as.ExpEq(cdtOobExp(), as.ExpIntVal(1))

		ae := expectFilteredGet(1, exp, types.FILTERED_OUT)
		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeOpNotCDTIndexOutOfBounds))
		gm.Expect(ae.ExpTrace).To(gm.BeNil(), "Tier 1 must surface no trace")

		gm.Expect(ae.ServerMessage).NotTo(gm.BeEmpty())
		gm.Expect(ae.ServerMessage).To(gm.ContainSubstring("subcode=1"), "Expected bare subcode form")
		gm.Expect(ae.ServerMessage).NotTo(gm.ContainSubstring("out of bounds"), "Tier 1 must surface no message text")
	})

	gg.It("verbosity 2 eval fault ships message but no trace", func() {
		// Tier 2: message present, trace suppressed.
		ae := expectFilteredGet(2, divZeroFilterExp(), types.FILTERED_OUT)
		assertMessageContains(ae, "integer division by zero")
		gm.Expect(ae.ExpTrace).To(gm.BeNil(), "Verbosity 2 must surface NO expression trace")
	})

	// ---------------------------------------------------------------------
	// 3. Verb parity: the filter stages identically across single-record
	// verbs (shared server rw_utils), except metadata-phase FALSE which is
	// verb-dependent.
	//
	// Python: test_ael_error_details.py PARITY_CASES.
	// ---------------------------------------------------------------------

	parityVerbs := []string{"put", "delete", "operate"}

	filterPolicy := func(verbosity int, filter *as.Expression) *as.WritePolicy {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = verbosity
		wp.FilterExpression = filter
		return wp
	}

	// Reseed the scratch record, run the verb, and return the resulting error (or nil).
	runVerb := func(verb string, wp *as.WritePolicy) error {
		reseedScratch()
		switch verb {
		case "put":
			return client.PutBins(wp, scratchKey, as.NewBin("other", 1))
		case "delete":
			_, err := client.Delete(wp, scratchKey)
			return err
		default:
			_, err := client.Operate(wp, scratchKey, as.GetBinOp(binInt))
			return err
		}
	}

	expectVerbError := func(verb string, wp *as.WritePolicy, expectedRc types.ResultCode) *as.AerospikeError {
		err := runVerb(verb, wp)
		gm.Expect(err).To(gm.HaveOccurred(), "[%s] Expected an error", verb)
		ae := &as.AerospikeError{}
		gm.Expect(errors.As(err, &ae)).To(gm.BeTrue(), "[%s] Expected an AerospikeError", verb)
		gm.Expect(ae.ResultCode).To(gm.Equal(expectedRc), "[%s] Unexpected result code", verb)
		return ae
	}

	gg.It("parity: build error across put/delete/operate", func() {
		for _, verb := range parityVerbs {
			ae := expectVerbError(verb, filterPolicy(3, buildErrorExp()), types.PARAMETER_ERROR)
			assertMessageContains(ae, "invalid filter expression in request")
			assertBuildTrace(ae)
		}
	})

	gg.It("parity: eval fault across put/delete/operate", func() {
		for _, verb := range parityVerbs {
			ae := expectVerbError(verb, filterPolicy(3, divZeroFilterExp()), types.FILTERED_OUT)
			assertMessageContains(ae, "integer division by zero")
			assertEvalTrace(ae, "div", 2, []string{"gt", "div"})
		}
	})

	gg.It("parity: filter FALSE across put/delete/operate", func() {
		// Clean FALSE explain. Outcome and operands get dedicated coverage
		// below; here assert phase and deciding op across the three verbs.
		exp := as.ExpEq(as.ExpIntBin(binInt), as.ExpIntVal(11))

		for _, verb := range parityVerbs {
			ae := expectVerbError(verb, filterPolicy(3, exp), types.FILTERED_OUT)
			assertMessageContains(ae, "filter expression evaluated to false")

			t := ae.ExpTrace
			gm.Expect(t).NotTo(gm.BeNil(), "[%s] Expected an explain trace at verbosity 3", verb)
			gm.Expect(t.Phase).To(gm.Equal(as.ExpTracePhaseEval), "[%s] Expected an eval-phase trace", verb)
			gm.Expect(t.Op).To(gm.Equal("eq"), "[%s] Expected the deciding comparison op", verb)
		}
	})

	gg.It("parity: filter absent bin across put/delete/operate", func() {
		// First absent bin reference through the chain decides the outcome.
		exp := as.ExpEq(as.ExpIntBin(binMissing), as.ExpIntVal(2))

		for _, verb := range parityVerbs {
			ae := expectVerbError(verb, filterPolicy(3, exp), types.FILTERED_OUT)
			assertMessageContains(ae, "filter references an absent bin or key")

			t := ae.ExpTrace
			gm.Expect(t).NotTo(gm.BeNil(), "[%s] Expected an explain trace at verbosity 3", verb)
			gm.Expect(t.Phase).To(gm.Equal(as.ExpTracePhaseEval), "[%s] Expected an eval-phase trace", verb)
			gm.Expect(t.Op).To(gm.Equal("bin"), "[%s] Expected the absent accessor op", verb)
		}
	})

	gg.It("parity: metadata FALSE has verb-specific message and no trace", func() {
		// A metadata-only filter FALSE is staged per verb (write.c / delete
		// / read paths) and is message-only: NO trace even at verbosity 3.
		exp := as.ExpEq(as.ExpTTL(), as.ExpIntVal(-5))

		expected := map[string]string{
			"put":     "write filtered out by metadata filter",
			"delete":  "delete filtered out by metadata filter",
			"operate": "read filtered out by metadata filter",
		}

		for _, verb := range parityVerbs {
			ae := expectVerbError(verb, filterPolicy(3, exp), types.FILTERED_OUT)
			assertMessageContains(ae, expected[verb])
			gm.Expect(ae.ExpTrace).To(gm.BeNil(), "[%s] Metadata-phase FALSE must stage no trace", verb)
		}
	})

	gg.It("parity: tier 2 ships message but no trace across put/delete/operate", func() {
		for _, verb := range parityVerbs {
			ae := expectVerbError(verb, filterPolicy(2, divZeroFilterExp()), types.FILTERED_OUT)
			assertMessageContains(ae, "integer division by zero")
			gm.Expect(ae.ExpTrace).To(gm.BeNil(), "[%s] Verbosity 2 must surface NO trace", verb)
		}
	})

	gg.It("parity: clean pass across put/delete/operate", func() {
		// Filter TRUE: every verb succeeds with verbosity set.
		exp := as.ExpEq(as.ExpIntBin(binInt), as.ExpIntVal(10))

		for _, verb := range parityVerbs {
			err := runVerb(verb, filterPolicy(3, exp))
			gm.Expect(err).NotTo(gm.HaveOccurred(), "[%s] Expected success", verb)
		}
	})

	// ---------------------------------------------------------------------
	// 4. Exp-op context breadth: expression read ops.
	//
	// Python: test_ael_expop_error_details.py CASES_READOP_* (behavior-level
	// rows; the AEL source diagnostics themselves are not portable).
	// ---------------------------------------------------------------------

	gg.It("exp read build failure surfaces a build trace", func() {
		ae := expectOperateError(stdKey, 3, types.PARAMETER_ERROR,
			as.ExpReadOp("result", buildErrorExp(), as.ExpReadFlagDefault))

		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "invalid expression in operation request")
		assertBuildTrace(ae)
	})

	gg.It("exp read non-boolean root is legal", func() {
		// A non-boolean root is illegal for a filter but is the whole point of
		// a value read: $.x + 1 -> 11, rc 0, no error details.
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 3

		rec, err := client.Operate(wp, stdKey,
			as.ExpReadOp("result", as.ExpNumAdd(as.ExpIntBin(binInt), as.ExpIntVal(1)),
				as.ExpReadFlagDefault))

		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rec).NotTo(gm.BeNil())
		gm.Expect(rec.Bins["result"]).To(gm.Equal(11))
	})

	gg.It("exp read invalid flags stages no details", func() {
		// A write-only policy flag on a read op is rejected structurally
		// before any expression is built: rc 4 with NO error details staged.
		ae := expectOperateError(stdKey, 3, types.PARAMETER_ERROR,
			as.ExpReadOp("result", as.ExpNumAdd(as.ExpIntBin(binInt), as.ExpIntVal(1)),
				as.ExpReadFlags(as.ExpWriteFlagCreateOnly)))

		assertNoDetails(ae, types.PARAMETER_ERROR)
	})

	gg.It("exp read eval fault div by zero surfaces an eval trace", func() {
		ae := expectOperateError(stdKey, 3, types.OP_NOT_APPLICABLE,
			as.ExpReadOp("result", as.ExpNumDiv(as.ExpIntBin(binInt), as.ExpIntVal(0)),
				as.ExpReadFlagDefault))

		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "integer division by zero")
		assertEvalTrace(ae, "div", 1, []string{"div"})
	})

	gg.It("exp read CDT out of bounds carries subcode and eval trace", func() {
		ae := expectOperateError(stdKey, 3, types.OP_NOT_APPLICABLE,
			as.ExpReadOp("result", cdtOobExp(), as.ExpReadFlagDefault))

		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeOpNotCDTIndexOutOfBounds))
		assertMessageContains(ae, "out of bounds")
		assertEvalTrace(ae, "call", 1, []string{"call"})
	})

	gg.It("exp read absent bin surfaces an eval trace", func() {
		ae := expectOperateError(stdKey, 3, types.OP_NOT_APPLICABLE,
			as.ExpReadOp("result", as.ExpIntBin(binMissing), as.ExpReadFlagDefault))

		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "expression references an absent bin or key")
		assertEvalTrace(ae, "bin", 1, []string{"bin"})
	})

	gg.It("exp read wrong-typed bin reads as absent", func() {
		// A present bin read at the wrong type folds to UNK -> ABSENT
		// (binFloat holds a float, read as INT).
		ae := expectOperateError(stdKey, 3, types.OP_NOT_APPLICABLE,
			as.ExpReadOp("result", as.ExpIntBin(binFloat), as.ExpReadFlagDefault))

		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "expression references an absent bin or key")
		assertEvalTrace(ae, "bin", 1, []string{"bin"})
	})

	gg.It("exp read unknown literal reads as absent", func() {
		// A bare unknown() produces no value -> ABSENT; op=unknown.
		ae := expectOperateError(stdKey, 3, types.OP_NOT_APPLICABLE,
			as.ExpReadOp("result", as.ExpUnknown(), as.ExpReadFlagDefault))

		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "expression references an absent bin or key")
		assertEvalTrace(ae, "unknown", 1, []string{"unknown"})
	})

	gg.It("exp read EVAL_NO_FAIL swallows absent bin", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 3

		rec, err := client.Operate(wp, stdKey,
			as.ExpReadOp("result", as.ExpIntBin(binMissing), as.ExpReadFlagEvalNoFail))

		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rec).NotTo(gm.BeNil())
	})

	gg.It("exp read EVAL_NO_FAIL swallows eval fault", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 3

		rec, err := client.Operate(wp, stdKey,
			as.ExpReadOp("result", as.ExpNumDiv(as.ExpIntBin(binInt), as.ExpIntVal(0)),
				as.ExpReadFlagEvalNoFail))

		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rec).NotTo(gm.BeNil())
	})

	// ---------------------------------------------------------------------
	// 4 (continued). Exp-op context breadth: expression write ops.
	//
	// Python: test_ael_expop_error_details.py CASES_WRITEOP_FAULT and
	// CASES_WRITEOP_POLICY.
	// ---------------------------------------------------------------------

	gg.It("exp write eval fault div by zero surfaces an eval trace", func() {
		ae := expectOperateError(scratchKey, 3, types.OP_NOT_APPLICABLE,
			as.ExpWriteOp("wb", as.ExpNumDiv(as.ExpIntBin(binInt), as.ExpIntVal(0)),
				as.ExpWriteFlagDefault))

		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeNone))
		assertMessageContains(ae, "integer division by zero")
		assertEvalTrace(ae, "div", 1, []string{"div"})
	})

	gg.It("exp write CDT out of bounds carries subcode and eval trace", func() {
		ae := expectOperateError(stdKey, 3, types.OP_NOT_APPLICABLE,
			as.ExpWriteOp("wb", cdtOobExp(), as.ExpWriteFlagDefault))

		gm.Expect(ae.SubCode).To(gm.Equal(types.SubCodeOpNotCDTIndexOutOfBounds))
		assertMessageContains(ae, "out of bounds")
		assertEvalTrace(ae, "call", 1, []string{"call"})
	})

	gg.It("exp write CREATE_ONLY on existing bin stages no details", func() {
		// A policy rejection is not an expression diagnostic: rc 6 with NO
		// error details staged, even at verbosity 3.
		ae := expectOperateError(scratchKey, 3, types.BIN_EXISTS_ERROR,
			as.ExpWriteOp(binInt, as.ExpNumAdd(as.ExpIntBin(binInt), as.ExpIntVal(1)),
				as.ExpWriteFlagCreateOnly))

		assertNoDetails(ae, types.BIN_EXISTS_ERROR)
	})

	gg.It("exp write UPDATE_ONLY on missing bin stages no details", func() {
		ae := expectOperateError(scratchKey, 3, types.BIN_NOT_FOUND,
			as.ExpWriteOp(binMissing, as.ExpIntVal(1), as.ExpWriteFlagUpdateOnly))

		assertNoDetails(ae, types.BIN_NOT_FOUND)
	})

	gg.It("exp write nil without ALLOW_DELETE stages no details", func() {
		// A NIL result would delete the target bin; without ALLOW_DELETE that
		// is OP_NOT_APPLICABLE with NO error details (contrast the eval-fault
		// rows above, which stage message + trace under the same rc).
		ae := expectOperateError(scratchKey, 3, types.OP_NOT_APPLICABLE,
			as.ExpWriteOp(binInt, as.ExpNilValue(), as.ExpWriteFlagDefault))

		assertNoDetails(ae, types.OP_NOT_APPLICABLE)
	})

	gg.It("exp write nil with ALLOW_DELETE deletes the bin", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 3

		_, err := client.Operate(wp, scratchKey,
			as.ExpWriteOp(binInt, as.ExpNilValue(), as.ExpWriteFlagAllowDelete))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		rec, err := client.Get(nil, scratchKey)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rec).NotTo(gm.BeNil())
		gm.Expect(rec.Bins).NotTo(gm.HaveKey(binInt), "Expected bin to be deleted")
		gm.Expect(rec.Bins).To(gm.HaveKey("keep"), "Expected untouched bin to remain")
	})

	gg.It("exp write POLICY_NO_FAIL swallows the policy violation", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 3

		_, err := client.Operate(wp, scratchKey,
			as.ExpWriteOp(binInt, as.ExpNumAdd(as.ExpIntBin(binInt), as.ExpIntVal(1)),
				as.ExpWriteFlagCreateOnly|as.ExpWriteFlagPolicyNoFail))
		gm.Expect(err).NotTo(gm.HaveOccurred())

		// The CREATE_ONLY violation was swallowed; the bin is unchanged.
		rec, err := client.Get(nil, scratchKey)
		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rec).NotTo(gm.BeNil())
		gm.Expect(rec.Bins[binInt]).To(gm.Equal(10))
	})

	gg.It("exp write EVAL_NO_FAIL swallows the eval fault", func() {
		wp := as.NewWritePolicy(0, 0)
		wp.ErrorDetailVerbosity = 3

		rec, err := client.Operate(wp, scratchKey,
			as.ExpWriteOp(binInt, as.ExpNumDiv(as.ExpIntBin(binInt), as.ExpIntVal(0)),
				as.ExpWriteFlagEvalNoFail))

		gm.Expect(err).NotTo(gm.HaveOccurred())
		gm.Expect(rec).NotTo(gm.BeNil())
	})

	// ---------------------------------------------------------------------
	// 5. Filter-decision explainer (SERVER-1139): outcome (key 7) and the
	//    decisive operand pair (key 13), end to end.
	//
	//    The parity specs above assert phase and deciding op across verbs;
	//    these assert the explainer keys themselves, which no other
	//    integration spec exercises.
	// ---------------------------------------------------------------------

	gg.It("explainer reports a clean FALSE outcome with the deciding operands", func() {
		// bin x is 10; compare against 11 so the expression is well-formed and
		// simply does not match.
		ae := expectFilteredGet(3, as.ExpEq(as.ExpIntBin(binInt), as.ExpIntVal(11)),
			types.FILTERED_OUT)

		t := ae.ExpTrace
		gm.Expect(t).NotTo(gm.BeNil(), "Expected an explain trace at verbosity 3")
		gm.Expect(t.Phase).To(gm.Equal(as.ExpTracePhaseEval))
		gm.Expect(t.Outcome).To(gm.Equal(as.ExpTraceOutcomeFalse), "Expected a clean-FALSE outcome")

		// Operands are the first tier the budget drops, so presence is not
		// guaranteed; assert the shape only when the server sent them.
		if t.Operands != nil {
			gm.Expect(t.Operands).To(gm.HaveLen(2), "Operands are a [lhs, rhs] pair")
			gm.Expect(t.Operands[0]).To(gm.Equal("10"), "lhs is the bin value")
			gm.Expect(t.Operands[1]).To(gm.Equal("11"), "rhs is the literal")
		}
	})

	gg.It("explainer reports an absent-bin outcome without operands", func() {
		ae := expectFilteredGet(3, as.ExpEq(as.ExpIntBin(binMissing), as.ExpIntVal(2)),
			types.FILTERED_OUT)

		t := ae.ExpTrace
		gm.Expect(t).NotTo(gm.BeNil(), "Expected an explain trace at verbosity 3")
		gm.Expect(t.Phase).To(gm.Equal(as.ExpTracePhaseEval))
		gm.Expect(t.Outcome).To(gm.Equal(as.ExpTraceOutcomeAbsent), "Expected an absent outcome")
		// Operands ride only an outcome=FALSE comparison.
		gm.Expect(t.Operands).To(gm.BeNil(), "Absent outcomes carry no operand pair")
	})

	// ---------------------------------------------------------------------
	// 6. Multi-record paths: the query start-failure reply and a batch row.
	//    Both stage the trace from their own server site with a distinct
	//    message, and both reach the client through a different field walk
	//    than the single-record path.
	// ---------------------------------------------------------------------

	gg.It("query filter build failure surfaces a build trace on the start-failure reply", func() {
		qp := as.NewQueryPolicy()
		qp.ErrorDetailVerbosity = 3
		qp.FilterExpression = buildErrorExp()

		stm := as.NewStatement(stdKey.Namespace(), stdKey.SetName())
		rs, err := client.Query(qp, stm)

		// The failure may surface from Query itself or from the record stream.
		if err == nil {
			gm.Expect(rs).NotTo(gm.BeNil())
			for res := range rs.Results() {
				if res.Err != nil {
					err = res.Err
					break
				}
			}
		}

		ae := toAerospikeError(err, types.PARAMETER_ERROR)
		assertMessageContains(ae, "invalid filter expression in query")

		t := assertBuildTrace(ae)
		// A query filters many records per request, so the server hard-disables
		// the explainer here - build traces only, never outcome/operands.
		gm.Expect(t.Outcome).To(gm.Equal(-1), "A query trace must not carry an outcome")
		gm.Expect(t.Operands).To(gm.BeNil(), "A query trace must not carry operands")
	})

	gg.It("batch row filter build failure surfaces a build trace on the row", func() {
		bp := as.NewBatchPolicy()
		bp.ErrorDetailVerbosity = 3

		// The filter goes on the per-record policy, not the batch policy. A
		// batch-wide filter that fails to build aborts the whole batch before
		// any row is returned individually, and per §2.7 the server writes a
		// row's field 45 only where that row's reply is sent on its own.
		badPolicy := as.NewBatchReadPolicy()
		badPolicy.FilterExpression = buildErrorExp()

		recs := []*as.BatchRead{
			as.NewBatchRead(badPolicy, stdKey, nil),
			as.NewBatchRead(nil, scratchKey, nil),
		}

		// RespondAllKeys returns every row, so the call itself does not error;
		// the failure is reported on the row.
		_ = client.BatchGetComplex(bp, recs)

		bad := recs[0].BatchRecord
		gm.Expect(bad.ResultCode).To(gm.Equal(types.PARAMETER_ERROR), "Unexpected row result code")
		gm.Expect(bad.ServerMessage).To(gm.ContainSubstring("invalid filter expression in batch request"))

		t := bad.ExpTrace
		gm.Expect(t).NotTo(gm.BeNil(), "Expected a build trace on the failing row")
		gm.Expect(t.Phase).To(gm.Equal(as.ExpTracePhaseBuild))
		gm.Expect(t.ByteOffset).To(gm.BeNumerically(">=", 0), "Build traces carry byte_offset")

		// The sibling row is unaffected and carries no detail of its own.
		gm.Expect(recs[1].BatchRecord.ExpTrace).To(gm.BeNil(),
			"Detail must reset per row, not leak from the previous one")
	})
})
