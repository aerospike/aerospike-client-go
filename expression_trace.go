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
	"strconv"
	"strings"
)

// Wire constants for the nested expression-trace map (mirror server proto.h).
// They are append-only.
const (
	// asErrorDetailKeyExpTrace is the top-level field-45 error-detail key carrying
	// the nested expression-trace map.
	asErrorDetailKeyExpTrace = 3

	// Nested trace keys.
	expTraceKeyPhase      = 1  // phase (uint; ExpTracePhaseBuild / ExpTracePhaseEval)
	expTraceKeyByteOffset = 2  // byte_offset into the msgpack expression payload (uint)
	expTraceKeyOp         = 3  // failing op name (str)
	expTraceKeyDepth      = 4  // true nesting depth of the fault (uint)
	expTraceKeyPath       = 5  // op-name chain root->fault (array of str)
	expTraceKeySnippet    = 6  // human-only rendered snippet of the failing element (str)
	expTraceKeyOutcome    = 7  // eval-phase outcome (uint; ExpTraceOutcome*)
	expTraceKeyLang       = 8  // source language (uint; ExpTraceLangMsgpack / ExpTraceLangAel)
	expTraceKeyAelOffset  = 9  // char offset into AEL source text (uint)
	expTraceKeyAelSpan    = 10 // byte width of the offending AEL source region (uint)
	expTraceKeyAelLine    = 11 // 1-based line in AEL source (uint; reserved)
	expTraceKeyAelCol     = 12 // 1-based column in AEL source (uint; reserved)
	expTraceKeyOperands   = 13 // decisive comparison's operand values (array of 2 str)
)

// Expression-trace semantic constants surfaced to consumers.
const (
	// ExpTracePhaseBuild indicates the expression build failed.
	ExpTracePhaseBuild = 1
	// ExpTracePhaseEval indicates expression evaluation failed.
	ExpTracePhaseEval = 2

	// ExpTraceOutcomeFault indicates evaluation faulted.
	ExpTraceOutcomeFault = 1
	// ExpTraceOutcomeFalse indicates the expression evaluated cleanly to FALSE -
	// the record was not matched, and nothing went wrong.
	ExpTraceOutcomeFalse = 2
	// ExpTraceOutcomeAbsent indicates a referenced bin or key was absent.
	ExpTraceOutcomeAbsent = 3

	// ExpTraceLangMsgpack is the msgpack source language (the implied default when
	// the lang key is absent).
	ExpTraceLangMsgpack = 1
	// ExpTraceLangAel is the AEL DSL source language.
	ExpTraceLangAel = 2

	// ExpTracePathTruncationSentinel is the "..." sentinel the server splices into
	// [ExpressionTrace.Path] when the true nesting depth exceeds the path-frame cap.
	// [ExpressionTrace.Depth] still reports the true count.
	ExpTracePathTruncationSentinel = "..."
)

// ExpressionTrace is a structured expression build/eval trace surfaced at
// error-detail verbosity 3.
//
// When extended error detail is requested at verbosity 3 (see
// [BasePolicy.ErrorDetailVerbosity]) and the server fails to build or evaluate an
// expression - a metadata/predicate filter (filter_exp) or an exp_read/exp_write
// operation - it attaches this trace as a nested map under the field-45 error-detail
// key 3. It is surfaced on [AerospikeError.ExpTrace].
//
// Expression build failures carry [types.PARAMETER_ERROR] and [types.SubCodeNone] (no
// subcode); the contextual message is on the error. Eval-phase traces ride the
// result code the eval produced ([types.FILTERED_OUT], [types.OP_NOT_APPLICABLE]),
// not PARAMETER_ERROR. The trace is purely additive diagnostic detail - it never
// changes the result code, subcode, or message-string format.
//
// Every field is optional. The server caps the whole error-detail payload and drops
// whole tiers in a fixed order when the budget is tight - Operands first, then
// Snippet, then Path - so those may be absent even within a present trace. Absent
// integer fields read as -1 (except Lang, which defaults to [ExpTraceLangMsgpack]);
// absent string fields read as "" and an absent Path/Operands reads as nil. Never
// require any field.
//
// Two coordinate spaces - do not conflate them. ByteOffset is a byte offset into the
// msgpack expression payload the client sent. The AelOffset/AelSpan pair are offsets
// into AEL source text - a different coordinate space, populated only when Lang is
// [ExpTraceLangAel].
type ExpressionTrace struct {
	// Phase that failed: [ExpTracePhaseBuild] or [ExpTracePhaseEval]; -1 when absent.
	Phase int

	// ByteOffset into the msgpack expression payload of the failing element, or -1
	// when absent. This is a coordinate into the wire payload the client sent - not
	// into AEL source text (see AelOffset).
	ByteOffset int

	// Op is the failing op name (pre-rendered server-side), or "" when absent.
	Op string

	// Depth is the true nesting depth of the fault, or -1 when absent. Reports the
	// true count even when Path was truncated to the frame cap.
	Depth int

	// Path is the op-name chain from root to fault, or nil when absent. May contain
	// an [ExpTracePathTruncationSentinel] ("...") element mid-slice when the true
	// nesting exceeded the server's path-frame cap; Depth still reports the true count.
	Path []string

	// Snippet is a human-only rendered snippet of the failing element, or "" when
	// absent (the server drops it once Operands are gone and the budget is still tight).
	Snippet string

	// Outcome is why the record was not matched, on an eval-phase trace:
	// [ExpTraceOutcomeFault], [ExpTraceOutcomeFalse] or [ExpTraceOutcomeAbsent];
	// -1 when absent. The build phase never emits it.
	Outcome int

	// Operands are the decisive comparison's operand values as [lhs, rhs] - e.g.
	// ["15", "18"] - or nil when absent. Emitted only for an [ExpTraceOutcomeFalse]
	// trace whose decisive op is a comparison, and dropped first by the budget, so
	// a FALSE outcome does not guarantee them. Server-rendered display strings
	// capped at 48 bytes: numbers always fit, non-scalars render as a placeholder
	// ("<blob>", "<collection>"), and only a long string bin is silently clipped.
	Operands []string

	// Lang is the source language: [ExpTraceLangMsgpack] or [ExpTraceLangAel]. An
	// absent lang key means msgpack (the default), so this reads as ExpTraceLangMsgpack
	// when the server omitted it.
	Lang int

	// AelOffset is the char offset into the AEL source text, or -1 when absent.
	// Populated only when Lang is [ExpTraceLangAel]. A different coordinate space
	// from ByteOffset.
	AelOffset int

	// AelSpan is the byte width of the offending AEL source region, or -1 when absent.
	// Populated only when Lang is [ExpTraceLangAel].
	AelSpan int
}

// String implements the Stringer interface.
func (t *ExpressionTrace) String() string {
	if t == nil {
		return "<nil>"
	}

	var sb strings.Builder
	sb.WriteString("ExpressionTrace[phase=")
	sb.WriteString(strconv.Itoa(t.Phase))
	sb.WriteString(", byteOffset=")
	sb.WriteString(strconv.Itoa(t.ByteOffset))
	if t.Op != "" {
		sb.WriteString(", op=")
		sb.WriteString(t.Op)
	}
	sb.WriteString(", depth=")
	sb.WriteString(strconv.Itoa(t.Depth))
	if t.Path != nil {
		sb.WriteString(", path=[")
		sb.WriteString(strings.Join(t.Path, " "))
		sb.WriteString("]")
	}
	if t.Snippet != "" {
		sb.WriteString(", snippet=")
		sb.WriteString(t.Snippet)
	}
	// 0 is not a wire value, so this also skips a zero-valued literal.
	if t.Outcome > 0 {
		sb.WriteString(", outcome=")
		sb.WriteString(strconv.Itoa(t.Outcome))
	}
	if t.Operands != nil {
		sb.WriteString(", operands=[")
		sb.WriteString(strings.Join(t.Operands, " "))
		sb.WriteString("]")
	}
	sb.WriteString(", lang=")
	sb.WriteString(strconv.Itoa(t.Lang))
	if t.AelOffset >= 0 {
		sb.WriteString(", aelOffset=")
		sb.WriteString(strconv.Itoa(t.AelOffset))
	}
	if t.AelSpan >= 0 {
		sb.WriteString(", aelSpan=")
		sb.WriteString(strconv.Itoa(t.AelSpan))
	}
	sb.WriteString("]")
	return sb.String()
}
