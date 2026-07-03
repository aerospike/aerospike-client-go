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

// Server error detail subcodes.
//
// When extended error detail is requested (see [BasePolicy.ErrorDetailVerbosity]),
// the server may attach a numeric subcode to a failure response. The subcode is
// surfaced on [AerospikeError.SubCode].
//
// Match on the (ResultCode, SubCode) pair. Subcode integer values are scoped to
// their parent ResultCode and are NOT globally unique - the value 1, for example,
// recurs under every parent status. A subcode is only meaningful when interpreted
// together with the result code. Always check the result code first.
//
// SubCodeNone (0) means "no subcode" - it is reserved universally and is the value
// returned when the server did not send a subcode (verbosity disabled, or the
// failing branch had no dispatchable subcode).
//
// This catalogue mirrors the server's per-status enums in as/include/base/proto.h
// and is server-version-specific. It is append-only: published values are immutable
// and are never renumbered or reused. New failure modes get new values appended to
// their group. Treat any subcode value not declared here as an opaque integer
// rather than assuming it is absent.
const (
	// SubCodeNone is returned when the server did not supply a subcode.
	SubCodeNone = 0

	// -------------------------------------------------------
	// Pairs with ResultCode.PARAMETER_ERROR (4)  [AS_ERR_PARAMETER]
	// -------------------------------------------------------

	// SubCodeParamTTLInvalid: per-record TTL exceeds the namespace's max-ttl.
	SubCodeParamTTLInvalid = 1

	// SubCodeParamBitsOffsetOutOfRange: bit op offset lands past the blob (or above the proto cap).
	SubCodeParamBitsOffsetOutOfRange = 2

	// SubCodeParamBitsSizeOutOfRange: bit op size is out of range (e.g. zero, or too large).
	SubCodeParamBitsSizeOutOfRange = 3

	// SubCodeParamBitsResizeExceeded: blob resize would exceed the maximum blob size.
	SubCodeParamBitsResizeExceeded = 4

	// SubCodeParamBinCountTooLarge: write would exceed the per-record bin-count limit (write path).
	SubCodeParamBinCountTooLarge = 5

	// SubCodeParamStringOpParamsInvalid: string op wire/expression args malformed or out of range.
	SubCodeParamStringOpParamsInvalid = 6

	// SubCodeParamStringOpInvalid: string op code or modifier/read class mismatch on the wire path.
	SubCodeParamStringOpInvalid = 7

	// SubCodeParamStringCtxNotApplicable: string context-eval path malformed.
	SubCodeParamStringCtxNotApplicable = 8

	// SubCodeParamStringIndexOutOfBounds: string modify/read index or code-point range out of bounds.
	SubCodeParamStringIndexOutOfBounds = 9

	// SubCodeParamStringRegexInvalid: string regex pattern invalid (compile / ICU failure).
	SubCodeParamStringRegexInvalid = 10

	// SubCodeParamStringUTF8Invalid: string or string op argument is not valid UTF-8.
	SubCodeParamStringUTF8Invalid = 11

	// -------------------------------------------------------
	// Pairs with ResultCode.PARTITION_UNAVAILABLE (11)  [AS_ERR_UNAVAILABLE]
	// -------------------------------------------------------

	// SubCodeUnavailInitialBalanceUnresolved: cluster is still resolving initial partition balance at startup.
	SubCodeUnavailInitialBalanceUnresolved = 1

	// SubCodeUnavailReplicaUnavailable: a needed replica is unavailable (likely a partition split).
	SubCodeUnavailReplicaUnavailable = 2

	// -------------------------------------------------------
	// Pairs with ResultCode.UNSUPPORTED_FEATURE (16)  [AS_ERR_UNSUPPORTED_FEATURE]
	// -------------------------------------------------------

	// SubCodeUnsuppFeatMRTRequiresStrongConsistency: MRT attempted against a non-SC (AP) namespace.
	SubCodeUnsuppFeatMRTRequiresStrongConsistency = 1

	// SubCodeUnsuppFeatGeneric: requested feature is unsupported in this context (generic).
	SubCodeUnsuppFeatGeneric = 2

	// -------------------------------------------------------
	// Pairs with ResultCode.BIN_NOT_FOUND (17)  [AS_ERR_BIN_NOT_FOUND]
	// -------------------------------------------------------

	// SubCodeBinNotFoundHLLCannotCreateWithOp: HLL op needs an existing bin and can't auto-create one.
	SubCodeBinNotFoundHLLCannotCreateWithOp = 1

	// SubCodeBinNotFoundStringValueNotFound: string modify on a missing bin (non-NO_FAIL path).
	SubCodeBinNotFoundStringValueNotFound = 2

	// -------------------------------------------------------
	// Pairs with ResultCode.BIN_NAME_TOO_LONG (21)  [AS_ERR_BIN_NAME]
	// -------------------------------------------------------

	// SubCodeBinNameCountTooLarge: write would exceed the per-record bin-count limit (UDF path).
	SubCodeBinNameCountTooLarge = 1

	// -------------------------------------------------------
	// Pairs with ResultCode.FAIL_FORBIDDEN (22)  [AS_ERR_FORBIDDEN]
	// -------------------------------------------------------

	// SubCodeForbidXDRFilterBlocked: write bounced by an XDR ship filter at the destination.
	SubCodeForbidXDRFilterBlocked = 1

	// SubCodeForbidSetCountStopWrites: set-level record-count stop-writes limit reached.
	SubCodeForbidSetCountStopWrites = 2

	// SubCodeForbidSetSizeStopWrites: set-level size stop-writes limit reached.
	SubCodeForbidSetSizeStopWrites = 3

	// SubCodeForbidClockSkewStopWrites: writes stopped due to cluster clock skew.
	SubCodeForbidClockSkewStopWrites = 4

	// SubCodeForbidReplaceConflictResolving: REPLACE / CREATE_OR_REPLACE forbidden while resolving conflicts.
	SubCodeForbidReplaceConflictResolving = 5

	// SubCodeForbidTruncated: write forbidden because the set/namespace is mid-truncate.
	SubCodeForbidTruncated = 6

	// Note: server subcodes 7 and 9 in this family are retired (masking violations
	// return ROLE_VIOLATION, not FORBIDDEN) and are intentionally not declared.

	// SubCodeForbidDurabilityViolation: non-durable delete forbidden (would violate durability).
	SubCodeForbidDurabilityViolation = 8

	// -------------------------------------------------------
	// Pairs with ResultCode.OP_NOT_APPLICABLE (26)  [AS_ERR_OP_NOT_APPLICABLE]
	// -------------------------------------------------------

	// SubCodeOpNotCDTIndexOutOfBounds: list index is outside the current element range.
	SubCodeOpNotCDTIndexOutOfBounds = 1

	// SubCodeOpNotCDTRankOutOfBounds: requested rank is past the current population.
	SubCodeOpNotCDTRankOutOfBounds = 2

	// SubCodeOpNotCDTBoundedListOverflow: insert would exceed an ordered+bounded list's cap.
	SubCodeOpNotCDTBoundedListOverflow = 3

	// SubCodeOpNotHLLIndexBitsUnset: HLL op needs index_bits but the sketch has none set.
	SubCodeOpNotHLLIndexBitsUnset = 4

	// SubCodeOpNotHLLCannotReduceIndexBits: union needs to reduce index_bits but folding isn't allowed.
	SubCodeOpNotHLLCannotReduceIndexBits = 5

	// SubCodeOpNotHLLCannotReduceMinhashBits: as above, for the minhash dimension.
	SubCodeOpNotHLLCannotReduceMinhashBits = 6

	// SubCodeOpNotHLLCannotFoldMinhash: fold blocked because the sketch carries minhash bits.
	SubCodeOpNotHLLCannotFoldMinhash = 7

	// SubCodeOpNotHLLFoldIndexBitsTooLarge: fold target index_bits >= current (fold can only reduce).
	SubCodeOpNotHLLFoldIndexBitsTooLarge = 8

	// SubCodeOpNotHLLIntersectMinhashMismatch: intersect inputs have mismatched minhash parameters.
	SubCodeOpNotHLLIntersectMinhashMismatch = 9

	// SubCodeOpNotStringConversionFailed: string to numeric conversion failed (strtoll/strtod).
	SubCodeOpNotStringConversionFailed = 10

	// SubCodeOpNotStringUTF8Invalid: source blob/string is not valid UTF-8 for an OP_NOT_APPLICABLE path.
	SubCodeOpNotStringUTF8Invalid = 11

	// -------------------------------------------------------
	// ResultCode.FILTERED_OUT (27) [AS_ERR_FILTERED_OUT] carries NO subcode:
	// the server emits AS_SUB_NONE plus a contextual "filtered out ..." message.
	// (The as_sub_filtered_t enum was removed server-side and never shipped, so
	// no SubCodeFiltered* constants are defined here. Match on the message, not a
	// subcode.)
	// -------------------------------------------------------

	// -------------------------------------------------------
	// Pairs with ResultCode.MRT_BLOCKED (120)  [AS_ERR_MRT_BLOCKED]
	// -------------------------------------------------------

	// SubCodeMRTBlockedRecordLocked: record is provisionally locked by another MRT.
	SubCodeMRTBlockedRecordLocked = 1

	// SubCodeMRTBlockedIDMismatch: op belongs to a different MRT than the one holding the lock.
	SubCodeMRTBlockedIDMismatch = 2
)
