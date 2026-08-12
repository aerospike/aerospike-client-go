//go:build go1.27

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

package sdk

import (
	"errors"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// ErrorKind classifies an [Error] into a category. It is the flattening of the
// Python SDK's exception hierarchy into a single enumeration; the family
// predicates on [Error] (IsSecurity, IsBinError, ...) recover the grouping.
type ErrorKind int

// The error categories.
const (
	// KindAerospike is the catch-all for server errors with no finer category.
	KindAerospike ErrorKind = iota
	// KindInvalidArgument is client-side input validation.
	KindInvalidArgument

	KindTimeout
	KindConnection
	KindInvalidNode
	KindInvalidNamespace

	KindSecurity
	KindAuthentication
	KindAuthorization

	KindGeneration
	KindQuota
	KindSerialization

	KindRecordNotFound
	KindRecordExists
	KindRecordTooBig
	KindFilteredOut

	KindBin
	KindBinExists
	KindBinNotFound
	KindBinType
	KindBinOpInvalid

	KindElement
	KindElementNotFound
	KindElementExists

	KindCapacity
	KindKeyBusy

	KindSecondaryIndex
	KindIndexNotFound
	KindIndexAlreadyExists

	KindQueryTerminated
	KindBackoff
	KindMaxErrorRate

	KindTransaction
	KindCommit
)

var errorKindNames = map[ErrorKind]string{
	KindAerospike: "Aerospike", KindInvalidArgument: "InvalidArgument",
	KindTimeout: "Timeout", KindConnection: "Connection",
	KindInvalidNode: "InvalidNode", KindInvalidNamespace: "InvalidNamespace",
	KindSecurity: "Security", KindAuthentication: "Authentication",
	KindAuthorization: "Authorization", KindGeneration: "Generation",
	KindQuota: "Quota", KindSerialization: "Serialization",
	KindRecordNotFound: "RecordNotFound", KindRecordExists: "RecordExists",
	KindRecordTooBig: "RecordTooBig", KindFilteredOut: "FilteredOut",
	KindBin: "Bin", KindBinExists: "BinExists", KindBinNotFound: "BinNotFound",
	KindBinType: "BinType", KindBinOpInvalid: "BinOpInvalid",
	KindElement: "Element", KindElementNotFound: "ElementNotFound",
	KindElementExists: "ElementExists", KindCapacity: "Capacity",
	KindKeyBusy: "KeyBusy", KindSecondaryIndex: "SecondaryIndex",
	KindIndexNotFound: "IndexNotFound", KindIndexAlreadyExists: "IndexAlreadyExists",
	KindQueryTerminated: "QueryTerminated", KindBackoff: "Backoff",
	KindMaxErrorRate: "MaxErrorRate", KindTransaction: "Transaction",
	KindCommit: "Commit",
}

// String implements fmt.Stringer.
func (k ErrorKind) String() string {
	if s, ok := errorKindNames[k]; ok {
		return s
	}
	return fmt.Sprintf("ErrorKind(%d)", int(k))
}

// resultCodeKinds maps server result codes onto categories. Codes absent from
// the table classify as KindAerospike.
var resultCodeKinds = map[types.ResultCode]ErrorKind{
	types.TIMEOUT:                       KindTimeout,
	types.KEY_NOT_FOUND_ERROR:           KindRecordNotFound,
	types.KEY_EXISTS_ERROR:              KindRecordExists,
	types.GENERATION_ERROR:              KindGeneration,
	types.PARAMETER_ERROR:               KindInvalidArgument,
	types.RECORD_TOO_BIG:                KindRecordTooBig,
	types.FILTERED_OUT:                  KindFilteredOut,
	types.BIN_EXISTS_ERROR:              KindBinExists,
	types.BIN_NOT_FOUND:                 KindBinNotFound,
	types.BIN_TYPE_ERROR:                KindBinType,
	types.BIN_NAME_TOO_LONG:             KindBin,
	types.OP_NOT_APPLICABLE:             KindBinOpInvalid,
	types.INVALID_NAMESPACE:             KindInvalidNamespace,
	types.INVALID_NODE_ERROR:            KindInvalidNode,
	types.SERIALIZE_ERROR:               KindSerialization,
	types.SECURITY_NOT_SUPPORTED:        KindSecurity,
	types.SECURITY_NOT_ENABLED:          KindSecurity,
	types.SECURITY_SCHEME_NOT_SUPPORTED: KindSecurity,
	types.INVALID_COMMAND:               KindSecurity,
	types.INVALID_FIELD:                 KindSecurity,
	types.ILLEGAL_STATE:                 KindSecurity,
	types.INVALID_USER:                  KindAuthentication,
	types.USER_ALREADY_EXISTS:           KindAuthentication,
	types.INVALID_PASSWORD:              KindAuthentication,
	types.EXPIRED_PASSWORD:              KindAuthentication,
	types.FORBIDDEN_PASSWORD:            KindAuthentication,
	types.INVALID_CREDENTIAL:            KindAuthentication,
	types.EXPIRED_SESSION:               KindAuthentication,
	types.INVALID_ROLE:                  KindAuthorization,
	types.ROLE_ALREADY_EXISTS:           KindAuthorization,
	types.INVALID_PRIVILEGE:             KindAuthorization,
	types.NOT_AUTHENTICATED:             KindAuthentication,
	types.ROLE_VIOLATION:                KindAuthorization,
	types.NOT_WHITELISTED:               KindAuthorization,
	types.QUOTA_EXCEEDED:                KindQuota,
	types.FAIL_ELEMENT_NOT_FOUND:        KindElementNotFound,
	types.FAIL_ELEMENT_EXISTS:           KindElementExists,
	types.DEVICE_OVERLOAD:               KindCapacity,
	types.KEY_BUSY:                      KindKeyBusy,
	types.XDR_KEY_BUSY:                  KindKeyBusy,
	types.INDEX_FOUND:                   KindIndexAlreadyExists,
	types.INDEX_NOTFOUND:                KindIndexNotFound,
	types.INDEX_OOM:                     KindSecondaryIndex,
	types.INDEX_NOTREADABLE:             KindSecondaryIndex,
	types.INDEX_GENERIC:                 KindSecondaryIndex,
	types.INDEX_NAME_MAXLEN:             KindSecondaryIndex,
	types.INDEX_MAXCOUNT:                KindSecondaryIndex,
	types.QUERY_ABORTED:                 KindQueryTerminated,
	types.QUERY_TERMINATED:              KindQueryTerminated,
	types.QUERY_QUEUEFULL:               KindBackoff,
	types.MAX_ERROR_RATE:                KindMaxErrorRate,
	types.MRT_BLOCKED:                   KindTransaction,
	types.MRT_VERSION_MISMATCH:          KindTransaction,
	types.MRT_EXPIRED:                   KindTransaction,
	types.MRT_TOO_MANY_WRITES:           KindTransaction,
	types.MRT_COMMITTED:                 KindTransaction,
	types.MRT_ABORTED:                   KindTransaction,
	types.MRT_ALREADY_LOCKED:            KindTransaction,
	types.MRT_MONITOR_EXISTS:            KindTransaction,
}

// KindForResultCode classifies a server result code.
func KindForResultCode(rc types.ResultCode) ErrorKind {
	if k, ok := resultCodeKinds[rc]; ok {
		return k
	}
	return KindAerospike
}

// Error is the SDK error type. It wraps the underlying core client error (see
// [Error.Unwrap], so errors.Is and errors.As reach through it) and adds the
// classification and server error detail the SDK surfaces.
//
// Error satisfies the standard error interface. It is returned by value as an
// interface-free pointer type so that a nil *Error is never accidentally
// wrapped into a non-nil error interface: SDK functions return the `error`
// interface and produce nil directly when they succeed.
type Error struct {
	kind          ErrorKind
	message       string
	resultCode    types.ResultCode
	hasResultCode bool
	inDoubt       bool

	subCode       uint32
	hasSubCode    bool
	serverMessage string

	source error
}

var _ error = (*Error)(nil)

// NewError builds a client-side SDK error with no server result code.
func NewError(kind ErrorKind, format string, args ...any) *Error {
	return &Error{kind: kind, message: fmt.Sprintf(format, args...)}
}

// ErrorFromResultCode builds an SDK error from a server result code.
func ErrorFromResultCode(rc types.ResultCode, message string, inDoubt bool) *Error {
	if message == "" {
		message = types.ResultCodeToString(rc)
	}
	return &Error{
		kind:          KindForResultCode(rc),
		message:       message,
		resultCode:    rc,
		hasResultCode: true,
		inDoubt:       inDoubt,
	}
}

// WrapError converts a core client error into an SDK error. It returns nil for
// a nil input, so it is safe to use directly on a core call's error return.
func WrapError(err as.Error) *Error {
	if err == nil {
		return nil
	}
	e := &Error{
		kind:    KindAerospike,
		message: err.Error(),
		inDoubt: err.IsInDoubt(),
		source:  err,
	}

	// The core Error interface does not expose the result code; the concrete
	// type carries it as a public field.
	var rc types.ResultCode
	var ae *as.AerospikeError
	if errors.As(error(err), &ae) {
		rc = ae.ResultCode
	}

	if rc != types.OK {
		e.resultCode = rc
		e.hasResultCode = true
		e.kind = KindForResultCode(rc)
	} else {
		// Client-side core errors carry no result code; classify the common
		// transport categories by matching the code the core assigned.
		switch {
		case err.Matches(types.TIMEOUT):
			e.kind = KindTimeout
		case err.Matches(types.NO_AVAILABLE_CONNECTIONS_TO_NODE, types.SERVER_NOT_AVAILABLE):
			e.kind = KindConnection
		case err.Matches(types.INVALID_NODE_ERROR):
			e.kind = KindInvalidNode
		case err.Matches(types.MAX_ERROR_RATE):
			e.kind = KindMaxErrorRate
		}
	}
	return e
}

// Kind reports the error's category.
func (e *Error) Kind() ErrorKind { return e.kind }

// Message reports the human-readable message.
func (e *Error) Message() string { return e.message }

// ResultCode reports the server result code and whether one was present.
func (e *Error) ResultCode() (types.ResultCode, bool) { return e.resultCode, e.hasResultCode }

// InDoubt reports whether a write may have completed despite the error.
func (e *Error) InDoubt() bool { return e.inDoubt }

// SubCode reports the server error subcode and whether one was returned.
//
// Subcodes are only populated when the Behavior asked for extended error
// detail and the server supports it (8.1.3+). A present subcode of 0 means
// "detail returned, but no finer subcode than the result code"; absent means
// no detail was returned at all. Subcode values are scoped to their parent
// result code and repeat across parents, so always match on the pair.
func (e *Error) SubCode() (uint32, bool) { return e.subCode, e.hasSubCode }

// ServerMessage reports the server-supplied message, when extended error
// detail was requested and returned.
func (e *Error) ServerMessage() string { return e.serverMessage }

// Error implements the error interface.
func (e *Error) Error() string {
	s := e.kind.String() + ": " + e.message
	if e.hasResultCode {
		s += fmt.Sprintf(" (result code: %d %s)", int(e.resultCode), types.ResultCodeToString(e.resultCode))
	}
	if e.serverMessage != "" {
		s += " -- server: " + e.serverMessage
	}
	return s
}

// Unwrap returns the underlying core client error, if any, so that errors.Is
// and errors.As reach the core error chain.
func (e *Error) Unwrap() error { return e.source }

// Matches reports whether the error carries any of the given result codes.
// It also consults the wrapped core error, so it matches the core client's
// own Matches semantics.
func (e *Error) Matches(codes ...types.ResultCode) bool {
	for _, c := range codes {
		if e.hasResultCode && e.resultCode == c {
			return true
		}
	}
	var ae as.Error
	if errors.As(e.source, &ae) {
		return ae.Matches(codes...)
	}
	return false
}

// IsSecurity reports whether the error is in the security family
// (security, authentication, or authorization).
func (e *Error) IsSecurity() bool {
	switch e.kind {
	case KindSecurity, KindAuthentication, KindAuthorization:
		return true
	}
	return false
}

// IsBinError reports whether the error is in the bin family.
func (e *Error) IsBinError() bool {
	switch e.kind {
	case KindBin, KindBinExists, KindBinNotFound, KindBinType, KindBinOpInvalid:
		return true
	}
	return false
}

// IsElementError reports whether the error is in the CDT element family.
func (e *Error) IsElementError() bool {
	switch e.kind {
	case KindElement, KindElementNotFound, KindElementExists:
		return true
	}
	return false
}

// IsCapacityError reports whether the error is in the capacity family.
func (e *Error) IsCapacityError() bool {
	return e.kind == KindCapacity || e.kind == KindKeyBusy
}

// IsSecondaryIndexError reports whether the error is in the secondary-index family.
func (e *Error) IsSecondaryIndexError() bool {
	switch e.kind {
	case KindSecondaryIndex, KindIndexNotFound, KindIndexAlreadyExists:
		return true
	}
	return false
}

// IsBackoffError reports whether the error asks the caller to back off.
func (e *Error) IsBackoffError() bool {
	return e.kind == KindBackoff || e.kind == KindMaxErrorRate
}

// IsTransactionError reports whether the error is in the transaction family.
func (e *Error) IsTransactionError() bool {
	return e.kind == KindTransaction || e.kind == KindCommit
}
