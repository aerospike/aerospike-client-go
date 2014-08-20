// Copyright 2013-2014 Aerospike, Inc.
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

package types

import (
	"errors"
	"strings"
)

// Aerospike error implements error interface for aerospike specific errors
type AerospikeError struct {
	error

	resultCode ResultCode
}

func (ase *AerospikeError) ResultCode() ResultCode {
	return ase.resultCode
}

// Error Types
type ErrInvalidNode struct{ AerospikeError }
type ErrTimeout struct{ AerospikeError }
type ErrSerialization struct{ AerospikeError }
type ErrParse struct{ AerospikeError }
type ErrConnection struct{ AerospikeError }
type ErrQueryTerminated struct{ AerospikeError }
type ErrScanTerminated struct{ AerospikeError }
type ErrCommandRejected struct{ AerospikeError }
type ErrTypeNotSupported struct{ AerospikeError }

////////////////////////////////////////////////////////////////////////////////
// Generator for Aerospike errors.
// If no message is provided, the result code will be translated into the default
// error message
func NewAerospikeError(code ResultCode, messages ...string) (err error) {
	if len(messages) == 0 {
		messages = []string{ResultCodeToString(code)}
	}

	errMsg := errors.New(strings.Join(messages, " "))

	switch ResultCode(code) {
	case TYPE_NOT_SUPPORTED:
		err = ErrTypeNotSupported{AerospikeError{error: errMsg, resultCode: code}}

	case COMMAND_REJECTED:
		err = ErrCommandRejected{AerospikeError{error: errMsg, resultCode: code}}

	case QUERY_TERMINATED:
		err = ErrQueryTerminated{AerospikeError{error: errMsg, resultCode: code}}

	case SCAN_TERMINATED:
		err = ErrScanTerminated{AerospikeError{error: errMsg, resultCode: code}}

	case INVALID_NODE_ERROR:
		err = ErrInvalidNode{AerospikeError{error: errMsg, resultCode: code}}

	case PARSE_ERROR:
		err = ErrParse{AerospikeError{error: errMsg, resultCode: code}}

	case SERIALIZE_ERROR:
		err = ErrSerialization{AerospikeError{error: errMsg, resultCode: code}}

	case OK:
		err = AerospikeError{error: errMsg, resultCode: code}

	case SERVER_ERROR:
		err = AerospikeError{error: errMsg, resultCode: code}

	case KEY_NOT_FOUND_ERROR:
		err = AerospikeError{error: errMsg, resultCode: code}

	case GENERATION_ERROR:
		err = AerospikeError{error: errMsg, resultCode: code}

	case PARAMETER_ERROR:
		err = AerospikeError{error: errMsg, resultCode: code}

	case KEY_EXISTS_ERROR:
		err = AerospikeError{error: errMsg, resultCode: code}

	case BIN_EXISTS_ERROR:
		err = AerospikeError{error: errMsg, resultCode: code}

	case CLUSTER_KEY_MISMATCH:
		err = AerospikeError{error: errMsg, resultCode: code}

	case SERVER_MEM_ERROR:
		err = AerospikeError{error: errMsg, resultCode: code}

	case TIMEOUT:
		err = ErrTimeout{AerospikeError{error: errMsg, resultCode: code}}

	case NO_XDS:
		err = AerospikeError{error: errMsg, resultCode: code}

	case SERVER_NOT_AVAILABLE:
		err = ErrConnection{AerospikeError{error: errMsg, resultCode: code}}

	case BIN_TYPE_ERROR:
		err = AerospikeError{error: errMsg, resultCode: code}

	case RECORD_TOO_BIG:
		err = AerospikeError{error: errMsg, resultCode: code}

	case KEY_BUSY:
		err = AerospikeError{error: errMsg, resultCode: code}

	case SCAN_ABORT:
		err = AerospikeError{error: errMsg, resultCode: code}

	case UNSUPPORTED_FEATURE:
		err = AerospikeError{error: errMsg, resultCode: code}

	case BIN_NOT_FOUND:
		err = AerospikeError{error: errMsg, resultCode: code}

	case DEVICE_OVERLOAD:
		err = AerospikeError{error: errMsg, resultCode: code}

	case KEY_MISMATCH:
		err = AerospikeError{error: errMsg, resultCode: code}

	case UDF_BAD_RESPONSE:
		err = AerospikeError{error: errMsg, resultCode: code}

	case INDEX_FOUND:
		err = AerospikeError{error: errMsg, resultCode: code}

	case INDEX_NOTFOUND:
		err = AerospikeError{error: errMsg, resultCode: code}

	case INDEX_OOM:
		err = AerospikeError{error: errMsg, resultCode: code}

	case INDEX_NOTREADABLE:
		err = AerospikeError{error: errMsg, resultCode: code}

	case INDEX_GENERIC:
		err = AerospikeError{error: errMsg, resultCode: code}

	case INDEX_NAME_MAXLEN:
		err = AerospikeError{error: errMsg, resultCode: code}

	case INDEX_MAXCOUNT:
		err = AerospikeError{error: errMsg, resultCode: code}

	case QUERY_ABORTED:
		err = AerospikeError{error: errMsg, resultCode: code}

	case QUERY_QUEUEFULL:
		err = AerospikeError{error: errMsg, resultCode: code}

	case QUERY_TIMEOUT:
		err = AerospikeError{error: errMsg, resultCode: code}

	case QUERY_GENERIC:
		err = AerospikeError{error: errMsg, resultCode: code}

	default:
		err = AerospikeError{error: errMsg, resultCode: code}
	}

	return err
}
