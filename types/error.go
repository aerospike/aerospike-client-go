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

func (this AerospikeError) ResultCode() ResultCode {
	return this.resultCode
}

func TypeNotSupportedErr() AerospikeError { return NewAerospikeError(999, "Value type not supported") }
func InvalidNodeErr(msgs ...string) AerospikeError {
	return NewAerospikeError(INVALID_NODE_ERROR, msgs...)
}
func TimeoutErr(msgs ...string) AerospikeError         { return NewAerospikeError(TIMEOUT) }
func SerializationErr(msgs ...string) AerospikeError   { return NewAerospikeError(SERIALIZE_ERROR) }
func ParseErr(msgs ...string) AerospikeError           { return NewAerospikeError(PARSE_ERROR) }
func ConnectionErr(msgs ...string) AerospikeError      { return NewAerospikeError(SERVER_NOT_AVAILABLE) }
func ScanTerminatedErr(msgs ...string) AerospikeError  { return NewAerospikeError(SCAN_TERMINATED) }
func QueryTerminatedErr(msgs ...string) AerospikeError { return NewAerospikeError(QUERY_TERMINATED) }
func CommandRejectedErr(msgs ...string) AerospikeError { return NewAerospikeError(COMMAND_REJECTED) }

// Generator for Aerospike errors.
// If no message is provided, the result code will be translated into the default
// error message
func NewAerospikeError(code ResultCode, messages ...string) AerospikeError {
	finalMsg := messages
	if len(finalMsg) == 0 {
		finalMsg = []string{ResultCodeToString(code)}
	}

	return AerospikeError{
		error:      errors.New(strings.Join(finalMsg, " ")),
		resultCode: code,
	}
}
