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

// ResultCode returns the ResultCode from AerospikeError object
func (ase AerospikeError) ResultCode() ResultCode {
	return ase.resultCode
}

////////////////////////////////////////////////////////////////////////////////
// Generator for Aerospike errors.
// If no message is provided, the result code will be translated into the default
// error message
func NewAerospikeError(code ResultCode, messages ...string) error {
	if len(messages) == 0 {
		messages = []string{ResultCodeToString(code)}
	}

	err := errors.New(strings.Join(messages, " "))
	return AerospikeError{error: err, resultCode: code}
}
