// Copyright 2014-2021 Aerospike, Inc.
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
	"strings"

	"github.com/aerospike/aerospike-client-go/types"
)

// AerospikeError implements error interface for aerospike specific errors.
// All errors returning from the library are of this type.
// Errors resulting from Go's stdlib are not translated to this type, unless
// they are a net.Timeout error.
type AerospikeError struct {
	error

	Node       *Node
	ResultCode types.ResultCode
	InDoubt    bool
}

// SetInDoubt sets whether it is possible that the write transaction may have completed
// even though this error was generated.  This may be the case when a
// client error occurs (like timeout) after the command was sent to the server.
func (ase *AerospikeError) SetInDoubt(isRead bool, commandSentCounter int) {
	if !isRead && (commandSentCounter > 1 || (commandSentCounter == 1 && (ase.ResultCode == types.TIMEOUT || ase.ResultCode <= 0))) {
		ase.InDoubt = true
	}
}

// NewAerospikeError generates a new AerospikeError instance.
// If no message is provided, the result code will be translated into the default
// error message automatically.
// To be able to check for error type, you could use the following:
//   if aerr, ok := err.(AerospikeError); ok {
//       errCode := aerr.ResultCode
//       errMessage := aerr.Error()
//   }
func NewAerospikeError(code types.ResultCode, messages ...string) error {
	if len(messages) == 0 {
		messages = []string{types.ResultCodeToString(code)}
	}

	err := errors.New(strings.Join(messages, " "))
	return AerospikeError{error: err, ResultCode: code}
}

//revive:disable

var (
	ErrServerNotAvailable             = NewAerospikeError(types.SERVER_NOT_AVAILABLE)
	ErrKeyNotFound                    = NewAerospikeError(types.KEY_NOT_FOUND_ERROR)
	ErrRecordsetClosed                = NewAerospikeError(types.RECORDSET_CLOSED)
	ErrConnectionPoolEmpty            = NewAerospikeError(types.NO_AVAILABLE_CONNECTIONS_TO_NODE, "Connection pool is empty. This happens when either all connection are in-use already, or no connections were available")
	ErrTooManyConnectionsForNode      = NewAerospikeError(types.NO_AVAILABLE_CONNECTIONS_TO_NODE, "Connection limit reached for this node. This value is controlled via ClientPolicy.LimitConnectionsToQueueSize")
	ErrTooManyOpeningConnections      = NewAerospikeError(types.NO_AVAILABLE_CONNECTIONS_TO_NODE, "Too many connections are trying to open at once. This value is controlled via ClientPolicy.OpeningConnectionThreshold")
	ErrTimeout                        = NewAerospikeError(types.TIMEOUT, "command execution timed out on client: See `Policy.Timeout`")
	ErrUDFBadResponse                 = NewAerospikeError(types.UDF_BAD_RESPONSE, "Invalid UDF return value")
	ErrNoOperationsSpecified          = NewAerospikeError(types.INVALID_COMMAND, "No operations were passed to QueryExecute")
	ErrNoBinNamesAlloedInQueryExecute = NewAerospikeError(types.INVALID_COMMAND, "Statement.BinNames must be empty for QueryExecute")
	ErrFilteredOut                    = NewAerospikeError(types.FILTERED_OUT)
	ErrPartitionScanQueryNotSupported = NewAerospikeError(types.PARAMETER_ERROR, "Partition Scans/Queries are not supported by all nodes in this cluster")
	ErrScanTerminated                 = NewAerospikeError(types.SCAN_TERMINATED)
	ErrQueryTerminated                = NewAerospikeError(types.QUERY_TERMINATED)
)

//revive:enable
