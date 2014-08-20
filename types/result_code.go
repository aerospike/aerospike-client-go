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

// Database operation error codes.  The positive numbers align with the server
// side file proto.h.
type ResultCode int

const (
	// Asynchronous max concurrent database commands have been exceeded and therefore rejected.
	TYPE_NOT_SUPPORTED ResultCode = -7

	// Asynchronous max concurrent database commands have been exceeded and therefore rejected.
	COMMAND_REJECTED ResultCode = -6

	// Query was terminated by user.
	QUERY_TERMINATED ResultCode = -5

	// Scan was terminated by user.
	SCAN_TERMINATED ResultCode = -4

	// Chosen node is not currently active.
	INVALID_NODE_ERROR ResultCode = -3

	// Client parse error.
	PARSE_ERROR ResultCode = -2

	// Client serialization error.
	SERIALIZE_ERROR ResultCode = -1

	// Operation was successful.
	OK ResultCode = 0

	// Unknown server failure.
	SERVER_ERROR ResultCode = 1

	// On retrieving, touching or replacing a record that doesn't exist.
	KEY_NOT_FOUND_ERROR ResultCode = 2

	// On modifying a record with unexpected generation.
	GENERATION_ERROR ResultCode = 3

	// Bad parameter(s) were passed in database operation call.
	PARAMETER_ERROR ResultCode = 4

	// On create-only (write unique) operations on a record that already
	// exists.
	KEY_EXISTS_ERROR ResultCode = 5

	// On create-only (write unique) operations on a bin that already
	// exists.
	BIN_EXISTS_ERROR ResultCode = 6

	// Expected cluster ID was not received.
	CLUSTER_KEY_MISMATCH ResultCode = 7

	// Server has run out of memory.
	SERVER_MEM_ERROR ResultCode = 8

	// Client or server has timed out.
	TIMEOUT ResultCode = 9

	// XDS product is not available.
	NO_XDS ResultCode = 10

	// Server is not accepting requests.
	SERVER_NOT_AVAILABLE ResultCode = 11

	// Operation is not supported with configured bin type (single-bin or
	// multi-bin).
	BIN_TYPE_ERROR ResultCode = 12

	// Record size exceeds limit.
	RECORD_TOO_BIG ResultCode = 13

	// Too many concurrent operations on the same record.
	KEY_BUSY ResultCode = 14

	// Scan aborted by server.
	SCAN_ABORT ResultCode = 15

	// Unsupported Server Feature (e.g. Scan + UDF)
	UNSUPPORTED_FEATURE ResultCode = 16

	// Specified bin name does not exist in record.
	BIN_NOT_FOUND ResultCode = 17

	// Specified bin name does not exist in record.
	DEVICE_OVERLOAD ResultCode = 18

	// Key type mismatch.
	KEY_MISMATCH ResultCode = 19

	// A user defined function returned an error code.
	UDF_BAD_RESPONSE ResultCode = 100

	// Secondary index already exists.
	INDEX_FOUND ResultCode = 200

	// Requested secondary index does not exist.
	INDEX_NOTFOUND ResultCode = 201

	// Secondary index memory space exceeded.
	INDEX_OOM ResultCode = 202

	// Secondary index not available.
	INDEX_NOTREADABLE ResultCode = 203

	// Generic secondary index error.
	INDEX_GENERIC ResultCode = 204

	// Index name maximum length exceeded.
	INDEX_NAME_MAXLEN ResultCode = 205

	// Maximum number of indicies exceeded.
	INDEX_MAXCOUNT ResultCode = 206

	// Secondary index query aborted.
	QUERY_ABORTED ResultCode = 210

	// Secondary index queue full.
	QUERY_QUEUEFULL ResultCode = 211

	// Secondary index query timed out on server.
	QUERY_TIMEOUT ResultCode = 212

	// Generic query error.
	QUERY_GENERIC ResultCode = 213
)

// Should connection be put back into pool.
func KeepConnection(resultCode int) bool {
	switch ResultCode(resultCode) {
	case OK, // Exception did not originate on server.
		QUERY_TERMINATED,
		SCAN_TERMINATED,
		INVALID_NODE_ERROR,
		PARSE_ERROR,
		SERIALIZE_ERROR,
		SERVER_MEM_ERROR,
		TIMEOUT,
		SERVER_NOT_AVAILABLE,
		SCAN_ABORT,
		INDEX_OOM,
		QUERY_ABORTED,
		QUERY_TIMEOUT:
		return false

	default:
		return true
	}
}

// Return result code as a string.
func ResultCodeToString(resultCode ResultCode) string {
	switch ResultCode(resultCode) {
	case TYPE_NOT_SUPPORTED:
		return "Type cannot be converted to Value Type."

	case COMMAND_REJECTED:
		return "command rejected"

	case QUERY_TERMINATED:
		return "Query terminated"

	case SCAN_TERMINATED:
		return "Scan terminated"

	case INVALID_NODE_ERROR:
		return "Invalid node"

	case PARSE_ERROR:
		return "Parse error"

	case SERIALIZE_ERROR:
		return "Serialize error"

	case OK:
		return "ok"

	case SERVER_ERROR:
		return "Server error"

	case KEY_NOT_FOUND_ERROR:
		return "Key not found"

	case GENERATION_ERROR:
		return "Generation error"

	case PARAMETER_ERROR:
		return "Parameter error"

	case KEY_EXISTS_ERROR:
		return "Key already exists"

	case BIN_EXISTS_ERROR:
		return "Bin already exists"

	case CLUSTER_KEY_MISMATCH:
		return "Cluster key mismatch"

	case SERVER_MEM_ERROR:
		return "Server memory error"

	case TIMEOUT:
		return "Timeout"

	case NO_XDS:
		return "XDS not available"

	case SERVER_NOT_AVAILABLE:
		return "Server not available"

	case BIN_TYPE_ERROR:
		return "Bin type error"

	case RECORD_TOO_BIG:
		return "Record too big"

	case KEY_BUSY:
		return "Hot key"

	case SCAN_ABORT:
		return "Scan aborted"

	case UNSUPPORTED_FEATURE:
		return "Unsupported Server Feature"

	case BIN_NOT_FOUND:
		return "Bin not found"

	case DEVICE_OVERLOAD:
		return "Device overload"

	case KEY_MISMATCH:
		return "Key mismatch"

	case UDF_BAD_RESPONSE:
		return "UDF returned error"

	case INDEX_FOUND:
		return "Index already exists"

	case INDEX_NOTFOUND:
		return "Index not found"

	case INDEX_OOM:
		return "Index out of memory"

	case INDEX_NOTREADABLE:
		return "Index not readable"

	case INDEX_GENERIC:
		return "Index error"

	case INDEX_NAME_MAXLEN:
		return "Index name max length exceeded"

	case INDEX_MAXCOUNT:
		return "Index count exceeds max"

	case QUERY_ABORTED:
		return "Query aborted"

	case QUERY_QUEUEFULL:
		return "Query queue full"

	case QUERY_TIMEOUT:
		return "Query timeout"

	case QUERY_GENERIC:
		return "Query error"

	default:
		return ""
	}
}
