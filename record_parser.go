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
	"fmt"

	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

// Task interface defines methods for asynchronous tasks.
type recordParser struct {
	resultCode types.ResultCode
	generation uint32
	expiration uint32
	fieldCount int
	opCount    int

	// serverMessage is the formatted server-side error detail
	// (message and/or subcode) when verbosity > 0 and the failing
	// branch dispatched a subcode. Empty when no detail was sent.
	serverMessage string

	// serverSubcode is the numeric server-supplied subcode (see SubCode*
	// constants in the types package). Defaults to [types.SubCodeNone] (0).
	serverSubcode types.SubCode

	// expTrace is the server-supplied expression build trace, parsed from the
	// nested error-detail key-3 map at verbosity 3 on expression build-failure
	// paths. nil when absent.
	expTrace *ExpressionTrace

	cmd *baseCommand
}

// recordParser initializes task with fields needed to query server nodes.
func newRecordParser(cmd *baseCommand) (*recordParser, Error) {
	rp := &recordParser{
		cmd:           cmd,
		serverSubcode: types.SubCodeNone,
	}

	// Read proto and check if compressed
	if _, err := rp.cmd.conn.Read(rp.cmd.dataBuffer, 8); err != nil {
		logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
		return nil, err
	}

	rp.cmd.dataOffset = 5
	if compressedSize := rp.cmd.compressedSize(); compressedSize > 0 {
		// Read compressed size
		if _, err := rp.cmd.conn.Read(rp.cmd.dataBuffer, 8); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return nil, err
		}

		if err := rp.cmd.conn.initInflater(true, compressedSize); err != nil {
			return nil, newError(types.PARSE_ERROR, fmt.Sprintf("Error setting up zlib inflater for size `%d`: %s", compressedSize, err.Error()))
		}
		rp.cmd.dataOffset = 13
	}

	sz := Buffer.BytesToInt64(rp.cmd.dataBuffer, 0)

	// Read remaining message bytes.
	receiveSize := int((sz & 0xFFFFFFFFFFFF))

	if receiveSize > 0 {
		cmd.receiveSize = int64(receiveSize)
		if err := rp.cmd.sizeBufferSz(receiveSize, false); err != nil {
			return rp, err
		}
		if _, err := rp.cmd.conn.Read(rp.cmd.dataBuffer, receiveSize); err != nil {
			logger.Logger.Debug("Connection error reading data for ReadCommand: %s", err.Error())
			return rp, err
		}
	}

	// Validate header to make sure we are at the beginning of a message
	if err := rp.cmd.validateHeader(sz); err != nil {
		return nil, err
	}

	rp.resultCode = types.ResultCode(rp.cmd.dataBuffer[rp.cmd.dataOffset] & 0xFF)
	rp.cmd.dataOffset++
	rp.generation = Buffer.BytesToUint32(rp.cmd.dataBuffer, rp.cmd.dataOffset)
	rp.cmd.dataOffset += 4
	rp.expiration = types.TTL(Buffer.BytesToUint32(rp.cmd.dataBuffer, rp.cmd.dataOffset))
	rp.cmd.dataOffset += 8
	rp.fieldCount = int(Buffer.BytesToUint16(rp.cmd.dataBuffer, rp.cmd.dataOffset))
	rp.cmd.dataOffset += 2
	rp.opCount = int(Buffer.BytesToUint16(rp.cmd.dataBuffer, rp.cmd.dataOffset))
	rp.cmd.dataOffset += 2

	return rp, nil
}

func (rp *recordParser) parseFields(
	txn *Txn,
	key *Key,
	hasWrite bool,
) Error {
	if txn == nil {
		rp.parseFieldsError()
		return nil
	}

	var version *uint64

	for i := 0; i < rp.fieldCount; i++ {
		fieldLen := Buffer.BytesToInt32(rp.cmd.dataBuffer, rp.cmd.dataOffset)
		rp.cmd.dataOffset += 4

		typ := FieldType(rp.cmd.dataBuffer[rp.cmd.dataOffset])
		rp.cmd.dataOffset++
		size := int(fieldLen) - 1

		if typ == RECORD_VERSION {
			if size == 7 {
				version = Buffer.VersionBytesToUint64(rp.cmd.dataBuffer, rp.cmd.dataOffset)
			} else {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Record version field has invalid size: %v", size))
			}
		} else if typ == ERROR_MESSAGE && size > 0 {
			rp.serverMessage = rp.parseErrorDetails(rp.cmd.dataOffset, size)
		}
		rp.cmd.dataOffset += size
	}

	if hasWrite {
		txn.OnWrite(key, version, rp.resultCode)
	} else {
		txn.OnRead(key, version)
	}

	return nil
}

// parseFieldsError walks fields when there's no Txn to track, capturing the
// server-side error detail field if present.
func (rp *recordParser) parseFieldsError() {
	for i := 0; i < rp.fieldCount; i++ {
		fieldLen := Buffer.BytesToInt32(rp.cmd.dataBuffer, rp.cmd.dataOffset)
		rp.cmd.dataOffset += 4

		typ := FieldType(rp.cmd.dataBuffer[rp.cmd.dataOffset])
		rp.cmd.dataOffset++
		size := int(fieldLen) - 1

		if typ == ERROR_MESSAGE && size > 0 {
			rp.serverMessage = rp.parseErrorDetails(rp.cmd.dataOffset, size)
		}
		rp.cmd.dataOffset += size
	}
}

// parseErrorDetails decodes the msgpack error-detail map the server attaches
// when ErrorDetailVerbosity > 0.
// Map keys: 1 = subcode (uint), 2 = message (string).
// Returns the formatted error message string, and stores the numeric subcode
// on rp.serverSubcode as a side effect.
func (rp *recordParser) parseErrorDetails(offset int, size int) string {
	end := offset + size
	if offset >= end {
		return ""
	}

	buf := rp.cmd.dataBuffer

	b := int(buf[offset]) & 0xFF
	offset++
	var count int

	if (b & 0xF0) == 0x80 {
		count = b & 0x0F
	} else if b == 0xDE && offset+2 <= end {
		count = int(Buffer.BytesToUint16(buf, offset))
		offset += 2
	} else if b == 0xDF && offset+4 <= end {
		count = int(Buffer.BytesToInt32(buf, offset))
		offset += 4
	} else {
		return ""
	}

	if count <= 0 {
		return ""
	}

	var message string
	var subcode int64 = -1

	for i := 0; i < count && offset < end; i++ {
		var key int
		b = int(buf[offset]) & 0xFF
		offset++

		if b <= 0x7F {
			key = b
		} else if b == 0xCC && offset < end {
			key = int(buf[offset]) & 0xFF
			offset++
		} else {
			break
		}

		switch key {
		case 1: // AS_ERROR_DETAIL_KEY_SUBCODE
			subcode = rp.unpackUint(offset, end)
			offset = rp.skipMsgpackValue(offset, end)
		case 2: // AS_ERROR_DETAIL_KEY_MESSAGE
			strOffset, strLen, ok := rp.unpackStr(offset, end)
			if ok {
				message = string(buf[strOffset : strOffset+strLen])
				offset = strOffset + strLen
			} else {
				offset = rp.skipMsgpackValue(offset, end)
			}
		case asErrorDetailKeyExpTrace: // nested expression-trace map (verbosity 3)
			rp.expTrace = rp.parseExpTrace(offset, end)
			offset = rp.skipMsgpackValue(offset, end)
		default:
			offset = rp.skipMsgpackValue(offset, end)
		}
	}

	// The server only serializes subcodes >= 1 (SubCodeNone = 0 is never
	// sent), so a parsed subcode always overrides the default.
	if subcode >= 0 {
		rp.serverSubcode = types.SubCode(subcode)
	}

	if message != "" && subcode >= 0 {
		return fmt.Sprintf("%s (subcode=%d)", message, subcode)
	} else if subcode >= 0 {
		return fmt.Sprintf("error subcode=%d", subcode)
	} else if message != "" {
		return message
	}
	return ""
}

// parseExpTrace decodes the nested expression-trace map (top-level error-detail
// key 3, only sent at verbosity 3 on expression build- and eval-failure paths)
// into an *ExpressionTrace.
//
// Reuses the shared msgpack decoder. Treats every trace key as optional (never
// requires key 1 - build failures carry types.SubCodeNone), skips unknown trace keys,
// tolerates the "..." path-truncation sentinel as an ordinary element, and never
// panics on a missing/truncated trace. An absent lang key surfaces as msgpack.
// Returns nil when the value is not a readable, non-empty map.
func (rp *recordParser) parseExpTrace(offset int, end int) *ExpressionTrace {
	if offset >= end {
		return nil
	}

	buf := rp.cmd.dataBuffer

	// Read nested map header (fixmap, map16, map32).
	b := int(buf[offset]) & 0xFF
	offset++
	var count int

	if (b & 0xF0) == 0x80 {
		count = b & 0x0F
	} else if b == 0xDE && offset+2 <= end {
		count = int(Buffer.BytesToUint16(buf, offset))
		offset += 2
	} else if b == 0xDF && offset+4 <= end {
		count = int(Buffer.BytesToInt32(buf, offset))
		offset += 4
	} else {
		return nil
	}

	if count <= 0 {
		return nil
	}

	// Absent integer fields read as -1; an absent lang surfaces as msgpack.
	t := &ExpressionTrace{
		Phase:      -1,
		ByteOffset: -1,
		Depth:      -1,
		Outcome:    -1,
		Lang:       ExpTraceLangMsgpack,
		AelOffset:  -1,
		AelSpan:    -1,
	}

	for i := 0; i < count && offset < end; i++ {
		// Read key (positive fixint or uint8).
		var key int
		b = int(buf[offset]) & 0xFF
		offset++

		if b <= 0x7F {
			key = b
		} else if b == 0xCC && offset < end {
			key = int(buf[offset]) & 0xFF
			offset++
		} else {
			break
		}

		switch key {
		case expTraceKeyPhase:
			t.Phase = int(rp.unpackUint(offset, end))
		case expTraceKeyByteOffset:
			t.ByteOffset = int(rp.unpackUint(offset, end))
		case expTraceKeyOp:
			t.Op = rp.unpackStrValue(offset, end)
		case expTraceKeyDepth:
			t.Depth = int(rp.unpackUint(offset, end))
		case expTraceKeyPath:
			t.Path = rp.unpackStrArray(offset, end)
		case expTraceKeySnippet:
			t.Snippet = rp.unpackStrValue(offset, end)
		case expTraceKeyOutcome:
			t.Outcome = int(rp.unpackUint(offset, end))
		case expTraceKeyOperands:
			t.Operands = rp.unpackStrArray(offset, end)
		case expTraceKeyLang:
			if lang := int(rp.unpackUint(offset, end)); lang >= 0 {
				t.Lang = lang
			}
		case expTraceKeyAelOffset:
			t.AelOffset = int(rp.unpackUint(offset, end))
		case expTraceKeyAelSpan:
			t.AelSpan = int(rp.unpackUint(offset, end))
		default:
			// Unknown / reserved trace key (ael_line, ael_col, etc.) - skip.
		}

		// Advance past the value regardless of whether the key was recognized.
		offset = rp.skipMsgpackValue(offset, end)
	}

	return t
}

// unpackStrValue decodes a msgpack string value to a string, or "" if the value
// at the offset is not a readable string.
func (rp *recordParser) unpackStrValue(offset int, end int) string {
	strOffset, strLen, ok := rp.unpackStr(offset, end)
	if !ok {
		return ""
	}
	return string(rp.cmd.dataBuffer[strOffset : strOffset+strLen])
}

// unpackStrArray decodes a msgpack array of strings (the expression-trace path).
// Preserves element order, keeps the "..." truncation sentinel as an ordinary
// element, and leaves an empty slot for any element that is not a readable string.
// Returns nil when the value is not a readable array.
func (rp *recordParser) unpackStrArray(offset int, end int) []string {
	if offset >= end {
		return nil
	}

	buf := rp.cmd.dataBuffer
	b := int(buf[offset]) & 0xFF
	offset++
	var length int

	if (b & 0xF0) == 0x90 {
		length = b & 0x0F
	} else if b == 0xDC && offset+2 <= end {
		length = int(Buffer.BytesToUint16(buf, offset))
		offset += 2
	} else if b == 0xDD && offset+4 <= end {
		length = int(Buffer.BytesToInt32(buf, offset))
		offset += 4
	} else {
		return nil
	}

	if length < 0 {
		return nil
	}

	result := make([]string, length)

	for i := 0; i < length && offset < end; i++ {
		result[i] = rp.unpackStrValue(offset, end)
		offset = rp.skipMsgpackValue(offset, end)
	}
	return result
}

// unpackUint decodes a msgpack unsigned integer at offset. Returns -1 on failure.
func (rp *recordParser) unpackUint(offset int, end int) int64 {
	if offset >= end {
		return -1
	}
	buf := rp.cmd.dataBuffer
	b := int(buf[offset]) & 0xFF

	switch {
	case b <= 0x7F:
		return int64(b)
	case b == 0xCC && offset+1 < end:
		return int64(buf[offset+1]) & 0xFF
	case b == 0xCD && offset+2 < end:
		return int64(Buffer.BytesToUint16(buf, offset+1))
	case b == 0xCE && offset+4 < end:
		return int64(Buffer.BytesToUint32(buf, offset+1))
	case b == 0xCF && offset+8 < end:
		return Buffer.BytesToInt64(buf, offset+1)
	}
	return -1
}

// unpackStr decodes a msgpack string header. Returns (offset, length, ok).
func (rp *recordParser) unpackStr(offset int, end int) (int, int, bool) {
	if offset >= end {
		return 0, 0, false
	}
	buf := rp.cmd.dataBuffer
	b := int(buf[offset]) & 0xFF
	offset++
	var l int

	switch {
	case (b & 0xE0) == 0xA0:
		l = b & 0x1F
	case b == 0xD9 && offset < end:
		l = int(buf[offset]) & 0xFF
		offset++
	case b == 0xDA && offset+1 < end:
		l = int(Buffer.BytesToUint16(buf, offset))
		offset += 2
	case b == 0xDB && offset+3 < end:
		l = int(Buffer.BytesToInt32(buf, offset))
		offset += 4
	default:
		return 0, 0, false
	}

	if l < 0 || offset+l > end {
		return 0, 0, false
	}
	return offset, l, true
}

// skipMsgpackValue advances past a single msgpack value, returning the new offset.
func (rp *recordParser) skipMsgpackValue(offset int, end int) int {
	if offset >= end {
		return end
	}
	buf := rp.cmd.dataBuffer
	b := int(buf[offset]) & 0xFF
	offset++

	// positive/negative fixint
	if b <= 0x7F || b >= 0xE0 {
		return offset
	}
	// fixstr
	if (b & 0xE0) == 0xA0 {
		return offset + (b & 0x1F)
	}
	// fixmap
	if (b & 0xF0) == 0x80 {
		count := (b & 0x0F) * 2
		for i := 0; i < count && offset < end; i++ {
			offset = rp.skipMsgpackValue(offset, end)
		}
		return offset
	}
	// fixarray
	if (b & 0xF0) == 0x90 {
		count := b & 0x0F
		for i := 0; i < count && offset < end; i++ {
			offset = rp.skipMsgpackValue(offset, end)
		}
		return offset
	}

	switch b {
	case 0xC0, 0xC2, 0xC3: // nil, false, true
		return offset
	case 0xCC, 0xD0: // uint8, int8
		return offset + 1
	case 0xCD, 0xD1: // uint16, int16
		return offset + 2
	case 0xCE, 0xD2, 0xCA: // uint32, int32, float32
		return offset + 4
	case 0xCF, 0xD3, 0xCB: // uint64, int64, float64
		return offset + 8
	case 0xD9, 0xC4: // str8, bin8
		if offset < end {
			return offset + 1 + (int(buf[offset]) & 0xFF)
		}
		return end
	case 0xDA, 0xC5: // str16, bin16
		if offset+1 < end {
			return offset + 2 + int(Buffer.BytesToUint16(buf, offset))
		}
		return end
	case 0xDB, 0xC6: // str32, bin32
		if offset+3 < end {
			return offset + 4 + int(Buffer.BytesToInt32(buf, offset))
		}
		return end
	case 0xDC, 0xDE: // array16, map16
		if offset+1 >= end {
			return end
		}
		mult := 1
		if b == 0xDE {
			mult = 2
		}
		count := int(Buffer.BytesToUint16(buf, offset)) * mult
		offset += 2
		for i := 0; i < count && offset < end; i++ {
			offset = rp.skipMsgpackValue(offset, end)
		}
		return offset
	case 0xDD, 0xDF: // array32, map32
		if offset+3 >= end {
			return end
		}
		mult := 1
		if b == 0xDF {
			mult = 2
		}
		count := int(Buffer.BytesToInt32(buf, offset)) * mult
		offset += 4
		for i := 0; i < count && offset < end; i++ {
			offset = rp.skipMsgpackValue(offset, end)
		}
		return offset
	}
	return end
}

func (rp *recordParser) skipFields() {
	// There can be fields in the response (setname etc).
	// But for now, ignore them. Expose them to the API if needed in the future.
	for i := 0; i < rp.fieldCount; i++ {
		fieldLen := Buffer.BytesToUint32(rp.cmd.dataBuffer, rp.cmd.dataOffset)
		rp.cmd.dataOffset += 4 + int(fieldLen)
	}
}

func (rp *recordParser) parseTranDeadline(txn *Txn) {
	for i := 0; i < rp.fieldCount; i++ {
		len := Buffer.BytesToInt32(rp.cmd.dataBuffer, rp.cmd.dataOffset)
		rp.cmd.dataOffset += 4

		typ := rp.cmd.dataBuffer[rp.cmd.dataOffset]
		rp.cmd.dataOffset++
		size := len - 1

		if FieldType(typ) == MRT_DEADLINE {
			deadline := Buffer.LittleBytesToInt32(rp.cmd.dataBuffer, rp.cmd.dataOffset)
			txn.deadline = int(deadline)
		}
		rp.cmd.dataOffset += int(size)
	}
}
func (rp *recordParser) parseRecord(key *Key, isOperation bool) (*Record, Error) {
	if rp.opCount == 0 {
		// Bin data was not returned.
		return newRecord(rp.cmd.node, key, nil, rp.generation, rp.expiration), nil
	}

	receiveOffset := rp.cmd.dataOffset

	bins := make(BinMap, rp.opCount)
	for i := 0; i < rp.opCount; i++ {
		opSize := int(Buffer.BytesToUint32(rp.cmd.dataBuffer, receiveOffset))
		particleType := int(rp.cmd.dataBuffer[receiveOffset+5])
		nameSize := int(rp.cmd.dataBuffer[receiveOffset+7])
		name := string(rp.cmd.dataBuffer[receiveOffset+8 : receiveOffset+8+nameSize])
		receiveOffset += 4 + 4 + nameSize

		particleBytesSize := opSize - (4 + nameSize)
		value, _ := bytesToParticle(particleType, rp.cmd.dataBuffer, receiveOffset, particleBytesSize)
		receiveOffset += particleBytesSize

		if bins == nil {
			bins = make(BinMap, rp.opCount)
		}

		if isOperation {
			// for operate list command results
			if prev, exists := bins[name]; exists {
				if res, ok := prev.(OpResults); ok {
					// List already exists.  Add to it.
					bins[name] = append(res, value)
				} else {
					// Make a list to store all values.
					bins[name] = OpResults{prev, value}
				}
			} else {
				bins[name] = value
			}
		} else {
			bins[name] = value
		}
	}

	return newRecord(rp.cmd.node, key, bins, rp.generation, rp.expiration), nil
}
