// Copyright 2014-2022 Aerospike, Inc.
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

	cmd *baseCommand
}

// recordParser initializes task with fields needed to query server nodes.
func newRecordParser(cmd *baseCommand) (*recordParser, Error) {
	rp := &recordParser{
		cmd: cmd,
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
		rp.skipFields()
		return nil
	}

	var version *uint64

	for i := 0; i < rp.fieldCount; i++ {
		len := Buffer.BytesToInt32(rp.cmd.dataBuffer, rp.cmd.dataOffset)
		rp.cmd.dataOffset += 4

		typ := FieldType(rp.cmd.dataBuffer[rp.cmd.dataOffset])
		rp.cmd.dataOffset++
		size := len - 1

		if typ == RECORD_VERSION {
			if size == 7 {
				version = Buffer.VersionBytesToUint64(rp.cmd.dataBuffer, rp.cmd.dataOffset)
			} else {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Record version field has invalid size: %v", size))
			}
		}
		rp.cmd.dataOffset += int(size)
	}

	if hasWrite {
		txn.OnWrite(key, version, rp.resultCode)
	} else {
		txn.OnRead(key, version)
	}

	return nil
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
