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

package aerospike

import (
	"fmt"
	"strconv"
	"strings"

	// . "github.com/aerospike/aerospike-client-go/logger"

	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type ReadCommand struct {
	SingleCommand

	policy   Policy
	binNames []string
	record   *Record
}

func NewReadCommand(cluster *Cluster, policy Policy, key *Key, binNames []string) *ReadCommand {
	newReadCmd := &ReadCommand{
		SingleCommand: *NewSingleCommand(cluster, key),
		binNames:      binNames,
	}

	if policy != nil {
		newReadCmd.policy = policy
	} else {
		newReadCmd.policy = NewPolicy()
	}

	return newReadCmd
}

func (this *ReadCommand) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *ReadCommand) writeBuffer(ifc Command) error {
	this.SetRead(this.key, this.binNames)
	// TODO: investigate why return an error without justification
	return nil
}

func (this *ReadCommand) parseResult(ifc Command, conn *Connection) error {
	// Read header.
	// Logger.Debug("ReadCommand Parse Result: trying to read %d bytes from the connection...", int(MSG_TOTAL_HEADER_SIZE))
	conn.Read(this.dataBuffer, int(MSG_TOTAL_HEADER_SIZE))

	// A number of these are commented out because we just don't care enough to read
	// that section of the header. If we do care, uncomment and check!
	// var sz int64 = Buffer.BytesToInt64(this.dataBuffer, 0)
	headerLength := int(this.dataBuffer[8])
	resultCode := ResultCode(this.dataBuffer[13] & 0xFF)
	generation := int(Buffer.BytesToInt32(this.dataBuffer, 14))
	expiration := TTL(int(Buffer.BytesToInt32(this.dataBuffer, 18)))
	fieldCount := int(Buffer.BytesToInt16(this.dataBuffer, 26)) // almost certainly 0
	opCount := int(Buffer.BytesToInt16(this.dataBuffer, 28))
	receiveSize := int(Buffer.MsgLenFromBytes(this.dataBuffer[2:]) - int64(headerLength))

	// Logger.Debug("ReadCommand Parse Result: resultCode: %d, headerLength: %d, generation: %d, expiration: %d, fieldCount: %d, opCount: %d, receiveSize: %d", resultCode, headerLength, generation, expiration, fieldCount, opCount, receiveSize)

	// Read remaining message bytes.
	if receiveSize > 0 {
		this.sizeBufferSz(receiveSize)
		conn.Read(this.dataBuffer, receiveSize)
	}

	if resultCode != 0 {
		if resultCode == KEY_NOT_FOUND_ERROR {
			return nil
		}

		if resultCode == UDF_BAD_RESPONSE {
			this.record, _ = this.parseRecord(opCount, fieldCount, generation, expiration)
			this.handleUdfError(resultCode)
		}
		// return new AerospikeException(resultCode);
		return NewAerospikeError(resultCode)
	}

	if opCount == 0 {
		// data Bin was not returned.
		this.record = NewRecord(this.key, nil, nil, generation, expiration)
		return nil
	}
	var err error
	this.record, err = this.parseRecord(opCount, fieldCount, generation, expiration)
	return err
}

func parseFailure(ret string) string {
	if list := strings.Split(ret, ":"); len(list) >= 3 {
		if code, err := strconv.Atoi(strings.Trim(list[2], " ")); err != nil {
			return fmt.Sprintf("%s", code)
		} else {
			return fmt.Sprintf("%s:%s %s", list[0], list[1], list[3])
		}
	} else {
		return ret
	}
}

func (this *ReadCommand) handleUdfError(resultCode ResultCode) error {
	if ret, exists := this.record.Bins["FAILURE"].(string); exists {
		message := parseFailure(ret)
		return NewAerospikeError(resultCode, message)
	} else {
		return NewAerospikeError(resultCode, "")
	}
}

func (this *ReadCommand) parseRecord(
	opCount int,
	fieldCount int,
	generation int,
	expiration int,
) (*Record, error) {
	var bins BinMap
	var duplicates []BinMap
	receiveOffset := 0

	// There can be fields in the response (setname etc).
	// But for now, ignore them. Expose them to the API if needed in the future.
	// Logger.Debug("field count: %d, databuffer: %v\n %s", fieldCount, this.dataBuffer, this.dataBuffer)
	if fieldCount != 0 {
		// Just skip over all the fields
		for i := 0; i < fieldCount; i++ {
			// Logger.Debug("%d", receiveOffset)
			fieldSize := int(Buffer.BytesToInt32(this.dataBuffer, receiveOffset))
			receiveOffset += (4 + fieldSize)
		}
	}

	for i := 0; i < opCount; i++ {
		opSize := int(Buffer.BytesToInt32(this.dataBuffer, receiveOffset))
		particleType := int(this.dataBuffer[receiveOffset+5])
		version := int(this.dataBuffer[receiveOffset+6])
		nameSize := int(this.dataBuffer[receiveOffset+7])
		// name := Buffer.utf8ToString(this.dataBuffer, receiveOffset+8, nameSize);
		name := string(this.dataBuffer[receiveOffset+8 : receiveOffset+8+nameSize])
		receiveOffset += 4 + 4 + nameSize

		particleBytesSize := int(opSize - (4 + nameSize))
		value, _ := BytesToParticle(particleType, this.dataBuffer, receiveOffset, particleBytesSize)
		receiveOffset += particleBytesSize

		var vmap BinMap

		if version > 0 || duplicates != nil {
			if duplicates == nil {
				duplicates = make([]BinMap, 4)
				duplicates = append(duplicates, bins)
				bins = nil

				for j := 0; j < version; j++ {
					duplicates = append(duplicates, nil)
				}
			} else {
				for j := len(duplicates); j < version+1; j++ {
					duplicates = append(duplicates, nil)
				}
			}

			vmap = duplicates[version]
			if vmap == nil {
				vmap = make(BinMap)
				duplicates[version] = vmap
			}
		} else {
			if bins == nil {
				bins = make(BinMap)
			}
			vmap = bins
		}
		vmap[name] = value
	}

	// Remove nil duplicates just in case there were holes in the version number space.
	if duplicates != nil {
		lastElem := 0
		for _, d := range duplicates {
			if d != nil {
				duplicates[lastElem] = d
				lastElem++
			}
		}
		duplicates = duplicates[:lastElem]
	}

	return NewRecord(this.key, bins, duplicates, generation, expiration), nil
}

func (this *ReadCommand) GetRecord() *Record {
	return this.record
}

func (this *ReadCommand) Execute() error {
	return this.execute(this)
}
