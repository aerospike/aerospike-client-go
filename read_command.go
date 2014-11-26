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
	. "github.com/aerospike/aerospike-client-go/logger"

	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type readCommand struct {
	*singleCommand

	policy   Policy
	binNames []string
	record   *Record
}

func newReadCommand(cluster *Cluster, policy Policy, key *Key, binNames []string) *readCommand {
	newReadCmd := &readCommand{
		singleCommand: newSingleCommand(cluster, key),
		binNames:      binNames,
	}

	if policy != nil {
		newReadCmd.policy = policy
	} else {
		newReadCmd.policy = NewPolicy()
	}

	return newReadCmd
}

func (cmd *readCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *readCommand) writeBuffer(ifc command) error {
	return cmd.setRead(cmd.key, cmd.binNames)
}

func (cmd *readCommand) parseResult(ifc command, conn *Connection) error {
	// Read header.
	// Logger.Debug("readCommand Parse Result: trying to read %d bytes from the connection...", int(_MSG_TOTAL_HEADER_SIZE))
	_, err := conn.Read(cmd.dataBuffer, int(_MSG_TOTAL_HEADER_SIZE))
	if err != nil {
		Logger.Warn("parse result error: " + err.Error())
		return err
	}

	// A number of these are commented out because we just don't care enough to read
	// that section of the header. If we do care, uncomment and check!
	sz := Buffer.BytesToInt64(cmd.dataBuffer, 0)
	headerLength := int(cmd.dataBuffer[8])
	resultCode := ResultCode(cmd.dataBuffer[13] & 0xFF)
	generation := int(uint32(Buffer.BytesToInt32(cmd.dataBuffer, 14)))
	expiration := TTL(int(uint32(Buffer.BytesToInt32(cmd.dataBuffer, 18))))
	fieldCount := int(uint16(Buffer.BytesToInt16(cmd.dataBuffer, 26))) // almost certainly 0
	opCount := int(uint16(Buffer.BytesToInt16(cmd.dataBuffer, 28)))
	receiveSize := int((sz & 0xFFFFFFFFFFFF) - int64(headerLength))

	// Logger.Debug("readCommand Parse Result: resultCode: %d, headerLength: %d, generation: %d, expiration: %d, fieldCount: %d, opCount: %d, receiveSize: %d", resultCode, headerLength, generation, expiration, fieldCount, opCount, receiveSize)

	// Read remaining message bytes.
	if receiveSize > 0 {
		if err = cmd.sizeBufferSz(receiveSize); err != nil {
			return err
		}
		_, err = conn.Read(cmd.dataBuffer, receiveSize)
		if err != nil {
			Logger.Warn("parse result error: " + err.Error())
			return err
		}

	}

	if resultCode != 0 {
		if resultCode == KEY_NOT_FOUND_ERROR {
			return nil
		}

		if resultCode == UDF_BAD_RESPONSE {
			cmd.record, _ = cmd.parseRecord(opCount, fieldCount, generation, expiration)
			err := cmd.handleUdfError(resultCode)
			Logger.Warn("UDF execution error: " + err.Error())
			return err
		}

		return NewAerospikeError(resultCode)
	}

	if opCount == 0 {
		// data Bin was not returned.
		cmd.record = newRecord(cmd.node, cmd.key, nil, nil, generation, expiration)
		return nil
	}

	cmd.record, err = cmd.parseRecord(opCount, fieldCount, generation, expiration)
	if err != nil {
		return err
	}

	return nil
}

func (cmd *readCommand) handleUdfError(resultCode ResultCode) error {
	if ret, exists := cmd.record.Bins["FAILURE"]; exists {
		return NewAerospikeError(resultCode, ret.(string))
	}
	return NewAerospikeError(resultCode)
}

func (cmd *readCommand) parseRecord(
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
	// Logger.Debug("field count: %d, databuffer: %v", fieldCount, cmd.dataBuffer)
	if fieldCount != 0 {
		// Just skip over all the fields
		for i := 0; i < fieldCount; i++ {
			// Logger.Debug("%d", receiveOffset)
			fieldSize := int(uint32(Buffer.BytesToInt32(cmd.dataBuffer, receiveOffset)))
			receiveOffset += (4 + fieldSize)
		}
	}

	for i := 0; i < opCount; i++ {
		opSize := int(uint32(Buffer.BytesToInt32(cmd.dataBuffer, receiveOffset)))
		particleType := int(cmd.dataBuffer[receiveOffset+5])
		version := int(cmd.dataBuffer[receiveOffset+6])
		nameSize := int(cmd.dataBuffer[receiveOffset+7])
		name := string(cmd.dataBuffer[receiveOffset+8 : receiveOffset+8+nameSize])
		receiveOffset += 4 + 4 + nameSize

		particleBytesSize := int(opSize - (4 + nameSize))
		value, _ := bytesToParticle(particleType, cmd.dataBuffer, receiveOffset, particleBytesSize)
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

	return newRecord(cmd.node, cmd.key, bins, duplicates, generation, expiration), nil
}

func (cmd *readCommand) GetRecord() *Record {
	return cmd.record
}

func (cmd *readCommand) Execute() error {
	return cmd.execute(cmd)
}
