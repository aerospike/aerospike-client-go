// Copyright 2013-2015 Aerospike, Inc.
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
	"reflect"

	. "github.com/aerospike/aerospike-client-go/types"
	// . "github.com/aerospike/aerospike-client-go/types/atomic"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type multiCommand interface {
	Stop()
}

type baseMultiCommand struct {
	*baseCommand

	recordset *Recordset

	terminationErrorType ResultCode

	errChan chan error

	resObjType  reflect.Type
	selectCases []reflect.SelectCase
}

func newMultiCommand(node *Node, recordset *Recordset) *baseMultiCommand {
	cmd := &baseMultiCommand{
		baseCommand: &baseCommand{node: node},
		recordset:   recordset,
	}

	// if a channel is assigned, assign its value type
	if cmd.recordset != nil && !cmd.recordset.objChan.IsNil() {
		// this channel must be of type chan *T
		cmd.resObjType = cmd.recordset.objChan.Type().Elem().Elem()

		cmd.selectCases = []reflect.SelectCase{
			reflect.SelectCase{Dir: reflect.SelectSend, Chan: cmd.recordset.objChan},
			reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(cmd.recordset.cancelled)},
		}
	}

	return cmd
}

func (cmd *baseMultiCommand) getNode(ifc command) (*Node, error) {
	return cmd.node, nil
}

func (cmd *baseMultiCommand) parseResult(ifc command, conn *Connection) error {
	// Read socket into receive buffer one record at a time.  Do not read entire receive size
	// because the receive buffer would be too big.
	status := true

	for status {
		// Read header.
		if err := cmd.readBytes(8); err != nil {
			return err
		}

		size := Buffer.BytesToInt64(cmd.dataBuffer, 0)
		receiveSize := int(size & 0xFFFFFFFFFFFF)

		if receiveSize > 0 {
			var err error
			if status, err = ifc.parseRecordResults(ifc, receiveSize); err != nil {
				return err
			}
		} else {
			status = false
		}
	}
	return nil
}

func (cmd *baseMultiCommand) parseKey(fieldCount int) (*Key, error) {
	var digest []byte
	var namespace, setName string
	var userKey Value
	var err error

	for i := 0; i < fieldCount; i++ {
		if err = cmd.readBytes(4); err != nil {
			return nil, err
		}

		fieldlen := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		if err = cmd.readBytes(fieldlen); err != nil {
			return nil, err
		}

		fieldtype := FieldType(cmd.dataBuffer[0])
		size := fieldlen - 1

		switch fieldtype {
		case DIGEST_RIPE:
			digest = make([]byte, size, size)
			copy(digest, cmd.dataBuffer[1:size+1])
		case NAMESPACE:
			namespace = string(cmd.dataBuffer[1 : size+1])
		case TABLE:
			setName = string(cmd.dataBuffer[1 : size+1])
		case KEY:
			if userKey, err = bytesToKeyValue(int(cmd.dataBuffer[1]), cmd.dataBuffer, 2, size-1); err != nil {
				return nil, err
			}
		}
	}

	return &Key{namespace: namespace, setName: setName, digest: digest, userKey: userKey}, nil
}

func (cmd *baseMultiCommand) readBytes(length int) error {
	if length > len(cmd.dataBuffer) {
		// Corrupted data streams can result in a huge length.
		// Do a sanity check here.
		if length > MaxBufferSize {
			return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Invalid readBytes length: %d", length))
		}
		cmd.dataBuffer = make([]byte, length)
	}

	_, err := cmd.conn.Read(cmd.dataBuffer, length)
	if err != nil {
		return err
	}

	cmd.dataOffset += length
	return nil
}

func (cmd *baseMultiCommand) parseRecordResults(ifc command, receiveSize int) (bool, error) {
	// Read/parse remaining message bytes one record at a time.
	cmd.dataOffset = 0

	for cmd.dataOffset < receiveSize {
		if err := cmd.readBytes(int(_MSG_REMAINING_HEADER_SIZE)); err != nil {
			cmd.recordset.Errors <- newNodeError(cmd.node, err)
			return false, err
		}
		resultCode := ResultCode(cmd.dataBuffer[5] & 0xFF)

		if resultCode != 0 {
			if resultCode == KEY_NOT_FOUND_ERROR {
				// consume the rest of the input buffer from the socket
				if cmd.dataOffset < receiveSize {
					if err := cmd.readBytes(receiveSize - cmd.dataOffset); err != nil {
						cmd.recordset.Errors <- newNodeError(cmd.node, err)
						return false, err
					}
				}
				return false, nil
			}
			err := NewAerospikeError(resultCode)
			cmd.recordset.Errors <- newNodeError(cmd.node, err)
			return false, err
		}

		info3 := int(cmd.dataBuffer[3])

		// If cmd is the end marker of the response, do not proceed further
		if (info3 & _INFO3_LAST) == _INFO3_LAST {
			return false, nil
		}

		generation := int(Buffer.BytesToUint32(cmd.dataBuffer, 6))
		expiration := TTL(int(Buffer.BytesToUint32(cmd.dataBuffer, 10)))
		fieldCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 18))
		opCount := int(Buffer.BytesToUint16(cmd.dataBuffer, 20))

		key, err := cmd.parseKey(fieldCount)
		if err != nil {
			cmd.recordset.Errors <- newNodeError(cmd.node, err)
			return false, err
		}

		// if there is a recordset, process the record traditionally
		// otherwise, it is supposed to be a record channel
		if cmd.selectCases == nil {
			// Parse bins.
			var bins BinMap

			for i := 0; i < opCount; i++ {
				if err := cmd.readBytes(8); err != nil {
					cmd.recordset.Errors <- newNodeError(cmd.node, err)
					return false, err
				}

				opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
				particleType := int(cmd.dataBuffer[5])
				nameSize := int(cmd.dataBuffer[7])

				if err := cmd.readBytes(nameSize); err != nil {
					cmd.recordset.Errors <- newNodeError(cmd.node, err)
					return false, err
				}
				name := string(cmd.dataBuffer[:nameSize])

				particleBytesSize := int((opSize - (4 + nameSize)))
				if err = cmd.readBytes(particleBytesSize); err != nil {
					cmd.recordset.Errors <- newNodeError(cmd.node, err)
					return false, err
				}
				value, err := bytesToParticle(particleType, cmd.dataBuffer, 0, particleBytesSize)
				if err != nil {
					cmd.recordset.Errors <- newNodeError(cmd.node, err)
					return false, err
				}

				if bins == nil {
					bins = make(BinMap, opCount)
				}
				bins[name] = value
			}

			// If the channel is full and it blocks, we don't want this command to
			// block forever, or panic in case the channel is closed in the meantime.
			select {
			// send back the result on the async channel
			case cmd.recordset.Records <- newRecord(cmd.node, key, bins, generation, expiration):
			case <-cmd.recordset.cancelled:
				return false, NewAerospikeError(cmd.terminationErrorType)
			}
		} else {
			obj := reflect.New(cmd.resObjType)
			if err := cmd.parseObject(obj, opCount, fieldCount, generation, expiration); err != nil {
				cmd.recordset.Errors <- newNodeError(cmd.node, err)
				return false, err
			}

			// set the object to send
			cmd.selectCases[0].Send = obj

			chosen, _, _ := reflect.Select(cmd.selectCases)
			switch chosen {
			case 0: // object sent
			case 1: // cancel channel is closed
				return false, NewAerospikeError(cmd.terminationErrorType)
			}
		}
	}

	return true, nil
}

func (cmd *baseMultiCommand) parseObject(
	obj reflect.Value,
	opCount int,
	fieldCount int,
	generation int,
	expiration int,
) error {
	for i := 0; i < opCount; i++ {
		if err := cmd.readBytes(8); err != nil {
			cmd.recordset.Errors <- newNodeError(cmd.node, err)
			return err
		}

		opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		particleType := int(cmd.dataBuffer[5])
		nameSize := int(cmd.dataBuffer[7])

		if err := cmd.readBytes(nameSize); err != nil {
			cmd.recordset.Errors <- newNodeError(cmd.node, err)
			return err
		}
		name := string(cmd.dataBuffer[:nameSize])

		particleBytesSize := int((opSize - (4 + nameSize)))
		if err := cmd.readBytes(particleBytesSize); err != nil {
			cmd.recordset.Errors <- newNodeError(cmd.node, err)
			return err
		}
		value, err := bytesToParticle(particleType, cmd.dataBuffer, 0, particleBytesSize)
		if err != nil {
			cmd.recordset.Errors <- newNodeError(cmd.node, err)
			return err
		}

		if err := setObjectField(obj, name, value); err != nil {
			return err
		}
	}

	return nil
}
