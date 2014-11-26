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

	. "github.com/aerospike/aerospike-client-go/types"
	. "github.com/aerospike/aerospike-client-go/types/atomic"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

const (
	_MAX_BUFFER_SIZE = 1024 * 1024 * 10 // 10 MB
)

type multiCommand interface {
	Stop()
	IsValid() bool
}

type baseMultiCommand struct {
	*baseCommand

	Records chan *Record
	Errors  chan error

	valid *AtomicBool
}

func newMultiCommand(node *Node, recChan chan *Record, errChan chan error) *baseMultiCommand {
	return &baseMultiCommand{
		baseCommand: &baseCommand{node: node},
		Records:     recChan,
		Errors:      errChan,
		valid:       NewAtomicBool(true),
	}
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

		fieldlen := int(uint32(Buffer.BytesToInt32(cmd.dataBuffer, 0)))
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
		if length > _MAX_BUFFER_SIZE {
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

func (cmd *baseMultiCommand) Stop() {
	cmd.valid.Set(false)
}

func (cmd *baseMultiCommand) IsValid() bool {
	return cmd.valid.Get()
}
