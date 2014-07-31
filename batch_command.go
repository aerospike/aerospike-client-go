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

	// . "github.com/aerospike/aerospike-client-go/logger"
	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

const (
	_MAX_BUFFER_SIZE = 1024 * 1024 * 10 // 10 MB
)

type MultiCommand interface {
	// parseRecordResults(ifc MultiCommand, receiveSize int) (bool, error)
	Stop()
	IsValid() bool
}

type BaseMultiCommand struct {
	BaseCommand

	bis   *Connection
	node  *Node
	valid bool //= true
}

func NewMultiCommand(node *Node) *BaseMultiCommand {
	return &BaseMultiCommand{
		node:  node,
		valid: true,
	}
}

func (this *BaseMultiCommand) getNode(ifc Command) (*Node, error) {
	return this.node, nil
}

func (this *BaseMultiCommand) parseResult(ifc Command, conn *Connection) error {
	// Read socket into receive buffer one record at a time.  Do not read entire receive size
	// because the thread local receive buffer would be too big.  Also, scan callbacks can nest
	// further database commands which contend with the receive buffer.
	this.bis = conn
	status := true

	for status {
		// Read header.
		if err := this.readBytes(8); err != nil {
			return err
		}

		size := Buffer.BytesToInt64(this.dataBuffer, 0)
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

func (this *BaseMultiCommand) parseKey(fieldCount int) (*Key, error) {
	var digest []byte
	var namespace, setName *string

	for i := 0; i < fieldCount; i++ {
		this.readBytes(4)
		fieldlen := int(Buffer.BytesToInt32(this.dataBuffer, 0))
		this.readBytes(fieldlen)
		fieldtype := FieldType(this.dataBuffer[0])
		size := fieldlen - 1

		if fieldtype == DIGEST_RIPE {
			digest = make([]byte, size)
			copy(digest, this.dataBuffer[1:size+1])
		} else if fieldtype == NAMESPACE {
			r := string(this.dataBuffer[1 : size+1])
			namespace = &r
		} else if fieldtype == TABLE {
			r := string(this.dataBuffer[1 : size+1])
			setName = &r
		}
	}
	return &Key{namespace: namespace, setName: setName, digest: digest}, nil
}

func (this *BaseMultiCommand) readBytes(length int) error {
	if length > len(this.dataBuffer) {
		// Corrupted data streams can result in a huge length.
		// Do a sanity check here.
		if length > _MAX_BUFFER_SIZE {
			return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Invalid readBytes length: %d", length))
		}
		this.dataBuffer = make([]byte, length)
	}

	if _, err := this.bis.Read(this.dataBuffer, length); err != nil {
		return err
	}

	this.dataOffset += length
	return nil
}

func (this *BaseMultiCommand) Stop() {
	this.valid = false
}

func (this *BaseMultiCommand) IsValid() bool {
	return this.valid
}
