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
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

type QueryCommand struct {
	BaseMultiCommand

	policy    *QueryPolicy
	statement *Statement

	// RecordSet recordSet;
	Records chan *Record
}

func NewQueryCommand(node *Node, policy *QueryPolicy, statement *Statement, recChan chan *Record) *QueryCommand {
	// make recChan in case it is nil
	if recChan == nil {
		recChan = make(chan *Record, 1024)
	}

	return &QueryCommand{
		BaseMultiCommand: *NewMultiCommand(node),
		policy:           policy,
		statement:        statement,
		Records:          recChan,
	}
}

func (this *QueryCommand) getPolicy(ifc Command) Policy {
	return this.policy
}

func (this *QueryCommand) writeBuffer(ifc Command) error {
	var err error
	var functionArgBuffer []byte

	fieldCount := 0
	filterSize := 0
	binNameSize := 0

	this.begin()

	if this.statement.namespace != "" {
		this.dataOffset += len(this.statement.namespace) + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	if this.statement.indexName != "" {
		this.dataOffset += len(this.statement.indexName) + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	if this.statement.setName != "" {
		this.dataOffset += len(this.statement.setName) + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	if this.statement.filters != nil {
		this.dataOffset += int(FIELD_HEADER_SIZE)
		filterSize++ // num filters

		for _, filter := range this.statement.filters {
			sz, err := filter.estimateSize()
			if err != nil {
				return err
			}
			filterSize += sz
		}
		this.dataOffset += filterSize
		fieldCount++
	} else {
		// Calling query with no filters is more efficiently handled by a primary index scan.
		// Estimate scan options size.
		this.dataOffset += 2 + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	if this.statement.binNames != nil {
		this.dataOffset += int(FIELD_HEADER_SIZE)
		binNameSize++ // num bin names

		for _, binName := range this.statement.binNames {
			binNameSize += len(binName) + 1
		}
		this.dataOffset += binNameSize
		fieldCount++
	}

	if this.statement.taskId > 0 {
		this.dataOffset += 8 + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	if this.statement.functionName != "" {
		this.dataOffset += int(FIELD_HEADER_SIZE) + 1 // udf type
		this.dataOffset += len(this.statement.packageName) + int(FIELD_HEADER_SIZE)
		this.dataOffset += len(this.statement.functionName) + int(FIELD_HEADER_SIZE)

		if len(this.statement.functionArgs) > 0 {
			functionArgBuffer, err = PackValueArray(this.statement.functionArgs)
			if err != nil {
				return err
			}
		} else {
			functionArgBuffer = []byte{}
		}
		this.dataOffset += int(FIELD_HEADER_SIZE) + len(functionArgBuffer)
		fieldCount += 4
	}

	this.sizeBuffer()

	readAttr := INFO1_READ
	this.writeHeader(readAttr, 0, fieldCount, 0)

	if this.statement.namespace != "" {
		this.WriteFieldString(this.statement.namespace, NAMESPACE)
	}

	if this.statement.indexName != "" {
		this.WriteFieldString(this.statement.indexName, INDEX_NAME)
	}

	if this.statement.setName != "" {
		this.WriteFieldString(this.statement.setName, TABLE)
	}

	if this.statement.filters != nil {
		this.WriteFieldHeader(filterSize, INDEX_RANGE)
		this.dataBuffer[this.dataOffset] = byte(len(this.statement.filters))
		this.dataOffset++

		for _, filter := range this.statement.filters {
			this.dataOffset, err = filter.write(this.dataBuffer, this.dataOffset)
			if err != nil {
				return err
			}
		}
	} else {
		// Calling query with no filters is more efficiently handled by a primary index scan.
		this.WriteFieldHeader(2, SCAN_OPTIONS)
		priority := byte(this.policy.Priority)
		priority <<= 4
		this.dataBuffer[this.dataOffset] = priority
		this.dataOffset++
		this.dataBuffer[this.dataOffset] = byte(100)
		this.dataOffset++
	}

	if this.statement.binNames != nil {
		this.WriteFieldHeader(binNameSize, QUERY_BINLIST)
		this.dataBuffer[this.dataOffset] = byte(len(this.statement.binNames))
		this.dataOffset++

		for _, binName := range this.statement.binNames {
			len := copy(this.dataBuffer[this.dataOffset+1:], []byte(binName))
			this.dataBuffer[this.dataOffset] = byte(len)
			this.dataOffset += len + 1
		}
	}

	if this.statement.taskId > 0 {
		this.WriteFieldHeader(8, TRAN_ID)
		Buffer.Int64ToBytes(int64(this.statement.taskId), this.dataBuffer, this.dataOffset)
		this.dataOffset += 8
	}

	if this.statement.functionName != "" {
		this.WriteFieldHeader(1, UDF_OP)
		if this.statement.returnData {
			this.dataBuffer[this.dataOffset] = byte(1)
			this.dataOffset++
		} else {
			this.dataBuffer[this.dataOffset] = byte(2)
			this.dataOffset++
		}

		this.WriteFieldString(this.statement.packageName, UDF_PACKAGE_NAME)
		this.WriteFieldString(this.statement.functionName, UDF_FUNCTION)
		this.WriteFieldBytes(functionArgBuffer, UDF_ARGLIST)
	}
	this.end()

	return nil
}

func (this *QueryCommand) parseResult(ifc Command, conn *Connection) error {
	// close the channel
	defer close(this.Records)

	return this.BaseMultiCommand.parseResult(ifc, conn)
}
