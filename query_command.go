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

import Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"

type queryCommand struct {
	*baseMultiCommand

	policy    *QueryPolicy
	statement *Statement
}

func newQueryCommand(node *Node, policy *QueryPolicy, statement *Statement, recChan chan *Record, errChan chan error) *queryCommand {
	// make recChan in case it is nil
	if recChan == nil {
		recChan = make(chan *Record, 1024)
	}

	// make errChan in case it is nil
	if errChan == nil {
		errChan = make(chan error, 1024)
	}

	return &queryCommand{
		baseMultiCommand: newMultiCommand(node, recChan, errChan),
		policy:           policy,
		statement:        statement,
	}
}

func (cmd *queryCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *queryCommand) writeBuffer(ifc command) (err error) {
	var functionArgBuffer []byte

	fieldCount := 0
	filterSize := 0
	binNameSize := 0

	cmd.begin()

	if cmd.statement.Namespace != "" {
		cmd.dataOffset += len(cmd.statement.Namespace) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if cmd.statement.IndexName != "" {
		cmd.dataOffset += len(cmd.statement.IndexName) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if cmd.statement.SetName != "" {
		cmd.dataOffset += len(cmd.statement.SetName) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if len(cmd.statement.Filters) > 0 {
		cmd.dataOffset += int(_FIELD_HEADER_SIZE)
		filterSize++ // num filters

		for _, filter := range cmd.statement.Filters {
			sz, err := filter.estimateSize()
			if err != nil {
				return err
			}
			filterSize += sz
		}
		cmd.dataOffset += filterSize
		fieldCount++
	} else {
		// Calling query with no filters is more efficiently handled by a primary index scan.
		// Estimate scan options size.
		cmd.dataOffset += (2 + int(_FIELD_HEADER_SIZE))
		fieldCount++
	}

	if len(cmd.statement.BinNames) > 0 {
		cmd.dataOffset += int(_FIELD_HEADER_SIZE)
		binNameSize++ // num bin names

		for _, binName := range cmd.statement.BinNames {
			binNameSize += len(binName) + 1
		}
		cmd.dataOffset += binNameSize
		fieldCount++
	}

	// make sure taskId is a non-zero random 64bit number
	cmd.statement.setTaskId()

	cmd.dataOffset += 8 + int(_FIELD_HEADER_SIZE)
	fieldCount++

	if cmd.statement.functionName != "" {
		cmd.dataOffset += int(_FIELD_HEADER_SIZE) + 1 // udf type
		cmd.dataOffset += len(cmd.statement.packageName) + int(_FIELD_HEADER_SIZE)
		cmd.dataOffset += len(cmd.statement.functionName) + int(_FIELD_HEADER_SIZE)

		if len(cmd.statement.functionArgs) > 0 {
			functionArgBuffer, err = packValueArray(cmd.statement.functionArgs)
			if err != nil {
				return err
			}
		} else {
			functionArgBuffer = []byte{}
		}
		cmd.dataOffset += int(_FIELD_HEADER_SIZE) + len(functionArgBuffer)
		fieldCount += 4
	}
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	readAttr := _INFO1_READ
	cmd.writeHeader(cmd.policy.GetBasePolicy(), readAttr, 0, fieldCount, 0)

	if cmd.statement.Namespace != "" {
		cmd.writeFieldString(cmd.statement.Namespace, NAMESPACE)
	}

	if cmd.statement.IndexName != "" {
		cmd.writeFieldString(cmd.statement.IndexName, INDEX_NAME)
	}

	if cmd.statement.SetName != "" {
		cmd.writeFieldString(cmd.statement.SetName, TABLE)
	}

	if len(cmd.statement.Filters) > 0 {
		cmd.writeFieldHeader(filterSize, INDEX_RANGE)
		cmd.dataBuffer[cmd.dataOffset] = byte(len(cmd.statement.Filters))
		cmd.dataOffset++

		for _, filter := range cmd.statement.Filters {
			cmd.dataOffset, err = filter.write(cmd.dataBuffer, cmd.dataOffset)
			if err != nil {
				return err
			}
		}
	} else {
		// Calling query with no filters is more efficiently handled by a primary index scan.
		cmd.writeFieldHeader(2, SCAN_OPTIONS)
		priority := byte(cmd.policy.Priority)
		priority <<= 4
		cmd.dataBuffer[cmd.dataOffset] = priority
		cmd.dataOffset++
		cmd.dataBuffer[cmd.dataOffset] = byte(100)
		cmd.dataOffset++
	}

	if len(cmd.statement.BinNames) > 0 {
		cmd.writeFieldHeader(binNameSize, QUERY_BINLIST)
		cmd.dataBuffer[cmd.dataOffset] = byte(len(cmd.statement.BinNames))
		cmd.dataOffset++

		for _, binName := range cmd.statement.BinNames {
			len := copy(cmd.dataBuffer[cmd.dataOffset+1:], binName)
			cmd.dataBuffer[cmd.dataOffset] = byte(len)
			cmd.dataOffset += len + 1
		}
	}

	cmd.writeFieldHeader(8, TRAN_ID)
	Buffer.Int64ToBytes(int64(cmd.statement.TaskId), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 8

	if cmd.statement.functionName != "" {
		cmd.writeFieldHeader(1, UDF_OP)
		if cmd.statement.returnData {
			cmd.dataBuffer[cmd.dataOffset] = byte(1)
			cmd.dataOffset++
		} else {
			cmd.dataBuffer[cmd.dataOffset] = byte(2)
			cmd.dataOffset++
		}

		cmd.writeFieldString(cmd.statement.packageName, UDF_PACKAGE_NAME)
		cmd.writeFieldString(cmd.statement.functionName, UDF_FUNCTION)
		cmd.writeFieldBytes(functionArgBuffer, UDF_ARGLIST)
	}
	cmd.end()

	return nil
}

func (cmd *queryCommand) parseResult(ifc command, conn *Connection) error {
	// close the channel
	defer close(cmd.Records)
	defer close(cmd.Errors)

	return cmd.baseMultiCommand.parseResult(ifc, conn)
}
