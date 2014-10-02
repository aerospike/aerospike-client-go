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
	"errors"
	"fmt"
	"strings"
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"

	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

const (
	// Flags commented out are not supported by cmd client.
	// Contains a read operation.
	_INFO1_READ int = (1 << 0)
	// Get all bins.
	_INFO1_GET_ALL int = (1 << 1)

	// Do not read the bins
	_INFO1_NOBINDATA int = (1 << 5)

	// Create or update record
	_INFO2_WRITE int = (1 << 0)
	// Fling a record into the belly of Moloch.
	_INFO2_DELETE int = (1 << 1)
	// Update if expected generation == old.
	_INFO2_GENERATION int = (1 << 2)
	// Update if new generation >= old, good for restore.
	_INFO2_GENERATION_GT int = (1 << 3)
	// Create a duplicate on a generation collision.
	_INFO2_GENERATION_DUP int = (1 << 4)
	// Create only. Fail if record already exists.
	_INFO2_CREATE_ONLY int = (1 << 5)

	// This is the last of a multi-part message.
	_INFO3_LAST int = (1 << 0)
	// Update only. Merge bins.
	_INFO3_UPDATE_ONLY int = (1 << 3)

	// Create or completely replace record.
	_INFO3_CREATE_OR_REPLACE int = (1 << 4)
	// Completely replace existing record only.
	_INFO3_REPLACE_ONLY int = (1 << 5)

	_MSG_TOTAL_HEADER_SIZE     uint8 = 30
	_FIELD_HEADER_SIZE         uint8 = 5
	_OPERATION_HEADER_SIZE     uint8 = 8
	_MSG_REMAINING_HEADER_SIZE uint8 = 22
	_DIGEST_SIZE               uint8 = 20
	_CL_MSG_VERSION            int64 = 2
	_AS_MSG_TYPE               int64 = 3
)

// command intrerface describes all commands available
type command interface {
	getPolicy(ifc command) Policy

	setConnection(conn *Connection)
	getConnection() *Connection

	writeBuffer(ifc command) error
	getNode(ifc command) (*Node, error)
	parseResult(ifc command, conn *Connection) error
	parseRecordResults(ifc command, receiveSize int) (bool, error)

	execute(ifc command) error
	// Executes the command
	Execute() error
}

// Holds data buffer for the command
type baseCommand struct {
	node *Node
	conn *Connection

	dataBuffer []byte
	dataOffset int
}

// Writes the command for write operations
func (cmd *baseCommand) setWrite(policy *WritePolicy, operation OperationType, key *Key, bins []*Bin) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key)

	if policy.SendKey {
		// field header size + key size
		cmd.dataOffset += key.userKey.estimateSize() + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	for _, bin := range bins {
		cmd.estimateOperationSizeForBin(bin)
	}
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeaderWithPolicy(policy, 0, _INFO2_WRITE, fieldCount, len(bins))
	cmd.writeKey(key)

	if policy.SendKey {
		cmd.writeFieldValue(key.userKey, KEY)
	}

	for _, bin := range bins {
		if err := cmd.writeOperationForBin(bin, operation); err != nil {
			return err
		}
	}
	cmd.end()

	return nil
}

// Writes the command for delete operations
func (cmd *baseCommand) setDelete(policy *WritePolicy, key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeaderWithPolicy(policy, 0, _INFO2_WRITE|_INFO2_DELETE, fieldCount, 0)
	cmd.writeKey(key)
	cmd.end()
	return nil

}

// Writes the command for touch operations
func (cmd *baseCommand) setTouch(policy *WritePolicy, key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key)
	cmd.estimateOperationSize()
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeaderWithPolicy(policy, 0, _INFO2_WRITE, fieldCount, 1)
	cmd.writeKey(key)
	cmd.writeOperationForOperationType(TOUCH)
	cmd.end()
	return nil

}

// Writes the command for exist operations
func (cmd *baseCommand) setExists(key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeader(_INFO1_READ|_INFO1_NOBINDATA, 0, fieldCount, 0)
	cmd.writeKey(key)
	cmd.end()
	return nil

}

// Writes the command for get operations (all bins)
func (cmd *baseCommand) setReadForKeyOnly(key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeader(_INFO1_READ|_INFO1_GET_ALL, 0, fieldCount, 0)
	cmd.writeKey(key)
	cmd.end()
	return nil

}

// Writes the command for get operations (specified bins)
func (cmd *baseCommand) setRead(key *Key, binNames []string) (err error) {
	if binNames != nil && len(binNames) > 0 {
		cmd.begin()
		fieldCount := cmd.estimateKeySize(key)

		for _, binName := range binNames {
			cmd.estimateOperationSizeForBinName(binName)
		}
		if err = cmd.sizeBuffer(); err != nil {
			return nil
		}
		cmd.writeHeader(_INFO1_READ, 0, fieldCount, len(binNames))
		cmd.writeKey(key)

		for _, binName := range binNames {
			cmd.writeOperationForBinName(binName, READ)
		}
		cmd.end()
	} else {
		err = cmd.setReadForKeyOnly(key)
	}

	return err
}

// Writes the command for getting metadata operations
func (cmd *baseCommand) setReadHeader(key *Key) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key)
	cmd.estimateOperationSizeForBinName("")
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	// The server does not currently return record header data with _INFO1_NOBINDATA attribute set.
	// The workaround is to request a non-existent bin.
	// TODO: Fix this on server.
	//command.setRead(_INFO1_READ | _INFO1_NOBINDATA);
	cmd.writeHeader(_INFO1_READ, 0, fieldCount, 1)

	cmd.writeKey(key)
	cmd.writeOperationForBinName("", READ)
	cmd.end()
	return nil

}

// Implements different command operations
func (cmd *baseCommand) setOperate(policy *WritePolicy, key *Key, operations []*Operation) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key)
	readAttr := 0
	writeAttr := 0
	readHeader := false

	for _, operation := range operations {
		switch operation.OpType {
		case READ:
			readAttr |= _INFO1_READ

			// Read all bins if no bin is specified.
			if operation.BinName == nil {
				readAttr |= _INFO1_GET_ALL
			}

		case READ_HEADER:
			// The server does not currently return record header data with _INFO1_NOBINDATA attribute set.
			// The workaround is to request a non-existent bin.
			// TODO: Fix this on server.
			// readAttr |= _INFO1_READ | _INFO1_NOBINDATA
			readAttr |= _INFO1_READ
			readHeader = true

		default:
			writeAttr = _INFO2_WRITE
		}
		cmd.estimateOperationSizeForOperation(operation)
	}
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	if writeAttr != 0 {
		cmd.writeHeaderWithPolicy(policy, readAttr, writeAttr, fieldCount, len(operations))
	} else {
		cmd.writeHeader(readAttr, writeAttr, fieldCount, len(operations))
	}
	cmd.writeKey(key)

	for _, operation := range operations {
		if err := cmd.writeOperationForOperation(operation); err != nil {
			return err
		}
	}

	if readHeader {
		if err := cmd.writeOperationForBin(nil, READ); err != nil {
			return err
		}
	}
	cmd.end()

	return nil
}

func (cmd *baseCommand) setUdf(key *Key, packageName string, functionName string, args []Value) error {
	cmd.begin()
	fieldCount := cmd.estimateKeySize(key)
	argBytes, err := packValueArray(args)
	if err != nil {
		return err
	}

	fieldCount += cmd.estimateUdfSize(packageName, functionName, argBytes)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	cmd.writeHeader(0, _INFO2_WRITE, fieldCount, 0)
	cmd.writeKey(key)
	cmd.writeFieldString(packageName, UDF_PACKAGE_NAME)
	cmd.writeFieldString(functionName, UDF_FUNCTION)
	cmd.writeFieldBytes(argBytes, UDF_ARGLIST)
	cmd.end()

	return nil
}

func (cmd *baseCommand) setBatchExists(batchNamespace *batchNamespace) error {
	// Estimate buffer size
	cmd.begin()
	keys := batchNamespace.keys
	byteSize := len(keys) * int(_DIGEST_SIZE)

	cmd.dataOffset += len(*batchNamespace.namespace) +
		int(_FIELD_HEADER_SIZE) + byteSize + int(_FIELD_HEADER_SIZE)
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	cmd.writeHeader(_INFO1_READ|_INFO1_NOBINDATA, 0, 2, 0)
	cmd.writeFieldString(*batchNamespace.namespace, NAMESPACE)
	cmd.writeFieldHeader(byteSize, DIGEST_RIPE_ARRAY)

	for _, key := range keys {
		copy(cmd.dataBuffer[cmd.dataOffset:], key.digest)
		cmd.dataOffset += len(key.digest)
	}
	cmd.end()

	return nil
}

func (cmd *baseCommand) setBatchGet(batchNamespace *batchNamespace, binNames map[string]struct{}, readAttr int) error {
	// Estimate buffer size
	cmd.begin()
	keys := batchNamespace.keys
	byteSize := len(keys) * int(_DIGEST_SIZE)

	cmd.dataOffset += len(*batchNamespace.namespace) +
		int(_FIELD_HEADER_SIZE) + byteSize + int(_FIELD_HEADER_SIZE)

	if binNames != nil {
		for binName := range binNames {
			cmd.estimateOperationSizeForBinName(binName)
		}
	}
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}

	operationCount := 0
	if binNames != nil {
		operationCount = len(binNames)
	}
	cmd.writeHeader(readAttr, 0, 2, operationCount)
	cmd.writeFieldString(*batchNamespace.namespace, NAMESPACE)
	cmd.writeFieldHeader(byteSize, DIGEST_RIPE_ARRAY)

	for _, key := range keys {
		copy(cmd.dataBuffer[cmd.dataOffset:], key.digest)
		cmd.dataOffset += len(key.digest)
	}

	if binNames != nil {
		for binName := range binNames {
			cmd.writeOperationForBinName(binName, READ)
		}
	}
	cmd.end()

	return nil
}

func (cmd *baseCommand) setScan(policy *ScanPolicy, namespace *string, setName *string, binNames []string) error {
	cmd.begin()
	fieldCount := 0

	if namespace != nil {
		cmd.dataOffset += len(*namespace) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if setName != nil {
		cmd.dataOffset += len(*setName) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	// Estimate scan options size.
	cmd.dataOffset += 2 + int(_FIELD_HEADER_SIZE)
	fieldCount++

	if binNames != nil {
		for _, binName := range binNames {
			cmd.estimateOperationSizeForBinName(binName)
		}
	}
	if err := cmd.sizeBuffer(); err != nil {
		return nil
	}
	readAttr := _INFO1_READ

	if !policy.IncludeBinData {
		readAttr |= _INFO1_NOBINDATA
	}

	operationCount := 0
	if binNames != nil {
		operationCount = len(binNames)
	}
	cmd.writeHeader(readAttr, 0, fieldCount, operationCount)

	if namespace != nil {
		cmd.writeFieldString(*namespace, NAMESPACE)
	}

	if setName != nil {
		cmd.writeFieldString(*setName, TABLE)
	}

	cmd.writeFieldHeader(2, SCAN_OPTIONS)
	priority := byte(policy.Priority)
	priority <<= 4

	if policy.FailOnClusterChange {
		priority |= 0x08
	}
	cmd.dataBuffer[cmd.dataOffset] = priority
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = byte(policy.ScanPercent)
	cmd.dataOffset++

	if binNames != nil {
		for _, binName := range binNames {
			cmd.writeOperationForBinName(binName, READ)
		}
	}
	cmd.end()

	return nil
}

func (cmd *baseCommand) estimateKeySize(key *Key) int {
	fieldCount := 0

	if strings.Trim(key.namespace, " ") != "" {
		cmd.dataOffset += len(key.namespace) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	if strings.Trim(key.setName, " ") != "" {
		cmd.dataOffset += len(key.setName) + int(_FIELD_HEADER_SIZE)
		fieldCount++
	}

	cmd.dataOffset += len(key.digest) + int(_FIELD_HEADER_SIZE)
	fieldCount++

	return fieldCount
}

func (cmd *baseCommand) estimateUdfSize(packageName string, functionName string, bytes []byte) int {
	cmd.dataOffset += len(packageName) + int(_FIELD_HEADER_SIZE)
	cmd.dataOffset += len(functionName) + int(_FIELD_HEADER_SIZE)
	cmd.dataOffset += len(bytes) + int(_FIELD_HEADER_SIZE)
	return 3
}

func (cmd *baseCommand) estimateOperationSizeForBin(bin *Bin) {
	cmd.dataOffset += len(bin.Name) + int(_OPERATION_HEADER_SIZE)
	cmd.dataOffset += bin.Value.estimateSize()
}

func (cmd *baseCommand) estimateOperationSizeForOperation(operation *Operation) {
	binLen := 0
	if operation.BinName != nil {
		binLen = len(*operation.BinName)
	}
	cmd.dataOffset += binLen + int(_OPERATION_HEADER_SIZE)

	if operation.BinValue != nil {
		cmd.dataOffset += operation.BinValue.estimateSize()
	}
}

func (cmd *baseCommand) estimateOperationSizeForBinName(binName string) {
	cmd.dataOffset += len(binName) + int(_OPERATION_HEADER_SIZE)
}

func (cmd *baseCommand) estimateOperationSize() {
	cmd.dataOffset += int(_OPERATION_HEADER_SIZE)
}

// Generic header write.
func (cmd *baseCommand) writeHeader(readAttr int, writeAttr int, fieldCount int, operationCount int) {
	// Write all header data except total size which must be written last.
	cmd.dataBuffer[8] = _MSG_REMAINING_HEADER_SIZE // Message header length.
	cmd.dataBuffer[9] = byte(readAttr)
	cmd.dataBuffer[10] = byte(writeAttr)

	for i := 11; i < 26; i++ {
		cmd.dataBuffer[i] = 0
	}
	Buffer.Int16ToBytes(int16(fieldCount), cmd.dataBuffer, 26)
	Buffer.Int16ToBytes(int16(operationCount), cmd.dataBuffer, 28)
	cmd.dataOffset = int(_MSG_TOTAL_HEADER_SIZE)
}

// Header write for write operations.
func (cmd *baseCommand) writeHeaderWithPolicy(policy *WritePolicy, readAttr int, writeAttr int, fieldCount int, operationCount int) {
	// Set flags.
	generation := 0
	infoAttr := 0

	switch policy.RecordExistsAction {
	case UPDATE:
		break
	case UPDATE_ONLY:
		infoAttr |= _INFO3_UPDATE_ONLY
		break
	case REPLACE:
		infoAttr |= _INFO3_CREATE_OR_REPLACE
		break
	case REPLACE_ONLY:
		infoAttr |= _INFO3_REPLACE_ONLY
		break
	case CREATE_ONLY:
		writeAttr |= _INFO2_CREATE_ONLY
		break
	}

	switch policy.GenerationPolicy {
	case NONE:
		break
	case EXPECT_GEN_EQUAL:
		generation = policy.Generation
		writeAttr |= _INFO2_GENERATION
		break
	case EXPECT_GEN_GT:
		generation = policy.Generation
		writeAttr |= _INFO2_GENERATION_GT
		break
	case DUPLICATE:
		generation = policy.Generation
		writeAttr |= _INFO2_GENERATION_DUP
		break
	}

	// Write all header data except total size which must be written last.
	cmd.dataBuffer[8] = _MSG_REMAINING_HEADER_SIZE // Message header length.
	cmd.dataBuffer[9] = byte(readAttr)
	cmd.dataBuffer[10] = byte(writeAttr)
	cmd.dataBuffer[11] = byte(infoAttr)
	cmd.dataBuffer[12] = 0 // unused
	cmd.dataBuffer[13] = 0 // clear the result code
	Buffer.Int32ToBytes(int32(generation), cmd.dataBuffer, 14)
	Buffer.Int32ToBytes(int32(policy.Expiration), cmd.dataBuffer, 18)

	// Initialize timeout. It will be written later.
	cmd.dataBuffer[22] = 0
	cmd.dataBuffer[23] = 0
	cmd.dataBuffer[24] = 0
	cmd.dataBuffer[25] = 0

	Buffer.Int16ToBytes(int16(fieldCount), cmd.dataBuffer, 26)
	Buffer.Int16ToBytes(int16(operationCount), cmd.dataBuffer, 28)
	cmd.dataOffset = int(_MSG_TOTAL_HEADER_SIZE)
}

func (cmd *baseCommand) writeKey(key *Key) {
	// Write key into buffer.
	if strings.Trim(key.namespace, " ") != "" {
		cmd.writeFieldString(key.namespace, NAMESPACE)
	}

	if strings.Trim(key.setName, " ") != "" {
		cmd.writeFieldString(key.setName, TABLE)
	}

	cmd.writeFieldBytes(key.digest[:], DIGEST_RIPE)
}

func (cmd *baseCommand) writeOperationForBin(bin *Bin, operation OperationType) error {
	nameLength := copy(cmd.dataBuffer[(cmd.dataOffset+int(_OPERATION_HEADER_SIZE)):], []byte(bin.Name))
	valueLength, err := bin.Value.write(cmd.dataBuffer, cmd.dataOffset+int(_OPERATION_HEADER_SIZE)+nameLength)
	if err != nil {
		return err
	}

	Buffer.Int32ToBytes(int32(nameLength+valueLength+4), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(bin.Value.GetType()))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(0))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(nameLength))
	cmd.dataOffset++
	cmd.dataOffset += nameLength + valueLength

	return nil
}

func (cmd *baseCommand) writeOperationForOperation(operation *Operation) error {
	nameLength := 0
	if operation.BinName != nil {
		nameLength = copy(cmd.dataBuffer[(cmd.dataOffset+int(_OPERATION_HEADER_SIZE)):], []byte(*operation.BinName))
	}

	valueLength, err := operation.BinValue.write(cmd.dataBuffer, cmd.dataOffset+int(_OPERATION_HEADER_SIZE)+nameLength)
	if err != nil {
		return err
	}

	Buffer.Int32ToBytes(int32(nameLength+valueLength+4), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation.OpType))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation.BinValue.GetType()))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(0))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(nameLength))
	cmd.dataOffset++
	cmd.dataOffset += nameLength + valueLength
	return nil
}

func (cmd *baseCommand) writeOperationForBinName(name string, operation OperationType) {
	nameLength := copy(cmd.dataBuffer[(cmd.dataOffset+int(_OPERATION_HEADER_SIZE)):], []byte(name))
	Buffer.Int32ToBytes(int32(nameLength+4), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(0))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(0))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (byte(nameLength))
	cmd.dataOffset++
	cmd.dataOffset += nameLength
}

func (cmd *baseCommand) writeOperationForOperationType(operation OperationType) {
	Buffer.Int32ToBytes(int32(4), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(operation))
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (0)
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (0)
	cmd.dataOffset++
	cmd.dataBuffer[cmd.dataOffset] = (0)
	cmd.dataOffset++
}

func (cmd *baseCommand) writeFieldValue(value Value, ftype FieldType) {
	offset := cmd.dataOffset + int(_FIELD_HEADER_SIZE)
	cmd.dataBuffer[offset] = byte(value.GetType())
	offset++
	len, _ := value.write(cmd.dataBuffer, offset)
	len++
	cmd.writeFieldHeader(len, ftype)
	cmd.dataOffset += len
}

func (cmd *baseCommand) writeFieldString(str string, ftype FieldType) {
	len := copy(cmd.dataBuffer[(cmd.dataOffset+int(_FIELD_HEADER_SIZE)):], []byte(str))
	cmd.writeFieldHeader(len, ftype)
	cmd.dataOffset += len
}

func (cmd *baseCommand) writeFieldBytes(bytes []byte, ftype FieldType) {
	copy(cmd.dataBuffer[cmd.dataOffset+int(_FIELD_HEADER_SIZE):], bytes)

	cmd.writeFieldHeader(len(bytes), ftype)
	cmd.dataOffset += len(bytes)
}

func (cmd *baseCommand) writeFieldHeader(size int, ftype FieldType) {
	Buffer.Int32ToBytes(int32(size+1), cmd.dataBuffer, cmd.dataOffset)
	cmd.dataOffset += 4
	cmd.dataBuffer[cmd.dataOffset] = (byte(ftype))
	cmd.dataOffset++
}

func (cmd *baseCommand) begin() {
	cmd.dataOffset = int(_MSG_TOTAL_HEADER_SIZE)
}

func (cmd *baseCommand) sizeBuffer() error {
	return cmd.sizeBufferSz(cmd.dataOffset)
}

func (cmd *baseCommand) sizeBufferSz(size int) error {
	// Corrupted data streams can result in a huge length.
	// Do a sanity check here.
	if size > _MAX_BUFFER_SIZE {
		return NewAerospikeError(PARSE_ERROR, fmt.Sprintf("Invalid size for buffer: %d", size))
	}

	if size <= len(cmd.dataBuffer) {
		// don't touch the buffer
	} else if size <= cap(cmd.dataBuffer) {
		cmd.dataBuffer = cmd.dataBuffer[:size]
	} else {
		// not enough space
		cmd.dataBuffer = make([]byte, size)
	}

	return nil
}

func (cmd *baseCommand) end() {
	var size = int64(cmd.dataOffset-8) | (_CL_MSG_VERSION << 56) | (_AS_MSG_TYPE << 48)
	Buffer.Int64ToBytes(size, cmd.dataBuffer, 0)
}

////////////////////////////////////

// a custom buffer pool with fine grained control over its contents
// maxSize: 128KiB
// initial bufferSize: 16 KiB
// maximum buffer size to keep in the pool: 128K
var bufPool = NewBufferPool(512, 16*1024, 128*1024)

// SetBufferPool can be used to customize the command Buffer Pool parameters to calibrate
// the pool for different workloads
func SetCommandBufferPool(poolSize, initBufSize, maxBufferSize int) {
	bufPool = NewBufferPool(poolSize, initBufSize, maxBufferSize)
}

func (cmd *baseCommand) execute(ifc command) (err error) {
	policy := ifc.getPolicy(ifc).GetBasePolicy()
	iterations := 0

	// set timeout outside the loop
	limit := time.Now().Add(policy.timeout())

	// Execute command until successful, timed out or maximum iterations have been reached.
	for {
		// too many retries
		if iterations++; (policy.MaxRetries > 0) && (iterations > policy.MaxRetries+1) {
			break
		}

		// Sleep before trying again, after the first iteration
		if iterations > 1 && policy.SleepBetweenRetries > 0 {
			time.Sleep(policy.SleepBetweenRetries)
		}

		// check for command timeout
		if time.Now().After(limit) {
			break
		}

		node, err := ifc.getNode(ifc)
		if err != nil {
			// Node is currently inactive.  Retry.
			continue
		}

		// set command node, so when you return a record it has the node
		cmd.node = node

		cmd.conn, err = node.GetConnection(policy.timeout())
		if err != nil {
			// Socket connection error has occurred. Decrease health and retry.
			node.DecreaseHealth()

			Logger.Warn("Node " + node.String() + ": " + err.Error())
			continue
		}

		// Draw a buffer from buffer pool, and make sure it will be put back
		cmd.dataBuffer = bufPool.Get()
		defer bufPool.Put(cmd.dataBuffer)

		// Set command buffer.
		err = ifc.writeBuffer(ifc)
		if err != nil {
			// All runtime exceptions are considered fatal. Do not retry.
			// Close socket to flush out possible garbage. Do not put back in pool.
			cmd.conn.Close()
			return err
		}

		// Reset timeout in send buffer (destined for server) and socket.
		Buffer.Int32ToBytes(int32(policy.Timeout/time.Millisecond), cmd.dataBuffer, 22)

		// Send command.
		_, err = cmd.conn.Write(cmd.dataBuffer[:cmd.dataOffset])
		if err != nil {
			// IO errors are considered temporary anomalies. Retry.
			// Close socket to flush out possible garbage. Do not put back in pool.
			cmd.conn.Close()

			Logger.Warn("Node " + node.String() + ": " + err.Error())
			// IO error means connection to server node is unhealthy.
			// Reflect cmd status.
			node.DecreaseHealth()
			continue
		}

		// Parse results.
		err = ifc.parseResult(ifc, cmd.conn)
		if err != nil {
			// close the connection
			// cancelling/closing the batch/multi commands will return an error, which will
			// close the connection to throw away its data and signal the server about the
			// situation. We will not put back the connection in the buffer.
			cmd.conn.Close()
			return err
		}

		// Reflect healthy status.
		node.RestoreHealth()

		// Put connection back in pool.
		node.PutConnection(cmd.conn)

		// command has completed successfully.  Exit method.
		return nil

	}

	// execution timeout
	return NewAerospikeError(TIMEOUT, "command execution timed out.")
}

func (cmd *baseCommand) parseRecordResults(ifc command, receiveSize int) (bool, error) {
	panic(errors.New("Abstract method. Should not end up here"))
}

func (cmd *baseCommand) setConnection(conn *Connection) {
	cmd.conn = conn
}

func (cmd *baseCommand) getConnection() *Connection {
	return cmd.conn
}
