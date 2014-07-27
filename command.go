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
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"

	. "github.com/aerospike/aerospike-client-go/types"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

const (
	// Flags commented out are not supported by this client.
	// Contains a read operation.
	INFO1_READ int = (1 << 0)
	// Get all bins.
	INFO1_GET_ALL int = (1 << 1)

	// Do not read the bins
	INFO1_NOBINDATA int = (1 << 5)

	// Create or update record
	INFO2_WRITE int = (1 << 0)
	// Fling a record into the belly of Moloch.
	INFO2_DELETE int = (1 << 1)
	// Update if expected generation == old.
	INFO2_GENERATION int = (1 << 2)
	// Update if new generation >= old, good for restore.
	INFO2_GENERATION_GT int = (1 << 3)
	// Create a duplicate on a generation collision.
	INFO2_GENERATION_DUP int = (1 << 4)
	// Create only. Fail if record already exists.
	INFO2_CREATE_ONLY int = (1 << 5)

	// This is the last of a multi-part message.
	INFO3_LAST int = (1 << 0)
	// Update only. Merge bins.
	INFO3_UPDATE_ONLY int = (1 << 3)

	// Create or completely replace record.
	INFO3_CREATE_OR_REPLACE int = (1 << 4)
	// Completely replace existing record only.
	INFO3_REPLACE_ONLY int = (1 << 5)

	MSG_TOTAL_HEADER_SIZE     uint8 = 30
	FIELD_HEADER_SIZE         uint8 = 5
	OPERATION_HEADER_SIZE     uint8 = 8
	MSG_REMAINING_HEADER_SIZE uint8 = 22
	DIGEST_SIZE               uint8 = 20
	CL_MSG_VERSION            int64 = 2
	AS_MSG_TYPE               int64 = 3
)

// Command intrerface describes all commands available
type Command interface {
	getPolicy(ifc Command) Policy
	writeBuffer(ifc Command) error
	getNode(ifc Command) (*Node, error)
	parseResult(ifc Command, conn *Connection) error

	execute(ifc Command) error
	// Executes the command
	Execute() error
}

// Holds data buffer for the command
type BaseCommand struct {
	dataBuffer []byte
	dataOffset int
}

// Writes the command for write operations
func (this *BaseCommand) SetWrite(policy *WritePolicy, operation OperationType, key *Key, bins []*Bin) error {
	this.begin()
	fieldCount := this.estimateKeySize(key)

	for _, bin := range bins {
		this.estimateOperationSizeForBin(bin)
	}

	this.sizeBuffer()
	this.writeHeaderWithPolicy(policy, 0, INFO2_WRITE, fieldCount, len(bins))
	this.writeKey(key)

	for _, bin := range bins {
		this.writeOperationForBin(bin, operation)
	}
	this.end()

	return nil
}

// Writes the command for delete operations
func (this *BaseCommand) SetDelete(policy *WritePolicy, key *Key) {
	this.begin()
	fieldCount := this.estimateKeySize(key)
	this.sizeBuffer()
	this.writeHeaderWithPolicy(policy, 0, INFO2_WRITE|INFO2_DELETE, fieldCount, 0)
	this.writeKey(key)
	this.end()
}

// Writes the command for touch operations
func (this *BaseCommand) SetTouch(policy *WritePolicy, key *Key) {
	this.begin()
	fieldCount := this.estimateKeySize(key)
	this.estimateOperationSize()
	this.sizeBuffer()
	this.writeHeaderWithPolicy(policy, 0, INFO2_WRITE, fieldCount, 1)
	this.writeKey(key)
	this.writeOperationForOperationType(TOUCH)
	this.end()
}

// Writes the command for exist operations
func (this *BaseCommand) SetExists(key *Key) {
	this.begin()
	fieldCount := this.estimateKeySize(key)
	this.sizeBuffer()
	this.writeHeader(INFO1_READ|INFO1_NOBINDATA, 0, fieldCount, 0)
	this.writeKey(key)
	this.end()
}

// Writes the command for get operations (all bins)
func (this *BaseCommand) SetReadForKeyOnly(key *Key) {
	this.begin()
	fieldCount := this.estimateKeySize(key)
	this.sizeBuffer()
	this.writeHeader(INFO1_READ|INFO1_GET_ALL, 0, fieldCount, 0)
	this.writeKey(key)
	this.end()
}

// Writes the command for get operations (specified bins)
func (this *BaseCommand) SetRead(key *Key, binNames []string) {
	if binNames != nil && len(binNames) > 0 {
		this.begin()
		fieldCount := this.estimateKeySize(key)

		for _, binName := range binNames {
			this.estimateOperationSizeForBinName(binName)
		}
		this.sizeBuffer()
		this.writeHeader(INFO1_READ, 0, fieldCount, len(binNames))
		this.writeKey(key)

		for _, binName := range binNames {
			this.writeOperationForBinName(binName, READ)
		}
		this.end()
	} else {
		this.SetReadForKeyOnly(key)
	}
}

// Writes the command for getting metadata operations
func (this *BaseCommand) SetReadHeader(key *Key) {
	this.begin()
	fieldCount := this.estimateKeySize(key)
	this.estimateOperationSizeForBinName("")
	this.sizeBuffer()

	// The server does not currently return record header data with INFO1_NOBINDATA attribute set.
	// The workaround is to request a non-existent bin.
	// TODO: Fix this on server.
	//command.this.SetRead(INFO1_READ | INFO1_NOBINDATA);
	this.writeHeader(INFO1_READ, 0, fieldCount, 1)

	this.writeKey(key)
	this.writeOperationForBinName("", READ)
	this.end()
}

// Implements different command operations
func (this *BaseCommand) SetOperate(policy *WritePolicy, key *Key, operations []*Operation) error {
	this.begin()
	fieldCount := this.estimateKeySize(key)
	readAttr := 0
	writeAttr := 0
	readHeader := false

	for _, operation := range operations {
		switch operation.OpType {
		case READ:
			readAttr |= INFO1_READ

			// Read all bins if no bin is specified.
			if operation.BinName == nil {
				readAttr |= INFO1_GET_ALL
			}
			break

		case READ_HEADER:
			// The server does not currently return record header data with INFO1_NOBINDATA attribute set.
			// The workaround is to request a non-existent bin.
			// TODO: Fix this on server.
			//readAttr |= INFO1_READ | INFO1_NOBINDATA;
			readAttr |= INFO1_READ
			readHeader = true
			break

		default:
			writeAttr = INFO2_WRITE
			break
		}
		this.estimateOperationSizeForOperation(operation)
	}
	this.sizeBuffer()

	if writeAttr != 0 {
		this.writeHeaderWithPolicy(policy, readAttr, writeAttr, fieldCount, len(operations))
	} else {
		this.writeHeader(readAttr, writeAttr, fieldCount, len(operations))
	}
	this.writeKey(key)

	for _, operation := range operations {
		this.writeOperationForOperation(operation)
	}

	if readHeader {
		this.writeOperationForBin(nil, READ)
	}
	this.end()

	return nil
}

// func (this *BaseCommand) SetUdf(key *Key, packageName string, functionName string, args []*Value) error {
// 	this.begin()
// 	fieldCount := this.estimateKeySize(key)
// 	argBytes := Packer.pack(args)
// 	fieldCount += estimateUdfSize(packageName, functionName, argBytes)

// 	this.sizeBuffer()
// 	this.writeHeaderWithPolicy(0, INFO2_WRITE, fieldCount, 0)
// 	this.writeKey(key)
// 	this.writeField(packageName, UDF_PACKAGE_NAME)
// 	this.writeField(functionName, UDF_FUNCTION)
// 	this.writeField(argBytes, UDF_ARGLIST)
// 	this.end()
// }

// func (this *BaseCommand) SetBatchExists(batchNamespace BatchNamespace) {
// 	// Estimate buffer size
// 	this.begin()
// 	keys := batchNamespace.keys
// 	byteSize := keys.size() * SyncDIGEST_SIZE

// 	this.dataOffset += len(batchNamespace.namespace) +
// 		FIELD_HEADER_SIZE + byteSize + int(FIELD_HEADER_SIZE)

// 	this.sizeBuffer()

// 	this.writeHeaderWithPolicy(INFO1_READ|INFO1_NOBINDATA, 0, 2, 0)
// 	this.writeField(batchNamespace.namespace, NAMESPACE)
// 	this.writeFieldHeader(byteSize, DIGEST_RIPE_ARRAY)

// 	for _, key := range keys {
// 		digest := key.Digest()
// 		System.arraycopy(digest, 0, this.dataBuffer, this.dataOffset, len(digest))
// 		this.dataOffset += len(digest)
// 	}
// 	this.end()
// }

// func (this *BaseCommand) SetBatchGet(batchNamespace BatchNamespace, binNames StringSet, readAttr int) {
// 	// Estimate buffer size
// 	this.begin()
// 	keys := batchNamespace.keys
// 	byteSize := keys.size() * SyncDIGEST_SIZE

// 	this.dataOffset += len(batchNamespace.namespace) +
// 		FIELD_HEADER_SIZE + byteSize + int(FIELD_HEADER_SIZE)

// 	if binNames != nil {
// 		for _, binName := range binNames {
// 			this.estimateOperationSize(binName)
// 		}
// 	}

// 	this.sizeBuffer()

// 	operationCount := 0
// 	if binNames != nil {
// 		operationCount = binNames.size()
// 	}
// 	this.writeHeaderWithPolicy(readAttr, 0, 2, operationCount)
// 	this.writeField(batchNamespace.namespace, NAMESPACE)
// 	this.writeFieldHeader(byteSize, DIGEST_RIPE_ARRAY)

// 	for _, key := range keys {
// 		digest := key.Digest()
// 		System.arraycopy(digest, 0, this.dataBuffer, this.dataOffset, len(digest))
// 		this.dataOffset += len(digest)
// 	}

// 	if binNames != nil {
// 		for _, binName := range binNames {
// 			this.writeOperation(binName, READ)
// 		}
// 	}
// 	this.end()
// }

func (this *BaseCommand) SetScan(policy *ScanPolicy, namespace *string, setName *string, binNames []string) {
	this.begin()
	fieldCount := 0

	if namespace != nil {
		this.dataOffset += len(*namespace) + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	if setName != nil {
		this.dataOffset += len(*setName) + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	// Estimate scan options size.
	this.dataOffset += 2 + int(FIELD_HEADER_SIZE)
	fieldCount++

	if binNames != nil {
		for _, binName := range binNames {
			this.estimateOperationSizeForBinName(binName)
		}
	}

	this.sizeBuffer()
	readAttr := INFO1_READ

	if !policy.IncludeBinData {
		readAttr |= INFO1_NOBINDATA
	}

	var operationCount int
	if binNames == nil {
		operationCount = 0
	} else {
		operationCount = len(binNames)
	}
	this.writeHeader(readAttr, 0, fieldCount, operationCount)

	if namespace != nil {
		this.WriteFieldString(*namespace, NAMESPACE)
	}

	if setName != nil {
		this.WriteFieldString(*setName, TABLE)
	}

	this.WriteFieldHeader(2, SCAN_OPTIONS)
	priority := byte(policy.Priority)
	priority <<= 4

	if policy.FailOnClusterChange {
		priority |= 0x08
	}
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = priority
	this.dataBuffer[this.dataOffset] = byte(policy.ScanPercent)

	if binNames != nil {
		for _, binName := range binNames {
			this.writeOperationForBinName(binName, READ)
		}
	}
	this.end()
}

func (this *BaseCommand) estimateKeySize(key *Key) int {
	fieldCount := 0

	if key.Namespace() != nil {
		this.dataOffset += len(*key.Namespace()) + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	if key.SetName() != nil {
		this.dataOffset += len(*key.SetName()) + int(FIELD_HEADER_SIZE)
		fieldCount++
	}

	this.dataOffset += len(key.Digest()) + int(FIELD_HEADER_SIZE)
	fieldCount++

	return fieldCount
}

func (this *BaseCommand) estimateUdfSize(packageName string, functionName string, bytes []byte) int {
	this.dataOffset += len(packageName) + int(FIELD_HEADER_SIZE)
	this.dataOffset += len(functionName) + int(FIELD_HEADER_SIZE)
	this.dataOffset += len(bytes) + int(FIELD_HEADER_SIZE)
	return 3
}

func (this *BaseCommand) estimateOperationSizeForBin(bin *Bin) error {

	this.dataOffset += len(bin.Name) + int(OPERATION_HEADER_SIZE)
	this.dataOffset += bin.Value.EstimateSize()
	return nil
}

func (this *BaseCommand) estimateOperationSizeForOperation(operation *Operation) error {
	this.dataOffset += len(*operation.BinName) + int(OPERATION_HEADER_SIZE)
	this.dataOffset += operation.BinValue.EstimateSize()
	return nil
}

func (this *BaseCommand) estimateOperationSizeForBinName(binName string) {
	this.dataOffset += len(binName) + int(OPERATION_HEADER_SIZE)
}

func (this *BaseCommand) estimateOperationSize() {
	this.dataOffset += int(OPERATION_HEADER_SIZE)
}

/**
 * Generic header write.
 */
func (this *BaseCommand) writeHeader(readAttr int, writeAttr int, fieldCount int, operationCount int) {
	// Write all header data except total size which must be written last.
	this.dataBuffer[8] = MSG_REMAINING_HEADER_SIZE // Message header length.
	this.dataBuffer[9] = byte(readAttr)
	this.dataBuffer[10] = byte(writeAttr)

	for i := 11; i < 26; i++ {
		this.dataBuffer[i] = 0
	}
	Buffer.Int16ToBytes(int16(fieldCount), this.dataBuffer, 26)
	Buffer.Int16ToBytes(int16(operationCount), this.dataBuffer, 28)
	this.dataOffset = int(MSG_TOTAL_HEADER_SIZE)
}

// Header write for write operations.
func (this *BaseCommand) writeHeaderWithPolicy(policy *WritePolicy, readAttr int, writeAttr int, fieldCount int, operationCount int) {
	// Set flags.
	generation := 0
	infoAttr := 0

	switch policy.RecordExistsAction {
	case UPDATE:
		break
	case UPDATE_ONLY:
		infoAttr |= INFO3_UPDATE_ONLY
		break
	case REPLACE:
		infoAttr |= INFO3_CREATE_OR_REPLACE
		break
	case REPLACE_ONLY:
		infoAttr |= INFO3_REPLACE_ONLY
		break
	case CREATE_ONLY:
		writeAttr |= INFO2_CREATE_ONLY
		break
	}

	switch policy.GenerationPolicy {
	case NONE:
		break
	case EXPECT_GEN_EQUAL:
		generation = policy.Generation
		writeAttr |= INFO2_GENERATION
		break
	case EXPECT_GEN_GT:
		generation = policy.Generation
		writeAttr |= INFO2_GENERATION_GT
		break
	case DUPLICATE:
		generation = policy.Generation
		writeAttr |= INFO2_GENERATION_DUP
		break
	}

	// Write all header data except total size which must be written last.
	this.dataBuffer[8] = MSG_REMAINING_HEADER_SIZE // Message header length.
	this.dataBuffer[9] = byte(readAttr)
	this.dataBuffer[10] = byte(writeAttr)
	this.dataBuffer[11] = byte(infoAttr)
	this.dataBuffer[12] = 0 // unused
	this.dataBuffer[13] = 0 // clear the result code
	Buffer.Int32ToBytes(int32(generation), this.dataBuffer, 14)
	Buffer.Int32ToBytes(int32(policy.Expiration), this.dataBuffer, 18)

	// Initialize timeout. It will be written later.
	this.dataBuffer[22] = 0
	this.dataBuffer[23] = 0
	this.dataBuffer[24] = 0
	this.dataBuffer[25] = 0

	Buffer.Int16ToBytes(int16(fieldCount), this.dataBuffer, 26)
	Buffer.Int16ToBytes(int16(operationCount), this.dataBuffer, 28)
	this.dataOffset = int(MSG_TOTAL_HEADER_SIZE)
}

func (this *BaseCommand) writeKey(key *Key) {
	// Write key into buffer.
	if key.Namespace() != nil {
		this.WriteFieldString(*key.Namespace(), NAMESPACE)
	}

	if key.SetName() != nil {
		this.WriteFieldString(*key.SetName(), TABLE)
	}

	this.WriteFieldBytes(key.Digest()[:], DIGEST_RIPE)
}

func (this *BaseCommand) writeOperationForBin(bin *Bin, operation OperationType) error {
	nameLength := copy(this.dataBuffer[(this.dataOffset+int(OPERATION_HEADER_SIZE)):], []byte(bin.Name))
	valueLength, _ := bin.Value.Write(this.dataBuffer, this.dataOffset+int(OPERATION_HEADER_SIZE)+nameLength)

	Buffer.Int32ToBytes(int32(nameLength+valueLength+4), this.dataBuffer, this.dataOffset)
	this.dataOffset += 4
	this.dataBuffer[this.dataOffset] = (byte(operation))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(bin.Value.GetType()))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(0))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(nameLength))
	this.dataOffset++
	this.dataOffset += nameLength + valueLength

	return nil
}

func (this *BaseCommand) writeOperationForOperation(operation *Operation) error {
	nameLength := copy(this.dataBuffer[(this.dataOffset+int(OPERATION_HEADER_SIZE)):], []byte(*operation.BinName))
	valueLength, _ := operation.BinValue.Write(this.dataBuffer, this.dataOffset+int(OPERATION_HEADER_SIZE)+nameLength)

	Buffer.Int32ToBytes(int32(nameLength+valueLength+4), this.dataBuffer, this.dataOffset)
	this.dataOffset += 4
	this.dataBuffer[this.dataOffset] = (byte(operation.OpType))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(operation.BinValue.GetType()))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(0))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(nameLength))
	this.dataOffset++
	this.dataOffset += nameLength + valueLength
	return nil
}

func (this *BaseCommand) writeOperationForBinName(name string, operation OperationType) {
	nameLength := copy(this.dataBuffer[(this.dataOffset+int(OPERATION_HEADER_SIZE)):], []byte(name))
	Buffer.Int32ToBytes(int32(nameLength+4), this.dataBuffer, this.dataOffset)
	this.dataOffset += 4
	this.dataBuffer[this.dataOffset] = (byte(operation))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(0))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(0))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (byte(nameLength))
	this.dataOffset++
	this.dataOffset += nameLength
}

func (this *BaseCommand) writeOperationForOperationType(operation OperationType) {
	Buffer.Int32ToBytes(int32(4), this.dataBuffer, this.dataOffset)
	this.dataOffset += 4
	this.dataBuffer[this.dataOffset] = (byte(operation))
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (0)
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (0)
	this.dataOffset++
	this.dataBuffer[this.dataOffset] = (0)
	this.dataOffset++
}

func (this *BaseCommand) WriteFieldString(str string, ftype FieldType) {
	len := copy(this.dataBuffer[(this.dataOffset+int(FIELD_HEADER_SIZE)):], []byte(str))
	this.WriteFieldHeader(len, ftype)
	this.dataOffset += len
}

func (this *BaseCommand) WriteFieldBytes(bytes []byte, ftype FieldType) {
	copy(this.dataBuffer[this.dataOffset+int(FIELD_HEADER_SIZE):], bytes)

	this.WriteFieldHeader(len(bytes), ftype)
	this.dataOffset += len(bytes)
}

func (this *BaseCommand) WriteFieldHeader(size int, ftype FieldType) {
	Buffer.Int32ToBytes(int32(size+1), this.dataBuffer, this.dataOffset)
	this.dataOffset += 4
	this.dataBuffer[this.dataOffset] = (byte(ftype))
	this.dataOffset++
}

func (this *BaseCommand) begin() {
	this.dataOffset = int(MSG_TOTAL_HEADER_SIZE)
}

// TODO: should be slow and stressful for GC; optimize later
func (this *BaseCommand) sizeBuffer() {
	if this.dataOffset > len(this.dataBuffer) {
		newBuffer := make([]byte, this.dataOffset)
		this.dataBuffer = newBuffer
	}
}

// TODO: should be slow and stressful for GC; optimize later
func (this *BaseCommand) sizeBufferSz(size int) {
	if size > len(this.dataBuffer) {
		newBuffer := make([]byte, size)
		this.dataBuffer = newBuffer
	}
}

func (this *BaseCommand) end() {
	var size int64 = int64(this.dataOffset-8) | (CL_MSG_VERSION << 56) | (AS_MSG_TYPE << 48)
	Buffer.Int64ToBytes(size, this.dataBuffer, 0)
}

////////////////////////////////////

// TODO: LEAKY Abstraction: Command should not get a connction to node
//   On the contrary, it should be passed to the node
func (this *BaseCommand) execute(ifc Command) error {
	policy := ifc.getPolicy(ifc).GetBasePolicy()
	limit := time.Now().Add(policy.Timeout)
	failedNodes := 0
	failedConns := 0
	iterations := 0

	// Execute command until successful, timed out or maximum iterations have been reached.
	for {
		if iterations++; iterations > policy.MaxRetries {
			break
		}

		// Check for client timeout.
		if policy.Timeout > 0 && time.Now().After(limit) {
			break
		}

		// Sleep before trying again, after the first iteration
		if iterations > 1 && policy.SleepBetweenRetries > 0 {
			time.Sleep(policy.SleepBetweenRetries)
		}

		node, err := ifc.getNode(ifc)
		if err != nil {
			// Node is currently inactive.  Retry.
			failedNodes++
			continue
		}

		conn, err := node.GetConnection(policy.Timeout)
		if err != nil {
			// Socket connection error has occurred. Decrease health and retry.
			node.DecreaseHealth()

			Logger.Warn("Node " + node.String() + ": " + err.Error())
			failedConns++
			continue
		}

		// Set command buffer.
		err = ifc.writeBuffer(ifc)
		if err != nil {
			// All runtime exceptions are considered fatal.  Do not retry.
			// Close socket to flush out possible garbage.  Do not put back in pool.
			conn.Close()
			return err
		}

		// Reset timeout in send buffer (destined for server) and socket.
		Buffer.Int32ToBytes(int32(policy.Timeout/time.Millisecond), this.dataBuffer, 22)

		// Send command.
		_, err = conn.Write(this.dataBuffer)
		if err != nil {
			// IO errors are considered temporary anomalies.  Retry.
			// Close socket to flush out possible garbage.  Do not put back in pool.
			conn.Close()

			Logger.Warn("Node " + node.String() + ": " + err.Error())
			// IO error means connection to server node is unhealthy.
			// Reflect this status.
			node.DecreaseHealth()
			continue
		}

		// Parse results.
		err = ifc.parseResult(ifc, conn)
		if err != nil {
			return err
		}

		// Reflect healthy status.
		// TODO: This is SUPER leaky abstraction! Only connection itself should
		// 	 check when it was last used
		// conn.UpdateLastUsed()
		node.RestoreHealth()

		// Put connection back in pool.
		node.PutConnection(conn)

		// Command has completed successfully.  Exit method.
		return nil
	}

	// execution timeout
	return TimeoutErr()
}
