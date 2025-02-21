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
	"github.com/aerospike/aerospike-client-go/v8/types"

	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

// guarantee writePayloadCommand implements command interface
var _ command = &writePayloadCommand{}

type writePayloadCommand struct {
	singleCommand

	policy   *WritePolicy
	ogPolicy *WritePolicy
	payload  []byte
}

func newWritePayloadCommand(
	cluster *Cluster,
	policy *WritePolicy,
	ogPolicy *WritePolicy,
	key *Key,
	payload []byte,
) (writePayloadCommand, Error) {

	var partition *Partition
	var err Error
	if cluster != nil {
		partition, err = PartitionForWrite(cluster, &policy.BasePolicy, key)
		if err != nil {
			return writePayloadCommand{}, err
		}
	}

	newWriteCmd := writePayloadCommand{
		singleCommand: newSingleCommand(cluster, key, partition),
		policy:        policy,
		ogPolicy:      ogPolicy,
		payload:       payload,
	}

	return newWriteCmd, nil
}

func (cmd *writePayloadCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *writePayloadCommand) writeBuffer(ifc command) Error {
	cmd.dataBuffer = cmd.payload
	cmd.applyPolicy()
	cmd.dataOffset = len(cmd.payload)
	return nil
}

// Header write for write commands.
func (cmd *writePayloadCommand) applyPolicy() {
	if cmd.ogPolicy == nil {
		return
	}

	policy := cmd.ogPolicy

	// Set flags.

	generation := uint32(Buffer.BytesToInt32(cmd.dataBuffer, 14))
	writeAttr := _INFO2_WRITE
	readAttr := 0
	infoAttr := 0

	switch policy.RecordExistsAction {
	case UPDATE:
	case UPDATE_ONLY:
		infoAttr |= _INFO3_UPDATE_ONLY
	case REPLACE:
		infoAttr |= _INFO3_CREATE_OR_REPLACE
	case REPLACE_ONLY:
		infoAttr |= _INFO3_REPLACE_ONLY
	case CREATE_ONLY:
		writeAttr |= _INFO2_CREATE_ONLY
	}

	switch policy.GenerationPolicy {
	case NONE:
	case EXPECT_GEN_EQUAL:
		generation = policy.Generation
		writeAttr |= _INFO2_GENERATION
	case EXPECT_GEN_GT:
		generation = policy.Generation
		writeAttr |= _INFO2_GENERATION_GT
	}

	if policy.CommitLevel == COMMIT_MASTER {
		infoAttr |= _INFO3_COMMIT_MASTER
	}

	if policy.DurableDelete {
		writeAttr |= _INFO2_DURABLE_DELETE
	}

	// cmd.dataBuffer[8] = _MSG_REMAINING_HEADER_SIZE // Message header length.
	cmd.dataBuffer[9] = byte(readAttr)
	cmd.dataBuffer[10] = byte(writeAttr)
	cmd.dataBuffer[11] = byte(infoAttr)
	// cmd.dataBuffer[12] = byte(txnAttr)
	cmd.dataBuffer[13] = 0 // clear the result code
	cmd.dataOffset = 14
	cmd.WriteUint32(generation)
	if policy.Expiration != TTLDontUpdate {
		cmd.WriteUint32(policy.Expiration)
	}
}

func (cmd *writePayloadCommand) getNode(ifc command) (*Node, Error) {
	return cmd.partition.GetNodeWrite(cmd.cluster)
}

func (cmd *writePayloadCommand) prepareRetry(ifc command, isTimeout bool) bool {
	cmd.partition.PrepareRetryWrite(isTimeout)
	return true
}

func (cmd *writePayloadCommand) parseResult(ifc command, conn *Connection) Error {
	// make sure the payload is not put back in the buffer pool
	defer func() {
		cmd.dataBuffer = cmd.conn.origDataBuffer
		cmd.dataOffset = 0
	}()

	// Read header.
	if _, err := conn.Read(cmd.dataBuffer, int(_MSG_TOTAL_HEADER_SIZE)); err != nil {
		return err
	}

	header := Buffer.BytesToInt64(cmd.dataBuffer, 0)

	// Validate header to make sure we are at the beginning of a message
	if err := cmd.validateHeader(header); err != nil {
		return err
	}

	resultCode := cmd.dataBuffer[13] & 0xFF

	if resultCode != 0 {
		if resultCode == byte(types.KEY_NOT_FOUND_ERROR) {
			return ErrKeyNotFound.err()
		} else if types.ResultCode(resultCode) == types.FILTERED_OUT {
			return ErrFilteredOut.err()
		}

		return newCustomNodeError(cmd.node, types.ResultCode(resultCode))
	}
	return cmd.emptySocket(conn)
}

func (cmd *writePayloadCommand) isRead() bool {
	return false
}

func (cmd *writePayloadCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *writePayloadCommand) commandType() commandType {
	return ttPut
}
