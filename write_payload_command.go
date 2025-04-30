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

	policy  *WritePolicy
	payload []byte
}

func newWritePayloadCommand(
	cluster *Cluster,
	policy *WritePolicy,
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
		payload:       payload,
	}

	return newWriteCmd, nil
}

func (cmd *writePayloadCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *writePayloadCommand) writeBuffer(ifc command) Error {
	if err := cmd.sizeBufferSz(len(cmd.payload), false); err != nil {
		return err
	}
	cmd.dataOffset = copy(cmd.dataBuffer, cmd.payload)
	return nil
}

func (cmd *writePayloadCommand) getNode(ifc command) (*Node, Error) {
	return cmd.partition.GetNodeWrite(cmd.cluster)
}

func (cmd *writePayloadCommand) prepareRetry(ifc command, isTimeout bool) bool {
	cmd.partition.PrepareRetryWrite(isTimeout)
	return true
}

func (cmd *writePayloadCommand) parseResult(ifc command, conn *Connection) Error {
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

	// Aggregate metrics
	metricsEnabled := cmd.node.cluster.metricsEnabled.Load()
	if metricsEnabled {
		cmd.node.stats.updateOrInsert(ifc, types.ResultCode(resultCode))
	}

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

func (cmd *writePayloadCommand) getnamespaces() *map[string]uint64 {
	return nil
}

func (cmd *writePayloadCommand) getNamespace() *string {
	return &cmd.key.namespace
}
