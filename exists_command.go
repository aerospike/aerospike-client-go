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
	"math/rand"

	kvs "github.com/aerospike/aerospike-client-go/v6/proto/kvs"
	"github.com/aerospike/aerospike-client-go/v6/types"

	Buffer "github.com/aerospike/aerospike-client-go/v6/utils/buffer"
)

// guarantee existsCommand implements command interface
var _ command = &existsCommand{}

type existsCommand struct {
	singleCommand

	policy *BasePolicy
	exists bool
}

func newExistsCommand(cluster *Cluster, policy *BasePolicy, key *Key) (*existsCommand, Error) {
	var err Error
	var partition *Partition
	if cluster != nil {
		partition, err = PartitionForRead(cluster, policy, key)
		if err != nil {
			return nil, err
		}
	}

	return &existsCommand{
		singleCommand: newSingleCommand(cluster, key, partition),
		policy:        policy,
	}, nil
}

func (cmd *existsCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *existsCommand) writeBuffer(ifc command) Error {
	return cmd.setExists(cmd.policy, cmd.key)
}

func (cmd *existsCommand) getNode(ifc command) (*Node, Error) {
	return cmd.partition.GetNodeRead(cmd.cluster)
}

func (cmd *existsCommand) prepareRetry(ifc command, isTimeout bool) bool {
	cmd.partition.PrepareRetryRead(isTimeout)
	return true
}

func (cmd *existsCommand) parseResult(ifc command, conn *Connection) Error {
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

	switch types.ResultCode(resultCode) {
	case 0:
		cmd.exists = true
	case types.KEY_NOT_FOUND_ERROR:
		cmd.exists = false
	case types.FILTERED_OUT:
		if err := cmd.emptySocket(conn); err != nil {
			return err
		}
		cmd.exists = true
		return ErrFilteredOut.err()
	default:
		return newError(types.ResultCode(resultCode))
	}

	return cmd.emptySocket(conn)
}

func (cmd *existsCommand) Exists() bool {
	return cmd.exists
}

func (cmd *existsCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *existsCommand) ExecuteGRPC(clnt *ProxyClient) Error {
	err := cmd.prepareBuffer(cmd, cmd.policy.deadline())
	if err != nil {
		return err
	}

	req := kvs.AerospikeRequestPayload{
		Id:         rand.Uint32(),
		Iteration:  1,
		Payload:    cmd.dataBuffer[:cmd.dataOffset],
		ReadPolicy: cmd.policy.grpc(),
	}

	conn, err := clnt.grpcConn()
	if err != nil {
		return err
	}

	client := kvs.NewKVSClient(conn)

	ctx := cmd.policy.grpcDeadlineContext()

	res, gerr := client.Exists(ctx, &req)
	if gerr != nil {
		return newGrpcError(gerr, gerr.Error())
	}

	defer clnt.returnGrpcConnToPool(conn)

	if res.Status != 0 {
		return newGrpcStatusError(res)
	}

	cmd.conn = newGrpcFakeConnection(res.Payload, nil)
	err = cmd.parseResult(cmd, cmd.conn)
	if err != nil {
		return err
	}

	return nil
}
