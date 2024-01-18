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

	kvs "github.com/aerospike/aerospike-client-go/v7/proto/kvs"
)

type grpcQueryPartitionCommand struct {
	baseMultiCommand

	policy          *QueryPolicy
	writePolicy     *WritePolicy
	statement       *Statement
	partitionFilter *PartitionFilter
	operations      []*Operation
}

func newGrpcQueryPartitionCommand(
	policy *QueryPolicy,
	writePolicy *WritePolicy,
	statement *Statement,
	operations []*Operation,
	partitionTracker *partitionTracker,
	partitionFilter *PartitionFilter,
	recordset *Recordset,
) *grpcQueryPartitionCommand {
	cmd := &grpcQueryPartitionCommand{
		baseMultiCommand: *newCorrectStreamingMultiCommand(recordset, statement.Namespace),
		policy:           policy,
		writePolicy:      writePolicy,
		statement:        statement,
		partitionFilter:  partitionFilter,
		operations:       operations,
	}
	cmd.tracker = partitionTracker
	cmd.terminationErrorType = statement.terminationError()
	cmd.nodePartitions = newNodePartitions(nil, _PARTITIONS)

	return cmd
}

func (cmd *grpcQueryPartitionCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *grpcQueryPartitionCommand) writeBuffer(ifc command) Error {
	return cmd.setQuery(cmd.policy, cmd.writePolicy, cmd.statement, cmd.recordset.TaskId(), cmd.operations, cmd.writePolicy != nil, nil)
}

func (cmd *grpcQueryPartitionCommand) shouldRetry(e Error) bool {
	panic("UNREACHABLE")
}

func (cmd *grpcQueryPartitionCommand) Execute() Error {
	panic("UNREACHABLE")
}

func (cmd *grpcQueryPartitionCommand) ExecuteGRPC(clnt *ProxyClient) Error {
	defer cmd.recordset.signalEnd()

	cmd.dataBuffer = bufPool.Get().([]byte)
	defer cmd.grpcPutBufferBack()

	err := cmd.prepareBuffer(cmd, cmd.policy.deadline())
	if err != nil {
		return err
	}

	queryReq := &kvs.QueryRequest{
		Statement:       cmd.statement.grpc(cmd.policy, cmd.operations),
		PartitionFilter: cmd.partitionFilter.grpc(),
		QueryPolicy:     cmd.policy.grpc(),
	}

	req := kvs.AerospikeRequestPayload{
		Id:           rand.Uint32(),
		Iteration:    1,
		Payload:      cmd.dataBuffer[:cmd.dataOffset],
		QueryRequest: queryReq,
	}

	conn, err := clnt.grpcConn()
	if err != nil {
		return err
	}

	client := kvs.NewQueryClient(conn)

	ctx := cmd.policy.grpcDeadlineContext()

	streamRes, gerr := client.Query(ctx, &req)
	if gerr != nil {
		return newGrpcError(gerr, gerr.Error())
	}

	cmd.commandWasSent = true

	readCallback := func() ([]byte, Error) {
		if cmd.grpcEOS {
			return nil, errGRPCStreamEnd
		}

		res, gerr := streamRes.Recv()
		if gerr != nil {
			e := newGrpcError(gerr)
			cmd.recordset.sendError(e)
			return nil, e
		}

		if res.Status != 0 {
			e := newGrpcStatusError(res)
			cmd.recordset.sendError(e)
			return res.Payload, e
		}

		cmd.grpcEOS = !res.HasNext

		return res.Payload, nil
	}

	cmd.conn = newGrpcFakeConnection(nil, readCallback)
	err = cmd.parseResult(cmd, cmd.conn)
	if err != nil && err != errGRPCStreamEnd {
		cmd.recordset.sendError(err)
		return err
	}

	done, err := cmd.tracker.isComplete(false, &cmd.policy.BasePolicy, []*nodePartitions{cmd.nodePartitions})
	if !cmd.recordset.IsActive() || done || err != nil {
		// Query is complete.
		if err != nil {
			cmd.tracker.partitionError()
			cmd.recordset.sendError(err)
		}
	}

	clnt.returnGrpcConnToPool(conn)

	return nil
}
