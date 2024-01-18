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
	"github.com/aerospike/aerospike-client-go/v7/types"
)

type grpcScanPartitionCommand struct {
	baseMultiCommand

	policy          *ScanPolicy
	namespace       string
	setName         string
	binNames        []string
	partitionFilter *PartitionFilter
}

func newGrpcScanPartitionCommand(
	policy *ScanPolicy,
	partitionTracker *partitionTracker,
	partitionFilter *PartitionFilter,
	namespace string,
	setName string,
	binNames []string,
	recordset *Recordset,
) *grpcScanPartitionCommand {
	cmd := &grpcScanPartitionCommand{
		baseMultiCommand: *newCorrectStreamingMultiCommand(recordset, namespace),
		policy:           policy,
		namespace:        namespace,
		setName:          setName,
		binNames:         binNames,
		partitionFilter:  partitionFilter,
	}
	cmd.tracker = partitionTracker
	cmd.terminationErrorType = types.SCAN_TERMINATED
	cmd.nodePartitions = newNodePartitions(nil, _PARTITIONS)

	return cmd
}

func (cmd *grpcScanPartitionCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *grpcScanPartitionCommand) writeBuffer(ifc command) Error {
	return cmd.setScan(cmd.policy, &cmd.namespace, &cmd.setName, cmd.binNames, cmd.recordset.taskID, nil)
}

func (cmd *grpcScanPartitionCommand) shouldRetry(e Error) bool {
	panic("UNREACHABLE")
}

func (cmd *grpcScanPartitionCommand) Execute() Error {
	panic("UNREACHABLE")
}

func (cmd *grpcScanPartitionCommand) ExecuteGRPC(clnt *ProxyClient) Error {
	defer cmd.recordset.signalEnd()

	cmd.dataBuffer = bufPool.Get().([]byte)
	defer cmd.grpcPutBufferBack()

	err := cmd.prepareBuffer(cmd, cmd.policy.deadline())
	if err != nil {
		return err
	}

	scanReq := &kvs.ScanRequest{
		Namespace:       cmd.namespace,
		SetName:         &cmd.setName,
		BinNames:        cmd.binNames,
		PartitionFilter: cmd.partitionFilter.grpc(),
		ScanPolicy:      cmd.policy.grpc(),
	}

	req := kvs.AerospikeRequestPayload{
		Id:          rand.Uint32(),
		Iteration:   1,
		Payload:     cmd.dataBuffer[:cmd.dataOffset],
		ScanRequest: scanReq,
	}

	conn, err := clnt.grpcConn()
	if err != nil {
		return err
	}

	client := kvs.NewScanClient(conn)

	ctx := cmd.policy.grpcDeadlineContext()

	streamRes, gerr := client.Scan(ctx, &req)
	if gerr != nil {
		return newGrpcError(gerr, gerr.Error())
	}

	cmd.commandWasSent = true

	readCallback := func() ([]byte, Error) {
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

		if !res.HasNext {
			done, err := cmd.tracker.isComplete(false, &cmd.policy.BasePolicy, []*nodePartitions{cmd.nodePartitions})
			if !cmd.recordset.IsActive() || done || err != nil {
				// Query is complete.
				if err != nil {
					cmd.tracker.partitionError()
					cmd.recordset.sendError(err)
				}
			}

			return nil, errGRPCStreamEnd
		}

		return res.Payload, nil
	}

	cmd.conn = newGrpcFakeConnection(nil, readCallback)
	err = cmd.parseResult(cmd, cmd.conn)
	if err != nil && err != errGRPCStreamEnd {
		cmd.recordset.sendError(err)
		return err
	}

	clnt.returnGrpcConnToPool(conn)

	return nil
}
