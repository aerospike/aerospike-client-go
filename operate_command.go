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
)

type operateCommand struct {
	readCommand

	policy *WritePolicy
	args   operateArgs
}

func newOperateCommand(cluster *Cluster, policy *WritePolicy, key *Key, args operateArgs) (operateCommand, Error) {
	rdCommand, err := newReadCommand(cluster, &policy.BasePolicy, key, nil, args.partition)
	if err != nil {
		return operateCommand{}, err
	}

	return operateCommand{
		readCommand: rdCommand,
		policy:      policy,
		args:        args,
	}, nil
}

func (cmd *operateCommand) writeBuffer(ifc command) (err Error) {
	return cmd.setOperate(cmd.policy, cmd.key, &cmd.args)
}

func (cmd *operateCommand) getNode(ifc command) (*Node, Error) {
	if cmd.args.hasWrite {
		return cmd.partition.GetNodeWrite(cmd.cluster)
	}

	// this may be affected by Rackaware
	return cmd.partition.GetNodeRead(cmd.cluster)
}

func (cmd *operateCommand) prepareRetry(ifc command, isTimeout bool) bool {
	if cmd.args.hasWrite {
		cmd.partition.PrepareRetryWrite(isTimeout)
	} else {
		cmd.partition.PrepareRetryRead(isTimeout)
	}
	return true
}

func (cmd *operateCommand) isRead() bool {
	return !cmd.args.hasWrite
}

func (cmd *operateCommand) Execute() Error {
	return cmd.execute(cmd)
}

func (cmd *operateCommand) ExecuteGRPC(clnt *ProxyClient) Error {
	err := cmd.prepareBuffer(cmd, cmd.policy.deadline())
	if err != nil {
		return err
	}

	req := kvs.AerospikeRequestPayload{
		Id:          rand.Uint32(),
		Iteration:   1,
		Payload:     cmd.dataBuffer[:cmd.dataOffset],
		WritePolicy: cmd.policy.grpc(),
	}

	conn, err := clnt.grpcConn()
	if err != nil {
		return err
	}

	client := kvs.NewKVSClient(conn)

	ctx := cmd.policy.grpcDeadlineContext()

	res, gerr := client.Operate(ctx, &req)
	if gerr != nil {
		return newGrpcError(gerr, gerr.Error())
	}

	cmd.commandWasSent = true

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
