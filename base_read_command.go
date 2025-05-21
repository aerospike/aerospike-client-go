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
	"iter"
	"reflect"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

type baseReadCommand struct {
	singleCommand

	policy   *BasePolicy
	binNames []string
	record   *Record

	// pointer to the object that's going to be unmarshalled
	object *reflect.Value

	replicaSequence int
}

// this method uses reflection.
// Will not be set if performance flag is passed for the build.
var objectParser func(
	brc *baseReadCommand,
	opCount int,
	fieldCount int,
	generation uint32,
	expiration uint32,
) Error

func newBaseReadCommand(cluster *Cluster, policy *BasePolicy, key *Key) (baseReadCommand, Error) {
	var partition *Partition
	var err Error
	if cluster != nil {
		partition, err = PartitionForRead(cluster, policy, key)
		if err != nil {
			return baseReadCommand{}, err
		}
	}

	return baseReadCommand{
		singleCommand: newSingleCommand(cluster, key, partition),
		policy:        policy,
	}, nil
}

func (cmd *baseReadCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *baseReadCommand) writeBuffer(ifc command) Error {
	panic(unreachable)
}

func (cmd *baseReadCommand) getNode(ifc command) (*Node, Error) {
	return cmd.partition.GetNodeRead(cmd.cluster)
}

func (cmd *baseReadCommand) prepareRetry(ifc command, isTimeout bool) bool {
	cmd.partition.PrepareRetryRead(isTimeout)
	return true
}

func (cmd *baseReadCommand) parseResult(ifc command, conn *Connection) Error {
	panic(unreachable)
}

func (cmd *baseReadCommand) handleUdfError(resultCode types.ResultCode) Error {
	if ret, exists := cmd.record.Bins["FAILURE"]; exists {
		return newError(resultCode, ret.(string))
	}
	return newError(resultCode)
}

func (cmd *baseReadCommand) GetRecord() *Record {
	return cmd.record
}

func (cmd *baseReadCommand) Execute() Error {
	panic(unreachable)
}

func (cmd *baseReadCommand) commandType() commandType {
	return ttGet
}

func (cmd *baseReadCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *baseReadCommand) getNamespace() *string {
	return &cmd.key.namespace
}
