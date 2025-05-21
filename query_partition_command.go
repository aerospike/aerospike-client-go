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

import "iter"

type queryPartitionCommand queryCommand

func newQueryPartitionCommand(
	policy *QueryPolicy,
	tracker *partitionTracker,
	nodePartitions *nodePartitions,
	statement *Statement,
	recordset *Recordset,
) *queryPartitionCommand {
	cmd := &queryPartitionCommand{
		baseMultiCommand: *newCorrectStreamingMultiCommand(recordset, statement.Namespace),
		policy:           policy,
		writePolicy:      nil,
		statement:        statement,
		operations:       nil,
	}
	cmd.rawCDT = policy.RawCDT
	cmd.terminationErrorType = statement.terminationError()
	cmd.tracker = tracker
	cmd.nodePartitions = nodePartitions
	cmd.node = nodePartitions.node

	return cmd
}

func (cmd *queryPartitionCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *queryPartitionCommand) writeBuffer(ifc command) Error {
	return cmd.setQuery(cmd.policy, cmd.writePolicy, cmd.statement, cmd.recordset.TaskId(), cmd.operations, cmd.writePolicy != nil, cmd.nodePartitions)
}

func (cmd *queryPartitionCommand) shouldRetry(e Error) bool {
	return cmd.tracker != nil && cmd.tracker.shouldRetry(cmd.nodePartitions, e)
}

func (cmd *queryPartitionCommand) commandType() commandType {
	return ttQuery
}

func (cmd *queryPartitionCommand) Execute() Error {
	err := cmd.execute(cmd)
	if err != nil {
		// signal to the executor that no retries should be attempted
		// don't send error unless no retries are planned
		if !cmd.shouldRetry(err) {
			cmd.recordset.sendError(err)
		}
	}
	return err
}

func (cmd *queryPartitionCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *queryPartitionCommand) getNamespace() *string {
	return &cmd.statement.Namespace
}
