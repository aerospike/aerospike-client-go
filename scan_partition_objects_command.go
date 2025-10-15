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
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

type scanPartitionObjectsCommand struct {
	baseMultiCommand

	policy    *ScanPolicy
	namespace string
	setName   string
	binNames  []string
	taskID    uint64
}

func newScanPartitionObjectsCommand(
	policy *ScanPolicy,
	tracker *partitionTracker,
	nodePartitions *nodePartitions,
	namespace string,
	setName string,
	binNames []string,
	recordset *Recordset,
) *scanPartitionObjectsCommand {
	cmd := &scanPartitionObjectsCommand{
		baseMultiCommand: *newCorrectStreamingMultiCommand(recordset, namespace),
		policy:           policy,
		namespace:        namespace,
		setName:          setName,
		binNames:         binNames,
	}
	cmd.terminationErrorType = types.SCAN_TERMINATED
	cmd.tracker = tracker
	cmd.nodePartitions = nodePartitions
	cmd.node = nodePartitions.node

	return cmd
}

func (cmd *scanPartitionObjectsCommand) getPolicy(ifc command) Policy {
	return cmd.policy
}

func (cmd *scanPartitionObjectsCommand) writeBuffer(ifc command) Error {
	return cmd.setScan(cmd.policy, &cmd.namespace, &cmd.setName, cmd.binNames, cmd.recordset.taskID, cmd.nodePartitions)
}

func (cmd *scanPartitionObjectsCommand) parseResult(ifc command, conn *Connection) Error {
	return cmd.baseMultiCommand.parseResult(ifc, conn)
}

func (cmd *scanPartitionObjectsCommand) shouldRetry(e Error) bool {
	return cmd.tracker != nil && cmd.tracker.shouldRetry(cmd.nodePartitions, e)
}

func (cmd *scanPartitionObjectsCommand) commandType() commandType {
	return ttScan
}

func (cmd *scanPartitionObjectsCommand) Execute() Error {
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

func (cmd *scanPartitionObjectsCommand) getNamespaces() iter.Seq2[string, uint64] {
	return nil
}

func (cmd *scanPartitionObjectsCommand) getNamespace() *string {
	return &cmd.namespace
}

func (cmd *scanPartitionObjectsCommand) salvageConn(timeoutDelay time.Duration, conn *Connection, node *Node) {
	// TODO: implement scan and query commands later
	conn.Close()
}