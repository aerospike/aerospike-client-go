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
	"strconv"
	"strings"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// ExecuteTask is used to poll for long running server execute job completion.
type ExecuteTask struct {
	*baseTask

	taskID uint64
	scan   bool

	// The following map keeps an account of what nodes were ever observed with the job registered on them.
	// If the job was ever observed, the task will return true for it is not found anymore (purged from task queue after completion)
	observed map[string]struct{}
}

// NewExecuteTask initializes task with fields needed to query server nodes.
func NewExecuteTask(cluster *Cluster, statement *Statement, taskId uint64) *ExecuteTask {
	return &ExecuteTask{
		baseTask: newTask(cluster),
		taskID:   taskId,
		scan:     statement.IsScan(),
		observed: make(map[string]struct{}, len(cluster.GetNodes())),
	}
}

// TaskId returns the task id that was sent to the server.
func (etsk *ExecuteTask) TaskId() uint64 {
	return etsk.taskID
}

// IsDone queries all nodes for task completion status.
func (etsk *ExecuteTask) IsDone() (bool, Error) {
	var module string
	if etsk.scan {
		module = "scan"
	} else {
		module = "query"
	}

	taskId := strconv.FormatUint(etsk.taskID, 10)

	cmd1 := "query-show:trid=" + taskId
	cmd2 := module + "-show:trid=" + taskId
	cmd3 := "jobs:module=" + module + ";cmd=get-job;trid=" + taskId

	nodes := etsk.cluster.GetNodes()

	for _, node := range nodes {
		var command string
		if node.SupportsPartitionQuery() {
			// query-show works for both scan and query.
			command = cmd1
		} else if node.SupportsQueryShow() {
			command = cmd2
		} else {
			command = cmd3
		}

		responseMap, err := node.requestInfoWithRetry(&etsk.cluster.infoPolicy, 5, command)
		if err != nil {
			return false, err
		}
		response := responseMap[command]

		if strings.HasPrefix(response, "ERROR:2") {
			// If the server node is v6+, it will immediately put the job on queue, so if it is not found,
			// it means that it is completed.
			if !node.SupportsPartitionQuery() {
				// Task not found. On server prior to v6, this could mean task was already completed or not started yet.
				// If the job was not observed before, its completion is in doubt.
				// Otherwise it means it was completed.
				if _, existed := etsk.observed[node.GetName()]; !existed && etsk.retries.Get() < 20 {
					// If the job was not found in some nodes, it may mean that the job was not started yet.
					// So we will keep retrying.
					return false, nil
				}
			}

			// Assume the task was completed.
			continue
		}

		if strings.HasPrefix(response, "ERROR:") {
			// Mark done and quit immediately.
			return false, newError(types.UDF_BAD_RESPONSE, response)
		}

		// mark the node as observed
		etsk.observed[node.GetName()] = struct{}{}

		find := "status="
		index := strings.Index(response, find)

		if index < 0 {
			return false, nil
		}

		begin := index + len(find)
		response = response[begin:]
		find = ":"
		index = strings.Index(response, find)

		if index < 0 {
			continue
		}

		status := strings.ToLower(response[:index])
		if !strings.HasPrefix(status, "done") {
			return false, nil
		}
	}

	return true, nil
}

// OnComplete returns a channel which will be closed when the task is
// completed.
// If an error is encountered while performing the task, an error
// will be sent on the channel.
func (etsk *ExecuteTask) OnComplete() chan Error {
	return etsk.onComplete(etsk)
}
