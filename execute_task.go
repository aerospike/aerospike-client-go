// Copyright 2013-2014 Aerospike, Inc.
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

	. "github.com/aerospike/aerospike-client-go/types"
)

/**
 * Task used to poll for long running server execute job completion.
 */
type ExecuteTask struct {
	BaseTask

	taskId int
	scan   bool
}

/**
 * Initialize task with fields needed to query server nodes.
 */
func NewExecuteTask(cluster *Cluster, statement *Statement) *ExecuteTask {
	return &ExecuteTask{
		BaseTask: *NewTask(cluster, false),
		taskId:   statement.GetTaskId(),
		scan:     statement.IsScan(),
	}
}

/**
 * Query all nodes for task completion status.
 */
func (this *ExecuteTask) IsDone() (bool, error) {
	var command string
	if this.scan {
		command = "scan-list"
	} else {
		command = "query-list"
	}

	nodes := this.cluster.GetNodes()
	done := false

	for _, node := range nodes {
		responseMap, err := RequestInfoForNode(node, command)
		if err != nil {
			return false, err
		}

		response := responseMap[command]
		find := "job_id=" + strconv.Itoa(this.taskId) + ":"
		index := strings.Index(response, find)

		if index < 0 {
			done = true
			continue
		}

		begin := index + len(find)
		find = "job_status="
		index = strings.Index(response[begin:], find)

		if index < 0 {
			continue
		}

		begin = index + len(find)
		end := strings.Index(response[begin:], ":")
		status := response[begin:end]

		if status == "ABORTED" {
			return false, QueryTerminatedErr()
		} else if status == "IN PROGRESS" {
			return false, nil
		} else if status == "DONE" {
			done = true
		}
	}

	return done, nil
}

func (this *ExecuteTask) OnComplete() chan error {
	return this.onComplete(this)
}
