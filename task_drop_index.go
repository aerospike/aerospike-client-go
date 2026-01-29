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
	"time"

	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// DropIndexTask is used to poll for long running create index completion.
type DropIndexTask struct {
	*baseTask

	namespace string
	indexName string
}

// NewDropIndexTask initializes a task with fields needed to query server nodes.
func NewDropIndexTask(cluster *Cluster, namespace string, indexName string) *DropIndexTask {
	return &DropIndexTask{
		baseTask:  newTask(cluster, 0),
		namespace: namespace,
		indexName: indexName,
	}
}

// newDropIndexTaskWithTimeout initializes a task with fields needed to query server nodes and a timeout.
func newDropIndexTaskWithTimeout(cluster *Cluster, namespace string, indexName string, timeout time.Duration) *DropIndexTask {
	return &DropIndexTask{
		baseTask:  newTask(cluster, timeout),
		namespace: namespace,
		indexName: indexName,
	}
}

// IsDone queries all nodes for task completion status.
func (tski *DropIndexTask) IsDone() (bool, Error) {
	nodes := tski.cluster.GetNodes()

	for _, node := range nodes {
		serverVersion := node.GetServerVersion()
		statusCommand := types.Ternary(
			serverVersion.IsGreaterOrEqual(version.ServerVersion_8_1),
			"sindex-exists:namespace="+tski.namespace+";indexname="+tski.indexName,
			"sindex-exists:ns="+tski.namespace+";indexname="+tski.indexName)
		
		responseMap, err := node.requestInfoWithRetry(&tski.cluster.infoPolicy, 5, statusCommand)
		if err != nil {
			return false, err
		}

		// Get the response for our status command
		response, exists := responseMap[statusCommand]
		
		// Handle missing or empty response
		if !exists || response == "" {
			return false, newError(types.INDEX_GENERIC, 
				"sindex-exists failed: empty or missing response from node "+node.GetName())
		}

		if response == "false" {
			// Index does not exist on this node (dropped successfully)
			continue
		}

		if response == "true" {
			// Index still exists on this node (not yet dropped)
			return false, nil
		}

		// Unexpected response
		return false, newError(types.INDEX_GENERIC, 
			"sindex-exists failed: unexpected response '"+response+"' from node "+node.GetName())
	}

	// All nodes report index does not exist
	return true, nil
}

// OnComplete returns a channel that will be closed as soon as the task is finished.
// If an error is encountered during operation, an error will be sent on the channel.
func (tski *DropIndexTask) OnComplete() chan Error {
	return tski.onComplete(tski)
}
