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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// IndexTask is used to poll for long running create index completion.
type IndexTask struct {
	*baseTask

	namespace string
	indexName string
}

// NewIndexTask initializes a task with fields needed to query server nodes.
func NewIndexTask(cluster *Cluster, namespace string, indexName string) *IndexTask {
	return &IndexTask{
		baseTask:  newTask(cluster, 0),
		namespace: namespace,
		indexName: indexName,
	}
}

// newIndexTaskWithTimeout initializes a task with fields needed to query server nodes and a timeout.
func newIndexTaskWithTimeout(cluster *Cluster, namespace string, indexName string, timeout time.Duration) *IndexTask {
	return &IndexTask{
		baseTask:  newTask(cluster, timeout),
		namespace: namespace,
		indexName: indexName,
	}
}

// IsDone queries all nodes for task completion status.
func (tski *IndexTask) IsDone() (bool, Error) {
	nodes := tski.cluster.GetNodes()

	r := regexp.MustCompile(`load_pct=(\d+)`)

	for _, node := range nodes {
		serverVersion := node.GetServerVersion()
		statusCommand := types.Ternary(
			serverVersion.IsGreaterOrEqual(version.ServerVersion_8_1),
			"sindex-stat:namespace="+tski.namespace+";indexname="+tski.indexName,
			"sindex/"+tski.namespace+"/"+tski.indexName)
		
		responseMap, err := node.requestInfoWithRetry(&tski.cluster.infoPolicy, 5, statusCommand)
		if err != nil {
			return false, err
		}

		// Get the response for our status command
		response, exists := responseMap[statusCommand]
		
		// Handle missing or empty response
		if !exists || response == "" {
			return false, newError(types.INDEX_GENERIC, 
				"sindex-stat failed: empty or missing response from node "+node.GetName())
		}

		// Check for load_pct in response
		find := "load_pct="
		index := strings.Index(response, find)

		if index < 0 {
			// Index not found - check if it's an error response
			if strings.Contains(response, "FAIL") || strings.Contains(response, "ERROR") {
				return false, newError(types.INDEX_GENERIC, 
					"sindex-stat failed: "+response+" from node "+node.GetName())
			}
			// Index not readable yet, continue polling
			return false, nil
		}

		matchRes := r.FindStringSubmatch(response)
		if len(matchRes) < 2 {
			return false, newError(types.INDEX_GENERIC, 
				"sindex-stat failed: could not parse load_pct from response '"+response+"'")
		}
		
		pct, parseErr := strconv.Atoi(matchRes[1])
		if parseErr != nil {
			return false, newError(types.INDEX_GENERIC, 
				"sindex-stat failed: invalid load_pct value '"+matchRes[1]+"'")
		}

		if pct < 100 {
			// Index still building on this node
			return false, nil
		}
	}
	
	// All nodes report 100% complete
	return true, nil
}

// OnComplete returns a channel that will be closed as soon as the task is finished.
// If an error is encountered during operation, an error will be sent on the channel.
func (tski *IndexTask) OnComplete() chan Error {
	return tski.onComplete(tski)
}
