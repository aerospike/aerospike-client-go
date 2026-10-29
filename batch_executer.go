// Copyright 2014-2026 Aerospike, Inc.
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

import "time"

// batchExecute Uses werrGroup to run commands using multiple goroutines,
// and waits for their return.
//
// Sampler / aggregates (Decision 11 / CM-20): one metrics_on for the whole user
// batch. Children record namespace.latency.batch only; this coordinator records
// command.count and, on success, cluster.latency.batch.
func (clnt *Client) batchExecute(policy *BatchPolicy, batchNodes []*batchNode, cmd batcher) (int, Error) {
	maxConcurrentNodes := policy.ConcurrentNodes
	if maxConcurrentNodes <= 0 {
		maxConcurrentNodes = len(batchNodes)
	}

	transStart := time.Now()
	metricsOn := clnt != nil && clnt.cluster != nil && clnt.cluster.metricsEnabled.Load()
	sampled := false
	if metricsOn {
		var filter *Expression
		if policy != nil {
			filter = policy.FilterExpression
		}
		if filter == nil {
			filter = batchCommandAnyFilter(cmd)
		}
		ct := cmd.commandType()
		if ct == ttNone {
			ct = ttBatchRead
		}
		clnt.cluster.recordUsage(ct, false, filter)
		sampled = clnt.cluster.decideOperationalSample()
		if sampled {
			clnt.cluster.recordCommandCount()
		}
	}

	// we need this list to count the number of filtered out records
	list := make([]batcher, 0, len(batchNodes))

	weg := newWeightedErrGroup(maxConcurrentNodes)
	for _, batchNode := range batchNodes {
		newCmd := cmd.cloneBatchCommand(batchNode)
		if metricsOn {
			newCmd.setBatchChildMetrics(sampled)
		}
		list = append(list, newCmd)
		weg.execute(newCmd)
	}

	errs := weg.wait()

	if errs == nil && sampled && clnt != nil && clnt.cluster != nil {
		clnt.cluster.addClusterLatency(LatencyBatch, time.Since(transStart))
		// Once per key on an otherwise successful batch (includes pre-send key
		// errors; avoids per-attempt double count from setError on retries).
		countBatchPartialFailuresOnce(clnt.cluster, cmd)
	}

	// count the filtered out records
	filteredOut := 0
	for i := range list {
		filteredOut += list[i].filteredOut()
	}

	return filteredOut, errs
}

// batchExecuteSimple Uses werrGroup to run commands using multiple goroutines,
// and waits for their return.
//
// Txn verify/roll internals: one sampler decision for the fan-out so node RPCs
// do not each increment command.count / feature.*. Parent does not record
// usage (commands already set usageSkip) or cluster.latency (not a public call).
func (clnt *Client) batchExecuteSimple(policy *BatchPolicy, cmds []command) Error {
	maxConcurrentNodes := policy.ConcurrentNodes
	if maxConcurrentNodes <= 0 {
		maxConcurrentNodes = len(cmds)
	}

	metricsOn := clnt != nil && clnt.cluster != nil && clnt.cluster.metricsEnabled.Load()
	sampled := false
	if metricsOn {
		sampled = clnt.cluster.decideOperationalSample()
	}

	weg := newWeightedErrGroup(maxConcurrentNodes)
	for _, cmd := range cmds {
		if metricsOn {
			if setter, ok := cmd.(interface{ setMetricsChild(bool) }); ok {
				setter.setMetricsChild(sampled)
			}
		}
		weg.execute(cmd)
	}

	return weg.wait()
}
