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

import (
	"time"
)

func (clnt *Client) queryPartitions(policy *QueryPolicy, tracker *partitionTracker, statement *Statement, recordset *Recordset) {
	defer recordset.signalEnd()

	// One sampler / usage / command.count decision for the whole public Query
	// (including partition rounds and per-node RPCs).
	var filter *Expression
	if policy != nil {
		filter = policy.FilterExpression
	}
	metricsOn, sampled, callStart := clnt.beginPartitionCallMetrics(ttQuery, filter)
	var terminalErr Error

	// for exponential backoff
	interval := policy.SleepBetweenRetries

	var errs Error
	for {
		list, err := tracker.assignPartitionsToNodes(clnt.Cluster(), statement.Namespace)
		if err != nil {
			errs = chainErrors(err, errs)
			recordset.sendError(errs)
			tracker.partitionError()
			terminalErr = errs
			clnt.endPartitionCallMetrics(sampled, callStart, LatencyQuery, terminalErr)
			return
		}

		maxConcurrentNodes := policy.MaxConcurrentNodes
		if maxConcurrentNodes <= 0 {
			maxConcurrentNodes = len(list)
		}

		if recordset.IsActive() {
			weg := newWeightedErrGroup(maxConcurrentNodes)
			for _, nodePartition := range list {
				cmd := newQueryPartitionCommand(policy, tracker, nodePartition, statement, recordset)
				if metricsOn {
					cmd.setMetricsChild(sampled)
				}
				weg.execute(cmd)
			}
			errs = chainErrors(weg.wait(), errs)
		}

		done, err := tracker.isClusterComplete(clnt.Cluster(), &policy.BasePolicy)
		if !recordset.IsActive() || done || err != nil {
			errs = chainErrors(err, errs)
			// Query is complete.
			if errs != nil {
				tracker.partitionError()
				recordset.sendError(errs)
			}
			terminalErr = errs
			clnt.endPartitionCallMetrics(sampled, callStart, LatencyQuery, terminalErr)
			return
		}

		if policy.SleepBetweenRetries > 0 {
			// Sleep before trying again.
			time.Sleep(interval)

			if policy.SleepMultiplier > 1 {
				interval = time.Duration(float64(interval) * policy.SleepMultiplier)
			}
		}

		recordset.resetTaskID()
	}

}

// beginPartitionCallMetrics records usage/command.count once for a query/scan
// fan-out. Node partition commands must call setMetricsChild with the returned
// sampled flag.
func (clnt *Client) beginPartitionCallMetrics(ct commandType, filter *Expression) (metricsOn, sampled bool, start time.Time) {
	start = time.Now()
	if clnt == nil || clnt.cluster == nil || !clnt.cluster.metricsEnabled.Load() {
		return false, false, start
	}
	clnt.cluster.recordUsage(ct, false, filter)
	sampled = clnt.cluster.decideOperationalSample()
	if sampled {
		clnt.cluster.recordCommandCount()
	}
	return true, sampled, start
}

// endPartitionCallMetrics records cluster.latency.{query} on whole-call success.
func (clnt *Client) endPartitionCallMetrics(sampled bool, start time.Time, lt LatencyType, err Error) {
	if !sampled || err != nil || clnt == nil || clnt.cluster == nil {
		return
	}
	clnt.cluster.addClusterLatency(lt, time.Since(start))
}
