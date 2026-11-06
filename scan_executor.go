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

// scanPartitions fans out per-node scan RPCs. Metrics: one decision for the
// whole Scan (see beginPartitionCallMetrics).
func (clnt *Client) scanPartitions(policy *ScanPolicy, tracker *partitionTracker, namespace string, setName string, recordset *Recordset, binNames ...string) {
	defer recordset.signalEnd()

	var filter *Expression
	if policy != nil {
		filter = policy.FilterExpression
	}
	metricsOn, sampled, callStart := clnt.beginPartitionCallMetrics(ttScan, filter)

	// for exponential backoff
	interval := policy.SleepBetweenRetries

	var errs Error
	for {
		list, err := tracker.assignPartitionsToNodes(clnt.Cluster(), namespace)
		if err != nil {
			errs = chainErrors(err, errs)
			recordset.sendError(errs)
			tracker.partitionError()
			clnt.endPartitionCallMetrics(sampled, callStart, LatencyQuery, errs)
			return
		}

		maxConcurrentNodes := policy.MaxConcurrentNodes
		if maxConcurrentNodes <= 0 {
			maxConcurrentNodes = len(list)
		}

		if recordset.IsActive() {
			weg := newWeightedErrGroup(maxConcurrentNodes)
			for _, nodePartition := range list {
				cmd := newScanPartitionCommand(policy, tracker, nodePartition, namespace, setName, binNames, recordset)
				if metricsOn {
					cmd.setMetricsChild(sampled)
				}
				weg.execute(cmd)
			}
			errs = chainErrors(weg.wait(), errs)
		}

		if done, err := tracker.isClusterComplete(clnt.Cluster(), &policy.BasePolicy); !recordset.IsActive() || done || err != nil {
			// Scan is complete.
			if err != nil {
				errs = chainErrors(err, errs)
				tracker.partitionError()
				recordset.sendError(errs)
			}
			clnt.endPartitionCallMetrics(sampled, callStart, LatencyQuery, errs)
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
