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
)

func (clnt *Client) queryPartitions(policy *QueryPolicy, tracker *partitionTracker, statement *Statement, recordset *Recordset) {
	defer recordset.signalEnd()

	// for exponential backoff
	interval := policy.SleepBetweenRetries

	var errs Error
	for {
		list, err := tracker.assignPartitionsToNodes(clnt.Cluster(), statement.Namespace)
		if err != nil {
			errs = chainErrors(err, errs)
			recordset.sendError(errs)
			tracker.partitionError()
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
