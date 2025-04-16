// Copyright 2014-2021 Aerospike, Inc.
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
	"context"
	"sync"
	"time"

	"github.com/aerospike/aerospike-client-go/logger"
	"golang.org/x/sync/semaphore"
)

func (clnt *Client) scanPartitions(policy *ScanPolicy, tracker *partitionTracker, namespace string, setName string, rs *Recordset, binNames ...string) error {
	defer rs.signalEnd()

	// for exponential backoff
	interval := policy.SleepBetweenRetries
	for {
		rs.resetTaskID()
		list, err := tracker.assignPartitionsToNodes(clnt.Cluster(), namespace)
		if err != nil {
			return err
		}

		wg := new(sync.WaitGroup)

		// the whole call should be wrapped in a goroutine
		if policy.ConcurrentNodes && len(list) > 1 {
			wg.Add(len(list))

			maxConcurrentNodes := policy.MaxConcurrentNodes
			if maxConcurrentNodes <= 0 {
				maxConcurrentNodes = len(list)
			}
			sem := semaphore.NewWeighted(int64(maxConcurrentNodes))
			ctx := context.Background()

			for _, nodePartition := range list {
				if err := sem.Acquire(ctx, 1); err != nil {
					logger.Logger.Error("Constraint Semaphore failed for Scan: %s", err.Error())
				}
				go func(nodePartition *nodePartitions) {
					defer sem.Release(1)
					defer wg.Done()
					if err := clnt.scanNodePartition(policy, rs, tracker, nodePartition, namespace, setName, binNames...); err != nil {
						logger.Logger.Debug("Error while Executing scan for node %s: %s", nodePartition.node.String(), err.Error())
					}
				}(nodePartition)
			}
		} else {
			wg.Add(1)
			// scan nodes one by one
			go func() {
				defer wg.Done()
				for _, nodePartition := range list {
					if err := clnt.scanNodePartition(policy, rs, tracker, nodePartition, namespace, setName, binNames...); err != nil {
						logger.Logger.Debug("Error while Executing scan for node %s: %s", nodePartition.node.String(), err.Error())
					}
				}
			}()
		}

		wg.Wait()

		if done, err := tracker.isComplete(&policy.BasePolicy); done || err != nil {
			// Scan is complete.
			return err
		}

		if policy.SleepBetweenRetries > 0 {
			// Sleep before trying again.
			time.Sleep(interval)

			if policy.SleepMultiplier > 1 {
				interval = time.Duration(float64(interval) * policy.SleepMultiplier)
			}
		}
	}

}

// ScanNode reads all records in specified namespace and set for one node only.
// If the policy is nil, the default relevant policy will be used.
func (clnt *Client) scanNodePartition(policy *ScanPolicy, recordset *Recordset, tracker *partitionTracker, nodePartition *nodePartitions, namespace string, setName string, binNames ...string) error {
	command := newScanPartitionCommand(policy, tracker, nodePartition, namespace, setName, binNames, recordset)
	return command.Execute()
}
