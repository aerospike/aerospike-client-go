/*
 * Copyright 2014-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Count the unique objects in a set using the server's info interface: sum
// the per-node object counts and divide by the replication factor.
func runCountSetObjects() error {
	replFactor, err := replicationFactor(ns)
	if err != nil {
		return err
	}
	log.Println("Replication Factor:", replFactor)

	totalObjects, err := countSetObjects(ns, set)
	if err != nil {
		return err
	}
	log.Println("Total Objects:", totalObjects)

	log.Println("Total Unique Object Count:", totalObjects/replFactor)
	return nil
}

// countSetObjects sums the "objects" statistic of the set across all nodes.
func countSetObjects(ns, set string) (int, error) {
	infoPolicy := as.NewInfoPolicy()
	objCount := 0

N:
	for _, node := range client.GetNodes() {
		cmd := fmt.Sprintf("sets/%s/%s", ns, set)
		info, err := node.RequestInfo(infoPolicy, cmd)
		if err != nil {
			return -1, err
		}
		for _, val := range strings.Split(info[cmd], ":") {
			if i := strings.Index(val, "objects"); i > -1 {
				cnt, err := strconv.Atoi(val[i+len("objects")+1:])
				if err != nil {
					return -1, err
				}
				objCount += cnt
				continue N
			}
		}
	}
	return objCount, nil
}

// replicationFactor reads the namespace's effective replication factor and
// verifies it is consistent across the cluster.
func replicationFactor(ns string) (int, error) {
	const statKey = "effective_replication_factor"
	infoPolicy := as.NewInfoPolicy()
	replFactor := -1

N:
	for _, node := range client.GetNodes() {
		cmd := fmt.Sprintf("namespace/%s", ns)
		info, err := node.RequestInfo(infoPolicy, cmd)
		if err != nil {
			return -1, err
		}
		for _, val := range strings.Split(info[cmd], ";") {
			if i := strings.Index(val, statKey); i > -1 {
				rf, err := strconv.Atoi(val[i+len(statKey)+1:])
				if err != nil {
					return -1, err
				}
				if replFactor == -1 {
					replFactor = rf
				} else if replFactor != rf {
					return -1, fmt.Errorf("inconsistent replication factor for namespace %s in cluster", ns)
				}
				continue N
			}
		}
	}
	return replFactor, nil
}
