// Copyright 2014-2024 Aerospike, Inc.
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

//go:build multinode

package aerospike_test

import (
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// CLIENT-5000 end-to-end: on a multi-node cluster, a batch-UDF whose target node
// blocks past SocketTimeout must come back with per-record InDoubt=true on the
// timed-out writes. The buggy path (split retry across nodes) only runs when keys
// span more than one node, so it cannot be reproduced on a single-node cluster and
// is gated behind the "multinode" build tag (it Skips if only one node is found).
//
// Run against a 2+ node cluster, e.g.:
//
//	go test -tags multinode -h <host> -p <port> -use-services-alternate \
//	    -ginkgo.focus "CLIENT-5000 multi-node"
var _ = gg.Describe("Batch UDF in-doubt on node timeout (CLIENT-5000 multi-node)", func() {
	const nRecPerNode = 10 // any value > 1 per node reproduces it
	const sleepSeconds = 2
	const moduleName = "client5000_ops.lua"
	const pkgName = "client5000_ops"

	// The Lua sandbox has no sleep(). os.time() is wall-clock with 1s granularity,
	// so align to the next second boundary then busy-wait n full seconds; the call
	// then reliably blocks longer than SocketTimeout. (os.clock() is unusable here:
	// it reports CPU time summed across all server threads, crossing the deadline
	// almost instantly under load.)
	const udf = `
local function busy_sleep(n)
    if n == nil or n <= 0 then return end
    local s = os.time()
    while os.time() == s do end
    local start = os.time()
    while os.time() - start < n do end
end

function wait_and_update(rec, bins, n)
    busy_sleep(n)
    if bins ~= nil then
        for b, bv in map.pairs(bins) do
            rec[b] = bv
        end
    end
    return aerospike:update(rec)
end`

	// keysPerNode returns nRecPerNode write-master keys for each node, so the batch
	// can be ordered node-by-node and only the last node made to sleep.
	keysPerNode := func(ns, set string) map[string][]int {
		cluster := client.Cluster()
		pol := as.NewPolicy()
		pol.ReplicaPolicy = as.MASTER

		perNode := map[string][]int{}
		for _, n := range client.GetNodes() {
			perNode[n.GetName()] = []int{}
		}
		for pk := 0; pk < 4_000_000; pk++ {
			key, err := as.NewKey(ns, set, pk)
			if err != nil {
				continue
			}
			ptn, err := as.PartitionForWrite(cluster, pol, key)
			if err != nil {
				continue
			}
			master, err := ptn.GetNodeWrite(cluster)
			if err != nil {
				continue
			}
			if lst, ok := perNode[master.GetName()]; ok && len(lst) < nRecPerNode {
				perNode[master.GetName()] = append(lst, pk)
			}
			full := true
			for _, v := range perNode {
				if len(v) != nRecPerNode {
					full = false
					break
				}
			}
			if full {
				break
			}
		}
		return perNode
	}

	gg.It("marks the slow node's batch-UDF writes in-doubt, leaving the others untouched", func() {
		nodes := client.GetNodes()
		if len(nodes) < 2 {
			gg.Skip("CLIENT-5000 split-retry only reproduces on a multi-node cluster")
		}

		ns := *namespace
		set := randString(50)

		registerUDF(udf, moduleName)
		defer removeUDF(moduleName)

		perNode := keysPerNode(ns, set)
		clusterSize := len(nodes)

		// Seed every key so the UDF updates an existing record.
		for _, n := range nodes {
			for _, pk := range perNode[n.GetName()] {
				key, _ := as.NewKey(ns, set, pk)
				gm.Expect(client.PutBins(nil, key, as.NewBin("bin1_int", 1))).ToNot(gm.HaveOccurred())
			}
		}

		// Build the batch in node order; only the last node's records sleep.
		var batchRecords []as.BatchRecordIfc
		count := 1
		for _, n := range nodes {
			for _, pk := range perNode[n.GetName()] {
				sleep := 0
				if count > (clusterSize-1)*nRecPerNode {
					sleep = sleepSeconds
				}
				key, _ := as.NewKey(ns, set, pk)
				bins := map[string]int{"bin1_int": pk}
				batchRecords = append(batchRecords,
					as.NewBatchUDF(nil, key, pkgName, "wait_and_update",
						as.NewValue(bins), as.NewValue(sleep)))
				count++
			}
		}
		lastNodeStart := (clusterSize - 1) * nRecPerNode
		gm.Expect(len(batchRecords)).To(gm.Equal(clusterSize * nRecPerNode))

		bp := as.NewBatchPolicy()
		bp.SocketTimeout = 1000 * time.Millisecond
		bp.TotalTimeout = time.Duration(sleepSeconds*5) * time.Second
		bp.MaxRetries = 5

		// The batch fails overall because the slow node times out; that is expected.
		// What matters is the per-record in-doubt classification below.
		_ = client.BatchOperate(bp, batchRecords)

		for i, bri := range batchRecords {
			br := bri.BatchRec()
			if i >= lastNodeStart {
				gm.Expect(br.ResultCode).To(gm.Equal(types.NO_RESPONSE),
					"last-node write record %d should have timed out (NO_RESPONSE)", i)
				gm.Expect(br.InDoubt).To(gm.BeTrue(),
					"last-node write record %d must be in-doubt", i)
			} else {
				gm.Expect(br.InDoubt).To(gm.BeFalse(),
					"non-last-node record %d must not be in-doubt", i)
			}
		}
	})
})
