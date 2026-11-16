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

package aerospike_test

import (
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Stats with metrics disabled", func() {

	var ns = *namespace
	var set = randString(50)

	clusterStat := func(name string) float64 {
		stats, err := client.Stats()
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return stats["cluster-aggregated-stats"].(map[string]any)[name].(float64)
	}

	gg.BeforeEach(func() {
		client.DisableMetrics()
		gg.DeferCleanup(func() {
			client.EnableMetrics(nil)
		})
	})

	gg.It("must count transaction errors and retries", func() {
		key, err := as.NewKey(ns, set, randString(50))
		gm.Expect(err).ToNot(gm.HaveOccurred())
		gm.Expect(client.PutBins(nil, key, as.NewBin("blob", make([]byte, 512*1024)))).To(gm.Succeed())
		gg.DeferCleanup(func() {
			client.Delete(nil, key)
		})

		errorsBefore := clusterStat("transaction-error-count")
		retriesBefore := clusterStat("transaction-retry-count")

		policy := as.NewPolicy()
		policy.TotalTimeout = 2 * time.Millisecond
		policy.SocketTimeout = 20 * time.Microsecond
		policy.MaxRetries = 3
		policy.ReadModeSC = client.DefaultPolicy.ReadModeSC

		failed := 0
		for i := 0; i < 2000 && failed < 50; i++ {
			if _, err := client.Get(policy, key); err != nil {
				failed++
			}
		}
		gm.Expect(failed).To(gm.BeNumerically(">", 0))

		gm.Expect(client.MetricsEnabled()).To(gm.BeFalse())
		gm.Expect(clusterStat("transaction-error-count")).To(gm.BeNumerically(">", errorsBefore))
		gm.Expect(clusterStat("transaction-retry-count")).To(gm.BeNumerically(">", retriesBefore))
	})
})
