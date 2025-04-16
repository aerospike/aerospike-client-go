// Copyright 2013-2020 Aerospike, Inc.
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
	as "github.com/aerospike/aerospike-client-go"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike", func() {

	gg.Describe("Host", func() {

		gg.It("must handle multiple valid host strings", func() {
			// use the same client for all
			hosts, err := as.NewHosts("host1:4000", "host2:3000", "127.0.0.1:1200", "[2001:0db8:85a3:0000:0000:8a2e:0370]:7334")
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(hosts).To(gm.Equal([]*as.Host{as.NewHost("host1", 4000), as.NewHost("host2", 3000), as.NewHost("127.0.0.1", 1200), as.NewHost("2001:0db8:85a3:0000:0000:8a2e:0370", 7334)}))
		})

		gg.It("must error on invalid host strings", func() {
			// use the same client for all
			hosts, err := as.NewHosts("host1:4000", "host2://+3000")
			gm.Expect(err).To(gm.HaveOccurred())
			gm.Expect(hosts).To(gm.BeNil())
		})
	})
})
