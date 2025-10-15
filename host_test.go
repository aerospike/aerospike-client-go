// Copyright 2013-2022 Aerospike, Inc.
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
	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
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

		gg.It("must correctly identify localhost hosts", func() {
			// Test cases that should return true (localhost)
			localhostCases := []*as.Host{
				as.NewHost("localhost", 3000),
				as.NewHost("127.0.0.1", 3000),
				as.NewHost("127.0.0.2", 3000),
				as.NewHost("127.255.255.254", 3000),
				as.NewHost("::1", 3000),
			}

			for _, host := range localhostCases {
				gm.Expect(host.IsLocalhost()).To(gm.BeTrue(), "Expected %s to be localhost", host.Name)
			}

			// Test cases that should return false (not localhost)
			nonLocalhostCases := []*as.Host{
				as.NewHost("192.168.1.1", 3000),
				as.NewHost("10.0.0.1", 3000),
				as.NewHost("example.com", 3000),
				as.NewHost("::2", 3000),
				as.NewHost("2001:db8::1", 3000),
				as.NewHost("", 3000), // empty string
			}

			for _, host := range nonLocalhostCases {
				gm.Expect(host.IsLocalhost()).To(gm.BeFalse(), "Expected %s to not be localhost", host.Name)
			}
		})

		gg.It("must correctly compare Host instances for equality", func() {
			// Test equal hosts
			host1 := as.NewHost("localhost", 3000)
			host2 := as.NewHost("localhost", 3000)
			gm.Expect(host1.Equals(host2)).To(gm.BeTrue(), "Expected identical hosts to be equal")

			// Test hosts with TLS names
			host1.TLSName = "tls.example.com"
			host2.TLSName = "tls.example.com"
			gm.Expect(host1.Equals(host2)).To(gm.BeTrue(), "Expected hosts with same TLS names to be equal")

			// Test different names
			host3 := as.NewHost("different-host", 3000)
			gm.Expect(host1.Equals(host3)).To(gm.BeFalse(), "Expected hosts with different names to not be equal")

			// Test different ports
			host4 := as.NewHost("localhost", 4000)
			gm.Expect(host1.Equals(host4)).To(gm.BeFalse(), "Expected hosts with different ports to not be equal")

			// Test different TLS names
			host5 := as.NewHost("localhost", 3000)
			host5.TLSName = "different-tls.example.com"
			gm.Expect(host1.Equals(host5)).To(gm.BeFalse(), "Expected hosts with different TLS names to not be equal")

			// Test nil comparison
			gm.Expect(host1.Equals(nil)).To(gm.BeFalse(), "Expected host compared with nil to be false")

			// Test empty vs non-empty TLS names
			host6 := as.NewHost("localhost", 3000)
			host7 := as.NewHost("localhost", 3000)
			host6.TLSName = ""
			host7.TLSName = "tls.example.com"
			gm.Expect(host6.Equals(host7)).To(gm.BeFalse(), "Expected hosts with different TLS name states to not be equal")

			// Test both empty TLS names
			host8 := as.NewHost("localhost", 3000)
			host9 := as.NewHost("localhost", 3000)
			gm.Expect(host8.Equals(host9)).To(gm.BeTrue(), "Expected hosts with both empty TLS names to be equal")
		})
	})
})
