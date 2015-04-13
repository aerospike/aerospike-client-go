// Copyright 2013-2015 Aerospike, Inc.
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

	. "github.com/aerospike/aerospike-client-go"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Aerospike", func() {
	initTestVars()

	Describe("Node Connection Pool", func() {
		// connection data
		var err error
		var client *Client

		BeforeEach(func() {
			// use the same client for all
			client, err = NewClientWithPolicy(clientPolicy, *host, *port)
			Expect(err).ToNot(HaveOccurred())
		})

		Context("When No Connection Count Limit Is Set", func() {

			It("must return a new connection on every poll", func() {
				clientPolicy := NewClientPolicy()
				clientPolicy.LimitConnectionsToQueueSize = false
				clientPolicy.ConnectionQueueSize = 4

				client, err = NewClientWithPolicy(clientPolicy, *host, *port)
				Expect(err).ToNot(HaveOccurred())
				defer client.Close()

				node := client.GetNodes()[0]

				for i := 0; i < 20; i++ {
					c, err := node.GetConnection(0)
					Expect(err).NotTo(HaveOccurred())
					Expect(c).NotTo(BeNil())
					Expect(c.IsConnected()).To(BeTrue())

					c.Close()
				}

			})

		})

		Context("When A Connection Count Limit Is Set", func() {

			It("must return an error when maximum number of connections are polled", func() {
				clientPolicy := NewClientPolicy()
				clientPolicy.LimitConnectionsToQueueSize = true
				clientPolicy.ConnectionQueueSize = 4

				client, err = NewClientWithPolicy(clientPolicy, *host, *port)
				Expect(err).ToNot(HaveOccurred())
				defer client.Close()

				node := client.GetNodes()[0]

				for i := 0; i < 4; i++ {
					c, err := node.GetConnection(0)
					Expect(err).NotTo(HaveOccurred())
					Expect(c).NotTo(BeNil())
					Expect(c.IsConnected()).To(BeTrue())

					c.Close()
				}

				for i := 0; i < 4; i++ {
					t := time.Now()
					_, err := node.GetConnection(0)
					Expect(err).To(HaveOccurred())
					Expect(time.Now().Sub(t)).To(BeNumerically(">=", time.Millisecond))
					Expect(time.Now().Sub(t)).To(BeNumerically("<", 2*time.Millisecond))
				}

			})

		})
	})
})
