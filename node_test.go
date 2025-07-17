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

package aerospike_test

import (
	"errors"

	as "github.com/aerospike/aerospike-client-go/v8"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike Node Tests", func() {

	gg.Describe("Node Connection Pool", func() {
		// connection data
		var err error
		var client *as.Client

		dbHost := as.NewHost(*host, *port)
		dbHost.TLSName = *nodeTLSName

		gg.BeforeEach(func() {
			// use the same client for all
			client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHost)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.Context("When Authentication is Used", func() {

			if *user != "" {

				gg.It("must return error if it fails to authenticate", func() {
					clientPolicy := as.NewClientPolicy()
					clientPolicy.TlsConfig = tlsConfig
					clientPolicy.User = "non_existent_user"
					clientPolicy.Password = "non_existent_user"

					client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHost)
					gm.Expect(err).To(gm.HaveOccurred())
				})

			}

		})

		gg.Context("When No Connection Count Limit Is Set", func() {

			gg.It("must return a new connection on every poll", func() {
				clientPolicy := as.NewClientPolicy()
				clientPolicy.TlsConfig = tlsConfig
				clientPolicy.LimitConnectionsToQueueSize = false
				clientPolicy.ConnectionQueueSize = 4
				clientPolicy.User = *user
				clientPolicy.Password = *password

				client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHost)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer client.Close()

				for _, node := range client.GetNodes() {
					for i := 0; i < 20; i++ {
						c, err := node.GetConnection(0)
						gm.Expect(err).NotTo(gm.HaveOccurred())
						gm.Expect(c).NotTo(gm.BeNil())
						gm.Expect(c.IsConnected()).To(gm.BeTrue())

						node.InvalidateConnection(c)
					}
				}

			})

		})

		gg.Context("When A Connection Count Limit Is Set", func() {

			gg.Context("When ExitFastOnExhaustedConnectionPool is set", func() {

				gg.It("must return appropriate error when pool is exhausted", func() {
					clientPolicy := as.NewClientPolicy()
					clientPolicy.TlsConfig = tlsConfig
					clientPolicy.LimitConnectionsToQueueSize = true
					clientPolicy.ConnectionQueueSize = 4
					clientPolicy.User = *user
					clientPolicy.Password = *password

					client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHost)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					defer client.Close()

					for _, node := range client.GetNodes() {
						for i := 0; i < clientPolicy.ConnectionQueueSize-1; i++ {
							c, err := node.GetConnection(0)
							gm.Expect(err).NotTo(gm.HaveOccurred())
							gm.Expect(c).NotTo(gm.BeNil())
							gm.Expect(c.IsConnected()).To(gm.BeTrue())

							defer node.InvalidateConnection(c)
						}

						// pool exhausted
						c, err := node.GetConnection(0)
						gm.Expect(err).To(gm.HaveOccurred())
						gm.Expect(errors.Is(err, as.ErrConnectionPoolExhausted)).To(gm.BeTrue())
						gm.Expect(c).To(gm.BeNil())
					}

					// same error on a command
					p := as.NewPolicy()
					p.ExitFastOnExhaustedConnectionPool = true
					p.MaxRetries = 5

					key, _ := as.NewKey(*namespace, randString(50), 5)
					_, err := client.Get(p, key)
					gm.Expect(err).To(gm.HaveOccurred())
					gm.Expect(errors.Is(err, as.ErrConnectionPoolExhausted)).To(gm.BeTrue())

					ae := new(as.AerospikeError)
					res := errors.As(err, &ae)
					gm.Expect(res).To(gm.BeTrue())
					gm.Expect(ae.Iteration).To(gm.Equal(0))
					gm.Expect(ae.Node).ToNot(gm.BeNil())
				})

			})

			gg.It("must return an error when maximum number of connections are polled", func() {
				clientPolicy := as.NewClientPolicy()
				clientPolicy.TlsConfig = tlsConfig
				clientPolicy.LimitConnectionsToQueueSize = true
				clientPolicy.ConnectionQueueSize = 4
				clientPolicy.User = *user
				clientPolicy.Password = *password

				client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHost)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer client.Close()

				node := client.GetNodes()[0]

				cList := []*as.Connection{}

				// 4-1 is because we reserve a connection for tend
				for i := 0; i < 4-1; i++ {
					c, err := node.GetConnection(0)
					gm.Expect(err).NotTo(gm.HaveOccurred())
					gm.Expect(c).NotTo(gm.BeNil())
					gm.Expect(c.IsConnected()).To(gm.BeTrue())

					// don't call invalidate here; we are testing node's connection queue behaviour
					// if there are connections which are not invalidated.
					// Don't call close as well, since it automatically reduces the total conn count.
					// c.Close()
					// append the connections to the list to prevent the invalidator closing them
					cList = append(cList, c)
				}

				// 4-1 is because we reserve a connection for tend
				for i := 0; i < 4-1; i++ {
					_, err := node.GetConnection(0)
					gm.Expect(err).To(gm.HaveOccurred())
				}

				// prevent the optimizer optimizing the cList and it's contents out, since that would trigger the connection finzalizer
				for _, c := range cList {
					gm.Expect(c.IsConnected()).To(gm.BeTrue())
				}
			})

		})
	})
})
