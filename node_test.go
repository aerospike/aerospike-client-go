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
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// resolveServicesType converts the test flag values into a ServicesType.
// It reads ServicesTypeFlag first; if it is "auto" (the default) it also
// checks the legacy -use-services-alternate bool for backward compat.
func resolveServicesType() as.ServicesType {
	switch *ServicesTypeFlag {
	case "alternate":
		return as.ServicesAlternate
	case "main":
		return as.ServicesMain
	default:
		if *UseServicesAlternate {
			return as.ServicesAlternate
		}
		return as.ServicesAuto
	}
}

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Aerospike Node Tests", func() {

	gg.Describe("Node Connection Pool", func() {
		// connection data
		var err error
		var client *as.Client

		gg.BeforeEach(func() {
			// use the same client for all
			client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.Context("When Authentication is Used", func() {

			if *user != "" {

				gg.It("must return error if it fails to authenticate", func() {
					clientPolicy := as.NewClientPolicy()
					clientPolicy.TlsConfig = tlsConfig
				clientPolicy.User = "non_existent_user"
				clientPolicy.Password = "non_existent_user"
				clientPolicy.ServicesType = resolveServicesType()
					client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
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
			clientPolicy.ServicesType = resolveServicesType()

			client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
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
				clientPolicy.ServicesType = resolveServicesType()

				client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
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
			clientPolicy.ServicesType = resolveServicesType()

			client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
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

		gg.Context("When Idle Timeout Is Used", func() {

			gg.It("must reuse connections before they become idle", func() {
				clientPolicy := as.NewClientPolicy()
				clientPolicy.TlsConfig = tlsConfig
			clientPolicy.IdleTimeout = 1000 * time.Millisecond
			// clientPolicy.TendInterval = time.Hour
			clientPolicy.User = *user
			clientPolicy.Password = *password
			clientPolicy.ServicesType = resolveServicesType()

			client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer client.Close()

			node := client.GetNodes()[0]

			// get a few connections at once
				var conns []*as.Connection
				for i := 0; i < 4; i++ {
					// gg.By(fmt.Sprintf("Retrieving conns i=%d", i))
					c, err := node.GetConnection(0)
					gm.Expect(err).NotTo(gm.HaveOccurred())
					gm.Expect(c).NotTo(gm.BeNil())
					gm.Expect(c.IsConnected()).To(gm.BeTrue())

					conns = append(conns, c)
				}

				// return them to the pool
				for _, c := range conns {
					node.PutConnection(c)
				}

				start := time.Now()
				estimatedDeadline := start.Add(clientPolicy.IdleTimeout)
				deadlineThreshold := clientPolicy.IdleTimeout / 10

				// make sure the same connections are all retrieved again
				checkCount := 0
				for time.Until(estimatedDeadline) > deadlineThreshold {
					checkCount++
					// gg.By(fmt.Sprintf("Retrieving conns2 checkCount=%d", checkCount))
					var conns2 []*as.Connection
					for i := 0; i < len(conns); i++ {
						c, err := node.GetConnection(0)
						gm.Expect(err).NotTo(gm.HaveOccurred())
						gm.Expect(c).NotTo(gm.BeNil())
						gm.Expect(c.IsConnected()).To(gm.BeTrue())
						gm.Expect(conns).To(gm.ContainElement(c))
						gm.Expect(conns2).NotTo(gm.ContainElement(c))

						conns2 = append(conns2, c)
					}

					// just put them in the pool
					for _, c := range conns2 {
						node.PutConnection(c)
					}

					time.Sleep(time.Millisecond)
				}

				// we should be called lots of times
				gm.Expect(checkCount).To(gm.BeNumerically(">", 500))

				// sleep again until all connections are all idle
				<-time.After(2 * clientPolicy.IdleTimeout)

				// get connections again, making sure they are all new
				var conns3 []*as.Connection
				for i := 0; i < len(conns); i++ {
					// gg.By(fmt.Sprintf("Retrieving conns3 i=%d", i))
					c, err := node.GetConnection(0)
					gm.Expect(err).NotTo(gm.HaveOccurred())
					gm.Expect(c).NotTo(gm.BeNil())
					gm.Expect(c.IsConnected()).To(gm.BeTrue())

					gm.Expect(conns).NotTo(gm.ContainElement(c))
					gm.Expect(conns3).NotTo(gm.ContainElement(c))

					conns3 = append(conns3, c)
				}

				// refresh and return them to the pool
				for _, c := range conns {
					gm.Expect(c.IsConnected()).To(gm.BeFalse())
				}

				// don't forget to close connections
				for _, c := range conns3 {
					c.Close()
				}
			})

			gg.It("must maintain a minimum number of connections per client policy even if idle", func() {
				clientPolicy := as.NewClientPolicy()
				clientPolicy.TlsConfig = tlsConfig
			clientPolicy.IdleTimeout = 2000 * time.Millisecond
			clientPolicy.MinConnectionsPerNode = 5
			clientPolicy.User = *user
			clientPolicy.Password = *password
			clientPolicy.ServicesType = resolveServicesType()

				client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer client.Close()

				client.WarmUp(10)

				node := client.GetNodes()[0]

				gm.Expect(node.ConnsCount()).To(gm.BeNumerically(">=", 10))

				// sleep again until all connections are all idle
				<-time.After(2 * clientPolicy.IdleTimeout)
				gm.Expect(node.ConnsCount()).To(gm.Equal(clientPolicy.MinConnectionsPerNode + 1)) // min + 1 reserved for tend
			})

			gg.It("must delay the connection from becoming idle if it is put back in the queue", func() {
				clientPolicy := as.NewClientPolicy()
				clientPolicy.TlsConfig = tlsConfig
			clientPolicy.IdleTimeout = 1000 * time.Millisecond
			clientPolicy.User = *user
			clientPolicy.Password = *password
			clientPolicy.ServicesType = resolveServicesType()

			client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer client.Close()

			node := client.GetNodes()[0]

			deadlineThreshold := clientPolicy.IdleTimeout / 10

				// gg.By("Retrieving c")
				c, err := node.GetConnection(0)
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(c).NotTo(gm.BeNil())
				gm.Expect(c.IsConnected()).To(gm.BeTrue())
				node.PutConnection(c)

				// continuously refresh the connection just before it goes idle
				for i := 0; i < 5; i++ {
					time.Sleep(clientPolicy.IdleTimeout - deadlineThreshold)
					// gg.By(fmt.Sprintf("Retrieving c2 i=%d", i))

					c2, err := node.GetConnection(0)
					gm.Expect(err).NotTo(gm.HaveOccurred())
					gm.Expect(c2).NotTo(gm.BeNil())
					gm.Expect(c2).To(gm.Equal(c))
					gm.Expect(c2.IsConnected()).To(gm.BeTrue())

					node.PutConnection(c2)
				}

				// wait about the required time to become idle
				<-time.After(2 * clientPolicy.IdleTimeout)

				// we should get a new connection
				c3, err := node.GetConnection(0)
				gm.Expect(err).NotTo(gm.HaveOccurred())
				gm.Expect(c3).NotTo(gm.BeNil())
				defer node.InvalidateConnection(c3)
				gm.Expect(c3).ToNot(gm.Equal(c))
				gm.Expect(c3.IsConnected()).To(gm.BeTrue())

				// the original connection should be closed
				gm.Expect(c.IsConnected()).To(gm.BeFalse())
			})

		})
	})

	gg.Describe("Node circuit breaker", func() {
		var err error
		var client *as.Client
		var maxErrorRate int
		var errRateWindow int

		gg.BeforeEach(func() {
			maxErrorRate = 100
			errRateWindow = 2

			clientPolicy = as.NewClientPolicy()
			clientPolicy.TlsConfig = tlsConfig
		clientPolicy.IdleTimeout = 1000 * time.Millisecond
		clientPolicy.User = *user
		clientPolicy.Password = *password
		clientPolicy.MaxErrorRate = maxErrorRate
		clientPolicy.ErrorRateWindow = errRateWindow
		clientPolicy.ServicesType = resolveServicesType()

			client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHosts...)
			gm.Expect(err).ToNot(gm.HaveOccurred())
		})

		gg.AfterEach(func() {
			if client != nil {
				client.Close()
			}
		})

		gg.Context("When Circuit Breaker is Used", func() {
			gg.It("should not trip when error count is below threshold", func() {
				node := client.GetNodes()[0]

				// Add errors but stay below threshold
				for i := 0; i < clientPolicy.MaxErrorRate-1; i++ {
					node.IncrementErrorCount()
				}

				// Should not trip
				err := node.ValidateErrorCount()
				gm.Expect(err).To(gm.BeNil())

				// Error count should be reset
				gm.Expect(node.GetErrorCount()).To(gm.Equal(0))
			})

			gg.It("should trip the circuit breaker when error count exceeds maxErrorRate", func() {
				node := client.GetNodes()[0]

				// Add enough errors to exceed maxErrorRate
				for i := 0; i < clientPolicy.MaxErrorRate+1; i++ {
					node.IncrementErrorCount()
				}

				// Verify circuit breaker trips
				err := node.ValidateErrorCount()
				gm.Expect(err).ToNot(gm.BeNil())
				gm.Expect(err.Matches(types.MAX_ERROR_RATE)).To(gm.BeTrue())

				// Error count should be reset
				gm.Expect(node.GetErrorCount()).To(gm.Equal(0))
			})

			gg.It("should halve maxErrorCount threshold after each breach", func() {
				node := client.GetNodes()[0]
				initialMax := clientPolicy.MaxErrorRate

				// First breach
				for i := 0; i < initialMax+1; i++ {
					node.IncrementErrorCount()
				}
				err := node.ValidateErrorCount()
				gm.Expect(err).ToNot(gm.BeNil())

				// Second breach with lower threshold
				for i := 0; i < initialMax/2+1; i++ {
					node.IncrementErrorCount()
				}
				err = node.ValidateErrorCount()
				gm.Expect(err).ToNot(gm.BeNil())

				// Third breach with even lower threshold
				for i := 0; i < initialMax/4+1; i++ {
					node.IncrementErrorCount()
				}
				err = node.ValidateErrorCount()
				gm.Expect(err).ToNot(gm.BeNil())
			})

			gg.It("should not reduce maxErrorCount below 1", func() {
				node := client.GetNodes()[0]

				// Set up a small initial max error rate
				node.SetMaxErrorCount(2)

				// First breach - should halve to 1
				for i := 0; i < 3; i++ {
					node.IncrementErrorCount()
				}
				err := node.ValidateErrorCount()
				gm.Expect(err).ToNot(gm.BeNil())

				// Second breach - should still be 1
				for i := 0; i < 2; i++ {
					node.IncrementErrorCount()
				}
				err = node.ValidateErrorCount()
				gm.Expect(err).ToNot(gm.BeNil())

				// Should not breach with just 1 error (threshold is 1, so 1 <= 1 should pass)
				node.IncrementErrorCount()
				err = node.ValidateErrorCount()
				gm.Expect(err).To(gm.BeNil())
			})

			gg.It("should disable circuit breaker when MaxErrorRate is 0", func() {
				// Create new client with circuit breaker disabled
				disabledPolicy := as.NewClientPolicy()
				disabledPolicy.TlsConfig = tlsConfig
			disabledPolicy.MaxErrorRate = 0 // Disable circuit breaker
			disabledPolicy.ServicesType = resolveServicesType()

				disabledClient, err := as.NewClientWithPolicyAndHost(disabledPolicy, dbHosts...)
				gm.Expect(err).ToNot(gm.HaveOccurred())
				defer disabledClient.Close()

				node := disabledClient.GetNodes()[0]

				// When MaxErrorRate is 0, IncrementErrorCount doesn't actually increment
				// So we need to verify that ValidateErrorCount always passes, even after
				// calling IncrementErrorCount many times
				for i := 0; i < 1000; i++ {
					node.IncrementErrorCount()
				}

				// Verify error count remains 0 because MaxErrorRate is 0
				gm.Expect(node.GetErrorCount()).To(gm.Equal(0))

				// Circuit breaker should not trip because errors aren't being counted
				err = node.ValidateErrorCount()
				gm.Expect(err).To(gm.BeNil())
			})

			gg.It("should restore maxErrorCount to policy value after successful validation", func() {
				node := client.GetNodes()[0]
				initialMax := clientPolicy.MaxErrorRate

				// First breach - reduces threshold to half
				for i := 0; i < initialMax+1; i++ {
					node.IncrementErrorCount()
				}
				err := node.ValidateErrorCount()
				gm.Expect(err).ToNot(gm.BeNil())

				// Successful validation with error count below new threshold
				node.IncrementErrorCount() // Just one error
				err = node.ValidateErrorCount()
				gm.Expect(err).To(gm.BeNil())

				// Verify maxErrorCount was restored to policy value
				for i := 0; i < initialMax+1; i++ {
					node.IncrementErrorCount()
				}
				err = node.ValidateErrorCount()
				gm.Expect(err).ToNot(gm.BeNil())
			})
		})
	})
})
