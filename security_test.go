// Copyright 2013-2016 Aerospike, Inc.
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
	"fmt"
	"time"

	. "github.com/aerospike/aerospike-client-go"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random charachters
var _ = Describe("Security tests", func() {

	initTestVars()

	// test is not supported because there has been no auth data provided
	if clientPolicy.RequiresAuthentication() {

		// connection data
		var client *Client
		var err error

		BeforeEach(func() {
			client, err = NewClientWithPolicy(clientPolicy, *host, *port)
			Expect(err).ToNot(HaveOccurred())
		})

		AfterEach(func() {
			client.DropUser(nil, "test_user")
		})

		Context("Roles", func() {

			BeforeEach(func() {
				client.CreateUser(nil, "test_user", "test", []string{"user-admin"})
			})

			It("Must query Roles Perfectly", func() {
				admin, err := client.QueryUser(nil, "test_user")
				Expect(err).ToNot(HaveOccurred())

				Expect(admin.User).To(Equal("test_user"))
				Expect(admin.Roles).To(ContainElement("user-admin"))
			})

			It("Must Revoke/Grant Roles Perfectly", func() {
				err := client.GrantRoles(nil, "test_user", []string{"user-admin", "sys-admin", "read-write", "read"})
				Expect(err).ToNot(HaveOccurred())

				admin, err := client.QueryUser(nil, "test_user")
				Expect(err).ToNot(HaveOccurred())

				Expect(admin.User).To(Equal("test_user"))
				Expect(admin.Roles).To(ConsistOf("user-admin", "sys-admin", "read-write", "read"))

				err = client.RevokeRoles(nil, "test_user", []string{"sys-admin"})
				Expect(err).ToNot(HaveOccurred())

				admin, err = client.QueryUser(nil, "test_user")
				Expect(err).ToNot(HaveOccurred())

				Expect(admin.User).To(Equal("test_user"))
				Expect(admin.Roles).To(ConsistOf("user-admin", "read-write", "read"))
			})

		}) // describe roles

		Describe("Users", func() {

			It("Must Create/Drop User", func() {
				// drop before test
				client.DropUser(nil, "test_user")

				err := client.CreateUser(nil, "test_user", "test", []string{"user-admin", "read"})
				Expect(err).ToNot(HaveOccurred())

				admin, err := client.QueryUser(nil, "test_user")
				Expect(err).ToNot(HaveOccurred())

				Expect(admin.User).To(Equal("test_user"))
				Expect(admin.Roles).To(ConsistOf("user-admin", "read"))
			})

			It("Must Change User Password", func() {
				// drop before test
				client.DropUser(nil, "test_user")

				err := client.CreateUser(nil, "test_user", "test", []string{"user-admin", "read"})
				Expect(err).ToNot(HaveOccurred())

				// connect using the new user
				client_policy := NewClientPolicy()
				client_policy.User = "test_user"
				client_policy.Password = "test"
				new_client, err := NewClientWithPolicy(client_policy, *host, *port)
				Expect(err).ToNot(HaveOccurred())
				defer new_client.Close()

				// change current user's password on the fly
				err = new_client.ChangePassword(nil, "test_user", "test1")
				Expect(err).ToNot(HaveOccurred())

				// exhaust all node connections
				for _, node := range new_client.GetNodes() {
					for i := 0; i < client_policy.ConnectionQueueSize; i++ {
						conn, err := node.GetConnection(time.Second)
						Expect(err).ToNot(HaveOccurred())
						node.InvalidateConnection(conn)
					}
				}

				// should have the password changed in the cluster, so that a new connection
				// will be established and used
				admin, err := new_client.QueryUser(nil, "test_user")
				Expect(err).ToNot(HaveOccurred())

				Expect(admin.User).To(Equal("test_user"))
				Expect(admin.Roles).To(ConsistOf("user-admin", "read"))
			})

			It("Must Query all users", func() {
				const USER_COUNT = 10
				// drop before test
				for i := 1; i < USER_COUNT; i++ {
					client.DropUser(nil, fmt.Sprintf("test_user%d", i))
				}

				for i := 1; i < USER_COUNT; i++ {
					err := client.CreateUser(nil, fmt.Sprintf("test_user%d", i), "test", []string{"read"})
					Expect(err).ToNot(HaveOccurred())
				}

				// should have the password changed in the cluster, so that a new connection
				// will be established and used
				users, err := client.QueryUsers(nil)
				Expect(err).ToNot(HaveOccurred())
				Expect(len(users)).To(BeNumerically(">=", USER_COUNT-1))

				for i := 1; i < USER_COUNT; i++ {
					err := client.DropUser(nil, fmt.Sprintf("test_user%d", i))
					Expect(err).ToNot(HaveOccurred())
				}

			})

		}) // describe users
	} // IF
})
