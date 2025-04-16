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

package aerospike_test

import (
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

// ALL tests are isolated by SetName and Key, which are 50 random characters
var _ = gg.Describe("Security tests", func() {

	var ns = *namespace

	// connection data
	var client *as.Client
	var err error

	gg.BeforeEach(func() {
		if !securityEnabled() {
			gg.Skip("Security Tests are not supported in the Community Edition.")
		}

		client, err = as.NewClientWithPolicy(clientPolicy, *host, *port)
		gm.Expect(err).ToNot(gm.HaveOccurred())
	})

	gg.AfterEach(func() {
		client.DropUser(nil, "test_user")
		time.Sleep(time.Second)
	})

	gg.Context("Roles", func() {

		gg.BeforeEach(func() {
			client.CreateUser(nil, "test_user", "test", []string{"user-admin"})
			time.Sleep(time.Second)
		})

		gg.It("Must work with Roles Perfectly", func() {
			defer client.DropRole(nil, "role-read-test-test")
			defer client.DropRole(nil, "role-write-test")
			defer client.DropRole(nil, "dummy-role")

			// Add a user defined Role
			err := client.CreateRole(nil, "role-read-test-test", []as.Privilege{{Code: as.Read, Namespace: ns, SetName: "test"}}, []string{getOutboundIP().String()})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.CreateRole(nil, "role-write-test", []as.Privilege{{Code: as.ReadWrite, Namespace: ns, SetName: ""}}, []string{getOutboundIP().String()})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			// Add privileges to the roles
			err = client.GrantPrivileges(nil, "role-read-test-test", []as.Privilege{{Code: as.ReadWrite, Namespace: ns, SetName: "bar"}, {Code: as.ReadWriteUDF, Namespace: ns, SetName: "test"}})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Wait until servers syncronize
			time.Sleep(1 * time.Second)

			// Revoke privileges from the roles
			err = client.RevokePrivileges(nil, "role-read-test-test", []as.Privilege{{Code: as.ReadWriteUDF, Namespace: ns, SetName: "test"}})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			err = client.CreateRole(nil, "dummy-role", []as.Privilege{{Code: as.Read, Namespace: "", SetName: ""}}, []string{getOutboundIP().String()})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			// Drop the dummy role to make sure DropRoles Works
			err = client.DropRole(nil, "dummy-role")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// Wait until servers syncronize
			time.Sleep(3 * time.Second)

			roles, err := client.QueryRoles(nil)
			gm.Expect(err).ToNot(gm.HaveOccurred())

			// gm.Expect(len(roles)).To(gm.Equal(8))

			// Predefined Roles
			gm.Expect(roles).To(gm.ContainElement(&as.Role{Name: "read", Privileges: []as.Privilege{{Code: as.Read, Namespace: "", SetName: ""}}}))
			gm.Expect(roles).To(gm.ContainElement(&as.Role{Name: "read-write", Privileges: []as.Privilege{{Code: as.ReadWrite, Namespace: "", SetName: ""}}}))
			gm.Expect(roles).To(gm.ContainElement(&as.Role{Name: "read-write-udf", Privileges: []as.Privilege{{Code: as.ReadWriteUDF, Namespace: "", SetName: ""}}}))
			gm.Expect(roles).To(gm.ContainElement(&as.Role{Name: "sys-admin", Privileges: []as.Privilege{{Code: as.SysAdmin, Namespace: "", SetName: ""}}}))
			gm.Expect(roles).To(gm.ContainElement(&as.Role{Name: "user-admin", Privileges: []as.Privilege{{Code: as.UserAdmin, Namespace: "", SetName: ""}}}))
			gm.Expect(roles).To(gm.ContainElement(&as.Role{Name: "data-admin", Privileges: []as.Privilege{{Code: as.DataAdmin, Namespace: "", SetName: ""}}}))

			// Our test Roles
			gm.Expect(roles).To(gm.ContainElement(&as.Role{Name: "role-read-test-test", Privileges: []as.Privilege{{Code: as.Read, Namespace: ns, SetName: "test"}, {Code: as.ReadWrite, Namespace: ns, SetName: "bar"}}, Whitelist: []string{getOutboundIP().String()}}))
			gm.Expect(roles).To(gm.ContainElement(&as.Role{Name: "role-write-test", Privileges: []as.Privilege{{Code: as.ReadWrite, Namespace: ns, SetName: ""}}, Whitelist: []string{getOutboundIP().String()}}))
		})

		gg.It("Must set and query Whitelist for Roles Perfectly", func() {
			defer client.DropRole(nil, "whitelist-test")

			err = client.CreateRole(nil, "whitelist-test", []as.Privilege{{Code: as.Read, Namespace: "", SetName: ""}}, []string{})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			err = client.SetWhitelist(nil, "whitelist-test", []string{getOutboundIP().String()})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			role, err := client.QueryRole(nil, "whitelist-test")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(role).To(gm.Equal(&as.Role{Name: "whitelist-test", Privileges: []as.Privilege{{Code: as.Read, Namespace: "", SetName: ""}}, Whitelist: []string{getOutboundIP().String()}}))
		})

		gg.It("Must query User Roles Perfectly", func() {
			admin, err := client.QueryUser(nil, "test_user")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(admin.User).To(gm.Equal("test_user"))
			gm.Expect(admin.Roles).To(gm.ContainElement("user-admin"))
		})

		gg.It("Must Revoke/Grant Roles Perfectly", func() {
			err := client.GrantRoles(nil, "test_user", []string{"user-admin", "sys-admin", "read-write", "read"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			admin, err := client.QueryUser(nil, "test_user")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(admin.User).To(gm.Equal("test_user"))
			gm.Expect(admin.Roles).To(gm.ConsistOf("user-admin", "sys-admin", "read-write", "read"))

			err = client.RevokeRoles(nil, "test_user", []string{"sys-admin"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			admin, err = client.QueryUser(nil, "test_user")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(admin.User).To(gm.Equal("test_user"))
			gm.Expect(admin.Roles).To(gm.ConsistOf("user-admin", "read-write", "read"))
		})

	}) // describe roles

	gg.Describe("Users", func() {

		gg.It("Must Create/Drop User", func() {
			// drop before test
			client.DropUser(nil, "test_user")

			err := client.CreateUser(nil, "test_user", "test", []string{"user-admin", "read"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			admin, err := client.QueryUser(nil, "test_user")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(admin.User).To(gm.Equal("test_user"))
			gm.Expect(admin.Roles).To(gm.ConsistOf("user-admin", "read"))
		})

		gg.It("Must Change User Password", func() {
			// drop before test
			client.DropUser(nil, "test_user")

			err := client.CreateUser(nil, "test_user", "test", []string{"user-admin", "read"})
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			// connect using the new user
			clientPolicy := as.NewClientPolicy()
			clientPolicy.TlsConfig = tlsConfig
			clientPolicy.User = "test_user"
			clientPolicy.Password = "test"
			new_client, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			defer new_client.Close()

			// change current user's password on the fly
			err = new_client.ChangePassword(nil, "test_user", "test1")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			time.Sleep(time.Second)

			// exhaust all node connections
			for _, node := range new_client.GetNodes() {
				for i := 0; i < clientPolicy.ConnectionQueueSize; i++ {
					conn, err := node.GetConnection(time.Second)
					gm.Expect(err).ToNot(gm.HaveOccurred())
					node.InvalidateConnection(conn)
				}
			}

			// should have the password changed in the cluster, so that a new connection
			// will be established and used
			admin, err := new_client.QueryUser(nil, "test_user")
			gm.Expect(err).ToNot(gm.HaveOccurred())

			gm.Expect(admin.User).To(gm.Equal("test_user"))
			gm.Expect(admin.Roles).To(gm.ConsistOf("user-admin", "read"))
		})

		gg.It("Must Query all users", func() {
			const USER_COUNT = 10
			// drop before test
			for i := 1; i < USER_COUNT; i++ {
				client.DropUser(nil, fmt.Sprintf("test_user%d", i))
			}

			for i := 1; i < USER_COUNT; i++ {
				err := client.CreateUser(nil, fmt.Sprintf("test_user%d", i), "test", []string{"read"})
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

			time.Sleep(time.Second)

			// should have the password changed in the cluster, so that a new connection
			// will be established and used
			users, err := client.QueryUsers(nil)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(len(users)).To(gm.BeNumerically(">=", USER_COUNT-1))

			for i := 1; i < USER_COUNT; i++ {
				err := client.DropUser(nil, fmt.Sprintf("test_user%d", i))
				gm.Expect(err).ToNot(gm.HaveOccurred())
			}

		})

	}) // describe users
})
