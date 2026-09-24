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

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Authenticate with PKI (the client certificate carries the identity) and
// with internal username/password auth, creating a user for each mode.
// See pki_client.go for how each client is built.
func runPKIAuth() error {
	log.Println("Creating PKI auth client...")
	testUsers := map[string]testUser{
		"pki":      {username: "testuser", password: "", authType: as.AuthModePKI},
		"internal": {username: "testusersec", password: "testpass", authType: as.AuthModeInternal},
	}

	c, err := connectAs(testUsers["pki"], pkiClientPolicy)
	if err != nil {
		return fmt.Errorf("failed to connect to the server cluster: %s", err)
	}
	log.Println("Connection successful using PKI auth. Discovered nodes:", c.Cluster().GetNodes())

	adminPolicy := as.NewAdminPolicy()

	// Delete users if they already exist
	roles, qerr := c.QueryUsers(adminPolicy)
	if qerr != nil {
		return fmt.Errorf("failed to fetch roles for users: %s", qerr)
	}
	for _, role := range roles {
		for _, user := range testUsers {
			if role.User == user.username {
				c.DropUser(adminPolicy, user.username)
			}
		}
	}

	// Create a PKI user example
	user := testUsers["pki"]
	if err := c.CreatePKIUser(adminPolicy, user.username, []string{"read-write"}); err != nil {
		return fmt.Errorf("failed to create user %s: %s", user.username, err)
	}
	// Fetch users and make sure the PKI user was created
	if roles, err := c.QueryUsers(adminPolicy); err != nil {
		return fmt.Errorf("failed to fetch roles for user %s: %s", user.username, err)
	} else {
		for _, role := range roles {
			if role.User == user.username {
				log.Printf("Role: %s, Permissions: %v", role.User, role.Roles)
			}
		}
	}

	// Create an internal auth user example
	user = testUsers["internal"]
	if err := c.CreateUser(adminPolicy, user.username, user.password, []string{"read-write"}); err != nil {
		return fmt.Errorf("failed to create user %s: %s", user.username, err)
	}
	if roles, err := c.QueryUsers(adminPolicy); err != nil {
		return fmt.Errorf("failed to fetch roles for user %s: %s", user.username, err)
	} else {
		for _, role := range roles {
			if role.User == user.username {
				log.Printf("Role: %s, Permissions: %v", role.User, role.Roles)
			}
		}
	}

	// Closing the client connection
	c.Close()

	log.Println("Creating internal auth client...")
	c, err = connectAs(testUsers["internal"], internalClientPolicy)
	if err != nil {
		return fmt.Errorf("failed to connect to the server cluster: %s", err)
	}
	log.Println("Connection successful using username and password auth. Discovered nodes:", c.Cluster().GetNodes())
	c.Close()

	log.Println("Create PKI finished successfully.")
	return nil
}
