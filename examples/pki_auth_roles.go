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
	"crypto/tls"
	"errors"
	"fmt"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Role-based password management with PKI authentication: PKI users have no
// password to change, while admins manage passwords for internal users.
// Requires the test users to be created on the server beforehand.
func runPKIAuthRoles() error {
	log.Println("Testing PKI authentication with role-based password management...")

	testUsers := map[string]testUser{
		"pkiAdmin":        {username: "testuser", password: "", authType: as.AuthModePKI, roles: []string{"user-admin", "read-write"}},
		"pkiNonAdmin":     {username: "testuserpki2", password: "", authType: as.AuthModePKI, roles: []string{"read-write"}},
		"internalAdmin":   {username: "testusersec", password: "testpass", authType: as.AuthModeInternal, roles: []string{"user-admin", "read-write"}},
		"internalRegular": {username: "regularuser", password: "regularpass", authType: as.AuthModeInternal, roles: []string{"read-write"}},
	}
	adminPolicy := as.NewAdminPolicy()

	log.Println("Connecting as PKI admin user...")
	pkiAdminClient, err := connectAs(testUsers["pkiAdmin"], pkiClientPolicy)
	if err != nil {
		log.Println("Make sure to create the users before running the test...")
		log.Println("\nRun these commands on the Aerospike server:")
		log.Println("================================================")
		log.Println("\n# PKI authenticated users (use 'nopassword'):")
		log.Printf("manage acl create user %s password 'nopassword' roles %s\n", testUsers["pkiAdmin"].username, "user-admin,read-write")
		log.Printf("manage acl create user %s password 'nopassword' roles %s\n", testUsers["pkiNonAdmin"].username, "read-write")
		log.Println("\n# Internal authenticated users (use actual passwords):")
		log.Printf("manage acl create user %s password '<password>' roles %s\n", testUsers["internalAdmin"].username, "user-admin,read-write")
		log.Printf("manage acl create user %s password '<password>' roles %s\n", testUsers["internalRegular"].username, "read-write")
		log.Println("\n# Verify users were created:")
		log.Println("manage acl show users")
		log.Println("================================================")
		return fmt.Errorf("failed to connect to the server cluster: %s", err)
	}
	defer pkiAdminClient.Close()
	log.Println("Connection successful. Discovered nodes:", pkiAdminClient.Cluster().GetNodes())

	log.Println("=== Test 1: PKI user cannot change password for another PKI user ===")
	if err := pkiAdminClient.ChangePassword(adminPolicy, testUsers["pkiNonAdmin"].username, "anypassword"); err != nil {
		log.Printf("SUCCESS: Server rejected password change for PKI user: %s", err)
	} else {
		return errors.New("PKI user should not be able to change password for another PKI user")
	}

	log.Println("=== Test 2: PKI user cannot change password using their own certificate username ===")
	if err := pkiAdminClient.ChangePassword(adminPolicy, testUsers["pkiAdmin"].username, "anypassword"); err != nil {
		log.Printf("SUCCESS: Server rejected password change for PKI user's own username: %s", err)
	} else {
		return errors.New("PKI user should not be able to set password for their own username")
	}

	log.Println("=== Test 3: PKI user can change password for non-PKI user ===")
	newPassword := "newpassword123"
	if err := pkiAdminClient.ChangePassword(adminPolicy, testUsers["internalRegular"].username, newPassword); err != nil {
		return fmt.Errorf("PKI user should be able to change password for non-PKI user: %s", err)
	}
	log.Printf("SUCCESS: PKI user changed password for non-PKI user '%s'", testUsers["internalRegular"].username)

	log.Println("=== Test 4: Non-PKI user can change their own password ===")
	testUserUpdated := testUsers["internalRegular"]
	testUserUpdated.password = newPassword
	internalClient, err := connectAs(testUserUpdated, internalClientPolicy)
	if err != nil {
		return fmt.Errorf("failed to connect as internal user: %s", err)
	}
	defer internalClient.Close()
	ownNewPassword := "myownpassword789"
	if err := internalClient.ChangePassword(adminPolicy, testUsers["internalRegular"].username, ownNewPassword); err != nil {
		return fmt.Errorf("non-PKI user should be able to change their own password: %s", err)
	}
	log.Printf("SUCCESS: Non-PKI user '%s' changed their own password", testUsers["internalRegular"].username)

	log.Println("=== Test 5: Non-PKI admin user can change password for another non-PKI user ===")
	internalAdminClient, err := connectAs(testUsers["internalAdmin"], internalClientPolicy)
	if err != nil {
		return fmt.Errorf("failed to connect as internal admin user: %s", err)
	}
	defer internalAdminClient.Close()
	finalPassword := "finalpassword456"
	if err := internalAdminClient.ChangePassword(adminPolicy, testUsers["internalRegular"].username, finalPassword); err != nil {
		return fmt.Errorf("non-PKI admin user should be able to change password for another non-PKI user: %s", err)
	}
	log.Printf("SUCCESS: Non-PKI admin user changed password for '%s'", testUsers["internalRegular"].username)

	log.Println("=== Test 6: Attempting to use password auth with PKI mode (should fail) ===")
	serverCertPool, clientCertPool, err := readCertificates(*serverCertDir, *clientCertFile, *clientKeyFile)
	if err != nil {
		return err
	}
	badClientPolicy := as.NewClientPolicy()
	badClientPolicy.AuthMode = as.AuthModePKI
	badClientPolicy.User = testUsers["pkiAdmin"].username
	badClientPolicy.Password = "somepassword"
	if len(*tlsName) > 0 || *encryptOnly {
		badClientPolicy.TlsConfig = &tls.Config{
			Certificates:             clientCertPool,
			RootCAs:                  serverCertPool,
			InsecureSkipVerify:       *encryptOnly,
			PreferServerCipherSuites: true,
		}
	}
	if badClient, err := as.NewClientWithPolicy(badClientPolicy, host, port); err != nil {
		log.Printf("SUCCESS: Client correctly rejected PKI mode with password credentials: %s", err)
	} else {
		badClient.Close()
		return errors.New("should not be able to use password authentication with PKI mode")
	}

	log.Println("=== All password management tests passed ===")
	return nil
}
