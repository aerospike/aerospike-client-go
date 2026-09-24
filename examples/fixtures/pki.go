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

// Fixture factories for the PKI authentication examples.
//
// These examples run against an operator-provisioned cluster: the test users
// must already exist, as the examples themselves document. The fixtures do
// not create users - they check the prerequisites, verify the outcome, and
// undo the two things the examples leave behind: the PKI admin\'s stripped
// roles and the regular user\'s changed password.
//
// They use the runner\'s own connection, so it needs the user-admin role.

package fixtures

import (
	"fmt"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Users the PKI examples rely on. PKI users authenticate by certificate
// common name.
const (
	pkiAdminUser    = "testuser"     // certificate CN, manages other users
	pkiRegularUser  = "testuserpki2" // certificate CN, no admin rights
	internalAdmin   = "testusersec"  // created by the pki_auth example
	internalRegular = "regularuser"  // password changed by pki_auth_roles

	internalAdminPassword = "testpass"
	regularUserPassword   = "regularpass"
)

// userRoles returns the roles currently granted to a user.
func userRoles(username string) ([]string, error) {
	users, err := client.QueryUsers(as.NewAdminPolicy())
	if err != nil {
		return nil, err
	}
	for _, u := range users {
		if u.User == username {
			return u.Roles, nil
		}
	}
	return nil, fmt.Errorf("user %q not found", username)
}

// requireUsers skips the example unless every named user exists. The cluster
// operator provisions them; the examples print the commands to use.
func requireUsers(usernames ...string) error {
	for _, username := range usernames {
		if _, err := userRoles(username); err != nil {
			return Skip(fmt.Sprintf("requires user %q on the cluster", username))
		}
	}
	return nil
}

// restoreUser puts a user back the way the cluster was provisioned: created
// if the example's drop removed it, then granted its roles.
//
// Security metadata propagates asynchronously, so this never drops a user
// first - a late-landing drop would clobber the create that follows it,
// which is exactly how the examples lose their users.
func restoreUser(username, password string, pki bool, roles []string) error {
	adminPolicy := as.NewAdminPolicy()

	// Security metadata propagates asynchronously, so confirm the end state
	// and retry rather than assuming a single attempt took effect.
	for attempt := range 3 {
		if _, err := userRoles(username); err != nil {
			var createErr as.Error
			if pki {
				createErr = client.CreatePKIUser(adminPolicy, username, roles)
			} else {
				createErr = client.CreateUser(adminPolicy, username, password, roles)
			}
			if createErr != nil {
				return fmt.Errorf("restoring user %q: %s", username, createErr)
			}
			if err := waitForUser(username); err != nil {
				return err
			}
		}
		_ = client.GrantRoles(adminPolicy, username, roles)

		if granted, err := userRoles(username); err == nil && len(granted) >= len(roles) {
			return nil
		}
		if attempt < 2 {
			time.Sleep(500 * time.Millisecond)
		}
	}
	return fmt.Errorf("could not restore user %q with roles %v", username, roles)
}

// waitForUser waits until a newly created user is visible cluster-wide.
func waitForUser(username string) error {
	for range 50 {
		if _, err := userRoles(username); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("user %q did not appear after being created", username)
}

// Connect skips unless the PKI admin user is already provisioned, matching
// the prerequisite the PKI examples themselves depend on.
func Connect() Fixture {
	return Fixture{
		Setup: func() error { return requireUsers(pkiAdminUser) },
	}
}

func PKIAuth() Fixture {
	return Fixture{
		// The example authenticates as the PKI admin and then manages users,
		// so that user must exist and hold user-admin.
		Setup: func() error {
			if err := requireUsers(pkiAdminUser); err != nil {
				return err
			}
			roles, err := userRoles(pkiAdminUser)
			if err != nil {
				return err
			}
			for _, role := range roles {
				if role == "user-admin" {
					return nil
				}
			}
			return Skip(fmt.Sprintf("requires user %q to hold the user-admin role", pkiAdminUser))
		},
		// The example recreates both users with read-write.
		Validate: func() error {
			for _, username := range []string{pkiAdminUser, internalAdmin} {
				roles, err := userRoles(username)
				if err != nil {
					return err
				}
				if len(roles) != 1 || roles[0] != "read-write" {
					return fmt.Errorf("user %q has roles %v, want [read-write]", username, roles)
				}
			}
			return nil
		},
		// Remove the user the example created and restore the PKI admin\'s
		// rights, which the example strips - otherwise reruns and the
		// pki_auth_roles example would fail.
		Cleanup: func() error {
			adminRoles := []string{"user-admin", "read-write"}
			if err := restoreUser(pkiAdminUser, "", true, adminRoles); err != nil {
				return err
			}
			return restoreUser(internalAdmin, internalAdminPassword, false, adminRoles)
		},
	}
}

func PKIAuthRoles() Fixture {
	return Fixture{
		// All four users must be provisioned; the example changes their
		// passwords and expects the roles to already be in place.
		Setup: func() error {
			return requireUsers(pkiAdminUser, pkiRegularUser, internalAdmin, internalRegular)
		},
		// The example leaves the regular user with a different password;
		// restore it so the example can run again.
		Cleanup: func() error {
			if err := client.ChangePassword(as.NewAdminPolicy(), internalRegular, regularUserPassword); err != nil {
				return fmt.Errorf("restoring the password of %q: %s", internalRegular, err)
			}
			return nil
		},
	}
}
