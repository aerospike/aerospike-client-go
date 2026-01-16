// Copyright 2012-2025 Aerospike, Inc.
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
	"github.com/aerospike/aerospike-client-go/v8/internal/version"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Query Roles Tests", func() {
	var (
		serverVersion version.Version
		err           error
	)

	// Expected roles by version
	var (
		baseRoles = []string{
			string(as.UserAdmin),
			string(as.SysAdmin),
			string(as.DataAdmin),
			string(as.UDFAdmin),
			string(as.SIndexAdmin),
			string(as.Read),
			string(as.ReadWrite),
			string(as.ReadWriteUDF),
			string(as.Write),
			string(as.Truncate),
		}

		maskingRoles = []string{
			string(as.MaskingAdmin),
			string(as.ReadMasked),
			string(as.WriteMasked),
		}
	)

	gg.BeforeEach(func() {
		// Check if credentials are provided
		if *user == "" || *password == "" {
			gg.Skip("Skipping test: Credentials not provided")
		}

		// Check if security is enabled on this specific server
		_, err = client.QueryRoles(nil)
		if err != nil {
			gg.Skip("Skipping test: Security is not enabled on the server")
		}

		// Get server version
		nodes := client.GetNodes()
		if len(nodes) > 0 {
			serverVersion = nodes[0].GetServerVersion()
		}
	})

	gg.Context("Query Roles", func() {
		gg.It("should query all roles successfully", func() {
			policy := as.NewAdminPolicy()
			roles, err := client.QueryRoles(policy)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(roles).ToNot(gm.BeNil())
			gm.Expect(len(roles)).To(gm.BeNumerically(">", 0))
		})

		gg.It("should have base roles for server version < 8.1.1", func() {
			expectedVersion := version.Version{Major: 8, Minor: 1, Patch: 1, Build: 0}
			if !serverVersion.IsSmaller(&expectedVersion) {
				gg.Skip("Skipping test: Server version is >= 8.1.1")
			}

			policy := as.NewAdminPolicy()
			roles, err := client.QueryRoles(policy)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(roles).ToNot(gm.BeNil())

			// Convert roles to a set of role names
			roleNames := make(map[string]bool)
			for _, role := range roles {
				roleNames[role.Name] = true
			}

			// Check that ALL base roles exist
			missingRoles := []string{}
			for _, expectedRole := range baseRoles {
				if !roleNames[expectedRole] {
					missingRoles = append(missingRoles, expectedRole)
				}
			}
			gm.Expect(missingRoles).To(gm.BeEmpty(), "Missing base roles: %v", missingRoles)

			// Verify each base role individually for clear failure messages
			for _, expectedRole := range baseRoles {
				gm.Expect(roleNames[expectedRole]).To(gm.BeTrue(),
					"Role '%s' should exist for server version < 8.1.1", expectedRole)
			}

			// Check that masking roles do NOT exist
			gm.Expect(roleNames[string(as.MaskingAdmin)]).To(gm.BeFalse(),
				"Masking role '%s' should NOT exist for server version < 8.1.1", as.MaskingAdmin)
			gm.Expect(roleNames[string(as.ReadMasked)]).To(gm.BeFalse(),
				"Masking role '%s' should NOT exist for server version < 8.1.1", as.ReadMasked)
			gm.Expect(roleNames[string(as.WriteMasked)]).To(gm.BeFalse(),
				"Masking role '%s' should NOT exist for server version < 8.1.1", as.WriteMasked)
		})

		gg.It("should have base and masking roles for server version >= 8.1.1", func() {
			expectedVersion := version.Version{Major: 8, Minor: 1, Patch: 1, Build: 0}
			if serverVersion.IsSmaller(&expectedVersion) {
				gg.Skip("Skipping test: Server version is < 8.1.1")
			}

			policy := as.NewAdminPolicy()
			roles, err := client.QueryRoles(policy)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(roles).ToNot(gm.BeNil())

			// Convert roles to a set of role names
			roleNames := make(map[string]bool)
			for _, role := range roles {
				roleNames[role.Name] = true
			}

			// Check that ALL base roles exist
			missingBaseRoles := []string{}
			for _, expectedRole := range baseRoles {
				if !roleNames[expectedRole] {
					missingBaseRoles = append(missingBaseRoles, expectedRole)
				}
			}
			gm.Expect(missingBaseRoles).To(gm.BeEmpty(), "Missing base roles: %v", missingBaseRoles)

			// Check that ALL masking roles exist
			missingMaskingRoles := []string{}
			for _, expectedRole := range maskingRoles {
				if !roleNames[expectedRole] {
					missingMaskingRoles = append(missingMaskingRoles, expectedRole)
				}
			}
			gm.Expect(missingMaskingRoles).To(gm.BeEmpty(), "Missing masking roles: %v", missingMaskingRoles)

			// Verify each base role individually for clear failure messages
			for _, expectedRole := range baseRoles {
				gm.Expect(roleNames[expectedRole]).To(gm.BeTrue(),
					"Role '%s' should exist for server version >= 8.1.1", expectedRole)
			}

			// Verify each masking role individually
			for _, expectedRole := range maskingRoles {
				gm.Expect(roleNames[expectedRole]).To(gm.BeTrue(),
					"Masking role '%s' should exist for server version >= 8.1.1", expectedRole)
			}
		})

		gg.It("should have appropriate roles based on server version", func() {
			policy := as.NewAdminPolicy()
			roles, err := client.QueryRoles(policy)
			gm.Expect(err).ToNot(gm.HaveOccurred())
			gm.Expect(roles).ToNot(gm.BeNil())

			// Convert roles to a set of role names
			roleNames := make(map[string]bool)
			for _, role := range roles {
				roleNames[role.Name] = true
			}

			expectedVersion := version.Version{Major: 8, Minor: 1, Patch: 1, Build: 0}

			if serverVersion.IsGreaterOrEqual(&expectedVersion) {
				// Server version >= 8.1.1: check all roles including masking
				allRoles := append(baseRoles, maskingRoles...)

				missingRoles := []string{}
				for _, expectedRole := range allRoles {
					if !roleNames[expectedRole] {
						missingRoles = append(missingRoles, expectedRole)
					}
				}
				gm.Expect(missingRoles).To(gm.BeEmpty(), "Missing roles for version >= 8.1.1: %v", missingRoles)

				// Verify all roles individually
				for _, expectedRole := range allRoles {
					gm.Expect(roleNames[expectedRole]).To(gm.BeTrue(),
						"Role '%s' should exist for server version %s", expectedRole, serverVersion.String())
				}
			} else {
				// Server version < 8.1.1: check only base roles
				missingRoles := []string{}
				for _, expectedRole := range baseRoles {
					if !roleNames[expectedRole] {
						missingRoles = append(missingRoles, expectedRole)
					}
				}
				gm.Expect(missingRoles).To(gm.BeEmpty(), "Missing base roles for version < 8.1.1: %v", missingRoles)

				// Verify all base roles individually
				for _, expectedRole := range baseRoles {
					gm.Expect(roleNames[expectedRole]).To(gm.BeTrue(),
						"Role '%s' should exist for server version %s", expectedRole, serverVersion.String())
				}

				// Verify masking roles do NOT exist
				unexpectedRoles := []string{}
				for _, maskingRole := range maskingRoles {
					if roleNames[maskingRole] {
						unexpectedRoles = append(unexpectedRoles, maskingRole)
					}
				}
				gm.Expect(unexpectedRoles).To(gm.BeEmpty(), "Unexpected masking roles for version < 8.1.1: %v", unexpectedRoles)
			}
		})
	})
})
