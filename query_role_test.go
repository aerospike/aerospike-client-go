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
		credentialsProvided bool
		serverVersion       version.Version
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
		credentialsProvided = (*user != "" && *password != "")

		if !credentialsProvided {
			gg.Skip("Skipping test: Credentials not provided")
		}

		// Ensure client is available and security is enabled
		if client == nil {
			gg.Skip("Skipping test: Client not initialized")
		}

		// Check if security is enabled
		_, err := client.QueryRoles(nil)
		if err != nil {
			gg.Skip("Skipping test: Security is not enabled on the server")
		}

		// Get server version
		nodes := client.GetNodes()
		if len(nodes) > 0 {
			serverVersion = nodes[0].GetServerVersion()
		}
	})

	gg.Context("QueryRoles", func() {
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

			// Check that all base roles exist
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

			// Check that all base roles exist
			for _, expectedRole := range baseRoles {
				gm.Expect(roleNames[expectedRole]).To(gm.BeTrue(),
					"Role '%s' should exist for server version >= 8.1.1", expectedRole)
			}

			// Check that masking roles exist
			gm.Expect(roleNames[string(as.MaskingAdmin)]).To(gm.BeTrue(),
				"Masking role '%s' should exist for server version >= 8.1.1", as.MaskingAdmin)
			gm.Expect(roleNames[string(as.ReadMasked)]).To(gm.BeTrue(),
				"Masking role '%s' should exist for server version >= 8.1.1", as.ReadMasked)
			gm.Expect(roleNames[string(as.WriteMasked)]).To(gm.BeTrue(),
				"Masking role '%s' should exist for server version >= 8.1.1", as.WriteMasked)
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
				for _, expectedRole := range allRoles {
					gm.Expect(roleNames[expectedRole]).To(gm.BeTrue(),
						"Role '%s' should exist for server version %s", expectedRole, serverVersion.String())
				}

				// Verify masking roles specifically
				gm.Expect(roleNames[string(as.MaskingAdmin)]).To(gm.BeTrue(),
					"Masking role '%s' should exist for server version >= 8.1.1", as.MaskingAdmin)
				gm.Expect(roleNames[string(as.ReadMasked)]).To(gm.BeTrue(),
					"Masking role '%s' should exist for server version >= 8.1.1", as.ReadMasked)
				gm.Expect(roleNames[string(as.WriteMasked)]).To(gm.BeTrue(),
					"Masking role '%s' should exist for server version >= 8.1.1", as.WriteMasked)
			} else {
				// Server version < 8.1.1: check only base roles
				for _, expectedRole := range baseRoles {
					gm.Expect(roleNames[expectedRole]).To(gm.BeTrue(),
						"Role '%s' should exist for server version %s", expectedRole, serverVersion.String())
				}

				// Verify masking roles do NOT exist
				gm.Expect(roleNames[string(as.MaskingAdmin)]).To(gm.BeFalse(),
					"Masking role '%s' should NOT exist for server version < 8.1.1", as.MaskingAdmin)
				gm.Expect(roleNames[string(as.ReadMasked)]).To(gm.BeFalse(),
					"Masking role '%s' should NOT exist for server version < 8.1.1", as.ReadMasked)
				gm.Expect(roleNames[string(as.WriteMasked)]).To(gm.BeFalse(),
					"Masking role '%s' should NOT exist for server version < 8.1.1", as.WriteMasked)
			}
		})
	})
})
