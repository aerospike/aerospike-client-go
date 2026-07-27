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
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Connect with no credentials: the simplest way to reach an Aerospike
// cluster with no security enabled.
func runConnectBasic() error {
	c, err := as.NewClient(host, port)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Println("Basic connect successful.")
	return nil
}

// Connect with a username and password.
func runConnectAuth() error {
	policy := as.NewClientPolicy()
	policy.User = user
	policy.Password = password

	c, err := as.NewClientWithPolicy(policy, host, port)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Println("Auth connect successful.")
	return nil
}

// Connect over TLS: the seed host carries the TLS server name, and the
// client policy carries the certificate configuration.
func runConnectTLS() error {
	policy := as.NewClientPolicy()
	policy.TlsConfig = tlsConfig

	seed := as.NewHost(host, port)
	seed.TLSName = tlsServerName

	c, err := as.NewClientWithPolicyAndHost(policy, seed)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Println("TLS connect successful.")
	return nil
}

// Connect over TLS using PKI authentication: the client certificate's
// common name is the username, so no password is sent.
func runConnectTLSPKI() error {
	policy := as.NewClientPolicy()
	policy.TlsConfig = tlsConfig
	policy.AuthMode = as.AuthModePKI

	seed := as.NewHost(host, port)
	seed.TLSName = tlsServerName

	c, err := as.NewClientWithPolicyAndHost(policy, seed)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Println("TLS+PKI connect successful.")
	return nil
}
