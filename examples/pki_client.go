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
	"crypto/x509"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Shared by pki_auth.go and pki_auth_roles.go: building a client authenticated
// either by PKI certificate or by internal username/password. Certificate
// loading (readCertificates/dirFiles) is shared from tls_secure_connection.go.

type testUser struct {
	username string
	password string
	authType as.AuthMode
	roles    []string
}

func connectAs(user testUser, fn func(testUser, []tls.Certificate, *x509.CertPool) *as.ClientPolicy) (*as.Client, error) {
	serverCertPool, clientCertPool, err := readCertificates(*serverCertDir, *clientCertFile, *clientKeyFile)
	if err != nil {
		return nil, err
	}
	clientPolicy := fn(user, clientCertPool, serverCertPool)
	return as.NewClientWithPolicy(clientPolicy, host, port)
}

// pkiClientPolicy authenticates via the client certificate: the
// certificate's common name is the username.
func pkiClientPolicy(user testUser, clientCertPool []tls.Certificate, serverCertPool *x509.CertPool) *as.ClientPolicy {
	clientPolicy := as.NewClientPolicy()
	if len(*tlsName) > 0 || *encryptOnly {
		clientPolicy.TlsConfig = &tls.Config{
			Certificates:             clientCertPool,
			RootCAs:                  serverCertPool,
			InsecureSkipVerify:       *encryptOnly,
			PreferServerCipherSuites: true,
		}
	}
	clientPolicy.AuthMode = user.authType
	return clientPolicy
}

// internalClientPolicy authenticates with username/password over
// the TLS connection.
func internalClientPolicy(user testUser, clientCertPool []tls.Certificate, serverCertPool *x509.CertPool) *as.ClientPolicy {
	clientPolicy := as.NewClientPolicy()
	clientPolicy.User = user.username
	clientPolicy.Password = user.password
	if len(*tlsName) > 0 || *encryptOnly {
		clientPolicy.TlsConfig = &tls.Config{
			Certificates:             clientCertPool,
			RootCAs:                  serverCertPool,
			InsecureSkipVerify:       *encryptOnly,
			PreferServerCipherSuites: true,
		}
	}
	clientPolicy.AuthMode = user.authType
	return clientPolicy
}
