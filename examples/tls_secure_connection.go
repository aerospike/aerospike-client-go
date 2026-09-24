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
	"fmt"
	"log"
	"os"
	"path/filepath"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Connect to a TLS-secured cluster: build certificate pools, configure
// tls.Config on the client policy, and connect.
func runTLSSecureConnection() error {
	serverCertPool, clientCertPool, err := readCertificates(*serverCertDir, *clientCertFile, *clientKeyFile)
	if err != nil {
		return err
	}

	clientPolicy := as.NewClientPolicy()
	clientPolicy.User = user
	clientPolicy.Password = password

	// The seed host carries the TLS name: the client uses it as the TLS
	// server name and verifies the server certificate against it.
	seed := as.NewHost(host, port)
	if len(*tlsName) > 0 || *encryptOnly {
		// Setup TLS Config
		tlsConfig := &tls.Config{
			Certificates:             clientCertPool,
			RootCAs:                  serverCertPool,
			InsecureSkipVerify:       *encryptOnly,
			PreferServerCipherSuites: true,
		}
		clientPolicy.TlsConfig = tlsConfig
		seed.TLSName = *tlsName
	}

	c, err := as.NewClientWithPolicyAndHost(clientPolicy, seed)
	if err != nil {
		return err
	}
	defer c.Close()

	log.Println("Connection successful. Discovered nodes:", c.Cluster().GetNodes())
	return nil
}

// readCertificates builds the server CA pool and the client certificate list
// from the configured files.
func readCertificates(serverCertDir, clientCertFile, clientKeyFile string) (serverPool *x509.CertPool, clientPool []tls.Certificate, err error) {
	if *useSystemCerts {
		// Try to load system CA certs, otherwise just make an empty pool
		serverPool, err = x509.SystemCertPool()
		if serverPool == nil || err != nil {
			log.Printf("Adding system certificates to the pool failed: %s", err)
			serverPool = x509.NewCertPool()
		}
	} else {
		serverPool = x509.NewCertPool()
	}

	// Load server certs from directory.
	// These certificates are used to verify the identity of the server nodes
	// to the client.
	if len(serverCertDir) > 0 {
		serverCerts, err := dirFiles(serverCertDir)
		if err != nil {
			return nil, nil, err
		}
		for _, caFile := range serverCerts {
			// Need to skip the directory itself
			if caFile == serverCertDir {
				continue
			}
			caCert, err := os.ReadFile(caFile)
			if err != nil {
				return nil, nil, fmt.Errorf("adding server certificate %s to the pool failed: %s", caFile, err)
			}
			log.Printf("Adding server certificate %s to the pool...", caFile)
			serverPool.AppendCertsFromPEM(caCert)
		}
	}

	// Load the client certificate.
	// This certificate is used to verify the identity of the client to the
	// server nodes.
	if len(clientCertFile)+len(clientKeyFile) > 0 {
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("adding client certificate %s to the pool failed: %s", clientCertFile, err)
		}
		log.Printf("Adding client certificate %s to the pool...", clientCertFile)
		clientPool = append(clientPool, cert)
	}

	return serverPool, clientPool, nil
}

func dirFiles(root string) (files []string, err error) {
	err = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	return files, err
}
