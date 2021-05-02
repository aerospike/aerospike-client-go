/*
 * Copyright 2014-2021 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not
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
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	as "github.com/aerospike/aerospike-client-go"
)

var host = flag.String("h", "127.0.0.1", "Aerospike server seed hostnames or IP addresses")
var port = flag.Int("p", 3000, "Aerospike server seed hostname or IP address port number.")
var showUsage = flag.Bool("u", false, "Show usage information.")

var tlsName = flag.String("tlsName", "", "Aerospike server TLS name")
var encryptOnly = flag.Bool("encryptOnly", false, "Should the TLS connection be encrypted only without authentication?")

var useSystemCerts = flag.Bool("useSystemCerts", false, "Add system certificates to the RootCA list?")
var serverCertDir = flag.String("serverCertDir", "", "Server certificate dir.")
var clientCertFile = flag.String("clientCertFile", "", "Client Cert File")
var clientKeyFile = flag.String("clientKeyFile", "", "Client Key File")

func printParams() {
	log.Printf("hosts:\t\t%s", *host)
	log.Printf("port:\t\t%d", *port)
}

func main() {
	log.SetOutput(os.Stdout)

	flag.Parse()

	if *showUsage {
		flag.Usage()
		os.Exit(0)
	}

	printParams()
	serverCertPool, clientCertPool := readCertificates(*serverCertDir, *clientCertFile, *clientKeyFile)

	clientPolicy := as.NewClientPolicy()

	if len(*tlsName) > 0 || *encryptOnly == true {
		// Setup TLS Config
		tlsConfig := &tls.Config{
			Certificates:             clientCertPool,
			RootCAs:                  serverCertPool,
			InsecureSkipVerify:       *encryptOnly,
			PreferServerCipherSuites: true,
		}
		tlsConfig.BuildNameToCertificate()

		clientPolicy.TlsConfig = tlsConfig
	}

	client, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		log.Fatalln("Failed to connect to the server cluster: ", err)
	}

	log.Println("Connection successful. Discovered nodes:", client.Cluster().GetNodes())

	log.Println("Example finished successfully.")
}

func readCertificates(serverCertDir string, clientCertFile, clientKeyFile string) (serverPool *x509.CertPool, clientPool []tls.Certificate) {
	var err error

	if *useSystemCerts {
		// Try to load system CA certs, otherwise just make an empty pool
		serverPool, err = x509.SystemCertPool()
		if serverPool == nil || err != nil {
			log.Printf("FAILED: Adding system certificates to the pool failed: %s", err)
			serverPool = x509.NewCertPool()
		}
	}

	// Load server certs from directory
	if len(serverCertDir) > 0 {
		serverCerts := dirFiles(serverCertDir)
		// Adding server certificates to the pool.
		// These certificates are used to verify the identity of the server nodes to the client.
		for _, caFile := range serverCerts {
			caCert, err := ioutil.ReadFile(caFile)
			if err != nil {
				log.Fatalf("FAILED: Adding server certificate %s to the pool failed: %s", caFile, err)
			}

			log.Printf("Adding server certificate %s to the pool...", caFile)
			serverPool.AppendCertsFromPEM(caCert)
		}
	}

	// Try to load client cert
	if len(clientCertFile)+len(clientCertFile) > 0 {
		// Loading the client certificate.
		// This certificate is used to verify the identity of the client to the server nodes.
		cert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
		if err != nil {
			log.Fatalf("FAILED: Adding client certificate %s to the pool failed: %s", clientCertFile, err)
		}

		log.Printf("Adding client certificate %s to the pool...", clientCertFile)
		clientPool = append(clientPool, cert)
	}

	return serverPool, clientPool
}

func dirFiles(root string) (files []string) {
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		files = append(files, path)
		return nil
	})
	if err != nil {
		log.Fatalln(err)
	}

	return files
}
