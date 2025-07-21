/*
 * Copyright 2014-2022 Aerospike, Inc.
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
	"log"
	"os"
	"path/filepath"

	as "github.com/aerospike/aerospike-client-go/v8"
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

type testUser struct {
	username string
	password string
	authType as.AuthMode
}

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
	log.Println("Creating PKI auth client...")

	testUsers := map[string]testUser{
		"pki":      {username: "testuser", password: "", authType: as.AuthModePKI},
		"internal": {username: "testusersec", password: "testpass", authType: as.AuthModeInternal},
	}

	client, err := initializeClient(testUsers["pki"], initializeClientPolicyPKI)
	if err != nil {
		log.Fatalln("Failed to connect to the server cluster: ", err)
	}

	log.Println("Connection successful using PKI auth. Discovered nodes:", client.Cluster().GetNodes())

	adminPolicy := as.NewAdminPolicy()

	// Delete users if they already exist
	if roles, err := client.QueryUsers(adminPolicy); err != nil {
		log.Fatalln("Failed to fetch roles for users")
	} else {
		for _, role := range roles {
			for _, user := range testUsers {
				if role.User == user.username {
					client.DropUser(adminPolicy, user.username)
				}
			}
		}
	}

	user := testUsers["pki"]
	// Create a PKI user example
	if err := client.CreatePKIUser(adminPolicy, user.username, []string{"read-write"}); err != nil {
		log.Fatalln("Failed to create user ", user.username)
	}

	// Fetch users and make sure PKI user was created
	if roles, err := client.QueryUsers(adminPolicy); err != nil {
		log.Fatalln("Failed to fetch roles for user ", user.username)
	} else {
		for _, role := range roles {
			if role.User == user.username {
				log.Printf("Role: %s, Permissions: %v", role.User, role.Roles)
			}
		}
	}

	user = testUsers["internal"]
	// Create a internal auth user example
	if err := client.CreateUser(adminPolicy, user.username, user.password, []string{"read-write"}); err != nil {
		log.Fatalln("Failed to create user ", user.username)
	}

	// Fetch users and make sure PKI user was created
	if roles, err := client.QueryUsers(adminPolicy); err != nil {
		log.Fatalln("Failed to fetch roles for user ", user.username)
	} else {
		for _, role := range roles {
			if role.User == user.username {
				log.Printf("Role: %s, Permissions: %v", role.User, role.Roles)
			}
		}
	}

	// Closing the client connection
	client.Close()

	log.Println("Creating internal auth client...")
	client, err = initializeClient(testUsers["internal"], initializeClientPolicyInternal)
	if err != nil {
		log.Fatalln("Failed to connect to the server cluster: ", err)
	}

	log.Println("Connection successful using username and password auth. Discovered nodes:", client.Cluster().GetNodes())

	client.Close()

	log.Println("Set password for PKI user")
	pkiUserWithPassword := testUser{
		username: testUsers["pki"].username,
		password: "testpass",
		authType: testUsers["pki"].authType,
	}
	client, err = initializeClient(pkiUserWithPassword, initializeClientPolicyInternal)

	if err != nil {
		log.Fatalln("Failed to connect to the server cluster: ", err)
	}

	log.Println("Connection successful using username and password auth. Discovered nodes:", client.Cluster().GetNodes())

	if err = client.ChangePassword(adminPolicy, pkiUserWithPassword.username, "something"); err != nil {
		log.Fatalf("Failed to change password for user %s. Error: %s", pkiUserWithPassword.username, err.Error())
	}

	client.Close()

	log.Println("Create PKI finished successfully.")
}

func initializeClient(user testUser, fn func(testUser, []tls.Certificate, *x509.CertPool) *as.ClientPolicy) (*as.Client, as.Error) {
	serverCertPool, clientCertPool := readCertificates(*serverCertDir, *clientCertFile, *clientKeyFile)

	clientPolicy := fn(user, clientCertPool, serverCertPool)

	client, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	return client, err
}

func initializeClientPolicyPKI(testUser testUser, clientCertPool []tls.Certificate, serverCertPool *x509.CertPool) *as.ClientPolicy {
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

	clientPolicy.AuthMode = testUser.authType
	return clientPolicy
}

func initializeClientPolicyInternal(testUser testUser, clientCertPool []tls.Certificate, serverCertPool *x509.CertPool) *as.ClientPolicy {
	clientPolicy := as.NewClientPolicy()
	clientPolicy.User = testUser.username
	clientPolicy.Password = testUser.password

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

	clientPolicy.AuthMode = testUser.authType
	return clientPolicy
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
	} else {
		serverPool = x509.NewCertPool()
	}

	// Load server certs from directory
	if len(serverCertDir) > 0 {
		serverCerts := dirFiles(serverCertDir)
		// Adding server certificates to the pool.
		// These certificates are used to verify the identity of the server nodes to the client.
		for _, caFile := range serverCerts {
			// Need to skip the directory itself
			if caFile == serverCertDir {
				continue
			}
			caCert, err := os.ReadFile(caFile)
			if err != nil {
				log.Fatalf("FAILED: Adding server certificate %s to the pool failed: %s", caFile, err)
			}

			log.Printf("Adding server certificate %s to the pool...", caFile)
			serverPool.AppendCertsFromPEM(caCert)
		}
	}

	// Try to load client cert
	if len(clientCertFile)+len(clientKeyFile) > 0 {
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
