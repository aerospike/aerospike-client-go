/*
 * Copyright 2014-2022 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements.
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
	roles    []string
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
	log.Println("Testing PKI authentication with role-based password management...")

	testUsers := map[string]testUser{
		"pkiAdmin":        {username: "testuser", password: "", authType: as.AuthModePKI, roles: []string{"user-admin", "read-write"}},
		"pkiNonAdmin":     {username: "testuserpki2", password: "", authType: as.AuthModePKI, roles: []string{"read-write"}},
		"internalAdmin":   {username: "testusersec", password: "testpass", authType: as.AuthModeInternal, roles: []string{"user-admin", "read-write"}},
		"internalRegular": {username: "regularuser", password: "regularpass", authType: as.AuthModeInternal, roles: []string{"read-write"}},
	}

	adminPolicy := as.NewAdminPolicy()

	log.Println("Connecting as PKI admin user...")
	pkiAdminClient, err := initializeClient(testUsers["pkiAdmin"], initializeClientPolicyPKI)
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
		log.Println("================================================\n")
		log.Fatalln("Failed to connect to the server cluster: ", err)
	}
	defer pkiAdminClient.Close()

	log.Println("Connection successful. Discovered nodes:", pkiAdminClient.Cluster().GetNodes())

	log.Println("\n=== Test 1: PKI user cannot change password for another PKI user ===")
	if err := pkiAdminClient.ChangePassword(adminPolicy, testUsers["pkiNonAdmin"].username, "anypassword"); err != nil {
		log.Printf("SUCCESS: Server rejected password change for PKI user: %s", err)
	} else {
		log.Fatalln("FAILED: PKI user should not be able to change password for another PKI user")
	}

	log.Println("\n=== Test 2: PKI user cannot change password using their own certificate username ===")
	if err := pkiAdminClient.ChangePassword(adminPolicy, testUsers["pkiAdmin"].username, "anypassword"); err != nil {
		log.Printf("SUCCESS: Server rejected password change for PKI user's own username: %s", err)
	} else {
		log.Fatalln("FAILED: PKI user should not be able to set password for their own username")
	}

	log.Println("\n=== Test 3: PKI user can change password for non-PKI user ===")
	newPassword := "newpassword123"
	if err := pkiAdminClient.ChangePassword(adminPolicy, testUsers["internalRegular"].username, newPassword); err != nil {
		log.Fatalf("FAILED: PKI user should be able to change password for non-PKI user: %s", err)
	}
	log.Printf("SUCCESS: PKI user changed password for non-PKI user '%s'", testUsers["internalRegular"].username)

	log.Println("\n=== Test 4: Non-PKI user can change their own password ===")
	testUserUpdated := testUsers["internalRegular"]
	testUserUpdated.password = newPassword
	internalClient, err := initializeClient(testUserUpdated, initializeClientPolicyInternal)
	if err != nil {
		log.Fatalf("Failed to connect as internal user: %s", err)
	}
	defer internalClient.Close()

	ownNewPassword := "myownpassword789"
	if err := internalClient.ChangePassword(adminPolicy, testUsers["internalRegular"].username, ownNewPassword); err != nil {
		log.Fatalf("FAILED: Non-PKI user should be able to change their own password: %s", err)
	}
	log.Printf("SUCCESS: Non-PKI user '%s' changed their own password", testUsers["internalRegular"].username)

	log.Println("\n=== Test 5: Non-PKI admin user can change password for another non-PKI user ===")
	internalAdminClient, err := initializeClient(testUsers["internalAdmin"], initializeClientPolicyInternal)
	if err != nil {
		log.Fatalf("Failed to connect as internal admin user: %s", err)
	}
	defer internalAdminClient.Close()

	finalPassword := "finalpassword456"
	if err := internalAdminClient.ChangePassword(adminPolicy, testUsers["internalRegular"].username, finalPassword); err != nil {
		log.Fatalf("FAILED: Non-PKI admin user should be able to change password for another non-PKI user: %s", err)
	}
	log.Printf("SUCCESS: Non-PKI admin user changed password for '%s'", testUsers["internalRegular"].username)

	log.Println("\n=== Test 6: Attempting to use password auth with PKI mode (should fail) ===")
	serverCertPool, clientCertPool := readCertificates(*serverCertDir, *clientCertFile, *clientKeyFile)
	badClientPolicy := as.NewClientPolicy()
	badClientPolicy.AuthMode = as.AuthModePKI
	badClientPolicy.User = testUsers["pkiAdmin"].username
	badClientPolicy.Password = "somepassword"

	if len(*tlsName) > 0 || *encryptOnly == true {
		tlsConfig := &tls.Config{
			Certificates:             clientCertPool,
			RootCAs:                  serverCertPool,
			InsecureSkipVerify:       *encryptOnly,
			PreferServerCipherSuites: true,
		}
		tlsConfig.BuildNameToCertificate()
		badClientPolicy.TlsConfig = tlsConfig
	}

	if badClient, err := as.NewClientWithPolicy(badClientPolicy, *host, *port); err != nil {
		log.Printf("SUCCESS: Client correctly rejected PKI mode with password credentials: %s", err)
	} else {
		badClient.Close()
		log.Fatalln("FAILED: Should not be able to use password authentication with PKI mode")
	}

	log.Println("\n=== All password management tests passed ===")
}

func initializeClient(testUser testUser, fn func(testUser, []tls.Certificate, *x509.CertPool) *as.ClientPolicy) (*as.Client, as.Error) {
	serverCertPool, clientCertPool := readCertificates(*serverCertDir, *clientCertFile, *clientKeyFile)

	clientPolicy := fn(testUser, clientCertPool, serverCertPool)

	client, err := as.NewClientWithPolicy(clientPolicy, *host, *port)
	return client, err
}

func initializeClientPolicyPKI(testUser testUser, clientCertPool []tls.Certificate, serverCertPool *x509.CertPool) *as.ClientPolicy {
	clientPolicy := as.NewClientPolicy()

	if len(*tlsName) > 0 || *encryptOnly == true {
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
		serverPool, err = x509.SystemCertPool()
		if serverPool == nil || err != nil {
			log.Printf("FAILED: Adding system certificates to the pool failed: %s", err)
			serverPool = x509.NewCertPool()
		}
	} else {
		serverPool = x509.NewCertPool()
	}

	if len(serverCertDir) > 0 {
		serverCerts := dirFiles(serverCertDir)
		for _, caFile := range serverCerts {
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

	if len(clientCertFile)+len(clientKeyFile) > 0 {
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
