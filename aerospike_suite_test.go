// Copyright 2014-2021 Aerospike, Inc.
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
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	as "github.com/aerospike/aerospike-client-go"
	asl "github.com/aerospike/aerospike-client-go/logger"
	ast "github.com/aerospike/aerospike-client-go/types"

	gg "github.com/onsi/ginkgo"
	gm "github.com/onsi/gomega"
)

var (
	host        = flag.String("h", "127.0.0.1", "Aerospike server seed hostnames or IP addresses")
	port        = flag.Int("p", 3000, "Aerospike server seed hostname or IP address port number.")
	user        = flag.String("U", "", "Username.")
	password    = flag.String("P", "", "Password.")
	authMode    = flag.String("A", "internal", "Authentication mode: internal | external")
	useReplicas = flag.Bool("use-replicas", false, "Aerospike will use replicas as well as master partitions.")
	debug       = flag.Bool("debug", false, "Will set the logging level to DEBUG.")
	namespace   = flag.String("n", "test", "Namespace")

	certFile          = flag.String("cert_file", "", "Certificate file name.")
	keyFile           = flag.String("key_file", "", "Key file name.")
	keyFilePassphrase = flag.String("key_file_passphrase", "", "Key file's pass phrase.")
	nodeTLSName       = flag.String("node_tls_name", "", "Node's TLS name.")
	rootCA            = flag.String("root_ca", "", "Root certificate.")

	tlsConfig    *tls.Config
	clientPolicy *as.ClientPolicy
	client       *as.Client
)

func initTestVars() {
	var buf bytes.Buffer
	logger := log.New(&buf, "", log.LstdFlags|log.Lshortfile)
	logger.SetOutput(os.Stdout)
	asl.Logger.SetLogger(logger)

	if *debug {
		asl.Logger.SetLevel(asl.DEBUG)
	}

	clientPolicy = as.NewClientPolicy()
	if *user != "" {
		clientPolicy.User = *user
		clientPolicy.Password = *password
	}

	*authMode = strings.ToLower(strings.TrimSpace(*authMode))
	if *authMode != "internal" && *authMode != "external" {
		log.Fatalln("Invalid auth mode: only `internal` and `external` values are accepted.")
	}

	if *authMode == "external" {
		clientPolicy.AuthMode = as.AuthModeExternal
	}

	// setup TLS
	tlsConfig = initTLS()
	clientPolicy.TlsConfig = tlsConfig

	dbHost := as.NewHost(*host, *port)
	dbHost.TLSName = *nodeTLSName

	client, err = as.NewClientWithPolicyAndHost(clientPolicy, dbHost)
	if err != nil {
		log.Fatal(err.Error())
	}

	// set default policies
	if *useReplicas {
		client.DefaultPolicy.ReplicaPolicy = as.MASTER_PROLES
	}
}

func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	// setup the client object
	initTestVars()
	os.Exit(m.Run())
}

func TestAerospike(t *testing.T) {
	// TestMain will be called here, no need to do more

	gm.RegisterFailHandler(gg.Fail)
	gg.RunSpecs(t, "Aerospike Client Library Suite")
}

func featureEnabled(feature string) bool {
	node := client.GetNodes()[0]
	infoMap, err := node.RequestInfo(as.NewInfoPolicy(), "features")
	if err != nil {
		log.Fatal("Failed to connect to aerospike: err:", err)
	}

	return strings.Contains(infoMap["features"], feature)
}

func isEnterpriseEdition() bool {
	node := client.GetNodes()[0]
	infoMap, err := node.RequestInfo(as.NewInfoPolicy(), "edition")
	if err != nil {
		log.Fatal("Failed to connect to aerospike: err:", err)
	}

	return strings.Contains(infoMap["edition"], "Enterprise")
}

func securityEnabled() bool {
	if !isEnterpriseEdition() {
		return false
	}

	_, err := client.QueryRoles(nil)
	return err == nil
}

func xdrEnabled() bool {
	res := info(client, "get-config:context=xdr")
	return len(res) > 0 && !strings.HasPrefix(res, "ERROR")
}

func nsInfo(ns string, feature string) string {
	node := client.GetNodes()[0]
	infoMap, err := node.RequestInfo(as.NewInfoPolicy(), "namespace/"+ns)
	if err != nil {
		log.Fatal("Failed to connect to aerospike: err:", err)
	}

	infoStr := infoMap["namespace/"+ns]
	infoPairs := strings.Split(infoStr, ";")
	for _, pairs := range infoPairs {
		pair := strings.Split(pairs, "=")
		if pair[0] == feature {
			return pair[1]
		}
	}

	return ""
}

func info(client *as.Client, feature string) string {
	node := client.GetNodes()[0]
	infoMap, err := node.RequestInfo(as.NewInfoPolicy(), feature)
	if err != nil {
		if ae, ok := err.(ast.AerospikeError); ok {
			return ae.Error()
		} else {
			log.Fatal("Failed to connect to aerospike: err:", err)
		}
	}

	return infoMap[feature]
}

func initTLS() *tls.Config {
	if len(*rootCA) == 0 && len(*certFile) == 0 && len(*keyFile) == 0 {
		return nil
	}

	// Try to load system CA certs, otherwise just make an empty pool
	serverPool, err := x509.SystemCertPool()
	if serverPool == nil || err != nil {
		log.Printf("Adding system certificates to the cert pool failed: %s", err)
		serverPool = x509.NewCertPool()
	}

	if len(*rootCA) > 0 {
		// Try to load system CA certs and add them to the system cert pool
		caCert, err := readFromFile(*rootCA)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Adding CA certificate to the pool...")
		serverPool.AppendCertsFromPEM(caCert)
	}

	var clientPool []tls.Certificate
	if len(*certFile) > 0 || len(*keyFile) > 0 {

		// Read cert file
		certFileBytes, err := readFromFile(*certFile)
		if err != nil {
			log.Fatal(err)
		}

		// Read key file
		keyFileBytes, err := readFromFile(*keyFile)
		if err != nil {
			log.Fatal(err)
		}

		// Decode PEM data
		keyBlock, _ := pem.Decode(keyFileBytes)
		certBlock, _ := pem.Decode(certFileBytes)

		if keyBlock == nil || certBlock == nil {
			log.Fatalf("Failed to decode PEM data for key or certificate")
		}

		// Check and Decrypt the the Key Block using passphrase
		if x509.IsEncryptedPEMBlock(keyBlock) {
			decryptedDERBytes, err := x509.DecryptPEMBlock(keyBlock, []byte(*keyFilePassphrase))
			if err != nil {
				log.Fatalf("Failed to decrypt PEM Block: `%s`", err)
			}

			keyBlock.Bytes = decryptedDERBytes
			keyBlock.Headers = nil
		}

		// Encode PEM data
		keyPEM := pem.EncodeToMemory(keyBlock)
		certPEM := pem.EncodeToMemory(certBlock)

		if keyPEM == nil || certPEM == nil {
			log.Fatalf("Failed to encode PEM data for key or certificate")
		}

		cert, err := tls.X509KeyPair(certPEM, keyPEM)

		if err != nil {
			log.Fatalf("Failed to add client certificate and key to the pool: `%s`", err)
		}

		log.Printf("Adding client certificate and key to the pool...")
		clientPool = append(clientPool, cert)
	}

	tlsConfig := &tls.Config{
		Certificates:             clientPool,
		RootCAs:                  serverPool,
		InsecureSkipVerify:       false,
		PreferServerCipherSuites: true,
	}
	tlsConfig.BuildNameToCertificate()

	return tlsConfig
}

// Read content from file
func readFromFile(filePath string) ([]byte, error) {
	dataBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("Failed to read from file `%s`: `%v`", filePath, err)
	}

	data := bytes.TrimSuffix(dataBytes, []byte("\n"))

	return data, nil
}
