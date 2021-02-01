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
	"flag"
	"log"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	as "github.com/aerospike/aerospike-client-go"
	asl "github.com/aerospike/aerospike-client-go/logger"
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

	client, err = as.NewClientWithPolicy(clientPolicy, *host, *port)
	if err != nil {
		log.Fatal(err.Error())
	}

	// set default policies
	if *useReplicas {
		client.DefaultPolicy.ReplicaPolicy = as.MASTER_PROLES
	}
}

func TestAerospike(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	// setup the client object
	initTestVars()

	RegisterFailHandler(Fail)
	RunSpecs(t, "Aerospike Client Library Suite")
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
		log.Fatal("Failed to connect to aerospike: err:", err)
	}

	return infoMap[feature]
}
