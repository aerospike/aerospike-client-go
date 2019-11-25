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

var host = flag.String("h", "127.0.0.1", "Aerospike server seed hostnames or IP addresses")
var port = flag.Int("p", 3000, "Aerospike server seed hostname or IP address port number.")
var user = flag.String("U", "", "Username.")
var password = flag.String("P", "", "Password.")
var authMode = flag.String("A", "internal", "Authentication mode: internal | external")
var clientPolicy *as.ClientPolicy
var client *as.Client
var useReplicas = flag.Bool("use-replicas", false, "Aerospike will use replicas as well as master partitions.")
var debug = flag.Bool("debug", false, "Will set the logging level to DEBUG.")

var namespace = flag.String("n", "test", "Namespace")

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
