package aerospike_test

import (
	"flag"
	"math/rand"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	. "github.com/aerospike/aerospike-client-go"
)

var host = flag.String("h", "127.0.0.1", "Aerospike server seed hostnames or IP addresses")
var port = flag.Int("p", 3000, "Aerospike server seed hostname or IP address port number.")
var user = flag.String("U", "", "Username.")
var password = flag.String("P", "", "Password.")
var clientPolicy *ClientPolicy

func initTestVars() {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	clientPolicy = NewClientPolicy()
	if *user != "" {
		clientPolicy.User = *user
		clientPolicy.Password = *password
	}
}

func TestAerospike(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Aerospike Client Library Suite")
}
