package aerospike_test

import (
	"flag"
	"math/rand"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var host = flag.String("h", "127.0.0.1", "Aerospike server seed hostnames or IP addresses")
var port = flag.Int("p", 3000, "Aerospike server seed hostname or IP address port number.")

func TestAerospike(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	flag.Parse()

	RegisterFailHandler(Fail)
	RunSpecs(t, "Aerospike Client Library Suite")
}
