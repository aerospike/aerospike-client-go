package aerospike_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestAerospike(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Aerospike Suite")
}
