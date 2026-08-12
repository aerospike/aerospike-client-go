//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
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

package sdk_test

import (
	"flag"
	"math/rand"
	"testing"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// The suite mirrors the core client's flags so `port=3000 make test` and
// `focus='...' make test` work the same way here.
var (
	host        = flag.String("h", "127.0.0.1", "Aerospike server seed host")
	port        = flag.Int("p", 3000, "Aerospike server seed port")
	namespace   = flag.String("n", "test", "Namespace for the tests")
	scNamespace = flag.String("sc-namespace", "",
		"Strong-consistency namespace. Transaction and durability tests self-skip when it is empty.")
	servicesAlternate = flag.Bool("use-services-alternate", false,
		"Use alternate service addresses")
)

var testCluster *sdk.Cluster

// TestSDK is the ginkgo entry point.
func TestSDK(t *testing.T) {
	gm.RegisterFailHandler(gg.Fail)
	gg.RunSpecs(t, "Aerospike Go SDK Suite")
}

var _ = gg.BeforeSuite(func() {
	def := sdk.NewClusterDefinition(*host, *port)
	if *servicesAlternate {
		def = def.UsingServicesAlternate()
	}
	c, err := def.Connect()
	gm.Expect(err).ToNot(gm.HaveOccurred(),
		"cannot connect to the cluster; check -h, -p and -use-services-alternate")
	testCluster = c
})

var _ = gg.AfterSuite(func() {
	if testCluster != nil {
		testCluster.Close()
	}
})

// randomSet gives each spec its own set, so runs are parallel-safe and
// repeatable.
func randomSet() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 16)
	for i := range b {
		b[i] = alphabet[rand.Intn(len(alphabet))]
	}
	return "sdk_" + string(b)
}

// newSession is the common fixture: a session plus a private dataset.
func newSession() (*sdk.Session, *sdk.DataSet) {
	s, err := testCluster.CreateSession(nil)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	ds, err := sdk.DataSetOf(*namespace, randomSet())
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return s, ds
}

// requireSC skips the spec when no strong-consistency namespace was supplied.
func requireSC() {
	if *scNamespace == "" {
		gg.Skip("no -sc-namespace supplied; this spec needs a strong-consistency namespace")
	}
	if !testCluster.SupportsMRT() {
		gg.Skip("cluster does not support multi-record transactions")
	}
}

// scSession is the fixture for strong-consistency specs.
func scSession() (*sdk.Session, *sdk.DataSet) {
	s, err := testCluster.CreateSession(nil)
	gm.Expect(err).ToNot(gm.HaveOccurred())
	ds, err := sdk.DataSetOf(*scNamespace, randomSet())
	gm.Expect(err).ToNot(gm.HaveOccurred())
	return s, ds
}
