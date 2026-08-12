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

package examples_test

import (
	"flag"
	"testing"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/batch"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/behaviorhierarchy"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/cdtpath"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/ecommerce"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/internal/exrun"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/mapremoverange"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/opdifferences"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/queryexamples"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/roster"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/stringops"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/studentscores"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/txnprocessing"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/typedmapping"
	"github.com/aerospike/aerospike-client-go/v8/sdk/examples/yamlconfig"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// The same flags the SDK suite takes, so both are driven identically.
var (
	host        = flag.String("h", "127.0.0.1", "Aerospike server seed host")
	port        = flag.Int("p", 3000, "Aerospike server seed port")
	namespace   = flag.String("n", "test", "Namespace for the examples")
	scNamespace = flag.String("sc-namespace", "",
		"Strong-consistency namespace. Examples needing one skip cleanly without it.")
	servicesAlternate = flag.Bool("use-services-alternate", false,
		"Use alternate service addresses")
)

var testCluster *sdk.Cluster

// TestExamples is the ginkgo entry point.
func TestExamples(t *testing.T) {
	gm.RegisterFailHandler(gg.Fail)
	gg.RunSpecs(t, "Aerospike Go SDK Examples")
}

var _ = gg.BeforeSuite(func() {
	def := sdk.NewClusterDefinition(*host, *port)
	if *servicesAlternate {
		def = def.UsingServicesAlternate()
	}
	c, err := def.Connect()
	gm.Expect(err).ToNot(gm.HaveOccurred(),
		"cannot connect; check -h, -p and -use-services-alternate")
	testCluster = c
})

var _ = gg.AfterSuite(func() {
	if testCluster != nil {
		testCluster.Close()
	}
})

// Every example runs against a live cluster.
//
// Their narration goes to the ginkgo writer, so it surfaces for a failing spec
// or under -v and stays out of the way otherwise.
var _ = gg.Describe("SDK examples", func() {
	env := func() *exrun.Env {
		session, err := testCluster.CreateSession(nil)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return &exrun.Env{
			Session:     session,
			Cluster:     testCluster,
			Namespace:   *namespace,
			SCNamespace: *scNamespace,
			Out:         gg.GinkgoWriter,
		}
	}

	for _, ex := range []struct {
		name string
		run  func(*exrun.Env) error
	}{
		{"batch", batch.Run},
		{"student scores", studentscores.Run},
		{"string operations", stringops.Run},
		{"map remove by key range", mapremoverange.Run},
		{"typed mapping", typedmapping.Run},
		{"behavior hierarchy", behaviorhierarchy.Run},
		{"CDT path expressions", cdtpath.Run},
		{"transaction processing", txnprocessing.Run},
		{"operation differences", opdifferences.Run},
		{"roster", roster.Run},
		{"ecommerce", ecommerce.Run},
		{"query examples", queryexamples.Run},
		{"yaml config", yamlconfig.Run},
	} {
		example := ex
		gg.It("must run the "+example.name+" example", func() {
			gm.Expect(example.run(env())).ToNot(gm.HaveOccurred())
		})
	}
})
