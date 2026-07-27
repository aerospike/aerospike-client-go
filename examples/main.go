/*
 * Copyright 2014-2026 Aerospike, Inc.
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

// Command examples runs the documentation code examples against a live
// Aerospike server and verifies their results.
//
// Run all examples, or selected ones by name:
//
//	go run ./examples all
//	go run ./examples put get
//
// The target cluster is configured with flags (-h, -p, -n, -s, -U, -P) or the
// matching AEROSPIKE_* environment variables. See README.md for details.
package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/examples/fixtures"
)

func main() {
	flag.Usage = usage
	flag.Parse()

	selected, err := selectExamples(flag.Args())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		usage()
		os.Exit(2)
	}

	log.SetOutput(os.Stdout)

	// Assign the ambient state shared by all examples, then connect.
	host = *hostFlag
	port = *portFlag
	user = *userFlag
	password = *passwordFlag
	ns = *namespaceFlag
	set = *setFlag

	log.Printf("Connecting to %s:%d namespace=%s set=%s", host, port, ns, set)

	policy := as.NewClientPolicy()
	policy.User = user
	policy.Password = password
	policy.Timeout = 3 * time.Second

	// Connect over TLS when TLS options are configured, so the whole suite
	// can run against a TLS-secured cluster.
	seed := as.NewHost(host, port)
	if *tlsName != "" || *encryptOnly {
		serverCertPool, clientCertPool, certErr := readCertificates(*serverCertDir, *clientCertFile, *clientKeyFile)
		if certErr != nil {
			log.Fatalf("failed to read certificates: %v", certErr)
		}
		tlsConfig = &tls.Config{
			Certificates:             clientCertPool,
			RootCAs:                  serverCertPool,
			InsecureSkipVerify:       *encryptOnly,
			PreferServerCipherSuites: true,
		}
		tlsServerName = *tlsName
		policy.TlsConfig = tlsConfig
		seed.TLSName = tlsServerName
	}

	client, err = as.NewClientWithPolicyAndHost(policy, seed)
	if err != nil {
		log.Fatalf("failed to connect to Aerospike at %s:%d: %v", host, port, err)
	}
	defer client.Close()

	facts := probeServerFacts()

	// Hand the connection and target to the fixtures package. Strong
	// consistency namespaces forbid non-durable deletes, so fixtures need to
	// know which kind of namespace they are cleaning up.
	fixtures.Init(client, ns, set, facts.strongConsistency)

	results := make([]result, 0, len(selected))
	for _, ex := range selected {
		log.Printf("=== %s", ex.Name)
		results = append(results, execute(ex, facts))
	}

	if printSummary(results) > 0 {
		os.Exit(1)
	}
}

// selectExamples resolves command-line arguments against the registry.
func selectExamples(names []string) ([]Example, error) {
	if len(names) == 0 {
		return nil, fmt.Errorf("no example specified")
	}
	if len(names) == 1 && names[0] == "all" {
		return examples, nil
	}

	byName := make(map[string]Example, len(examples))
	for _, ex := range examples {
		byName[ex.Name] = ex
	}

	selected := make([]Example, 0, len(names))
	for _, name := range names {
		ex, ok := byName[name]
		if !ok {
			return nil, fmt.Errorf("unknown example %q", name)
		}
		selected = append(selected, ex)
	}
	return selected, nil
}

func printSummary(results []result) (failures int) {
	log.Println("--- example run summary ---")
	for _, res := range results {
		line := fmt.Sprintf("%-4s %-20s %v", res.status, res.name, res.elapsed.Round(time.Millisecond))
		if res.detail != "" {
			line += " (" + res.detail + ")"
		}
		log.Println(line)
		if res.status == statusFail {
			failures++
		}
	}
	log.Printf("%d examples: %d failed", len(results), failures)
	return failures
}

func usage() {
	out := flag.CommandLine.Output()
	fmt.Fprintln(out, "Usage: go run ./examples [flags] all|<example>...")
	fmt.Fprintln(out, "\nExamples:")
	for _, ex := range examples {
		fmt.Fprintf(out, "  %s\n", ex.Name)
	}
	fmt.Fprintln(out, "\nFlags:")
	flag.PrintDefaults()
}
