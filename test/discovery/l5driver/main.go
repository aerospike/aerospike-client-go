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

// Command l5driver is the Go driver of the aerospike-client-conformance driver
// contract (l5-aws/DRIVER.md there), used by the L5, L2 and L3 harnesses. It
// builds a client from the flags, applies the contract's load, and reports
// what the client sees as JSON lines on stdout. It asserts nothing.
package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/logger"
)

const (
	contractVersion = 5
	quitWaitLimit   = 30 * time.Second
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	password, passwordSet := os.LookupEnv("AEROSPIKE_ADMIN_PASSWORD")
	options, err := parseDriverOptions(args, password, passwordSet)
	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
		fmt.Fprintln(os.Stderr, usage)
		return 2
	}

	output := newDriverOutput(os.Stdout, os.Stderr)
	os.Stdout = os.Stderr
	logger.Logger.SetLogger(logForwarder{output: output})
	logger.Logger.SetLevel(logger.WARNING)

	client, err := createClient(options)
	if err != nil {
		message := "client not created: " + err.Error()
		output.writeNow("error", func(int64) line { return line{"message": message} })
		fmt.Fprintln(os.Stderr, "ERROR: "+message)
		return 3
	}
	defer client.Close()

	stats := &loadStats{}
	monitor, err := newRoutingMonitor(client, output, stats, options.namespace)
	if err != nil {
		output.writeNow("error", func(int64) line { return line{"message": err.Error()} })
		fmt.Fprintln(os.Stderr, "ERROR: "+err.Error())
		return 3
	}

	stop := make(chan struct{})
	var background sync.WaitGroup
	var readyOnce sync.Once
	load := &loadRunner{
		client:  client,
		options: options,
		output:  output,
		stats:   stats,
		stop:    stop,
		ready: func() {
			readyOnce.Do(func() {
				output.openWith(readyLine(options.shape))
				background.Add(1)
				go monitor.runPolls(stop, &background)
			})
		},
	}

	background.Add(1)
	go monitor.runReachability(stop, &background)
	load.start()

	commands := &commandRunner{
		options: options,
		output:  output,
		store:   clientRecordStore{client: client, readPolicy: options.readPolicy(), writePolicy: options.writePolicy()},
		stats:   stats,
		monitor: monitor,
	}
	input := bufio.NewScanner(os.Stdin)
	input.Buffer(make([]byte, 64*1024), 1024*1024)
	for input.Scan() {
		if !commands.handle(input.Text()) {
			break
		}
	}

	deadline := time.Now().Add(quitWaitLimit)
	close(stop)
	load.waitStopped(deadline)
	commands.waitForWatch(deadline)
	readyOnce.Do(func() {})
	background.Wait()
	return 0
}

// createClient builds the client, turning a panic during construction into
// an error.
func createClient(options *driverOptions) (client *as.Client, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			client, err = nil, fmt.Errorf("%v", recovered)
		}
	}()
	created, clientErr := as.NewClientWithPolicyAndHost(options.clientPolicy(), options.seed())
	if clientErr != nil {
		if created != nil && created.Cluster() != nil {
			created.Close()
		}
		return nil, clientErr
	}
	return created, nil
}

func readyLine(shape string) func(t int64) line {
	return func(int64) line {
		return line{
			"contract":      contractVersion,
			"epochMs":       time.Now().UnixMilli(),
			"client":        "go",
			"clientVersion": clientVersion(),
			"shape":         shape,
		}
	}
}

// clientVersion is the client module version, with the source revision when
// the build records one.
func clientVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	version := info.Main.Version
	for _, dependency := range info.Deps {
		if dependency.Path == "github.com/aerospike/aerospike-client-go/v8" {
			version = dependency.Version
		}
	}
	for _, setting := range info.Settings {
		if setting.Key == "vcs.revision" && len(setting.Value) >= 12 && !strings.Contains(version, setting.Value[:12]) {
			return version + "+" + setting.Value[:12]
		}
	}
	return version
}
