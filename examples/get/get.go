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

package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"

	as "github.com/aerospike/aerospike-client-go"
)

var (
	host      = "127.0.0.1"
	port      = 3000
	namespace = "test"
	set       = "demo"
)

func main() {

	var err error

	// arguments
	flag.StringVar(&host, "host", host, "Remote host")
	flag.IntVar(&port, "port", port, "Remote port")
	flag.StringVar(&namespace, "namespace", namespace, "Namespace")
	flag.StringVar(&set, "set", set, "Set name")

	// parse flags
	flag.Parse()

	// args
	args := flag.Args()

	if len(args) < 1 {
		printError("Missing argument")
	}

	client, err := as.NewClient(host, port)
	panicOnError(err)

	var key *as.Key

	skey := flag.Arg(0)
	ikey, err := strconv.ParseInt(skey, 10, 64)
	if err == nil {
		key, err = as.NewKey(namespace, set, ikey)
		panicOnError(err)
	} else {
		key, err = as.NewKey(namespace, set, skey)
		panicOnError(err)
	}

	policy := as.NewPolicy()
	rec, err := client.Get(policy, key)
	panicOnError(err)
	if rec != nil {
		printOK("%v", rec.Bins)
	} else {
		printError("record not found: namespace=%s set=%s key=%v", key.Namespace(), key.SetName(), key.Value())
	}
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func printOK(format string, a ...interface{}) {
	fmt.Printf("ok: "+format+"\n", a...)
	os.Exit(0)
}

func printError(format string, a ...interface{}) {
	fmt.Printf("error: "+format+"\n", a...)
	os.Exit(1)
}
