// Copyright 2013-2014 Aerospike, Inc.
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
	"log"
	"os"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go"
)

var host = flag.String("h", "127.0.0.1", "host (default 127.0.0.1)")
var port = flag.Int("p", 3000, "port (default 3000)")
var value = flag.String("v", "", "(fetch single value - default all)")
var sepLines = flag.Bool("l", false, "(print in seperate lines - default false)")
var user = flag.String("U", "", "User.")
var password = flag.String("P", "", "Password.")
var clientPolicy *as.ClientPolicy

func main() {
	flag.Parse()
	log.SetOutput(os.Stdout)
	log.SetFlags(0)

	clientPolicy = as.NewClientPolicy()
	if *user != "" {
		clientPolicy.User = *user
		clientPolicy.Password = *password
	}

	// connect to the host
	if client, err := as.NewClientWithPolicy(clientPolicy, *host, *port); err != nil {
		log.Fatalln(err.Error())
	} else {
		node := client.GetNodes()[0]
		if conn, err := node.GetConnection(time.Second); err != nil {
			log.Fatalln(err.Error())
		} else {
			if infoMap, err := as.RequestInfo(conn, strings.Trim(*value, " ")); err != nil {
				log.Fatalln(err.Error())
			} else {
				cnt := 1
				for k, v := range infoMap {
					log.Printf("%d :  %s\n     %s\n\n", cnt, k, v)
					cnt++
				}
			}
		}
	}
}
