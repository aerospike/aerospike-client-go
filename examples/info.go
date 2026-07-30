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

package main

import (
	"log"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// Request the server's full info map over a raw node connection.
func runInfo() error {
	connectionPolicy := as.NewClientPolicy()
	connectionPolicy.Timeout = 10 * time.Second
	connectionPolicy.User = user
	connectionPolicy.Password = password

	// A TLS-secured cluster needs the TLS settings on the policy, and the
	// certificate name on the host.
	connectionPolicy.TlsConfig = tlsConfig
	seed := as.NewHost(host, port)
	seed.TLSName = tlsServerName

	conn, err := as.NewConnection(connectionPolicy, seed)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Unlike a client, a raw connection does not authenticate on its own.
	if connectionPolicy.RequiresAuthentication() {
		if err := conn.Login(connectionPolicy); err != nil {
			return err
		}
	}

	// An empty command requests every info key the server knows.
	infoMap, err := conn.RequestInfo("")
	if err != nil {
		return err
	}

	cnt := 1
	for k, v := range infoMap {
		log.Printf("%d :  %s\n     %s", cnt, k, v)
		cnt++
	}
	return nil
}
