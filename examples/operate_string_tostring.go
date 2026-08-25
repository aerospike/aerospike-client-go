/*
 * Copyright 2026 Aerospike, Inc.
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

// toString — convert any int / float / string / blob bin to its string
// representation. Unlike the other ops, this does not accept a CTX.
// Requires server version 8.1.3 or later.

package main

import (
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

func runOperateStringToString() error {
	key, err := as.NewKey(ns, set, "opstr_tostring")
	if err != nil {
		return err
	}

	const numBin = "n"

	if _, err := client.Delete(nil, key); err != nil {
		return err
	}
	if err := client.PutBins(nil, key, as.NewBin(numBin, 42)); err != nil {
		return err
	}

	r, err := client.Operate(nil, key, as.StrToStringOp(numBin))
	if err != nil {
		return err
	}
	log.Printf(`toString(int 42) = %q`, r.Bins[numBin])
	return nil
}
