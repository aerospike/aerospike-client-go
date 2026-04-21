// Copyright 2014-2022 Aerospike, Inc.
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

package aerospike

import (
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// GetObjectBinSerDer reads a record for the specified key and populates
// the provided object in place, using the object's BinSerDer methods.
//
// This is a zero-reflection alternative to GetObject: the client does
// not inspect the object's type via the reflect package. Instead, it
// calls obj.AerospikeBinNames() to determine which bins to request, and
// obj.UnmarshalBin(name, value) once per returned bin.
//
// Typical usage:
//
//	//go:generate go run github.com/aerospike/aerospike-client-go/v8/tools/binserdergen -type Person -out person_binserder.go
//	type Person struct {
//	    Name string `as:"name"`
//	    Age  int    `as:"age"`
//	}
//
//	var p Person
//	if err := client.GetObjectBinSerDer(nil, key, &p); err != nil { ... }
//
// obj must be a non-nil BinSerDer (typically a pointer to a user
// struct).
func (clnt *Client) GetObjectBinSerDer(policy *BasePolicy, key *Key, obj BinSerDer) Error {
	if obj == nil {
		return newError(types.PARAMETER_ERROR, "GetObjectBinSerDer: obj is nil")
	}

	policy = clnt.getUsablePolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareRead(key.namespace); err != nil {
			return err
		}
	}

	binNames := obj.AerospikeBinNames()

	command, err := newReadCommand(clnt.cluster, policy, key, binNames)
	if err != nil {
		return err
	}

	command.binSerDer = obj
	return command.Execute()
}
