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

package aerospike

import (
	"bytes"
	"fmt"
	"hash"

	"github.com/aerospike/aerospike-client-go/pkg/ripemd160"
	. "github.com/aerospike/aerospike-client-go/types"
	ParticleType "github.com/aerospike/aerospike-client-go/types/particle_type"
	Buffer "github.com/aerospike/aerospike-client-go/utils/buffer"
)

// Unique record identifier. Records can be identified using a specified namespace,
// an optional set name, and a user defined key which must be unique within a set.
// Records can also be identified by namespace/digest which is the combination used
// on the server.
type Key struct {
	// namespace. Equivalent to database name.
	namespace string

	// Optional set name. Equivalent to database table.
	setName string

	// Unique server hash value generated from set name and user key.
	digest []byte

	// Original user key. This key is immediately converted to a hash digest.
	// This key is not used or returned by the server by default. If the user key needs
	// to persist on the server, use one of the following methods:
	//
	// Set "WritePolicy.sendKey" to true. In this case, the key will be sent to the server for storage on writes
	// and retrieved on multi-record scans and queries.
	// Explicitly store and retrieve the key in a bin.
	userKey Value
}

// returns Namespace
func (ky *Key) Namespace() string {
	return ky.namespace
}

// Returns Set name
func (ky *Key) SetName() string {
	return ky.setName
}

// Returns key's value
func (ky *Key) Value() Value {
	return ky.userKey
}

// Returns current key digest
func (ky *Key) Digest() []byte {
	return ky.digest
}

// Uses key digests to compare key equality.
func (ky *Key) Equals(other *Key) bool {
	return bytes.Equal(ky.digest, other.digest)
}

// Return string representation of key.
func (ky *Key) String() string {
	if ky.userKey != nil {
		return fmt.Sprintf("%s:%s:%s:%v", ky.namespace, ky.setName, ky.userKey.String(), Buffer.BytesToHexString(ky.digest))
	}
	return fmt.Sprintf("%s:%s::%v", ky.namespace, ky.setName, Buffer.BytesToHexString(ky.digest))
}

// Initialize key from namespace, optional set name and user key.
// The set name and user defined key are converted to a digest before sending to the server.
// The server handles record identifiers by digest only.
func NewKey(namespace string, setName string, key interface{}) (newKey *Key, err error) {
	newKey = &Key{
		namespace: namespace,
		setName:   setName,
		userKey:   NewValue(key),
	}

	newKey.digest, err = computeDigest(&newKey.setName, NewValue(key))

	return newKey, err
}

// Generate unique server hash value from set name, key type and user defined key.
// The hash function is RIPEMD-160 (a 160 bit hash).
func computeDigest(setName *string, key Value) ([]byte, error) {
	keyType := key.GetType()

	if keyType == ParticleType.NULL {
		return nil, NewAerospikeError(PARAMETER_ERROR, "Invalid key: nil")
	}

	// retrieve hash from hash pool
	h := hashPool.Get().(hash.Hash)
	h.Reset()
	defer hashPool.Put(h)
	// write will not fail; no error checking necessary
	h.Write([]byte(*setName))
	h.Write([]byte{byte(keyType)})
	h.Write(key.getBytes())

	return h.Sum(nil), nil
}

// hash pool
var hashPool *Pool

func init() {
	hashPool = NewPool(1024)
	hashPool.New = func() interface{} {
		return ripemd160.New()
	}
}
