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
	"code.google.com/p/go.crypto/ripemd160"

	// . "github.com/citrusleaf/aerospike-client-go/logger"
	. "github.com/citrusleaf/aerospike-client-go/types"
	ParticleType "github.com/citrusleaf/aerospike-client-go/types/particle_type"
)

// Unique record identifier. Records can be identified using a specified namespace,
// an optional set name, and a user defined key which must be unique within a set.
// Records can also be identified by namespace/digest which is the combination used
// on the server.
type Key struct {
	// namespace. Equivalent to database name.
	namespace *string

	// Optional set name. Equivalent to database table.
	setName *string

	// Unique server hash value generated from set name and user key.
	digest []byte

	// Original user key. This key is immediately converted to a hash digest.
	// This key is not used or returned by the server.  If the user key needs
	// to persist on the server, the key should be explicitly stored in a bin.
	userKey interface{}
}

// returns Namespace
func (this *Key) Namespace() *string {
	namespace := *this.namespace
	return &namespace
}

// Returns Set name
func (this *Key) SetName() *string {
	setName := *this.setName
	return &setName
}

// Returns current key digest
func (this *Key) Digest() []byte {
	digest := make([]byte, len(this.digest))
	copy(digest, this.digest)
	return digest
}

func (this *Key) Equals(other *Key) bool {
	return (*this.namespace == *other.namespace) && bytes.Equal(this.digest, other.digest)
}

// Initialize key from namespace, optional set name and user key.
// The set name and user defined key are converted to a digest before sending to the server.
// The server handles record identifiers by digest only.
func NewKey(namespace string, setName string, key interface{}) (*Key, error) {
	newKey := &Key{
		namespace: &namespace,
		setName:   &setName,
		userKey:   key,
		digest:    make([]byte, 20),
	}

	var err error
	newKey.digest, err = ComputeDigest(*newKey.setName, NewValue(key))

	return newKey, err
}

// Initialize key from namespace, digest and optional set name.
func NewKeyByDigest(namespace string, setName string, digest [20]byte) *Key {
	return &Key{
		namespace: &namespace,
		setName:   &setName,
		digest:    digest[:],
	}
}

// Generate unique server hash value from set name, key type and user defined key.
// The hash function is RIPEMD-160 (a 160 bit hash).
func ComputeDigest(setName string, key Value) ([]byte, error) {
	keyType := key.GetType()

	if keyType == ParticleType.NULL {
		return nil, NewAerospikeError(PARAMETER_ERROR, "Invalid key: nil")
	}
	// TODO: This method must be optimized
	buffer := make([]byte, len(setName)+key.EstimateSize()+1)
	copy(buffer[0:], []byte(setName))
	buffer[len(setName)] = byte(keyType)
	key.Write(buffer, len(setName)+1)

	hash := ripemd160.New()
	hash.Write(buffer)

	return hash.Sum(nil), nil
}
