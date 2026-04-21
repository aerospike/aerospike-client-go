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
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

// parseObjectBinSerDer parses the bins of a record directly into the
// supplied BinSerDer implementation, avoiding the reflection-based
// parseObject path entirely. It is used by Client.GetObjectBinSerDer
// and is intentionally kept independent of the //go:build !as_performance
// guards so that the zero-reflection read path is available in both
// default and as_performance builds.
func parseObjectBinSerDer(
	brc *baseReadCommand,
	dst BinSerDer,
	opCount int,
	fieldCount int,
	generation uint32,
	expiration uint32,
) Error {
	// Skip record fields (set name etc.); same logic as parseObject.
	if fieldCount > 0 {
		for i := 0; i < fieldCount; i++ {
			fieldSize := int(Buffer.BytesToUint32(brc.dataBuffer, brc.dataOffset))
			brc.dataOffset += 4 + fieldSize
		}
	}

	for i := 0; i < opCount; i++ {
		opSize := int(Buffer.BytesToUint32(brc.dataBuffer, brc.dataOffset))
		particleType := int(brc.dataBuffer[brc.dataOffset+5])
		nameSize := int(brc.dataBuffer[brc.dataOffset+7])
		// Use a string conversion on the name bytes; this matches what
		// parseObject does and is cheaper than maintaining a lookup
		// table on the caller's side. The string allocation is the one
		// unavoidable per-bin allocation on this path — everything else
		// a user's generated UnmarshalBin does is on their own stack.
		name := string(brc.dataBuffer[brc.dataOffset+8 : brc.dataOffset+8+nameSize])
		brc.dataOffset += 4 + 4 + nameSize

		particleBytesSize := opSize - (4 + nameSize)
		value, err := bytesToParticle(particleType, brc.dataBuffer, brc.dataOffset, particleBytesSize)
		if err != nil {
			return err
		}

		if err := dst.UnmarshalBin(name, value); err != nil {
			return err
		}

		brc.dataOffset += particleBytesSize
	}

	// Forward metadata if the user's implementation asked for it.
	if meta, ok := dst.(BinSerDerMeta); ok {
		meta.SetAerospikeMeta(generation, expiration)
	}

	return nil
}
