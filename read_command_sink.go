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

package aerospike

import (
	Buffer "github.com/aerospike/aerospike-client-go/v8/utils/buffer"
)

func init() {
	sinkParser = parseSink
}

func parseSink(
	brc *baseReadCommand,
	opCount int,
	fieldCount int,
	generation uint32,
	expiration uint32,
) Error {
	if fieldCount > 0 {
		for i := 0; i < fieldCount; i++ {
			fieldSize := int(Buffer.BytesToUint32(brc.dataBuffer, brc.dataOffset))
			brc.dataOffset += 4 + fieldSize
		}
	}

	sink := brc.sink
	if meta, ok := sink.(BinReceiverMetadata); ok {
		if err := meta.AerospikeMetadata(generation, expiration); err != nil {
			return err
		}
	}

	for i := 0; i < opCount; i++ {
		opSize := int(Buffer.BytesToUint32(brc.dataBuffer, brc.dataOffset))
		particleType := int(brc.dataBuffer[brc.dataOffset+5])
		nameSize := int(brc.dataBuffer[brc.dataOffset+7])
		name := string(brc.dataBuffer[brc.dataOffset+8 : brc.dataOffset+8+nameSize])
		brc.dataOffset += 4 + 4 + nameSize

		particleBytesSize := opSize - (4 + nameSize)
		value, err := bytesToParticle(particleType, brc.dataBuffer, brc.dataOffset, particleBytesSize)
		if err != nil {
			return err
		}
		if err := sink.AerospikeBin(name, value); err != nil {
			return err
		}

		brc.dataOffset += particleBytesSize
	}

	return nil
}
