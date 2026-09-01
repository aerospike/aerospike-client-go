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
	batchSinkParser = parseBatchSink
}

func parseBatchSink(
	cmd batchSinkParserIfc,
	offset int,
	opCount int,
	fieldCount int,
	generation uint32,
	expiration uint32,
) Error {
	sink := cmd.sink(offset)
	if sink == nil {
		// No sink registered for this offset — skip the bin payload so the
		// stream position stays correct for subsequent records.
		return drainBins(cmd, opCount)
	}

	if meta, ok := sink.(BinReceiverMetadata); ok {
		if err := meta.AerospikeMetadata(generation, expiration); err != nil {
			return err
		}
	}

	for i := 0; i < opCount; i++ {
		if err := cmd.readBytes(8); err != nil {
			return err
		}
		opSize := int(Buffer.BytesToUint32(cmd.buf(), 0))
		particleType := int(cmd.buf()[5])
		nameSize := int(cmd.buf()[7])

		if err := cmd.readBytes(nameSize); err != nil {
			return err
		}
		name := string(cmd.buf()[:nameSize])

		particleBytesSize := opSize - (4 + nameSize)
		if err := cmd.readBytes(particleBytesSize); err != nil {
			return err
		}
		value, err := bytesToParticle(particleType, cmd.buf(), 0, particleBytesSize)
		if err != nil {
			return err
		}
		if err := sink.AerospikeBin(name, value); err != nil {
			return err
		}
	}
	return nil
}

func drainBins(cmd batchSinkParserIfc, opCount int) Error {
	for i := 0; i < opCount; i++ {
		if err := cmd.readBytes(8); err != nil {
			return err
		}
		opSize := int(Buffer.BytesToUint32(cmd.buf(), 0))
		nameSize := int(cmd.buf()[7])
		if err := cmd.readBytes(nameSize); err != nil {
			return err
		}
		particleBytesSize := opSize - (4 + nameSize)
		if err := cmd.readBytes(particleBytesSize); err != nil {
			return err
		}
	}
	return nil
}
