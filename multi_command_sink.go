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

// parseMultiSink decodes one record's bins from a scan / query stream
// and dispatches each bin to the BinReceiver. Mirrors batchParseObject
// in structure but calls the sink interface instead of reflecting into
// a struct, so no reflect package dependency.
func parseMultiSink(
	cmd *baseMultiCommand,
	sink BinReceiver,
	opCount int,
	generation uint32,
	expiration uint32,
) Error {
	if meta, ok := sink.(BinReceiverMetadata); ok {
		if err := meta.AerospikeMetadata(generation, expiration); err != nil {
			return err
		}
	}

	for i := 0; i < opCount; i++ {
		if err := cmd.readBytes(8); err != nil {
			return err
		}
		opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, 0))
		particleType := int(cmd.dataBuffer[5])
		nameSize := int(cmd.dataBuffer[7])

		if err := cmd.readBytes(nameSize); err != nil {
			return err
		}
		name := string(cmd.dataBuffer[:nameSize])

		particleBytesSize := opSize - (4 + nameSize)
		if err := cmd.readBytes(particleBytesSize); err != nil {
			return err
		}
		value, err := bytesToParticleRaw(particleType, cmd.dataBuffer, 0, particleBytesSize, cmd.rawCDT)
		if err != nil {
			return err
		}
		if err := sink.AerospikeBin(name, value); err != nil {
			return err
		}
	}
	return nil
}
