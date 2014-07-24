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
	"fmt"

	Buffer "github.com/citrusleaf/aerospike-client-go/utils/buffer"
)

type Partition struct {
	Namespace   string
	PartitionId int
}

func NewPartitionByKey(key *Key) *Partition {
	return &Partition{
		Namespace: *key.Namespace(),

		// CAN'T USE MOD directly - mod will give negative numbers.
		// First AND makes positive and negative correctly, then mod.
		PartitionId: (Buffer.BytesToIntIntel(key.Digest(), 0) & 0xFFFF) % _PARTITIONS,
	}
}

func NewPartition(namespace string, partitionId int) *Partition {
	return &Partition{
		Namespace:   namespace,
		PartitionId: partitionId,
	}
}

func (this *Partition) String() string {
	return fmt.Sprintf("%s:%d", this.Namespace, this.PartitionId)
}

func (this *Partition) Equals(other *Partition) bool {
	return this.PartitionId == other.PartitionId && this.Namespace == other.Namespace
}
