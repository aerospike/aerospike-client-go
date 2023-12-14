// Copyright 2014-2022 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

package aerospike

import (
	"bytes"
	"encoding/gob"

	kvs "github.com/aerospike/aerospike-client-go/v7/proto/kvs"
	"github.com/aerospike/aerospike-client-go/v7/types"
)

// PartitionFilter is used in scan/queries. This filter is also used as a cursor.
//
// If a previous scan/query returned all records specified by a PartitionFilter instance, a
// future scan/query using the same PartitionFilter instance will only return new records added
// after the last record read (in digest order) in each partition in the previous scan/query.
type PartitionFilter struct {
	Begin  int
	Count  int
	Digest []byte
	// Partitions encapsulates the cursor for the progress of the scan/query to be used for pagination.
	Partitions []*PartitionStatus
	Done       bool
	Retry      bool
}

// NewPartitionFilterAll creates a partition filter that
// reads all the partitions.
func NewPartitionFilterAll() *PartitionFilter {
	return newPartitionFilter(0, _PARTITIONS)
}

// NewPartitionFilterById creates a partition filter by partition id.
// Partition id is between 0 - 4095
func NewPartitionFilterById(partitionId int) *PartitionFilter {
	return newPartitionFilter(partitionId, 1)
}

// NewPartitionFilterByRange creates a partition filter by partition range.
// begin partition id is between 0 - 4095
// count is the number of partitions, in the range of 1 - 4096 inclusive.
func NewPartitionFilterByRange(begin, count int) *PartitionFilter {
	return newPartitionFilter(begin, count)
}

// NewPartitionFilterByKey creates a partition filter that will return
// records after key's digest in the partition containing the digest.
// Note that digest order is not the same as userKey order. This method
// only works for scan or query with nil filter.
func NewPartitionFilterByKey(key *Key) *PartitionFilter {
	return &PartitionFilter{Begin: key.PartitionId(), Count: 1, Digest: key.Digest()}
}

func newPartitionFilter(begin, count int) *PartitionFilter {
	return &PartitionFilter{Begin: begin, Count: count}
}

// IsDone returns - if using ScanPolicy.MaxRecords or QueryPolicy,MaxRecords -
// if the previous paginated scans with this partition filter instance return all records?
func (pf *PartitionFilter) IsDone() bool {
	return pf.Done
}

// EncodeCursor encodes and returns the cursor for the partition filter.
// This cursor can be persisted and reused later for pagination via PartitionFilter.DecodeCursor.
func (pf *PartitionFilter) EncodeCursor() ([]byte, Error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(pf.Partitions)
	if err != nil {
		return nil, newError(types.PARAMETER_ERROR, err.Error())
	}

	return buf.Bytes(), nil
}

// Decodes and sets the cursor for the partition filter using the output of PartitionFilter.EncodeCursor.
func (pf *PartitionFilter) DecodeCursor(b []byte) Error {
	buf := bytes.NewBuffer(b)
	dec := gob.NewDecoder(buf)

	var parts []*PartitionStatus
	if err := dec.Decode(&parts); err != nil {
		return newError(types.PARSE_ERROR, err.Error())
	}

	pf.Partitions = parts
	return nil
}

func (pf *PartitionFilter) grpc() *kvs.PartitionFilter {
	begin := uint32(pf.Begin)
	ps := make([]*kvs.PartitionStatus, len(pf.Partitions))
	for i := range pf.Partitions {
		ps[i] = pf.Partitions[i].grpc()
	}

	return &kvs.PartitionFilter{
		Begin:             &begin,
		Count:             uint32(pf.Count),
		Digest:            pf.Digest,
		PartitionStatuses: ps,
		Retry:             true,
	}

}
