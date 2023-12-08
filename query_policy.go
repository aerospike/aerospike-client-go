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
	"time"

	kvs "github.com/aerospike/aerospike-client-go/v7/proto/kvs"
)

// QueryPolicy encapsulates parameters for policy attributes used in query operations.
type QueryPolicy struct {
	MultiPolicy

	// ShortQuery detemines wether query expected to return less than 100 records.
	// If true, the server will optimize the query for a small record set.
	// This field is ignored for aggregation queries, background queries
	// and server versions 6.0+.
	//
	// Default: false
	ShortQuery bool
}

// NewQueryPolicy generates a new QueryPolicy instance with default values.
// Set MaxRetries for non-aggregation queries with a nil filter on
// server versions >= 4.9. All other queries are not retried.
//
// The latest servers support retries on individual data partitions.
// This feature is useful when a cluster is migrating and partition(s)
// are missed or incomplete on the first query (with nil filter) attempt.
//
// If the first query attempt misses 2 of 4096 partitions, then only
// those 2 partitions are retried in the next query attempt from the
// last key digest received for each respective partition. A higher
// default MaxRetries is used because it's wasteful to invalidate
// all query results because a single partition was missed.
func NewQueryPolicy() *QueryPolicy {
	return &QueryPolicy{
		MultiPolicy: *NewMultiPolicy(),
	}
}

func (qp *QueryPolicy) grpc() *kvs.QueryPolicy {
	SendKey := qp.SendKey
	TotalTimeout := uint32(qp.TotalTimeout / time.Millisecond)
	RecordQueueSize := uint32(qp.RecordQueueSize)
	MaxConcurrentNodes := uint32(qp.MaxConcurrentNodes)
	IncludeBinData := qp.IncludeBinData
	FailOnClusterChange := false //qp.FailOnClusterChange
	ShortQuery := qp.ShortQuery
	InfoTimeout := uint32(qp.SocketTimeout / time.Millisecond)

	return &kvs.QueryPolicy{
		Replica:             qp.ReplicaPolicy.grpc(),
		ReadModeAP:          qp.ReadModeAP.grpc(),
		ReadModeSC:          qp.ReadModeSC.grpc(),
		SendKey:             &SendKey,
		Compress:            qp.UseCompression,
		Expression:          qp.FilterExpression.grpc(),
		TotalTimeout:        &TotalTimeout,
		MaxConcurrentNodes:  &MaxConcurrentNodes,
		RecordQueueSize:     &RecordQueueSize,
		IncludeBinData:      &IncludeBinData,
		FailOnClusterChange: &FailOnClusterChange,
		ShortQuery:          &ShortQuery,
		InfoTimeout:         &InfoTimeout,
	}
}
