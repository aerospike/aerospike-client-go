//go:build go1.27

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

package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// aelExpression converts Aerospike Expression Language source text into a
// filter expression.
//
// The client does not parse the text: it packs the source and the server
// compiles it, which is why a syntax error surfaces as a server error at
// execution time. The caller has already checked that every node is new enough
// (see chain.prepare).
func aelExpression(src string) (*as.Expression, error) {
	if src == "" {
		return nil, NewError(KindInvalidArgument, "AEL filter text must not be empty")
	}
	return as.ExpAEL(src), nil
}

// executeDatasetQuery runs a set-wide query or scan.
func (c *chain) executeDatasetQuery(specs []operationSpec) (*RecordStream, error) {
	core, err := c.session.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	if len(specs) != 1 {
		return nil, NewError(KindInvalidArgument,
			"a dataset query cannot be combined with other segments")
	}
	spec := specs[0]

	policy, err := ToQueryPolicy(c.settings(OpRead, ShapeQuery))
	if err != nil {
		return nil, err
	}
	if f, err := c.filterExpression(spec); err != nil {
		return nil, err
	} else if f != nil {
		policy.FilterExpression = f
	}
	if c.recordsPerSecond > 0 {
		policy.RecordsPerSecond = c.recordsPerSecond
	}
	if c.txn != nil && !c.txnOptOut {
		policy.Txn = c.txn
	}

	stmt := as.NewStatement(c.dataset.namespace, c.dataset.setName)
	if len(spec.binNames) > 0 {
		stmt.BinNames = spec.binNames
	}
	// A header-only scan is a policy setting, not a projection: an empty bin
	// list on a statement reads everything, and a header read operation returns
	// no rows at all.
	if spec.noBins {
		policy.IncludeBinData = false
	}
	for _, f := range c.filters {
		if err := stmt.SetFilter(f); err != nil {
			return nil, WrapError(err)
		}
	}
	if len(spec.operations) > 0 {
		stmt.Operations = spec.operations
	}

	// A chunked query caps each round at the chunk size and resumes from the
	// partition cursor the previous round left behind.
	limit := c.maxRecords
	if c.chunkSize > 0 {
		policy.MaxRecords = c.chunkSize
	} else if limit > 0 {
		policy.MaxRecords = limit
	}

	pf := c.partitionFilter
	if pf == nil {
		pf = as.NewPartitionFilterAll()
	}

	rs, aerr := core.QueryPartitions(policy, stmt, pf)
	if aerr != nil {
		return nil, WrapError(aerr)
	}

	if c.chunkSize == 0 {
		return newRecordStreamFromRecordset(rs), nil
	}

	// Re-issue the query from the advanced cursor for the next chunk.
	reexec := func() (*as.Recordset, error) {
		if pf.IsDone() {
			return nil, nil
		}
		next, aerr := core.QueryPartitions(policy, stmt, pf)
		if aerr != nil {
			return nil, WrapError(aerr)
		}
		return next, nil
	}
	return newChunkedRecordStream(rs, limit, reexec), nil
}
