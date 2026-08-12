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

// ExecuteBackgroundTask applies the operations given to
// [QueryBuilder.WithWriteOperations] to every record the query matches.
//
// The builder must target a dataset rather than keys, and at least one write
// operation is required. A background task returns no per-record rows, so
// asking for filtered or missing keys is rejected eagerly.
func (q *QueryBuilder) ExecuteBackgroundTask() (*as.ExecuteTask, error) {
	if len(q.c.writeOperations) == 0 {
		return nil, NewError(KindInvalidArgument,
			"a background task needs at least one write operation: call WithWriteOperations")
	}
	return q.runBackground(q.c.writeOperations)
}

// ExecuteBackgroundDelete deletes every record the query matches.
func (q *QueryBuilder) ExecuteBackgroundDelete() (*as.ExecuteTask, error) {
	if len(q.c.writeOperations) > 0 {
		return nil, NewError(KindInvalidArgument,
			"a background delete injects its own operation; do not set write operations")
	}
	return q.runBackground([]*as.Operation{as.DeleteOp()})
}

// ExecuteBackgroundTouch resets the expiration of every record the query
// matches.
func (q *QueryBuilder) ExecuteBackgroundTouch() (*as.ExecuteTask, error) {
	if len(q.c.writeOperations) > 0 {
		return nil, NewError(KindInvalidArgument,
			"a background touch injects its own operation; do not set write operations")
	}
	return q.runBackground([]*as.Operation{as.TouchOp()})
}

// ExecuteUDFBackgroundTask applies a registered user-defined function to every
// record the query matches.
func (q *QueryBuilder) ExecuteUDFBackgroundTask(pkg, function string, args ...as.Value) (*as.ExecuteTask, error) {
	if len(q.c.writeOperations) > 0 {
		return nil, NewError(KindInvalidArgument,
			"a background user-defined function task must not also set write operations")
	}
	stmt, policy, err := q.backgroundStatement()
	if err != nil {
		return nil, err
	}
	core, err := q.c.session.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	// The core client's background user-defined function path takes no write
	// policy: the function's own writes use the server's defaults.
	task, aerr := core.ExecuteUDF(policy, stmt, pkg, function, args...)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return task, nil
}

// runBackground issues the background write with the given operations.
func (q *QueryBuilder) runBackground(ops []*as.Operation) (*as.ExecuteTask, error) {
	stmt, policy, err := q.backgroundStatement()
	if err != nil {
		return nil, err
	}
	core, err := q.c.session.client.UnderlyingClient()
	if err != nil {
		return nil, err
	}
	writePolicy, err := q.backgroundWritePolicy()
	if err != nil {
		return nil, err
	}
	task, aerr := core.QueryExecute(policy, writePolicy, stmt, ops...)
	if aerr != nil {
		return nil, WrapError(aerr)
	}
	return task, nil
}

// backgroundStatement validates the builder and assembles the statement.
func (q *QueryBuilder) backgroundStatement() (*as.Statement, *as.QueryPolicy, error) {
	c := q.c
	if c.pendingErr != nil {
		return nil, nil, c.pendingErr
	}
	if c.dataset == nil {
		return nil, nil, NewError(KindInvalidArgument,
			"a background task must target a dataset, not keys")
	}
	if c.failOnFilteredOut || c.respondAllKeys {
		return nil, nil, NewError(KindInvalidArgument,
			"a background task returns no per-record rows, so FailOnFilteredOut and IncludeMissingKeys cannot apply")
	}
	if c.finalized {
		return nil, nil, NewError(KindInvalidArgument, "this chain has already been executed")
	}
	c.finalized = true

	if c.usesAEL() && !c.session.client.SupportsServerCompiledAEL() {
		return nil, nil, NewError(KindInvalidArgument,
			"Aerospike Expression Language filter text requires server 8.1.3 or newer on every node")
	}

	specs := c.allSpecs()
	spec := operationSpec{}
	if len(specs) > 0 {
		spec = specs[0]
	}

	policy, err := ToQueryPolicy(c.settings(OpRead, ShapeQuery))
	if err != nil {
		return nil, nil, err
	}
	if f, err := c.filterExpression(spec); err != nil {
		return nil, nil, err
	} else if f != nil {
		policy.FilterExpression = f
	}
	if c.recordsPerSecond > 0 {
		policy.RecordsPerSecond = c.recordsPerSecond
	}

	stmt := as.NewStatement(c.dataset.namespace, c.dataset.setName)
	for _, f := range c.filters {
		if err := stmt.SetFilter(f); err != nil {
			return nil, nil, WrapError(err)
		}
	}
	return stmt, policy, nil
}

// backgroundWritePolicy assembles the write policy the background task applies.
func (q *QueryBuilder) backgroundWritePolicy() (*as.WritePolicy, error) {
	c := q.c
	settings := c.settings(OpWriteNonRetryable, ShapeQuery)
	p, err := ToWritePolicy(settings)
	if err != nil {
		return nil, err
	}
	ttl := c.defaultTTLSeconds
	for _, s := range c.specs {
		if s.ttlSeconds != nil {
			ttl = s.ttlSeconds
			break
		}
	}
	p.Expiration = expirationFor(ttl)

	var override, commandDefault *bool
	for _, s := range c.specs {
		if s.durableDelete != nil {
			override = s.durableDelete
		}
		if s.durableDeleteDefault != nil {
			commandDefault = s.durableDeleteDefault
		}
	}
	p.DurableDelete = ResolveDurableDelete(settings.DurableDelete, commandDefault, override)
	return p, nil
}
