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
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// UDFFunctionBuilder is the first state of a user-defined function segment: it
// only accepts the function to call.
type UDFFunctionBuilder struct {
	c *chain
}

// Function names the module and function to call. The package name omits the
// ".lua" suffix.
func (b *UDFFunctionBuilder) Function(pkg, function string) *UDFBuilder {
	if b.c.requireCurrent("Function") {
		b.c.current.udfPackage = pkg
		b.c.current.udfFunction = function
	}
	return &UDFBuilder{c: b.c}
}

// UDFBuilder builds a user-defined function segment.
type UDFBuilder struct {
	c *chain
}

// Passing supplies the function arguments.
func (b *UDFBuilder) Passing(args ...as.Value) *UDFBuilder {
	if b.c.requireCurrent("Passing") {
		b.c.current.udfArgs = args
	}
	return b
}

// Where sets a server-side filter for this segment.
func (b *UDFBuilder) Where[P Predicate](pred P) *UDFBuilder {
	p, err := resolvePredicate(pred)
	if err != nil {
		b.c.deferErr(err)
		return b
	}
	if b.c.requireCurrent("Where") {
		b.c.current.filter = p
	}
	return b
}

// IncludeMissingKeys emits rows for keys that do not exist.
func (b *UDFBuilder) IncludeMissingKeys() *UDFBuilder {
	b.c.respondAllKeys = true
	return b
}

// FailOnFilteredOut reports filtered records instead of skipping them.
func (b *UDFBuilder) FailOnFilteredOut() *UDFBuilder {
	b.c.failOnFilteredOut = true
	return b
}

// ExpireRecordAfterSeconds sets the expiration the function's writes apply.
func (b *UDFBuilder) ExpireRecordAfterSeconds(seconds int64) *UDFBuilder {
	b.c.setTTLSeconds(seconds)
	return b
}

// ExpireRecordAfter sets the expiration from a duration.
func (b *UDFBuilder) ExpireRecordAfter(d time.Duration) *UDFBuilder {
	return b.ExpireRecordAfterSeconds(int64(d / time.Second))
}

// NeverExpire keeps the written records forever.
func (b *UDFBuilder) NeverExpire() *UDFBuilder {
	b.c.setTTLSeconds(ttlNeverExpire)
	return b
}

// WithNoChangeInExpiration leaves expirations untouched.
func (b *UDFBuilder) WithNoChangeInExpiration() *UDFBuilder {
	b.c.setTTLSeconds(ttlDontUpdate)
	return b
}

// Query finalizes this segment and starts a read segment.
func (b *UDFBuilder) Query[T QueryTarget](target T) *QueryBuilder {
	return chainQuery(b.c, target)
}

// Upsert finalizes this segment and starts an upsert segment.
func (b *UDFBuilder) Upsert[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opUpsert, target)
}

// Delete finalizes this segment and starts a delete segment.
func (b *UDFBuilder) Delete[T WriteTarget](target T) *WriteSegmentBuilder {
	return chainWrite(b.c, opDelete, target)
}

// ExecuteUDF finalizes this segment and starts another function segment.
func (b *UDFBuilder) ExecuteUDF[T WriteTarget](target T) *UDFFunctionBuilder {
	return chainUDF(b.c, target)
}

// Execute runs the chain.
func (b *UDFBuilder) Execute() (*RecordStream, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	return b.c.execute(nil)
}

// ExecuteOnError runs the chain with an explicit error disposition.
func (b *UDFBuilder) ExecuteOnError(onErr *OnError) (*RecordStream, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	return b.c.execute(onErr)
}

// validate checks that a function was named.
func (b *UDFBuilder) validate() error {
	if b.c.hasCurrent && b.c.current.udfFunction == "" {
		return NewError(KindInvalidArgument, "ExecuteUDF requires Function to be called")
	}
	return nil
}

// chainUDF finalizes the current segment and opens a function segment.
func chainUDF[T WriteTarget](c *chain, target T) *UDFFunctionBuilder {
	keys, single, err := resolveWriteTarget(target)
	if err != nil {
		c.deferErr(err)
		c.startSegment(opUDF, nil, false)
		return &UDFFunctionBuilder{c: c}
	}
	c.startSegment(opUDF, keys, single)
	return &UDFFunctionBuilder{c: c}
}

// ExecuteUDF opens a user-defined function segment on a new chain.
func (s *Session) ExecuteUDF[T WriteTarget](target T) *UDFFunctionBuilder {
	return chainUDF(newChain(s), target)
}
