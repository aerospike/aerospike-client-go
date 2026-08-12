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

// The write verbs. Each opens a [WriteSegmentBuilder] on a new chain.
//
// Every verb is a generic method over [WriteTarget], so one method accepts
// either a single key or a slice of keys -- the Go spelling of the overload
// set the other Aerospike SDKs expose. Argument errors are deferred to the
// terminal, so the chain itself never returns an error.

// Upsert creates or updates the addressed records (the server default).
func (s *Session) Upsert[T WriteTarget](target T) *WriteSegmentBuilder {
	return startWrite(s, opUpsert, target)
}

// Insert creates records, failing with RecordExists when one is present.
func (s *Session) Insert[T WriteTarget](target T) *WriteSegmentBuilder {
	return startWrite(s, opInsert, target)
}

// Update updates existing records, failing with RecordNotFound when absent.
func (s *Session) Update[T WriteTarget](target T) *WriteSegmentBuilder {
	return startWrite(s, opUpdate, target)
}

// Replace creates or replaces records; bins that are not written are removed.
func (s *Session) Replace[T WriteTarget](target T) *WriteSegmentBuilder {
	return startWrite(s, opReplace, target)
}

// ReplaceIfExists replaces existing records, failing when absent.
func (s *Session) ReplaceIfExists[T WriteTarget](target T) *WriteSegmentBuilder {
	return startWrite(s, opReplaceIfExists, target)
}

// Delete removes the addressed records.
func (s *Session) Delete[T WriteTarget](target T) *WriteSegmentBuilder {
	return startWrite(s, opDelete, target)
}

// Touch resets the expiration and bumps the generation.
func (s *Session) Touch[T WriteTarget](target T) *WriteSegmentBuilder {
	return startWrite(s, opTouch, target)
}

// Exists checks for the presence of the addressed records. Read the outcome
// with [RecordResult.AsBool].
func (s *Session) Exists[T WriteTarget](target T) *WriteSegmentBuilder {
	return startWrite(s, opExists, target)
}

// startWrite opens a chain on a write verb.
func startWrite[T WriteTarget](s *Session, verb opType, target T) *WriteSegmentBuilder {
	c := newChain(s)
	keys, single, err := resolveWriteTarget(target)
	if err != nil {
		c.deferErr(err)
		c.startSegment(verb, nil, false)
		return &WriteSegmentBuilder{c: c}
	}
	c.startSegment(verb, keys, single)
	return &WriteSegmentBuilder{c: c}
}

// Query opens a read.
//
// The target is a single key (a point read), a slice of keys (a batch read),
// or a dataset (a set-wide index query or scan) -- one method for all three,
// mirroring the other SDKs' `Into<QueryTarget>`.
func (s *Session) Query[T QueryTarget](target T) *QueryBuilder {
	c := newChain(s)
	resolved, err := resolveQueryTarget(target)
	if err != nil {
		c.deferErr(err)
		c.startSegment(opRead, nil, false)
		return &QueryBuilder{c: c}
	}
	if resolved.isDataset() {
		c.dataset = resolved.dataset
		c.startSegment(opRead, nil, false)
	} else {
		c.startSegment(opRead, resolved.keys, resolved.single)
	}
	return &QueryBuilder{c: c}
}
