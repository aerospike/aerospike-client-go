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

// ErrorHandler receives per-record failures that were routed away from the
// stream. The index is the row's position in the originating batch: 0 for a
// single-key operation, -1 for a query.
type ErrorHandler func(key *as.Key, index int64, err *Error)

// OnError selects how per-record failures surface.
//
// Build one with [InStream] or [Handler]. The zero value means "use the
// default disposition", which returns single-key failures from the terminal
// and embeds batch failures in the stream.
type OnError struct {
	inStream bool
	handler  ErrorHandler
}

// InStream asks for per-record failures to be embedded as rows, even for a
// single-key operation.
func InStream() *OnError { return &OnError{inStream: true} }

// Handler routes per-record failures to a callback and excludes them from the
// stream.
func Handler(h ErrorHandler) *OnError { return &OnError{handler: h} }

// disposition is the resolved routing for one segment.
type disposition int

const (
	// dispRaise returns the failure from the terminal.
	dispRaise disposition = iota
	// dispInStream embeds the failure as a row.
	dispInStream
	// dispHandler routes the failure to the callback and drops the row.
	dispHandler
)

// resolveDisposition applies the documented precedence. A handler always wins;
// otherwise an explicit in-stream request or a multi-key operation embeds;
// otherwise a single-key failure is raised.
func resolveDisposition(onErr *OnError, single bool) (disposition, ErrorHandler) {
	if onErr != nil && onErr.handler != nil {
		return dispHandler, onErr.handler
	}
	if (onErr != nil && onErr.inStream) || !single {
		return dispInStream, nil
	}
	return dispRaise, nil
}
