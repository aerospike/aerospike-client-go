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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

type failingBlob struct{}

func (failingBlob) EncodeBlob() ([]byte, error) { return nil, errors.New("encode failed") }

// A bin value that cannot be encoded must fail the command with an error and
// close the connection it took from the pool, not panic and leak it.
func TestExecuteAtClosesConnectionWhenBinValueCannotBeEncoded(t *testing.T) {
	cases := []struct {
		name  string
		value any
		code  types.ResultCode
	}{
		{"unsupported type", struct{}{}, types.TYPE_NOT_SUPPORTED},
		{"failing blob", failingBlob{}, types.SERIALIZE_ERROR},
		{"uint64 overflowing int64", uint64(1 << 63), types.PARAMETER_ERROR},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := newBufferProbeCommand(t, "", false)
			cmd.binMap = BinMap{"bin": tc.value}
			conn := cmd.fakeConn
			conn.node = cmd.node
			cmd.node.connectionCount.Set(1)

			var err Error
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("executeAt panicked: %v", r)
					}
				}()
				err = cmd.executeAt(cmd, cmd.policy.GetBasePolicy(), time.Now().Add(time.Minute), 1)
			}()

			if err == nil || !err.Matches(tc.code) {
				t.Fatalf("executeAt got %v, want %v", err, tc.code)
			}
			if err.IsInDoubt() {
				t.Error("error is in doubt although nothing was sent")
			}
			if cmd.connPutBack {
				t.Error("connection was returned to the pool")
			}
			if conn.conn != nil {
				t.Error("connection was not closed")
			}
			if got := cmd.node.connectionCount.Get(); got != 0 {
				t.Errorf("node connection count got %d, want 0", got)
			}
		})
	}
}

func TestNewBinWithFailingBlobDoesNotPanic(t *testing.T) {
	bin := NewBin("bin", failingBlob{})
	if _, err := bin.Value.EstimateSize(); err == nil || !err.Matches(types.SERIALIZE_ERROR) {
		t.Fatalf("EstimateSize got %v, want SERIALIZE_ERROR", err)
	}
}

// A Value shared by concurrent commands, such as a query filter sent to every
// node, must hand each command its own error to annotate.
func TestSharedInvalidValueReturnsDistinctErrors(t *testing.T) {
	shared := NewValue(uint64(1 << 63))

	errs := make([]Error, 2)
	var wg sync.WaitGroup
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cmd := newBufferProbeCommand(t, "", false)
			cmd.binMap = BinMap{"bin": shared}
			errs[i] = cmd.executeAt(cmd, cmd.policy.GetBasePolicy(), time.Now().Add(time.Minute), 1)
		}(i)
	}
	wg.Wait()

	if errs[0] == nil || errs[1] == nil {
		t.Fatalf("executeAt got %v and %v, want errors", errs[0], errs[1])
	}
	if errs[0] == errs[1] {
		t.Error("concurrent commands shared one error instance")
	}
}

func TestNewKeyWithDigestRejectsUint64OverflowingInt64(t *testing.T) {
	_, err := NewKeyWithDigest("test", "set", uint64(1<<63), make([]byte, 20))
	if err == nil || !err.Matches(types.PARAMETER_ERROR) {
		t.Fatalf("NewKeyWithDigest got %v, want PARAMETER_ERROR", err)
	}
}
