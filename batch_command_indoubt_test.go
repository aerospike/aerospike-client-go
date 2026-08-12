// Copyright 2014-2024 Aerospike, Inc.
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
	"iter"
	"time"

	"github.com/aerospike/aerospike-client-go/v8/types"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// CLIENT-5000: a multi-key batch WRITE sub-command that times out (server
// NO_RESPONSE) must propagate the per-record InDoubt flag onto each affected
// write record. These tests build the sub-command in its post-timeout state
// (records still at NO_RESPONSE, as left by prepare()) and drive the in-doubt
// pass through setInDoubt, mirroring what Execute does when execute() fails.
var _ = gg.Describe("Batch multi-key write in-doubt propagation (CLIENT-5000)", func() {

	mkKey := func(idx int) *Key {
		key, err := NewKey("test", "indoubt", idx)
		gm.Expect(err).ToNot(gm.HaveOccurred())
		return key
	}

	// markSucceeded simulates a record that came back OK from another node and
	// must therefore NOT be flagged in-doubt.
	markSucceeded := func(br *BatchRecord) {
		br.ResultCode = types.OK
	}

	gg.Context("batchCommandOperate (mixed read/write batch)", func() {
		gg.It("marks only timed-out write records in-doubt, leaving reads and successes untouched", func() {
			bw0 := NewBatchWrite(nil, mkKey(0), PutOp(NewBin("a", 1))) // timed out, write
			br1 := NewBatchReadHeader(nil, mkKey(1))                   // timed out, read-only
			bw2 := NewBatchWrite(nil, mkKey(2), PutOp(NewBin("a", 1))) // succeeded, write

			markSucceeded(bw2.BatchRec())

			cmd := &batchCommandOperate{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{0, 1, 2}}},
				records:      []BatchRecordIfc{bw0, br1, bw2},
			}

			// Pre-condition: nothing is in-doubt yet.
			gm.Expect(bw0.InDoubt).To(gm.BeFalse())

			cmd.setInDoubt(cmd)

			gm.Expect(bw0.InDoubt).To(gm.BeTrue(), "timed-out write must be in-doubt")
			gm.Expect(br1.InDoubt).To(gm.BeFalse(), "read-only record must never be in-doubt")
			gm.Expect(bw2.InDoubt).To(gm.BeFalse(), "successful write must not be in-doubt")
		})

		gg.It("does nothing when the command was split-retried", func() {
			bw0 := NewBatchWrite(nil, mkKey(0), PutOp(NewBin("a", 1)))

			cmd := &batchCommandOperate{
				batchCommand: batchCommand{
					batch:      &batchNode{offsets: []int{0}},
					splitRetry: true,
				},
				records: []BatchRecordIfc{bw0},
			}

			cmd.setInDoubt(cmd)

			gm.Expect(bw0.InDoubt).To(gm.BeFalse(), "split retry defers in-doubt to subcommands")
		})
	})

	gg.Context("batchCommandUDF", func() {
		gg.It("marks timed-out write records in-doubt and leaves successes untouched", func() {
			records := []*BatchRecord{
				newSimpleBatchRecord(mkKey(0), true), // timed out
				newSimpleBatchRecord(mkKey(1), true), // succeeded
			}
			markSucceeded(records[1])

			cmd := &batchCommandUDF{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{0, 1}}},
				records:      records,
				attr:         &batchAttr{hasWrite: true},
			}

			cmd.setInDoubt(cmd)

			gm.Expect(records[0].InDoubt).To(gm.BeTrue())
			gm.Expect(records[1].InDoubt).To(gm.BeFalse())
		})

		gg.It("does not mark records when the command has no write", func() {
			records := []*BatchRecord{newSimpleBatchRecord(mkKey(0), false)}

			cmd := &batchCommandUDF{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{0}}},
				records:      records,
				attr:         &batchAttr{hasWrite: false},
			}

			cmd.setInDoubt(cmd)

			gm.Expect(records[0].InDoubt).To(gm.BeFalse())
		})
	})

	gg.Context("batchCommandDelete", func() {
		gg.It("marks timed-out delete records in-doubt and leaves successes untouched", func() {
			records := []*BatchRecord{
				newSimpleBatchRecord(mkKey(0), true), // timed out
				newSimpleBatchRecord(mkKey(1), true), // succeeded
			}
			markSucceeded(records[1])

			cmd := &batchCommandDelete{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{0, 1}}},
				records:      records,
				attr:         &batchAttr{hasWrite: true},
			}

			cmd.setInDoubt(cmd)

			gm.Expect(records[0].InDoubt).To(gm.BeTrue())
			gm.Expect(records[1].InDoubt).To(gm.BeFalse())
		})
	})

	gg.Context("batchTxnRollCommand", func() {
		gg.It("marks timed-out write records in-doubt and leaves successes untouched", func() {
			records := []*BatchRecord{
				newSimpleBatchRecord(mkKey(0), true), // timed out
				newSimpleBatchRecord(mkKey(1), true), // succeeded
			}
			markSucceeded(records[1])

			cmd := &batchTxnRollCommand{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{0, 1}}},
				records:      records,
				attr:         &batchAttr{hasWrite: true},
			}

			cmd.setInDoubt(cmd)

			gm.Expect(records[0].InDoubt).To(gm.BeTrue())
			gm.Expect(records[1].InDoubt).To(gm.BeFalse())
		})
	})

	// inDoubt must index the shared, global records slice by offset VALUE, not by
	// slice position. cmd.records is the full batch (every node's records); a
	// subcommand owns only the indices in cmd.batch.offsets. On any node but the
	// first, those offsets are not 0..n-1, so a position-based loop
	// (for i := range offsets) would mark the wrong records. These tests give the
	// subcommand non-contiguous offsets {2,4} into a 5-record slice: only those
	// two may be flagged; a position-based bug would instead hit indices 0 and 1.
	gg.Context("offset-value indexing (multi-node, non-aligned offsets)", func() {
		// writeRecs builds n NO_RESPONSE write records (the post-prepare() state).
		writeRecs := func(n int) []*BatchRecord {
			recs := make([]*BatchRecord, n)
			for i := range recs {
				recs[i] = newSimpleBatchRecord(mkKey(i), true)
			}
			return recs
		}
		assertOnlyOwned := func(get func(int) *BatchRecord, n int, owned ...int) {
			ownedSet := map[int]bool{}
			for _, o := range owned {
				ownedSet[o] = true
				gm.Expect(get(o).InDoubt).To(gm.BeTrue(), "owned offset %d must be in-doubt", o)
			}
			for i := 0; i < n; i++ {
				if !ownedSet[i] {
					gm.Expect(get(i).InDoubt).To(gm.BeFalse(),
						"offset %d is not owned by this subcommand and must stay false", i)
				}
			}
		}

		gg.It("batchCommandOperate marks records[2],[4] only", func() {
			records := []BatchRecordIfc{
				NewBatchWrite(nil, mkKey(0), PutOp(NewBin("a", 1))),
				NewBatchWrite(nil, mkKey(1), PutOp(NewBin("a", 1))),
				NewBatchWrite(nil, mkKey(2), PutOp(NewBin("a", 1))),
				NewBatchWrite(nil, mkKey(3), PutOp(NewBin("a", 1))),
				NewBatchWrite(nil, mkKey(4), PutOp(NewBin("a", 1))),
			}
			cmd := &batchCommandOperate{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{2, 4}}},
				records:      records,
			}
			cmd.setInDoubt(cmd)
			assertOnlyOwned(func(i int) *BatchRecord { return records[i].BatchRec() }, 5, 2, 4)
		})

		gg.It("batchCommandUDF marks records[2],[4] only", func() {
			records := writeRecs(5)
			cmd := &batchCommandUDF{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{2, 4}}},
				records:      records,
				attr:         &batchAttr{hasWrite: true},
			}
			cmd.setInDoubt(cmd)
			assertOnlyOwned(func(i int) *BatchRecord { return records[i] }, 5, 2, 4)
		})

		gg.It("batchCommandDelete marks records[2],[4] only", func() {
			records := writeRecs(5)
			cmd := &batchCommandDelete{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{2, 4}}},
				records:      records,
				attr:         &batchAttr{hasWrite: true},
			}
			cmd.setInDoubt(cmd)
			assertOnlyOwned(func(i int) *BatchRecord { return records[i] }, 5, 2, 4)
		})

		gg.It("batchTxnRollCommand marks records[2],[4] only", func() {
			records := writeRecs(5)
			cmd := &batchTxnRollCommand{
				batchCommand: batchCommand{batch: &batchNode{offsets: []int{2, 4}}},
				records:      records,
				attr:         &batchAttr{hasWrite: true},
			}
			cmd.setInDoubt(cmd)
			assertOnlyOwned(func(i int) *BatchRecord { return records[i] }, 5, 2, 4)
		})
	})

	// CLIENT-5000 bug #1: retryBatch sets the parent's splitRetry=true before
	// cloning subcommands, and cloneBatchCommand is a shallow copy, so each clone
	// inherits splitRetry=true and its own setInDoubt becomes a no-op. clearSplitRetry
	// (called on each clone in retryBatch) restores the Java behavior, where each
	// split subcommand is a fresh object with splitRetry=false.
	gg.Context("split-retry clone inheritance (CLIENT-5000 bug #1)", func() {
		gg.It("clearSplitRetry lets a cloned subcommand own its in-doubt pass", func() {
			records := []*BatchRecord{
				newSimpleBatchRecord(mkKey(0), true),
				newSimpleBatchRecord(mkKey(1), true),
			}
			parent := &batchCommandUDF{
				batchCommand: batchCommand{
					batch:      &batchNode{offsets: []int{0, 1}},
					splitRetry: true, // parent has been split
				},
				records: records,
				attr:    &batchAttr{hasWrite: true},
			}

			// Shallow clone inherits splitRetry=true: its in-doubt pass is suppressed.
			clone := parent.cloneBatchCommand(&batchNode{offsets: []int{0, 1}})
			clone.setInDoubt(clone)
			gm.Expect(records[0].InDoubt).To(gm.BeFalse(), "inherited splitRetry suppresses marking (the bug)")

			// Clearing it (the fix) lets the leaf subcommand mark its own records.
			clone.clearSplitRetry()
			clone.setInDoubt(clone)
			gm.Expect(records[0].InDoubt).To(gm.BeTrue())
			gm.Expect(records[1].InDoubt).To(gm.BeTrue())
		})
	})

	// CLIENT-5000 bug #2: when a split subcommand fails and AllowPartialResults is
	// false, retryBatch bails before the remaining subcommands run. Those un-run
	// records were attempted on a prior iteration and must still be flagged.
	// failFastInDoubt is the extracted decision: mark this command's full set and
	// stop when partial results are not allowed; keep going when they are.
	gg.Context("fail-fast in-doubt on bail (CLIENT-5000 bug #2)", func() {
		newCmd := func(allowPartial bool, recs []*BatchRecord) *batchCommandUDF {
			pol := NewBatchPolicy()
			pol.AllowPartialResults = allowPartial
			return &batchCommandUDF{
				batchCommand: batchCommand{
					batch:  &batchNode{offsets: []int{0, 1}},
					policy: pol,
				},
				records: recs,
				attr:    &batchAttr{hasWrite: true},
			}
		}

		gg.It("marks the full set and signals stop when partial results are not allowed", func() {
			recs := []*BatchRecord{
				newSimpleBatchRecord(mkKey(0), true), // already handled by a subcommand
				newSimpleBatchRecord(mkKey(1), true), // belongs to an un-run subcommand
			}
			recs[0].InDoubt = true // simulate the first subcommand already marked its own

			cmd := newCmd(false, recs)
			stop := cmd.failFastInDoubt(cmd)

			gm.Expect(stop).To(gm.BeTrue(), "fail fast when partial results are not allowed")
			gm.Expect(recs[0].InDoubt).To(gm.BeTrue(), "already-marked record stays marked (idempotent)")
			gm.Expect(recs[1].InDoubt).To(gm.BeTrue(), "un-run subcommand's record must also be in-doubt")
		})

		gg.It("does not mark or stop when partial results are allowed", func() {
			recs := []*BatchRecord{
				newSimpleBatchRecord(mkKey(0), true),
				newSimpleBatchRecord(mkKey(1), true),
			}
			cmd := newCmd(true, recs)
			stop := cmd.failFastInDoubt(cmd)

			gm.Expect(stop).To(gm.BeFalse(), "keep running remaining subcommands")
			gm.Expect(recs[0].InDoubt).To(gm.BeFalse())
			gm.Expect(recs[1].InDoubt).To(gm.BeFalse())
		})
	})
})

// budgetStubCommand drives the executeAt retry loop without any I/O: every
// attempt fails at node acquisition, and the stub counts the attempts.
type budgetStubCommand struct {
	baseCommand
	policy       *BasePolicy
	getNodeCalls int
}

func (c *budgetStubCommand) getPolicy(ifc command) Policy { return c.policy }
func (c *budgetStubCommand) getNode(ifc command) (*Node, Error) {
	c.getNodeCalls++
	return nil, newError(types.INVALID_NODE_ERROR, "stub node failure")
}
func (c *budgetStubCommand) prepareRetry(ifc command, isTimeout bool) bool { return true }
func (c *budgetStubCommand) commandType() commandType                      { return ttNone }
func (c *budgetStubCommand) getNamespaces() iter.Seq2[string, uint64]      { return nil }
func (c *budgetStubCommand) getNamespace() *string                         { return nil }
func (c *budgetStubCommand) putConnection(conn *Connection)                {}
func (c *budgetStubCommand) salvageConn(timeoutDelay time.Duration, conn *Connection, node *Node) {
}
func (c *budgetStubCommand) writeBuffer(ifc command) Error { panic("unreachable") }
func (c *budgetStubCommand) getConnection(policy Policy) (*Connection, Error) {
	panic("unreachable")
}
func (c *budgetStubCommand) parseResult(ifc command, conn *Connection) Error { panic("unreachable") }
func (c *budgetStubCommand) Execute() Error                                  { panic("unreachable") }

// A split batch retry re-executes subcommands through executeIter. The
// subcommands must run under the parent's remaining budget: its deadline (so a
// split retry cannot outlive the caller's TotalTimeout, which executeIter
// previously recomputed from scratch) and its attempt count (so the retries do
// not restart at zero).
var _ = gg.Describe("Split-retry budget inheritance", func() {

	newStub := func(totalTimeout time.Duration, maxRetries int) *budgetStubCommand {
		policy := NewPolicy()
		policy.TotalTimeout = totalTimeout
		policy.MaxRetries = maxRetries
		policy.SleepBetweenRetries = 0
		return &budgetStubCommand{policy: policy}
	}

	gg.It("must honor the parent's already-expired deadline instead of a fresh TotalTimeout", func() {
		stub := newStub(5*time.Second, 3)
		expired := time.Now().Add(-time.Second)

		err := stub.executeIter(stub, 1, expired)

		gm.Expect(err).To(gm.HaveOccurred())
		gm.Expect(err.Matches(types.TIMEOUT)).To(gm.BeTrue(),
			"an inherited expired deadline must surface as a timeout, got: %v", err)
		gm.Expect(err.Matches(types.MAX_RETRIES_EXCEEDED)).To(gm.BeFalse(),
			"the command must not burn its retry budget against an expired deadline")
		gm.Expect(stub.getNodeCalls).To(gm.BeZero(),
			"no attempt may start after the parent's deadline has passed")
	})

	gg.It("must continue the parent's attempt count instead of restarting the budget", func() {
		// A fresh command gets MaxRetries+1 attempts.
		fresh := newStub(0, 3)
		gm.Expect(fresh.executeIter(fresh, 0, time.Time{})).To(gm.HaveOccurred())
		gm.Expect(fresh.getNodeCalls).To(gm.Equal(4))

		// A subcommand that inherits 3 consumed attempts gets only the remainder.
		inherited := newStub(0, 3)
		err := inherited.executeIter(inherited, 3, time.Time{})
		gm.Expect(err.Matches(types.MAX_RETRIES_EXCEEDED)).To(gm.BeTrue())
		gm.Expect(inherited.getNodeCalls).To(gm.Equal(1),
			"3 of the 4 attempts were already spent by the parent")
	})
})
