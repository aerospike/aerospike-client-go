package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// WriteOp is one entry in a BatchWrite([]WriteOp) heterogeneous batch
// (D-10) — batch and transaction deliberately do not share one fluent
// shape.
type WriteOp struct{}

func UpsertOp(keys ...*as.Key) WriteOp {
	return WriteOp{}
}

func DeleteOp(keys ...*as.Key) WriteOp {
	return WriteOp{}
}

// TouchOp is named explicitly in the PRD's own §10.5 table ("Touch builder
// ... multi-key → TouchOp in BatchWrite") but was missing here — every
// other op constructor it names (UpsertOp, DeleteOp, UpdateOp) was already
// present. Added to match; not a new invention.
func TouchOp(keys ...*as.Key) WriteOp {
	return WriteOp{}
}

func UpdateOp(keys ...*as.Key) WriteOp {
	return WriteOp{}
}

// GAP: InsertOp previously lived here as a batch-op counterpart to the
// single-key Insert(ctx, key) write verb — removed: sdk/PRD.md's §10.5
// table names UpsertOp/DeleteOp/UpdateOp explicitly (and TouchOp via a
// second, separate mention), but never InsertOp, not even via the
// table's trailing "…". Same invented-by-symmetry reasoning already
// ruled out elsewhere (sdk/FUNCTIONAL_GAPS.md finding #27). A batch
// insert-only op has no PRD-grounded spelling in sdk/ until the PRD
// names one.

func (op WriteOp) Set(name string, v any) WriteOp {
	return op
}

func (op WriteOp) Add(name string, delta any) WriteOp {
	return op
}
