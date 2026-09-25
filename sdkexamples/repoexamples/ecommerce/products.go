package ecommerce

import (
	"context"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// productOp marshals a Product into an UpsertOp; see marshalOp for why.
func productOp(key *as.Key, p Product) (sdk.WriteOp, error) {
	return marshalOp(key, p, fmt.Sprintf("product %s", p.SKU))
}

// RecordProductRatings exercises CDT map mutation on a single key's
// "ratings" bin: add several ratings, update one, and remove one.
//
// GAP: this only covers the write side. Reading a CDT sub-element back
// (a specific key's value, the top-ranked entry, a count of a value range)
// is a real, unaddressed gap in the PRD for two separate reasons, and
// removing that demonstration is deliberate rather than an oversight:
//
//  1. WriteResult{Key, Generation, Affected, ResultCode} (10.14) has no
//     field for a value a read-style terminal (GetValues/GetAsOrderedMap/
//     Count) actually produces — there's no way to get the value back out
//     of ExecuteOne() even if you knew which entry point to call it on.
//  2. There is no PRD-defined entry point for a pure CDT read at all.
//     Get (the real point-read method) doesn't support CDT navigation, and
//     the write-verb builders (Upsert/Update/...) are the only way to
//     reach OnMapKey/OnMapRank/etc. An earlier version of this function
//     used Update(ctx, key) for the reads, reasoning that Update's
//     existence-precondition ("record must already exist") was the
//     closest fit — but that was this example inventing an unreviewed
//     convention, not something the PRD specifies. The classic v8 client
//     already solves this with a neutral Operate(policy, key, ops...)
//     verb (client.go) that doesn't imply a write either way; the PRD's
//     write-verb-only builder design dropped that neutral entry point, so
//     a decision on how a customer is meant to perform a pure CDT read is
//     still owed here.
func (s *Service) RecordProductRatings(ctx context.Context) error {
	tvKey := sdk.Key(s.productDS.DataSet(), "SKU-TV55")

	// Six customers' star ratings, set as one write against the "ratings"
	// map bin — one round trip, one WriteResult per rating (D-19: each
	// key in the map can independently apply or not).
	initialRatings := []struct {
		customerID string
		stars      int64
	}{
		{"C-100", 5},
		{"C-103", 4},
		{"C-107", 3},
		{"C-110", 2},
		{"C-112", 5},
		{"C-117", 4},
	}
	// DX/readability GAP: this reads as two separate steps — "create a
	// builder" on one line, then "do OnBin/OnMapKey/SetTo stuff to it" on
	// another, inside a loop — because Go's `builder = builder.Method()`
	// reassignment pattern looks exactly like an ordinary imperative
	// sequence (the same shape as `sum = sum + x` in a loop). It isn't:
	// nothing is sent over the wire until Execute() below; every
	// iteration is just extending the same in-memory, not-yet-sent
	// request. The underlying reason this loop exists at all is that the
	// CDT builder has no declarative "apply these N map-key writes at
	// once" primitive (something like .OnBin("ratings").SetMany(pairs)) —
	// so a customer with data-driven ratings is stuck choosing between a
	// hand-repeated fluent chain (confusing because of the repetition) or
	// this reassignment loop (confusing because it visually reads as a
	// sequence of separate operations rather than one accumulating
	// write). Neither is genuinely clean; a bulk CDT-write entry point
	// would remove the need for either.
	builder := s.session.Upsert(ctx, tvKey)
	for _, r := range initialRatings {
		builder = builder.OnBin("ratings").OnMapKey(r.customerID).SetTo(r.stars)
	}
	stream, err := builder.Execute()
	if err := checkWrite(ctx, stream, err, "add ratings"); err != nil {
		return err
	}

	stream, err = s.session.Upsert(ctx, tvKey).
		OnBin("ratings").OnMapKey("C-107").SetTo(int64(5)).
		Execute()
	if err := checkWrite(ctx, stream, err, "update C-107's rating"); err != nil {
		return err
	}

	stream, err = s.session.Upsert(ctx, tvKey).
		OnBin("ratings").OnMapKey("C-110").Remove().
		Execute()
	if err := checkWrite(ctx, stream, err, "remove C-110's rating"); err != nil {
		return err
	}

	return nil
}

// ApplySalePrices runs a background scan that writes a computed sale price
// into matching records: stock > 250 and price <= $50, sale = 80% of price
// when price >= $10, else 90%.
//
// GAP: the PRD has no bin-level AEL write primitive for computing a value
// into a bin (WhereAEL only covers filtering; ModifyBy(exp) is for
// path-expression traversal, not "compute this scalar into that bin").
// WithWriteOperations(ops ...*as.Operation) on QueryBuilder is the
// documented mechanism for attaching a write to a background scan, so the
// conditional sale price below is built with the root package's own
// expression builders (ExpCond/ExpWriteOp/...) instead.
func (s *Service) ApplySalePrices(ctx context.Context) error {
	eightyPercent := as.ExpNumDiv(as.ExpNumMul(as.ExpIntBin(productPriceBin), as.ExpIntVal(8)), as.ExpIntVal(10))
	ninetyPercent := as.ExpNumDiv(as.ExpNumMul(as.ExpIntBin(productPriceBin), as.ExpIntVal(9)), as.ExpIntVal(10))
	salePriceExp := as.ExpCond(
		as.ExpGreaterEq(as.ExpIntBin(productPriceBin), as.ExpIntVal(1000)), eightyPercent,
		ninetyPercent,
	)
	saleOp := as.ExpWriteOp(productSalePriceBin, salePriceExp, as.ExpWriteFlagDefault)

	filterExp := as.ExpAnd(
		as.ExpGreater(as.ExpIntBin(productStockBin), as.ExpIntVal(250)),
		as.ExpLessEq(as.ExpIntBin(productPriceBin), as.ExpIntVal(5000)),
	)

	// DX GAP: nothing forces task.Wait(ctx) below to actually be called —
	// Go has no must-use mechanism, and "_, err := ...ExecuteBackgroundTask()"
	// compiles clean, silently never checking whether the scan finished
	// or even ran. The real fix would be a bodyclose/sqlclosecheck-style
	// linter for Task, not anything expressible in this API's signatures.
	task, err := s.session.Scan(ctx, s.productDS.DataSet()).
		Where(filterExp).
		WithWriteOperations(saleOp).
		ExecuteBackgroundTask()
	if err != nil {
		return fmt.Errorf("start sale-price background scan: %w", err)
	}
	if err := task.Wait(ctx); err != nil {
		return fmt.Errorf("wait for sale-price background scan: %w", err)
	}
	return nil
}

// ScanAffordableProducts ranges over a filtered scan the same way
// StreamOrders does, including the same ExecuteOnError(InStream())
// disposition.
func (s *Service) ScanAffordableProducts(ctx context.Context) ([]Product, error) {
	stream, err := s.session.Scan(ctx, s.productDS.DataSet()).
		WhereAEL(fmt.Sprintf("$.%s > 100 and $.%s < 10000", productStockBin, productPriceBin)).
		ExecuteOnError(sdk.InStream())
	if err != nil {
		return nil, fmt.Errorf("scan affordable products: %w", err)
	}
	defer stream.Close()

	var products []Product
	for row, rowErr := range stream.Iter(ctx) {
		if rowErr != nil {
			fmt.Printf("  Scan error: %v\n", rowErr)
			continue
		}
		rec, err := row.Record()
		if err != nil {
			fmt.Printf("  Record error: %v\n", err)
			continue
		}
		p, err := sdk.Decode[Product](rec)
		if err != nil {
			fmt.Printf("  Decode error: %v\n", err)
			continue
		}
		products = append(products, p)
	}
	if err := stream.Err(); err != nil {
		return nil, fmt.Errorf("scan affordable products: %w", err)
	}
	return products, nil
}
