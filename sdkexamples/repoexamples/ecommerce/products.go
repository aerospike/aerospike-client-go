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

// RecordProductRatings exercises CDT map navigation on a single key's
// "ratings" bin: add several ratings, read one back a few different ways,
// update one, remove one, and recount. Reads that navigate into a CDT
// sub-element (rank, value range, a specific key) go through the same
// write-verb entry point as writes — Aerospike's operate command mixes
// read and write CDT ops in one call, so there's no separate read-only CDT
// builder; Update is used for the pure reads here since the record must
// already exist.
//
// GAP: ExecuteOne's result type — WriteResult{Key, Generation, Affected,
// ResultCode} (10.14) — has no field for a value a read-style terminal
// (GetValues/GetAsOrderedMap/Count) actually produces. There is currently
// no way for a caller to get the rating value, the top entry, or the count
// back out of these calls; the results below are discarded because the
// return type has nowhere to put them, not because this example doesn't
// need them.
func (s *Service) RecordProductRatings(ctx context.Context) error {
	tvKey := sdk.Key(s.productDS.DataSet(), "SKU-TV55")

	stream, err := s.session.Upsert(ctx, tvKey).
		OnBin("ratings").OnMapKey("C-100").SetTo(int64(5)).
		OnBin("ratings").OnMapKey("C-103").SetTo(int64(4)).
		OnBin("ratings").OnMapKey("C-107").SetTo(int64(3)).
		OnBin("ratings").OnMapKey("C-112").SetTo(int64(5)).
		OnBin("ratings").OnMapKey("C-110").SetTo(int64(2)).
		OnBin("ratings").OnMapKey("C-117").SetTo(int64(4)).
		Execute()
	if err := checkWrite(ctx, stream, err, "add ratings"); err != nil {
		return err
	}

	if _, err := s.session.Update(ctx, tvKey).
		OnBin("ratings").OnMapKey("C-100").GetValues().
		ExecuteOne(); err != nil {
		return fmt.Errorf("read C-100's rating: %w", err)
	}

	if _, err := s.session.Update(ctx, tvKey).
		OnBin("ratings").OnMapRank(-1).GetAsOrderedMap().
		ExecuteOne(); err != nil {
		return fmt.Errorf("read highest-rated entry: %w", err)
	}

	if _, err := s.session.Update(ctx, tvKey).
		OnBin("ratings").OnMapValueRange(int64(4), int64(6)).Count().
		ExecuteOne(); err != nil {
		return fmt.Errorf("count 4+ star ratings: %w", err)
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

	if _, err := s.session.Update(ctx, tvKey).
		OnBin("ratings").OnMapValueRange(int64(4), int64(6)).Count().
		ExecuteOne(); err != nil {
		return fmt.Errorf("recount 4+ star ratings: %w", err)
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
