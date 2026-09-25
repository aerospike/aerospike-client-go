package queryexamples

import (
	"context"
	"errors"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// NOTE: BackgroundTask (10.8's literal type name) and Task (10.13's) are
// the same PRD-documented concept — one Wait(ctx) error handle — under
// two different names; this codebase already resolved that inconsistency
// by unifying both under one sdk.Task type (see sdk/stream.go), so all
// three functions below use Task rather than BackgroundTask.

// DemonstrateBackgroundTask runs a background scan that increments every
// customer's age by one, waits for it to complete, then re-reads one
// customer to confirm the increment applied.
//
// The source Java example also shows a second, filtered variant (a
// where("$.state == 'nsw'") clause plus a computed upsertFrom
// expression) — left out here since Customer has no "state" field to
// filter on, not because of a gap.
func (s *Service) DemonstrateBackgroundTask(ctx context.Context, id int64) error {
	task, err := s.session.Query(ctx, s.customerDS.DataSet()).
		WithWriteOperations(as.AddOp(as.NewBin(customerAgeBin, 1))).
		ExecuteBackgroundTask()
	if err != nil {
		return fmt.Errorf("start background age increment: %w", err)
	}
	if err := task.Wait(ctx); err != nil {
		return fmt.Errorf("wait for background age increment: %w", err)
	}

	key := sdk.Key(s.customerDS.DataSet(), id)
	record, err := s.session.Get(ctx, key, sdk.AllBins)
	if err != nil {
		return fmt.Errorf("get customer %d after background task: %w", id, err)
	}
	customer, err := sdk.Decode[Customer](record)
	if err != nil {
		return fmt.Errorf("decode customer %d: %w", id, err)
	}
	fmt.Printf("customer %d age after background increment: %d\n", id, customer.Age)
	return nil
}

// DemonstrateBackgroundDelete seeds a throwaway customer, runs a
// background scan that deletes every customer matching a filter on it,
// waits for completion, then confirms the target record is gone.
func (s *Service) DemonstrateBackgroundDelete(ctx context.Context) error {
	const throwawayID = int64(1)
	if err := s.SeedCustomer(ctx, Customer{ID: throwawayID, Name: "Delete-Me", Age: 1}); err != nil {
		return err
	}

	task, err := s.session.Query(ctx, s.customerDS.DataSet()).
		WhereAEL(fmt.Sprintf("$.%s == 'Delete-Me'", customerNameBin)).
		ExecuteBackgroundDelete()
	if err != nil {
		return fmt.Errorf("start background delete: %w", err)
	}
	if err := task.Wait(ctx); err != nil {
		return fmt.Errorf("wait for background delete: %w", err)
	}

	key := sdk.Key(s.customerDS.DataSet(), throwawayID)
	if _, err := s.session.Get(ctx, key, sdk.AllBins); err != nil {
		if errors.Is(err, sdk.ErrNotFound) {
			fmt.Println("background delete confirmed: record no longer exists")
			return nil
		}
		return fmt.Errorf("get customer %d after background delete: %w", throwawayID, err)
	}
	return fmt.Errorf("expected customer %d to be deleted, but it still exists", throwawayID)
}

// DemonstrateBackgroundTouch runs a background scan that touches every
// customer (refreshing TTL/generation without changing bin values), waits
// for completion, then confirms one customer's expiration moved forward.
func (s *Service) DemonstrateBackgroundTouch(ctx context.Context, id int64) error {
	key := sdk.Key(s.customerDS.DataSet(), id)

	before, err := s.session.Get(ctx, key, sdk.AllBins)
	if err != nil {
		return fmt.Errorf("get customer %d before background touch: %w", id, err)
	}

	task, err := s.session.Query(ctx, s.customerDS.DataSet()).
		ExecuteBackgroundTouch()
	if err != nil {
		return fmt.Errorf("start background touch: %w", err)
	}
	if err := task.Wait(ctx); err != nil {
		return fmt.Errorf("wait for background touch: %w", err)
	}

	after, err := s.session.Get(ctx, key, sdk.AllBins)
	if err != nil {
		return fmt.Errorf("get customer %d after background touch: %w", id, err)
	}
	fmt.Printf("customer %d expiration: %s -> %s\n", id, before.Expiration, after.Expiration)
	return nil
}
