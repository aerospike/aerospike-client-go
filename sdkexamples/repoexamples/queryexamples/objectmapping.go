package queryexamples

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateObjectMapping marshals a typed Customer — including a nested
// Address — into bins, writes it with the plain point Put, reads it back,
// and decodes it into a fresh Customer to confirm the round trip. This is
// the PRD's own 10.10 Look example pattern (Marshal + Put, Get + Decode),
// not a bulk/row-oriented insert of many typed objects — that's 10.9's
// RowWriteBuilder, already flagged as a real, separate gap in
// ecommerce/seed.go (no row-oriented bulk typed write exists in sdk/ yet).
//
// DX GAP: original.ID is typed twice below — once in the struct (tagged
// as:",key"), once again passed into sdk.Key. The tag already says which
// field is the key; the SDK just doesn't use it to build one. A
// sdk.KeyOf(dataset, original) that read the tagged field itself would
// remove the second copy. (Same redundancy is in the PRD's own 10.10 Look
// example.)
func (s *Service) DemonstrateObjectMapping(ctx context.Context, id int64) error {
	original := Customer{
		ID:   id,
		Name: "sample",
		Age:  456,
		Address: &Address{
			Line1:   "123 Main St",
			City:    "Denver",
			State:   "CO",
			Country: "USA",
			ZipCode: "80112",
		},
	}
	fmt.Printf("reference customer: %s\n", original)

	key := sdk.Key(s.customerDS.DataSet(), original.ID)
	if err := s.session.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete customer %d before object mapping test: %w", id, err)
	}

	bins, err := sdk.Marshal(original)
	if err != nil {
		return fmt.Errorf("marshal customer %d: %w", id, err)
	}
	if err := s.session.Put(ctx, key, bins); err != nil {
		return fmt.Errorf("put customer %d: %w", id, err)
	}

	record, err := s.session.Get(ctx, key, sdk.AllBins)
	if err != nil {
		return fmt.Errorf("get customer %d: %w", id, err)
	}
	readBack, err := sdk.Decode[Customer](record)
	if err != nil {
		return fmt.Errorf("decode customer %d: %w", id, err)
	}
	fmt.Printf("customer read back: %s\n", readBack)
	return nil
}
