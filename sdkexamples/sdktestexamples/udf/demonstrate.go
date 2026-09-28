package udf

import (
	"context"
	"fmt"

	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

// DemonstrateWriteUsingUdf mirrors the source Java test writeUsingUdf():
// one key, one bin, a single writeBin UDF call.
//
// GAP: the source Java test then queries the key and reads the bin back
// (rec.getString(binName), confirming it's really "string value") to
// verify the write actually happened, not just that the call was
// accepted. Same reason as DemonstrateWriteIfNotExists: Record (sdk/
// session.go) has no bin-accessor methods at all, so that verification
// read can't be done here either — this can only confirm the UDF call
// was accepted.
func (s *Service) DemonstrateWriteUsingUdf(ctx context.Context) error {
	key := sdk.Key(s.ds, "writeUsingUdf")
	const binName = "udfbin1"

	if err := s.WriteBin(ctx, key, binName, "string value"); err != nil {
		return err
	}
	fmt.Println("writeUsingUdf: wrote udfbin1 via UDF (can't verify the value read back — see GAP comment)")
	return nil
}

// DemonstrateWriteIfGenerationNotChanged mirrors the source Java test of
// the same name: seed the record with a plain write (not the UDF —
// deliberately, to show getGeneration works against a normally-written
// record, not just a UDF-written one), read its generation back via the
// getGeneration UDF, then conditionally write via
// writeIfGenerationNotChanged.
func (s *Service) DemonstrateWriteIfGenerationNotChanged(ctx context.Context) error {
	key := sdk.Key(s.ds, "writeIfGenerationNotChanged")
	const binName = "udfbin2"

	if _, err := s.session.Upsert(ctx, key).
		Append(binName, "string value").
		ExecuteOne(); err != nil {
		return fmt.Errorf("seed %v with a plain write: %w", key, err)
	}

	gen, err := s.GetGeneration(ctx, key)
	if err != nil {
		return err
	}

	if err := s.WriteIfGenerationNotChanged(ctx, key, binName, "string value", gen); err != nil {
		return err
	}
	fmt.Printf("writeIfGenerationNotChanged: wrote udfbin2 at generation %d\n", gen)
	return nil
}

// DemonstrateWriteIfNotExists mirrors the source Java test writeIfNotExists():
// delete the key first (guaranteeing a clean precondition, the same as
// the source test's explicit session.delete(key).execute()), then call
// writeUnique twice — the second call should be a silent no-op.
//
// GAP: the source Java test then verifies this by reading the bin back
// (rec.getString(binName), confirming it's still "first", not
// overwritten by the second call's "second"). That verification can't be
// done here: Record (sdk/session.go) is completely opaque — no bin
// accessor methods at all (D-8's rec.String(bin)-style accessors are P2
// and not implemented in the current stub), and Decode needs a typed
// struct, which this plain untyped dataset doesn't have. So this can
// confirm both UDF calls were accepted, not that the second one actually
// left the value unchanged — a different, narrower facet of the same
// Record-opacity gap already documented for generation/expiration
// (sdk/FUNCTIONAL_GAPS.md finding #16), this time for bin values.
func (s *Service) DemonstrateWriteIfNotExists(ctx context.Context) error {
	key := sdk.Key(s.ds, "writeIfNotExists")
	const binName = "udfbin3"

	if err := s.session.Delete(ctx, key); err != nil {
		return fmt.Errorf("delete %v before writeIfNotExists demo: %w", key, err)
	}

	if err := s.WriteUnique(ctx, key, binName, "first"); err != nil {
		return err
	}
	if err := s.WriteUnique(ctx, key, binName, "second (should no-op)"); err != nil {
		return err
	}
	fmt.Println("writeIfNotExists: two writeUnique calls accepted (can't verify the second was a no-op — see GAP comment)")
	return nil
}

// DemonstrateWriteWithValidation mirrors the source Java test of the
// same name: a valid value (4) succeeds, an invalid one (11) is rejected
// with the exact propagated code 1000.
func (s *Service) DemonstrateWriteWithValidation(ctx context.Context) error {
	key := sdk.Key(s.ds, "writeWithValidation")
	const binName = "udfbin4"

	if invalid, err := s.WriteWithValidation(ctx, key, binName, 4); err != nil {
		return fmt.Errorf("writeWithValidation with a valid value: %w", err)
	} else if invalid {
		return fmt.Errorf("writeWithValidation with a valid value: unexpectedly rejected")
	}

	invalid, err := s.WriteWithValidation(ctx, key, binName, 11)
	if err != nil {
		return fmt.Errorf("writeWithValidation with an invalid value: %w", err)
	}
	if !invalid {
		return fmt.Errorf("writeWithValidation with an invalid value: expected rejection, got success")
	}
	fmt.Println("writeWithValidation: invalid value correctly rejected with code 1000")
	return nil
}
