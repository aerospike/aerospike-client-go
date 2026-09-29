package udf

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"

	as "github.com/aerospike/aerospike-client-go/v8"
	sdk "github.com/aerospike/aerospike-client-go/v8/sdk"
)

//go:embed scripts/record_example.lua
var recordExampleLua []byte

const moduleName = "record_example.lua"

// modulePackage is moduleName without its .lua extension — the package
// name a UDF call actually addresses (matching the source Java test's
// RegisterTask registration under "bg_test_example.lua", then called via
// "bg_test_example" — the standard Lua-module-name-without-extension
// convention). ExecuteUDFBackgroundTask (unlike ExecuteUDF's
// handle-based Module(...).Function(...) path) takes this as a bare
// string per its own signature (10.8's ExecuteUDFBackgroundTask(pkg, fn
// string, args ...any)) — there's no handle-based alternative for it.
const modulePackage = "record_example"

// RegisterModule registers the embedded Lua module and waits for it to
// be ready everywhere — RegisterUDF (10.11, D-13/D-18) is synchronous and
// returns the *UDFModule handle directly, unlike the source Java example
// (registerUdfString returns a RegisterTask requiring
// task.waitTillComplete() separately). A real, deliberate shape
// difference, not a gap — Go's version needs no separate wait step at
// all.
func (s *Service) RegisterModule(ctx context.Context) error {
	mod, err := s.session.RegisterUDF(ctx, sdk.UDF{
		Name:     moduleName,
		Language: sdk.LUA,
		Source:   bytes.NewReader(recordExampleLua),
	})
	if err != nil {
		return fmt.Errorf("register UDF module %s: %w", moduleName, err)
	}
	s.module = mod
	return nil
}

// WriteBin calls the module's writeBin(r, name, value) function — the
// simplest UDF write. writeBin has no return statement (implicit Lua
// nil), and the source Java test confirms that explicitly (assertNull on
// the execute() result) — matched here by checking Return is nil.
func (s *Service) WriteBin(ctx context.Context, key *as.Key, binName, value string) error {
	result, err := s.session.ExecuteUDF(ctx, key).
		Module(s.module).
		Function("writeBin").
		Passing(binName, value).
		Execute()
	if err != nil {
		return fmt.Errorf("writeBin on %v: %w", key, err)
	}
	if result.Return != nil {
		return fmt.Errorf("writeBin on %v: expected no return value, got %v", key, result.Return)
	}
	return nil
}

// GetGeneration calls getGeneration(r), returning the record's current
// generation as reported by the UDF itself — not read from Record
// (sdk/session.go's Record is completely opaque, no PRD-defined way to
// read a generation back from it at all; see sdk/FUNCTIONAL_GAPS.md
// finding #16). This is a genuinely different, UDF-side path to the same
// value, and it isn't blocked by that gap: UDFResult (unlike WriteResult)
// has a real Return field (10.14), so the value actually comes back.
func (s *Service) GetGeneration(ctx context.Context, key *as.Key) (int64, error) {
	result, err := s.session.ExecuteUDF(ctx, key).
		Module(s.module).
		Function("getGeneration").
		Execute()
	if err != nil {
		return 0, fmt.Errorf("getGeneration on %v: %w", key, err)
	}
	gen, ok := result.Return.(int64)
	if !ok {
		return 0, fmt.Errorf("getGeneration on %v: unexpected return type %T", key, result.Return)
	}
	return gen, nil
}

// WriteIfGenerationNotChanged calls writeIfGenerationNotChanged(r, name,
// value, gen) — a UDF-side optimistic-concurrency check, evaluated
// server-side inside the Lua function rather than via IfGeneration on a
// WriteSegmentBuilder. A second, independent path to the same "only
// write if nothing changed underneath me" guarantee IfGeneration gives —
// this one never needed a client-side generation read from Record at
// all, since the UDF reads record.gen(r) itself. Like writeBin, this
// function has no return statement; the source Java test confirms that
// explicitly (assertTrue(obj.isEmpty())) — matched here the same way.
func (s *Service) WriteIfGenerationNotChanged(ctx context.Context, key *as.Key, binName, value string, gen int64) error {
	result, err := s.session.ExecuteUDF(ctx, key).
		Module(s.module).
		Function("writeIfGenerationNotChanged").
		Passing(binName, value, gen).
		Execute()
	if err != nil {
		return fmt.Errorf("writeIfGenerationNotChanged on %v: %w", key, err)
	}
	if result.Return != nil {
		return fmt.Errorf("writeIfGenerationNotChanged on %v: expected no return value, got %v", key, result.Return)
	}
	return nil
}

// WriteUnique calls writeUnique(r, name, value) — a UDF-side
// create-only guard: the Lua function checks aerospike:exists(r) itself
// before writing, so a second call against an already-existing record is
// a silent no-op, not an error.
func (s *Service) WriteUnique(ctx context.Context, key *as.Key, binName, value string) error {
	if _, err := s.session.ExecuteUDF(ctx, key).
		Module(s.module).
		Function("writeUnique").
		Passing(binName, value).
		Execute(); err != nil {
		return fmt.Errorf("writeUnique on %v: %w", key, err)
	}
	return nil
}

// WriteWithValidation calls writeWithValidation(r, name, value), which
// raises a Lua error("1000:Invalid value") for any value outside 1-10.
// The source Java test asserts the specific propagated code
// (assertEquals(1000, ae.getResultCode())), not just "some UDF error" —
// matched here via errors.As into *sdk.Error and checking Code, not the
// looser errors.Is(err, sdk.ErrUDF) an earlier version of this function
// used. (Note: sdk.Error.Is (sdk/errors.go) always returns false in the
// current stub by its own documented admission, so errors.Is(err,
// sdk.ErrUDF) could never have matched anything regardless — errors.As
// doesn't depend on Is at all, which is the other reason it's the
// correct check here, not just the more precise one.)
func (s *Service) WriteWithValidation(ctx context.Context, key *as.Key, binName string, value int64) (invalid bool, err error) {
	_, err = s.session.ExecuteUDF(ctx, key).
		Module(s.module).
		Function("writeWithValidation").
		Passing(binName, value).
		Execute()
	var sdkErr *sdk.Error
	if errors.As(err, &sdkErr) && sdkErr.Code == 1000 {
		return true, nil
	}
	return false, err
}

// ListModules reports how many UDF modules are registered on the
// cluster.
func (s *Service) ListModules(ctx context.Context) (int, error) {
	modules, err := s.session.ListUDF(ctx)
	if err != nil {
		return 0, fmt.Errorf("list UDF modules: %w", err)
	}
	return len(modules), nil
}

// RemoveModule removes the registered module — cleanup, mirroring
// RegisterModule.
func (s *Service) RemoveModule(ctx context.Context) error {
	if err := s.session.RemoveUDF(ctx, moduleName); err != nil {
		return fmt.Errorf("remove UDF module %s: %w", moduleName, err)
	}
	return nil
}
