package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

var (
	ErrNotFound           error = &Error{}
	ErrGenerationMismatch error = &Error{}
	ErrBinExists          error = &Error{}
	ErrTimeout            error = &Error{}
	ErrCanceled           error = &Error{}
	ErrFilterExpression   error = &Error{}
	ErrUDF                error = &Error{}
	ErrPoolExhausted      error = &Error{}
)

type Error struct {
	Code    int
	InDoubt bool
	Op      string
	Key     *as.Key
	Cause   error
}

func (e *Error) Error() string {
	return ""
}

func (e *Error) Unwrap() error {
	return e.Cause
}

// Is always returns false in this stub. A real implementation needs to
// match by Code (or whatever identifies a sentinel), not by pointer
// identity to the package-level var — errors.Is(err, ErrNotFound) can only
// ever succeed today because err is that exact var, which won't hold once
// a real Get constructs a fresh *Error per call.
func (e *Error) Is(target error) bool {
	return false
}
