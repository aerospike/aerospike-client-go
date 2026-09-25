package sdk

import (
	"io"
)

type Language int

// UDF describes a module to register — Source accepts an io.Reader (D-18),
// so embedding via the standard directive works directly, instead of
// requiring a file path.
type UDF struct {
	Name     string
	Language Language
	Source   io.Reader
}

// UDFModule is the handle RegisterUDF returns, giving module identity
// without relying on the registered name alone (D-13).
type UDFModule struct{}

type UDFInfo struct{}

// UDFFunctionBuilder is entered via Session.ExecuteUDF(ctx, key).
type UDFFunctionBuilder struct{}

func (b *UDFFunctionBuilder) Module(mod *UDFModule) *UDFFunctionBuilder {
	return b
}

func (b *UDFFunctionBuilder) Function(name string) *UDFFunctionBuilder {
	return b
}

func (b *UDFFunctionBuilder) Passing(args ...any) *UDFFunctionBuilder {
	return b
}

func (b *UDFFunctionBuilder) Execute() (UDFResult, error) {
	return UDFResult{}, nil
}
