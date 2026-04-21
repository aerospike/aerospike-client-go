// Copyright 2014-2022 Aerospike, Inc.
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

// BinSerDer is implemented by user structs that want to opt into the
// zero-reflection read path. Types that implement BinSerDer can be
// passed to Client.GetObjectBinSerDer (and related APIs) to be
// populated from an Aerospike record without using reflection on the
// user's struct.
//
// The code generator under tools/binserdergen produces a valid
// BinSerDer implementation for any struct annotated with the existing
// `as:"binName"` struct tags.
//
// Implementations are expected to populate their own fields in place.
// A zero-value struct pointer is the typical argument. Implementations
// must be safe to call with any order of bins and with a subset of the
// bins returned by AerospikeBinNames.
//
// The BinSerDer interface is intentionally minimal to keep the number
// of temporal allocations on the read path as small as possible. No
// reflection, maps, or intermediate structures are required to
// deserialize a record when this interface is used.
//
// A minimal hand-written implementation looks like this:
//
//	type Person struct {
//	    Name string
//	    Age  int
//	}
//
//	// Cache the bin-name slice at package scope so every read reuses it.
//	var personBinNames = []string{"name", "age"}
//
//	func (p *Person) AerospikeBinNames() []string { return personBinNames }
//
//	func (p *Person) UnmarshalBin(name string, value any) as.Error {
//	    if value == nil {
//	        return nil
//	    }
//	    switch name {
//	    case "name":
//	        if v, ok := value.(string); ok {
//	            p.Name = v
//	        }
//	    case "age":
//	        if v, ok := value.(int); ok {
//	            p.Age = v
//	        }
//	    }
//	    return nil
//	}
//
//	// Usage:
//	p := &Person{}
//	if err := client.GetObjectBinSerDer(nil, key, p); err != nil { ... }
type BinSerDer interface {
	// AerospikeBinNames returns the list of bin names to request from
	// the server for this type. The returned slice is used verbatim by
	// the client; implementations should return a cached (e.g.
	// package-level) slice to avoid allocations on every call.
	//
	// The slice must not be mutated after it is returned — the client
	// may retain a reference to it for the duration of the read.
	//
	// Example:
	//
	//	var personBinNames = []string{"name", "age", "bio"}
	//
	//	func (p *Person) AerospikeBinNames() []string {
	//	    return personBinNames
	//	}
	AerospikeBinNames() []string

	// UnmarshalBin is invoked once for each bin returned by the server,
	// in the order the server returns them. name is the bin name and
	// value is the decoded bin value, using the same concrete Go types
	// that appear in Record.Bins: int, string, []byte, float64,
	// map[any]any, []any, and so on. Implementations are expected to
	// dispatch on name (typically via a switch statement) and assign
	// the correctly-typed value to the target field.
	//
	// Implementations must tolerate:
	//   - a nil value (bin present but empty), and
	//   - a subset of the bins returned by AerospikeBinNames (bins the
	//     server did not have for this record are simply never passed
	//     in).
	//
	// Bins the implementation does not recognise should be ignored by
	// returning nil — they are not an error.
	//
	// If an implementation cannot decode value into a target field, it
	// must return a non-nil Error describing the mismatch so that
	// callers can distinguish between "bin not recognised" (nil) and
	// "bin recognised but decode failed" (non-nil).
	//
	// Example:
	//
	//	func (p *Person) UnmarshalBin(name string, value any) as.Error {
	//	    if value == nil {
	//	        return nil
	//	    }
	//	    switch name {
	//	    case "name":
	//	        v, ok := value.(string)
	//	        if !ok {
	//	            return as.ErrInvalidParam // or a custom as.Error
	//	        }
	//	        p.Name = v
	//	    case "age":
	//	        if v, ok := value.(int); ok {
	//	            p.Age = v
	//	        }
	//	    }
	//	    return nil
	//	}
	UnmarshalBin(name string, value any) Error
}

// BinSerDerMeta is an optional extension of BinSerDer. Types that
// implement it will receive the record's generation and expiration
// values after all bins have been unmarshalled. Implement this only if
// you need access to the record metadata — a plain BinSerDer is
// sufficient for reads that care about bin data alone.
//
// Example:
//
//	type Person struct {
//	    Name string
//	    Age  int
//
//	    Gen uint32 // populated from record metadata
//	    TTL uint32 // seconds until expiration
//	}
//
//	// Person already implements BinSerDer (see the BinSerDer example).
//
//	func (p *Person) SetAerospikeMeta(generation uint32, expiration uint32) {
//	    p.Gen = generation
//	    p.TTL = expiration
//	}
type BinSerDerMeta interface {
	BinSerDer

	// SetAerospikeMeta is called once per read, after all UnmarshalBin
	// calls complete, with the generation and TTL (expiration, in
	// seconds from epoch or offset, matching the reflection-based
	// asm:"ttl"/asm:"gen" tags) values returned by the server.
	//
	// It is called exactly once per successful read — never before
	// UnmarshalBin and never if the record does not exist.
	SetAerospikeMeta(generation uint32, expiration uint32)
}
