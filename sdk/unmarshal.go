//go:build go1.27

// Copyright 2014-2026 Aerospike, Inc.
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

package sdk

import (
	"reflect"
	"strings"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// unmarshalRecord fills a struct from a record's bins, honoring the `as`
// struct tags: an explicit bin name, `-` to skip, and the `,key` option, whose
// field is filled from the key rather than a bin.
//
// The core client's own reflection reader is unexported, so the SDK carries a
// focused equivalent that understands the SDK's additional `,key` option.
func unmarshalRecord(rec *as.Record, out any) error {
	rv := reflect.ValueOf(out)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return NewError(KindInvalidArgument, "unmarshal target must be a non-nil pointer")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return NewError(KindInvalidArgument, "unmarshal target must point to a struct")
	}
	rt := rv.Type()

	for i := range rt.NumField() {
		f := rt.Field(i)
		if f.PkgPath != "" {
			continue
		}
		tag := f.Tag.Get("as")
		parts := strings.Split(tag, ",")
		name := strings.TrimSpace(parts[0])
		if name == "-" {
			continue
		}
		// The key and metadata fields are filled by the caller.
		if hasTagOption(parts, keyTagOption) {
			continue
		}
		if strings.TrimSpace(f.Tag.Get("asm")) != "" {
			continue
		}
		if name == "" {
			name = f.Name
		}

		raw, ok := rec.Bins[name]
		if !ok || raw == nil {
			continue
		}
		if err := assignBinValue(rv.Field(i), raw); err != nil {
			return NewError(KindInvalidArgument,
				"cannot map bin %q into field %s: %s", name, f.Name, err)
		}
	}
	return nil
}

// hasTagOption reports whether a tag carries the given option.
func hasTagOption(parts []string, option string) bool {
	for _, p := range parts[1:] {
		if strings.TrimSpace(p) == option {
			return true
		}
	}
	return false
}

// assignBinValue writes a bin value into a struct field, converting where the
// types differ but are compatible.
func assignBinValue(field reflect.Value, raw any) error {
	if !field.CanSet() {
		return nil
	}
	rv := reflect.ValueOf(raw)

	if rv.Type().AssignableTo(field.Type()) {
		field.Set(rv)
		return nil
	}

	// A pointer field models an optional value: allocate and fill the pointee,
	// so a struct can distinguish "absent" (nil) from "present and zero".
	if field.Kind() == reflect.Ptr {
		pointee := reflect.New(field.Type().Elem())
		if err := assignBinValue(pointee.Elem(), raw); err != nil {
			return err
		}
		field.Set(pointee)
		return nil
	}

	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if v, ok := toInt64(raw); ok {
			field.SetInt(v)
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if v, ok := toInt64(raw); ok && v >= 0 {
			field.SetUint(uint64(v))
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch t := raw.(type) {
		case float64:
			field.SetFloat(t)
			return nil
		case float32:
			field.SetFloat(float64(t))
			return nil
		default:
			if v, ok := toInt64(raw); ok {
				field.SetFloat(float64(v))
				return nil
			}
		}
	case reflect.Bool:
		switch t := raw.(type) {
		case bool:
			field.SetBool(t)
			return nil
		default:
			if v, ok := toInt64(raw); ok {
				field.SetBool(v != 0)
				return nil
			}
		}
	case reflect.String:
		if s, ok := raw.(string); ok {
			field.SetString(s)
			return nil
		}
	}

	if rv.Type().ConvertibleTo(field.Type()) {
		field.Set(rv.Convert(field.Type()))
		return nil
	}
	return NewError(KindInvalidArgument, "incompatible types %T and %s", raw, field.Type())
}
