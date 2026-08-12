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

import "reflect"

// normalizeNamedKeyValue handles the [KeyValue] members that reach
// keyValueToAny as *named* types (`type UserID int64`), whose dynamic type does
// not match the plain type switch. The constraint guarantees the underlying
// kind is one the core client accepts, so this cannot fail.
func normalizeNamedKeyValue[T KeyValue](v T) any {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return rv.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int()
	case reflect.Uint8, reflect.Uint16, reflect.Uint32:
		return int64(rv.Uint())
	case reflect.Slice:
		// The constraint admits only ~[]byte among slices.
		return rv.Bytes()
	}
	// Unreachable while KeyValue lists only the kinds handled above.
	return rv.Interface()
}
