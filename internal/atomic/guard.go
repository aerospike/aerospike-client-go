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

package atomic

import "sync"

// Guard allows synchronized access to a value
type Guard[T any] struct {
	val *T
	m   sync.Mutex
}

// NewGuard creates a new instance of Guard
func NewGuard[T any](val *T) *Guard[T] {
	return &Guard[T]{val: val}
}

// Do calls the passed closure.
func (g *Guard[T]) Do(f func(*T)) {
	g.m.Lock()
	defer g.m.Unlock()
	f(g.val)
}

// DoVal calls the passed closure with a dereferenced internal value.
func (g *Guard[T]) DoVal(f func(T)) {
	g.m.Lock()
	defer g.m.Unlock()
	f(*g.val)
}

// Call the passed closure allowing to replace the content.
func (g *Guard[T]) Update(f func(**T) error) error {
	g.m.Lock()
	defer g.m.Unlock()
	return f(&g.val)
}

// Calls the passed closure allowing to replace the content.
// It will call the init func if the internal values is nil.
func (g *Guard[T]) InitDo(init func() *T, f func(*T)) {
	g.m.Lock()
	defer g.m.Unlock()
	if g.val == nil {
		g.val = init()
	}
	f(g.val)
}

// Calls the passed closure allowing to replace the content.
// It will call the init func if the internal values is nil.
// It is used for reference values like slices and maps.
func (g *Guard[T]) InitDoVal(init func() T, f func(T)) {
	g.m.Lock()
	defer g.m.Unlock()
	if g.val == nil {
		t := init()
		g.val = &t
	}
	f(*g.val)
}

// Release returns the internal value and sets it to nil
func (g *Guard[T]) Release() *T {
	g.m.Lock()
	defer g.m.Unlock()
	res := g.val
	g.val = nil
	return res
}
