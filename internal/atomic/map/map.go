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

import (
	"maps"
	"sync"
)

// Map implements a Map with atomic semantics.
type Map[K comparable, V any] struct {
	m     map[K]V
	mutex sync.RWMutex
}

// New generates a new Map instance.
func New[K comparable, V any](length int) *Map[K, V] {
	return &Map[K, V]{
		m: make(map[K]V, length),
	}
}

// New generates a new Map instance with initial entry.
func NewWithValue[K comparable, V any](k K, v V) *Map[K, V] {
	value := New[K, V](0)
	value.m[k] = v

	return value
}

// Exists atomically checks if a key exists in the map
func (m *Map[K, V]) Exists(k K) bool {
	m.mutex.RLock()
	_, ok := m.m[k]
	m.mutex.RUnlock()
	return ok
}

// Get atomically retrieves an element from the Map.
func (m *Map[K, V]) Get(k K) V {
	m.mutex.RLock()
	res := m.m[k]
	m.mutex.RUnlock()
	return res
}

// Set atomically sets an element in the Map.
// If idx is out of range, it will return an error.
func (m *Map[K, V]) Set(k K, v V) {
	m.mutex.Lock()
	m.m[k] = v
	m.mutex.Unlock()
}

// Replace replaces the internal map with the provided one.
func (m *Map[K, V]) Replace(nm map[K]V) {
	m.mutex.Lock()
	m.m = nm
	m.mutex.Unlock()
}

// Length returns the Map size.
func (m *Map[K, V]) Length() int {
	m.mutex.RLock()
	res := len(m.m)
	m.mutex.RUnlock()

	return res
}

// Clone copies the map and returns the copy.
func (m *Map[K, V]) Clone() map[K]V {
	m.mutex.RLock()
	res := make(map[K]V, len(m.m))

	maps.Copy(res, m.m)

	m.mutex.RUnlock()

	return res
}

// CloneMap copies the map and returns the copy.
func (m *Map[K, V]) CloneMap() *Map[K, V] {
	m.mutex.RLock()
	res := New[K, V](len(m.m))
	res.mutex.Lock()

	// Using deep copy
	maps.Copy(res.m, m.m)

	res.mutex.Unlock()
	m.mutex.RUnlock()

	return res
}

// CloneAndResetMap copies the map and resets the original map.
func (m *Map[K, V]) CloneAndResetMap() *Map[K, V] {
	m.mutex.Lock()
	res := New[K, V](len(m.m))
	res.mutex.Lock()

	// Using deep copy
	maps.Copy(res.m, m.m)

	// Reset the original map
	m.m = make(map[K]V, len(m.m))

	res.mutex.Unlock()
	m.mutex.Unlock()

	return res
}

// Returns the keys from the map.
func (m *Map[K, V]) Keys() []K {
	m.mutex.RLock()
	res := make([]K, 0, len(m.m))
	for k := range m.m {
		res = append(res, k)
	}
	m.mutex.RUnlock()

	return res
}

// Clear will remove all entries.
func (m *Map[K, V]) Clear() {
	m.mutex.Lock()
	m.m = make(map[K]V, len(m.m))
	m.mutex.Unlock()
}

// Delete will remove the key and return its value.
func (m *Map[K, V]) Delete(k K) V {
	m.mutex.Lock()
	res := m.m[k]
	delete(m.m, k)
	m.mutex.Unlock()
	return res
}

// DeleteDeref will dereference and remove the key and return its value.
func (m *Map[K, V]) DeleteDeref(k *K) V {
	m.mutex.Lock()
	res := m.m[*k]
	delete(m.m, *k)
	m.mutex.Unlock()
	return res
}

// DeleteAllDeref will dereferences and removes the keys.
func (m *Map[K, V]) DeleteAll(ks ...K) {
	m.mutex.Lock()
	for i := range ks {
		delete(m.m, ks[i])
	}
	m.mutex.Unlock()
}

// DeleteAll will remove the keys.
func (m *Map[K, V]) DeleteAllDeref(ks ...*K) {
	m.mutex.Lock()
	for i := range ks {
		delete(m.m, *ks[i])
	}
	m.mutex.Unlock()
}

func MapAllF[K comparable, V any, U any](m *Map[K, V], f func(map[K]V) U) U {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return f(m.m)
}

// UpdateOrInsert will update the value if it exists, otherwise it will insert the default value.
func (mp *Map[K, V]) UpdateOrInsert(key K, updateFn func(V) V, defaultVal V) V {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	if currentVal, exists := mp.m[key]; exists {
		newVal := updateFn(currentVal)
		mp.m[key] = newVal
		return newVal
	} else {
		newVal := updateFn(defaultVal)
		mp.m[key] = newVal
		return newVal
	}
}

// UpdateOrInsert will update the value if it exists, otherwise it will insert the default value.
func (mp *Map[K, V]) UpdateOrInsertFn(key K, updateFn func(V) V, defaultValFn func() V) V {
	mp.mutex.Lock()
	defer mp.mutex.Unlock()

	if currentVal, exists := mp.m[key]; exists {
		newVal := updateFn(currentVal)
		mp.m[key] = newVal
		return newVal
	} else {
		defaultVal := defaultValFn()
		newVal := updateFn(defaultVal)
		mp.m[key] = newVal
		return newVal
	}
}