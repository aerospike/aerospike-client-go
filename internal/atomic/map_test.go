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

package atomic_test

import (
	"runtime"
	"sync"

	atomicmap "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"

	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

var _ = gg.Describe("Atomic Map", func() {
	// atomic tests require actual parallelism
	runtime.GOMAXPROCS(runtime.NumCPU())

	var m *atomicmap.Map[string, int]

	gg.BeforeEach(func() {
		m = atomicmap.New[string, int](0)
	})

	gg.Describe("Constructor Tests", func() {
		gg.It("should create new map with capacity", func() {
			newMap := atomicmap.New[string, int](10)
			gm.Expect(newMap).ToNot(gm.BeNil())
			gm.Expect(newMap.Length()).To(gm.Equal(0))
		})

		gg.It("should create new map with zero capacity", func() {
			newMap := atomicmap.New[string, int](0)
			gm.Expect(newMap).ToNot(gm.BeNil())
			gm.Expect(newMap.Length()).To(gm.Equal(0))
		})

		gg.It("should create new map with initial value", func() {
			newMap := atomicmap.NewWithValue("key1", 42)
			gm.Expect(newMap).ToNot(gm.BeNil())
			gm.Expect(newMap.Length()).To(gm.Equal(1))
			gm.Expect(newMap.Exists("key1")).To(gm.BeTrue())
			gm.Expect(newMap.Get("key1")).To(gm.Equal(42))
		})

		gg.It("should create new map with int key", func() {
			newMap := atomicmap.NewWithValue(123, "value")
			gm.Expect(newMap).ToNot(gm.BeNil())
			gm.Expect(newMap.Length()).To(gm.Equal(1))
			gm.Expect(newMap.Exists(123)).To(gm.BeTrue())
			gm.Expect(newMap.Get(123)).To(gm.Equal("value"))
		})
	})

	gg.Describe("Basic Operations", func() {
		gg.It("should get zero value for non-existent key", func() {
			val := m.Get("nonexistent")
			gm.Expect(val).To(gm.Equal(0))
		})

		gg.It("should get existing value", func() {
			m.Set("key1", 42)
			val := m.Get("key1")
			gm.Expect(val).To(gm.Equal(42))
		})

		gg.It("should set new key", func() {
			m.Set("key1", 42)
			gm.Expect(m.Exists("key1")).To(gm.BeTrue())
			gm.Expect(m.Get("key1")).To(gm.Equal(42))
		})

		gg.It("should update existing key", func() {
			m.Set("key1", 42)
			m.Set("key1", 100)
			gm.Expect(m.Get("key1")).To(gm.Equal(100))
		})

		gg.It("should return false for non-existent key", func() {
			gm.Expect(m.Exists("nonexistent")).To(gm.BeFalse())
		})

		gg.It("should return true for existing key", func() {
			m.Set("key1", 42)
			gm.Expect(m.Exists("key1")).To(gm.BeTrue())
		})

		gg.It("should delete non-existent key and return zero value", func() {
			val := m.Delete("nonexistent")
			gm.Expect(val).To(gm.Equal(0))
		})

		gg.It("should delete existing key and return its value", func() {
			m.Set("key1", 42)
			val := m.Delete("key1")
			gm.Expect(val).To(gm.Equal(42))
			gm.Expect(m.Exists("key1")).To(gm.BeFalse())
		})

		gg.It("should delete key by reference", func() {
			m.Set("key1", 42)
			key := "key1"
			val := m.DeleteDeref(&key)
			gm.Expect(val).To(gm.Equal(42))
			gm.Expect(m.Exists("key1")).To(gm.BeFalse())
		})
	})

	gg.Describe("Bulk Operations", func() {
		gg.It("should replace with new map", func() {
			m.Set("key1", 42)
			m.Set("key2", 100)

			newMap := map[string]int{
				"key3": 200,
				"key4": 300,
			}

			m.Replace(newMap)

			gm.Expect(m.Length()).To(gm.Equal(2))
			gm.Expect(m.Exists("key1")).To(gm.BeFalse())
			gm.Expect(m.Exists("key2")).To(gm.BeFalse())
			gm.Expect(m.Exists("key3")).To(gm.BeTrue())
			gm.Expect(m.Exists("key4")).To(gm.BeTrue())
			gm.Expect(m.Get("key3")).To(gm.Equal(200))
			gm.Expect(m.Get("key4")).To(gm.Equal(300))
		})

		gg.It("should replace with empty map", func() {
			m.Set("key1", 42)
			m.Replace(map[string]int{})
			gm.Expect(m.Length()).To(gm.Equal(0))
		})

		gg.It("should clear all entries", func() {
			m.Set("key1", 42)
			m.Set("key2", 100)
			m.Clear()
			gm.Expect(m.Length()).To(gm.Equal(0))
			gm.Expect(m.Exists("key1")).To(gm.BeFalse())
			gm.Expect(m.Exists("key2")).To(gm.BeFalse())
		})

		gg.It("should delete multiple keys", func() {
			m.Set("key1", 42)
			m.Set("key2", 100)
			m.Set("key3", 200)
			m.DeleteAll("key1", "key3")
			gm.Expect(m.Length()).To(gm.Equal(1))
			gm.Expect(m.Exists("key1")).To(gm.BeFalse())
			gm.Expect(m.Exists("key3")).To(gm.BeFalse())
			gm.Expect(m.Exists("key2")).To(gm.BeTrue())
		})

		gg.It("should delete multiple keys by reference", func() {
			m.Set("key1", 42)
			m.Set("key2", 100)
			m.Set("key3", 200)
			key1 := "key1"
			key3 := "key3"
			m.DeleteAllDeref(&key1, &key3)
			gm.Expect(m.Length()).To(gm.Equal(1))
			gm.Expect(m.Exists("key1")).To(gm.BeFalse())
			gm.Expect(m.Exists("key3")).To(gm.BeFalse())
			gm.Expect(m.Exists("key2")).To(gm.BeTrue())
		})
	})

	gg.Describe("Clone Operations", func() {
		gg.BeforeEach(func() {
			m.Set("key1", 42)
			m.Set("key2", 100)
		})

		gg.It("should clone to regular map", func() {
			cloned := m.Clone()
			gm.Expect(len(cloned)).To(gm.Equal(2))
			gm.Expect(cloned["key1"]).To(gm.Equal(42))
			gm.Expect(cloned["key2"]).To(gm.Equal(100))

			cloned["key1"] = 999
			gm.Expect(m.Get("key1")).To(gm.Equal(42))
		})

		gg.It("should clone to new Map instance", func() {
			cloned := m.CloneMap()
			gm.Expect(cloned.Length()).To(gm.Equal(2))
			gm.Expect(cloned.Get("key1")).To(gm.Equal(42))
			gm.Expect(cloned.Get("key2")).To(gm.Equal(100))

			cloned.Set("key1", 999)
			gm.Expect(m.Get("key1")).To(gm.Equal(42))
		})

		gg.It("should clone and reset original map", func() {
			cloned := m.CloneAndResetMap()
			gm.Expect(cloned.Length()).To(gm.Equal(2))
			gm.Expect(cloned.Get("key1")).To(gm.Equal(42))
			gm.Expect(cloned.Get("key2")).To(gm.Equal(100))

			gm.Expect(m.Length()).To(gm.Equal(0))
			gm.Expect(m.Exists("key1")).To(gm.BeFalse())
			gm.Expect(m.Exists("key2")).To(gm.BeFalse())
		})
	})

	gg.Describe("Utility Functions", func() {
		gg.It("should return correct length", func() {
			gm.Expect(m.Length()).To(gm.Equal(0))
			m.Set("key1", 42)
			gm.Expect(m.Length()).To(gm.Equal(1))
			m.Set("key2", 100)
			gm.Expect(m.Length()).To(gm.Equal(2))
			m.Delete("key1")
			gm.Expect(m.Length()).To(gm.Equal(1))
		})

		gg.It("should return all keys", func() {
			keys := m.Keys()
			gm.Expect(len(keys)).To(gm.Equal(0))

			m.Set("key1", 42)
			m.Set("key2", 100)
			m.Set("key3", 200)

			keys = m.Keys()
			gm.Expect(len(keys)).To(gm.Equal(3))

			keyMap := make(map[string]bool)
			for _, k := range keys {
				keyMap[k] = true
			}
			gm.Expect(keyMap["key1"]).To(gm.BeTrue())
			gm.Expect(keyMap["key2"]).To(gm.BeTrue())
			gm.Expect(keyMap["key3"]).To(gm.BeTrue())
		})

		gg.It("should apply function to map", func() {
			m.Set("key1", 42)
			m.Set("key2", 100)

			sum := atomicmap.MapAllF(m, func(m map[string]int) int {
				total := 0
				for _, v := range m {
					total += v
				}
				return total
			})
			gm.Expect(sum).To(gm.Equal(142))

			count := atomicmap.MapAllF(m, func(m map[string]int) int {
				return len(m)
			})
			gm.Expect(count).To(gm.Equal(2))
		})

		gg.It("should update or insert existing key", func() {
			m.Set("key1", 42)
			result := m.UpdateOrInsert("key1", func(v int) int { return v * 2 }, 0)
			gm.Expect(result).To(gm.Equal(84))
			gm.Expect(m.Get("key1")).To(gm.Equal(84))
		})

		gg.It("should update or insert non-existing key", func() {
			result := m.UpdateOrInsert("key2", func(v int) int { return v + 10 }, 5)
			gm.Expect(result).To(gm.Equal(15))
			gm.Expect(m.Get("key2")).To(gm.Equal(15))
		})

		gg.It("should update or insert with function", func() {
			m.Set("key1", 42)
			result := m.UpdateOrInsertFn("key1", func(v int) int { return v * 2 }, func() int { return 0 })
			gm.Expect(result).To(gm.Equal(84))
			gm.Expect(m.Get("key1")).To(gm.Equal(84))

			result = m.UpdateOrInsertFn("key2", func(v int) int { return v + 10 }, func() int { return 5 })
			gm.Expect(result).To(gm.Equal(15))
			gm.Expect(m.Get("key2")).To(gm.Equal(15))
		})
	})

	gg.Describe("Concurrent Access", func() {
		gg.It("should handle concurrent writes", func() {
			const numGoroutines = 100
			const numOperations = 1000
			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						key := string(rune('a' + (id % 26)))
						m.Set(key, j)
					}
				}(i)
			}

			wg.Wait()
			gm.Expect(m.Length()).To(gm.BeNumerically(">", 0))
		})

		gg.It("should handle concurrent reads and writes", func() {
			m.Clear()
			const numGoroutines = 100
			const numOperations = 1000
			var wg sync.WaitGroup
			wg.Add(numGoroutines * 2)

			// Write
			for i := 0; i < numGoroutines; i++ {
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						key := string(rune('a' + (id % 26)))
						m.Set(key, j)
					}
				}(i)
			}

			// Read
			for i := 0; i < numGoroutines; i++ {
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						key := string(rune('a' + (id % 26)))
						m.Get(key)
						m.Exists(key)
					}
				}(i)
			}

			wg.Wait()
		})

		gg.It("should handle concurrent deletes", func() {
			for i := 0; i < 100; i++ {
				key := string(rune('a' + (i % 26)))
				m.Set(key, i)
			}

			const numGoroutines = 100
			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func(id int) {
					defer wg.Done()
					for j := 0; j < 10; j++ {
						key := string(rune('a' + (id % 26)))
						m.Delete(key)
					}
				}(i)
			}

			wg.Wait()
		})

		gg.It("should handle race conditions with mixed operations", func() {
			const numGoroutines = 10
			const numOperations = 1000
			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for i := 0; i < numGoroutines; i++ {
				go func(id int) {
					defer wg.Done()
					for j := 0; j < numOperations; j++ {
						key := string(rune('a' + (id % 26)))

						// Mix operations
						switch j % 4 {
						case 0:
							m.Set(key, j)
						case 1:
							m.Get(key)
						case 2:
							m.Exists(key)
						case 3:
							m.Delete(key)
						}
					}
				}(i)
			}

			wg.Wait()
		})
	})

	gg.Describe("Performance Tests", func() {
		gg.It("should perform well under load", func() {
			const numOperations = 100000

			for i := 0; i < numOperations; i++ {
				m.Set("key", i)
			}

			for i := 0; i < numOperations; i++ {
				m.Get("key")
			}

			for i := 0; i < numOperations; i++ {
				m.Exists("key")
			}

			for i := 0; i < numOperations; i++ {
				m.Set("key", i)
				m.Delete("key")
			}
		})
	})
})
