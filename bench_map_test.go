package aerospike

import (
	"testing"

	amap "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
)

func BenchmarkCloneMap(b *testing.B) {
	m := amap.New[commandType, int](1)
	commands := []commandType{ttPut, ttBatchRead, ttDelete, ttGet, ttQuery, ttScan, ttExists, ttNone, ttUDF, ttBatchRead, ttBatchWrite}
	for i := 0; i < 20000; i++ {
		command := commands[i%len(commands)]
		m.Set(command, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.CloneMap()
	}
}

func BenchmarkCloneAndResetMap(b *testing.B) {
	m := amap.New[commandType, int](1)
	commands := []commandType{ttPut, ttBatchRead, ttDelete, ttGet, ttQuery, ttScan, ttExists, ttNone, ttUDF, ttBatchRead, ttBatchWrite}
	for i := 0; i < 20000; i++ {
		command := commands[i%len(commands)]
		m.Set(command, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.CloneAndResetMap()
	}
}

func BenchmarkClone(b *testing.B) {
	m := amap.New[commandType, int](1)
	commands := []commandType{ttPut, ttBatchRead, ttDelete, ttGet, ttQuery, ttScan, ttExists, ttNone, ttUDF, ttBatchRead, ttBatchWrite}
	for i := 0; i < 20000; i++ {
		command := commands[i%len(commands)]
		m.Set(command, i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Clone()
	}
}
