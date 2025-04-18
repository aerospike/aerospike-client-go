package aerospike

import "sync"

var binMapPool = sync.Pool{
	New: func() interface{} {
		return make(BinMap, 16)
	},
}

var mapPool = sync.Pool{
	New: func() interface{} {
		return make(map[interface{}]interface{}, 8)
	},
}

var listPool = sync.Pool{
	New: func() interface{} {
		return make([]interface{}, 0, 8)
	},
}

func freeValue(i any) {
	switch v := i.(type) {
	case map[interface{}]interface{}:
		freeMap(v)
	case []interface{}:
		freeList(v)
	}
}

func freeMap(m map[interface{}]interface{}) {
	if m == nil {
		return
	}

	for k := range m {
		freeValue(m[k])
		delete(m, k)
	}

	mapPool.Put(m)
}

func freeList(l []interface{}) {
	for k := range l {
		freeValue(l[k])
	}
	l = l[:0]
	listPool.Put(l)
}
