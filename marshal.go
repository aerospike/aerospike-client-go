// Copyright 2013-2014 Aerospike, Inc.
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

import (
	"math"
	"reflect"
	"strings"
	"sync"
	"time"
)

const (
	aerospikeTag = "as"
	keyTag       = "key"
)

func valueToInterface(f reflect.Value) interface{} {
	// get to the core value
	for f.Kind() == reflect.Ptr {
		if f.IsNil() {
			return nil
		}
		f = reflect.Indirect(f)
	}

	switch f.Kind() {
	case reflect.Uint64:
		return int64(f.Uint())
	case reflect.Float64, reflect.Float32:
		return int(math.Float64bits(f.Float()))
	case reflect.Struct:
		if f.Type().PkgPath() == "time" && f.Type().Name() == "Time" {
			return f.Interface().(time.Time).UTC().UnixNano()
		} else {
			return structToMap(f)
		}
	case reflect.Bool:
		if f.Bool() == true {
			return int64(1)
		}
		return int64(0)
	case reflect.Map, reflect.Slice, reflect.Interface:
		if f.IsNil() {
			return nil
		}
		return f.Interface()
	default:
		return f.Interface()
	}
}

func fieldAlias(f reflect.StructField) string {
	alias := f.Tag.Get(aerospikeTag)
	if alias != "" {
		alias = strings.Trim(alias, " ")

		// if tag is -, the field should not be persisted
		if alias == "-" {
			return ""
		}
		return alias
	} else {
		return f.Name
	}
}

func structToMap(s reflect.Value) map[string]interface{} {
	if !s.IsValid() {
		return nil
	}

	// map tags
	cacheObjectTags(s)

	typeOfT := s.Type()
	numFields := s.NumField()

	var binMap map[string]interface{}
	for i := 0; i < numFields; i++ {
		// skip unexported fields
		if typeOfT.Field(i).PkgPath != "" {
			continue
		}

		binValue := valueToInterface(s.Field(i))

		if binValue != nil {
			if binMap == nil {
				binMap = make(map[string]interface{}, numFields)
			}

			alias := fieldAlias(typeOfT.Field(i))
			if alias == "" {
				continue
			}

			binMap[alias] = binValue
		}
	}

	return binMap
}

func marshal(v interface{}) []*Bin {
	s := reflect.Indirect(reflect.ValueOf(v).Elem())
	typeOfT := s.Type()

	// map tags
	cacheObjectTags(s)

	numFields := s.NumField()
	bins := binPool.Get(numFields).([]*Bin)

	binCount := 0
	for i := 0; i < numFields; i++ {
		// skip unexported fields
		if typeOfT.Field(i).PkgPath != "" {
			continue
		}

		binValue := valueToInterface(s.Field(i))

		if binValue != nil {
			alias := fieldAlias(typeOfT.Field(i))
			if alias == "" {
				continue
			}

			bins[binCount].Name = alias
			bins[binCount].Value = NewValue(binValue)
			binCount++
		}
	}

	return bins[:binCount]
}

type SyncMap struct {
	objectMappings map[string]map[string]string
	objectFields   map[string][]string
	mutex          sync.RWMutex
}

func (sm *SyncMap) setMapping(objType string, mapping map[string]string, fields []string) {
	sm.mutex.Lock()
	sm.objectMappings[objType] = mapping
	sm.mutex.Unlock()
}

func (sm *SyncMap) mappingExists(objType string) bool {
	sm.mutex.RLock()
	_, exists := sm.objectMappings[objType]
	sm.mutex.RUnlock()
	return exists
}

func (sm *SyncMap) getMapping(objType string) map[string]string {
	sm.mutex.RLock()
	mapping := sm.objectMappings[objType]
	sm.mutex.RUnlock()
	return mapping
}

func (sm *SyncMap) getFields(objType string) []string {
	sm.mutex.RLock()
	fields := sm.objectFields[objType]
	sm.mutex.RUnlock()
	return fields
}

var objectMappings = &SyncMap{objectMappings: map[string]map[string]string{}, objectFields: map[string][]string{}}

func cacheObjectTags(obj reflect.Value) {
	objType := obj.Type().Name()
	// exit if already processed
	if objectMappings.mappingExists(objType) {
		return
	}

	mapping := map[string]string{}
	fields := []string{}

	typeOfT := obj.Type()
	numFields := obj.NumField()
	for i := 0; i < numFields; i++ {
		f := typeOfT.Field(i)
		// skip unexported fields
		if f.PkgPath != "" {
			continue
		}

		tag := strings.Trim(f.Tag.Get(aerospikeTag), " ")
		if tag != "-" {
			if tag != "" {
				mapping[tag] = f.Name
				fields = append(fields, tag)
			} else {
				fields = append(fields, f.Name)
			}
		}
	}

	objectMappings.setMapping(objType, mapping, fields)
}
