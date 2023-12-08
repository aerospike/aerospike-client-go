//go:build !as_performance
// +build !as_performance

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

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v7/types"
	Buffer "github.com/aerospike/aerospike-client-go/v7/utils/buffer"
)

// if this file is included in the build, it will include this method
func init() {
	objectParser = parseObject
}

func parseObject(
	cmd *readCommand,
	opCount int,
	fieldCount int,
	generation uint32,
	expiration uint32,
) Error {
	receiveOffset := 0

	// There can be fields in the response (setname etc).
	// But for now, ignore them. Expose them to the API if needed in the future.
	//logger.Logger.Debug("field count: %d, databuffer: %v", fieldCount, cmd.dataBuffer)
	if fieldCount > 0 {
		// Just skip over all the fields
		for i := 0; i < fieldCount; i++ {
			//logger.Logger.Debug("%d", receiveOffset)
			fieldSize := int(Buffer.BytesToUint32(cmd.dataBuffer, receiveOffset))
			receiveOffset += (4 + fieldSize)
		}
	}

	if opCount > 0 {
		rv := *cmd.object

		if rv.Kind() != reflect.Ptr {
			return ErrInvalidObjectType.err()
		}
		rv = rv.Elem()

		if !rv.CanAddr() {
			return ErrInvalidObjectType.err()
		}

		if rv.Kind() != reflect.Struct {
			return ErrInvalidObjectType.err()
		}

		// find the name based on tag mapping
		iobj := indirect(rv)
		mappings := objectMappings.getMapping(iobj.Type())

		if err := setObjectMetaFields(iobj, expiration, generation); err != nil {
			return err
		}

		for i := 0; i < opCount; i++ {
			opSize := int(Buffer.BytesToUint32(cmd.dataBuffer, receiveOffset))
			particleType := int(cmd.dataBuffer[receiveOffset+5])
			nameSize := int(cmd.dataBuffer[receiveOffset+7])
			name := string(cmd.dataBuffer[receiveOffset+8 : receiveOffset+8+nameSize])
			receiveOffset += 4 + 4 + nameSize

			particleBytesSize := opSize - (4 + nameSize)
			value, _ := bytesToParticle(particleType, cmd.dataBuffer, receiveOffset, particleBytesSize)
			if err := setObjectField(mappings, iobj, name, value); err != nil {
				return err
			}

			receiveOffset += particleBytesSize
		}
	}

	return nil
}

func setObjectMetaFields(obj reflect.Value, ttl, gen uint32) Error {
	// find the name based on tag mapping
	iobj := indirect(obj)

	ttlMap, genMap := objectMappings.getMetaMappings(iobj.Type())

	for i := range ttlMap {
		f := iobj.FieldByIndex(ttlMap[i])
		if err := setValue(f, ttl); err != nil {
			return err
		}
	}

	for i := range genMap {
		f := iobj.FieldByIndex(genMap[i])
		if err := setValue(f, gen); err != nil {
			return err
		}
	}

	return nil
}

func setObjectField(mappings map[string][]int, obj reflect.Value, fieldName string, value interface{}) Error {
	if value == nil {
		return nil
	}

	var f reflect.Value

	if index, exists := mappings[fieldName]; exists {
		f = obj.FieldByIndex(index)
	} else {
		f = obj.FieldByName(fieldName)
	}
	return setValue(f, value)
}

func setValue(f reflect.Value, value interface{}) Error {
	// find the name based on tag mapping
	if f.CanSet() {
		if value == nil {
			if f.IsValid() && !f.IsNil() {
				f.Set(reflect.ValueOf(value))
			}
			return nil
		}

		switch fieldKind := f.Kind(); fieldKind {
		case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32,
			reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32:
			v := reflect.ValueOf(value)
			t := f.Type()
			if !v.CanConvert(t) {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
			}

			v = v.Convert(t)
			f.Set(v)
		case reflect.Float64, reflect.Float32:
			switch v := value.(type) {
			case float64:
				f.SetFloat(v)
			case float32:
				f.SetFloat(float64(v))
			case int:
				f.SetFloat(float64(v))
			default:
				return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
			}
		case reflect.String:
			v, ok := value.(string)
			if !ok {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
			}

			rv := reflect.ValueOf(v)
			if rv.Type() != f.Type() {
				rv = rv.Convert(f.Type())
			}
			f.Set(rv)
		case reflect.Bool:
			switch v := value.(type) {
			case int:
				f.SetBool(v == 1)
			case bool:
				f.SetBool(v)
			default:
				return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
			}
		case reflect.Interface:
			if value != nil {
				f.Set(reflect.ValueOf(value))
			}
		case reflect.Ptr:
			switch fieldKind := f.Type().Elem().Kind(); fieldKind {
			case reflect.String:
				tempV, ok := value.(string)
				if !ok {
					return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for *%s field", value, fieldKind))
				}
				rv := reflect.ValueOf(&tempV)
				if rv.Type() != f.Type() {
					rv = rv.Convert(f.Type())
				}
				f.Set(rv)
			case reflect.Int, reflect.Int64, reflect.Int8, reflect.Int16, reflect.Int32,
				reflect.Uint, reflect.Uint64, reflect.Uint8, reflect.Uint16, reflect.Uint32:
				v := reflect.ValueOf(value)
				t := f.Type().Elem()
				if !v.CanConvert(t) {
					return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for *%s field", value, fieldKind))
				}
				v = v.Convert(t)
				if f.IsZero() {
					f.Set(reflect.New(f.Type().Elem()))
				}

				f.Elem().Set(v)
			case reflect.Float64:
				// it is possible that the value is an integer set in the field
				// via the old float<->int64 type cast
				var tempV float64
				if fv, ok := value.(float64); ok {
					tempV = fv
				} else {
					v, ok := value.(int)
					if !ok {
						return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for *%s field", value, fieldKind))
					}
					tempV = math.Float64frombits(uint64(v))
				}

				rv := reflect.ValueOf(&tempV)
				if rv.Type() != f.Type() {
					rv = rv.Convert(f.Type())
				}
				f.Set(rv)
			case reflect.Bool:
				var tempV bool
				switch v := value.(type) {
				case int:
					tempV = v == 1
				case bool:
					tempV = v
				default:
					return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for boolean field", value))
				}

				rv := reflect.ValueOf(&tempV)
				if rv.Type() != f.Type() {
					rv = rv.Convert(f.Type())
				}
				f.Set(rv)
			case reflect.Float32:
				if v, ok := value.(float32); ok {
					value = float64(v)
				}

				// it is possible that the value is an integer set in the field
				// via the old float<->int64 type cast
				var tempV64 float64
				if fv, ok := value.(float64); ok {
					tempV64 = fv
				} else {
					v, ok := value.(int)
					if !ok {
						return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for *%s field", value, fieldKind))
					}
					tempV64 = math.Float64frombits(uint64(v))
				}

				tempV := float32(tempV64)
				rv := reflect.ValueOf(&tempV)
				if rv.Type() != f.Type() {
					rv = rv.Convert(f.Type())
				}
				f.Set(rv)
			case reflect.Interface:
				f.Set(reflect.ValueOf(&value))
			case reflect.Struct:
				// support time.Time
				if f.Type().Elem().PkgPath() == "time" && f.Type().Elem().Name() == "Time" {
					v, ok := value.(int)
					if !ok {
						return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for *%s field", value, fieldKind))
					}
					tm := time.Unix(0, int64(v))
					f.Set(reflect.ValueOf(&tm))
					break
				}
				valMap, ok := value.(map[interface{}]interface{})
				if !ok {
					return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
				}
				// iterate over struct fields and recursively fill them up
				if valMap != nil {
					newObjPtr := f
					if f.IsNil() {
						newObjPtr = reflect.New(f.Type().Elem())
					}

					theStruct := newObjPtr.Elem()
					if err := setStructValue(theStruct, valMap, theStruct.Type(), nil); err != nil {
						return err
					}

					// set the field
					f.Set(newObjPtr)
				}
			} // switch ptr
		case reflect.Slice, reflect.Array:
			// BLOBs come back as []byte
			theArray := reflect.ValueOf(value)
			if theArray.Kind() != reflect.Slice {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
			}

			if f.Kind() == reflect.Slice {
				if f.IsNil() {
					f.Set(reflect.MakeSlice(reflect.SliceOf(f.Type().Elem()), theArray.Len(), theArray.Len()))
				} else if f.Len() < theArray.Len() {
					count := theArray.Len() - f.Len()
					f.Set(reflect.AppendSlice(f, reflect.MakeSlice(reflect.SliceOf(f.Type().Elem()), count, count)))
				}
			}

			for i := 0; i < theArray.Len(); i++ {
				if err := setValue(f.Index(i), theArray.Index(i).Interface()); err != nil {
					return err
				}
			}
		case reflect.Map:
			emptyStruct := reflect.ValueOf(struct{}{})
			theMap, ok := value.(map[interface{}]interface{})
			if !ok {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
			}
			if theMap != nil {
				newMap := reflect.MakeMap(f.Type())
				var newKey, newVal reflect.Value
				for key, elem := range theMap {
					fKeyType := f.Type().Key()
					if key != nil {
						newKey = reflect.ValueOf(key)
					} else {
						newKey = reflect.Zero(fKeyType)
					}

					if newKey.Type() != fKeyType {
						if !newKey.CanConvert(fKeyType) {
							return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid key `%#v` for %s field", value, fieldKind))
						}
						newKey = newKey.Convert(fKeyType)
					}

					fElemType := f.Type().Elem()
					if elem != nil {
						newVal = reflect.ValueOf(elem)
					} else {
						newVal = reflect.Zero(fElemType)
					}

					if newVal.Type() != fElemType {
						switch newVal.Kind() {
						case reflect.Map, reflect.Slice, reflect.Array:
							newVal = reflect.New(fElemType)
							if err := setValue(newVal.Elem(), elem); err != nil {
								return err
							}
							newVal = reflect.Indirect(newVal)
						default:
							if !newVal.CanConvert(fElemType) {
								return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
							}
							newVal = newVal.Convert(fElemType)
						}
					}

					if newVal.Kind() == reflect.Map && newVal.Len() == 0 && newMap.Type().Elem().Kind() == emptyStruct.Type().Kind() {
						if newMap.Type().Elem().NumField() == 0 {
							newMap.SetMapIndex(newKey, emptyStruct)
						} else {
							return newError(types.PARSE_ERROR, "Map value type is struct{}, but data returned from database is a non-empty map[interface{}]interface{}")
						}
					} else {
						newMap.SetMapIndex(newKey, newVal)
					}
				}
				f.Set(newMap)
			}

		case reflect.Struct:
			// support time.Time
			if f.Type().PkgPath() == "time" && f.Type().Name() == "Time" {
				v, ok := value.(int)
				if !ok {
					return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for time %s field", value, fieldKind))
				}
				f.Set(reflect.ValueOf(time.Unix(0, int64(v))))
				break
			}

			valMap, ok := value.(map[interface{}]interface{})
			if !ok {
				return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid value `%#v` for %s field", value, fieldKind))
			}
			// iterate over struct fields and recursively fill them up
			if err := setStructValue(f, valMap, f.Type(), nil); err != nil {
				return err
			}

			// set the field
			f.Set(f)
		}
	}

	return nil
}

func setStructValue(f reflect.Value, valMap map[interface{}]interface{}, typeOfT reflect.Type, index []int) (err Error) {
	numFields := typeOfT.NumField()
	for i := 0; i < numFields; i++ {
		fld := typeOfT.Field(i)
		fldIndex := append(index, fld.Index...)
		if fld.Anonymous && fld.Type.Kind() == reflect.Struct {
			if err := setStructValue(f, valMap, fld.Type, fldIndex); err != nil {
				return err
			}
			continue
		}

		if fld.PkgPath != "" {
			continue
		}

		alias := fld.Name
		tag := strings.Trim(stripOptions(fld.Tag.Get(aerospikeTag)), " ")
		if tag != "" {
			alias = tag
		}

		if valMap[alias] != nil {
			if err := setValue(f.FieldByIndex(fldIndex), valMap[alias]); err != nil {
				return err
			}
		}
	}
	return nil
}
