/*
 * Copyright 2014-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package main

import (
	"bytes"
	"errors"
	"log"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const udfCode = `
local function putBin(r,name,value)
    if not aerospike:exists(r) then aerospike:create(r) end
    r[name] = value
    aerospike:update(r)
end

-- Set a particular bin
function writeBin(r,name,value)
    putBin(r,name,value)
end

-- Get a particular bin
function readBin(r,name)
    return r[name]
end

-- Return generation count of record
function getGeneration(r)
    return record.gen(r)
end

-- Update record only if gen hasn't changed
function writeIfGenerationNotChanged(r,name,value,gen)
    if record.gen(r) == gen then
        r[name] = value
        aerospike:update(r)
    end
end

-- Set a particular bin only if record does not already exist.
function writeUnique(r,name,value)
    if not aerospike:exists(r) then
        aerospike:create(r)
        r[name] = value
        aerospike:update(r)
    end
end

-- Validate value before writing.
function writeWithValidation(r,name,value)
    if (value >= 1 and value <= 10) then
        putBin(r,name,value)
    else
        error("1000:Invalid value")
    end
end
`

// Register a Lua module on the server and invoke its functions on records.
func runUDF() error {
	// Register the UDF module and wait until it reaches all cluster nodes.
	task, err := client.RegisterUDF(nil, []byte(udfCode), "record_example.lua", as.LUA)
	if err != nil {
		return err
	}
	if err := <-task.OnComplete(); err != nil {
		return err
	}

	if err := writeUsingUDF(); err != nil {
		return err
	}
	if err := writeIfGenerationNotChanged(); err != nil {
		return err
	}
	if err := writeIfNotExists(); err != nil {
		return err
	}
	if err := writeWithValidation(); err != nil {
		return err
	}
	if err := writeListMapUsingUDF(); err != nil {
		return err
	}
	return writeBlobUsingUDF()
}

// Write a bin through the Lua function and read it back.
func writeUsingUDF() error {
	key, err := as.NewKey(ns, set, "udfkey1")
	if err != nil {
		return err
	}

	if _, err := client.Execute(nil, key, "record_example", "writeBin",
		as.NewValue("udfbin1"), as.NewValue("string value")); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "udfbin1")
	if err != nil {
		return err
	}
	log.Printf("udfbin1: %v", record.Bins["udfbin1"])
	return nil
}

// Update a record only when its generation has not changed.
func writeIfGenerationNotChanged() error {
	key, err := as.NewKey(ns, set, "udfkey2")
	if err != nil {
		return err
	}

	// Seed the record.
	if err := client.PutBins(nil, key, as.NewBin("udfbin2", "string value")); err != nil {
		return err
	}

	// Get the record generation.
	gen, err := client.Execute(nil, key, "record_example", "getGeneration")
	if err != nil {
		return err
	}

	// Write the record only if the generation has not changed.
	if _, err := client.Execute(nil, key, "record_example", "writeIfGenerationNotChanged",
		as.NewValue("udfbin2"), as.NewValue("string value"), as.NewValue(gen)); err != nil {
		return err
	}
	log.Printf("Record written with matching generation %v.", gen)
	return nil
}

// Write a record only if it does not already exist.
func writeIfNotExists() error {
	key, err := as.NewKey(ns, set, "udfkey3")
	if err != nil {
		return err
	}

	// The first write succeeds because the record does not exist.
	if _, err := client.Execute(nil, key, "record_example", "writeUnique",
		as.NewValue("udfbin3"), as.NewValue("first")); err != nil {
		return err
	}

	record, err := client.Get(nil, key, "udfbin3")
	if err != nil {
		return err
	}
	log.Printf("udfbin3 after first write: %v", record.Bins["udfbin3"])

	// The second write leaves the record unchanged because it already exists.
	if _, err := client.Execute(nil, key, "record_example", "writeUnique",
		as.NewValue("udfbin3"), as.NewValue("second")); err != nil {
		return err
	}

	record, err = client.Get(nil, key, "udfbin3")
	if err != nil {
		return err
	}
	log.Printf("udfbin3 after second write: %v", record.Bins["udfbin3"])
	return nil
}

// Let the Lua function validate values before writing them.
func writeWithValidation() error {
	key, err := as.NewKey(ns, set, "udfkey4")
	if err != nil {
		return err
	}

	// The Lua function accepts numbers between 1 and 10: this write succeeds.
	if _, err := client.Execute(nil, key, "record_example", "writeWithValidation",
		as.NewValue("udfbin4"), as.NewValue(4)); err != nil {
		return err
	}
	log.Printf("Write with valid value succeeded.")

	// A value outside the range makes the UDF raise an error.
	if _, err := client.Execute(nil, key, "record_example", "writeWithValidation",
		as.NewValue("udfbin4"), as.NewValue(11)); err == nil {
		return errors.New("UDF write with invalid value should have failed")
	}
	log.Printf("Write with invalid value rejected, as expected.")
	return nil
}

// Store and read back a nested list/map structure through the UDF.
func writeListMapUsingUDF() error {
	key, err := as.NewKey(ns, set, "udfkey5")
	if err != nil {
		return err
	}

	inner := []any{"string2", int64(8)}
	innerMap := map[any]any{"a": int64(1), int64(2): "b", "list": inner}
	list := []any{"string1", int64(4), inner, innerMap}

	if _, err := client.Execute(nil, key, "record_example", "writeBin",
		as.NewValue("udfbin5"), as.NewValue(list)); err != nil {
		return err
	}

	received, err := client.Execute(nil, key, "record_example", "readBin", as.NewValue("udfbin5"))
	if err != nil {
		return err
	}
	log.Printf("udfbin5: %v", received)
	return nil
}

// Store and read back a byte blob through the UDF.
func writeBlobUsingUDF() error {
	key, err := as.NewKey(ns, set, "udfkey6")
	if err != nil {
		return err
	}

	blob := bytes.Buffer{}
	blob.WriteString("Hello world.")

	if _, err := client.Execute(nil, key, "record_example", "writeBin",
		as.NewValue("udfbin6"), as.NewValue(blob.Bytes())); err != nil {
		return err
	}

	received, err := client.Execute(nil, key, "record_example", "readBin", as.NewValue("udfbin6"))
	if err != nil {
		return err
	}
	log.Printf("udfbin6: %v", received)
	return nil
}
