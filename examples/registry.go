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
	"github.com/aerospike/aerospike-client-go/v8/examples/fixtures"
)

// examples is the central registry of documentation examples in given execution order.
var examples = []Example{
	{Name: "put", Run: runPut, Fixture: fixtures.Put()},
	{Name: "get", Run: runGet, Fixture: fixtures.Get()},
	{Name: "simple", Run: runSimple, Fixture: fixtures.Simple()},
	{Name: "add", Run: runAdd, Fixture: fixtures.Add()},
	{Name: "append", Run: runAppend, Fixture: fixtures.Append()},
	{Name: "prepend", Run: runPrepend, Fixture: fixtures.Prepend()},
	{Name: "replace", Run: runReplace, Fixture: fixtures.Replace()},
	{Name: "operate", Run: runOperate, Fixture: fixtures.Operate()},
	{Name: "generation", Run: runGeneration, Fixture: fixtures.Generation()},
	{Name: "blob", Run: runBlob, Fixture: fixtures.Blob()},
	{Name: "batch", Run: runBatch, Fixture: fixtures.Batch()},
	{Name: "expire", Run: runExpire, Fixture: fixtures.Expire(), Requires: TTLSupported()},
	{Name: "touch", Run: runTouch, Fixture: fixtures.Touch(), Requires: TTLSupported()},
	{Name: "scan_serial", Run: runScanSerial, Fixture: fixtures.ScanSerial()},
	{Name: "udf", Run: runUDF, Fixture: fixtures.UDF()},
}
