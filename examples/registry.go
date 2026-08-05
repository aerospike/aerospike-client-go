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
	{Name: "connect_basic", Run: runConnectBasic},
	{Name: "connect_auth", Run: runConnectAuth},
	{Name: "connect_tls", Run: runConnectTLS, Requires: TLSConfigured()},
	{Name: "connect_tls_pki", Run: runConnectTLSPKI, Fixture: fixtures.Connect(),
		Requires: TLSConfigured().AndEnterpriseEdition().AndSecurityEnabled()},
	{Name: "put", Run: runPut, Fixture: fixtures.Put()},
	{Name: "get", Run: runGet, Fixture: fixtures.Get()},
	{Name: "simple", Run: runSimple, Fixture: fixtures.Simple()},
	{Name: "add", Run: runAdd, Fixture: fixtures.Add()},
	{Name: "append", Run: runAppend, Fixture: fixtures.Append()},
	{Name: "prepend", Run: runPrepend, Fixture: fixtures.Prepend()},
	{Name: "replace", Run: runReplace, Fixture: fixtures.Replace()},
	{Name: "operate", Run: runOperate, Fixture: fixtures.Operate()},
	{Name: "generation", Run: runGeneration, Fixture: fixtures.Generation(), Requires: TTLSupported()},
	{Name: "blob", Run: runBlob, Fixture: fixtures.Blob()},
	{Name: "list_map", Run: runListMap, Fixture: fixtures.ListMap()},
	{Name: "custom_list_iter", Run: runCustomListIter, Fixture: fixtures.ListIter(ll)},
	{Name: "batch", Run: runBatch, Fixture: fixtures.Batch()},
	{Name: "expire", Run: runExpire, Fixture: fixtures.Expire(), Requires: TTLSupported()},
	{Name: "touch", Run: runTouch, Fixture: fixtures.Touch(), Requires: TTLSupported()},
	{Name: "scan_serial", Run: runScanSerial, Fixture: fixtures.ScanSerial()},
	{Name: "scan_parallel", Run: runScanParallel, Fixture: fixtures.ScanParallel()},
	{Name: "scan_paginate", Run: runScanPaginate, Fixture: fixtures.ScanPaginate()},
	{Name: "count_set_objects", Run: runCountSetObjects, Fixture: fixtures.CountSetObjects()},
	{Name: "expressions", Run: runExpressions, Fixture: fixtures.Expressions()},
	{Name: "geojson_query", Run: runGeoJSONQuery, Fixture: fixtures.GeoJSONQuery()},
	{Name: "query_aggregate_average", Run: runQueryAggregateAverage, Fixture: fixtures.QueryAggregateAverage()},
	{Name: "query_aggregate_sum", Run: runQueryAggregateSum, Fixture: fixtures.QueryAggregateSum()},
	{Name: "udf", Run: runUDF, Fixture: fixtures.UDF()},
	{Name: "query", Run: runQuery, Fixture: fixtures.Query()},
	{Name: "operate_list", Run: runOperateList, Fixture: fixtures.OperateList()},
	{Name: "operate_map", Run: runOperateMap, Fixture: fixtures.OperateMap()},
	{Name: "info", Run: runInfo, Fixture: fixtures.Info()},
	{Name: "txn_basic", Run: runTxnBasic, Fixture: fixtures.TxnBasic(),
		Requires: EnterpriseEdition().AndStrongConsistency().AndMinServerVersion(8, 0)},
	{Name: "txn_concurrent", Run: runTxnConcurrent,
		Fixture:  fixtures.TxnConcurrent(keyRange, batchIterations*mixedBatchSize, queryDataSize),
		Requires: EnterpriseEdition().AndStrongConsistency().AndMinServerVersion(8, 0)},
	{Name: "tls_secure_connection", Run: runTLSSecureConnection, Requires: TLSConfigured()},
	{Name: "pki_auth", Run: runPKIAuth, Fixture: fixtures.PKIAuth(),
		Requires: TLSConfigured().AndEnterpriseEdition().AndSecurityEnabled()},
	{Name: "pki_auth_roles", Run: runPKIAuthRoles, Fixture: fixtures.PKIAuthRoles(),
		Requires: TLSConfigured().AndEnterpriseEdition().AndSecurityEnabled()},
}
