// Copyright 2014-2026 Aerospike, Inc.
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
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// GetSink reads a record and delivers each bin to the supplied BinReceiver
// without allocating a BinMap and without using reflection.
//
// If sink implements BinReceiverNames, only those bins are requested from
// the server; otherwise all bins are requested. If sink implements
// BinReceiverMetadata, the record's generation and expiration are delivered
// before any bin callback.
func (clnt *Client) GetSink(policy *BasePolicy, key *Key, sink BinReceiver) Error {
	policy = clnt.getUsablePolicy(policy)

	if policy.Txn != nil {
		if err := policy.Txn.prepareRead(key.namespace); err != nil {
			return err
		}
	}

	var binNames []string
	if n, ok := sink.(BinReceiverNames); ok {
		binNames = n.AerospikeBinNames()
	}

	command, err := newReadCommand(clnt.cluster, policy, key, binNames)
	if err != nil {
		return err
	}

	command.sink = sink
	return command.Execute()
}

// GetSinkHeader reads only a record's generation and expiration and
// delivers them to the sink via AerospikeMetadata. Bins are not read. The
// sink must implement BinReceiverMetadata; otherwise the call is a no-op
// past the network round trip.
func (clnt *Client) GetSinkHeader(policy *BasePolicy, key *Key, sink BinReceiver) Error {
	rec, err := clnt.GetHeader(policy, key)
	if err != nil {
		return err
	}
	if rec == nil {
		return ErrKeyNotFound.err()
	}
	if meta, ok := sink.(BinReceiverMetadata); ok {
		return meta.AerospikeMetadata(rec.Generation, rec.Expiration)
	}
	return nil
}

// BatchGetSink reads records for the given keys and delivers each record's
// bins to the matching BinReceiver in `sinks`. The two slices must have
// equal length; entry i of `sinks` receives the bins for key i. A nil
// entry in `sinks` causes that record's bins to be discarded.
//
// The returned `found` slice mirrors `sinks` — entry i is true iff that
// record existed on the server.
//
// The union of all bins referenced by the sinks' AerospikeBinNames
// (if implemented) is requested from the server; sinks that do not
// implement BinReceiverNames cause all bins to be requested.
func (clnt *Client) BatchGetSink(policy *BatchPolicy, keys []*Key, sinks []BinReceiver) (found []bool, err Error) {
	policy = clnt.getUsableBatchPolicy(policy)

	if len(keys) != len(sinks) {
		return nil, newError(types.PARAMETER_ERROR, "wrong number of arguments to BatchGetSink: number of keys and sinks do not match")
	}
	if len(keys) == 0 {
		return nil, newError(types.PARAMETER_ERROR, "wrong number of arguments to BatchGetSink: keys are empty")
	}

	binNames := unionBinNames(sinks)

	sinksFound := make([]bool, len(keys))
	cmd := newBatchCommandGet(clnt, nil, policy, keys, binNames, nil, nil, _INFO1_READ, false)
	cmd.sinks = sinks
	cmd.sinksFound = sinksFound

	batchNodes, err := newBatchNodeList(clnt.cluster, policy, keys, nil, false)
	if err != nil {
		return nil, err
	}

	filteredOut, err := clnt.batchExecute(policy, batchNodes, cmd)
	if err != nil {
		return nil, err
	}

	if filteredOut > 0 {
		err = chainErrors(ErrFilteredOut.err(), err)
	}

	return sinksFound, err
}

// ScanAllSink streams every record in the given namespace/set into fresh
// BinReceivers produced by the factory. Each populated receiver is sent
// on the sinks channel; the channel is closed when the scan completes.
//
// If binNames is empty and the factory's first product implements
// BinReceiverNames, that allowlist is used; otherwise all bins are
// requested. The sinks channel is the caller's; the library writes to it
// and closes it on completion.
func (clnt *Client) ScanAllSink(
	apolicy *ScanPolicy,
	sinks chan BinReceiver,
	factory func() BinReceiver,
	namespace string,
	setName string,
	binNames ...string,
) (*Recordset, Error) {
	if factory == nil {
		return nil, newError(types.PARAMETER_ERROR, "ScanAllSink: factory must not be nil")
	}
	if sinks == nil {
		return nil, newError(types.PARAMETER_ERROR, "ScanAllSink: sinks channel must not be nil")
	}

	policy := *clnt.getUsableScanPolicy(apolicy)
	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, newError(types.SERVER_NOT_AVAILABLE, "Scan failed because cluster is empty.")
	}

	if len(binNames) == 0 {
		binNames = sinkFactoryBinNames(factory)
	}

	tracker := newPartitionTrackerForNodes(&policy.MultiPolicy, nodes)
	rs := newSinkRecordset(sinks, factory, 1)
	go clnt.scanPartitions(&policy, tracker, namespace, setName, rs, binNames...)
	return rs, nil
}

// QuerySink executes a query and streams every matching record into
// fresh BinReceivers produced by the factory. Each populated receiver is
// sent on the sinks channel; the channel is closed on completion.
func (clnt *Client) QuerySink(
	policy *QueryPolicy,
	statement *Statement,
	sinks chan BinReceiver,
	factory func() BinReceiver,
) (*Recordset, Error) {
	if factory == nil {
		return nil, newError(types.PARAMETER_ERROR, "QuerySink: factory must not be nil")
	}
	if sinks == nil {
		return nil, newError(types.PARAMETER_ERROR, "QuerySink: sinks channel must not be nil")
	}

	policy = clnt.getUsableQueryPolicy(policy)
	nodes := clnt.cluster.GetNodes()
	if len(nodes) == 0 {
		return nil, newError(types.SERVER_NOT_AVAILABLE, "Query failed because cluster is empty.")
	}

	// If the user hasn't already pinned the statement's bin names and
	// the factory has a BinReceiverNames opinion, honor it.
	if len(statement.BinNames) == 0 {
		if names := sinkFactoryBinNames(factory); len(names) > 0 {
			statement.BinNames = names
		}
	}

	tracker := newPartitionTrackerForNodes(&policy.MultiPolicy, nodes)
	rs := newSinkRecordset(sinks, factory, 1)
	go clnt.queryPartitions(policy, tracker, statement, rs)
	return rs, nil
}

// sinkFactoryBinNames builds a sample sink from the factory and consults
// it for AerospikeBinNames. The sample is discarded.
func sinkFactoryBinNames(factory func() BinReceiver) []string {
	sample := factory()
	if n, ok := sample.(BinReceiverNames); ok {
		return n.AerospikeBinNames()
	}
	return nil
}

// unionBinNames returns the set union of all sinks' AerospikeBinNames.
// If any sink does not implement BinReceiverNames, an empty slice is
// returned, signalling "all bins."
func unionBinNames(sinks []BinReceiver) []string {
	binSet := map[string]struct{}{}
	for _, s := range sinks {
		if s == nil {
			continue
		}
		nm, ok := s.(BinReceiverNames)
		if !ok {
			return nil
		}
		for _, bn := range nm.AerospikeBinNames() {
			binSet[bn] = struct{}{}
		}
	}
	if len(binSet) == 0 {
		return nil
	}
	out := make([]string, 0, len(binSet))
	for bn := range binSet {
		out = append(out, bn)
	}
	return out
}
