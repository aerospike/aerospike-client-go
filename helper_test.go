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
	"net"
	"testing"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
)

func DefaultTimeout() time.Duration {
	return _DEFAULT_TIMEOUT
}

func ParseInfoErrorCode(response string) Error {
	return parseInfoErrorCode(response)
}

func (e *AerospikeError) Msg() string {
	return e.msg
}

func (clstr *Cluster) GetMasterNode(partition *Partition) (*Node, Error) {
	return partition.getMasterNode(clstr)
}

// implements GomegaStringer to avoid some of the pain points
// in formatting the code
func (nd *Node) GomegaString() string {
	return nd.String()
}

func (ptn *Partition) GetMasterNode(cluster *Cluster) (*Node, Error) {
	return ptn.getMasterNode(cluster)
}

func (ptn *Partition) GetMasterProlesNode(cluster *Cluster) (*Node, Error) {
	return ptn.getMasterProlesNode(cluster)
}

// fillMinCounts will fill the connection pool to the minimum required
// by the ClientPolicy.MinConnectionsPerNode
func (nd *Node) ConnsCount() int {
	return nd.connectionCount.Get()
}

// CloseConnections closes all the node connections
func (nd *Node) CloseConnections() {
	nd.closeConnections()
}

// PartitionForWrite returns a partition for write purposes
func ConfiguredAsStrongConsistency(client *Client, namespace string) bool {
	// Must copy hashmap reference for copy on write semantics to work.
	pmap := client.cluster.getPartitions()
	p := pmap[namespace]
	if p == nil {
		return false
	}

	return p.SCMode
}

// TendConnectionIPv4 is the IPv4 of the node's open tend connection.
// Empty when that connection is closed. The L5 driver reports this as the
// snapshot address, and leaves GetHost().Name as the advertised hostname.
func TendConnectionIPv4(node *Node) string {
	if node == nil {
		return ""
	}
	address := ""
	node.tendConn.Do(func(conn *Connection) {
		if conn != nil && conn.IsConnected() {
			address = dottedIPv4(conn.conn.RemoteAddr())
		}
	})
	return address
}

// dottedIPv4 returns the dotted IPv4 form, unwrapping an IPv4-mapped IPv6
// address. A native IPv6 address and a missing address are empty.
func dottedIPv4(addr net.Addr) string {
	tcp, ok := addr.(*net.TCPAddr)
	if !ok || tcp == nil || tcp.IP == nil {
		return ""
	}
	ip := tcp.IP.To4()
	if ip == nil {
		return ""
	}
	return ip.String()
}

// PartitionMasters returns the master node of each partition of namespace in
// the client's partition map, nil where the partition has no master.
func PartitionMasters(client *Client, namespace string) []*Node {
	masters := make([]*Node, _PARTITIONS)
	p := client.cluster.getPartitions()[namespace]
	if p != nil && len(p.Replicas) > 0 {
		copy(masters, p.Replicas[0])
	}
	return masters
}

// Whether nodeName is in any replica list of namespace.
func PartitionContains(client *Client, namespace, nodeName string) bool {
	p := client.cluster.getPartitions()[namespace]
	if p == nil {
		return false
	}
	for _, replicas := range p.Replicas {
		for _, node := range replicas {
			if node != nil && node.GetName() == nodeName {
				return true
			}
		}
	}
	return false
}

// Cumulative command errors and retries, including hosts tend has dropped.
func ClusterCommandCounts(client *Client) (errors, retries int) {
	for _, stats := range client.cluster.statsCopy() {
		errors += stats.TransactionErrorCount.Get()
		retries += stats.TransactionRetryCount.Get()
	}
	return
}

func NewWriteCommand(
	cluster *Cluster,
	policy *WritePolicy,
	key *Key,
	bins []*Bin,
	binMap BinMap) (writeCommand, Error) {
	return newWriteCommand(
		cluster,
		policy,
		key,
		bins,
		binMap,
		_WRITE)
}

func (cmd *writeCommand) WriteBuffer(ifc command) Error {
	return cmd.writeBuffer(ifc)
}

func (cmd *writeCommand) Buffer() []byte {
	return cmd.dataBuffer[:cmd.dataOffset]
}

func NewDeleteCommand(cluster *Cluster, policy *WritePolicy, key *Key) (*deleteCommand, Error) {
	return newDeleteCommand(cluster, policy, key)
}

func (cmd *deleteCommand) WriteBuffer(ifc command) Error {
	return cmd.writeBuffer(ifc)
}

func (cmd *deleteCommand) Buffer() []byte {
	return cmd.dataBuffer[:cmd.dataOffset]
}

func (ctn *Connection) UpdateDeadline() (time.Time, time.Time, time.Duration, Error) {
	err := ctn.updateDeadline()
	return ctn.deadline, ctn.socketDeadline, ctn.socketTimeout, err
}

func (node *Node) ValidateErrorCount() Error {
	return node.validateErrorCount()
}

func (node *Node) IncrementErrorCount() {
	node.incrErrorCount()
}

func (node *Node) SetMaxErrorCount(value int) {
	node.maxErrorCount.Set(value)
}

func (node *Node) GetErrorCount() int {
	return node.errorCount.Get()
}

func NewDynConfigForTest(config *dynconfig.Config) *DynConfig {
	return &DynConfig{
		config: config,
	}
}

func (host *Host) Equals(other *Host) bool {
	return host.equals(other)
}

func NewExpression(e any) *Expression {
	return newExpression(e)
}

// ExecuteSingleBatchOperate calls batchCommandOperate.executeSingle with
// the given records and offsets, bypassing the normal Execute condition.
func ExecuteSingleBatchOperate(client *Client, policy *BatchPolicy, records []BatchRecordIfc, offsets []int) Error {
	batch := &batchNode{offsets: offsets}
	cmd := newBatchCommandOperate(client, batch, policy, records)
	return cmd.executeSingle(client)
}

// ExecuteSingleBatchIndexGet calls batchIndexCommandGet.executeSingle with
// the given records and offsets, bypassing the normal Execute condition.
func ExecuteSingleBatchIndexGet(client *Client, policy *BatchPolicy, records []*BatchRead, offsets []int) Error {
	batch := &batchNode{offsets: offsets}
	cmd := newBatchIndexCommandGet(client, batch, policy, records, false)
	return cmd.executeSingle(client)
}

// ExecuteSingleBatchDelete calls batchCommandDelete.executeSingle with
// the given keys, records, and offsets, bypassing the normal Execute condition.
func ExecuteSingleBatchDelete(client *Client, policy *BatchPolicy, deletePolicy *BatchDeletePolicy, keys []*Key, records []*BatchRecord, offsets []int) Error {
	batch := &batchNode{offsets: offsets}
	cmd := newBatchCommandDelete(client, batch, policy, deletePolicy, keys, records, nil)
	return cmd.executeSingle(client)
}

// ExecuteSingleBatchUDF calls batchCommandUDF.executeSingle with
// the given keys, records, and offsets, bypassing the normal Execute condition.
func ExecuteSingleBatchUDF(client *Client, policy *BatchPolicy, udfPolicy *BatchUDFPolicy, keys []*Key, packageName, functionName string, args []Value, records []*BatchRecord, offsets []int) Error {
	batch := &batchNode{offsets: offsets}
	cmd := newBatchCommandUDF(client, batch, policy, udfPolicy, keys, packageName, functionName, args, records, nil)
	return cmd.executeSingle(client)
}

// RollRecords returns the batch records of the last Roll, so tests can assert
// the roll commands ran rather than inferring it from the commit status.
func (txr *TxnRoll) RollRecords() []*BatchRecord {
	return txr.rollRecords
}

// AlreadyCommitted reports whether the last mark-roll-forward was answered
// with MRT_COMMITTED.
func (txr *TxnRoll) AlreadyCommitted() bool {
	return txr.alreadyCommitted
}

func TestL5TendConnectionIPv4ReadsTheTendSocket(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, acceptErr := ln.Accept()
		if acceptErr == nil {
			accepted <- conn
		}
	}()
	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	server := <-accepted
	defer server.Close()

	node := &Node{}
	if err := node.tendConn.Update(func(slot **Connection) error {
		*slot = &Connection{conn: client}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got := TendConnectionIPv4(node)
	want := server.LocalAddr().(*net.TCPAddr).IP.To4().String()
	if got != want {
		t.Fatalf("address = %q, want %q", got, want)
	}
	if TendConnectionIPv4(nil) != "" {
		t.Fatal("nil node reported an address")
	}
}

func TestL5DottedIPv4UnwrapsAMappedAddress(t *testing.T) {
	if got := dottedIPv4(&net.TCPAddr{IP: net.ParseIP("::ffff:10.1.2.3"), Port: 3000}); got != "10.1.2.3" {
		t.Fatalf("mapped = %q", got)
	}
	if got := dottedIPv4(&net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 3000}); got != "" {
		t.Fatalf("ipv6 = %q", got)
	}
	if got := dottedIPv4(nil); got != "" {
		t.Fatalf("nil = %q", got)
	}
}
