//go:build as_proxy

package aerospike

import (
	"context"
	"math/rand"
	"time"

	kvs "github.com/aerospike/aerospike-client-go/v7/proto/kvs"
	"github.com/aerospike/aerospike-client-go/v7/types"
)

func (fltr *Filter) grpc() *kvs.Filter {
	if fltr == nil {
		return nil
	}

	res := &kvs.Filter{
		Name:      fltr.name,
		ColType:   fltr.idxType.grpc(),
		PackedCtx: fltr.grpcPackCtxPayload(),
		ValType:   int32(fltr.valueParticleType),
		Begin:     grpcValuePacked(fltr.begin),
		End:       grpcValuePacked(fltr.end),
	}

	return res
}

///////////////////////////////////////////////////////////////////

var simpleCancelFunc = func() {}

func (p *InfoPolicy) grpcDeadlineContext() (context.Context, context.CancelFunc) {
	timeout := p.Timeout
	if timeout <= 0 {
		return context.Background(), simpleCancelFunc

	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return ctx, cancel
}

func (p *InfoPolicy) grpc() *kvs.InfoPolicy {
	if p == nil {
		return nil
	}

	Timeout := uint32(p.Timeout / time.Millisecond)
	res := &kvs.InfoPolicy{
		Timeout: &Timeout,
	}

	return res
}

///////////////////////////////////////////////////////////////////

func (op *Operation) grpc() *kvs.Operation {
	BinName := op.binName
	return &kvs.Operation{
		Type:    op.grpc_op_type(),
		BinName: &BinName,
		Value:   grpcValuePacked(op.binValue),
	}
}

///////////////////////////////////////////////////////////////////

func (pf *PartitionFilter) grpc() *kvs.PartitionFilter {
	begin := uint32(pf.Begin)
	ps := make([]*kvs.PartitionStatus, len(pf.Partitions))
	for i := range pf.Partitions {
		ps[i] = pf.Partitions[i].grpc()
	}

	return &kvs.PartitionFilter{
		Begin:             &begin,
		Count:             uint32(pf.Count),
		Digest:            pf.Digest,
		PartitionStatuses: ps,
		Retry:             true,
	}

}

///////////////////////////////////////////////////////////////////

func (ps *PartitionStatus) grpc() *kvs.PartitionStatus {
	id := uint32(ps.Id)
	bVal := ps.BVal
	digest := ps.Digest
	return &kvs.PartitionStatus{
		Id:     &id,
		BVal:   &bVal,
		Digest: digest,
		Retry:  ps.Retry,
	}
}

///////////////////////////////////////////////////////////////////

func (p *BasePolicy) grpc() *kvs.ReadPolicy {
	return &kvs.ReadPolicy{
		Replica:    p.ReplicaPolicy.grpc(),
		ReadModeSC: p.ReadModeSC.grpc(),
		ReadModeAP: p.ReadModeAP.grpc(),
	}
}

func (p *BasePolicy) grpcDeadlineContext() (context.Context, context.CancelFunc) {
	timeout := p.TotalTimeout
	if timeout <= 0 {
		return context.Background(), simpleCancelFunc

	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	return ctx, cancel
}

///////////////////////////////////////////////////////////////////

func (qp *QueryPolicy) grpc() *kvs.QueryPolicy {
	SendKey := qp.SendKey
	TotalTimeout := uint32(qp.TotalTimeout / time.Millisecond)
	RecordQueueSize := uint32(qp.RecordQueueSize)
	MaxConcurrentNodes := uint32(qp.MaxConcurrentNodes)
	IncludeBinData := qp.IncludeBinData
	FailOnClusterChange := false //qp.FailOnClusterChange
	ShortQuery := qp.ShortQuery || qp.ExpectedDuration == SHORT
	InfoTimeout := uint32(qp.SocketTimeout / time.Millisecond)
	ExpectedDuration := qp.ExpectedDuration.grpc()

	return &kvs.QueryPolicy{
		Replica:             qp.ReplicaPolicy.grpc(),
		ReadModeAP:          qp.ReadModeAP.grpc(),
		ReadModeSC:          qp.ReadModeSC.grpc(),
		SendKey:             &SendKey,
		Compress:            qp.UseCompression,
		Expression:          qp.FilterExpression.grpc(),
		TotalTimeout:        &TotalTimeout,
		MaxConcurrentNodes:  &MaxConcurrentNodes,
		RecordQueueSize:     &RecordQueueSize,
		IncludeBinData:      &IncludeBinData,
		FailOnClusterChange: &FailOnClusterChange,
		ShortQuery:          &ShortQuery,
		InfoTimeout:         &InfoTimeout,
		ExpectedDuration:    &ExpectedDuration,
	}
}

///////////////////////////////////////////////////////////////////

func (sp *ScanPolicy) grpc() *kvs.ScanPolicy {
	TotalTimeout := uint32(sp.TotalTimeout / time.Millisecond)
	MaxRecords := uint64(sp.MaxRecords)
	RecordsPerSecond := uint32(sp.RecordsPerSecond)
	MaxConcurrentNodes := uint32(sp.MaxConcurrentNodes)
	IncludeBinData := sp.IncludeBinData
	ConcurrentNodes := MaxConcurrentNodes > 1

	return &kvs.ScanPolicy{
		Replica:            sp.ReplicaPolicy.grpc(),
		ReadModeAP:         sp.ReadModeAP.grpc(),
		ReadModeSC:         sp.ReadModeSC.grpc(),
		Compress:           sp.UseCompression,
		Expression:         sp.FilterExpression.grpc(),
		TotalTimeout:       &TotalTimeout,
		MaxRecords:         &MaxRecords,
		RecordsPerSecond:   &RecordsPerSecond,
		ConcurrentNodes:    &ConcurrentNodes,
		MaxConcurrentNodes: &MaxConcurrentNodes,
		IncludeBinData:     &IncludeBinData,
	}
}

///////////////////////////////////////////////////////////////////

func (p *WritePolicy) grpc() *kvs.WritePolicy {
	return &kvs.WritePolicy{
		Replica:    p.ReplicaPolicy.grpc(),
		ReadModeSC: p.ReadModeSC.grpc(),
		ReadModeAP: p.ReadModeAP.grpc(),
	}
}

func (p *WritePolicy) grpc_exec(expr *Expression) *kvs.BackgroundExecutePolicy {
	if p == nil {
		return nil
	}

	SendKey := p.SendKey
	TotalTimeout := uint32(p.TotalTimeout / time.Millisecond)
	RecordExistsAction := p.RecordExistsAction.grpc()
	GenerationPolicy := p.GenerationPolicy.grpc()
	CommitLevel := p.CommitLevel.grpc()
	Generation := p.Generation
	Expiration := p.Expiration
	RespondAllOps := p.RespondPerEachOp
	DurableDelete := p.DurableDelete

	fe := expr
	if fe == nil {
		fe = p.FilterExpression
	}

	res := &kvs.BackgroundExecutePolicy{
		Replica:      p.ReplicaPolicy.grpc(),
		ReadModeAP:   p.ReadModeAP.grpc(),
		ReadModeSC:   p.ReadModeSC.grpc(),
		SendKey:      &SendKey,
		Compress:     p.UseCompression,
		Expression:   fe.grpc(),
		TotalTimeout: &TotalTimeout,

		Xdr: nil,

		RecordExistsAction: &RecordExistsAction,
		GenerationPolicy:   &GenerationPolicy,
		CommitLevel:        &CommitLevel,
		Generation:         &Generation,
		Expiration:         &Expiration,
		RespondAllOps:      &RespondAllOps,
		DurableDelete:      &DurableDelete,
	}

	return res
}

func (p *BatchPolicy) grpc_write() *kvs.WritePolicy {
	return &kvs.WritePolicy{
		Replica:    p.ReplicaPolicy.grpc(),
		ReadModeSC: p.ReadModeSC.grpc(),
		ReadModeAP: p.ReadModeAP.grpc(),
	}
}

func (cl CommitLevel) grpc() kvs.CommitLevel {
	switch cl {
	case COMMIT_ALL:
		return kvs.CommitLevel_COMMIT_ALL
	case COMMIT_MASTER:
		return kvs.CommitLevel_COMMIT_MASTER
	}
	panic(unreachable)
}

func newGrpcStatusError(res *kvs.AerospikeResponsePayload) Error {
	if res.GetStatus() >= 0 {
		return newError(types.ResultCode(res.GetStatus())).markInDoubt(res.GetInDoubt())
	}

	var resultCode = types.OK
	switch res.GetStatus() {
	case -16:
		// BATCH_FAILED
		resultCode = types.BATCH_FAILED
	case -15:
		// NO_RESPONSE
		resultCode = types.NO_RESPONSE
	case -12:
		// MAX_ERROR_RATE
		resultCode = types.MAX_ERROR_RATE
	case -11:
		// MAX_RETRIES_EXCEEDED
		resultCode = types.MAX_RETRIES_EXCEEDED
	case -10:
		// SERIALIZE_ERROR
		resultCode = types.SERIALIZE_ERROR
	case -9:
		// ASYNC_QUEUE_FULL
		// resultCode = types.ASYNC_QUEUE_FULL
		return newError(types.SERVER_ERROR, "Server ASYNC_QUEUE_FULL").markInDoubt(res.GetInDoubt())
	case -8:
		// SERVER_NOT_AVAILABLE
		resultCode = types.SERVER_NOT_AVAILABLE
	case -7:
		// NO_MORE_CONNECTIONS
		resultCode = types.NO_AVAILABLE_CONNECTIONS_TO_NODE
	case -5:
		// QUERY_TERMINATED
		resultCode = types.QUERY_TERMINATED
	case -4:
		// SCAN_TERMINATED
		resultCode = types.SCAN_TERMINATED
	case -3:
		// INVALID_NODE_ERROR
		resultCode = types.INVALID_NODE_ERROR
	case -2:
		// PARSE_ERROR
		resultCode = types.PARSE_ERROR
	case -1:
		// CLIENT_ERROR
		resultCode = types.COMMON_ERROR
	}

	return newError(resultCode).markInDoubt(res.GetInDoubt())
}

func (gp GenerationPolicy) grpc() kvs.GenerationPolicy {
	switch gp {
	case NONE:
		return kvs.GenerationPolicy_NONE
	case EXPECT_GEN_EQUAL:
		return kvs.GenerationPolicy_EXPECT_GEN_EQUAL
	case EXPECT_GEN_GT:
		return kvs.GenerationPolicy_EXPECT_GEN_GT
	}
	panic(unreachable)
}

func (ict IndexCollectionType) grpc() kvs.IndexCollectionType {
	switch ict {
	// Normal scalar index.
	case ICT_DEFAULT:
		return kvs.IndexCollectionType_DEFAULT
	// Index list elements.
	case ICT_LIST:
		return kvs.IndexCollectionType_LIST
	// Index map keys.
	case ICT_MAPKEYS:
		return kvs.IndexCollectionType_MAPKEYS
	// Index map values.
	case ICT_MAPVALUES:
		return kvs.IndexCollectionType_MAPVALUES
	}
	panic(unreachable)
}

func (o *Operation) grpc_op_type() kvs.OperationType {
	// case _READ: return  kvs.OperationType_READ
	switch o.opType {
	case _READ:
		return kvs.OperationType_READ
	case _READ_HEADER:
		return kvs.OperationType_READ_HEADER
	case _WRITE:
		return kvs.OperationType_WRITE
	case _CDT_READ:
		return kvs.OperationType_CDT_READ
	case _CDT_MODIFY:
		return kvs.OperationType_CDT_MODIFY
	case _MAP_READ:
		return kvs.OperationType_MAP_READ
	case _MAP_MODIFY:
		return kvs.OperationType_MAP_MODIFY
	case _ADD:
		return kvs.OperationType_ADD
	case _EXP_READ:
		return kvs.OperationType_EXP_READ
	case _EXP_MODIFY:
		return kvs.OperationType_EXP_MODIFY
	case _APPEND:
		return kvs.OperationType_APPEND
	case _PREPEND:
		return kvs.OperationType_PREPEND
	case _TOUCH:
		return kvs.OperationType_TOUCH
	case _BIT_READ:
		return kvs.OperationType_BIT_READ
	case _BIT_MODIFY:
		return kvs.OperationType_BIT_MODIFY
	case _DELETE:
		return kvs.OperationType_DELETE
	case _HLL_READ:
		return kvs.OperationType_HLL_READ
	case _HLL_MODIFY:
		return kvs.OperationType_HLL_MODIFY
	}

	panic(unreachable)
}

func (stmt *Statement) grpc(policy *QueryPolicy, ops []*Operation) *kvs.Statement {
	IndexName := stmt.IndexName
	// reset taskID every time
	TaskId := rand.Int63()
	SetName := stmt.SetName

	MaxRecords := uint64(policy.MaxRecords)
	RecordsPerSecond := uint32(policy.RecordsPerSecond)

	funcArgs := make([][]byte, 0, len(stmt.functionArgs))
	for i := range stmt.functionArgs {
		funcArgs = append(funcArgs, grpcValuePacked(stmt.functionArgs[i]))
	}

	return &kvs.Statement{
		Namespace:        stmt.Namespace,
		SetName:          &SetName,
		IndexName:        &IndexName,
		BinNames:         stmt.BinNames,
		Filter:           stmt.Filter.grpc(),
		PackageName:      stmt.packageName,
		FunctionName:     stmt.functionName,
		FunctionArgs:     funcArgs,
		Operations:       grpcOperations(ops),
		TaskId:           &TaskId,
		MaxRecords:       &MaxRecords,
		RecordsPerSecond: &RecordsPerSecond,
	}
}

func grpcOperations(ops []*Operation) []*kvs.Operation {
	res := make([]*kvs.Operation, 0, len(ops))
	for i := range ops {
		res = append(res, ops[i].grpc())
	}
	return res
}

func (qd QueryDuration) grpc() kvs.QueryDuration {
	switch qd {
	case LONG:
		return kvs.QueryDuration(kvs.QueryDuration_LONG)
	case SHORT:
		return kvs.QueryDuration(kvs.QueryDuration_SHORT)
	case LONG_RELAX_AP:
		return kvs.QueryDuration(kvs.QueryDuration_LONG_RELAX_AP)
	}
	panic(unreachable)
}

func (rm ReadModeAP) grpc() kvs.ReadModeAP {
	switch rm {
	case ReadModeAPOne:
		return kvs.ReadModeAP_ONE
	case ReadModeAPAll:
		return kvs.ReadModeAP_ALL
	}
	panic(unreachable)
}

func (rm ReadModeSC) grpc() kvs.ReadModeSC {
	switch rm {
	case ReadModeSCSession:
		return kvs.ReadModeSC_SESSION
	case ReadModeSCLinearize:
		return kvs.ReadModeSC_LINEARIZE
	case ReadModeSCAllowReplica:
		return kvs.ReadModeSC_ALLOW_REPLICA
	case ReadModeSCAllowUnavailable:
		return kvs.ReadModeSC_ALLOW_UNAVAILABLE
	}
	panic(unreachable)
}

func (rea RecordExistsAction) grpc() kvs.RecordExistsAction {
	switch rea {
	case UPDATE:
		return kvs.RecordExistsAction_UPDATE
	case UPDATE_ONLY:
		return kvs.RecordExistsAction_UPDATE_ONLY
	case REPLACE:
		return kvs.RecordExistsAction_REPLACE
	case REPLACE_ONLY:
		return kvs.RecordExistsAction_REPLACE_ONLY
	case CREATE_ONLY:
		return kvs.RecordExistsAction_CREATE_ONLY
	}
	panic(unreachable)
}

func (rp ReplicaPolicy) grpc() kvs.Replica {
	switch rp {
	case MASTER:
		return kvs.Replica_MASTER
	case MASTER_PROLES:
		return kvs.Replica_MASTER_PROLES
	case RANDOM:
		return kvs.Replica_RANDOM
	case SEQUENCE:
		return kvs.Replica_SEQUENCE
	case PREFER_RACK:
		return kvs.Replica_PREFER_RACK
	}
	panic(unreachable)
}
