//go:build go1.27

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

package sdk

import (
	as "github.com/aerospike/aerospike-client-go/v8"
)

// applyBase copies the settings common to every policy onto a BasePolicy.
func applyBase(p *as.BasePolicy, s Settings) error {
	if s.TotalTimeout != nil {
		p.TotalTimeout = *s.TotalTimeout
	}
	if s.SocketTimeout != nil {
		p.SocketTimeout = *s.SocketTimeout
	}
	if s.RetryDelay != nil {
		p.SleepBetweenRetries = *s.RetryDelay
	}
	if s.MaxRetries != nil {
		p.MaxRetries = *s.MaxRetries
	}
	if s.SendKey != nil {
		p.SendKey = *s.SendKey
	}
	if s.UseCompression != nil {
		p.UseCompression = *s.UseCompression
	}
	if s.Replica != nil {
		p.ReplicaPolicy = *s.Replica
	}
	if s.ReadModeAP != nil {
		p.ReadModeAP = *s.ReadModeAP
	}
	if s.ReadModeSC != nil {
		p.ReadModeSC = *s.ReadModeSC
	}
	if s.ReadTouchTTLPercent != nil {
		v := *s.ReadTouchTTLPercent
		if v != -1 && v != 0 && (v < 1 || v > 100) {
			return NewError(KindInvalidArgument,
				"read_touch_ttl_percent must be -1, 0, or 1..100, got %d", v)
		}
		p.ReadTouchTTLPercent = v
	}
	return nil
}

// ToReadPolicy builds a read policy from resolved settings.
func ToReadPolicy(s Settings) (*as.BasePolicy, error) {
	p := as.NewPolicy()
	if err := applyBase(p, s); err != nil {
		return nil, err
	}
	return p, nil
}

// ToWritePolicy builds a write policy from resolved settings.
func ToWritePolicy(s Settings) (*as.WritePolicy, error) {
	p := as.NewWritePolicy(0, 0)
	if err := applyBase(&p.BasePolicy, s); err != nil {
		return nil, err
	}
	if s.CommitLevel != nil {
		p.CommitLevel = *s.CommitLevel
	}
	if s.DurableDelete != nil {
		p.DurableDelete = *s.DurableDelete
	}
	return p, nil
}

// ToQueryPolicy builds a query policy from resolved settings.
func ToQueryPolicy(s Settings) (*as.QueryPolicy, error) {
	p := as.NewQueryPolicy()
	if err := applyBase(&p.BasePolicy, s); err != nil {
		return nil, err
	}
	if s.MaxConcurrentNodes != nil {
		p.MaxConcurrentNodes = *s.MaxConcurrentNodes
	}
	if s.RecordQueueSize != nil {
		p.RecordQueueSize = *s.RecordQueueSize
	}
	return p, nil
}

// ToScanPolicy builds a scan policy from resolved settings.
func ToScanPolicy(s Settings) (*as.ScanPolicy, error) {
	p := as.NewScanPolicy()
	if err := applyBase(&p.BasePolicy, s); err != nil {
		return nil, err
	}
	if s.MaxConcurrentNodes != nil {
		p.MaxConcurrentNodes = *s.MaxConcurrentNodes
	}
	if s.RecordQueueSize != nil {
		p.RecordQueueSize = *s.RecordQueueSize
	}
	return p, nil
}

// ToBatchPolicy builds a batch policy from resolved settings.
func ToBatchPolicy(s Settings) (*as.BatchPolicy, error) {
	p := as.NewBatchPolicy()
	if err := applyBase(&p.BasePolicy, s); err != nil {
		return nil, err
	}
	if s.MaxConcurrentNodes != nil {
		p.ConcurrentNodes = *s.MaxConcurrentNodes
	}
	if s.AllowInline != nil {
		p.AllowInline = *s.AllowInline
	}
	if s.AllowInlineSSD != nil {
		p.AllowInlineSSD = *s.AllowInlineSSD
	}
	return p, nil
}

// ToBatchReadPolicy builds a per-record batch read policy.
func ToBatchReadPolicy(s Settings) (*as.BatchReadPolicy, error) {
	p := as.NewBatchReadPolicy()
	if s.ReadTouchTTLPercent != nil {
		v := *s.ReadTouchTTLPercent
		if v != -1 && v != 0 && (v < 1 || v > 100) {
			return nil, NewError(KindInvalidArgument,
				"read_touch_ttl_percent must be -1, 0, or 1..100, got %d", v)
		}
		p.ReadTouchTTLPercent = v
	}
	return p, nil
}

// ToBatchWritePolicy builds a per-record batch write policy.
func ToBatchWritePolicy(s Settings) *as.BatchWritePolicy {
	p := as.NewBatchWritePolicy()
	if s.CommitLevel != nil {
		p.CommitLevel = *s.CommitLevel
	}
	if s.DurableDelete != nil {
		p.DurableDelete = *s.DurableDelete
	}
	return p
}

// ToBatchDeletePolicy builds a per-record batch delete policy.
func ToBatchDeletePolicy(s Settings) *as.BatchDeletePolicy {
	p := as.NewBatchDeletePolicy()
	if s.CommitLevel != nil {
		p.CommitLevel = *s.CommitLevel
	}
	if s.DurableDelete != nil {
		p.DurableDelete = *s.DurableDelete
	}
	return p
}

// ToBatchUDFPolicy builds a per-record batch UDF policy.
func ToBatchUDFPolicy(s Settings) *as.BatchUDFPolicy {
	p := as.NewBatchUDFPolicy()
	if s.CommitLevel != nil {
		p.CommitLevel = *s.CommitLevel
	}
	if s.DurableDelete != nil {
		p.DurableDelete = *s.DurableDelete
	}
	return p
}

// ToTxnVerifyPolicy builds the transaction verify-phase policy from the
// ScopeSystemTxnVerify settings.
func ToTxnVerifyPolicy(s Settings) (*as.TxnVerifyPolicy, error) {
	p := as.NewTxnVerifyPolicy()
	if err := applyBase(&p.BasePolicy, s); err != nil {
		return nil, err
	}
	return p, nil
}

// ToTxnRollPolicy builds the transaction roll-phase policy from the
// ScopeSystemTxnRoll settings.
func ToTxnRollPolicy(s Settings) (*as.TxnRollPolicy, error) {
	p := as.NewTxnRollPolicy()
	if err := applyBase(&p.BasePolicy, s); err != nil {
		return nil, err
	}
	return p, nil
}

// ResolveDurableDelete picks the durable-delete flag with the documented
// precedence: an explicit per-command override wins, then the command's own
// default, then the Behavior setting, then false.
func ResolveDurableDelete(setting *bool, commandDefault *bool, override *bool) bool {
	if override != nil {
		return *override
	}
	if commandDefault != nil {
		return *commandDefault
	}
	if setting != nil {
		return *setting
	}
	return false
}
