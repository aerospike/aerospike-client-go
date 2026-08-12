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
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// TransactionSettings tunes the SDK-side transaction runtime. Unlike the
// connection settings, this group is read at operation time, so a
// configuration reload takes effect on the next command.
type TransactionSettings struct {
	// ImplicitBatchWriteTransactions wraps multi-key write batches on
	// strong-consistency namespaces in an implicit transaction. Default true.
	ImplicitBatchWriteTransactions *bool
	// NumberOfAttempts bounds implicit-transaction retries. Default 5.
	NumberOfAttempts *int
	// SleepBetweenAttempts delays between implicit-transaction retries.
	// Default one second.
	SleepBetweenAttempts *time.Duration
}

// SystemSettings holds cluster-wide settings that cannot vary per [Behavior].
//
// The connection, refresh and circuit-breaker groups are applied to the client
// policy at connect time and cannot be changed by a later configuration
// reload; only the Transactions group is consulted per operation.
type SystemSettings struct {
	MinConnectionsPerNode *int
	MaxConnectionsPerNode *int
	ConnPoolsPerNode      *int
	MaxSocketIdleTime     *time.Duration

	TendInterval *time.Duration

	NumTendIntervalsInErrorWindow *int
	MaxErrorsInErrorWindow        *int

	Transactions TransactionSettings
}

// implicitBatchWriteTransactions reports the effective setting.
func (s *SystemSettings) implicitBatchWriteTransactions() bool {
	if s == nil || s.Transactions.ImplicitBatchWriteTransactions == nil {
		return true
	}
	return *s.Transactions.ImplicitBatchWriteTransactions
}

// numberOfAttempts reports the effective implicit-transaction attempt count.
func (s *SystemSettings) numberOfAttempts() int {
	if s == nil || s.Transactions.NumberOfAttempts == nil || *s.Transactions.NumberOfAttempts < 1 {
		return 5
	}
	return *s.Transactions.NumberOfAttempts
}

// sleepBetweenAttempts reports the effective implicit-transaction retry delay.
func (s *SystemSettings) sleepBetweenAttempts() time.Duration {
	if s == nil || s.Transactions.SleepBetweenAttempts == nil {
		return time.Second
	}
	return *s.Transactions.SleepBetweenAttempts
}

// applyTo copies the connection, refresh and circuit-breaker groups onto a
// client policy. The transaction group is deliberately not applied: it is SDK
// runtime configuration read per operation.
func (s *SystemSettings) applyTo(p *as.ClientPolicy) {
	if s == nil {
		return
	}
	if s.MinConnectionsPerNode != nil {
		p.MinConnectionsPerNode = *s.MinConnectionsPerNode
	}
	if s.MaxConnectionsPerNode != nil {
		p.ConnectionQueueSize = *s.MaxConnectionsPerNode
	}
	if s.MaxSocketIdleTime != nil {
		p.IdleTimeout = *s.MaxSocketIdleTime
	}
	if s.TendInterval != nil {
		p.TendInterval = *s.TendInterval
	}
	if s.NumTendIntervalsInErrorWindow != nil {
		p.ErrorRateWindow = *s.NumTendIntervalsInErrorWindow
	}
	if s.MaxErrorsInErrorWindow != nil {
		p.MaxErrorRate = *s.MaxErrorsInErrorWindow
	}
}

// mergeSystemSettings layers a higher-precedence set over a lower one:
// every field set in higher wins.
func mergeSystemSettings(higher, lower SystemSettings) SystemSettings {
	out := lower
	if higher.MinConnectionsPerNode != nil {
		out.MinConnectionsPerNode = higher.MinConnectionsPerNode
	}
	if higher.MaxConnectionsPerNode != nil {
		out.MaxConnectionsPerNode = higher.MaxConnectionsPerNode
	}
	if higher.ConnPoolsPerNode != nil {
		out.ConnPoolsPerNode = higher.ConnPoolsPerNode
	}
	if higher.MaxSocketIdleTime != nil {
		out.MaxSocketIdleTime = higher.MaxSocketIdleTime
	}
	if higher.TendInterval != nil {
		out.TendInterval = higher.TendInterval
	}
	if higher.NumTendIntervalsInErrorWindow != nil {
		out.NumTendIntervalsInErrorWindow = higher.NumTendIntervalsInErrorWindow
	}
	if higher.MaxErrorsInErrorWindow != nil {
		out.MaxErrorsInErrorWindow = higher.MaxErrorsInErrorWindow
	}
	if higher.Transactions.ImplicitBatchWriteTransactions != nil {
		out.Transactions.ImplicitBatchWriteTransactions = higher.Transactions.ImplicitBatchWriteTransactions
	}
	if higher.Transactions.NumberOfAttempts != nil {
		out.Transactions.NumberOfAttempts = higher.Transactions.NumberOfAttempts
	}
	if higher.Transactions.SleepBetweenAttempts != nil {
		out.Transactions.SleepBetweenAttempts = higher.Transactions.SleepBetweenAttempts
	}
	return out
}
