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

// Package dynconfig provides a configuration provider interface and structures
// for loading and managing dynamic configurations in a system.
//
// It includes static and dynamic configurations for various components such as
// client, read, write, query, scan, batch operations, transactions, and metrics.
// The configurations are defined using YAML tags for easy serialization and
// deserialization.
package dynconfig

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

// ConfigProvider represents a configuration provider.
type ConfigProvider interface {
	LoadConfig(dsn string) *Config
}

// ----------------------------------------------------------------
// Structures used to serialize and deserialize the configuration
// ----------------------------------------------------------------

type Config struct {
	Version *string        `yaml:"version"`
	Static  *StaticConfig  `yaml:"static"`
	Dynamic *DynamicConfig `yaml:"dynamic"`
}

type StaticConfig struct {
	Client *Client `yaml:"client"`
}

type DynamicConfig struct {
	Client      *Client      `yaml:"client"`
	Read        *Read        `yaml:"read"`
	Write       *Write       `yaml:"write"`
	Query       *Query       `yaml:"query"`
	Scan        *Scan        `yaml:"scan"`
	BatchRead   *BatchRead   `yaml:"batch_read"`
	BatchWrite  *BatchWrite  `yaml:"batch_write"`
	BatchUdf    *BatchUdf    `yaml:"batch_udf"`
	BatchDelete *BatchDelete `yaml:"batch_delete"`
	TxnRoll     *TxnRoll     `yaml:"txn_roll"`
	TxnVerify   *TxnVerify   `yaml:"txn_verify"`
	Metrics     *Metrics     `yaml:"metrics"`
}

type Client struct {
	// static config
	ConfigInterval        *int `yaml:"config_interval"`
	ConnectionQueueSize   *int `yaml:"max_connections_per_node"`
	MinConnectionsPerNode *int `yaml:"min_connections_per_node"`

	// dynamic config
	IdleTimeout         *int    `yaml:"max_socket_idle"`
	Timeout             *int    `yaml:"timeout"`
	ErrorRateWindow     *int    `yaml:"error_rate_window"`
	MaxErrorRate        *int    `yaml:"max_error_rate"`
	LoginTimeout        *int    `yaml:"login_timeout"`
	RackAware           *bool   `yaml:"rack_aware"`
	RackIds             *[]int  `yaml:"rack_ids"`
	TendInterval        *int    `yaml:"tend_interval"`
	UseServiceAlternate *bool   `yaml:"use_service_alternate"`
	ApplicationId       *string `yaml:"app_id"`
}

type Read struct {
	ReadModeAp          *ReadModeAp `yaml:"read_mode_ap"`
	ReadModeSc          *ReadModeSc `yaml:"read_mode_sc"`
	Replica             *Replica    `yaml:"replica"`
	SleepBetweenRetries *int        `yaml:"sleep_between_retries"`
	SocketTimeout       *int        `yaml:"socket_timeout"`
	TotalTimeout        *int        `yaml:"total_timeout"`
	MaxRetries          *int        `yaml:"max_retries"`
	TimeoutDelay        *int        `yaml:"timeout_delay"`
}

type Write struct {
	Replica             *Replica `yaml:"replica"`
	SendKey             *bool    `yaml:"send_key"`
	SleepBetweenRetries *int     `yaml:"sleep_between_retries"`
	SocketTimeout       *int     `yaml:"socket_timeout"`
	TotalTimeout        *int     `yaml:"total_timeout"`
	MaxRetries          *int     `yaml:"max_retries"`
	DurableDelete       *bool    `yaml:"durable_delete"`
	TimeoutDelay        *int     `yaml:"timeout_delay"`
}

type Query struct {
	Replica             *Replica       `yaml:"replica"`
	SleepBetweenRetries *int           `yaml:"sleep_between_retries"`
	SocketTimeout       *int           `yaml:"socket_timeout"`
	TotalTimeout        *int           `yaml:"total_timeout"`
	MaxRetries          *int           `yaml:"max_retries"`
	IncludeBinData      *bool          `yaml:"include_bin_data"`
	RecordQueueSize     *int           `yaml:"record_queue_size"`
	ExpectedDuration    *QueryDuration `yaml:"expected_duration"`
	TimeoutDelay        *int           `yaml:"timeout_delay"`
}

type Scan struct {
	Replica             *Replica `yaml:"replica"`
	SleepBetweenRetries *int     `yaml:"sleep_between_retries"`
	SocketTimeout       *int     `yaml:"socket_timeout"`
	TimeoutDelay        *int     `yaml:"timeout_delay"`
	TotalTimeout        *int     `yaml:"total_timeout"`
	MaxRetries          *int     `yaml:"max_retries"`
	MaxConcurrentNodes  *int     `yaml:"max_concurrent_nodes"`
}

type BatchRead struct {
	ReadModeAp          *ReadModeAp `yaml:"read_mode_ap"`
	ReadModeSc          *ReadModeSc `yaml:"read_mode_sc"`
	Replica             *Replica    `yaml:"replica"`
	SleepBetweenRetries *int        `yaml:"sleep_between_retries"`
	SocketTimeout       *int        `yaml:"socket_timeout"`
	TotalTimeout        *int        `yaml:"total_timeout"`
	MaxRetries          *int        `yaml:"max_retries"`
	MaxConcurrentThread *int        `yaml:"max_concurrent_thread"`
	AllowInline         *bool       `yaml:"allow_inline"`
	AllowInlineSSD      *bool       `yaml:"allow_inline_ssd"`
	RespondAllKeys      *bool       `yaml:"respond_all_keys"`
	TimeoutDelay        *int        `yaml:"timeout_delay"`
}

type BatchWrite struct {
	Replica             *Replica `yaml:"replica"`
	SleepBetweenRetries *int     `yaml:"sleep_between_retries"`
	SocketTimeout       *int     `yaml:"socket_timeout"`
	TotalTimeout        *int     `yaml:"total_timeout"`
	MaxRetries          *int     `yaml:"max_retries"`
	DurableDelete       *bool    `yaml:"durable_delete"`
	SendKey             *bool    `yaml:"send_key"`
	MaxConcurrentThread *int     `yaml:"max_concurrent_thread"`
	AllowInline         *bool    `yaml:"allow_inline"`
	AllowInlineSSD      *bool    `yaml:"allow_inline_ssd"`
	RespondAllKeys      *bool    `yaml:"respond_all_keys"`
	TimeoutDelay        *int     `yaml:"timeout_delay"`
}

type BatchUdf struct {
	DurableDelete *bool `yaml:"durable_delete"`
	SendKey       *bool `yaml:"send_key"`
}

type BatchDelete struct {
	DurableDelete *bool `yaml:"durable_delete"`
	SendKey       *bool `yaml:"send_key"`
}

type TxnRoll struct {
	ReadModeAp          *ReadModeAp `yaml:"read_mode_ap"`
	ReadModeSc          *ReadModeSc `yaml:"read_mode_sc"`
	Replica             *Replica    `yaml:"replica"`
	SleepBetweenRetries *int        `yaml:"sleep_between_retries"`
	SocketTimeout       *int        `yaml:"socket_timeout"`
	TotalTimeout        *int        `yaml:"total_timeout"`
	MaxRetries          *int        `yaml:"max_retries"`
	AllowInline         *bool       `yaml:"allow_inline"`
	AllowInlineSSD      *bool       `yaml:"allow_inline_ssd"`
	RespondAllKeys      *bool       `yaml:"respond_all_keys"`
	TimeoutDelay        *int        `yaml:"timeout_delay"`
}

type TxnVerify struct {
	ReadModeAp          *ReadModeAp `yaml:"read_mode_ap"`
	ReadModeSc          *ReadModeSc `yaml:"read_mode_sc"`
	Replica             *Replica    `yaml:"replica"`
	SleepBetweenRetries *int        `yaml:"sleep_between_retries"`
	SocketTimeout       *int        `yaml:"socket_timeout"`
	TotalTimeout        *int        `yaml:"total_timeout"`
	MaxRetries          *int        `yaml:"max_retries"`
	AllowInline         *bool       `yaml:"allow_inline"`
	AllowInlineSSD      *bool       `yaml:"allow_inline_ssd"`
	RespondAllKeys      *bool       `yaml:"respond_all_keys"`
	TimeoutDelay        *int        `yaml:"timeout_delay"`
}

type Metrics struct {
	Enable         *bool              `yaml:"enable"`
	LatencyColumns *int               `yaml:"latency_columns"`
	LatencyBase    *int               `yaml:"latency_base"`
	Labels         *map[string]string `yaml:"labels"`
}

// ----------------------------------------------------------------
// Enum types
// ----------------------------------------------------------------

// TODO(Khosrow): Deal with the circular dependencies to remove the redefinition of these types.
// The subtypes can also be their own types instead of map to validate their own input

type ReadModeAp int

const (
	ONE ReadModeAp = iota
	ALL
)

var ReadModeApYaml = map[ReadModeAp]string{
	ONE: "ONE",
	ALL: "ALL",
}

type ReadModeSc int

const (
	SESSION ReadModeSc = iota
	LINEARIZE
	ALLOW_REPLICA
	ALLOW_UNAVAILABLE
)

var ReadModeScYaml = map[ReadModeSc]string{
	SESSION:           "SESSION",
	LINEARIZE:         "LINEARIZE",
	ALLOW_REPLICA:     "ALLOW_REPLICA",
	ALLOW_UNAVAILABLE: "ALLOW_UNAVAILABLE",
}

type Replica int

const (
	MASTER Replica = iota
	MASTER_PROLES
	SEQUENCE
	PREFER_RACK
)

var ReplicaYaml = map[Replica]string{
	MASTER:        "MASTER",
	MASTER_PROLES: "MASTER_PROLES",
	SEQUENCE:      "SEQUENCE",
	PREFER_RACK:   "PREFER_RACK",
}

type QueryDuration int

const (
	LONG QueryDuration = iota
	SHORT
	LONG_RELAX_AP
)

// ----------------------------------------------------------------
// UnmarshalYAML methods for enum types
// ----------------------------------------------------------------

func (r *ReadModeAp) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToUpper(s) {
	case "ONE":
		*r = ONE
	case "ALL":
		*r = ALL
	default:
		return fmt.Errorf("invalid ReadModeAp value: %s", s)
	}
	return nil
}

func (r *ReadModeSc) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToUpper(s) {
	case "SESSION":
		*r = SESSION
	case "LINEARIZE":
		*r = LINEARIZE
	case "ALLOW_REPLICA":
		*r = ALLOW_REPLICA
	case "ALLOW_UNAVAILABLE":
		*r = ALLOW_UNAVAILABLE
	default:
		return fmt.Errorf("invalid ReadModeSc value: %s", s)
	}
	return nil
}

func (r *Replica) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToUpper(s) {
	case "MASTER":
		*r = MASTER
	case "MASTER_PROLES":
		*r = MASTER_PROLES
	case "SEQUENCE":
		*r = SEQUENCE
	case "PREFER_RACK":
		*r = PREFER_RACK
	default:
		return fmt.Errorf("invalid Replica value: %s", s)
	}
	return nil
}

func (r *QueryDuration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	switch strings.ToUpper(s) {
	case "LONG":
		*r = LONG
	case "SHORT":
		*r = SHORT
	case "LONG_RELAX_AP":
		*r = LONG_RELAX_AP
	default:
		return fmt.Errorf("invalid QueryDuration value: %s", s)
	}
	return nil
}
