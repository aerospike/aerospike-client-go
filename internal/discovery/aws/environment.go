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

package aws

import (
	"os"
	"strings"
	"time"
)

// Environment is the process environment and default AWS endpoints. Tests replace it.
type Environment interface {
	Getenv(name string) string
	UserHome() string
	EC2Endpoint(region string) string
	STSEndpoint(region string) string
	ECSEndpoint() string
	IMDSEndpoint() string
	Now() time.Time
}

// SystemEnvironment reads the process environment.
type SystemEnvironment struct{}

// Getenv implements Environment.
func (SystemEnvironment) Getenv(name string) string {
	return os.Getenv(name)
}

// UserHome implements Environment.
func (SystemEnvironment) UserHome() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return home
}

// EC2Endpoint implements Environment.
func (SystemEnvironment) EC2Endpoint(region string) string {
	return "https://ec2." + region + "." + awsDomain(region)
}

// STSEndpoint implements Environment.
func (SystemEnvironment) STSEndpoint(region string) string {
	return "https://sts." + region + "." + awsDomain(region)
}

// ECSEndpoint implements Environment.
func (SystemEnvironment) ECSEndpoint() string {
	return "http://169.254.170.2"
}

// IMDSEndpoint implements Environment.
func (SystemEnvironment) IMDSEndpoint() string {
	return "http://169.254.169.254"
}

// Now implements Environment.
func (SystemEnvironment) Now() time.Time {
	return time.Now()
}

func awsDomain(region string) string {
	if strings.HasPrefix(region, "cn-") {
		return "amazonaws.com.cn"
	}
	return "amazonaws.com"
}
