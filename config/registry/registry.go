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

package configregistry

import (
	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	atomic "github.com/aerospike/aerospike-client-go/v8/internal/atomic/map"
)

const (
	DSN_REGEX_PATTERN = `^\s*(?P<scheme>[A-Za-z][A-Za-z0-9+.-]*://)?(?P<path>.*)$`
	DEFAULT_SCHEME    = "file://"
	DSN_SCHEME        = "scheme"
	DSN_PATH          = "path"
)

var (
	ConfigProviders = atomic.New[string, dynconfig.ConfigProvider](0)
)

// Register registers a config provider by name.
func Register(driverType string, provider dynconfig.ConfigProvider) {
	if provider == nil {
		panic("Config provider cannot be nil")
	}

	if config := ConfigProviders.Get(driverType); config != nil {
		panic("Config provider " + driverType + " is already registered")
	}

	ConfigProviders.Set(driverType, provider)
}

// Get retrieves a config provider by name.
func Get(name string) (dynconfig.ConfigProvider, bool) {
	if provider := ConfigProviders.Get(name); provider != nil {
		return provider, true
	}

	return nil, false
}
