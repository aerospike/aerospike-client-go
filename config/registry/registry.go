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
	"sync"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
)

const (
	DSN_REGEX_PATTERN = `^\s*([A-Za-z][A-Za-z0-9+.-]*://)?(.*)$`
	DEFAULT_SCHEMA    = "file://"
)

var (
	ConfigProvidersMu sync.RWMutex
	ConfigProviders   = make(map[string]dynconfig.ConfigProvider)
)

// Register registers a config provider by name.
func Register(driverType string, provider dynconfig.ConfigProvider) {
	if provider == nil {
		panic("Config provider cannot be nil")
	}

	ConfigProvidersMu.Lock()
	defer ConfigProvidersMu.Unlock()

	if _, found := ConfigProviders[driverType]; found {
		panic("Config provider " + driverType + " is already registered")
	}
	ConfigProviders[driverType] = provider
}

// Get retrieves a config provider by name.
func Get(name string) (dynconfig.ConfigProvider, bool) {
	ConfigProvidersMu.RLock()
	defer ConfigProvidersMu.RUnlock()
	provider, ok := ConfigProviders[name]

	return provider, ok
}
