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

package provider

import (
	"net/url"
	"os"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	registry "github.com/aerospike/aerospike-client-go/v8/config/registry"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"gopkg.in/yaml.v3"
)

const driverName = "file"

type YamlConfigProvider struct {
	configFilePath string
	oldModTime     time.Time
}

// Register the YamlConfigProvider with the configuration provider registry
func init() {
	registry.Register(driverName, NewYamlConfigProvider())
}

func NewYamlConfigProvider() dynconfig.ConfigProvider {
	return &YamlConfigProvider{}
}

func NewYamlConfigProviderWithPath(configFilePath string) dynconfig.ConfigProvider {
	return &YamlConfigProvider{
		configFilePath: configFilePath,
	}
}

// LoadConfig loads the configuration from a YAML file specified by the DSN.
func (yc *YamlConfigProvider) LoadConfig(dsn string) *dynconfig.Config {
	parsedUrl, err := url.Parse(dsn)
	if err != nil {
		logger.Logger.Error("Failed to parse config URL %s. Error: %v", dsn, err)
		return nil
	}

	filePath := parsedUrl.Path
	// Get the file info
	info, err := os.Stat(filePath)
	if err != nil {
		logger.Logger.Error("Failed to stat file %s. Error: %v", filePath, err)
		return nil
	}

	modTime := info.ModTime()
	// Compare to previously stored modTime
	if modTime.After(yc.oldModTime) {
		yc.oldModTime = modTime

		data, err := os.ReadFile(filePath)
		if err != nil {
			logger.Logger.Error("Failed to read file %s. Error: %v", filePath, err)
			return nil
		}

		var config dynconfig.Config
		if err := yaml.Unmarshal(data, &config); err != nil {
			logger.Logger.Error("Failed to serialize file %s to object. Error: %v", filePath, err)
		}

		return &config
	}

	return nil
}
