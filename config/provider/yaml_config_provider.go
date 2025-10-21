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
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	registry "github.com/aerospike/aerospike-client-go/v8/config/registry"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"gopkg.in/yaml.v3"
)

const (
	DRIVER_NAME           = "file://"
	VERSION_REGEX_PATTERN = `(?m)^(?P<version>version):\s*(?P<value>.+)$`
)

type YamlConfigProvider struct {
	oldModTime time.Time
}

// Register the YamlConfigProvider with the configuration provider registry
func init() {
	registry.Register(DRIVER_NAME, NewYamlConfigProvider())
}

func NewYamlConfigProvider() dynconfig.ConfigProvider {
	return &YamlConfigProvider{oldModTime: time.Time{}}
}

func NewYamlConfigProviderWithPath(configFilePath string) dynconfig.ConfigProvider {
	return &YamlConfigProvider{
		oldModTime: time.Time{},
	}
}

// containsVersion checks if the provided YAML data contains a version field.
// Since the file is a yaml file and yaml spec does not dictate
// order of fields we have to read the whole file and check for the version field.
func (yc *YamlConfigProvider) containsVersion(data []byte) bool {
	content, err := strconv.Unquote(string(data))
	if err != nil {
		// fallback to raw data if not quoted
		content = string(data)
	}
	re := regexp.MustCompile(VERSION_REGEX_PATTERN)

	matches := re.FindStringSubmatch(content)
	if matches == nil {
		logger.Logger.Warn("`version` is missing in provided configuration")
		return false
	}
	return true
}

// LoadConfig loads the configuration from a YAML file specified by the DSN.
func (yc *YamlConfigProvider) LoadConfig(filePath string) *dynconfig.Config {
	// Get the file info
	info, err := os.Stat(filePath)
	if err != nil {
		logger.Logger.Warn("File %s could not be found. Error: %v", filePath, err)
		return nil
	}

	modTime := info.ModTime()
	// Compare to previously stored modTime
	if modTime.After(yc.oldModTime) {
		yc.oldModTime = modTime
		data, err := os.ReadFile(filePath)
		if err != nil {
			logger.Logger.Warn("Failed to read file %s. Error: %v", filePath, err)
			return nil
		}

		// Validate if the file contains a version. Will not attempt to serialize
		// the file into config if the version is not present.
		if !yc.containsVersion(data) {
			return nil
		}

		var config dynconfig.Config
		if err := yaml.Unmarshal(data, &config); err != nil {
			logger.Logger.Warn("Failed to serialize file %s to object. Error: %s",
				filePath, strings.ReplaceAll(err.Error(), "\n", " "))
			return nil
		}
		return &config
	}

	return nil
}
