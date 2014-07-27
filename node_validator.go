// Copyright 2013-2014 Aerospike, Inc.
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
	"errors"
	"net"
	"regexp"
	"strconv"
	"time"

	. "github.com/aerospike/aerospike-client-go/logger"
)

// Validates a Database server node
type NodeValidator struct {
	name       string
	aliases    []*Host
	address    string
	useNewInfo bool //= true
}

// Generates a node validator
func NewNodeValidator(host *Host, timeout time.Duration) (*NodeValidator, error) {
	newNodeValidator := &NodeValidator{
		useNewInfo: true,
	}
	newNodeValidator.setAliases(host)
	newNodeValidator.setAddress(timeout)

	return newNodeValidator, nil
}

func (this *NodeValidator) setAliases(host *Host) error {
	if addresses, err := net.LookupHost(host.Name); err != nil {
		return err
	} else {
		aliases := make([]*Host, len(addresses))
		for idx, addr := range addresses {
			aliases[idx] = NewHost(addr, host.Port)
		}
		this.aliases = aliases
		Logger.Debug("Node Validator has %d nodes.", len(aliases))
		return nil
	}
}

func (this *NodeValidator) setAddress(timeout time.Duration) error {
	for _, alias := range this.aliases {
		address := net.JoinHostPort(alias.Name, strconv.Itoa(alias.Port))
		if conn, err := NewConnection(address, timeout); err != nil {
			return err
		} else {
			defer conn.Close()

			if infoMap, err := RequestInfo(conn, "node", "build"); err != nil {
				return err
			} else {
				if nodeName, exists := infoMap["node"]; exists {
					this.name = nodeName
					this.address = address

					// Check new info protocol support for >= 2.6.6 build
					if buildVersion, exists := infoMap["build"]; exists {
						if v1, v2, v3, err := parseVersionString(buildVersion); err != nil {
							Logger.Error(err.Error())
							return err
						} else {
							this.useNewInfo = v1 > 2 || (v1 == 2 && (v2 > 6 || (v2 == 6 && v3 >= 6)))
						}
					}
				}
			}
		}
	}
	return nil
}

// parses a version string
func parseVersionString(version string) (int, int, int, error) {
	r := regexp.MustCompile(`(\d+)\.(\d+)\.(\d+).*`)
	vNumber := r.FindStringSubmatch(version)
	if len(vNumber) < 4 {
		return -1, -1, -1, errors.New("Invalid build version string in Info: " + version)
	} else {
		v1, err1 := strconv.Atoi(vNumber[1])
		v2, err2 := strconv.Atoi(vNumber[2])
		v3, err3 := strconv.Atoi(vNumber[3])
		if err1 == nil && err2 == nil && err3 == nil {
			return v1, v2, v3, nil
		} else {
			Logger.Error("Invalid build version string in Info: %s", version)
			return -1, -1, -1, errors.New("Invalid build version string in Info: " + version)
		}
	}
}
