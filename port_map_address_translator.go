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

package aerospike

import (
	"strconv"
	"strings"

	"github.com/aerospike/aerospike-client-go/v8/types"
)

// NewPortMapAddressTranslator returns a translator for nodes that sit behind
// one endpoint and are told apart only by port. A key is an advertised
// "host:port" (any key containing a colon) or a node name; every value is the
// "host:port" to connect to. IPv6 hosts must be bracketed, as in
// "[2001:db8::1]:4000". Lookup is by "host:port" first, then node name; no
// match is a miss. An invalid entry fails construction.
func NewPortMapAddressTranslator(portMap map[string]string) (AddressTranslator, Error) {
	if portMap == nil {
		return nil, newError(types.PARAMETER_ERROR, "Invalid port-map: map is nil")
	}

	endpointTargets := make(map[hostAndPort]Host, len(portMap))
	nodeTargets := make(map[string]Host, len(portMap))

	for key, value := range portMap {
		if key == "" {
			return nil, invalidPortMapEntry(key, value, "key is empty")
		}

		target, err := parsePortMapAddress(key, value, value)
		if err != nil {
			return nil, err
		}

		if strings.IndexByte(key, ':') >= 0 {
			advertised, err := parsePortMapAddress(key, value, key)
			if err != nil {
				return nil, err
			}

			advertisedKey := hostAndPort{host: advertised.Name, port: advertised.Port}
			if _, exists := endpointTargets[advertisedKey]; exists {
				return nil, invalidPortMapEntry(key, value, "duplicates another key")
			}
			endpointTargets[advertisedKey] = target
		} else {
			nodeTargets[key] = target
		}
	}

	return portMapAddressTranslator{endpointTargets: endpointTargets, nodeTargets: nodeTargets}, nil
}

type hostAndPort struct {
	host string
	port int
}

type portMapAddressTranslator struct {
	endpointTargets map[hostAndPort]Host
	nodeTargets     map[string]Host
}

// Translate implements the AddressTranslator interface.
func (translator portMapAddressTranslator) Translate(advertised Endpoint) (Host, bool) {
	target, found := translator.endpointTargets[hostAndPort{host: advertised.Host, port: advertised.Port}]
	if !found {
		if advertised.NodeName == "" {
			return Host{}, false
		}

		target, found = translator.nodeTargets[advertised.NodeName]
		if !found {
			return Host{}, false
		}
	}

	return Host{Name: target.Name, TLSName: advertised.TLSName, Port: target.Port}, true
}

// parsePortMapAddress reads a "host:port" string from a port-map entry,
// unwrapping a bracketed IPv6 host and validating the port range. key and
// value name the entry in error messages; address is the string to parse.
func parsePortMapAddress(key, value, address string) (Host, Error) {
	separator := strings.LastIndexByte(address, ':')
	if separator < 0 {
		return Host{}, invalidPortMapEntry(key, value, address+" is not host:port")
	}

	host := address[:separator]
	if strings.HasPrefix(host, "[") {
		if !strings.HasSuffix(host, "]") || len(host) < 3 {
			return Host{}, invalidPortMapEntry(key, value, address+" has an invalid bracketed IPv6 host")
		}
		host = host[1 : len(host)-1]
	} else if strings.IndexByte(host, ':') >= 0 {
		return Host{}, invalidPortMapEntry(key, value, address+" has an IPv6 host that is not in brackets")
	}

	if host == "" {
		return Host{}, invalidPortMapEntry(key, value, address+" has an empty host")
	}

	portString := address[separator+1:]
	port, err := strconv.Atoi(portString)
	if err != nil {
		return Host{}, invalidPortMapEntry(key, value, address+" has an invalid port "+portString)
	}
	if port < 1 || port > 65535 {
		return Host{}, invalidPortMapEntry(key, value, address+" has port "+portString+" out of range 1-65535")
	}

	return Host{Name: host, Port: port}, nil
}

func invalidPortMapEntry(key, value, reason string) Error {
	return newError(types.PARAMETER_ERROR, "Invalid port-map entry "+key+": "+value+": "+reason)
}
