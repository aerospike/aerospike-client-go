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

// NewStaticMapAddressTranslator returns the address translator the client
// uses by default, built over the given map. It behaves exactly like
// ClientPolicy.IpMap: the key is the advertised host without its port, and
// the value is either a bare host, which keeps the advertised port, or
// "host:port", which replaces both. An advertised host with no entry, or
// with an empty value, is a miss.
func NewStaticMapAddressTranslator(addressMap map[string]string) AddressTranslator {
	return staticMapAddressTranslator{addressMap: addressMap}
}

type staticMapAddressTranslator struct {
	addressMap map[string]string
}

// Translate implements the AddressTranslator interface.
func (translator staticMapAddressTranslator) Translate(advertised Endpoint) (Host, bool) {
	value, found := translator.addressMap[advertised.Host]
	if !found || value == "" {
		return Host{}, false
	}

	translatedHost, translatedPort := parseTranslationValue(value, advertised.Port)
	return Host{Name: translatedHost, TLSName: advertised.TLSName, Port: translatedPort}, true
}
