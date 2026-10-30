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

// NewPassThroughAddressTranslator returns a translator that uses every
// advertised address unchanged. Use it when the server already advertises
// addresses the client can reach, typically together with
// ClientPolicy.UseServicesAlternate.
func NewPassThroughAddressTranslator() AddressTranslator {
	return passThroughAddressTranslator{}
}

type passThroughAddressTranslator struct{}

// Translate implements the AddressTranslator interface.
func (passThroughAddressTranslator) Translate(advertised Endpoint) (Host, bool) {
	return Host{Name: advertised.Host, TLSName: advertised.TLSName, Port: advertised.Port}, true
}
