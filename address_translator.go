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
	"fmt"
	"sync"

	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

// SourceList identifies which server-advertised address list an endpoint came from.
type SourceList uint8

const (
	// SourceListStandard is the standard address list, used when
	// ClientPolicy.UseServicesAlternate is false.
	SourceListStandard SourceList = iota

	// SourceListAlternate is the alternate address list, used when
	// ClientPolicy.UseServicesAlternate is true.
	SourceListAlternate
)

// String implements the Stringer interface.
func (sourceList SourceList) String() string {
	if sourceList == SourceListAlternate {
		return "alternate"
	}
	return "standard"
}

// Endpoint is one server-advertised node address, exactly as the server
// advertised it, before any translation.
type Endpoint struct {
	// NodeName is the name of the node that advertised the address.
	NodeName string

	// TLSName is the node's TLS name; translation cannot change it.
	TLSName string

	// Host is the advertised host name or IP address.
	Host string

	// Port is the advertised port.
	Port int

	// SourceList says which advertised address list the endpoint came from.
	SourceList SourceList
}

// AddressTranslator maps a server-advertised endpoint to the address the
// client connects to. It changes only the transport address; TLS still
// authenticates against the node's own TLS name.
// Translate runs on the tend goroutine once per advertised address and must
// be in-memory only: no locking, no I/O. Returning ok false is a miss
// ClientPolicy.TranslationOnMiss decides what happens next.
// A translator may additionally implement ClusterViewChangeObserver.
type AddressTranslator interface {
	Translate(advertised Endpoint) (translated Host, ok bool)
}

// ClusterViewChangeObserver is optionally implemented by an AddressTranslator
// to invalidate cached state when the cluster view changes.
// OnClusterViewChanged runs on the tend goroutine whenever the node set or an
// advertised address changed; viewChange increases by one per change. It must
// not block or perform I/O.
type ClusterViewChangeObserver interface {
	OnClusterViewChanged(viewChange int64)
}

// translatorDisplayName names a translator in logs and errors.
func translatorDisplayName(translator AddressTranslator) string {
	switch translator.(type) {
	case staticMapAddressTranslator:
		return translatorNameStaticMap
	case portMapAddressTranslator:
		return translatorNamePortMap
	case passThroughAddressTranslator:
		return translatorNamePassThrough
	}
	return fmt.Sprintf("%T", translator)
}

// translate applies translator to advertised and the onMiss rule to a miss.
// A pass_through miss warns through misses, and a nil misses never warns.
func translate(translator AddressTranslator, onMiss TranslationMissMode, advertised Endpoint, misses *translationMisses) (Host, Error) {
	host, ok := translator.Translate(advertised)
	if !ok {
		if onMiss == TranslationMissStrict {
			return Host{}, newTranslationMiss(translator, advertised)
		}
		misses.warn(translator, advertised)
		return Host{Name: advertised.Host, TLSName: advertised.TLSName, Port: advertised.Port}, nil
	}
	host.TLSName = advertised.TLSName
	return host, nil
}

// translationMisses remembers the last pass_through miss of each node, so a
// user-set translator warns once per node name and advertised address.
// Tend parses peers of several nodes at once, hence the mutex.
type translationMisses struct {
	mu   sync.Mutex
	last map[string]hostAndPort
}

func newTranslationMisses() *translationMisses {
	return &translationMisses{last: map[string]hostAndPort{}}
}

func (misses *translationMisses) warn(translator AddressTranslator, advertised Endpoint) {
	if misses == nil {
		return
	}
	address := hostAndPort{host: advertised.Host, port: advertised.Port}
	misses.mu.Lock()
	previous, seen := misses.last[advertised.NodeName]
	misses.last[advertised.NodeName] = address
	misses.mu.Unlock()
	if seen && previous == address {
		return
	}
	logger.Logger.Warn(
		"Address translator %s has no mapping for node %s advertised address %s:%d in the %s list. Connecting to the advertised address because translation.on_miss is pass_through",
		translatorDisplayName(translator), advertised.NodeName, advertised.Host, advertised.Port, advertised.SourceList,
	)
}

func newTranslationMiss(translator AddressTranslator, advertised Endpoint) Error {
	return newError(types.TRANSLATION_MISS, fmt.Sprintf(
		"Address translator %s has no mapping for node %s advertised address %s:%d in the %s list. Add a mapping for that endpoint or node, or set translation.on_miss to pass_through",
		translatorDisplayName(translator), advertised.NodeName, advertised.Host, advertised.Port, advertised.SourceList))
}
