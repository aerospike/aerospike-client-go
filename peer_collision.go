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
	"strings"

	"github.com/aerospike/aerospike-client-go/v8/logger"
)

// validatePeer sets peer.validated from the first address of the peer that
// is its own. An address another node also has, by string or by the node that
// answers on it, is skipped. A different node already on the dialed socket
// stops the peer. A peer left with no address of its own is reported as a
// TRANSLATION_COLLISION.
func (clstr *Cluster) validatePeer(ps *peers, p *peer) {
	var shared []sharedAddress
	var notTried []*Host
	collided := false

	for i, host := range p.hosts {
		if owner := clstr.sharedOwner(ps, p, host); owner != "" {
			clstr.logSkippedAddress(p, host, owner)
			shared = append(shared, sharedAddress{host: host, owner: owner})
			continue
		}

		nv := nodeValidator{seedOnlyCluster: clstr.clientPolicy.Load().SeedOnlyCluster}
		if err := nv.validateNode(clstr, host); err != nil {
			// IpMap/NLB fork: include the advertised peer node name so a
			// failing peer can be correlated against server-side membership
			// (asadm / `peers-clear-std`). `host` here is the post-IpMap target
			// address; the pre-translation address is logged in peers_parser.go.
			logger.Logger.Warn("Add node `%s` (peer node `%s`) failed: `%s`", host, p.nodeName, err)
			continue
		}

		if owner := clstr.healthyNodeAt(nv.dialedHost, p.nodeName); owner != nil {
			shared = append(shared, sharedAddress{host: host, owner: owner.name})
			notTried = p.hosts[i+1:]
			collided = true
			break
		}

		// Must look for new node name in the unlikely event that node names do not agree.
		if p.nodeName != nv.name {
			if clstr.isKnownNode(ps, nv.name) {
				clstr.logSkippedAddress(p, host, nv.name)
				shared = append(shared, sharedAddress{host: host, owner: nv.name})
				continue
			}
			logger.Logger.Warn("Peer node `%s` is different than actual node `%s` for host `%s`", p.nodeName, nv.name, host)
		}

		p.validated = &nv
		return
	}

	if collided || (len(shared) > 0 && len(shared) == len(p.hosts)) {
		logger.Logger.Warn("%s", newTranslationCollision(clstr.translatorName, p.nodeName, shared, notTried))
	}
}

// sharedOwner returns the name of another peer of this tend or a healthy
// node that also has host, or "".
func (clstr *Cluster) sharedOwner(ps *peers, p *peer, host *Host) string {
	for _, other := range ps.peers() {
		if other.nodeName == p.nodeName {
			continue
		}
		for _, h := range other.hosts {
			if sameAddress(h, host) {
				return other.nodeName
			}
		}
	}

	if node := clstr.healthyNodeAt(host, p.nodeName); node != nil {
		return node.name
	}
	return ""
}

// isKnownNode reports whether name is a cluster node, a node added this tend
// or a peer listed this tend.
func (clstr *Cluster) isKnownNode(ps *peers, name string) bool {
	return clstr.findNodeByName(name) != nil || ps.nodeByName(name) != nil || ps._peers.Get(name) != nil
}

// logSkippedAddress logs at warning level when the translator produced the
// address, since that is a mapping mistake, and at info level otherwise.
func (clstr *Cluster) logSkippedAddress(p *peer, host *Host, owner string) {
	level := logger.INFO
	if p.translated[host] {
		level = logger.WARNING
	}
	logger.Logger.LogAtLevel(level, "Skip address %s:%d of node %s because node %s also has it. Address translator %s",
		host.Name, host.Port, p.nodeName, owner, clstr.translatorName)
}

// sameAddress reports whether both hosts have the same port and hostname,
// ignoring case. Names are not resolved.
func sameAddress(h1, h2 *Host) bool {
	return h1.Port == h2.Port && strings.EqualFold(h1.Name, h2.Name)
}
