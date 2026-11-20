package aerospike

import "net"

// isAlias reports whether a validated host answers as another known node, and no node with a
// different name already answers at the dialed socket. The dialed host is compared, since load
// balancer detection replaces the validator's primary host with the node's own address.
func (clstr *Cluster) isAlias(ps *peers, nv *nodeValidator, dialed *Host) bool {
	known := clstr.findNodeByName(nv.name) != nil || ps.nodeByName(nv.name) != nil || ps._peers.Get(nv.name) != nil
	return known && !clstr.socketHasOtherName(ps, nv.name, dialed)
}

func (clstr *Cluster) socketHasOtherName(ps *peers, name string, dialed *Host) bool {
	ips := resolve(dialed.Name)

	for _, node := range clstr.GetNodes() {
		if node.name != name && node.IsActive() && hasIP(ips, dialed.Port, node.host) {
			return true
		}
	}

	for _, node := range ps.nodes() {
		if node.name != name && hasIP(ips, dialed.Port, node.host) {
			return true
		}
	}
	return false
}

func hasIP(ips []string, port int, h *Host) bool {
	if h.Port != port {
		return false
	}
	for _, ip := range resolve(h.Name) {
		for _, d := range ips {
			if ip == d {
				return true
			}
		}
	}
	return false
}

func resolve(name string) []string {
	if ip := net.ParseIP(name); ip != nil {
		return []string{ip.String()}
	}
	ips, err := lookupHost(name)
	if err != nil {
		return nil
	}
	return ips
}
