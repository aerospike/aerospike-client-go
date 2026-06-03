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

package aerospike

import (
	"bytes"
	"fmt"
	"net"
	"strconv"

	"github.com/aerospike/aerospike-client-go/v8/internal/version"
	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

type nodesToAddT map[string]*Node

func (nta nodesToAddT) addNodeIfNotExists(ndv *nodeValidator, cluster *Cluster) bool {
	_, exists := nta[ndv.name]
	if !exists {
		// found a new node
		node := cluster.createNode(ndv)
		nta[ndv.name] = node
	}
	return exists
}

// Validates a Database server node
type nodeValidator struct {
	name        string
	aliases     []*Host
	primaryHost *Host

	seedOnlyCluster    bool
	detectLoadBalancer bool

	sessionInfo *sessionInfo

	features      int
	serverVersion version.Version
}

func (ndv *nodeValidator) seedNodes(cluster *Cluster, host *Host, nodesToAdd nodesToAddT) Error {
	if err := ndv.setAliases(host); err != nil {
		return err
	}

	found := false
	var resultErr Error
	for _, alias := range ndv.aliases {
		if resultErr = ndv.validateAlias(cluster, alias); resultErr != nil {
			logger.Logger.Debug("Alias %s failed: %s", alias, resultErr)
			continue
		}

		found = true
		nodesToAdd.addNodeIfNotExists(ndv, cluster)
	}

	if !found {
		return resultErr
	}
	return nil
}

func (ndv *nodeValidator) validateNode(cluster *Cluster, host *Host) Error {
	clientPolicy := cluster.clientPolicy.Load()
	if clusterNodes := cluster.GetNodes(); clientPolicy != nil && clientPolicy.IgnoreOtherSubnetAliases && len(clusterNodes) > 0 {
		masterHostname := clusterNodes[0].host.Name
		ip, ipnet, err := net.ParseCIDR(masterHostname + "/24")
		if err != nil {
			logger.Logger.Error("%s", err.Error())
			return newError(types.NO_AVAILABLE_CONNECTIONS_TO_NODE, "Failed parsing hostname...")
		}

		stop := ip.Mask(ipnet.Mask)
		stop[3] += 255
		if bytes.Compare(net.ParseIP(host.Name).To4(), ip.Mask(ipnet.Mask).To4()) >= 0 && bytes.Compare(net.ParseIP(host.Name).To4(), stop.To4()) >= 0 {
			return newError(types.NO_AVAILABLE_CONNECTIONS_TO_NODE, "Ignored hostname from other subnet...")
		}
	}

	if err := ndv.setAliases(host); err != nil {
		return err
	}

	var resultErr Error
	for _, alias := range ndv.aliases {
		if err := ndv.validateAlias(cluster, alias); err != nil {
			resultErr = chainErrors(err, resultErr)
			logger.Logger.Debug("Aliases %s failed: %s", alias, err)
			continue
		}
		return nil
	}

	return resultErr
}

func (ndv *nodeValidator) setAliases(host *Host) Error {
	ndv.detectLoadBalancer = !ndv.seedOnlyCluster

	// IP addresses do not need a lookup
	ip := net.ParseIP(host.Name)
	if ip != nil {
		// avoid detecting load balancer on localhost
		ndv.detectLoadBalancer = ndv.detectLoadBalancer && !ip.IsLoopback()

		aliases := make([]*Host, 1)
		aliases[0] = NewHost(host.Name, host.Port)
		aliases[0].TLSName = host.TLSName
		ndv.aliases = aliases
	} else {
		addresses, err := net.LookupHost(host.Name)
		if err != nil {
			logger.Logger.Error("Host lookup failed with error: %s", err.Error())
			return errToAerospikeErr(nil, err)
		}
		aliases := make([]*Host, len(addresses))
		for idx, addr := range addresses {
			aliases[idx] = NewHost(addr, host.Port)
			aliases[idx].TLSName = host.TLSName

			// avoid detecting load balancer on localhost
			if ip := net.ParseIP(host.Name); ip != nil && ip.IsLoopback() {
				ndv.detectLoadBalancer = false
			}
		}
		ndv.aliases = aliases
	}
	logger.Logger.Debug("Node Validator has %d nodes and they are: %v", len(ndv.aliases), ndv.aliases)
	return nil
}

func (ndv *nodeValidator) validateAlias(cluster *Cluster, alias *Host) Error {
	clientPolicy := *cluster.clientPolicy.Load()
	clientPolicy.Timeout /= 2

	conn, err := NewConnection(&clientPolicy, alias)
	if err != nil {
		return err
	}
	defer conn.Close()

	if clientPolicy.RequiresAuthentication() {
		// need to authenticate
		acmd := newLoginCommand(conn.dataBuffer)
		err = acmd.login(&clientPolicy, conn, cluster.Password())
		if err != nil {
			return err
		}

		ndv.sessionInfo = acmd.sessionInfo()
	}

	// check to make sure we have actually connected
	info, err := conn.RequestInfo("build")
	if err != nil {
		return err
	}

	if _, exists := info["ERROR:80:not authenticated"]; exists {
		return ErrNotAuthenticated.err()
	}

	hasClusterName := len(clientPolicy.ClusterName) > 0

	infoKeys := []string{"node", "partition-generation", "build"}
	if hasClusterName {
		infoKeys = append(infoKeys, "cluster-name")
	}

	// Build the service info command(s) to request.
	// In ServicesAuto mode we request both std and alt so we can detect which
	// variant the server is reachable under.
	stdCmd := clientPolicy.serviceStringFor(ServicesMain)
	altCmd := clientPolicy.serviceStringFor(ServicesAlternate)
	addressCommand := clientPolicy.serviceString() // std for Auto/Main, alt for Alternate

	if ndv.detectLoadBalancer && !ndv.seedOnlyCluster {
		if clientPolicy.ServicesType == ServicesAuto {
			infoKeys = append(infoKeys, stdCmd, altCmd)
		} else {
			infoKeys = append(infoKeys, addressCommand)
		}
	}

	infoMap, err := conn.RequestInfo(infoKeys...)
	if err != nil {
		return err
	}

	nodeName, exists := infoMap["node"]
	if !exists {
		return newError(types.INVALID_NODE_ERROR, "Invalid node alias:"+alias.String())
	}

	genStr, exists := infoMap["partition-generation"]
	if !exists {
		return newError(types.INVALID_NODE_ERROR, "Invalid partition-generation for node:"+alias.String())
	}

	gen, nerr := strconv.Atoi(genStr)
	if nerr != nil {
		return newError(types.PARSE_ERROR, fmt.Sprintf("Invalid partition-generation for Node %s (%s), value: %s", nodeName, alias.String(), genStr))
	}

	if gen == -1 {
		return newError(types.INVALID_NODE_ERROR, fmt.Sprintf("Node %s (%s) is not yet fully initialized", nodeName, alias.String()))
	}

	if hasClusterName {
		id := infoMap["cluster-name"]

		if len(id) == 0 || id != clientPolicy.ClusterName {
			return newError(types.CLUSTER_NAME_MISMATCH_ERROR, fmt.Sprintf("Node %s (%s) expected cluster name `%s` but received `%s`", nodeName, alias.String(), clientPolicy.ClusterName, id))
		}
	}

	// check if the serverVersion, aka server version, is a valid semantic version.
	// If build does not exist we assume the server is not using semantic versioning.
	// This is done for backward compatibility with older servers
	if serverVersionString, exists := infoMap["build"]; exists {
		if version, err := version.Parse(serverVersionString); err != nil {
			return newCommonError(err, fmt.Sprintf("Node %s %s version is invalid: %s", nodeName, alias.String(), serverVersionString))
		} else {
			ndv.serverVersion = *version
		}
	}

	if featError := ndv.setFeatures(alias); featError != nil {
		return featError
	}

	// Load-balancer detection and ServicesAuto resolution.
	if ndv.detectLoadBalancer && !ndv.seedOnlyCluster {
		if clientPolicy.ServicesType == ServicesAuto {
			alias, err = ndv.resolveServicesAuto(cluster, &clientPolicy, alias, infoMap, stdCmd, altCmd)
			if err != nil {
				return err
			}
		} else if peersStr, exists := infoMap[addressCommand]; exists {
			alias = ndv.applyLoadBalancerDetection(cluster, &clientPolicy, alias, peersStr)
		}
	}

	ndv.name = nodeName
	ndv.primaryHost = alias

	return nil
}

// resolveServicesAuto is called when ServicesType == ServicesAuto. It requests
// both the std and alt service addresses, compares them against the seed alias,
// and resolves the correct ServicesType for the cluster. It also performs
// load-balancer detection and returns the (possibly rewritten) alias.
func (ndv *nodeValidator) resolveServicesAuto(
	cluster *Cluster,
	clientPolicy *ClientPolicy,
	alias *Host,
	infoMap map[string]string,
	stdCmd, altCmd string,
) (*Host, Error) {
	parseList := func(cmd string) []*Host {
		raw, ok := infoMap[cmd]
		if !ok || raw == "" {
			return nil
		}
		pp := peerListParser{buf: []byte("[" + raw + "]")}
		hosts, err := pp.readHosts(alias.TLSName)
		if err != nil {
			logger.Logger.Error("Failed to parse `%s` results for alias `%s`: %s", cmd, alias.String(), err.Error())
			return nil
		}
		return hosts
	}

	stdHosts := parseList(stdCmd)
	altHosts := parseList(altCmd)

	aliasMatchesAny := func(list []*Host) bool {
		for _, h := range list {
			for _, a := range ndv.aliases {
				if h.equals(a) {
					return true
				}
			}
		}
		return false
	}

	stdMatch := aliasMatchesAny(stdHosts)
	altMatch := aliasMatchesAny(altHosts)

	var resolved ServicesType
	var winningList []*Host

	switch {
	case stdMatch:
		resolved = ServicesMain
		winningList = stdHosts
	case altMatch:
		resolved = ServicesAlternate
		winningList = altHosts
	default:
		// Neither list contains the seed — likely a load balancer.
		// Try to connect to std addresses first, then alt.
		resolved, winningList = ndv.tryConnectLists(clientPolicy, cluster, stdHosts, altHosts)
	}

	logger.Logger.Debug("ServicesAuto resolved to %v for seed %s", resolved, alias.String())
	cluster.setResolvedServicesType(resolved)

	// Apply LB rewrite if the seed is not in the winning list.
	if winningList != nil && !aliasMatchesAny(winningList) {
		alias = ndv.tryReplaceWithConnectable(clientPolicy, cluster, alias, winningList)
	}

	return alias, nil
}

// tryConnectLists tries to establish a connection to addresses in stdHosts
// first, then altHosts. Returns the resolved ServicesType and the winning list.
func (ndv *nodeValidator) tryConnectLists(
	clientPolicy *ClientPolicy,
	cluster *Cluster,
	stdHosts, altHosts []*Host,
) (ServicesType, []*Host) {
	tryList := func(hosts []*Host) bool {
		for _, h := range hosts {
			conn, err := NewConnection(clientPolicy, h)
			if err != nil {
				continue
			}
			conn.Close()
			return true
		}
		return false
	}

	if tryList(stdHosts) {
		return ServicesMain, stdHosts
	}
	if tryList(altHosts) {
		return ServicesAlternate, altHosts
	}
	// Fall back to main if neither list yields a connection.
	return ServicesMain, stdHosts
}

// tryReplaceWithConnectable replaces the current alias with the first
// connectable host from hostAddresses (load-balancer rewrite). If none
// connects, the original alias is returned unchanged.
func (ndv *nodeValidator) tryReplaceWithConnectable(
	clientPolicy *ClientPolicy,
	cluster *Cluster,
	alias *Host,
	hostAddresses []*Host,
) *Host {
	logger.Logger.Info("Host `%s` seems to be a load balancer. It is going to be replaced by `%v`", alias.String(), hostAddresses[0])

	for _, h := range hostAddresses {
		hconn, err := NewConnection(clientPolicy, h)
		if err != nil {
			continue
		}
		defer hconn.Close()

		if clientPolicy.RequiresAuthentication() {
			acmd := newLoginCommand(hconn.dataBuffer)
			if err = acmd.login(clientPolicy, hconn, cluster.Password()); err != nil {
				continue
			}
			ndv.sessionInfo = acmd.sessionInfo()
		}

		ndv.aliases = hostAddresses
		return h
	}

	logger.Logger.Info("Inaccessible address `%s` as cluster seed. access-address is probably not configured on server.", alias.String())
	return alias
}

// applyLoadBalancerDetection is the non-auto path: it checks whether the seed
// is a load balancer using the pre-selected service command response and, if
// so, replaces it with the first connectable real host.
func (ndv *nodeValidator) applyLoadBalancerDetection(
	cluster *Cluster,
	clientPolicy *ClientPolicy,
	alias *Host,
	peersStr string,
) *Host {
	var hostAddress []*Host
	peerParser := peerListParser{buf: []byte("[" + peersStr + "]")}
	var err Error
	if hostAddress, err = peerParser.readHosts(alias.TLSName); err != nil {
		logger.Logger.Error("Failed to parse service results for `%s`: %s", alias.String(), err.Error())
		return alias
	}

	if len(hostAddress) == 0 {
		return alias
	}

	isLoadBalancer := true
LOAD_BALANCER:
	for _, h := range hostAddress {
		for _, a := range ndv.aliases {
			if h.equals(a) {
				isLoadBalancer = false
				break LOAD_BALANCER
			}
		}
	}

	if !isLoadBalancer {
		return alias
	}

	return ndv.tryReplaceWithConnectable(clientPolicy, cluster, alias, hostAddress)
}

func (ndv *nodeValidator) setFeatures(alias *Host) Error {
	if ndv.serverVersion.IsGreaterOrEqual(version.ServerVersionPScan) {
		ndv.features |= _SUPPORTS_PARTITION_SCAN
	} else {
		// This client requires partition scan support. Partition scans were first
		// supported in server version 4.9. Do not allow any server node into the
		// cluster that is running server version < 4.9.
		if (ndv.features & _SUPPORTS_PARTITION_SCAN) == 0 {
			return newError(types.INVALID_NODE_ERROR, fmt.Sprintf("Node %s (%s) is version < 4.9. This client supports server versions >= 4.9", ndv.name, alias.String()))
		}
	}
	if ndv.serverVersion.IsGreaterOrEqual(version.ServerVersionQueryShow) {
		ndv.features |= _SUPPORTS_QUERY_SHOW
	}
	if ndv.serverVersion.IsGreaterOrEqual(version.ServerVersionPQueryBatchAny) {
		ndv.features |= _SUPPORTS_BATCH_ANY
		ndv.features |= _SUPPORTS_PARTITION_QUERY
	}
	if ndv.serverVersion.IsGreaterOrEqual(version.ServerVersionQueryOpsProjectionExt) {
		ndv.features |= _SUPPORTS_QUERY_OPS_PROJECTION_EXT
	}

	return nil
}
