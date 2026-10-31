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
	"io"
	"net/netip"
	"strconv"
	"strings"

	"github.com/aerospike/aerospike-client-go/v8/logger"
	"github.com/aerospike/aerospike-client-go/v8/types"
)

var aeroerr = newError(types.PARSE_ERROR, "Error parsing peers list.")

func parsePeers(cluster *Cluster, node *Node) (*peerListParser, Error) {
	policy := cluster.clientPolicy.Load()
	cmd := policy.peersString()

	// The alternate list omits peers with no alternate address, so the
	// standard list is fetched alongside to name them.
	commands := []string{cmd}
	standardCmd := ""
	if policy.UseServicesAlternate {
		standardCmd = "peers-clear-std"
		if policy.TlsConfig != nil {
			standardCmd = "peers-tls-std"
		}
		commands = append(commands, standardCmd)
	}

	info, err := node.RequestInfo(&cluster.infoPolicy, commands...)
	if err != nil {
		return nil, err
	}

	peersStr, exists := info[cmd]
	if !exists {
		return nil, newError(types.PARSE_ERROR, "Info Command response was empty.")
	}

	sourceList := SourceListStandard
	if policy.UseServicesAlternate {
		sourceList = SourceListAlternate
	}

	p := peerListParser{
		buf:            []byte(peersStr),
		translator:     cluster.addressTranslator,
		translatorName: cluster.translatorName,
		onMiss:         cluster.translationOnMiss,
		sourceList:     sourceList,
	}
	if err := p.Parse(); err != nil {
		return nil, err
	}

	if standardCmd != "" {
		p.addMissingAlternates(info[standardCmd])
	}

	return &p, nil
}

// addMissingAlternates names the peers present in the standard list but
// absent from the parsed alternate list.
func (p *peerListParser) addMissingAlternates(standardPeersStr string) {
	standardParser := peerListParser{buf: []byte(standardPeersStr)}
	if err := standardParser.Parse(); err != nil {
		return
	}

	for _, standardPeer := range standardParser.peers {
		found := false
		for _, alternatePeer := range p.peers {
			if alternatePeer.nodeName == standardPeer.nodeName {
				found = true
				break
			}
		}
		if !found {
			p.missingAlternates = append(p.missingAlternates, standardPeer.nodeName)
		}
	}
}

type peerListParser struct {
	buf []byte
	pos int

	defPort *int64
	gen     *int64
	peers   []*peer

	// translator, when set, translates each advertised address into a
	// client-reachable one. onMiss decides what a miss does and sourceList
	// names the advertised list being parsed.
	translator     AddressTranslator
	translatorName string
	onMiss         TranslationMissMode
	sourceList     SourceList

	// nodeName and tlsName identify the peer whose hosts are being parsed.
	nodeName string
	tlsName  string

	// parsedHostCount counts the advertised hosts of the current peer before
	// translation. missingAlternates names peers with no alternate address.
	parsedHostCount   int
	missingAlternates []string
}

func (p *peerListParser) generation() int64 {
	if p.gen != nil {
		return *p.gen
	}
	return 0
}

func (p *peerListParser) Expect(ch byte) bool {
	if p.pos == len(p.buf) {
		return false
	}

	if p.buf[p.pos] == ch {
		p.pos++
		return true
	}
	return false
}

func (p *peerListParser) readByte() *byte {
	if p.pos == len(p.buf) {
		return nil
	}

	ch := p.buf[p.pos]
	p.pos++
	return &ch
}

func (p *peerListParser) PeekByte() *byte {
	if p.pos == len(p.buf) {
		return nil
	}

	ch := p.buf[p.pos]
	return &ch
}

func (p *peerListParser) readInt64() (*int64, Error) {
	if p.pos == len(p.buf) {
		return nil, newErrorAndWrap(io.EOF, types.PARSE_ERROR, "Error Parsing the peers list")
	}

	if p.buf[p.pos] == ',' {
		return nil, nil
	}

	begin := p.pos
	for p.pos < len(p.buf) {
		ch := p.buf[p.pos]
		if ch == ',' {
			break
		}
		p.pos++
	}

	num, err := strconv.ParseInt(string(p.buf[begin:p.pos]), 10, 64)
	if err != nil {
		return nil, newErrorAndWrap(err, types.PARSE_ERROR, "Error Parsing the peers list")
	}
	return &num, nil
}

func (p *peerListParser) readString() (string, Error) {
	if p.pos == len(p.buf) {
		return "", newErrorAndWrap(io.EOF, types.PARSE_ERROR, "Error Parsing the peers list")
	}

	if p.buf[p.pos] == ',' {
		return "", nil
	}

	begin := p.pos
	bracket := p.buf[p.pos] == '['
	for p.pos < len(p.buf) {
		ch := p.buf[p.pos]
		if ch == ',' {
			break
		}

		if ch == ']' {
			if !bracket {
				break
			}
			bracket = false
		}
		p.pos++
	}

	return string(p.buf[begin:p.pos]), nil
}

func (p *peerListParser) ParseHost(host string) (*Host, Error) {
	ppos := -1
	bpos := -1
	for i := 0; i < len(host); i++ {
		switch host[i] {
		case ':':
			ppos = i
		case ']':
			ppos = -1
			bpos = i
		}
	}

	port := 0
	if p.defPort != nil {
		port = int(*p.defPort)
	}
	var err error
	if ppos >= 0 {
		portStr := host[ppos+1:]
		port, err = strconv.Atoi(portStr)
		if err != nil {
			return nil, newErrorAndWrap(err, types.PARSE_ERROR, "Error Parsing the peers list")
		}
	}

	var addr string
	if bpos >= 0 {
		addr = host[1:bpos]
	} else {
		if ppos >= 0 {
			addr = host[:ppos]
		} else {
			addr = host
		}
	}

	if p.translator != nil {
		advertised := Endpoint{NodeName: p.nodeName, TLSName: p.tlsName, Host: addr, Port: port, SourceList: p.sourceList}
		if translated, found := p.translator.Translate(advertised); found {
			addr = translated.Name
			port = translated.Port
			// Debug level keeps the per-peer, per-tend translation off the
			// normal log path; raise to Debug to trace a failed `Add node`
			// back to the advertised address.
			logger.Logger.Debug("Translated peer `%s` -> `%s:%d`", host, addr, port)
		} else if p.onMiss == TranslationMissStrict {
			logger.Logger.Warn("Address translator %s has no mapping for node %s advertised address %s:%d in the %s list. Add a mapping for that endpoint or node, or set TranslationOnMiss to pass_through", p.translatorName, p.nodeName, addr, port, p.sourceList.String())
			return nil, nil
		}
	}

	return NewHost(addr, port), nil
}

// parseTranslationValue splits a translation map value into the host and port
// to connect to. An IP literal value is a bare host and keeps the advertised
// port. A trailing ":<number>" replaces the port. Any other value is a bare
// host.
func parseTranslationValue(value string, advertisedPort int) (translatedHost string, translatedPort int) {
	firstColon := strings.IndexByte(value, ':')
	if firstColon < 0 {
		return value, advertisedPort
	}

	lastColon := strings.LastIndexByte(value, ':')
	if firstColon != lastColon {
		// Only a value with more than one colon can be an IPv6 literal.
		if _, err := netip.ParseAddr(value); err == nil {
			return value, advertisedPort
		}
	}

	if port, err := strconv.Atoi(value[lastColon+1:]); err == nil {
		return value[:lastColon], port
	}

	return value, advertisedPort
}

func (p *peerListParser) readHosts(tlsName string) ([]*Host, Error) {
	if !p.Expect('[') {
		return nil, aeroerr
	}

	p.parsedHostCount = 0
	hostList := []*Host{}
	for {
		hostStr, err := p.readString()
		if err != nil {
			return nil, err
		}

		if hostStr == "" {
			break
		}

		p.parsedHostCount++
		host, err := p.ParseHost(hostStr)
		if err != nil {
			return nil, aeroerr
		}

		// A nil host is a strict translation miss; the address is dropped and
		// retried on the next tend.
		if host != nil {
			host.TLSName = tlsName
			hostList = append(hostList, host)
		}

		if !p.Expect(',') {
			break
		}
	}

	if !p.Expect(']') {
		return nil, aeroerr
	}

	return hostList, nil
}

func (p *peerListParser) readPeer() (*peer, Error) {
	if !p.Expect('[') {
		return nil, nil
	}

	nodeName, err := p.readString()
	if err != nil {
		return nil, err
	}

	if !p.Expect(',') {
		return nil, aeroerr
	}
	tlsName, err := p.readString()
	if err != nil {
		return nil, err
	}

	if !p.Expect(',') {
		return nil, aeroerr
	}

	p.nodeName = nodeName
	p.tlsName = tlsName
	hostList, err := p.readHosts(tlsName)
	if err != nil {
		return nil, err
	}

	if p.sourceList == SourceListAlternate && p.parsedHostCount == 0 {
		p.missingAlternates = append(p.missingAlternates, nodeName)
	}

	if !p.Expect(']') {
		return nil, aeroerr
	}

	nodeData := &peer{nodeName: nodeName, tlsName: tlsName, hosts: hostList}
	return nodeData, nil
}

func (p *peerListParser) readNodeList() ([]*peer, Error) {
	ch := p.readByte()
	if ch == nil {
		return nil, nil
	}

	if *ch != '[' {
		return nil, aeroerr
	}

	nodeList := []*peer{}
	for {
		node, err := p.readPeer()
		if err != nil {
			return nil, err
		}

		if node == nil {
			break
		}

		nodeList = append(nodeList, node)

		if !p.Expect(',') {
			break
		}
	}

	if !p.Expect(']') {
		return nil, aeroerr
	}

	return nodeList, nil
}

func (p *peerListParser) Parse() Error {
	var err Error
	p.gen, err = p.readInt64()
	if err != nil {
		return err
	}

	if !p.Expect(',') {
		return aeroerr
	}

	p.defPort, err = p.readInt64()
	if err != nil {
		return err
	}

	if !p.Expect(',') {
		return aeroerr
	}

	p.peers, err = p.readNodeList()
	if err != nil {
		return err
	}

	return nil
}
