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

package aerospike_test

import (
	"crypto/x509"
	"errors"
	"os"
	"testing"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const l2CnOnlyProfile = " (run_l2.sh DISCOVERY_SUBSET=ee L2_PROFILE=cn-only)"

func TestL2CnOnlyCertificateIsRejected(t *testing.T) {
	skipUnlessL2(t)
	if os.Getenv("DISCOVERY_SUBSET") != "ee" {
		t.Skip("N-17 is ee only: it needs server TLS and TLS listeners" + l2CnOnlyProfile + "; subset is " + os.Getenv("DISCOVERY_SUBSET"))
	}
	if os.Getenv("DISCOVERY_TLS_CERT_SHAPE") != "cn-only" {
		t.Skip("N-17 needs DISCOVERY_TLS_CERT_SHAPE=cn-only, a server certificate with the tls-name only as its CN" + l2CnOnlyProfile + "; shape is " + os.Getenv("DISCOVERY_TLS_CERT_SHAPE"))
	}
	host, nodes := l2Discover(t)
	first := nodes[0]
	if first.tlsPort == 0 {
		t.Fatal("no TLS listener for " + first.name)
	}
	if first.tlsName == "" {
		t.Fatal("no tls-name for " + first.name)
	}

	policy := l2PortMapPolicy(t, l2TLSPortMapByEndpoint(host, nodes))
	policy.TlsConfig = l2TLSConfig(t)
	client, err := as.NewClientWithPolicyAndHost(policy, &as.Host{Name: host, TLSName: first.tlsName, Port: first.tlsPort})
	if err == nil {
		client.Close()
		t.Fatalf("TLS handshake accepted a certificate that carries %s only as its CN; nodes=%v", first.tlsName, client.GetNodeNames())
	}
	var mismatch x509.HostnameError
	if !errors.As(err, &mismatch) || mismatch.Host != first.tlsName {
		t.Fatalf("CN-only certificate failed with %v, want a certificate name mismatch for %s", err, first.tlsName)
	}
}
