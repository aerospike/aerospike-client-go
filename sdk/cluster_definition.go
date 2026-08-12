//go:build go1.27

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

package sdk

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

// envUseServicesAlternate enables alternate service addresses without a code
// change.
const envUseServicesAlternate = "AEROSPIKE_USE_SERVICES_ALTERNATE"

// ClusterDefinition describes how to reach a cluster. Every setter returns the
// definition, so calls chain in any order; nothing connects until [Connect].
type ClusterDefinition struct {
	hosts []*as.Host

	user     string
	password string
	authMode as.AuthMode

	clusterName           string
	preferredRacks        []int
	useServicesAlternate  bool
	failIfNotConnected    bool
	ipMap                 map[string]string
	appID                 string
	tendTimeout           time.Duration
	loginTimeout          time.Duration
	systemSettings        SystemSettings
	tlsConfig             *tls.Config
	allowInsecureExternal bool

	pendingErr error
}

// NewClusterDefinition starts a definition with a single seed host.
func NewClusterDefinition(hostname string, port int) *ClusterDefinition {
	return WithHosts(as.NewHost(hostname, port))
}

// WithHosts starts a definition with several seed hosts.
func WithHosts(hosts ...*as.Host) *ClusterDefinition {
	d := &ClusterDefinition{
		hosts:              hosts,
		authMode:           as.AuthModeInternal,
		failIfNotConnected: true,
	}
	if v := strings.ToLower(strings.TrimSpace(os.Getenv(envUseServicesAlternate))); v == "true" || v == "1" || v == "yes" {
		d.useServicesAlternate = true
	}
	return d
}

// deferErr records the first configuration error, surfaced from Connect.
func (d *ClusterDefinition) deferErr(err error) *ClusterDefinition {
	if d.pendingErr == nil {
		d.pendingErr = err
	}
	return d
}

// WithNativeCredentials selects internal authentication. An empty user
// disables authentication.
func (d *ClusterDefinition) WithNativeCredentials(user, password string) *ClusterDefinition {
	d.user, d.password, d.authMode = user, password, as.AuthModeInternal
	return d
}

// WithExternalCredentials selects external authentication (for example LDAP).
// It requires TLS: without it the password would cross the wire in the clear,
// so Connect rejects the combination.
func (d *ClusterDefinition) WithExternalCredentials(user, password string) *ClusterDefinition {
	d.user, d.password, d.authMode = user, password, as.AuthModeExternal
	return d
}

// WithExternalInsecureCredentials selects external authentication that sends
// the clear password with or without TLS. It is the explicit opt-in for test
// setups.
//
// The login payload is identical to [ClusterDefinition.WithExternalCredentials];
// the difference is that this form waives the TLS requirement instead of
// refusing to send a password in the clear.
func (d *ClusterDefinition) WithExternalInsecureCredentials(user, password string) *ClusterDefinition {
	d.user, d.password, d.authMode = user, password, as.AuthModeExternal
	d.allowInsecureExternal = true
	return d
}

// WithCertificateCredentials selects PKI authentication. It requires TLS and a
// TLS name on every host.
func (d *ClusterDefinition) WithCertificateCredentials() *ClusterDefinition {
	d.authMode = as.AuthModePKI
	return d
}

// AuthMode reports the configured authentication mode.
func (d *ClusterDefinition) AuthMode() as.AuthMode { return d.authMode }

// ValidateClusterNameIs refuses to connect to a differently-named cluster.
func (d *ClusterDefinition) ValidateClusterNameIs(name string) *ClusterDefinition {
	d.clusterName = name
	return d
}

// PreferringRacks enables rack awareness with an ordered rack preference.
func (d *ClusterDefinition) PreferringRacks(racks ...int) *ClusterDefinition {
	d.preferredRacks = racks
	return d
}

// UsingServicesAlternate uses alternate service endpoints (NAT or a service
// mesh).
func (d *ClusterDefinition) UsingServicesAlternate() *ClusterDefinition {
	d.useServicesAlternate = true
	return d
}

// FailIfNotConnected controls whether Connect errors when the cluster is
// unreachable. The default is true; false builds a partial cluster.
func (d *ClusterDefinition) FailIfNotConnected(v bool) *ClusterDefinition {
	d.failIfNotConnected = v
	return d
}

// WithIPMap translates server-reported addresses to reachable ones.
func (d *ClusterDefinition) WithIPMap(m map[string]string) *ClusterDefinition {
	d.ipMap = m
	return d
}

// WithSystemSettings sets the cluster-wide settings.
func (d *ClusterDefinition) WithSystemSettings(s SystemSettings) *ClusterDefinition {
	d.systemSettings = s
	return d
}

// AppID tags traffic with an application identifier reported to the server.
func (d *ClusterDefinition) AppID(name string) *ClusterDefinition {
	d.appID = name
	return d
}

// TendTimeout sets the timeout for cluster-tend info and admin commands.
func (d *ClusterDefinition) TendTimeout(t time.Duration) *ClusterDefinition {
	d.tendTimeout = t
	return d
}

// LoginTimeout sets the timeout for the initial connect and login handshake.
func (d *ClusterDefinition) LoginTimeout(t time.Duration) *ClusterDefinition {
	d.loginTimeout = t
	return d
}

// WithTLSConfig supplies a pre-built TLS configuration.
func (d *ClusterDefinition) WithTLSConfig(c *tls.Config) *ClusterDefinition {
	d.tlsConfig = c
	return d
}

// WithTLSConfigOf starts the TLS builder.
func (d *ClusterDefinition) WithTLSConfigOf() *TLSBuilder {
	return &TLSBuilder{def: d}
}

// TLSBuilder builds a TLS configuration for a [ClusterDefinition].
type TLSBuilder struct {
	def *ClusterDefinition

	tlsName        string
	caFile         string
	clientCertFile string
	clientKeyFile  string
}

// TLSName sets the name used for certificate validation, SNI and hostname
// override. It is applied to every host that has no TLS name of its own.
func (b *TLSBuilder) TLSName(name string) *TLSBuilder { b.tlsName = name; return b }

// CAFile sets the certificate authority PEM. It is required.
func (b *TLSBuilder) CAFile(path string) *TLSBuilder { b.caFile = path; return b }

// ClientCertFile sets the client certificate PEM for mutual TLS.
func (b *TLSBuilder) ClientCertFile(path string) *TLSBuilder { b.clientCertFile = path; return b }

// ClientKeyFile sets the client private key PEM for mutual TLS.
func (b *TLSBuilder) ClientKeyFile(path string) *TLSBuilder { b.clientKeyFile = path; return b }

// Done builds the TLS configuration and returns the parent definition.
//
// TLS misconfiguration surfaces here rather than at connect time: a missing CA
// file, an unreadable or unparsable PEM, or a rejected certificate and key
// pair all fail now. A client certificate is used only when both the
// certificate and the key are set.
func (b *TLSBuilder) Done() *ClusterDefinition {
	d := b.def
	if b.caFile == "" {
		return d.deferErr(NewError(KindInvalidArgument, "TLS configuration requires a CA file"))
	}
	caPEM, err := os.ReadFile(b.caFile)
	if err != nil {
		return d.deferErr(NewError(KindInvalidArgument, "cannot read CA file %q: %s", b.caFile, err))
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return d.deferErr(NewError(KindInvalidArgument, "CA file %q contains no usable certificate", b.caFile))
	}

	cfg := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if b.clientCertFile != "" && b.clientKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(b.clientCertFile, b.clientKeyFile)
		if err != nil {
			return d.deferErr(NewError(KindInvalidArgument, "cannot load client certificate and key: %s", err))
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	if b.tlsName != "" {
		cfg.ServerName = b.tlsName
		for _, h := range d.hosts {
			if h.TLSName == "" {
				h.TLSName = b.tlsName
			}
		}
	}
	d.tlsConfig = cfg
	return d
}

// validate checks the definition before any I/O.
func (d *ClusterDefinition) validate() error {
	if d.pendingErr != nil {
		return d.pendingErr
	}
	if len(d.hosts) == 0 {
		return NewError(KindInvalidArgument, "at least one seed host is required")
	}
	if d.authMode == as.AuthModePKI {
		if d.tlsConfig == nil {
			return NewError(KindInvalidArgument, "certificate authentication requires a TLS configuration")
		}
		var missing []string
		for _, h := range d.hosts {
			if h.TLSName == "" {
				missing = append(missing, h.Name)
			}
		}
		if len(missing) > 0 {
			return NewError(KindInvalidArgument,
				"certificate authentication requires a TLS name on every host; missing for: %s",
				strings.Join(missing, ", "))
		}
	}
	if d.authMode == as.AuthModeExternal && d.tlsConfig == nil && !d.allowInsecureExternal {
		return NewError(KindInvalidArgument,
			"external authentication requires TLS; use WithExternalInsecureCredentials to send the password in the clear deliberately")
	}
	return nil
}

// buildPolicy assembles the core client policy.
func (d *ClusterDefinition) buildPolicy(sys SystemSettings) *as.ClientPolicy {
	p := as.NewClientPolicy()
	p.User = d.user
	p.Password = d.password
	p.AuthMode = d.authMode
	p.ClusterName = d.clusterName
	p.UseServicesAlternate = d.useServicesAlternate
	p.FailIfNotConnected = d.failIfNotConnected
	p.TlsConfig = d.tlsConfig
	p.IpMap = d.ipMap

	if len(d.preferredRacks) > 0 {
		p.RackAware = true
		p.RackIds = d.preferredRacks
	}
	if d.tendTimeout > 0 {
		p.Timeout = d.tendTimeout
	}
	if d.loginTimeout > 0 {
		p.LoginTimeout = d.loginTimeout
	}
	sys.applyTo(p)
	return p
}

// Connect validates the definition, applies any SDK configuration file, opens
// the connection and returns a [Cluster].
func (d *ClusterDefinition) Connect() (*Cluster, error) {
	if err := d.validate(); err != nil {
		return nil, err
	}

	// Resolve the configuration file over the programmatic settings, and
	// register the behaviors it declares.
	loaded, sys := loadConfigAtConnect(d.clusterName, d.systemSettings)

	policy := d.buildPolicy(sys)
	client, err := as.NewClientWithPolicyAndHost(policy, d.hosts...)
	if err != nil {
		return nil, WrapError(err)
	}
	if d.failIfNotConnected && !client.IsConnected() {
		client.Close()
		return nil, NewError(KindConnection, "connected to the cluster but it reports as not connected")
	}

	c := newClient(client, d.clusterName, sys)
	if loaded.path != "" {
		c.startConfigMonitor(loaded, d.clusterName, d.systemSettings)
	}
	return &Cluster{client: c}, nil
}
