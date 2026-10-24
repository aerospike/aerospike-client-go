package sdk

import (
	"context"
	"crypto/tls"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

type ClusterDefinition struct{}

func NewClusterDefinition(host string, port int) *ClusterDefinition {
	return &ClusterDefinition{}
}

func WithHosts(hosts ...*as.Host) *ClusterDefinition {
	return &ClusterDefinition{}
}

func (d *ClusterDefinition) WithNativeCredentials(user, pass string) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) WithExternalCredentials(user, pass string) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) WithExternalInsecureCredentials(user, pass string) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) WithCertificateCredentials() *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) ValidateClusterNameIs(name string) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) PreferringRacks(racks ...int) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) UsingServicesAlternate() *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) FailIfNotConnected(v bool) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) WithIPMap(m map[string]string) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) WithSystemSettings(s SystemSettings) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) AppID(name string) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) TendTimeout(dur time.Duration) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) LoginTimeout(dur time.Duration) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) WithTLSConfig(cfg *tls.Config) *ClusterDefinition {
	return d
}

func (d *ClusterDefinition) WithTLSConfigOf() *TLSBuilder {
	return nil
}

func (d *ClusterDefinition) Connect(ctx context.Context) (*Cluster, error) {
	return nil, nil
}

type TLSBuilder struct{}

type Cluster struct{}

func (c *Cluster) CreateSession(ctx context.Context, b *Behavior) (*Session, error) {
	return nil, nil
}

func (c *Cluster) Close() error {
	return nil
}

func (c *Cluster) Ping(ctx context.Context) error {
	return nil
}

func (c *Cluster) IsConnected() bool {
	return false
}

func (c *Cluster) ClusterName() string {
	return ""
}

func (c *Cluster) SystemSettings() SystemSettings {
	return SystemSettings{}
}

func (c *Cluster) Client() *as.Client {
	return nil
}

func (c *Cluster) SupportsMRT() bool {
	return false
}

func (c *Cluster) SupportsCDTPathExpressions() bool {
	return false
}

func (c *Cluster) SupportsStringOperations() bool {
	return false
}

func (c *Cluster) SupportsExtendedErrorDetail() bool {
	return false
}

func (c *Cluster) SupportsServerCompiledAEL() bool {
	return false
}

func (c *Cluster) SupportsBlobIndex() bool {
	return false
}

func (c *Cluster) EnableMetrics() {}

func (c *Cluster) DisableMetrics() {}

func (c *Cluster) MetricsEnabled() bool {
	return false
}
