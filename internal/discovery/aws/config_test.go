package aws

import (
	"strings"
	"testing"
	"time"
)

func TestConfigZeroValuesBecomeDocumentedDefaults(t *testing.T) {
	cfg, err := mustNormalize(Config{
		Region:  "eu-west-1",
		Filters: []Filter{{Name: "tag:aerospike-cluster", Values: []string{"prod-a"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != DefaultPort || cfg.Timeout != DefaultTimeout {
		t.Fatalf("defaults: port=%d timeout=%s", cfg.Port, cfg.Timeout)
	}
	if cfg.AddressField != AddressFieldPrivateIP || cfg.TLSName != "" || cfg.Endpoint != "" {
		t.Fatalf("optional defaults: %+v", cfg)
	}
	if cfg.Timeout != 80*time.Millisecond || cfg.Port != 3000 {
		t.Fatalf("got timeout=%s port=%d", cfg.Timeout, cfg.Port)
	}
}

func TestConfigRejectsInvalidSettings(t *testing.T) {
	cases := []struct {
		intent  string
		cfg     Config
		message string
	}{
		{
			intent:  "missing region is shown as 'null'",
			cfg:     Config{Filters: []Filter{{Name: "a", Values: []string{"b"}}}},
			message: "Invalid AWS EC2 seed provider region: 'null'",
		},
		{
			intent:  "region must not contain a space",
			cfg:     Config{Region: "US East", Filters: []Filter{{Name: "a", Values: []string{"b"}}}},
			message: "Invalid AWS EC2 seed provider region: 'US East'",
		},
		{
			intent:  "at least one filter is required",
			cfg:     Config{Region: "us-east-1"},
			message: "Invalid AWS EC2 seed provider filters: at least one filter is required",
		},
		{
			intent:  "filter name must not be empty",
			cfg:     Config{Region: "us-east-1", Filters: []Filter{{Name: "", Values: []string{"x"}}}},
			message: "Invalid AWS EC2 seed provider filter name: ''",
		},
		{
			intent:  "filter values must not be empty",
			cfg:     Config{Region: "us-east-1", Filters: []Filter{{Name: "subnet-id"}}},
			message: "Invalid AWS EC2 seed provider filter 'subnet-id' values: empty",
		},
		{
			intent:  "each filter value must not be empty",
			cfg:     Config{Region: "us-east-1", Filters: []Filter{{Name: "subnet-id", Values: []string{""}}}},
			message: "Invalid AWS EC2 seed provider filter 'subnet-id' value: ''",
		},
		{
			intent: "instance-state-name is reserved because the provider always adds running",
			cfg: Config{Region: "us-east-1", Filters: []Filter{
				{Name: "tag:a", Values: []string{"b"}},
				{Name: "instance-state-name", Values: []string{"stopped"}},
			}},
			message: "Invalid AWS EC2 seed provider filter name: 'instance-state-name': the provider adds instance-state-name=running",
		},
		{
			intent:  "port must be at most 65535",
			cfg:     Config{Region: "us-east-1", Filters: []Filter{{Name: "a", Values: []string{"b"}}}, Port: 65536},
			message: "Invalid AWS EC2 seed provider port: 65536",
		},
		{
			intent:  "address field must be one of the four EC2 fields",
			cfg:     Config{Region: "us-east-1", Filters: []Filter{{Name: "a", Values: []string{"b"}}}, AddressField: 9},
			message: "Invalid AWS EC2 seed provider addressField: 9",
		},
		{
			intent:  "endpoint must be http or https with a host and no query",
			cfg:     Config{Region: "us-east-1", Filters: []Filter{{Name: "a", Values: []string{"b"}}}, Endpoint: "ftp://ec2.example.com"},
			message: "expected http or https URL with a host and no query",
		},
		{
			intent: "credentials and credentialsHook cannot both be set",
			cfg: Config{
				Region:          "us-east-1",
				Filters:         []Filter{{Name: "a", Values: []string{"b"}}},
				Credentials:     &Credentials{AccessKeyID: "k", SecretAccessKey: "s"},
				CredentialsHook: func() (*Credentials, error) { return nil, nil },
			},
			message: "credentials and credentialsHook cannot both be set",
		},
		{
			intent: "secret access key must not be empty",
			cfg: Config{
				Region:      "us-east-1",
				Filters:     []Filter{{Name: "a", Values: []string{"b"}}},
				Credentials: &Credentials{AccessKeyID: "k", SecretAccessKey: ""},
			},
			message: "secret access key is empty",
		},
	}

	for _, tc := range cases {
		t.Run(tc.intent, func(t *testing.T) {
			_, err := mustNormalize(tc.cfg)
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("got %v, want substring %q", err, tc.message)
			}
		})
	}
}

func TestEC2EndpointUsesChinaDomainForCnRegions(t *testing.T) {
	env := SystemEnvironment{}
	if got := env.EC2Endpoint("cn-north-1"); got != "https://ec2.cn-north-1.amazonaws.com.cn" {
		t.Fatalf("got %s", got)
	}
	if got := env.EC2Endpoint("us-east-1"); got != "https://ec2.us-east-1.amazonaws.com" {
		t.Fatalf("got %s", got)
	}
}

func TestCredentialsStringOmitsSecretAndSessionToken(t *testing.T) {
	text := Credentials{AccessKeyID: "AKIDEXAMPLE", SecretAccessKey: exampleSecret, SessionToken: "token"}.String()
	if strings.Contains(text, exampleSecret) || strings.Contains(text, "token") {
		t.Fatalf("leaked secret: %s", text)
	}
	if !strings.Contains(text, "AKIDEXAMPLE") {
		t.Fatalf("missing access key: %s", text)
	}
}

func mustNormalize(cfg Config) (Config, error) {
	normalized, _, _, _, err := normalize(cfg)
	return normalized, err
}
