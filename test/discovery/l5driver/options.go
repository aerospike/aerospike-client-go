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

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
)

const usage = "usage: l5driver --seed HOST:PORT --shape pass-through|port-map " +
	"[--port-map NODE=HOST:PORT,...] [--on-miss strict|pass-through] [--user USER] --tend-interval MS " +
	"--timeout MS --namespace NS --set SET [--workers N] [--keys N] [--tls-name NAME [--tls-ca-file PATH]]"

const (
	shapePassThrough = "pass-through"
	shapePortMap     = "port-map"
)

// driverOptions are the flags of one driver run.
type driverOptions struct {
	seedHost     string
	seedPort     int
	shape        string
	portMap      map[string]string
	onMiss       as.TranslationMissMode
	user         string
	password     string
	tendInterval time.Duration
	timeout      time.Duration
	namespace    string
	set          string
	workers      int
	keys         int
	tlsName      string
	tlsCAFile    string
	translator   as.AddressTranslator
	tlsConfig    *tls.Config
}

// parseDriverOptions reads the flags and builds the translator and the TLS
// settings they name, so a bad map or CA file is a bad flag. password is
// AEROSPIKE_ADMIN_PASSWORD, required only with --user.
func parseDriverOptions(args []string, password string, passwordSet bool) (*driverOptions, error) {
	options := &driverOptions{onMiss: as.TranslationMissStrict, workers: 4, keys: 1000}
	seen := map[string]bool{}

	for index := 0; index < len(args); index += 2 {
		flag := args[index]
		if !strings.HasPrefix(flag, "--") {
			return nil, fmt.Errorf("unexpected argument: %s", flag)
		}
		if index+1 >= len(args) {
			return nil, fmt.Errorf("%s needs a value", flag)
		}
		if seen[flag] {
			return nil, fmt.Errorf("%s given twice", flag)
		}
		seen[flag] = true
		value := args[index+1]

		var err error
		switch flag {
		case "--seed":
			options.seedHost, options.seedPort, err = parseHostPort(value)
			if err != nil {
				err = fmt.Errorf("--seed must be HOST:PORT: %s", value)
			}
		case "--shape":
			if value != shapePassThrough && value != shapePortMap {
				err = fmt.Errorf("--shape must be pass-through or port-map: %s", value)
			}
			options.shape = value
		case "--port-map":
			options.portMap, err = parsePortMapFlag(value)
		case "--on-miss":
			switch value {
			case "strict":
				options.onMiss = as.TranslationMissStrict
			case "pass-through":
				options.onMiss = as.TranslationMissPassThrough
			default:
				err = fmt.Errorf("--on-miss must be strict or pass-through: %s", value)
			}
		case "--user":
			if !passwordSet {
				err = errors.New("--user needs AEROSPIKE_ADMIN_PASSWORD in the environment")
			}
			options.user = value
			options.password = password
		case "--tend-interval":
			options.tendInterval, err = positiveMillis(flag, value)
		case "--timeout":
			options.timeout, err = positiveMillis(flag, value)
		case "--namespace":
			options.namespace = value
		case "--set":
			options.set = value
		case "--workers":
			options.workers, err = positiveCount(flag, value)
		case "--keys":
			options.keys, err = positiveCount(flag, value)
		case "--tls-name":
			options.tlsName = value
		case "--tls-ca-file":
			options.tlsCAFile = value
		default:
			err = fmt.Errorf("unknown flag: %s", flag)
		}
		if err != nil {
			return nil, err
		}
	}

	for _, required := range []string{"--seed", "--shape", "--tend-interval", "--timeout", "--namespace", "--set"} {
		if !seen[required] {
			return nil, fmt.Errorf("%s is required", required)
		}
	}
	if options.shape == shapePortMap && !seen["--port-map"] {
		return nil, errors.New("--port-map is required with --shape port-map")
	}
	if options.shape != shapePortMap && seen["--port-map"] {
		return nil, errors.New("--port-map only with --shape port-map")
	}
	if seen["--tls-ca-file"] && !seen["--tls-name"] {
		return nil, errors.New("--tls-ca-file only with --tls-name")
	}

	if options.shape == shapePassThrough {
		options.translator = as.NewPassThroughAddressTranslator()
	} else {
		translator, err := as.NewPortMapAddressTranslator(options.portMap)
		if err != nil {
			return nil, fmt.Errorf("--port-map: %s", err.Error())
		}
		options.translator = translator
	}
	if seen["--tls-name"] {
		tlsConfig, err := tlsConfigTrusting(options.tlsCAFile)
		if err != nil {
			return nil, err
		}
		options.tlsConfig = tlsConfig
	}
	return options, nil
}

// tlsConfigTrusting trusts the CAs in the PEM file, or the system roots when
// there is no file.
func tlsConfigTrusting(caFile string) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile == "" {
		return tlsConfig, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("--tls-ca-file: %s", err.Error())
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("--tls-ca-file has no certificate: %s", caFile)
	}
	tlsConfig.RootCAs = roots
	return tlsConfig, nil
}

// parsePortMapFlag reads NODE=HOST:PORT and HOST:PORT=HOST:PORT entries.
func parsePortMapFlag(value string) (map[string]string, error) {
	portMap := map[string]string{}
	for _, entry := range strings.Split(value, ",") {
		name, target, _ := strings.Cut(entry, "=")
		if _, _, err := parseHostPort(target); name == "" || err != nil {
			return nil, fmt.Errorf("--port-map entry must be NODE=HOST:PORT or HOST:PORT=HOST:PORT: %s", entry)
		}
		if _, duplicate := portMap[name]; duplicate {
			return nil, fmt.Errorf("--port-map names %s twice", name)
		}
		portMap[name] = target
	}
	return portMap, nil
}

func parseHostPort(address string) (string, int, error) {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return "", 0, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || host == "" || port < 1 || port > 65535 {
		return "", 0, fmt.Errorf("invalid host:port %s", address)
	}
	return host, port, nil
}

func positiveMillis(flag, value string) (time.Duration, error) {
	count, err := positiveCount(flag, value)
	return time.Duration(count) * time.Millisecond, err
}

func positiveCount(flag, value string) (int, error) {
	count, err := strconv.Atoi(value)
	if err != nil || count <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer: %s", flag, value)
	}
	return count, nil
}

// clientPolicy maps the flags to the client's own settings of the same
// meaning.
func (options *driverOptions) clientPolicy() *as.ClientPolicy {
	policy := as.NewClientPolicy()
	policy.TendInterval = options.tendInterval
	policy.Timeout = options.timeout
	policy.User = options.user
	policy.Password = options.password
	policy.TranslationOnMiss = options.onMiss
	policy.UseServicesAlternate = options.shape == shapePassThrough
	policy.AddressTranslator = options.translator
	policy.TlsConfig = options.tlsConfig
	return policy
}

func (options *driverOptions) seed() *as.Host {
	seed := as.NewHost(options.seedHost, options.seedPort)
	seed.TLSName = options.tlsName
	return seed
}

// readPolicy and writePolicy make one attempt per command; the driver retries.
func (options *driverOptions) readPolicy() *as.BasePolicy {
	policy := as.NewPolicy()
	oneAttempt(policy, options.timeout)
	return policy
}

func (options *driverOptions) writePolicy() *as.WritePolicy {
	policy := as.NewWritePolicy(0, 0)
	oneAttempt(&policy.BasePolicy, options.timeout)
	return policy
}

func oneAttempt(policy *as.BasePolicy, timeout time.Duration) {
	policy.SocketTimeout = timeout
	policy.TotalTimeout = timeout
	policy.MaxRetries = 0
}
