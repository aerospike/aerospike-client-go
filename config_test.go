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
	"sync/atomic"
	"time"

	dynconfig "github.com/aerospike/aerospike-client-go/v8/config"
	gg "github.com/onsi/ginkgo/v2"
	gm "github.com/onsi/gomega"
)

// fakeConfigProvider implements the dynconfig.ConfigProvider interface.
type fakeConfigProvider struct {
	config *dynconfig.Config
}

func (f *fakeConfigProvider) LoadConfig(dsn string) *dynconfig.Config {
	return f.config
}

var _ = gg.Describe("DynConfig - initConfig and providerLoadConfig", func() {
	var (
		dc  *DynConfig
		dsn = "dummy"
	)

	gg.Context("initConfig()", func() {
		gg.Context("when loadedConfig is nil", func() {
			gg.BeforeEach(func() {
				fakeProvider := &fakeConfigProvider{config: nil}

				dc = &DynConfig{
					configProvider:    fakeProvider,
					configInitialized: &atomic.Bool{},
					config:            &dynconfig.Config{},
				}
				dc.client = &Client{
					dynConfig: dc,
				}
				dc.initConfig()
				dc.updateCachedPolicies()
			})

			gg.It("should update dc.config.Dynamic with Defaults", func() {
				defaultDynamic := dc.client.dynDefaultPolicy.Load()
				gm.Expect(dc.client.dynDefaultClientPolicy.Load()).ToNot(gm.BeNil())
				gm.Expect(defaultDynamic).ToNot(gm.BeNil())

				// Making sure config has not been updated if LoadConfig() invoked from configProvider is nil.
				// In those case we should load/update default policies into the cache.
				gm.Expect(defaultDynamic.TotalTimeout).To(gm.Equal(time.Duration(1000 * time.Millisecond)))
			})
		})

		gg.Context("when loadedConfig is not nil", func() {
			var newTimeout int
			gg.BeforeEach(func() {
				newTimeout = 5000
				// Create a dummy loaded dynamic configuration.
				dummyDyn := &dynconfig.DynamicConfig{
					Read: &dynconfig.Read{
						TotalTimeout: func() *int {
							d := newTimeout
							return &d
						}(),
					},
				}
				dummyCfg := &dynconfig.Config{
					Version: func() *string {
						d := "1.0.0"
						return &d
					}(),
					Dynamic: dummyDyn,
				}
				fakeProvider := &fakeConfigProvider{config: dummyCfg}

				// Initialize dc.config with an old dynamic configuration.
				oldDyn := &dynconfig.DynamicConfig{
					Read: &dynconfig.Read{
						TotalTimeout: func() *int {
							d := 1
							return &d
						}(),
					},
				}

				dc = &DynConfig{
					configProvider:    fakeProvider,
					configInitialized: &atomic.Bool{},
					dsn:               dsn,
					config: &dynconfig.Config{
						Dynamic: oldDyn,
					},
				}

				dc.client = &Client{
					dynConfig: dc,
				}
				dc.client.dynDefaultPolicy.Store(&BasePolicy{TotalTimeout: 1 * time.Second})

				// Call initConfig to update dc.config and rehydrate dynamic cache.
				dc.initConfig()
				dc.updateCachedPolicies()
			})

			gg.It("should clear the cache and update dc.config.Dynamic based on loaded config", func() {
				// dc.config.Dynamic should be updated.
				gm.Expect(dc.config.Dynamic).ToNot(gm.BeNil())
				readCfg := dc.config.Dynamic.Read
				gm.Expect(readCfg).ToNot(gm.BeNil())
				// Ensuring the new value is set in the config as well.
				gm.Expect(*readCfg.TotalTimeout).To(gm.Equal(int(newTimeout)))

				policy := dc.client.dynDefaultPolicy.Load()
				// At this point the dynamic cache should be updated with the new value.
				gm.Expect(policy.TotalTimeout).To(gm.Equal(time.Duration(newTimeout) * time.Millisecond))
			})
		})
	})

	gg.Context("providerLoadConfig()", func() {
		gg.Context("when loadedConfig is nil", func() {
			gg.BeforeEach(func() {
				// Fake provider that returns nil.
				fakeProvider := &fakeConfigProvider{config: nil}

				// Prepopulate previous dynamic configuration.
				prevDyn := &dynconfig.DynamicConfig{
					Read: &dynconfig.Read{
						TotalTimeout: func() *int {
							d := int(2 * time.Second)
							return &d
						}(),
					},
				}

				dc = &DynConfig{
					configProvider:    fakeProvider,
					configInitialized: &atomic.Bool{},
					dsn:               dsn,
					config: &dynconfig.Config{
						Dynamic: prevDyn,
					},
				}
				dc.client = &Client{
					dynConfig: dc,
				}
				dc.client.dynDefaultPolicy.Store(&BasePolicy{TotalTimeout: 1 * time.Second})

				// Call providerLoadConfig which should do nothing as loadedConfig is nil.
				dc.providerLoadConfig()
			})

			gg.It("should NOT update dc.config.Dynamic nor the dynamic cache", func() {
				gm.Expect(dc.config.Dynamic).ToNot(gm.BeNil())
				expected := int(2 * time.Second)
				gm.Expect(*dc.config.Dynamic.Read.TotalTimeout).To(gm.Equal(expected))

				policy := dc.client.dynDefaultPolicy.Load()
				// The cached policy remains with its old value.
				gm.Expect(policy.TotalTimeout).To(gm.Equal(1 * time.Second))
			})
		})

		gg.Context("when loadedConfig is not nil", func() {
			var newTimeout time.Duration
			gg.BeforeEach(func() {
				newTimeout = 5000 * time.Millisecond
				// New loaded dynamic configuration.
				dummyDyn := &dynconfig.DynamicConfig{
					Read: &dynconfig.Read{
						TotalTimeout: func() *int {
							d := int(newTimeout)
							return &d
						}(),
					},
				}
				dummyCfg := &dynconfig.Config{
					Version: func() *string {
						d := "1.0.0"
						return &d
					}(),
					Dynamic: dummyDyn,
				}
				fakeProvider := &fakeConfigProvider{config: dummyCfg}

				dc = &DynConfig{
					configProvider:    fakeProvider,
					configInitialized: &atomic.Bool{},
					dsn:               dsn,
					// Start with an old dynamic configuration.
					config: &dynconfig.Config{
						Dynamic: &dynconfig.DynamicConfig{
							Read: &dynconfig.Read{
								TotalTimeout: func() *int {
									d := int(1 * time.Second)
									return &d
								}(),
							},
						},
					},
				}

				dc.client = &Client{
					dynConfig: dc,
				}
				dc.client.dynDefaultPolicy.Store(&BasePolicy{TotalTimeout: 1 * time.Second})

				dc.providerLoadConfig()
			})

			gg.It("should update dc.config.Dynamic and rehydrate the dynamic cache", func() {
				gm.Expect(dc.config.Dynamic).ToNot(gm.BeNil())
				readCfg := dc.config.Dynamic.Read
				gm.Expect(readCfg).ToNot(gm.BeNil())
				gm.Expect(*readCfg.TotalTimeout).To(gm.Equal(int(time.Duration(newTimeout))))

				policy := dc.client.dynDefaultPolicy.Load()
				gm.Expect(policy.TotalTimeout).To(gm.Equal(time.Duration(newTimeout) * time.Millisecond))
			})
		})
	})
})
