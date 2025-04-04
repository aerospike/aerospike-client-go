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
	pc "github.com/aerospike/aerospike-client-go/v8/internal/cache"
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
		dc    *DynConfig
		cache *pc.PolicyCache
		dsn   = "dummy"
	)

	gg.BeforeEach(func() {
		cache = pc.NewPolicyCache()
	})

	gg.Context("initConfig()", func() {
		gg.Context("when loadedConfig is nil", func() {
			gg.BeforeEach(func() {
				fakeProvider := &fakeConfigProvider{config: nil}

				prevDyn := &dynconfig.DynamicConfig{
					Read: &dynconfig.Read{
						TotalTimeout: func() *dynconfig.Duration {
							d := dynconfig.Duration(2 * time.Second)
							return &d
						}(),
					},
				}

				dc = &DynConfig{
					configProvider:    fakeProvider,
					configInitialized: &atomic.Bool{},
					mappedPolicies:    cache,
					config: &dynconfig.Config{
						Dynamic: prevDyn,
					},
				}
				dc.initConfig()
			})

			gg.It("should NOT update dc.config.Dynamic", func() {
				gm.Expect(dc.mappedPolicies.Static).ToNot(gm.BeNil())
				defaultStatic, ok := dc.mappedPolicies.Static[pc.CLIENT_POLICY]
				gm.Expect(ok).To(gm.BeTrue())
				gm.Expect(defaultStatic).ToNot(gm.BeNil())

				gm.Expect(dc.mappedPolicies.Dynamic).ToNot(gm.BeNil())
				defaultDynamic, ok := dc.mappedPolicies.Dynamic[pc.READ_POLICY].(*BasePolicy)
				gm.Expect(ok).To(gm.BeTrue())
				gm.Expect(defaultDynamic).ToNot(gm.BeNil())

				// Making sure config has not been updated if LoadConfig() invoked from configProvider is nil.
				// In those case we should load/update default policies into the cache.
				gm.Expect(defaultDynamic.TotalTimeout).To(gm.Equal(time.Duration(2 * time.Second)))
				gm.Expect(time.Duration(*dc.config.Dynamic.Read.TotalTimeout)).To(gm.Equal(time.Duration(2 * time.Second)))
				gm.Expect(defaultDynamic.TotalTimeout).ToNot(gm.Equal(dc.config.Dynamic.Read.TotalTimeout))
			})
		})

		gg.Context("when loadedConfig is not nil", func() {
			var newTimeout time.Duration
			gg.BeforeEach(func() {
				newTimeout = 5 * time.Second
				// Create a dummy loaded dynamic configuration.
				dummyDyn := &dynconfig.DynamicConfig{
					Read: &dynconfig.Read{
						TotalTimeout: func() *dynconfig.Duration {
							d := dynconfig.Duration(newTimeout)
							return &d
						}(),
					},
				}
				dummyCfg := &dynconfig.Config{
					Dynamic: dummyDyn,
				}
				fakeProvider := &fakeConfigProvider{config: dummyCfg}

				// Prepopulate dynamic cache with an older value.
				cache.Dynamic = make(map[pc.PolicyT]any)
				cache.Dynamic[pc.READ_POLICY] = &BasePolicy{TotalTimeout: 1 * time.Second}

				// Initialize dc.config with an old dynamic configuration.
				oldDyn := &dynconfig.DynamicConfig{
					Read: &dynconfig.Read{
						TotalTimeout: func() *dynconfig.Duration {
							d := dynconfig.Duration(1 * time.Second)
							return &d
						}(),
					},
				}

				dc = &DynConfig{
					configProvider:    fakeProvider,
					configInitialized: &atomic.Bool{},
					mappedPolicies:    cache,
					dsn:               dsn,
					config: &dynconfig.Config{
						Dynamic: oldDyn,
					},
				}

				// Call initConfig to update dc.config and rehydrate dynamic cache.
				dc.initConfig()
			})

			gg.It("should clear the cache and update dc.config.Dynamic based on loaded config", func() {
				// dc.config.Dynamic should be updated.
				gm.Expect(dc.config.Dynamic).ToNot(gm.BeNil())
				readCfg := dc.config.Dynamic.Read
				gm.Expect(readCfg).ToNot(gm.BeNil())
				// Ensuring the new value is set in the config as well.
				gm.Expect(*readCfg.TotalTimeout).To(gm.Equal(dynconfig.Duration(newTimeout)))

				policy, ok := dc.mappedPolicies.Dynamic[pc.READ_POLICY].(*BasePolicy)
				gm.Expect(ok).To(gm.BeTrue())
				// At this point the dynamic cache should be updated with the new value.
				gm.Expect(policy.TotalTimeout).To(gm.Equal(newTimeout))
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
						TotalTimeout: func() *dynconfig.Duration {
							d := dynconfig.Duration(2 * time.Second)
							return &d
						}(),
					},
				}

				dc = &DynConfig{
					configProvider:    fakeProvider,
					configInitialized: &atomic.Bool{},
					mappedPolicies:    cache,
					dsn:               dsn,
					config: &dynconfig.Config{
						Dynamic: prevDyn,
					},
				}

				// Prepopulate dynamic cache with an old BasePolicy.
				cache.Dynamic = make(map[pc.PolicyT]any)
				cache.Dynamic[pc.READ_POLICY] = &BasePolicy{TotalTimeout: 1 * time.Second}

				// Call providerLoadConfig which should do nothing as loadedConfig is nil.
				dc.providerLoadConfig()
			})

			gg.It("should NOT update dc.config.Dynamic nor the dynamic cache", func() {
				gm.Expect(dc.config.Dynamic).ToNot(gm.BeNil())
				expected := dynconfig.Duration(2 * time.Second)
				gm.Expect(*dc.config.Dynamic.Read.TotalTimeout).To(gm.Equal(expected))

				policy, ok := dc.mappedPolicies.Dynamic[pc.READ_POLICY].(*BasePolicy)
				gm.Expect(ok).To(gm.BeTrue())
				// The cached policy remains with its old value.
				gm.Expect(policy.TotalTimeout).To(gm.Equal(1 * time.Second))
			})
		})

		gg.Context("when loadedConfig is not nil", func() {
			var newTimeout time.Duration
			gg.BeforeEach(func() {
				newTimeout = 5 * time.Second
				// New loaded dynamic configuration.
				dummyDyn := &dynconfig.DynamicConfig{
					Read: &dynconfig.Read{
						TotalTimeout: func() *dynconfig.Duration {
							d := dynconfig.Duration(newTimeout)
							return &d
						}(),
					},
				}
				dummyCfg := &dynconfig.Config{
					Dynamic: dummyDyn,
				}
				fakeProvider := &fakeConfigProvider{config: dummyCfg}

				// Pre-populate dynamic cache with an older value.
				cache.Dynamic = make(map[pc.PolicyT]any)
				cache.Dynamic[pc.READ_POLICY] = &BasePolicy{TotalTimeout: 1 * time.Second}

				dc = &DynConfig{
					configProvider:    fakeProvider,
					configInitialized: &atomic.Bool{},
					mappedPolicies:    cache,
					dsn:               dsn,
					// Start with an old dynamic configuration.
					config: &dynconfig.Config{
						Dynamic: &dynconfig.DynamicConfig{
							Read: &dynconfig.Read{
								TotalTimeout: func() *dynconfig.Duration {
									d := dynconfig.Duration(1 * time.Second)
									return &d
								}(),
							},
						},
					},
				}

				// Call providerLoadConfig to trigger cache pruning and update.
				dc.providerLoadConfig()
			})

			gg.It("should update dc.config.Dynamic and rehydrate the dynamic cache", func() {
				gm.Expect(dc.config.Dynamic).ToNot(gm.BeNil())
				readCfg := dc.config.Dynamic.Read
				gm.Expect(readCfg).ToNot(gm.BeNil())
				gm.Expect(*readCfg.TotalTimeout).To(gm.Equal(dynconfig.Duration(newTimeout)))

				policy, ok := dc.mappedPolicies.Dynamic[pc.READ_POLICY].(*BasePolicy)
				gm.Expect(ok).To(gm.BeTrue())
				gm.Expect(policy.TotalTimeout).To(gm.Equal(newTimeout))
			})
		})
	})
})
