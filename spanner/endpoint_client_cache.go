/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package spanner

import (
	"context"
	"sync"
	"sync/atomic"
)

// grpcChannelEndpoint is a channelEndpoint backed by a real gRPC connection.
type grpcChannelEndpoint struct {
	address string
	client  spannerClient
	healthy atomic.Bool
}

func (e *grpcChannelEndpoint) Address() string {
	return e.address
}

func (e *grpcChannelEndpoint) IsHealthy() bool {
	return e.healthy.Load()
}

// endpointClientCache implements channelEndpointCache with actual gRPC
// connections to specific server addresses.
type endpointClientCache struct {
	mu            sync.RWMutex
	endpoints     map[string]*grpcChannelEndpoint
	clientFactory func(ctx context.Context, address string) (spannerClient, error)
}

func newEndpointClientCache(clientFactory func(ctx context.Context, address string) (spannerClient, error)) *endpointClientCache {
	return &endpointClientCache{
		endpoints:     make(map[string]*grpcChannelEndpoint),
		clientFactory: clientFactory,
	}
}

// Get returns a channelEndpoint for the given address, creating a new gRPC
// connection if one does not already exist (double-checked locking).
func (c *endpointClientCache) Get(address string) channelEndpoint {
	// Fast path: read lock.
	c.mu.RLock()
	if ep, ok := c.endpoints[address]; ok {
		c.mu.RUnlock()
		return ep
	}
	c.mu.RUnlock()

	// Slow path: write lock + create.
	c.mu.Lock()
	defer c.mu.Unlock()
	if ep, ok := c.endpoints[address]; ok {
		return ep
	}
	client, err := c.clientFactory(context.Background(), address)
	if err != nil {
		return nil
	}
	ep := &grpcChannelEndpoint{
		address: address,
		client:  client,
	}
	ep.healthy.Store(true)
	c.endpoints[address] = ep
	return ep
}

// ClientFor resolves a channelEndpoint to the underlying spannerClient.
func (c *endpointClientCache) ClientFor(ep channelEndpoint) spannerClient {
	if ep == nil {
		return nil
	}
	gep, ok := ep.(*grpcChannelEndpoint)
	if !ok {
		return nil
	}
	return gep.client
}

// Close shuts down all cached gRPC connections.
func (c *endpointClientCache) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	var firstErr error
	for addr, ep := range c.endpoints {
		if ep.client != nil {
			if err := ep.client.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(c.endpoints, addr)
	}
	return firstErr
}
