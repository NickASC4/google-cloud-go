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
	"fmt"
	"sync"
	"time"

	vkit "cloud.google.com/go/spanner/apiv1"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/googleapis/gax-go/v2"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
)

// locationAwareSpannerClient is a spannerClient wrapper that routes RPCs to
// specific server endpoints based on location-aware routing hints.
//
// Routed RPCs (StreamingRead, Read, ExecuteStreamingSql, ExecuteSql,
// BeginTransaction) first ask the locationRouter for a routing hint and
// endpoint, then dispatch to the endpoint's spannerClient if available.
//
// Affinity RPCs (Commit, Rollback) look up the transaction affinity set by
// prior RPCs and route to the same server.
//
// All other RPCs are passed through to the default client.
type locationAwareSpannerClient struct {
	defaultClient           spannerClient
	router                  *locationRouter
	endpointCache           channelEndpointCache
	defaultAffinityEndpoint channelEndpoint
}

var _ spannerClient = (*locationAwareSpannerClient)(nil)

// asGRPCSpannerClient extracts the underlying *grpcSpannerClient from a
// spannerClient, handling the locationAwareSpannerClient wrapper.
func asGRPCSpannerClient(c spannerClient) *grpcSpannerClient {
	if gsc, ok := c.(*grpcSpannerClient); ok {
		return gsc
	}
	if lac, ok := c.(*locationAwareSpannerClient); ok {
		return asGRPCSpannerClient(lac.defaultClient)
	}
	return nil
}

func newLocationAwareSpannerClient(defaultClient spannerClient, router *locationRouter, endpointCache channelEndpointCache) *locationAwareSpannerClient {
	return &locationAwareSpannerClient{
		defaultClient:           defaultClient,
		router:                  router,
		endpointCache:           endpointCache,
		defaultAffinityEndpoint: &passthroughChannelEndpoint{address: ""},
	}
}

func (c *locationAwareSpannerClient) affinityTrackingEndpoint(ep channelEndpoint) channelEndpoint {
	if ep != nil {
		return ep
	}
	return c.defaultAffinityEndpoint
}

func endpointAddressOrDefault(ep channelEndpoint) string {
	if ep == nil {
		return "default"
	}
	return ep.Address()
}

func (c *locationAwareSpannerClient) traceRoutingDecision(ctx context.Context, start time.Time, source string, ep channelEndpoint, method string) {
	larTraceEvent(ctx, "lar.routing_decision",
		attribute.Int64("duration_us", time.Since(start).Microseconds()),
		attribute.String("routing_source", source),
		attribute.String("target_address", endpointAddressOrDefault(ep)),
		attribute.String("method", method),
	)
}

// clientForEndpoint resolves a channelEndpoint to a spannerClient, falling
// back to the default client if the endpoint is nil, unhealthy, or has no
// associated client.
func (c *locationAwareSpannerClient) clientForEndpoint(ep channelEndpoint) spannerClient {
	if ep == nil || !ep.IsHealthy() {
		return c.defaultClient
	}
	client := c.endpointCache.ClientFor(ep)
	if client == nil {
		return c.defaultClient
	}
	return client
}

// affinityClient returns the spannerClient for a given transaction ID based on
// affinity, falling back to the default client.
func (c *locationAwareSpannerClient) affinityClient(txID []byte) spannerClient {
	if len(txID) == 0 {
		return c.defaultClient
	}
	ep := c.router.getTransactionAffinity(string(txID))
	return c.clientForEndpoint(ep)
}

// --- Pass-through methods ---

func (c *locationAwareSpannerClient) CallOptions() *vkit.CallOptions {
	return c.defaultClient.CallOptions()
}

func (c *locationAwareSpannerClient) Close() error {
	return c.defaultClient.Close()
}

func (c *locationAwareSpannerClient) Connection() *grpc.ClientConn {
	return c.defaultClient.Connection()
}

func (c *locationAwareSpannerClient) CreateSession(ctx context.Context, req *spannerpb.CreateSessionRequest, opts ...gax.CallOption) (*spannerpb.Session, error) {
	return c.defaultClient.CreateSession(ctx, req, opts...)
}

func (c *locationAwareSpannerClient) BatchCreateSessions(ctx context.Context, req *spannerpb.BatchCreateSessionsRequest, opts ...gax.CallOption) (*spannerpb.BatchCreateSessionsResponse, error) {
	return c.defaultClient.BatchCreateSessions(ctx, req, opts...)
}

func (c *locationAwareSpannerClient) GetSession(ctx context.Context, req *spannerpb.GetSessionRequest, opts ...gax.CallOption) (*spannerpb.Session, error) {
	return c.defaultClient.GetSession(ctx, req, opts...)
}

func (c *locationAwareSpannerClient) ListSessions(ctx context.Context, req *spannerpb.ListSessionsRequest, opts ...gax.CallOption) *vkit.SessionIterator {
	return c.defaultClient.ListSessions(ctx, req, opts...)
}

func (c *locationAwareSpannerClient) DeleteSession(ctx context.Context, req *spannerpb.DeleteSessionRequest, opts ...gax.CallOption) error {
	return c.defaultClient.DeleteSession(ctx, req, opts...)
}

func (c *locationAwareSpannerClient) ExecuteBatchDml(ctx context.Context, req *spannerpb.ExecuteBatchDmlRequest, opts ...gax.CallOption) (*spannerpb.ExecuteBatchDmlResponse, error) {
	return c.defaultClient.ExecuteBatchDml(ctx, req, opts...)
}

func (c *locationAwareSpannerClient) PartitionQuery(ctx context.Context, req *spannerpb.PartitionQueryRequest, opts ...gax.CallOption) (*spannerpb.PartitionResponse, error) {
	return c.defaultClient.PartitionQuery(ctx, req, opts...)
}

func (c *locationAwareSpannerClient) PartitionRead(ctx context.Context, req *spannerpb.PartitionReadRequest, opts ...gax.CallOption) (*spannerpb.PartitionResponse, error) {
	return c.defaultClient.PartitionRead(ctx, req, opts...)
}

func (c *locationAwareSpannerClient) BatchWrite(ctx context.Context, req *spannerpb.BatchWriteRequest, opts ...gax.CallOption) (spannerpb.Spanner_BatchWriteClient, error) {
	return c.defaultClient.BatchWrite(ctx, req, opts...)
}

// --- Routed RPCs ---

func (c *locationAwareSpannerClient) StreamingRead(ctx context.Context, req *spannerpb.ReadRequest, opts ...gax.CallOption) (spannerpb.Spanner_StreamingReadClient, error) {
	routeStart := time.Now()
	ep, source := c.router.prepareReadRequest(ctx, req)
	c.traceRoutingDecision(ctx, routeStart, source, ep, "/google.spanner.v1.Spanner/StreamingRead")
	client := c.clientForEndpoint(ep)
	stream, err := client.StreamingRead(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	isReadOnlyBegin, readOnlyStrong := readOnlyBeginFromSelector(req.GetTransaction())
	return newAffinityTrackingStream(
		stream,
		c.router,
		c.affinityTrackingEndpoint(ep),
		isReadOnlyBegin,
		readOnlyStrong,
		isReadWriteBeginFromSelector(req.GetTransaction()),
		ctx,
	), nil
}

func (c *locationAwareSpannerClient) Read(ctx context.Context, req *spannerpb.ReadRequest, opts ...gax.CallOption) (*spannerpb.ResultSet, error) {
	routeStart := time.Now()
	ep, source := c.router.prepareReadRequest(ctx, req)
	c.traceRoutingDecision(ctx, routeStart, source, ep, "/google.spanner.v1.Spanner/Read")
	client := c.clientForEndpoint(ep)
	resp, err := client.Read(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	c.router.observeResultSet(ctx, resp)
	if txMeta := resp.GetMetadata().GetTransaction(); txMeta != nil && len(txMeta.GetId()) > 0 {
		if isReadOnlyBegin, readOnlyStrong := readOnlyBeginFromSelector(req.GetTransaction()); isReadOnlyBegin {
			c.router.trackReadOnlyTransaction(string(txMeta.GetId()), readOnlyStrong)
			larTraceEvent(ctx, "lar.affinity_track_readonly",
				attribute.Bool("prefer_leader", readOnlyStrong),
			)
		} else if isReadWriteBeginFromSelector(req.GetTransaction()) {
			c.router.setTransactionAffinity(string(txMeta.GetId()), c.affinityTrackingEndpoint(ep))
			larTraceEvent(ctx, "lar.affinity_recorded",
				attribute.String("target_address", endpointAddressOrDefault(c.affinityTrackingEndpoint(ep))),
			)
		}
	}
	return resp, nil
}

func (c *locationAwareSpannerClient) ExecuteStreamingSql(ctx context.Context, req *spannerpb.ExecuteSqlRequest, opts ...gax.CallOption) (spannerpb.Spanner_ExecuteStreamingSqlClient, error) {
	routeStart := time.Now()
	ep, source := c.router.prepareExecuteSQLRequest(ctx, req)
	c.traceRoutingDecision(ctx, routeStart, source, ep, "/google.spanner.v1.Spanner/ExecuteStreamingSql")
	requestAttrs := []attribute.KeyValue{
		attribute.String("query_mode", req.GetQueryMode().String()),
	}
	requestAttrs = append(requestAttrs, larJSONPayloadAttrs(larProtoJSON(req))...)
	larTraceEvent(ctx, "lar.execute_streaming_sql.request_json", requestAttrs...)
	client := c.clientForEndpoint(ep)
	stream, err := client.ExecuteStreamingSql(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	isReadOnlyBegin, readOnlyStrong := readOnlyBeginFromSelector(req.GetTransaction())
	return newAffinityTrackingStream(
		stream,
		c.router,
		c.affinityTrackingEndpoint(ep),
		isReadOnlyBegin,
		readOnlyStrong,
		isReadWriteBeginFromSelector(req.GetTransaction()),
		ctx,
	), nil
}

func (c *locationAwareSpannerClient) ExecuteSql(ctx context.Context, req *spannerpb.ExecuteSqlRequest, opts ...gax.CallOption) (*spannerpb.ResultSet, error) {
	routeStart := time.Now()
	ep, source := c.router.prepareExecuteSQLRequest(ctx, req)
	c.traceRoutingDecision(ctx, routeStart, source, ep, "/google.spanner.v1.Spanner/ExecuteSql")
	client := c.clientForEndpoint(ep)
	resp, err := client.ExecuteSql(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	c.router.observeResultSet(ctx, resp)
	if txMeta := resp.GetMetadata().GetTransaction(); txMeta != nil && len(txMeta.GetId()) > 0 {
		if isReadOnlyBegin, readOnlyStrong := readOnlyBeginFromSelector(req.GetTransaction()); isReadOnlyBegin {
			c.router.trackReadOnlyTransaction(string(txMeta.GetId()), readOnlyStrong)
			larTraceEvent(ctx, "lar.affinity_track_readonly",
				attribute.Bool("prefer_leader", readOnlyStrong),
			)
		} else if isReadWriteBeginFromSelector(req.GetTransaction()) {
			c.router.setTransactionAffinity(string(txMeta.GetId()), c.affinityTrackingEndpoint(ep))
			larTraceEvent(ctx, "lar.affinity_recorded",
				attribute.String("target_address", endpointAddressOrDefault(c.affinityTrackingEndpoint(ep))),
			)
		}
	}
	return resp, nil
}

func (c *locationAwareSpannerClient) BeginTransaction(ctx context.Context, req *spannerpb.BeginTransactionRequest, opts ...gax.CallOption) (*spannerpb.Transaction, error) {
	routeStart := time.Now()
	ep, source := c.router.prepareBeginTransactionRequest(ctx, req)
	c.traceRoutingDecision(ctx, routeStart, source, ep, "/google.spanner.v1.Spanner/BeginTransaction")
	client := c.clientForEndpoint(ep)
	resp, err := client.BeginTransaction(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	if len(resp.GetId()) > 0 {
		if isReadOnly, readOnlyStrong := readOnlyBeginFromTransactionOptions(req.GetOptions()); isReadOnly {
			c.router.trackReadOnlyTransaction(string(resp.GetId()), readOnlyStrong)
			larTraceEvent(ctx, "lar.affinity_track_readonly",
				attribute.Bool("prefer_leader", readOnlyStrong),
			)
		} else {
			c.router.setTransactionAffinity(string(resp.GetId()), c.affinityTrackingEndpoint(ep))
			larTraceEvent(ctx, "lar.affinity_recorded",
				attribute.String("target_address", endpointAddressOrDefault(c.affinityTrackingEndpoint(ep))),
			)
		}
	}
	return resp, nil
}

// --- Affinity RPCs ---

func (c *locationAwareSpannerClient) Commit(ctx context.Context, req *spannerpb.CommitRequest, opts ...gax.CallOption) (*spannerpb.CommitResponse, error) {
	routeStart := time.Now()
	source := "default"
	var txID []byte
	if req != nil {
		txID = req.GetTransactionId()
	}
	if len(txID) > 0 && c.router.getTransactionAffinity(string(txID)) != nil {
		source = "affinity"
	}
	ep := c.router.getTransactionAffinity(string(txID))
	c.traceRoutingDecision(ctx, routeStart, source, ep, "/google.spanner.v1.Spanner/Commit")
	client := c.affinityClient(txID)
	resp, err := client.Commit(ctx, req, opts...)
	c.router.clearTransactionAffinity(string(txID))
	return resp, err
}

func (c *locationAwareSpannerClient) Rollback(ctx context.Context, req *spannerpb.RollbackRequest, opts ...gax.CallOption) error {
	routeStart := time.Now()
	source := "default"
	var txID []byte
	if req != nil {
		txID = req.GetTransactionId()
	}
	if len(txID) > 0 && c.router.getTransactionAffinity(string(txID)) != nil {
		source = "affinity"
	}
	ep := c.router.getTransactionAffinity(string(txID))
	c.traceRoutingDecision(ctx, routeStart, source, ep, "/google.spanner.v1.Spanner/Rollback")
	client := c.affinityClient(txID)
	err := client.Rollback(ctx, req, opts...)
	c.router.clearTransactionAffinity(string(txID))
	return err
}

// affinityTrackingStream wraps a streaming RPC client to intercept Recv()
// calls and record transaction affinity from the first PartialResultSet that
// contains a transaction ID.
type affinityTrackingStream struct {
	grpc.ClientStream
	router             *locationRouter
	affinityEndpoint   channelEndpoint
	trackReadOnlyBegin bool
	readOnlyStrong     bool
	trackAffinity      bool
	ctx                context.Context
	once               sync.Once
	inner              spannerpb.Spanner_ExecuteStreamingSqlClient
}

// streamingClient is the common interface for StreamingRead and ExecuteStreamingSql
// response streams.
type streamingClient interface {
	Recv() (*spannerpb.PartialResultSet, error)
	grpc.ClientStream
}

func newAffinityTrackingStream(
	inner streamingClient,
	router *locationRouter,
	affinityEndpoint channelEndpoint,
	trackReadOnlyBegin bool,
	readOnlyStrong bool,
	trackAffinity bool,
	ctx context.Context,
) *affinityTrackingStream {
	return &affinityTrackingStream{
		ClientStream:       inner,
		router:             router,
		affinityEndpoint:   affinityEndpoint,
		trackReadOnlyBegin: trackReadOnlyBegin,
		readOnlyStrong:     readOnlyStrong,
		trackAffinity:      trackAffinity,
		ctx:                ctx,
		inner:              inner,
	}
}

func (s *affinityTrackingStream) Recv() (*spannerpb.PartialResultSet, error) {
	prs, err := s.inner.Recv()
	if err != nil {
		return nil, err
	}
	if prs.GetStats() != nil {
		fmt.Println("lar.execute_streaming_sql.partial_result_set_stats_json", larProtoJSON(prs.GetStats()))
		responseAttrs := []attribute.KeyValue{
			attribute.Bool("has_stats", true),
		}
		responseAttrs = append(responseAttrs, larJSONPayloadAttrs(larProtoJSON(prs))...)
		larTraceEvent(s.ctx, "lar.execute_streaming_sql.partial_result_set_json", responseAttrs...)
	}
	// Record transaction metadata from the first PartialResultSet that contains
	// a transaction ID.
	if txMeta := prs.GetMetadata().GetTransaction(); txMeta != nil && len(txMeta.GetId()) > 0 {
		txID := string(txMeta.GetId())
		s.once.Do(func() {
			if s.trackReadOnlyBegin {
				s.router.trackReadOnlyTransaction(txID, s.readOnlyStrong)
				larTraceEvent(s.ctx, "lar.affinity_track_readonly",
					attribute.Bool("prefer_leader", s.readOnlyStrong),
				)
				return
			}
			if s.trackAffinity {
				s.router.setTransactionAffinity(txID, s.affinityEndpoint)
				larTraceEvent(s.ctx, "lar.affinity_recorded",
					attribute.String("target_address", endpointAddressOrDefault(s.affinityEndpoint)),
				)
			}
		})
	}
	// Observe cache updates from every PartialResultSet.
	s.router.observePartialResultSet(s.ctx, prs)
	return prs, nil
}

func readOnlyBeginFromSelector(selector *spannerpb.TransactionSelector) (bool, bool) {
	if selector == nil {
		return false, false
	}
	begin := selector.GetBegin()
	if begin == nil || begin.GetReadOnly() == nil {
		return false, false
	}
	return true, begin.GetReadOnly().GetStrong()
}

func isReadWriteBeginFromSelector(selector *spannerpb.TransactionSelector) bool {
	if selector == nil {
		return false
	}
	begin := selector.GetBegin()
	return begin != nil && begin.GetReadOnly() == nil
}

func readOnlyBeginFromTransactionOptions(options *spannerpb.TransactionOptions) (bool, bool) {
	if options == nil || options.GetReadOnly() == nil {
		return false, false
	}
	return true, options.GetReadOnly().GetStrong()
}
