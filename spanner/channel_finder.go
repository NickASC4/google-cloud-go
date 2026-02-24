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
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	"go.opentelemetry.io/otel/attribute"
)

type channelFinder struct {
	updateMu sync.Mutex

	databaseID  atomic.Uint64
	recipeCache *keyRecipeCache
	rangeCache  *keyRangeCache
}

func newChannelFinder(endpointCache channelEndpointCache) *channelFinder {
	return &channelFinder{
		recipeCache: newKeyRecipeCache(),
		rangeCache:  newKeyRangeCache(endpointCache),
	}
}

func (f *channelFinder) useDeterministicRandom() {
	f.rangeCache.useDeterministicRandom()
}

func (f *channelFinder) update(ctx context.Context, update *sppb.CacheUpdate) {
	if update == nil {
		return
	}
	f.updateMu.Lock()
	defer f.updateMu.Unlock()

	currentID := f.databaseID.Load()
	cacheCleared := false
	if currentID != update.GetDatabaseId() {
		if currentID != 0 {
			f.recipeCache.clear()
			f.rangeCache.clear()
			cacheCleared = true
		}
		f.databaseID.Store(update.GetDatabaseId())
	}
	if update.GetKeyRecipes() != nil {
		f.recipeCache.addRecipes(update.GetKeyRecipes())
	}
	f.rangeCache.addRanges(update)
	recipesCount := int64(0)
	if update.GetKeyRecipes() != nil {
		recipesCount = int64(len(update.GetKeyRecipes().GetRecipe()))
	}
	larTraceEvent(ctx, "lar.cache_update",
		attribute.Bool("cache_cleared", cacheCleared),
		attribute.Int64("recipes_count", recipesCount),
		attribute.Int64("ranges_count", int64(len(update.GetRange()))),
		attribute.Int64("groups_count", int64(len(update.GetGroup()))),
	)
}

func (f *channelFinder) findServerRead(ctx context.Context, req *sppb.ReadRequest, preferLeader bool) channelEndpoint {
	if req == nil {
		return nil
	}
	findStart := time.Now()
	sskeyStart := time.Now()
	f.recipeCache.computeReadKeys(req)
	sskeyDurationMicros := time.Since(sskeyStart).Microseconds()
	larTraceEvent(ctx, "lar.sskey_generation",
		attribute.Int64("duration_us", sskeyDurationMicros),
		attribute.String("operation_type", "read"),
	)
	hint := ensureReadRoutingHint(req)
	lookupStart := time.Now()
	endpoint := f.fillRoutingHint(ctx, preferLeader, rangeModeCoveringSplit, req.GetDirectedReadOptions(), hint)
	lookupDurationMicros := time.Since(lookupStart).Microseconds()
	totalDurationMicros := time.Since(findStart).Microseconds()
	targetAddress := "default"
	result := "miss"
	if endpoint != nil {
		targetAddress = endpoint.Address()
		result = "hit"
	}
	larTraceEvent(ctx, "lar.range_cache_lookup",
		attribute.Int64("duration_us", lookupDurationMicros),
		attribute.Bool("cache_hit", endpoint != nil),
	)
	larTraceEvent(ctx, "lar.find_server",
		attribute.Int64("duration_us", totalDurationMicros),
		attribute.String("result", result),
		attribute.String("target_address", targetAddress),
	)
	return endpoint
}

func (f *channelFinder) findServerReadWithTransaction(ctx context.Context, req *sppb.ReadRequest) channelEndpoint {
	if req == nil {
		return nil
	}
	return f.findServerRead(ctx, req, preferLeaderFromSelector(req.GetTransaction()))
}

func (f *channelFinder) findServerExecuteSQL(ctx context.Context, req *sppb.ExecuteSqlRequest, preferLeader bool) channelEndpoint {
	if req == nil {
		return nil
	}
	debugStale := shouldLogStaleQueryDebugRequest(req)
	findStart := time.Now()
	sskeyStart := time.Now()
	f.recipeCache.computeQueryKeys(req)
	sskeyDurationMicros := time.Since(sskeyStart).Microseconds()
	larTraceEvent(ctx, "lar.sskey_generation",
		attribute.Int64("duration_us", sskeyDurationMicros),
		attribute.String("operation_type", "query"),
	)
	hint := ensureExecuteSQLRoutingHint(req)
	generatedKey := append([]byte(nil), hint.GetKey()...)
	generatedLimitKey := append([]byte(nil), hint.GetLimitKey()...)
	lookupStart := time.Now()
	endpoint := f.fillRoutingHint(ctx, preferLeader, rangeModePickRandom, req.GetDirectedReadOptions(), hint)
	lookupDurationMicros := time.Since(lookupStart).Microseconds()
	totalDurationMicros := time.Since(findStart).Microseconds()
	targetAddress := "default"
	result := "miss"
	if endpoint != nil {
		targetAddress = endpoint.Address()
		result = "hit"
	}
	if debugStale {
		fmt.Println(
			"lar.debug.stale_query.routing_hint",
			"request_tag="+req.GetRequestOptions().GetRequestTag(),
			"param_Id="+paramValueForDebug(req, "Id"),
			"generated_key_b64="+base64.StdEncoding.EncodeToString(generatedKey),
			"generated_key_hex="+hex.EncodeToString(generatedKey),
			"generated_limit_key_b64="+base64.StdEncoding.EncodeToString(generatedLimitKey),
			"generated_limit_key_hex="+hex.EncodeToString(generatedLimitKey),
			"final_key_b64="+base64.StdEncoding.EncodeToString(hint.GetKey()),
			"final_key_hex="+hex.EncodeToString(hint.GetKey()),
			"final_limit_key_b64="+base64.StdEncoding.EncodeToString(hint.GetLimitKey()),
			"final_limit_key_hex="+hex.EncodeToString(hint.GetLimitKey()),
			"group_uid="+fmt.Sprint(hint.GetGroupUid()),
			"split_id="+fmt.Sprint(hint.GetSplitId()),
			"tablet_uid="+fmt.Sprint(hint.GetTabletUid()),
			"target_address="+targetAddress,
		)
	}
	larTraceEvent(ctx, "lar.range_cache_lookup",
		attribute.Int64("duration_us", lookupDurationMicros),
		attribute.Bool("cache_hit", endpoint != nil),
	)
	larTraceEvent(ctx, "lar.find_server",
		attribute.Int64("duration_us", totalDurationMicros),
		attribute.String("result", result),
		attribute.String("target_address", targetAddress),
	)
	return endpoint
}

func paramValueForDebug(req *sppb.ExecuteSqlRequest, name string) string {
	if req == nil || req.GetParams() == nil {
		return ""
	}
	value := req.GetParams().GetFields()[name]
	if value == nil {
		return ""
	}
	if s := value.GetStringValue(); s != "" {
		return s
	}
	if value.GetNumberValue() != 0 {
		return fmt.Sprintf("%v", value.GetNumberValue())
	}
	if value.GetBoolValue() {
		return "true"
	}
	if value.GetNullValue() != 0 {
		return "NULL"
	}
	return value.String()
}

func (f *channelFinder) findServerExecuteSQLWithTransaction(ctx context.Context, req *sppb.ExecuteSqlRequest) channelEndpoint {
	if req == nil {
		return nil
	}
	return f.findServerExecuteSQL(ctx, req, preferLeaderFromSelector(req.GetTransaction()))
}

func (f *channelFinder) findServerBeginTransaction(ctx context.Context, req *sppb.BeginTransactionRequest) channelEndpoint {
	if req == nil || req.GetMutationKey() == nil {
		return nil
	}
	findStart := time.Now()
	target := f.recipeCache.mutationToTargetRange(req.GetMutationKey())
	if target == nil {
		return nil
	}
	hint := &sppb.RoutingHint{Key: append([]byte(nil), target.start...)}
	if len(target.limit) > 0 {
		hint.LimitKey = append([]byte(nil), target.limit...)
	}
	endpoint := f.fillRoutingHint(ctx, preferLeaderFromTransactionOptions(req.GetOptions()), rangeModeCoveringSplit, &sppb.DirectedReadOptions{}, hint)
	totalDurationMicros := time.Since(findStart).Microseconds()
	targetAddress := "default"
	result := "miss"
	if endpoint != nil {
		targetAddress = endpoint.Address()
		result = "hit"
	}
	larTraceEvent(ctx, "lar.find_server",
		attribute.Int64("duration_us", totalDurationMicros),
		attribute.String("operation_type", "begin_transaction"),
		attribute.String("result", result),
		attribute.String("target_address", targetAddress),
	)
	return endpoint
}

func (f *channelFinder) fillRoutingHint(ctx context.Context, preferLeader bool, mode rangeMode, directedReadOptions *sppb.DirectedReadOptions, hint *sppb.RoutingHint) channelEndpoint {
	if hint == nil {
		return nil
	}
	databaseID := f.databaseID.Load()
	if databaseID == 0 {
		return nil
	}
	hint.DatabaseId = databaseID
	return f.rangeCache.fillRoutingHint(ctx, preferLeader, mode, directedReadOptions, hint)
}

func preferLeaderFromSelector(selector *sppb.TransactionSelector) bool {
	if selector == nil {
		return true
	}
	switch s := selector.GetSelector().(type) {
	case *sppb.TransactionSelector_Begin:
		if s.Begin == nil || s.Begin.GetReadOnly() == nil {
			return true
		}
		return s.Begin.GetReadOnly().GetStrong()
	case *sppb.TransactionSelector_SingleUse:
		if s.SingleUse == nil || s.SingleUse.GetReadOnly() == nil {
			return true
		}
		return s.SingleUse.GetReadOnly().GetStrong()
	default:
		return true
	}
}

func preferLeaderFromTransactionOptions(options *sppb.TransactionOptions) bool {
	if options == nil || options.GetReadOnly() == nil {
		return true
	}
	return options.GetReadOnly().GetStrong()
}
