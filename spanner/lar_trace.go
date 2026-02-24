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

	"cloud.google.com/go/internal/trace"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func larTraceEvent(ctx context.Context, eventName string, attrs ...attribute.KeyValue) {
	if ctx == nil {
		return
	}
	trace.TracePrintf(ctx, attrsToMap(attrs...), "%s", eventName)
}

func larProtoJSON(m proto.Message) string {
	if m == nil {
		return "{}"
	}
	out, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}.Marshal(m)
	if err != nil {
		return fmt.Sprintf("{\"marshal_error\":%q}", err.Error())
	}
	return string(out)
}

func attrsToMap(attrs ...attribute.KeyValue) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	result := make(map[string]interface{}, len(attrs))
	for _, attr := range attrs {
		key := string(attr.Key)
		switch attr.Value.Type() {
		case attribute.BOOL:
			result[key] = attr.Value.AsBool()
		case attribute.INT64:
			result[key] = attr.Value.AsInt64()
		case attribute.FLOAT64:
			result[key] = attr.Value.AsFloat64()
		case attribute.STRING:
			result[key] = attr.Value.AsString()
		case attribute.BOOLSLICE:
			result[key] = attr.Value.AsBoolSlice()
		case attribute.INT64SLICE:
			result[key] = attr.Value.AsInt64Slice()
		case attribute.FLOAT64SLICE:
			result[key] = attr.Value.AsFloat64Slice()
		case attribute.STRINGSLICE:
			result[key] = attr.Value.AsStringSlice()
		default:
			result[key] = attr.Value.AsString()
		}
	}
	return result
}
