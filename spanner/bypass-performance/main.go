// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	gcpmetric "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	gcptrace "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/trace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	otelmetricdata "go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultProject            = "default"
	defaultInstance           = "default"
	defaultDatabase           = "db"
	defaultEndpoint           = "spanner.spanner-ns:15000"
	defaultProbeType          = "strong_read"
	defaultQPS                = 4
	defaultNumRows            = 10_000_000
	defaultPayloadSize        = 1000
	defaultMaxStalenessSecond = 60
	defaultWarmupCycles       = 1000
	defaultParallelism        = 8
	defaultLogIntervalSeconds = 10

	defaultOTELServiceName                = "irahul-gbypass"
	defaultOTELProjectID                  = "outbound-flight"
	defaultOTELMetricPrefix               = "custom.googleapis.com/irahul"
	defaultOTELTraceSamplingFraction      = 1.0
	defaultOTELMetricExportIntervalSecond = 10

	tableName = "T"
	columnKey = "Key"
	columnVal = "Value"
)

type config struct {
	project             string
	instance            string
	database            string
	databasePath        string
	endpoint            string
	insecure            bool
	probeType           string
	queryMode           string
	qps                 int
	numRows             int64
	payloadSize         int
	maxStalenessSeconds int64
	warmupCycles        int
	parallelism         int
	logIntervalSeconds  int

	enableOTEL                     bool
	otelProjectID                  string
	otelServiceName                string
	otelMetricPrefix               string
	enableProbeTracing             bool
	otelTraceSamplingFraction      float64
	otelMetricExportIntervalSecond int
	cloudTraceEndpoint             string
	monitoringEndpoint             string
	enableSpannerEndToEndTracing   bool
}

type probe interface {
	Name() string
	Probe(context.Context) error
}

type metrics struct {
	okCount         atomic.Int64
	errorCount      atomic.Int64
	totalLatencyMic atomic.Int64
}

type otelRuntime struct {
	shutdown         func(context.Context) error
	meterProvider    otelmetric.MeterProvider
	tracer           oteltrace.Tracer
	requestCounter   otelmetric.Int64Counter
	latencyHistogram otelmetric.Float64Histogram
	host             string
	bypassEnabled    bool
}

func main() {
	rand.Seed(time.Now().UnixNano())
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("invalid configuration: %v", err)
	}
	printConfig(cfg)

	rootCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx := rootCtx

	otelState, err := initializeOpenTelemetry(ctx, cfg)
	if err != nil {
		log.Fatalf("failed to initialize OpenTelemetry: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if shutdownErr := otelState.shutdown(shutdownCtx); shutdownErr != nil {
			log.Printf("OpenTelemetry shutdown error: %v", shutdownErr)
		}
	}()

	client, err := newClient(ctx, cfg, otelState)
	if err != nil {
		log.Fatalf("failed to create spanner client: %v", err)
	}
	defer client.Close()

	p, err := newProbe(client, cfg)
	if err != nil {
		log.Fatalf("failed to create probe: %v", err)
	}
	if err := warmup(ctx, p, cfg.warmupCycles); err != nil {
		log.Fatalf("warmup failed: %v", err)
	}
	log.Printf("warmup complete (%d cycles), starting probe loop", cfg.warmupCycles)
	run(ctx, cfg, p, otelState)
}

func loadConfig() (config, error) {
	cfg := config{
		project:             getEnv("GOOGLE_CLOUD_PROJECT", defaultProject),
		instance:            getEnv("SPANNER_INSTANCE", defaultInstance),
		database:            getEnv("SPANNER_DATABASE", defaultDatabase),
		databasePath:        strings.TrimSpace(os.Getenv("SPANNER_DATABASE_PATH")),
		endpoint:            normalizeEndpoint(getEnv("ENDPOINT", defaultEndpoint)),
		insecure:            getEnvBool("INSECURE", true),
		probeType:           strings.ToLower(getEnv("PROBE_TYPE", defaultProbeType)),
		queryMode:           strings.ToLower(getEnv("QUERY_MODE", "normal")),
		qps:                 getEnvInt("QPS", defaultQPS),
		numRows:             getEnvInt64("NUM_ROWS", defaultNumRows),
		payloadSize:         getEnvInt("PAYLOAD_SIZE", defaultPayloadSize),
		maxStalenessSeconds: getEnvInt64("MAX_STALENESS_SECONDS", defaultMaxStalenessSecond),
		warmupCycles:        getEnvInt("WARMUP_CYCLES", defaultWarmupCycles),
		parallelism:         getEnvInt("PARALLELISM", defaultParallelism),
		logIntervalSeconds:  getEnvInt("LOG_INTERVAL_SECONDS", defaultLogIntervalSeconds),

		enableOTEL:                     getEnvBool("OTEL_ENABLED", true),
		otelProjectID:                  getEnv("OTEL_PROJECT_ID", defaultOTELProjectID),
		otelServiceName:                getEnv("OTEL_SERVICE_NAME", defaultOTELServiceName),
		otelMetricPrefix:               getEnv("OTEL_METRIC_PREFIX", defaultOTELMetricPrefix),
		enableProbeTracing:             getEnvBool("PROBE_TRACING_ENABLED", true),
		otelTraceSamplingFraction:      getEnvFloat64("OTEL_TRACE_SAMPLING_FRACTION", defaultOTELTraceSamplingFraction),
		otelMetricExportIntervalSecond: getEnvInt("OTEL_METRIC_EXPORT_INTERVAL_SECONDS", defaultOTELMetricExportIntervalSecond),
		cloudTraceEndpoint:             normalizeEndpoint(strings.TrimSpace(os.Getenv("CLOUD_TRACE_ENDPOINT"))),
		monitoringEndpoint:             normalizeEndpoint(strings.TrimSpace(os.Getenv("SPANNER_MONITORING_HOST"))),
		enableSpannerEndToEndTracing:   getEnvBool("SPANNER_ENABLE_END_TO_END_TRACING", true),
	}
	if cfg.databasePath == "" {
		cfg.databasePath = fmt.Sprintf("projects/%s/instances/%s/databases/%s", cfg.project, cfg.instance, cfg.database)
	}
	switch {
	case cfg.qps <= 0:
		return cfg, fmt.Errorf("QPS must be > 0, got %d", cfg.qps)
	case cfg.numRows <= 0:
		return cfg, fmt.Errorf("NUM_ROWS must be > 0, got %d", cfg.numRows)
	case cfg.payloadSize <= 0:
		return cfg, fmt.Errorf("PAYLOAD_SIZE must be > 0, got %d", cfg.payloadSize)
	case cfg.parallelism <= 0:
		return cfg, fmt.Errorf("PARALLELISM must be > 0, got %d", cfg.parallelism)
	case cfg.maxStalenessSeconds <= 0:
		return cfg, fmt.Errorf("MAX_STALENESS_SECONDS must be > 0, got %d", cfg.maxStalenessSeconds)
	case cfg.warmupCycles < 0:
		return cfg, fmt.Errorf("WARMUP_CYCLES must be >= 0, got %d", cfg.warmupCycles)
	case cfg.logIntervalSeconds <= 0:
		return cfg, fmt.Errorf("LOG_INTERVAL_SECONDS must be > 0, got %d", cfg.logIntervalSeconds)
	case cfg.otelTraceSamplingFraction < 0 || cfg.otelTraceSamplingFraction > 1:
		return cfg, fmt.Errorf("OTEL_TRACE_SAMPLING_FRACTION must be in [0,1], got %f", cfg.otelTraceSamplingFraction)
	case cfg.otelMetricExportIntervalSecond <= 0:
		return cfg, fmt.Errorf("OTEL_METRIC_EXPORT_INTERVAL_SECONDS must be > 0, got %d", cfg.otelMetricExportIntervalSecond)
	}
	switch cfg.queryMode {
	case "normal", "stats":
	default:
		return cfg, fmt.Errorf("QUERY_MODE must be one of [normal, stats], got %q", cfg.queryMode)
	}
	return cfg, nil
}

func printConfig(cfg config) {
	log.Printf("probe=%s query_mode=%s qps=%d parallelism=%d endpoint=%s db=%s num_rows=%d payload_size=%d max_staleness_s=%d bypass=%s otel_enabled=%t probe_tracing_enabled=%t otel_service=%s",
		cfg.probeType,
		cfg.queryMode,
		cfg.qps,
		cfg.parallelism,
		cfg.endpoint,
		cfg.databasePath,
		cfg.numRows,
		cfg.payloadSize,
		cfg.maxStalenessSeconds,
		os.Getenv("GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API"),
		cfg.enableOTEL,
		cfg.enableProbeTracing,
		cfg.otelServiceName,
	)
}

func initializeOpenTelemetry(ctx context.Context, cfg config) (*otelRuntime, error) {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	bypassEnabled := getEnvBool("GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API", false)
	noop := &otelRuntime{
		shutdown:      func(context.Context) error { return nil },
		meterProvider: otel.GetMeterProvider(),
		tracer:        otel.Tracer("gbypass"),
		host:          host,
		bypassEnabled: bypassEnabled,
	}
	if !cfg.enableOTEL {
		return noop, nil
	}

	res := resource.NewWithAttributes(
		"",
		attribute.String("service.name", cfg.otelServiceName),
	)

	traceOpts := []gcptrace.Option{gcptrace.WithProjectID(cfg.otelProjectID)}
	if cfg.cloudTraceEndpoint != "" {
		traceOpts = append(traceOpts, gcptrace.WithTraceClientOptions([]option.ClientOption{option.WithEndpoint(cfg.cloudTraceEndpoint)}))
	}
	traceExp, err := gcptrace.New(traceOpts...)
	if err != nil {
		return nil, fmt.Errorf("create cloud trace exporter: %w", err)
	}
	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(cfg.otelTraceSamplingFraction)),
		sdktrace.WithBatcher(traceExp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(traceProvider)

	metricOpts := []gcpmetric.Option{
		gcpmetric.WithProjectID(cfg.otelProjectID),
		gcpmetric.WithMetricDescriptorTypeFormatter(func(m otelmetricdata.Metrics) string {
			prefix := strings.TrimSuffix(cfg.otelMetricPrefix, "/")
			name := strings.ReplaceAll(m.Name, ".", "/")
			return prefix + "/" + name
		}),
	}
	if cfg.monitoringEndpoint != "" {
		metricOpts = append(metricOpts, gcpmetric.WithMonitoringClientOptions(option.WithEndpoint(cfg.monitoringEndpoint)))
	}
	metricExp, err := gcpmetric.New(metricOpts...)
	if err != nil {
		return nil, fmt.Errorf("create cloud monitoring exporter: %w", err)
	}
	metricReader := sdkmetric.NewPeriodicReader(
		metricExp,
		sdkmetric.WithInterval(time.Duration(cfg.otelMetricExportIntervalSecond)*time.Second),
	)
	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(metricReader),
	)
	otel.SetMeterProvider(meterProvider)

	meter := meterProvider.Meter("gbypass")
	requestCounter, err := meter.Int64Counter(
		"gop_count",
		otelmetric.WithDescription("Total requests processed by gbypass"),
		otelmetric.WithUnit("1"),
	)
	if err != nil {
		return nil, fmt.Errorf("create counter instrument: %w", err)
	}
	latencyHistogram, err := meter.Float64Histogram(
		"glatency",
		otelmetric.WithDescription("Request latency in milliseconds"),
		otelmetric.WithUnit("ms"),
		otelmetric.WithExplicitBucketBoundaries(
			0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000,
		),
	)
	if err != nil {
		return nil, fmt.Errorf("create histogram instrument: %w", err)
	}

	return &otelRuntime{
		shutdown: func(shutdownCtx context.Context) error {
			return errors.Join(
				meterProvider.Shutdown(shutdownCtx),
				traceProvider.Shutdown(shutdownCtx),
			)
		},
		meterProvider:    meterProvider,
		tracer:           otel.Tracer("gbypass"),
		requestCounter:   requestCounter,
		latencyHistogram: latencyHistogram,
		host:             host,
		bypassEnabled:    bypassEnabled,
	}, nil
}

func (o *otelRuntime) observeProbe(ctx context.Context, probeName string, latency time.Duration) {
	if o == nil || o.requestCounter == nil || o.latencyHistogram == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("method", probeName),
		attribute.Bool("bypass", o.bypassEnabled),
		attribute.String("host", o.host),
	}
	o.requestCounter.Add(ctx, 1, otelmetric.WithAttributes(attrs...))
	o.latencyHistogram.Record(ctx, float64(latency.Microseconds())/1000.0, otelmetric.WithAttributes(attrs...))
}

func newClient(ctx context.Context, cfg config, otelState *otelRuntime) (*spanner.Client, error) {
	opts := []option.ClientOption{
		option.WithEndpoint(cfg.endpoint),
	}
	if cfg.insecure {
		opts = append(opts, option.WithoutAuthentication())
		opts = append(opts, option.WithGRPCDialOption(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		))
	}
	clientConfig := spanner.ClientConfig{
		SessionPoolConfig:          spanner.DefaultSessionPoolConfig,
		DisableRouteToLeader:       false,
		OpenTelemetryMeterProvider: otelState.meterProvider,
		EnableEndToEndTracing:      cfg.enableSpannerEndToEndTracing,
	}
	return spanner.NewClientWithConfig(ctx, cfg.databasePath, clientConfig, opts...)
}

func run(ctx context.Context, cfg config, p probe, otelState *otelRuntime) {
	period := time.Second / time.Duration(cfg.qps)
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	statsTicker := time.NewTicker(time.Duration(cfg.logIntervalSeconds) * time.Second)
	defer statsTicker.Stop()

	sem := make(chan struct{}, cfg.parallelism)
	m := &metrics{}
	start := time.Now()

	for {
		select {
		case <-ctx.Done():
			log.Printf("stopping (%v)", ctx.Err())
			printSummary(m, time.Since(start))
			return
		case <-statsTicker.C:
			printSummary(m, time.Since(start))
		case <-ticker.C:
			select {
			case sem <- struct{}{}:
				go func() {
					defer func() { <-sem }()

					probeCtx := ctx
					var span oteltrace.Span
					if cfg.enableProbeTracing && otelState != nil && otelState.tracer != nil {
						probeCtx, span = otelState.tracer.Start(
							ctx,
							"probe."+p.Name(),
							oteltrace.WithAttributes(
								attribute.String("probe.type", p.Name()),
								attribute.Bool("probe.bypass_enabled", otelState.bypassEnabled),
								attribute.String("probe.host", otelState.host),
							),
						)
					}

					callStart := time.Now()
					err := p.Probe(probeCtx)
					latency := time.Since(callStart)
					m.totalLatencyMic.Add(latency.Microseconds())

					if span != nil {
						if err != nil {
							span.SetStatus(otelcodes.Error, err.Error())
							span.RecordError(err)
						}
						span.SetAttributes(attribute.Float64("probe.latency_ms", float64(latency.Microseconds())/1000.0))
						span.End()
					}
					if otelState != nil {
						otelState.observeProbe(probeCtx, p.Name(), latency)
					}

					if err != nil {
						m.errorCount.Add(1)
						log.Printf("probe_error type=%s err=%v", p.Name(), err)
						return
					}
					m.okCount.Add(1)
				}()
			default:
			}
		}
	}
}

func warmup(ctx context.Context, p probe, cycles int) error {
	for i := 0; i < cycles; i++ {
		if err := p.Probe(ctx); err != nil {
			return fmt.Errorf("warmup cycle %d failed: %w", i, err)
		}
	}
	return nil
}

func printSummary(m *metrics, elapsed time.Duration) {
	ok := m.okCount.Load()
	errs := m.errorCount.Load()
	total := ok + errs
	avgLatMic := int64(0)
	if total > 0 {
		avgLatMic = m.totalLatencyMic.Load() / total
	}
	qps := 0.0
	if elapsed > 0 {
		qps = float64(ok) / elapsed.Seconds()
	}
	log.Printf("stats elapsed=%s ok=%d err=%d avg_latency_us=%d achieved_qps=%.2f", elapsed.Truncate(time.Second), ok, errs, avgLatMic, qps)
}

func newProbe(client *spanner.Client, cfg config) (probe, error) {
	switch cfg.probeType {
	case "strong_read":
		return &strongReadProbe{client: client, numRows: cfg.numRows}, nil
	case "stale_read":
		return &staleReadProbe{client: client, numRows: cfg.numRows, maxStalenessSeconds: cfg.maxStalenessSeconds}, nil
	case "read_write":
		return &readWriteProbe{client: client, numRows: cfg.numRows, payloadSize: cfg.payloadSize}, nil
	case "rr_occ_dml":
		return &dmlProbe{client: client, numRows: cfg.numRows, payloadSize: cfg.payloadSize, repeatableRead: true}, nil
	case "dml":
		return &dmlProbe{client: client, numRows: cfg.numRows, payloadSize: cfg.payloadSize, repeatableRead: false}, nil
	case "blind_dml":
		return &blindDMLProbe{client: client, numRows: cfg.numRows, payloadSize: cfg.payloadSize}, nil
	case "multi_blind_dml":
		return &multiBlindDMLProbe{client: client, numRows: cfg.numRows, payloadSize: cfg.payloadSize, statements: 5}, nil
	case "strong_query":
		return &queryProbe{client: client, numRows: cfg.numRows, queryMode: cfg.queryMode}, nil
	case "stale_query":
		return &queryProbe{client: client, numRows: cfg.numRows, maxStalenessSeconds: cfg.maxStalenessSeconds, queryMode: cfg.queryMode}, nil
	case "multi_use_ro_query":
		return &multiUseReadOnlyQueryProbe{client: client, numRows: cfg.numRows, queryMode: cfg.queryMode}, nil
	case "write":
		return &writeProbe{client: client, numRows: cfg.numRows, payloadSize: cfg.payloadSize, replayProtection: true}, nil
	case "write_no_rp":
		return &writeProbe{client: client, numRows: cfg.numRows, payloadSize: cfg.payloadSize, replayProtection: false}, nil
	default:
		return nil, fmt.Errorf("unsupported PROBE_TYPE: %s", cfg.probeType)
	}
}

type strongReadProbe struct {
	client  *spanner.Client
	numRows int64
}

func (p *strongReadProbe) Name() string { return "strong_read" }

func (p *strongReadProbe) Probe(ctx context.Context) error {
	key := randomKey(p.numRows)
	iter := p.client.Single().ReadWithOptions(
		ctx,
		tableName,
		spanner.Key{key},
		[]string{columnKey, columnVal},
		&spanner.ReadOptions{RequestTag: requestTag(p.Name())},
	)
	return consumeRows(iter)
}

type staleReadProbe struct {
	client              *spanner.Client
	numRows             int64
	maxStalenessSeconds int64
}

func (p *staleReadProbe) Name() string { return "stale_read" }

func (p *staleReadProbe) Probe(ctx context.Context) error {
	key := randomKey(p.numRows)
	iter := p.client.Single().
		WithTimestampBound(spanner.MaxStaleness(time.Duration(p.maxStalenessSeconds)*time.Second)).
		ReadWithOptions(
			ctx,
			tableName,
			spanner.Key{key},
			[]string{columnKey, columnVal},
			&spanner.ReadOptions{RequestTag: requestTag(p.Name())},
		)
	return consumeRows(iter)
}

type queryProbe struct {
	client              *spanner.Client
	numRows             int64
	maxStalenessSeconds int64
	queryMode           string
}

func (p *queryProbe) Name() string {
	if p.maxStalenessSeconds > 0 {
		return "stale_query"
	}
	return "strong_query"
}

func (p *queryProbe) Probe(ctx context.Context) error {
	key := randomKey(p.numRows)
	stmt := spanner.Statement{
		SQL:    "SELECT Key, Value FROM T WHERE Key = @Id",
		Params: map[string]interface{}{"Id": key},
	}
	ro := p.client.Single()
	if p.maxStalenessSeconds > 0 {
		ro = ro.WithTimestampBound(spanner.MaxStaleness(time.Duration(p.maxStalenessSeconds) * time.Second))
	}
	queryOpts := spanner.QueryOptions{RequestTag: requestTag(p.Name())}
	if p.queryMode == "stats" {
		mode := sppb.ExecuteSqlRequest_WITH_STATS
		queryOpts.Mode = &mode
	}
	iter := ro.QueryWithOptions(ctx, stmt, queryOpts)
	return consumeRows(iter)
}

type readWriteProbe struct {
	client      *spanner.Client
	numRows     int64
	payloadSize int
}

func (p *readWriteProbe) Name() string { return "read_write" }

func (p *readWriteProbe) Probe(ctx context.Context) error {
	_, err := p.client.ReadWriteTransactionWithOptions(
		ctx,
		func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			key := randomKey(p.numRows)
			_, readErr := tx.ReadRow(ctx, tableName, spanner.Key{key}, []string{columnKey, columnVal})
			if readErr != nil && spanner.ErrCode(readErr) != codes.NotFound {
				return readErr
			}
			m := spanner.InsertOrUpdate(tableName, []string{columnKey, columnVal}, []interface{}{key, randomString(p.payloadSize)})
			return tx.BufferWrite([]*spanner.Mutation{m})
		},
		spanner.TransactionOptions{TransactionTag: requestTag(p.Name())},
	)
	return err
}

type dmlProbe struct {
	client         *spanner.Client
	numRows        int64
	payloadSize    int
	repeatableRead bool
}

func (p *dmlProbe) Name() string {
	if p.repeatableRead {
		return "rr_occ_dml"
	}
	return "dml"
}

func (p *dmlProbe) Probe(ctx context.Context) error {
	insertSQL := "INSERT T (Key, Value) VALUES(@Id, @payload)"
	updateSQL := "UPDATE T SET Value = @payload WHERE Key = @Id"
	_, err := p.client.ReadWriteTransactionWithOptions(
		ctx,
		func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			key := randomKey(p.numRows)
			forUpdate := ""
			if p.repeatableRead {
				forUpdate = " FOR UPDATE"
			}
			readStmt := spanner.Statement{
				SQL:    "SELECT Key, Value FROM T WHERE Key = @Id" + forUpdate,
				Params: map[string]interface{}{"Id": key},
			}
			iter := tx.QueryWithOptions(ctx, readStmt, spanner.QueryOptions{RequestTag: requestTag(p.Name())})
			found, err := hasAnyRow(iter)
			if err != nil {
				return err
			}
			sql := insertSQL
			if found {
				sql = updateSQL
			}
			_, err = tx.UpdateWithOptions(
				ctx,
				spanner.Statement{
					SQL: sql,
					Params: map[string]interface{}{
						"Id":      key,
						"payload": randomString(p.payloadSize),
					},
				},
				spanner.QueryOptions{
					RequestTag:    requestTag(p.Name()),
					LastStatement: true,
				},
			)
			return err
		},
		func() spanner.TransactionOptions {
			options := spanner.TransactionOptions{TransactionTag: requestTag(p.Name())}
			if p.repeatableRead {
				options.IsolationLevel = sppb.TransactionOptions_REPEATABLE_READ
			}
			return options
		}(),
	)
	return err
}

type blindDMLProbe struct {
	client      *spanner.Client
	numRows     int64
	payloadSize int
}

func (p *blindDMLProbe) Name() string { return "blind_dml" }

func (p *blindDMLProbe) Probe(ctx context.Context) error {
	_, err := p.client.ReadWriteTransactionWithOptions(
		ctx,
		func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			key := randomKey(p.numRows)
			_, err := tx.UpdateWithOptions(
				ctx,
				spanner.Statement{
					SQL: "INSERT OR UPDATE T (Key, Value) VALUES(@Id, @payload)",
					Params: map[string]interface{}{
						"Id":      key,
						"payload": randomString(p.payloadSize),
					},
				},
				spanner.QueryOptions{
					RequestTag:    requestTag(p.Name()),
					LastStatement: true,
				},
			)
			return err
		},
		spanner.TransactionOptions{
			TransactionTag: requestTag(p.Name()),
			CommitOptions: spanner.CommitOptions{
				MaxCommitDelay: durationPtr(0),
			},
		},
	)
	return err
}

type multiBlindDMLProbe struct {
	client      *spanner.Client
	numRows     int64
	payloadSize int
	statements  int
}

func (p *multiBlindDMLProbe) Name() string { return "multi_blind_dml" }

func (p *multiBlindDMLProbe) Probe(ctx context.Context) error {
	_, err := p.client.ReadWriteTransactionWithOptions(
		ctx,
		func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
			keys := make([]int64, p.statements)
			if p.numRows <= int64(p.statements) {
				for i := range keys {
					keys[i] = int64(i)
				}
			} else {
				keys[0] = randomKey(p.numRows)
				for i := 1; i < len(keys); i++ {
					keys[i] = keys[i-1] + 1
				}
			}
			payload := randomString(p.payloadSize)
			for i, key := range keys {
				_, err := tx.UpdateWithOptions(
					ctx,
					spanner.Statement{
						SQL: "INSERT OR UPDATE T (Key, Value) VALUES(@Id, @payload)",
						Params: map[string]interface{}{
							"Id":      key,
							"payload": payload,
						},
					},
					spanner.QueryOptions{
						RequestTag:    requestTag(p.Name()),
						LastStatement: i == len(keys)-1,
					},
				)
				if err != nil {
					return err
				}
			}
			return nil
		},
		spanner.TransactionOptions{
			TransactionTag: requestTag(p.Name()),
			CommitOptions: spanner.CommitOptions{
				MaxCommitDelay: durationPtr(0),
			},
		},
	)
	return err
}

type writeProbe struct {
	client           *spanner.Client
	numRows          int64
	payloadSize      int
	replayProtection bool
}

func (p *writeProbe) Name() string {
	if p.replayProtection {
		return "write"
	}
	return "write_no_rp"
}

func (p *writeProbe) Probe(ctx context.Context) error {
	key := randomKey(p.numRows)
	m := spanner.InsertOrUpdate(tableName, []string{columnKey, columnVal}, []interface{}{key, randomString(p.payloadSize)})
	opts := []spanner.ApplyOption{
		spanner.TransactionTag(requestTag(p.Name())),
		spanner.ApplyCommitOptions(spanner.CommitOptions{MaxCommitDelay: durationPtr(0)}),
	}
	if !p.replayProtection {
		opts = append(opts, spanner.ApplyAtLeastOnce())
	}
	_, err := p.client.Apply(ctx, []*spanner.Mutation{m}, opts...)
	return err
}

type multiUseReadOnlyQueryProbe struct {
	client    *spanner.Client
	numRows   int64
	queryMode string
}

func (p *multiUseReadOnlyQueryProbe) Name() string { return "multi_use_ro_query" }

func (p *multiUseReadOnlyQueryProbe) Probe(ctx context.Context) error {
	firstKey := randomKey(p.numRows)
	secondKey := randomKey(p.numRows)
	tx := p.client.ReadOnlyTransaction()
	defer tx.Close()
	readIter := tx.ReadWithOptions(
		ctx,
		tableName,
		spanner.Key{firstKey},
		[]string{columnKey, columnVal},
		&spanner.ReadOptions{RequestTag: requestTag(p.Name())},
	)
	if err := consumeRows(readIter); err != nil {
		return err
	}
	stmt := spanner.Statement{
		SQL:    "SELECT Key, Value FROM T WHERE Key = @Id",
		Params: map[string]interface{}{"Id": secondKey},
	}
	queryOpts := spanner.QueryOptions{RequestTag: requestTag(p.Name())}
	if p.queryMode == "stats" {
		mode := sppb.ExecuteSqlRequest_PROFILE
		queryOpts.Mode = &mode
	}
	queryIter := tx.QueryWithOptions(ctx, stmt, queryOpts)
	return consumeRows(queryIter)
}

func consumeRows(iter *spanner.RowIterator) error {
	defer iter.Stop()
	for {
		_, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func hasAnyRow(iter *spanner.RowIterator) (bool, error) {
	defer iter.Stop()
	_, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func randomKey(numRows int64) int64 {
	return rand.Int63n(numRows)
}

func requestTag(name string) string {
	return "probe_type=" + name
}

func randomString(length int) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
	out := make([]byte, length)
	for i := range out {
		out[i] = chars[rand.Intn(len(chars))]
	}
	return string(out)
}

func durationPtr(d time.Duration) *time.Duration {
	return &d
}

func normalizeEndpoint(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://") {
		u, err := url.Parse(raw)
		if err == nil && u.Host != "" {
			return u.Host
		}
	}
	return strings.TrimPrefix(strings.TrimPrefix(raw, "http://"), "https://")
}

func getEnv(key, defaultVal string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return defaultVal
}

func getEnvBool(key string, defaultVal bool) bool {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultVal
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return defaultVal
	}
	return parsed
}

func getEnvInt(key string, defaultVal int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultVal
	}
	parsed, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return parsed
}

func getEnvInt64(key string, defaultVal int64) int64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultVal
	}
	parsed, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return defaultVal
	}
	return parsed
}

func getEnvFloat64(key string, defaultVal float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return defaultVal
	}
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return defaultVal
	}
	return parsed
}
