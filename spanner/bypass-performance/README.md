# Go Spanner Bypass Performance

This tool runs continuous workload probes against Cloud Spanner and supports
classic routing vs. location-aware routing (`GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API=true`).

## Build and run

```bash
go run ./bypass-performance
```

## Environment variables

- `SPANNER_DATABASE_PATH`: full database path, e.g. `projects/p/instances/i/databases/d`.
- `GOOGLE_CLOUD_PROJECT`: used when `SPANNER_DATABASE_PATH` is unset. Default: `default`.
- `SPANNER_INSTANCE`: used when `SPANNER_DATABASE_PATH` is unset. Default: `default`.
- `SPANNER_DATABASE`: used when `SPANNER_DATABASE_PATH` is unset. Default: `db`.
- `ENDPOINT`: Spanner endpoint. Default: `spanner.spanner-ns:15000`.
- `INSECURE`: use plaintext + no auth (`true|false`). Default: `true`.
- `GOOGLE_SPANNER_EXPERIMENTAL_LOCATION_API`: set `true` to enable bypass routing in client.

### Workload controls

- `PROBE_TYPE`: default `strong_read`.
- `QUERY_MODE`: query execution mode for query probes (`normal|stats`). Default: `normal`.
- `QPS`: target QPS. Default: `4`.
- `PARALLELISM`: max in-flight requests. Default: `8`.
- `WARMUP_CYCLES`: warmup request count. Default: `1000`.
- `LOG_INTERVAL_SECONDS`: stats print interval. Default: `10`.
- `NUM_ROWS`: key-space size. Default: `10000000`.
- `PAYLOAD_SIZE`: write payload size. Default: `1000`.
- `MAX_STALENESS_SECONDS`: staleness for stale probes. Default: `60`.

### OpenTelemetry controls

- `OTEL_ENABLED`: enable/disable OTel init (`true|false`). Default: `true`.
- `OTEL_PROJECT_ID`: metrics/traces project id. Default: `outbound-flight`.
- `OTEL_SERVICE_NAME`: OTel `service.name`. Default: `irahul-gbypass`.
- `OTEL_METRIC_PREFIX`: metric descriptor prefix. Default: `custom.googleapis.com/irahul`.
- `PROBE_TRACING_ENABLED`: enable/disable trace pipeline for probe process (`true|false`). When `false`, probe spans and Spanner end-to-end traces are disabled. Default: `true`.
- `OTEL_TRACE_SAMPLING_FRACTION`: trace sampling ratio in `[0,1]`. Default: `1.0`.
- `OTEL_METRIC_EXPORT_INTERVAL_SECONDS`: metric export interval. Default: `10`.
- `CLOUD_TRACE_ENDPOINT`: optional Cloud Trace override endpoint.
- `SPANNER_MONITORING_HOST`: optional Cloud Monitoring override endpoint.
- `SPANNER_ENABLE_END_TO_END_TRACING`: enables client end-to-end tracing (`true|false`). Default: `true`.

## Supported probes

- `strong_read`
- `stale_read`
- `read_write`
- `rr_occ_dml`
- `dml`
- `blind_dml`
- `multi_blind_dml`
- `strong_query`
- `stale_query`
- `stale_query_test` (runs 3 stale query requests per probe call on fixed key `5000000`)
- `multi_use_ro_query`
- `write`
- `write_no_rp`
