// Package clickhouseprometheusv2 is the second-generation ClickHouse-backed
// Prometheus provider. It exists because the v1 provider fetches every raw
// sample of a query's union window through the remote-read protobuf layer
// and hands it to the engine — the cost is a function of ingested data, not
// of the question asked. Here the stock promql engine evaluates over a
// native storage.Querier: no translation layer, per-selector fetch windows,
// and fetch reductions that are provably invisible to the engine. Every
// reduction either preserves engine semantics exactly or is not performed.
//
// # Series lookup
//
// Matchers resolve to series once per selector (selectSeries) against the
// time-series tables, which hold one row per (fingerprint, bucket) at
// 1h/6h/1d/1w granularities; timeSeriesTableFor picks the table whose bucket
// fits the window and rounds the window start down to the bucket boundary.
// How matchers become SQL, and why regexes are anchored, is documented at
// applySeriesConditions. Empty-valued labels come off at this boundary: an
// empty value means "label absent" in Prometheus, but stored attribute JSON
// can carry them.
//
// # Sample fetch
//
// Samples are fetched per selector using the engine's per-selector hints,
// not the query-wide union window, so foo / foo offset 1d reads two narrow
// windows instead of the widest one twice. Instant selectors of
// subquery-free queries fetch only the last sample per step bucket — see
// lastSamplePerStep for the correctness argument — while range selectors
// always fetch raw: every sample feeds the range function. Row assembly maps
// stale flags to the engine's StaleNaN and merges series with identical
// label sets (sortAndMerge), because the engine assumes storages never emit
// duplicates.
//
// # Sharding
//
// samples_v4 and time_series_v4 (and all their rollups) shard on the same
// key — cityHash64(env, temporality, metric_name, fingerprint) — so a
// series' samples and catalog rows live on the same shard. The samples
// fetch exploits that: it restricts by a shard-local series subquery, not a
// GLOBAL broadcast of the matched set. Delta-temporality
// series stay invisible to PromQL exactly as they are in v1: the rollout
// gate is parity with v1, and a Delta stream fed to rate() as-if-cumulative
// would be wrong, not just new.
//
// # Observability
//
// Every statement carries a log_comment with
// code.namespace=clickhouse-prometheus-v2 and code.function.name naming the
// call site, so this provider's work is attributable in system.query_log.
package clickhouseprometheusv2
