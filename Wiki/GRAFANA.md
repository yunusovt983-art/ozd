# Grafana Dashboard — OpenZFS Daemon

> Импорт: Dashboards → Import → вставить JSON ниже.
> Datasource: Prometheus, scrape target `ozd:9100/metrics`.

## Панели

| Панель | Метрика | Тип |
|--------|---------|-----|
| PUT ops/s | `rate(ozd_puts_total[1m])` | Graph |
| GET ops/s | `rate(ozd_gets_total[1m])` | Graph |
| PUT p50/p99 | `histogram_quantile(0.5, rate(ozd_put_duration_seconds_bucket[5m]))` | Graph |
| GET p50/p99 | `histogram_quantile(0.99, rate(ozd_get_duration_seconds_bucket[5m]))` | Graph |
| Errors | `rate(ozd_put_errors_total[1m])` + `rate(ozd_get_errors_total[1m])` | Graph |
| Hedge rate | `rate(ozd_hedged_reads_total[1m])` | Stat |
| Handoff writes | `rate(ozd_handoff_writes_total[1m])` | Stat |
| MRF queue | `ozd_mrf_queue` | Gauge |
| Cache hit-rate | `rate(ozd_cache_hits_total[1m]) / (rate(ozd_cache_hits_total[1m]) + rate(ozd_cache_misses_total[1m]))` | Gauge % |
| Cache coalesced | `rate(ozd_cache_coalesced_total[1m])` | Stat |
| BG throttle rate | `ozd_bg_rate_bps` | Gauge |
| BG throttle waits | `rate(ozd_bg_throttle_waits_total[1m])` | Graph |
| GC reclaimed | `rate(ozd_gc_reclaimed_bytes_total[1m])` | Graph |
| Scrub corrupt | `ozd_scrub_corrupt_total` | Stat |
| Resilver progress | `rate(ozd_resilver_repaired_total[1m])` | Graph |
| EC reconstructs | `rate(ozd_ec_reconstructs_total[1m])` | Graph |
| Per-shard capacity | `ozd_shard_free_bytes` | Bar gauge |
| Per-shard status | `ozd_shard_status` (0=online, 1=suspect, 2=faulted) | State timeline |
| Per-shard EWMA | `ozd_shard_lat_ewma_ms` | Heatmap |
| Hedge threshold | `ozd_hedge_threshold_ms` | Stat |

## Provisioning (YAML)

```yaml
apiVersion: 1
providers:
  - name: ozd
    folder: OZD
    type: file
    options:
      path: /etc/grafana/dashboards/ozd.json
```

## Минимальный JSON (импорт)

```json
{
  "title": "OZD — OpenZFS Daemon",
  "uid": "ozd-main",
  "panels": [
    {
      "title": "PUT/GET ops/s",
      "type": "timeseries",
      "targets": [
        {"expr": "rate(ozd_puts_total[1m])", "legendFormat": "PUT"},
        {"expr": "rate(ozd_gets_total[1m])", "legendFormat": "GET"}
      ],
      "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8}
    },
    {
      "title": "PUT latency p50/p99",
      "type": "timeseries",
      "targets": [
        {"expr": "histogram_quantile(0.5, rate(ozd_put_duration_seconds_bucket[5m]))", "legendFormat": "p50"},
        {"expr": "histogram_quantile(0.99, rate(ozd_put_duration_seconds_bucket[5m]))", "legendFormat": "p99"}
      ],
      "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8}
    },
    {
      "title": "GET latency p50/p99",
      "type": "timeseries",
      "targets": [
        {"expr": "histogram_quantile(0.5, rate(ozd_get_duration_seconds_bucket[5m]))", "legendFormat": "p50"},
        {"expr": "histogram_quantile(0.99, rate(ozd_get_duration_seconds_bucket[5m]))", "legendFormat": "p99"}
      ],
      "gridPos": {"x": 0, "y": 8, "w": 12, "h": 8}
    },
    {
      "title": "Cache hit-rate",
      "type": "gauge",
      "targets": [
        {"expr": "rate(ozd_cache_hits_total[5m]) / (rate(ozd_cache_hits_total[5m]) + rate(ozd_cache_misses_total[5m]))"}
      ],
      "gridPos": {"x": 12, "y": 8, "w": 6, "h": 8}
    },
    {
      "title": "MRF queue / Hedge threshold",
      "type": "stat",
      "targets": [
        {"expr": "ozd_mrf_queue", "legendFormat": "MRF"},
        {"expr": "ozd_hedge_threshold_ms", "legendFormat": "hedge ms"}
      ],
      "gridPos": {"x": 18, "y": 8, "w": 6, "h": 8}
    },
    {
      "title": "Shard capacity",
      "type": "bargauge",
      "targets": [
        {"expr": "ozd_shard_free_bytes", "legendFormat": "shard {{shard}}"}
      ],
      "gridPos": {"x": 0, "y": 16, "w": 24, "h": 6}
    }
  ],
  "schemaVersion": 39,
  "version": 1
}
```
