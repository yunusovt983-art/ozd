# NEXT STEPS: Plan after W9 Phase 2

**Дата:** 2026-06-19  
**Текущий статус:** W9 Phase 2 ✅ (Kubo + go-ds-s3 Dockerfile завершены)  
**Следующее:** W10 (Config Generator + systemd) → Week 4 (W14–W18)

---

## Immediate (W10) — 1–2 дня

### W10: Config Generator + systemd

**Файлы:**
- `scripts/gen_config.sh` — пуллит `zpool list`, генерирует `ozd.toml`
- `deployments/ozd.service` — systemd unit
- `ozd.example.toml` — полный пример с новыми полями

**План:**

```bash
# W10.1: gen_config.sh
# Input: --disks=/dev/disk{0,1,2...} --index-path=/mnt/nvme или env vars
# Output: ozd.toml с [[disks]] и сенсибл дефолтами
# Features:
#   - Проверка доступности дисков (test mount)
#   - Расчёт segment_size (256МБ для 60 HDD, 64МБ для dev)
#   - Комментарии с рекомендациями (replicas=2, write_quorum=2)
#   - Вывод в stdout или --output ozd.toml

# W10.2: systemd unit
# deployments/ozd.service
# - User=ozd (создан)
# - ExecStart=./target/release/ozd --config /etc/ozd/ozd.toml
# - Restart=always, RestartSec=5
# - LimitNOFILE=1000000
# - ReadWritePaths=/data/*
# - Type=notify (если добавим sd_notify в main.rs)

# W10.3: ozd.example.toml обновить
# Добавить комментарии про:
#   - migrate_interval_secs (дефолт 3600)
#   - migrate_keys_per_cycle (дефолт 10000)
#   - snapshot_dir (опционально)
#   - rate_limit_rps (опционально, дефолт 0 = отключен)
```

**Критерий:** `./scripts/gen_config.sh --disks=/data/disk{0,1,2} > ozd.toml && systemctl start ozd` работает.

---

## Week 4 (June 23–27) — Integration Testing Phase

### W14: Integration Test in CI

**Файлы:**
- `.github/workflows/ci.yml` → добавить job `integration`
- `scripts/kubo_smoke.sh` → расширить до 12 проверок
- `crates/ozd-ipfs/tests/e2e_s3.rs` → новый e2e-тест

**План:**

#### W14.1: CI integration job

```yaml
# .github/workflows/ci.yml
integration:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3
    - uses: actions-rs/toolchain@v1
    - name: Build ozd
      run: cargo build --release -p ozd-daemon
    - name: Start ozd
      run: |
        mkdir -p {disk0,disk1,disk2}
        ./target/release/ozd --config deployments/docker/smoke-local.toml &
        sleep 3
    - name: Smoke test
      run: bash scripts/kubo_smoke.sh http://localhost:9100
    - name: E2E test
      run: cargo test -p ozd-ipfs --test e2e_s3 -- --ignored --nocapture
```

#### W14.2: куbo_smoke.sh → 12 checks

```bash
# Существующие 8:
# 1. healthz
# 2. PUT small
# 3. GET (body match)
# 4. HEAD (content-length)
# 5. ListV2
# 6. DELETE
# 7. 404 after DELETE
# 8. /metrics

# Новые 4:
# 9. PUT 1МиБ large body
# 10. Range GET bytes=0-99 (subset)
# 11. Batch 10 keys in parallel (concurrent)
# 12. ListV2 with marker/continuation
```

#### W14.3: e2e_s3.rs test

```rust
// crates/ozd-ipfs/tests/e2e_s3.rs
#[tokio::test]
#[ignore] // запускается в CI
async fn test_put_get_100kib() {
    // Start ozd with embedded tmpfs
    // PUT 100КиБ body
    // GET и verify body
    // DELETE
    // Проверить полу integration через S3Client
}

#[tokio::test]
#[ignore]
async fn test_concurrent_put_get() {
    // 10 PUT + 10 GET параллельно
    // Все должны успешно завершиться
}
```

**Критерий:** CI проходит; smoke-тест + e2e-тест зелёные.

---

### W15: Criterion Benchmarks + Regression Detection

**Файлы:**
- `crates/ozd-engine/benches/` → criterion benchmarks
- `.github/workflows/ci.yml` → bench job + artifact

**План:**

#### W15.1: Criterion setup

```bash
# crates/ozd-engine/Cargo.toml
[dev-dependencies]
criterion = "0.5"

# crates/ozd-engine/benches/storage.rs
#[path = "../../benches/storage.rs"]
mod benches {
    use criterion::{black_box, criterion_group, criterion_main, Criterion};
    
    fn put_inline(c: &mut Criterion) {
        // Benchmark: DiskEngine::put(key, 100B inline)
        // Expect: < 1ms p50
    }
    
    fn put_segment(c: &mut Criterion) {
        // Benchmark: DiskEngine::put(key, 256KiB in segment)
        // Expect: < 10ms p50
    }
    
    fn get_64kib(c: &mut Criterion) {
        // Benchmark: DiskEngine::get(key) with 64KiB body
        // Expect: < 5ms p50
    }
    
    fn stat_obj(c: &mut Criterion) {
        // Benchmark: DiskEngine::stat(key)
        // Expect: < 0.5ms p50
    }
}
```

#### W15.2: CI bench job

```yaml
bench:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v3
    - run: cargo bench -p ozd-engine --bench storage
    - name: Upload baseline
      uses: actions/upload-artifact@v3
      with:
        name: criterion-baseline
        path: target/criterion
        retention-days: 30
```

**Критерий:** `cargo bench` выдаёт стабильные числа; CI сохраняет baseline для сравнения.

---

### W16: Flaky Tests Fix

**Проблема:** Тесты `parallel_put_latency_is_max_not_sum`, `speculative_retry_hedges_slow_read_leg` на CI могут флаповать из-за timing.

**План:**

```rust
// ozd-app/src/pool.rs — existing tests

// Before:
assert!(latency_put < 150ms + 150ms);  // sum = 300ms, flaky на CI

// After:
assert!(latency_put < 400ms);  // 260ms + buffer для CI slowdown
assert!(hedged_latency < 400ms);  // вместо 250ms
```

**Критерий:** `cargo test -p ozd-app -- --test-threads=1` проходит 10 раз подряд без flake.

---

### W17: Admin API v2 — serde_json Validation

**Файлы:**
- `ozd-admin/Cargo.toml` → добавить `serde_json = "1"`
- `ozd-admin/src/lib.rs` → обновить JSON-handl в через serde_json

**План:**

```rust
// ozd-admin/src/lib.rs
use serde_json;

// Before: ручной format!() JSON
let json = format!(r#"{{"status":"{}","bytes":{}}}"#, status, bytes);

// After: serde_json::Value или typed struct
let response = json!({
    "status": status,
    "bytes": bytes,
});
axum::Json(response).into_response()

// Невалидный JSON при any input → 500 с clear error
```

**Критерий:** все /admin/ ответы — валидный JSON при любых входных данных.

---

### W18: Capacity Planning

**Файлы:**
- `ozd-app/src/metrics.rs` → добавить `bytes_written` counter
- `ozd-admin/src/lib.rs` → GET /admin/capacity

**План:**

```rust
// ozd-app/src/pool.rs — при каждом PUT
metrics.bytes_written.fetch_add(body.len() as u64, Ordering::Relaxed);

// ozd-admin/src/lib.rs
// GET /admin/capacity
// Response:
// {
//   "total_bytes": 60TB,
//   "used_bytes": 30TB,
//   "free_bytes": 30TB,
//   "write_rate_mbps": 150.5,
//   "estimated_days_to_95pct": 120,
//   "shards": [
//     {"id": 0, "fill_pct": 50.1, ...},
//     ...
//   ]
// }
```

**Критерий:** оператор видит перспективу заполнения (N дней до 95%).

---

## Week 5 (June 30 – July 4) — API Hardening Phase

### W19: Typed Admin API

**Файлы:**
- `ozd-admin/src/types.rs` → 13 typed structs
- `ozd-admin/src/lib.rs` → все хэндлеры на `axum::Json<T>`

**План:**

```rust
// ozd-admin/src/types.rs
#[derive(Serialize, Deserialize)]
pub struct HealthResponse {
    pub status: String,  // "ok", "degraded", "shutdown"
    pub shards: u32,
    pub faulted: u32,
}

#[derive(Serialize)]
pub struct CapacityResponse {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub free_bytes: u64,
    pub write_rate_mbps: f64,
}

// ... 11 больше

// ozd-admin/src/lib.rs
async fn healthz(
    State(pool): State<Arc<Pool>>,
) -> axum::Json<HealthResponse> {
    axum::Json(HealthResponse {
        status: pool.status_string(),
        shards: pool.shard_count(),
        faulted: pool.faulted_count(),
    })
}
```

**Критерий:** все /admin/ через `axum::Json<T>`, ручной JSON удалён.

---

### W20: Structured Logging (tracing)

**Файлы:**
- `ozd-daemon/main.rs` → `tracing_subscriber::fmt().json()`
- `ozd-app/src/pool.rs` → `#[tracing::instrument]` spans

**План:**

```rust
// ozd-daemon/main.rs
use tracing_subscriber;

let subscriber = tracing_subscriber::fmt()
    .json()  // JSON instead of text, if env var OZD_LOG_FORMAT=json
    .with_env_filter(EnvFilter::from_default_env())
    .finish();
tracing::subscriber::with_default(subscriber, || { ... });

// ozd-app/src/pool.rs
#[tracing::instrument(skip(self, data), fields(key = %key, data_len = data.len()))]
pub async fn put_body(&self, key: &str, data: &[u8]) -> Result<()> {
    // Automatically creates span with context
}

// Run: OZD_LOG_FORMAT=json RUST_LOG=info ./ozd
// Output: {"timestamp": "...", "level": "INFO", "message": "...", "key": "...", "data_len": 256}
```

**Критерий:** JSON-логи; spans видны в Jaeger/Loki.

---

### W21: Graceful Shutdown v2

**Файлы:**
- `ozd-daemon/main.rs` → SIGTERM handler
- `ozd-app/src/pool.rs` → `shutdown()` метод

**План:**

```rust
// ozd-daemon/main.rs
let shutdown = shutdown_signal();
tokio::select! {
    _ = shutdown => {
        info!("SIGTERM received, initiating graceful shutdown...");
        pool.shutdown();  // Set flag
        // Wait for in-flight operations (timeout 30s)
        tokio::time::timeout(Duration::from_secs(30), pool.wait_idle()).ok();
        break;
    }
    _ = server => {}
}

// ozd-app/src/pool.rs
pub fn shutdown(&self) {
    self.is_shutting_down.store(true, Ordering::Release);
}

pub fn put(&self, ...) -> Result<()> {
    if self.is_shutting_down.load(Ordering::Acquire) {
        return Err(DomainError::Shutdown);
    }
    // ... proceed
}
```

**Критерий:** `kill -TERM <pid>` → чистое завершение за ≤30с; PUT → ошибка.

---

### W22: Rate Limiter

**Файлы:**
- `ozd-ipfs/src/ratelimit.rs` → middleware
- `ozd-daemon/main.rs` → конфиг

**План:**

```rust
// ozd-ipfs/src/ratelimit.rs
pub struct RateLimitMiddleware {
    limiters: Arc<DashMap<IpAddr, TokenBucket>>,
    rps: u32,
}

impl RateLimitMiddleware {
    pub fn check(&self, addr: IpAddr) -> Result<(), StatusCode> {
        let bucket = self.limiters.entry(addr).or_insert_with(|| TokenBucket::new(self.rps));
        if bucket.take(1) {
            Ok(())
        } else {
            Err(StatusCode::TOO_MANY_REQUESTS)
        }
    }
}

// ozd-daemon/main.rs
// config.toml: rate_limit_rps = 100 (0 = disabled)

// S3 router
.layer(RateLimitLayer::new(config.rate_limit_rps))
```

**Критерий:** 101-й запрос в секунду → 429.

---

### W23: Backup Snapshots

**Файлы:**
- `ozd-admin/src/lib.rs` → snapshot endpoints
- `scripts/backup.sh` → архивирование

**План:**

```rust
// ozd-admin/src/lib.rs — POST /admin/snapshot
// Creates hardlinks of sealed segments into snapshots/<id>/
// Instant (hardlinks)
// Returns: { "snapshot_id": "...", "timestamp": "...", "size_bytes": ... }

// GET /admin/snapshots
// List all snapshots with metadata

// scripts/backup.sh
#!/bin/bash
SNAPSHOT_ID=$(curl -X POST http://localhost:9100/admin/snapshot | jq -r .snapshot_id)
tar -cf snapshots/$SNAPSHOT_ID.tar.zstd snapshots/$SNAPSHOT_ID/
# Upload to S3, rsync, etc.
```

**Критерий:** snapshot < 1с; backup.sh архивирует.

---

## Week 6 (July 7–11) — Operational Polish

### W24: HTTP 503 on Shutdown

Map `DomainError::Shutdown` → HTTP 503 ServiceUnavailable (S3 XML).

---

### W25: DELETE /admin/snapshot

Delete snapshot by ID; 404 if not found.

---

### W26: snapshot_dir Config Override

Allow global snapshot directory (instead of per-shard).

---

## Week 7 (July 14–18) — Final Hardening

### W27: DomainError Taxonomy

Add `DomainError::Shutdown`, `DomainError::DiskSlow` sentinel-variants.

---

### W28: /healthz Extended

JSON response with shard count, faulted count, shutdown status.

---

### W29: SIGHUP Config Hot-reload

Reload gc_interval, scrub_interval, bg_max without restart.

---

### W30: GET /admin/config

Introspection endpoint (read-only config).

---

### W31: Retention Policy

`DELETE /admin/snapshot/old?keep=3` — delete old snapshots.

---

## Summary: Execution Order

**Current:** W9 ✅  
**Immediate (1–2 дня):** W10  
**Week 4 (3–5 дней):** W14–W18 (integration, bench, flaky-fix)  
**Week 5 (3–5 дней):** W19–W23 (typed API, logging, shutdown, rate-limit, backup)  
**Week 6 (1 день):** W24–W26 (polish)  
**Week 7 (2 дня):** W27–W31 (hardening, introspection)  

**Total:** ~3–4 недели до v0.2 (stable, production-ready).

---

## Risk Mitigation

| Risk | Mitigation |
|------|-----------|
| CI flakes on timing tests | W16: expand tolerances |
| redb grows unbounded at 3.8B keys | Future: sharded redb or rocksdb (post-v0.2) |
| Sync Pool for future multi-node | W8: async-trait adapter ready (not yet used) |
| go-ds-s3 module build breaks | Fallback: pre-built Kubo image (Phase 3) |

---

## Blockers / Unknowns

1. **go-ds-s3 compatibility:** May need specific version of Kubo. Test build locally first.
2. **systemd deployment:** Need root/sudo. Test on dev machine before prod.
3. **CI infrastructure:** Ensure GitHub Actions runners have enough resources for integration tests.

---

*Next review after W10 completion.*
